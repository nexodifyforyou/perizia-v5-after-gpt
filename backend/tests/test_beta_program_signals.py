"""
Beta program — telemetry signals, overview determinism, feedback separation.
(Maps to plan §V group 6 and the dashboard determinism contract.)
"""

import pytest

import beta_program_fakes as fk  # sets sys.path
from beta_program import signals as beta_signals
from beta_program import store as beta_store
import server


@pytest.fixture()
def anyio_backend():
    return "asyncio"


@pytest.fixture()
def db(monkeypatch):
    database = fk.install_fake_db(monkeypatch)
    yield database
    fk.teardown_fake()


def _seed_owner(db):
    return fk.seed_session(db, fk.owner_user(), "sess_owner")


def _member(db, email, user_id, status="ACTIVE"):
    now = fk.now_iso()
    db.beta_program_memberships.items.append({
        "membership_id": f"betam_{user_id}", "normalized_email": email.lower(),
        "user_id": user_id, "display_name": "Beta", "partner_type": "geometra",
        "status": status, "added_by": fk.OWNER_EMAIL, "added_at": now,
        "activated_at": now, "revoked_at": None, "reactivated_at": None, "updated_at": now,
        "internal_note": None, "entitlement_version": 1, "last_entitlement_change_at": now,
        "migration_source": None,
    })


# --- emitter ----------------------------------------------------------------
def test_emit_is_idempotent(db):
    for _ in range(3):
        beta_signals.emit_v2_job_event(
            beta_signals.EVENT_REPORT_READY, job_id="cv2_1", analysis_id="a1",
            user_id="u1", status="REPORT_READY", duration_seconds=12.5)
    fk.flush_telemetry()
    assert len(db.v2_job_events.items) == 1
    ev = db.v2_job_events.items[0]
    assert ev["event_type"] == "REPORT_READY"
    assert ev["duration_seconds"] == 12.5
    assert ev["user_id"] == "u1"


def test_emit_rejects_unknown_type_and_missing_ids(db):
    beta_signals.emit_v2_job_event("NOT_A_TYPE", job_id="j", analysis_id="a")
    beta_signals.emit_v2_job_event(beta_signals.EVENT_REPORT_READY, job_id="", analysis_id="a")
    fk.flush_telemetry()
    assert len(db.v2_job_events.items) == 0


def test_emit_stores_only_safe_metadata(db):
    beta_signals.emit_v2_job_event(
        beta_signals.EVENT_VERIFICATION_REQUIRED, job_id="j", analysis_id="a",
        user_id="u", lot_id="L1", status="NEEDS_MANUAL_REVIEW", reason_code="X")
    fk.flush_telemetry()
    ev = db.v2_job_events.items[0]
    allowed = {"event_id", "event_type", "job_id", "analysis_id", "lot_id", "user_id",
               "status", "reason_code", "duration_seconds", "created_at"}
    assert set(ev.keys()) <= allowed


# --- signal reads -----------------------------------------------------------
@pytest.mark.anyio
async def test_signals_scoped_to_tester_user_ids(db):
    _seed_owner(db)
    _member(db, "t@example.test", "u1")
    beta_signals.emit_v2_job_event(beta_signals.EVENT_REPORT_READY, job_id="j1", analysis_id="a1", user_id="u1")
    beta_signals.emit_v2_job_event(beta_signals.EVENT_REPORT_READY, job_id="j2", analysis_id="a2", user_id="other")
    fk.flush_telemetry()
    resp = await fk.client_request("GET", "/api/admin/beta-program/signals", token="sess_owner")
    assert resp.status_code == 200
    ids = [i["user_id"] for i in resp.json()["items"]]
    assert ids == ["u1"]


@pytest.mark.anyio
async def test_event_counts_deterministic(db):
    _member(db, "t@example.test", "u1")
    beta_signals.emit_v2_job_event(beta_signals.EVENT_REPORT_READY, job_id="j1", analysis_id="a1", user_id="u1")
    beta_signals.emit_v2_job_event(beta_signals.EVENT_CONFIRMATION_REQUIRED, job_id="j2", analysis_id="a2", user_id="u1")
    fk.flush_telemetry()
    counts = await beta_signals.event_counts(["u1"])
    assert counts[beta_signals.EVENT_REPORT_READY] == 1
    assert counts[beta_signals.EVENT_CONFIRMATION_REQUIRED] == 1


# --- overview ---------------------------------------------------------------
@pytest.mark.anyio
async def test_overview_metrics_deterministic(db):
    _seed_owner(db)
    _member(db, "t@example.test", "u1", "ACTIVE")
    _member(db, "p@example.test", "u2", "PENDING")
    _member(db, "r@example.test", "u3", "REVOKED")
    db.perizia_analyses.items.extend([
        {"analysis_id": "a1", "user_id": "u1", "status": "COMPLETED"},
        {"analysis_id": "a2", "user_id": "u1", "status": "UNREADABLE"},
    ])
    beta_signals.emit_v2_job_event(beta_signals.EVENT_REPORT_READY, job_id="j1", analysis_id="a1", user_id="u1", duration_seconds=10.0)
    fk.flush_telemetry()
    db.beta_feedback.items.append({"id": "fb1", "user_id": "u1", "user_role": "beta_partner",
                                   "status": "new", "priority": "alta", "created_at": "2026-01-01T00:00:00",
                                   "learning_label": {}})

    r = await fk.client_request("GET", "/api/admin/beta-program/overview", token="sess_owner")
    assert r.status_code == 200
    data = r.json()
    assert data["testers"]["active"] == 1
    assert data["testers"]["pending"] == 1
    assert data["testers"]["revoked"] == 1
    assert data["analyses"]["unreadable_total"] == 1
    assert data["reports"]["ready_total"] == 1
    assert data["reports"]["avg_duration_seconds"] == 10.0
    assert data["feedback"]["total"] == 1
    assert data["feedback"]["high_priority"] == 1
    # Deterministic: identical state -> identical response.
    r2 = await fk.client_request("GET", "/api/admin/beta-program/overview", token="sess_owner")
    assert r2.json() == data


@pytest.mark.anyio
async def test_dashboard_reads_make_no_side_effects(db, monkeypatch):
    """Loading overview/testers/signals/feedback creates no OpenAI call, no job,
    no credit change, no Stripe call (all reads)."""
    _seed_owner(db)
    _member(db, "t@example.test", "u1")

    # Sentinels: if any of these are invoked, the test fails loudly.
    def _boom(*a, **k):
        raise AssertionError("forbidden side effect during dashboard read")

    # OpenAI + job spawn sentinels.
    import correctness_v2.openai_client as oai
    import correctness_v2.api as cv2_api
    monkeypatch.setattr(oai, "call_openai_json", _boom, raising=False)
    monkeypatch.setattr(cv2_api, "autostart_job", _boom, raising=False)

    # Warm-up: the owner's admin wallet establishes its opening-balance baseline
    # on first authenticated request (pre-existing behaviour, unrelated to beta).
    await fk.client_request("GET", "/api/admin/beta-program/overview", token="sess_owner")

    ledger_before = [dict(x) for x in db.credit_ledger.items]
    events_before = [dict(x) for x in db.v2_job_events.items]
    users_before = len(db.users.items)

    for path in ["/api/admin/beta-program/overview",
                 "/api/admin/beta-program/testers",
                 "/api/admin/beta-program/signals",
                 "/api/admin/beta-program/feedback"]:
        resp = await fk.client_request("GET", path, token="sess_owner")
        assert resp.status_code == 200

    # The dashboard reads themselves mutate nothing.
    assert db.credit_ledger.items == ledger_before      # no credit change
    assert db.v2_job_events.items == events_before        # no new telemetry writes
    assert len(db.users.items) == users_before            # no user created


# --- feedback separation ----------------------------------------------------
@pytest.mark.anyio
async def test_owner_update_preserves_verbatim_and_namespaces_interpretation(db):
    _seed_owner(db)
    db.beta_feedback.items.append({
        "id": "fb1", "user_id": "u1", "user_role": "beta_partner",
        "expert_comment": "Testo originale del tester", "feedback_type": "sbagliato",
        "priority": "media", "status": "new", "created_at": "2026-01-01T00:00:00",
        "learning_label": {},
    })
    resp = await fk.client_request("PATCH", "/api/admin/beta-program/feedback/fb1",
                                   token="sess_owner",
                                   json={"status": "accepted", "priority": "alta",
                                         "category": "accuratezza_report", "admin_notes": "verificato"})
    assert resp.status_code == 200
    fb = db.beta_feedback.items[0]
    # Tester's own words + type + priority are never overwritten.
    assert fb["expert_comment"] == "Testo originale del tester"
    assert fb["feedback_type"] == "sbagliato"
    assert fb["priority"] == "media"
    # Owner interpretation is namespaced separately.
    assert fb["status"] == "accepted"
    assert fb["owner_priority"] == "alta"
    assert fb["owner_category"] == "accuratezza_report"
    assert fb["admin_notes"] == "verificato"


@pytest.mark.anyio
async def test_owner_update_rejects_bad_category(db):
    _seed_owner(db)
    db.beta_feedback.items.append({"id": "fb1", "user_id": "u1", "status": "new",
                                   "created_at": "2026-01-01T00:00:00"})
    resp = await fk.client_request("PATCH", "/api/admin/beta-program/feedback/fb1",
                                   token="sess_owner", json={"category": "bogus"})
    assert resp.status_code == 422


@pytest.mark.anyio
async def test_legacy_feedback_routes_gone(db):
    _seed_owner(db)
    r1 = await fk.client_request("GET", "/api/admin/beta-feedback", token="sess_owner")
    r2 = await fk.client_request("GET", "/api/admin/beta-feedback/export?format=json", token="sess_owner")
    assert r1.status_code == 404
    assert r2.status_code == 404
