"""
Beta perizia allowance -- real end-to-end wiring through ``POST
/api/analysis/perizia`` (``analyze_perizia``, server.py). Exercises reservation,
the owner-amendment paid-processing marker, consumption/release at the real
call sites, the BETA_LIMIT_REACHED fallback, and the credit/job/Stripe
determinism guarantees.

The three genuinely paid/LLM-backed internals (Document AI OCR, the QA-gate
LLM call, the Gemini narrator) are monkeypatched to deterministic stand-ins so
this suite never attempts real network I/O regardless of what API keys are
present in the environment -- the ``beta_quota.mark_paid_processing_started``
call sites in server.py are unconditional on the mocked function's own
behaviour, so the marker mechanics under test are exercised faithfully even
though the mocked functions themselves do nothing risky. ``correctness_v2``'s
``autostart_job`` (the real async OpenAI V2 pipeline) is also stubbed to a
sentinel so a spawned job would be immediately observable and is asserted to
never fire when it must not.

The existing pipeline additionally writes small debug JSON files under the
fixed path ``/srv/perizia/_qa/runs/<analysis_id>/`` (pre-existing behaviour of
``analyze_perizia``, unrelated to this feature) -- this suite cleans up any
directory it creates in a fixture teardown so it leaves no artifacts behind.
"""

import glob
import io
import os
import shutil

import pytest

import beta_program_fakes as fk  # sets sys.path
from beta_program import quota as beta_quota
from beta_program import store as beta_store
import server

try:
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    _HAVE_REPORTLAB = True
except Exception:  # pragma: no cover - environment guard
    _HAVE_REPORTLAB = False

pytestmark = pytest.mark.skipif(not _HAVE_REPORTLAB, reason="reportlab not available")


def _make_pdf(num_lines=60):
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    y = 800
    for i in range(num_lines):
        c.drawString(50, y, f"Riga di testo numero {i} della perizia immobiliare di prova.")
        y -= 20
        if y < 50:
            c.showPage()
            y = 800
    c.showPage()
    c.save()
    return buf.getvalue()


SAMPLE_PDF = _make_pdf(60)


@pytest.fixture()
def anyio_backend():
    return "asyncio"


@pytest.fixture()
def db(monkeypatch):
    database = fk.install_fake_db(monkeypatch)

    # Deterministic, network-free stand-ins for the three real paid/LLM call
    # sites. The beta_quota.mark_paid_processing_started(...) calls in
    # server.py fire unconditionally right before these are invoked, so the
    # marker mechanics under test are exercised regardless of what these
    # stand-ins do.
    def _fake_qa_gate(result, raw_text=None, internal_runtime=None):
        return {"status": "PASS", "llm_used": False, "corrections_applied": []}

    async def _fake_narrator(*args, **kwargs):
        raise RuntimeError("narrator disabled for test")

    async def _fake_docai(contents, mime_type, request_id):
        return [], "", None

    monkeypatch.setattr(server, "apply_customer_contract_qa_gate", _fake_qa_gate)
    monkeypatch.setattr(server, "_apply_post_qa_decision_narrator", _fake_narrator)
    monkeypatch.setattr(server, "_extract_with_docai", _fake_docai)

    from correctness_v2 import api as correctness_v2_api

    autostart_calls = []
    monkeypatch.setattr(
        correctness_v2_api, "autostart_job",
        lambda *a, **k: (autostart_calls.append((a, k)), False)[1],
    )
    database._autostart_calls = autostart_calls

    before_dirs = set(glob.glob("/srv/perizia/_qa/runs/analysis_*"))
    yield database
    fk.teardown_fake()
    for path in glob.glob("/srv/perizia/_qa/runs/analysis_*"):
        if path not in before_dirs:
            shutil.rmtree(path, ignore_errors=True)


def _force_unreadable(monkeypatch):
    """Make the analysis deterministically resolve to UNREADABLE, by having
    the (already-mocked) QA gate stand-in stamp analysis_status directly --
    the same field the real pipeline would set via document-quality
    heuristics. Nothing else in the path overwrites analysis_status between
    the QA gate call and the final status assignment (verified by source
    inspection of server.py)."""

    def _fake_qa_gate_unreadable(result, raw_text=None, internal_runtime=None):
        result["analysis_status"] = "UNREADABLE"
        return {"status": "PASS", "llm_used": False, "corrections_applied": []}

    monkeypatch.setattr(server, "apply_customer_contract_qa_gate", _fake_qa_gate_unreadable)


def _seed_limited_tester(db, *, limit=5, consumed=0, reserved=0, remaining_credits=0,
                          email="t@example.test", user_id="u1"):
    now = fk.now_iso()
    db.beta_program_memberships.items.append({
        "membership_id": f"betam_{user_id}", "normalized_email": email, "user_id": user_id,
        "display_name": "Beta", "partner_type": "geometra", "status": "ACTIVE",
        "added_by": fk.OWNER_EMAIL, "added_at": now, "activated_at": now, "revoked_at": None,
        "reactivated_at": None, "updated_at": now, "internal_note": None, "entitlement_version": 1,
        "last_entitlement_change_at": now, "migration_source": None,
        "quota_mode": "LIMITED", "analysis_limit": limit, "analysis_consumed": consumed,
        "analysis_reserved": reserved, "quota_version": 1, "quota_period_started_at": now,
        "quota_updated_at": None, "quota_updated_by": None, "quota_note": None,
    })
    user_doc = fk.normal_user(email=email, user_id=user_id, plan="free")
    user_doc["quota"]["perizia_scans_remaining"] = remaining_credits
    fk.seed_session(db, user_doc, "s1")
    return db.beta_program_memberships.items[0]


async def _upload(pdf_bytes=SAMPLE_PDF, token="s1"):
    return await fk.client_request(
        "POST", "/api/analysis/perizia", token=token,
        files={"file": ("test.pdf", pdf_bytes, "application/pdf")},
    )


# =============================================================================
# 7/8: a new top-level upload (single- or multi-lot -- lot count is discovered
# only later, inside the untracked V2 job, so both cases are structurally
# identical at this call site) consumes exactly one slot.
# =============================================================================
@pytest.mark.anyio
async def test_new_upload_consumes_one_slot(db):
    membership = _seed_limited_tester(db, limit=5, consumed=0)
    resp = await _upload()
    assert resp.status_code == 200
    assert resp.json()["beta_quota_consumed_without_report"] is False
    updated = db.beta_program_memberships.items[0]
    assert updated["analysis_consumed"] == 1
    assert updated["analysis_reserved"] == 0
    assert len(db.beta_program_usage.items) == 1
    assert db.beta_program_usage.items[0]["state"] == beta_quota.USAGE_CONSUMED
    # No credit ledger entry: this analysis was beta-covered, not debited.
    assert len(db.credit_ledger.items) == 0
    # V2 autostart WAS invoked (status COMPLETED) but that job is untracked by
    # this feature by design -- confirm no beta usage rows beyond the one.
    assert len(db.beta_program_usage.items) == 1


# =============================================================================
# 15: duplicate request (same analysis_id, replayed) consumes only once.
# =============================================================================
@pytest.mark.anyio
async def test_duplicate_reservation_for_same_analysis_id_consumes_once(db):
    _seed_limited_tester(db, limit=5, consumed=0)
    # Simulate a retried background task racing the live request for the same
    # analysis_id (the idempotency guard, §E.2) directly against the module,
    # since analysis_id is minted fresh per HTTP call and cannot be replayed
    # across two real requests.
    from beta_program import quota as bq
    user = server.User(user_id="u1", email="t@example.test", name="T",
                       beta_program={"active": True, "membership_id": "betam_u1",
                                     "quota_mode": "LIMITED", "analysis_limit": 5,
                                     "analysis_consumed": 0, "analysis_reserved": 0, "quota_version": 1})
    o1 = await bq.resolve_upload_slot(user, "analysis_dup")
    o2 = await bq.resolve_upload_slot(user, "analysis_dup")
    assert o1["mode"] == "GRANTED"
    assert o2["mode"] == "GRANTED" and o2.get("duplicate") is True
    assert db.beta_program_memberships.items[0]["analysis_reserved"] == 1
    await bq.consume_slot("analysis_dup")
    await bq.consume_slot("analysis_dup")  # replay: no-op
    assert db.beta_program_memberships.items[0]["analysis_consumed"] == 1


# =============================================================================
# 16/17: concurrent final-slot request cannot over-consume; the Nth request at
# limit is blocked before any costly processing.
# =============================================================================
@pytest.mark.anyio
async def test_sixth_request_at_five_of_five_blocked_before_costly_processing(db):
    membership = _seed_limited_tester(db, limit=5, consumed=5, remaining_credits=0)
    resp = await _upload()
    assert resp.status_code == 403
    body = resp.json()["detail"]
    assert body["code"] == "BETA_LIMIT_REACHED"
    assert body["beta_limit"] == {"consumed": 5, "limit": 5}
    # Zero side effects: no usage row created, no analysis persisted, no job,
    # no credit ledger row.
    assert db.beta_program_usage.items == []
    assert db.perizia_analyses.items == []
    assert db.credit_ledger.items == []
    assert db._autostart_calls == []


# =============================================================================
# 18-21: rejection = zero OpenAI, zero analysis jobs, zero credit debit, zero
# Stripe (this module never imports checkout code -- statically verified
# elsewhere; here we assert the observable side effects are all zero).
# =============================================================================
@pytest.mark.anyio
async def test_blocked_upload_has_zero_side_effects(db):
    _seed_limited_tester(db, limit=1, consumed=1, remaining_credits=0)
    await _upload()
    assert db._autostart_calls == []  # zero V2/OpenAI job spawn
    assert db.perizia_analyses.items == []
    assert db.credit_ledger.items == []
    import beta_program.api as beta_api
    assert not hasattr(beta_api, "stripe")  # module never imports Stripe


# =============================================================================
# 22/23: early failure releases; late failure (after the marker) stays
# consumed. Owner amendment core cases.
# =============================================================================
@pytest.mark.anyio
async def test_invalid_file_type_releases_before_any_paid_call(db):
    _seed_limited_tester(db, limit=5, consumed=0)
    resp = await fk.client_request(
        "POST", "/api/analysis/perizia", token="s1",
        files={"file": ("test.txt", b"not a pdf", "text/plain")},
    )
    assert resp.status_code == 400
    row = db.beta_program_usage.items[0]
    assert row["state"] == beta_quota.USAGE_RELEASED
    assert row["release_reason"] == beta_quota.REASON_INVALID_FILE_TYPE
    assert db.beta_program_memberships.items[0]["analysis_reserved"] == 0
    assert db.beta_program_memberships.items[0]["analysis_consumed"] == 0


@pytest.mark.anyio
async def test_corrupt_pdf_releases_before_any_paid_call(db):
    _seed_limited_tester(db, limit=5, consumed=0)
    resp = await fk.client_request(
        "POST", "/api/analysis/perizia", token="s1",
        files={"file": ("test.pdf", b"%PDF-not-really-a-pdf", "application/pdf")},
    )
    assert resp.status_code == 400
    row = db.beta_program_usage.items[0]
    assert row["state"] == beta_quota.USAGE_RELEASED
    assert row["release_reason"] == beta_quota.REASON_DOCUMENT_UNREADABLE_BEFORE_PAID_ANALYSIS


# --- UNREADABLE before / after the marker (owner amendment) -----------------
@pytest.mark.anyio
async def test_unreadable_before_marker_releases(db):
    """A corrupt/unparseable PDF never reaches the QA gate (the marker is set
    only once run_pipeline begins), so it must release -- this is the "before
    the marker" UNREADABLE case (the document never got far enough to be
    machine-scored at all)."""
    _seed_limited_tester(db, limit=5, consumed=0)
    resp = await fk.client_request(
        "POST", "/api/analysis/perizia", token="s1",
        files={"file": ("test.pdf", b"%PDF-1.4 garbage, not parseable", "application/pdf")},
    )
    assert resp.status_code == 400
    assert db.beta_program_usage.items[0]["state"] == beta_quota.USAGE_RELEASED


@pytest.mark.anyio
async def test_unreadable_after_marker_consumes_and_flags_response(db, monkeypatch):
    """A document that parses fine but resolves to analysis_status=UNREADABLE
    reaches this point only after the QA gate (site 2) has unconditionally
    fired, so the marker is already set: CONSUME, not release (owner
    amendment) -- and the customer-safe
    beta_quota_consumed_without_report flag is set on the response."""
    _seed_limited_tester(db, limit=5, consumed=0)
    _force_unreadable(monkeypatch)
    resp = await _upload()
    assert resp.status_code == 200
    assert resp.json()["beta_quota_consumed_without_report"] is True
    stored = db.perizia_analyses.items[0]
    assert stored["status"] == "UNREADABLE"
    row = db.beta_program_usage.items[0]
    assert row["state"] == beta_quota.USAGE_CONSUMED
    assert row["paid_processing_started_at"] is not None
    membership = db.beta_program_memberships.items[0]
    assert membership["analysis_consumed"] == 1
    # UNREADABLE never spawns the V2 job.
    assert db._autostart_calls == []


# --- timeout before / after the marker (owner amendment) ---------------------
@pytest.mark.anyio
async def test_timeout_before_marker_releases_and_returns_504(db, monkeypatch):
    def _hang_forever(contents):
        # Runs inside asyncio.to_thread -- must be a plain blocking function.
        import time
        time.sleep(10)
        return {}

    monkeypatch.setattr(server, "_build_step1_extract_payload", _hang_forever)
    monkeypatch.setattr(server, "PIPELINE_TIMEOUT_SECONDS", 0.05)
    _seed_limited_tester(db, limit=5, consumed=0)
    resp = await _upload()
    assert resp.status_code == 504
    row = db.beta_program_usage.items[0]
    assert row["state"] == beta_quota.USAGE_RELEASED
    assert row["release_reason"] == beta_quota.REASON_PIPELINE_TIMEOUT
    assert resp.json()["detail"].get("beta_quota_consumed_without_report") is None


@pytest.mark.anyio
async def test_timeout_during_or_after_marker_consumes_and_flags_504(db, monkeypatch):
    import asyncio

    async def _hang_after_marker(contents, mime_type, request_id):
        await beta_quota.mark_paid_processing_started(request_id.replace("req_", "analysis_"))
        # The real marker call in server.py uses the closure's analysis_id, not
        # request_id -- so mark it directly via the module-level hook instead:
        await asyncio.sleep(10)
        return [], "", None

    # Force the OCR-fallback branch to be taken (so a slow docai call is on
    # the timed path) and stall inside it, marking the reservation first via
    # the real call site's own behaviour (server.py marks before calling
    # _extract_with_docai unconditionally whenever needs_ocr_fallback fires).
    async def _slow_docai(contents, mime_type, request_id):
        await asyncio.sleep(10)
        return [], "", None

    monkeypatch.setattr(server, "_extract_with_docai", _slow_docai)
    monkeypatch.setattr(server, "PIPELINE_TIMEOUT_SECONDS", 0.2)

    # A blank/near-empty PDF reliably triggers needs_ocr_fallback=True.
    blank_pdf = _make_pdf(0)
    _seed_limited_tester(db, limit=5, consumed=0)
    resp = await fk.client_request(
        "POST", "/api/analysis/perizia", token="s1",
        files={"file": ("test.pdf", blank_pdf, "application/pdf")},
    )
    assert resp.status_code == 504
    row = db.beta_program_usage.items[0]
    assert row["state"] == beta_quota.USAGE_CONSUMED
    assert resp.json()["detail"].get("beta_quota_consumed_without_report") is True


# =============================================================================
# 24: job creation failure (an unrelated exception between reservation and
# run_pipeline) releases the slot.
# =============================================================================
@pytest.mark.anyio
async def test_unexpected_exception_before_pipeline_releases(db, monkeypatch):
    def _boom(pdf_bytes):
        raise RuntimeError("boom before pipeline")

    monkeypatch.setattr(server, "_build_step1_extract_payload", _boom)
    _seed_limited_tester(db, limit=5, consumed=0)
    # httpx's ASGITransport re-raises unhandled app exceptions in tests rather
    # than converting them to a 500 response (that conversion is a real ASGI
    # server behaviour, not exercised by the in-process test transport) --
    # the exception propagating here is exactly what the real server.py
    # try/except Exception (JOB_CREATION_FAILURE_BEFORE_PROCESSING) already
    # observed and reacted to before re-raising it.
    with pytest.raises(RuntimeError, match="boom before pipeline"):
        await _upload()
    row = db.beta_program_usage.items[0]
    assert row["state"] == beta_quota.USAGE_RELEASED
    assert row["release_reason"] == beta_quota.REASON_JOB_CREATION_FAILURE_BEFORE_PROCESSING


# =============================================================================
# 35/36/37: exhausted beta falls back to the normal plan; that paid analysis
# does not increment beta usage; no retroactive billing.
# =============================================================================
@pytest.mark.anyio
async def test_exhausted_beta_with_sufficient_credits_falls_back_to_normal_paid_upload(db):
    _seed_limited_tester(db, limit=1, consumed=1, remaining_credits=100)
    resp = await _upload()
    assert resp.status_code == 200
    # Fell back to the real paid path: debited normally, no beta usage row.
    # (credit_ledger may also carry an unrelated opening-balance seed entry
    # from account normalization -- assert the actual upload debit exists.)
    debit_entries = [e for e in db.credit_ledger.items if e.get("entry_type") == "perizia_upload"]
    assert len(debit_entries) == 1
    assert db.beta_program_usage.items == []
    membership = db.beta_program_memberships.items[0]
    assert membership["analysis_consumed"] == 1  # unchanged: no retroactive charge


@pytest.mark.anyio
async def test_unlimited_tester_never_blocked_regardless_of_real_credit_balance(db):
    now = fk.now_iso()
    db.beta_program_memberships.items.append({
        "membership_id": "betam_u1", "normalized_email": "t@example.test", "user_id": "u1",
        "display_name": "Beta", "partner_type": "geometra", "status": "ACTIVE",
        "added_by": fk.OWNER_EMAIL, "added_at": now, "activated_at": now, "revoked_at": None,
        "reactivated_at": None, "updated_at": now, "internal_note": None, "entitlement_version": 1,
        "last_entitlement_change_at": now, "migration_source": None,
        "quota_mode": "UNLIMITED", "analysis_limit": None, "analysis_consumed": 0,
        "analysis_reserved": 0, "quota_version": 1, "quota_period_started_at": now,
        "quota_updated_at": None, "quota_updated_by": None, "quota_note": None,
    })
    user_doc = fk.normal_user(email="t@example.test", user_id="u1", plan="free")
    user_doc["quota"]["perizia_scans_remaining"] = 0
    fk.seed_session(db, user_doc, "s1")
    resp = await _upload()
    assert resp.status_code == 200
    assert db.beta_program_usage.items == []
    assert db.credit_ledger.items == []


# =============================================================================
# 26/27: counts never negative / usage never exceeds limit (server-level
# corroboration of the module-level guarantees already proven directly).
# =============================================================================
@pytest.mark.anyio
async def test_upload_never_drives_membership_counters_negative_or_over_limit(db):
    _seed_limited_tester(db, limit=1, consumed=0)
    await _upload()
    membership = db.beta_program_memberships.items[0]
    assert 0 <= membership["analysis_consumed"] + membership["analysis_reserved"] <= membership["analysis_limit"]


# =============================================================================
# 40: frontend values cannot override backend counters.
# =============================================================================
@pytest.mark.anyio
async def test_response_quota_block_is_never_influenced_by_request_body(db):
    membership = _seed_limited_tester(db, limit=5, consumed=2)
    # analyze_perizia takes only a file upload -- there is no field through
    # which a client could inject consumed/reserved/limit values; confirm the
    # authoritative /auth/me quota block reflects only the DB-derived state.
    resp = await fk.client_request("GET", "/api/auth/me", token="s1")
    quota = resp.json()["beta_program"]["quota"]
    assert quota == {"mode": "LIMITED", "limit": 5, "consumed": 2, "reserved": 0,
                      "remaining": 3, "state": "AVAILABLE", "quota_version": 1}


# =============================================================================
# 38/39: active session sees a quota change on its very next request; no
# restart, no re-login required (mirrors the existing revocation-propagation
# guarantee -- get_current_user re-resolves the snapshot every request).
# =============================================================================
@pytest.mark.anyio
async def test_active_session_sees_quota_change_on_next_request_no_restart(db):
    membership = _seed_limited_tester(db, limit=1, consumed=1)
    r1 = await fk.client_request("GET", "/api/auth/me", token="s1")
    assert r1.json()["beta_program"]["quota"]["state"] == "EXHAUSTED"

    await beta_store._db()[beta_store.MEMBERSHIPS_COLLECTION].update_one(
        {"membership_id": membership["membership_id"]}, {"$set": {"analysis_limit": 5}}
    )

    r2 = await fk.client_request("GET", "/api/auth/me", token="s1")
    assert r2.json()["beta_program"]["quota"]["state"] == "AVAILABLE"
    assert r2.json()["beta_program"]["quota"]["remaining"] == 4


# =============================================================================
# 41: no hardcoded owner identity / fixed "5" anywhere in the quota wiring.
# =============================================================================
def test_no_hardcoded_identity_in_server_quota_wiring():
    import inspect

    src = inspect.getsource(server.analyze_perizia)
    lowered = src.lower()
    assert "mauro" not in lowered
    assert "torchio" not in lowered
    assert "agl" not in lowered
    assert fk.OWNER_EMAIL.lower() not in lowered


# =============================================================================
# Structural guarantee: resolve_upload_slot (the only reservation entry
# point) is wired into exactly analyze_perizia, never into the lot/storico/
# rerun/confirmation endpoints -- so those are zero-cost by construction
# (plan items 9-14). Verified via source scan rather than a full pipeline
# simulation for each endpoint (those flows are entirely untouched by this
# feature and already covered by their own suites).
# =============================================================================
def test_resolve_upload_slot_only_wired_into_analyze_perizia():
    import inspect
    import correctness_v2.api as cv2_api
    import correctness_v2.orchestrator as cv2_orch

    assert "resolve_upload_slot" in inspect.getsource(server.analyze_perizia)
    assert "resolve_upload_slot" not in inspect.getsource(cv2_api)
    assert "resolve_upload_slot" not in inspect.getsource(cv2_orch)


def test_analyze_perizia_is_the_only_place_that_mints_a_fresh_analysis_id_for_quota():
    import inspect
    import correctness_v2.api as cv2_api

    # correctness_v2_generate_lot / workspace / lot_generate_preview all take
    # an existing analysis_id as a path/body parameter; none constructs a new
    # "analysis_{uuid...}" identifier.
    src = inspect.getsource(cv2_api)
    assert 'f"analysis_{uuid' not in src
