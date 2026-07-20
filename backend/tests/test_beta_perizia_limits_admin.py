"""
Beta perizia allowance -- owner-only admin API (set mode/limit, start a new
phase, list historical phases) + the quota-defaults migration backfill.
(Maps to docs/beta_perizia_limits_plan.md §U groups 6, 8.)
"""

import pytest

import beta_program_fakes as fk  # sets sys.path
from beta_program import migrate as beta_migrate
from beta_program import quota as beta_quota
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


def _beta_member_row(db, email, user_id, status="ACTIVE", **quota_overrides):
    now = fk.now_iso()
    doc = {
        "membership_id": f"betam_{user_id}", "normalized_email": email.lower(),
        "user_id": user_id if status != "PENDING" else None, "display_name": "Beta",
        "partner_type": "geometra", "status": status, "added_by": fk.OWNER_EMAIL,
        "added_at": now, "activated_at": now if status == "ACTIVE" else None,
        "revoked_at": now if status == "REVOKED" else None, "reactivated_at": None,
        "updated_at": now, "internal_note": None, "entitlement_version": 1,
        "last_entitlement_change_at": now, "migration_source": None,
        "quota_mode": "UNLIMITED", "analysis_limit": None, "analysis_consumed": 0,
        "analysis_reserved": 0, "quota_version": 1, "quota_period_started_at": now,
        "quota_updated_at": None, "quota_updated_by": None, "quota_note": None,
    }
    doc.update(quota_overrides)
    db.beta_program_memberships.items.append(doc)
    return doc


# --- authorization matrix (owner / non-owner-admin / tester / customer / anon) ---
@pytest.mark.anyio
async def test_owner_can_set_a_limit(db):
    _seed_owner(db)
    _beta_member_row(db, "t@example.test", "u1", "ACTIVE")
    mid = db.beta_program_memberships.items[0]["membership_id"]
    resp = await fk.client_request(
        "PATCH", f"/api/admin/beta-program/testers/{mid}/quota", token="sess_owner",
        json={"quota_mode": "LIMITED", "analysis_limit": 5},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["tester"]["quota"] == {
        "mode": "LIMITED", "limit": 5, "consumed": 0, "reserved": 0,
        "remaining": 5, "state": "AVAILABLE", "quota_version": 1,
    }
    assert any(a["action"] == beta_quota.ACTION_QUOTA_MODE_CHANGED for a in db.beta_program_audit.items)
    assert any(a["action"] == beta_quota.ACTION_QUOTA_LIMIT_CHANGED for a in db.beta_program_audit.items)


@pytest.mark.anyio
async def test_non_owner_admin_cannot_manage_quota(db, monkeypatch):
    monkeypatch.setattr(server, "ADMIN_EMAILS", frozenset({fk.OWNER_EMAIL, "other-admin@nexodify.com"}))
    server.CORRECTNESS_V2_ADMIN_VIEW_EMAIL = fk.OWNER_EMAIL
    _beta_member_row(db, "t@example.test", "u1", "ACTIVE")
    mid = db.beta_program_memberships.items[0]["membership_id"]
    fk.seed_session(db, {"user_id": "ua", "email": "other-admin@nexodify.com", "name": "A",
                         "plan": "free", "is_master_admin": False, "quota": {}}, "sess_a")
    resp = await fk.client_request(
        "PATCH", f"/api/admin/beta-program/testers/{mid}/quota", token="sess_a",
        json={"quota_mode": "LIMITED", "analysis_limit": 5},
    )
    assert resp.status_code == 403


@pytest.mark.anyio
async def test_tester_cannot_manage_own_limit(db):
    _beta_member_row(db, "t@example.test", "u1", "ACTIVE")
    mid = db.beta_program_memberships.items[0]["membership_id"]
    fk.seed_session(db, fk.normal_user(email="t@example.test", user_id="u1", plan="free"), "sess_t")
    resp = await fk.client_request(
        "PATCH", f"/api/admin/beta-program/testers/{mid}/quota", token="sess_t",
        json={"quota_mode": "LIMITED", "analysis_limit": 5},
    )
    assert resp.status_code == 403


@pytest.mark.anyio
async def test_normal_customer_cannot_manage_quota(db):
    _beta_member_row(db, "other@example.test", "u2", "ACTIVE")
    mid = db.beta_program_memberships.items[0]["membership_id"]
    fk.seed_session(db, fk.normal_user(email="cust@example.test", user_id="uc", plan="free"), "sess_c")
    resp = await fk.client_request(
        "PATCH", f"/api/admin/beta-program/testers/{mid}/quota", token="sess_c",
        json={"quota_mode": "LIMITED", "analysis_limit": 5},
    )
    assert resp.status_code == 403


@pytest.mark.anyio
async def test_unauthenticated_cannot_manage_quota(db):
    _beta_member_row(db, "t@example.test", "u1", "ACTIVE")
    mid = db.beta_program_memberships.items[0]["membership_id"]
    resp = await fk.client_request(
        "PATCH", f"/api/admin/beta-program/testers/{mid}/quota",
        json={"quota_mode": "LIMITED", "analysis_limit": 5},
    )
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_limit_validates_positive_integer(db):
    _seed_owner(db)
    _beta_member_row(db, "t@example.test", "u1", "ACTIVE")
    mid = db.beta_program_memberships.items[0]["membership_id"]
    for bad in (0, -1):
        resp = await fk.client_request(
            "PATCH", f"/api/admin/beta-program/testers/{mid}/quota", token="sess_owner",
            json={"quota_mode": "LIMITED", "analysis_limit": bad},
        )
        assert resp.status_code == 422
    resp_over = await fk.client_request(
        "PATCH", f"/api/admin/beta-program/testers/{mid}/quota", token="sess_owner",
        json={"quota_mode": "LIMITED", "analysis_limit": beta_quota.MAX_ANALYSIS_LIMIT + 1},
    )
    assert resp_over.status_code == 422
    resp_bad_mode = await fk.client_request(
        "PATCH", f"/api/admin/beta-program/testers/{mid}/quota", token="sess_owner",
        json={"quota_mode": "NOT_A_MODE", "analysis_limit": 5},
    )
    assert resp_bad_mode.status_code == 422


# --- increase / decrease semantics -------------------------------------------
@pytest.mark.anyio
async def test_increasing_limit_preserves_consumed(db):
    _seed_owner(db)
    _beta_member_row(db, "t@example.test", "u1", "ACTIVE",
                      quota_mode="LIMITED", analysis_limit=5, analysis_consumed=4)
    mid = db.beta_program_memberships.items[0]["membership_id"]
    resp = await fk.client_request(
        "PATCH", f"/api/admin/beta-program/testers/{mid}/quota", token="sess_owner",
        json={"quota_mode": "LIMITED", "analysis_limit": 8},
    )
    assert resp.status_code == 200
    body = resp.json()["tester"]
    assert body["analysis_consumed"] == 4
    assert body["quota"]["remaining"] == 4
    assert body["quota"]["state"] == "AVAILABLE"


@pytest.mark.anyio
async def test_lowering_below_consumed_sets_exhausted_no_negative_no_retroactive_charge(db):
    _seed_owner(db)
    _beta_member_row(db, "t@example.test", "u1", "ACTIVE",
                      quota_mode="LIMITED", analysis_limit=5, analysis_consumed=4)
    mid = db.beta_program_memberships.items[0]["membership_id"]
    resp = await fk.client_request(
        "PATCH", f"/api/admin/beta-program/testers/{mid}/quota", token="sess_owner",
        json={"quota_mode": "LIMITED", "analysis_limit": 2},
    )
    assert resp.status_code == 200
    body = resp.json()["tester"]
    assert body["analysis_consumed"] == 4  # untouched, no retroactive charge
    assert body["quota"]["state"] == "EXHAUSTED"
    assert body["quota"]["remaining"] == 0  # never negative
    assert any(a["action"] == beta_quota.ACTION_QUOTA_EXHAUSTED for a in db.beta_program_audit.items)


@pytest.mark.anyio
async def test_increasing_restores_availability_and_audits_available_again(db):
    _seed_owner(db)
    _beta_member_row(db, "t@example.test", "u1", "ACTIVE",
                      quota_mode="LIMITED", analysis_limit=2, analysis_consumed=2)
    mid = db.beta_program_memberships.items[0]["membership_id"]
    resp = await fk.client_request(
        "PATCH", f"/api/admin/beta-program/testers/{mid}/quota", token="sess_owner",
        json={"quota_mode": "LIMITED", "analysis_limit": 10},
    )
    assert resp.status_code == 200
    assert resp.json()["tester"]["quota"]["state"] == "AVAILABLE"
    assert any(a["action"] == beta_quota.ACTION_QUOTA_AVAILABLE_AGAIN for a in db.beta_program_audit.items)


# --- phases -------------------------------------------------------------------
@pytest.mark.anyio
async def test_new_phase_creates_new_quota_version_and_resets_counters(db):
    _seed_owner(db)
    _beta_member_row(db, "t@example.test", "u1", "ACTIVE",
                      quota_mode="LIMITED", analysis_limit=5, analysis_consumed=5)
    mid = db.beta_program_memberships.items[0]["membership_id"]
    resp = await fk.client_request(
        "POST", f"/api/admin/beta-program/testers/{mid}/quota/new-phase", token="sess_owner",
        json={"confirm": True},
    )
    assert resp.status_code == 200
    body = resp.json()["tester"]
    assert body["quota_version"] == 2
    assert body["analysis_consumed"] == 0
    assert body["analysis_reserved"] == 0
    assert any(a["action"] == beta_quota.ACTION_QUOTA_PHASE_STARTED for a in db.beta_program_audit.items)


@pytest.mark.anyio
async def test_new_phase_requires_explicit_confirmation(db):
    _seed_owner(db)
    _beta_member_row(db, "t@example.test", "u1", "ACTIVE", quota_mode="LIMITED", analysis_limit=5)
    mid = db.beta_program_memberships.items[0]["membership_id"]
    resp = await fk.client_request(
        "POST", f"/api/admin/beta-program/testers/{mid}/quota/new-phase", token="sess_owner",
        json={"confirm": False},
    )
    assert resp.status_code == 422


@pytest.mark.anyio
async def test_previous_phase_preserved_in_usage_ledger_and_phases_view(db):
    _seed_owner(db)
    _beta_member_row(db, "t@example.test", "u1", "ACTIVE", quota_mode="LIMITED", analysis_limit=5, analysis_consumed=3)
    mid = db.beta_program_memberships.items[0]["membership_id"]
    db.beta_program_usage.items.append({
        "usage_id": "u1", "membership_id": mid, "user_id": "u1", "normalized_email": "t@example.test",
        "analysis_id": "a1", "quota_version": 1, "state": "CONSUMED", "reserved_at": fk.now_iso(),
        "consumed_at": fk.now_iso(), "released_at": None, "release_reason": None,
        "paid_processing_started_at": fk.now_iso(), "created_at": fk.now_iso(), "updated_at": fk.now_iso(),
    })
    await fk.client_request(
        "POST", f"/api/admin/beta-program/testers/{mid}/quota/new-phase", token="sess_owner",
        json={"confirm": True},
    )
    resp = await fk.client_request(
        "GET", f"/api/admin/beta-program/testers/{mid}/quota/phases", token="sess_owner"
    )
    assert resp.status_code == 200
    items = resp.json()["items"]
    assert len(items) == 2
    old_phase = items[0]
    assert old_phase["quota_version"] == 1
    assert old_phase["consumed"] == 3
    assert old_phase["ended_at"] is not None
    new_phase = items[1]
    assert new_phase["quota_version"] == 2
    assert new_phase["ended_at"] is None
    # Version-1 usage row is preserved verbatim in the ledger (history, never deleted).
    assert db.beta_program_usage.items[0]["quota_version"] == 1


@pytest.mark.anyio
async def test_revocation_preserves_usage_and_history(db):
    _seed_owner(db)
    _beta_member_row(db, "t@example.test", "u1", "ACTIVE", quota_mode="LIMITED", analysis_limit=5, analysis_consumed=3)
    mid = db.beta_program_memberships.items[0]["membership_id"]
    db.beta_program_usage.items.append({
        "usage_id": "u1", "membership_id": mid, "user_id": "u1", "normalized_email": "t@example.test",
        "analysis_id": "a1", "quota_version": 1, "state": "CONSUMED", "reserved_at": fk.now_iso(),
        "consumed_at": fk.now_iso(), "released_at": None, "release_reason": None,
        "paid_processing_started_at": fk.now_iso(), "created_at": fk.now_iso(), "updated_at": fk.now_iso(),
    })
    await beta_store.revoke(membership_id=mid, actor_email=fk.OWNER_EMAIL, actor_user_id="owner")
    membership = db.beta_program_memberships.items[0]
    assert membership["status"] == "REVOKED"
    assert membership["analysis_consumed"] == 3  # revocation never resets usage
    assert len(db.beta_program_usage.items) == 1  # history preserved


@pytest.mark.anyio
async def test_reactivation_does_not_reset_usage(db):
    _seed_owner(db)
    _beta_member_row(db, "t@example.test", "u1", "REVOKED", quota_mode="LIMITED", analysis_limit=5, analysis_consumed=3)
    mid = db.beta_program_memberships.items[0]["membership_id"]
    await beta_store.reactivate(membership_id=mid, actor_email=fk.OWNER_EMAIL, actor_user_id="owner")
    membership = db.beta_program_memberships.items[0]
    assert membership["status"] == "ACTIVE"
    assert membership["analysis_consumed"] == 3  # reactivation preserves the current phase


# --- migration (quota defaults backfill) -------------------------------------
@pytest.mark.anyio
async def test_existing_memberships_default_to_unlimited_on_migration(db):
    now = fk.now_iso()
    # A pre-feature membership document (no quota_* fields at all).
    db.beta_program_memberships.items.append({
        "membership_id": "betam_old", "normalized_email": "old@example.test", "user_id": "uo",
        "display_name": None, "partner_type": "geometra", "status": "ACTIVE",
        "added_by": fk.OWNER_EMAIL, "added_at": now, "activated_at": now, "revoked_at": None,
        "reactivated_at": None, "updated_at": now, "internal_note": None, "entitlement_version": 1,
        "last_entitlement_change_at": now, "migration_source": None,
    })
    report = await beta_migrate.apply_quota_defaults(dry_run=False)
    assert report["defaulted"] == ["old@example.test"]
    doc = db.beta_program_memberships.items[0]
    assert doc["quota_mode"] == "UNLIMITED"
    assert doc["analysis_limit"] is None
    assert doc["quota_version"] == 1
    assert doc["analysis_consumed"] == 0
    assert doc["analysis_reserved"] == 0


@pytest.mark.anyio
async def test_quota_defaults_migration_second_apply_is_total_noop(db):
    now = fk.now_iso()
    db.beta_program_memberships.items.append({
        "membership_id": "betam_old", "normalized_email": "old@example.test", "user_id": "uo",
        "display_name": None, "partner_type": "geometra", "status": "ACTIVE",
        "added_by": fk.OWNER_EMAIL, "added_at": now, "activated_at": now, "revoked_at": None,
        "reactivated_at": None, "updated_at": now, "internal_note": None, "entitlement_version": 1,
        "last_entitlement_change_at": now, "migration_source": None,
    })
    r1 = await beta_migrate.apply_quota_defaults(dry_run=False)
    assert r1["defaulted"] == ["old@example.test"]
    r2 = await beta_migrate.apply_quota_defaults(dry_run=False)
    assert r2["defaulted"] == []
    assert r2["skipped_existing_quota"] == ["old@example.test"]


@pytest.mark.anyio
async def test_full_migration_run_includes_quota_defaults_report(db, monkeypatch):
    monkeypatch.setattr(server, "BETA_UNLIMITED_EMAILS", frozenset())
    report = await beta_migrate.run_migration(dry_run=False)
    assert "quota_defaults" in report
    assert report["quota_defaults"]["dry_run"] is False
