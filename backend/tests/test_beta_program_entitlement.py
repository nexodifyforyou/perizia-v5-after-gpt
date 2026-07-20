"""
Beta program — entitlement resolution, credit behaviour, active-session
revocation. (Maps to plan §V groups 1-2 + revocation.)
"""

import pytest

import beta_program_fakes as fk  # sets sys.path so beta_program/server import
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


def _beta_membership(db, email, user_id, status="ACTIVE"):
    now = fk.now_iso()
    db.beta_program_memberships.items.append({
        "membership_id": f"betam_{user_id}", "normalized_email": email.lower(),
        "user_id": user_id if status != "PENDING" else None,
        "display_name": "Geom. Beta", "partner_type": "geometra", "status": status,
        "added_by": fk.OWNER_EMAIL, "added_at": now,
        "activated_at": now if status == "ACTIVE" else None,
        "revoked_at": now if status == "REVOKED" else None, "reactivated_at": None,
        "updated_at": now, "internal_note": None, "entitlement_version": 1,
        "last_entitlement_change_at": now, "migration_source": None,
    })


# --- resolver ---------------------------------------------------------------
@pytest.mark.anyio
async def test_active_membership_resolves_active(db):
    _beta_membership(db, "t@example.test", "u1", "ACTIVE")
    snap = await beta_store.resolve_snapshot("t@example.test")
    assert snap["active"] is True
    assert snap["display_name"] == "Geom. Beta"


@pytest.mark.anyio
async def test_pending_membership_resolves_inactive(db):
    _beta_membership(db, "t@example.test", "u1", "PENDING")
    assert await beta_store.resolve_snapshot("t@example.test") == {}


@pytest.mark.anyio
async def test_revoked_membership_resolves_inactive(db):
    _beta_membership(db, "t@example.test", "u1", "REVOKED")
    assert await beta_store.resolve_snapshot("t@example.test") == {}


@pytest.mark.anyio
async def test_no_membership_resolves_inactive(db):
    assert await beta_store.resolve_snapshot("nobody@example.test") == {}


@pytest.mark.anyio
async def test_normalization_matches_case_and_whitespace(db):
    _beta_membership(db, "t@example.test", "u1", "ACTIVE")
    assert (await beta_store.resolve_snapshot("  T@Example.TEST "))["active"] is True


@pytest.mark.anyio
async def test_admin_email_never_treated_as_beta(db):
    # Admin resolves through get_current_user which hard-skips beta for admins.
    _beta_membership(db, fk.OWNER_EMAIL, "user_owner", "ACTIVE")
    fk.seed_session(db, fk.owner_user(), "sess_owner")
    resp = await fk.client_request("GET", "/api/auth/me", token="sess_owner")
    assert resp.status_code == 200
    body = resp.json()
    assert body["is_master_admin"] is True
    assert body["is_beta_partner"] is False


def test_user_has_active_beta_from_snapshot():
    u = server.User(user_id="u", email="t@example.test", name="T",
                    beta_program={"active": True, "membership_id": "m"})
    assert server._user_has_active_beta(u) is True
    u2 = server.User(user_id="u", email="t@example.test", name="T", beta_program={})
    assert server._user_has_active_beta(u2) is False


def test_credit_exempt_reflects_snapshot():
    beta = server.User(user_id="u", email="t@example.test", name="T",
                       beta_program={"active": True})
    normal = server.User(user_id="u2", email="n@example.test", name="N")
    assert server._is_credit_exempt_user(beta) is True
    assert server._is_credit_exempt_user(normal) is False


def test_user_model_beta_program_defaults_empty():
    assert server.User(user_id="u", email="e@e.t", name="N").beta_program == {}


@pytest.mark.anyio
async def test_env_allowlist_populated_grants_nothing(db, monkeypatch):
    """A populated BETA_UNLIMITED_EMAILS grants no runtime beta without a DB row."""
    monkeypatch.setattr(server, "BETA_UNLIMITED_EMAILS", frozenset({"t@example.test"}))
    assert await beta_store.resolve_snapshot("t@example.test") == {}
    fk.seed_session(db, fk.normal_user(email="t@example.test", user_id="u1", plan="free"), "s1")
    resp = await fk.client_request("GET", "/api/auth/me", token="s1")
    assert resp.json()["is_beta_partner"] is False


@pytest.mark.anyio
async def test_snapshot_not_persisted_to_users(db):
    _beta_membership(db, "t@example.test", "u1", "ACTIVE")
    fk.seed_session(db, fk.normal_user(email="t@example.test", user_id="u1", plan="free"), "s1")
    await fk.client_request("GET", "/api/auth/me", token="s1")
    stored = db.users.items[0]
    assert "beta_program" not in stored or not stored.get("beta_program")


# --- credit behaviour -------------------------------------------------------
@pytest.mark.anyio
async def test_active_beta_upload_allowed_at_zero_credits(db):
    _beta_membership(db, "t@example.test", "u1", "ACTIVE")
    user_doc = fk.normal_user(email="t@example.test", user_id="u1", plan="free")
    user_doc["quota"]["perizia_scans_remaining"] = 0
    fk.seed_session(db, user_doc, "s1")
    # Resolve entitlement the same way get_current_user does.
    user_doc["beta_program"] = await beta_store.resolve_snapshot("t@example.test")
    user = server.User(**user_doc)
    assert server._is_credit_exempt_user(user) is True


@pytest.mark.anyio
async def test_active_beta_debit_skipped_no_ledger(db):
    _beta_membership(db, "t@example.test", "u1", "ACTIVE")
    user = server.User(user_id="u1", email="t@example.test", name="T",
                       beta_program={"active": True, "membership_id": "betam_u1"})
    result = await server._apply_perizia_credit_debit_with_ledger(
        user, amount=4, entry_type="perizia_upload", reference_type="analysis",
        reference_id="analysis_x", description_it="t")
    assert result is False
    assert len(db.credit_ledger.items) == 0


@pytest.mark.anyio
async def test_no_9999_placeholder_written(db):
    _beta_membership(db, "t@example.test", "u1", "ACTIVE")
    user_doc = fk.normal_user(email="t@example.test", user_id="u1", plan="free")
    fk.seed_session(db, user_doc, "s1")
    await fk.client_request("GET", "/api/auth/me", token="s1")
    stored = db.users.items[0]
    quota = stored.get("quota", {})
    assert quota.get("perizia_scans_remaining") != 9999
    assert stored.get("perizia_credits", {}).get("total_available") != 9999


def test_entitlement_context_resolution():
    admin = server.User(user_id="a", email=fk.OWNER_EMAIL, name="A", is_master_admin=True)
    assert server._resolve_entitlement_context(admin) == "OWNER"
    beta = server.User(user_id="b", email="b@e.t", name="B", beta_program={"active": True})
    assert server._resolve_entitlement_context(beta) == "BETA"
    paid = server.User(user_id="p", email="p@e.t", name="P", plan="pro")
    assert server._resolve_entitlement_context(paid) == "PAID"
    free = server.User(user_id="f", email="f@e.t", name="F", plan="free")
    assert server._resolve_entitlement_context(free) == "FREE"


# --- revocation semantics ---------------------------------------------------
@pytest.mark.anyio
async def test_revocation_removes_exemption_next_request(db):
    _beta_membership(db, "t@example.test", "u1", "ACTIVE")
    user_doc = fk.normal_user(email="t@example.test", user_id="u1", plan="free")
    user_doc["quota"]["perizia_scans_remaining"] = 0
    fk.seed_session(db, user_doc, "s1")

    r1 = await fk.client_request("GET", "/api/auth/me", token="s1")
    assert r1.json()["is_beta_partner"] is True

    # Revoke directly on the membership (as the owner API would).
    membership = db.beta_program_memberships.items[0]
    await beta_store.revoke(membership_id=membership["membership_id"],
                            actor_email=fk.OWNER_EMAIL, actor_user_id="user_owner")

    # SAME session token, next request: beta gone, account still authenticates,
    # purchased balance (0 here) untouched, no restart.
    r2 = await fk.client_request("GET", "/api/auth/me", token="s1")
    assert r2.status_code == 200
    assert r2.json()["is_beta_partner"] is False


@pytest.mark.anyio
async def test_purchased_credits_survive_activate_and_revoke(db):
    user_doc = fk.normal_user(email="t@example.test", user_id="u1", plan="free")
    user_doc["quota"]["perizia_scans_remaining"] = 0
    user_doc["perizia_credits"] = {
        "monthly_remaining": 0,
        "pack_grants": [{"amount_granted": 8, "amount_remaining": 8, "source": "stripe_pack"}],
        "total_available": 8,
    }
    fk.seed_session(db, user_doc, "s1")
    import copy

    # Baseline wallet after the first normalization (before any beta interaction).
    await fk.client_request("GET", "/api/auth/me", token="s1")
    baseline_wallet = copy.deepcopy(db.users.items[0].get("perizia_credits"))

    # Activating beta must write NOTHING to the user's wallet.
    _beta_membership(db, "t@example.test", "u1", "ACTIVE")
    await fk.client_request("GET", "/api/auth/me", token="s1")
    assert db.users.items[0].get("perizia_credits") == baseline_wallet

    # Revoking beta must also write nothing to the wallet (no claw-back).
    membership = db.beta_program_memberships.items[0]
    await beta_store.revoke(membership_id=membership["membership_id"],
                            actor_email=fk.OWNER_EMAIL, actor_user_id="user_owner")
    await fk.client_request("GET", "/api/auth/me", token="s1")
    assert db.users.items[0].get("perizia_credits") == baseline_wallet
