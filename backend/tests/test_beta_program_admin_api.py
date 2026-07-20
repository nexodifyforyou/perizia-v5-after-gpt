"""
Beta program — owner-only admin API + activation/audit lifecycle.
(Maps to plan §V groups 3-4 and the authorization matrix.)
"""

import pytest

import beta_program_fakes as fk  # sets sys.path
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


def _beta_member_row(db, email, user_id, status="ACTIVE"):
    now = fk.now_iso()
    db.beta_program_memberships.items.append({
        "membership_id": f"betam_{user_id}", "normalized_email": email.lower(),
        "user_id": user_id if status != "PENDING" else None, "display_name": "Beta",
        "partner_type": "geometra", "status": status, "added_by": fk.OWNER_EMAIL,
        "added_at": now, "activated_at": now if status == "ACTIVE" else None,
        "revoked_at": now if status == "REVOKED" else None, "reactivated_at": None,
        "updated_at": now, "internal_note": None, "entitlement_version": 1,
        "last_entitlement_change_at": now, "migration_source": None,
    })


# --- authorization ----------------------------------------------------------
@pytest.mark.anyio
async def test_owner_can_list_testers(db):
    _seed_owner(db)
    resp = await fk.client_request("GET", "/api/admin/beta-program/testers", token="sess_owner")
    assert resp.status_code == 200
    assert "items" in resp.json()


@pytest.mark.anyio
async def test_unauthenticated_denied(db):
    resp = await fk.client_request("GET", "/api/admin/beta-program/testers")
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_normal_customer_denied(db):
    fk.seed_session(db, fk.normal_user(), "sess_norm")
    resp = await fk.client_request("GET", "/api/admin/beta-program/testers", token="sess_norm")
    assert resp.status_code == 403


@pytest.mark.anyio
async def test_active_tester_denied(db):
    _beta_member_row(db, "t@example.test", "u1", "ACTIVE")
    fk.seed_session(db, fk.normal_user(email="t@example.test", user_id="u1", plan="free"), "sess_t")
    resp = await fk.client_request("GET", "/api/admin/beta-program/testers", token="sess_t")
    assert resp.status_code == 403
    resp2 = await fk.client_request("POST", "/api/admin/beta-program/testers",
                                    token="sess_t", json={"email": "x@example.test"})
    assert resp2.status_code == 403


@pytest.mark.anyio
async def test_non_owner_admin_denied(db, monkeypatch):
    # A master-admin who is NOT the exact owner must be denied the beta program.
    monkeypatch.setattr(server, "ADMIN_EMAILS", frozenset({fk.OWNER_EMAIL, "other-admin@nexodify.com"}))
    server.CORRECTNESS_V2_ADMIN_VIEW_EMAIL = fk.OWNER_EMAIL
    fk.seed_session(db, {"user_id": "ua", "email": "other-admin@nexodify.com",
                         "name": "A", "plan": "free", "is_master_admin": False, "quota": {}}, "sess_a")
    resp = await fk.client_request("GET", "/api/admin/beta-program/testers", token="sess_a")
    assert resp.status_code == 403


# --- add / activation -------------------------------------------------------
@pytest.mark.anyio
async def test_add_pending_when_no_account(db):
    _seed_owner(db)
    resp = await fk.client_request("POST", "/api/admin/beta-program/testers",
                                   token="sess_owner", json={"email": "New.Tester@Example.TEST"})
    assert resp.status_code == 200
    tester = resp.json()["tester"]
    assert tester["status"] == "PENDING"
    assert tester["normalized_email"] == "new.tester@example.test"
    assert tester["account_linked"] is False
    # audit row created
    assert any(a["action"] == "MEMBER_ADDED" for a in db.beta_program_audit.items)


@pytest.mark.anyio
async def test_add_existing_user_active_immediately(db):
    _seed_owner(db)
    db.users.items.append(fk.normal_user(email="existing@example.test", user_id="u9", plan="free"))
    resp = await fk.client_request("POST", "/api/admin/beta-program/testers",
                                   token="sess_owner", json={"email": "existing@example.test"})
    assert resp.status_code == 200
    tester = resp.json()["tester"]
    assert tester["status"] == "ACTIVE"
    assert tester["account_linked"] is True
    # users doc untouched (no wallet/plan mutation).
    tester_doc = [u for u in db.users.items if u["email"] == "existing@example.test"][0]
    assert tester_doc["plan"] == "free"


@pytest.mark.anyio
async def test_add_admin_email_refused(db):
    _seed_owner(db)
    resp = await fk.client_request("POST", "/api/admin/beta-program/testers",
                                   token="sess_owner", json={"email": fk.OWNER_EMAIL})
    assert resp.status_code == 400
    assert resp.json()["detail"]["reason_code"] == "OWNER_CANNOT_BE_TESTER"


@pytest.mark.anyio
async def test_duplicate_active_email_409(db):
    _seed_owner(db)
    _beta_member_row(db, "dup@example.test", "u1", "ACTIVE")
    resp = await fk.client_request("POST", "/api/admin/beta-program/testers",
                                   token="sess_owner", json={"email": "dup@example.test"})
    assert resp.status_code == 409
    assert resp.json()["detail"]["reason_code"] == "MEMBERSHIP_EXISTS"


@pytest.mark.anyio
async def test_revoked_email_re_add_409_points_to_reactivate(db):
    _seed_owner(db)
    _beta_member_row(db, "rev@example.test", "u1", "REVOKED")
    resp = await fk.client_request("POST", "/api/admin/beta-program/testers",
                                   token="sess_owner", json={"email": "rev@example.test"})
    assert resp.status_code == 409
    assert resp.json()["detail"]["reason_code"] == "MEMBERSHIP_REVOKED"


@pytest.mark.anyio
async def test_invalid_email_422(db):
    _seed_owner(db)
    resp = await fk.client_request("POST", "/api/admin/beta-program/testers",
                                   token="sess_owner", json={"email": "not-an-email"})
    assert resp.status_code == 422


# --- revoke / reactivate ----------------------------------------------------
@pytest.mark.anyio
async def test_revoke_then_reactivate(db):
    _seed_owner(db)
    _beta_member_row(db, "t@example.test", "u1", "ACTIVE")
    mid = db.beta_program_memberships.items[0]["membership_id"]

    r = await fk.client_request("POST", f"/api/admin/beta-program/testers/{mid}/revoke", token="sess_owner")
    assert r.status_code == 200
    assert r.json()["tester"]["status"] == "REVOKED"

    # Revoking an already-revoked membership is an invalid transition.
    r2 = await fk.client_request("POST", f"/api/admin/beta-program/testers/{mid}/revoke", token="sess_owner")
    assert r2.status_code == 409

    ra = await fk.client_request("POST", f"/api/admin/beta-program/testers/{mid}/reactivate", token="sess_owner")
    assert ra.status_code == 200
    assert ra.json()["tester"]["status"] == "ACTIVE"
    # Same membership reused (no duplicate).
    assert len(db.beta_program_memberships.items) == 1


@pytest.mark.anyio
async def test_patch_restricted_fields_only(db):
    _seed_owner(db)
    _beta_member_row(db, "t@example.test", "u1", "ACTIVE")
    mid = db.beta_program_memberships.items[0]["membership_id"]
    resp = await fk.client_request("PATCH", f"/api/admin/beta-program/testers/{mid}",
                                   token="sess_owner",
                                   json={"display_name": "Nuovo Nome", "internal_note": "vip"})
    assert resp.status_code == 200
    assert resp.json()["tester"]["display_name"] == "Nuovo Nome"
    # Status is never patchable here.
    assert db.beta_program_memberships.items[0]["status"] == "ACTIVE"


# --- login-time activation --------------------------------------------------
@pytest.mark.anyio
async def test_login_activates_pending_not_revoked(db):
    # PENDING -> activated on login; REVOKED -> untouched.
    _beta_member_row(db, "pending@example.test", "u_p", "PENDING")
    _beta_member_row(db, "revoked@example.test", "u_r", "REVOKED")

    m = await beta_store.link_pending_membership("pending@example.test", "u_p")
    assert m["status"] == "ACTIVE"
    assert m["user_id"] == "u_p"

    r = await beta_store.link_pending_membership("revoked@example.test", "u_r")
    assert r is None  # REVOKED never reactivated by login
    revoked = [x for x in db.beta_program_memberships.items if x["normalized_email"] == "revoked@example.test"][0]
    assert revoked["status"] == "REVOKED"


@pytest.mark.anyio
async def test_pending_not_claimed_by_different_email(db):
    _beta_member_row(db, "pending@example.test", None, "PENDING")
    r = await beta_store.link_pending_membership("other@example.test", "u_other")
    assert r is None
    pending = db.beta_program_memberships.items[0]
    assert pending["status"] == "PENDING"


@pytest.mark.anyio
async def test_every_transition_writes_audit(db):
    _seed_owner(db)
    await fk.client_request("POST", "/api/admin/beta-program/testers",
                            token="sess_owner", json={"email": "a@example.test"})
    mid = db.beta_program_memberships.items[0]["membership_id"]
    await fk.client_request("POST", f"/api/admin/beta-program/testers/{mid}/revoke", token="sess_owner")
    await fk.client_request("POST", f"/api/admin/beta-program/testers/{mid}/reactivate", token="sess_owner")
    actions = [a["action"] for a in db.beta_program_audit.items]
    assert "MEMBER_ADDED" in actions
    assert "MEMBER_REVOKED" in actions
    assert "MEMBER_REACTIVATED" in actions
    # entitlement_version is monotonic across transitions.
    versions = [a["entitlement_version"] for a in db.beta_program_audit.items if a["membership_id"] == mid]
    assert versions == sorted(versions)


@pytest.mark.anyio
async def test_internal_note_not_exposed_to_tester(db):
    # Note is stored, visible to owner list, but never on the customer /auth/me.
    _seed_owner(db)
    await fk.client_request("POST", "/api/admin/beta-program/testers",
                            token="sess_owner", json={"email": "t@example.test", "internal_note": "secret"})
    listing = await fk.client_request("GET", "/api/admin/beta-program/testers", token="sess_owner")
    assert listing.json()["items"][0]["internal_note"] == "secret"

    # Tester side: /auth/me must never carry internal_note.
    db.users.items.append(fk.normal_user(email="t@example.test", user_id="u1", plan="free"))
    # link + activate as a login would
    await beta_store.link_pending_membership("t@example.test", "u1")
    fk.seed_session(db, db.users.items[-1], "sess_t")
    me = await fk.client_request("GET", "/api/auth/me", token="sess_t")
    assert "internal_note" not in str(me.json().get("beta_program", {}))


@pytest.mark.anyio
async def test_pagination_and_search(db):
    _seed_owner(db)
    for i in range(5):
        _beta_member_row(db, f"user{i}@example.test", f"u{i}", "ACTIVE")
    _beta_member_row(db, "special@example.test", "us", "ACTIVE")
    r = await fk.client_request("GET", "/api/admin/beta-program/testers?q=special", token="sess_owner")
    assert r.json()["total"] == 1
    r2 = await fk.client_request("GET", "/api/admin/beta-program/testers?page=1&page_size=2", token="sess_owner")
    assert len(r2.json()["items"]) == 2
    assert r2.json()["total"] == 6
