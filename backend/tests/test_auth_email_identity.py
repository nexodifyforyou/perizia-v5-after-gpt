"""Account identity, provider linking, beta membership and owner authorization.

Scenarios A-E from the mission brief. Real Mongo, fake sender, no network.
No tester-specific logic exists or is tested: every address here is synthetic.
"""

import os
import sys

import pytest
from fastapi import Response

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import server  # noqa: E402
from auth_email import identity  # noqa: E402
from beta_program import store as beta_store  # noqa: E402
from tests.auth_email_helpers import (  # noqa: E402
    CORP_EMAIL,
    OWNER_EMAIL,
    api_request,
    apply_test_env,
    clear_sender,
    install_sender,
    request_and_capture,
    reset_auth_email_state,
    verify_code,
)


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    apply_test_env(monkeypatch)
    yield
    clear_sender()


async def _google_login(email, name="Google User"):
    """Drive the shared login chokepoint exactly as the Google callback does."""
    return await server._create_local_login(
        email=email,
        name=name,
        picture="https://example.invalid/avatar.png",
        response=Response(),
        auth_method=identity.METHOD_GOOGLE,
        email_verified=True,
    )


async def _otp_login(monkeypatch, email):
    fake = install_sender(monkeypatch)
    _, challenge_id, code = await request_and_capture(fake, email)
    response = await verify_code(challenge_id, code)
    assert response.status_code == 200, response.text
    return response


async def _seed_membership(email, status, **overrides):
    doc = {
        "membership_id": f"betam_{status.lower()}",
        "normalized_email": email,
        "status": status,
        "quota_mode": "LIMITED",
        "analysis_limit": 5,
        "analysis_consumed": 0,
        "analysis_reserved": 0,
        "quota_version": 1,
        "entitlement_version": 1,
        "partner_type": "geometra",
    }
    doc.update(overrides)
    await server.db["beta_program_memberships"].insert_one(doc)
    return doc


# ---------------------------------------------------------------------------
# Scenario A — new corporate-email user
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_new_corporate_user_created_once(monkeypatch):
    await reset_auth_email_state()
    await _otp_login(monkeypatch, CORP_EMAIL)

    users = await server.db["users"].find({}, {"_id": 0}).to_list(10)
    assert len(users) == 1
    assert users[0]["normalized_email"] == CORP_EMAIL
    assert users[0]["email_verified"] is True
    assert identity.METHOD_EMAIL_OTP in users[0]["auth_methods"]
    assert users[0]["last_login_method"] == identity.METHOD_EMAIL_OTP


@pytest.mark.anyio
async def test_repeated_otp_logins_reuse_the_same_account(monkeypatch):
    await reset_auth_email_state()
    monkeypatch.setenv("AUTH_EMAIL_RESEND_COOLDOWN_SECONDS", "0")

    first = await _otp_login(monkeypatch, CORP_EMAIL)
    second = await _otp_login(monkeypatch, CORP_EMAIL)

    assert first.json()["user"]["user_id"] == second.json()["user"]["user_id"]
    assert await server.db["users"].count_documents({}) == 1


# ---------------------------------------------------------------------------
# Scenario C — existing Google user logs in with OTP
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_existing_google_user_is_reused_not_duplicated(monkeypatch):
    await reset_auth_email_state()
    google_user, _ = await _google_login(CORP_EMAIL)

    response = await _otp_login(monkeypatch, CORP_EMAIL)

    assert response.json()["user"]["user_id"] == google_user.user_id
    assert await server.db["users"].count_documents({}) == 1

    stored = await server.db["users"].find_one({"user_id": google_user.user_id}, {"_id": 0})
    assert set(stored["auth_methods"]) == {identity.METHOD_GOOGLE, identity.METHOD_EMAIL_OTP}


@pytest.mark.anyio
async def test_otp_login_preserves_google_profile(monkeypatch):
    """An OTP login supplies no name or picture; it must not blank them."""
    await reset_auth_email_state()
    await _google_login(CORP_EMAIL, name="Mario Rossi")

    await _otp_login(monkeypatch, CORP_EMAIL)

    stored = await server.db["users"].find_one({"normalized_email": CORP_EMAIL}, {"_id": 0})
    assert stored["name"] == "Mario Rossi"
    assert stored["picture"] == "https://example.invalid/avatar.png"


@pytest.mark.anyio
async def test_google_links_to_an_otp_created_user(monkeypatch):
    """Scenario B: same verified email, so the same account gains a method."""
    await reset_auth_email_state()
    otp_response = await _otp_login(monkeypatch, CORP_EMAIL)
    otp_user_id = otp_response.json()["user"]["user_id"]

    google_user, _ = await _google_login(CORP_EMAIL)

    assert google_user.user_id == otp_user_id
    assert await server.db["users"].count_documents({}) == 1
    stored = await server.db["users"].find_one({"user_id": otp_user_id}, {"_id": 0})
    assert set(stored["auth_methods"]) == {identity.METHOD_EMAIL_OTP, identity.METHOD_GOOGLE}


@pytest.mark.anyio
async def test_unverified_provider_email_does_not_mark_verified(monkeypatch):
    """The legacy provider asserts nothing, so it cannot confer verification."""
    await reset_auth_email_state()
    await server._create_local_login(
        email=CORP_EMAIL,
        name="Legacy",
        picture=None,
        response=Response(),
        auth_method=identity.METHOD_LEGACY,
    )

    stored = await server.db["users"].find_one({"normalized_email": CORP_EMAIL}, {"_id": 0})
    assert stored["email_verified"] is False
    assert stored["auth_methods"] == [identity.METHOD_LEGACY]


@pytest.mark.anyio
async def test_distinct_addresses_never_merge(monkeypatch):
    await reset_auth_email_state()
    monkeypatch.setenv("AUTH_EMAIL_RESEND_COOLDOWN_SECONDS", "0")

    await _otp_login(monkeypatch, "name@example-corp.it")
    await _otp_login(monkeypatch, "name+beta@example-corp.it")

    assert await server.db["users"].count_documents({}) == 2


# ---------------------------------------------------------------------------
# Value preservation
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_credits_reports_and_subscription_survive_otp_login(monkeypatch):
    await reset_auth_email_state()
    google_user, _ = await _google_login(CORP_EMAIL)

    await server.db["users"].update_one(
        {"user_id": google_user.user_id},
        {
            "$set": {
                # A structurally valid wallet: an incomplete one is legitimately
                # rebuilt by the existing normalization on ANY login, Google
                # included, so seeding a partial dict would test nothing.
                "perizia_credits": {
                    "monthly_remaining": 0,
                    "extra_remaining": 7,
                    "monthly_plan_id": None,
                    "pack_grants": [],
                    "processed_invoice_ids": [],
                    "total_available": 7,
                },
                "subscription_state": {"status": "active", "plan": "pro"},
                "plan": "pro",
            }
        },
    )
    await server.db["perizia_analyses"].insert_one(
        {"analysis_id": "an_1", "user_id": google_user.user_id, "status": "COMPLETED"}
    )
    await server.db["beta_feedback"].insert_one(
        {"user_id": google_user.user_id, "feedback_type": "POSITIVE"}
    )

    await _otp_login(monkeypatch, CORP_EMAIL)

    stored = await server.db["users"].find_one({"user_id": google_user.user_id}, {"_id": 0})
    assert stored["perizia_credits"]["total_available"] == 7
    assert stored["subscription_state"]["status"] == "active"
    assert stored["plan"] == "pro"
    assert await server.db["perizia_analyses"].count_documents({"user_id": google_user.user_id}) == 1
    assert await server.db["beta_feedback"].count_documents({"user_id": google_user.user_id}) == 1


@pytest.mark.anyio
async def test_user_id_is_stable_across_methods(monkeypatch):
    await reset_auth_email_state()
    monkeypatch.setenv("AUTH_EMAIL_RESEND_COOLDOWN_SECONDS", "0")

    google_user, _ = await _google_login(CORP_EMAIL)
    otp = await _otp_login(monkeypatch, CORP_EMAIL)
    google_again, _ = await _google_login(CORP_EMAIL)

    assert google_user.user_id == otp.json()["user"]["user_id"] == google_again.user_id


# ---------------------------------------------------------------------------
# Beta membership scenarios C/D/E
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_pending_membership_activates_on_otp_login(monkeypatch):
    await reset_auth_email_state()
    await _seed_membership(CORP_EMAIL, beta_store.STATUS_PENDING)

    response = await _otp_login(monkeypatch, CORP_EMAIL)
    user_id = response.json()["user"]["user_id"]

    membership = await server.db["beta_program_memberships"].find_one(
        {"normalized_email": CORP_EMAIL}, {"_id": 0}
    )
    assert membership["status"] == beta_store.STATUS_ACTIVE
    assert membership["user_id"] == user_id
    assert await server.db["beta_program_memberships"].count_documents({}) == 1


@pytest.mark.anyio
async def test_activation_preserves_configured_allowance(monkeypatch):
    await reset_auth_email_state()
    await _seed_membership(
        CORP_EMAIL, beta_store.STATUS_PENDING, analysis_limit=5, quota_mode="LIMITED", quota_version=3
    )

    await _otp_login(monkeypatch, CORP_EMAIL)

    membership = await server.db["beta_program_memberships"].find_one(
        {"normalized_email": CORP_EMAIL}, {"_id": 0}
    )
    assert membership["analysis_limit"] == 5
    assert membership["quota_mode"] == "LIMITED"
    assert membership["quota_version"] == 3
    assert membership["analysis_consumed"] == 0


@pytest.mark.anyio
async def test_activation_writes_a_beta_audit_event(monkeypatch):
    await reset_auth_email_state()
    await _seed_membership(CORP_EMAIL, beta_store.STATUS_PENDING)

    await _otp_login(monkeypatch, CORP_EMAIL)

    events = await server.db["beta_program_audit"].find({}, {"_id": 0}).to_list(10)
    actions = [e["action"] for e in events]
    assert beta_store.ACTION_ACTIVATED in actions


@pytest.mark.anyio
async def test_active_membership_and_consumed_quota_preserved(monkeypatch):
    """Scenario D: logging in must not reset usage."""
    await reset_auth_email_state()
    await _seed_membership(
        CORP_EMAIL,
        beta_store.STATUS_ACTIVE,
        analysis_limit=5,
        analysis_consumed=3,
        analysis_reserved=1,
    )

    await _otp_login(monkeypatch, CORP_EMAIL)

    membership = await server.db["beta_program_memberships"].find_one(
        {"normalized_email": CORP_EMAIL}, {"_id": 0}
    )
    assert membership["status"] == beta_store.STATUS_ACTIVE
    assert membership["analysis_consumed"] == 3
    assert membership["analysis_reserved"] == 1
    assert membership["analysis_limit"] == 5


@pytest.mark.anyio
async def test_revoked_membership_is_never_reactivated(monkeypatch):
    """Scenario E: verifying the email again must not restore beta access."""
    await reset_auth_email_state()
    await _seed_membership(CORP_EMAIL, beta_store.STATUS_REVOKED)

    response = await _otp_login(monkeypatch, CORP_EMAIL)
    assert response.status_code == 200  # login itself still succeeds

    membership = await server.db["beta_program_memberships"].find_one(
        {"normalized_email": CORP_EMAIL}, {"_id": 0}
    )
    assert membership["status"] == beta_store.STATUS_REVOKED

    token = response.json()["session_token"]
    me = await api_request("GET", "/api/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert not (me.json().get("beta_program") or {}).get("active")


@pytest.mark.anyio
async def test_no_duplicate_membership_created(monkeypatch):
    await reset_auth_email_state()
    monkeypatch.setenv("AUTH_EMAIL_RESEND_COOLDOWN_SECONDS", "0")
    await _seed_membership(CORP_EMAIL, beta_store.STATUS_PENDING)

    await _otp_login(monkeypatch, CORP_EMAIL)
    await _otp_login(monkeypatch, CORP_EMAIL)

    assert await server.db["beta_program_memberships"].count_documents({}) == 1


@pytest.mark.anyio
async def test_beta_tester_sees_allowance_but_no_admin_rights(monkeypatch):
    await reset_auth_email_state()
    await _seed_membership(CORP_EMAIL, beta_store.STATUS_PENDING, analysis_limit=5)

    response = await _otp_login(monkeypatch, CORP_EMAIL)
    token = response.json()["session_token"]
    me = await api_request("GET", "/api/auth/me", headers={"Authorization": f"Bearer {token}"})
    body = me.json()

    assert body["is_master_admin"] is False
    beta = body.get("beta_program") or {}
    assert beta.get("active") is True
    quota = beta.get("quota") or {}
    assert quota.get("limit") == 5
    assert quota.get("remaining") == 5


@pytest.mark.anyio
async def test_beta_tester_cannot_reach_owner_endpoints(monkeypatch):
    await reset_auth_email_state()
    await _seed_membership(CORP_EMAIL, beta_store.STATUS_PENDING)

    response = await _otp_login(monkeypatch, CORP_EMAIL)
    token = response.json()["session_token"]

    admin = await api_request(
        "GET", "/api/beta-program/members", headers={"Authorization": f"Bearer {token}"}
    )
    assert admin.status_code in (401, 403, 404)


# ---------------------------------------------------------------------------
# Owner authorization
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_owner_remains_owner_through_otp_login(monkeypatch):
    """Authorization follows the verified email, not the login method."""
    await reset_auth_email_state()

    response = await _otp_login(monkeypatch, OWNER_EMAIL)
    token = response.json()["session_token"]
    me = await api_request("GET", "/api/auth/me", headers={"Authorization": f"Bearer {token}"})

    assert me.json()["is_master_admin"] is True


@pytest.mark.anyio
async def test_owner_identity_is_shared_across_methods(monkeypatch):
    await reset_auth_email_state()
    google_owner, _ = await _google_login(OWNER_EMAIL, name="Owner")

    response = await _otp_login(monkeypatch, OWNER_EMAIL)

    assert response.json()["user"]["user_id"] == google_owner.user_id
    assert await server.db["users"].count_documents({}) == 1


@pytest.mark.anyio
async def test_normal_customer_does_not_become_owner(monkeypatch):
    await reset_auth_email_state()

    response = await _otp_login(monkeypatch, CORP_EMAIL)
    token = response.json()["session_token"]
    me = await api_request("GET", "/api/auth/me", headers={"Authorization": f"Bearer {token}"})

    assert me.json()["is_master_admin"] is False


@pytest.mark.anyio
async def test_owner_matching_is_exact_not_by_domain(monkeypatch):
    """A lookalike address must not inherit owner rights."""
    await reset_auth_email_state()
    monkeypatch.setenv("AUTH_EMAIL_RESEND_COOLDOWN_SECONDS", "0")

    for lookalike in (
        "nexodifyforyou@gmail.com.example-corp.it",
        "notnexodifyforyou@gmail.com",
        "nexodifyforyou+alias@gmail.com",
    ):
        response = await _otp_login(monkeypatch, lookalike)
        token = response.json()["session_token"]
        me = await api_request("GET", "/api/auth/me", headers={"Authorization": f"Bearer {token}"})
        assert me.json()["is_master_admin"] is False, lookalike


# ---------------------------------------------------------------------------
# Uniqueness guarantee
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_unique_index_exists_on_normalized_email():
    await reset_auth_email_state()
    assert await identity.unique_index_ready(server.db, use_cache=False) is True


@pytest.mark.anyio
async def test_index_is_on_normalized_email_not_raw_email():
    await reset_auth_email_state()
    info = await server.db["users"].index_information()

    spec = info[identity.NORMALIZED_EMAIL_INDEX]
    fields = [k[0] if isinstance(k, (list, tuple)) else k for k in spec["key"]]
    assert fields == ["normalized_email"]
    assert spec.get("unique") is True

    # Uniqueness must never be imposed on the historical `email` field, which
    # predates normalization and is not guaranteed canonical on old documents.
    email_indexes = [
        s for n, s in info.items()
        if any((k[0] if isinstance(k, (list, tuple)) else k) == "email" for k in s.get("key", []))
    ]
    assert all(not s.get("unique") for s in email_indexes)


@pytest.mark.anyio
async def test_duplicate_insert_is_refused_by_the_database():
    await reset_auth_email_state()
    from pymongo.errors import DuplicateKeyError

    await server.db["users"].insert_one({"user_id": "u1", "normalized_email": CORP_EMAIL})
    with pytest.raises(DuplicateKeyError):
        await server.db["users"].insert_one({"user_id": "u2", "normalized_email": CORP_EMAIL})


@pytest.mark.anyio
async def test_index_creation_refused_when_duplicates_exist():
    """Historical conflicts are reported, never merged."""
    await reset_auth_email_state()
    await server.db["users"].drop_index(identity.NORMALIZED_EMAIL_INDEX)
    identity.reset_index_cache()

    await server.db["users"].insert_one({"user_id": "u1", "normalized_email": CORP_EMAIL})
    await server.db["users"].insert_one({"user_id": "u2", "normalized_email": CORP_EMAIL})

    created = await identity.ensure_unique_index(server.db)
    assert created is False
    assert await identity.unique_index_ready(server.db, use_cache=False) is False
    # Nothing was deleted or merged.
    assert await server.db["users"].count_documents({"normalized_email": CORP_EMAIL}) == 2
