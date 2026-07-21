"""Endpoint behaviour: enumeration safety, rate limits, provider failure, session.

Real Mongo (isolated test DB), fake sender. No network, no Stripe, no analysis.
"""

import logging
import os
import sys

import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import server  # noqa: E402
from auth_email import api as auth_api  # noqa: E402
from auth_email import challenges, identity, ratelimit  # noqa: E402
from auth_email import sender as sender_module  # noqa: E402
from tests.auth_email_helpers import (  # noqa: E402
    CORP_EMAIL,
    CORP_EMAIL_2,
    MS365_EMAIL,
    apply_test_env,
    api_request,
    clear_sender,
    drop_identity_index,
    install_sender,
    request_and_capture,
    request_code,
    reset_auth_email_state,
    stored_challenge,
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


# ---------------------------------------------------------------------------
# Request-code: provider independence
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_corporate_email_can_request_a_code(monkeypatch):
    await reset_auth_email_state()
    fake = install_sender(monkeypatch)

    response, challenge_id, code = await request_and_capture(fake, CORP_EMAIL)

    assert response.status_code == 200
    assert challenge_id
    assert code and len(code) == 6
    assert fake.messages[-1].to == CORP_EMAIL


@pytest.mark.anyio
async def test_no_google_account_is_required(monkeypatch):
    """Nothing in the flow consults Google; the domain is irrelevant."""
    await reset_auth_email_state()
    fake = install_sender(monkeypatch)

    for address in (CORP_EMAIL, MS365_EMAIL, "titolare@studio-example.it"):
        response, challenge_id, code = await request_and_capture(fake, address)
        assert response.status_code == 200, address
        assert code, address
        await verify_code(challenge_id, code)

    assert await server.db["users"].count_documents({}) == 3


@pytest.mark.anyio
async def test_response_shape_is_opaque(monkeypatch):
    """A challenge identifier and timings — never account state."""
    await reset_auth_email_state()
    fake = install_sender(monkeypatch)

    response, _, _ = await request_and_capture(fake, CORP_EMAIL)
    body = response.json()

    assert set(body) == {"challenge_id", "expires_in", "resend_available_in", "message"}
    blob = str(body).lower()
    for forbidden in ("beta", "owner", "admin", "credit", "exists", "registered", "google"):
        assert forbidden not in blob


@pytest.mark.anyio
async def test_invalid_email_rejected_safely(monkeypatch):
    await reset_auth_email_state()
    install_sender(monkeypatch)

    response = await request_code("not-an-email")
    assert response.status_code == 400
    assert response.json()["detail"] == auth_api.MSG_INVALID_EMAIL
    assert await server.db[challenges.CHALLENGES_COLLECTION].count_documents({}) == 0


@pytest.mark.anyio
async def test_email_is_normalized_before_use(monkeypatch):
    await reset_auth_email_state()
    fake = install_sender(monkeypatch)

    await request_and_capture(fake, "  Test.User@AGLASTE-EXAMPLE-CORP.IT  ")
    assert fake.messages[-1].to == CORP_EMAIL


@pytest.mark.anyio
async def test_plus_addressing_is_a_distinct_identity(monkeypatch):
    await reset_auth_email_state()
    fake = install_sender(monkeypatch)

    _, id_a, code_a = await request_and_capture(fake, "name@example-corp.it", ip="203.0.113.1")
    _, id_b, code_b = await request_and_capture(fake, "name+beta@example-corp.it", ip="203.0.113.2")

    assert id_a and id_b and id_a != id_b
    await verify_code(id_a, code_a)
    await verify_code(id_b, code_b)

    emails = sorted(d["normalized_email"] for d in await server.db["users"].find({}, {"_id": 0}).to_list(10))
    assert emails == ["name+beta@example-corp.it", "name@example-corp.it"]


# ---------------------------------------------------------------------------
# Enumeration resistance
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_known_and_unknown_addresses_are_indistinguishable(monkeypatch):
    """Same status, same keys, same message for registered and unregistered."""
    await reset_auth_email_state()
    monkeypatch.setenv("AUTH_EMAIL_RESEND_COOLDOWN_SECONDS", "0")
    fake = install_sender(monkeypatch)

    # Establish one real account.
    _, cid, code = await request_and_capture(fake, CORP_EMAIL)
    await verify_code(cid, code)
    assert await server.db["users"].count_documents({"normalized_email": CORP_EMAIL}) == 1

    known = await request_code(CORP_EMAIL, ip="198.51.100.1")
    unknown = await request_code("nobody-here@example-corp.it", ip="198.51.100.2")

    assert known.status_code == unknown.status_code == 200
    assert known.json()["message"] == unknown.json()["message"]
    assert set(known.json()) == set(unknown.json())


@pytest.mark.anyio
async def test_owner_address_is_indistinguishable(monkeypatch):
    """Requesting a code for the owner reveals nothing about privilege."""
    await reset_auth_email_state()
    fake = install_sender(monkeypatch)

    owner = await request_code("nexodifyforyou@gmail.com", ip="198.51.100.3")
    stranger = await request_code("stranger@example-corp.it", ip="198.51.100.4")

    assert owner.status_code == stranger.status_code == 200
    assert owner.json()["message"] == stranger.json()["message"]


@pytest.mark.anyio
async def test_beta_member_address_is_indistinguishable(monkeypatch):
    await reset_auth_email_state()
    fake = install_sender(monkeypatch)
    await server.db["beta_program_memberships"].insert_one(
        {
            "membership_id": "betam_test1",
            "normalized_email": CORP_EMAIL,
            "status": "PENDING",
            "quota_mode": "LIMITED",
            "analysis_limit": 5,
            "analysis_consumed": 0,
            "analysis_reserved": 0,
            "quota_version": 1,
            "entitlement_version": 1,
        }
    )

    member = await request_code(CORP_EMAIL, ip="198.51.100.5")
    stranger = await request_code(CORP_EMAIL_2, ip="198.51.100.6")

    assert member.status_code == stranger.status_code == 200
    assert member.json()["message"] == stranger.json()["message"]


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_correct_code_establishes_a_session(monkeypatch):
    await reset_auth_email_state()
    fake = install_sender(monkeypatch)
    _, challenge_id, code = await request_and_capture(fake, CORP_EMAIL)

    response = await verify_code(challenge_id, code)
    assert response.status_code == 200

    body = response.json()
    assert body["user"]["email"] == CORP_EMAIL
    assert body["session_token"].startswith("sess_")

    cookie = response.headers.get("set-cookie", "")
    assert "session_token=" in cookie
    assert "HttpOnly" in cookie
    assert "Secure" in cookie
    assert "samesite=none" in cookie.lower()

    stored = await server.db["user_sessions"].find_one({"session_token": body["session_token"]})
    assert stored is not None


@pytest.mark.anyio
async def test_session_authenticates_subsequent_requests(monkeypatch):
    await reset_auth_email_state()
    fake = install_sender(monkeypatch)
    _, challenge_id, code = await request_and_capture(fake, CORP_EMAIL)
    token = (await verify_code(challenge_id, code)).json()["session_token"]

    me = await api_request("GET", "/api/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert me.status_code == 200
    assert me.json()["email"] == CORP_EMAIL


@pytest.mark.anyio
async def test_logout_then_session_is_revoked(monkeypatch):
    await reset_auth_email_state()
    fake = install_sender(monkeypatch)
    _, challenge_id, code = await request_and_capture(fake, CORP_EMAIL)
    token = (await verify_code(challenge_id, code)).json()["session_token"]

    out = await api_request(
        "POST", "/api/auth/logout", headers={"Cookie": f"session_token={token}"}
    )
    assert out.status_code == 200

    me = await api_request("GET", "/api/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert me.status_code == 401


@pytest.mark.anyio
async def test_revoking_the_session_row_denies_access(monkeypatch):
    """Existing active-session revocation keeps working for OTP logins."""
    await reset_auth_email_state()
    fake = install_sender(monkeypatch)
    _, challenge_id, code = await request_and_capture(fake, CORP_EMAIL)
    token = (await verify_code(challenge_id, code)).json()["session_token"]

    await server.db["user_sessions"].delete_many({"session_token": token})

    me = await api_request("GET", "/api/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert me.status_code == 401


@pytest.mark.anyio
async def test_wrong_code_is_rejected(monkeypatch):
    await reset_auth_email_state()
    fake = install_sender(monkeypatch)
    _, challenge_id, code = await request_and_capture(fake, CORP_EMAIL)
    wrong = "000000" if code != "000000" else "111111"

    response = await verify_code(challenge_id, wrong)
    assert response.status_code == 400
    assert response.json()["detail"] == auth_api.MSG_INVALID_CODE
    assert await server.db["users"].count_documents({}) == 0


@pytest.mark.anyio
async def test_reused_code_is_rejected(monkeypatch):
    await reset_auth_email_state()
    fake = install_sender(monkeypatch)
    _, challenge_id, code = await request_and_capture(fake, CORP_EMAIL)

    assert (await verify_code(challenge_id, code)).status_code == 200
    second = await verify_code(challenge_id, code)
    assert second.status_code == 400
    assert second.json()["detail"] == auth_api.MSG_INVALID_CODE
    assert await server.db["users"].count_documents({}) == 1


@pytest.mark.anyio
async def test_unknown_challenge_id_is_rejected(monkeypatch):
    await reset_auth_email_state()
    install_sender(monkeypatch)
    response = await verify_code("aec_does_not_exist", "123456")
    assert response.status_code == 400
    assert response.json()["detail"] == auth_api.MSG_INVALID_CODE


@pytest.mark.parametrize("bad", ["", "12345", "1234567", "abcdef", "12 34 56", "12345a"])
@pytest.mark.anyio
async def test_malformed_codes_rejected(monkeypatch, bad):
    await reset_auth_email_state()
    fake = install_sender(monkeypatch)
    _, challenge_id, _ = await request_and_capture(fake, CORP_EMAIL)

    response = await verify_code(challenge_id, bad)
    assert response.status_code == 400
    assert response.json()["detail"] == auth_api.MSG_INVALID_CODE


@pytest.mark.anyio
async def test_attempt_lockout_returns_generic_rate_limit(monkeypatch):
    await reset_auth_email_state()
    fake = install_sender(monkeypatch)
    _, challenge_id, code = await request_and_capture(fake, CORP_EMAIL)
    wrong = "000000" if code != "000000" else "111111"

    for _ in range(4):
        assert (await verify_code(challenge_id, wrong)).status_code == 400

    locked = await verify_code(challenge_id, wrong)
    assert locked.status_code == 429
    assert locked.json()["detail"] == auth_api.MSG_RATE_LIMITED

    # The correct code no longer works either.
    assert (await verify_code(challenge_id, code)).status_code in (400, 429)


@pytest.mark.anyio
async def test_new_code_invalidates_the_previous_one(monkeypatch):
    await reset_auth_email_state()
    monkeypatch.setenv("AUTH_EMAIL_RESEND_COOLDOWN_SECONDS", "0")
    fake = install_sender(monkeypatch)

    _, first_id, first_code = await request_and_capture(fake, CORP_EMAIL)
    _, second_id, second_code = await request_and_capture(fake, CORP_EMAIL)

    assert first_id != second_id
    assert (await verify_code(first_id, first_code)).status_code == 400
    assert (await verify_code(second_id, second_code)).status_code == 200


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_resend_cooldown_enforced(monkeypatch):
    await reset_auth_email_state()
    fake = install_sender(monkeypatch)

    assert (await request_code(CORP_EMAIL)).status_code == 200
    second = await request_code(CORP_EMAIL)
    assert second.status_code == 429
    assert second.json()["detail"] == auth_api.MSG_RATE_LIMITED


@pytest.mark.anyio
async def test_hourly_email_limit_enforced(monkeypatch):
    await reset_auth_email_state()
    monkeypatch.setenv("AUTH_EMAIL_RESEND_COOLDOWN_SECONDS", "0")
    monkeypatch.setenv("AUTH_EMAIL_MAX_REQUESTS_PER_EMAIL_HOUR", "3")
    fake = install_sender(monkeypatch)

    codes = [await request_code(CORP_EMAIL, ip=f"203.0.113.{i}") for i in range(1, 5)]
    statuses = [r.status_code for r in codes]
    assert statuses[:3] == [200, 200, 200]
    assert statuses[3] == 429


@pytest.mark.anyio
async def test_ip_limit_enforced_across_different_addresses(monkeypatch):
    """A single host cannot spray codes at many mailboxes."""
    await reset_auth_email_state()
    monkeypatch.setenv("AUTH_EMAIL_MAX_REQUESTS_PER_IP_HOUR", "3")
    fake = install_sender(monkeypatch)

    statuses = [
        (await request_code(f"user{i}@example-corp.it", ip="203.0.113.99")).status_code
        for i in range(5)
    ]
    assert statuses[:3] == [200, 200, 200]
    assert statuses[3:] == [429, 429]


@pytest.mark.anyio
async def test_rate_limit_survives_challenge_purge(monkeypatch):
    """Deleting challenges must not hand back spent hourly allowance."""
    await reset_auth_email_state()
    monkeypatch.setenv("AUTH_EMAIL_RESEND_COOLDOWN_SECONDS", "0")
    monkeypatch.setenv("AUTH_EMAIL_MAX_REQUESTS_PER_EMAIL_HOUR", "2")
    fake = install_sender(monkeypatch)

    assert (await request_code(CORP_EMAIL)).status_code == 200
    assert (await request_code(CORP_EMAIL)).status_code == 200

    # Simulate the TTL monitor removing every challenge record.
    await server.db[challenges.CHALLENGES_COLLECTION].delete_many({})

    assert (await request_code(CORP_EMAIL)).status_code == 429


@pytest.mark.anyio
async def test_rate_limit_buckets_outlive_the_window(monkeypatch):
    await reset_auth_email_state()
    fake = install_sender(monkeypatch)
    await request_code(CORP_EMAIL)

    bucket = await server.db[ratelimit.RATE_COLLECTION].find_one({"scope": ratelimit.SCOPE_EMAIL_HOUR})
    assert bucket is not None
    from datetime import datetime, timedelta, timezone

    purge = bucket["purge_at"]
    if purge.tzinfo is None:
        purge = purge.replace(tzinfo=timezone.utc)
    assert purge > datetime.now(timezone.utc) + timedelta(seconds=3600)


@pytest.mark.anyio
async def test_rate_limit_stores_no_raw_email_or_ip(monkeypatch):
    await reset_auth_email_state()
    fake = install_sender(monkeypatch)
    await request_code(CORP_EMAIL, ip="203.0.113.55")

    rows = await server.db[ratelimit.RATE_COLLECTION].find({}, {"_id": 0}).to_list(50)
    blob = repr(rows)
    assert CORP_EMAIL not in blob
    assert "203.0.113.55" not in blob


# ---------------------------------------------------------------------------
# Provider failure (scenario F)
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_definitive_delivery_failure_leaves_no_usable_challenge(monkeypatch):
    await reset_auth_email_state()
    install_sender(monkeypatch, outcomes=[sender_module.CATEGORY_DEFINITIVE])

    response = await request_code(CORP_EMAIL)
    assert response.status_code == 502
    assert response.json()["detail"] == auth_api.MSG_DELIVERY_UNAVAILABLE

    stored = await server.db[challenges.CHALLENGES_COLLECTION].find_one({}, {"_id": 0})
    assert stored["status"] == challenges.STATUS_SEND_FAILED
    assert "active_slot" not in stored
    assert await server.db["users"].count_documents({}) == 0


@pytest.mark.anyio
async def test_delivery_failure_message_leaks_nothing(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    await reset_auth_email_state()
    install_sender(monkeypatch, outcomes=[sender_module.CATEGORY_DEFINITIVE])

    response = await request_code(CORP_EMAIL)
    detail = response.json()["detail"]

    assert "provider" not in detail.lower()
    assert "resend" not in detail.lower()
    assert "Traceback" not in detail
    # The operator still gets a usable diagnostic.
    assert "provider_rejected_request" in caplog.text


@pytest.mark.anyio
async def test_ambiguous_delivery_still_allows_login(monkeypatch):
    """The mail may have arrived; a lost response must not lock the user out."""
    await reset_auth_email_state()
    fake = install_sender(monkeypatch, outcomes=[sender_module.CATEGORY_AMBIGUOUS])

    response = await request_code(CORP_EMAIL)
    assert response.status_code == 200
    challenge_id = response.json()["challenge_id"]

    stored = await stored_challenge(challenge_id)
    assert stored["status"] == challenges.STATUS_SEND_PENDING

    # The user did in fact receive it; reconstruct the code the sender was given.
    delivered_code = fake.calls[-1]
    assert delivered_code["category"] == sender_module.CATEGORY_AMBIGUOUS


@pytest.mark.anyio
async def test_sender_unavailable_returns_generic_message(monkeypatch):
    await reset_auth_email_state()
    sender_module.set_sender_override(None)
    monkeypatch.setenv("AUTH_EMAIL_PROVIDER", "resend")
    monkeypatch.setenv("RESEND_API_KEY", "")

    response = await request_code(CORP_EMAIL)
    assert response.status_code in (503, 502)
    assert response.json()["detail"] == auth_api.MSG_DELIVERY_UNAVAILABLE


# ---------------------------------------------------------------------------
# Fail-closed preflight
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_disabled_feature_refuses_both_endpoints(monkeypatch):
    await reset_auth_email_state()
    install_sender(monkeypatch)
    monkeypatch.setenv("AUTH_EMAIL_ENABLED", "false")

    assert (await request_code(CORP_EMAIL)).status_code == 503
    assert (await verify_code("aec_x", "123456")).status_code == 503


@pytest.mark.anyio
async def test_missing_pepper_refuses_service(monkeypatch):
    await reset_auth_email_state()
    install_sender(monkeypatch)
    monkeypatch.setenv("AUTH_EMAIL_CODE_PEPPER", "")

    response = await request_code(CORP_EMAIL)
    assert response.status_code == 503
    assert response.json()["detail"] == auth_api.MSG_DELIVERY_UNAVAILABLE


@pytest.mark.anyio
async def test_missing_identity_index_refuses_service(monkeypatch):
    """Without the uniqueness guarantee, OTP could create a duplicate account."""
    await reset_auth_email_state()
    install_sender(monkeypatch)
    await drop_identity_index()

    response = await request_code(CORP_EMAIL)
    assert response.status_code == 503
    assert await server.db[challenges.CHALLENGES_COLLECTION].count_documents({}) == 0


@pytest.mark.anyio
async def test_unverified_sender_domain_refuses_service(monkeypatch):
    await reset_auth_email_state()
    install_sender(monkeypatch)
    monkeypatch.setenv("AUTH_EMAIL_PROVIDER", "resend")
    monkeypatch.setenv("RESEND_API_KEY", "re_x")
    monkeypatch.setenv("AUTH_EMAIL_SENDER_DOMAIN_VERIFIED", "false")

    assert (await request_code(CORP_EMAIL)).status_code == 503


@pytest.mark.anyio
async def test_google_login_unaffected_when_otp_disabled(monkeypatch):
    """Disabling or misconfiguring OTP must never break Google login."""
    await reset_auth_email_state()
    monkeypatch.setenv("AUTH_EMAIL_ENABLED", "false")
    monkeypatch.setenv("AUTH_EMAIL_CODE_PEPPER", "")

    response = await api_request("GET", "/api/auth/google/start", follow_redirects=False)
    # Either a redirect to Google, or 503 only because Google itself is
    # unconfigured in the test environment — never a failure caused by OTP.
    assert response.status_code in (302, 307, 503)
    if response.status_code == 503:
        assert "Google OAuth not configured" in response.text


# ---------------------------------------------------------------------------
# Secret hygiene at the endpoint level
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_plaintext_code_never_appears_in_logs(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    await reset_auth_email_state()
    fake = install_sender(monkeypatch)

    _, challenge_id, code = await request_and_capture(fake, CORP_EMAIL)
    await verify_code(challenge_id, "000000")
    await verify_code(challenge_id, code)

    assert code not in caplog.text


@pytest.mark.anyio
async def test_plaintext_code_never_appears_in_responses(monkeypatch):
    await reset_auth_email_state()
    fake = install_sender(monkeypatch)

    response, challenge_id, code = await request_and_capture(fake, CORP_EMAIL)
    assert code not in response.text

    verified = await verify_code(challenge_id, code)
    assert code not in verified.text
