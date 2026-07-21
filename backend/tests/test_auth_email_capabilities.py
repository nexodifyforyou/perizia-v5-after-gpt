"""The public capability probe the login screen reads before any session exists.

The deployment contract these tests defend: the frontend auto-deploys to Vercel
on a push to main, while the backend rolls out separately with
``AUTH_EMAIL_ENABLED=false``. For that window the new build must not advertise
an email-login button that cannot work, and it must learn that from the backend
rather than from a build-time variable that could be stale or wrong.
"""

import os
import sys

import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from auth_email import api as auth_api  # noqa: E402
from auth_email import identity  # noqa: E402
import server  # noqa: E402

from tests.auth_email_helpers import (  # noqa: E402
    CORP_EMAIL,
    api_request,
    apply_test_env,
    clear_sender,
    drop_identity_index,
    install_sender,
    request_and_capture,
    request_code,
    reset_auth_email_state,
)

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    apply_test_env(monkeypatch)
    yield
    clear_sender()


async def capabilities():
    return await api_request("GET", auth_api.CAPABILITIES_PATH)


# ---------------------------------------------------------------------------
# 1 / 2 — the capability tracks the backend, in both directions
# ---------------------------------------------------------------------------
async def test_capability_is_false_when_feature_disabled(monkeypatch):
    await reset_auth_email_state()
    monkeypatch.setenv("AUTH_EMAIL_ENABLED", "false")

    response = await capabilities()
    assert response.status_code == 200
    assert response.json()["email_otp_enabled"] is False


async def test_capability_is_true_when_feature_fully_configured():
    await reset_auth_email_state()

    response = await capabilities()
    assert response.status_code == 200
    assert response.json()["email_otp_enabled"] is True


async def test_capability_flips_with_the_backend_flag_without_restart(monkeypatch):
    """A normal refresh after flipping the flag must see the new value.

    Nothing is memoised per process, so the second call re-reads the
    environment. This is what makes "enable by restarting the backend with the
    flag on" sufficient, with no frontend redeploy.
    """
    await reset_auth_email_state()
    assert (await capabilities()).json()["email_otp_enabled"] is True

    monkeypatch.setenv("AUTH_EMAIL_ENABLED", "false")
    assert (await capabilities()).json()["email_otp_enabled"] is False

    monkeypatch.setenv("AUTH_EMAIL_ENABLED", "true")
    assert (await capabilities()).json()["email_otp_enabled"] is True


# ---------------------------------------------------------------------------
# The capability mirrors the *whole* preflight, not just the flag
# ---------------------------------------------------------------------------
async def test_capability_false_when_flag_on_but_pepper_missing(monkeypatch):
    """Flag on + broken config would 503 on every request; do not advertise it."""
    await reset_auth_email_state()
    monkeypatch.setenv("AUTH_EMAIL_CODE_PEPPER", "")

    assert (await capabilities()).json()["email_otp_enabled"] is False
    assert (await request_code(CORP_EMAIL)).status_code == 503


async def test_capability_false_when_identity_index_missing():
    await reset_auth_email_state()
    await drop_identity_index()

    assert (await capabilities()).json()["email_otp_enabled"] is False


async def test_capability_false_when_sender_domain_unverified(monkeypatch):
    await reset_auth_email_state()
    monkeypatch.setenv("AUTH_EMAIL_PROVIDER", "resend")
    monkeypatch.setenv("RESEND_API_KEY", "re_x")
    monkeypatch.setenv("AUTH_EMAIL_SENDER_DOMAIN_VERIFIED", "false")

    assert (await capabilities()).json()["email_otp_enabled"] is False


async def test_capability_agrees_with_the_endpoint_it_describes(monkeypatch):
    """The advertised capability and the served behaviour never disagree."""
    await reset_auth_email_state()
    install_sender(monkeypatch)

    for enabled in ("true", "false"):
        monkeypatch.setenv("AUTH_EMAIL_ENABLED", enabled)
        advertised = (await capabilities()).json()["email_otp_enabled"]
        served = (await request_code(CORP_EMAIL)).status_code != 503
        assert advertised == served, f"capability {advertised} but served {served}"


# ---------------------------------------------------------------------------
# 3 — Google is never gated on the email feature
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("enabled", ["true", "false"])
async def test_google_capability_reported_in_both_states(monkeypatch, enabled):
    await reset_auth_email_state()
    monkeypatch.setenv("AUTH_EMAIL_ENABLED", enabled)

    assert (await capabilities()).json()["google_enabled"] is True


async def test_google_start_reachable_in_both_states(monkeypatch):
    await reset_auth_email_state()
    for enabled in ("true", "false"):
        monkeypatch.setenv("AUTH_EMAIL_ENABLED", enabled)
        response = await api_request(
            "GET", "/api/auth/google/start", follow_redirects=False
        )
        # 503 only if Google itself is unconfigured here — never caused by OTP.
        assert response.status_code in (302, 307, 503)
        if response.status_code == 503:
            assert "Google OAuth not configured" in response.text


# ---------------------------------------------------------------------------
# 4 — direct call while disabled still fails closed
# ---------------------------------------------------------------------------
async def test_direct_request_code_fails_closed_while_disabled(monkeypatch):
    """A client that ignores the capability gains nothing."""
    await reset_auth_email_state()
    install_sender(monkeypatch)
    monkeypatch.setenv("AUTH_EMAIL_ENABLED", "false")

    assert (await capabilities()).json()["email_otp_enabled"] is False

    response = await request_code(CORP_EMAIL)
    assert response.status_code == 503
    assert response.json()["detail"] == auth_api.MSG_DELIVERY_UNAVAILABLE
    # And no challenge was created, so no code was minted or sent.
    from auth_email import challenges

    assert await server.db[challenges.CHALLENGES_COLLECTION].count_documents({}) == 0


# ---------------------------------------------------------------------------
# 5 / 6 — the payload leaks nothing
# ---------------------------------------------------------------------------
async def test_capability_is_identical_for_every_caller(monkeypatch):
    """The probe takes no input, so it cannot disclose account existence."""
    await reset_auth_email_state()
    install_sender(monkeypatch)

    baseline = (await capabilities()).json()

    # Register a real identity, then ask again from a different client IP.
    fake = install_sender(monkeypatch)
    _, challenge_id, code = await request_and_capture(fake, CORP_EMAIL)
    assert challenge_id and code
    await api_request(
        "POST",
        "/api/auth/email/verify-code",
        json={"challenge_id": challenge_id, "code": code},
    )
    assert await server.db["users"].count_documents({}) == 1

    after = await api_request(
        "GET",
        auth_api.CAPABILITIES_PATH,
        headers={"x-forwarded-for": "198.51.100.7"},
    )
    assert after.json() == baseline


async def test_payload_contains_no_secret_or_internal_configuration(monkeypatch):
    """Exactly two booleans. No provider, no sender, no key, no reasons."""
    await reset_auth_email_state()
    monkeypatch.setenv("AUTH_EMAIL_PROVIDER", "resend")
    monkeypatch.setenv("RESEND_API_KEY", "re_supersecret_value_0123456789")
    monkeypatch.setenv("AUTH_EMAIL_CODE_PEPPER", "pepper-that-must-never-be-echoed-0123456789")
    monkeypatch.setenv("AUTH_EMAIL_SENDER_DOMAIN_VERIFIED", "false")

    response = await capabilities()
    body = response.json()

    assert set(body.keys()) == {"email_otp_enabled", "google_enabled"}
    assert all(isinstance(v, bool) for v in body.values())

    raw = response.text.lower()
    for forbidden in (
        "re_supersecret",
        "pepper",
        "resend",
        "nexodify.com",
        "sender_domain_not_verified",
        "feature_disabled",
        "reason",
        "mongo",
        "index",
    ):
        assert forbidden not in raw, f"capability payload leaked {forbidden!r}"


async def test_capability_requires_no_authentication():
    """It is called before a session exists, so it must be public."""
    await reset_auth_email_state()

    response = await capabilities()
    assert response.status_code == 200


async def test_capability_is_listed_as_a_public_path():
    """OpenAPI must not mark the probe as requiring a session."""
    schema = server.app.openapi()
    operation = schema["paths"][auth_api.CAPABILITIES_PATH]["get"]
    assert "security" not in operation


# ---------------------------------------------------------------------------
# Fails closed when the database is unreachable
# ---------------------------------------------------------------------------
async def test_capability_false_when_index_probe_raises(monkeypatch):
    await reset_auth_email_state()

    async def _boom(*args, **kwargs):
        raise RuntimeError("mongo unreachable")

    monkeypatch.setattr(identity, "unique_index_ready", _boom)

    response = await capabilities()
    assert response.status_code == 200
    assert response.json()["email_otp_enabled"] is False
    assert response.json()["google_enabled"] is True
