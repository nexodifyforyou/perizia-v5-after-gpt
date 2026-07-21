"""End-to-end Google OAuth callback against real Mongo.

The pre-existing ``test_google_oauth.py`` covers only the start redirect and
state rejection; none of its cases reach the shared user-resolution chokepoint.
Since passwordless login rewrote that chokepoint into an atomic upsert, the full
Google callback path needs direct coverage — otherwise a regression in Google
login would pass unnoticed.

Google is never contacted: the token and userinfo calls are intercepted.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

import httpx
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import server  # noqa: E402
from auth_email import identity  # noqa: E402
from beta_program import store as beta_store  # noqa: E402
from tests.auth_email_helpers import (  # noqa: E402
    CORP_EMAIL,
    api_request,
    apply_test_env,
    reset_auth_email_state,
)

FRONTEND = "https://periziascan.nexodify.com"


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    apply_test_env(monkeypatch)
    monkeypatch.setattr(server, "GOOGLE_CLIENT_ID", "test-client-id")
    monkeypatch.setattr(server, "GOOGLE_CLIENT_SECRET", "test-client-secret")
    monkeypatch.setattr(server, "GOOGLE_OAUTH_REDIRECT_URI", f"{FRONTEND}/api/auth/google/callback")
    monkeypatch.setattr(server, "FRONTEND_URL", FRONTEND)


def _install_google(monkeypatch, *, email=CORP_EMAIL, email_verified=True, name="Mario Rossi"):
    def _handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "oauth2.googleapis.com":
            return httpx.Response(200, json={"access_token": "ya29.test"})
        return httpx.Response(
            200,
            json={
                "email": email,
                "email_verified": email_verified,
                "name": name,
                "picture": "https://example.invalid/a.png",
            },
        )

    original = httpx.AsyncClient

    def _factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(_handler)
        return original(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", _factory)


async def _seed_state(state="state-token"):
    await server.db["oauth_states"].delete_many({})
    await server.db["oauth_states"].insert_one(
        {
            "state": state,
            "redirect_url": f"{FRONTEND}/dashboard",
            "used": False,
            "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
        }
    )
    return state


async def _callback(state="state-token"):
    return await api_request(
        "GET",
        f"/api/auth/google/callback?code=auth-code&state={state}",
        follow_redirects=False,
    )


@pytest.mark.anyio
async def test_google_callback_creates_user_and_session(monkeypatch):
    await reset_auth_email_state()
    _install_google(monkeypatch)
    await _seed_state()

    response = await _callback()

    assert response.status_code == 302
    assert response.headers["location"] == f"{FRONTEND}/dashboard"
    assert "session_token=" in response.headers.get("set-cookie", "")

    stored = await server.db["users"].find_one({"normalized_email": CORP_EMAIL}, {"_id": 0})
    assert stored is not None
    assert stored["email_verified"] is True
    assert stored["auth_methods"] == [identity.METHOD_GOOGLE]
    assert stored["last_login_method"] == identity.METHOD_GOOGLE
    assert stored["name"] == "Mario Rossi"


@pytest.mark.anyio
async def test_repeated_google_logins_do_not_duplicate(monkeypatch):
    await reset_auth_email_state()
    _install_google(monkeypatch)

    await _seed_state("s1")
    first = await _callback("s1")
    await _seed_state("s2")
    second = await _callback("s2")

    assert first.status_code == second.status_code == 302
    assert await server.db["users"].count_documents({}) == 1


@pytest.mark.anyio
async def test_google_callback_activates_pending_beta_membership(monkeypatch):
    await reset_auth_email_state()
    _install_google(monkeypatch)
    await server.db["beta_program_memberships"].insert_one(
        {
            "membership_id": "betam_g",
            "normalized_email": CORP_EMAIL,
            "status": beta_store.STATUS_PENDING,
            "quota_mode": "LIMITED",
            "analysis_limit": 5,
            "analysis_consumed": 0,
            "analysis_reserved": 0,
            "quota_version": 1,
            "entitlement_version": 1,
        }
    )
    await _seed_state()

    await _callback()

    membership = await server.db["beta_program_memberships"].find_one(
        {"normalized_email": CORP_EMAIL}, {"_id": 0}
    )
    assert membership["status"] == beta_store.STATUS_ACTIVE
    assert membership["analysis_limit"] == 5


@pytest.mark.anyio
async def test_unverified_google_email_is_refused(monkeypatch):
    """An unverified provider email must never create or link an account."""
    await reset_auth_email_state()
    _install_google(monkeypatch, email_verified=False)
    await _seed_state()

    response = await _callback()

    assert response.status_code == 401
    assert await server.db["users"].count_documents({}) == 0


@pytest.mark.anyio
async def test_google_session_authenticates_and_logs_out(monkeypatch):
    await reset_auth_email_state()
    _install_google(monkeypatch)
    await _seed_state()

    response = await _callback()
    cookie = response.headers["set-cookie"]
    token = cookie.split("session_token=")[1].split(";")[0]

    me = await api_request("GET", "/api/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert me.status_code == 200
    assert me.json()["email"] == CORP_EMAIL

    out = await api_request(
        "POST", "/api/auth/logout", headers={"Cookie": f"session_token={token}"}
    )
    assert out.status_code == 200

    after = await api_request("GET", "/api/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert after.status_code == 401


@pytest.mark.anyio
async def test_google_login_works_while_otp_disabled(monkeypatch):
    """The two methods are independent; disabling OTP must not affect Google."""
    await reset_auth_email_state()
    monkeypatch.setenv("AUTH_EMAIL_ENABLED", "false")
    monkeypatch.setenv("AUTH_EMAIL_CODE_PEPPER", "")
    _install_google(monkeypatch)
    await _seed_state()

    response = await _callback()

    assert response.status_code == 302
    assert await server.db["users"].count_documents({"normalized_email": CORP_EMAIL}) == 1
