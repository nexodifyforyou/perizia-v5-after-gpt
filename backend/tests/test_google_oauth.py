import os
import sys
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import server as server
from test_admin import FakeCollection, FakeDB


@pytest.fixture()
def fake_db(monkeypatch):
    fake_db = FakeDB()
    fake_db.oauth_states = FakeCollection("oauth_states")
    monkeypatch.setattr(server, "db", fake_db)
    monkeypatch.setattr(server, "GOOGLE_CLIENT_ID", "google-client-test")
    monkeypatch.setattr(server, "GOOGLE_CLIENT_SECRET", "google-secret-test")
    monkeypatch.setattr(server, "GOOGLE_OAUTH_REDIRECT_URI", "https://api.test/api/auth/google/callback")
    monkeypatch.setattr(server, "FRONTEND_URL", "https://frontend.test")
    return fake_db


@pytest.mark.anyio
async def test_google_oauth_start_redirects_to_google(fake_db):
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.head(
            "/api/auth/google/start?redirect=https%3A%2F%2Fperiziascan.nexodify.com%2Fdashboard",
            follow_redirects=False,
        )

    assert response.status_code in {302, 307}
    location = response.headers["location"]
    assert location.startswith(server.GOOGLE_OAUTH_AUTH_URL)
    blocked_auth_host = ".".join(["auth", "emergentagent", "com"])
    assert blocked_auth_host not in location

    query = parse_qs(urlsplit(location).query)
    assert query["client_id"] == ["google-client-test"]
    assert query["redirect_uri"] == ["https://api.test/api/auth/google/callback"]
    assert query["scope"] == ["openid email profile"]
    assert query["response_type"] == ["code"]
    assert query["state"]

    assert len(fake_db.oauth_states.items) == 1
    state_doc = fake_db.oauth_states.items[0]
    assert state_doc["state"] == query["state"][0]
    assert state_doc["redirect_url"] == "https://periziascan.nexodify.com/dashboard"
    assert state_doc["used"] is False


@pytest.mark.anyio
async def test_google_oauth_start_rejects_unsafe_redirect(fake_db):
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/api/auth/google/start?redirect=https%3A%2F%2Fevil.example%2Fdashboard",
            follow_redirects=False,
        )

    assert response.status_code == 400
    assert fake_db.oauth_states.items == []


@pytest.mark.anyio
async def test_google_oauth_callback_rejects_invalid_state(fake_db):
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/api/auth/google/callback?code=code-test&state=missing-state",
            follow_redirects=False,
        )

    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid OAuth state"
