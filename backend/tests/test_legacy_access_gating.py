"""Access gating for the TEMPORARY legacy report routes + the /meta endpoint.

Covers the owner-approved customer-access/legacy-removal contract:

* GET /api/analysis/perizia/{id}/meta  -> any authenticated OWNER of the
  analysis, metadata ONLY (whitelist projection, no report content).
* GET /api/analysis/perizia/{id} and /api/history/perizia/{id} (full OLD
  payload) -> exact owner/admin only; normal owners and non-exact admins are
  rejected.
* GET .../pdf, .../pdf-html, .../html -> exact owner/admin only, and the
  headless renderer subprocess must NEVER be spawned for an unauthorized call.
"""

import os
import sys
from datetime import datetime, timezone

import httpx
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import server as server
from test_admin import FakeDB, _seed_session

OWNER_EMAIL = "owner@example.test"
OTHER_ADMIN_EMAIL = "other.admin@example.test"
NORMAL_EMAIL = "customer@example.test"

# Legacy report content keys that must NEVER appear in the /meta response.
FORBIDDEN_META_KEYS = (
    "result",
    "data",
    "payload",
    "red_flags",
    "summary",
    "decision",
    "costs",
    "risks",
    "raw_text",
    "internal_runtime",
    "headline_overrides",
    "field_overrides",
)

META_ALLOWED_KEYS = {
    "analysis_id",
    "case_id",
    "case_title",
    "file_name",
    "created_at",
    "pages_count",
    "document_hash",
}


@pytest.fixture()
def anyio_backend():
    return "asyncio"


@pytest.fixture()
def fake_db(monkeypatch):
    db = FakeDB()
    monkeypatch.setattr(server, "db", db)
    # Exact owner/admin identity: both exact-email gates point at the owner.
    monkeypatch.setattr(server, "MASTER_ADMIN_EMAIL", OWNER_EMAIL)
    monkeypatch.setattr(server, "CORRECTNESS_V2_ADMIN_VIEW_EMAIL", OWNER_EMAIL)
    # A second, NON-exact admin (general allowlist only): must NOT pass the gate.
    monkeypatch.setattr(
        server, "ADMIN_EMAILS", frozenset({OWNER_EMAIL, OTHER_ADMIN_EMAIL})
    )
    return db


def _user_doc(user_id, email):
    return {
        "user_id": user_id,
        "email": email,
        "name": email.split("@")[0],
        "plan": "free",
        "is_master_admin": False,
        "quota": {},
    }


def _seed_analysis(db, *, analysis_id, user_id, with_result=True):
    doc = {
        "analysis_id": analysis_id,
        "user_id": user_id,
        "case_id": "case_gating",
        "case_title": "Perizia di prova",
        "file_name": "perizia.pdf",
        "pages_count": 7,
        "input_sha256": "deadbeef" * 8,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    if with_result:
        # Legacy report content that must never leak through /meta.
        doc["result"] = None  # kept falsy so the detail read path stays trivial
    db.perizia_analyses.items.append(doc)
    return doc


async def _get(path, token=None, cookies=None):
    transport = httpx.ASGITransport(app=server.app)
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test", cookies=cookies or {}
    ) as client:
        return await client.get(path, headers=headers)


# ---------------------------------------------------------------------------
# /meta — metadata only, for the analysis owner
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_meta_returns_whitelisted_metadata_only_to_owner(fake_db):
    token = _seed_session(fake_db, _user_doc("user_norm", NORMAL_EMAIL), "sess_norm")
    doc = _seed_analysis(fake_db, analysis_id="analysis_meta", user_id="user_norm")
    doc["result"] = {"red_flags": ["X"], "summary": "S", "decision": "D"}

    resp = await _get("/api/analysis/perizia/analysis_meta/meta", token=token)
    assert resp.status_code == 200
    data = resp.json()
    assert set(data.keys()) == META_ALLOWED_KEYS
    for key in FORBIDDEN_META_KEYS:
        assert key not in data, key
    assert data["analysis_id"] == "analysis_meta"
    assert data["case_id"] == "case_gating"
    assert data["case_title"] == "Perizia di prova"
    assert data["file_name"] == "perizia.pdf"
    assert data["pages_count"] == 7
    assert data["document_hash"] == "deadbeef" * 8
    # No legacy report content anywhere in the body.
    assert "red_flags" not in resp.text


@pytest.mark.anyio
async def test_meta_enforces_ownership(fake_db):
    token = _seed_session(fake_db, _user_doc("user_norm", NORMAL_EMAIL), "sess_norm")
    _seed_analysis(fake_db, analysis_id="analysis_foreign", user_id="someone_else")

    resp = await _get("/api/analysis/perizia/analysis_foreign/meta", token=token)
    assert resp.status_code == 404


@pytest.mark.anyio
async def test_meta_requires_auth(fake_db):
    _seed_analysis(fake_db, analysis_id="analysis_meta2", user_id="user_norm")
    resp = await _get("/api/analysis/perizia/analysis_meta2/meta")
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Full-payload detail routes — exact owner/admin only
# ---------------------------------------------------------------------------
@pytest.mark.anyio
@pytest.mark.parametrize(
    "path",
    [
        "/api/analysis/perizia/analysis_full",
        "/api/history/perizia/analysis_full",
    ],
)
async def test_full_payload_rejected_for_normal_owner(fake_db, path):
    token = _seed_session(fake_db, _user_doc("user_norm", NORMAL_EMAIL), "sess_norm")
    _seed_analysis(fake_db, analysis_id="analysis_full", user_id="user_norm")

    resp = await _get(path, token=token)
    assert resp.status_code == 403


@pytest.mark.anyio
@pytest.mark.parametrize(
    "path",
    [
        "/api/analysis/perizia/analysis_full",
        "/api/history/perizia/analysis_full",
    ],
)
async def test_full_payload_rejected_for_non_exact_admin(fake_db, path):
    token = _seed_session(
        fake_db, _user_doc("user_other_admin", OTHER_ADMIN_EMAIL), "sess_other"
    )
    _seed_analysis(fake_db, analysis_id="analysis_full", user_id="user_other_admin")

    resp = await _get(path, token=token)
    assert resp.status_code == 403


@pytest.mark.anyio
async def test_full_payload_still_served_to_exact_owner(fake_db):
    token = _seed_session(fake_db, _user_doc("user_owner", OWNER_EMAIL), "sess_owner")
    _seed_analysis(fake_db, analysis_id="analysis_full", user_id="user_owner")

    resp = await _get("/api/analysis/perizia/analysis_full", token=token)
    assert resp.status_code == 200
    assert resp.json()["analysis_id"] == "analysis_full"


@pytest.mark.anyio
async def test_full_payload_requires_auth(fake_db):
    _seed_analysis(fake_db, analysis_id="analysis_full", user_id="user_owner")
    resp = await _get("/api/analysis/perizia/analysis_full")
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Legacy render endpoints — exact owner/admin only; NO render for others
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_pdf_html_normal_user_rejected_and_no_render_spawned(fake_db, monkeypatch):
    calls = []

    async def _spy_render(*args, **kwargs):
        calls.append((args, kwargs))
        return b"%PDF-fake"

    monkeypatch.setattr(server, "_render_print_pdf_via_frontend", _spy_render)
    token = _seed_session(fake_db, _user_doc("user_norm", NORMAL_EMAIL), "sess_norm")
    _seed_analysis(fake_db, analysis_id="analysis_pdf", user_id="user_norm")

    resp = await _get(
        "/api/analysis/perizia/analysis_pdf/pdf-html",
        token=token,
        cookies={"session_token": "sess_norm"},
    )
    assert resp.status_code == 403
    assert calls == []  # authorization must precede ANY headless render


@pytest.mark.anyio
async def test_pdf_html_unauthenticated_rejected_and_no_render_spawned(fake_db, monkeypatch):
    calls = []

    async def _spy_render(*args, **kwargs):
        calls.append((args, kwargs))
        return b"%PDF-fake"

    monkeypatch.setattr(server, "_render_print_pdf_via_frontend", _spy_render)
    _seed_analysis(fake_db, analysis_id="analysis_pdf", user_id="user_owner")

    resp = await _get("/api/analysis/perizia/analysis_pdf/pdf-html")
    assert resp.status_code == 401
    assert calls == []


@pytest.mark.anyio
async def test_pdf_html_exact_owner_permitted(fake_db, monkeypatch):
    calls = []

    async def _spy_render(analysis_id, session_token, api_base_url):
        calls.append(analysis_id)
        return b"%PDF-fake"

    monkeypatch.setattr(server, "_render_print_pdf_via_frontend", _spy_render)
    token = _seed_session(fake_db, _user_doc("user_owner", OWNER_EMAIL), "sess_owner")
    _seed_analysis(fake_db, analysis_id="analysis_pdf", user_id="user_owner")

    resp = await _get(
        "/api/analysis/perizia/analysis_pdf/pdf-html",
        token=token,
        cookies={"session_token": "sess_owner"},
    )
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/pdf")
    assert calls == ["analysis_pdf"]


@pytest.mark.anyio
@pytest.mark.parametrize("suffix", ["pdf", "html"])
async def test_other_legacy_render_routes_rejected_for_normal_user(fake_db, suffix):
    token = _seed_session(fake_db, _user_doc("user_norm", NORMAL_EMAIL), "sess_norm")
    _seed_analysis(fake_db, analysis_id="analysis_render", user_id="user_norm")

    resp = await _get(f"/api/analysis/perizia/analysis_render/{suffix}", token=token)
    assert resp.status_code == 403
