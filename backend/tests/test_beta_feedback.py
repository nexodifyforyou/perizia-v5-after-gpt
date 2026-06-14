import os
import re
import sys
from datetime import datetime, timezone, timedelta

import pytest
import httpx

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import server as server


# ---------------------------------------------------------------------------
# Minimal async Mongo fake (supports dotted nested-key matching).
# ---------------------------------------------------------------------------
class FakeCursor:
    def __init__(self, items):
        self.items = list(items)

    def sort(self, key, direction):
        reverse = direction == -1
        self.items.sort(key=lambda x: (x.get(key) is None, x.get(key)), reverse=reverse)
        return self

    def skip(self, n):
        self.items = self.items[n:]
        return self

    def limit(self, n):
        self.items = self.items[:n]
        return self

    async def to_list(self, length):
        if length is None:
            return list(self.items)
        return list(self.items[:length])


def _get_nested(doc, dotted):
    cur = doc
    for part in dotted.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


class FakeCollection:
    def __init__(self, name, items=None):
        self.name = name
        self.items = items or []
        self.inserted = []
        self.update_calls = []

    def _match(self, doc, filt):
        if not filt:
            return True
        for key, value in filt.items():
            if key == "$and":
                if not all(self._match(doc, f) for f in value):
                    return False
                continue
            if key == "$or":
                if not any(self._match(doc, f) for f in value):
                    return False
                continue
            doc_val = _get_nested(doc, key) if "." in key else doc.get(key)
            if isinstance(value, dict):
                if "$regex" in value:
                    flags = re.I if "i" in value.get("$options", "") else 0
                    if not re.search(value["$regex"], str(doc_val or ""), flags):
                        return False
                    continue
                if "$gte" in value or "$lte" in value:
                    gte, lte = value.get("$gte"), value.get("$lte")
                    if gte is not None and (doc_val is None or doc_val < gte):
                        return False
                    if lte is not None and (doc_val is None or doc_val > lte):
                        return False
                    continue
                if doc_val != value:
                    return False
            else:
                if doc_val != value:
                    return False
        return True

    def _apply_projection(self, doc, projection):
        if projection is None:
            return dict(doc)
        include = {k for k, v in projection.items() if v}
        exclude = {k for k, v in projection.items() if v == 0}
        if include:
            out = {}
            for k in include:
                if "." in k:
                    val = _get_nested(doc, k)
                    if val is not None:
                        out[k] = val
                elif k in doc:
                    out[k] = doc.get(k)
            return out
        return {k: v for k, v in doc.items() if k not in exclude}

    async def find_one(self, filt, projection=None, sort=None):
        matches = [d for d in self.items if self._match(d, filt)]
        if not matches:
            return None
        return self._apply_projection(matches[0], projection)

    def find(self, filt, projection=None):
        matches = [self._apply_projection(d, projection) for d in self.items if self._match(d, filt)]
        return FakeCursor(matches)

    async def count_documents(self, filt):
        return len([d for d in self.items if self._match(d, filt)])

    async def insert_one(self, doc):
        self.items.append(doc)
        self.inserted.append(doc)

    async def update_one(self, filt, update, upsert=False):
        self.update_calls.append(update)
        matches = [d for d in self.items if self._match(d, filt)]
        if not matches:
            if not upsert:
                return
            new_doc = dict(filt)
            if "$set" in update:
                new_doc.update(update["$set"])
            self.items.append(new_doc)
            return
        doc = matches[0]
        if "$set" in update:
            doc.update(update["$set"])
        if "$inc" in update:
            for k, v in update["$inc"].items():
                parts = k.split(".")
                if len(parts) == 2:
                    parent = doc.setdefault(parts[0], {})
                    parent[parts[1]] = parent.get(parts[1], 0) + v
                else:
                    doc[k] = doc.get(k, 0) + v

    async def create_index(self, field):
        return None


class FakeDB:
    def __init__(self):
        for name in [
            "users", "user_sessions", "perizia_analyses", "image_forensics",
            "assistant_qa", "payment_transactions", "credit_ledger",
            "billing_records", "admin_audit_log", "admin_user_notes",
            "perizia_confirmations", "beta_feedback", "security_audit_log",
        ]:
            setattr(self, name, FakeCollection(name))


@pytest.fixture()
def anyio_backend():
    return "asyncio"


@pytest.fixture()
def fake_db(monkeypatch):
    db = FakeDB()
    monkeypatch.setattr(server, "db", db)
    server.MASTER_ADMIN_EMAIL = "admin@nexodify.com"
    monkeypatch.setattr(server, "ADMIN_EMAILS", frozenset({"nexodifyforyou@gmail.com"}))
    monkeypatch.setattr(server, "BETA_UNLIMITED_EMAILS", frozenset({"geomazzantiriccardo@gmail.com"}))
    return db


def _seed_session(db, user_doc, token):
    db.users.items.append(user_doc)
    db.user_sessions.items.append({
        "session_token": token,
        "user_id": user_doc["user_id"],
        "expires_at": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat(),
    })
    return token


def _beta_user():
    return {
        "user_id": "user_beta", "email": "geomazzantiriccardo@gmail.com",
        "name": "Geom. Mazzanti", "plan": "free", "is_master_admin": False, "quota": {},
    }


def _normal_user():
    return {
        "user_id": "user_norm", "email": "mario@example.com", "name": "Mario",
        "plan": "solo", "is_master_admin": False,
        "quota": {"perizia_scans_remaining": 28, "image_scans_remaining": 0, "assistant_messages_remaining": 0},
    }


def _admin_user():
    return {
        "user_id": "user_admin", "email": "nexodifyforyou@gmail.com", "name": "Syed / Nexodify Admin",
        "plan": "free", "is_master_admin": False, "quota": {},
    }


async def _client_request(method, path, token=None, **kwargs):
    transport = httpx.ASGITransport(app=server.app)
    headers = kwargs.pop("headers", {})
    if token:
        headers["Authorization"] = f"Bearer {token}"
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.request(method, path, headers=headers, **kwargs)


# ---------------------------------------------------------------------------
# Part 5 — learning_label derivation (pure)
# ---------------------------------------------------------------------------
def test_learning_label_mapping_all_types():
    expected = {
        "corretto": (False, None, False, True, False),
        "parzialmente_corretto": (True, "partial_error", True, True, True),
        "sbagliato": (True, "wrong_output", True, True, True),
        "manca_informazione": (True, "missing_information", True, True, True),
        "classificazione_troppo_forte": (True, "over_classification", True, True, True),
        "classificazione_troppo_debole": (True, "under_classification", True, True, True),
        "fonte_pagina_errata": (True, "wrong_source_page", True, True, True),
        "valore_estratto_errato": (True, "wrong_extracted_value", True, True, True),
        "duplicato": (True, "duplicate_output", True, True, True),
        "non_utile": (False, "low_utility", False, True, True),
        "wording_confuso": (True, "unclear_wording", True, True, True),
        "altro": (True, "other", True, True, True),
    }
    for ft, (is_err, cat, corr, learn, human) in expected.items():
        label = server._derive_beta_learning_label(ft)
        assert label["is_error"] is is_err, ft
        assert label["error_category"] == cat, ft
        assert label["correction_needed"] is corr, ft
        assert label["model_should_learn"] is learn, ft
        assert label["human_review_required"] is human, ft


def test_beta_email_helpers():
    assert server._is_beta_unlimited_email("geomazzantiriccardo@gmail.com") is True
    assert server._is_beta_unlimited_email("GEOMazzantiRiccardo@Gmail.com") is True
    assert server._is_beta_unlimited_email("someone@else.com") is False
    assert server._is_beta_unlimited_email(None) is False


def test_admin_email_helpers(fake_db):
    assert server._is_admin_email("admin@nexodify.com") is True
    assert server._is_admin_email("nexodifyforyou@gmail.com") is True
    assert server._is_admin_email("NEXODIFYFORYOU@GMAIL.COM") is True
    assert server._is_admin_email("geomazzantiriccardo@gmail.com") is False
    assert server._user_is_admin(server.User(**_admin_user())) is True
    assert server._user_is_admin(server.User(**_beta_user())) is False


# ---------------------------------------------------------------------------
# Part 1 — credit exemption / debit
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_beta_user_not_debited(fake_db):
    fake_db.users.items.append(_beta_user())
    user = server.User(**_beta_user())
    result = await server._apply_perizia_credit_debit_with_ledger(
        user, amount=4, entry_type="perizia_upload", reference_type="analysis",
        reference_id="analysis_x", description_it="test",
    )
    assert result is False
    assert len(fake_db.credit_ledger.items) == 0


@pytest.mark.anyio
async def test_normal_user_is_debited(fake_db):
    fake_db.users.items.append(_normal_user())
    user = server.User(**_normal_user())
    user.quota["perizia_scans_remaining"] = 28
    result = await server._apply_perizia_credit_debit_with_ledger(
        user, amount=4, entry_type="perizia_upload", reference_type="analysis",
        reference_id="analysis_y", description_it="test",
    )
    assert result is True
    assert len(fake_db.credit_ledger.items) >= 1


def test_credit_exempt_user():
    assert server._is_credit_exempt_user(server.User(**_beta_user())) is True
    assert server._is_credit_exempt_user(server.User(**_admin_user())) is True
    assert server._is_credit_exempt_user(server.User(**_normal_user())) is False


# ---------------------------------------------------------------------------
# Part 6 — endpoints
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_unauthenticated_cannot_submit(fake_db):
    resp = await _client_request("POST", "/api/beta-feedback", json={
        "feedback_type": "sbagliato", "expert_comment": "x",
    })
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_authenticated_user_can_submit_and_label_derived(fake_db):
    token = _seed_session(fake_db, _beta_user(), "sess_beta")
    fake_db.perizia_analyses.items.append({
        "analysis_id": "analysis_1", "user_id": "user_beta",
        "case_id": "case_1", "file_name": "perizia.pdf",
    })
    resp = await _client_request("POST", "/api/beta-feedback", token=token, json={
        "analysis_id": "analysis_1",
        "section_key": "costi_oneri",
        "feedback_type": "classificazione_troppo_forte",
        "priority": "alta",
        "expert_comment": "La classificazione è eccessiva.",
        "expected_correction": "Dovrebbe essere un punto di attenzione.",
        "original_ai_output": {"title": "Rischio X", "classification": "blocco"},
    })
    assert resp.status_code == 200, resp.text
    fb = resp.json()["feedback"]
    assert fb["analysis_id"] == "analysis_1"
    assert fb["section_key"] == "costi_oneri"
    assert fb["section_label_it"] == "Costi e oneri"
    assert fb["feedback_type"] == "classificazione_troppo_forte"
    assert fb["priority"] == "alta"
    assert fb["expert_comment"] == "La classificazione è eccessiva."
    assert fb["original_ai_output"]["title"] == "Rischio X"
    assert fb["learning_label"]["error_category"] == "over_classification"
    assert fb["learning_label"]["is_error"] is True
    assert fb["status"] == "new"


@pytest.mark.anyio
async def test_submit_rejects_invalid_feedback_type(fake_db):
    token = _seed_session(fake_db, _beta_user(), "sess_beta")
    resp = await _client_request("POST", "/api/beta-feedback", token=token, json={
        "feedback_type": "not_a_real_type", "expert_comment": "x",
    })
    assert resp.status_code == 422


@pytest.mark.anyio
async def test_submit_requires_comment(fake_db):
    token = _seed_session(fake_db, _beta_user(), "sess_beta")
    resp = await _client_request("POST", "/api/beta-feedback", token=token, json={
        "feedback_type": "sbagliato", "expert_comment": "   ",
    })
    assert resp.status_code == 422


@pytest.mark.anyio
async def test_comment_length_truncated(fake_db):
    token = _seed_session(fake_db, _beta_user(), "sess_beta")
    long_comment = "a" * 9000
    resp = await _client_request("POST", "/api/beta-feedback", token=token, json={
        "feedback_type": "sbagliato", "expert_comment": long_comment,
    })
    assert resp.status_code == 200
    assert len(resp.json()["feedback"]["expert_comment"]) == server.BETA_COMMENT_MAX_LEN


@pytest.mark.anyio
async def test_analysis_ownership_enforced_for_non_owner(fake_db):
    token = _seed_session(fake_db, _normal_user(), "sess_norm")
    fake_db.perizia_analyses.items.append({
        "analysis_id": "analysis_other", "user_id": "someone_else",
    })
    resp = await _client_request("POST", "/api/beta-feedback", token=token, json={
        "analysis_id": "analysis_other", "feedback_type": "sbagliato", "expert_comment": "x",
    })
    assert resp.status_code == 403


@pytest.mark.anyio
async def test_user_sees_only_own_feedback(fake_db):
    token_beta = _seed_session(fake_db, _beta_user(), "sess_beta")
    _seed_session(fake_db, _normal_user(), "sess_norm")
    fake_db.beta_feedback.items.extend([
        {"id": "fb_a", "user_id": "user_beta", "created_at": "2026-01-01T00:00:00", "expert_comment": "a"},
        {"id": "fb_b", "user_id": "user_norm", "created_at": "2026-01-02T00:00:00", "expert_comment": "b"},
    ])
    resp = await _client_request("GET", "/api/beta-feedback/my", token=token_beta)
    assert resp.status_code == 200
    ids = [i["id"] for i in resp.json()["items"]]
    assert ids == ["fb_a"]


@pytest.mark.anyio
async def test_admin_sees_all_feedback(fake_db):
    token = _seed_session(fake_db, _admin_user(), "sess_admin")
    fake_db.beta_feedback.items.extend([
        {"id": "fb_a", "user_id": "user_beta", "user_email": "geomazzantiriccardo@gmail.com",
         "created_at": "2026-01-01T00:00:00", "section_key": "costi_oneri", "priority": "alta",
         "status": "new", "learning_label": {"error_category": "over_classification", "is_error": True, "model_should_learn": True}},
        {"id": "fb_b", "user_id": "user_norm", "user_email": "mario@example.com",
         "created_at": "2026-01-02T00:00:00", "section_key": "superficie", "priority": "bassa",
         "status": "accepted", "learning_label": {"error_category": "low_utility", "is_error": False, "model_should_learn": True}},
    ])
    resp = await _client_request("GET", "/api/admin/beta-feedback", token=token)
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 2
    assert data["metrics"]["total"] == 2
    assert data["metrics"]["high_priority"] == 1


@pytest.mark.anyio
async def test_admin_owner_can_access_admin_feedback_endpoints(fake_db):
    token = _seed_session(fake_db, _admin_user(), "sess_admin")
    resp = await _client_request("GET", "/api/admin/beta-feedback", token=token)
    assert resp.status_code == 200
    assert resp.json()["total"] == 0


@pytest.mark.anyio
async def test_unauthenticated_cannot_access_admin_feedback_endpoint(fake_db):
    resp = await _client_request("GET", "/api/admin/beta-feedback")
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_admin_filter_model_should_learn(fake_db):
    token = _seed_session(fake_db, _admin_user(), "sess_admin")
    fake_db.beta_feedback.items.extend([
        {"id": "fb_a", "user_id": "u1", "created_at": "2026-01-01T00:00:00",
         "learning_label": {"model_should_learn": True}},
        {"id": "fb_b", "user_id": "u2", "created_at": "2026-01-02T00:00:00",
         "learning_label": {"model_should_learn": False}},
    ])
    resp = await _client_request("GET", "/api/admin/beta-feedback?model_should_learn=true", token=token)
    assert resp.status_code == 200
    ids = [i["id"] for i in resp.json()["items"]]
    assert ids == ["fb_a"]


@pytest.mark.anyio
async def test_admin_filter_by_user_email(fake_db):
    token = _seed_session(fake_db, _admin_user(), "sess_admin")
    fake_db.beta_feedback.items.extend([
        {"id": "fb_mazzanti", "user_id": "user_beta", "user_email": "geomazzantiriccardo@gmail.com",
         "created_at": "2026-01-01T00:00:00", "status": "new", "priority": "media", "learning_label": {}},
        {"id": "fb_admin", "user_id": "user_admin", "user_email": "nexodifyforyou@gmail.com",
         "created_at": "2026-01-02T00:00:00", "status": "new", "priority": "bassa", "learning_label": {}},
    ])
    resp = await _client_request(
        "GET",
        "/api/admin/beta-feedback?user_email=geomazzantiriccardo",
        token=token,
    )
    assert resp.status_code == 200
    ids = [i["id"] for i in resp.json()["items"]]
    assert ids == ["fb_mazzanti"]


@pytest.mark.anyio
async def test_non_admin_cannot_access_admin_endpoint(fake_db):
    token = _seed_session(fake_db, _beta_user(), "sess_beta")
    resp = await _client_request("GET", "/api/admin/beta-feedback", token=token)
    assert resp.status_code == 403
    export_resp = await _client_request("GET", "/api/admin/beta-feedback/export?format=json", token=token)
    assert export_resp.status_code == 403


@pytest.mark.anyio
async def test_admin_can_update_status(fake_db):
    token = _seed_session(fake_db, _admin_user(), "sess_admin")
    fake_db.beta_feedback.items.append({
        "id": "fb_x", "user_id": "user_beta", "created_at": "2026-01-01T00:00:00",
        "status": "new", "admin_notes": None,
    })
    resp = await _client_request("PATCH", "/api/admin/beta-feedback/fb_x", token=token, json={
        "status": "accepted", "admin_notes": "Verificato.",
    })
    assert resp.status_code == 200
    fb = resp.json()["feedback"]
    assert fb["status"] == "accepted"
    assert fb["admin_notes"] == "Verificato."
    assert fb["reviewed_by"] == "nexodifyforyou@gmail.com"


@pytest.mark.anyio
async def test_admin_update_rejects_bad_status(fake_db):
    token = _seed_session(fake_db, _admin_user(), "sess_admin")
    fake_db.beta_feedback.items.append({"id": "fb_x", "user_id": "u", "created_at": "2026-01-01T00:00:00", "status": "new"})
    resp = await _client_request("PATCH", "/api/admin/beta-feedback/fb_x", token=token, json={"status": "bogus"})
    assert resp.status_code == 422


@pytest.mark.anyio
async def test_export_json(fake_db):
    token = _seed_session(fake_db, _admin_user(), "sess_admin")
    fake_db.beta_feedback.items.append({
        "id": "fb_x", "user_id": "user_beta", "user_email": "geomazzantiriccardo@gmail.com",
        "created_at": "2026-01-01T00:00:00", "feedback_type": "sbagliato",
        "learning_label": {"error_category": "wrong_output"},
        "item_reference": {}, "original_ai_output": {},
    })
    resp = await _client_request("GET", "/api/admin/beta-feedback/export?format=json", token=token)
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert data["items"][0]["id"] == "fb_x"


@pytest.mark.anyio
async def test_export_csv(fake_db):
    token = _seed_session(fake_db, _admin_user(), "sess_admin")
    fake_db.beta_feedback.items.append({
        "id": "fb_x", "user_id": "user_beta", "user_email": "geomazzantiriccardo@gmail.com",
        "created_at": "2026-01-01T00:00:00", "feedback_type": "sbagliato", "priority": "alta",
        "section_key": "costi_oneri", "section_label_it": "Costi e oneri",
        "expert_comment": "comment with, comma", "status": "new",
        "learning_label": {"error_category": "wrong_output", "is_error": True},
        "item_reference": {"item_path": "result.x[0]"}, "original_ai_output": {"title": "T"},
    })
    resp = await _client_request("GET", "/api/admin/beta-feedback/export?format=csv", token=token)
    assert resp.status_code == 200
    assert "text/csv" in resp.headers["content-type"]
    body = resp.text
    assert "id,created_at" in body
    assert "fb_x" in body
    assert "result.x[0]" in body


@pytest.mark.anyio
async def test_admin_can_submit_test_feedback_distinguishable_from_mazzanti(fake_db):
    admin_token = _seed_session(fake_db, _admin_user(), "sess_admin")
    beta_token = _seed_session(fake_db, _beta_user(), "sess_beta")

    admin_resp = await _client_request("POST", "/api/beta-feedback", token=admin_token, json={
        "section_key": "rischi_punti_critici",
        "feedback_type": "wording_confuso",
        "priority": "bassa",
        "expert_comment": "TEST ADMIN PROOF — not expert feedback",
        "expected_correction": "TEST ADMIN PROOF — no real correction",
        "permission_for_learning": False,
        "source": "admin_entry",
    })
    assert admin_resp.status_code == 200, admin_resp.text
    admin_fb = admin_resp.json()["feedback"]
    assert admin_fb["user_email"] == "nexodifyforyou@gmail.com"
    assert admin_fb["user_role"] == "admin"
    assert admin_fb["beta_partner_name"] == "Syed / Nexodify Admin"
    assert admin_fb["beta_partner_type"] == "altro"
    assert admin_fb["permission_for_learning"] is False
    assert admin_fb["source"] == "admin_entry"

    beta_resp = await _client_request("POST", "/api/beta-feedback", token=beta_token, json={
        "feedback_type": "sbagliato",
        "expert_comment": "Feedback tecnico Mazzanti",
    })
    assert beta_resp.status_code == 200, beta_resp.text
    beta_fb = beta_resp.json()["feedback"]
    assert beta_fb["user_email"] == "geomazzantiriccardo@gmail.com"
    assert beta_fb["user_role"] == "beta_partner"
    assert beta_fb["beta_partner_name"] == "Geom. Riccardo Mazzanti"
    assert beta_fb["beta_partner_type"] == "geometra"

    assert admin_fb["user_role"] != beta_fb["user_role"]
    assert admin_fb["permission_for_learning"] is False
    assert beta_fb["permission_for_learning"] is True


@pytest.mark.anyio
async def test_beta_dashboard_summary(fake_db):
    token = _seed_session(fake_db, _beta_user(), "sess_beta")
    fake_db.perizia_analyses.items.append({
        "analysis_id": "analysis_1", "user_id": "user_beta", "case_id": "case_1",
        "file_name": "perizia.pdf", "created_at": "2026-01-01T00:00:00", "status": "COMPLETED",
    })
    fake_db.beta_feedback.items.extend([
        {"id": "fb_1", "user_id": "user_beta", "analysis_id": "analysis_1",
         "created_at": "2026-01-02T00:00:00", "status": "new", "feedback_type": "manca_informazione", "priority": "alta",
         "learning_label": {"error_category": "missing_information"}},
        {"id": "fb_2", "user_id": "user_beta", "analysis_id": "analysis_1",
         "created_at": "2026-01-03T00:00:00", "status": "accepted", "feedback_type": "classificazione_troppo_forte", "priority": "media",
         "learning_label": {"error_category": "over_classification"}},
    ])
    resp = await _client_request("GET", "/api/beta/dashboard-summary", token=token)
    assert resp.status_code == 200
    data = resp.json()
    assert data["synthesis"]["feedback_totali"] == 2
    assert data["synthesis"]["informazioni_mancanti"] == 1
    assert data["synthesis"]["classificazioni_troppo_forti"] == 1
    assert data["synthesis"]["osservazioni_alta_priorita"] == 1
    row = data["analyses"][0]
    assert row["feedback_count"] == 2
    assert row["unresolved_feedback_count"] == 1


@pytest.mark.anyio
async def test_admin_owner_can_access_beta_dashboard_summary(fake_db):
    token = _seed_session(fake_db, _admin_user(), "sess_admin")
    fake_db.perizia_analyses.items.append({
        "analysis_id": "analysis_admin", "user_id": "user_admin", "case_id": "case_admin",
        "file_name": "admin-test.pdf", "created_at": "2026-01-01T00:00:00", "status": "COMPLETED",
    })
    resp = await _client_request("GET", "/api/beta/dashboard-summary", token=token)
    assert resp.status_code == 200
    data = resp.json()
    assert data["user"]["email"] == "nexodifyforyou@gmail.com"
    assert data["user"]["is_master_admin"] is True
    assert data["user"]["is_beta_partner"] is False
    assert data["analyses"][0]["analysis_id"] == "analysis_admin"


@pytest.mark.anyio
async def test_beta_dashboard_blocks_normal_user(fake_db):
    token = _seed_session(fake_db, _normal_user(), "sess_norm")
    resp = await _client_request("GET", "/api/beta/dashboard-summary", token=token)
    assert resp.status_code == 403


@pytest.mark.anyio
async def test_user_response_exposes_beta_flag(fake_db):
    token = _seed_session(fake_db, _beta_user(), "sess_beta")
    resp = await _client_request("GET", "/api/auth/me", token=token)
    assert resp.status_code == 200
    assert resp.json()["is_beta_partner"] is True
    assert resp.json()["is_master_admin"] is False
    # Normal user must not be flagged.
    token2 = _seed_session(fake_db, _normal_user(), "sess_norm")
    resp2 = await _client_request("GET", "/api/auth/me", token=token2)
    assert resp2.json()["is_beta_partner"] is False


@pytest.mark.anyio
async def test_user_response_exposes_admin_owner_flag(fake_db):
    token = _seed_session(fake_db, _admin_user(), "sess_admin")
    resp = await _client_request("GET", "/api/auth/me", token=token)
    assert resp.status_code == 200
    data = resp.json()
    assert data["email"] == "nexodifyforyou@gmail.com"
    assert data["is_master_admin"] is True
    assert data["is_beta_partner"] is False
    assert data["plan"] == "enterprise"
