import re
import os
import sys
import asyncio
from datetime import datetime, timezone, timedelta
import io

import pytest
import httpx

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import server as server


def _parse_dt(value):
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


class FakeCursor:
    def __init__(self, items):
        self.items = list(items)

    def sort(self, key, direction):
        reverse = direction == -1
        self.items.sort(key=lambda x: x.get(key), reverse=reverse)
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
            doc_val = doc.get(key)
            if isinstance(value, dict):
                if "$regex" in value:
                    pattern = value.get("$regex", "")
                    options = value.get("$options", "")
                    flags = re.I if "i" in options else 0
                    if not re.search(pattern, str(doc_val or ""), flags):
                        return False
                    continue
                if "$in" in value:
                    if doc_val not in value.get("$in", []):
                        return False
                    continue
                if "$gte" in value or "$lte" in value:
                    gte = value.get("$gte")
                    lte = value.get("$lte")
                    doc_dt = _parse_dt(doc_val)
                    gte_dt = _parse_dt(gte)
                    lte_dt = _parse_dt(lte)
                    if doc_dt and (gte_dt or lte_dt):
                        if gte_dt and doc_dt < gte_dt:
                            return False
                        if lte_dt and doc_dt > lte_dt:
                            return False
                    else:
                        if gte is not None and doc_val < gte:
                            return False
                        if lte is not None and doc_val > lte:
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
            return {k: doc.get(k) for k in include if k in doc}
        return {k: v for k, v in doc.items() if k not in exclude}

    async def find_one(self, filt, projection=None, sort=None):
        matches = [d for d in self.items if self._match(d, filt)]
        if sort:
            key, direction = sort[0]
            reverse = direction == -1
            matches.sort(key=lambda x: x.get(key), reverse=reverse)
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
            if "$setOnInsert" in update:
                new_doc.update(update["$setOnInsert"])
            if "$set" in update:
                new_doc.update(update["$set"])
            self.items.append(new_doc)
            return
        doc = matches[0]
        if "$setOnInsert" in update:
            pass
        if "$set" in update:
            doc.update(update["$set"])
        if "$inc" in update:
            for key, val in update["$inc"].items():
                parts = key.split(".")
                if len(parts) == 2:
                    parent = doc.setdefault(parts[0], {})
                    parent[parts[1]] = parent.get(parts[1], 0) + val
                else:
                    doc[key] = doc.get(key, 0) + val

    def aggregate(self, pipeline):
        data = list(self.items)
        for stage in pipeline:
            if "$match" in stage:
                data = [d for d in data if self._match(d, stage["$match"])]
            if "$group" in stage:
                group_spec = stage["$group"]
                grouped = {}
                for d in data:
                    group_key = group_spec.get("_id")
                    if isinstance(group_key, str) and group_key.startswith("$"):
                        key_val = d.get(group_key[1:])
                    else:
                        key_val = group_key
                    group = grouped.setdefault(key_val, {"_id": key_val})
                    if "count" in group_spec:
                        group["count"] = group.get("count", 0) + 1
                    if "last_active" in group_spec:
                        current = group.get("last_active")
                        candidate = d.get("created_at")
                        if current is None:
                            group["last_active"] = candidate
                        else:
                            cur_dt = _parse_dt(current)
                            cand_dt = _parse_dt(candidate)
                            if cur_dt and cand_dt and cand_dt > cur_dt:
                                group["last_active"] = candidate
                    if "total" in group_spec:
                        group["total"] = group.get("total", 0) + float(d.get("amount", 0) or 0)
                data = list(grouped.values())
            if "$sort" in stage:
                sort_key, direction = list(stage["$sort"].items())[0]
                reverse = direction == -1
                data.sort(key=lambda x: x.get(sort_key), reverse=reverse)
            if "$limit" in stage:
                data = data[:stage["$limit"]]
        return FakeCursor(data)

    async def distinct(self, field, filt=None):
        filt = filt or {}
        values = set()
        for d in self.items:
            if self._match(d, filt):
                values.add(d.get(field))
        return list(values)

    async def create_index(self, field):
        return None


class FakeDB:
    def __init__(self):
        self.users = FakeCollection("users")
        self.user_sessions = FakeCollection("user_sessions")
        self.perizia_analyses = FakeCollection("perizia_analyses")
        self.image_forensics = FakeCollection("image_forensics")
        self.assistant_qa = FakeCollection("assistant_qa")
        self.payment_transactions = FakeCollection("payment_transactions")
        self.credit_ledger = FakeCollection("credit_ledger")
        self.billing_records = FakeCollection("billing_records")
        self.admin_audit_log = FakeCollection("admin_audit_log")
        self.admin_user_notes = FakeCollection("admin_user_notes")


@pytest.fixture()
def fake_db(monkeypatch):
    fake_db = FakeDB()
    monkeypatch.setattr(server, "db", fake_db)
    server.MASTER_ADMIN_EMAIL = "admin@nexodify.com"
    return fake_db


def _seed_session(fake_db, user_doc, session_token="sess_test"):
    fake_db.users.items.append(user_doc)
    fake_db.user_sessions.items.append({
        "session_token": session_token,
        "user_id": user_doc["user_id"],
        "expires_at": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    })
    return session_token


@pytest.mark.anyio
async def test_admin_overview_unauthenticated(fake_db):
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/admin/overview")
    assert response.status_code == 401


@pytest.mark.anyio
async def test_admin_overview_forbidden(fake_db):
    session_token = _seed_session(fake_db, {
        "user_id": "user_1",
        "email": "user@example.com",
        "name": "User",
        "plan": "free",
        "is_master_admin": False,
        "quota": {}
    })
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/admin/overview", headers={"Authorization": f"Bearer {session_token}"})
    assert response.status_code == 403


@pytest.mark.anyio
async def test_admin_overview_master_admin(fake_db):
    session_token = _seed_session(fake_db, {
        "user_id": "user_admin",
        "email": "admin@nexodify.com",
        "name": "Admin",
        "plan": "enterprise",
        "is_master_admin": True,
        "quota": {}
    })
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/admin/overview", headers={"Authorization": f"Bearer {session_token}"})
    assert response.status_code == 200
    data = response.json()
    assert "totals" in data
    assert "plan_counts" in data
    assert "last_30d" in data
    assert "top_users_30d" in data


@pytest.mark.anyio
async def test_admin_overview_includes_ledger_and_billing_summaries(fake_db):
    session_token = _seed_session(fake_db, {
        "user_id": "user_admin",
        "email": "admin@nexodify.com",
        "name": "Admin",
        "plan": "enterprise",
        "is_master_admin": True,
        "quota": {}
    })
    fake_db.credit_ledger.items.extend([
        {
            "ledger_id": "ledger_1",
            "user_id": "user_a",
            "user_email": "a@example.com",
            "quota_field": "perizia_scans_remaining",
            "direction": "debit",
            "amount": 4,
            "entry_type": "perizia_upload",
            "description_it": "Addebito perizia",
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
        {
            "ledger_id": "ledger_2",
            "user_id": "user_b",
            "user_email": "b@example.com",
            "quota_field": "image_scans_remaining",
            "direction": "debit",
            "amount": 2,
            "entry_type": "image_forensics",
            "description_it": "Addebito immagini",
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
    ])
    fake_db.billing_records.items.extend([
        {
            "billing_record_id": "bill_1",
            "user_id": "user_a",
            "user_email": "a@example.com",
            "plan_id": "pro",
            "purchase_type": "subscription",
            "amount_total": 29.0,
            "currency": "eur",
            "status": "paid",
            "description_it": "Acquisto piano Pro",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "paid_at": datetime.now(timezone.utc).isoformat(),
        },
        {
            "billing_record_id": "bill_2",
            "user_id": "user_b",
            "user_email": "b@example.com",
            "plan_id": "solo",
            "purchase_type": "pack",
            "amount_total": 19.0,
            "currency": "eur",
            "status": "pending",
            "description_it": "Pack crediti",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "paid_at": None,
        },
    ])

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/admin/overview", headers={"Authorization": f"Bearer {session_token}"})

    assert response.status_code == 200
    data = response.json()
    assert data["credit_ledger_30d"]["total_debits"] == 6
    assert data["credit_ledger_30d"]["perizia_scans_remaining"] == 4
    assert data["billing_records_30d"]["status_counts"]["paid"] == 1
    assert data["billing_records_30d"]["status_counts"]["pending"] == 1
    assert len(data["latest_credit_movements"]) == 2
    assert len(data["latest_billing_activity"]) == 2


@pytest.mark.anyio
async def test_admin_patch_user_writes_audit_log(fake_db):
    session_token = _seed_session(fake_db, {
        "user_id": "user_admin",
        "email": "admin@nexodify.com",
        "name": "Admin",
        "plan": "enterprise",
        "is_master_admin": True,
        "quota": {}
    })
    fake_db.users.items.append({
        "user_id": "user_target",
        "email": "target@example.com",
        "name": "Target",
        "plan": "free",
        "quota": {"perizia_scans_remaining": 1}
    })
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.patch(
            "/api/admin/users/user_target",
            json={"plan": "pro"},
            headers={"Authorization": f"Bearer {session_token}"}
        )
    assert response.status_code == 200
    actions = [a.get("action") for a in fake_db.admin_audit_log.inserted]
    assert "USER_SET_PLAN" in actions


@pytest.mark.anyio
async def test_admin_user_financial_summaries_and_detail_endpoints(fake_db):
    session_token = _seed_session(fake_db, {
        "user_id": "user_admin",
        "email": "admin@nexodify.com",
        "name": "Admin",
        "plan": "enterprise",
        "is_master_admin": True,
        "quota": {}
    })
    fake_db.users.items.append({
        "user_id": "user_target",
        "email": "target@example.com",
        "name": "Target",
        "plan": "pro",
        "quota": {
            "perizia_scans_remaining": 7,
            "image_scans_remaining": 1,
            "assistant_messages_remaining": 3,
        },
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
    fake_db.credit_ledger.items.extend([
        {
            "ledger_id": "ledger_old",
            "user_id": "user_target",
            "user_email": "target@example.com",
            "quota_field": "perizia_scans_remaining",
            "direction": "credit",
            "amount": 10,
            "balance_before": 0,
            "balance_after": 10,
            "entry_type": "plan_purchase",
            "reference_type": "checkout_session",
            "reference_id": "sess_old",
            "description_it": "Accredito crediti",
            "metadata": {},
            "created_at": "2026-03-10T10:00:00+00:00",
        },
        {
            "ledger_id": "ledger_new",
            "user_id": "user_target",
            "user_email": "target@example.com",
            "quota_field": "perizia_scans_remaining",
            "direction": "debit",
            "amount": 3,
            "balance_before": 10,
            "balance_after": 7,
            "entry_type": "perizia_upload",
            "reference_type": "analysis",
            "reference_id": "analysis_1",
            "description_it": "Addebito perizia",
            "metadata": {"pages_count": 24},
            "created_at": "2026-03-12T10:00:00+00:00",
        },
    ])
    fake_db.billing_records.items.extend([
        {
            "billing_record_id": "bill_old",
            "user_id": "user_target",
            "user_email": "target@example.com",
            "customer_type": "individual",
            "customer_name": "Target",
            "billing_email": "target@example.com",
            "country_code": "IT",
            "plan_id": "pro",
            "purchase_type": "subscription",
            "amount_subtotal": 29.0,
            "amount_tax": 0.0,
            "amount_total": 29.0,
            "currency": "eur",
            "status": "paid",
            "payment_provider": "manual",
            "invoice_status": "ready",
            "description_it": "Acquisto piano",
            "metadata": {},
            "created_at": "2026-03-11T10:00:00+00:00",
            "updated_at": "2026-03-11T10:00:00+00:00",
            "paid_at": "2026-03-11T10:00:00+00:00",
        },
        {
            "billing_record_id": "bill_new",
            "user_id": "user_target",
            "user_email": "target@example.com",
            "customer_type": "individual",
            "customer_name": "Target",
            "billing_email": "target@example.com",
            "country_code": "IT",
            "plan_id": "solo",
            "purchase_type": "pack",
            "amount_subtotal": 19.0,
            "amount_tax": 0.0,
            "amount_total": 19.0,
            "currency": "eur",
            "status": "pending",
            "payment_provider": "manual",
            "invoice_status": "pending",
            "description_it": "Pack crediti",
            "metadata": {},
            "created_at": "2026-03-13T10:00:00+00:00",
            "updated_at": "2026-03-13T10:00:00+00:00",
            "paid_at": None,
        },
    ])

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        users_response = await client.get("/api/admin/users", headers={"Authorization": f"Bearer {session_token}"})
        detail_response = await client.get("/api/admin/users/user_target", headers={"Authorization": f"Bearer {session_token}"})
        ledger_response = await client.get("/api/admin/users/user_target/ledger?limit=10", headers={"Authorization": f"Bearer {session_token}"})
        billing_response = await client.get("/api/admin/users/user_target/billing-records?limit=10", headers={"Authorization": f"Bearer {session_token}"})

    assert users_response.status_code == 200
    users_payload = users_response.json()
    target_user = next(item for item in users_payload["items"] if item["user_id"] == "user_target")
    assert target_user["financial_summary"]["latest_credit_movement_at"] == "2026-03-12T10:00:00+00:00"
    assert target_user["financial_summary"]["latest_billing_status"] == "pending"
    assert target_user["financial_summary"]["billing_records_count"] == 2
    assert target_user["financial_summary"]["latest_purchase_type"] == "pack"

    assert detail_response.status_code == 200
    assert detail_response.json()["user"]["financial_summary"]["latest_billing_status"] == "pending"

    assert ledger_response.status_code == 200
    assert ledger_response.json()["total"] == 2
    assert ledger_response.json()["entries"][0]["ledger_id"] == "ledger_new"

    assert billing_response.status_code == 200
    assert billing_response.json()["total"] == 2
    assert billing_response.json()["records"][0]["billing_record_id"] == "bill_new"


@pytest.mark.anyio
async def test_opening_balance_baseline_created_once_and_ledger_endpoint_returns_entries(fake_db):
    session_token = _seed_session(fake_db, {
        "user_id": "user_baseline",
        "email": "baseline@example.com",
        "name": "Baseline",
        "plan": "pro",
        "is_master_admin": False,
        "quota": {
            "perizia_scans_remaining": 12,
            "image_scans_remaining": 2,
            "assistant_messages_remaining": 3,
        },
    })
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        first = await client.get("/api/auth/me", headers={"Authorization": f"Bearer {session_token}"})
        second = await client.get("/api/auth/me", headers={"Authorization": f"Bearer {session_token}"})
        ledger = await client.get("/api/billing/ledger?limit=10", headers={"Authorization": f"Bearer {session_token}"})

    assert first.status_code == 200
    assert second.status_code == 200
    assert ledger.status_code == 200
    assert len(fake_db.credit_ledger.items) == 3
    assert {item["quota_field"] for item in fake_db.credit_ledger.items} == {
        "perizia_scans_remaining",
        "image_scans_remaining",
        "assistant_messages_remaining",
    }
    assert all(item["entry_type"] == "opening_balance" for item in fake_db.credit_ledger.items)
    payload = ledger.json()
    assert payload["total"] == 3
    assert len(payload["entries"]) == 3


@pytest.mark.anyio
async def test_admin_patch_user_quota_writes_ledger_entries(fake_db):
    session_token = _seed_session(fake_db, {
        "user_id": "user_admin",
        "email": "admin@nexodify.com",
        "name": "Admin",
        "plan": "enterprise",
        "is_master_admin": True,
        "quota": {},
    })
    fake_db.users.items.append({
        "user_id": "user_target",
        "email": "target@example.com",
        "name": "Target",
        "plan": "free",
        "quota": {
            "perizia_scans_remaining": 12,
            "image_scans_remaining": 0,
            "assistant_messages_remaining": 0,
        },
    })

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.patch(
            "/api/admin/users/user_target",
            json={
                "quota": {
                    "perizia_scans_remaining": 9,
                    "image_scans_remaining": 2,
                }
            },
            headers={"Authorization": f"Bearer {session_token}"},
        )

    assert response.status_code == 200
    admin_entries = [item for item in fake_db.credit_ledger.items if item["entry_type"] == "admin_adjustment"]
    assert len(admin_entries) == 2
    assert {item["quota_field"] for item in admin_entries} == {"perizia_scans_remaining", "image_scans_remaining"}
    directions = {item["quota_field"]: item["direction"] for item in admin_entries}
    assert directions["perizia_scans_remaining"] == "debit"
    assert directions["image_scans_remaining"] == "credit"


@pytest.mark.anyio
async def test_quota_decrement_skipped_for_master_admin(fake_db, monkeypatch):

    async def fake_openai(*args, **kwargs):
        return '{"answer_it":"ok","answer_en":"ok","confidence":"LOW","sources":[],"needs_more_info":"NO","missing_inputs":[],"out_of_scope":false,"safe_disclaimer_it":"x","safe_disclaimer_en":"x","qa_pass":{"status":"PASS","reason":"ok"}}'

    monkeypatch.setattr(server, "openai_chat_completion", fake_openai)

    session_token = _seed_session(fake_db, {
        "user_id": "user_admin",
        "email": "admin@nexodify.com",
        "name": "Admin",
        "plan": "enterprise",
        "is_master_admin": True,
        "quota": {"assistant_messages_remaining": 10}
    })

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/analysis/assistant",
            json={"question": "Test question"},
            headers={"Authorization": f"Bearer {session_token}"}
        )
    assert response.status_code == 200
    assert not any("$inc" in call for call in fake_db.users.update_calls)
    assert not any(item["entry_type"] == "assistant_message" for item in fake_db.credit_ledger.items)


@pytest.mark.anyio
async def test_master_admin_auth_baseline_keeps_enterprise_identity_and_quota(fake_db):
    session_token = _seed_session(fake_db, {
        "user_id": "user_admin",
        "email": "admin@nexodify.com",
        "name": "Admin",
        "plan": "enterprise",
        "is_master_admin": True,
        "quota": {
            "perizia_scans_remaining": 9999,
            "image_scans_remaining": 9999,
            "assistant_messages_remaining": 9999,
        },
    })

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/auth/me", headers={"Authorization": f"Bearer {session_token}"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["is_master_admin"] is True
    assert payload["plan"] == "enterprise"
    assert payload["quota"] == {
        "perizia_scans_remaining": 9999,
        "image_scans_remaining": 9999,
        "assistant_messages_remaining": 9999,
    }
    opening_entries = [item for item in fake_db.credit_ledger.items if item["entry_type"] == "opening_balance"]
    assert len(opening_entries) == 3
    assert not any(item["entry_type"] in {"assistant_message", "image_forensics", "perizia_upload"} for item in fake_db.credit_ledger.items)


def _make_pdf_bytes(page_count: int) -> bytes:
    writer = server.PdfWriter()
    for _ in range(page_count):
        writer.add_blank_page(width=612, height=792)
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


def _stub_perizia_pipeline(monkeypatch):
    monkeypatch.setattr(server, "_build_step1_extract_payload", lambda contents: {"document_quality": {}, "extraction_summary": {}})
    monkeypatch.setattr(
        server,
        "_extract_pdf_text_digital",
        lambda contents: {
            "success": True,
            "pages": [{"page_number": 1, "text": "testo sufficiente", "tables": [], "form_fields": [], "char_count": 17}],
            "full_text": "testo sufficiente",
            "total_pages": 1,
            "covered_pages": 1,
            "coverage_ratio": 1.0,
            "blank_pages": 0,
            "blank_ratio": 0.0,
            "error": None,
        },
    )
    monkeypatch.setattr(server, "create_fallback_analysis", lambda *args, **kwargs: {"analysis_status": "COMPLETED", "debug": {}})
    monkeypatch.setattr(server, "_normalize_legal_killers", lambda *args, **kwargs: None)
    monkeypatch.setattr(server, "_apply_headline_field_states", lambda *args, **kwargs: None)
    monkeypatch.setattr(server, "_apply_decision_field_states", lambda *args, **kwargs: None)
    monkeypatch.setattr(server, "_apply_market_ranges_to_money_box", lambda *args, **kwargs: None)
    monkeypatch.setattr(server, "_normalize_evidence_offsets", lambda *args, **kwargs: None)
    monkeypatch.setattr(server, "_build_panoramica_contract", lambda *args, **kwargs: {})
    monkeypatch.setattr(server, "_apply_unreadable_hard_stop", lambda *args, **kwargs: None)
    monkeypatch.setattr(server, "_write_extraction_pack", lambda *args, **kwargs: None)
    monkeypatch.setattr(server, "run_candidate_miner_for_analysis", lambda *args, **kwargs: {"money_count": 0, "date_count": 0, "trigger_count": 0, "low_quality_pages": [], "candidates_folder": "/tmp"})
    monkeypatch.setattr(server, "build_estratto_quality", lambda *args, **kwargs: {"sections": [], "build_meta": {}})
    monkeypatch.setattr(server, "_sanitize_lot_conservative_outputs", lambda *args, **kwargs: None)
    monkeypatch.setattr(server, "_build_user_messages", lambda *args, **kwargs: [])

    async def _fake_summary(*args, **kwargs):
        return None

    async def _fake_narration(*args, **kwargs):
        return None, {"status": "disabled", "enabled": False}

    monkeypatch.setattr(server, "_enrich_summary_with_optional_llm", _fake_summary)
    monkeypatch.setattr(server, "build_decisione_rapida_narration", _fake_narration)
    monkeypatch.setattr(server, "_build_case_aware_narration_payload", lambda *args, **kwargs: None)


def test_required_perizia_credits_bands():
    assert server._get_required_perizia_credits(1) == 4
    assert server._get_required_perizia_credits(20) == 4
    assert server._get_required_perizia_credits(21) == 7
    assert server._get_required_perizia_credits(40) == 7
    assert server._get_required_perizia_credits(41) == 10
    assert server._get_required_perizia_credits(60) == 10
    assert server._get_required_perizia_credits(61) == 13
    assert server._get_required_perizia_credits(80) == 13
    assert server._get_required_perizia_credits(81) == 16
    assert server._get_required_perizia_credits(100) == 16
    assert server._get_required_perizia_credits(101) is None


@pytest.mark.anyio
async def test_perizia_upload_blocks_when_band_credits_are_insufficient(fake_db, monkeypatch):
    class FakePdfReader:
        def __init__(self, *args, **kwargs):
            self.pages = [object()] * 80

    monkeypatch.setattr(server, "PdfReader", FakePdfReader)
    session_token = _seed_session(fake_db, {
        "user_id": "user_free",
        "email": "user@example.com",
        "name": "User",
        "plan": "free",
        "is_master_admin": False,
        "quota": {"perizia_scans_remaining": 4, "image_scans_remaining": 0, "assistant_messages_remaining": 0}
    })

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/analysis/perizia",
            files={"file": ("perizia_80p.pdf", b"%PDF-1.4 test", "application/pdf")},
            headers={"Authorization": f"Bearer {session_token}"}
        )

    assert response.status_code == 403
    payload = response.json()["detail"]
    assert payload["code"] == "INSUFFICIENT_PERIZIA_CREDITS"
    assert payload["required_credits"] == 13
    assert payload["remaining_credits"] == 4
    assert payload["pages_count"] == 80
    assert "Crediti insufficienti" in payload["message_it"]
    assert fake_db.perizia_analyses.inserted == []
    assert not any("quota.perizia_scans_remaining" in call.get("$inc", {}) for call in fake_db.users.update_calls)
    assert not any(item["entry_type"] == "perizia_upload" for item in fake_db.credit_ledger.items)
    user_doc = next(item for item in fake_db.users.items if item["user_id"] == "user_free")
    assert user_doc["quota"]["perizia_scans_remaining"] == 4


def test_perizia_upload_consumes_exact_band_credits_on_success(fake_db):
    async def _run():
        user_doc = {
            "user_id": "user_paid",
            "email": "paid@example.com",
            "name": "Paid",
            "plan": "starter",
            "is_master_admin": False,
            "quota": {
                "perizia_scans_remaining": 20,
                "image_scans_remaining": 0,
                "assistant_messages_remaining": 0,
            },
        }
        fake_db.users.items.append(dict(user_doc))
        user = server.User(**user_doc)

        applied = await server._apply_quota_debit_with_ledger(
            user,
            field="perizia_scans_remaining",
            amount=13,
            entry_type="perizia_upload",
            reference_type="analysis",
            reference_id="analysis_test_123",
            description_it="Addebito crediti per analisi perizia completata",
            metadata={
                "analysis_id": "analysis_test_123",
                "pages_count": 80,
                "required_credits": 13,
            },
        )

        assert applied is True
        inc_calls = [call for call in fake_db.users.update_calls if "quota.perizia_scans_remaining" in call.get("$inc", {})]
        assert inc_calls
        assert inc_calls[-1]["$inc"]["quota.perizia_scans_remaining"] == -13
        stored_user = next(item for item in fake_db.users.items if item["user_id"] == "user_paid")
        assert stored_user["quota"]["perizia_scans_remaining"] == 7
        ledger_entries = [item for item in fake_db.credit_ledger.items if item["entry_type"] == "perizia_upload"]
        assert len(ledger_entries) == 1
        assert ledger_entries[0]["quota_field"] == "perizia_scans_remaining"
        assert ledger_entries[0]["direction"] == "debit"
        assert ledger_entries[0]["amount"] == 13
        assert ledger_entries[0]["balance_before"] == 20
        assert ledger_entries[0]["balance_after"] == 7

    asyncio.run(_run())
