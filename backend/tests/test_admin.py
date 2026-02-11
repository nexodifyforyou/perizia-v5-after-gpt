import re
import os
import sys
from datetime import datetime, timezone, timedelta

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
            if "$set" in update:
                new_doc.update(update["$set"])
            self.items.append(new_doc)
            return
        doc = matches[0]
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
