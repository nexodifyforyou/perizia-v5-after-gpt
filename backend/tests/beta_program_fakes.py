"""
Shared async-Mongo fakes + helpers for the beta program test suites.

Synthetic identities only — no real tester email ever appears here.
"""

import os
import re
import sys
from datetime import datetime, timezone, timedelta

import httpx

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import server as server  # noqa: E402
from beta_program import signals as beta_signals  # noqa: E402
from beta_program import store as beta_store  # noqa: E402

OWNER_EMAIL = "nexodifyforyou@gmail.com"
MASTER_EMAIL = "admin@nexodify.com"


def _get_nested(doc, dotted):
    cur = doc
    for part in dotted.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


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


def _match(doc, filt):
    if not filt:
        return True
    for key, value in filt.items():
        if key == "$and":
            if not all(_match(doc, f) for f in value):
                return False
            continue
        if key == "$or":
            if not any(_match(doc, f) for f in value):
                return False
            continue
        doc_val = _get_nested(doc, key) if "." in key else doc.get(key)
        if isinstance(value, dict):
            if "$regex" in value:
                flags = re.I if "i" in value.get("$options", "") else 0
                if not re.search(value["$regex"], str(doc_val or ""), flags):
                    return False
                continue
            if "$in" in value:
                if doc_val not in value["$in"]:
                    return False
                continue
            if "$ne" in value:
                if doc_val == value["$ne"]:
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


def _project(doc, projection):
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


class FakeCollection:
    def __init__(self, name):
        self.name = name
        self.items = []
        self.inserted = []

    async def find_one(self, filt, projection=None, sort=None):
        matches = [d for d in self.items if _match(d, filt)]
        if not matches:
            return None
        return _project(matches[0], projection)

    def find(self, filt, projection=None):
        return FakeCursor([_project(d, projection) for d in self.items if _match(d, filt)])

    async def count_documents(self, filt):
        return len([d for d in self.items if _match(d, filt)])

    async def insert_one(self, doc):
        # Enforce a unique normalized_email for the memberships collection.
        if self.name == "beta_program_memberships":
            email = doc.get("normalized_email")
            if any(d.get("normalized_email") == email for d in self.items):
                raise DuplicateKeyError("dup email")
        self.items.append(doc)
        self.inserted.append(doc)

    async def update_one(self, filt, update, upsert=False):
        matches = [d for d in self.items if _match(d, filt)]
        if not matches:
            if upsert:
                new_doc = dict(update.get("$setOnInsert", {}))
                if "$set" in update:
                    new_doc.update(update["$set"])
                self.items.append(new_doc)
            return
        doc = matches[0]
        if "$set" in update:
            doc.update(update["$set"])
        if "$inc" in update:
            for k, v in update["$inc"].items():
                doc[k] = doc.get(k, 0) + v

    async def create_index(self, keys, **kwargs):
        return None


class DuplicateKeyError(Exception):
    pass


class FakeDB:
    def __init__(self):
        self._collections = {}

    def __getattr__(self, name):
        # Any collection name resolves to a FakeCollection lazily.
        if name.startswith("_"):
            raise AttributeError(name)
        return self[name]

    def __getitem__(self, name):
        if name not in self._collections:
            self._collections[name] = FakeCollection(name)
        return self._collections[name]


class SyncEventsView:
    """Sync view over the async v2_job_events FakeCollection (shared items)."""

    def __init__(self, backing):
        self.backing = backing

    def update_one(self, filt, update, upsert=False):
        eid = filt.get("event_id")
        if any(d.get("event_id") == eid for d in self.backing.items):
            return
        doc = dict(update.get("$setOnInsert", {}))
        self.backing.items.append(doc)

    def create_index(self, *a, **k):
        return None


def install_fake_db(monkeypatch):
    db = FakeDB()
    monkeypatch.setattr(server, "db", db)
    server.MASTER_ADMIN_EMAIL = MASTER_EMAIL
    server.CORRECTNESS_V2_ADMIN_VIEW_EMAIL = OWNER_EMAIL
    monkeypatch.setattr(server, "ADMIN_EMAILS", frozenset({OWNER_EMAIL}))
    monkeypatch.setattr(server, "BETA_UNLIMITED_EMAILS", frozenset())
    monkeypatch.setattr(server, "BETA_PARTNER_NAMES", {})
    beta_store._indexes_ready = False
    # Route telemetry emits into the same fake collection the async reads use.
    beta_signals.reset_for_tests()
    beta_signals.set_events_collection_override(SyncEventsView(db["v2_job_events"]))
    beta_signals._analysis_user_cache.clear()
    return db


def flush_telemetry(timeout: float = 5.0):
    """Drain the bounded telemetry queue so emitted events are observable.

    Emits are asynchronous by contract (they must never block the pipeline), so
    any test asserting on written events must drain first."""
    assert beta_signals.flush(timeout), "telemetry queue did not drain in time"


def teardown_fake(monkeypatch=None):
    beta_signals.reset_for_tests()
    beta_signals.set_events_collection_override(None)


async def client_request(method, path, token=None, **kwargs):
    transport = httpx.ASGITransport(app=server.app)
    headers = kwargs.pop("headers", {})
    if token:
        headers["Authorization"] = f"Bearer {token}"
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.request(method, path, headers=headers, **kwargs)


def seed_session(db, user_doc, token):
    db.users.items.append(user_doc)
    db.user_sessions.items.append({
        "session_token": token,
        "user_id": user_doc["user_id"],
        "expires_at": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat(),
    })
    return token


def owner_user():
    return {"user_id": "user_owner", "email": OWNER_EMAIL, "name": "Owner",
            "plan": "free", "is_master_admin": False, "quota": {}}


def normal_user(email="mario@example.com", user_id="user_norm", plan="solo"):
    return {"user_id": user_id, "email": email, "name": "Mario", "plan": plan,
            "is_master_admin": False,
            "quota": {"perizia_scans_remaining": 28, "image_scans_remaining": 0,
                      "assistant_messages_remaining": 0}}


def now_iso():
    return datetime.now(timezone.utc).isoformat()
