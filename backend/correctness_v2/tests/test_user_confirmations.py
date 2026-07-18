"""Tests for the MongoDB-backed user-confirmation store (plan Part 24 #21-26)."""

import asyncio
import sys
import types

import pytest

from correctness_v2 import user_confirmations as uc


# ---------------------------------------------------------------------------
# Minimal in-memory async Mongo double (Motor-shaped surface used by the store)
# ---------------------------------------------------------------------------
class FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    async def to_list(self, _n):
        return list(self._rows)


class FakeCollection:
    def __init__(self):
        self.docs = []
        self.indexes = []

    async def create_index(self, keys, **kw):
        self.indexes.append((tuple(keys), kw))
        return kw.get("name", "idx")

    @staticmethod
    def _match(doc, query):
        return all(doc.get(k) == v for k, v in query.items())

    async def find_one(self, query, projection=None):
        for d in self.docs:
            if self._match(d, query):
                return {k: v for k, v in d.items() if k != "_id"}
        return None

    async def update_one(self, query, update, upsert=False):
        setd = update.get("$set", {})
        for d in self.docs:
            if self._match(d, query):
                d.update(setd)
                return
        if upsert:
            newd = dict(query)
            newd.update(setd)
            self.docs.append(newd)

    async def insert_one(self, doc):
        self.docs.append(dict(doc))

    def find(self, query, projection=None):
        rows = [
            {k: v for k, v in d.items() if k != "_id"}
            for d in self.docs
            if self._match(d, query)
        ]
        return FakeCursor(rows)


class FakeDB:
    def __init__(self):
        self.cols = {}

    def __getitem__(self, name):
        return self.cols.setdefault(name, FakeCollection())


@pytest.fixture()
def fake_db(monkeypatch):
    db = FakeDB()
    fake_server = types.ModuleType("server")
    fake_server.db = db
    monkeypatch.setitem(sys.modules, "server", fake_server)
    monkeypatch.setattr(uc, "_indexes_ready", False)
    return db


def _finding(finding_id="mon-abc123", excerpt="valore di vendita 38110"):
    return {
        "finding_id": finding_id,
        "section": "numeri",
        "title": "Importo",
        "evidence": {"page": 8, "excerpt": excerpt, "verbatim": True},
        "page": 8,
        "confirmation": {
            "eligible": True,
            "question": "Q?",
            "options": [
                {"option_id": "costo_acquirente", "label": "Costo a mio carico"},
                {"option_id": "gia_compreso", "label": "Già compreso"},
            ],
            "unsure_option": {"option_id": "non_sicuro", "label": "Non sono sicuro"},
        },
    }


def _submit(**kw):
    defaults = dict(
        analysis_id="analysis_1", lot_id="1", finding=_finding(),
        option_id="costo_acquirente", user_id="owner", report_version="cv2.customer_report.v1",
        job_id="cv2_1",
    )
    defaults.update(kw)
    return asyncio.run(uc.submit(**defaults))


# 21. submit persists full state doc + one audit doc; upsert on unique tuple (not a 2nd row)
def test_submit_persists_state_and_audit(fake_db):
    doc = _submit()
    assert doc["source"] == "USER_CONFIRMED"
    assert doc["status"] == "confermato_utente"
    assert doc["selected_label"] == "Costo a mio carico"
    assert doc["report_version"] == "cv2.customer_report.v1"
    assert doc["page"] == 8 and doc["evidence_hash"]
    state = fake_db["correctness_v2_confirmations"].docs
    audit = fake_db["correctness_v2_confirmation_audit"].docs
    assert len(state) == 1 and len(audit) == 1
    assert audit[0]["action"] == "created" and audit[0]["to_option"] == "costo_acquirente"
    # unique identity index declared
    idx = fake_db["correctness_v2_confirmations"].indexes
    assert any(kw.get("unique") for _keys, kw in idx)


# 22. re-submit same tuple UPDATES (still one row) + appends audit, preserves created_at
def test_resubmit_updates_and_audits(fake_db):
    first = _submit(option_id="costo_acquirente")
    second = _submit(option_id="gia_compreso")
    state = fake_db["correctness_v2_confirmations"].docs
    audit = fake_db["correctness_v2_confirmation_audit"].docs
    assert len(state) == 1  # updated in place, not duplicated
    assert state[0]["selected_option"] == "gia_compreso"
    assert second["created_at"] == first["created_at"]  # created_at preserved
    assert len(audit) == 2  # history preserved (append-only)
    assert audit[1]["action"] == "updated"
    assert audit[1]["from_option"] == "costo_acquirente"
    assert audit[1]["to_option"] == "gia_compreso"


# 23. unoffered option rejected; ineligible finding rejected
def test_invalid_option_and_ineligible_rejected(fake_db):
    with pytest.raises(ValueError):
        _submit(option_id="not_an_option")
    ineligible = _finding()
    ineligible["confirmation"]["eligible"] = False
    with pytest.raises(ValueError):
        _submit(finding=ineligible)
    # nothing persisted on rejection
    assert fake_db["correctness_v2_confirmations"].docs == []


# 24. evidence_hash captured so a later rerun can flag stale (hash changes with excerpt)
def test_evidence_hash_tracks_excerpt(fake_db):
    d1 = _submit(finding=_finding(excerpt="testo A"))
    fake_db["correctness_v2_confirmations"].docs.clear()
    fake_db["correctness_v2_confirmation_audit"].docs.clear()
    monkey = _finding(excerpt="testo B DIVERSO")
    d2 = _submit(finding=monkey, analysis_id="analysis_2")
    assert d1["evidence_hash"] != d2["evidence_hash"]


# 25. non_sicuro is a valid persisted answer with status non_sicuro
def test_non_sicuro_answer(fake_db):
    doc = _submit(option_id="non_sicuro")
    assert doc["status"] == "non_sicuro"
    assert doc["selected_label"] == "Non sono sicuro"


# 26. list_for_analysis returns owner's confirmations; admin list returns all
def test_list_owner_and_admin(fake_db):
    _submit(user_id="owner", finding=_finding("mon-1"))
    _submit(user_id="other", finding=_finding("mon-2"), analysis_id="analysis_1")
    owner_rows = asyncio.run(uc.list_for_analysis("analysis_1", "owner"))
    all_rows = asyncio.run(uc.list_all_for_analysis("analysis_1"))
    assert {r["finding_id"] for r in owner_rows} == {"mon-1"}
    assert {r["finding_id"] for r in all_rows} == {"mon-1", "mon-2"}
    audit = asyncio.run(uc.audit_for_analysis("analysis_1"))
    assert len(audit) == 2
