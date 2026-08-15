from __future__ import annotations

import hashlib

from pdf_retention import blob_store, ingest, store
from pdf_retention.erasure import erase_all_for_user, erase_for_analysis
from pdf_retention.tests.fixtures import isolated_retention  # noqa: F401
from pdf_retention.tests.loop import run


async def _analysis(analysis_id: str, user_id: str, payload: bytes):
    await store.database().perizia_analyses.insert_one({
        "analysis_id": analysis_id,
        "user_id": user_id,
        "input_sha256": hashlib.sha256(payload).hexdigest(),
        "result": {},
    })


async def _retain(analysis_id="analysis_ret_a", user_id="ret_test_a", payload=b"%PDF retained marker"):
    await _analysis(analysis_id, user_id, payload)
    result = await ingest.retain_if_consented(
        analysis_id=analysis_id, user_id=user_id, contents=payload,
        input_sha256=hashlib.sha256(payload).hexdigest(), consent_requested=True,
    )
    record = await store.database()[store.RECORDS_COLLECTION].find_one(
        {"analysis_id": analysis_id, "user_id": user_id}, {"_id": 0}
    )
    return result, record


def test_consent_upload_creates_only_encrypted_private_file():
    async def scenario():
        payload = b"%PDF-1.7\nPLAINTEXT_AT_REST_PROBE_72a1\n%%EOF"
        result, record = await _retain(payload=payload)
        assert result["retained"] is True
        assert record["deletion_state"] == store.STATE_ACTIVE
        assert record["input_sha256"] == hashlib.sha256(payload).hexdigest()
        assert record["consent_version"] == "DIAGNOSTIC_RETENTION_V1"
        assert record["retention_days_at_consent"] == 30
        path = blob_store.object_path(record["encrypted_storage_id"])
        disk = path.read_bytes()
        assert disk != payload
        assert b"PLAINTEXT_AT_REST_PROBE_72a1" not in disk
        assert path.stat().st_mode & 0o777 == 0o600
    run(scenario())


def test_no_consent_and_flag_off_make_no_retention_writes(monkeypatch):
    async def scenario():
        payload = b"no consent"
        await _analysis("analysis_ret_none", "ret_test_none", payload)
        result = await ingest.retain_if_consented(
            analysis_id="analysis_ret_none", user_id="ret_test_none", contents=payload,
            input_sha256=hashlib.sha256(payload).hexdigest(), consent_requested=False,
        )
        assert result == {"attempted": False, "retained": False}
        monkeypatch.setenv("CORRECTNESS_V2_PDF_RETENTION_ENABLED", "false")
        result = await ingest.retain_if_consented(
            analysis_id="analysis_ret_none", user_id="ret_test_none", contents=payload,
            input_sha256=hashlib.sha256(payload).hexdigest(), consent_requested=True,
        )
        assert result == {"attempted": False, "retained": False}
        assert await store.database()[store.RECORDS_COLLECTION].count_documents({}) == 0
        assert not blob_store.config.retention_root().exists()
    run(scenario())


def test_failed_processing_without_durable_analysis_is_not_retained():
    async def scenario():
        payload = b"failed pipeline"
        result = await ingest.retain_if_consented(
            analysis_id="analysis_ret_failed", user_id="ret_test_failed", contents=payload,
            input_sha256=hashlib.sha256(payload).hexdigest(), consent_requested=True,
        )
        assert result["failure_code"] == "ANALYSIS_NOT_DURABLE"
        assert await store.database()[store.RECORDS_COLLECTION].count_documents({}) == 0
    run(scenario())


def test_storage_failure_keeps_analysis_and_never_plaintext_fallback(monkeypatch):
    async def scenario():
        payload = b"PLAINTEXT_FAILURE_PROBE_33dd"
        await _analysis("analysis_ret_io", "ret_test_io", payload)
        monkeypatch.setattr(blob_store, "write_encrypted_temp", lambda *_a, **_k: (_ for _ in ()).throw(OSError("disk")))
        result = await ingest.retain_if_consented(
            analysis_id="analysis_ret_io", user_id="ret_test_io", contents=payload,
            input_sha256=hashlib.sha256(payload).hexdigest(), consent_requested=True,
        )
        assert result == {"attempted": True, "retained": False, "failure_code": "STORAGE_IO_FAILED"}
        assert await store.database().perizia_analyses.count_documents({
            "analysis_id": "analysis_ret_io", "user_id": "ret_test_io"
        }) == 1
        root = blob_store.config.retention_root()
        if root.exists():
            assert all(payload not in path.read_bytes() for path in root.rglob("*") if path.is_file())
    run(scenario())


def test_ttl_is_computed_once_and_future_default_does_not_extend(monkeypatch):
    async def scenario():
        _result, record = await _retain("analysis_ret_ttl", "ret_test_ttl", b"ttl")
        expires = record["expires_at"]
        monkeypatch.setenv("CORRECTNESS_V2_PDF_RETENTION_DAYS", "90")
        persisted = await store.database()[store.RECORDS_COLLECTION].find_one({
            "analysis_id": "analysis_ret_ttl", "user_id": "ret_test_ttl"
        })
        assert persisted["expires_at"] == expires
        assert persisted["retention_days_at_consent"] == 30
    run(scenario())


def test_customer_erasure_removes_bytes_tombstones_and_is_idempotent():
    async def scenario():
        _result, record = await _retain("analysis_ret_delete", "ret_test_delete", b"delete me")
        path = blob_store.object_path(record["encrypted_storage_id"])
        assert path.exists()
        first = await erase_for_analysis(
            "analysis_ret_delete", user_id="ret_test_delete",
            reason="ANALYSIS_DELETED", actor_type="CUSTOMER",
        )
        second = await erase_for_analysis(
            "analysis_ret_delete", user_id="ret_test_delete",
            reason="ANALYSIS_DELETED", actor_type="CUSTOMER",
        )
        assert first["deleted"] and second["deleted"]
        assert not path.exists()
        tombstone = await store.database()[store.RECORDS_COLLECTION].find_one({
            "analysis_id": "analysis_ret_delete", "user_id": "ret_test_delete"
        })
        assert tombstone["deletion_state"] == store.STATE_DELETED
        assert tombstone["wrapped_dek_b64"] is None
    run(scenario())


def test_erase_for_analysis_lookup_fault_is_reported_not_raised(monkeypatch):
    """A previously-unguarded internal fault (e.g. a transient Mongo error) on
    the very first record lookup must degrade to a safe, retryable failure
    dict -- never propagate as a raw exception into an unrelated caller such
    as the existing delete-analysis/delete-all-history endpoints (server.py).
    """
    async def scenario():
        await _retain("analysis_ret_lookup_fault", "ret_test_lookup_fault", b"lookup fault")
        # Motor hands back a fresh collection wrapper on every ``db[name]``
        # lookup, so the patch must land on the shared class, not on one
        # particular instance, to be observed by the code under test.
        collection_cls = type(store.database()[store.RECORDS_COLLECTION])

        async def _boom(*_a, **_k):
            raise RuntimeError("injected transient lookup fault")

        monkeypatch.setattr(collection_cls, "find_one", _boom)
        result = await erase_for_analysis(
            "analysis_ret_lookup_fault", user_id="ret_test_lookup_fault",
            reason="ANALYSIS_DELETED", actor_type="CUSTOMER",
        )
        assert result["deleted"] is False
        assert result["existed"] is True
        assert result.get("failure_code")
    run(scenario())


def test_erase_all_for_user_lookup_fault_is_reported_not_raised(monkeypatch):
    async def scenario():
        await _retain("analysis_ret_bulk_fault", "ret_test_bulk_fault", b"bulk fault")
        collection_cls = type(store.database()[store.RECORDS_COLLECTION])

        def _boom_find(*_a, **_k):
            raise RuntimeError("injected transient bulk lookup fault")

        monkeypatch.setattr(collection_cls, "find", _boom_find)
        result = await erase_all_for_user(
            "ret_test_bulk_fault", reason="HISTORY_DELETED", actor_type="CUSTOMER"
        )
        assert result["deleted"] is False
        assert result["failures"]
    run(scenario())


def test_delete_all_is_tenant_scoped():
    async def scenario():
        _a, record_a = await _retain("analysis_ret_all_a", "ret_test_all_a", b"A")
        _b, record_b = await _retain("analysis_ret_all_b", "ret_test_all_b", b"B")
        result = await erase_all_for_user(
            "ret_test_all_a", reason="HISTORY_DELETED", actor_type="CUSTOMER"
        )
        assert result["deleted"] is True
        assert not blob_store.object_path(record_a["encrypted_storage_id"]).exists()
        assert blob_store.object_path(record_b["encrypted_storage_id"]).exists()
        other = await store.database()[store.RECORDS_COLLECTION].find_one({
            "analysis_id": "analysis_ret_all_b", "user_id": "ret_test_all_b"
        })
        assert other["deletion_state"] == store.STATE_ACTIVE
    run(scenario())
