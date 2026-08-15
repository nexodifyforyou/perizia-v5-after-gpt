from __future__ import annotations

import asyncio
import hashlib
import os
from datetime import datetime, timedelta, timezone

from pdf_retention import blob_store, ingest, store, sweep
from pdf_retention.tests.fixtures import isolated_retention  # noqa: F401
from pdf_retention.tests.loop import run


async def _retain(analysis_id: str, user_id: str, payload: bytes = b"ttl pdf"):
    digest = hashlib.sha256(payload).hexdigest()
    await store.database().perizia_analyses.insert_one({
        "analysis_id": analysis_id, "user_id": user_id, "input_sha256": digest,
    })
    result = await ingest.retain_if_consented(
        analysis_id=analysis_id, user_id=user_id, contents=payload,
        input_sha256=digest, consent_requested=True,
    )
    assert result["retained"]
    return await store.database()[store.RECORDS_COLLECTION].find_one(
        {"analysis_id": analysis_id, "user_id": user_id}, {"_id": 0}
    )


def test_not_expired_is_untouched_and_just_expired_is_deleted():
    async def scenario():
        future = await _retain("analysis_ret_future", "ret_test_future")
        due = await _retain("analysis_ret_due", "ret_test_due")
        now = datetime.now(timezone.utc)
        await store.database()[store.RECORDS_COLLECTION].update_one(
            {"retention_id": due["retention_id"], "user_id": due["user_id"]},
            {"$set": {"expires_at": now.isoformat()}},
        )
        counts = await sweep.run_once(now=now)
        assert counts["expired"] == 1
        assert blob_store.object_path(future["encrypted_storage_id"]).exists()
        assert not blob_store.object_path(due["encrypted_storage_id"]).exists()
    run(scenario())


def test_already_deleted_is_idempotent_and_missing_file_is_tombstoned():
    async def scenario():
        record = await _retain("analysis_ret_missing", "ret_test_missing")
        blob_store.delete_ciphertext(record["encrypted_storage_id"])
        await store.database()[store.RECORDS_COLLECTION].update_one(
            {"retention_id": record["retention_id"], "user_id": record["user_id"]},
            {"$set": {"expires_at": datetime.now(timezone.utc).isoformat()}},
        )
        first = await sweep.run_once()
        second = await sweep.run_once()
        assert first["expired"] == 1
        assert second["expired"] == 0
        row = await store.database()[store.RECORDS_COLLECTION].find_one({
            "retention_id": record["retention_id"], "user_id": record["user_id"]
        })
        assert row["deletion_state"] == store.STATE_DELETED
    run(scenario())


def test_row_missing_file_exists_is_reconciled_without_plaintext():
    async def scenario():
        storage_id = blob_store.new_storage_id()
        temp = blob_store.write_encrypted_temp(storage_id, b"orphan ciphertext only")
        final = blob_store.promote_temp(storage_id, temp)
        assert final.exists()
        counts = await sweep.run_once()
        assert counts["orphan_deleted"] == 1
        assert not final.exists()
    run(scenario())


def test_delete_failure_stays_pending_then_retry_succeeds(monkeypatch):
    async def scenario():
        record = await _retain("analysis_ret_retry", "ret_test_retry")
        await store.database()[store.RECORDS_COLLECTION].update_one(
            {"retention_id": record["retention_id"], "user_id": record["user_id"]},
            {"$set": {"expires_at": datetime.now(timezone.utc).isoformat()}},
        )
        real_delete = blob_store.delete_ciphertext
        monkeypatch.setattr(blob_store, "delete_ciphertext", lambda _sid: (_ for _ in ()).throw(OSError("denied")))
        first = await sweep.run_once()
        assert first["failed"] >= 1
        pending = await store.database()[store.RECORDS_COLLECTION].find_one({
            "retention_id": record["retention_id"], "user_id": record["user_id"]
        })
        assert pending["deletion_state"] == store.STATE_DELETE_PENDING
        monkeypatch.setattr(blob_store, "delete_ciphertext", real_delete)
        second = await sweep.run_once()
        assert second["delete_retried"] == 1
        assert not blob_store.object_path(record["encrypted_storage_id"]).exists()
    run(scenario())


def test_simultaneous_cleanup_is_safe_and_idempotent():
    async def scenario():
        record = await _retain("analysis_ret_race", "ret_test_race")
        await store.database()[store.RECORDS_COLLECTION].update_one(
            {"retention_id": record["retention_id"], "user_id": record["user_id"]},
            {"$set": {"expires_at": datetime.now(timezone.utc).isoformat()}},
        )
        results = await asyncio.gather(sweep.run_once(), sweep.run_once())
        row = await store.database()[store.RECORDS_COLLECTION].find_one({
            "retention_id": record["retention_id"], "user_id": record["user_id"]
        })
        assert row["deletion_state"] == store.STATE_DELETED
        assert not blob_store.object_path(record["encrypted_storage_id"]).exists()
        assert sum(item["failed"] for item in results) == 0
    run(scenario())


def test_malformed_and_path_traversal_storage_ids_fail_closed():
    async def scenario():
        for suffix, malicious in (("malformed", "bad"), ("traversal", "../../outside.pdf")):
            record = await _retain(f"analysis_ret_{suffix}", f"ret_test_{suffix}")
            await store.database()[store.RECORDS_COLLECTION].update_one(
                {"retention_id": record["retention_id"], "user_id": record["user_id"]},
                {"$set": {
                    "encrypted_storage_id": malicious,
                    "expires_at": datetime.now(timezone.utc).isoformat(),
                }},
            )
        counts = await sweep.run_once()
        assert counts["failed"] >= 2
        rows = await store.database()[store.RECORDS_COLLECTION].find(
            {"user_id": {"$in": ["ret_test_malformed", "ret_test_traversal"]}}
        ).to_list(length=2)
        assert all(row["deletion_state"] == store.STATE_DELETE_PENDING for row in rows)
    run(scenario())


def test_symlink_escape_and_hardlink_are_never_followed(tmp_path):
    async def scenario():
        record = await _retain("analysis_ret_symlink", "ret_test_symlink")
        path = blob_store.object_path(record["encrypted_storage_id"])
        path.unlink()
        target = tmp_path / "outside_target"
        target.write_bytes(b"outside must survive")
        path.symlink_to(target)
        await store.database()[store.RECORDS_COLLECTION].update_one(
            {"retention_id": record["retention_id"], "user_id": record["user_id"]},
            {"$set": {"expires_at": datetime.now(timezone.utc).isoformat()}},
        )
        result = await sweep.run_once()
        assert result["failed"] >= 1
        assert target.read_bytes() == b"outside must survive"

        hard = await _retain("analysis_ret_hard", "ret_test_hard")
        hard_path = blob_store.object_path(hard["encrypted_storage_id"])
        extra_link = tmp_path / "hardlink_copy"
        os.link(hard_path, extra_link)
        await store.database()[store.RECORDS_COLLECTION].update_one(
            {"retention_id": hard["retention_id"], "user_id": hard["user_id"]},
            {"$set": {"expires_at": datetime.now(timezone.utc).isoformat()}},
        )
        result = await sweep.run_once()
        assert result["failed"] >= 1
        assert hard_path.exists() and extra_link.exists()
    run(scenario())


def test_stale_encrypted_temp_is_removed():
    async def scenario():
        storage_id = blob_store.new_storage_id()
        temp = blob_store.write_encrypted_temp(storage_id, b"encrypted temp")
        old = (datetime.now(timezone.utc) - timedelta(hours=2)).timestamp()
        os.utime(temp, (old, old))
        counts = await sweep.run_once()
        assert counts["temp_deleted"] == 1
        assert not temp.exists()
    run(scenario())
