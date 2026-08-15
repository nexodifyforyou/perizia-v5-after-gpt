"""Standalone TTL cleanup and crash-state reconciliation."""

from __future__ import annotations

import asyncio
import logging
import os
import stat
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from pymongo import ReturnDocument

from . import blob_store, erasure, store

logger = logging.getLogger(__name__)
DEFAULT_LIMIT = 500
DEFAULT_STALE_TEMP_SECONDS = 3600


def _timestamp(now: Optional[datetime]) -> str:
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc).isoformat()


def _safe_unlink_orphan(path: Path) -> bool:
    st = path.lstat()
    if not stat.S_ISREG(st.st_mode) or stat.S_ISLNK(st.st_mode) or st.st_nlink != 1:
        raise blob_store.UnsafeStoragePath("UNSAFE_ORPHAN_OBJECT")
    path.unlink()
    return True


async def _reconcile_create_pending(record: Dict[str, Any]) -> str:
    db = store.database()
    user_id = str(record.get("user_id") or "")
    analysis_id = str(record.get("analysis_id") or "")
    retention_id = str(record.get("retention_id") or "")
    try:
        exists = await asyncio.to_thread(
            blob_store.ciphertext_exists, record.get("encrypted_storage_id")
        )
    except Exception:
        exists = False
    if exists:
        consent = await db[store.CONSENTS_COLLECTION].find_one(
            {
                "analysis_id": analysis_id,
                "user_id": user_id,
                "status": "GRANTED",
                "consent_version": record.get("consent_version"),
            },
            {"_id": 1},
        )
        analysis = await db.perizia_analyses.find_one(
            {
                "analysis_id": analysis_id,
                "user_id": user_id,
                "input_sha256": record.get("input_sha256"),
            },
            {"_id": 1},
        )
        if consent and analysis:
            activated = await db[store.RECORDS_COLLECTION].find_one_and_update(
                {
                    "retention_id": retention_id,
                    "user_id": user_id,
                    "deletion_state": store.STATE_CREATE_PENDING,
                },
                {"$set": {"deletion_state": store.STATE_ACTIVE, "activated_at": store.now_iso()}},
                return_document=ReturnDocument.AFTER,
            )
            if activated:
                await store.set_consent_retention_status(analysis_id, user_id, "RETAINED")
                await store.audit_event(
                    store.EVENT_RECONCILED,
                    retention_id=retention_id,
                    analysis_id=analysis_id,
                    user_id=user_id,
                    actor_type="SYSTEM_SWEEP",
                    reconciliation_code="CREATE_PENDING_FILE_EXISTS_ACTIVATED",
                )
                return "activated"
    result = await erasure.erase_for_analysis(
        analysis_id,
        user_id=user_id,
        reason="CREATE_FAILED",
        actor_type="SYSTEM_SWEEP",
    )
    return "deleted" if result.get("deleted") else "failed"


async def run_once(*, now: Optional[datetime] = None, limit: int = DEFAULT_LIMIT) -> Dict[str, int]:
    """Expire due rows and reconcile only structurally unambiguous states."""
    db = store.database()
    now_value = _timestamp(now)
    counts = {
        "expired": 0,
        "delete_retried": 0,
        "create_reconciled": 0,
        "db_file_missing": 0,
        "orphan_deleted": 0,
        "temp_deleted": 0,
        "failed": 0,
    }

    expired = await db[store.RECORDS_COLLECTION].find(
        {"deletion_state": store.STATE_ACTIVE, "expires_at": {"$lte": now_value}},
        {"_id": 0},
    ).sort("expires_at", 1).limit(limit).to_list(length=limit)
    for record in expired:
        result = await erasure.erase_for_analysis(
            record["analysis_id"], user_id=record["user_id"],
            reason="TTL_EXPIRED", actor_type="SYSTEM_SWEEP",
        )
        counts["expired" if result.get("deleted") else "failed"] += 1

    pending = await db[store.RECORDS_COLLECTION].find(
        {"deletion_state": store.STATE_DELETE_PENDING}, {"_id": 0}
    ).limit(limit).to_list(length=limit)
    for record in pending:
        reason = record.get("deletion_reason") or "CREATE_FAILED"
        if reason not in erasure.DELETION_REASONS:
            reason = "CREATE_FAILED"
        result = await erasure.erase_for_analysis(
            record["analysis_id"], user_id=record["user_id"],
            reason=reason, actor_type="SYSTEM_SWEEP",
        )
        counts["delete_retried" if result.get("deleted") else "failed"] += 1

    creating = await db[store.RECORDS_COLLECTION].find(
        {"deletion_state": store.STATE_CREATE_PENDING}, {"_id": 0}
    ).limit(limit).to_list(length=limit)
    for record in creating:
        outcome = await _reconcile_create_pending(record)
        counts["create_reconciled" if outcome in {"activated", "deleted"} else "failed"] += 1

    active = await db[store.RECORDS_COLLECTION].find(
        {"deletion_state": store.STATE_ACTIVE}, {"_id": 0}
    ).limit(limit).to_list(length=limit)
    for record in active:
        try:
            exists = await asyncio.to_thread(
                blob_store.ciphertext_exists, record.get("encrypted_storage_id")
            )
        except Exception:
            continue
        if exists:
            continue
        result = await erasure.erase_for_analysis(
            record["analysis_id"], user_id=record["user_id"],
            reason="CREATE_FAILED", actor_type="SYSTEM_SWEEP",
        )
        counts["db_file_missing" if result.get("deleted") else "failed"] += 1

    try:
        for storage_id, path in blob_store.iter_objects():
            if counts["orphan_deleted"] >= limit:
                break
            if storage_id is None:
                continue
            record = await db[store.RECORDS_COLLECTION].find_one(
                {"encrypted_storage_id": storage_id}, {"_id": 1}
            )
            if record:
                continue
            try:
                await asyncio.to_thread(_safe_unlink_orphan, path)
                counts["orphan_deleted"] += 1
                await store.audit_event(
                    store.EVENT_RECONCILED,
                    retention_id=None,
                    analysis_id="SYSTEM_ORPHAN",
                    user_id="SYSTEM_ORPHAN",
                    actor_type="SYSTEM_SWEEP",
                    reconciliation_code="FILE_EXISTS_DB_MISSING",
                )
            except Exception:
                counts["failed"] += 1
    except FileNotFoundError:
        pass

    stale_seconds_raw = os.environ.get(
        "CORRECTNESS_V2_PDF_RETENTION_STALE_TEMP_SECONDS", str(DEFAULT_STALE_TEMP_SECONDS)
    )
    try:
        stale_seconds = max(60, int(stale_seconds_raw))
    except (TypeError, ValueError):
        stale_seconds = DEFAULT_STALE_TEMP_SECONDS
    cutoff = time.time() - stale_seconds
    try:
        for _storage_id, path in blob_store.iter_temps():
            if counts["temp_deleted"] >= limit:
                break
            try:
                st = path.lstat()
                if st.st_mtime > cutoff:
                    continue
                await asyncio.to_thread(blob_store.discard_temp, path)
                counts["temp_deleted"] += 1
            except Exception:
                counts["failed"] += 1
    except FileNotFoundError:
        pass
    return counts
