"""Crash-safe, owner-scoped encrypted-object erasure."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional

from pymongo import ReturnDocument

from . import blob_store, store

logger = logging.getLogger(__name__)

DELETION_REASONS = frozenset({
    "TTL_EXPIRED", "CUSTOMER_ERASURE", "CONSENT_WITHDRAWN",
    "ANALYSIS_DELETED", "HISTORY_DELETED", "ACCOUNT_DELETED", "CREATE_FAILED",
})


def _safe_failure_code(exc: BaseException) -> str:
    if isinstance(exc, blob_store.UnsafeStoragePath):
        return str(exc) if str(exc) in {
            "MALFORMED_STORAGE_ID", "UNSAFE_STORAGE_OBJECT_TYPE",
            "UNSAFE_STORAGE_HARDLINK", "UNSAFE_STORAGE_PERMISSIONS",
        } else "UNSAFE_STORAGE_PATH"
    if isinstance(exc, OSError):
        return "FILE_DELETE_IO_FAILED"
    return "FILE_DELETE_FAILED"


async def erase_for_analysis(
    analysis_id: str,
    *,
    user_id: str,
    reason: str,
    actor_type: str,
    actor_user_id: Optional[str] = None,
) -> Dict[str, Any]:
    if reason not in DELETION_REASONS:
        raise ValueError("INVALID_PDF_RETENTION_DELETION_REASON")
    db = store.database()
    # Every lookup/transition below is wrapped: callers (including the
    # existing single-analysis and bulk-history delete endpoints) must never
    # see a raw, unhandled exception from this module. An unexpected error
    # here (e.g. a transient Mongo fault unrelated to any retained blob) is
    # reported the same way a genuine ciphertext-delete failure is -- a safe,
    # retryable "not deleted yet" -- rather than propagating and crashing an
    # otherwise-unrelated delete-analysis/delete-all-history request.
    try:
        record = await db[store.RECORDS_COLLECTION].find_one(
            {"analysis_id": analysis_id, "user_id": user_id}, {"_id": 0}
        )
        if not record:
            return {"deleted": True, "existed": False}
        if record.get("deletion_state") == store.STATE_DELETED:
            return {"deleted": True, "existed": True}
        retention_id = record.get("retention_id")
        if record.get("deletion_state") != store.STATE_DELETE_PENDING:
            record = await db[store.RECORDS_COLLECTION].find_one_and_update(
                {
                    "retention_id": retention_id,
                    "user_id": user_id,
                    "deletion_state": {"$in": [store.STATE_ACTIVE, store.STATE_CREATE_PENDING]},
                },
                {"$set": {
                    "deletion_state": store.STATE_DELETE_PENDING,
                    "delete_requested_at": store.now_iso(),
                    "deletion_reason": reason,
                    "last_failure_code": None,
                }},
                return_document=ReturnDocument.AFTER,
                projection={"_id": 0},
            )
            if not record:
                record = await db[store.RECORDS_COLLECTION].find_one(
                    {"retention_id": retention_id, "user_id": user_id}, {"_id": 0}
                )
        if not record:
            return {"deleted": True, "existed": True}
    except Exception as exc:
        code = _safe_failure_code(exc)
        logger.warning("pdf_retention erasure lookup failed code=%s", code)
        return {"deleted": False, "existed": True, "failure_code": code}
    await store.audit_event(
        store.EVENT_DELETE_REQUESTED,
        retention_id=retention_id,
        analysis_id=analysis_id,
        user_id=user_id,
        actor_type=actor_type,
        actor_user_id=actor_user_id,
        reason_code=reason,
    )
    try:
        removed = await asyncio.to_thread(
            blob_store.delete_ciphertext, record.get("encrypted_storage_id")
        )
        completed = await db[store.RECORDS_COLLECTION].find_one_and_update(
            {
                "retention_id": retention_id,
                "user_id": user_id,
                "deletion_state": store.STATE_DELETE_PENDING,
            },
            {"$set": {
                "deletion_state": store.STATE_DELETED,
                "deleted_at": store.now_iso(),
                "deletion_reason": reason,
                "last_failure_code": None,
                "wrapped_dek_b64": None,
                "auth_tag_b64": None,
            }},
            return_document=ReturnDocument.AFTER,
            projection={"_id": 0},
        )
        if not completed:
            already_completed = await db[store.RECORDS_COLLECTION].find_one(
                {
                    "retention_id": retention_id,
                    "user_id": user_id,
                    "deletion_state": store.STATE_DELETED,
                },
                {"_id": 1},
            )
            if not already_completed:
                raise RuntimeError("PDF_RETENTION_TOMBSTONE_UPDATE_FAILED")
        if not removed:
            await store.audit_event(
                store.EVENT_RECONCILED,
                retention_id=retention_id,
                analysis_id=analysis_id,
                user_id=user_id,
                actor_type=actor_type,
                actor_user_id=actor_user_id,
                reason_code=reason,
                reconciliation_code="DB_EXISTS_FILE_MISSING",
            )
        if reason == "TTL_EXPIRED":
            await store.audit_event(
                store.EVENT_EXPIRED,
                retention_id=retention_id,
                analysis_id=analysis_id,
                user_id=user_id,
                actor_type=actor_type,
                reason_code=reason,
            )
        await store.audit_event(
            store.EVENT_DELETED,
            retention_id=retention_id,
            analysis_id=analysis_id,
            user_id=user_id,
            actor_type=actor_type,
            actor_user_id=actor_user_id,
            reason_code=reason,
        )
        return {"deleted": True, "existed": True}
    except Exception as exc:
        code = _safe_failure_code(exc)
        await db[store.RECORDS_COLLECTION].update_one(
            {
                "retention_id": retention_id,
                "user_id": user_id,
                "deletion_state": store.STATE_DELETE_PENDING,
            },
            {"$set": {"last_failure_code": code, "last_delete_failed_at": store.now_iso()}},
        )
        await store.audit_event(
            store.EVENT_DELETE_FAILED,
            retention_id=retention_id,
            analysis_id=analysis_id,
            user_id=user_id,
            actor_type=actor_type,
            actor_user_id=actor_user_id,
            reason_code=reason,
        )
        logger.warning("pdf_retention deletion failed code=%s", code)
        return {"deleted": False, "existed": True, "failure_code": code}


async def withdraw_and_erase(analysis_id: str, *, user_id: str) -> Dict[str, Any]:
    await store.request_consent_withdrawal(analysis_id, user_id)
    result = await erase_for_analysis(
        analysis_id,
        user_id=user_id,
        reason="CONSENT_WITHDRAWN",
        actor_type="CUSTOMER",
        actor_user_id=user_id,
    )
    if result.get("deleted"):
        await store.withdraw_consent(analysis_id, user_id)
    return result


async def erase_all_for_user(user_id: str, *, reason: str, actor_type: str) -> Dict[str, Any]:
    db = store.database()
    try:
        rows = await db[store.RECORDS_COLLECTION].find(
            {
                "user_id": user_id,
                "deletion_state": {"$in": [
                    store.STATE_ACTIVE, store.STATE_CREATE_PENDING, store.STATE_DELETE_PENDING,
                ]},
            },
            {"_id": 0, "analysis_id": 1},
        ).to_list(length=None)
    except Exception as exc:
        code = _safe_failure_code(exc)
        logger.warning("pdf_retention bulk erasure lookup failed code=%s", code)
        return {"deleted": False, "processed": 0, "failures": [{"analysis_id": None, "failure_code": code}]}
    failures = []
    for row in rows:
        result = await erase_for_analysis(
            row["analysis_id"], user_id=user_id, reason=reason, actor_type=actor_type
        )
        if not result.get("deleted"):
            failures.append({"analysis_id": row["analysis_id"], "failure_code": result.get("failure_code")})
    return {"deleted": not failures, "processed": len(rows), "failures": failures}
