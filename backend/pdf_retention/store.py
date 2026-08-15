"""Mongo metadata and append-only safe lifecycle events.

Mongo is standalone, so all correctness-critical transitions are conditional
single-document updates.  Filesystem ordering is implemented by ``ingest`` and
``erasure``; this module never pretends those operations are transactional.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pymongo import ReturnDocument

CONSENTS_COLLECTION = "pdf_retention_consents"
RECORDS_COLLECTION = "pdf_retention_records"
AUDIT_COLLECTION = "pdf_retention_audit"

STATE_CREATE_PENDING = "CREATE_PENDING"
STATE_ACTIVE = "ACTIVE"
STATE_DELETE_PENDING = "DELETE_PENDING"
STATE_DELETED = "DELETED"

EVENT_CONSENTED = "PDF_RETENTION_CONSENTED"
EVENT_CREATED = "PDF_RETENTION_CREATED"
EVENT_DELETE_REQUESTED = "PDF_RETENTION_DELETE_REQUESTED"
EVENT_DELETED = "PDF_RETENTION_DELETED"
EVENT_EXPIRED = "PDF_RETENTION_EXPIRED"
EVENT_DELETE_FAILED = "PDF_RETENTION_DELETE_FAILED"
EVENT_RECONCILED = "PDF_RETENTION_RECONCILED"

logger = logging.getLogger(__name__)
_db_override = None


def set_database_override(database: Any) -> None:
    global _db_override
    _db_override = database


def clear_database_override() -> None:
    global _db_override
    _db_override = None


def database():
    if _db_override is not None:
        return _db_override
    import server  # type: ignore

    return server.db


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_retention_id() -> str:
    return f"pdfr_{uuid.uuid4().hex}"


async def ensure_indexes() -> None:
    db = database()
    await db[CONSENTS_COLLECTION].create_index(
        [("analysis_id", 1), ("user_id", 1)], unique=True,
        name="uq_pdf_retention_consent_analysis_owner", background=True,
    )
    await db[RECORDS_COLLECTION].create_index(
        [("analysis_id", 1), ("user_id", 1)], unique=True,
        name="uq_pdf_retention_record_analysis_owner", background=True,
    )
    await db[RECORDS_COLLECTION].create_index(
        "retention_id", unique=True, name="uq_pdf_retention_id", background=True,
    )
    await db[RECORDS_COLLECTION].create_index(
        "encrypted_storage_id", unique=True, name="uq_pdf_retention_storage_id", background=True,
    )
    await db[RECORDS_COLLECTION].create_index(
        [("deletion_state", 1), ("expires_at", 1)],
        name="ix_pdf_retention_expiry", background=True,
    )
    await db[RECORDS_COLLECTION].create_index(
        [("deletion_state", 1), ("delete_requested_at", 1)],
        name="ix_pdf_retention_delete_pending", background=True,
    )
    await db[RECORDS_COLLECTION].create_index(
        [("user_id", 1), ("deletion_state", 1)],
        name="ix_pdf_retention_owner_state", background=True,
    )
    await db[AUDIT_COLLECTION].create_index(
        [("retention_id", 1), ("created_at", 1)],
        name="ix_pdf_retention_audit_record", background=True,
    )
    await db[AUDIT_COLLECTION].create_index(
        [("user_id", 1), ("created_at", -1)],
        name="ix_pdf_retention_audit_owner", background=True,
    )


async def audit_event(
    event: str,
    *,
    retention_id: Optional[str],
    analysis_id: str,
    user_id: str,
    actor_type: str,
    reason_code: Optional[str] = None,
    actor_user_id: Optional[str] = None,
    reconciliation_code: Optional[str] = None,
    reason_detail: Optional[str] = None,
) -> None:
    """Write only closed, non-content metadata. Audit failure never leaks data."""
    row = {
        "audit_id": f"pdfaud_{uuid.uuid4().hex}",
        "event": event,
        "retention_id": retention_id,
        "analysis_id": str(analysis_id),
        "user_id": str(user_id),
        "actor_type": str(actor_type),
        "actor_user_id": str(actor_user_id) if actor_user_id else None,
        "reason_code": str(reason_code) if reason_code else None,
        "reconciliation_code": str(reconciliation_code) if reconciliation_code else None,
        "reason_detail": str(reason_detail) if reason_detail else None,
        "created_at": now_iso(),
    }
    try:
        await database()[AUDIT_COLLECTION].insert_one(row)
    except Exception:
        logger.warning("pdf_retention audit write failed event=%s", event)


async def grant_consent(
    *, analysis_id: str, user_id: str, consent_version: str, consented_at: str,
    retention_days_at_consent: int,
) -> Dict[str, Any]:
    db = database()
    doc = await db[CONSENTS_COLLECTION].find_one_and_update(
        {"analysis_id": analysis_id, "user_id": user_id},
        {"$setOnInsert": {
            "consent_id": f"pdfc_{uuid.uuid4().hex}",
            "analysis_id": analysis_id,
            "user_id": user_id,
            "consent_version": consent_version,
            "consented_at": consented_at,
            "retention_days_at_consent": int(retention_days_at_consent),
            "status": "GRANTED",
            "retention_status": "PENDING",
            "created_at": consented_at,
            "updated_at": consented_at,
            "withdrawn_at": None,
        }},
        upsert=True,
        return_document=ReturnDocument.AFTER,
        projection={"_id": 0},
    )
    if not doc or doc.get("status") != "GRANTED":
        raise RuntimeError("PDF_RETENTION_CONSENT_NOT_GRANTED")
    if (
        doc.get("consent_version") != consent_version
        or int(doc.get("retention_days_at_consent") or 0) != int(retention_days_at_consent)
    ):
        raise RuntimeError("PDF_RETENTION_CONSENT_CONTRACT_MISMATCH")
    await audit_event(
        EVENT_CONSENTED, retention_id=None, analysis_id=analysis_id, user_id=user_id,
        actor_type="CUSTOMER", actor_user_id=user_id,
    )
    return doc


async def set_consent_retention_status(
    analysis_id: str, user_id: str, status: str, failure_code: Optional[str] = None
) -> None:
    update: Dict[str, Any] = {
        "retention_status": status,
        "updated_at": now_iso(),
        "retention_failure_code": failure_code,
    }
    await database()[CONSENTS_COLLECTION].update_one(
        {"analysis_id": analysis_id, "user_id": user_id}, {"$set": update}
    )


async def withdraw_consent(analysis_id: str, user_id: str) -> None:
    now = now_iso()
    await database()[CONSENTS_COLLECTION].update_one(
        {"analysis_id": analysis_id, "user_id": user_id},
        {"$set": {"status": "WITHDRAWN", "withdrawn_at": now, "updated_at": now}},
    )


async def request_consent_withdrawal(analysis_id: str, user_id: str) -> None:
    now = now_iso()
    await database()[CONSENTS_COLLECTION].update_one(
        {"analysis_id": analysis_id, "user_id": user_id, "status": "GRANTED"},
        {"$set": {"status": "WITHDRAWAL_PENDING", "updated_at": now}},
    )
