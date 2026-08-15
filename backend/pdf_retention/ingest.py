"""Durable-analysis-first creation of encrypted retained originals."""

from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from pymongo.errors import DuplicateKeyError
from pymongo import ReturnDocument

from . import blob_store, config, crypto, store

logger = logging.getLogger(__name__)


def _failure_code(exc: BaseException) -> str:
    if isinstance(exc, config.RetentionConfigurationError):
        return "CONFIGURATION_UNAVAILABLE"
    if isinstance(exc, blob_store.UnsafeStoragePath):
        return "UNSAFE_STORAGE_PATH"
    if isinstance(exc, crypto.RetentionCryptoError):
        return "ENCRYPTION_FAILED"
    if isinstance(exc, DuplicateKeyError):
        return "DUPLICATE_RECORD"
    if isinstance(exc, OSError):
        return "STORAGE_IO_FAILED"
    return "RETENTION_INTERNAL_FAILED"


async def retain_if_consented(
    *,
    analysis_id: str,
    user_id: str,
    contents: bytes,
    input_sha256: str,
    consent_requested: bool,
) -> Dict[str, Any]:
    """Retain only after the owner-scoped analysis row is durable.

    Returns customer-safe diagnostic state; it never raises into the analysis
    pipeline and never writes plaintext as a fallback.
    """
    if not config.is_enabled() or not bool(consent_requested):
        return {"attempted": False, "retained": False}
    if not isinstance(contents, bytes) or hashlib.sha256(contents).hexdigest() != input_sha256:
        return {"attempted": True, "retained": False, "failure_code": "INPUT_HASH_MISMATCH"}

    db = store.database()
    analysis = await db.perizia_analyses.find_one(
        {"analysis_id": analysis_id, "user_id": user_id, "input_sha256": input_sha256},
        {"_id": 1},
    )
    if not analysis:
        return {"attempted": True, "retained": False, "failure_code": "ANALYSIS_NOT_DURABLE"}

    now = datetime.now(timezone.utc)
    created_at = now.isoformat()
    days = config.retention_days()
    expires_at = (now + timedelta(days=days)).isoformat()
    retention_id = store.new_retention_id()
    storage_id = blob_store.new_storage_id()
    temp_path: Optional[Path] = None
    record_inserted = False
    try:
        consent = await store.grant_consent(
            analysis_id=analysis_id,
            user_id=user_id,
            consent_version=config.CONSENT_VERSION,
            consented_at=created_at,
            retention_days_at_consent=days,
        )
        encrypted = await asyncio.to_thread(
            crypto.encrypt_pdf,
            contents,
            retention_id=retention_id,
            analysis_id=analysis_id,
            user_id=user_id,
            input_sha256=input_sha256,
            consent_version=config.CONSENT_VERSION,
        )
        temp_path = await asyncio.to_thread(
            blob_store.write_encrypted_temp, storage_id, encrypted.ciphertext
        )
        record = {
            "retention_id": retention_id,
            "user_id": user_id,
            "analysis_id": analysis_id,
            "input_sha256": input_sha256,
            "encrypted_storage_id": storage_id,
            "created_at": created_at,
            "expires_at": expires_at,
            "consent_id": consent.get("consent_id"),
            "consent_version": config.CONSENT_VERSION,
            "consented_at": created_at,
            "retention_days_at_consent": days,
            **encrypted.metadata,
            "deletion_state": store.STATE_CREATE_PENDING,
            "delete_requested_at": None,
            "deleted_at": None,
            "deletion_reason": None,
            "last_failure_code": None,
        }
        await db[store.RECORDS_COLLECTION].insert_one(dict(record))
        record_inserted = True
        await asyncio.to_thread(blob_store.promote_temp, storage_id, temp_path)
        temp_path = None
        activated = await db[store.RECORDS_COLLECTION].find_one_and_update(
            {
                "retention_id": retention_id,
                "user_id": user_id,
                "deletion_state": store.STATE_CREATE_PENDING,
            },
            {"$set": {"deletion_state": store.STATE_ACTIVE, "activated_at": store.now_iso()}},
            return_document=ReturnDocument.AFTER,
            projection={"_id": 0},
        )
        if not activated:
            raise RuntimeError("PDF_RETENTION_ACTIVATION_FAILED")
        await store.set_consent_retention_status(analysis_id, user_id, "RETAINED")
        await store.audit_event(
            store.EVENT_CREATED,
            retention_id=retention_id,
            analysis_id=analysis_id,
            user_id=user_id,
            actor_type="SYSTEM_UPLOAD",
        )
        return {"attempted": True, "retained": True, "expires_at": expires_at}
    except Exception as exc:
        code = _failure_code(exc)
        if temp_path is not None:
            try:
                await asyncio.to_thread(blob_store.discard_temp, temp_path)
                temp_path = None
            except Exception:
                pass
        if record_inserted:
            file_absent = False
            try:
                await asyncio.to_thread(blob_store.delete_ciphertext, storage_id)
                file_absent = True
            except FileNotFoundError:
                file_absent = True
            except Exception:
                file_absent = False
            next_state = store.STATE_DELETED if file_absent and temp_path is None else store.STATE_DELETE_PENDING
            update = {
                "deletion_state": next_state,
                "delete_requested_at": store.now_iso(),
                "deletion_reason": "CREATE_FAILED",
                "last_failure_code": code,
            }
            if next_state == store.STATE_DELETED:
                update["deleted_at"] = store.now_iso()
                update["wrapped_dek_b64"] = None
                update["auth_tag_b64"] = None
            try:
                await db[store.RECORDS_COLLECTION].update_one(
                    {"retention_id": retention_id, "user_id": user_id}, {"$set": update}
                )
            except Exception:
                pass
        try:
            await store.set_consent_retention_status(analysis_id, user_id, "FAILED", code)
        except Exception:
            pass
        logger.warning("pdf_retention creation failed code=%s", code)
        return {"attempted": True, "retained": False, "failure_code": code}
