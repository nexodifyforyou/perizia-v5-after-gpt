"""Non-web owner diagnostic access with mandatory authz, reason, and audit."""

from __future__ import annotations

import asyncio
import hashlib
import os
import re
import shutil
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator, Dict

from . import blob_store, config, crypto, store


class DiagnosticAccessDenied(RuntimeError):
    pass


def _sanitize_reason(reason: str) -> str:
    text = "".join(ch for ch in str(reason or "") if ord(ch) >= 32).strip()[:500]
    if not text:
        raise DiagnosticAccessDenied("DIAGNOSTIC_REASON_REQUIRED")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/ -]{2,199}", text):
        raise DiagnosticAccessDenied("DIAGNOSTIC_REASON_UNSAFE")
    return text


async def decrypt_for_owner_diagnostic(
    *, analysis_id: str, owner_user_id: str, owner_email: str, reason: str
) -> bytes:
    """Explicit ops action. It is intentionally not mounted in FastAPI."""
    import server  # type: ignore

    safe_reason = _sanitize_reason(reason)
    normalized_email = str(owner_email or "").strip().lower()
    if not server._is_exact_owner_admin_email(normalized_email):
        raise DiagnosticAccessDenied("OWNER_AUTHORIZATION_REQUIRED")
    owner = await store.database().users.find_one(
        {"user_id": owner_user_id, "email": normalized_email},
        {"_id": 0, "user_id": 1, "email": 1},
    )
    if not owner:
        raise DiagnosticAccessDenied("OWNER_AUTHORIZATION_REQUIRED")
    analysis = await store.database().perizia_analyses.find_one(
        {"analysis_id": analysis_id}, {"_id": 0, "analysis_id": 1, "user_id": 1, "input_sha256": 1}
    )
    if not analysis:
        raise DiagnosticAccessDenied("ANALYSIS_CONTEXT_NOT_FOUND")
    record = await store.database()[store.RECORDS_COLLECTION].find_one(
        {
            "analysis_id": analysis_id,
            "user_id": analysis["user_id"],
            "input_sha256": analysis["input_sha256"],
            "deletion_state": store.STATE_ACTIVE,
        },
        {"_id": 0},
    )
    if not record or str(record.get("expires_at") or "") <= store.now_iso():
        raise DiagnosticAccessDenied("RETAINED_PDF_NOT_AVAILABLE")
    ciphertext = await asyncio.to_thread(
        blob_store.read_ciphertext, record.get("encrypted_storage_id")
    )
    plaintext = await asyncio.to_thread(crypto.decrypt_pdf, ciphertext, record)
    if hashlib.sha256(plaintext).hexdigest() != analysis["input_sha256"]:
        raise DiagnosticAccessDenied("DIAGNOSTIC_LINEAGE_MISMATCH")
    await store.audit_event(
        "PDF_RETENTION_ACCESSED",
        retention_id=record["retention_id"],
        analysis_id=analysis_id,
        user_id=analysis["user_id"],
        actor_type="OWNER_OPS",
        actor_user_id=owner_user_id,
        reason_code="OWNER_DIAGNOSTIC_ACCESS",
        reason_detail=safe_reason,
    )
    return plaintext


@asynccontextmanager
async def materialize_for_owner_diagnostic(
    *, analysis_id: str, owner_user_id: str, owner_email: str, reason: str
) -> AsyncIterator[Path]:
    """Materialize mode 0600 under a private runtime dir and always remove it."""
    plaintext = await decrypt_for_owner_diagnostic(
        analysis_id=analysis_id, owner_user_id=owner_user_id,
        owner_email=owner_email, reason=reason,
    )
    root = config.diagnostic_temp_root()
    if root == Path("/run/periziascan/pdf_retention") and os.geteuid() != 0:
        raise DiagnosticAccessDenied("ROOT_DIAGNOSTIC_MATERIALIZATION_REQUIRED")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(root, 0o700)
    if root == Path("/run/periziascan/pdf_retention") and root.lstat().st_uid != 0:
        raise DiagnosticAccessDenied("ROOT_DIAGNOSTIC_MATERIALIZATION_REQUIRED")
    temp_dir = Path(tempfile.mkdtemp(prefix="diag_", dir=root))
    path = temp_dir / "original.pdf"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        view = memoryview(plaintext)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("diagnostic materialization write failed")
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)
    try:
        yield path
    finally:
        try:
            path.unlink(missing_ok=True)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
