"""
Job status contract for Correctness Mode v2.

Builds the strict status response object every job returns. Centralises the
diagnostic contract so failures can NEVER be vague: a failure always carries
reason_code / reason_human / troubleshoot_message / next_steps / artifacts_saved
and is hard-pinned to customer_report_generated=False, safe_to_show_customer=False.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from . import schemas
from .schemas import JobStatus


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_job_id() -> str:
    return f"cv2_{uuid.uuid4().hex}"


def make_status(
    *,
    job_id: str,
    analysis_id: str,
    status: str,
    current_stage: str,
    admin_only: bool = True,
    customer_report_generated: bool = False,
    safe_to_show_customer: bool = False,
    reason_code: Optional[str] = None,
    reason_human: Optional[str] = None,
    troubleshoot_message: Optional[str] = None,
    next_steps: Optional[List[str]] = None,
    artifacts_saved: Optional[Dict[str, Any]] = None,
    created_at: Optional[str] = None,
    updated_at: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build a status response dict.

    For non-failure statuses, the diagnostic fields may be ``None``/empty.
    For failure statuses, callers should use :func:`make_failure_status` which
    enforces the full diagnostic contract.
    """
    now = _now_iso()
    payload: Dict[str, Any] = {
        "job_id": job_id,
        "analysis_id": analysis_id,
        "mode": schemas.MODE,
        "status": status,
        "current_stage": current_stage,
        "customer_report_generated": bool(customer_report_generated),
        "safe_to_show_customer": bool(safe_to_show_customer),
        "admin_only": bool(admin_only),
        "reason_code": reason_code,
        "reason_human": reason_human,
        "troubleshoot_message": troubleshoot_message,
        "next_steps": list(next_steps or []),
        "artifacts_saved": dict(artifacts_saved or {}),
        "created_at": created_at or now,
        "updated_at": updated_at or now,
    }
    if extra:
        # Never allow extra to clobber contract keys.
        for k, v in extra.items():
            if k not in payload:
                payload[k] = v
    return payload


def make_failure_status(
    *,
    job_id: str,
    analysis_id: str,
    status: str,
    current_stage: str,
    reason_code: str,
    reason_human: str,
    troubleshoot_message: str,
    next_steps: List[str],
    artifacts_saved: Optional[Dict[str, Any]] = None,
    created_at: Optional[str] = None,
    admin_only: bool = True,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build a FAILURE status. Always fails closed:
    customer_report_generated=False, safe_to_show_customer=False.

    Raises ValueError if the resulting payload does not satisfy the diagnostic
    contract — we would rather crash the job shell than emit a vague failure.
    """
    payload = make_status(
        job_id=job_id,
        analysis_id=analysis_id,
        status=status,
        current_stage=current_stage,
        admin_only=admin_only,
        customer_report_generated=False,
        safe_to_show_customer=False,
        reason_code=reason_code,
        reason_human=reason_human,
        troubleshoot_message=troubleshoot_message,
        next_steps=list(next_steps or []),
        artifacts_saved=artifacts_saved,
        created_at=created_at,
        extra=extra,
    )
    problems = schemas.validate_failure_payload(payload)
    if problems:
        raise ValueError(f"invalid failure payload: {problems}")
    return payload


def touch(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Update the ``updated_at`` timestamp in place and return the payload."""
    payload["updated_at"] = _now_iso()
    return payload


def sanitize_for_customer(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Produce a customer-safe view of a status payload.

    Strips any local filesystem paths from artifacts_saved (customers must never
    see raw local paths) while keeping the diagnostic semantics. Admin responses
    use the raw payload directly.
    """
    safe = dict(payload)
    artifacts = safe.get("artifacts_saved")
    if isinstance(artifacts, dict):
        safe["artifacts_saved"] = {
            key: "<hidden>" for key in artifacts.keys()
        }
    # Customers never see local job folder absolute paths.
    safe.pop("job_dir", None)
    return safe
