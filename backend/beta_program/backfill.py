"""
One-shot OFFLINE backfill of clearly-terminal existing jobs into
``v2_job_events``.

Run only from the migration CLI, never at request time. Walks the on-disk job
folders once, and mirrors ONLY unambiguously-terminal statuses into the
telemetry collection (idempotent via the unique ``event_id`` index). Ambiguous /
in-flight / unknown states are skipped — never backfilled as fact.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from . import signals

logger = logging.getLogger(__name__)

# Map unambiguous terminal job statuses -> telemetry event types.
_STATUS_EVENT_MAP = {
    "REPORT_READY": signals.EVENT_REPORT_READY,
    "MONEY_CONFIRMATION_REQUIRED": signals.EVENT_CONFIRMATION_REQUIRED,
    "CONTRACT_VALIDATION_FAILED": signals.EVENT_VERIFICATION_REQUIRED,
    "NEEDS_MANUAL_REVIEW": signals.EVENT_VERIFICATION_REQUIRED,
    "FAILED_GROUNDING": signals.EVENT_VERIFICATION_REQUIRED,
    "PDF_QUALITY_BLOCKED": signals.EVENT_DOCUMENT_NOT_READABLE,
}


def _resolve_user_id_sync(analysis_id: str) -> Optional[str]:
    """Best-effort sync lookup of the owning user_id for an analysis."""
    try:
        import os

        from pymongo import MongoClient

        client = MongoClient(os.environ["MONGO_URL"], serverSelectionTimeoutMS=2000)
        doc = client[os.environ["DB_NAME"]]["perizia_analyses"].find_one(
            {"analysis_id": analysis_id}, {"_id": 0, "user_id": 1}
        )
        return (doc or {}).get("user_id")
    except Exception:
        return None


def backfill_terminal_job_events(*, dry_run: bool = True) -> Dict[str, Any]:
    report: Dict[str, Any] = {"dry_run": dry_run, "emitted": 0, "skipped": 0, "scanned": 0}
    try:
        from correctness_v2 import artifacts
    except Exception as exc:  # pragma: no cover
        report["error"] = f"artifacts import failed: {exc}"
        return report

    try:
        job_ids = artifacts.list_jobs()
    except Exception as exc:  # pragma: no cover
        report["error"] = f"list_jobs failed: {exc}"
        return report

    for job_id in job_ids:
        report["scanned"] += 1
        status_doc = None
        try:
            status_doc = artifacts.read_job_status(job_id)
        except Exception:
            status_doc = None
        if not isinstance(status_doc, dict):
            report["skipped"] += 1
            continue
        status = str(status_doc.get("status") or "")
        event_type = _STATUS_EVENT_MAP.get(status)
        if not event_type:
            # Ambiguous / in-flight / unknown -> never backfill as fact.
            report["skipped"] += 1
            continue
        analysis_id = status_doc.get("analysis_id")
        if not analysis_id:
            report["skipped"] += 1
            continue
        if dry_run:
            report["emitted"] += 1
            continue
        user_id = _resolve_user_id_sync(analysis_id)
        signals.emit_v2_job_event(
            event_type,
            job_id=job_id,
            analysis_id=analysis_id,
            user_id=user_id,
            status=status,
            reason_code=status_doc.get("reason_code"),
        )
        report["emitted"] += 1
        # Offline script: telemetry emits are queued, so drain periodically to
        # stay well inside the bounded queue instead of dropping backfill events.
        if report["emitted"] % 500 == 0:
            signals.flush(60.0)
    if not dry_run:
        signals.flush(120.0)
    return report
