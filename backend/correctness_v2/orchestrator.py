"""
Job shell / orchestrator for Correctness Mode v2 (step 1).

Responsibilities in this step:
  1. Create an isolated job (id + artifact folder).
  2. Load page-by-page text via an injected ``page_loader`` (kept decoupled from
     the old pipeline on purpose).
  3. Run the PDF quality blocker and persist artifacts.
  4. Map the quality verdict to a strict job status.
  5. FAIL CLOSED on any error — never fall back to the old analyzer.

HARD RULE — NO OLD FALLBACK:
This module deliberately does not import, reference, or call any symbol from the
old report analyzer. If anything fails, we return a diagnostic FAILED_ANALYSIS /
PDF_QUALITY_BLOCKED status with customer_report_generated=False and
safe_to_show_customer=False. A wrong report is worse than no report.

OpenAI analysis and Gemini narration are intentionally NOT implemented here.
"""

from __future__ import annotations

import traceback
from typing import Any, Callable, Dict, List, Optional

from . import artifacts, feature_flags, job_status
from .pdf_quality import assess_pdf_quality
from .schemas import JobStatus, PdfQualityStatus

# Sentinel string asserting intent; referenced by the no-old-fallback guard test.
NO_OLD_ANALYZER_FALLBACK = True

STEP1_OK_MESSAGE = (
    "PDF quality checked. Correctness analysis not implemented in this step."
)

PageLoader = Callable[[str], List[Dict[str, Any]]]


def _stage(name: str) -> str:
    return f"step1:{name}"


def start_job(
    analysis_id: str,
    page_loader: PageLoader,
    *,
    is_admin: bool = True,
    ocr_failed: bool = False,
) -> Dict[str, Any]:
    """
    Create and run a Correctness v2 job for ``analysis_id``.

    ``page_loader`` is a callable that returns the list of
    ``{"page_number": int, "text": str, ...}`` pages for the analysis. It is
    injected so this orchestrator never reaches into the old pipeline directly.

    Returns the final job status dict. Always persists artifacts. Never raises
    for expected failures — it converts them into a diagnostic failure status.
    """
    job_id = job_status.new_job_id()
    admin_only = feature_flags.is_admin_only()
    artifacts.ensure_job_dir(job_id)

    # 1) QUEUED
    queued = job_status.make_status(
        job_id=job_id,
        analysis_id=analysis_id,
        status=JobStatus.QUEUED,
        current_stage=_stage("queued"),
        admin_only=admin_only,
    )
    saved_status_path = artifacts.save_job_status(job_id, queued)

    # 2) RUNNING
    running = job_status.make_status(
        job_id=job_id,
        analysis_id=analysis_id,
        status=JobStatus.RUNNING,
        current_stage=_stage("loading_pages"),
        admin_only=admin_only,
        created_at=queued["created_at"],
        artifacts_saved={"job_status": saved_status_path},
    )
    artifacts.save_job_status(job_id, running)

    created_at = queued["created_at"]

    try:
        pages = page_loader(analysis_id)
        if not isinstance(pages, list):
            raise ValueError("page_loader did not return a list of pages")

        pages_path = artifacts.save_input_pages(job_id, pages)

        # 3) PDF quality blocker
        report = assess_pdf_quality(pages, ocr_failed=ocr_failed, analysis_id=analysis_id)
        quality_path = artifacts.save_pdf_quality_report(job_id, report)

        artifacts_saved = {
            "job_status": saved_status_path,
            "input_pages": pages_path,
            "pdf_quality_report": quality_path,
        }

        quality_status = report.get("quality_status")

        if quality_status == PdfQualityStatus.BLOCKED:
            return _finish_blocked(
                job_id, analysis_id, report, artifacts_saved, created_at, admin_only
            )
        if quality_status == PdfQualityStatus.WARNING:
            return _finish_quality(
                job_id,
                analysis_id,
                JobStatus.PDF_QUALITY_WARNING,
                report,
                artifacts_saved,
                created_at,
                admin_only,
            )
        # OK
        return _finish_quality(
            job_id,
            analysis_id,
            JobStatus.PDF_QUALITY_OK,
            report,
            artifacts_saved,
            created_at,
            admin_only,
        )

    except Exception as exc:  # FAIL CLOSED — never fall back to old analyzer.
        return _finish_failed_analysis(
            job_id, analysis_id, exc, saved_status_path, created_at, admin_only
        )


def _finish_quality(
    job_id: str,
    analysis_id: str,
    status: str,
    report: Dict[str, Any],
    artifacts_saved: Dict[str, Any],
    created_at: str,
    admin_only: bool,
) -> Dict[str, Any]:
    """Terminal OK/WARNING status (no analysis in this step)."""
    extra = {
        "message": STEP1_OK_MESSAGE,
        "pdf_quality_status": report.get("quality_status"),
        "pdf_quality_warnings": report.get("warnings", []),
        "key_sections_detected": report.get("key_sections_detected", {}),
    }
    payload = job_status.make_status(
        job_id=job_id,
        analysis_id=analysis_id,
        status=status,
        current_stage=_stage("pdf_quality_checked"),
        admin_only=admin_only,
        customer_report_generated=False,
        safe_to_show_customer=False,
        artifacts_saved=artifacts_saved,
        created_at=created_at,
        extra=extra,
    )
    artifacts.save_job_status(job_id, payload)
    return payload


def _finish_blocked(
    job_id: str,
    analysis_id: str,
    report: Dict[str, Any],
    artifacts_saved: Dict[str, Any],
    created_at: str,
    admin_only: bool,
) -> Dict[str, Any]:
    """Terminal PDF_QUALITY_BLOCKED status — fail closed, full diagnostics."""
    reason_code = report.get("reason_code") or "PDF_QUALITY_BLOCKED"
    reason_human = report.get("reason_human") or "Qualità del PDF non sufficiente."
    troubleshoot = report.get("troubleshoot_message") or (
        "Il blocco qualità PDF ha interrotto l'analisi."
    )
    next_steps = report.get("next_steps") or [
        "Verificare il PDF di origine e rieseguire l'estrazione."
    ]
    payload = job_status.make_failure_status(
        job_id=job_id,
        analysis_id=analysis_id,
        status=JobStatus.PDF_QUALITY_BLOCKED,
        current_stage=_stage("pdf_quality_blocked"),
        reason_code=reason_code,
        reason_human=reason_human,
        troubleshoot_message=troubleshoot,
        next_steps=next_steps,
        artifacts_saved=artifacts_saved,
        created_at=created_at,
        admin_only=admin_only,
        extra={"pdf_quality_status": PdfQualityStatus.BLOCKED},
    )
    error_path = artifacts.save_error(
        job_id,
        {
            "status": JobStatus.PDF_QUALITY_BLOCKED,
            "reason_code": reason_code,
            "reason_human": reason_human,
            "troubleshoot_message": troubleshoot,
            "next_steps": next_steps,
            "pdf_quality_report_summary": {
                "quality_status": report.get("quality_status"),
                "unreadable_pages": report.get("unreadable_pages"),
                "missing_sections": report.get("details", {}).get("missing_sections"),
            },
        },
    )
    payload["artifacts_saved"]["error"] = error_path
    artifacts.save_job_status(job_id, payload)
    return payload


def _finish_failed_analysis(
    job_id: str,
    analysis_id: str,
    exc: Exception,
    saved_status_path: str,
    created_at: str,
    admin_only: bool,
) -> Dict[str, Any]:
    """Unexpected failure -> FAILED_ANALYSIS. Fail closed. No old fallback."""
    detail = f"{type(exc).__name__}: {exc}"
    artifacts_saved = {"job_status": saved_status_path}
    try:
        error_path = artifacts.save_error(
            job_id,
            {
                "status": JobStatus.FAILED_ANALYSIS,
                "reason_code": "CORRECTNESS_V2_UNEXPECTED_ERROR",
                "exception": detail,
                "traceback": traceback.format_exc(),
                "no_old_fallback": True,
            },
        )
        artifacts_saved["error"] = error_path
    except Exception:
        pass

    payload = job_status.make_failure_status(
        job_id=job_id,
        analysis_id=analysis_id,
        status=JobStatus.FAILED_ANALYSIS,
        current_stage=_stage("failed_analysis"),
        reason_code="CORRECTNESS_V2_UNEXPECTED_ERROR",
        reason_human="Si è verificato un errore inatteso durante la Correctness Mode.",
        troubleshoot_message=(
            "La Correctness Mode è fallita in modo controllato (fail-closed). "
            f"Dettaglio tecnico: {detail}. Nessun fallback al vecchio analizzatore "
            "è stato eseguito."
        ),
        next_steps=[
            "Controllare error.json nella cartella del job per il traceback.",
            "Verificare l'estrazione delle pagine per questa analisi.",
            "Riprovare dopo aver corretto la causa dell'errore.",
        ],
        artifacts_saved=artifacts_saved,
        created_at=created_at,
        admin_only=admin_only,
    )
    try:
        artifacts.save_job_status(job_id, payload)
    except Exception:
        pass
    return payload
