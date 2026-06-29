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

from . import analyst as analyst_mod
from . import artifacts, contract as contract_mod, feature_flags, job_status, lots as lots_mod, validator as validator_mod
from .analyst import AnalystError
from .pdf_quality import assess_pdf_quality
from .schemas import JobStatus, PdfQualityStatus

# Sentinel string asserting intent; referenced by the no-old-fallback guard test.
NO_OLD_ANALYZER_FALLBACK = True

STEP1_OK_MESSAGE = (
    "PDF quality checked. Correctness analysis not implemented in this step."
)

STEP2_OK_MESSAGE = (
    "Contract built from generic OpenAI analyst worksheet and deterministic validation."
)

PageLoader = Callable[[str], List[Dict[str, Any]]]
# A caller compatible with openai_client.call_openai_json(messages, *, model=None).
OpenAICaller = Callable[..., Dict[str, Any]]


def _stage(name: str) -> str:
    return f"step1:{name}"


def start_job(
    analysis_id: str,
    page_loader: PageLoader,
    *,
    is_admin: bool = True,
    ocr_failed: bool = False,
    openai_caller: Optional[OpenAICaller] = None,
    model: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create and run a Correctness v2 job for ``analysis_id``.

    ``page_loader`` is a callable that returns the list of
    ``{"page_number": int, "text": str, ...}`` pages for the analysis. It is
    injected so this orchestrator never reaches into the old pipeline directly.

    ``openai_caller`` is the (injected) OpenAI invoker for Step 2. When it is
    ``None`` the job stops after the PDF-quality gate (Step 1 behavior). When it
    is provided AND quality is OK/WARNING, the job runs the Step 2 pipeline:
    analyst worksheet -> deterministic validator -> verified_report_contract.
    If quality is BLOCKED, OpenAI is NEVER called.

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
            # Fail closed: OpenAI is NEVER called when PDF quality is blocked.
            return _finish_blocked(
                job_id, analysis_id, report, artifacts_saved, created_at, admin_only
            )

        # OK / WARNING -> eligible for Step 2.
        quality_job_status = (
            JobStatus.PDF_QUALITY_WARNING
            if quality_status == PdfQualityStatus.WARNING
            else JobStatus.PDF_QUALITY_OK
        )

        if openai_caller is None:
            # Step-1-only mode: stop at the quality gate (no analysis here).
            return _finish_quality(
                job_id,
                analysis_id,
                quality_job_status,
                report,
                artifacts_saved,
                created_at,
                admin_only,
            )

        # Step 2 pipeline (analyst -> validator -> contract).
        return _run_step2(
            job_id=job_id,
            analysis_id=analysis_id,
            pages=pages,
            report=report,
            artifacts_saved=artifacts_saved,
            created_at=created_at,
            admin_only=admin_only,
            openai_caller=openai_caller,
            model=model,
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


# ---------------------------------------------------------------------------
# Step 2: OpenAI analyst worksheet -> deterministic validator -> contract
# ---------------------------------------------------------------------------
def _run_step2(
    *,
    job_id: str,
    analysis_id: str,
    pages: List[Dict[str, Any]],
    report: Dict[str, Any],
    artifacts_saved: Dict[str, Any],
    created_at: str,
    admin_only: bool,
    openai_caller: OpenAICaller,
    model: Optional[str],
) -> Dict[str, Any]:
    """Run the generic Step 2 pipeline. Fails closed at every stage."""
    source_quality = report.get("quality_status") or PdfQualityStatus.OK

    # 1) OpenAI analyst worksheet.
    try:
        result = analyst_mod.run_analyst(pages, openai_caller=openai_caller, model=model)
    except AnalystError as exc:
        # Persist the redacted request if we can rebuild it (no secrets).
        try:
            messages = analyst_mod.build_messages(pages)
            from .openai_client import redacted_request

            req_path = artifacts.save_openai_request(
                job_id, redacted_request(messages, model=model)
            )
            artifacts_saved["openai_request"] = req_path
        except Exception:
            pass
        return _finish_analyst_failed(
            job_id, analysis_id, exc, artifacts_saved, created_at, admin_only
        )

    # Persist analyst artifacts (request is already redacted / secret-free).
    artifacts_saved["openai_request"] = artifacts.save_openai_request(
        job_id, result.redacted_request
    )
    artifacts_saved["openai_response"] = artifacts.save_openai_response(
        job_id, result.response_artifact
    )
    artifacts_saved["analyst_worksheet"] = artifacts.save_analyst_worksheet(
        job_id, result.worksheet
    )

    # 1b) Multi-lot gate (fail closed). A perizia with two or more lots must NEVER
    # be blended into a single customer contract. We detect lots deterministically
    # and, when more than one is present, stop at NEEDS_MANUAL_REVIEW with a
    # per-lot index instead of guessing/contaminating.
    lot_report = lots_mod.build_lot_report(result.worksheet, pages)
    artifacts_saved["lot_report"] = artifacts.save_lot_report(job_id, lot_report)
    if lot_report.get("multi_lot"):
        return _finish_multilot_manual_review(
            job_id, analysis_id, lot_report, artifacts_saved, created_at, admin_only
        )

    # 2) Deterministic validator.
    validator_report = validator_mod.validate_worksheet(result.worksheet, pages)
    artifacts_saved["validator_report"] = artifacts.save_validator_report(
        job_id, validator_report
    )

    if validator_report.get("validation_status") != validator_mod.STATUS_VALIDATED:
        return _finish_validation_failed(
            job_id, analysis_id, validator_report, artifacts_saved, created_at, admin_only
        )

    # 3) Deterministic, renderer-ready contract.
    try:
        contract = contract_mod.build_contract(
            worksheet=result.worksheet,
            validator_report=validator_report,
            analysis_id=analysis_id,
            job_id=job_id,
            source_pdf_quality_status=source_quality,
            lot_report=lot_report,
        )
    except Exception as exc:
        return _finish_contract_build_failed(
            job_id, analysis_id, exc, artifacts_saved, created_at, admin_only
        )

    artifacts_saved["verified_report_contract"] = artifacts.save_verified_contract(
        job_id, contract
    )

    extra = {
        "message": STEP2_OK_MESSAGE,
        "pdf_quality_status": source_quality,
        "openai_model": result.model,
        "validation_status": validator_report.get("validation_status"),
        "validator_warning_count": validator_report.get("warning_count", 0),
        "contract_generated": True,
        "contract_schema_version": contract.get("schema_version"),
    }
    payload = job_status.make_status(
        job_id=job_id,
        analysis_id=analysis_id,
        status=JobStatus.CONTRACT_READY,
        current_stage=_step2_stage("contract_ready"),
        admin_only=admin_only,
        customer_report_generated=False,  # admin-only / shadow: not customer-facing yet
        safe_to_show_customer=False,
        artifacts_saved=artifacts_saved,
        created_at=created_at,
        extra=extra,
    )
    artifacts.save_job_status(job_id, payload)
    return payload


def _step2_stage(name: str) -> str:
    return f"step2:{name}"


def _finish_multilot_manual_review(
    job_id: str,
    analysis_id: str,
    lot_report: Dict[str, Any],
    artifacts_saved: Dict[str, Any],
    created_at: str,
    admin_only: bool,
) -> Dict[str, Any]:
    """Multi-lot document -> NEEDS_MANUAL_REVIEW. No blended contract is built.

    This is not a hard failure: the analysis succeeded, but a single customer
    contract cannot be safely produced without choosing a target lot. We preserve
    the per-lot index (with evidence) so a human/UX can pick a lot next.
    """
    lot_ids = lot_report.get("lot_ids", [])
    lot_count = lot_report.get("lot_count", len(lot_ids))
    lot_index = [
        {
            "lot_id": L.get("lot_id"),
            "identifiers": L.get("identifiers", []),
            "money": L.get("money", []),
            "evidence_pages": L.get("evidence_pages", []),
        }
        for L in lot_report.get("lots", [])
    ]
    payload = job_status.make_status(
        job_id=job_id,
        analysis_id=analysis_id,
        status=JobStatus.NEEDS_MANUAL_REVIEW,
        current_stage=_step2_stage("multi_lot_manual_review"),
        admin_only=admin_only,
        customer_report_generated=False,
        safe_to_show_customer=False,
        reason_code="MULTI_LOT_MANUAL_REVIEW_REQUIRED",
        reason_human=(
            f"La perizia contiene {lot_count} lotti distinti ({', '.join(lot_ids)}). "
            "Non è possibile produrre un singolo report cliente senza selezionare il "
            "lotto di interesse; i lotti non vengono mai fusi."
        ),
        troubleshoot_message=(
            "Documento multi-lotto rilevato in modo deterministico. Il job si ferma a "
            "NEEDS_MANUAL_REVIEW (fail-closed) per non contaminare i lotti. L'indice "
            "per-lotto con le evidenze è in lot_report.json."
        ),
        next_steps=[
            "Selezionare il lotto di interesse (input target-lot non ancora disponibile nell'API).",
            "Rieseguire l'analisi sul singolo lotto selezionato.",
            "Controllare lot_report.json per l'indice per-lotto con le evidenze.",
        ],
        artifacts_saved=artifacts_saved,
        created_at=created_at,
        extra={
            "multi_lot": True,
            "lot_count": lot_count,
            "lot_ids": lot_ids,
            "lot_index": lot_index,
            "contaminated_fields": lot_report.get("contaminated_fields", []),
            "manual_review_required": True,
            "selected_lot": None,
            "no_report": True,
            "contract_generated": False,
        },
    )
    artifacts.save_job_status(job_id, payload)
    return payload


def _finish_analyst_failed(
    job_id: str,
    analysis_id: str,
    exc: AnalystError,
    artifacts_saved: Dict[str, Any],
    created_at: str,
    admin_only: bool,
) -> Dict[str, Any]:
    """OpenAI/analyst failure -> FAILED_ANALYSIS. No report. No fallback."""
    reason_code = getattr(exc, "reason_code", "ANALYST_FAILED") or "ANALYST_FAILED"
    detail = str(exc)
    error_path = artifacts.save_error(
        job_id,
        {
            "status": JobStatus.FAILED_ANALYSIS,
            "stage": "step2:analyst",
            "reason_code": reason_code,
            "detail": detail,
            "no_old_fallback": True,
            "no_report": True,
        },
    )
    artifacts_saved["error"] = error_path
    payload = job_status.make_failure_status(
        job_id=job_id,
        analysis_id=analysis_id,
        status=JobStatus.FAILED_ANALYSIS,
        current_stage=_step2_stage("analyst_failed"),
        reason_code=reason_code,
        reason_human="La generazione del foglio di lavoro analista (OpenAI) è fallita.",
        troubleshoot_message=(
            "Lo stadio OpenAI della Correctness Mode v2 è fallito in modo controllato "
            f"(fail-closed). Dettaglio: {detail}. Nessun report è stato generato e non "
            "è stato eseguito alcun fallback al vecchio analizzatore."
        ),
        next_steps=[
            "Controllare openai_request.json (senza segreti) e error.json nel job.",
            "Verificare la configurazione del modello (CORRECTNESS_V2_OPENAI_MODEL) e la chiave API.",
            "Riprovare una volta risolta la causa.",
        ],
        artifacts_saved=artifacts_saved,
        created_at=created_at,
        admin_only=admin_only,
        extra={"no_report": True},
    )
    artifacts.save_job_status(job_id, payload)
    return payload


def _finish_validation_failed(
    job_id: str,
    analysis_id: str,
    validator_report: Dict[str, Any],
    artifacts_saved: Dict[str, Any],
    created_at: str,
    admin_only: bool,
) -> Dict[str, Any]:
    """Validator rejected the worksheet -> CONTRACT_VALIDATION_FAILED. No report."""
    violations = validator_report.get("violations", [])
    codes = sorted({v.get("code") for v in violations if v.get("code")})
    error_path = artifacts.save_error(
        job_id,
        {
            "status": JobStatus.CONTRACT_VALIDATION_FAILED,
            "stage": "step2:validator",
            "reason_code": "CONTRACT_VALIDATION_FAILED",
            "violation_codes": codes,
            "violations": violations,
            "no_report": True,
        },
    )
    artifacts_saved["error"] = error_path
    payload = job_status.make_failure_status(
        job_id=job_id,
        analysis_id=analysis_id,
        status=JobStatus.CONTRACT_VALIDATION_FAILED,
        current_stage=_step2_stage("validation_failed"),
        reason_code="CONTRACT_VALIDATION_FAILED",
        reason_human="La validazione deterministica ha rifiutato il foglio di lavoro.",
        troubleshoot_message=(
            "Il validatore ha rilevato affermazioni non supportate o contraddittorie. "
            f"Codici violazione: {codes}. Nessun report è stato generato (fail-closed)."
        ),
        next_steps=[
            "Controllare validator_report.json per le violazioni puntuali.",
            "Verificare l'estrazione del PDF nelle pagine citate come evidenza.",
            "Questo documento può richiedere revisione manuale.",
        ],
        artifacts_saved=artifacts_saved,
        created_at=created_at,
        admin_only=admin_only,
        extra={"no_report": True, "violation_codes": codes},
    )
    artifacts.save_job_status(job_id, payload)
    return payload


def _finish_contract_build_failed(
    job_id: str,
    analysis_id: str,
    exc: Exception,
    artifacts_saved: Dict[str, Any],
    created_at: str,
    admin_only: bool,
) -> Dict[str, Any]:
    """Deterministic contract build crashed -> FAILED_CONTRACT_BUILD. No report."""
    detail = f"{type(exc).__name__}: {exc}"
    error_path = artifacts.save_error(
        job_id,
        {
            "status": JobStatus.FAILED_CONTRACT_BUILD,
            "stage": "step2:contract",
            "reason_code": "CONTRACT_BUILD_ERROR",
            "detail": detail,
            "traceback": traceback.format_exc(),
            "no_report": True,
        },
    )
    artifacts_saved["error"] = error_path
    payload = job_status.make_failure_status(
        job_id=job_id,
        analysis_id=analysis_id,
        status=JobStatus.FAILED_CONTRACT_BUILD,
        current_stage=_step2_stage("contract_build_failed"),
        reason_code="CONTRACT_BUILD_ERROR",
        reason_human="La costruzione deterministica del contratto è fallita.",
        troubleshoot_message=(
            "Il foglio di lavoro è stato validato ma la costruzione del contratto ha "
            f"generato un errore: {detail}. Nessun report è stato generato."
        ),
        next_steps=[
            "Controllare error.json per il traceback.",
            "Verificare la forma del foglio di lavoro (analyst_worksheet.json).",
        ],
        artifacts_saved=artifacts_saved,
        created_at=created_at,
        admin_only=admin_only,
        extra={"no_report": True},
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
