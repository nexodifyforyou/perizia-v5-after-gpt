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
from . import (
    artifacts,
    contract as contract_mod,
    feature_flags,
    job_status,
    lot_packets as lot_packets_mod,
    lots as lots_mod,
    validator as validator_mod,
)
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

STEP3_SELECTED_LOT_MESSAGE = (
    "Single-lot contract built from the selected lot's isolated page context "
    "(no lot blending)."
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
    selected_lot_id: Optional[str] = None,
    analyze_all: bool = False,
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

        # Step 2/3 pipeline (analyst -> lot routing -> validator -> contract).
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
            selected_lot_id=selected_lot_id,
            analyze_all=analyze_all,
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
    selected_lot_id: Optional[str] = None,
    analyze_all: bool = False,
) -> Dict[str, Any]:
    """Run the generic lot-aware pipeline. Fails closed at every stage.

    Flow:
      analyst (full doc) -> lot detection -> route:
        * single lot (incl. multi-bene) -> validator -> contract (CONTRACT_READY)
        * multi-lot + analyze_all        -> per-lot contracts
        * multi-lot + selected_lot_id    -> re-analyze that lot's pages only
        * multi-lot + no selection       -> LOT_SELECTION_REQUIRED (never blended)
    """
    source_quality = report.get("quality_status") or PdfQualityStatus.OK

    # 1) OpenAI analyst worksheet (full document — used for lot detection + index).
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

    # 1b) Lot detection (deterministic + wording-agnostic via the analyst lots[]).
    lot_report = lots_mod.build_lot_report(result.worksheet, pages)
    artifacts_saved["lot_report"] = artifacts.save_lot_report(job_id, lot_report)

    # SINGLE LOT (including multi-bene inside one lot): proceed normally. Multi-bene
    # is never blocked — the gate keys only on distinct lots, never on beni.
    if not lot_report.get("multi_lot"):
        return _build_single_lot_contract(
            job_id=job_id,
            analysis_id=analysis_id,
            worksheet=result.worksheet,
            pages=pages,
            lot_report=lot_report,
            artifacts_saved=artifacts_saved,
            created_at=created_at,
            admin_only=admin_only,
            source_quality=source_quality,
            model_name=result.model,
            extra={"multi_lot": False, "lot_count": lot_report.get("lot_count", 1)},
        )

    # MULTI-LOT: always build the inspectable lot index + per-lot packets. We never
    # blend lots; the document is segmented page-by-page into per-lot contexts.
    segmentation = lot_packets_mod.segment_pages(pages, lot_report.get("lot_ids"))
    lot_index = lot_packets_mod.build_lot_index(result.worksheet, pages, lot_report, segmentation)
    per_lot_packets = lot_packets_mod.build_per_lot_packets(result.worksheet, pages, lot_report, segmentation)
    artifacts_saved["lot_index"] = artifacts.save_lot_index(job_id, lot_index)
    artifacts_saved["per_lot_packets"] = artifacts.save_per_lot_packets(job_id, per_lot_packets)

    if analyze_all:
        return _run_analyze_all(
            job_id=job_id,
            analysis_id=analysis_id,
            pages=pages,
            worksheet=result.worksheet,
            lot_report=lot_report,
            segmentation=segmentation,
            lot_index=lot_index,
            artifacts_saved=artifacts_saved,
            created_at=created_at,
            admin_only=admin_only,
            source_quality=source_quality,
            openai_caller=openai_caller,
            model=model,
        )

    if selected_lot_id is not None and str(selected_lot_id).strip() != "":
        violation = validator_mod.check_selected_lot_present(
            lot_report.get("lot_ids", []), selected_lot_id
        )
        if violation is not None:
            return _finish_selected_lot_not_found(
                job_id, analysis_id, selected_lot_id, lot_report, violation,
                artifacts_saved, created_at, admin_only,
            )
        return _run_selected_lot(
            job_id=job_id,
            analysis_id=analysis_id,
            pages=pages,
            worksheet=result.worksheet,
            lot_report=lot_report,
            segmentation=segmentation,
            lot_index=lot_index,
            selected_lot_id=str(selected_lot_id).strip(),
            artifacts_saved=artifacts_saved,
            created_at=created_at,
            admin_only=admin_only,
            source_quality=source_quality,
            openai_caller=openai_caller,
            model=model,
        )

    # No selection, no analyze_all -> ask the caller to choose (expected behavior).
    return _finish_lot_selection_required(
        job_id, analysis_id, lot_report, lot_index, artifacts_saved, created_at, admin_only
    )


def _step2_stage(name: str) -> str:
    return f"step2:{name}"


def _step3_stage(name: str) -> str:
    return f"step3:{name}"


# ---------------------------------------------------------------------------
# Single-lot contract build (shared by the single-lot and selected-lot paths)
# ---------------------------------------------------------------------------
def _build_single_lot_contract(
    *,
    job_id: str,
    analysis_id: str,
    worksheet: Dict[str, Any],
    pages: List[Dict[str, Any]],
    lot_report: Dict[str, Any],
    artifacts_saved: Dict[str, Any],
    created_at: str,
    admin_only: bool,
    source_quality: str,
    model_name: str,
    extra: Optional[Dict[str, Any]] = None,
    current_stage: Optional[str] = None,
    message: str = STEP2_OK_MESSAGE,
) -> Dict[str, Any]:
    """Validate a single-lot worksheet and build the verified contract (CONTRACT_READY).

    This is the ONLY path that ever produces verified_report_contract.json, and it
    only runs when exactly one safe lot context exists (single-lot doc, or a chosen
    lot's isolated re-analysis). Fails closed on validation / build errors.
    """
    validator_report = validator_mod.validate_worksheet(worksheet, pages)
    artifacts_saved["validator_report"] = artifacts.save_validator_report(
        job_id, validator_report
    )

    if validator_report.get("validation_status") != validator_mod.STATUS_VALIDATED:
        return _finish_validation_failed(
            job_id, analysis_id, validator_report, artifacts_saved, created_at, admin_only
        )

    try:
        contract = contract_mod.build_contract(
            worksheet=worksheet,
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

    payload_extra = {
        "message": message,
        "pdf_quality_status": source_quality,
        "openai_model": model_name,
        "validation_status": validator_report.get("validation_status"),
        "validator_warning_count": validator_report.get("warning_count", 0),
        "contract_generated": True,
        "contract_schema_version": contract.get("schema_version"),
    }
    if extra:
        payload_extra.update(extra)
    payload = job_status.make_status(
        job_id=job_id,
        analysis_id=analysis_id,
        status=JobStatus.CONTRACT_READY,
        current_stage=current_stage or _step2_stage("contract_ready"),
        admin_only=admin_only,
        customer_report_generated=False,  # admin-only / shadow: not customer-facing yet
        safe_to_show_customer=False,
        artifacts_saved=artifacts_saved,
        created_at=created_at,
        extra=payload_extra,
    )
    artifacts.save_job_status(job_id, payload)
    return payload


# ---------------------------------------------------------------------------
# Multi-lot: selected lot -> re-analyze ONLY that lot's isolated pages
# ---------------------------------------------------------------------------
def _run_selected_lot(
    *,
    job_id: str,
    analysis_id: str,
    pages: List[Dict[str, Any]],
    worksheet: Dict[str, Any],
    lot_report: Dict[str, Any],
    segmentation: Dict[str, Any],
    lot_index: Dict[str, Any],
    selected_lot_id: str,
    artifacts_saved: Dict[str, Any],
    created_at: str,
    admin_only: bool,
    source_quality: str,
    openai_caller: OpenAICaller,
    model: Optional[str],
) -> Dict[str, Any]:
    """Analyze ONLY the selected lot, from its isolated page context (no blending).

    The selected lot's safe page subset (global + lot-specific pages, shared
    multi-lot pages excluded) is re-analyzed into a fresh single-lot worksheet, then
    validated and turned into a contract. Defense-in-depth: if the re-analysis still
    looks multi-lot, validation fails closed.
    """
    norm_lot = lots_mod.normalize_lot_token(selected_lot_id) or selected_lot_id
    selected_pages = lot_packets_mod.select_lot_pages(pages, segmentation, norm_lot)
    context = lot_packets_mod.build_selected_lot_context(
        pages, segmentation, norm_lot, lot_index, worksheet=worksheet
    )
    artifacts_saved["selected_lot_context"] = artifacts.save_selected_lot_context(job_id, context)

    if not selected_pages:
        # Nothing safe to analyze for this lot — genuine ambiguity, not a blend.
        return _finish_lot_ambiguous(
            job_id, analysis_id, selected_lot_id, lot_report,
            artifacts_saved, created_at, admin_only,
            detail=(
                f"Per il lotto selezionato '{selected_lot_id}' non sono state isolate "
                "pagine sicure (solo pagine globali/condivise). Selezione non risolvibile "
                "automaticamente senza segmentazione manuale."
            ),
        )

    try:
        result = analyst_mod.run_analyst(
            selected_pages, openai_caller=openai_caller, model=model, target_lot=str(norm_lot)
        )
    except AnalystError as exc:
        return _finish_analyst_failed(
            job_id, analysis_id, exc, artifacts_saved, created_at, admin_only
        )

    artifacts_saved["selected_lot_worksheet"] = artifacts.save_lot_subartifact(
        job_id, norm_lot, artifacts.ANALYST_WORKSHEET_FILE, result.worksheet
    )

    sub_lot_report = lots_mod.build_lot_report(result.worksheet, selected_pages)
    extra = {
        "multi_lot": True,
        "selected_lot": str(norm_lot),
        "lot_count": lot_report.get("lot_count"),
        "lot_ids": lot_report.get("lot_ids", []),
        "analyzed_pages": context.get("analysis_pages", []),
    }
    return _build_single_lot_contract(
        job_id=job_id,
        analysis_id=analysis_id,
        worksheet=result.worksheet,
        pages=selected_pages,
        lot_report=sub_lot_report,
        artifacts_saved=artifacts_saved,
        created_at=created_at,
        admin_only=admin_only,
        source_quality=source_quality,
        model_name=result.model,
        extra=extra,
        current_stage=_step3_stage("selected_lot_contract_ready"),
        message=STEP3_SELECTED_LOT_MESSAGE,
    )


# ---------------------------------------------------------------------------
# Multi-lot: analyze_all -> a separate contract per lot (never blended)
# ---------------------------------------------------------------------------
def _run_analyze_all(
    *,
    job_id: str,
    analysis_id: str,
    pages: List[Dict[str, Any]],
    worksheet: Dict[str, Any],
    lot_report: Dict[str, Any],
    segmentation: Dict[str, Any],
    lot_index: Dict[str, Any],
    artifacts_saved: Dict[str, Any],
    created_at: str,
    admin_only: bool,
    source_quality: str,
    openai_caller: OpenAICaller,
    model: Optional[str],
) -> Dict[str, Any]:
    """Analyze every lot separately, each from its own isolated page context.

    Produces one independent (validated) contract per lot under
    jobs/{job_id}/lots/{lot_id}/. No lot is blended with another. The parent job is
    CONTRACT_READY only if every lot produced a contract; otherwise it is
    NEEDS_MANUAL_REVIEW listing which lots could not be safely produced.
    """
    per_lot_results: List[Dict[str, Any]] = []
    for lot_id in lot_report.get("lot_ids", []):
        norm_lot = lots_mod.normalize_lot_token(lot_id) or lot_id
        selected_pages = lot_packets_mod.select_lot_pages(pages, segmentation, norm_lot)
        context = lot_packets_mod.build_selected_lot_context(
            pages, segmentation, norm_lot, lot_index, worksheet=worksheet
        )
        artifacts.save_lot_subartifact(
            job_id, norm_lot, artifacts.SELECTED_LOT_CONTEXT_FILE, context
        )

        entry: Dict[str, Any] = {
            "lot_id": str(norm_lot),
            "analyzed_pages": context.get("analysis_pages", []),
        }
        if not selected_pages:
            entry.update({"status": JobStatus.NEEDS_MANUAL_REVIEW, "reason": "no_isolated_pages"})
            per_lot_results.append(entry)
            continue

        try:
            result = analyst_mod.run_analyst(
                selected_pages, openai_caller=openai_caller, model=model, target_lot=str(norm_lot)
            )
        except AnalystError as exc:
            entry.update({"status": JobStatus.FAILED_ANALYSIS, "reason": str(exc)})
            per_lot_results.append(entry)
            continue

        artifacts.save_lot_subartifact(
            job_id, norm_lot, artifacts.ANALYST_WORKSHEET_FILE, result.worksheet
        )
        validator_report = validator_mod.validate_worksheet(result.worksheet, selected_pages)
        artifacts.save_lot_subartifact(
            job_id, norm_lot, artifacts.VALIDATOR_REPORT_FILE, validator_report
        )
        if validator_report.get("validation_status") != validator_mod.STATUS_VALIDATED:
            entry.update(
                {
                    "status": JobStatus.CONTRACT_VALIDATION_FAILED,
                    "violation_codes": sorted(
                        {v.get("code") for v in validator_report.get("violations", []) if v.get("code")}
                    ),
                }
            )
            per_lot_results.append(entry)
            continue

        sub_lot_report = lots_mod.build_lot_report(result.worksheet, selected_pages)
        try:
            contract = contract_mod.build_contract(
                worksheet=result.worksheet,
                validator_report=validator_report,
                analysis_id=analysis_id,
                job_id=job_id,
                source_pdf_quality_status=source_quality,
                lot_report=sub_lot_report,
            )
        except Exception as exc:  # noqa: BLE001 - recorded per lot, never blended
            entry.update({"status": JobStatus.FAILED_CONTRACT_BUILD, "reason": str(exc)})
            per_lot_results.append(entry)
            continue

        path = artifacts.save_lot_subartifact(
            job_id, norm_lot, artifacts.VERIFIED_CONTRACT_FILE, contract
        )
        entry.update({"status": JobStatus.CONTRACT_READY, "contract_path": path})
        per_lot_results.append(entry)

    all_ok = bool(per_lot_results) and all(
        e.get("status") == JobStatus.CONTRACT_READY for e in per_lot_results
    )
    aggregate = {
        "analyze_all": True,
        "lot_count": lot_report.get("lot_count"),
        "lot_ids": lot_report.get("lot_ids", []),
        "all_lots_ready": all_ok,
        "per_lot_results": per_lot_results,
    }
    artifacts_saved["analyze_all_result"] = artifacts.save_analyze_all_result(job_id, aggregate)

    status = JobStatus.CONTRACT_READY if all_ok else JobStatus.NEEDS_MANUAL_REVIEW
    extra = {
        "message": "analyze_all: una contratto verificato per ciascun lotto (nessuna fusione).",
        "multi_lot": True,
        "analyze_all": True,
        "lot_count": lot_report.get("lot_count"),
        "lot_ids": lot_report.get("lot_ids", []),
        "per_lot_results": per_lot_results,
        "all_lots_ready": all_ok,
        "contract_generated": all_ok,
    }
    common = dict(
        job_id=job_id,
        analysis_id=analysis_id,
        status=status,
        current_stage=_step3_stage("analyze_all"),
        admin_only=admin_only,
        artifacts_saved=artifacts_saved,
        created_at=created_at,
        extra=extra,
    )
    if status == JobStatus.NEEDS_MANUAL_REVIEW:
        payload = job_status.make_status(
            customer_report_generated=False,
            safe_to_show_customer=False,
            reason_code="ANALYZE_ALL_PARTIAL",
            reason_human="Alcuni lotti non hanno prodotto un contratto sicuro in modalità analyze_all.",
            **common,
        )
    else:
        payload = job_status.make_status(
            customer_report_generated=False,
            safe_to_show_customer=False,
            **common,
        )
    artifacts.save_job_status(job_id, payload)
    return payload


# ---------------------------------------------------------------------------
# Multi-lot: no selection -> LOT_SELECTION_REQUIRED (expected, not a failure)
# ---------------------------------------------------------------------------
def _finish_lot_selection_required(
    job_id: str,
    analysis_id: str,
    lot_report: Dict[str, Any],
    lot_index: Dict[str, Any],
    artifacts_saved: Dict[str, Any],
    created_at: str,
    admin_only: bool,
) -> Dict[str, Any]:
    """Multi-lot document with no chosen lot -> LOT_SELECTION_REQUIRED.

    This is NOT a failure: the analysis succeeded and we segmented the document per
    lot. No blended contract is produced. The caller must pick a target lot
    (selected_lot_id) or request analyze_all.
    """
    lot_ids = lot_report.get("lot_ids", [])
    lot_count = lot_report.get("lot_count", len(lot_ids))

    available_lots = [
        {
            "lot_id": L.get("lot_id"),
            "label": L.get("label"),
            "address": L.get("address"),
            "property_type": L.get("property_type"),
            "ownership_right": L.get("ownership_right"),
            "occupancy_summary": L.get("occupancy_summary"),
            "key_money": L.get("key_money", []),
            "page_evidence": L.get("page_evidence", []),
            "confidence": L.get("confidence"),
            "notes": L.get("notes", []),
        }
        for L in lot_index.get("lots", [])
    ]

    selection_payload = {
        "schema_version": "cv2.lot_selection_required.v1",
        "analysis_id": analysis_id,
        "job_id": job_id,
        "status": JobStatus.LOT_SELECTION_REQUIRED,
        "reason_code": "LOT_SELECTION_REQUIRED",
        "multi_lot": True,
        "lot_count": lot_count,
        "lot_ids": lot_ids,
        "message": (
            f"Rilevati {lot_count} lotti distinti. Selezionare un lotto da analizzare "
            "oppure richiedere l'analisi di tutti i lotti. I lotti non vengono mai fusi."
        ),
        "available_lots": available_lots,
        "available_actions": [
            {
                "action": "analyze_selected_lot",
                "parameter": "selected_lot_id",
                "values": lot_ids,
            },
            {"action": "analyze_all", "parameter": "analyze_all", "analyze_all_supported": True},
        ],
    }
    artifacts_saved["lot_selection_required"] = artifacts.save_lot_selection_required(
        job_id, selection_payload
    )

    payload = job_status.make_status(
        job_id=job_id,
        analysis_id=analysis_id,
        status=JobStatus.LOT_SELECTION_REQUIRED,
        current_stage=_step3_stage("lot_selection_required"),
        admin_only=admin_only,
        customer_report_generated=False,
        safe_to_show_customer=False,
        reason_code="LOT_SELECTION_REQUIRED",
        reason_human=(
            f"La perizia contiene {lot_count} lotti distinti ({', '.join(str(x) for x in lot_ids)}). "
            "Selezionare un lotto (selected_lot_id) o richiedere analyze_all. I lotti non "
            "vengono mai fusi."
        ),
        artifacts_saved=artifacts_saved,
        created_at=created_at,
        extra={
            "multi_lot": True,
            "lot_count": lot_count,
            "lot_ids": lot_ids,
            "available_lots": available_lots,
            "available_actions": selection_payload["available_actions"],
            "selected_lot": None,
            "contract_generated": False,
            "blended_report_prevented": True,
        },
    )
    artifacts.save_job_status(job_id, payload)
    return payload


def _finish_selected_lot_not_found(
    job_id: str,
    analysis_id: str,
    selected_lot_id: Any,
    lot_report: Dict[str, Any],
    violation: Dict[str, Any],
    artifacts_saved: Dict[str, Any],
    created_at: str,
    admin_only: bool,
) -> Dict[str, Any]:
    """Selected lot not present in a multi-lot document -> CONTRACT_VALIDATION_FAILED."""
    lot_ids = lot_report.get("lot_ids", [])
    error_path = artifacts.save_error(
        job_id,
        {
            "status": JobStatus.CONTRACT_VALIDATION_FAILED,
            "stage": "step3:selected_lot",
            "reason_code": "SELECTED_LOT_NOT_FOUND",
            "selected_lot_id": selected_lot_id,
            "available_lot_ids": lot_ids,
            "violation": violation,
            "no_report": True,
        },
    )
    artifacts_saved["error"] = error_path
    payload = job_status.make_failure_status(
        job_id=job_id,
        analysis_id=analysis_id,
        status=JobStatus.CONTRACT_VALIDATION_FAILED,
        current_stage=_step3_stage("selected_lot_not_found"),
        reason_code="SELECTED_LOT_NOT_FOUND",
        reason_human=(
            f"Il lotto selezionato '{selected_lot_id}' non esiste nel documento. "
            f"Lotti disponibili: {', '.join(str(x) for x in lot_ids)}."
        ),
        troubleshoot_message=(
            "La selezione del lotto non corrisponde ad alcun lotto rilevato. Nessun "
            "report è stato generato (fail-closed). Controllare lot_index.json per i "
            "lotti disponibili."
        ),
        next_steps=[
            "Selezionare un selected_lot_id presente in lot_index.json.",
            "Oppure richiedere analyze_all per analizzare tutti i lotti.",
        ],
        artifacts_saved=artifacts_saved,
        created_at=created_at,
        admin_only=admin_only,
        extra={"no_report": True, "available_lot_ids": lot_ids, "selected_lot_id": selected_lot_id},
    )
    artifacts.save_job_status(job_id, payload)
    return payload


def _finish_lot_ambiguous(
    job_id: str,
    analysis_id: str,
    selected_lot_id: Any,
    lot_report: Dict[str, Any],
    artifacts_saved: Dict[str, Any],
    created_at: str,
    admin_only: bool,
    *,
    detail: str,
) -> Dict[str, Any]:
    """Genuine ambiguity (no safe isolated pages for the chosen lot) -> NEEDS_MANUAL_REVIEW."""
    payload = job_status.make_status(
        job_id=job_id,
        analysis_id=analysis_id,
        status=JobStatus.NEEDS_MANUAL_REVIEW,
        current_stage=_step3_stage("lot_ambiguous"),
        admin_only=admin_only,
        customer_report_generated=False,
        safe_to_show_customer=False,
        reason_code="LOT_SEGMENTATION_AMBIGUOUS",
        reason_human=detail,
        troubleshoot_message=(
            "La segmentazione automatica non ha isolato pagine sicure per il lotto "
            "selezionato. Revisione manuale necessaria; nessun lotto è stato fuso."
        ),
        next_steps=[
            "Verificare la struttura del PDF per il lotto selezionato.",
            "Eventualmente fornire un intervallo di pagine per il lotto.",
        ],
        artifacts_saved=artifacts_saved,
        created_at=created_at,
        extra={
            "multi_lot": True,
            "selected_lot": str(selected_lot_id),
            "lot_ids": lot_report.get("lot_ids", []),
            "contract_generated": False,
            "manual_review_required": True,
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
