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

import concurrent.futures
import os
import traceback
from typing import Any, Callable, Dict, List, Optional

from . import analyst as analyst_mod
from . import (
    artifacts,
    contract as contract_mod,
    customer_report as customer_report_mod,
    doc_signals as doc_signals_mod,
    feature_flags,
    job_status,
    lot_packets as lot_packets_mod,
    lots as lots_mod,
    money_confirmation as money_confirmation_mod,
    quality_gate as quality_gate_mod,
    validator as validator_mod,
)
from .analyst import AnalystError
from .pdf_quality import assess_pdf_quality
from .schemas import JobStatus, PdfBlockReason, PdfQualityStatus

# Block reasons that mean "we could not read your document at all" — these are
# surfaced to the customer as a plain "upload a readable PDF" message. Other
# block reasons (missing sections, money tables, page order) stay admin-only.
_CUSTOMER_NOT_READABLE_BLOCKS = frozenset({
    PdfBlockReason.DOCUMENT_NOT_TEXT_EXTRACTABLE,
    PdfBlockReason.DOCUMENT_TEXT_EMPTY,
    PdfBlockReason.SCANNED_PDF_WITHOUT_USABLE_TEXT,
    PdfBlockReason.OCR_EXTRACTION_FAILED,
    PdfBlockReason.TOO_MANY_UNREADABLE_PAGES,
})

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

STEP3B_REPORT_MESSAGE = (
    "Customer report rendered deterministically from the verified contract "
    "(no LLM, no PDF access, no new facts)."
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
        job_id, analysis_id, lot_report, lot_index, artifacts_saved, created_at, admin_only,
        pages=pages, worksheet=result.worksheet,
    )


def _step2_stage(name: str) -> str:
    return f"step2:{name}"


def _step3_stage(name: str) -> str:
    return f"step3:{name}"


def _step4_stage(name: str) -> str:
    return f"step4:{name}"


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
    shared_summary_rows: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Validate a single-lot worksheet and build the verified contract (CONTRACT_READY).

    This is the ONLY path that ever produces verified_report_contract.json, and it
    only runs when exactly one safe lot context exists (single-lot doc, or a chosen
    lot's isolated re-analysis). Fails closed on validation / build errors.
    """
    # Deterministic valuation-chain completion: make grounded doc_signals the
    # authority for the SELECTED lot's terminal net values (state-of-fact /
    # judicial sale) — correcting an analyst mislabel and injecting a missing
    # terminal — plus label-promotion from uncertain_money, BEFORE validation, so
    # the validator chain check, the contract chain and the coverage audit all
    # see the same values. Grounded + additive: already-correct perizie untouched.
    worksheet = contract_mod.complete_valuation_terminals(worksheet, pages)

    # Deterministic compliance evidence gate: unsupported 'conforming' claims (and
    # claims with no evidence at all) are downgraded to 'uncertain' + manual review
    # BEFORE validation, so the contract never overclaims and never defaults to
    # conforming. The validator keeps its own overclaim checks as defense-in-depth.
    worksheet, gate_report = validator_mod.apply_compliance_evidence_gate(worksheet, pages)
    artifacts_saved["compliance_gate_report"] = artifacts.save_compliance_gate_report(
        job_id, gate_report
    )

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
            shared_summary_rows=shared_summary_rows,
            # Deterministic surface/cadastral facts read verbatim from THIS
            # contract's page context (single lot / isolated selected-lot pages).
            surface_cadastral=doc_signals_mod.extract_surface_cadastral(pages),
        )
    except Exception as exc:
        return _finish_contract_build_failed(
            job_id, analysis_id, exc, artifacts_saved, created_at, admin_only
        )

    artifacts_saved["verified_report_contract"] = artifacts.save_verified_contract(
        job_id, contract
    )

    # Step 3B: deterministic customer report rendered ONLY from the verified
    # contract (no LLM, no PDF). Render failure fails closed — never a half report.
    try:
        customer_report = customer_report_mod.render_success_report(contract, pages)
    except Exception as exc:
        return _finish_report_render_failed(
            job_id, analysis_id, exc, artifacts_saved, created_at, admin_only
        )
    artifacts_saved["customer_report"] = artifacts.save_customer_report(
        job_id, customer_report
    )

    # Step 4: no-silent-omissions quality gate. A rendered report is NEVER
    # exposed as clean REPORT_READY unless the coverage/quality audit passes.
    # A gate crash fails closed (quality failure), never skips the audit.
    try:
        gate = quality_gate_mod.run_quality_gate(
            job_id=job_id,
            analysis_id=analysis_id,
            pages=pages,
            worksheet=worksheet,
            contract=contract,
            customer_report=customer_report,
            validator_report=validator_report,
            lot_report=lot_report,
            artifacts_saved=artifacts_saved,
        )
    except Exception as exc:  # noqa: BLE001 — fail closed on the gate itself
        return _finish_quality_gate_error(
            job_id, analysis_id, exc, artifacts_saved, created_at, admin_only
        )

    if gate["gate_status"] == quality_gate_mod.GATE_FAIL:
        # Human-in-the-loop money confirmation: when the ONLY thing blocking the
        # report is a small, resolvable set of money-role ambiguities (an amount
        # the document supports under >=2 readings, e.g. market value vs
        # regularization cost), PAUSE and ask the customer to confirm instead of
        # a NEEDS_MANUAL_REVIEW dead end. Anything else (missing critical facts,
        # unresolvable omissions, too many ambiguities) stays manual review.
        mc_payload = money_confirmation_mod.build_money_confirmation(
            analysis_id=analysis_id,
            job_id=job_id,
            coverage_audit=gate.get("coverage_audit") or {},
            blocking_issues=(gate.get("quality_report") or {}).get("blocking_issues") or [],
        )
        if mc_payload:
            return _finish_money_confirmation_required(
                job_id, analysis_id, gate, mc_payload, customer_report,
                artifacts_saved, created_at, admin_only,
            )
        return _finish_quality_gate_failed(
            job_id, analysis_id, gate, artifacts_saved, created_at, admin_only
        )

    payload_extra = {
        "quality_gate_status": gate["gate_status"],
        "coverage_status": gate["coverage_audit"].get("coverage_status"),
        "quality_status": gate["quality_report"].get("overall_quality_status"),
        "customer_readiness": gate["quality_report"].get("customer_readiness"),
        "satisfaction_score": gate["scorecard"].get("overall_score"),
        "satisfaction_status": gate["scorecard"].get("status"),
        "message": f"{message} {STEP3B_REPORT_MESSAGE}",
        "pdf_quality_status": source_quality,
        "openai_model": model_name,
        "validation_status": validator_report.get("validation_status"),
        "validator_warning_count": validator_report.get("warning_count", 0),
        "compliance_downgrade_count": gate_report.get("downgrade_count", 0),
        "contract_generated": True,
        "contract_schema_version": contract.get("schema_version"),
        "report_status": customer_report.get("report_status"),
        "customer_report_schema_version": customer_report.get("schema_version"),
    }
    if extra:
        payload_extra.update(extra)
    payload = job_status.make_status(
        job_id=job_id,
        analysis_id=analysis_id,
        status=JobStatus.REPORT_READY,
        current_stage=current_stage or _step3_stage("report_ready"),
        admin_only=admin_only,
        customer_report_generated=True,
        safe_to_show_customer=True,
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

    # The whole-document map (lot detection + segmentation + compliance scopes)
    # keeps the selected-lot pass oriented without exposing other lots' content.
    document_map = lot_packets_mod.build_document_map(
        lot_report, segmentation, lot_index, str(norm_lot)
    )
    try:
        result = analyst_mod.run_analyst(
            selected_pages,
            openai_caller=openai_caller,
            model=model,
            target_lot=str(norm_lot),
            document_map=document_map,
        )
    except AnalystError as exc:
        return _finish_analyst_failed(
            job_id, analysis_id, exc, artifacts_saved, created_at, admin_only
        )

    artifacts_saved["selected_lot_worksheet"] = artifacts.save_lot_subartifact(
        job_id, norm_lot, artifacts.ANALYST_WORKSHEET_FILE, result.worksheet
    )

    sub_lot_report = lots_mod.build_lot_report(result.worksheet, selected_pages)
    # This lot's clearly tagged money rows projected from the excluded shared
    # summary pages (deterministic; other lots' rows never enter this contract).
    shared_rows = (context.get("lot_money") or {}).get("shared_summary_rows") or []
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
        current_stage=_step3_stage("selected_lot_report_ready"),
        message=STEP3_SELECTED_LOT_MESSAGE,
        shared_summary_rows=shared_rows,
    )


# ---------------------------------------------------------------------------
# Multi-lot: analyze_all -> a separate contract per lot (never blended)
# ---------------------------------------------------------------------------
def _lot_concurrency() -> int:
    """Max number of per-lot analyst OpenAI calls run concurrently in analyze_all.

    Read from CORRECTNESS_V2_LOT_CONCURRENCY (default 1 = serial, clamped to
    >= 1). Serial is the default because that is the behavior validated end-to-end
    by the real-perizia stability smoke; parallel per-lot analyst calls remain
    available by setting the env var > 1 once that path is separately validated.
    This only bounds the network-call scheduling; every deterministic step stays
    sequential and ordered.
    """
    try:
        value = int(os.environ.get("CORRECTNESS_V2_LOT_CONCURRENCY", "1"))
    except (TypeError, ValueError):
        value = 1
    return max(1, value)


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
    lot_ids = list(lot_report.get("lot_ids", []))

    # Precompute each lot's isolated inputs (pure, deterministic, no I/O) so the
    # independent per-lot analyst OpenAI calls can be issued concurrently below.
    prepared: Dict[str, Dict[str, Any]] = {}
    for lot_id in lot_ids:
        norm_lot = lots_mod.normalize_lot_token(lot_id) or lot_id
        selected_pages = lot_packets_mod.select_lot_pages(pages, segmentation, norm_lot)
        context = lot_packets_mod.build_selected_lot_context(
            pages, segmentation, norm_lot, lot_index, worksheet=worksheet
        )
        document_map = (
            lot_packets_mod.build_document_map(
                lot_report, segmentation, lot_index, str(norm_lot)
            )
            if selected_pages
            else None
        )
        prepared[str(lot_id)] = {
            "norm_lot": norm_lot,
            "selected_pages": selected_pages,
            "context": context,
            "document_map": document_map,
        }

    # Concurrency is confined to the network calls: run_analyst builds its own
    # OpenAI client per call and shares no mutable state, so the per-lot calls
    # (the dominant wall-clock cost) can overlap. Workers only return a result
    # or an exception into this dict; every artifact write, validation and
    # per_lot_results.append happens in the ordered sequential loop below, so
    # outputs are byte-for-byte identical to the serial schedule.
    lots_to_analyze = [
        lot_id for lot_id in lot_ids if prepared[str(lot_id)]["selected_pages"]
    ]
    analyst_outcomes: Dict[str, Any] = {}
    if lots_to_analyze:

        def _call_analyst(key: str) -> Any:
            prep = prepared[key]
            return analyst_mod.run_analyst(
                prep["selected_pages"],
                openai_caller=openai_caller,
                model=model,
                target_lot=str(prep["norm_lot"]),
                document_map=prep["document_map"],
            )

        max_workers = min(len(lots_to_analyze), _lot_concurrency())
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                str(lot_id): pool.submit(_call_analyst, str(lot_id))
                for lot_id in lots_to_analyze
            }
            for key, future in futures.items():
                try:
                    analyst_outcomes[key] = future.result()
                except Exception as exc:  # noqa: BLE001 — handled per lot in the loop
                    analyst_outcomes[key] = exc

    per_lot_results: List[Dict[str, Any]] = []
    for lot_id in lot_ids:
        prep = prepared[str(lot_id)]
        norm_lot = prep["norm_lot"]
        selected_pages = prep["selected_pages"]
        context = prep["context"]
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
            result = analyst_outcomes[str(lot_id)]
            if isinstance(result, BaseException):
                raise result
        except AnalystError as exc:
            entry.update({"status": JobStatus.FAILED_ANALYSIS, "reason": str(exc)})
            per_lot_results.append(entry)
            continue

        artifacts.save_lot_subartifact(
            job_id, norm_lot, artifacts.ANALYST_WORKSHEET_FILE, result.worksheet
        )
        lot_worksheet, gate_report = validator_mod.apply_compliance_evidence_gate(
            result.worksheet, selected_pages
        )
        artifacts.save_lot_subartifact(
            job_id, norm_lot, artifacts.COMPLIANCE_GATE_FILE, gate_report
        )
        validator_report = validator_mod.validate_worksheet(lot_worksheet, selected_pages)
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

        sub_lot_report = lots_mod.build_lot_report(lot_worksheet, selected_pages)
        try:
            contract = contract_mod.build_contract(
                worksheet=lot_worksheet,
                validator_report=validator_report,
                analysis_id=analysis_id,
                job_id=job_id,
                source_pdf_quality_status=source_quality,
                lot_report=sub_lot_report,
                shared_summary_rows=(
                    (context.get("lot_money") or {}).get("shared_summary_rows") or []
                ),
                surface_cadastral=doc_signals_mod.extract_surface_cadastral(selected_pages),
            )
        except Exception as exc:  # noqa: BLE001 - recorded per lot, never blended
            entry.update({"status": JobStatus.FAILED_CONTRACT_BUILD, "reason": str(exc)})
            per_lot_results.append(entry)
            continue

        path = artifacts.save_lot_subartifact(
            job_id, norm_lot, artifacts.VERIFIED_CONTRACT_FILE, contract
        )

        # Step 3B: per-lot customer report, rendered from that lot's contract only.
        try:
            lot_customer_report = customer_report_mod.render_success_report(
                contract, selected_pages
            )
        except Exception as exc:  # noqa: BLE001 - recorded per lot, fail closed
            entry.update(
                {
                    "status": JobStatus.FAILED_CONTRACT_BUILD,
                    "reason": f"customer_report render failed: {exc}",
                    "contract_path": path,
                }
            )
            per_lot_results.append(entry)
            continue
        # Per-lot quality gate: a lot's report is only READY if its own coverage
        # audit passes. Gate crash fails closed for that lot.
        try:
            lot_gate = quality_gate_mod.run_quality_gate(
                job_id=job_id,
                analysis_id=analysis_id,
                pages=selected_pages,
                worksheet=lot_worksheet,
                contract=contract,
                customer_report=lot_customer_report,
                validator_report=validator_report,
                lot_report=sub_lot_report,
                persist=False,
            )
        except Exception as exc:  # noqa: BLE001 — per-lot fail closed
            entry.update({"status": JobStatus.NEEDS_MANUAL_REVIEW, "reason": f"quality gate error: {exc}"})
            per_lot_results.append(entry)
            continue
        artifacts.save_lot_subartifact(
            job_id, norm_lot, artifacts.COVERAGE_AUDIT_FILE, lot_gate["coverage_audit"]
        )
        artifacts.save_lot_subartifact(
            job_id, norm_lot, artifacts.PAGE_AUDIT_FILE, lot_gate["page_audit"]
        )
        artifacts.save_lot_subartifact(
            job_id, norm_lot, artifacts.QUALITY_REPORT_FILE, lot_gate["quality_report"]
        )
        artifacts.save_lot_subartifact(
            job_id, norm_lot, artifacts.SCORECARD_FILE, lot_gate["scorecard"]
        )
        report_path = artifacts.save_lot_subartifact(
            job_id, norm_lot, artifacts.CUSTOMER_REPORT_FILE, lot_gate["customer_report"]
        )
        if lot_gate["gate_status"] == quality_gate_mod.GATE_FAIL:
            entry.update(
                {
                    "status": JobStatus.NEEDS_MANUAL_REVIEW,
                    "reason": "quality_gate_failed",
                    "blocking_codes": sorted(
                        {
                            b.get("code")
                            for b in lot_gate["quality_report"].get("blocking_issues", [])
                            if b.get("code")
                        }
                    ),
                    "contract_path": path,
                    "customer_report_path": report_path,
                }
            )
            per_lot_results.append(entry)
            continue
        entry.update(
            {
                "status": JobStatus.REPORT_READY,
                "quality_gate_status": lot_gate["gate_status"],
                "contract_path": path,
                "customer_report_path": report_path,
            }
        )
        per_lot_results.append(entry)

    all_ok = bool(per_lot_results) and all(
        e.get("status") == JobStatus.REPORT_READY for e in per_lot_results
    )
    aggregate = {
        "analyze_all": True,
        "lot_count": lot_report.get("lot_count"),
        "lot_ids": lot_report.get("lot_ids", []),
        "all_lots_ready": all_ok,
        "per_lot_results": per_lot_results,
    }
    artifacts_saved["analyze_all_result"] = artifacts.save_analyze_all_result(job_id, aggregate)

    status = JobStatus.REPORT_READY if all_ok else JobStatus.NEEDS_MANUAL_REVIEW
    extra = {
        "message": (
            "analyze_all: un contratto verificato e un customer report per ciascun "
            "lotto (nessuna fusione)."
        ),
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
        reason_human = (
            "Alcuni lotti non hanno prodotto un contratto sicuro in modalità analyze_all."
        )
        _save_safe_customer_report(
            job_id,
            analysis_id,
            artifacts_saved,
            report_status=JobStatus.NEEDS_MANUAL_REVIEW,
            job_status_value=JobStatus.NEEDS_MANUAL_REVIEW,
            reason_code="ANALYZE_ALL_PARTIAL",
            reason_human=reason_human,
        )
        payload = job_status.make_status(
            customer_report_generated=False,
            safe_to_show_customer=False,
            reason_code="ANALYZE_ALL_PARTIAL",
            reason_human=reason_human,
            **common,
        )
    else:
        payload = job_status.make_status(
            customer_report_generated=True,
            safe_to_show_customer=True,
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
    *,
    pages: Optional[List[Dict[str, Any]]] = None,
    worksheet: Optional[Dict[str, Any]] = None,
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

    # Step 3B: customer-facing lot-selection report (a selector, never a blended
    # report). Deterministic render from the selection payload + lot index only.
    selection_report = customer_report_mod.render_lot_selection_report(
        selection_payload, lot_index
    )
    artifacts_saved["customer_report"] = artifacts.save_customer_report(
        job_id, selection_report
    )

    # Quality gate on the selector too: no lot may be lost, per-lot money must be
    # preserved. The status stays LOT_SELECTION_REQUIRED (expected behavior), but
    # a FAIL is surfaced and the selector is not marked safe for customers.
    gate_extra: Dict[str, Any] = {}
    selector_safe = True
    try:
        gate = quality_gate_mod.run_quality_gate(
            job_id=job_id,
            analysis_id=analysis_id,
            pages=pages or [],
            worksheet=worksheet,
            contract=None,
            customer_report=selection_report,
            lot_report=lot_report,
            lot_index=lot_index,
            artifacts_saved=artifacts_saved,
        )
        gate_extra = {
            "quality_gate_status": gate["gate_status"],
            "coverage_status": gate["coverage_audit"].get("coverage_status"),
            "quality_status": gate["quality_report"].get("overall_quality_status"),
        }
        selector_safe = gate["gate_status"] != quality_gate_mod.GATE_FAIL
    except Exception:  # noqa: BLE001 — fail closed: uncertified selector
        gate_extra = {"quality_gate_status": "ERROR"}
        selector_safe = False

    payload = job_status.make_status(
        job_id=job_id,
        analysis_id=analysis_id,
        status=JobStatus.LOT_SELECTION_REQUIRED,
        current_stage=_step3_stage("lot_selection_required"),
        admin_only=admin_only,
        customer_report_generated=True,
        safe_to_show_customer=selector_safe,
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
            "report_status": JobStatus.LOT_SELECTION_REQUIRED,
            "customer_report_schema_version": selection_report.get("schema_version"),
            **gate_extra,
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
    reason_human = (
        f"Il lotto selezionato '{selected_lot_id}' non esiste nel documento. "
        f"Lotti disponibili: {', '.join(str(x) for x in lot_ids)}."
    )
    _save_safe_customer_report(
        job_id,
        analysis_id,
        artifacts_saved,
        report_status=JobStatus.CONTRACT_VALIDATION_FAILED,
        job_status_value=JobStatus.CONTRACT_VALIDATION_FAILED,
        reason_code="SELECTED_LOT_NOT_FOUND",
        reason_human=reason_human,
    )
    payload = job_status.make_failure_status(
        job_id=job_id,
        analysis_id=analysis_id,
        status=JobStatus.CONTRACT_VALIDATION_FAILED,
        current_stage=_step3_stage("selected_lot_not_found"),
        reason_code="SELECTED_LOT_NOT_FOUND",
        reason_human=reason_human,
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
    _save_safe_customer_report(
        job_id,
        analysis_id,
        artifacts_saved,
        report_status=JobStatus.NEEDS_MANUAL_REVIEW,
        job_status_value=JobStatus.NEEDS_MANUAL_REVIEW,
        reason_code="LOT_SEGMENTATION_AMBIGUOUS",
        reason_human=detail,
    )
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


def _save_safe_customer_report(
    job_id: str,
    analysis_id: str,
    artifacts_saved: Dict[str, Any],
    *,
    report_status: str,
    job_status_value: str,
    reason_code: str,
    reason_human: str,
    next_steps: Optional[List[str]] = None,
    violation_codes: Optional[List[str]] = None,
) -> None:
    """Best-effort: persist the fail-closed customer report (uncertainty only).

    Never raises — a failure path must never be masked by its own safe-report
    rendering. The job status keeps customer_report_generated=False: this artifact
    is a safe placeholder, not a verified report.
    """
    try:
        report = customer_report_mod.render_safe_report(
            analysis_id=analysis_id,
            job_id=job_id,
            report_status=report_status,
            job_status_value=job_status_value,
            reason_code=reason_code,
            reason_human=reason_human,
            next_steps=next_steps,
            violation_codes=violation_codes,
        )
        artifacts_saved["customer_report"] = artifacts.save_customer_report(job_id, report)
    except Exception:
        pass


def _finish_quality_gate_failed(
    job_id: str,
    analysis_id: str,
    gate: Dict[str, Any],
    artifacts_saved: Dict[str, Any],
    created_at: str,
    admin_only: bool,
) -> Dict[str, Any]:
    """Coverage/quality gate found critical omissions -> NEEDS_MANUAL_REVIEW.

    The rendered report and all quality artifacts stay on disk for admin
    inspection, but the job never claims a clean REPORT_READY: a report with
    critical silent omissions is worse than no report.
    """
    quality = gate.get("quality_report") or {}
    audit = gate.get("coverage_audit") or {}
    blocking = quality.get("blocking_issues") or []
    codes = sorted({b.get("code") for b in blocking if b.get("code")})
    reason_human = (
        "Il controllo qualità ha rilevato omissioni o violazioni critiche: il "
        "report non può essere esposto come completo."
    )
    error_path = artifacts.save_error(
        job_id,
        {
            "status": JobStatus.NEEDS_MANUAL_REVIEW,
            "stage": "step4:quality_gate",
            "reason_code": "REPORT_QUALITY_GATE_FAILED",
            "blocking_codes": codes,
            "blocking_issues": blocking,
            "coverage_status": audit.get("coverage_status"),
            "no_clean_report": True,
        },
    )
    artifacts_saved["error"] = error_path
    payload = job_status.make_status(
        job_id=job_id,
        analysis_id=analysis_id,
        status=JobStatus.NEEDS_MANUAL_REVIEW,
        current_stage=_step4_stage("quality_gate_failed"),
        admin_only=admin_only,
        customer_report_generated=True,
        safe_to_show_customer=False,
        reason_code="REPORT_QUALITY_GATE_FAILED",
        reason_human=reason_human,
        troubleshoot_message=(
            "Il gate di copertura ha bloccato il report. Codici bloccanti: "
            f"{codes}. Controllare coverage_audit.json e "
            "quality_standard_report.json per il dettaglio fatto-per-fatto."
        ),
        next_steps=[
            "Aprire quality_standard_report.json per le violazioni bloccanti.",
            "Aprire coverage_audit.json per le omissioni fatto-per-fatto.",
            "Correggere l'estrazione/contratto e rieseguire il job.",
        ],
        artifacts_saved=artifacts_saved,
        created_at=created_at,
        extra={
            "quality_gate_status": quality_gate_mod.GATE_FAIL,
            "coverage_status": audit.get("coverage_status"),
            "quality_status": quality.get("overall_quality_status"),
            "customer_readiness": quality.get("customer_readiness"),
            "satisfaction_score": (gate.get("scorecard") or {}).get("overall_score"),
            "blocking_codes": codes,
            "report_status": JobStatus.NEEDS_MANUAL_REVIEW,
            "contract_generated": True,
        },
    )
    artifacts.save_job_status(job_id, payload)
    return payload


def _finish_money_confirmation_required(
    job_id: str,
    analysis_id: str,
    gate: Dict[str, Any],
    mc_payload: Dict[str, Any],
    customer_report: Dict[str, Any],
    artifacts_saved: Dict[str, Any],
    created_at: str,
    admin_only: bool,
) -> Dict[str, Any]:
    """Resolvable money ambiguities -> pause for customer confirmation.

    Mirrors _finish_lot_selection_required: a controlled, customer-safe pause
    (NOT a failure). The closest-guess report is kept and the money-confirmation
    prompt overlaid; the customer's answer is later applied as ground truth
    (resolve_money_confirmation) to produce the final clean report.
    """
    ambiguities = mc_payload.get("ambiguities") or []
    artifacts_saved["money_confirmation_required"] = (
        artifacts.save_money_confirmation_required(job_id, mc_payload)
    )
    overlay = customer_report_mod.render_money_confirmation_report(
        customer_report, mc_payload
    )
    artifacts_saved["customer_report"] = artifacts.save_customer_report(job_id, overlay)

    payload = job_status.make_status(
        job_id=job_id,
        analysis_id=analysis_id,
        status=JobStatus.MONEY_CONFIRMATION_REQUIRED,
        current_stage=_step4_stage("money_confirmation_required"),
        admin_only=admin_only,
        customer_report_generated=True,
        safe_to_show_customer=True,
        reason_code="MONEY_CONFIRMATION_REQUIRED",
        reason_human=(
            f"Il report è pronto ma {len(ambiguities)} importo/i richiede/richiedono "
            "una conferma del cliente sull'interpretazione corretta (il documento "
            "supporta più letture). Nessun dato viene inventato."
        ),
        artifacts_saved=artifacts_saved,
        created_at=created_at,
        extra={
            "money_confirmation_required": True,
            "ambiguity_count": len(ambiguities),
            "ambiguity_ids": [a.get("ambiguity_id") for a in ambiguities],
            "report_status": JobStatus.MONEY_CONFIRMATION_REQUIRED,
            "customer_report_schema_version": overlay.get("schema_version"),
            "contract_generated": True,
        },
    )
    artifacts.save_job_status(job_id, payload)
    return payload


def resolve_money_confirmation(
    job_id: str,
    answers: Dict[str, Any],
    *,
    admin_only: bool = False,
) -> Dict[str, Any]:
    """Apply the customer's money-confirmation answers and finalize the report.

    Deterministic (NO OpenAI, NO PDF): re-renders the report from the already
    verified contract, re-runs the quality gate with the confirmed roles as
    ground truth, and finishes REPORT_READY when the gate now passes. If the
    answers do not clear the block the job stays NEEDS_MANUAL_REVIEW
    (fail-closed). The contract/analysis is never re-run.
    """
    status = artifacts.read_job_status(job_id)
    if not isinstance(status, dict):
        raise ValueError("Job non trovato.")
    if str(status.get("status")) != JobStatus.MONEY_CONFIRMATION_REQUIRED:
        raise ValueError("Il job non è in attesa di conferma importi.")

    analysis_id = str(status.get("analysis_id"))
    created_at = str(status.get("created_at") or "")
    mc_payload = artifacts.read_json(job_id, artifacts.MONEY_CONFIRMATION_REQUIRED_FILE)
    if not isinstance(mc_payload, dict):
        raise ValueError("Richiesta di conferma non disponibile.")

    # Strict validation: only offered options are ever accepted.
    confirmations = money_confirmation_mod.validate_answers(mc_payload, answers)

    contract = artifacts.read_json(job_id, artifacts.VERIFIED_CONTRACT_FILE)
    pages_payload = artifacts.read_json(job_id, artifacts.INPUT_PAGES_FILE) or {}
    pages = pages_payload.get("pages") or []
    worksheet = artifacts.read_json(job_id, artifacts.ANALYST_WORKSHEET_FILE)
    validator_report = artifacts.read_json(job_id, artifacts.VALIDATOR_REPORT_FILE)
    lot_report = artifacts.read_json(job_id, artifacts.LOT_REPORT_FILE)
    if not isinstance(contract, dict):
        raise ValueError("Contratto verificato non disponibile.")

    # Deterministic re-render of the closest-guess report from the SAME contract.
    customer_report = customer_report_mod.render_success_report(contract, pages)
    artifacts_saved = dict(status.get("artifacts_saved") or {})

    gate = quality_gate_mod.run_quality_gate(
        job_id=job_id,
        analysis_id=analysis_id,
        pages=pages,
        worksheet=worksheet,
        contract=contract,
        customer_report=customer_report,
        validator_report=validator_report,
        lot_report=lot_report,
        artifacts_saved=artifacts_saved,
        money_confirmations=confirmations,
    )

    if gate["gate_status"] == quality_gate_mod.GATE_FAIL:
        # The answers did not clear the block: still not customer-safe.
        return _finish_quality_gate_failed(
            job_id, analysis_id, gate, artifacts_saved, created_at, admin_only
        )

    quality = gate.get("quality_report") or {}
    audit = gate.get("coverage_audit") or {}
    payload = job_status.make_status(
        job_id=job_id,
        analysis_id=analysis_id,
        status=JobStatus.REPORT_READY,
        current_stage=_step4_stage("report_ready"),
        admin_only=admin_only,
        customer_report_generated=True,
        safe_to_show_customer=True,
        artifacts_saved=artifacts_saved,
        created_at=created_at,
        extra={
            "quality_gate_status": gate["gate_status"],
            "coverage_status": audit.get("coverage_status"),
            "quality_status": quality.get("overall_quality_status"),
            "customer_readiness": quality.get("customer_readiness"),
            "report_status": JobStatus.REPORT_READY,
            "contract_generated": True,
            # Traceable record of the human-in-the-loop resolution.
            "money_confirmation_resolved": True,
            "money_confirmations": confirmations,
        },
    )
    artifacts.save_job_status(job_id, payload)
    return payload


def _finish_quality_gate_error(
    job_id: str,
    analysis_id: str,
    exc: Exception,
    artifacts_saved: Dict[str, Any],
    created_at: str,
    admin_only: bool,
) -> Dict[str, Any]:
    """The quality gate itself crashed -> fail closed (no unaudited clean report)."""
    detail = f"{type(exc).__name__}: {exc}"
    error_path = artifacts.save_error(
        job_id,
        {
            "status": JobStatus.NEEDS_MANUAL_REVIEW,
            "stage": "step4:quality_gate",
            "reason_code": "QUALITY_GATE_ERROR",
            "detail": detail,
            "traceback": traceback.format_exc(),
            "no_clean_report": True,
        },
    )
    artifacts_saved["error"] = error_path
    reason_human = (
        "Il controllo qualità non è stato completato: il report non può essere "
        "certificato come completo."
    )
    payload = job_status.make_status(
        job_id=job_id,
        analysis_id=analysis_id,
        status=JobStatus.NEEDS_MANUAL_REVIEW,
        current_stage=_step4_stage("quality_gate_error"),
        admin_only=admin_only,
        customer_report_generated=True,
        safe_to_show_customer=False,
        reason_code="QUALITY_GATE_ERROR",
        reason_human=reason_human,
        troubleshoot_message=(
            f"Il gate qualità ha generato un errore: {detail}. Il report renderizzato "
            "resta disponibile per gli admin ma non è certificato (fail-closed)."
        ),
        next_steps=[
            "Controllare error.json per il traceback del gate qualità.",
            "Rieseguire il job dopo la correzione.",
        ],
        artifacts_saved=artifacts_saved,
        created_at=created_at,
        extra={"quality_gate_status": "ERROR", "contract_generated": True},
    )
    artifacts.save_job_status(job_id, payload)
    return payload


def _finish_report_render_failed(
    job_id: str,
    analysis_id: str,
    exc: Exception,
    artifacts_saved: Dict[str, Any],
    created_at: str,
    admin_only: bool,
) -> Dict[str, Any]:
    """Customer-report render crashed -> FAILED_CONTRACT_BUILD. Fail closed.

    The verified contract exists, but a report that cannot be rendered
    deterministically is never half-shown to a customer.
    """
    detail = f"{type(exc).__name__}: {exc}"
    error_path = artifacts.save_error(
        job_id,
        {
            "status": JobStatus.FAILED_CONTRACT_BUILD,
            "stage": "step3b:customer_report",
            "reason_code": "CUSTOMER_REPORT_RENDER_ERROR",
            "detail": detail,
            "traceback": traceback.format_exc(),
            "no_report": True,
        },
    )
    artifacts_saved["error"] = error_path
    reason_human = "La generazione del report cliente dal contratto verificato è fallita."
    _save_safe_customer_report(
        job_id,
        analysis_id,
        artifacts_saved,
        report_status=JobStatus.NEEDS_MANUAL_REVIEW,
        job_status_value=JobStatus.FAILED_CONTRACT_BUILD,
        reason_code="CUSTOMER_REPORT_RENDER_ERROR",
        reason_human=reason_human,
    )
    payload = job_status.make_failure_status(
        job_id=job_id,
        analysis_id=analysis_id,
        status=JobStatus.FAILED_CONTRACT_BUILD,
        current_stage=_step3_stage("customer_report_failed"),
        reason_code="CUSTOMER_REPORT_RENDER_ERROR",
        reason_human=reason_human,
        troubleshoot_message=(
            "Il contratto verificato è stato prodotto ma il renderer deterministico "
            f"del report cliente ha generato un errore: {detail}. Nessun report è "
            "stato mostrato al cliente (fail-closed)."
        ),
        next_steps=[
            "Controllare error.json per il traceback.",
            "Verificare la forma di verified_report_contract.json.",
        ],
        artifacts_saved=artifacts_saved,
        created_at=created_at,
        admin_only=admin_only,
        extra={"no_report": True},
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
    reason_human = "La generazione del foglio di lavoro analista (OpenAI) è fallita."
    _save_safe_customer_report(
        job_id,
        analysis_id,
        artifacts_saved,
        report_status=JobStatus.NEEDS_MANUAL_REVIEW,
        job_status_value=JobStatus.FAILED_ANALYSIS,
        reason_code=reason_code,
        reason_human=reason_human,
    )
    payload = job_status.make_failure_status(
        job_id=job_id,
        analysis_id=analysis_id,
        status=JobStatus.FAILED_ANALYSIS,
        current_stage=_step2_stage("analyst_failed"),
        reason_code=reason_code,
        reason_human=reason_human,
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
    reason_human = "La validazione deterministica ha rifiutato il foglio di lavoro."
    _save_safe_customer_report(
        job_id,
        analysis_id,
        artifacts_saved,
        report_status=JobStatus.CONTRACT_VALIDATION_FAILED,
        job_status_value=JobStatus.CONTRACT_VALIDATION_FAILED,
        reason_code="CONTRACT_VALIDATION_FAILED",
        reason_human=reason_human,
        violation_codes=codes,
    )
    payload = job_status.make_failure_status(
        job_id=job_id,
        analysis_id=analysis_id,
        status=JobStatus.CONTRACT_VALIDATION_FAILED,
        current_stage=_step2_stage("validation_failed"),
        reason_code="CONTRACT_VALIDATION_FAILED",
        reason_human=reason_human,
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
    reason_human = "La costruzione deterministica del contratto è fallita."
    _save_safe_customer_report(
        job_id,
        analysis_id,
        artifacts_saved,
        report_status=JobStatus.NEEDS_MANUAL_REVIEW,
        job_status_value=JobStatus.FAILED_CONTRACT_BUILD,
        reason_code="CONTRACT_BUILD_ERROR",
        reason_human=reason_human,
    )
    payload = job_status.make_failure_status(
        job_id=job_id,
        analysis_id=analysis_id,
        status=JobStatus.FAILED_CONTRACT_BUILD,
        current_stage=_step2_stage("contract_build_failed"),
        reason_code="CONTRACT_BUILD_ERROR",
        reason_human=reason_human,
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

    # For "document unreadable" blocks, also render a customer-safe message so the
    # customer is told the perizia is images/not extractable and to upload a
    # readable PDF — instead of silently getting no report at all.
    if reason_code in _CUSTOMER_NOT_READABLE_BLOCKS:
        not_readable = customer_report_mod.render_not_readable_report(
            analysis_id=analysis_id,
            job_id=job_id,
            reason_code=reason_code,
            reason_human=reason_human,
            troubleshoot_message=troubleshoot,
            next_steps=next_steps,
        )
        payload["artifacts_saved"]["customer_report"] = artifacts.save_customer_report(
            job_id, not_readable
        )
        payload["customer_report_generated"] = True
        payload["safe_to_show_customer"] = True
        payload["report_status"] = not_readable.get("report_status")

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

    _save_safe_customer_report(
        job_id,
        analysis_id,
        artifacts_saved,
        report_status=JobStatus.NEEDS_MANUAL_REVIEW,
        job_status_value=JobStatus.FAILED_ANALYSIS,
        reason_code="CORRECTNESS_V2_UNEXPECTED_ERROR",
        reason_human="Si è verificato un errore inatteso durante la Correctness Mode.",
    )
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
