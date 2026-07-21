"""
Admin-only API endpoints for Correctness Mode v2 (step 1).

Routes (registered under the existing /api prefix by server.py):

    POST /api/analysis/perizia/{analysis_id}/correctness-v2/start
    GET  /api/analysis/perizia/{analysis_id}/correctness-v2/jobs/{job_id}
    GET  /api/analysis/perizia/{analysis_id}/correctness-v2/latest

Access rules:
  * Feature disabled            -> 404 {"detail": {"reason_code": "CORRECTNESS_V2_DISABLED"}}
  * Admin-only & caller not admin -> 403 {"detail": {"reason_code": "ADMIN_ONLY_FEATURE"}}

Server symbols (auth, db, page loader) are imported lazily inside handlers to
avoid a circular import with the very large server.py module, and to keep this
subsystem isolated. This file never references the old analyzer.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request

from . import (
    artifacts,
    customer_view,
    decision_model,
    feature_flags,
    job_status,
    openai_client,
    user_confirmations,
    workspace,
)
from .orchestrator import resolve_money_confirmation, start_job
from .schemas import JobStatus

router = APIRouter(prefix="/analysis/perizia", tags=["correctness_v2"])

logger = logging.getLogger(__name__)


def _emit_lot_signal(
    event_type: str,
    analysis_id: str,
    lot_id: Optional[str],
    user_id: Optional[str],
    *,
    job_id: Optional[str] = None,
) -> None:
    """Best-effort operational telemetry for a lot decision. Never raises.

    Telemetry only: does not affect reuse/dedup/spawn behaviour. Idempotent on
    (job_id-or-analysis, lot_id, event_type) so repeated polls don't inflate a
    metric.
    """
    try:
        from beta_program import signals as _sig

        _sig.emit_v2_job_event(
            event_type,
            job_id=job_id or f"lot_{analysis_id}",
            analysis_id=analysis_id,
            user_id=user_id,
            lot_id=lot_id,
        )
    except Exception:  # telemetry must never affect the pipeline
        pass

# Statuses of a job that is still working towards a report. Everything else is
# terminal (a report, a controlled stop, or a diagnosed failure). Kept in sync
# with workspace._IN_PROGRESS_STATUSES and the orchestrator's _STALEABLE_STATUSES:
# a persisted PDF_QUALITY_OK/WARNING is always a TERMINAL step-1-only artifact
# (the full customer pipeline never persists it), so it is NOT in progress.
_IN_PROGRESS_STATUSES = frozenset(
    {
        JobStatus.QUEUED,
        JobStatus.RUNNING,
    }
)

# Terminal step-1-only statuses (quality gate ran, analysis never did).
_QUALITY_ONLY_STATUSES = frozenset(
    {
        JobStatus.PDF_QUALITY_OK,
        JobStatus.PDF_QUALITY_WARNING,
    }
)


def _is_in_progress_status(status: Any) -> bool:
    return str(status or "") in _IN_PROGRESS_STATUSES


def _has_in_progress_job(analysis_id: str) -> bool:
    latest = artifacts.latest_job_for_analysis(analysis_id)
    return bool(latest) and _is_in_progress_status(latest.get("status"))


# ---------------------------------------------------------------------------
# Customer-safe reason codes (closed public enum).
#
# These are the ONLY reason_code values a customer may ever receive from the
# customer-view endpoint when no report is available. Internal job statuses,
# OpenAI error names, validator codes, stack traces and artifact paths must
# never pass through: everything is whitelist-mapped, and anything unrecognised
# degrades to SERVICE_UNAVAILABLE (never the raw value). The exact-admin
# diagnostic path (Vista admin) is unaffected and keeps full internal detail.
# ---------------------------------------------------------------------------
PUBLIC_REASON_PREPARING = "PREPARING"
PUBLIC_REASON_SERVICE_BUSY = "SERVICE_BUSY"
PUBLIC_REASON_VERIFICATION_REQUIRED = "VERIFICATION_REQUIRED"
PUBLIC_REASON_SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"
PUBLIC_REASON_NO_REPORT = "NO_REPORT"

PUBLIC_REASON_CODES = frozenset(
    {
        PUBLIC_REASON_PREPARING,
        PUBLIC_REASON_SERVICE_BUSY,
        PUBLIC_REASON_VERIFICATION_REQUIRED,
        PUBLIC_REASON_SERVICE_UNAVAILABLE,
        PUBLIC_REASON_NO_REPORT,
    }
)

# Terminal job statuses meaning the pipeline failed closed on a correctness
# concern: a human must verify before a customer report can be produced.
_VERIFICATION_REQUIRED_STATUSES = frozenset(
    {
        JobStatus.CONTRACT_VALIDATION_FAILED,
        JobStatus.NEEDS_MANUAL_REVIEW,
        JobStatus.FAILED_GROUNDING,
    }
)


def _public_unavailable_reason(analysis_id: str, preparing: bool) -> str:
    """Map the latest job's internal state to the closed public enum.

    Whitelist-map only: no raw job status or internal reason_code is ever
    returned. The result is always a member of ``PUBLIC_REASON_CODES``.
    """
    if preparing:
        return PUBLIC_REASON_PREPARING
    latest = artifacts.latest_job_for_analysis(analysis_id)
    if not isinstance(latest, dict) or not latest:
        # Historical analysis: no V2 job and no active preparation.
        return PUBLIC_REASON_NO_REPORT
    status = str(latest.get("status") or "")
    internal_reason = latest.get("reason_code")
    if _is_in_progress_status(status):
        return PUBLIC_REASON_PREPARING
    if status in _QUALITY_ONLY_STATUSES:
        # Terminal step-1-only artifact: the quality gate ran but no analysis
        # ever did — there is no report and none is being prepared.
        return PUBLIC_REASON_NO_REPORT
    # Temporary capacity/quota failures: retrying later can succeed (quota
    # after a recharge; rate-limit/timeout/5xx on their own). Reuse the
    # openai_client helpers -- never re-derive the classification here.
    if openai_client.is_quota_exhausted_reason(internal_reason) or openai_client.is_transient_reason(
        internal_reason
    ):
        return PUBLIC_REASON_SERVICE_BUSY
    if status in _VERIFICATION_REQUIRED_STATUSES:
        return PUBLIC_REASON_VERIFICATION_REQUIRED
    # Any other terminal state (dependency failure, unrecoverable transient
    # failure, or anything unrecognised) degrades to the safe generic value.
    return PUBLIC_REASON_SERVICE_UNAVAILABLE


# ---------------------------------------------------------------------------
# System auto-start (product path): run a V2 job in the background for a new
# analysis, or for a customer-selected lot that has no report yet. Never blocks
# the caller, never raises, and refuses to stack jobs on a running analysis.
# ---------------------------------------------------------------------------
def autostart_job(
    analysis_id: str,
    pages_count: int,
    *,
    selected_lot_id: Optional[str] = None,
    reason: str = "upload",
) -> bool:
    """Spawn a background Correctness v2 job for ``analysis_id``.

    Returns True when a job was actually spawned (or one is already running),
    False when auto-start is disabled or spawning failed. Safe to call from any
    request handler: all failures are logged and swallowed.
    """
    if not feature_flags.auto_start_enabled():
        return False
    try:
        if _has_in_progress_job(analysis_id):
            return True

        def _run() -> None:
            try:
                import server  # type: ignore  # lazy: avoid circular import

                def _page_loader(aid: str) -> List[Dict[str, Any]]:
                    return server._load_pages_for_analysis(aid, pages_count)

                start_job(
                    analysis_id,
                    _page_loader,
                    is_admin=False,
                    openai_caller=openai_client.call_openai_json,
                    selected_lot_id=selected_lot_id,
                )
            except Exception:
                logger.exception(
                    "correctness_v2 autostart job failed analysis_id=%s", analysis_id
                )

        thread = threading.Thread(
            target=_run,
            name=f"cv2-autostart-{analysis_id}",
            daemon=True,
        )
        thread.start()
        logger.info(
            "correctness_v2 autostart spawned analysis_id=%s lot=%s reason=%s",
            analysis_id,
            selected_lot_id,
            reason,
        )
        return True
    except Exception:
        logger.exception("correctness_v2 autostart spawn failed analysis_id=%s", analysis_id)
        return False


async def _resolve_user_and_guard(request: Request):
    """
    Authenticate and enforce feature-flag + admin-only access.

    Returns (user, is_admin). Raises HTTPException with a precise reason_code:
      * 404 CORRECTNESS_V2_DISABLED
      * 403 ADMIN_ONLY_FEATURE
    """
    # Lazy import to avoid circular import at module load time.
    import server  # type: ignore

    user = await server.require_auth(request)
    is_admin = bool(server._user_is_admin(user))

    block = feature_flags.access_block_reason(is_admin)
    if block == "CORRECTNESS_V2_DISABLED":
        raise HTTPException(
            status_code=404,
            detail={
                "reason_code": "CORRECTNESS_V2_DISABLED",
                "reason_human": "Correctness Mode v2 non è abilitata.",
            },
        )
    if block == "ADMIN_ONLY_FEATURE":
        raise HTTPException(
            status_code=403,
            detail={
                "reason_code": "ADMIN_ONLY_FEATURE",
                "reason_human": "Funzionalità riservata agli amministratori.",
            },
        )
    return user, is_admin


def _build_admin_page_loader():
    """
    Return a page_loader(analysis_id) -> List[pages] for admin scope.

    Loads page-by-page text using server's existing extraction artifact loader.
    Does NOT touch the old analyzer pipeline.
    """
    import server  # type: ignore

    async def _fetch_pages_count(analysis_id: str) -> int:
        record = await server.db.perizia_analyses.find_one(
            {"analysis_id": analysis_id}, {"_id": 0, "pages_count": 1}
        )
        if not record:
            raise HTTPException(status_code=404, detail="Analysis not found")
        try:
            return int(record.get("pages_count") or 0)
        except Exception:
            return 0

    return _fetch_pages_count


def _parse_lot_selection(body: Any) -> Dict[str, Any]:
    """Read optional lot-selection inputs from the request body (back-compatible).

    Accepts {"selected_lot_id": "1"} or {"target_lot": "1"} and an optional
    {"analyze_all": true}. No body / empty body keeps the original behavior.
    """
    if not isinstance(body, dict):
        return {"selected_lot_id": None, "analyze_all": False}
    raw_lot = body.get("selected_lot_id")
    if raw_lot is None:
        raw_lot = body.get("target_lot")
    selected = None if raw_lot is None else str(raw_lot).strip() or None
    return {"selected_lot_id": selected, "analyze_all": bool(body.get("analyze_all"))}


@router.post("/{analysis_id}/correctness-v2/start")
async def correctness_v2_start(analysis_id: str, request: Request) -> Dict[str, Any]:
    import server  # type: ignore

    user, is_admin = await _resolve_user_and_guard(request)

    # Optional lot-selection body (no body still works for single-lot perizie).
    try:
        body = await request.json()
    except Exception:
        body = None
    selection = _parse_lot_selection(body)

    # Resolve pages_count (also validates the analysis exists) up front.
    fetch_pages_count = _build_admin_page_loader()
    pages_count = await fetch_pages_count(analysis_id)

    def _page_loader(aid: str) -> List[Dict[str, Any]]:
        return server._load_pages_for_analysis(aid, pages_count)

    # Inject the real OpenAI caller so the job runs the full lot-aware pipeline
    # (analyst -> lot routing -> validator -> contract). Quality-blocked jobs never
    # reach it. Multi-lot with no selection returns LOT_SELECTION_REQUIRED.
    status = start_job(
        analysis_id,
        _page_loader,
        is_admin=is_admin,
        openai_caller=openai_client.call_openai_json,
        selected_lot_id=selection["selected_lot_id"],
        analyze_all=selection["analyze_all"],
    )

    # Admin responses keep raw artifact paths; non-admin would be sanitized, but
    # this endpoint is admin-only so we return the full diagnostic payload.
    return status


@router.get("/{analysis_id}/correctness-v2/jobs/{job_id}")
async def correctness_v2_job(analysis_id: str, job_id: str, request: Request) -> Dict[str, Any]:
    await _resolve_user_and_guard(request)
    status = artifacts.read_job_status(job_id)
    if not status or str(status.get("analysis_id")) != str(analysis_id):
        raise HTTPException(status_code=404, detail="Job not found")
    return status


def _read_known_job_artifact(analysis_id: str, job_id: str, filename: str) -> Dict[str, Any]:
    status = artifacts.read_job_status(job_id)
    if not status or str(status.get("analysis_id")) != str(analysis_id):
        raise HTTPException(status_code=404, detail="Job not found")
    payload = artifacts.read_json(job_id, filename)
    if not isinstance(payload, dict):
        raise HTTPException(status_code=404, detail="Artifact not found")
    return payload


@router.get("/{analysis_id}/correctness-v2/jobs/{job_id}/customer-report")
async def correctness_v2_customer_report(
    analysis_id: str, job_id: str, request: Request
) -> Dict[str, Any]:
    await _resolve_user_and_guard(request)
    return _read_known_job_artifact(analysis_id, job_id, artifacts.CUSTOMER_REPORT_FILE)


@router.get("/{analysis_id}/correctness-v2/jobs/{job_id}/lot-selection-report")
async def correctness_v2_lot_selection_report(
    analysis_id: str, job_id: str, request: Request
) -> Dict[str, Any]:
    await _resolve_user_and_guard(request)
    return _read_known_job_artifact(analysis_id, job_id, artifacts.LOT_SELECTION_REQUIRED_FILE)


@router.get("/{analysis_id}/correctness-v2/jobs/{job_id}/money-confirmation-required")
async def correctness_v2_money_confirmation_required(
    analysis_id: str, job_id: str, request: Request
) -> Dict[str, Any]:
    await _resolve_user_and_guard(request)
    return _read_known_job_artifact(
        analysis_id, job_id, artifacts.MONEY_CONFIRMATION_REQUIRED_FILE
    )


@router.get("/{analysis_id}/correctness-v2/jobs/{job_id}/validator-report")
async def correctness_v2_validator_report(
    analysis_id: str, job_id: str, request: Request
) -> Dict[str, Any]:
    await _resolve_user_and_guard(request)
    return _read_known_job_artifact(analysis_id, job_id, artifacts.VALIDATOR_REPORT_FILE)


@router.get("/{analysis_id}/correctness-v2/jobs/{job_id}/coverage-audit")
async def correctness_v2_coverage_audit(
    analysis_id: str, job_id: str, request: Request
) -> Dict[str, Any]:
    await _resolve_user_and_guard(request)
    return _read_known_job_artifact(analysis_id, job_id, artifacts.COVERAGE_AUDIT_FILE)


@router.get("/{analysis_id}/correctness-v2/jobs/{job_id}/page-audit")
async def correctness_v2_page_audit(
    analysis_id: str, job_id: str, request: Request
) -> Dict[str, Any]:
    await _resolve_user_and_guard(request)
    return _read_known_job_artifact(analysis_id, job_id, artifacts.PAGE_AUDIT_FILE)


@router.get("/{analysis_id}/correctness-v2/jobs/{job_id}/quality-report")
async def correctness_v2_quality_report(
    analysis_id: str, job_id: str, request: Request
) -> Dict[str, Any]:
    await _resolve_user_and_guard(request)
    return _read_known_job_artifact(analysis_id, job_id, artifacts.QUALITY_REPORT_FILE)


@router.get("/{analysis_id}/correctness-v2/jobs/{job_id}/satisfaction-scorecard")
async def correctness_v2_scorecard(
    analysis_id: str, job_id: str, request: Request
) -> Dict[str, Any]:
    await _resolve_user_and_guard(request)
    return _read_known_job_artifact(analysis_id, job_id, artifacts.SCORECARD_FILE)


# ---------------------------------------------------------------------------
# Customer-safe view (NOT admin-only): sanitized customer report, never any
# admin/debug/quality/artifact data. Gated only by the feature flag + auth +
# ownership (admins may inspect any analysis; normal users only their own).
# ---------------------------------------------------------------------------
async def _resolve_customer_access(request: Request, analysis_id: str):
    """Authenticate, require the feature enabled, and enforce ownership.

    Unlike ``_resolve_user_and_guard`` this does NOT require admin: any
    authenticated owner (or an admin) may read the sanitized customer view.
    """
    import server  # type: ignore

    user = await server.require_auth(request)
    is_admin = bool(server._user_is_admin(user))

    if not feature_flags.is_enabled():
        raise HTTPException(
            status_code=404,
            detail={
                "reason_code": "CORRECTNESS_V2_DISABLED",
                "reason_human": "Correctness Mode v2 non è abilitata.",
            },
        )

    if not is_admin:
        owned = await server.db.perizia_analyses.find_one(
            {"analysis_id": analysis_id, "user_id": user.user_id}, {"_id": 0, "analysis_id": 1}
        )
        if not owned:
            raise HTTPException(status_code=404, detail="Analysis not found")
    return user, is_admin


async def _analysis_pages_count(analysis_id: str) -> int:
    """pages_count for an analysis, or 0 when unknown. Never raises."""
    try:
        import server  # type: ignore

        record = await server.db.perizia_analyses.find_one(
            {"analysis_id": analysis_id}, {"_id": 0, "pages_count": 1}
        )
        return int((record or {}).get("pages_count") or 0)
    except Exception:
        return 0


def _find_customer_job(analysis_id: str, selected_lot_id: Optional[str] = None):
    """Latest customer-safe (job_status, report, enumerated_job_id) tuple.

    When ``selected_lot_id`` is given, prefers the most recent REPORT_READY job
    whose report is for exactly that lot, so a selected lot's report never
    contaminates another lot. Returns a triple of ``None`` when nothing safe
    exists.
    """
    best: Optional[tuple] = None
    for jid in artifacts.list_jobs():
        status = artifacts.read_job_status(jid)
        if not isinstance(status, dict):
            continue
        if str(status.get("analysis_id")) != str(analysis_id):
            continue
        report = artifacts.read_json(jid, artifacts.CUSTOMER_REPORT_FILE)
        if not isinstance(report, dict):
            continue

        # The directory name selected by list_jobs() is the storage identity.
        # Embedded identities are useful consistency checks, never path inputs:
        # a stale/corrupt report must not redirect this read to another job's
        # cached pages.  Missing legacy fields remain readable, but every
        # identity that is present must agree with the enumerated directory and
        # requested analysis.
        status_job_id = str(status.get("job_id") or "").strip()
        report_job_id = str(report.get("job_id") or "").strip()
        report_analysis_id = str(report.get("analysis_id") or "").strip()
        if status_job_id and status_job_id != str(jid):
            continue
        if report_job_id and report_job_id != str(jid):
            continue
        if report_analysis_id and report_analysis_id != str(analysis_id):
            continue
        if not customer_view.is_customer_safe(report, status):
            continue
        if selected_lot_id is not None:
            lot = (report.get("lot_structure") or {})
            # A paused MONEY_CONFIRMATION_REQUIRED report is customer-safe and
            # must resume its confirmation prompt instead of being skipped
            # (skipping it made the lot look report-less and caused duplicates).
            if str(report.get("report_status")) not in (
                "REPORT_READY",
                "MONEY_CONFIRMATION_REQUIRED",
            ):
                continue
            if str(lot.get("selected_lot")) != str(selected_lot_id):
                continue
        sort_key = str(status.get("updated_at") or status.get("created_at") or "")
        if best is None or sort_key > best[0]:
            best = (sort_key, status, report, str(jid))
    if best is None:
        return None, None, None
    return best[1], best[2], best[3]


async def _confirmations_for(analysis_id: str, user) -> List[Dict[str, Any]]:
    """Owner's persisted confirmations for the analysis (never raises on read)."""
    try:
        return await user_confirmations.list_for_analysis(analysis_id, user.user_id)
    except Exception:  # pragma: no cover - a store hiccup must not break the read
        logger.exception("Failed to load user confirmations for %s", analysis_id)
        return []


def _cached_input_pages(selected_job_id: Any) -> List[Dict[str, Any]]:
    """Read cached pages from the already-enumerated job; never write/redirect."""
    job_id = str(selected_job_id or "").strip()
    if not job_id:
        return []
    payload = artifacts.read_json(job_id, artifacts.INPUT_PAGES_FILE)
    pages = (payload or {}).get("pages") if isinstance(payload, dict) else None
    return [p for p in (pages or []) if isinstance(p, dict)]


@router.get("/{analysis_id}/correctness-v2/customer-view/latest")
async def correctness_v2_customer_view(analysis_id: str, request: Request) -> Dict[str, Any]:
    user, _is_admin = await _resolve_customer_access(request, analysis_id)

    raw_lot = request.query_params.get("selected_lot_id")
    selected_lot_id = (str(raw_lot).strip() or None) if raw_lot is not None else None

    status, report, selected_job_id = _find_customer_job(analysis_id, selected_lot_id)
    if not report:
        # No safe report (yet). Tell the client whether one is being prepared,
        # so the UI can show the correct customer state (preparing / busy /
        # verification required / unavailable / no report). The reason_code is
        # ALWAYS a member of the closed public enum -- never an internal code.
        #
        # PURE READ: this GET never spawns a job. All customer job creation
        # goes through POST .../lots/{lot_id}/generate (explicit action only) --
        # a failed lot must never be silently re-run by opening or polling it.
        preparing = _has_in_progress_job(analysis_id)
        return {
            "available": False,
            "selected_lot_id": selected_lot_id,
            "preparing": bool(preparing),
            "reason_code": _public_unavailable_reason(analysis_id, bool(preparing)),
        }
    confirmations = await _confirmations_for(analysis_id, user)
    return {
        "available": True,
        "selected_lot_id": selected_lot_id,
        "preparing": False,
        "report": customer_view.sanitize_customer_report(
            report,
            status,
            confirmations,
            cached_pages=_cached_input_pages(selected_job_id),
        ),
    }


def _find_decision_finding(report: Dict[str, Any], finding_id: str) -> Optional[Dict[str, Any]]:
    """Rebuild the decision model server-side and return the eligible finding, if any."""
    model = decision_model.build_decision_model(report, [])
    for finding in model.get("findings") or []:
        if finding.get("finding_id") == finding_id:
            return finding
    return None


@router.post("/{analysis_id}/correctness-v2/customer-view/confirm-finding")
async def correctness_v2_confirm_finding(
    analysis_id: str, request: Request
) -> Dict[str, Any]:
    """Owner submits/updates a focused confirmation on a decision-model finding.

    Body: {"job_id", "finding_id", "option_id", "note"?}. The finding must exist
    in the decision model rebuilt for that job's report AND be confirmation-
    eligible; the option must be one of the offered options (or "non_sicuro").
    Persisted authoritatively in MongoDB (owner only). Zero OpenAI/jobs/credits.
    Returns the refreshed sanitized report with the confirmation joined.
    """
    user, _is_admin = await _resolve_customer_access(request, analysis_id)

    try:
        body = await request.json()
    except Exception:
        body = None
    if not isinstance(body, dict):
        raise HTTPException(
            status_code=400,
            detail={"reason_code": "INVALID_BODY", "reason_human": "Richiesta non valida."},
        )
    job_id = str(body.get("job_id") or "").strip()
    finding_id = str(body.get("finding_id") or "").strip()
    option_id = str(body.get("option_id") or "").strip()
    note = body.get("note")
    if not (job_id and finding_id and option_id):
        raise HTTPException(
            status_code=400,
            detail={"reason_code": "INVALID_BODY", "reason_human": "Dati di conferma mancanti."},
        )

    status = artifacts.read_job_status(job_id)
    if not isinstance(status, dict) or str(status.get("analysis_id")) != str(analysis_id):
        raise HTTPException(status_code=404, detail="Job not found")
    report = artifacts.read_json(job_id, artifacts.CUSTOMER_REPORT_FILE)
    if not customer_view.is_customer_safe(report, status):
        raise HTTPException(status_code=404, detail="Report not available")

    finding = _find_decision_finding(report, finding_id)
    if not finding or not (finding.get("confirmation") or {}).get("eligible"):
        raise HTTPException(
            status_code=400,
            detail={
                "reason_code": "INVALID_CONFIRMATION",
                "reason_human": "Conferma non disponibile per questo elemento.",
            },
        )

    lot_id = (report.get("lot_structure") or {}).get("selected_lot")
    try:
        await user_confirmations.submit(
            analysis_id=analysis_id,
            lot_id=lot_id,
            finding=finding,
            option_id=option_id,
            user_id=user.user_id,
            report_version=report.get("schema_version"),
            job_id=job_id,
            note=note,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"reason_code": "INVALID_CONFIRMATION", "reason_human": str(exc)},
        )

    confirmations = await _confirmations_for(analysis_id, user)
    return {
        "available": True,
        "job_id": job_id,
        "report": customer_view.sanitize_customer_report(report, status, confirmations),
    }


@router.get("/{analysis_id}/correctness-v2/customer-view/confirmations")
async def correctness_v2_confirmations(analysis_id: str, request: Request) -> Dict[str, Any]:
    """Owner view of their own confirmations (customer projection only)."""
    user, _is_admin = await _resolve_customer_access(request, analysis_id)
    confirmations = await _confirmations_for(analysis_id, user)
    return {
        "analysis_id": analysis_id,
        "confirmations": [
            {
                "finding_id": c.get("finding_id"),
                "selected_option": c.get("selected_option"),
                "selected_label": c.get("selected_label"),
                "page": c.get("page"),
                "status": c.get("status"),
                "updated_at": c.get("updated_at"),
            }
            for c in confirmations
        ],
    }


@router.get("/{analysis_id}/correctness-v2/jobs/{job_id}/decision-model")
async def correctness_v2_decision_model(
    analysis_id: str, job_id: str, request: Request
) -> Dict[str, Any]:
    """Admin-only (Vista admin): raw decision model + confirmations + audit.

    Behind the exact-email admin gate (``_resolve_user_and_guard``). Shows the
    original finding vs the user confirmation, readiness, and evidence identity.
    """
    await _resolve_user_and_guard(request)
    status = artifacts.read_job_status(job_id)
    if not isinstance(status, dict) or str(status.get("analysis_id")) != str(analysis_id):
        raise HTTPException(status_code=404, detail="Job not found")
    report = artifacts.read_json(job_id, artifacts.CUSTOMER_REPORT_FILE)
    if not isinstance(report, dict):
        raise HTTPException(status_code=404, detail="Report not found")
    confirmations = await user_confirmations.list_all_for_analysis(analysis_id)
    audit = await user_confirmations.audit_for_analysis(analysis_id)
    return {
        "analysis_id": analysis_id,
        "job_id": job_id,
        "decision_model": decision_model.build_decision_model(report, confirmations),
        "confirmations": confirmations,
        "audit": audit,
    }


@router.post("/{analysis_id}/correctness-v2/customer-view/confirm-money")
async def correctness_v2_confirm_money(
    analysis_id: str, request: Request
) -> Dict[str, Any]:
    """Customer submits money-confirmation answers; re-gate and return the report.

    Body: {"job_id": "...", "answers": {ambiguity_id: option_id, ...}}. Ownership
    is enforced (any authenticated owner or an admin). Deterministic: no OpenAI.
    """
    _cm_user, _cm_is_admin = await _resolve_customer_access(request, analysis_id)
    _cm_user_id = getattr(_cm_user, "user_id", None)

    try:
        body = await request.json()
    except Exception:
        body = None
    if not isinstance(body, dict):
        raise HTTPException(
            status_code=400,
            detail={"reason_code": "INVALID_BODY", "reason_human": "Richiesta non valida."},
        )
    job_id = str(body.get("job_id") or "").strip()
    answers = body.get("answers")
    if not job_id:
        raise HTTPException(
            status_code=400,
            detail={"reason_code": "MISSING_JOB_ID", "reason_human": "job_id mancante."},
        )

    status = artifacts.read_job_status(job_id)
    if not isinstance(status, dict) or str(status.get("analysis_id")) != str(analysis_id):
        raise HTTPException(status_code=404, detail="Job not found")

    try:
        result = resolve_money_confirmation(job_id, answers)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"reason_code": "INVALID_CONFIRMATION", "reason_human": str(exc)},
        )

    report = artifacts.read_json(job_id, artifacts.CUSTOMER_REPORT_FILE)
    safe = customer_view.is_customer_safe(report, result)
    _emit_lot_signal(
        "CONFIRMATION_COMPLETED", analysis_id, None, _cm_user_id, job_id=job_id
    )
    return {
        "available": bool(safe and report),
        "job_id": job_id,
        "report_status": result.get("status"),
        "report": customer_view.sanitize_customer_report(report, result) if safe and report else None,
    }


# ---------------------------------------------------------------------------
# Storico lot workspace (customer-facing, ownership-gated).
#
#   GET  .../workspace                      -> pure-read lot overview, 0 side effects
#   GET  .../lots/{lot_id}/generate/preview -> authoritative credit preview, read-only
#   POST .../lots/{lot_id}/generate         -> the ONLY customer job-creation path
#
# Billing rule (plan §D/§M): credits are charged ONCE per upload (page-banded,
# lot-agnostic) by the existing upload debit. Lot generation and rerun consume
# ZERO additional credits: no debit is ever issued from these endpoints and no
# per-lot price exists. The preview only READS the existing wallet/exemption
# accessors and reports that truthfully.
# ---------------------------------------------------------------------------
async def _lot_credit_preview(user: Any, analysis_id: str) -> Dict[str, Any]:
    """Authoritative credit preview for lot generation/rerun (read-only)."""
    import server  # type: ignore  # lazy: avoid circular import

    quota = getattr(user, "quota", None)
    quota = quota if isinstance(quota, dict) else {}
    try:
        available = int(quota.get("perizia_scans_remaining", 0) or 0)
    except (TypeError, ValueError):
        available = 0
    return {
        "can_start": True,
        "will_consume_credit": False,
        "credits_required": 0,
        "available_credits": available,
        "already_paid_at_upload": True,
        "exempt": bool(server._is_credit_exempt_user(user)),
        "reason": None,
    }


@router.get("/{analysis_id}/correctness-v2/workspace")
async def correctness_v2_workspace(analysis_id: str, request: Request) -> Dict[str, Any]:
    """Customer-safe lot overview for an analysis. Pure read: ZERO side effects.

    Never spawns a job, never calls OpenAI, never debits credits -- opening or
    polling the workspace is always free and always safe.
    """
    user, _is_admin = await _resolve_customer_access(request, analysis_id)
    payload = workspace.build_workspace(analysis_id)
    payload["credit_preview"] = await _lot_credit_preview(user, analysis_id)
    return payload


@router.get("/{analysis_id}/correctness-v2/lots/{lot_id}/generate/preview")
async def correctness_v2_lot_generate_preview(
    analysis_id: str, lot_id: str, request: Request
) -> Dict[str, Any]:
    """Read-only preview of what generating/rerunning a lot would do."""
    user, _is_admin = await _resolve_customer_access(request, analysis_id)
    preview = await _lot_credit_preview(user, analysis_id)
    preview["lot_state"] = (
        workspace.latest_lot_outcome(analysis_id, lot_id) or workspace.STATE_NOT_ANALYZED
    )
    preview["can_start"] = workspace.lot_in_progress(analysis_id, lot_id) is None
    return preview


def _lot_dedup_response(running: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """The 'a job already covers this lot' response (no spawn, reuse it)."""
    return {
        "deduplicated": True,
        "reused_report": False,
        "spawned": False,
        "job_id": (running or {}).get("job_id"),
        "state": "RUNNING",
        "preparing": True,
    }


@router.post("/{analysis_id}/correctness-v2/lots/{lot_id}/generate")
async def correctness_v2_generate_lot(
    analysis_id: str, lot_id: str, request: Request
) -> Dict[str, Any]:
    """Explicitly generate (or, with force, rerun) one lot's report.

    Server-authoritative reuse/dedup rules, applied in order (plan §E):
      A. a customer-safe report already exists and not force -> reuse it, no job;
      B. a job already covers this lot -> deduplicate onto it, no new job;
      C. a FAILED/VERIFICATION_REQUIRED lot never auto-reruns: 409 unless force;
      D. otherwise exactly ONE job is spawned (in-flight marker closes the race).
    No credit debit ever happens here (see billing rule above).
    """
    _lot_user, _lot_is_admin = await _resolve_customer_access(request, analysis_id)
    _lot_user_id = getattr(_lot_user, "user_id", None)

    try:
        body = await request.json()
    except Exception:
        body = None
    force = bool(body.get("force")) if isinstance(body, dict) else False

    # A. Existing safe report: serve the stored artifact, never rebuild via LLM.
    status, report = workspace.find_lot_safe_report(analysis_id, lot_id)
    if report is not None and not force:
        _emit_lot_signal(
            "LOT_REPORT_REUSED", analysis_id, lot_id, _lot_user_id,
            job_id=(status or {}).get("job_id"),
        )
        return {
            "deduplicated": False,
            "reused_report": True,
            "spawned": False,
            "job_id": (status or {}).get("job_id"),
            "state": report.get("report_status"),
            "preparing": False,
        }

    # B. A job is already working on this lot: reuse it, no duplicate.
    running = workspace.lot_in_progress(analysis_id, lot_id)
    if running is not None:
        _emit_lot_signal(
            "LOT_JOB_DEDUPLICATED", analysis_id, lot_id, _lot_user_id,
            job_id=(running or {}).get("job_id"),
        )
        return _lot_dedup_response(running)

    # A forced rerun of an existing lot (customer explicitly re-ran).
    if force:
        _emit_lot_signal("LOT_RERUN_FORCED", analysis_id, lot_id, _lot_user_id)

    # C. Failed/verification-required lots never auto-rerun (the core fix):
    # the customer must explicitly confirm the rerun (force=true).
    if not force:
        outcome = workspace.latest_lot_outcome(analysis_id, lot_id)
        if outcome in (workspace.STATE_FAILED, workspace.STATE_VERIFICATION_REQUIRED):
            raise HTTPException(
                status_code=409,
                detail={
                    "reason_code": "LOT_FAILED_RERUN_REQUIRED",
                    "reason_human": (
                        "Il lotto ha un tentativo non completato: confermare la rigenerazione."
                    ),
                },
            )

    # D. Claim the per-(analysis, lot) generation slot atomically so two
    # simultaneous requests spawn exactly one job.
    if not workspace.begin_generation(analysis_id, lot_id):
        return _lot_dedup_response(workspace.lot_in_progress(analysis_id, lot_id))

    spawned = False
    try:
        selected = None if not workspace.is_multi_lot(analysis_id) else str(lot_id)
        pages_count = await _analysis_pages_count(analysis_id)
        if pages_count <= 0:
            raise HTTPException(
                status_code=409,
                detail={
                    "reason_code": "ANALYSIS_NOT_READY",
                    "reason_human": "L'analisi non è pronta: pagine non disponibili.",
                },
            )

        def _run() -> None:
            try:
                import server  # type: ignore  # lazy: avoid circular import

                def _page_loader(aid: str) -> List[Dict[str, Any]]:
                    return server._load_pages_for_analysis(aid, pages_count)

                start_job(
                    analysis_id,
                    _page_loader,
                    is_admin=False,
                    openai_caller=openai_client.call_openai_json,
                    selected_lot_id=selected,
                    analyze_all=False,
                )
            except Exception:
                logger.exception(
                    "correctness_v2 lot generate job failed analysis_id=%s lot=%s",
                    analysis_id,
                    lot_id,
                )
            finally:
                workspace.finish_generation(analysis_id, lot_id)

        thread = threading.Thread(
            target=_run,
            name=f"cv2-lot-generate-{analysis_id}-{lot_id}",
            daemon=True,
        )
        thread.start()
        spawned = True
        logger.info(
            "correctness_v2 lot generate spawned analysis_id=%s lot=%s force=%s",
            analysis_id,
            lot_id,
            force,
        )
    finally:
        # Once the thread is running IT owns the marker (released in its own
        # finally); on any earlier exit the request must release it here.
        if not spawned:
            workspace.finish_generation(analysis_id, lot_id)

    return {
        "deduplicated": False,
        "reused_report": False,
        "spawned": True,
        "state": "RUNNING",
        "preparing": True,
    }


@router.get("/{analysis_id}/correctness-v2/latest")
async def correctness_v2_latest(analysis_id: str, request: Request) -> Dict[str, Any]:
    await _resolve_user_and_guard(request)
    status = artifacts.latest_job_for_analysis(analysis_id)
    if not status:
        raise HTTPException(status_code=404, detail="No Correctness v2 job for this analysis")
    return status
