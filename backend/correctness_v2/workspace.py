"""
Storico lot workspace for Correctness Mode v2.

This module turns the persisted job artifacts of an analysis into a customer-safe
**lot workspace**: a per-lot overview showing, for every lot, whether a safe
report already exists, whether a job is running, whether verification/confirmation
is required, or whether the lot has never been analyzed.

Design rules (see docs/storico_lot_workspace_plan.md):

  * PURE READ. ``build_workspace`` and every ``find_*`` helper have ZERO side
    effects: they never spawn a job, never call OpenAI, never debit credits. All
    job creation is an explicit action routed through the generate endpoint.
  * REUSE. An already-generated lot report (job-level REPORT_READY, an
    analyze_all per-lot report, or a paused MONEY_CONFIRMATION_REQUIRED report) is
    surfaced as-is; the workspace never rebuilds it through the LLM.
  * FAIL-CLOSED. Only the closed set of customer-safe statuses is ever shown as a
    lot report; every other terminal status maps to VERIFICATION_REQUIRED or
    FAILED and never auto-reruns.
  * SAFE-REPORT SURVIVAL. When a newer attempt fails, the last safe report stays
    reachable and the lot is flagged ``last_attempt_failed`` — a failed rerun
    never replaces a safe report with an error page.

Lot-generation duplicate-suppression (rule E in the plan) is enforced here too:
``begin_generation`` / ``finish_generation`` guard a per-(analysis, lot) in-flight
marker, and ``lot_in_progress`` consults both the persisted job files and that
marker so two simultaneous start requests create exactly one job.
"""

from __future__ import annotations

import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from . import artifacts, customer_view
from .schemas import JobStatus

# ---------------------------------------------------------------------------
# Public per-lot workspace states (customer-safe; never a raw internal status).
# ---------------------------------------------------------------------------
STATE_REPORT_READY = "REPORT_READY"
STATE_RUNNING = "RUNNING"
STATE_MONEY_CONFIRMATION_REQUIRED = "MONEY_CONFIRMATION_REQUIRED"
STATE_VERIFICATION_REQUIRED = "VERIFICATION_REQUIRED"
STATE_FAILED = "FAILED"
STATE_NOT_ANALYZED = "NOT_ANALYZED"

# Allowed customer actions per lot.
ACTION_OPEN_REPORT = "open_report"
ACTION_GENERATE = "generate"
ACTION_RERUN = "rerun"

# Convention for a single-lot analysis whose lot id was never made explicit.
SINGLE_LOT_ID = "1"

# Only QUEUED/RUNNING are genuinely in-flight (mirrors the orchestrator's
# _STALEABLE_STATUSES). A persisted PDF_QUALITY_OK/WARNING is always a TERMINAL
# step-1-only artifact (_finish_quality, admin mode without an OpenAI caller):
# the full customer pipeline never persists it. Treating it as in-progress made
# such a job dedup-block legitimate reruns forever.
_IN_PROGRESS_STATUSES = frozenset(
    {
        JobStatus.QUEUED,
        JobStatus.RUNNING,
    }
)

# Terminal step-1-only statuses: the PDF-quality gate ran and the job stopped
# there — no analysis was attempted, so the job carries NO lot outcome. A lot
# whose only history is such a job presents as NOT_ANALYZED (never FAILED, and
# never blocking generation).
_QUALITY_ONLY_STATUSES = frozenset(
    {
        JobStatus.PDF_QUALITY_OK,
        JobStatus.PDF_QUALITY_WARNING,
    }
)

# Terminal statuses meaning a human must verify before a report can be shown.
_VERIFICATION_STATUSES = frozenset(
    {
        JobStatus.CONTRACT_VALIDATION_FAILED,
        JobStatus.NEEDS_MANUAL_REVIEW,
        JobStatus.FAILED_GROUNDING,
    }
)

# Statuses that carry a customer-safe report the workspace may surface.
_SAFE_STATES = frozenset({STATE_REPORT_READY, STATE_MONEY_CONFIRMATION_REQUIRED})

# In-flight generation registry: (analysis_id, lot_id) -> epoch spawned. A marker
# older than this many seconds is treated as stale (the spawning request/thread is
# assumed gone) so a wedged marker can never block generation forever.
_INFLIGHT_TTL_SECONDS = 180.0
_gen_lock = threading.Lock()
_inflight: Dict[Tuple[str, str], float] = {}


# ---------------------------------------------------------------------------
# Job → lot state mapping
# ---------------------------------------------------------------------------
def _state_from_status(status_value: Any) -> str:
    """Map a raw JobStatus/report-status to a public lot workspace state."""
    s = str(status_value or "")
    if s in _IN_PROGRESS_STATUSES:
        return STATE_RUNNING
    if s == JobStatus.REPORT_READY:
        return STATE_REPORT_READY
    if s == JobStatus.MONEY_CONFIRMATION_REQUIRED:
        return STATE_MONEY_CONFIRMATION_REQUIRED
    if s in _VERIFICATION_STATUSES:
        return STATE_VERIFICATION_REQUIRED
    # Every other terminal status (FAILED_*, JOB_STALLED, CANCELLED, ...): failed.
    return STATE_FAILED


def _lot_report_filename(lot_id: str) -> str:
    """analyze_all per-lot customer report path, mirroring save_lot_subartifact."""
    safe_lot = str(lot_id).replace("/", "_").replace("..", "_")
    return os.path.join("lots", safe_lot, artifacts.CUSTOMER_REPORT_FILE)


def _sort_key(status: Dict[str, Any]) -> str:
    return str(status.get("updated_at") or status.get("created_at") or "")


class _Record:
    """One (job, lot) outcome, used to fold a lot's history into a display state.

    ``lot_id`` may be ``None``: the sentinel for "the sole lot of a single-lot
    analysis" (used when a job carries no explicit lot id, e.g. a failed run
    that never produced a customer report). A sentinel record can never invent
    a phantom lot: it only ever folds into the canonical single lot.
    """

    __slots__ = ("lot_id", "state", "job_id", "report_filename", "updated_at", "raw_status")

    def __init__(
        self,
        lot_id: Optional[str],
        state: str,
        job_id: str,
        report_filename: Optional[str],
        updated_at: str,
        raw_status: str,
    ) -> None:
        self.lot_id = None if lot_id is None else str(lot_id)
        self.state = state
        self.job_id = job_id
        self.report_filename = report_filename
        self.updated_at = updated_at
        self.raw_status = raw_status


def _analysis_jobs(analysis_id: str) -> List[Dict[str, Any]]:
    """All job_status dicts for an analysis, oldest → newest (never raises)."""
    out: List[Dict[str, Any]] = []
    try:
        job_ids = artifacts.list_jobs()
    except Exception:
        return out
    for jid in job_ids:
        status = artifacts.read_job_status(jid)
        if isinstance(status, dict) and str(status.get("analysis_id")) == str(analysis_id):
            out.append(status)
    out.sort(key=_sort_key)
    return out


def _best_lot_index(jobs: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """The most recent job's lot_index.json (rich lot metadata), or None."""
    for status in reversed(jobs):
        idx = artifacts.read_json(str(status.get("job_id")), artifacts.LOT_INDEX_FILE)
        if isinstance(idx, dict) and idx.get("lots"):
            return idx
    return None


def _records_for_job(status: Dict[str, Any]) -> List[_Record]:
    """Per-lot outcome records contributed by a single job."""
    jid = str(status.get("job_id") or "")
    updated_at = _sort_key(status)
    raw = str(status.get("status") or "")
    records: List[_Record] = []

    # Terminal step-1-only job (quality gate only, analysis never ran):
    # contributes no lot outcome at all — it must never appear as an attempt,
    # never set last_attempt_failed, and never block or dedup a rerun.
    if raw in _QUALITY_ONLY_STATUSES:
        return records

    # analyze_all: a batch job producing one contract per lot.
    if status.get("analyze_all"):
        if raw in _IN_PROGRESS_STATUSES:
            # Still running: every targeted lot is preparing.
            for lid in status.get("lot_ids") or []:
                records.append(_Record(lid, STATE_RUNNING, jid, None, updated_at, raw))
            return records
        for entry in status.get("per_lot_results") or []:
            if not isinstance(entry, dict):
                continue
            lid = str(entry.get("lot_id") or "")
            if not lid:
                continue
            state = _state_from_status(entry.get("status"))
            fname = _lot_report_filename(lid) if state in _SAFE_STATES else None
            records.append(_Record(lid, state, jid, fname, updated_at, str(entry.get("status") or "")))
        return records

    # Selected-lot job: a single lot re-analyzed on its own isolated pages.
    selected = status.get("selected_lot")
    if selected not in (None, "", []):
        state = _state_from_status(raw)
        fname = artifacts.CUSTOMER_REPORT_FILE if state in _SAFE_STATES else None
        records.append(_Record(str(selected), state, jid, fname, updated_at, raw))
        return records

    # Multi-lot document with no selection: the selector, not a lot outcome.
    if raw == JobStatus.LOT_SELECTION_REQUIRED:
        return records

    # Otherwise a single-lot analysis (initial upload job or its rerun): maps to
    # the sole lot. Running, ready or failed all attach to the single lot. When
    # the report names its lot we use that id; otherwise the record carries the
    # None sentinel ("the sole lot") so a failed job with no customer report can
    # never invent a phantom lot id.
    report = artifacts.read_json(jid, artifacts.CUSTOMER_REPORT_FILE)
    single_lot: Optional[str] = None
    if isinstance(report, dict):
        sel = ((report.get("lot_structure") or {}).get("selected_lot"))
        if sel not in (None, "", []):
            single_lot = str(sel)
    state = _state_from_status(raw)
    fname = artifacts.CUSTOMER_REPORT_FILE if state in _SAFE_STATES else None
    records.append(_Record(single_lot, state, jid, fname, updated_at, raw))
    return records


def _fold_lot(records: List[_Record], inflight: bool) -> Dict[str, Any]:
    """Fold a lot's outcome records (any order) into a single display state."""
    records = sorted(records, key=lambda r: r.updated_at)
    latest_safe: Optional[_Record] = None
    latest_any: Optional[_Record] = None
    for rec in records:
        latest_any = rec if latest_any is None or rec.updated_at >= latest_any.updated_at else latest_any
        if rec.state in _SAFE_STATES:
            if latest_safe is None or rec.updated_at >= latest_safe.updated_at:
                latest_safe = rec
    # Only the NEWEST record can mean "running": an old abandoned RUNNING job
    # must never mask a newer safe report (or a newer terminal failure).
    running = inflight or (latest_any is not None and latest_any.state == STATE_RUNNING)

    has_safe = latest_safe is not None
    last_attempt_failed = False

    if has_safe:
        display = latest_safe.state
        if running:
            display = STATE_RUNNING
        elif latest_any is not None and latest_any.state not in _SAFE_STATES:
            # A newer attempt did not complete: keep the safe report, flag it.
            if latest_any.updated_at > latest_safe.updated_at:
                last_attempt_failed = True
    else:
        if running:
            display = STATE_RUNNING
        elif latest_any is not None:
            display = latest_any.state
        else:
            display = STATE_NOT_ANALYZED

    safe_ref = None
    if has_safe:
        safe_ref = {"job_id": latest_safe.job_id, "report_filename": latest_safe.report_filename}

    return {
        "state": display,
        "has_safe_report": has_safe,
        "job_running": running,
        "last_attempt_failed": last_attempt_failed,
        "latest_report_at": latest_safe.updated_at if has_safe else None,
        "latest_attempt_at": latest_any.updated_at if latest_any else None,
        "report_version": len([r for r in records if r.state in _SAFE_STATES]),
        "safe_ref": safe_ref,
        "actions": _actions_for(display, has_safe),
    }


def _actions_for(state: str, has_safe: bool) -> List[str]:
    if state == STATE_NOT_ANALYZED:
        return [ACTION_GENERATE]
    if state == STATE_RUNNING:
        return [ACTION_OPEN_REPORT] if has_safe else []
    if state in _SAFE_STATES:
        return [ACTION_OPEN_REPORT, ACTION_RERUN]
    # FAILED / VERIFICATION_REQUIRED: explicit retry, plus the safe report if kept.
    return ([ACTION_OPEN_REPORT, ACTION_RERUN] if has_safe else [ACTION_RERUN])


# ---------------------------------------------------------------------------
# Public: build the workspace payload (pure read)
# ---------------------------------------------------------------------------
def build_workspace(analysis_id: str) -> Dict[str, Any]:
    """Customer-safe lot overview for an analysis. No side effects, ever."""
    jobs = _analysis_jobs(analysis_id)
    lot_index = _best_lot_index(jobs)
    multi_lot = _is_multi_lot(jobs)

    # Fold every job into per-lot records (lot_id None = single-lot sentinel).
    by_lot: Dict[Optional[str], List[_Record]] = {}
    for status in jobs:
        for rec in _records_for_job(status):
            by_lot.setdefault(rec.lot_id, []).append(rec)

    # Canonical lot set + display metadata.
    meta_by_lot: Dict[str, Dict[str, Any]] = {}
    ordered_ids: List[str] = []
    if isinstance(lot_index, dict) and lot_index.get("lots"):
        for lot in lot_index.get("lots") or []:
            lid = str(lot.get("lot_id"))
            ordered_ids.append(lid)
            meta_by_lot[lid] = {
                "label": lot.get("label") or f"Lotto {lid}",
                "address": lot.get("address"),
                "property_type": lot.get("property_type"),
                "ownership_right": lot.get("ownership_right"),
                "occupancy_summary": lot.get("occupancy_summary"),
            }

    lot_records: Dict[str, List[_Record]]
    if multi_lot:
        # Include any explicit lot that has outcomes but is not in the index
        # (defensive). Sentinel records (no explicit lot id) cannot be safely
        # attributed to one lot of a multi-lot analysis: they are dropped, never
        # allowed to invent a phantom lot.
        for lid in by_lot.keys():
            if lid is not None and lid not in meta_by_lot:
                ordered_ids.append(lid)
                meta_by_lot[lid] = {"label": f"Lotto {lid}"}
        lot_records = {lid: by_lot.get(lid, []) for lid in ordered_ids}
    else:
        # Single-lot analysis: exactly ONE canonical lot (the index lot when
        # present, else any explicit record id, else the "1" convention). ALL
        # records — sentinel or stray — fold into that single entry, so a failed
        # attempt with no report can never appear as a separate phantom lot.
        canonical = ordered_ids[0] if ordered_ids else None
        if canonical is None:
            canonical = next((lid for lid in by_lot.keys() if lid is not None), None)
        if canonical is None:
            canonical = SINGLE_LOT_ID
        meta_by_lot.setdefault(canonical, {"label": "Immobile"})
        ordered_ids = [canonical]
        lot_records = {canonical: [rec for recs in by_lot.values() for rec in recs]}

    lots_out: List[Dict[str, Any]] = []
    for lid in ordered_ids:
        folded = _fold_lot(lot_records.get(lid, []), _is_inflight(analysis_id, lid))
        entry = {
            "lot_id": lid,
            "label": meta_by_lot.get(lid, {}).get("label") or f"Lotto {lid}",
            "address": meta_by_lot.get(lid, {}).get("address"),
            "property_type": meta_by_lot.get(lid, {}).get("property_type"),
            "ownership_right": meta_by_lot.get(lid, {}).get("ownership_right"),
            "occupancy_summary": meta_by_lot.get(lid, {}).get("occupancy_summary"),
            "final_value": _final_value_display(analysis_id, lid, folded),
            "state": folded["state"],
            "has_safe_report": folded["has_safe_report"],
            "job_running": folded["job_running"],
            "last_attempt_failed": folded["last_attempt_failed"],
            "latest_report_at": folded["latest_report_at"],
            "report_version": folded["report_version"],
            "actions": folded["actions"],
        }
        lots_out.append(entry)

    lot_count = len(lots_out)
    # Summary counters for the Storico progress line.
    summary = {
        "lot_count": lot_count,
        "ready": sum(1 for L in lots_out if L["state"] == STATE_REPORT_READY),
        "preparing": sum(1 for L in lots_out if L["state"] == STATE_RUNNING),
        "confirmation_required": sum(
            1 for L in lots_out if L["state"] == STATE_MONEY_CONFIRMATION_REQUIRED
        ),
        "verification_required": sum(
            1 for L in lots_out if L["state"] == STATE_VERIFICATION_REQUIRED
        ),
        "failed": sum(1 for L in lots_out if L["state"] == STATE_FAILED),
        "not_analyzed": sum(1 for L in lots_out if L["state"] == STATE_NOT_ANALYZED),
    }

    if multi_lot or lot_count > 1:
        analysis_state = "LOT_OVERVIEW"
    else:
        analysis_state = "SINGLE_LOT"

    return {
        "analysis_id": str(analysis_id),
        "multi_lot": bool(multi_lot or lot_count > 1),
        "lot_count": lot_count,
        "analysis_state": analysis_state,
        "summary": summary,
        "lots": lots_out,
    }


def _final_value_display(analysis_id: str, lot_id: str, folded: Dict[str, Any]) -> Optional[str]:
    """Best-effort safe judicial/final sale display value for a ready lot."""
    if not folded.get("has_safe_report"):
        return None
    ref = folded.get("safe_ref") or {}
    report = artifacts.read_json(str(ref.get("job_id")), str(ref.get("report_filename") or ""))
    if not isinstance(report, dict):
        return None
    chain = ((report.get("money_sections") or {}).get("valuation_chain")) or []
    for row in reversed(chain):
        if isinstance(row, dict) and row.get("amount_display"):
            return str(row.get("amount_display"))
    return None


# ---------------------------------------------------------------------------
# Reuse / duplicate-suppression helpers for the generate endpoint
# ---------------------------------------------------------------------------
def find_lot_safe_report(
    analysis_id: str, lot_id: str
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Most recent customer-safe (job_status, customer_report) for a lot.

    Covers job-level REPORT_READY / MONEY_CONFIRMATION_REQUIRED selected-lot and
    single-lot jobs AND analyze_all per-lot reports. Returns (None, None) when no
    safe report exists for the lot.
    """
    jobs = _analysis_jobs(analysis_id)
    single_lot = not _is_multi_lot(jobs)
    best: Optional[Tuple[str, Dict[str, Any], Dict[str, Any]]] = None
    for status in jobs:
        for rec in _records_for_job(status):
            if rec.state not in _SAFE_STATES:
                continue
            if not _lot_matches(rec.lot_id, lot_id, single_lot):
                continue
            report = artifacts.read_json(rec.job_id, rec.report_filename or artifacts.CUSTOMER_REPORT_FILE)
            if not customer_view.is_customer_safe(report, status):
                continue
            if best is None or rec.updated_at >= best[0]:
                best = (rec.updated_at, status, report)
    if best is None:
        return None, None
    return best[1], best[2]


def lot_in_progress(analysis_id: str, lot_id: str) -> Optional[Dict[str, Any]]:
    """The in-progress job covering a lot, or None. Consults the in-flight marker.

    Lot-aware (unlike the analysis-level latest-only check): a running job for a
    different lot never masks or blocks this lot.
    """
    jobs = _analysis_jobs(analysis_id)
    single_lot = not _is_multi_lot(jobs)
    for status in jobs:
        if str(status.get("status") or "") not in _IN_PROGRESS_STATUSES:
            continue
        for rec in _records_for_job(status):
            if rec.state == STATE_RUNNING and _lot_matches(rec.lot_id, lot_id, single_lot):
                return status
    return None


def latest_lot_outcome(analysis_id: str, lot_id: str) -> Optional[str]:
    """The most recent terminal state for a lot (or None if never analyzed)."""
    jobs = _analysis_jobs(analysis_id)
    single_lot = not _is_multi_lot(jobs)
    latest: Optional[Tuple[str, str]] = None
    for status in jobs:
        for rec in _records_for_job(status):
            if not _lot_matches(rec.lot_id, lot_id, single_lot):
                continue
            if latest is None or rec.updated_at >= latest[0]:
                latest = (rec.updated_at, rec.state)
    return latest[1] if latest else None


def is_multi_lot(analysis_id: str) -> bool:
    return _is_multi_lot(_analysis_jobs(analysis_id))


def _is_multi_lot(jobs: List[Dict[str, Any]]) -> bool:
    """True IFF the analysis genuinely has >= 2 lots.

    Signals: lot_index.multi_lot, any job lot_count >= 2, any job in
    LOT_SELECTION_REQUIRED, or an analyze_all batch over >= 2 lots. A bare
    ``selected_lot`` is deliberately NOT a signal: a single "unico" lot can be
    (re)run through the selected-lot path and must stay single-lot.
    """
    for status in jobs:
        if status.get("analyze_all"):
            lot_ids = status.get("lot_ids")
            if isinstance(lot_ids, list) and len(lot_ids) >= 2:
                return True
        try:
            if int(status.get("lot_count") or 0) >= 2:
                return True
        except (TypeError, ValueError):
            pass
        if str(status.get("status") or "") == JobStatus.LOT_SELECTION_REQUIRED:
            return True
    idx = _best_lot_index(jobs)
    return bool(isinstance(idx, dict) and idx.get("multi_lot"))


def _lot_matches(rec_lot: Optional[str], wanted: str, single_lot: bool) -> bool:
    if rec_lot is not None and str(rec_lot) == str(wanted):
        return True
    # In a single-lot analysis the sole lot may be recorded under the None
    # sentinel or a detected id while the caller addresses it differently: every
    # record belongs to the one lot. In multi-lot mode a sentinel never matches.
    return single_lot


# ---------------------------------------------------------------------------
# Generation in-flight registry (rule E: two simultaneous starts -> one job)
# ---------------------------------------------------------------------------
def _is_inflight(analysis_id: str, lot_id: str) -> bool:
    marker = _inflight.get((str(analysis_id), str(lot_id)))
    if marker is None:
        return False
    if time.time() - marker > _INFLIGHT_TTL_SECONDS:
        _inflight.pop((str(analysis_id), str(lot_id)), None)
        return False
    return True


def begin_generation(analysis_id: str, lot_id: str) -> bool:
    """Atomically claim the generation slot for a lot.

    Returns True if the caller may spawn a job (and marks the slot in-flight), or
    False when a job for this lot is already running/in-flight (the caller must
    reuse it). The whole check-then-claim runs under a lock so two simultaneous
    requests can never both spawn.
    """
    key = (str(analysis_id), str(lot_id))
    with _gen_lock:
        if _is_inflight(analysis_id, lot_id):
            return False
        if lot_in_progress(analysis_id, lot_id) is not None:
            return False
        _inflight[key] = time.time()
        return True


def finish_generation(analysis_id: str, lot_id: str) -> None:
    """Release a lot's in-flight generation marker."""
    with _gen_lock:
        _inflight.pop((str(analysis_id), str(lot_id)), None)
