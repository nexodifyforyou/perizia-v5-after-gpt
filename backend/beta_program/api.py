"""
Exact-owner-only admin API for the beta program.

All routes are gated by ``server.require_exact_owner_admin`` (the single owner
email). Non-owner admins, testers, normal customers -> 403; unauthenticated ->
401. Every mutation writes an append-only ``beta_program_audit`` row (in the
store) and a generic ``BETA_PROGRAM_*`` admin-audit entry.

Reads perform Mongo counts/reads only: no OpenAI, no job spawn, no wallet/ledger
write, no Stripe. Server symbols are imported lazily to avoid a circular import
with the large server.py module.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from . import quota, signals, store

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/beta-program", tags=["beta_program"])


def _membership_view(doc: Dict[str, Any], *, include_note: bool = True) -> Dict[str, Any]:
    """Owner-facing membership view + the derived quota block (frontend
    contract: {mode, limit, consumed, reserved, remaining, state,
    quota_version}), same shape as the customer entitlement snapshot."""
    view = store.public_membership(doc, include_note=include_note)
    view["quota"] = quota.derive_quota_state(doc)
    return view


def _server():
    import server  # type: ignore  # lazy

    return server


async def _require_owner(request: Request):
    server = _server()
    return await server.require_exact_owner_admin(request)


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------
class AddTesterPayload(BaseModel):
    email: str
    display_name: Optional[str] = None
    partner_type: Optional[str] = None
    internal_note: Optional[str] = None


class UpdateTesterPayload(BaseModel):
    display_name: Optional[str] = None
    partner_type: Optional[str] = None
    internal_note: Optional[str] = None
    # Sentinel handling: only fields explicitly provided are updated.


class FeedbackPatchPayload(BaseModel):
    status: Optional[str] = None
    admin_notes: Optional[str] = None
    priority: Optional[str] = None
    category: Optional[str] = None


def _raise_beta_error(exc: store.BetaProgramError) -> None:
    raise HTTPException(
        status_code=exc.status_code,
        detail={"reason_code": exc.reason_code, "reason_human": exc.message},
    )


# ---------------------------------------------------------------------------
# Tester management
# ---------------------------------------------------------------------------
@router.get("/testers")
async def list_testers(
    request: Request,
    status: Optional[str] = None,
    q: Optional[str] = None,
    page: int = 1,
    page_size: int = 25,
) -> Dict[str, Any]:
    await _require_owner(request)
    server = _server()
    result = await store.list_memberships(status=status, q=q, page=page, page_size=page_size)
    db = server.db
    items: List[Dict[str, Any]] = []
    for doc in result["items"]:
        view = _membership_view(doc, include_note=True)
        uid = doc.get("user_id")
        if uid:
            view["analyses_total"] = await db.perizia_analyses.count_documents({"user_id": uid})
            view["unreadable_total"] = await db.perizia_analyses.count_documents(
                {"user_id": uid, "status": "UNREADABLE"}
            )
            view["feedback_total"] = await db.beta_feedback.count_documents({"user_id": uid})
        else:
            view["analyses_total"] = 0
            view["unreadable_total"] = 0
            view["feedback_total"] = await db.beta_feedback.count_documents(
                {"user_email": doc.get("normalized_email")}
            )
        items.append(view)
    return {
        "items": items,
        "total": result["total"],
        "page": result["page"],
        "page_size": result["page_size"],
    }


@router.post("/testers")
async def add_tester(request: Request, payload: AddTesterPayload) -> Dict[str, Any]:
    owner = await _require_owner(request)
    server = _server()

    async def _user_lookup(normalized_email: str):
        return await server.db.users.find_one({"email": normalized_email}, {"_id": 0})

    try:
        membership = await store.add_tester(
            email=payload.email,
            display_name=payload.display_name,
            partner_type=payload.partner_type,
            internal_note=payload.internal_note,
            actor_email=owner.email,
            actor_user_id=owner.user_id,
            user_lookup=_user_lookup,
            is_admin_email=server._is_admin_email,
        )
    except store.BetaProgramError as exc:
        _raise_beta_error(exc)
    await server._write_admin_audit(
        owner,
        "BETA_PROGRAM_TESTER_ADDED",
        target_user_id=membership.get("user_id"),
        target_email=membership.get("normalized_email"),
        meta={"membership_id": membership.get("membership_id"), "status": membership.get("status")},
    )
    return {"ok": True, "tester": _membership_view(membership)}


@router.get("/testers/{membership_id}")
async def get_tester(request: Request, membership_id: str) -> Dict[str, Any]:
    await _require_owner(request)
    server = _server()
    doc = await store.get_membership(membership_id)
    if not doc:
        raise HTTPException(
            status_code=404,
            detail={"reason_code": "MEMBERSHIP_NOT_FOUND", "reason_human": "Membership non trovata."},
        )
    db = server.db
    uid = doc.get("user_id")
    activity: Dict[str, Any] = {"analyses_total": 0, "unreadable_total": 0, "feedback_total": 0}
    signal_counts: Dict[str, int] = {}
    if uid:
        activity["analyses_total"] = await db.perizia_analyses.count_documents({"user_id": uid})
        activity["unreadable_total"] = await db.perizia_analyses.count_documents(
            {"user_id": uid, "status": "UNREADABLE"}
        )
        activity["feedback_total"] = await db.beta_feedback.count_documents({"user_id": uid})
        signal_counts = await signals.event_counts([uid])
    audit = await store.list_audit(membership_id=membership_id, page=1, page_size=50)
    return {
        "tester": _membership_view(doc),
        "activity": activity,
        "signals": signal_counts,
        "audit": audit["items"],
    }


@router.patch("/testers/{membership_id}")
async def update_tester(request: Request, membership_id: str, payload: UpdateTesterPayload) -> Dict[str, Any]:
    owner = await _require_owner(request)
    server = _server()
    provided = payload.model_dump(exclude_unset=True)
    kwargs: Dict[str, Any] = {}
    if "display_name" in provided:
        kwargs["display_name"] = provided["display_name"]
    if "partner_type" in provided:
        kwargs["partner_type"] = provided["partner_type"]
    if "internal_note" in provided:
        kwargs["internal_note"] = provided["internal_note"]
    try:
        membership = await store.update_metadata(
            membership_id=membership_id,
            actor_email=owner.email,
            actor_user_id=owner.user_id,
            **kwargs,
        )
    except store.BetaProgramError as exc:
        _raise_beta_error(exc)
    await server._write_admin_audit(
        owner, "BETA_PROGRAM_TESTER_UPDATED",
        target_email=membership.get("normalized_email"),
        meta={"membership_id": membership_id, "fields": sorted(kwargs.keys())},
    )
    return {"ok": True, "tester": _membership_view(membership)}


@router.post("/testers/{membership_id}/revoke")
async def revoke_tester(request: Request, membership_id: str) -> Dict[str, Any]:
    owner = await _require_owner(request)
    server = _server()
    try:
        membership = await store.revoke(
            membership_id=membership_id, actor_email=owner.email, actor_user_id=owner.user_id
        )
    except store.BetaProgramError as exc:
        _raise_beta_error(exc)
    await server._write_admin_audit(
        owner, "BETA_PROGRAM_TESTER_REVOKED",
        target_user_id=membership.get("user_id"),
        target_email=membership.get("normalized_email"),
        meta={"membership_id": membership_id},
    )
    return {"ok": True, "tester": _membership_view(membership)}


@router.post("/testers/{membership_id}/reactivate")
async def reactivate_tester(request: Request, membership_id: str) -> Dict[str, Any]:
    owner = await _require_owner(request)
    server = _server()
    try:
        membership = await store.reactivate(
            membership_id=membership_id, actor_email=owner.email, actor_user_id=owner.user_id
        )
    except store.BetaProgramError as exc:
        _raise_beta_error(exc)
    await server._write_admin_audit(
        owner, "BETA_PROGRAM_TESTER_REACTIVATED",
        target_user_id=membership.get("user_id"),
        target_email=membership.get("normalized_email"),
        meta={"membership_id": membership_id},
    )
    return {"ok": True, "tester": _membership_view(membership)}


# ---------------------------------------------------------------------------
# Quota (configurable beta perizia allowance) -- see beta_program/quota.py.
# ---------------------------------------------------------------------------
class SetQuotaPayload(BaseModel):
    quota_mode: str
    analysis_limit: Optional[int] = None


class NewPhasePayload(BaseModel):
    confirm: bool = False
    force_release: bool = False


@router.patch("/testers/{membership_id}/quota")
async def set_tester_quota(request: Request, membership_id: str, payload: SetQuotaPayload) -> Dict[str, Any]:
    owner = await _require_owner(request)
    server = _server()
    try:
        membership = await quota.set_quota(
            membership_id=membership_id,
            quota_mode=payload.quota_mode,
            analysis_limit=payload.analysis_limit,
            actor_email=owner.email,
            actor_user_id=owner.user_id,
        )
    except store.BetaProgramError as exc:
        _raise_beta_error(exc)
    await server._write_admin_audit(
        owner, "BETA_PROGRAM_TESTER_QUOTA_CHANGED",
        target_user_id=membership.get("user_id"),
        target_email=membership.get("normalized_email"),
        meta={
            "membership_id": membership_id,
            "quota_mode": membership.get("quota_mode"),
            "analysis_limit": membership.get("analysis_limit"),
        },
    )
    return {"ok": True, "tester": _membership_view(membership)}


@router.post("/testers/{membership_id}/quota/new-phase")
async def start_tester_new_phase(request: Request, membership_id: str, payload: NewPhasePayload) -> Dict[str, Any]:
    owner = await _require_owner(request)
    server = _server()
    if not payload.confirm:
        raise HTTPException(
            status_code=422,
            detail={
                "reason_code": "CONFIRMATION_REQUIRED",
                "reason_human": "Conferma richiesta per avviare una nuova fase beta.",
            },
        )
    try:
        membership = await quota.start_new_phase(
            membership_id=membership_id,
            actor_email=owner.email,
            actor_user_id=owner.user_id,
            force_release=payload.force_release,
        )
    except store.BetaProgramError as exc:
        _raise_beta_error(exc)
    await server._write_admin_audit(
        owner, "BETA_PROGRAM_TESTER_QUOTA_PHASE_STARTED",
        target_user_id=membership.get("user_id"),
        target_email=membership.get("normalized_email"),
        meta={"membership_id": membership_id, "quota_version": membership.get("quota_version")},
    )
    return {"ok": True, "tester": _membership_view(membership)}


@router.get("/testers/{membership_id}/quota/phases")
async def get_tester_quota_phases(request: Request, membership_id: str) -> Dict[str, Any]:
    await _require_owner(request)
    doc = await store.get_membership(membership_id)
    if not doc:
        raise HTTPException(
            status_code=404,
            detail={"reason_code": "MEMBERSHIP_NOT_FOUND", "reason_human": "Membership non trovata."},
        )
    result = await quota.list_phases(membership_id)
    result["current_quota"] = quota.derive_quota_state(doc)
    return result


# ---------------------------------------------------------------------------
# Audit
# ---------------------------------------------------------------------------
@router.get("/audit")
async def list_audit(
    request: Request,
    membership_id: Optional[str] = None,
    action: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
) -> Dict[str, Any]:
    await _require_owner(request)
    return await store.list_audit(
        membership_id=membership_id, action=action, page=page, page_size=page_size
    )


# ---------------------------------------------------------------------------
# Feedback (owner-gated; reuses server helpers so tester text stays verbatim)
# ---------------------------------------------------------------------------
@router.get("/feedback")
async def list_feedback(
    request: Request,
    user_email: Optional[str] = None,
    analysis_id: Optional[str] = None,
    section_key: Optional[str] = None,
    feedback_type: Optional[str] = None,
    priority: Optional[str] = None,
    status: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    model_should_learn: Optional[bool] = None,
    error_category: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
) -> Dict[str, Any]:
    owner = await _require_owner(request)
    server = _server()
    page = max(1, page)
    page_size = max(1, min(200, page_size))
    query = server._build_beta_feedback_admin_query(
        user_email=user_email, analysis_id=analysis_id, section_key=section_key,
        feedback_type=feedback_type, priority=priority, status=status,
        date_from=date_from, date_to=date_to, model_should_learn=model_should_learn,
        error_category=error_category,
    )
    db = server.db
    total = await db.beta_feedback.count_documents(query)
    items = (
        await db.beta_feedback.find(query, {"_id": 0})
        .sort("created_at", -1)
        .skip((page - 1) * page_size)
        .limit(page_size)
        .to_list(page_size)
    )
    metrics = await server._compute_beta_feedback_metrics_aggregated(query)
    await server._write_admin_audit(
        owner, "BETA_PROGRAM_FEEDBACK_VIEW", meta={"page": page}
    )
    return {"items": items, "page": page, "page_size": page_size, "total": total, "metrics": metrics}


@router.patch("/feedback/{feedback_id}")
async def patch_feedback(request: Request, feedback_id: str, payload: FeedbackPatchPayload) -> Dict[str, Any]:
    owner = await _require_owner(request)
    server = _server()
    return await server._apply_beta_feedback_owner_update(
        owner, feedback_id, payload.model_dump(exclude_unset=True)
    )


@router.get("/feedback/export")
async def export_feedback(
    request: Request,
    format: str = "json",
    user_email: Optional[str] = None,
    analysis_id: Optional[str] = None,
    section_key: Optional[str] = None,
    feedback_type: Optional[str] = None,
    priority: Optional[str] = None,
    status: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    model_should_learn: Optional[bool] = None,
    error_category: Optional[str] = None,
):
    owner = await _require_owner(request)
    server = _server()
    query = server._build_beta_feedback_admin_query(
        user_email=user_email, analysis_id=analysis_id, section_key=section_key,
        feedback_type=feedback_type, priority=priority, status=status,
        date_from=date_from, date_to=date_to, model_should_learn=model_should_learn,
        error_category=error_category,
    )
    return await server._export_beta_feedback_for_query(owner, query, format)


# ---------------------------------------------------------------------------
# Signals (operational; separate from tester feedback)
# ---------------------------------------------------------------------------
@router.get("/signals")
async def list_signals(
    request: Request,
    membership_id: Optional[str] = None,
    user_id: Optional[str] = None,
    signal: Optional[str] = None,
    analysis_id: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
) -> Dict[str, Any]:
    await _require_owner(request)
    user_ids: Optional[List[str]] = None
    if membership_id:
        doc = await store.get_membership(membership_id)
        uid = doc.get("user_id") if doc else None
        user_ids = [uid] if uid else ["__none__"]
    elif user_id:
        user_ids = [user_id]
    else:
        grouped = await store.active_and_revoked_user_ids()
        combined = grouped[store.STATUS_ACTIVE] + grouped[store.STATUS_REVOKED]
        user_ids = combined or None
    return await signals.list_signals(
        user_ids=user_ids,
        analysis_id=analysis_id,
        event_type=signal,
        date_from=date_from,
        date_to=date_to,
        page=page,
        page_size=page_size,
    )


# ---------------------------------------------------------------------------
# Overview (Panoramica)
# ---------------------------------------------------------------------------
@router.get("/overview")
async def overview(request: Request) -> Dict[str, Any]:
    await _require_owner(request)
    server = _server()
    db = server.db

    counts = await store.status_counts()
    grouped = await store.active_and_revoked_user_ids()
    active_ids = grouped[store.STATUS_ACTIVE]
    revoked_ids = grouped[store.STATUS_REVOKED]
    all_ids = active_ids + revoked_ids
    registered = len(all_ids)

    analyses_beta_total = 0
    unreadable_total = 0
    if all_ids:
        analyses_beta_total = await db.perizia_analyses.count_documents(
            {"user_id": {"$in": all_ids}}
        )
        unreadable_total = await db.perizia_analyses.count_documents(
            {"user_id": {"$in": all_ids}, "status": "UNREADABLE"}
        )

    sig_counts = await signals.event_counts(all_ids) if all_ids else {}
    avg_duration = await signals.avg_report_duration_seconds(all_ids) if all_ids else None

    feedback_query = {"user_role": "beta_partner"}
    feedback_metrics = await server._compute_beta_feedback_metrics_aggregated(feedback_query)

    return {
        "testers": {
            "active": counts[store.STATUS_ACTIVE],
            "pending": counts[store.STATUS_PENDING],
            "revoked": counts[store.STATUS_REVOKED],
            "registered": registered,
        },
        "analyses": {
            "beta_total": analyses_beta_total,
            "unreadable_total": unreadable_total,
        },
        "reports": {
            "ready_total": sig_counts.get(signals.EVENT_REPORT_READY, 0),
            "verification_required_total": sig_counts.get(
                signals.EVENT_VERIFICATION_REQUIRED, 0
            ),
            "confirmation_required_total": sig_counts.get(
                signals.EVENT_CONFIRMATION_REQUIRED, 0
            ),
            "confirmation_completed_total": sig_counts.get(
                signals.EVENT_CONFIRMATION_COMPLETED, 0
            ),
            "reused_total": sig_counts.get(signals.EVENT_LOT_REPORT_REUSED, 0),
            "forced_rerun_total": sig_counts.get(signals.EVENT_LOT_RERUN_FORCED, 0),
            "service_busy_total": sig_counts.get(signals.EVENT_SERVICE_BUSY, 0),
            "service_unavailable_total": sig_counts.get(
                signals.EVENT_SERVICE_UNAVAILABLE, 0
            ),
            "avg_duration_seconds": avg_duration,
        },
        "feedback": feedback_metrics,
        "signals": sig_counts,
    }
