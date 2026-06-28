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

from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, Request

from . import artifacts, feature_flags, job_status
from .orchestrator import start_job

router = APIRouter(prefix="/analysis/perizia", tags=["correctness_v2"])


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


@router.post("/{analysis_id}/correctness-v2/start")
async def correctness_v2_start(analysis_id: str, request: Request) -> Dict[str, Any]:
    import server  # type: ignore

    user, is_admin = await _resolve_user_and_guard(request)

    # Resolve pages_count (also validates the analysis exists) up front.
    fetch_pages_count = _build_admin_page_loader()
    pages_count = await fetch_pages_count(analysis_id)

    def _page_loader(aid: str) -> List[Dict[str, Any]]:
        return server._load_pages_for_analysis(aid, pages_count)

    status = start_job(analysis_id, _page_loader, is_admin=is_admin)

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


@router.get("/{analysis_id}/correctness-v2/latest")
async def correctness_v2_latest(analysis_id: str, request: Request) -> Dict[str, Any]:
    await _resolve_user_and_guard(request)
    status = artifacts.latest_job_for_analysis(analysis_id)
    if not status:
        raise HTTPException(status_code=404, detail="No Correctness v2 job for this analysis")
    return status
