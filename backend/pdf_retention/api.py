"""Customer metadata and consent-withdrawal API; never serves PDF bytes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from . import config, erasure, store

router = APIRouter(tags=["pdf-retention"])


async def _server_user(request: Request):
    import server  # type: ignore

    return await server.require_auth(request)


@router.get("/pdf-retention")
async def list_my_retained_pdfs(request: Request):
    if not config.is_enabled():
        raise HTTPException(status_code=404, detail="PDF_RETENTION_DISABLED")
    user = await _server_user(request)
    db = store.database()
    rows = await db[store.RECORDS_COLLECTION].find(
        {"user_id": user.user_id, "deletion_state": store.STATE_ACTIVE},
        {
            "_id": 0,
            "analysis_id": 1,
            "created_at": 1,
            "expires_at": 1,
            "consent_version": 1,
            "retention_days_at_consent": 1,
        },
    ).sort("created_at", -1).to_list(length=500)
    analysis_ids = [row["analysis_id"] for row in rows]
    owned = await db.perizia_analyses.find(
        {"analysis_id": {"$in": analysis_ids}, "user_id": user.user_id},
        {"_id": 0, "analysis_id": 1},
    ).to_list(length=len(analysis_ids) or 1)
    owned_ids = {row["analysis_id"] for row in owned}
    safe_rows = [row for row in rows if row.get("analysis_id") in owned_ids]
    return {"retained_pdfs": safe_rows, "retention_enabled": config.is_enabled()}


@router.delete("/analysis/perizia/{analysis_id}/retained-pdf")
async def withdraw_pdf_retention(analysis_id: str, request: Request):
    user = await _server_user(request)
    db = store.database()
    analysis = await db.perizia_analyses.find_one(
        {"analysis_id": analysis_id, "user_id": user.user_id}, {"_id": 1}
    )
    if not analysis:
        raise HTTPException(status_code=404, detail="Analysis not found")
    result = await erasure.withdraw_and_erase(analysis_id, user_id=user.user_id)
    if not result.get("deleted"):
        raise HTTPException(
            status_code=503,
            detail={"code": "PDF_RETENTION_DELETE_FAILED", "retry": True},
        )
    return {"ok": True, "retained": False}
