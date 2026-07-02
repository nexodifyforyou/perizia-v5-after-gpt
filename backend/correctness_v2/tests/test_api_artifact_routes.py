import asyncio

import httpx
from fastapi import FastAPI
import pytest

from correctness_v2 import api, artifacts


@pytest.fixture()
def artifact_app(monkeypatch):
    async def _allow_admin(request):
        return {"user_id": "admin"}, True

    monkeypatch.setattr(api, "_resolve_user_and_guard", _allow_admin)
    app = FastAPI()
    app.include_router(api.router, prefix="/api")
    return app


async def _get(app, path):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.get(path)


def _sync_get(app, path):
    return asyncio.run(_get(app, path))


def test_customer_report_route_returns_known_artifact_only(artifact_app, artifacts_root):
    job_id = "cv2_route_customer"
    analysis_id = "analysis_route"
    artifacts.save_job_status(
        job_id,
        {
            "job_id": job_id,
            "analysis_id": analysis_id,
            "status": "REPORT_READY",
            "artifacts_saved": {
                "customer_report": str(artifacts.job_dir(job_id) / artifacts.CUSTOMER_REPORT_FILE)
            },
        },
    )
    artifacts.save_customer_report(
        job_id,
        {
            "schema_version": "cv2.customer_report.v1",
            "analysis_id": analysis_id,
            "job_id": job_id,
            "report_status": "REPORT_READY",
            "title": "Report generico",
        },
    )

    response = _sync_get(
        artifact_app,
        f"/api/analysis/perizia/{analysis_id}/correctness-v2/jobs/{job_id}/customer-report",
    )

    assert response.status_code == 200
    data = response.json()
    assert data["report_status"] == "REPORT_READY"
    assert data["title"] == "Report generico"
    assert str(artifacts_root) not in response.text


def test_artifact_route_rejects_wrong_analysis_id(artifact_app, artifacts_root):
    job_id = "cv2_route_wrong_analysis"
    artifacts.save_job_status(
        job_id,
        {
            "job_id": job_id,
            "analysis_id": "analysis_owner",
            "status": "REPORT_READY",
            "artifacts_saved": {},
        },
    )
    artifacts.save_customer_report(job_id, {"report_status": "REPORT_READY"})

    response = _sync_get(
        artifact_app,
        f"/api/analysis/perizia/analysis_other/correctness-v2/jobs/{job_id}/customer-report",
    )

    assert response.status_code == 404


def test_lot_selection_and_validator_routes_return_artifacts(artifact_app, artifacts_root):
    job_id = "cv2_route_extra_artifacts"
    analysis_id = "analysis_extra"
    artifacts.save_job_status(
        job_id,
        {
            "job_id": job_id,
            "analysis_id": analysis_id,
            "status": "LOT_SELECTION_REQUIRED",
            "artifacts_saved": {},
        },
    )
    artifacts.save_lot_selection_required(
        job_id,
        {
            "schema_version": "cv2.lot_selection_required.v1",
            "status": "LOT_SELECTION_REQUIRED",
            "available_lots": [{"lot_id": "1", "label": "Lotto 1"}],
        },
    )
    artifacts.save_validator_report(
        job_id,
        {"validation_status": "VALIDATED", "violations": []},
    )

    lot_response = _sync_get(
        artifact_app,
        f"/api/analysis/perizia/{analysis_id}/correctness-v2/jobs/{job_id}/lot-selection-report",
    )
    validator_response = _sync_get(
        artifact_app,
        f"/api/analysis/perizia/{analysis_id}/correctness-v2/jobs/{job_id}/validator-report",
    )

    assert lot_response.status_code == 200
    assert lot_response.json()["available_lots"][0]["lot_id"] == "1"
    assert validator_response.status_code == 200
    assert validator_response.json()["validation_status"] == "VALIDATED"
