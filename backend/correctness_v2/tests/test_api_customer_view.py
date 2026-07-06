import asyncio

import httpx
from fastapi import FastAPI
import pytest

from correctness_v2 import api, artifacts


@pytest.fixture()
def customer_app(monkeypatch):
    async def _allow(request, analysis_id):
        return {"user_id": "owner"}, False  # authenticated non-admin owner

    monkeypatch.setattr(api, "_resolve_customer_access", _allow)
    app = FastAPI()
    app.include_router(api.router, prefix="/api")
    return app


async def _get(app, path):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.get(path)


def _sync_get(app, path):
    return asyncio.run(_get(app, path))


def _save(job_id, analysis_id, report, status="REPORT_READY"):
    artifacts.save_job_status(
        job_id,
        {
            "job_id": job_id,
            "analysis_id": analysis_id,
            "status": status,
            "safe_to_show_customer": True,
            "artifacts_saved": {},
        },
    )
    artifacts.save_customer_report(job_id, report)


def test_customer_view_returns_sanitized_report_without_admin_fields(customer_app, artifacts_root):
    analysis_id = "analysis_cust_view"
    _save(
        "cv2_cust_view",
        analysis_id,
        {
            "schema_version": "cv2.customer_report.v1",
            "analysis_id": analysis_id,
            "job_id": "cv2_cust_view",
            "report_status": "REPORT_READY",
            "title": "Report cliente",
            "money_sections": {
                "valuation_chain": [{"label": "Valore", "amount_display": "EUR 100,00"}],
                "market_comparatives": [{"label": "Comparativo", "amount_display": "EUR 999,00"}],
            },
            "quality_control": {"rows": [{"pagina": 1}], "coverage_status": "PASS"},
            "admin_evidence_index": [{"page": 1, "raw_keys": "money[0]"}],
            "evidence_index": [{"page": 1, "referenced_by": ["money[0]"]}],
        },
    )

    response = _sync_get(
        customer_app,
        f"/api/analysis/perizia/{analysis_id}/correctness-v2/customer-view/latest",
    )

    assert response.status_code == 200
    data = response.json()
    assert data["available"] is True
    report = data["report"]
    assert report["report_status"] == "REPORT_READY"
    assert report["report_status_label"] == "Report pronto"
    assert "decision" in report
    # Admin/debug machinery must never reach the customer projection.
    body = response.text
    assert "quality_control" not in report
    assert "admin_evidence_index" not in report
    assert "evidence_index" not in report
    assert "market_comparatives" not in report["money_sections"]
    assert "EUR 999,00" not in body


def test_customer_view_unavailable_when_no_safe_report(customer_app, artifacts_root):
    analysis_id = "analysis_cust_none"
    _save(
        "cv2_cust_failed",
        analysis_id,
        {"report_status": "FAILED_ANALYSIS", "title": "Non disponibile"},
        status="FAILED_ANALYSIS",
    )

    response = _sync_get(
        customer_app,
        f"/api/analysis/perizia/{analysis_id}/correctness-v2/customer-view/latest",
    )

    assert response.status_code == 200
    data = response.json()
    assert data["available"] is False
    assert data["reason_code"] == "NO_CUSTOMER_REPORT"
    assert data["preparing"] is False


def test_customer_view_reports_preparing_while_a_job_runs(customer_app, artifacts_root):
    analysis_id = "analysis_cust_running"
    artifacts.save_job_status(
        "cv2_cust_running",
        {
            "job_id": "cv2_cust_running",
            "analysis_id": analysis_id,
            "status": "RUNNING",
            "safe_to_show_customer": False,
            "artifacts_saved": {},
        },
    )

    response = _sync_get(
        customer_app,
        f"/api/analysis/perizia/{analysis_id}/correctness-v2/customer-view/latest",
    )

    assert response.status_code == 200
    data = response.json()
    assert data["available"] is False
    assert data["preparing"] is True


def test_customer_view_missing_lot_triggers_autostart_when_enabled(
    customer_app, artifacts_root, monkeypatch
):
    analysis_id = "analysis_cust_lot_autostart"
    calls = {}

    async def _pages_count(aid):
        return 42

    def _fake_autostart(aid, pages_count, *, selected_lot_id=None, reason=""):
        calls["args"] = (aid, pages_count, selected_lot_id, reason)
        return True

    monkeypatch.setattr(api, "_analysis_pages_count", _pages_count)
    monkeypatch.setattr(api, "autostart_job", _fake_autostart)

    response = _sync_get(
        customer_app,
        f"/api/analysis/perizia/{analysis_id}/correctness-v2/customer-view/latest?selected_lot_id=2",
    )

    assert response.status_code == 200
    data = response.json()
    assert data["available"] is False
    assert data["preparing"] is True
    assert calls["args"] == (analysis_id, 42, "2", "customer_lot_selection")


def test_autostart_job_noop_when_flag_disabled(artifacts_root, monkeypatch):
    monkeypatch.delenv("CORRECTNESS_V2_AUTO_START", raising=False)
    monkeypatch.setenv("CORRECTNESS_V2_ENABLED", "true")
    assert api.autostart_job("analysis_flag_off", 10) is False


def test_autostart_job_does_not_stack_on_running_job(artifacts_root, monkeypatch):
    monkeypatch.setenv("CORRECTNESS_V2_ENABLED", "true")
    monkeypatch.setenv("CORRECTNESS_V2_AUTO_START", "true")
    analysis_id = "analysis_no_stack"
    artifacts.save_job_status(
        "cv2_no_stack",
        {
            "job_id": "cv2_no_stack",
            "analysis_id": analysis_id,
            "status": "RUNNING",
            "artifacts_saved": {},
        },
    )
    started = {}
    monkeypatch.setattr(
        api, "start_job", lambda *a, **k: started.setdefault("called", True)
    )
    # Reports True (a job is already preparing the report) without spawning.
    assert api.autostart_job(analysis_id, 10) is True
    assert "called" not in started


def test_customer_view_selects_the_requested_lot_report(customer_app, artifacts_root):
    analysis_id = "analysis_cust_lots"
    for lot_id in ("1", "2"):
        _save(
            f"cv2_lot_{lot_id}",
            analysis_id,
            {
                "schema_version": "cv2.customer_report.v1",
                "analysis_id": analysis_id,
                "job_id": f"cv2_lot_{lot_id}",
                "report_status": "REPORT_READY",
                "title": f"Lotto {lot_id}",
                "lot_structure": {"selected_lot": lot_id},
            },
        )

    response = _sync_get(
        customer_app,
        f"/api/analysis/perizia/{analysis_id}/correctness-v2/customer-view/latest?selected_lot_id=2",
    )

    assert response.status_code == 200
    data = response.json()
    assert data["available"] is True
    assert data["selected_lot_id"] == "2"
    assert data["report"]["title"] == "Lotto 2"
    assert data["report"]["lot_structure"]["selected_lot"] == "2"
