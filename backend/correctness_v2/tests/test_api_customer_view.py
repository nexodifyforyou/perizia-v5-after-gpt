import asyncio
import json
from pathlib import Path

import httpx
from fastapi import FastAPI
import pytest

from correctness_v2 import api, artifacts


_MANTOVA_REPORT_FIXTURE = (
    Path(__file__).parent / "fixtures" / "mantova_customer_report_sanitized.json"
)
_MANTOVA_PAGES_FIXTURE = (
    Path(__file__).parent / "fixtures" / "mantova_cached_pages_sanitized.json"
)


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
            "customer_evidence_index": [
                {
                    "page": 25,
                    "topic": "Valore Bene 3",
                    "perizia_excerpt": "Valore Bene 2 EUR 26.100,00",
                }
            ],
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
    assert "customer_evidence_index" not in report
    assert "market_comparatives" not in report["money_sections"]
    assert "EUR 999,00" not in body
    assert "Valore Bene 2 EUR 26.100,00" not in body


def test_cached_mantova_route_repairs_legacy_evidence_without_writes(
    customer_app, artifacts_root, monkeypatch
):
    """The GET repairs the old cached shape from existing text, read-only.

    The legacy report deliberately retains only the incorrect Bene 3 evidence
    row (Bene 2 / EUR 26,100).  The decisive Bene 3 / EUR 6,480 span exists only
    in the already-cached input page, matching the production compatibility
    path without requiring report regeneration.
    """
    analysis_id = "analysis_cached_mantova_fixture"
    job_id = "cv2_cached_mantova_fixture"
    report = json.loads(_MANTOVA_REPORT_FIXTURE.read_text(encoding="utf-8"))
    report["analysis_id"] = analysis_id
    report["job_id"] = job_id
    report["customer_evidence_index"] = [
        row
        for row in report["customer_evidence_index"]
        if not (
            row.get("topic") == "Valore di stima Bene N° 3 - Garage"
            and row.get("page") == 26
        )
    ]
    _save(job_id, analysis_id, report)
    cached_pages = json.loads(_MANTOVA_PAGES_FIXTURE.read_text(encoding="utf-8"))
    artifacts.save_input_pages(job_id, cached_pages)

    async def _no_confirmations(*_args, **_kwargs):
        return []

    def _forbidden(*_args, **_kwargs):
        raise AssertionError("cached customer-view GET attempted a write or job")

    monkeypatch.setattr(api, "_confirmations_for", _no_confirmations)
    monkeypatch.setattr(api, "autostart_job", _forbidden)
    monkeypatch.setattr(api, "start_job", _forbidden)

    job_path = artifacts.job_dir(job_id)
    before = {
        path.name: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in job_path.iterdir()
        if path.is_file()
    }
    # Apply write sentinels only after arranging the isolated cached fixture.
    monkeypatch.setattr(artifacts, "save_json", _forbidden)
    monkeypatch.setattr(artifacts, "_write_json", _forbidden)

    response = _sync_get(
        customer_app,
        f"/api/analysis/perizia/{analysis_id}/correctness-v2/customer-view/latest",
    )

    assert response.status_code == 200
    data = response.json()
    assert data["available"] is True
    model = data["report"]["decision_model"]
    components = model["sections"]["numeri"]["composizione_valore"]["items"]
    assert "6.480,00" in components[2]["evidence"]["excerpt"]
    assert "26.100,00" not in components[2]["evidence"]["excerpt"]
    assert model["sections"]["verifiche"]["items"][0]["title"] == (
        "Verificare il titolo di accesso al magazzino/rustico"
    )
    assert model["sections"]["numeri"]["riconciliazione"]["difference"] == 1432
    assert all(
        row["cancellation_state"] == "to_be_cancelled"
        for row in model["sections"]["formalita"]["cancellate"]
    )
    assert "schema non prevede" not in response.text.lower()
    assert "_cached_input_pages" not in response.text
    assert "customer_evidence_index" not in data["report"]

    after = {
        path.name: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in job_path.iterdir()
        if path.is_file()
    }
    assert after == before


@pytest.mark.parametrize(
    "mismatch",
    ("report_job", "report_analysis", "status_job"),
)
def test_customer_view_fails_closed_on_cached_artifact_identity_mismatch(
    customer_app, artifacts_root, mismatch
):
    analysis_id = f"analysis_identity_{mismatch}"
    actual_job_id = f"cv2_identity_{mismatch}"
    report = {
        "schema_version": "cv2.customer_report.v1",
        "analysis_id": analysis_id,
        "job_id": actual_job_id,
        "report_status": "REPORT_READY",
        "title": "Report cache",
    }
    if mismatch == "report_job":
        report["job_id"] = "cv2_other_directory"
    elif mismatch == "report_analysis":
        report["analysis_id"] = "analysis_other_owner"
    _save(actual_job_id, analysis_id, report)
    if mismatch == "status_job":
        artifacts.save_job_status(
            actual_job_id,
            {
                "job_id": "cv2_other_directory",
                "analysis_id": analysis_id,
                "status": "REPORT_READY",
                "safe_to_show_customer": True,
                "artifacts_saved": {},
            },
        )

    response = _sync_get(
        customer_app,
        f"/api/analysis/perizia/{analysis_id}/correctness-v2/customer-view/latest",
    )

    assert response.status_code == 200
    assert response.json()["available"] is False


def test_cached_pages_are_read_from_enumerated_job_not_report_field(
    customer_app, artifacts_root, monkeypatch
):
    """A legacy report without an embedded job id stays bound to its directory."""
    analysis_id = "analysis_enumerated_cache_binding"
    actual_job_id = "cv2_enumerated_cache_binding"
    report = {
        "schema_version": "cv2.customer_report.v1",
        "analysis_id": analysis_id,
        # Legacy cache deliberately has no report['job_id'].
        "report_status": "REPORT_READY",
        "title": "Report cache legacy",
    }
    _save(actual_job_id, analysis_id, report)
    artifacts.save_input_pages(actual_job_id, [{"page": 1, "text": "testo cache"}])

    calls = []
    original_read_json = artifacts.read_json

    def _record_read(job_id, filename):
        calls.append((job_id, filename))
        return original_read_json(job_id, filename)

    monkeypatch.setattr(artifacts, "read_json", _record_read)

    response = _sync_get(
        customer_app,
        f"/api/analysis/perizia/{analysis_id}/correctness-v2/customer-view/latest",
    )

    assert response.status_code == 200
    assert response.json()["available"] is True
    assert (actual_job_id, artifacts.INPUT_PAGES_FILE) in calls
    assert all(
        job_id == actual_job_id
        for job_id, filename in calls
        if filename == artifacts.INPUT_PAGES_FILE
    )


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
    # Unrecoverable pipeline failure -> safe generic public code, never the
    # internal status.
    assert data["reason_code"] == "SERVICE_UNAVAILABLE"
    assert data["preparing"] is False
    assert "FAILED_ANALYSIS" not in response.text


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
    assert data["reason_code"] == "PREPARING"


def _count_job_dirs():
    root = artifacts.jobs_root()
    if not root.exists():
        return 0
    return sum(1 for p in root.iterdir() if p.is_dir())


def test_customer_view_missing_lot_does_not_autostart(
    customer_app, artifacts_root, monkeypatch
):
    """Opening/polling a lot with no report must NEVER create a job.

    All customer job creation goes through POST .../lots/{lot_id}/generate;
    the GET only reports state (preparing derives solely from an existing
    in-progress job).
    """
    analysis_id = "analysis_cust_lot_no_autostart"

    def _never(*args, **kwargs):  # the removed side effect must not resurface
        raise AssertionError("customer-view GET must never call autostart_job")

    monkeypatch.setattr(api, "autostart_job", _never)

    jobs_before = _count_job_dirs()
    response = _sync_get(
        customer_app,
        f"/api/analysis/perizia/{analysis_id}/correctness-v2/customer-view/latest?selected_lot_id=2",
    )
    assert response.status_code == 200
    data = response.json()
    assert data["available"] is False
    assert data["preparing"] is False  # no in-progress job exists
    assert data["reason_code"] == "NO_REPORT"
    assert _count_job_dirs() == jobs_before  # zero jobs created by the GET

    # With an existing in-progress job, preparing is True -- still no new job.
    artifacts.save_job_status(
        "cv2_no_autostart_running",
        {
            "job_id": "cv2_no_autostart_running",
            "analysis_id": analysis_id,
            "status": "RUNNING",
            "safe_to_show_customer": False,
            "artifacts_saved": {},
        },
    )
    jobs_before = _count_job_dirs()
    response = _sync_get(
        customer_app,
        f"/api/analysis/perizia/{analysis_id}/correctness-v2/customer-view/latest?selected_lot_id=2",
    )
    assert response.status_code == 200
    data = response.json()
    assert data["available"] is False
    assert data["preparing"] is True
    assert _count_job_dirs() == jobs_before


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


# ---------------------------------------------------------------------------
# Customer-safe reason-code mapping (closed public enum). No internal job
# status, OpenAI error name or validator code may ever reach a customer.
# ---------------------------------------------------------------------------

_ALLOWED_CUSTOMER_KEYS = {"available", "selected_lot_id", "preparing", "reason_code", "report"}

# Internal raw codes that must NEVER appear in a customer response body.
_INTERNAL_CODES = (
    "OPENAI_QUOTA_EXHAUSTED",
    "OPENAI_RATE_LIMITED",
    "OPENAI_TIMEOUT",
    "OPENAI_SERVER_ERROR",
    "OPENAI_CALL_FAILED",
    "CONTRACT_VALIDATION_FAILED",
    "NEEDS_MANUAL_REVIEW",
    "FAILED_ANALYSIS",
    "FAILED_CONTRACT_BUILD",
    "FAILED_GROUNDING",
    "JOB_STALLED",
    "REPORT_QUALITY_GATE_FAILED",
    "NO_CUSTOMER_REPORT",
)


def _save_terminal_job(job_id, analysis_id, status, reason_code=None):
    artifacts.save_job_status(
        job_id,
        {
            "job_id": job_id,
            "analysis_id": analysis_id,
            "status": status,
            "reason_code": reason_code,
            "safe_to_show_customer": False,
            "artifacts_saved": {},
        },
    )


def _assert_customer_payload_is_safe(response):
    data = response.json()
    assert set(data.keys()) <= _ALLOWED_CUSTOMER_KEYS
    assert data["reason_code"] in api.PUBLIC_REASON_CODES
    for raw in _INTERNAL_CODES:
        assert raw not in response.text
    return data


@pytest.mark.parametrize(
    "status,reason_code,expected",
    [
        ("QUEUED", None, "PREPARING"),
        ("RUNNING", None, "PREPARING"),
        # Terminal step-1-only artifact: no report exists and none is being
        # prepared (PDF_QUALITY_* is never persisted by the customer pipeline).
        ("PDF_QUALITY_OK", None, "NO_REPORT"),
        ("PDF_QUALITY_WARNING", None, "NO_REPORT"),
        ("FAILED_ANALYSIS", "OPENAI_QUOTA_EXHAUSTED", "SERVICE_BUSY"),
        ("FAILED_ANALYSIS", "OPENAI_RATE_LIMITED", "SERVICE_BUSY"),
        ("FAILED_ANALYSIS", "OPENAI_TIMEOUT", "SERVICE_BUSY"),
        ("FAILED_ANALYSIS", "OPENAI_SERVER_ERROR", "SERVICE_BUSY"),
        ("CONTRACT_VALIDATION_FAILED", None, "VERIFICATION_REQUIRED"),
        ("NEEDS_MANUAL_REVIEW", "REPORT_QUALITY_GATE_FAILED", "VERIFICATION_REQUIRED"),
        ("FAILED_GROUNDING", None, "VERIFICATION_REQUIRED"),
        ("FAILED_ANALYSIS", "OPENAI_CALL_FAILED", "SERVICE_UNAVAILABLE"),
        ("FAILED_CONTRACT_BUILD", None, "SERVICE_UNAVAILABLE"),
        ("JOB_STALLED", None, "SERVICE_UNAVAILABLE"),
        ("CANCELLED", None, "SERVICE_UNAVAILABLE"),
        ("SOME_FUTURE_STATUS", "SOME_FUTURE_REASON", "SERVICE_UNAVAILABLE"),
    ],
)
def test_customer_view_maps_every_job_state_to_the_closed_enum(
    customer_app, artifacts_root, status, reason_code, expected
):
    analysis_id = f"analysis_enum_{status.lower()}_{str(reason_code).lower()}"
    _save_terminal_job(f"cv2_enum_{analysis_id}", analysis_id, status, reason_code)

    response = _sync_get(
        customer_app,
        f"/api/analysis/perizia/{analysis_id}/correctness-v2/customer-view/latest",
    )

    assert response.status_code == 200
    data = _assert_customer_payload_is_safe(response)
    assert data["available"] is False
    assert data["reason_code"] == expected


def test_customer_view_no_job_yields_no_report(customer_app, artifacts_root):
    response = _sync_get(
        customer_app,
        "/api/analysis/perizia/analysis_without_any_job/correctness-v2/customer-view/latest",
    )
    assert response.status_code == 200
    data = _assert_customer_payload_is_safe(response)
    assert data["available"] is False
    assert data["preparing"] is False
    assert data["reason_code"] == "NO_REPORT"


def test_customer_view_available_payload_has_no_extra_keys(customer_app, artifacts_root):
    analysis_id = "analysis_keys_ready"
    _save(
        "cv2_keys_ready",
        analysis_id,
        {
            "schema_version": "cv2.customer_report.v1",
            "analysis_id": analysis_id,
            "job_id": "cv2_keys_ready",
            "report_status": "REPORT_READY",
            "title": "Report cliente",
        },
    )
    response = _sync_get(
        customer_app,
        f"/api/analysis/perizia/{analysis_id}/correctness-v2/customer-view/latest",
    )
    assert response.status_code == 200
    data = response.json()
    assert set(data.keys()) <= _ALLOWED_CUSTOMER_KEYS
    assert data["available"] is True
