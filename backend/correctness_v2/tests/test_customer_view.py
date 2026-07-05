"""Customer-safe projection + route tests for Correctness Mode v2.

Covers:
  * sanitize_customer_report strips every admin/debug field and keeps only
    presentable customer content plus a derived decision box.
  * is_customer_safe fails closed for non-safe statuses.
  * derive_decision picks the right level from risks/occupancy/compliance.
  * the /customer-view route returns sanitized data (no admin keys) while the
    admin /customer-report route keeps the full artifact.
  * selected_lot_id keeps a selected lot's report lot-specific.
"""

import asyncio

import httpx
import pytest
from fastapi import FastAPI

from correctness_v2 import api, artifacts, customer_view, feature_flags


# ---------------------------------------------------------------------------
# A representative full customer_report.json (mirrors the persisted contract).
# ---------------------------------------------------------------------------
def _full_report(**overrides):
    report = {
        "schema_version": "cv2.customer_report.v1",
        "analysis_id": "analysis_cv",
        "job_id": "cv2_view",
        "report_status": "REPORT_READY",
        "title": "Appartamento – Via Esempio 1",
        "subtitle": "Tribunale di Test · 1/2025 · Lotto 1",
        "case_identity": {"tribunale": "Tribunale di Test", "property_type": "Appartamento"},
        "lot_structure": {
            "multi_lot": False,
            "lot_count": 1,
            "selected_lot": "1",
            "bene_count": 2,
            "multi_bene": True,
            "bene_ids": ["1", "2"],
        },
        "executive_summary": [{"text": "Immobile oggetto della perizia.", "evidence_pages": [1]}],
        "key_facts": [{"label": "Valore di vendita giudiziaria", "value": 38110.2, "evidence_pages": [4]}],
        "risk_sections": [
            {
                "section_id": "rischi_gestibili",
                "title": "Difformità gestibili",
                "items": [{"area": "edilizia", "severity": "media", "summary": "Regolarizzabile.", "evidence_pages": [8]}],
            }
        ],
        "money_sections": {
            "valuation_chain": [{"label": "Valore di mercato", "amount": 43654.2, "evidence_pages": [4]}],
            "auction_terms": [],
            "buyer_side_costs": [{"label": "Costi di regolarizzazione", "amount": 5250, "evidence_pages": [8]}],
            "procedure_cancelled_formalities": [],
            "market_comparatives": [{"label": "OMI medio", "amount": 900, "evidence_pages": [16]}],
            "context_values": [{"label": "Rendita catastale", "amount": 472.56, "evidence_pages": [2]}],
            "uncertain_money": [{"label": "Importo incerto", "amount": 294, "evidence_pages": [19]}],
        },
        "beni_sections": [{"bene_id": "1", "title": "Bene 1"}, {"bene_id": "2", "title": "Bene 2"}],
        "occupancy_section": {"status": "occupato", "status_label": "Occupato", "evidence_pages": [3]},
        "compliance_section": [{"area": "urbanistica", "classification": "conforming", "evidence_pages": [8]}],
        "formalities_section": [{"type": "ipoteca", "amount": 150000, "cancelled_by_procedure": True, "evidence_pages": [5]}],
        "surfaces_section": [{"label": "Superficie", "value": "46,95", "evidence_pages": [2]}],
        "buyer_checklist": [{"action": "Verificare", "detail": "Confermare spese", "evidence_pages": [9]}],
        "manual_review_flags": [
            {"kind": "validator_warning", "detail": "ZERO_AMOUNT_BUYER_COST", "debug_detail": "raw english"},
            {"kind": "uncertain_money", "detail": "Importo da verificare", "evidence_pages": [19]},
        ],
        "evidence_index": [{"page": 4, "referenced_by": ["valuation_chain[0]"]}],
        "customer_evidence_index": [{"page": 8, "topic": "Conformità urbanistica", "perizia_excerpt": "conforme"}],
        "admin_evidence_index": [{"page": 8, "raw_keys": ["technical_compliance[2]"], "artifact_source": "verified_report_contract.json"}],
        "quality_control": {"coverage_status": "PASS", "satisfaction_score": 96, "rows": [{"pagina": 2}]},
        "sections_meta": {"validation_status": "VALIDATED", "source_contract_schema": "x"},
        "disclaimer": "Disclaimer.",
        "_saved_at": "2026-07-05T00:00:00Z",
    }
    report.update(overrides)
    return report


_ADMIN_KEYS = [
    "quality_control",
    "admin_evidence_index",
    "evidence_index",
    "sections_meta",
    "surfaces_section",
    "manual_review_flags",
    "_saved_at",
]


def test_sanitize_strips_all_admin_and_debug_fields():
    out = customer_view.sanitize_customer_report(_full_report())
    for key in _ADMIN_KEYS:
        assert key not in out, f"admin/debug key leaked: {key}"
    # Money background buckets are gone; buyer-facing buckets remain.
    assert "market_comparatives" not in out["money_sections"]
    assert "context_values" not in out["money_sections"]
    assert set(out["money_sections"]) == set(customer_view._CUSTOMER_MONEY_KEYS)
    # Machine lot flags removed.
    assert "multi_lot" not in out["lot_structure"]
    assert "bene_ids" not in out["lot_structure"]
    assert "multi_bene" not in out["lot_structure"]


def test_sanitize_keeps_customer_content_and_adds_decision():
    out = customer_view.sanitize_customer_report(_full_report())
    assert out["report_status"] == "REPORT_READY"
    assert out["report_status_label"] == "Report pronto"
    assert out["title"].startswith("Appartamento")
    assert out["occupancy_section"]["status_label"] == "Occupato"
    assert out["compliance_section"][0]["area"] == "urbanistica"
    assert out["formalities_section"][0]["type"] == "ipoteca"
    assert out["buyer_checklist"][0]["action"] == "Verificare"
    assert out["customer_evidence_index"][0]["topic"] == "Conformità urbanistica"
    decision = out["decision"]
    assert decision["level"] in customer_view._DECISION_LABELS
    assert decision["label"]
    assert decision["reason"]


def test_sanitize_does_not_mutate_input():
    report = _full_report()
    customer_view.sanitize_customer_report(report)
    assert "quality_control" in report  # original untouched
    assert "market_comparatives" in report["money_sections"]


def test_decision_attenzione_for_critical_risk():
    report = _full_report(
        risk_sections=[
            {
                "section_id": "criticita",
                "title": "Criticità",
                "items": [{"area": "struttura", "severity": "grave", "summary": "edificio collabente, rischio crollo", "evidence_pages": [5]}],
            }
        ]
    )
    decision = customer_view.derive_decision(report)
    assert decision["level"] == customer_view._DECISION_ATTENZIONE
    assert "strutturali" in decision["reason"]


def test_decision_da_verificare_when_occupied_no_critical():
    report = _full_report(risk_sections=[], compliance_section=[])
    decision = customer_view.derive_decision(report)
    assert decision["level"] == customer_view._DECISION_DA_VERIFICARE
    assert "occupato" in decision["reason"]


def test_decision_pronto_when_clean():
    report = _full_report(
        risk_sections=[],
        compliance_section=[],
        occupancy_section={"status": "libero", "status_label": "Libero"},
        money_sections={
            "valuation_chain": [{"label": "Valore di mercato", "amount": 100000, "evidence_pages": [4]}],
            "auction_terms": [],
            "buyer_side_costs": [],
            "procedure_cancelled_formalities": [],
            "market_comparatives": [],
            "context_values": [],
            "uncertain_money": [],
        },
    )
    decision = customer_view.derive_decision(report)
    assert decision["level"] == customer_view._DECISION_PRONTO


def test_is_customer_safe_fails_closed():
    assert customer_view.is_customer_safe(_full_report()) is True
    assert customer_view.is_customer_safe(_full_report(report_status="NEEDS_MANUAL_REVIEW")) is False
    assert customer_view.is_customer_safe(_full_report(report_status="CONTRACT_VALIDATION_FAILED")) is False
    # Explicit pipeline "not safe" flag overrides a READY status.
    assert customer_view.is_customer_safe(_full_report(), {"safe_to_show_customer": False}) is False
    assert customer_view.is_customer_safe(None) is False


# ---------------------------------------------------------------------------
# Route tests
# ---------------------------------------------------------------------------
@pytest.fixture()
def customer_app(monkeypatch):
    async def _allow_customer(request, analysis_id):
        return {"user_id": "u1"}, False  # authenticated, non-admin owner

    async def _allow_admin(request):
        return {"user_id": "admin"}, True

    monkeypatch.setattr(api, "_resolve_customer_access", _allow_customer)
    monkeypatch.setattr(api, "_resolve_user_and_guard", _allow_admin)
    monkeypatch.setenv(feature_flags.FLAG_ENABLED, "true")
    app = FastAPI()
    app.include_router(api.router, prefix="/api")
    return app


def _get(app, path):
    async def _run():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get(path)

    return asyncio.run(_run())


def _seed(job_id, analysis_id, report, *, safe=True, updated="2026-01-01"):
    artifacts.save_job_status(
        job_id,
        {
            "job_id": job_id,
            "analysis_id": analysis_id,
            "status": report.get("report_status", "REPORT_READY"),
            "safe_to_show_customer": safe,
            "updated_at": updated,
            "artifacts_saved": {"customer_report": str(artifacts.job_dir(job_id) / artifacts.CUSTOMER_REPORT_FILE)},
        },
    )
    artifacts.save_customer_report(job_id, report)


def test_customer_view_route_returns_sanitized_report(customer_app, artifacts_root):
    _seed("cv2_view_ok", "analysis_cust", _full_report(analysis_id="analysis_cust", job_id="cv2_view_ok"))

    resp = _get(customer_app, "/api/analysis/perizia/analysis_cust/correctness-v2/customer-view/latest")

    assert resp.status_code == 200
    body = resp.json()
    assert body["available"] is True
    report = body["report"]
    assert report["decision"]["label"]
    for key in _ADMIN_KEYS:
        assert key not in report
    # Raw admin substrings must not appear anywhere in the response text.
    assert "technical_compliance[" not in resp.text
    assert "quality_control" not in resp.text
    assert "verified_report_contract.json" not in resp.text


def test_customer_view_route_hides_unsafe_report(customer_app, artifacts_root):
    _seed(
        "cv2_view_unsafe",
        "analysis_unsafe",
        _full_report(analysis_id="analysis_unsafe", job_id="cv2_view_unsafe", report_status="NEEDS_MANUAL_REVIEW"),
        safe=False,
    )

    resp = _get(customer_app, "/api/analysis/perizia/analysis_unsafe/correctness-v2/customer-view/latest")

    assert resp.status_code == 200
    assert resp.json()["available"] is False


def test_customer_view_selected_lot_stays_lot_specific(customer_app, artifacts_root):
    _seed(
        "cv2_lot1",
        "analysis_multi",
        _full_report(analysis_id="analysis_multi", job_id="cv2_lot1", title="Lotto 1", lot_structure={"selected_lot": "1", "bene_count": 1}),
        updated="2026-01-01",
    )
    _seed(
        "cv2_lot2",
        "analysis_multi",
        _full_report(analysis_id="analysis_multi", job_id="cv2_lot2", title="Lotto 2", lot_structure={"selected_lot": "2", "bene_count": 1}),
        updated="2026-02-01",
    )

    resp1 = _get(customer_app, "/api/analysis/perizia/analysis_multi/correctness-v2/customer-view/latest?selected_lot_id=1")
    resp2 = _get(customer_app, "/api/analysis/perizia/analysis_multi/correctness-v2/customer-view/latest?selected_lot_id=2")

    assert resp1.json()["report"]["title"] == "Lotto 1"
    assert resp1.json()["report"]["lot_structure"]["selected_lot"] == "1"
    assert resp2.json()["report"]["title"] == "Lotto 2"


def test_admin_customer_report_route_keeps_full_artifact(customer_app, artifacts_root):
    """The admin route still exposes the full (unsanitized) artifact."""
    _seed("cv2_admin_full", "analysis_admin", _full_report(analysis_id="analysis_admin", job_id="cv2_admin_full"))

    resp = _get(
        customer_app,
        "/api/analysis/perizia/analysis_admin/correctness-v2/jobs/cv2_admin_full/customer-report",
    )

    assert resp.status_code == 200
    data = resp.json()
    assert "quality_control" in data
    assert "admin_evidence_index" in data
