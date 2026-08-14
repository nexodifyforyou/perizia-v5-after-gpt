"""Sanitized external-beta blocked-lot golden acceptance (offline only)."""

from __future__ import annotations

import copy
import json

from correctness_v2 import (
    customer_view, feature_flags, lots, partial_report, quality_gate, verdict_model,
)

from .beta_fixture import build_lot


def test_beta_blocked_lot_golden_partial_acceptance(monkeypatch):
    monkeypatch.setenv(feature_flags.FLAG_CANONICAL_VERDICT, "true")
    monkeypatch.setenv(feature_flags.FLAG_PARTIAL_LOT_REPORTS, "true")
    built = build_lot("4")
    sub_lot_report = lots.build_lot_report(built["worksheet"], built["selected"])
    gate = quality_gate.run_quality_gate(
        job_id="fixture_job_lot_4",
        analysis_id="fixture_beta_multilot",
        pages=built["verification"],
        worksheet=built["worksheet"],
        contract=built["contract"],
        customer_report=built["customer_report"],
        validator_report=built["validator"],
        lot_report=sub_lot_report,
        persist=False,
        full_document_pages=built["pages"],
        selected_analysis_pages=[p["page_number"] for p in built["selected"]],
        lot_id="4",
        segmentation=built["segmentation"],
        case_ledger=built["ledger"],
        lot_fact_projection_report=built["projection"],
    )
    verdict_input = {
        **built["customer_report"],
        "lot_fact_projection": built["projection"],
    }
    canonical = verdict_model.build_lot_verdict(
        verdict_input,
        reconciled_worksheet=built["worksheet"],
        case_ledger=built["ledger"],
        segmentation=built["segmentation"],
        lot_report=sub_lot_report,
        lot_id="4",
    )
    canonical_before = copy.deepcopy(canonical)
    projected = partial_report.build_partial_projection(
        report=gate["customer_report"],
        canonical_verdict=canonical,
        quality_report=gate["quality_report"],
        coverage_audit=gate["coverage_audit"],
        gate_status=gate["gate_status"],
        lot_id="4",
    )
    assert gate["gate_status"] == "FAIL"                                      # 1 before blocked
    assert projected is not None                                                # 2 eligible
    report = projected["report"]
    customer = customer_view.sanitize_customer_report(
        report, {"safe_to_show_customer": True}
    )
    original_customer = customer_view.sanitize_customer_report(
        built["customer_report"], {"safe_to_show_customer": True}
    )
    unresolved = customer["partial_status"]["unresolved_fields"]

    assert report["report_status"] == "PARTIAL_REPORT_AVAILABLE"               # 3 disclosure transition
    assert customer["case_identity"]["address"] == "Via del Tiglio 2, Borgo Esempio"  # 4 identity
    assert customer["case_identity"]["property_type"] == "Locale accessorio"  # 5 typology
    assert customer["occupancy_section"] == original_customer["occupancy_section"]  # 6 occupancy preserved
    assert customer["compliance_section"] == original_customer["compliance_section"]  # 7 compliance
    assert customer["formalities_section"] == []                               # 8 verified empty formalities
    assert customer["money_sections"]["valuation_chain"] == original_customer["money_sections"]["valuation_chain"]  # 9 reliable money
    assert len(unresolved) == 1 and unresolved[0]["field_label"] == "Dato economico"  # 10 explicit unresolved money
    assert not any(key in unresolved[0] for key in ("amount", "amount_display"))  # 11 no substitute
    assert unresolved[0]["source_pages"] == [3] and unresolved[0]["evidence_available"] is True  # 12 evidence
    assert customer["partial_status"]["full_readiness"] is False               # 13 not ready
    assert customer["partial_status"]["professional_verification_required"] is True  # 14 professional verification
    assert verdict_model.SEVERITY_ORDER[projected["canonical_verdict"]["severity"]] >= verdict_model.SEVERITY_ORDER[canonical_before["severity"]]  # 15 no downgrade
    case = verdict_model.build_case_verdict(
        [projected["canonical_verdict"]], scope_id="fixture_beta_multilot",
        all_lot_ids=["1", "2", "3", "4"],
    )
    assert case["readiness"] != "READY_FOR_REVIEW" and case["severity"] == "grave"  # 16 case not improved
    body = json.dumps(customer, ensure_ascii=False).lower()
    assert "lotto 1" not in body and "lotto 2" not in body and "lotto 3" not in body  # 17 no leak
    for raw in ("MISSING_IMPORTANT_MONEY", "TECHNICAL_REVIEW_REQUIRED", "base_price"):
        assert raw not in json.dumps(customer["partial_status"], ensure_ascii=False)  # 18 display boundary
    assert canonical == canonical_before                                          # 19 classifier/projection input immutable
