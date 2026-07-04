"""
Tests for the no-silent-omissions coverage gate + quality certificate.

All fixtures are generic synthetic perizie (Tribunale di Esempio): nothing here
is overfit to a specific real document.
"""

from __future__ import annotations

import copy

from correctness_v2 import (
    contract as contract_mod,
    coverage_audit as coverage_mod,
    customer_report as customer_report_mod,
    doc_signals,
    lots as lots_mod,
    quality_gate,
    quality_report as quality_mod,
    validator as validator_mod,
)
from correctness_v2.tests.sample_perizia import GENERIC_PERIZIA_PAGES, make_worksheet


def _normalized(ws_raw):
    from correctness_v2 import analyst as analyst_mod

    return analyst_mod.normalize_worksheet(ws_raw)


def _build_all(pages, ws_raw, **contract_kwargs):
    worksheet = _normalized(ws_raw)
    worksheet, _gate = validator_mod.apply_compliance_evidence_gate(worksheet, pages)
    validator_report = validator_mod.validate_worksheet(worksheet, pages)
    assert validator_report["validation_status"] == validator_mod.STATUS_VALIDATED
    lot_report = lots_mod.build_lot_report(worksheet, pages)
    contract = contract_mod.build_contract(
        worksheet=worksheet,
        validator_report=validator_report,
        analysis_id="an_q",
        job_id="job_q",
        source_pdf_quality_status="PDF_QUALITY_OK",
        lot_report=lot_report,
        surface_cadastral=doc_signals.extract_surface_cadastral(pages),
        **contract_kwargs,
    )
    report = customer_report_mod.render_success_report(contract)
    return worksheet, validator_report, lot_report, contract, report


def _gate(pages, worksheet, contract, report, validator_report=None, lot_report=None):
    return quality_gate.run_quality_gate(
        job_id="job_q",
        analysis_id="an_q",
        pages=pages,
        worksheet=worksheet,
        contract=contract,
        customer_report=report,
        validator_report=validator_report,
        lot_report=lot_report,
        persist=False,
    )


# ---------------------------------------------------------------------------
# Happy path: complete report passes and produces all artifacts
# ---------------------------------------------------------------------------
def test_quality_gate_passes_on_complete_report():
    ws, vr, lr, contract, report = _build_all(GENERIC_PERIZIA_PAGES, make_worksheet())
    gate = _gate(GENERIC_PERIZIA_PAGES, ws, contract, report, vr, lr)
    assert gate["gate_status"] in ("PASS", "WARNING")
    assert gate["quality_report"]["overall_quality_status"] in ("PASS", "PASS_WITH_WARNINGS")
    assert not gate["quality_report"]["blocking_issues"]
    assert gate["coverage_audit"]["coverage_status"] in ("PASS", "WARNING")
    assert not gate["coverage_audit"]["critical_omissions"]
    # All four artifact payloads exist with their schema versions.
    assert gate["coverage_audit"]["schema_version"] == "cv2.coverage_audit.v1"
    assert gate["page_audit"]["schema_version"] == "cv2.page_by_page_audit.v1"
    assert gate["quality_report"]["schema_version"] == "cv2.quality_standard_report.v1"
    assert gate["scorecard"]["schema_version"] == "cv2.customer_satisfaction_scorecard.v1"


def test_page_by_page_table_created_and_attached():
    ws, vr, lr, contract, report = _build_all(GENERIC_PERIZIA_PAGES, make_worksheet())
    gate = _gate(GENERIC_PERIZIA_PAGES, ws, contract, report, vr, lr)
    page_audit = gate["page_audit"]
    assert page_audit["rows"], "page audit must extract per-page facts"
    pages_seen = {row["page"] for row in page_audit["rows"]}
    assert pages_seen.issubset({1, 2})
    for row in page_audit["rows"]:
        assert row["esito"] in ("Coperto", "Parziale", "Mancante", "Da verificare", "Non materiale")
    # The customer report carries the customer-facing table.
    qc = gate["customer_report"]["quality_control"]
    assert qc["title"] == "Controllo qualità pagina per pagina"
    assert qc["rows"]
    assert qc["columns"][0] == "Pagina"


# ---------------------------------------------------------------------------
# Detection tests: each critical failure mode must be caught
# ---------------------------------------------------------------------------
def test_detects_missing_important_money():
    ws, vr, lr, contract, report = _build_all(GENERIC_PERIZIA_PAGES, make_worksheet())
    # Simulate a renderer bug: judicial sale value silently dropped everywhere.
    broken = copy.deepcopy(report)
    for section in broken["money_sections"].values():
        section[:] = [r for r in section if r.get("amount") != 94700.0]
    broken["key_facts"] = [f for f in broken["key_facts"] if f.get("value") != 94700.0]
    broken["executive_summary"] = [
        s for s in broken["executive_summary"] if "94.700" not in str(s.get("text"))
    ]
    gate = _gate(GENERIC_PERIZIA_PAGES, ws, contract, broken, vr, lr)
    assert gate["gate_status"] == "FAIL"
    codes = {b["code"] for b in gate["quality_report"]["blocking_issues"]}
    assert "MISSING_IMPORTANT_MONEY" in codes
    assert gate["quality_report"]["final_decision"] == "REJECT"
    assert gate["scorecard"]["status"] == "NOT_READY"


def test_detects_missing_bene():
    ws, vr, lr, contract, report = _build_all(GENERIC_PERIZIA_PAGES, make_worksheet())
    lr = dict(lr)
    lr["bene_ids"] = ["1", "2"]
    lr["multi_bene"] = True
    lr["bene_count"] = 2
    # Contract silently lost bene 2.
    broken_contract = copy.deepcopy(contract)
    broken_contract["lot_summary"]["bene_ids"] = ["1"]
    gate = _gate(GENERIC_PERIZIA_PAGES, ws, broken_contract, report, vr, lr)
    codes = {b["code"] for b in gate["quality_report"]["blocking_issues"]}
    assert "BENE_LOST" in codes
    assert gate["gate_status"] == "FAIL"


def test_detects_buyer_vs_procedure_confusion():
    ws, vr, lr, contract, report = _build_all(GENERIC_PERIZIA_PAGES, make_worksheet())
    broken_contract = copy.deepcopy(contract)
    broken_contract["legal_formalities"][0]["amount"] = 300.0  # cancelled formality
    broken_report = copy.deepcopy(report)
    broken_report["money_sections"]["buyer_side_costs"].append(
        {"label": "Ipoteca a carico acquirente", "amount": 300.0,
         "amount_display": "€ 300,00", "kind": "buyer_side", "evidence_pages": [2]}
    )
    gate = _gate(GENERIC_PERIZIA_PAGES, ws, broken_contract, broken_report, vr, lr)
    codes = {b["code"] for b in gate["quality_report"]["blocking_issues"]}
    assert "PROCEDURE_FORMALITY_AS_BUYER_DEBT" in codes


def test_detects_invented_buyer_cost():
    ws, vr, lr, contract, report = _build_all(GENERIC_PERIZIA_PAGES, make_worksheet())
    broken = copy.deepcopy(report)
    broken["money_sections"]["buyer_side_costs"].append(
        {"label": "Costo inventato", "amount": 123456.78,
         "amount_display": "€ 123.456,78", "kind": "buyer_side", "evidence_pages": []}
    )
    gate = _gate(GENERIC_PERIZIA_PAGES, ws, contract, broken, vr, lr)
    codes = {b["code"] for b in gate["quality_report"]["blocking_issues"]}
    assert "INVENTED_BUYER_COST" in codes


def test_detects_fake_prezzo_base():
    ws, vr, lr, contract, report = _build_all(GENERIC_PERIZIA_PAGES, make_worksheet())
    # Force the validator signal to say there is NO explicit base-price text,
    # while an auction row still claims a prezzo base.
    vr_fake = copy.deepcopy(vr)
    vr_fake.setdefault("checks", {})["money_signals"] = {
        "base_price_candidate": 94700.0,
        "base_price_explicit_text": False,
    }
    broken = copy.deepcopy(report)
    broken["money_sections"]["auction_terms"] = [
        {"label": "Prezzo base d'asta", "amount": 94700.0,
         "amount_display": "€ 94.700,00", "kind": "auction_term", "evidence_pages": [2]}
    ]
    gate = _gate(GENERIC_PERIZIA_PAGES, ws, contract, broken, vr_fake, lr)
    codes = {b["code"] for b in gate["quality_report"]["blocking_issues"]}
    assert "FAKE_PREZZO_BASE" in codes


def test_detects_section_contradiction():
    ws, vr, lr, contract, report = _build_all(GENERIC_PERIZIA_PAGES, make_worksheet())
    broken = copy.deepcopy(report)
    broken["key_facts"].append(
        {"label": "Valore di mercato", "value": 88888.0, "evidence_pages": [2]}
    )
    gate = _gate(GENERIC_PERIZIA_PAGES, ws, contract, broken, vr, lr)
    codes = {b["code"] for b in gate["quality_report"]["blocking_issues"]}
    assert "SECTION_CONTRADICTION" in codes


def test_accepts_facts_rendered_as_uncertainty():
    ws_raw = make_worksheet()
    ws_raw["money"]["uncertain_money"] = [
        {
            "label": "Canone di locazione dichiarato",
            "amount": 300.0,
            "reason": "Periodicità non specificata nel documento.",
            "evidence_pages": [2],
        }
    ]
    ws, vr, lr, contract, report = _build_all(GENERIC_PERIZIA_PAGES, ws_raw)
    gate = _gate(GENERIC_PERIZIA_PAGES, ws, contract, report, vr, lr)
    assert gate["gate_status"] != "FAIL"
    facts = {f["fact_id"]: f for f in gate["coverage_audit"]["fact_coverage"]}
    unc = facts.get("money.uncertain_money[0]")
    assert unc is not None
    # Rendered somewhere (uncertainty or matching row); never a critical omission.
    assert unc["match_status"] in ("match", "partial")
    assert not any(
        o["fact_id"] == "money.uncertain_money[0]"
        for o in gate["coverage_audit"]["critical_omissions"]
    )


def test_surface_and_rendita_covered_when_extracted():
    pages = copy.deepcopy(GENERIC_PERIZIA_PAGES)
    pages[0]["text"] += (
        " Dati catastali: foglio 12, particella 345, subalterno 6, categoria A/3, "
        "classe 2, rendita catastale EUR 500,00. Superficie commerciale 80,00 mq."
    )
    ws, vr, lr, contract, report = _build_all(pages, make_worksheet())
    # The deterministic extraction must reach the customer report.
    labels = {s["label"] for s in report["surfaces_section"]}
    assert "Rendita catastale" in labels
    assert "Superficie commerciale" in labels
    gate = _gate(pages, ws, contract, report, vr, lr)
    assert gate["gate_status"] != "FAIL"
    # And the coverage audit must see the surface/rendita topics as covered.
    for fact in gate["coverage_audit"]["fact_coverage"]:
        if fact.get("category") in ("surface", "cadastral") and fact["severity"] == "important":
            assert fact["match_status"] in ("match", "partial"), fact


def test_surface_missing_from_report_is_flagged():
    pages = copy.deepcopy(GENERIC_PERIZIA_PAGES)
    pages[0]["text"] += " Superficie commerciale 80,00 mq. Rendita catastale EUR 500,00."
    ws, vr, lr, contract, report = _build_all(pages, make_worksheet())
    broken = copy.deepcopy(report)
    broken["surfaces_section"] = []  # renderer silently dropped surfaces
    audit, _page = coverage_mod.build_coverage_audit(
        analysis_id="an_q", job_id="job_q", pages=pages, worksheet=ws,
        contract=contract, customer_report=broken, validator_report=vr, lot_report=lr,
    )
    flagged = [
        f for f in audit["fact_coverage"]
        if f.get("category") in ("surface", "cadastral") and f["match_status"] == "missing"
    ]
    assert flagged, "dropped surface/rendita facts must be flagged"


def test_quality_gate_blocks_critical_omission_end_to_end():
    """Quality score must FAIL on a critical omission (occupancy dropped)."""
    ws, vr, lr, contract, report = _build_all(GENERIC_PERIZIA_PAGES, make_worksheet())
    broken = copy.deepcopy(report)
    broken["occupancy_section"] = {}
    broken["key_facts"] = [f for f in broken["key_facts"] if f.get("label") != "Stato occupazione"]
    broken["executive_summary"] = [
        s for s in broken["executive_summary"] if "occupazione" not in str(s.get("text")).lower()
    ]
    gate = _gate(GENERIC_PERIZIA_PAGES, ws, contract, broken, vr, lr)
    assert gate["gate_status"] == "FAIL"
    assert gate["quality_report"]["overall_quality_status"] == "FAIL"
    assert gate["scorecard"]["status"] == "NOT_READY"


def test_missed_noncritical_topic_becomes_visible_manual_review():
    pages = copy.deepcopy(GENERIC_PERIZIA_PAGES)
    pages[1]["text"] += " L'attestazione di agibilità non risulta reperita agli atti."
    ws, vr, lr, contract, report = _build_all(pages, make_worksheet())
    gate = _gate(pages, ws, contract, report, vr, lr)
    assert gate["gate_status"] != "FAIL"
    flags = gate["customer_report"]["manual_review_flags"]
    assert any(f.get("kind") == "coverage_topic" and "pag. 2" in str(f.get("detail"))
               for f in flags), flags


def test_selection_report_audit_detects_lost_lot():
    selection = {
        "schema_version": "cv2.lot_selection_required.v1",
        "analysis_id": "an_q", "job_id": "job_q",
        "status": "LOT_SELECTION_REQUIRED", "multi_lot": True,
        "lot_count": 2, "lot_ids": ["1", "2"],
        "available_lots": [
            {"lot_id": "1", "label": "Bene N° 1", "page_evidence": [1]},
            # lot 2 lost by the selector
        ],
        "available_actions": [],
    }
    lot_index = {"lots": [{"lot_id": "1", "money": {}}, {"lot_id": "2", "money": {}}]}
    lot_report = {"multi_lot": True, "lot_ids": ["1", "2"], "lot_count": 2}
    report = customer_report_mod.render_lot_selection_report(selection, lot_index)
    # Simulate the loss also in lot_structure (the selector's own list).
    report["lot_structure"]["lot_ids"] = ["1"]
    gate = quality_gate.run_quality_gate(
        job_id="job_q", analysis_id="an_q", pages=GENERIC_PERIZIA_PAGES,
        worksheet=None, contract=None, customer_report=report,
        lot_report=lot_report, lot_index=lot_index, persist=False,
    )
    assert gate["gate_status"] == "FAIL"
    assert any(
        o["fact_id"] == "selection.lot[2]"
        for o in gate["coverage_audit"]["critical_omissions"]
    )


def test_selection_report_per_lot_money_preserved():
    selection = {
        "schema_version": "cv2.lot_selection_required.v1",
        "analysis_id": "an_q", "job_id": "job_q",
        "status": "LOT_SELECTION_REQUIRED", "multi_lot": True,
        "lot_count": 2, "lot_ids": ["1", "2"],
        "available_lots": [
            {"lot_id": "1", "label": "Lotto 1", "page_evidence": [1]},
            {"lot_id": "2", "label": "Lotto 2", "page_evidence": [2]},
        ],
        "available_actions": [{"action": "analyze_selected_lot"}],
    }
    lot_index = {
        "lots": [
            {"lot_id": "1", "money": {"prezzo_base_asta": {"amount": 64198.0, "evidence_pages": [1]}}},
            {"lot_id": "2", "money": {"prezzo_base_asta": {"amount": 30000.0, "evidence_pages": [2]}}},
        ]
    }
    lot_report = {"multi_lot": True, "lot_ids": ["1", "2"], "lot_count": 2}
    report = customer_report_mod.render_lot_selection_report(selection, lot_index)
    gate = quality_gate.run_quality_gate(
        job_id="job_q", analysis_id="an_q", pages=[],
        worksheet=None, contract=None, customer_report=report,
        lot_report=lot_report, lot_index=lot_index, persist=False,
    )
    assert gate["gate_status"] != "FAIL"
    facts = {f["fact_id"]: f for f in gate["coverage_audit"]["fact_coverage"]}
    assert facts["selection.lot[1].prezzo_base_asta"]["match_status"] == "match"
    assert facts["selection.lot[2].prezzo_base_asta"]["match_status"] == "match"

    # Now drop lot 2's money from the selector -> critical omission.
    broken = copy.deepcopy(report)
    for lot in broken["lot_selection"]["lots"]:
        if lot["lot_id"] == "2":
            lot["money_summary"] = []
    gate2 = quality_gate.run_quality_gate(
        job_id="job_q", analysis_id="an_q", pages=[],
        worksheet=None, contract=None, customer_report=broken,
        lot_report=lot_report, lot_index=lot_index, persist=False,
    )
    assert gate2["gate_status"] == "FAIL"


# ---------------------------------------------------------------------------
# Customer wording / scorecard
# ---------------------------------------------------------------------------
def test_no_raw_internal_labels_in_customer_report():
    ws, vr, lr, contract, report = _build_all(GENERIC_PERIZIA_PAGES, make_worksheet())
    gate = _gate(GENERIC_PERIZIA_PAGES, ws, contract, report, vr, lr)
    raw_warns = [
        w for w in gate["quality_report"]["warnings"]
        if w["code"] == "RAW_INTERNAL_LABEL_VISIBLE"
    ]
    assert not raw_warns, raw_warns
    # Every manual-review flag must carry an Italian kind_label.
    for flag in gate["customer_report"]["manual_review_flags"]:
        assert flag.get("kind_label"), flag


def test_compliance_section_includes_conforming_with_evidence():
    ws, vr, lr, contract, report = _build_all(GENERIC_PERIZIA_PAGES, make_worksheet())
    by_area = {c["area"]: c for c in report["compliance_section"]}
    assert by_area["urbanistica"]["status_label"] == "conforme secondo la perizia"
    assert by_area["urbanistica"]["evidence_pages"], "conforming needs evidence pages"
    assert by_area["edilizia"]["status_label"] == "regolarizzabile secondo la perizia"


def test_formalities_section_never_buyer_debt_wording():
    ws, vr, lr, contract, report = _build_all(GENERIC_PERIZIA_PAGES, make_worksheet())
    for item in report["formalities_section"]:
        assert item["cancelled_by_procedure"] is True
        assert "cancellazione indicata a cura della procedura" in item["status_label"]


def test_markdown_certificate_renders():
    ws, vr, lr, contract, report = _build_all(GENERIC_PERIZIA_PAGES, make_worksheet())
    gate = _gate(GENERIC_PERIZIA_PAGES, ws, contract, report, vr, lr)
    md = quality_mod.render_quality_markdown(
        gate["quality_report"], gate["scorecard"], gate["page_audit"]
    )
    assert "Controllo qualità pagina per pagina" in md
    assert "| Pagina |" in md
