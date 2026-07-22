from correctness_v2 import coverage_audit, validator

from .beta_fixture import build_lot, prepare


def test_beta_multilot_lot_1_repaired_customer_report():
    built = build_lot("1")
    report = built["customer_report"]
    assert built["report"]["lot_count"] == 4
    assert built["validator"]["validation_status"] == validator.STATUS_VALIDATED
    assert report["case_identity"]["address"] == "Via delle Magnolie 17, Borgo Esempio"
    assert "Abitazione" in report["case_identity"]["property_type"]
    assert {item["bene_id"] for item in report["beni_sections"]} == {"1", "2"}
    assert "locazione" in report["occupancy_section"]["title_info"].lower()
    amounts = [row["amount"] for rows in report["money_sections"].values() for row in rows if isinstance(row, dict) and row.get("amount") is not None]
    assert {124000, 108000, 16000, 1800} <= set(amounts)
    assert any(row["amount"] == 1800 and "comparativo" in row["label"].lower() for row in report["money_sections"]["valuation_chain"])
    assert any(item["value"] == "69,04" for item in report["surfaces_section"])
    assert all(item["classification"] == "conforming" for item in report["compliance_section"])
    assert all("verificare" not in item["status_label"].lower() for item in report["compliance_section"])
    assert {item["type"] for item in report["formalities_section"]} == {"ipoteca", "pignoramento"}
    assert all(item["cancelled_by_procedure"] and not item["buyer_burden"] for item in report["formalities_section"])
    assert built["projection"]["conflicts"] == []
    assert not any("lotto 2" in str(value).lower() or "lotto 3" in str(value).lower() or "lotto 4" in str(value).lower() for value in report.values())


def test_beta_multilot_historical_vs_repaired_coverage_contract():
    built = build_lot("1")
    args = dict(
        analysis_id="fixture_beta_multilot", job_id="fixture_job_lot_1",
        pages=built["verification"], contract=built["contract"],
        validator_report=built["validator"], lot_report=built["report"],
        full_document_pages=built["pages"], lot_id="1", segmentation=built["segmentation"],
        case_ledger=built["ledger"], selected_analysis_pages=[p["page_number"] for p in built["selected"]],
    )
    historical, _ = coverage_audit.build_coverage_audit(
        **args,
        worksheet=built["case"]["lot_worksheets"]["1"],
        customer_report=built["case"]["stored_customer_reports"]["historical_lot_1"],
        lot_fact_projection_report={},
    )
    repaired, _ = coverage_audit.build_coverage_audit(
        **args, worksheet=built["worksheet"], customer_report=built["customer_report"],
        lot_fact_projection_report=built["projection"],
    )
    assert historical["coverage_status"] in {"WARNING", "FAIL"}
    assert historical["lot_coverage"]["general_fact_recall"] < 0.85
    assert all(repaired["lot_coverage"][key] == "PASS" for key in (
        "extraction_coverage_state", "report_completeness_state",
        "evidence_completeness_state", "user_visible_completeness_state",
    ))
    assert repaired["lot_coverage"]["critical_fact_recall"] == 1.0
    assert repaired["lot_coverage"]["general_fact_recall"] >= 0.85


def test_beta_multilot_cross_lot_and_blocked_lot_facts():
    lot2, lot3, lot4 = build_lot("2"), build_lot("3"), build_lot("4")
    assert "libero" not in lot2["worksheet"]["occupancy"]["status"].lower()
    assert {item["type"] for item in lot2["worksheet"]["legal_formalities"]} == {"ipoteca"}
    issue = next(item for item in lot3["worksheet"]["technical_compliance"] if "Regolarità" in item["area"])
    assert issue["classification"] == "regularizable" and issue["blocks_saleability"] is False
    assert not lot3["worksheet"]["legal_formalities"]
    assert lot4["worksheet"]["case_identity"]["address"] == "Via del Tiglio 2, Borgo Esempio"
    assert lot4["worksheet"]["legal_formalities"] == []
    assert lot4["case"]["blocked_lot_id"] == "4"
    assert "Stato occupativo critico mancante" in lot4["worksheet"]["missing_or_uncertain"]
