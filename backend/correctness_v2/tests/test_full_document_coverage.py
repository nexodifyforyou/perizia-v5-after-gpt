import copy
import json
import re

from correctness_v2 import coverage_audit

from .beta_fixture import build_lot


def _audit(built, customer=None, worksheet=None, projection=None):
    return coverage_audit.build_coverage_audit(
        analysis_id="fixture_beta_multilot",
        job_id="fixture_job_lot_1",
        pages=built["verification"],
        worksheet=worksheet or built["worksheet"],
        contract=built["contract"],
        customer_report=customer or built["customer_report"],
        validator_report=built["validator"],
        lot_report=built["report"],
        full_document_pages=built["pages"],
        lot_id="1",
        segmentation=built["segmentation"],
        case_ledger=built["ledger"],
        lot_fact_projection_report=projection if projection is not None else built["projection"],
        selected_analysis_pages=[page["page_number"] for page in built["selected"]],
    )[0]


def test_matrix_11_shared_forced_sale_value_survives_money_channel():
    built = build_lot("1")
    rows = built["contract"]["valuation_chain"]
    assert any(row["amount"] == 108000 for row in rows)


def test_matrix_12_shared_reduction_survives_money_channel():
    built = build_lot("1")
    rows = built["contract"]["valuation_chain"]
    assert any(row["amount"] == 16000 and "riduzione" in row["label"].lower() for row in rows)


def test_matrix_16_expectations_come_from_full_document():
    built = build_lot("1")
    historical = built["case"]["stored_customer_reports"]["historical_lot_1"]
    audit = _audit(built, customer=historical, worksheet=built["case"]["lot_worksheets"]["1"], projection={})
    metrics = audit["lot_coverage"]
    assert metrics["total_document_pages"] == len(built["pages"])
    assert metrics["expected_material_facts"] > metrics["lot_analysis_facts"]
    assert metrics["general_fact_recall"] < 0.85


def test_matrix_17_starved_lot_cannot_score_perfect_completeness():
    built = build_lot("1")
    historical = built["case"]["stored_customer_reports"]["historical_lot_1"]
    audit = _audit(built, customer=historical, worksheet=built["case"]["lot_worksheets"]["1"], projection={})
    metrics = audit["lot_coverage"]
    assert metrics["selected_page_ratio"] < 0.5
    assert metrics["report_completeness_state"] != "PASS" or metrics["user_visible_completeness_state"] != "PASS"
    assert audit["coverage_status"] != "PASS"


def test_matrix_18_user_visible_completeness_is_independent():
    built = build_lot("1")
    customer = copy.deepcopy(built["customer_report"])
    for key in ("formalities_section", "compliance_section", "occupancy_section", "risk_sections", "buyer_checklist", "key_facts"):
        customer[key] = [] if key != "occupancy_section" else {}
    audit = _audit(built, customer=customer)
    metrics = audit["lot_coverage"]
    assert metrics["report_completeness_state"] == "PASS"
    assert metrics["evidence_completeness_state"] == "PASS"
    assert metrics["user_visible_completeness_state"] != "PASS"


def test_matrix_19_projection_artifact_contains_no_production_identifiers():
    built = build_lot("1")
    serialized = json.dumps(built["projection"], ensure_ascii=False).lower()
    assert not re.search(r"\bcv2_[0-9a-f]{8,}\b", serialized)
    assert "@" not in serialized


def test_matrix_20_offline_replay_has_zero_calls_and_writes(tmp_path):
    from correctness_v2.scripts.offline_historical_replay import run_replay
    fixture_root = __import__("pathlib").Path(__file__).parent / "fixtures"
    result = run_replay(output_dir=tmp_path, fixture_root=fixture_root)
    assert result["network_calls"] == result["database_writes"] == result["paid_calls"] == 0
    assert result["quota_or_credit_consumption"] == 0
    assert not result["hallucinated_fact_ids"]
