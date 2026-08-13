from __future__ import annotations

import copy

import pytest

from correctness_v2 import fact_lineage, lot_fact_projection, verdict_model
from correctness_v2.tests.beta_fixture import build_lot


def test_live_verdict_reuses_branch1_ids_for_projected_and_native_facts():
    built = build_lot("1")
    verdict = verdict_model.build_lot_verdict(
        built["customer_report"], reconciled_worksheet=built["worksheet"],
        case_ledger=built["ledger"], segmentation=built["segmentation"],
        lot_report=built["report"], lot_id="1",
    )
    ledger = verdict_model.build_reconciled_fact_ledger(
        built["worksheet"], "1", segmentation=built["segmentation"],
        lot_report=built["report"],
    )
    resolvable = {fact["fact_id"] for fact in built["ledger"]["facts"] + ledger["facts"]}
    leaves = [
        verdict["field_verdicts"]["typology"],
        verdict["field_verdicts"]["occupancy"],
        *verdict["field_verdicts"]["compliance"],
        *verdict["field_verdicts"]["formalities"],
        verdict["field_verdicts"]["money"]["final_value"],
    ]
    assert all(leaf.get("source_fact_id") in resolvable for leaf in leaves)


def test_legacy_verdict_uses_only_closed_fail_safe_reason():
    verdict = verdict_model.build_lot_verdict({
        "analysis_id": "old", "report_status": "REPORT_READY",
        "case_identity": {"property_type": "Immobile"},
        "occupancy_section": {"status": "libero"},
    })
    assert verdict["field_verdicts"]["occupancy"]["value"] == "UNKNOWN"
    assert verdict["field_verdicts"]["occupancy"]["provenance_reason"] == verdict_model.LEGACY_ARTIFACT_NO_LEDGER


def test_live_occupancy_omission_is_unknown_and_not_canonically_clean():
    built = build_lot("4")
    verdict = verdict_model.build_lot_verdict(
        built["customer_report"], reconciled_worksheet=built["worksheet"],
        case_ledger=built["ledger"], segmentation=built["segmentation"],
        lot_report=built["report"], lot_id="4",
    )
    assert verdict["field_verdicts"]["occupancy"]["value"] == "UNKNOWN"
    assert verdict["severity"] == "media"


@pytest.mark.parametrize(
    ("severity", "decision", "esito"),
    [
        ("verde", "pronto_con_avvertenze", "verde"),
        ("info", "pronto_con_avvertenze", "verde"),
        ("minore", "da_verificare", "ambra"),
        ("media", "da_verificare", "ambra"),
        ("grave", "attenzione", "rosso"),
    ],
)
def test_frozen_projection_tables(severity, decision, esito):
    assert verdict_model.project_to_decision_level(severity) == decision
    assert verdict_model.project_to_esito_level(severity) == esito


def test_unreadable_is_interactive_amber_non_legible():
    verdict = verdict_model.build_lot_verdict({
        "analysis_id": "unreadable", "report_status": "DOCUMENT_NOT_READABLE"
    })
    assert verdict_model.project_to_esito_level(verdict) == "ambra"
    assert verdict_model.project_to_decision_level(verdict) == "non_leggibile"


def test_unknown_schema_rejected_without_best_effort_render():
    with pytest.raises(ValueError, match="unsupported canonical verdict schema"):
        verdict_model.validate_verdict({"schema_version": "future", "severity": "verde"})


def test_runtime_refinement_preserves_canonical_identity():
    base = verdict_model.build_lot_verdict({
        "analysis_id": "runtime", "report_status": "REPORT_READY"
    })
    refined = verdict_model.refine_for_runtime(
        base, report_status="REPORT_READY",
        readiness={"state": "READY_FOR_REVIEW"}, open_checks=True,
    )
    assert refined["canonical_ref"] == base["canonical_ref"]
    assert refined["severity"] == base["severity"]
    assert verdict_model.project_to_esito_level(refined) == "ambra"


def test_case_aggregation_rejects_mixed_schema_versions():
    good = verdict_model.build_lot_verdict({"analysis_id": "a", "report_status": "REPORT_READY"}, lot_id="1")
    bad = copy.deepcopy(good)
    bad["schema_version"] = "cv2.verdict.v0"
    with pytest.raises(ValueError):
        verdict_model.build_case_verdict([good, bad], all_lot_ids=["1", "2"])


def test_open_formality_is_not_clean_and_case_aggregation_preserves_it():
    report = {
        "analysis_id": "formality", "report_status": "REPORT_READY",
        "formalities_section": [{
            "type": "ipoteca", "description": "Trattamento da chiarire",
            "cancelled_by_procedure": False, "buyer_burden": False,
        }],
    }
    lot = verdict_model.build_lot_verdict(report, lot_id="1")
    case = verdict_model.build_case_verdict([lot], all_lot_ids=["1"])
    assert lot["severity"] == "media"
    assert verdict_model.project_to_esito_level(lot) == "ambra"
    assert case["severity"] == "media"


def test_case_global_severe_fact_does_not_override_superseding_lot_fact():
    built = build_lot("3")
    lot = verdict_model.build_lot_verdict(
        built["customer_report"], reconciled_worksheet=built["worksheet"],
        case_ledger=built["ledger"], segmentation=built["segmentation"],
        lot_report=built["report"], lot_id="3",
    )
    addressed = lot["provenance"]["addressed_fact_keys"]
    compliance_key = next(key for key in addressed if key["category"] == "compliance")
    severe_global = {
        "fact_id": "compliance:global:99", "category": "compliance",
        "field": compliance_key["field_token"],
        "source_path": "technical_compliance[99]", "applicability": "CASE_GLOBAL",
        "value": {"classification": "non_conforming", "severity": "grave"},
    }
    case = verdict_model.build_case_verdict(
        [lot], all_lot_ids=["3"], case_global_facts=[severe_global]
    )
    assert lot["severity"] == "media"
    assert case["severity"] == "media"
    assert case["drivers"] == []


def test_unrelated_same_index_fact_does_not_suppress_case_global_severe():
    built = build_lot("3")
    worksheet = copy.deepcopy(built["worksheet"])
    worksheet["technical_compliance"] = [{
        "area": "Impianto elettrico",
        "classification": "regularizable",
        "blocks_saleability": False,
        "notes": "Adeguamento ordinario da verificare",
        "evidence_pages": [8],
    }]
    worksheet["risk_classification"] = []
    lot = verdict_model.build_lot_verdict(
        built["customer_report"], reconciled_worksheet=worksheet,
        segmentation=built["segmentation"], lot_report=built["report"], lot_id="3",
    )
    severe_global = {
        "fact_id": "compliance:amianto:global", "category": "compliance",
        "field": "amianto", "source_path": "technical_compliance[0]",
        "applicability": "CASE_GLOBAL",
        "value": {
            "area": "Amianto", "classification": "non_conforming",
            "severity": "grave", "summary": "Rischio amianto da verifica tecnica",
        },
    }
    case = verdict_model.build_case_verdict(
        [lot], all_lot_ids=["3"], case_global_facts=[severe_global]
    )
    assert lot["severity"] == "media"
    assert [
        key for key in lot["provenance"]["addressed_fact_keys"]
        if key["category"] == "compliance"
    ] == [{"category": "compliance", "field_token": "impianti"}]
    assert case["severity"] == "grave"
    assert case["drivers"][0]["source_fact_id"] == "compliance:amianto:global"


def _lot_with_untyped_formality(description):
    built = build_lot("3")
    worksheet = copy.deepcopy(built["worksheet"])
    worksheet["technical_compliance"] = []
    worksheet["risk_classification"] = []
    worksheet["legal_formalities"] = [{
        "description": description,
        "cancelled_by_procedure": False,
        "buyer_burden": False,
        "evidence_pages": [8],
    }]
    return verdict_model.build_lot_verdict(
        built["customer_report"], reconciled_worksheet=worksheet,
        segmentation=built["segmentation"], lot_report=built["report"], lot_id="3",
    )


def test_untyped_formality_does_not_suppress_case_global_severe():
    lot = _lot_with_untyped_formality("Servitù di passaggio")
    severe_global = {
        "fact_id": "formality:vincolo:global", "category": "formality",
        "field": "other", "label": "Vincolo storico-artistico",
        "source_path": "legal_formalities[0]", "applicability": "CASE_GLOBAL",
        "value": {
            "description": "Vincolo storico-artistico", "severity": "grave",
        },
    }
    case = verdict_model.build_case_verdict(
        [lot], all_lot_ids=["3"], case_global_facts=[severe_global]
    )
    assert lot["severity"] == "media"
    assert {
        key["field_token"] for key in lot["provenance"]["addressed_fact_keys"]
        if key["category"] == "formality"
    } == {"servitu di passaggio"}
    assert case["severity"] == "grave"
    assert case["drivers"][0]["source_fact_id"] == "formality:vincolo:global"


def test_untyped_formality_genuine_supersession_still_deescalates():
    description = "Vincolo storico-artistico"
    lot = _lot_with_untyped_formality(description)
    severe_global = {
        "fact_id": "formality:vincolo:global", "category": "formality",
        "field": "other", "label": description,
        "source_path": "legal_formalities[99]", "applicability": "CASE_GLOBAL",
        "value": {"description": description, "severity": "grave"},
    }
    case = verdict_model.build_case_verdict(
        [lot], all_lot_ids=["3"], case_global_facts=[severe_global]
    )
    assert lot["severity"] == "media"
    assert case["severity"] == "media"
    assert case["drivers"] == []


def test_contentless_generic_fact_is_not_matchable_without_exact_id():
    lot = _lot_with_untyped_formality("")
    severe_global = {
        "fact_id": "formality:generic:global", "category": "formality",
        "field": "other", "source_path": "legal_formalities[0]",
        "applicability": "CASE_GLOBAL", "value": {"severity": "grave"},
    }
    case = verdict_model.build_case_verdict(
        [lot], all_lot_ids=["3"], case_global_facts=[severe_global]
    )
    assert not any(
        key["category"] == "formality"
        for key in lot["provenance"]["addressed_fact_keys"]
    )
    assert case["severity"] == "grave"
    assert case["drivers"][0]["source_fact_id"] == "formality:generic:global"


def _real_ledger_inputs(case_description, lot_description):
    case_worksheet = {
        "case_identity": {"property_type": "Appartamento", "evidence_pages": [1]},
        "legal_formalities": [{
            "description": case_description,
            "severity": "grave",
            "cancelled_by_procedure": False,
            "buyer_burden": False,
            "evidence_pages": [1],
        }],
    }
    lot_worksheet = {
        "case_identity": {"property_type": "Appartamento", "evidence_pages": [2]},
        "legal_formalities": ([{
            "description": lot_description,
            "cancelled_by_procedure": False,
            "buyer_burden": False,
            "evidence_pages": [2],
        }] if lot_description is not None else []),
    }
    case_segmentation = {
        "lot_ids": ["1", "2"], "lot_pages": {"2": [2]},
        "global_pages": [1], "shared_pages": [],
        "page_assignments": [
            {"page": 1, "method": "global", "assigned_lot": None},
            {"page": 2, "method": "explicit", "assigned_lot": "2"},
        ],
    }
    lot_segmentation = {
        "lot_ids": ["2"], "lot_pages": {"2": [2]},
        "global_pages": [], "shared_pages": [],
        "page_assignments": [
            {"page": 2, "method": "explicit", "assigned_lot": "2"},
        ],
    }
    case_report = {"lot_ids": ["1", "2"], "lot_count": 2, "multi_lot": True}
    lot_report = {"lot_ids": ["2"], "lot_count": 1, "multi_lot": False}
    return (
        case_worksheet, lot_worksheet, case_segmentation, lot_segmentation,
        case_report, lot_report,
    )


def test_lotnative_id_collision_does_not_suppress_case_global_severe():
    (
        case_worksheet, lot_worksheet, case_segmentation, lot_segmentation,
        case_report, lot_report,
    ) = _real_ledger_inputs("Vincolo storico-artistico", "Servitù di passaggio")
    case_ledger = fact_lineage.build_case_fact_ledger(
        case_worksheet, case_segmentation, case_report
    )
    lot_ledger = fact_lineage.build_case_fact_ledger(
        lot_worksheet, lot_segmentation, lot_report
    )
    case_formality = next(
        fact for fact in case_ledger["facts"] if fact["category"] == "formality"
    )
    lot_formality = next(
        fact for fact in lot_ledger["facts"] if fact["category"] == "formality"
    )
    assert case_formality["fact_id"] == lot_formality["fact_id"] == "formality:other:1"
    assert case_formality["applicability"] == fact_lineage.ALL_LOTS

    lot = verdict_model.build_lot_verdict(
        {"analysis_id": "collision", "report_status": "REPORT_READY"},
        reconciled_worksheet=lot_worksheet, case_ledger=case_ledger,
        segmentation=lot_segmentation, lot_report=lot_report, lot_id="2",
    )
    case = verdict_model.build_case_verdict(
        [lot], all_lot_ids=["2"], case_global_facts=case_ledger["facts"]
    )

    assert lot["severity"] == "media"
    assert lot["provenance"]["case_fact_ids"] == []
    assert case_formality["fact_id"] in lot["provenance"]["lot_fact_ids"]
    assert case["severity"] == "grave"
    assert case["drivers"][0]["source_fact_id"] == case_formality["fact_id"]


def test_real_projected_case_fact_exact_id_still_counts_as_reflected():
    (
        case_worksheet, lot_worksheet, case_segmentation, lot_segmentation,
        case_report, lot_report,
    ) = _real_ledger_inputs(None, None)
    case_ledger = fact_lineage.build_case_fact_ledger(
        case_worksheet, case_segmentation, case_report
    )
    case_formality = next(
        fact for fact in case_ledger["facts"] if fact["category"] == "formality"
    )
    reconciled, _ = lot_fact_projection.project_and_reconcile(
        case_ledger=case_ledger, lot_worksheet=lot_worksheet, lot_id="2",
        segmentation=case_segmentation, all_lot_ids=["1", "2"],
    )
    assert reconciled["legal_formalities"][0]["projected"] is True
    assert reconciled["legal_formalities"][0]["fact_id"] == case_formality["fact_id"]

    lot = verdict_model.build_lot_verdict(
        {"analysis_id": "projected", "report_status": "REPORT_READY"},
        reconciled_worksheet=reconciled, case_ledger=case_ledger,
        segmentation=lot_segmentation, lot_report=lot_report, lot_id="2",
    )
    case = verdict_model.build_case_verdict(
        [lot], all_lot_ids=["2"], case_global_facts=case_ledger["facts"]
    )

    assert lot["provenance"]["case_fact_ids"] == [case_formality["fact_id"]]
    assert lot["provenance"]["case_fact_ids_origin"] == verdict_model.PROJECTED_CASE_FACT_ORIGIN
    assert case["severity"] == "media"
    assert case["drivers"] == []


def test_priority_is_canonical_severity_base_plus_offset():
    assert verdict_model.priority_from_severity("grave") == 0
    assert verdict_model.priority_from_severity("media") == 3
    assert verdict_model.priority_from_severity("minore") == 5
    assert verdict_model.priority_from_severity("info") == 6
    assert verdict_model.priority_from_severity("grave", 2) == 2
    assert verdict_model.priority_from_severity("media", 1) == 4
