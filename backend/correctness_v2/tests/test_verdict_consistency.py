from __future__ import annotations

import copy
import inspect
import json
from pathlib import Path

import pytest

from correctness_v2 import (
    contract, customer_report, customer_view, decision_model, feature_flags,
    lot_packets, verdict_model,
)
from correctness_v2.scripts.offline_historical_replay import run_replay
from correctness_v2.tests.beta_fixture import ROOT as BETA_ROOT, build_lot


def _money():
    return {
        "valuation_chain": [], "auction_terms": [], "buyer_side_costs": [],
        "procedure_cancelled_formalities": [], "market_comparatives": [],
        "context_values": [], "uncertain_money": [],
    }


def _report(**overrides):
    report = {
        "schema_version": "cv2.customer_report.v1", "analysis_id": "generic",
        "job_id": "generic-job", "report_status": "REPORT_READY",
        "case_identity": {"property_type": "Immobile", "evidence_pages": [1]},
        "lot_structure": {"selected_lot": "1", "lot_count": 1, "bene_count": 1},
        "beni_sections": [], "occupancy_section": {}, "compliance_section": [],
        "formalities_section": [], "risk_sections": [], "buyer_checklist": [],
        "customer_evidence_index": [], "money_sections": _money(),
    }
    report.update(overrides)
    return report


def _lot_verdict(lot_id: str, severity: str):
    report = _report(analysis_id=f"lot-{lot_id}", lot_structure={"selected_lot": lot_id})
    if severity == "grave":
        report["risk_sections"] = [{"items": [{"area": "struttura", "severity": "grave", "summary": f"Difetto specifico {lot_id}"}]}]
    elif severity == "media":
        report["compliance_section"] = [{"area": "edilizia", "classification": "regularizable"}]
    elif severity == "minore":
        report["compliance_section"] = [{"area": "edilizia", "classification": "uncertain"}]
    return verdict_model.build_lot_verdict(report, lot_id=lot_id)


def test_matrix_01_clean_single_lot_semantic_consistency():
    out = customer_view.sanitize_customer_report(_report())
    assert verdict_model.same_canonical_source(out["decision"], out["decision_model"]["esito"])
    assert not verdict_model.cross_band_contradiction(
        out["decision"]["level"], out["decision_model"]["esito"]["level"]
    )


def test_matrix_02_uncertain_compliance_no_dual_verdict():
    out = customer_view.sanitize_customer_report(_report(compliance_section=[{
        "area": "urbanistica", "classification": "uncertain", "notes": "Da verificare"
    }]))
    assert out["decision"]["level"] == "da_verificare"
    assert out["decision_model"]["esito"]["level"] == "verde"
    assert verdict_model.same_canonical_source(out["decision"], out["decision_model"]["esito"])
    assert not verdict_model.cross_band_contradiction(
        out["decision"]["level"], out["decision_model"]["esito"]["level"]
    )


def test_matrix_03_case_floor_is_worst_completed_lot():
    case = verdict_model.build_case_verdict(
        [_lot_verdict("1", "verde"), _lot_verdict("2", "grave")],
        all_lot_ids=["1", "2"],
    )
    assert case["severity"] == "grave"


def test_matrix_04_unresolved_lot_is_unknown_not_green():
    case = verdict_model.build_case_verdict([_lot_verdict("1", "verde")], all_lot_ids=["1", "2"])
    assert case["field_verdicts"]["per_lot"]["2"] == {
        "status": "UNKNOWN", "provenance_reason": verdict_model.UNRESOLVED_LOT,
    }


def test_matrix_05_case_global_severe_exception_is_source_backed():
    global_fact = {
        "fact_id": "risk:global:0", "applicability": "CASE_GLOBAL",
        "value": {"severity": "grave", "summary": "Vincolo globale dichiarato"},
    }
    case = verdict_model.build_case_verdict(
        [_lot_verdict("1", "verde")], all_lot_ids=["1"], case_global_facts=[global_fact]
    )
    assert case["severity"] == "grave"
    assert case["drivers"][0]["source_fact_id"] == global_fact["fact_id"]


def test_matrix_06_projected_lease_never_becomes_vacant():
    built = build_lot("1")
    verdict = verdict_model.build_lot_verdict(
        built["customer_report"], reconciled_worksheet=built["worksheet"],
        case_ledger=built["ledger"], segmentation=built["segmentation"],
        lot_report=built["report"], lot_id="1",
    )
    assert verdict["field_verdicts"]["occupancy"]["value"] != "UNKNOWN"
    assert "liber" not in str(verdict["field_verdicts"]["occupancy"]["value"]).lower()


def test_matrix_07_regularizable_lot_not_reescalated_by_case_snapshot():
    built = build_lot("3")
    stale_case_report = copy.deepcopy(built["customer_report"])
    stale_case_report["risk_sections"] = [{"items": [{
        "area": "Regolarità interna", "severity": "grave",
        "summary": "Snapshot di caso superato dalla riconciliazione",
    }]}]
    verdict = verdict_model.build_lot_verdict(
        stale_case_report, reconciled_worksheet=built["worksheet"],
        case_ledger=built["ledger"], segmentation=built["segmentation"],
        lot_report=built["report"], lot_id="3",
    )
    assert verdict["field_verdicts"]["compliance"][0]["classification"] == "regularizable"
    assert verdict["severity"] == "media"


def test_matrix_08_typology_conflict_is_explicit_and_lot_wins():
    report = _report(case_identity={"property_type": "Tipologia riconciliata"})
    report["lot_fact_projection"] = {"conflicts": [{
        "path": "case_identity.property_type", "case_value": "Tipo caso",
        "lot_value": "Tipologia riconciliata", "reason": "CONFLICT_REQUIRES_REVIEW",
    }]}
    verdict = verdict_model.build_lot_verdict(report)
    assert verdict["field_verdicts"]["typology"]["value"] == "Tipologia riconciliata"
    assert verdict["conflicts"][0]["reason_code"] == "CONFLICT_REQUIRES_REVIEW"


def test_matrix_09_risk_fallback_uses_italian_label(monkeypatch):
    monkeypatch.setenv(feature_flags.FLAG_CANONICAL_VERDICT, "true")
    cards = contract._risk_cards({
        "technical_compliance": [{"area": "Edilizia", "classification": "regularizable"}],
        "risk_classification": [],
    })
    assert "regolarizzabile secondo la perizia" in cards[0]["summary"]
    assert "regularizable" not in cards[0]["summary"]


def test_matrix_10_negated_keyword_does_not_escalate_minor(monkeypatch):
    monkeypatch.setenv(feature_flags.FLAG_CANONICAL_VERDICT, "true")
    report = _report(risk_sections=[{"items": [{
        "area": "stato", "severity": "minore",
        "summary": "non presenta rischio di crollo",
    }]}])
    verdict = verdict_model.build_lot_verdict(report)
    assert verdict["severity"] == "minore"
    assert customer_view.derive_decision(report)["level"] != "attenzione"


def test_matrix_11_selector_reads_completed_canonical_fields():
    verdict = verdict_model.build_lot_verdict(_report(
        case_identity={"property_type": "Tipo riconciliato"},
        occupancy_section={"status": "occupato"},
    ), lot_id="1")
    index = lot_packets.apply_reconciled_verdicts({"lots": [{
        "lot_id": "1", "property_type": "Tipo vecchio", "occupancy_summary": "libero",
    }]}, {"1": verdict})
    selection = {"analysis_id": "a", "job_id": "j", "lot_ids": ["1"], "lot_count": 1,
                 "available_lots": [{"lot_id": "1", "property_type": "Tipo vecchio", "occupancy_summary": "libero"}]}
    rendered = customer_report.render_lot_selection_report(selection, index)
    lot = rendered["lot_selection"]["lots"][0]
    assert lot["property_type"] == verdict["field_verdicts"]["typology"]["value"]
    assert lot["occupancy_summary"] == verdict["field_verdicts"]["occupancy"]["value"]


def test_matrix_12_unresolved_selector_snapshot_marked_unconfirmed():
    index = lot_packets.apply_reconciled_verdicts({"lots": [{"lot_id": "2"}]}, {})
    selection = {"analysis_id": "a", "job_id": "j", "lot_ids": ["2"], "lot_count": 1,
                 "available_lots": [{"lot_id": "2"}]}
    lot = customer_report.render_lot_selection_report(selection, index)["lot_selection"]["lots"][0]
    assert lot["confidence"] == "unconfirmed"


def test_matrix_13_old_artifact_off_path_unchanged(monkeypatch):
    report = _report(occupancy_section={"status": "libero", "status_label": "Libero"})
    monkeypatch.setenv(feature_flags.FLAG_CANONICAL_VERDICT, "false")
    out = customer_view.sanitize_customer_report(report)
    assert "canonical_verdict" not in out
    assert "canonical_verdict_ref" not in out["decision"]
    assert out["decision"]["level"] == "pronto_con_avvertenze"


def test_matrix_14_formality_cancelled_consistent():
    verdict = verdict_model.build_lot_verdict(_report(formalities_section=[{
        "type": "ipoteca", "cancelled_by_procedure": True, "buyer_burden": False,
    }]))
    assert verdict["field_verdicts"]["formalities"][0]["status"] == "cancelled_by_procedure"


def test_matrix_15_workspace_money_source_matches_canonical_final():
    built = build_lot("1")
    verdict = verdict_model.build_lot_verdict(
        built["customer_report"], reconciled_worksheet=built["worksheet"],
        case_ledger=built["ledger"], segmentation=built["segmentation"],
        lot_report=built["report"], lot_id="1",
    )
    assert verdict["field_verdicts"]["money"]["final_value"]["amount"] is not None


def test_matrix_16_multi_bene_typologies_remain_distinct():
    verdict = verdict_model.build_lot_verdict(_report(beni_sections=[
        {"bene_id": "1", "property_type": "Abitazione"},
        {"bene_id": "2", "property_type": "Autorimessa"},
    ]))
    assert [row["value"] for row in verdict["field_verdicts"]["typology"]["per_bene"]] == [
        "Abitazione", "Autorimessa",
    ]


def test_matrix_17_duplicate_specific_text_different_ids_flags_leak():
    text = "Difetto specifico e circostanziato della copertura comune"
    left, right = _lot_verdict("1", "grave"), _lot_verdict("2", "grave")
    left["drivers"] = [{"driver_id": "a", "source_fact_id": "risk:a:1", "text": text}]
    right["drivers"] = [{"driver_id": "b", "source_fact_id": "risk:b:1", "text": text}]
    case = verdict_model.build_case_verdict([left, right], all_lot_ids=["1", "2"])
    assert any(c["reason_code"] == "POSSIBLE_CROSS_LOT_LEAKAGE" for c in case["conflicts"])


def test_matrix_18_unknown_schema_fails_closed_in_consumer():
    original = {"lots": [{
        "lot_id": "1", "property_type": "Tipo legacy",
        "occupancy_summary": "Occupazione legacy",
    }]}
    rendered = lot_packets.apply_reconciled_verdicts(
        original, {"1": {"schema_version": "unknown", "severity": "verde"}},
    )
    assert rendered["lots"][0]["property_type"] == "Tipo legacy"
    assert rendered["lots"][0]["occupancy_summary"] == "Occupazione legacy"
    assert "canonical_verdict" not in rendered["lots"][0]


def test_stored_unknown_verdict_renders_exact_legacy_projection(monkeypatch):
    report = _report(occupancy_section={"status": "occupato", "status_label": "Occupato"})
    report["canonical_verdict"] = {
        "schema_version": "cv2.verdict.v0", "severity": "verde",
    }
    monkeypatch.setenv(feature_flags.FLAG_CANONICAL_VERDICT, "true")
    repaired = customer_view.sanitize_customer_report(report)
    monkeypatch.setenv(feature_flags.FLAG_CANONICAL_VERDICT, "false")
    legacy = customer_view.sanitize_customer_report(report)
    assert repaired == legacy
    assert "canonical_verdict" not in repaired


def test_matrix_19_runtime_has_no_historical_fixture_literals():
    source = inspect.getsource(verdict_model)
    fixture = json.loads((BETA_ROOT / "beta_multilot_case_sanitized.json").read_text(encoding="utf-8"))
    checks = fixture["replay_expectations"]["fact_checks"].values()
    forbidden = [
        str(spec["value"])
        for spec in checks
        if isinstance(spec.get("value"), (int, float)) and float(spec["value"]) >= 1000
    ]
    forbidden.extend([
        fixture["case_worksheet"]["case_identity"]["address"],
        fixture["case_worksheet"]["case_identity"]["procedura_rge"],
    ])
    assert all(value not in source for value in forbidden)


def test_matrix_20_offline_sanitized_replay_acceptance(tmp_path):
    result = run_replay(output_dir=tmp_path, fixture_root=BETA_ROOT)
    assert result["critical_fact_coverage_after"] == 1.0
    assert result["cross_lot_leakage"] is False
