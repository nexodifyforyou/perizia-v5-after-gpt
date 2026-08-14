"""R3-02: case verdict rebuilds retain every known lot verdict."""

from __future__ import annotations

import copy
import json

import pytest

from correctness_v2 import (
    artifacts,
    feature_flags,
    orchestrator,
    verdict_model,
    workspace,
)


def _report(lot_id: str, property_type: str) -> dict:
    return {
        "schema_version": "cv2.customer_report.v1",
        "analysis_id": "analysis_r302",
        "job_id": "job_r302",
        "report_status": "REPORT_READY",
        "title": f"Lotto {lot_id}",
        "case_identity": {
            "address": f"Via Lotto {lot_id}",
            "property_type": property_type,
        },
        "lot_structure": {"selected_lot": str(lot_id), "bene_count": 1},
        "occupancy_section": {"status": f"occupancy-{lot_id}"},
        "compliance_section": [],
        "formalities_section": [],
        "risk_sections": [],
        "money_sections": {
            "valuation_chain": [
                {"label": "Valore finale", "amount": int(lot_id) * 10000}
            ]
        },
    }


def _verdict(lot_id: str, property_type: str, severity: str) -> dict:
    verdict = verdict_model.build_lot_verdict(
        _report(lot_id, property_type), lot_id=lot_id
    )
    verdict["severity"] = severity
    verdict["severity_label_it"] = verdict_model.SEVERITY_LABELS_IT[severity]
    return verdict


def _json_bytes(value: dict) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


@pytest.fixture()
def single_lot_success_case(artifacts_root, monkeypatch):
    monkeypatch.setenv(feature_flags.FLAG_CANONICAL_VERDICT, "true")
    monkeypatch.setenv(feature_flags.FLAG_PARTIAL_LOT_REPORTS, "false")
    job_id = "job_r302_single_success"
    analysis_id = "analysis_r302_single_success"
    old_verdicts = {
        "1": _verdict("1", "Abitazione Lotto 1", "verde"),
        "2": _verdict("2", "Vecchio Deposito Lotto 2", "verde"),
        "3": _verdict("3", "Autorimessa Lotto 3", "grave"),
    }
    for lot_id, verdict in old_verdicts.items():
        artifacts.save_lot_verdict(job_id, lot_id, verdict)
    original_case = verdict_model.build_case_verdict(
        list(old_verdicts.values()),
        scope_id=analysis_id,
        all_lot_ids=["1", "2", "3"],
    )
    artifacts.save_case_verdict(job_id, original_case)
    original_per_lot = copy.deepcopy(original_case["field_verdicts"]["per_lot"])

    fresh = _verdict("2", "Nuovo Ufficio Lotto 2", "media")
    fresh_report = _report("2", "Nuovo Ufficio Lotto 2")
    monkeypatch.setattr(
        orchestrator.contract_mod,
        "complete_valuation_terminals",
        lambda worksheet, _pages: worksheet,
    )
    monkeypatch.setattr(
        orchestrator.validator_mod,
        "apply_compliance_evidence_gate",
        lambda worksheet, _pages: (worksheet, {"downgrade_count": 0}),
    )
    monkeypatch.setattr(
        orchestrator.validator_mod,
        "validate_worksheet",
        lambda _worksheet, _pages: {
            "validation_status": "VALIDATED",
            "warning_count": 0,
        },
    )
    monkeypatch.setattr(
        orchestrator.contract_mod,
        "build_contract",
        lambda **_kwargs: {"schema_version": "cv2.contract.test"},
    )
    monkeypatch.setattr(
        orchestrator.customer_report_mod,
        "render_success_report",
        lambda _contract, _pages: copy.deepcopy(fresh_report),
    )
    monkeypatch.setattr(
        orchestrator.verdict_model_mod,
        "build_lot_verdict",
        lambda *_args, **_kwargs: copy.deepcopy(fresh),
    )
    monkeypatch.setattr(
        orchestrator.verdict_model_mod,
        "build_reconciled_fact_ledger",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        orchestrator,
        "_refine_canonical_for_report",
        lambda _report_value, verdict: verdict,
    )
    monkeypatch.setattr(
        orchestrator.quality_gate_mod,
        "run_quality_gate",
        lambda **_kwargs: {
            "gate_status": "PASS",
            "customer_report": copy.deepcopy(fresh_report),
            "coverage_audit": {"coverage_status": "PASS"},
            "quality_report": {
                "overall_quality_status": "PASS",
                "customer_readiness": "READY",
            },
            "scorecard": {"overall_score": 100, "status": "PASS"},
        },
    )

    status = orchestrator._build_single_lot_contract(
        job_id=job_id,
        analysis_id=analysis_id,
        worksheet={},
        pages=[],
        lot_report={"lot_ids": ["2"]},
        artifacts_saved={},
        created_at="2026-08-14T10:00:00+00:00",
        admin_only=False,
        source_quality="PDF_QUALITY_OK",
        model_name="offline-test",
        extra={"lot_ids": ["1", "2", "3"]},
        lot_id="2",
    )
    persisted = artifacts.read_json(job_id, artifacts.CASE_VERDICT_FILE)
    return {
        "job_id": job_id,
        "analysis_id": analysis_id,
        "status": status,
        "fresh": fresh,
        "original_per_lot": original_per_lot,
        "persisted": persisted,
    }


def test_r302_01_three_lot_success_preserves_sister_verdict_bytes(
    single_lot_success_case,
):
    result = single_lot_success_case
    per_lot = result["persisted"]["field_verdicts"]["per_lot"]
    assert result["status"]["status"] == "REPORT_READY"
    assert _json_bytes(per_lot["1"]) == _json_bytes(result["original_per_lot"]["1"])
    assert _json_bytes(per_lot["3"]) == _json_bytes(result["original_per_lot"]["3"])


def test_r302_03_genuinely_unresolved_lot_stays_unknown(artifacts_root):
    job_id = "job_r302_unresolved"
    lot_1 = _verdict("1", "Abitazione Lotto 1", "verde")
    fresh_lot_2 = _verdict("2", "Ufficio Lotto 2", "media")
    artifacts.save_lot_verdict(job_id, "1", lot_1)
    existing = verdict_model.build_case_verdict(
        [lot_1], all_lot_ids=["1"]
    )
    inputs, lot_ids = orchestrator._reconstruct_case_lot_verdicts(
        job_id=job_id,
        current_lot_id="2",
        current_verdict=fresh_lot_2,
        existing_case=existing,
        known_lot_ids=["1", "2", "3"],
    )
    rebuilt = verdict_model.build_case_verdict(inputs, all_lot_ids=lot_ids)
    assert rebuilt["field_verdicts"]["per_lot"]["3"] == {
        "status": "UNKNOWN",
        "provenance_reason": verdict_model.UNRESOLVED_LOT,
    }


def test_r302_04_known_sister_absent_from_operation_is_not_downgraded(
    artifacts_root,
):
    job_id = "job_r302_absent_operation_input"
    lot_1 = _verdict("1", "Abitazione Lotto 1", "verde")
    old_lot_2 = _verdict("2", "Vecchio Lotto 2", "verde")
    fresh_lot_2 = _verdict("2", "Nuovo Lotto 2", "media")
    lot_3 = _verdict("3", "Autorimessa Lotto 3", "grave")
    for lot_id, verdict in (("1", lot_1), ("2", old_lot_2), ("3", lot_3)):
        artifacts.save_lot_verdict(job_id, lot_id, verdict)
    singleton_case = verdict_model.build_case_verdict(
        [old_lot_2], all_lot_ids=["1", "2", "3"]
    )
    inputs, lot_ids = orchestrator._reconstruct_case_lot_verdicts(
        job_id=job_id,
        current_lot_id="2",
        current_verdict=fresh_lot_2,
        existing_case=singleton_case,
        known_lot_ids=["2"],
    )
    rebuilt = verdict_model.build_case_verdict(inputs, all_lot_ids=lot_ids)
    assert rebuilt["field_verdicts"]["per_lot"]["1"] == lot_1["field_verdicts"]
    assert rebuilt["field_verdicts"]["per_lot"]["3"] == lot_3["field_verdicts"]


def test_r302_05_updated_lot_receives_fresh_canonical_verdict(single_lot_success_case):
    result = single_lot_success_case
    assert result["persisted"]["field_verdicts"]["per_lot"]["2"] == (
        result["fresh"]["field_verdicts"]
    )
    stored_lot = artifacts.read_json(
        result["job_id"], "lots/2/canonical_verdict.json"
    )
    assert stored_lot["field_verdicts"] == result["fresh"]["field_verdicts"]


def test_r302_06_case_aggregation_recomputes_from_all_known_lots(
    single_lot_success_case,
):
    persisted = single_lot_success_case["persisted"]
    assert persisted["severity"] == "grave"
    assert persisted["readiness"] == "TECHNICAL_REVIEW_REQUIRED"


def test_r302_07_no_cross_lot_semantic_contamination(single_lot_success_case):
    result = single_lot_success_case
    per_lot = result["persisted"]["field_verdicts"]["per_lot"]
    assert per_lot["1"]["typology"]["value"] == "Abitazione Lotto 1"
    assert per_lot["2"]["typology"]["value"] == "Nuovo Ufficio Lotto 2"
    assert per_lot["3"]["typology"]["value"] == "Autorimessa Lotto 3"
    assert result["persisted"]["conflicts"] == []


def test_r302_08_persisted_case_contains_every_known_lot(single_lot_success_case):
    persisted = single_lot_success_case["persisted"]
    assert list(persisted["field_verdicts"]["per_lot"]) == ["1", "2", "3"]
    assert persisted["unresolved_lot_ids"] == []


def test_r302_09_workspace_and_persisted_case_verdict_agree(
    artifacts_root, monkeypatch,
):
    monkeypatch.setenv(feature_flags.FLAG_CANONICAL_VERDICT, "true")
    monkeypatch.setenv(feature_flags.FLAG_PARTIAL_LOT_REPORTS, "false")
    job_id = "job_r302_workspace"
    analysis_id = "analysis_r302_workspace"
    verdicts = {
        lot_id: verdict_model.build_lot_verdict(
            _report(lot_id, property_type), lot_id=lot_id
        )
        for lot_id, property_type in (
            ("1", "Abitazione Lotto 1"),
            ("2", "Ufficio Lotto 2"),
            ("3", "Autorimessa Lotto 3"),
        )
    }
    for lot_id, verdict in verdicts.items():
        artifacts.save_lot_verdict(job_id, lot_id, verdict)
        artifacts.save_lot_subartifact(
            job_id, lot_id, artifacts.CUSTOMER_REPORT_FILE,
            _report(lot_id, verdict["field_verdicts"]["typology"]["value"]),
        )
    artifacts.save_lot_index(
        job_id,
        {"multi_lot": True, "lots": [
            {"lot_id": lot_id, "label": f"Lotto {lot_id}"}
            for lot_id in verdicts
        ]},
    )
    artifacts.save_job_status(job_id, {
        "job_id": job_id,
        "analysis_id": analysis_id,
        "status": "REPORT_READY",
        "created_at": "2026-08-14T10:00:00+00:00",
        "analyze_all": True,
        "per_lot_results": [
            {"lot_id": lot_id, "status": "REPORT_READY"}
            for lot_id in verdicts
        ],
    })
    persisted = verdict_model.build_case_verdict(
        list(verdicts.values()),
        scope_id=analysis_id,
        all_lot_ids=list(verdicts),
    )
    artifacts.save_case_verdict(job_id, persisted)

    read_time = workspace.build_workspace(analysis_id)["canonical_verdict"]
    stored = artifacts.read_json(job_id, artifacts.CASE_VERDICT_FILE)
    assert read_time["field_verdicts"] == stored["field_verdicts"]
    assert read_time["severity"] == stored["severity"]
    assert read_time["readiness"] == stored["readiness"]
    assert read_time["unresolved_lot_ids"] == stored["unresolved_lot_ids"]
