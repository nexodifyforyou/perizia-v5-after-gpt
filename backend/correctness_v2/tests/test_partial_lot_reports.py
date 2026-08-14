"""Branch 3 disclosure, 24-case matrix, and hard-block regressions."""

from __future__ import annotations

import copy
import asyncio
import json

import httpx
import pytest
from fastapi import FastAPI, HTTPException

from correctness_v2 import (
    api, artifacts, customer_view, feature_flags, orchestrator, partial_report,
    verdict_model, workspace,
)

from .sample_perizia import (
    MULTI_LOT_PAGES, fake_sequence_caller, make_multilot_worksheet,
    single_lot_worksheet_on_page,
)


@pytest.fixture(autouse=True)
def _partial_enabled(monkeypatch):
    monkeypatch.setenv(feature_flags.FLAG_CANONICAL_VERDICT, "true")
    monkeypatch.setenv(feature_flags.FLAG_PARTIAL_LOT_REPORTS, "true")


def _report(*, lot_id="1", money=False):
    report = {
        "schema_version": "cv2.customer_report.v1",
        "analysis_id": "analysis_partial_matrix",
        "job_id": "cv2_partial_matrix",
        "report_status": "REPORT_READY",
        "title": "Report verificato",
        "subtitle": "Contenuti affidabili",
        "case_identity": {"address": "Via Esempio", "property_type": "Abitazione"},
        "lot_structure": {"selected_lot": str(lot_id), "bene_count": 1},
        "executive_summary": [{"label": "Lotto", "value": str(lot_id)}],
        "key_facts": [{"label": "Tipologia", "value": "Abitazione"}],
        "risk_sections": [{"title": "Rischio", "items": [{"text": "Verifica richiesta"}]}],
        "money_sections": {
            "valuation_chain": [] if money else [
                {"label": "Valore finale", "amount": 81000, "amount_display": "€ 81.000,00"}
            ],
            "auction_terms": [], "buyer_side_costs": [],
            "procedure_cancelled_formalities": [], "uncertain_money": [],
        },
        "beni_sections": [{"bene_id": "A", "description": "Unità immobiliare"}],
        "occupancy_section": {},
        "compliance_section": [{"area": "Urbanistica", "status_label": "Da verificare", "evidence_pages": [7]}],
        "formalities_section": [{"type": "ipoteca", "cancelled_by_procedure": True, "evidence_pages": [8]}],
        "buyer_checklist": [{"action": "Verificare il dato mancante", "status": "required"}],
        "lot_fact_projection": {"kept_fact_ids": ["fact-stable"], "conflicts": []},
    }
    return report


def _gate(*, categories=("occupancy",), codes=None, contradicted=0, conflicts=0):
    codes = tuple(codes or (
        "MISSING_IMPORTANT_MONEY" if category in {"money", "sale_terms"}
        else "CRITICAL_FACT_MISSING"
        for category in categories
    ))
    omissions = []
    issues = []
    facts = []
    for index, (category, code) in enumerate(zip(categories, codes), start=1):
        fact_id = f"fact-{index}"
        role = "base_price" if category in {"money", "sale_terms"} else None
        omission = {
            "fact_id": fact_id, "category": category,
            "document_fact": f"Dato omesso {index}", "evidence_pages": [index + 2],
            "severity": "critical", "match_status": "missing",
            "reason": "Il dato non è presente nel report.",
        }
        if role:
            omission["role"] = role
        omissions.append(omission)
        issues.append({"code": code, "detail": f"Dato omesso {index}", "fact_id": fact_id})
        facts.append({
            **omission, "source": "worksheet", "source_stage": "reconciled",
            "source_fact_ids": [f"source-{index}"],
        })
    return {
        "gate_status": "FAIL",
        "quality_report": {
            "overall_quality_status": "FAIL", "customer_readiness": "NOT_READY",
            "blocking_issues": issues,
        },
        "coverage_audit": {
            "coverage_status": "FAIL", "critical_omissions": omissions,
            "fact_coverage": facts,
            "totals": {"contradicted": contradicted},
            "lot_coverage": {"lot_id": "1", "unresolved_conflicts": conflicts},
        },
    }


def _classify(gate, report=None):
    return partial_report.classify_partial_eligibility(
        gate_status=gate["gate_status"], quality_report=gate["quality_report"],
        coverage_audit=gate["coverage_audit"], report=report or _report(), lot_id="1",
    )


def _project(gate, report=None, canonical=None):
    report = report or _report()
    canonical = canonical or verdict_model.build_lot_verdict(report, lot_id="1")
    return partial_report.build_partial_projection(
        report=report, canonical_verdict=canonical,
        quality_report=gate["quality_report"], coverage_audit=gate["coverage_audit"],
        gate_status=gate["gate_status"], lot_id="1",
    )


# Matrix 1-3: omission-only shapes.
def test_matrix_01_one_missing_critical_non_money_is_partial():
    decision = _classify(_gate())
    assert decision["eligible"] is True
    assert decision["disclosure_state"] == partial_report.PARTIAL_REPORT_AVAILABLE
    assert decision["unresolved_fields"][0]["field"] == "occupancy"


def test_matrix_02_one_missing_important_money_is_partial():
    decision = _classify(_gate(categories=("money",)))
    item = decision["unresolved_fields"][0]
    assert decision["eligible"] is True
    assert item["monetary_role"] == "base_price"
    assert "amount" not in item and "amount_display" not in item


def test_matrix_03_multiple_independently_safe_omissions_are_partial():
    decision = _classify(_gate(categories=("occupancy", "compliance")))
    assert decision["eligible"] is True
    assert [item["source_pages"] for item in decision["unresolved_fields"]] == [[3], [4]]


# Matrix 4-6 plus the binding 10-code hard-block table.
@pytest.mark.parametrize("code", [
    "INVENTED_BUYER_COST", "FAKE_PREZZO_BASE", "MONEY_ROLE_MISMATCH",
    "BENE_LOST", "SECTION_CONTRADICTION", "LOT_CONTAMINATION",
    "SCHEMA_CORRUPTION", "AMBIGUOUS_OWNERSHIP_SCOPE",
    "EXCERPT_NOT_VERBATIM", "FUTURE_UNCLASSIFIED_CODE",
])
def test_full_block_regression_matrix_unknown_and_integrity_codes(code):
    gate = _gate()
    gate["quality_report"]["blocking_issues"].append(
        {"code": code, "detail": "integrity failure"}
    )
    decision = _classify(gate)
    assert decision["eligible"] is False
    assert decision["disclosure_state"] == partial_report.REPORT_BLOCKED
    assert code in decision["accounting"]["hard_blocking_codes"]


def test_matrix_05_fabrication_plus_missing_fact_is_blocked():
    gate = _gate()
    gate["quality_report"]["blocking_issues"].append(
        {"code": "INVENTED_BUYER_COST", "detail": "fabricated"}
    )
    assert _classify(gate)["disclosure_state"] == partial_report.REPORT_BLOCKED


def test_matrix_06_contradiction_plus_missing_fact_is_blocked():
    assert _classify(_gate(contradicted=1))["disclosure_state"] == partial_report.REPORT_BLOCKED


@pytest.mark.parametrize("damage", ["scope", "pages", "source_ids", "money_role"])
def test_allow_list_still_blocks_ambiguous_or_corrupt_structured_evidence(damage):
    gate = _gate(categories=("money",)) if damage == "money_role" else _gate()
    if damage == "scope":
        gate["coverage_audit"]["lot_coverage"]["lot_id"] = "2"
    elif damage == "pages":
        gate["coverage_audit"]["critical_omissions"][0]["evidence_pages"] = "pagina 3"
    elif damage == "source_ids":
        gate["coverage_audit"]["fact_coverage"][0]["source_fact_ids"] = "source-1"
    else:
        gate["coverage_audit"]["critical_omissions"][0].pop("role")
    assert _classify(gate)["disclosure_state"] == partial_report.REPORT_BLOCKED


# Matrix 7-11: content, visibility, money, and readiness invariants.
def test_matrix_07_valid_sections_are_preserved_exactly():
    report = _report()
    before = copy.deepcopy(report)
    projected = _project(_gate(), report=report)
    for key in (
        "case_identity", "lot_structure", "executive_summary", "key_facts",
        "risk_sections", "money_sections", "beni_sections", "occupancy_section",
        "compliance_section", "formalities_section", "buyer_checklist",
        "lot_fact_projection",
    ):
        assert projected["report"][key] == before[key]
    assert report == before


def test_matrix_08_unresolved_field_never_silently_disappears():
    projected = _project(_gate(categories=("occupancy", "compliance")))
    customer = customer_view.sanitize_customer_report(
        projected["report"], {"safe_to_show_customer": True}
    )
    assert len(projected["decision"]["unresolved_fields"]) == 2
    assert len(customer["partial_status"]["unresolved_fields"]) == 2


def test_matrix_09_missing_money_never_acquires_a_substitute():
    report = _report(money=True)
    projected = _project(_gate(categories=("money",)), report=report)
    assert projected["report"]["money_sections"] == report["money_sections"]
    assert not projected["report"]["money_sections"]["valuation_chain"]
    assert "amount" not in projected["report"]["partial_status"]["unresolved_fields"][0]


def test_matrix_10_partial_report_cannot_be_ready():
    projected = _project(_gate())
    customer = customer_view.sanitize_customer_report(
        projected["report"], {"safe_to_show_customer": True}
    )
    assert customer["partial_status"]["full_readiness"] is False
    assert customer["decision_model"]["readiness"]["state"] == "TECHNICAL_REVIEW_REQUIRED"
    assert customer["canonical_verdict"]["severity"] == "grave"
    assert customer["canonical_verdict"]["display_projections"]["esito_level"] == "rosso"
    assert customer["canonical_verdict"]["display_projections"]["decision_level"] == "attenzione"


def test_matrix_11_partial_lot_cannot_inflate_case_readiness():
    projected = _project(_gate())
    clean = verdict_model.build_lot_verdict(_report(lot_id="2"), lot_id="2")
    case = verdict_model.build_case_verdict(
        [projected["canonical_verdict"], clean], scope_id="case", all_lot_ids=["1", "2"]
    )
    assert case["severity"] == "grave"
    assert case["readiness"] == "TECHNICAL_REVIEW_REQUIRED"


# Matrix 12-13: existing full/unsafe states.
def test_matrix_12_existing_full_report_remains_full():
    report = _report()
    customer = customer_view.sanitize_customer_report(report, {"safe_to_show_customer": True})
    assert customer["report_status"] == "REPORT_READY"
    assert customer["disclosure_state"] == partial_report.FULL_REPORT_AVAILABLE
    assert "partial_status" not in customer


def test_matrix_13_existing_unsafe_report_remains_blocked():
    gate = _gate(codes=("MONEY_ROLE_MISMATCH",))
    assert _project(gate) is None


def _save_historical(*, sufficient=True):
    job_id = "cv2_historical_partial"
    analysis_id = "analysis_historical_partial"
    report = _report()
    report.update({"job_id": job_id, "analysis_id": analysis_id})
    gate = _gate(categories=("money",))
    canonical = verdict_model.build_lot_verdict(report, lot_id="1")
    artifacts.save_job_status(job_id, {
        "job_id": job_id, "analysis_id": analysis_id,
        "status": "NEEDS_MANUAL_REVIEW", "safe_to_show_customer": False,
        "customer_report_generated": True, "selected_lot": "1",
        "updated_at": "2026-08-14T10:00:00+00:00", "artifacts_saved": {},
    })
    artifacts.save_customer_report(job_id, report)
    artifacts.save_quality_report(job_id, gate["quality_report"])
    if sufficient:
        artifacts.save_coverage_audit(job_id, gate["coverage_audit"])
    artifacts.save_lot_verdict(job_id, "1", canonical)
    artifacts.save_lot_index(job_id, {"lots": [{
        "lot_id": "1", "address": "SNAPSHOT ERRATO",
        "property_type": "SNAPSHOT ERRATO",
        "occupancy_summary": "SNAPSHOT ERRATO",
    }]})
    return analysis_id, job_id, report


# Matrix 14-18: cached replay and consumer consistency.
def test_matrix_14_cached_historical_eligible_becomes_partial_without_regeneration(
    artifacts_root, monkeypatch
):
    analysis_id, job_id, _ = _save_historical()
    before = {p.relative_to(artifacts.job_dir(job_id)).as_posix(): p.read_bytes()
              for p in artifacts.job_dir(job_id).rglob("*") if p.is_file()}
    monkeypatch.setattr(artifacts, "save_json", lambda *_a, **_k: pytest.fail("write during replay"))
    status, report = workspace.find_lot_safe_report(analysis_id, "1")
    after = {p.relative_to(artifacts.job_dir(job_id)).as_posix(): p.read_bytes()
             for p in artifacts.job_dir(job_id).rglob("*") if p.is_file()}
    assert status["status"] == "PARTIAL_REPORT_AVAILABLE"
    assert report["report_status"] == "PARTIAL_REPORT_AVAILABLE"
    assert before == after


def test_matrix_15_old_cached_report_without_structured_evidence_stays_blocked(artifacts_root):
    analysis_id, _, _ = _save_historical(sufficient=False)
    assert workspace.find_lot_safe_report(analysis_id, "1") == (None, None)


def test_matrix_16_customer_read_path_agrees_on_disclosure_state(artifacts_root):
    analysis_id, _, _ = _save_historical()
    status, report, _ = api._find_customer_job(analysis_id, "1")
    customer = customer_view.sanitize_customer_report(report, status)
    assert customer["disclosure_state"] == partial_report.PARTIAL_REPORT_AVAILABLE
    assert report["report_status"] == status["status"] == "PARTIAL_REPORT_AVAILABLE"


def test_matrix_17_storico_agrees_with_opened_report_and_uses_canonical(artifacts_root):
    analysis_id, _, _ = _save_historical()
    ws = workspace.build_workspace(analysis_id)
    lot = ws["lots"][0]
    status, report = workspace.find_lot_safe_report(analysis_id, "1")
    assert lot["state"] == report["report_status"] == status["status"]
    assert lot["disclosure_state"] == partial_report.PARTIAL_REPORT_AVAILABLE
    assert lot["property_type"] != "SNAPSHOT ERRATO"
    assert lot["address"] == "Via Esempio"
    assert len(lot["partial_status"]["unresolved_fields"]) == 1


def test_matrix_18_checklist_agrees_with_partial_report(artifacts_root):
    analysis_id, _, source = _save_historical()
    status, report = workspace.find_lot_safe_report(analysis_id, "1")
    customer = customer_view.sanitize_customer_report(report, status)
    assert customer["buyer_checklist"] == source["buyer_checklist"]
    assert customer["decision_model"]["readiness"]["state"] == "TECHNICAL_REVIEW_REQUIRED"


def test_blocked_storico_uses_reconciled_verdict_without_exposing_report(artifacts_root):
    analysis_id, job_id, _ = _save_historical()
    quality = artifacts.read_json(job_id, artifacts.QUALITY_REPORT_FILE)
    quality["blocking_issues"].append({
        "code": "LOT_CONTAMINATION", "detail": "integrity failure"
    })
    artifacts.save_quality_report(job_id, quality)
    ws = workspace.build_workspace(analysis_id)
    lot = ws["lots"][0]
    assert lot["disclosure_state"] == partial_report.REPORT_BLOCKED
    assert lot["has_safe_report"] is False
    assert lot["property_type"] != "SNAPSHOT ERRATO"
    assert lot["address"] == "Via Esempio"
    assert workspace.find_lot_safe_report(analysis_id, "1") == (None, None)
    assert ws["canonical_verdict"]["severity"] == "grave"


# Matrix 19-24: semantic immutability and no-side-effect accounting.
def test_matrix_19_classifier_does_not_change_canonical_verdict():
    report = _report()
    canonical = verdict_model.build_lot_verdict(report, lot_id="1")
    before = copy.deepcopy(canonical)
    _classify(_gate(), report)
    assert canonical == before


def test_matrix_20_branch1_facts_and_provenance_are_unchanged():
    report = _report()
    before = copy.deepcopy(report["lot_fact_projection"])
    projected = _project(_gate(), report=report)
    assert report["lot_fact_projection"] == before
    assert projected["report"]["lot_fact_projection"] == before


def test_matrix_21_branch2_semantic_input_is_unchanged_and_disclosure_is_separate():
    report = _report()
    canonical = verdict_model.build_lot_verdict(report, lot_id="1")
    before = copy.deepcopy(canonical)
    projected = _project(_gate(), report=report, canonical=canonical)
    assert canonical == before
    assert "disclosure_state" not in canonical
    assert projected["report"]["disclosure_state"] == "PARTIAL_REPORT_AVAILABLE"
    assert projected["canonical_verdict"]["report_status"] == "NEEDS_MANUAL_REVIEW"
    assert "disclosure_state" not in projected["canonical_verdict"]


def test_matrix_22_projection_needs_no_model_or_external_api(monkeypatch):
    import correctness_v2.analyst as analyst
    monkeypatch.setattr(analyst, "run_analyst", lambda *_a, **_k: pytest.fail("model called"))
    assert _project(_gate()) is not None


def test_matrix_23_projection_consumes_no_credit_or_beta_state(monkeypatch):
    monkeypatch.setattr(api, "_lot_credit_preview", lambda *_a, **_k: pytest.fail("credit read"))
    assert _project(_gate()) is not None


def test_matrix_24_projection_performs_no_production_or_artifact_write(monkeypatch):
    monkeypatch.setattr(artifacts, "save_json", lambda *_a, **_k: pytest.fail("artifact write"))
    assert _project(_gate()) is not None


def test_flag_off_is_legacy_safe_and_canonical_off_disables_partial(monkeypatch):
    report = _report()
    gate = _gate()
    canonical = verdict_model.build_lot_verdict(report, lot_id="1")
    monkeypatch.setenv(feature_flags.FLAG_PARTIAL_LOT_REPORTS, "false")
    assert _project(gate, report, canonical) is None
    assert "disclosure_state" not in customer_view.sanitize_customer_report(
        report, {"safe_to_show_customer": True}
    )
    monkeypatch.setenv(feature_flags.FLAG_PARTIAL_LOT_REPORTS, "true")
    monkeypatch.setenv(feature_flags.FLAG_CANONICAL_VERDICT, "false")
    assert partial_report.build_partial_projection(
        report=report, canonical_verdict=canonical,
        quality_report=gate["quality_report"], coverage_audit=gate["coverage_audit"],
        gate_status="FAIL", lot_id="1",
    ) is None


def test_single_lot_finish_path_persists_partial_with_safe_accounting(artifacts_root):
    report = _report()
    gate = _gate()
    gate["customer_report"] = report
    gate["scorecard"] = {"overall_score": 30}
    canonical = verdict_model.build_lot_verdict(report, lot_id="1")
    artifacts.save_lot_verdict(report["job_id"], "1", canonical)
    artifacts.save_case_verdict(
        report["job_id"], verdict_model.build_case_verdict(
            [canonical], scope_id=report["analysis_id"], all_lot_ids=["1"]
        )
    )
    status = orchestrator._finish_quality_gate_failed(
        report["job_id"], report["analysis_id"], gate, {},
        "2026-08-14T10:00:00+00:00", False,
    )
    stored = artifacts.read_json(report["job_id"], artifacts.CUSTOMER_REPORT_FILE)
    assert status["status"] == "PARTIAL_REPORT_AVAILABLE"
    assert status["safe_to_show_customer"] is True
    assert status["gate_outcome"] == "FAIL"
    assert status["hard_blocking_codes"] == []
    assert stored["report_status"] == "PARTIAL_REPORT_AVAILABLE"
    assert stored["canonical_verdict"]["severity"] == "grave"


def test_partial_rerun_preserves_other_lots_in_persisted_case_verdict(
    artifacts_root,
):
    job_id = "cv2_partial_rerun_three_lots"
    analysis_id = "analysis_partial_rerun_three_lots"
    verdicts = []
    reports = {}
    for lot_id in ("1", "2", "3"):
        report = _report(lot_id=lot_id)
        report.update({"job_id": job_id, "analysis_id": analysis_id})
        report["case_identity"] = {
            "address": f"Via Lotto {lot_id}",
            "property_type": f"Tipologia reale {lot_id}",
        }
        reports[lot_id] = report
        verdict = verdict_model.build_lot_verdict(report, lot_id=lot_id)
        verdicts.append(verdict)
        artifacts.save_lot_verdict(job_id, lot_id, verdict)

    original_case = verdict_model.build_case_verdict(
        verdicts, scope_id=analysis_id, all_lot_ids=["1", "2", "3"]
    )
    artifacts.save_case_verdict(job_id, original_case)
    original_per_lot = copy.deepcopy(original_case["field_verdicts"]["per_lot"])

    gate = _gate(categories=("money",))
    gate["coverage_audit"]["lot_coverage"]["lot_id"] = "2"
    gate["customer_report"] = reports["2"]
    gate["scorecard"] = {"overall_score": 30}
    status = orchestrator._finish_quality_gate_failed(
        job_id, analysis_id, gate, {}, "2026-08-14T10:00:00+00:00", False,
    )

    repaired_case = artifacts.read_json(job_id, artifacts.CASE_VERDICT_FILE)
    repaired_per_lot = repaired_case["field_verdicts"]["per_lot"]
    assert status["status"] == "PARTIAL_REPORT_AVAILABLE"
    assert repaired_per_lot["1"] == original_per_lot["1"]
    assert repaired_per_lot["3"] == original_per_lot["3"]
    assert repaired_per_lot["1"].get("status") != "UNKNOWN"
    assert repaired_per_lot["3"].get("status") != "UNKNOWN"
    rerun_verdict = artifacts.read_json(
        job_id, "lots/2/canonical_verdict.json"
    )
    assert repaired_per_lot["2"] == rerun_verdict["field_verdicts"]
    assert rerun_verdict["report_status"] == "NEEDS_MANUAL_REVIEW"
    assert repaired_case["severity"] == "grave"
    assert repaired_case["readiness"] == "TECHNICAL_REVIEW_REQUIRED"


def test_case_reconstruction_falls_back_to_existing_real_per_lot_entry(
    artifacts_root,
):
    current = verdict_model.build_lot_verdict(_report(lot_id="1"), lot_id="1")
    missing_file_verdict = verdict_model.build_lot_verdict(
        _report(lot_id="2"), lot_id="2"
    )
    existing_case = verdict_model.build_case_verdict(
        [current, missing_file_verdict], all_lot_ids=["1", "2"]
    )
    inputs, lot_ids = orchestrator._reconstruct_case_lot_verdicts(
        job_id="cv2_missing_sister_file",
        current_lot_id="1",
        current_verdict=current,
        existing_case=existing_case,
    )
    rebuilt = verdict_model.build_case_verdict(inputs, all_lot_ids=lot_ids)
    assert rebuilt["field_verdicts"]["per_lot"]["2"] == (
        existing_case["field_verdicts"]["per_lot"]["2"]
    )
    assert rebuilt["field_verdicts"]["per_lot"]["2"].get("status") != "UNKNOWN"


def test_single_lot_finish_path_flag_off_is_unchanged_block(monkeypatch, artifacts_root):
    monkeypatch.setenv(feature_flags.FLAG_PARTIAL_LOT_REPORTS, "false")
    report = _report()
    gate = _gate()
    gate["customer_report"] = report
    gate["scorecard"] = {"overall_score": 30}
    status = orchestrator._finish_quality_gate_failed(
        report["job_id"], report["analysis_id"], gate, {},
        "2026-08-14T10:00:00+00:00", False,
    )
    assert status["status"] == "NEEDS_MANUAL_REVIEW"
    assert status["safe_to_show_customer"] is False


def test_flag_off_cached_workspace_keeps_legacy_blocked_projection(
    monkeypatch, artifacts_root
):
    analysis_id, _, _ = _save_historical()
    monkeypatch.setenv(feature_flags.FLAG_PARTIAL_LOT_REPORTS, "false")
    lot = workspace.build_workspace(analysis_id)["lots"][0]
    assert lot["state"] == "VERIFICATION_REQUIRED"
    assert lot["has_safe_report"] is False
    assert lot["address"] == "SNAPSHOT ERRATO"
    assert "disclosure_state" not in lot
    assert workspace.find_lot_safe_report(analysis_id, "1") == (None, None)


def test_analyze_all_inline_gate_fail_exposes_only_eligible_lot_partial(
    monkeypatch, artifacts_root
):
    original_gate = orchestrator.quality_gate_mod.run_quality_gate

    def force_one_omission(**kwargs):
        result = original_gate(**kwargs)
        if str(kwargs.get("lot_id")) == "2":
            synthetic = _gate(categories=("money",))
            synthetic["coverage_audit"]["lot_coverage"]["lot_id"] = "2"
            result.update(synthetic)
        return result

    monkeypatch.setattr(orchestrator.quality_gate_mod, "run_quality_gate", force_one_omission)
    caller = fake_sequence_caller([
        make_multilot_worksheet(),
        single_lot_worksheet_on_page(2, "1"),
        single_lot_worksheet_on_page(4, "2"),
    ])
    status = orchestrator.start_job(
        "analysis_inline_partial", lambda _analysis_id: MULTI_LOT_PAGES,
        is_admin=True, openai_caller=caller, analyze_all=True,
    )
    by_lot = {entry["lot_id"]: entry for entry in status["per_lot_results"]}
    assert status["status"] == "NEEDS_MANUAL_REVIEW"
    assert status["safe_to_show_customer"] is False
    assert status["all_lots_ready"] is False
    assert status["disclosure_state"] == "PARTIAL_REPORT_AVAILABLE"
    assert by_lot["1"]["status"] == "REPORT_READY"
    assert by_lot["2"]["status"] == "PARTIAL_REPORT_AVAILABLE"
    assert by_lot["2"]["quality_gate_status"] == "FAIL"
    report = artifacts.read_json(
        status["job_id"], "lots/2/customer_report.json"
    )
    assert report["report_status"] == "PARTIAL_REPORT_AVAILABLE"
    assert report["canonical_verdict"]["severity"] == "grave"
    case = artifacts.read_json(status["job_id"], artifacts.CASE_VERDICT_FILE)
    assert case["severity"] == "grave"
    assert case["readiness"] == "TECHNICAL_REVIEW_REQUIRED"
    lot_status, opened = workspace.find_lot_safe_report(
        "analysis_inline_partial", "2"
    )
    assert lot_status["status"] == opened["report_status"] == "PARTIAL_REPORT_AVAILABLE"


def test_customer_projection_contains_no_raw_internal_enums():
    projected = _project(_gate(categories=("money",)))
    customer = customer_view.sanitize_customer_report(
        projected["report"], {"safe_to_show_customer": True}
    )
    text = json.dumps(customer["partial_status"], ensure_ascii=False)
    for token in (
        "MISSING_IMPORTANT_MONEY", "CRITICAL_FACT_MISSING", "critical",
        "TECHNICAL_REVIEW_REQUIRED", "base_price", "reconciled",
    ):
        assert token not in text


def test_partial_customer_route_preserves_owner_gate_before_artifact_lookup(monkeypatch):
    async def deny(_request, _analysis_id):
        raise HTTPException(status_code=404, detail="Analysis not found")

    def forbidden(*_args, **_kwargs):
        pytest.fail("artifact lookup ran before ownership check")

    monkeypatch.setattr(api, "_resolve_customer_access", deny)
    monkeypatch.setattr(api, "_find_customer_job", forbidden)
    app = FastAPI()
    app.include_router(api.router, prefix="/api")

    async def request():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get(
                "/api/analysis/perizia/not-owned/correctness-v2/customer-view/latest"
            )

    response = asyncio.run(request())
    assert response.status_code == 404
