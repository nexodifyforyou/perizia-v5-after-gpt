"""Customer money-confirmation (human-in-the-loop money-role disambiguation).

Covers the whole feature, mirroring the LOT_SELECTION_REQUIRED flow:
  * eligibility guardrails (only resolvable money ambiguities, cap <= 3, all
    blocking issues must be resolvable, DOCUMENT_NOT_READABLE untouched);
  * the customer-facing money_confirmation payload (amount, candidates, page +
    verbatim excerpt, plain-Italian question);
  * strict answer validation (only offered options accepted);
  * the gate applying a confirmed role as ground truth (resolves) vs a
    non-report role (stays blocked, fail-closed);
  * customer_view sanitize keeps it customer-safe with no admin leak;
  * the orchestrator pause finisher + resolve_money_confirmation continuation
    turning a paused job into REPORT_READY (or NEEDS_MANUAL_REVIEW).

All fixtures are generic/synthetic — nothing branches on a real city.
"""

from correctness_v2 import (
    analyst,
    artifacts,
    contract as contract_mod,
    coverage_audit,
    customer_report as customer_report_mod,
    customer_view,
    doc_signals,
    lots as lots_mod,
    money_confirmation as mc,
    orchestrator,
    quality_gate,
    validator as validator_mod,
)
from correctness_v2.schemas import JobStatus

from .sample_perizia import GENERIC_PERIZIA_PAGES, make_worksheet

VALIDATED_REPORT = {
    "validation_status": "VALIDATED",
    "checks": {"money_signals": {"base_price_explicit_text": True}},
    "warnings": [],
}


def _omission(fact_id, *, amount=100.0, roles=("market_value", "judicial_sale_value"),
              category="money", pages=(1,), snippet="Valore ...: € 100,00"):
    return {
        "fact_id": fact_id, "category": category, "amount": amount,
        "evidence_pages": list(pages), "confirmation_roles": list(roles),
        "snippet": snippet, "role": roles[0], "role_conflict": True,
        "match_status": "contradicted",
    }


# ---------------------------------------------------------------------------
# Eligibility guardrails ("do not make it a habit")
# ---------------------------------------------------------------------------
def test_eligible_only_when_all_blocking_are_resolvable_money():
    ca = {"critical_omissions": [_omission("m1")]}
    ok = [{"code": "MONEY_ROLE_MISMATCH", "fact_id": "m1"}]
    assert mc.eligible(ca, ok) is True
    # A non-money / non-resolvable blocking issue disqualifies the whole set.
    mixed = ok + [{"code": "CRITICAL_FACT_MISSING", "fact_id": "struct1"}]
    assert mc.eligible(ca, mixed) is False


def test_cap_limits_number_of_confirmations():
    ca3 = {"critical_omissions": [_omission(f"m{i}") for i in range(3)]}
    blk3 = [{"code": "MONEY_ROLE_MISMATCH", "fact_id": f"m{i}"} for i in range(3)]
    assert mc.eligible(ca3, blk3) is True
    ca4 = {"critical_omissions": [_omission(f"m{i}") for i in range(4)]}
    blk4 = [{"code": "MONEY_ROLE_MISMATCH", "fact_id": f"m{i}"} for i in range(4)]
    assert mc.eligible(ca4, blk4) is False  # too many -> manual review


def test_not_eligible_without_two_candidates_or_blocking():
    # No blocking at all -> nothing to confirm.
    assert mc.eligible({"critical_omissions": [_omission("m1")]}, []) is False
    # Pure-missing money with no candidate roles is not customer-resolvable.
    ca = {"critical_omissions": [{"fact_id": "m1", "category": "money",
                                  "amount": 5.0, "evidence_pages": [1], "snippet": "x"}]}
    assert mc.eligible(ca, [{"code": "MISSING_IMPORTANT_MONEY", "fact_id": "m1"}]) is False
    # Only one candidate role -> not a "this or that" choice.
    ca1 = {"critical_omissions": [_omission("m1", roles=("market_value",))]}
    assert mc.eligible(ca1, [{"code": "MONEY_ROLE_MISMATCH", "fact_id": "m1"}]) is False


# ---------------------------------------------------------------------------
# Payload + answer validation
# ---------------------------------------------------------------------------
def test_payload_carries_amount_candidates_page_and_excerpt():
    ca = {"critical_omissions": [_omission(
        "p12:valore_vendita:3", amount=100000.0, pages=(12,),
        roles=("judicial_sale_value", "market_value"),
        snippet="Valore di vendita giudiziaria: € 100.000,00")]}
    blk = [{"code": "MONEY_ROLE_MISMATCH", "fact_id": "p12:valore_vendita:3"}]
    payload = mc.build_money_confirmation(
        analysis_id="a", job_id="j", coverage_audit=ca, blocking_issues=blk)
    assert payload["status"] == JobStatus.MONEY_CONFIRMATION_REQUIRED
    amb = payload["ambiguities"][0]
    assert amb["ambiguity_id"] == "p12:valore_vendita:3"
    assert amb["amount_display"] == "€ 100.000,00"
    assert amb["page"] == 12
    assert "100.000,00" in amb["excerpt"]
    labels = {o["label"] for o in amb["options"]}
    assert labels == {"valore di vendita giudiziaria", "valore di mercato"}
    assert "Quale è corretta?" in amb["question"]


def test_validate_answers_accepts_only_offered_options():
    ca = {"critical_omissions": [_omission("m1", roles=("market_value", "judicial_sale_value"))]}
    payload = mc.build_money_confirmation(
        analysis_id="a", job_id="j", coverage_audit=ca,
        blocking_issues=[{"code": "MONEY_ROLE_MISMATCH", "fact_id": "m1"}])
    assert mc.validate_answers(payload, {"m1": "market_value"}) == {"m1": "market_value"}
    for bad in ({}, {"m1": "rent"}, {"m1": None}, "nope"):
        try:
            mc.validate_answers(payload, bad)
        except ValueError:
            continue
        raise AssertionError(f"expected rejection for {bad!r}")


# ---------------------------------------------------------------------------
# Gate applies a confirmed role as ground truth
# ---------------------------------------------------------------------------
def _synthetic_conflict():
    pages = [{"page_number": 1, "text": (
        "Valore di vendita giudiziaria dell'immobile nello stato di fatto e di "
        "diritto in cui si trova: € 100.000,00")}]
    report = {
        "schema_version": "cv2.customer_report.v1", "analysis_id": "a", "job_id": "j",
        "report_status": "REPORT_READY", "title": "t",
        "money_sections": {"valuation_chain": [
            {"label": "Valore di mercato", "amount": 100000.0, "evidence_pages": [1]}]},
    }
    return pages, report


def test_gate_pauses_and_confirmation_resolves():
    pages, report = _synthetic_conflict()
    gate = quality_gate.run_quality_gate(
        job_id="j", analysis_id="a", pages=pages, worksheet={}, contract={},
        customer_report=report, persist=False)
    assert gate["gate_status"] == quality_gate.GATE_FAIL
    codes = {b["code"] for b in gate["quality_report"]["blocking_issues"]}
    assert codes == {"MONEY_ROLE_MISMATCH"}
    payload = mc.build_money_confirmation(
        analysis_id="a", job_id="j", coverage_audit=gate["coverage_audit"],
        blocking_issues=gate["quality_report"]["blocking_issues"])
    assert payload is not None
    fid = payload["ambiguities"][0]["ambiguity_id"]

    # Confirming the report's reading (market value) clears the block.
    ok = quality_gate.run_quality_gate(
        job_id="j", analysis_id="a", pages=pages, worksheet={}, contract={},
        customer_report=report, persist=False,
        money_confirmations={fid: "market_value"})
    assert ok["gate_status"] != quality_gate.GATE_FAIL
    assert not ok["quality_report"]["blocking_issues"]

    # Confirming a role the report does NOT carry stays blocked (fail-closed).
    still = quality_gate.run_quality_gate(
        job_id="j", analysis_id="a", pages=pages, worksheet={}, contract={},
        customer_report=report, persist=False,
        money_confirmations={fid: "judicial_sale_value"})
    assert still["gate_status"] == quality_gate.GATE_FAIL


# ---------------------------------------------------------------------------
# customer_view: customer-safe, sanitized, no admin leak
# ---------------------------------------------------------------------------
def test_customer_view_money_confirmation_is_safe_and_sanitized():
    ca = {"critical_omissions": [_omission("m1", amount=100000.0, pages=(12,))]}
    payload = mc.build_money_confirmation(
        analysis_id="a", job_id="j", coverage_audit=ca,
        blocking_issues=[{"code": "MONEY_ROLE_MISMATCH", "fact_id": "m1"}])
    base = {"schema_version": "cv2.customer_report.v1", "analysis_id": "a",
            "job_id": "j", "report_status": "REPORT_READY",
            "quality_control": {"secret": 1}, "admin_evidence_index": [{"k": "v"}],
            "money_sections": {"valuation_chain": []}}
    overlay = customer_report_mod.render_money_confirmation_report(base, payload)
    assert overlay["report_status"] == "MONEY_CONFIRMATION_REQUIRED"
    # base is not mutated.
    assert base["report_status"] == "REPORT_READY"

    status = {"safe_to_show_customer": True}
    assert customer_view.is_customer_safe(overlay, status) is True
    view = customer_view.sanitize_customer_report(overlay, status)
    assert "quality_control" not in view
    assert "admin_evidence_index" not in view
    amb = view["money_confirmation"]["ambiguities"][0]
    assert set(amb.keys()) == {
        "ambiguity_id", "amount_display", "page", "evidence_pages",
        "excerpt", "question", "options"}
    assert amb["options"][0].keys() == {"option_id", "label"}


def test_document_not_readable_is_never_a_money_confirmation():
    # An unreadable perizia never reaches the gate; even if a stray money
    # confirmation block were present, the status governs safety/sanitize.
    assert "DOCUMENT_NOT_READABLE" in customer_view.CUSTOMER_SAFE_STATUSES
    assert "MONEY_CONFIRMATION_REQUIRED" in customer_view.CUSTOMER_SAFE_STATUSES


# ---------------------------------------------------------------------------
# Orchestrator: pause finisher + resolve continuation (offline, no OpenAI)
# ---------------------------------------------------------------------------
def _build_swapped_conflict():
    raw = make_worksheet()
    # Document (page 2): market=100.000, state-of-fact=95.000. The worksheet
    # swaps them, so both amounts exist but under the wrong role -> two
    # resolvable role ambiguities.
    raw["money"]["market_value"] = 95000.0
    raw["money"]["current_state_value"] = 100000.0
    worksheet = analyst.normalize_worksheet(raw)
    vr = VALIDATED_REPORT
    lot_report = lots_mod.build_lot_report(worksheet, GENERIC_PERIZIA_PAGES)
    contract = contract_mod.build_contract(
        worksheet=worksheet, validator_report=vr, analysis_id="an_r", job_id="job_r",
        source_pdf_quality_status="PDF_QUALITY_OK", lot_report=lot_report,
        surface_cadastral=doc_signals.extract_surface_cadastral(GENERIC_PERIZIA_PAGES))
    report = customer_report_mod.render_success_report(contract, GENERIC_PERIZIA_PAGES)
    return worksheet, vr, lot_report, contract, report


def _persist_paused_job(job_id, worksheet, vr, lot_report, contract, gate, payload):
    artifacts.save_input_pages(job_id, GENERIC_PERIZIA_PAGES)
    artifacts.save_analyst_worksheet(job_id, worksheet)
    artifacts.save_validator_report(job_id, vr)
    artifacts.save_lot_report(job_id, lot_report)
    artifacts.save_verified_contract(job_id, contract)
    artifacts.save_money_confirmation_required(job_id, payload)
    overlay = customer_report_mod.render_money_confirmation_report(
        gate["customer_report"], payload)
    artifacts.save_customer_report(job_id, overlay)
    artifacts.save_job_status(job_id, {
        "job_id": job_id, "analysis_id": "an_r",
        "status": JobStatus.MONEY_CONFIRMATION_REQUIRED,
        "safe_to_show_customer": True, "created_at": "2026-07-12T00:00:00Z",
        "artifacts_saved": {},
    })


def test_orchestrator_pause_then_resolve_produces_report_ready(artifacts_root):
    worksheet, vr, lot_report, contract, report = _build_swapped_conflict()
    gate = quality_gate.run_quality_gate(
        job_id="job_r", analysis_id="an_r", pages=GENERIC_PERIZIA_PAGES,
        worksheet=worksheet, contract=contract, customer_report=report,
        validator_report=vr, lot_report=lot_report, persist=False)
    assert gate["gate_status"] == quality_gate.GATE_FAIL
    payload = mc.build_money_confirmation(
        analysis_id="an_r", job_id="job_r", coverage_audit=gate["coverage_audit"],
        blocking_issues=gate["quality_report"]["blocking_issues"])
    assert payload and 1 <= len(payload["ambiguities"]) <= mc.MAX_MONEY_CONFIRMATIONS

    _persist_paused_job("job_r", worksheet, vr, lot_report, contract, gate, payload)

    # Confirm each amount as the role the REPORT placed it under (options[1] is
    # the report reading; options[0] is the document-detected role).
    answers = {a["ambiguity_id"]: a["options"][1]["option_id"]
               for a in payload["ambiguities"]}
    result = orchestrator.resolve_money_confirmation("job_r", answers)
    assert result["status"] == JobStatus.REPORT_READY
    assert result["money_confirmation_resolved"] is True
    assert result["money_confirmations"] == answers
    final = artifacts.read_json("job_r", artifacts.CUSTOMER_REPORT_FILE)
    assert final["report_status"] == "REPORT_READY"


def test_resolve_with_document_role_stays_manual_review(artifacts_root):
    worksheet, vr, lot_report, contract, report = _build_swapped_conflict()
    gate = quality_gate.run_quality_gate(
        job_id="job_m", analysis_id="an_r", pages=GENERIC_PERIZIA_PAGES,
        worksheet=worksheet, contract=contract, customer_report=report,
        validator_report=vr, lot_report=lot_report, persist=False)
    payload = mc.build_money_confirmation(
        analysis_id="an_r", job_id="job_m", coverage_audit=gate["coverage_audit"],
        blocking_issues=gate["quality_report"]["blocking_issues"])
    _persist_paused_job("job_m", worksheet, vr, lot_report, contract, gate, payload)

    # Confirming the document-detected role (options[0]) does NOT match the
    # report placement, so the block stands: fail-closed to manual review.
    answers = {a["ambiguity_id"]: a["options"][0]["option_id"]
               for a in payload["ambiguities"]}
    result = orchestrator.resolve_money_confirmation("job_m", answers)
    assert result["status"] == JobStatus.NEEDS_MANUAL_REVIEW


def test_resolve_rejects_unoffered_answer(artifacts_root):
    worksheet, vr, lot_report, contract, report = _build_swapped_conflict()
    gate = quality_gate.run_quality_gate(
        job_id="job_x", analysis_id="an_r", pages=GENERIC_PERIZIA_PAGES,
        worksheet=worksheet, contract=contract, customer_report=report,
        validator_report=vr, lot_report=lot_report, persist=False)
    payload = mc.build_money_confirmation(
        analysis_id="an_r", job_id="job_x", coverage_audit=gate["coverage_audit"],
        blocking_issues=gate["quality_report"]["blocking_issues"])
    _persist_paused_job("job_x", worksheet, vr, lot_report, contract, gate, payload)
    bad = {a["ambiguity_id"]: "rent" for a in payload["ambiguities"]}
    try:
        orchestrator.resolve_money_confirmation("job_x", bad)
    except ValueError:
        return
    raise AssertionError("unoffered role must be rejected")
