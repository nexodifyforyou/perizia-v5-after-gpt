"""Regression + invariant tests for the coverage-only hard-block hotfix.

Root cause: a depressed covered-ratio (from legitimately-"parziale"
subordinate/detail money rows) drove ``coverage_status=FAIL`` even with zero
critical omissions and zero blocking issues, which hard-failed the gate and
forced REPORT_BLOCKED on a trustworthy report.

These tests pin the producer repair (coverage_audit._state + explainability,
quality_gate gate logic) and the non-negotiable invariants, and prove the
hard-block matrix still blocks.
"""

from __future__ import annotations

import copy

from correctness_v2 import (
    coverage_audit as c,
    doc_signals,
    partial_report as pr,
    quality_gate as qg,
)

from . import fresh_case_fixture as f


def _gate(inp):
    return qg.run_quality_gate(persist=False, **inp)


def _classify(gate, report, lot_id="1"):
    return pr.classify_partial_eligibility(
        gate_status=gate["gate_status"],
        quality_report=gate["quality_report"],
        coverage_audit=gate["coverage_audit"],
        report=report,
        lot_id=lot_id,
    )


# ---------------------------------------------------------------------------
# _state unit behaviour (the exact producer knob)
# ---------------------------------------------------------------------------
def test_state_low_ratio_without_critical_missing_is_warning_not_fail():
    # The over-block bug: a sub-0.60 ratio with NO critical missing fact.
    assert c._state(0.30, critical_missing=False) == c.STATUS_WARNING
    assert c._state(0.59, critical_missing=False) == c.STATUS_WARNING


def test_state_critical_missing_still_fails():
    assert c._state(0.99, critical_missing=True) == c.STATUS_FAIL
    assert c._state(0.30, critical_missing=True) == c.STATUS_FAIL


def test_state_bands_preserved_for_non_critical():
    assert c._state(0.70) == c.STATUS_WARNING
    assert c._state(0.90) == c.STATUS_PASS


# ---------------------------------------------------------------------------
# Fresh production shape: valid report + warnings, never BLOCKED
# ---------------------------------------------------------------------------
def test_fresh_case_no_longer_coverage_fails():
    for caution in (False, True):
        inp = f.build_inputs("1", with_caution=caution)
        audit, _page = c.build_coverage_audit(**inp)
        assert audit["coverage_status"] == c.STATUS_WARNING
        assert audit["critical_omissions"] == []
        # subordinate/detail incompleteness depresses the ratio ...
        assert audit["lot_coverage"]["general_fact_recall"] < 0.60
        # ... but every DECISIVE (critical) value is present.
        assert audit["lot_coverage"]["critical_fact_recall"] == 1.0
        # not a FAIL -> no structured failure reason recorded.
        assert audit["coverage_failure_reasons"] == []


def test_fresh_case_disclosure_is_ready_with_warnings_not_blocked():
    inp = f.build_inputs("1", with_caution=True)
    gate = _gate(inp)
    assert gate["gate_status"] == qg.GATE_WARNING
    q = gate["quality_report"]
    assert q["overall_quality_status"] == "PASS_WITH_WARNINGS"
    assert q["customer_readiness"] == "READY_WITH_WARNINGS"
    assert q["final_decision"] == "APPROVE_WITH_WARNINGS"
    assert q["blocking_issues"] == []
    # The non-critical money-role caution is visible as a warning, not a block.
    assert len(gate["coverage_audit"]["important_warnings"]) == 1

    # Disclosure route (gate != FAIL): the report is shown with a structural
    # "coverage_incomplete" marker so the esito is NOT clean-green.
    accounting = pr.full_disclosure_accounting(
        gate["gate_status"], gate["coverage_audit"].get("coverage_status")
    )
    assert accounting["disclosure_state"] == pr.FULL_REPORT_AVAILABLE
    assert accounting["coverage_incomplete"] is True
    assert accounting["coverage_incomplete_reason"]


def test_fresh_case_partial_classifier_never_grants_on_empty_codes():
    # classify_partial_eligibility only runs on a FAILED gate; here the gate is
    # WARNING, so it must NOT be reached as a disclosure path. Even if invoked,
    # it must fail closed (never auto-grant partial from empty codes).
    inp = f.build_inputs("1", with_caution=True)
    gate = _gate(inp)
    decision = _classify(gate, inp["customer_report"])
    assert decision["eligible"] is False
    assert decision["disclosure_state"] == pr.REPORT_BLOCKED
    assert decision["reason"] == "gate_not_failed"


# ---------------------------------------------------------------------------
# Invariant: subordinate/detail incompleteness != critical omission
# ---------------------------------------------------------------------------
def test_detail_incompleteness_is_not_a_critical_omission():
    inp = f.build_inputs("1", with_caution=False)
    audit, _page = c.build_coverage_audit(**inp)
    # The seven detail facts are absent, yet none is a critical omission.
    assert audit["critical_omissions"] == []
    lc = audit["lot_coverage"]
    assert lc["expected_material_facts"] > lc["customer_visible_facts"]
    assert lc["critical_fact_recall"] == 1.0


# ---------------------------------------------------------------------------
# Invariant: coverage_status == FAIL => explicit structured reason
# ---------------------------------------------------------------------------
def _make_critical_missing_inputs():
    """Drop a DECISIVE value from the report so a critical ledger fact is truly
    missing -> a genuine critical coverage failure that must still FAIL."""
    inp = f.build_inputs("1", with_caution=False)
    report = copy.deepcopy(inp["customer_report"])
    # Remove the market-value row (decisive/critical) from the report.
    report["money_sections"]["valuation_chain"] = [
        row for row in report["money_sections"]["valuation_chain"]
        if row["label"] != "Valore di mercato"
    ]
    report["key_facts"] = []
    inp["customer_report"] = report
    # Also remove it from the worksheet so it is not a worksheet omission here;
    # the point is the LEDGER critical fact missing from the customer report.
    inp["worksheet"]["money"].pop("market_value", None)
    return inp


def test_genuine_critical_coverage_failure_still_fails_and_is_explained():
    inp = _make_critical_missing_inputs()
    audit, _page = c.build_coverage_audit(**inp)
    assert audit["coverage_status"] == c.STATUS_FAIL
    assert audit["lot_coverage"]["critical_fact_recall"] < 1.0
    # The FAIL is explainable (never an unexplained aggregate ratio block).
    assert audit["coverage_failure_reasons"]
    assert all(
        r["code"] in (
            c.COVERAGE_FAIL_CRITICAL_OMISSION, c.COVERAGE_FAIL_CRITICAL_COVERAGE
        )
        for r in audit["coverage_failure_reasons"]
    )


def test_coverage_fail_invariant_holds_across_fixture_shapes():
    """coverage_status == FAIL  =>  (critical_omissions OR coverage_failure_reasons)."""
    shapes = [
        f.build_inputs("1", with_caution=False),
        f.build_inputs("1", with_caution=True),
        f.build_inputs("2", high_coverage=True),
        _make_critical_missing_inputs(),
    ]
    for inp in shapes:
        audit, _page = c.build_coverage_audit(**inp)
        if audit["coverage_status"] == c.STATUS_FAIL:
            assert audit["critical_omissions"] or audit["coverage_failure_reasons"], (
                "coverage FAIL without any structured reason"
            )


# ---------------------------------------------------------------------------
# Invariant: REPORT_BLOCKED => an explicit reason exists
# ---------------------------------------------------------------------------
def test_report_blocked_always_carries_explicit_reason():
    inp = _make_critical_missing_inputs()
    gate = _gate(inp)
    decision = _classify(gate, inp["customer_report"])
    if decision["disclosure_state"] == pr.REPORT_BLOCKED:
        assert decision["reason"], "REPORT_BLOCKED with no reason"


# ---------------------------------------------------------------------------
# Multi-lot isolation: one lot's degradation must not affect a sibling
# ---------------------------------------------------------------------------
def test_multi_lot_coverage_isolation():
    degraded = c.build_coverage_audit(**f.build_inputs("1", with_caution=True))[0]
    clean = c.build_coverage_audit(**f.build_inputs("2", high_coverage=True))[0]
    # Degraded lot -> WARNING (not FAIL, not clean PASS).
    assert degraded["coverage_status"] == c.STATUS_WARNING
    # Clean sibling -> PASS, unaffected by lot 1's low ratio.
    assert clean["coverage_status"] == c.STATUS_PASS
    assert clean["lot_coverage"]["general_fact_recall"] >= 0.85
    # Independent lot ids: no cross-lot leakage of coverage facts.
    assert degraded["lot_coverage"]["lot_id"] == "1"
    assert clean["lot_coverage"]["lot_id"] == "2"


# ---------------------------------------------------------------------------
# Hard-block regression: genuine violations must STILL block
# ---------------------------------------------------------------------------
def test_fake_prezzo_base_still_hard_blocks():
    # Drop the explicit-base-price validator support -> FAKE_PREZZO_BASE fires.
    inp = f.build_inputs("1", with_caution=False)
    inp["validator_report"] = {"validation_status": "OK", "checks": {}}
    gate = _gate(inp)
    codes = {b["code"] for b in gate["quality_report"]["blocking_issues"]}
    assert "FAKE_PREZZO_BASE" in codes
    assert gate["gate_status"] == qg.GATE_FAIL


def test_money_role_mismatch_still_hard_blocks():
    # Expose the decisive market value under a MISLEADING core role
    # (buyer-side cost) -> MONEY_ROLE_MISMATCH, must block, must not go partial.
    inp = f.build_inputs("1", with_caution=False)
    report = inp["customer_report"]
    market = f._DECISIVE["1"]["market_value"]
    report["money_sections"]["valuation_chain"] = [
        row for row in report["money_sections"]["valuation_chain"]
        if row["label"] != "Valore di mercato"
    ]
    report["money_sections"]["buyer_side_costs"] = [
        {"label": "Costo a carico acquirente", "amount": market, "evidence_pages": [3]}
    ]
    gate = _gate(inp)
    codes = {b["code"] for b in gate["quality_report"]["blocking_issues"]}
    assert "MONEY_ROLE_MISMATCH" in codes
    assert gate["gate_status"] == qg.GATE_FAIL
    decision = _classify(gate, report)
    assert decision["disclosure_state"] == pr.REPORT_BLOCKED


def test_genuine_critical_coverage_failure_gate_fails():
    inp = _make_critical_missing_inputs()
    gate = _gate(inp)
    assert gate["gate_status"] == qg.GATE_FAIL
    assert gate["coverage_audit"]["coverage_status"] == c.STATUS_FAIL
