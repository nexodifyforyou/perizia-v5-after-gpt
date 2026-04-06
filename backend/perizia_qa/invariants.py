from __future__ import annotations

from typing import Any, Dict, List


def run_invariants(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    case = payload.get("canonical_case", {}) if isinstance(payload.get("canonical_case"), dict) else {}
    checks: List[Dict[str, Any]] = []
    quota = ((case.get("rights") or {}).get("quota") or {}).get("value")
    checks.append(
        {
            "code": "quota_date_contamination",
            "ok": not (isinstance(quota, str) and quota.count("/") == 2),
            "detail": quota,
        }
    )
    pricing = case.get("pricing", {}) if isinstance(case.get("pricing"), dict) else {}
    selected_price = pricing.get("selected_price")
    benchmark = pricing.get("benchmark_value")
    checks.append(
        {
            "code": "absurd_tiny_price",
            "ok": not (isinstance(selected_price, (int, float)) and isinstance(benchmark, (int, float)) and selected_price < 1000 and benchmark >= 10000),
            "detail": {"selected_price": selected_price, "benchmark_value": benchmark},
        }
    )
    occupancy = case.get("occupancy", {}) if isinstance(case.get("occupancy"), dict) else {}
    invalid_occupancy = [
        cand for cand in occupancy.get("candidates", [])
        if isinstance(cand, dict) and not cand.get("valid") and cand.get("reason") == "valuation_coefficient_not_valid_occupancy"
    ]
    chosen_occupancy = occupancy.get("status")
    checks.append(
        {
            "code": "occupancy_invalid_evidence",
            "ok": not (invalid_occupancy and chosen_occupancy == "OCCUPATO"),
            "detail": {"status": chosen_occupancy, "invalid_candidates": len(invalid_occupancy)},
        }
    )
    legal = case.get("legal", {}) if isinstance(case.get("legal"), dict) else {}
    priority = case.get("priority", {}) if isinstance(case.get("priority"), dict) else {}
    top_issue = priority.get("top_issue", {}) if isinstance(priority.get("top_issue"), dict) else {}
    checks.append(
        {
            "code": "legal_cost_priority_inversion",
            "ok": not (
                legal.get("cancellable")
                and not legal.get("surviving")
                and ((case.get("costs") or {}).get("explicit_total") or 0) > 0
                and "cancell" in str(top_issue.get("title_it") or "").lower()
            ),
            "detail": top_issue.get("title_it"),
        }
    )
    bundle = case.get("summary_bundle", {}) if isinstance(case.get("summary_bundle"), dict) else {}
    checks.append(
        {
            "code": "summary_mentions_top_issue",
            "ok": str(bundle.get("top_issue_it") or "").strip().lower() in str(bundle.get("decision_summary_it") or "").strip().lower() if bundle.get("top_issue_it") else True,
            "detail": {"top_issue_it": bundle.get("top_issue_it"), "decision_summary_it": bundle.get("decision_summary_it")},
        }
    )
    return checks

