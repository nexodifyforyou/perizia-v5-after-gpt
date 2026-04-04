from __future__ import annotations

from typing import Any, Dict, List


def build_report(
    payload: Dict[str, Any],
    invariant_results: List[Dict[str, Any]],
    legacy_vs_verifier: Dict[str, Any],
    expected_results: Dict[str, Any],
) -> Dict[str, Any]:
    invariants_ok = all(bool(item.get("ok")) for item in invariant_results)
    expected_ok = bool(expected_results.get("all_ok"))
    return {
        "status": "FAIL" if not expected_ok else ("PASS" if invariants_ok else "WARN"),
        "invariant_results": invariant_results,
        "legacy_vs_verifier": legacy_vs_verifier,
        "expected_results": expected_results,
        "checks": invariant_results,
        "comparisons": legacy_vs_verifier,
        "top_issue": (((payload.get("canonical_case") or {}).get("priority") or {}).get("top_issue") or {}),
    }
