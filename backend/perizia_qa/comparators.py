from __future__ import annotations

import math
import re
from typing import Any, Dict


def compare_legacy_and_verifier(result: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    case = payload.get("canonical_case", {}) if isinstance(payload.get("canonical_case"), dict) else {}
    rights = case.get("rights", {}) if isinstance(case.get("rights"), dict) else {}
    occupancy = case.get("occupancy", {}) if isinstance(case.get("occupancy"), dict) else {}
    pricing = case.get("pricing", {}) if isinstance(case.get("pricing"), dict) else {}
    field_states = result.get("field_states", {}) if isinstance(result.get("field_states"), dict) else {}
    return {
        "quota": {
            "legacy": ((result.get("dati_certi_del_lotto") or {}).get("quota") or {}).get("value") if isinstance(result.get("dati_certi_del_lotto"), dict) else None,
            "verifier": ((rights.get("quota") or {}).get("value") if isinstance(rights.get("quota"), dict) else None),
        },
        "pricing": {
            "legacy": ((result.get("dati_certi_del_lotto") or {}).get("prezzo_base_asta") or {}).get("value") if isinstance(result.get("dati_certi_del_lotto"), dict) else None,
            "verifier": pricing.get("selected_price"),
        },
        "occupancy": {
            "legacy": (field_states.get("stato_occupativo") or {}).get("value") if isinstance(field_states.get("stato_occupativo"), dict) else None,
            "verifier": occupancy.get("status"),
        },
    }


def extract_fixture_actuals(payload: Dict[str, Any]) -> Dict[str, Any]:
    case = payload.get("canonical_case", {}) if isinstance(payload.get("canonical_case"), dict) else {}
    rights = case.get("rights", {}) if isinstance(case.get("rights"), dict) else {}
    pricing = case.get("pricing", {}) if isinstance(case.get("pricing"), dict) else {}
    occupancy = case.get("occupancy", {}) if isinstance(case.get("occupancy"), dict) else {}
    costs = case.get("costs", {}) if isinstance(case.get("costs"), dict) else {}
    priority = case.get("priority", {}) if isinstance(case.get("priority"), dict) else {}
    top_issue = priority.get("top_issue", {}) if isinstance(priority.get("top_issue"), dict) else {}
    summary = case.get("summary_bundle", {}) if isinstance(case.get("summary_bundle"), dict) else {}
    legal = case.get("legal", {}) if isinstance(case.get("legal"), dict) else {}
    return {
        "quota": ((rights.get("quota") or {}).get("value") if isinstance(rights.get("quota"), dict) else None),
        "pricing": {
            "selected_price": pricing.get("selected_price"),
            "benchmark_value": pricing.get("benchmark_value"),
            "absurdity_guard_triggered": pricing.get("absurdity_guard_triggered"),
        },
        "occupancy": {
            "status": occupancy.get("status"),
            "opponibilita": occupancy.get("opponibilita"),
            "invalid_candidate_count": len(
                [item for item in occupancy.get("candidates", []) if isinstance(item, dict) and not item.get("valid")]
            ),
        },
        "costs": {
            "explicit_total": costs.get("explicit_total"),
            "explicit_total_low_confidence": costs.get("explicit_total_low_confidence"),
            "explicit_item_count": len(costs.get("explicit_buyer_costs", []) if isinstance(costs.get("explicit_buyer_costs"), list) else []),
        },
        "top_issue": {
            "code": top_issue.get("code"),
            "title_it": top_issue.get("title_it"),
            "category": top_issue.get("category"),
        },
        "summary_mentions_top_issue": bool((case.get("qa") or {}).get("summary_mentions_top_issue")),
        "summary_top_issue_it": summary.get("top_issue_it"),
        "summary_decision_it": summary.get("decision_summary_it"),
        "legal_notes": {
            "cancellable_count": len(legal.get("cancellable", []) if isinstance(legal.get("cancellable"), list) else []),
            "surviving_count": len(legal.get("surviving", []) if isinstance(legal.get("surviving"), list) else []),
            "background_count": len(legal.get("background", []) if isinstance(legal.get("background"), list) else []),
        },
    }


def _normalize_string(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    return re.sub(r"\s+", " ", value).strip()


def _values_match(expected: Any, actual: Any) -> bool:
    if expected is None:
        return actual is None
    if isinstance(expected, bool):
        return bool(actual) is expected
    if isinstance(expected, int) and not isinstance(expected, bool):
        return isinstance(actual, int) and actual == expected
    if isinstance(expected, float):
        return isinstance(actual, (int, float)) and math.isclose(float(actual), expected, rel_tol=0.0, abs_tol=0.01)
    if isinstance(expected, str):
        return _normalize_string(actual) == _normalize_string(expected)
    return actual == expected


def compare_expected_to_actual(expected: Dict[str, Any], actual: Dict[str, Any]) -> Dict[str, Any]:
    checks = []

    def walk(exp: Any, act: Any, path: str) -> None:
        if isinstance(exp, dict):
            current_actual = act if isinstance(act, dict) else {}
            for key, value in exp.items():
                walk(value, current_actual.get(key), f"{path}.{key}" if path else key)
            return
        ok = _values_match(exp, act)
        checks.append(
            {
                "path": path,
                "expected": exp,
                "actual": act,
                "ok": ok,
            }
        )

    walk(expected, actual, "")
    return {
        "actual": actual,
        "checks": checks,
        "all_ok": all(bool(item.get("ok")) for item in checks),
    }
