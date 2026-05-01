#!/usr/bin/env python3
"""Compare legacy customer outputs with Phase 3A authority-shadow outputs.

This is Phase 3B only: it reports differences and error classes. It does not
replace customer-facing facts or mutate saved analyses.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

try:
    from scripts.audit_authority_corpus import _extract_pdf_pages
except Exception:  # pragma: no cover - direct script execution fallback
    from audit_authority_corpus import _extract_pdf_pages

from perizia_authority_resolvers import build_authority_shadow_resolvers
from perizia_section_authority import build_section_authority_map


RUNS_ROOT = Path("/srv/perizia/_qa/runs")
FIXTURE_PATH = BACKEND_DIR / "tests" / "fixtures" / "perizia_authority_golden_cases.json"

AUTHORITY_LEAK_KEYS = {
    "section_zone",
    "authority_level",
    "authority_score",
    "domain_hint",
    "domain_hints",
    "answer_point",
    "reason_for_authority",
    "is_instruction_like",
    "is_answer_like",
    "source_stage",
    "extractor_version",
    "shadow_authority",
    "authority_shadow_resolvers",
    "authority_resolver",
}

INTERNAL_RESULT_KEYS = {
    "debug",
    "internal_runtime",
    "verifier_runtime",
    "canonical_freeze_contract",
    "canonical_freeze_explanations",
}

PLACEHOLDER_PATTERNS = [
    r"\bTBD\b",
    r"\bNOT_SPECIFIED\b",
    r"\bNOT_SPECIFIED_IN_PERIZIA\b",
    r"\bINTERNAL\s+DIRTY\b",
    r"\bTODO\b",
    r"\{\{[^{}]+\}\}",
]

STALE_VIA_UMBRIA_MONEY_RE = re.compile(
    r"regolarizzazion\w*\s*:\s*(?:€|\beuro\b)\s*(?:31|6)(?:[,\.]00)?\b",
    flags=re.IGNORECASE | re.UNICODE,
)

TABLE_HEADERS = [
    "file",
    "analysis_id",
    "expected_lot_mode",
    "legacy_lot_mode",
    "authority_shadow_lot_mode",
    "expected_occupancy",
    "legacy_occupancy",
    "authority_shadow_occupancy",
    "expected_opponibilita",
    "legacy_opponibilita",
    "authority_shadow_opponibilita",
    "legacy_money_summary",
    "authority_money_role_counts",
    "legacy_legal_killers_count",
    "authority_legal_candidates_count",
    "instruction_leak_suspects",
    "placeholder_leak_suspects",
    "customer_authority_key_leak",
    "comparison_verdict",
    "notes",
]


def _read_json(path: Path, fallback: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return fallback


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _page_number(page: Dict[str, Any], default: int) -> int:
    for key in ("page_number", "page", "page_num"):
        try:
            value = int(page.get(key))
            if value > 0:
                return value
        except Exception:
            continue
    return default


def _normalize_pages(pages: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for idx, page in enumerate(pages or [], start=1):
        if not isinstance(page, dict):
            continue
        out.append({"page_number": _page_number(page, idx), "text": str(page.get("text") or "")})
    out.sort(key=lambda row: row["page_number"])
    return out


def _all_text(pages: Sequence[Dict[str, Any]]) -> str:
    return "\n\n".join(str(page.get("text") or "") for page in pages if isinstance(page, dict))


def _value_at_path(obj: Any, path: Sequence[str]) -> Any:
    current = obj
    for part in path:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _first_text_value(*values: Any) -> str:
    for value in values:
        if isinstance(value, dict):
            for key in ("value", "status", "status_it", "label", "label_it", "headline_it", "text"):
                text = _first_text_value(value.get(key))
                if text:
                    return text
        elif isinstance(value, list):
            for item in value:
                text = _first_text_value(item)
                if text:
                    return text
        else:
            text = str(value or "").strip()
            if text:
                return text
    return ""


def _normalize_lot_mode(value: Any) -> str:
    text = _normalize_text(value)
    if text in {"single_lot", "single", "lotto unico", "lotto_unico"}:
        return "single_lot"
    if text in {"multi_lot", "multi", "multilot"}:
        return "multi_lot"
    if re.search(r"\blott[oi]\s+\d+\s*(?:[,/;]|e|ed|-|–)\s*\d+\b", text):
        return "multi_lot"
    if re.search(r"\blotto\s+unico\b|\bunico\s+lotto\b", text):
        return "single_lot"
    if re.search(r"\bmulti[-\s]?lotto\b|\bpiu\s+lotti\b|\bplurimi\s+lotti\b", text):
        return "multi_lot"
    return "unknown"


def _normalize_occupancy(value: Any) -> str:
    text = _normalize_text(value)
    if not text or text in {"unknown", "not_found", "non specificato in perizia"}:
        return "UNKNOWN"
    if re.search(r"\bnon\s+verificabile\b|\bda\s+verificare\b|\bnot_found\b|\blow_confidence\b", text):
        return "NON_VERIFICABILE"
    if re.search(r"\bterzi\b|\bsenza\s+titolo\b", text) and re.search(r"\boccup", text):
        return "OCCUPATO_DA_TERZI"
    if re.search(r"\b(debitore|debitori|esecutat[oaie]|coniuge)\b", text) and re.search(r"\boccup", text):
        return "OCCUPATO_DA_DEBITORE"
    if re.search(r"\boccup", text):
        return "OCCUPATO"
    if re.search(r"\bliber", text):
        return "LIBERO"
    return "UNKNOWN"


def _normalize_opponibilita(value: Any) -> str:
    text = _normalize_text(value)
    if not text or text in {"unknown", "not_found", "non specificato in perizia"}:
        return "UNKNOWN"
    if re.search(r"\bnon\s+verificabile\b|\bda\s+verificare\b|\bnot_found\b|\blow_confidence\b", text):
        return "NON_VERIFICABILE"
    if re.search(r"\b(non\s+opponibil|inopponibil|senza\s+titolo|posteriore\s+al\s+pignoramento)\b", text):
        return "NON_OPPONIBILE"
    if re.search(r"\bopponibil", text):
        return "OPPONIBILE"
    return "UNKNOWN"


def _occupancy_compatible(expected: str, actual: str) -> bool:
    if not expected or expected == "UNKNOWN":
        return False
    if expected == actual:
        return True
    if expected.startswith("OCCUPATO") and actual.startswith("OCCUPATO"):
        return True
    return False


def _status_is_certain(value: str) -> bool:
    return value not in {"", "unknown", "UNKNOWN", "NON_VERIFICABILE"}


def _load_golden_cases() -> List[Dict[str, Any]]:
    payload = _read_json(FIXTURE_PATH, [])
    return payload if isinstance(payload, list) else []


def _resolve_case_path(case: Dict[str, Any]) -> Tuple[Optional[Path], str]:
    tried: List[str] = []
    for raw in case.get("paths") or []:
        path = Path(str(raw)).expanduser()
        tried.append(str(path))
        if path.exists() and path.is_file():
            return path.resolve(), ""
    return None, "missing_pdf:" + "|".join(tried)


def _expected_from_case(case: Dict[str, Any]) -> Dict[str, str]:
    expectations = case.get("expectations") if isinstance(case.get("expectations"), dict) else {}
    expected_class = str(case.get("expected_class") or "")
    expected_lot_mode = ""
    if expectations.get("requires_high_lotto_unico") or "SINGLE_LOT" in expected_class:
        expected_lot_mode = "single_lot"
    elif len(expectations.get("requires_high_lot_numbers") or []) >= 2 or "MULTI_LOT" in expected_class:
        expected_lot_mode = "multi_lot"

    case_id = str(case.get("id") or "")
    expected_occupancy = ""
    if case_id == "1859886_c_perizia":
        expected_occupancy = "OCCUPATO"
    elif case_id == "multilot_69_2024":
        expected_occupancy = "LIBERO"

    return {
        "expected_lot_mode": expected_lot_mode,
        "expected_occupancy": expected_occupancy,
        "expected_opponibilita": "",
    }


def _expectations_for_file(path: Path) -> Dict[str, str]:
    resolved = str(path.resolve()) if path.exists() else str(path)
    name = path.name.lower()
    for case in _load_golden_cases():
        for raw in case.get("paths") or []:
            candidate = Path(str(raw)).expanduser()
            if str(candidate) == resolved or (candidate.exists() and str(candidate.resolve()) == resolved):
                return _expected_from_case(case)
    if "1859886" in name:
        return {"expected_lot_mode": "single_lot", "expected_occupancy": "OCCUPATO", "expected_opponibilita": ""}
    if "multilot" in name or "69-2024" in name or "69_2024" in name:
        return {"expected_lot_mode": "multi_lot", "expected_occupancy": "LIBERO", "expected_opponibilita": ""}
    return {"expected_lot_mode": "", "expected_occupancy": "", "expected_opponibilita": ""}


def _legacy_lot_mode_from_result(result: Dict[str, Any]) -> str:
    if bool(result.get("is_multi_lot")):
        return "multi_lot"
    try:
        lots_count = int(result.get("lots_count") or 0)
        if lots_count >= 2:
            return "multi_lot"
        if lots_count == 1:
            return "single_lot"
    except Exception:
        pass
    lots = result.get("lots")
    if isinstance(lots, list):
        if len(lots) >= 2:
            return "multi_lot"
        if len(lots) == 1:
            return "single_lot"
    return _normalize_lot_mode(
        _first_text_value(
            _value_at_path(result, ["field_states", "lotto"]),
            _value_at_path(result, ["report_header", "lotto"]),
            _value_at_path(result, ["case_header", "lotto"]),
        )
    )


def _legacy_status_from_result(result: Dict[str, Any], field_key: str, root_key: str, normalizer) -> str:
    return normalizer(
        _first_text_value(
            _value_at_path(result, ["field_states", field_key]),
            result.get(root_key),
            _value_at_path(result, ["customer_decision_contract", "field_states", field_key]),
        )
    )


def _legacy_money_summary_from_result(result: Dict[str, Any]) -> Dict[str, Any]:
    box = result.get("money_box") if isinstance(result.get("money_box"), dict) else {}
    section3 = result.get("section_3_money_box") if isinstance(result.get("section_3_money_box"), dict) else {}
    items = box.get("items") if isinstance(box.get("items"), list) else section3.get("items") if isinstance(section3.get("items"), list) else []
    text = json.dumps({"money_box": box, "section_3_money_box": section3}, ensure_ascii=False)
    return {
        "items_count": len(items),
        "buyer_cost_like": len(items),
        "rendita_mentions": len(re.findall(r"\brendita\b", text, flags=re.IGNORECASE)),
        "valuation_mentions": len(re.findall(r"\b(valore|stima|deprezz)", text, flags=re.IGNORECASE)),
        "formalities_mentions": len(re.findall(r"\b(formalita|ipotec|pignorament|trascrizion|iscrizion)", text, flags=re.IGNORECASE)),
        "stale_via_umbria_regolarizzazione": bool(STALE_VIA_UMBRIA_MONEY_RE.search(text)),
    }


def _legacy_legal_killers_count_from_result(result: Dict[str, Any]) -> int:
    for path in (["section_9_legal_killers", "items"], ["legal_killers"], ["customer_decision_contract", "legal_killers"]):
        value = _value_at_path(result, path)
        if isinstance(value, list):
            return len(value)
        if isinstance(value, dict):
            items = value.get("items")
            if isinstance(items, list):
                return len(items)
    return 0


def _extract_legacy_from_result(result: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(result, dict):
        return None
    return {
        "legacy_lot_mode": _legacy_lot_mode_from_result(result),
        "legacy_occupancy": _legacy_status_from_result(result, "stato_occupativo", "stato_occupativo", _normalize_occupancy),
        "legacy_opponibilita": _legacy_status_from_result(result, "opponibilita_occupazione", "opponibilita_occupazione", _normalize_opponibilita),
        "legacy_money_summary": _legacy_money_summary_from_result(result),
        "legacy_legal_killers_count": _legacy_legal_killers_count_from_result(result),
    }


def _legacy_lot_mode_from_pages(pages: Sequence[Dict[str, Any]]) -> str:
    text = _all_text(pages)
    lot_numbers = {int(match.group(1)) for match in re.finditer(r"\blotto\s*(?:n\.?|nr\.?|numero)?\s*([1-9]\d*)\b", text, re.I)}
    if len(lot_numbers) >= 2:
        return "multi_lot"
    if re.search(r"\blotto\s+unico\b|\bunico\s+lotto\b", text, re.I):
        return "single_lot"
    if len(lot_numbers) == 1:
        return "single_lot"
    return "unknown"


def _legacy_occupancy_from_pages(pages: Sequence[Dict[str, Any]]) -> str:
    text = _all_text(pages)
    matches = list(
        re.finditer(
            r".{0,80}(stato\s+di\s+(?:occupazione|possesso)|occupat\w*|liber\w*|senza\s+titolo|debitore|coniuge|terzi).{0,120}",
            text,
            flags=re.IGNORECASE | re.UNICODE | re.DOTALL,
        )
    )
    if not matches:
        return "UNKNOWN"
    return _normalize_occupancy(matches[-1].group(0))


def _legacy_opponibilita_from_pages(pages: Sequence[Dict[str, Any]]) -> str:
    text = _all_text(pages)
    matches = list(
        re.finditer(
            r".{0,100}(opponibil\w*|locazion\w*|contratto\s+di\s+locazione|comodato|senza\s+titolo).{0,140}",
            text,
            flags=re.IGNORECASE | re.UNICODE | re.DOTALL,
        )
    )
    if not matches:
        return "UNKNOWN"
    return _normalize_opponibilita(matches[-1].group(0))


def _legacy_money_summary_from_pages(pages: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    text = _all_text(pages)
    amount_count = len(re.findall(r"(?:€|\beuro\b)\s*\d|\d[\d\.\s]*,\d{2}\s*(?:€|\beuro\b)", text, flags=re.I))
    return {
        "items_count": amount_count,
        "buyer_cost_like": len(re.findall(r"\b(spese|costi|oneri|sanzion|regolarizzazion|sanator|docfa)\b", text, flags=re.I)),
        "rendita_mentions": len(re.findall(r"\brendita\s+catastale\b", text, flags=re.I)),
        "valuation_mentions": len(re.findall(r"\b(valore\s+di\s+stima|valore\s+finale|prezzo\s+base|deprezzament)", text, flags=re.I)),
        "formalities_mentions": len(re.findall(r"\b(formalita|ipotec|pignorament|trascrizion|iscrizion)", text, flags=re.I)),
        "stale_via_umbria_regolarizzazione": bool(STALE_VIA_UMBRIA_MONEY_RE.search(text)),
    }


def _legacy_legal_killers_count_from_pages(pages: Sequence[Dict[str, Any]]) -> int:
    text = _all_text(pages)
    return len(re.findall(r"\b(ipotec\w*|pignorament\w*|servitu\s+non\s+cancellabil\w*|vincol\w*)\b", text, flags=re.I))


def _extract_legacy_from_pages(pages: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "legacy_lot_mode": _legacy_lot_mode_from_pages(pages),
        "legacy_occupancy": _legacy_occupancy_from_pages(pages),
        "legacy_opponibilita": _legacy_opponibilita_from_pages(pages),
        "legacy_money_summary": _legacy_money_summary_from_pages(pages),
        "legacy_legal_killers_count": _legacy_legal_killers_count_from_pages(pages),
    }


def _shadow_value(shadow: Dict[str, Any], domain: str) -> Dict[str, Any]:
    row = shadow.get(domain) if isinstance(shadow, dict) else {}
    value = row.get("value") if isinstance(row, dict) else {}
    return value if isinstance(value, dict) else {}


def _authority_legal_candidates_count(shadow: Dict[str, Any]) -> int:
    legal = _shadow_value(shadow, "legal_formalities")
    return len(legal.get("surviving_formalities") or []) + len(legal.get("legal_killer_candidates") or [])


def _instruction_leak_suspects(shadow: Dict[str, Any]) -> int:
    opp = _shadow_value(shadow, "opponibilita")
    legal = _shadow_value(shadow, "legal_formalities")
    return len(opp.get("instruction_only_mentions") or []) + len(legal.get("instruction_only_legal_mentions") or [])


def _authority_outputs(shadow: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "authority_shadow_lot_mode": str(_shadow_value(shadow, "lot_structure").get("shadow_lot_mode") or "unknown"),
        "authority_shadow_occupancy": str(_shadow_value(shadow, "occupancy").get("shadow_occupancy_status") or "UNKNOWN"),
        "authority_shadow_opponibilita": str(_shadow_value(shadow, "opponibilita").get("shadow_opponibilita_status") or "UNKNOWN"),
        "authority_money_role_counts": _shadow_value(shadow, "money_roles").get("money_role_counts") or {},
        "authority_legal_candidates_count": _authority_legal_candidates_count(shadow),
        "instruction_leak_suspects": _instruction_leak_suspects(shadow),
    }


def collect_authority_key_leaks(value: Any, path: str = "result") -> List[str]:
    hits: List[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            key_text = str(key)
            if path == "result" and key_text in INTERNAL_RESULT_KEYS:
                continue
            child_path = f"{path}.{key_text}"
            if key_text.startswith("authority_") or key_text in AUTHORITY_LEAK_KEYS:
                hits.append(child_path)
            hits.extend(collect_authority_key_leaks(child, child_path))
    elif isinstance(value, list):
        for idx, item in enumerate(value):
            hits.extend(collect_authority_key_leaks(item, f"{path}[{idx}]"))
    return hits


def collect_placeholder_leaks(value: Any, path: str = "result") -> List[str]:
    hits: List[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            key_text = str(key)
            if path == "result" and key_text in INTERNAL_RESULT_KEYS:
                continue
            hits.extend(collect_placeholder_leaks(child, f"{path}.{key_text}"))
    elif isinstance(value, list):
        for idx, item in enumerate(value):
            hits.extend(collect_placeholder_leaks(item, f"{path}[{idx}]"))
    elif isinstance(value, str):
        for pattern in PLACEHOLDER_PATTERNS:
            if re.search(pattern, value):
                hits.append(path)
                break
    return hits


def _money_error_classes(legacy_money_summary: Dict[str, Any], authority_counts: Dict[str, Any]) -> List[str]:
    classes: List[str] = []
    buyer_like = int(legacy_money_summary.get("buyer_cost_like") or 0)
    if buyer_like <= 0:
        return classes
    if int(legacy_money_summary.get("rendita_mentions") or 0) and int(authority_counts.get("cadastral_rendita") or 0):
        classes.append("rendita_as_buyer_cost")
    if int(legacy_money_summary.get("valuation_mentions") or 0) and (
        int(authority_counts.get("market_value") or 0)
        or int(authority_counts.get("final_value") or 0)
        or int(authority_counts.get("valuation_deduction") or 0)
        or int(authority_counts.get("base_auction") or 0)
    ):
        classes.append("valuation_as_buyer_cost")
    if int(legacy_money_summary.get("formalities_mentions") or 0) and int(authority_counts.get("formalities_procedural_amount") or 0):
        classes.append("formalita_as_buyer_cost")
    if int(authority_counts.get("total_candidate") or 0) and int(authority_counts.get("component_of_total") or 0):
        classes.append("component_total_double_count")
    return classes


def compare_outputs(
    *,
    expected_lot_mode: str,
    legacy_lot_mode: str,
    authority_shadow_lot_mode: str,
    expected_occupancy: str,
    legacy_occupancy: str,
    authority_shadow_occupancy: str,
    expected_opponibilita: str,
    legacy_opponibilita: str,
    authority_shadow_opponibilita: str,
    legacy_money_summary: Dict[str, Any],
    authority_money_role_counts: Dict[str, Any],
    legacy_legal_killers_count: int,
    authority_legal_candidates_count: int,
    instruction_leak_suspects: int,
    placeholder_leak_suspects: int,
    customer_authority_key_leak: int,
) -> Tuple[str, List[str], List[str]]:
    classes: List[str] = []
    notes: List[str] = []
    better = False
    worse = False
    expected_known = False

    if customer_authority_key_leak:
        classes.append("authority_internal_customer_leak")
        worse = True
    if placeholder_leak_suspects:
        classes.append("placeholder_customer_leak")
        notes.append("placeholder_leak_is_legacy_customer_payload_signal")

    if expected_lot_mode:
        expected_known = True
        legacy_wrong = legacy_lot_mode not in {"unknown", expected_lot_mode}
        authority_wrong = authority_shadow_lot_mode not in {"unknown", expected_lot_mode}
        legacy_correct = legacy_lot_mode == expected_lot_mode
        authority_correct = authority_shadow_lot_mode == expected_lot_mode
        if legacy_lot_mode == "single_lot" and expected_lot_mode == "multi_lot":
            classes.append("false_single_lot")
        if legacy_lot_mode == "multi_lot" and expected_lot_mode == "single_lot":
            classes.append("false_multilot")
        if authority_shadow_lot_mode == "single_lot" and expected_lot_mode == "multi_lot":
            classes.append("false_single_lot")
            worse = True
        if authority_shadow_lot_mode == "multi_lot" and expected_lot_mode == "single_lot":
            classes.append("false_multilot")
            worse = True
        if legacy_wrong and (authority_correct or authority_shadow_lot_mode == "unknown"):
            better = True
        if authority_correct and legacy_wrong:
            better = True
        if legacy_correct and authority_shadow_lot_mode == "unknown":
            classes.append("unknown_when_legacy_correct")
            worse = True
        if authority_wrong:
            worse = True

    if expected_occupancy:
        expected_known = True
        legacy_correct = _occupancy_compatible(expected_occupancy, legacy_occupancy)
        authority_correct = _occupancy_compatible(expected_occupancy, authority_shadow_occupancy)
        if legacy_correct and authority_shadow_occupancy in {"UNKNOWN", "NON_VERIFICABILE"}:
            classes.append("unknown_when_legacy_correct")
            worse = True
        elif not legacy_correct and authority_correct:
            better = True
        elif not authority_correct and _status_is_certain(authority_shadow_occupancy):
            classes.append("weak_occupancy_override")
            worse = True

    if expected_opponibilita:
        expected_known = True
        legacy_correct = legacy_opponibilita == expected_opponibilita
        authority_correct = authority_shadow_opponibilita == expected_opponibilita
        if legacy_correct and authority_shadow_opponibilita in {"UNKNOWN", "NON_VERIFICABILE"}:
            classes.append("unknown_when_legacy_correct")
            worse = True
        elif not legacy_correct and authority_correct:
            better = True
        elif not authority_correct and _status_is_certain(authority_shadow_opponibilita):
            classes.append("fake_opponibilita")
            worse = True

    if instruction_leak_suspects and (
        authority_shadow_occupancy in {"UNKNOWN", "NON_VERIFICABILE"}
        or authority_shadow_opponibilita in {"UNKNOWN", "NON_VERIFICABILE"}
    ):
        classes.append("instruction_based_fact")
        if _status_is_certain(legacy_occupancy) or _status_is_certain(legacy_opponibilita):
            better = True

    if legacy_opponibilita in {"OPPONIBILE", "NON_OPPONIBILE"} and authority_shadow_opponibilita in {"UNKNOWN", "NON_VERIFICABILE"} and not expected_opponibilita:
        classes.append("fake_opponibilita")
        better = True

    if legacy_legal_killers_count > authority_legal_candidates_count and authority_legal_candidates_count == 0:
        classes.append("generic_legal_killer")
        better = True

    money_classes = _money_error_classes(legacy_money_summary, authority_money_role_counts)
    if money_classes:
        classes.extend(money_classes)
        better = True
    if bool(legacy_money_summary.get("stale_via_umbria_regolarizzazione")):
        classes.append("valuation_as_buyer_cost")
        notes.append("stale_via_umbria_regolarizzazione_label_detected")
        better = True

    classes = list(dict.fromkeys(classes))
    if worse:
        classes.append("authority_worse_than_legacy")
        verdict = "AUTHORITY_WORSE_THAN_LEGACY"
    elif better:
        classes.append("authority_better_than_legacy")
        verdict = "AUTHORITY_BETTER_THAN_LEGACY"
    elif not expected_known:
        classes.append("insufficient_expected_truth")
        verdict = "INSUFFICIENT_EXPECTED_TRUTH"
    elif (
        legacy_lot_mode == authority_shadow_lot_mode
        and legacy_occupancy == authority_shadow_occupancy
        and legacy_opponibilita == authority_shadow_opponibilita
    ):
        classes.append("same_as_legacy")
        verdict = "SAME_AS_LEGACY"
    else:
        classes.append("same_as_legacy")
        verdict = "SAME_AS_LEGACY"

    return verdict, list(dict.fromkeys(classes)), notes


def _json_summary(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _load_candidates_for_analysis(analysis_id: str) -> Dict[str, Any]:
    candidates_dir = RUNS_ROOT / analysis_id / "candidates"
    out: Dict[str, Any] = {}
    for key, filename in (("money", "candidates_money.json"), ("triggers", "candidates_triggers.json")):
        payload = _read_json(candidates_dir / filename)
        if isinstance(payload, list):
            out[key] = payload
    return out


def _find_saved_analysis_payload(analysis_id: str = "", file_hint: str = "") -> Optional[Dict[str, Any]]:
    direct_paths = []
    if analysis_id:
        direct_paths.extend(
            [
                RUNS_ROOT / analysis_id / "analysis.json",
                RUNS_ROOT / analysis_id / "system.json",
                Path("/tmp/perizia_qa_run/analysis.json"),
            ]
        )
    for path in direct_paths:
        payload = _read_json(path)
        if isinstance(payload, dict) and (not analysis_id or str(payload.get("analysis_id") or "") == analysis_id):
            return payload

    hint = file_hint.lower().strip()
    if not hint and not analysis_id:
        return None
    for path in sorted(RUNS_ROOT.glob("*/system.json"), key=lambda p: str(p), reverse=True):
        payload = _read_json(path)
        if not isinstance(payload, dict):
            continue
        if analysis_id and str(payload.get("analysis_id") or "") == analysis_id:
            return payload
        file_name = str(payload.get("file_name") or payload.get("case_title") or "").lower()
        if hint and hint in file_name:
            return payload
    return None


def _row_from_components(
    *,
    file_label: str,
    analysis_id: str = "",
    expected: Optional[Dict[str, str]] = None,
    legacy: Optional[Dict[str, Any]] = None,
    shadow: Optional[Dict[str, Any]] = None,
    customer_result: Optional[Dict[str, Any]] = None,
    notes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    expected = expected or {}
    legacy = legacy or {
        "legacy_lot_mode": "unknown",
        "legacy_occupancy": "UNKNOWN",
        "legacy_opponibilita": "UNKNOWN",
        "legacy_money_summary": {},
        "legacy_legal_killers_count": 0,
    }
    shadow = shadow or {}
    authority = _authority_outputs(shadow)
    leak_hits = collect_authority_key_leaks(customer_result or {})
    placeholder_hits = collect_placeholder_leaks(customer_result or {})
    verdict, classes, comparison_notes = compare_outputs(
        expected_lot_mode=str(expected.get("expected_lot_mode") or ""),
        legacy_lot_mode=str(legacy.get("legacy_lot_mode") or "unknown"),
        authority_shadow_lot_mode=str(authority.get("authority_shadow_lot_mode") or "unknown"),
        expected_occupancy=str(expected.get("expected_occupancy") or ""),
        legacy_occupancy=str(legacy.get("legacy_occupancy") or "UNKNOWN"),
        authority_shadow_occupancy=str(authority.get("authority_shadow_occupancy") or "UNKNOWN"),
        expected_opponibilita=str(expected.get("expected_opponibilita") or ""),
        legacy_opponibilita=str(legacy.get("legacy_opponibilita") or "UNKNOWN"),
        authority_shadow_opponibilita=str(authority.get("authority_shadow_opponibilita") or "UNKNOWN"),
        legacy_money_summary=legacy.get("legacy_money_summary") if isinstance(legacy.get("legacy_money_summary"), dict) else {},
        authority_money_role_counts=authority.get("authority_money_role_counts") if isinstance(authority.get("authority_money_role_counts"), dict) else {},
        legacy_legal_killers_count=int(legacy.get("legacy_legal_killers_count") or 0),
        authority_legal_candidates_count=int(authority.get("authority_legal_candidates_count") or 0),
        instruction_leak_suspects=int(authority.get("instruction_leak_suspects") or 0),
        placeholder_leak_suspects=len(placeholder_hits),
        customer_authority_key_leak=len(leak_hits),
    )
    all_notes = list(notes or []) + comparison_notes
    if leak_hits:
        all_notes.append("authority_leaks=" + "|".join(leak_hits[:5]))
    if placeholder_hits:
        all_notes.append("placeholder_leaks=" + "|".join(placeholder_hits[:5]))
    if shadow.get("fail_open"):
        all_notes.append("authority_fail_open")
    warnings = shadow.get("warnings") if isinstance(shadow.get("warnings"), list) else []
    all_notes.extend(str(item) for item in warnings[:5])

    return {
        "file": file_label,
        "analysis_id": analysis_id,
        "expected_lot_mode": str(expected.get("expected_lot_mode") or ""),
        "legacy_lot_mode": str(legacy.get("legacy_lot_mode") or "unknown"),
        "authority_shadow_lot_mode": str(authority.get("authority_shadow_lot_mode") or "unknown"),
        "expected_occupancy": str(expected.get("expected_occupancy") or ""),
        "legacy_occupancy": str(legacy.get("legacy_occupancy") or "UNKNOWN"),
        "authority_shadow_occupancy": str(authority.get("authority_shadow_occupancy") or "UNKNOWN"),
        "expected_opponibilita": str(expected.get("expected_opponibilita") or ""),
        "legacy_opponibilita": str(legacy.get("legacy_opponibilita") or "UNKNOWN"),
        "authority_shadow_opponibilita": str(authority.get("authority_shadow_opponibilita") or "UNKNOWN"),
        "legacy_money_summary": legacy.get("legacy_money_summary") if isinstance(legacy.get("legacy_money_summary"), dict) else {},
        "authority_money_role_counts": authority.get("authority_money_role_counts") if isinstance(authority.get("authority_money_role_counts"), dict) else {},
        "legacy_legal_killers_count": int(legacy.get("legacy_legal_killers_count") or 0),
        "authority_legal_candidates_count": int(authority.get("authority_legal_candidates_count") or 0),
        "instruction_leak_suspects": int(authority.get("instruction_leak_suspects") or 0),
        "placeholder_leak_suspects": len(placeholder_hits),
        "customer_authority_key_leak": len(leak_hits),
        "comparison_verdict": verdict,
        "error_classes": classes,
        "notes": ";".join(dict.fromkeys(str(note) for note in all_notes if str(note or "").strip())),
    }


def compare_pdf(path: Path, *, expected: Optional[Dict[str, str]] = None, analysis_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {
            "file": str(path),
            "analysis_id": "",
            "expected_lot_mode": str((expected or {}).get("expected_lot_mode") or ""),
            "legacy_lot_mode": "unknown",
            "authority_shadow_lot_mode": "unknown",
            "expected_occupancy": str((expected or {}).get("expected_occupancy") or ""),
            "legacy_occupancy": "UNKNOWN",
            "authority_shadow_occupancy": "UNKNOWN",
            "expected_opponibilita": str((expected or {}).get("expected_opponibilita") or ""),
            "legacy_opponibilita": "UNKNOWN",
            "authority_shadow_opponibilita": "UNKNOWN",
            "legacy_money_summary": {},
            "authority_money_role_counts": {},
            "legacy_legal_killers_count": 0,
            "authority_legal_candidates_count": 0,
            "instruction_leak_suspects": 0,
            "placeholder_leak_suspects": 0,
            "customer_authority_key_leak": 0,
            "comparison_verdict": "SKIPPED_MISSING_PDF",
            "error_classes": ["insufficient_expected_truth"],
            "notes": f"missing_pdf:{path}",
        }
    pages = _normalize_pages(_extract_pdf_pages(path))
    section_map = build_section_authority_map(pages)
    shadow = build_authority_shadow_resolvers(pages, section_map)
    result = analysis_payload.get("result") if isinstance(analysis_payload, dict) else None
    legacy = _extract_legacy_from_result(result) or _extract_legacy_from_pages(pages)
    if expected is None:
        expected = _expectations_for_file(path)
    return _row_from_components(
        file_label=str(path),
        analysis_id=str((analysis_payload or {}).get("analysis_id") or ""),
        expected=expected,
        legacy=legacy,
        shadow=shadow,
        customer_result=result,
    )


def compare_analysis_id(analysis_id: str) -> Dict[str, Any]:
    extract_dir = RUNS_ROOT / analysis_id / "extract"
    pages_payload = _read_json(extract_dir / "pages_raw.json", [])
    pages = _normalize_pages(pages_payload if isinstance(pages_payload, list) else [])
    section_map = _read_json(extract_dir / "section_authority.json")
    if not isinstance(section_map, dict) and pages:
        section_map = build_section_authority_map(pages)
    candidates = _load_candidates_for_analysis(analysis_id)
    shadow = build_authority_shadow_resolvers(pages, section_map if isinstance(section_map, dict) else {}, candidates=candidates)
    saved = _find_saved_analysis_payload(analysis_id=analysis_id)
    result = saved.get("result") if isinstance(saved, dict) else None
    legacy = _extract_legacy_from_result(result) or _extract_legacy_from_pages(pages)
    file_label = str((saved or {}).get("file_name") or (saved or {}).get("case_title") or analysis_id)
    expected = _expectations_for_file(Path(file_label))
    return _row_from_components(
        file_label=file_label,
        analysis_id=analysis_id,
        expected=expected,
        legacy=legacy,
        shadow=shadow,
        customer_result=result,
        notes=[] if pages else ["missing_extract_pages"],
    )


def compare_corpus() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for case in _load_golden_cases():
        expected = _expected_from_case(case)
        path, missing_reason = _resolve_case_path(case)
        if path is None:
            saved = None
            if str(case.get("id") or "") == "via_umbria":
                saved = _find_saved_analysis_payload(file_hint="umbria")
            if isinstance(saved, dict):
                result = saved.get("result") if isinstance(saved.get("result"), dict) else {}
                legacy = _extract_legacy_from_result(result) or {}
                shadow = _value_at_path(saved, ["internal_runtime", "debug", "authority_shadow_resolvers"]) or _value_at_path(result, ["debug", "authority_shadow_resolvers"]) or {}
                rows.append(
                    _row_from_components(
                        file_label=str(saved.get("file_name") or saved.get("case_title") or case.get("label") or case.get("id")),
                        analysis_id=str(saved.get("analysis_id") or ""),
                        expected=expected,
                        legacy=legacy,
                        shadow=shadow if isinstance(shadow, dict) else {},
                        customer_result=result,
                        notes=["pdf_missing_but_saved_analysis_found", missing_reason],
                    )
                )
                continue
            rows.append(compare_pdf(Path(str((case.get("paths") or ["missing"])[0])), expected=expected))
            rows[-1]["notes"] = missing_reason
            continue
        saved = _find_saved_analysis_payload(file_hint=path.name)
        rows.append(compare_pdf(path, expected=expected, analysis_payload=saved))
    return rows


def _print_table(rows: List[Dict[str, Any]]) -> None:
    print("\t".join(TABLE_HEADERS))
    for row in rows:
        printable = dict(row)
        printable["legacy_money_summary"] = _json_summary(row.get("legacy_money_summary") or {})
        printable["authority_money_role_counts"] = _json_summary(row.get("authority_money_role_counts") or {})
        print("\t".join(str(printable.get(header, "")) for header in TABLE_HEADERS))


def _write_json(path: Path, rows: List[Dict[str, Any]]) -> None:
    summary = {
        "total": len(rows),
        "authority_better": sum(1 for row in rows if row.get("comparison_verdict") == "AUTHORITY_BETTER_THAN_LEGACY"),
        "authority_worse": sum(1 for row in rows if row.get("comparison_verdict") == "AUTHORITY_WORSE_THAN_LEGACY"),
        "same": sum(1 for row in rows if row.get("comparison_verdict") == "SAME_AS_LEGACY"),
        "insufficient": sum(1 for row in rows if row.get("comparison_verdict") == "INSUFFICIENT_EXPECTED_TRUTH"),
        "skipped": sum(1 for row in rows if str(row.get("comparison_verdict") or "").startswith("SKIPPED")),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"summary": summary, "rows": rows}, f, ensure_ascii=False, indent=2)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Compare legacy Perizia outputs with authority-shadow resolver outputs.")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--corpus", action="store_true", help="Compare the golden authority corpus.")
    mode.add_argument("--file", dest="file_path", help="Compare one PDF file.")
    mode.add_argument("--analysis-id", help="Compare one saved extraction/analysis id.")
    parser.add_argument("--json", action="store_true", help="Print JSON instead of TSV.")
    parser.add_argument("--json-out", help="Optional JSON report path.")
    args = parser.parse_args(argv)

    if args.corpus:
        rows = compare_corpus()
    elif args.file_path:
        path = Path(args.file_path).expanduser()
        rows = [compare_pdf(path, expected=_expectations_for_file(path))]
    else:
        rows = [compare_analysis_id(str(args.analysis_id or "").strip())]

    if args.json or args.json_out:
        payload = {"rows": rows}
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
    if not args.json:
        _print_table(rows)
    if args.json_out:
        _write_json(Path(args.json_out).expanduser().resolve(), rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
