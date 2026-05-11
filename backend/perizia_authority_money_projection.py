"""Feature-flagged authority projection for customer Money Box output.

This module is intentionally narrow: it only rewrites customer-facing money
structures when AUTHORITY_MONEY_PROJECTION_ENABLED is exactly "1". Authority
classification remains the source of truth for what may be surfaced, while all
debug/projection metadata is attached under result["debug"] for the existing
customer sanitizer to remove from outbound API payloads.
"""

from __future__ import annotations

import copy
import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from perizia_authority_resolvers import build_authority_shadow_resolvers
from perizia_section_authority import build_section_authority_map


FEATURE_FLAG = "AUTHORITY_MONEY_PROJECTION_ENABLED"

SAFE_MONEY_STATUSES = {"OK", "PARTIAL"}
NON_BUYER_COST_ROLES = {
    "valuation_deduction",
    "price",
    "base_auction",
    "final_value",
    "market_value",
    "cadastral_rendita",
    "formalities_procedural_amount",
}
BUYER_SIGNAL_ROLES = {"buyer_cost_signal_to_verify", "condominium_arrears"}
STALE_REGOLARIZZAZIONE_RE = re.compile(
    r"(?<!tempi necessari per la )(?<!tempo necessario per la )regolarizzazion\w*\s*:\s*(?:€|\beuro\b)?\s*(?:31|6)(?:[,\.]00)?\b(?!\s*(?:mesi?|giorni?|anni?)\b)",
    re.IGNORECASE,
)
GENERIC_REGOLARIZZAZIONE_CERTAINTY_RE = re.compile(
    r"\b(?<!tempi necessari per la )(?<!tempo necessario per la )regolarizzazion\w*\s*:\s*(?:€|\beuro\b)?\s*\d+(?:[\.,]\d+)?\b(?!\s*(?:mesi?|giorni?|anni?)\b)",
    re.IGNORECASE,
)
MONEY_AMOUNT_RE = re.compile(r"(?:€|\beuro\b)\s*\d|\d[\d\.\s]*,\d{2}\b", re.IGNORECASE)
MONEY_QA_TOPIC_RE = re.compile(
    r"\b(?:costi?|spese?|oneri?|import[oi]|regolarizzazion\w*|sanatori\w*|ripristin\w*|"
    r"fiscalizzazion\w*|formal(?:it|i)à?|ipotec\w*|pignorament\w*|rendita\s+catastal\w*|"
    r"prezzo\s+base|base\s+d['’]?\s*asta|valore\s+(?:di\s+)?stima|valore\s+finale|"
    r"market\s+value|deprezzament\w*|totale\s+(?:stimato|costi?|extra|oneri?|spese?))\b",
    re.IGNORECASE,
)
BUYER_COST_CERTAINTY_RE = re.compile(
    r"\b(?:costo\s+certo|costi?\s+(?:extra|espliciti|a\s+carico)|a\s+carico\s+(?:dell['’]?)?"
    r"(?:acquirente|aggiudicatario)|buyer[-\s]?side|extra\s+cost|totale\s+(?:stimato|costi?|extra|oneri?|spese?))\b",
    re.IGNORECASE,
)
NON_BUYER_COST_AS_BUYER_RE = re.compile(
    r"\b(?:formal(?:it|i)à?|ipotec\w*|pignorament\w*|rendita\s+catastal\w*|prezzo\s+base|"
    r"base\s+d['’]?\s*asta|valore\s+(?:di\s+)?stima|valore\s+finale|market\s+value|"
    r"deprezzament\w*)\b.*\b(?:costi?|spese?|oneri?|a\s+carico|acquirente|aggiudicatario|extra)\b",
    re.IGNORECASE,
)
QA_MONEY_TEXT_FIELDS = {
    "current_wrong_claim",
    "claim",
    "message",
    "text",
    "problem_it",
    "description",
    "detail",
    "details",
}
QA_CLAIM_LIST_MARKERS = {"contradiction", "warning", "warn", "claim", "qa_gate"}


def _base_meta(enabled: bool) -> Dict[str, Any]:
    return {
        "enabled": enabled,
        "status": "DISABLED" if not enabled else "NOT_EVALUATED",
        "applied": False,
        "reason": "feature_flag_disabled" if not enabled else "",
        "money_status": "unknown",
        "authority_confidence": 0.0,
        "candidate_count": 0,
        "projected_items_count": 0,
        "cost_signals_to_verify_count": 0,
        "excluded_non_buyer_cost_count": 0,
        "valuation_reference_count": 0,
        "component_total_double_count_prevented": False,
        "stale_money_removed": False,
        "changed_fields": [],
        "notes": [],
    }


def _deepclone(value: Any) -> Any:
    return copy.deepcopy(value)


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _read_json(path: Path) -> Any:
    try:
        import json

        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _load_candidate_artifacts(candidate_artifacts: Any) -> Dict[str, Any]:
    if isinstance(candidate_artifacts, dict):
        return copy.deepcopy(candidate_artifacts)
    if not candidate_artifacts:
        return {}
    folder = Path(str(candidate_artifacts))
    out: Dict[str, Any] = {}
    for key, filename in (("money", "candidates_money.json"), ("triggers", "candidates_triggers.json")):
        payload = _read_json(folder / filename)
        if isinstance(payload, list):
            out[key] = payload
        elif payload is not None:
            out[key] = payload
    return out


def _shadow_from_inputs(
    pages_raw: Optional[Sequence[Dict[str, Any]]],
    section_authority_map: Any,
    candidate_artifacts: Any,
    authority_shadow: Optional[Dict[str, Any]],
) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    if isinstance(authority_shadow, dict) and isinstance(authority_shadow.get("money_roles"), dict):
        return copy.deepcopy(authority_shadow), ["reused_authority_shadow"]

    if isinstance(section_authority_map, dict):
        status = str(section_authority_map.get("_authority_tagging_status") or "").strip()
        if status in {"missing_map", "corrupt_map"}:
            return None, [f"section_authority_{status}"]
    if not isinstance(pages_raw, Sequence) or isinstance(pages_raw, (str, bytes)) or not pages_raw:
        return None, ["missing_pages_raw"]

    try:
        section_map = section_authority_map if isinstance(section_authority_map, dict) else build_section_authority_map(list(pages_raw))
        shadow = build_authority_shadow_resolvers(
            list(pages_raw),
            section_map,
            candidates=_load_candidate_artifacts(candidate_artifacts),
        )
        notes = ["built_authority_shadow_from_inputs"]
        if not isinstance(section_authority_map, dict):
            notes.append("rebuilt_section_authority_from_pages")
        return shadow, notes
    except Exception as exc:
        return None, [f"authority_shadow_build_failed:{str(exc)[:160]}"]


def _money_row(authority_shadow: Dict[str, Any]) -> Dict[str, Any]:
    row = authority_shadow.get("money_roles") if isinstance(authority_shadow, dict) else {}
    return row if isinstance(row, dict) else {}


def _money_value(money_row: Dict[str, Any]) -> Dict[str, Any]:
    value = money_row.get("value") if isinstance(money_row, dict) else {}
    return value if isinstance(value, dict) else {}


def _candidate_amount(candidate: Dict[str, Any]) -> Optional[float]:
    try:
        amount = float(candidate.get("amount_eur"))
    except Exception:
        return None
    if amount <= 0:
        return None
    return amount


def _amount_label(amount: Optional[float]) -> str:
    if amount is None:
        return ""
    rounded = int(round(float(amount)))
    return f"€ {rounded:,.0f}".replace(",", ".")


def _evidence_from_candidate(candidate: Dict[str, Any]) -> List[Dict[str, Any]]:
    quote = _normalize_text(candidate.get("raw_text"))
    evidence: Dict[str, Any] = {}
    try:
        page = int(candidate.get("page"))
        if page > 0:
            evidence["page"] = page
    except Exception:
        pass
    if quote:
        evidence["quote"] = quote[:500]
    return [evidence] if evidence else []


def _candidate_sort_key(candidate: Dict[str, Any]) -> Tuple[int, float, str]:
    try:
        page = int(candidate.get("page") or 0)
    except Exception:
        page = 0
    amount = _candidate_amount(candidate) or 0.0
    return page, amount, _normalize_text(candidate.get("raw_text"))[:80]


def _dedupe_candidates(candidates: Iterable[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen = set()
    for candidate in sorted((item for item in candidates if isinstance(item, dict)), key=_candidate_sort_key):
        sig = (
            str(candidate.get("role") or ""),
            round(float(candidate.get("amount_eur") or 0.0), 2),
            _normalize_text(candidate.get("raw_text"))[:120],
        )
        if sig in seen:
            continue
        seen.add(sig)
        out.append(candidate)
        if len(out) >= limit:
            break
    return out


def _role_label(candidate: Dict[str, Any]) -> str:
    role = str(candidate.get("role") or "")
    base_role = str(candidate.get("semantic_base_role") or "")
    if role == "condominium_arrears" or base_role == "condominium_arrears":
        return "Spese condominiali da verificare"
    if role in {"total_candidate", "buyer_cost_signal_to_verify"} or base_role == "buyer_cost_signal_to_verify":
        return "Costo da verificare"
    return "Importo segnalato in perizia, debenza da verificare"


def _cost_signal_payload(candidate: Dict[str, Any], index: int, *, safe_cost: bool) -> Dict[str, Any]:
    amount = _candidate_amount(candidate)
    label = _role_label(candidate)
    amount_text = _amount_label(amount)
    if amount_text:
        label = f"{label}: {amount_text}"
    note = (
        "Importo segnalato in perizia, debenza da verificare prima dell'offerta."
        if not safe_cost
        else "Importo indicato con obbligo buyer-side esplicito in perizia; verificare comunque con tecnico/delegato."
    )
    payload = {
        "code": f"AUTH_COST_VERIFY_{index:02d}",
        "label_it": label,
        "label_en": label,
        "type": "SIGNAL_TO_VERIFY" if not safe_cost else "ESTIMATE",
        "stima_euro": int(round(amount)) if amount is not None else None,
        "stima_nota": note,
        "note_it": note,
        "additive_to_extra_total": False,
        "contract_state": "cost_signal_to_verify" if not safe_cost else "quantified_estimate",
        "customer_visible_amount_status": "to_verify" if not safe_cost else "explicit_buyer_obligation",
        "evidence": _evidence_from_candidate(candidate),
        "fonte_perizia": {"value": "Perizia", "evidence": _evidence_from_candidate(candidate)},
    }
    return payload


def _excluded_payload(candidate: Dict[str, Any], index: int) -> Dict[str, Any]:
    role = str(candidate.get("role") or "")
    amount = _candidate_amount(candidate)
    amount_text = _amount_label(amount)
    if role == "cadastral_rendita":
        label = "Rendita catastale: dato fiscale, non costo per l'acquirente"
    elif role == "formalities_procedural_amount":
        label = "Formalita/cancellazione: importo procedurale, non trattato come costo extra certo"
    else:
        label = "Importo valutativo, non costo extra"
    if amount_text:
        label = f"{label}: {amount_text}"
    return {
        "code": f"AUTH_EXCLUDED_{index:02d}",
        "label_it": label,
        "label_en": label,
        "amount_eur": int(round(amount)) if amount is not None else None,
        "role": role,
        "note_it": "Non trattato come costo extra certo per l'acquirente.",
        "evidence": _evidence_from_candidate(candidate),
    }


def _legacy_money_text(result: Dict[str, Any]) -> str:
    pieces: List[str] = []

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for item in value:
                walk(item)
        else:
            pieces.append(str(value or ""))

    for path in (
        ("money_box",),
        ("section_3_money_box",),
        ("customer_decision_contract", "money_box"),
        ("customer_decision_contract", "section_3_money_box"),
    ):
        cur: Any = result
        for part in path:
            cur = cur.get(part) if isinstance(cur, dict) else None
        walk(cur)
    return " ".join(pieces)


def _money_box_has_projected_downgrades(money_box: Dict[str, Any]) -> bool:
    if not isinstance(money_box, dict):
        return False
    if money_box.get("policy") != "AUTHORITY_CONSERVATIVE":
        return False
    downgrade_keys = (
        "cost_signals_to_verify",
        "buyer_cost_signals_to_verify",
        "qualitative_burdens",
        "valuation_reference_amounts",
        "excluded_non_buyer_cost_amounts",
        "unsupported_or_unknown_amounts",
    )
    return any(isinstance(money_box.get(key), list) and bool(money_box.get(key)) for key in downgrade_keys)


def _qa_list_path_is_customer_warning_or_contradiction(path: str) -> bool:
    path_text = str(path or "").lower()
    return any(marker in path_text for marker in QA_CLAIM_LIST_MARKERS)


def _qa_money_claim_texts(item: Dict[str, Any]) -> List[str]:
    texts: List[str] = []
    if not isinstance(item, dict):
        return texts
    for key, value in item.items():
        key_text = str(key or "")
        if key_text in QA_MONEY_TEXT_FIELDS or key_text.endswith("_claim") or key_text.endswith("_message"):
            if isinstance(value, (dict, list)):
                continue
            normalized = _normalize_text(value)
            if normalized:
                texts.append(normalized)
    return texts


def _is_stale_money_qa_claim(item: Dict[str, Any], projected_money_box: Dict[str, Any]) -> bool:
    if not _money_box_has_projected_downgrades(projected_money_box):
        return False
    for text in _qa_money_claim_texts(item):
        if GENERIC_REGOLARIZZAZIONE_CERTAINTY_RE.search(text):
            return True
        if re.search(r"\btotale\s+stimato\s+in\s+perizia\s*:\s*(?:€|\beuro\b)?\s*\d", text, re.IGNORECASE):
            return True
        if NON_BUYER_COST_AS_BUYER_RE.search(text):
            return True
        if MONEY_AMOUNT_RE.search(text) and MONEY_QA_TOPIC_RE.search(text) and BUYER_COST_CERTAINTY_RE.search(text):
            return True
    return False


def _sanitize_stale_money_qa_claims_after_projection(result: Dict[str, Any], projected_money_box: Dict[str, Any]) -> Dict[str, Any]:
    meta: Dict[str, Any] = {
        "removed_money_qa_claims_count": 0,
        "removed_paths": [],
        "reason_codes": [],
    }
    if not isinstance(result, dict) or not _money_box_has_projected_downgrades(projected_money_box):
        return meta

    def walk(value: Any, path: str) -> Any:
        if isinstance(value, dict):
            for key in list(value.keys()):
                value[key] = walk(value.get(key), f"{path}.{key}")
            return value
        if isinstance(value, list):
            cleaned: List[Any] = []
            claim_list = _qa_list_path_is_customer_warning_or_contradiction(path)
            for idx, item in enumerate(value):
                item_path = f"{path}[{idx}]"
                if claim_list and isinstance(item, dict) and _is_stale_money_qa_claim(item, projected_money_box):
                    meta["removed_money_qa_claims_count"] += 1
                    meta["removed_paths"].append(item_path)
                    meta["reason_codes"].append("STALE_MONEY_QA_CLAIM_REMOVED")
                    continue
                cleaned.append(walk(item, item_path))
            return cleaned
        return value

    walk(result.get("qa_gate"), "result.qa_gate")
    customer_contract = result.get("customer_decision_contract")
    if isinstance(customer_contract, dict):
        walk(customer_contract.get("qa_gate"), "result.customer_decision_contract.qa_gate")
    for key in list(result.keys()):
        key_text = str(key or "").lower()
        if key_text == "qa_gate" or key_text == "customer_decision_contract":
            continue
        if any(marker in key_text for marker in ("contradiction", "warning", "warn", "claim")):
            result[key] = walk(result.get(key), f"result.{key}")

    meta["removed_paths"] = list(dict.fromkeys(str(path) for path in meta["removed_paths"]))
    meta["reason_codes"] = list(dict.fromkeys(str(code) for code in meta["reason_codes"]))
    return meta


def _section3_from_money_box(money_box: Dict[str, Any]) -> Dict[str, Any]:
    section3 = copy.deepcopy(money_box)
    total = money_box.get("total_extra_costs") if isinstance(money_box.get("total_extra_costs"), dict) else {}
    if isinstance(total.get("range"), dict):
        min_value = total["range"].get("min")
        max_value = total["range"].get("max")
    else:
        min_value = total.get("min")
        max_value = total.get("max")
    section3["totale_extra_budget"] = {
        "min": min_value,
        "max": max_value,
        "nota": total.get("note") or total.get("nota"),
        "contract_state": total.get("contract_state"),
        "evidence": copy.deepcopy(total.get("evidence", [])),
    }
    return section3


def _build_projected_money_box(money_value: Dict[str, Any], legacy_result: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    raw_candidates = money_value.get("money_candidates") if isinstance(money_value.get("money_candidates"), list) else []
    candidates = [candidate for candidate in raw_candidates if isinstance(candidate, dict)]
    safe_costs = _dedupe_candidates(
        (
            candidate
            for candidate in candidates
            if candidate.get("is_customer_safe_cost")
            and candidate.get("role") != "component_of_total"
            and _candidate_amount(candidate) is not None
        ),
        12,
    )
    signals = _dedupe_candidates(
        (
            candidate
            for candidate in candidates
            if not candidate.get("is_customer_safe_cost")
            and (
                candidate.get("role") in BUYER_SIGNAL_ROLES
                or (
                    candidate.get("role") == "total_candidate"
                    and candidate.get("semantic_base_role") in BUYER_SIGNAL_ROLES
                )
            )
            and candidate.get("role") != "component_of_total"
            and _candidate_amount(candidate) is not None
        ),
        16,
    )
    excluded = _dedupe_candidates(
        (
            candidate
            for candidate in candidates
            if candidate.get("role") in NON_BUYER_COST_ROLES
            and _candidate_amount(candidate) is not None
        ),
        24,
    )
    valuation = [candidate for candidate in excluded if candidate.get("role") in {"valuation_deduction", "price", "base_auction", "final_value", "market_value"}]
    safe_total_candidates = _dedupe_candidates(
        (
            candidate
            for candidate in candidates
            if candidate.get("role") == "total_candidate"
            and candidate.get("is_customer_safe_cost")
            and candidate.get("should_sum")
            and _candidate_amount(candidate) is not None
        ),
        3,
    )
    summary = money_value.get("summary") if isinstance(money_value.get("summary"), dict) else {}
    double_count_risk = bool(summary.get("double_count_risk")) or any(candidate.get("parent_total_candidate_id") for candidate in candidates)
    stale_removed = bool(STALE_REGOLARIZZAZIONE_RE.search(_legacy_money_text(legacy_result)))
    if not (safe_costs or signals or excluded or stale_removed or double_count_risk):
        return None, {
            "safe_cost_count": 0,
            "signal_count": 0,
            "excluded_count": 0,
            "valuation_count": 0,
            "double_count_risk": double_count_risk,
            "stale_removed": stale_removed,
        }

    items = [_cost_signal_payload(candidate, idx, safe_cost=True) for idx, candidate in enumerate(safe_costs, start=1)]
    signal_items = [_cost_signal_payload(candidate, idx, safe_cost=False) for idx, candidate in enumerate(signals, start=1)]
    excluded_items = [_excluded_payload(candidate, idx) for idx, candidate in enumerate(excluded, start=1)]
    valuation_items = [_excluded_payload(candidate, idx) for idx, candidate in enumerate(valuation[:16], start=1)]

    total: Dict[str, Any]
    if safe_total_candidates:
        candidate = safe_total_candidates[0]
        amount = int(round(float(candidate.get("amount_eur"))))
        total = {
            "range": {"min": amount, "max": amount},
            "max_is_open": False,
            "note": "Totale buyer-side esplicitamente supportato in perizia; componenti non sommate una seconda volta.",
            "contract_state": "quantified_estimate",
            "evidence": _evidence_from_candidate(candidate),
        }
    else:
        note = "Oneri non quantificati in modo difendibile: usare le voci come checklist da verificare; nessun totale economico certo e' indicato."
        if not (items or signal_items):
            note = "Nessun costo extra buyer-side certo ricavabile dalla perizia; importi valutativi/procedurali esclusi dal totale."
        total = {
            "min": None,
            "max": None,
            "max_is_open": False,
            "note": note,
            "contract_state": "unresolved_explained" if (items or signal_items) else "info_only",
            "evidence": copy.deepcopy((items or signal_items or excluded_items or [{}])[0].get("evidence", [])),
        }

    money_box = {
        "policy": "AUTHORITY_CONSERVATIVE",
        "items": items,
        "cost_signals_to_verify": signal_items,
        "qualitative_burdens": copy.deepcopy(signal_items),
        "valuation_deductions": [item for item in valuation_items if "valutativo" in str(item.get("label_it") or "").lower()],
        "valuation_reference_amounts": valuation_items,
        "excluded_non_buyer_cost_amounts": excluded_items,
        "unsupported_or_unknown_amounts": [],
        "total_extra_costs": total,
    }
    if double_count_risk:
        money_box["component_total_policy"] = "componenti_non_sommate_con_totale"
    return money_box, {
        "safe_cost_count": len(items),
        "signal_count": len(signal_items),
        "excluded_count": len(excluded_items),
        "valuation_count": len(valuation_items),
        "double_count_risk": double_count_risk,
        "stale_removed": stale_removed,
    }


def _set_money_boxes(result: Dict[str, Any], money_box: Dict[str, Any]) -> List[str]:
    changed: List[str] = []
    section3 = _section3_from_money_box(money_box)
    cdc = result.get("customer_decision_contract") if isinstance(result.get("customer_decision_contract"), dict) else None
    if isinstance(cdc, dict):
        if cdc.get("money_box") != money_box:
            changed.append("customer_decision_contract.money_box")
        cdc["money_box"] = copy.deepcopy(money_box)
        if cdc.get("section_3_money_box") != section3:
            changed.append("customer_decision_contract.section_3_money_box")
        cdc["section_3_money_box"] = copy.deepcopy(section3)
        if "money_box" in result:
            if result.get("money_box") != money_box:
                changed.append("money_box")
            result["money_box"] = copy.deepcopy(money_box)
        if "section_3_money_box" in result:
            if result.get("section_3_money_box") != section3:
                changed.append("section_3_money_box")
            result["section_3_money_box"] = copy.deepcopy(section3)
        return changed

    if result.get("money_box") != money_box:
        changed.append("money_box")
    result["money_box"] = copy.deepcopy(money_box)
    if result.get("section_3_money_box") != section3:
        changed.append("section_3_money_box")
    result["section_3_money_box"] = copy.deepcopy(section3)
    return changed


def apply_authority_money_projection_if_enabled(
    result: Dict[str, Any],
    pages_raw: Optional[Sequence[Dict[str, Any]]] = None,
    section_authority_map: Any = None,
    candidate_artifacts: Any = None,
    *,
    analysis_id: Optional[str] = None,
    authority_shadow: Optional[Dict[str, Any]] = None,
    request_id: Optional[str] = None,
) -> Dict[str, Any]:
    enabled = os.environ.get(FEATURE_FLAG) == "1"
    meta = _base_meta(enabled)
    if analysis_id:
        meta["analysis_id"] = str(analysis_id)
    if request_id:
        meta["request_id"] = str(request_id)
    if not enabled:
        return meta
    if not isinstance(result, dict):
        meta.update({"status": "FAIL_OPEN", "reason": "invalid_result", "fail_open": True})
        return meta

    shadow, build_notes = _shadow_from_inputs(pages_raw, section_authority_map, candidate_artifacts, authority_shadow)
    meta["notes"].extend(build_notes)
    if not isinstance(shadow, dict):
        meta.update({"status": "FAIL_OPEN", "reason": "missing_or_invalid_authority_shadow", "fail_open": True})
        _attach_meta(result, meta)
        return meta

    money_row = _money_row(shadow)
    money_value = _money_value(money_row)
    money_status = str(money_row.get("status") or "unknown")
    confidence = float(money_row.get("confidence") or 0.0)
    notes = [str(note) for note in (money_row.get("notes") or [])]
    candidates = money_value.get("money_candidates") if isinstance(money_value.get("money_candidates"), list) else []
    meta.update(
        {
            "money_status": money_status,
            "authority_confidence": round(confidence, 4),
            "candidate_count": len(candidates),
        }
    )
    meta["notes"].extend(notes)

    if bool(money_row.get("fail_open")) or money_status == "FAIL_OPEN" or "mostly_unknown_authority_map" in notes:
        meta.update({"status": "FAIL_OPEN", "reason": "authority_money_fail_open", "fail_open": True})
        _attach_meta(result, meta)
        return meta
    if money_status not in SAFE_MONEY_STATUSES:
        meta.update({"status": "INSUFFICIENT_EVIDENCE", "reason": "authority_money_not_projectable"})
        _attach_meta(result, meta)
        return meta
    if confidence < 0.55:
        meta.update({"status": "NOT_APPLIED_LOW_CONFIDENCE", "reason": "authority_money_low_confidence"})
        _attach_meta(result, meta)
        return meta

    projected, stats = _build_projected_money_box(money_value, result)
    meta["component_total_double_count_prevented"] = bool(stats.get("double_count_risk"))
    meta["stale_money_removed"] = bool(stats.get("stale_removed"))
    if not isinstance(projected, dict):
        meta.update({"status": "NOT_APPLIED_NO_ACTIONABLE_AUTHORITY", "reason": "no_projectable_money_roles"})
        _attach_meta(result, meta)
        return meta

    changed_fields = _set_money_boxes(result, projected)
    meta.update(
        {
            "status": "APPLIED" if changed_fields else "ALREADY_MATCHES",
            "applied": bool(changed_fields),
            "reason": "authority_money_projection_applied" if changed_fields else "authority_money_projection_already_matches",
            "projected_items_count": len(projected.get("items") or []),
            "cost_signals_to_verify_count": int(stats.get("signal_count") or 0),
            "excluded_non_buyer_cost_count": int(stats.get("excluded_count") or 0),
            "valuation_reference_count": int(stats.get("valuation_count") or 0),
            "changed_fields": changed_fields,
        }
    )
    if changed_fields:
        qa_sanitize_meta = _sanitize_stale_money_qa_claims_after_projection(result, projected)
        removed_count = int(qa_sanitize_meta.get("removed_money_qa_claims_count") or 0)
        meta["removed_money_qa_claims_count"] = removed_count
        meta["removed_paths"] = qa_sanitize_meta.get("removed_paths") or []
        meta["qa_money_claim_sanitizer_reason_codes"] = qa_sanitize_meta.get("reason_codes") or []
        if removed_count:
            meta["notes"].append("stale_money_qa_claims_removed")
    _attach_meta(result, meta)
    return meta


def _attach_meta(result: Dict[str, Any], meta: Dict[str, Any]) -> None:
    if not isinstance(result, dict):
        return
    debug = result.get("debug") if isinstance(result.get("debug"), dict) else {}
    debug["authority_money_projection"] = copy.deepcopy(meta)
    result["debug"] = debug
