"""Feature-flagged customer projection for authority-backed lot structure.

Phase 3C keeps this intentionally narrow: it may adjust only lot-structure
fields when the shadow lot resolver is high-confidence and non-fail-open.
"""

from __future__ import annotations

import copy
import json
import os
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


FEATURE_FLAG = "AUTHORITY_LOT_PROJECTION_ENABLED"
MIN_CONFIDENCE = 0.85
LOT_RULES = {
    "high_authority_lotto_unico_beats_toc_context_and_generic_lot_mentions",
    "high_authority_multilot_beats_toc_context_and_generic_lot_mentions",
    "chapter_based_multi_lot_topology",
}
APPLIED_STATUSES = {"APPLIED_AUTHORITY_SINGLE_LOT", "APPLIED_AUTHORITY_MULTI_LOT"}

_LOT_NUMBER_RE = re.compile(r"\blott[oi]\s*(?:n(?:\.|umero)?\s*)?([0-9]{1,3})\b", re.I)
_LOTTI_LIST_RE = re.compile(r"\blotti\s+([0-9]{1,3}(?:\s*(?:,|e|/|-|–)\s*[0-9]{1,3})+)", re.I)
_DIGIT_LOT_COUNT_RE = re.compile(r"\b([0-9]{1,3})\s+lotti\b", re.I)
_WORD_LOT_COUNT_RE = re.compile(r"\b(due|tre|quattro|cinque|sei|sette|otto|nove|dieci)\s+lotti\b", re.I)
_GENERIC_MULTI_LOT_CLAIM_RE = re.compile(
    r"\b(?:pi[ùu]\s+lotti|lotti\s+multipli|multipli\s+lotti|multi[-\s]?lotto|multi[-\s]?lot)\b",
    re.I,
)
_COUNT_WORDS = {
    "due": 2,
    "tre": 3,
    "quattro": 4,
    "cinque": 5,
    "sei": 6,
    "sette": 7,
    "otto": 8,
    "nove": 9,
    "dieci": 10,
}
_ACTIVE_LOT_TEXT_KEYS = {
    "summary_it",
    "summary_en",
    "factual_summary_it",
    "factual_summary_en",
    "decision_summary_it",
    "decision_summary_en",
    "main_risk_it",
    "main_risk_en",
    "why_it_matters_it",
    "why_it_matters_en",
    "not_to_confuse_it",
    "not_to_confuse_en",
    "killer",
    "detail",
    "description",
    "text",
    "title_it",
    "title_en",
    "flag_it",
    "flag_en",
    "action_it",
    "action_en",
    "label_it",
    "label_en",
    "problem_it",
    "problem_en",
    "current_wrong_claim",
}
_STALE_ITEM_LIST_KEYS = {
    "items",
    "top_items",
    "legal_killers",
    "red_flags",
    "red_flags_operativi",
    "section_11_red_flags",
    "warnings",
    "driver_rosso",
}


def _meta(enabled: bool, status: str, reason: str = "") -> Dict[str, Any]:
    return {
        "enabled": bool(enabled),
        "status": status,
        "applied": False,
        "reason": reason,
        "legacy_lot_mode": "unknown",
        "authority_lot_mode": "unknown",
        "authority_confidence": 0.0,
        "detected_lot_numbers": [],
        "source_pages": [],
        "changed_fields": [],
    }


def _safe_int(value: Any) -> Optional[int]:
    try:
        parsed = int(value)
    except Exception:
        return None
    return parsed if parsed > 0 else None


def _as_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _lot_number(lot: Dict[str, Any]) -> Optional[int]:
    for key in ("lot_number", "lot", "lotto", "numero_lotto"):
        parsed = _safe_int(lot.get(key))
        if parsed is not None:
            return parsed
    return None


def _lot_mode_from_result(result: Dict[str, Any]) -> str:
    lots = result.get("lots")
    if bool(result.get("is_multi_lot")):
        return "multi_lot"
    if isinstance(lots, list):
        dict_lots = [lot for lot in lots if isinstance(lot, dict)]
        if len(dict_lots) >= 2:
            return "multi_lot"
        if len(dict_lots) == 1:
            return "single_lot"
    for key in ("lots_count", "lot_count"):
        parsed = _safe_int(result.get(key))
        if parsed is None:
            continue
        if parsed >= 2:
            return "multi_lot"
        if parsed == 1:
            return "single_lot"
    text_parts: List[str] = []
    case_header = result.get("case_header") if isinstance(result.get("case_header"), dict) else {}
    report_header = result.get("report_header") if isinstance(result.get("report_header"), dict) else {}
    text_parts.append(str(case_header.get("lotto") or ""))
    report_lotto = report_header.get("lotto")
    if isinstance(report_lotto, dict):
        text_parts.append(str(report_lotto.get("value") or ""))
    else:
        text_parts.append(str(report_lotto or ""))
    text = " ".join(text_parts).lower()
    if "lotto unico" in text or "unico lotto" in text:
        return "single_lot"
    if "lotti" in text:
        return "multi_lot"
    return "unknown"


def _source_pages(lot_shadow: Dict[str, Any]) -> List[int]:
    pages: List[int] = []
    basis = lot_shadow.get("authority_basis") if isinstance(lot_shadow.get("authority_basis"), dict) else {}
    for item in _as_list(basis.get("pages_used")):
        parsed = _safe_int(item)
        if parsed is not None and parsed not in pages:
            pages.append(parsed)
    for ev in _as_list(lot_shadow.get("winning_evidence")):
        if not isinstance(ev, dict):
            continue
        parsed = _safe_int(ev.get("page"))
        if parsed is not None and parsed not in pages:
            pages.append(parsed)
    return sorted(pages)


def _rules(lot_shadow: Dict[str, Any]) -> List[str]:
    basis = lot_shadow.get("authority_basis") if isinstance(lot_shadow.get("authority_basis"), dict) else {}
    return [str(rule) for rule in _as_list(basis.get("rules_triggered")) if str(rule or "").strip()]


def _dedupe_numbers(values: Iterable[Any]) -> List[int]:
    out: List[int] = []
    for value in values:
        parsed = _safe_int(value)
        if parsed is not None and parsed not in out:
            out.append(parsed)
    return sorted(out)


def _authority_decision(authority_shadow: Any) -> Tuple[str, Dict[str, Any], str]:
    if not isinstance(authority_shadow, dict):
        return "unknown", {}, "missing_or_invalid_authority_shadow"
    if authority_shadow.get("fail_open") is True or str(authority_shadow.get("status") or "") == "FAIL_OPEN":
        return "unknown", {}, "authority_shadow_fail_open"
    warnings = [str(item) for item in _as_list(authority_shadow.get("warnings"))]
    if any("mostly_unknown_authority_map" in item for item in warnings):
        return "unknown", {}, "mostly_unknown_authority_map"

    lot_shadow = authority_shadow.get("lot_structure")
    if not isinstance(lot_shadow, dict):
        return "unknown", {}, "missing_lot_structure_shadow"
    if lot_shadow.get("fail_open") is True or str(lot_shadow.get("status") or "") == "FAIL_OPEN":
        return "unknown", lot_shadow, "lot_structure_fail_open"
    value = lot_shadow.get("value") if isinstance(lot_shadow.get("value"), dict) else {}
    notes = [str(item) for item in _as_list(lot_shadow.get("notes"))]
    if any("mostly_unknown_authority_map" in item for item in notes):
        return "unknown", lot_shadow, "mostly_unknown_authority_map"
    if value.get("chapter_topology_conflicts"):
        return "unknown", lot_shadow, "chapter_topology_conflict"

    try:
        confidence = float(lot_shadow.get("confidence") or 0.0)
    except Exception:
        confidence = 0.0
    if confidence < MIN_CONFIDENCE:
        return "unknown", lot_shadow, "low_confidence"

    mode = str(value.get("shadow_lot_mode") or "unknown")
    rules = set(_rules(lot_shadow))
    if not rules.intersection(LOT_RULES):
        return "unknown", lot_shadow, "missing_projection_safe_lot_rule"
    if not _as_list(lot_shadow.get("winning_evidence")):
        return "unknown", lot_shadow, "missing_winning_evidence"

    if mode == "single_lot":
        if not bool(value.get("has_high_authority_lotto_unico")):
            return "unknown", lot_shadow, "single_lot_without_high_authority_lotto_unico"
        return "single_lot", lot_shadow, "high_authority_single_lot"

    if mode == "multi_lot":
        numbers = _dedupe_numbers(value.get("detected_lot_numbers") or value.get("chapter_lot_numbers") or [])
        if len(numbers) < 2 or not bool(value.get("has_high_authority_multilot")):
            return "unknown", lot_shadow, "multi_lot_without_distinct_high_authority_lots"
        return "multi_lot", lot_shadow, "high_authority_multi_lot"

    return "unknown", lot_shadow, "authority_lot_mode_unknown"


def _json_key(value: Dict[str, Any]) -> str:
    try:
        return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)
    except Exception:
        return str(id(value))


def _collect_beni(result: Dict[str, Any], lots: Sequence[Any]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    seen = set()
    candidates: List[Any] = []
    if isinstance(result.get("beni"), list):
        candidates.extend(result.get("beni") or [])
    for lot in lots:
        if isinstance(lot, dict) and isinstance(lot.get("beni"), list):
            candidates.extend(lot.get("beni") or [])
    for item in candidates:
        if not isinstance(item, dict):
            continue
        key = _json_key(item)
        if key in seen:
            continue
        seen.add(key)
        items.append(copy.deepcopy(item))
    return items


def _lot_index_entries(lots: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for lot in lots:
        number = _lot_number(lot)
        if number is None:
            continue
        evidence = lot.get("evidence") if isinstance(lot.get("evidence"), dict) else {}
        lotto_evidence = evidence.get("lotto") if isinstance(evidence.get("lotto"), list) else []
        first_ev = lotto_evidence[0] if lotto_evidence and isinstance(lotto_evidence[0], dict) else {}
        entries.append(
            {
                "lot": number,
                "prezzo": lot.get("prezzo_base_eur"),
                "ubicazione": str(lot.get("ubicazione") or "")[:80],
                "page": first_ev.get("page"),
                "quote": first_ev.get("quote"),
            }
        )
    return entries


def _lot_label(mode: str, numbers: Sequence[int]) -> str:
    if mode == "single_lot":
        return "Lotto Unico"
    return "Lotti " + ", ".join(str(num) for num in numbers)


def _set_lot_header_fields(result: Dict[str, Any], mode: str, lots: List[Dict[str, Any]], changed: List[str]) -> None:
    numbers = [_lot_number(lot) or idx for idx, lot in enumerate(lots, start=1)]
    label = _lot_label(mode, numbers)
    for key, value in (("lots", lots), ("lots_count", len(lots)), ("is_multi_lot", mode == "multi_lot")):
        if result.get(key) != value:
            changed.append(key)
        result[key] = value
    if "lot_count" in result:
        if result.get("lot_count") != len(lots):
            changed.append("lot_count")
        result["lot_count"] = len(lots)

    case_header = result.get("case_header")
    if isinstance(case_header, dict) and "lotto" in case_header:
        if case_header.get("lotto") != label:
            changed.append("case_header.lotto")
        case_header["lotto"] = label

    report_header = result.get("report_header")
    if isinstance(report_header, dict):
        if isinstance(report_header.get("lotto"), dict):
            if report_header["lotto"].get("value") != label:
                changed.append("report_header.lotto.value")
            report_header["lotto"]["value"] = label
        elif "lotto" in report_header:
            if report_header.get("lotto") != label:
                changed.append("report_header.lotto")
            report_header["lotto"] = label
        if "is_multi_lot" in report_header:
            projected_multi = mode == "multi_lot"
            if report_header.get("is_multi_lot") != projected_multi:
                changed.append("report_header.is_multi_lot")
            report_header["is_multi_lot"] = projected_multi

    if isinstance(result.get("lot_index"), list):
        new_index = _lot_index_entries(lots)
        if result.get("lot_index") != new_index:
            changed.append("lot_index")
        result["lot_index"] = new_index


def _apply_single_lot(result: Dict[str, Any]) -> List[str]:
    changed: List[str] = []
    existing_lots = result.get("lots") if isinstance(result.get("lots"), list) else []
    base = copy.deepcopy(next((lot for lot in existing_lots if isinstance(lot, dict)), {}))
    base["lot_number"] = 1
    base["lot_id"] = str(base.get("lot_id") or "1")
    beni = _collect_beni(result, existing_lots)
    if beni:
        base["beni"] = beni
    _set_lot_header_fields(result, "single_lot", [base], changed)
    return list(dict.fromkeys(changed))


def _apply_multi_lot(result: Dict[str, Any], numbers: Sequence[int]) -> List[str]:
    changed: List[str] = []
    distinct_numbers = _dedupe_numbers(numbers)
    existing_lots = result.get("lots") if isinstance(result.get("lots"), list) else []
    existing_by_number: Dict[int, Dict[str, Any]] = {}
    first_existing = next((lot for lot in existing_lots if isinstance(lot, dict)), None)
    for lot in existing_lots:
        if not isinstance(lot, dict):
            continue
        number = _lot_number(lot)
        if number is not None and number not in existing_by_number:
            existing_by_number[number] = copy.deepcopy(lot)

    projected: List[Dict[str, Any]] = []
    used_first_existing = False
    for number in distinct_numbers:
        if number in existing_by_number:
            lot = copy.deepcopy(existing_by_number[number])
        elif not used_first_existing and isinstance(first_existing, dict):
            lot = copy.deepcopy(first_existing)
            used_first_existing = True
        else:
            lot = {}
        lot["lot_number"] = number
        lot["lot_id"] = str(lot.get("lot_id") or number)
        projected.append(lot)
    if not projected:
        return changed
    _set_lot_header_fields(result, "multi_lot", projected, changed)
    return list(dict.fromkeys(changed))


def apply_authority_lot_projection_if_enabled(
    result: Dict[str, Any],
    authority_shadow: Any,
    *,
    request_id: Optional[str] = None,
) -> Dict[str, Any]:
    enabled = os.environ.get(FEATURE_FLAG) == "1"
    meta = _meta(enabled=enabled, status="DISABLED" if not enabled else "NOT_EVALUATED", reason="feature_flag_disabled" if not enabled else "")
    if not isinstance(result, dict):
        meta.update({"status": "FAIL_OPEN", "reason": "invalid_result"})
        return meta
    meta["legacy_lot_mode"] = _lot_mode_from_result(result)
    if not enabled:
        return meta

    mode, lot_shadow, reason = _authority_decision(authority_shadow)
    value = lot_shadow.get("value") if isinstance(lot_shadow.get("value"), dict) else {}
    try:
        meta["authority_confidence"] = round(float(lot_shadow.get("confidence") or 0.0), 4)
    except Exception:
        meta["authority_confidence"] = 0.0
    meta["authority_lot_mode"] = mode
    meta["detected_lot_numbers"] = _dedupe_numbers(value.get("detected_lot_numbers") or value.get("chapter_lot_numbers") or [])
    meta["source_pages"] = _source_pages(lot_shadow) if isinstance(lot_shadow, dict) else []
    meta["reason"] = reason
    if mode not in {"single_lot", "multi_lot"}:
        meta["status"] = "FAIL_OPEN" if "fail_open" in reason or "missing" in reason or "corrupt" in reason else "NOT_APPLIED"
        return meta

    if mode == "single_lot":
        changed = _apply_single_lot(result)
        meta["status"] = "APPLIED_AUTHORITY_SINGLE_LOT" if changed else "ALREADY_MATCHES"
    else:
        changed = _apply_multi_lot(result, meta["detected_lot_numbers"])
        meta["status"] = "APPLIED_AUTHORITY_MULTI_LOT" if changed else "ALREADY_MATCHES"
    meta["changed_fields"] = changed
    meta["applied"] = bool(changed)
    return meta


def _numbers_from_lot_text(text: str) -> List[int]:
    numbers = [_safe_int(match.group(1)) for match in _LOT_NUMBER_RE.finditer(text or "")]
    for match in _LOTTI_LIST_RE.finditer(text or ""):
        numbers.extend(_safe_int(item) for item in re.findall(r"[0-9]{1,3}", match.group(1) or ""))
    return _dedupe_numbers(item for item in numbers if item is not None)


def _count_claims_from_lot_text(text: str) -> List[int]:
    counts = [_safe_int(match.group(1)) for match in _DIGIT_LOT_COUNT_RE.finditer(text or "")]
    for match in _WORD_LOT_COUNT_RE.finditer(text or ""):
        counts.append(_COUNT_WORDS.get(str(match.group(1) or "").lower()))
    return [count for count in counts if count is not None]


def _projected_lot_numbers(result: Dict[str, Any], projection_meta: Dict[str, Any]) -> List[int]:
    lots = result.get("lots") if isinstance(result.get("lots"), list) else []
    numbers = [_lot_number(lot) for lot in lots if isinstance(lot, dict)]
    if not numbers:
        numbers = [_safe_int(item) for item in projection_meta.get("detected_lot_numbers") or []]
    numbers = _dedupe_numbers(item for item in numbers if item is not None)
    if numbers:
        return numbers
    count = _safe_int(result.get("lots_count") or result.get("lot_count"))
    if count and count > 1:
        return list(range(1, count + 1))
    return [1]


def _has_stale_lot_text(text: Any, mode: str, valid_numbers: Sequence[int]) -> bool:
    if not isinstance(text, str) or not text.strip():
        return False
    valid = set(valid_numbers or [1])
    numbers = _numbers_from_lot_text(text)
    counts = _count_claims_from_lot_text(text)
    if mode == "single_lot":
        if _GENERIC_MULTI_LOT_CLAIM_RE.search(text):
            return True
        if any(count != 1 for count in counts):
            return True
        return any(number not in valid for number in numbers)
    if mode == "multi_lot":
        projected_count = len(valid)
        if any(count != projected_count for count in counts):
            return True
        return any(number not in valid for number in numbers)
    return False


def _clean_sentence_spacing(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", text or "").strip()
    cleaned = re.sub(r"\s+([,.;:!?])", r"\1", cleaned)
    return cleaned


def _remove_stale_lot_sentences(text: str, mode: str, valid_numbers: Sequence[int]) -> Optional[str]:
    if not _has_stale_lot_text(text, mode, valid_numbers):
        return text
    parts = re.split(r"(?<=[.!?;])\s+", text)
    kept = [part.strip() for part in parts if part.strip() and not _has_stale_lot_text(part, mode, valid_numbers)]
    cleaned = _clean_sentence_spacing(" ".join(kept))
    if cleaned and not _has_stale_lot_text(cleaned, mode, valid_numbers):
        return cleaned

    downgraded = re.sub(r"^\s*Lotto\s+[0-9]{1,3}\s*[:\-–]\s*", "", text, flags=re.I)
    downgraded = _clean_sentence_spacing(downgraded)
    if downgraded and downgraded != _clean_sentence_spacing(text) and not _has_stale_lot_text(downgraded, mode, valid_numbers):
        return downgraded[:1].upper() + downgraded[1:]
    return None


def _item_has_stale_active_lot_text(item: Any, mode: str, valid_numbers: Sequence[int]) -> bool:
    if isinstance(item, dict):
        for key, child in item.items():
            if isinstance(child, str) and str(key) in _ACTIVE_LOT_TEXT_KEYS and _has_stale_lot_text(child, mode, valid_numbers):
                return True
            if isinstance(child, dict) and _item_has_stale_active_lot_text(child, mode, valid_numbers):
                return True
            if isinstance(child, list):
                for nested in child:
                    if isinstance(nested, dict) and _item_has_stale_active_lot_text(nested, mode, valid_numbers):
                        return True
                    if isinstance(nested, str) and str(key) in _ACTIVE_LOT_TEXT_KEYS and _has_stale_lot_text(nested, mode, valid_numbers):
                        return True
    return False


def _sanitize_stale_lot_texts(
    value: Any,
    *,
    path: str,
    key: str = "",
    list_key: str = "",
    mode: str,
    valid_numbers: Sequence[int],
    removed_paths: List[str],
) -> Any:
    if isinstance(value, dict):
        for child_key in list(value.keys()):
            child_path = f"{path}.{child_key}"
            child_list_key = str(child_key) if isinstance(value[child_key], list) else list_key
            sanitized = _sanitize_stale_lot_texts(
                value[child_key],
                path=child_path,
                key=str(child_key),
                list_key=child_list_key,
                mode=mode,
                valid_numbers=valid_numbers,
                removed_paths=removed_paths,
            )
            if sanitized is None and str(child_key) in _ACTIVE_LOT_TEXT_KEYS:
                removed_paths.append(child_path)
                value.pop(child_key, None)
            else:
                value[child_key] = sanitized
        return value

    if isinstance(value, list):
        sanitized_items: List[Any] = []
        for idx, item in enumerate(value):
            item_path = f"{path}[{idx}]"
            if list_key in _STALE_ITEM_LIST_KEYS and _item_has_stale_active_lot_text(item, mode, valid_numbers):
                removed_paths.append(item_path)
                continue
            sanitized = _sanitize_stale_lot_texts(
                item,
                path=item_path,
                key="",
                list_key=list_key,
                mode=mode,
                valid_numbers=valid_numbers,
                removed_paths=removed_paths,
            )
            if sanitized is None:
                removed_paths.append(item_path)
                continue
            sanitized_items.append(sanitized)
        return sanitized_items

    if isinstance(value, str) and key in _ACTIVE_LOT_TEXT_KEYS and _has_stale_lot_text(value, mode, valid_numbers):
        return _remove_stale_lot_sentences(value, mode, valid_numbers)

    if isinstance(value, str) and list_key in _STALE_ITEM_LIST_KEYS and _has_stale_lot_text(value, mode, valid_numbers):
        return _remove_stale_lot_sentences(value, mode, valid_numbers)

    return value


def sanitize_stale_lot_narratives_after_projection(
    result: Dict[str, Any],
    projection_meta: Dict[str, Any],
) -> Dict[str, Any]:
    """Scrub customer-visible lot text that contradicts an applied authority projection.

    The function mutates only the supplied response object. It returns metadata for
    internal debug/runtime storage; callers must not place that metadata in the
    customer-visible result.
    """
    enabled = os.environ.get(FEATURE_FLAG) == "1"
    status = str((projection_meta or {}).get("status") or "")
    mode = str((projection_meta or {}).get("authority_lot_mode") or "")
    meta = {
        "enabled": enabled,
        "status": "SKIPPED",
        "projection_status": status,
        "projection_mode": mode,
        "removed_stale_lot_narrative_count": 0,
        "removed_paths": [],
    }
    if not enabled or status not in APPLIED_STATUSES or mode not in {"single_lot", "multi_lot"}:
        return meta
    if not isinstance(result, dict):
        meta["status"] = "FAIL_OPEN"
        return meta

    valid_numbers = _projected_lot_numbers(result, projection_meta)
    removed_paths: List[str] = []
    _sanitize_stale_lot_texts(
        result,
        path="result",
        mode=mode,
        valid_numbers=valid_numbers,
        removed_paths=removed_paths,
    )
    meta["status"] = "APPLIED" if removed_paths else "NO_STALE_LOT_NARRATIVE"
    meta["removed_stale_lot_narrative_count"] = len(removed_paths)
    meta["removed_paths"] = list(dict.fromkeys(removed_paths))
    return meta
