"""
LLM clarification issue packet builder.

Produces: clarification_issue_pack.json

This stage does not call an LLM. It converts deterministic blocked/conflict
surfaces into bounded, evidence-backed issue packets that a later clarification
stage may inspect.
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from .corpus_registry import list_case_keys
from .runner import build_context


ALLOWED_ISSUE_TYPES = {
    "FIELD_CONFLICT",
    "SUSPICIOUS_SILENCE",
    "SCOPE_AMBIGUITY",
    "GROUPED_CONTEXT_NEEDS_EXPLANATION",
    "OCR_VARIANT_COLLISION",
    "TABLE_RECAP_DUPLICATE_UNCLEAR",
}

SOURCE_ARTIFACTS = [
    "location_candidate_pack.json",
    "rights_candidate_pack.json",
    "occupancy_candidate_pack.json",
    "valuation_candidate_pack.json",
    "cost_candidate_pack.json",
    "impianti_candidate_pack.json",
    "table_zone_map.json",
    "lot_scope_map.json",
    "bene_scope_map.json",
    "raw_pages.json",
]

PACK_FAMILIES = {
    "location_candidate_pack.json": "location",
    "rights_candidate_pack.json": "rights",
    "occupancy_candidate_pack.json": "occupancy",
    "valuation_candidate_pack.json": "valuation",
    "cost_candidate_pack.json": "cost",
    "impianti_candidate_pack.json": "impianti",
}

_TEXT_KEYS = {
    "quote",
    "context",
    "context_window",
    "line_quote",
    "reason",
    "scope_basis",
    "extracted_value",
    "occupancy_status_raw",
}


def _read_json(path: Path, default):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _norm(text: object) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _compact_dict(row: Dict, keys: Sequence[str]) -> Dict:
    return {k: row.get(k) for k in keys if k in row and row.get(k) is not None}


def _field_family_from_artifact(artifact_name: str) -> str:
    return PACK_FAMILIES.get(artifact_name, artifact_name.replace("_candidate_pack.json", ""))


def _issue_type(block_type: str, row: Dict) -> str:
    upper = str(block_type or "").upper()
    reason = str(row.get("reason") or "").upper()
    haystack = f"{upper} {reason}"
    if "OCR" in haystack:
        return "OCR_VARIANT_COLLISION"
    if "MULTI_VALUE" in haystack or "FIELD_CONFLICT" in haystack or "CONFLICT" in haystack:
        return "FIELD_CONFLICT"
    if "SUMMARY_DUPLICATE" in haystack or "RECAP" in haystack or "DUPLICATE" in haystack:
        return "TABLE_RECAP_DUPLICATE_UNCLEAR"
    if "SCOPE" in haystack or "TRANSITION" in haystack or "LOCAL_LAST_BENE" in haystack:
        return "SCOPE_AMBIGUITY"
    if "CONTEXT" in haystack or "NOT_PROMOTABLE" in haystack or "GROUP" in haystack:
        return "GROUPED_CONTEXT_NEEDS_EXPLANATION"
    return "SUSPICIOUS_SILENCE"


def _needs_llm(issue_type: str) -> bool:
    return issue_type in ALLOWED_ISSUE_TYPES


def _reason_codes(row: Dict) -> List[str]:
    codes = []
    if row.get("type"):
        codes.append(str(row["type"]))
    if row.get("extraction_method"):
        codes.append(str(row["extraction_method"]))
    if row.get("attribution"):
        codes.append(str(row["attribution"]))
    return codes


def _candidate_projection(candidate: Dict) -> Dict:
    return _compact_dict(
        candidate,
        [
            "candidate_id",
            "field_type",
            "extracted_value",
            "page",
            "line_index",
            "quote",
            "context",
            "context_window",
            "candidate_status",
            "attribution",
            "source_type",
            "scope_basis",
            "lot_id",
            "bene_id",
            "corpo_id",
            "composite_key",
            "extraction_method",
        ],
    )


def _candidate_values(row: Dict) -> List[str]:
    values: List[str] = []
    for candidate in row.get("candidates") or []:
        value = candidate.get("extracted_value")
        if value is not None and str(value).strip() not in values:
            values.append(str(value).strip())
    for value in row.get("distinct_values") or []:
        if value is not None and str(value).strip() not in values:
            values.append(str(value).strip())
    return values


def _blocked_values(row: Dict) -> List[str]:
    values: List[str] = []
    for key in ("extracted_value", "occupancy_status_raw"):
        value = row.get(key)
        if value is not None and str(value).strip() not in values:
            values.append(str(value).strip())
    for value in row.get("distinct_values") or []:
        if value is not None and str(value).strip() not in values:
            values.append(str(value).strip())
    return values


def _source_line_indices(row: Dict) -> List[int]:
    line_indices: Set[int] = set()
    if isinstance(row.get("line_index"), int):
        line_indices.add(row["line_index"])
    for candidate in row.get("candidates") or []:
        if isinstance(candidate.get("line_index"), int):
            line_indices.add(candidate["line_index"])
    return sorted(line_indices)


def _shell_quotes(row: Dict) -> List[str]:
    quotes: List[str] = []
    for key in ("quote", "line_quote", "context"):
        value = _norm(row.get(key))
        if value and value not in quotes:
            quotes.append(value)
    for candidate in row.get("candidates") or []:
        for key in ("quote", "context", "context_window"):
            value = _norm(candidate.get(key))
            if value and value not in quotes:
                quotes.append(value)
    return quotes


def _blocked_projection(row: Dict) -> Dict:
    out = _compact_dict(
        row,
        [
            "type",
            "reason",
            "field_type",
            "page",
            "line_index",
            "line_quote",
            "quote",
            "context",
            "extracted_value",
            "distinct_values",
            "candidate_count",
            "duplicate_of",
            "extraction_method",
            "lot_id",
            "bene_id",
            "scope_key",
            "attribution_bucket",
            "occupancy_status_raw",
        ],
    )
    if row.get("candidates"):
        out["candidates"] = [_candidate_projection(c) for c in row.get("candidates") or []]
    return out


def _collect_pages(row: Dict) -> List[int]:
    pages: Set[int] = set()
    for key in ("page", "source_pages"):
        value = row.get(key)
        if isinstance(value, int):
            pages.add(value)
        elif isinstance(value, list):
            for p in value:
                if isinstance(p, int):
                    pages.add(p)
    for candidate in row.get("candidates") or []:
        if isinstance(candidate.get("page"), int):
            pages.add(candidate["page"])
    return sorted(pages)


def _text_needles(row: Dict) -> List[str]:
    needles: List[str] = []
    for key in _TEXT_KEYS:
        value = _norm(row.get(key))
        if value:
            needles.append(value)
    for candidate in row.get("candidates") or []:
        for key in _TEXT_KEYS:
            value = _norm(candidate.get(key))
            if value:
                needles.append(value)
    return needles


def _window_for_page(raw_pages_by_num: Dict[int, Dict], page: int, line_index: Optional[int], needles: Sequence[str]) -> Dict:
    text = raw_pages_by_num.get(page, {}).get("text", "") or ""
    lines = text.splitlines()
    if not lines:
        return {"window_type": "exact_evidence_window", "page": page, "line_start": None, "line_end": None, "text": ""}

    anchor = line_index if isinstance(line_index, int) else None
    if anchor is None:
        folded_lines = [_norm(line).lower() for line in lines]
        for needle in needles:
            short = _norm(needle).lower()
            if not short:
                continue
            short = short[:140]
            for idx, line in enumerate(folded_lines):
                if short and short in line:
                    anchor = idx
                    break
            if anchor is not None:
                break
    if anchor is None:
        anchor = 0

    start = max(0, anchor - 12)
    end = min(len(lines), anchor + 13)
    snippet = "\n".join(line for line in lines[start:end] if line.strip())
    return {
        "window_type": "exact_evidence_window",
        "page": page,
        "anchor_line_index": anchor,
        "line_start": start,
        "line_end": end - 1,
        "text": snippet[:4500],
    }


def _bounded_page_text(raw_pages_by_num: Dict[int, Dict], page: int, max_len: int = 4500) -> Dict:
    text = raw_pages_by_num.get(page, {}).get("text", "") or ""
    return {"page": page, "text": text[:max_len]}


def _scope_key_from_row(row: Dict[str, Any]) -> Optional[str]:
    scope_key = row.get("scope_key")
    if isinstance(scope_key, str) and scope_key:
        return scope_key
    lot_id = row.get("lot_id")
    bene_id = row.get("bene_id")
    if lot_id not in {None, ""} and bene_id not in {None, ""}:
        return f"bene:{lot_id}/{bene_id}"
    if lot_id not in {None, ""}:
        return f"lot:{lot_id}"
    return None


def _build_scope_lookup(scope_rows: Sequence[Dict[str, Any]], key_field: str) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for row in scope_rows:
        key = row.get(key_field)
        if isinstance(key, str) and key:
            lookup[key] = row
    return lookup


def _scope_kind(scope_key: Optional[str]) -> str:
    if isinstance(scope_key, str):
        if scope_key.startswith("bene:"):
            return "bene"
        if scope_key.startswith("lot:"):
            return "lot"
        if scope_key == "document":
            return "document"
    return "unknown"


def _scope_rows_for_pages(
    scope_rows: Sequence[Dict[str, Any]],
    pages: Sequence[int],
    *,
    key_field: str,
) -> List[Dict[str, Any]]:
    if not pages:
        return []
    page_set = set(pages)
    matches: List[Dict[str, Any]] = []
    for row in scope_rows:
        start_page = row.get("start_page")
        end_page = row.get("end_page")
        key = row.get(key_field)
        if not isinstance(start_page, int) or not isinstance(end_page, int):
            continue
        if not isinstance(key, str) or not key:
            continue
        if all(start_page <= page <= end_page for page in page_set):
            matches.append(row)
    return matches


def _infer_scope_from_pages(
    pages: Sequence[int],
    lot_scope_map: Dict[str, Any],
    bene_scope_map: Dict[str, Any],
) -> Dict[str, Any]:
    bene_matches = _scope_rows_for_pages(
        bene_scope_map.get("bene_scopes") or [],
        pages,
        key_field="composite_key",
    )
    if len(bene_matches) == 1:
        row = bene_matches[0]
        return {
            "scope_key": f"bene:{row['composite_key']}",
            "scope_row": row,
            "lot_row": None,
            "scope_status": "inferred_from_page_containment",
        }

    lot_matches = _scope_rows_for_pages(
        lot_scope_map.get("lot_scopes") or [],
        pages,
        key_field="lot_id",
    )
    if len(lot_matches) == 1:
        row = lot_matches[0]
        return {
            "scope_key": f"lot:{row['lot_id']}",
            "scope_row": row,
            "lot_row": row,
            "scope_status": "inferred_from_page_containment",
        }

    if len(bene_matches) > 1 or len(lot_matches) > 1:
        return {
            "scope_key": None,
            "scope_row": None,
            "lot_row": None,
            "scope_status": "ambiguous_from_page_containment",
        }

    return {
        "scope_key": None,
        "scope_row": None,
        "lot_row": None,
        "scope_status": "missing_scope",
    }


def _issue_scope_context(
    row: Dict[str, Any],
    pages: Sequence[int],
    lot_scope_map: Dict[str, Any],
    bene_scope_map: Dict[str, Any],
) -> Dict[str, Any]:
    scope_key = _scope_key_from_row(row)
    lot_lookup = _build_scope_lookup(lot_scope_map.get("lot_scopes") or [], "lot_id")
    bene_lookup = _build_scope_lookup(bene_scope_map.get("bene_scopes") or [], "composite_key")
    if scope_key and scope_key.startswith("bene:"):
        rest = scope_key[5:]
        bene_row = bene_lookup.get(rest)
        lot_row = lot_lookup.get(rest.split("/", 1)[0])
        return {
            "scope_key": scope_key,
            "scope_row": bene_row,
            "lot_row": lot_row,
            "scope_start_page": bene_row.get("start_page") if isinstance(bene_row, dict) else None,
            "scope_end_page": bene_row.get("end_page") if isinstance(bene_row, dict) else None,
            "scope_status": "explicit",
        }
    if scope_key and scope_key.startswith("lot:"):
        lot_id = scope_key[4:]
        lot_row = lot_lookup.get(lot_id)
        return {
            "scope_key": scope_key,
            "scope_row": lot_row,
            "lot_row": lot_row,
            "scope_start_page": lot_row.get("start_page") if isinstance(lot_row, dict) else None,
            "scope_end_page": lot_row.get("end_page") if isinstance(lot_row, dict) else None,
            "scope_status": "explicit",
        }
    inferred = _infer_scope_from_pages(pages, lot_scope_map, bene_scope_map)
    inferred_scope_key = inferred.get("scope_key")
    inferred_scope_row = inferred.get("scope_row")
    inferred_lot_row = inferred.get("lot_row")
    if isinstance(inferred_scope_key, str) and inferred_scope_key.startswith("bene:"):
        lot_id = inferred_scope_key[5:].split("/", 1)[0]
        inferred_lot_row = lot_lookup.get(lot_id)
    return {
        "scope_key": inferred_scope_key,
        "scope_row": inferred_scope_row,
        "lot_row": inferred_lot_row,
        "scope_start_page": inferred_scope_row.get("start_page") if isinstance(inferred_scope_row, dict) else None,
        "scope_end_page": inferred_scope_row.get("end_page") if isinstance(inferred_scope_row, dict) else None,
        "scope_status": inferred.get("scope_status"),
    }


def _page_text(raw_pages_by_num: Dict[int, Dict], page: int) -> str:
    return str(raw_pages_by_num.get(page, {}).get("text", "") or "")


def _page_labels(
    raw_pages_by_num: Dict[int, Dict],
    page: int,
    *,
    target_lot_id: Optional[str],
    target_bene_id: Optional[str],
) -> List[str]:
    text = _page_text(raw_pages_by_num, page)
    labels: List[str] = []
    folded = text.casefold()
    if (
        "schema riassuntivo" in folded
        or "riepilogo bando d'asta" in folded
        or "prezzo base d'asta" in folded
    ):
        labels.append("summary_or_index")
    if page <= 4:
        labels.append("summary_or_index")
    lot_matches = re.findall(r"\bLOTTO\s+(\d+)\b", text, flags=re.IGNORECASE)
    other_lots = {
        match for match in lot_matches
        if target_lot_id not in {None, ""} and match != str(target_lot_id)
    }
    if other_lots:
        labels.append("transition_page")
    bene_matches = re.findall(r"\bBene\s*N[°o.]?\s*(\d+)\b", text, flags=re.IGNORECASE)
    other_beni = {
        match for match in bene_matches
        if target_bene_id not in {None, ""} and match != str(target_bene_id)
    }
    if other_beni:
        labels.append("transition_page")
    return labels


def _page_mentions_target(text: str, *, target_lot_id: Optional[str], target_bene_id: Optional[str]) -> bool:
    if target_lot_id not in {None, ""}:
        if re.search(rf"\bLOTTO\s+{re.escape(str(target_lot_id))}\b", text, flags=re.IGNORECASE):
            return True
    if target_bene_id not in {None, ""}:
        if re.search(rf"\bBene\s*N[°o.]?\s*{re.escape(str(target_bene_id))}\b", text, flags=re.IGNORECASE):
            return True
    return False


def _target_section_entry_pages(
    raw_pages_by_num: Dict[int, Dict],
    *,
    scope_context: Dict[str, Any],
    target_scope_key: Optional[str],
) -> List[Dict[str, Any]]:
    pages: List[Tuple[int, str]] = []
    lot_row = scope_context.get("lot_row") if isinstance(scope_context, dict) else None
    scope_row = scope_context.get("scope_row") if isinstance(scope_context, dict) else None
    if isinstance(lot_row, dict) and isinstance(lot_row.get("first_header_page"), int):
        pages.append((int(lot_row["first_header_page"]), "lot_header_entry"))
    if (
        target_scope_key
        and target_scope_key.startswith("bene:")
        and isinstance(scope_row, dict)
        and isinstance(scope_row.get("first_header_page"), int)
    ):
        pages.append((int(scope_row["first_header_page"]), "bene_header_entry"))
    seen: Set[int] = set()
    selected: List[Dict[str, Any]] = []
    for page, role in pages:
        if page in seen:
            continue
        seen.add(page)
        selected.append(
            _bounded_page_text(raw_pages_by_num, page) | {"role": role}
        )
    return selected


def _nearest_anchor_pages(
    raw_pages_by_num: Dict[int, Dict],
    pages: Sequence[int],
    *,
    target_section_entry_pages: Sequence[Dict[str, Any]],
    target_lot_id: Optional[str],
    target_bene_id: Optional[str],
    limit: int = 3,
) -> List[Dict]:
    selected: List[int] = [
        int(page["page"])
        for page in target_section_entry_pages
        if isinstance(page, dict) and isinstance(page.get("page"), int)
    ]
    anchor_re = re.compile(r"\b(?:LOTTO|Bene\s*N[°o.]?|CORPO)\b", re.IGNORECASE)
    for page in sorted(set(pages)):
        text = _page_text(raw_pages_by_num, page)
        if not anchor_re.search(text):
            continue
        if "transition_page" in _page_labels(
            raw_pages_by_num,
            page,
            target_lot_id=target_lot_id,
            target_bene_id=target_bene_id,
        ):
            continue
        if page not in selected:
            selected.append(page)
        if len(selected) >= limit:
            break
    return [_bounded_page_text(raw_pages_by_num, page) for page in selected[:limit]]


def _recap_pages(
    raw_pages_by_num: Dict[int, Dict],
    pages: Sequence[int],
    *,
    scope_start_page: Optional[int],
    scope_end_page: Optional[int],
    target_lot_id: Optional[str],
    target_bene_id: Optional[str],
    limit: int = 3,
) -> List[Dict]:
    recap_re = re.compile(
        r"\b(?:schema\s+riassuntivo|riepilogo|conclusioni|prezzo\s+base|valore\s+di\s+stima)\b",
        re.IGNORECASE,
    )
    selected: List[int] = []
    for page in sorted(raw_pages_by_num):
        if isinstance(scope_start_page, int) and page < scope_start_page:
            continue
        if isinstance(scope_end_page, int) and page > scope_end_page:
            continue
        text = _page_text(raw_pages_by_num, page)
        labels = _page_labels(
            raw_pages_by_num,
            page,
            target_lot_id=target_lot_id,
            target_bene_id=target_bene_id,
        )
        if recap_re.search(text):
            if "transition_page" in labels and not _page_mentions_target(
                text,
                target_lot_id=target_lot_id,
                target_bene_id=target_bene_id,
            ):
                continue
            selected.append(page)
        if len(selected) >= limit:
            break
    return [_bounded_page_text(raw_pages_by_num, page) for page in selected]


def _table_zone_types(table_zone_map: Dict, page: int, line_index: Optional[int]) -> List[str]:
    zones = []
    for zone in table_zone_map.get("table_zones") or []:
        if zone.get("page") != page:
            continue
        start = zone.get("start_line_index")
        end = zone.get("end_line_index")
        if isinstance(line_index, int) and isinstance(start, int) and isinstance(end, int):
            if not (start <= line_index <= end):
                continue
        zone_type = zone.get("zone_type")
        if zone_type and zone_type not in zones:
            zones.append(zone_type)
    return zones


def _scope_metadata(row: Dict, table_zone_types: List[str], scope_context: Dict[str, Any]) -> Dict:
    candidates = row.get("candidates") or []
    target_scope_key = scope_context.get("scope_key") if isinstance(scope_context, dict) else None
    return {
        "lot_id": row.get("lot_id"),
        "bene_id": row.get("bene_id"),
        "scope_key": target_scope_key,
        "scope_kind": _scope_kind(target_scope_key),
        "scope_status": scope_context.get("scope_status"),
        "attribution_bucket": row.get("attribution_bucket"),
        "scope_start_page": scope_context.get("scope_start_page"),
        "scope_end_page": scope_context.get("scope_end_page"),
        "table_zone_types": table_zone_types,
        "candidate_scopes": [
            _compact_dict(
                candidate,
                ["candidate_id", "lot_id", "bene_id", "corpo_id", "composite_key", "attribution", "scope_basis"],
            )
            for candidate in candidates
        ],
    }


def _target_scope_ids(target_scope_key: Optional[str], row: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    if isinstance(target_scope_key, str):
        if target_scope_key.startswith("bene:"):
            rest = target_scope_key[5:]
            if "/" in rest:
                lot_id, bene_id = rest.split("/", 1)
                return lot_id, bene_id
        if target_scope_key.startswith("lot:"):
            return target_scope_key[4:], None
    lot_id = row.get("lot_id")
    bene_id = row.get("bene_id")
    return (
        str(lot_id) if lot_id not in {None, ""} else None,
        str(bene_id) if bene_id not in {None, ""} else None,
    )


def _page_within_scope(page: int, scope_context: Dict[str, Any]) -> bool:
    start_page = scope_context.get("scope_start_page")
    end_page = scope_context.get("scope_end_page")
    if not isinstance(start_page, int) or not isinstance(end_page, int):
        return False
    return start_page <= page <= end_page


def _has_valid_anchor_chain(
    pages: Sequence[int],
    target_section_entry_pages: Sequence[Dict[str, Any]],
    scope_context: Dict[str, Any],
) -> bool:
    if target_section_entry_pages:
        return True
    if not pages:
        return False
    return all(_page_within_scope(page, scope_context) for page in pages)


def _admissibility_signals(
    *,
    issue_type: str,
    field_family: str,
    field_type: str,
    pages: Sequence[int],
    target_scope: Dict[str, Any],
    scope_context: Dict[str, Any],
    target_section_entry_pages: Sequence[Dict[str, Any]],
    page_flags: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    target_scope_key = target_scope.get("scope_key")
    target_scope_kind = _scope_kind(target_scope_key)
    has_target_section_entry_page = bool(target_section_entry_pages)
    has_valid_anchor_chain = _has_valid_anchor_chain(pages, target_section_entry_pages, scope_context)
    uses_summary_or_index_page = any("summary_or_index" in item.get("labels", []) for item in page_flags)
    uses_transition_page = any("transition_page" in item.get("labels", []) for item in page_flags)
    summary_primary_only = bool(pages) and all("summary_or_index" in item.get("labels", []) for item in page_flags)
    transition_primary_only = bool(pages) and all("transition_page" in item.get("labels", []) for item in page_flags)
    out_of_scope_primary_pages = [
        int(item.get("page"))
        for item in page_flags
        if isinstance(item.get("page"), int) and not _page_within_scope(int(item.get("page")), scope_context)
    ]
    cross_scope_contamination_detected = any(
        "transition_page" in item.get("labels", []) or not _page_within_scope(int(item.get("page")), scope_context)
        for item in page_flags
        if isinstance(item.get("page"), int)
    )

    reason_codes: List[str] = []
    contamination_reason_codes: List[str] = []
    if target_scope_kind not in {"lot", "bene"}:
        status = target_scope.get("scope_status")
        if status == "ambiguous_from_page_containment":
            reason_codes.append("AMBIGUOUS_TARGET_SCOPE")
        else:
            reason_codes.append("MISSING_TARGET_SCOPE")
    if target_scope_kind in {"lot", "bene"} and not (has_target_section_entry_page or has_valid_anchor_chain):
        reason_codes.append("NO_TARGET_SECTION_ENTRY_OR_ANCHOR_CHAIN")
    has_strong_scope_proof = has_target_section_entry_page or has_valid_anchor_chain
    if summary_primary_only and not has_strong_scope_proof:
        reason_codes.append("SUMMARY_INDEX_PRIMARY_ONLY")
    if transition_primary_only and not has_strong_scope_proof:
        reason_codes.append("TRANSITION_PRIMARY_ONLY")
    if cross_scope_contamination_detected and transition_primary_only and not has_strong_scope_proof:
        reason_codes.append("CROSS_SCOPE_PRIMARY_CONTAMINATION")
    if issue_type == "GROUPED_CONTEXT_NEEDS_EXPLANATION" and target_scope_kind == "unknown":
        reason_codes.append("GROUPED_TRACE_INCOMPLETE")
    if issue_type == "GROUPED_CONTEXT_NEEDS_EXPLANATION" and field_type == field_family:
        reason_codes.append("GROUPED_TRACE_INCOMPLETE")
    if not pages:
        reason_codes.append("INCOMPLETE_TRACE")

    contamination_class = "clean"
    contamination_disposition = "none"
    if cross_scope_contamination_detected:
        contamination_class = "cross_scope_contamination"
        if uses_transition_page:
            contamination_reason_codes.append("CONTAMINATION_WITH_TRANSITION_PAGE")
        if uses_summary_or_index_page:
            contamination_reason_codes.append("CONTAMINATION_WITH_SUMMARY_INDEX_PAGE")
        if out_of_scope_primary_pages:
            contamination_reason_codes.append("PRIMARY_PAGES_OUTSIDE_TARGET_SCOPE")
        if has_strong_scope_proof and not (
            uses_transition_page or uses_summary_or_index_page or out_of_scope_primary_pages
        ):
            contamination_reason_codes.append("CONTAMINATION_TOLERATED_BY_STRONG_ANCHOR_CHAIN")

    hard_contamination = bool(
        cross_scope_contamination_detected
        and (
            uses_transition_page
            or uses_summary_or_index_page
            or out_of_scope_primary_pages
        )
    )
    tolerated_contamination = bool(cross_scope_contamination_detected and not hard_contamination)

    reason_codes.extend(code for code in contamination_reason_codes if code not in reason_codes)

    if hard_contamination:
        admissibility_status = "upstream_blocked_packet"
        contamination_disposition = "blocked"
    elif tolerated_contamination:
        admissibility_status = "admissible_tainted"
        contamination_disposition = "tolerated"
    elif not reason_codes:
        admissibility_status = "admissible_clean"
    else:
        admissibility_status = "upstream_blocked_packet"

    quality_label = {
        "admissible_clean": "admissible_clean",
        "admissible_tainted": "admissible_tainted",
        "upstream_blocked_packet": "blocked_packet",
    }[admissibility_status]
    reason_for_label = (
        "packet satisfies scope, anchor, and primary-evidence integrity rules"
        if admissibility_status == "admissible_clean"
        else ", ".join(reason_codes)
    )
    llm_safe = admissibility_status == "admissible_clean"
    return {
        "scope_status": target_scope.get("scope_status"),
        "target_scope_kind": target_scope_kind,
        "has_target_section_entry_page": has_target_section_entry_page,
        "has_valid_anchor_chain": has_valid_anchor_chain,
        "uses_summary_or_index_page": uses_summary_or_index_page,
        "uses_transition_page": uses_transition_page,
        "summary_or_index_primary_only": summary_primary_only,
        "transition_primary_only": transition_primary_only,
        "out_of_scope_primary_pages": out_of_scope_primary_pages,
        "cross_scope_contamination_detected": cross_scope_contamination_detected,
        "contamination_class": contamination_class,
        "contamination_disposition": contamination_disposition,
        "admissibility_status": admissibility_status,
        "admissibility_reason_codes": reason_codes,
        "quality_label": quality_label,
        "reason_for_label": reason_for_label,
        "llm_safe": llm_safe,
    }


def _build_issue(
    *,
    case_key: str,
    artifact_name: str,
    ordinal: int,
    row: Dict,
    raw_pages_by_num: Dict[int, Dict],
    table_zone_map: Dict,
    lot_scope_map: Dict[str, Any],
    bene_scope_map: Dict[str, Any],
) -> Dict:
    field_family = _field_family_from_artifact(artifact_name)
    field_type = row.get("field_type") or field_family
    block_type = row.get("type") or "UNRESOLVED_DETERMINISTIC_SURFACE"
    issue_type = _issue_type(str(block_type), row)
    pages = _collect_pages(row)
    if not pages:
        pages = sorted(raw_pages_by_num)[:1]
    needles = _text_needles(row)
    local_windows = [
        _window_for_page(raw_pages_by_num, page, row.get("line_index"), needles)
        for page in pages
    ]
    reason_codes = _reason_codes(row)
    candidate_values = _candidate_values(row)
    blocked_values = _blocked_values(row)
    zone_types: List[str] = []
    for page in pages:
        for zone_type in _table_zone_types(table_zone_map, page, row.get("line_index")):
            if zone_type not in zone_types:
                zone_types.append(zone_type)

    scope_context = _issue_scope_context(row, pages, lot_scope_map, bene_scope_map)
    target_scope = _scope_metadata(row, zone_types, scope_context)
    target_lot_id, target_bene_id = _target_scope_ids(target_scope.get("scope_key"), row)
    target_section_entry_pages = _target_section_entry_pages(
        raw_pages_by_num,
        scope_context=scope_context,
        target_scope_key=target_scope.get("scope_key"),
    )
    anchor_pages = _nearest_anchor_pages(
        raw_pages_by_num,
        pages,
        target_section_entry_pages=target_section_entry_pages,
        target_lot_id=target_lot_id,
        target_bene_id=target_bene_id,
    )
    recap_pages = _recap_pages(
        raw_pages_by_num,
        pages,
        scope_start_page=target_scope.get("scope_start_page"),
        scope_end_page=target_scope.get("scope_end_page"),
        target_lot_id=target_lot_id,
        target_bene_id=target_bene_id,
    )
    page_flags = [
        {
            "page": page,
            "labels": _page_labels(
                raw_pages_by_num,
                page,
                target_lot_id=target_lot_id,
                target_bene_id=target_bene_id,
            ),
        }
        for page in pages
    ]
    admissibility = _admissibility_signals(
        issue_type=issue_type,
        field_family=field_family,
        field_type=field_type,
        pages=pages,
        target_scope=target_scope,
        scope_context=scope_context,
        target_section_entry_pages=target_section_entry_pages,
        page_flags=page_flags,
    )

    supporting_candidates = [_candidate_projection(c) for c in row.get("candidates") or []]
    issue_id = f"{case_key}::{field_family}::{issue_type.lower()}::{ordinal:04d}"
    return {
        "issue_id": issue_id,
        "case_key": case_key,
        "field_family": field_family,
        "field_type": field_type,
        "lot_id": row.get("lot_id"),
        "bene_id": row.get("bene_id"),
        "issue_type": issue_type,
        "deterministic_status": "UNRESOLVED",
        "reason_codes": reason_codes,
        "candidate_values": candidate_values,
        "blocked_values": blocked_values,
        "supporting_candidates": supporting_candidates,
        "supporting_blocked_entries": [_blocked_projection(row)],
        "source_pages": pages,
        "target_case_key": case_key,
        "target_field": field_type,
        "target_scope": target_scope,
        "known_candidates": supporting_candidates,
        "blocked_reasons": [
            {"reason_code": code, "text": str(row.get("reason") or "")}
            for code in reason_codes
        ],
        "relevant_pages": [_bounded_page_text(raw_pages_by_num, page) for page in pages],
        "target_section_entry_pages": target_section_entry_pages,
        "anchor_pages": anchor_pages,
        "recap_pages": recap_pages,
        "page_selection": {
            "has_target_section_entry_page": admissibility["has_target_section_entry_page"],
            "has_valid_anchor_chain": admissibility["has_valid_anchor_chain"],
            "uses_transition_page": admissibility["uses_transition_page"],
            "uses_summary_or_index_page": admissibility["uses_summary_or_index_page"],
            "summary_or_index_primary_only": admissibility["summary_or_index_primary_only"],
            "transition_primary_only": admissibility["transition_primary_only"],
            "out_of_scope_primary_pages": admissibility["out_of_scope_primary_pages"],
            "cross_scope_contamination_detected": admissibility["cross_scope_contamination_detected"],
            "llm_safe": admissibility["llm_safe"],
            "page_flags": page_flags,
        },
        "contamination_class": admissibility["contamination_class"],
        "contamination_disposition": admissibility["contamination_disposition"],
        "scope_status": admissibility["scope_status"],
        "target_scope_kind": admissibility["target_scope_kind"],
        "has_target_section_entry_page": admissibility["has_target_section_entry_page"],
        "has_valid_anchor_chain": admissibility["has_valid_anchor_chain"],
        "uses_summary_or_index_page": admissibility["uses_summary_or_index_page"],
        "uses_transition_page": admissibility["uses_transition_page"],
        "cross_scope_contamination_detected": admissibility["cross_scope_contamination_detected"],
        "admissibility_status": admissibility["admissibility_status"],
        "admissibility_reason_codes": admissibility["admissibility_reason_codes"],
        "quality_label": admissibility["quality_label"],
        "reason_for_label": admissibility["reason_for_label"],
        "supporting_evidence_snippets": _shell_quotes(row),
        "current_ambiguity_summary": {
            "issue_type": issue_type,
            "deterministic_status": "UNRESOLVED",
            "why_deterministic_promotion_failed": str(row.get("reason") or block_type),
            "candidate_values": candidate_values,
            "blocked_values": blocked_values,
            "reason_codes": reason_codes,
        },
        "source_line_indices": _source_line_indices(row),
        "shell_quotes": _shell_quotes(row),
        "local_text_windows": local_windows,
        "table_zone_types": zone_types,
        "scope_metadata": target_scope,
        "needs_llm": _needs_llm(issue_type),
        "shell_sources": [artifact_name, "table_zone_map.json", "raw_pages.json"],
    }


def build_clarification_issue_pack(case_key: str) -> Dict[str, object]:
    ctx = build_context(case_key)
    raw_pages = _read_json(ctx.artifact_dir / "raw_pages.json", [])
    raw_pages_by_num = {int(p["page_number"]): p for p in raw_pages if "page_number" in p}
    table_zone_map = _read_json(ctx.artifact_dir / "table_zone_map.json", {})
    lot_scope_map = _read_json(ctx.artifact_dir / "lot_scope_map.json", {})
    bene_scope_map = _read_json(ctx.artifact_dir / "bene_scope_map.json", {})

    issues: List[Dict] = []
    tainted_packets: List[Dict] = []
    blocked_packets: List[Dict] = []
    warnings: List[str] = []
    source_paths = {}
    ordinal = 1
    for artifact_name in PACK_FAMILIES:
        path = ctx.artifact_dir / artifact_name
        source_paths[artifact_name] = str(path)
        data = _read_json(path, {})
        if not data:
            warnings.append(f"Missing or empty source artifact: {artifact_name}")
            continue
        for row in data.get("blocked_or_ambiguous") or []:
            issue = _build_issue(
                case_key=case_key,
                artifact_name=artifact_name,
                ordinal=ordinal,
                row=row,
                raw_pages_by_num=raw_pages_by_num,
                table_zone_map=table_zone_map,
                lot_scope_map=lot_scope_map,
                bene_scope_map=bene_scope_map,
            )
            if not issue["needs_llm"]:
                continue
            if issue.get("admissibility_status") == "admissible_clean":
                issues.append(issue)
            elif issue.get("admissibility_status") == "admissible_tainted":
                tainted_packets.append(issue)
            else:
                blocked_packets.append(issue)
            ordinal += 1

    out = {
        "case_key": case_key,
        "status": "OK",
        "issue_count": len(issues),
        "tainted_packet_count": len(tainted_packets),
        "blocked_packet_count": len(blocked_packets),
        "issues": issues,
        "tainted_packets": tainted_packets,
        "blocked_packets": blocked_packets,
        "resolutions": [],
        "warnings": warnings,
        "source_artifacts": source_paths | {
            "table_zone_map.json": str(ctx.artifact_dir / "table_zone_map.json"),
            "lot_scope_map.json": str(ctx.artifact_dir / "lot_scope_map.json"),
            "bene_scope_map.json": str(ctx.artifact_dir / "bene_scope_map.json"),
            "raw_pages.json": str(ctx.artifact_dir / "raw_pages.json"),
        },
    }
    dst = ctx.artifact_dir / "clarification_issue_pack.json"
    dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def select_issues(
    pack: Dict[str, object],
    *,
    issue_type: Optional[str] = None,
    field_family: Optional[str] = None,
    field_type: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Dict]:
    result = []
    for issue in pack.get("issues", []):
        if issue_type and issue.get("issue_type") != issue_type:
            continue
        if field_family and issue.get("field_family") != field_family:
            continue
        if field_type and issue.get("field_type") != field_type:
            continue
        result.append(issue)
        if limit and len(result) >= limit:
            break
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Build canonical LLM clarification issue packet")
    parser.add_argument("--case", required=True, choices=list_case_keys())
    args = parser.parse_args()
    out = build_clarification_issue_pack(args.case)
    print(json.dumps({"case_key": out["case_key"], "status": out["status"], "issue_count": out["issue_count"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
