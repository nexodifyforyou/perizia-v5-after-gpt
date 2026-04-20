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
from typing import Dict, List, Optional, Sequence, Set

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


def _nearest_anchor_pages(raw_pages_by_num: Dict[int, Dict], pages: Sequence[int], limit: int = 2) -> List[Dict]:
    if not pages:
        return []
    anchor_re = re.compile(r"\b(?:LOTTO|Bene\s*N[°o.]?|CORPO)\b", re.IGNORECASE)
    selected: List[int] = []
    min_page = min(pages)
    for page in sorted(raw_pages_by_num, reverse=True):
        if page > min_page:
            continue
        text = raw_pages_by_num.get(page, {}).get("text", "") or ""
        if anchor_re.search(text):
            selected.append(page)
        if len(selected) >= limit:
            break
    return [_bounded_page_text(raw_pages_by_num, page) for page in sorted(selected)]


def _recap_pages(raw_pages_by_num: Dict[int, Dict], pages: Sequence[int], limit: int = 3) -> List[Dict]:
    recap_re = re.compile(
        r"\b(?:schema\s+riassuntivo|riepilogo|conclusioni|prezzo\s+base|valore\s+di\s+stima)\b",
        re.IGNORECASE,
    )
    selected: List[int] = []
    page_set = set(pages)
    for page in sorted(raw_pages_by_num):
        text = raw_pages_by_num.get(page, {}).get("text", "") or ""
        if page in page_set or recap_re.search(text):
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


def _scope_metadata(row: Dict, table_zone_types: List[str]) -> Dict:
    candidates = row.get("candidates") or []
    return {
        "lot_id": row.get("lot_id"),
        "bene_id": row.get("bene_id"),
        "scope_key": row.get("scope_key"),
        "attribution_bucket": row.get("attribution_bucket"),
        "table_zone_types": table_zone_types,
        "candidate_scopes": [
            _compact_dict(
                candidate,
                ["candidate_id", "lot_id", "bene_id", "corpo_id", "composite_key", "attribution", "scope_basis"],
            )
            for candidate in candidates
        ],
    }


def _build_issue(
    *,
    case_key: str,
    artifact_name: str,
    ordinal: int,
    row: Dict,
    raw_pages_by_num: Dict[int, Dict],
    table_zone_map: Dict,
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
        "target_scope": _scope_metadata(row, zone_types),
        "known_candidates": supporting_candidates,
        "blocked_reasons": [
            {"reason_code": code, "text": str(row.get("reason") or "")}
            for code in reason_codes
        ],
        "relevant_pages": [_bounded_page_text(raw_pages_by_num, page) for page in pages],
        "anchor_pages": _nearest_anchor_pages(raw_pages_by_num, pages),
        "recap_pages": _recap_pages(raw_pages_by_num, pages),
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
        "scope_metadata": _scope_metadata(row, zone_types),
        "needs_llm": _needs_llm(issue_type),
        "shell_sources": [artifact_name, "table_zone_map.json", "raw_pages.json"],
    }


def build_clarification_issue_pack(case_key: str) -> Dict[str, object]:
    ctx = build_context(case_key)
    raw_pages = _read_json(ctx.artifact_dir / "raw_pages.json", [])
    raw_pages_by_num = {int(p["page_number"]): p for p in raw_pages if "page_number" in p}
    table_zone_map = _read_json(ctx.artifact_dir / "table_zone_map.json", {})

    issues: List[Dict] = []
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
            )
            if issue["needs_llm"]:
                issues.append(issue)
                ordinal += 1

    out = {
        "case_key": case_key,
        "status": "OK",
        "issue_count": len(issues),
        "issues": issues,
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
