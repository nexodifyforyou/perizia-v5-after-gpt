from __future__ import annotations

import re
from typing import Any, Dict, List

from perizia_runtime.state import Candidate
from perizia_tools.evidence_span_tool import make_evidence


_IDENTIFIED_BLOCK_RE = re.compile(r"Identificat\w*\s+al\s+catasto[\s\S]{0,420}", re.IGNORECASE)
_FG_TOKEN_RE = re.compile(r"(?:fg\.?|foglio)\s*(\d+)", re.IGNORECASE)
_PART_TOKEN_RE = re.compile(r"(?:part\.?|particella|mapp\.?|mappale)\s*(\d+)", re.IGNORECASE)
_SUB_TOKEN_RE = re.compile(r"(?:sub\.?|subalterno)\s*(\d+)", re.IGNORECASE)
_CATEGORY_TOKEN_RE = re.compile(r"categoria\s*([A-Z]\s*/?\s*\d+)", re.IGNORECASE)
_STOP_TOKENS = (
    "superficie",
    "stato conservativo",
    "descrizione:",
    "vendita soggetta",
    "continuità trascrizioni",
    "stato di occupazione",
    "valore di stima",
    "l'immobile viene posto",
    "firmato da:",
    "pubblicazione ufficiale",
)
_HEADING_CONTEXT_RE = re.compile(r"(lotto\s*(?:n[°º.]?\s*)?(?:\d+|unico)|bene\s*n[°º.]?\s*\d+[^\n]{0,120})", re.IGNORECASE)


def _normalize_spaces(text: str) -> str:
    return " ".join(str(text or "").replace("\n", " ").split()).strip()


def _normalize_category(value: str | None) -> str | None:
    if not value:
        return None
    normalized = re.sub(r"\s+", "", str(value).upper())
    if re.fullmatch(r"[A-Z]\d+", normalized):
        return f"{normalized[0]}/{normalized[1:]}"
    return normalized


def _quote_priority(quote: str, *, record_index: int, category: str | None, subalterno: str | None, source_kind: str) -> int:
    priority = 2
    low = quote.lower()
    if source_kind == "identified_line":
        priority += 1
    if "bene n" in low or "lotto " in low:
        priority += 2
    if record_index == 0:
        priority += 1
    if category and str(category).upper() not in {"F/1", "F1"}:
        priority += 1
    if not subalterno:
        priority -= 1
    if "cronistoria dati catastali" in low:
        priority -= 2
    if "stradella" in low or "compropriet" in low:
        priority -= 2
    return max(priority, 1)


def _candidate_metadata(
    *,
    match_start: int,
    match_end: int,
    priority: int,
    record_index: int,
    source_kind: str,
    scope_hint: str | None = None,
) -> Dict[str, Any]:
    return {
        "match_start": int(match_start),
        "match_end": int(match_end),
        "priority": int(priority),
        "record_index": int(record_index),
        "source_kind": source_kind,
        "scope_hint": scope_hint,
    }


def _heading_context(text: str, absolute_start: int) -> tuple[str, str | None]:
    window = text[max(0, absolute_start - 4000):absolute_start]
    matches = list(_HEADING_CONTEXT_RE.finditer(window))
    if not matches:
        return "", None
    heading = _normalize_spaces(matches[-1].group(0))
    low = heading.lower()
    bene_match = re.search(r"bene\s*n[°º.]?\s*(\d+)", low, re.IGNORECASE)
    if bene_match:
        return heading, f"bene:{bene_match.group(1)}"
    lotto_match = re.search(r"lotto\s*(?:n[°º.]?\s*)?(\d+|unico)", low, re.IGNORECASE)
    if lotto_match:
        token = lotto_match.group(1).lower()
        normalized = token if token == "unico" else re.sub(r"\D+", "", token)
        return heading, f"lotto:{normalized}"
    return heading, None


def _append_field_candidates(
    out: List[Candidate],
    *,
    page_number: int,
    quote: str,
    fields: Dict[str, str | None],
    match_start: int,
    match_end: int,
    priority: int,
    record_index: int,
    source_kind: str,
    scope_hint: str | None = None,
) -> None:
    for field_key, value in fields.items():
        if value is None:
            continue
        out.append(
            Candidate(
                value=str(value),
                field_key=field_key,
                confidence=0.85 if source_kind == "identified_line" else 0.8,
                evidence=[make_evidence(page_number, quote, "catasto", [field_key], 0.85 if source_kind == "identified_line" else 0.8)],
                section_type="catasto",
                semantic_role=f"catasto_{field_key}",
                source="pages",
                metadata=_candidate_metadata(
                    match_start=match_start,
                    match_end=match_end,
                    priority=priority,
                    record_index=record_index,
                    source_kind=source_kind,
                    scope_hint=scope_hint,
                ),
            )
        )


def _parse_identified_records(block: str) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    fg_matches = list(_FG_TOKEN_RE.finditer(block))
    if not fg_matches:
        return records
    for index, fg_match in enumerate(fg_matches):
        segment_end = fg_matches[index + 1].start() if index + 1 < len(fg_matches) else len(block)
        segment = block[fg_match.start():segment_end]
        stop_positions = [segment.lower().find(token) for token in _STOP_TOKENS if token in segment.lower()]
        valid_stops = [pos for pos in stop_positions if pos >= 0]
        if valid_stops:
            segment = segment[: min(valid_stops)]
        particella_match = _PART_TOKEN_RE.search(segment)
        if not particella_match:
            continue
        sub_match = _SUB_TOKEN_RE.search(segment)
        category_match = _CATEGORY_TOKEN_RE.search(segment)
        records.append(
            {
                "fields": {
                    "foglio": fg_match.group(1),
                    "particella": particella_match.group(1),
                    "subalterno": sub_match.group(1) if sub_match else None,
                    "categoria": _normalize_category(category_match.group(1)) if category_match else None,
                },
                "segment": _normalize_spaces(segment),
                "match_start": fg_match.start(),
                "match_end": fg_match.start() + len(segment),
                "record_index": index,
            }
        )
    return records


def catasto_candidates(pages: List[Dict[str, Any]]) -> List[Candidate]:
    out: List[Candidate] = []
    for idx, page in enumerate(pages or [], start=1):
        text = str((page or {}).get("text") or "")
        page_number = int((page or {}).get("page_number") or (page or {}).get("page") or idx)

        for block_match in _IDENTIFIED_BLOCK_RE.finditer(text):
            for record in _parse_identified_records(block_match.group(0)):
                fields = record["fields"]
                absolute_start = block_match.start() + int(record["match_start"])
                absolute_end = block_match.start() + int(record["match_end"])
                heading, scope_hint = _heading_context(text, absolute_start)
                quote = _normalize_spaces(f"{heading} {record.get('segment') or ''}")
                priority = _quote_priority(
                    quote,
                    record_index=int(record["record_index"]),
                    category=fields.get("categoria"),
                    subalterno=fields.get("subalterno"),
                    source_kind="identified_line",
                )
                _append_field_candidates(
                    out,
                    page_number=page_number,
                    quote=quote,
                    fields=fields,
                    match_start=absolute_start,
                    match_end=absolute_end,
                    priority=priority,
                    record_index=int(record["record_index"]),
                    source_kind="identified_line",
                    scope_hint=scope_hint,
                )

    return out
