from __future__ import annotations

import re
from typing import Any, Dict, List

from perizia_runtime.state import Candidate
from perizia_tools.evidence_span_tool import make_evidence
from perizia_tools.numeric_parser_tool import parse_fraction

_DATEISH_RE = re.compile(r"\b[0-3]?\d\s*/\s*[01]?\d(?:\s*/\s*\d{2,4})?\b")
_FRACTION_RE = re.compile(r"\b\d{1,3}\s*/\s*\d{1,3}\b")
_DATE_TAIL_RE = re.compile(r"^\s*/\s*\d{2,4}\b")
_DATE_PREFIX_RE = re.compile(r"(?:\bil\b|\bdal\b|\bal\b|\bdata\b|\bdel\b)\s*$", re.IGNORECASE)
_PRIMARY_RIGHTS_PATTERNS = (
    "diritti di piena proprietà per la quota",
    "diritto reale",
    "proprietà quota",
    "diritto di proprietà",
    "tipologia del diritto",
    "tutti i beni del lotto",
    "intero lotto",
)
_SUBORDINATE_RIGHTS_PATTERNS = (
    "seguenti esecutati",
    "seguenti diritti",
    "compropriet",
    "stradella",
    "servitù",
    "formalità",
    "iscritto",
    "trascritto",
    "reg. part.",
    "reg. gen.",
    "attribuire al bene costituente",
    "residua quota",
)


def _is_date_like_fraction(raw: str, text: str, start: int, end: int) -> bool:
    if _DATEISH_RE.fullmatch(raw) and _DATE_TAIL_RE.match(text[end:]):
        return True
    num, den = [int(part.strip()) for part in raw.split("/", 1)]
    if num > 31 or den > 12:
        return False
    prefix = text[max(0, start - 12):start]
    return bool(_DATE_PREFIX_RE.search(prefix))


def _quota_priority(quote: str, local_context: str) -> int:
    low = quote.lower()
    local_low = local_context.lower()
    if any(pattern in local_low for pattern in _PRIMARY_RIGHTS_PATTERNS):
        return 3
    if any(pattern in low for pattern in _SUBORDINATE_RIGHTS_PATTERNS):
        return 1
    return 2


def quota_candidates(pages: List[Dict[str, Any]], result: Dict[str, Any]) -> List[Candidate]:
    out: List[Candidate] = []
    seen_occurrences = set()
    seen_legacy_values = set()
    for idx, page in enumerate(pages or [], start=1):
        text = str((page or {}).get("text") or "")
        page_number = int((page or {}).get("page_number") or (page or {}).get("page") or idx)
        for match in _FRACTION_RE.finditer(text):
            raw = match.group(0)
            start = max(0, match.start() - 80)
            end = min(len(text), match.end() + 80)
            quote = text[start:end].strip()
            local_start = max(0, match.start() - 40)
            local_end = min(len(text), match.end() + 40)
            local_context = text[local_start:local_end].strip()
            low = quote.lower()
            has_rights_context = any(token in low for token in ("quota", "propriet", "usufrutto", "nuda propriet", "piena"))
            if not has_rights_context:
                continue
            if _is_date_like_fraction(raw, text, match.start(), match.end()):
                continue
            fraction = parse_fraction(raw)
            occurrence_key = (fraction, page_number, match.start())
            if not fraction or occurrence_key in seen_occurrences:
                continue
            seen_occurrences.add(occurrence_key)
            out.append(
                Candidate(
                    value=fraction,
                    field_key="quota",
                    confidence=0.85,
                    evidence=[make_evidence(page_number, quote, "rights_fraction", ["quota", "diritto_reale"], 0.85)],
                    section_type="rights",
                    semantic_role="rights_fraction",
                    source="pages",
                    metadata={
                        "match_start": match.start(),
                        "match_end": match.end(),
                        "priority": _quota_priority(quote, local_context),
                    },
                )
            )
    lots = result.get("lots") if isinstance(result.get("lots"), list) else []
    for lot in lots[:1]:
        if not isinstance(lot, dict):
            continue
        lot_quota = parse_fraction(str(lot.get("quota") or "")) or parse_fraction(str(lot.get("diritto_reale") or ""))
        if lot_quota and lot_quota not in seen_legacy_values:
            seen_legacy_values.add(lot_quota)
            quote = str(lot.get("diritto_reale") or lot.get("quota") or "").strip()
            ev = make_evidence(0, quote or lot_quota, "rights_fraction", ["quota", "diritto_reale"], 0.75, source="legacy_result")
            out.append(
                Candidate(
                    value=lot_quota,
                    field_key="quota",
                    confidence=0.75,
                    evidence=[ev],
                    section_type="rights",
                    semantic_role="rights_fraction",
                    source="legacy_result",
                    metadata={"priority": 2},
                )
            )
    return out
