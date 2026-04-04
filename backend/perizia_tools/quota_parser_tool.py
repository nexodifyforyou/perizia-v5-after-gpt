from __future__ import annotations

import re
from typing import Any, Dict, List

from perizia_runtime.state import Candidate
from perizia_tools.evidence_span_tool import make_evidence
from perizia_tools.numeric_parser_tool import parse_fraction

_DATEISH_RE = re.compile(r"\b[0-3]?\d\s*/\s*[01]?\d(?:\s*/\s*\d{2,4})?\b")


def quota_candidates(pages: List[Dict[str, Any]], result: Dict[str, Any]) -> List[Candidate]:
    out: List[Candidate] = []
    seen = set()
    for idx, page in enumerate(pages or [], start=1):
        text = str((page or {}).get("text") or "")
        page_number = int((page or {}).get("page_number") or (page or {}).get("page") or idx)
        for match in re.finditer(r"\b\d{1,3}\s*/\s*\d{1,3}\b", text):
            raw = match.group(0)
            if _DATEISH_RE.fullmatch(raw):
                continue
            start = max(0, match.start() - 80)
            end = min(len(text), match.end() + 80)
            quote = text[start:end].strip()
            low = quote.lower()
            if not any(token in low for token in ("quota", "propriet", "usufrutto", "nuda propriet", "piena")):
                continue
            fraction = parse_fraction(raw)
            if not fraction or fraction in seen:
                continue
            seen.add(fraction)
            out.append(
                Candidate(
                    value=fraction,
                    field_key="quota",
                    confidence=0.85,
                    evidence=[make_evidence(page_number, quote, "rights_fraction", ["quota", "diritto_reale"], 0.85)],
                    section_type="rights",
                    semantic_role="rights_fraction",
                    source="pages",
                )
            )
    lots = result.get("lots") if isinstance(result.get("lots"), list) else []
    for lot in lots[:1]:
        if not isinstance(lot, dict):
            continue
        lot_quota = parse_fraction(str(lot.get("quota") or "")) or parse_fraction(str(lot.get("diritto_reale") or ""))
        if lot_quota and lot_quota not in seen:
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
                )
            )
    return out

