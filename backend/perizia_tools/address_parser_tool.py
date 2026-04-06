from __future__ import annotations

from typing import Any, Dict, List

from perizia_runtime.state import Candidate
from perizia_tools.evidence_span_tool import make_evidence


def address_candidates(result: Dict[str, Any]) -> List[Candidate]:
    out: List[Candidate] = []
    report_header = result.get("report_header", {}) if isinstance(result.get("report_header"), dict) else {}
    address = report_header.get("address", {}) if isinstance(report_header.get("address"), dict) else {}
    value = str(address.get("value") or "").strip()
    evidence = address.get("evidence", []) if isinstance(address.get("evidence"), list) else []
    if value:
        ev = []
        for item in evidence[:1]:
            if isinstance(item, dict):
                ev.append(make_evidence(item.get("page", 0), item.get("quote", value), "address", ["address"], 0.8, source="legacy_result"))
        if not ev:
            ev.append(make_evidence(0, value, "address", ["address"], 0.65, source="legacy_result"))
        out.append(Candidate(value=value, field_key="address", confidence=0.8, evidence=ev, section_type="identity", semantic_role="address", source="legacy_result"))
    return out

