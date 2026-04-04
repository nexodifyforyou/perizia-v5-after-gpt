from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from perizia_runtime.state import Candidate
from perizia_tools.evidence_span_tool import make_evidence

RUNS_ROOT = Path("/srv/perizia/_qa/runs")
FIXTURES_ROOT = Path(__file__).resolve().parents[1] / "perizia_qa" / "fixtures"


def _read_json(path: Path) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return None


def valuation_candidates(analysis_id: str) -> List[Candidate]:
    rows = _read_json(RUNS_ROOT / analysis_id / "candidates" / "candidates_money.json")
    if not isinstance(rows, list):
        rows = _read_json(FIXTURES_ROOT / analysis_id / "candidates_money.json")
    if not isinstance(rows, list):
        return []
    out: List[Candidate] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        amount = row.get("amount_eur")
        if not isinstance(amount, (int, float)):
            continue
        quote = str(row.get("quote") or "")
        page = int(row.get("page") or 0)
        low = quote.lower()
        semantic_role = "money"
        valid_fields = ["pricing"]
        section_type = "valuation"
        confidence = 0.4
        if "subalterno" in low and amount < 10:
            semantic_role = "subalterno_id"
            valid_fields = []
            confidence = 0.0
        elif "prezzo a base d'asta" in low or "prezzo a base asta" in low:
            semantic_role = "auction_price"
            confidence = 0.98
        elif "valore complessivo" in low:
            semantic_role = "valuation_total"
            confidence = 0.9
        elif "riduzione cautelativa" in low:
            semantic_role = "valuation_adjustment"
            valid_fields = ["costs", "pricing"]
            section_type = "costs"
            confidence = 0.92
        elif "spese tecniche" in low or "spese condominiali" in low or "totale" in low or "costi di regolarizzazione" in low:
            semantic_role = "buyer_cost"
            valid_fields = ["costs"]
            section_type = "costs"
            confidence = 0.9
        elif "valore al netto" in low:
            semantic_role = "net_valuation"
            confidence = 0.88
        elif "valore unitario" in low:
            semantic_role = "unit_price"
            confidence = 0.85
        if semantic_role == "subalterno_id":
            out.append(
                Candidate(
                    value=amount,
                    field_key="pricing",
                    confidence=0.0,
                    evidence=[make_evidence(page, quote, semantic_role, [], 0.0, source="candidates_money")],
                    section_type="valuation",
                    semantic_role=semantic_role,
                    valid=False,
                    invalid_reason="subalterno_number_contamination",
                    source="candidates_money",
                    metadata={"amount": amount},
                )
            )
            continue
        out.append(
            Candidate(
                value=float(amount),
                field_key="pricing" if "cost" not in semantic_role else "costs",
                confidence=confidence,
                evidence=[make_evidence(page, quote, semantic_role, valid_fields, confidence, source="candidates_money")],
                section_type=section_type,
                semantic_role=semantic_role,
                valid=bool(valid_fields),
                invalid_reason=None if valid_fields else "invalid_role",
                source="candidates_money",
                metadata={"amount": float(amount)},
            )
        )
    return out


def cost_line_candidates(pages: List[Dict[str, Any]]) -> List[Candidate]:
    out: List[Candidate] = []
    for idx, page in enumerate(pages or [], start=1):
        text = str((page or {}).get("text") or "")
        page_number = int((page or {}).get("page_number") or (page or {}).get("page") or idx)
        if "spese" not in text.lower() and "costi" not in text.lower():
            continue
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        for i, line in enumerate(lines):
            low = line.lower()
            if "spese tecniche" in low or "spese condominiali" in low or low == "totale":
                window = " ".join(lines[i:i + 3])[:320]
                out.append(
                    Candidate(
                        value=window,
                        field_key="costs",
                        confidence=0.8,
                        evidence=[make_evidence(page_number, window, "cost_line", ["costs"], 0.8, source="pages")],
                        section_type="costs",
                        semantic_role="cost_line",
                        source="pages",
                    )
                )
    return out
