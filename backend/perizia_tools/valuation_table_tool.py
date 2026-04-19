from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List

from perizia_runtime.state import Candidate
from perizia_tools.evidence_span_tool import make_evidence

RUNS_ROOT = Path("/srv/perizia/_qa/runs")
FIXTURES_ROOT = Path(__file__).resolve().parents[1] / "perizia_qa" / "fixtures"

_AUCTION_PRICE_RE = re.compile(r"prezzo\s+(?:a\s+)?base\s+d[' ]asta|prezzo\s+base\s+asta", re.IGNORECASE)
_NET_VALUATION_RE = re.compile(
    r"valore\s+finale\s+di\s+stima|valore\s+al\s+netto(?:\s+dei\s+costi\s+di\s+regolarizzazione(?:\s+e\s+della\s+riduzione\s+cautelativa)?)?|valore\s+in\s+caso\s+di\s+regolarizzazione",
    re.IGNORECASE,
)
_STIMA_VALUE_RE = re.compile(r"valore\s+di\s+stima(?:\s+del\s+bene)?|valore\s+complessivo", re.IGNORECASE)
_UNIT_PRICE_RE = re.compile(r"valore\s+unitario|€/mq", re.IGNORECASE)
_ROOT_AGGREGATE_RE = re.compile(r"valore\s+complessivo|valore\s+complessivo\s*\(vc\)|\bvc\b", re.IGNORECASE)
_STANDALONE_STIMA_RE = re.compile(r"valore\s+di\s+stima\s*:", re.IGNORECASE)
_LOT_ANCHOR_RE = re.compile(r"\blotto\s*(?:n[°º.]?\s*)?(\d+|unico)\b", re.IGNORECASE)
_BENE_ANCHOR_RE = re.compile(r"\bbene\s*n[°º.]?\s*(\d+)\b", re.IGNORECASE)


def _read_json(path: Path) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return None


def _pricing_role_context(row: Dict[str, Any]) -> str:
    quote = str(row.get("quote") or "")
    context = str(row.get("context") or "")
    low = f"{quote}\n{context}".lower()
    if _AUCTION_PRICE_RE.search(low):
        return "auction_price"
    if (
        "riduzione cautelativa" in low
        or "riduzione del valore" in low
        or "assenza di garanzia" in low
        or "rimborso forfettario" in low
        or "smaltimento di beni mobili" in low
    ):
        return "valuation_adjustment"
    if _NET_VALUATION_RE.search(low):
        return "net_valuation"
    if _STIMA_VALUE_RE.search(low):
        return "valuation_total"
    if "spese tecniche" in low or "spese condominiali" in low or "totale" in low or "costi di regolarizzazione" in low:
        return "buyer_cost"
    if _UNIT_PRICE_RE.search(low):
        return "unit_price"
    return "money"


def _is_valuation_table_ratio_noise(row: Dict[str, Any], amount: float, semantic_role: str) -> bool:
    if semantic_role != "valuation_total" or amount > 1000:
        return False
    quote = str(row.get("quote") or "")
    context = str(row.get("context") or "")
    low = f"{quote}\n{context}".lower()
    return "quota invendita" in low or "quota in vendita" in low or "%" in low or "1/1" in low


def _is_unit_price_contamination(row: Dict[str, Any], amount: float, semantic_role: str) -> bool:
    if semantic_role != "valuation_total" or amount <= 1000 or amount >= 10000:
        return False
    quote = str(row.get("quote") or "")
    context = str(row.get("context") or "")
    low = f"{quote}\n{context}".lower()
    return _UNIT_PRICE_RE.search(quote.lower()) is not None or ("valore unitario" in low and "valore complessivo" in low)


def _structural_anchor(row: Dict[str, Any]) -> str | None:
    quote = str(row.get("quote") or "")
    context = str(row.get("context") or "")
    text = f"{quote}\n{context}"
    lot_match = _LOT_ANCHOR_RE.search(text)
    if lot_match:
        return f"lotto:{lot_match.group(1).lower()}"
    bene_match = _BENE_ANCHOR_RE.search(text)
    if bene_match:
        return f"bene:{bene_match.group(1)}"
    return None


def _structural_scope(row: Dict[str, Any], semantic_role: str, *, invalid_reason: str | None = None) -> str:
    if invalid_reason in {"valuation_table_ratio_contamination", "unit_price_contamination"}:
        return "contaminated_fragment"
    if semantic_role != "valuation_total":
        return "unknown"
    quote = str(row.get("quote") or "")
    context = str(row.get("context") or "")
    low = f"{quote}\n{context}".lower()
    aggregate_hits = len(_STANDALONE_STIMA_RE.findall(low))
    if _ROOT_AGGREGATE_RE.search(low):
        return "document_root_aggregate"
    if aggregate_hits >= 2:
        return "document_root_aggregate"
    if aggregate_hits == 1 and "valore di stima del bene" not in low and "identificativo corpo" not in low:
        return "document_root_aggregate"
    anchor = _structural_anchor(row)
    if anchor and anchor.startswith("lotto:"):
        return "lot_level"
    if anchor and anchor.startswith("bene:"):
        return "component_level"
    return "unknown"


def _annotate_structural_duplicates(candidates: List[Candidate]) -> None:
    seen: set[tuple[str, str | None, float]] = set()
    for cand in candidates:
        if cand.semantic_role != "valuation_total" or not cand.valid:
            continue
        scope = str((cand.metadata or {}).get("structural_scope") or "unknown")
        anchor = (cand.metadata or {}).get("structural_anchor")
        key = (scope, anchor, round(float(cand.value), 2))
        if scope != "unknown" and key in seen:
            cand.metadata["structural_scope"] = "duplicate_repeat"
        else:
            seen.add(key)


def _annotate_normalized_ownership(candidates: List[Candidate]) -> None:
    valuation_totals = [cand for cand in candidates if cand.semantic_role == "valuation_total" and cand.valid]
    unique_values = {round(float(cand.value), 2) for cand in valuation_totals}
    unique_anchors = {str((cand.metadata or {}).get("structural_anchor")) for cand in valuation_totals if (cand.metadata or {}).get("structural_anchor")}
    single_scope_root_equivalent = len(unique_anchors) == 1 and len(unique_values) == 1

    def has_root_pair(value: float) -> bool:
        for cand in valuation_totals:
            if round(float(cand.value), 2) == round(float(value), 2) and str((cand.metadata or {}).get("structural_scope") or "") == "document_root_aggregate" and not (cand.metadata or {}).get("structural_anchor"):
                return True
        return False

    for cand in candidates:
        metadata = cand.metadata or {}
        structural_scope = str(metadata.get("structural_scope") or "unknown")
        anchor = str(metadata.get("structural_anchor") or "")
        normalized = "unknown"
        if cand.invalid_reason in {"valuation_table_ratio_contamination", "unit_price_contamination", "subalterno_number_contamination"}:
            normalized = "contaminated_fragment"
        elif structural_scope == "duplicate_repeat":
            normalized = "repeated_echo"
        elif structural_scope == "document_root_aggregate":
            if not anchor or has_root_pair(float(cand.value)) or single_scope_root_equivalent:
                normalized = "document_root_effective"
            elif anchor.startswith("lotto:"):
                normalized = "lot_owned"
            elif anchor.startswith("bene:"):
                normalized = "component_only"
        elif anchor.startswith("lotto:"):
            normalized = "lot_owned"
        elif anchor.startswith("bene:"):
            normalized = "document_root_effective" if single_scope_root_equivalent else "component_only"
        metadata["normalized_ownership"] = normalized
        cand.metadata = metadata


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
        semantic_role = _pricing_role_context(row)
        valid_fields = ["pricing"]
        section_type = "valuation"
        confidence = 0.4
        if "subalterno" in low and amount < 10:
            semantic_role = "subalterno_id"
            valid_fields = []
            confidence = 0.0
        elif semantic_role == "auction_price":
            confidence = 0.98
        elif semantic_role == "valuation_total":
            confidence = 0.9
        elif semantic_role == "valuation_adjustment":
            valid_fields = ["costs", "pricing"]
            section_type = "costs"
            confidence = 0.92
        elif semantic_role == "buyer_cost":
            valid_fields = ["costs"]
            section_type = "costs"
            confidence = 0.9
        elif semantic_role == "net_valuation":
            confidence = 0.88
        elif semantic_role == "unit_price":
            confidence = 0.85
        if _is_valuation_table_ratio_noise(row, float(amount), semantic_role):
            out.append(
                Candidate(
                    value=float(amount),
                    field_key="pricing",
                    confidence=0.0,
                    evidence=[make_evidence(page, quote, "valuation_table_ratio_noise", [], 0.0, source="candidates_money")],
                    section_type="valuation",
                    semantic_role="valuation_table_ratio_noise",
                    valid=False,
                    invalid_reason="valuation_table_ratio_contamination",
                    source="candidates_money",
                    metadata={
                        "amount": float(amount),
                        "structural_scope": _structural_scope(row, semantic_role, invalid_reason="valuation_table_ratio_contamination"),
                        "structural_anchor": _structural_anchor(row),
                    },
                )
            )
            continue
        if _is_unit_price_contamination(row, float(amount), semantic_role):
            out.append(
                Candidate(
                    value=float(amount),
                    field_key="pricing",
                    confidence=0.0,
                    evidence=[make_evidence(page, quote, "unit_price_contamination", [], 0.0, source="candidates_money")],
                    section_type="valuation",
                    semantic_role="unit_price_contamination",
                    valid=False,
                    invalid_reason="unit_price_contamination",
                    source="candidates_money",
                    metadata={
                        "amount": float(amount),
                        "structural_scope": _structural_scope(row, semantic_role, invalid_reason="unit_price_contamination"),
                        "structural_anchor": _structural_anchor(row),
                    },
                )
            )
            continue
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
                    metadata={
                        "amount": amount,
                        "structural_scope": "contaminated_fragment",
                        "structural_anchor": _structural_anchor(row),
                    },
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
                metadata={
                    "amount": float(amount),
                    "structural_scope": _structural_scope(row, semantic_role),
                    "structural_anchor": _structural_anchor(row),
                },
            )
        )
    _annotate_structural_duplicates(out)
    _annotate_normalized_ownership(out)
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
