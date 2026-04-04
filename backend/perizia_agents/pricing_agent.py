from __future__ import annotations

import re

from perizia_runtime.state import Judgment, RuntimeState
from perizia_tools.valuation_table_tool import valuation_candidates


def _parse_it_amount(raw: str) -> float:
    return float(str(raw).replace(".", "").replace(",", "."))


def _direct_price_from_pages(state: RuntimeState):
    patterns = [
        re.compile(
            r"prezzo\s+a\s+base\s+d[' ]asta[\s\S]{0,260}?€\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)",
            re.IGNORECASE | re.DOTALL,
        ),
        re.compile(
            r"valore\s+al\s+netto\s+dei\s+costi\s+di\s+regolarizzazione\s+e\s+della\s+riduzione\s+cautelativa[\s\S]{0,260}?€\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)",
            re.IGNORECASE | re.DOTALL,
        ),
    ]
    for idx, page in enumerate(state.pages, start=1):
        text = str((page or {}).get("text") or "")
        page_number = int((page or {}).get("page_number") or (page or {}).get("page") or idx)
        for pattern in patterns:
            match = pattern.search(text)
            if match:
                amount = _parse_it_amount(match.group(1))
                start = max(0, match.start() - 80)
                end = min(len(text), match.end() + 80)
                quote = text[start:end].strip()
                return {
                    "amount": amount,
                    "page": page_number,
                    "quote": quote,
                    "confidence": 0.99 if "prezzo a base" in match.group(0).lower() else 0.96,
                }
    return None


def run_pricing_agent(state: RuntimeState) -> None:
    candidates = valuation_candidates(state.analysis_id)
    valid = [cand for cand in candidates if cand.valid]
    direct = _direct_price_from_pages(state)
    auction = [cand for cand in valid if cand.semantic_role == "auction_price"]
    net_values = [cand for cand in valid if cand.semantic_role == "net_valuation"]
    totals = [cand for cand in valid if cand.semantic_role == "valuation_total"]
    chosen = None
    if direct:
        chosen = None
    elif auction:
        chosen = sorted(auction, key=lambda item: -item.confidence)[0]
    elif net_values:
        chosen = sorted(net_values, key=lambda item: (-item.confidence, float(item.value)))[0]
    elif totals:
        chosen = sorted(totals, key=lambda item: (-item.confidence, float(item.value)))[0]
    benchmark = max([float(item.value) for item in totals + net_values] or [0.0])
    direct_absurd = bool(direct and direct["amount"] < 1000 and benchmark >= 10000)
    if direct_absurd:
        direct = None
    absurd = bool(chosen and float(chosen.value) < 1000 and benchmark >= 10000)
    state.canonical_case.pricing = {
        "selected_price": None if absurd else (direct["amount"] if direct else (float(chosen.value) if chosen else None)),
        "benchmark_value": benchmark or None,
        "absurdity_guard_triggered": absurd or direct_absurd,
        "candidate_count": len(valid),
        "invalid_candidates": [
            {
                "value": cand.value,
                "reason": cand.invalid_reason,
                "evidence": cand.evidence,
            }
            for cand in candidates
            if not cand.valid
        ] + (
            [{"value": None if not direct_absurd else "direct_price_candidate", "reason": "direct_price_absurdity_guard", "evidence": []}]
            if direct_absurd
            else []
        ),
        "guards": [
            "price_absurdity_guard",
            "nearby_number_contamination_rejected",
        ],
    }
    if direct and not absurd:
        state.judgments["pricing"] = Judgment(
            "pricing",
            float(direct["amount"]),
            "FOUND",
            float(direct["confidence"]),
            [],
            "pricing selected from explicit auction/net valuation sentence",
            {"benchmark_value": benchmark or None, "page": direct["page"], "quote": direct["quote"]},
        )
    elif chosen and not absurd:
        state.judgments["pricing"] = Judgment(
            "pricing",
            float(chosen.value),
            "FOUND",
            chosen.confidence,
            chosen.evidence,
            "pricing selected from valuation context after contamination guards",
            {"benchmark_value": benchmark or None},
        )
    else:
        state.judgments["pricing"] = Judgment(
            "pricing",
            None,
            "LOW_CONFIDENCE" if (absurd or direct_absurd) else "NOT_FOUND",
            0.0,
            chosen.evidence if chosen else [],
            "price rejected by absurdity guard" if (absurd or direct_absurd) else "no reliable price found",
            {"benchmark_value": benchmark or None, "absurdity_guard_triggered": absurd or direct_absurd},
        )
