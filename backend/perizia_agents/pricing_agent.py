from __future__ import annotations

import re

from perizia_runtime.state import Judgment, RuntimeState
from perizia_tools.valuation_table_tool import valuation_candidates


def _parse_it_amount(raw: str) -> float:
    return float(str(raw).replace(".", "").replace(",", "."))


def _has_multiple_distinct_auction_prices(candidates) -> bool:
    amounts = sorted({round(float(item.value), 2) for item in candidates if isinstance(item.value, (int, float))})
    return len(amounts) > 1


def _unique_amount(values):
    unique = sorted({round(float(value), 2) for value in values if isinstance(value, (int, float))})
    if len(unique) == 1:
        return unique[0]
    return None


def _unique_document_root_benchmark(valuation_totals):
    aggregate = [
        cand for cand in valuation_totals
        if str((cand.metadata or {}).get("normalized_ownership") or "") == "document_root_effective"
    ]
    return _unique_amount([cand.value for cand in aggregate])


def _select_executable_price_candidate(auction, net_values, *, has_multiple_auction_prices: bool):
    if has_multiple_auction_prices:
        return None
    if auction:
        return sorted(auction, key=lambda item: -item.confidence)[0]
    if net_values:
        return sorted(net_values, key=lambda item: (-item.confidence, float(item.value)))[0]
    return None


def _collect_page_amount_matches(state: RuntimeState, patterns, *, confidence: float):
    matches = []
    for idx, page in enumerate(state.pages, start=1):
        text = str((page or {}).get("text") or "")
        page_number = int((page or {}).get("page_number") or (page or {}).get("page") or idx)
        for pattern in patterns:
            for match in pattern.finditer(text):
                amount = _parse_it_amount(match.group(1))
                start = max(0, match.start() - 100)
                end = min(len(text), match.end() + 100)
                quote = text[start:end].strip()
                matches.append(
                    {
                        "amount": amount,
                        "page": page_number,
                        "quote": quote,
                        "confidence": confidence,
                    }
                )
    return matches


def _unique_explicit_layer(state: RuntimeState, patterns, *, confidence: float):
    matches = _collect_page_amount_matches(state, patterns, confidence=confidence)
    amount = _unique_amount([row["amount"] for row in matches])
    if amount is None:
        return None
    for row in matches:
        if round(float(row["amount"]), 2) == round(float(amount), 2):
            return row
    return None


def _direct_price_from_pages(state: RuntimeState):
    patterns = [
        re.compile(
            r"prezzo\s+a\s+base\s+d[' ]asta[\s\S]{0,260}?€\.?\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)",
            re.IGNORECASE | re.DOTALL,
        ),
        re.compile(
            r"valore\s+di\s+vendita\s+giudiziaria[\s\S]{0,260}?€\.?\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)",
            re.IGNORECASE | re.DOTALL,
        ),
        re.compile(
            r"valore\s+al\s+netto\s+dei\s+costi\s+di\s+regolarizzazione\s+e\s+della\s+riduzione\s+cautelativa[\s\S]{0,260}?€\.?\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)",
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


def _direct_adjusted_market_from_pages(state: RuntimeState):
    patterns = [
        re.compile(
            r"valore\s+di\s+mercato\s+dell[' ]immobile\s+nello\s+stato\s+di\s+fatto\s+e\s+di\s+diritto\s+in\s+cui\s+si\s+trova[\s\S]{0,260}?€\.?\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)",
            re.IGNORECASE | re.DOTALL,
        ),
        re.compile(
            r"valore\s+al\s+netto\s+dei\s+costi\s+di\s+regolarizzazione(?!\s+e\s+della\s+riduzione\s+cautelativa)[\s\S]{0,260}?€\.?\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)",
            re.IGNORECASE | re.DOTALL,
        ),
        re.compile(
            r"valore\s+al\s+netto\s+delle\s+decurtazioni(?![\s\S]{0,80}vendita\s+giudiziaria)[\s\S]{0,260}?€\.?\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)",
            re.IGNORECASE | re.DOTALL,
        ),
    ]
    return _unique_explicit_layer(state, patterns, confidence=0.95)


def _direct_gross_market_from_pages(state: RuntimeState):
    patterns = [
        re.compile(
            r"valore\s+di\s+mercato\s*\(1000/1000[\s\S]{0,200}?€\.?\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)",
            re.IGNORECASE | re.DOTALL,
        ),
        re.compile(
            r"valore\s+complessivo\s*\(vc\)[\s\S]{0,120}?€\.?\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)",
            re.IGNORECASE | re.DOTALL,
        ),
    ]
    return _unique_explicit_layer(state, patterns, confidence=0.94)


def run_pricing_agent(state: RuntimeState) -> None:
    candidates = valuation_candidates(state.analysis_id)
    valid = [cand for cand in candidates if cand.valid]
    direct = _direct_price_from_pages(state)
    direct_adjusted = _direct_adjusted_market_from_pages(state)
    direct_benchmark = _direct_gross_market_from_pages(state)
    auction = [cand for cand in valid if cand.semantic_role == "auction_price"]
    net_values = [cand for cand in valid if cand.semantic_role == "net_valuation"]
    totals = [cand for cand in valid if cand.semantic_role == "valuation_total"]
    has_multiple_auction_prices = _has_multiple_distinct_auction_prices(auction)
    chosen = None if direct else _select_executable_price_candidate(
        auction,
        net_values,
        has_multiple_auction_prices=has_multiple_auction_prices,
    )
    benchmark = direct_benchmark["amount"] if direct_benchmark else _unique_document_root_benchmark(totals)
    adjusted_market_value = direct_adjusted["amount"] if direct_adjusted else None
    direct_absurd = bool(direct and direct["amount"] < 1000 and benchmark >= 10000)
    if direct_absurd:
        direct = None
    absurd = bool(chosen and float(chosen.value) < 1000 and benchmark >= 10000)
    state.canonical_case.pricing = {
        "selected_price": None if absurd else (direct["amount"] if direct else (float(chosen.value) if chosen else None)),
        "benchmark_value": benchmark,
        "adjusted_market_value": adjusted_market_value,
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
        ) + (
            [{"value": "multiple_auction_prices", "reason": "multi_lot_scalar_price_suppressed", "evidence": []}]
            if has_multiple_auction_prices
            else []
        ) + (
            [{"value": "multiple_benchmark_values", "reason": "multi_lot_scalar_benchmark_suppressed", "evidence": []}]
            if benchmark is None and any(str((cand.metadata or {}).get("normalized_ownership") or "") in {"lot_owned", "component_only"} for cand in totals)
            else []
        ),
        "guards": [
            "price_absurdity_guard",
            "nearby_number_contamination_rejected",
            "multi_lot_scalar_price_suppressed",
            "multi_lot_scalar_benchmark_suppressed",
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
            {
                "benchmark_value": benchmark,
                "adjusted_market_value": adjusted_market_value,
                "page": direct["page"],
                "quote": direct["quote"],
            },
        )
    elif chosen and not absurd:
        state.judgments["pricing"] = Judgment(
            "pricing",
            float(chosen.value),
            "FOUND",
            chosen.confidence,
            chosen.evidence,
            "pricing selected from valuation context after contamination guards",
            {"benchmark_value": benchmark, "adjusted_market_value": adjusted_market_value},
        )
    else:
        state.judgments["pricing"] = Judgment(
            "pricing",
            None,
            "LOW_CONFIDENCE" if (absurd or direct_absurd) else "NOT_FOUND",
            0.0,
            chosen.evidence if chosen else [],
            "price rejected by absurdity guard" if (absurd or direct_absurd) else "no reliable price found",
            {
                "benchmark_value": benchmark,
                "adjusted_market_value": adjusted_market_value,
                "absurdity_guard_triggered": absurd or direct_absurd,
            },
        )
