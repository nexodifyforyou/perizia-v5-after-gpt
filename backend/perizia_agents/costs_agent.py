from __future__ import annotations

import re

from perizia_runtime.state import CanonicalIssue, Judgment, RuntimeState
from perizia_tools.evidence_span_tool import make_evidence
from perizia_tools.valuation_table_tool import cost_line_candidates, valuation_candidates


def _parse_it_amount(raw: str) -> float:
    return float(str(raw).replace(".", "").replace(",", "."))


def _extract_amounts(text: str):
    # Handles both "€ X" and "€. X" (Italian abbreviated currency format)
    return [_parse_it_amount(match) for match in re.findall(r"€\.?\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)", text or "")]


def _extract_direct_costs(state: RuntimeState):
    explicit_costs = []
    valuation_adjustments = []
    explicit_total = None
    label_order = [
        ("spese_tecniche_urbanistiche", "spese tecniche di regolazione difformità urbanistico edilizie"),
        ("spese_tecniche_catastali", "spese tecniche di regolazione catastale"),
        ("spese_condominiali", "spese condominiali scadute e non pagate"),
        # Handles "Spese tecniche di regolarizzazione urbanistico e/o catastale" format
        ("spese_regolarizzazione", "spese tecniche di regolarizzazione"),
    ]
    adjustment_pattern = re.compile(
        r"(?:riduzione\s+cautelativa|riduzione\s+del\s+valore)[\s\S]{0,260}?€\.?\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)",
        re.I | re.DOTALL,
    )
    for idx, page in enumerate(state.pages, start=1):
        text = str((page or {}).get("text") or "")
        low = text.lower()
        page_number = int((page or {}).get("page_number") or (page or {}).get("page") or idx)
        for i, (label, anchor) in enumerate(label_order):
            start_idx = low.find(anchor)
            if start_idx < 0:
                continue
            next_markers = [low.find(next_anchor, start_idx + len(anchor)) for _, next_anchor in label_order[i + 1:]]
            next_markers += [
                low.find("totale", start_idx + len(anchor)),
                low.find("riduzione cautelativa", start_idx + len(anchor)),
                # "Prezzo base d'asta" introduces the auction price, not a buyer cost.
                low.find("prezzo base", start_idx + len(anchor)),
            ]
            next_markers = [marker for marker in next_markers if marker > start_idx]
            end_idx = min(next_markers) if next_markers else min(len(text), start_idx + 260)
            window = text[start_idx:end_idx]
            amounts = _extract_amounts(window)
            if not amounts:
                continue
            # Take the first amount after the label — the last may fall outside the cost
            # line and into the auction-price section if the window crosses a heading.
            amount = amounts[0]
            explicit_costs.append(
                {
                    "label": label,
                    "amount": amount,
                    "evidence": [make_evidence(page_number, window[:260], "buyer_cost", ["costs"], 0.96)],
                }
            )
        total_idx = low.find("totale")
        riduzione_idx = low.find("riduzione cautelativa")
        if total_idx >= 0 and riduzione_idx > total_idx:
            total_window = text[total_idx:riduzione_idx]
            total_amounts = _extract_amounts(total_window)
            if total_amounts:
                explicit_total = max(total_amounts)
        adjustment_match = adjustment_pattern.search(text)
        if adjustment_match:
            amount = _parse_it_amount(adjustment_match.group(1))
            quote = text[max(0, adjustment_match.start() - 40): min(len(text), adjustment_match.end() + 40)].strip()
            valuation_adjustments.append(
                {
                    "amount": amount,
                    "evidence": [make_evidence(page_number, quote, "valuation_adjustment", ["costs", "pricing"], 0.95)],
                }
            )
    return explicit_costs, valuation_adjustments, explicit_total


def run_costs_agent(state: RuntimeState) -> None:
    money_candidates = valuation_candidates(state.analysis_id)
    line_candidates = cost_line_candidates(state.pages)
    direct_costs, direct_adjustments, direct_total = _extract_direct_costs(state)
    state.candidates["valuation_money"] = money_candidates
    explicit_costs = []
    valuation_adjustments = []
    if direct_costs or direct_total is not None:
        # Deduplicate costs with same label+amount (same cost appearing on multiple pages)
        seen_label_amount: set = set()
        deduped_costs = []
        for cost in direct_costs:
            key = (cost.get("label"), round(float(cost.get("amount", 0)), 2))
            if key not in seen_label_amount:
                seen_label_amount.add(key)
                deduped_costs.append(cost)
        direct_costs = deduped_costs
        explicit_costs = direct_costs
        valuation_adjustments = direct_adjustments
        # When label-matched costs exist, sum them directly — direct_total from the
        # TOTALE→riduzione-cautelativa window may capture the valuation table total,
        # not the buyer-cost sub-total.
        if direct_costs:
            explicit_total = round(sum(item["amount"] for item in direct_costs), 2)
        elif direct_total is not None:
            explicit_total = round(float(direct_total), 2)
        else:
            explicit_total = 0.0
        if not explicit_costs and direct_total is not None:
            explicit_costs = [
                {
                    "label": "explicit_total",
                    "amount": explicit_total,
                    "evidence": [],
                    "confidence": 0.9,
                }
            ]
    else:
        for cand in money_candidates:
            if not cand.valid:
                continue
            if cand.semantic_role == "buyer_cost":
                explicit_costs.append({"amount": float(cand.value), "confidence": cand.confidence, "evidence": cand.evidence})
            elif cand.semantic_role == "valuation_adjustment":
                valuation_adjustments.append({"amount": float(cand.value), "confidence": cand.confidence, "evidence": cand.evidence})
        explicit_costs.sort(key=lambda item: float(item["amount"]))
        explicit_total = round(sum(float(item["amount"]) for item in explicit_costs), 2) if explicit_costs else 0.0
    support_evidence = []
    for item in explicit_costs:
        for ev in item.get("evidence", []):
            if ev:
                support_evidence.append(ev)
    if not support_evidence and direct_total is not None:
        support_evidence = [cand.evidence[0] for cand in line_candidates if cand.evidence][:2]
        if support_evidence:
            explicit_costs = [
                {
                    "label": "explicit_total_supported",
                    "amount": explicit_total,
                    "evidence": support_evidence,
                    "confidence": 0.72,
                }
            ]
    state.canonical_case.costs = {
        "explicit_buyer_costs": [
            {
                "amount": float(item["amount"]),
                "confidence": item.get("confidence", 0.96),
                "evidence": item.get("evidence", []),
                "label": (item.get("label") or (item.get("evidence", [{}])[0].quote[:120] if item.get("evidence") else "")),
            }
            for item in explicit_costs
        ],
        "valuation_adjustments": [
            {
                "amount": float(item["amount"]),
                "confidence": item.get("confidence", 0.95),
                "evidence": item.get("evidence", []),
            }
            for item in valuation_adjustments
        ],
        "explicit_total": explicit_total if explicit_costs or direct_total is not None else None,
        "explicit_total_low_confidence": round(float(direct_total), 2) if direct_total is not None and not support_evidence else None,
        "lines": [cand.evidence[0].quote for cand in line_candidates if cand.evidence][:3],
        "guards": [
            "explicit_buyer_side_costs_preferred_over_vague_legal_noise",
            "no_explicit_buyer_cost_issue_without_grounded_evidence",
            "riduzione_cautelativa_kept_as_valuation_adjustment",
        ],
    }
    if explicit_costs and support_evidence:
        top_cost = explicit_costs[-1]
        state.judgments["costs_summary"] = Judgment(
            "costs_summary",
            {"explicit_total": explicit_total},
            "FOUND",
            0.92,
            top_cost.get("evidence", []),
            "explicit buyer-side costs extracted from perizia cost table",
        )
        state.issues.append(
            CanonicalIssue(
                code="EXPLICIT_BUYER_COSTS",
                title_it=f"Costi espliciti a carico dell'acquirente: € {explicit_total:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                severity="AMBER",
                category="costs",
                priority_score=70.0 if explicit_total >= 1000 else 55.0,
                evidence=top_cost.get("evidence", []),
                summary_it="La perizia indica costi espliciti a carico dell'acquirente.",
                action_it="Verifica l'importo totale dei costi e il perimetro delle spese prima dell'offerta.",
                metadata={"explicit_total": explicit_total},
            )
        )
    elif direct_total is not None:
        state.judgments["costs_summary"] = Judgment(
            "costs_summary",
            {"explicit_total": round(float(direct_total), 2)},
            "LOW_CONFIDENCE",
            0.35,
            support_evidence,
            "cost total detected without grounded supporting evidence lines; not promoted to issue",
        )
    else:
        state.judgments["costs_summary"] = Judgment("costs_summary", None, "NOT_FOUND", 0.0, [], "no explicit buyer-side costs found")
