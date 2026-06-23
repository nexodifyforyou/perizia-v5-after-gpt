"""General money/cost representation normalization gate.

These tests lock the display-time normalization layer that prevents monetary
fragments, unit rates, percentages and income figures from being shown to a
customer as if they were valuation values or buyer costs. They reproduce the
real Roma EI 928 (analysis_e8e45af93680) leak modes deterministically by
re-projecting candidate-level inputs through ``_build_projected_money_box``.

Every monetary figure must have a role, evidence, confidence and a display
decision: no role => no value display.
"""

import sys
from pathlib import Path
from typing import Any, Dict, List

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import copy  # noqa: E402

from perizia_authority_money_projection import (  # noqa: E402
    _build_projected_money_box,
    _resolve_value_anchors,
    _value_suppression_reason,
)
from customer_decision_contract import sanitize_customer_facing_result  # noqa: E402
from pdf_report import money_report_payload_from_result  # noqa: E402


# The exact Roma EI 928 fragment/rate amounts that must never be customer-visible
# as money lines.
ROMA_SUPPRESSED_AMOUNTS = (100, 300, 832, 1832, 2000, 2950, 3900, 4300)


def _customer_money_amounts(box: Dict[str, Any]) -> set:
    # The groups a fragment could misleadingly surface in (value/price/other/unknown).
    out = set()
    for g in ("valuation_references", "price_references", "valuation_deductions",
              "other_monetary_mentions", "unsupported_or_unknown_amounts",
              "buyer_costs_confirmed", "buyer_cost_signals_to_verify"):
        for it in box.get(g) or []:
            if isinstance(it, dict):
                out.add(it.get("amount_eur"))
    return out


def _pdf_money_text(box: Dict[str, Any]) -> str:
    payload = money_report_payload_from_result({"section_3_money_box": box})
    rows_text = []
    for grp in payload.get("items") or []:
        rows_text.append(str(grp.get("label") or ""))
        for r in grp.get("rows") or grp.get("items") or []:
            rows_text.append(str(r.get("title") or "") + " " + str(r.get("amount") or ""))
    return " ".join(rows_text)


def _cand(amount: int, role: str, raw: str, *, page: int = 11, quote: str = "", lot: str = None) -> Dict[str, Any]:
    return {
        "amount_eur": amount,
        "role": role,
        "semantic_base_role": role,
        "raw_text": quote or f"... {raw} ...",
        "amount_raw": raw,
        "page": page,
        "lot_label": lot,
        "confidence": 0.8,
    }


def _project(cands: List[Dict[str, Any]]) -> Dict[str, Any]:
    box, _meta = _build_projected_money_box({"money_candidates": cands, "summary": {}}, {})
    assert box is not None
    return box


def _amounts(box: Dict[str, Any], group: str) -> set:
    return {it.get("amount_eur") for it in box.get(group, []) if isinstance(it, dict)}


def _titles(box: Dict[str, Any], group: str) -> List[str]:
    return [str(it.get("customer_title_it") or "") for it in box.get(group, [])]


# Roma-like candidate set: real anchors plus the exact fragment/rate leak modes.
def _roma_like() -> List[Dict[str, Any]]:
    return [
        _cand(312708, "market_value", "312.708,00", quote="Valore di stima: € 312.708,00"),
        _cand(172000, "final_value", "172.000,00", quote="Valore finale di stima € 172.000,00"),
        _cand(172000, "base_auction", "172.000,00", page=2, quote="Prezzo base d'asta € 172.000,00"),
        _cand(3300, "market_value", "3.300,00", quote="94,76 mq 3.300,00 €/mq € 312.708,00"),
        _cand(3900, "price", "€/Mq 3.900,00 4", page=10, quote="€/Mq 3.900,00"),
        _cand(2950, "unknown_money", "€/Mq) 2.950 4.300", page=10, quote="€/Mq) 2.950 4.300"),
        _cand(4300, "unknown_money", "4.300 CANONI (€", page=10, quote="4.300 CANONI"),
        _cand(832, "unknown_money", "832,13 €", page=5,
              quote="94,76 mq 3.300,00 €/mq € 312.708,00 ... 832,13 € ..."),
        _cand(300, "market_value", "300,00 €", quote="... 300,00 € ..."),
        _cand(100, "market_value", "100,00% €", quote="vizi occulti 100,00%"),
        _cand(2000, "final_value", "2.000,00", quote="Deprezzamento ... 2.000,00"),
    ]


def test_real_anchors_remain_visible_as_value_and_price():
    box = _project(_roma_like())
    value_visible = _amounts(box, "valuation_references") | _amounts(box, "price_references")
    # prezzo base + valore di stima + valore finale must still be displayable
    assert 312708 in value_visible
    assert 172000 in value_visible


def test_unit_mq_rate_not_shown_as_market_total():
    box = _project(_roma_like())
    # €3.300/mq, €3.900/mq, €2.950/mq must not be a valuation/price headline.
    for rate in (3900, 2950):
        assert rate not in _amounts(box, "valuation_references")
        assert rate not in _amounts(box, "price_references")


def test_rendita_fragment_832_is_not_valuation():
    box = _project(_roma_like())
    assert 832 not in _amounts(box, "valuation_references")
    assert 832 not in _amounts(box, "price_references")
    # The OCR shard is demoted out of the value section and flagged not visible.
    hidden = [it for it in (box.get("other_monetary_mentions") or []) if it.get("amount_eur") == 832]
    assert hidden and all(it.get("customer_visible") is False for it in hidden)


def test_explicitly_labelled_rendita_goes_to_cadastral_not_valuation():
    # A cleanly-labelled "Rendita catastale" is allowed in the cadastral group
    # (catasto/dettagli), never as a valuation/price total.
    cands = [
        _cand(312708, "market_value", "312.708,00", quote="Valore di stima € 312.708,00"),
        _cand(832, "cadastral_rendita", "832,13 €", page=5, quote="Rendita catastale Euro 832,13"),
    ]
    box = _project(cands)
    assert 832 not in _amounts(box, "valuation_references")
    assert 832 not in _amounts(box, "price_references")
    assert 832 in _amounts(box, "cadastral_values")


def test_small_fragments_suppressed_when_anchor_large():
    box = _project(_roma_like())
    for frag in (100, 300):
        assert frag not in _amounts(box, "valuation_references")
        assert frag not in _amounts(box, "price_references")


def test_deprezzamento_fragment_2000_not_final_value():
    box = _project(_roma_like())
    assert 2000 not in _amounts(box, "valuation_references")
    assert 2000 not in _amounts(box, "price_references")


def test_canone_income_not_valuation():
    box = _project(_roma_like())
    assert 4300 not in _amounts(box, "valuation_references")
    assert 4300 not in _amounts(box, "price_references")


def test_percentage_not_shown_as_euro_value():
    box = _project(_roma_like())
    # "100,00%" must never become a euro valuation.
    assert 100 not in _amounts(box, "valuation_references")


def test_no_fake_zero_total():
    box = _project(_roma_like())
    total = box.get("total_extra_cost_eur")
    assert total is None or total > 0


def test_anchor_resolution_picks_largest_trusted_value():
    cands = _roma_like()
    by_lot, global_anchor = _resolve_value_anchors(cands)
    assert global_anchor == 312708


def test_unit_price_reference_role_kept_as_rate_label():
    # An amount explicitly tagged unit_price_reference is allowed to display as a
    # supporting rate (never demoted, never a total).
    cands = [
        _cand(312708, "market_value", "312.708,00", quote="Valore di stima € 312.708,00"),
        _cand(3300, "unit_price_reference", "3.300,00 €", quote="3.300,00 €/mq"),
    ]
    box = _project(cands)
    assert _value_suppression_reason(cands[1], "valuation_references", _resolve_value_anchors(cands)) is None
    assert 3300 in _amounts(box, "valuation_references")


def test_small_lot_without_anchor_does_not_over_suppress():
    # No trusted anchor (>= 8000) => no anchor-implausibility suppression; a small
    # standalone market value stays visible (conservative, avoids nuking cheap lots).
    cands = [_cand(6000, "market_value", "6.000,00", quote="Valore di stima € 6.000,00")]
    by_lot, global_anchor = _resolve_value_anchors(cands)
    assert global_anchor == 0.0
    box = _project(cands)
    assert 6000 in (_amounts(box, "valuation_references") | _amounts(box, "price_references"))


def test_per_lot_anchor_does_not_cross_contaminate():
    # A small but legitimate value in lot B is judged against lot B's own anchor,
    # not lot A's much larger anchor.
    cands = [
        _cand(500000, "market_value", "500.000,00", lot="LOTTO 1", quote="Valore di stima € 500.000,00"),
        _cand(40000, "market_value", "40.000,00", lot="LOTTO 2", quote="Valore di stima € 40.000,00"),
    ]
    box = _project(cands)
    # 40.000 is implausible vs 500.000 but plausible vs its own lot anchor (itself)
    assert 40000 in (_amounts(box, "valuation_references") | _amounts(box, "price_references"))


def test_suppressed_fragments_not_in_customer_api_money_box():
    box = _project(_roma_like())
    cust = copy.deepcopy({"money_box": box})
    sanitize_customer_facing_result(cust)
    visible = _customer_money_amounts(cust["money_box"])
    for amt in ROMA_SUPPRESSED_AMOUNTS:
        assert amt not in visible, f"€{amt} must be dropped from the customer API money box"
    # Real anchors survive in the customer payload.
    assert 312708 in visible
    assert 172000 in visible


def test_suppressed_fragments_not_rendered_as_pdf_money_rows():
    box = _project(_roma_like())
    text = _pdf_money_text(box)
    # No suppressed fragment may appear as a money-row title/amount.
    assert "Importo monetario citato in perizia" not in text
    for label in ("€ 832", "€ 300", "€ 100", "€ 4.300", "€ 2.950", "€ 3.900"):
        assert label not in text, f"{label} must not be a customer money row"
    # Customer-safe categories still present.
    assert "Prezzo base" in text or "Valore" in text


def test_suppressed_fragments_retained_internally_for_audit():
    box = _project(_roma_like())
    # The amounts still exist in the projected box (hidden), so the audit trail is intact.
    internal = set()
    for g in ("other_monetary_mentions", "cadastral_values", "valuation_deductions"):
        for it in box.get(g) or []:
            internal.add(it.get("amount_eur"))
    assert any(a in internal for a in ROMA_SUPPRESSED_AMOUNTS)
    # ...but every such row is explicitly flagged not customer-visible (or cadastral).
    for g in ("other_monetary_mentions",):
        for it in box.get(g) or []:
            if it.get("amount_eur") in ROMA_SUPPRESSED_AMOUNTS:
                assert it.get("customer_visible") is False


def test_unknown_money_value_duplicate_hidden_but_label_kept():
    box = _project(_roma_like())
    cust = copy.deepcopy({"money_box": box})
    sanitize_customer_facing_result(cust)
    vr = cust["money_box"].get("valuation_references") or []
    roles_312 = [it.get("role") for it in vr if it.get("amount_eur") == 312708]
    # The duplicate unknown_money echo is gone; the labelled value remains exactly once per group.
    assert "unknown_money" not in roles_312
    assert roles_312.count("market_value") <= 1
    # In the raw box, the duplicate is retained but flagged hidden.
    raw_vr = box.get("valuation_references") or []
    dup = [it for it in raw_vr if it.get("amount_eur") == 312708 and it.get("role") == "unknown_money"]
    assert all(it.get("customer_visible") is False for it in dup)


def test_no_debug_or_internal_money_terms_in_customer_payload():
    box = _project(_roma_like())
    cust = copy.deepcopy({"money_box": box})
    sanitize_customer_facing_result(cust)
    text = str(cust)
    # Internal flags introduced by this patch must never reach the customer payload.
    for term in ("value_suppression_reason", "force_generic_money_title", "customer_hidden"):
        assert term not in text, f"internal term {term!r} leaked into customer payload"
