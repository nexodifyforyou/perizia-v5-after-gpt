"""Valuation-chain completion: the customer chain must reach the document's
TERMINAL net values (state-of-fact + judicial sale), even when the analyst
mis-slots them into uncertain_money.

Root cause (Codogno lot 1): the two 8.4-net lines
  "valore dell'immobile al netto delle decurtazioni ... a carico della procedura"
  "valore dell'immobile al netto delle decurtazioni ... a carico dell'acquirente"
were dumped into money.uncertain_money, so the contract chain stopped at the
deductions and never showed the actual sale price. The fix promotes them into
current_state_value / sale_value (deterministic, additive, grounded).

All fixtures are generic/synthetic — nothing branches on a real city.
"""

import copy

from correctness_v2 import (
    analyst,
    contract as contract_mod,
    coverage_audit,
    doc_signals,
    lots as lots_mod,
    customer_report,
    quality_gate,
    validator as validator_mod,
)

from .sample_perizia import GENERIC_PERIZIA_PAGES, make_worksheet

_STATE_LABEL = (
    "Valore dell'immobile al netto delle decurtazioni nello stato di fatto in cui "
    "si trova, con le spese tecniche di regolarizzazione a carico della procedura"
)
_SALE_LABEL = (
    "Valore dell'immobile al netto delle decurtazioni nello stato di fatto in cui "
    "si trova, con le spese tecniche di regolarizzazione a carico dell'acquirente"
)


# ---------------------------------------------------------------------------
# doc_signals: canonical terminal-net-value phrasing
# ---------------------------------------------------------------------------
def test_a_carico_phrases_classify_terminal_net_values():
    state = doc_signals.classify_net_terminal(doc_signals.norm_text(_STATE_LABEL))
    sale = doc_signals.classify_net_terminal(doc_signals.norm_text(_SALE_LABEL))
    assert state and state[0] == "valore_stato"
    assert sale and sale[0] == "valore_vendita"


def test_a_carico_dell_acquirente_needs_the_net_anchor():
    # Without the "al netto delle decurtazioni" anchor, an ordinary buyer cost
    # "a carico dell'acquirente" is NEVER read as a judicial sale value.
    assert doc_signals.classify_net_terminal(
        doc_signals.norm_text("Oneri notarili a carico dell'acquirente")
    ) is None


def test_nearest_carico_decides_when_both_appear():
    # A wide window can contain an earlier "a carico dell'acquirente" cost line;
    # the phrase NEAREST the amount (rightmost) wins, so the two net lines are
    # not swapped.
    window = doc_signals.norm_text(
        "oneri a carico dell'acquirente nessuno ... valore dell'immobile al netto "
        "delle decurtazioni nello stato di fatto a carico della procedura:"
    )
    role = doc_signals.classify_net_terminal(window)
    assert role and role[0] == "valore_stato"


def test_terminal_net_values_extracted_when_before_window_is_generic():
    # The "al netto delle decurtazioni" anchor sits beyond the default before
    # window (long filler between it and the amount) so the amount would default
    # to importo_generico; the wider terminal check rescues it to state / sale.
    filler = (
        "il perito determina quanto segue in via del tutto definitiva e "
        "conclusiva e riepilogativa per il presente singolo lotto immobiliare "
        "pari alla somma complessiva di"
    )
    pages = [{
        "page_number": 1,
        "text": (
            f"riepilogo al netto delle decurtazioni a carico della procedura {filler} "
            f"€ 100.000,00 e inoltre al netto delle decurtazioni a carico "
            f"dell'acquirente {filler} € 90.000,00"
        ),
    }]
    by_amount = {s["amount"]: s for s in doc_signals.extract_money_signals(pages)}
    assert by_amount[100000.0]["role"] == doc_signals.ROLE_STATE_OF_FACT_VALUE
    assert by_amount[90000.0]["role"] == doc_signals.ROLE_JUDICIAL_SALE_VALUE


# ---------------------------------------------------------------------------
# contract.complete_valuation_terminals: promotion + additivity
# ---------------------------------------------------------------------------
def _worksheet_with_terminals_in_uncertain():
    ws = analyst.normalize_worksheet(make_worksheet())
    money = ws["money"]
    state = money["current_state_value"]
    sale = money["sale_value"]
    money["current_state_value"] = None
    money["sale_value"] = None
    money["uncertain_money"] = [
        {"label": _STATE_LABEL, "amount": state, "evidence_pages": [2]},
        {"label": _SALE_LABEL, "amount": sale, "evidence_pages": [2]},
        {"label": "Importo capitale ipoteca", "amount": 75000.0, "evidence_pages": [2]},
    ]
    return ws, state, sale


def test_promotes_terminal_net_values_out_of_uncertain_money():
    ws, state, sale = _worksheet_with_terminals_in_uncertain()
    out = contract_mod.complete_valuation_terminals(ws)
    money = out["money"]
    assert money["current_state_value"] == state
    assert money["sale_value"] == sale
    # Promoted rows leave uncertain_money; unrelated rows stay.
    labels = [r["label"] for r in money["uncertain_money"]]
    assert _STATE_LABEL not in labels and _SALE_LABEL not in labels
    assert any("ipoteca" in l.lower() for l in labels)
    # The input worksheet is never mutated.
    assert ws["money"]["current_state_value"] is None


def test_completion_is_additive_when_terminals_are_explicit():
    ws = analyst.normalize_worksheet(make_worksheet())
    out = contract_mod.complete_valuation_terminals(ws)
    # Explicit terminals present -> no change at all (same object returned).
    assert out is ws


# ---------------------------------------------------------------------------
# End to end: the chain reaches the sale price and the gate stays non-blocking
# ---------------------------------------------------------------------------
def _build_and_gate(worksheet, pages):
    worksheet = contract_mod.complete_valuation_terminals(worksheet, pages)
    worksheet, _gr = validator_mod.apply_compliance_evidence_gate(worksheet, pages)
    vr = validator_mod.validate_worksheet(worksheet, pages)
    assert vr["validation_status"] == validator_mod.STATUS_VALIDATED, vr.get("violations")
    lot_report = lots_mod.build_lot_report(worksheet, pages)
    contract = contract_mod.build_contract(
        worksheet=worksheet, validator_report=vr, analysis_id="an", job_id="jb",
        source_pdf_quality_status="PDF_QUALITY_OK", lot_report=lot_report,
        surface_cadastral=doc_signals.extract_surface_cadastral(pages),
    )
    report = customer_report.render_success_report(contract, pages)
    gate = quality_gate.run_quality_gate(
        job_id="jb", analysis_id="an", pages=pages, worksheet=worksheet,
        contract=contract, customer_report=report, validator_report=vr,
        lot_report=lot_report, persist=False,
    )
    return contract, gate


def test_chain_reaches_sale_price_and_gate_not_blocking():
    ws, state, sale = _worksheet_with_terminals_in_uncertain()
    contract, gate = _build_and_gate(ws, copy.deepcopy(GENERIC_PERIZIA_PAGES))
    chain = [(r["label"], r["amount"]) for r in contract["valuation_chain"]]
    amounts = [a for _l, a in chain]
    # The terminal net values are now IN the chain, ending with the sale price.
    assert state in amounts and sale in amounts
    assert amounts[-1] == sale
    assert any("stato di fatto" in l.lower() for l, _a in chain)
    assert any("vendita giudiziaria" in l.lower() for l, _a in chain)
    # Promoting a genuinely-present value keeps the gate non-blocking.
    assert gate["gate_status"] != quality_gate.GATE_FAIL
    assert not gate["quality_report"]["blocking_issues"]


def test_correct_perizia_chain_is_unchanged_by_completion():
    # A worksheet that already carries explicit terminals must produce the exact
    # same chain as before (additivity / no regression).
    ws = analyst.normalize_worksheet(make_worksheet())
    contract, gate = _build_and_gate(ws, copy.deepcopy(GENERIC_PERIZIA_PAGES))
    amounts = [r["amount"] for r in contract["valuation_chain"]]
    # market -> regularization -> state-of-fact -> cancellation -> judicial sale.
    assert amounts == [100000.0, 5000.0, 95000.0, 300.0, 94700.0]
    assert gate["gate_status"] != quality_gate.GATE_FAIL


# ---------------------------------------------------------------------------
# Grounded doc-signals AUTHORITY over the analyst's terminal-role slotting.
# Codogno lot 1 (live): the analyst put the SALE value into current_state_value
# ("stato di fatto") and dropped the real state-of-fact value out of the chain.
# The document names both net values unambiguously, so it is the authority.
# ---------------------------------------------------------------------------
_FILLER = (
    "il perito determina quanto segue in via del tutto definitiva e "
    "conclusiva e riepilogativa per il presente singolo lotto immobiliare "
    "pari alla somma complessiva di"
)


def _pages_with_unique_terminals(state_disp, sale_disp):
    return [{
        "page_number": 2,
        "text": (
            f"riepilogo al netto delle decurtazioni a carico della procedura {_FILLER} "
            f"€ {state_disp} e inoltre al netto delle decurtazioni a carico "
            f"dell'acquirente {_FILLER} € {sale_disp}"
        ),
    }]


def test_doc_authority_corrects_a_mislabeled_terminal():
    # current_state_value carries the SALE amount (mislabel) and the real
    # state-of-fact value is missing; doc_signals confidently classifies both on
    # the page -> the field is corrected and both terminals land in the right slot.
    ws = analyst.normalize_worksheet(make_worksheet())
    ws["money"]["current_state_value"] = 80000.0   # sale value, mislabeled
    ws["money"]["sale_value"] = None
    out = contract_mod.complete_valuation_terminals(
        ws, _pages_with_unique_terminals("88.000,00", "80.000,00"))
    assert out["money"]["current_state_value"] == 88000.0
    assert out["money"]["sale_value"] == 80000.0
    # Input worksheet is never mutated.
    assert ws["money"]["current_state_value"] == 80000.0


def test_doc_authority_needs_confidence_else_confirmation_fallback():
    # Two different state-of-fact candidates on the page => not confident, so the
    # authority does NOT inject: genuinely ambiguous docs fall through to the
    # money-confirmation feature instead of guessing.
    pages = [{
        "page_number": 2,
        "text": (
            f"al netto delle decurtazioni a carico della procedura {_FILLER} € 88.000,00 "
            f"e ancora al netto delle decurtazioni a carico della procedura {_FILLER} "
            f"€ 89.000,00"
        ),
    }]
    ws = analyst.normalize_worksheet(make_worksheet())
    ws["money"]["current_state_value"] = None
    out = contract_mod.complete_valuation_terminals(ws, pages)
    assert out["money"]["current_state_value"] is None  # not injected from ambiguous doc
