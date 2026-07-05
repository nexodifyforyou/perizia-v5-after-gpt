"""Quality-gate role fixes: money-role-aware audit, bene/property structure,
buyer-side/formality consistency, comparatives grouping, customer evidence.

All fixtures are generic/synthetic — nothing here branches on a real city.
Covers the required behaviors:
  1.  bene_count is never 0 for a real single-property report
  2.  accessory/pertinenza (soffitta/cantina/...) renders under the main bene
  3.  a buyer-side cost already in the valuation chain renders once in
      buyer_side_costs with included_in_valuation=True
  4.  the procedure-cancelled money section is not empty when formalities exist
  5.  the page audit CONTRADICTS (gate FAIL) when an amount matches but the
      money role is wrong (swapped market/state-of-fact values)
  6.  market comparatives group separately from uncertain_money
  7.  the customer evidence index hides raw internal keys and quotes VERBATIM
      page excerpts; a missing excerpt creates a coverage warning
"""

import copy
import json
import re

from correctness_v2 import (
    analyst,
    contract as contract_mod,
    doc_signals,
    lots as lots_mod,
    customer_report,
    quality_gate,
    validator as validator_mod,
)

from .sample_perizia import GENERIC_PERIZIA_PAGES, make_worksheet

VALIDATED_REPORT = {
    "validation_status": "VALIDATED",
    "checks": {"money_signals": {"base_price_explicit_text": True}},
    "warnings": [],
}


def _build(pages, ws_raw, validator_report=None):
    worksheet = analyst.normalize_worksheet(ws_raw)
    vr = validator_report or validator_mod.validate_worksheet(worksheet, pages)
    lot_report = lots_mod.build_lot_report(worksheet, pages)
    contract = contract_mod.build_contract(
        worksheet=worksheet,
        validator_report=vr,
        analysis_id="an_r",
        job_id="job_r",
        source_pdf_quality_status="PDF_QUALITY_OK",
        lot_report=lot_report,
        surface_cadastral=doc_signals.extract_surface_cadastral(pages),
    )
    report = customer_report.render_success_report(contract, pages)
    return worksheet, vr, lot_report, contract, report


def _gate(pages, worksheet, contract, report, vr, lot_report):
    return quality_gate.run_quality_gate(
        job_id="job_r",
        analysis_id="an_r",
        pages=pages,
        worksheet=worksheet,
        contract=contract,
        customer_report=report,
        validator_report=vr,
        lot_report=lot_report,
        persist=False,
    )


# ---------------------------------------------------------------------------
# Money-role classification of chained value labels (document side)
# ---------------------------------------------------------------------------
def test_chained_value_labels_classified_by_role_not_by_bleed():
    pages = [
        {
            "page_number": 1,
            "text": (
                "Valore di mercato (calcolato in quota e diritto al netto degli "
                "aggiustamenti): €. 43.654,20 Spese di regolarizzazione delle "
                "difformità (vedi cap.8): €. 5.250,00 Valore di Mercato "
                "dell'immobile nello stato di fatto e di diritto in cui si "
                "trova: €. 38.404,20"
            ),
        },
        {
            "page_number": 2,
            "text": (
                "Spese di cancellazione delle trascrizioni ed iscrizioni a "
                "carico dell'acquirente: €. 294,00 Arrotondamento del valore "
                "finale: €. 0,00 Valore di vendita giudiziaria dell'immobile al "
                "netto delle decurtazioni nello stato di fatto e di diritto in "
                "cui si trova:€. 38.110,20"
            ),
        },
    ]
    signals = doc_signals.extract_money_signals(pages)
    by_amount = {s["amount"]: s for s in signals}
    assert by_amount[43654.2]["role"] == doc_signals.ROLE_MARKET_VALUE
    assert by_amount[5250.0]["role"] == doc_signals.ROLE_REGULARIZATION_COST
    # "Valore di Mercato ... nello stato di fatto" is the state-of-fact value.
    assert by_amount[38404.2]["role"] == doc_signals.ROLE_STATE_OF_FACT_VALUE
    assert by_amount[294.0]["role"] == doc_signals.ROLE_BUYER_SIDE_COST
    # "Valore di vendita giudiziaria ... nello stato di fatto" is judicial sale.
    assert by_amount[38110.2]["role"] == doc_signals.ROLE_JUDICIAL_SALE_VALUE


def test_label_after_amount_classified_via_fallback():
    pages = [{
        "page_number": 1,
        "text": "L'importo è pari a EUR 75.000,00 prezzo base come da ordinanza di vendita.",
    }]
    signals = doc_signals.extract_money_signals(pages)
    assert signals[0]["kind"] == "prezzo_base"
    assert signals[0]["role"] == doc_signals.ROLE_AUCTION_BASE_PRICE


def test_next_section_heading_never_labels_previous_amount():
    # The table's last amount is followed by the NEXT section's heading: it
    # must stay generic, never become "valore di vendita giudiziaria".
    pages = [{
        "page_number": 1,
        "text": (
            "RIEPILOGO VALUTAZIONE: appartamento 46,94 0,00 43.654,20 € "
            "VALORE DI VENDITA GIUDIZIARIA (FJV): Espropriazioni immobiliari"
        ),
    }]
    signals = doc_signals.extract_money_signals(pages)
    big = [s for s in signals if s["amount"] == 43654.20]
    assert big and big[0]["kind"] == "importo_generico"


def test_explicit_value_phrase_beats_comparable_wording():
    # "valore di mercato ... con procedimento comparativo / quotazioni OMI"
    # names the ROLE: it is the market value, not a comparable listing.
    pages = [{
        "page_number": 1,
        "text": (
            "Valore di mercato determinato con procedimento comparativo sulla "
            "base delle quotazioni OMI: €. 100.000,00"
        ),
    }]
    signals = doc_signals.extract_money_signals(pages)
    assert signals[0]["kind"] == "valore_mercato"
    assert signals[0]["severity"] == "critical"


# ---------------------------------------------------------------------------
# 5) Swapped money roles must FAIL the quality gate
# ---------------------------------------------------------------------------
def test_swapped_money_roles_fail_the_gate():
    raw = make_worksheet()
    # The document says market=100.000 and state-of-fact=95.000 (page 2). The
    # (buggy) worksheet swaps them: the amounts still all exist in the report,
    # so an amount-only audit would pass — the role-aware audit must not.
    raw["money"]["market_value"] = 95000.0
    raw["money"]["current_state_value"] = 100000.0
    ws, vr, lr, contract, report = _build(
        GENERIC_PERIZIA_PAGES, raw, validator_report=VALIDATED_REPORT
    )
    gate = _gate(GENERIC_PERIZIA_PAGES, ws, contract, report, vr, lr)
    assert gate["gate_status"] == quality_gate.GATE_FAIL
    codes = {b["code"] for b in gate["quality_report"]["blocking_issues"]}
    assert "MONEY_ROLE_MISMATCH" in codes
    contradicted = [
        f for f in gate["coverage_audit"]["fact_coverage"]
        if f.get("role_conflict") and f["match_status"] == "contradicted"
    ]
    assert contradicted, "swapped roles must be recorded as contradictions"


def test_correct_money_roles_pass_the_gate():
    ws, vr, lr, contract, report = _build(GENERIC_PERIZIA_PAGES, make_worksheet())
    gate = _gate(GENERIC_PERIZIA_PAGES, ws, contract, report, vr, lr)
    assert gate["gate_status"] != quality_gate.GATE_FAIL
    codes = {b["code"] for b in gate["quality_report"]["blocking_issues"]}
    assert "MONEY_ROLE_MISMATCH" not in codes


# ---------------------------------------------------------------------------
# 1) + 2) Bene/property structure
# ---------------------------------------------------------------------------
def test_bene_count_never_zero_for_real_single_property():
    ws, vr, lr, contract, report = _build(GENERIC_PERIZIA_PAGES, make_worksheet())
    assert (contract["lot_summary"].get("bene_count") or 0) == 0  # detection truth
    structure = report["lot_structure"]
    assert structure["bene_count"] == 1
    assert structure["detected_bene_count"] == 0
    beni = report["beni_sections"]
    assert len(beni) == 1
    assert beni[0]["is_main_property"] is True
    assert "Bene principale" in beni[0]["title"]
    assert "Appartamento" in beni[0]["title"]


def test_accessory_renders_under_main_bene():
    pages = copy.deepcopy(GENERIC_PERIZIA_PAGES)
    pages[0]["text"] += (
        " Il compendio comprende una soffitta pertinenziale al piano sottotetto "
        "e una cantina al piano interrato."
    )
    ws, vr, lr, contract, report = _build(pages, make_worksheet())
    beni = report["beni_sections"]
    assert len(beni) == 1
    labels = {a["label"] for a in beni[0].get("accessories") or []}
    assert "soffitta" in labels
    assert "cantina" in labels
    # Accessories never become extra fake beni.
    assert report["lot_structure"]["bene_count"] == 1


def test_explicit_multi_bene_structure_untouched():
    from .sample_perizia import make_multibene_single_lot_worksheet

    ws, vr, lr, contract, report = _build(
        GENERIC_PERIZIA_PAGES, make_multibene_single_lot_worksheet()
    )
    if report["lot_structure"]["bene_count"] >= 2:
        assert len(report["beni_sections"]) == report["lot_structure"]["bene_count"]
        assert not any(b.get("is_main_property") for b in report["beni_sections"])


# ---------------------------------------------------------------------------
# 3) Buyer-side cost already included in the valuation chain
# ---------------------------------------------------------------------------
def test_buyer_side_cost_in_valuation_chain_rendered_consistently():
    raw = make_worksheet()
    raw["money"]["buyer_side_costs"] = [
        {
            "label": "Spese di cancellazione formalità a carico acquirente",
            "amount": 300.0,
            "evidence_pages": [2],
        }
    ]
    ws, vr, lr, contract, report = _build(
        GENERIC_PERIZIA_PAGES, raw, validator_report=VALIDATED_REPORT
    )
    buyer_rows = report["money_sections"]["buyer_side_costs"]
    assert len(buyer_rows) == 1
    row = buyer_rows[0]
    assert row["amount"] == 300.0
    assert row["included_in_valuation"] is True
    assert "già considerato" in row["notes"].lower()
    # Still exactly once in the chain (never double counted).
    chain_300 = [
        r for r in report["money_sections"]["valuation_chain"]
        if r.get("amount") == 300.0
    ]
    assert len(chain_300) == 1
    gate = _gate(GENERIC_PERIZIA_PAGES, ws, contract, report, vr, lr)
    codes = {b["code"] for b in gate["quality_report"]["blocking_issues"]}
    assert "SECTION_CONTRADICTION" not in codes
    assert "PROCEDURE_FORMALITY_AS_BUYER_DEBT" not in codes


# ---------------------------------------------------------------------------
# 4) Formality sections are never contradictorily empty
# ---------------------------------------------------------------------------
def test_procedure_cancelled_section_not_empty_when_formalities_exist():
    raw = make_worksheet()
    raw["money"]["procedure_cancelled_costs"] = []
    raw["money"]["cancellation_costs"] = None
    ws, vr, lr, contract, report = _build(
        GENERIC_PERIZIA_PAGES, raw, validator_report=VALIDATED_REPORT
    )
    assert report["formalities_section"], "formalities must render"
    rows = report["money_sections"]["procedure_cancelled_formalities"]
    assert rows, "money section must reference the cancelled formalities"
    labels = " ".join(r["label"] for r in rows).lower()
    assert "ipoteca" in labels
    assert "pignoramento" in labels
    # Reference rows are facts, not costs: no amounts are invented.
    assert all(r.get("amount") is None for r in rows)
    assert all(r.get("informational") for r in rows)


def test_formalities_section_dedups_identical_rows():
    raw = make_worksheet()
    raw["legal_formalities"].append(dict(raw["legal_formalities"][0]))
    ws, vr, lr, contract, report = _build(
        GENERIC_PERIZIA_PAGES, raw, validator_report=VALIDATED_REPORT
    )
    ipoteca_rows = [
        f for f in report["formalities_section"] if f["type"] == "ipoteca"
    ]
    assert len(ipoteca_rows) == 1
    assert ipoteca_rows[0]["type_label"] == "Ipoteca"


# ---------------------------------------------------------------------------
# 6) Market comparatives grouped separately from uncertain money
# ---------------------------------------------------------------------------
def test_comparatives_and_context_values_grouped_separately():
    raw = make_worksheet()
    raw["money"]["uncertain_money"] = [
        {"label": "Comparativo 1 - prezzo annuncio PORTALE Via Uno", "amount": 49000.0,
         "reason": "confronto di mercato", "evidence_pages": [2]},
        {"label": "Valore medio OMI di zona", "amount": 870.0,
         "reason": "confronto di mercato", "evidence_pages": [2]},
        {"label": "Rendita catastale", "amount": 472.56,
         "reason": "dato catastale", "evidence_pages": [1]},
        {"label": "Canone di locazione dichiarato nel contratto di affitto",
         "amount": 3720.0, "reason": "dato occupazione", "evidence_pages": [1]},
        {"label": "Importo indicato senza contesto", "amount": 1234.0,
         "reason": "ruolo non chiaro", "evidence_pages": [2]},
    ]
    ws, vr, lr, contract, report = _build(
        GENERIC_PERIZIA_PAGES, raw, validator_report=VALIDATED_REPORT
    )
    ms = report["money_sections"]
    comp_labels = {r["label"] for r in ms["market_comparatives"]}
    assert comp_labels == {
        "Comparativo 1 - prezzo annuncio PORTALE Via Uno",
        "Valore medio OMI di zona",
    }
    ctx_labels = {r["label"] for r in ms["context_values"]}
    assert "Rendita catastale" in ctx_labels
    assert "Canone di locazione dichiarato nel contratto di affitto" in ctx_labels
    unc_labels = {r["label"] for r in ms["uncertain_money"]}
    assert unc_labels == {"Importo indicato senza contesto"}
    # Comparatives/context are stated as context, never as confirmed costs.
    assert all(r["status"] == "comparativo" for r in ms["market_comparatives"])
    assert all(r["status"] == "contesto" for r in ms["context_values"])
    # Only the genuinely unclear amount is flagged for manual review.
    money_flags = [
        f for f in report["manual_review_flags"] if f["kind"] == "uncertain_money"
    ]
    assert len(money_flags) == 1
    assert "senza contesto" in money_flags[0]["detail"].lower()


# ---------------------------------------------------------------------------
# 7) Customer evidence index: human topics + verbatim excerpts, no raw keys
# ---------------------------------------------------------------------------
_RAW_KEY_RE = re.compile(
    r"(technical_compliance|risk_classification|legal_formalities|"
    r"missing_or_uncertain|uncertain_money)\[\d+\]"
)


def _normalize_ws(text):
    return re.sub(r"\s+", " ", str(text or "")).strip()


def test_customer_evidence_hides_raw_keys_and_quotes_verbatim():
    ws, vr, lr, contract, report = _build(GENERIC_PERIZIA_PAGES, make_worksheet())
    customer = report["customer_evidence_index"]
    assert customer, "customer evidence index must be populated"
    dumped = json.dumps(customer, ensure_ascii=False)
    assert not _RAW_KEY_RE.search(dumped), "raw internal keys leaked to customers"

    page_texts = {
        p["page_number"]: _normalize_ws(p["text"]).lower() for p in GENERIC_PERIZIA_PAGES
    }
    covered = [e for e in customer if e["coverage_status"] == "covered"]
    assert covered, "at least some entries must carry verbatim excerpts"
    for entry in covered:
        excerpt = _normalize_ws(entry["perizia_excerpt"]).lower()
        assert excerpt in page_texts[entry["page"]], entry

    # Raw keys still exist for admin debug, in the admin view only.
    admin = report["admin_evidence_index"]
    assert admin
    admin_dump = json.dumps(admin, ensure_ascii=False)
    assert "technical_compliance[" in admin_dump
    assert all(e["artifact_source"] == "verified_report_contract.json" for e in admin)


def test_missing_excerpt_marked_and_warned():
    ws, vr, lr, contract, _ = _build(GENERIC_PERIZIA_PAGES, make_worksheet())
    # Render WITHOUT page text: no excerpt can be safely found anywhere.
    report = customer_report.render_success_report(contract, pages=None)
    customer = report["customer_evidence_index"]
    assert customer
    assert all(e["coverage_status"] == "excerpt_missing" for e in customer)
    assert all(
        e["perizia_excerpt"] is None
        and "Estratto non disponibile automaticamente" in e["note"]
        for e in customer
    )
    gate = _gate(GENERIC_PERIZIA_PAGES, ws, contract, report, vr, lr)
    warn_codes = {w["code"] for w in gate["quality_report"]["warnings"]}
    assert "EVIDENCE_EXCERPT_MISSING" in warn_codes


def test_invented_excerpt_blocks_the_gate():
    ws, vr, lr, contract, report = _build(GENERIC_PERIZIA_PAGES, make_worksheet())
    victim = next(
        e for e in report["customer_evidence_index"] if e["coverage_status"] == "covered"
    )
    victim["perizia_excerpt"] = "Frase inventata che non esiste nel documento."
    gate = _gate(GENERIC_PERIZIA_PAGES, ws, contract, report, vr, lr)
    codes = {b["code"] for b in gate["quality_report"]["blocking_issues"]}
    assert "EXCERPT_NOT_VERBATIM" in codes
    assert gate["gate_status"] == quality_gate.GATE_FAIL
