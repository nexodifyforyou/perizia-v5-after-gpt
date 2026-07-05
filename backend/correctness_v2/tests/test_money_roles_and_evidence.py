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
    coverage_audit,
    doc_signals,
    lots as lots_mod,
    customer_report,
    quality_gate,
    quality_report as quality_report_mod,
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


# ---------------------------------------------------------------------------
# Review finding 1: role-compatibility tiers (covered / partial / misleading)
# ---------------------------------------------------------------------------
def _pool_with(section_rows):
    """ReportPool from a minimal synthetic customer report."""
    report = {"money_sections": section_rows}
    return coverage_audit.build_report_pool(report)


def _money_sig(kind, amount, severity, page=5):
    return {
        "signal_type": "money", "signal_id": f"p{page}:{kind}:0", "page": page,
        "category": "money", "kind": kind, "severity": severity,
        "label": kind, "amount": amount, "role": doc_signals.role_for_kind(kind),
        "snippet": "",
    }


def test_background_amount_shown_as_core_value_contradicts():
    # Doc says 472,56 is the RENDITA; the report shows it ONLY as a confirmed
    # market value -> misleading -> contradiction (warning-level severity).
    pool = _pool_with({"valuation_chain": [
        {"label": "Valore di mercato", "amount": 472.56, "kind": "value"},
    ]})
    fact = coverage_audit._evaluate_money_signal(
        _money_sig("rendita", 472.56, "important"), pool, {}
    )
    assert fact["match_status"] == coverage_audit.CONTRADICTED
    assert fact["role_conflict"] is True


def test_formality_capital_shown_as_buyer_cost_contradicts():
    pool = _pool_with({"buyer_side_costs": [
        {"label": "Costo a carico acquirente", "amount": 150000.0},
    ]})
    fact = coverage_audit._evaluate_money_signal(
        _money_sig("formalita_capitale", 150000.0, "background"), pool, {}
    )
    assert fact["match_status"] == coverage_audit.CONTRADICTED


def test_comparable_shown_as_confirmed_market_value_contradicts():
    pool = _pool_with({"valuation_chain": [
        {"label": "Valore di mercato", "amount": 49000.0, "kind": "value"},
    ]})
    fact = coverage_audit._evaluate_money_signal(
        _money_sig("comparativo", 49000.0, "background"), pool, {}
    )
    assert fact["match_status"] == coverage_audit.CONTRADICTED


def test_background_vs_background_conflict_is_partial_not_blocking():
    # Doc classifies 3720 near mortgage words (formalita_capitale); the report
    # shows it as rent context -> same economic background fact, safe bucket.
    pool = _pool_with({"context_values": [
        {"label": "Canone di locazione dichiarato", "amount": 3720.0},
    ]})
    fact = coverage_audit._evaluate_money_signal(
        _money_sig("formalita_capitale", 3720.0, "background"), pool, {}
    )
    assert fact["match_status"] == coverage_audit.PARTIAL
    assert fact["action"] == coverage_audit.ACTION_BACKGROUND
    assert fact["role_conflict"] is True


def test_matching_role_is_covered():
    pool = _pool_with({"context_values": [
        {"label": "Rendita catastale", "amount": 472.56},
    ]})
    fact = coverage_audit._evaluate_money_signal(
        _money_sig("rendita", 472.56, "important"), pool, {}
    )
    assert fact["match_status"] == coverage_audit.MATCH


def test_noncritical_contradiction_warns_not_blocks():
    raw = make_worksheet()
    ws, vr, lr, contract, report = _build(
        GENERIC_PERIZIA_PAGES, raw, validator_report=VALIDATED_REPORT
    )
    # Tamper: present the cadastral income as a confirmed extra market value.
    report["money_sections"]["valuation_chain"].append(
        {"label": "Valore di mercato aggiuntivo", "amount": 999.77,
         "amount_display": "€ 999,77", "kind": "value", "evidence_pages": [2]}
    )
    pages = copy.deepcopy(GENERIC_PERIZIA_PAGES)
    pages[1]["text"] += " Rendita catastale: Euro 999,77."
    gate = _gate(pages, ws, contract, report, vr, lr)
    warn_codes = {w["code"] for w in gate["quality_report"]["warnings"]}
    assert "MONEY_ROLE_CONFLICT" in warn_codes
    # Important-severity conflict warns; it never silently counts as covered.
    assert gate["gate_status"] != quality_gate.GATE_PASS


# ---------------------------------------------------------------------------
# Review finding 2: cancellation costs never suppress formality references
# ---------------------------------------------------------------------------
def test_cancellation_cost_rows_and_formality_references_both_render():
    raw = make_worksheet()  # has procedure_cancelled_costs row + 2 formalities
    ws, vr, lr, contract, report = _build(
        GENERIC_PERIZIA_PAGES, raw, validator_report=VALIDATED_REPORT
    )
    rows = report["money_sections"]["procedure_cancelled_formalities"]
    cost_rows = [r for r in rows if r.get("amount") is not None]
    ref_rows = [r for r in rows if r.get("kind") == "procedure_cancelled_reference"]
    assert cost_rows, "the real cancellation cost row must stay visible"
    ref_labels = " ".join(r["label"] for r in ref_rows).lower()
    assert "ipoteca" in ref_labels and "pignoramento" in ref_labels
    # Buyer-side and procedure-cancelled stay separate sections.
    buyer_labels = " ".join(
        str(r.get("label")) for r in report["money_sections"]["buyer_side_costs"]
    ).lower()
    assert "ipoteca" not in buyer_labels and "pignoramento" not in buyer_labels
    gate = _gate(GENERIC_PERIZIA_PAGES, ws, contract, report, vr, lr)
    codes = {b["code"] for b in gate["quality_report"]["blocking_issues"]}
    assert "PROCEDURE_FORMALITY_AS_BUYER_DEBT" not in codes


# ---------------------------------------------------------------------------
# Review finding 3: excerpts are topic-aware — wrong-topic verbatim rejected
# ---------------------------------------------------------------------------
def test_wrong_topic_sentence_rejected_even_if_verbatim():
    text = customer_report._normalize_ws(
        "La conformità dell'impianto idraulico è stata verificata con esito "
        "positivo durante il sopralluogo."
    )
    excerpt = customer_report._find_verbatim_excerpt(
        text, needles=["impianto elettrico conformità"]
    )
    assert excerpt is None  # 2/3 topic words is not topical coverage


def test_ambiguous_amount_resolved_by_topic_words():
    text = customer_report._normalize_ws(
        "Cauzione richiesta per l'offerta: Euro 500,00. Il canone mensile "
        "dichiarato nel contratto: Euro 500,00."
    )
    excerpt = customer_report._find_verbatim_excerpt(
        text, amount=500.0, needles=["Canone di locazione dichiarato"],
        role=doc_signals.ROLE_RENT,
    )
    assert excerpt is not None
    assert "canone" in excerpt.lower()
    assert "cauzione" not in excerpt.lower()


def test_ambiguous_amount_without_topic_anchor_is_not_quoted():
    text = customer_report._normalize_ws(
        "Prima voce elencata: Euro 500,00. Seconda voce elencata: Euro 500,00."
    )
    excerpt = customer_report._find_verbatim_excerpt(
        text, amount=500.0, needles=["Dato senza riscontro"],
    )
    assert excerpt is None


def test_unique_amount_is_its_own_anchor():
    text = customer_report._normalize_ws(
        "Spese di cancellazione a carico dell'acquirente: Euro 294,00."
    )
    excerpt = customer_report._find_verbatim_excerpt(
        text, amount=294.0, needles=["Voce con parole non presenti"],
    )
    assert excerpt is not None and "294,00" in excerpt


# ---------------------------------------------------------------------------
# Review finding 5: actionability not over-penalized for grouped background
# ---------------------------------------------------------------------------
def _scorecard_for(report, coverage):
    quality = {
        "analysis_id": "a", "job_id": "j", "blocking_issues": [], "warnings": [],
        "scores": {"coverage_completeness": 100, "evidence_traceability": 100},
        "customer_satisfaction_risks": [],
    }
    return quality_report_mod.build_customer_satisfaction_scorecard(
        quality_report=quality, coverage_audit=coverage, customer_report=report,
    )


def test_actionability_not_penalized_for_comparatives_only():
    report = {
        "report_status": "REPORT_READY",
        "buyer_checklist": [{"action": "x", "detail": "y"}],
        "manual_review_flags": [],
        "evidence_index": [],
        "money_sections": {
            "valuation_chain": [{"label": "Valore di mercato", "amount": 1.0}],
            "buyer_side_costs": [],
            "market_comparatives": [{"label": "Comparativo 1", "amount": 2.0}],
            "context_values": [],
            "uncertain_money": [],
        },
    }
    coverage = {"important_warnings": [{"fact_id": "x"}], "coverage_status": "WARNING"}
    scorecard = _scorecard_for(report, coverage)
    assert scorecard["scores"]["actionability"] == 100


def test_actionability_penalized_for_unflagged_uncertain_money():
    report = {
        "report_status": "REPORT_READY",
        "buyer_checklist": [{"action": "x", "detail": "y"}],
        "manual_review_flags": [],
        "evidence_index": [],
        "money_sections": {
            "valuation_chain": [{"label": "Valore di mercato", "amount": 1.0}],
            "buyer_side_costs": [],
            "market_comparatives": [],
            "context_values": [],
            "uncertain_money": [{"label": "Importo ignoto", "amount": 3.0}],
        },
    }
    scorecard = _scorecard_for(report, {"important_warnings": [], "coverage_status": "PASS"})
    assert scorecard["scores"]["actionability"] == 85


# ---------------------------------------------------------------------------
# Review finding 6: indexed amount lookup equals the linear definition
# ---------------------------------------------------------------------------
def test_pool_index_matches_linear_scan_with_tolerance_edges():
    pool = coverage_audit.ReportPool()
    values = [100.0, 100.005, 100.02, 99.995, 43654.20, 294.0, 0.5]
    for i, v in enumerate(values):
        pool.add_amount(f"sec{i}", v)
    for probe in values + [100.01, 100.011, 100.03, 293.99, 43654.21]:
        expected = sorted({
            sec for val, sec, _r in pool.amounts
            if coverage_audit._approx_equal(val, probe)
        })
        assert pool.find_amount(probe) == expected, probe
    # Entries added after a lookup are picked up (index rebuild).
    pool.add_amount("late", 100.0)
    assert "late" in pool.find_amount(100.0)


# ---------------------------------------------------------------------------
# Review finding 7: one shared role taxonomy, no drift possible
# ---------------------------------------------------------------------------
def test_role_taxonomy_is_shared_and_complete():
    assert customer_report._COMPARATIVE_KINDS is doc_signals.COMPARATIVE_LABEL_KINDS
    assert customer_report._CONTEXT_KINDS is doc_signals.CONTEXT_LABEL_KINDS
    kinds = {k for k, _s, _l, _p in doc_signals._MONEY_KINDS} | {"importo_generico"}
    for kind in kinds:
        assert kind in doc_signals.ROLE_BY_KIND, kind
    all_roles = set(doc_signals.ROLE_BY_KIND.values())
    assert all_roles <= doc_signals.CORE_MONEY_ROLES | doc_signals.BACKGROUND_MONEY_ROLES
    assert not (doc_signals.CORE_MONEY_ROLES & doc_signals.BACKGROUND_MONEY_ROLES)
    for role in all_roles:
        assert role in doc_signals.ROLE_LABELS_IT, role
    # Compatibility is symmetric and reflexive.
    for a in all_roles:
        assert doc_signals.roles_compatible(a, a)
        for b in all_roles:
            assert doc_signals.roles_compatible(a, b) == doc_signals.roles_compatible(b, a)


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


# ---------------------------------------------------------------------------
# Review finding 1: role compatibility tiers (covered / partial / warn / fail)
# ---------------------------------------------------------------------------
def _minimal_report(**money_sections):
    """A minimal customer_report carrying only the given money rows."""
    sections = {
        "valuation_chain": [], "auction_terms": [], "buyer_side_costs": [],
        "procedure_cancelled_formalities": [], "market_comparatives": [],
        "context_values": [], "uncertain_money": [],
    }
    sections.update(money_sections)
    return {
        "report_status": "REPORT_READY",
        "money_sections": sections,
        "manual_review_flags": [],
    }


def _audit(pages, report):
    audit, _page = coverage_audit.build_coverage_audit(
        analysis_id="a", job_id="j", pages=pages, worksheet={},
        contract={}, customer_report=report,
    )
    return audit


def test_core_value_shown_only_as_comparable_fails():
    # Document: explicit market value. Report: same amount ONLY as comparable.
    pages = [{"page_number": 1, "text": "Valore di mercato stimato: €. 100.000,00"}]
    report = _minimal_report(market_comparatives=[
        {"label": "Comparativo di zona", "amount": 100000.0, "evidence_pages": [1]},
    ])
    audit = _audit(pages, report)
    assert audit["coverage_status"] == "FAIL"
    conflict = [o for o in audit["critical_omissions"] if o.get("role_conflict")]
    assert conflict, "core value demoted to comparable must contradict"


def test_background_amount_promoted_to_core_role_warns():
    # Document: rendita catastale (background, important). Report: the same
    # amount ONLY as a confirmed market value -> misleading -> warning tier.
    pages = [{"page_number": 1, "text": "rendita catastale: 999,99 euro"}]
    report = _minimal_report(valuation_chain=[
        {"label": "Valore di mercato", "amount": 999.99, "evidence_pages": [1]},
    ])
    audit = _audit(pages, report)
    conflict = [o for o in audit["important_warnings"] if o.get("role_conflict")]
    assert conflict, "rendita rendered as confirmed value must at least warn"
    assert audit["coverage_status"] in ("WARNING", "FAIL")


def test_background_bucket_difference_stays_partial():
    # Document: capitale di formalità (background). Report: the amount sits in
    # a DIFFERENT background bucket (context row classified as rent). Same
    # economic fact, safe bucket -> PARTIAL, never a blocker or warning.
    pages = [{"page_number": 1, "text": "capitale iscrizione ipotecaria: 3.720,00"}]
    report = _minimal_report(context_values=[
        {"label": "Canone di locazione dichiarato", "amount": 3720.0,
         "evidence_pages": [1]},
    ])
    audit = _audit(pages, report)
    partial = [
        f for f in audit["fact_coverage"]
        if f.get("role_conflict") and f["match_status"] == "partial"
    ]
    assert partial, "background-vs-background must stay a visible PARTIAL"
    # The safe-bucket difference itself never blocks or warns (the synthetic
    # page may trip unrelated TOPIC warnings; role conflicts must not).
    assert audit["coverage_status"] != "FAIL"
    assert not any(
        o.get("role_conflict")
        for o in audit["critical_omissions"] + audit["important_warnings"]
    )


def test_formality_capital_as_buyer_cost_warns():
    # Document: capitale di ipoteca (background). Report: same amount as a
    # BUYER cost -> core role involved -> misleading -> contradiction tier.
    # The buyer row's label must not itself name the formality (that case is
    # already blocked as PROCEDURE_FORMALITY_AS_BUYER_DEBT): a generic label
    # gets its buyer_side_cost role from the SECTION alone.
    pages = [{"page_number": 1, "text": "capitale ipoteca iscritta: 150.000,00"}]
    report = _minimal_report(buyer_side_costs=[
        {"label": "Spesa a carico dell'acquirente", "amount": 150000.0,
         "evidence_pages": [1]},
    ])
    audit = _audit(pages, report)
    conflicts = [
        f for f in audit["fact_coverage"]
        if f.get("role_conflict") and f["match_status"] == "contradicted"
    ]
    assert conflicts, "formality capital shown as buyer cost must contradict"
    assert audit["coverage_status"] in ("WARNING", "FAIL")


def test_rent_amount_in_rent_bucket_is_covered():
    pages = [{"page_number": 1, "text": "canone di locazione mensile: 3.720,00"}]
    report = _minimal_report(context_values=[
        {"label": "Canone di locazione dichiarato", "amount": 3720.0,
         "evidence_pages": [1]},
    ])
    audit = _audit(pages, report)
    assert audit["coverage_status"] == "PASS"
    match = [
        f for f in audit["fact_coverage"]
        if f.get("source") == "page_money" and f["match_status"] == "match"
    ]
    assert match, "same amount + same role must be covered"


# ---------------------------------------------------------------------------
# Review finding 2: cancellation cost never suppresses formality references
# ---------------------------------------------------------------------------
def test_cancellation_cost_and_formalities_render_together():
    # make_worksheet carries BOTH a procedure-cancelled COST row (300) and
    # cancelled ipoteca+pignoramento formalities: all must render.
    ws, vr, lr, contract, report = _build(GENERIC_PERIZIA_PAGES, make_worksheet())
    rows = report["money_sections"]["procedure_cancelled_formalities"]
    labels = " | ".join(_normalize_ws(r["label"]).lower() for r in rows)
    amounts = [r.get("amount") for r in rows]
    assert 300.0 in amounts, "the real cancellation cost row must stay"
    assert "ipoteca: cancellazione a cura della procedura" in labels
    assert "pignoramento: cancellazione a cura della procedura" in labels
    # Reference rows carry no invented amounts.
    refs = [r for r in rows if r.get("kind") == "procedure_cancelled_reference"]
    assert refs and all(r["amount"] is None for r in refs)
    # Buyer-side stays separate and the gate raises no confusion blockers.
    gate = _gate(GENERIC_PERIZIA_PAGES, ws, contract, report, vr, lr)
    codes = {b["code"] for b in gate["quality_report"]["blocking_issues"]}
    assert "PROCEDURE_FORMALITY_AS_BUYER_DEBT" not in codes
    assert gate["gate_status"] != quality_gate.GATE_FAIL


# ---------------------------------------------------------------------------
# Review finding 3: excerpts are topic/role-aware, wrong-topic quotes rejected
# ---------------------------------------------------------------------------
def test_wrong_topic_verbatim_sentence_rejected():
    text = customer_report._normalize_ws(
        "L'impianto idraulico risulta conforme alla normativa vigente. "
        "Il giardino condominiale risulta ben curato e recintato."
    )
    excerpt = customer_report._find_verbatim_excerpt(
        text, needles=["impianto elettrico senza certificazione"],
    )
    assert excerpt is None, "a verbatim but wrong-topic sentence must be rejected"


def test_ambiguous_amount_needs_topic_anchor():
    text = customer_report._normalize_ws(
        "Cauzione da versare: €. 5.000,00. "
        "Costi di regolarizzazione delle difformità: €. 5.000,00."
    )
    reg = customer_report._find_verbatim_excerpt(
        text, amount=5000.0, needles=["Costi di regolarizzazione"],
        role=doc_signals.ROLE_REGULARIZATION_COST,
    )
    assert reg and "regolarizzazione" in reg.lower()
    cau = customer_report._find_verbatim_excerpt(
        text, amount=5000.0, needles=["Cauzione"],
        role=doc_signals.ROLE_AUCTION_DEPOSIT,
    )
    assert cau and "cauzione" in cau.lower()


def test_unique_amount_is_its_own_anchor():
    text = customer_report._normalize_ws(
        "Spese di cancellazione a carico dell'acquirente: €. 294,00."
    )
    excerpt = customer_report._find_verbatim_excerpt(
        text, amount=294.0, needles=["Costi di cancellazione formalità"],
        role=doc_signals.ROLE_BUYER_SIDE_COST,
    )
    assert excerpt and "294,00" in excerpt


# ---------------------------------------------------------------------------
# Review finding 5: actionability not over-penalized for comparatives-only
# ---------------------------------------------------------------------------
def test_actionability_not_penalized_for_comparatives_only():
    raw = make_worksheet()
    raw["money"]["uncertain_money"] = [
        {"label": "Comparativo 1 - prezzo annuncio PORTALE", "amount": 49000.0,
         "reason": "confronto", "evidence_pages": [2]},
        {"label": "Valore medio OMI di zona", "amount": 870.0,
         "reason": "confronto", "evidence_pages": [2]},
        {"label": "Rendita catastale", "amount": 472.56,
         "reason": "contesto", "evidence_pages": [1]},
    ]
    ws, vr, lr, contract, report = _build(
        GENERIC_PERIZIA_PAGES, raw, validator_report=VALIDATED_REPORT
    )
    assert report["money_sections"]["uncertain_money"] == []
    gate = _gate(GENERIC_PERIZIA_PAGES, ws, contract, report, vr, lr)
    scores = gate["scorecard"]["scores"]
    # checklist + costs present, uncertain section empty -> no -15 penalty.
    assert scores["actionability"] == 100, scores


def test_actionability_still_penalizes_unsurfaced_uncertain_money():
    report = {
        "report_status": "REPORT_READY",
        "money_sections": {
            "valuation_chain": [{"label": "Valore", "amount": 1.0}],
            "uncertain_money": [{"label": "Importo ignoto", "amount": 2.0}],
        },
        "buyer_checklist": [{"action": "x"}],
        "manual_review_flags": [],  # nothing surfaces the uncertain amount
    }
    scorecard = quality_report_mod.build_customer_satisfaction_scorecard(
        quality_report={"scores": {}, "blocking_issues": [], "warnings": []},
        coverage_audit={"coverage_status": "PASS"},
        customer_report=report,
    )
    assert scorecard["scores"]["actionability"] <= 85


# ---------------------------------------------------------------------------
# Review finding 6: indexed amount lookup === linear scan semantics
# ---------------------------------------------------------------------------
def test_report_pool_indexed_lookup_matches_linear_scan():
    pool = coverage_audit.ReportPool()
    entries = [
        (100.0, "money_sections.valuation_chain", {doc_signals.ROLE_MARKET_VALUE}),
        (100.005, "key_facts", {doc_signals.ROLE_MARKET_VALUE}),
        (100.02, "surfaces_section", {doc_signals.ROLE_CADASTRAL_INCOME}),
        (250000.0, "money_sections.context_values", {doc_signals.ROLE_RENT}),
        (0.5, "money_sections.uncertain_money", {doc_signals.ROLE_UNCERTAIN_MONEY}),
    ]
    for amount, section, roles in entries:
        pool.add_amount(section, amount, roles)

    def linear(amount, role):
        out = set()
        for val, sec, entry_roles in pool.amounts:
            if coverage_audit._approx_equal(val, amount) and pool._entry_matches_role(
                entry_roles, role
            ):
                out.add(sec)
        return sorted(out)

    probes = [
        (100.0, doc_signals.ROLE_MARKET_VALUE),
        (100.0, doc_signals.ROLE_CADASTRAL_INCOME),
        (100.005, None),
        (100.02, doc_signals.ROLE_CADASTRAL_INCOME),
        (100.03, doc_signals.ROLE_CADASTRAL_INCOME),
        (250000.0, doc_signals.ROLE_RENT),
        (250000.0, doc_signals.ROLE_MARKET_VALUE),
        (0.5, doc_signals.ROLE_JUDICIAL_SALE_VALUE),
        (99.98, doc_signals.ROLE_MARKET_VALUE),
    ]
    for amount, role in probes:
        assert pool.find_amount(amount, role) == linear(amount, role), (amount, role)
    # The tolerance edge (0.011) is honored across cent buckets.
    assert pool.find_amount(100.01, doc_signals.ROLE_MARKET_VALUE)
    # Adding after a lookup transparently rebuilds the index.
    pool.add_amount("auction_terms", 777.0, {doc_signals.ROLE_AUCTION_BASE_PRICE})
    assert pool.find_amount(777.0, doc_signals.ROLE_AUCTION_BASE_PRICE) == ["auction_terms"]


# ---------------------------------------------------------------------------
# Review finding 7: one shared role taxonomy, no drift between modules
# ---------------------------------------------------------------------------
def test_role_taxonomy_is_shared_and_consistent():
    # The renderer's bucketing sets ARE the doc_signals sets (same objects).
    assert customer_report._COMPARATIVE_KINDS is doc_signals.COMPARATIVE_LABEL_KINDS
    assert customer_report._CONTEXT_KINDS is doc_signals.CONTEXT_LABEL_KINDS
    # Every detector kind has a role; every role has a label and a tier.
    kinds = {k for k, _s, _l, _p in doc_signals._MONEY_KINDS} | {"importo_generico"}
    assert kinds <= set(doc_signals.ROLE_BY_KIND)
    roles = set(doc_signals.ROLE_BY_KIND.values())
    assert roles <= set(doc_signals.ROLE_LABELS_IT)
    assert roles <= (doc_signals.CORE_MONEY_ROLES | doc_signals.BACKGROUND_MONEY_ROLES)
    assert not (doc_signals.CORE_MONEY_ROLES & doc_signals.BACKGROUND_MONEY_ROLES)
    # Compatibility rules behave as documented.
    assert doc_signals.roles_compatible(
        doc_signals.ROLE_REGULARIZATION_COST, doc_signals.ROLE_DEPRECIATION
    )
    assert not doc_signals.roles_compatible(
        doc_signals.ROLE_MARKET_VALUE, doc_signals.ROLE_STATE_OF_FACT_VALUE
    )
    assert doc_signals.conflict_is_misleading(
        doc_signals.ROLE_MARKET_VALUE, {doc_signals.ROLE_COMPARABLE_MARKET_VALUE}
    )
    assert not doc_signals.conflict_is_misleading(
        doc_signals.ROLE_RENT, {doc_signals.ROLE_CADASTRAL_INCOME}
    )
    # coverage_audit derives roles through the same shared mapping.
    assert set(coverage_audit._ROW_ROLE_TOKENS.values()) <= roles
