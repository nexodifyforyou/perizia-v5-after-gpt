"""Tests for the deterministic contract builder (generic money + risk normalization)."""

from correctness_v2 import analyst, contract, validator

from .sample_perizia import GENERIC_PERIZIA_PAGES, make_worksheet


def _normalized(raw):
    return analyst.normalize_worksheet(raw)


def _build(raw, pages=GENERIC_PERIZIA_PAGES):
    ws = _normalized(raw)
    report = validator.validate_worksheet(ws, pages)
    con = contract.build_contract(
        worksheet=ws,
        validator_report=report,
        analysis_id="a1",
        job_id="j1",
        source_pdf_quality_status="PDF_QUALITY_OK",
    )
    return con, report


def _amounts(rows):
    return [r["amount"] for r in rows]


def _labels(rows):
    return [r["label"] for r in rows]


def test_duplicate_money_rows_normalized_away():
    # Same 5000 appears as a deduction breakdown line AND the regularization scalar;
    # same 300 appears as deduction, cancellation scalar and a buyer-side line.
    raw = make_worksheet()
    raw["money"]["deductions"] = [
        {"label": "Spese di regolarizzazione", "amount": 5000.0, "evidence_pages": []},
        {"label": "Spese di cancellazione", "amount": 300.0, "evidence_pages": []},
    ]
    raw["money"]["buyer_side_costs"] = [
        {"label": "Spese di cancellazione", "amount": 300.0, "evidence_pages": [2]},
    ]
    # Isolate the dedup behavior from the unrelated procedure-cancelled formality.
    raw["money"]["procedure_cancelled_costs"] = []
    con, _ = _build(raw)
    table = con["money_table"]
    # 5000 appears exactly once; 300 appears exactly once.
    assert _amounts(table).count(5000.0) == 1
    assert _amounts(table).count(300.0) == 1
    # The merged 300 row records it is also buyer-side.
    row_300 = next(r for r in table if r["amount"] == 300.0)
    assert "buyer_side" in row_300["roles"]
    assert row_300.get("notes")  # explicit "anche a carico acquirente" note
    # No money row is left without evidence.
    for r in table:
        if r["kind"] in {"deduction", "buyer_side", "value"} and r["amount"]:
            assert r["evidence_pages"], r


def test_judicial_sale_value_always_visible():
    con, _ = _build(make_worksheet())
    # The judicial sale value is shown exactly once, in the valuation chain.
    chain_labels = _labels(con["valuation_chain"])
    assert chain_labels.count("Valore di vendita giudiziaria") == 1
    table_labels = _labels(con["money_table"])
    assert table_labels.count("Valore di vendita giudiziaria") == 1


def test_judicial_sale_value_not_relabeled_as_prezzo_base_without_explicit_text():
    # A prezzo base candidate that merely echoes the sale value, with no explicit
    # 'prezzo base' text on its cited page, must NOT be labeled prezzo base. The
    # amount is not lost: it remains visible as the judicial sale value.
    raw = make_worksheet()
    raw["money"]["auction_terms"] = {
        "prezzo_base_asta": raw["money"]["sale_value"],  # 94700, echoes sale value
        "offerta_minima": None,
        "rialzo_minimo": None,
        "cauzione": None,
        "evidence_pages": [2],  # page 2 has no 'prezzo base' wording
    }
    con, _ = _build(raw)
    assert "Prezzo base d'asta" not in _labels(con["money_table"])
    assert _labels(con["valuation_chain"]).count("Valore di vendita giudiziaria") == 1


def test_prezzo_base_appears_only_with_explicit_text():
    # Sample auction terms cite page 1 which explicitly says "Prezzo base".
    con, _ = _build(make_worksheet())
    auction_labels = _labels(con["auction_terms"])
    assert "Prezzo base d'asta" in auction_labels
    # It lives in auction_terms, never inside the valuation chain.
    assert "Prezzo base d'asta" not in _labels(con["valuation_chain"])


def test_prezzo_base_and_sale_value_shown_separately():
    # prezzo base 75000 (explicit), offerta minima 56250, sale value 94700 -> all
    # three appear separately; none is merged into another.
    con, _ = _build(make_worksheet())
    auction_amounts = _amounts(con["auction_terms"])
    chain_amounts = _amounts(con["valuation_chain"])
    assert 75000.0 in auction_amounts          # prezzo base
    assert 56250.0 in auction_amounts          # offerta minima
    assert 94700.0 in chain_amounts            # judicial sale value
    # Distinct figures, not collapsed together.
    assert 75000.0 not in chain_amounts
    assert 94700.0 not in auction_amounts


def test_significant_unclear_amount_preserved_in_uncertain_money():
    # A base/asta candidate with no explicit text AND not equal to any shown value
    # is significant but unclear -> kept under uncertain_money, never dropped.
    raw = make_worksheet()
    raw["money"]["auction_terms"] = {
        "prezzo_base_asta": 61234.0,  # distinct from market/current/sale
        "offerta_minima": None,
        "rialzo_minimo": None,
        "cauzione": None,
        "evidence_pages": [2],  # no 'prezzo base' wording here
    }
    con, _ = _build(raw)
    assert con["needs_manual_review_money"] is True
    unc_amounts = _amounts(con["uncertain_money"])
    assert 61234.0 in unc_amounts
    # It is preserved in the flat table and carries its evidence.
    assert 61234.0 in _amounts(con["money_table"])
    row = next(r for r in con["uncertain_money"] if r["amount"] == 61234.0)
    assert row["manual_review"] is True
    assert row["evidence_pages"] == [2]
    # Not mislabeled as prezzo base.
    assert "Prezzo base d'asta" not in _labels(con["money_table"])


def test_zero_amount_buyer_action_removed():
    raw = make_worksheet()
    raw["money"]["buyer_side_costs"] = [
        {"label": "Oneri notarili", "amount": 0.0, "evidence_pages": [2]},
        {"label": "Spese vive", "amount": 150.0, "evidence_pages": [2]},
    ]
    con, _ = _build(raw)
    details = [b["detail"] for b in con["buyer_action_checklist"]]
    assert not any("Oneri notarili" in d for d in details)
    assert any("Spese vive" in d for d in details)
    # The zero-amount cost is also gone from the money table / buyer-side section.
    assert 0.0 not in _amounts(con["money_table"])


def test_duplicated_risk_cards_normalized():
    # A generic risk card and a detailed technical_compliance card for the SAME
    # area must collapse to the single detailed card; gas and elettrico stay split.
    raw = make_worksheet()
    raw["technical_compliance"] = [
        {
            "area": "conformità edilizia",
            "classification": "regularizable",
            "blocks_saleability": False,
            "cost": 2000.0,
            "timing": "6 mesi",
            "notes": "Difformità edilizia regolarizzabile.",
            "evidence_pages": [2],
        },
        {
            "area": "impianto gas domestico",
            "classification": "non_conforming",
            "blocks_saleability": False,
            "cost": None,
            "timing": None,
            "notes": "Impianto gas senza certificazione.",
            "evidence_pages": [2],
        },
        {
            "area": "impianto elettrico",
            "classification": "non_conforming",
            "blocks_saleability": False,
            "cost": None,
            "timing": None,
            "notes": "Impianto elettrico senza certificazione.",
            "evidence_pages": [2],
        },
    ]
    raw["risk_classification"] = [
        {"area": "difformità edilizie", "severity": "media", "summary": "x", "regularizable": True, "evidence_pages": [2]},
        {"area": "impianto gas", "severity": "media", "summary": "y", "regularizable": True, "evidence_pages": [2]},
        {"area": "impianto elettrico", "severity": "media", "summary": "z", "regularizable": True, "evidence_pages": [2]},
        {"area": "occupazione", "severity": "media", "summary": "occupato", "regularizable": False, "evidence_pages": [2]},
    ]
    con, _ = _build(raw)
    cards = con["risk_cards"]
    # edilizia, impianto gas, impianto elettrico each appear once (detailed wins).
    sources_by_area = {}
    for c in cards:
        sources_by_area.setdefault(contract._area_token(c["area"]), []).append(c["source"])
    assert sources_by_area["edilizia"] == ["technical_compliance"]
    assert sources_by_area["impianto_gas"] == ["technical_compliance"]
    assert sources_by_area["impianto_elettrico"] == ["technical_compliance"]
    # gas and elettrico are NOT collapsed into one another.
    assert "impianto_gas" in sources_by_area and "impianto_elettrico" in sources_by_area
    # occupazione has no detailed counterpart -> generic card survives.
    assert sources_by_area["occupazione"] == ["risk_classification"]


def test_valuation_chain_is_ordered_and_separated():
    con, _ = _build(make_worksheet())
    # Five distinct money concepts are exposed separately.
    assert "valuation_chain" in con
    assert "auction_terms" in con
    assert "buyer_side_costs" in con
    assert "procedure_cancelled_formalities" in con
    assert "uncertain_money" in con
    chain_labels = _labels(con["valuation_chain"])
    assert chain_labels[0] == "Valore di mercato"
    assert chain_labels[-1] == "Valore di vendita giudiziaria"
    # Procedure-cancelled formalities are not mixed into the valuation chain.
    assert all("cancellate a cura" not in l.lower() for l in chain_labels)
