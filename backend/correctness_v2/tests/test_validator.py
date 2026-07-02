"""Tests for the deterministic, generic validator (rule-based, no LLM)."""

from correctness_v2 import analyst, validator

from .sample_perizia import GENERIC_PERIZIA_PAGES, make_worksheet


def _normalized(raw):
    return analyst.normalize_worksheet(raw)


def _codes(report):
    return {v["code"] for v in report["violations"]}


def test_clean_worksheet_validates():
    ws = _normalized(make_worksheet())
    report = validator.validate_worksheet(ws, GENERIC_PERIZIA_PAGES)
    assert report["validation_status"] == validator.STATUS_VALIDATED, report["violations"]


def test_money_chain_accepted_when_consistent():
    # Base worksheet: 100000 - 5000 = 95000; 95000 - 300 = 94700. Both chains hold.
    ws = _normalized(make_worksheet())
    report = validator.validate_worksheet(ws, GENERIC_PERIZIA_PAGES)
    assert "MONEY_CHAIN_INCONSISTENT" not in _codes(report)
    assert "market-regularization=current" in report["checks"]["money_chains_checked"]
    assert "current-cancellation=sale" in report["checks"]["money_chains_checked"]


def test_money_chain_uses_explicit_deductions_when_present():
    # If the source valuation table contains multiple deprezzamenti, the current
    # value must reconcile against their sum, not only against regularization.
    raw = make_worksheet()
    raw["money"]["deductions"] = [
        {"label": "Oneri di regolarizzazione", "amount": 5000.0, "evidence_pages": [2]},
        {"label": "Rischio assunto per mancata garanzia", "amount": 3000.0, "evidence_pages": [2]},
    ]
    raw["money"]["regularization_costs"] = 5000.0
    raw["money"]["current_state_value"] = 92000.0
    raw["money"]["sale_value"] = 91700.0
    ws = _normalized(raw)
    report = validator.validate_worksheet(ws, GENERIC_PERIZIA_PAGES)
    assert report["validation_status"] == validator.STATUS_VALIDATED, report["violations"]
    assert "MONEY_CHAIN_INCONSISTENT" not in _codes(report)
    assert "market-deductions=current" in report["checks"]["money_chains_checked"]


def test_money_chain_accepts_staged_deductions_to_sale_value():
    # Some perizie first discount a completed-works market value to current
    # condition, then apply another sale-market discount to reach the judicial
    # sale/base value. The validator must not force both rows into the
    # market-to-current step.
    raw = make_worksheet()
    raw["money"]["market_value"] = 622970.0
    raw["money"]["deductions"] = [
        {"label": "Deprezzamento per stato al grezzo", "amount": 342634.0, "evidence_pages": [2]},
        {"label": "Deprezzamento rispetto al libero mercato", "amount": 56068.0, "evidence_pages": [2]},
    ]
    raw["money"]["regularization_costs"] = None
    raw["money"]["current_state_value"] = 280336.0
    raw["money"]["cancellation_costs"] = None
    raw["money"]["sale_value"] = 224268.0
    raw["money"]["auction_terms"]["prezzo_base_asta"] = 224268.0
    ws = _normalized(raw)
    report = validator.validate_worksheet(ws, GENERIC_PERIZIA_PAGES)
    assert report["validation_status"] == validator.STATUS_VALIDATED, report["violations"]
    assert "MONEY_CHAIN_INCONSISTENT" not in _codes(report)
    assert "market-deduction-subset=current" in report["checks"]["money_chains_checked"]
    assert "current-remaining-deductions=sale" in report["checks"]["money_chains_checked"]


def test_money_chain_excludes_cancellation_rows_from_current_state_step():
    raw = make_worksheet()
    raw["money"]["market_value"] = 100000.0
    raw["money"]["deductions"] = [
        {"label": "Spese di regolarizzazione", "amount": 5000.0, "evidence_pages": [2]},
        {"label": "Spese di cancellazione delle trascrizioni ed iscrizioni", "amount": 300.0, "evidence_pages": [2]},
    ]
    raw["money"]["regularization_costs"] = 5000.0
    raw["money"]["current_state_value"] = 95000.0
    raw["money"]["cancellation_costs"] = 300.0
    raw["money"]["sale_value"] = 94700.0
    ws = _normalized(raw)
    report = validator.validate_worksheet(ws, GENERIC_PERIZIA_PAGES)
    assert report["validation_status"] == validator.STATUS_VALIDATED, report["violations"]
    assert "MONEY_CHAIN_INCONSISTENT" not in _codes(report)
    assert "market-deductions=current" in report["checks"]["money_chains_checked"]
    assert "current-cancellation=sale" in report["checks"]["money_chains_checked"]


def test_money_chain_rejected_when_inconsistent():
    raw = make_worksheet()
    raw["money"]["sale_value"] = 90000.0  # breaks current - cancellation = sale
    ws = _normalized(raw)
    report = validator.validate_worksheet(ws, GENERIC_PERIZIA_PAGES)
    assert report["validation_status"] == validator.STATUS_FAILED
    assert "MONEY_CHAIN_INCONSISTENT" in _codes(report)


def test_unsupported_evidence_page_rejected():
    raw = make_worksheet()
    raw["technical_compliance"][0]["evidence_pages"] = [99]  # no such page
    ws = _normalized(raw)
    report = validator.validate_worksheet(ws, GENERIC_PERIZIA_PAGES)
    assert report["validation_status"] == validator.STATUS_FAILED
    assert "UNSUPPORTED_EVIDENCE_PAGE" in _codes(report)


def test_missing_evidence_rejected():
    raw = make_worksheet()
    raw["technical_compliance"][0]["evidence_pages"] = []  # claim without evidence
    ws = _normalized(raw)
    report = validator.validate_worksheet(ws, GENERIC_PERIZIA_PAGES)
    assert report["validation_status"] == validator.STATUS_FAILED
    assert "MISSING_EVIDENCE" in _codes(report)


def test_buyer_side_misclassification_of_ipoteca_rejected():
    raw = make_worksheet()
    raw["money"]["buyer_side_costs"] = [
        {"label": "Ipoteca Banca Esempio", "amount": 5000.0, "evidence_pages": [2]}
    ]
    ws = _normalized(raw)
    report = validator.validate_worksheet(ws, GENERIC_PERIZIA_PAGES)
    assert report["validation_status"] == validator.STATUS_FAILED
    assert "BUYER_SIDE_INCLUDES_CANCELLABLE_FORMALITY" in _codes(report)


def test_formality_buyer_burden_contradiction_rejected():
    raw = make_worksheet()
    # ipoteca both cancelled by procedure AND claimed as buyer burden -> contradiction.
    raw["legal_formalities"][0]["buyer_burden"] = True
    ws = _normalized(raw)
    report = validator.validate_worksheet(ws, GENERIC_PERIZIA_PAGES)
    assert report["validation_status"] == validator.STATUS_FAILED
    assert "FORMALITY_BUYER_BURDEN_CONTRADICTION" in _codes(report)


def test_formality_buyer_burden_unsupported_rejected():
    raw = make_worksheet()
    # buyer_burden claimed, not cancelled, but text doesn't say buyer pays it.
    raw["legal_formalities"][0]["cancelled_by_procedure"] = False
    raw["legal_formalities"][0]["buyer_burden"] = True
    raw["legal_formalities"][0]["evidence_pages"] = [1]  # page 1 has no buyer-burden token
    ws = _normalized(raw)
    report = validator.validate_worksheet(ws, GENERIC_PERIZIA_PAGES)
    assert report["validation_status"] == validator.STATUS_FAILED
    assert "FORMALITY_BUYER_BURDEN_UNSUPPORTED" in _codes(report)


def test_urbanistica_grave_contradiction_when_evidence_says_conforme():
    raw = make_worksheet()
    # urbanistica is conforming in compliance; promote it to a grave risk citing
    # page 1, whose text says "urbanisticamente conforme" (no negative marker).
    raw["risk_classification"].append(
        {
            "area": "urbanistica",
            "severity": "grave",
            "summary": "Presunto abuso urbanistico",
            "regularizable": False,
            "evidence_pages": [1],
        }
    )
    ws = _normalized(raw)
    report = validator.validate_worksheet(ws, GENERIC_PERIZIA_PAGES)
    assert report["validation_status"] == validator.STATUS_FAILED
    assert "COMPLIANCE_CONTRADICTION" in _codes(report)


def test_grave_risk_allowed_with_stronger_contradictory_evidence():
    # Same as above but the cited page genuinely contains a non-conformity marker:
    # the grave classification is then supported and must NOT be a contradiction.
    pages = [
        GENERIC_PERIZIA_PAGES[0],
        {
            "page_number": 2,
            "text": (
                GENERIC_PERIZIA_PAGES[1]["text"]
                + " Si rileva inoltre un abuso edilizio non sanabile sull'immobile."
            ),
        },
    ]
    raw = make_worksheet()
    raw["risk_classification"].append(
        {
            "area": "urbanistica",
            "severity": "grave",
            "summary": "Abuso non sanabile",
            "regularizable": False,
            "evidence_pages": [2],  # page 2 now has 'abuso' + 'non sanabil'
        }
    )
    ws = _normalized(raw)
    report = validator.validate_worksheet(ws, pages)
    assert "COMPLIANCE_CONTRADICTION" not in _codes(report)


def test_conforming_area_not_rejected_on_mixed_page():
    # The cited page says urbanistica conforme AND mentions an edilizia difformità.
    # A 'conforming' urbanistica claim must NOT be rejected because of the other
    # area's negative marker on the same page (perizia pages mix areas).
    ws = _normalized(make_worksheet())
    report = validator.validate_worksheet(ws, GENERIC_PERIZIA_PAGES)
    assert "COMPLIANCE_EVIDENCE_CONTRADICTION" not in _codes(report)
    assert "UNSUPPORTED_COMPLIANCE_CLAIM" not in _codes(report)


def test_conforming_claim_without_conformity_language_rejected():
    raw = make_worksheet()
    # Point the urbanistica conforming claim at a page with no conformity wording.
    pages = [
        {"page_number": 1, "text": "Pagina senza alcun giudizio. " * 10},
        GENERIC_PERIZIA_PAGES[1],
    ]
    raw["technical_compliance"][0]["evidence_pages"] = [1]
    ws = _normalized(raw)
    report = validator.validate_worksheet(ws, pages)
    assert report["validation_status"] == validator.STATUS_FAILED
    assert "UNSUPPORTED_COMPLIANCE_CLAIM" in _codes(report)


def test_absent_declaration_of_conformity_is_not_positive_conformity():
    pages = GENERIC_PERIZIA_PAGES + [
        {
            "page_number": 3,
            "text": (
                "Certificazioni energetiche e dichiarazioni di conformità. "
                "Non esiste il certificato energetico dell'immobile / APE. "
                "Non esiste la dichiarazione di conformità dell'impianto elettrico."
            ),
        }
    ]
    raw = make_worksheet()
    raw["technical_compliance"][1] = {
        "area": "Certificazioni energetiche e dichiarazioni di conformità impianti",
        "classification": "non_conforming",
        "blocks_saleability": False,
        "cost": None,
        "timing": None,
        "notes": "Assenti APE e dichiarazioni di conformità degli impianti.",
        "evidence_pages": [3],
    }
    ws = _normalized(raw)
    report = validator.validate_worksheet(ws, pages)
    assert "COMPLIANCE_EVIDENCE_CONTRADICTION" not in _codes(report)


def test_regularizable_marked_blocking_rejected():
    raw = make_worksheet()
    raw["technical_compliance"][1]["blocks_saleability"] = True  # regularizable w/ cost
    ws = _normalized(raw)
    report = validator.validate_worksheet(ws, GENERIC_PERIZIA_PAGES)
    assert report["validation_status"] == validator.STATUS_FAILED
    assert "REGULARIZABLE_MARKED_BLOCKING" in _codes(report)


def _warn_codes(report):
    return {w["code"] for w in report["warnings"]}


def test_money_row_without_evidence_orphan_rejected():
    # A deduction with no evidence and no evidenced row of the same amount is a
    # hard error (cannot be merged into an evidenced row).
    raw = make_worksheet()
    raw["money"]["deductions"] = [
        {"label": "Costo non documentato", "amount": 1234.0, "evidence_pages": []}
    ]
    ws = _normalized(raw)
    report = validator.validate_worksheet(ws, GENERIC_PERIZIA_PAGES)
    assert report["validation_status"] == validator.STATUS_FAILED
    assert "MONEY_ROW_MISSING_EVIDENCE" in _codes(report)


def test_money_row_without_evidence_but_mergeable_warns():
    # A deduction with no evidence whose amount equals an evidenced scalar is only
    # a warning — it will be merged into the evidenced row by the contract builder.
    raw = make_worksheet()
    raw["money"]["deductions"] = [
        {"label": "Costi di regolarizzazione", "amount": 5000.0, "evidence_pages": []}
    ]
    ws = _normalized(raw)
    report = validator.validate_worksheet(ws, GENERIC_PERIZIA_PAGES)
    assert report["validation_status"] == validator.STATUS_VALIDATED
    assert "MONEY_ROW_EVIDENCE_VIA_MERGE" in _warn_codes(report)
    assert "MONEY_ROW_MISSING_EVIDENCE" not in _codes(report)


def test_duplicate_money_row_warns():
    raw = make_worksheet()
    raw["money"]["buyer_side_costs"] = [
        {"label": "Spese di cancellazione", "amount": 300.0, "evidence_pages": [2]},
    ]
    raw["money"]["procedure_cancelled_costs"] = [
        {"label": "Spese di cancellazione", "amount": 300.0, "evidence_pages": [2]},
    ]
    ws = _normalized(raw)
    report = validator.validate_worksheet(ws, GENERIC_PERIZIA_PAGES)
    assert "DUPLICATE_MONEY_ROW" in _warn_codes(report)


def test_zero_amount_buyer_cost_warns():
    raw = make_worksheet()
    raw["money"]["buyer_side_costs"] = [
        {"label": "Oneri notarili", "amount": 0.0, "evidence_pages": [2]},
    ]
    ws = _normalized(raw)
    report = validator.validate_worksheet(ws, GENERIC_PERIZIA_PAGES)
    assert "ZERO_AMOUNT_BUYER_COST" in _warn_codes(report)


def test_base_price_mislabel_warns_without_explicit_text():
    # prezzo base candidate echoes sale_value and the cited page has no
    # 'prezzo base'/'base d'asta' wording -> warn, do not relabel as base price.
    raw = make_worksheet()
    raw["money"]["auction_terms"] = {
        "prezzo_base_asta": raw["money"]["sale_value"],  # 94700 == sale
        "offerta_minima": None,
        "rialzo_minimo": None,
        "cauzione": None,
        "evidence_pages": [2],  # page 2 has no 'prezzo base' wording
    }
    ws = _normalized(raw)
    report = validator.validate_worksheet(ws, GENERIC_PERIZIA_PAGES)
    assert "BASE_PRICE_MISLABELED" in _warn_codes(report)
    assert report["checks"]["money_signals"]["base_price_explicit_text"] is False


def test_base_price_not_flagged_when_distinct_and_supported():
    # Sample prezzo base (75000) differs from sale_value (94700) and page 1
    # explicitly says "Prezzo base" -> no mislabel warning, explicit text detected.
    ws = _normalized(make_worksheet())
    report = validator.validate_worksheet(ws, GENERIC_PERIZIA_PAGES)
    assert "BASE_PRICE_MISLABELED" not in _warn_codes(report)
    assert report["checks"]["money_signals"]["base_price_explicit_text"] is True


def test_uncertain_money_without_evidence_rejected():
    # Page evidence is unavoidable: an uncertain amount with no evidence is a hard
    # error (fail closed), not a silent pass.
    raw = make_worksheet()
    raw["money"]["uncertain_money"] = [
        {"label": "Importo non chiaro", "amount": 4321.0, "evidence_pages": [], "reason": "ruolo ignoto"}
    ]
    ws = _normalized(raw)
    report = validator.validate_worksheet(ws, GENERIC_PERIZIA_PAGES)
    assert report["validation_status"] == validator.STATUS_FAILED
    assert "MONEY_ROW_MISSING_EVIDENCE" in _codes(report)
