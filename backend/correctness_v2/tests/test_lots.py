"""Tests for generic multi-lot detection, the manual-review gate, and lot×bene cases."""

from correctness_v2 import analyst, contract, lots, validator

from .sample_perizia import (
    GENERIC_PERIZIA_PAGES,
    make_multibene_single_lot_worksheet,
    make_multilot_worksheet,
    make_worksheet,
)


def _n(raw):
    return analyst.normalize_worksheet(raw)


def _codes(report):
    return {v["code"] for v in report["violations"]}


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------
def test_single_lot_not_flagged():
    ws = _n(make_worksheet())
    rep = lots.build_lot_report(ws, GENERIC_PERIZIA_PAGES)
    assert rep["multi_lot"] is False
    assert rep["lot_count"] <= 1


def test_multi_lot_detected_from_worksheet():
    ws = _n(make_multilot_worksheet())
    rep = lots.build_lot_report(ws, GENERIC_PERIZIA_PAGES)
    assert rep["multi_lot"] is True
    assert rep["lot_ids"] == ["1", "2"]
    # Per-lot index preserves evidence and is keyed by lot.
    assert {L["lot_id"] for L in rep["lots"]} == {"1", "2"}
    assert all(L["evidence_pages"] for L in rep["lots"])


def test_contaminated_flat_fields_detected():
    ws = _n(make_multilot_worksheet())
    paths = {f["path"] for f in lots.contaminated_flat_fields(ws)}
    assert "case_identity.address" in paths


def test_lot_enumeration_parsed():
    # "Lotti 1, 2 e 3" must yield three lots, not one.
    assert lots.lot_ids_in_text("Lotti 1, 2 e 3") == ["1", "2", "3"]
    assert lots.lot_ids_in_text("LOTTO 1") == ["1"]
    assert lots.lot_ids_in_text("lotto unico") == ["unico"]


def test_zero_lot_id_is_footer_noise_not_a_lot():
    # Lots are 1-indexed: "Lotto 00" is an authoring-tool footer artifact
    # ("Relazione Lotto 00 2 creata in data ..."), never a real lot.
    assert lots._numeric_ids(lots.lot_ids_in_text("Relazione Lotto 00 2 creata in data")) == []
    assert lots.normalize_lot_token("Lotto 00") is None
    assert lots.normalize_lot_token("Lotto 0") is None
    # Real lots are unaffected.
    assert lots.normalize_lot_token("Lotto 2") == "2"
    assert lots._numeric_ids(["1", "2"]) == ["1", "2"]
    ws = _n(make_multilot_worksheet())
    footer_pages = [
        {**p, "text": p["text"] + "\nRelazione Lotto 00 2 creata in data 03/10/2025"}
        for p in GENERIC_PERIZIA_PAGES
    ]
    rep = lots.build_lot_report(ws, footer_pages)
    assert "00" not in rep["lot_ids"]
    assert rep["lot_ids"] == ["1", "2"]


def test_bene_is_not_a_lot():
    assert lots.bene_ids_in_text("Bene 2 - box auto") == ["2"]
    # The bare word 'bene' without a number is not counted.
    assert lots.bene_ids_in_text("il bene immobile oggetto di stima") == []


# ---------------------------------------------------------------------------
# The four lot x bene combinations
# ---------------------------------------------------------------------------
def test_single_lot_multi_bene_is_not_multi_lot():
    # single-lot / multi-bene: several beni in ONE lot -> NOT manual review.
    ws = _n(make_multibene_single_lot_worksheet())
    rep = lots.build_lot_report(ws, GENERIC_PERIZIA_PAGES)
    assert rep["multi_lot"] is False
    assert rep["multi_bene"] is True
    assert rep["bene_count"] >= 2
    # The validator must NOT reject a single-lot multi-bene worksheet.
    vr = validator.validate_worksheet(ws, GENERIC_PERIZIA_PAGES)
    assert "MULTI_LOT_SELECTION_UNCLEAR" not in _codes(vr)
    assert "LOT_CONTAMINATION" not in _codes(vr)


def test_multi_lot_single_bene_is_multi_lot():
    # multi-lot / single-bene: each lot is one property -> manual review.
    ws = _n(make_multilot_worksheet())
    rep = lots.build_lot_report(ws, GENERIC_PERIZIA_PAGES)
    assert rep["multi_lot"] is True


# ---------------------------------------------------------------------------
# Validator (defense-in-depth)
# ---------------------------------------------------------------------------
def test_validator_rejects_blended_multi_lot():
    ws = _n(make_multilot_worksheet())
    vr = validator.validate_worksheet(ws, GENERIC_PERIZIA_PAGES)
    assert vr["validation_status"] == validator.STATUS_FAILED
    assert "MULTI_LOT_SELECTION_UNCLEAR" in _codes(vr)
    assert "LOT_CONTAMINATION" in _codes(vr)


def test_validator_clean_single_lot_has_no_lot_violations():
    ws = _n(make_worksheet())
    vr = validator.validate_worksheet(ws, GENERIC_PERIZIA_PAGES)
    assert "MULTI_LOT_SELECTION_UNCLEAR" not in _codes(vr)
    assert "LOT_CONTAMINATION" not in _codes(vr)


# ---------------------------------------------------------------------------
# Contract lot summary (single-lot path)
# ---------------------------------------------------------------------------
def test_contract_single_lot_summary_present():
    ws = _n(make_worksheet())
    vr = validator.validate_worksheet(ws, GENERIC_PERIZIA_PAGES)
    rep = lots.build_lot_report(ws, GENERIC_PERIZIA_PAGES)
    con = contract.build_contract(
        worksheet=ws,
        validator_report=vr,
        analysis_id="a",
        job_id="j",
        source_pdf_quality_status="PDF_QUALITY_OK",
        lot_report=rep,
    )
    ls = con["lot_summary"]
    assert ls["multi_lot"] is False
    assert ls["manual_review_required"] is False
    assert "selected_lot" in ls
