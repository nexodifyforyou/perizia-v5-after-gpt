"""Tests for shared-summary row projection (lot-tagged rows on shared pages).

A shared multi-lot page (TOC / schema riassuntivo) is excluded wholesale from
single-lot re-analysis, but rows on it that are CLEARLY tagged with one lot
("LOTTO 1 - PREZZO BASE D'ASTA: € 50.000,00") must be projected into that lot's
money — never blended, never dropped. Rows whose lot association is unclear are
preserved under uncertain_money with evidence + manual_review. All generic: no
real document, tribunale or città.
"""

import json

from correctness_v2 import analyst, artifacts, contract as contract_mod, lot_packets, lots, orchestrator
from correctness_v2.schemas import JobStatus

from .sample_perizia import (
    MULTI_LOT_PAGES,
    fake_sequence_caller,
    make_multilot_worksheet,
    make_worksheet,
    single_lot_worksheet_on_page,
)

# MULTI_LOT_PAGES (1 global, 2-3 lot 1, 4-5 lot 2) plus a shared summary page
# whose rows are clearly lot-tagged, plus one untagged amount.
SUMMARY_PAGE = {
    "page_number": 6,
    "text": (
        "SCHEMA RIASSUNTIVO DELLA VENDITA - LOTTO 1 e LOTTO 2\n"
        "Lotto 1 - Prezzo base d'asta: € 75.000,00\n"
        "Lotto 2 - Prezzo base d'asta: € 200.000,00\n"
        "Spese di procedura complessive: € 1.234,00\n"
    ),
}
PAGES_WITH_SUMMARY = MULTI_LOT_PAGES + [SUMMARY_PAGE]


def _seg(pages):
    return lot_packets.segment_pages(pages, ["1", "2"])


def test_summary_page_is_shared_and_rows_project_per_lot():
    seg = _seg(PAGES_WITH_SUMMARY)
    assert 6 in seg["shared_pages"]

    proj = lot_packets.project_shared_summary_rows(PAGES_WITH_SUMMARY, seg)
    lot1 = proj["projected"]["1"]
    lot2 = proj["projected"]["2"]
    assert [r["amount"] for r in lot1] == [75000.0]
    assert lot1[0]["field"] == "prezzo_base_asta"
    assert lot1[0]["evidence_pages"] == [6]
    assert [r["amount"] for r in lot2] == [200000.0]
    # The untagged amount is preserved as uncertain with manual review — never
    # assigned to a lot, never dropped.
    unc = proj["uncertain"]
    assert [r["amount"] for r in unc] == [1234.0]
    assert unc[0]["manual_review"] is True


def test_rows_repeated_on_toc_and_summary_are_deduplicated():
    toc = {
        "page_number": 7,
        "text": (
            "SOMMARIO Lotto 1 e Lotto 2\n"
            "Lotto 1 - Prezzo base d'asta: € 75.000,00...................41\n"
            "Lotto 2 - Prezzo base d'asta: € 200.000,00..................58\n"
        ),
    }
    pages = PAGES_WITH_SUMMARY + [toc]
    seg = _seg(pages)
    proj = lot_packets.project_shared_summary_rows(pages, seg)
    lot1 = proj["projected"]["1"]
    # One row per (lot, field, amount) with unioned evidence — not two duplicates.
    assert [r["amount"] for r in lot1] == [75000.0]
    assert lot1[0]["evidence_pages"] == [6, 7]


def test_inline_multi_lot_row_splits_at_each_lot_tag():
    # One line naming both lots: each amount follows its own lot tag; the amount
    # BEFORE the first tag has no clear owner and stays uncertain.
    page = {
        "page_number": 6,
        "text": "Totale € 5.000,00 LOTTO 1 valore € 100.000,00 LOTTO 2 valore € 200.000,00",
    }
    pages = MULTI_LOT_PAGES + [page]
    seg = _seg(pages)
    proj = lot_packets.project_shared_summary_rows(pages, seg)
    assert [r["amount"] for r in proj["projected"]["1"]] == [100000.0]
    assert [r["amount"] for r in proj["projected"]["2"]] == [200000.0]
    assert [r["amount"] for r in proj["uncertain"]] == [5000.0]


def test_build_lot_money_projects_rows_and_fills_missing_fields():
    ws = analyst.normalize_worksheet(make_multilot_worksheet())
    seg = _seg(PAGES_WITH_SUMMARY)
    money = lot_packets.build_lot_money(ws, seg, PAGES_WITH_SUMMARY)

    lot1 = money["by_lot"]["1"]
    lot2 = money["by_lot"]["2"]
    assert [r["amount"] for r in lot1["shared_summary_rows"]] == [75000.0]
    assert [r["amount"] for r in lot2["shared_summary_rows"]] == [200000.0]
    # The projected row fills the canonical field when the lot has no value yet.
    assert lot1["prezzo_base_asta"]["amount"] == 75000.0
    assert lot1["prezzo_base_asta"]["source"] == "shared_summary_projection"
    assert lot2["prezzo_base_asta"]["amount"] == 200000.0
    # No cross-lot leakage.
    assert not any(r["amount"] == 200000.0 for r in lot1["shared_summary_rows"])


def test_build_lot_money_never_overwrites_model_linked_values():
    ws = analyst.normalize_worksheet(make_multilot_worksheet())
    ws["lots"] = [
        {"lot_id": "1", "label": "Lotto 1", "prezzo_base_asta": 99999.0, "evidence_pages": [2]},
    ]
    seg = _seg(PAGES_WITH_SUMMARY)
    money = lot_packets.build_lot_money(ws, seg, PAGES_WITH_SUMMARY)
    # The analyst-linked value wins; the projected row is still kept as a row.
    assert money["by_lot"]["1"]["prezzo_base_asta"]["amount"] == 99999.0
    assert [r["amount"] for r in money["by_lot"]["1"]["shared_summary_rows"]] == [75000.0]


def test_selected_lot_context_carries_only_that_lots_projected_rows():
    ws = analyst.normalize_worksheet(make_multilot_worksheet())
    seg = _seg(PAGES_WITH_SUMMARY)
    ctx = lot_packets.build_selected_lot_context(
        PAGES_WITH_SUMMARY, seg, "1", worksheet=ws
    )
    rows = ctx["lot_money"]["shared_summary_rows"]
    assert [r["amount"] for r in rows] == [75000.0]
    assert "200000" not in json.dumps(ctx["lot_money"])


def test_contract_merges_projected_auction_term_without_duplicates():
    # Worksheet already carries prezzo base 75000 -> the equal projected row must
    # NOT be double-listed in auction_terms, but stays in shared_summary_money.
    ws = analyst.normalize_worksheet(make_worksheet())
    validator_report = {"validation_status": "VALIDATED", "warnings": [], "checks": {"money_signals": {"base_price_explicit_text": True}}}
    row = {
        "label": "Lotto 1 - Prezzo base d'asta",
        "amount": 75000.0,
        "field": "prezzo_base_asta",
        "evidence_pages": [6],
        "source": "shared_summary_projection",
    }
    c = contract_mod.build_contract(
        worksheet=ws,
        validator_report=validator_report,
        analysis_id="an_x",
        job_id="job_x",
        source_pdf_quality_status="OK",
        shared_summary_rows=[row],
    )
    base_rows = [r for r in c["auction_terms"] if abs(r["amount"] - 75000.0) <= 1.0]
    assert len(base_rows) == 1
    assert [r["amount"] for r in c["shared_summary_money"]] == [75000.0]


def test_contract_adds_projected_auction_term_when_missing():
    ws = analyst.normalize_worksheet(make_worksheet())
    ws["money"]["auction_terms"] = {}
    ws["money"]["base_auction_value"] = None
    validator_report = {"validation_status": "VALIDATED", "warnings": [], "checks": {}}
    row = {
        "label": "Lotto 1 - Prezzo base d'asta",
        "amount": 75000.0,
        "field": "prezzo_base_asta",
        "evidence_pages": [6],
        "source": "shared_summary_projection",
    }
    c = contract_mod.build_contract(
        worksheet=ws,
        validator_report=validator_report,
        analysis_id="an_x",
        job_id="job_x",
        source_pdf_quality_status="OK",
        shared_summary_rows=[row],
    )
    added = [r for r in c["auction_terms"] if r.get("source") == "shared_summary_projection"]
    assert [r["amount"] for r in added] == [75000.0]
    assert added[0]["evidence_pages"] == [6]
    # And it is visible in the flat renderer table too.
    assert any(
        r.get("source") == "shared_summary_projection" and r["amount"] == 75000.0
        for r in c["money_table"]
    )


def test_contract_keeps_distinct_equal_amount_projection_rows_end_to_end():
    ws = analyst.normalize_worksheet(make_worksheet())
    ws["money"]["regularization_costs"] = None
    rows = [
        {"label": "Deprezzamento per vetustà", "amount": 5000, "field": None, "evidence_pages": [4]},
        {"label": "Costi di regolarizzazione impianto elettrico", "amount": 5000, "field": None, "evidence_pages": [7]},
    ]
    built = contract_mod.build_contract(
        worksheet=ws,
        validator_report={"validation_status": "VALIDATED", "warnings": [], "checks": {}},
        analysis_id="an_equal_distinct",
        job_id="job_equal_distinct",
        source_pdf_quality_status="OK",
        shared_summary_rows=rows,
    )

    projected = [
        row for row in built["valuation_chain"]
        if row.get("source") == "shared_summary_projection" and row["amount"] == 5000
    ]
    assert {row["label"] for row in projected} == {row["label"] for row in rows}
    assert len([
        row for row in built["money_table"]
        if row.get("source") == "shared_summary_projection" and row["amount"] == 5000
    ]) == 2


def test_contract_merges_repeated_compatible_projection_row_across_pages():
    ws = analyst.normalize_worksheet(make_worksheet())
    ws["money"]["regularization_costs"] = None
    rows = [
        {"label": "Deprezzamento per vetustà", "amount": 5000, "field": None, "evidence_pages": [4]},
        {"label": "Deprezzamento per vetustà", "amount": 5000, "field": None, "evidence_pages": [7]},
    ]
    built = contract_mod.build_contract(
        worksheet=ws,
        validator_report={"validation_status": "VALIDATED", "warnings": [], "checks": {}},
        analysis_id="an_equal_repeat",
        job_id="job_equal_repeat",
        source_pdf_quality_status="OK",
        shared_summary_rows=rows,
    )

    projected = [
        row for row in built["valuation_chain"]
        if row.get("source") == "shared_summary_projection" and row["amount"] == 5000
    ]
    assert len(projected) == 1
    assert projected[0]["evidence_pages"] == [4, 7]
    assert len([
        row for row in built["money_table"]
        if row.get("source") == "shared_summary_projection" and row["amount"] == 5000
    ]) == 1


def _loader(pages):
    return lambda analysis_id: pages


def test_selected_lot_contract_preserves_own_summary_row_never_other_lots(artifacts_root):
    # End-to-end: multi-lot doc with a shared summary page; lot 1 selected. The
    # re-analysis never sees page 6, yet lot 1's tagged summary row is preserved
    # in the contract; lot 2's row (200000) never appears anywhere in it.
    caller = fake_sequence_caller(
        [make_multilot_worksheet(), single_lot_worksheet_on_page(2, "1")]
    )
    status = orchestrator.start_job(
        "an_sum_sel",
        _loader(PAGES_WITH_SUMMARY),
        is_admin=True,
        openai_caller=caller,
        selected_lot_id="1",
    )
    assert status["status"] == JobStatus.REPORT_READY, status

    # Shared page 6 stays out of the re-analysis input (no contamination).
    assert 6 not in caller.calls[1]["pages_seen"]

    job_dir = artifacts.job_dir(status["job_id"])
    c = json.loads((job_dir / artifacts.VERIFIED_CONTRACT_FILE).read_text())
    blob = json.dumps(c)
    assert [r["amount"] for r in c["shared_summary_money"]] == [75000.0]
    assert "200000" not in blob  # lot 2's summary money never leaks into lot 1
    # The worksheet's own prezzo base equals the summary row -> exactly one row.
    base_rows = [r for r in c["auction_terms"] if abs(r["amount"] - 75000.0) <= 1.0]
    assert len(base_rows) == 1
