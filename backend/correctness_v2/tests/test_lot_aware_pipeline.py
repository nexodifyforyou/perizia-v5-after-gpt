"""End-to-end lot-aware orchestrator tests (no live OpenAI — fakes injected).

Covers the five required behaviors:
  A) single-lot proceeds normally
  B) multi-bene inside one lot is NOT blocked
  C) multi-lot, no selection -> LOT_SELECTION_REQUIRED (+ lot_index/per_lot_packets)
  D) multi-lot, selected lot -> analyze only that lot's isolated pages
  E) multi-lot, analyze_all -> a separate contract per lot (never blended)
"""

from correctness_v2 import artifacts, orchestrator
from correctness_v2.schemas import JobStatus

from .sample_perizia import (
    GENERIC_PERIZIA_PAGES,
    MULTI_LOT_PAGES,
    fake_caller_returning,
    fake_sequence_caller,
    make_multibene_single_lot_worksheet,
    make_multilot_worksheet,
    make_worksheet,
    single_lot_worksheet_on_page,
)


def _loader(pages):
    def _inner(analysis_id):
        return pages

    return _inner


# A) single-lot ------------------------------------------------------------
def test_single_lot_proceeds_to_contract(artifacts_root):
    caller = fake_caller_returning(make_worksheet())
    status = orchestrator.start_job(
        "an_single", _loader(GENERIC_PERIZIA_PAGES), is_admin=True, openai_caller=caller
    )
    assert status["status"] == JobStatus.CONTRACT_READY, status
    assert status["contract_generated"] is True
    job_dir = artifacts.job_dir(status["job_id"])
    assert (job_dir / artifacts.VERIFIED_CONTRACT_FILE).exists()
    # Single-lot must NOT emit lot-selection artifacts.
    assert not (job_dir / artifacts.LOT_SELECTION_REQUIRED_FILE).exists()


# B) multi-bene inside one lot --------------------------------------------
def test_multi_bene_single_lot_not_blocked(artifacts_root):
    caller = fake_caller_returning(make_multibene_single_lot_worksheet())
    status = orchestrator.start_job(
        "an_multibene", _loader(GENERIC_PERIZIA_PAGES), is_admin=True, openai_caller=caller
    )
    # Several beni in ONE lot is normal -> a contract is produced, never blocked.
    assert status["status"] == JobStatus.CONTRACT_READY, status
    assert status["contract_generated"] is True


# C) multi-lot, no selection ----------------------------------------------
def test_multi_lot_no_selection_requires_selection(artifacts_root):
    caller = fake_caller_returning(make_multilot_worksheet())
    status = orchestrator.start_job(
        "an_ml_nosel", _loader(MULTI_LOT_PAGES), is_admin=True, openai_caller=caller
    )
    assert status["status"] == JobStatus.LOT_SELECTION_REQUIRED, status
    assert status["blended_report_prevented"] is True
    assert sorted(status["lot_ids"]) == ["1", "2"]
    job_dir = artifacts.job_dir(status["job_id"])
    assert (job_dir / artifacts.LOT_INDEX_FILE).exists()
    assert (job_dir / artifacts.PER_LOT_PACKETS_FILE).exists()
    assert not (job_dir / artifacts.VERIFIED_CONTRACT_FILE).exists()


# D) multi-lot, selected lot ----------------------------------------------
def test_selected_lot_analyzes_only_that_lot(artifacts_root):
    # 1st call: full-doc multilot worksheet (triggers detection).
    # 2nd call: clean single-lot worksheet for lot 1 (evidence only on page 2).
    caller = fake_sequence_caller(
        [make_multilot_worksheet(), single_lot_worksheet_on_page(2, "1")]
    )
    status = orchestrator.start_job(
        "an_ml_sel",
        _loader(MULTI_LOT_PAGES),
        is_admin=True,
        openai_caller=caller,
        selected_lot_id="1",
    )
    assert status["status"] == JobStatus.CONTRACT_READY, status
    assert status["selected_lot"] == "1"
    assert status["contract_generated"] is True

    # The re-analysis (2nd call) saw ONLY lot 1's isolated pages (global + lot1),
    # never lot 2's pages -> no cross-lot contamination of the analyzed context.
    second_call_pages = caller.calls[1]["pages_seen"]
    assert set(second_call_pages) == {1, 2, 3}
    assert 4 not in second_call_pages and 5 not in second_call_pages

    job_dir = artifacts.job_dir(status["job_id"])
    assert (job_dir / artifacts.SELECTED_LOT_CONTEXT_FILE).exists()
    assert (job_dir / artifacts.VERIFIED_CONTRACT_FILE).exists()


def test_selected_lot_not_found_fails_closed(artifacts_root):
    caller = fake_sequence_caller([make_multilot_worksheet()])
    status = orchestrator.start_job(
        "an_ml_badsel",
        _loader(MULTI_LOT_PAGES),
        is_admin=True,
        openai_caller=caller,
        selected_lot_id="9",  # no such lot
    )
    assert status["status"] == JobStatus.CONTRACT_VALIDATION_FAILED, status
    assert status["reason_code"] == "SELECTED_LOT_NOT_FOUND"
    job_dir = artifacts.job_dir(status["job_id"])
    assert not (job_dir / artifacts.VERIFIED_CONTRACT_FILE).exists()


# E) multi-lot, analyze_all -----------------------------------------------
def test_analyze_all_produces_one_contract_per_lot(artifacts_root):
    caller = fake_sequence_caller(
        [
            make_multilot_worksheet(),              # full-doc detection
            single_lot_worksheet_on_page(2, "1"),   # lot 1 re-analysis (pages 1,2,3)
            single_lot_worksheet_on_page(4, "2"),   # lot 2 re-analysis (pages 1,4,5)
        ]
    )
    status = orchestrator.start_job(
        "an_ml_all",
        _loader(MULTI_LOT_PAGES),
        is_admin=True,
        openai_caller=caller,
        analyze_all=True,
    )
    assert status["status"] == JobStatus.CONTRACT_READY, status
    assert status["analyze_all"] is True
    assert status["all_lots_ready"] is True
    results = {r["lot_id"]: r for r in status["per_lot_results"]}
    assert set(results) == {"1", "2"}
    for lot_id, res in results.items():
        assert res["status"] == JobStatus.CONTRACT_READY

    job_dir = artifacts.job_dir(status["job_id"])
    assert (job_dir / artifacts.ANALYZE_ALL_RESULT_FILE).exists()
    # One independent contract per lot, kept under its own folder (never blended).
    assert (job_dir / "lots" / "1" / artifacts.VERIFIED_CONTRACT_FILE).exists()
    assert (job_dir / "lots" / "2" / artifacts.VERIFIED_CONTRACT_FILE).exists()
    # No top-level blended contract.
    assert not (job_dir / artifacts.VERIFIED_CONTRACT_FILE).exists()

    # lot 2's re-analysis never saw lot 1's pages.
    third_call_pages = caller.calls[2]["pages_seen"]
    assert set(third_call_pages) == {1, 4, 5}
