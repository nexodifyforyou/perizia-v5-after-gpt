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
    assert status["status"] == JobStatus.REPORT_READY, status
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
    assert status["status"] == JobStatus.REPORT_READY, status
    assert status["status"] != JobStatus.LOT_SELECTION_REQUIRED
    assert status["contract_generated"] is True
    # ONE lot contract carrying both beni — beni are tracked, never split into lots.
    import json as _json

    job_dir = artifacts.job_dir(status["job_id"])
    contract = _json.loads((job_dir / artifacts.VERIFIED_CONTRACT_FILE).read_text())
    assert contract["lot_summary"]["multi_lot"] is False
    assert contract["lot_summary"]["multi_bene"] is True
    assert contract["lot_summary"]["bene_count"] >= 2
    assert not (job_dir / artifacts.LOT_SELECTION_REQUIRED_FILE).exists()


def test_multi_bene_apartment_plus_garage_pages_stay_single_lot(artifacts_root):
    # Lotto 1 with Bene 1 (appartamento) + Bene 2 (garage) + Bene 3 (terreno)
    # described in the PAGE TEXT too: bene mentions must never count as lots.
    pages = [
        {
            "page_number": p["page_number"],
            "text": p["text"] + (
                " Bene N. 1: appartamento ad uso abitativo. Bene N. 2: garage e "
                "cantina di pertinenza. Bene N. 3: terreno annesso."
            ),
        }
        for p in GENERIC_PERIZIA_PAGES
    ]
    caller = fake_caller_returning(make_multibene_single_lot_worksheet())
    status = orchestrator.start_job(
        "an_multibene_pages", _loader(pages), is_admin=True, openai_caller=caller
    )
    assert status["status"] == JobStatus.REPORT_READY, status
    assert status["status"] != JobStatus.LOT_SELECTION_REQUIRED
    assert status.get("multi_lot") is False


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
    assert status["status"] == JobStatus.REPORT_READY, status
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


def test_selected_lot_unsupported_conforming_downgraded_not_failed(artifacts_root):
    # The re-analysis claims 'conforming' for an administrative area whose cited
    # page (3) has NO conformity statement. That must NOT fail the job with
    # UNSUPPORTED_COMPLIANCE_CLAIM: the evidence gate downgrades it to
    # 'uncertain' (manual review) and the contract is still produced.
    reanalysis = single_lot_worksheet_on_page(2, "1")
    reanalysis["technical_compliance"].append(
        {
            "area": "Completezza documentazione ex art. 567 c.p.c.",
            "classification": "conforming",
            "blocks_saleability": False,
            "cost": None,
            "timing": None,
            "notes": None,
            "evidence_pages": [3],  # page 3 has no conformity wording
        }
    )
    caller = fake_sequence_caller([make_multilot_worksheet(), reanalysis])
    status = orchestrator.start_job(
        "an_ml_downgrade",
        _loader(MULTI_LOT_PAGES),
        is_admin=True,
        openai_caller=caller,
        selected_lot_id="1",
    )
    assert status["status"] == JobStatus.REPORT_READY, status
    assert status["compliance_downgrade_count"] == 1

    job_dir = artifacts.job_dir(status["job_id"])
    assert (job_dir / artifacts.COMPLIANCE_GATE_FILE).exists()

    import json

    contract = json.loads((job_dir / artifacts.VERIFIED_CONTRACT_FILE).read_text())
    cards = {c["area"]: c for c in contract["risk_cards"]}
    card = cards["Completezza documentazione ex art. 567 c.p.c."]
    assert card["classification"] == "uncertain"
    # The downgrade is surfaced as an uncertainty flag, never as conformity.
    assert any(
        "Completezza documentazione" in str(f.get("detail"))
        for f in contract["uncertainty_flags"]
    )

    # The second (re-analysis) call carried the whole-document map.
    second_user = caller.calls[1]["user_text"]
    assert "MAPPA DEL DOCUMENTO" in second_user


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
    assert status["status"] == JobStatus.REPORT_READY, status
    assert status["analyze_all"] is True
    assert status["all_lots_ready"] is True
    results = {r["lot_id"]: r for r in status["per_lot_results"]}
    assert set(results) == {"1", "2"}
    for lot_id, res in results.items():
        assert res["status"] == JobStatus.REPORT_READY

    job_dir = artifacts.job_dir(status["job_id"])
    assert (job_dir / artifacts.ANALYZE_ALL_RESULT_FILE).exists()
    # One independent contract per lot, kept under its own folder (never blended).
    assert (job_dir / "lots" / "1" / artifacts.VERIFIED_CONTRACT_FILE).exists()
    assert (job_dir / "lots" / "2" / artifacts.VERIFIED_CONTRACT_FILE).exists()
    # No top-level blended contract.
    assert not (job_dir / artifacts.VERIFIED_CONTRACT_FILE).exists()

    # lot 2's re-analysis never saw lot 1's pages (per-lot analyst calls run
    # concurrently, so it is looked up by target lot, not by call order).
    lot2_call = next(c for c in caller.calls if c["target_lot"] == "2")
    assert set(lot2_call["pages_seen"]) == {1, 4, 5}


# E2) analyze_all with concurrent per-lot analyst calls ---------------------
# The per-lot OpenAI calls are issued concurrently (scheduling only); results,
# ordering, artifacts and failure handling must be identical to the serial run.
import json as _json
import re as _re
import threading as _threading

# Three page-segmentable lots: global preamble + two pages per lot.
THREE_LOT_PAGES = MULTI_LOT_PAGES + [
    {"page_number": 6, "text": MULTI_LOT_PAGES[3]["text"].replace("LOTTO 2", "LOTTO 3")},
    {"page_number": 7, "text": "Segue la descrizione del LOTTO 3 e relativi allegati."},
]


def _three_lot_detection_worksheet():
    ws = make_multilot_worksheet()
    ws["case_identity"]["lotto"] = "Lotti 1, 2 e 3"
    ws["case_identity"]["address"] = (
        "Lotto 1: Via Uno 1; Lotto 2: Via Due 2; Lotto 3: Via Tre 3"
    )
    ws["technical_compliance"].append(
        {
            "area": "Lotto 3 - regolarità edilizia",
            "classification": "regularizable",
            "blocks_saleability": False,
            "cost": 3000.0,
            "timing": "30 giorni",
            "notes": "Difformità del Lotto 3.",
            "evidence_pages": [2],
        }
    )
    return ws


def _lot_keyed_caller(detection_ws, per_lot_ws, raise_for=frozenset()):
    """Deterministic mock caller: dispatches by target lot, never by timing.

    Thread-safe (analyze_all issues the per-lot calls concurrently). A call with
    no target lot gets the detection worksheet; a per-lot call gets that lot's
    worksheet, or raises OpenAIClientError if the lot is in ``raise_for``.
    """
    from correctness_v2.openai_client import OpenAIClientError

    calls = []
    lock = _threading.Lock()

    def _caller(messages, *, model=None, timeout=None):
        user = next((m.get("content", "") for m in messages if m.get("role") == "user"), "")
        match = _re.search(r"STAI ANALIZZANDO ESCLUSIVAMENTE IL LOTTO (\S+)\.", user)
        target_lot = match.group(1) if match else None
        pages_seen = [int(n) for n in _re.findall(r"=== PAGINA (\d+) ===", user)]
        with lock:
            calls.append({"target_lot": target_lot, "pages_seen": pages_seen})
        if target_lot in raise_for:
            raise OpenAIClientError(
                f"simulated OpenAI failure for lot {target_lot}",
                reason_code="OPENAI_CALL_FAILED",
            )
        ws = detection_ws if target_lot is None else per_lot_ws[target_lot]
        return {
            "content": _json.dumps(ws, ensure_ascii=False),
            "model": model or "fake-model",
            "finish_reason": "stop",
            "usage": {"total_tokens": 1},
            "response_id": "resp_fake",
        }

    _caller.calls = calls  # type: ignore[attr-defined]
    return _caller


def _three_lot_worksheets():
    return {
        "1": single_lot_worksheet_on_page(2, "1"),
        "2": single_lot_worksheet_on_page(4, "2"),
        "3": single_lot_worksheet_on_page(6, "3"),
    }


def test_analyze_all_concurrent_calls_keep_order_results_and_isolation(artifacts_root):
    caller = _lot_keyed_caller(_three_lot_detection_worksheet(), _three_lot_worksheets())
    status = orchestrator.start_job(
        "an_ml_conc",
        _loader(THREE_LOT_PAGES),
        is_admin=True,
        openai_caller=caller,
        analyze_all=True,
    )
    assert status["status"] == JobStatus.REPORT_READY, status
    assert sorted(status["lot_ids"]) == ["1", "2", "3"]

    # (a) per_lot_results order matches lot_ids order (deterministic, not
    # dependent on which analyst call finished first).
    assert [r["lot_id"] for r in status["per_lot_results"]] == status["lot_ids"]
    assert all(r["status"] == JobStatus.REPORT_READY for r in status["per_lot_results"])

    # (b) exactly one analyst invocation per lot with pages (+1 detection call).
    per_lot_calls = [c for c in caller.calls if c["target_lot"] is not None]
    assert sorted(c["target_lot"] for c in per_lot_calls) == ["1", "2", "3"]
    assert len(caller.calls) == 4

    # Isolation is unchanged: lot 3's call saw only global + its own pages.
    lot3_call = next(c for c in per_lot_calls if c["target_lot"] == "3")
    assert set(lot3_call["pages_seen"]) == {1, 6, 7}

    # Each lot got ITS OWN worksheet (no cross-lot swap under concurrency).
    job_dir = artifacts.job_dir(status["job_id"])
    for lot_id in ("1", "2", "3"):
        contract_path = job_dir / "lots" / lot_id / artifacts.VERIFIED_CONTRACT_FILE
        assert contract_path.exists(), contract_path
        contract = _json.loads(contract_path.read_text())
        assert contract["case_identity"]["address"] == f"Via del Lotto {lot_id}"


def test_analyze_all_one_lot_analyst_failure_stays_isolated(artifacts_root):
    caller = _lot_keyed_caller(
        _three_lot_detection_worksheet(), _three_lot_worksheets(), raise_for={"2"}
    )
    status = orchestrator.start_job(
        "an_ml_conc_fail",
        _loader(THREE_LOT_PAGES),
        is_admin=True,
        openai_caller=caller,
        analyze_all=True,
    )
    # (c) the failing lot yields the same FAILED_ANALYSIS entry as the serial
    # run; the other lots still succeed, and order is preserved.
    assert status["status"] == JobStatus.NEEDS_MANUAL_REVIEW, status
    assert status["all_lots_ready"] is False
    assert [r["lot_id"] for r in status["per_lot_results"]] == status["lot_ids"]

    by_lot = {r["lot_id"]: r for r in status["per_lot_results"]}
    assert by_lot["2"]["status"] == JobStatus.FAILED_ANALYSIS
    assert "simulated OpenAI failure for lot 2" in by_lot["2"]["reason"]
    assert by_lot["1"]["status"] == JobStatus.REPORT_READY
    assert by_lot["3"]["status"] == JobStatus.REPORT_READY

    job_dir = artifacts.job_dir(status["job_id"])
    assert (job_dir / "lots" / "1" / artifacts.VERIFIED_CONTRACT_FILE).exists()
    assert (job_dir / "lots" / "3" / artifacts.VERIFIED_CONTRACT_FILE).exists()
    assert not (job_dir / "lots" / "2" / artifacts.VERIFIED_CONTRACT_FILE).exists()


def test_lot_concurrency_env_default_and_clamp(monkeypatch):
    # Default is 1 (serial) — the validated behavior; parallel is opt-in via env.
    monkeypatch.delenv("CORRECTNESS_V2_LOT_CONCURRENCY", raising=False)
    assert orchestrator._lot_concurrency() == 1
    monkeypatch.setenv("CORRECTNESS_V2_LOT_CONCURRENCY", "4")
    assert orchestrator._lot_concurrency() == 4
    monkeypatch.setenv("CORRECTNESS_V2_LOT_CONCURRENCY", "2")
    assert orchestrator._lot_concurrency() == 2
    monkeypatch.setenv("CORRECTNESS_V2_LOT_CONCURRENCY", "0")
    assert orchestrator._lot_concurrency() == 1
    monkeypatch.setenv("CORRECTNESS_V2_LOT_CONCURRENCY", "garbage")
    assert orchestrator._lot_concurrency() == 1
