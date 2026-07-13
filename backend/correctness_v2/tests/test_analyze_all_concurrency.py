"""Integration tests for concurrent analyze_all through the real orchestrator.

These assert the mission's hard invariant: the PARALLEL output is semantically
identical to the validated SERIAL output. They also cover completed-lot reuse,
rate-limit degradation, stale-job recovery, and customer/admin visibility — all
with deterministic mocked analyst responses (no live OpenAI).
"""

import json
import re
import threading

from correctness_v2 import artifacts, customer_view, job_status, orchestrator
from correctness_v2.openai_client import REASON_RATE_LIMITED, OpenAIClientError
from correctness_v2.schemas import JobStatus

from .sample_perizia import (
    MULTI_LOT_PAGES,
    make_multilot_worksheet,
    single_lot_worksheet_on_page,
)

# Three page-segmentable lots: global preamble + per-lot pages (mirrors the
# lot-aware pipeline test fixtures).
THREE_LOT_PAGES = MULTI_LOT_PAGES + [
    {"page_number": 6, "text": MULTI_LOT_PAGES[3]["text"].replace("LOTTO 2", "LOTTO 3")},
    {"page_number": 7, "text": "Segue la descrizione del LOTTO 3 e relativi allegati."},
]


def _loader(pages):
    def _inner(analysis_id):
        return pages

    return _inner


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


def _three_lot_worksheets():
    return {
        "1": single_lot_worksheet_on_page(2, "1"),
        "2": single_lot_worksheet_on_page(4, "2"),
        "3": single_lot_worksheet_on_page(6, "3"),
    }


def _lot_keyed_caller(detection_ws, per_lot_ws, *, rate_limit_once=frozenset(), raise_for=frozenset()):
    """Deterministic, thread-safe caller keyed by the target lot (never by timing).

    ``rate_limit_once`` lots raise a 429 on their FIRST per-lot call then succeed.
    ``raise_for`` lots always raise a non-retryable analyst error.
    """
    calls = []
    lock = threading.Lock()
    attempts = {}

    def _caller(messages, *, model=None, timeout=None):
        user = next((m.get("content", "") for m in messages if m.get("role") == "user"), "")
        match = re.search(r"STAI ANALIZZANDO ESCLUSIVAMENTE IL LOTTO (\S+)\.", user)
        target_lot = match.group(1) if match else None
        pages_seen = [int(n) for n in re.findall(r"=== PAGINA (\d+) ===", user)]
        with lock:
            calls.append({"target_lot": target_lot, "pages_seen": pages_seen})
            n = attempts.get(target_lot, 0) + 1
            attempts[target_lot] = n
        if target_lot in raise_for:
            raise OpenAIClientError(
                f"simulated failure for lot {target_lot}", reason_code="OPENAI_CALL_FAILED"
            )
        if target_lot in rate_limit_once and n == 1:
            raise OpenAIClientError(
                f"simulated 429 for lot {target_lot}", reason_code=REASON_RATE_LIMITED
            )
        ws = detection_ws if target_lot is None else per_lot_ws[target_lot]
        return {
            "content": json.dumps(ws, ensure_ascii=False),
            "model": model or "fake-model",
            "finish_reason": "stop",
            "usage": {"total_tokens": 1},
            "response_id": "resp_fake",
        }

    _caller.calls = calls  # type: ignore[attr-defined]
    _caller.per_lot_calls = lambda: [c for c in calls if c["target_lot"] is not None]
    return _caller


def _strip_volatile(obj):
    """Drop volatile fields so two independent runs can be compared for equality."""
    volatile = {"_saved_at", "job_id", "generated_at", "created_at", "updated_at"}
    if isinstance(obj, dict):
        return {k: _strip_volatile(v) for k, v in obj.items() if k not in volatile}
    if isinstance(obj, list):
        return [_strip_volatile(v) for v in obj]
    return obj


def _lot_contract(job_id, lot_id):
    path = artifacts.job_dir(job_id) / "lots" / lot_id / artifacts.VERIFIED_CONTRACT_FILE
    return json.loads(path.read_text())


def _lot_customer_report(job_id, lot_id):
    path = artifacts.job_dir(job_id) / "lots" / lot_id / artifacts.CUSTOMER_REPORT_FILE
    return json.loads(path.read_text())


# --- (10) serial-versus-parallel equality ----------------------------------
def test_serial_and_parallel_outputs_are_identical(artifacts_root, monkeypatch):
    """Concurrency 1 and concurrency 2 must yield byte-identical canonical output."""
    monkeypatch.delenv("CORRECTNESS_V2_LOT_REUSE", raising=False)  # both fully re-derive

    def _run(concurrency):
        monkeypatch.setenv("CORRECTNESS_V2_LOT_CONCURRENCY", str(concurrency))
        caller = _lot_keyed_caller(_three_lot_detection_worksheet(), _three_lot_worksheets())
        status = orchestrator.start_job(
            "an_equality",
            _loader(THREE_LOT_PAGES),
            is_admin=True,
            openai_caller=caller,
            analyze_all=True,
        )
        assert status["status"] == JobStatus.REPORT_READY, status
        contracts = {lid: _lot_contract(status["job_id"], lid) for lid in ("1", "2", "3")}
        reports = {lid: _lot_customer_report(status["job_id"], lid) for lid in ("1", "2", "3")}
        return status, contracts, reports

    serial_status, serial_contracts, serial_reports = _run(1)
    par_status, par_contracts, par_reports = _run(2)

    # (a) same per-lot statuses and ordering.
    assert [r["lot_id"] for r in serial_status["per_lot_results"]] == \
        [r["lot_id"] for r in par_status["per_lot_results"]]

    # (b) per-lot verified contracts are identical (ignoring volatile metadata).
    for lid in ("1", "2", "3"):
        assert _strip_volatile(serial_contracts[lid]) == _strip_volatile(par_contracts[lid]), lid
        assert _strip_volatile(serial_reports[lid]) == _strip_volatile(par_reports[lid]), lid

    # (c) parallel run genuinely used the configured concurrency ceiling.
    assert par_status["concurrency"]["configured_concurrency"] == 2
    assert serial_status["concurrency"]["configured_concurrency"] == 1


# --- (6)/(7-reuse) completed-lot reuse without a new analyst call -----------
def test_completed_lots_are_reused_without_new_openai_call(artifacts_root, monkeypatch):
    monkeypatch.setenv("CORRECTNESS_V2_LOT_CONCURRENCY", "2")
    monkeypatch.setenv("CORRECTNESS_V2_LOT_REUSE", "1")

    # First run: produce all three lot reports.
    caller1 = _lot_keyed_caller(_three_lot_detection_worksheet(), _three_lot_worksheets())
    status1 = orchestrator.start_job(
        "an_reuse", _loader(THREE_LOT_PAGES), is_admin=True,
        openai_caller=caller1, analyze_all=True,
    )
    assert status1["status"] == JobStatus.REPORT_READY, status1

    # Second run: any PER-LOT call would raise. Detection still runs, but the
    # already-completed lots must be reused with NO new per-lot analyst call.
    caller2 = _lot_keyed_caller(
        _three_lot_detection_worksheet(), _three_lot_worksheets(),
        raise_for={"1", "2", "3"},
    )
    status2 = orchestrator.start_job(
        "an_reuse", _loader(THREE_LOT_PAGES), is_admin=True,
        openai_caller=caller2, analyze_all=True,
    )
    assert status2["status"] == JobStatus.REPORT_READY, status2
    # No per-lot analyst calls were made in the second run (all reused).
    assert caller2.per_lot_calls() == []
    assert status2["concurrency"]["reused_lot_count"] == 3


# --- (8) rate-limit degradation end-to-end ---------------------------------
def test_analyze_all_rate_limit_degrades_but_completes(artifacts_root, monkeypatch):
    monkeypatch.setenv("CORRECTNESS_V2_LOT_CONCURRENCY", "3")
    monkeypatch.delenv("CORRECTNESS_V2_LOT_REUSE", raising=False)
    caller = _lot_keyed_caller(
        _three_lot_detection_worksheet(), _three_lot_worksheets(),
        rate_limit_once={"1", "2", "3"},
    )
    status = orchestrator.start_job(
        "an_degrade", _loader(THREE_LOT_PAGES), is_admin=True,
        openai_caller=caller, analyze_all=True,
    )
    # All lots still complete correctly after the safe serial degradation.
    assert status["status"] == JobStatus.REPORT_READY, status
    assert status["all_lots_ready"] is True
    assert status["degraded_to_serial"] is True
    assert status["concurrency"]["total_retries"] >= 3
    for lid in ("1", "2", "3"):
        assert _lot_contract(status["job_id"], lid)["case_identity"]["address"] == \
            f"Via del Lotto {lid}"


# --- (9) restart / stale-job recovery --------------------------------------
def test_recover_stale_jobs_marks_running_as_stalled(artifacts_root):
    # A job left RUNNING (its process died) and a terminal REPORT_READY job.
    running = job_status.make_status(
        job_id="cv2_stuck", analysis_id="an_stale", status=JobStatus.RUNNING,
        current_stage="step2:running", admin_only=True,
    )
    artifacts.save_job_status("cv2_stuck", running)
    done = job_status.make_status(
        job_id="cv2_done", analysis_id="an_stale", status=JobStatus.REPORT_READY,
        current_stage="step3:report_ready", admin_only=True,
        customer_report_generated=True, safe_to_show_customer=True,
    )
    artifacts.save_job_status("cv2_done", done)

    recovered = orchestrator.recover_stale_jobs(force=True)
    assert "cv2_stuck" in recovered
    assert "cv2_done" not in recovered

    stuck = artifacts.read_job_status("cv2_stuck")
    assert stuck["status"] == JobStatus.JOB_STALLED
    assert stuck["reason_code"] == "JOB_STALLED_RECOVERED"
    assert stuck["safe_to_show_customer"] is False
    # The terminal job is untouched.
    assert artifacts.read_job_status("cv2_done")["status"] == JobStatus.REPORT_READY


def test_recover_stale_jobs_respects_age_threshold(artifacts_root):
    fresh = job_status.make_status(
        job_id="cv2_fresh", analysis_id="an_fresh", status=JobStatus.RUNNING,
        current_stage="step2:running", admin_only=True,
    )
    artifacts.save_job_status("cv2_fresh", fresh)
    # Not forced and well within the age window -> not recovered.
    recovered = orchestrator.recover_stale_jobs(force=False, max_age_seconds=10_000)
    assert "cv2_fresh" not in recovered
    assert artifacts.read_job_status("cv2_fresh")["status"] == JobStatus.RUNNING


# --- (11) customer visibility: no unsafe/legacy leak on partial failure -----
def test_partial_failure_batch_is_not_customer_safe(artifacts_root, monkeypatch):
    monkeypatch.setenv("CORRECTNESS_V2_LOT_CONCURRENCY", "2")
    caller = _lot_keyed_caller(
        _three_lot_detection_worksheet(), _three_lot_worksheets(), raise_for={"2"}
    )
    status = orchestrator.start_job(
        "an_partial", _loader(THREE_LOT_PAGES), is_admin=True,
        openai_caller=caller, analyze_all=True,
    )
    assert status["status"] == JobStatus.NEEDS_MANUAL_REVIEW, status
    assert status["safe_to_show_customer"] is False
    report = artifacts.read_json(status["job_id"], artifacts.CUSTOMER_REPORT_FILE)
    # The overall analyze_all report is never exposed as a clean customer report.
    assert customer_view.is_customer_safe(report, status) is False


# --- (12) admin visibility: concurrency diagnostics admin-only --------------
def test_concurrency_diagnostics_are_admin_only(artifacts_root, monkeypatch):
    monkeypatch.setenv("CORRECTNESS_V2_LOT_CONCURRENCY", "2")
    caller = _lot_keyed_caller(_three_lot_detection_worksheet(), _three_lot_worksheets())
    status = orchestrator.start_job(
        "an_admin_meta", _loader(THREE_LOT_PAGES), is_admin=True,
        openai_caller=caller, analyze_all=True,
    )
    # Admin status carries the technical concurrency block.
    assert "concurrency" in status
    assert status["concurrency"]["max_active_concurrency"] >= 1

    # The customer sanitizer builds its own report and never surfaces the raw
    # job-status concurrency diagnostics.
    lot_report = _lot_customer_report(status["job_id"], "1")
    sanitized = customer_view.sanitize_customer_report(lot_report, status)
    assert "concurrency" not in json.dumps(sanitized)
    assert "max_active_concurrency" not in json.dumps(sanitized)
