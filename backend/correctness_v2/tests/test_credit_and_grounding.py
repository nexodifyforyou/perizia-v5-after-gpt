"""Tests for two reliability fixes:

  1. analyze_all applies the SAME deterministic valuation-terminal grounding
     (complete_valuation_terminals) per lot that the single-lot path uses, so a
     multi-lot lot is no longer at the mercy of which terminal the analyst emits.
  2. OpenAI account-credit exhaustion (429 insufficient_quota) is classified as a
     distinct, NON-retryable reason, the customer sees only a neutral message
     (never OpenAI/quota), and the admin sees an unmistakable recharge signal.
"""

from correctness_v2 import artifacts, contract as contract_mod, lot_runner, openai_client, orchestrator
from correctness_v2.openai_client import (
    REASON_QUOTA_EXHAUSTED,
    REASON_RATE_LIMITED,
    OpenAIClientError,
)
from correctness_v2.schemas import JobStatus

from .sample_perizia import (
    MULTI_LOT_PAGES,
    make_multilot_worksheet,
    single_lot_worksheet_on_page,
    fake_sequence_caller,
)


def _loader(pages):
    def _inner(analysis_id):
        return pages

    return _inner


# --- (1) analyze_all applies per-lot valuation grounding -------------------
def test_analyze_all_applies_complete_valuation_terminals_per_lot(artifacts_root, monkeypatch):
    calls = []
    real = contract_mod.complete_valuation_terminals

    def _spy(worksheet, pages):
        calls.append([p.get("page_number") for p in pages])
        return real(worksheet, pages)

    monkeypatch.setattr(orchestrator.contract_mod, "complete_valuation_terminals", _spy)

    caller = fake_sequence_caller(
        [
            make_multilot_worksheet(),            # detection
            single_lot_worksheet_on_page(2, "1"),  # lot 1 (pages 1,2,3)
            single_lot_worksheet_on_page(4, "2"),  # lot 2 (pages 1,4,5)
        ]
    )
    status = orchestrator.start_job(
        "an_grounding", _loader(MULTI_LOT_PAGES), is_admin=True,
        openai_caller=caller, analyze_all=True,
    )
    assert status["status"] == JobStatus.REPORT_READY, status
    # Grounding ran once per analyzed lot, each on that lot's own isolated pages
    # (NOT the whole document) — mirroring the single-lot path.
    assert len(calls) == 2, calls
    for page_set in calls:
        assert set(page_set).issubset({1, 2, 3, 4, 5})
        assert len(page_set) <= 3  # a lot's isolated subset, never all 5 pages


# --- (2a) quota classification + non-retryability --------------------------
class _FakeQuotaError(Exception):
    status_code = 429
    code = "insufficient_quota"


class _FakeRateLimitError(Exception):
    status_code = 429


def test_classify_quota_vs_rate_limit():
    assert openai_client.classify_openai_exception(_FakeQuotaError()) == REASON_QUOTA_EXHAUSTED
    assert openai_client.classify_openai_exception(_FakeRateLimitError()) == REASON_RATE_LIMITED
    # Quota is NOT transient (retrying cannot help); rate-limit IS.
    assert openai_client.is_quota_exhausted_reason(REASON_QUOTA_EXHAUSTED) is True
    assert openai_client.is_transient_reason(REASON_QUOTA_EXHAUSTED) is False
    assert openai_client.is_transient_reason(REASON_RATE_LIMITED) is True


def test_lot_runner_does_not_retry_quota_exhausted():
    attempts = {"n": 0}

    def _call(key):
        attempts["n"] += 1
        raise OpenAIClientError("no credit", reason_code=REASON_QUOTA_EXHAUSTED)

    report = lot_runner.run_lot_batch(
        ["1"], _call, analysis_id="A", concurrency=1, sleep_fn=lambda _s: None
    )
    out = report.outcomes["1"]
    assert not out.ok
    assert out.attempts == 1          # failed fast, no retry
    assert out.retry_count == 0
    assert attempts["n"] == 1
    assert report.degraded_to_serial is False  # quota is not a rate-limit degrade


# --- (2b) customer neutral message vs admin recharge signal ----------------
def test_quota_exhaustion_customer_neutral_admin_recharge(artifacts_root):
    def _quota_caller(messages, *, model=None, timeout=None):
        raise OpenAIClientError(
            "Error code: 429 - insufficient_quota", reason_code=REASON_QUOTA_EXHAUSTED
        )

    status = orchestrator.start_job(
        "an_credit_test", _loader(MULTI_LOT_PAGES), is_admin=True, openai_caller=_quota_caller
    )
    # Admin status: precise, unmistakable recharge signal.
    assert status["status"] == JobStatus.FAILED_ANALYSIS
    assert status["reason_code"] == REASON_QUOTA_EXHAUSTED
    assert status.get("credit_exhausted") is True
    assert "CREDITO API ESAURITO" in status["reason_human"]

    # Customer report: neutral only — never leaks OpenAI/quota/credit/technical.
    report = artifacts.read_json(status["job_id"], artifacts.CUSTOMER_REPORT_FILE)
    blob = __import__("json").dumps(report, ensure_ascii=False).lower()
    for banned in ("openai", "gpt", "insufficient", "credito", "ricarica"):
        assert banned not in blob, f"customer report leaked '{banned}'"
    assert "contatta l'amministratore" in blob


def test_non_quota_analyst_failure_customer_still_neutral(artifacts_root):
    def _bad_caller(messages, *, model=None, timeout=None):
        raise OpenAIClientError("malformed", reason_code="OPENAI_CALL_FAILED")

    status = orchestrator.start_job(
        "an_generic_fail", _loader(MULTI_LOT_PAGES), is_admin=True, openai_caller=_bad_caller
    )
    assert status["status"] == JobStatus.FAILED_ANALYSIS
    assert status.get("credit_exhausted") is False
    report = artifacts.read_json(status["job_id"], artifacts.CUSTOMER_REPORT_FILE)
    blob = __import__("json").dumps(report, ensure_ascii=False).lower()
    assert "openai" not in blob
    assert "contatta l'amministratore" in blob
