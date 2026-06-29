"""Step 2 orchestrator tests: fail-closed seams + happy path (no live OpenAI)."""

from correctness_v2 import artifacts, orchestrator
from correctness_v2.schemas import JobStatus

from .sample_pages import EMPTY_PAGES
from .sample_perizia import (
    GENERIC_PERIZIA_PAGES,
    fake_caller_raising,
    fake_caller_returning,
    make_multilot_worksheet,
    make_worksheet,
    recording_caller,
)


def _loader(pages):
    def _inner(analysis_id):
        return pages

    return _inner


def test_contract_ready_happy_path(artifacts_root):
    caller = fake_caller_returning(make_worksheet())
    status = orchestrator.start_job(
        "an_ok2", _loader(GENERIC_PERIZIA_PAGES), is_admin=True, openai_caller=caller
    )
    assert status["status"] == JobStatus.CONTRACT_READY, status
    assert status["contract_generated"] is True
    # OpenAI was actually invoked.
    assert len(caller.calls) == 1

    job_id = status["job_id"]
    job_dir = artifacts.job_dir(job_id)
    for fname in (
        artifacts.ANALYST_WORKSHEET_FILE,
        artifacts.VERIFIED_CONTRACT_FILE,
        artifacts.OPENAI_REQUEST_FILE,
        artifacts.OPENAI_RESPONSE_FILE,
        artifacts.VALIDATOR_REPORT_FILE,
    ):
        assert (job_dir / fname).exists(), f"missing artifact {fname}"


def test_openai_request_artifact_has_no_secret(artifacts_root):
    caller = fake_caller_returning(make_worksheet())
    status = orchestrator.start_job(
        "an_secret", _loader(GENERIC_PERIZIA_PAGES), is_admin=True, openai_caller=caller
    )
    req = artifacts.read_json(status["job_id"], artifacts.OPENAI_REQUEST_FILE)
    assert req["secrets_included"] is False
    assert req["api_key"] == "<omitted>"
    # No raw key-looking material in the persisted request.
    assert "sk-" not in str(req)


def test_openai_failure_fails_closed_no_report(artifacts_root):
    caller = fake_caller_raising("OPENAI_CALL_FAILED")
    status = orchestrator.start_job(
        "an_fail", _loader(GENERIC_PERIZIA_PAGES), is_admin=True, openai_caller=caller
    )
    assert status["status"] == JobStatus.FAILED_ANALYSIS
    assert status["reason_code"] == "OPENAI_CALL_FAILED"
    assert status["customer_report_generated"] is False
    assert status["safe_to_show_customer"] is False

    job_dir = artifacts.job_dir(status["job_id"])
    # No contract on failure; error + redacted request are present.
    assert not (job_dir / artifacts.VERIFIED_CONTRACT_FILE).exists()
    assert (job_dir / artifacts.ERROR_FILE).exists()


def test_pdf_quality_blocked_does_not_call_openai(artifacts_root):
    caller = recording_caller()
    status = orchestrator.start_job(
        "an_blocked2", _loader(EMPTY_PAGES), is_admin=True, openai_caller=caller
    )
    assert status["status"] == JobStatus.PDF_QUALITY_BLOCKED
    # The hard rule: OpenAI must never run when quality is blocked.
    assert caller.calls == []
    job_dir = artifacts.job_dir(status["job_id"])
    assert not (job_dir / artifacts.OPENAI_REQUEST_FILE).exists()
    assert not (job_dir / artifacts.VERIFIED_CONTRACT_FILE).exists()


def test_validation_failure_blocks_contract(artifacts_root):
    # Worksheet with an out-of-range evidence page -> validator fails -> no contract.
    raw = make_worksheet()
    raw["technical_compliance"][0]["evidence_pages"] = [99]
    caller = fake_caller_returning(raw)
    status = orchestrator.start_job(
        "an_valfail", _loader(GENERIC_PERIZIA_PAGES), is_admin=True, openai_caller=caller
    )
    assert status["status"] == JobStatus.CONTRACT_VALIDATION_FAILED
    assert status["customer_report_generated"] is False
    job_dir = artifacts.job_dir(status["job_id"])
    assert (job_dir / artifacts.VALIDATOR_REPORT_FILE).exists()
    assert not (job_dir / artifacts.VERIFIED_CONTRACT_FILE).exists()


def test_multi_lot_gate_needs_manual_review_no_blended_contract(artifacts_root):
    # A worksheet that blends two lots must NOT become a single contract; the job
    # stops at NEEDS_MANUAL_REVIEW with a per-lot index, fail-closed.
    caller = fake_caller_returning(make_multilot_worksheet())
    status = orchestrator.start_job(
        "an_multilot", _loader(GENERIC_PERIZIA_PAGES), is_admin=True, openai_caller=caller
    )
    assert status["status"] == JobStatus.NEEDS_MANUAL_REVIEW, status
    assert status["multi_lot"] is True
    assert status["lot_ids"] == ["1", "2"]
    assert status["manual_review_required"] is True
    assert status["selected_lot"] is None
    assert status["contract_generated"] is False
    assert status["safe_to_show_customer"] is False
    assert status["reason_code"] == "MULTI_LOT_MANUAL_REVIEW_REQUIRED"
    assert status["lot_index"]  # per-lot index preserved

    job_dir = artifacts.job_dir(status["job_id"])
    # No blended contract; the lot report IS persisted.
    assert not (job_dir / artifacts.VERIFIED_CONTRACT_FILE).exists()
    assert (job_dir / artifacts.LOT_REPORT_FILE).exists()
    assert (job_dir / artifacts.ANALYST_WORKSHEET_FILE).exists()


def test_step1_only_when_no_caller(artifacts_root):
    # Back-compat: without an injected caller the job stops at the quality gate.
    status = orchestrator.start_job("an_step1", _loader(GENERIC_PERIZIA_PAGES), is_admin=True)
    assert status["status"] in (JobStatus.PDF_QUALITY_OK, JobStatus.PDF_QUALITY_WARNING)
    job_dir = artifacts.job_dir(status["job_id"])
    assert not (job_dir / artifacts.OPENAI_REQUEST_FILE).exists()
