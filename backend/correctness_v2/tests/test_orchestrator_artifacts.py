"""Tests for the job shell orchestrator and artifact storage."""

import json
from pathlib import Path

from correctness_v2 import artifacts, orchestrator
from correctness_v2.schemas import JobStatus, PdfBlockReason

from .sample_pages import EMPTY_PAGES, GOOD_PAGES


def _loader(pages):
    def _inner(analysis_id):
        return pages
    return _inner


def test_start_job_ok_saves_all_artifacts(artifacts_root):
    status = orchestrator.start_job("an_ok", _loader(GOOD_PAGES), is_admin=True)
    assert status["status"] == JobStatus.PDF_QUALITY_OK
    assert status["customer_report_generated"] is False
    assert status["safe_to_show_customer"] is False
    assert orchestrator.STEP1_OK_MESSAGE == status["message"]

    job_id = status["job_id"]
    job_dir = artifacts.job_dir(job_id)
    assert (job_dir / artifacts.JOB_STATUS_FILE).exists()
    assert (job_dir / artifacts.INPUT_PAGES_FILE).exists()
    assert (job_dir / artifacts.PDF_QUALITY_FILE).exists()
    # OK job has no error.json.
    assert not (job_dir / artifacts.ERROR_FILE).exists()

    # input_pages.json round-trips.
    saved = json.loads((job_dir / artifacts.INPUT_PAGES_FILE).read_text())
    assert saved["page_count"] == len(GOOD_PAGES)


def test_start_job_blocked_writes_error_and_fails_closed(artifacts_root):
    status = orchestrator.start_job("an_blocked", _loader(EMPTY_PAGES), is_admin=True)
    assert status["status"] == JobStatus.PDF_QUALITY_BLOCKED
    assert status["reason_code"] == PdfBlockReason.DOCUMENT_TEXT_EMPTY
    # An unreadable/empty-text PDF is a "document not readable" block: it now
    # renders a customer-safe "upload a readable PDF" message (no perizia facts),
    # so the customer is told what happened instead of getting nothing.
    assert status["customer_report_generated"] is True
    assert status["safe_to_show_customer"] is True
    assert status["report_status"] == "DOCUMENT_NOT_READABLE"
    assert status["next_steps"]

    job_id = status["job_id"]
    job_dir = artifacts.job_dir(job_id)
    assert (job_dir / artifacts.ERROR_FILE).exists()
    assert (job_dir / artifacts.PDF_QUALITY_FILE).exists()


def test_latest_job_lookup(artifacts_root):
    s1 = orchestrator.start_job("an_latest", _loader(GOOD_PAGES), is_admin=True)
    s2 = orchestrator.start_job("an_latest", _loader(EMPTY_PAGES), is_admin=True)
    latest = artifacts.latest_job_for_analysis("an_latest")
    assert latest is not None
    assert latest["job_id"] in {s1["job_id"], s2["job_id"]}
    # Different analysis id has no job.
    assert artifacts.latest_job_for_analysis("does_not_exist") is None


def test_read_job_status_roundtrip(artifacts_root):
    status = orchestrator.start_job("an_rt", _loader(GOOD_PAGES), is_admin=True)
    read = artifacts.read_job_status(status["job_id"])
    assert read["job_id"] == status["job_id"]
    assert read["analysis_id"] == "an_rt"
