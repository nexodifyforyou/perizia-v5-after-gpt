"""Tests for the strict job-status diagnostic contract."""

import pytest

from correctness_v2 import job_status, schemas
from correctness_v2.schemas import FAILURE_STATUSES, JobStatus


REQUIRED_FIELDS = {
    "job_id",
    "analysis_id",
    "mode",
    "status",
    "current_stage",
    "customer_report_generated",
    "safe_to_show_customer",
    "admin_only",
    "reason_code",
    "reason_human",
    "troubleshoot_message",
    "next_steps",
    "artifacts_saved",
    "created_at",
    "updated_at",
}


def _build_failure(status):
    return job_status.make_failure_status(
        job_id="job1",
        analysis_id="an1",
        status=status,
        current_stage="stage",
        reason_code="SOME_REASON",
        reason_human="Motivo leggibile.",
        troubleshoot_message="Cosa fare per risolvere.",
        next_steps=["passo 1", "passo 2"],
        artifacts_saved={"job_status": "/x/job_status.json"},
    )


def test_status_has_all_contract_fields():
    payload = job_status.make_status(
        job_id="job1",
        analysis_id="an1",
        status=JobStatus.QUEUED,
        current_stage="queued",
    )
    assert REQUIRED_FIELDS.issubset(payload.keys())
    assert payload["mode"] == "correctness_v2"
    assert payload["customer_report_generated"] is False
    assert payload["safe_to_show_customer"] is False


@pytest.mark.parametrize("status", FAILURE_STATUSES)
def test_every_failure_status_carries_full_diagnostics(status):
    payload = _build_failure(status)
    # Contract validation passes.
    assert schemas.validate_failure_payload(payload) == []
    # Fails closed.
    assert payload["customer_report_generated"] is False
    assert payload["safe_to_show_customer"] is False
    # No vague error: every diagnostic field is populated.
    assert payload["reason_code"].strip()
    assert payload["reason_human"].strip()
    assert payload["troubleshoot_message"].strip()
    assert payload["next_steps"]
    assert isinstance(payload["artifacts_saved"], dict)


def test_failure_builder_rejects_empty_reason():
    with pytest.raises(ValueError):
        job_status.make_failure_status(
            job_id="j",
            analysis_id="a",
            status=JobStatus.FAILED_ANALYSIS,
            current_stage="s",
            reason_code="",  # invalid
            reason_human="x",
            troubleshoot_message="y",
            next_steps=["z"],
        )


def test_validate_failure_payload_detects_unsafe_flags():
    payload = _build_failure(JobStatus.FAILED_ANALYSIS)
    payload["safe_to_show_customer"] = True
    problems = schemas.validate_failure_payload(payload)
    assert "safe_to_show_customer_must_be_false" in problems


def test_all_statuses_defined():
    # Sanity: every status referenced in the spec exists in ALL_STATUSES.
    for name in [
        "QUEUED", "RUNNING", "PDF_QUALITY_OK", "PDF_QUALITY_WARNING",
        "PDF_QUALITY_BLOCKED", "FAILED_ANALYSIS", "FAILED_CONTRACT_BUILD",
        "FAILED_GROUNDING", "NEEDS_MANUAL_REVIEW", "CONTRACT_READY",
        "FAILED_NARRATION_USED_DETERMINISTIC_TEXT", "FAILED_NARRATION_NO_REPORT",
        "JOB_STALLED", "CANCELLED",
    ]:
        assert name in schemas.ALL_STATUSES


def test_sanitize_for_customer_hides_local_paths():
    payload = _build_failure(JobStatus.PDF_QUALITY_BLOCKED)
    payload["artifacts_saved"] = {"job_status": "/srv/perizia/app/_correctness_v2/jobs/x/job_status.json"}
    safe = job_status.sanitize_for_customer(payload)
    assert safe["artifacts_saved"] == {"job_status": "<hidden>"}
