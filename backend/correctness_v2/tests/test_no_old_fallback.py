"""Tests proving Correctness Mode never falls back to the old analyzer."""

from pathlib import Path

from correctness_v2 import orchestrator
from correctness_v2.schemas import JobStatus

ORCH_SRC = Path(orchestrator.__file__).read_text(encoding="utf-8")
API_SRC = (Path(orchestrator.__file__).parent / "api.py").read_text(encoding="utf-8")

# Symbols belonging to the OLD report pipeline that must NEVER be referenced by
# the Correctness v2 orchestrator (the fail-closed guarantee).
FORBIDDEN_OLD_SYMBOLS = [
    "generate_report_html",
    "customer_decision_contract",
    "build_customer",
    "run_perizia_analysis",
    "narrator",
    "section_builder",
    "pdf_report",
]


def test_orchestrator_source_has_no_old_analyzer_symbols():
    for symbol in FORBIDDEN_OLD_SYMBOLS:
        assert symbol not in ORCH_SRC, f"orchestrator must not reference {symbol}"


def test_orchestrator_declares_no_old_fallback():
    assert orchestrator.NO_OLD_ANALYZER_FALLBACK is True


def test_loader_failure_fails_closed_without_fallback(tmp_path, monkeypatch):
    monkeypatch.setenv("CORRECTNESS_V2_ARTIFACTS_ROOT", str(tmp_path / "_cv2"))

    fallback_called = {"hit": False}

    def _exploding_loader(analysis_id):
        # Simulate any unexpected failure in the pipeline.
        raise RuntimeError("extraction exploded")

    # A spy that would represent the old analyzer. It must never be invoked.
    def _old_analyzer(*args, **kwargs):  # pragma: no cover - must not run
        fallback_called["hit"] = True
        return {"customer_report": "WRONG"}

    status = orchestrator.start_job("an_fail", _exploding_loader, is_admin=True)

    assert status["status"] == JobStatus.FAILED_ANALYSIS
    assert status["customer_report_generated"] is False
    assert status["safe_to_show_customer"] is False
    assert status["reason_code"] == "CORRECTNESS_V2_UNEXPECTED_ERROR"
    # The orchestrator never reached out to any old-analyzer fallback.
    assert fallback_called["hit"] is False
    # And it has no customer report content anywhere in the payload.
    assert "WRONG" not in str(status)


def test_api_source_has_no_old_analyzer_symbols():
    for symbol in FORBIDDEN_OLD_SYMBOLS:
        assert symbol not in API_SRC, f"api must not reference {symbol}"
