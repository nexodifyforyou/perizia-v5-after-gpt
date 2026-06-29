"""Tests for the generic analyst worksheet generation + normalization."""

import json

from correctness_v2 import analyst
from correctness_v2.analyst import AnalystError

from .sample_perizia import (
    GENERIC_PERIZIA_PAGES,
    base_raw_worksheet,
    fake_caller_raising,
    fake_caller_returning,
)


def test_to_number_handles_italian_format():
    assert analyst._to_number("43.654,20") == 43654.20
    assert analyst._to_number("€ 5.000,00") == 5000.0
    assert analyst._to_number("38110.20") == 38110.20
    assert analyst._to_number(294) == 294.0
    assert analyst._to_number(None) is None
    assert analyst._to_number("") is None


def test_normalize_coerces_enums_and_shape():
    raw = base_raw_worksheet()
    raw["technical_compliance"][0]["classification"] = "TOTALLY_BOGUS"
    ws = analyst.normalize_worksheet(raw)
    assert ws["schema_version"] == analyst.WORKSHEET_SCHEMA_VERSION
    # Unknown classification falls back to 'uncertain'.
    assert ws["technical_compliance"][0]["classification"] == "uncertain"
    assert ws["money"]["market_value"] == 100000.0
    assert isinstance(ws["legal_formalities"], list)


def test_run_analyst_with_fake_caller_normalizes():
    caller = fake_caller_returning(base_raw_worksheet())
    result = analyst.run_analyst(GENERIC_PERIZIA_PAGES, openai_caller=caller, model="m")
    assert result.worksheet["case_identity"]["tribunale"] == "Tribunale di Esempio"
    assert result.redacted_request["api_key"] == "<omitted>"
    assert result.response_artifact["raw_content"]
    assert len(caller.calls) == 1


def test_run_analyst_propagates_openai_failure():
    caller = fake_caller_raising("OPENAI_CALL_FAILED")
    try:
        analyst.run_analyst(GENERIC_PERIZIA_PAGES, openai_caller=caller, model="m")
        assert False, "expected AnalystError"
    except AnalystError as exc:
        assert exc.reason_code == "OPENAI_CALL_FAILED"


def test_run_analyst_rejects_non_json():
    def bad_caller(messages, *, model=None, timeout=None):
        return {"content": "this is not json", "model": "m"}

    try:
        analyst.run_analyst(GENERIC_PERIZIA_PAGES, openai_caller=bad_caller, model="m")
        assert False, "expected AnalystError"
    except AnalystError as exc:
        assert exc.reason_code == "ANALYST_JSON_INVALID"


def test_build_messages_includes_page_markers():
    messages = analyst.build_messages(GENERIC_PERIZIA_PAGES)
    user = messages[-1]["content"]
    assert "=== PAGINA 1 ===" in user
    assert "=== PAGINA 2 ===" in user
