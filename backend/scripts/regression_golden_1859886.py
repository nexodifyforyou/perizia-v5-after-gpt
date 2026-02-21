#!/usr/bin/env python3
import json
import os
import sys
from getpass import getpass
from pathlib import Path

import requests

FIXTURE_PATH = Path("/srv/perizia/app/backend/tests/fixtures/perizie/1859886_c_perizia.pdf")
EXPECTED_PATH = Path("/srv/perizia/app/backend/tests/golden/1859886_expected.json")


def _fail(msg: str) -> None:
    print(f"FAIL: {msg}")
    sys.exit(1)


def _pass(msg: str) -> None:
    print(f"PASS: {msg}")


def _get_base_url() -> str:
    base_url = os.environ.get("BASE_URL")
    if not base_url:
        base_url = input("Base URL [http://127.0.0.1:8081]: ").strip() or "http://127.0.0.1:8081"
    return base_url.rstrip("/")


def _get_session_token() -> str:
    token = os.environ.get("SESSION_TOKEN") or os.environ.get("session_token")
    if token:
        return token.strip()
    return getpass("Paste session_token: ").strip()


def _history_lookup(base_url: str, headers: dict, file_name: str) -> str | None:
    url = f"{base_url}/api/history/perizia?limit=50&skip=0"
    resp = requests.get(url, headers=headers, timeout=30)
    if resp.status_code != 200:
        return None
    payload = resp.json()
    latest_id = None
    latest_ts = None
    for item in payload.get("analyses", []):
        if item.get("file_name") != file_name:
            continue
        raw_ts = item.get("created_at") or item.get("created_at_utc") or item.get("created_at_iso")
        ts = None
        if isinstance(raw_ts, str):
            try:
                ts = raw_ts.replace("Z", "+00:00")
                ts = __import__("datetime").datetime.fromisoformat(ts)
            except Exception:
                ts = None
        if latest_ts is None or (ts is not None and ts > latest_ts):
            latest_ts = ts
            latest_id = item.get("analysis_id")
        if latest_ts is None and latest_id is None:
            latest_id = item.get("analysis_id")
    return latest_id


def _post_analysis(base_url: str, headers: dict) -> str:
    if not FIXTURE_PATH.exists():
        _fail(f"Fixture PDF not found at {FIXTURE_PATH}")
    with FIXTURE_PATH.open("rb") as f:
        files = {"file": (FIXTURE_PATH.name, f, "application/pdf")}
        resp = requests.post(f"{base_url}/api/analysis/perizia", headers=headers, files=files, timeout=180)
    if resp.status_code != 200:
        _fail(f"POST /analysis/perizia failed: {resp.status_code} {resp.text[:200]}")
    payload = resp.json()
    analysis_id = payload.get("analysis_id")
    if not analysis_id:
        _fail("POST response missing analysis_id")
    return analysis_id


def _assert_evidence_entry(entry: dict, context: str) -> None:
    for key in ("page", "quote", "start_offset", "end_offset", "offset_mode"):
        if key not in entry:
            _fail(f"{context} entry missing {key}: {entry}")
    page = entry.get("page")
    if page is None or not isinstance(page, int):
        _fail(f"{context} entry page invalid: {entry}")
    quote = entry.get("quote")
    if not isinstance(quote, str) or not quote.strip():
        _fail(f"{context} entry quote empty: {entry}")


def _normalize_text(value: str) -> str:
    return " ".join(str(value).split()).strip()

def _collapse_spaced_letters(tokens: list[str]) -> list[str]:
    collapsed = []
    buffer = []
    for token in tokens:
        if len(token) == 1 and token.isalpha():
            buffer.append(token)
            continue
        if buffer:
            collapsed.append("".join(buffer))
            buffer = []
        collapsed.append(token)
    if buffer:
        collapsed.append("".join(buffer))
    return collapsed


def _normalize_evidence_text(text: str) -> str:
    tokens = str(text).replace("\n", " ").split()
    tokens = _collapse_spaced_letters(tokens)
    return " ".join(tokens).strip()


def _coerce_numeric(value):
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, dict) and "value" in value:
        return _coerce_numeric(value.get("value"))
    if isinstance(value, str):
        raw = value.replace("€", "").replace("EUR", "").replace("Euro", "").strip()
        raw = raw.replace(" ", "")
        if "," in raw and "." in raw:
            raw = raw.replace(".", "").replace(",", ".")
        elif "," in raw:
            raw = raw.replace(".", "").replace(",", ".")
        else:
            raw = raw.replace(".", "")
        try:
            return float(raw)
        except Exception:
            return None
    return None


def _value_matches(expected, actual) -> bool:
    if isinstance(expected, (int, float)):
        actual_num = _coerce_numeric(actual)
        if actual_num is None:
            return False
        return abs(actual_num - float(expected)) < 0.01
    actual_val = actual
    if isinstance(actual_val, dict) and "value" in actual_val:
        actual_val = actual_val.get("value")
    if actual_val is None:
        return False
    return _normalize_text(expected) == _normalize_text(actual_val)


def _evidence_contains(evidence: list, page: int, substring: str) -> bool:
    target = substring.lower()
    target_norm = _normalize_evidence_text(substring).lower()
    target_compact = "".join(target_norm.split())
    for ev in evidence or []:
        if not isinstance(ev, dict):
            continue
        if ev.get("page") != page:
            continue
        quote = str(ev.get("quote") or "").lower()
        if target in quote:
            return True
        quote_norm = _normalize_evidence_text(ev.get("quote") or "").lower()
        if target_norm and target_norm in quote_norm:
            return True
        quote_compact = "".join(quote_norm.split())
        if target_compact and target_compact in quote_compact:
            return True
    return False


def main() -> None:
    if not EXPECTED_PATH.exists():
        _fail(f"Expected golden file missing: {EXPECTED_PATH}")

    base_url = _get_base_url()
    session_token = _get_session_token()
    if not session_token:
        _fail("session_token is required")

    headers = {"Cookie": f"session_token={session_token}"}
    analysis_id = os.environ.get("ANALYSIS_ID") or os.environ.get("analysis_id")
    if analysis_id:
        analysis_id = analysis_id.strip()
    if not analysis_id:
        analysis_id = _history_lookup(base_url, headers, FIXTURE_PATH.name)
        if analysis_id:
            _pass(f"Using existing analysis_id: {analysis_id}")
        else:
            analysis_id = _post_analysis(base_url, headers)
            _pass(f"Uploaded fixture for analysis_id: {analysis_id}")

    detail_url = f"{base_url}/api/analysis/perizia/{analysis_id}"
    resp = requests.get(detail_url, headers=headers, timeout=30)
    if resp.status_code != 200:
        _fail(f"GET detail failed: {resp.status_code} {resp.text[:200]}")
    analysis = resp.json()

    result = analysis.get("result") or {}
    field_states = result.get("field_states")
    if not isinstance(field_states, dict):
        _fail("field_states missing or not a dict")

    case_header = result.get("case_header") or {}
    report_header = result.get("report_header") or {}
    for key in ("procedure_id", "tribunale", "lotto", "address"):
        val = case_header.get(key)
        if not isinstance(val, str):
            _fail(f"case_header.{key} is not a string: {type(val)}")
        if "LOW_CONFIDENCE" in val.upper():
            _fail(f"case_header.{key} contains LOW_CONFIDENCE")
    for key in ("procedure", "tribunale", "lotto", "address"):
        val = report_header.get(key, {}).get("value") if isinstance(report_header.get(key), dict) else report_header.get(key)
        if not isinstance(val, str):
            _fail(f"report_header.{key} value is not a string: {type(val)}")
        if "LOW_CONFIDENCE" in val.upper():
            _fail(f"report_header.{key} contains LOW_CONFIDENCE")
    _pass("headline values contain no LOW_CONFIDENCE leaks")

    allowed = {"FOUND", "NOT_FOUND", "LOW_CONFIDENCE", "USER_PROVIDED"}
    for key, state in field_states.items():
        if not isinstance(state, dict):
            _fail(f"{key} state is not a dict")
        status = state.get("status")
        if status not in allowed:
            _fail(f"{key} status invalid: {status}")
        if status == "FOUND":
            ev = state.get("evidence") or []
            if not ev:
                _fail(f"{key} FOUND without evidence")
            _assert_evidence_entry(ev[0], f"{key} evidence")
            if state.get("searched_in") != []:
                _fail(f"{key} FOUND searched_in not empty list: {state.get('searched_in')}")
        if status in {"NOT_FOUND", "LOW_CONFIDENCE"}:
            searched = state.get("searched_in")
            if not isinstance(searched, list) or not searched:
                _fail(f"{key} {status} without searched_in")
            for entry in searched:
                _assert_evidence_entry(entry, f"{key} searched_in")
        if status == "USER_PROVIDED":
            if state.get("searched_in") != []:
                _fail(f"{key} USER_PROVIDED searched_in not empty list: {state.get('searched_in')}")
    _pass("field_states invariants satisfied")

    lotto_state = field_states.get("lotto") or {}
    ev = lotto_state.get("evidence") or []
    if ev:
        quote = str(ev[0].get("quote") or "").lower()
        if "lotto unico" in quote and "unico" not in str(lotto_state.get("value") or "").lower():
            _fail(f"lotto value missing 'Unico': value={lotto_state.get('value')} evidence={ev[0].get('quote')}")
    _pass("lotto evidence/value consistency")

    with EXPECTED_PATH.open("r", encoding="utf-8") as f:
        expected = json.load(f)

    for key, expectation in expected.items():
        state = field_states.get(key) or {}
        status = state.get("status")
        expected_statuses = expectation.get("expected_statuses") or ["FOUND", "USER_PROVIDED"]
        if status not in expected_statuses:
            _fail(f"{key} status {status} not in expected {expected_statuses}")
        actual_value = state.get("value")
        if not _value_matches(expectation.get("expected_value"), actual_value):
            _fail(f"{key} value mismatch: expected={expectation.get('expected_value')} actual={actual_value}")
        evidence = state.get("evidence") or []
        if not _evidence_contains(evidence, expectation.get("expected_page"), expectation.get("expected_quote_substring")):
            _fail(f"{key} evidence missing page {expectation.get('expected_page')} substring {expectation.get('expected_quote_substring')}")
    _pass("golden expectations satisfied")

    print("PASS: regression_golden_1859886 completed")


if __name__ == "__main__":
    main()
