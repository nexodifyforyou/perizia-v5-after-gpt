#!/usr/bin/env python3
import json
import os
import sys
from getpass import getpass
from pathlib import Path

import requests


PDF_PATH = Path("/srv/perizia/app/uploads/1859886_c_perizia.pdf")


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
    if not PDF_PATH.exists():
        _fail(f"PDF not found at {PDF_PATH}")
    with PDF_PATH.open("rb") as f:
        files = {"file": (PDF_PATH.name, f, "application/pdf")}
        resp = requests.post(f"{base_url}/api/analysis/perizia", headers=headers, files=files, timeout=180)
    if resp.status_code != 200:
        _fail(f"POST /analysis/perizia failed: {resp.status_code} {resp.text[:200]}")
    payload = resp.json()
    analysis_id = payload.get("analysis_id")
    if not analysis_id:
        _fail("POST response missing analysis_id")
    return analysis_id


def _assert_evidence_entry(entry: dict) -> None:
    for key in ("page", "quote", "start_offset", "end_offset", "offset_mode"):
        if key not in entry:
            _fail(f"evidence entry missing {key}: {entry}")
    page = entry.get("page")
    if page is None or not isinstance(page, int):
        _fail(f"evidence entry page invalid: {entry}")
    quote = entry.get("quote")
    if not isinstance(quote, str) or not quote.strip():
        _fail(f"evidence entry quote empty: {entry}")

def _assert_searched_entries(entries: list, context: str) -> None:
    if not isinstance(entries, list):
        _fail(f"{context} searched_in is not a list: {type(entries)}")
    if not entries:
        _fail(f"{context} searched_in is empty")
    for entry in entries:
        if not isinstance(entry, dict):
            _fail(f"{context} searched_in entry is not a dict: {entry}")
        _assert_evidence_entry(entry)


def _normalize_lotto_value(text: str) -> str | None:
    import re
    if not text:
        return None
    cleaned = " ".join(str(text).split())
    if re.search(r"\\bLOTTO\\s+UNICO\\b", cleaned, re.I):
        return "Lotto Unico"
    match = re.search(r"\\bLOTTI?\\s+([0-9]+(?:\\s*[,/-]\\s*[0-9]+)*)", cleaned, re.I)
    if match:
        nums = re.findall(r"\\d+", match.group(1))
        if len(nums) == 1:
            return f"Lotto {nums[0]}"
        if len(nums) > 1:
            return "Lotti " + ", ".join(nums)
    return None


def main() -> None:
    base_url = _get_base_url()
    session_token = _get_session_token()
    if not session_token:
        _fail("session_token is required")

    headers = {"Cookie": f"session_token={session_token}"}
    analysis_id = os.environ.get("ANALYSIS_ID") or os.environ.get("analysis_id")
    if analysis_id:
        analysis_id = analysis_id.strip()
    if not analysis_id:
        analysis_id = _history_lookup(base_url, headers, PDF_PATH.name)
        if analysis_id:
            _pass(f"Using existing analysis_id: {analysis_id}")
        else:
            _fail(f"No existing analysis found for {PDF_PATH.name}")

    detail_url = f"{base_url}/api/analysis/perizia/{analysis_id}"
    resp = requests.get(detail_url, headers=headers, timeout=30)
    if resp.status_code != 200:
        _fail(f"GET detail failed: {resp.status_code} {resp.text[:200]}")
    analysis = resp.json()

    result = analysis.get("result") or {}
    field_states = result.get("field_states")
    if not isinstance(field_states, dict):
        _fail("field_states missing or not a dict")

    headline_keys = ("tribunale", "procedura", "lotto", "address")
    decision_keys = (
        "prezzo_base_asta",
        "superficie",
        "diritto_reale",
        "stato_occupativo",
        "regolarita_urbanistica",
        "conformita_catastale",
        "spese_condominiali_arretrate",
        "formalita_pregiudizievoli",
    )
    for key in headline_keys + decision_keys:
        if key not in field_states:
            _fail(f"field_states missing key: {key}")
    _pass("field_states contains headline + decision keys")

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
    _pass("case_header/report_header headline values are strings and no LOW_CONFIDENCE leaks")

    allowed = {"FOUND", "NOT_FOUND", "LOW_CONFIDENCE", "USER_PROVIDED"}
    for key in headline_keys + decision_keys:
        state = field_states.get(key) or {}
        status = state.get("status")
        if status not in allowed:
            _fail(f"{key} status invalid: {status}")
        if status == "FOUND":
            ev = state.get("evidence") or []
            if not ev:
                _fail(f"{key} FOUND without evidence")
            _assert_evidence_entry(ev[0])
            searched = state.get("searched_in")
            if searched != []:
                _fail(f"{key} FOUND searched_in not empty list: {searched}")
        if status in {"NOT_FOUND", "LOW_CONFIDENCE"}:
            searched = state.get("searched_in")
            _assert_searched_entries(searched, f"{key} {status}")
        if status == "USER_PROVIDED":
            searched = state.get("searched_in")
            if searched != []:
                _fail(f"{key} USER_PROVIDED searched_in not empty list: {searched}")
    _pass("field_states have valid statuses and evidence/search proofs")

    lotto_state = field_states.get("lotto") or {}
    if lotto_state.get("status") == "FOUND":
        ev = lotto_state.get("evidence") or []
        if ev:
            normalized_ev = _normalize_lotto_value(ev[0].get("quote"))
            normalized_val = _normalize_lotto_value(lotto_state.get("value"))
            if normalized_ev and normalized_val and normalized_ev != normalized_val:
                _fail(f"lotto value mismatch: value={lotto_state.get('value')} evidence={ev[0].get('quote')}")
            if "lotto unico" in str(ev[0].get("quote") or "").lower():
                if "unico" not in str(lotto_state.get("value") or "").lower():
                    _fail(f"lotto value missing 'Unico': value={lotto_state.get('value')} evidence={ev[0].get('quote')}")
    _pass("lotto value matches evidence normalization when FOUND")

    override_value = 123456
    patch_url = f"{base_url}/api/analysis/perizia/{analysis_id}/overrides"
    resp = requests.patch(patch_url, headers=headers, json={"prezzo_base_asta": override_value}, timeout=30)
    if resp.status_code != 200:
        _fail(f"PATCH overrides failed: {resp.status_code} {resp.text[:200]}")
    _pass("PATCH overrides accepted")

    resp = requests.get(detail_url, headers=headers, timeout=30)
    if resp.status_code != 200:
        _fail(f"GET detail after PATCH failed: {resp.status_code} {resp.text[:200]}")
    analysis = resp.json()
    result = analysis.get("result") or {}
    field_states = result.get("field_states") or {}
    price_state = field_states.get("prezzo_base_asta") or {}
    if price_state.get("status") != "USER_PROVIDED":
        _fail(f"prezzo_base_asta status not USER_PROVIDED: {price_state.get('status')}")
    value = price_state.get("value")
    if value not in (override_value, float(override_value)):
        _fail(f"prezzo_base_asta override value not applied: {value}")
    _pass("PATCH override reflected in field_states")

    print("PASS: regression_field_states completed")


if __name__ == "__main__":
    main()
