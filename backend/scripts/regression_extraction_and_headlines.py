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
    for item in payload.get("analyses", []):
        if item.get("file_name") == file_name:
            return item.get("analysis_id")
    return None


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
            analysis_id = _post_analysis(base_url, headers)
            _pass(f"Created analysis_id: {analysis_id}")

    detail_url = f"{base_url}/api/analysis/perizia/{analysis_id}"
    resp = requests.get(detail_url, headers=headers, timeout=30)
    if resp.status_code != 200:
        _fail(f"GET detail failed: {resp.status_code} {resp.text[:200]}")
    analysis = resp.json()

    pages_count = int(analysis.get("pages_count", 0) or 0)
    result = analysis.get("result") or {}
    coverage_log = result.get("page_coverage_log") or []
    coverage_ratio = (len(coverage_log) / pages_count) if pages_count else 0.0
    print(f"coverage_ratio={coverage_ratio:.2f} (page_coverage_log={len(coverage_log)}/{pages_count})")

    field_states = result.get("field_states")
    if not isinstance(field_states, dict):
        _fail("field_states missing or not a dict")
    for key in ("tribunale", "procedura", "lotto", "address"):
        if key not in field_states:
            _fail(f"field_states missing key: {key}")
    _pass("field_states contains required keys")

    case_header = result.get("case_header") or {}
    for key in ("procedure_id", "tribunale", "lotto", "address"):
        val = case_header.get(key)
        if not isinstance(val, str):
            _fail(f"case_header.{key} is not a string: {type(val)}")
        if "LOW_CONFIDENCE" in val.upper():
            _fail(f"case_header.{key} contains LOW_CONFIDENCE")
    _pass("case_header headline fields are strings and no LOW_CONFIDENCE leaks")

    html_url = f"{base_url}/api/analysis/perizia/{analysis_id}/html"
    resp = requests.get(html_url, headers=headers, timeout=30)
    if resp.status_code != 200:
        _fail(f"HTML endpoint failed: {resp.status_code} {resp.text[:200]}")
    content_type = resp.headers.get("content-type", "")
    if "text/html" not in content_type.lower():
        _fail(f"HTML content-type mismatch: {content_type}")
    if not resp.content.startswith(b"<!DOCTYPE html"):
        _fail("HTML payload does not start with <!DOCTYPE html")
    _pass("HTML endpoint content-type and payload header OK")

    pdf_url = f"{base_url}/api/analysis/perizia/{analysis_id}/pdf"
    resp = requests.get(pdf_url, headers=headers, timeout=30)
    if resp.status_code != 200:
        _fail(f"PDF endpoint failed: {resp.status_code} {resp.text[:200]}")
    content_type = resp.headers.get("content-type", "")
    if "application/pdf" not in content_type.lower():
        _fail(f"PDF content-type mismatch: {content_type}")
    if not resp.content.startswith(b"%PDF-"):
        _fail("PDF payload does not start with %PDF-")
    _pass("PDF endpoint content-type and payload header OK")

    override_address = "Via Test Override 123, Mantova (MN)"
    patch_url = f"{base_url}/api/analysis/perizia/{analysis_id}/headline"
    resp = requests.patch(patch_url, headers=headers, json={"address": override_address}, timeout=30)
    if resp.status_code != 200:
        _fail(f"PATCH headline failed: {resp.status_code} {resp.text[:200]}")
    _pass("PATCH headline accepted")

    resp = requests.get(detail_url, headers=headers, timeout=30)
    if resp.status_code != 200:
        _fail(f"GET detail after PATCH failed: {resp.status_code} {resp.text[:200]}")
    analysis = resp.json()
    result = analysis.get("result") or {}
    field_states = result.get("field_states") or {}
    addr_state = field_states.get("address") or {}
    if addr_state.get("status") != "USER_PROVIDED":
        _fail(f"address status not USER_PROVIDED: {addr_state.get('status')}")
    if addr_state.get("value") != override_address:
        _fail("address override value not applied in field_states")
    case_header = result.get("case_header") or {}
    report_header = result.get("report_header") or {}
    if case_header.get("address") != override_address:
        _fail("address override not applied in case_header")
    if report_header.get("address", {}).get("value") != override_address:
        _fail("address override not applied in report_header")
    _pass("PATCH override reflected in field_states/case_header/report_header")

    print("PASS: regression_extraction_and_headlines completed")


if __name__ == "__main__":
    main()
