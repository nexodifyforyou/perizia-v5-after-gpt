#!/usr/bin/env python3
import json
import os
import sys
import urllib.error
import urllib.request


BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:8081").rstrip("/")
SESSION_TOKEN = os.environ.get("SESSION_TOKEN") or os.environ.get("session_token")
ANALYSIS_ID = os.environ.get("ANALYSIS_ID") or os.environ.get("analysis_id")


def _fetch(url: str, headers: dict | None = None) -> tuple[int, dict, bytes]:
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=20) as resp:
        body = resp.read()
        hdrs = {k.lower(): v for k, v in resp.headers.items()}
        return resp.status, hdrs, body


def _fail(msg: str) -> None:
    print(f"FAIL: {msg}")
    sys.exit(1)


def _pass(msg: str) -> None:
    print(f"PASS: {msg}")


def main() -> None:
    try:
        status, _, openapi_raw = _fetch(f"{BASE_URL}/openapi.json")
    except Exception as e:
        _fail(f"cannot fetch openapi.json: {e}")
        return
    if status != 200:
        _fail(f"openapi.json status {status}")
    try:
        spec = json.loads(openapi_raw.decode("utf-8"))
    except Exception as e:
        _fail(f"openapi.json not valid JSON: {e}")
        return

    target_path = "/api/analysis/perizia/{analysis_id}"
    if target_path not in (spec.get("paths") or {}):
        _fail(f"path missing in openapi: {target_path}")
    _pass(f"openapi contains {target_path}")

    if not SESSION_TOKEN:
        _fail("SESSION_TOKEN (or session_token) env is required")
    if not ANALYSIS_ID:
        _fail("ANALYSIS_ID (or analysis_id) env is required")

    headers = {"Cookie": f"session_token={SESSION_TOKEN}"}

    html_url = f"{BASE_URL}/api/analysis/perizia/{ANALYSIS_ID}/html"
    try:
        status, html_headers, html_bytes = _fetch(html_url, headers=headers)
    except urllib.error.HTTPError as e:
        _fail(f"html endpoint HTTP {e.code}")
        return
    except Exception as e:
        _fail(f"html endpoint error: {e}")
        return
    if status != 200:
        _fail(f"html endpoint status {status}")
    html_content_type = html_headers.get("content-type", "")
    if "text/html" not in html_content_type.lower():
        _fail(f"html content-type mismatch: {html_content_type}")
    if not html_bytes.startswith(b"<!DOCTYPE html"):
        _fail("html body does not start with <!DOCTYPE html")
    _pass("html endpoint content-type is text/html and payload starts with <!DOCTYPE html")

    pdf_url = f"{BASE_URL}/api/analysis/perizia/{ANALYSIS_ID}/pdf"
    try:
        status, pdf_headers, pdf_bytes = _fetch(pdf_url, headers=headers)
    except urllib.error.HTTPError as e:
        _fail(f"pdf endpoint HTTP {e.code}")
        return
    except Exception as e:
        _fail(f"pdf endpoint error: {e}")
        return
    if status != 200:
        _fail(f"pdf endpoint status {status}")
    pdf_content_type = pdf_headers.get("content-type", "")
    if "application/pdf" not in pdf_content_type.lower():
        _fail(f"pdf content-type mismatch: {pdf_content_type}")
    if not pdf_bytes.startswith(b"%PDF-"):
        _fail("pdf body does not start with %PDF-")
    _pass("pdf endpoint content-type is application/pdf and payload starts with %PDF-")

    print("PASS: regression_endpoints completed")


if __name__ == "__main__":
    main()
