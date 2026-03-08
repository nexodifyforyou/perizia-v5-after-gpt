#!/usr/bin/env python3
import argparse
import os
import sys
from typing import Any, Dict, Optional

import requests


def _fail(message: str) -> None:
    print(f"FAIL: {message}")
    sys.exit(1)


def _pick_analysis_id(
    analysis_id: Optional[str],
    base_url: str,
    session_token: str,
) -> str:
    if analysis_id:
        return analysis_id.strip()
    resp = requests.get(
        f"{base_url.rstrip('/')}/api/history/perizia?limit=20&skip=0",
        headers={"Cookie": f"session_token={session_token}"},
        timeout=30,
    )
    if resp.status_code != 200:
        _fail(f"history lookup failed: {resp.status_code} {resp.text[:180]}")
    analyses = (resp.json() or {}).get("analyses") or []
    if not analyses:
        _fail("history lookup returned no analyses")
    picked = str((analyses[0] or {}).get("analysis_id") or "").strip()
    if not picked:
        _fail("history lookup returned empty analysis_id")
    return picked


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify narrator fields on perizia analysis")
    parser.add_argument("--analysis-id", dest="analysis_id")
    args = parser.parse_args()

    base_url = (os.environ.get("BASE_URL") or "http://127.0.0.1:8081").strip()
    session_token = (
        os.environ.get("SESSION_TOKEN")
        or os.environ.get("session_token")
        or ""
    ).strip()
    if not session_token:
        _fail("SESSION_TOKEN/session_token is required")

    analysis_id = _pick_analysis_id(args.analysis_id, base_url, session_token)
    detail_url = f"{base_url.rstrip('/')}/api/analysis/perizia/{analysis_id}"
    resp = requests.get(detail_url, headers={"Cookie": f"session_token={session_token}"}, timeout=30)
    if resp.status_code != 200:
        _fail(f"GET {detail_url} failed: {resp.status_code} {resp.text[:180]}")

    payload: Dict[str, Any] = resp.json() or {}
    result: Dict[str, Any] = payload.get("result") or {}
    narrator_meta = result.get("narrator_meta")
    if not isinstance(narrator_meta, dict):
        _fail("result.narrator_meta missing or not object")

    narrator_enabled = os.environ.get("NARRATOR_ENABLED", "0").strip() == "1"
    status = str(narrator_meta.get("status") or "").strip()
    if narrator_enabled:
        if status not in {"OK", "FALLBACK"}:
            _fail(f"NARRATOR_ENABLED=1 requires narrator_meta.status in {{OK,FALLBACK}}, got {status!r}")
        if status == "OK":
            narrated = result.get("decision_rapida_narrated")
            if not isinstance(narrated, dict):
                _fail("status OK but decision_rapida_narrated missing")
            refs = narrated.get("evidence_refs")
            if not isinstance(refs, list) or len(refs) < 1:
                _fail("status OK but evidence_refs missing/empty")
    else:
        if status != "SKIPPED":
            _fail(f"NARRATOR_ENABLED!=1 requires narrator_meta.status='SKIPPED', got {status!r}")

    print(f"PASS: verify_narrator analysis_id={analysis_id} status={status}")


if __name__ == "__main__":
    main()
