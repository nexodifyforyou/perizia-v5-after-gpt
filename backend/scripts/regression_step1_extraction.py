#!/usr/bin/env python3
import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import requests


def _fail(msg: str) -> None:
    print(f"FAIL: {msg}")
    sys.exit(1)


def _get_latest_analysis_id(base_url: str, token: str, file_name: Optional[str]) -> str:
    headers = {"Cookie": f"session_token={token}"}
    url = f"{base_url.rstrip('/')}/api/history/perizia?limit=50&skip=0"
    resp = requests.get(url, headers=headers, timeout=30)
    if resp.status_code != 200:
        _fail(f"history lookup failed ({resp.status_code}): {resp.text[:180]}")
    payload = resp.json()
    analyses = payload.get("analyses") or []
    if file_name:
        analyses = [a for a in analyses if (a.get("file_name") or "") == file_name]
    if not analyses:
        if file_name:
            _fail(f"No analyses found in history for file_name={file_name}")
        _fail("No analyses found in history")
    return analyses[0].get("analysis_id") or ""


def _parse_out_folder(stdout: str) -> Optional[str]:
    match = re.search(r"^OUT_FOLDER:\s*(.+)\s*$", stdout, re.MULTILINE)
    return match.group(1).strip() if match else None


def main() -> None:
    parser = argparse.ArgumentParser(description="Regression wrapper for Step1 extraction sanity.")
    parser.add_argument("--analysis-id", dest="analysis_id")
    parser.add_argument("--file-name", dest="file_name")
    parser.add_argument("--out")
    args = parser.parse_args()

    analysis_id = args.analysis_id
    if not analysis_id:
        base_url = os.environ.get("BASE_URL")
        token = os.environ.get("SESSION_TOKEN") or os.environ.get("session_token")
        if not base_url or not token:
            _fail("Provide --analysis-id for offline mode, or set BASE_URL + SESSION_TOKEN/session_token")
        analysis_id = _get_latest_analysis_id(base_url, token.strip(), args.file_name)
        if not analysis_id:
            _fail("Latest analysis_id was empty")
        print(f"Using latest analysis_id from history: {analysis_id}")

    script_path = Path(__file__).resolve().parent / "step1_extraction_sanity.py"
    if not script_path.exists():
        _fail(f"Missing script: {script_path}")

    cmd = [sys.executable, str(script_path), "--analysis-id", analysis_id]
    if args.out:
        cmd.extend(["--out", args.out])

    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.stdout:
        print(proc.stdout, end="" if proc.stdout.endswith("\n") else "\n")
    if proc.stderr:
        print(proc.stderr, end="" if proc.stderr.endswith("\n") else "\n")
    if proc.returncode != 0:
        _fail(f"step1_extraction_sanity.py failed with exit code {proc.returncode}")

    out_folder = _parse_out_folder(proc.stdout or "")
    if not out_folder:
        _fail("Could not parse OUT_FOLDER from step1 output")

    out_path = Path(out_folder)
    metrics_path = out_path / "metrics.json"
    anchors_path = out_path / "anchors.json"
    if not metrics_path.exists() or not anchors_path.exists():
        _fail(f"Missing artifacts in {out_path} (metrics.json and/or anchors.json)")

    metrics: Dict[str, Any] = json.loads(metrics_path.read_text(encoding="utf-8"))
    anchors = json.loads(anchors_path.read_text(encoding="utf-8"))
    anchors_by_name = {a.get("name"): a for a in anchors if isinstance(a, dict)}

    tribunale_ok = bool((anchors_by_name.get("tribunale") or {}).get("page"))
    coverage_ok = float(metrics.get("coverage_ratio", 0.0) or 0.0) >= 0.95
    if not tribunale_ok:
        _fail("anchors.tribunale not found in anchors.json")
    if not coverage_ok:
        _fail(f"coverage_ratio below threshold: {metrics.get('coverage_ratio')}")

    print(f"OUT_FOLDER_FINAL: {out_path}")
    print("PASS: regression_step1_extraction")


if __name__ == "__main__":
    main()
