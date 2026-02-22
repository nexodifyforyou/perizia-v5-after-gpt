#!/usr/bin/env python3
import argparse
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import List

import requests


def _fail_many(reasons: List[str]) -> None:
    if not reasons:
        reasons = ["unknown failure"]
    print("FAIL:")
    for reason in reasons:
        print(f"- {reason}")
    sys.exit(1)


def _run_pdftotext(pdf_path: Path, txt_path: Path) -> None:
    try:
        subprocess.run(
            ["pdftotext", "-raw", str(pdf_path), str(txt_path)],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except FileNotFoundError:
        _fail_many(["pdftotext is required on PATH"])
    except subprocess.CalledProcessError as exc:
        msg = (exc.stderr or exc.stdout or "").strip()
        _fail_many([f"pdftotext failed: {msg[:200]}"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Regression Step4: PDF content checks.")
    parser.add_argument("--analysis-id", dest="analysis_id", default="analysis_51bae75af061")
    args = parser.parse_args()

    base_url = (os.environ.get("BASE_URL") or "").strip()
    session_token = (
        os.environ.get("SESSION_TOKEN")
        or os.environ.get("session_token")
        or ""
    ).strip()

    if not base_url:
        _fail_many(["BASE_URL is required (prod API endpoint)"])
    if not session_token:
        _fail_many(["SESSION_TOKEN/session_token is required for API access"])

    analysis_id = args.analysis_id.strip() if args.analysis_id else "analysis_51bae75af061"

    headers = {"Cookie": f"session_token={session_token}"}
    pdf_url = f"{base_url.rstrip('/')}/api/analysis/perizia/{analysis_id}/pdf"
    resp = requests.get(pdf_url, headers=headers, timeout=120)
    if resp.status_code != 200:
        _fail_many([f"GET {pdf_url} failed: {resp.status_code} {resp.text[:180]}"])

    out_dir = Path("/tmp/perizia_pdf").resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = out_dir / f"{analysis_id}.pdf"
    txt_path = out_dir / f"{analysis_id}.txt"
    pdf_path.write_bytes(resp.content)

    _run_pdftotext(pdf_path, txt_path)
    text = txt_path.read_text(encoding="utf-8", errors="ignore")

    failures: List[str] = []

    if "Money Box" not in text:
        failures.append("Money Box header missing")
    if "Legal Killers" not in text:
        failures.append("Legal Killers header missing")
    if "Decisione Rapida" not in text:
        failures.append("Decisione Rapida header missing")

    if "TBD" in text:
        total_lines = re.findall(r"Totale\s*\(min\)[^\n]*", text, flags=re.IGNORECASE)
        if not total_lines:
            failures.append("Totale (min) line missing")
        else:
            total_line = total_lines[0]
            if "TBD" not in total_line.upper():
                failures.append(f"Totale (min) is not TBD when TBD items exist: {total_line.strip()}")
            if re.search(r"\b0\b", total_line) and "TBD" not in total_line.upper():
                failures.append(f"Totale (min) shows 0 with TBD items: {total_line.strip()}")

    if analysis_id == "analysis_51bae75af061" and "Usi civici" not in text:
        failures.append("Usi civici missing in Legal Killers for analysis_51bae75af061")

    if "RISCHIO" not in text.upper():
        failures.append("Decisione Rapida missing RISCHIO label")

    if failures:
        _fail_many(failures)

    print(f"PASS: regression_step4_pdf_content analysis_id={analysis_id}")


if __name__ == "__main__":
    main()
