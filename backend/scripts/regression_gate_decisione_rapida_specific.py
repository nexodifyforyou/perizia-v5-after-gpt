#!/usr/bin/env python3
import argparse
import os
import sys
from typing import Any, Dict, List

import requests


GENERIC_PATTERNS = [
    "alcuni dati richiedono verifica manuale",
    "consultare un professionista prima di procedere",
    "document analyzed. some data requires manual verification",
]


def _fail(msgs: List[str]) -> None:
    print("FAIL:")
    for m in msgs:
        print(f"- {m}")
    sys.exit(1)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--analysis-id", required=True)
    args = ap.parse_args()

    base_url = (os.getenv("BASE_URL") or "").strip()
    session_token = (os.getenv("SESSION_TOKEN") or os.getenv("session_token") or "").strip()
    if not base_url or not session_token:
        _fail(["BASE_URL and SESSION_TOKEN are required"])

    url = f"{base_url.rstrip('/')}/api/analysis/perizia/{args.analysis_id}"
    headers = {"Cookie": f"session_token={session_token}"}
    if os.getenv("OFFLINE_QA") == "1":
        headers["X-OFFLINE-QA"] = "1"
        headers["X-OFFLINE-QA-TOKEN"] = os.getenv("OFFLINE_QA_TOKEN", "")
    res = requests.get(url, headers=headers, timeout=60)
    if res.status_code != 200:
        _fail([f"GET {url} -> {res.status_code}"])
    payload = res.json()
    result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    if not isinstance(result, dict):
        _fail(["API response missing result"])

    decision = result.get("decision_rapida_client") or result.get("section_2_decisione_rapida") or {}
    if not isinstance(decision, dict):
        _fail(["decision block missing"])
    semaforo = result.get("semaforo_generale") or result.get("section_1_semaforo_generale") or {}

    summary_it = str(decision.get("summary_it") or "").strip()
    summary_en = str(decision.get("summary_en") or "").strip()
    disclaimer = str((result.get("summary_for_client") or {}).get("disclaimer_it") or "").strip()
    blockers = semaforo.get("top_blockers") if isinstance(semaforo, dict) else []
    blockers = blockers if isinstance(blockers, list) else []
    blocker_labels = [str(b.get("label_it") or b.get("key") or "").strip().lower() for b in blockers if isinstance(b, dict)]
    blocker_labels = [x for x in blocker_labels if x]

    failures: List[str] = []
    for pat in GENERIC_PATTERNS:
        if pat in summary_it.lower() or pat in summary_en.lower():
            failures.append("Decisione Rapida is generic boilerplate")
            break

    mentions = 0
    for lbl in blocker_labels[:5]:
        if lbl and lbl in summary_it.lower():
            mentions += 1
    required_mentions = min(2, len(blocker_labels))
    if mentions < required_mentions:
        failures.append(f"summary_it must mention at least {required_mentions} real blockers/critical fields")

    if not summary_it:
        failures.append("summary_it missing")
    if not summary_en:
        failures.append("summary_en missing")
    if not disclaimer:
        failures.append("disclaimer_it missing (must be separate line in summary_for_client)")

    if failures:
        _fail(failures)

    print("PASS: regression_gate_decisione_rapida_specific")
    print(f"IT: {summary_it}")
    print(f"EN: {summary_en}")
    print(f"DISCLAIMER: {disclaimer}")


if __name__ == "__main__":
    main()
