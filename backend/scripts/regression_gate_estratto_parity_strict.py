#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

import requests


def _fail(msgs: List[str]) -> None:
    print("FAIL:")
    for m in msgs:
        print(f"- {m}")
    sys.exit(1)


def _missing(v: Any) -> bool:
    if v is None:
        return True
    s = str(v).strip().upper()
    return s in {"", "NOT_FOUND", "TBD", "NONE", "NON SPECIFICATO IN PERIZIA"}


def _eq(a: Any, b: Any) -> bool:
    syn = {
        "ASSENTE": "NON PRESENTE",
        "NON PRESENTE": "NON PRESENTE",
        "PRESENTE": "PRESENTE",
    }
    ua = str(a).strip().upper()
    ub = str(b).strip().upper()
    if ua in syn and ub in syn:
        return syn[ua] == syn[ub]
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return abs(float(a) - float(b)) < 0.01
    sa = " ".join(str(a).lower().split())
    sb = " ".join(str(b).lower().split())
    return sa == sb or sa in sb or sb in sa


def _field_state(result: Dict[str, Any], key: str) -> Dict[str, Any]:
    states = result.get("field_states", {})
    return states.get(key, {}) if isinstance(states, dict) else {}


def _backend_values(result: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    money = result.get("money_box", {}) if isinstance(result.get("money_box"), dict) else {}
    item_a = {}
    for it in money.get("items", []) if isinstance(money.get("items"), list) else []:
        if str(it.get("code")) == "A":
            item_a = it
            break
    beni = result.get("beni")
    if not isinstance(beni, list):
        lots = result.get("lots", [])
        if isinstance(lots, list) and lots and isinstance(lots[0], dict):
            beni = lots[0].get("beni")
    if not isinstance(beni, list):
        beni = []

    dati_asta = result.get("dati_asta", {})
    return {
        "tribunale": {"value": _field_state(result, "tribunale").get("value") or result.get("case_header", {}).get("tribunale"), "state": _field_state(result, "tribunale")},
        "procedure_id": {"value": _field_state(result, "procedura").get("value") or result.get("case_header", {}).get("procedure_id"), "state": _field_state(result, "procedura")},
        "occupancy": {"value": _field_state(result, "stato_occupativo").get("value"), "state": _field_state(result, "stato_occupativo")},
        "beni_count": {
            "value": len(beni),
            "state": {
                "status": "FOUND" if beni else "NOT_FOUND",
                "evidence": (
                    (beni[0].get("evidence", {}).get("tipologia", []) if isinstance(beni[0], dict) else [])
                    if beni else []
                ),
            },
        },
        "ape_status": {"value": _field_state(result, "ape").get("value"), "state": _field_state(result, "ape")},
        "spese_condominiali_arretrate": {"value": _field_state(result, "spese_condominiali_arretrate").get("value"), "state": _field_state(result, "spese_condominiali_arretrate")},
        "sanatoria_estimate_eur": {"value": item_a.get("stima_euro"), "state": {"status": "FOUND" if not _missing(item_a.get("stima_euro")) else "NOT_FOUND", "evidence": (item_a.get("fonte_perizia") or {}).get("evidence", [])}},
        "prezzo_base_eur": {"value": _field_state(result, "prezzo_base_asta").get("value"), "state": _field_state(result, "prezzo_base_asta")},
        "dati_asta": {"value": dati_asta if isinstance(dati_asta, dict) and dati_asta else None, "state": _field_state(result, "dati_asta")},
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--analysis-id", required=True)
    ap.add_argument("--estratto-pdf", required=True)
    ap.add_argument("--run-dir", required=True)
    args = ap.parse_args()

    base_url = (os.getenv("BASE_URL") or "").strip()
    session_token = (os.getenv("SESSION_TOKEN") or os.getenv("session_token") or "").strip()
    if not base_url or not session_token:
        _fail(["BASE_URL and SESSION_TOKEN are required"])

    run_dir = Path(args.run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    estratto_ref = run_dir / "estratto_ref.json"
    subprocess.run(
        [
            str(Path(__file__).parent.parent / ".venv/bin/python"),
            str(Path(__file__).parent / "estratto_ref_build.py"),
            "--pdf",
            args.estratto_pdf,
            "--out",
            str(estratto_ref),
        ],
        check=True,
    )
    ref = json.loads(estratto_ref.read_text(encoding="utf-8")).get("fields", {})

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

    bvals = _backend_values(result)
    failures: List[str] = []
    for key, b in bvals.items():
        rv = ref.get(key, {})
        expected = rv.get("value", "NOT_FOUND") if isinstance(rv, dict) else "NOT_FOUND"
        observed = b.get("value")
        state = b.get("state", {})
        evidence = state.get("evidence", []) if isinstance(state, dict) else []
        searched = state.get("searched_in", []) if isinstance(state, dict) else []

        if not _missing(expected):
            if _missing(observed):
                # Some customer estratti include auction detail that may be absent in CTU text layer.
                # Accept NOT_FOUND only when backend provides searched_in proof.
                if key == "dati_asta" and state.get("status") == "NOT_FOUND" and state.get("searched_in"):
                    continue
                failures.append(f"{key}: expected={expected} observed missing")
                continue
            if not _eq(expected, observed):
                failures.append(f"{key}: expected={expected} observed={observed}")
                continue
        if state.get("status") == "FOUND" and not evidence:
            failures.append(f"{key}: FOUND without evidence")
        if state.get("status") == "NOT_FOUND" and not searched:
            failures.append(f"{key}: NOT_FOUND without searched_in proof")

    if failures:
        _fail(failures)

    print(f"PASS: regression_gate_estratto_parity_strict analysis_id={args.analysis_id}")


if __name__ == "__main__":
    main()
