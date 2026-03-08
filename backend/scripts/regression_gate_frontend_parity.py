#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List


REQUIRED_KEYS = [
    "tribunale",
    "procedure_id",
    "occupancy",
    "beni_count",
    "ape_status",
    "spese_condominiali_arretrate",
    "sanatoria_estimate",
    "prezzo_base",
    "dati_asta",
    "decisione_rapida_it",
    "decisione_rapida_en",
    "semaforo_status",
]


def _fail(msgs: List[str]) -> None:
    print("FAIL:")
    for m in msgs:
        print(f"- {m}")
    sys.exit(1)


def _missing(v: Any) -> bool:
    if v is None:
        return True
    s = str(v).strip().upper()
    return s in {"", "NULL", "NOT_FOUND", "NON SPECIFICATO IN PERIZIA"}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--analysis-id", required=False)
    ap.add_argument("--frontend-url", required=False)
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    if args.frontend_url and args.analysis_id:
        node = Path("/srv/perizia/app/frontend/scripts/capture_ui_snapshot.mjs")
        if not node.exists():
            _fail(["capture_ui_snapshot.mjs not found"])
        env = os.environ.copy()
        env["NEW_AID"] = args.analysis_id
        env["RUN_DIR"] = str(run_dir)
        env["FRONTEND_URL"] = args.frontend_url
        # Token is not printed by this script.
        if not env.get("SESSION_TOKEN"):
            _fail(["SESSION_TOKEN required to run Playwright capture"])
        subprocess.run(["node", str(node)], check=True, env=env, cwd="/srv/perizia/app/frontend")

    snap_path = run_dir / "frontend_snapshot.json"
    if not snap_path.exists():
        _fail([f"{snap_path} not found"])
    snap = json.loads(snap_path.read_text(encoding="utf-8"))
    if not isinstance(snap, dict):
        _fail(["frontend_snapshot.json is null/non-object"])

    displayed = snap.get("displayed_fields")
    if not isinstance(displayed, dict):
        _fail(["displayed_fields missing in frontend snapshot"])

    failures: List[str] = []
    backend_states: Dict[str, Any] = {}
    system_path = run_dir / "system.json"
    if system_path.exists():
        try:
            sysj = json.loads(system_path.read_text(encoding="utf-8"))
            result = sysj.get("result") if isinstance(sysj.get("result"), dict) else {}
            backend_states = result.get("field_states", {}) if isinstance(result.get("field_states"), dict) else {}
        except Exception:
            backend_states = {}
    for key in REQUIRED_KEYS:
        if key not in displayed:
            failures.append(f"displayed_fields missing key: {key}")

    for key in ("occupancy", "spese_condominiali_arretrate"):
        val = str(displayed.get(key, ""))
        backend_key = "stato_occupativo" if key == "occupancy" else "spese_condominiali_arretrate"
        backend_status = str((backend_states.get(backend_key) or {}).get("status") or "")
        if ("Non presenti" in val or "NON PRESENTI" in val) and backend_status == "NOT_FOUND":
            failures.append(f"{key}: masking detected ('Non presenti' shown while backend is NOT_FOUND)")
        if key == "spese_condominiali_arretrate" and backend_status == "NOT_FOUND":
            # For missing condo arrears evidence, frontend may show null/empty/"Non specificato in perizia".
            continue
        if _missing(val):
            failures.append(f"{key}: printed value missing")

    for key in ("decisione_rapida_it", "decisione_rapida_en", "semaforo_status"):
        if _missing(displayed.get(key)):
            failures.append(f"{key}: printed value missing")

    if failures:
        _fail(failures)

    print("PASS: regression_gate_frontend_parity")


if __name__ == "__main__":
    main()
