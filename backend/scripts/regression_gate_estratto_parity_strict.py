#!/usr/bin/env python3
import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

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
    sa = re.sub(r"[^a-z0-9 ]+", " ", " ".join(str(a).lower().split()))
    sb = re.sub(r"[^a-z0-9 ]+", " ", " ".join(str(b).lower().split()))
    sa = " ".join(sa.split())
    sb = " ".join(sb.split())
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


def _to_amount(v: Any) -> Optional[float]:
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        m = re.search(r"([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?|[0-9]+(?:\.[0-9]+)?)", v)
        if not m:
            return None
        raw = m.group(1)
        if "," in raw:
            raw = raw.replace(".", "").replace(",", ".")
        try:
            return float(raw)
        except Exception:
            return None
    return None


def _money_amounts(result: Dict[str, Any]) -> List[float]:
    out: List[float] = []
    money = result.get("money_box", {})
    if not isinstance(money, dict):
        return out
    items = money.get("items", [])
    if not isinstance(items, list):
        return out
    for it in items:
        if not isinstance(it, dict):
            continue
        for key in ("stima_euro", "importo", "amount", "value"):
            val = _to_amount(it.get(key))
            if val is not None:
                out.append(val)
    return out


def _mirror_keys(result: Dict[str, Any]) -> Set[str]:
    keys: Set[str] = set()
    mirror = result.get("estratto_mirror")
    if not isinstance(mirror, dict):
        return keys
    sections = mirror.get("sections", [])
    if not isinstance(sections, list):
        return keys
    for sec in sections:
        if not isinstance(sec, dict):
            continue
        items = sec.get("items", [])
        if not isinstance(items, list):
            continue
        for it in items:
            if isinstance(it, dict) and it.get("key"):
                keys.add(str(it.get("key")))
    return keys


def _legacy_blueprint_match(item: Dict[str, Any], backend_vals: Dict[str, Dict[str, Any]]) -> bool:
    k = str(item.get("key") or "")
    mapping = {
        "occupancy": "occupancy",
        "ape": "ape_status",
        "spese_condominiali_arretrate": "spese_condominiali_arretrate",
        "dati_asta": "dati_asta",
    }
    bkey = mapping.get(k)
    if not bkey:
        return False
    observed = (backend_vals.get(bkey) or {}).get("value")
    state = (backend_vals.get(bkey) or {}).get("state") or {}
    if k == "occupancy" and isinstance(observed, str):
        obs = observed.upper()
        if "OCCUPATO" in obs and ("DEBITOR" in obs or "FAMILIAR" in obs):
            return True
    if k == "dati_asta":
        if isinstance(observed, dict) and observed:
            return True
        if state.get("status") == "NOT_FOUND" and state.get("searched_in"):
            return True
    if _missing(observed):
        return False
    if "value" in item:
        return _eq(item.get("value"), observed)
    return True


def _legacy_section_match(item: Dict[str, Any], result: Dict[str, Any]) -> bool:
    key = str(item.get("key") or "")
    if key != "incongruenze_catasto":
        return False
    fs = result.get("field_states", {}) if isinstance(result.get("field_states"), dict) else {}
    reg_val = str((fs.get("regolarita_urbanistica") or {}).get("value") or "").upper()
    cat_val = str((fs.get("conformita_catastale") or {}).get("value") or "").upper()
    return ("DIFFORM" in reg_val) or ("INCONGRUEN" in cat_val) or ("DIFFORM" in cat_val)


def _blueprint_failures(ref_doc: Dict[str, Any], result: Dict[str, Any], backend_vals: Dict[str, Dict[str, Any]]) -> List[str]:
    sections = ref_doc.get("sections", [])
    if not isinstance(sections, list) or not sections:
        return []

    failures: List[str] = []
    mirror = _mirror_keys(result)
    amounts = _money_amounts(result)

    for sec in sections:
        if not isinstance(sec, dict):
            continue
        sec_name = str(sec.get("name") or "UNKNOWN")
        items = sec.get("items", [])
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            key = str(item.get("key") or "").strip()
            if not key:
                continue

            represented = False
            typ = str(item.get("type") or "").lower()
            if typ == "cost":
                eur = _to_amount(item.get("value_eur"))
                if eur is not None and any(abs(x - eur) < 0.01 for x in amounts):
                    represented = True
                if key in mirror:
                    represented = True
            else:
                if key in mirror:
                    represented = True
                elif _legacy_blueprint_match(item, backend_vals):
                    represented = True
                elif _legacy_section_match(item, result):
                    represented = True

            if not represented:
                ev = item.get("evidence") if isinstance(item.get("evidence"), list) else []
                ev0 = ev[0] if ev and isinstance(ev[0], dict) else {}
                page = ev0.get("page", "?")
                quote = str(ev0.get("quote") or "").strip()
                expected = item.get("value_eur") if "value_eur" in item else item.get("value")
                failures.append(
                    f"blueprint missing [{sec_name}] {key}: expected={expected} evidence(page={page}, quote={quote})"
                )

    return failures


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
    ref_doc = json.loads(estratto_ref.read_text(encoding="utf-8"))
    ref = ref_doc.get("fields", {}) if isinstance(ref_doc.get("fields"), dict) else {}

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

    failures.extend(_blueprint_failures(ref_doc, result, bvals))

    if failures:
        _fail(failures)

    print(f"PASS: regression_gate_estratto_parity_strict analysis_id={args.analysis_id}")


if __name__ == "__main__":
    main()
