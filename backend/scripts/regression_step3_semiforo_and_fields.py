#!/usr/bin/env python3
import argparse
import os
import sys
from typing import Any, Dict, List, Optional

import requests

REQUIRED_FIELD_STATE_KEYS = [
    "tribunale",
    "procedura",
    "lotto",
    "address",
    "prezzo_base_asta",
    "superficie",
    "diritto_reale",
    "stato_occupativo",
    "regolarita_urbanistica",
    "conformita_catastale",
    "spese_condominiali_arretrate",
    "formalita_pregiudizievoli",
]

REQUIRED_LOT_FIELDS = [
    "lot_number",
    "prezzo_base_eur",
    "ubicazione",
    "superficie_mq",
    "diritto_reale",
]

REQUIRED_LOT_EVIDENCE_KEYS = [
    "lotto",
    "prezzo_base",
    "ubicazione",
    "superficie",
    "diritto_reale",
]


def _fail_many(reasons: List[str]) -> None:
    if not reasons:
        reasons = ["unknown failure"]
    print("FAIL:")
    for reason in reasons:
        print(f"- {reason}")
    sys.exit(1)


def _assert_proof_entry(entry: Any, context: str, failures: List[str]) -> None:
    if not isinstance(entry, dict):
        failures.append(f"{context}: proof entry is not a dict")
        return
    page = entry.get("page")
    quote = entry.get("quote")
    if not isinstance(page, int):
        failures.append(f"{context}: page must be int, got {type(page).__name__}")
    if not isinstance(quote, str) or not quote.strip():
        failures.append(f"{context}: quote must be non-empty string")
    for key in ("start_offset", "end_offset"):
        if key not in entry:
            failures.append(f"{context}: missing {key}")
        elif not isinstance(entry.get(key), int):
            failures.append(f"{context}: {key} must be int")
    if entry.get("offset_mode") != "PAGE_LOCAL":
        failures.append(f"{context}: offset_mode must be PAGE_LOCAL")


def _normalize_lotto_from_text(text: Any) -> Optional[str]:
    import re

    cleaned = " ".join(str(text or "").split())
    if not cleaned:
        return None
    if re.search(r"\bLOTTO\s+UNICO\b", cleaned, re.I):
        return "Lotto Unico"
    m = re.search(r"\bLOTTI?\s+([0-9]+(?:\s*[,/-]\s*[0-9]+)*)", cleaned, re.I)
    if not m:
        return None
    nums = [int(n) for n in re.findall(r"\d+", m.group(1))]
    if len(nums) == 1:
        return f"Lotto {nums[0]}"
    if len(nums) > 1:
        return f"Lotti {min(nums)}–{max(nums)}"
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Regression Step3: semaforo + decision fields + lots contract.")
    parser.add_argument("--analysis-id", dest="analysis_id", default="analysis_3a7186b71d9b")
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

    analysis_id = args.analysis_id.strip() if args.analysis_id else "analysis_3a7186b71d9b"

    headers = {"Cookie": f"session_token={session_token}"}
    detail_url = f"{base_url.rstrip('/')}/api/analysis/perizia/{analysis_id}"
    resp = requests.get(detail_url, headers=headers, timeout=60)
    if resp.status_code != 200:
        _fail_many([f"GET {detail_url} failed: {resp.status_code} {resp.text[:180]}"])

    payload = resp.json()
    result: Dict[str, Any] = payload.get("result") or {}
    states: Dict[str, Any] = result.get("field_states") or {}
    failures: List[str] = []

    if not isinstance(states, dict):
        _fail_many(["result.field_states missing or not an object"])

    for key in REQUIRED_FIELD_STATE_KEYS:
        if key not in states:
            failures.append(f"Missing field_states key: {key}")

    for key in REQUIRED_FIELD_STATE_KEYS:
        state = states.get(key) if isinstance(states.get(key), dict) else {}
        status = state.get("status")
        evidence = state.get("evidence") if isinstance(state.get("evidence"), list) else []
        searched_in = state.get("searched_in")
        if status == "FOUND":
            if len(evidence) < 1:
                failures.append(f"{key}: status FOUND but evidence is empty")
            else:
                for idx, ev in enumerate(evidence):
                    _assert_proof_entry(ev, f"{key}.evidence[{idx}]", failures)
            if searched_in != []:
                failures.append(f"{key}: status FOUND must have searched_in=[]")
        elif status in {"NOT_FOUND", "LOW_CONFIDENCE", "DA_VERIFICARE"}:
            if not isinstance(searched_in, list) or not searched_in:
                failures.append(f"{key}: status {status} must have non-empty searched_in list")
            else:
                for idx, entry in enumerate(searched_in):
                    _assert_proof_entry(entry, f"{key}.searched_in[{idx}]", failures)
        elif status == "USER_PROVIDED":
            if searched_in != []:
                failures.append(f"{key}: status USER_PROVIDED must have searched_in=[]")
        else:
            failures.append(f"{key}: invalid status {status!r}")

    lotto_state = states.get("lotto") if isinstance(states.get("lotto"), dict) else {}
    lotto_evidence = lotto_state.get("evidence") if isinstance(lotto_state.get("evidence"), list) else []
    if lotto_state.get("status") == "FOUND" and lotto_evidence:
        evidence_quote = str((lotto_evidence[0] or {}).get("quote") or "")
        if "lotto unico" in evidence_quote.lower():
            value = str(lotto_state.get("value") or "")
            if value != "Lotto Unico":
                failures.append(
                    f"lotto mismatch: evidence contains 'Lotto Unico' but value is {value!r}"
                )
        normalized_ev = _normalize_lotto_from_text(evidence_quote)
        normalized_val = _normalize_lotto_from_text(lotto_state.get("value"))
        if normalized_ev and normalized_val and normalized_ev != normalized_val:
            failures.append(
                f"lotto mismatch: normalized evidence={normalized_ev!r} normalized value={normalized_val!r}"
            )

    lots = result.get("lots") if isinstance(result.get("lots"), list) else []
    if not lots:
        failures.append("result.lots missing or empty")
    for idx, lot in enumerate(lots):
        if not isinstance(lot, dict):
            failures.append(f"lots[{idx}] is not an object")
            continue
        for key in REQUIRED_LOT_FIELDS:
            if key not in lot:
                failures.append(f"lots[{idx}] missing {key}")
        evidence = lot.get("evidence") if isinstance(lot.get("evidence"), dict) else None
        if evidence is None:
            failures.append(f"lots[{idx}] missing evidence object")
            continue
        for ev_key in REQUIRED_LOT_EVIDENCE_KEYS:
            if ev_key not in evidence:
                failures.append(f"lots[{idx}].evidence missing {ev_key}")
            elif not isinstance(evidence.get(ev_key), list):
                failures.append(f"lots[{idx}].evidence.{ev_key} must be list")
            else:
                for ev_idx, ev in enumerate(evidence.get(ev_key) or []):
                    _assert_proof_entry(ev, f"lots[{idx}].evidence.{ev_key}[{ev_idx}]", failures)

    semaforo = None
    if isinstance(result.get("section_1_semaforo_generale"), dict):
        semaforo = result.get("section_1_semaforo_generale")
    elif isinstance(result.get("semaforo_generale"), dict):
        semaforo = result.get("semaforo_generale")
    if not isinstance(semaforo, dict):
        failures.append("semaforo structure missing")
    else:
        blockers = semaforo.get("top_blockers")
        if not isinstance(blockers, list):
            failures.append("semaforo.top_blockers missing or not a list")
        else:
            if len(blockers) > 10:
                failures.append(f"semaforo.top_blockers too long: {len(blockers)}")
            for idx, blocker in enumerate(blockers):
                if not isinstance(blocker, dict):
                    failures.append(f"top_blockers[{idx}] is not an object")
                    continue
                evidence = blocker.get("evidence") if isinstance(blocker.get("evidence"), list) else []
                searched_in = blocker.get("searched_in") if isinstance(blocker.get("searched_in"), list) else []
                if not evidence and not searched_in:
                    failures.append(f"top_blockers[{idx}] must have evidence or searched_in")
                for ev_idx, ev in enumerate(evidence):
                    _assert_proof_entry(ev, f"top_blockers[{idx}].evidence[{ev_idx}]", failures)
                for ev_idx, ev in enumerate(searched_in):
                    _assert_proof_entry(ev, f"top_blockers[{idx}].searched_in[{ev_idx}]", failures)

    if failures:
        _fail_many(failures)

    print(f"PASS: regression_step3_semiforo_and_fields analysis_id={analysis_id}")
    print("Checked:")
    print("- required field_states keys")
    print("- FOUND has evidence >= 1")
    print("- NOT_FOUND/LOW_CONFIDENCE searched_in proof shape")
    print("- lotto value/evidence consistency")
    print("- lots required fields + evidence keys")
    print("- semaforo top_blockers structure")


if __name__ == "__main__":
    main()
