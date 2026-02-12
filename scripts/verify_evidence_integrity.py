#!/usr/bin/env python3
import json
import re
import sys
from typing import Any, Dict, List

ALLOWED_MODES = {"DOC_GLOBAL", "PAGE_LOCAL"}


def load_payload(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    return payload.get("result", payload)


def has_evidence(obj: Dict[str, Any]) -> bool:
    ev = obj.get("evidence", [])
    return isinstance(ev, list) and len(ev) > 0


def parse_numeric_surface(value: Any) -> bool:
    if isinstance(value, (int, float)):
        return True
    if isinstance(value, str):
        return bool(re.search(r"\d", value))
    return False


def collect_evidence_entries(obj: Any, out: List[Dict[str, Any]]) -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "evidence" and isinstance(v, list):
                for ev in v:
                    if isinstance(ev, dict):
                        out.append(ev)
            else:
                collect_evidence_entries(v, out)
    elif isinstance(obj, list):
        for item in obj:
            collect_evidence_entries(item, out)


def main() -> int:
    path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/perizia_qa_run/response.json"
    result = load_payload(path)
    errors: List[str] = []

    mode = result.get("offset_mode")
    if mode not in ALLOWED_MODES:
        errors.append(f"offset_mode missing/invalid: {mode}")

    lots = result.get("lots", [])
    for i, lot in enumerate(lots):
        prezzo = lot.get("prezzo_base_value")
        if isinstance(prezzo, (int, float)) and prezzo > 0 and not has_evidence({"evidence": lot.get("evidence", {}).get("prezzo_base", [])}):
            errors.append(f"lots[{i}].prezzo_base_value numeric without evidence")
        superficie = lot.get("superficie_mq")
        if parse_numeric_surface(superficie) and not has_evidence({"evidence": lot.get("evidence", {}).get("superficie", [])}):
            errors.append(f"lots[{i}].superficie_mq numeric without evidence")

    dati = result.get("dati_certi_del_lotto", {})
    prezzo_field = dati.get("prezzo_base_asta", {})
    if isinstance(prezzo_field, dict):
        pval = prezzo_field.get("value")
        if isinstance(pval, (int, float)) and pval > 0 and not has_evidence(prezzo_field):
            errors.append("dati_certi_del_lotto.prezzo_base_asta numeric without evidence")

    money_box = result.get("money_box", {})
    items = money_box.get("items", []) if isinstance(money_box, dict) else []
    for i, item in enumerate(items):
        val = item.get("stima_euro")
        if isinstance(val, (int, float)) and val > 0:
            fonte = item.get("fonte_perizia", {})
            ev = fonte.get("evidence", []) if isinstance(fonte, dict) else []
            if not isinstance(ev, list) or not ev:
                errors.append(f"money_box.items[{i}].stima_euro numeric without evidence")

    evidence_entries: List[Dict[str, Any]] = []
    collect_evidence_entries(result, evidence_entries)
    for i, ev in enumerate(evidence_entries):
        for key in ("page", "quote", "start_offset", "end_offset"):
            if key not in ev:
                errors.append(f"evidence[{i}] missing {key}")
        if ev.get("offset_mode") != mode:
            errors.append(f"evidence[{i}] offset_mode mismatch: {ev.get('offset_mode')} vs {mode}")
        if mode == "PAGE_LOCAL" and not ev.get("page_text_hash"):
            errors.append(f"evidence[{i}] missing page_text_hash for PAGE_LOCAL mode")

    if errors:
        print("EVIDENCE INTEGRITY FAIL")
        for e in errors:
            print(f"- {e}")
        return 1

    print("EVIDENCE INTEGRITY PASS")
    print(f"offset_mode={mode}")
    print(f"evidence_entries={len(evidence_entries)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
