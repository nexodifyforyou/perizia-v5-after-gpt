#!/usr/bin/env python3
import json
import sys
from typing import Any, Dict, List

ALLOWED_STATUS = {"VERDE", "GIALLO", "ROSSO", "DA_VERIFICARE"}
ALLOWED_STATUS_IT = {"OK", "ATTENZIONE", "CRITICO", "DA VERIFICARE"}


def load_result(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    return payload.get("result", payload)


def main() -> int:
    path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/perizia_qa_run/response.json"
    result = load_result(path)
    errors: List[str] = []

    section = result.get("section_9_legal_killers", {})
    items = section.get("items", []) if isinstance(section, dict) else []
    if not isinstance(items, list):
        errors.append("section_9_legal_killers.items must be a list")
        items = []

    seen = set()
    for i, item in enumerate(items):
        if not isinstance(item, dict):
            errors.append(f"item[{i}] is not an object")
            continue
        killer = str(item.get("killer") or item.get("title") or "").strip()
        if not killer:
            errors.append(f"item[{i}] missing killer")
            continue
        key = killer.lower()
        if key in seen:
            errors.append(f"duplicate killer key: {killer}")
        seen.add(key)

        status = str(item.get("status", "")).strip().upper()
        if status not in ALLOWED_STATUS:
            errors.append(f"item[{i}] invalid status: {status}")

        status_it = str(item.get("status_it", "")).strip()
        if status_it not in ALLOWED_STATUS_IT:
            errors.append(f"item[{i}] invalid status_it: {status_it}")

        reason_it = str(item.get("reason_it", "")).strip()
        if not reason_it:
            errors.append(f"item[{i}] missing reason_it")

        evidence = item.get("evidence", [])
        has_evidence = isinstance(evidence, list) and len(evidence) > 0
        searched_in = item.get("searched_in")
        has_searched = isinstance(searched_in, dict)
        if not (has_evidence or has_searched):
            errors.append(f"item[{i}] missing both evidence[] and searched_in")

    if errors:
        print("LEGAL KILLERS VERIFY FAIL")
        for e in errors:
            print(f"- {e}")
        return 1

    print("LEGAL KILLERS VERIFY PASS")
    print(f"items={len(items)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
