#!/usr/bin/env python3
import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    def load_dotenv(*_args: Any, **_kwargs: Any) -> bool:
        return False
try:
    from pymongo import MongoClient
except Exception:  # pragma: no cover
    MongoClient = None

REQUIRED_HEADINGS = {
    "occupancy",
    "ape",
    "abusi_agibilita",
    "impianti",
    "catasto",
    "formalita",
    "dati_asta",
    "legal",
}
COST_TERMS = ("sanatoria", "regolarizz", "agibil", "abitabil", "lavor", "spese", "oneri", "costo")


def _load_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _fail(messages: List[str]) -> None:
    print("FAIL:")
    for msg in messages:
        print(f"- {msg}")
    sys.exit(1)


def _load_analysis_doc(analysis_id: str) -> Optional[Dict[str, Any]]:
    env_path = Path("/srv/perizia/app/backend/.env")
    if env_path.exists():
        load_dotenv(env_path)

    mongo_url = os.environ.get("MONGO_URL")
    db_name = os.environ.get("DB_NAME")
    if mongo_url and db_name and MongoClient is not None:
        client = None
        try:
            client = MongoClient(mongo_url, serverSelectionTimeoutMS=3000)
            client.admin.command("ping")
            doc = client[db_name]["perizia_analyses"].find_one({"analysis_id": analysis_id}, {"_id": 0})
            if isinstance(doc, dict):
                return doc
        except Exception:
            pass
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass

    offline_path = Path("/tmp/perizia_qa_run/analysis.json")
    if offline_path.exists():
        try:
            data = _load_json(offline_path)
            if isinstance(data, dict) and data.get("analysis_id") == analysis_id:
                return data
        except Exception:
            return None
    return None


def _costs_present_in_candidates(analysis_id: str) -> bool:
    money_path = Path("/srv/perizia/_qa/runs") / analysis_id / "candidates" / "candidates_money.json"
    if not money_path.exists():
        return False
    try:
        money = _load_json(money_path)
    except Exception:
        return False
    if not isinstance(money, list):
        return False
    for item in money:
        if not isinstance(item, dict):
            continue
        amount = item.get("amount_eur")
        if not isinstance(amount, (int, float)):
            continue
        text = f"{item.get('context', '')} {item.get('quote', '')}".lower()
        if any(term in text for term in COST_TERMS):
            return True
    return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify Step 3 sections under result.estratto_quality")
    parser.add_argument("--analysis-id", required=True, dest="analysis_id")
    args = parser.parse_args()

    analysis_id = args.analysis_id.strip()
    doc = _load_analysis_doc(analysis_id)
    if not isinstance(doc, dict):
        _fail([f"Could not load analysis document for analysis_id={analysis_id}"])

    result = doc.get("result")
    if not isinstance(result, dict):
        _fail(["result object missing in analysis doc"])

    errors: List[str] = []

    estratto = result.get("estratto_quality")
    if not isinstance(estratto, dict):
        errors.append("result.estratto_quality missing or invalid")
        _fail(errors)

    sections = estratto.get("sections")
    if not isinstance(sections, list):
        errors.append("result.estratto_quality.sections missing or invalid")
        _fail(errors)

    found_heading_keys = set()
    total_items = 0
    for s in sections:
        if not isinstance(s, dict):
            continue
        key = s.get("heading_key")
        if isinstance(key, str):
            found_heading_keys.add(key)
        items = s.get("items")
        if not isinstance(items, list):
            continue
        for idx, item in enumerate(items, start=1):
            total_items += 1
            if not isinstance(item, dict):
                errors.append(f"section {key} item[{idx}] is not an object")
                continue
            cand_ids = item.get("candidate_ids")
            evidence = item.get("evidence")
            if not isinstance(cand_ids, list) or not cand_ids:
                errors.append(f"section {key} item[{idx}] missing candidate_ids")
            if not isinstance(evidence, list) or not evidence:
                errors.append(f"section {key} item[{idx}] missing evidence")
            else:
                for eidx, ev in enumerate(evidence, start=1):
                    if not isinstance(ev, dict):
                        errors.append(f"section {key} item[{idx}] evidence[{eidx}] invalid")
                        continue
                    if not isinstance(ev.get("page"), int):
                        errors.append(f"section {key} item[{idx}] evidence[{eidx}] missing page")
                    quote = str(ev.get("quote") or "").strip()
                    if not quote:
                        errors.append(f"section {key} item[{idx}] evidence[{eidx}] missing quote")

    required_present = len(found_heading_keys.intersection(REQUIRED_HEADINGS))
    if required_present < 6:
        errors.append(f"only {required_present}/8 required headings present (need at least 6)")

    money_box = result.get("money_box") if isinstance(result.get("money_box"), dict) else {}
    money_items = money_box.get("items") if isinstance(money_box.get("items"), list) else []

    if _costs_present_in_candidates(analysis_id):
        cost_items_with_evidence = 0
        for item in money_items:
            if not isinstance(item, dict):
                continue
            ev = []
            fonte = item.get("fonte_perizia") if isinstance(item.get("fonte_perizia"), dict) else {}
            if isinstance(fonte.get("evidence"), list):
                ev = fonte.get("evidence")
            elif isinstance(item.get("evidence"), list):
                ev = item.get("evidence")
            if ev:
                cost_items_with_evidence += 1
        if cost_items_with_evidence < 3:
            errors.append(
                f"money_box evidence-backed cost items too low: {cost_items_with_evidence} (need >= 3 when costs present)"
            )

    if errors:
        _fail(errors)

    print(
        "PASS: "
        f"analysis_id={analysis_id} "
        f"headings_present={required_present}/8 "
        f"sections={len(sections)} "
        f"section_items={total_items} "
        f"money_items={len(money_items)}"
    )


if __name__ == "__main__":
    main()
