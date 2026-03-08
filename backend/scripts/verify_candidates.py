#!/usr/bin/env python3
import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, List


def _load_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _fail(messages: List[str]) -> None:
    print("FAIL:")
    for msg in messages:
        print(f"- {msg}")
    sys.exit(1)


def _normalized_number_variants(raw: str) -> List[str]:
    variants = [raw]
    num_match = re.search(r"(?:\d{1,3}(?:\.\d{3})+|\d+)(?:,\d{2})?", raw)
    if not num_match:
        return variants
    num = num_match.group(0)
    variants.append(num)
    variants.append(num.replace(".", ""))
    variants.append(num.replace(".", "").replace(",", "."))
    return [v for v in variants if v]


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify Step 2 candidates artifacts under _qa/runs/<analysis_id>/candidates")
    parser.add_argument("--analysis-id", required=True, dest="analysis_id")
    args = parser.parse_args()

    analysis_id = args.analysis_id.strip()
    candidates_dir = Path("/srv/perizia/_qa/runs") / analysis_id / "candidates"
    required = [
        "candidates_money.json",
        "candidates_dates.json",
        "candidates_triggers.json",
        "candidates_index.json",
    ]

    errors: List[str] = []
    if not candidates_dir.exists() or not candidates_dir.is_dir():
        _fail([f"Missing candidates folder: {candidates_dir}"])

    for name in required:
        fp = candidates_dir / name
        if not fp.exists() or not fp.is_file():
            errors.append(f"Missing required file: {fp}")

    if errors:
        _fail(errors)

    money = _load_json(candidates_dir / "candidates_money.json")
    dates = _load_json(candidates_dir / "candidates_dates.json")
    triggers = _load_json(candidates_dir / "candidates_triggers.json")

    if not isinstance(money, list):
        errors.append("candidates_money.json must contain a list")
        money = []
    if not isinstance(dates, list):
        errors.append("candidates_dates.json must contain a list")
        dates = []
    if not isinstance(triggers, list):
        errors.append("candidates_triggers.json must contain a list")
        triggers = []

    if len(money) < 1:
        errors.append("money candidates must be >= 1")
    if len(dates) < 1:
        errors.append("date/time candidates must be >= 1")
    if len(triggers) < 1:
        errors.append("trigger candidates must be >= 1")

    has_391849_euro_quote = False

    for idx, item in enumerate(money, start=1):
        if not isinstance(item, dict):
            errors.append(f"money[{idx}] is not an object")
            continue
        raw = str(item.get("amount_raw", ""))
        quote = str(item.get("quote", ""))
        amount_eur = item.get("amount_eur")

        if not raw:
            errors.append(f"money[{idx}] missing amount_raw")
            continue
        if not quote:
            errors.append(f"money[{idx}] missing quote")
            continue

        variants = _normalized_number_variants(raw)
        if not any(v in quote for v in variants):
            errors.append(f"money[{idx}] quote does not include amount value: amount_raw='{raw}' quote='{quote[:120]}'")

        if isinstance(amount_eur, (int, float)) and float(amount_eur) < 10.0:
            lowered_raw = raw.lower()
            if "€" not in raw and "euro" not in lowered_raw:
                errors.append(f"money[{idx}] suspicious small amount without explicit currency in amount_raw: {raw}")

        if isinstance(amount_eur, (int, float)) and abs(float(amount_eur) - 391849.0) < 1e-6 and "€" in quote:
            has_391849_euro_quote = True

    if not has_391849_euro_quote:
        errors.append("Expected prezzo base candidate 391849.0 with € in quote not found")

    unique_dates = set()
    for item in dates:
        if not isinstance(item, dict):
            continue
        unique_dates.add((item.get("date"), item.get("time")))

    if len(dates) >= 200:
        errors.append(f"dates not sufficiently deduped: total={len(dates)} expected significantly less than 261")

    if errors:
        _fail(errors)

    print(
        "PASS: "
        f"analysis_id={analysis_id} "
        f"money={len(money)} "
        f"dates={len(dates)} "
        f"unique_date_pairs={len(unique_dates)} "
        f"triggers={len(triggers)} "
        f"folder={candidates_dir}"
    )


if __name__ == "__main__":
    main()
