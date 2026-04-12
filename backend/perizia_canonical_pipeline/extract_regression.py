from __future__ import annotations

import json
from pathlib import Path

from .corpus_registry import load_cases
from .extract import extract_case


def main() -> None:
    rows = []
    for case in load_cases():
        rows.append(extract_case(case.case_key))

    out = Path("/srv/perizia/_qa/canonical_pipeline/extract_regression_summary.json")
    out.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(out)
    print(f"COUNT={len(rows)}")


if __name__ == "__main__":
    main()
