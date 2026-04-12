from __future__ import annotations

import json
from pathlib import Path

from .corpus_registry import load_cases
from .structure_precheck import build_structure_precheck


def main() -> None:
    rows = []
    for case in load_cases():
        rows.append(build_structure_precheck(case.case_key))

    fp = Path("/srv/perizia/_qa/canonical_pipeline/structure_precheck_regression_summary.json")
    fp.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(fp)
    print(f"COUNT={len(rows)}")


if __name__ == "__main__":
    main()
