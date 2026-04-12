from __future__ import annotations

import json
from pathlib import Path

from .corpus_registry import load_cases
from .structure_hypotheses import build_structure_hypotheses


def main() -> None:
    rows = []
    for case in load_cases():
        out = build_structure_hypotheses(case.case_key)
        rows.append({
            "case_key": case.case_key,
            "winner": out["winner"],
            "confidence": out["confidence"],
            "warnings": out["warnings"],
            "artifact": f"/srv/perizia/_qa/canonical_pipeline/runs/{case.case_key}/artifacts/structure_hypotheses.json",
        })

    fp = Path("/srv/perizia/_qa/canonical_pipeline/structure_hypotheses_regression_summary.json")
    fp.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(fp)
    print(f"COUNT={len(rows)}")


if __name__ == "__main__":
    main()
