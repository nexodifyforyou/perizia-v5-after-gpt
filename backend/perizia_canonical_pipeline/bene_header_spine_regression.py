from __future__ import annotations

import json
from pathlib import Path

from .corpus_registry import load_cases
from .bene_header_spine import build_bene_header_spine


def main() -> None:
    rows = []
    for case in load_cases():
        out = build_bene_header_spine(case.case_key)
        rows.append({
            "case_key": case.case_key,
            "status": out["status"],
            "summary": out["summary"],
            "artifact": f"/srv/perizia/_qa/canonical_pipeline/runs/{case.case_key}/artifacts/bene_header_spine.json",
        })

    fp = Path("/srv/perizia/_qa/canonical_pipeline/bene_header_spine_regression_summary.json")
    fp.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(fp)
    print(f"COUNT={len(rows)}")


if __name__ == "__main__":
    main()
