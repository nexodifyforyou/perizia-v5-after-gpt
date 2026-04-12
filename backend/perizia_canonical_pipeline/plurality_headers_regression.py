from __future__ import annotations

import json
from pathlib import Path

from .corpus_registry import load_cases
from .plurality_headers import classify_case


def main() -> None:
    rows = []
    for case in load_cases():
        out = classify_case(case.case_key)
        rows.append(
            {
                "case_key": case.case_key,
                "summary": out["summary"],
                "artifact": f"/srv/perizia/_qa/canonical_pipeline/runs/{case.case_key}/artifacts/plurality_headers.json",
            }
        )

    fp = Path("/srv/perizia/_qa/canonical_pipeline/plurality_headers_regression_summary.json")
    fp.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(fp)
    print(f"COUNT={len(rows)}")


if __name__ == "__main__":
    main()
