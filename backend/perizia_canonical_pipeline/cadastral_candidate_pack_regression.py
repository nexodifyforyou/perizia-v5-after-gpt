from __future__ import annotations

import json
from pathlib import Path

from .corpus_registry import load_cases
from .cadastral_candidate_pack import build_cadastral_candidate_pack


def main() -> None:
    rows = []
    for case in load_cases():
        out = build_cadastral_candidate_pack(case.case_key)
        rows.append({
            "case_key": case.case_key,
            "status": out["status"],
            "winner": out["winner"],
            "coverage": out["coverage"],
            "blocked_or_ambiguous_count": len(out["blocked_or_ambiguous"]),
            "artifact": (
                f"/srv/perizia/_qa/canonical_pipeline/runs/"
                f"{case.case_key}/artifacts/cadastral_candidate_pack.json"
            ),
        })

    fp = Path("/srv/perizia/_qa/canonical_pipeline/cadastral_candidate_pack_regression_summary.json")
    fp.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(fp)
    print(f"COUNT={len(rows)}")


if __name__ == "__main__":
    main()
