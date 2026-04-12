from __future__ import annotations

import json
from pathlib import Path

from .corpus_registry import load_cases
from .lot_scope_map import build_lot_scope_map


def main() -> None:
    rows = []
    for case in load_cases():
        out = build_lot_scope_map(case.case_key)
        rows.append({
            "case_key": case.case_key,
            "status": out["status"],
            "winner": out["winner"],
            "scope_mode": out["scope_mode"],
            "lot_count": len(out["lot_scopes"]),
            "unassigned_pages": out["unassigned_pages"],
            "artifact": f"/srv/perizia/_qa/canonical_pipeline/runs/{case.case_key}/artifacts/lot_scope_map.json",
        })

    fp = Path("/srv/perizia/_qa/canonical_pipeline/lot_scope_map_regression_summary.json")
    fp.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(fp)
    print(f"COUNT={len(rows)}")


if __name__ == "__main__":
    main()
