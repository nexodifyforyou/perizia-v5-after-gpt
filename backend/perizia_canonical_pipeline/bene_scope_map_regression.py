from __future__ import annotations

import json
from pathlib import Path

from .corpus_registry import load_cases
from .bene_scope_map import build_bene_scope_map


def main() -> None:
    rows = []
    for case in load_cases():
        out = build_bene_scope_map(case.case_key)
        rows.append({
            "case_key": case.case_key,
            "status": out["status"],
            "winner": out["winner"],
            "scope_mode": out["scope_mode"],
            "bene_scope_count": len(out["bene_scopes"]),
            "pre_header_zone_count": len(out["bene_pre_header_zones"]),
            "same_page_bene_collisions": len(out["same_page_bene_collisions"]),
            "blocked_or_ambiguous_count": len(out["blocked_or_ambiguous_bene_zones"]),
            "artifact": (
                f"/srv/perizia/_qa/canonical_pipeline/runs/"
                f"{case.case_key}/artifacts/bene_scope_map.json"
            ),
        })

    fp = Path("/srv/perizia/_qa/canonical_pipeline/bene_scope_map_regression_summary.json")
    fp.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(fp)
    print(f"COUNT={len(rows)}")


if __name__ == "__main__":
    main()
