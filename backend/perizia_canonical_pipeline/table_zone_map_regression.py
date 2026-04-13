"""
Regression runner for table_zone_map.

Runs all corpus cases, writes per-case artifacts and a summary JSON.
"""
from __future__ import annotations

import json
from pathlib import Path

from .corpus_registry import load_cases
from .table_zone_map import build_table_zone_map


def main() -> None:
    rows = []
    for case in load_cases():
        out = build_table_zone_map(case.case_key)
        summary = out.get("summary", {})
        by_type = summary.get("by_type", {})
        rows.append({
            "case_key": case.case_key,
            "status": out["status"],
            "winner": out["winner"],
            "total_zones": summary.get("total_zones", 0),
            "authoritative": by_type.get("AUTHORITATIVE_FIELD_TABLE", 0),
            "recap_summary": by_type.get("RECAP_SUMMARY_TABLE", 0),
            "arithmetic_rollup": by_type.get("ARITHMETIC_ROLLUP_TABLE", 0),
            "methodology_comparable": by_type.get("METHODOLOGY_COMPARABLE_TABLE", 0),
            "unknown": by_type.get("UNKNOWN_TABLE", 0),
            "toc_pages_skipped": summary.get("toc_pages_skipped", []),
            "warnings": out.get("warnings", []),
            "artifact": (
                f"/srv/perizia/_qa/canonical_pipeline/runs/"
                f"{case.case_key}/artifacts/table_zone_map.json"
            ),
        })

    qa_dir = Path("/srv/perizia/_qa/canonical_pipeline")
    fp = qa_dir / "table_zone_map_regression_summary.json"
    fp.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(fp)
    print(f"COUNT={len(rows)}")
    for r in rows:
        print(
            f"  {r['case_key']}: status={r['status']} "
            f"total={r['total_zones']} "
            f"auth={r['authoritative']} "
            f"recap={r['recap_summary']} "
            f"rollup={r['arithmetic_rollup']} "
            f"method={r['methodology_comparable']} "
            f"unknown={r['unknown']}"
        )


if __name__ == "__main__":
    main()
