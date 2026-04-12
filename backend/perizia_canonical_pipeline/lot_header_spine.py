from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

from .runner import build_context
from .corpus_registry import load_cases, list_case_keys


def _row_order(row: Dict[str, object]) -> tuple[int, int, int, str]:
    return (
        int(row.get("page", 0) or 0),
        int(row.get("line_index", 0) or 0),
        int(row.get("occurrence_index", 0) or 0),
        str(row.get("quote", "")),
    )


def build_spine(case_key: str) -> Dict[str, object]:
    ctx = build_context(case_key)
    src = ctx.artifact_dir / "plurality_headers.json"
    data = json.loads(src.read_text(encoding="utf-8"))

    lot_signals = data.get("lot_signals", [])
    header_rows = [
        row for row in lot_signals
        if row.get("class") in {"HEADER_GRADE", "TABLE_GRADE"} and row.get("value") not in (None, "")
    ]

    by_id: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for row in header_rows:
        lot_id = str(row["value"]).strip().lower()
        by_id[lot_id].append(row)

    ordered_ids = sorted(by_id.keys(), key=lambda lot_id: _row_order(sorted(by_id[lot_id], key=_row_order)[0]))

    spine_rows = []
    duplicate_rows = []

    for lot_id in ordered_ids:
        rows = sorted(by_id[lot_id], key=_row_order)
        first = rows[0]
        pages = sorted({int(r.get("page", 0) or 0) for r in rows})

        spine_rows.append({
            "lot_id": lot_id,
            "first_header_page": int(first.get("page", 0) or 0),
            "first_header_line_index": int(first.get("line_index", 0) or 0),
            "first_header_occurrence_index": int(first.get("occurrence_index", 0) or 0),
            "first_header_quote": first.get("quote"),
            "occurrences": len(rows),
            "all_header_pages": pages,
        })

        if len(rows) > 1:
            duplicate_rows.append({
                "lot_id": lot_id,
                "occurrences": len(rows),
                "rows": rows,
            })

    out = {
        "case_key": case_key,
        "source_artifact": str(src),
        "status": "OK" if spine_rows else "NO_HEADERS",
        "lot_header_spine": spine_rows,
        "duplicate_header_grade_hits": duplicate_rows,
        "summary": {
            "lot_count_from_headers": len(spine_rows),
            "header_grade_signal_count": len(header_rows),
            "duplicate_lot_ids": [row["lot_id"] for row in duplicate_rows],
        },
    }

    dst = ctx.artifact_dir / "lot_header_spine.json"
    dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Build deterministic lot header spine")
    parser.add_argument("--case", required=True, choices=list_case_keys())
    args = parser.parse_args()

    out = build_spine(args.case)
    print(json.dumps(out["summary"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
