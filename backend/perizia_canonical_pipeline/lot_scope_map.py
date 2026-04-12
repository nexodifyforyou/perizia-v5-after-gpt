from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List

from .runner import build_context
from .corpus_registry import load_cases, list_case_keys


def _spine_order(row: Dict[str, object]) -> tuple[int, int, int, str]:
    return (
        int(row.get("first_header_page", 0) or 0),
        int(row.get("first_header_line_index", 0) or 0),
        int(row.get("first_header_occurrence_index", 0) or 0),
        str(row.get("lot_id", "")),
    )


def _global_pre_lot_zone(spine_rows: List[Dict[str, object]]) -> Dict[str, object] | None:
    if not spine_rows:
        return None
    first_header_page = int(sorted(spine_rows, key=_spine_order)[0].get("first_header_page", 0) or 0)
    if first_header_page <= 1:
        return None
    return {
        "start_page": 1,
        "end_page": first_header_page - 1,
        "pages": list(range(1, first_header_page)),
        "range_reason": "Pages before the first explicit lot header; global document context, not lot evidence.",
    }


def build_lot_scope_map(case_key: str) -> Dict[str, object]:
    ctx = build_context(case_key)

    extract_fp = ctx.artifact_dir / "extract_metrics.json"
    spine_fp = ctx.artifact_dir / "lot_header_spine.json"
    hyp_fp = ctx.artifact_dir / "structure_hypotheses.json"

    extract = json.loads(extract_fp.read_text(encoding="utf-8"))
    spine = json.loads(spine_fp.read_text(encoding="utf-8"))
    hyp = json.loads(hyp_fp.read_text(encoding="utf-8"))

    pages_count = int(extract.get("pages_count", 0) or 0)
    winner = hyp.get("winner")
    spine_rows = spine.get("lot_header_spine", []) or []
    warnings = list(hyp.get("warnings", []) or [])

    out: Dict[str, object] = {
        "case_key": case_key,
        "winner": winner,
        "pages_count": pages_count,
        "status": "OK",
        "scope_mode": None,
        "lot_scopes": [],
        "unassigned_pages": [],
        "global_pre_lot_zone": None,
        "same_page_collisions": [],
        "warnings": warnings,
        "source_artifacts": {
            "extract_metrics": str(extract_fp),
            "lot_header_spine": str(spine_fp),
            "structure_hypotheses": str(hyp_fp),
        },
    }

    if winner == "BLOCKED_UNREADABLE":
        out["status"] = "BLOCKED_UNREADABLE"
        out["scope_mode"] = "NONE"
        dst = ctx.artifact_dir / "lot_scope_map.json"
        dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        return out

    if winner in {"H1_EXPLICIT_SINGLE_LOT", "H3_CANDIDATE_SINGLE_LOT_MULTI_BENE"}:
        if len(spine_rows) != 1:
            out["status"] = "INVALID_SINGLE_LOT_SPINE"
            out["scope_mode"] = "ERROR"
        else:
            row = spine_rows[0]
            out["scope_mode"] = "FULL_DOCUMENT_SINGLE_LOT"
            out["global_pre_lot_zone"] = None
            out["unassigned_pages"] = []
            out["lot_scopes"] = [
                {
                    "lot_id": row["lot_id"],
                    "start_page": 1,
                    "end_page": pages_count,
                    "first_header_page": row["first_header_page"],
                    "first_header_line_index": row.get("first_header_line_index"),
                    "first_header_quote": row["first_header_quote"],
                    "range_reason": "Single-lot winner: full document assigned to the only lot for navigation purposes.",
                }
            ]
    elif winner in {"H2_EXPLICIT_MULTI_LOT", "H4_CANDIDATE_MULTI_LOT_MULTI_BENE"}:
        ordered = sorted(spine_rows, key=_spine_order)
        out["scope_mode"] = "BETWEEN_EXPLICIT_LOT_HEADERS"
        scopes: List[Dict[str, object]] = []
        for idx, row in enumerate(ordered):
            start_page = int(row["first_header_page"])
            start_line_index = int(row.get("first_header_line_index", 0) or 0)
            next_row = ordered[idx + 1] if idx + 1 < len(ordered) else None
            next_start = int(next_row["first_header_page"]) if next_row else (pages_count + 1)
            end_page = max(start_page, next_start - 1)
            line_range_hint = None
            if next_row and next_start == start_page:
                next_line_index = int(next_row.get("first_header_line_index", 0) or 0)
                end_page = start_page
                line_range_hint = {
                    "start_line_index": start_line_index,
                    "end_before_line_index": next_line_index,
                    "note": "Same-page lot transition; page-level scope overlaps by design until evidence harvesting supports line slicing.",
                }
                collision = {
                    "page": start_page,
                    "from_lot_id": row["lot_id"],
                    "to_lot_id": next_row["lot_id"],
                    "from_first_header_line_index": start_line_index,
                    "to_first_header_line_index": next_line_index,
                    "state": "SAME_PAGE_COLLISION",
                    "resolution": "DETERMINISTIC_LINE_ORDER_ONLY",
                }
                out["same_page_collisions"].append(collision)
                out["warnings"].append(
                    f"Same-page lot header transition detected on page {start_page}: "
                    f"{row['lot_id']} line {start_line_index} -> {next_row['lot_id']} line {next_line_index}"
                )
            scopes.append(
                {
                    "lot_id": row["lot_id"],
                    "start_page": start_page,
                    "end_page": end_page,
                    "first_header_page": row["first_header_page"],
                    "first_header_line_index": row.get("first_header_line_index"),
                    "first_header_quote": row["first_header_quote"],
                    "line_range_hint": line_range_hint,
                    "range_reason": "Multi-lot winner: pages assigned from this explicit lot header until the page before the next explicit lot header.",
                }
            )
        out["lot_scopes"] = scopes
        if out["same_page_collisions"]:
            out["scope_mode"] = "BETWEEN_EXPLICIT_LOT_HEADERS_WITH_SAME_PAGE_COLLISION"

        assigned = set()
        for row in scopes:
            assigned.update(range(int(row["start_page"]), int(row["end_page"]) + 1))
        out["unassigned_pages"] = [p for p in range(1, pages_count + 1) if p not in assigned]
        if out["unassigned_pages"]:
            out["warnings"].append(
                f"Unassigned prefix/suffix pages detected: {out['unassigned_pages']}"
            )
    else:
        out["status"] = "NEEDS_NON_HEADER_STRUCTURE_SIGNALS"
        out["scope_mode"] = "NONE"

    if winner not in {"H1_EXPLICIT_SINGLE_LOT", "H3_CANDIDATE_SINGLE_LOT_MULTI_BENE"}:
        out["global_pre_lot_zone"] = _global_pre_lot_zone(spine_rows)

    dst = ctx.artifact_dir / "lot_scope_map.json"
    dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Build lot scope map from minimal structure hypotheses")
    parser.add_argument("--case", required=True, choices=list_case_keys())
    args = parser.parse_args()

    out = build_lot_scope_map(args.case)
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
