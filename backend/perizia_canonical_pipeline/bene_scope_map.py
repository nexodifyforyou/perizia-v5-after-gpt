from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .runner import build_context
from .corpus_registry import load_cases, list_case_keys


def _bene_row_order(row: Dict[str, object]) -> Tuple[int, int, int]:
    return (
        int(row.get("first_header_page", 0) or 0),
        int(row.get("first_header_line_index", 0) or 0),
        int(row.get("first_header_occurrence_index", 0) or 0),
    )


def _assign_bene_scopes(
    sorted_rows: List[Dict[str, object]],
    lot_end: int,
    out: Dict[str, object],
    range_reason_last: str,
    range_reason_mid: str,
) -> None:
    """
    Assign start/end page ranges to each bene in sorted_rows, analogous to
    the lot_scope_map between-headers logic.

    - from bene N to page before bene N+1's start
    - last bene extends to lot_end
    - same-page bene transition: line_range_hint on from-bene, collision record emitted
    """
    for idx, row in enumerate(sorted_rows):
        lot_id = str(row["lot_id"])
        bene_id = str(row["bene_id"])
        composite_key = str(row["composite_key"])
        start_page = int(row["first_header_page"])
        start_line = int(row["first_header_line_index"])
        attribution = str(row.get("attribution", "ATTRIBUTED_BY_SCOPE"))

        next_row = sorted_rows[idx + 1] if idx + 1 < len(sorted_rows) else None
        next_start_page = int(next_row["first_header_page"]) if next_row else lot_end + 1
        end_page = max(start_page, next_start_page - 1)
        line_range_hint: Optional[Dict[str, object]] = None

        if next_row and next_start_page == start_page:
            next_line = int(next_row["first_header_line_index"])
            end_page = start_page
            line_range_hint = {
                "start_line_index": start_line,
                "end_before_line_index": next_line,
                "note": (
                    f"Same-page bene transition on page {start_page}: "
                    f"{composite_key} line {start_line} -> {next_row['composite_key']} line {next_line}. "
                    "Page-level scope overlaps until line-level slicing is supported."
                ),
            }
            collision = {
                "lot_id": lot_id,
                "page": start_page,
                "from_composite_key": composite_key,
                "to_composite_key": str(next_row["composite_key"]),
                "from_bene_id": bene_id,
                "to_bene_id": str(next_row["bene_id"]),
                "from_first_header_line_index": start_line,
                "to_first_header_line_index": next_line,
                "state": "SAME_PAGE_BENE_COLLISION",
                "resolution": "DETERMINISTIC_LINE_ORDER_ONLY",
            }
            out["same_page_bene_collisions"].append(collision)
            out["warnings"].append(
                f"Same-page bene header transition on page {start_page}: "
                f"{composite_key} line {start_line} -> {next_row['composite_key']} line {next_line}"
            )

        range_reason = range_reason_last if not next_row else range_reason_mid

        out["bene_scopes"].append({
            "lot_id": lot_id,
            "bene_id": bene_id,
            "composite_key": composite_key,
            "start_page": start_page,
            "end_page": end_page,
            "first_header_page": start_page,
            "first_header_line_index": start_line,
            "first_header_quote": row.get("first_header_quote"),
            "line_range_hint": line_range_hint,
            "range_reason": range_reason,
            "attribution": attribution,
        })


def build_bene_scope_map(case_key: str) -> Dict[str, object]:
    ctx = build_context(case_key)

    hyp_fp = ctx.artifact_dir / "structure_hypotheses.json"
    scope_fp = ctx.artifact_dir / "lot_scope_map.json"
    spine_fp = ctx.artifact_dir / "bene_header_spine.json"
    extract_fp = ctx.artifact_dir / "extract_metrics.json"

    hyp = json.loads(hyp_fp.read_text(encoding="utf-8"))
    scope = json.loads(scope_fp.read_text(encoding="utf-8"))
    bene_spine = json.loads(spine_fp.read_text(encoding="utf-8"))
    extract = json.loads(extract_fp.read_text(encoding="utf-8"))

    winner = hyp.get("winner")
    pages_count = int(extract.get("pages_count", 0) or 0)

    out: Dict[str, object] = {
        "case_key": case_key,
        "winner": winner,
        "pages_count": pages_count,
        "status": "OK",
        "scope_mode": "NONE",
        "bene_scopes": [],
        "bene_pre_header_zones": [],
        "same_page_bene_collisions": [],
        "blocked_or_ambiguous_bene_zones": [],
        "warnings": [],
        "source_artifacts": {
            "structure_hypotheses": str(hyp_fp),
            "lot_scope_map": str(scope_fp),
            "bene_header_spine": str(spine_fp),
            "extract_metrics": str(extract_fp),
        },
    }

    if winner == "BLOCKED_UNREADABLE":
        out["status"] = "BLOCKED_UNREADABLE"
        dst = ctx.artifact_dir / "bene_scope_map.json"
        dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        return out

    bene_rows = bene_spine.get("bene_header_spine", []) or []

    # Pass through any signals the spine could not attribute at all.
    for sig in bene_spine.get("unattributable_bene_signals", []):
        out["blocked_or_ambiguous_bene_zones"].append({
            "type": "BENE_UNATTRIBUTABLE_FROM_SPINE",
            "reason": (
                f"Bene signal for bene_id={sig['bene_id']} on page {sig['page']} "
                f"could not be attributed to a lot ({sig['reason']}); "
                "no bene scope emitted."
            ),
            "bene_id": sig["bene_id"],
            "page": sig["page"],
            "line_index": sig.get("line_index"),
        })

    if not bene_rows:
        out["scope_mode"] = "NO_BENE_HEADERS"
        dst = ctx.artifact_dir / "bene_scope_map.json"
        dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        return out

    lot_scopes = scope.get("lot_scopes", []) or []

    if winner in {"H1_EXPLICIT_SINGLE_LOT", "H3_CANDIDATE_SINGLE_LOT_MULTI_BENE"}:
        lot_scope = lot_scopes[0]
        lot_start = int(lot_scope["start_page"])
        lot_end = int(lot_scope["end_page"])

        sorted_rows = sorted(bene_rows, key=_bene_row_order)
        first_bene_page = int(sorted_rows[0]["first_header_page"])

        if first_bene_page > lot_start:
            out["bene_pre_header_zones"].append({
                "lot_id": sorted_rows[0]["lot_id"],
                "start_page": lot_start,
                "end_page": first_bene_page - 1,
                "pages": list(range(lot_start, first_bene_page)),
                "role": "PRE_BENE_HEADER_CONTEXT",
                "note": (
                    "Pages within the lot scope before the first bene header. "
                    "Non-evidentiary navigation context only; not a blocked zone."
                ),
            })

        _assign_bene_scopes(
            sorted_rows,
            lot_end,
            out,
            range_reason_last=(
                "Single-lot winner: last bene in lot; scope extends to end of lot."
            ),
            range_reason_mid=(
                "Single-lot winner: bene scope from this bene header to page before the next bene header."
            ),
        )

    elif winner in {"H2_EXPLICIT_MULTI_LOT", "H4_CANDIDATE_MULTI_LOT_MULTI_BENE"}:
        by_lot: Dict[str, List[Dict[str, object]]] = defaultdict(list)
        for row in bene_rows:
            by_lot[str(row["lot_id"])].append(row)

        for lot_scope in lot_scopes:
            lot_id = str(lot_scope["lot_id"])
            lot_start = int(lot_scope["start_page"])
            lot_end = int(lot_scope["end_page"])
            lot_bene_rows = sorted(by_lot.get(lot_id, []), key=_bene_row_order)

            if not lot_bene_rows:
                continue

            first_bene_page = int(lot_bene_rows[0]["first_header_page"])
            if first_bene_page > lot_start:
                out["bene_pre_header_zones"].append({
                    "lot_id": lot_id,
                    "start_page": lot_start,
                    "end_page": first_bene_page - 1,
                    "pages": list(range(lot_start, first_bene_page)),
                    "role": "PRE_BENE_HEADER_CONTEXT",
                    "note": (
                        f"Pages within lot {lot_id} scope before its first bene header. "
                        "Non-evidentiary navigation context only; not a blocked zone."
                    ),
                })

            _assign_bene_scopes(
                lot_bene_rows,
                lot_end,
                out,
                range_reason_last=(
                    f"Multi-lot winner: last bene in lot {lot_id}; "
                    "scope extends to end of lot scope. Attribution by page containment."
                ),
                range_reason_mid=(
                    f"Multi-lot winner: bene scope within lot {lot_id} from this bene header "
                    "to page before the next bene header in the same lot."
                ),
            )

        # Explicit warning when attribution is page-containment only (not confirmed by structure).
        scope_attributed = [r for r in bene_rows if r.get("attribution") == "ATTRIBUTED_BY_SCOPE"]
        if scope_attributed:
            keys = [r["composite_key"] for r in scope_attributed]
            out["warnings"].append(
                f"Bene scopes for {keys} are ATTRIBUTED_BY_SCOPE (page containment within lot scope only). "
                "Ranges are provisionally safe but not confirmed by explicit bene-to-lot structure signals."
            )

    # Set scope_mode from what was produced.
    if out["bene_scopes"]:
        if out["same_page_bene_collisions"]:
            out["scope_mode"] = "BENE_SEQUENTIAL_WITH_SAME_PAGE_COLLISION"
        else:
            out["scope_mode"] = "BENE_SEQUENTIAL"

    dst = ctx.artifact_dir / "bene_scope_map.json"
    dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Build bene-level scope map")
    parser.add_argument("--case", required=True, choices=list_case_keys())
    args = parser.parse_args()

    out = build_bene_scope_map(args.case)
    print(json.dumps({
        "case_key": out["case_key"],
        "status": out["status"],
        "scope_mode": out["scope_mode"],
        "bene_scope_count": len(out["bene_scopes"]),
        "pre_header_zone_count": len(out["bene_pre_header_zones"]),
        "same_page_bene_collisions": len(out["same_page_bene_collisions"]),
        "blocked_or_ambiguous_count": len(out["blocked_or_ambiguous_bene_zones"]),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
