from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

from .runner import build_context
from .corpus_registry import load_cases, list_case_keys


def _row_order(row: Dict[str, object]) -> Tuple[int, int, int]:
    return (
        int(row.get("page", 0) or 0),
        int(row.get("line_index", 0) or 0),
        int(row.get("occurrence_index", 0) or 0),
    )


def build_bene_header_spine(case_key: str) -> Dict[str, object]:
    ctx = build_context(case_key)
    src = ctx.artifact_dir / "plurality_headers.json"
    scope_fp = ctx.artifact_dir / "lot_scope_map.json"
    hyp_fp = ctx.artifact_dir / "structure_hypotheses.json"

    data = json.loads(src.read_text(encoding="utf-8"))
    scope = json.loads(scope_fp.read_text(encoding="utf-8"))
    hyp = json.loads(hyp_fp.read_text(encoding="utf-8"))

    winner = hyp.get("winner")

    bene_signals = data.get("bene_signals", []) or []
    header_rows = [
        row for row in bene_signals
        if row.get("class") == "HEADER_GRADE" and row.get("value") not in (None, "")
    ]

    _empty = {
        "case_key": case_key,
        "source_artifact": str(src),
        "status": "NO_BENE_HEADERS",
        "bene_header_spine": [],
        "unattributable_bene_signals": [],
        "summary": {
            "bene_count_from_headers": 0,
            "header_grade_bene_signal_count": len(header_rows),
        },
    }

    if winner == "BLOCKED_UNREADABLE" or not header_rows:
        dst = ctx.artifact_dir / "bene_header_spine.json"
        dst.write_text(json.dumps(_empty, ensure_ascii=False, indent=2), encoding="utf-8")
        return _empty

    if winner in {"H1_EXPLICIT_SINGLE_LOT", "H3_CANDIDATE_SINGLE_LOT_MULTI_BENE"}:
        # Single-lot: there is exactly one lot by definition.
        # Group purely by bene_id; no cross-lot merge risk.
        lot_scopes = scope.get("lot_scopes", [])
        sole_lot_id = str(lot_scopes[0]["lot_id"]) if lot_scopes else "unico"

        by_bene: Dict[str, List[Dict[str, object]]] = defaultdict(list)
        for row in header_rows:
            bene_id = str(int(row["value"]))
            by_bene[bene_id].append(row)

        ordered_ids = sorted(
            by_bene.keys(),
            key=lambda bid: _row_order(sorted(by_bene[bid], key=_row_order)[0]),
        )

        spine_rows = []
        for bene_id in ordered_ids:
            rows = sorted(by_bene[bene_id], key=_row_order)
            first = rows[0]
            pages = sorted({int(r.get("page", 0) or 0) for r in rows})
            spine_rows.append({
                "lot_id": sole_lot_id,
                "bene_id": bene_id,
                "composite_key": f"{sole_lot_id}/{bene_id}",
                "attribution": "CONFIRMED_BY_SINGLE_LOT",
                "first_header_page": int(first.get("page", 0) or 0),
                "first_header_line_index": int(first.get("line_index", 0) or 0),
                "first_header_occurrence_index": int(first.get("occurrence_index", 0) or 0),
                "first_header_quote": first.get("quote"),
                "occurrences": len(rows),
                "all_header_pages": pages,
            })

        out = {
            "case_key": case_key,
            "source_artifact": str(src),
            "status": "OK",
            "bene_header_spine": spine_rows,
            "unattributable_bene_signals": [],
            "summary": {
                "bene_count_from_headers": len(spine_rows),
                "header_grade_bene_signal_count": len(header_rows),
                "cross_lot_merge_safe": True,
                "cross_lot_merge_reason": (
                    "Single-lot winner; bene numbers cannot collide across lots by definition."
                ),
            },
        }

    elif winner in {"H2_EXPLICIT_MULTI_LOT", "H4_CANDIDATE_MULTI_LOT_MULTI_BENE"}:
        # Multi-lot: must attribute each signal to a specific lot before grouping.
        # Group by composite key (lot_id, bene_id).
        # Same bene number under different lots becomes separate entries.
        lot_scopes = scope.get("lot_scopes", [])
        global_pre_lot = scope.get("global_pre_lot_zone") or {}
        pre_lot_pages = set(global_pre_lot.get("pages", []))
        collision_pages = {c["page"] for c in (scope.get("same_page_collisions") or [])}

        by_composite: Dict[Tuple[str, str], List[Dict[str, object]]] = defaultdict(list)
        unattributable: List[Dict[str, object]] = []

        for row in header_rows:
            bene_id = str(int(row["value"]))
            page = int(row.get("page", 0) or 0)

            if page in pre_lot_pages:
                unattributable.append({
                    "bene_id": bene_id,
                    "page": page,
                    "line_index": row.get("line_index"),
                    "quote": row.get("quote"),
                    "reason": "GLOBAL_PRE_LOT_ZONE",
                })
                continue

            if page in collision_pages:
                unattributable.append({
                    "bene_id": bene_id,
                    "page": page,
                    "line_index": row.get("line_index"),
                    "quote": row.get("quote"),
                    "reason": "SAME_PAGE_COLLISION_ZONE",
                })
                continue

            matched_lot: str | None = None
            for ls in lot_scopes:
                if int(ls["start_page"]) <= page <= int(ls["end_page"]):
                    matched_lot = str(ls["lot_id"])
                    break

            if matched_lot is None:
                unattributable.append({
                    "bene_id": bene_id,
                    "page": page,
                    "line_index": row.get("line_index"),
                    "quote": row.get("quote"),
                    "reason": "NO_MATCHING_LOT_SCOPE",
                })
                continue

            by_composite[(matched_lot, bene_id)].append(row)

        ordered_keys = sorted(
            by_composite.keys(),
            key=lambda k: _row_order(sorted(by_composite[k], key=_row_order)[0]),
        )

        spine_rows = []
        for (lot_id, bene_id) in ordered_keys:
            rows = sorted(by_composite[(lot_id, bene_id)], key=_row_order)
            first = rows[0]
            pages = sorted({int(r.get("page", 0) or 0) for r in rows})
            spine_rows.append({
                "lot_id": lot_id,
                "bene_id": bene_id,
                "composite_key": f"{lot_id}/{bene_id}",
                "attribution": "ATTRIBUTED_BY_SCOPE",
                "first_header_page": int(first.get("page", 0) or 0),
                "first_header_line_index": int(first.get("line_index", 0) or 0),
                "first_header_occurrence_index": int(first.get("occurrence_index", 0) or 0),
                "first_header_quote": first.get("quote"),
                "occurrences": len(rows),
                "all_header_pages": pages,
            })

        # Explicit cross-lot safeguard: detect any bene_id that appears under more than one lot.
        lots_by_bene: Dict[str, List[str]] = defaultdict(list)
        for (lot_id, bene_id) in ordered_keys:
            lots_by_bene[bene_id].append(lot_id)
        cross_lot_bene_ids = sorted(bid for bid, lots in lots_by_bene.items() if len(lots) > 1)
        cross_lot_merge_safe = len(cross_lot_bene_ids) == 0

        out = {
            "case_key": case_key,
            "source_artifact": str(src),
            "status": "OK" if spine_rows else "NO_BENE_HEADERS",
            "bene_header_spine": spine_rows,
            "unattributable_bene_signals": unattributable,
            "summary": {
                "bene_count_from_headers": len(spine_rows),
                "header_grade_bene_signal_count": len(header_rows),
                "cross_lot_merge_safe": cross_lot_merge_safe,
                "cross_lot_bene_ids_seen_under_multiple_lots": cross_lot_bene_ids,
                "cross_lot_merge_reason": (
                    "All attributed bene numbers appear under exactly one lot; no cross-lot identity collision."
                    if cross_lot_merge_safe else
                    f"Bene id(s) {cross_lot_bene_ids} appear under multiple lots; "
                    "composite key (lot_id/bene_id) keeps them separate."
                ),
            },
        }

    else:
        # Winner is NEEDS_NON_HEADER_STRUCTURE_SIGNALS or unknown: cannot attribute safely.
        out = _empty

    dst = ctx.artifact_dir / "bene_header_spine.json"
    dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Build lot-aware bene header spine")
    parser.add_argument("--case", required=True, choices=list_case_keys())
    args = parser.parse_args()

    out = build_bene_header_spine(args.case)
    print(json.dumps(out["summary"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
