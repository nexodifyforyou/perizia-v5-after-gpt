from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict

from .runner import build_context
from .corpus_registry import load_cases, list_case_keys


def build_structure_precheck(case_key: str) -> Dict[str, object]:
    ctx = build_context(case_key)

    extract_fp = ctx.artifact_dir / "extract_metrics.json"
    spine_fp = ctx.artifact_dir / "lot_header_spine.json"

    extract = json.loads(extract_fp.read_text(encoding="utf-8"))
    spine = json.loads(spine_fp.read_text(encoding="utf-8"))

    quality = extract.get("global_quality_tier")
    lot_count = int(spine.get("summary", {}).get("lot_count_from_headers", 0) or 0)
    duplicate_ids = spine.get("summary", {}).get("duplicate_lot_ids", []) or []
    header_signal_count = int(spine.get("summary", {}).get("header_grade_signal_count", 0) or 0)

    if quality == "UNREADABLE":
        status = "BLOCKED_UNREADABLE"
        route = "STOP_BEFORE_STRUCTURE"
        reason = "Extraction quality is unreadable; structure inference would be fiction."
    elif lot_count >= 2:
        status = "EXPLICIT_MULTI_LOT_HEADERS"
        route = "READY_FOR_MINIMAL_STRUCTURE_HYPOTHESIS"
        reason = "Multiple explicit lot headers found."
    elif lot_count == 1:
        status = "EXPLICIT_SINGLE_LOT_HEADER"
        route = "READY_FOR_MINIMAL_STRUCTURE_HYPOTHESIS"
        reason = "Exactly one explicit lot header found."
    else:
        status = "NO_EXPLICIT_LOT_HEADERS"
        route = "NEEDS_NON_HEADER_STRUCTURE_SIGNALS"
        reason = "Readable document but no explicit lot headers found."

    if duplicate_ids:
        duplicate_policy = "SURFACE_AND_REVIEW_IN_STRUCTURE_STAGE"
    else:
        duplicate_policy = "NONE"

    out = {
        "case_key": case_key,
        "extract_metrics_artifact": str(extract_fp),
        "lot_header_spine_artifact": str(spine_fp),
        "global_quality_tier": quality,
        "status": status,
        "route": route,
        "reason": reason,
        "lot_count_from_headers": lot_count,
        "header_grade_signal_count": header_signal_count,
        "duplicate_lot_ids": duplicate_ids,
        "duplicate_policy": duplicate_policy,
        "unreadable_pages": extract.get("unreadable_pages", []),
    }

    dst = ctx.artifact_dir / "structure_precheck.json"
    dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Structure precheck gate")
    parser.add_argument("--case", required=True, choices=list_case_keys())
    args = parser.parse_args()

    out = build_structure_precheck(args.case)
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
