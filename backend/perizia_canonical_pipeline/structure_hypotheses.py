from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List

from .runner import build_context
from .corpus_registry import load_cases, list_case_keys


def build_structure_hypotheses(case_key: str) -> Dict[str, object]:
    ctx = build_context(case_key)

    precheck_fp = ctx.artifact_dir / "structure_precheck.json"
    headers_fp = ctx.artifact_dir / "plurality_headers.json"
    spine_fp = ctx.artifact_dir / "lot_header_spine.json"

    precheck = json.loads(precheck_fp.read_text(encoding="utf-8"))
    headers = json.loads(headers_fp.read_text(encoding="utf-8"))
    spine = json.loads(spine_fp.read_text(encoding="utf-8"))

    status = precheck["status"]
    duplicate_ids = precheck.get("duplicate_lot_ids", []) or []
    lot_count = int(precheck.get("lot_count_from_headers", 0) or 0)

    unique_bene_ids = headers.get("summary", {}).get("unique_bene_header_ids", []) or []
    unique_corpo_ids = headers.get("summary", {}).get("unique_corpo_header_ids", []) or []
    bene_count = len(unique_bene_ids)
    corpo_count = len(unique_corpo_ids)

    warnings: List[str] = []
    evidence_basis: List[Dict[str, object]] = []

    for row in spine.get("lot_header_spine", []):
        evidence_basis.append({
            "type": "lot_header",
            "lot_id": row["lot_id"],
            "page": row["first_header_page"],
            "quote": row["first_header_quote"],
        })

    if duplicate_ids:
        warnings.append(f"Duplicate header-grade lot ids detected: {', '.join(duplicate_ids)}")

    if status == "BLOCKED_UNREADABLE":
        winner = "BLOCKED_UNREADABLE"
        confidence = 1.0
        reason = "Unreadable extraction blocks structure inference."
    elif lot_count >= 2 and bene_count >= 2:
        winner = "H4_CANDIDATE_MULTI_LOT_MULTI_BENE"
        confidence = 0.75
        reason = "Multiple explicit lot headers and multiple distinct bene header ids detected."
    elif lot_count >= 2:
        winner = "H2_EXPLICIT_MULTI_LOT"
        confidence = 0.90 if not duplicate_ids else 0.80
        reason = "Multiple explicit lot headers detected."
    elif lot_count == 1 and bene_count >= 2:
        winner = "H3_CANDIDATE_SINGLE_LOT_MULTI_BENE"
        confidence = 0.80 if not duplicate_ids else 0.70
        reason = "Single explicit lot header with multiple distinct bene header ids."
    elif lot_count == 1:
        winner = "H1_EXPLICIT_SINGLE_LOT"
        confidence = 0.90 if not duplicate_ids else 0.75
        reason = "Exactly one explicit lot header detected."
    else:
        winner = "NEEDS_NON_HEADER_STRUCTURE_SIGNALS"
        confidence = 0.25
        reason = "Readable case but no explicit lot headers; header-led structure is insufficient."

    out = {
        "case_key": case_key,
        "source_artifacts": {
            "structure_precheck": str(precheck_fp),
            "plurality_headers": str(headers_fp),
            "lot_header_spine": str(spine_fp),
        },
        "winner": winner,
        "confidence": confidence,
        "reason": reason,
        "lot_count_from_headers": lot_count,
        "unique_bene_header_ids": unique_bene_ids,
        "unique_corpo_header_ids": unique_corpo_ids,
        "warnings": warnings,
        "evidence_basis": evidence_basis,
    }

    dst = ctx.artifact_dir / "structure_hypotheses.json"
    dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Minimal structure hypothesis builder")
    parser.add_argument("--case", required=True, choices=list_case_keys())
    args = parser.parse_args()

    out = build_structure_hypotheses(args.case)
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
