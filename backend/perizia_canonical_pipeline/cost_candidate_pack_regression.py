"""
Regression runner for cost_candidate_pack.

Runs all corpus cases, writes per-case artifacts and a summary JSON.
"""
from __future__ import annotations

import json
from pathlib import Path

from .corpus_registry import load_cases
from .cost_candidate_pack import build_cost_candidate_pack


def main() -> None:
    rows = []
    for case in load_cases():
        out = build_cost_candidate_pack(case.case_key)
        active_quant = [
            c for c in out.get("candidates", [])
            if c.get("candidate_status") == "ACTIVE" and c.get("is_quantified")
        ]
        active_ctx = [
            c for c in out.get("candidates", [])
            if c.get("candidate_status") == "ACTIVE" and not c.get("is_quantified")
        ]
        rows.append({
            "case_key": case.case_key,
            "status": out["status"],
            "winner": out["winner"],
            "coverage": out["coverage"],
            "active_quantified_count": len(active_quant),
            "active_context_count": len(active_ctx),
            "blocked_or_ambiguous_count": len(out["blocked_or_ambiguous"]),
            "warnings": out["warnings"],
            "artifact": (
                f"/srv/perizia/_qa/canonical_pipeline/runs/"
                f"{case.case_key}/artifacts/cost_candidate_pack.json"
            ),
        })

    fp = Path(
        "/srv/perizia/_qa/canonical_pipeline/cost_candidate_pack_regression_summary.json"
    )
    fp.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(fp)
    print(f"COUNT={len(rows)}")
    for r in rows:
        cov = r["coverage"]
        print(
            f"  {r['case_key']}: status={r['status']} "
            f"active_quant={r['active_quantified_count']} "
            f"active_ctx={r['active_context_count']} "
            f"blocked={r['blocked_or_ambiguous_count']} "
            f"fields={cov.get('cost_fields_present', [])} "
            f"scope_keys={cov.get('cost_scope_keys', [])}"
        )


if __name__ == "__main__":
    main()
