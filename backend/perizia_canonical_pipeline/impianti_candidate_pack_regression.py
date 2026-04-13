"""
Regression runner for impianti_candidate_pack.

Runs all corpus cases, writes per-case artifacts and a summary JSON.
"""
from __future__ import annotations

import json
from pathlib import Path

from .corpus_registry import load_cases
from .impianti_candidate_pack import build_impianti_candidate_pack


def main() -> None:
    rows = []
    for case in load_cases():
        out = build_impianti_candidate_pack(case.case_key)
        active_status = [
            c for c in out.get("candidates", [])
            if c.get("candidate_status") == "ACTIVE"
        ]
        context_only = [
            c for c in out.get("candidates", [])
            if c.get("candidate_status") == "CONTEXT_ONLY"
        ]
        rows.append({
            "case_key": case.case_key,
            "status": out["status"],
            "winner": out["winner"],
            "coverage": out["coverage"],
            "active_per_system_count": len(active_status),
            "context_only_count": len(context_only),
            "blocked_or_ambiguous_count": len(out["blocked_or_ambiguous"]),
            "warnings": out["warnings"],
            "artifact": (
                f"/srv/perizia/_qa/canonical_pipeline/runs/"
                f"{case.case_key}/artifacts/impianti_candidate_pack.json"
            ),
        })

    fp = Path(
        "/srv/perizia/_qa/canonical_pipeline/impianti_candidate_pack_regression_summary.json"
    )
    fp.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(fp)
    print(f"COUNT={len(rows)}")
    for r in rows:
        cov = r["coverage"]
        print(
            f"  {r['case_key']}: status={r['status']} "
            f"active_per_sys={r['active_per_system_count']} "
            f"context={r['context_only_count']} "
            f"blocked={r['blocked_or_ambiguous_count']} "
            f"fields={cov.get('impianti_fields_present', [])} "
            f"scope_keys={cov.get('impianti_scope_keys', [])}"
        )


if __name__ == "__main__":
    main()
