from __future__ import annotations

import json
from pathlib import Path

from .corpus_registry import load_cases
from .occupancy_candidate_pack import build_occupancy_candidate_pack


def main() -> None:
    rows = []
    for case in load_cases():
        out = build_occupancy_candidate_pack(case.case_key)
        active = [c for c in out.get("candidates", []) if c.get("candidate_status") == "ACTIVE"]
        rows.append({
            "case_key": case.case_key,
            "status": out["status"],
            "winner": out["winner"],
            "coverage": out["coverage"],
            "blocked_or_ambiguous_count": len(out["blocked_or_ambiguous"]),
            "active_candidate_count": len(active),
            "warnings": out["warnings"],
            "artifact": (
                f"/srv/perizia/_qa/canonical_pipeline/runs/"
                f"{case.case_key}/artifacts/occupancy_candidate_pack.json"
            ),
        })

    fp = Path(
        "/srv/perizia/_qa/canonical_pipeline/occupancy_candidate_pack_regression_summary.json"
    )
    fp.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(fp)
    print(f"COUNT={len(rows)}")
    for r in rows:
        active_keys = r["coverage"].get("occupancy_scope_keys", [])
        fields = r["coverage"].get("occupancy_fields_present", [])
        print(
            f"  {r['case_key']}: status={r['status']} "
            f"active={r['active_candidate_count']} "
            f"blocked={r['blocked_or_ambiguous_count']} "
            f"fields={fields} scope_keys={active_keys}"
        )


if __name__ == "__main__":
    main()
