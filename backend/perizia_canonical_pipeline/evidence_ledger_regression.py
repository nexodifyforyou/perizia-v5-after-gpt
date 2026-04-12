from __future__ import annotations

import json
from pathlib import Path

from .corpus_registry import load_cases
from .evidence_ledger import build_evidence_ledger


def main() -> None:
    rows = []
    for case in load_cases():
        out = build_evidence_ledger(case.case_key)
        rows.append({
            "case_key": case.case_key,
            "status": out["status"],
            "field_scope": out["field_scope"],
            "lot_header_packets_count": out["coverage"]["lot_header_packets_count"],
            "blocked_zone_count": len(out["blocked_zones"]),
            "artifact": f"/srv/perizia/_qa/canonical_pipeline/runs/{case.case_key}/artifacts/evidence_ledger.json",
        })

    fp = Path("/srv/perizia/_qa/canonical_pipeline/evidence_ledger_regression_summary.json")
    fp.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(fp)
    print(f"COUNT={len(rows)}")


if __name__ == "__main__":
    main()
