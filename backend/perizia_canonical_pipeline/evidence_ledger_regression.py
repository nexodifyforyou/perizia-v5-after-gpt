from __future__ import annotations

import json
from pathlib import Path

from .corpus_registry import load_cases
from .evidence_ledger import build_evidence_ledger


def main() -> None:
    rows = []
    for case in load_cases():
        out = build_evidence_ledger(case.case_key)
        cov = out["coverage"]
        rows.append({
            "case_key": case.case_key,
            "status": out["status"],
            "field_scope": out["field_scope"],
            "lot_header_packets_count": cov["lot_header_packets_count"],
            "blocked_zone_count": len(out["blocked_zones"]),
            "cost_packet_count": cov.get("cost_packet_count", 0),
            "cost_fields_present": cov.get("cost_fields_present", []),
            "cost_scope_keys": cov.get("cost_scope_keys", []),
            "cost_context_count": cov.get("cost_context_count", 0),
            "artifact": f"/srv/perizia/_qa/canonical_pipeline/runs/{case.case_key}/artifacts/evidence_ledger.json",
        })

    fp = Path("/srv/perizia/_qa/canonical_pipeline/evidence_ledger_regression_summary.json")
    fp.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(fp)
    print(f"COUNT={len(rows)}")
    for r in rows:
        print(
            f"  {r['case_key']}: status={r['status']} "
            f"cost_pkts={r['cost_packet_count']} "
            f"cost_ctx={r['cost_context_count']} "
            f"cost_fields={r['cost_fields_present']} "
            f"cost_keys={r['cost_scope_keys']}"
        )


if __name__ == "__main__":
    main()
