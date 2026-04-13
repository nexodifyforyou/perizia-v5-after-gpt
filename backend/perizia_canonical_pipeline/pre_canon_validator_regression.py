"""
Pre-canon validator regression runner.

Runs validate_case for every corpus case and writes a regression summary to:
  /srv/perizia/_qa/canonical_pipeline/pre_canon_validator_regression_summary.json
"""
from __future__ import annotations

import json
from pathlib import Path

from .corpus_registry import load_cases
from .pre_canon_validator import validate_case


def main() -> None:
    rows = []
    for case in load_cases():
        result = validate_case(case.case_key)
        checks_by_status = {"PASS": 0, "WARN": 0, "FAIL": 0}
        for chk in result.get("checks", []):
            checks_by_status[chk["status"]] = checks_by_status.get(chk["status"], 0) + 1

        rows.append({
            "case_key": case.case_key,
            "status": result["status"],
            "freeze_ready": result["freeze_ready"],
            "checks_pass": checks_by_status["PASS"],
            "checks_warn": checks_by_status["WARN"],
            "checks_fail": checks_by_status["FAIL"],
            "warnings": result.get("warnings", []),
            "errors": result.get("errors", []),
            "summary": result.get("summary", ""),
            "artifact": (
                f"/srv/perizia/_qa/canonical_pipeline/runs/"
                f"{case.case_key}/artifacts/pre_canon_validation_report.json"
            ),
        })

    fp = Path("/srv/perizia/_qa/canonical_pipeline/pre_canon_validator_regression_summary.json")
    fp.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(fp)
    print(f"COUNT={len(rows)}")
    for r in rows:
        freeze_str = "FREEZE_READY" if r["freeze_ready"] else "NOT_READY"
        print(
            f"  {r['case_key']}: {r['status']} [{freeze_str}] "
            f"pass={r['checks_pass']} warn={r['checks_warn']} fail={r['checks_fail']}"
        )
        if r["errors"]:
            for e in r["errors"]:
                print(f"    ERROR: {e}")
        if r["warnings"]:
            for w in r["warnings"]:
                print(f"    WARN:  {w}")


if __name__ == "__main__":
    main()
