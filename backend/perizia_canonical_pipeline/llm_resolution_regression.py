"""
Regression runner for the bounded LLM clarification stage.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Optional

from .corpus_registry import list_case_keys
from .llm_resolution_pack import build_llm_resolution_pack
from .llm_resolution_schema_sweep import scan_llm_resolution_pack_artifacts


DEFAULT_SCENARIOS = [
    {
        "name": "valuation_conflict",
        "case_key": "torino_via_marchese_visconti_6_1",
        "issue_type": "FIELD_CONFLICT",
        "field_family": "valuation",
        "field_type": "valuation_market_raw",
    },
    {
        "name": "cost_grouped_context",
        "case_key": "ostuni_via_viterbo_2",
        "issue_type": "GROUPED_CONTEXT_NEEDS_EXPLANATION",
        "field_family": "cost",
        "field_type": "cost_sanatoria_raw",
    },
    {
        "name": "occupancy_scope_ambiguity",
        "case_key": "multilot_69_2024",
        "issue_type": "SCOPE_AMBIGUITY",
        "field_family": "occupancy",
    },
]


def _scenario_summary(scenario: Dict, out: Dict) -> Dict:
    issue = out["issues"][0]
    resolution = out["resolutions"][0]
    return {
        "scenario": scenario["name"],
        "case_key": out["case_key"],
        "issue_id": issue["issue_id"],
        "issue_type": issue["issue_type"],
        "field_family": issue["field_family"],
        "field_type": issue["field_type"],
        "llm_outcome": resolution["llm_outcome"],
        "resolved_value": resolution["resolved_value"],
        "needs_human_review": resolution["needs_human_review"],
        "source_pages": resolution["source_pages"],
        "supporting_evidence_count": len(resolution.get("supporting_evidence") or []),
        "validation_warnings": resolution.get("validation_warnings", []),
    }


def run_regression(name: Optional[str] = None) -> List[Dict]:
    scenarios = [s for s in DEFAULT_SCENARIOS if name is None or s["name"] == name]
    if not scenarios:
        raise SystemExit(f"Unknown scenario: {name}")
    rows: List[Dict] = []
    for scenario in scenarios:
        out = build_llm_resolution_pack(
            scenario["case_key"],
            issue_type=scenario.get("issue_type"),
            field_family=scenario.get("field_family"),
            field_type=scenario.get("field_type"),
            limit=1,
        )
        rows.append(_scenario_summary(scenario, out))
    fp = Path("/srv/perizia/_qa/canonical_pipeline/llm_resolution_regression_summary.json")
    fp.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    sweep = scan_llm_resolution_pack_artifacts()
    sweep_fp = Path("/srv/perizia/_qa/canonical_pipeline/llm_resolution_schema_sweep_summary.json")
    sweep_fp.write_text(json.dumps(sweep, ensure_ascii=False, indent=2), encoding="utf-8")
    if sweep["violation_count"]:
        raise AssertionError(
            "LLM resolution schema sweep failed: "
            + json.dumps(sweep["violation_counts"], ensure_ascii=False)
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Run bounded LLM clarification regression scenarios")
    parser.add_argument("--scenario", choices=[s["name"] for s in DEFAULT_SCENARIOS])
    args = parser.parse_args()
    rows = run_regression(args.scenario)
    print(json.dumps({"status": "OK", "run_count": len(rows), "rows": rows}, ensure_ascii=False))


if __name__ == "__main__":
    main()
