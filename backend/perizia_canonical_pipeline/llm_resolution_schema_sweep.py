"""
Corpus-wide schema sweep for generated LLM resolution artifacts.
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List

from .llm_resolution_pack import _explanation_has_case_shape


DEFAULT_RUNS_ROOT = Path("/srv/perizia/_qa/canonical_pipeline/runs")
DEFAULT_SUMMARY_PATH = Path("/srv/perizia/_qa/canonical_pipeline/llm_resolution_schema_sweep_summary.json")
_USER_VISIBLE_MACHINE_CODE_RE = re.compile(
    r"\b(?:NON_QUANTIFICATO_IN_PERIZIA|NON_QUANTIFICATO|MISSING_EVIDENCE|"
    r"SCOPE_AMBIGUITY|SCOPE AMBIGUITY|OCR_NOISE|FIELD_CONFLICT|FIELD CONFLICT|"
    r"SUSPICIOUS_SILENCE|SUSPICIOUS SILENCE|GROUPED_CONTEXT_NEEDS_EXPLANATION|"
    r"GROUPED CONTEXT NEEDS EXPLANATION|OCR_VARIANT_COLLISION|OCR VARIANT COLLISION|"
    r"TABLE_RECAP_DUPLICATE_UNCLEAR|TABLE RECAP DUPLICATE UNCLEAR)\b",
    re.IGNORECASE,
)


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def scan_llm_resolution_pack_artifacts(runs_root: Path = DEFAULT_RUNS_ROOT) -> Dict[str, Any]:
    violations: List[Dict[str, str]] = []
    pack_count = 0
    resolution_count = 0

    for pack_path in sorted(runs_root.glob("*/artifacts/llm_resolution_pack.json")):
        pack_count += 1
        case_key = pack_path.parent.parent.name
        try:
            pack = _load_json(pack_path)
        except Exception as exc:
            violations.append({
                "case_key": case_key,
                "issue_id": "",
                "violation": "invalid_resolution_pack_json",
                "detail": f"{type(exc).__name__}: {exc}",
            })
            continue

        issue_path = pack_path.with_name("clarification_issue_pack.json")
        known_issue_ids = set()
        issues_by_id: Dict[str, Dict[str, Any]] = {}
        if issue_path.exists():
            issue_pack = _load_json(issue_path)
            issues_by_id = {
                issue.get("issue_id"): issue
                for issue in issue_pack.get("issues", [])
                if isinstance(issue, dict) and issue.get("issue_id")
            }
            known_issue_ids = {
                issue.get("issue_id")
                for issue in issues_by_id.values()
            }

        for resolution in pack.get("resolutions") or []:
            resolution_count += 1
            issue_id = str(resolution.get("issue_id") or "")
            outcome = resolution.get("llm_outcome")
            resolution_mode = resolution.get("resolution_mode")
            resolved_value = resolution.get("resolved_value")
            why_not_resolved = resolution.get("why_not_resolved")
            user_visible_explanation = str(resolution.get("user_visible_explanation") or "").strip()
            issue = issues_by_id.get(issue_id)

            if known_issue_ids and issue_id not in known_issue_ids:
                violations.append({
                    "case_key": case_key,
                    "issue_id": issue_id,
                    "violation": "unknown_issue_id",
                    "detail": "resolution references an issue_id absent from clarification_issue_pack.json",
                })

            if outcome == "unresolved_explained" and not str(why_not_resolved or "").strip():
                violations.append({
                    "case_key": case_key,
                    "issue_id": issue_id,
                    "violation": "unresolved_explained_missing_why_not_resolved",
                    "detail": "unresolved_explained requires non-empty why_not_resolved",
                })

            if outcome == "resolved" and resolved_value is None:
                violations.append({
                    "case_key": case_key,
                    "issue_id": issue_id,
                    "violation": "resolved_missing_resolved_value",
                    "detail": "resolved requires resolved_value",
                })

            if outcome == "upgraded_context" and resolved_value is not None and resolution_mode != "qualified_resolution":
                violations.append({
                    "case_key": case_key,
                    "issue_id": issue_id,
                    "violation": "upgraded_context_has_resolved_value",
                    "detail": "upgraded_context may carry resolved_value only for qualified_resolution",
                })

            if resolution_mode == "qualified_resolution":
                if resolved_value is None:
                    violations.append({
                        "case_key": case_key,
                        "issue_id": issue_id,
                        "violation": "qualified_resolution_missing_value",
                        "detail": "qualified_resolution requires a safe best value",
                    })
                if not str(resolution.get("context_qualification") or "").strip():
                    violations.append({
                        "case_key": case_key,
                        "issue_id": issue_id,
                        "violation": "qualified_resolution_missing_context",
                        "detail": "qualified_resolution requires context_qualification",
                    })
                if not str(resolution.get("why_not_fully_certain") or "").strip():
                    violations.append({
                        "case_key": case_key,
                        "issue_id": issue_id,
                        "violation": "qualified_resolution_missing_uncertainty",
                        "detail": "qualified_resolution requires why_not_fully_certain",
                    })

            if issue and not _explanation_has_case_shape(issue, user_visible_explanation, outcome, resolved_value):
                violations.append({
                    "case_key": case_key,
                    "issue_id": issue_id,
                    "violation": "user_visible_explanation_not_case_shaped",
                    "detail": "user_visible_explanation must include field, scope, and concrete bounded evidence",
                })

            user_text = " ".join(str(resolution.get(k) or "") for k in ("user_visible_explanation", "why_not_resolved"))
            if _USER_VISIBLE_MACHINE_CODE_RE.search(user_text):
                violations.append({
                    "case_key": case_key,
                    "issue_id": issue_id,
                    "violation": "user_visible_machine_code",
                    "detail": "user-visible unresolved text exposes internal machine reason or issue codes",
                })

    by_type: Dict[str, int] = {}
    for violation in violations:
        key = violation["violation"]
        by_type[key] = by_type.get(key, 0) + 1

    return {
        "runs_root": str(runs_root),
        "pack_count": pack_count,
        "resolution_count": resolution_count,
        "violation_count": len(violations),
        "violation_counts": by_type,
        "violations": violations,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Sweep generated LLM resolution packs for schema violations")
    parser.add_argument("--runs-root", default=str(DEFAULT_RUNS_ROOT))
    parser.add_argument("--summary-path", default=str(DEFAULT_SUMMARY_PATH))
    args = parser.parse_args()

    summary = scan_llm_resolution_pack_artifacts(Path(args.runs_root))
    summary_path = Path(args.summary_path)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(summary_path)
    print(json.dumps({
        "status": "OK" if summary["violation_count"] == 0 else "FAIL",
        "pack_count": summary["pack_count"],
        "resolution_count": summary["resolution_count"],
        "violation_count": summary["violation_count"],
        "violation_counts": summary["violation_counts"],
    }, ensure_ascii=False))
    if summary["violation_count"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
