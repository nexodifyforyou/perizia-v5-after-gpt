from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .corpus_registry import list_case_keys
from .runner import build_context


OUTPUT_DIR = Path("/tmp/perizia_pipeline_settlement")


def _load_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _freeze_lookup(doc_map: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for scope_key, scope_data in (doc_map.get("fields") or {}).items():
        for family, family_data in (scope_data or {}).items():
            for field_type, entry in (family_data or {}).items():
                for issue_id in entry.get("issue_ids") or []:
                    if issue_id:
                        lookup[str(issue_id)] = {
                            "freeze_state": entry.get("state"),
                            "freeze_scope_key": scope_key,
                            "freeze_family": family,
                            "freeze_field_type": field_type,
                        }
    for item in doc_map.get("blocked_items") or []:
        for issue_id in item.get("issue_ids") or []:
            if issue_id:
                lookup[str(issue_id)] = {
                    "freeze_state": item.get("freeze_state") or "blocked",
                    "freeze_scope_key": item.get("scope_key"),
                    "freeze_family": item.get("field_family"),
                    "freeze_field_type": item.get("field_type"),
                }
    for item in doc_map.get("grouped_llm_explanations") or []:
        issue_id = item.get("issue_id")
        if issue_id:
            lookup[str(issue_id)] = {
                "freeze_state": "grouped_llm_explanation",
                "freeze_scope_key": item.get("scope_key"),
                "freeze_family": item.get("field_type"),
                "freeze_field_type": item.get("field_type"),
            }
    return lookup


def _resolution_lookup(pack: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for row in pack.get("resolutions") or []:
        issue_id = row.get("issue_id")
        if issue_id:
            lookup[str(issue_id)] = row
    return lookup


def _packet_row(case_key: str, issue: Dict[str, Any], resolution: Optional[Dict[str, Any]], freeze: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    target_scope = issue.get("target_scope") or issue.get("scope_metadata") or {}
    page_selection = issue.get("page_selection") or {}
    row = {
        "case_key": case_key,
        "issue_id": issue.get("issue_id"),
        "issue_type": issue.get("issue_type"),
        "family": issue.get("field_family"),
        "field": issue.get("field_type"),
        "target_scope": target_scope.get("scope_key"),
        "target_scope_kind": issue.get("target_scope_kind") or target_scope.get("scope_kind"),
        "packet_pages": issue.get("source_pages") or [],
        "anchor_pages": [item.get("page") for item in issue.get("anchor_pages") or [] if isinstance(item, dict)],
        "recap_pages": [item.get("page") for item in issue.get("recap_pages") or [] if isinstance(item, dict)],
        "has_target_section_entry_page": issue.get("has_target_section_entry_page", page_selection.get("has_target_section_entry_page")),
        "has_valid_anchor_chain": issue.get("has_valid_anchor_chain", page_selection.get("has_valid_anchor_chain")),
        "uses_summary_or_index_page": issue.get("uses_summary_or_index_page", page_selection.get("uses_summary_or_index_page")),
        "uses_transition_page": issue.get("uses_transition_page", page_selection.get("uses_transition_page")),
        "out_of_scope_primary_pages": issue.get("page_selection", {}).get("out_of_scope_primary_pages", []),
        "cross_scope_contamination_detected": issue.get("cross_scope_contamination_detected", page_selection.get("cross_scope_contamination_detected")),
        "contamination_class": issue.get("contamination_class"),
        "contamination_disposition": issue.get("contamination_disposition"),
        "admissibility_status": issue.get("admissibility_status", "admissible"),
        "admissibility_reason_codes": issue.get("admissibility_reason_codes") or [],
        "llm_attempted": bool(resolution),
        "llm_outcome": resolution.get("llm_outcome") if resolution else None,
        "resolution_mode": resolution.get("resolution_mode") if resolution else None,
        "freeze_state": freeze.get("freeze_state") if freeze else None,
        "quality_label": issue.get("quality_label"),
        "reason_for_label": issue.get("reason_for_label"),
    }
    return row


def _metric_counts(rows: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    rows = list(rows)
    return {
        "total_packets": len(rows),
        "admissible_packets": sum(1 for row in rows if row.get("admissibility_status") in {"admissible", "admissible_clean", "admissible_tainted"}),
        "admissible_clean_packets": sum(1 for row in rows if row.get("admissibility_status") == "admissible_clean"),
        "admissible_tainted_packets": sum(1 for row in rows if row.get("admissibility_status") == "admissible_tainted"),
        "upstream_blocked_packet_count": sum(1 for row in rows if row.get("admissibility_status") == "upstream_blocked_packet"),
        "missing_scope_packets": sum(1 for row in rows if "MISSING_TARGET_SCOPE" in (row.get("admissibility_reason_codes") or []) or "AMBIGUOUS_TARGET_SCOPE" in (row.get("admissibility_reason_codes") or [])),
        "no_target_section_entry_packets": sum(1 for row in rows if not row.get("has_target_section_entry_page")),
        "summary_index_misuse_packets": sum(1 for row in rows if "SUMMARY_INDEX_PRIMARY_ONLY" in (row.get("admissibility_reason_codes") or [])),
        "transition_page_misuse_packets": sum(1 for row in rows if "TRANSITION_PRIMARY_ONLY" in (row.get("admissibility_reason_codes") or [])),
        "grouped_trace_incomplete_count": sum(
            1
            for row in rows
            if row.get("issue_type") == "GROUPED_CONTEXT_NEEDS_EXPLANATION"
            and row.get("admissibility_status") in {"admissible", "admissible_clean", "admissible_tainted"}
            and row.get("freeze_state") is None
        ),
        "weak_issues": sum(1 for row in rows if _is_weak_issue(row)),
        "live_selected_weak_issues": 0,
        "structurally_unfit_packets_reaching_llm": sum(
            1
            for row in rows
            if row.get("admissibility_status") not in {"admissible", "admissible_clean", "admissible_tainted"}
            and row.get("llm_attempted")
        ),
        "cross_lot_contamination_count": sum(1 for row in rows if row.get("cross_scope_contamination_detected")),
        "contaminated_admissible_packets": sum(
            1
            for row in rows
            if row.get("cross_scope_contamination_detected")
            and row.get("admissibility_status") in {"admissible_clean", "admissible_tainted"}
        ),
        "contaminated_blocked_packets": sum(
            1
            for row in rows
            if row.get("cross_scope_contamination_detected")
            and row.get("admissibility_status") == "upstream_blocked_packet"
        ),
        "contaminated_admissible_empty_reason_codes": sum(
            1
            for row in rows
            if row.get("cross_scope_contamination_detected")
            and row.get("admissibility_status") in {"admissible_clean", "admissible_tainted"}
            and not (row.get("admissibility_reason_codes") or [])
        ),
    }


def _is_weak_issue(row: Dict[str, Any]) -> bool:
    return (
        row.get("admissibility_status") == "admissible"
        or row.get("admissibility_status") == "admissible_clean"
        or row.get("admissibility_status") == "admissible_tainted"
    ) and (
        not row.get("has_target_section_entry_page")
        or row.get("uses_summary_or_index_page")
        or row.get("uses_transition_page")
        or row.get("cross_scope_contamination_detected")
    )


def _has_resolution_evidence(issue: Dict[str, Any]) -> bool:
    for key in ("source_pages", "anchor_pages", "local_text_windows", "supporting_candidates", "supporting_blocked_entries"):
        value = issue.get(key)
        if isinstance(value, list) and value:
            return True
    return False


def _selected_weak_issue_count(pack: Dict[str, Any]) -> int:
    count = 0
    for issue in pack.get("issues") or []:
        if not isinstance(issue, dict):
            continue
        if issue.get("needs_llm") is False:
            continue
        if issue.get("admissibility_status", "admissible_clean") != "admissible_clean":
            continue
        if (issue.get("page_selection") or {}).get("llm_safe") is False:
            continue
        if not _has_resolution_evidence(issue):
            continue
        if not _is_weak_issue(_packet_row("", issue, None, None)):
            continue
        count += 1
    return count


def _family_breakdown(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
    families = defaultdict(list)
    for row in rows:
        families[str(row.get("family") or "unknown")].append(row)
    return {family: _metric_counts(items) for family, items in sorted(families.items())}


def snapshot_corpus() -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    stage_audit: List[Dict[str, Any]] = []
    for case_key in list_case_keys():
        ctx = build_context(case_key)
        artifact_dir = ctx.artifact_dir
        issue_pack = _load_json(artifact_dir / "clarification_issue_pack.json") or {}
        normalized_issue_pack = {
            **issue_pack,
            "issues": [
                issue | {"admissibility_status": issue.get("admissibility_status", "admissible")}
                | {"contamination_disposition": issue.get("contamination_disposition", "none")}
                for issue in issue_pack.get("issues") or []
                if isinstance(issue, dict)
            ],
            "tainted_packets": [issue for issue in issue_pack.get("tainted_packets") or [] if isinstance(issue, dict)],
            "blocked_packets": [issue for issue in issue_pack.get("blocked_packets") or [] if isinstance(issue, dict)],
        }
        resolution_pack = _load_json(artifact_dir / "llm_resolution_pack.json") or {}
        doc_map = _load_json(artifact_dir / "doc_map.json") or {}
        resolution_lookup = _resolution_lookup(resolution_pack)
        freeze_lookup = _freeze_lookup(doc_map)
        all_packets = (
            list(normalized_issue_pack.get("issues") or [])
            + list(normalized_issue_pack.get("tainted_packets") or [])
            + list(normalized_issue_pack.get("blocked_packets") or [])
        )
        for issue in all_packets:
            resolution = resolution_lookup.get(str(issue.get("issue_id") or ""))
            freeze = freeze_lookup.get(str(issue.get("issue_id") or ""))
            rows.append(_packet_row(case_key, issue, resolution, freeze))
        selected_count = _selected_weak_issue_count(normalized_issue_pack)
        stage_audit.append({
            "case_key": case_key,
            "issue_count": len(normalized_issue_pack.get("issues") or []),
            "tainted_packet_count": len(normalized_issue_pack.get("tainted_packets") or []),
            "blocked_packet_count": len(normalized_issue_pack.get("blocked_packets") or []),
            "selected_weak_issue_count": selected_count,
            "resolution_count": len(resolution_pack.get("resolutions") or []),
            "blocked_items_count": len(doc_map.get("blocked_items") or []),
            "grouped_llm_explanations_count": len(doc_map.get("grouped_llm_explanations") or []),
        })
    metrics = _metric_counts(rows)
    metrics["live_selected_weak_issues"] = sum(item["selected_weak_issue_count"] for item in stage_audit)
    return {
        "metrics": metrics,
        "family_breakdown": _family_breakdown(rows),
        "packet_rows": rows,
        "stage_audit": stage_audit,
    }


def _diff_metrics(before: Dict[str, int], after: Dict[str, int]) -> Dict[str, Dict[str, int]]:
    keys = sorted(set(before) | set(after))
    return {
        key: {
            "before": int(before.get(key, 0)),
            "after": int(after.get(key, 0)),
            "delta": int(after.get(key, 0)) - int(before.get(key, 0)),
        }
        for key in keys
    }


def write_outputs(before: Dict[str, Any], after: Dict[str, Any]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    before_metrics = _metric_counts(before.get("packet_rows", []))
    after_metrics = _metric_counts(after.get("packet_rows", []))
    before_metrics["live_selected_weak_issues"] = sum(
        item.get("selected_weak_issue_count", item.get("selected_issue_count", before_metrics["weak_issues"]))
        for item in before.get("stage_audit", [])
    ) or before_metrics["weak_issues"]
    after_metrics["live_selected_weak_issues"] = sum(
        item.get("selected_weak_issue_count", item.get("selected_issue_count", after_metrics["weak_issues"]))
        for item in after.get("stage_audit", [])
    ) or after_metrics["weak_issues"]
    metric_diff = _diff_metrics(before_metrics, after_metrics)
    family_before = before.get("family_breakdown", {})
    family_after = after.get("family_breakdown", {})
    family_diff = {
        family: _diff_metrics(family_before.get(family, {}), family_after.get(family, {}))
        for family in sorted(set(family_before) | set(family_after))
    }
    before_rows = {row["issue_id"]: row for row in before.get("packet_rows", []) if row.get("issue_id")}
    after_rows = {row["issue_id"]: row for row in after.get("packet_rows", []) if row.get("issue_id")}
    changed_examples: List[Dict[str, Any]] = []
    for issue_id, after_row in after_rows.items():
        before_row = before_rows.get(issue_id)
        if before_row and before_row.get("admissibility_status") != after_row.get("admissibility_status"):
            changed_examples.append({
                "issue_id": issue_id,
                "case_key": after_row.get("case_key"),
                "family": after_row.get("family"),
                "before_status": before_row.get("admissibility_status"),
                "after_status": after_row.get("admissibility_status"),
                "before_reasons": before_row.get("admissibility_reason_codes"),
                "after_reasons": after_row.get("admissibility_reason_codes"),
            })
    summary = {
        "before_metrics": before_metrics,
        "after_metrics": after_metrics,
        "metric_diff": metric_diff,
        "family_breakdown": family_diff,
    }
    (OUTPUT_DIR / "pipeline_settlement_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (OUTPUT_DIR / "before_after_examples.json").write_text(
        json.dumps(changed_examples[:50], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (OUTPUT_DIR / "agent_stage_audit.json").write_text(
        json.dumps(after.get("stage_audit", []), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (OUTPUT_DIR / "multilot_issue_quality_audit.json").write_text(
        json.dumps(
            [row for row in after.get("packet_rows", []) if row.get("case_key") == "multilot_69_2024"],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (OUTPUT_DIR / "packet_admissibility_audit.json").write_text(
        json.dumps(after.get("packet_rows", []), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    lines = [
        "# Pipeline Settlement Report",
        "",
        "## Corpus Metrics",
    ]
    for key, row in metric_diff.items():
        lines.append(f"- {key}: before={row['before']} after={row['after']} delta={row['delta']}")
    lines.append("")
    lines.append("## Family Breakdown")
    for family, metrics in family_diff.items():
        lines.append(f"- {family}")
        for key, row in metrics.items():
            lines.append(f"  - {key}: before={row['before']} after={row['after']} delta={row['delta']}")
    (OUTPUT_DIR / "pipeline_settlement_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline settlement snapshot and diff")
    sub = parser.add_subparsers(dest="cmd", required=True)
    snap = sub.add_parser("snapshot")
    snap.add_argument("--output", required=True)
    compare = sub.add_parser("compare")
    compare.add_argument("--before", required=True)
    compare.add_argument("--after", required=True)
    args = parser.parse_args()

    if args.cmd == "snapshot":
        snapshot = snapshot_corpus()
        Path(args.output).write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
        return

    before = _load_json(Path(args.before)) or {}
    after = _load_json(Path(args.after)) or {}
    write_outputs(before, after)


if __name__ == "__main__":
    main()
