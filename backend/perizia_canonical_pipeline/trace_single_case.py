"""
Bounded single-case canonical trace runner.

This module is observability-only: it runs the accepted canonical pipeline for
one existing corpus case, summarizes each stage artifact, and writes trace
artifacts. It does not replace or alter canonical business logic.
"""
from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from .bene_header_spine import build_bene_header_spine
from .bene_scope_map import build_bene_scope_map
from .cadastral_candidate_pack import build_cadastral_candidate_pack
from .corpus_registry import list_case_keys
from .cost_candidate_pack import build_cost_candidate_pack
from .doc_map_freeze import run_freeze
from .evidence_ledger import build_evidence_ledger
from .extract import extract_case
from .impianti_candidate_pack import build_impianti_candidate_pack
from .llm_clarification_issue_pack import build_clarification_issue_pack, select_issues
from .llm_resolution_pack import LLMResolutionUnavailable, build_llm_resolution_pack
from .location_candidate_pack import build_location_candidate_pack
from .lot_header_spine import build_spine as build_lot_header_spine
from .lot_scope_map import build_lot_scope_map
from .missing_slot_review import _build_synthetic_issue
from .occupancy_candidate_pack import build_occupancy_candidate_pack
from .plurality import scan_case
from .plurality_headers import classify_case
from .pre_canon_validator import validate_case
from .rights_candidate_pack import build_rights_candidate_pack
from .runner import build_context, write_manifest
from .structure_hypotheses import build_structure_hypotheses
from .structure_precheck import build_structure_precheck
from .table_zone_map import build_table_zone_map
from .valuation_candidate_pack import build_valuation_candidate_pack


StageFn = Callable[[str], Any]


STAGE_ORDER: List[Tuple[str, Optional[StageFn], Sequence[str]]] = [
    ("extract", extract_case, ("raw_pages.json", "extract_metrics.json")),
    ("plurality", scan_case, ("plurality_signals.json",)),
    ("plurality_headers", classify_case, ("plurality_headers.json",)),
    ("lot_header_spine", build_lot_header_spine, ("lot_header_spine.json",)),
    ("structure_precheck", build_structure_precheck, ("structure_precheck.json",)),
    ("structure_hypotheses", build_structure_hypotheses, ("structure_hypotheses.json",)),
    ("lot_scope_map", build_lot_scope_map, ("lot_scope_map.json",)),
    ("bene_header_spine", build_bene_header_spine, ("bene_header_spine.json",)),
    ("bene_scope_map", build_bene_scope_map, ("bene_scope_map.json",)),
    ("table_zone_map", build_table_zone_map, ("table_zone_map.json",)),
    ("cadastral_candidate_pack", build_cadastral_candidate_pack, ("cadastral_candidate_pack.json",)),
    ("location_candidate_pack", build_location_candidate_pack, ("location_candidate_pack.json",)),
    ("rights_candidate_pack", build_rights_candidate_pack, ("rights_candidate_pack.json",)),
    ("occupancy_candidate_pack", build_occupancy_candidate_pack, ("occupancy_candidate_pack.json",)),
    ("valuation_candidate_pack", build_valuation_candidate_pack, ("valuation_candidate_pack.json",)),
    ("cost_candidate_pack", build_cost_candidate_pack, ("cost_candidate_pack.json",)),
    ("impianti_candidate_pack", build_impianti_candidate_pack, ("impianti_candidate_pack.json",)),
    ("evidence_ledger", build_evidence_ledger, ("evidence_ledger.json",)),
    ("llm_clarification_issue_pack", build_clarification_issue_pack, ("clarification_issue_pack.json",)),
    ("llm_resolution_pack", None, ("llm_resolution_pack.json",)),
    ("pre_canon_validator", validate_case, ("pre_canon_validation_report.json",)),
    # missing_slot_review and missing_slot_escalation are produced inside
    # doc_map freeze; the trace runner invokes freeze once at this stage.
    ("missing_slot_review", None, ("missing_slot_review_pack.json", "missing_slot_escalation_pack.json")),
    ("doc_map_freeze", None, ("doc_map.json",)),
]


def _read_json(path: Path) -> Optional[Any]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _first_existing(paths: Iterable[Path]) -> Optional[Path]:
    for path in paths:
        if path.exists():
            return path
    return None


def _listify(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _scope_keys_from_obj(obj: Any) -> List[str]:
    scopes = set()

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            for key in ("scope_key", "scope", "target_scope"):
                raw = value.get(key)
                if isinstance(raw, str) and raw:
                    scopes.add(raw)
            lot_id = value.get("lot_id")
            bene_id = value.get("bene_id")
            if lot_id is not None and bene_id is not None:
                scopes.add(f"bene:{lot_id}/{bene_id}")
            elif lot_id is not None:
                scopes.add(f"lot:{lot_id}")
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(obj)
    return sorted(scopes)


def _field_state_counts(doc_map: Dict[str, Any]) -> Dict[str, int]:
    counts: Counter[str] = Counter()
    for scope_data in (doc_map.get("fields") or {}).values():
        for family_data in scope_data.values():
            for entry in family_data.values():
                counts[str(entry.get("state", "unknown"))] += 1
    return dict(counts)


def _artifact_summary(stage_name: str, artifact_data: Any) -> Dict[str, Any]:
    data = artifact_data
    summary: Dict[str, Any] = {}
    if isinstance(data, list):
        summary["items_count"] = len(data)
        if stage_name == "extract":
            summary["raw_pages_count"] = len(data)
            quality_counts = Counter(
                str(row.get("quality_tier", "unknown"))
                for row in data
                if isinstance(row, dict)
            )
            if quality_counts:
                summary["quality_tiers"] = dict(sorted(quality_counts.items()))
        return summary

    if not isinstance(data, dict):
        return summary

    for key in (
        "status",
        "winner",
        "scope_mode",
        "bene_scope_mode",
        "review_count",
        "escalation_count",
        "llm_succeeded",
        "issue_count",
        "freeze_ready",
        "freeze_status",
    ):
        if key in data:
            summary[key] = data.get(key)

    list_keys = [
        "raw_pages",
        "lot_scopes",
        "bene_scopes",
        "table_zones",
        "zones",
        "candidates",
        "packets",
        "blocked_zones",
        "issues",
        "resolutions",
        "reviews",
        "escalations",
        "unresolved_items",
        "context_items",
        "blocked_items",
    ]
    for key in list_keys:
        value = data.get(key)
        if isinstance(value, list):
            summary[f"{key}_count"] = len(value)

    if stage_name == "doc_map_freeze":
        summary["state_counts"] = _field_state_counts(data)
        grouped = data.get("grouped_llm_explanations")
        if isinstance(grouped, list):
            summary["grouped_llm_explanations_count"] = len(grouped)
            if grouped:
                summary["grouped_llm_explanations_sample"] = [
                    {
                        "scope_key": g.get("scope_key"),
                        "field_type": g.get("field_type"),
                        "llm_outcome": g.get("llm_outcome"),
                        "user_visible_explanation": (g.get("user_visible_explanation") or "")[:120],
                    }
                    for g in grouped[:3]
                ]
    if "coverage" in data and isinstance(data["coverage"], dict):
        summary["coverage"] = data["coverage"]
    if "summary" in data and isinstance(data["summary"], dict):
        summary["summary"] = data["summary"]
    return summary


def _stage_truth_state(stage_name: str, data: Any) -> str:
    if isinstance(data, list):
        if stage_name == "extract":
            return f"raw pages extracted={len(data)}"
        return f"rows={len(data)}"
    if not isinstance(data, dict):
        return "artifact only"
    if stage_name == "doc_map_freeze":
        counts = _field_state_counts(data)
        return ", ".join(f"{k}={v}" for k, v in sorted(counts.items())) or "no fields"
    if stage_name == "evidence_ledger":
        packets = _listify(data.get("packets"))
        active = sum(1 for p in packets if p.get("status") == "ACTIVE")
        context = sum(1 for p in packets if p.get("status") == "CONTEXT_ONLY")
        blocked = len(_listify(data.get("blocked_zones")))
        return f"deterministic packets active={active}, context={context}, blocked_zones={blocked}"
    if stage_name in {"llm_clarification_issue_pack", "llm_resolution_pack"}:
        return f"issues={len(_listify(data.get('issues')))}, resolutions={len(_listify(data.get('resolutions')))}"
    if stage_name == "pre_canon_validator":
        return f"status={data.get('status')}, freeze_ready={data.get('freeze_ready')}"
    if stage_name == "missing_slot_review":
        return f"reviews={data.get('review_count')}, escalations={data.get('escalation_count')}"
    return data.get("status") or "completed"


def _truncate_issue_windows(issue: Dict[str, Any], max_chars: int = 1600) -> Dict[str, Any]:
    """Keep issue-level structure, but cap large evidence text windows in trace files."""
    cloned = json.loads(json.dumps(issue, ensure_ascii=False))
    for window in cloned.get("local_text_windows") or []:
        text = window.get("text")
        if isinstance(text, str) and len(text) > max_chars:
            window["text"] = text[:max_chars] + "…"
            window["text_truncated"] = True
            window["original_text_chars"] = len(text)
    return cloned


def _evidence_summary(issue: Dict[str, Any], resolution: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for page in issue.get("source_pages") or []:
        rows.append({"page": page, "source": "issue_source_pages"})
    for quote in issue.get("shell_quotes") or []:
        rows.append({"snippet": quote, "source": "shell_quotes"})
    for window in issue.get("local_text_windows") or []:
        rows.append({
            "page": window.get("page"),
            "snippet": str(window.get("text") or "")[:240],
            "source": window.get("window_type") or "local_text_window",
        })
    if resolution:
        for ev in resolution.get("supporting_evidence") or []:
            if isinstance(ev, dict):
                rows.append({
                    "page": ev.get("page"),
                    "snippet": ev.get("quote"),
                    "reason": ev.get("reason"),
                    "source": "llm_supporting_evidence",
                })
    return rows


def _find_freeze_entry_by_issue(doc_map: Dict[str, Any], issue_id: str) -> Optional[Dict[str, Any]]:
    for scope_key, scope_data in (doc_map.get("fields") or {}).items():
        for family, family_data in scope_data.items():
            for field_type, entry in family_data.items():
                if issue_id in (entry.get("issue_ids") or []):
                    return {
                        "scope_key": scope_key,
                        "family": family,
                        "field_type": field_type,
                        "state": entry.get("state"),
                        "value": entry.get("value"),
                        "source_stage": entry.get("source_stage"),
                        "why_not_resolved": entry.get("why_not_resolved"),
                        "needs_human_review": entry.get("needs_human_review"),
                        "scope_guard": entry.get("scope_guard"),
                    }
    return None


def _artifact_meta(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    stat = path.stat()
    return {
        "path": str(path),
        "size_bytes": stat.st_size,
        "mtime": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
    }


def _artifact_is_current(path: Path, stage_started_at: float) -> bool:
    if not path.exists():
        return False
    # Allow a small tolerance for filesystem timestamp granularity.
    return path.stat().st_mtime >= stage_started_at - 1.0


def _raw_pages_idx(artifact_dir: Path) -> Dict[int, str]:
    raw = _read_json(artifact_dir / "raw_pages.json") or []
    return {
        row["page_number"]: row.get("text", "")
        for row in raw
        if isinstance(row, dict) and isinstance(row.get("page_number"), int)
    }


def _build_llm_call_trace(
    artifact_dir: Path,
    case_key: str,
    current_artifacts: Optional[set[str]] = None,
) -> List[Dict[str, Any]]:
    doc_map = _read_json(artifact_dir / "doc_map.json") or {}
    rows: List[Dict[str, Any]] = []

    resolution_pack_path = artifact_dir / "llm_resolution_pack.json"
    resolution_pack = (
        _read_json(resolution_pack_path)
        if current_artifacts is None or str(resolution_pack_path) in current_artifacts
        else {}
    ) or {}
    issue_by_id = {
        issue.get("issue_id"): issue
        for issue in resolution_pack.get("issues") or []
        if isinstance(issue, dict)
    }
    for resolution in resolution_pack.get("resolutions") or []:
        if not isinstance(resolution, dict):
            continue
        issue_id = resolution.get("issue_id")
        issue = issue_by_id.get(issue_id) or {}
        rows.append({
            "source_stage": "llm_resolution_pack",
            "issue_id": issue_id,
            "issue_type": issue.get("issue_type"),
            "target_scope": issue.get("scope_metadata", {}).get("scope_key") or issue.get("scope_key"),
            "family": issue.get("field_family"),
            "field": issue.get("field_type"),
            "evidence": _evidence_summary(issue, resolution),
            "structured_payload_sent": _truncate_issue_windows(issue) if issue else None,
            "structured_llm_response": resolution,
            "llm_attempted": True,
            "llm_succeeded": True,
            "llm_error": None,
            "post_guard_result_after_freeze": _find_freeze_entry_by_issue(doc_map, str(issue_id)),
        })

    review_pack_path = artifact_dir / "missing_slot_review_pack.json"
    escalation_pack_path = artifact_dir / "missing_slot_escalation_pack.json"
    review_pack = (
        _read_json(review_pack_path)
        if current_artifacts is None or str(review_pack_path) in current_artifacts
        else {}
    ) or {}
    escalation_pack = (
        _read_json(escalation_pack_path)
        if current_artifacts is None or str(escalation_pack_path) in current_artifacts
        else {}
    ) or {}
    review_by_id = {
        review.get("review_id"): review
        for review in review_pack.get("reviews") or []
        if isinstance(review, dict)
    }
    raw_idx = _raw_pages_idx(artifact_dir)
    for record in escalation_pack.get("escalations") or []:
        if not isinstance(record, dict):
            continue
        review = review_by_id.get(record.get("review_id")) or {}
        scope_key = record.get("scope_key") or review.get("scope_key")
        family = record.get("field_family") or review.get("field_family")
        field_type = record.get("field_type") or review.get("field_type")
        payload: Dict[str, Any] = {}
        if review and scope_key and family and field_type:
            payload = _build_synthetic_issue(
                case_key=case_key,
                scope_key=str(scope_key),
                family=str(family),
                field_type=str(field_type),
                review=review,
                raw_pages_idx=raw_idx,
            )
        resolution = record.get("resolution") if isinstance(record.get("resolution"), dict) else None
        issue_id = record.get("issue_id") or payload.get("issue_id")
        rows.append({
            "source_stage": "missing_slot_escalation",
            "issue_id": issue_id,
            "issue_type": payload.get("issue_type") or "SUSPICIOUS_SILENCE",
            "target_scope": scope_key,
            "family": family,
            "field": field_type,
            "evidence": _evidence_summary(payload, resolution),
            "structured_payload_sent": _truncate_issue_windows(payload) if payload else None,
            "structured_llm_response": resolution,
            "llm_attempted": _llm_call_attempted(record, resolution),
            "llm_succeeded": _llm_call_succeeded(record, resolution),
            "llm_error": record.get("error"),
            "post_guard_result_after_freeze": _find_freeze_entry_by_issue(doc_map, str(issue_id)),
        })

    # Grouped LLM explanations from doc_map — surfaced as bounded contextual outputs.
    # These are family-level/scope-ambiguity resolutions that do not map to a single
    # field slot.  They appear here so they are visible in the final analysis trace,
    # not silently stored only in doc_map.json.
    for grouped in doc_map.get("grouped_llm_explanations") or []:
        if not isinstance(grouped, dict):
            continue
        rows.append({
            "source_stage": "grouped_llm_explanation",
            "issue_id": grouped.get("issue_id"),
            "issue_type": "GROUPED_FAMILY_LEVEL",
            "target_scope": grouped.get("scope_key"),
            "family": grouped.get("field_type"),
            "field": grouped.get("field_type"),
            "evidence": [],
            "structured_payload_sent": None,
            "structured_llm_response": {
                "llm_outcome": grouped.get("llm_outcome"),
                "user_visible_explanation": grouped.get("user_visible_explanation"),
                "why_not_resolved": grouped.get("why_not_resolved"),
                "confidence_band": grouped.get("confidence_band"),
                "needs_human_review": grouped.get("needs_human_review"),
                "lot_id": grouped.get("lot_id"),
                "bene_id": grouped.get("bene_id"),
            },
            "llm_attempted": True,
            "llm_succeeded": True,
            "llm_error": None,
            "post_guard_result_after_freeze": None,
        })

    return rows


def _llm_error_implies_attempt(error: Optional[str]) -> bool:
    if not error:
        return False
    not_attempted_markers = (
        "OPENAI_API_KEY missing",
        "OPENAI_API_KEY not configured",
        "No matching clarification issues",
    )
    return not any(marker in error for marker in not_attempted_markers)


def _llm_call_attempted(record: Dict[str, Any], resolution: Optional[Dict[str, Any]]) -> bool:
    return bool(record.get("llm_attempted") or resolution or _llm_error_implies_attempt(record.get("error")))


def _llm_call_succeeded(record: Dict[str, Any], resolution: Optional[Dict[str, Any]]) -> bool:
    return bool(resolution or record.get("llm_outcome"))


def _llm_stage_activity(stage_name: str, data: Any, error: Optional[str]) -> Dict[str, Any]:
    activity: Dict[str, Any] = {
        "attempted": False,
        "succeeded": False,
        "attempt_count": 0,
        "success_count": 0,
        "error_count": 0,
        "errors": [],
    }
    if stage_name == "llm_resolution_pack":
        resolutions = _listify(data.get("resolutions")) if isinstance(data, dict) else []
        attempted = bool(resolutions) or _llm_error_implies_attempt(error)
        activity["attempted"] = attempted
        activity["succeeded"] = bool(resolutions)
        activity["attempt_count"] = len(resolutions) if resolutions else int(attempted)
        activity["success_count"] = len(resolutions)
        activity["error_count"] = int(bool(error))
        activity["errors"] = [error] if error else []
        return activity
    if stage_name == "missing_slot_review":
        escalations = _listify(data.get("escalations")) if isinstance(data, dict) else []
        attempts = 0
        successes = 0
        errors: List[str] = []
        for record in escalations:
            if not isinstance(record, dict):
                continue
            resolution = record.get("resolution") if isinstance(record.get("resolution"), dict) else None
            if _llm_call_attempted(record, resolution):
                attempts += 1
            if _llm_call_succeeded(record, resolution):
                successes += 1
            if record.get("error"):
                errors.append(str(record["error"]))
        activity["attempted"] = attempts > 0
        activity["succeeded"] = successes > 0
        activity["attempt_count"] = attempts
        activity["success_count"] = successes
        activity["error_count"] = len(errors)
        activity["errors"] = sorted(set(errors))
        return activity
    return activity


def _write_report(
    *,
    case_key: str,
    stage_rows: List[Dict[str, Any]],
    llm_rows: List[Dict[str, Any]],
    out_path: Path,
) -> None:
    lines = [
        f"# Single Case Canonical Trace: {case_key}",
        "",
        f"Generated at: {datetime.now(timezone.utc).isoformat()}",
        "",
        "## Stage Trace",
        "",
    ]
    previous_outputs: List[str] = []
    for row in stage_rows:
        stage = row["stage"]
        lines.extend([
            f"### {stage}",
            "",
            f"- Input: {', '.join(previous_outputs) if previous_outputs else 'corpus case PDF and registry metadata'}",
            f"- Output artifact(s): {', '.join(row.get('artifact_paths') or ['none'])}",
            f"- LLM attempted: {'yes' if row.get('llm_attempted') else 'no'}",
            f"- LLM succeeded: {'yes' if row.get('llm_succeeded') else 'no'}",
            f"- Scopes touched: {', '.join(row.get('scopes_touched') or []) or 'none reported'}",
            f"- Truth/state: {row.get('truth_state')}",
            f"- Summary: `{json.dumps(row.get('summary', {}), ensure_ascii=False)}`",
            f"- Artifact metadata: `{json.dumps(row.get('artifact_metadata', []), ensure_ascii=False)}`",
            f"- State change: {row.get('state_change')}",
            "",
        ])
        previous_outputs = row.get("artifact_paths") or previous_outputs

    lines.extend([
        "## LLM Calls",
        "",
        f"Total bounded LLM-touched issues recorded: {len(llm_rows)}",
        "",
    ])
    for row in llm_rows:
        result = row.get("post_guard_result_after_freeze") or {}
        resp = row.get("structured_llm_response") or {}
        lines.extend([
            f"### {row.get('source_stage')} / {row.get('issue_id')}",
            "",
            f"- Issue type: {row.get('issue_type')}",
            f"- Target scope: {row.get('target_scope')}",
            f"- Family/field: {row.get('family')} / {row.get('field')}",
            f"- LLM attempted: {'yes' if row.get('llm_attempted') else 'no'}",
            f"- LLM succeeded: {'yes' if row.get('llm_succeeded') else 'no'}",
            f"- LLM error: {row.get('llm_error') or 'none'}",
            f"- LLM outcome: {resp.get('llm_outcome')}",
            f"- Freeze result: {result.get('state')} value={result.get('value')!r}",
            f"- Evidence items: {len(row.get('evidence') or [])}",
        ])
        if row.get("source_stage") == "grouped_llm_explanation":
            expl = resp.get("user_visible_explanation") or ""
            lines.append(f"- Explanation (grouped/contextual): {expl[:300]}")
        lines.append("")
    out_path.write_text("\n".join(lines), encoding="utf-8")


def _print_stage(row: Dict[str, Any]) -> None:
    print(f"\n[{row['stage']}]")
    print(f"  artifact: {', '.join(row.get('artifact_paths') or ['none'])}")
    print(f"  summary: {json.dumps(row.get('summary', {}), ensure_ascii=False)}")
    print(f"  scopes: {', '.join(row.get('scopes_touched') or []) or 'none'}")
    print(f"  llm_attempted: {'yes' if row.get('llm_attempted') else 'no'}")
    print(f"  llm_succeeded: {'yes' if row.get('llm_succeeded') else 'no'}")
    if row.get("error"):
        print(f"  error: {row['error']}")


def run_trace(case_key: str, output_dir: Optional[Path] = None) -> Dict[str, Path]:
    if case_key not in list_case_keys():
        candidate = Path(case_key)
        if candidate.exists():
            raise SystemExit(
                "Direct PDF tracing is not supported cleanly by the current corpus "
                "registry-backed pipeline. Add the PDF to the working corpus first."
            )
        raise SystemExit(f"Unknown case key: {case_key}")

    ctx = build_context(case_key)
    write_manifest(ctx)
    trace_dir = output_dir or ctx.artifact_dir
    trace_dir.mkdir(parents=True, exist_ok=True)

    stage_rows: List[Dict[str, Any]] = []
    failed_llm_trace_rows: List[Dict[str, Any]] = []
    freeze_already_run = False

    for stage_name, func, artifact_names in STAGE_ORDER:
        error: Optional[str] = None
        stage_started_at = time.time()
        if stage_name == "llm_resolution_pack":
            try:
                build_llm_resolution_pack(case_key)
            except LLMResolutionUnavailable as exc:
                error = str(exc)
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
            if error:
                issue_pack = _read_json(ctx.artifact_dir / "clarification_issue_pack.json") or {}
                selected = select_issues(issue_pack, limit=1)
                for issue in selected:
                    failed_llm_trace_rows.append({
                        "source_stage": "llm_resolution_pack",
                        "issue_id": issue.get("issue_id"),
                        "issue_type": issue.get("issue_type"),
                        "target_scope": issue.get("scope_metadata", {}).get("scope_key") or issue.get("scope_key"),
                        "family": issue.get("field_family"),
                        "field": issue.get("field_type"),
                        "evidence": _evidence_summary(issue, None),
                        "structured_payload_sent": _truncate_issue_windows(issue),
                        "structured_llm_response": None,
                        "llm_attempted": True,
                        "llm_succeeded": False,
                        "llm_error": error,
                        "post_guard_result_after_freeze": None,
                    })
        elif stage_name == "missing_slot_review":
            run_freeze(case_key)
            freeze_already_run = True
        elif stage_name == "doc_map_freeze":
            if not freeze_already_run:
                run_freeze(case_key)
        elif func is not None:
            func(case_key)

        artifact_paths = [ctx.artifact_dir / name for name in artifact_names]
        current_artifact_paths = [
            path for path in artifact_paths if _artifact_is_current(path, stage_started_at)
        ]
        stale_artifact_paths = [
            path for path in artifact_paths if path.exists() and path not in current_artifact_paths
        ]
        primary = _first_existing(current_artifact_paths)
        primary_data = _read_json(primary) if primary else None
        if stage_name == "missing_slot_review":
            review_path = ctx.artifact_dir / "missing_slot_review_pack.json"
            escalation_path = ctx.artifact_dir / "missing_slot_escalation_pack.json"
            review = (
                _read_json(review_path)
                if _artifact_is_current(review_path, stage_started_at)
                else {}
            ) or {}
            escalation = (
                _read_json(escalation_path)
                if _artifact_is_current(escalation_path, stage_started_at)
                else {}
            ) or {}
            primary_data = {
                **review,
                "escalation_count": escalation.get("escalation_count", 0),
                "llm_succeeded": escalation.get("llm_succeeded", 0),
                "escalations": escalation.get("escalations", []),
                "provider": escalation.get("provider"),
                "model": escalation.get("model"),
            }

        summary = _artifact_summary(stage_name, primary_data or {})
        if error:
            summary["error"] = error
        if stale_artifact_paths:
            summary["stale_artifacts_ignored"] = [str(path) for path in stale_artifact_paths]

        llm_activity = _llm_stage_activity(stage_name, primary_data, error)
        row = {
            "stage": stage_name,
            "artifact_paths": [str(path) for path in current_artifact_paths],
            "stale_artifact_paths": [str(path) for path in stale_artifact_paths],
            "artifact_metadata": [
                meta
                for meta in (_artifact_meta(path) for path in current_artifact_paths)
                if meta is not None
            ],
            "summary": summary,
            "scopes_touched": _scope_keys_from_obj(primary_data),
            "llm_attempted": llm_activity["attempted"],
            "llm_succeeded": llm_activity["succeeded"],
            "llm_attempt_count": llm_activity["attempt_count"],
            "llm_success_count": llm_activity["success_count"],
            "llm_error_count": llm_activity["error_count"],
            "llm_errors": llm_activity["errors"],
            "llm_used": llm_activity["attempted"],
            "truth_state": _stage_truth_state(stage_name, primary_data),
            "state_change": _stage_truth_state(stage_name, primary_data),
            "error": error,
        }
        stage_rows.append(row)
        _print_stage(row)

    current_artifact_set = {
        path
        for row in stage_rows
        for path in row.get("artifact_paths", [])
    }
    llm_rows = failed_llm_trace_rows + _build_llm_call_trace(
        ctx.artifact_dir,
        case_key,
        current_artifact_set,
    )
    trace = {
        "case_key": case_key,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "artifact_dir": str(ctx.artifact_dir),
        "stages": stage_rows,
        "llm_call_trace_path": str(trace_dir / "llm_call_trace.json"),
        "report_path": str(trace_dir / "single_case_trace_report.md"),
    }

    trace_path = trace_dir / "single_case_trace.json"
    llm_path = trace_dir / "llm_call_trace.json"
    report_path = trace_dir / "single_case_trace_report.md"
    trace_path.write_text(json.dumps(trace, ensure_ascii=False, indent=2), encoding="utf-8")
    llm_path.write_text(json.dumps({
        "case_key": case_key,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "llm_call_count": len(llm_rows),
        "calls": llm_rows,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_report(case_key=case_key, stage_rows=stage_rows, llm_rows=llm_rows, out_path=report_path)

    print("\nTRACE_OUTPUTS")
    print(f"  single_case_trace_json={trace_path}")
    print(f"  llm_call_trace_json={llm_path}")
    print(f"  markdown_report={report_path}")
    return {
        "single_case_trace_json": trace_path,
        "llm_call_trace_json": llm_path,
        "markdown_report": report_path,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a bounded single-case canonical trace")
    parser.add_argument("case", help="Existing corpus case key. Direct PDF paths are not currently supported cleanly.")
    parser.add_argument("--output-dir", type=Path, help="Optional directory for trace JSON and markdown report.")
    args = parser.parse_args()
    run_trace(args.case, output_dir=args.output_dir)


if __name__ == "__main__":
    main()
