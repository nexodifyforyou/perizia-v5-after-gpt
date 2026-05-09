#!/usr/bin/env python3
"""Audit feature-flagged authority Money Box projection without writing data."""

from __future__ import annotations

import argparse
import copy
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from customer_decision_contract import sanitize_customer_facing_result, separate_internal_runtime_from_customer_result
from perizia_authority_money_projection import FEATURE_FLAG, apply_authority_money_projection_if_enabled
from scripts import compare_authority_money_vs_legacy as compare


STALE_RE = re.compile(r"regolarizzazion\w*\s*:\s*(?:€|\beuro\b)?\s*(?:31|6)(?:[,\.]00)?\b", re.I)
FORBIDDEN_KEYS = {
    "debug",
    "internal_runtime",
    "authority_money_projection",
    "authority_shadow_resolvers",
    "authority_score",
    "authority_level",
    "section_zone",
    "domain_hints",
    "answer_point",
    "reason_for_authority",
    "is_instruction_like",
    "is_answer_like",
    "source_stage",
    "extractor_version",
}


def _flatten_text(value: Any) -> str:
    if isinstance(value, dict):
        return " ".join(_flatten_text(child) for child in value.values())
    if isinstance(value, list):
        return " ".join(_flatten_text(item) for item in value)
    return str(value or "")


def _leak_paths(value: Any, path: str = "result") -> List[str]:
    hits: List[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            key_text = str(key)
            child_path = f"{path}.{key_text}"
            if key_text in FORBIDDEN_KEYS or key_text.startswith("authority_") or "shadow_" in key_text:
                hits.append(child_path)
            hits.extend(_leak_paths(child, child_path))
    elif isinstance(value, list):
        for idx, item in enumerate(value):
            hits.extend(_leak_paths(item, f"{path}[{idx}]"))
    return hits


def _customer_clean_result(result: Dict[str, Any]) -> Dict[str, Any]:
    clone = copy.deepcopy(result)
    sanitize_customer_facing_result(clone)
    separate_internal_runtime_from_customer_result(clone)
    return clone


def _minimal_result(label: str) -> Dict[str, Any]:
    return {
        "case_title": label,
        "money_box": {"policy": "LEGACY_EMPTY", "items": []},
        "section_3_money_box": {"policy": "LEGACY_EMPTY", "items": [], "totale_extra_budget": {"min": None, "max": None}},
        "customer_decision_contract": {
            "version": "customer_decision_contract_v1",
            "money_box": {"policy": "LEGACY_EMPTY", "items": []},
            "section_3_money_box": {"policy": "LEGACY_EMPTY", "items": [], "totale_extra_budget": {"min": None, "max": None}},
        },
    }


def _total_min_max(box: Dict[str, Any]) -> Dict[str, Any]:
    total = box.get("total_extra_costs") if isinstance(box.get("total_extra_costs"), dict) else {}
    if isinstance(total.get("range"), dict):
        return {"min": total["range"].get("min"), "max": total["range"].get("max")}
    return {"min": total.get("min"), "max": total.get("max")}


def _row_for_pages(*, file_label: str, analysis_id: str, pages: Sequence[Dict[str, Any]], result: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    notes: List[str] = []
    shadow = compare._build_shadow_for_pages(pages, analysis_id=analysis_id, notes=notes)
    target = copy.deepcopy(result) if isinstance(result, dict) and result else _minimal_result(file_label)
    before = _flatten_text(target)
    old_flag = os.environ.get(FEATURE_FLAG)
    os.environ[FEATURE_FLAG] = "1"
    try:
        meta = apply_authority_money_projection_if_enabled(
            target,
            authority_shadow=shadow,
            analysis_id=analysis_id or None,
            request_id=f"audit:{analysis_id or file_label}",
        )
    finally:
        if old_flag is None:
            os.environ.pop(FEATURE_FLAG, None)
        else:
            os.environ[FEATURE_FLAG] = old_flag
    cdc = target.get("customer_decision_contract") if isinstance(target.get("customer_decision_contract"), dict) else {}
    box = cdc.get("money_box") if isinstance(cdc.get("money_box"), dict) else target.get("money_box", {})
    clean = _customer_clean_result(target)
    leak_paths = _leak_paths(clean)
    after = _flatten_text(clean)
    stale_before = bool(STALE_RE.search(before))
    stale_after = bool(STALE_RE.search(after))
    if leak_paths:
        verdict = "FAIL_LEAK"
    elif meta.get("status") == "FAIL_OPEN":
        verdict = "FAIL_OPEN_ACCEPTABLE"
    elif stale_after:
        verdict = "FAIL_STALE_MONEY"
    else:
        verdict = "OK"
    totals = _total_min_max(box if isinstance(box, dict) else {})
    return {
        "file": file_label,
        "analysis_id": analysis_id,
        "projection_status": meta.get("status"),
        "projection_applied": bool(meta.get("applied")),
        "money_status": meta.get("money_status"),
        "projected_items_count": meta.get("projected_items_count"),
        "cost_signals_to_verify_count": meta.get("cost_signals_to_verify_count"),
        "excluded_non_buyer_cost_count": meta.get("excluded_non_buyer_cost_count"),
        "component_total_double_count_prevented": bool(meta.get("component_total_double_count_prevented")),
        "stale_before": stale_before,
        "stale_after": stale_after,
        "total_min": totals.get("min"),
        "total_max": totals.get("max"),
        "leak_count": len(leak_paths),
        "leak_paths": leak_paths[:12],
        "verdict": verdict,
        "notes": ";".join(str(note) for note in notes + list(meta.get("notes") or []) if note),
    }


def audit_analysis_id(analysis_id: str) -> Dict[str, Any]:
    extract_dir = compare.RUNS_ROOT / analysis_id / "extract"
    pages = compare._normalize_pages(compare._read_json(extract_dir / "pages_raw.json", []))
    saved = compare.load_saved_analysis(analysis_id)
    result = compare._customer_result_from_analysis(saved)
    label = str((saved or {}).get("file_name") or (saved or {}).get("case_title") or analysis_id)
    return _row_for_pages(file_label=label, analysis_id=analysis_id, pages=pages, result=result)


def audit_pdf(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {
            "file": str(path),
            "analysis_id": "",
            "projection_status": "FAIL_OPEN",
            "projection_applied": False,
            "money_status": "missing_pdf",
            "projected_items_count": 0,
            "cost_signals_to_verify_count": 0,
            "excluded_non_buyer_cost_count": 0,
            "component_total_double_count_prevented": False,
            "stale_before": False,
            "stale_after": False,
            "total_min": None,
            "total_max": None,
            "leak_count": 0,
            "leak_paths": [],
            "verdict": "FAIL_OPEN_ACCEPTABLE",
            "notes": f"missing_pdf:{path}",
        }
    pages = compare._normalize_pages(compare._extract_pdf_pages(path))
    return _row_for_pages(file_label=str(path), analysis_id="", pages=pages, result=None)


def audit_corpus() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for case in compare._fixture_cases():
        analysis_ids = [str(item) for item in case.get("analysis_ids") or [] if str(item or "").strip()]
        if analysis_ids:
            rows.extend(audit_analysis_id(analysis_id) for analysis_id in analysis_ids)
            continue
        path = compare._resolve_case_path(case)
        if path is not None:
            rows.append(audit_pdf(path))
        else:
            rows.append(audit_pdf(Path(str((case.get("paths") or [case.get("label") or "missing"])[0]))))
    return rows


def audit_sample(limit: int, seed: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for kind, value in compare._sample_sources(limit, seed):
        rows.append(audit_analysis_id(value) if kind == "analysis" else audit_pdf(Path(value)))
    return rows


def _payload(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    verdict_counts: Dict[str, int] = {}
    for row in rows:
        verdict = str(row.get("verdict") or "")
        verdict_counts[verdict] = verdict_counts.get(verdict, 0) + 1
    return {"summary": {"total": len(rows), "verdict_counts": verdict_counts}, "rows": rows}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Audit feature-flagged authority money projection.")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--corpus", action="store_true")
    mode.add_argument("--sample", type=int)
    mode.add_argument("--analysis-id")
    mode.add_argument("--file")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--json-out")
    args = parser.parse_args(argv)

    if args.corpus:
        rows = audit_corpus()
    elif args.sample is not None:
        rows = audit_sample(int(args.sample or 0), int(args.seed))
    elif args.analysis_id:
        rows = [audit_analysis_id(str(args.analysis_id))]
    else:
        rows = [audit_pdf(Path(str(args.file)).expanduser())]

    payload = _payload(rows)
    if args.json or args.json_out:
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        if args.json_out:
            out = Path(args.json_out).expanduser().resolve()
            out.parent.mkdir(parents=True, exist_ok=True)
            with open(out, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
    else:
        for row in rows:
            print("\t".join(str(row.get(key, "")) for key in ("file", "analysis_id", "projection_status", "verdict", "leak_count", "notes")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
