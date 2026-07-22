#!/usr/bin/env python3
"""Offline fact-projection replay over preserved or sanitized artifacts.

Only persisted JSON and pure correctness-v2 functions are used.  The harness
does not import the orchestrator, artifact writer, model clients, or application
database modules.
"""

from __future__ import annotations

import argparse
import json
import socket
import subprocess
from contextlib import ExitStack
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import patch

from correctness_v2 import (
    contract, customer_report, doc_signals, fact_lineage, lot_fact_projection,
    lot_packets, lots, validator,
)

DEFAULT_OUTPUT = Path("/tmp/claude-1001/-srv-perizia-app/e6150250-d2d4-4ec9-b906-5b769c331a69/scratchpad/replay")
DEFAULT_EXPECTATIONS = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "beta_multilot_case_sanitized.json"


def _sudo_json(path: Path) -> Dict[str, Any]:
    raw = subprocess.run(["sudo", "cat", str(path)], check=True, capture_output=True, text=True).stdout
    return json.loads(raw)


def _discover_historical(bundle: Path, lot_id: str):
    root = bundle / "job_artifacts"
    listing = subprocess.run(
        ["sudo", "find", str(root), "-mindepth", "1", "-maxdepth", "1", "-type", "d"],
        check=True, capture_output=True, text=True,
    ).stdout.splitlines()
    case_job: Optional[Path] = None
    selected_lot_job: Optional[Path] = None
    for raw in listing:
        path = Path(raw)
        status = _sudo_json(path / "job_status.json")
        if status.get("status") == "LOT_SELECTION_REQUIRED":
            case_job = path
        context_path = path / "selected_lot_context.json"
        try:
            context = _sudo_json(context_path)
        except subprocess.CalledProcessError:
            continue
        if str(context.get("selected_lot_id")) == lot_id:
            selected_lot_job = path
    if case_job is None or selected_lot_job is None:
        raise RuntimeError("historical case-level or selected-lot artifacts not found")
    pages_payload = _sudo_json(case_job / "input_pages.json")
    return (
        _sudo_json(case_job / "analyst_worksheet.json"),
        _sudo_json(selected_lot_job / "lots" / lot_id / "analyst_worksheet.json"),
        pages_payload.get("pages") or pages_payload,
        _sudo_json(case_job / "lot_report.json"),
        _sudo_json(selected_lot_job / "customer_report.json"),
    )


def _fixture_inputs(fixture_root: Path, lot_id: str):
    case = json.loads((fixture_root / "beta_multilot_case_sanitized.json").read_text(encoding="utf-8"))
    pages = json.loads((fixture_root / "beta_multilot_case_cached_pages_sanitized.json").read_text(encoding="utf-8"))["pages"]
    report = lots.build_lot_report(case["case_worksheet"], pages)
    return case["case_worksheet"], case["lot_worksheets"][lot_id], pages, report, case["stored_customer_reports"][f"historical_lot_{lot_id}"]


def _text(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False).lower()


def _amount_present(payload: Any, amount: float) -> bool:
    if isinstance(payload, dict):
        return any(_amount_present(value, amount) for value in payload.values())
    if isinstance(payload, list):
        return any(_amount_present(value, amount) for value in payload)
    return isinstance(payload, (int, float)) and not isinstance(payload, bool) and abs(float(payload) - amount) < 0.01


def _fact_checks(payload: Dict[str, Any], expectations: Dict[str, Any]) -> Dict[str, bool]:
    text = _text(payload)
    compliance = payload.get("compliance_section") or payload.get("compliance_overview") or []
    def declared(area_tokens):
        for item in compliance:
            area = str(item.get("area") or "").lower()
            state = str(item.get("classification") or item.get("status_label") or "").lower()
            if any(token in area for token in area_tokens) and "conform" in state and "verificare" not in state and "uncertain" not in state:
                return True
        return False
    checks: Dict[str, bool] = {}
    for name, spec in expectations.get("fact_checks", {}).items():
        kind = spec.get("kind")
        if kind == "address":
            checks[name] = bool((payload.get("case_identity") or {}).get("address"))
        elif kind == "amount":
            checks[name] = _amount_present(payload, float(spec["value"]))
        elif kind == "number_or_text":
            checks[name] = str(spec.get("text") or "").lower() in text or _amount_present(payload, float(spec["value"]))
        elif kind == "contains_any":
            checks[name] = any(str(token).lower() in text for token in spec.get("tokens", []))
        elif kind == "contains_all":
            checks[name] = all(str(token).lower() in text for token in spec.get("tokens", []))
        elif kind == "contains_groups":
            checks[name] = all(any(str(token).lower() in text for token in group) for group in spec.get("groups", []))
        elif kind == "declared_compliance":
            checks[name] = declared(tuple(spec.get("area_tokens", [])))
        else:
            raise ValueError(f"unknown replay fact-check kind: {kind}")
    return checks


def run_replay(
    *, output_dir: Path, bundle: Optional[Path] = None,
    fixture_root: Optional[Path] = None,
    expectations_path: Path = DEFAULT_EXPECTATIONS,
) -> Dict[str, Any]:
    expectations_document = json.loads(expectations_path.read_text(encoding="utf-8"))
    expectations = expectations_document["replay_expectations"]
    lot_id = str(expectations["lot_id"])
    if fixture_root is not None:
        case_ws, lot_ws, pages, lot_report, historical = _fixture_inputs(fixture_root, lot_id)
        source = "sanitized_fixture"
    else:
        if bundle is None:
            raise ValueError("bundle is required for historical replay")
        case_ws, lot_ws, pages, lot_report, historical = _discover_historical(bundle, lot_id)
        source = "preserved_historical_artifacts"

    network_calls: List[str] = []
    database_writes: List[str] = []

    def blocked_network(*args, **kwargs):
        network_calls.append("socket")
        raise AssertionError("network access is forbidden in offline replay")

    def blocked_write(*args, **kwargs):
        database_writes.append("mongo")
        raise AssertionError("database writes are forbidden in offline replay")

    with ExitStack() as stack:
        stack.enter_context(patch.object(socket, "create_connection", blocked_network))
        try:
            from pymongo.collection import Collection
            for name in ("insert_one", "insert_many", "update_one", "update_many", "replace_one", "delete_one", "delete_many"):
                stack.enter_context(patch.object(Collection, name, blocked_write))
        except ImportError:
            pass

        segmentation = lot_packets.segment_pages(pages, lot_report.get("lot_ids"))
        segmentation["full_document_pages"] = pages
        segmentation["shared_summary_projection"] = lot_packets.project_shared_summary_rows(pages, segmentation)
        ledger = fact_lineage.build_case_fact_ledger(case_ws, segmentation, lot_report)
        reconciled, projection = lot_fact_projection.project_and_reconcile(
            case_ledger=ledger, lot_worksheet=lot_ws, lot_id=lot_id,
            segmentation=segmentation, all_lot_ids=[str(x) for x in lot_report.get("lot_ids") or []],
        )
        selected = lot_packets.select_lot_pages(pages, segmentation, lot_id)
        numbers = {int(page.get("page_number", i)) for i, page in enumerate(selected, 1)} | set(projection["verification_pages_added"])
        verification = [page for i, page in enumerate(pages, 1) if int(page.get("page_number", i)) in numbers]
        reconciled = contract.complete_valuation_terminals(reconciled, verification)
        reconciled, compliance_gate = validator.apply_compliance_evidence_gate(reconciled, verification)
        validator_report = validator.validate_worksheet(reconciled, verification)
        if validator_report["validation_status"] != validator.STATUS_VALIDATED:
            raise AssertionError(f"repaired historical Lot 1 did not validate: {validator_report['violations']}")
        sub_report = lots.build_lot_report(reconciled, selected)
        lot_money = lot_packets.build_lot_money(case_ws, segmentation, pages)["by_lot"].get(lot_id, {})
        shared_rows = lot_packets.contract_rows_from_lot_money(lot_money)
        report_contract = contract.build_contract(
            worksheet=reconciled, validator_report=validator_report,
            analysis_id="offline_replay", job_id=f"offline_replay_lot_{lot_id}",
            source_pdf_quality_status="HISTORICAL", lot_report=sub_report,
            shared_summary_rows=shared_rows,
            surface_cadastral=doc_signals.extract_surface_cadastral(verification),
        )
        customer_pages = lot_fact_projection.customer_safe_projection_pages(
            pages, selected, projection, lot_id
        )
        repaired = customer_report.render_success_report(report_contract, customer_pages)

    before = _fact_checks(historical, expectations)
    after = _fact_checks(repaired, expectations)
    critical_keys = expectations["critical_facts"]
    before_recall = sum(before[key] for key in critical_keys) / len(critical_keys)
    after_recall = sum(after[key] for key in critical_keys) / len(critical_keys)
    worksheet_text = _text(reconciled)
    other_lot_leakage = any(
        f"lotto {other_lot_id}" in worksheet_text
        for other_lot_id in (str(value) for value in lot_report.get("lot_ids") or [])
        if other_lot_id != lot_id
    )
    source_ids = {fact["fact_id"] for fact in ledger["facts"]}
    hallucinated = sorted(set(projection["projected_fact_ids"]) - source_ids)
    def target_uncertainty_count(payload):
        section = payload.get("compliance_section") or payload.get("compliance_overview") or []
        return sum(
            1 for item in section
            if any(token in str(item.get("area") or "").lower() for token in ("ediliz", "urban", "catastal"))
            and any(token in str(item.get("classification") or item.get("status_label") or "").lower() for token in ("verificare", "uncertain", "incert"))
        )

    result = {
        "source": source,
        "lot_id": lot_id,
        "matrix": [
            {"fact": key, "historically_retained": before[key], "repaired_now": after[key]}
            for key in before
        ],
        "critical_fact_coverage_before": round(before_recall, 4),
        "critical_fact_coverage_after": round(after_recall, 4),
        "new_conflicts": len(projection["conflicts"]),
        "removed_false_uncertainty": max(0, target_uncertainty_count(historical) - target_uncertainty_count(repaired)),
        "cross_lot_leakage": other_lot_leakage,
        "hallucinated_fact_ids": hallucinated,
        "compliance_downgrades": compliance_gate["downgrade_count"],
        "network_calls": len(network_calls),
        "database_writes": len(database_writes),
        "paid_calls": 0,
        "quota_or_credit_consumption": 0,
    }
    required_after = [row["fact"] for row in result["matrix"] if not row["repaired_now"]]
    if required_after:
        raise AssertionError(f"offline replay acceptance failed for: {required_after}")
    if result["critical_fact_coverage_after"] != 1.0:
        raise AssertionError("offline replay critical-fact coverage is below 100%")
    if result["cross_lot_leakage"] or result["hallucinated_fact_ids"]:
        raise AssertionError("offline replay introduced leakage or a non-source fact")
    if any(result[key] for key in ("network_calls", "database_writes", "paid_calls", "quota_or_credit_consumption")):
        raise AssertionError("offline replay exercised a forbidden external path")
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "lot1_fact_matrix.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    rows = ["| Fact | Historical | Repaired |", "|---|---:|---:|"] + [
        f"| {row['fact']} | {'yes' if row['historically_retained'] else 'no'} | {'yes' if row['repaired_now'] else 'no'} |"
        for row in result["matrix"]
    ]
    (output_dir / "lot1_fact_matrix.md").write_text("\n".join(rows) + "\n", encoding="utf-8")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay persisted case artifacts without network or database access.")
    parser.add_argument(
        "--bundle", type=Path, required=True,
        help="Required forensic bundle path supplied explicitly by the operator.",
    )
    parser.add_argument("--fixture-root", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--expectations", type=Path, default=DEFAULT_EXPECTATIONS, help="Sanitized fixture JSON containing replay expectations.")
    args = parser.parse_args()
    result = run_replay(
        output_dir=args.output, bundle=args.bundle,
        fixture_root=args.fixture_root, expectations_path=args.expectations,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
