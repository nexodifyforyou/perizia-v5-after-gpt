"""
Offline quality-loop replay for Correctness Mode v2.

Re-runs the DETERMINISTIC part of the pipeline (compliance gate -> validator ->
contract -> customer report -> quality gate) from a job's already-saved
artifacts (input_pages.json + analyst_worksheet.json). No OpenAI call, no PDF
access — pure replay for fast iteration on the quality gate.

Usage:
    python -m correctness_v2.scripts.replay_quality JOB_DIR [--out OUT_DIR]

Writes the regenerated artifacts to OUT_DIR (default: JOB_DIR + "_replay" under
the scratch root given by CORRECTNESS_V2_ARTIFACTS_ROOT) and prints a summary.
Never touches the original job folder.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


def _load(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("job_dir")
    parser.add_argument("--out", default=None)
    parser.add_argument("--selection", action="store_true",
                        help="replay a LOT_SELECTION_REQUIRED job (selector audit only)")
    parser.add_argument("--lot", default=None,
                        help="replay a selected-lot job using lots/<LOT>/analyst_worksheet.json")
    args = parser.parse_args()

    job_dir = Path(args.job_dir)
    if not job_dir.is_dir():
        print(f"not a job dir: {job_dir}", file=sys.stderr)
        return 2

    out_root = Path(
        os.environ.get("CORRECTNESS_V2_REPLAY_ROOT")
        or (job_dir.parent.parent / "replays")
    )
    out_dir = Path(args.out) if args.out else out_root / (job_dir.name + "_replay")
    out_dir.mkdir(parents=True, exist_ok=True)
    # Route artifact writes into the replay folder, never the live one.
    os.environ["CORRECTNESS_V2_ARTIFACTS_ROOT"] = str(out_dir)

    from correctness_v2 import (  # noqa: E402 (after env override)
        contract as contract_mod,
        customer_report as customer_report_mod,
        doc_signals,
        lots as lots_mod,
        quality_gate,
        validator as validator_mod,
    )

    pages = (_load(job_dir / "input_pages.json") or {}).get("pages") or []
    status = _load(job_dir / "job_status.json")
    analysis_id = status.get("analysis_id")
    job_id = "replay_" + job_dir.name

    if args.selection:
        selection = _load(job_dir / "lot_selection_required.json")
        lot_index = _load(job_dir / "lot_index.json")
        lot_report = _load(job_dir / "lot_report.json")
        report = customer_report_mod.render_lot_selection_report(selection, lot_index)
        gate = quality_gate.run_quality_gate(
            job_id=job_id, analysis_id=analysis_id, pages=pages,
            worksheet=None, contract=None, customer_report=report,
            lot_report=lot_report, lot_index=lot_index,
        )
        _summary(gate)
        return 0

    shared_summary_rows = []
    if args.lot:
        worksheet = _load(job_dir / "lots" / args.lot / "analyst_worksheet.json")
        context = _load(job_dir / "selected_lot_context.json")
        analysis_pages = set(context.get("analysis_pages") or [])
        pages = [p for p in pages if int(p.get("page_number", -1)) in analysis_pages]
        shared_summary_rows = (context.get("lot_money") or {}).get("shared_summary_rows") or []
    else:
        worksheet = _load(job_dir / "analyst_worksheet.json")
    worksheet.pop("_saved_at", None)
    # Match the orchestrator: complete the valuation chain's terminal net values
    # before validation/contract/gate (grounded doc-signals authority on the
    # lot's pages + promotion from mis-slotted uncertain_money).
    worksheet = contract_mod.complete_valuation_terminals(worksheet, pages)
    worksheet, gate_report = validator_mod.apply_compliance_evidence_gate(worksheet, pages)
    validator_report = validator_mod.validate_worksheet(worksheet, pages)
    if validator_report.get("validation_status") != validator_mod.STATUS_VALIDATED:
        print("VALIDATION FAILED:", [v.get("code") for v in validator_report.get("violations", [])])
        return 1
    lot_report = lots_mod.build_lot_report(worksheet, pages)
    contract = contract_mod.build_contract(
        worksheet=worksheet,
        validator_report=validator_report,
        analysis_id=analysis_id,
        job_id=job_id,
        source_pdf_quality_status="PDF_QUALITY_OK",
        lot_report=lot_report,
        shared_summary_rows=shared_summary_rows,
        surface_cadastral=doc_signals.extract_surface_cadastral(pages),
    )
    report = customer_report_mod.render_success_report(contract, pages)
    gate = quality_gate.run_quality_gate(
        job_id=job_id, analysis_id=analysis_id, pages=pages,
        worksheet=worksheet, contract=contract, customer_report=report,
        validator_report=validator_report, lot_report=lot_report,
    )
    _summary(gate)
    return 0


def _summary(gate) -> None:
    audit = gate["coverage_audit"]
    quality = gate["quality_report"]
    scorecard = gate["scorecard"]
    print("gate_status:", gate["gate_status"])
    print("coverage_status:", audit["coverage_status"], audit["totals"])
    print("quality:", quality["overall_quality_status"], quality["customer_readiness"],
          "scores:", quality["scores"])
    print("scorecard:", scorecard["overall_score"], scorecard["status"], scorecard["scores"])
    print("blocking:")
    for b in quality["blocking_issues"]:
        print("  -", b["code"], "|", b["detail"][:160])
    print("critical_omissions:")
    for o in audit["critical_omissions"]:
        print("  -", o["fact_id"], "|", o["document_fact"][:140], "| pages", o["evidence_pages"])
    print("important_warnings:", len(audit["important_warnings"]))
    for o in audit["important_warnings"][:15]:
        print("  -", o["fact_id"], "|", o["document_fact"][:140])
    print("warnings:", len(quality["warnings"]))
    for w in quality["warnings"][:15]:
        print("  -", w["code"], "|", w["detail"][:140])


if __name__ == "__main__":
    raise SystemExit(main())
