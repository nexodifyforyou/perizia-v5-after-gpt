from __future__ import annotations

import json
from pathlib import Path

from correctness_v2 import (
    contract, customer_report, doc_signals, fact_lineage, lot_fact_projection,
    lot_packets, lots, validator,
)

ROOT = Path(__file__).parent / "fixtures"


def load_fixture():
    case = json.loads((ROOT / "beta_multilot_case_sanitized.json").read_text(encoding="utf-8"))
    pages = json.loads((ROOT / "beta_multilot_case_cached_pages_sanitized.json").read_text(encoding="utf-8"))["pages"]
    return case, pages


def prepare():
    case, pages = load_fixture()
    report = lots.build_lot_report(case["case_worksheet"], pages)
    segmentation = lot_packets.segment_pages(pages, report["lot_ids"])
    segmentation["full_document_pages"] = pages
    segmentation["shared_summary_projection"] = lot_packets.project_shared_summary_rows(pages, segmentation)
    ledger = fact_lineage.build_case_fact_ledger(case["case_worksheet"], segmentation, report)
    return case, pages, report, segmentation, ledger


def build_lot(lot_id="1"):
    case, pages, report, segmentation, ledger = prepare()
    selected = lot_packets.select_lot_pages(pages, segmentation, lot_id)
    reconciled, projection = lot_fact_projection.project_and_reconcile(
        case_ledger=ledger,
        lot_worksheet=case["lot_worksheets"][str(lot_id)],
        lot_id=str(lot_id),
        segmentation=segmentation,
        all_lot_ids=report["lot_ids"],
    )
    numbers = {page["page_number"] for page in selected} | set(projection["verification_pages_added"])
    verification = [page for page in pages if page["page_number"] in numbers]
    reconciled = contract.complete_valuation_terminals(reconciled, verification)
    reconciled, gate = validator.apply_compliance_evidence_gate(reconciled, verification)
    validator_report = validator.validate_worksheet(reconciled, verification)
    sub_report = lots.build_lot_report(reconciled, selected)
    lot_money = lot_packets.build_lot_money(case["case_worksheet"], segmentation, pages)["by_lot"].get(str(lot_id), {})
    rows = lot_packets.contract_rows_from_lot_money(lot_money)
    report_contract = contract.build_contract(
        worksheet=reconciled,
        validator_report=validator_report,
        analysis_id="fixture_beta_multilot",
        job_id=f"fixture_job_lot_{lot_id}",
        source_pdf_quality_status="OK",
        lot_report=sub_report,
        shared_summary_rows=rows,
        surface_cadastral=doc_signals.extract_surface_cadastral(verification),
    )
    customer_pages = lot_fact_projection.customer_safe_projection_pages(
        pages, selected, projection, str(lot_id)
    )
    rendered = customer_report.render_success_report(report_contract, customer_pages)
    return {
        "case": case, "pages": pages, "report": report, "segmentation": segmentation,
        "ledger": ledger, "selected": selected, "verification": verification, "customer_pages": customer_pages,
        "worksheet": reconciled, "projection": projection, "compliance_gate": gate,
        "validator": validator_report, "contract": report_contract, "customer_report": rendered,
    }
