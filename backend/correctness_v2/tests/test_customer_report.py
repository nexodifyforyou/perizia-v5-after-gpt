"""Step 3B tests: deterministic customer report renderer.

All fixtures are generic/synthetic (Tribunale di Esempio) — nothing here encodes a
real perizia. Covers the required behaviors:
  1. the renderer never invents missing fields
  2. sale value visible / no fake prezzo base / regularization visible /
     no duplicated money rows / no invented grave risk
  3. multi-lot no-selection -> LOT_SELECTION_REQUIRED selection report (no blend)
  4. selected lot -> only that lot's data
  5. single-lot multi-bene -> beni rendered separately in ONE report
  6. procedure-cancelled formalities stay separate from buyer-side costs
  7. uncertain money stays visible as "Importi da verificare"
  8. failure/manual-review statuses render a safe report with zero fake certainty
"""

import json

from correctness_v2 import analyst, artifacts, contract as contract_mod
from correctness_v2 import customer_report, orchestrator, validator
from correctness_v2.schemas import JobStatus

from .sample_perizia import (
    GENERIC_PERIZIA_PAGES,
    MULTI_LOT_PAGES,
    fake_caller_raising,
    fake_caller_returning,
    fake_sequence_caller,
    make_multibene_single_lot_worksheet,
    make_multilot_worksheet,
    make_worksheet,
    single_lot_worksheet_on_page,
)

VALIDATED_REPORT = {"validation_status": "VALIDATED", "checks": {}, "warnings": []}

EXPECTED_TOP_KEYS = {
    "schema_version",
    "analysis_id",
    "job_id",
    "report_status",
    "title",
    "subtitle",
    "case_identity",
    "lot_structure",
    "executive_summary",
    "key_facts",
    "risk_sections",
    "money_sections",
    "beni_sections",
    "buyer_checklist",
    "manual_review_flags",
    "evidence_index",
    "disclaimer",
}

MONEY_SECTION_KEYS = {
    "valuation_chain",
    "auction_terms",
    "buyer_side_costs",
    "procedure_cancelled_formalities",
    "uncertain_money",
}


def _loader(pages):
    def _inner(analysis_id):
        return pages

    return _inner


def _contract(raw, validator_report=None, pages=GENERIC_PERIZIA_PAGES, lot_report=None):
    ws = analyst.normalize_worksheet(raw)
    report = validator_report or validator.validate_worksheet(ws, pages)
    return contract_mod.build_contract(
        worksheet=ws,
        validator_report=report,
        analysis_id="a1",
        job_id="j1",
        source_pdf_quality_status="PDF_QUALITY_OK",
        lot_report=lot_report,
    )


def _all_money_rows(report):
    rows = []
    for key in MONEY_SECTION_KEYS:
        rows.extend(report["money_sections"][key])
    return rows


def _contract_amounts(con):
    amounts = set()
    for section in (
        "valuation_chain",
        "auction_terms",
        "buyer_side_costs",
        "procedure_cancelled_formalities",
        "uncertain_money",
    ):
        for row in con.get(section) or []:
            if row.get("amount") is not None:
                amounts.add(round(float(row["amount"]), 2))
    return amounts


def _read_customer_report(job_id):
    return artifacts.read_json(job_id, artifacts.CUSTOMER_REPORT_FILE)


# 1) The renderer never invents missing fields ------------------------------
def test_renderer_does_not_invent_missing_fields():
    raw = make_worksheet()
    raw["case_identity"]["address"] = None
    raw["case_identity"]["property_type"] = None
    raw["money"]["auction_terms"] = {}
    raw["money"]["uncertain_money"] = []
    con = _contract(raw, validator_report=VALIDATED_REPORT)
    report = customer_report.render_success_report(con)

    assert set(EXPECTED_TOP_KEYS) <= set(report.keys())
    assert set(report["money_sections"].keys()) == MONEY_SECTION_KEYS
    # Missing identity fields are absent, not fabricated.
    assert "address" not in report["case_identity"]
    assert "property_type" not in report["case_identity"]
    assert report["title"] == "Report di analisi della perizia"
    # No auction terms in the contract -> none in the report (rule 7).
    assert report["money_sections"]["auction_terms"] == []
    dumped = json.dumps(report, ensure_ascii=False).lower()
    assert "prezzo base" not in dumped
    # Every rendered amount exists in the contract's own money sections.
    allowed = _contract_amounts(con)
    for row in _all_money_rows(report):
        assert round(float(row["amount"]), 2) in allowed, row
    # Every key fact is a contract executive-summary fact.
    contract_labels = {f["label"] for f in con["executive_summary_facts"]}
    assert {f["label"] for f in report["key_facts"]} <= contract_labels


# 2) Sale value visible, no fake prezzo base, no dup rows, no invented grave --
def test_sale_value_visible_no_fake_prezzo_base_no_duplicates():
    raw = make_worksheet()
    # A base candidate equal to the sale value but WITHOUT explicit text support:
    # it must never be relabeled "prezzo base" (it is already visible as sale value).
    raw["money"]["auction_terms"] = {"prezzo_base_asta": 94700.0, "evidence_pages": [2]}
    con = _contract(raw, validator_report=VALIDATED_REPORT)  # no money_signals
    report = customer_report.render_success_report(con)

    chain_labels = [r["label"] for r in report["money_sections"]["valuation_chain"]]
    assert "Valore di vendita giudiziaria" in chain_labels  # rule 6
    assert "Costi di regolarizzazione" in " / ".join(
        str(r["label"]) for r in _all_money_rows(report)
    )
    dumped = json.dumps(report, ensure_ascii=False).lower()
    assert "prezzo base" not in dumped  # rule 7: no explicit support -> no base row

    # No duplicated money rows anywhere (rule 4).
    keys = [
        (str(r["label"]).lower(), round(float(r["amount"]), 2))
        for r in _all_money_rows(report)
    ]
    assert len(keys) == len(set(keys)), keys

    # No invented grave risk: the contract has no grave card, so no "criticita".
    assert all(
        s["section_id"] != "criticita" or not s["items"] for s in report["risk_sections"]
    )
    grave_items = [
        i for s in report["risk_sections"] for i in s["items"] if i.get("severity") == "grave"
    ]
    assert grave_items == []


def test_explicit_prezzo_base_is_rendered_as_auction_term():
    raw = make_worksheet()
    con = _contract(
        raw,
        validator_report={
            "validation_status": "VALIDATED",
            "checks": {"money_signals": {"base_price_explicit_text": True}},
            "warnings": [],
        },
    )
    report = customer_report.render_success_report(con)
    labels = [r["label"] for r in report["money_sections"]["auction_terms"]]
    assert "Prezzo base d'asta" in labels
    row = next(
        r
        for r in report["money_sections"]["auction_terms"]
        if r["label"] == "Prezzo base d'asta"
    )
    assert row["amount"] == 75000.0
    assert row["amount_display"] == "€ 75.000,00"


# 3) Multi-lot no-selection -> selection report, never a blended report ------
def test_multi_lot_no_selection_renders_selection_report(artifacts_root):
    caller = fake_caller_returning(make_multilot_worksheet())
    status = orchestrator.start_job(
        "an_sel_report", _loader(MULTI_LOT_PAGES), is_admin=True, openai_caller=caller
    )
    assert status["status"] == JobStatus.LOT_SELECTION_REQUIRED

    report = _read_customer_report(status["job_id"])
    assert report is not None
    assert report["report_status"] == "LOT_SELECTION_REQUIRED"
    assert report["lot_structure"]["multi_lot"] is True
    assert report["lot_structure"]["lot_ids"] == ["1", "2"]
    # A selector, not a fake blended report: no facts, risks or blended money.
    assert report["key_facts"] == []
    assert report["risk_sections"] == []
    assert all(rows == [] for rows in report["money_sections"].values())
    # Lot list with per-lot data + both actions for the frontend.
    lots = report["lot_selection"]["lots"]
    assert [L["lot_id"] for L in lots] == ["1", "2"]
    actions = {a["action"] for a in report["lot_selection"]["available_actions"]}
    assert {"analyze_selected_lot", "analyze_all"} <= actions
    # Evidence references point back to per-lot pages.
    assert report["evidence_index"], report["evidence_index"]


# 4) Selected lot -> only that lot's data ------------------------------------
def test_selected_lot_report_contains_only_selected_lot(artifacts_root):
    caller = fake_sequence_caller(
        [make_multilot_worksheet(), single_lot_worksheet_on_page(2, "1")]
    )
    status = orchestrator.start_job(
        "an_sel_lot_report",
        _loader(MULTI_LOT_PAGES),
        is_admin=True,
        openai_caller=caller,
        selected_lot_id="1",
    )
    assert status["status"] == JobStatus.REPORT_READY, status

    report = _read_customer_report(status["job_id"])
    assert report["report_status"] == "REPORT_READY"
    assert report["lot_structure"]["selected_lot"] == "1"
    assert report["case_identity"]["address"] == "Via del Lotto 1"
    dumped = json.dumps(report, ensure_ascii=False)
    assert "Via del Lotto 2" not in dumped  # no cross-lot contamination


# 5) Single-lot multi-bene -> beni rendered separately in ONE report ---------
def test_multi_bene_single_lot_renders_beni_sections(artifacts_root):
    caller = fake_caller_returning(make_multibene_single_lot_worksheet())
    status = orchestrator.start_job(
        "an_multibene_report",
        _loader(GENERIC_PERIZIA_PAGES),
        is_admin=True,
        openai_caller=caller,
    )
    assert status["status"] == JobStatus.REPORT_READY, status

    report = _read_customer_report(status["job_id"])
    assert report["report_status"] == "REPORT_READY"
    assert report["lot_structure"]["multi_lot"] is False
    assert report["lot_structure"]["multi_bene"] is True
    beni = report["beni_sections"]
    assert [b["bene_id"] for b in beni] == report["lot_structure"]["bene_ids"]
    assert len(beni) >= 2
    # Each bene's explicitly tagged risk landed in ITS section.
    bene1 = next(b for b in beni if b["bene_id"] == "1")
    bene2 = next(b for b in beni if b["bene_id"] == "2")
    assert any("Bene 1" in str(r.get("area")) for r in bene1["risks"])
    assert any("Bene 2" in str(r.get("area")) for r in bene2["risks"])
    assert not any("Bene 2" in str(r.get("area")) for r in bene1["risks"])


# 6) Procedure-cancelled formalities stay separate ----------------------------
def test_procedure_cancelled_formalities_stay_separate():
    con = _contract(make_worksheet(), validator_report=VALIDATED_REPORT)
    report = customer_report.render_success_report(con)

    cancelled = report["money_sections"]["procedure_cancelled_formalities"]
    assert cancelled, "procedure-cancelled section must stay visible"
    cancelled_labels = {str(r["label"]).lower() for r in cancelled}
    buyer_labels = {
        str(r["label"]).lower() for r in report["money_sections"]["buyer_side_costs"]
    }
    assert cancelled_labels.isdisjoint(buyer_labels)  # rule 8
    # And no buyer action is fabricated from a procedure-cancelled formality.
    for item in report["buyer_checklist"]:
        assert "cancellazione ipoteca" not in str(item.get("detail", "")).lower()


# 7) Uncertain money stays visible as "Importi da verificare" -----------------
def test_uncertain_money_stays_visible_as_da_verificare():
    raw = make_worksheet()
    raw["money"]["uncertain_money"] = [
        {
            "label": "Importo citato senza contesto",
            "amount": 12345.0,
            "reason": "Ruolo non chiaro dal testo.",
            "evidence_pages": [2],
        }
    ]
    con = _contract(raw, validator_report=VALIDATED_REPORT)
    report = customer_report.render_success_report(con)

    uncertain = report["money_sections"]["uncertain_money"]
    assert any(r["amount"] == 12345.0 for r in uncertain)
    for row in uncertain:
        assert row["status"] == "da_verificare"
        assert row["status_label"] == "Importo da verificare"
    # Never shown as a confirmed value/cost in the other sections.
    for key in MONEY_SECTION_KEYS - {"uncertain_money"}:
        assert all(r["amount"] != 12345.0 for r in report["money_sections"][key])
    # Flagged for manual review too.
    assert any(f["kind"] == "uncertain_money" for f in report["manual_review_flags"])


# 13) Uncertain compliance renders as uncertainty, never as conforming -------
def test_uncertain_compliance_renders_as_da_verificare():
    raw = make_worksheet()
    raw["technical_compliance"].append(
        {
            "area": "agibilità",
            "classification": "uncertain",
            "blocks_saleability": False,
            "cost": None,
            "timing": None,
            "notes": "Documentazione non reperita.",
            "evidence_pages": [2],
        }
    )
    con = _contract(raw, validator_report=VALIDATED_REPORT)
    report = customer_report.render_success_report(con)

    verify_section = next(
        s for s in report["risk_sections"] if s["section_id"] == "da_verificare"
    )
    item = next(i for i in verify_section["items"] if i["area"] == "agibilità")
    assert item["status_label"] == "Da verificare"
    dumped = json.dumps(report, ensure_ascii=False).lower()
    assert "agibilità: conforme" not in dumped
    assert any(f["kind"] == "compliance_uncertain" for f in report["manual_review_flags"])


# 8) Failure / manual-review statuses render a SAFE report --------------------
def test_validation_failure_renders_safe_report(artifacts_root):
    raw = make_worksheet()
    raw["money"]["evidence_pages"] = [99]  # out of range -> validator rejects
    caller = fake_caller_returning(raw)
    status = orchestrator.start_job(
        "an_valfail_report",
        _loader(GENERIC_PERIZIA_PAGES),
        is_admin=True,
        openai_caller=caller,
    )
    assert status["status"] == JobStatus.CONTRACT_VALIDATION_FAILED
    assert status["customer_report_generated"] is False  # fail-closed status intact

    report = _read_customer_report(status["job_id"])
    assert report is not None
    assert report["report_status"] == "CONTRACT_VALIDATION_FAILED"
    # Zero fake certainty: no facts, no money, no risks.
    assert report["key_facts"] == []
    assert all(rows == [] for rows in report["money_sections"].values())
    assert report["risk_sections"] == []
    assert report["manual_review_flags"], report
    assert report["disclaimer"]


def test_analyst_failure_renders_safe_report(artifacts_root):
    caller = fake_caller_raising("OPENAI_CALL_FAILED")
    status = orchestrator.start_job(
        "an_failed_report",
        _loader(GENERIC_PERIZIA_PAGES),
        is_admin=True,
        openai_caller=caller,
    )
    assert status["status"] == JobStatus.FAILED_ANALYSIS
    assert status["customer_report_generated"] is False

    report = _read_customer_report(status["job_id"])
    assert report is not None
    assert report["report_status"] == "NEEDS_MANUAL_REVIEW"
    assert report["job_status"] == JobStatus.FAILED_ANALYSIS
    assert report["key_facts"] == []
    assert all(rows == [] for rows in report["money_sections"].values())
    assert any(f["kind"] == "status" for f in report["manual_review_flags"])


# 9) Buyer checklist has no zero-value fake actions ---------------------------
def test_buyer_checklist_has_no_zero_value_actions():
    raw = make_worksheet()
    raw["money"]["buyer_side_costs"] = [
        {"label": "Spese a carico acquirente", "amount": 0, "evidence_pages": [2]},
        {"label": "Compenso custode", "amount": 1500.0, "evidence_pages": [2]},
    ]
    con = _contract(raw, validator_report=VALIDATED_REPORT)
    report = customer_report.render_success_report(con)

    details = [str(i.get("detail", "")) for i in report["buyer_checklist"]]
    assert not any(d.endswith(": 0") or d.endswith(": 0.0") for d in details)
    assert any("Compenso custode" in d for d in details)


# analyze_all: one customer report per lot, never blended ---------------------
def test_analyze_all_renders_one_customer_report_per_lot(artifacts_root):
    caller = fake_sequence_caller(
        [
            make_multilot_worksheet(),
            single_lot_worksheet_on_page(2, "1"),
            single_lot_worksheet_on_page(4, "2"),
        ]
    )
    status = orchestrator.start_job(
        "an_all_report",
        _loader(MULTI_LOT_PAGES),
        is_admin=True,
        openai_caller=caller,
        analyze_all=True,
    )
    assert status["status"] == JobStatus.REPORT_READY, status
    job_dir = artifacts.job_dir(status["job_id"])
    for lot_id, address in (("1", "Via del Lotto 1"), ("2", "Via del Lotto 2")):
        path = job_dir / "lots" / lot_id / artifacts.CUSTOMER_REPORT_FILE
        assert path.exists(), path
        report = json.loads(path.read_text())
        assert report["report_status"] == "REPORT_READY"
        assert report["case_identity"]["address"] == address
