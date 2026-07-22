import json

from correctness_v2 import lot_fact_projection, validator

from .beta_fixture import build_lot, prepare


def test_matrix_03_strong_lot_finding_wins_and_widened_pages_validate():
    built = build_lot("3")
    issue = next(x for x in built["worksheet"]["technical_compliance"] if "Regolarità" in x["area"])
    assert issue["classification"] == "regularizable"
    assert built["compliance_gate"]["downgrade_count"] == 0
    assert built["validator"]["validation_status"] == validator.STATUS_VALIDATED


def test_matrix_05_case_fact_fills_missing_lot_field():
    built = build_lot("1")
    assert built["worksheet"]["legal_formalities"]
    assert all(item["projected"] is True for item in built["worksheet"]["legal_formalities"])


def test_matrix_06_exclusive_lot_evidence_overrides_case_inference():
    case, _, report, segmentation, ledger = prepare()
    lot = case["lot_worksheets"]["3"]
    reconciled, projection = lot_fact_projection.project_and_reconcile(case_ledger=ledger, lot_worksheet=lot, lot_id="3", segmentation=segmentation, all_lot_ids=report["lot_ids"])
    retained = next(x for x in reconciled["technical_compliance"] if "Regolarità" in x["area"])
    assert retained["classification"] == "regularizable"
    assert retained["evidence_pages"] == [8, 9]
    assert any(x["reason_code"] in {lot_fact_projection.DUPLICATE_EQUIVALENT, lot_fact_projection.INVALIDATED_BY_STRONGER_SOURCE} for x in projection["dropped_facts"])


def test_matrix_07_material_conflict_is_never_silent():
    case, _, report, segmentation, ledger = prepare()
    lot = case["lot_worksheets"]["3"]
    lot["technical_compliance"][0]["classification"] = "not_regularizable"
    reconciled, projection = lot_fact_projection.project_and_reconcile(case_ledger=ledger, lot_worksheet=lot, lot_id="3", segmentation=segmentation, all_lot_ids=report["lot_ids"])
    assert reconciled["technical_compliance"][0]["classification"] == "not_regularizable"
    assert projection["conflicts"] and projection["conflicts"][0]["reason"] == lot_fact_projection.CONFLICT_REQUIRES_REVIEW


def test_matrix_09_occupancy_omission_never_becomes_libero():
    built = build_lot("2")
    assert "libero" not in built["worksheet"]["occupancy"]["status"].lower()
    assert "termine" in built["worksheet"]["occupancy"]["title_info"].lower()


def test_matrix_10_declared_compliance_not_downgraded_to_da_verificare():
    built = build_lot("1")
    assert {x["classification"] for x in built["worksheet"]["technical_compliance"]} == {"conforming"}
    assert all("verificare" not in x["status_label"].lower() for x in built["customer_report"]["compliance_section"])


def test_matrix_13_formalities_are_scoped_and_delotified():
    lot1, lot3 = build_lot("1"), build_lot("3")
    assert {x["type"] for x in lot1["worksheet"]["legal_formalities"]} == {"ipoteca", "pignoramento"}
    assert lot3["worksheet"]["legal_formalities"] == []
    assert all(len(__import__("correctness_v2.lots", fromlist=["lot_ids_in_text"]).lot_ids_in_text(x["description"])) <= 1 for x in lot1["worksheet"]["legal_formalities"])


def test_matrix_14_every_projected_fact_is_evidence_traceable():
    built = build_lot("1")
    assert built["projection"]["verification_pages_added"]
    assert all(row.get("evidence_pages") for row in built["worksheet"]["technical_compliance"] + built["worksheet"]["legal_formalities"])
    indexed = set(built["contract"]["evidence_index"])
    assert {"2", "3"} <= indexed


def test_matrix_15_dropped_fact_reason_codes_are_exhaustive():
    report = {"dropped_facts": []}
    for index, reason in enumerate(sorted(lot_fact_projection.REASON_CODES)):
        lot_fact_projection._drop(report, {"fact_id": f"fixture:{index}"}, reason, "forced invariant branch")
    assert {row["reason_code"] for row in report["dropped_facts"]} == lot_fact_projection.REASON_CODES


def test_customer_excerpt_expands_range_for_intermediate_lot_and_keeps_continuation():
    full_pages = [{
        "page_number": 7,
        "text": (
            "Lotti 1-4: dichiarazione comune applicabile.\n"
            "Dettaglio di continuazione privo di etichetta.\n"
            "Lotto 3: contenuto esclusivo del terzo lotto."
        ),
    }]
    pages = lot_fact_projection.customer_safe_projection_pages(
        full_pages, [], {"verification_pages_added": [7]}, "2"
    )
    assert len(pages) == 1
    assert "dichiarazione comune" in pages[0]["text"]
    assert "Dettaglio di continuazione" in pages[0]["text"]
    assert "contenuto esclusivo" not in pages[0]["text"]


def test_customer_excerpt_does_not_cross_sibling_lot_paragraph_boundary():
    full_pages = [{
        "page_number": 8,
        "text": (
            "Lotto 2: descrizione del box auto.\n"
            "Il box e di modeste dimensioni con accesso carrabile diretto.\n"
            "Lotto 1: descrizione appartamento.\n"
            "L appartamento presenta finiture di pregio."
        ),
    }]
    pages = lot_fact_projection.customer_safe_projection_pages(
        full_pages, [], {"verification_pages_added": [8]}, "1"
    )

    assert "descrizione appartamento" in pages[0]["text"]
    assert "finiture di pregio" in pages[0]["text"]
    assert "modeste dimensioni" not in pages[0]["text"]
    assert "descrizione del box" not in pages[0]["text"]


def test_customer_excerpt_keeps_legitimate_same_lot_continuation():
    full_pages = [{
        "page_number": 9,
        "text": "Lotto 1: descrizione appartamento.\nContinuazione specifica del medesimo appartamento.",
    }]
    pages = lot_fact_projection.customer_safe_projection_pages(
        full_pages, [], {"verification_pages_added": [9]}, "1"
    )
    assert "Continuazione specifica" in pages[0]["text"]


def test_non_projected_claim_on_added_verification_page_still_runs_full_gate():
    built = build_lot("1")
    worksheet = json.loads(json.dumps(built["worksheet"]))
    worksheet["technical_compliance"].append({
        "area": "Impianto elettrico",
        "classification": "conforming",
        "blocks_saleability": False,
        "cost": None,
        "timing": None,
        "notes": "Affermazione non prodotta dalla proiezione",
        "evidence_pages": [3],
    })
    assert 3 in built["projection"]["verification_pages_added"]
    gated, gate = validator.apply_compliance_evidence_gate(worksheet, built["verification"])
    injected = gated["technical_compliance"][-1]

    assert injected.get("projected") is not True
    assert injected["classification"] == "uncertain"
    assert any(
        row["path"] == f"technical_compliance[{len(gated['technical_compliance']) - 1}]"
        for row in gate["downgrades"]
    )
    assert any(item.get("projected") is True and item["classification"] == "conforming" for item in gated["technical_compliance"][:-1])
