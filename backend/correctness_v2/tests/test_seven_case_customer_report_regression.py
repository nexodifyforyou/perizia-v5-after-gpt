"""Permanent aggregate regression for the seven customer-report golden cases.

This module is the named, deterministic entry point for the customer-report
corpus.  Every case is synthetic/sanitized (Mantova uses the checked-in
sanitized old-report fixture); the runner never opens a production artifact,
database, network connection, or model client.

Run from the repository root with::

    backend/.venv/bin/pytest -vv \
      backend/correctness_v2/tests/test_seven_case_customer_report_regression.py
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from correctness_v2 import customer_view, decision_model, lot_packets


MANTOVA_FIXTURE = (
    Path(__file__).parent / "fixtures" / "mantova_customer_report_sanitized.json"
)
MANTOVA_CACHED_PAGES_FIXTURE = (
    Path(__file__).parent / "fixtures" / "mantova_cached_pages_sanitized.json"
)


def _empty_money():
    return {
        "valuation_chain": [],
        "auction_terms": [],
        "buyer_side_costs": [],
        "procedure_cancelled_formalities": [],
        "market_comparatives": [],
        "context_values": [],
        "uncertain_money": [],
    }


def _report(case_name: str, **overrides):
    report = {
        "schema_version": "cv2.customer_report.v1",
        "analysis_id": f"fixture_{case_name.lower()}",
        "job_id": f"fixture_job_{case_name.lower()}",
        "report_status": "REPORT_READY",
        "case_identity": {
            "tribunale": "Tribunale fixture",
            "lotto": "Lotto Unico",
            "address": "Indirizzo omesso",
            "property_type": "Immobile",
            "evidence_pages": [1],
        },
        "lot_structure": {"selected_lot": "unico", "lot_count": 1, "bene_count": 1},
        "beni_sections": [
            {
                "bene_id": "1",
                "title": "Bene 1",
                "property_type": "Immobile",
                "evidence_pages": [1],
            }
        ],
        "occupancy_section": {},
        "compliance_section": [],
        "formalities_section": [],
        "buyer_checklist": [],
        "risk_sections": [],
        "customer_evidence_index": [],
        "money_sections": _empty_money(),
    }
    report.update(overrides)
    return report


def _model(report):
    return customer_view.sanitize_customer_report(
        report, {"safe_to_show_customer": True}
    )["decision_model"]


def test_01_torino_valuation_chain_and_buyer_economics():
    """Torino: both arithmetic steps survive and no auction base is invented."""
    money = _empty_money()
    money["valuation_chain"] = [
        {"label": "Valore di mercato", "amount": 43654.20, "kind": "value", "evidence_pages": [18]},
        {"label": "Spese di regolarizzazione", "amount": 5250.00, "kind": "deduction", "evidence_pages": [18]},
        {"label": "Valore nello stato di fatto", "amount": 38404.20, "kind": "value", "evidence_pages": [18]},
        {"label": "Spese di cancellazione formalità", "amount": 294.00, "kind": "deduction", "evidence_pages": [18]},
        {"label": "Valore di vendita giudiziaria", "amount": 38110.20, "kind": "value", "evidence_pages": [18]},
    ]
    money["buyer_side_costs"] = [
        {
            "label": "Spese di cancellazione formalità",
            "amount": 294.00,
            "included_in_valuation": True,
            "evidence_pages": [18],
        }
    ]
    report = _report(
        "Torino",
        money_sections=money,
        formalities_section=[
            {
                "type": "ipoteca",
                "type_label": "Ipoteca",
                "description": "Da cancellare con il decreto di trasferimento",
                "cancelled_by_procedure": True,
                "buyer_burden": False,
                "amount": 150000,
                "evidence_pages": [19],
            }
        ],
    )
    model = _model(report)
    numeri = model["sections"]["numeri"]
    chain = numeri["catena"]

    assert [row["amount"] for row in chain] == [43654.20, 5250.00, 38404.20, 294.00, 38110.20]
    assert chain[0]["amount"] - chain[1]["amount"] == pytest.approx(chain[2]["amount"])
    assert chain[2]["amount"] - chain[3]["amount"] == pytest.approx(chain[4]["amount"])
    assert "auction" not in numeri
    assert numeri["costi_potenziali"] == [
        {
            "label": "Spese di cancellazione formalità",
            "amount_display": "€ 294,00",
            "included_in_valuation": True,
            "nota": "Già considerato nel valore finale: non sommare nuovamente.",
        }
    ]
    formalita = model["sections"]["formalita"]
    assert "costi_cancellazione" not in formalita
    assert formalita["cancellate"][0]["cancellation_state"] == "to_be_cancelled"
    assert "non è un debito da sommare" in formalita["cancellate"][0]["note"]


def test_02_pistoia_selected_lot_isolation():
    """Pistoia: selecting lot 3 cannot import pages or money from lots 1/2."""
    pages = [
        {"page_number": 1, "text": "Premessa comune senza importi."},
        {"page_number": 2, "text": "Lotto 1 - bene uno. Prezzo base € 111.000,00."},
        {"page_number": 3, "text": "Dettaglio esclusivo lotto uno € 11.100,00."},
        {"page_number": 4, "text": "Lotto 2 - bene due. Prezzo base € 222.000,00."},
        {"page_number": 5, "text": "Dettaglio esclusivo lotto due € 22.200,00."},
        {"page_number": 6, "text": "Lotto 3 - bene tre. Prezzo base € 333.000,00."},
        {"page_number": 7, "text": "Dettaglio esclusivo lotto tre € 33.300,00."},
    ]
    segmentation = lot_packets.segment_pages(pages, ["1", "2", "3"])
    context = lot_packets.build_selected_lot_context(pages, segmentation, "3")
    selected = lot_packets.select_lot_pages(pages, segmentation, "3")
    selected_text = " ".join(page["text"] for page in selected)

    assert context["selected_lot_id"] == "3"
    assert context["analysis_pages"] == [1, 6, 7]
    assert [page["page_number"] for page in selected] == [1, 6, 7]
    assert "333.000,00" in selected_text and "33.300,00" in selected_text
    assert "111.000,00" not in selected_text and "222.000,00" not in selected_text


def test_03_1859886_c_preserves_exactly_four_beni():
    """1859886_C: a single lot containing four Beni remains four Beni."""
    beni = [
        {
            "bene_id": str(index),
            "title": f"Bene {index}",
            "property_type": kind,
            "evidence_pages": [index],
        }
        for index, kind in enumerate(
            ("Abitazione", "Autorimessa", "Deposito", "Terreno"), start=1
        )
    ]
    report = _report(
        "1859886_C",
        lot_structure={"selected_lot": "unico", "lot_count": 1, "bene_count": 4},
        beni_sections=beni,
    )
    customer = customer_view.sanitize_customer_report(
        report, {"safe_to_show_customer": True}
    )

    assert customer["lot_structure"]["lot_count"] == 1
    assert customer["lot_structure"]["bene_count"] == 4
    assert len(customer["beni_sections"]) == 4
    assert len(customer["decision_model"]["sections"]["acquisto"]["beni"]) == 4


@pytest.mark.parametrize(
    ("case_name", "status"),
    [
        pytest.param("Orecchiazzi", "CONTRACT_VALIDATION_FAILED", id="orecchiazzi"),
        pytest.param("Cairate", "CONTRACT_VALIDATION_FAILED", id="cairate"),
    ],
)
def test_04_05_fail_closed_cases_remain_unavailable(case_name, status):
    """Orecchiazzi/Cairate: validator failures never become customer reports."""
    report = _report(case_name, report_status=status)
    job = {"status": status, "safe_to_show_customer": False}
    model = decision_model.build_decision_model(report, [])

    assert customer_view.is_customer_safe(report, job) is False
    assert model["report_status"] == status
    assert model["esito"]["level"] == "rosso"
    assert model["esito"]["headline"] == "Verifica tecnica richiesta"
    assert model["findings"] == []
    assert list(model["sections"]) == ["stato_verifiche"]
    assert model["readiness"]["state"] == "TECHNICAL_REVIEW_REQUIRED"


def test_06_codogno_six_real_lot_alternatives_without_lotto_00():
    """Codogno: six selector alternatives survive; footer Lotto 00 never does."""
    pages = [{"page_number": 1, "text": "Premessa comune."}]
    for lot_id in range(1, 7):
        pages.extend(
            [
                {
                    "page_number": lot_id * 2,
                    "text": f"Lotto {lot_id} - descrizione del bene.",
                },
                {
                    "page_number": lot_id * 2 + 1,
                    "text": (
                        f"Valore nello stato di fatto lotto {lot_id}: "
                        f"€ {lot_id}00.000,00. Relazione Lotto 00."
                    ),
                },
            ]
        )
    pages.append(
        {
            "page_number": 20,
            "text": "\n".join(
                f"Lotto {lot_id} - prezzo base alternativo: € {lot_id}10.000,00"
                for lot_id in range(1, 7)
            ),
        }
    )
    segmentation = lot_packets.segment_pages(pages, [str(i) for i in range(1, 7)])
    projected = lot_packets.project_shared_summary_rows(pages, segmentation)["projected"]
    alternatives = [
        {
            "lot_id": str(lot_id),
            "label": f"Lotto {lot_id}",
            "address": f"Indirizzo lotto {lot_id}",
            "money_summary": projected[str(lot_id)],
            "evidence_pages": segmentation["lot_pages"][str(lot_id)],
        }
        for lot_id in range(1, 7)
    ]
    report = _report(
        "Codogno",
        report_status="LOT_SELECTION_REQUIRED",
        lot_structure={"lot_count": 6, "bene_count": 6},
        lot_selection={"message": "Seleziona il lotto", "lots": alternatives},
    )
    customer = customer_view.sanitize_customer_report(
        report, {"safe_to_show_customer": True}
    )
    selector_lots = customer["lot_selection"]["lots"]

    assert segmentation["lot_ids"] == ["1", "2", "3", "4", "5", "6"]
    assert set(segmentation["lot_pages"]) == {"1", "2", "3", "4", "5", "6"}
    assert "00" not in segmentation["lot_ids"] and "0" not in segmentation["lot_ids"]
    assert [lot["lot_id"] for lot in selector_lots] == ["1", "2", "3", "4", "5", "6"]
    assert all(len(lot["money_summary"]) == 1 for lot in selector_lots)
    assert "Lotto 00" not in json.dumps(customer["lot_selection"], ensure_ascii=False)


def test_07_mantova_grounding_priority_and_reconciliation_contract():
    """Mantova: full seventh-case customer-report contract from sanitized facts."""
    report = json.loads(MANTOVA_FIXTURE.read_text(encoding="utf-8"))
    cached_pages = json.loads(
        MANTOVA_CACHED_PAGES_FIXTURE.read_text(encoding="utf-8")
    )
    customer = customer_view.sanitize_customer_report(
        report, {"safe_to_show_customer": True}, cached_pages=cached_pages
    )
    model = customer["decision_model"]
    sections = model["sections"]
    numeri = sections["numeri"]

    # One Lotto Unico, three distinct Beni and their exact grounded values.
    assert report["lot_structure"] == {
        "lot_count": 1,
        "selected_lot": "unico",
        "bene_count": 3,
    }
    assert len(sections["acquisto"]["beni"]) == 3
    composition = numeri["composizione_valore"]
    assert [item["amount"] for item in composition["items"]] == [92700, 26100, 6480]
    assert composition["total"] == 125280
    for expected_bene, expected_text, item in zip(
        ("Bene N° 1", "Bene N° 2", "Bene N° 3"),
        ("92.700,00", "26.100,00", "6.480,00"),
        composition["items"],
    ):
        excerpt = item["evidence"]["excerpt"]
        assert expected_bene in excerpt and expected_text in excerpt
    assert "26.100,00" not in composition["items"][2]["evidence"]["excerpt"]

    # Declared final/base stay €80,000; the unstated minimum bid stays absent.
    assert numeri["catena"][0]["label"] == "Valore di stima prima dei deprezzamenti"
    assert numeri["catena"][-1]["label"] == "Valore finale dichiarato"
    assert numeri["catena"][-1]["amount"] == 80000
    assert numeri["auction"]["amount_display"] == "€ 80.000,00"
    assert "offerta minima" not in json.dumps(model, ensure_ascii=False).lower()

    reconciliation = numeri["riconciliazione"]
    assert reconciliation["component_sum"] == 125280
    assert reconciliation["percentage_sum"] == 35
    assert reconciliation["expected_final"] == 81432
    assert reconciliation["declared_final"] == 80000
    assert reconciliation["difference"] == 1432
    assert numeri["da_chiarire"] == [
        {
            "label": "Calcolo da chiarire",
            "amount_display": "€ 1.432,00",
            "motivo": reconciliation["explanation"],
        }
    ]

    # Highest-impact action is access through third-party property.
    verifiche = sections["verifiche"]
    assert verifiche["items"][0]["title"] == "Verificare il titolo di accesso al magazzino/rustico"
    assert "immobile di altra proprietà" in verifiche["items"][0]["why"]
    assert model["esito"]["headline"] == (
        "ATTENZIONE — Verifiche tecniche e legali necessarie prima di procedere"
    )

    # Occupation is source-faithful: debtor household, no invented lease.
    occupancy = sections["occupazione"]
    assert "nucleo famigliare del soggetto esecutato" in occupancy["stato"]
    assert "non indica contratti di locazione" in occupancy["cosa_verificare"][0]
    occupancy_blob = json.dumps(occupancy, ensure_ascii=False).lower()
    assert "conduttore" not in occupancy_blob and "canone" not in occupancy_blob

    findings = {finding["title"]: finding for finding in model["findings"]}
    assert findings["Completezza documentazione ex art. 567 c.p.c."]["status_label"] == "Dichiarata completa dalla perizia"
    assert findings["Titolarità e diritti posti in vendita"]["status_label"] == "Proprietà 1/1 dichiarata dalla perizia"
    assert findings["Continuità delle trascrizioni"]["status_label"] == "Continuità delle trascrizioni dichiarata dalla perizia"
    assert findings["Agibilità/abitabilità"]["customer_summary"] == (
        "Bene 1 e Bene 2 agibili; Bene 3 non risulta agibile"
    )

    docfa = findings["Catastale"]
    assert "aggiornamenti DOCFA predisposti dal perito" in docfa["customer_summary"]
    assert "Verificare l’avvenuta registrazione" in docfa["customer_summary"]
    assert "registrazione non è attestata" in docfa["evidence"]["excerpt"]
    assert "DOCFA registrato" not in json.dumps(docfa, ensure_ascii=False)

    # Existing mortgage/seizure are future procedural cancellations, not debt.
    formalities = sections["formalita"]["cancellate"]
    assert {row["type_label"] for row in formalities} == {"Ipoteca", "Pignoramento"}
    assert all(row["cancellation_state"] == "to_be_cancelled" for row in formalities)
    assert all(row["statement"].startswith("Da cancellare") for row in formalities)
    assert all("non è un debito da sommare" in row["note"] for row in formalities)
    assert "Cancellata dalla procedura" not in json.dumps(formalities, ensure_ascii=False)

    # APE may support APE only. Unrelated categories either have their own
    # decisive excerpt or the explicit fail-closed placeholder.
    for title in ("Edilizia", "Agibilità/abitabilità", "Urbanistica"):
        evidence = findings[title]["evidence"]
        assert "certificato energetico" not in str(evidence.get("excerpt") or "").lower()
    sources = sections["fonti"]["primary"]
    garage_sources = [source for source in sources if "Bene N° 3" in source["title"]]
    assert garage_sources
    assert all(
        "Bene N° 3" in str(source.get("excerpt") or "")
        and "6.480,00" in str(source.get("excerpt") or "")
        for source in garage_sources
    )
    assert all(
        "26.100,00" not in str(source.get("excerpt") or "")
        for source in garage_sources
    )

    # One authoritative action model drives all counters.
    open_statuses = {
        "da_verificare",
        "conferma_necessaria",
        "verifica_tecnica_richiesta",
    }
    rendered_open = sum(
        item["status"] in open_statuses for item in verifiche["items"]
    )
    assert verifiche["open_count"] == rendered_open
    assert verifiche["total"] == len(verifiche["items"])
    rendered_confirmations = sum(
        item["status"] == "conferma_necessaria" for item in verifiche["items"]
    )
    rendered_professional = sum(
        item["status"] in {"da_verificare", "verifica_tecnica_richiesta"}
        for item in verifiche["items"]
    )
    assert (rendered_confirmations, rendered_professional) == (1, 7)
    assert model["readiness"]["confirmations_open"] == rendered_confirmations
    assert model["readiness"]["professional_checks_open"] == rendered_professional
    assert rendered_confirmations + rendered_professional == rendered_open
    assert all(
        model["readiness"][key] == value
        for key, value in sections["stato_verifiche"].items()
    )

    # The customer payload contains source facts, not implementation or PII.
    customer_blob = json.dumps(customer, ensure_ascii=False).lower()
    for forbidden in (
        "schema non prevede",
        "parser limitation",
        "backend-field",
        "database-field",
        "beta_quota",
        "beta_perizia",
    ):
        assert forbidden not in customer_blob
    assert "@gmail.com" not in customer_blob
    assert "_cached_input_pages" not in customer
