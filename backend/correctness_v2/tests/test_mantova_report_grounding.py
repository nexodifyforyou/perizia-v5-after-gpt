import json
from copy import deepcopy
from pathlib import Path

from correctness_v2 import customer_view, decision_model


FIXTURE = Path(__file__).parent / "fixtures" / "mantova_customer_report_sanitized.json"


def _report():
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _model():
    return customer_view.sanitize_customer_report(_report(), {"safe_to_show_customer": True})["decision_model"]


def test_mantova_golden_structure_values_and_reconciliation():
    report = _report()
    model = _model()
    assert report["lot_structure"]["lot_count"] == 1
    assert report["lot_structure"]["bene_count"] == len(report["beni_sections"]) == 3
    composition = model["sections"]["numeri"]["composizione_valore"]
    assert [x["amount"] for x in composition["items"]] == [92700, 26100, 6480]
    assert "92.700,00" in composition["items"][0]["evidence"]["excerpt"]
    assert "26.100,00" in composition["items"][1]["evidence"]["excerpt"]
    assert composition["items"][2]["evidence"]["page"] == 26
    assert "6.480,00" in composition["items"][2]["evidence"]["excerpt"]
    assert composition["total"] == 125280
    rec = model["sections"]["numeri"]["riconciliazione"]
    assert rec == {**rec, "component_sum": 125280, "percentage_sum": 35.0,
                   "expected_final": 81432.0, "declared_final": 80000.0, "difference": 1432.0}
    assert len(model["sections"]["numeri"]["da_chiarire"]) == 1
    assert model["sections"]["numeri"]["catena"][-1]["amount"] == 80000
    assert model["sections"]["numeri"]["auction"]["amount_display"] == "€ 80.000,00"
    assert "offerta minima" not in json.dumps(model, ensure_ascii=False).lower()
    assert not [f for f in model["findings"] if f["section"] == "numeri"]


def test_evidence_validation_is_category_bene_and_amount_scoped_fail_closed():
    evidence = _report()["customer_evidence_index"]
    garage = decision_model._find_excerpt(evidence, [25, 26], ("valore di stima bene n 3",), expected_amount=6480, expected_bene="Bene N° 3")
    assert garage["page"] == 26 and "6.480,00" in garage["excerpt"]
    assert decision_model._find_excerpt(evidence, [25], ("valore di stima bene n 3",), expected_amount=6480, expected_bene="Bene N° 3") is None
    for category in ("urbanistica", "agibilita", "vincoli", "edilizia"):
        assert decision_model._find_excerpt(evidence, [23], (category,)) is None
    missing = decision_model._missing_evidence([23, 24])
    assert missing["note"] == "Estratto decisivo non disponibile" and missing["page"] == 23
    assert json.loads(json.dumps(garage, ensure_ascii=False)) == garage
    metadata_only_bene = [{
        "page": 25,
        "topic": "Valore di stima Bene N° 3 - Garage",
        "report_section": "Valori",
        "perizia_excerpt": "Valore di stima del bene: € 6.480,00",
        "coverage_status": "covered",
    }]
    assert decision_model._find_excerpt(
        metadata_only_bene, [25], ("valore di stima bene n 3",),
        expected_amount=6480, expected_bene="Bene N° 3",
    ) is None
    wrong_asset_same_bene_and_amount = [{
        "page": 26,
        "topic": "Valore di stima Bene N° 3 - Garage",
        "report_section": "Valori",
        "perizia_excerpt": "Bene N° 3 - Magazzino. Valore di stima: € 6.480,00",
        "coverage_status": "covered",
    }]
    assert decision_model._find_excerpt(
        wrong_asset_same_bene_and_amount, [26], ("valore di stima bene n 3",),
        expected_amount=6480, expected_bene="Bene N° 3", expected_asset="Garage",
    ) is None


def test_declared_statuses_disambiguation_priority_formalities_and_counters():
    model = _model()
    by_title = {f["title"]: f for f in model["findings"]}
    assert by_title["Completezza documentazione ex art. 567 c.p.c."]["status_label"] == "Dichiarata completa dalla perizia"
    assert by_title["Titolarità e diritti posti in vendita"]["status_label"] == "Proprietà 1/1 dichiarata dalla perizia"
    assert by_title["Continuità delle trascrizioni"]["status_label"] == "Continuità delle trascrizioni dichiarata dalla perizia"
    checks = model["sections"]["verifiche"]
    assert checks["items"][0]["title"] == "Verificare il titolo di accesso al magazzino/rustico"
    assert checks["open_count"] == sum(i["status"] in {"da_verificare", "conferma_necessaria", "verifica_tecnica_richiesta"} for i in checks["items"])
    assert model["readiness"]["professional_checks_open"] >= 1
    assert model["esito"]["headline"].startswith("ATTENZIONE")
    assert model["esito"]["drivers"][0]["title"] == (
        "Verificare il titolo di accesso al magazzino/rustico"
    )
    formalities = model["sections"]["formalita"]["cancellate"]
    assert all(c["cancellation_state"] == "to_be_cancelled" for c in formalities)
    assert all("Da cancellare" in c["statement"] for c in formalities)
    assert "Cancellata dalla procedura" not in json.dumps(model, ensure_ascii=False)


def test_customer_payload_is_sanitized_and_has_no_internal_or_personal_language():
    out = customer_view.sanitize_customer_report(_report(), {"safe_to_show_customer": True})
    blob = json.dumps(out, ensure_ascii=False).lower()
    for forbidden in ("schema non prevede", "parser", "backend", "database", "beta_quota", "beta_perizia"):
        assert forbidden not in blob
    assert "@gmail.com" not in blob
    assert "vincoli di edilizia convenzionata o pubblica" in blob
    assert "impianti non a norma" not in blob
    assert "bene 1 e bene 2 agibili; bene 3 non risulta agibile" in blob


def test_declared_status_mapping_is_negative_first_and_does_not_invent_full_ownership():
    report = _report()
    report["compliance_section"] = [
        {
            "area": "Completezza documentazione ex art. 567 c.p.c.",
            "classification": "uncertain",
            "notes": "Non dichiarata dalla perizia.",
            "evidence_pages": [6],
        },
        {
            "area": "Completezza documentazione ex art. 567 c.p.c. — allegati",
            "classification": "uncertain",
            "notes": "Dichiarata incompleta dalla perizia.",
            "evidence_pages": [7],
        },
        {
            "area": "Titolarità e diritti posti in vendita",
            "classification": "uncertain",
            "notes": "Proprietà dichiarata dalla perizia.",
            "evidence_pages": [8],
        },
        {
            "area": "Titolarità e diritti posti in vendita — quota non esposta",
            "classification": "uncertain",
            "notes": "Titolarità non dichiarata dalla perizia.",
            "evidence_pages": [9],
        },
        {
            "area": "Continuità delle trascrizioni",
            "classification": "uncertain",
            "notes": "Continuità non dichiarata dalla perizia.",
            "evidence_pages": [10],
        },
    ]
    findings = decision_model._build_conformity_findings(report, [], "1")

    not_declared = [item for item in findings if item["page"] in {6, 9, 10}]
    assert all(item["status"] == "non_dichiarato" for item in not_declared)
    assert all(item["status_label"] == "Non dichiarato dalla perizia" for item in not_declared)
    incomplete = next(item for item in findings if item["page"] == 7)
    assert incomplete["status"] == "da_chiarire"
    assert incomplete["status_label"] == "Dichiarata incompleta dalla perizia"
    generic_ownership = next(item for item in findings if item["page"] == 8)
    assert generic_ownership["status_label"] == "Dichiarato dalla perizia"
    assert "1/1" not in generic_ownership["status_label"]
    assert len(findings) == 5


def test_cached_component_recovery_uses_a_narrow_pii_safe_span_and_fails_closed():
    safe_after_amount = {
        "_cached_input_pages": [{
            "page_number": 26,
            "text": (
                "Bene N° 3 - Garage\nValore di stima del bene: € 6.480,00\n"
                "Contatto tecnico: fixture.person@example.test"
            ),
        }],
    }
    recovered = decision_model._find_cached_component_excerpt(
        safe_after_amount, [26], "Bene N° 3", 6480, "Garage",
    )
    assert recovered is not None
    assert recovered["excerpt"].endswith("€ 6.480,00")
    assert "@" not in recovered["excerpt"]

    for sensitive_text in (
        "fixture.person@example.test",
        "RSSMRA85M01H501Z",
    ):
        unsafe_before_amount = {
            "_cached_input_pages": [{
                "page_number": 26,
                "text": (
                    f"Bene N° 3 - Garage {sensitive_text} "
                    "Valore di stima del bene: € 6.480,00"
                ),
            }],
        }
        assert decision_model._find_cached_component_excerpt(
            unsafe_before_amount, [26], "Bene N° 3", 6480, "Garage",
        ) is None


def test_valuation_reconciliation_deduplicates_spans_and_ignores_adjacent_iva():
    report = _report()
    decisive = next(
        item for item in report["customer_evidence_index"]
        if item["topic"] == "Valore finale di stima"
    )
    decisive["perizia_excerpt"] = (
        "IVA 22% - Rischio mancata garanzia 10% - Stato d'uso e manutenzione 10% - "
        "Oneri di regolarizzazione 15% - Valore finale di stima: € 80.000,00"
    )
    report["customer_evidence_index"].append(deepcopy(decisive))

    model = customer_view.sanitize_customer_report(
        report, {"safe_to_show_customer": True}
    )["decision_model"]
    reconciliation = model["sections"]["numeri"]["riconciliazione"]
    assert reconciliation["percentage_sum"] == 35.0
    assert reconciliation["expected_final"] == 81432.0
    assert reconciliation["difference"] == 1432.0
    assert "22%" not in reconciliation["explanation"]


def test_formality_states_distinguish_already_future_negated_and_not_stated():
    report = _report()
    report["formalities_section"] = [
        {
            "type": "ipoteca",
            "type_label": "Ipoteca",
            "description": "Risulta già cancellata secondo la perizia.",
            "cancelled_by_procedure": True,
            "buyer_burden": False,
            "amount": 100000,
            "evidence_pages": [31],
        },
        {
            "type": "pignoramento",
            "type_label": "Pignoramento",
            "description": "Non risulta già cancellata; da cancellare con il decreto di trasferimento.",
            "cancelled_by_procedure": True,
            "buyer_burden": False,
            "evidence_pages": [32],
        },
        {
            "type": "servitu",
            "type_label": "Servitù",
            "description": "La perizia elenca la formalità ma non ne indica la cancellazione.",
            "cancelled_by_procedure": False,
            "buyer_burden": False,
            "evidence_pages": [33],
        },
    ]
    section, findings = decision_model._build_formalita(report, "1")
    by_type = {item["type_label"]: item for item in section["cancellate"]}
    assert by_type["Ipoteca"]["cancellation_state"] == "already_cancelled"
    assert by_type["Ipoteca"]["statement"] == "Già cancellata secondo la perizia."
    assert by_type["Pignoramento"]["cancellation_state"] == "to_be_cancelled"
    assert by_type["Pignoramento"]["statement"].startswith("Da cancellare")
    assert section["da_verificare"][0]["type_label"] == "Servitù"
    assert len(findings) == 1 and findings[0]["status"] == "da_verificare"


def test_third_party_access_action_uses_generic_asset_title_outside_warehouse_and_garage():
    report = _report()
    report["risk_sections"] = [{
        "section_id": "rischi",
        "items": [{
            "area": "Accesso all'appartamento",
            "summary": "Accesso attraverso un immobile di altra proprietà non compreso nella procedura",
            "evidence_pages": [14],
        }],
    }]
    model = decision_model.build_decision_model(report, [])
    access = model["sections"]["verifiche"]["items"][0]
    assert access["title"] == "Verificare il titolo di accesso all’immobile"
    assert "magazzino" not in access["title"].lower()


def test_missing_gas_and_electric_declarations_do_not_become_proven_noncompliance():
    report = _report()
    report["compliance_section"] = [
        {
            "area": "Impianto gas",
            "classification": "non_conforming",
            "notes": "Non contiene una dichiarazione di conformità dell'impianto gas.",
            "evidence_pages": [20],
        },
        {
            "area": "Impianto elettrico",
            "classification": "non_conforming",
            "notes": "Dichiarazioni non disponibili per l'impianto elettrico.",
            "evidence_pages": [21],
        },
    ]
    missing = decision_model._build_conformity_findings(report, [], "1")
    assert {item["topic"] for item in missing} == {"impianto_gas", "impianto_elettrico"}
    for item in missing:
        assert item["status"] == "da_verificare"
        assert item["status_label"] == "Dichiarazioni non disponibili secondo la perizia"
        assert item["recommended_action"].startswith("Verificare")
        customer_text = json.dumps(item, ensure_ascii=False).lower()
        assert "impianto non a norma" not in customer_text
        assert "impianti siano non conformi" in customer_text

    report["compliance_section"] = [{
        "area": "Impianto gas",
        "classification": "non_conforming",
        "notes": "Impianto gas non conforme secondo la perizia.",
        "evidence_pages": [20],
    }]
    explicit = decision_model._build_conformity_findings(report, [], "1")[0]
    assert explicit["status"] == "non_conforme"
    assert explicit["status_label"] == "Non conforme secondo la perizia"


def test_wrong_first_component_source_does_not_hide_a_later_correct_source():
    model = _model()
    source = next(
        item for item in model["sections"]["fonti"]["primary"]
        if item["title"] == "Valore di stima Bene N° 3 - Garage"
    )
    assert source["page"] == 26
    assert source["excerpt_status"] == "covered"
    assert "Bene N° 3" in source["excerpt"]
    assert "6.480,00" in source["excerpt"]
    assert "26.100,00" not in source["excerpt"]


def test_normalized_fact_validation_suppresses_heading_only_and_wrong_urban_code():
    report = _report()
    report["compliance_section"] = [
        {
            "area": "Completezza documentazione ex art. 567 c.p.c.",
            "classification": "uncertain",
            "notes": "La documentazione risulta completa.",
            "evidence_pages": [6],
        },
        {
            "area": "Destinazione urbanistica PGT",
            "classification": "uncertain",
            "notes": "Classificazione AREC 2 in area di attenzione archeologica.",
            "evidence_pages": [28],
        },
    ]
    report["customer_evidence_index"] = [
        {
            "page": 6,
            "topic": "Completezza documentazione ex art. 567 c.p.c.",
            "report_section": "Conformità",
            "perizia_excerpt": "9 COMPLETEZZA DOCUMENTAZIONE EX ART. 567 C.P.C.",
            "coverage_status": "covered",
        },
        {
            "page": 28,
            "topic": "Destinazione urbanistica PGT",
            "report_section": "Conformità",
            "perizia_excerpt": "Destinazione urbanistica secondo il PGT vigente: ambito residenziale.",
            "coverage_status": "covered",
        },
    ]
    model = decision_model.build_decision_model(report, [])
    findings = {
        item["title"]: item for item in model["findings"]
        if item["section"] == "conformita"
    }
    for title in ("Completezza documentazione ex art. 567 c.p.c.", "Urbanistica"):
        assert findings[title]["evidence"]["excerpt"] is None
        assert findings[title]["evidence"]["note"] == "Estratto decisivo non disponibile"
    sources = {item["title"]: item for item in model["sections"]["fonti"]["primary"]}
    assert sources["Completezza documentazione ex art. 567 c.p.c."]["excerpt_status"] == "excerpt_missing"
    assert sources["Destinazione urbanistica PGT"]["excerpt_status"] == "excerpt_missing"


def test_docfa_prepared_is_not_rendered_as_registered_and_requires_final_alignment_check():
    report = _report()
    report["compliance_section"] = [{
        "area": "Conformità catastale / DOCFA",
        "classification": "uncertain",
        "notes": "Difformità catastali rilevate; il perito ha predisposto nuove planimetrie DOCFA.",
        "evidence_pages": [22],
    }]
    finding = decision_model._build_conformity_findings(report, [], "1")[0]
    assert finding["status"] == "da_verificare"
    assert finding["customer_summary"] == (
        "Difformità rilevate; aggiornamenti DOCFA predisposti dal perito. "
        "Verificare l’avvenuta registrazione e la situazione catastale definitiva."
    )
    assert "DOCFA registrato" not in finding["customer_summary"]
    assert finding["recommended_action"].startswith("Verificare")


def test_mantova_checklist_has_exact_buyer_impact_order_and_reconciled_count():
    model = _model()
    checks = model["sections"]["verifiche"]
    assert [item["title"] for item in checks["items"]] == [
        "Verificare il titolo di accesso al magazzino/rustico",
        "Verificare le difformità edilizie e l’agibilità del garage",
        "Verificare la situazione di occupazione e l'opponibilità del titolo",
        "Calcolo da chiarire",
        "Verificare registrazione DOCFA e allineamento catastale definitivo",
        "Verificare la copertura danneggiata del magazzino",
        "Verificare dichiarazioni e stato degli impianti",
        "Verificare la disponibilità dell’APE",
    ]
    assert checks["total"] == len(checks["items"]) == 8
    assert checks["open_count"] == 8
    assert checks["completed_count"] == 0
    confirmation_items = sum(
        item["status"] == "conferma_necessaria" for item in checks["items"]
    )
    professional_items = sum(
        item["status"] in {"da_verificare", "verifica_tecnica_richiesta"}
        for item in checks["items"]
    )
    assert (confirmation_items, professional_items) == (1, 7)
    assert model["readiness"]["confirmations_open"] == confirmation_items
    assert model["readiness"]["professional_checks_open"] == professional_items
    assert confirmation_items + professional_items == checks["open_count"]
