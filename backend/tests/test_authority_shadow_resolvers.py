import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import perizia_authority_resolvers as shadow  # noqa: E402
from customer_decision_contract import (  # noqa: E402
    sanitize_customer_facing_result,
    separate_internal_runtime_from_customer_result,
)
from perizia_section_authority import build_section_authority_map  # noqa: E402


FORBIDDEN_PAYLOAD_KEYS = {
    "authority_shadow_resolvers",
    "authority_resolver",
    "shadow_authority",
    "section_zone",
    "authority_level",
    "authority_score",
    "domain_hints",
    "answer_point",
    "reason_for_authority",
    "is_instruction_like",
    "is_answer_like",
    "source_stage",
    "extractor_version",
}


def _pages(*texts: str) -> List[Dict[str, Any]]:
    return [{"page_number": idx, "text": text} for idx, text in enumerate(texts, start=1)]


def _build(*texts: str) -> Dict[str, Any]:
    pages = _pages(*texts)
    section_map = build_section_authority_map(pages)
    return shadow.build_authority_shadow_resolvers(pages, section_map)


def _value(payload: Dict[str, Any], domain: str) -> Dict[str, Any]:
    return payload[domain]["value"]


def _collect_forbidden(value: Any, path: str = "result") -> List[str]:
    hits: List[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            key_text = str(key)
            child_path = f"{path}.{key_text}"
            if key_text.startswith("authority_") or key_text in FORBIDDEN_PAYLOAD_KEYS:
                hits.append(child_path)
            hits.extend(_collect_forbidden(child, child_path))
    elif isinstance(value, list):
        for idx, item in enumerate(value):
            hits.extend(_collect_forbidden(item, f"{path}[{idx}]"))
    return hits


def test_lot_toc_unico_final_multilot_wins():
    payload = _build(
        "INDICE\nLOTTO UNICO ........ 2\nFormalita ........ 3\nStima ........ 4",
        "SCHEMA RIASSUNTIVO\nLOTTO 1 prezzo base Euro 10.000,00\nLOTTO 2 prezzo base Euro 20.000,00\nLOTTO 3 prezzo base Euro 30.000,00",
    )

    lot = _value(payload, "lot_structure")
    assert lot["shadow_lot_mode"] == "multi_lot"
    assert set(lot["detected_lot_numbers"]) >= {1, 2, 3}
    assert lot["has_high_authority_multilot"] is True
    assert any(ev.get("signal") == "lotto_unico" for ev in payload["lot_structure"]["rejected_conflicts"])


def test_lot_toc_multilot_final_unico_wins():
    payload = _build(
        "INDICE\nLOTTO 1 ........ 2\nLOTTO 2 ........ 3\nLOTTO 3 ........ 4",
        "SCHEMA RIASSUNTIVO\nFORMAZIONE LOTTI\nLOTTO UNICO\nIl compendio e vendibile in un unico lotto.",
    )

    lot = _value(payload, "lot_structure")
    assert lot["shadow_lot_mode"] == "single_lot"
    assert lot["has_high_authority_lotto_unico"] is True
    assert any(ev.get("lot_number") in {1, 2, 3} for ev in payload["lot_structure"]["rejected_conflicts"])


def test_lot_toc_only_repeated_lot_numbers_do_not_create_multilot():
    payload = _build("INDICE\nLOTTO 1 ........ 2\nLOTTO 2 ........ 18\nLOTTO 3 ........ 29")

    lot_result = payload["lot_structure"]
    lot = lot_result["value"]
    assert lot["shadow_lot_mode"] == "unknown"
    assert lot["has_high_authority_multilot"] is False
    assert lot_result["confidence"] < 0.5


def test_lot_low_authority_unico_only_is_not_confident_single_lot():
    payload = _build("INDICE\nLOTTO UNICO ........ 2\nDescrizione ........ 3\nStima ........ 4")

    lot_result = payload["lot_structure"]
    lot = lot_result["value"]
    assert lot["shadow_lot_mode"] == "unknown" or lot_result["confidence"] < 0.5
    assert lot["has_high_authority_lotto_unico"] is False


def test_lot_final_formazione_lotti_unico_is_high_confidence_single_lot():
    payload = _build("STIMA / FORMAZIONE LOTTI\nLOTTO UNICO\nValore finale di stima Euro 100.000,00")

    lot_result = payload["lot_structure"]
    assert lot_result["value"]["shadow_lot_mode"] == "single_lot"
    assert lot_result["confidence"] >= 0.85


def test_lot_chapter_based_multilot_topology_is_high_confidence_multilot():
    payload = _build(
        "TRIBUNALE ORDINARIO\nLOTTO 1\n"
        "1. IDENTIFICAZIONE DEI BENI IMMOBILI OGGETTO DI VENDITA:\n"
        "A appartamento in Via Roma, della superficie commerciale di 80 mq per la quota di 1/1.\n"
        "3. STATO DI POSSESSO AL MOMENTO DEL SOPRALLUOGO: occupato.\n"
        "4. VINCOLI ED ONERI GIURIDICI.\n"
        "VALUTAZIONE: valore di mercato Euro 100.000,00.",
        "TRIBUNALE ORDINARIO\nLOTTO 2\n"
        "1. IDENTIFICAZIONE DEI BENI IMMOBILI OGGETTO DI VENDITA:\n"
        "A box auto in Via Roma, della superficie commerciale di 20 mq per la quota di 1/1.\n"
        "GIUDIZI DI CONFORMITA urbanistica e catastale.\n"
        "VALUTAZIONE: valore di mercato Euro 20.000,00.",
        "TRIBUNALE ORDINARIO\nLOTTO 3\n"
        "1. IDENTIFICAZIONE DEI BENI IMMOBILI OGGETTO DI VENDITA:\n"
        "A deposito in Via Roma, della superficie commerciale di 12 mq per la quota di 1/1.\n"
        "VINCOLI ED ONERI GIURIDICI: nessuno.\n"
        "VALUTAZIONE: valore di mercato Euro 10.000,00.",
    )

    lot_result = payload["lot_structure"]
    lot = lot_result["value"]
    assert lot["shadow_lot_mode"] == "multi_lot"
    assert set(lot["detected_lot_numbers"]) == {1, 2, 3}
    assert set(lot["chapter_lot_numbers"]) == {1, 2, 3}
    assert lot["has_high_authority_multilot"] is True
    assert lot_result["confidence"] >= 0.85
    assert "chapter_based_multi_lot_topology" in lot_result["authority_basis"]["rules_triggered"]


def test_lot_context_only_lotto_references_do_not_create_chapter_multilot():
    payload = _build(
        "Nella descrizione si richiama lo stesso Lotto 1 della procedura riunita.",
        "Il CTU cita il Lotto 2 di una precedente perizia senza aprire un capitolo autonomo.",
    )

    lot_result = payload["lot_structure"]
    assert lot_result["value"]["shadow_lot_mode"] == "unknown"
    assert lot_result["value"]["has_high_authority_multilot"] is False


def test_lot_unico_with_multiple_beni_is_not_chapter_multilot():
    payload = _build(
        "STIMA / FORMAZIONE LOTTI\nLOTTO UNICO\n"
        "Bene N. 1 - appartamento.\n"
        "Bene N. 2 - autorimessa.\n"
        "Il compendio e vendibile in unico lotto."
    )

    lot_result = payload["lot_structure"]
    assert lot_result["value"]["shadow_lot_mode"] == "single_lot"
    assert lot_result["value"]["chapter_lot_numbers"] == []
    assert lot_result["value"]["has_high_authority_multilot"] is False


def test_lot_chapter_multilot_conflicting_with_high_lotto_unico_stays_unknown():
    payload = _build(
        "STIMA / FORMAZIONE LOTTI\nLOTTO UNICO\nIl compendio e vendibile in un unico lotto.",
        "LOTTO 1\n1. IDENTIFICAZIONE DEI BENI IMMOBILI OGGETTO DI VENDITA:\n"
        "A appartamento in Via Roma, della superficie commerciale di 80 mq per la quota di 1/1.\n"
        "VALUTAZIONE: Euro 100.000,00.",
        "LOTTO 2\n1. IDENTIFICAZIONE DEI BENI IMMOBILI OGGETTO DI VENDITA:\n"
        "A box in Via Roma, della superficie commerciale di 20 mq per la quota di 1/1.\n"
        "VALUTAZIONE: Euro 20.000,00.",
    )

    lot_result = payload["lot_structure"]
    lot = lot_result["value"]
    assert lot["shadow_lot_mode"] == "unknown"
    assert lot["chapter_topology_conflicts"]
    assert lot_result["status"] == "WARN"


def test_occupancy_instruction_only_does_not_create_factual_status():
    payload = _build("Quesito: verifichi lo stato di occupazione dell'immobile e dica se sia libero.")

    occupancy = _value(payload, "occupancy")
    assert occupancy["shadow_occupancy_status"] in {"UNKNOWN", "NON_VERIFICABILE"}


def test_occupancy_final_schema_terzi_without_title_wins():
    payload = _build("SCHEMA RIASSUNTIVO\nStato di occupazione: Occupato da terzi senza titolo.")

    assert _value(payload, "occupancy")["shadow_occupancy_status"] == "OCCUPATO_DA_TERZI"


def test_occupancy_final_schema_libero_wins():
    payload = _build("SCHEMA RIASSUNTIVO\nStato di occupazione: Libero.")

    assert _value(payload, "occupancy")["shadow_occupancy_status"] == "LIBERO"


def test_occupancy_high_authority_occupied_rejects_weak_libero_conflict():
    payload = _build(
        "Si richiama la precedente perizia: l'immobile era libero in passato.",
        "SCHEMA RIASSUNTIVO\nStato di occupazione: Occupato dal debitore esecutato.",
    )

    result = payload["occupancy"]
    assert result["value"]["shadow_occupancy_status"] == "OCCUPATO_DA_DEBITORE"
    assert any(ev.get("status") == "LIBERO" for ev in result["rejected_conflicts"])


def test_opponibilita_instruction_only_is_non_verificabile():
    payload = _build("Quesito: verifichi se esistano contratti di locazione opponibili alla procedura.")

    opp = _value(payload, "opponibilita")
    assert opp["shadow_opponibilita_status"] in {"UNKNOWN", "NON_VERIFICABILE"}
    assert opp["instruction_only_mentions"]


def test_opponibilita_factual_lease_discussion_classifies_status():
    payload = _build(
        "Risposta al quesito n. 6\nContratto di locazione registrato anteriormente al pignoramento, opponibile alla procedura."
    )

    opp = _value(payload, "opponibilita")
    assert opp["shadow_opponibilita_status"] == "OPPONIBILE"
    assert opp["lease_title_evidence"]


def test_opponibilita_no_factual_lease_basis_has_no_legal_killer_shape():
    payload = _build("Descrizione dell'immobile e dei dati catastali senza contratti di locazione.")

    opp = _value(payload, "opponibilita")
    assert opp["shadow_opponibilita_status"] in {"UNKNOWN", "NON_VERIFICABILE"}
    assert "legal_killer_candidates" not in opp


def test_legal_formalities_cancellable_are_not_killers():
    payload = _build(
        "Formalita pregiudizievoli da cancellare: iscrizione ipotecaria e trascrizione del pignoramento."
    )

    legal = _value(payload, "legal_formalities")
    assert legal["formalities_to_cancel"]
    assert legal["legal_killer_candidates"] == []


def test_legal_generic_ipoteca_mention_is_not_killer():
    payload = _build("Nel fascicolo della procedura e citata una ipoteca.")

    legal = _value(payload, "legal_formalities")
    assert legal["generic_legal_mentions"]
    assert legal["legal_killer_candidates"] == []


def test_legal_explicit_surviving_encumbrance_can_be_killer_candidate():
    payload = _build(
        "Formalita pregiudizievoli: servitu non cancellabile, opponibile all'acquirente e destinata a permanere."
    )

    legal = _value(payload, "legal_formalities")
    assert legal["surviving_formalities"]
    assert legal["legal_killer_candidates"]


def test_money_roles_do_not_promote_non_cost_amounts_to_buyer_costs():
    payload = _build(
        "Dati catastali: rendita catastale Euro 123,00.",
        "Prezzo base d'asta Euro 80.000,00.",
        "Valore di stima Euro 100.000,00.\nDeprezzamento Euro 10.000,00.",
        "Spese tecniche di regolarizzazione Euro 1.500,00.",
        "Importo Euro 777,00.",
    )

    money = _value(payload, "money_roles")
    assert money["cadastral_rendita"]
    assert money["base_auction"]
    assert money["market_value"] or money["final_value"]
    assert money["valuation_deduction"]
    assert money["buyer_cost_signal_to_verify"]
    assert money["unknown_money"]
    assert "buyer_cost" not in money


def test_money_total_and_components_are_identified_without_double_counting_policy_gap():
    payload = _build(
        "Regolarizzazione urbanistica:\n"
        "Spese tecniche Euro 1.000,00\n"
        "Sanzione Euro 500,00\n"
        "Totale regolarizzazione Euro 1.500,00"
    )

    money = _value(payload, "money_roles")
    assert money["total_candidate"]
    assert money["component_of_total"]
    assert money["counting_policy"] == "components_are_not_summed_when_total_candidate_is_present"


def test_shadow_resolvers_missing_and_corrupt_authority_maps_fail_open():
    pages = _pages("SCHEMA RIASSUNTIVO\nLOTTO UNICO\nStato di occupazione: Libero.")
    missing = shadow.build_authority_shadow_resolvers(pages, {"_authority_tagging_status": "missing_map"})
    corrupt = shadow.build_authority_shadow_resolvers(pages, {"_authority_tagging_status": "corrupt_map"})

    for payload in (missing, corrupt):
        assert payload["fail_open"] is True
        for domain in ("lot_structure", "occupancy", "opponibilita", "legal_formalities", "money_roles"):
            assert payload[domain]["status"] == "FAIL_OPEN"
            assert payload[domain]["fail_open"] is True


def test_bad_authority_classification_on_one_page_is_partial_fail_open(monkeypatch: pytest.MonkeyPatch):
    pages = _pages(
        "Rendita catastale Euro 123,00.",
        "Prezzo base d'asta Euro 80.000,00.",
    )
    section_map = build_section_authority_map(pages)
    original = shadow.classify_quote_authority

    def flaky(page_number: int, quote: str, section_map: Dict[str, Any], domain: str | None = None):
        if int(page_number) == 1:
            raise ValueError("bad page authority row")
        return original(page_number, quote, section_map, domain=domain)

    monkeypatch.setattr(shadow, "classify_quote_authority", flaky)

    payload = shadow.build_authority_shadow_resolvers(pages, section_map)
    money = payload["money_roles"]
    assert money["status"] == "PARTIAL"
    assert money["fail_open"] is True
    assert money["value"]["cadastral_rendita"]
    assert money["value"]["base_auction"]


def test_customer_payload_invariance_strips_shadow_authority_keys():
    shadow_payload = _build("SCHEMA RIASSUNTIVO\nLOTTO UNICO\nStato di occupazione: Libero.")
    result = {
        "section_3_money_box": {
            "items": [
                {
                    "label": "Voce cliente",
                    "shadow_authority": {"section_zone": "FINAL_LOT_FORMATION"},
                    "authority_resolver": {"authority_level": "HIGH_FACTUAL"},
                }
            ],
            "authority_shadow_resolvers": shadow_payload,
        },
        "debug": {"authority_shadow_resolvers": shadow_payload},
    }

    sanitize_customer_facing_result(result)
    internal_runtime = separate_internal_runtime_from_customer_result(result)

    assert _collect_forbidden(result) == []
    assert internal_runtime["debug"]["authority_shadow_resolvers"]["schema_version"] == shadow.SCHEMA_VERSION
