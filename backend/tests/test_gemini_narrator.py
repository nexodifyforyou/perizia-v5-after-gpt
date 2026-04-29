import copy
import json
import os
import sys

import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import narrator
import server
from customer_decision_contract import apply_customer_decision_contract


SAMPLE_TOTAL_EUR = 1200
SAMPLE_COMPONENT_EUR = 400


@pytest.fixture()
def anyio_backend():
    return "asyncio"


def _base_result() -> dict:
    result = {
        "lots_count": 1,
        "is_multi_lot": False,
        "lots": [
            {
                "lot_number": "1",
                "tipologia": "Appartamento",
                "ubicazione": "Indirizzo indicato in perizia",
                "prezzo_base_eur": 100000,
                "valore_stima_eur": 120000,
                "stato_occupativo": "OCCUPATO",
            }
        ],
        "field_states": {
            "stato_occupativo": {"value": "OCCUPATO", "status": "FOUND", "headline_it": "Stato occupativo: OCCUPATO."},
            "agibilita": {"value": "DA VERIFICARE", "status": "LOW_CONFIDENCE"},
            "regolarita_urbanistica": {"value": "PRESENTI DIFFORMITA", "status": "FOUND"},
        },
        "issues": [
            {
                "severity": "RED",
                "family": "occupancy",
                "headline_it": "Immobile occupato.",
                "action_it": "Verificare titolo e tempi di liberazione.",
            },
            {
                "severity": "AMBER",
                "family": "urbanistica",
                "headline_it": "Regolarità urbanistica da approfondire.",
                "action_it": "Verificare sanabilità delle difformità.",
            },
        ],
        "semaforo_generale": {"status": "RED", "reason_it": "Occupazione e difformità urbanistiche."},
        "summary_for_client_bundle": {
            "decision_summary_it": "Deterministic Decisione Rapida.",
            "main_risk_it": "Immobile occupato.",
            "before_offer_it": ["Verificare titolo di occupazione."],
        },
        "summary_for_client": {"summary_it": "Deterministic Decisione Rapida."},
        "decision_rapida_client": {"summary_it": "Deterministic Decisione Rapida."},
        "section_2_decisione_rapida": {"summary_it": "Deterministic Decisione Rapida."},
        "money_box": {"policy": "CONSERVATIVE", "items": [], "cost_signals_to_verify": []},
    }
    result["customer_decision_contract"] = {
        "version": "customer_decision_contract_v1",
        "field_states": copy.deepcopy(result["field_states"]),
        "issues": copy.deepcopy(result["issues"]),
        "semaforo_generale": copy.deepcopy(result["semaforo_generale"]),
        "summary_for_client_bundle": copy.deepcopy(result["summary_for_client_bundle"]),
        "money_box": copy.deepcopy(result["money_box"]),
        "decision_rapida_client": copy.deepcopy(result["decision_rapida_client"]),
    }
    return result


def _valid_payload(**overrides) -> dict:
    payload = {
        "summary_it": "La perizia riguarda un appartamento con occupazione indicata e regolarità urbanistica da approfondire.",
        "decisione_rapida_it": "Prima dell'offerta imposta una verifica prudenziale su occupazione, titolo di detenzione e difformità urbanistiche con tecnico e delegato.",
        "main_risk_it": "Occupazione e regolarità urbanistica sono i principali punti da chiudere.",
        "why_it_matters_it": "Incidono su tempi di disponibilità del bene e margine economico dell'offerta.",
        "before_offer_it": [
            "Chiarire titolo di occupazione e tempi di liberazione.",
            "Verificare sanabilità delle difformità urbanistiche.",
            "Confrontare prezzo base e valore di stima senza sommare voci automatiche.",
        ],
        "not_to_confuse_it": "Prezzo base, valore di stima e deprezzamenti sono componenti valutative, non esborsi automatici.",
    }
    payload.update(overrides)
    return payload


def _serialized_customer_projection(result: dict) -> str:
    cdc = result.get("customer_decision_contract") if isinstance(result.get("customer_decision_contract"), dict) else {}
    payload = {
        "money_box": result.get("money_box"),
        "section_3_money_box": result.get("section_3_money_box"),
        "summary_for_client": result.get("summary_for_client"),
        "summary_for_client_bundle": result.get("summary_for_client_bundle"),
        "section_2_decisione_rapida": result.get("section_2_decisione_rapida"),
        "decision_rapida_client": result.get("decision_rapida_client"),
        "decision_rapida_narrated": result.get("decision_rapida_narrated"),
        "customer_decision_contract": {
            "money_box": cdc.get("money_box"),
            "section_3_money_box": cdc.get("section_3_money_box"),
            "summary_for_client": cdc.get("summary_for_client"),
            "summary_for_client_bundle": cdc.get("summary_for_client_bundle"),
            "section_2_decisione_rapida": cdc.get("section_2_decisione_rapida"),
            "decision_rapida_client": cdc.get("decision_rapida_client"),
            "decision_rapida_narrated": cdc.get("decision_rapida_narrated"),
        },
    }
    return json.dumps(payload, ensure_ascii=False)


def _component_total_result() -> dict:
    result = _base_result()
    money_box = {
        "policy": "CANONICAL_RUNTIME",
        "items": [
            {
                "label_it": f"Oblazione/sanatoria: € {SAMPLE_COMPONENT_EUR}",
                "amount_eur": SAMPLE_COMPONENT_EUR,
                "classification": "cost_signal_to_verify",
                "additive_to_extra_total": False,
                "evidence": [{"page": 1, "quote": f"Oblazione sanatoria euro {SAMPLE_COMPONENT_EUR}"}],
            }
        ],
        "cost_signals_to_verify": [
            {
                "label_it": f"Oblazione/sanatoria: € {SAMPLE_COMPONENT_EUR}",
                "amount_eur": SAMPLE_COMPONENT_EUR,
                "classification": "cost_signal_to_verify",
                "additive_to_extra_total": False,
                "evidence": [{"page": 1, "quote": f"Oblazione sanatoria euro {SAMPLE_COMPONENT_EUR}"}],
            }
        ],
        "valuation_deductions": [],
        "total_extra_costs": {
            "range": {"min": SAMPLE_TOTAL_EUR, "max": SAMPLE_TOTAL_EUR},
            "note": f"Totale stimato in perizia: € {SAMPLE_TOTAL_EUR}. Le singole voci quantificate sotto sono componenti del totale e non un secondo totale autonomo.",
        },
    }
    result["money_box"] = money_box
    result["customer_decision_contract"]["money_box"] = copy.deepcopy(money_box)
    return result


def _weak_occupancy_result() -> dict:
    result = _base_result()
    result["field_states"]["stato_occupativo"] = {
        "value": "LIBERO",
        "status": "FOUND",
        "headline_it": "Stato occupativo: LIBERO.",
        "evidence": [{"page": 1, "quote": "Trascrizione del pignoramento immobiliare presso i registri competenti."}],
    }
    result["lots"][0]["stato_occupativo"] = "LIBERO"
    result["customer_decision_contract"]["field_states"] = copy.deepcopy(result["field_states"])
    result["issues"] = [
        issue for issue in result["issues"] if issue.get("family") != "occupancy"
    ]
    result["customer_decision_contract"]["issues"] = copy.deepcopy(result["issues"])
    return result


async def _run_gemini_with_payload(monkeypatch, result, payload):
    async def fake_call(**_kwargs):
        return json.dumps(payload, ensure_ascii=False)

    monkeypatch.setattr(narrator, "_call_gemini_narrator_llm", fake_call)
    return await narrator.build_decisione_rapida_narration(
        result=result,
        request_id="test-request",
        enabled=True,
        provider="gemini",
        model="gemini-2.5-flash",
        api_key="test-key",
        timeout_seconds=1,
    )


@pytest.mark.anyio
async def test_gemini_disabled_fallback_keeps_deterministic_decisione_rapida():
    result = _base_result()
    original = copy.deepcopy(result["decision_rapida_client"])

    payload, meta = await narrator.build_decisione_rapida_narration(
        result=result,
        request_id="test-request",
        enabled=False,
        provider="gemini",
        model="gemini-2.5-flash",
        api_key="test-key",
    )

    assert payload is None
    assert meta["status"] == "SKIPPED"
    assert result["decision_rapida_client"] == original


@pytest.mark.anyio
async def test_gemini_valid_payload_writes_separate_summary_and_decision(monkeypatch):
    result = _base_result()
    payload, meta = await _run_gemini_with_payload(monkeypatch, result, _valid_payload())

    assert meta["status"] == "OK"
    narrator.apply_narrated_payload_to_result(result, payload, meta)

    assert result["summary_for_client"]["summary_it"] == payload["summary_it"]
    assert result["decision_rapida_client"]["decisione_rapida_it"] == payload["decisione_rapida_it"]
    assert result["decision_rapida_client"]["summary_it"] == payload["decisione_rapida_it"]
    assert result["section_2_decisione_rapida"]["summary_it"] == payload["decisione_rapida_it"]
    assert result["summary_for_client"]["summary_it"] != result["section_2_decisione_rapida"]["summary_it"]
    assert result["customer_decision_contract"]["summary_for_client"]["summary_it"] == payload["summary_it"]
    assert result["customer_decision_contract"]["decision_rapida_client"]["decisione_rapida_it"] == payload["decisione_rapida_it"]


@pytest.mark.anyio
async def test_gemini_rejects_identical_summary_and_decision(monkeypatch):
    same = "La perizia riguarda un appartamento occupato con difformità urbanistiche da verificare."
    payload, meta = await _run_gemini_with_payload(
        monkeypatch,
        _base_result(),
        _valid_payload(summary_it=same, decisione_rapida_it=same),
    )

    assert payload is None
    assert meta["status"] == "REJECTED_VALIDATION"
    assert "summary_decision_near_identical" in " ".join(meta["errors"])


@pytest.mark.anyio
async def test_gemini_rejects_near_identical_summary_and_decision(monkeypatch):
    payload, meta = await _run_gemini_with_payload(
        monkeypatch,
        _base_result(),
        _valid_payload(
            summary_it="La perizia riguarda un appartamento occupato con difformità urbanistiche da verificare prima dell'offerta.",
            decisione_rapida_it="La perizia riguarda un appartamento occupato con difformità urbanistiche da verificare prima dell'offerta con cautela.",
        ),
    )

    assert payload is None
    assert meta["status"] == "REJECTED_VALIDATION"


@pytest.mark.anyio
async def test_gemini_rejects_invented_euro_amount(monkeypatch):
    payload, meta = await _run_gemini_with_payload(
        monkeypatch,
        _base_result(),
        _valid_payload(decisione_rapida_it="Prima dell'offerta verifica occupazione e difformità; non assumere un costo di € 99.000 se non ancorato."),
    )

    assert payload is None
    assert meta["status"] == "REJECTED_VALIDATION"
    assert "unsupported_euro_amount" in " ".join(meta["errors"])


@pytest.mark.anyio
async def test_gemini_rejects_invented_lot_count(monkeypatch):
    payload, meta = await _run_gemini_with_payload(
        monkeypatch,
        _base_result(),
        _valid_payload(summary_it="La perizia riguarda due lotti con occupazione e difformità urbanistiche da approfondire."),
    )

    assert payload is None
    assert meta["status"] == "REJECTED_VALIDATION"
    assert "unsupported_lot_count" in " ".join(meta["errors"])


@pytest.mark.anyio
async def test_multi_lot_contract_accepts_decision_text_with_lot_by_lot_structure(monkeypatch):
    result = _base_result()
    result["lots_count"] = 2
    result["is_multi_lot"] = True
    result["lots"].append({"lot_number": "2", "tipologia": "Magazzino", "ubicazione": "Secondo indirizzo indicato"})
    result["customer_decision_contract"]["lots_count"] = 2
    result["customer_decision_contract"]["is_multi_lot"] = True
    result["customer_decision_contract"]["lots"] = copy.deepcopy(result["lots"])

    payload, meta = await _run_gemini_with_payload(
        monkeypatch,
        result,
        _valid_payload(
            summary_it="La perizia riguarda due lotti con destinazioni diverse e verifiche tecniche separate.",
            decisione_rapida_it="Prima dell'offerta leggi il caso lotto per lotto: separa occupazione, difformità e valore di ciascun bene prima di fissare il rilancio.",
        ),
    )

    assert meta["status"] == "OK"
    assert payload["decisione_rapida_it"]
    assert "lotto" in payload["decisione_rapida_it"].lower()


@pytest.mark.anyio
async def test_money_box_empty_rejects_buyer_side_cost_claim(monkeypatch):
    payload, meta = await _run_gemini_with_payload(
        monkeypatch,
        _base_result(),
        _valid_payload(decisione_rapida_it="Prima dell'offerta considera un costo extra a carico dell'acquirente e verifica occupazione e urbanistica."),
    )

    assert payload is None
    assert meta["status"] == "REJECTED_VALIDATION"
    assert "unsupported_buyer_cost_claim" in " ".join(meta["errors"])


def test_validator_rejects_strong_buyer_obligation_without_confirmation():
    fact_pack = narrator.build_clean_customer_decision_fact_pack(_base_result())

    errors = narrator.validate_gemini_decision_payload(
        _valid_payload(
            decisione_rapida_it=(
                "Prima dell'offerta verifica difformità e quantifica l'esborso effettivo "
                "a carico dell'acquirente con tecnico e delegato."
            )
        ),
        fact_pack,
    )

    assert any("unsupported_buyer_cost_claim" in error for error in errors)


def test_validator_accepts_safe_buyer_economic_wording():
    fact_pack = narrator.build_clean_customer_decision_fact_pack(_base_result())

    errors = narrator.validate_gemini_decision_payload(
        _valid_payload(
            decisione_rapida_it=(
                "Prima dell'offerta verifica le difformità e l'eventuale incidenza economica "
                "per l'acquirente con tecnico e delegato."
            )
        ),
        fact_pack,
    )

    assert errors == []


def test_validator_rejects_component_total_double_counting_language():
    fact_pack = narrator.build_clean_customer_decision_fact_pack(_component_total_result())

    errors = narrator.validate_gemini_decision_payload(
        _valid_payload(
            decisione_rapida_it=(
                f"Prima dell'offerta verifica costi di regolarizzazione stimati in € {SAMPLE_TOTAL_EUR} "
                f"e oblazione/sanatoria in € {SAMPLE_COMPONENT_EUR} con tecnico e delegato."
            )
        ),
        fact_pack,
    )

    assert any("double_counting" in error for error in errors)


def test_validator_accepts_total_with_component_wording():
    fact_pack = narrator.build_clean_customer_decision_fact_pack(_component_total_result())

    errors = narrator.validate_gemini_decision_payload(
        _valid_payload(
            decisione_rapida_it=(
                f"Prima dell'offerta considera € {SAMPLE_TOTAL_EUR} come totale stimato di regolarizzazione, "
                f"con componente interna di € {SAMPLE_COMPONENT_EUR} per sanatoria/oblazione da verificare."
            ),
            not_to_confuse_it=(
                f"€ {SAMPLE_TOTAL_EUR} è il totale stimato; € {SAMPLE_COMPONENT_EUR} è una componente interna e non va sommata due volte."
            ),
        ),
        fact_pack,
    )

    assert errors == []


def test_validator_rejects_confident_occupancy_claim_when_evidence_is_weak():
    fact_pack = narrator.build_clean_customer_decision_fact_pack(_weak_occupancy_result())

    errors = narrator.validate_gemini_decision_payload(
        _valid_payload(
            summary_it="La perizia riguarda un appartamento libero da occupazioni e con urbanistica da approfondire.",
            decisione_rapida_it="Prima dell'offerta verifica urbanistica e importi segnalati, assumendo l'immobile libero.",
        ),
        fact_pack,
    )

    assert any("unsupported_confident_occupancy_claim" in error for error in errors)


def test_validator_accepts_cautious_occupancy_wording_when_evidence_is_weak():
    fact_pack = narrator.build_clean_customer_decision_fact_pack(_weak_occupancy_result())

    errors = narrator.validate_gemini_decision_payload(
        _valid_payload(
            summary_it="La perizia riguarda un appartamento con regolarità urbanistica da approfondire.",
            decisione_rapida_it="Prima dell'offerta verifica la sezione stato di possesso, urbanistica e importi segnalati con tecnico e delegato.",
        ),
        fact_pack,
    )

    assert errors == []


def test_fact_pack_marks_weak_occupancy_evidence_as_cautious():
    fact_pack = narrator.build_clean_customer_decision_fact_pack(_weak_occupancy_result())

    occupancy = fact_pack["field_states"]["stato_occupativo"]
    assert occupancy["value"] == "LIBERO"
    assert occupancy["evidence_supports_claim"] is False
    assert occupancy["wording_instruction"] == "needs_cautious_wording"
    assert "stato_occupativo" not in fact_pack["lots"][0]
    assert fact_pack["lots"][0]["stato_occupativo_reported_value"] == "LIBERO"


def test_fact_pack_includes_money_total_note_and_component_hint():
    fact_pack = narrator.build_clean_customer_decision_fact_pack(_component_total_result())

    money_box = fact_pack["money_box"]
    assert "componenti del totale" in money_box["total_extra_costs"]["note"]
    assert money_box["money_interpretation"]["has_non_additive_items"] is True
    assert money_box["money_interpretation"]["has_components_of_total"] is True
    assert SAMPLE_TOTAL_EUR in money_box["money_interpretation"]["total_amounts_eur"]
    assert SAMPLE_COMPONENT_EUR in money_box["money_interpretation"]["component_amounts_eur"]


def test_deterministic_cost_guidance_uses_safe_buyer_exposure_wording():
    result = {
        "field_states": {
            "spese_condominiali_arretrate": {
                "value": "DA VERIFICARE",
                "status": "LOW_CONFIDENCE",
                "evidence": [{"page": 10, "quote": "Spese condominiali insolute da verificare."}],
            }
        },
        "verifier_runtime": {
            "canonical_case": {
                "priority": {},
                "grouped_llm_explanations": [],
            }
        },
    }

    apply_customer_decision_contract(result)
    serialized = json.dumps(
        {
            "issues": result.get("issues"),
            "red_flags_operativi": result.get("red_flags_operativi"),
            "section_2_decisione_rapida": result.get("section_2_decisione_rapida"),
            "customer_decision_contract": result.get("customer_decision_contract"),
        },
        ensure_ascii=False,
    ).lower()

    assert "costo a carico dell'acquirente" not in serialized
    assert "verificare l'eventuale incidenza economica per l'acquirente prima dell'offerta" in serialized


@pytest.mark.anyio
async def test_anchored_regolarizzazione_signal_can_be_mentioned(monkeypatch):
    result = _base_result()
    result["money_box"] = {
        "policy": "CONSERVATIVE",
        "items": [
            {
                "label_it": "Oneri di regolarizzazione urbanistica: € 2.500",
                "amount_eur": 2500,
                "classification": "cost_signal_to_verify",
                "additive_to_extra_total": False,
                "evidence": [{"page": 11, "quote": "Oneri di regolarizzazione urbanistica 2500,00 €"}],
            }
        ],
        "cost_signals_to_verify": [
            {
                "label_it": "Oneri di regolarizzazione urbanistica: € 2.500",
                "amount_eur": 2500,
                "classification": "cost_signal_to_verify",
                "evidence": [{"page": 11, "quote": "Oneri di regolarizzazione urbanistica 2500,00 €"}],
            }
        ],
    }
    result["customer_decision_contract"]["money_box"] = copy.deepcopy(result["money_box"])

    payload, meta = await _run_gemini_with_payload(
        monkeypatch,
        result,
        _valid_payload(
            decisione_rapida_it="Prima dell'offerta verifica occupazione, difformità e il segnale di regolarizzazione urbanistica da € 2.500 senza trattarlo come esborso automatico.",
            not_to_confuse_it="L'onere di regolarizzazione da € 2.500 è un segnale economico da verificare, non un costo automatico.",
        ),
    )

    assert meta["status"] == "OK"
    assert payload["decisione_rapida_it"]


def test_fact_pack_uses_repaired_money_not_stale_runtime_amounts():
    result = _base_result()
    result["money_box"] = {
        "policy": "CONSERVATIVE",
        "items": [{"label_it": "Oneri di regolarizzazione urbanistica: € 2.500", "amount_eur": 2500}],
    }
    result["customer_decision_contract"]["money_box"] = copy.deepcopy(result["money_box"])
    result["verifier_runtime"] = {"stale_money": [{"label": "stale € 6"}, {"label": "stale € 31"}]}

    fact_pack = narrator.build_clean_customer_decision_fact_pack(result)
    serialized = json.dumps(fact_pack, ensure_ascii=False)

    assert "€ 2.500" in serialized
    assert "stale € 6" not in serialized
    assert "stale € 31" not in serialized
    rejected = narrator.validate_gemini_decision_payload(_valid_payload(decisione_rapida_it="Verificare un costo di € 31 prima dell'offerta."), fact_pack)
    assert any("unsupported_euro_amount" in error for error in rejected)


@pytest.mark.anyio
async def test_case_aware_fallback_does_not_overwrite_gemini_success(monkeypatch):
    result = _base_result()
    result["lots_count"] = 2
    result["is_multi_lot"] = True
    result["lots"].append({"lot_number": "2", "tipologia": "Magazzino", "ubicazione": "Secondo indirizzo indicato"})

    async def fake_builder(**_kwargs):
        return _valid_payload(
            summary_it="La perizia riguarda due lotti con verifiche distinte.",
            decisione_rapida_it="Prima dell'offerta procedi lotto per lotto e separa occupazione, urbanistica e prezzo di ciascun bene.",
        ) | {"generation_mode": "gemini_clean_contract", "provider": "gemini", "model": "gemini-2.5-flash"}, {
            "enabled": True,
            "provider": "gemini",
            "model": "gemini-2.5-flash",
            "status": "OK",
            "errors": [],
            "error": None,
        }

    monkeypatch.setattr(server, "_decision_narrator_config", lambda: ("gemini", True, "gemini-2.5-flash", "test-key", 1))
    monkeypatch.setattr(server, "build_decisione_rapida_narration", fake_builder)

    await server._apply_post_qa_decision_narrator(result, request_id="test-request")

    assert result["decision_rapida_narrated"]["generation_mode"] == "gemini_clean_contract"
    assert "it" not in result["decision_rapida_narrated"]
    assert result["section_2_decisione_rapida"]["summary_it"] == result["decision_rapida_narrated"]["decisione_rapida_it"]


@pytest.mark.anyio
async def test_rejected_gemini_output_produces_separated_deterministic_fallback(monkeypatch):
    result = _base_result()

    async def fake_builder(**_kwargs):
        return None, {
            "enabled": True,
            "provider": "gemini",
            "model": "gemini-2.5-flash",
            "status": "REJECTED_VALIDATION",
            "error": "invalid:unsupported_buyer_cost_claim",
            "errors": ["invalid:unsupported_buyer_cost_claim"],
        }

    monkeypatch.setattr(server, "_decision_narrator_config", lambda: ("gemini", True, "gemini-2.5-flash", "test-key", 1))
    monkeypatch.setattr(server, "build_decisione_rapida_narration", fake_builder)

    await server._apply_post_qa_decision_narrator(result, request_id="test-request")

    factual = result["summary_for_client"]["summary_it"]
    decision = result["decision_rapida_client"]["decisione_rapida_it"]
    assert result["narrator_meta"]["status"] == "REJECTED_VALIDATION"
    assert result["narrator_meta"]["fallback_applied"] is True
    assert result["decision_rapida_narrated"]["generation_mode"] == "deterministic_separated_fallback"
    assert factual
    assert decision
    assert factual != decision
    assert result["decision_rapida_client"]["summary_it"] == decision
    assert result["section_2_decisione_rapida"]["summary_it"] == decision
    assert result["section_2_decisione_rapida"]["summary_it"] != factual
    assert result["customer_decision_contract"]["summary_for_client"]["summary_it"] == factual
    assert result["customer_decision_contract"]["decision_rapida_client"]["decisione_rapida_it"] == decision
    assert result["customer_decision_contract"]["section_2_decisione_rapida"]["summary_it"] == decision
    assert not narrator.scan_customer_facing_narrator_issues(result)


@pytest.mark.anyio
async def test_gemini_error_fallback_is_safe_and_does_not_break_upload(monkeypatch):
    result = _base_result()

    async def fake_builder(**_kwargs):
        return None, {
            "enabled": True,
            "provider": "gemini",
            "model": "gemini-2.5-flash",
            "status": "ERROR",
            "error": "boom",
            "errors": ["boom"],
        }

    monkeypatch.setattr(server, "_decision_narrator_config", lambda: ("gemini", True, "gemini-2.5-flash", "test-key", 1))
    monkeypatch.setattr(server, "build_decisione_rapida_narration", fake_builder)

    await server._apply_post_qa_decision_narrator(result, request_id="test-request")

    serialized = _serialized_customer_projection(result).lower()
    assert result["narrator_meta"]["status"] == "ERROR"
    assert result["decision_rapida_client"]["decisione_rapida_it"]
    assert result["summary_for_client"]["summary_it"] != result["decision_rapida_client"]["decisione_rapida_it"]
    assert "esborso effettivo" not in serialized
    assert "deve pagare" not in serialized
    assert "costo certo" not in serialized
    assert "a carico dell'acquirente" not in serialized


@pytest.mark.anyio
async def test_final_fallback_sync_scrubs_stale_regolarizzazione_money_labels(monkeypatch):
    result = _base_result()
    stale_item_31 = {
        "label_it": "Regolarizzazione: € 31",
        "label_en": "Regolarizzazione: € 31",
        "title": "Regolarizzazione: € 31",
        "headline_it": "Regolarizzazione: € 31",
        "amount_eur": 5032,
    }
    stale_item_6 = {
        "label_it": "Regolarizzazione: € 6",
        "label_en": "Regolarizzazione: € 6",
        "title": "Regolarizzazione urbanistica: € 6",
        "headline_it": "Regolarizzazione: € 6",
        "amount_eur": 3000,
    }
    money_box = {
        "policy": "CANONICAL_RUNTIME",
        "items": [copy.deepcopy(stale_item_31)],
        "cost_signals_to_verify": [copy.deepcopy(stale_item_6)],
        "qualitative_burdens": [copy.deepcopy(stale_item_31)],
    }
    result["money_box"] = copy.deepcopy(money_box)
    result["section_3_money_box"] = copy.deepcopy(money_box)
    result["summary_for_client_bundle"] = {"decision_summary_it": "Controllare Regolarizzazione: € 31 prima dell'offerta."}
    result["section_2_decisione_rapida"] = {"summary_it": "Controllare Regolarizzazione: € 6 prima dell'offerta."}
    result["decision_rapida_client"] = {"summary_it": "Controllare Regolarizzazione: € 31 prima dell'offerta."}
    result["decision_rapida_narrated"] = {"summary_it": "Regolarizzazione: € 6", "decisione_rapida_it": "Regolarizzazione: € 31"}
    result["customer_decision_contract"]["money_box"] = copy.deepcopy(money_box)
    result["customer_decision_contract"]["section_3_money_box"] = copy.deepcopy(money_box)
    result["customer_decision_contract"]["summary_for_client_bundle"] = copy.deepcopy(result["summary_for_client_bundle"])
    result["customer_decision_contract"]["section_2_decisione_rapida"] = copy.deepcopy(result["section_2_decisione_rapida"])
    result["customer_decision_contract"]["decision_rapida_client"] = copy.deepcopy(result["decision_rapida_client"])
    result["customer_decision_contract"]["decision_rapida_narrated"] = copy.deepcopy(result["decision_rapida_narrated"])

    async def fake_builder(**_kwargs):
        return None, {
            "enabled": True,
            "provider": "gemini",
            "model": "gemini-2.5-flash",
            "status": "REJECTED_VALIDATION",
            "error": "invalid:unsupported_buyer_cost_claim",
            "errors": ["invalid:unsupported_buyer_cost_claim"],
        }

    monkeypatch.setattr(server, "_decision_narrator_config", lambda: ("gemini", True, "gemini-2.5-flash", "test-key", 1))
    monkeypatch.setattr(server, "build_decisione_rapida_narration", fake_builder)

    await server._apply_post_qa_decision_narrator(result, request_id="test-request")

    serialized = _serialized_customer_projection(result)
    assert "Regolarizzazione: € 31" not in serialized
    assert "Regolarizzazione: €31" not in serialized
    assert "Regolarizzazione: € 6" not in serialized
    assert "Regolarizzazione: €6" not in serialized
    assert not narrator.scan_customer_facing_narrator_issues(result)
