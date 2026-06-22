"""Constrained Gemini narrator: input contract, output validators, and a
non-duplicative top-priority-aware deterministic fallback.

Covers the patch-scope additions in narrator.py / narration_rejection_log.py.
"""
import copy
import json
import sys
from io import BytesIO
from pathlib import Path

import pytest
from PyPDF2 import PdfReader

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import narrator
import pdf_report
import server
from customer_decision_contract import sanitize_customer_facing_result
from narration_rejection_log import pop_rejected_narration_data, store_rejected_narration_artifact


@pytest.fixture()
def anyio_backend():
    return "asyncio"


def _severe_urbanistic_result(money_box=None, issues=None, field_states=None):
    result = {
        "lots_count": 1,
        "is_multi_lot": False,
        "lots": [
            {"lot_number": "1", "tipologia": "Appartamento", "ubicazione": "Indirizzo indicato in perizia", "prezzo_base_eur": 100000}
        ],
        "field_states": field_states or {"regolarita_urbanistica": {"value": "NON CONFORME / GRAVE", "status": "FOUND"}},
        "issues": issues
        or [
            {
                "issue_id": "urbanistica_customer_priority",
                "family": "urbanistica",
                "severity": "BLOCKER",
                "classification": "blocker",
                "headline_it": "Urbanistica: non conformità grave / commerciabilità limitata",
                "action_it": "Verificare sanatoria non rilasciata e commerciabilità.",
                "evidence": [{"page": 7, "quote": "non conforme grave; sanatoria non rilasciata; non commerciabile fuori dalla vendita forzata"}],
            },
            {"family": "agibilita", "severity": "RED", "headline_it": "Agibilità assente"},
            {"family": "occupancy", "severity": "RED", "headline_it": "Immobile occupato"},
        ],
        "semaforo_generale": {
            "status": "RED",
            "reason_it": "Urbanistica",
            "top_blockers": [{"issue_id": "urbanistica_customer_priority", "label_it": "Urbanistica: non conformità grave / commerciabilità limitata"}],
        },
        "money_box": money_box or {"policy": "CONSERVATIVE", "items": [], "cost_signals_to_verify": []},
        "summary_for_client": {},
        "summary_for_client_bundle": {},
    }
    result["customer_decision_contract"] = {
        "issues": copy.deepcopy(result["issues"]),
        "semaforo_generale": copy.deepcopy(result["semaforo_generale"]),
        "money_box": copy.deepcopy(result["money_box"]),
        "field_states": copy.deepcopy(result["field_states"]),
    }
    return result


def _good_payload(**overrides):
    payload = {
        "executive_summary_it": "La perizia segnala una non conformità urbanistica grave con sanatoria non rilasciata e possibile limitazione alla commerciabilità del bene.",
        "decision_focus_it": "Prima dell'offerta chiarisci con tecnico e legale la non conformità grave e la commerciabilità, prima di guardare alle altre voci.",
        "top_reason_to_pause_it": "La non conformità urbanistica grave incide sulla commerciabilità e sulla rivendita.",
        "what_to_verify_before_offer_it": [
            "Verificare titoli edilizi e sanabilità della difformità.",
            "Chiarire la commerciabilità del bene con un legale.",
            "Verificare agibilità e stato di occupazione.",
        ],
        "what_is_not_extra_cost_it": ["Deprezzamenti e formalità non sono costi automatici per chi acquista."],
        "confidence_note_it": "Restano da verificare i titoli edilizi e la commerciabilità effettiva.",
    }
    payload.update(overrides)
    return payload


# 1 -------------------------------------------------------------------------
def test_gemini_input_excludes_raw_noisy_money_candidates():
    money_box = {
        "policy": "CONSERVATIVE",
        "items": [{"label_it": "Oneri di regolarizzazione: € 2.500", "amount_eur": 2500, "additive_to_extra_total": False}],
        "cost_signals_to_verify": [],
    }
    result = _severe_urbanistic_result(money_box=money_box)
    result["verifier_runtime"] = {"raw_money_candidates": [{"label": "RAW_NOISE_CANDIDATE 99999", "amount": 99999}]}
    fact_pack = narrator.build_clean_customer_decision_fact_pack(result)
    approved = fact_pack["approved"]
    serialized = json.dumps(fact_pack, ensure_ascii=False)
    assert "RAW_NOISE_CANDIDATE" not in serialized
    # No confirmed buyer obligation -> no buyer costs surfaced as facts.
    assert approved["approved_money_box"]["buyer_costs_confirmed"] == []
    # Non-additive valuation/regularization stays in the non-additive bucket, not as a cost.
    assert approved["approved_money_box"]["buyer_costs_to_verify"] == []
    assert any("regolarizzazione" in x.lower() for x in approved["approved_money_box"]["non_additive_valuation_references"])
    assert fact_pack["forbidden_claims"]["no_extra_buyer_cost_unless_additive_true"] is True


# 2 -------------------------------------------------------------------------
def test_gemini_cannot_create_buyer_cost_when_no_confirmed_costs():
    result = _severe_urbanistic_result()
    fact_pack = narrator.build_clean_customer_decision_fact_pack(result)
    assert fact_pack["approved"]["approved_money_box"]["buyer_costs_confirmed"] == []
    payload = _good_payload(
        decision_focus_it="Prima dell'offerta considera che la regolarizzazione è a carico dell'acquirente e che deve pagare gli oneri.",
    )
    errors = narrator.validate_gemini_decision_payload(payload, fact_pack)
    assert "invalid:unsupported_buyer_cost_claim" in " ".join(errors)


# 3 -------------------------------------------------------------------------
def test_gemini_cannot_treat_valuation_discount_as_buyer_cost():
    result = _severe_urbanistic_result()
    fact_pack = narrator.build_clean_customer_decision_fact_pack(result)
    payload = _good_payload(
        decision_focus_it="Prima dell'offerta ricorda che il deprezzamento è un costo a carico dell'acquirente da pagare oltre al prezzo.",
    )
    errors = narrator.validate_gemini_decision_payload(payload, fact_pack)
    assert "invalid:valuation_discount_as_buyer_cost" in " ".join(errors)


# 4 -------------------------------------------------------------------------
def test_gemini_cannot_treat_ipoteca_pignoramento_as_buyer_cost():
    result = _severe_urbanistic_result()
    fact_pack = narrator.build_clean_customer_decision_fact_pack(result)
    payload = _good_payload(
        decision_focus_it="Prima dell'offerta considera che l'ipoteca e il pignoramento sono a carico dell'acquirente, che deve pagare la cancellazione.",
    )
    errors = narrator.validate_gemini_decision_payload(payload, fact_pack)
    assert "invalid:formality_as_buyer_cost" in " ".join(errors)

    safe = _good_payload(
        decision_focus_it="Ipoteca e pignoramento sono formalità a carico della procedura, cancellabili con il decreto di trasferimento, non costi dell'acquirente.",
    )
    safe_errors = narrator.validate_gemini_decision_payload(safe, fact_pack)
    assert "invalid:formality_as_buyer_cost" not in " ".join(safe_errors)


# 5 -------------------------------------------------------------------------
def test_gemini_must_mention_top_blocker_when_one_exists():
    result = _severe_urbanistic_result()
    fact_pack = narrator.build_clean_customer_decision_fact_pack(result)
    omits = _good_payload(
        executive_summary_it="La perizia riguarda un appartamento con agibilità assente e immobile occupato da chiarire.",
        decision_focus_it="Prima dell'offerta verifica agibilità, occupazione e tempi di liberazione con tecnico e delegato.",
        top_reason_to_pause_it="Agibilità e occupazione da chiudere.",
    )
    errors = narrator.validate_gemini_decision_payload(omits, fact_pack)
    assert "invalid:omits_top_blocker" in " ".join(errors)
    # The good payload mentions the urbanistic blocker and passes.
    assert narrator.validate_gemini_decision_payload(_good_payload(), fact_pack) == []


# 6 -------------------------------------------------------------------------
def test_gemini_must_not_soften_severe_urbanistic_into_generic_da_verificare():
    result = _severe_urbanistic_result()
    fact_pack = narrator.build_clean_customer_decision_fact_pack(result)
    softened = _good_payload(
        executive_summary_it="La perizia riguarda un appartamento; la regolarità urbanistica è semplicemente da verificare insieme ad agibilità e occupazione.",
        decision_focus_it="Prima dell'offerta verifica la regolarità urbanistica da approfondire e l'occupazione del bene.",
        top_reason_to_pause_it="Regolarità urbanistica da verificare.",
    )
    errors = narrator.validate_gemini_decision_payload(softened, fact_pack)
    assert "invalid:softened_severe_urbanistic" in " ".join(errors)
    # Carrying the real severity is accepted.
    assert narrator.validate_gemini_decision_payload(_good_payload(), fact_pack) == []


# 7 -------------------------------------------------------------------------
@pytest.mark.anyio
async def test_rejected_constrained_gemini_payload_is_logged_internally(monkeypatch, tmp_path):
    async def fake_call(**_kwargs):
        return json.dumps(
            _good_payload(
                decision_focus_it="L'ipoteca è a carico dell'acquirente e deve pagare REJECTED_CONSTRAINT_MARKER.",
            )
        )

    monkeypatch.setattr(narrator, "_call_gemini_narrator_llm", fake_call)
    payload, meta = await narrator.build_decisione_rapida_narration(
        result=_severe_urbanistic_result(),
        request_id="req_constraint",
        enabled=True,
        provider="gemini",
        model="gemini-test",
        api_key="test-key",
    )
    assert payload is None
    assert meta["status"] == "REJECTED_VALIDATION"
    assert "formality_as_buyer_cost" in " ".join(meta["errors"])

    rejected = pop_rejected_narration_data(meta)
    path = store_rejected_narration_artifact(
        analysis_id="analysis_constraint",
        case_id="case_constraint",
        run_id="run_constraint",
        provider="gemini",
        model="gemini-test",
        narrator_meta=meta,
        rejected_data=rejected,
        artifact_root=tmp_path,
    )
    artifact = json.loads(path.read_text(encoding="utf-8"))
    assert "REJECTED_CONSTRAINT_MARKER" in json.dumps(artifact["rejected_payload"], ensure_ascii=False)
    assert artifact["fallback_applied"] is True


# 8 -------------------------------------------------------------------------
def test_rejected_constrained_payload_not_exposed_in_api_html_pdf():
    result = _severe_urbanistic_result()
    sanitize_customer_facing_result(result)
    result["narrator_meta"] = {
        "status": "REJECTED_VALIDATION",
        "provider": "gemini",
        "model": "gemini-test",
        "rejected_payload": {"decision_focus_it": "REJECTED_CONSTRAINT_MARKER a carico dell'acquirente"},
        "validation_error": "invalid:formality_as_buyer_cost",
    }
    analysis = {"analysis_id": "analysis_constraint", "result": result}
    api = server._sanitize_perizia_detail_response(analysis)
    html = server.generate_report_html(analysis, api["result"])
    pdf = pdf_report.build_perizia_pdf_bytes(analysis, api["result"])
    pdf_text = " ".join((page.extract_text() or "") for page in PdfReader(BytesIO(pdf)).pages)
    assert "REJECTED_CONSTRAINT_MARKER" not in json.dumps(api, ensure_ascii=False)
    assert "REJECTED_CONSTRAINT_MARKER" not in html
    assert "REJECTED_CONSTRAINT_MARKER" not in pdf_text


# 9 -------------------------------------------------------------------------
def test_deterministic_fallback_is_non_duplicative_and_top_priority_aware():
    result = _severe_urbanistic_result()
    fact_pack = narrator.build_clean_customer_decision_fact_pack(result)
    fallback = narrator.build_deterministic_separated_fallback_payload(result)
    summary = fallback["summary_it"]
    decision = fallback["decisione_rapida_it"]
    # Leads with the highest-priority issue (urbanistic/commerciability), not agibilità/occupazione.
    assert "urbanistic" in summary.lower() or "commerciabil" in summary.lower()
    assert "criticità principale" in summary.lower()
    assert "urbanistic" in fallback["main_risk_it"].lower() or "commerciabil" in fallback["main_risk_it"].lower()
    # Non-duplicative: not a mechanical restatement of the card titles.
    assert summary != decision
    assert not narrator._is_duplicative_of_cards(fallback, fact_pack)
    # 3-5 verification actions, urbanistic-first.
    assert 3 <= len(fallback["before_offer_it"]) <= 5
    assert "urbanistic" in fallback["before_offer_it"][0].lower()
    # Exposes the constrained output contract.
    for key in ("executive_summary_it", "decision_focus_it", "what_to_verify_before_offer_it", "what_is_not_extra_cost_it", "confidence_note_it"):
        assert fallback.get(key)


# 10 ------------------------------------------------------------------------
def test_frontend_decision_summary_does_not_become_identical_to_risk_cards():
    issues = [
        {"family": "urbanistica", "severity": "BLOCKER", "classification": "blocker", "headline_it": "Urbanistica: non conformità grave"},
        {"family": "agibilita", "severity": "RED", "headline_it": "Agibilità assente"},
        {"family": "occupancy", "severity": "RED", "headline_it": "Immobile occupato"},
    ]
    result = _severe_urbanistic_result(issues=issues)
    fallback = narrator.build_deterministic_separated_fallback_payload(result)
    narrator.apply_narrated_payload_to_result(result, fallback)
    decision_summary = result["summary_for_client_bundle"]["decision_summary_it"]
    joined_cards = "; ".join(i["headline_it"] for i in issues)
    assert not narrator._near_identical_text(decision_summary, joined_cards)
    # The applied summary keeps narrative wording beyond the bare card titles.
    assert "prima dell'offerta" in decision_summary.lower()
    assert not narrator.scan_customer_facing_narrator_issues(result)
