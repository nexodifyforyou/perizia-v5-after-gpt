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
from urbanistic_warning_priority import ISSUE_ID


def _result(text, *, quote=None, page=4):
    evidence = [{"page": page, "quote": quote or text}]
    return {
        "abusi_edilizi_conformita": {
            "conformita_urbanistica": {
                "status": "NON CONFORME / GRAVE" if "grave" in text.lower() else "PRESENTI DIFFORMITA",
                "explanation_it": text,
                "evidence": evidence,
            },
            "agibilita": {
                "status": "ASSENTE",
                "detail_it": "Agibilità assente",
                "evidence": [{"page": page + 1, "quote": "Non risulta rilasciato il certificato di agibilità."}],
            },
        },
        "issues": [
            {
                "issue_id": "agibilita_existing",
                "family": "agibilita",
                "severity": "RED",
                "headline_it": "Agibilità assente",
                "evidence": [{"page": page + 1, "quote": "Non risulta rilasciato il certificato di agibilità."}],
            },
            {
                "issue_id": "occupancy_existing",
                "family": "occupancy",
                "severity": "RED",
                "headline_it": "Immobile occupato da terzi",
                "evidence": [{"page": page + 2, "quote": "Occupato da terzi senza titolo"}],
            },
        ],
        "section_1_semaforo_generale": {
            "status": "RED",
            "reason_it": "Agibilità assente",
            "top_blockers": [
                {"issue_id": "agibilita_existing", "key": "agibilita", "label_it": "Agibilità assente", "severity": "RED"},
                {"issue_id": "occupancy_existing", "key": "occupancy", "label_it": "Immobile occupato", "severity": "RED"},
            ],
        },
        "section_2_decisione_rapida": {"summary_it": "Verificare agibilità e occupazione.", "issue_ids": ["agibilita_existing", "occupancy_existing"]},
        "section_9_legal_killers": {
            "items": [
                {"killer": "Agibilità assente", "status": "AMBER", "category": "agibilita", "action": "Verificare agibilità e titoli edilizi."},
                {"killer": "Occupazione da verificare", "status": "AMBER", "category": "occupancy"},
            ]
        },
        "section_11_red_flags": [],
        "red_flags_operativi": [],
        "customer_decision_contract": {},
        "summary_for_client": {"summary_it": "Sintesi preesistente."},
        "summary_for_client_bundle": {"decision_summary_it": "Sintesi preesistente."},
        "money_box": {"buyer_costs_confirmed": []},
        "section_3_money_box": {"buyer_costs_confirmed": []},
    }


def _sanitize(text, **kwargs):
    result = _result(text, **kwargs)
    sanitize_customer_facing_result(result)
    return result


def _top(result):
    return result["section_9_legal_killers"]["items"][0]


def _flatten(value):
    return json.dumps(value, ensure_ascii=False)


@pytest.mark.parametrize(
    "text",
    [
        "La perizia indica che l'immobile non è commerciabile.",
        "L'immobile non è commerciabile al di fuori di vendita forzata.",
        "Il bene è non liberamente commerciabile.",
        "La difformità è insanabile e non regolarizzabile.",
    ],
)
def test_critical_commerciability_signals_create_top_critical_issue(text):
    result = _sanitize(text)
    assert _top(result)["classification"] in {"blocker", "critical_blocker"}
    assert _top(result)["badge_it"] == "Blocco critico"
    assert result["section_1_semaforo_generale"]["top_blockers"][0]["issue_id"] == ISSUE_ID
    assert result["issues"][0]["issue_id"] == ISSUE_ID


@pytest.mark.parametrize(
    "text",
    [
        "La concessione in sanatoria non rilasciata costituisce una criticità.",
        "È presente un abuso edilizio.",
        "L'esito è NON CONFORME / GRAVE.",
        "La domanda di condono è ancora aperta.",
        "È prevista la fiscalizzazione dell'abuso.",
    ],
)
def test_severe_urbanistic_signals_create_rischio_grave(text):
    result = _sanitize(text)
    assert _top(result)["classification"] == "severe_risk_to_verify"
    assert _top(result)["badge_it"] == "Rischio grave"
    assert _top(result)["status"] == "RED"
    assert "GIALLO" not in _flatten(_top(result))


def test_use_destination_mismatch_is_surfaced_in_customer_wording():
    result = _sanitize("La destinazione legittimata è cantina/locale accessorio, non residenziale.")
    text = _flatten(result)
    assert "cantina o locale accessorio" in text
    assert "non residenziale" in text


def test_non_conforme_grave_is_not_softened_to_only_da_verificare():
    result = _sanitize("Regolarità urbanistica: NON CONFORME / GRAVE.")
    top = _top(result)
    assert top["badge_it"] == "Rischio grave"
    assert "non conformità grave" in top["killer"].lower()
    assert top.get("fact_status_it") == "Fatto dichiarato dal perito"


def test_priority_order_is_urbanistica_then_agibilita_then_occupation():
    result = _sanitize("È presente un abuso edilizio con sanatoria non rilasciata.")
    issues = result["issues"]
    assert issues[0]["family"] == "urbanistica"
    assert issues[1]["family"] == "agibilita"
    assert issues[2]["family"] == "occupancy"
    assert "Agibilità" in _flatten(result)


def test_multiple_urbanistic_signals_dedupe_to_one_card_with_evidence():
    result = _result("NON CONFORME / GRAVE; abuso edilizio; sanatoria non rilasciata; destinazione legittimata cantina.")
    result["issues"].append(
        {
            "family": "urbanistica",
            "headline_it": "Accertamento di conformità richiesto",
            "severity": "AMBER",
            "evidence": [{"page": 8, "quote": "Accertamento di conformità richiesto."}],
        }
    )
    sanitize_customer_facing_result(result)
    urban_cards = [item for item in result["section_9_legal_killers"]["items"] if item.get("category") == "urbanistica"]
    assert len(urban_cards) == 1
    assert not any(item.get("killer") == "Accertamento di conformità richiesto" for item in result["section_9_legal_killers"]["items"])
    assert len(urban_cards[0]["evidence_bullets"]) >= 2
    assert {4, 8}.issubset(set(urban_cards[0]["supporting_pages"]))


def test_projection_is_idempotent_without_self_generated_evidence_growth():
    result = _sanitize("Sanatoria non rilasciata e abuso edilizio.")
    first = copy.deepcopy(_top(result))
    sanitize_customer_facing_result(result)
    second = _top(result)
    assert second["classification"] == first["classification"]
    assert second["evidence"] == first["evidence"]
    assert second["evidence_bullets"] == first["evidence_bullets"]


def test_formalities_remain_facts_not_blockers():
    result = _sanitize("È presente un abuso edilizio.")
    result["section_9_legal_killers"]["items"].append(
        {
            "killer": "Ipoteca e pignoramento da cancellare con decreto di trasferimento",
            "evidence": [{"page": 12, "quote": "Ipoteca e pignoramento: formalità a carico della procedura."}],
        }
    )
    sanitize_customer_facing_result(result)
    formalities = [item for item in result["section_9_legal_killers"]["items"] if "ipoteca" in item.get("killer", "").lower()]
    assert formalities
    assert formalities[0]["classification"] == "fact"
    assert formalities[0]["is_blocker"] is False


def test_valuation_deduction_remains_non_additive_and_not_buyer_cost():
    result = _result("È presente un abuso edilizio.")
    deduction = {"label_it": "Deprezzamento estimativo 30%", "amount": 30000, "note": "Non additivo"}
    result["money_box"] = {"valuation_deductions": [copy.deepcopy(deduction)], "buyer_costs_confirmed": []}
    result["section_3_money_box"] = copy.deepcopy(result["money_box"])
    sanitize_customer_facing_result(result)
    assert result["money_box"]["valuation_deductions"] == [deduction]
    assert result["money_box"]["buyer_costs_confirmed"] == []


def test_html_pdf_and_api_surfaces_contain_promoted_warning():
    result = _sanitize("Sanatoria non rilasciata e abuso edilizio.")
    analysis = {"analysis_id": "analysis_synthetic", "case_id": "case_synthetic", "file_name": "synthetic.pdf", "result": result}
    api = server._sanitize_perizia_detail_response(analysis)
    html = server.generate_report_html(analysis, api["result"])
    pdf = pdf_report.build_perizia_pdf_bytes(analysis, api["result"])
    pdf_text = " ".join((page.extract_text() or "") for page in PdfReader(BytesIO(pdf)).pages)
    assert "Rischio grave" in _flatten(api)
    assert "non conformità grave" in html.lower()
    assert "non conformità grave" in pdf_text.lower()
    assert "LEGAL KILLERS" not in html.upper()
    assert "LEGAL KILLERS" not in pdf_text.upper()


def test_no_false_blocker_when_only_agibilita_absent():
    result = _result("La regolarizzazione è da verificare.")
    result["abusi_edilizi_conformita"].pop("conformita_urbanistica")
    sanitize_customer_facing_result(result)
    assert not any(item.get("issue_id") == ISSUE_ID for item in result.get("issues", []))
    assert "Agibilità" in _flatten(result)


def test_ripristino_cost_mention_alone_is_not_promoted_but_only_outcome_is_severe():
    cost_only = _result("Oneri di ripristino stimati al cinque per cento.")
    cost_only["abusi_edilizi_conformita"].pop("agibilita")
    sanitize_customer_facing_result(cost_only)
    assert not any(item.get("issue_id") == ISSUE_ID for item in cost_only.get("issues", []))
    severe = _sanitize("L'unico esito possibile è il ripristino obbligatorio dello stato legittimo.")
    assert _top(severe)["badge_it"] == "Rischio grave"


@pytest.mark.anyio
async def test_rejected_gemini_payload_is_captured_for_internal_storage(monkeypatch):
    async def fake_call(**_kwargs):
        return json.dumps({"summary_it": "REJECTED_SECRET_MARKER"})

    monkeypatch.setattr(narrator, "_call_gemini_narrator_llm", fake_call)
    payload, meta = await narrator.build_decisione_rapida_narration(
        result=_result("È presente un abuso edilizio."),
        request_id="req_synthetic",
        enabled=True,
        provider="gemini",
        model="gemini-test",
        api_key="test-key",
    )
    assert payload is None
    assert meta["status"] == "REJECTED_VALIDATION"
    assert meta["_rejected_payload"]["summary_it"] == "REJECTED_SECRET_MARKER"


def test_rejected_payload_artifact_is_internal_and_sanitized(tmp_path):
    meta = {
        "provider": "gemini",
        "model": "gemini-test",
        "status": "REJECTED_VALIDATION",
        "error": "missing_key:before_offer_it",
        "errors": ["missing_key:before_offer_it"],
        "_rejected_payload": {"summary_it": "REJECTED_SECRET_MARKER"},
    }
    rejected = pop_rejected_narration_data(meta)
    path = store_rejected_narration_artifact(
        analysis_id="analysis_synthetic",
        case_id="case_synthetic",
        run_id="run_synthetic",
        provider="gemini",
        model="gemini-test",
        narrator_meta=meta,
        rejected_data=rejected,
        artifact_root=tmp_path,
    )
    assert path == tmp_path / "analysis_synthetic" / "rejected_narration.json"
    artifact = json.loads(path.read_text(encoding="utf-8"))
    assert artifact["rejected_payload"]["summary_it"] == "REJECTED_SECRET_MARKER"
    assert artifact["fallback_applied"] is True
    assert artifact["final_fallback_generation_mode"] == "deterministic_separated_fallback"
    assert "_rejected_payload" not in meta

    result = _sanitize("È presente un abuso edilizio.")
    result["narrator_meta"] = {**meta, "rejected_payload": artifact["rejected_payload"]}
    analysis = {"analysis_id": "analysis_synthetic", "result": result}
    api = server._sanitize_perizia_detail_response(analysis)
    html = server.generate_report_html(analysis, api["result"])
    pdf = pdf_report.build_perizia_pdf_bytes(analysis, api["result"])
    pdf_text = " ".join((page.extract_text() or "") for page in PdfReader(BytesIO(pdf)).pages)
    assert "REJECTED_SECRET_MARKER" not in _flatten(api)
    assert "REJECTED_SECRET_MARKER" not in html
    assert "REJECTED_SECRET_MARKER" not in pdf_text


@pytest.mark.anyio
async def test_server_rejection_path_writes_artifact_and_removes_raw_from_result(monkeypatch, tmp_path):
    async def fake_build(**_kwargs):
        return None, {
            "enabled": True,
            "provider": "gemini",
            "model": "gemini-test",
            "status": "REJECTED_VALIDATION",
            "error": "invalid:unsupported_claim",
            "errors": ["invalid:unsupported_claim"],
            "_rejected_payload": {"summary_it": "REJECTED_INTEGRATION_MARKER"},
        }

    monkeypatch.setenv("PERIZIA_QA_RUNS_ROOT", str(tmp_path))
    monkeypatch.setattr(server, "build_decisione_rapida_narration", fake_build)
    monkeypatch.setattr(server, "_decision_narrator_config", lambda: ("gemini", True, "gemini-test", "test-key", 1.0))
    result = _result("È presente un abuso edilizio.")
    meta = await server._apply_post_qa_decision_narrator(
        result,
        request_id="req_synthetic",
        analysis_id="analysis_synthetic",
        case_id="case_synthetic",
        run_id="run_synthetic",
    )
    artifact = json.loads((tmp_path / "analysis_synthetic" / "rejected_narration.json").read_text(encoding="utf-8"))
    assert artifact["rejected_payload"]["summary_it"] == "REJECTED_INTEGRATION_MARKER"
    assert meta["fallback_applied"] is True
    assert "REJECTED_INTEGRATION_MARKER" not in _flatten(result)
    assert "_rejected_payload" not in _flatten(result)


def test_no_internal_leak_tokens_in_promoted_customer_result():
    result = _sanitize("NON CONFORME / GRAVE con sanatoria non rilasciata.")
    result["qa_gate"] = {"note_it": "LEGAL KILLERS troppo generici"}
    text = _flatten(server._sanitize_perizia_detail_response({"analysis_id": "analysis_synthetic", "result": result}))
    for token in ("source_paths", "signal_groups", "validation_error", "rejected_payload", "rejected_text", "LEGAL KILLERS"):
        assert token not in text


def test_qa_gate_critique_never_becomes_customer_evidence_or_survives_sanitization():
    # qa_gate carries internal QA critique; it may inform severity but its
    # critique text must never reach customer-facing output. Only qa_gate.status
    # (the established customer contract) is preserved.
    result = _result("Regolarità urbanistica: NON CONFORME / GRAVE.")
    result["qa_gate"] = {
        "status": "WARN",
        "section_verdicts": {
            "urbanistica": {"note_it": "Classificazione troppo morbida: NON CONFORME / GRAVE, immobile non regolare."}
        },
        "contradictions_detected": [
            {"recommended_action": "Alzare severità a NON CONFORME / GRAVE e sostituire l'evidenza con le frasi tecniche."}
        ],
    }
    result["customer_decision_contract"]["qa_gate"] = copy.deepcopy(result["qa_gate"])
    sanitized = server._sanitize_perizia_detail_response({"analysis_id": "analysis_synthetic", "result": result})
    text = _flatten(sanitized)

    # The urbanistic promotion still fires (qa_gate informed the severity decision).
    promoted = [i for i in sanitized["result"]["issues"] if i.get("issue_id") == ISSUE_ID]
    assert promoted, "urbanistic promotion must still occur"

    # No internal QA critique survives anywhere in the customer payload.
    for token in (
        "qa_gate",
        "section_verdicts",
        "contradictions_detected",
        "Alzare severità",
        "classificazione troppo morbida",
        "troppo morbida",
        "sostituire l'evidenza",
    ):
        assert token not in text, f"leaked internal token: {token}"
    # The bare internal recommended_action field (distinct from customer recommended_action_it) is gone.
    assert "recommended_action" not in text.replace("recommended_action_it", "")
    # qa_gate is moved to the internal runtime sidecar, never in customer output.
    assert "qa_gate" not in sanitized["result"]
    cdc = sanitized["result"].get("customer_decision_contract")
    if isinstance(cdc, dict):
        assert "qa_gate" not in cdc

    # evidence_bullets are document-backed, never critique wording.
    for issue in promoted:
        for bullet in issue.get("evidence_bullets") or []:
            assert "alzare severit" not in bullet.lower()
            assert "troppo morbida" not in bullet.lower()
            assert "sostituire" not in bullet.lower()
