import copy
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import server as server  # noqa: E402
from customer_decision_contract import sanitize_customer_facing_result, separate_internal_runtime_from_customer_result  # noqa: E402
from perizia_authority_money_projection import FEATURE_FLAG, apply_authority_money_projection_if_enabled  # noqa: E402
from perizia_authority_resolvers import build_authority_shadow_resolvers  # noqa: E402
from perizia_section_authority import build_section_authority_map  # noqa: E402
from test_admin import FakeDB  # noqa: E402


FORBIDDEN_KEYS = {
    "debug",
    "internal_runtime",
    "authority_money_projection",
    "authority_shadow_resolvers",
    "authority_score",
    "authority_level",
    "section_zone",
    "domain_hints",
    "answer_point",
    "reason_for_authority",
    "is_instruction_like",
    "is_answer_like",
    "source_stage",
    "extractor_version",
    "removed_paths",
    "shadow_money",
}


@pytest.fixture()
def anyio_backend():
    return "asyncio"


@pytest.fixture()
def fake_db(monkeypatch):
    fake = FakeDB()
    monkeypatch.setattr(server, "db", fake)
    return fake


def _pages(*texts: str) -> List[Dict[str, Any]]:
    return [{"page_number": idx, "text": text} for idx, text in enumerate(texts, start=1)]


def _shadow(*texts: str) -> Dict[str, Any]:
    pages = _pages(*texts)
    section_map = build_section_authority_map(pages)
    return build_authority_shadow_resolvers(pages, section_map)


def _result_with_money_box(items: List[Dict[str, Any]] | None = None) -> Dict[str, Any]:
    money_box = {
        "policy": "LEGACY",
        "items": copy.deepcopy(items or []),
        "total_extra_costs": {
            "range": {"min": 37, "max": 37},
            "note": "Legacy total",
            "contract_state": "quantified_estimate",
        },
    }
    section3 = copy.deepcopy(money_box)
    section3["totale_extra_budget"] = {"min": 37, "max": 37, "nota": "Legacy total"}
    cdc = {
        "version": "customer_decision_contract_v1",
        "money_box": copy.deepcopy(money_box),
        "section_3_money_box": copy.deepcopy(section3),
    }
    return {
        "money_box": copy.deepcopy(money_box),
        "section_3_money_box": copy.deepcopy(section3),
        "customer_decision_contract": cdc,
        "field_states": {"stato_occupativo": {"status": "UNKNOWN"}},
        "summary_for_client": {"summary_it": "unchanged"},
        "decision_rapida_client": {"summary_it": "unchanged"},
        "section_9_legal_killers": {"items": [{"title_it": "unchanged"}]},
    }


def _text_blob(value: Any) -> str:
    if isinstance(value, dict):
        return " ".join(_text_blob(child) for child in value.values())
    if isinstance(value, list):
        return " ".join(_text_blob(item) for item in value)
    return str(value or "")


def _leak_paths(value: Any, path: str = "response") -> List[str]:
    hits: List[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            key_text = str(key)
            child_path = f"{path}.{key_text}"
            if key_text in FORBIDDEN_KEYS or key_text.startswith("authority_") or "shadow_" in key_text:
                hits.append(child_path)
            hits.extend(_leak_paths(child, child_path))
    elif isinstance(value, list):
        for idx, item in enumerate(value):
            hits.extend(_leak_paths(item, f"{path}[{idx}]"))
    return hits


def _assert_customer_clean(result: Dict[str, Any]) -> None:
    cloned = copy.deepcopy(result)
    sanitize_customer_facing_result(cloned)
    separate_internal_runtime_from_customer_result(cloned)
    assert _leak_paths(cloned) == []


def test_money_projection_flag_off_is_identical(monkeypatch):
    monkeypatch.delenv(FEATURE_FLAG, raising=False)
    result = _result_with_money_box([{"label_it": "Regolarizzazione: € 31", "stima_euro": 31}])
    before = copy.deepcopy(result)

    meta = apply_authority_money_projection_if_enabled(
        result,
        authority_shadow=_shadow("STIMA / FORMAZIONE LOTTI\nSpese tecniche di regolarizzazione Euro 5.032,00."),
    )

    assert meta["status"] == "DISABLED"
    assert result == before


def test_money_projection_scrubs_via_umbria_stale_31_6_and_surfaces_verify_signals(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _result_with_money_box(
        [
            {"label_it": "Regolarizzazione: € 31", "stima_euro": 31},
            {"label_it": "Regolarizzazione: € 6", "stima_euro": 6},
        ]
    )
    shadow = _shadow(
        "STIMA / FORMAZIONE LOTTI\n"
        "Spese tecniche di regolarizzazione Euro 5.032,00 a carico dell'aggiudicatario.\n"
        "Sanzione per sanatoria Euro 3.000,00 a carico dell'acquirente."
    )

    meta = apply_authority_money_projection_if_enabled(result, authority_shadow=shadow, analysis_id="analysis_85b6655bedd2")

    assert meta["status"] == "APPLIED"
    assert meta["stale_money_removed"] is True
    blob = _text_blob(result)
    assert "Regolarizzazione: € 31" not in blob
    assert "Regolarizzazione: € 6" not in blob
    cdc_box = result["customer_decision_contract"]["money_box"]
    signal_amounts = {item.get("stima_euro") for item in cdc_box["cost_signals_to_verify"]}
    assert {5032, 3000}.issubset(signal_amounts)
    assert cdc_box["total_extra_costs"]["min"] is None
    assert cdc_box["total_extra_costs"]["max"] is None
    assert all(item["customer_visible_amount_status"] == "to_verify" for item in cdc_box["cost_signals_to_verify"])


def test_money_projection_removes_stale_qa_gate_money_claim_from_customer_response(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _result_with_money_box(
        [
            {"label_it": "Regolarizzazione: € 31", "stima_euro": 31},
            {"label_it": "Regolarizzazione: € 6", "stima_euro": 6},
        ]
    )
    stale_claim = "Totale stimato in perizia: € 30535; Regolarizzazione: € 31; Regolarizzazione: € 6."
    result["qa_gate"] = {
        "contradictions_detected": [
            {"id": "money_stale", "current_wrong_claim": stale_claim},
            {"id": "occupancy", "current_wrong_claim": "Occupazione: LIBERO, ma lo schema dice occupato."},
        ]
    }
    result["customer_decision_contract"]["qa_gate"] = {
        "contradictions_detected": [{"id": "cdc_money_stale", "current_wrong_claim": stale_claim}]
    }
    shadow = _shadow(
        "STIMA / FORMAZIONE LOTTI\n"
        "Spese tecniche di regolarizzazione Euro 5.032,00 a carico dell'aggiudicatario.\n"
        "Sanzione per sanatoria Euro 3.000,00 a carico dell'acquirente."
    )

    meta = apply_authority_money_projection_if_enabled(result, authority_shadow=shadow)

    assert meta["status"] == "APPLIED"
    assert meta["removed_money_qa_claims_count"] == 2
    assert "stale_money_qa_claims_removed" in meta["notes"]
    response_blob = _text_blob(result)
    assert "Regolarizzazione: € 31" not in response_blob
    assert "Regolarizzazione: € 6" not in response_blob
    assert "Totale stimato in perizia" not in response_blob
    assert "Occupazione: LIBERO" in response_blob
    box = result["customer_decision_contract"]["money_box"]
    assert {5032, 3000}.issubset({item.get("stima_euro") for item in box["cost_signals_to_verify"]})

    clean = copy.deepcopy(result)
    sanitize_customer_facing_result(clean)
    separate_internal_runtime_from_customer_result(clean)
    assert "removed_paths" not in _text_blob(clean)
    assert _leak_paths(clean) == []


def test_money_projection_preserves_non_money_qa_contradictions(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _result_with_money_box([{"label_it": "Regolarizzazione: € 31", "stima_euro": 31}])
    result["qa_gate"] = {
        "contradictions_detected": [
            {"id": "lot", "current_wrong_claim": "Lotto unico indicato, ma il documento ha LOTTO 1 e LOTTO 2."},
            {"id": "occupancy", "message": "Occupazione da verificare con custode."},
        ]
    }

    meta = apply_authority_money_projection_if_enabled(
        result,
        authority_shadow=_shadow("STIMA / FORMAZIONE LOTTI\nSpese tecniche di regolarizzazione Euro 5.032,00."),
    )

    assert meta["status"] == "APPLIED"
    assert meta.get("removed_money_qa_claims_count", 0) == 0
    text = _text_blob(result["qa_gate"])
    assert "Lotto unico indicato" in text
    assert "Occupazione da verificare" in text


def test_money_projection_removes_generic_regolarizzazione_qa_claim(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _result_with_money_box([{"label_it": "Regolarizzazione: € 1234", "stima_euro": 1234}])
    result["qa_gate"] = {
        "warnings": [
            {"id": "generic_reg", "claim": "Regolarizzazione: € 1234 come costo certo per l'acquirente."}
        ]
    }

    meta = apply_authority_money_projection_if_enabled(
        result,
        authority_shadow=_shadow("STIMA / FORMAZIONE LOTTI\nSpese tecniche di regolarizzazione Euro 5.032,00."),
    )

    assert meta["status"] == "APPLIED"
    assert meta["removed_money_qa_claims_count"] == 1
    assert "Regolarizzazione: € 1234" not in _text_blob(result)


def test_money_projection_preserves_regolarizzazione_duration_as_non_money_claim(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _result_with_money_box([{"label_it": "Regolarizzazione: 6 mesi", "note_it": "durata pratica"}])
    result["qa_gate"] = {
        "warnings": [{"id": "reg_duration", "claim": "Regolarizzazione: 6 mesi per completare la pratica."}]
    }

    meta = apply_authority_money_projection_if_enabled(
        result,
        authority_shadow=_shadow("STIMA / FORMAZIONE LOTTI\nSpese tecniche di regolarizzazione Euro 5.032,00."),
    )

    assert meta["status"] == "APPLIED"
    assert meta.get("removed_money_qa_claims_count", 0) == 0
    assert "Regolarizzazione: 6 mesi" in _text_blob(result)


def test_money_projection_preserves_truncated_regolarizzazione_duration_context(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    duration_text = "Tempi necessari per la regolarizzazione: 6"
    result = _result_with_money_box([{"label_it": duration_text, "note_it": "durata pratica"}])
    result["qa_gate"] = {"warnings": [{"id": "reg_duration", "claim": duration_text}]}

    meta = apply_authority_money_projection_if_enabled(
        result,
        authority_shadow=_shadow("STIMA / FORMAZIONE LOTTI\nSpese tecniche di regolarizzazione Euro 5.032,00."),
    )

    assert meta["status"] == "APPLIED"
    assert meta.get("removed_money_qa_claims_count", 0) == 0
    assert duration_text in _text_blob(result)


@pytest.mark.parametrize(
    "claim",
    [
        "Formalita ipotecaria di € 200 come costo a carico dell'acquirente.",
        "Rendita catastale € 123 trattata come costo extra.",
        "Prezzo base d'asta € 80.000 e deprezzamento € 10.000 come costi buyer-side.",
    ],
)
def test_money_projection_removes_non_buyer_amount_qa_claims(monkeypatch, claim):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _result_with_money_box([{"label_it": claim, "stima_euro": 200}])
    result["qa_gate"] = {"contradictions_detected": [{"id": "non_buyer_cost", "current_wrong_claim": claim}]}
    shadow = _shadow(
        "STIMA / FORMAZIONE LOTTI\nRendita catastale Euro 123,00.",
        "STIMA / FORMAZIONE LOTTI\nPrezzo base d'asta Euro 80.000,00.",
        "STIMA / FORMAZIONE LOTTI\nDeprezzamento Euro 10.000,00.",
        "FORMALITA PREGIUDIZIEVOLI\nCancellazione ipoteca Euro 200,00.",
    )

    meta = apply_authority_money_projection_if_enabled(result, authority_shadow=shadow)

    assert meta["status"] == "APPLIED"
    assert meta["removed_money_qa_claims_count"] == 1
    assert claim not in _text_blob(result)


def test_money_projection_qa_sanitizer_flag_off_invariance(monkeypatch):
    monkeypatch.delenv(FEATURE_FLAG, raising=False)
    stale_claim = "Regolarizzazione: € 1234 come costo certo per l'acquirente."
    result = _result_with_money_box([{"label_it": "Regolarizzazione: € 1234", "stima_euro": 1234}])
    result["qa_gate"] = {"contradictions_detected": [{"current_wrong_claim": stale_claim}]}
    before = copy.deepcopy(result)

    meta = apply_authority_money_projection_if_enabled(
        result,
        authority_shadow=_shadow("STIMA / FORMAZIONE LOTTI\nSpese tecniche di regolarizzazione Euro 5.032,00."),
    )

    assert meta["status"] == "DISABLED"
    assert result == before
    assert stale_claim in _text_blob(result)


def test_non_buyer_amount_roles_do_not_become_extra_cost_items(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _result_with_money_box(
        [
            {"label_it": "Rendita catastale", "stima_euro": 123},
            {"label_it": "Prezzo base", "stima_euro": 80000},
            {"label_it": "Formalita", "stima_euro": 200},
        ]
    )
    shadow = _shadow(
        "STIMA / FORMAZIONE LOTTI\nRendita catastale Euro 123,00.",
        "STIMA / FORMAZIONE LOTTI\nPrezzo base d'asta Euro 80.000,00.",
        "STIMA / FORMAZIONE LOTTI\nValore di stima Euro 100.000,00.",
        "STIMA / FORMAZIONE LOTTI\nDeprezzamento Euro 10.000,00.",
        "FORMALITA PREGIUDIZIEVOLI\nCancellazione ipoteca Euro 200,00.",
    )

    meta = apply_authority_money_projection_if_enabled(result, authority_shadow=shadow)

    assert meta["status"] == "APPLIED"
    box = result["customer_decision_contract"]["money_box"]
    assert box["items"] == []
    assert box["cost_signals_to_verify"] == []
    assert box["total_extra_costs"]["min"] is None
    assert box["total_extra_costs"]["max"] is None
    cadastral_amounts = {item.get("amount_eur") for item in box["cadastral_values"]}
    price_amounts = {item.get("amount_eur") for item in box["price_references"]}
    valuation_dedution_amounts = {item.get("amount_eur") for item in box["valuation_deductions"]}
    formality_amounts = {item.get("amount_eur") for item in box["formalities_and_procedural_amounts"]}
    assert 123 in cadastral_amounts
    assert 80000 in price_amounts
    assert 100000 in price_amounts
    assert 10000 in valuation_dedution_amounts
    assert 200 in formality_amounts
    excluded_amounts = {item.get("amount_eur") for item in box["excluded_non_buyer_cost_amounts"]}
    assert {123, 80000, 100000, 10000, 200}.issubset(excluded_amounts)


def test_component_total_double_count_is_blocked(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _result_with_money_box(
        [
            {"label_it": "Spese tecniche", "stima_euro": 1000},
            {"label_it": "Sanzione", "stima_euro": 500},
            {"label_it": "Totale regolarizzazione", "stima_euro": 1500},
        ]
    )
    shadow = _shadow(
        "STIMA / FORMAZIONE LOTTI\n"
        "Regolarizzazione urbanistica: spese tecniche Euro 1.000,00; "
        "sanzione Euro 500,00; Totale regolarizzazione Euro 1.500,00 a carico dell'aggiudicatario."
    )

    meta = apply_authority_money_projection_if_enabled(result, authority_shadow=shadow)

    assert meta["status"] == "APPLIED"
    assert meta["component_total_double_count_prevented"] is True
    box = result["customer_decision_contract"]["money_box"]
    assert box["total_extra_costs"]["range"] == {"min": 1500, "max": 1500}
    blob = _text_blob(box["items"])
    assert "€ 1.500" in blob
    assert "€ 1.000" not in blob
    assert "€ 500" not in blob


def test_missing_or_corrupt_authority_fails_open_without_money_change(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _result_with_money_box([{"label_it": "Regolarizzazione: € 31", "stima_euro": 31}])
    before_money = copy.deepcopy(result["money_box"])
    before_cdc_money = copy.deepcopy(result["customer_decision_contract"]["money_box"])

    meta = apply_authority_money_projection_if_enabled(
        result,
        pages_raw=_pages("STIMA / FORMAZIONE LOTTI\nSpese Euro 1.000,00."),
        section_authority_map={"_authority_tagging_status": "corrupt_map"},
    )

    assert meta["status"] == "FAIL_OPEN"
    assert result["money_box"] == before_money
    assert result["customer_decision_contract"]["money_box"] == before_cdc_money


def test_customer_response_has_no_authority_money_leak(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _result_with_money_box([{"label_it": "Rendita catastale", "stima_euro": 123}])
    apply_authority_money_projection_if_enabled(
        result,
        authority_shadow=_shadow("STIMA / FORMAZIONE LOTTI\nRendita catastale Euro 123,00."),
    )

    _assert_customer_clean(result)


@pytest.mark.anyio
async def test_read_time_money_projection_is_response_only_and_mongo_unchanged(fake_db, monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    monkeypatch.delenv(server.AUTHORITY_LOT_PROJECTION_FLAG, raising=False)
    shadow = _shadow(
        "STIMA / FORMAZIONE LOTTI\n"
        "Spese tecniche di regolarizzazione Euro 5.032,00 a carico dell'aggiudicatario.\n"
        "Sanzione per sanatoria Euro 3.000,00 a carico dell'acquirente."
    )
    monkeypatch.setattr(server, "_load_authority_shadow_for_detail_read", lambda _analysis_id, _analysis: shadow)
    result = _result_with_money_box(
        [
            {"label_it": "Regolarizzazione: € 31", "stima_euro": 31},
            {"label_it": "Regolarizzazione: € 6", "stima_euro": 6},
        ]
    )
    result["qa_gate"] = {
        "contradictions_detected": [
            {
                "id": "stale_money_read_path",
                "current_wrong_claim": "Totale stimato in perizia: € 30535; Regolarizzazione: € 31; Regolarizzazione: € 6.",
            }
        ]
    }
    result.update(
        {
            "issues": [],
            "summary_for_client_bundle": {"semaforo_status": "GREEN"},
            "section_1_semaforo_generale": {"status": "GREEN"},
            "section_9_legal_killers": {"items": []},
            "section_11_red_flags": [],
        }
    )
    stored = {
        "analysis_id": "analysis_money_read",
        "user_id": "user_test",
        "case_id": "case_money_read",
        "case_title": "money-read.pdf",
        "file_name": "money-read.pdf",
        "created_at": datetime(2026, 5, 9, tzinfo=timezone.utc),
        "status": "COMPLETED",
        "pages_count": 5,
        "result": result,
    }
    fake_db.perizia_analyses.items.append(stored)
    before = copy.deepcopy(stored)

    response = await server._get_perizia_analysis_for_user(
        "analysis_money_read",
        server.User(user_id="user_test", email="user@test.local", name="Test User"),
    )

    assert fake_db.perizia_analyses.items[0] == before
    response_blob = _text_blob(response)
    assert "Regolarizzazione: € 31" not in response_blob
    assert "Regolarizzazione: € 6" not in response_blob
    box = response["result"]["customer_decision_contract"]["money_box"]
    assert {5032, 3000}.issubset({item.get("stima_euro") for item in box["cost_signals_to_verify"]})
    assert _leak_paths(response) == []


# ---------------------------------------------------------------------------
# Money Map redesign — tests 1-13 from brief
# ---------------------------------------------------------------------------


def _amounts_for(box: Dict[str, Any], key: str) -> set:
    return {item.get("amount_eur") for item in box.get(key, []) if isinstance(item, dict)}


def _money_map_apply(monkeypatch, *texts: str) -> Dict[str, Any]:
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _result_with_money_box([])
    shadow = _shadow(*texts)
    apply_authority_money_projection_if_enabled(result, authority_shadow=shadow)
    return result["customer_decision_contract"]["money_box"]


_CASA_LIKE_RICH_CONTEXT = (
    "STIMA / FORMAZIONE LOTTI\n"
    "Valore di stima Euro 500.000,00. Prezzo base d'asta Euro 482.799,00. "
    "Spese tecniche di regolarizzazione a carico dell'aggiudicatario Euro 1.500,00."
)


def test_money_map_valuation_arithmetic_is_not_buyer_cost(monkeypatch):
    box = _money_map_apply(
        monkeypatch,
        "STIMA / FORMAZIONE LOTTI\nmq 374,23 x €/mq 1.300,00 = € 486.499,00",
        _CASA_LIKE_RICH_CONTEXT,
    )
    assert 486499 not in _amounts_for(box, "cost_signals_to_verify")
    assert 486499 not in _amounts_for(box, "buyer_cost_signals_to_verify")
    val_refs = _amounts_for(box, "valuation_references")
    val_ref_amounts = _amounts_for(box, "valuation_reference_amounts")
    assert 486499 in (val_refs | val_ref_amounts)
    for item in box.get("valuation_references", []):
        if item.get("amount_eur") == 486499:
            assert item.get("page")
            assert item.get("evidence")
            assert item.get("explanation_it")


def test_money_map_decurtation_formula_is_not_buyer_cost(monkeypatch):
    box = _money_map_apply(
        monkeypatch,
        "STIMA / FORMAZIONE LOTTI\nIl valore viene decurtato delle spese di regolarizzazione catastali "
        "Euro 1.700,00 e regolarizzazione urbanistica Euro 2.000,00, totale Euro 3.700,00. "
        "486.499,00 - 3.700,00 = 482.799,00.",
        _CASA_LIKE_RICH_CONTEXT,
    )
    buyer_amounts = _amounts_for(box, "cost_signals_to_verify") | _amounts_for(box, "buyer_cost_signals_to_verify")
    confirmed_amounts = _amounts_for(box, "buyer_costs_confirmed")
    for amount in (2000, 1700, 3700, 486499, 482799):
        assert amount not in buyer_amounts, f"€{amount} should not be a buyer cost signal"
        assert amount not in confirmed_amounts, f"€{amount} should not be a confirmed buyer cost"
    surfaced = (
        _amounts_for(box, "excluded_non_buyer_cost_amounts")
        | _amounts_for(box, "valuation_deductions")
        | _amounts_for(box, "valuation_references")
        | _amounts_for(box, "other_monetary_mentions")
    )
    for amount in (2000, 1700, 3700):
        assert amount in surfaced, f"€{amount} must be visible in a non-buyer money group"


def test_money_map_regolarizzazione_alone_is_not_buyer_cost(monkeypatch):
    box = _money_map_apply(
        monkeypatch,
        "STIMA / FORMAZIONE LOTTI\nRegolarizzazione urbanistica Euro 2.000,00.",
    )
    buyer = _amounts_for(box, "cost_signals_to_verify") | _amounts_for(box, "buyer_cost_signals_to_verify")
    assert 2000 not in buyer
    surfaced = (
        _amounts_for(box, "other_monetary_mentions")
        | _amounts_for(box, "valuation_references")
        | _amounts_for(box, "unsupported_or_unknown_amounts")
    )
    assert 2000 in surfaced


def test_money_map_regolarizzazione_with_buyer_obligation_is_signal(monkeypatch):
    box = _money_map_apply(
        monkeypatch,
        "STIMA / FORMAZIONE LOTTI\nCosti di regolarizzazione a carico dell'aggiudicatario Euro 2.000,00.",
    )
    signals = box.get("buyer_cost_signals_to_verify", []) + box.get("cost_signals_to_verify", [])
    target = next((item for item in signals if item.get("amount_eur") == 2000), None)
    assert target is not None, "€2.000 should appear as buyer_cost_signal_to_verify"
    assert target.get("page")
    assert target.get("evidence")
    assert target.get("verification_note_it")


def test_money_map_rendita_prezzo_stima_never_buyer_cost(monkeypatch):
    box = _money_map_apply(
        monkeypatch,
        "STIMA / FORMAZIONE LOTTI\nRendita catastale Euro 123,00.",
        "STIMA / FORMAZIONE LOTTI\nPrezzo base d'asta Euro 80.000,00.",
        "STIMA / FORMAZIONE LOTTI\nValore di stima Euro 100.000,00.",
        "STIMA / FORMAZIONE LOTTI\nDeprezzamento Euro 10.000,00.",
    )
    buyer = _amounts_for(box, "cost_signals_to_verify") | _amounts_for(box, "buyer_cost_signals_to_verify") | _amounts_for(box, "buyer_costs_confirmed")
    for amount in (123, 80000, 100000, 10000):
        assert amount not in buyer


def test_money_map_formality_amounts_are_not_buyer_cost(monkeypatch):
    box = _money_map_apply(
        monkeypatch,
        "FORMALITA PREGIUDIZIEVOLI\nCancellazione ipoteca Euro 200,00.\nIscrizione ipotecaria Euro 500,00.",
    )
    buyer = _amounts_for(box, "cost_signals_to_verify") | _amounts_for(box, "buyer_cost_signals_to_verify") | _amounts_for(box, "buyer_costs_confirmed")
    assert 200 not in buyer
    assert 500 not in buyer
    formality_amounts = _amounts_for(box, "formalities_and_procedural_amounts")
    assert {200, 500}.issubset(formality_amounts)


def test_money_map_casa_ai_venti_regression(monkeypatch):
    box = _money_map_apply(
        monkeypatch,
        "STIMA / FORMAZIONE LOTTI\n"
        "mq 374,23 x €/mq 1.300,00 = € 486.499,00. "
        "Il valore viene decurtato: regolarizzazioni catastali Euro 1.700,00, "
        "regolarizzazioni urbanistiche Euro 2.000,00, totale Euro 3.700,00. "
        "486.499,00 - 3.700,00 = 482.799,00.",
        _CASA_LIKE_RICH_CONTEXT,
    )
    buyer = _amounts_for(box, "cost_signals_to_verify") | _amounts_for(box, "buyer_cost_signals_to_verify") | _amounts_for(box, "buyer_costs_confirmed")
    assert 486499 not in buyer
    assert 2000 not in buyer
    val_visible = _amounts_for(box, "valuation_references") | _amounts_for(box, "valuation_reference_amounts") | _amounts_for(box, "excluded_non_buyer_cost_amounts")
    assert 486499 in val_visible
    deductions_or_excluded = (
        _amounts_for(box, "valuation_deductions")
        | _amounts_for(box, "excluded_non_buyer_cost_amounts")
        | _amounts_for(box, "other_monetary_mentions")
        | _amounts_for(box, "valuation_references")
    )
    assert 2000 in deductions_or_excluded
    assert box.get("total_extra_cost_eur") is None


def test_money_map_via_umbria_no_stale_amounts(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _result_with_money_box(
        [
            {"label_it": "Regolarizzazione: € 31", "stima_euro": 31},
            {"label_it": "Regolarizzazione: € 6", "stima_euro": 6},
        ]
    )
    shadow = _shadow(
        "STIMA / FORMAZIONE LOTTI\n"
        "Valore di stima Euro 50.000,00. Deprezzamento Euro 5.000,00. "
        "Rendita catastale Euro 250,00."
    )
    apply_authority_money_projection_if_enabled(result, authority_shadow=shadow)
    box = result["customer_decision_contract"]["money_box"]
    text = _text_blob(box)
    assert "Regolarizzazione: € 31" not in text
    assert "Regolarizzazione: € 6" not in text
    assert box["total_extra_costs"]["min"] is None
    assert box["total_extra_costs"]["max"] is None
    buyer = _amounts_for(box, "cost_signals_to_verify") | _amounts_for(box, "buyer_costs_confirmed")
    assert 50000 not in buyer
    assert 5000 not in buyer
    assert 250 not in buyer


def test_money_map_ostuni_regolarizzazione_duration_is_not_money(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _result_with_money_box([{"label_it": "Regolarizzazione: 6 mesi", "note_it": "durata pratica"}])
    shadow = _shadow(
        "STIMA / FORMAZIONE LOTTI\n"
        "Tempi necessari per la regolarizzazione: 6 mesi."
    )
    apply_authority_money_projection_if_enabled(result, authority_shadow=shadow)
    box = result["customer_decision_contract"]["money_box"]
    buyer = _amounts_for(box, "cost_signals_to_verify") | _amounts_for(box, "buyer_costs_confirmed")
    assert 6 not in buyer


def test_money_map_flag_off_preserves_legacy_money_box(monkeypatch):
    monkeypatch.delenv(FEATURE_FLAG, raising=False)
    result = _result_with_money_box([{"label_it": "Regolarizzazione: € 31", "stima_euro": 31}])
    before = copy.deepcopy(result)
    apply_authority_money_projection_if_enabled(
        result,
        authority_shadow=_shadow(
            "STIMA / FORMAZIONE LOTTI\nmq 374,23 x €/mq 1.300,00 = € 486.499,00",
        ),
    )
    assert result == before


def test_money_map_da_verificare_always_has_evidence(monkeypatch):
    box = _money_map_apply(
        monkeypatch,
        "STIMA / FORMAZIONE LOTTI\nCosti di regolarizzazione a carico dell'aggiudicatario Euro 2.000,00 da verificare.",
    )
    signals = (
        box.get("buyer_cost_signals_to_verify", [])
        + box.get("cost_signals_to_verify", [])
        + box.get("other_monetary_mentions", [])
        + box.get("unsupported_or_unknown_amounts", [])
    )
    for item in signals:
        page = item.get("page")
        if page is None or page == 0:
            assert item.get("group") == "unsupported_or_unknown_amounts"
            assert "pagine non determinate" in str(item.get("verification_note_it") or "").lower() or item.get("verification_note_it")
        else:
            assert item.get("evidence"), f"DA VERIFICARE item must have evidence: {item}"
            assert item.get("verification_note_it") or item.get("explanation_it"), (
                "Each customer-visible money item needs explanation_it or verification_note_it"
            )


def test_money_map_no_invented_total_when_only_references(monkeypatch):
    box = _money_map_apply(
        monkeypatch,
        "STIMA / FORMAZIONE LOTTI\nValore di stima Euro 100.000,00. Rendita catastale Euro 250,00.",
    )
    assert box["total_extra_costs"]["min"] is None
    assert box["total_extra_costs"]["max"] is None
    assert box.get("total_extra_cost_eur") is None


@pytest.mark.parametrize(
    "formula_text, expected_amount",
    [
        ("mq 250,00 x €/mq 1.500,00 = € 375.000,00", 375000),
        ("mq 80,50 x €/mq 2.100,00 = € 169.050,00", 169050),
        ("ha 1,25 x €/ha 12.000,00 = € 15.000,00", 15000),
    ],
)
def test_money_map_generic_unit_price_formulas_are_valuation_references(monkeypatch, formula_text, expected_amount):
    box = _money_map_apply(
        monkeypatch,
        f"STIMA / FORMAZIONE LOTTI\n{formula_text}",
        _CASA_LIKE_RICH_CONTEXT,
    )
    buyer = _amounts_for(box, "cost_signals_to_verify") | _amounts_for(box, "buyer_costs_confirmed")
    assert expected_amount not in buyer, f"{expected_amount} from {formula_text} leaked into buyer costs"
    val_visible = _amounts_for(box, "valuation_references") | _amounts_for(box, "valuation_reference_amounts")
    assert expected_amount in val_visible, f"{expected_amount} from {formula_text} missing from valuation_references"


@pytest.mark.parametrize(
    "decurtation_text, decurted_amounts",
    [
        (
            "Il valore di stima viene decurtato per le spese tecniche di Euro 4.500,00 e per i diritti Euro 1.200,00. "
            "Totale decurtazioni Euro 5.700,00.",
            (4500, 1200, 5700),
        ),
        (
            "L'importo è stato decurtato della somma di Euro 8.250,00 quale deprezzamento, "
            "e di Euro 2.750,00 per arrotondamento.",
            (8250, 2750),
        ),
    ],
)
def test_money_map_generic_decurtation_formulas_are_not_buyer_cost(monkeypatch, decurtation_text, decurted_amounts):
    box = _money_map_apply(
        monkeypatch,
        f"STIMA / FORMAZIONE LOTTI\n{decurtation_text}",
        _CASA_LIKE_RICH_CONTEXT,
    )
    buyer = _amounts_for(box, "cost_signals_to_verify") | _amounts_for(box, "buyer_costs_confirmed")
    surfaced_non_buyer = (
        _amounts_for(box, "valuation_deductions")
        | _amounts_for(box, "valuation_references")
        | _amounts_for(box, "excluded_non_buyer_cost_amounts")
        | _amounts_for(box, "other_monetary_mentions")
    )
    for amount in decurted_amounts:
        assert amount not in buyer, f"€{amount} leaked into buyer costs"
        assert amount in surfaced_non_buyer, f"€{amount} missing from non-buyer Money Map sections"


def test_money_map_classifier_helpers_unit_smoke():
    from perizia_authority_money_projection import (
        classify_money_context,
        is_cadastral_context,
        is_explicit_buyer_obligation,
        is_formality_procedural_context,
        is_price_reference_context,
        is_valuation_arithmetic_context,
        is_valuation_deduction_context,
    )

    assert is_valuation_arithmetic_context("mq 374,23 x €/mq 1.300,00 = € 486.499,00")
    assert is_valuation_arithmetic_context("486.499,00 - 3.700,00 = 482.799,00")
    assert is_valuation_deduction_context("Il valore viene decurtato delle spese tecniche")
    assert is_valuation_deduction_context("Deprezzamento per anomalie urbanistiche")
    assert is_price_reference_context("Prezzo base d'asta Euro 80.000,00")
    assert is_price_reference_context("Valore di stima Euro 100.000,00")
    assert is_cadastral_context("Rendita catastale Euro 123,00")
    assert is_formality_procedural_context("Cancellazione ipoteca Euro 200,00")
    assert is_explicit_buyer_obligation("Costi a carico dell'aggiudicatario Euro 2.000,00")
    assert is_explicit_buyer_obligation("Sanatoria a carico dell'acquirente Euro 1.000,00")
    assert not is_explicit_buyer_obligation("Regolarizzazione urbanistica Euro 2.000,00")
    assert not is_explicit_buyer_obligation("Spese tecniche di regolarizzazione Euro 5.032,00")
    classification = classify_money_context(
        {
            "raw_text": "mq 374,23 x €/mq 1.300,00 = € 486.499,00",
            "amount_eur": 486499,
            "page": 22,
            "role": "unknown_money",
        }
    )
    assert classification["group"] == "valuation_references"
    classification = classify_money_context(
        {
            "raw_text": "Costi di regolarizzazione a carico dell'aggiudicatario Euro 2.000,00",
            "amount_eur": 2000,
            "page": 12,
            "role": "buyer_cost_signal_to_verify",
        }
    )
    assert classification["group"] == "buyer_cost_signals_to_verify"


# ---------------------------------------------------------------------------
# Money error-class regression tests (fix-money-error-classes branch)
#
# Each test below covers one error class from the brief: headline values,
# formula direction, buyer-cost promotion, multi-lot attribution, cadastral
# routing, formality grouping, dedup canonicalization, unit-price preservation,
# and OCR noise demotion. Plus flag-off invariance and Mongo immutability.
# ---------------------------------------------------------------------------


def test_error_class_a_headline_values_routed_correctly(monkeypatch):
    box = _money_map_apply(
        monkeypatch,
        "STIMA / FORMAZIONE LOTTI\nPrezzo base d'asta Euro 391.849,00.",
        "STIMA / FORMAZIONE LOTTI\nValore di stima Euro 419.849,00.",
        "STIMA / FORMAZIONE LOTTI\nValore di vendita giudiziaria Euro 78.500,00.",
        "STIMA / FORMAZIONE LOTTI\nPiu probabile valore immobiliare Euro 204.450,00.",
    )
    prices = _amounts_for(box, "price_references")
    assert {391849, 419849, 78500, 204450}.issubset(prices)


def test_error_class_b_formula_direction_minus_equals(monkeypatch):
    # 486.499 - 3.700 = 482.799  →  486499 is minuend (reference), 3700 is
    # deduction, 482799 is the final value after deductions. None of them
    # should ever be a buyer cost.
    box = _money_map_apply(
        monkeypatch,
        "STIMA / FORMAZIONE LOTTI\n"
        "Il valore viene decurtato di Euro 3.700,00 totale.\n"
        "Euro 486.499,00 - Euro 3.700,00 = Euro 482.799,00.",
        _CASA_LIKE_RICH_CONTEXT,
    )
    buyer = (
        _amounts_for(box, "buyer_costs_confirmed")
        | _amounts_for(box, "buyer_cost_signals_to_verify")
        | _amounts_for(box, "cost_signals_to_verify")
    )
    for amount in (486499, 3700, 482799):
        assert amount not in buyer, f"€{amount} leaked into buyer costs"
    # The result of the formula must show up as a price/final value, NOT a deduction.
    visible_as_price_or_value = (
        _amounts_for(box, "price_references")
        | _amounts_for(box, "valuation_references")
        | _amounts_for(box, "excluded_non_buyer_cost_amounts")
    )
    assert 482799 in visible_as_price_or_value, "final value after deductions must not be lost"
    # The subtracted amount (3700) must be a deduction (it has "decurtato di" context).
    deductions = _amounts_for(box, "valuation_deductions") | _amounts_for(box, "excluded_non_buyer_cost_amounts")
    assert 3700 in deductions


def test_error_class_c_buyer_cost_signal_promotion(monkeypatch):
    box = _money_map_apply(
        monkeypatch,
        "STIMA / FORMAZIONE LOTTI\nSpese condominiali insolute Euro 5.932,00 a carico dell'aggiudicatario.",
        "STIMA / FORMAZIONE LOTTI\nDebito pregresso condominio Euro 3.668,80 a carico dell'acquirente.",
    )
    signals = _amounts_for(box, "buyer_cost_signals_to_verify") | _amounts_for(box, "cost_signals_to_verify")
    assert 5932 in signals
    assert 3669 in signals or 3668 in signals


def test_error_class_d_multi_lot_money_attribution(monkeypatch):
    box = _money_map_apply(
        monkeypatch,
        "STIMA / FORMAZIONE LOTTI\nLotto 1\nPrezzo base d'asta Euro 64.198,00.",
        "STIMA / FORMAZIONE LOTTI\nLotto 2\nPrezzo base d'asta Euro 84.000,00.",
        "STIMA / FORMAZIONE LOTTI\nLotto 3\nPrezzo base d'asta Euro 224.268,00.",
    )
    by_lot: Dict[str, set] = {}
    for item in box.get("price_references", []):
        lot = item.get("lot_label") or ""
        by_lot.setdefault(lot, set()).add(item.get("amount_eur"))
    assert 64198 in by_lot.get("Lotto 1", set())
    assert 84000 in by_lot.get("Lotto 2", set())
    assert 224268 in by_lot.get("Lotto 3", set())


def test_error_class_d_lotto_unico_attribution(monkeypatch):
    box = _money_map_apply(
        monkeypatch,
        "STIMA / FORMAZIONE LOTTI\nLOTTO UNICO\nValore di stima finale Euro 503.930,00.",
    )
    found_lot_unico = False
    for item in box.get("price_references", []):
        if item.get("amount_eur") == 503930:
            assert item.get("lot_label") == "LOTTO UNICO"
            found_lot_unico = True
    assert found_lot_unico


def test_error_class_e_cadastral_routing(monkeypatch):
    box = _money_map_apply(
        monkeypatch,
        "STIMA / FORMAZIONE LOTTI\nRendita catastale Euro 250,00.",
        "STIMA / FORMAZIONE LOTTI\nRendita catastale Euro 123,50.",
    )
    cadastral = _amounts_for(box, "cadastral_values")
    assert 250 in cadastral
    assert 124 in cadastral or 123 in cadastral
    # Cadastral rendite must not appear as buyer costs.
    buyer = _amounts_for(box, "buyer_cost_signals_to_verify") | _amounts_for(box, "buyer_costs_confirmed")
    assert 250 not in buyer


def test_error_class_e_formality_routing_and_grouping(monkeypatch):
    # Same mortgage amount repeated on three pages — should appear once in
    # formalities_and_procedural_amounts with evidence covering all pages.
    box = _money_map_apply(
        monkeypatch,
        "FORMALITA PREGIUDIZIEVOLI\nIscrizione ipotecaria Euro 200.000,00 capitale.",
        "FORMALITA PREGIUDIZIEVOLI\nIscrizione ipotecaria Euro 200.000,00 capitale.",
        "FORMALITA PREGIUDIZIEVOLI\nIscrizione ipotecaria Euro 200.000,00 capitale.",
    )
    formalities = box.get("formalities_and_procedural_amounts", [])
    matching = [item for item in formalities if item.get("amount_eur") == 200000]
    assert len(matching) == 1, f"mortgage should be consolidated, got {len(matching)} entries"
    pages = matching[0].get("pages") or [matching[0].get("page")]
    assert len(set(pages)) >= 1


def test_error_class_f_dedup_canonicalization_marker(monkeypatch):
    box = _money_map_apply(
        monkeypatch,
        _CASA_LIKE_RICH_CONTEXT,
    )
    assert "canonical_display_groups" in box
    canonical = box["canonical_display_groups"]
    assert "valuation_references" in canonical
    assert "valuation_reference_amounts" not in canonical
    aliases = box.get("deprecated_alias_groups") or {}
    assert aliases.get("valuation_reference_amounts") == "valuation_references"
    assert aliases.get("cost_signals_to_verify") == "buyer_cost_signals_to_verify"


def test_error_class_g_unit_price_preserves_unit_context(monkeypatch):
    box = _money_map_apply(
        monkeypatch,
        "STIMA / FORMAZIONE LOTTI\nmq 374,23 x €/mq 1.300,00 = € 486.499,00",
    )
    # €/mq unit price must survive (not be demoted as small-amount OCR noise)
    # because of the unit-price context, even though 1300 is a relatively small
    # amount that could otherwise look like a fragment.
    unit_visible = (
        _amounts_for(box, "valuation_references")
        | _amounts_for(box, "other_monetary_mentions")
        | _amounts_for(box, "excluded_non_buyer_cost_amounts")
    )
    assert 1300 in unit_visible or 486499 in unit_visible


def test_error_class_g_ocr_noise_demoted(monkeypatch):
    # Cadastral table fragment with €1 / €5 / €24 amounts without economic
    # keywords must end up in the low-confidence bucket, not in primary groups.
    box = _money_map_apply(
        monkeypatch,
        "DATI IDENTIFICATIVI Foglio 12 Particella 345 Sub 1 Categoria A/3 Classe T-1 € 1,00 € 5,00 € 24,00",
    )
    for group in ("buyer_costs_confirmed", "buyer_cost_signals_to_verify", "valuation_references", "price_references"):
        assert 1 not in _amounts_for(box, group)
        assert 5 not in _amounts_for(box, group)
        assert 24 not in _amounts_for(box, group)


def test_error_class_flag_off_keeps_legacy_money_box_intact(monkeypatch):
    monkeypatch.delenv(FEATURE_FLAG, raising=False)
    result = _result_with_money_box(
        [
            {"label_it": "Spese di regolarizzazione: € 5.032", "stima_euro": 5032},
        ]
    )
    before = copy.deepcopy(result)
    meta = apply_authority_money_projection_if_enabled(
        result,
        authority_shadow=_shadow(
            "STIMA / FORMAZIONE LOTTI\nLotto 1\nPrezzo base d'asta Euro 64.198,00."
        ),
    )
    assert meta["status"] == "DISABLED"
    assert result == before


def test_error_class_no_internal_leak_in_customer_response(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _result_with_money_box([{"label_it": "Rendita catastale", "stima_euro": 123}])
    apply_authority_money_projection_if_enabled(
        result,
        authority_shadow=_shadow(
            "STIMA / FORMAZIONE LOTTI\nLotto 1\nValore di stima Euro 100.000,00.\n"
            "Rendita catastale Euro 123,00."
        ),
    )
    _assert_customer_clean(result)


@pytest.mark.anyio
async def test_error_class_mongo_immutable_on_read_money_projection(fake_db, monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    monkeypatch.delenv(server.AUTHORITY_LOT_PROJECTION_FLAG, raising=False)
    shadow = _shadow(
        "STIMA / FORMAZIONE LOTTI\nLotto 1\nPrezzo base d'asta Euro 64.198,00.",
        "STIMA / FORMAZIONE LOTTI\nLotto 2\nPrezzo base d'asta Euro 84.000,00.",
    )
    monkeypatch.setattr(server, "_load_authority_shadow_for_detail_read", lambda _aid, _a: shadow)
    result = _result_with_money_box([{"label_it": "Legacy: €100", "stima_euro": 100}])
    result.update(
        {
            "issues": [],
            "summary_for_client_bundle": {"semaforo_status": "GREEN"},
            "section_1_semaforo_generale": {"status": "GREEN"},
            "section_9_legal_killers": {"items": []},
            "section_11_red_flags": [],
        }
    )
    stored = {
        "analysis_id": "analysis_money_error_classes",
        "user_id": "user_test",
        "case_id": "case_money_error_classes",
        "case_title": "money-err.pdf",
        "file_name": "money-err.pdf",
        "created_at": datetime(2026, 5, 11, tzinfo=timezone.utc),
        "status": "COMPLETED",
        "pages_count": 4,
        "result": result,
    }
    fake_db.perizia_analyses.items.append(stored)
    before = copy.deepcopy(stored)

    response = await server._get_perizia_analysis_for_user(
        "analysis_money_error_classes",
        server.User(user_id="user_test", email="user@test.local", name="Test User"),
    )
    # Mongo doc must not be mutated by the read-time projection.
    assert fake_db.perizia_analyses.items[0] == before
    # The projected response carries the new lot-tagged headline values.
    box = response["result"]["customer_decision_contract"]["money_box"]
    price_amounts = {item.get("amount_eur") for item in box.get("price_references", [])}
    assert {64198, 84000}.issubset(price_amounts)


def test_error_class_resolver_position_aware_classification():
    from perizia_authority_resolvers import _money_role_reason_from_text

    # Formula result after "=" → final_valuation_after_deductions
    role, code = _money_role_reason_from_text(
        "486.499,00 - 3.700,00 = 482.799,00", "482.799,00"
    )
    assert role == "final_valuation_after_deductions"
    assert code == "FORMULA_RESULT_AFTER_EQUALS"

    # Subtrahend in deduction formula → valuation_deduction
    role, code = _money_role_reason_from_text(
        "Il valore viene decurtato 486.499,00 - 3.700,00 = 482.799,00", "3.700,00"
    )
    assert role == "valuation_deduction"

    # Minuend with decurtazione context → market_value (reference value being decurted)
    role, code = _money_role_reason_from_text(
        "Il valore viene decurtato 486.499,00 - 3.700,00 = 482.799,00", "486.499,00"
    )
    assert role == "market_value"

    # Judicial sale value headline
    role, code = _money_role_reason_from_text(
        "Valore di vendita giudiziaria Euro 78.500,00.", "Euro 78.500,00"
    )
    assert role == "judicial_sale_value"

    # "Valore arrotondato finale" → final_valuation_after_deductions
    role, code = _money_role_reason_from_text(
        "Valore arrotondato finale Euro 503.930,00.", "Euro 503.930,00"
    )
    assert role == "final_valuation_after_deductions"

    # Condominium arrears (buyer signal)
    role, code = _money_role_reason_from_text(
        "Spese condominiali insolute Euro 5.932,00 a carico dell'aggiudicatario.",
        "Euro 5.932,00",
    )
    assert role == "condominium_arrears"


# ---------------------------------------------------------------------------
# Money error-class V2 regression tests (real audit blockers)
#
# These tests pin the routing rules the V2 brief calls out so the 6-case real
# audit cannot silently regress. Each test reproduces a representative quote
# from one of the failing analyses.
# ---------------------------------------------------------------------------


def test_v2_casa_lotto_unico_final_value_is_market_value(monkeypatch):
    box = _money_map_apply(
        monkeypatch,
        "STIMA / FORMAZIONE LOTTI\n"
        "Il totale arrotondato del piu probabile valore immobiliare degli immobili "
        "del LOTTO UNICO risulta essere pari a € 503.930,00.",
    )
    visible = (
        _amounts_for(box, "price_references")
        | _amounts_for(box, "valuation_references")
        | _amounts_for(box, "excluded_non_buyer_cost_amounts")
    )
    assert 503930 in visible
    buyer = _amounts_for(box, "buyer_cost_signals_to_verify") | _amounts_for(box, "buyer_costs_confirmed")
    assert 503930 not in buyer


def test_v2_casa_formality_cancellation_294_is_procedural(monkeypatch):
    box = _money_map_apply(
        monkeypatch,
        "FORMALITA PREGIUDIZIEVOLI\n"
        "I costi necessari alla cancellazione delle formalita ipotecarie sono di € 294,00 "
        "(imposta ipotecaria, imposta di bollo e tassa ipotecaria).",
    )
    formality = _amounts_for(box, "formalities_and_procedural_amounts")
    assert 294 in formality
    buyer = _amounts_for(box, "buyer_cost_signals_to_verify") | _amounts_for(box, "buyer_costs_confirmed")
    assert 294 not in buyer


def test_v2_via_umbria_condominium_arrears_buyer_signal(monkeypatch):
    box = _money_map_apply(
        monkeypatch,
        "STIMA / FORMAZIONE LOTTI\n"
        "Spese condominiali insolute ai sensi dell'art.568 cpc: €. 5.932,00 "
        "€. 197.068,00 €. 0,00 €. 0,00 "
        "Valore di vendita giudiziaria dell'immobile al netto delle decurtazioni: "
        "€. 191.273,57.",
    )
    signals = _amounts_for(box, "buyer_cost_signals_to_verify") | _amounts_for(box, "cost_signals_to_verify")
    assert 5932 in signals, f"Spese condominiali insolute €5.932 must surface as buyer signal, got {signals}"
    # Final value/judicial sale must remain a price reference, not a buyer cost
    buyer_costs = _amounts_for(box, "buyer_costs_confirmed")
    assert 191274 not in buyer_costs and 191273 not in buyer_costs


def test_v2_1859886_oneri_regolarizzazione_is_valuation_deduction(monkeypatch):
    box = _money_map_apply(
        monkeypatch,
        "STIMA / FORMAZIONE LOTTI\n"
        "Tipologia deprezzamento  Valore  Tipo\n"
        "Oneri di regolarizzazione urbanistica  23000,00  €\n"
        "Rischio assunto per mancata garanzia  5000,00  €\n"
        "Valore finale di stima: € 391.849,00.",
    )
    deductions = (
        _amounts_for(box, "valuation_deductions")
        | _amounts_for(box, "excluded_non_buyer_cost_amounts")
    )
    assert 23000 in deductions
    assert 5000 in deductions
    # Final value still a price reference
    price = _amounts_for(box, "price_references") | _amounts_for(box, "valuation_references")
    assert 391849 in price


def test_v2_multilot_a_lavori_ultimati_is_valuation_reference(monkeypatch):
    box = _money_map_apply(
        monkeypatch,
        "STIMA / FORMAZIONE LOTTI\n"
        "Valore considerato a lavori ultimati: € 622.970,00. "
        "Valore di stima (stato di fatto): € 280.336,00.",
    )
    visible = (
        _amounts_for(box, "valuation_references")
        | _amounts_for(box, "price_references")
        | _amounts_for(box, "excluded_non_buyer_cost_amounts")
    )
    assert 622970 in visible
    deductions = _amounts_for(box, "valuation_deductions")
    assert 622970 not in deductions, "a lavori ultimati amount must never become a deduction"
    buyer = _amounts_for(box, "buyer_cost_signals_to_verify") | _amounts_for(box, "buyer_costs_confirmed")
    assert 622970 not in buyer


def test_v2_via_nuova_canonical_runtime_judicial_sale_and_arrears(monkeypatch):
    # Reproduces the page-24 table where amounts appear without € prefix and
    # where the column header sits two rows above the actual cell value, plus
    # the condominium debit on a different page.
    box = _money_map_apply(
        monkeypatch,
        "STIMA / FORMAZIONE LOTTI\n"
        "CALCOLO DEL VALORE DI MERCATO PER VENDITA GIUDIZIARIA - COMPENDIO A\n"
        "Valore arrotondato finale per vendita giudiziaria (€)\n"
        "197.400,00 + 7.050,00 = 125.921,05\n"
        "78.528,95\n"
        "78.500,00\n"
        "= 204.450,00",
        "STIMA / FORMAZIONE LOTTI\n"
        "Tramite accertamenti presso l'amministrazione del condominio risulta a "
        "carico dell'immobile in stima un debito pregresso di 3.668,80 €.",
    )
    visible = (
        _amounts_for(box, "valuation_references")
        | _amounts_for(box, "price_references")
        | _amounts_for(box, "excluded_non_buyer_cost_amounts")
    )
    assert 204450 in visible
    assert 78500 in visible
    arrears = _amounts_for(box, "buyer_cost_signals_to_verify") | _amounts_for(box, "cost_signals_to_verify")
    assert 3669 in arrears or 3668 in arrears


def test_v2_via_del_mare_pignoramento_amount_is_formality(monkeypatch):
    box = _money_map_apply(
        monkeypatch,
        "FORMALITA PREGIUDIZIEVOLI\n"
        "IL PRESENTE PIGNORAMENTO VIENE TRASCRITTO PER LA COMPLESSIVA SOMMA DI "
        "EURO 133.544,84 OLTRE INTERESSI E SPESE FINO AL SODDISFO.",
    )
    formality = _amounts_for(box, "formalities_and_procedural_amounts")
    # Accept either an exact integer or rounded form
    assert any(amt in {133544, 133545} for amt in formality)
    buyer = _amounts_for(box, "buyer_cost_signals_to_verify") | _amounts_for(box, "buyer_costs_confirmed")
    for amt in (133544, 133545):
        assert amt not in buyer


def test_v2_resolver_position_aware_v2_classifications():
    from perizia_authority_resolvers import _money_role_reason_from_text

    # "valore arrotondato finale per vendita giudiziaria" → judicial_sale_value
    role, code = _money_role_reason_from_text(
        "Valore arrotondato finale per vendita giudiziaria (€) 204.450,00",
        "204.450,00",
    )
    assert role == "judicial_sale_value"

    # "Oneri di regolarizzazione urbanistica" before amount → valuation_deduction
    role, code = _money_role_reason_from_text(
        "Oneri di regolarizzazione urbanistica  23000,00  €",
        "23000,00",
    )
    assert role == "valuation_deduction"

    # "Rischio assunto per mancata garanzia" before amount → valuation_deduction
    role, code = _money_role_reason_from_text(
        "Rischio assunto per mancata garanzia  5000,00  €",
        "5000,00",
    )
    assert role == "valuation_deduction"

    # "a lavori ultimati" → market_value (valuation reference), not deduction
    role, code = _money_role_reason_from_text(
        "Valore considerato a lavori ultimati € 622.970,00",
        "€ 622.970,00",
    )
    assert role == "market_value"

    # "Spese condominiali insolute ai sensi dell'art.568 cpc:" with periods in
    # the label window — must still classify as condominium_arrears.
    role, code = _money_role_reason_from_text(
        "Spese condominiali insolute ai sensi dell'art.568 cpc: €. 5.932,00",
        "€. 5.932,00",
    )
    assert role == "condominium_arrears"


def test_v2_parse_amount_handles_bare_thousands():
    # The pre-fix parser collapsed "23000,00" into 230 because of greedy
    # thousand-separator regex; the fix tries the dotted form first then falls
    # back to bare digits.
    from perizia_authority_resolvers import _parse_amount

    assert _parse_amount("23000,00 €") == 23000.0
    assert _parse_amount("€ 5000") == 5000.0
    assert _parse_amount("23.000,00") == 23000.0


def test_v2_standalone_table_cell_amount_captured(monkeypatch):
    # An amount in an Italian-format table cell without a € symbol that sits
    # under a "vendita giudiziaria" column header must be captured.
    box = _money_map_apply(
        monkeypatch,
        "STIMA / FORMAZIONE LOTTI\n"
        "Valore arrotondato finale per vendita giudiziaria (€)\n"
        "Lotto 1\n"
        "78.500,00",
    )
    visible = (
        _amounts_for(box, "price_references")
        | _amounts_for(box, "valuation_references")
        | _amounts_for(box, "excluded_non_buyer_cost_amounts")
    )
    assert 78500 in visible


def test_v2_estratto_quality_bridge_adds_candidates(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _result_with_money_box([])
    # Provide a minimal estratto_quality with a reliable headline value at a
    # specific page; the projection must surface it as a valuation_reference.
    result["estratto_quality"] = {
        "sections": [
            {
                "items": [
                    {
                        "label_it": "Valore di stima LOTTO UNICO",
                        "evidence": [
                            {"page": 23, "quote": "LOTTO UNICO valore € 503.930,00"}
                        ],
                        "amount_eur": 503930,
                    }
                ]
            }
        ]
    }
    shadow = _shadow(
        "STIMA / FORMAZIONE LOTTI\nValore di stima Euro 100.000,00.",
        "STIMA / FORMAZIONE LOTTI\nLOTTO UNICO valore € 503.930,00.",
    )
    meta = apply_authority_money_projection_if_enabled(result, authority_shadow=shadow)
    assert meta.get("status") in {"APPLIED", "ALREADY_MATCHES"}
    box = result["customer_decision_contract"]["money_box"]
    visible = (
        _amounts_for(box, "price_references")
        | _amounts_for(box, "valuation_references")
        | _amounts_for(box, "excluded_non_buyer_cost_amounts")
    )
    assert 503930 in visible


def test_v2_flag_off_with_v2_inputs_does_not_mutate(monkeypatch):
    monkeypatch.delenv(FEATURE_FLAG, raising=False)
    result = _result_with_money_box(
        [
            {"label_it": "Spese condominiali insolute: € 5.932", "stima_euro": 5932},
        ]
    )
    result["estratto_quality"] = {
        "items": [
            {
                "label_it": "Valore LOTTO UNICO",
                "evidence": [{"page": 23, "quote": "€ 503.930,00"}],
                "amount_eur": 503930,
            }
        ]
    }
    before = copy.deepcopy(result)
    meta = apply_authority_money_projection_if_enabled(
        result,
        authority_shadow=_shadow(
            "STIMA / FORMAZIONE LOTTI\nSpese condominiali insolute €. 5.932,00 a carico dell'acquirente."
        ),
    )
    assert meta["status"] == "DISABLED"
    assert result == before
