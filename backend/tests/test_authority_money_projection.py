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
        "Spese tecniche di regolarizzazione Euro 5.032,00.\n"
        "Sanzione per sanatoria Euro 3.000,00."
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
        "Spese tecniche di regolarizzazione Euro 5.032,00.\n"
        "Sanzione per sanatoria Euro 3.000,00."
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
    labels = _text_blob(box["excluded_non_buyer_cost_amounts"])
    assert "Rendita catastale: dato fiscale" in labels
    assert "Formalita/cancellazione" in labels
    assert "Importo valutativo" in labels


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
        "Spese tecniche di regolarizzazione Euro 5.032,00.\n"
        "Sanzione per sanatoria Euro 3.000,00."
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
