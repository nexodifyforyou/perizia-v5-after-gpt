import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import perizia_authority_resolvers as money_shadow  # noqa: E402
from perizia_section_authority import build_section_authority_map  # noqa: E402


def _pages(*texts: str) -> List[Dict[str, Any]]:
    return [{"page_number": idx, "text": text} for idx, text in enumerate(texts, start=1)]


def _money(*texts: str, candidates: Dict[str, Any] | None = None, section_map: Dict[str, Any] | None = None) -> Dict[str, Any]:
    pages = _pages(*texts)
    authority_map = section_map if section_map is not None else build_section_authority_map(pages)
    return money_shadow.resolve_money_roles_shadow(pages, authority_map, candidates=candidates)


def _value(result: Dict[str, Any]) -> Dict[str, Any]:
    return result["value"]


def _candidate(value: Dict[str, Any], role: str) -> Dict[str, Any]:
    for candidate in value["money_candidates"]:
        if candidate["role"] == role:
            return candidate
    raise AssertionError(f"missing role {role}: {value['money_candidates']}")


def test_rendita_catastale_is_not_buyer_cost():
    value = _value(_money("STIMA / FORMAZIONE LOTTI\nRendita catastale Euro 123,00."))

    candidate = _candidate(value, "cadastral_rendita")
    assert candidate["reason_code"] == "RENDITA_CATASTALE_AMOUNT"
    assert candidate["is_customer_safe_cost"] is False
    assert candidate["should_surface_in_money_box"] is False
    assert candidate["should_sum"] is False


def test_prezzo_base_and_valuation_are_not_buyer_costs():
    value = _value(
        _money(
            "STIMA / FORMAZIONE LOTTI\nPrezzo base d'asta Euro 80.000,00.",
            "STIMA / FORMAZIONE LOTTI\nValore di stima Euro 100.000,00.",
            "STIMA / FORMAZIONE LOTTI\nValore finale di stima Euro 90.000,00.",
        )
    )

    for role in ("base_auction", "market_value", "final_value"):
        candidate = _candidate(value, role)
        assert candidate["is_customer_safe_cost"] is False
        assert candidate["should_surface_in_money_box"] is False
        assert candidate["should_sum"] is False


def test_deprezzamento_is_valuation_deduction_not_extra_cost():
    value = _value(_money("STIMA / FORMAZIONE LOTTI\nDeprezzamento per stato d'uso Euro 10.000,00."))

    candidate = _candidate(value, "valuation_deduction")
    assert candidate["reason_code"] == "VALUATION_DEDUCTION_AMOUNT"
    assert candidate["is_customer_safe_cost"] is False
    assert candidate["should_sum"] is False


def test_formality_cancellation_amount_is_not_buyer_cost():
    value = _value(
        _money(
            "FORMALITA PREGIUDIZIEVOLI\n"
            "Imposta ipotecaria Euro 200,00 e tassa Euro 35,00 per cancellazione dell'ipoteca."
        )
    )

    candidate = _candidate(value, "formalities_procedural_amount")
    assert candidate["reason_code"] == "FORMALITA_PROCEDURAL_AMOUNT"
    assert candidate["is_customer_safe_cost"] is False
    assert candidate["should_surface_in_money_box"] is False


def test_explicit_regolarizzazione_is_cost_signal_to_verify_when_factual_authority_supported():
    value = _value(
        _money(
            "STIMA / FORMAZIONE LOTTI\n"
            "Spese tecniche di regolarizzazione urbanistica Euro 1.500,00 a carico dell'aggiudicatario."
        )
    )

    candidate = _candidate(value, "buyer_cost_signal_to_verify")
    assert candidate["reason_code"] == "EXPLICIT_BUYER_COST_SIGNAL"
    assert candidate["is_customer_safe_cost"] is True
    assert candidate["should_surface_in_money_box"] is True
    assert candidate["should_sum"] is False


def test_component_and_total_relationship_does_not_double_count_components():
    value = _value(
        _money(
            "STIMA / FORMAZIONE LOTTI\n"
            "Regolarizzazione urbanistica:\n"
            "Spese tecniche Euro 1.000,00\n"
            "Sanzione Euro 500,00\n"
            "Totale regolarizzazione Euro 1.500,00 a carico dell'aggiudicatario"
        )
    )

    assert value["summary"]["double_count_risk"] is True
    total = _candidate(value, "total_candidate")
    components = [candidate for candidate in value["money_candidates"] if candidate["role"] == "component_of_total"]
    assert total["should_sum"] is True
    assert len(components) == 2
    assert all(component["should_sum"] is False for component in components)
    assert all(component["parent_total_candidate_id"] == total["candidate_id"] for component in components)


def test_instruction_only_amount_is_unknown_or_weak_not_customer_safe_cost():
    value = _value(_money("QUESITO\nVerifichi le spese e gli oneri Euro 1.000,00 eventualmente necessari."))

    candidate = value["money_candidates"][0]
    assert candidate["role"] == "unknown_money"
    assert candidate["reason_code"] == "INSTRUCTION_OR_BOILERPLATE_AMOUNT"
    assert candidate["is_customer_safe_cost"] is False
    assert "WEAK_OR_INSTRUCTION_AUTHORITY" in candidate["warnings"]


def test_missing_and_corrupt_section_authority_fail_open():
    pages = _pages("STIMA / FORMAZIONE LOTTI\nRendita catastale Euro 123,00.")

    missing = money_shadow.resolve_money_roles_shadow(pages, {"_authority_tagging_status": "missing_map"})
    corrupt = money_shadow.resolve_money_roles_shadow(pages, {"_authority_tagging_status": "corrupt_map"})

    for result in (missing, corrupt):
        assert result["status"] == "FAIL_OPEN"
        assert result["fail_open"] is True


def test_malformed_candidate_money_file_is_partial_not_crash():
    result = _money(
        "STIMA / FORMAZIONE LOTTI\nPrezzo base Euro 80.000,00.",
        candidates={"money": {"bad": True}},
    )

    assert result["status"] == "PARTIAL"
    assert "malformed_candidate_money_file" in result["notes"]
    assert _value(result)["base_auction"]


def test_bad_single_candidate_authority_classification_is_partial_fail_open(monkeypatch: pytest.MonkeyPatch):
    pages = _pages(
        "STIMA / FORMAZIONE LOTTI\nRendita catastale Euro 123,00.",
        "STIMA / FORMAZIONE LOTTI\nPrezzo base Euro 80.000,00.",
    )
    section_map = build_section_authority_map(pages)
    original = money_shadow.classify_quote_authority

    def flaky(page_number: int, quote: str, section_map: Dict[str, Any], domain: str | None = None):
        if int(page_number) == 1:
            raise ValueError("bad authority row")
        return original(page_number, quote, section_map, domain=domain)

    monkeypatch.setattr(money_shadow, "classify_quote_authority", flaky)

    result = money_shadow.resolve_money_roles_shadow(pages, section_map)

    assert result["status"] == "PARTIAL"
    assert result["fail_open"] is True
    assert _value(result)["cadastral_rendita"]
    assert _value(result)["base_auction"]
