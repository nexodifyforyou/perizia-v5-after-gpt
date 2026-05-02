import sys
from pathlib import Path
from typing import Any, Dict, List

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from scripts import compare_authority_money_vs_legacy as compare  # noqa: E402


def _pages(*texts: str) -> List[Dict[str, Any]]:
    return [{"page_number": idx, "text": text} for idx, text in enumerate(texts, start=1)]


def test_money_comparison_row_identifies_unsafe_legacy_cost_classes():
    pages = _pages(
        "STIMA / FORMAZIONE LOTTI\n"
        "Rendita catastale Euro 123,00.\n"
        "Prezzo base d'asta Euro 80.000,00.\n"
        "Valore di stima Euro 100.000,00.\n"
        "Deprezzamento Euro 10.000,00.\n"
        "FORMALITA: cancellazione ipoteca Euro 200,00."
    )
    legacy_result = {
        "money_box": {
            "items": [
                {"label": "Rendita catastale", "amount": "Euro 123,00"},
                {"label": "Prezzo base", "amount": "Euro 80.000,00"},
                {"label": "Valore di stima", "amount": "Euro 100.000,00"},
                {"label": "Deprezzamento", "amount": "Euro 10.000,00"},
                {"label": "Formalita ipotecaria", "amount": "Euro 200,00"},
            ]
        }
    }

    row = compare._row(file_label="synthetic.pdf", analysis_id="", pages=pages, result=legacy_result)

    assert row["comparison_verdict"] == "AUTHORITY_BETTER_THAN_LEGACY"
    assert "rendita_as_buyer_cost" in row["unsafe_legacy_signals"]
    assert "price_as_buyer_cost" in row["unsafe_legacy_signals"]
    assert "valuation_as_buyer_cost" in row["unsafe_legacy_signals"]
    assert "deprezzamento_as_extra_cost" in row["unsafe_legacy_signals"]
    assert "formalita_as_buyer_cost" in row["unsafe_legacy_signals"]


def test_via_umbria_stale_money_projection_is_detected(monkeypatch):
    monkeypatch.setattr(compare, "_load_candidates_for_analysis", lambda _analysis_id: {})
    pages = _pages(
        "STIMA / FORMAZIONE LOTTI\n"
        "Spese tecniche di regolarizzazione Euro 5.032,00.\n"
        "Sanzione per sanatoria Euro 3.000,00."
    )
    legacy_result = {"money_box": {"items": [{"label": "Regolarizzazione: € 31"}, {"label": "Regolarizzazione: € 6"}]}}

    row = compare._row(file_label="VIA UMBRIA N. 26.pdf", analysis_id="analysis_85b6655bedd2", pages=pages, result=legacy_result)

    assert row["stale_via_umbria_money_risk"] is True
    assert "stale_money_projection" in row["unsafe_legacy_signals"]
    assert 5032.0 in row["authority_amounts_by_role"]["buyer_cost_signal_to_verify"]
    assert 3000.0 in row["authority_amounts_by_role"]["buyer_cost_signal_to_verify"]


def test_component_total_double_count_risk_is_reported_conservatively():
    pages = _pages(
        "STIMA / FORMAZIONE LOTTI\n"
        "Regolarizzazione urbanistica: spese tecniche Euro 1.000,00; "
        "sanzione Euro 500,00; Totale regolarizzazione Euro 1.500,00."
    )
    legacy_result = {
        "money_box": {
            "items": [
                {"label": "Spese tecniche", "amount": "Euro 1.000,00"},
                {"label": "Sanzione", "amount": "Euro 500,00"},
                {"label": "Totale regolarizzazione", "amount": "Euro 1.500,00"},
            ]
        }
    }

    row = compare._row(file_label="synthetic-total.pdf", analysis_id="", pages=pages, result=legacy_result)

    assert row["component_total_double_count_risk"] is True
    assert "component_total_double_count" in row["unsafe_legacy_signals"]


def test_missing_pdf_fails_open_clearly():
    row = compare.compare_pdf(Path("/tmp/perizia_missing_money_phase_3e.pdf"))

    assert row["comparison_verdict"] == "FAIL_OPEN_ACCEPTABLE"
    assert "missing_pdf" in row["unsafe_legacy_signals"]
    assert "missing_pdf:" in row["notes"]


def test_customer_authority_money_keys_do_not_leak_after_sanitization():
    result = {
        "debug": {"authority_money": {"leak": True}},
        "internal_runtime": {"debug": {"shadow_money": {"leak": True}}},
        "money_box": {
            "items": [
                {
                    "label": "Spese",
                    "amount": "Euro 1.000,00",
                    "authority_money": {"authority_level": "HIGH_FACTUAL"},
                    "shadow_money": {"section_zone": "FINAL_LOT_FORMATION"},
                }
            ]
        },
    }

    cleaned = compare.sanitized_customer_result(result)

    assert compare.collect_customer_leaks(cleaned) == []
    assert cleaned["money_box"]["items"][0]["label"] == "Spese"


def test_verdict_can_identify_authority_worse_when_customer_leaks_exist():
    verdict = compare.verdict_for_money(
        money_status="OK",
        unsafe_signals=[],
        customer_leaks=["result.money_box.items[0].authority_money"],
        money_value={"money_candidates": []},
    )

    assert verdict == "AUTHORITY_WORSE_THAN_LEGACY"


def test_randomized_sample_mode_does_not_crash(monkeypatch):
    monkeypatch.setattr(compare, "_sample_sources", lambda limit, seed: [("file", "/tmp/perizia_missing_money_phase_3e.pdf")])

    rows = compare.compare_sample(8, 42)

    assert len(rows) == 1
    assert rows[0]["comparison_verdict"] == "FAIL_OPEN_ACCEPTABLE"


def test_golden_corpus_fixture_includes_required_money_cases():
    cases = compare._fixture_cases()
    ids = {case.get("id") for case in cases}

    assert {
        "1859886_c_perizia",
        "multilot_69_2024",
        "ostuni_via_viterbo_2",
        "via_umbria",
        "casa_ai_venti",
        "via_nuova_19_1",
        "via_del_mare_4591_4593",
    }.issubset(ids)
    via_umbria = next(case for case in cases if case.get("id") == "via_umbria")
    assert "analysis_85b6655bedd2" in via_umbria.get("analysis_ids", [])
