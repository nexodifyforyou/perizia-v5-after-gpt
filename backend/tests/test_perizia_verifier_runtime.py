import math
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from perizia_qa.fixture_runner import run_named_fixture
from perizia_runtime.runtime import apply_verifier_to_result, run_quality_verifier


def _repo_fixture(name: str):
    fixture_dir = Path(__file__).resolve().parents[1] / "perizia_qa" / "fixtures" / name
    result = json.loads((fixture_dir / "result_seed.json").read_text(encoding="utf-8"))
    pages = json.loads((fixture_dir / "pages_raw.json").read_text(encoding="utf-8"))
    normalized_pages = [
        {
            "page_number": int(row.get("page_number") or row.get("page") or idx),
            "text": str(row.get("text") or ""),
        }
        for idx, row in enumerate(pages or [], start=1)
        if isinstance(row, dict)
    ]
    return result, normalized_pages


def _silvabella_fixture():
    pages = [
        {
            "page_number": 1,
            "text": (
                "Immobile sito nel Comune di Mortara, Piazza Silvabella n. 12. "
                "Proprietà. in forza di TRASCRIZIONE A FAVORE del 02/11/2016. "
                "VALUTAZIONE COMPLESSIVA. Valore al netto dei costi di regolarizzazione. "
                "Subalterno 2. € 56.861,33. € 53.339,39. Valore al netto dei costi di regolarizzazione e della riduzione cautelativa. € 45.338,48."
            ),
        },
        {
            "page_number": 5,
            "text": (
                "Si precisa che, al momento del sopralluogo, avvenuto in data 11/09/2025, "
                "il bene non appariva occupato da nessuno."
            ),
        },
        {
            "page_number": 7,
            "text": (
                "Vincoli ed oneri giuridici che saranno cancellati a cura e spese della procedura. "
                "ISCRIZIONE CONTRO del 02/11/2016. IPOTECA VOLONTARIA derivante da CONCESSIONE A GARANZIA DI MUTUO FONDIARIO."
            ),
        },
        {
            "page_number": 21,
            "text": (
                "COEFFICIENTI. Coefficiente di locazione 1,000. Coefficiente di Usufrutto 1,000. "
                "Valore complessivo (VC) € 56.861,33."
            ),
        },
        {
            "page_number": 22,
            "text": (
                "Adeguamenti e correzioni di stima. Spese tecniche di regolazione difformità urbanistico edilizie € 500,00. "
                "Spese condominiali scadute e non pagate negli ultimi due anni dalla data di trascrione del pignoramento € 3.021,94. "
                "TOTALE € 3.521,94. Riduzione cautelativa € 8.000,91. PREZZO A BASE D'ASTA DELL'IMMOBILE. "
                "Valore complessivo € 56.861,33. Valore al netto dei costi di regolarizzazione € 53.339,39. "
                "Valore al netto dei costi di regolarizzazione e della riduzione cautelativa € 45.338,48."
            ),
        },
    ]
    result = {
        "report_header": {"address": {"value": "Mortara, Piazza Silvabella 12", "evidence": []}},
        "section_9_legal_killers": {"items": []},
        "field_states": {},
        "dati_certi_del_lotto": {},
        "document_quality": {"status": "TEXT_OK"},
        "semaforo_generale": {"status": "AMBER"},
    }
    return result, pages


def test_verifier_catches_silvabella_failure_modes():
    result, pages = _silvabella_fixture()
    payload = run_quality_verifier(
        analysis_id="analysis_8954c511ed4e",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    canonical = payload["canonical_case"]
    assert canonical["rights"]["quota"]["value"] is None
    assert canonical["pricing"]["selected_price"] == 45338.48
    invalid_reasons = {item["reason"] for item in canonical["pricing"]["invalid_candidates"]}
    assert "subalterno_number_contamination" in invalid_reasons
    assert canonical["occupancy"]["status"] == "LIBERO"
    invalid_occ = [item for item in canonical["occupancy"]["candidates"] if not item["valid"]]
    assert any(item["reason"] == "valuation_coefficient_not_valid_occupancy" for item in invalid_occ)
    assert math.isclose(canonical["costs"]["explicit_total"], 3521.94, rel_tol=0.0, abs_tol=0.01)
    assert "Costi espliciti" in canonical["priority"]["top_issue"]["title_it"]
    assert canonical["summary_bundle"]["top_issue_it"] in canonical["summary_bundle"]["decision_summary_it"]


def test_verifier_bridge_updates_legacy_result_for_routed_fields():
    result, pages = _silvabella_fixture()
    payload = run_quality_verifier(
        analysis_id="analysis_8954c511ed4e",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    apply_verifier_to_result(result, payload)
    assert result["field_states"]["stato_occupativo"]["value"] == "LIBERO"
    assert result["field_states"]["opponibilita_occupazione"]["value"] == "NON VERIFICABILE"
    assert "Costi espliciti" in result["section_9_legal_killers"]["top_items"][0]["killer"]
    assert math.isclose(result["money_box"]["verifier_costs_summary"]["explicit_total_eur"], 3521.94, rel_tol=0.0, abs_tol=0.01)
    assert result["summary_for_client"]["generation_mode"] == "deterministic_canonical_bundle"


def test_named_fixture_runner_for_existing_cases():
    silvabella = run_named_fixture("silvabella")
    assert silvabella["status"] == "PASS"
    rmei = run_named_fixture("rmei_928_2022")
    assert rmei["status"] == "PASS"


def test_verifier_emits_legal_attention_fallback_for_cancellable_only_cases():
    for analysis_id, fixture_name in [
        ("mantova", "mantova"),
        ("multilot_69_2024", "multilot_69_2024"),
    ]:
        result, pages = _repo_fixture(fixture_name)
        payload = run_quality_verifier(
            analysis_id=analysis_id,
            result=result,
            pages=pages,
            full_text="\n\n".join(page["text"] for page in pages),
        )
        top_issue = payload["canonical_case"]["priority"]["top_issue"]
        summary = payload["canonical_case"]["summary_bundle"]
        assert top_issue["code"] == "LEGAL_CANCELLABLE_ATTENTION"
        assert "Formalità da cancellare" in top_issue["title_it"]
        assert top_issue["category"] == "legal_background"
        assert summary["top_issue_it"] == top_issue["title_it"]
        assert summary["decision_summary_it"] != "Verifica manualmente i punti critici prima dell'offerta."
        assert "cancellazione delle formalità" in summary["decision_summary_it"]


def test_multibene_occupancy_prefers_property_occupied_state_over_libero_noise():
    result, pages = _repo_fixture("multibene_1859886")
    payload = run_quality_verifier(
        analysis_id="multibene_1859886",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    canonical = payload["canonical_case"]
    invalid_occ = [item for item in canonical["occupancy"]["candidates"] if not item["valid"]]
    assert canonical["occupancy"]["status"] == "OCCUPATO"
    assert canonical["priority"]["top_issue"]["code"] == "OCCUPANCY_RISK"
    assert canonical["summary_bundle"]["decision_summary_it"] != "Verifica manualmente i punti critici prima dell'offerta."
    assert any(item["reason"] == "non_property_libero_noise" for item in invalid_occ)
