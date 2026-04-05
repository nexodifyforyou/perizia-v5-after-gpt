import math
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from perizia_qa.fixture_runner import run_named_fixture
from perizia_runtime.runtime import apply_verifier_to_result, run_quality_verifier
from perizia_tools import valuation_table_tool


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


def _pricing_probe(name: str):
    result, pages = _repo_fixture(name)
    payload = run_quality_verifier(
        analysis_id=name,
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    return payload["canonical_case"]["pricing"]


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
    assert canonical["pricing"]["adjusted_market_value"] == 53339.39
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
    for analysis_id, fixture_name in [("mantova", "mantova")]:
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


def test_bare_nessuno_does_not_create_libero_without_property_anchor():
    result = {
        "field_states": {},
        "dati_certi_del_lotto": {},
        "document_quality": {"status": "TEXT_OK"},
        "semaforo_generale": {"status": "AMBER"},
    }
    pages = [
        {
            "page_number": 1,
            "text": "Tipologia del diritto 1/1 di piena proprietà. Comproprietari: Nessuno.",
        },
        {
            "page_number": 2,
            "text": "È presente un contratto di locazione stipulato in data anteriore al pignoramento.",
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_bare_nessuno_block",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    canonical = payload["canonical_case"]
    assert canonical["occupancy"]["status"] is None
    assert not canonical["occupancy"]["candidates"]


def test_tenure_signal_creates_nonfree_occupancy_with_cautious_opponibilita():
    result = {
        "field_states": {},
        "dati_certi_del_lotto": {},
        "document_quality": {"status": "TEXT_OK"},
        "semaforo_generale": {"status": "AMBER"},
    }
    pages = [
        {
            "page_number": 1,
            "text": "È presente un contratto di locazione stipulato in data anteriore al pignoramento per una porzione del bene.",
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_tenure_signal_occupancy",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    canonical = payload["canonical_case"]
    assert canonical["occupancy"]["status"] == "OCCUPATO"
    assert canonical["occupancy"]["opponibilita"] == "LOCAZIONE DA VERIFICARE"
    assert canonical["priority"]["top_issue"]["code"] == "OCCUPANCY_RISK"


def test_multi_lot_auction_prices_do_not_force_scalar_selected_price():
    result = {
        "field_states": {},
        "dati_certi_del_lotto": {},
        "document_quality": {"status": "TEXT_OK"},
        "semaforo_generale": {"status": "AMBER"},
    }
    pages = [
        {
            "page_number": 1,
            "text": (
                "Schema riassuntivo. Lotto 1 - Prezzo base d'asta: € 64.198,00. "
                "Lotto 2 - Prezzo base d'asta: € 84.000,00. "
                "Lotto 3 - Prezzo base d'asta: € 224.268,00."
            ),
        },
        {
            "page_number": 2,
            "text": (
                "Lotto 1. Valore di stima del bene: € 80.248,00. "
                "Lotto 2. Valore di stima del bene: € 105.000,00. "
                "Lotto 3. Valore di stima del bene: € 280.336,00."
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_multi_lot_pricing_policy",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    canonical = payload["canonical_case"]
    invalid_reasons = {item["reason"] for item in canonical["pricing"]["invalid_candidates"]}
    assert canonical["pricing"]["selected_price"] is None
    assert canonical["pricing"]["benchmark_value"] is None
    assert "multi_lot_scalar_price_suppressed" in invalid_reasons
    assert "multi_lot_scalar_benchmark_suppressed" in invalid_reasons


def test_negative_agibilita_creates_real_issue_and_beats_legal_fallback():
    result = {
        "field_states": {},
        "dati_certi_del_lotto": {},
        "document_quality": {"status": "TEXT_OK"},
        "semaforo_generale": {"status": "AMBER"},
    }
    pages = [
        {
            "page_number": 1,
            "text": (
                "FORMALITÀ DA CANCELLARE CON IL DECRETO DI TRASFERIMENTO. "
                "Ipoteca volontaria iscritta a carico della procedura."
            ),
        },
        {
            "page_number": 2,
            "text": (
                "REGOLARITÀ EDILIZIA. L'immobile non risulta agibile. "
                "Non risulta rilasciato il certificato di agibilità."
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_negative_agibilita_priority",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    canonical = payload["canonical_case"]
    assert canonical["agibilita"]["status"] == "ASSENTE"
    assert canonical["priority"]["top_issue"]["code"] == "AGIBILITA_NEGATIVE"
    assert canonical["priority"]["top_issue"]["category"] == "agibilita"
    assert canonical["summary_bundle"]["decision_summary_it"] != "Formalità da cancellare. Verifica che il decreto di trasferimento disponga la cancellazione delle formalità indicate."


def test_valuation_candidates_classify_common_pricing_roles():
    rows = [
        {
            "page": 1,
            "amount_eur": 64198.0,
            "quote": "Lotto 1 - Prezzo base d'asta: € 64.198,00",
            "context": "Schema riassuntivo Lotto 1 - Prezzo base d'asta: € 64.198,00",
        },
        {
            "page": 2,
            "amount_eur": 80248.0,
            "quote": "Valore di stima del bene: € 80.248,00",
            "context": "L'immobile viene posto in vendita per il diritto di Proprietà (1/1) Valore di stima del bene: € 80.248,00",
        },
        {
            "page": 3,
            "amount_eur": 224268.0,
            "quote": "Valore finale di stima: € 224.268,00",
            "context": "Deprezzamenti Altro 20,00 % Valore finale di stima: € 224.268,00",
        },
    ]
    with tempfile.TemporaryDirectory() as tmpdir:
        runs_root = Path(tmpdir)
        analysis_dir = runs_root / "synthetic_pricing_roles" / "candidates"
        analysis_dir.mkdir(parents=True)
        (analysis_dir / "candidates_money.json").write_text(json.dumps(rows), encoding="utf-8")
        old_runs_root = valuation_table_tool.RUNS_ROOT
        try:
            valuation_table_tool.RUNS_ROOT = runs_root
            candidates = valuation_table_tool.valuation_candidates("synthetic_pricing_roles")
        finally:
            valuation_table_tool.RUNS_ROOT = old_runs_root
    by_role = {cand.semantic_role: cand for cand in candidates}
    assert by_role["auction_price"].value == 64198.0
    assert by_role["valuation_total"].value == 80248.0
    assert by_role["net_valuation"].value == 224268.0


def test_valuation_candidates_reject_table_ratio_contamination_from_totals():
    rows = [
        {
            "page": 1,
            "amount_eur": 1.0,
            "quote": "Bene N° 1 ... € 129.312,00 1/1 € 129.312,00 Valore di stima: € 129.312,00",
            "context": "Identificativo corpo Valore complessivo Quota invendita Totale Bene N° 1 ... € 129.312,00 1/1 € 129.312,00 Valore di stima: € 129.312,00",
        },
        {
            "page": 1,
            "amount_eur": 100.0,
            "quote": "€ 129.312,00 100,00% € 129.312,00 Valore di stima: € 129.312,00",
            "context": "Identificativo corpo Valore complessivo Quota invendita Totale ... 100,00% € 129.312,00 Valore di stima: € 129.312,00",
        },
        {
            "page": 1,
            "amount_eur": 129312.0,
            "quote": "Valore di stima del bene: € 129.312,00",
            "context": "L'immobile viene posto in vendita per il diritto di Proprietà (1/1) Valore di stima del bene: € 129.312,00",
        },
    ]
    with tempfile.TemporaryDirectory() as tmpdir:
        runs_root = Path(tmpdir)
        analysis_dir = runs_root / "synthetic_ratio_noise" / "candidates"
        analysis_dir.mkdir(parents=True)
        (analysis_dir / "candidates_money.json").write_text(json.dumps(rows), encoding="utf-8")
        old_runs_root = valuation_table_tool.RUNS_ROOT
        try:
            valuation_table_tool.RUNS_ROOT = runs_root
            candidates = valuation_table_tool.valuation_candidates("synthetic_ratio_noise")
        finally:
            valuation_table_tool.RUNS_ROOT = old_runs_root
    invalid_reasons = {cand.invalid_reason for cand in candidates if not cand.valid}
    totals = [cand.value for cand in candidates if cand.valid and cand.semantic_role == "valuation_total"]
    assert "valuation_table_ratio_contamination" in invalid_reasons
    assert totals == [129312.0]


def test_valuation_candidates_reject_unit_price_contamination_from_benchmark_totals():
    rows = [
        {
            "page": 1,
            "amount_eur": 1300.0,
            "quote": "Valore unitario (Vu) € 1.300,00",
            "context": "VALORI Valore unitario (Vu) € 1.300,00 Valore complessivo (VC) € 56.861,33",
        },
        {
            "page": 1,
            "amount_eur": 56861.33,
            "quote": "Valore complessivo (VC) € 56.861,33",
            "context": "VALORI Valore unitario (Vu) € 1.300,00 Valore complessivo (VC) € 56.861,33",
        },
    ]
    with tempfile.TemporaryDirectory() as tmpdir:
        runs_root = Path(tmpdir)
        analysis_dir = runs_root / "synthetic_unit_price_noise" / "candidates"
        analysis_dir.mkdir(parents=True)
        (analysis_dir / "candidates_money.json").write_text(json.dumps(rows), encoding="utf-8")
        old_runs_root = valuation_table_tool.RUNS_ROOT
        try:
            valuation_table_tool.RUNS_ROOT = runs_root
            candidates = valuation_table_tool.valuation_candidates("synthetic_unit_price_noise")
        finally:
            valuation_table_tool.RUNS_ROOT = old_runs_root
    invalid_reasons = {cand.invalid_reason for cand in candidates if not cand.valid}
    totals = [cand.value for cand in candidates if cand.valid and cand.semantic_role == "valuation_total"]
    assert "unit_price_contamination" in invalid_reasons
    assert totals == [56861.33]


def test_pricing_invariant_selected_price_requires_executable_evidence():
    mantova = _pricing_probe("mantova")
    multibene = _pricing_probe("multibene_1859886")
    rmei = _pricing_probe("rmei_928_2022")
    multilot = _pricing_probe("multilot_69_2024")
    assert mantova["selected_price"] is None
    assert multibene["selected_price"] == 391849.0
    assert rmei["selected_price"] == 172000.0
    assert multilot["selected_price"] is None


def test_pricing_invariant_benchmark_only_carries_single_gross_valuation():
    mantova = _pricing_probe("mantova")
    silvabella_payload = run_quality_verifier(
        analysis_id="analysis_8954c511ed4e",
        result=_silvabella_fixture()[0],
        pages=_silvabella_fixture()[1],
        full_text="\n\n".join(page["text"] for page in _silvabella_fixture()[1]),
    )
    silvabella = silvabella_payload["canonical_case"]["pricing"]
    multibene = _pricing_probe("multibene_1859886")
    rmei = _pricing_probe("rmei_928_2022")
    multilot = _pricing_probe("multilot_69_2024")
    assert mantova["benchmark_value"] == 129312.0
    assert silvabella["benchmark_value"] == 56861.33
    assert multibene["benchmark_value"] == 419849.0
    assert rmei["benchmark_value"] == 312708.0
    assert multilot["benchmark_value"] is None


def test_pricing_invariant_adjusted_market_value_requires_distinct_intermediate_layer():
    mantova = _pricing_probe("mantova")
    silvabella_payload = run_quality_verifier(
        analysis_id="analysis_8954c511ed4e",
        result=_silvabella_fixture()[0],
        pages=_silvabella_fixture()[1],
        full_text="\n\n".join(page["text"] for page in _silvabella_fixture()[1]),
    )
    silvabella = silvabella_payload["canonical_case"]["pricing"]
    multilot = _pricing_probe("multilot_69_2024")
    assert mantova["adjusted_market_value"] is None
    assert silvabella["adjusted_market_value"] == 53339.39
    assert multilot["adjusted_market_value"] is None


def test_pricing_invariant_single_root_aggregate_benchmark_survives_component_values():
    pricing = _pricing_probe("multibene_1859886")
    assert pricing["benchmark_value"] == 419849.0


def test_pricing_invariant_multi_lot_root_scalars_are_suppressed():
    pricing = _pricing_probe("multilot_69_2024")
    invalid_reasons = {item["reason"] for item in pricing["invalid_candidates"]}
    assert pricing["selected_price"] is None
    assert pricing["benchmark_value"] is None
    assert "multi_lot_scalar_price_suppressed" in invalid_reasons
    assert "multi_lot_scalar_benchmark_suppressed" in invalid_reasons


def test_mantova_plain_stima_populates_benchmark_not_selected_price():
    pricing = _pricing_probe("mantova")
    assert pricing["selected_price"] is None
    assert pricing["benchmark_value"] == 129312.0
    assert pricing["adjusted_market_value"] is None
    assert pricing["absurdity_guard_triggered"] is False


def test_multibene_explicit_base_and_gross_stima_split_selected_from_benchmark():
    pricing = _pricing_probe("multibene_1859886")
    assert pricing["selected_price"] == 391849.0
    assert pricing["benchmark_value"] == 419849.0
    assert pricing["adjusted_market_value"] is None
    assert pricing["absurdity_guard_triggered"] is False


def test_rmei_explicit_base_and_gross_stima_split_selected_from_benchmark():
    pricing = _pricing_probe("rmei_928_2022")
    assert pricing["selected_price"] == 172000.0
    assert pricing["benchmark_value"] == 312708.0
    assert pricing["adjusted_market_value"] is None
    assert pricing["absurdity_guard_triggered"] is False


def test_multilot_document_root_selected_price_stays_null():
    pricing = _pricing_probe("multilot_69_2024")
    invalid_reasons = {item["reason"] for item in pricing["invalid_candidates"]}
    assert pricing["selected_price"] is None
    assert pricing["benchmark_value"] is None
    assert pricing["adjusted_market_value"] is None
    assert pricing["absurdity_guard_triggered"] is False
    assert "multi_lot_scalar_price_suppressed" in invalid_reasons
    assert "multi_lot_scalar_benchmark_suppressed" in invalid_reasons


def test_torino_out_of_sample_pricing_layers_are_distinct_when_pdf_is_available():
    pdf_path = Path("/home/syedtajmeelshah/Torino, Via Marchese Visconti 6_1.pdf")
    if not pdf_path.exists():
        return
    raw = subprocess.run(["pdftotext", str(pdf_path), "-"], capture_output=True, text=True, check=True).stdout
    pages = [
        {"page_number": idx, "text": chunk}
        for idx, chunk in enumerate(raw.split("\f"), start=1)
        if chunk.strip()
    ]
    result = {
        "field_states": {},
        "dati_certi_del_lotto": {},
        "document_quality": {"status": "TEXT_OK"},
        "semaforo_generale": {"status": "AMBER"},
    }
    payload = run_quality_verifier(
        analysis_id="torino_via_marchese_visconti_6_1",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    pricing = payload["canonical_case"]["pricing"]
    assert pricing["benchmark_value"] == 43654.20
    assert pricing["adjusted_market_value"] == 38404.20
    assert pricing["selected_price"] == 38110.20
