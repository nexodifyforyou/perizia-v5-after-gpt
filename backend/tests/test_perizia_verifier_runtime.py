import math
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import perizia_agents.agibilita_agent as agibilita_agent
import perizia_runtime.runtime as verifier_runtime_module
from perizia_canonical_pipeline.doc_map_freeze import _build_field_entry
from perizia_canonical_pipeline.llm_clarification_issue_pack import build_clarification_issue_pack
from perizia_canonical_pipeline.llm_resolution_pack import select_prioritized_issues
from perizia_canonical_pipeline.llm_resolution_pack import _validate_resolution
from perizia_canonical_pipeline.trace_single_case import _build_llm_call_trace
from perizia_ingest.readability_gate import READABLE_BUT_EXTRACTION_BAD, READABLE_DOCUMENT, UNREADABLE_FROM_AVAILABLE_SURFACES
from perizia_qa.fixture_runner import run_named_fixture
from perizia_runtime.evidence_mode import DEGRADED_TEXT, STOP_UNREADABLE, TEXT_FIRST
from perizia_runtime.runtime import apply_verifier_to_result, run_quality_verifier
from perizia_runtime.state import RuntimeState
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


def _force_readability_mode(monkeypatch, pages, verdict: str):
    readability = verifier_runtime_module.assess_document_readability(pages)
    readability["readability_verdict"] = verdict
    monkeypatch.setattr(verifier_runtime_module, "assess_document_readability", lambda _: readability)


def _clarification_issue(case_key: str, *, field_family: str, source_pages: list[int], lot_id=None, bene_id=None):
    pack = build_clarification_issue_pack(case_key)
    for issue in pack["issues"]:
        if issue.get("field_family") != field_family:
            continue
        if issue.get("source_pages") != source_pages:
            continue
        if lot_id is not None and issue.get("lot_id") != lot_id:
            continue
        if bene_id is not None and issue.get("bene_id") != bene_id:
            continue
        return issue
    raise AssertionError(
        f"Missing clarification issue for case={case_key} family={field_family} "
        f"pages={source_pages} lot_id={lot_id} bene_id={bene_id}"
    )


def test_runtime_state_initializes_document_root_scope():
    state = RuntimeState(
        analysis_id="scope_init",
        result={},
        pages=[],
        full_text="",
    )
    assert "document_root" in state.scopes
    root = state.scopes["document_root"]
    assert root.scope_type == "document_root"
    assert root.parent_scope_id is None
    assert root.label == "Document Root"


def test_runtime_state_can_create_child_scopes_with_parent_links():
    state = RuntimeState(
        analysis_id="scope_children",
        result={},
        pages=[],
        full_text="",
    )
    state.get_or_create_scope("lotto:1", scope_type="lotto", parent_scope_id="document_root", label="Lotto 1")
    state.get_or_create_scope("bene:1", scope_type="bene", parent_scope_id="lotto:1", label="Bene 1")
    children = state.list_child_scopes("document_root")
    assert [scope.scope_id for scope in children] == ["lotto:1"]
    assert state.scope_path("bene:1") == ["document_root", "lotto:1", "bene:1"]


def test_runtime_state_can_attach_evidence_ownership_to_scope():
    state = RuntimeState(
        analysis_id="scope_evidence",
        result={},
        pages=[],
        full_text="",
    )
    state.get_or_create_scope("lotto:1", scope_type="lotto", parent_scope_id="document_root", label="Lotto 1")
    ownership = state.attach_evidence_ownership(
        scope_id="lotto:1",
        field_target="pricing.selected_price",
        source_page=7,
        quote="Lotto 1 - Prezzo base d'asta: € 64.198,00",
        confidence=0.98,
        ownership_method="heading_propagation",
        evidence_id="ev_test_price",
    )
    assert ownership.evidence_id == "ev_test_price"
    assert ownership.scope_id == "lotto:1"
    assert ownership.scope_path == ["document_root", "lotto:1"]
    assert state.scopes["lotto:1"].evidence_ids == ["ev_test_price"]
    assert state.evidence_ownership["ev_test_price"].field_target == "pricing.selected_price"


def test_structure_agent_discovers_single_lot_single_bene_scope_tree():
    result = {
        "field_states": {},
        "dati_certi_del_lotto": {},
        "document_quality": {"status": "TEXT_OK"},
        "semaforo_generale": {"status": "AMBER"},
    }
    pages = [
        {
            "page_number": 1,
            "text": "LOTTO UNICO\nBene N° 1 - Appartamento\nValore di stima del bene: € 129.312,00",
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_scope_single",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    scopes = payload["scopes"]
    assert "document_root" in scopes
    assert "lotto:unico" in scopes
    assert "bene:1" in scopes
    assert scopes["lotto:unico"]["parent_scope_id"] == "document_root"
    assert scopes["bene:1"]["parent_scope_id"] == "lotto:unico"
    assert scopes["bene:1"]["metadata"]["ownership_method"] == "nearest_lotto_heading"


def test_runtime_propagates_full_freeze_contract_into_result(monkeypatch):
    contract = {
        "fields": {
            "field::document::extra_costs": {
                "state": "unresolved_explained",
                "field_type": "extra_costs",
                "source_pages": [3],
                "explanation": "La perizia menziona il costo, ma non fornisce un importo finale sicuro.",
                "why_not_resolved": "Il pacchetto bounded non contiene un importo normalizzato conclusivo.",
                "confidence_band": "low",
                "needs_human_review": True,
            }
        },
        "unresolved_items": [],
        "blocked_items": [],
        "grouped_llm_explanations": [{"field_family": "extra_costs", "user_visible_explanation": "copy"}],
    }
    monkeypatch.setattr(verifier_runtime_module, "_load_freeze_contract", lambda _: contract)
    pages = [{"page_number": 1, "text": "Documento sintetico con testo sufficiente per il controllo di leggibilita. Lotto unico e dati disponibili."}]
    _force_readability_mode(monkeypatch, pages, READABLE_DOCUMENT)
    payload = run_quality_verifier(
        analysis_id="freeze_contract_runtime",
        result={"field_states": {}, "pdf_sha256": "abc"},
        pages=pages,
        full_text=pages[0]["text"],
        pdf_sha256="abc",
    )
    result = {}
    apply_verifier_to_result(result, payload)

    assert result["canonical_freeze_contract"]["fields"]["field::document::extra_costs"]["state"] == "unresolved_explained"
    assert result["canonical_freeze_explanations"][0]["field_family"] == "extra_costs"


def test_blocked_unreadable_freeze_contract_scrubs_visible_placeholders(monkeypatch):
    contract = {
        "case_key": "via_del_mare_4591_4593",
        "status": "BLOCKED_UNREADABLE",
        "freeze_status": "blocked_unreadable",
        "blocked_items": [
            {
                "reason": "Document quality is BLOCKED_UNREADABLE",
                "freeze_status": "blocked_unreadable",
            }
        ],
    }
    monkeypatch.setattr(verifier_runtime_module, "_load_freeze_contract", lambda _: contract)
    pages = [{"page_number": 1, "text": "Documento sintetico con testo sufficiente per il controllo di leggibilita. Lotto unico e dati disponibili."}]
    _force_readability_mode(monkeypatch, pages, READABLE_DOCUMENT)
    payload = run_quality_verifier(
        analysis_id="blocked_freeze_runtime",
        result={"field_states": {}, "pdf_sha256": "abc"},
        pages=pages,
        full_text=pages[0]["text"],
        pdf_sha256="abc",
    )
    result = {
        "lots": [
            {
                "prezzo_base_eur": "TBD",
                "ubicazione": "TBD",
                "superficie_mq": "TBD",
                "diritto_reale": "TBD",
                "diritto": "TBD",
            }
        ],
        "lot_index": [{"prezzo": "TBD", "ubicazione": "TBD"}],
        "money_box": {
            "items": [{"type": "TBD", "stima_euro": "TBD", "source": "TBD"}],
            "total_extra_costs": {"range": {"min": "TBD", "max": "TBD"}},
        },
        "section_3_money_box": {
            "items": [{"type": "TBD", "stima_euro": "TBD", "source": "TBD"}],
            "totale_extra_budget": {"min": "TBD", "max": "TBD"},
        },
        "indice_di_convenienza": {
            "extra_costs_min": "TBD",
            "extra_costs_max": "TBD",
            "all_in_light_min": "TBD",
            "all_in_light_max": "TBD",
        },
    }
    apply_verifier_to_result(result, payload)

    assert result["analysis_status"] == "UNREADABLE"
    assert result["canonical_contract_state"]["reason"] == "canonical_freeze_blocked_unreadable"
    assert result["lots"][0]["prezzo_base_eur"] is None
    assert result["lots"][0]["field_contract_metadata"]["prezzo_base_eur"]["state"] == "blocked"
    assert result["lot_index"][0]["prezzo"] is None
    assert result["money_box"]["items"][0]["stima_euro"] is None
    assert result["money_box"]["total_extra_costs"]["range"]["min"] is None
    assert result["section_3_money_box"]["totale_extra_budget"]["min"] is None
    assert result["indice_di_convenienza"]["all_in_light_min"] is None
    assert "TBD" not in json.dumps(result)


def test_llm_validation_preserves_qualified_resolution_as_context_value():
    issue = {
        "issue_id": "case::occupancy::0001",
        "case_key": "case",
        "field_family": "occupancy",
        "field_type": "occupancy_status_raw",
        "lot_id": "1",
        "issue_type": "GROUPED_CONTEXT_NEEDS_EXPLANATION",
        "candidate_values": ["LIBERO"],
        "source_pages": [12, 15],
        "supporting_candidates": [{"extracted_value": "LIBERO", "page": 12, "quote": "Lotto 1 risulta libero"}],
        "local_text_windows": [{"page": 15, "text": "Liberazione a cura della procedura e occupazione saltuaria non opponibile"}],
        "shell_quotes": ["Lotto 1 risulta libero"],
    }
    raw = {
        "llm_outcome": "upgraded_context",
        "resolution_mode": "qualified_resolution",
        "resolved_value": "LIBERO",
        "resolved_value_type": "occupancy_status_raw",
        "context_qualification": "Libero, con nota su occupazione saltuaria e liberazione a cura della procedura.",
        "why_not_fully_certain": "Una resa come solo LIBERO perderebbe la qualifica indicata a pagina 15.",
        "why_not_resolved": "Il valore è usabile solo con qualifica.",
        "user_visible_explanation": "Per Lotto 1, stato di occupazione: il passaggio “Lotto 1 risulta libero” sostiene LIBERO, ma pagina 15 richiede contesto su liberazione a cura della procedura.",
        "source_pages": [12, 15],
        "supporting_pages": [12],
        "tension_pages": [15],
        "confidence_band": "medium",
        "needs_human_review": True,
    }

    resolution = _validate_resolution(issue, raw, "test", "model")

    assert resolution["llm_outcome"] == "upgraded_context"
    assert resolution["resolution_mode"] == "qualified_resolution"
    assert resolution["resolved_value"] == "LIBERO"
    assert resolution["context_qualification"]
    assert resolution["why_not_fully_certain"]


def test_freeze_maps_qualified_resolution_to_resolved_with_context():
    issue = {
        "issue_id": "case::occupancy::0001",
        "field_type": "occupancy_status_raw",
        "issue_type": "GROUPED_CONTEXT_NEEDS_EXPLANATION",
        "needs_llm": True,
    }
    resolution = {
        "issue_id": issue["issue_id"],
        "llm_outcome": "upgraded_context",
        "resolution_mode": "qualified_resolution",
        "resolved_value": "LIBERO",
        "resolved_value_type": "occupancy_status_raw",
        "confidence_band": "medium",
        "supporting_evidence": [{"page": 12, "quote": "Lotto 1 risulta libero"}],
        "supporting_pages": [12],
        "tension_pages": [15],
        "user_visible_explanation": "Libero con qualifica da verificare sulle pagine 12 e 15.",
        "context_qualification": "Libero, ma con qualifica su liberazione e occupazione saltuaria.",
        "why_not_fully_certain": "Il valore nudo perderebbe il contesto della pagina 15.",
        "why_not_resolved": "Il valore richiede qualifica.",
        "needs_human_review": True,
    }

    entry = _build_field_entry(
        scope_key="lot:1",
        field_type="occupancy_status_raw",
        active_packets=[],
        context_packets=[],
        blocked_zones=[],
        issues=[issue],
        resolutions=[resolution],
        warnings=[],
    )

    assert entry["state"] == "resolved_with_context"
    assert entry["value"] == "LIBERO"
    assert entry["context_qualification"]
    assert entry["why_not_fully_certain"]


def test_structure_agent_discovers_multiple_lotto_scopes():
    result = {
        "field_states": {},
        "dati_certi_del_lotto": {},
        "document_quality": {"status": "TEXT_OK"},
        "semaforo_generale": {"status": "AMBER"},
    }
    pages = [
        {
            "page_number": 1,
            "text": "LOTTO 1\nBene N° 1 - Appartamento\nLOTTO 2\nBene N° 2 - Garage",
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_scope_multilot",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    scopes = payload["scopes"]
    assert "lotto:1" in scopes
    assert "lotto:2" in scopes
    assert scopes["lotto:1"]["parent_scope_id"] == "document_root"
    assert scopes["lotto:2"]["parent_scope_id"] == "document_root"
    assert scopes["bene:1"]["parent_scope_id"] == "lotto:1"
    assert scopes["bene:2"]["parent_scope_id"] == "lotto:2"


def test_structure_agent_discovers_multi_bene_fixture_scopes():
    result, pages = _repo_fixture("multibene_1859886")
    payload = run_quality_verifier(
        analysis_id="multibene_scope_probe",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    scopes = payload["scopes"]
    bene_scope_ids = sorted(scope_id for scope_id, scope in scopes.items() if scope["scope_type"] == "bene")
    assert len(bene_scope_ids) >= 4
    assert "bene:1" in bene_scope_ids
    assert "bene:2" in bene_scope_ids
    assert "bene:3" in bene_scope_ids
    assert "bene:4" in bene_scope_ids
    assert payload["scopes"]["bene:4"]["parent_scope_id"] == "lotto:unico"


def test_structure_agent_parents_bene_to_lotto_when_structure_supports_it():
    result, pages = _repo_fixture("multilot_69_2024")
    payload = run_quality_verifier(
        analysis_id="multilot_scope_probe",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    scopes = payload["scopes"]
    assert "lotto:1" in scopes
    assert "lotto:2" in scopes
    assert "lotto:3" in scopes
    assert scopes["bene:1"]["parent_scope_id"] == "lotto:1"
    assert scopes["bene:2"]["parent_scope_id"] == "lotto:2"
    assert scopes["bene:3"]["parent_scope_id"] == "lotto:3"


def test_multilot_69_live_api_lots_keep_lot_scoped_contract_fields():
    import server  # type: ignore[import]

    result, pages = _repo_fixture("multilot_69_2024")

    # Simulate the customer-facing API read path, where persisted analyses are
    # refreshed before being returned from /api/analysis/perizia/{analysis_id}.
    server._refresh_customer_facing_result_on_read(
        result,
        pages,
        analysis_id="multilot_69_2024",
    )
    live_payload = {"ok": True, "analysis_id": "multilot_69_2024", "result": result}
    lots = live_payload["result"].get("lots")

    assert isinstance(lots, list)
    assert len(lots) == 3
    assert [lot.get("lot_id") for lot in lots] == ["1", "2", "3"]
    assert [lot.get("prezzo_base_asta") for lot in lots] == [64198.0, 84000.0, 224268.0]
    assert [lot.get("valore_stima") for lot in lots] == [80248, 105000, 280336]
    assert [lot.get("quota") for lot in lots] == ["1/1", "1/1", "1/1"]
    assert all(lot.get("diritto") for lot in lots)
    assert all(lot.get("titolo") for lot in lots)

    # The document-level field_state may keep the selected first-lot value for
    # backward compatibility, but it must not flatten over the per-lot facts.
    assert (result.get("field_states") or {}).get("prezzo_base_asta", {}).get("value") == 64198.0
    assert len({lot.get("prezzo_base_asta") for lot in lots}) == 3
    assert len({lot.get("valore_stima") for lot in lots}) == 3


def test_agibilita_negative_writes_to_bene_scope_first():
    result = {
        "field_states": {},
        "dati_certi_del_lotto": {},
        "document_quality": {"status": "TEXT_OK"},
        "semaforo_generale": {"status": "AMBER"},
    }
    pages = [
        {
            "page_number": 1,
            "text": "LOTTO UNICO\nBene N° 1 - Appartamento\nL'immobile non risulta agibile.",
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_agibilita_bene_negative",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    bene = payload["scopes"]["bene:1"]["agibilita"]
    assert bene["status"] == "ASSENTE"
    assert bene["issue_code"] == "AGIBILITA_NEGATIVE"
    ownership = list(payload["evidence_ownership"].values())
    assert any(item["scope_id"] == "bene:1" and item["field_target"] == "agibilita" for item in ownership)


def test_agibilita_positive_writes_to_bene_scope_first():
    result = {
        "field_states": {},
        "dati_certi_del_lotto": {},
        "document_quality": {"status": "TEXT_OK"},
        "semaforo_generale": {"status": "AMBER"},
    }
    pages = [
        {
            "page_number": 1,
            "text": "LOTTO UNICO\nBene N° 1 - Appartamento\nL'immobile risulta agibile.",
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_agibilita_bene_positive",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    bene = payload["scopes"]["bene:1"]["agibilita"]
    assert bene["status"] == "PRESENTE"
    assert payload["canonical_case"]["agibilita"]["status"] == "PRESENTE"


def test_agibilita_mixed_across_beni_is_not_same_scope_conflict():
    result = {
        "field_states": {},
        "dati_certi_del_lotto": {},
        "document_quality": {"status": "TEXT_OK"},
        "semaforo_generale": {"status": "AMBER"},
    }
    pages = [
        {
            "page_number": 1,
            "text": "LOTTO UNICO\nBene N° 1 - Appartamento\nL'immobile non risulta agibile.\nBene N° 2 - Garage\nL'immobile risulta agibile.",
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_agibilita_mixed_beni",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    assert payload["scopes"]["bene:1"]["agibilita"]["status"] == "ASSENTE"
    assert payload["scopes"]["bene:2"]["agibilita"]["status"] == "PRESENTE"
    assert payload["canonical_case"]["agibilita"]["status"] == "NON_VERIFICABILE"
    assert "mixed_scope_agibilita_non_collapsible" in payload["canonical_case"]["agibilita"]["guards"]
    assert payload["scopes"]["bene:1"]["metadata"]["agibilita_internal"]["raw_conflict_detected"] is False
    assert payload["scopes"]["bene:2"]["metadata"]["agibilita_internal"]["raw_conflict_detected"] is False
    trail = payload["canonical_case"]["agibilita"]["verification_trail"]
    assert trail["reason_unresolved"] == "truth differs by scope"
    assert trail["checked_pages"] == [1]
    assert trail["key_evidence_found"]
    assert trail["verify_next"]
    assert trail["checked_scope_label"] == "Documento"


def test_agibilita_same_scope_explicit_negative_beats_positive_and_resolves_assente():
    result = {
        "field_states": {},
        "dati_certi_del_lotto": {},
        "document_quality": {"status": "TEXT_OK"},
        "semaforo_generale": {"status": "AMBER"},
    }
    pages = [
        {
            "page_number": 1,
            "text": "LOTTO UNICO\nBene N° 1 - Appartamento\nL'immobile non risulta agibile.\nL'immobile risulta agibile.",
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_agibilita_same_scope_negative_wins",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    scoped = payload["scopes"]["bene:1"]["agibilita"]
    assert scoped["status"] == "ASSENTE"
    assert "resolver_meta" not in scoped
    assert "verification_trail" not in scoped
    assert payload["scopes"]["bene:1"]["metadata"]["agibilita_internal"]["raw_conflict_detected"] is True
    assert payload["scopes"]["bene:1"]["metadata"]["agibilita_internal"]["resolution_reason"] == "higher_tier_negative_beats_positive"
    assert payload["canonical_case"]["agibilita"]["status"] == "ASSENTE"


def test_agibilita_same_scope_local_positive_beats_inherited_negative():
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
                "LOTTO UNICO\nBene N° 1 - Appartamento\n"
                "Tutti i beni del lotto unico non risultano agibili.\n"
                "Bene N° 1 - Appartamento\nL'immobile risulta agibile."
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_agibilita_local_positive_beats_inherited_negative",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    scoped = payload["scopes"]["bene:1"]["agibilita"]
    assert scoped["status"] == "PRESENTE"
    assert "resolver_meta" not in scoped
    assert "verification_trail" not in scoped
    assert payload["scopes"]["bene:1"]["metadata"]["agibilita_internal"]["raw_conflict_detected"] is True
    assert payload["scopes"]["bene:1"]["metadata"]["agibilita_internal"]["resolution_reason"] == "higher_tier_positive_beats_negative"
    assert payload["canonical_case"]["agibilita"]["status"] == "PRESENTE"


def test_agibilita_same_scope_unresolved_ambiguity_becomes_non_verificabile_not_conflict():
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
                "LOTTO UNICO\nBene N° 1 - Appartamento\n"
                "Tutti i beni del lotto unico non risultano agibili.\n"
                "Tutti i beni del lotto unico: l'immobile risulta agibile."
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_agibilita_same_scope_unresolved_ambiguity",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    scoped = payload["scopes"]["bene:1"]["agibilita"]
    assert scoped["status"] == "NON_VERIFICABILE"
    assert "resolver_meta" not in scoped
    assert payload["scopes"]["bene:1"]["metadata"]["agibilita_internal"]["raw_conflict_detected"] is True
    assert payload["scopes"]["bene:1"]["metadata"]["agibilita_internal"]["unresolved_reason"] == "same_scope_conflict_survives_expanded_reading"
    trail = scoped["verification_trail"]
    assert trail["checked_scope_label"] == "Bene 1"
    assert trail["checked_pages"] == [1]
    assert trail["key_evidence_found"]
    assert trail["verify_next"]
    assert payload["canonical_case"]["agibilita"]["status"] == "NON_VERIFICABILE"
    assert payload["canonical_case"]["agibilita"]["status"] != "CONFLITTO"


def test_agibilita_parent_scope_universal_statement_inherits_to_children():
    result = {
        "field_states": {},
        "dati_certi_del_lotto": {},
        "document_quality": {"status": "TEXT_OK"},
        "semaforo_generale": {"status": "AMBER"},
    }
    pages = [
        {
            "page_number": 1,
            "text": "LOTTO UNICO\nBene N° 1 - Appartamento\nBene N° 2 - Garage\nTutti i beni del lotto unico non risultano agibili.",
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_agibilita_inheritance",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    bene1 = payload["scopes"]["bene:1"]["agibilita"]
    bene2 = payload["scopes"]["bene:2"]["agibilita"]
    assert bene1["status"] == "ASSENTE"
    assert bene2["status"] == "ASSENTE"
    internal = payload["scopes"]["bene:1"]["metadata"]["agibilita_internal"]
    assert internal["winner_inherited"] is True
    assert internal["winner_inherited_from_scope_id"] == "lotto:unico"


def test_agibilita_inherited_parent_statement_loses_to_explicit_local_contrary_evidence():
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
                "LOTTO UNICO\nBene N° 1 - Appartamento\n"
                "Tutti i beni del lotto unico non risultano agibili.\n"
                "Bene N° 1 - Appartamento\nL'immobile risulta agibile."
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_agibilita_inherited_loses_to_local",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    scoped = payload["scopes"]["bene:1"]["agibilita"]
    assert scoped["status"] == "PRESENTE"
    assert payload["scopes"]["bene:1"]["metadata"]["agibilita_internal"]["resolution_reason"] == "higher_tier_positive_beats_negative"


def test_agibilita_root_output_remains_backward_compatible_for_negative_case():
    result = {
        "field_states": {},
        "dati_certi_del_lotto": {},
        "document_quality": {"status": "TEXT_OK"},
        "semaforo_generale": {"status": "AMBER"},
    }
    pages = [
        {
            "page_number": 1,
            "text": "REGOLARITÀ EDILIZIA. Lotto 2. L'immobile non risulta agibile.",
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_agibilita_root_compat",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    root = payload["canonical_case"]["agibilita"]
    assert root["status"] == "ASSENTE"
    assert root["issue_code"] == "AGIBILITA_NEGATIVE"


def test_agibilita_root_never_exposes_raw_conflict():
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
                "LOTTO UNICO\nBene N° 1 - Appartamento\n"
                "Tutti i beni del lotto unico non risultano agibili.\n"
                "Tutti i beni del lotto unico risultano agibili."
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_agibilita_root_never_conflict",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    assert payload["canonical_case"]["agibilita"]["status"] != "CONFLITTO"
    assert payload["canonical_case"]["agibilita"]["verification_trail"]["verify_next"]


def test_agibilita_multi_bene_same_lotto_keeps_bene_ownership_and_does_not_create_false_lotto_assente():
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
                "LOTTO UNICO\n"
                "Bene N° 1 - Appartamento\nL'immobile non risulta agibile.\n"
                "Bene N° 2 - Garage\nL'immobile risulta agibile."
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_agibilita_bene_stickiness",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    assert payload["scopes"]["bene:1"]["agibilita"]["status"] == "ASSENTE"
    assert payload["scopes"]["bene:2"]["agibilita"]["status"] == "PRESENTE"
    assert payload["scopes"]["lotto:unico"]["agibilita"] == {}


def test_agibilita_lot_wide_universal_wording_can_assign_to_lotto_scope():
    result = {
        "field_states": {},
        "dati_certi_del_lotto": {},
        "document_quality": {"status": "TEXT_OK"},
        "semaforo_generale": {"status": "AMBER"},
    }
    pages = [
        {
            "page_number": 1,
            "text": "LOTTO UNICO\nBene N° 1 - Appartamento\nBene N° 2 - Garage\nTutti i beni del lotto unico non risultano agibili.",
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_agibilita_lotto_universal_owner",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    assert payload["scopes"]["lotto:unico"]["agibilita"]["status"] == "ASSENTE"
    assert payload["scopes"]["bene:1"]["agibilita"]["status"] == "ASSENTE"
    assert payload["scopes"]["bene:2"]["agibilita"]["status"] == "ASSENTE"


def test_agibilita_single_scope_document_can_fall_back_safely():
    result = {
        "field_states": {},
        "dati_certi_del_lotto": {},
        "document_quality": {"status": "TEXT_OK"},
        "semaforo_generale": {"status": "AMBER"},
    }
    pages = [
        {
            "page_number": 1,
            "text": "L'immobile non risulta agibile.",
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_agibilita_single_scope_fallback",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    assert payload["canonical_case"]["agibilita"]["status"] == "ASSENTE"


def test_agibilita_root_checked_scope_label_is_buyer_readable():
    result = {
        "field_states": {},
        "dati_certi_del_lotto": {},
        "document_quality": {"status": "TEXT_OK"},
        "semaforo_generale": {"status": "AMBER"},
    }
    pages = [
        {
            "page_number": 1,
            "text": "L'immobile risulta agibile.\nL'immobile non risulta agibile.",
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_agibilita_root_scope_label",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    trail = payload["canonical_case"]["agibilita"]["verification_trail"]
    assert trail["checked_scope_label"] == "Documento"
    assert "checked_scope_id" not in trail


def test_agibilita_ownership_decision_can_become_unresolved_when_signals_are_too_close():
    state = RuntimeState(
        analysis_id="ownership_decision_ambiguity",
        result={},
        pages=[],
        full_text="",
    )
    state.get_or_create_scope("lotto:unico", scope_type="lotto", parent_scope_id="document_root", label="Lotto Unico")
    state.get_or_create_scope("bene:1", scope_type="bene", parent_scope_id="lotto:unico", label="Bene 1")
    state.get_or_create_scope("bene:2", scope_type="bene", parent_scope_id="lotto:unico", label="Bene 2")
    raw_text = "LOTTO UNICO\nBene N° 1 - Appartamento\nBene N° 2 - Garage\nL'immobile non risulta agibile."
    start = raw_text.lower().find("non risulta agibile")
    decision = agibilita_agent._build_ownership_decision(
        state,
        quote="Bene N° 1 - Appartamento\nBene N° 2 - Garage\nL'immobile non risulta agibile.",
        page_number=1,
        raw_text=raw_text,
        expanded_quote=raw_text,
        start=start,
    )
    assert decision.winning_scope is None
    assert decision.ownership_method == "UNRESOLVED"
    assert decision.unresolved_reason is not None


def test_agibilita_mixed_scope_root_rollup_no_longer_crashes_on_fixture():
    result, pages = _repo_fixture("multibene_1859886")
    payload = run_quality_verifier(
        analysis_id="multibene_agibilita_rollup_probe",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    root = payload["canonical_case"]["agibilita"]
    assert root["status"] == "NON_VERIFICABILE"
    assert root["verification_trail"]["reason_unresolved"] == "truth differs by scope"
    assert root["verification_trail"]["checked_pages"]
    assert payload["scopes"]["lotto:unico"]["agibilita"] == {}
    assert payload["scopes"]["bene:1"]["agibilita"]["status"] == "ASSENTE"
    assert payload["scopes"]["bene:4"]["agibilita"]["status"] == "PRESENTE"


def test_agibilita_client_facing_output_does_not_leak_internal_labels():
    result = {
        "field_states": {},
        "dati_certi_del_lotto": {},
        "document_quality": {"status": "TEXT_OK"},
        "semaforo_generale": {"status": "AMBER"},
    }
    pages = [
        {
            "page_number": 1,
            "text": "REGOLARITÀ EDILIZIA. Lotto 2. L'immobile non risulta agibile.",
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_agibilita_client_safety",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    client_text = json.dumps(
        {
            "root": payload["canonical_case"]["agibilita"],
            "scoped": payload["scopes"]["lotto:2"]["agibilita"],
        },
        ensure_ascii=False,
    )
    for token in ["explicit_negative", "explicit_positive", "certificate_missing", "inherited_negative", "scope_id", "evidence_id", "checked_scope_id"]:
        assert token not in client_text


def test_agibilita_client_facing_trail_uses_only_readable_field_names():
    result, pages = _repo_fixture("multibene_1859886")
    payload = run_quality_verifier(
        analysis_id="multibene_agibilita_trail_field_names",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    trail = payload["canonical_case"]["agibilita"]["verification_trail"]
    assert trail["checked_scope_label"] == "Documento"
    assert "checked_scope_id" not in trail
    assert set(trail.keys()) == {
        "checked_scope_label",
        "checked_pages",
        "checked_sections",
        "key_evidence_found",
        "reason_unresolved",
        "verify_next",
    }


def test_agibilita_torino_non_verificabile_semantics_unchanged_after_trail_cleanup():
    result, pages = _repo_fixture("torino_via_marchese_visconti")
    payload = run_quality_verifier(
        analysis_id="torino_agibilita_trail_cleanup_probe",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    root = payload["canonical_case"]["agibilita"]
    assert root["status"] == "NON_VERIFICABILE"
    assert root["verification_trail"]["checked_scope_label"] == "Documento"
    assert root["verification_trail"]["reason_unresolved"] == "no decisive certificate or explicit same-scope statement found"
    assert root["verification_trail"]["verify_next"]


def test_agibilita_internal_diagnostics_still_preserved_separately():
    result = {
        "field_states": {},
        "dati_certi_del_lotto": {},
        "document_quality": {"status": "TEXT_OK"},
        "semaforo_generale": {"status": "AMBER"},
    }
    pages = [
        {
            "page_number": 1,
            "text": "LOTTO UNICO\nBene N° 1 - Appartamento\nL'immobile non risulta agibile.\nL'immobile risulta agibile.",
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_agibilita_internal_diagnostics",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    internal = payload["scopes"]["bene:1"]["metadata"]["agibilita_internal"]
    assert internal["raw_conflict_detected"] is True
    assert internal["competing_evidence_ids"]
    assert internal["winning_evidence_tier"] == 2


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


def test_text_first_packaging_stays_clean_for_packaged_outputs(monkeypatch):
    result, pages = _silvabella_fixture()
    _force_readability_mode(monkeypatch, pages, READABLE_DOCUMENT)
    payload = run_quality_verifier(
        analysis_id="silvabella_text_first_packaging",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    packaged = json.loads(json.dumps(result))
    apply_verifier_to_result(packaged, payload)

    assert payload["evidence_mode"] == TEXT_FIRST
    assert payload["source_quality_note"] is None
    assert payload["packaging_guards"] == []
    assert packaged["document_quality"]["source_quality_note"] is None
    assert "packaging_guards" not in packaged
    assert "guards" not in packaged["dati_certi_del_lotto"]["prezzo_base_asta_verifier"]
    assert packaged["field_states"]["stato_occupativo"]["status"] == "FOUND"
    assert packaged["field_states"]["stato_occupativo"]["confidence"] > 0.6


def test_degraded_text_packaging_adds_guards_notes_and_caps_confidence(monkeypatch):
    result, pages = _silvabella_fixture()
    _force_readability_mode(monkeypatch, pages, READABLE_BUT_EXTRACTION_BAD)
    payload = run_quality_verifier(
        analysis_id="silvabella_degraded_text_packaging",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    packaged = json.loads(json.dumps(result))
    apply_verifier_to_result(packaged, payload)

    assert payload["evidence_mode"] == DEGRADED_TEXT
    assert payload["reasoning_status"] == "DEGRADED_TEXT_CAUTION"
    assert payload["source_quality_note"]
    assert payload["packaging_guards"] == ["degraded_source_text_only", "confidence_capped_due_to_extraction_quality"]
    assert payload["canonical_case"]["packaging_guards"] == ["degraded_source_text_only", "confidence_capped_due_to_extraction_quality"]
    assert payload["canonical_case"]["occupancy"]["confidence"] == 0.6
    assert packaged["source_quality_note"]
    assert packaged["packaging_guards"] == ["degraded_source_text_only", "confidence_capped_due_to_extraction_quality"]
    assert packaged["document_quality"]["source_quality_note"]
    assert packaged["dati_certi_del_lotto"]["prezzo_base_asta_verifier"]["guards"] == ["degraded_source_text_only", "confidence_capped_due_to_extraction_quality"]
    assert packaged["field_states"]["stato_occupativo"]["status"] == "LOW_CONFIDENCE"
    assert packaged["field_states"]["stato_occupativo"]["confidence"] == 0.6
    assert packaged["field_states"]["stato_occupativo"]["resolver_meta"]["source_quality_note"]


def test_stop_unreadable_still_suppresses_normal_packaging(monkeypatch):
    result, pages = _silvabella_fixture()
    _force_readability_mode(monkeypatch, pages, UNREADABLE_FROM_AVAILABLE_SURFACES)
    payload = run_quality_verifier(
        analysis_id="silvabella_stop_unreadable_packaging",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    packaged = json.loads(json.dumps(result))
    apply_verifier_to_result(packaged, payload)

    assert payload["evidence_mode"] == STOP_UNREADABLE
    assert payload["reasoning_status"] == "SUPPRESSED_UNREADABLE"
    assert payload["canonical_case"] == {}
    assert packaged["document_quality"]["reasoning_status"] == "SUPPRESSED_UNREADABLE"
    assert packaged["field_states"] == {}
    assert packaged["dati_certi_del_lotto"] == {}
    assert "stato_occupativo" not in packaged


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


def test_verifier_payload_exposes_scope_registry_without_breaking_root_outputs():
    result, pages = _silvabella_fixture()
    payload = run_quality_verifier(
        analysis_id="scope_registry_bridge",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    assert payload["canonical_case"]["pricing"]["selected_price"] == 45338.48
    assert "document_root" in payload["scopes"]
    root_scope = payload["scopes"]["document_root"]
    assert root_scope["scope_type"] == "document_root"
    assert root_scope["parent_scope_id"] is None
    assert payload["canonical_case"]["occupancy"]["status"] == "LIBERO"
    assert any(item["field_target"] == "occupancy" for item in payload["evidence_ownership"].values())


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


def test_summary_fallback_uses_case_specific_unresolved_signals():
    result, pages = _repo_fixture("torino_via_marchese_visconti")
    payload = run_quality_verifier(
        analysis_id="torino_summary_fallback",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    canonical = payload["canonical_case"]
    summary = canonical["summary_bundle"]
    decision_summary = str(summary.get("decision_summary_it") or "")
    next_step = str(summary.get("next_step_it") or "")

    assert canonical["priority"]["top_issue"] is None
    assert decision_summary != "Verifica manualmente i punti critici prima dell'offerta."
    assert "opponibilità" in decision_summary.lower()
    assert "segnale decisivo" in next_step.lower() or "agibilità" in next_step.lower()


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


def test_occupancy_writes_bene_scope_first_and_rolls_root_up_leaf_first():
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
                "LOTTO UNICO\n"
                "Bene N° 1 - Appartamento\nL'immobile risulta libero.\n"
                "Bene N° 2 - Garage\nL'immobile occupato da terzi."
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_occupancy_leaf_first_rollup",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    assert payload["scopes"]["bene:1"]["occupancy"]["status"] == "LIBERO"
    assert payload["scopes"]["bene:2"]["occupancy"]["status"] == "OCCUPATO"
    assert payload["canonical_case"]["occupancy"]["status"] == "NON_VERIFICABILE"
    assert payload["canonical_case"]["occupancy"]["status"] != "CONFLITTO"
    assert payload["scopes"]["document_root"]["metadata"]["occupancy_internal"]["unresolved_reason"] == "different_scopes_have_different_resolved_truth"


def test_occupancy_same_scope_conflict_stays_internal_and_client_safe():
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
                "LOTTO UNICO\nBene N° 1 - Appartamento\n"
                "L'immobile risulta libero.\n"
                "L'immobile occupato da terzi."
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_occupancy_same_scope_conflict",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    assert payload["scopes"]["bene:1"]["occupancy"]["status"] == "OCCUPATO"
    assert payload["scopes"]["bene:1"]["metadata"]["occupancy_internal"]["raw_conflict_detected"] is True
    assert payload["canonical_case"]["occupancy"]["status"] == "OCCUPATO"
    assert payload["canonical_case"]["occupancy"]["status"] != "CONFLITTO"


def test_occupancy_universal_lotto_statement_inherits_to_child_beni():
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
                "LOTTO UNICO\nBene N° 1 - Appartamento\nBene N° 2 - Garage\n"
                "Tutti i beni del lotto unico risultano liberi."
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_occupancy_universal_lotto_inheritance",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    assert payload["scopes"]["lotto:unico"]["occupancy"]["status"] == "LIBERO"
    assert payload["scopes"]["bene:1"]["occupancy"]["status"] == "LIBERO"
    assert payload["scopes"]["bene:2"]["occupancy"]["status"] == "LIBERO"
    assert payload["canonical_case"]["occupancy"]["status"] == "LIBERO"


def test_urbanistica_regular_writes_smallest_scope_first_and_rolls_root_up():
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
                "LOTTO UNICO\n"
                "Bene N° 1 - Appartamento\n"
                "REGOLARITA EDILIZIA\n"
                "L'immobile risulta regolare per la legge n° 47/1985.\n"
                "Le unita immobiliari sono conformi a quanto depositato."
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_urbanistica_regolare",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    scoped = payload["scopes"]["bene:1"]["urbanistica"]["urbanistica_status"]
    assert scoped["value"] == "REGOLARE"
    assert payload["canonical_case"]["urbanistica"]["urbanistica_status"]["value"] == "REGOLARE"
    assert payload["canonical_case"]["urbanistica"]["sanatoria_status"]["value"] == "NON_VERIFICABILE"


def test_urbanistica_same_scope_difformita_beats_generic_regolare_wording():
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
                "LOTTO UNICO\n"
                "Bene N° 1 - Ufficio\n"
                "L'immobile risulta regolare per la legge n° 47/1985.\n"
                "Durante il sopralluogo sono state riscontrate incongruenze nello stato di fatto attuale."
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_urbanistica_negative_wins",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    scoped = payload["scopes"]["bene:1"]["urbanistica"]["urbanistica_status"]
    assert scoped["value"] == "DIFFORMITA_PRESENTE"
    internal = payload["scopes"]["bene:1"]["metadata"]["urbanistica_internal"]["urbanistica_status"]
    assert internal["raw_conflict_detected"] is True
    assert internal["resolution_reason"] == "highest_tier_signal_wins"


def test_urbanistica_universal_lotto_statement_inherits_to_child_beni():
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
                "LOTTO UNICO\n"
                "Bene N° 1 - Appartamento\n"
                "Bene N° 2 - Garage\n"
                "Tutti i beni del lotto unico risultano regolari urbanisticamente."
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_urbanistica_lotto_inheritance",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    assert payload["scopes"]["lotto:unico"]["urbanistica"]["urbanistica_status"]["value"] == "REGOLARE"
    assert payload["scopes"]["bene:1"]["urbanistica"]["urbanistica_status"]["value"] == "REGOLARE"
    assert payload["scopes"]["bene:2"]["urbanistica"]["urbanistica_status"]["value"] == "REGOLARE"


def test_urbanistica_root_rollup_stays_non_verificabile_when_leaf_truth_differs():
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
                "LOTTO UNICO\n"
                "Bene N° 1 - Appartamento\n"
                "L'immobile risulta regolare per la legge n° 47/1985.\n"
                "Bene N° 2 - Garage\n"
                "Durante il sopralluogo sono state riscontrate incongruenze nello stato di fatto attuale."
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_urbanistica_leaf_mixed",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    root = payload["canonical_case"]["urbanistica"]["urbanistica_status"]
    assert root["value"] == "NON_VERIFICABILE"
    assert root["verification_trail"]["reason_unresolved"] == "truth differs by scope"


def test_urbanistica_sanatoria_condono_and_ripristino_stay_bounded_and_conservative():
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
                "LOTTO UNICO\n"
                "Bene N° 1 - Garage\n"
                "Le modifiche eseguite senza pratiche edilizie sono sanabili.\n"
                "Domanda di condono edilizio presentata in data 12/03/1998.\n"
                "Si rende necessaria la demolizione delle opere abusive.\n"
                "Storico: vecchio riferimento a sanatoria in archivio."
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_urbanistica_bounded_fields",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    scoped = payload["scopes"]["bene:1"]["urbanistica"]
    assert scoped["sanatoria_status"]["value"] == "SANABILE"
    assert scoped["condono_status"]["value"] == "PRESENTE"
    assert scoped["ripristino_or_demolition_signal"]["value"] == "YES"


def test_urbanistica_open_condono_does_not_force_sanabile_from_conditional_wording():
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
                "LOTTO UNICO\n"
                "Bene N° 1 - Appartamento\n"
                "L'immobile non risulta regolare per la legge n° 47/1985.\n"
                "La Concessione in Sanatoria non è stata ad oggi rilasciata.\n"
                "In merito alla sanabilita degli interventi realizzati senza titolo, essendo ancora aperta la Domanda di Condono n. 40689/86 del 17/03/1986 si tratterebbe di dare impulso all'istruttoria.\n"
                "Resterebbe quindi aperta la strada del ripristino."
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_urbanistica_open_condono_conditional_sanatoria",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    scoped = payload["scopes"]["bene:1"]["urbanistica"]
    assert scoped["urbanistica_status"]["value"] == "DIFFORMITA_PRESENTE"
    assert scoped["condono_status"]["value"] == "PRESENTE"
    assert "sanatoria_status" not in scoped
    assert payload["canonical_case"]["urbanistica"]["sanatoria_status"]["value"] == "NON_VERIFICABILE"


def test_urbanistica_unscoped_summary_noise_does_not_write_document_root_truth_in_multi_scope_docs():
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
                "LOTTO 1\n"
                "Bene N° 1 - Appartamento\n"
                "L'immobile non risulta regolare per la legge n° 47/1985.\n"
                "LOTTO 2\n"
                "Bene N° 2 - Magazzino\n"
                "L'immobile non risulta regolare per la legge n° 47/1985.\n"
                "LOTTO 3\n"
                "Bene N° 3 - Deposito\n"
                "L'immobile non risulta regolare per la legge n° 47/1985.\n"
                "all'assenza di garanzia per vizi, mancanza di qualità e/o difformità della cosa venduta."
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_urbanistica_unscoped_summary_noise",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    assert payload["scopes"]["bene:1"]["urbanistica"]["urbanistica_status"]["value"] == "DIFFORMITA_PRESENTE"
    assert payload["scopes"]["bene:2"]["urbanistica"]["urbanistica_status"]["value"] == "DIFFORMITA_PRESENTE"
    assert payload["scopes"]["bene:3"]["urbanistica"]["urbanistica_status"]["value"] == "DIFFORMITA_PRESENTE"
    assert payload["scopes"]["document_root"]["urbanistica"] == {}
    assert payload["canonical_case"]["urbanistica"]["urbanistica_status"]["value"] == "DIFFORMITA_PRESENTE"


def test_legal_burdens_write_smallest_scope_first_and_roll_root_up():
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
                "LOTTO UNICO\n"
                "Bene N° 1 - Appartamento\n"
                "Non sono presenti vincoli pregiudizievoli sul bene.\n"
                "Non risultano servitù a carico dell'immobile.\n"
                "Il contratto è opponibile alla procedura esecutiva."
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_legal_burdens_single_scope",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    scoped = payload["scopes"]["bene:1"]["legal"]
    assert scoped["vincoli_status"]["value"] == "ASSENTE"
    assert scoped["servitu_status"]["value"] == "ASSENTE"
    assert scoped["opponibilita_status"]["value"] == "OPPONIBILE"
    assert payload["canonical_case"]["legal"]["vincoli_status"]["value"] == "ASSENTE"
    assert payload["canonical_case"]["legal"]["servitu_status"]["value"] == "ASSENTE"
    assert payload["canonical_case"]["legal"]["opponibilita_status"]["value"] == "OPPONIBILE"


def test_legal_burdens_root_rollup_stays_non_verificabile_when_leaf_truth_differs():
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
                "LOTTO UNICO\n"
                "Bene N° 1 - Appartamento\n"
                "Non sono presenti vincoli sul bene.\n"
                "Bene N° 2 - Garage\n"
                "Il bene è gravato da vincolo paesaggistico."
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_legal_burdens_leaf_mixed",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    assert payload["scopes"]["bene:1"]["legal"]["vincoli_status"]["value"] == "ASSENTE"
    assert payload["scopes"]["bene:2"]["legal"]["vincoli_status"]["value"] == "PRESENTE"
    root = payload["canonical_case"]["legal"]["vincoli_status"]
    assert root["value"] == "NON_VERIFICABILE"
    assert root["verification_trail"]["reason_unresolved"] == "truth differs by scope"


def test_legal_burdens_universal_lotto_statement_inherits_and_da_verificare_stays_cautious():
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
                "LOTTO UNICO\n"
                "Bene N° 1 - Appartamento\n"
                "Bene N° 2 - Garage\n"
                "Tutti i beni del lotto unico sono liberi da servitù.\n"
                "Bene N° 1 - Appartamento\n"
                "Opponibilità del titolo da verificare."
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_legal_burdens_lotto_inheritance",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    assert payload["scopes"]["lotto:unico"]["legal"]["servitu_status"]["value"] == "ASSENTE"
    assert payload["scopes"]["bene:1"]["legal"]["servitu_status"]["value"] == "ASSENTE"
    assert payload["scopes"]["bene:2"]["legal"]["servitu_status"]["value"] == "ASSENTE"
    assert payload["scopes"]["bene:1"]["legal"]["opponibilita_status"]["value"] == "DA_VERIFICARE"
    assert payload["canonical_case"]["legal"]["opponibilita_status"]["value"] == "NON_VERIFICABILE"


def test_legal_burdens_explicit_instrument_beats_generic_negative_subcategory_wording():
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
                "LOTTO UNICO\n"
                "Bene N° 1 - Appartamento\n"
                "Non sono presenti vincoli artistici, storici o alberghieri.\n"
                "Con l'Atto d'obbligo del 09/04/1969 è stata vincolata la destinazione d'uso dei locali.\n"
                "Con l'Atto di Costituzione Servitù del 09/04/1969 sono state vincolate a verde tutte le aree.\n"
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_legal_burdens_explicit_instrument",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    scoped = payload["scopes"]["bene:1"]["legal"]
    assert scoped["vincoli_status"]["value"] == "PRESENTE"
    assert scoped["vincoli_status"]["source_scope_id"] == "bene:1"
    assert scoped["servitu_status"]["value"] == "PRESENTE"
    assert scoped["servitu_status"]["source_scope_id"] == "bene:1"


def test_legal_burdens_generic_boilerplate_does_not_create_decisive_servitu_truth():
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
                "LOTTO UNICO\n"
                "Bene N° 1 - Appartamento\n"
                "Il valore commerciale tiene conto di eventuali vincoli e servitù passive o attive.\n"
                "Bene N° 2 - Garage\n"
                "Atto di Costituzione Servitù del 06/03/1997.\n"
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_legal_burdens_generic_boilerplate",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    assert payload["scopes"]["bene:1"]["legal"] == {}
    assert payload["scopes"]["bene:2"]["legal"]["servitu_status"]["value"] == "PRESENTE"
    assert payload["canonical_case"]["legal"]["servitu_status"]["value"] == "NON_VERIFICABILE"


def test_verifier_bridge_updates_legacy_regolarita_urbanistica_from_new_root_truth():
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
                "LOTTO UNICO\n"
                "Bene N° 1 - Appartamento\n"
                "Durante il sopralluogo sono state riscontrate incongruenze nello stato di fatto attuale."
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_urbanistica_bridge",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    apply_verifier_to_result(result, payload)
    assert result["field_states"]["regolarita_urbanistica"]["value"] == "PRESENTI DIFFORMITA"
    assert result["field_states"]["regolarita_urbanistica"]["status"] == "FOUND"


def test_quota_writes_bene_scope_first_and_root_stays_null_when_scopes_disagree():
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
                "LOTTO UNICO\n"
                "Bene N° 1 - Appartamento\nTipologia del diritto 1/1 di piena proprietà.\n"
                "Bene N° 2 - Garage\nTipologia del diritto 1/2 di nuda proprietà."
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_quota_leaf_first_rollup",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    assert payload["scopes"]["bene:1"]["rights"]["quota"]["value"] == "1/1"
    assert payload["scopes"]["bene:2"]["rights"]["quota"]["value"] == "1/2"
    assert "quota" not in payload["scopes"]["bene:1"]["catasto"]
    assert "quota" not in payload["scopes"]["bene:2"]["catasto"]
    assert payload["canonical_case"]["rights"]["quota"]["value"] is None
    assert (
        payload["scopes"]["document_root"]["metadata"]["rights_internal"]["quota"]["unresolved_reason"]
        == "different_scopes_have_different_resolved_truth"
    )


def test_quota_universal_lotto_statement_inherits_to_child_beni_and_rolls_up_root():
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
                "LOTTO UNICO\n"
                "Bene N° 1 - Appartamento\n"
                "Bene N° 2 - Garage\n"
                "Tutti i beni del lotto unico sono in piena proprietà per la quota di 1/1."
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_quota_universal_lotto_inheritance",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    assert payload["scopes"]["lotto:unico"]["rights"]["quota"]["value"] == "1/1"
    assert payload["scopes"]["bene:1"]["rights"]["quota"]["value"] == "1/1"
    assert payload["scopes"]["bene:2"]["rights"]["quota"]["value"] == "1/1"
    assert "quota" not in payload["scopes"]["lotto:unico"]["catasto"]
    assert payload["canonical_case"]["rights"]["quota"]["value"] == "1/1"


def test_quota_rejects_date_like_fragments_even_with_property_context():
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
                "LOTTO UNICO\n"
                "Bene N° 1 - Appartamento\n"
                "Diritti di piena proprietà per la quota di 1/1.\n"
                "Atto in data 06/03/1997 relativo a proprietà confinante."
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_quota_date_like_rejection",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    values = [item["value"] for item in payload["candidates"]["quota"]]
    assert "6/3" not in values
    assert payload["canonical_case"]["rights"]["quota"]["value"] == "1/1"


def test_multilot_fixture_keeps_primary_lot_quota_over_ancillary_shared_access_shares():
    result, pages = _repo_fixture("multilot_69_2024")
    payload = run_quality_verifier(
        analysis_id="multilot_quota_primary_beats_ancillary_probe",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    assert payload["scopes"]["lotto:1"]["rights"]["quota"]["value"] == "1/1"
    assert payload["scopes"]["lotto:2"]["rights"]["quota"]["value"] == "1/1"
    assert payload["scopes"]["lotto:3"]["rights"]["quota"]["value"] == "1/1"
    assert "quota" not in payload["scopes"]["lotto:1"]["catasto"]
    assert payload["canonical_case"]["rights"]["quota"]["value"] == "1/1"


def test_multibene_fixture_writes_scoped_catasto_fields_per_bene():
    result, pages = _repo_fixture("multibene_1859886")
    payload = run_quality_verifier(
        analysis_id="multibene_scoped_catasto_probe",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    assert payload["scopes"]["bene:1"]["catasto"]["foglio"]["value"] == "20"
    assert payload["scopes"]["bene:1"]["catasto"]["particella"]["value"] == "433"
    assert payload["scopes"]["bene:1"]["catasto"]["subalterno"]["value"] == "301"
    assert payload["scopes"]["bene:1"]["catasto"]["categoria"]["value"] == "A/10"
    assert payload["scopes"]["bene:4"]["catasto"]["particella"]["value"] == "600"
    assert payload["scopes"]["bene:4"]["catasto"]["subalterno"]["value"] == "3"
    assert payload["scopes"]["bene:4"]["catasto"]["categoria"]["value"] == "A/7"
    assert payload["canonical_case"]["catasto"]["particella"]["value"] is None
    assert payload["canonical_case"]["rights"]["quota"]["source_scope_id"] == "rollup_from_children"
    assert payload["canonical_case"]["catasto"]["foglio"]["source_scope_id"] == "rollup_from_children"


def test_multilot_fixture_writes_scoped_catasto_and_prefers_primary_over_ancillary_entries():
    result, pages = _repo_fixture("multilot_69_2024")
    payload = run_quality_verifier(
        analysis_id="multilot_scoped_catasto_probe",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    assert payload["scopes"]["lotto:1"]["catasto"]["foglio"]["value"] == "18"
    assert payload["scopes"]["lotto:1"]["catasto"]["particella"]["value"] == "465"
    assert payload["scopes"]["lotto:1"]["catasto"]["categoria"]["value"] == "A/3"
    lot2 = payload["scopes"]["lotto:2"]["catasto"]
    lot3 = payload["scopes"]["lotto:3"]["catasto"]
    primary_records = {
        (lot2["particella"]["value"], lot2["subalterno"]["value"], lot2["categoria"]["value"]),
        (lot3["particella"]["value"], lot3["subalterno"]["value"], lot3["categoria"]["value"]),
    }
    assert primary_records == {("1700", "5", "C/2"), ("94", "8", "F/3")}
    assert lot2["categoria"]["value"] != "F/1"
    assert lot3["categoria"]["value"] != "F/1"
    assert payload["canonical_case"]["catasto"]["particella"]["value"] is None


def test_multilot_clarification_issue_pack_derives_scope_and_target_entry_pages():
    pack = build_clarification_issue_pack("multilot_69_2024")
    issue = next(
        item for item in pack["blocked_packets"]
        if item.get("field_family") == "valuation"
        and item.get("source_pages") == [52, 53]
        and item.get("lot_id") == "3"
        and item.get("bene_id") == "1"
    )
    assert issue["target_scope"]["scope_key"] == "bene:3/1"
    assert issue["target_scope"]["scope_start_page"] == 52
    assert issue["target_scope"]["scope_end_page"] == 54
    assert [page["page"] for page in issue["target_section_entry_pages"]] == [33, 52]
    assert issue["page_selection"]["has_target_section_entry_page"] is True
    assert issue["page_selection"]["uses_summary_or_index_page"] is False
    assert issue["page_selection"]["uses_transition_page"] is True
    assert issue["admissibility_status"] == "upstream_blocked_packet"
    assert "CONTAMINATION_WITH_TRANSITION_PAGE" in issue["admissibility_reason_codes"]
    assert [page["page"] for page in issue["anchor_pages"]] == [33, 52]


def test_multilot_clarification_issue_pack_marks_transition_and_scope_bounded_recap_pages():
    pack = build_clarification_issue_pack("multilot_69_2024")
    issue = next(
        item for item in pack["blocked_packets"]
        if item.get("field_family") == "valuation"
        and item.get("source_pages") == [67, 68, 69]
        and item.get("lot_id") == "3"
    )
    assert issue["target_scope"]["scope_key"] == "lot:3"
    assert issue["page_selection"]["uses_transition_page"] is True
    assert issue["page_selection"]["uses_summary_or_index_page"] is True
    assert issue["admissibility_status"] == "upstream_blocked_packet"
    assert "CONTAMINATION_WITH_TRANSITION_PAGE" in issue["admissibility_reason_codes"]
    assert "CONTAMINATION_WITH_SUMMARY_INDEX_PAGE" in issue["admissibility_reason_codes"]
    recap_pages = [page["page"] for page in issue["recap_pages"]]
    assert recap_pages
    assert all(33 <= page <= 81 for page in recap_pages)
    assert 3 not in recap_pages
    anchor_pages = [page["page"] for page in issue["anchor_pages"]]
    assert anchor_pages[:2] == [33, 68]


def test_llm_selection_skips_structurally_unsafe_multilot_packets():
    pack = build_clarification_issue_pack("multilot_69_2024")
    selected = select_prioritized_issues(pack, issue_type="SCOPE_AMBIGUITY", field_family="occupancy", limit=8)
    selected_ids = {issue["issue_id"] for issue in selected}
    unsafe_issue = next(
        issue
        for issue in pack["blocked_packets"]
        if issue.get("field_family") == "occupancy" and issue.get("source_pages") == [3]
    )
    assert unsafe_issue["page_selection"]["llm_safe"] is False
    assert unsafe_issue["issue_id"] not in selected_ids


def test_single_lot_clarification_issue_pack_infers_scope_from_scope_maps():
    issue = _clarification_issue(
        "via_cristoforo_colombo_2_4",
        field_family="valuation",
        source_pages=[15],
    )
    assert issue["target_scope"]["scope_key"] == "lot:unico"
    assert issue["target_scope_kind"] == "lot"
    assert issue["admissibility_status"] == "admissible_clean"


def test_multilot_clarification_issue_pack_blocks_missing_scope_summary_packets_upstream():
    pack = build_clarification_issue_pack("multilot_69_2024")
    blocked = next(
        issue for issue in pack["blocked_packets"]
        if issue.get("field_family") == "location" and issue.get("source_pages") == [3]
    )
    assert blocked["admissibility_status"] == "upstream_blocked_packet"
    assert "MISSING_TARGET_SCOPE" in blocked["admissibility_reason_codes"]
    assert "SUMMARY_INDEX_PRIMARY_ONLY" in blocked["admissibility_reason_codes"]
    assert blocked["issue_id"] not in {issue["issue_id"] for issue in pack["issues"]}


def test_grouped_llm_trace_rows_keep_structured_fields():
    with tempfile.TemporaryDirectory() as tmp_dir:
        artifact_dir = Path(tmp_dir)
        (artifact_dir / "doc_map.json").write_text(
            json.dumps(
                {
                    "fields": {},
                    "grouped_llm_explanations": [
                        {
                            "issue_id": "issue-1",
                            "scope_key": "lot:3",
                            "field_type": "occupancy",
                            "llm_outcome": "unresolved_explained",
                            "resolution_mode": "true_unresolved",
                            "user_visible_explanation": "copy",
                            "why_not_resolved": "ambiguous scope",
                            "confidence_band": "low",
                            "needs_human_review": True,
                            "source_pages": [71],
                            "evidence_pages": [71],
                            "supporting_pages": [33, 71],
                            "tension_pages": [70],
                        }
                    ],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        (artifact_dir / "llm_resolution_pack.json").write_text(
            json.dumps({"issues": [], "resolutions": []}, ensure_ascii=False),
            encoding="utf-8",
        )
        (artifact_dir / "missing_slot_review_pack.json").write_text(
            json.dumps({"reviews": []}, ensure_ascii=False),
            encoding="utf-8",
        )
        (artifact_dir / "missing_slot_escalation_pack.json").write_text(
            json.dumps({"escalations": []}, ensure_ascii=False),
            encoding="utf-8",
        )
        (artifact_dir / "raw_pages.json").write_text(
            json.dumps([{"page_number": 71, "text": "LOTTO 3"}], ensure_ascii=False),
            encoding="utf-8",
        )
        rows = _build_llm_call_trace(artifact_dir, "multilot_69_2024")
    grouped = next(row for row in rows if row["source_stage"] == "grouped_llm_explanation")
    assert grouped["structured_llm_response"]["resolution_mode"] == "true_unresolved"
    assert grouped["structured_llm_response"]["evidence_pages"] == [71]
    assert grouped["structured_llm_response"]["supporting_pages"] == [33, 71]
    assert grouped["structured_llm_response"]["tension_pages"] == [70]
    assert grouped["post_guard_result_after_freeze"]["state"] == "grouped_llm_explanation"


def test_rmei_fixture_collapses_single_scope_catasto_to_root():
    result, pages = _repo_fixture("rmei_928_2022")
    payload = run_quality_verifier(
        analysis_id="rmei_scoped_catasto_probe",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    assert payload["scopes"]["bene:1"]["catasto"]["foglio"]["value"] == "242"
    assert payload["scopes"]["bene:1"]["catasto"]["particella"]["value"] == "301"
    assert payload["scopes"]["bene:1"]["catasto"]["subalterno"]["value"] == "516"
    assert payload["scopes"]["bene:1"]["catasto"]["categoria"]["value"] == "A/2"
    assert payload["canonical_case"]["catasto"]["foglio"]["value"] == "242"
    assert payload["canonical_case"]["catasto"]["particella"]["value"] == "301"
    assert payload["canonical_case"]["catasto"]["subalterno"]["value"] == "516"
    assert payload["canonical_case"]["catasto"]["categoria"]["value"] == "A/2"
    assert payload["canonical_case"]["rights"]["quota"]["source_scope_id"] == "rollup_from_children"
    assert payload["canonical_case"]["catasto"]["foglio"]["source_scope_id"] == "rollup_from_children"


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


def test_multilot_fixture_writes_scoped_pricing_before_root_suppression():
    result, pages = _repo_fixture("multilot_69_2024")
    payload = run_quality_verifier(
        analysis_id="multilot_scoped_pricing_probe",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    assert payload["scopes"]["lotto:1"]["pricing"]["selected_price"] == 64198.0
    assert payload["scopes"]["lotto:2"]["pricing"]["selected_price"] == 84000.0
    assert payload["scopes"]["lotto:3"]["pricing"]["selected_price"] == 224268.0
    assert payload["scopes"]["bene:1"]["pricing"] == {}
    assert payload["scopes"]["bene:2"]["pricing"] == {}
    assert payload["scopes"]["bene:3"]["pricing"] == {}
    assert payload["canonical_case"]["pricing"]["selected_price"] is None


def test_pricing_explicit_lotto_price_does_not_drift_to_neighbor_bene_scope():
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
                "LOTTO 1\n"
                "Bene N° 1 - Appartamento\n"
                "Descrizione del bene.\n"
                "LOTTO 2 - PREZZO BASE D'ASTA: € 84.000,00\n"
                "Bene N° 2 - Garage\n"
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_pricing_lotto_anchor_stability",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    assert payload["scopes"]["lotto:2"]["pricing"]["selected_price"] == 84000.0
    assert payload["scopes"]["bene:1"]["pricing"] == {}
    assert payload["scopes"]["bene:2"]["pricing"] == {}


def test_pricing_toc_style_lot_line_does_not_attach_to_previous_bene_heading():
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
                "LOTTO UNICO\n"
                "Bene N° 4 - Villetta ................................ 45\n"
                "Lotto Unico - Prezzo base d'asta: € 391.849,00 ................................ 45\n"
            ),
        },
    ]
    payload = run_quality_verifier(
        analysis_id="synthetic_pricing_toc_lotto_scope",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    assert payload["scopes"]["lotto:unico"]["pricing"]["selected_price"] == 391849.0
    assert not any(
        scope.get("scope_type") == "bene" and scope.get("pricing")
        for scope in payload["scopes"].values()
    )


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
        {
            "page": 4,
            "amount_eur": 97321.61,
            "quote": "smaltimento rifiuti a carico dell'acquirente: € 97.321,61",
            "context": "Prezzo base d'asta Valore in caso di regolarizzazione urbanistica e catastale, spese di smaltimento rifiuti a carico dell'acquirente: € 97.321,61",
        },
        {
            "page": 5,
            "amount_eur": 17809.69,
            "quote": "smaltimento di beni mobili presenti all'interno. € 17.809,69",
            "context": "Riduzione del valore del 15% per assenza di garanzia per vizi, rimborso forfettario e smaltimento di beni mobili presenti all'interno. € 17.809,69",
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
    assert sorted(cand.value for cand in candidates if cand.semantic_role == "auction_price") == [64198.0, 97321.61]
    assert by_role["valuation_total"].value == 80248.0
    assert sorted(cand.value for cand in candidates if cand.semantic_role == "net_valuation") == [224268.0]
    assert by_role["valuation_adjustment"].value == 17809.69
    buyer_cost_values = {round(cand.value, 2) for cand in candidates if cand.semantic_role == "buyer_cost"}
    assert 97321.61 not in buyer_cost_values
    assert 17809.69 not in buyer_cost_values


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


def test_verifier_selected_price_overrides_invalid_legacy_field_state():
    result, pages = _repo_fixture("silvabella")
    payload = run_quality_verifier(
        analysis_id="silvabella_selected_price_bridge",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    packaged = json.loads(json.dumps(result))
    apply_verifier_to_result(packaged, payload)

    pricing = payload["canonical_case"]["pricing"]
    field_state = (packaged.get("field_states") or {}).get("prezzo_base_asta") or {}
    assert pricing["selected_price"] == 45338.48
    assert abs(float(field_state.get("value")) - 45338.48) < 1.0
    assert field_state.get("resolver_meta", {}).get("source") == "canonical_pricing.selected_price"
    previous = field_state.get("resolver_meta", {}).get("previous_state") or {}
    assert previous.get("value") == 2.0


def test_read_refresh_preserves_verifier_selected_price_over_invalid_legacy_state():
    import server

    result, pages = _repo_fixture("silvabella")
    payload = run_quality_verifier(
        analysis_id="silvabella_selected_price_read_refresh",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    packaged = json.loads(json.dumps(result))
    apply_verifier_to_result(packaged, payload)
    # Simulate a stale legacy state being recomputed or retained before read refresh.
    packaged.setdefault("field_states", {})["prezzo_base_asta"] = copy_state = {
        "value": 2.0,
        "status": "FOUND",
        "confidence": 0.9,
        "evidence": [{"page": 22, "quote": "PREZZO ABASED'ASTA DELL'IMMOBILE (Subalterno 2)"}],
    }
    server._refresh_customer_facing_result_on_read(packaged, pages)

    field_state = (packaged.get("field_states") or {}).get("prezzo_base_asta") or {}
    assert copy_state["value"] == 2.0
    assert abs(float(field_state.get("value")) - 45338.48) < 1.0
    assert field_state.get("resolver_meta", {}).get("source") == "canonical_pricing.selected_price"


def test_verifier_bridge_prunes_pricing_amounts_from_money_box():
    for fixture_name in ("multibene_1859886", "rmei_928_2022", "silvabella"):
        result, pages = _repo_fixture(fixture_name)
        payload = run_quality_verifier(
            analysis_id=f"{fixture_name}_money_box_pricing_prune",
            result=result,
            pages=pages,
            full_text="\n\n".join(page["text"] for page in pages),
        )
        packaged = json.loads(json.dumps(result))
        apply_verifier_to_result(packaged, payload)

        pricing = payload["canonical_case"]["pricing"]
        pricing_amounts = {
            round(float(value), 2)
            for value in (
                pricing.get("selected_price"),
                pricing.get("benchmark_value"),
                pricing.get("adjusted_market_value"),
            )
            if isinstance(value, (int, float))
        }
        for box_key in ("money_box", "section_3_money_box"):
            box = packaged.get(box_key) if isinstance(packaged.get(box_key), dict) else {}
            for item in box.get("items") or []:
                if not isinstance(item, dict) or item.get("stima_euro") is None:
                    continue
                try:
                    amount = round(float(item["stima_euro"]), 2)
                except Exception:
                    continue
                assert amount not in pricing_amounts, (
                    f"{fixture_name} leaked pricing amount {item['stima_euro']} into {box_key}"
                )


def test_read_refresh_prunes_pricing_amounts_from_money_box():
    import server

    result, pages = _repo_fixture("silvabella")
    payload = run_quality_verifier(
        analysis_id="silvabella_money_box_read_refresh",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    packaged = json.loads(json.dumps(result))
    apply_verifier_to_result(packaged, payload)
    packaged.setdefault("money_box", {}).setdefault("items", []).append({
        "label_it": "Costo rilevato da perizia",
        "stima_euro": 45338.48,
        "source": "step3_candidates",
    })

    server._refresh_customer_facing_result_on_read(packaged, pages)

    pricing_amounts = {45338.48, 56861.33}
    for item in (packaged.get("money_box") or {}).get("items") or []:
        if isinstance(item, dict) and item.get("stima_euro") is not None:
            try:
                amount = round(float(item["stima_euro"]), 2)
            except Exception:
                continue
            assert amount not in pricing_amounts
    removed = (packaged.get("money_box") or {}).get("removed_pricing_amount_items") or []
    assert any(round(float(item.get("amount") or 0), 2) == 45338.48 for item in removed)


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


# ---------------------------------------------------------------------------
# Regression: Vidigulfo synthesis layer — occupancy contradiction + fake cost
# ---------------------------------------------------------------------------
# These tests pin the exact synthesis failures fixed in commit after 2d8b378:
# 1. document-root OCCUPATO+NON_OPPONIBILE + all lots LIBERO → synthesise LIBERO
# 2. valuation TOTALE must not override label-matched buyer costs explicit_total
# 3. NON_OPPONIBILE occupancy must not escalate to RED "Immobile occupato"


def _vidigulfo_synthesis_payload():
    """
    Synthetic fixture that reproduces the Vidigulfo contradiction scenario:
    - Document page says stato occupativo OCCUPATO, non opponibile
    - Lot-level result seed says occupancy_status=LIBERO
    - Cost table has spese condominiali €3.600,00 AND a valuation TOTALE €129.312,00
      followed by "riduzione cautelativa" on the same page — mimicking the false-total bug.
    """
    pages = [
        {
            "page_number": 1,
            "text": (
                "LOTTO 1\n"
                "STATO OCCUPATIVO\n"
                "L'immobile risulta occupato da parte dell'esecutato a titolo di mera detenzione.\n"
                "Occupazione non opponibile al terzo acquirente.\n"
                "Valore di stima lordo: € 129.312,00\n"
                "TOTALE  € 129.312,00\n"
                "Riduzione cautelativa 3%  € 3.879,36\n"
            ),
        },
        {
            "page_number": 2,
            "text": (
                "LOTTO 1 – ONERI A CARICO DELL'ACQUIRENTE\n"
                "spese condominiali scadute e non pagate  € 3.600,00\n"
            ),
        },
    ]
    result = {
        "field_states": {},
        "dati_certi_del_lotto": {},
        "document_quality": {"status": "TEXT_OK"},
        "semaforo_generale": {"status": "AMBER"},
        "lots": [
            {
                "lot_number": 1,
                "occupancy_status": "LIBERO",
                "stato_occupativo": "LIBERO",
                "evidence": {"occupancy_status": []},
            }
        ],
    }
    return run_quality_verifier(
        analysis_id="vidigulfo_synthesis_regression",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )


def test_vidigulfo_lot_libero_plus_non_opponibile_resolves_to_libero():
    payload = _vidigulfo_synthesis_payload()
    occupancy = payload["canonical_case"]["occupancy"]
    assert occupancy["status"] == "LIBERO", (
        f"Expected LIBERO after NON_OPPONIBILE+lot-LIBERO synthesis, got {occupancy['status']!r}"
    )
    opponibilita = str(occupancy.get("opponibilita") or "").upper()
    assert "NON OPPONIBILE" in opponibilita, (
        f"NON OPPONIBILE must be preserved in opponibilita, got {opponibilita!r}"
    )
    assert "non_opponibile_with_lot_libero_resolved_to_libero" in occupancy.get("guards", []), (
        "synthesis guard token must appear in occupancy guards"
    )


def test_vidigulfo_top_and_lot_occupancy_not_contradictory():
    payload = _vidigulfo_synthesis_payload()
    # Apply result packaging so result["stato_occupativo"] is populated
    result = {}
    apply_verifier_to_result(result, payload)
    lot_status = "LIBERO"  # seed value — lots[0].stato_occupativo
    top_status = (result.get("stato_occupativo") or {}).get("status")
    assert top_status == lot_status, (
        f"lot-level and top-level stato_occupativo must agree; lot={lot_status!r} top={top_status!r}"
    )


def test_vidigulfo_no_red_immobile_occupato_issue():
    payload = _vidigulfo_synthesis_payload()
    issues = payload["canonical_case"]["priority"].get("issues", [])
    red_occupancy = [
        issue for issue in issues
        if issue.get("code") == "OCCUPANCY_RISK" and issue.get("severity") == "RED"
    ]
    assert not red_occupancy, (
        "RED OCCUPANCY_RISK must not be raised when occupancy is NON_OPPONIBILE+lot-LIBERO"
    )


def test_vidigulfo_summary_does_not_say_immobile_occupato():
    payload = _vidigulfo_synthesis_payload()
    result = {}
    apply_verifier_to_result(result, payload)
    summary_it = str((result.get("summary_for_client") or {}).get("summary_it") or "").lower()
    assert "immobile occupato" not in summary_it, (
        f"summary_it must not say 'immobile occupato' for NON_OPPONIBILE case; got: {summary_it!r}"
    )


def test_vidigulfo_cost_total_uses_label_matched_items_not_valuation_total():
    payload = _vidigulfo_synthesis_payload()
    costs = payload["canonical_case"]["costs"]
    explicit_total = costs.get("explicit_total")
    assert explicit_total is not None, "explicit_total must be set"
    assert abs(float(explicit_total) - 3600.0) < 1.0, (
        f"explicit_total must be €3.600 (from spese condominiali), got {explicit_total}"
    )
    assert float(explicit_total) < 10000, (
        f"explicit_total must not include valuation TOTALE (129.312); got {explicit_total}"
    )


def test_vidigulfo_summary_does_not_contain_fake_valuation_total():
    payload = _vidigulfo_synthesis_payload()
    result = {}
    apply_verifier_to_result(result, payload)
    summary_it = str((result.get("summary_for_client") or {}).get("summary_it") or "")
    summary_bundle = result.get("summary_for_client_bundle") or {}
    decision_summary = str(summary_bundle.get("decision_summary_it") or "")
    caution_points = " ".join(str(p) for p in summary_bundle.get("caution_points_it") or [])
    combined = summary_it + decision_summary + caution_points
    assert "129.851" not in combined, f"fake cost total 129.851 must not appear in summary; got: {combined!r}"
    assert "129.312" not in combined, f"valuation total 129.312 must not appear in cost summary; got: {combined!r}"


# ---------------------------------------------------------------------------
# Vidigulfo Round 2 — field_states, delivery_timeline, money_box, summary
# ---------------------------------------------------------------------------

def _vidigulfo_round2_pages():
    """
    Key pages from the real Vidigulfo document (via Cristoforo Colombo 2/4).
    Covers: address, quota, occupancy (LIBERO+non opponibile+liberazione),
    costs (€3.600) AND valuation totals (€118.731,30 / €17.809,69 / €97.321,61)
    that must NOT bleed into money_box or cost totals.
    """
    return [
        {
            "page_number": 1,
            # Case number on a separate page from the quota section so the "254/25"
            # fraction candidate does not acquire a rights context and is filtered.
            "text": (
                "TRIBUNALE DI PAVIA\n"
                "ESECUZIONE IMMOBILIARE n. 254/25 Reg. Esec.\n"
                "Immobile in Comune di Vidigulfo – via Cristoforo Colombo, 2/4\n"
            ),
        },
        {
            "page_number": 2,
            # Blank lines between the address and the quota section match the real PDF,
            # ensuring the civic-address fraction "2/4" is not confused with the rights quota.
            "text": (
                "CONCLUSIONI DEFINITIVE\n"
                "Immobile in Vidigulfo – Via Cristoforo Colombo, 2/4\n"
                "\n"
                "\n"
                "1. QUOTA DI PROPRIETA' DEL BENE PIGNORATO\n"
                "APPARTAMENTO AL PIANO PRIMO E GARAGE AL PT:\n"
                "Quota di 1/1 propr. OMISSIS\n"
            ),
        },
        {
            "page_number": 3,
            # Exact line structure from the real Vidigulfo PDF (page 3 of raw_pages.json):
            # Each topographic word is on its own line up to "saltuariamente", then the
            # remainder of the paragraph flows as full sentences.  This ensures the 6-line
            # lookahead from "occupato" reaches "E pertanto non" (which contains "non"),
            # and the next sentence line starts with "opponibile" — so the agent captures
            # NON OPPONIBILE.  The synthesis guard then fires: OCCUPATO + NON OPPONIBILE
            # + all lots LIBERO → resolved to LIBERO.
            "text": (
                "4. STATO DI POSSESSO DEL BENE\n"
                "Al momento del sopralluogo l'immobile oggetto di\n"
                "pignoramento\n"
                "risulta\n"
                "LIBERO,\n"
                "in\n"
                "quanto\n"
                "occupato\n"
                "saltuariamente\n"
                "dall'esecutato, a causa della mancanza degli allacciamenti. E pertanto non\n"
                "opponibile all'aggiudicatario, con liberazione a cura e spese della procedura\n"
                "esecutiva.\n"
            ),
        },
        {
            "page_number": 4,
            "text": (
                "Spese tecniche di regolarizzazione urbanistico e/o catastale          €.     3.600,00\n"
                "\n"
                "Prezzo base d'asta\n"
                "Valore in caso di regolarizzazione urbanistica e catastale, spese di\n"
                "smaltimento rifiuti a carico dell'acquirente:\n"
                "€.   97.321,61\n"
            ),
        },
        {
            "page_number": 15,
            "text": (
                "Valore complessivo del lotto:\n"
                "€. 118.731,30\n"
                "Valore della quota di 1/1:\n"
                "€. 118.731,30\n"
                "\n"
                "8.4. Adeguamenti e correzioni della stima\n"
                "Riduzione del valore del 15%\n"
                "€.     17.809,69\n"
                "\n"
                "Spese tecniche di regolarizzazione urbanistico e/o catastale          €.     3.600,00\n"
                "\n"
                "8.5. Prezzo base d'asta\n"
                "Valore in caso di regolarizzazione urbanistica e catastale, spese di\n"
                "smaltimento rifiuti a carico dell'acquirente:\n"
                "€.   97.321,61\n"
            ),
        },
    ]


def _vidigulfo_round2_result_seed():
    return {
        "field_states": {},
        "dati_certi_del_lotto": {},
        "document_quality": {"status": "TEXT_OK"},
        "semaforo_generale": {"status": "AMBER"},
        "lots": [
            {
                "lot_number": 1,
                "occupancy_status": "LIBERO",
                "stato_occupativo": "LIBERO",
                "ubicazione": "Via Cristoforo Colombo 2/4, Vidigulfo",
                # quota and diritto_reale mirror the real server-side pre-extraction,
                # providing the legacy quota candidate that the catasto agent uses.
                "quota": "1/1",
                "diritto_reale": "1/1 piena proprietà",
                "evidence": {"occupancy_status": [], "ubicazione": []},
            }
        ],
    }


def _vidigulfo_round2_payload():
    pages = _vidigulfo_round2_pages()
    result = _vidigulfo_round2_result_seed()
    return run_quality_verifier(
        analysis_id="vidigulfo_round2_regression",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )


def test_vidigulfo_r2_occupancy_is_libero_non_opponibile():
    payload = _vidigulfo_round2_payload()
    occ = payload["canonical_case"]["occupancy"]
    assert occ["status"] == "LIBERO", f"occupancy must resolve to LIBERO; got {occ['status']!r}"
    assert "NON OPPONIBILE" in str(occ.get("opponibilita") or "").upper(), (
        f"opponibilita must be NON OPPONIBILE; got {occ.get('opponibilita')!r}"
    )


def test_vidigulfo_r2_cost_is_3600_not_valuation_total():
    payload = _vidigulfo_round2_payload()
    costs = payload["canonical_case"]["costs"]
    explicit_total = costs.get("explicit_total") or 0.0
    assert abs(float(explicit_total) - 3600.0) < 5.0, (
        f"explicit_total must be ~€3.600 from spese tecniche, got {explicit_total}"
    )
    assert float(explicit_total) < 50000, (
        f"explicit_total must not include valuation amounts (97321.61 / 118731.30); got {explicit_total}"
    )


def test_vidigulfo_r2_quota_written_to_field_states():
    pages = _vidigulfo_round2_pages()
    result = _vidigulfo_round2_result_seed()
    payload = run_quality_verifier(
        analysis_id="vidigulfo_r2_quota",
        result=result,
        pages=pages,
        full_text="\n\n".join(p["text"] for p in pages),
    )
    packaged = json.loads(json.dumps(result))
    apply_verifier_to_result(packaged, payload)
    quota_val = (packaged.get("field_states") or {}).get("quota", {}).get("value")
    assert quota_val is not None, "field_states.quota.value must not be None after verifier bridge"
    assert "1/1" in str(quota_val), f"field_states.quota.value must contain '1/1'; got {quota_val!r}"


def test_vidigulfo_r2_prezzo_base_written_to_field_states():
    pages = _vidigulfo_round2_pages()
    result = _vidigulfo_round2_result_seed()
    payload = run_quality_verifier(
        analysis_id="vidigulfo_r2_prezzo",
        result=result,
        pages=pages,
        full_text="\n\n".join(p["text"] for p in pages),
    )
    packaged = json.loads(json.dumps(result))
    apply_verifier_to_result(packaged, payload)
    prezzo = (packaged.get("field_states") or {}).get("prezzo_base_asta", {}).get("value")
    assert prezzo is not None, "field_states.prezzo_base_asta.value must not be None after verifier bridge"
    assert abs(float(prezzo) - 97321.61) < 1.0, (
        f"field_states.prezzo_base_asta.value must be ~97321.61; got {prezzo!r}"
    )


def test_vidigulfo_r2_valore_stima_written_from_freeze_contract_to_field_states(monkeypatch):
    freeze_contract = {
        "case_key": "via_cristoforo_colombo_2_4",
        "status": "OK",
        "freeze_status": "frozen_with_context_only",
        "fields": {
            "document": {
                "valuation": {
                    "prezzo_base_raw": {
                        "state": "resolved_with_context",
                        "value": "€.   97.321,61",
                        "explanation": "A pag. 15 il paragrafo 8.5 riporta il prezzo base pari a €. 97.321,61.",
                        "context_qualification": "Prezzo base d'asta con spese a carico dell'acquirente.",
                        "why_not_fully_certain": "La cifra compare anche nel riepilogo iniziale.",
                        "supporting_evidence": [{"page": 15, "quote": "Prezzo base d'asta €. 97.321,61"}],
                        "supporting_pages": [15],
                    },
                    "valore_stima_raw": {
                        "state": "resolved_with_context",
                        "value": "€. 118.731,30",
                        "explanation": "Il valore è usato per valore stima perché il passaggio Valore complessivo del lotto lo sostiene direttamente.",
                        "context_qualification": "Il valore è espresso come Valore complessivo del lotto e coincide con Valore della quota di 1/1.",
                        "why_not_fully_certain": "La promozione automatica era stata bloccata come duplicato/recap, ma la finestra locale è chiara.",
                        "supporting_evidence": [{"page": 15, "quote": "Valore complessivo del lotto: €. 118.731,30"}],
                        "supporting_pages": [15],
                        "tension_pages": [15],
                    },
                }
            }
        },
    }
    monkeypatch.setattr(verifier_runtime_module, "_load_freeze_contract", lambda _: freeze_contract)
    pages = _vidigulfo_round2_pages()
    result = _vidigulfo_round2_result_seed()
    payload = run_quality_verifier(
        analysis_id="vidigulfo_r2_valore_stima_bridge",
        result=result,
        pages=pages,
        full_text="\n\n".join(p["text"] for p in pages),
        pdf_sha256="vidigulfo-live-shaped",
    )
    packaged = json.loads(json.dumps(result))
    before_valore = (packaged.get("field_states") or {}).get("valore_stima")
    before_prezzo = (packaged.get("field_states") or {}).get("prezzo_base_asta")
    apply_verifier_to_result(packaged, payload)

    valuation_contract = packaged["canonical_freeze_contract"]["fields"]["document"]["valuation"]
    valore_state = (packaged.get("field_states") or {}).get("valore_stima") or {}
    prezzo_state = (packaged.get("field_states") or {}).get("prezzo_base_asta") or {}
    assert before_valore is None
    assert before_prezzo is None
    assert valuation_contract["valore_stima_raw"]["state"] == "resolved_with_context"
    assert valuation_contract["valore_stima_raw"]["value"] == "€. 118.731,30"
    assert abs(float(valore_state.get("value")) - 118731.30) < 1.0
    assert valore_state.get("explanation") == valuation_contract["valore_stima_raw"]["explanation"]
    assert valore_state.get("context_qualification") == valuation_contract["valore_stima_raw"]["context_qualification"]
    assert valore_state.get("why_not_fully_certain") == valuation_contract["valore_stima_raw"]["why_not_fully_certain"]
    assert valore_state.get("resolver_meta", {}).get("source") == "canonical_freeze_contract.fields.document.valuation.valore_stima_raw"
    assert valore_state.get("resolver_meta", {}).get("canonical_pricing_source") == "canonical_pricing.benchmark_value"
    assert abs(float(prezzo_state.get("value")) - 97321.61) < 1.0


def test_vidigulfo_r2_summary_leads_with_non_opponibile_liberazione():
    payload = _vidigulfo_round2_payload()
    bundle = payload["canonical_case"]["summary_bundle"]
    decision = str(bundle.get("decision_summary_it") or "").lower()
    assert "non opponibile" in decision, (
        f"decision_summary_it must reference 'non opponibile'; got: {decision!r}"
    )
    assert "liberazion" in decision, (
        f"decision_summary_it must reference 'liberazione'; got: {decision!r}"
    )


def test_vidigulfo_r2_summary_does_not_say_immobile_occupato():
    pages = _vidigulfo_round2_pages()
    result = _vidigulfo_round2_result_seed()
    payload = run_quality_verifier(
        analysis_id="vidigulfo_r2_summary_occupato",
        result=result,
        pages=pages,
        full_text="\n\n".join(p["text"] for p in pages),
    )
    packaged = json.loads(json.dumps(result))
    apply_verifier_to_result(packaged, payload)
    summary_it = str((packaged.get("summary_for_client") or {}).get("summary_it") or "").lower()
    assert "immobile occupato" not in summary_it, (
        f"summary_it must not say 'immobile occupato' for LIBERO+NON_OPPONIBILE case; got: {summary_it!r}"
    )


def test_vidigulfo_r2_address_accepts_real_street_rejects_garbage():
    """_extract_address_state must accept Via Cristoforo Colombo and reject methodology paragraphs."""
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from server import _extract_address_state  # type: ignore[import]

    pages = _vidigulfo_round2_pages()

    # With a valid street address, value must contain the street name
    good_lots = [{"ubicazione": "Via Cristoforo Colombo 2/4, Vidigulfo", "evidence": {}}]
    good = _extract_address_state(pages, good_lots)
    assert good["value"] is not None, "must find address when lot.ubicazione is a real street"
    assert "colombo" in str(good["value"]).lower() or "vidigulfo" in str(good["value"]).lower(), (
        f"address value must reference Colombo or Vidigulfo; got {good['value']!r}"
    )

    # With a garbage methodology paragraph, must fall through to page scan
    garbage_lots = [
        {
            "ubicazione": (
                "La metodologia estimativa adottata si fonda sulla comparazione tra il "
                "complesso delle caratteristiche dell'unità immobiliare in esame e quello "
                "di altri immobili sostanzialmente analoghi di cui è stato accertato il "
                "prezzo di vendita."
            ),
            "evidence": {},
        }
    ]
    garbage = _extract_address_state(pages, garbage_lots)
    if garbage["value"] is not None:
        assert "metodologia" not in str(garbage["value"]).lower(), (
            f"address value must not contain methodology text; got {garbage['value']!r}"
        )


def test_vidigulfo_r2_delivery_timeline_matches_liberazione():
    """_extract_delivery_timeline_state must match 'liberazione a cura e spese della procedura'."""
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from server import _extract_delivery_timeline_state  # type: ignore[import]

    pages = _vidigulfo_round2_pages()
    state = _extract_delivery_timeline_state(pages)
    assert state["value"] is not None, (
        "delivery_timeline.value must not be None when 'liberazione a cura e spese della procedura' is present"
    )
    assert "liberazion" in str(state["value"]).lower(), (
        f"delivery_timeline.value must reference 'liberazione'; got {state['value']!r}"
    )


def test_vidigulfo_r2_money_box_excludes_valuation_amounts():
    """_select_cost_money_candidates must exclude valuation totals; include real cost €3.600."""
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from section_builder import _select_cost_money_candidates  # type: ignore[import]

    candidates = [
        # Valuation total — must be excluded
        {
            "amount_eur": 118731.30,
            "quote": "Valore complessivo del lotto: €. 118.731,30",
            "context": "Valore complessivo del lotto",
            "page": 15,
        },
        # Valuation reduction — must be excluded (contains "riduzione")
        {
            "amount_eur": 17809.69,
            "quote": "Riduzione del valore del 15% ... €. 17.809,69",
            "context": "riduzione cautelativa valore",
            "page": 15,
        },
        # Prezzo base — must be excluded (context contains "prezzo base")
        {
            "amount_eur": 97321.61,
            "quote": "Valore in caso di regolarizzazione ... €. 97.321,61",
            "context": "prezzo base d'asta spese a carico dell'acquirente",
            "page": 4,
        },
        # Real buyer cost — must be included
        {
            "amount_eur": 3600.00,
            "quote": "Spese tecniche di regolarizzazione urbanistico e/o catastale €. 3.600,00",
            "context": "spese tecniche regolarizzazione catastale",
            "page": 4,
        },
    ]

    selected = _select_cost_money_candidates(candidates)
    selected_amounts = [c["amount_eur"] for c in selected]

    assert 3600.0 in selected_amounts, (
        f"€3.600 (spese tecniche) must be selected as a buyer cost; got {selected_amounts}"
    )
    assert 118731.30 not in selected_amounts, (
        f"€118.731,30 (valore complessivo) must be excluded as a valuation amount; got {selected_amounts}"
    )
    assert 17809.69 not in selected_amounts, (
        f"€17.809,69 (riduzione cautelativa) must be excluded as a valuation amount; got {selected_amounts}"
    )
    assert 97321.61 not in selected_amounts, (
        f"€97.321,61 (prezzo base) must be excluded as a valuation amount; got {selected_amounts}"
    )


def test_vidigulfo_r2_api_payload_address_and_money_box_are_clean():
    import server  # type: ignore[import]
    from section_builder import _integrate_money_box_cost_items  # type: ignore[import]

    pages = _vidigulfo_round2_pages() + [
        {
            "page_number": 13,
            "text": (
                "La superficie commerciale è da intendersi come la somma della superficie lorda "
                "dell'appartamento, ragguagliata dalla superficie relativa ad accessori e "
                "pertinenze ai sensi della Norma UNI 10750/2005 e DPR. n. 138/98. "
                "Le percentuali indicate nelle citate normative possono variare in più o in meno "
                "in base ad un insieme di fattori, tra questi: la particolare ubicazione "
                "dell'immobile, l'entità delle superfici esterne, il livello dei piani."
            ),
        }
    ]
    result = _vidigulfo_round2_result_seed()
    result["lots"][0]["ubicazione"] = (
        "pertinenze ai sensi della Norma UNI 10750/2005 e DPR. n. 138/98. "
        "Le percentuali indicate nelle citate normative possono variare in più o in meno "
        "in base ad un insieme di fattori, tra questi: la particolare ubicazione "
        "dell'immobile, l'entità delle superfici esterne, il livello dei piani."
    )
    result["lots"][0]["evidence"]["ubicazione"] = [
        {
            "page": 13,
            "quote": result["lots"][0]["ubicazione"],
            "search_hint": "ubicazione dell'immobile",
        }
    ]
    result["money_box"] = {
        "items": [],
        "total_extra_costs": {"range": {"min": 0, "max": 0}, "max_is_open": False},
    }
    money_candidates = [
        {
            "amount_eur": 17809.69,
            "quote": "smaltimento di beni mobili presenti all'interno. €. 17.809,69 Spese relative a lavori di manutenzione",
            "context": "oneri tributari su base catastale e reale, per assenza di garanzia per vizi, rimborso forfettario e di eventuale smaltimento di beni mobili presenti all'interno. €. 17.809,69",
            "page": 15,
        },
        {
            "amount_eur": 97321.61,
            "quote": "catastale, spese di smaltimento rifiuti a carico dell'acquirente: €. 97.321,61",
            "context": "Prezzo base d'asta Valore in caso di regolarizzazione urbanistica e catastale, spese di smaltimento rifiuti a carico dell'acquirente: €. 97.321,61",
            "page": 15,
        },
        {
            "amount_eur": 97321.61,
            "quote": "Valore in caso di regolarizzazione urbanistica e catastale, spese di smaltimento rifiuti a carico dell'acquirente: €. 97.321,61",
            "context": "8.5. Prezzo base d'asta",
            "page": 4,
        },
        {
            "amount_eur": 3600.00,
            "quote": "Spese tecniche di regolarizzazione urbanistico e/o catastale €. 3.600,00",
            "context": "Spese tecniche di regolarizzazione urbanistico e/o catastale €. 3.600,00",
            "page": 4,
        },
    ]

    server._apply_headline_field_states(result, pages)
    _integrate_money_box_cost_items(result, money_candidates)
    result["section_3_money_box"] = json.loads(json.dumps(result["money_box"]))
    server._apply_market_ranges_to_money_box(result)

    address_value = (result.get("field_states") or {}).get("address", {}).get("value")
    assert address_value is not None
    assert "cristoforo colombo" in str(address_value).lower()
    assert "vidigulfo" in str(address_value).lower() or str(address_value).lower().startswith("via cristoforo colombo")
    assert "norma uni" not in str(address_value).lower()
    assert "particolare ubicazione" not in str(address_value).lower()
    lot_address = ((result.get("lots") or [{}])[0] or {}).get("ubicazione")
    assert lot_address == address_value

    money_items = (result.get("money_box") or {}).get("items") or []
    numeric_amounts = [
        round(float(item.get("stima_euro")), 2)
        for item in money_items
        if isinstance(item, dict) and isinstance(item.get("stima_euro"), (int, float))
    ]
    assert 17809.69 not in numeric_amounts
    assert 97321.61 not in numeric_amounts
