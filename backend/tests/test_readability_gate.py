from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from perizia_ingest.readability_gate import (
    READABLE_BUT_EXTRACTION_BAD,
    READABLE_DOCUMENT,
    UNREADABLE_FROM_AVAILABLE_SURFACES,
    assess_document_readability,
)
from perizia_runtime.evidence_mode import (
    DEGRADED_TEXT,
    STOP_UNREADABLE,
    TEXT_FIRST,
    select_evidence_mode,
)
from perizia_runtime.runtime import apply_verifier_to_result, run_quality_verifier


def test_readability_gate_marks_normal_text_as_readable_document():
    pages = [
        {
            "page_number": 1,
            "text": (
                "TRIBUNALE DI MILANO\nLOTTO 1\nDescrizione del bene\n"
                "L'appartamento si sviluppa su due livelli con soggiorno, cucina, due camere e servizi.\n"
                "La superficie commerciale e i confini sono riportati nella perizia."
            ),
        }
    ]

    payload = assess_document_readability(pages)

    assert payload["readability_verdict"] == READABLE_DOCUMENT
    assert payload["surface_inventory_pages"][0]["heading_like_line_count"] >= 2
    assert payload["limitations"]["rendered_page_images_inspected"] is False


def test_readability_gate_marks_sparse_corrupted_text_as_extraction_bad():
    pages = [
        {
            "page_number": 1,
            "text": (
                "LOTTO 1\n"
                "Appartamento piano primo con soggiorno, cucina e camera. "
                "Confini e consistenza risultano in parte leggibili.\n"
            ),
        },
        {
            "page_number": 2,
            "text": "A�A�A�\n12 34\n",
        },
    ]

    payload = assess_document_readability(pages)

    assert payload["readability_verdict"] == READABLE_BUT_EXTRACTION_BAD
    assert payload["surface_inventory_pages"][1]["suspicious_replacement_count"] >= 3
    assert payload["surface_inventory_summary"]["degraded_pages"] >= 1


def test_readability_gate_marks_effectively_empty_text_as_unreadable():
    pages = [
        {"page_number": 1, "text": "12\n34\n"},
        {"page_number": 2, "text": " \n \n"},
    ]

    payload = assess_document_readability(pages)

    assert payload["readability_verdict"] == UNREADABLE_FROM_AVAILABLE_SURFACES
    assert payload["surface_inventory_summary"]["effectively_empty_pages"] == 2


def test_evidence_mode_selection_matches_readability_verdicts():
    assert select_evidence_mode(READABLE_DOCUMENT)["evidence_mode"] == TEXT_FIRST
    assert select_evidence_mode(READABLE_BUT_EXTRACTION_BAD)["evidence_mode"] == DEGRADED_TEXT
    assert select_evidence_mode(UNREADABLE_FROM_AVAILABLE_SURFACES)["evidence_mode"] == STOP_UNREADABLE


def test_verifier_runtime_text_first_runs_normal_reasoning():
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
                "TRIBUNALE DI MILANO\nLOTTO UNICO\nBene N° 1 - Appartamento\nL'immobile risulta agibile.\n"
                "Prezzo base d'asta Euro 129.312,00.\nLa descrizione del bene e i confini sono riportati nella perizia."
            ),
        }
    ]

    payload = run_quality_verifier(
        analysis_id="text_first_probe",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    apply_verifier_to_result(result, payload)

    assert payload["readability_verdict"] == READABLE_DOCUMENT
    assert payload["evidence_mode"] == TEXT_FIRST
    assert payload["reasoning_status"] == "NORMAL"
    assert "document_root" in payload["scopes"]
    assert payload["canonical_case"]["agibilita"]["status"] == "PRESENTE"
    assert payload["canonical_case"]["agibilita"]["confidence"] == 0.9
    assert payload["verifier_cautions"] == []
    assert payload["source_quality_note"] is None
    assert payload["packaging_guards"] == []
    assert "source_quality_note" not in payload["canonical_case"]["agibilita"]
    assert result["document_quality"]["source_quality_note"] is None


def test_verifier_runtime_degraded_text_caps_packaged_confidence_and_adds_guards():
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
                "L'immobile risulta agibile.\n"
            ),
        },
        {
            "page_number": 2,
            "text": "A�A�A�\n12 34\n",
        },
    ]

    payload = run_quality_verifier(
        analysis_id="degraded_text_probe",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    apply_verifier_to_result(result, payload)

    assert payload["readability_verdict"] == READABLE_BUT_EXTRACTION_BAD
    assert payload["evidence_mode"] == DEGRADED_TEXT
    assert payload["reasoning_status"] == "DEGRADED_TEXT_CAUTION"
    assert payload["verifier_cautions"][0]["code"] == "degraded_text_sources"
    assert "degraded" in payload["verifier_cautions"][0]["message"].lower()
    assert payload["source_quality_note"]
    assert payload["packaging_guards"] == ["degraded_source_text_only", "confidence_capped_due_to_extraction_quality"]
    assert payload["canonical_case"]["agibilita"]["status"] == "PRESENTE"
    assert payload["canonical_case"]["agibilita"]["confidence"] == 0.6
    assert "degraded_source_text_only" in payload["canonical_case"]["agibilita"]["guards"]
    assert payload["canonical_case"]["agibilita"]["source_quality_note"] == payload["source_quality_note"]
    assert result["document_quality"]["verifier_cautions"] == payload["verifier_cautions"]
    assert result["document_quality"]["source_quality_note"] == payload["source_quality_note"]
    assert result["field_states"]["opponibilita_occupazione"]["status"] == "LOW_CONFIDENCE"
    assert result["field_states"]["opponibilita_occupazione"]["guards"] == [
        "degraded_source_text_only",
        "confidence_capped_due_to_extraction_quality",
    ]
    assert result["field_states"]["opponibilita_occupazione"]["source_quality_note"] == payload["source_quality_note"]
    assert result["summary_for_client"]["source_quality_note"] == payload["source_quality_note"]


def test_verifier_runtime_stop_unreadable_suppresses_reasoning():
    result = {
        "field_states": {},
        "dati_certi_del_lotto": {},
        "document_quality": {"status": "TEXT_OK"},
        "semaforo_generale": {"status": "AMBER"},
    }
    pages = [
        {
            "page_number": 1,
            "text": "12\n34\n",
        }
    ]

    payload = run_quality_verifier(
        analysis_id="readability_probe",
        result=result,
        pages=pages,
        full_text="\n\n".join(page["text"] for page in pages),
    )
    apply_verifier_to_result(result, payload)

    assert payload["readability_verdict"] == UNREADABLE_FROM_AVAILABLE_SURFACES
    assert payload["evidence_mode"] == STOP_UNREADABLE
    assert payload["reasoning_status"] == "SUPPRESSED_UNREADABLE"
    assert payload["canonical_case"] == {}
    assert payload["scopes"] == {}
    assert payload["comparison"] == {}
    assert payload["qa_checks"] == []
    assert "surface_inventory_pages" in payload
    assert result["document_quality"]["readability_verdict"] == UNREADABLE_FROM_AVAILABLE_SURFACES
    assert result["document_quality"]["evidence_mode"] == STOP_UNREADABLE
    assert result["document_quality"]["evidence_mode_reason"] == payload["evidence_mode_reason"]
    assert result["document_quality"]["reasoning_status"] == "SUPPRESSED_UNREADABLE"
    assert result["field_states"] == {}
    assert result["dati_certi_del_lotto"] == {}
    assert "rendered page images were not inspected" in result["document_quality"]["document_quality_note"]
