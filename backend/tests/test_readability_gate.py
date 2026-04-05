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


def test_verifier_runtime_exposes_readability_fields_and_result_note():
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
    assert "stop" in payload["evidence_mode_reason"].lower()
    assert "surface_inventory_pages" in payload
    assert result["document_quality"]["readability_verdict"] == UNREADABLE_FROM_AVAILABLE_SURFACES
    assert result["document_quality"]["evidence_mode"] == STOP_UNREADABLE
    assert result["document_quality"]["evidence_mode_reason"] == payload["evidence_mode_reason"]
    assert "rendered page images were not inspected" in result["document_quality"]["document_quality_note"]
