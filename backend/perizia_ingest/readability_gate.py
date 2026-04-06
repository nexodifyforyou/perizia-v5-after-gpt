from __future__ import annotations

from typing import Any, Dict, List

from .surface_inventory import build_surface_inventory


READABLE_DOCUMENT = "READABLE_DOCUMENT"
READABLE_BUT_EXTRACTION_BAD = "READABLE_BUT_EXTRACTION_BAD"
UNREADABLE_FROM_AVAILABLE_SURFACES = "UNREADABLE_FROM_AVAILABLE_SURFACES"


def assess_document_readability(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    surface_inventory = build_surface_inventory(pages)
    summary = surface_inventory["summary"]
    inventory_pages = surface_inventory["pages"]
    page_count = int(summary["page_count"] or 0)
    total_alpha_chars = int(summary["total_alphabetic_chars"] or 0)
    degraded_ratio = float(summary["degraded_page_ratio"] or 0.0)
    empty_ratio = float(summary["effectively_empty_page_ratio"] or 0.0)
    suspicious_ratio = float(summary["suspicious_page_ratio"] or 0.0)
    heading_like_lines = sum(int(page.get("heading_like_line_count") or 0) for page in inventory_pages if isinstance(page, dict))

    if (
        page_count == 0
        or total_alpha_chars < 10
        or (empty_ratio >= 0.8 and total_alpha_chars < 20 and heading_like_lines == 0)
        or (degraded_ratio >= 0.85 and total_alpha_chars < 10)
    ):
        readability_verdict = UNREADABLE_FROM_AVAILABLE_SURFACES
        document_quality_note = (
            "Readability gate used extracted text surfaces only; rendered page images were not inspected. "
            "Available extracted text is too sparse to support reasoning."
        )
    elif degraded_ratio >= 0.5 or suspicious_ratio >= 0.3 or total_alpha_chars < 50:
        readability_verdict = READABLE_BUT_EXTRACTION_BAD
        document_quality_note = (
            "Readability gate used extracted text surfaces only; rendered page images were not inspected. "
            "Some readable content exists, but extraction looks sparse or degraded."
        )
    else:
        readability_verdict = READABLE_DOCUMENT
        document_quality_note = (
            "Readability gate used extracted text surfaces only; rendered page images were not inspected. "
            "Extracted text appears broadly usable for reasoning."
        )

    return {
        "readability_verdict": readability_verdict,
        "document_quality_note": document_quality_note,
        "surface_inventory": surface_inventory,
        "surface_inventory_pages": inventory_pages,
        "surface_inventory_summary": summary,
        "limitations": {
            "surface_mode": "text_only",
            "rendered_page_images_inspected": False,
        },
        "thresholds": {
            "unreadable_total_alphabetic_chars_lt": 10,
            "unreadable_effectively_empty_page_ratio_gte": 0.8,
            "unreadable_effectively_empty_total_alphabetic_chars_lt": 20,
            "unreadable_effectively_empty_heading_like_lines_eq": 0,
            "unreadable_when_degraded_page_ratio_gte": 0.85,
            "unreadable_when_degraded_total_alphabetic_chars_lt": 10,
            "extraction_bad_degraded_page_ratio_gte": 0.5,
            "extraction_bad_suspicious_page_ratio_gte": 0.3,
            "extraction_bad_total_alphabetic_chars_lt": 50,
        },
    }
