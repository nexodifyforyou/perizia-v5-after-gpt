"""Tests for the PDF quality blocker."""

from correctness_v2.pdf_quality import assess_pdf_quality
from correctness_v2.schemas import (
    KEY_SECTIONS,
    PdfBlockReason,
    PdfQualityStatus,
    PdfWarnReason,
)

from .sample_pages import (
    EMPTY_PAGES,
    GOOD_PAGES,
    MANY_UNREADABLE_PAGES,
    MISSING_KEY_SECTIONS_PAGES,
    WARNING_PAGES,
)


def test_empty_text_is_blocked_document_text_empty():
    report = assess_pdf_quality(EMPTY_PAGES)
    assert report["quality_status"] == PdfQualityStatus.BLOCKED
    assert report["reason_code"] == PdfBlockReason.DOCUMENT_TEXT_EMPTY
    assert report["reason_human"]
    assert report["troubleshoot_message"]
    assert isinstance(report["next_steps"], list) and report["next_steps"]


def test_no_pages_is_blocked():
    report = assess_pdf_quality([])
    assert report["quality_status"] == PdfQualityStatus.BLOCKED
    assert report["reason_code"] == PdfBlockReason.DOCUMENT_TEXT_EMPTY
    assert report["total_pages"] == 0


def test_many_unreadable_pages_blocked():
    report = assess_pdf_quality(MANY_UNREADABLE_PAGES)
    assert report["quality_status"] == PdfQualityStatus.BLOCKED
    assert report["reason_code"] == PdfBlockReason.TOO_MANY_UNREADABLE_PAGES
    # The blank pages are reported by physical index.
    assert set(report["unreadable_pages"]) == {1, 2, 4}
    assert report["readable_pages"] == 1


def test_scanned_pdf_without_usable_text_blocked():
    pages = [{"page_number": i, "text": "x"} for i in range(1, 6)]
    report = assess_pdf_quality(pages)
    assert report["quality_status"] == PdfQualityStatus.BLOCKED
    assert report["reason_code"] == PdfBlockReason.SCANNED_PDF_WITHOUT_USABLE_TEXT


def test_ocr_failed_with_empty_text_blocked():
    report = assess_pdf_quality(EMPTY_PAGES, ocr_failed=True)
    assert report["quality_status"] == PdfQualityStatus.BLOCKED
    # DOCUMENT_TEXT_EMPTY has higher priority than OCR_EXTRACTION_FAILED, but
    # OCR_EXTRACTION_FAILED must still be recorded among all block reasons.
    assert report["reason_code"] == PdfBlockReason.DOCUMENT_TEXT_EMPTY
    assert PdfBlockReason.OCR_EXTRACTION_FAILED in report["details"]["block_reasons_all"]


def test_control_char_garbage_blocked_not_extractable():
    # A non-extractable / CID-font PDF decodes to control-char noise. It carries
    # plenty of "characters" per page (so it clears the scanned/empty guards) but
    # is not real text. It must block with the dedicated, customer-clear reason.
    garbage = ("\x01\x02\x03\x04\x05\x06\x07\x08 " * 60).strip()
    pages = [{"page_number": i, "text": garbage} for i in range(1, 9)]
    report = assess_pdf_quality(pages)
    assert report["quality_status"] == PdfQualityStatus.BLOCKED
    assert report["reason_code"] == PdfBlockReason.DOCUMENT_NOT_TEXT_EXTRACTABLE
    assert report["details"]["doc_control_ratio"] > 0.15
    # Customer-facing message must tell them to upload a readable PDF.
    assert report["reason_human"]
    assert "leggibile" in " ".join(report["next_steps"]).lower()


def test_symbol_garbage_low_letter_ratio_blocked():
    # CID garbage that decodes to punctuation instead of control chars: almost no
    # letters. The letter-ratio floor catches it as not text-extractable.
    garbage = ('!"#$%#&"$\'!())!*+%,-./:;<=>?@ ' * 40).strip()
    pages = [{"page_number": i, "text": garbage} for i in range(1, 9)]
    report = assess_pdf_quality(pages)
    assert report["quality_status"] == PdfQualityStatus.BLOCKED
    assert report["reason_code"] == PdfBlockReason.DOCUMENT_NOT_TEXT_EXTRACTABLE
    assert report["details"]["doc_letter_ratio"] < 0.45


def test_good_document_not_flagged_as_garbage():
    # Guardrail: a legitimate letter-dominated document must never trip the
    # non-extractable detector.
    report = assess_pdf_quality(GOOD_PAGES)
    assert report["reason_code"] != PdfBlockReason.DOCUMENT_NOT_TEXT_EXTRACTABLE
    assert report["details"]["doc_control_ratio"] == 0.0
    assert report["details"]["doc_letter_ratio"] >= 0.45


def test_missing_key_sections_blocked():
    report = assess_pdf_quality(MISSING_KEY_SECTIONS_PAGES)
    assert report["quality_status"] == PdfQualityStatus.BLOCKED
    assert report["reason_code"] == PdfBlockReason.KEY_SECTIONS_UNREADABLE
    # lotto + possesso detected; valuation/costi/vincoli/conformita missing.
    detected = report["key_sections_detected"]
    assert detected["lotto_beni"] is True
    assert detected["possesso"] is True
    assert detected["valutazione"] is False
    assert detected["costi_money"] is False


def test_good_document_is_ok():
    report = assess_pdf_quality(GOOD_PAGES)
    assert report["quality_status"] == PdfQualityStatus.OK, report
    assert report["reason_code"] is None
    assert report["warnings"] == []
    assert report["page_order_ok"] is True
    # All six key sections present.
    for section in KEY_SECTIONS:
        assert report["key_sections_detected"][section] is True, section
    assert report["readable_pages"] == report["total_pages"] == 2


def test_warning_document_when_one_section_weak():
    report = assess_pdf_quality(WARNING_PAGES)
    assert report["quality_status"] == PdfQualityStatus.WARNING, report
    assert PdfWarnReason.SOME_KEY_SECTIONS_WEAK in report["warnings"]
    assert report["key_sections_detected"]["possesso"] is False
    # Warning is not a failure: no block reason code is set.
    assert report["reason_code"] is None


def test_report_records_visible_page_labels():
    report = assess_pdf_quality(GOOD_PAGES)
    labels = [p["visible_label"] for p in report["details"]["pages"]]
    assert "Pagina 1 di 2" in labels
    assert "Pagina 2 di 2" in labels


def test_page_order_broken_is_blocked():
    pages = [
        {"page_number": 1, "text": "Relazione di stima del bene. Pagina 5 di 6. " * 5},
        {"page_number": 2, "text": "Descrizione del bene e pertinenze. Pagina 2 di 6. " * 5},
    ]
    report = assess_pdf_quality(pages)
    assert report["page_order_ok"] is False
    assert report["quality_status"] == PdfQualityStatus.BLOCKED
    assert PdfBlockReason.PAGE_ORDER_BROKEN in report["details"]["block_reasons_all"]
