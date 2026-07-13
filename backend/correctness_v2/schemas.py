"""
Strict schemas / contracts for Correctness Mode v2.

This defines:
  * the full set of job STATUSES (all defined now, only a subset produced in step 1)
  * PDF quality status + reason codes
  * the diagnostic failure contract requirements

Every failure status MUST carry:
  reason_code, reason_human, troubleshoot_message, next_steps, artifacts_saved,
  customer_report_generated=false, safe_to_show_customer=false.

No vague "error occurred." is ever allowed.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

MODE = "correctness_v2"


# ---------------------------------------------------------------------------
# Job statuses (ALL defined now; step 1 only actually emits a subset)
# ---------------------------------------------------------------------------
class JobStatus:
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    PDF_QUALITY_OK = "PDF_QUALITY_OK"
    PDF_QUALITY_WARNING = "PDF_QUALITY_WARNING"
    PDF_QUALITY_BLOCKED = "PDF_QUALITY_BLOCKED"
    FAILED_ANALYSIS = "FAILED_ANALYSIS"
    FAILED_CONTRACT_BUILD = "FAILED_CONTRACT_BUILD"
    FAILED_GROUNDING = "FAILED_GROUNDING"
    CONTRACT_VALIDATION_FAILED = "CONTRACT_VALIDATION_FAILED"
    NEEDS_MANUAL_REVIEW = "NEEDS_MANUAL_REVIEW"
    # Multi-lot document with no safe single-lot selection. NOT a failure: it is
    # expected controlled behavior asking the caller to pick a lot (or analyze_all).
    LOT_SELECTION_REQUIRED = "LOT_SELECTION_REQUIRED"
    # A rendered report would otherwise HARD-BLOCK on a small number of GENUINE,
    # RESOLVABLE money-role ambiguities (an amount the document supports under two
    # readings, e.g. market value vs regularization cost). NOT a failure: like
    # LOT_SELECTION_REQUIRED it is controlled human-in-the-loop behavior asking the
    # customer to confirm which interpretation is correct; the confirmed answer is
    # then used as ground truth to produce the final clean report.
    MONEY_CONFIRMATION_REQUIRED = "MONEY_CONFIRMATION_REQUIRED"
    CONTRACT_READY = "CONTRACT_READY"
    # Customer-facing report rendered from the verified contract (step 3 renderer).
    REPORT_READY = "REPORT_READY"
    FAILED_NARRATION_USED_DETERMINISTIC_TEXT = "FAILED_NARRATION_USED_DETERMINISTIC_TEXT"
    FAILED_NARRATION_NO_REPORT = "FAILED_NARRATION_NO_REPORT"
    JOB_STALLED = "JOB_STALLED"
    CANCELLED = "CANCELLED"


ALL_STATUSES: List[str] = [
    JobStatus.QUEUED,
    JobStatus.RUNNING,
    JobStatus.PDF_QUALITY_OK,
    JobStatus.PDF_QUALITY_WARNING,
    JobStatus.PDF_QUALITY_BLOCKED,
    JobStatus.FAILED_ANALYSIS,
    JobStatus.FAILED_CONTRACT_BUILD,
    JobStatus.FAILED_GROUNDING,
    JobStatus.CONTRACT_VALIDATION_FAILED,
    JobStatus.NEEDS_MANUAL_REVIEW,
    JobStatus.LOT_SELECTION_REQUIRED,
    JobStatus.MONEY_CONFIRMATION_REQUIRED,
    JobStatus.CONTRACT_READY,
    JobStatus.REPORT_READY,
    JobStatus.FAILED_NARRATION_USED_DETERMINISTIC_TEXT,
    JobStatus.FAILED_NARRATION_NO_REPORT,
    JobStatus.JOB_STALLED,
    JobStatus.CANCELLED,
]

# Statuses that represent a terminal failure and MUST carry the full diagnostic
# contract (reason_code/reason_human/troubleshoot_message/next_steps/...).
FAILURE_STATUSES: List[str] = [
    JobStatus.PDF_QUALITY_BLOCKED,
    JobStatus.FAILED_ANALYSIS,
    JobStatus.FAILED_CONTRACT_BUILD,
    JobStatus.FAILED_GROUNDING,
    JobStatus.CONTRACT_VALIDATION_FAILED,
    JobStatus.FAILED_NARRATION_NO_REPORT,
    JobStatus.JOB_STALLED,
]

# Statuses that step 1 can actually produce.
STEP1_PRODUCED_STATUSES: List[str] = [
    JobStatus.QUEUED,
    JobStatus.RUNNING,
    JobStatus.PDF_QUALITY_OK,
    JobStatus.PDF_QUALITY_WARNING,
    JobStatus.PDF_QUALITY_BLOCKED,
    JobStatus.FAILED_ANALYSIS,
]

# Terminal statuses (job no longer running).
TERMINAL_STATUSES: List[str] = [
    JobStatus.PDF_QUALITY_OK,
    JobStatus.PDF_QUALITY_WARNING,
    JobStatus.PDF_QUALITY_BLOCKED,
    JobStatus.FAILED_ANALYSIS,
    JobStatus.FAILED_CONTRACT_BUILD,
    JobStatus.FAILED_GROUNDING,
    JobStatus.CONTRACT_VALIDATION_FAILED,
    JobStatus.NEEDS_MANUAL_REVIEW,
    JobStatus.LOT_SELECTION_REQUIRED,
    JobStatus.MONEY_CONFIRMATION_REQUIRED,
    JobStatus.CONTRACT_READY,
    JobStatus.REPORT_READY,
    JobStatus.FAILED_NARRATION_USED_DETERMINISTIC_TEXT,
    JobStatus.FAILED_NARRATION_NO_REPORT,
    JobStatus.JOB_STALLED,
    JobStatus.CANCELLED,
]


# ---------------------------------------------------------------------------
# PDF quality contract
# ---------------------------------------------------------------------------
class PdfQualityStatus:
    OK = "PDF_QUALITY_OK"
    WARNING = "PDF_QUALITY_WARNING"
    BLOCKED = "PDF_QUALITY_BLOCKED"


# Reason codes that BLOCK (fail closed).
class PdfBlockReason:
    DOCUMENT_TEXT_EMPTY = "DOCUMENT_TEXT_EMPTY"
    # PDF has "text" but it is not real language: images without OCR, scanned
    # pages, or a non-extractable/CID font that decodes to control-char / symbol
    # garbage. The customer must upload a readable (text-based) PDF.
    DOCUMENT_NOT_TEXT_EXTRACTABLE = "DOCUMENT_NOT_TEXT_EXTRACTABLE"
    TOO_MANY_UNREADABLE_PAGES = "TOO_MANY_UNREADABLE_PAGES"
    KEY_SECTIONS_UNREADABLE = "KEY_SECTIONS_UNREADABLE"
    PAGE_ORDER_BROKEN = "PAGE_ORDER_BROKEN"
    MONEY_TABLES_UNREADABLE = "MONEY_TABLES_UNREADABLE"
    OCR_EXTRACTION_FAILED = "OCR_EXTRACTION_FAILED"
    SCANNED_PDF_WITHOUT_USABLE_TEXT = "SCANNED_PDF_WITHOUT_USABLE_TEXT"


BLOCK_REASON_CODES: List[str] = [
    PdfBlockReason.DOCUMENT_TEXT_EMPTY,
    PdfBlockReason.DOCUMENT_NOT_TEXT_EXTRACTABLE,
    PdfBlockReason.TOO_MANY_UNREADABLE_PAGES,
    PdfBlockReason.KEY_SECTIONS_UNREADABLE,
    PdfBlockReason.PAGE_ORDER_BROKEN,
    PdfBlockReason.MONEY_TABLES_UNREADABLE,
    PdfBlockReason.OCR_EXTRACTION_FAILED,
    PdfBlockReason.SCANNED_PDF_WITHOUT_USABLE_TEXT,
]


# Reason codes that WARN (continue later, but flagged).
class PdfWarnReason:
    SOME_LOW_TEXT_PAGES = "SOME_LOW_TEXT_PAGES"
    SOME_KEY_SECTIONS_WEAK = "SOME_KEY_SECTIONS_WEAK"
    MONEY_TABLES_WEAK_BUT_PRESENT = "MONEY_TABLES_WEAK_BUT_PRESENT"
    PAGE_LABELS_UNCERTAIN = "PAGE_LABELS_UNCERTAIN"


WARN_REASON_CODES: List[str] = [
    PdfWarnReason.SOME_LOW_TEXT_PAGES,
    PdfWarnReason.SOME_KEY_SECTIONS_WEAK,
    PdfWarnReason.MONEY_TABLES_WEAK_BUT_PRESENT,
    PdfWarnReason.PAGE_LABELS_UNCERTAIN,
]


# Canonical key sections we expect to detect in a perizia.
KEY_SECTIONS: List[str] = [
    "lotto_beni",
    "possesso",
    "vincoli_oneri",
    "conformita",
    "valutazione",
    "costi_money",
]


# Required keys on every failure diagnostic payload.
REQUIRED_FAILURE_FIELDS: List[str] = [
    "reason_code",
    "reason_human",
    "troubleshoot_message",
    "next_steps",
    "artifacts_saved",
    "customer_report_generated",
    "safe_to_show_customer",
]


def is_failure_status(status: str) -> bool:
    return status in FAILURE_STATUSES


def validate_failure_payload(payload: Dict[str, Any]) -> List[str]:
    """
    Validate that a failure payload carries the full diagnostic contract.

    Returns a list of problems (empty list == valid). Does not raise.
    """
    problems: List[str] = []
    if not isinstance(payload, dict):
        return ["payload_not_a_dict"]

    for field in REQUIRED_FAILURE_FIELDS:
        if field not in payload:
            problems.append(f"missing_field:{field}")

    # Specific value constraints for a failure.
    rc = payload.get("reason_code")
    if not (isinstance(rc, str) and rc.strip()):
        problems.append("reason_code_empty")
    rh = payload.get("reason_human")
    if not (isinstance(rh, str) and rh.strip()):
        problems.append("reason_human_empty")
    tm = payload.get("troubleshoot_message")
    if not (isinstance(tm, str) and tm.strip()):
        problems.append("troubleshoot_message_empty")
    if not isinstance(payload.get("next_steps"), list):
        problems.append("next_steps_not_list")
    if not isinstance(payload.get("artifacts_saved"), dict):
        problems.append("artifacts_saved_not_dict")
    if payload.get("customer_report_generated") is not False:
        problems.append("customer_report_generated_must_be_false")
    if payload.get("safe_to_show_customer") is not False:
        problems.append("safe_to_show_customer_must_be_false")

    return problems
