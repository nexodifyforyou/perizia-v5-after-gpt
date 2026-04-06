from __future__ import annotations

from typing import Dict

from perizia_ingest.readability_gate import (
    READABLE_BUT_EXTRACTION_BAD,
    READABLE_DOCUMENT,
    UNREADABLE_FROM_AVAILABLE_SURFACES,
)


TEXT_FIRST = "TEXT_FIRST"
DEGRADED_TEXT = "DEGRADED_TEXT"
STOP_UNREADABLE = "STOP_UNREADABLE"


_MODE_BY_VERDICT = {
    READABLE_DOCUMENT: (
        TEXT_FIRST,
        "Existing text surfaces are readable enough for text-first reasoning.",
    ),
    READABLE_BUT_EXTRACTION_BAD: (
        DEGRADED_TEXT,
        "Some text is usable, but the readability gate marked extraction as degraded.",
    ),
    UNREADABLE_FROM_AVAILABLE_SURFACES: (
        STOP_UNREADABLE,
        "Available extracted text is too weak for reliable reasoning; stop on unreadable text surfaces.",
    ),
}


def select_evidence_mode(readability_verdict: str) -> Dict[str, str]:
    mode, reason = _MODE_BY_VERDICT.get(
        str(readability_verdict or ""),
        (
            STOP_UNREADABLE,
            "Readability verdict was missing or unknown; defaulting to unreadable stop mode.",
        ),
    )
    return {
        "evidence_mode": mode,
        "evidence_mode_reason": reason,
    }
