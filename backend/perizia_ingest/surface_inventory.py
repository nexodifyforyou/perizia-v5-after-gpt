from __future__ import annotations

import re
from typing import Any, Dict, List


_HEADING_PREFIX_RE = re.compile(
    r"^(lotto|bene|sezione|capitolo|allegato|art\.?|tribunale|confini|dati|provenienza|occupazione)\b",
    flags=re.IGNORECASE,
)


def _is_heading_like_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    alpha_count = sum(1 for char in stripped if char.isalpha())
    if alpha_count < 4:
        return False
    if _HEADING_PREFIX_RE.match(stripped):
        return True
    if stripped.endswith(":"):
        return True
    if stripped.upper() == stripped and alpha_count >= 6:
        return True
    return False


def _page_number(page: Dict[str, Any], fallback: int) -> int:
    raw_value = page.get("page_number") or page.get("page") or fallback
    try:
        return int(raw_value)
    except Exception:
        return fallback


def build_surface_inventory(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    normalized_pages: List[Dict[str, Any]] = []
    degraded_pages = 0
    effectively_empty_pages = 0
    suspicious_pages = 0
    total_alpha_chars = 0

    for idx, page in enumerate(pages or [], start=1):
        if not isinstance(page, dict):
            continue
        page_number = _page_number(page, idx)
        text = str(page.get("text") or "")
        stripped_text = text.strip()
        lines = [line for line in text.splitlines() if line.strip()]
        non_whitespace_chars = [char for char in text if not char.isspace()]
        non_ws_count = len(non_whitespace_chars)
        alpha_count = sum(1 for char in non_whitespace_chars if char.isalpha())
        digit_count = sum(1 for char in non_whitespace_chars if char.isdigit())
        suspicious_replacement_count = text.count("\ufffd") + text.count("�")
        heading_like_line_count = sum(1 for line in lines if _is_heading_like_line(line))
        alpha_ratio = (alpha_count / non_ws_count) if non_ws_count else 0.0
        digit_ratio = (digit_count / non_ws_count) if non_ws_count else 0.0
        is_effectively_empty = len(stripped_text) < 40 or alpha_count < 20
        is_degraded = bool(
            is_effectively_empty
            or alpha_ratio < 0.35
            or suspicious_replacement_count >= 3
            or (len(lines) <= 1 and alpha_count < 60)
        )
        if is_degraded:
            degraded_pages += 1
        if is_effectively_empty:
            effectively_empty_pages += 1
        if suspicious_replacement_count > 0:
            suspicious_pages += 1
        total_alpha_chars += alpha_count
        normalized_pages.append(
            {
                "page_number": page_number,
                "extracted_text_length": len(text),
                "non_whitespace_char_count": non_ws_count,
                "alphabetic_char_count": alpha_count,
                "alphabetic_char_ratio": round(alpha_ratio, 6),
                "digit_char_count": digit_count,
                "digit_char_ratio": round(digit_ratio, 6),
                "line_count": len(lines),
                "suspicious_replacement_count": suspicious_replacement_count,
                "heading_like_line_count": heading_like_line_count,
                "is_effectively_empty": is_effectively_empty,
                "is_degraded": is_degraded,
            }
        )

    page_count = len(normalized_pages)
    degraded_ratio = (degraded_pages / page_count) if page_count else 0.0
    empty_ratio = (effectively_empty_pages / page_count) if page_count else 0.0
    suspicious_ratio = (suspicious_pages / page_count) if page_count else 0.0

    return {
        "pages": normalized_pages,
        "summary": {
            "page_count": page_count,
            "degraded_pages": degraded_pages,
            "effectively_empty_pages": effectively_empty_pages,
            "pages_with_suspicious_replacements": suspicious_pages,
            "degraded_page_ratio": round(degraded_ratio, 6),
            "effectively_empty_page_ratio": round(empty_ratio, 6),
            "suspicious_page_ratio": round(suspicious_ratio, 6),
            "total_alphabetic_chars": total_alpha_chars,
        },
    }
