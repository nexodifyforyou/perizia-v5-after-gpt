from __future__ import annotations

from typing import Any, Dict, List


def build_pdf_text_payload(pages: List[Dict[str, Any]], full_text: str) -> Dict[str, Any]:
    normalized_pages: List[Dict[str, Any]] = []
    for idx, page in enumerate(pages or [], start=1):
        if not isinstance(page, dict):
            continue
        page_number = int(page.get("page_number") or page.get("page") or idx)
        text = str(page.get("text") or "")
        normalized_pages.append({"page_number": page_number, "text": text})
    return {
        "pages": normalized_pages,
        "full_text": full_text or "\n\n".join(p["text"] for p in normalized_pages),
    }

