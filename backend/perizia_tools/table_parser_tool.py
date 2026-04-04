from __future__ import annotations

from typing import Any, Dict, List


def table_like_blocks(pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for idx, page in enumerate(pages or [], start=1):
        text = str((page or {}).get("text") or "")
        page_number = int((page or {}).get("page_number") or (page or {}).get("page") or idx)
        if "€" in text and ("\n\n" in text or "coefficiente" in text.lower()):
            out.append({"page": page_number, "text": text})
    return out

