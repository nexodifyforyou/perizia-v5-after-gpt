from __future__ import annotations

import re
from typing import Any, Dict, List


def find_keyword_spans(pages: List[Dict[str, Any]], pattern: str, flags: int = re.IGNORECASE) -> List[Dict[str, Any]]:
    rx = re.compile(pattern, flags)
    out: List[Dict[str, Any]] = []
    for idx, page in enumerate(pages or [], start=1):
        text = str((page or {}).get("text") or "")
        page_number = int((page or {}).get("page_number") or (page or {}).get("page") or idx)
        for match in rx.finditer(text):
            start = max(0, match.start() - 120)
            end = min(len(text), match.end() + 120)
            out.append(
                {
                    "page": page_number,
                    "quote": text[start:end].strip(),
                    "match": match.group(0),
                    "start": match.start(),
                    "end": match.end(),
                }
            )
    return out

