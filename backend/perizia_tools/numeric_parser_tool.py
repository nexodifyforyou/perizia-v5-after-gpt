from __future__ import annotations

import re
from typing import Optional

_MONEY_RE = re.compile(r"€\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)|([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)\s*€")
_FRACTION_RE = re.compile(r"\b(\d{1,3})\s*/\s*(\d{1,3})\b")


def parse_it_money(text: str) -> Optional[float]:
    match = _MONEY_RE.search(str(text or ""))
    if not match:
        return None
    raw = next((x for x in match.groups() if x), "")
    return parse_it_number(raw)


def parse_it_number(text: str) -> Optional[float]:
    cleaned = str(text or "").strip().replace(".", "").replace(",", ".")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except Exception:
        return None


def parse_fraction(text: str) -> Optional[str]:
    match = _FRACTION_RE.search(str(text or ""))
    if not match:
        return None
    return f"{int(match.group(1))}/{int(match.group(2))}"

