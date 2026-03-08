import re
from typing import Tuple


_SENTENCE_BOUNDARY_CHARS = ".;:?!"


def _normalize_ws(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _snap_word_left(text: str, idx: int) -> int:
    i = max(0, min(idx, len(text)))
    while 0 < i < len(text) and text[i - 1].isalnum() and text[i].isalnum():
        i -= 1
    return i


def _snap_word_right(text: str, idx: int) -> int:
    i = max(0, min(idx, len(text)))
    while 0 < i < len(text) and text[i - 1].isalnum() and text[i].isalnum():
        i += 1
    return i


def _find_left_boundary(text: str, start: int) -> int:
    s = max(0, min(start, len(text)))
    for i in range(s - 1, -1, -1):
        if text[i] in _SENTENCE_BOUNDARY_CHARS:
            return i + 1
    nl = text.rfind("\n", 0, s)
    if nl >= 0:
        return nl + 1
    for i in range(s - 1, -1, -1):
        if text[i].isspace():
            return i + 1
    return 0


def _find_right_boundary(text: str, end: int) -> int:
    e = max(0, min(end, len(text)))
    for i in range(e, len(text)):
        if text[i] in _SENTENCE_BOUNDARY_CHARS:
            return i + 1
    nl = text.find("\n", e)
    if nl >= 0:
        return nl
    for i in range(e, len(text)):
        if text[i].isspace():
            return i
    return len(text)


def normalize_evidence_quote(
    page_text: str,
    start_offset: int,
    end_offset: int,
    max_len: int = 520,
) -> Tuple[str, str]:
    text = str(page_text or "")
    n = len(text)
    if n == 0:
        return "", ""

    start = max(0, min(int(start_offset), n))
    end = max(start, min(int(end_offset), n))

    left = _find_left_boundary(text, start)
    right = _find_right_boundary(text, end)
    left = _snap_word_left(text, left)
    right = _snap_word_right(text, right)
    right = max(right, min(n, end))

    quote = text[left:right].strip()
    if not quote:
        quote = text[start:end].strip()
        left = start
        right = end

    if len(quote) > max_len:
        limit = min(n, left + max_len)
        cut = limit
        for i in range(limit, left, -1):
            ch = text[i - 1]
            if ch in _SENTENCE_BOUNDARY_CHARS or ch == "\n" or ch.isspace():
                cut = i
                break
        cut = _snap_word_right(text, cut)
        quote = text[left:cut].strip()
        if len(quote) > max_len:
            quote = quote[:max_len].rstrip()

    if not quote:
        quote = text[start:end].strip()[:max_len]

    around_start = max(0, start - 30)
    around_end = min(n, end + 30)
    if around_end - around_start > 60:
        mid = (start + end) // 2
        around_start = max(0, mid - 30)
        around_end = min(n, around_start + 60)
    search_hint = _normalize_ws(text[around_start:around_end])

    return _normalize_ws(quote), search_hint
