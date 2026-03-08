import re
from typing import Dict, List, Optional, Tuple


_SENTENCE_BOUNDARY_CHARS = ".;:?!"
_GENERIC_BLEED_LABELS = [
    "bene n",
    "ubicazione",
    "tipologia immobile",
    "diritto reale",
    "superficie",
    "stato conservativo",
    "stato occupativo",
    "conformità urbanistica",
    "conformita urbanistica",
    "conformità catastale",
    "conformita catastale",
    "agibilità",
    "agibilita",
    "ape",
    "spese condominiali",
]
_FIELD_KEYWORDS: Dict[str, List[str]] = {
    "tribunale": ["tribunale", "tribunale di"],
    "procedura": ["r.g.e", "rge", "procedura", "esecuzione immobiliare"],
    "lotto": ["lotto", "lotti", "lotto unico"],
    "prezzo_base_asta": ["prezzo base", "prezzo base d'asta", "prezzo base asta", "€"],
    "superficie_catastale": ["superficie", "mq", "m²"],
    "superficie": ["superficie", "mq", "m²"],
    "diritto_reale": ["diritto reale", "proprietà", "nuda proprietà", "usufrutto"],
    "stato_occupativo": ["occupato", "libero", "debitore", "coniuge", "locazione"],
    "conformita_urbanistica": ["conformità urbanistica", "abusi", "sanatoria", "condono"],
    "regolarita_urbanistica": ["conformità urbanistica", "abusi", "sanatoria", "condono"],
    "conformita_catastale": ["conformità catastale", "catasto", "planimetria", "difformità"],
    "ape": ["ape", "attestato di prestazione energetica", "certificato energetico"],
    "spese_condominiali_arretrate": ["spese condominiali", "arretrate", "arretrati", "morosità"],
    "agibilita": ["agibilità", "agibilita", "abitabilità", "abitabilita"],
}
_FIELD_BLEED_LABELS: Dict[str, List[str]] = {
    "prezzo_base_asta": ["bene n", "ubicazione", "diritto reale", "tipologia immobile", "superficie", "stato occupativo"],
    "superficie_catastale": ["tipologia immobile", "diritto reale", "ubicazione", "bene n", "stato conservativo", "stato occupativo"],
    "superficie": ["tipologia immobile", "diritto reale", "ubicazione", "bene n", "stato conservativo", "stato occupativo"],
    "diritto_reale": ["tipologia immobile", "superficie", "ubicazione", "bene n", "stato occupativo"],
    "conformita_urbanistica": ["valore tipo", "deprezzamento", "valore finale", "rischio assunto", "bene n"],
    "conformita_catastale": ["bene n", "valore tipo", "deprezzamento"],
    "agibilita": ["deprezzamento", "valore finale", "rischio assunto", "bene n"],
}


def _normalize_ws(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _normalize_multiline_ws(text: str) -> str:
    cleaned_lines: List[str] = []
    for line in str(text or "").splitlines():
        line = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]+", " ", line)
        line = re.sub(r"[•·▪●◦\t]+", " ", line)
        line = re.sub(r"\s{2,}", " ", line).strip()
        if line:
            cleaned_lines.append(line)
    return "\n".join(cleaned_lines).strip()


def _collapse_ocr_spaced_caps(text: str) -> str:
    # Collapse obvious OCR artifacts like "T R I B U N A L E" -> "TRIBUNALE".
    text = re.sub(r"\b(?:[A-ZÀ-Ù]\s+){3,}[A-ZÀ-Ù]\b", lambda m: re.sub(r"\s+", "", m.group(0)), text)

    # Collapse fragmented all-caps clusters like "SE ZI ON E" / "P IANO".
    token_rx = re.compile(r"\b[A-ZÀ-Ù]{1,5}\b")
    out_lines: List[str] = []
    for line in str(text or "").splitlines():
        parts: List[str] = []
        pos = 0
        tokens = list(token_rx.finditer(line))
        i = 0
        while i < len(tokens):
            t = tokens[i]
            if t.start() < pos:
                i += 1
                continue
            run = [t]
            j = i
            while j + 1 < len(tokens):
                a = tokens[j]
                b = tokens[j + 1]
                gap = line[a.end():b.start()]
                if not re.fullmatch(r"\s+", gap or ""):
                    break
                run.append(b)
                j += 1
            merge = False
            if len(run) >= 3:
                avg_len = sum(len(x.group(0)) for x in run) / float(len(run))
                if avg_len <= 2.4:
                    merge = True
            elif len(run) == 2:
                a_len = len(run[0].group(0))
                b_len = len(run[1].group(0))
                if min(a_len, b_len) == 1 and max(a_len, b_len) <= 5:
                    merge = True
            if merge:
                parts.append(line[pos:run[0].start()])
                parts.append("".join(x.group(0) for x in run))
                pos = run[-1].end()
                i = j + 1
                continue
            i += 1
        parts.append(line[pos:])
        out_lines.append("".join(parts))
    return "\n".join(out_lines)


def _clean_quote_text(text: str) -> str:
    cleaned = _normalize_multiline_ws(text)
    cleaned = _collapse_ocr_spaced_caps(cleaned)
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
    return cleaned


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


def _iter_line_spans(text: str) -> List[Tuple[int, int]]:
    spans: List[Tuple[int, int]] = []
    cursor = 0
    for line in text.splitlines(keepends=True):
        start = cursor
        end = cursor + len(line)
        spans.append((start, end))
        cursor = end
    if not spans:
        spans.append((0, len(text)))
    return spans


def _line_index_for_offset(spans: List[Tuple[int, int]], offset: int) -> int:
    for idx, (s, e) in enumerate(spans):
        if s <= offset <= e:
            return idx
    return max(0, len(spans) - 1)


def _is_probable_label_line(line: str) -> bool:
    txt = _normalize_ws(line).lower()
    if not txt:
        return False
    if re.match(r"^(bene\s*n[°º\.]?\s*\d+|lotto\s+\d+)\b", txt):
        return True
    if re.match(r"^[a-zà-ù' ]{3,35}\s*:\s*$", txt):
        return True
    return any(lbl in txt for lbl in _GENERIC_BLEED_LABELS)


def _is_toc_like_quote(quote: str) -> bool:
    compact = _normalize_ws(str(quote or ""))
    if not compact:
        return False
    low = compact.lower()
    if "sommario" in low or "indice" in low:
        return True
    if (re.search(r"\.{4,}|…{2,}", compact) and re.search(r"\b\d{1,3}\s*$", compact)):
        return True
    if re.search(r"\bLOTTO\s+(UNICO|\d+)\b", compact, re.I) and re.search(r"\b\d{1,3}\s*$", compact):
        if "." in compact or "…" in compact:
            return True
    if len(compact) <= 90 and re.search(r"\b\d{1,3}\s*$", compact) and ":" not in compact:
        # TOC-like short row with trailing page number and no explicit field-value separator.
        if len(compact.split()) <= 8:
            return True
    return False


def _apply_bleed_trim(quote: str, field_key: Optional[str]) -> str:
    if not quote:
        return quote
    labels = list(_GENERIC_BLEED_LABELS)
    if field_key and field_key in _FIELD_BLEED_LABELS:
        labels.extend(_FIELD_BLEED_LABELS[field_key])
    key = str(field_key or "").lower()
    skip_by_field = {
        "superficie": {"superficie"},
        "superficie_catastale": {"superficie"},
        "diritto_reale": {"diritto reale"},
        "stato_occupativo": {"stato occupativo"},
        "conformita_urbanistica": {"conformità urbanistica", "conformita urbanistica"},
        "conformita_catastale": {"conformità catastale", "conformita catastale"},
    }
    skip_labels = skip_by_field.get(key, set())
    best_cut: Optional[int] = None
    for lbl in labels:
        if lbl.lower() in skip_labels:
            continue
        for m in re.finditer(rf"\b{re.escape(lbl)}\b", quote, re.I):
            s = m.start()
            if s <= 20:
                continue
            prev = quote[s - 1]
            if prev.isspace() or prev in ".;:,-/()":
                next_chunk = quote[m.end():m.end() + 24]
                # Require label-like transition to avoid cutting natural prose.
                if re.match(r"^\s*(?::|-|n[°º\.]?\s*\d+|\d+|[A-ZÀ-Ù])", next_chunk):
                    if best_cut is None or s < best_cut:
                        best_cut = s
                    continue
            # Keep previous strict behavior as fallback.
            strict = re.search(rf"(?:\n|[\.;:]\s+|\s{{2,}})\b{re.escape(lbl)}\b", quote[s - 1:s + len(lbl) + 4], re.I)
            if strict and (best_cut is None or s < best_cut):
                best_cut = s
    if best_cut is not None:
        quote = quote[:best_cut]
    return quote.strip()


def _pick_field_focused_sentence(quote: str, field_key: Optional[str]) -> str:
    if not quote:
        return quote
    key = str(field_key or "").lower()
    if key in {"tribunale"}:
        fixed = re.sub(r"\bTRIBUNALEDI([A-ZÀ-Ù])", r"TRIBUNALE DI \1", quote, flags=re.I)
        fixed = re.sub(r"\bTRIBUNALE\s*DI([A-ZÀ-Ù])", r"TRIBUNALE DI \1", fixed, flags=re.I)
        m = re.search(
            r"\bTRIBUNALE\s+DI\s+((?!SEZION\w+\b)[A-ZÀ-Ù][A-ZÀ-Ù'\-]*(?:\s+(?!SEZION\w+\b)[A-ZÀ-Ù][A-ZÀ-Ù'\-]*){0,3})",
            fixed,
            re.I,
        )
        if m:
            phrase = _normalize_ws(f"TRIBUNALE DI {m.group(1)}")
            phrase = re.split(r"\b(?:SEZION\w*|ESECUZIONE|PERIZIA|R\.?\s*G\.?\s*E\.?)\b", phrase, maxsplit=1, flags=re.I)[0].strip()
            return phrase
    if key in {"procedura"}:
        m = re.search(r"\bEsecuzione\s+Immobiliare\s+\d+\s*/\s*\d+\s+del\s+R\.?\s*G\.?\s*E\.?\b", quote, re.I)
        if m:
            return _normalize_ws(m.group(0))
        m = re.search(r"\bR\.?\s*G\.?\s*E\.?\s*\d+\s*/\s*\d+\b", quote, re.I)
        if m:
            return _normalize_ws(m.group(0))
    if key in {"ape"}:
        m = re.search(r"([^\n\.;:!?]*\b(?:ape|certificato energetico|attestato di prestazione energetica)\b[^\n\.;:!?]*[\.!?]?)", quote, re.I)
        if m:
            sentence = _normalize_ws(m.group(1))
            if any(tok in sentence.lower() for tok in ("non esiste", "assente", "non presente", "presente")):
                return sentence
        sentences = re.split(r"(?<=[\.;:!?])\s+", quote)
        for s in sentences:
            low = s.lower()
            if ("ape" in low or "certificato energetico" in low or "attestato di prestazione energetica" in low) and (
                "assente" in low or "non esiste" in low or "non presente" in low or "presente" in low
            ):
                return s.strip()
    if key in {"spese_condominiali_arretrate"}:
        sentences = re.split(r"(?<=[\.;:!?])\s+", quote)
        for s in sentences:
            low = s.lower()
            if "spese condominiali" in low and any(t in low for t in ("arretrat", "morosit", "insolut", "non risultano", "nessun", "non presenti")):
                return s.strip()
        for s in sentences:
            if _is_short_spese_absence_phrase(s):
                return s.strip()
        return ""
    if key in {"lotto"}:
        if _is_toc_like_quote(quote):
            return ""
        m = re.search(r"\bLOTTO\s+UNICO\b|\bLOTTO\s+\d+\b", quote, re.I)
        if m:
            return _normalize_ws(m.group(0))
    if key in {"prezzo_base_asta"}:
        m = re.search(r"(PREZZO\s+BASE(?:\s*D['’]ASTA)?\s*:\s*€?\s*[0-9]{1,3}(?:[.\s][0-9]{3})*(?:,[0-9]{2})?)", quote, re.I)
        if not m:
            m = re.search(r"(Prezzo\s+base(?:\s*d['’]asta)?\s*:\s*€?\s*[0-9]{1,3}(?:[.\s][0-9]{3})*(?:,[0-9]{2})?)", quote, re.I)
        if m:
            cleaned = _normalize_ws(m.group(1))
            cleaned = re.sub(r"(?i)BASED['’]ASTA", "BASE D'ASTA", cleaned)
            cleaned = re.sub(r"(?i)^PREZZO", "Prezzo", cleaned)
            cleaned = re.sub(r"(?i)BASE D['’]ASTA", "base d'asta", cleaned)
            return cleaned
    if key in {"superficie_catastale", "superficie"}:
        m = re.search(r"(Superficie(?:\s+catastale|\s+commerciale)?\s*[0-9]{1,4}(?:[\.,][0-9]{1,2})?\s*(?:mq|m²|m2))", quote, re.I)
        if m:
            return _normalize_ws(m.group(1))
    if key in {"diritto_reale"}:
        m = re.search(r"(Diritto\s+reale\s*:\s*[^\n\.;:]{3,60})", quote, re.I)
        if m:
            return _normalize_ws(m.group(1))
        m = re.search(r"\b(?:Nuda\s+proprietà|Piena\s+proprietà|Proprietà|Usufrutto)\b[^\n\.;:]{0,40}", quote, re.I)
        if m:
            return _normalize_ws(m.group(0))
    if key in {"agibilita"}:
        sentences = re.split(r"(?<=[\.;:!?])\s+", quote)
        for s in sentences:
            low = s.lower()
            if any(t in low for t in ("non risulta agibil", "non è presente l'abitabilit", "non e presente l'abitabilit", "agibilità assente", "agibilita assente")):
                return s.strip()
    if key in {"conformita_urbanistica"}:
        m = re.search(r"([^\n\.;:!?]*(?:sanatoria|abusi?|difformit[aà]|incongruenz\w+|non\s+conforme)[^\n\.;:!?]*[\.!?]?)", quote, re.I)
        if m:
            return _normalize_ws(m.group(1))
        sentences = re.split(r"(?<=[\.;:!?])\s+", quote)
        for s in sentences:
            low = s.lower()
            if any(t in low for t in ("abusi", "difform", "incongruenz", "non conforme", "irregolarit", "conformità urbanistica", "conformita urbanistica")):
                return s.strip()
    if key in {"conformita_catastale"}:
        sentences = re.split(r"(?<=[\.;:!?])\s+", quote)
        for s in sentences:
            low = s.lower()
            if any(t in low for t in ("planimetria catastale", "difform", "incongruenz", "non conforme", "conformità catastale", "conformita catastale")):
                return s.strip()
    if key in {"stato_occupativo"}:
        m = re.search(r"((?:Occupat[oa]|Liber[oa]|Detenut[oa])[^\n\.;:!?]{0,140}[\.!?]?)", quote, re.I)
        if m:
            return _normalize_ws(m.group(1))
    return quote


def _tight_line_quote(text: str, start: int, end: int, field_key: Optional[str]) -> str:
    spans = _iter_line_spans(text)
    mid = (start + end) // 2
    idx = _line_index_for_offset(spans, mid)
    chosen = [idx]

    # If anchor spans more than one line, include touched lines.
    for i, (s, e) in enumerate(spans):
        if not (end <= s or start >= e):
            if i not in chosen:
                chosen.append(i)

    # Extend by one following line only if current line is too short and next line is value-like.
    quote_lines = []
    for i in sorted(chosen):
        s, e = spans[i]
        quote_lines.append(text[s:e])
    quote = _clean_quote_text("\n".join(quote_lines))
    if len(quote) < 24:
        next_idx = max(chosen) + 1
        if next_idx < len(spans):
            ns, ne = spans[next_idx]
            next_line = _clean_quote_text(text[ns:ne])
            if next_line and not _is_probable_label_line(next_line):
                quote = _clean_quote_text(f"{quote}\n{next_line}")
    quote = _apply_bleed_trim(quote, field_key)
    quote = _pick_field_focused_sentence(quote, field_key)
    return quote


def _truncate_on_word_boundary(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    cut = text.rfind(" ", 0, max_len + 1)
    if cut < max(20, int(max_len * 0.65)):
        cut = max_len
    return text[:cut].rstrip()


def _is_spese_section_context(text: str) -> bool:
    t = _normalize_ws(_collapse_ocr_spaced_caps(text or "").lower())
    if not t:
        return False
    return any(tok in t for tok in ("spese condominiali", "oneri condominiali", "arretrat", "morosit", "insolut"))


def _is_short_spese_absence_phrase(text: str) -> bool:
    t = _normalize_ws((text or "").lower()).strip(" .;:,-")
    if not t:
        return False
    return bool(re.fullmatch(r"(non\s+presenti|non\s+risultano|nessuna|nessuno|nessun(?:\s+arretrato)?)", t))


def _is_strong_spese_evidence(quote: str, anchor_hint: Optional[str] = None, context_text: Optional[str] = None) -> bool:
    q = _normalize_ws((quote or "").lower())
    if not q:
        return False
    has_signal = any(tok in q for tok in ("arretrat", "morosit", "insolut", "non risultano", "nessun", "non presenti"))
    if "spese condominiali" in q and has_signal:
        return True
    if _is_short_spese_absence_phrase(q) and (_is_spese_section_context(anchor_hint or "") or _is_spese_section_context(context_text or "")):
        return True
    return False


def _build_search_hint(quote: str, field_key: Optional[str], anchor_hint: Optional[str]) -> str:
    q = _normalize_ws(_collapse_ocr_spaced_caps(quote or ""))
    if not q:
        return ""
    key = str(field_key or "").lower()
    if key == "procedura":
        m = re.search(r"\bEsecuzione\s+Immobiliare\s+\d+\s*/\s*\d+\s+del\s+R\.?\s*G\.?\s*E\.?\b", q, re.I)
        if m:
            return _truncate_on_word_boundary(_normalize_ws(m.group(0)), 80)
    if key == "tribunale":
        m = re.search(
            r"\bTRIBUNALE\s+DI\s+((?!SEZION\w+\b)[A-ZÀ-Ù][A-ZÀ-Ù'\-]*(?:\s+(?!SEZION\w+\b)[A-ZÀ-Ù][A-ZÀ-Ù'\-]*){0,3})",
            q,
            re.I,
        )
        if m:
            return _truncate_on_word_boundary(_normalize_ws(f'TRIBUNALE DI {m.group(1)}'), 80)
    if key == "prezzo_base_asta":
        m = re.search(r"\bPrezzo\s+base(?:\s*d['’]asta)?\s*:\s*€?\s*[0-9]{1,3}(?:[.\s][0-9]{3})*(?:,[0-9]{2})?\b", q, re.I)
        if not m:
            m = re.search(r"\bPREZZO\s+BASED['’]ASTA\s*:\s*€?\s*[0-9]{1,3}(?:[.\s][0-9]{3})*(?:,[0-9]{2})?\b", q, re.I)
        if m:
            hint = _normalize_ws(m.group(0))
            hint = re.sub(r"(?i)BASED['’]ASTA", "base d'asta", hint)
            hint = re.sub(r"(?i)^PREZZO", "Prezzo", hint)
            return _truncate_on_word_boundary(hint, 80)
    if key in {"superficie", "superficie_catastale"}:
        m = re.search(r"\bSuperficie(?:\s+catastale|\s+commerciale)?\s*[0-9]{1,4}(?:[\.,][0-9]{1,2})?\s*(?:mq|m²|m2)\b", q, re.I)
        if m:
            return _truncate_on_word_boundary(_normalize_ws(m.group(0)), 80)
    if anchor_hint:
        anchor = _normalize_ws(_collapse_ocr_spaced_caps(str(anchor_hint)))
        if 6 <= len(anchor) <= 90 and anchor.lower() in q.lower():
            return _truncate_on_word_boundary(anchor, 80)
    keywords = _FIELD_KEYWORDS.get(str(field_key or "").lower(), [])
    low = q.lower()
    for kw in keywords:
        idx = low.find(kw.lower())
        if idx >= 0:
            start = max(0, idx - 18)
            end = min(len(q), idx + max(32, len(kw) + 26))
            hint = _truncate_on_word_boundary(q[start:end].strip(" ,;:-"), 80)
            if len(hint) >= 16:
                return hint
    fallback = _truncate_on_word_boundary(q, 80)
    if len(fallback) < 30 and len(q) > 30:
        fallback = _truncate_on_word_boundary(q[:96], 80)
    return fallback


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
    field_key: Optional[str] = None,
    anchor_hint: Optional[str] = None,
) -> Tuple[str, str]:
    text = str(page_text or "")
    n = len(text)
    if n == 0:
        return "", ""

    start = max(0, min(int(start_offset), n))
    end = max(start, min(int(end_offset), n))

    quote = _tight_line_quote(text, start, end, field_key=field_key)
    if not quote:
        left = _find_left_boundary(text, start)
        right = _find_right_boundary(text, end)
        left = _snap_word_left(text, left)
        right = _snap_word_right(text, right)
        right = max(right, min(n, end))
        quote = text[left:right].strip()

    quote = _clean_quote_text(quote)
    quote = _apply_bleed_trim(quote, field_key)
    quote = _pick_field_focused_sentence(quote, field_key)
    key = str(field_key or "").lower()
    if key in {"lotto", "prezzo_base_asta", "superficie", "superficie_catastale", "tribunale"} and _is_toc_like_quote(quote):
        quote = ""
    if str(field_key or "").lower() == "spese_condominiali_arretrate":
        context_window = text[max(0, start - 220):min(n, end + 220)]
        if not _is_strong_spese_evidence(quote, anchor_hint=anchor_hint, context_text=context_window):
            return "", ""
    quote = _truncate_on_word_boundary(quote, max_len).strip()

    if not quote:
        fallback = text[start:end].strip() or text[max(0, start - 40):min(n, end + 80)].strip()
        quote = _truncate_on_word_boundary(_clean_quote_text(fallback), max_len)

    search_hint = _build_search_hint(quote, field_key=field_key, anchor_hint=anchor_hint)
    return quote, search_hint
