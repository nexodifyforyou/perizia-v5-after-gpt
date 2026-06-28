"""
PDF quality blocker for Correctness Mode v2.

Given page-by-page extracted text, decide whether the document is good enough to
proceed with a correctness analysis. This is the FIRST fail-closed gate:

    PDF_QUALITY_OK       -> proceed (later steps)
    PDF_QUALITY_WARNING  -> proceed but flagged
    PDF_QUALITY_BLOCKED  -> STOP. No OpenAI, no Gemini, no old fallback.

Detection rules are intentionally GENERAL (keyword/semantic + density heuristics),
not overfit to a single PDF. We store both the physical page index and any visible
"Pagina X di Y" labels found inside the text, since perizie often carry their own
internal pagination.

The output dict matches the documented contract shape. A ``details`` block carries
extra debugging info (per-page density, detected labels, money signal) without
changing the contract surface.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

from .schemas import (
    KEY_SECTIONS,
    PdfBlockReason,
    PdfQualityStatus,
    PdfWarnReason,
)

# ---------------------------------------------------------------------------
# Tunable thresholds (general, not overfit)
# ---------------------------------------------------------------------------
MIN_READABLE_CHARS = 40          # a page needs at least this many alpha chars
MIN_READABLE_WORDS = 8           # ...and at least this many word-like tokens
DENSITY_TARGET_CHARS = 600       # chars per page that maps to density ~1.0
BLOCK_UNREADABLE_RATIO = 0.5     # > this fraction unreadable -> BLOCKED
WARN_UNREADABLE_RATIO = 0.15     # > this fraction unreadable -> WARNING
SCANNED_AVG_CHARS = 25           # avg chars/page below this == effectively scanned
LOW_TEXT_PAGE_CHARS = 120        # a "low text" (but present) page

# Sections that, if unreadable, are most damaging to a correctness analysis.
CRITICAL_SECTIONS = ["valutazione", "costi_money", "conformita"]

# Number of MISSING key sections at/above which we BLOCK.
BLOCK_MISSING_SECTIONS = 4


# ---------------------------------------------------------------------------
# Semantic keyword banks per key section (accent-insensitive matching)
# ---------------------------------------------------------------------------
SECTION_KEYWORDS: Dict[str, List[str]] = {
    "lotto_beni": [
        "lotto", "beni", "identificazione dei beni", "identificazione beni",
        "oggetto della vendita", "oggetto vendita", "descrizione del bene",
        "descrizione dei beni",
    ],
    "possesso": [
        "stato di possesso", "possesso", "occupazione", "occupato", "occupante",
        "locazione", "locato", "conduttore", "libero", "detenzione",
    ],
    "vincoli_oneri": [
        "vincoli e oneri", "vincoli", "oneri", "formalita", "formalità",
        "ipoteca", "pignoramento", "gravami", "trascrizioni", "iscrizioni",
    ],
    "conformita": [
        "conformita", "conformità", "giudizio di conformita", "giudizi di conformita",
        "edilizia", "catastale", "urbanistica", "urbanistico", "impianti",
        "agibilita", "agibilità", "abitabilita",
    ],
    "valutazione": [
        "valutazione", "stima", "prezzo", "valore", "vendita giudiziaria",
        "prezzo base", "valore di mercato", "piu probabile valore",
    ],
    "costi_money": [
        "costi", "spese", "oneri", "regolarizzazione", "deprezzamento",
        "spese condominiali", "spese di regolarizzazione", "abbattimento",
    ],
}

# Visible internal pagination label, e.g. "Pagina 3 di 19".
_PAGE_LABEL_RE = re.compile(
    r"pag(?:ina)?\.?\s*(\d{1,4})\s*(?:/|di|of)\s*(\d{1,4})",
    re.IGNORECASE,
)

# Money signal: euro symbol / EUR / euro word / thousands-formatted numbers.
_MONEY_RE = re.compile(
    r"(€|\beur\b|\beuro\b|\bprezzo\s+base\b|\d{1,3}(?:\.\d{3})+(?:,\d{2})?)",
    re.IGNORECASE,
)
_WORD_RE = re.compile(r"[A-Za-zÀ-ÿ]{2,}")


def _strip_accents(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c)
    )


def _norm(text: str) -> str:
    return _strip_accents(str(text or "")).lower()


def _alpha_count(text: str) -> int:
    return sum(1 for c in text if c.isalpha())


def _page_readable(text: str) -> Tuple[bool, int, float]:
    """Return (is_readable, char_count, density_score) for one page."""
    raw = str(text or "")
    char_count = len(raw.strip())
    alpha = _alpha_count(raw)
    words = len(_WORD_RE.findall(raw))
    readable = (alpha >= MIN_READABLE_CHARS) and (words >= MIN_READABLE_WORDS)
    density = min(1.0, char_count / float(DENSITY_TARGET_CHARS)) if char_count else 0.0
    return readable, char_count, round(density, 3)


def _normalize_pages(pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Normalize input pages into a stable internal shape."""
    out: List[Dict[str, Any]] = []
    for idx, entry in enumerate(pages or [], start=1):
        if not isinstance(entry, dict):
            entry = {"page_number": idx, "text": str(entry or "")}
        try:
            physical = int(entry.get("page_number", idx))
        except Exception:
            physical = idx
        text = str(entry.get("text", "") or "")
        readable, chars, density = _page_readable(text)

        visible_label: Optional[str] = None
        label_x: Optional[int] = None
        label_y: Optional[int] = None
        m = _PAGE_LABEL_RE.search(text)
        if m:
            visible_label = m.group(0)
            try:
                label_x = int(m.group(1))
                label_y = int(m.group(2))
            except Exception:
                label_x = label_y = None

        out.append(
            {
                "physical_index": physical,
                "order_index": idx,
                "char_count": chars,
                "density": density,
                "readable": readable,
                "visible_label": visible_label,
                "label_x": label_x,
                "label_y": label_y,
                "has_money_signal": bool(_MONEY_RE.search(text)),
                "_norm_text": _norm(text),
            }
        )
    return out


def _detect_key_sections(norm_corpus: str) -> Dict[str, bool]:
    detected: Dict[str, bool] = {}
    for section in KEY_SECTIONS:
        keywords = SECTION_KEYWORDS.get(section, [])
        detected[section] = any(_norm(kw) in norm_corpus for kw in keywords)
    return detected


def _check_page_order(page_details: List[Dict[str, Any]]) -> Tuple[bool, bool]:
    """
    Inspect visible 'Pagina X di Y' labels.

    Returns (order_ok, labels_uncertain).
      * order_ok=False only when labels clearly contradict physical order.
      * labels_uncertain=True only when labels EXIST but are ambiguous
        (duplicates/plateaus). Documents with no internal pagination are NOT
        treated as uncertain — that is normal, not a quality problem.
    """
    labeled = [(p["order_index"], p["label_x"]) for p in page_details if p["label_x"] is not None]
    if len(labeled) < 2:
        # No (or a single) internal label: nothing to contradict -> clean, certain.
        return True, False

    labels_in_order = [x for _, x in labeled]
    strictly_increasing = all(
        b > a for a, b in zip(labels_in_order, labels_in_order[1:])
    )
    # Allow plateaus/duplicates as "uncertain" rather than "broken".
    non_decreasing = all(
        b >= a for a, b in zip(labels_in_order, labels_in_order[1:])
    )
    if strictly_increasing:
        return True, False
    if non_decreasing:
        return True, True
    return False, False


def assess_pdf_quality(
    pages: List[Dict[str, Any]],
    *,
    ocr_failed: bool = False,
    analysis_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Run the PDF quality blocker over page-by-page extracted text.

    Args:
        pages: list of {"page_number": int, "text": str, ...}
        ocr_failed: signal from the extraction stage that OCR could not run.
        analysis_id: optional, echoed into the report for traceability.

    Returns a dict matching the documented PDF-quality contract.
    """
    page_details = _normalize_pages(pages)
    total_pages = len(page_details)

    warnings: List[str] = []
    block_reasons: List[str] = []

    # Aggregate text corpus (normalized) for section detection.
    norm_corpus = "\n".join(p["_norm_text"] for p in page_details)
    total_chars = sum(p["char_count"] for p in page_details)
    readable_pages_list = [p for p in page_details if p["readable"]]
    readable_pages = len(readable_pages_list)
    unreadable_pages = [p["physical_index"] for p in page_details if not p["readable"]]
    unreadable_ratio = (len(unreadable_pages) / total_pages) if total_pages else 1.0
    avg_chars = (total_chars / total_pages) if total_pages else 0
    densities = [p["density"] for p in page_details]
    text_density_score = round(sum(densities) / len(densities), 3) if densities else 0.0

    key_sections_detected = _detect_key_sections(norm_corpus)
    missing_sections = [s for s, ok in key_sections_detected.items() if not ok]
    missing_critical = [s for s in CRITICAL_SECTIONS if s in missing_sections]

    page_order_ok, labels_uncertain = _check_page_order(page_details)

    # Money signal: do we see money figures anywhere, and specifically in the
    # value/cost context (i.e. valuation/costi sections were detected)?
    money_pages = [p["physical_index"] for p in page_details if p["has_money_signal"]]
    has_money_signal = bool(money_pages)
    valuation_or_costi_present = (
        key_sections_detected.get("valutazione") or key_sections_detected.get("costi_money")
    )

    # ---------------- BLOCK rules (fail closed) -----------------------------
    if total_pages == 0 or total_chars == 0:
        block_reasons.append(PdfBlockReason.DOCUMENT_TEXT_EMPTY)

    if ocr_failed and total_chars == 0:
        block_reasons.append(PdfBlockReason.OCR_EXTRACTION_FAILED)

    if total_pages > 0 and total_chars > 0:
        # Effectively scanned: pages exist but almost no usable text.
        if avg_chars < SCANNED_AVG_CHARS and unreadable_ratio > BLOCK_UNREADABLE_RATIO:
            block_reasons.append(PdfBlockReason.SCANNED_PDF_WITHOUT_USABLE_TEXT)
        elif unreadable_ratio > BLOCK_UNREADABLE_RATIO:
            block_reasons.append(PdfBlockReason.TOO_MANY_UNREADABLE_PAGES)

    if len(missing_sections) >= BLOCK_MISSING_SECTIONS:
        block_reasons.append(PdfBlockReason.KEY_SECTIONS_UNREADABLE)

    if not page_order_ok:
        block_reasons.append(PdfBlockReason.PAGE_ORDER_BROKEN)

    if valuation_or_costi_present and not has_money_signal and total_chars > 0:
        # The doc talks about value/costs but no money figures survived extraction.
        block_reasons.append(PdfBlockReason.MONEY_TABLES_UNREADABLE)

    # ---------------- WARNING rules (only if not blocked) -------------------
    if WARN_UNREADABLE_RATIO < unreadable_ratio <= BLOCK_UNREADABLE_RATIO:
        warnings.append(PdfWarnReason.SOME_LOW_TEXT_PAGES)
    else:
        # Even if ratio is low, flag if some present pages are thin on text.
        low_text_pages = [
            p["physical_index"]
            for p in readable_pages_list
            if p["char_count"] < LOW_TEXT_PAGE_CHARS
        ]
        if low_text_pages:
            warnings.append(PdfWarnReason.SOME_LOW_TEXT_PAGES)

    if 0 < len(missing_sections) < BLOCK_MISSING_SECTIONS:
        warnings.append(PdfWarnReason.SOME_KEY_SECTIONS_WEAK)

    if valuation_or_costi_present and has_money_signal and len(money_pages) <= 1:
        warnings.append(PdfWarnReason.MONEY_TABLES_WEAK_BUT_PRESENT)

    if labels_uncertain:
        warnings.append(PdfWarnReason.PAGE_LABELS_UNCERTAIN)

    warnings = sorted(set(warnings))

    # ---------------- Decide final status -----------------------------------
    if block_reasons:
        quality_status = PdfQualityStatus.BLOCKED
        # Pick the highest-priority block reason as the primary reason_code.
        primary = _primary_block_reason(block_reasons)
        reason_code = primary
        reason_human, troubleshoot_message, next_steps = _block_explanation(
            primary, missing_sections=missing_sections, unreadable_pages=unreadable_pages
        )
    elif warnings:
        quality_status = PdfQualityStatus.WARNING
        reason_code = None
        reason_human = None
        troubleshoot_message = None
        next_steps = []
    else:
        quality_status = PdfQualityStatus.OK
        reason_code = None
        reason_human = None
        troubleshoot_message = None
        next_steps = []

    report: Dict[str, Any] = {
        "quality_status": quality_status,
        "total_pages": total_pages,
        "readable_pages": readable_pages,
        "unreadable_pages": unreadable_pages,
        "text_density_score": text_density_score,
        "page_order_ok": page_order_ok,
        "key_sections_detected": key_sections_detected,
        "warnings": warnings,
        "reason_code": reason_code,
        "reason_human": reason_human,
        "troubleshoot_message": troubleshoot_message,
        "next_steps": next_steps,
        "details": {
            "analysis_id": analysis_id,
            "avg_chars_per_page": round(avg_chars, 1),
            "total_chars": total_chars,
            "unreadable_ratio": round(unreadable_ratio, 3),
            "missing_sections": missing_sections,
            "missing_critical_sections": missing_critical,
            "labels_uncertain": labels_uncertain,
            "money_pages": money_pages,
            "block_reasons_all": _ordered_unique(block_reasons),
            "pages": [
                {
                    "physical_index": p["physical_index"],
                    "order_index": p["order_index"],
                    "visible_label": p["visible_label"],
                    "char_count": p["char_count"],
                    "density": p["density"],
                    "readable": p["readable"],
                    "has_money_signal": p["has_money_signal"],
                }
                for p in page_details
            ],
        },
    }
    return report


# ---------------------------------------------------------------------------
# Block-reason explanation + priority
# ---------------------------------------------------------------------------
_BLOCK_PRIORITY = [
    PdfBlockReason.DOCUMENT_TEXT_EMPTY,
    PdfBlockReason.OCR_EXTRACTION_FAILED,
    PdfBlockReason.SCANNED_PDF_WITHOUT_USABLE_TEXT,
    PdfBlockReason.TOO_MANY_UNREADABLE_PAGES,
    PdfBlockReason.KEY_SECTIONS_UNREADABLE,
    PdfBlockReason.MONEY_TABLES_UNREADABLE,
    PdfBlockReason.PAGE_ORDER_BROKEN,
]


def _ordered_unique(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for it in items:
        if it not in seen:
            seen.add(it)
            out.append(it)
    return out


def _primary_block_reason(block_reasons: List[str]) -> str:
    for reason in _BLOCK_PRIORITY:
        if reason in block_reasons:
            return reason
    return block_reasons[0]


def _block_explanation(
    reason: str,
    *,
    missing_sections: List[str],
    unreadable_pages: List[int],
) -> Tuple[str, str, List[str]]:
    """Return (reason_human, troubleshoot_message, next_steps) for a block reason."""
    table: Dict[str, Tuple[str, str, List[str]]] = {
        PdfBlockReason.DOCUMENT_TEXT_EMPTY: (
            "Il documento non contiene testo estraibile.",
            "L'estrazione non ha prodotto alcun testo. Il PDF potrebbe essere "
            "vuoto, corrotto o composto solo da immagini senza OCR.",
            [
                "Verificare che il PDF caricato sia il file corretto e non vuoto.",
                "Rieseguire l'estrazione con OCR abilitato.",
            ],
        ),
        PdfBlockReason.OCR_EXTRACTION_FAILED: (
            "L'estrazione OCR è fallita e non è disponibile testo utilizzabile.",
            "Il fallback OCR non ha prodotto testo. Controllare la pipeline di "
            "estrazione (Document AI / OCR) e i log della fase di ingest.",
            [
                "Controllare i log della fase OCR/Document AI.",
                "Riprovare l'estrazione una volta ripristinato il servizio OCR.",
            ],
        ),
        PdfBlockReason.SCANNED_PDF_WITHOUT_USABLE_TEXT: (
            "Il PDF sembra scansionato senza testo utilizzabile.",
            "Le pagine esistono ma contengono pochissimo testo: probabile "
            "scansione di immagini senza un livello di testo OCR valido.",
            [
                "Fornire una versione del PDF con testo selezionabile.",
                "Eseguire un OCR di alta qualità prima della Correctness Mode.",
            ],
        ),
        PdfBlockReason.TOO_MANY_UNREADABLE_PAGES: (
            "Troppe pagine risultano illeggibili.",
            f"Pagine illeggibili: {unreadable_pages}. La quota di pagine senza "
            "testo affidabile supera la soglia consentita.",
            [
                "Verificare la qualità del PDF di origine.",
                "Rieseguire l'estrazione/OCR sulle pagine illeggibili.",
            ],
        ),
        PdfBlockReason.KEY_SECTIONS_UNREADABLE: (
            "Sezioni chiave della perizia non sono leggibili.",
            f"Sezioni mancanti/illeggibili: {missing_sections}. Senza queste "
            "sezioni una analisi di correttezza non è affidabile.",
            [
                "Controllare che la perizia includa lotto/possesso/vincoli/"
                "conformità/valutazione/costi.",
                "Verificare l'estrazione delle pagine relative a queste sezioni.",
            ],
        ),
        PdfBlockReason.MONEY_TABLES_UNREADABLE: (
            "Le tabelle economiche/di valore non sono leggibili.",
            "La perizia menziona valutazione o costi ma non sono stati rilevati "
            "importi monetari: le tabelle dei valori potrebbero non essere state "
            "estratte correttamente.",
            [
                "Verificare l'estrazione delle tabelle di stima/costi.",
                "Controllare le pagine con prezzo base, valore e spese.",
            ],
        ),
        PdfBlockReason.PAGE_ORDER_BROKEN: (
            "L'ordine delle pagine risulta incoerente.",
            "Le etichette interne 'Pagina X di Y' non sono in sequenza crescente: "
            "le pagine potrebbero essere disordinate o mancanti.",
            [
                "Verificare che le pagine siano nell'ordine corretto.",
                "Ricaricare un PDF con la sequenza delle pagine integra.",
            ],
        ),
    }
    if reason in table:
        return table[reason]
    return (
        f"Qualità PDF non sufficiente: {reason}.",
        f"Il blocco qualità PDF ha rilevato: {reason}.",
        ["Verificare il PDF di origine e rieseguire l'estrazione."],
    )
