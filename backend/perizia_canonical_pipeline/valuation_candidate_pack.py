"""
Valuation / Prezzo Base bounded field shell — candidate harvesting only.

Produces: valuation_candidate_pack.json

Field types built:
  prezzo_base_raw        — raw prezzo base d'asta / prezzo base di vendita
  valore_stima_raw       — raw valore di stima del bene / valore di stima
  valuation_market_raw   — raw valore di mercato / valore commerciale / valore venale

What is NOT built here:
  - full valuation model or waterfall
  - deprezzamento decomposition
  - arithmetic reconciliation or inferred net/gross
  - final user-facing "best value"
  - doc_map, money-box, oneri

Scope rules mirror the cadastral / occupancy pipeline geometry.
Local HEADER_GRADE lot-label mismatches are detected via plurality_headers.json.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from .runner import build_context
from .corpus_registry import load_cases, list_case_keys
from .bene_scope_map import build_bene_scope_map
from .cadastral_candidate_pack import _build_lookup_tables, _determine_scope as _cadat_determine_scope


# ---------------------------------------------------------------------------
# Monetary amount helpers
# ---------------------------------------------------------------------------

# Matches €, €., Euro followed by a numeric amount.
# OCR may insert spaces inside thousands separators (e.g. "800 .000,00",
# "800. 000,00", "800 000,00").  The separator group [\s.]+ handles any
# combination of whitespace and dots between digit groups.
_AMOUNT_PAT = re.compile(
    r"(?:€\.?\s*|Euro\s+)"
    r"([\d]{1,3}(?:[\s.]+\d{2,3})*(?:,\d{1,2})?)",
    re.IGNORECASE,
)

# A standalone amount-only line (for split-pattern lookahead).
_AMOUNT_ONLY_LINE_PAT = re.compile(
    r"^\s*(?:€\.?\s*|Euro\s+)"
    r"([\d]{1,3}(?:[\s.]+\d{2,3})*(?:,\d{1,2})?)\s*$",
    re.IGNORECASE,
)


def _extract_amount_inline(line: str) -> Optional[str]:
    """Return the raw matched amount token (including € prefix) or None."""
    m = _AMOUNT_PAT.search(line)
    if not m:
        return None
    return m.group(0).strip()


def _normalize_amount_str(raw: str) -> str:
    """
    Collapse OCR spaces in numeric part for comparison purposes only.
    "€   800 .000,00" → "€ 800.000,00"
    The original raw string is preserved in extracted_value.
    """
    # Collapse runs of spaces around decimal separators
    return re.sub(r"\s+", " ", re.sub(r"(\d)\s+\.", r"\1.", re.sub(r"\.\s+(\d)", r".\1", raw))).strip()


# ---------------------------------------------------------------------------
# Exclusion patterns
# ---------------------------------------------------------------------------

# Table-of-contents dot-leader lines — skip entirely
_DOT_LEADER_PAT = re.compile(r"\.{5,}|(?:\s*\.\s*){6,}")

# SCHEMA RIASSUNTIVO page marker
_SCHEMA_RIASSUNTIVO_PAT = re.compile(r"\bSCHEMA\s+RIASSUNTIVO\b", re.IGNORECASE)

# Per-unit rate (€/mq etc.) — not a total value, not a field assignment
_PER_UNIT_RATE_PAT = re.compile(
    r"€\s*/\s*mq|€\s*[\d.,]+\s*/\s*mq|\d+[\s,.]?\d*\s*€\s*/\s*mq|€\s*/\s*m²",
    re.IGNORECASE,
)

# "Valore finale di stima" — post-deprezzamento result, excluded per scope rules
_VALORE_FINALE_PAT = re.compile(r"\bvalore\s+finale\s+di\s+stima\b", re.IGNORECASE)

# Calculation section header — not a result
_CALCOLO_HEADER_PAT = re.compile(
    r"^\s*CALCOLO\s+DEL\s+VALORE\s+DI\s+MERCATO\s*:?\s*$",
    re.IGNORECASE,
)

# RIEPILOGO VALORI / RIEPILOGO VALUTAZIONE section headers — not field assignments
_RIEPILOGO_HEADER_PAT = re.compile(
    r"^\s*RIEPILOGO\s+(?:VALORI|VALUTAZIONE)",
    re.IGNORECASE,
)

# Methodology / prose signals — these indicate that a keyword appears in
# explanatory prose, not as a field assignment.
_METHOD_PROSE_PAT = re.compile(
    r"(?:"
    r"la\s+dottrina\s+estimale"
    r"|determinare\s+il\s+(?:valore|più\s+probabile)"
    r"|più\s+probabile\s+valore\s+di"
    r"|in\s+condizioni\s+di\s+libero\s+mercato"
    r"|banca\s+dati\s+delle\s+quotazioni"
    r"|osservatorio\s+(?:del\s+mercato|immobiliare)\b(?!\s*:)"
    r"|in\s+sede\s+di\s+(?:formulazione|valutazione)"
    r"|attribuire\s+(?:un|al)\s+(?:valore|bene)\s+(?:medio|unitario|di\s+mercato)\s+di\s+(?:\d|un)"
    r"|si\s+è\s+valutato\s+(?:un|il)\s+valore"
    r"|ha\s+condotto\s+alla\s+determinazione"
    r")",
    re.IGNORECASE,
)

# Rendita catastale / cadastral income references — not market value
_RENDITA_PAT = re.compile(
    r"\b(?:rendita\s+catastale|reddito\s+catastale|rendita\s+urbana)\b",
    re.IGNORECASE,
)

# Riduzione / deprezzamento in a sentence prefix — indicates a reduction sentence
# (e.g. "Riduzione del 10% per lo stato di occupazione")
_RIDUZIONE_PREFIX_PAT = re.compile(
    r"^\s*(?:Riduzione|Deprezzamento)\s+del\b",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Field trigger patterns
# ---------------------------------------------------------------------------

# Prezzo base d'asta with optional explicit lot prefix.
# Group 1 captures the explicit lot ID when present (e.g. "LOTTO 1").
# Apostrophe alternatives: ASCII ' (U+0027), curly left ' (U+2018), curly right ' (U+2019), period.
_PREZZO_BASE_PAT = re.compile(
    r"(?:LOTTO\s+([A-Z0-9]+)\s*[-–]\s*)?"
    r"(?:PREZZO\s+BASE\s+D[\x27\u2018\u2019.]ASTA|prezzo\s+(?:a\s+)?base\s+d[\x27\u2018\u2019.]asta|PREZZO\s+BASE\s+DI\s+VENDITA|prezzo\s+base\s+di\s+vendita)",
    re.IGNORECASE,
)

# "Valore complessivo del lotto" — total lot valuation, maps to valore_stima_raw
_VALORE_COMPLESSIVO_LOTTO_PAT = re.compile(
    r"\bvalore\s+complessivo\s+del\s+lotto\b",
    re.IGNORECASE,
)

# "Valore di stima del bene" — more specific, must be checked before _VALORE_STIMA_PAT
_VALORE_STIMA_BENE_PAT = re.compile(
    r"\bvalore\s+di\s+stima\s+del\s+bene\b",
    re.IGNORECASE,
)

# "Valore di stima" (generic) — but NOT "valore finale di stima" (excluded separately)
# Negative lookahead prevents matching "finale"
_VALORE_STIMA_PAT = re.compile(
    r"\bvalore\s+di\s+stima\b(?!\s+del\s+bene)(?!\s*\s+finale\b)",
    re.IGNORECASE,
)

# Valuation market patterns — valore di mercato, commerciale, venale, and the
# long "Valore di Mercato dell'immobile nello stato di fatto e di diritto"
_VALUATION_MARKET_PAT = re.compile(
    r"\b(?:"
    r"valore\s+di\s+mercato(?:\s*\([^)]{0,100}\))?"
    r"|valore\s+commerciale"
    r"|valore\s+venale"
    r")\b",
    re.IGNORECASE,
)

# A line that is ONLY a label with no field-assignment colon ending in amount.
# "VALORE DI MERCATO (OMV):" without an amount on the same line → header only.
_OMV_HEADER_PAT = re.compile(
    r"VALORE\s+DI\s+MERCATO\s*\(OMV\)\s*:",
    re.IGNORECASE,
)

# Explicit HEADER_GRADE-style lot label: "LOTTO X" or "LOTTO N° X"
_EXPLICIT_LOT_LABEL_PAT = re.compile(
    r"\bLOTTO\s+([A-Z0-9]+)\b",
    re.IGNORECASE,
)

# Split-pattern trigger: line ends with ":" after a valuation keyword but has no amount
# (i.e. the amount is on the next line).  We require the colon to be followed by
# optional whitespace and end-of-line.
_LABEL_ENDS_COLON_PAT = re.compile(r":\s*$")

# Italian auction perizia pattern: "Prezzo base d'asta" followed by sub-lines
# "Valore in caso di regolarizzazione ... a carico dell'acquirente: €.X"
# where the "procedura" variant is "non previsto".
# This covers the case where prezzo_base is presented in a two-scenario table.
_PREZZO_BASE_ACQUIRENTE_PAT = re.compile(
    r"a\s+carico\s+dell[''\u2019]acquirente\s*:",
    re.IGNORECASE,
)
_PREZZO_BASE_PROCEDURA_NOT_PREVISTO_PAT = re.compile(
    r"a\s+carico\s+della\s+procedura.*?:\s*\n?\s*non\s+previsto",
    re.IGNORECASE | re.DOTALL,
)


# ---------------------------------------------------------------------------
# Scope helpers (reused from cadastral_candidate_pack)
# ---------------------------------------------------------------------------

def _build_lot_header_grade_lookup(plurality_headers: Dict) -> Dict[int, List[Dict]]:
    """
    Build {page: [header_grade_signals]} for HEADER_GRADE lot signals.
    Only includes signals where value is not None (actual lot ID).
    """
    result: Dict[int, List[Dict]] = {}
    for sig in plurality_headers.get("lot_signals", []):
        if sig.get("class") != "HEADER_GRADE":
            continue
        if sig.get("value") is None:
            continue
        page = int(sig["page"])
        result.setdefault(page, []).append(sig)
    return result


def _check_local_lot_mismatch(
    page: int,
    trigger_line_index: int,
    page_scope_lot_id: str,
    hg_lookup: Dict[int, List[Dict]],
    winner: str,
) -> Optional[str]:
    """
    For multi-lot documents: check whether a HEADER_GRADE lot signal that
    appears BEFORE the trigger line on the same page references a different
    lot than the page-scope lot.

    Returns the mismatched lot_id string if a mismatch is found, else None.
    Only applies to multi-lot winners (H2/H4).
    """
    if winner not in ("H2_EXPLICIT_MULTI_LOT", "H4_CANDIDATE_MULTI_LOT_MULTI_BENE"):
        return None

    scope_lot_norm = str(page_scope_lot_id).strip().lower()
    for sig in hg_lookup.get(page, []):
        sig_lot = str(sig["value"]).strip().lower()
        sig_line = int(sig.get("line_index", 0))
        if sig_line < trigger_line_index and sig_lot != scope_lot_norm:
            return sig_lot
    return None


def _find_local_lot_in_context(
    lines: List[str],
    trigger_idx: int,
    known_lot_ids: Set[str],
    window: int = 5,
) -> Optional[str]:
    """
    Scan up to `window` lines immediately before trigger_idx for an explicit
    'LOTTO X' mention whose normalised lot ID is in known_lot_ids.

    Returns the normalised (lower-case) lot_id of the LAST such mention found
    in the window (i.e. the closest line to the trigger), or None.

    Callers must restrict this to multi-lot documents only.
    """
    found: Optional[str] = None
    start = max(0, trigger_idx - window)
    for line in lines[start:trigger_idx]:
        m = _EXPLICIT_LOT_LABEL_PAT.search(line)
        if m:
            candidate = m.group(1).strip().lower()
            if candidate in known_lot_ids:
                found = candidate
    return found


def _build_last_bene_lookup(bsm: Dict) -> Dict[str, Dict]:
    """
    For each lot that has bene structure, return metadata about its last bene.

    Returns:
      {lot_id: {"last_bene_id": str,
                "start_page": int, "end_page": int,
                "first_header_page": int, "first_header_line_index": int}}

    The last bene's page scope is extended to end-of-lot by the bene_scope_map
    (navigation fallback).  The cadastral shell therefore returns LOT_LEVEL_ONLY
    for pages in that range.  The valuation shell needs to know when a trigger
    falls inside that range so it can decide whether the stima value is
    genuinely lot-level or bene-local.
    """
    by_lot: Dict[str, List[Dict]] = {}
    for bs in (bsm.get("bene_scopes") or []):
        lid = str(bs["lot_id"])
        by_lot.setdefault(lid, []).append(bs)
    result: Dict[str, Dict] = {}
    for lid, benes in by_lot.items():
        if not benes:
            continue
        lb = benes[-1]
        result[lid] = {
            "last_bene_id": str(lb["bene_id"]),
            "start_page": int(lb["start_page"]),
            "end_page": int(lb["end_page"]),
            "first_header_page": int(lb.get("first_header_page") or 0),
            "first_header_line_index": int(lb.get("first_header_line_index") or -1),
        }
    return result


# ---------------------------------------------------------------------------
# Schema page detection
# ---------------------------------------------------------------------------

def _find_schema_pages(raw_pages: List[Dict]) -> Set[int]:
    """
    Return the set of pages that are SCHEMA RIASSUNTIVO (summary) pages,
    plus the following 2 pages (continuation).
    """
    schema_pages: Set[int] = set()
    for page_data in raw_pages:
        pn = int(page_data["page_number"])
        text = page_data.get("text", "") or ""
        if _SCHEMA_RIASSUNTIVO_PAT.search(text):
            schema_pages.add(pn)
            schema_pages.add(pn + 1)
            schema_pages.add(pn + 2)
    return schema_pages


# ---------------------------------------------------------------------------
# Amount extraction helpers
# ---------------------------------------------------------------------------

def _try_prezzo_base_acquirente_lookahead(
    lines: List[str],
    trigger_idx: int,
    max_lookahead: int = 18,
) -> Tuple[Optional[str], int]:
    """
    Extended lookahead for 'Prezzo base d'asta' that follows the Italian auction
    two-scenario structure:
      Prezzo base d'asta
        Valore in caso di regolarizzazione ... a carico della procedura.: non previsto
        Valore in caso di regolarizzazione ... a carico dell'acquirente:
          €. 97.321,61

    Returns (raw_amount_token, line_offset) or (None, -1).
    Requires at least one "acquirente" sub-line to be present.
    If the "procedura" variant is present and set to "non previsto", treat
    the "acquirente" amount as the authoritative prezzo base.
    """
    end = min(len(lines), trigger_idx + max_lookahead + 1)
    window_text = "\n".join(lines[trigger_idx + 1:end])

    # Must have the acquirente pattern at all
    if not _PREZZO_BASE_ACQUIRENTE_PAT.search(window_text):
        return None, -1

    # Walk forward to find "a carico dell'acquirente:" then extract the amount
    acquirente_found = False
    for offset in range(1, min(max_lookahead + 1, len(lines) - trigger_idx)):
        line = lines[trigger_idx + offset]
        stripped = line.strip()
        if _PREZZO_BASE_ACQUIRENTE_PAT.search(line):
            acquirente_found = True
            # Amount may be inline on the same line
            inline = _extract_amount_inline(line)
            if inline:
                return inline, offset
            # Or on the next non-empty lines
            for next_off in range(offset + 1, min(offset + 6, len(lines) - trigger_idx)):
                next_line = lines[trigger_idx + next_off]
                if not next_line.strip():
                    continue
                if _AMOUNT_ONLY_LINE_PAT.match(next_line):
                    return _extract_amount_inline(next_line), next_off
                # Two-line split: "€." then number on next line
                if re.match(r"^\s*€\.?\s*$", next_line, re.IGNORECASE):
                    for nn_off in range(next_off + 1, min(next_off + 4, len(lines) - trigger_idx)):
                        nn_line = lines[trigger_idx + nn_off]
                        if not nn_line.strip():
                            continue
                        combined = f"€. {nn_line.strip()}"
                        if _AMOUNT_ONLY_LINE_PAT.match(combined):
                            return _extract_amount_inline(combined), nn_off
                        break
                    break
                # Non-empty, non-amount line → stop inner lookahead
                inline2 = _extract_amount_inline(next_line)
                if inline2:
                    return inline2, next_off
                break
    return None, -1


def _try_extract_amount(
    trigger_line: str,
    lines: List[str],
    trigger_idx: int,
    field_type: Optional[str] = None,
) -> Tuple[Optional[str], int]:
    """
    Try to extract an amount from the trigger line or the next 3 non-empty lines.

    Returns (raw_amount_token, line_offset) where:
      line_offset == 0   → inline (amount on trigger line)
      line_offset >= 1   → split (amount on next non-empty line(s))
      (None, -1)         → no amount found
    """
    # Inline: amount on the same line
    inline = _extract_amount_inline(trigger_line)
    if inline:
        return inline, 0

    # The trigger line must end with ":" for split pattern to apply
    if not _LABEL_ENDS_COLON_PAT.search(trigger_line):
        # Special case: prezzo_base_raw may follow the two-scenario Italian structure
        # where the amount is many lines below under "a carico dell'acquirente"
        if field_type == "prezzo_base_raw":
            return _try_prezzo_base_acquirente_lookahead(lines, trigger_idx)
        return None, -1

    # Split: look ahead up to 4 non-empty lines
    lookahead_count = 0
    for offset in range(1, min(5, len(lines) - trigger_idx)):
        candidate_line = lines[trigger_idx + offset]
        stripped = candidate_line.strip()
        if not stripped:
            continue
        lookahead_count += 1

        # Amount-only line (standalone €. amount)
        if _AMOUNT_ONLY_LINE_PAT.match(candidate_line):
            return _extract_amount_inline(candidate_line), offset

        # 2-line split: "€." alone on one line, number digits on the next.
        # e.g. trigger: "Valore complessivo del lotto:"
        #       offset+0: "€."
        #       offset+1: "118.731,30"
        if re.match(r"^\s*€\.?\s*$", candidate_line, re.IGNORECASE):
            for next_off in range(offset + 1, min(offset + 4, len(lines) - trigger_idx)):
                next_line = lines[trigger_idx + next_off]
                if not next_line.strip():
                    continue
                combined = f"€. {next_line.strip()}"
                if _AMOUNT_ONLY_LINE_PAT.match(combined):
                    return _extract_amount_inline(combined), next_off
                break
            break

        # Non-amount content → stop
        if lookahead_count >= 1:
            break

    return None, -1


# ---------------------------------------------------------------------------
# Trigger classification
# ---------------------------------------------------------------------------

def _classify_trigger(line: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Classify the line for a valuation trigger.

    Returns (field_type, explicit_lot_label_or_None).
    field_type is one of:
      "prezzo_base_raw"
      "valore_stima_raw"
      "valuation_market_raw"
      None  (no trigger)
    """
    # OMV header (always excluded — amount never follows in observed documents)
    if _OMV_HEADER_PAT.search(line):
        return None, None

    # Prezzo base (most unambiguous)
    m_pb = _PREZZO_BASE_PAT.search(line)
    if m_pb:
        explicit_lot = m_pb.group(1)  # may be None
        return "prezzo_base_raw", explicit_lot

    # Valore finale di stima → excluded (deprezzamento result)
    if _VALORE_FINALE_PAT.search(line):
        return None, None

    # Valore di stima del bene (most specific stima form)
    if _VALORE_STIMA_BENE_PAT.search(line):
        return "valore_stima_raw", None

    # Valore complessivo del lotto → treated as lot-level valore_stima_raw
    if _VALORE_COMPLESSIVO_LOTTO_PAT.search(line):
        return "valore_stima_raw", None

    # Generic valore di stima
    if _VALORE_STIMA_PAT.search(line):
        return "valore_stima_raw", None

    # Valuation market
    if _VALUATION_MARKET_PAT.search(line):
        return "valuation_market_raw", None

    return None, None


# ---------------------------------------------------------------------------
# Context window helper
# ---------------------------------------------------------------------------

def _make_context_window(lines: List[str], idx: int, back: int = 5, fwd: int = 3) -> str:
    start = max(0, idx - back)
    end = min(len(lines), idx + fwd + 1)
    parts = [l.strip() for l in lines[start:end] if l.strip()]
    return " | ".join(parts)


# ---------------------------------------------------------------------------
# Candidate ID
# ---------------------------------------------------------------------------

def _make_candidate_id(
    field_type: str,
    lot_id: Optional[str],
    bene_id: Optional[str],
    page: int,
    line_index: int,
    match_idx: int,
) -> str:
    lot_part = lot_id or "unknown"
    bene_part = bene_id or "na"
    return f"{field_type}::{lot_part}::{bene_part}::p{page}::l{line_index}::m{match_idx}"


# ---------------------------------------------------------------------------
# Scope attribution → valuation block types
# ---------------------------------------------------------------------------

_CADAT_TO_VAL_BLOCK: Dict[str, str] = {
    "CADASTRAL_IN_GLOBAL_PRE_LOT_ZONE":      "VALUATION_IN_GLOBAL_PRE_LOT_ZONE",
    "CADASTRAL_IN_SAME_PAGE_LOT_COLLISION":  "VALUATION_IN_SAME_PAGE_LOT_COLLISION",
    "CADASTRAL_IN_SAME_PAGE_BENE_COLLISION": "VALUATION_IN_SAME_PAGE_BENE_COLLISION",
    "CADASTRAL_SCOPE_AMBIGUOUS":             "VALUATION_SCOPE_AMBIGUOUS",
    "CADASTRAL_IN_BLOCKED_UNREADABLE":       "VALUATION_SCOPE_AMBIGUOUS",
}

_VAL_LEDGER_SAFE_ATTRIBUTIONS = {
    "CONFIRMED",
    "ATTRIBUTED_BY_SCOPE",
    "LOT_LEVEL_ONLY",
    "LOT_LEVEL_ONLY_PRE_BENE_CONTEXT",
}


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_valuation_candidate_pack(case_key: str) -> Dict[str, object]:
    ctx = build_context(case_key)

    hyp_fp    = ctx.artifact_dir / "structure_hypotheses.json"
    scope_fp  = ctx.artifact_dir / "lot_scope_map.json"
    pages_fp  = ctx.artifact_dir / "raw_pages.json"
    plh_fp    = ctx.artifact_dir / "plurality_headers.json"

    hyp       = json.loads(hyp_fp.read_text(encoding="utf-8"))
    scope     = json.loads(scope_fp.read_text(encoding="utf-8"))
    raw_pages: List[Dict] = json.loads(pages_fp.read_text(encoding="utf-8"))
    plurality_headers: Dict = (
        json.loads(plh_fp.read_text(encoding="utf-8")) if plh_fp.exists() else {}
    )

    winner = hyp.get("winner")

    out: Dict[str, object] = {
        "case_key": case_key,
        "winner": winner,
        "status": "OK",
        "candidates": [],
        "blocked_or_ambiguous": [],
        "warnings": [],
        "coverage": {
            "pages_scanned": len(raw_pages),
            "candidates_harvested": 0,
            "blocked_or_ambiguous_count": 0,
            "valuation_packet_count": 0,
            "valuation_fields_present": [],
            "valuation_scope_keys": [],
        },
        "summary": {},
        "source_artifacts": {
            "structure_hypotheses": str(hyp_fp),
            "lot_scope_map": str(scope_fp),
            "raw_pages": str(pages_fp),
            "plurality_headers": str(plh_fp),
        },
    }

    dst = ctx.artifact_dir / "valuation_candidate_pack.json"

    # Early exit: unreadable document
    if winner == "BLOCKED_UNREADABLE":
        out["status"] = "BLOCKED_UNREADABLE"
        out["summary"]["note"] = "Valuation harvesting blocked: document quality is BLOCKED_UNREADABLE."
        dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        return out

    # Build scope lookup tables
    bsm = build_bene_scope_map(case_key)
    lut = _build_lookup_tables(scope, bsm)
    hg_lookup = _build_lot_header_grade_lookup(plurality_headers)
    schema_pages = _find_schema_pages(raw_pages)
    last_bene_lookup = _build_last_bene_lookup(bsm)
    # Known lot IDs (normalised lower-case) used by the local-lot-context override.
    known_lot_ids: Set[str] = {
        str(ls["lot_id"]).strip().lower()
        for ls in (scope.get("lot_scopes") or [])
    }

    candidates: List[Dict] = []
    blocked_or_ambiguous: List[Dict] = []
    match_counter = 0

    # -----------------------------------------------------------------------
    # Page scan
    # -----------------------------------------------------------------------
    for page_data in raw_pages:
        page = int(page_data["page_number"])
        text = page_data.get("text", "") or ""
        lines = text.split("\n")
        is_schema_page = page in schema_pages

        for i, line in enumerate(lines):
            stripped = line.strip()
            if not stripped:
                continue

            # --- Basic line-level exclusions ---
            if _DOT_LEADER_PAT.search(line):
                continue
            if _RIDUZIONE_PREFIX_PAT.match(line):
                continue
            if _CALCOLO_HEADER_PAT.match(line):
                continue
            if _RIEPILOGO_HEADER_PAT.match(line):
                continue
            if _RENDITA_PAT.search(line):
                continue

            # PER_UNIT_RATE lines should be excluded only if the amount is a rate
            # (i.e. we would not capture it anyway since the rate pattern comes before
            # any colon; skip if the entire economic content is a rate)
            if _PER_UNIT_RATE_PAT.search(line) and not _AMOUNT_PAT.search(
                re.sub(r"[\d.,]+\s*€\s*/\s*mq.*", "", line, flags=re.IGNORECASE)
            ):
                continue

            # Valore finale → always excluded
            if _VALORE_FINALE_PAT.search(line):
                continue

            # Method prose lines (keyword in explanatory context) → skip
            if _METHOD_PROSE_PAT.search(line):
                continue

            # --- Trigger classification ---
            field_type, explicit_lot_label = _classify_trigger(line)
            if field_type is None:
                continue

            # --- OMV header detection (classify_trigger already returns None for OMV) ---
            # (handled above)

            # --- Try to extract amount ---
            amount_raw, amount_offset = _try_extract_amount(line, lines, i, field_type=field_type)

            # --- SCHEMA RIASSUNTIVO page handling ---
            if is_schema_page:
                if amount_raw:
                    blocked_or_ambiguous.append({
                        "type": "VALUATION_SUMMARY_DUPLICATE_UNSAFE",
                        "reason": (
                            "Candidate found on SCHEMA RIASSUNTIVO page; "
                            "treated as non-authoritative summary duplicate."
                        ),
                        "field_type": field_type,
                        "page": page,
                        "line_index": i,
                        "quote": stripped,
                        "extracted_value": amount_raw,
                    })
                else:
                    # No amount, table-header-only on summary page — silently skip
                    pass
                continue

            # --- No amount found → table header or label only ---
            if amount_raw is None:
                # OMV and other label-only lines
                blocked_or_ambiguous.append({
                    "type": "VALUATION_TABLE_HEADER_ONLY_EXCLUDED",
                    "reason": "Valuation label found with no associated amount value on this or next 3 lines.",
                    "field_type": field_type,
                    "page": page,
                    "line_index": i,
                    "quote": stripped,
                })
                continue

            # --- Determine scope ---
            scope_result = _cadat_determine_scope(page, winner, lut)
            attr = scope_result.get("attribution", "CADASTRAL_SCOPE_AMBIGUOUS")
            lot_id = scope_result.get("lot_id")
            bene_id = scope_result.get("bene_id")
            is_blocked_by_scope = scope_result.get("blocked", False)

            # --- Blocked by geometric scope ---
            if is_blocked_by_scope:
                block_type = _CADAT_TO_VAL_BLOCK.get(attr, "VALUATION_SCOPE_AMBIGUOUS")
                blocked_or_ambiguous.append({
                    "type": block_type,
                    "reason": f"Page {page} falls in a blocked geometric zone: {attr}.",
                    "field_type": field_type,
                    "page": page,
                    "line_index": i,
                    "quote": stripped,
                    "extracted_value": amount_raw,
                    "scope_attribution": attr,
                })
                continue

            # --- Local lot context override (multi-lot only) ---
            # For multi-lot documents the cadastral _determine_scope assigns the
            # page-scope lot.  But valuation paragraphs often open with an explicit
            # "LOTTO X viene …" sentence immediately before the trigger line.
            # When such a sentence is found in the preceding context AND the
            # referenced lot is a known lot in the document that DIFFERS from the
            # page-scope lot, we trust the local sentence over the page geometry.
            # (The HEADER_GRADE mismatch check below is skipped after an override
            # because the local context already resolved the attribution.)
            local_lot_override = False
            if (
                winner in ("H2_EXPLICIT_MULTI_LOT", "H4_CANDIDATE_MULTI_LOT_MULTI_BENE")
                and lot_id is not None
                and known_lot_ids
            ):
                local_lot = _find_local_lot_in_context(lines, i, known_lot_ids)
                if local_lot is not None and local_lot != str(lot_id).strip().lower():
                    lot_id = local_lot
                    bene_id = None   # bene attribution from the original scope is now invalid
                    attr = "LOT_LOCAL_CONTEXT_OVERRIDE"
                    local_lot_override = True

            # --- Local lot label mismatch (multi-lot only) ---
            # Skip when a local-context override has already resolved the lot, to
            # avoid false mismatch blocks against the now-overridden attribution.
            if not local_lot_override:
                mismatch_lot = _check_local_lot_mismatch(page, i, lot_id or "", hg_lookup, winner)
                if mismatch_lot is not None:
                    blocked_or_ambiguous.append({
                        "type": "VALUATION_LOCAL_SCOPE_HEADER_MISMATCH",
                        "reason": (
                            f"A HEADER_GRADE lot-label signal for lot '{mismatch_lot}' appears "
                            f"before line {i} on page {page}, which falls in lot '{lot_id}' "
                            "by page-scope geometry. Cross-lot attribution is not safe."
                        ),
                        "field_type": field_type,
                        "page": page,
                        "line_index": i,
                        "quote": stripped,
                        "extracted_value": amount_raw,
                        "page_scope_lot_id": lot_id,
                        "conflicting_lot_label": mismatch_lot,
                    })
                    continue

            # --- Explicit lot label mismatch for PREZZO_BASE prefix form ---
            if field_type == "prezzo_base_raw" and explicit_lot_label is not None:
                label_norm = str(explicit_lot_label).strip().lower()
                scope_lot_norm = str(lot_id or "").strip().lower()
                if scope_lot_norm and label_norm != scope_lot_norm:
                    blocked_or_ambiguous.append({
                        "type": "VALUATION_LOCAL_SCOPE_HEADER_MISMATCH",
                        "reason": (
                            f"Prezzo base line carries explicit lot label '{explicit_lot_label}' "
                            f"but page {page} belongs to lot '{lot_id}' by page-scope geometry."
                        ),
                        "field_type": field_type,
                        "page": page,
                        "line_index": i,
                        "quote": stripped,
                        "extracted_value": amount_raw,
                        "explicit_lot_label": explicit_lot_label,
                        "page_scope_lot_id": lot_id,
                    })
                    continue

            # --- Last-bene bene-local scope check (valuation-specific) ---
            # The shared cadastral _determine_scope returns LOT_LEVEL_ONLY for pages
            # inside the last bene's extended range (by design, for cadastral safety).
            # For *valore_stima_raw* this is wrong: a "Valore di stima" on a page
            # inside the last bene's scope is bene-local, not lot-level.
            #
            # Two sub-cases:
            #   A. Trigger is on the SAME PAGE as the bene's first header AND the
            #      header line precedes the trigger → safely attribute to that bene.
            #   B. Trigger is on a later page fully within the last bene's range →
            #      bene-local but sub-bene identity unknown; block explicitly.
            if (
                field_type == "valore_stima_raw"
                and attr in ("LOT_LEVEL_ONLY", "LOT_LEVEL_ONLY_PRE_BENE_CONTEXT")
                and lot_id is not None
                and not local_lot_override
            ):
                lb = last_bene_lookup.get(str(lot_id).strip())
                if lb is not None and lb["start_page"] <= page <= lb["end_page"]:
                    if (
                        lb["first_header_page"] == page
                        and lb["first_header_line_index"] >= 0
                        and lb["first_header_line_index"] < i
                    ):
                        # Case A: bene header clearly precedes this trigger on the
                        # same page → attribute to the last bene.
                        bene_id = lb["last_bene_id"]
                        attr = "ATTRIBUTED_BY_SCOPE"
                    else:
                        # Case B: trigger is inside the last bene's range but no
                        # bene header immediately precedes it → block as bene-local.
                        blocked_or_ambiguous.append({
                            "type": "VALUATION_BENE_LOCAL_LAST_BENE_SCOPE",
                            "reason": (
                                f"valore_stima_raw on page {page} falls within the last bene "
                                f"({lb['last_bene_id']}) page scope for lot {lot_id}. "
                                "Bene-local stima cannot safely be emitted as lot-level; "
                                "no bene header appears before this trigger line on this page."
                            ),
                            "field_type": field_type,
                            "page": page,
                            "line_index": i,
                            "quote": stripped,
                            "extracted_value": amount_raw,
                            "lot_id": lot_id,
                            "last_bene_id": lb["last_bene_id"],
                        })
                        continue

            # --- Emit candidate ---
            match_counter += 1
            cand_id = _make_candidate_id(field_type, lot_id, bene_id, page, i, match_counter)
            if amount_offset == 0:
                extraction_method = "REGEX_VAL_INLINE"
            elif field_type == "prezzo_base_raw" and not _LABEL_ENDS_COLON_PAT.search(line):
                extraction_method = f"REGEX_VAL_PREZZO_BASE_ACQUIRENTE_L+{amount_offset}"
            else:
                extraction_method = f"REGEX_VAL_SPLIT_L+{amount_offset}"

            context_window = _make_context_window(lines, i)

            # scope_basis text
            if attr in ("CONFIRMED", "CONFIRMED_BY_SINGLE_LOT"):
                scope_basis = f"Bene {bene_id} confirmed in lot {lot_id} by single-lot geometry."
            elif attr == "ATTRIBUTED_BY_SCOPE":
                scope_basis = f"Attributed to bene {bene_id} in lot {lot_id} by page containment."
            elif attr in ("LOT_LEVEL_ONLY", "LOT_LEVEL_ONLY_PRE_BENE_CONTEXT"):
                scope_basis = f"Page {page} falls in lot {lot_id} scope; no safe bene attribution."
            elif attr == "LOT_LOCAL_CONTEXT_OVERRIDE":
                scope_basis = (
                    f"Local 'LOTTO {str(lot_id).upper()}' mention in preceding context "
                    f"overrides page-scope geometry; attributed to lot {lot_id}."
                )
            else:
                scope_basis = f"Attribution: {attr}."

            candidates.append({
                "candidate_id": cand_id,
                "field_type": field_type,
                "extracted_value": amount_raw,
                "page": page,
                "line_index": i,
                "quote": stripped,
                "context_window": context_window,
                "extraction_method": extraction_method,
                "lot_id": lot_id,
                "bene_id": bene_id,
                "corpo_id": None,
                "attribution": attr,
                "scope_basis": scope_basis,
                "candidate_status": "ACTIVE",
                "explicit_lot_label": explicit_lot_label,
            })

    # -----------------------------------------------------------------------
    # Deduplication: same (lot_id, bene_id, field_type, normalized_value) →
    # keep the earliest by (page, line_index).
    # -----------------------------------------------------------------------
    seen_keys: Set[tuple] = set()
    deduped: List[Dict] = []
    dup_ids: List[str] = []

    for cand in sorted(candidates, key=lambda c: (c["page"], c["line_index"])):
        norm_val = _normalize_amount_str(cand["extracted_value"]).lower()
        key = (
            str(cand["lot_id"] or ""),
            str(cand["bene_id"] or ""),
            cand["field_type"],
            norm_val,
        )
        if key in seen_keys:
            dup_ids.append(cand["candidate_id"])
            blocked_or_ambiguous.append({
                "type": "VALUATION_SUMMARY_DUPLICATE_UNSAFE",
                "reason": "Duplicate candidate: same field/scope/amount already harvested at an earlier position.",
                "field_type": cand["field_type"],
                "page": cand["page"],
                "line_index": cand["line_index"],
                "quote": cand["quote"],
                "extracted_value": cand["extracted_value"],
                "duplicate_of": [c["candidate_id"] for c in deduped if (
                    str(c["lot_id"] or "") == str(cand["lot_id"] or "")
                    and str(c["bene_id"] or "") == str(cand["bene_id"] or "")
                    and c["field_type"] == cand["field_type"]
                    and _normalize_amount_str(c["extracted_value"]).lower() == norm_val
                )],
            })
        else:
            seen_keys.add(key)
            deduped.append(cand)

    # -----------------------------------------------------------------------
    # Conflict detection: group surviving candidates by (lot_id, bene_key,
    # field_type).  If multiple distinct values → VALUATION_MULTI_VALUE_UNRESOLVED.
    # -----------------------------------------------------------------------
    grouped: Dict[tuple, List[Dict]] = {}
    for cand in deduped:
        lot_id = cand["lot_id"] or "unknown"
        bene_key = cand["bene_id"] or "lot"
        key = (lot_id, bene_key, cand["field_type"])
        grouped.setdefault(key, []).append(cand)

    final_candidates: List[Dict] = []
    for (lot_id, bene_key, field_type), group in sorted(
        grouped.items(), key=lambda item: item[0]
    ):
        distinct_norm = sorted({
            _normalize_amount_str(c["extracted_value"]).lower() for c in group
        })
        if len(distinct_norm) > 1:
            # Mark all as BLOCKED; emit one MULTI_VALUE_UNRESOLVED entry
            for cand in group:
                cand = dict(cand)
                cand["candidate_status"] = "BLOCKED"
                final_candidates.append(cand)
            blocked_or_ambiguous.append({
                "type": "VALUATION_MULTI_VALUE_UNRESOLVED",
                "reason": (
                    f"Multiple distinct {field_type} values exist for the same "
                    f"scope (lot={lot_id}, bene={bene_key}); "
                    "no ACTIVE packet emitted. Synthesis not performed."
                ),
                "field_type": field_type,
                "lot_id": lot_id,
                "bene_id": None if bene_key == "lot" else bene_key,
                "distinct_values": sorted(
                    {c["extracted_value"] for c in group},
                    key=lambda v: (c["page"] for c in group if c["extracted_value"] == v).__next__()
                    if any(c["extracted_value"] == v for c in group) else 0,
                ),
                "candidate_count": len(group),
                "candidates": [
                    {
                        "candidate_id": c["candidate_id"],
                        "extracted_value": c["extracted_value"],
                        "page": c["page"],
                        "line_index": c["line_index"],
                        "quote": c["quote"],
                        "attribution": c["attribution"],
                    }
                    for c in group
                ],
            })
        else:
            for cand in group:
                final_candidates.append(cand)

    # -----------------------------------------------------------------------
    # Coverage counters
    # -----------------------------------------------------------------------
    active_cands = [c for c in final_candidates if c["candidate_status"] == "ACTIVE"]
    fields_present = sorted({c["field_type"] for c in active_cands})
    scope_keys = sorted({
        (f"{c['lot_id']}/{c['bene_id']}" if c["bene_id"] else f"lot:{c['lot_id']}")
        for c in active_cands
    })

    out["candidates"] = final_candidates
    out["blocked_or_ambiguous"] = blocked_or_ambiguous
    out["coverage"]["candidates_harvested"] = len(final_candidates)
    out["coverage"]["blocked_or_ambiguous_count"] = len(blocked_or_ambiguous)
    out["coverage"]["valuation_packet_count"] = len(active_cands)
    out["coverage"]["valuation_fields_present"] = fields_present
    out["coverage"]["valuation_scope_keys"] = scope_keys

    # Summary
    active_by_field: Dict[str, List[str]] = {}
    for c in active_cands:
        active_by_field.setdefault(c["field_type"], []).append(
            f"lot:{c['lot_id']}|bene:{c['bene_id']}|p{c['page']}"
        )
    out["summary"] = {
        "active_candidate_count": len(active_cands),
        "blocked_or_ambiguous_count": len(blocked_or_ambiguous),
        "active_by_field": active_by_field,
        "scope_keys": scope_keys,
    }

    # Warnings: note if no valuation candidates at all
    if not active_cands and winner not in ("BLOCKED_UNREADABLE",):
        out["warnings"].append(
            "No ACTIVE valuation candidates harvested; "
            "check blocked_or_ambiguous for reasons."
        )

    dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Valuation candidate pack shell")
    parser.add_argument("--case", required=True, choices=list_case_keys())
    args = parser.parse_args()

    out = build_valuation_candidate_pack(args.case)
    import json as _json
    print(_json.dumps({
        "case_key": out["case_key"],
        "status": out["status"],
        "winner": out["winner"],
        "active_candidates": out["coverage"]["valuation_packet_count"],
        "blocked_or_ambiguous": out["coverage"]["blocked_or_ambiguous_count"],
        "fields_present": out["coverage"]["valuation_fields_present"],
        "scope_keys": out["coverage"]["valuation_scope_keys"],
        "warnings": out["warnings"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
