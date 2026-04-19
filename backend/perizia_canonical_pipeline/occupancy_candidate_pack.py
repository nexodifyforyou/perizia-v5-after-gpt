from __future__ import annotations

import argparse
import json
import re
from typing import Dict, List, Optional, Tuple

from .runner import build_context
from .corpus_registry import load_cases, list_case_keys
from .bene_scope_map import build_bene_scope_map
from .cadastral_candidate_pack import _build_lookup_tables, _determine_scope as _cadat_determine_scope


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------

def _join_window(lines: List[str], start_idx: int, window: int = 6) -> str:
    """Join non-blank lines in a forward window into a single space-separated string."""
    parts: List[str] = []
    for raw in lines[start_idx : start_idx + window]:
        part = raw.strip()
        if part:
            parts.append(part)
    return " ".join(parts)


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _normalize_redaction(text: str) -> str:
    """Collapse OCR-split redaction markers (e.g. '** **', '*** ***') → '****'.

    Only replaces when two or more asterisk groups are separated by whitespace
    (the OCR split case).  Single-block markers like '****' are left intact so
    surrounding spaces are preserved.
    """
    return re.sub(r"\*+(\s+\*+)+", "****", text)


# ---------------------------------------------------------------------------
# Occupancy trigger and exclusion patterns
# ---------------------------------------------------------------------------

# Primary trigger: lines containing "stato di occupazione" OR
# "stato di possesso dell'immobile" OR
# "stato di possesso al momento del sopralluogo" (incl. OCR variant "a l").
# The possesso / sopralluogo heading forms introduce inspection-narrative
# evidence and are handled with a wider forward window and dedicated patterns.
OCC_TRIGGER_PAT = re.compile(
    r"\bstato\s+di\s+(?:"
    r"occupazione"
    r"|possesso\s+dell.immobile"
    r"|possesso\s+a[\s]*l\s+momento\s+del\s+sopralluogo"
    r"|possesso\s+del\s+bene"
    r")\b"
    r"|\b\d+[a-z]?\.\s+stato\s+di\s+possesso\s*:",
    re.IGNORECASE | re.UNICODE,
)

# Detect which form fired so we can apply the right window width and patterns.
# The sopralluogo heading form ("STATO DI POSSESSO AL/A L MOMENTO DEL
# SOPRALLUOGO") is treated identically to the possesso form: wide window,
# patterns E/F/G first, then fall-through to A→D.
OCC_POSSESSO_FORM_PAT = re.compile(
    r"\bstato\s+di\s+possesso\s+(?:dell.immobile|a[\s]*l\s+momento\s+del\s+sopralluogo|del\s+bene)\b"
    r"|\b\d+[a-z]?\.\s+stato\s+di\s+possesso\s*:",
    re.IGNORECASE | re.UNICODE,
)

# Procedural / valuation context signals in the 8-line lookback window.
# Any match → block the candidate.
OCC_PROC_SIGNAL_PAT = re.compile(
    r"\b(?:"
    # Valuation: "Riduzione del X% per lo stato di occupazione"
    r"riduzione\s+del\s+\d"
    # Boilerplate conveyance text
    r"|stato\s+di\s+possesso\s+goduto\s+dalla"
    # Valuation: "in condizioni di libero mercato"
    r"|in\s+condizioni\s+di\s+libero\s+mercato"
    # Custodian eviction process note
    r"|ha\s+intimato\s+(?:loro\s+)?la\s+liberazione"
    # Registry confirmation of no lease
    r"|non\s+risultano\s+registrati\s+atti\s+di\s+locazione"
    # Inspection / sopralluogo narrative — only suppress when these phrases
    # appear in the LOOKBACK (i.e. before the trigger line).  They cannot
    # appear after the trigger, so this is safe for possesso sections too.
    r"|durante\s+il\s+sopralluogo"
    r"|in\s+sede\s+di\s+sopralluogo"
    # Valuation prose
    r"|si\s+sceglie\s+di\s+considerare\s+come\s+parametro"
    # Formalità pregiudizievoli context
    r"|FORMALIT[AÀ]\s+PREGIUDIZIEVOLI"
    # Table of contents detection (many consecutive dots)
    r"|\.\s*\.\s*\.\s*\.\s*\."
    r")\b",
    re.IGNORECASE | re.UNICODE,
)

# Schema riassuntivo pages contain data for ALL lots/benes in sequence.
# Cross-lot contamination is unavoidable with short local-header lookbacks,
# so we block all occupancy candidates from pages that carry this header.
_SCHEMA_RIASSUNTIVO_PAT = re.compile(
    r"\bSCHEMA\s+RIASSUNTIVO\b",
    re.IGNORECASE | re.UNICODE,
)

# ---------------------------------------------------------------------------
# Sibling occupancy nuance patterns
# Detected from the forward window when a primary occupancy status is found.
# ---------------------------------------------------------------------------

# Non-opponibilità: "non opponibile all'aggiudicatario"
_OCC_NON_OPPONIBILE_PAT = re.compile(
    r"\bnon\s+opponibil[ei]\b",
    re.IGNORECASE,
)

# Saltuaria occupazione dell'esecutato: "occupato saltuariamente dall'esecutato"
_OCC_SALTUARIA_PAT = re.compile(
    r"\boccupat[oa]\s+saltuariamente\b|\bsaltuaria\s+occupazione\b",
    re.IGNORECASE,
)

# Liberazione a cura della procedura: "liberazione a cura e spese della procedura"
_OCC_LIBERAZIONE_PROCEDURA_PAT = re.compile(
    r"\bliberazione\s+a\s+cura\s+(?:e\s+spese\s+)?della\s+procedura\b",
    re.IGNORECASE,
)

# Liberazione a cura dell'acquirente: "liberazione a cura e spese dell'acquirente"
_OCC_LIBERAZIONE_ACQUIRENTE_PAT = re.compile(
    r"\bliberazione\s+a\s+cura\s+(?:e\s+spese\s+)?dell.acquirente\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Value extraction patterns (applied to joined forward window)
# ---------------------------------------------------------------------------
# NOTE: re.IGNORECASE is in effect for all patterns.  Uppercase character
# classes like [A-Z] also match lowercase letters under IGNORECASE, so we
# cannot use [A-Z] to distinguish sentence-starts; instead we use explicit
# stop-word lookaheads or period/semicolon terminators.

# Pattern A — "Stato di occupazione: <value>" (inline label form).
# Each alternative is designed to stop naturally without an overcapturing
# non-greedy continuation:
#   • "libero" → captured as a single word (optional suffix for "al decreto...")
#   • "non occupato" / "in corso di liberazione" → exact phrases
#   • "occupato da ..." → stops at first period or semicolon
#   • "locato / detenuto ..." → stop at period or semicolon
OCC_PAT_LABEL = re.compile(
    r"stato\s+di\s+occupazione\s*:\s*"
    r"(libero(?:\s+al\s+decreto\s+di\s+trasferimento)?\b"
    r"|non\s+occupato\b"
    r"|in\s+corso\s+di\s+liberazione\b"
    r"|occupato\s+da\s+[^.;]{2,200}?(?=[.;]|\s*(?:Allegato|LOTTO\s|\bBene\s+N|\bFirmato)|\s*$)"
    r"|occupato\s+dal?\s+(?:debitore|coniuge|terzi|proprietario)[^.;]{0,60}?(?=[.;]|\s*(?:Allegato|LOTTO\s)|\s*$)"
    r"|locato\s+[^.;]{2,100}?(?=[.;]|\s*$)"
    r"|detenuto\s+[^.;]{2,100}?(?=[.;]|\s*$)"
    r")",
    re.IGNORECASE | re.UNICODE,
)

# Pattern B — "L'immobile risulta / è <status>" or "L'unità immobiliare è <status>".
# "libero" is captured as an exact word (doesn't extend into following prose).
# Complex statuses stop at period or semicolon.
# Handles both "da " and "dal " (Italian contraction) to cover
# "occupato dal debitore" as well as "occupato da terzi".
# Also handles "occupato con contratto di affitto ..." for tenanted cases.
OCC_PAT_RISULTA = re.compile(
    r"(?:l'?immobile|il\s+bene|l'?unit[aà]\s+immobiliare|l\W?unit[aà]\s+immobiliare)\s+"
    r"(?:risulta|è|e)\s+"
    r"(libero(?:\s+al\s+decreto\s+di\s+trasferimento)?\b"
    r"|non\s+occupato\b"
    r"|occupat[ao]\s+da\s+[^.;]{2,200}?(?=[.;]|\s*$)"
    r"|occupat[ao]\s+dal\s+[^.;]{0,200}?(?=[.;]|\s*$)"
    r"|occupat[ao]\s+con\s+contratto[^.;]{0,200}?(?=[.;]|\s*$)"
    r"|locato\s+[^.;]{2,100}?(?=[.;]|\s*$)"
    r"|detenuto\s+[^.;]{2,100}?(?=[.;]|\s*$)"
    r")",
    re.IGNORECASE | re.UNICODE,
)

# Pattern C — standalone "Occupato da ..." direct statement.
# Used when the trigger is a section header and the occupancy value follows
# on a subsequent line within the forward window.
OCC_PAT_OCCUPATO_DA = re.compile(
    r"\b(occupato\s+da\s+[^.;]{2,200}?"
    r"(?:debitore|coniuge|terzi|conduttore|comodatario|proprietario)[^.;]{0,80}?)"
    r"(?=[.;]|\s*$)",
    re.IGNORECASE | re.UNICODE,
)

# Pattern D — "Libero" standalone when after "Stato di occupazione:" (colon or no colon).
# Kept as a final fallback for cases where none of the above fire.
OCC_PAT_STANDALONE_LIBERO = re.compile(
    r"\bstato\s+di\s+occupazione\b[^a-z]{0,10}?(libero)\b",
    re.IGNORECASE | re.UNICODE,
)

# Pattern D2 — "il bene/l'immobile/unità risulta libero" with flexible intervening words.
# Handles cases where intervening words (e.g. "attualmente") appear between subject and
# "risulta", and Unicode apostrophe (U+2019) in "l'immobile".
OCC_PAT_RISULTA_LIBERO = re.compile(
    r"\b(?:immobile|bene|unit[aà]\s+immobiliare)[^.;]{0,60}?\b(risulta)\b[^.;]{0,30}?\b"
    r"(libero(?:\s+al\s+decreto\s+di\s+trasferimento)?)\b",
    re.IGNORECASE | re.UNICODE,
)

# ---------------------------------------------------------------------------
# Possesso-form specific patterns (applied in 12-line window)
# These extract occupancy evidence from "STATO DI POSSESSO DELL'IMMOBILE"
# inspection-narrative sections, which describe who the inspector found
# in the property and under what title.
# ---------------------------------------------------------------------------

# Pattern E — tenant/conduttore role found in inspection narrative.
# Presence of "conduttrice" or "conduttore" signals the property is tenanted.
OCC_PAT_POSSESSO_CONDUTTORE = re.compile(
    r"\b(conduttr(?:ice|ore))\b",
    re.IGNORECASE | re.UNICODE,
)

# Pattern F — lease/rental contract mentioned in inspection narrative.
# Extracts the contract description as occupancy_title_raw and implies "locato".
OCC_PAT_POSSESSO_TITLE = re.compile(
    r"((?:in\s+forza\s+del\s+)?[Cc]ontratto\s+di\s+(?:[Ll]ocazione|[Aa]ffitto)[^.;]{0,120}?)"
    r"(?=[.;]|\s*$)",
    re.IGNORECASE | re.UNICODE,
)

# Pattern G — comodato (loan for use) in inspection narrative.
OCC_PAT_POSSESSO_COMODATO = re.compile(
    r"\b(comodato[^.;]{0,80}?)"
    r"(?=[.;]|\s*$)",
    re.IGNORECASE | re.UNICODE,
)

# ---------------------------------------------------------------------------
# Title extraction (from status_raw string)
# ---------------------------------------------------------------------------

OCC_TITLE_PATTERNS = [
    re.compile(r"\b(senza\s+titolo)\b", re.IGNORECASE | re.UNICODE),
    re.compile(r"\b(con\s+contratto\s+di\s+locazione[^.]{0,80}?)\s*(?:\.|$)", re.IGNORECASE | re.UNICODE),
    re.compile(r"\b(con\s+contratto\s+di\s+natura\s+\w+[^.]{0,60}?)\s*(?:\.|$)", re.IGNORECASE | re.UNICODE),
    re.compile(r"\b(in\s+forza\s+di\s+comodato)\b", re.IGNORECASE | re.UNICODE),
    re.compile(r"\b(non\s+opponibile)\b", re.IGNORECASE | re.UNICODE),
    re.compile(r"\b(opponibile)\b", re.IGNORECASE | re.UNICODE),
    re.compile(r"\b(coniuge\s+assegnatario)\b", re.IGNORECASE | re.UNICODE),
]


def _extract_title_raw(status_raw: str) -> Optional[str]:
    """Return the occupancy title substring if a known title pattern is present."""
    for pat in OCC_TITLE_PATTERNS:
        m = pat.search(status_raw)
        if m:
            return _normalize(m.group(1))
    return None


# ---------------------------------------------------------------------------
# Local header consistency patterns (mirrors rights_candidate_pack)
# ---------------------------------------------------------------------------

_LOCAL_LOT_HEADER_PAT = re.compile(r"^\s*LOTTO\s+(\S+)\s*$", re.IGNORECASE)
_LOCAL_BENE_HEADER_PAT = re.compile(r"\bBene\s+N\s*[°\.]\s*([0-9]+)", re.IGNORECASE)


def _nearest_local_headers(
    lines: List[str], line_idx: int, lookback: int = 12
) -> Tuple[Optional[str], Optional[str]]:
    start = max(0, line_idx - lookback)
    local_lot_id: Optional[str] = None
    local_bene_id: Optional[str] = None
    for raw_line in reversed(lines[start : line_idx + 1]):
        if local_lot_id is None:
            m = _LOCAL_LOT_HEADER_PAT.match(raw_line)
            if m:
                local_lot_id = m.group(1).lower()
        if local_bene_id is None:
            m = _LOCAL_BENE_HEADER_PAT.search(raw_line)
            if m:
                local_bene_id = m.group(1).strip()
        if local_lot_id is not None and local_bene_id is not None:
            break
    return local_lot_id, local_bene_id


# Pattern: "BENE N° X" or "BENE N. X" — used in the forward-window guard.
_BENE_HEADING_FORWARD_PAT = re.compile(r"\bBENE\s+N[°\.]\s*[0-9]+\b", re.IGNORECASE)

# Line-start-anchored bene section header — used in the wider backward-lookback guard.
# The anchor prevents matching inline prose like "il successivo bene n. 3".
_BENE_SECTION_HEADER_WIDE_PAT = re.compile(
    r"^\s*Bene\s+N\s*[°\.]\s*([0-9]+)\b", re.IGNORECASE
)

_LOCAL_BENE_OCC_STOP_PAT = re.compile(
    r"^\s*(?:"
    r"Bene\s+N\s*[°\.]"
    r"|LOTTO\s+\S+"
    r"|PROVENIENZE\s+VENTENNALI"
    r"|FORMALIT"
    r"|VINCOLI"
    r"|STIMA"
    r"|VALORE"
    r"|CONFORMIT"
    r"|Allegato\s+n"
    r")",
    re.IGNORECASE,
)

# Lot-transition: a line that begins a new LOTTO section (schema or narrative).
_LOT_TRANSITION_PAT = re.compile(r"^\s*LOTTO\s+\S+", re.IGNORECASE)


def _nearest_bene_section_header(
    lines: List[str], line_idx: int, lookback: int = 30
) -> Optional[str]:
    """Find the nearest explicit bene section header within a wider backward window.

    Uses a line-start-anchored pattern to avoid matching inline bene references
    embedded in description prose (e.g. "il successivo bene n. 3").
    Returns the bene id string if found, else None.
    """
    start = max(0, line_idx - lookback)
    for raw_line in reversed(lines[start : line_idx + 1]):
        m = _BENE_SECTION_HEADER_WIDE_PAT.match(raw_line)
        if m:
            return m.group(1).strip()
    return None


def _has_forward_lot_transition(
    lines: List[str], line_idx: int, forward: int = 3
) -> bool:
    """Return True if a new LOTTO section header appears within `forward` non-blank lines.

    Catches candidates that appear at the tail of one lot's schema entry immediately
    before the next LOTTO heading begins.  Such candidates are transition-boundary
    text and cannot be safely attributed to the lot whose scope page-range contains
    this page.
    """
    count = 0
    for i in range(line_idx + 1, len(lines)):
        stripped = lines[i].strip()
        if not stripped:
            continue
        count += 1
        if _LOT_TRANSITION_PAT.match(stripped):
            return True
        if count >= forward:
            break
    return False


def _has_forward_bene_heading(lines: List[str], line_idx: int, forward: int = 2) -> bool:
    """Return True if a BENE N° heading appears within the next `forward` non-blank lines.

    Used to detect occupancy-section headings such as "STATO DI OCCUPAZIONE"
    that are immediately followed by a bene-specific sub-heading ("BENE N° 1 …").
    In that layout the evidence is bene-scoped, not lot-level.

    forward=2 (default) deliberately keeps the window tight: the bene sub-heading
    must appear within two non-blank lines of the trigger, not merely nearby.
    This avoids treating the start of the *next* schema table entry (several lines
    later) as a sub-heading for the current trigger.
    """
    count = 0
    for i in range(line_idx + 1, len(lines)):
        stripped = lines[i].strip()
        if not stripped:
            continue
        count += 1
        if _BENE_HEADING_FORWARD_PAT.search(stripped):
            return True
        if count >= forward:
            break
    return False


# ---------------------------------------------------------------------------
# Scope remapping (CADASTRAL_ → OCCUPANCY_)
# ---------------------------------------------------------------------------

_CADAT_TO_OCC_ATTR = {
    "CADASTRAL_IN_BLOCKED_UNREADABLE": "OCCUPANCY_IN_BLOCKED_UNREADABLE",
    "CADASTRAL_IN_GLOBAL_PRE_LOT_ZONE": "OCCUPANCY_IN_GLOBAL_PRE_LOT_ZONE",
    "CADASTRAL_IN_SAME_PAGE_LOT_COLLISION": "OCCUPANCY_IN_SAME_PAGE_LOT_COLLISION",
    "CADASTRAL_SCOPE_AMBIGUOUS": "OCCUPANCY_SCOPE_AMBIGUOUS",
    "CADASTRAL_IN_SAME_PAGE_BENE_COLLISION": "OCCUPANCY_IN_SAME_PAGE_BENE_COLLISION",
}

_LEDGER_SAFE_ATTRIBUTIONS = {
    "CONFIRMED",
    "ATTRIBUTED_BY_SCOPE",
    "LOT_LEVEL_ONLY",
    "LOT_LEVEL_ONLY_PRE_BENE_CONTEXT",
}


def _determine_occ_scope(page: int, winner: str, lut: Dict) -> Dict:
    result = dict(_cadat_determine_scope(page, winner, lut))
    result["attribution"] = _CADAT_TO_OCC_ATTR.get(result["attribution"], result["attribution"])
    return result


def _scope_for_local_bene(scope_info: Dict, local_bene_id: Optional[str], lut: Dict) -> Dict:
    if not local_bene_id:
        return scope_info
    lot_id = scope_info.get("lot_id")
    if not lot_id:
        return scope_info
    for bs in lut["bene_by_lot"].get(str(lot_id), []):
        if str(bs.get("bene_id")) == str(local_bene_id):
            return {
                "attribution": "CONFIRMED",
                "blocked": False,
                "lot_id": str(lot_id),
                "bene_id": str(bs.get("bene_id")),
                "composite_key": str(bs.get("composite_key")),
            }
    return scope_info


def _collect_local_bene_occupancy_lines(lines: List[str], header_idx: int) -> List[str]:
    values: List[str] = []
    for raw in lines[header_idx + 1 : min(len(lines), header_idx + 10)]:
        stripped = raw.strip()
        if not stripped:
            continue
        if _LOCAL_BENE_OCC_STOP_PAT.match(stripped):
            break
        m = OCC_PAT_OCCUPATO_DA.search(stripped)
        if m:
            status = _normalize_redaction(_normalize(m.group(1)))
            status = re.sub(r"\s+\.", ".", status).strip()
            values.append(status)
    return values


def _material_occupancy_key(value: str) -> str:
    value = _normalize_redaction(value)
    value = re.sub(r"\s+", " ", value).strip().lower()
    value = re.sub(r"\s+\.", ".", value)
    return value


# ---------------------------------------------------------------------------
# Procedural context check (lookback window)
# ---------------------------------------------------------------------------

def _has_proc_context(lines: List[str], line_idx: int, before: int = 8) -> bool:
    start = max(0, line_idx - before)
    for line in lines[start : line_idx + 1]:
        if OCC_PROC_SIGNAL_PAT.search(line):
            return True
    return False


# ---------------------------------------------------------------------------
# Candidate ID
# ---------------------------------------------------------------------------

def _make_cid(
    field_type: str,
    lot_id: Optional[str],
    bene_id: Optional[str],
    page: int,
    line_index: int,
    idx: int,
) -> str:
    lot_part = lot_id or "unknown"
    bene_part = bene_id or "na"
    return f"occ_{field_type}::{lot_part}::{bene_part}::p{page}::l{line_index}::m{idx}"


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_occupancy_candidate_pack(case_key: str) -> Dict:
    ctx = build_context(case_key)
    bsm = build_bene_scope_map(case_key)

    hyp_fp = ctx.artifact_dir / "structure_hypotheses.json"
    scope_fp = ctx.artifact_dir / "lot_scope_map.json"
    pages_fp = ctx.artifact_dir / "raw_pages.json"

    hyp = json.loads(hyp_fp.read_text(encoding="utf-8"))
    scope = json.loads(scope_fp.read_text(encoding="utf-8"))
    raw_pages: List[Dict] = json.loads(pages_fp.read_text(encoding="utf-8"))

    winner = hyp.get("winner")

    out: Dict = {
        "case_key": case_key,
        "winner": winner,
        "status": "OK",
        "candidates": [],
        "blocked_or_ambiguous": [],
        "warnings": [],
        "summary": {},
        "coverage": {
            "pages_scanned": len(raw_pages),
            "candidate_count": 0,
            "blocked_or_ambiguous_count": 0,
            "occupancy_fields_present": [],
            "occupancy_scope_keys": [],
        },
        "source_artifacts": {
            "structure_hypotheses": str(hyp_fp),
            "lot_scope_map": str(scope_fp),
            "bene_scope_map": str(ctx.artifact_dir / "bene_scope_map.json"),
            "raw_pages": str(pages_fp),
        },
    }

    if winner == "BLOCKED_UNREADABLE":
        out["status"] = "BLOCKED_UNREADABLE"
        dst = ctx.artifact_dir / "occupancy_candidate_pack.json"
        dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        return out

    lut = _build_lookup_tables(scope, bsm)

    candidates: List[Dict] = []
    blocked_or_ambiguous: List[Dict] = []
    seen_dedup: set = set()
    match_counter = [0]

    def _next_idx() -> int:
        match_counter[0] += 1
        return match_counter[0]

    # -----------------------------------------------------------------------
    # Emit helpers
    # -----------------------------------------------------------------------

    def _emit_active(
        fields: List[Tuple[str, str]],
        page: int,
        line_index: int,
        quote: str,
        context_window: str,
        extraction_method: str,
        lot_id: Optional[str],
        bene_id: Optional[str],
        composite_key: Optional[str],
        attribution: str,
        scope_basis: str,
    ) -> None:
        if not fields:
            return

        scope_key = composite_key or f"lot:{lot_id or 'unknown'}"

        status_val = next((v for ft, v in fields if ft == "occupancy_status_raw"), "")
        value_dedup_key = (scope_key, status_val.lower()[:80], attribution)
        page_dedup_key = (page, line_index, extraction_method)

        if value_dedup_key in seen_dedup or page_dedup_key in seen_dedup:
            return
        seen_dedup.add(value_dedup_key)
        seen_dedup.add(page_dedup_key)

        idx = _next_idx()
        sibling_map = {ft: fv for ft, fv in fields}

        for fi, (field_type, value) in enumerate(fields):
            candidates.append({
                "candidate_id": _make_cid(field_type, lot_id, bene_id, page, line_index, idx + fi),
                "field_type": field_type,
                "extracted_value": value,
                "page": page,
                "line_index": line_index,
                "quote": quote[:300],
                "context_window": context_window[:400],
                "extraction_method": extraction_method,
                "lot_id": lot_id,
                "bene_id": bene_id,
                "composite_key": composite_key,
                "attribution": attribution,
                "scope_basis": scope_basis,
                "candidate_status": "ACTIVE",
                "source_type": "BODY_TEXT",
                "sibling_fields": {ft: fv for ft, fv in sibling_map.items() if ft != field_type},
            })

    def _emit_blocked(
        block_type: str,
        page: int,
        line_index: int,
        quote: str,
        context: str,
        extraction_method: str,
        lot_id: Optional[str],
        bene_id: Optional[str],
        reason: str,
        extra: Optional[Dict] = None,
    ) -> None:
        entry: Dict = {
            "type": block_type,
            "page": page,
            "line_index": line_index,
            "line_quote": quote[:200],
            "context": context[:300],
            "extraction_method": extraction_method,
            "lot_id": lot_id,
            "bene_id": bene_id,
            "reason": reason,
        }
        if extra:
            entry.update(extra)
        blocked_or_ambiguous.append(entry)

    # -----------------------------------------------------------------------
    # Page scanning
    # -----------------------------------------------------------------------

    for page_data in raw_pages:
        page = int(page_data["page_number"])
        lines = page_data["text"].split("\n")
        scope_info = _determine_occ_scope(page, winner, lut)
        is_blocked = scope_info["blocked"]
        attribution = scope_info["attribution"]

        # Block the entire page if it is a "SCHEMA RIASSUNTIVO" page.
        # These summary pages list all lots/benes sequentially; the local
        # lot headers span many lines apart so our lookback guard cannot
        # reliably prevent cross-lot contamination.
        page_text = page_data["text"]
        if _SCHEMA_RIASSUNTIVO_PAT.search(page_text):
            # Still surface a single blocked entry so the caller knows this
            # page was considered but skipped.
            blocked_or_ambiguous.append({
                "type": "OCCUPANCY_SCOPE_AMBIGUOUS",
                "page": page,
                "line_index": None,
                "line_quote": "",
                "context": "Schema riassuntivo page — multiple lots/benes appear in sequence; cross-lot scope guard not possible.",
                "extraction_method": "PAGE_LEVEL_EXCLUSION",
                "lot_id": scope_info["lot_id"],
                "bene_id": scope_info["bene_id"],
                "reason": (
                    "SCHEMA RIASSUNTIVO detected on this page. All lots/benes appear "
                    "in sequence; reliable lot-level scope attribution is impossible "
                    "without full-page line-order analysis. Page skipped entirely."
                ),
            })
            continue

        for line_idx, line in enumerate(lines):
            line_s = line.strip()
            if not line_s:
                continue

            local_bene_match = _BENE_SECTION_HEADER_WIDE_PAT.match(line_s)
            if local_bene_match:
                local_status_parts = _collect_local_bene_occupancy_lines(lines, line_idx)
                if local_status_parts:
                    local_scope = _scope_for_local_bene(
                        scope_info, local_bene_match.group(1).strip(), lut
                    )
                    if local_scope.get("blocked"):
                        _emit_blocked(
                            local_scope.get("attribution", "OCCUPANCY_SCOPE_AMBIGUOUS"),
                            page, line_idx, line_s,
                            _normalize(_join_window(lines, line_idx, 8)),
                            "OCC_LOCAL_BENE_OCCUPATO_DA",
                            local_scope.get("lot_id"), local_scope.get("bene_id"),
                            reason=(
                                "Local Bene occupancy block found but scope attribution "
                                f"is {local_scope.get('attribution')}."
                            ),
                            extra={"occupancy_status_raw": " ".join(local_status_parts)},
                        )
                    else:
                        status_raw = _normalize(" ".join(local_status_parts))
                        title_raw = _extract_title_raw(status_raw)
                        fields: List[Tuple[str, str]] = [("occupancy_status_raw", status_raw)]
                        if title_raw:
                            fields.append(("occupancy_title_raw", title_raw))
                        scope_basis = (
                            f"Explicit local Bene N° {local_bene_match.group(1).strip()} "
                            f"occupancy block on p{page}:l{line_idx}; "
                            f"scope attribution: {local_scope.get('attribution')}."
                        )
                        _emit_active(
                            fields, page, line_idx, line_s,
                            _normalize(_join_window(lines, line_idx, 8)),
                            "OCC_LOCAL_BENE_OCCUPATO_DA",
                            local_scope.get("lot_id"), local_scope.get("bene_id"),
                            local_scope.get("composite_key"),
                            local_scope.get("attribution", "CONFIRMED"),
                            scope_basis,
                        )

            # Only trigger on lines containing "stato di occupazione" or
            # "stato di possesso dell'immobile"
            if not OCC_TRIGGER_PAT.search(line_s):
                continue

            # Detect which form fired so we can use the right window width
            # and extraction strategy.
            is_possesso_form = bool(OCC_POSSESSO_FORM_PAT.search(line_s))

            # Procedural / valuation context exclusion (8-line lookback).
            # For the possesso form this check is applied too, but the signals
            # that would fire (e.g. "in sede di sopralluogo") appear BEFORE the
            # heading, not inside the narrative, so the lookback is still safe.
            if _has_proc_context(lines, line_idx, before=8):
                _emit_blocked(
                    "OCCUPANCY_PROCEDURAL_PROSE_EXCLUDED",
                    page, line_idx, line_s,
                    _normalize(_join_window(lines, max(0, line_idx - 3), 5)),
                    "BODY_TEXT",
                    scope_info["lot_id"], scope_info["bene_id"],
                    reason=(
                        "Occupancy trigger found but procedural/valuation context "
                        "signal detected in 8-line lookback; candidate excluded."
                    ),
                )
                continue

            # Bene-headed block guard (non-possesso form only).
            # If the trigger is a bare occupancy section heading (e.g. "STATO DI
            # OCCUPAZIONE") and the immediately following non-blank lines name a
            # specific "BENE N° X", the evidence block is bene-scoped.  It must
            # not be promoted to LOT_LEVEL_ONLY.
            if (
                not is_possesso_form
                and attribution in ("LOT_LEVEL_ONLY", "LOT_LEVEL_ONLY_PRE_BENE_CONTEXT")
                and _has_forward_bene_heading(lines, line_idx)
            ):
                _emit_blocked(
                    "OCCUPANCY_BENE_HEADED_BLOCK_NOT_PROMOTABLE",
                    page, line_idx, line_s,
                    _normalize(_join_window(lines, line_idx, 6)),
                    "BODY_TEXT",
                    scope_info["lot_id"], scope_info["bene_id"],
                    reason=(
                        "Occupancy trigger is a bare section heading; immediately "
                        "following lines identify a specific Bene N°. Evidence is "
                        "bene-scoped and cannot safely be promoted to LOT_LEVEL_ONLY."
                    ),
                )
                continue

            # Build forward window.  Possesso sections embed narrative evidence
            # several lines after the heading, so use a wider window (12 lines)
            # to reach the relevant sentences.
            window_size = 12 if is_possesso_form else 6
            window_text = _normalize(_join_window(lines, line_idx, window=window_size))

            # -----------------------------------------------------------------
            # Try patterns in order; first match wins.
            # For the possesso form, use dedicated patterns (E / F / G) that
            # are designed for the inspection-narrative layout.
            # For the occupazione form, use the direct extraction patterns
            # (A → B → C → D).
            # -----------------------------------------------------------------
            status_raw: Optional[str] = None
            title_raw_override: Optional[str] = None
            extraction_method: Optional[str] = None

            if is_possesso_form:
                # Pattern E — tenant/conduttore found in narrative → "locato"
                m_cond = OCC_PAT_POSSESSO_CONDUTTORE.search(window_text)
                # Pattern F — lease/rental contract title in narrative
                m_title = OCC_PAT_POSSESSO_TITLE.search(window_text)
                # Pattern G — comodato mention in narrative
                m_com = OCC_PAT_POSSESSO_COMODATO.search(window_text)

                if m_cond:
                    status_raw = "locato"
                    extraction_method = "OCC_POSSESSO_CONDUTTORE"
                    if m_title:
                        title_raw_override = _normalize(m_title.group(1))
                elif m_title:
                    # Contract title found even without explicit role word
                    status_raw = "locato"
                    extraction_method = "OCC_POSSESSO_AFFITTO"
                    title_raw_override = _normalize(m_title.group(1))
                elif m_com:
                    status_raw = "occupato in comodato"
                    extraction_method = "OCC_POSSESSO_COMODATO"
                    title_raw_override = _normalize(m_com.group(1))
                # If none of E/F/G match, fall through to the standard patterns
                # below (A→D) — this handles possesso sections that still state
                # "Stato di occupazione: libero" within the narrative block.

            if status_raw is None:
                # Pattern A: inline label "Stato di occupazione: <value>"
                m = OCC_PAT_LABEL.search(window_text)
                if m:
                    status_raw = _normalize(m.group(1))
                    # Strip any trailing "Allegato" reference that leaked
                    status_raw = re.sub(r"\s+Allegato\b.*$", "", status_raw, flags=re.IGNORECASE).strip()
                    extraction_method = "OCC_LABEL_INLINE"

            if status_raw is None:
                # Pattern B: "L'immobile risulta / è <status>"
                m = OCC_PAT_RISULTA.search(window_text)
                if m:
                    status_raw = _normalize(m.group(1))
                    extraction_method = "OCC_RISULTA"

            if status_raw is None:
                # Pattern C: "Occupato da ... [role]" (section-header form)
                m = OCC_PAT_OCCUPATO_DA.search(window_text)
                if m:
                    status_raw = _normalize(m.group(1))
                    extraction_method = "OCC_OCCUPATO_DA"

            if status_raw is None:
                # Pattern D: standalone "Libero" (section-header + single-word value)
                m = OCC_PAT_STANDALONE_LIBERO.search(window_text)
                if m:
                    status_raw = _normalize(m.group(1))
                    extraction_method = "OCC_STANDALONE_LIBERO"

            if status_raw is None:
                # Pattern D2: "bene/immobile ... risulta ... libero" (flexible gap)
                # Handles Unicode apostrophe and intervening adverbs
                m = OCC_PAT_RISULTA_LIBERO.search(window_text)
                if m:
                    status_raw = _normalize(m.group(2))
                    extraction_method = "OCC_RISULTA_LIBERO"

            # Normalise OCR redaction-marker variants so that
            # "** ** Omissis ****" ≡ "**** Omissis ****" for dedup purposes.
            if status_raw:
                status_raw = _normalize_redaction(status_raw)

            if status_raw is None or not status_raw:
                # No extractable value found; log as warning and skip
                out["warnings"].append(
                    f"OCC_NO_VALUE_EXTRACTED: p{page}:l{line_idx} — "
                    f"trigger matched but no status value extracted. "
                    f"Window: {window_text[:120]}"
                )
                continue

            # Sanity guard: reject values that are clearly procedural despite pattern match
            if re.search(
                r"\b(?:riduzione|parametro|mercato|intimato|sopralluogo|liberazione\s+dell)\b",
                status_raw,
                re.IGNORECASE,
            ):
                _emit_blocked(
                    "OCCUPANCY_PROCEDURAL_PROSE_EXCLUDED",
                    page, line_idx, line_s,
                    window_text[:300],
                    extraction_method or "BODY_TEXT",
                    scope_info["lot_id"], scope_info["bene_id"],
                    reason=(
                        "Extracted status_raw contains procedural vocabulary; "
                        "rejected after pattern match."
                    ),
                    extra={"extracted_status_raw": status_raw},
                )
                continue

            # Extract optional title_raw.  For possesso-form matches the title
            # was already pulled from the window into title_raw_override.
            # For occupazione-form matches, derive it from the status string.
            if title_raw_override is not None:
                title_raw = title_raw_override
            else:
                title_raw = _extract_title_raw(status_raw)

            # Build field list (status always; title if found; nuance signals if present)
            fields: List[Tuple[str, str]] = [("occupancy_status_raw", status_raw)]
            if title_raw:
                fields.append(("occupancy_title_raw", title_raw))

            # Detect occupancy nuance signals from the window text.
            # These enrich the candidate without altering the primary status.
            if _OCC_NON_OPPONIBILE_PAT.search(window_text):
                fields.append(("occupancy_opponibilita_raw", "NON_OPPONIBILE"))
            elif re.search(r"\bopponibil[ei]\b", window_text, re.IGNORECASE):
                # Opponibile without "non" prefix → explicitly opponibile
                fields.append(("occupancy_opponibilita_raw", "OPPONIBILE"))

            if _OCC_SALTUARIA_PAT.search(window_text):
                fields.append(("occupancy_saltuaria_raw", "saltuaria_occupazione_esecutato"))

            if _OCC_LIBERAZIONE_PROCEDURA_PAT.search(window_text):
                fields.append(("occupancy_liberazione_raw", "a_carico_procedura"))
            elif _OCC_LIBERAZIONE_ACQUIRENTE_PAT.search(window_text):
                fields.append(("occupancy_liberazione_raw", "a_carico_acquirente"))

            quote = line_s[:300]
            ctx_win = window_text[:400]

            if is_blocked:
                extra_b = {ft: fv for ft, fv in fields}
                _emit_blocked(
                    attribution,
                    page, line_idx, line_s, window_text[:300],
                    extraction_method,
                    scope_info["lot_id"], scope_info["bene_id"],
                    reason=f"Occupancy match found but scope attribution is {attribution}.",
                    extra=extra_b,
                )
                continue

            # -----------------------------------------------------------------
            # Local header consistency guard
            # -----------------------------------------------------------------
            local_lot_id, local_bene_id = _nearest_local_headers(lines, line_idx)
            attributed_lot = (scope_info.get("lot_id") or "").lower()
            attributed_bene = scope_info.get("bene_id")
            mismatch_reason: Optional[str] = None

            if local_lot_id is not None and local_lot_id != attributed_lot:
                mismatch_reason = (
                    f"Local explicit lot header 'LOTTO {local_lot_id.upper()}' "
                    f"disagrees with attributed lot_id '{scope_info.get('lot_id')}'."
                )
            elif (
                local_bene_id is not None
                and attributed_bene is not None
                and local_bene_id != str(attributed_bene)
            ):
                mismatch_reason = (
                    f"Local explicit bene header 'Bene N° {local_bene_id}' "
                    f"disagrees with attributed bene_id '{attributed_bene}'."
                )

            if mismatch_reason:
                extra_mm: Dict = {
                    "local_lot_id": local_lot_id,
                    "local_bene_id": local_bene_id,
                    "attributed_lot_id": scope_info.get("lot_id"),
                    "attributed_bene_id": str(attributed_bene)
                    if attributed_bene is not None
                    else None,
                }
                extra_mm.update({ft: fv for ft, fv in fields})
                _emit_blocked(
                    "OCCUPANCY_LOCAL_SCOPE_HEADER_MISMATCH",
                    page, line_idx, line_s, window_text[:300],
                    extraction_method,
                    scope_info["lot_id"], scope_info["bene_id"],
                    reason=mismatch_reason,
                    extra=extra_mm,
                )
                continue

            # -----------------------------------------------------------------
            # Wider bene-block guard
            # -----------------------------------------------------------------
            # The 12-line lookback may not reach a bene section header that starts
            # a long schema table row (e.g. "Bene N° 4 - Villetta" followed by
            # 21 lines of description ending with "Stato di occupazione: ...").
            # Use a 30-line lookback with a line-start-anchored pattern so that
            # inline prose references ("il bene n. 3") are not mistaken for
            # section headers.
            # If a bene section header is found and the attribution is lot-level,
            # this candidate is inside a bene block and must not be promoted.
            if (
                local_bene_id is None
                and attribution in ("LOT_LEVEL_ONLY", "LOT_LEVEL_ONLY_PRE_BENE_CONTEXT")
            ):
                wider_bene_id = _nearest_bene_section_header(lines, line_idx)
                if wider_bene_id is not None:
                    _emit_blocked(
                        "OCCUPANCY_BENE_HEADED_BLOCK_NOT_PROMOTABLE",
                        page, line_idx, line_s, window_text[:300],
                        extraction_method,
                        scope_info["lot_id"], scope_info["bene_id"],
                        reason=(
                            f"Occupancy candidate is inside 'Bene N° {wider_bene_id}' "
                            f"section (found in 30-line backward window); "
                            "cannot safely promote to LOT_LEVEL_ONLY."
                        ),
                        extra={
                            "wider_local_bene_id": wider_bene_id,
                            **{ft: fv for ft, fv in fields},
                        },
                    )
                    continue

            # -----------------------------------------------------------------
            # Forward lot-transition boundary guard
            # -----------------------------------------------------------------
            # An inline occupancy statement that appears immediately before a new
            # LOTTO section header is at a lot-boundary transition.  The page-range
            # scope may assign it to the next lot while it actually belongs to the
            # tail of the previous lot's entry.  Block it to prevent misattribution.
            if _has_forward_lot_transition(lines, line_idx):
                _emit_blocked(
                    "OCCUPANCY_LOT_TRANSITION_BOUNDARY",
                    page, line_idx, line_s, window_text[:300],
                    extraction_method,
                    scope_info["lot_id"], scope_info["bene_id"],
                    reason=(
                        "Occupancy candidate appears immediately before a new LOTTO "
                        "section header; treated as lot-boundary transition text, "
                        f"not safe evidence for attributed lot "
                        f"'{scope_info.get('lot_id')}'."
                    ),
                    extra={ft: fv for ft, fv in fields},
                )
                continue

            scope_basis = (
                f"Occupancy trigger on p{page}:l{line_idx}; "
                f"scope attribution: {attribution}; "
                f"method: {extraction_method}."
            )
            _emit_active(
                fields, page, line_idx, quote, ctx_win,
                extraction_method,
                scope_info["lot_id"], scope_info["bene_id"],
                scope_info["composite_key"],
                attribution, scope_basis,
            )

    # -----------------------------------------------------------------------
    # Lot-level rollup: when every declared bene under a lot has the same
    # explicit occupancy status, emit a lot-level deterministic candidate.
    # This preserves the existing bare-section guard: lot evidence is derived
    # only from complete, uniform bene evidence.
    # -----------------------------------------------------------------------

    active_bene_status: Dict[str, Dict[str, List[Dict]]] = {}
    for cand in candidates:
        if cand.get("candidate_status") != "ACTIVE":
            continue
        if cand.get("field_type") != "occupancy_status_raw":
            continue
        if not cand.get("lot_id") or not cand.get("bene_id"):
            continue
        lot_key = str(cand["lot_id"])
        bene_key = str(cand["bene_id"])
        active_bene_status.setdefault(lot_key, {}).setdefault(bene_key, []).append(cand)

    for lot_id, bene_scopes in sorted(lut["bene_by_lot"].items()):
        if not bene_scopes:
            continue
        declared_bene_ids = [str(bs.get("bene_id")) for bs in bene_scopes]
        per_bene = active_bene_status.get(str(lot_id), {})
        if any(bid not in per_bene for bid in declared_bene_ids):
            continue

        bene_values: Dict[str, str] = {}
        representative: Optional[Dict] = None
        complete = True
        for bene_id in declared_bene_ids:
            vals = {
                _material_occupancy_key(str(c.get("extracted_value", "")))
                for c in per_bene.get(bene_id, [])
                if c.get("extracted_value")
            }
            if len(vals) != 1:
                complete = False
                break
            chosen = next(iter(vals))
            bene_values[bene_id] = chosen
            if representative is None:
                representative = per_bene[bene_id][0]

        if not complete or len(set(bene_values.values())) != 1 or representative is None:
            continue

        status_raw = str(representative.get("extracted_value", "")).strip()
        evidence_pages = sorted({
            int(c["page"])
            for bid in declared_bene_ids
            for c in per_bene[bid]
            if isinstance(c.get("page"), int)
        })
        evidence_line = int(representative.get("line_index", 0))
        evidence_page = int(representative.get("page", 0))
        scope_basis = (
            "Uniform occupancy rollup across declared bene scopes "
            f"for lot:{lot_id}: {', '.join(declared_bene_ids)}; "
            f"evidence pages: {', '.join(str(p) for p in evidence_pages)}."
        )
        _emit_active(
            [("occupancy_status_raw", status_raw)],
            evidence_page,
            evidence_line,
            f"Uniform bene occupancy rollup: {status_raw}",
            scope_basis,
            "OCC_BENE_UNIFORM_ROLLUP",
            str(lot_id),
            None,
            None,
            "CONFIRMED",
            scope_basis,
        )

    # -----------------------------------------------------------------------
    # Post-processing: intra-scope conflict detection
    # -----------------------------------------------------------------------

    def _attr_bucket(attr: str) -> str:
        if attr in ("CONFIRMED", "ATTRIBUTED_BY_SCOPE"):
            return attr
        if attr in {"LOT_LEVEL_ONLY", "LOT_LEVEL_ONLY_PRE_BENE_CONTEXT"}:
            return "LOT_LEVEL"
        return attr

    grouped: Dict[Tuple, List[Dict]] = {}
    for cand in candidates:
        scope_key = cand.get("composite_key") or f"lot:{cand.get('lot_id', 'unknown')}"
        bucket = _attr_bucket(cand["attribution"])
        key = (scope_key, cand["field_type"], bucket)
        grouped.setdefault(key, []).append(cand)

    final_candidates: List[Dict] = []
    for (scope_key, field_type, bucket), group in sorted(grouped.items()):
        # Case-insensitive dedup: "Libero" and "libero" are the same value.
        distinct_vals = sorted({str(c["extracted_value"]).strip().lower() for c in group})
        if len(distinct_vals) > 1:
            blocked_or_ambiguous.append({
                "type": "OCCUPANCY_MULTI_VALUE_UNRESOLVED",
                "scope_key": scope_key,
                "field_type": field_type,
                "attribution_bucket": bucket,
                "distinct_values": distinct_vals,
                "candidate_count": len(group),
                "candidates": [
                    {k: c.get(k) for k in (
                        "candidate_id", "extracted_value", "page",
                        "line_index", "attribution", "source_type",
                    )}
                    for c in group
                ],
                "reason": (
                    f"Multiple distinct {field_type} values for scope {scope_key}; "
                    "no synthesis applied."
                ),
            })
            # Still include all candidates in the output for traceability,
            # but mark them CONFLICT so the ledger can identify them.
            for c in group:
                c = dict(c)
                c["candidate_status"] = "CONFLICT"
                final_candidates.append(c)
        else:
            final_candidates.extend(group)

    final_candidates.sort(
        key=lambda c: (c.get("page", 0), c.get("line_index", 0), c.get("field_type", ""))
    )

    out["candidates"] = final_candidates
    out["blocked_or_ambiguous"] = blocked_or_ambiguous
    out["coverage"]["candidate_count"] = len(final_candidates)
    out["coverage"]["blocked_or_ambiguous_count"] = len(blocked_or_ambiguous)
    out["coverage"]["occupancy_fields_present"] = sorted(
        {c["field_type"] for c in final_candidates if c.get("candidate_status") == "ACTIVE"}
    )
    out["coverage"]["occupancy_scope_keys"] = sorted(
        {
            c.get("composite_key") or f"lot:{c.get('lot_id', 'unknown')}"
            for c in final_candidates
            if c.get("candidate_status") == "ACTIVE"
        }
    )

    # Summary per scope key
    summary: Dict[str, Dict] = {}
    for c in final_candidates:
        if c.get("candidate_status") != "ACTIVE":
            continue
        sk = c.get("composite_key") or f"lot:{c.get('lot_id', 'unknown')}"
        if sk not in summary:
            summary[sk] = {
                "lot_id": c.get("lot_id"),
                "bene_id": c.get("bene_id"),
                "composite_key": c.get("composite_key"),
                "fields": {},
            }
        ft = c["field_type"]
        summary[sk]["fields"].setdefault(ft, [])
        val = c["extracted_value"]
        if val not in summary[sk]["fields"][ft]:
            summary[sk]["fields"][ft].append(val)

    out["summary"] = summary

    dst = ctx.artifact_dir / "occupancy_candidate_pack.json"
    dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Harvest occupancy field candidates")
    parser.add_argument("--case", required=True, choices=list_case_keys())
    args = parser.parse_args()

    out = build_occupancy_candidate_pack(args.case)
    print(json.dumps({
        "case_key": out["case_key"],
        "status": out["status"],
        "winner": out["winner"],
        "coverage": out["coverage"],
        "blocked_or_ambiguous_count": len(out["blocked_or_ambiguous"]),
        "warnings": out["warnings"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
