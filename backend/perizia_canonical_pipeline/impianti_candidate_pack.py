"""
Impianti / Utilities bounded field shell — candidate harvesting only.

Produces: impianti_candidate_pack.json

Per-system status field types:
  impianto_elettrico_status       — electrical system
  impianto_idrico_status          — water/plumbing system
  impianto_gas_status             — gas supply system
  impianto_fognario_status        — sewer / drainage system
  impianto_riscaldamento_status   — heating system
  impianto_climatizzazione_status — cooling / climate system

Optional per-system status (emitted only if clearly supported):
  impianto_ascensore_status       — lift / montacarichi / elevator

Context-only field types (extracted_value = normalized label or None):
  impianti_conformita_context       — conformità certificates absent / not produced
  impianti_non_verificati_context   — collective non-verified / not inspectable statement
  allacci_presenza_context          — grid connection / allacciamento statement
  utenze_attive_context             — utenze active / not active / distaccate

What is NOT built here:
  - final habitability score
  - legal / risk synthesis
  - LLM conflict resolution
  - doc_map freeze
  - full building-code compliance engine

Critical modeling rules enforced:
  - "presente" ≠ "conforme"
  - "esistente" ≠ "funzionante"
  - "non verificato" ≠ "assente"
  - Boilerplate conformità prose ≠ explicit system-level status
  - Collective "impianti" statements → context-only; never per-system certainty
  - RECAP_SUMMARY_TABLE / ARITHMETIC_ROLLUP_TABLE → conservative handling
  - METHODOLOGY_COMPARABLE_TABLE → excluded from active truth
  - Non-utility "impianto" usages (urbano, catastale, originario) → excluded
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
# Basic helpers
# ---------------------------------------------------------------------------

def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _make_context_window(lines: List[str], idx: int, back: int = 4, fwd: int = 3) -> str:
    start = max(0, idx - back)
    end = min(len(lines), idx + fwd + 1)
    parts = [ln.strip() for ln in lines[start:end] if ln.strip()]
    return " | ".join(parts)


# Pattern that signals the start of a new per-system impianti line.
# Used to truncate the forward-join window to avoid cross-system contamination.
_IMPIANTI_SYSTEM_LINE_PAT = re.compile(
    r"impianto\s+(?:elettri|idrico|(?:del\s+)?gas|fognario|(?:di\s+)?riscaldamento"
    r"|termico|(?:di\s+)?climatizzazione|(?:elevatore|ascensore|montacarichi))",
    re.IGNORECASE,
)


def _join_forward(lines: List[str], idx: int, count: int = 3) -> str:
    """
    Join the trigger line with up to `count` subsequent non-blank lines,
    stopping early if a new per-system impianti trigger is detected.
    This prevents cross-system status contamination in bullet-list formats.
    """
    parts: List[str] = []
    first = True
    for ln in lines[idx : idx + count + 1]:
        stripped = ln.strip()
        if not stripped:
            continue
        if first:
            parts.append(stripped)
            first = False
            continue
        # Stop if this continuation line starts a different system description
        if _IMPIANTI_SYSTEM_LINE_PAT.search(stripped):
            break
        parts.append(stripped)
    return " ".join(parts)


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
# Non-utility "impianto" exclusion
# Lines that mention "impianto" but in urban / cadastral / structural context.
# These must not trigger per-system utility harvesting.
# ---------------------------------------------------------------------------

_NON_UTILITY_IMPIANTO_PAT = re.compile(
    r"(?:"
    r"\bimpianto\s+urbano\b"
    r"|\bimpianto\s+originario\b"
    r"|\bimpianto\s+(?:del\s+(?:nuovo\s+)?|catastale\s+)catasto\b"
    r"|\bimpianto\s+del\s+tessuto\b"
    r"|\bmappa\s+di\s+impianto\b"
    r"|\bepoca\s+dell[\W\x27\u2018\u2019]impianto\b"
    r"|\bimpianto\s+(?:di\s+)?trasmissione\b"
    r"|\bimpianto\s+difensivo\b"
    r"|\bimpianto\s+di\s+sostituzione\b"
    r"|\bimpianto\s+radiotelevisivo\b"  # aerial, not utility
    r"|\bnuovo\s+catasto\s+sin\s+dall[\W\x27\u2018\u2019]impianto\b"
    r")",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Table-of-contents dot-leader lines
# ---------------------------------------------------------------------------

_DOT_LEADER_PAT = re.compile(r"\.{5,}|(?:\s*\.\s*){6,}")

# ---------------------------------------------------------------------------
# Valuation / methodology prose exclusion
# Lines that discuss impianti only in a valuation/deprezzamento context.
# ---------------------------------------------------------------------------

_VALUATION_PROSE_PAT = re.compile(
    r"(?:"
    r"\bdeprezzamento\s+del\s+\d"
    r"|\bvalore\s+(?:di\s+stima|venale|commerciale|di\s+mercato)\b"
    r"|\bOMI\b.*quotazion"
    r"|\bquotazioni\s+OMI\b"
    r"|\bprezzo\s+base\s+d['.'\u2018\u2019]asta\b"
    r"|\bcoefficiente\s+di\s+vetust"
    r"|\bvalore\s+(?:medio|unitario)\s+di\s+mercato\s+di\s+€"
    r"|\bla\s+dottrina\s+estimale\b"
    r"|\bpi[uù]\s+probabile\s+valore\s+di\b"
    r"|\bin\s+condizioni\s+di\s+libero\s+mercato\b"
    r"|\bha\s+condotto\s+alla\s+determinazione\b"
    r"|\bstato\s+di\s+manutenzione\s+mediocre\b"  # general condition, not system-level
    r"|\bla\s+mancanza\s+di\s+adeguati\s+impianti\b"  # deprezzamento context
    r")",
    re.IGNORECASE,
)

# Property description / marketing prose — too vague
_GENERIC_DESC_PAT = re.compile(
    r"(?:"
    r"\bimpianti\s+inalterati\s+rispetto\s+all[\W\x27\u2018\u2019]epoca\s+di\s+costruzione\b"
    r"|\bimpianti\s+in\s+quanto\s+non\s+vi\s+sono\s+elementi\b"
    r"|\bsebbene\s+conservino\s+gli\s+elementi\s+costruttivi\b"
    r"|\binfissi,\s+impianti\b(?!.*\bpresente\b)(?!.*\bassente\b)"  # listing without status
    r")",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# RECAP page detection
# ---------------------------------------------------------------------------

_RECAP_ZONE_PAT = re.compile(
    r"RIEPILOGO\s+BANDO\s+D[\x27\u2018\u2019\W]ASTA",
    re.IGNORECASE,
)

_SCHEMA_RIASSUNTIVO_PAT = re.compile(r"\bSCHEMA\s+RIASSUNTIVO\b", re.IGNORECASE)


def _find_schema_pages(raw_pages: List[Dict]) -> Set[int]:
    pages: Set[int] = set()
    for pd in raw_pages:
        pn = int(pd["page_number"])
        text = pd.get("text", "") or ""
        if _SCHEMA_RIASSUNTIVO_PAT.search(text):
            pages.add(pn)
            pages.add(pn + 1)
            pages.add(pn + 2)
    return pages


def _find_recap_pages(raw_pages: List[Dict], winner: str) -> Set[int]:
    if winner not in ("H2_EXPLICIT_MULTI_LOT", "H4_CANDIDATE_MULTI_LOT_MULTI_BENE"):
        return set()
    recap: Set[int] = set()
    for pd in raw_pages:
        pn = int(pd["page_number"])
        text = pd.get("text", "") or ""
        first_lines = "\n".join(text.split("\n")[:10])
        if _RECAP_ZONE_PAT.search(first_lines):
            recap.add(pn)
    return recap


# ---------------------------------------------------------------------------
# Table zone index
# ---------------------------------------------------------------------------

def _build_table_zone_index(table_zone_map: Dict) -> Dict[int, List[Dict]]:
    """Index table zones by page → list of zone dicts."""
    idx: Dict[int, List[Dict]] = {}
    for z in (table_zone_map.get("table_zones") or []):
        page = int(z.get("page", -1))
        idx.setdefault(page, []).append(z)
    return idx


def _get_zone_type_for_line(
    page: int,
    line_idx: int,
    zone_index: Dict[int, List[Dict]],
) -> Optional[str]:
    """Return the zone_type of the table zone containing (page, line_idx), or None."""
    for zone in zone_index.get(page, []):
        start = int(zone.get("start_line_index", -1))
        end = int(zone.get("end_line_index", -1))
        if start <= line_idx <= end:
            return zone.get("zone_type")
    return None


# ---------------------------------------------------------------------------
# Per-system trigger patterns
# Each maps field_type → compiled regex detecting that system in a line.
# Patterns handle common OCR splits (e.g. "elettri co", "elettri\n").
# ---------------------------------------------------------------------------

_SYSTEM_PATS: Dict[str, re.Pattern] = {
    "impianto_elettrico_status": re.compile(
        r"impianto\s+elettri(?:co)?",
        re.IGNORECASE,
    ),
    "impianto_idrico_status": re.compile(
        r"impianto\s+idrico",
        re.IGNORECASE,
    ),
    "impianto_gas_status": re.compile(
        r"impianto\s+(?:(?:di\s+)?distribuzione\s+)?(?:del\s+)?gas"
        r"|impianto\s+a\s+gas"
        r"|utenza\s+gas\s+domestico"
        r"|impianto\s+metano",
        re.IGNORECASE,
    ),
    "impianto_fognario_status": re.compile(
        r"impianto\s+fognario"
        r"|rete\s+fognari",
        re.IGNORECASE,
    ),
    "impianto_riscaldamento_status": re.compile(
        r"impianto\s+(?:di\s+)?riscaldamento"
        r"|impianto\s+termico",
        re.IGNORECASE,
    ),
    "impianto_climatizzazione_status": re.compile(
        r"impianto\s+(?:di\s+)?climatizzazione",
        re.IGNORECASE,
    ),
    "impianto_ascensore_status": re.compile(
        r"impianto\s+(?:elevatore|ascensore|montacarichi|elevat)",
        re.IGNORECASE,
    ),
}

# ---------------------------------------------------------------------------
# Status value normalization
# Applied to a joined text window (trigger line + up to 2 forward lines).
# Returns a normalized label, or None if status is not determinable.
# ---------------------------------------------------------------------------

# Ordered: most specific first. Each (pattern, value) tuple.
_STATUS_RULES: List[Tuple[re.Pattern, str]] = [
    # "non funzionante" / "non funziona" must come before positive "funzionante"
    (re.compile(r"\bnon\s+funzionant", re.IGNORECASE), "non_funzionante"),
    (re.compile(r"\bnon\s+risulta\s+funzionant", re.IGNORECASE), "non_funzionante"),
    (re.compile(r"\bnon\s+(?:è\s+)?(?:più\s+)?(?:in\s+)?funzion", re.IGNORECASE), "non_funzionante"),

    # "non presente" / "assente" / "non esiste" / "mancante" — must precede "presente"
    (re.compile(r"\bnon\s+(?:è\s+)?presente\b", re.IGNORECASE), "assente"),
    (re.compile(r"\bnon\s+esiste\b(?!\s+la\s+dichiarazione)", re.IGNORECASE), "assente"),
    (re.compile(r"\bassente\b", re.IGNORECASE), "assente"),
    (re.compile(r"\bmancante\b", re.IGNORECASE), "assente"),
    (re.compile(r"\bnon\s+present[eo]\b", re.IGNORECASE), "assente"),

    # disallacciato / distaccato / sigillo
    (re.compile(r"\bdisallacciato\b", re.IGNORECASE), "disallacciato"),
    (re.compile(r"\bdisattivato\b", re.IGNORECASE), "disallacciato"),
    (re.compile(r"\bcon\s+sigillo\s+della\s+societ", re.IGNORECASE), "disallacciato"),

    # non conforme / non a norma
    (re.compile(r"\bnon\s+(?:a\s+norma|conform[ei]|rispondente)\b", re.IGNORECASE), "non_conforme"),
    (re.compile(r"\btotale\s+mancanza\s+di\s+ogni\s+e\s+qualsiasi\s+requisito\s+di\s+sicurezza\b",
                re.IGNORECASE), "non_conforme"),

    # da adeguare / da mettere a norma
    (re.compile(r"\bda\s+adeguare\b", re.IGNORECASE), "da_adeguare"),
    (re.compile(r"\bda\s+mettere\s+a\s+norma\b", re.IGNORECASE), "da_adeguare"),
    (re.compile(r"\binadeguato\b", re.IGNORECASE), "da_adeguare"),

    # non verificato / non accertato
    (re.compile(r"\bnon\s+(?:è\s+stato\s+)?(?:possibile\s+)?verific", re.IGNORECASE), "non_verificato"),
    (re.compile(r"\bnon\s+(?:è\s+stato\s+)?accertat", re.IGNORECASE), "non_verificato"),
    (re.compile(r"\bnon\s+verificat", re.IGNORECASE), "non_verificato"),

    # funzionante (positive, after non_funzionante check)
    (re.compile(r"\bfunzionant[ei]\b", re.IGNORECASE), "funzionante"),

    # presente / alimentato / sottotraccia / in dotazione / servito da
    (re.compile(r"\bpresente\b", re.IGNORECASE), "presente"),
    (re.compile(r"\balimentato\s+da\b", re.IGNORECASE), "presente"),
    (re.compile(r"\bsottotraccia\b", re.IGNORECASE), "presente"),
    (re.compile(r"\bin\s+dotazione\b", re.IGNORECASE), "presente"),
    (re.compile(r"\bgenerato\s+da\b", re.IGNORECASE), "presente"),   # "generato da una caldaia"
    (re.compile(r"\bè\s+(?:il\s+)?(?:un\s+)?(?:autonomo|centralizzato)\b", re.IGNORECASE), "presente"),

    # autonomo / centralizzato (heating-specific)
    (re.compile(r"\bautono(?:mo|ma)\b", re.IGNORECASE), "autonomo"),
    (re.compile(r"\bcentralizzato\b", re.IGNORECASE), "centralizzato"),
]


# Guard: "non esiste la dichiarazione" variants (incl. OCR splits like "dichiarazio ne")
# This text is about certificate absence, NOT system absence.
_DICHIARAZIONE_CONFORMITA_PAT = re.compile(
    r"non\s+esiste\s+la\s+dichiaraz",
    re.IGNORECASE,
)

# Guard: "senza certificazione" in impianti context
_SENZA_CERTIF_PAT = re.compile(
    r"\bsenza\s+certificazione\b",
    re.IGNORECASE,
)

# Guard: generic "non esiste" that refers to certificates / conformità docs
_CERT_CONTEXT_PAT = re.compile(
    r"(?:dichiarazione|certificazione|certificato)\s+di\s+conformit",
    re.IGNORECASE,
)


def _extract_status_from_window(window_text: str) -> Optional[str]:
    """
    Apply status rules to a joined window text.
    Returns the first matching normalized value, or None.

    Guards:
    - If the window is about certificate absence ("non esiste la dichiarazione"),
      return None rather than "assente" (certificate absence ≠ system absence).
    - If the window is "senza certificazione" without functional status, return None.
    """
    # Conformità-absence guard: these phrases are about missing certificates, not
    # about the system being absent or non-functional. Returning None causes the
    # per-system classifier to emit a CONTEXT_ONLY entry rather than fake "assente".
    if _DICHIARAZIONE_CONFORMITA_PAT.search(window_text):
        return None
    if _SENZA_CERTIF_PAT.search(window_text) and _CERT_CONTEXT_PAT.search(window_text):
        return None

    for pat, val in _STATUS_RULES:
        if pat.search(window_text):
            return val
    return None


# ---------------------------------------------------------------------------
# Conformità context patterns (context-only; no per-system status)
# ---------------------------------------------------------------------------

# "Non esiste la dichiarazione di conformità dell'impianto elettrico/termico/idrico"
_CONFORMITA_ABSENT_PAT = re.compile(
    r"(?:"
    r"non\s+esiste\s+la\s+dichiarazione\s+di\s+conformit"
    r"|dichiarazione\s+di\s+conformit[aà]\s+(?:dell[\W\x27\u2018\u2019]impianto\s+\w+\s+)?(?:non\s+)?(?:prodott|repert|present)"
    r"|senza\s+certificazione\b(?!\s+energetica)"  # "senza certificazione" in impianti section
    r"|certificazione\s+di\s+conformit[aà]\s+(?:assente|non\s+(?:repert|prodott|present))"
    r"|non\s+è\s+stata\s+(?:prodott|repert)a\s+la\s+certificazione"
    r"|conformit[aà]\s+tecnica\s+impiantistica\s*:"  # section header
    r")",
    re.IGNORECASE,
)

# "conformità degli impianti non verificata" / "non è stato possibile verificare gli impianti"
_IMPIANTI_NON_VERIF_PAT = re.compile(
    r"(?:"
    r"conformit[aà]\s+degli\s+impianti\s+non\s+verificat"
    r"|non\s+[èe]\s+stato\s+possibile\s+verific(?:are\s+(?:la\s+)?(?:lo\s+stato|gli\s+impianti|il\s+funzionamento)|gli\s+impianti)"
    r"|impianti\s+presenti\s+ma\s+non\s+(?:è\s+stata\s+)?verific"
    r"|impianti\s+tecnologici\s+presenti\s+ma\s+non\s+verificat"
    r")",
    re.IGNORECASE,
)

# Collective allacci / reti tecnologiche / allacciamento context
_ALLACCI_PAT = re.compile(
    r"(?:"
    r"servit[oa]\s+dall[aei]\s+rete\s+tecnologic"
    r"|rete\s+tecnologic"
    r"|servizi?\s+di\s+(?:acqua|acquedotto|fognatura|gas|energia)"
    r"|allacciamento\s+alla\s+fognatura"
    r"|allacciamento\s+alla\s+rete"
    r"|allacciato\s+(?:alla\s+rete|all[\W\x27\u2018\u2019]acquedotto|alla\s+fognatura)"
    r"|non\s+allacciato\b"
    r")",
    re.IGNORECASE,
)

# Utenze attive / non attive / distaccate
_UTENZE_PAT = re.compile(
    r"(?:"
    r"\butenze\s+(?:attive|non\s+attive|distaccate|sospese|non\s+funzionanti)\b"
    r"|\bcontatori?\s+del\s+gas\s+[eèé]\s+in\s+comune\b"
    r"|\bcontatori?\s+(?:distaccati?|sospesi?|non\s+attivi?)\b"
    r")",
    re.IGNORECASE,
)

# Grouped utilities absence — explicit "no technological systems present" or
# "no services detected" statements that do not name any specific system.
# These are context-only evidence: they confirm total absence at lot level but
# must NOT be exploded into fake per-system certainty.
# Maps to allacci_presenza_context with label "assenza_totale_impianti_e_servizi".
_GROUPED_UTILITIES_ABSENCE_PAT = re.compile(
    r"(?:"
    r"\bnon\s+sono\s+presenti\s+impianti\s+tecnologici\b"
    r"|\bnon\s+sono\s+stati\s+rilevati\s+servizi\s+di\s+(?:alcun|nessun)\s+tipo\b"
    r"|\bnessun\s+(?:impianto|servizio)\s+tecnologico\s+(?:[èe]\s+)?(?:presente|rilevato)\b"
    r"|\bassenza\s+(?:totale\s+)?(?:di\s+)?(?:impianti\s+tecnologici|servizi\s+tecnologici)\b"
    r")",
    re.IGNORECASE,
)

# Collective impianti non conformi (no per-system specificity)
_IMPIANTI_NON_CONFORMI_PAT = re.compile(
    r"(?:"
    r"\bimpianti\s+non\s+(?:a\s+norma|conform[ei])\s+(?:alle|alla)\s+normativ"
    r"|\bimpianti\s+non\s+rispondenti\b"
    r"|\bnecessit[aà]\s+di\s+adeguamento\s+degli\s+impianti\b"
    r"|\badeguamento\s+degli\s+impianti\s+alle\s+leggi\s+vigenti\b"
    r")",
    re.IGNORECASE,
)

# Conformità section header "8.5. ALTRE CONFORMITÀ:" / "Certificazioni energetiche e dichiarazioni"
_CONFORMITA_SECTION_HEADER_PAT = re.compile(
    r"(?:"
    r"certificazioni\s+energetiche\s+e\s+dichiarazioni\s+di\s+conformit"
    r"|dichiarazioni?\s+di\s+conformit[aà]"
    r")",
    re.IGNORECASE,
)

# Whether the line mentions any specific system (used to sub-classify conformità context)
_ANY_SYSTEM_PAT = re.compile(
    r"(?:impianto\s+elettri|impianto\s+termico|impianto\s+idrico|impianto\s+fognario"
    r"|impianto\s+(?:di\s+)?riscaldamento|impianto\s+(?:di\s+)?climatizzazione"
    r"|impianto\s+(?:del\s+)?gas)",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Explicit lot / bene label lookups (multi-structure context override)
# ---------------------------------------------------------------------------

_EXPLICIT_LOT_LABEL_PAT = re.compile(r"\bLOTTO\s+([A-Z0-9]+)\b", re.IGNORECASE)
_EXPLICIT_BENE_LABEL_PAT = re.compile(
    r"\bBENE\s+N[°o\.°]?\s*([0-9]+)\b",
    re.IGNORECASE,
)


def _find_local_lot_in_context(
    lines: List[str],
    trigger_idx: int,
    known_lot_ids: Set[str],
    window: int = 5,
) -> Optional[str]:
    start = max(0, trigger_idx - window)
    found: Optional[str] = None
    for line in lines[start:trigger_idx]:
        m = _EXPLICIT_LOT_LABEL_PAT.search(line)
        if m:
            candidate = m.group(1).strip().lower()
            if candidate in known_lot_ids:
                found = candidate
    return found


def _find_local_bene_in_context(
    lines: List[str],
    trigger_idx: int,
    known_bene_ids: Set[str],
    window: int = 10,
) -> Optional[str]:
    """
    Scan the preceding `window` lines for an explicit BENE N° X header.
    Returns the bene_id string if found and it is a known bene, else None.
    This enables bene attribution within pages where the last-bene fallback
    produced LOT_LEVEL_ONLY scope but explicit bene sub-sections are present.
    """
    start = max(0, trigger_idx - window)
    found: Optional[str] = None
    for line in lines[start:trigger_idx]:
        m = _EXPLICIT_BENE_LABEL_PAT.search(line)
        if m:
            candidate = str(m.group(1)).strip()
            if candidate in known_bene_ids:
                found = candidate
    return found


def _find_forward_lot_in_window(
    lines: List[str],
    trigger_idx: int,
    known_lot_ids: Set[str],
    window: int = 5,
) -> Optional[str]:
    end = min(len(lines), trigger_idx + window + 1)
    for line in lines[trigger_idx + 1 : end]:
        stripped = line.strip()
        if not stripped:
            continue
        m = _EXPLICIT_LOT_LABEL_PAT.search(stripped)
        if m:
            lot = m.group(1).strip().lower()
            if lot in known_lot_ids:
                return lot
    return None


def _build_lot_header_grade_lookup(plurality_headers: Dict) -> Dict[int, List[Dict]]:
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
    if winner not in ("H2_EXPLICIT_MULTI_LOT", "H4_CANDIDATE_MULTI_LOT_MULTI_BENE"):
        return None
    scope_lot_norm = str(page_scope_lot_id).strip().lower()
    for sig in hg_lookup.get(page, []):
        sig_lot = str(sig["value"]).strip().lower()
        sig_line = int(sig.get("line_index", 0))
        if sig_line < trigger_line_index and sig_lot != scope_lot_norm:
            return sig_lot
    return None


# ---------------------------------------------------------------------------
# Scope attribution → impianti block types
# ---------------------------------------------------------------------------

_CADAT_TO_IMPIANTI_BLOCK: Dict[str, str] = {
    "CADASTRAL_IN_GLOBAL_PRE_LOT_ZONE":      "IMPIANTI_IN_GLOBAL_PRE_LOT_ZONE",
    "CADASTRAL_IN_SAME_PAGE_LOT_COLLISION":  "IMPIANTI_IN_SAME_PAGE_LOT_COLLISION",
    "CADASTRAL_IN_SAME_PAGE_BENE_COLLISION": "IMPIANTI_IN_SAME_PAGE_BENE_COLLISION",
    "CADASTRAL_SCOPE_AMBIGUOUS":             "IMPIANTI_SCOPE_FIELD_CONFLICT",
    "CADASTRAL_IN_BLOCKED_UNREADABLE":       "IMPIANTI_SCOPE_FIELD_CONFLICT",
}

_IMPIANTI_SAFE_ATTRIBUTIONS = {
    "CONFIRMED",
    "ATTRIBUTED_BY_SCOPE",
    "LOT_LEVEL_ONLY",
    "LOT_LEVEL_ONLY_PRE_BENE_CONTEXT",
    "LOT_LOCAL_CONTEXT_OVERRIDE",
    "BENE_LOCAL_CONTEXT_OVERRIDE",
}

_CONTEXT_SAFE_ATTRIBUTIONS = {
    "CONFIRMED",
    "ATTRIBUTED_BY_SCOPE",
    "LOT_LEVEL_ONLY",
    "LOT_LOCAL_CONTEXT_OVERRIDE",
    "BENE_LOCAL_CONTEXT_OVERRIDE",
}


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def _classify_per_system(
    line: str,
    joined_window: str,
) -> Optional[Tuple[str, Optional[str]]]:
    """
    Try to classify the line as a per-system impianti candidate.

    Returns (field_type, extracted_value) or None.
    extracted_value may be None if the system is mentioned but status is unclear.
    """
    # Hard non-utility exclusions
    if _NON_UTILITY_IMPIANTO_PAT.search(line):
        return None
    if _VALUATION_PROSE_PAT.search(line):
        return None
    if _GENERIC_DESC_PAT.search(line):
        return None

    # Try each system pattern
    for field_type, pat in _SYSTEM_PATS.items():
        if pat.search(line):
            # Extract status from the wider window (handles OCR splits)
            status = _extract_status_from_window(joined_window)
            return field_type, status

    return None


def _classify_context_only(line: str) -> Optional[Tuple[str, Optional[str]]]:
    """
    Classify the line as a context-only impianti field.
    Returns (field_type, extracted_value_label_or_None) or None.
    """
    # Hard exclusions
    if _NON_UTILITY_IMPIANTO_PAT.search(line):
        return None
    if _VALUATION_PROSE_PAT.search(line):
        return None

    if _CONFORMITA_ABSENT_PAT.search(line):
        # Sub-classify by which system the conformità line mentions
        if _ANY_SYSTEM_PAT.search(line):
            # e.g. "Non esiste la dichiarazione di conformità dell'impianto elettrico"
            # Extract which system
            for ft, spat in _SYSTEM_PATS.items():
                if spat.search(line):
                    label = ft.replace("_status", "_conformita_absent")
                    return "impianti_conformita_context", label
        return "impianti_conformita_context", "conformita_assente"

    if _IMPIANTI_NON_VERIF_PAT.search(line):
        return "impianti_non_verificati_context", "non_verificato"

    if _IMPIANTI_NON_CONFORMI_PAT.search(line):
        return "impianti_non_verificati_context", "impianti_non_conformi_generico"

    if _ALLACCI_PAT.search(line):
        return "allacci_presenza_context", "allacci_reti_tecnologiche"

    if _UTENZE_PAT.search(line):
        return "utenze_attive_context", "utenze_context"

    if _GROUPED_UTILITIES_ABSENCE_PAT.search(line):
        return "allacci_presenza_context", "assenza_totale_impianti_e_servizi"

    if _CONFORMITA_SECTION_HEADER_PAT.search(line):
        return "impianti_conformita_context", "conformita_section_header"

    return None


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_impianti_candidate_pack(case_key: str) -> Dict:
    ctx = build_context(case_key)

    hyp_fp   = ctx.artifact_dir / "structure_hypotheses.json"
    scope_fp = ctx.artifact_dir / "lot_scope_map.json"
    pages_fp = ctx.artifact_dir / "raw_pages.json"
    plh_fp   = ctx.artifact_dir / "plurality_headers.json"
    tzm_fp   = ctx.artifact_dir / "table_zone_map.json"

    hyp        = json.loads(hyp_fp.read_text(encoding="utf-8"))
    scope      = json.loads(scope_fp.read_text(encoding="utf-8"))
    raw_pages: List[Dict] = json.loads(pages_fp.read_text(encoding="utf-8"))
    plh_headers: Dict = (
        json.loads(plh_fp.read_text(encoding="utf-8")) if plh_fp.exists() else {}
    )
    tzm: Dict = (
        json.loads(tzm_fp.read_text(encoding="utf-8")) if tzm_fp.exists() else {}
    )

    winner = hyp.get("winner")

    out: Dict = {
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
            "impianti_packet_count": 0,
            "impianti_fields_present": [],
            "impianti_scope_keys": [],
            "impianti_context_count": 0,
        },
        "summary": {},
        "source_artifacts": {
            "structure_hypotheses": str(hyp_fp),
            "lot_scope_map": str(scope_fp),
            "raw_pages": str(pages_fp),
            "plurality_headers": str(plh_fp),
            "table_zone_map": str(tzm_fp),
        },
    }

    dst = ctx.artifact_dir / "impianti_candidate_pack.json"

    # Early exit: unreadable document
    if winner == "BLOCKED_UNREADABLE":
        out["status"] = "BLOCKED_UNREADABLE"
        out["summary"]["note"] = (
            "Impianti harvesting blocked: document quality is BLOCKED_UNREADABLE."
        )
        dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        return out

    # Build scope / zone lookup structures
    bsm = build_bene_scope_map(case_key)
    lut = _build_lookup_tables(scope, bsm)
    hg_lookup = _build_lot_header_grade_lookup(plh_headers)
    schema_pages = _find_schema_pages(raw_pages)
    recap_pages  = _find_recap_pages(raw_pages, winner)
    zone_index   = _build_table_zone_index(tzm)

    known_lot_ids: Set[str] = {
        str(ls["lot_id"]).strip().lower()
        for ls in (scope.get("lot_scopes") or [])
    }
    known_bene_ids: Set[str] = {
        str(bs["bene_id"]).strip()
        for bs in (bsm.get("bene_scopes") or [])
    }
    known_bene_pairs: Set[Tuple[str, str]] = {
        (str(bs["lot_id"]).strip().lower(), str(bs["bene_id"]).strip())
        for bs in (bsm.get("bene_scopes") or [])
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
        is_recap_page  = page in recap_pages

        # Stateful bene tracking: reset per page.
        # Whenever we encounter an explicit "BENE N° X" header line, we
        # update current_page_bene so subsequent triggers on the same page
        # inherit the correct bene attribution without needing a fixed lookback window.
        current_page_bene: Optional[str] = None

        for i, line in enumerate(lines):
            # Update stateful bene tracker before processing the line
            bm = _EXPLICIT_BENE_LABEL_PAT.search(line)
            if bm:
                candidate_bene = str(bm.group(1)).strip()
                if candidate_bene in known_bene_ids:
                    current_page_bene = candidate_bene
            stripped = line.strip()
            if not stripped:
                continue
            if _DOT_LEADER_PAT.search(line):
                continue

            # Join with forward lines to handle OCR splits
            joined = _join_forward(lines, i, count=2)

            # ---------------------------------------------------------------
            # A. Try per-system status trigger
            # ---------------------------------------------------------------
            per_sys = _classify_per_system(stripped, joined)
            if per_sys is not None:
                field_type, extracted_value = per_sys
                context_win = _make_context_window(lines, i)
                match_counter += 1

                # If no status determinable, this becomes context-only
                is_context_only = (extracted_value is None)

                # Table zone check
                zone_type = _get_zone_type_for_line(page, i, zone_index)
                if zone_type in ("RECAP_SUMMARY_TABLE", "ARITHMETIC_ROLLUP_TABLE"):
                    blocked_or_ambiguous.append({
                        "type": "IMPIANTI_RECAP_DUPLICATE_UNSAFE",
                        "reason": (
                            f"Candidate on p{page}l{i} falls in {zone_type}; "
                            "treated as non-authoritative recap duplicate."
                        ),
                        "field_type": field_type,
                        "page": page,
                        "line_index": i,
                        "quote": stripped,
                        "extracted_value": extracted_value,
                        "zone_type": zone_type,
                    })
                    continue
                if zone_type == "METHODOLOGY_COMPARABLE_TABLE":
                    blocked_or_ambiguous.append({
                        "type": "IMPIANTI_METHOD_PROSE_EXCLUDED",
                        "reason": (
                            f"Candidate on p{page}l{i} falls in "
                            "METHODOLOGY_COMPARABLE_TABLE; not active utility truth."
                        ),
                        "field_type": field_type,
                        "page": page,
                        "line_index": i,
                        "quote": stripped,
                    })
                    continue
                # UNKNOWN_TABLE: allow if value is explicit, else context-only
                if zone_type == "UNKNOWN_TABLE" and not is_context_only:
                    if extracted_value not in ("presente", "assente", "non_funzionante",
                                               "disallacciato", "non_conforme", "da_adeguare"):
                        is_context_only = True

                # Recap / schema page handling for per-system
                if (is_schema_page or is_recap_page) and not is_context_only:
                    blocked_or_ambiguous.append({
                        "type": "IMPIANTI_RECAP_DUPLICATE_UNSAFE",
                        "reason": (
                            "Per-system candidate on SCHEMA RIASSUNTIVO / RIEPILOGO BANDO page; "
                            "treated as non-authoritative."
                        ),
                        "field_type": field_type,
                        "page": page,
                        "line_index": i,
                        "quote": stripped,
                        "extracted_value": extracted_value,
                    })
                    continue

                # Determine scope
                scope_result = _cadat_determine_scope(page, winner, lut)
                attr = scope_result.get("attribution", "CADASTRAL_SCOPE_AMBIGUOUS")
                lot_id = scope_result.get("lot_id")
                bene_id = scope_result.get("bene_id")
                is_blocked = scope_result.get("blocked", False)

                if is_blocked:
                    block_type = _CADAT_TO_IMPIANTI_BLOCK.get(attr, "IMPIANTI_SCOPE_FIELD_CONFLICT")
                    blocked_or_ambiguous.append({
                        "type": block_type,
                        "reason": f"Page {page} falls in blocked geometric zone: {attr}.",
                        "field_type": field_type,
                        "page": page,
                        "line_index": i,
                        "quote": stripped,
                        "extracted_value": extracted_value,
                        "scope_attribution": attr,
                    })
                    continue

                if attr == "LOT_LEVEL_ONLY_PRE_BENE_CONTEXT" and not is_context_only:
                    blocked_or_ambiguous.append({
                        "type": "IMPIANTI_IN_PRE_BENE_CONTEXT",
                        "reason": (
                            f"Impianti candidate on p{page}l{i} falls in pre-bene context zone "
                            f"for lot '{lot_id}'. LOT_LEVEL_ONLY_PRE_BENE_CONTEXT; "
                            "not safe to activate as per-system truth."
                        ),
                        "field_type": field_type,
                        "page": page,
                        "line_index": i,
                        "quote": stripped,
                        "extracted_value": extracted_value,
                        "lot_id": lot_id,
                        "scope_attribution": attr,
                    })
                    continue

                # Bene local context override — use the stateful page-level bene tracker
                # (updated whenever a "BENE N° X" header is seen while scanning).
                # Enables bene-level attribution on pages where the last-bene fallback
                # produced LOT_LEVEL_ONLY but explicit bene sub-sections are present.
                if known_bene_ids and bene_id is None and lot_id is not None:
                    current_pair = (
                        (str(lot_id).strip().lower(), str(current_page_bene).strip())
                        if current_page_bene else None
                    )
                    if current_pair in known_bene_pairs:
                        bene_id = current_page_bene
                        attr = "BENE_LOCAL_CONTEXT_OVERRIDE"

                # Local lot context override (multi-lot only)
                local_lot_override = False
                if (
                    winner in ("H2_EXPLICIT_MULTI_LOT", "H4_CANDIDATE_MULTI_LOT_MULTI_BENE")
                    and lot_id is not None
                    and known_lot_ids
                ):
                    local_lot = _find_local_lot_in_context(lines, i, known_lot_ids)
                    if local_lot is not None and local_lot != str(lot_id).strip().lower():
                        lot_id = local_lot
                        bene_id = None
                        attr = "LOT_LOCAL_CONTEXT_OVERRIDE"
                        local_lot_override = True

                # HEADER_GRADE lot-label mismatch
                if not local_lot_override:
                    mismatch_lot = _check_local_lot_mismatch(page, i, lot_id or "", hg_lookup, winner)
                    if mismatch_lot is not None:
                        blocked_or_ambiguous.append({
                            "type": "IMPIANTI_LOCAL_SCOPE_HEADER_MISMATCH",
                            "reason": (
                                f"HEADER_GRADE lot label '{mismatch_lot}' appears before "
                                f"p{page}l{i}; page-scope lot is '{lot_id}'. "
                                "Cross-lot attribution not safe."
                            ),
                            "field_type": field_type,
                            "page": page,
                            "line_index": i,
                            "quote": stripped,
                            "extracted_value": extracted_value,
                            "page_scope_lot_id": lot_id,
                            "conflicting_lot_label": mismatch_lot,
                        })
                        continue

                # Forward lot-transition guard (multi-lot only)
                if (
                    winner in ("H2_EXPLICIT_MULTI_LOT", "H4_CANDIDATE_MULTI_LOT_MULTI_BENE")
                    and lot_id is not None
                    and known_lot_ids
                    and not local_lot_override
                ):
                    fwd_lot = _find_forward_lot_in_window(lines, i, known_lot_ids)
                    if fwd_lot is not None and fwd_lot != str(lot_id).strip().lower():
                        blocked_or_ambiguous.append({
                            "type": "IMPIANTI_IN_SAME_PAGE_LOT_COLLISION",
                            "reason": (
                                f"Forward 'LOTTO {fwd_lot.upper()}' header within 5 lines "
                                f"after p{page}l{i} (page-scope lot '{lot_id}'). "
                                "Lot-transition zone; blocking."
                            ),
                            "field_type": field_type,
                            "page": page,
                            "line_index": i,
                            "quote": stripped,
                            "forward_lot_label": fwd_lot,
                            "page_scope_lot_id": lot_id,
                        })
                        continue

                cid = _make_candidate_id(
                    field_type, lot_id, bene_id, page, i, match_counter
                )

                if is_context_only:
                    candidates.append({
                        "candidate_id": cid,
                        "field_type": field_type,
                        "is_context_only": True,
                        "extracted_value": None,
                        "raw_label": None,
                        "page": page,
                        "line_index": i,
                        "quote": stripped,
                        "context_window": context_win,
                        "extraction_method": "REGEX_IMPIANTI_PER_SYS_NO_STATUS",
                        "lot_id": lot_id,
                        "bene_id": bene_id,
                        "corpo_id": None,
                        "attribution": attr,
                        "scope_basis": f"Per-system trigger; no status extracted; scope={attr}; lot={lot_id}; bene={bene_id}",
                        "candidate_status": "CONTEXT_ONLY",
                        "zone_type": zone_type,
                    })
                else:
                    candidates.append({
                        "candidate_id": cid,
                        "field_type": field_type,
                        "is_context_only": False,
                        "extracted_value": extracted_value,
                        "raw_label": extracted_value,
                        "page": page,
                        "line_index": i,
                        "quote": stripped,
                        "context_window": context_win,
                        "extraction_method": "REGEX_IMPIANTI_PER_SYS_STATUS",
                        "lot_id": lot_id,
                        "bene_id": bene_id,
                        "corpo_id": None,
                        "attribution": attr,
                        "scope_basis": f"Scope={attr}; lot={lot_id}; bene={bene_id}; p{page}l{i}",
                        "candidate_status": "ACTIVE",
                        "zone_type": zone_type,
                    })
                continue

            # ---------------------------------------------------------------
            # B. Try context-only impianti trigger
            # ---------------------------------------------------------------
            ctx_result = _classify_context_only(stripped)
            if ctx_result is not None:
                ctx_field_type, ctx_label = ctx_result
                context_win = _make_context_window(lines, i)
                match_counter += 1

                # Table zone check for context-only
                zone_type = _get_zone_type_for_line(page, i, zone_index)
                if zone_type == "METHODOLOGY_COMPARABLE_TABLE":
                    blocked_or_ambiguous.append({
                        "type": "IMPIANTI_METHOD_PROSE_EXCLUDED",
                        "reason": f"Context trigger on p{page}l{i} in METHODOLOGY_COMPARABLE_TABLE.",
                        "field_type": ctx_field_type,
                        "page": page,
                        "line_index": i,
                        "quote": stripped,
                    })
                    continue

                # Schema/recap pages: context-only conformità is still useful but mark it
                if is_schema_page or is_recap_page:
                    blocked_or_ambiguous.append({
                        "type": "IMPIANTI_TABLE_CONTEXT_UNSAFE",
                        "reason": (
                            "Context-only impianti on SCHEMA RIASSUNTIVO / RIEPILOGO page; "
                            "may be recap duplication."
                        ),
                        "field_type": ctx_field_type,
                        "page": page,
                        "line_index": i,
                        "quote": stripped,
                        "extracted_value": ctx_label,
                    })
                    continue

                # Determine scope
                scope_result = _cadat_determine_scope(page, winner, lut)
                attr = scope_result.get("attribution", "CADASTRAL_SCOPE_AMBIGUOUS")
                lot_id = scope_result.get("lot_id")
                bene_id = scope_result.get("bene_id")
                is_blocked = scope_result.get("blocked", False)

                if is_blocked:
                    block_type = _CADAT_TO_IMPIANTI_BLOCK.get(attr, "IMPIANTI_SCOPE_FIELD_CONFLICT")
                    blocked_or_ambiguous.append({
                        "type": block_type,
                        "reason": (
                            f"Context trigger on p{page}l{i} in blocked scope zone: {attr}."
                        ),
                        "field_type": ctx_field_type,
                        "page": page,
                        "line_index": i,
                        "quote": stripped,
                        "scope_attribution": attr,
                    })
                    continue

                # Bene local context override (context-only — same stateful tracker)
                if known_bene_ids and bene_id is None and lot_id is not None:
                    current_pair = (
                        (str(lot_id).strip().lower(), str(current_page_bene).strip())
                        if current_page_bene else None
                    )
                    if current_pair in known_bene_pairs:
                        bene_id = current_page_bene
                        attr = "BENE_LOCAL_CONTEXT_OVERRIDE"

                # Multi-lot local lot override
                if (
                    winner in ("H2_EXPLICIT_MULTI_LOT", "H4_CANDIDATE_MULTI_LOT_MULTI_BENE")
                    and lot_id is not None
                    and known_lot_ids
                ):
                    local_lot = _find_local_lot_in_context(lines, i, known_lot_ids)
                    if local_lot is not None and local_lot != str(lot_id).strip().lower():
                        lot_id = local_lot
                        bene_id = None
                        attr = "LOT_LOCAL_CONTEXT_OVERRIDE"

                cid = _make_candidate_id(
                    ctx_field_type, lot_id, bene_id, page, i, match_counter
                )
                candidates.append({
                    "candidate_id": cid,
                    "field_type": ctx_field_type,
                    "is_context_only": True,
                    "extracted_value": ctx_label,
                    "raw_label": ctx_label,
                    "page": page,
                    "line_index": i,
                    "quote": stripped,
                    "context_window": context_win,
                    "extraction_method": "REGEX_IMPIANTI_CONTEXT",
                    "lot_id": lot_id,
                    "bene_id": bene_id,
                    "corpo_id": None,
                    "attribution": attr,
                    "scope_basis": f"Context-only; scope={attr}; lot={lot_id}; bene={bene_id}",
                    "candidate_status": "CONTEXT_ONLY",
                    "zone_type": zone_type,
                })

    # -----------------------------------------------------------------------
    # Conflict detection for per-system ACTIVE candidates
    # Same scope + same field_type + multiple distinct values → BLOCKED
    # -----------------------------------------------------------------------
    active_per_sys = [
        c for c in candidates
        if c.get("candidate_status") == "ACTIVE" and not c.get("is_context_only")
    ]

    # Group by (lot_id, bene_id, field_type)
    grouped: Dict[tuple, List[Dict]] = {}
    for c in active_per_sys:
        lot_id = c.get("lot_id") or "unknown"
        bene_key = c.get("bene_id") or "lot"
        ft = c["field_type"]
        key = (lot_id, bene_key, ft)
        grouped.setdefault(key, []).append(c)

    final_candidates: List[Dict] = []
    # Keep context-only candidates as-is
    for c in candidates:
        if c.get("is_context_only") or c.get("candidate_status") == "CONTEXT_ONLY":
            final_candidates.append(c)

    for (lot_id, bene_key, field_type), group in sorted(grouped.items()):
        distinct_vals = sorted({str(c.get("extracted_value", "")).strip().lower() for c in group})
        if len(distinct_vals) > 1:
            blocked_or_ambiguous.append({
                "type": "IMPIANTI_SCOPE_FIELD_CONFLICT",
                "reason": (
                    "Multiple distinct values for same scope + field_type; "
                    "no active winner emitted."
                ),
                "field_type": field_type,
                "lot_id": lot_id,
                "bene_id": None if bene_key == "lot" else bene_key,
                "distinct_values": distinct_vals,
                "candidate_count": len(group),
                "candidates": [
                    {
                        "candidate_id": c.get("candidate_id"),
                        "extracted_value": c.get("extracted_value"),
                        "page": c.get("page"),
                        "line_index": c.get("line_index"),
                        "quote": c.get("quote"),
                        "attribution": c.get("attribution"),
                    }
                    for c in group
                ],
            })
        else:
            # Single distinct value — all candidates agree → keep all as ACTIVE
            final_candidates.extend(group)

    # -----------------------------------------------------------------------
    # Finalize
    # -----------------------------------------------------------------------
    out["candidates"] = final_candidates
    out["blocked_or_ambiguous"] = blocked_or_ambiguous

    active_status = [c for c in final_candidates if c.get("candidate_status") == "ACTIVE"]
    context_only  = [c for c in final_candidates if c.get("candidate_status") == "CONTEXT_ONLY"]

    out["coverage"]["candidates_harvested"] = len(final_candidates)
    out["coverage"]["blocked_or_ambiguous_count"] = len(blocked_or_ambiguous)
    out["coverage"]["impianti_packet_count"] = len(active_status)
    out["coverage"]["impianti_fields_present"] = sorted({c["field_type"] for c in active_status})
    out["coverage"]["impianti_scope_keys"] = sorted({
        (
            f"{c['lot_id']}/{c['bene_id']}"
            if c.get("bene_id")
            else f"lot:{c.get('lot_id', 'unknown')}"
        )
        for c in active_status
        if c.get("lot_id")
    })
    out["coverage"]["impianti_context_count"] = len(context_only)

    out["summary"] = {
        "active_per_system_count": len(active_status),
        "context_only_count": len(context_only),
        "blocked_or_ambiguous_count": len(blocked_or_ambiguous),
        "active_field_types": sorted({c["field_type"] for c in active_status}),
        "context_field_types": sorted({c["field_type"] for c in context_only}),
        "scope_keys_with_active_status": sorted({
            (
                f"{c['lot_id']}/{c['bene_id']}"
                if c.get("bene_id")
                else f"lot:{c.get('lot_id', 'unknown')}"
            )
            for c in active_status
        }),
    }

    dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Impianti / utilities bounded field shell")
    parser.add_argument("--case", required=True, choices=list(list_case_keys()))
    args = parser.parse_args()

    out = build_impianti_candidate_pack(args.case)
    print(json.dumps({
        "case_key": out["case_key"],
        "status": out["status"],
        "winner": out["winner"],
        "active_per_system": out["coverage"]["impianti_packet_count"],
        "impianti_fields_present": out["coverage"]["impianti_fields_present"],
        "impianti_scope_keys": out["coverage"]["impianti_scope_keys"],
        "context_only_count": out["coverage"]["impianti_context_count"],
        "blocked_or_ambiguous_count": out["coverage"]["blocked_or_ambiguous_count"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
