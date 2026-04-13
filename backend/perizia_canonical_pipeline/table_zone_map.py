"""
Table-aware canonical shared layer — zone detection and classification.

Produces: table_zone_map.json

This is shared canonical infrastructure that:
- Detects and classifies table-like zones from extracted text
- Exposes machine-readable zone metadata for use by later shells
- Helps later shells answer:
    * is this candidate inside a table?
    * what kind of table is it?
    * is the table likely authoritative or recap-only?
    * is this likely a rollup/summary total rather than raw field truth?

Allowed zone_type values:
  AUTHORITATIVE_FIELD_TABLE     — direct field-bearing table for one asset/lot/bene
  RECAP_SUMMARY_TABLE           — recap/summary/schema riassuntivo/bando asta pages
  ARITHMETIC_ROLLUP_TABLE       — totals/subtotals/arithmetic rollup (dangerous for raw truth)
  METHODOLOGY_COMPARABLE_TABLE  — OMI/comparable/€/mq method/procedural valuation
  UNKNOWN_TABLE                 — detected table-like block, type not safely classifiable

What is NOT built here:
  - full row/column normalized field extraction
  - cell graph reconstruction
  - universal span model
  - new final business logic
  - shell-by-shell table refit
  - LLM conflict resolution
  - doc_map freeze

Conservative typing rule:
  If a zone is ambiguous between authoritative vs recap, prefer:
    RECAP_SUMMARY_TABLE or UNKNOWN_TABLE
  not AUTHORITATIVE_FIELD_TABLE.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from .runner import build_context
from .corpus_registry import load_cases, list_case_keys


# ---------------------------------------------------------------------------
# Zone types
# ---------------------------------------------------------------------------

ZONE_TYPE_AUTHORITATIVE = "AUTHORITATIVE_FIELD_TABLE"
ZONE_TYPE_RECAP = "RECAP_SUMMARY_TABLE"
ZONE_TYPE_ROLLUP = "ARITHMETIC_ROLLUP_TABLE"
ZONE_TYPE_METHODOLOGY = "METHODOLOGY_COMPARABLE_TABLE"
ZONE_TYPE_UNKNOWN = "UNKNOWN_TABLE"


# ---------------------------------------------------------------------------
# TOC detection
# ---------------------------------------------------------------------------

_DOT_LEADER_PAT = re.compile(r"\.{5,}|(?:\s*\.\s*){6,}")


def _is_toc_page(lines: List[str]) -> bool:
    """A page dominated by dot-leaders is a table of contents, not a real table zone."""
    dot_leader_count = sum(1 for ln in lines if _DOT_LEADER_PAT.search(ln))
    return dot_leader_count >= 4


# ---------------------------------------------------------------------------
# RECAP_SUMMARY_TABLE markers
# ---------------------------------------------------------------------------

# "RIEPILOGO BANDO D'ASTA" — full auction-band summary
_RIEPILOGO_BANDO_PAT = re.compile(
    r"RIEPILOGO\s+BANDO\s+D[\x27\u2018\u2019'\W]ASTA",
    re.IGNORECASE,
)

# "SCHEMA RIASSUNTIVO" — the standardized summary schema page
_SCHEMA_RIASSUNTIVO_PAT = re.compile(
    r"\bSCHEMA\s+RIASSUNTIVO\b",
    re.IGNORECASE,
)

# "SCHEDA SINTETICA DEL BENE" — per-bene summary card
_SCHEDA_SINTETICA_PAT = re.compile(
    r"\bSCHEDA\s+SINTETICA\s+DEL\s+BENE\b",
    re.IGNORECASE,
)

# "DESCRIZIONE SOMMARIA E RIEPILOGO VALUTAZIONE" — per-lot section header
# (a section label, not a full page recap; treated as a short local recap marker)
_DESCR_SOMMARIA_RIEPILOGO_PAT = re.compile(
    r"\bDESCRIZIONE\s+SOMMARIA\s+E\s+RIEPILOGO\s+VALUTAZIONE\b",
    re.IGNORECASE,
)

# "RIEPILOGO VALUTAZIONE" as a short label (section heading, not table)
_RIEPILOGO_VALUTAZIONE_HDR_PAT = re.compile(
    r"^\s*RIEPILOGO\s+VALUTAZIONE\s*(?::|$)",
    re.IGNORECASE,
)

# "RIEPILOGO VALORI" standalone (section heading)
_RIEPILOGO_VALORI_HDR_PAT = re.compile(
    r"^\s*RIEPILOGO\s+VALORI\s*$",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# ARITHMETIC_ROLLUP_TABLE markers
# ---------------------------------------------------------------------------

# "VALORE ATTUALE DEL COMPENDIO PIGNORATO" — net value after cost deductions
_VALORE_ATTUALE_COMPENDIO_PAT = re.compile(
    r"\bVALORE\s+ATTUALE\s+DEL\s+COMPENDIO\s+PIGNORATO\b",
    re.IGNORECASE,
)

# "RIEPILOGO VALORI CORPO" — per-body rollup section marker
_RIEPILOGO_VALORI_CORPO_PAT = re.compile(
    r"\bRIEPILOGO\s+VALORI\s+CORPO\b",
    re.IGNORECASE,
)

# "Riepilogo dei valori attribuiti alle diverse tipologie" — category sum table
_RIEPILOGO_VALORI_ATTR_PAT = re.compile(
    r"\bRiepilogo\s+dei\s+valori\s+attribuiti\b",
    re.IGNORECASE,
)

# Arithmetic subtraction line pattern (value - cost = net)
_ARITHMETIC_SUBTRACTION_PAT = re.compile(
    r"(?:€\.?\s*[\d.,]+\s*[-–]\s*|€\.?\s*[\d.,]+\s+[-–]\s*$)",
    re.IGNORECASE,
)

# Horizontal rule / underscores (arithmetic separator line)
_UNDERLINE_PAT = re.compile(r"_{4,}|={4,}|[-]{4,}")


# ---------------------------------------------------------------------------
# METHODOLOGY_COMPARABLE_TABLE markers
# ---------------------------------------------------------------------------

# "RIEPILOGO VALUTAZIONE DI MERCATO DEI CORPI" — OMI tabular comparison
_RIEPILOGO_VAL_MERCATO_CORPI_PAT = re.compile(
    r"\bRIEPILOGO\s+VALUTAZIONE\s+DI\s+MERCATO(?:\s+DEI\s+CORPI)?\b",
    re.IGNORECASE,
)

# "VALORE DI MERCATO (OMV):" — OMV header line
_OMV_HEADER_PAT = re.compile(
    r"\bVALORE\s+DI\s+MERCATO\s*\(OMV\)\s*:",
    re.IGNORECASE,
)

# "CALCOLO DEL VALORE DI MERCATO" — step-by-step valuation section
_CALCOLO_VALORE_MERCATO_PAT = re.compile(
    r"^\s*CALCOLO\s+DEL\s+VALORE\s+DI\s+MERCATO\s*:?\s*$",
    re.IGNORECASE,
)

# "SVILUPPO VALUTAZIONE:" — valuation methodology development
_SVILUPPO_VALUTAZIONE_PAT = re.compile(
    r"^\s*SVILUPPO\s+VALUTAZIONE\s*:",
    re.IGNORECASE,
)

# OMI column headers (ID / descrizione / consistenza / cons. accessori / valore intero)
_OMI_COLUMN_HDR_PAT = re.compile(
    r"(?:consistenza|cons\.\s*accessori|valore\s+intero|valore\s+diritto)",
    re.IGNORECASE,
)

# Per-unit rate (€/mq) — methodology calculation, not a field value
_PER_UNIT_RATE_PAT = re.compile(
    r"€\s*/\s*mq|\d+[\s,.]?\d*\s*€\s*/\s*mq|€\s*/\s*m²|\d+[\s,.]\d+\s+Euro/mq",
    re.IGNORECASE,
)

# "Prezzo richiesto: NNN" or "Sconto trattativa: N%" — comparable analysis
_PREZZO_RICHIESTO_PAT = re.compile(
    r"Prezzo\s+richiesto\s*:\s*[\d.,]+",
    re.IGNORECASE,
)

# OMI/comparable market analysis
_OMI_COMPARABLE_PAT = re.compile(
    r"(?:\bOMI\b.*quotazion|quotazioni\s+OMI\b|banca\s+dati\s+delle\s+quotazioni)",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# AUTHORITATIVE_FIELD_TABLE heuristic markers
# ---------------------------------------------------------------------------

# Cadastral data dense cluster (foglio / mappale / sub / categoria)
_FOGLIO_PAT = re.compile(r"\bFoglio\s*n[°.°]?\s*\d+", re.IGNORECASE)
_MAPPALE_PAT = re.compile(r"\bMappale\s*n[°.°]?\s*\d+", re.IGNORECASE)
_SUB_CATASTALE_PAT = re.compile(r"\bSub(?:alterno)?\s*n[°.°]?\s*\d+", re.IGNORECASE)
_CATEGORIA_CATASTALE_PAT = re.compile(r"\bCategoria\s*:\s*[A-Z]/\d+", re.IGNORECASE)

# Dense label:value amount pattern
_LABEL_AMOUNT_PAT = re.compile(
    r"[\w\s]{3,40}\s*:\s*(?:€\.?\s*)?[\d]{1,3}(?:[\s.]+\d{2,3})*(?:,\d{1,2})?",
    re.IGNORECASE,
)

# Section stop — known anchors that end a zone scan
_SECTION_STOP_MAJOR_PAT = re.compile(
    r"^\s*(?:"
    r"\d+\s*\.\s+[A-ZÀÈÉÌÒÙ]"           # "1. Section title" or "8.1. CONFORMITÀ"
    r"|GIUDIZI\s+DI\s+CONFORMIT"
    r"|DICHIARAZIONE\s+DI\s+CONFORMIT"
    r"|STATO\s+DI\s+POSSESSO"
    r"|CONFORMIT[AÀ]\s+EDILIZIA"
    r"|CONFORMIT[AÀ]\s+URBANISTICA"
    r"|REGOLARIT[AÀ]\s+EDILIZIA"
    r"|REGOLARIT[AÀ]\s+URBANISTICA"
    r"|DESCRIZIONE\s+SOMMARIA"
    r"|VALUTAZIONE\s+COMPLESSIVA"
    r"|LOTTO\s+[A-Z0-9]+\s*[-–\s]*$"
    r"|Bene\s+N[°.]\s*\d+"
    r"|Giudizio\s+di\s+comoda"
    r")",
    re.IGNORECASE,
)

# Explicit lot header for multi-lot zone attribution
_LOT_HEADER_PAT = re.compile(r"\bLOTTO\s+([A-Z0-9]+)\b", re.IGNORECASE)

# Bene header
_BENE_HEADER_PAT = re.compile(r"\bBene\s+N[°.]\s*(\d+)\b", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Helpers: zone extent
# ---------------------------------------------------------------------------

def _find_zone_extent(
    lines: List[str],
    anchor_idx: int,
    max_forward: int = 25,
    stop_on_major_section: bool = True,
    stop_on_lot_header: bool = True,
) -> int:
    """
    Scan forward from anchor_idx to determine the last line of the zone.
    Returns end_line_index (inclusive).
    Stops at: major section boundary, or max_forward lines scanned.

    stop_on_lot_header: when False, LOTTO X patterns do not terminate the zone.
    Use False for RECAP zones where LOTTO headers are zone content, not boundaries.
    """
    # Section stop without the LOTTO pattern (used for recap zones)
    _SECTION_STOP_NO_LOT_PAT = re.compile(
        r"^\s*(?:"
        r"\d+\s*\.\s+[A-ZÀÈÉÌÒÙ]"
        r"|GIUDIZI\s+DI\s+CONFORMIT"
        r"|DICHIARAZIONE\s+DI\s+CONFORMIT"
        r"|STATO\s+DI\s+POSSESSO"
        r"|CONFORMIT[AÀ]\s+EDILIZIA"
        r"|CONFORMIT[AÀ]\s+URBANISTICA"
        r"|REGOLARIT[AÀ]\s+EDILIZIA"
        r"|REGOLARIT[AÀ]\s+URBANISTICA"
        r"|DESCRIZIONE\s+SOMMARIA"
        r"|VALUTAZIONE\s+COMPLESSIVA"
        r")",
        re.IGNORECASE,
    )
    stop_pat = _SECTION_STOP_MAJOR_PAT if stop_on_lot_header else _SECTION_STOP_NO_LOT_PAT

    end = anchor_idx
    for i in range(anchor_idx + 1, min(len(lines), anchor_idx + max_forward + 1)):
        stripped = lines[i].strip()
        # Skip blank lines but continue scanning
        if not stripped:
            end = i
            continue
        if stop_on_major_section and stop_pat.match(stripped):
            break
        end = i
    return end


def _find_compact_zone_extent(
    lines: List[str],
    anchor_idx: int,
    max_forward: int = 12,
) -> int:
    """
    Compact zone extent: stop at first empty line gap (≥2 consecutive blank lines)
    or at a major section boundary. Used for ARITHMETIC_ROLLUP and compact zones.
    """
    end = anchor_idx
    blank_run = 0
    for i in range(anchor_idx + 1, min(len(lines), anchor_idx + max_forward + 1)):
        stripped = lines[i].strip()
        if not stripped:
            blank_run += 1
            if blank_run >= 2:
                break
            end = i
            continue
        blank_run = 0
        if _SECTION_STOP_MAJOR_PAT.match(stripped):
            break
        end = i
    return end


# ---------------------------------------------------------------------------
# Helpers: local scope attribution from lot/bene scope maps
# ---------------------------------------------------------------------------

def _build_lot_page_lookup(lot_scope_map: Dict) -> Dict[int, str]:
    """
    Build {page_number: lot_id} from lot_scope_map.
    If a page is covered by multiple lots (collision), maps to None (ambiguous).
    """
    lookup: Dict[int, Optional[str]] = {}
    for scope in lot_scope_map.get("lot_scopes") or []:
        lot_id = str(scope.get("lot_id", "")).strip().lower()
        start_page = scope.get("start_page")
        end_page = scope.get("end_page")
        if not lot_id or start_page is None or end_page is None:
            continue
        for pg in range(int(start_page), int(end_page) + 1):
            if pg in lookup:
                lookup[pg] = None  # collision → ambiguous
            else:
                lookup[pg] = lot_id
    return lookup


def _build_bene_page_lookup(bene_scope_map: Dict) -> Dict[int, str]:
    """
    Build {page_number: composite_key} from bene_scope_map.
    If a page is covered by multiple benes (collision), maps to None.
    """
    lookup: Dict[int, Optional[str]] = {}
    for scope in bene_scope_map.get("bene_scopes") or []:
        composite_key = str(scope.get("composite_key", "")).strip()
        start_page = scope.get("start_page")
        end_page = scope.get("end_page")
        if not composite_key or start_page is None or end_page is None:
            continue
        for pg in range(int(start_page), int(end_page) + 1):
            if pg in lookup:
                lookup[pg] = None
            else:
                lookup[pg] = composite_key
    return lookup


def _resolve_zone_scope(
    page: int,
    anchor_idx: int,
    end_idx: int,
    lines: List[str],
    zone_type: str,
    lot_page_lookup: Dict[int, Optional[str]],
    bene_page_lookup: Dict[int, Optional[str]],
    known_lot_ids: Set[str],
) -> Tuple[Optional[str], Optional[str]]:
    """
    Resolve local_lot_id and local_bene_id for a detected zone.

    Priority rule for local_lot_id:
      1. Scan the full zone body (anchor_idx..end_idx) for explicit LOTTO markers.
         Only count IDs that appear in known_lot_ids (prevents false positives from
         narrative uses of "LOTTO" when no known lot set is available, we accept any).
      2. Exactly one distinct lot_id in zone body → use it (overrides ambient scope).
      3. Multiple distinct lot_ids in zone body → None (multi-scope-safe).
      4. Zero explicit markers → fall back to ambient page scope from lot_page_lookup
         (may be None if the page itself has ambiguous lot coverage).

    Rule for local_bene_id:
      - RECAP_SUMMARY_TABLE and ARITHMETIC_ROLLUP_TABLE: bene is null unless the
        zone body explicitly names exactly one Bene N° X with no conflicting markers.
        These zone types are typically lot-level or cross-bene; inheriting a stale
        ambient bene would stamp false specificity.
      - All other types: use ambient bene from bene_page_lookup (page-scope fallback).
    """
    # 1. Collect all distinct explicit lot IDs in the zone body
    zone_lot_ids: Set[str] = set()
    for li in range(anchor_idx, min(end_idx + 1, len(lines))):
        m = _LOT_HEADER_PAT.search(lines[li])
        if m:
            found_lot = m.group(1).strip().lower()
            # If we have a known lot set, filter to it; otherwise accept any match
            if not known_lot_ids or found_lot in known_lot_ids:
                zone_lot_ids.add(found_lot)

    # 2. Resolve local_lot_id from zone body, then fall back to ambient
    if len(zone_lot_ids) == 1:
        local_lot_id: Optional[str] = next(iter(zone_lot_ids))
    elif len(zone_lot_ids) > 1:
        local_lot_id = None  # multi-lot zone — do not stamp false specificity
    else:
        # No explicit lot marker found in zone body.
        # For RECAP/ROLLUP zones in multi-lot documents the ambient page scope is
        # unreliable: the page may sit inside one lot's range while the zone content
        # belongs to another (or spans multiple lots).  Emit None rather than a
        # potentially stale lot id.
        # For single-lot documents ambient scope is safe (there is only one lot).
        # For non-RECAP/ROLLUP zone types ambient scope is also acceptable.
        _is_multi_lot_doc = len(known_lot_ids) > 1
        if _is_multi_lot_doc and zone_type in {ZONE_TYPE_RECAP, ZONE_TYPE_ROLLUP}:
            local_lot_id = None  # conservative: no proof from zone body
        else:
            local_lot_id = lot_page_lookup.get(page)

    # 3. Resolve local_bene_id
    # For recap/rollup zones: only assign bene when zone body proves exactly one bene
    _recap_rollup_types = {ZONE_TYPE_RECAP, ZONE_TYPE_ROLLUP}
    if zone_type in _recap_rollup_types:
        zone_bene_ids: Set[str] = set()
        for li in range(anchor_idx, min(end_idx + 1, len(lines))):
            m = _BENE_HEADER_PAT.search(lines[li])
            if m:
                zone_bene_ids.add(m.group(1).strip())
        local_bene_id: Optional[str] = (
            next(iter(zone_bene_ids)) if len(zone_bene_ids) == 1 else None
        )
    else:
        # For other types, fall back to ambient bene from page scope
        local_bene_id = bene_page_lookup.get(page)

    return local_lot_id, local_bene_id


# ---------------------------------------------------------------------------
# Zone ID generator
# ---------------------------------------------------------------------------

_zone_counter: int = 0


def _make_zone_id(case_key: str, page: int, line: int) -> str:
    return f"zone::{case_key}::p{page}::l{line}"


# ---------------------------------------------------------------------------
# Per-page zone detection
# ---------------------------------------------------------------------------

def _detect_zones_on_page(
    page_data: Dict,
    case_key: str,
    winner: str,
    lot_page_lookup: Dict[int, Optional[str]],
    bene_page_lookup: Dict[int, Optional[str]],
    known_lot_ids: Set[str],
) -> List[Dict]:
    """
    Detect and classify table-like zones on a single page.
    Returns a list of zone dicts.
    """
    page = int(page_data["page_number"])
    text = page_data.get("text", "") or ""
    lines = text.split("\n")

    # Skip TOC pages
    if _is_toc_page(lines):
        return []

    zones: List[Dict] = []
    consumed_lines: Set[int] = set()

    def _emit_zone(
        anchor_idx: int,
        end_idx: int,
        zone_type: str,
        confidence: str,
        detected_markers: List[str],
        zone_basis: str,
        notes: str = "",
    ) -> None:
        if anchor_idx in consumed_lines:
            return
        for li in range(anchor_idx, end_idx + 1):
            consumed_lines.add(li)

        local_lot_id, local_bene_id = _resolve_zone_scope(
            page, anchor_idx, end_idx, lines, zone_type,
            lot_page_lookup, bene_page_lookup, known_lot_ids,
        )

        header_lines = [lines[anchor_idx].strip()]
        # Collect a few non-empty sample lines after the header
        sample: List[str] = []
        for si in range(anchor_idx + 1, min(len(lines), anchor_idx + 6)):
            stripped = lines[si].strip()
            if stripped and len(sample) < 4:
                sample.append(stripped[:80])

        zones.append({
            "zone_id": _make_zone_id(case_key, page, anchor_idx),
            "page": page,
            "start_line_index": anchor_idx,
            "end_line_index": end_idx,
            "header_lines": header_lines,
            "sample_lines": sample,
            "detected_markers": detected_markers,
            "zone_type": zone_type,
            "confidence": confidence,
            "zone_basis": zone_basis,
            "local_lot_id": local_lot_id,
            "local_bene_id": local_bene_id,
            "notes": notes,
        })

    # -----------------------------------------------------------------------
    # Pass 1: High-confidence anchor-based detection
    # -----------------------------------------------------------------------
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue

        # --- RECAP_SUMMARY_TABLE: RIEPILOGO BANDO D'ASTA ---
        if _RIEPILOGO_BANDO_PAT.search(stripped):
            # LOTTO headers are content of the recap, not section boundaries
            end = _find_zone_extent(lines, i, max_forward=60, stop_on_lot_header=False)
            _emit_zone(
                anchor_idx=i,
                end_idx=end,
                zone_type=ZONE_TYPE_RECAP,
                confidence="HIGH",
                detected_markers=["RIEPILOGO_BANDO_ASTA"],
                zone_basis="ANCHOR_MARKER",
                notes="Auction-band recap section. Content duplicates per-lot authoritative sections.",
            )
            continue

        # --- RECAP_SUMMARY_TABLE: SCHEMA RIASSUNTIVO ---
        if _SCHEMA_RIASSUNTIVO_PAT.search(stripped) and i not in consumed_lines:
            # LOTTO headers are content of the schema, not section boundaries
            end = _find_zone_extent(lines, i, max_forward=80, stop_on_lot_header=False)
            _emit_zone(
                anchor_idx=i,
                end_idx=end,
                zone_type=ZONE_TYPE_RECAP,
                confidence="HIGH",
                detected_markers=["SCHEMA_RIASSUNTIVO"],
                zone_basis="ANCHOR_MARKER",
                notes="Schema riassuntivo page. Contains recap field data mirroring earlier authoritative sections.",
            )
            continue

        # --- RECAP_SUMMARY_TABLE: SCHEDA SINTETICA DEL BENE ---
        if _SCHEDA_SINTETICA_PAT.search(stripped) and i not in consumed_lines:
            end = _find_zone_extent(lines, i, max_forward=60, stop_on_lot_header=False)
            _emit_zone(
                anchor_idx=i,
                end_idx=end,
                zone_type=ZONE_TYPE_RECAP,
                confidence="HIGH",
                detected_markers=["SCHEDA_SINTETICA_DEL_BENE"],
                zone_basis="ANCHOR_MARKER",
                notes="Per-bene summary card. Recap-type data.",
            )
            continue

        # --- RECAP_SUMMARY_TABLE: DESCRIZIONE SOMMARIA E RIEPILOGO VALUTAZIONE ---
        if _DESCR_SOMMARIA_RIEPILOGO_PAT.search(stripped) and i not in consumed_lines:
            # This is a section sub-heading, not a full table — compact zone
            end = _find_compact_zone_extent(lines, i, max_forward=6)
            _emit_zone(
                anchor_idx=i,
                end_idx=end,
                zone_type=ZONE_TYPE_RECAP,
                confidence="MEDIUM",
                detected_markers=["DESCRIZIONE_SOMMARIA_E_RIEPILOGO_VALUTAZIONE"],
                zone_basis="ANCHOR_MARKER",
                notes="Per-lot section sub-heading. Short recap label, not a full table.",
            )
            continue

        # --- ARITHMETIC_ROLLUP_TABLE: VALORE ATTUALE DEL COMPENDIO PIGNORATO ---
        if _VALORE_ATTUALE_COMPENDIO_PAT.search(stripped) and i not in consumed_lines:
            end = _find_compact_zone_extent(lines, i, max_forward=15)
            _emit_zone(
                anchor_idx=i,
                end_idx=end,
                zone_type=ZONE_TYPE_ROLLUP,
                confidence="HIGH",
                detected_markers=["VALORE_ATTUALE_COMPENDIO_PIGNORATO"],
                zone_basis="ANCHOR_MARKER",
                notes="Net valuation arithmetic: Valore di Stima - Costi = Net. Post-deduction result, not raw truth.",
            )
            continue

        # --- ARITHMETIC_ROLLUP_TABLE: RIEPILOGO VALORI CORPO ---
        if _RIEPILOGO_VALORI_CORPO_PAT.search(stripped) and i not in consumed_lines:
            # Typically a compact end-of-section marker
            end = _find_compact_zone_extent(lines, i, max_forward=6)
            _emit_zone(
                anchor_idx=i,
                end_idx=end,
                zone_type=ZONE_TYPE_ROLLUP,
                confidence="HIGH",
                detected_markers=["RIEPILOGO_VALORI_CORPO"],
                zone_basis="ANCHOR_MARKER",
                notes="Per-body rollup marker. Section-closing summary line.",
            )
            continue

        # --- ARITHMETIC_ROLLUP_TABLE: Riepilogo dei valori attribuiti ---
        if _RIEPILOGO_VALORI_ATTR_PAT.search(stripped) and i not in consumed_lines:
            end = _find_compact_zone_extent(lines, i, max_forward=12)
            _emit_zone(
                anchor_idx=i,
                end_idx=end,
                zone_type=ZONE_TYPE_ROLLUP,
                confidence="HIGH",
                detected_markers=["RIEPILOGO_VALORI_ATTRIBUITI"],
                zone_basis="ANCHOR_MARKER",
                notes="Category-level value rollup (e.g., TERRENI + FABBRICATI + CASE = total). Sum-type, not raw field truth.",
            )
            continue

        # --- METHODOLOGY_COMPARABLE_TABLE: RIEPILOGO VALUTAZIONE DI MERCATO DEI CORPI ---
        if _RIEPILOGO_VAL_MERCATO_CORPI_PAT.search(stripped) and i not in consumed_lines:
            end = _find_zone_extent(lines, i, max_forward=20)
            _emit_zone(
                anchor_idx=i,
                end_idx=end,
                zone_type=ZONE_TYPE_METHODOLOGY,
                confidence="HIGH",
                detected_markers=["RIEPILOGO_VALUTAZIONE_DI_MERCATO_DEI_CORPI"],
                zone_basis="ANCHOR_MARKER",
                notes="OMI-style comparative tabular summary. Methodology/comparable classification only.",
            )
            continue

        # --- METHODOLOGY_COMPARABLE_TABLE: VALORE DI MERCATO (OMV): ---
        if _OMV_HEADER_PAT.search(stripped) and i not in consumed_lines:
            end = _find_zone_extent(lines, i, max_forward=20)
            _emit_zone(
                anchor_idx=i,
                end_idx=end,
                zone_type=ZONE_TYPE_METHODOLOGY,
                confidence="HIGH",
                detected_markers=["VALORE_DI_MERCATO_OMV_HEADER"],
                zone_basis="ANCHOR_MARKER",
                notes="OMV header introducing comparable market value table.",
            )
            continue

        # --- METHODOLOGY_COMPARABLE_TABLE: CALCOLO DEL VALORE DI MERCATO ---
        if _CALCOLO_VALORE_MERCATO_PAT.match(stripped) and i not in consumed_lines:
            end = _find_compact_zone_extent(lines, i, max_forward=15)
            _emit_zone(
                anchor_idx=i,
                end_idx=end,
                zone_type=ZONE_TYPE_METHODOLOGY,
                confidence="HIGH",
                detected_markers=["CALCOLO_DEL_VALORE_DI_MERCATO"],
                zone_basis="ANCHOR_MARKER",
                notes="Step-by-step comparable market calculation block. Methodology, not field assignment.",
            )
            continue

        # --- METHODOLOGY_COMPARABLE_TABLE: SVILUPPO VALUTAZIONE ---
        if _SVILUPPO_VALUTAZIONE_PAT.match(stripped) and i not in consumed_lines:
            end = _find_zone_extent(lines, i, max_forward=20)
            _emit_zone(
                anchor_idx=i,
                end_idx=end,
                zone_type=ZONE_TYPE_METHODOLOGY,
                confidence="MEDIUM",
                detected_markers=["SVILUPPO_VALUTAZIONE"],
                zone_basis="ANCHOR_MARKER",
                notes="Valuation methodology development section.",
            )
            continue

    # -----------------------------------------------------------------------
    # Pass 2: Heuristic detection — dense amount clusters not already covered
    # -----------------------------------------------------------------------
    # Scan for runs of ≥3 consecutive non-empty lines each containing a
    # monetary amount. These are candidate UNKNOWN_TABLE zones if not already
    # consumed by a high-confidence anchor.
    _AMOUNT_HEURISTIC_PAT = re.compile(
        r"(?:€\.?\s*[\d]{1,3}(?:[\s.]+\d{2,3})*(?:,\d{1,2})?|[\d]{1,3}(?:[\s.]+\d{2,3})*,\d{2}\s*€)",
        re.IGNORECASE,
    )
    _LABEL_COLON_PAT = re.compile(r":\s*$|:\s*€")

    i = 0
    while i < len(lines):
        if i in consumed_lines:
            i += 1
            continue
        stripped = lines[i].strip()
        if not stripped or not _AMOUNT_HEURISTIC_PAT.search(stripped):
            i += 1
            continue

        # Found a line with an amount — check if it's part of a run
        run_start = i
        run_end = i
        j = i + 1
        consecutive_amount_lines = 1
        while j < len(lines) and j < i + 20:
            ls = lines[j].strip()
            if not ls:
                j += 1
                continue
            if _AMOUNT_HEURISTIC_PAT.search(ls):
                consecutive_amount_lines += 1
                run_end = j
                j += 1
            else:
                # Allow 1 non-amount content line gap in the run
                if consecutive_amount_lines >= 2:
                    break
                j += 1

        if consecutive_amount_lines >= 3 and run_start not in consumed_lines:
            # Determine zone basis
            # Check whether this looks like a methodology block
            context_block = "\n".join(lines[run_start:run_end + 1])
            zone_type = ZONE_TYPE_UNKNOWN
            markers: List[str] = ["HEURISTIC_DENSE_AMOUNT_CLUSTER"]
            confidence = "LOW"
            notes = "Heuristic: ≥3 consecutive lines with monetary amounts."

            if _PER_UNIT_RATE_PAT.search(context_block):
                zone_type = ZONE_TYPE_METHODOLOGY
                markers.append("PER_UNIT_RATE_DETECTED")
                confidence = "MEDIUM"
                notes += " Per-unit rate (€/mq) detected → likely methodology."
            elif _PREZZO_RICHIESTO_PAT.search(context_block):
                zone_type = ZONE_TYPE_METHODOLOGY
                markers.append("COMPARABLE_ANALYSIS_DETECTED")
                confidence = "MEDIUM"
                notes += " Comparable pricing pattern detected → methodology."

            local_lot_id, local_bene_id = _resolve_zone_scope(
                page, run_start, run_end, lines, zone_type,
                lot_page_lookup, bene_page_lookup, known_lot_ids,
            )

            header_line = lines[run_start].strip()
            sample = [
                lines[si].strip()[:80]
                for si in range(run_start, min(len(lines), run_start + 5))
                if lines[si].strip()
            ][:4]

            zones.append({
                "zone_id": _make_zone_id(case_key, page, run_start),
                "page": page,
                "start_line_index": run_start,
                "end_line_index": run_end,
                "header_lines": [header_line],
                "sample_lines": sample,
                "detected_markers": markers,
                "zone_type": zone_type,
                "confidence": confidence,
                "zone_basis": "HEURISTIC_AMOUNT_CLUSTER",
                "local_lot_id": local_lot_id,
                "local_bene_id": local_bene_id,
                "notes": notes,
            })
            for li in range(run_start, run_end + 1):
                consumed_lines.add(li)
            i = run_end + 1
        else:
            i += 1

    return zones


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_table_zone_map(case_key: str) -> Dict[str, object]:
    """
    Build the table_zone_map artifact for the given case_key.
    Detects and classifies all table-like zones in the document.
    """
    ctx = build_context(case_key)

    hyp_fp   = ctx.artifact_dir / "structure_hypotheses.json"
    scope_fp = ctx.artifact_dir / "lot_scope_map.json"
    pages_fp = ctx.artifact_dir / "raw_pages.json"
    bene_fp  = ctx.artifact_dir / "bene_scope_map.json"

    hyp       = json.loads(hyp_fp.read_text(encoding="utf-8"))
    lot_scope = json.loads(scope_fp.read_text(encoding="utf-8"))
    raw_pages: List[Dict] = json.loads(pages_fp.read_text(encoding="utf-8"))
    bene_scope = json.loads(bene_fp.read_text(encoding="utf-8")) if bene_fp.exists() else {}

    winner = hyp.get("winner")

    out: Dict[str, object] = {
        "case_key": case_key,
        "winner": winner,
        "status": "OK",
        "table_zones": [],
        "warnings": [],
        "summary": {},
        "source_artifacts": {
            "structure_hypotheses": str(hyp_fp),
            "lot_scope_map": str(scope_fp),
            "raw_pages": str(pages_fp),
            "bene_scope_map": str(bene_fp),
        },
    }

    dst = ctx.artifact_dir / "table_zone_map.json"

    # Early exit: unreadable document
    if winner == "BLOCKED_UNREADABLE":
        out["status"] = "BLOCKED_UNREADABLE"
        out["summary"] = {
            "note": "Table zone detection blocked: document quality is BLOCKED_UNREADABLE.",
            "total_zones": 0,
        }
        dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        return out

    # Build scope lookup tables
    lot_page_lookup = _build_lot_page_lookup(lot_scope)
    bene_page_lookup = _build_bene_page_lookup(bene_scope)

    # Build known lot IDs for explicit-marker validation in zone scope resolution
    known_lot_ids: Set[str] = {
        str(s.get("lot_id", "")).strip().lower()
        for s in (lot_scope.get("lot_scopes") or [])
        if s.get("lot_id")
    }

    all_zones: List[Dict] = []
    toc_pages_skipped: List[int] = []

    for page_data in raw_pages:
        page = int(page_data["page_number"])
        text = page_data.get("text", "") or ""
        lines = text.split("\n")
        if _is_toc_page(lines):
            toc_pages_skipped.append(page)
            continue

        page_zones = _detect_zones_on_page(
            page_data=page_data,
            case_key=case_key,
            winner=winner,
            lot_page_lookup=lot_page_lookup,
            bene_page_lookup=bene_page_lookup,
            known_lot_ids=known_lot_ids,
        )
        all_zones.extend(page_zones)

    if toc_pages_skipped:
        out["warnings"].append({
            "code": "TOC_PAGES_SKIPPED",
            "pages": toc_pages_skipped,
            "reason": "Pages dominated by dot-leaders (table of contents) are skipped for zone detection.",
        })

    # Summarize by zone type
    zone_type_counts: Dict[str, int] = {}
    for z in all_zones:
        zt = z["zone_type"]
        zone_type_counts[zt] = zone_type_counts.get(zt, 0) + 1

    out["table_zones"] = all_zones
    out["summary"] = {
        "total_zones": len(all_zones),
        "by_type": zone_type_counts,
        "pages_scanned": len(raw_pages),
        "toc_pages_skipped": toc_pages_skipped,
        "authoritative_zone_count": zone_type_counts.get(ZONE_TYPE_AUTHORITATIVE, 0),
        "recap_summary_zone_count": zone_type_counts.get(ZONE_TYPE_RECAP, 0),
        "arithmetic_rollup_zone_count": zone_type_counts.get(ZONE_TYPE_ROLLUP, 0),
        "methodology_comparable_zone_count": zone_type_counts.get(ZONE_TYPE_METHODOLOGY, 0),
        "unknown_zone_count": zone_type_counts.get(ZONE_TYPE_UNKNOWN, 0),
    }

    dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


# ---------------------------------------------------------------------------
# Shared helper: zone lookup for use by other shells
# ---------------------------------------------------------------------------

def load_table_zone_map(case_key: str) -> Dict[str, object]:
    """
    Load the table_zone_map.json artifact for the given case_key.
    If not yet built, builds it first.
    """
    ctx = build_context(case_key)
    dst = ctx.artifact_dir / "table_zone_map.json"
    if not dst.exists():
        return build_table_zone_map(case_key)
    return json.loads(dst.read_text(encoding="utf-8"))


def get_zone_at(
    table_zone_map: Dict[str, object],
    page: int,
    line_index: int,
) -> Optional[Dict]:
    """
    Return the first zone that contains (page, line_index), or None.
    Useful for shells to check whether a candidate line is inside a table zone.
    """
    for zone in table_zone_map.get("table_zones") or []:
        if zone.get("page") == page:
            start = zone.get("start_line_index", 0)
            end = zone.get("end_line_index", 0)
            if start <= line_index <= end:
                return zone
    return None


def is_in_recap_or_rollup(
    table_zone_map: Dict[str, object],
    page: int,
    line_index: int,
) -> bool:
    """
    Return True if (page, line_index) falls inside a RECAP_SUMMARY_TABLE or
    ARITHMETIC_ROLLUP_TABLE zone. Used by shells to suppress or flag candidates.
    """
    zone = get_zone_at(table_zone_map, page, line_index)
    if zone is None:
        return False
    return zone.get("zone_type") in (ZONE_TYPE_RECAP, ZONE_TYPE_ROLLUP)


def get_zone_type_at(
    table_zone_map: Dict[str, object],
    page: int,
    line_index: int,
) -> Optional[str]:
    """
    Return the zone_type string at (page, line_index), or None if not in a zone.
    """
    zone = get_zone_at(table_zone_map, page, line_index)
    return zone.get("zone_type") if zone else None


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Build table_zone_map for a single case")
    parser.add_argument("--case", required=True, choices=list_case_keys())
    args = parser.parse_args()

    out = build_table_zone_map(args.case)
    print(f"CASE={args.case}")
    print(f"STATUS={out['status']}")
    print(f"TOTAL_ZONES={out['summary'].get('total_zones', 0)}")
    print(f"BY_TYPE={out['summary'].get('by_type', {})}")


if __name__ == "__main__":
    main()
