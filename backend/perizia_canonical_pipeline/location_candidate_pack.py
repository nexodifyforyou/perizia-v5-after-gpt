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
# OCR normalisation helpers
# ---------------------------------------------------------------------------

# "(M N)" or "(S A)" → "(MN)" or "(SA)"  — province codes split by OCR
PROV_OCR_FIX = re.compile(r"\(\s*([A-Z])\s+([A-Z])\s*\)")

# "ViaAngelo" → "Via Angelo", "PiazzaLibertà" → "Piazza Libertà"
STREET_MERGE_FIX = re.compile(
    r"\b(Via|Viale|Piazza|Piazzale|Corso|Vicolo|Largo|Strada)([A-ZÀÈÉÌÒÙ])",
    re.UNICODE,
)


def _normalize_window_text(text: str) -> str:
    text = PROV_OCR_FIX.sub(lambda m: f"({m.group(1)}{m.group(2)})", text)
    text = STREET_MERGE_FIX.sub(r"\1 \2", text)
    return re.sub(r"\s+", " ", text).strip()


def _window_text(lines: List[str], start_idx: int, window: int = 12) -> str:
    parts = []
    for raw in lines[start_idx : start_idx + window]:
        part = raw.strip()
        if part:
            parts.append(part)
    return " ".join(parts)


# ---------------------------------------------------------------------------
# Location patterns
# ---------------------------------------------------------------------------

# Pattern A — "ubicato/a a COMUNE (PROV) [–-] VIA NOME n.c./n./n° CIVICO"
# Also handles: "senza numero civico" / "snc"
UBICATO_A_PAT = re.compile(
    r"ubicat[oa]\s+a\s+"
    r"([A-Za-zÀ-öø-ÿ][A-Za-zÀ-öø-ÿ\s\.\-\'\u2019\,]+?)\s*"
    r"\(([A-Z]{2})\)\s*"
    r"[-\u2013\s]*"
    r"(via|piazza|corso|viale|vicolo|largo|strada(?:\s+[a-z]+)?|piazzale)\s+"
    r"([A-Za-zÀ-öø-ÿ][A-Za-zÀ-öø-ÿ\s\.\-\'\,\°0-9]+?)\s+"
    r"(?:"
    r"n\.c\.\s*([0-9]+)"
    r"|n\.\s*c\.\s*([0-9]+)"
    r"|n°\s*([0-9]+)"
    r"|n\.\s*([0-9]+)"
    r"|nc\.\s*([0-9]+)"
    r"|(senza\s+numero\s+civico|snc|s\.n\.c\.)"
    r")",
    re.IGNORECASE | re.UNICODE,
)

# Pattern B — "collocato/a nel comune di COMUNE (PROV) in VIA NOME n°/n. CIVICO"
COLLOCATO_PAT = re.compile(
    r"collocat[oa]\s+nel\s+comune\s+di\s+"
    r"([A-Za-zÀ-öø-ÿ][A-Za-zÀ-öø-ÿ\s\.\-\'\u2019]+?)\s*"
    r"\(([A-Z]{2})\)\s+in\s+"
    r"(via|piazza|corso|viale|vicolo|largo|strada[a-z ]*?)\s+"
    r"([A-Za-zÀ-öø-ÿ][A-Za-zÀ-öø-ÿ\s\.\-\'\u2019]+?)\s*"
    r"n[°\.\s]\s*([0-9]+)",
    re.IGNORECASE | re.UNICODE,
)

# Pattern C — "posto/posta in Comune di COMUNE (PROV) via NOME snc/n.c. CIVICO"
POSTO_IN_COMUNE_PAT = re.compile(
    r"(?:posto|posta)\s+in\s+[Cc]omune\s+di\s+"
    r"([A-Za-zÀ-öø-ÿ][A-Za-zÀ-öø-ÿ\s\.\-\'\u2019]+?)\s*"
    r"\(([A-Z]{2})\)\s+"
    r"(via|piazza|corso|viale|vicolo|largo|strada[a-z ]*?)\s+"
    r"([A-Za-zÀ-öø-ÿ][A-Za-zÀ-öø-ÿ\s\.\-\'\u2019]+?)\s*"
    r"(?:"
    r"n\.c\.\s*([0-9]+)"
    r"|n°\s*([0-9]+)"
    r"|n\.\s*([0-9]+)"
    r"|(senza\s+numero\s+civico|snc|s\.n\.c\.)"
    r")",
    re.IGNORECASE | re.UNICODE,
)

# Pattern D (bene header windows only) — "COMUNE (PROV) – VIA NOME n.c./n./n° CIVICO"
# No "ubicato a" prefix; used for summary-table style bene headers
TABLE_LOC_PAT = re.compile(
    r"([A-Za-zÀ-öø-ÿ][A-Za-zÀ-öø-ÿ\s\.\-\'\,\u2019]+?)\s*"
    r"\(([A-Z]{2})\)\s*"
    r"[-\u2013]\s*"
    r"(via|piazza|corso|viale|vicolo|largo|strada)\s+"
    r"([A-Za-zÀ-öø-ÿ][A-Za-zÀ-öø-ÿ\s\.\-\'\u2019]+?)\s+"
    r"(?:"
    r"n\.c\.\s*([0-9]+)"
    r"|n°\s*([0-9]+)"
    r"|n\.\s*([0-9]+)"
    r"|(senza\s+numero\s+civico|snc|s\.n\.c\.)"
    r")",
    re.IGNORECASE | re.UNICODE,
)

# Trigger: any line that might contain location information worth scanning
LOCATION_TRIGGER_PAT = re.compile(
    r"\b(?:ubicat[oa]|collocat[oa]|posto\s+in\s+comune|situati?\s+in|siti?\s+in)\b",
    re.IGNORECASE,
)

# Confini / boundary prose signals — if nearby context matches, exclude
CONFINI_SIGNAL_PAT = re.compile(
    r"\b(?:"
    r"confin[aie]|CONFINI"
    r"|a\s+nord\b|ad?\s+est\b|ad?\s+ovest\b|a\s+sud\b"
    r"|oltre\s+alla\s+(?:via|pubblica\s+via)"
    r"|oltre\s+al\b"
    r"|beni\s+di\s+cui\s+al\s+mappale"
    r"|per\s+salto\s+(?:sporgente|rientrante)"
    r"|salto\s+rientrante"
    r"|salto\s+sporgente"
    r")\b",
    re.IGNORECASE | re.UNICODE,
)

# Cross-bene detection: "Bene N° X" in text
BENE_REF_PAT = re.compile(r"\bBene\s+N[°\.]\s*([0-9]+)", re.IGNORECASE)

# Strip leading property-type labels from a captured comune string.
# TABLE_LOC_PAT captures everything before "(PROV) –", so bene-header lines like
# "Appartamento Roma (RM) – Via Foo 5" yield comune="Appartamento Roma".
# These prefixes are unambiguous property-category nouns that cannot start a real
# Italian comune name; strip them to recover the bare comune.
_PROPERTY_TYPE_PREFIX_PAT = re.compile(
    r"^(?:"
    r"appartament[oi]"
    r"|fabbricato(?:\s+civile)?"
    r"|magazzino"
    r"|garage"
    r"|box(?:\s+auto)?"
    r"|cantina"
    r"|ufficio"
    r"|negozio"
    r"|opificio"
    r"|capannone"
    r"|porzione\s+di\s+fabbricato(?:\s+in\s+costruzione)?"
    r")\s+",
    re.IGNORECASE | re.UNICODE,
)

# Attribution priority — mirrors cadastral pack
_ATTR_PRIORITY = {
    "CONFIRMED": 0,
    "CONFIRMED_BY_SINGLE_LOT_BENE_HEADER": 0,
    "BENE_HEADER_CONFIRMED_BY_SINGLE_LOT": 0,
    "ATTRIBUTED_BY_SCOPE": 1,
    "BENE_HEADER_ATTRIBUTED_BY_SCOPE": 1,
    "LOT_LEVEL_ONLY": 2,
    "LOT_LEVEL_ONLY_PRE_BENE_CONTEXT": 3,
}
_LEDGER_SAFE_ATTRIBUTIONS = set(_ATTR_PRIORITY.keys())

# Cadastral attribute names → location-specific names
_CADAT_TO_LOC_ATTR = {
    "CADASTRAL_IN_BLOCKED_UNREADABLE": "LOCATION_IN_BLOCKED_UNREADABLE",
    "CADASTRAL_IN_GLOBAL_PRE_LOT_ZONE": "LOCATION_IN_GLOBAL_PRE_LOT_ZONE",
    "CADASTRAL_IN_SAME_PAGE_LOT_COLLISION": "LOCATION_IN_SAME_PAGE_LOT_COLLISION",
    "CADASTRAL_SCOPE_AMBIGUOUS": "LOCATION_SCOPE_AMBIGUOUS",
    "CADASTRAL_IN_SAME_PAGE_BENE_COLLISION": "LOCATION_IN_SAME_PAGE_BENE_COLLISION",
}


# ---------------------------------------------------------------------------
# Scope helper
# ---------------------------------------------------------------------------

def _determine_location_scope(page: int, winner: str, lut: Dict) -> Dict:
    """Wrap the cadastral scope function, remapping attribute names to LOCATION_ prefix."""
    result = dict(_cadat_determine_scope(page, winner, lut))
    result["attribution"] = _CADAT_TO_LOC_ATTR.get(result["attribution"], result["attribution"])
    return result


# ---------------------------------------------------------------------------
# Pattern parsing
# ---------------------------------------------------------------------------

def _parse_match(m: re.Match, pattern_name: str) -> Optional[Dict[str, Optional[str]]]:
    """Extract atomic location fields from a regex match."""
    try:
        if pattern_name == "UBICATO_A":
            comune = m.group(1)
            prov = m.group(2)
            via_type = m.group(3)
            via_name = m.group(4)
            civico = m.group(5) or m.group(6) or m.group(7) or m.group(8) or m.group(9)
            is_snc = bool(m.group(10))

        elif pattern_name == "COLLOCATO":
            comune = m.group(1)
            prov = m.group(2)
            via_type = m.group(3)
            via_name = m.group(4)
            civico = m.group(5)
            is_snc = False

        elif pattern_name == "POSTO_IN_COMUNE":
            comune = m.group(1)
            prov = m.group(2)
            via_type = m.group(3)
            via_name = m.group(4)
            civico = m.group(5) or m.group(6) or m.group(7)
            is_snc = bool(m.group(8))

        elif pattern_name == "TABLE_LOC":
            comune = m.group(1)
            prov = m.group(2)
            via_type = m.group(3)
            via_name = m.group(4)
            civico = m.group(5) or m.group(6) or m.group(7)
            is_snc = bool(m.group(8))

        else:
            return None

        comune = re.sub(r"\s+", " ", comune.strip()).strip(".,")
        # Strip leading property-type labels (e.g. "Appartamento Roma" → "Roma")
        comune = _PROPERTY_TYPE_PREFIX_PAT.sub("", comune).strip()
        if not comune:
            return None
        prov = prov.strip().upper()
        via_name = re.sub(r"\s+", " ", via_name.strip()).strip(".,")
        via_full = f"{via_type.capitalize()} {via_name}"

        return {
            "comune": comune,
            "provincia": prov,
            "via": via_full,
            "civico": civico.strip() if civico else None,
            "civico_raw": ("senza numero civico" if is_snc else (civico.strip() if civico else None)),
        }
    except (IndexError, AttributeError):
        return None


# ---------------------------------------------------------------------------
# Confini exclusion check
# ---------------------------------------------------------------------------

def _has_confini_context(lines: List[str], line_idx: int, before: int = 8) -> bool:
    start = max(0, line_idx - before)
    for line in lines[start : line_idx + 1]:
        if CONFINI_SIGNAL_PAT.search(line):
            return True
    return False


# ---------------------------------------------------------------------------
# Candidate ID builder
# ---------------------------------------------------------------------------

def _make_cid(field_type: str, lot_id: Optional[str], bene_id: Optional[str],
               page: int, line_index: int, idx: int) -> str:
    lot_part = lot_id or "unknown"
    bene_part = bene_id or "na"
    return f"loc_{field_type}::{lot_part}::{bene_part}::p{page}::l{line_index}::m{idx}"


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_location_candidate_pack(case_key: str) -> Dict:
    ctx = build_context(case_key)
    bsm = build_bene_scope_map(case_key)

    hyp_fp = ctx.artifact_dir / "structure_hypotheses.json"
    scope_fp = ctx.artifact_dir / "lot_scope_map.json"
    pages_fp = ctx.artifact_dir / "raw_pages.json"
    bene_spine_fp = ctx.artifact_dir / "bene_header_spine.json"

    hyp = json.loads(hyp_fp.read_text(encoding="utf-8"))
    scope = json.loads(scope_fp.read_text(encoding="utf-8"))
    raw_pages: List[Dict] = json.loads(pages_fp.read_text(encoding="utf-8"))
    bene_spine = json.loads(bene_spine_fp.read_text(encoding="utf-8"))

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
            "location_fields_present": [],
            "location_scope_keys": [],
        },
        "source_artifacts": {
            "structure_hypotheses": str(hyp_fp),
            "lot_scope_map": str(scope_fp),
            "bene_scope_map": str(ctx.artifact_dir / "bene_scope_map.json"),
            "bene_header_spine": str(bene_spine_fp),
            "raw_pages": str(pages_fp),
        },
    }

    if winner == "BLOCKED_UNREADABLE":
        out["status"] = "BLOCKED_UNREADABLE"
        dst = ctx.artifact_dir / "location_candidate_pack.json"
        dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        return out

    lut = _build_lookup_tables(scope, bsm)
    page_index: Dict[int, Dict] = {p["page_number"]: p for p in raw_pages}

    candidates: List[Dict] = []
    blocked_or_ambiguous: List[Dict] = []
    seen_dedup: set = set()  # (page, line_idx, pattern_name, comune_norm, via_norm)
    match_counter = [0]

    def _next_idx() -> int:
        match_counter[0] += 1
        return match_counter[0]

    # -----------------------------------------------------------------------
    # Emit helpers
    # -----------------------------------------------------------------------

    def _emit_active(
        parsed: Dict,
        page: int,
        line_index: int,
        quote: str,
        context_window: str,
        extraction_method: str,
        lot_id: Optional[str],
        bene_id: Optional[str],
        composite_key: Optional[str],
        attribution: str,
        scope_attribution_mode: str,
        scope_basis: str,
        source_type: str,
    ) -> None:
        scope_key = composite_key or f"lot:{lot_id or 'unknown'}"
        # Primary dedup: same (scope_key, comune, via, civico, attribution) — prevents
        # TOC-repeat lines across many pages from flooding the candidate list.
        # Secondary dedup: same (page, line_idx, method) — prevents double-firing.
        value_dedup_key = (
            scope_key,
            (parsed.get("comune") or "").lower(),
            (parsed.get("via") or "").lower(),
            (parsed.get("civico") or "").lower(),
            attribution,
        )
        page_dedup_key = (page, line_index, extraction_method)
        if value_dedup_key in seen_dedup or page_dedup_key in seen_dedup:
            return
        seen_dedup.add(value_dedup_key)
        seen_dedup.add(page_dedup_key)

        fields: List[Tuple[str, str]] = []
        if parsed.get("comune"):
            fields.append(("location_comune", parsed["comune"]))
        if parsed.get("via"):
            fields.append(("location_via", parsed["via"]))
        if parsed.get("civico"):
            fields.append(("location_civico", parsed["civico"]))
        if parsed.get("provincia"):
            fields.append(("location_provincia", parsed["provincia"]))

        if not fields:
            return

        idx = _next_idx()
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
                "scope_attribution_mode": scope_attribution_mode,
                "candidate_status": "ACTIVE",
                "source_type": source_type,
                "sibling_fields": {ft: fv for ft, fv in fields if ft != field_type},
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
    # Patterns tried in body-text context (no TABLE_LOC — too broad)
    # -----------------------------------------------------------------------
    BODY_PATTERNS = [
        (UBICATO_A_PAT, "UBICATO_A"),
        (COLLOCATO_PAT, "COLLOCATO"),
        (POSTO_IN_COMUNE_PAT, "POSTO_IN_COMUNE"),
    ]

    # Patterns tried in bene-header window context (includes TABLE_LOC)
    HEADER_PATTERNS = [
        (UBICATO_A_PAT, "UBICATO_A"),
        (TABLE_LOC_PAT, "TABLE_LOC"),
        (COLLOCATO_PAT, "COLLOCATO"),
        (POSTO_IN_COMUNE_PAT, "POSTO_IN_COMUNE"),
    ]

    # -----------------------------------------------------------------------
    # Source 1 — Bene header windows
    # -----------------------------------------------------------------------
    bene_spine_rows = bene_spine.get("bene_header_spine", []) or []

    for brow in bene_spine_rows:
        page = int(brow["first_header_page"])
        line_idx = brow.get("first_header_line_index")
        composite_key = str(brow["composite_key"])
        bene_id = str(brow["bene_id"])
        lot_id_bene = str(brow["lot_id"])
        bene_attr = str(brow.get("attribution", "ATTRIBUTED_BY_SCOPE"))

        if not isinstance(line_idx, int):
            continue
        page_data = page_index.get(page)
        if not page_data:
            continue

        lines_h = page_data["text"].split("\n")
        # The spine stores 1-based line indices (plurality_headers uses enumerate(start=1)).
        # Convert to 0-based for array access; the stored line_idx is preserved for
        # packet IDs to remain consistent with bene_header_spine references.
        actual_idx = max(0, line_idx - 1)
        raw_win = _window_text(lines_h, actual_idx, window=12)
        norm_win = _normalize_window_text(raw_win)
        line_quote = lines_h[actual_idx].strip() if actual_idx < len(lines_h) else ""

        # For ATTRIBUTED_BY_SCOPE benes, bene-header window extraction is risky
        # (multi-lot valuation tables may reference other lots' benes).
        # Emit a warning but still attempt; mark attribution accordingly.
        if bene_attr == "ATTRIBUTED_BY_SCOPE":
            out["warnings"].append(
                f"Bene {composite_key} is ATTRIBUTED_BY_SCOPE: bene-header location "
                f"extraction at p{page}:l{line_idx} is provisional and may reflect "
                f"cross-lot stima-section content."
            )

        # Determine attribution label for bene header source
        if bene_attr == "CONFIRMED_BY_SINGLE_LOT":
            attr_label = "BENE_HEADER_CONFIRMED_BY_SINGLE_LOT"
        else:
            attr_label = f"BENE_HEADER_{bene_attr}"

        scope_basis = (
            f"Location extracted from bene header window at p{page}:l{line_idx}; "
            f"bene {composite_key} attribution={bene_attr}."
        )

        found_any = False
        for pat, pname in HEADER_PATTERNS:
            m = pat.search(norm_win)
            if not m:
                continue
            parsed = _parse_match(m, pname)
            if not parsed:
                continue

            # Cross-bene guard for ATTRIBUTED_BY_SCOPE: if window references a "Bene N° X"
            # that does NOT match the current bene_id, this is a cross-bene window.
            bene_ref = BENE_REF_PAT.search(norm_win[:300])
            if bene_ref:
                ref_num = bene_ref.group(1).strip()
                if ref_num != bene_id and bene_attr == "ATTRIBUTED_BY_SCOPE":
                    _emit_blocked(
                        "LOCATION_CROSS_LOT_OR_CROSS_BENE_TEXT",
                        page, line_idx, line_quote, norm_win[:300],
                        f"BENE_HEADER_{pname}_WINDOW",
                        lot_id_bene, bene_id,
                        reason=(
                            f"Bene header window for {composite_key} references "
                            f"Bene N° {ref_num}; attribution is ATTRIBUTED_BY_SCOPE "
                            f"— cross-lot stima contamination likely."
                        ),
                    )
                    found_any = True
                    break

            # Narrow quote/context to the matched location span, stripping any
            # leading property-type label captured by TABLE_LOC_PAT before the
            # actual comune name (e.g. "Fabbricato civile Montecatini-Terme" →
            # "Montecatini-Terme (PT) – via ...").  Provenance (page, line_index)
            # is preserved; only the evidence text is narrowed.
            _match_quote = _PROPERTY_TYPE_PREFIX_PAT.sub("", m.group(0))
            _match_ctx = _PROPERTY_TYPE_PREFIX_PAT.sub("", norm_win[m.start():])
            _emit_active(
                parsed, page, line_idx, _match_quote, _match_ctx,
                f"BENE_HEADER_{pname}_WINDOW",
                lot_id_bene, bene_id, composite_key,
                attr_label, "BENE_LEVEL_FROM_HEADER", scope_basis,
                source_type="BENE_HEADER",
            )
            found_any = True
            break

    # -----------------------------------------------------------------------
    # Source 2 — Body-text scanning
    # -----------------------------------------------------------------------
    for page_data in raw_pages:
        page = int(page_data["page_number"])
        lines_b = page_data["text"].split("\n")
        scope_info = _determine_location_scope(page, winner, lut)

        for line_idx, line in enumerate(lines_b):
            line_s = line.strip()
            if not line_s:
                continue
            if not LOCATION_TRIGGER_PAT.search(line_s):
                continue

            # Check confini exclusion first
            if _has_confini_context(lines_b, line_idx, before=8):
                _emit_blocked(
                    "LOCATION_BOUNDARY_PROSE_EXCLUDED",
                    page, line_idx, line_s,
                    _window_text(lines_b, max(0, line_idx - 3), 5),
                    "BODY_TEXT",
                    scope_info["lot_id"], scope_info["bene_id"],
                    reason="Location trigger in confini/boundary prose context.",
                )
                continue

            raw_win_b = _window_text(lines_b, line_idx, window=10)
            norm_win_b = _normalize_window_text(raw_win_b)

            # If scope is blocked, try matching anyway but emit to blocked_or_ambiguous
            is_blocked = scope_info["blocked"]
            attribution = scope_info["attribution"]

            # Cross-bene detection for body text within a known bene scope
            current_bene_id = scope_info.get("bene_id")
            bene_ref_b = BENE_REF_PAT.search(norm_win_b[:250])
            if bene_ref_b and current_bene_id is not None:
                ref_num_b = bene_ref_b.group(1).strip()
                if ref_num_b != current_bene_id:
                    _emit_blocked(
                        "LOCATION_CROSS_LOT_OR_CROSS_BENE_TEXT",
                        page, line_idx, line_s, norm_win_b[:300],
                        "BODY_TEXT",
                        scope_info["lot_id"], current_bene_id,
                        reason=(
                            f"Body text references Bene N° {ref_num_b} but page is in "
                            f"scope of bene {current_bene_id}; potential cross-bene text."
                        ),
                    )
                    continue

            for pat_b, pname_b in BODY_PATTERNS:
                m_b = pat_b.search(norm_win_b)
                if not m_b:
                    continue
                parsed_b = _parse_match(m_b, pname_b)
                if not parsed_b:
                    continue

                method_b = f"BODY_{pname_b}_WINDOW"

                if is_blocked:
                    _emit_blocked(
                        attribution,
                        page, line_idx, line_s, norm_win_b[:300],
                        method_b,
                        scope_info["lot_id"], scope_info["bene_id"],
                        reason=f"Location match found but scope attribution is {attribution}.",
                        extra={
                            "parsed_comune": parsed_b.get("comune"),
                            "parsed_via": parsed_b.get("via"),
                            "parsed_civico": parsed_b.get("civico"),
                            "parsed_provincia": parsed_b.get("provincia"),
                        },
                    )
                else:
                    scope_basis_b = (
                        f"Body text location trigger on p{page}:l{line_idx}; "
                        f"scope attribution: {attribution}."
                    )
                    # Narrow evidence text to the matched span; strip any
                    # property-type prefix that may precede the location match.
                    _match_quote_b = _PROPERTY_TYPE_PREFIX_PAT.sub("", m_b.group(0))
                    _match_ctx_b = _PROPERTY_TYPE_PREFIX_PAT.sub("", norm_win_b[m_b.start():])
                    _emit_active(
                        parsed_b, page, line_idx, _match_quote_b, _match_ctx_b,
                        method_b,
                        scope_info["lot_id"], scope_info["bene_id"], scope_info["composite_key"],
                        attribution, attribution, scope_basis_b,
                        source_type="BODY_TEXT",
                    )
                break  # Only one pattern per line

    # -----------------------------------------------------------------------
    # Post-processing: conflict detection
    # -----------------------------------------------------------------------
    # Group active candidates by (scope_key, field_type, attribution_bucket)
    # Attribution bucket: reduce to priority tier for grouping
    def _attr_bucket(attr: str) -> str:
        if attr in {"CONFIRMED", "BENE_HEADER_CONFIRMED_BY_SINGLE_LOT",
                    "CONFIRMED_BY_SINGLE_LOT_BENE_HEADER"}:
            return "CONFIRMED"
        if attr in {"ATTRIBUTED_BY_SCOPE", "BENE_HEADER_ATTRIBUTED_BY_SCOPE"}:
            return "ATTRIBUTED"
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
        distinct_vals = sorted({str(c["extracted_value"]) for c in group})
        if len(distinct_vals) > 1:
            blocked_or_ambiguous.append({
                "type": "LOCATION_MULTI_VALUE_UNRESOLVED",
                "scope_key": scope_key,
                "field_type": field_type,
                "attribution_bucket": bucket,
                "distinct_values": distinct_vals,
                "candidate_count": len(group),
                "candidates": [
                    {k: c.get(k) for k in
                     ("candidate_id", "extracted_value", "page", "line_index",
                      "attribution", "source_type")}
                    for c in group
                ],
                "reason": (
                    f"Multiple distinct {field_type} values for scope {scope_key}; "
                    "no polishing or synthesis applied."
                ),
            })
            # Still emit all candidates — do NOT silently collapse
            final_candidates.extend(group)
        else:
            final_candidates.extend(group)

    # Sort by page, line_index
    final_candidates.sort(key=lambda c: (c.get("page", 0), c.get("line_index", 0), c.get("field_type", "")))

    out["candidates"] = final_candidates
    out["blocked_or_ambiguous"] = blocked_or_ambiguous
    out["coverage"]["candidate_count"] = len(final_candidates)
    out["coverage"]["blocked_or_ambiguous_count"] = len(blocked_or_ambiguous)
    out["coverage"]["location_fields_present"] = sorted({c["field_type"] for c in final_candidates})
    out["coverage"]["location_scope_keys"] = sorted({
        c.get("composite_key") or f"lot:{c.get('lot_id', 'unknown')}"
        for c in final_candidates
    })

    # Build summary per scope key
    summary: Dict[str, Dict] = {}
    for c in final_candidates:
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

    dst = ctx.artifact_dir / "location_candidate_pack.json"
    dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Harvest location field candidates")
    parser.add_argument("--case", required=True, choices=list_case_keys())
    args = parser.parse_args()

    out = build_location_candidate_pack(args.case)
    print(json.dumps({
        "case_key": out["case_key"],
        "status": out["status"],
        "winner": out["winner"],
        "coverage": out["coverage"],
        "blocked_or_ambiguous_count": len(out["blocked_or_ambiguous"]),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
