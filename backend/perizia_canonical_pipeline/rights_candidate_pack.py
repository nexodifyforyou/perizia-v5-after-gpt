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
# OCR normalisation helpers (mirrors location_candidate_pack)
# ---------------------------------------------------------------------------

def _window_text(lines: List[str], start_idx: int, window: int = 8) -> str:
    parts = []
    for raw in lines[start_idx : start_idx + window]:
        part = raw.strip()
        if part:
            parts.append(part)
    return " ".join(parts)


def _normalize_window_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


# ---------------------------------------------------------------------------
# Rights patterns
# ---------------------------------------------------------------------------

# Pattern 1 — "posto/posta in vendita per il diritto di DIRITTO (QUOTA)"
# e.g. "L'immobile viene posto in vendita per il diritto di Proprietà (1/1)"
RIGHTS_PAT_VENDITA = re.compile(
    r"(?:posto|posta)\s+in\s+vendita\s+per\s+il\s+diritto\s+di\s+"
    r"(piena\s+proprietà|nuda\s+proprietà|usufrutto|proprietà\s+superficiaria|propriet[aà])"
    r"\s*\((\d+/\d+)\)",
    re.IGNORECASE | re.UNICODE,
)

# Pattern 2 — "Diritti di DIRITTO per la quota di QUOTA"
# e.g. "Diritti di piena proprietà per la quota di 1/1 (un primo)"
RIGHTS_PAT_DIRITTI = re.compile(
    r"[Dd]iritti?\s+di\s+"
    r"(piena\s+proprietà|nuda\s+proprietà|usufrutto|proprietà\s+superficiaria)"
    r"\s+per\s+la\s+quota\s+di\s+(\d+/\d+)",
    re.IGNORECASE | re.UNICODE,
)

# Pattern 3 — "Diritto reale: DIRITTO ... Quota QUOTA"
# Handles the two-line schema riassuntivo format, which collapses to one line in
# the window text: "Diritto reale: Proprietà Quota 1/1"
RIGHTS_PAT_DIRITTO_REALE = re.compile(
    r"[Dd]iritto\s+reale\s*:\s*"
    r"(piena\s+proprietà|nuda\s+proprietà|usufrutto|proprietà)"
    r".{0,80}?[Qq]uota\s+(\d+/\d+)",
    re.IGNORECASE | re.UNICODE,
)

# Pattern 4 — "intestato/a in DIRITTO a:" (rights_diritto only; no quota)
# e.g. "Il bene risulta intestato in piena proprietà a:"
RIGHTS_PAT_INTESTATO = re.compile(
    r"intestat[oa]\s+in\s+(piena\s+proprietà|nuda\s+proprietà|usufrutto)",
    re.IGNORECASE | re.UNICODE,
)

# Trigger: any line that may contain rights information worth scanning
RIGHTS_TRIGGER_PAT = re.compile(
    r"\b(?:"
    r"diritti?\s+di\s+(?:piena\s+)?propriet[aà]"
    r"|posto\s+in\s+vendita\s+per\s+il\s+diritto"
    r"|diritto\s+reale"
    r"|intestat[oa]\s+in\s+(?:piena\s+|nuda\s+)?propriet[aà]"
    r"|intestat[oa]\s+in\s+usufrutto"
    r")\b",
    re.IGNORECASE | re.UNICODE,
)

# Procedural / mortgage context exclusion — if any of these appear in the
# 8-line lookback window, block the candidate.  The main risk is "Quota: 1/1"
# inside Formalità pregiudizievoli / Ipoteca sections, and procedural
# explanatory text that uses rights vocabulary generically.
RIGHTS_PROC_SIGNAL_PAT = re.compile(
    r"\b(?:"
    r"FORMALIT[AÀ]\s+PREGIUDIZIEVOLI"
    r"|[Ii]poteca\b"
    r"|VALUTAZIONE\s+DI\s+QUOTA\s+INDIVISA"
    r"|colpito\s+dal\s+vincolo"
    r"|diritti\s+di\s+terzi\s+sull"
    r"|comunione\s+legale"
    r"|stradella\s+di\s+penetrazione"
    r"|verbale\s+di\s+pignoramento"
    r"|[Ii]scritto\s+(?:il|al|a\s)"
    r"|Importo[:\s]"
    r")\b",
    re.IGNORECASE | re.UNICODE,
)

# ---------------------------------------------------------------------------
# Local header consistency patterns
# ---------------------------------------------------------------------------

# Standalone lot header line: "LOTTO 2", "LOTTO B", "LOTTO UNICO".
# Anchored at start+end so prose like "procedere alla formazione di un lotto unico
# cosi costituito:" does NOT match.
_LOCAL_LOT_HEADER_PAT = re.compile(r'^\s*LOTTO\s+(\S+)\s*$', re.IGNORECASE)

# Inline bene reference: "Bene N° 2", "Bene N. 1"
_LOCAL_BENE_HEADER_PAT = re.compile(r'\bBene\s+N[°\.]\s*([0-9]+)', re.IGNORECASE)


def _nearest_local_headers(
    lines: List[str], line_idx: int, lookback: int = 12
) -> Tuple[Optional[str], Optional[str]]:
    """Scan backward from line_idx (inclusive) to find the nearest explicit lot/bene
    header lines within `lookback` lines.  Returns (local_lot_id_lower, local_bene_id)
    where either may be None when not found."""
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


# Attribution priority (mirrors location pack)
_ATTR_PRIORITY = {
    "CONFIRMED": 0,
    "ATTRIBUTED_BY_SCOPE": 1,
    "LOT_LEVEL_ONLY": 2,
    "LOT_LEVEL_ONLY_PRE_BENE_CONTEXT": 3,
}
_LEDGER_SAFE_ATTRIBUTIONS = set(_ATTR_PRIORITY.keys())

# Remap cadastral block-type attribute names to rights-specific equivalents
_CADAT_TO_RIGHTS_ATTR = {
    "CADASTRAL_IN_BLOCKED_UNREADABLE": "RIGHTS_IN_BLOCKED_UNREADABLE",
    "CADASTRAL_IN_GLOBAL_PRE_LOT_ZONE": "RIGHTS_IN_GLOBAL_PRE_LOT_ZONE",
    "CADASTRAL_IN_SAME_PAGE_LOT_COLLISION": "RIGHTS_IN_SAME_PAGE_LOT_COLLISION",
    "CADASTRAL_SCOPE_AMBIGUOUS": "RIGHTS_SCOPE_AMBIGUOUS",
    "CADASTRAL_IN_SAME_PAGE_BENE_COLLISION": "RIGHTS_IN_SAME_PAGE_BENE_COLLISION",
}


# ---------------------------------------------------------------------------
# Scope helper
# ---------------------------------------------------------------------------

def _determine_rights_scope(page: int, winner: str, lut: Dict) -> Dict:
    """Wrap the cadastral scope function, remapping attribute names to RIGHTS_ prefix."""
    result = dict(_cadat_determine_scope(page, winner, lut))
    result["attribution"] = _CADAT_TO_RIGHTS_ATTR.get(result["attribution"], result["attribution"])
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


# ---------------------------------------------------------------------------
# Procedural context exclusion
# ---------------------------------------------------------------------------

def _has_proc_context(lines: List[str], line_idx: int, before: int = 8) -> bool:
    """Return True if any procedural/mortgage signal appears in the lookback window."""
    start = max(0, line_idx - before)
    for line in lines[start : line_idx + 1]:
        if RIGHTS_PROC_SIGNAL_PAT.search(line):
            return True
    return False


# ---------------------------------------------------------------------------
# Candidate ID builder
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
    return f"rts_{field_type}::{lot_part}::{bene_part}::p{page}::l{line_index}::m{idx}"


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_rights_candidate_pack(case_key: str) -> Dict:
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
            "rights_fields_present": [],
            "rights_scope_keys": [],
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
        dst = ctx.artifact_dir / "rights_candidate_pack.json"
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

        # Primary dedup: same (scope_key, diritto, quota, attribution)
        diritto_val = next((v for ft, v in fields if ft == "rights_diritto"), "")
        quota_val = next((v for ft, v in fields if ft == "rights_quota_raw"), "")
        value_dedup_key = (
            scope_key,
            diritto_val.lower(),
            quota_val.lower(),
            attribution,
        )
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
    # Patterns tried in order — first match wins
    # -----------------------------------------------------------------------
    PATTERNS = [
        (RIGHTS_PAT_DIRITTI,        "DIRITTI"),
        (RIGHTS_PAT_VENDITA,        "VENDITA"),
        (RIGHTS_PAT_DIRITTO_REALE,  "DIRITTO_REALE"),
        (RIGHTS_PAT_INTESTATO,      "INTESTATO"),
    ]

    # -----------------------------------------------------------------------
    # Body-text scanning (only source for rights — rights info is in body text)
    # -----------------------------------------------------------------------
    for page_data in raw_pages:
        page = int(page_data["page_number"])
        lines = page_data["text"].split("\n")
        scope_info = _determine_rights_scope(page, winner, lut)
        is_blocked = scope_info["blocked"]
        attribution = scope_info["attribution"]

        for line_idx, line in enumerate(lines):
            line_s = line.strip()
            if not line_s:
                continue
            if not RIGHTS_TRIGGER_PAT.search(line_s):
                continue

            # Procedural/mortgage exclusion check (8-line lookback)
            if _has_proc_context(lines, line_idx, before=8):
                _emit_blocked(
                    "RIGHTS_PROC_CONTEXT_EXCLUDED",
                    page, line_idx, line_s,
                    _normalize_window_text(_window_text(lines, max(0, line_idx - 3), 5)),
                    "BODY_TEXT",
                    scope_info["lot_id"], scope_info["bene_id"],
                    reason=(
                        "Rights trigger found but procedural/mortgage context signal "
                        "detected in 8-line lookback; candidate excluded."
                    ),
                )
                continue

            raw_win = _window_text(lines, line_idx, window=8)
            norm_win = _normalize_window_text(raw_win)

            for pat, pname in PATTERNS:
                m = pat.search(norm_win)
                if not m:
                    continue

                # Parse fields from this match
                fields: List[Tuple[str, str]] = []
                diritto_raw = m.group(1).strip()
                diritto = re.sub(r"\s+", " ", diritto_raw).lower()
                # Normalize bare "proprietà" → "piena proprietà".
                # The VENDITA pattern allows a short-form `propriet[aà]` alternative
                # that produces "proprietà", while DIRITTI produces the canonical
                # "piena proprietà" for the same right.  Normalise so both patterns
                # agree and no false RIGHTS_MULTI_VALUE_UNRESOLVED is emitted.
                if diritto == "proprietà":
                    diritto = "piena proprietà"
                fields.append(("rights_diritto", diritto))
                if pname != "INTESTATO":
                    quota = m.group(2).strip()
                    fields.append(("rights_quota_raw", quota))

                method = f"BODY_{pname}_WINDOW"
                quote = m.group(0)[:300]
                ctx_win = norm_win[m.start():][:400]
                local_lot_id, local_bene_id = _nearest_local_headers(lines, line_idx)
                effective_scope = _scope_for_local_bene(scope_info, local_bene_id, lut)
                effective_blocked = effective_scope["blocked"]
                effective_attribution = effective_scope["attribution"]

                if effective_blocked:
                    extra = {ft: fv for ft, fv in fields}
                    _emit_blocked(
                        effective_attribution,
                        page, line_idx, line_s, norm_win[:300],
                        method,
                        effective_scope["lot_id"], effective_scope["bene_id"],
                        reason=f"Rights match found but scope attribution is {effective_attribution}.",
                        extra=extra,
                    )
                else:
                    # ---------------------------------------------------------
                    # Local header consistency guard
                    # Before emitting, check whether the nearest explicit
                    # "LOTTO X" / "Bene N° Y" header in the preceding lines
                    # agrees with the attributed scope.  A mismatch means the
                    # rights text belongs to a different lot/bene than the
                    # page-level scope assigned — block it.
                    # ---------------------------------------------------------
                    attributed_lot = (effective_scope.get("lot_id") or "").lower()
                    attributed_bene = effective_scope.get("bene_id")
                    mismatch_reason: Optional[str] = None

                    if local_lot_id is not None and local_lot_id != attributed_lot:
                        mismatch_reason = (
                            f"Local explicit lot header 'LOTTO {local_lot_id.upper()}' "
                            f"disagrees with attributed lot_id "
                            f"'{effective_scope.get('lot_id')}'."
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
                            "attributed_lot_id": effective_scope.get("lot_id"),
                            "attributed_bene_id": str(attributed_bene)
                            if attributed_bene is not None else None,
                        }
                        extra_mm.update({ft: fv for ft, fv in fields})
                        _emit_blocked(
                            "RIGHTS_LOCAL_SCOPE_HEADER_MISMATCH",
                            page, line_idx, line_s, norm_win[:300],
                            method,
                            effective_scope["lot_id"], effective_scope["bene_id"],
                            reason=mismatch_reason,
                            extra=extra_mm,
                        )
                    else:
                        scope_basis = (
                            f"Rights trigger on p{page}:l{line_idx}; "
                            f"scope attribution: {effective_attribution}."
                        )
                        _emit_active(
                            fields, page, line_idx, quote, ctx_win,
                            method,
                            effective_scope["lot_id"], effective_scope["bene_id"],
                            effective_scope["composite_key"],
                            effective_attribution, scope_basis,
                        )
                break  # Only one pattern per line

    # -----------------------------------------------------------------------
    # Post-processing: intra-scope conflict detection
    # -----------------------------------------------------------------------
    def _attr_bucket(attr: str) -> str:
        if attr == "CONFIRMED":
            return "CONFIRMED"
        if attr == "ATTRIBUTED_BY_SCOPE":
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
                "type": "RIGHTS_MULTI_VALUE_UNRESOLVED",
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
                    "no synthesis applied."
                ),
            })
            final_candidates.extend(group)
        else:
            final_candidates.extend(group)

    final_candidates.sort(
        key=lambda c: (c.get("page", 0), c.get("line_index", 0), c.get("field_type", ""))
    )

    out["candidates"] = final_candidates
    out["blocked_or_ambiguous"] = blocked_or_ambiguous
    out["coverage"]["candidate_count"] = len(final_candidates)
    out["coverage"]["blocked_or_ambiguous_count"] = len(blocked_or_ambiguous)
    out["coverage"]["rights_fields_present"] = sorted({c["field_type"] for c in final_candidates})
    out["coverage"]["rights_scope_keys"] = sorted({
        c.get("composite_key") or f"lot:{c.get('lot_id', 'unknown')}"
        for c in final_candidates
    })

    # Summary per scope key
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

    dst = ctx.artifact_dir / "rights_candidate_pack.json"
    dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Harvest rights field candidates")
    parser.add_argument("--case", required=True, choices=list_case_keys())
    args = parser.parse_args()

    out = build_rights_candidate_pack(args.case)
    print(json.dumps({
        "case_key": out["case_key"],
        "status": out["status"],
        "winner": out["winner"],
        "coverage": out["coverage"],
        "blocked_or_ambiguous_count": len(out["blocked_or_ambiguous"]),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
