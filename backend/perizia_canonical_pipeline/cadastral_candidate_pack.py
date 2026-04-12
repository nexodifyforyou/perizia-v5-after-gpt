from __future__ import annotations

import argparse
import json
import re
from typing import Dict, List, Optional

from .runner import build_context
from .corpus_registry import load_cases, list_case_keys
from .bene_scope_map import build_bene_scope_map


# ---------------------------------------------------------------------------
# Cadastral inline pattern (CADAT_PAT)
# Captures: (foglio, mappale, sub_optional, categoria_optional)
# Handles: "Foglio 20, Particella 433, Sub. 301, Categoria A/10"
#          "Fg. 6, Part. 1700, Sub. 5, Categoria C2"
#          "Fg 242 Mapp. 301 Sub. 516"
# ---------------------------------------------------------------------------
CADAT_PAT = re.compile(
    r"(?:Foglio|Fg\.?)\s+(\d+)"
    r"[,\s]+"
    r"(?:Particella|Part(?:icella)?\.?|P\.lla\s*|Mapp(?:ale)?\.?)\s+(\d+)"
    r"(?:\s*\([^)]*\))?"
    r"(?:[,\s]+Sub(?:alterno)?\.?\s+(\d+))?"
    r"(?:\s*\([^)]*\))?"
    r"(?:[,\s]+(?:Zc\.?|Zona\s+Censuaria)\s+[\w\d]+)?"
    r"(?:[,\s]+(?:Categoria|Categ\.?|Cat\.?)\s+([\w/]+))?",
    re.IGNORECASE,
)

# Used to detect multiple sub references on a single line (multi-value signal).
SUB_PAT = re.compile(r"\bSub(?:alterno)?\.?\s+\d+", re.IGNORECASE)
FOGLIO_ANCHOR_PAT = re.compile(r"\b(?:Foglio|Fg\.?)\b", re.IGNORECASE)
FOGLIO_LABEL_LINE_PAT = re.compile(r"^\s*(?:[-:•▪]\s*)?(?:Foglio|Fg\.?)\b", re.IGNORECASE)
FOGLIO_VALUE_PAT = re.compile(r"\b(?:Foglio|Fg\.?)\s+(\d+)\b", re.IGNORECASE)
FRAGMENT_MAPPALE_PAT = re.compile(
    r"\b(?:Particella|Part(?:icella)?\.?|P\.lla\s*|Mapp(?:ale)?\.?)\s+(\d+)"
    r"(?:\s+Sub(?:alterno)?\.?\s*(\d+))?"
    r"(?:[,\s]+(?:Categoria|Categ\.?|Cat\.?)\s+([\w/]+))?",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Scope helpers
# ---------------------------------------------------------------------------

def _build_lookup_tables(scope: Dict, bsm: Dict) -> Dict:
    """Pre-compute lookup tables from lot_scope_map + bene_scope_map."""
    pre_lot_pages: set = set()
    gplz = scope.get("global_pre_lot_zone") or {}
    for p in gplz.get("pages", []):
        pre_lot_pages.add(int(p))

    collision_pages: set = set()
    for c in (scope.get("same_page_collisions") or []):
        collision_pages.add(int(c["page"]))

    lot_scopes: List[Dict] = scope.get("lot_scopes") or []

    # bene_pre_header_zone: {lot_id: set of pages}
    bene_pre_zones: Dict[str, set] = {}
    for z in (bsm.get("bene_pre_header_zones") or []):
        lid = str(z["lot_id"])
        bene_pre_zones.setdefault(lid, set()).update(int(p) for p in z.get("pages", []))

    # bene_scopes per lot: {lot_id: [bene_scope, ...]}  (already sorted by scope order)
    bene_by_lot: Dict[str, List[Dict]] = {}
    for bs in (bsm.get("bene_scopes") or []):
        lid = str(bs["lot_id"])
        bene_by_lot.setdefault(lid, []).append(bs)

    # same_page_bene_collisions: {(lot_id, page)}
    bene_collision_keys: set = set()
    for c in (bsm.get("same_page_bene_collisions") or []):
        bene_collision_keys.add((str(c["lot_id"]), int(c["page"])))

    return {
        "pre_lot_pages": pre_lot_pages,
        "collision_pages": collision_pages,
        "lot_scopes": lot_scopes,
        "bene_pre_zones": bene_pre_zones,
        "bene_by_lot": bene_by_lot,
        "bene_collision_keys": bene_collision_keys,
    }


def _determine_scope(
    page: int,
    winner: str,
    lut: Dict,
) -> Dict:
    """
    Return scope attribution for a single page.

    Return dict:
      attribution  — one of: CONFIRMED, ATTRIBUTED_BY_SCOPE,
                    LOT_LEVEL_ONLY, LOT_LEVEL_ONLY_PRE_BENE_CONTEXT,
                    CADASTRAL_IN_GLOBAL_PRE_LOT_ZONE,
                    CADASTRAL_IN_SAME_PAGE_LOT_COLLISION,
                    CADASTRAL_IN_SAME_PAGE_BENE_COLLISION,
                    CADASTRAL_SCOPE_AMBIGUOUS
      blocked      — bool (attribution is one of the CADASTRAL_IN_* or AMBIGUOUS types)
      lot_id       — str or None
      bene_id      — str or None
      composite_key — str or None
    """
    def _r(attribution: str, blocked: bool, lot_id=None, bene_id=None, ck=None):
        return {
            "attribution": attribution,
            "blocked": blocked,
            "lot_id": lot_id,
            "bene_id": bene_id,
            "composite_key": ck,
        }

    if winner == "BLOCKED_UNREADABLE":
        return _r("CADASTRAL_IN_BLOCKED_UNREADABLE", blocked=True)

    if page in lut["pre_lot_pages"]:
        return _r("CADASTRAL_IN_GLOBAL_PRE_LOT_ZONE", blocked=True)

    if page in lut["collision_pages"]:
        return _r("CADASTRAL_IN_SAME_PAGE_LOT_COLLISION", blocked=True)

    # Find the lot scope that contains this page.
    matched_lot: Optional[str] = None
    for ls in lut["lot_scopes"]:
        if int(ls["start_page"]) <= page <= int(ls["end_page"]):
            matched_lot = str(ls["lot_id"])
            break

    if matched_lot is None:
        return _r("CADASTRAL_SCOPE_AMBIGUOUS", blocked=True)

    # Is this page in the bene pre-header zone for this lot?
    if page in lut["bene_pre_zones"].get(matched_lot, set()):
        return _r("LOT_LEVEL_ONLY_PRE_BENE_CONTEXT", blocked=False, lot_id=matched_lot)

    benes_in_lot = lut["bene_by_lot"].get(matched_lot, [])

    if not benes_in_lot:
        # No bene structure → lot-level attribution only.
        return _r("LOT_LEVEL_ONLY", blocked=False, lot_id=matched_lot)

    # Same-page bene collision for this lot+page?
    if (matched_lot, page) in lut["bene_collision_keys"]:
        return _r("CADASTRAL_IN_SAME_PAGE_BENE_COLLISION", blocked=True, lot_id=matched_lot)

    # Find which bene scope contains this page.
    matched_bene: Optional[Dict] = None
    for bs in benes_in_lot:
        if int(bs["start_page"]) <= page <= int(bs["end_page"]):
            matched_bene = bs
            break

    if matched_bene is None:
        # Page is inside the lot but not covered by any bene scope.
        return _r("LOT_LEVEL_ONLY", blocked=False, lot_id=matched_lot)

    bene_id = str(matched_bene["bene_id"])
    ck = str(matched_bene["composite_key"])
    attribution = str(matched_bene.get("attribution", "ATTRIBUTED_BY_SCOPE"))

    # Last-bene navigation fallback: the last bene in a multi-bene lot has its
    # scope extended to end-of-lot for navigation purposes only, not as evidence
    # ownership.  We must not attribute cadastral evidence on those pages to it.
    if len(benes_in_lot) > 1 and matched_bene is benes_in_lot[-1]:
        return _r("LOT_LEVEL_ONLY", blocked=False, lot_id=matched_lot)

    if attribution == "CONFIRMED_BY_SINGLE_LOT":
        return _r("CONFIRMED", blocked=False, lot_id=matched_lot, bene_id=bene_id, ck=ck)

    # ATTRIBUTED_BY_SCOPE or any other attribution from the bene scope.
    return _r("ATTRIBUTED_BY_SCOPE", blocked=False, lot_id=matched_lot, bene_id=bene_id, ck=ck)


def _make_candidate_id(
    field_type: str,
    lot_id: Optional[str],
    bene_id: Optional[str],
    page: int,
    line_index: int,
    match_index: int,
) -> str:
    lot_part = lot_id or "unknown"
    bene_part = bene_id or "na"
    return f"{field_type}::{lot_part}::{bene_part}::p{page}::l{line_index}::m{match_index}"


def _fragmented_window_text(lines: List[str], start_idx: int, window: int = 18) -> str:
    parts: List[str] = []
    for raw in lines[start_idx:start_idx + window]:
        part = raw.strip()
        if not part or part in {"-", ":"}:
            continue
        parts.append(part)

    text = " ".join(parts)
    # OCR often splits "sub.501" as "sub.50" + "1" on the next line.
    text = re.sub(
        r"\bSub(?:alterno)?\.?\s*(\d{1,2})\s+(\d)\b(?=\s*(?:,|mappale|particella|categoria|classe|$))",
        r"Sub. \1\2",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"\b((?:Particella|Part(?:icella)?\.?|P\.lla\s*|Mapp(?:ale)?\.?)\s+)(\d{1,2})\s+(\d)\b"
        r"(?=\s*(?:,|sub|categoria|classe|ha\.?|ente|$))",
        r"\1\2\3",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"\b([A-Z])/\s+(\d+)\b", r"\1/\2", text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", text).strip()


def _find_fragmented_cadastral_matches(lines: List[str], start_idx: int) -> List[Dict[str, Optional[str]]]:
    if not FOGLIO_LABEL_LINE_PAT.search(lines[start_idx]):
        return []

    text = _fragmented_window_text(lines, start_idx)
    foglio_match = FOGLIO_VALUE_PAT.search(text)
    if not foglio_match:
        return []

    foglio = foglio_match.group(1)
    tail = text[foglio_match.end():]
    next_foglio = FOGLIO_ANCHOR_PAT.search(tail)
    if next_foglio:
        tail = tail[:next_foglio.start()]

    matches: List[Dict[str, Optional[str]]] = []
    for m in FRAGMENT_MAPPALE_PAT.finditer(tail):
        matches.append({
            "foglio": foglio,
            "mappale": m.group(1),
            "subalterno": m.group(2) or None,
            "categoria": m.group(3) or None,
        })
    return matches


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_cadastral_candidate_pack(case_key: str) -> Dict[str, object]:
    ctx = build_context(case_key)

    # Ensure bene_scope_map is fresh (idempotent).
    bsm = build_bene_scope_map(case_key)

    hyp_fp = ctx.artifact_dir / "structure_hypotheses.json"
    scope_fp = ctx.artifact_dir / "lot_scope_map.json"
    pages_fp = ctx.artifact_dir / "raw_pages.json"

    hyp = json.loads(hyp_fp.read_text(encoding="utf-8"))
    scope = json.loads(scope_fp.read_text(encoding="utf-8"))
    raw_pages: List[Dict] = json.loads(pages_fp.read_text(encoding="utf-8"))

    winner = hyp.get("winner")

    out: Dict[str, object] = {
        "case_key": case_key,
        "winner": winner,
        "status": "OK",
        "extraction_method": "REGEX_CADAT_INLINE",
        "candidates": [],
        "blocked_or_ambiguous": [],
        "coverage": {
            "pages_scanned": len(raw_pages),
            "lines_with_matches": 0,
            "candidate_count": 0,
            "blocked_or_ambiguous_count": 0,
        },
        "warnings": [],
        "source_artifacts": {
            "structure_hypotheses": str(hyp_fp),
            "lot_scope_map": str(scope_fp),
            "bene_scope_map": str(ctx.artifact_dir / "bene_scope_map.json"),
            "raw_pages": str(pages_fp),
        },
    }

    if winner == "BLOCKED_UNREADABLE":
        out["status"] = "BLOCKED_UNREADABLE"
        dst = ctx.artifact_dir / "cadastral_candidate_pack.json"
        dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        return out

    lut = _build_lookup_tables(scope, bsm)
    lines_with_matches = 0
    candidates: List[Dict] = []
    blocked_or_ambiguous: List[Dict] = []

    for page_data in raw_pages:
        page = int(page_data["page_number"])
        lines = page_data["text"].split("\n")
        scope_info = _determine_scope(page, winner, lut)

        for line_idx, line in enumerate(lines):
            matches = CADAT_PAT.findall(line)
            fragmented_matches: List[Dict[str, Optional[str]]] = []
            fragmented_quote = ""
            if not matches and FOGLIO_ANCHOR_PAT.search(line):
                fragmented_matches = _find_fragmented_cadastral_matches(lines, line_idx)
                if fragmented_matches:
                    fragmented_quote = _fragmented_window_text(lines, line_idx)[:300]
            if not matches:
                if fragmented_matches:
                    lines_with_matches += 1

                    if len(fragmented_matches) > 1:
                        blocked_or_ambiguous.append({
                            "type": "CADASTRAL_MULTI_VALUE_UNRESOLVED",
                            "page": page,
                            "line_index": line_idx,
                            "line_quote": fragmented_quote,
                            "match_count": len(fragmented_matches),
                            "matches": fragmented_matches,
                            "scope_attribution": scope_info["attribution"],
                            "lot_id": scope_info["lot_id"],
                            "bene_id": scope_info["bene_id"],
                            "composite_key": scope_info["composite_key"],
                            "extraction_method": "REGEX_CADAT_FRAGMENTED",
                        })
                        continue

                    match = fragmented_matches[0]
                    if scope_info["blocked"]:
                        blocked_or_ambiguous.append({
                            "type": scope_info["attribution"],
                            "page": page,
                            "line_index": line_idx,
                            "line_quote": fragmented_quote,
                            "foglio": match["foglio"],
                            "mappale": match["mappale"],
                            "subalterno": match["subalterno"],
                            "categoria": match["categoria"],
                            "lot_id": scope_info["lot_id"],
                            "bene_id": scope_info["bene_id"],
                            "composite_key": scope_info["composite_key"],
                            "extraction_method": "REGEX_CADAT_FRAGMENTED",
                        })
                        continue

                    field_values = [
                        ("cadastral_foglio", match["foglio"]),
                        ("cadastral_mappale", match["mappale"]),
                        ("cadastral_subalterno", match["subalterno"]),
                        ("cadastral_categoria", match["categoria"]),
                    ]
                    for field_idx, (field_type, value) in enumerate(field_values):
                        if not value:
                            continue
                        candidates.append({
                            "candidate_id": _make_candidate_id(
                                field_type, scope_info["lot_id"], scope_info["bene_id"],
                                page, line_idx, field_idx,
                            ),
                            "field_type": field_type,
                            "extracted_value": value,
                            "page": page,
                            "line_index": line_idx,
                            "line_quote": fragmented_quote,
                            "lot_id": scope_info["lot_id"],
                            "bene_id": scope_info["bene_id"],
                            "composite_key": scope_info["composite_key"],
                            "scope_attribution": scope_info["attribution"],
                            "extraction_method": "REGEX_CADAT_FRAGMENTED",
                            "sibling_fields": {
                                ft: fv
                                for ft, fv in field_values
                                if fv and ft != field_type
                            },
                        })
                    continue
                continue

            lines_with_matches += 1

            # Multi-value detection: >1 CADAT match OR a single CADAT match with >1 Sub references.
            sub_count = len(SUB_PAT.findall(line))
            is_multi = len(matches) > 1 or (len(matches) == 1 and sub_count > 1)

            if is_multi:
                blocked_or_ambiguous.append({
                    "type": "CADASTRAL_MULTI_VALUE_UNRESOLVED",
                    "page": page,
                    "line_index": line_idx,
                    "line_quote": line[:300],
                    "match_count": len(matches),
                    "matches": [
                        {"foglio": m[0], "mappale": m[1], "subalterno": m[2] or None, "categoria": m[3] or None}
                        for m in matches
                    ],
                    "scope_attribution": scope_info["attribution"],
                    "lot_id": scope_info["lot_id"],
                    "bene_id": scope_info["bene_id"],
                    "composite_key": scope_info["composite_key"],
                })
                continue

            # Single match — parse fields.
            m = matches[0]
            foglio, mappale, sub, cat = m[0], m[1], m[2] or None, m[3] or None

            if scope_info["blocked"]:
                blocked_or_ambiguous.append({
                    "type": scope_info["attribution"],
                    "page": page,
                    "line_index": line_idx,
                    "line_quote": line[:300],
                    "foglio": foglio,
                    "mappale": mappale,
                    "subalterno": sub,
                    "categoria": cat,
                    "lot_id": scope_info["lot_id"],
                    "bene_id": scope_info["bene_id"],
                    "composite_key": scope_info["composite_key"],
                })
                continue

            # Safe candidate: emit one record per parsed field (non-empty values only).
            field_values = [
                ("cadastral_foglio", foglio),
                ("cadastral_mappale", mappale),
                ("cadastral_subalterno", sub),
                ("cadastral_categoria", cat),
            ]
            for field_idx, (field_type, value) in enumerate(field_values):
                if not value:
                    continue
                candidates.append({
                    "candidate_id": _make_candidate_id(
                        field_type, scope_info["lot_id"], scope_info["bene_id"],
                        page, line_idx, field_idx,
                    ),
                    "field_type": field_type,
                    "extracted_value": value,
                    "page": page,
                    "line_index": line_idx,
                    "line_quote": line[:300],
                    "lot_id": scope_info["lot_id"],
                    "bene_id": scope_info["bene_id"],
                    "composite_key": scope_info["composite_key"],
                    "scope_attribution": scope_info["attribution"],
                    "extraction_method": "REGEX_CADAT_INLINE",
                    "sibling_fields": {
                        ft: fv
                        for ft, fv in field_values
                        if fv and ft != field_type
                    },
                })

    out["candidates"] = candidates
    out["blocked_or_ambiguous"] = blocked_or_ambiguous
    out["coverage"]["lines_with_matches"] = lines_with_matches
    out["coverage"]["candidate_count"] = len(candidates)
    out["coverage"]["blocked_or_ambiguous_count"] = len(blocked_or_ambiguous)

    dst = ctx.artifact_dir / "cadastral_candidate_pack.json"
    dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Harvest cadastral inline candidates")
    parser.add_argument("--case", required=True, choices=list_case_keys())
    args = parser.parse_args()

    out = build_cadastral_candidate_pack(args.case)
    print(json.dumps({
        "case_key": out["case_key"],
        "status": out["status"],
        "winner": out["winner"],
        "coverage": out["coverage"],
        "blocked_or_ambiguous_count": len(out["blocked_or_ambiguous"]),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
