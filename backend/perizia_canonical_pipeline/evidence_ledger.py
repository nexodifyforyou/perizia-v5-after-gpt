from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .runner import build_context
from .corpus_registry import load_cases, list_case_keys
from .bene_header_spine import build_bene_header_spine
from .cadastral_candidate_pack import build_cadastral_candidate_pack
from .location_candidate_pack import build_location_candidate_pack
from .rights_candidate_pack import build_rights_candidate_pack
from .occupancy_candidate_pack import build_occupancy_candidate_pack
from .valuation_candidate_pack import build_valuation_candidate_pack
from .cost_candidate_pack import build_cost_candidate_pack


def _make_packet_id(field_type: str, lot_id: str, page: int, line_index: Optional[int]) -> str:
    line_part = f"l{line_index}" if isinstance(line_index, int) else "lna"
    return f"{field_type}::{lot_id}::p{page}::{line_part}"


def _make_bene_packet_id(lot_id: str, bene_id: str, page: int, line_index: Optional[int]) -> str:
    line_part = f"l{line_index}" if isinstance(line_index, int) else "lna"
    return f"bene_header::{lot_id}::{bene_id}::p{page}::{line_part}"


def _candidate_sort_key(cand: Dict[str, object]) -> Tuple[int, int, str]:
    page = cand.get("page")
    line_index = cand.get("line_index")
    return (
        page if isinstance(page, int) else 999999,
        line_index if isinstance(line_index, int) else 999999,
        str(cand.get("candidate_id") or ""),
    )


def _location_candidate_ref(cand: Dict[str, object]) -> Dict[str, object]:
    return {
        "candidate_id": cand.get("candidate_id"),
        "field_type": cand.get("field_type"),
        "extracted_value": cand.get("extracted_value"),
        "page": cand.get("page"),
        "line_index": cand.get("line_index"),
        "line_quote": cand.get("quote"),
        "attribution": cand.get("attribution"),
        "lot_id": cand.get("lot_id"),
        "bene_id": cand.get("bene_id"),
        "composite_key": cand.get("composite_key"),
        "source_type": cand.get("source_type"),
    }


def _cadastral_candidate_ref(cand: Dict[str, object]) -> Dict[str, object]:
    return {
        "candidate_id": cand.get("candidate_id"),
        "field_type": cand.get("field_type"),
        "extracted_value": cand.get("extracted_value"),
        "page": cand.get("page"),
        "line_index": cand.get("line_index"),
        "line_quote": cand.get("line_quote"),
        "scope_attribution": cand.get("scope_attribution"),
        "lot_id": cand.get("lot_id"),
        "bene_id": cand.get("bene_id"),
        "composite_key": cand.get("composite_key"),
        "sibling_fields": cand.get("sibling_fields"),
    }


def build_evidence_ledger(case_key: str) -> Dict[str, object]:
    ctx = build_context(case_key)

    hyp_fp = ctx.artifact_dir / "structure_hypotheses.json"
    spine_fp = ctx.artifact_dir / "lot_header_spine.json"
    scope_fp = ctx.artifact_dir / "lot_scope_map.json"
    extract_fp = ctx.artifact_dir / "extract_metrics.json"

    hyp = json.loads(hyp_fp.read_text(encoding="utf-8"))
    spine = json.loads(spine_fp.read_text(encoding="utf-8"))
    scope = json.loads(scope_fp.read_text(encoding="utf-8"))
    extract = json.loads(extract_fp.read_text(encoding="utf-8"))

    winner = hyp.get("winner")
    quality = extract.get("global_quality_tier")

    out: Dict[str, object] = {
        "case_key": case_key,
        "winner": winner,
        "global_quality_tier": quality,
        "status": "OK",
        "field_scope": "CADASTRAL_LOCATION_RIGHTS_OCCUPANCY_VALUATION_COST_FIELD_SHELL",
        "packets": [],
        "scope_zones": {
            "global_pre_lot_zone": scope.get("global_pre_lot_zone"),
            "lot_scopes": scope.get("lot_scopes", []),
            "same_page_collisions": scope.get("same_page_collisions", []),
        },
        "blocked_zones": [],
        "coverage": {
            "lot_header_packets_count": 0,
            "lots_with_header_packets": [],
            "missing_lot_header_packet_ids": [],
            "bene_header_packets_count": 0,
            "benes_with_header_packets": [],
            "cadastral_packet_count": 0,
            "cadastral_fields_present": [],
            "location_packet_count": 0,
            "location_fields_present": [],
            "location_scope_keys": [],
            "rights_packet_count": 0,
            "rights_fields_present": [],
            "rights_scope_keys": [],
            "occupancy_packet_count": 0,
            "occupancy_fields_present": [],
            "occupancy_scope_keys": [],
            "cost_packet_count": 0,
            "cost_fields_present": [],
            "cost_scope_keys": [],
            "cost_context_count": 0,
        },
        "warnings": list(scope.get("warnings", []) or []),
        "source_artifacts": {
            "structure_hypotheses": str(hyp_fp),
            "lot_header_spine": str(spine_fp),
            "lot_scope_map": str(scope_fp),
            "extract_metrics": str(extract_fp),
        },
    }

    if winner == "BLOCKED_UNREADABLE":
        out["status"] = "BLOCKED_UNREADABLE"
        out["blocked_zones"].append({
            "type": "UNREADABLE_DOCUMENT",
            "reason": "Evidence harvesting blocked because extraction quality is unreadable.",
            "unreadable_pages": extract.get("unreadable_pages", []),
        })
        dst = ctx.artifact_dir / "evidence_ledger.json"
        dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        return out

    # --- lot_header packets (unchanged) ---
    spine_rows = spine.get("lot_header_spine", []) or []
    packets: List[Dict[str, object]] = []

    for row in spine_rows:
        lot_id = str(row["lot_id"])
        page = int(row["first_header_page"])
        line_index = row.get("first_header_line_index")
        quote = row.get("first_header_quote")

        packet = {
            "packet_id": _make_packet_id("lot_header", lot_id, page, line_index if isinstance(line_index, int) else None),
            "field_type": "lot_header",
            "lot_id": lot_id,
            "bene_id": None,
            "corpo_id": None,
            "scope_certainty": "CONFIRMED",
            "scope_basis": "Explicit lot header spine derived from header-grade classifier and ordered header spine.",
            "page": page,
            "line_index": line_index,
            "quote": quote,
            "context_window": quote,
            "extracted_value": lot_id,
            "extraction_method": "HEADER_CLASSIFIER",
            "confidence": 1.0,
            "status": "ACTIVE",
            "source_refs": {
                "first_header_occurrence_index": row.get("first_header_occurrence_index"),
                "occurrences": row.get("occurrences"),
            },
        }
        packets.append(packet)

    out["coverage"]["lot_header_packets_count"] = len(packets)
    out["coverage"]["lots_with_header_packets"] = [p["lot_id"] for p in packets]

    # --- bene_header packets ---
    # Lot attribution is resolved in bene_header_spine; read pre-attributed rows directly.
    bene_spine = build_bene_header_spine(case_key)
    bene_rows = bene_spine.get("bene_header_spine", []) or []
    bene_packets: List[Dict[str, object]] = []

    for row in bene_rows:
        lot_id = str(row["lot_id"])
        bene_id = str(row["bene_id"])
        page = int(row["first_header_page"])
        line_index = row.get("first_header_line_index")
        quote = row.get("first_header_quote")
        attribution = str(row.get("attribution", "ATTRIBUTED_BY_SCOPE"))

        if attribution == "CONFIRMED_BY_SINGLE_LOT":
            scope_basis = "Single-lot winner: bene header definitionally belongs to the sole lot."
        else:
            scope_basis = (
                f"Multi-lot winner: bene header page {page} falls within lot {lot_id} scope; "
                "attributed by page containment."
            )

        bene_packets.append({
            "packet_id": _make_bene_packet_id(lot_id, bene_id, page, line_index if isinstance(line_index, int) else None),
            "field_type": "bene_header",
            "lot_id": lot_id,
            "bene_id": bene_id,
            "corpo_id": None,
            "scope_certainty": attribution,
            "scope_basis": scope_basis,
            "page": page,
            "line_index": line_index,
            "quote": quote,
            "context_window": quote,
            "extracted_value": bene_id,
            "extraction_method": "HEADER_CLASSIFIER",
            "confidence": 1.0,
            "status": "ACTIVE",
            "source_refs": {
                "first_header_occurrence_index": row.get("first_header_occurrence_index"),
                "occurrences": row.get("occurrences"),
            },
        })

    # Emit blocked zones for signals the spine could not attribute to a lot.
    _bene_block_type = {
        "GLOBAL_PRE_LOT_ZONE": "BENE_IN_GLOBAL_PRE_LOT_ZONE",
        "SAME_PAGE_COLLISION_ZONE": "BENE_IN_SAME_PAGE_COLLISION_ZONE",
        "NO_MATCHING_LOT_SCOPE": "BENE_UNATTRIBUTABLE",
    }
    for sig in bene_spine.get("unattributable_bene_signals", []):
        blocked_type = _bene_block_type.get(sig["reason"], "BENE_UNATTRIBUTABLE")
        out["blocked_zones"].append({
            "type": blocked_type,
            "reason": (
                f"Bene header {sig['bene_id']} on page {sig['page']} blocked by {sig['reason']}; "
                "lot attribution is not safe."
            ),
            "bene_id": sig["bene_id"],
            "page": sig["page"],
        })

    out["coverage"]["bene_header_packets_count"] = len(bene_packets)
    out["coverage"]["benes_with_header_packets"] = [
        row["composite_key"] for row in bene_rows
    ]

    out["packets"] = packets + bene_packets

    # --- cadastral candidates ---
    # Keep cadastral evidence explicit.  Do not collapse competing values in
    # the same scope/field into one active packet.
    _ATTR_PRIORITY = {
        "CONFIRMED": 0,
        "ATTRIBUTED_BY_SCOPE": 1,
        "LOT_LEVEL_ONLY": 2,
        "LOT_LEVEL_ONLY_PRE_BENE_CONTEXT": 3,
    }
    _LEDGER_SAFE_ATTRIBUTIONS = set(_ATTR_PRIORITY.keys())

    cadat_pack = build_cadastral_candidate_pack(case_key)

    for blocked in (cadat_pack.get("blocked_or_ambiguous") or []):
        out["blocked_zones"].append({
            "type": blocked.get("type", "CADASTRAL_BLOCKED_OR_AMBIGUOUS"),
            "reason": "Cadastral candidate pack marked this evidence as blocked or ambiguous.",
            "source": "cadastral_candidate_pack.blocked_or_ambiguous",
            "cadastral_ambiguity": blocked,
        })

    grouped_cadat: Dict[tuple, List[Dict[str, object]]] = {}
    for cand in (cadat_pack.get("candidates") or []):
        attr = cand.get("scope_attribution", "")
        if attr not in _LEDGER_SAFE_ATTRIBUTIONS:
            continue
        lot_id = cand.get("lot_id") or "unknown"
        bene_id = cand.get("bene_id") or "lot"
        field_type = cand["field_type"]
        key = (lot_id, bene_id, field_type, attr)
        grouped_cadat.setdefault(key, []).append(cand)

    cadastral_packets: List[Dict[str, object]] = []
    for (lot_id, bene_key, field_type, attr), candidates_for_field in sorted(
        grouped_cadat.items(),
        key=lambda item: (
            _ATTR_PRIORITY.get(item[0][3], 99),
            item[0][0],
            item[0][1],
            item[0][2],
        ),
    ):
        ordered_candidates = sorted(candidates_for_field, key=_candidate_sort_key)
        distinct_values = sorted({str(cand.get("extracted_value")) for cand in ordered_candidates})

        if len(distinct_values) > 1:
            out["blocked_zones"].append({
                "type": "CADASTRAL_SCOPE_FIELD_CONFLICT",
                "reason": (
                    "Multiple distinct cadastral candidate values exist for the same "
                    "scope, field, and attribution; no active winner packet was emitted."
                ),
                "field_type": field_type,
                "lot_id": lot_id,
                "bene_id": None if bene_key == "lot" else bene_key,
                "scope_attribution": attr,
                "distinct_values": distinct_values,
                "candidate_count": len(ordered_candidates),
                "candidates": [_cadastral_candidate_ref(cand) for cand in ordered_candidates],
            })
            continue

        cand = ordered_candidates[0]
        bene_id_val = None if bene_key == "lot" else bene_key
        cadastral_packets.append({
            "packet_id": (
                f"{field_type}::{lot_id}::{bene_key}"
                f"::p{cand['page']}::l{cand['line_index']}"
            ),
            "field_type": field_type,
            "lot_id": lot_id,
            "bene_id": bene_id_val,
            "corpo_id": None,
            "scope_certainty": attr,
            "scope_basis": cand.get("composite_key") or f"lot:{lot_id}",
            "page": cand["page"],
            "line_index": cand["line_index"],
            "quote": cand.get("line_quote"),
            "context_window": cand.get("line_quote"),
            "extracted_value": cand["extracted_value"],
            "extraction_method": "REGEX_CADAT_INLINE",
            "confidence": 1.0 if attr == "CONFIRMED" else 0.9,
            "status": "ACTIVE",
            "source_refs": {
                "sibling_fields": cand.get("sibling_fields"),
                "candidate_id": cand.get("candidate_id"),
                "duplicate_candidate_ids": [
                    other.get("candidate_id")
                    for other in ordered_candidates[1:]
                    if other.get("candidate_id")
                ],
            },
        })

    out["packets"] = packets + bene_packets + cadastral_packets
    out["coverage"]["cadastral_packet_count"] = len(cadastral_packets)
    out["coverage"]["cadastral_fields_present"] = sorted(
        {p["field_type"] for p in cadastral_packets}
    )

    # --- location candidates ---
    # Mirror the cadastral approach: group by (lot_id, bene_key, field_type, attr),
    # emit a single ACTIVE packet when all candidates agree on one value, or
    # emit a LOCATION_SCOPE_FIELD_CONFLICT blocked zone when they disagree.
    _LOC_ATTR_PRIORITY = {
        "CONFIRMED": 0,
        "CONFIRMED_BY_SINGLE_LOT_BENE_HEADER": 0,
        "BENE_HEADER_CONFIRMED_BY_SINGLE_LOT": 0,
        "ATTRIBUTED_BY_SCOPE": 1,
        "BENE_HEADER_ATTRIBUTED_BY_SCOPE": 1,
        "LOT_LEVEL_ONLY": 2,
        "LOT_LEVEL_ONLY_PRE_BENE_CONTEXT": 3,
    }
    _LOC_LEDGER_SAFE_ATTRIBUTIONS = set(_LOC_ATTR_PRIORITY.keys())

    loc_pack = build_location_candidate_pack(case_key)

    # Propagate location warnings (e.g. ATTRIBUTED_BY_SCOPE bene header warnings)
    for w in (loc_pack.get("warnings") or []):
        if w not in out["warnings"]:
            out["warnings"].append(w)

    # Forward blocked/ambiguous entries from the location pack into blocked_zones
    for blk in (loc_pack.get("blocked_or_ambiguous") or []):
        out["blocked_zones"].append({
            "type": blk.get("type", "LOCATION_BLOCKED_OR_AMBIGUOUS"),
            "reason": "Location candidate pack marked this evidence as blocked or ambiguous.",
            "source": "location_candidate_pack.blocked_or_ambiguous",
            "location_ambiguity": blk,
        })

    # Group by (lot_id, bene_key, field_type) — intentionally WITHOUT attr so that
    # candidates from different attribution tiers for the same scope/field are tested
    # together.  If they disagree on value the whole group is blocked; if they agree
    # a single ACTIVE packet is emitted using the best (highest-priority) attr present.
    grouped_loc: Dict[tuple, List[Dict[str, object]]] = {}
    for cand in (loc_pack.get("candidates") or []):
        attr = cand.get("attribution", "")
        if attr not in _LOC_LEDGER_SAFE_ATTRIBUTIONS:
            continue
        lot_id = cand.get("lot_id") or "unknown"
        bene_key = cand.get("bene_id") or "lot"
        field_type = cand["field_type"]
        key = (lot_id, bene_key, field_type)
        grouped_loc.setdefault(key, []).append(cand)

    location_packets: List[Dict[str, object]] = []
    for (lot_id, bene_key, field_type), candidates_for_field in sorted(
        grouped_loc.items(),
        key=lambda item: (item[0][0], item[0][1], item[0][2]),
    ):
        ordered_loc = sorted(candidates_for_field, key=_candidate_sort_key)
        distinct_values = sorted({str(c.get("extracted_value")) for c in ordered_loc})
        all_attrs = sorted({c.get("attribution", "") for c in ordered_loc})
        # Best attribution = lowest priority number across the group
        best_attr = min(all_attrs, key=lambda a: _LOC_ATTR_PRIORITY.get(a, 99))

        if len(distinct_values) > 1:
            out["blocked_zones"].append({
                "type": "LOCATION_SCOPE_FIELD_CONFLICT",
                "reason": (
                    "Multiple distinct location candidate values exist for the same "
                    "scope and field across one or more attribution tiers; "
                    "no active winner packet was emitted."
                ),
                "field_type": field_type,
                "lot_id": lot_id,
                "bene_id": None if bene_key == "lot" else bene_key,
                "scope_attributions": all_attrs,
                "distinct_values": distinct_values,
                "candidate_count": len(ordered_loc),
                "candidates": [_location_candidate_ref(c) for c in ordered_loc],
            })
            continue

        cand = ordered_loc[0]
        bene_id_val = None if bene_key == "lot" else bene_key
        location_packets.append({
            "packet_id": (
                f"{field_type}::{lot_id}::{bene_key}"
                f"::p{cand['page']}::l{cand['line_index']}"
            ),
            "field_type": field_type,
            "lot_id": lot_id,
            "bene_id": bene_id_val,
            "corpo_id": None,
            "scope_certainty": best_attr,
            "scope_basis": cand.get("scope_basis") or f"lot:{lot_id}",
            "page": cand["page"],
            "line_index": cand["line_index"],
            "quote": cand.get("quote"),
            "context_window": cand.get("context_window"),
            "extracted_value": cand["extracted_value"],
            "extraction_method": cand.get("extraction_method", "REGEX_LOC_INLINE"),
            "confidence": (
                1.0 if best_attr in (
                    "CONFIRMED", "BENE_HEADER_CONFIRMED_BY_SINGLE_LOT",
                    "CONFIRMED_BY_SINGLE_LOT_BENE_HEADER",
                ) else 0.9
            ),
            "status": "ACTIVE",
            "source_refs": {
                "sibling_fields": cand.get("sibling_fields"),
                "candidate_id": cand.get("candidate_id"),
                "source_type": cand.get("source_type"),
                "duplicate_candidate_ids": [
                    other.get("candidate_id")
                    for other in ordered_loc[1:]
                    if other.get("candidate_id")
                ],
            },
        })

    out["coverage"]["location_packet_count"] = len(location_packets)
    out["coverage"]["location_fields_present"] = sorted(
        {p["field_type"] for p in location_packets}
    )
    out["coverage"]["location_scope_keys"] = sorted(
        {
            (
                f"{p['lot_id']}/{p['bene_id']}"
                if p["bene_id"]
                else f"lot:{p['lot_id']}"
            )
            for p in location_packets
        }
    )

    # --- rights candidates ---
    # Same pattern as location: group by (lot_id, bene_key, field_type) WITHOUT attr so
    # candidates from different attribution tiers for the same scope/field are tested
    # together.  Conflict → RIGHTS_SCOPE_FIELD_CONFLICT blocked zone; agreement → ACTIVE.
    _RTS_ATTR_PRIORITY = {
        "CONFIRMED": 0,
        "ATTRIBUTED_BY_SCOPE": 1,
        "LOT_LEVEL_ONLY": 2,
        "LOT_LEVEL_ONLY_PRE_BENE_CONTEXT": 3,
    }
    _RTS_LEDGER_SAFE_ATTRIBUTIONS = set(_RTS_ATTR_PRIORITY.keys())

    rts_pack = build_rights_candidate_pack(case_key)

    for w in (rts_pack.get("warnings") or []):
        if w not in out["warnings"]:
            out["warnings"].append(w)

    for blk in (rts_pack.get("blocked_or_ambiguous") or []):
        out["blocked_zones"].append({
            "type": blk.get("type", "RIGHTS_BLOCKED_OR_AMBIGUOUS"),
            "reason": "Rights candidate pack marked this evidence as blocked or ambiguous.",
            "source": "rights_candidate_pack.blocked_or_ambiguous",
            "rights_ambiguity": blk,
        })

    grouped_rts: Dict[tuple, List[Dict[str, object]]] = {}
    for cand in (rts_pack.get("candidates") or []):
        attr = cand.get("attribution", "")
        if attr not in _RTS_LEDGER_SAFE_ATTRIBUTIONS:
            continue
        lot_id = cand.get("lot_id") or "unknown"
        bene_key = cand.get("bene_id") or "lot"
        field_type = cand["field_type"]
        key = (lot_id, bene_key, field_type)
        grouped_rts.setdefault(key, []).append(cand)

    rights_packets: List[Dict[str, object]] = []
    for (lot_id, bene_key, field_type), candidates_for_field in sorted(
        grouped_rts.items(),
        key=lambda item: (item[0][0], item[0][1], item[0][2]),
    ):
        ordered_rts = sorted(candidates_for_field, key=_candidate_sort_key)
        distinct_values = sorted({str(c.get("extracted_value")) for c in ordered_rts})
        all_attrs = sorted({c.get("attribution", "") for c in ordered_rts})
        best_attr = min(all_attrs, key=lambda a: _RTS_ATTR_PRIORITY.get(a, 99))

        if len(distinct_values) > 1:
            out["blocked_zones"].append({
                "type": "RIGHTS_SCOPE_FIELD_CONFLICT",
                "reason": (
                    "Multiple distinct rights candidate values exist for the same "
                    "scope and field across one or more attribution tiers; "
                    "no active winner packet was emitted."
                ),
                "field_type": field_type,
                "lot_id": lot_id,
                "bene_id": None if bene_key == "lot" else bene_key,
                "scope_attributions": all_attrs,
                "distinct_values": distinct_values,
                "candidate_count": len(ordered_rts),
                "candidates": [
                    {
                        "candidate_id": c.get("candidate_id"),
                        "field_type": c.get("field_type"),
                        "extracted_value": c.get("extracted_value"),
                        "page": c.get("page"),
                        "line_index": c.get("line_index"),
                        "line_quote": c.get("quote"),
                        "attribution": c.get("attribution"),
                        "lot_id": c.get("lot_id"),
                        "bene_id": c.get("bene_id"),
                        "composite_key": c.get("composite_key"),
                        "source_type": c.get("source_type"),
                    }
                    for c in ordered_rts
                ],
            })
            continue

        cand = ordered_rts[0]
        bene_id_val = None if bene_key == "lot" else bene_key
        rights_packets.append({
            "packet_id": (
                f"{field_type}::{lot_id}::{bene_key}"
                f"::p{cand['page']}::l{cand['line_index']}"
            ),
            "field_type": field_type,
            "lot_id": lot_id,
            "bene_id": bene_id_val,
            "corpo_id": None,
            "scope_certainty": best_attr,
            "scope_basis": cand.get("scope_basis") or f"lot:{lot_id}",
            "page": cand["page"],
            "line_index": cand["line_index"],
            "quote": cand.get("quote"),
            "context_window": cand.get("context_window"),
            "extracted_value": cand["extracted_value"],
            "extraction_method": cand.get("extraction_method", "REGEX_RIGHTS_INLINE"),
            "confidence": (
                1.0 if best_attr == "CONFIRMED" else 0.9
            ),
            "status": "ACTIVE",
            "source_refs": {
                "sibling_fields": cand.get("sibling_fields"),
                "candidate_id": cand.get("candidate_id"),
                "source_type": cand.get("source_type"),
                "duplicate_candidate_ids": [
                    other.get("candidate_id")
                    for other in ordered_rts[1:]
                    if other.get("candidate_id")
                ],
            },
        })

    # packets list is finalized in the occupancy section below
    out["coverage"]["rights_packet_count"] = len(rights_packets)
    out["coverage"]["rights_fields_present"] = sorted(
        {p["field_type"] for p in rights_packets}
    )
    out["coverage"]["rights_scope_keys"] = sorted(
        {
            (
                f"{p['lot_id']}/{p['bene_id']}"
                if p["bene_id"]
                else f"lot:{p['lot_id']}"
            )
            for p in rights_packets
        }
    )

    # --- occupancy candidates ---
    # Same pattern as rights: group by (lot_id, bene_key, field_type) WITHOUT attr.
    # Conflict → OCCUPANCY_SCOPE_FIELD_CONFLICT blocked zone; agreement → ACTIVE packet.
    _OCC_ATTR_PRIORITY = {
        "CONFIRMED": 0,
        "ATTRIBUTED_BY_SCOPE": 1,
        "LOT_LEVEL_ONLY": 2,
        "LOT_LEVEL_ONLY_PRE_BENE_CONTEXT": 3,
    }
    _OCC_LEDGER_SAFE_ATTRIBUTIONS = set(_OCC_ATTR_PRIORITY.keys())

    occ_pack = build_occupancy_candidate_pack(case_key)

    for w in (occ_pack.get("warnings") or []):
        if w not in out["warnings"]:
            out["warnings"].append(w)

    for blk in (occ_pack.get("blocked_or_ambiguous") or []):
        # Skip MULTI_VALUE_UNRESOLVED entries — they are already surfaced via
        # OCCUPANCY_SCOPE_FIELD_CONFLICT below when candidate values diverge.
        if blk.get("type") == "OCCUPANCY_MULTI_VALUE_UNRESOLVED":
            continue
        out["blocked_zones"].append({
            "type": blk.get("type", "OCCUPANCY_BLOCKED_OR_AMBIGUOUS"),
            "reason": "Occupancy candidate pack marked this evidence as blocked or ambiguous.",
            "source": "occupancy_candidate_pack.blocked_or_ambiguous",
            "occupancy_ambiguity": blk,
        })

    # Only ACTIVE candidates are eligible for ledger packets.
    grouped_occ: Dict[tuple, List[Dict[str, object]]] = {}
    for cand in (occ_pack.get("candidates") or []):
        if cand.get("candidate_status") != "ACTIVE":
            continue
        attr = cand.get("attribution", "")
        if attr not in _OCC_LEDGER_SAFE_ATTRIBUTIONS:
            continue
        lot_id = cand.get("lot_id") or "unknown"
        bene_key = cand.get("bene_id") or "lot"
        field_type = cand["field_type"]
        key = (lot_id, bene_key, field_type)
        grouped_occ.setdefault(key, []).append(cand)

    occupancy_packets: List[Dict[str, object]] = []
    for (lot_id, bene_key, field_type), candidates_for_field in sorted(
        grouped_occ.items(),
        key=lambda item: (item[0][0], item[0][1], item[0][2]),
    ):
        ordered_occ = sorted(candidates_for_field, key=_candidate_sort_key)
        # Case-insensitive distinct values (matches pack-level dedup behaviour).
        distinct_values_norm = sorted({str(c.get("extracted_value", "")).strip().lower()
                                       for c in ordered_occ})
        all_attrs = sorted({c.get("attribution", "") for c in ordered_occ})
        best_attr = min(all_attrs, key=lambda a: _OCC_ATTR_PRIORITY.get(a, 99))

        if len(distinct_values_norm) > 1:
            out["blocked_zones"].append({
                "type": "OCCUPANCY_SCOPE_FIELD_CONFLICT",
                "reason": (
                    "Multiple distinct occupancy candidate values exist for the same "
                    "scope and field across one or more attribution tiers; "
                    "no active winner packet was emitted."
                ),
                "field_type": field_type,
                "lot_id": lot_id,
                "bene_id": None if bene_key == "lot" else bene_key,
                "scope_attributions": all_attrs,
                "distinct_values": sorted({str(c.get("extracted_value", ""))
                                           for c in ordered_occ}),
                "candidate_count": len(ordered_occ),
                "candidates": [
                    {
                        "candidate_id": c.get("candidate_id"),
                        "field_type": c.get("field_type"),
                        "extracted_value": c.get("extracted_value"),
                        "page": c.get("page"),
                        "line_index": c.get("line_index"),
                        "line_quote": c.get("quote"),
                        "attribution": c.get("attribution"),
                        "lot_id": c.get("lot_id"),
                        "bene_id": c.get("bene_id"),
                        "composite_key": c.get("composite_key"),
                        "source_type": c.get("source_type"),
                    }
                    for c in ordered_occ
                ],
            })
            continue

        cand = ordered_occ[0]
        bene_id_val = None if bene_key == "lot" else bene_key
        occupancy_packets.append({
            "packet_id": (
                f"{field_type}::{lot_id}::{bene_key}"
                f"::p{cand['page']}::l{cand['line_index']}"
            ),
            "field_type": field_type,
            "lot_id": lot_id,
            "bene_id": bene_id_val,
            "corpo_id": None,
            "scope_certainty": best_attr,
            "scope_basis": cand.get("scope_basis") or f"lot:{lot_id}",
            "page": cand["page"],
            "line_index": cand["line_index"],
            "quote": cand.get("quote"),
            "context_window": cand.get("context_window"),
            "extracted_value": cand["extracted_value"],
            "extraction_method": cand.get("extraction_method", "REGEX_OCC_INLINE"),
            "confidence": (
                1.0 if best_attr == "CONFIRMED" else 0.9
            ),
            "status": "ACTIVE",
            "source_refs": {
                "candidate_id": cand.get("candidate_id"),
                "source_type": cand.get("source_type"),
                "duplicate_candidate_ids": [
                    other.get("candidate_id")
                    for other in ordered_occ[1:]
                    if other.get("candidate_id")
                ],
            },
        })

    out["coverage"]["occupancy_packet_count"] = len(occupancy_packets)
    out["coverage"]["occupancy_fields_present"] = sorted(
        {p["field_type"] for p in occupancy_packets}
    )
    out["coverage"]["occupancy_scope_keys"] = sorted(
        {
            (
                f"{p['lot_id']}/{p['bene_id']}"
                if p["bene_id"]
                else f"lot:{p['lot_id']}"
            )
            for p in occupancy_packets
        }
    )

    # --- valuation candidates ---
    # Same pattern as occupancy: group by (lot_id, bene_key, field_type) WITHOUT attr.
    # Conflict → VALUATION_SCOPE_FIELD_CONFLICT blocked zone; agreement → ACTIVE packet.
    _VAL_ATTR_PRIORITY = {
        "CONFIRMED": 0,
        "ATTRIBUTED_BY_SCOPE": 1,
        # Local preceding-context lot attribution — reliable inline evidence.
        "LOT_LOCAL_CONTEXT_OVERRIDE": 1,
        "LOT_LEVEL_ONLY": 2,
        "LOT_LEVEL_ONLY_PRE_BENE_CONTEXT": 3,
    }
    _VAL_LEDGER_SAFE_ATTRIBUTIONS = set(_VAL_ATTR_PRIORITY.keys())

    val_pack = build_valuation_candidate_pack(case_key)

    for w in (val_pack.get("warnings") or []):
        if w not in out["warnings"]:
            out["warnings"].append(w)

    for blk in (val_pack.get("blocked_or_ambiguous") or []):
        # Skip MULTI_VALUE_UNRESOLVED entries — they are already surfaced via
        # VALUATION_SCOPE_FIELD_CONFLICT below when candidate values diverge.
        if blk.get("type") == "VALUATION_MULTI_VALUE_UNRESOLVED":
            continue
        out["blocked_zones"].append({
            "type": blk.get("type", "VALUATION_BLOCKED_OR_AMBIGUOUS"),
            "reason": "Valuation candidate pack marked this evidence as blocked or ambiguous.",
            "source": "valuation_candidate_pack.blocked_or_ambiguous",
            "valuation_ambiguity": blk,
        })

    # Only ACTIVE candidates with safe attributions are eligible for ledger packets.
    grouped_val: Dict[tuple, List[Dict[str, object]]] = {}
    for cand in (val_pack.get("candidates") or []):
        if cand.get("candidate_status") != "ACTIVE":
            continue
        attr = cand.get("attribution", "")
        if attr not in _VAL_LEDGER_SAFE_ATTRIBUTIONS:
            continue
        lot_id = cand.get("lot_id") or "unknown"
        bene_key = cand.get("bene_id") or "lot"
        field_type = cand["field_type"]
        key = (lot_id, bene_key, field_type)
        grouped_val.setdefault(key, []).append(cand)

    valuation_packets: List[Dict[str, object]] = []
    for (lot_id, bene_key, field_type), candidates_for_field in sorted(
        grouped_val.items(),
        key=lambda item: (item[0][0], item[0][1], item[0][2]),
    ):
        ordered_val = sorted(candidates_for_field, key=_candidate_sort_key)
        # Case-insensitive distinct values (matches pack-level dedup behaviour).
        distinct_values_norm = sorted({str(c.get("extracted_value", "")).strip().lower()
                                       for c in ordered_val})
        all_attrs = sorted({c.get("attribution", "") for c in ordered_val})
        best_attr = min(all_attrs, key=lambda a: _VAL_ATTR_PRIORITY.get(a, 99))

        if len(distinct_values_norm) > 1:
            out["blocked_zones"].append({
                "type": "VALUATION_SCOPE_FIELD_CONFLICT",
                "reason": (
                    "Multiple distinct valuation candidate values exist for the same "
                    "scope and field across one or more attribution tiers; "
                    "no active winner packet was emitted."
                ),
                "field_type": field_type,
                "lot_id": lot_id,
                "bene_id": None if bene_key == "lot" else bene_key,
                "scope_attributions": all_attrs,
                "distinct_values": sorted({str(c.get("extracted_value", ""))
                                           for c in ordered_val}),
                "candidate_count": len(ordered_val),
                "candidates": [
                    {
                        "candidate_id": c.get("candidate_id"),
                        "field_type": c.get("field_type"),
                        "extracted_value": c.get("extracted_value"),
                        "page": c.get("page"),
                        "line_index": c.get("line_index"),
                        "line_quote": c.get("quote"),
                        "attribution": c.get("attribution"),
                        "lot_id": c.get("lot_id"),
                        "bene_id": c.get("bene_id"),
                        "composite_key": c.get("composite_key"),
                        "source_type": c.get("source_type"),
                    }
                    for c in ordered_val
                ],
            })
            continue

        cand = ordered_val[0]
        bene_id_val = None if bene_key == "lot" else bene_key
        valuation_packets.append({
            "packet_id": (
                f"{field_type}::{lot_id}::{bene_key}"
                f"::p{cand['page']}::l{cand['line_index']}"
            ),
            "field_type": field_type,
            "lot_id": lot_id,
            "bene_id": bene_id_val,
            "corpo_id": None,
            "scope_certainty": best_attr,
            "scope_basis": cand.get("scope_basis") or f"lot:{lot_id}",
            "page": cand["page"],
            "line_index": cand["line_index"],
            "quote": cand.get("quote"),
            "context_window": cand.get("context_window"),
            "extracted_value": cand["extracted_value"],
            "extraction_method": cand.get("extraction_method", "REGEX_VAL_INLINE"),
            "confidence": (
                1.0 if best_attr == "CONFIRMED" else 0.9
            ),
            "status": "ACTIVE",
            "source_refs": {
                "candidate_id": cand.get("candidate_id"),
                "source_type": cand.get("source_type"),
                "duplicate_candidate_ids": [
                    other.get("candidate_id")
                    for other in ordered_val[1:]
                    if other.get("candidate_id")
                ],
            },
        })

    out["coverage"]["valuation_packet_count"] = len(valuation_packets)
    out["coverage"]["valuation_fields_present"] = sorted(
        {p["field_type"] for p in valuation_packets}
    )
    out["coverage"]["valuation_scope_keys"] = sorted(
        {
            (
                f"{p['lot_id']}/{p['bene_id']}"
                if p["bene_id"]
                else f"lot:{p['lot_id']}"
            )
            for p in valuation_packets
        }
    )

    # --- cost / oneri candidates ---
    # Quantified candidates: group by (lot_id, bene_key, field_type).
    #   - 1 distinct normalised amount → ACTIVE packet
    #   - Multiple distinct amounts → COST_SCOPE_FIELD_CONFLICT (blocked)
    # Non-quantified context candidates: forwarded individually as context packets
    # (no conflict detection — multiple context items for same scope are expected).
    _COST_ATTR_PRIORITY = {
        "CONFIRMED": 0,
        "ATTRIBUTED_BY_SCOPE": 1,
        "LOT_LOCAL_CONTEXT_OVERRIDE": 1,
        "LOT_LEVEL_ONLY": 2,
    }
    _COST_SAFE_ATTRIBUTIONS = set(_COST_ATTR_PRIORITY.keys())

    cost_pack = build_cost_candidate_pack(case_key)

    for w in (cost_pack.get("warnings") or []):
        if w not in out["warnings"]:
            out["warnings"].append(w)

    for blk in (cost_pack.get("blocked_or_ambiguous") or []):
        btype = blk.get("type", "COST_BLOCKED_OR_AMBIGUOUS")
        # Skip internal-only context types that don't need ledger surface
        if btype in ("COST_NON_QUANTIFIED_CONTEXT_ONLY",):
            continue
        out["blocked_zones"].append({
            "type": btype,
            "reason": "Cost candidate pack marked this evidence as blocked or ambiguous.",
            "source": "cost_candidate_pack.blocked_or_ambiguous",
            "cost_ambiguity": blk,
        })

    # Split ACTIVE candidates into quantified and context
    cost_quant_active: List[Dict[str, object]] = []
    cost_ctx_active: List[Dict[str, object]] = []

    for cand in (cost_pack.get("candidates") or []):
        if cand.get("candidate_status") != "ACTIVE":
            continue
        if not cand.get("is_quantified", True):
            cost_ctx_active.append(cand)
        else:
            attr = cand.get("attribution", "")
            if attr in _COST_SAFE_ATTRIBUTIONS:
                cost_quant_active.append(cand)

    # Group quantified candidates by (lot_id, bene_key, field_type)
    grouped_cost: Dict[tuple, List[Dict[str, object]]] = {}
    for cand in cost_quant_active:
        lot_id = cand.get("lot_id") or "unknown"
        bene_key = cand.get("bene_id") or "lot"
        field_type = cand["field_type"]
        key = (lot_id, bene_key, field_type)
        grouped_cost.setdefault(key, []).append(cand)

    cost_packets: List[Dict[str, object]] = []
    for (lot_id, bene_key, field_type), candidates_for_field in sorted(
        grouped_cost.items(),
        key=lambda item: (item[0][0], item[0][1], item[0][2]),
    ):
        ordered_cost = sorted(
            candidates_for_field,
            key=lambda c: (
                c.get("page") if isinstance(c.get("page"), int) else 999999,
                c.get("line_index") if isinstance(c.get("line_index"), int) else 999999,
            ),
        )
        # Normalise values for dedup / conflict detection
        distinct_values_norm = sorted({
            str(c.get("extracted_value", "")).strip().lower()
            for c in ordered_cost
        })
        all_attrs = sorted({c.get("attribution", "") for c in ordered_cost})
        best_attr = min(all_attrs, key=lambda a: _COST_ATTR_PRIORITY.get(a, 99))

        if len(distinct_values_norm) > 1:
            # Multiple distinct amounts for same scope/field → conflict, blocked
            out["blocked_zones"].append({
                "type": "COST_SCOPE_FIELD_CONFLICT",
                "reason": (
                    "Multiple distinct cost candidate values exist for the same "
                    "scope and field type; no active winner packet was emitted. "
                    "These are preserved here for downstream resolution."
                ),
                "field_type": field_type,
                "lot_id": lot_id,
                "bene_id": None if bene_key == "lot" else bene_key,
                "scope_attributions": all_attrs,
                "distinct_values": sorted({
                    str(c.get("extracted_value", "")) for c in ordered_cost
                }),
                "candidate_count": len(ordered_cost),
                "candidates": [
                    {
                        "candidate_id": c.get("candidate_id"),
                        "field_type": c.get("field_type"),
                        "extracted_value": c.get("extracted_value"),
                        "page": c.get("page"),
                        "line_index": c.get("line_index"),
                        "quote": c.get("quote"),
                        "attribution": c.get("attribution"),
                        "lot_id": c.get("lot_id"),
                        "bene_id": c.get("bene_id"),
                    }
                    for c in ordered_cost
                ],
            })
            continue

        # Single distinct value → ACTIVE packet
        cand = ordered_cost[0]
        bene_id_val = None if bene_key == "lot" else bene_key
        cost_packets.append({
            "packet_id": (
                f"{field_type}::{lot_id}::{bene_key}"
                f"::p{cand['page']}::l{cand['line_index']}"
            ),
            "field_type": field_type,
            "lot_id": lot_id,
            "bene_id": bene_id_val,
            "corpo_id": None,
            "scope_certainty": best_attr,
            "scope_basis": cand.get("scope_basis") or f"lot:{lot_id}",
            "page": cand["page"],
            "line_index": cand["line_index"],
            "quote": cand.get("quote"),
            "context_window": cand.get("context_window"),
            "extracted_value": cand["extracted_value"],
            "is_quantified": True,
            "extraction_method": cand.get("extraction_method", "REGEX_COST_INLINE"),
            "confidence": 1.0 if best_attr == "CONFIRMED" else 0.9,
            "status": "ACTIVE",
            "source_refs": {
                "candidate_id": cand.get("candidate_id"),
                "source_trigger_field_type": cand.get("source_trigger_field_type"),
                "duplicate_candidate_ids": [
                    other.get("candidate_id")
                    for other in ordered_cost[1:]
                    if other.get("candidate_id")
                ],
            },
        })

    # Non-quantified context items → individual context packets (no conflict check)
    cost_context_packets: List[Dict[str, object]] = []
    for cand in cost_ctx_active:
        lot_id = cand.get("lot_id") or "unknown"
        bene_key = cand.get("bene_id") or "lot"
        field_type = cand["field_type"]
        bene_id_val = None if bene_key == "lot" else bene_key
        cost_context_packets.append({
            "packet_id": (
                f"{field_type}::{lot_id}::{bene_key}"
                f"::p{cand['page']}::l{cand['line_index']}"
            ),
            "field_type": field_type,
            "lot_id": lot_id,
            "bene_id": bene_id_val,
            "corpo_id": None,
            "scope_certainty": cand.get("attribution", "LOT_LEVEL_ONLY"),
            "scope_basis": cand.get("scope_basis") or f"lot:{lot_id}",
            "page": cand["page"],
            "line_index": cand["line_index"],
            "quote": cand.get("quote"),
            "context_window": cand.get("context_window"),
            "extracted_value": None,
            "is_quantified": False,
            "extraction_method": cand.get("extraction_method", "REGEX_COST_NONQUANT"),
            "confidence": 0.8,
            "status": "ACTIVE",
            "source_refs": {
                "candidate_id": cand.get("candidate_id"),
            },
        })

    out["packets"] = (
        packets + bene_packets + cadastral_packets
        + location_packets + rights_packets + occupancy_packets
        + valuation_packets + cost_packets + cost_context_packets
    )
    out["coverage"]["cost_packet_count"] = len(cost_packets)
    out["coverage"]["cost_fields_present"] = sorted(
        {p["field_type"] for p in cost_packets}
    )
    out["coverage"]["cost_scope_keys"] = sorted(
        {
            (
                f"{p['lot_id']}/{p['bene_id']}"
                if p["bene_id"]
                else f"lot:{p['lot_id']}"
            )
            for p in cost_packets
        }
    )
    out["coverage"]["cost_context_count"] = len(cost_context_packets)

    # --- blocked zones for structural ambiguities ---
    same_page_collisions = scope.get("same_page_collisions", []) or []
    for collision in same_page_collisions:
        out["blocked_zones"].append({
            "type": "SAME_PAGE_COLLISION",
            "reason": "Page contains multiple ordered lot headers; downstream field harvesting must respect line ordering and avoid naive page-wide attribution.",
            "collision": collision,
        })

    if scope.get("global_pre_lot_zone"):
        out["blocked_zones"].append({
            "type": "GLOBAL_PRE_LOT_ZONE",
            "reason": "Pages before first explicit lot header are global context only, not direct lot evidence unless a later rule explicitly allows it.",
            "zone": scope["global_pre_lot_zone"],
        })

    dst = ctx.artifact_dir / "evidence_ledger.json"
    dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Minimal evidence ledger shell")
    parser.add_argument("--case", required=True, choices=list_case_keys())
    args = parser.parse_args()

    out = build_evidence_ledger(args.case)
    print(json.dumps({
        "case_key": out["case_key"],
        "status": out["status"],
        "field_scope": out["field_scope"],
        "lot_header_packets_count": out["coverage"]["lot_header_packets_count"],
        "bene_header_packets_count": out["coverage"]["bene_header_packets_count"],
        "cadastral_packet_count": out["coverage"]["cadastral_packet_count"],
        "cadastral_fields_present": out["coverage"]["cadastral_fields_present"],
        "location_packet_count": out["coverage"]["location_packet_count"],
        "location_fields_present": out["coverage"]["location_fields_present"],
        "location_scope_keys": out["coverage"]["location_scope_keys"],
        "rights_packet_count": out["coverage"]["rights_packet_count"],
        "rights_fields_present": out["coverage"]["rights_fields_present"],
        "rights_scope_keys": out["coverage"]["rights_scope_keys"],
        "occupancy_packet_count": out["coverage"]["occupancy_packet_count"],
        "occupancy_fields_present": out["coverage"]["occupancy_fields_present"],
        "occupancy_scope_keys": out["coverage"]["occupancy_scope_keys"],
        "valuation_packet_count": out["coverage"]["valuation_packet_count"],
        "valuation_fields_present": out["coverage"]["valuation_fields_present"],
        "valuation_scope_keys": out["coverage"]["valuation_scope_keys"],
        "cost_packet_count": out["coverage"]["cost_packet_count"],
        "cost_fields_present": out["coverage"]["cost_fields_present"],
        "cost_scope_keys": out["coverage"]["cost_scope_keys"],
        "cost_context_count": out["coverage"]["cost_context_count"],
        "blocked_zone_count": len(out["blocked_zones"]),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
