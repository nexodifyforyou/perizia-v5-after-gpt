from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .runner import build_context
from .corpus_registry import load_cases, list_case_keys
from .bene_header_spine import build_bene_header_spine
from .cadastral_candidate_pack import build_cadastral_candidate_pack


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
        "field_scope": "CADASTRAL_FIELD_SHELL",
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
        "blocked_zone_count": len(out["blocked_zones"]),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
