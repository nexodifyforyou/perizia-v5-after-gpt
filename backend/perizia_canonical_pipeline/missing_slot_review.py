"""
Bounded missing-slot review and escalation stage for doc_map freeze.

Produces: missing_slot_review_pack.json
          missing_slot_escalation_pack.json (when LLM is available)

REVIEW PASS (always):
  For each declared scope × primary field type slot with no evidence-based freeze
  entry, performs a bounded deterministic review:

  1. Extract cue windows from the scope's raw page text (not just a boolean).
  2. Check for related packets in the same family + scope.
  3. Check table zone types on scope pages.
  4. Emit a traceable review record with an evidence-aware, field-specific,
     scope-specific explanation.

ESCALATION PASS (when LLM is configured, for cue-hit reviews only):
  Builds a SUSPICIOUS_SILENCE clarification issue for each escalation-worthy
  review, sends it through the existing LLM clarification machinery (one call
  per slot, bounded), and returns the resolution so the freeze stage can emit
  llm_resolved / context_only / unresolved_explained instead of falling back
  to checked_no_reliable_basis.

  Escalation is capped at MAX_ESCALATIONS_PER_CASE slots per case, prioritised
  by cue evidence density and field family importance.
  At most one LLM resolution per slot. The shared provider helper may perform
  bounded transient-rate-limit retries; this stage does not do wider passes.

  When the LLM is not available, escalation is skipped silently and all missing
  slots fall back to checked_no_reliable_basis with full review traceability.

The review is fully deterministic (no LLM). It stays within the scope's
declared page range. No full-document roaming.

This replaces the former placeholder path (source_stage="freeze_cue_sweep")
that emitted blank traceability and generic phrase-bank explanations.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# Italian cue words per field family
# ---------------------------------------------------------------------------

_FAMILY_CUE_WORDS: Dict[str, List[str]] = {
    "cadastral": [
        "foglio", "mappale", "subalterno", "categoria", "particella",
        "catasto", "nceu", "rendita catastale", "catastale",
    ],
    "location": [
        "via ", "viale ", "corso ", "piazza ", "civico",
        "comune", "provincia", "ubicat", "sito in", "situato",
    ],
    "rights": [
        "propriet", "usufrutto", "nuda propriet", "diritto di",
        "quota di", "servit\u00f9", "superficie",
    ],
    "occupancy": [
        "occupat", "libero da", "locato", "locazione", "conduttore",
        "affittuario", "comodato", "detenuto",
    ],
    "valuation": [
        "valore di stima", "prezzo base", "stima", "valutazione",
        "valore complessivo", "perizia di stima",
    ],
    "cost": [
        "costo di", "onere", "spesa di", "ripristino", "sanatoria",
        "regolarizzazione", "bonifica", "importo lavori",
    ],
    "impianti": [
        "impianto", "riscaldamento", "impianto elettrico",
        "impianto idrico", "ascensore", "gas", "allacciamento",
    ],
}

# Family prefix inference (duplicated from doc_map_freeze to avoid circular import)
_FAMILY_PREFIXES: List[Tuple[str, str]] = [
    ("cadastral_", "cadastral"),
    ("location_", "location"),
    ("rights_", "rights"),
    ("occupancy_", "occupancy"),
    ("valore_", "valuation"),
    ("prezzo_", "valuation"),
    ("cost_", "cost"),
    ("onere_", "cost"),
    ("ripristino_", "cost"),
    ("sanatoria_", "cost"),
    ("impianto_", "impianti"),
    ("impianti_", "impianti"),
    ("allacci_", "impianti"),
    ("lot_", "structural"),
    ("bene_", "structural"),
]

# Maximum cue windows captured per review slot (keeps the artifact bounded)
_MAX_CUE_WINDOWS = 6


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _review_id(case_key: str, scope_key: str, field_type: str) -> str:
    """Deterministic review ID derived from case + slot identity."""
    digest = hashlib.md5(
        f"{case_key}:{scope_key}:{field_type}".encode("utf-8")
    ).hexdigest()[:12]
    return f"msr:{digest}"


def _scope_label(scope_key: str) -> str:
    """Short human-readable scope description (used inside 'Per ...' sentences)."""
    if scope_key == "document":
        return "document level"
    if scope_key.startswith("lot:"):
        lot_id = scope_key[4:]
        if lot_id.lower() == "unico":
            return "the sole lot"
        return f"Lotto {lot_id.upper()}"
    if scope_key.startswith("bene:"):
        rest = scope_key[5:]
        if "/" in rest:
            lot_id, bene_id = rest.split("/", 1)
            if lot_id.lower() == "unico":
                return f"Bene {bene_id}"
            return f"Bene {bene_id} (Lotto {lot_id.upper()})"
    return scope_key


def _page_range_label(pages: List[int]) -> str:
    if not pages:
        return "no pages"
    mn, mx = min(pages), max(pages)
    if mn == mx:
        return f"p.\u202f{mn}"
    return f"pp.\u202f{mn}\u2013{mx}"


def _infer_family(field_type: str) -> str:
    for prefix, family in _FAMILY_PREFIXES:
        if field_type.startswith(prefix):
            return family
    return "other"


def _scope_key_from_pkt(pkt: Dict[str, Any]) -> str:
    lot_id = pkt.get("lot_id")
    bene_id = pkt.get("bene_id")
    if lot_id is None and bene_id is None:
        return "document"
    if bene_id is None:
        return f"lot:{lot_id}"
    return f"bene:{lot_id}/{bene_id}"


# ---------------------------------------------------------------------------
# Step 1 — cue window extraction
# ---------------------------------------------------------------------------

def _extract_cue_windows(
    family: str,
    scope_pages: List[int],
    raw_pages_idx: Dict[int, str],
    window_chars: int = 120,
) -> List[Dict[str, Any]]:
    """
    Scan the scope's raw page text for family cue words.
    For each hit, capture a text window of ±window_chars characters.
    Returns at most _MAX_CUE_WINDOWS entries (bounded, not exhaustive).
    """
    cue_words = _FAMILY_CUE_WORDS.get(family, [])
    results: List[Dict[str, Any]] = []

    for page_num in scope_pages:
        page_text = raw_pages_idx.get(page_num, "")
        if not page_text:
            continue
        text_lower = page_text.lower()

        for cw in cue_words:
            pos = text_lower.find(cw)
            if pos < 0:
                continue
            half = window_chars // 2
            start = max(0, pos - half)
            end = min(len(page_text), pos + len(cw) + half)
            window = page_text[start:end].strip()
            results.append({
                "page": page_num,
                "cue_word": cw,
                "window": window,
            })
            if len(results) >= _MAX_CUE_WINDOWS:
                return results

    return results


# ---------------------------------------------------------------------------
# Step 2 — related packet check
# ---------------------------------------------------------------------------

def _find_related_packets(
    family: str,
    scope_key: str,
    all_packets: List[Any],
) -> List[Dict[str, Any]]:
    """
    Find packets from the same family in the same scope.
    Signals that the family is represented by at least some extraction
    in this scope, even if not for this specific field_type.
    """
    related: List[Dict[str, Any]] = []
    for pkt in all_packets:
        ft = pkt.get("field_type", "")
        if _infer_family(ft) != family:
            continue
        if _scope_key_from_pkt(pkt) != scope_key:
            continue
        related.append({
            "packet_id": pkt.get("packet_id"),
            "field_type": ft,
            "status": pkt.get("status"),
        })
        if len(related) >= 5:
            break
    return related


# ---------------------------------------------------------------------------
# Step 3 — table zone check
# ---------------------------------------------------------------------------

def _find_table_zone_types(
    scope_pages: List[int],
    table_zone_data: Dict[str, Any],
) -> List[str]:
    """
    Find table zone types whose page numbers overlap with the scope's pages.
    Used as context: if a table zone exists on a scope page for this family,
    the absence of a frozen value is more noteworthy.
    """
    if not scope_pages:
        return []
    scope_page_set: Set[int] = set(scope_pages)
    zones = table_zone_data.get("table_zones") or table_zone_data.get("zones", [])
    types_found: List[str] = []
    for z in zones:
        zone_page = z.get("page") or z.get("page_number")
        if zone_page is not None and int(zone_page) in scope_page_set:
            zt = z.get("type") or z.get("zone_type") or z.get("table_type")
            if zt and str(zt) not in types_found:
                types_found.append(str(zt))
    return types_found


# ---------------------------------------------------------------------------
# Explanation builder
# ---------------------------------------------------------------------------

def _build_explanation(
    scope_key: str,
    family: str,
    field_type: str,
    cue_windows: List[Dict[str, Any]],
    related_packets: List[Dict[str, Any]],
    reviewed_pages: List[int],
) -> str:
    """
    Generate a diplomatic, field-specific, scope-specific explanation.
    Derived from actual review findings — not a phrase bank.
    """
    scope_label = _scope_label(scope_key)
    page_label = _page_range_label(reviewed_pages)
    field_label = field_type.replace("_", " ")

    if not reviewed_pages:
        return (
            f"Per {scope_label}: no page range is defined for this scope, so the"
            f" {field_label} field cannot be reviewed from source text."
        )

    if cue_windows:
        found_cues = list(dict.fromkeys(w["cue_word"] for w in cue_windows))
        cue_str = ", ".join(f'"{c}"' for c in found_cues[:3])

        if related_packets:
            related_fts = list(dict.fromkeys(p["field_type"] for p in related_packets))
            related_str = ", ".join(related_fts[:2])
            return (
                f"Per {scope_label} ({page_label}): the bounded review found"
                f" {family}-related cue terms ({cue_str}) and related evidence for"
                f" {related_str}, but no passage provides a freeze-safe value for"
                f" {field_label}. The slot remains open pending human review."
            )
        else:
            return (
                f"Per {scope_label} ({page_label}): the bounded review found"
                f" {family}-related cue terms ({cue_str}) in the reviewed pages,"
                f" but none yields a reliably scoped value for {field_label}."
                f" The slot is recorded as reviewed but unfrozen."
            )
    else:
        return (
            f"Per {scope_label} ({page_label}): the bounded review found no"
            f" {family}-related content in the reviewed pages sufficient to freeze"
            f" a value for {field_label}. The slot is recorded as explicitly reviewed"
            f" and unfrozen."
        )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_missing_slot_review(
    case_key: str,
    missing_slots: List[Tuple[str, str, str]],  # (scope_key, family, field_type)
    raw_pages_idx: Dict[int, str],
    scope_page_ranges: Dict[str, List[int]],
    all_packets: List[Any],
    table_zone_data: Dict[str, Any],
    artifact_dir: Path,
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """
    Perform bounded deterministic review for each missing primary field slot.

    For each (scope_key, family, field_type) in missing_slots:
      - Extract cue windows from the scope's raw page text.
      - Check for related packets in the same family + scope.
      - Check table zone types on the scope's pages.
      - Build a traceable review record with an evidence-aware explanation.
      - Determine needs_human_review: True if cue hits were found (topic is
        present in the text but did not yield a freeze-safe extraction).

    Writes: missing_slot_review_pack.json to artifact_dir.
    Returns: mapping from (scope_key, field_type) → review record dict.
    """
    reviews: List[Dict[str, Any]] = []
    results: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for scope_key, family, field_type in missing_slots:
        rid = _review_id(case_key, scope_key, field_type)
        scope_pages = scope_page_ranges.get(scope_key, [])

        cue_windows = _extract_cue_windows(
            family=family,
            scope_pages=scope_pages,
            raw_pages_idx=raw_pages_idx,
        )
        related_packets = _find_related_packets(
            family=family,
            scope_key=scope_key,
            all_packets=all_packets,
        )
        table_zone_types = _find_table_zone_types(
            scope_pages=scope_pages,
            table_zone_data=table_zone_data,
        )

        has_cues = bool(cue_windows)
        # Cues found but no freeze-safe value → human review advised.
        # No cues at all → absence is genuine; human review not required.
        needs_human_review = has_cues

        explanation = _build_explanation(
            scope_key=scope_key,
            family=family,
            field_type=field_type,
            cue_windows=cue_windows,
            related_packets=related_packets,
            reviewed_pages=scope_pages,
        )

        explanation_basis = (
            "Bounded cue scan found relevant terms but no freeze-safe extraction"
            " was possible from the scoped text."
            if has_cues else
            "Bounded cue scan found no relevant content for this field family"
            " in the scope's page range."
        )

        review_rec: Dict[str, Any] = {
            "review_id": rid,
            "scope_key": scope_key,
            "field_family": family,
            "field_type": field_type,
            "reviewed_pages": scope_pages,
            "cue_hits": has_cues,
            "cue_windows": cue_windows,
            "table_zone_types": table_zone_types,
            "existing_related_packets": related_packets,
            "review_outcome": "checked_no_reliable_basis",
            "explanation_basis": explanation_basis,
            "needs_human_review": needs_human_review,
            "explanation": explanation,
            "source_refs": [],
        }
        reviews.append(review_rec)
        results[(scope_key, field_type)] = review_rec

    pack: Dict[str, Any] = {
        "case_key": case_key,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "review_count": len(reviews),
        "reviews": reviews,
    }
    out_path = artifact_dir / "missing_slot_review_pack.json"
    out_path.write_text(json.dumps(pack, ensure_ascii=False, indent=2), encoding="utf-8")

    return results


# ---------------------------------------------------------------------------
# Escalation path — bounded LLM clarification for cue-hit reviews
# ---------------------------------------------------------------------------

# Maximum escalation slots per case.  Keeps LLM costs bounded.
# Priority order: most cue windows first, then by field family importance.
MAX_ESCALATIONS_PER_CASE = 15

_FAMILY_ESCALATION_PRIORITY: Dict[str, int] = {
    "cadastral": 0,
    "location": 1,
    "rights": 2,
    "occupancy": 3,
    "valuation": 4,
    "cost": 5,
    "impianti": 6,
}


def _provider_failure_details(exc: Exception) -> Dict[str, Any]:
    return {
        "status_code": getattr(exc, "status_code", None),
        "error_type": getattr(exc, "error_type", None),
        "retry_after": getattr(exc, "retry_after", None),
        "retryable": getattr(exc, "retryable", False),
        "detail": getattr(exc, "detail", None),
    }


def _is_hard_provider_failure(exc: Exception) -> bool:
    return bool(getattr(exc, "hard_provider_failure", False))


def _parse_scope_key(scope_key: str) -> Tuple[Optional[str], Optional[str]]:
    """Extract (lot_id, bene_id) from a canonical scope_key string."""
    if scope_key == "document":
        return None, None
    if scope_key.startswith("lot:"):
        return scope_key[4:], None
    if scope_key.startswith("bene:"):
        rest = scope_key[5:]
        if "/" in rest:
            lot_id, bene_id = rest.split("/", 1)
            return lot_id, bene_id
    return None, None


def _build_synthetic_issue(
    case_key: str,
    scope_key: str,
    family: str,
    field_type: str,
    review: Dict[str, Any],
    raw_pages_idx: Dict[int, str],
) -> Dict[str, Any]:
    """
    Build a SUSPICIOUS_SILENCE clarification issue for a missing-slot cue-hit review.

    The issue is structurally compatible with the existing clarification issue schema
    so it can be passed directly to the LLM resolution machinery.

    Evidence supplied to the LLM comes from the cue windows (text around each cue
    hit) plus, if needed, the full page text for scope pages.  No new document
    reading — only what the bounded review already found.
    """
    lot_id, bene_id = _parse_scope_key(scope_key)
    cue_windows = review.get("cue_windows", [])
    scope_pages: List[int] = review.get("reviewed_pages", [])

    # Build local_text_windows: one window per unique cue-hit page, capped at 3.
    local_text_windows: List[Dict[str, Any]] = []
    seen_pages: Set[int] = set()
    for cw in cue_windows:
        pg = cw["page"]
        if pg in seen_pages:
            continue
        seen_pages.add(pg)
        # Prefer the full page text; fall back to the cue window excerpt.
        page_text = raw_pages_idx.get(pg, "")
        local_text_windows.append({
            "window_type": "exact_evidence_window",
            "page": pg,
            "anchor_line_index": None,
            "text": (page_text[:4500] if page_text else cw.get("window", ""))[:4500],
        })
        if len(local_text_windows) >= 3:
            break

    # For lot-scoped issues in multi-lot docs, ensure the lot section's first page
    # (which carries the "LOTTO N" heading) is included even when the cue hit is on
    # a later page.  Without it the scope guard cannot find the lot anchor and
    # rejects an otherwise correct high-confidence resolution.
    if lot_id is not None and bene_id is None and scope_pages:
        anchor_page = scope_pages[0]
        if anchor_page not in seen_pages:
            page_text = raw_pages_idx.get(anchor_page, "")
            if page_text:
                local_text_windows.append({
                    "window_type": "lot_anchor_context",
                    "page": anchor_page,
                    "anchor_line_index": None,
                    "text": page_text[:1200],
                })

    # If still no windows, include the first scope page even without a cue hit
    if not local_text_windows:
        for pg in scope_pages[:2]:
            page_text = raw_pages_idx.get(pg, "")
            if page_text:
                local_text_windows.append({
                    "window_type": "exact_evidence_window",
                    "page": pg,
                    "anchor_line_index": None,
                    "text": page_text[:4500],
                })

    # shell_quotes: the raw cue window text excerpts (at most 2)
    shell_quotes: List[str] = [cw.get("window", "")[:140] for cw in cue_windows[:2] if cw.get("window")]

    review_id = review["review_id"]
    issue_id = f"{review_id}::esc"

    return {
        "issue_id": issue_id,
        "case_key": case_key,
        "field_family": family,
        "field_type": field_type,
        "lot_id": lot_id,
        "bene_id": bene_id,
        "issue_type": "SUSPICIOUS_SILENCE",
        "deterministic_status": "UNRESOLVED",
        "reason_codes": ["MISSING_SLOT_CUE_HIT"],
        "candidate_values": [],
        "blocked_values": [],
        "supporting_candidates": [],
        "supporting_blocked_entries": [],
        "source_pages": scope_pages,
        "source_line_indices": [],
        "shell_quotes": shell_quotes,
        "local_text_windows": local_text_windows,
        "table_zone_types": review.get("table_zone_types", []),
        "scope_metadata": {
            "lot_id": lot_id,
            "bene_id": bene_id,
            "scope_key": scope_key,
            "attribution_bucket": None,
            "table_zone_types": review.get("table_zone_types", []),
            "candidate_scopes": [],
        },
        "needs_llm": True,
        "shell_sources": ["missing_slot_review_pack.json", "raw_pages.json"],
        "missing_slot_review_id": review_id,
    }


def run_missing_slot_escalation(
    case_key: str,
    review_results: Dict[Tuple[str, str], Dict[str, Any]],
    raw_pages_idx: Dict[int, str],
    artifact_dir: Path,
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """
    Run bounded LLM clarification for escalation-worthy missing-slot reviews.

    Only reviews with cue_hits=True are candidates.  Capped at
    MAX_ESCALATIONS_PER_CASE slots, prioritised by evidence density.
    One LLM resolution per slot maximum; transient provider retries are handled
    only by the shared request helper.

    Writes: missing_slot_escalation_pack.json to artifact_dir.
    Returns: mapping from (scope_key, field_type) → validated LLM resolution dict.
             Empty dict if LLM is unavailable or no escalation candidates exist.

    Raises LLMResolutionUnavailable if the API key is not configured.
    Per-slot LLM call failures are caught and recorded. Hard non-retryable
    provider failures stop later escalation calls for this case.
    """
    from .llm_resolution_pack import (
        discover_openai_config,
        resolve_single_issue,
        LLMResolutionUnavailable,
    )

    config = discover_openai_config()
    if not config["key_found"]:
        raise LLMResolutionUnavailable("OPENAI_API_KEY not configured; missing-slot escalation skipped")

    api_key = str(config["api_key"])
    model = str(config["model"])

    # Select escalation candidates: only reviews with cue hits
    candidates: List[Tuple[str, str, Dict[str, Any]]] = [
        (sk, ft, review)
        for (sk, ft), review in review_results.items()
        if review.get("cue_hits")
    ]

    # Sort: most cue windows first, then by field family priority
    candidates.sort(key=lambda x: (
        -len(x[2].get("cue_windows", [])),
        _FAMILY_ESCALATION_PRIORITY.get(x[2].get("field_family", ""), 9),
    ))
    candidates = candidates[:MAX_ESCALATIONS_PER_CASE]

    escalation_records: List[Dict[str, Any]] = []
    results: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for index, (sk, ft, review) in enumerate(candidates):
        family = review.get("field_family", _infer_family(ft))
        issue = _build_synthetic_issue(case_key, sk, ft, family, review, raw_pages_idx)

        record: Dict[str, Any] = {
            "scope_key": sk,
            "field_type": ft,
            "field_family": family,
            "review_id": review["review_id"],
            "issue_id": issue["issue_id"],
            "llm_attempted": False,
            "llm_succeeded": False,
            "llm_outcome": None,
            "resolution": None,
            "error": None,
            "llm_error": None,
            "provider_error": None,
            "skipped_due_to_provider_failure": False,
            "skip_reason": None,
        }
        hard_provider_failure = False

        try:
            record["llm_attempted"] = True
            resolution = resolve_single_issue(issue, api_key, model)
            record["llm_succeeded"] = True
            record["llm_outcome"] = resolution.get("llm_outcome")
            record["resolution"] = resolution
            results[(sk, ft)] = resolution
        except Exception as exc:
            record["error"] = str(exc)
            record["llm_error"] = str(exc)
            record["provider_error"] = _provider_failure_details(exc)
            hard_provider_failure = _is_hard_provider_failure(exc)

        escalation_records.append(record)

        if hard_provider_failure:
            for skipped_sk, skipped_ft, skipped_review in candidates[index + 1:]:
                skipped_family = skipped_review.get("field_family", _infer_family(skipped_ft))
                skipped_issue = _build_synthetic_issue(
                    case_key,
                    skipped_sk,
                    skipped_ft,
                    skipped_family,
                    skipped_review,
                    raw_pages_idx,
                )
                escalation_records.append({
                    "scope_key": skipped_sk,
                    "field_type": skipped_ft,
                    "field_family": skipped_family,
                    "review_id": skipped_review["review_id"],
                    "issue_id": skipped_issue["issue_id"],
                    "llm_attempted": False,
                    "llm_succeeded": False,
                    "llm_outcome": None,
                    "resolution": None,
                    "error": None,
                    "llm_error": None,
                    "provider_error": record["provider_error"],
                    "skipped_due_to_provider_failure": True,
                    "skip_reason": "skipped_due_to_provider_failure",
                })
            break

    pack: Dict[str, Any] = {
        "case_key": case_key,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "escalation_count": len(escalation_records),
        "llm_attempted": sum(1 for r in escalation_records if r["llm_attempted"]),
        "llm_succeeded": sum(1 for r in escalation_records if r["llm_succeeded"]),
        "provider": config.get("provider"),
        "model": model,
        "escalations": escalation_records,
    }
    out_path = artifact_dir / "missing_slot_escalation_pack.json"
    out_path.write_text(json.dumps(pack, ensure_ascii=False, indent=2), encoding="utf-8")

    return results
