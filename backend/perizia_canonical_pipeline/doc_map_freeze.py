"""
doc_map freeze stage.

Produces: doc_map.json

This stage deterministically freezes all pre-canon-accepted canonical artifacts
into one machine-readable, downstream-safe map for the case.

This is NOT a new extractor, shell, or LLM pass.
It freezes already-produced canonical artifacts into one artifact that is the
single canonical source of truth for downstream stages.

Inputs consumed (when present):
    pre_canon_validation_report.json  (required — freeze gate)
    evidence_ledger.json
    clarification_issue_pack.json
    llm_resolution_pack.json          (optional)
    lot_scope_map.json
    bene_scope_map.json
    table_zone_map.json               (optional)
    structure_hypotheses.json

Output:
    doc_map.json

Merge precedence (strict):
    1. pre-canon validator must approve case
    2. deterministic active truth remains baseline (not overridden by LLM)
    3. LLM resolved may fill slots that are blocked/unresolved
    4. LLM unresolved_explained attaches explanation to blocked slots
    5. LLM upgraded_context augments context_only slots
    6. blocked / context / unresolved / not_found are kept distinct

Field states:
    deterministic_active        — evidence_ledger ACTIVE packet; deterministic truth
    context_only                — evidence_ledger CONTEXT_ONLY or LLM upgraded_context
    llm_resolved                — LLM outcome=resolved (fills a previously blocked slot)
    unresolved_explained        — LLM outcome=unresolved_explained, or issue with no LLM run
    blocked                     — deterministic block with no issue or no resolution
    checked_no_reliable_basis   — bounded review ran for this slot; no freeze-safe basis found

Scope keys:
    document               — lot_id=None, bene_id=None
    lot:<lot_id>           — lot_id set, bene_id=None
    bene:<lot_id>/<bene_id> — both set
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from .corpus_registry import get_case
from .missing_slot_review import run_missing_slot_escalation, run_missing_slot_review
from .runner import build_context

# ---------------------------------------------------------------------------
# Field family inference
# ---------------------------------------------------------------------------

_FIELD_FAMILY_MAP: Dict[str, str] = {
    # structural
    "lot_header": "structural",
    "bene_header": "structural",
    # cadastral
    "cadastral_categoria": "cadastral",
    "cadastral_foglio": "cadastral",
    "cadastral_mappale": "cadastral",
    "cadastral_subalterno": "cadastral",
    "cadastral_superficie_mq": "cadastral",
    "cadastral_rendita_euro": "cadastral",
    "cadastral_consistenza_vani": "cadastral",
    "cadastral_classe": "cadastral",
    # location
    "location_via": "location",
    "location_civico": "location",
    "location_comune": "location",
    "location_provincia": "location",
    "location_piano": "location",
    "location_interno": "location",
    "location_cap": "location",
    "location_frazione": "location",
    "location_regione": "location",
    # rights
    "rights_diritto": "rights",
    "rights_quota_raw": "rights",
    # occupancy
    "occupancy_status_raw": "occupancy",
    "occupancy_title_raw": "occupancy",
    "occupancy_opponibilita_raw": "occupancy",
    "occupancy_saltuaria_raw": "occupancy",
    "occupancy_liberazione_raw": "occupancy",
    # valuation
    "valore_stima_raw": "valuation",
    "prezzo_base_raw": "valuation",
    "valore_perizia_raw": "valuation",
    # cost
    "cost_ripristino_raw": "cost",
    "cost_sanatoria_raw": "cost",
    "cost_regolarizzazione_raw": "cost",
    "cost_altri_oneri_quantificati_raw": "cost",
    "onere_non_quantificato_context": "cost",
    "ripristino_non_quantificato_context": "cost",
    "sanatoria_non_quantificata_context": "cost",
    # impianti
    "impianto_elettrico_status": "impianti",
    "impianto_idrico_status": "impianti",
    "impianto_riscaldamento_status": "impianti",
    "impianto_ascensore_status": "impianti",
    "impianto_gas_status": "impianti",
    "impianti_conformita_context": "impianti",
    "impianti_non_verificati_context": "impianti",
    "allacci_presenza_context": "impianti",
}

# Ordered prefix fallbacks (most specific first)
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

# Families that should appear in the field map (structural headers are scope metadata, not fields)
_FIELD_FAMILIES: Set[str] = {
    "cadastral", "location", "rights", "occupancy", "valuation", "cost", "impianti"
}

# Bounded primary field type universe used for not_found emission.
# Context-only supplementary field types (impianti_conformita_context, etc.) are excluded —
# those are optional enrichments, not expected truth slots.
# Each declared scope × each entry here gets a not_found entry if no other state applies.
_PRIMARY_FIELD_TYPES: Dict[str, List[str]] = {
    "cadastral": [
        "cadastral_foglio",
        "cadastral_mappale",
        "cadastral_subalterno",
        "cadastral_categoria",
    ],
    "location": [
        "location_via",
        "location_civico",
        "location_comune",
        "location_provincia",
    ],
    "rights": ["rights_diritto", "rights_quota_raw"],
    "occupancy": ["occupancy_status_raw"],
    "valuation": ["valore_stima_raw", "prezzo_base_raw"],
    "cost": [
        "cost_ripristino_raw",
        "cost_sanatoria_raw",
        "cost_regolarizzazione_raw",
        "cost_altri_oneri_quantificati_raw",
    ],
    "impianti": [
        "impianto_elettrico_status",
        "impianto_idrico_status",
        "impianto_riscaldamento_status",
    ],
}




def _build_scope_page_ranges(scope_index: Dict[str, Any]) -> Dict[str, List[int]]:
    """
    Build a mapping from scope_key → sorted list of page numbers for that scope.
    Derived from scope_index (already built from lot_scope_map + bene_scope_map).
    """
    ranges: Dict[str, List[int]] = {}
    for lot_entry in scope_index.get("lots", []):
        sk = lot_entry["scope_key"]
        start = lot_entry.get("start_page") or 1
        end = lot_entry.get("end_page") or start
        ranges[sk] = list(range(start, end + 1))
    for bene_entry in scope_index.get("benes", []):
        sk = bene_entry["scope_key"]
        start = bene_entry.get("start_page") or 1
        end = bene_entry.get("end_page") or start
        ranges[sk] = list(range(start, end + 1))
    return ranges


def _infer_family(field_type: str) -> str:
    """Infer field family from field_type string."""
    if not field_type:
        return "unknown"
    if field_type in _FIELD_FAMILY_MAP:
        return _FIELD_FAMILY_MAP[field_type]
    for prefix, family in _FAMILY_PREFIXES:
        if field_type.startswith(prefix):
            return family
    return "other"


# ---------------------------------------------------------------------------
# Scope key
# ---------------------------------------------------------------------------

def _scope_key(lot_id: Optional[str], bene_id: Optional[str]) -> str:
    """Return canonical scope key string."""
    if lot_id is None and bene_id is None:
        return "document"
    if bene_id is None:
        return f"lot:{lot_id}"
    return f"bene:{lot_id}/{bene_id}"


def _parse_scope_key(scope_key: str) -> Tuple[Optional[str], Optional[str]]:
    """Extract (lot_id, bene_id) from a canonical scope key."""
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


# ---------------------------------------------------------------------------
# IO helpers
# ---------------------------------------------------------------------------

def _load_json(path: Path) -> Optional[Any]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Missing-slot escalation scope guard
# ---------------------------------------------------------------------------

_BENE_ANCHOR_RE = re.compile(
    r"\bB\s*E\s*N\s*E\s*(?:N\s*[°º.]?|N\.?|NUM(?:ERO)?\.?)?\s*([0-9]+)\b",
    re.IGNORECASE,
)
_LOT_ANCHOR_RE = re.compile(
    r"\bLOTTO\s+(?:N\s*[°º.]?\s*)?(UNICO|[0-9]+)\b",
    re.IGNORECASE,
)


def _norm_token(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "").strip().lower())


def _norm_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _extract_bene_anchors(text: str) -> Set[str]:
    return {_norm_token(m.group(1)) for m in _BENE_ANCHOR_RE.finditer(text or "")}


def _extract_lot_anchors(text: str) -> Set[str]:
    anchors: Set[str] = set()
    for match in _LOT_ANCHOR_RE.finditer(text or ""):
        raw = _norm_token(match.group(1))
        anchors.add("unico" if raw in {"unico", "unica"} else raw)
    return anchors


def _window_around_quote(page_text: str, quote: str, radius: int = 900) -> str:
    """Return raw local text around an exact quote when the quote is findable."""
    if not page_text or not quote:
        return ""
    exact = str(quote).strip()
    idx = page_text.find(exact)
    if idx < 0:
        compact_page = _norm_text(page_text)
        compact_quote = _norm_text(exact)
        if compact_quote:
            idx = compact_page.find(compact_quote)
        if idx < 0:
            return ""
        return compact_page[max(0, idx - radius): idx + len(compact_quote) + radius]
    return page_text[max(0, idx - radius): idx + len(exact) + radius]


def _scope_entry(scope_index: Dict[str, Any], scope_key: str) -> Optional[Dict[str, Any]]:
    entries = scope_index.get("benes", []) if scope_key.startswith("bene:") else scope_index.get("lots", [])
    for entry in entries:
        if entry.get("scope_key") == scope_key:
            return entry
    return None


def _evidence_pages(resolution: Dict[str, Any]) -> Set[int]:
    pages: Set[int] = set()
    for evidence in resolution.get("supporting_evidence") or []:
        page = evidence.get("page") if isinstance(evidence, dict) else None
        if isinstance(page, int):
            pages.add(page)
    for page in resolution.get("source_pages") or []:
        if isinstance(page, int):
            pages.add(page)
    return pages


def _canonical_scope_supports_page(
    scope_index: Dict[str, Any],
    scope_key: str,
    pages: Set[int],
) -> bool:
    entry = _scope_entry(scope_index, scope_key)
    if not entry or not pages:
        return False
    first_page = entry.get("first_header_page") or entry.get("start_page")
    quote = str(entry.get("first_header_quote") or "")
    return bool(first_page in pages and quote)


def _scope_guard_failure(
    *,
    scope_key: str,
    resolution: Dict[str, Any],
    raw_pages_idx: Dict[int, str],
    scope_index: Dict[str, Any],
) -> Optional[str]:
    """
    Return a human-readable failure reason when a missing-slot LLM resolution's
    evidence is not scope-consistent enough to freeze as llm_resolved.
    """
    lot_id, bene_id = _parse_scope_key(scope_key)
    support_pages = _evidence_pages(resolution)
    evidence_items = resolution.get("supporting_evidence") or []

    if bene_id is not None:
        target = _norm_token(bene_id)
        has_target = _canonical_scope_supports_page(scope_index, scope_key, support_pages)
        conflicts: List[str] = []
        seen_conflicts: Set[str] = set()

        for evidence in evidence_items:
            if not isinstance(evidence, dict):
                continue
            page = evidence.get("page")
            quote = str(evidence.get("quote") or "")
            reason = str(evidence.get("reason") or "")
            page_text = raw_pages_idx.get(page, "") if isinstance(page, int) else ""
            local_window = _window_around_quote(page_text, quote)
            heading_window = page_text[:1800]
            evidence_text = " ".join([quote, reason, local_window, heading_window])
            anchors = _extract_bene_anchors(evidence_text)
            if target in anchors:
                has_target = True
            other = sorted(a for a in anchors if a != target)
            if other and target not in anchors:
                conflict = (
                    f"page {page} has explicit Bene anchor(s) {other} without target Bene {target}"
                )
                if conflict not in seen_conflicts:
                    seen_conflicts.add(conflict)
                    conflicts.append(conflict)

        if conflicts:
            return (
                "Scope guard rejected missing-slot resolution: supporting evidence "
                + "; ".join(conflicts[:3])
                + "."
            )
        if not has_target:
            return (
                "Scope guard rejected missing-slot resolution: no explicit or canonical "
                f"target Bene {target} anchor was found in the supporting evidence."
            )
        return None

    if lot_id is not None:
        target = "unico" if _norm_token(lot_id) in {"unico", "unica"} else _norm_token(lot_id)
        has_target = _canonical_scope_supports_page(scope_index, scope_key, support_pages)
        conflicts: List[str] = []
        seen_conflicts: Set[str] = set()

        for evidence in evidence_items:
            if not isinstance(evidence, dict):
                continue
            page = evidence.get("page")
            quote = str(evidence.get("quote") or "")
            reason = str(evidence.get("reason") or "")
            page_text = raw_pages_idx.get(page, "") if isinstance(page, int) else ""
            local_window = _window_around_quote(page_text, quote)
            heading_window = page_text[:1800]
            evidence_text = " ".join([quote, reason, local_window, heading_window])
            anchors = _extract_lot_anchors(evidence_text)
            if target in anchors:
                has_target = True
            other = sorted(a for a in anchors if a != target)
            if other and target not in anchors:
                conflict = (
                    f"page {page} has explicit Lotto anchor(s) {other} without target Lotto {target}"
                )
                if conflict not in seen_conflicts:
                    seen_conflicts.add(conflict)
                    conflicts.append(conflict)

        if conflicts:
            return (
                "Scope guard rejected missing-slot resolution: supporting evidence "
                + "; ".join(conflicts[:3])
                + "."
            )
        if not has_target:
            # Secondary fallback: evidence page within canonical lot page range is
            # sufficient proof of scope when no conflicting lot anchor was found.
            # Handles the common case where the cue page is mid-section and doesn't
            # repeat the "LOTTO N" heading that appears on the section start page.
            scope_entry = _scope_entry(scope_index, scope_key)
            if scope_entry:
                s_start = scope_entry.get("first_header_page") or scope_entry.get("start_page")
                s_end = scope_entry.get("end_page")
                s_quote = str(scope_entry.get("first_header_quote") or "")
                if s_start and s_end and s_quote and any(
                    s_start <= p <= s_end for p in support_pages
                ):
                    has_target = True
        if not has_target:
            return (
                "Scope guard rejected missing-slot resolution: no explicit or canonical "
                f"target Lotto {target} anchor was found in the supporting evidence."
            )
        return None

    # Document-level missing-slot escalation is currently not emitted, but keep
    # the acceptance rule explicit in case that boundary changes later.
    for evidence in evidence_items:
        if not isinstance(evidence, dict):
            continue
        text = " ".join([
            str(evidence.get("quote") or ""),
            str(evidence.get("reason") or ""),
        ])
        if _extract_bene_anchors(text):
            return (
                "Scope guard rejected missing-slot resolution: document-level target "
                "was supported only by a Bene-scoped evidence quote."
            )
    return None


# ---------------------------------------------------------------------------
# Scope index builder
# ---------------------------------------------------------------------------

def _build_scope_index(
    lot_scope: Dict[str, Any],
    bene_scope: Dict[str, Any],
) -> Dict[str, Any]:
    """Build the scope_index section from lot and bene scope maps."""
    lots = []
    for ls in lot_scope.get("lot_scopes", []):
        lots.append({
            "lot_id": ls.get("lot_id"),
            "scope_key": _scope_key(ls.get("lot_id"), None),
            "start_page": ls.get("start_page"),
            "end_page": ls.get("end_page"),
            "first_header_quote": ls.get("first_header_quote"),
            "range_reason": ls.get("range_reason"),
        })

    benes = []
    for bs in bene_scope.get("bene_scopes", []):
        benes.append({
            "lot_id": bs.get("lot_id"),
            "bene_id": bs.get("bene_id"),
            "composite_key": bs.get("composite_key"),
            "scope_key": _scope_key(bs.get("lot_id"), bs.get("bene_id")),
            "start_page": bs.get("start_page"),
            "end_page": bs.get("end_page"),
            "first_header_quote": bs.get("first_header_quote"),
            "attribution": bs.get("attribution"),
        })

    lot_keys = [lot["scope_key"] for lot in lots]
    bene_keys = [bene["scope_key"] for bene in benes]

    # Global pre-lot zone if present
    pre_lot = lot_scope.get("global_pre_lot_zone")

    return {
        "lots": lots,
        "benes": benes,
        "composite_scope_keys": lot_keys + bene_keys,
        "global_pre_lot_zone": pre_lot,
        "same_page_collisions": lot_scope.get("same_page_collisions", []),
        "scope_mode": lot_scope.get("scope_mode"),
        "bene_scope_mode": bene_scope.get("scope_mode"),
    }


# ---------------------------------------------------------------------------
# Blocked freeze artifact (for gate failures)
# ---------------------------------------------------------------------------

def _blocked_freeze(
    case_key: str,
    reason: str,
    freeze_status: str,
    artifact_dir: Path,
) -> Dict[str, Any]:
    source_artifacts = _build_source_artifacts(artifact_dir)
    return {
        "case_key": case_key,
        "status": "BLOCKED",
        "freeze_ready": False,
        "freeze_status": freeze_status,
        "case_summary": {
            "winner": None,
            "global_quality_tier": None,
            "is_unreadable": False,
            "lot_count": 0,
            "bene_count": 0,
            "has_llm_resolutions": False,
            "total_active_packets": 0,
            "total_context_packets": 0,
            "total_issues": 0,
            "total_unresolved_issues": 0,
            "total_blocked_zones": 0,
            "freeze_timestamp": datetime.now(timezone.utc).isoformat(),
        },
        "scope_index": {"lots": [], "benes": [], "composite_scope_keys": []},
        "fields": {},
        "unresolved_items": [],
        "context_items": [],
        "blocked_items": [{"reason": reason, "freeze_status": freeze_status}],
        "source_artifacts": source_artifacts,
        "warnings": [reason],
    }


# ---------------------------------------------------------------------------
# Source artifacts section
# ---------------------------------------------------------------------------

def _build_source_artifacts(artifact_dir: Path) -> Dict[str, Any]:
    names = [
        "pre_canon_validation_report.json",
        "evidence_ledger.json",
        "clarification_issue_pack.json",
        "llm_resolution_pack.json",
        "lot_scope_map.json",
        "bene_scope_map.json",
        "table_zone_map.json",
        "structure_hypotheses.json",
        "missing_slot_review_pack.json",
        "missing_slot_escalation_pack.json",
    ]
    return {
        name: str(artifact_dir / name) if (artifact_dir / name).exists() else None
        for name in names
    }


# ---------------------------------------------------------------------------
# Field map builder
# ---------------------------------------------------------------------------

def _resolve_scope(
    raw_sk: str,
    declared_scopes: Set[str],
    declared_lot_keys: Set[str],
    warnings: List[str],
    source_label: str,
) -> Optional[str]:
    """
    Validate a raw scope key against the declared scope registry.

    Returns the accepted (possibly remapped) scope key, or None if the scope
    cannot be safely placed and should be skipped.

    Remap rule (deterministic and provable):
      bene:X/Y where lot:X IS declared but bene:X/Y is NOT declared
      → remap to lot:X (the bene data belongs to a declared parent lot)

    Skip rule:
      anything else undeclared → None + warning (cannot safely remap)
    """
    if raw_sk == "document" or raw_sk in declared_scopes:
        return raw_sk

    # Try remap: undeclared bene whose parent lot is declared
    if raw_sk.startswith("bene:"):
        # bene:<lot_id>/<bene_id>
        rest = raw_sk[len("bene:"):]
        if "/" in rest:
            lot_id = rest.split("/", 1)[0]
            parent_lot_key = f"lot:{lot_id}"
            if parent_lot_key in declared_lot_keys:
                warnings.append(
                    f"[scope_enforcement] {source_label}: undeclared scope {raw_sk!r} "
                    f"remapped to declared parent {parent_lot_key!r}"
                )
                return parent_lot_key

    # Cannot remap — skip
    warnings.append(
        f"[scope_enforcement] {source_label}: undeclared scope {raw_sk!r} skipped; "
        f"not in scope_index and no declared parent lot available"
    )
    return None


def _build_field_map(
    ledger: Dict[str, Any],
    issue_pack: Dict[str, Any],
    resolution_map: Dict[str, Any],
    scope_index: Dict[str, Any],
    declared_scopes: Set[str],
    warnings: List[str],
) -> Tuple[Dict[str, Any], List[Any], List[Any], List[Any]]:
    """
    Build the frozen field map from evidence ledger + clarification issues + LLM resolutions.

    Scope enforcement: every field entry must land on a scope key that is either
    "document" or listed in scope_index.composite_scope_keys.  Packets/issues that
    reference undeclared bene scopes whose parent lot IS declared are remapped to
    that lot scope.  Everything else is skipped and recorded in warnings.

    Missing slots are NOT filled here.  After this function returns, the caller
    calls run_missing_slot_review to perform a bounded review for each missing
    primary field slot and fill in checked_no_reliable_basis entries with real
    traceability.

    Returns: (fields, unresolved_items, context_items, blocked_items)
    """
    packets = ledger.get("packets", [])
    blocked_zones = ledger.get("blocked_zones", [])
    issues = issue_pack.get("issues", [])

    # Pre-compute the set of declared lot keys for remap lookups
    declared_lot_keys: Set[str] = {
        sk for sk in declared_scopes if sk.startswith("lot:")
    }

    # ---- Index packets (with scope enforcement) ----
    # active_idx: (scope_key, field_type) -> list[packet]
    active_idx: Dict[Tuple[str, str], List[Any]] = {}
    # context_idx: (scope_key, field_type) -> list[packet]
    context_idx: Dict[Tuple[str, str], List[Any]] = {}

    for pkt in packets:
        ft = pkt.get("field_type", "")
        family = _infer_family(ft)
        if family not in _FIELD_FAMILIES:
            continue  # skip structural headers; they live in scope_index
        raw_sk = _scope_key(pkt.get("lot_id"), pkt.get("bene_id"))
        sk = _resolve_scope(raw_sk, declared_scopes, declared_lot_keys, warnings, f"packet {pkt.get('packet_id','?')}")
        if sk is None:
            continue
        status = pkt.get("status", "")
        key = (sk, ft)
        if status == "ACTIVE":
            active_idx.setdefault(key, []).append(pkt)
        elif status == "CONTEXT_ONLY":
            context_idx.setdefault(key, []).append(pkt)

    # ---- Index blocked zones (with scope enforcement) ----
    # bz_idx: (scope_key, field_type) -> list[blocked_zone]
    bz_idx: Dict[Tuple[str, str], List[Any]] = {}
    for bz in blocked_zones:
        ft = bz.get("field_type", "")
        if not ft:
            continue  # UNREADABLE_DOCUMENT blocks have no field_type
        family = _infer_family(ft)
        if family not in _FIELD_FAMILIES:
            continue
        raw_sk = _scope_key(bz.get("lot_id"), bz.get("bene_id"))
        sk = _resolve_scope(raw_sk, declared_scopes, declared_lot_keys, warnings, f"blocked_zone type={bz.get('type','?')}")
        if sk is None:
            continue
        bz_idx.setdefault((sk, ft), []).append(bz)

    # ---- Index issues (with scope enforcement) ----
    # issue_idx: (scope_key, field_type) -> list[issue]
    issue_idx: Dict[Tuple[str, str], List[Any]] = {}
    for iss in issues:
        ft = iss.get("field_type", "")
        if not ft:
            continue
        raw_sk = _scope_key(iss.get("lot_id"), iss.get("bene_id"))
        sk = _resolve_scope(raw_sk, declared_scopes, declared_lot_keys, warnings, f"issue {iss.get('issue_id','?')}")
        if sk is None:
            continue
        issue_idx.setdefault((sk, ft), []).append(iss)

    # ---- Collect all (scope_key, field_type, family) to process ----
    all_keys: Set[Tuple[str, str, str]] = set()
    for sk, ft in active_idx:
        all_keys.add((sk, ft, _infer_family(ft)))
    for sk, ft in context_idx:
        all_keys.add((sk, ft, _infer_family(ft)))
    for sk, ft in bz_idx:
        all_keys.add((sk, ft, _infer_family(ft)))
    for sk, ft in issue_idx:
        ft_family = _infer_family(ft)
        if ft_family in _FIELD_FAMILIES or ft_family == "other":
            all_keys.add((sk, ft, ft_family))

    # ---- Build field map from real evidence ----
    fields: Dict[str, Any] = {}
    unresolved_items: List[Any] = []
    context_items: List[Any] = []
    blocked_items: List[Any] = []

    for scope_key, field_type, family in sorted(all_keys):
        if family not in _FIELD_FAMILIES:
            continue

        a_packets = active_idx.get((scope_key, field_type), [])
        c_packets = context_idx.get((scope_key, field_type), [])
        bz_list = bz_idx.get((scope_key, field_type), [])
        field_issues = issue_idx.get((scope_key, field_type), [])

        # Collect resolutions for matching issues
        field_resolutions = []
        for iss in field_issues:
            iid = iss.get("issue_id")
            if iid and iid in resolution_map:
                field_resolutions.append(resolution_map[iid])

        entry = _build_field_entry(
            scope_key=scope_key,
            field_type=field_type,
            active_packets=a_packets,
            context_packets=c_packets,
            blocked_zones=bz_list,
            issues=field_issues,
            resolutions=field_resolutions,
            warnings=warnings,
        )

        # Place in nested fields structure: fields[scope_key][family][field_type]
        fields.setdefault(scope_key, {}).setdefault(family, {})[field_type] = entry

        # Populate summary lists
        state = entry["state"]
        if state == "unresolved_explained":
            unresolved_items.append({
                "scope_key": scope_key,
                "field_family": family,
                "field_type": field_type,
                "issue_ids": entry.get("issue_ids", []),
                "why_not_resolved": entry.get("why_not_resolved"),
                "needs_human_review": entry.get("needs_human_review", True),
            })
        elif state == "context_only":
            context_items.append({
                "scope_key": scope_key,
                "field_family": family,
                "field_type": field_type,
                "packet_ids": entry.get("packet_ids", []),
                "issue_ids": entry.get("issue_ids", []),
            })
        elif state == "blocked":
            blocked_items.append({
                "scope_key": scope_key,
                "field_family": family,
                "field_type": field_type,
                "issue_ids": entry.get("issue_ids", []),
                "explanation": entry.get("explanation"),
            })

    # ---- Also surface blocked zones WITHOUT matching issues ----
    # (deterministic blocks with no issue filed — not in all_keys because no issue_idx entry)
    for (sk, ft), bz_list in bz_idx.items():
        family = _infer_family(ft)
        if family not in _FIELD_FAMILIES:
            continue
        # Skip if already processed via the all_keys loop
        if (sk, ft, family) in all_keys:
            continue
        # No active, no context, no issue — pure silent block from evidence layer
        if not active_idx.get((sk, ft)) and not issue_idx.get((sk, ft)):
            entry = {
                "state": "blocked",
                "value": None,
                "value_type": None,
                "source_stage": "evidence_ledger",
                "confidence_band": None,
                "supporting_evidence": [
                    {
                        "reason": bz.get("reason"),
                        "distinct_values": bz.get("distinct_values", []),
                        "candidate_count": bz.get("candidate_count"),
                        "block_type": bz.get("type"),
                    }
                    for bz in bz_list
                ],
                "packet_ids": [],
                "issue_ids": [],
                "explanation": bz_list[0].get("reason") if bz_list else None,
                "why_not_resolved": "Deterministic block with no issue filed for LLM resolution.",
                "needs_human_review": False,
            }
            fields.setdefault(sk, {}).setdefault(family, {})[ft] = entry
            blocked_items.append({
                "scope_key": sk,
                "field_family": family,
                "field_type": ft,
                "issue_ids": [],
                "explanation": entry["explanation"],
            })

    return fields, unresolved_items, context_items, blocked_items


def _collect_missing_slots(
    fields: Dict[str, Any],
    declared_scopes: Set[str],
) -> List[Tuple[str, str, str]]:
    """
    Return (scope_key, family, field_type) for every primary field slot that has
    no evidence-based entry in fields.  Excludes the "document" catch-all scope.
    """
    missing: List[Tuple[str, str, str]] = []
    for sk in sorted(declared_scopes):
        if sk == "document":
            continue
        scope_families = fields.get(sk, {})
        for family, primary_fts in _PRIMARY_FIELD_TYPES.items():
            family_entries = scope_families.get(family, {})
            for ft in primary_fts:
                if ft not in family_entries:
                    missing.append((sk, family, ft))
    return missing


def _build_checked_entry_from_review(review: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a checked_no_reliable_basis field entry from a missing-slot review record.
    The entry carries real review traceability: review_ids, cue windows as
    supporting_evidence, and a field/scope-specific explanation.
    needs_human_review is True when the review found cue hits (topic is present
    in the text but did not yield a freeze-safe extraction).
    """
    cue_windows = review.get("cue_windows", [])
    supporting_evidence: List[Dict[str, Any]] = [
        {
            "page": w["page"],
            "cue_word": w["cue_word"],
            "window": w["window"],
        }
        for w in cue_windows
    ]
    return {
        "state": "checked_no_reliable_basis",
        "value": None,
        "value_type": None,
        "source_stage": "missing_slot_review",
        "confidence_band": None,
        "cues_found_in_scope_text": review.get("cue_hits", False),
        "supporting_evidence": supporting_evidence,
        "packet_ids": [],
        "issue_ids": [],
        "review_ids": [review["review_id"]],
        "explanation": review["explanation"],
        "why_not_resolved": review.get("explanation_basis"),
        "needs_human_review": review.get("needs_human_review", False),
    }


def _build_entry_from_escalation(
    review: Dict[str, Any],
    resolution: Dict[str, Any],
    raw_pages_idx: Dict[int, str],
    scope_index: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Build a field entry from an escalation LLM resolution result.

    Outcome mapping:
      resolved          → llm_resolved
      upgraded_context  → context_only
      unresolved_explained → unresolved_explained

    All entries carry review_ids (tracing back to the missing-slot review)
    plus issue_ids (the synthetic issue created for escalation).
    """
    outcome = resolution.get("llm_outcome", "unresolved_explained")
    review_ids = [review["review_id"]]
    issue_id = resolution.get("issue_id") or f"{review['review_id']}::esc"
    issue_ids = [issue_id]
    scope_key = review.get("scope_key", "")
    guard_failure = None
    if outcome == "resolved":
        guard_failure = _scope_guard_failure(
            scope_key=scope_key,
            resolution=resolution,
            raw_pages_idx=raw_pages_idx,
            scope_index=scope_index,
        )
        if guard_failure:
            outcome = "unresolved_explained"

    if outcome == "resolved":
        return {
            "state": "llm_resolved",
            "value": resolution.get("resolved_value"),
            "value_type": resolution.get("resolved_value_type"),
            "source_stage": "missing_slot_escalation",
            "confidence_band": resolution.get("confidence_band", "medium"),
            "supporting_evidence": resolution.get("supporting_evidence", []),
            "packet_ids": [],
            "issue_ids": issue_ids,
            "review_ids": review_ids,
            "explanation": resolution.get("user_visible_explanation"),
            "why_not_resolved": None,
            "needs_human_review": resolution.get("needs_human_review", False),
        }

    if outcome == "upgraded_context":
        # Merge cue windows as supplementary evidence alongside LLM evidence
        cue_evidence: List[Dict[str, Any]] = [
            {"page": w["page"], "cue_word": w.get("cue_word"), "window": w.get("window")}
            for w in review.get("cue_windows", [])
        ]
        return {
            "state": "context_only",
            "value": None,
            "value_type": None,
            "source_stage": "missing_slot_escalation",
            "confidence_band": resolution.get("confidence_band", "low"),
            "supporting_evidence": cue_evidence + (resolution.get("supporting_evidence") or []),
            "packet_ids": [],
            "issue_ids": issue_ids,
            "review_ids": review_ids,
            "explanation": resolution.get("user_visible_explanation"),
            "why_not_resolved": resolution.get("why_not_resolved"),
            "needs_human_review": resolution.get("needs_human_review", False),
        }

    # unresolved_explained (default)
    why_not_resolved = resolution.get("why_not_resolved")
    explanation = resolution.get("user_visible_explanation")
    rejected_alternatives = resolution.get("rejected_alternatives", [])
    if guard_failure:
        proposed = resolution.get("resolved_value")
        why_not_resolved = (
            f"{guard_failure} The LLM proposed {proposed!r}, but a missing-slot "
            "result cannot freeze as llm_resolved unless its supporting evidence "
            "is scope-consistent with the target slot."
        )
        explanation = (
            f"Escalation evidence was reviewed for {scope_key} / "
            f"{review.get('field_type')}. {guard_failure}"
        )
        rejected_alternatives = list(rejected_alternatives or []) + [{
            "value": proposed,
            "reason": guard_failure,
        }]
    return {
        "state": "unresolved_explained",
        "value": None,
        "value_type": None,
        "source_stage": "missing_slot_escalation",
        "confidence_band": resolution.get("confidence_band", "low"),
        "supporting_evidence": resolution.get("supporting_evidence", []),
        "packet_ids": [],
        "issue_ids": issue_ids,
        "review_ids": review_ids,
        "explanation": explanation,
        "why_not_resolved": why_not_resolved,
        "needs_human_review": True if guard_failure else resolution.get("needs_human_review", True),
        "rejected_alternatives": rejected_alternatives,
        "scope_guard": {
            "status": "failed",
            "reason": guard_failure,
        } if guard_failure else None,
    }


def _append_to_summary_lists(
    scope_key: str,
    family: str,
    field_type: str,
    entry: Dict[str, Any],
    unresolved_items: List[Any],
    context_items: List[Any],
) -> None:
    """Add an escalation-derived entry to the appropriate summary list."""
    state = entry.get("state")
    if state == "unresolved_explained":
        unresolved_items.append({
            "scope_key": scope_key,
            "field_family": family,
            "field_type": field_type,
            "issue_ids": entry.get("issue_ids", []),
            "why_not_resolved": entry.get("why_not_resolved"),
            "needs_human_review": entry.get("needs_human_review", True),
        })
    elif state == "context_only":
        context_items.append({
            "scope_key": scope_key,
            "field_family": family,
            "field_type": field_type,
            "packet_ids": entry.get("packet_ids", []),
            "issue_ids": entry.get("issue_ids", []),
        })


def _build_field_entry(
    scope_key: str,
    field_type: str,
    active_packets: List[Any],
    context_packets: List[Any],
    blocked_zones: List[Any],
    issues: List[Any],
    resolutions: List[Any],
    warnings: List[str],
) -> Dict[str, Any]:
    """
    Build a single frozen field entry for (scope_key, field_type).

    Merge precedence:
      1. ACTIVE packets → deterministic_active (not overridden by LLM or issues)
      2. LLM resolved → llm_resolved (only fills blocked slot)
      3. LLM unresolved_explained → unresolved_explained
      4. LLM upgraded_context → context_only (upgraded)
      5. Issue with no resolution → blocked or unresolved_explained (deterministic)
      6. CONTEXT_ONLY packets with no other signal → context_only
    """
    # ---- RULE 1: Deterministic active truth remains primary ----
    if active_packets:
        # Aggregate all active packets for this scope+field_type
        values = [p.get("extracted_value") for p in active_packets]
        unique_values = list(dict.fromkeys(str(v) for v in values if v is not None))

        # If multiple distinct values exist under the same ACTIVE scope+field_type,
        # the validator should have caught this. Warn but use first.
        if len(unique_values) > 1:
            warnings.append(
                f"scope={scope_key} field_type={field_type}: "
                f"multiple distinct ACTIVE values {unique_values}; using first."
            )

        first = active_packets[0]
        value = first.get("extracted_value")
        confidence = _aggregate_confidence(active_packets)

        # Build supporting evidence from all active packets
        supporting_evidence = [_packet_evidence(p) for p in active_packets]

        # Also note context packets as supplementary (do not change state)
        if context_packets:
            supporting_evidence.append({
                "note": f"{len(context_packets)} supplementary context packet(s) also present",
                "context_packet_ids": [p.get("packet_id") for p in context_packets],
            })

        issue_ids = [iss.get("issue_id") for iss in issues if iss.get("issue_id")]

        return {
            "state": "deterministic_active",
            "value": value,
            "value_type": "string" if isinstance(value, str) else type(value).__name__,
            "source_stage": "evidence_ledger",
            "confidence_band": confidence,
            "supporting_evidence": supporting_evidence,
            "packet_ids": [p.get("packet_id") for p in active_packets],
            "issue_ids": issue_ids,  # may exist as cross-reference but does NOT override
            "explanation": None,
            "why_not_resolved": None,
            "needs_human_review": False,
        }

    # ---- No deterministic active truth — check issues and resolutions ----
    if issues:
        issue_ids = [iss.get("issue_id") for iss in issues if iss.get("issue_id")]

        # Find the strongest resolution outcome
        # Priority: resolved > upgraded_context > unresolved_explained > no-resolution
        resolved_res = next(
            (r for r in resolutions if r.get("llm_outcome") == "resolved"), None
        )
        unresolved_res = next(
            (r for r in resolutions if r.get("llm_outcome") == "unresolved_explained"), None
        )
        upgraded_res = next(
            (r for r in resolutions if r.get("llm_outcome") == "upgraded_context"), None
        )

        if resolved_res:
            # LLM safely resolved the issue
            return {
                "state": "llm_resolved",
                "value": resolved_res.get("resolved_value"),
                "value_type": resolved_res.get("resolved_value_type"),
                "source_stage": "llm_resolution",
                "confidence_band": resolved_res.get("confidence_band", "medium"),
                "supporting_evidence": resolved_res.get("supporting_evidence", []),
                "packet_ids": [p.get("packet_id") for p in context_packets],
                "issue_ids": issue_ids,
                "explanation": resolved_res.get("user_visible_explanation"),
                "why_not_resolved": None,
                "needs_human_review": resolved_res.get("needs_human_review", False),
            }

        if unresolved_res:
            # LLM tried but could not resolve
            return {
                "state": "unresolved_explained",
                "value": None,
                "value_type": None,
                "source_stage": "llm_resolution",
                "confidence_band": unresolved_res.get("confidence_band", "low"),
                "supporting_evidence": unresolved_res.get("supporting_evidence", []),
                "packet_ids": [p.get("packet_id") for p in context_packets],
                "issue_ids": issue_ids,
                "explanation": unresolved_res.get("user_visible_explanation"),
                "why_not_resolved": unresolved_res.get("why_not_resolved"),
                "needs_human_review": unresolved_res.get("needs_human_review", True),
                "rejected_alternatives": unresolved_res.get("rejected_alternatives", []),
            }

        if upgraded_res:
            # LLM upgraded context — stays context, not resolved truth
            ctx_evidence = _build_context_evidence(context_packets)
            ctx_evidence.extend(upgraded_res.get("supporting_evidence", []))
            return {
                "state": "context_only",
                "value": None,
                "value_type": None,
                "source_stage": "llm_resolution",
                "confidence_band": upgraded_res.get("confidence_band", "medium"),
                "supporting_evidence": ctx_evidence,
                "packet_ids": [p.get("packet_id") for p in context_packets],
                "issue_ids": issue_ids,
                "explanation": upgraded_res.get("user_visible_explanation"),
                "why_not_resolved": upgraded_res.get("why_not_resolved"),
                "needs_human_review": upgraded_res.get("needs_human_review", False),
            }

        # Issue exists but no LLM resolution was run or available
        # Determine if it's a deterministic block or a suspicious silence
        first_issue = issues[0]
        issue_type = first_issue.get("issue_type", "")
        needs_llm = first_issue.get("needs_llm", False)

        # Build evidence from issue's supporting candidates/blocked entries
        issue_evidence = _build_issue_evidence(issues)
        if context_packets:
            issue_evidence.extend(_build_context_evidence(context_packets))

        if needs_llm:
            # Needs LLM but LLM was not run — report as unresolved_explained with
            # the deterministic explanation from the issue itself
            reason_codes = []
            for iss in issues:
                reason_codes.extend(iss.get("reason_codes", []))
            candidate_values = []
            for iss in issues:
                candidate_values.extend(iss.get("candidate_values", []))

            return {
                "state": "unresolved_explained",
                "value": None,
                "value_type": None,
                "source_stage": "clarification_issues",
                "confidence_band": "low",
                "supporting_evidence": issue_evidence,
                "packet_ids": [p.get("packet_id") for p in context_packets],
                "issue_ids": issue_ids,
                "explanation": (
                    f"Deterministic stage flagged issue type={issue_type}; "
                    f"reason_codes={reason_codes}; "
                    f"candidate_values={candidate_values}; "
                    f"LLM resolution not run."
                ),
                "why_not_resolved": "LLM resolution stage has not been applied to this issue.",
                "needs_human_review": True,
            }
        else:
            # Deterministic block — does not need LLM
            return {
                "state": "blocked",
                "value": None,
                "value_type": None,
                "source_stage": "clarification_issues",
                "confidence_band": None,
                "supporting_evidence": issue_evidence,
                "packet_ids": [p.get("packet_id") for p in context_packets],
                "issue_ids": issue_ids,
                "explanation": first_issue.get("reason_codes", []),
                "why_not_resolved": "Deterministic block; does not require LLM resolution.",
                "needs_human_review": False,
            }

    # ---- No active, no issues — check context_only packets ----
    if context_packets:
        return {
            "state": "context_only",
            "value": None,
            "value_type": None,
            "source_stage": "evidence_ledger",
            "confidence_band": "low",
            "supporting_evidence": _build_context_evidence(context_packets),
            "packet_ids": [p.get("packet_id") for p in context_packets],
            "issue_ids": [],
            "explanation": None,
            "why_not_resolved": None,
            "needs_human_review": False,
        }

    # ---- Only a blocked zone, no issue filed (handled in caller) ----
    # Should not reach here normally
    return {
        "state": "blocked",
        "value": None,
        "value_type": None,
        "source_stage": "evidence_ledger",
        "confidence_band": None,
        "supporting_evidence": [],
        "packet_ids": [],
        "issue_ids": [],
        "explanation": "Blocked deterministic zone with no matching packet or issue.",
        "why_not_resolved": None,
        "needs_human_review": False,
    }


def _packet_evidence(pkt: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "page": pkt.get("page"),
        "quote": pkt.get("quote"),
        "packet_id": pkt.get("packet_id"),
        "extraction_method": pkt.get("extraction_method"),
        "confidence": pkt.get("confidence"),
    }


def _build_context_evidence(packets: List[Any]) -> List[Dict[str, Any]]:
    return [
        {
            "page": p.get("page"),
            "quote": p.get("quote"),
            "packet_id": p.get("packet_id"),
            "extraction_method": p.get("extraction_method"),
            "note": "context_only",
        }
        for p in packets
    ]


def _build_issue_evidence(issues: List[Any]) -> List[Dict[str, Any]]:
    evidence = []
    for iss in issues:
        for cand in iss.get("supporting_candidates", []):
            evidence.append({
                "page": cand.get("page"),
                "quote": cand.get("quote"),
                "candidate_id": cand.get("candidate_id"),
                "extracted_value": cand.get("extracted_value"),
            })
        for blk in iss.get("supporting_blocked_entries", []):
            evidence.append({
                "page": blk.get("page"),
                "quote": blk.get("quote"),
                "block_type": blk.get("type"),
                "reason": blk.get("reason"),
            })
    return evidence


def _aggregate_confidence(packets: List[Any]) -> str:
    if not packets:
        return "none"
    avg = sum(p.get("confidence", 0.0) or 0.0 for p in packets) / len(packets)
    if avg >= 0.9:
        return "high"
    if avg >= 0.7:
        return "medium"
    return "low"


# ---------------------------------------------------------------------------
# Blocked items for BLOCKED_UNREADABLE cases
# ---------------------------------------------------------------------------

def _build_blocked_items_for_unreadable(ledger: Dict[str, Any]) -> List[Any]:
    blocked_zones = ledger.get("blocked_zones", [])
    items = []
    for bz in blocked_zones:
        if bz.get("type") == "UNREADABLE_DOCUMENT":
            items.append({
                "block_type": "UNREADABLE_DOCUMENT",
                "reason": bz.get("reason"),
                "unreadable_pages": bz.get("unreadable_pages", []),
            })
    if not items:
        items.append({
            "block_type": "UNREADABLE_DOCUMENT",
            "reason": "Evidence ledger reports BLOCKED_UNREADABLE winner; no field extraction possible.",
            "unreadable_pages": [],
        })
    return items


# ---------------------------------------------------------------------------
# Freeze status determination
# ---------------------------------------------------------------------------

def _determine_freeze_status(
    fields: Dict[str, Any],
    unresolved_items: List[Any],
    blocked_items: List[Any],
) -> str:
    if blocked_items:
        if unresolved_items:
            return "frozen_with_unresolved_and_blocks"
        return "frozen_with_blocks"
    if unresolved_items:
        return "frozen_with_unresolved"
    # Check if any field is not deterministic_active or llm_resolved
    has_context = False
    for scope_data in fields.values():
        for family_data in scope_data.values():
            for entry in family_data.values():
                state = entry.get("state")
                if state in ("unresolved_explained", "blocked"):
                    return "frozen_with_unresolved"
                if state == "context_only":
                    has_context = True
    if has_context:
        return "frozen_with_context_only"
    return "frozen_clean"


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def freeze_case(case_key: str) -> Dict[str, Any]:
    """
    Build and return the frozen doc_map for case_key.

    Reads all pre-canon artifacts from the case's artifact directory.
    Gates on pre_canon_validation_report.json; emits blocked freeze artifact
    if the validator has not approved the case.
    """
    ctx = build_context(case_key)
    artifact_dir = ctx.artifact_dir

    # ---- 1. Validator gate ----
    validator = _load_json(artifact_dir / "pre_canon_validation_report.json")
    if validator is None:
        return _blocked_freeze(
            case_key=case_key,
            reason="pre_canon_validation_report.json not found; cannot proceed with freeze.",
            freeze_status="blocked_no_validator",
            artifact_dir=artifact_dir,
        )
    if validator.get("status") == "FAIL" or not validator.get("freeze_ready", False):
        return _blocked_freeze(
            case_key=case_key,
            reason=(
                f"pre_canon validator gate not satisfied. "
                f"status={validator.get('status')} "
                f"freeze_ready={validator.get('freeze_ready')}. "
                f"summary: {validator.get('summary', '')}"
            ),
            freeze_status="blocked_validator_fail",
            artifact_dir=artifact_dir,
        )

    # ---- 2. Load artifacts ----
    ledger = _load_json(artifact_dir / "evidence_ledger.json") or {}
    issue_pack = _load_json(artifact_dir / "clarification_issue_pack.json") or {}
    resolution_pack = _load_json(artifact_dir / "llm_resolution_pack.json")
    lot_scope = _load_json(artifact_dir / "lot_scope_map.json") or {}
    bene_scope = _load_json(artifact_dir / "bene_scope_map.json") or {}
    table_zone = _load_json(artifact_dir / "table_zone_map.json") or {}
    structure = _load_json(artifact_dir / "structure_hypotheses.json") or {}

    # ---- 3. Detect BLOCKED_UNREADABLE ----
    ledger_winner = ledger.get("winner", "")
    ledger_status = ledger.get("status", "")
    is_unreadable = (
        ledger_winner == "BLOCKED_UNREADABLE"
        or ledger_status == "BLOCKED_UNREADABLE"
    )

    # ---- 4. Scope index ----
    scope_index = _build_scope_index(lot_scope, bene_scope)

    # ---- 5. Resolution map: issue_id → resolution ----
    resolution_map: Dict[str, Any] = {}
    _resolution_pack_issue_by_id: Dict[str, Any] = {}
    if resolution_pack:
        for res in resolution_pack.get("resolutions", []):
            iid = res.get("issue_id")
            if iid:
                resolution_map[iid] = res
        for iss in resolution_pack.get("issues", []):
            iid = iss.get("issue_id")
            if iid:
                _resolution_pack_issue_by_id[iid] = iss

    # ---- 6. Declared scope registry ----
    # "document" is always valid (unscoped catch-all).
    # All lot/bene scope keys from scope_index are the only other valid scopes.
    declared_scopes: Set[str] = {"document"} | set(scope_index.get("composite_scope_keys", []))

    # ---- 7. Raw pages index and scope page ranges (for cue sweep) ----
    raw_pages_data = _load_json(artifact_dir / "raw_pages.json") or []
    raw_pages_idx: Dict[int, str] = {
        p["page_number"]: p.get("text", "")
        for p in raw_pages_data
        if isinstance(p, dict) and "page_number" in p
    }
    scope_page_ranges: Dict[str, List[int]] = _build_scope_page_ranges(scope_index)

    # ---- 8. Field map ----
    warnings: List[str] = []

    if is_unreadable:
        fields: Dict[str, Any] = {}
        freeze_status = "blocked_unreadable"
        unresolved_items: List[Any] = []
        context_items: List[Any] = []
        blocked_items = _build_blocked_items_for_unreadable(ledger)
    else:
        fields, unresolved_items, context_items, blocked_items = _build_field_map(
            ledger=ledger,
            issue_pack=issue_pack,
            resolution_map=resolution_map,
            scope_index=scope_index,
            declared_scopes=declared_scopes,
            warnings=warnings,
        )

        # ---- Bounded missing-slot review + escalation ----
        # STEP 1: deterministic bounded review for every missing primary field slot.
        # STEP 2: bounded LLM escalation for cue-hit reviews (skipped gracefully
        #   when LLM is unavailable).
        # STEP 3: merge — escalation outcomes (llm_resolved / context_only /
        #   unresolved_explained) override checked_no_reliable_basis where the LLM
        #   succeeded; remaining slots keep checked_no_reliable_basis with full
        #   review traceability.
        missing_slots = _collect_missing_slots(fields, declared_scopes)
        if missing_slots:
            review_results = run_missing_slot_review(
                case_key=case_key,
                missing_slots=missing_slots,
                raw_pages_idx=raw_pages_idx,
                scope_page_ranges=scope_page_ranges,
                all_packets=ledger.get("packets", []),
                table_zone_data=table_zone,
                artifact_dir=artifact_dir,
            )

            # STEP 2 — escalation (graceful: skipped if LLM unavailable)
            escalation_results: Dict[Tuple[str, str], Any] = {}
            has_cue_hit_reviews = any(r.get("cue_hits") for r in review_results.values())
            if has_cue_hit_reviews:
                try:
                    escalation_results = run_missing_slot_escalation(
                        case_key=case_key,
                        review_results=review_results,
                        raw_pages_idx=raw_pages_idx,
                        artifact_dir=artifact_dir,
                    )
                except Exception as exc:
                    warnings.append(
                        f"[missing_slot_escalation] LLM escalation unavailable: {exc}"
                    )

            # STEP 3 — merge into fields
            for (sk, ft), review in review_results.items():
                family = _infer_family(ft)
                esc_resolution = escalation_results.get((sk, ft))

                if esc_resolution:
                    # Escalation produced an LLM-backed result — use it
                    entry = _build_entry_from_escalation(
                        review,
                        esc_resolution,
                        raw_pages_idx=raw_pages_idx,
                        scope_index=scope_index,
                    )
                    _append_to_summary_lists(
                        sk, family, ft, entry, unresolved_items, context_items
                    )
                else:
                    # No escalation warranted (no cues), or LLM unavailable/failed
                    entry = _build_checked_entry_from_review(review)

                fields.setdefault(sk, {}).setdefault(family, {})[ft] = entry

        # ---- Grouped llm_resolution_pack outputs (Fix A) ----
        # Resolutions whose field_type is a family name (e.g. "location") are
        # silently dropped by _build_field_map because _infer_family returns "other".
        # Collect them here without inventing scope or fabricating field values.
        grouped_llm_explanations: List[Dict[str, Any]] = []
        _grouped_declared_lot_keys: Set[str] = {
            sk for sk in declared_scopes if sk.startswith("lot:")
        }
        if resolution_pack:
            for res in resolution_pack.get("resolutions", []):
                iid = res.get("issue_id")
                if not iid:
                    continue
                iss = _resolution_pack_issue_by_id.get(iid)
                if not iss:
                    continue
                ft = iss.get("field_type", "")
                if _infer_family(ft) != "other":
                    continue  # handled by normal field-map path
                raw_sk = _scope_key(iss.get("lot_id"), iss.get("bene_id"))
                sk = _resolve_scope(
                    raw_sk, declared_scopes, _grouped_declared_lot_keys,
                    warnings, f"grouped_llm issue {iid}",
                )
                if sk is None:
                    continue
                lot_id_hint = iss.get("lot_id")
                bene_id_hint = iss.get("bene_id")
                scope_key_hint = (
                    iss.get("scope_metadata", {}).get("scope_key")
                    if isinstance(iss.get("scope_metadata"), dict)
                    else None
                ) or sk
                grouped_llm_explanations.append({
                    "issue_id": iid,
                    "scope_key": sk,
                    "scope_key_hint": scope_key_hint,
                    "lot_id": lot_id_hint,
                    "bene_id": bene_id_hint,
                    "field_type": ft,
                    "llm_outcome": res.get("llm_outcome"),
                    "user_visible_explanation": res.get("user_visible_explanation"),
                    "why_not_resolved": res.get("why_not_resolved"),
                    "confidence_band": res.get("confidence_band"),
                    "needs_human_review": res.get("needs_human_review"),
                })

        freeze_status = _determine_freeze_status(
            fields=fields,
            unresolved_items=unresolved_items,
            blocked_items=blocked_items,
        )

    # ---- 9. Case summary ----
    packets = ledger.get("packets", [])
    active_count = sum(1 for p in packets if p.get("status") == "ACTIVE")
    context_count = sum(1 for p in packets if p.get("status") == "CONTEXT_ONLY")
    all_issues = issue_pack.get("issues", [])
    unresolved_count = sum(
        1 for i in all_issues if i.get("deterministic_status") == "UNRESOLVED"
    )

    case_summary = {
        "winner": ledger_winner or structure.get("winner"),
        "global_quality_tier": ledger.get("global_quality_tier"),
        "is_unreadable": is_unreadable,
        "lot_count": len(scope_index.get("lots", [])),
        "bene_count": len(scope_index.get("benes", [])),
        "has_llm_resolutions": bool(resolution_map),
        "total_active_packets": active_count,
        "total_context_packets": context_count,
        "total_issues": len(all_issues),
        "total_unresolved_issues": unresolved_count,
        "total_blocked_zones": len(ledger.get("blocked_zones", [])),
        "table_zones_detected": len(table_zone.get("table_zones", []) or table_zone.get("zones", [])),
        "freeze_timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # ---- 10. Source artifacts ----
    source_artifacts = _build_source_artifacts(artifact_dir)

    return {
        "case_key": case_key,
        "status": "BLOCKED_UNREADABLE" if is_unreadable else "OK",
        "freeze_ready": True,
        "freeze_status": freeze_status,
        "case_summary": case_summary,
        "scope_index": scope_index,
        "fields": fields,
        "unresolved_items": unresolved_items,
        "context_items": context_items,
        "blocked_items": blocked_items,
        "grouped_llm_explanations": grouped_llm_explanations if not is_unreadable else [],
        "source_artifacts": source_artifacts,
        "warnings": warnings,
    }


def run_freeze(case_key: str) -> Path:
    """Run freeze and write doc_map.json to the case artifact dir. Returns path."""
    ctx = build_context(case_key)
    doc_map = freeze_case(case_key)
    out_path = ctx.artifact_dir / "doc_map.json"
    out_path.write_text(json.dumps(doc_map, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path


if __name__ == "__main__":
    import argparse
    from .corpus_registry import list_case_keys

    parser = argparse.ArgumentParser(description="doc_map freeze stage")
    parser.add_argument("--case", required=True, choices=list_case_keys())
    args = parser.parse_args()

    out = run_freeze(args.case)
    print(f"WROTE_DOC_MAP={out}")
