"""
Pre-canon deterministic validator.

Produces: pre_canon_validation_report.json

This stage does NOT call any LLM.
It inspects the current canonical artifact set for a case and emits a
machine-readable validation report suitable for doc_map freeze gating.

Checks implemented:
  ARTIFACT_001  required_artifacts_present
  ARTIFACT_002  artifact_json_loadable
  ARTIFACT_003  artifact_top_level_fields

  SCOPE_001     ledger_lot_scope_keys_valid
  SCOPE_002     ledger_bene_scope_keys_valid
  SCOPE_003     table_zone_scope_metadata_sane

  EVIDENCE_001  evidence_pages_exist_in_raw_pages
  EVIDENCE_002  llm_resolution_source_pages_valid
  EVIDENCE_003  llm_resolution_evidence_pages_bounded

  PACKET_001    no_same_scope_active_conflicts
  PACKET_002    context_only_not_masquerading_as_active
  PACKET_003    blocked_not_silently_duplicated_as_active
  PACKET_004    unreadable_no_fake_active_packets

  ISSUE_001     issue_pack_schema_valid
  ISSUE_002     issue_ids_unique
  ISSUE_003     issue_types_allowed
  ISSUE_004     issue_needs_llm_coherent

  RESOLUTION_001  llm_resolution_schema_valid
  RESOLUTION_002  llm_resolution_issue_links_valid
  RESOLUTION_003  llm_resolution_outcome_requirements
  RESOLUTION_004  llm_resolution_provider_model_present
  RESOLUTION_005  no_resolution_pack_for_unreadable_no_issues

  FREEZE_001    freeze_blockers_none
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from .corpus_registry import get_case
from .runner import build_context

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ALLOWED_ISSUE_TYPES = {
    "FIELD_CONFLICT",
    "SUSPICIOUS_SILENCE",
    "SCOPE_AMBIGUITY",
    "GROUPED_CONTEXT_NEEDS_EXPLANATION",
    "OCR_VARIANT_COLLISION",
    "TABLE_RECAP_DUPLICATE_UNCLEAR",
}

ALLOWED_LLM_OUTCOMES = {"resolved", "unresolved_explained", "upgraded_context"}

REQUIRED_ARTIFACTS = [
    "raw_pages.json",
    "extract_metrics.json",
    "structure_hypotheses.json",
    "lot_scope_map.json",
    "bene_scope_map.json",
    "evidence_ledger.json",
    "clarification_issue_pack.json",
]

OPTIONAL_ARTIFACTS = [
    "table_zone_map.json",
    "llm_resolution_pack.json",
]

# Top-level required fields per artifact
ARTIFACT_REQUIRED_FIELDS: Dict[str, List[str]] = {
    "raw_pages.json": [],                              # list, checked separately
    "extract_metrics.json": ["case_key", "global_quality_tier", "pages_count"],
    "structure_hypotheses.json": ["case_key", "winner"],
    "lot_scope_map.json": ["case_key", "status", "scope_mode"],
    "bene_scope_map.json": ["case_key", "scope_mode"],
    "evidence_ledger.json": ["case_key", "status", "packets"],
    "clarification_issue_pack.json": ["case_key", "status", "issue_count", "issues"],
    "table_zone_map.json": ["case_key", "status"],
    "llm_resolution_pack.json": ["case_key", "status", "issue_count", "resolutions"],
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load(path: Path) -> Optional[Any]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False  # sentinel: file exists but unloadable


def _check(
    check_id: str,
    check_name: str,
    status: str,
    severity: str,
    details: str,
    affected_artifacts: Optional[List[str]] = None,
    affected_pages: Optional[List[int]] = None,
    affected_issue_ids: Optional[List[str]] = None,
    affected_packet_ids: Optional[List[str]] = None,
) -> Dict:
    return {
        "check_id": check_id,
        "check_name": check_name,
        "status": status,
        "severity": severity,
        "details": details,
        "affected_artifacts": affected_artifacts or [],
        "affected_pages": affected_pages or [],
        "affected_issue_ids": affected_issue_ids or [],
        "affected_packet_ids": affected_packet_ids or [],
    }


def _pass(check_id: str, check_name: str, details: str = "OK",
          affected_artifacts: Optional[List[str]] = None) -> Dict:
    return _check(check_id, check_name, "PASS", "INFO", details,
                  affected_artifacts=affected_artifacts)


def _warn(check_id: str, check_name: str, details: str,
          severity: str = "WARN",
          affected_artifacts: Optional[List[str]] = None,
          affected_pages: Optional[List[int]] = None,
          affected_issue_ids: Optional[List[str]] = None,
          affected_packet_ids: Optional[List[str]] = None) -> Dict:
    return _check(check_id, check_name, "WARN", severity, details,
                  affected_artifacts=affected_artifacts,
                  affected_pages=affected_pages,
                  affected_issue_ids=affected_issue_ids,
                  affected_packet_ids=affected_packet_ids)


def _fail(check_id: str, check_name: str, details: str,
          affected_artifacts: Optional[List[str]] = None,
          affected_pages: Optional[List[int]] = None,
          affected_issue_ids: Optional[List[str]] = None,
          affected_packet_ids: Optional[List[str]] = None) -> Dict:
    return _check(check_id, check_name, "FAIL", "ERROR", details,
                  affected_artifacts=affected_artifacts,
                  affected_pages=affected_pages,
                  affected_issue_ids=affected_issue_ids,
                  affected_packet_ids=affected_packet_ids)


# ---------------------------------------------------------------------------
# Domain checks
# ---------------------------------------------------------------------------

def _check_artifact_presence(artifact_dir: Path, artifacts: Dict[str, Any]) -> List[Dict]:
    results = []
    missing_required = []
    for name in REQUIRED_ARTIFACTS:
        if name not in artifacts or artifacts[name] is None:
            missing_required.append(name)
    if missing_required:
        results.append(_fail(
            "ARTIFACT_001", "required_artifacts_present",
            f"Missing required artifacts: {missing_required}",
            affected_artifacts=missing_required,
        ))
    else:
        results.append(_pass("ARTIFACT_001", "required_artifacts_present",
                             f"All {len(REQUIRED_ARTIFACTS)} required artifacts present",
                             affected_artifacts=REQUIRED_ARTIFACTS))

    unloadable = []
    for name, data in artifacts.items():
        if data is False:  # exists but failed to load
            unloadable.append(name)
    if unloadable:
        results.append(_fail(
            "ARTIFACT_002", "artifact_json_loadable",
            f"JSON parse failed for: {unloadable}",
            affected_artifacts=unloadable,
        ))
    else:
        results.append(_pass("ARTIFACT_002", "artifact_json_loadable",
                             "All present artifacts parse as valid JSON"))

    bad_fields = []
    for name, required_keys in ARTIFACT_REQUIRED_FIELDS.items():
        if not required_keys:
            continue
        data = artifacts.get(name)
        if data is None or data is False or not isinstance(data, dict):
            continue
        missing_keys = [k for k in required_keys if k not in data]
        if missing_keys:
            bad_fields.append(f"{name}: missing {missing_keys}")
    if bad_fields:
        results.append(_warn(
            "ARTIFACT_003", "artifact_top_level_fields",
            f"Some artifacts missing expected top-level fields: {bad_fields}",
            affected_artifacts=[b.split(":")[0] for b in bad_fields],
        ))
    else:
        results.append(_pass("ARTIFACT_003", "artifact_top_level_fields",
                             "All present artifacts have expected top-level fields"))
    return results


def _check_scope_coherence(
    artifacts: Dict[str, Any],
    lot_ids_in_map: Set[str],
    bene_ids_in_map: Set[str],
    composite_bene_refs: Set[str],
) -> List[Dict]:
    """
    composite_bene_refs: set of valid "lot_id/bene_id" strings as emitted by
    table_zone_map (e.g. "unico/1", "3/1").  Validated alongside plain bene_ids.
    """
    results = []
    ledger = artifacts.get("evidence_ledger.json")
    if not isinstance(ledger, dict):
        return results

    packets = ledger.get("packets") or []
    bad_lot_refs = []
    bad_bene_refs = []
    for pkt in packets:
        lot_id = pkt.get("lot_id")
        bene_id = pkt.get("bene_id")
        pkt_id = pkt.get("packet_id", "?")
        if lot_id is not None and lot_ids_in_map and lot_id not in lot_ids_in_map:
            bad_lot_refs.append(f"{pkt_id}->lot:{lot_id}")
        if bene_id is not None and bene_ids_in_map and bene_id not in bene_ids_in_map:
            bad_bene_refs.append(f"{pkt_id}->bene:{bene_id}")

    if bad_lot_refs:
        results.append(_fail(
            "SCOPE_001", "ledger_lot_scope_keys_valid",
            f"Packets reference lot_ids not in lot_scope_map: {bad_lot_refs[:10]}",
            affected_artifacts=["evidence_ledger.json", "lot_scope_map.json"],
            affected_packet_ids=[r.split("->")[0] for r in bad_lot_refs[:10]],
        ))
    else:
        results.append(_pass("SCOPE_001", "ledger_lot_scope_keys_valid",
                             "All lot_id references in ledger packets are valid"))

    if bad_bene_refs:
        results.append(_fail(
            "SCOPE_002", "ledger_bene_scope_keys_valid",
            f"Packets reference bene_ids not in bene_scope_map: {bad_bene_refs[:10]}",
            affected_artifacts=["evidence_ledger.json", "bene_scope_map.json"],
            affected_packet_ids=[r.split("->")[0] for r in bad_bene_refs[:10]],
        ))
    else:
        results.append(_pass("SCOPE_002", "ledger_bene_scope_keys_valid",
                             "All bene_id references in ledger packets are valid"))

    # Table zone scope metadata.
    # local_bene_id may be stored as plain "bene_id" OR as "lot_id/bene_id" composite.
    # Both forms are valid per the accepted table_zone_map design.
    tzm = artifacts.get("table_zone_map.json")
    if isinstance(tzm, dict):
        zones = tzm.get("table_zones") or tzm.get("zones") or []
        bad_zones = []
        for z in zones:
            zone_id = z.get("zone_id", "?")
            lot_id = z.get("local_lot_id")
            bene_id = z.get("local_bene_id")
            # null scope is acceptable for recap / global zones
            if lot_id is not None and lot_ids_in_map and lot_id not in lot_ids_in_map:
                bad_zones.append(f"{zone_id}->lot:{lot_id}")
            if bene_id is not None and bene_ids_in_map:
                # Accept plain form or composite form
                plain_ok = bene_id in bene_ids_in_map
                composite_ok = bene_id in composite_bene_refs
                # Also accept if the bene_id parses as "lot_id/bene_id" where bene_id is known
                if "/" in str(bene_id):
                    parts = str(bene_id).split("/", 1)
                    bare_bene = parts[1] if len(parts) == 2 else bene_id
                    plain_ok = plain_ok or (bare_bene in bene_ids_in_map)
                if not (plain_ok or composite_ok):
                    bad_zones.append(f"{zone_id}->bene:{bene_id}")
        if bad_zones:
            results.append(_fail(
                "SCOPE_003", "table_zone_scope_metadata_sane",
                f"Table zones reference invalid scope keys: {bad_zones[:10]}",
                affected_artifacts=["table_zone_map.json"],
            ))
        else:
            results.append(_pass("SCOPE_003", "table_zone_scope_metadata_sane",
                                 "Table zone scope metadata is coherent"))
    else:
        results.append(_pass("SCOPE_003", "table_zone_scope_metadata_sane",
                             "table_zone_map.json not present; check skipped"))
    return results


def _check_evidence_coherence(
    artifacts: Dict[str, Any],
    raw_page_numbers: Set[int],
) -> List[Dict]:
    results = []

    ledger = artifacts.get("evidence_ledger.json")
    if isinstance(ledger, dict) and raw_page_numbers:
        packets = ledger.get("packets") or []
        bad_pages = []
        for pkt in packets:
            page = pkt.get("page")
            if page is not None and page not in raw_page_numbers:
                bad_pages.append((pkt.get("packet_id", "?"), page))
        if bad_pages:
            pages_list = sorted({p for _, p in bad_pages})
            results.append(_fail(
                "EVIDENCE_001", "evidence_pages_exist_in_raw_pages",
                f"Ledger packets reference pages not in raw_pages: {bad_pages[:10]}",
                affected_artifacts=["evidence_ledger.json", "raw_pages.json"],
                affected_pages=pages_list,
                affected_packet_ids=[pid for pid, _ in bad_pages[:10]],
            ))
        else:
            results.append(_pass("EVIDENCE_001", "evidence_pages_exist_in_raw_pages",
                                 "All ledger packet pages exist in raw_pages"))
    else:
        results.append(_pass("EVIDENCE_001", "evidence_pages_exist_in_raw_pages",
                             "Skipped (no ledger or no raw pages)"))

    res_pack = artifacts.get("llm_resolution_pack.json")
    if isinstance(res_pack, dict) and raw_page_numbers:
        issue_pack = artifacts.get("clarification_issue_pack.json") or {}
        issues_by_id = {iss["issue_id"]: iss for iss in (issue_pack.get("issues") or []) if "issue_id" in iss}

        bad_src_pages = []
        bad_evid_pages = []
        for res in (res_pack.get("resolutions") or []):
            issue_id = res.get("issue_id", "?")
            src_pages = res.get("source_pages") or []
            for p in src_pages:
                if p not in raw_page_numbers:
                    bad_src_pages.append((issue_id, p))

            # Evidence pages must belong to the raw pages
            for ev in (res.get("supporting_evidence") or []):
                ep = ev.get("page")
                if ep is not None and ep not in raw_page_numbers:
                    bad_evid_pages.append((issue_id, ep))

            # Evidence pages must also be bounded to issue source pages ± 5 page window
            iss = issues_by_id.get(issue_id)
            if iss:
                iss_pages = set(iss.get("source_pages") or [])
                lo = min(iss_pages) - 5 if iss_pages else 0
                hi = max(iss_pages) + 5 if iss_pages else 9999
                for ev in (res.get("supporting_evidence") or []):
                    ep = ev.get("page")
                    if ep is not None and ep in raw_page_numbers and not (lo <= ep <= hi):
                        bad_evid_pages.append((f"{issue_id}(unbounded)", ep))

        if bad_src_pages:
            results.append(_fail(
                "EVIDENCE_002", "llm_resolution_source_pages_valid",
                f"LLM resolution source_pages reference pages not in raw_pages: {bad_src_pages[:10]}",
                affected_artifacts=["llm_resolution_pack.json", "raw_pages.json"],
                affected_pages=sorted({p for _, p in bad_src_pages}),
                affected_issue_ids=[iid for iid, _ in bad_src_pages[:10]],
            ))
        else:
            results.append(_pass("EVIDENCE_002", "llm_resolution_source_pages_valid",
                                 "All LLM resolution source pages exist in raw_pages"))

        if bad_evid_pages:
            results.append(_warn(
                "EVIDENCE_003", "llm_resolution_evidence_pages_bounded",
                f"LLM resolution evidence pages out of bounded issue windows: {bad_evid_pages[:10]}",
                affected_artifacts=["llm_resolution_pack.json"],
                affected_pages=sorted({p for _, p in bad_evid_pages}),
                affected_issue_ids=list({iid for iid, _ in bad_evid_pages[:10]}),
            ))
        else:
            results.append(_pass("EVIDENCE_003", "llm_resolution_evidence_pages_bounded",
                                 "All LLM resolution evidence pages are within bounded windows"))
    else:
        results.append(_pass("EVIDENCE_002", "llm_resolution_source_pages_valid",
                             "No llm_resolution_pack present; check skipped"))
        results.append(_pass("EVIDENCE_003", "llm_resolution_evidence_pages_bounded",
                             "No llm_resolution_pack present; check skipped"))
    return results


def _check_packet_sanity(artifacts: Dict[str, Any]) -> List[Dict]:
    results = []
    ledger = artifacts.get("evidence_ledger.json")
    if not isinstance(ledger, dict):
        return results

    packets = ledger.get("packets") or []
    status = ledger.get("status", "")

    # PACKET_004: unreadable case must have zero active packets
    if status == "BLOCKED_UNREADABLE":
        active_pkts = [p for p in packets if p.get("status") == "ACTIVE"]
        if active_pkts:
            results.append(_fail(
                "PACKET_004", "unreadable_no_fake_active_packets",
                f"BLOCKED_UNREADABLE case has {len(active_pkts)} ACTIVE packets; expected 0",
                affected_artifacts=["evidence_ledger.json"],
                affected_packet_ids=[p.get("packet_id", "?") for p in active_pkts[:10]],
            ))
        else:
            results.append(_pass("PACKET_004", "unreadable_no_fake_active_packets",
                                 "BLOCKED_UNREADABLE case has no fake active packets"))
    else:
        results.append(_pass("PACKET_004", "unreadable_no_fake_active_packets",
                             "Case is not BLOCKED_UNREADABLE; check n/a"))

    # PACKET_001: no same-scope conflicting ACTIVE packets for same field_type
    # Group active packets by (lot_id, bene_id, field_type) and look for conflicts
    from collections import defaultdict
    active_by_scope_field: Dict[tuple, List] = defaultdict(list)
    for pkt in packets:
        if pkt.get("status") == "ACTIVE":
            key = (pkt.get("lot_id"), pkt.get("bene_id"), pkt.get("field_type"))
            active_by_scope_field[key].append(pkt)

    conflicts = []
    for (lot_id, bene_id, field_type), pkts in active_by_scope_field.items():
        if len(pkts) > 1:
            values = [p.get("extracted_value") for p in pkts]
            # Only a conflict if distinct non-null values
            distinct = set(v for v in values if v is not None)
            if len(distinct) > 1:
                conflicts.append({
                    "scope": f"lot:{lot_id}/bene:{bene_id}",
                    "field_type": field_type,
                    "distinct_values": list(distinct),
                    "packet_ids": [p.get("packet_id", "?") for p in pkts],
                })

    if conflicts:
        results.append(_fail(
            "PACKET_001", "no_same_scope_active_conflicts",
            f"Found {len(conflicts)} same-scope/field_type ACTIVE packet conflicts: {conflicts[:5]}",
            affected_artifacts=["evidence_ledger.json"],
            affected_packet_ids=[pid for c in conflicts[:5] for pid in c["packet_ids"]],
        ))
    else:
        results.append(_pass("PACKET_001", "no_same_scope_active_conflicts",
                             "No same-scope conflicting ACTIVE packets"))

    # PACKET_002: context-only field families should not appear as ACTIVE resolved truth
    # (only check for known context field types — do not over-police)
    CONTEXT_ONLY_TYPES = {
        "cost_context_only",
        "impianti_context_only",
    }
    bad_context = [
        p for p in packets
        if p.get("status") == "ACTIVE" and p.get("field_type") in CONTEXT_ONLY_TYPES
        and p.get("extracted_value") is not None
    ]
    if bad_context:
        results.append(_warn(
            "PACKET_002", "context_only_not_masquerading_as_active",
            f"Context-only typed packets appear as ACTIVE with non-null extracted_value: "
            f"{[p.get('packet_id') for p in bad_context[:5]]}",
            affected_artifacts=["evidence_ledger.json"],
            affected_packet_ids=[p.get("packet_id", "?") for p in bad_context[:5]],
        ))
    else:
        results.append(_pass("PACKET_002", "context_only_not_masquerading_as_active",
                             "No context-only typed packets masquerading as active resolved truth"))

    # PACKET_003: blocked states should not be silently duplicated as ACTIVE in same scope/field
    blocked_by_scope_field: Dict[tuple, List] = defaultdict(list)
    for pkt in packets:
        if pkt.get("status") in {"BLOCKED", "BLOCKED_CONFLICT", "BLOCKED_UNRESOLVED"}:
            key = (pkt.get("lot_id"), pkt.get("bene_id"), pkt.get("field_type"))
            blocked_by_scope_field[key].append(pkt)

    duplicate_blocks = []
    for key, blocked_pkts in blocked_by_scope_field.items():
        active_pkts_for_key = active_by_scope_field.get(key, [])
        if active_pkts_for_key:
            duplicate_blocks.append({
                "scope_field": key,
                "blocked_ids": [p.get("packet_id", "?") for p in blocked_pkts[:3]],
                "active_ids": [p.get("packet_id", "?") for p in active_pkts_for_key[:3]],
            })
    if duplicate_blocks:
        results.append(_warn(
            "PACKET_003", "blocked_not_silently_duplicated_as_active",
            f"Found {len(duplicate_blocks)} scope/field keys with both BLOCKED and ACTIVE packets: "
            f"{duplicate_blocks[:5]}",
            affected_artifacts=["evidence_ledger.json"],
        ))
    else:
        results.append(_pass("PACKET_003", "blocked_not_silently_duplicated_as_active",
                             "No blocked states silently duplicated as ACTIVE in same scope/field"))

    return results


def _check_issue_pack(artifacts: Dict[str, Any]) -> List[Dict]:
    results = []
    pack = artifacts.get("clarification_issue_pack.json")
    if not isinstance(pack, dict):
        return results

    issues = pack.get("issues") or []

    # Schema: required fields
    REQUIRED_ISSUE_FIELDS = [
        "issue_id", "field_family", "field_type", "issue_type",
        "deterministic_status", "source_pages", "needs_llm",
    ]
    schema_violations = []
    for iss in issues:
        missing = [f for f in REQUIRED_ISSUE_FIELDS if f not in iss]
        if missing:
            schema_violations.append(f"{iss.get('issue_id','?')}: missing {missing}")
    if schema_violations:
        results.append(_fail(
            "ISSUE_001", "issue_pack_schema_valid",
            f"Issue schema violations: {schema_violations[:10]}",
            affected_artifacts=["clarification_issue_pack.json"],
            affected_issue_ids=[v.split(":")[0] for v in schema_violations[:10]],
        ))
    else:
        results.append(_pass("ISSUE_001", "issue_pack_schema_valid",
                             f"All {len(issues)} issues pass schema check"))

    # Unique issue_ids
    ids = [iss.get("issue_id") for iss in issues if iss.get("issue_id")]
    if len(ids) != len(set(ids)):
        from collections import Counter
        dupes = [iid for iid, cnt in Counter(ids).items() if cnt > 1]
        results.append(_fail(
            "ISSUE_002", "issue_ids_unique",
            f"Duplicate issue_ids: {dupes}",
            affected_artifacts=["clarification_issue_pack.json"],
            affected_issue_ids=dupes,
        ))
    else:
        results.append(_pass("ISSUE_002", "issue_ids_unique",
                             "All issue_ids are unique"))

    # Allowed issue types
    bad_types = [
        (iss.get("issue_id", "?"), iss.get("issue_type"))
        for iss in issues
        if iss.get("issue_type") not in ALLOWED_ISSUE_TYPES
    ]
    if bad_types:
        results.append(_fail(
            "ISSUE_003", "issue_types_allowed",
            f"Issues with disallowed issue_type: {bad_types[:10]}",
            affected_artifacts=["clarification_issue_pack.json"],
            affected_issue_ids=[iid for iid, _ in bad_types[:10]],
        ))
    else:
        results.append(_pass("ISSUE_003", "issue_types_allowed",
                             "All issue types are from allowed set"))

    # needs_llm coherence: all ALLOWED_ISSUE_TYPES should have needs_llm=True
    bad_needs_llm = [
        iss.get("issue_id", "?")
        for iss in issues
        if iss.get("needs_llm") is not True
        and iss.get("issue_type") in ALLOWED_ISSUE_TYPES
    ]
    if bad_needs_llm:
        results.append(_warn(
            "ISSUE_004", "issue_needs_llm_coherent",
            f"Issues with known type but needs_llm!=True: {bad_needs_llm[:10]}",
            affected_artifacts=["clarification_issue_pack.json"],
            affected_issue_ids=bad_needs_llm[:10],
        ))
    else:
        results.append(_pass("ISSUE_004", "issue_needs_llm_coherent",
                             "needs_llm flags are coherent"))

    return results


def _check_resolution_pack(artifacts: Dict[str, Any]) -> List[Dict]:
    results = []
    res_pack = artifacts.get("llm_resolution_pack.json")

    # No resolution pack present
    if res_pack is None:
        # Check: if there are no issues, no resolution pack is perfectly fine
        pack = artifacts.get("clarification_issue_pack.json")
        issue_count = 0
        if isinstance(pack, dict):
            issue_count = pack.get("issue_count", len(pack.get("issues") or []))

        ledger = artifacts.get("evidence_ledger.json")
        is_unreadable = isinstance(ledger, dict) and ledger.get("status") == "BLOCKED_UNREADABLE"

        # RESOLUTION_005: No resolution pack for unreadable no-issue case
        if is_unreadable and issue_count == 0:
            results.append(_pass("RESOLUTION_005",
                                 "no_resolution_pack_for_unreadable_no_issues",
                                 "BLOCKED_UNREADABLE case with 0 issues correctly has no llm_resolution_pack"))
        else:
            results.append(_pass("RESOLUTION_005",
                                 "no_resolution_pack_for_unreadable_no_issues",
                                 "llm_resolution_pack absent; RESOLUTION_005 n/a"))

        for check_id, check_name in [
            ("RESOLUTION_001", "llm_resolution_schema_valid"),
            ("RESOLUTION_002", "llm_resolution_issue_links_valid"),
            ("RESOLUTION_003", "llm_resolution_outcome_requirements"),
            ("RESOLUTION_004", "llm_resolution_provider_model_present"),
        ]:
            results.append(_pass(check_id, check_name,
                                 "No llm_resolution_pack present; check skipped"))
        return results

    if not isinstance(res_pack, dict):
        for check_id, check_name in [
            ("RESOLUTION_001", "llm_resolution_schema_valid"),
            ("RESOLUTION_002", "llm_resolution_issue_links_valid"),
            ("RESOLUTION_003", "llm_resolution_outcome_requirements"),
            ("RESOLUTION_004", "llm_resolution_provider_model_present"),
            ("RESOLUTION_005", "no_resolution_pack_for_unreadable_no_issues"),
        ]:
            results.append(_fail(check_id, check_name,
                                 "llm_resolution_pack.json exists but is not a dict",
                                 affected_artifacts=["llm_resolution_pack.json"]))
        return results

    resolutions = res_pack.get("resolutions") or []
    issue_pack = artifacts.get("clarification_issue_pack.json")
    known_issue_ids: Set[str] = set()
    if isinstance(issue_pack, dict):
        known_issue_ids = {iss["issue_id"] for iss in (issue_pack.get("issues") or []) if "issue_id" in iss}

    # RESOLUTION_001: schema valid
    REQUIRED_RES_FIELDS = ["issue_id", "llm_outcome"]
    schema_violations = []
    for res in resolutions:
        missing = [f for f in REQUIRED_RES_FIELDS if f not in res]
        if missing:
            schema_violations.append(f"{res.get('issue_id','?')}: missing {missing}")
    if schema_violations:
        results.append(_fail(
            "RESOLUTION_001", "llm_resolution_schema_valid",
            f"Resolution schema violations: {schema_violations[:10]}",
            affected_artifacts=["llm_resolution_pack.json"],
            affected_issue_ids=[v.split(":")[0] for v in schema_violations[:10]],
        ))
    else:
        results.append(_pass("RESOLUTION_001", "llm_resolution_schema_valid",
                             f"All {len(resolutions)} resolutions pass schema check"))

    # RESOLUTION_002: every resolution references a real issue_id
    bad_links = [
        res.get("issue_id", "?")
        for res in resolutions
        if res.get("issue_id") not in known_issue_ids
    ]
    if bad_links and known_issue_ids:
        results.append(_fail(
            "RESOLUTION_002", "llm_resolution_issue_links_valid",
            f"Resolutions reference unknown issue_ids: {bad_links[:10]}",
            affected_artifacts=["llm_resolution_pack.json", "clarification_issue_pack.json"],
            affected_issue_ids=bad_links[:10],
        ))
    else:
        results.append(_pass("RESOLUTION_002", "llm_resolution_issue_links_valid",
                             "All resolution issue_ids link to known issues"))

    # RESOLUTION_003: outcome requirements
    outcome_violations = []
    for res in resolutions:
        issue_id = res.get("issue_id", "?")
        outcome = res.get("llm_outcome")
        resolved_value = res.get("resolved_value")
        supporting_evidence = res.get("supporting_evidence") or []
        why_not_resolved = res.get("why_not_resolved")

        if outcome not in ALLOWED_LLM_OUTCOMES:
            outcome_violations.append(f"{issue_id}: disallowed outcome {outcome!r}")
            continue

        if outcome == "resolved":
            if resolved_value is None:
                outcome_violations.append(f"{issue_id}: resolved but resolved_value is null")
            if not supporting_evidence:
                outcome_violations.append(f"{issue_id}: resolved but supporting_evidence is empty")
        elif outcome == "unresolved_explained":
            if not why_not_resolved:
                outcome_violations.append(f"{issue_id}: unresolved_explained but why_not_resolved is null/empty")
        elif outcome == "upgraded_context":
            if resolved_value is not None:
                outcome_violations.append(f"{issue_id}: upgraded_context must not have resolved_value set")

    if outcome_violations:
        bad_iids = [v.split(":")[0] for v in outcome_violations]
        results.append(_fail(
            "RESOLUTION_003", "llm_resolution_outcome_requirements",
            f"LLM outcome requirement violations: {outcome_violations[:10]}",
            affected_artifacts=["llm_resolution_pack.json"],
            affected_issue_ids=bad_iids[:10],
        ))
    else:
        results.append(_pass("RESOLUTION_003", "llm_resolution_outcome_requirements",
                             "All resolution outcome requirements satisfied"))

    # RESOLUTION_004: provider/model present
    top_provider = res_pack.get("provider")
    top_model = res_pack.get("model")
    missing_pm = []
    if not top_provider:
        missing_pm.append("provider")
    if not top_model:
        missing_pm.append("model")
    if missing_pm:
        results.append(_warn(
            "RESOLUTION_004", "llm_resolution_provider_model_present",
            f"llm_resolution_pack missing top-level fields: {missing_pm}",
            affected_artifacts=["llm_resolution_pack.json"],
        ))
    else:
        results.append(_pass("RESOLUTION_004", "llm_resolution_provider_model_present",
                             f"provider={top_provider!r} model={top_model!r}"))

    # RESOLUTION_005: no resolution pack for unreadable no-issue case
    ledger = artifacts.get("evidence_ledger.json")
    is_unreadable = isinstance(ledger, dict) and ledger.get("status") == "BLOCKED_UNREADABLE"
    pack = artifacts.get("clarification_issue_pack.json")
    issue_count = 0
    if isinstance(pack, dict):
        issue_count = pack.get("issue_count", len(pack.get("issues") or []))
    if is_unreadable and issue_count == 0 and resolutions:
        results.append(_warn(
            "RESOLUTION_005", "no_resolution_pack_for_unreadable_no_issues",
            "BLOCKED_UNREADABLE case with 0 issues has resolutions in llm_resolution_pack; review required",
            affected_artifacts=["llm_resolution_pack.json"],
        ))
    else:
        results.append(_pass("RESOLUTION_005", "no_resolution_pack_for_unreadable_no_issues",
                             "No unreadable/no-issues + resolution mismatch"))

    return results


def _check_unreadable_coherence(artifacts: Dict[str, Any]) -> List[Dict]:
    """Validate unreadable case is handled coherently (for FREEZE_001 gating purposes)."""
    results = []
    ledger = artifacts.get("evidence_ledger.json")
    if not isinstance(ledger, dict):
        return results
    status = ledger.get("status", "")
    if status != "BLOCKED_UNREADABLE":
        return results

    extract_metrics = artifacts.get("extract_metrics.json")
    if isinstance(extract_metrics, dict):
        quality_tier = extract_metrics.get("global_quality_tier")
        if quality_tier != "UNREADABLE":
            results.append(_warn(
                "ARTIFACT_003", "unreadable_case_coherent",
                f"evidence_ledger says BLOCKED_UNREADABLE but extract_metrics quality_tier={quality_tier!r}",
                affected_artifacts=["evidence_ledger.json", "extract_metrics.json"],
            ))
    return results


# ---------------------------------------------------------------------------
# Main validator entry point
# ---------------------------------------------------------------------------

def validate_case(case_key: str) -> Dict:
    ctx = build_context(case_key)
    artifact_dir = ctx.artifact_dir

    # Load all artifacts
    artifacts: Dict[str, Any] = {}
    all_names = REQUIRED_ARTIFACTS + OPTIONAL_ARTIFACTS
    for name in all_names:
        artifacts[name] = _load(artifact_dir / name)

    # Build raw page number set
    raw_pages_data = artifacts.get("raw_pages.json")
    raw_page_numbers: Set[int] = set()
    if isinstance(raw_pages_data, list):
        for p in raw_pages_data:
            if isinstance(p, dict) and "page_number" in p:
                raw_page_numbers.add(p["page_number"])

    # Build scope id sets from scope maps
    lot_scope_map = artifacts.get("lot_scope_map.json")
    lot_ids_in_map: Set[str] = set()
    if isinstance(lot_scope_map, dict):
        for lot in (lot_scope_map.get("lot_scopes") or []):
            lid = lot.get("lot_id")
            if lid:
                lot_ids_in_map.add(lid)

    bene_scope_map = artifacts.get("bene_scope_map.json")
    bene_ids_in_map: Set[str] = set()
    # composite_bene_refs: "lot_id/bene_id" strings as used by table_zone_map local_bene_id
    composite_bene_refs: Set[str] = set()
    if isinstance(bene_scope_map, dict):
        for bene in (bene_scope_map.get("bene_scopes") or []):
            bid = bene.get("bene_id")
            blot = bene.get("lot_id")
            if bid:
                bene_ids_in_map.add(bid)
            if bid and blot:
                composite_bene_refs.add(f"{blot}/{bid}")

    # Run all check domains
    all_checks: List[Dict] = []
    all_checks.extend(_check_artifact_presence(artifact_dir, artifacts))
    all_checks.extend(_check_scope_coherence(artifacts, lot_ids_in_map, bene_ids_in_map, composite_bene_refs))
    all_checks.extend(_check_evidence_coherence(artifacts, raw_page_numbers))
    all_checks.extend(_check_packet_sanity(artifacts))
    all_checks.extend(_check_issue_pack(artifacts))
    all_checks.extend(_check_resolution_pack(artifacts))
    all_checks.extend(_check_unreadable_coherence(artifacts))

    # Deduplicate check_ids (unreadable_case_coherent may duplicate ARTIFACT_003)
    seen_ids: Set[str] = set()
    deduped_checks: List[Dict] = []
    for chk in all_checks:
        cid = chk["check_id"]
        if cid not in seen_ids:
            deduped_checks.append(chk)
            seen_ids.add(cid)
        else:
            # If a later check for the same ID is FAIL/WARN, upgrade
            existing = next(c for c in deduped_checks if c["check_id"] == cid)
            rank = {"PASS": 0, "WARN": 1, "FAIL": 2}
            if rank.get(chk["status"], 0) > rank.get(existing["status"], 0):
                deduped_checks.remove(existing)
                deduped_checks.append(chk)
    all_checks = deduped_checks

    # Collect warnings and errors
    warnings = [chk["details"] for chk in all_checks if chk["status"] == "WARN"]
    errors = [chk["details"] for chk in all_checks if chk["status"] == "FAIL"]

    # Determine freeze_ready and overall status
    has_errors = bool(errors)
    has_warnings = bool(warnings)
    freeze_ready = not has_errors

    if has_errors:
        status = "FAIL"
    elif has_warnings:
        status = "PASS_WITH_WARNINGS"
    else:
        status = "PASS"

    # Special case: BLOCKED_UNREADABLE with zero issues is freeze-gated at the doc_map layer,
    # but the canonical artifact state is intentionally blocked. Not a FAIL here.
    ledger = artifacts.get("evidence_ledger.json")
    is_unreadable = isinstance(ledger, dict) and ledger.get("status") == "BLOCKED_UNREADABLE"
    if is_unreadable and not has_errors:
        freeze_ready = True  # intentionally blocked; pre-canon state is coherent

    # Build summary
    error_count = len(errors)
    warn_count = len(warnings)
    pass_count = sum(1 for chk in all_checks if chk["status"] == "PASS")
    if is_unreadable:
        summary = (
            f"BLOCKED_UNREADABLE case: {pass_count} PASS, {warn_count} WARN, {error_count} ERROR. "
            f"Canonical state is intentionally blocked. freeze_ready={freeze_ready}."
        )
    else:
        summary = (
            f"{pass_count} PASS, {warn_count} WARN, {error_count} ERROR. "
            f"freeze_ready={freeze_ready}."
        )

    # Source artifacts listing
    source_artifacts = {name: str(artifact_dir / name) for name in all_names}

    report = {
        "case_key": case_key,
        "status": status,
        "freeze_ready": freeze_ready,
        "checks": all_checks,
        "warnings": warnings,
        "errors": errors,
        "summary": summary,
        "source_artifacts": source_artifacts,
    }

    # Write artifact
    out_path = artifact_dir / "pre_canon_validation_report.json"
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


if __name__ == "__main__":
    import argparse
    from .corpus_registry import list_case_keys

    parser = argparse.ArgumentParser(description="Pre-canon deterministic validator")
    parser.add_argument("--case", required=True, choices=list_case_keys())
    args = parser.parse_args()
    result = validate_case(args.case)
    print(f"{args.case}: status={result['status']} freeze_ready={result['freeze_ready']}")
    print(f"  summary: {result['summary']}")
