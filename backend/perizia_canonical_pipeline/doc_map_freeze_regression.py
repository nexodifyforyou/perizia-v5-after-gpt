"""
doc_map freeze regression runner.

Runs freeze_case for every corpus case, writes doc_map.json to each case
artifact directory, and writes a regression summary to:
  /srv/perizia/_qa/canonical_pipeline/doc_map_freeze_regression_summary.json
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from .corpus_registry import load_cases
from .doc_map_freeze import (
    _load_json,
    _scope_guard_failure,
    freeze_case,
    run_freeze,
)
from .runner import build_context


_SUMMARY_PATH = Path(
    "/srv/perizia/_qa/canonical_pipeline/doc_map_freeze_regression_summary.json"
)

# Known state tallies for regression assertion
_KNOWN_STATES = {
    "deterministic_active",
    "context_only",
    "llm_resolved",
    "unresolved_explained",
    "blocked",
    "checked_no_reliable_basis",
}


def _count_states(fields: Dict[str, Any]) -> Dict[str, int]:
    counts: Dict[str, int] = {s: 0 for s in _KNOWN_STATES}
    for scope_data in fields.values():
        for family_data in scope_data.values():
            for entry in family_data.values():
                state = entry.get("state", "unknown")
                if state in counts:
                    counts[state] += 1
                else:
                    counts.setdefault(state, 0)
                    counts[state] += 1
    return counts


def _regression_checks(doc_map: Dict[str, Any]) -> List[str]:
    """Return a list of regression error strings (empty = pass)."""
    errors: List[str] = []
    status = doc_map.get("status")
    freeze_status = doc_map.get("freeze_status", "")
    freeze_ready = doc_map.get("freeze_ready", False)
    is_unreadable = doc_map.get("case_summary", {}).get("is_unreadable", False)
    fields = doc_map.get("fields", {})
    case_key = doc_map.get("case_key")
    raw_pages_idx: Dict[int, str] = {}
    if case_key:
        try:
            ctx = build_context(case_key)
            raw_pages_data = _load_json(ctx.artifact_dir / "raw_pages.json") or []
            raw_pages_idx = {
                p["page_number"]: p.get("text", "")
                for p in raw_pages_data
                if isinstance(p, dict) and "page_number" in p
            }
        except Exception as exc:
            errors.append(f"could not load raw_pages for scope-guard regression: {exc}")

    # freeze_ready must always be present and boolean
    if not isinstance(freeze_ready, bool):
        errors.append("freeze_ready is not a boolean")

    # Blocked artifact must explicitly say so
    if freeze_status in ("blocked_no_validator", "blocked_validator_fail"):
        if freeze_ready:
            errors.append("blocked freeze artifact must have freeze_ready=False")

    # Unreadable case must have empty fields and non-empty blocked_items
    if is_unreadable:
        if fields:
            errors.append("BLOCKED_UNREADABLE case must have empty fields")
        if not doc_map.get("blocked_items"):
            errors.append("BLOCKED_UNREADABLE case must have blocked_items")
        if status != "BLOCKED_UNREADABLE":
            errors.append(
                f"BLOCKED_UNREADABLE case must have status=BLOCKED_UNREADABLE, got {status}"
            )

    # Normal case: must not have freeze_status=blocked_validator_fail
    if not is_unreadable and status == "OK":
        if "blocked" in freeze_status and "unreadable" not in freeze_status:
            # blocked by validator is OK, but should not occur if validator passed
            if freeze_status not in ("frozen_clean", "frozen_with_context_only",
                                      "frozen_with_unresolved", "frozen_with_blocks",
                                      "frozen_with_unresolved_and_blocks"):
                errors.append(f"Unexpected freeze_status={freeze_status} for OK case")

    # ---- Build declared scope set for enforcement checks ----
    scope_index = doc_map.get("scope_index") or {}
    composite_scope_keys = set(scope_index.get("composite_scope_keys", []))
    declared_scopes = composite_scope_keys | {"document"}

    # scope_index must be present
    if not scope_index:
        errors.append("scope_index is missing or empty")

    # CRITICAL: every scope key in fields must be declared
    for scope_key in fields:
        if scope_key not in declared_scopes:
            errors.append(
                f"Undeclared scope key {scope_key!r} found in fields; "
                f"not in scope_index.composite_scope_keys and not 'document'"
            )

    # No field entry should have an unknown state
    has_checked = False
    for scope_key, scope_data in fields.items():
        for family, family_data in scope_data.items():
            for ft, entry in family_data.items():
                state = entry.get("state")
                if state not in _KNOWN_STATES:
                    errors.append(
                        f"Unknown state={state!r} for scope={scope_key} family={family} field={ft}"
                    )
                # not_found must never appear (it was replaced by checked_no_reliable_basis)
                if state == "not_found":
                    errors.append(
                        f"Forbidden state 'not_found' found at scope={scope_key} field={ft}; "
                        f"must be 'checked_no_reliable_basis' instead"
                    )
                if state == "checked_no_reliable_basis":
                    has_checked = True
                    # must have a non-empty explanation
                    if not entry.get("explanation"):
                        errors.append(
                            f"checked_no_reliable_basis entry has no explanation: "
                            f"scope={scope_key} field={ft}"
                        )
                    # must NOT have a value
                    if entry.get("value") is not None:
                        errors.append(
                            f"checked_no_reliable_basis entry has a non-null value: "
                            f"scope={scope_key} field={ft} value={entry.get('value')!r}"
                        )
                    # must NOT use the old fake fill source_stage
                    if entry.get("source_stage") == "freeze_cue_sweep":
                        errors.append(
                            f"checked_no_reliable_basis entry still uses forbidden "
                            f"source_stage='freeze_cue_sweep' (old placeholder path): "
                            f"scope={scope_key} field={ft}"
                        )
                    # must carry review traceability
                    if not entry.get("review_ids"):
                        errors.append(
                            f"checked_no_reliable_basis entry has no review_ids "
                            f"(missing bounded review traceability): "
                            f"scope={scope_key} field={ft}"
                        )
                    # when cues were found the slot must be flagged for human review
                    if entry.get("cues_found_in_scope_text") and not entry.get("needs_human_review"):
                        errors.append(
                            f"checked_no_reliable_basis entry has cues_found_in_scope_text=True "
                            f"but needs_human_review=False: "
                            f"scope={scope_key} field={ft}"
                        )
                # deterministic_active must have at least one supporting evidence
                if state == "deterministic_active":
                    if not entry.get("supporting_evidence") and not entry.get("packet_ids"):
                        errors.append(
                            f"deterministic_active entry has no evidence: "
                            f"scope={scope_key} field={ft}"
                        )
                # llm_resolved must have issue_ids
                if state == "llm_resolved":
                    if not entry.get("issue_ids"):
                        errors.append(
                            f"llm_resolved entry has no issue_ids: "
                            f"scope={scope_key} field={ft}"
                        )
                    if entry.get("source_stage") == "missing_slot_escalation":
                        resolution_like = {
                            "supporting_evidence": entry.get("supporting_evidence", []),
                            "source_pages": [
                                ev.get("page")
                                for ev in entry.get("supporting_evidence", [])
                                if isinstance(ev, dict) and isinstance(ev.get("page"), int)
                            ],
                        }
                        failure = _scope_guard_failure(
                            scope_key=scope_key,
                            resolution=resolution_like,
                            raw_pages_idx=raw_pages_idx,
                            scope_index=scope_index,
                        )
                        if failure:
                            errors.append(
                                "missing-slot llm_resolved failed scope guard: "
                                f"scope={scope_key} family={family} field={ft}: {failure}"
                            )
                # context_only must NOT have a non-null value masquerading as truth
                if state == "context_only" and entry.get("value") is not None:
                    errors.append(
                        f"context_only entry has a non-null value (masquerading as truth): "
                        f"scope={scope_key} field={ft} value={entry.get('value')!r}"
                    )

    # For normal cases with declared scopes, the missing-slot review pack must exist
    # in source_artifacts (proves the review path ran, regardless of escalation outcome).
    # checked_no_reliable_basis may be legitimately absent if all missing slots were
    # promoted to llm_resolved / context_only / unresolved_explained via escalation.
    if not is_unreadable and composite_scope_keys:
        msr_artifact = doc_map.get("source_artifacts", {}).get("missing_slot_review_pack.json")
        if msr_artifact is None:
            errors.append(
                "missing_slot_review_pack.json absent from source_artifacts for a normal case "
                "with declared scopes; missing-slot review path appears broken"
            )

    # unresolved_items must all be represented in fields
    for item in doc_map.get("unresolved_items", []):
        sk = item.get("scope_key")
        ft = item.get("field_type")
        ff = item.get("field_family")
        if sk and ft and ff:
            entry = fields.get(sk, {}).get(ff, {}).get(ft)
            if entry is None:
                errors.append(
                    f"unresolved_item references missing fields entry: "
                    f"scope={sk} family={ff} field={ft}"
                )
            elif entry.get("state") not in ("unresolved_explained",):
                errors.append(
                    f"unresolved_item references entry with wrong state={entry.get('state')}: "
                    f"scope={sk} field={ft}"
                )

    if case_key == "mantova_1859886":
        bad = (
            fields.get("bene:unico/4", {})
            .get("cadastral", {})
            .get("cadastral_categoria")
        )
        if bad and bad.get("state") == "llm_resolved" and bad.get("value") == "A/10":
            errors.append(
                "Mantova blocker regressed: bene:unico/4 cadastral_categoria "
                "froze as A/10 from missing-slot escalation"
            )
        safe = (
            fields.get("bene:unico/4", {})
            .get("valuation", {})
            .get("valore_stima_raw")
        )
        if safe and safe.get("source_stage") == "missing_slot_escalation":
            if safe.get("state") != "llm_resolved" or safe.get("value") != "€ 231.140,00":
                errors.append(
                    "Mantova safe valuation regressed: bene:unico/4 valore_stima_raw "
                    "missing-slot escalation no longer freezes the Bene-4-safe value"
                )

    return errors


def main() -> None:
    rows = []

    for case in load_cases():
        case_key = case.case_key
        try:
            out_path = run_freeze(case_key)
            doc_map = json.loads(out_path.read_text(encoding="utf-8"))
        except Exception as exc:
            rows.append({
                "case_key": case_key,
                "freeze_status": "ERROR",
                "freeze_ready": False,
                "is_unreadable": False,
                "status": "ERROR",
                "state_counts": {},
                "unresolved_count": 0,
                "context_count": 0,
                "blocked_count": 0,
                "regression_errors": [f"freeze raised exception: {exc}"],
                "warnings": [],
                "artifact": None,
            })
            continue

        errors = _regression_checks(doc_map)
        state_counts = _count_states(doc_map.get("fields", {}))
        cs = doc_map.get("case_summary", {})

        rows.append({
            "case_key": case_key,
            "freeze_status": doc_map.get("freeze_status"),
            "freeze_ready": doc_map.get("freeze_ready"),
            "is_unreadable": cs.get("is_unreadable", False),
            "status": doc_map.get("status"),
            "winner": cs.get("winner"),
            "lot_count": cs.get("lot_count", 0),
            "bene_count": cs.get("bene_count", 0),
            "has_llm_resolutions": cs.get("has_llm_resolutions", False),
            "state_counts": state_counts,
            "unresolved_count": len(doc_map.get("unresolved_items", [])),
            "context_count": len(doc_map.get("context_items", [])),
            "blocked_count": len(doc_map.get("blocked_items", [])),
            "regression_errors": errors,
            "warnings": doc_map.get("warnings", []),
            "artifact": str(out_path),
        })

    _SUMMARY_PATH.write_text(
        json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(_SUMMARY_PATH)
    print(f"COUNT={len(rows)}")

    all_pass = True
    for r in rows:
        errs = r["regression_errors"]
        warn_str = f" WARN={len(r['warnings'])}" if r["warnings"] else ""
        status_str = "PASS" if not errs else "FAIL"
        if errs:
            all_pass = False
        unreadable_str = " [UNREADABLE]" if r.get("is_unreadable") else ""
        sc = r.get("state_counts", {})
        print(
            f"  {r['case_key']}: {status_str} freeze_status={r['freeze_status']}"
            f"{unreadable_str}{warn_str} "
            f"active={sc.get('deterministic_active', 0)} "
            f"llm_res={sc.get('llm_resolved', 0)} "
            f"unresolved={sc.get('unresolved_explained', 0)} "
            f"context={sc.get('context_only', 0)} "
            f"blocked={sc.get('blocked', 0)} "
            f"checked={sc.get('checked_no_reliable_basis', 0)}"
        )
        for e in errs:
            print(f"    ERROR: {e}")
        for w in r.get("warnings", []):
            print(f"    WARN:  {w}")

    if all_pass:
        print("\nALL CASES PASS doc_map freeze regression.")
    else:
        print("\nSOME CASES FAILED doc_map freeze regression.")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
