"""Pure projection of applicable full-document facts into one lot worksheet."""

from __future__ import annotations

import copy
import re
from typing import Any, Dict, List, Tuple

from . import fact_lineage, lots, validator

LOT_FACT_PROJECTION_SCHEMA_VERSION = "cv2.lot_fact_projection.v1"

NOT_APPLICABLE_TO_LOT = "NOT_APPLICABLE_TO_LOT"
DUPLICATE_EQUIVALENT = "DUPLICATE_EQUIVALENT"
CONFLICT_REQUIRES_REVIEW = "CONFLICT_REQUIRES_REVIEW"
SOURCE_EVIDENCE_INSUFFICIENT = "SOURCE_EVIDENCE_INSUFFICIENT"
CUSTOMER_SAFETY_SUPPRESSION = "CUSTOMER_SAFETY_SUPPRESSION"
SCHEMA_UNREPRESENTABLE = "SCHEMA_UNREPRESENTABLE"
LOW_CONFIDENCE_NONCRITICAL = "LOW_CONFIDENCE_NONCRITICAL"
INVALIDATED_BY_STRONGER_SOURCE = "INVALIDATED_BY_STRONGER_SOURCE"

REASON_CODES = frozenset({
    NOT_APPLICABLE_TO_LOT, DUPLICATE_EQUIVALENT, CONFLICT_REQUIRES_REVIEW,
    SOURCE_EVIDENCE_INSUFFICIENT, CUSTOMER_SAFETY_SUPPRESSION,
    SCHEMA_UNREPRESENTABLE, LOW_CONFIDENCE_NONCRITICAL,
    INVALIDATED_BY_STRONGER_SOURCE,
})

_GENERIC = {"", "unknown", "uncertain", "da verificare", "non specificato", "non disponibile", "incerto"}
_CUSTOMER_EXCERPT_CONTEXT_LINES = 2


def _norm(value: Any) -> str:
    return " ".join(validator._norm(value).split())


def _is_generic(value: Any) -> bool:
    text = _norm(value)
    return not text or text in _GENERIC or any(token in text for token in ("da verificare", "non specificat", "nessun dettaglio", "non disponibile", "incert"))


def _delotify(value: Any, lot_id: str) -> Any:
    """Remove multi-lot enumerations before merging into a single-lot shape."""
    if not isinstance(value, str):
        return value
    # Covers comma/e/slash/hyphen enumerations while preserving surrounding text.
    pattern = re.compile(r"\blott[oi]\s+(?:n[.°ºo]*\s*)?\d{1,3}(?:\s*(?:[,;/\-]|\be\b)\s*\d{1,3})+", re.IGNORECASE)
    value = pattern.sub("dichiarazione applicabile al lotto", value)
    # A remaining explicit other-lot tag is provenance, not content for this lot.
    value = re.sub(
        rf"\blott[oi]\s+(?:n[.°ºo]*\s*)?(?!{re.escape(str(lot_id))}\b)\d{{1,3}}\b",
        "lotto interessato", value, flags=re.IGNORECASE,
    )
    return " ".join(value.split())


def _delotify_item(item: Any, lot_id: str) -> Any:
    if isinstance(item, dict):
        return {key: _delotify_item(value, lot_id) for key, value in item.items()}
    if isinstance(item, list):
        return [_delotify_item(value, lot_id) for value in item]
    return _delotify(item, lot_id)


def customer_safe_projection_pages(
    full_pages: List[Dict[str, Any]], selected_pages: List[Dict[str, Any]],
    projection_report: Dict[str, Any], lot_id: str,
) -> List[Dict[str, Any]]:
    """Add projected evidence pages with other-lot lines removed.

    Validation still receives the original page text.  This view is only for
    customer evidence excerpts, preventing a valid shared citation from echoing
    sibling-lot content into the selected lot's report.
    """
    selected_numbers = {
        int(page.get("page_number", index)) for index, page in enumerate(selected_pages, 1)
    }
    added = set(projection_report.get("verification_pages_added") or []) - selected_numbers
    out = [copy.deepcopy(page) for page in selected_pages]
    for index, page in enumerate(full_pages or [], 1):
        number = int(page.get("page_number", index))
        if number not in added:
            continue
        lines = str(page.get("text") or "").splitlines()
        tagged = {
            line_index: {
                str(value) for value in lots.lot_ids_in_text(
                    fact_lineage._expand_lot_ranges(line)
                )
            }
            for line_index, line in enumerate(lines)
        }
        owner = set()
        owner_by_line = {}
        for line_index in range(len(lines)):
            if tagged[line_index]:
                owner = tagged[line_index]
            owner_by_line[line_index] = owner
        keep = set()
        for line_index, ids in tagged.items():
            if str(lot_id) not in ids:
                continue
            keep.add(line_index)
            start = max(0, line_index - _CUSTOMER_EXCERPT_CONTEXT_LINES)
            end = min(len(lines), line_index + _CUSTOMER_EXCERPT_CONTEXT_LINES + 1)
            # Untagged continuation lines belong to the nearest preceding lot
            # tag. Never cross that paragraph boundary into a sibling lot.
            keep.update(
                index for index in range(start, end)
                if not tagged[index]
                and (not owner_by_line[index] or str(lot_id) in owner_by_line[index])
            )
        safe_lines = [
            _delotify(lines[index], str(lot_id)) if tagged[index] else lines[index]
            for index in sorted(keep)
        ]
        if safe_lines:
            safe_page = copy.deepcopy(page)
            safe_page["text"] = "\n".join(safe_lines)
            out.append(safe_page)
    return sorted(out, key=lambda page: int(page.get("page_number", 0)))


def _exclusive_pages(segmentation: Dict[str, Any], lot_id: str) -> set:
    return {int(p) for p in (segmentation.get("lot_pages") or {}).get(str(lot_id), [])}


def _strong_lot_item(item: Dict[str, Any], segmentation: Dict[str, Any], lot_id: str) -> bool:
    if item.get("projected"):
        return False
    return bool(set(item.get("evidence_pages") or []) & _exclusive_pages(segmentation, lot_id))


def _equivalent(left: Any, right: Any) -> bool:
    if isinstance(left, dict) and isinstance(right, dict):
        material = set(left) | set(right)
        material -= {"evidence_pages", "projected", "fact_id", "projection_reason", "notes", "summary"}
        return all(_equivalent(left.get(key), right.get(key)) for key in material)
    if isinstance(left, list) and isinstance(right, list):
        return {_norm(x) for x in left} == {_norm(x) for x in right}
    if isinstance(left, bool) or isinstance(right, bool):
        return bool(left) == bool(right)
    return _norm(left) == _norm(right)


def _projected_item(fact: Dict[str, Any], lot_id: str, reason: str) -> Dict[str, Any]:
    raw = fact.get("value") if isinstance(fact.get("value"), dict) else {}
    item = _delotify_item(copy.deepcopy(raw), lot_id)
    item["evidence_pages"] = list(fact.get("evidence_pages") or [])
    item.update({"projected": True, "fact_id": fact["fact_id"], "projection_reason": reason})
    return item


def _drop(report: Dict[str, Any], fact: Dict[str, Any], reason: str, detail: str) -> None:
    if reason not in REASON_CODES:
        raise ValueError(f"unknown projection reason: {reason}")
    report["dropped_facts"].append({"fact_id": fact["fact_id"], "reason_code": reason, "detail": detail})


def _record_merge(report: Dict[str, Any], path: str, fact: Dict[str, Any], reason: str) -> None:
    report["projected_fact_ids"].append(fact["fact_id"])
    report["filled_fields"].append({"path": path, "fact_id": fact["fact_id"], "reason": reason})
    report["verification_pages_added"].extend(fact.get("evidence_pages") or [])


def _conflict(report: Dict[str, Any], path: str, lot_value: Any, fact: Dict[str, Any]) -> None:
    report["conflicts"].append({
        "path": path, "lot_value": copy.deepcopy(lot_value),
        "case_value": copy.deepcopy(fact.get("value")), "fact_id": fact["fact_id"],
        "reason": CONFLICT_REQUIRES_REVIEW,
    })


def _reconcile_compliance(ws: Dict[str, Any], fact: Dict[str, Any], lot_id: str, segmentation: Dict[str, Any], report: Dict[str, Any]) -> None:
    items = ws.setdefault("technical_compliance", [])
    token = fact.get("field")
    existing = next((item for item in items if validator._area_token(item.get("area")) == token), None)
    incoming = _projected_item(fact, lot_id, "FILLED_FROM_APPLICABLE_CASE_FACT")
    if existing is None:
        items.append(incoming)
        _record_merge(report, f"technical_compliance[{len(items)-1}]", fact, "FILLED_MISSING_FIELD")
        return
    if _strong_lot_item(existing, segmentation, lot_id):
        _drop(report, fact, DUPLICATE_EQUIVALENT if _equivalent(existing, incoming) else INVALIDATED_BY_STRONGER_SOURCE, "lot-specific evidence retained")
        if not _equivalent(existing, incoming):
            _conflict(report, "technical_compliance", existing, fact)
        return
    old = existing.get("classification")
    new = incoming.get("classification")
    if old not in (None, "", "uncertain") and new == "uncertain":
        _drop(report, fact, INVALIDATED_BY_STRONGER_SOURCE, "uncertain case summary cannot downgrade a material finding")
        return
    if _equivalent(existing, incoming):
        _drop(report, fact, DUPLICATE_EQUIVALENT, "equivalent lot fact already present")
    elif _is_generic(old) or (fact.get("declaration_status") == "explicit_declaration" and fact.get("evidence_quality") == "specific_statement"):
        if old and new and old != new and not _is_generic(old):
            _conflict(report, "technical_compliance", existing, fact)
        existing.clear(); existing.update(incoming)
        _record_merge(report, "technical_compliance", fact, "EXPLICIT_DECLARATION_OVERRIDES_GENERIC")
    else:
        _conflict(report, "technical_compliance", existing, fact)
        _drop(report, fact, CONFLICT_REQUIRES_REVIEW, "material compliance conflict retained for review")


def _reconcile_occupancy(ws: Dict[str, Any], fact: Dict[str, Any], lot_id: str, segmentation: Dict[str, Any], report: Dict[str, Any]) -> None:
    oc = ws.setdefault("occupancy", {})
    if fact.get("field") == "risk":
        risk = _delotify(str(fact.get("value") or ""), lot_id)
        if risk and _norm(risk) not in {_norm(x) for x in oc.setdefault("risks", [])}:
            oc["risks"].append(risk)
            oc["evidence_pages"] = sorted(set(oc.get("evidence_pages") or []) | set(fact.get("evidence_pages") or []))
            oc.update({"projected": True, "fact_id": fact["fact_id"], "projection_reason": "FILLED_MISSING_FIELD"})
            _record_merge(report, "occupancy.risks", fact, "FILLED_MISSING_FIELD")
        else:
            _drop(report, fact, DUPLICATE_EQUIVALENT, "occupancy risk already present")
        return
    incoming = _delotify_item(copy.deepcopy(fact.get("value") or {}), lot_id)
    if _strong_lot_item(oc, segmentation, lot_id):
        material_disagreement = incoming.get("status") and oc.get("status") and not _equivalent(incoming.get("status"), oc.get("status"))
        if material_disagreement:
            _conflict(report, "occupancy.status", oc.get("status"), fact)
            _drop(report, fact, INVALIDATED_BY_STRONGER_SOURCE, "exclusive lot occupancy evidence retained")
        else:
            changed = False
            for key in ("title_info", "opponibility", "registration_dates", "expiry_dates"):
                if not oc.get(key) and incoming.get(key):
                    oc[key] = incoming[key]; changed = True
            if changed:
                oc["evidence_pages"] = sorted(set(oc.get("evidence_pages") or []) | set(fact.get("evidence_pages") or []))
                oc.update({"projected": True, "fact_id": fact["fact_id"], "projection_reason": "FILLED_MISSING_FIELD"})
                _record_merge(report, "occupancy", fact, "FILLED_MISSING_FIELD")
            else:
                _drop(report, fact, DUPLICATE_EQUIVALENT, "occupancy already complete")
        return
    changed = False
    old_status, new_status = oc.get("status"), incoming.get("status")
    if new_status and (not old_status or _is_generic(old_status) or (_norm(old_status) in {"libero", "free"} and incoming.get("title_info"))):
        if old_status and not _equivalent(old_status, new_status) and _norm(old_status) not in _norm(new_status):
            _conflict(report, "occupancy.status", old_status, fact)
        oc["status"] = new_status; changed = True
    elif old_status and new_status and not _equivalent(old_status, new_status) and _norm(old_status) not in _norm(new_status) and _norm(new_status) not in _norm(old_status):
        _conflict(report, "occupancy.status", old_status, fact)
    for key in ("title_info", "opponibility", "registration_dates", "expiry_dates"):
        if (not oc.get(key) or _is_generic(oc.get(key))) and incoming.get(key):
            oc[key] = incoming[key]; changed = True
    if changed:
        oc["evidence_pages"] = sorted(set(oc.get("evidence_pages") or []) | set(fact.get("evidence_pages") or []))
        oc.update({"projected": True, "fact_id": fact["fact_id"], "projection_reason": "FILLED_MISSING_FIELD"})
        _record_merge(report, "occupancy", fact, "FILLED_MISSING_FIELD")
    else:
        _drop(report, fact, CONFLICT_REQUIRES_REVIEW if report["conflicts"] and report["conflicts"][-1].get("fact_id") == fact["fact_id"] else DUPLICATE_EQUIVALENT, "occupancy not replaced")


def _reconcile_formality(ws: Dict[str, Any], fact: Dict[str, Any], lot_id: str, segmentation: Dict[str, Any], report: Dict[str, Any]) -> None:
    items = ws.setdefault("legal_formalities", [])
    incoming = _projected_item(fact, lot_id, "FILLED_FROM_APPLICABLE_CASE_FACT")
    candidates = [x for x in items if str(x.get("type")) == str(incoming.get("type"))]
    existing = next((x for x in candidates if _norm(x.get("description")) == _norm(incoming.get("description"))), None)
    if existing is None and len(candidates) == 1:
        existing = candidates[0]
    if existing is None:
        items.append(incoming); _record_merge(report, f"legal_formalities[{len(items)-1}]", fact, "FILLED_MISSING_FIELD"); return
    flags = ("cancelled_by_procedure", "buyer_burden")
    conflict = any(existing.get(k) is not None and incoming.get(k) is not None and bool(existing.get(k)) != bool(incoming.get(k)) for k in flags)
    if _strong_lot_item(existing, segmentation, lot_id):
        if conflict: _conflict(report, "legal_formalities", existing, fact)
        _drop(report, fact, INVALIDATED_BY_STRONGER_SOURCE if conflict else DUPLICATE_EQUIVALENT, "lot formality retained"); return
    if conflict:
        _conflict(report, "legal_formalities", existing, fact)
        if fact.get("declaration_status") != "explicit_declaration":
            _drop(report, fact, CONFLICT_REQUIRES_REVIEW, "formality conflict retained"); return
    existing.clear(); existing.update(incoming)
    _record_merge(report, "legal_formalities", fact, "EXPLICIT_DECLARATION_OVERRIDES_GENERIC")


def _reconcile_risk(ws: Dict[str, Any], fact: Dict[str, Any], lot_id: str, segmentation: Dict[str, Any], report: Dict[str, Any]) -> None:
    items = ws.setdefault("risk_classification", [])
    existing = next((x for x in items if validator._area_token(x.get("area")) == fact.get("field")), None)
    incoming = _projected_item(fact, lot_id, "FILLED_FROM_APPLICABLE_CASE_FACT")
    if existing is None:
        items.append(incoming); _record_merge(report, f"risk_classification[{len(items)-1}]", fact, "FILLED_MISSING_FIELD"); return
    if _strong_lot_item(existing, segmentation, lot_id):
        if not _equivalent(existing, incoming): _conflict(report, "risk_classification", existing, fact)
        _drop(report, fact, INVALIDATED_BY_STRONGER_SOURCE if not _equivalent(existing, incoming) else DUPLICATE_EQUIVALENT, "lot risk retained"); return
    if _equivalent(existing, incoming):
        _drop(report, fact, DUPLICATE_EQUIVALENT, "risk already present"); return
    if _is_generic(existing.get("summary")):
        existing.clear(); existing.update(incoming); _record_merge(report, "risk_classification", fact, "FILLED_MISSING_FIELD"); return
    _conflict(report, "risk_classification", existing, fact); _drop(report, fact, CONFLICT_REQUIRES_REVIEW, "risk conflict retained")


def _reconcile_identity(ws: Dict[str, Any], fact: Dict[str, Any], lot_id: str, segmentation: Dict[str, Any], report: Dict[str, Any]) -> None:
    ci = ws.setdefault("case_identity", {})
    field = str(fact.get("field") or "")
    if field not in {"tribunale", "procedura_rge", "lotto", "address", "property_type", "ownership_right"}:
        _drop(report, fact, SCHEMA_UNREPRESENTABLE, "identity field has no worksheet slot"); return
    value = _delotify(fact.get("value"), lot_id)
    if field == "lotto" and ci.get(field):
        _drop(report, fact, DUPLICATE_EQUIVALENT, "single-lot identity is already normalized")
        return
    if not ci.get(field) or _is_generic(ci.get(field)):
        ci[field] = value
        ci["evidence_pages"] = sorted(set(ci.get("evidence_pages") or []) | set(fact.get("evidence_pages") or []))
        ci.update({"projected": True, "fact_id": fact["fact_id"], "projection_reason": "FILLED_MISSING_FIELD"})
        _record_merge(report, f"case_identity.{field}", fact, "FILLED_MISSING_FIELD")
    elif _equivalent(ci.get(field), value):
        _drop(report, fact, DUPLICATE_EQUIVALENT, "identity already present")
    elif _strong_lot_item(ci, segmentation, lot_id):
        _conflict(report, f"case_identity.{field}", ci.get(field), fact); _drop(report, fact, INVALIDATED_BY_STRONGER_SOURCE, "lot identity retained")
    else:
        _conflict(report, f"case_identity.{field}", ci.get(field), fact); _drop(report, fact, CONFLICT_REQUIRES_REVIEW, "identity conflict retained")


def project_and_reconcile(
    *, case_ledger: Dict[str, Any], lot_worksheet: Dict[str, Any], lot_id: str,
    segmentation: Dict[str, Any], all_lot_ids: List[str],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Return a reconciled cv2 worksheet copy and its auditable projection report."""
    lot_id = str(lot_id)
    ws = copy.deepcopy(lot_worksheet)
    report: Dict[str, Any] = {
        "schema_version": LOT_FACT_PROJECTION_SCHEMA_VERSION, "lot_id": lot_id,
        "projected_fact_ids": [], "filled_fields": [], "conflicts": [],
        "dropped_facts": [], "unresolved_case_facts": [], "verification_pages_added": [],
    }
    for fact in case_ledger.get("facts") or []:
        applicability = fact.get("applicability")
        applicable_ids = {str(x) for x in fact.get("applicability_lot_ids") or []}
        applies = applicability in {fact_lineage.CASE_GLOBAL, fact_lineage.ALL_LOTS} or (
            applicability in {fact_lineage.LOT_SPECIFIC, fact_lineage.BENE_SPECIFIC, fact_lineage.MULTIPLE_LOTS}
            and lot_id in applicable_ids
        )
        if applicability == fact_lineage.UNKNOWN_APPLICABILITY:
            report["unresolved_case_facts"].append({"fact_id": fact["fact_id"], "applicability": applicability, "detail": fact.get("applicability_basis")})
            _drop(report, fact, SOURCE_EVIDENCE_INSUFFICIENT, "applicability unresolved; content not merged")
            continue
        if not applies:
            _drop(report, fact, NOT_APPLICABLE_TO_LOT, "fact is scoped to another lot")
            continue
        category = fact.get("category")
        if category == "compliance":
            _reconcile_compliance(ws, fact, lot_id, segmentation, report)
        elif category == "occupancy":
            _reconcile_occupancy(ws, fact, lot_id, segmentation, report)
        elif category == "formality":
            _reconcile_formality(ws, fact, lot_id, segmentation, report)
        elif category == "risk":
            _reconcile_risk(ws, fact, lot_id, segmentation, report)
        elif category == "identity":
            _reconcile_identity(ws, fact, lot_id, segmentation, report)
        elif category == "money" and fact.get("source_stage") == "shared_summary_projection":
            _record_merge(report, "shared_summary_money", fact, "EXISTING_MONEY_CHANNEL")
        elif category == "surface_cadastral" and fact.get("source_stage") == "document_signal":
            _record_merge(report, "surface_cadastral", fact, "EXISTING_SURFACE_CHANNEL")
        elif category == "money":
            # Money deliberately remains on the existing shared-summary/build_lot_money path.
            _drop(report, fact, SCHEMA_UNREPRESENTABLE, "money is reconciled by the existing lot-money channel")
        else:
            _drop(report, fact, SCHEMA_UNREPRESENTABLE, "category has no cv2 worksheet slot")
    report["projected_fact_ids"] = list(dict.fromkeys(report["projected_fact_ids"]))
    report["verification_pages_added"] = sorted({int(p) for p in report["verification_pages_added"]})
    return ws, report
