"""Deterministic case-fact lineage and lot applicability classification.

The ledger is an audit companion to the existing ``cv2.worksheet.v1`` shape.
It never calls a model and never changes the source worksheet.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

from . import doc_signals, lots
from . import validator

FACT_LEDGER_SCHEMA_VERSION = "cv2.fact_ledger.v1"

CASE_GLOBAL = "CASE_GLOBAL"
ALL_LOTS = "ALL_LOTS"
LOT_SPECIFIC = "LOT_SPECIFIC"
MULTIPLE_LOTS = "MULTIPLE_LOTS"
BENE_SPECIFIC = "BENE_SPECIFIC"
UNKNOWN_APPLICABILITY = "UNKNOWN_APPLICABILITY"

_SUBSTANTIVE = {"compliance", "occupancy", "formality", "risk", "money", "surface_cadastral"}


def _expand_lot_ranges(value: Any) -> str:
    """Normalize compact lot ranges/enumerations for the shared lot parser."""
    text = str(value or "")
    pattern = re.compile(
        r"\b(lott[oi])\s+((?:\d{1,3}\s*[-\u2013/]\s*)+\d{1,3})",
        re.IGNORECASE,
    )

    def expand(match: re.Match[str]) -> str:
        numbers = [int(number) for number in re.findall(r"\d{1,3}", match.group(2))]
        if len(numbers) == 2 and 0 < numbers[0] <= numbers[1] and numbers[1] - numbers[0] <= 100:
            numbers = list(range(numbers[0], numbers[1] + 1))
        return f"{match.group(1)} " + ", ".join(str(number) for number in numbers)

    return pattern.sub(expand, text)


def _pages(value: Any) -> List[int]:
    out: List[int] = []
    for page in value or []:
        try:
            number = int(page)
        except (TypeError, ValueError):
            continue
        if number not in out:
            out.append(number)
    return sorted(out)


def _page_methods(segmentation: Dict[str, Any]) -> Dict[int, Tuple[str, Optional[str]]]:
    return {
        int(row["page"]): (str(row.get("method") or ""), row.get("assigned_lot"))
        for row in segmentation.get("page_assignments") or []
        if row.get("page") is not None
    }


def _bene_lot_map(segmentation: Dict[str, Any]) -> Dict[str, str]:
    """Read an optional deterministic bene map without requiring a schema bump."""
    raw = segmentation.get("bene_lot_map") or {}
    return {str(k): str(v) for k, v in raw.items() if v is not None}


def _classify_applicability(
    item_text: Any,
    evidence_pages: Any,
    segmentation: Dict[str, Any],
    known_lot_ids: Iterable[Any],
    bene_ids_for_pages: Optional[Dict[str, Any]] = None,
    *,
    category: str = "compliance",
) -> Tuple[str, List[str], str]:
    """Classify scope using explicit text first, then bene/page evidence.

    A shared page alone can never imply ``ALL_LOTS``.  This property is kept in
    this single classifier so packet indexing and projection cannot drift.
    """
    known = {str(x) for x in known_lot_ids or [] if str(x)}
    enumeration_text = _expand_lot_ranges(item_text)
    explicit = [x for x in lots.lot_ids_in_text(enumeration_text) if not known or x in known]
    explicit = list(dict.fromkeys(explicit))
    explicit_set = set(explicit)
    if len(explicit_set) == 1:
        return LOT_SPECIFIC, sorted(explicit_set), "explicit_lot_reference_in_text"
    if len(explicit_set) >= 2:
        if known and explicit_set == known:
            return ALL_LOTS, sorted(explicit_set), "explicit_enumeration_matches_all_known_lots"
        return MULTIPLE_LOTS, sorted(explicit_set), "explicit_multi_lot_enumeration"

    bene_ids = list(dict.fromkeys(lots.bene_ids_in_text(item_text)))
    mapping = {**_bene_lot_map(segmentation), **{str(k): str(v) for k, v in (bene_ids_for_pages or {}).items()}}
    bene_lots = {mapping[b] for b in bene_ids if b in mapping}
    if bene_ids and len(bene_lots) == 1:
        return BENE_SPECIFIC, sorted(bene_lots), "explicit_bene_reference"

    evidence = _pages(evidence_pages)
    if not evidence:
        return UNKNOWN_APPLICABILITY, [], "no_evidence_pages"
    page_methods = _page_methods(segmentation)
    rows = [page_methods.get(page, ("", None)) for page in evidence]
    if any(method == "shared" for method, _ in rows):
        return UNKNOWN_APPLICABILITY, [], "shared_page_no_explicit_lot_reference"
    assigned = {str(lot_id) for method, lot_id in rows if method in {"explicit", "carry_forward"} and lot_id is not None}
    if len(assigned) == 1 and all(method in {"explicit", "carry_forward", "global"} for method, _ in rows):
        return LOT_SPECIFIC, sorted(assigned), "single_lot_page_evidence"
    if rows and all(method == "global" for method, _ in rows):
        if category == "identity":
            return CASE_GLOBAL, [], "global_page_no_lot_signal"
        if category in _SUBSTANTIVE:
            return ALL_LOTS, sorted(known), "global_page_no_lot_signal"
    return UNKNOWN_APPLICABILITY, [], "mixed_or_unmapped_page_evidence"


def _item_text(*values: Any) -> str:
    parts: List[str] = []
    for value in values:
        if isinstance(value, (list, tuple)):
            parts.extend(str(x) for x in value if x not in (None, ""))
        elif value not in (None, ""):
            parts.append(str(value))
    return " ".join(parts)


def _declaration_metadata(text: Any, classification: Optional[str] = None) -> Tuple[str, str, str]:
    normalized = validator._norm(text)  # shared, tested compliance normalization
    checkbox = bool(re.search(r"(?:\u2610|\u2611|\u25a0|\[\s*[xX]?\s*\]).{0,18}(?:si|no)|(?:si|no).{0,18}(?:\u2610|\u2611|\u25a0|\[)", normalized))
    positive = validator._has_positive_compliance_statement(normalized)
    negative = validator._has_negative(normalized)
    explicit_words = any(token in normalized for token in (
        "risulta", "dichiara", "attesta", "contratto", "registrat", "scadenza",
        "a cura della procedura", "a carico", "conforme", "regolarizzabile",
    ))
    if checkbox:
        declaration, quality = "checkbox_summary", "checkbox_only"
    elif positive or negative or explicit_words or classification not in (None, "", "uncertain"):
        declaration, quality = "explicit_declaration", "specific_statement"
    elif normalized:
        declaration, quality = "narrative_summary", "generic_administrative"
    else:
        declaration, quality = "derived", "generic_administrative"
    return declaration, quality, normalized


def _severity(category: str, value: Any) -> str:
    if category == "money":
        return doc_signals.SEV_CRITICAL
    if category == "occupancy":
        return doc_signals.SEV_CRITICAL
    if category == "compliance" and str(value) in {"non_conforming", "not_regularizable"}:
        return doc_signals.SEV_CRITICAL
    if category in {"compliance", "formality", "risk", "identity", "surface_cadastral"}:
        return doc_signals.SEV_IMPORTANT
    return doc_signals.SEV_USEFUL


def _fact(
    *, category: str, field: Optional[str], label: Optional[str], value: Any,
    source_path: str, evidence_pages: Any, segmentation: Dict[str, Any],
    lot_ids: List[str], ordinal: int, money: Optional[Dict[str, Any]] = None,
    source_stage: str = "case_worksheet",
) -> Dict[str, Any]:
    text = _item_text(label, value if not isinstance(value, dict) else value.values())
    applicability, applicable_lots, basis = _classify_applicability(
        text, evidence_pages, segmentation, lot_ids, category=category
    )
    declaration, quality, _ = _declaration_metadata(text, str(value) if category == "compliance" else None)
    evidence = _pages(evidence_pages)
    if applicability == UNKNOWN_APPLICABILITY or declaration in {"checkbox_summary", "derived"}:
        confidence = "low"
    elif declaration == "explicit_declaration" and quality == "specific_statement" and len(evidence) <= 2:
        confidence = "high"
    else:
        confidence = "medium"
    token = re.sub(r"[^a-z0-9]+", "_", validator._norm(field or label or "fact")).strip("_") or "fact"
    return {
        "fact_id": f"{category}:{token}:{ordinal}",
        "category": category,
        "field": field,
        "label": label,
        "value": value,
        "source_path": source_path,
        "source_stage": source_stage,
        "evidence_pages": evidence,
        "declaration_status": declaration,
        "evidence_quality": quality,
        "applicability": applicability,
        "applicability_lot_ids": applicable_lots,
        "applicability_bene_ids": lots.bene_ids_in_text(text),
        "applicability_basis": basis,
        "confidence": confidence,
        "severity": _severity(category, value),
        "money": money,
        "provenance_chain": [f"{source_stage}.{source_path}"],
    }


def build_case_fact_ledger(
    worksheet: Dict[str, Any], segmentation: Dict[str, Any], lot_report: Dict[str, Any]
) -> Dict[str, Any]:
    """Extract a stable fact ledger from the full-document worksheet."""
    facts: List[Dict[str, Any]] = []
    lot_ids = [str(x) for x in (lot_report.get("lot_ids") or segmentation.get("lot_ids") or [])]

    def add(**kwargs: Any) -> None:
        facts.append(_fact(segmentation=segmentation, lot_ids=lot_ids, ordinal=len(facts), **kwargs))

    ci = worksheet.get("case_identity") or {}
    for field in ("tribunale", "procedura_rge", "lotto", "address", "property_type", "ownership_right"):
        if ci.get(field) not in (None, ""):
            add(category="identity", field=field, label=field, value=ci[field], source_path=f"case_identity.{field}", evidence_pages=ci.get("evidence_pages"), money=None)

    for index, lot in enumerate(worksheet.get("lots") or []):
        lid = str(lot.get("lot_id") or "")
        lot_label = f"Lotto {lid}"
        for field in ("address", "property_type", "ownership_right"):
            if lot.get(field):
                add(category="identity", field=field, label=lot_label, value=lot[field], source_path=f"lots[{index}].{field}", evidence_pages=lot.get("evidence_pages"), money=None)
        if lot.get("occupancy_status"):
            add(category="occupancy", field="occupancy", label=lot_label, value={"status": lot.get("occupancy_status"), "title_info": lot.get("occupancy_status"), "opponibility": None, "registration_dates": [], "expiry_dates": []}, source_path=f"lots[{index}].occupancy_status", evidence_pages=lot.get("evidence_pages"), money=None)
        for field, role in (("prezzo_base_asta", doc_signals.ROLE_AUCTION_BASE_PRICE), ("sale_value", doc_signals.ROLE_JUDICIAL_SALE_VALUE)):
            if lot.get(field) not in (None, 0):
                value = {"amount": lot[field], "role": role}
                add(category="money", field=field, label=lot_label, value=value, source_path=f"lots[{index}].{field}", evidence_pages=lot.get("evidence_pages"), money=value)

    oc = worksheet.get("occupancy") or {}
    if any(oc.get(k) for k in ("status", "title_info", "opponibility", "registration_dates", "expiry_dates")):
        value = {k: oc.get(k) for k in ("status", "title_info", "opponibility", "registration_dates", "expiry_dates")}
        add(category="occupancy", field="occupancy", label="Occupazione", value=value, source_path="occupancy", evidence_pages=oc.get("evidence_pages"), money=None)
    for index, risk in enumerate(oc.get("risks") or []):
        add(category="occupancy", field="risk", label="Rischio occupazione", value=risk, source_path=f"occupancy.risks[{index}]", evidence_pages=oc.get("evidence_pages"), money=None)

    for index, item in enumerate(worksheet.get("technical_compliance") or []):
        add(category="compliance", field=validator._area_token(item.get("area")), label=item.get("area"), value=dict(item), source_path=f"technical_compliance[{index}]", evidence_pages=item.get("evidence_pages"), money=None)
    for index, item in enumerate(worksheet.get("legal_formalities") or []):
        add(category="formality", field=str(item.get("type") or "other"), label=item.get("description") or item.get("type"), value=dict(item), source_path=f"legal_formalities[{index}]", evidence_pages=item.get("evidence_pages"), money=None)
    for index, item in enumerate(worksheet.get("risk_classification") or []):
        add(category="risk", field=validator._area_token(item.get("area")), label=item.get("area"), value=dict(item), source_path=f"risk_classification[{index}]", evidence_pages=item.get("evidence_pages"), money=None)

    money = worksheet.get("money") or {}
    role_fields = {
        "market_value": doc_signals.ROLE_MARKET_VALUE,
        "current_state_value": doc_signals.ROLE_STATE_OF_FACT_VALUE,
        "sale_value": doc_signals.ROLE_JUDICIAL_SALE_VALUE,
        "regularization_costs": doc_signals.ROLE_REGULARIZATION_COST,
        "cancellation_costs": doc_signals.ROLE_BUYER_SIDE_COST,
    }
    for field, role in role_fields.items():
        amount = money.get(field)
        if amount not in (None, 0):
            value = {"amount": amount, "role": role}
            add(category="money", field=field, label=field, value=value, source_path=f"money.{field}", evidence_pages=money.get("evidence_pages"), money=value)
    for collection, role in (("deductions", doc_signals.ROLE_DEPRECIATION), ("buyer_side_costs", doc_signals.ROLE_BUYER_SIDE_COST), ("procedure_cancelled_costs", doc_signals.ROLE_PROCEDURE_CANCELLED_FORMALITY)):
        for index, row in enumerate(money.get(collection) or []):
            value = {"amount": row.get("amount"), "role": role}
            add(category="money", field=collection, label=row.get("label"), value=value, source_path=f"money.{collection}[{index}]", evidence_pages=row.get("evidence_pages"), money=value)
    shared = (segmentation.get("shared_summary_projection") or {}).get("projected") or {}
    for lid, rows in shared.items():
        for index, row in enumerate(rows or []):
            role = doc_signals.role_for_kind(doc_signals.label_kind(row.get("label")))
            value = {"amount": row.get("amount"), "role": role}
            fact = _fact(
                category="money", field=row.get("field"), label=f"Lotto {lid}: {row.get('label')}",
                value=value, source_path=f"projected[{lid}][{index}]",
                evidence_pages=row.get("evidence_pages"), segmentation=segmentation,
                lot_ids=lot_ids, ordinal=len(facts), money=value,
                source_stage="shared_summary_projection",
            )
            facts.append(fact)
    full_pages = segmentation.get("full_document_pages") or []
    if full_pages:
        page_text = {
            int(page.get("page_number", index)): str(page.get("text") or "")
            for index, page in enumerate(full_pages, 1)
        }
        for index, surface in enumerate(doc_signals.extract_surface_cadastral(full_pages)):
            evidence = surface.get("evidence_pages") or []
            raw_value = str(surface.get("value") or "")
            matching_lines: List[str] = []
            for page in evidence:
                matching_lines.extend(
                    line for line in page_text.get(int(page), "").splitlines()
                    if raw_value and raw_value in line
                )
            fact = _fact(
                category="surface_cadastral", field=surface.get("field"),
                label=_item_text(surface.get("label"), matching_lines), value=dict(surface),
                source_path=f"surface_cadastral[{index}]", evidence_pages=evidence,
                segmentation=segmentation, lot_ids=lot_ids, ordinal=len(facts), money=None,
                source_stage="document_signal",
            )
            facts.append(fact)
    return {"schema_version": FACT_LEDGER_SCHEMA_VERSION, "facts": facts}
