"""Canonical case/lot verdicts over Branch 1 reconciled semantic facts.

This module never selects pages, decides applicability, or reconciles facts. A
live lot verdict consumes the worksheet returned by Branch 1 reconciliation and
uses Branch 1's unchanged ``build_case_fact_ledger`` generator for native fact
IDs. Render-only legacy reports remain readable with explicit fail-safe lineage.
"""

from __future__ import annotations

import copy
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from . import doc_signals, fact_lineage, lots, validator

SCHEMA_VERSION = "cv2.verdict.v1"

SEVERITY_LABELS_IT = {
    "verde": "Nessuna criticità rilevata",
    "info": "Informativo",
    "minore": "Minore",
    "media": "Da verificare",
    "grave": "Grave",
}
SEVERITY_ORDER = {"verde": 0, "info": 1, "minore": 2, "media": 3, "grave": 4}
PRIORITY_BASE_BY_SEVERITY = {
    "grave": 0,
    "media": 3,
    "minore": 5,
    "info": 6,
    "verde": 7,
}

# Frozen projections reverse-engineered from current public behavior (A1).
DECISION_LEVEL_BY_SEVERITY = {
    "verde": "pronto_con_avvertenze",
    "info": "pronto_con_avvertenze",
    "minore": "da_verificare",
    "media": "da_verificare",
    "grave": "attenzione",
}
ESITO_LEVEL_BY_SEVERITY = {
    "verde": "verde",
    "info": "verde",
    "minore": "ambra",
    "media": "ambra",
    "grave": "rosso",
}

AGGREGATION_CEILING = "AGGREGATION_CEILING"
AGGREGATION_FLOOR = "AGGREGATION_FLOOR"
UNRESOLVED_LOT = "UNRESOLVED_LOT"
LEGACY_ARTIFACT_NO_LEDGER = "LEGACY_ARTIFACT_NO_LEDGER"
PROVENANCE_REASONS = frozenset({
    AGGREGATION_CEILING,
    AGGREGATION_FLOOR,
    UNRESOLVED_LOT,
    LEGACY_ARTIFACT_NO_LEDGER,
})
PROJECTED_CASE_FACT_ORIGIN = "BRANCH1_PROJECTED_STAMP"

_SAFE_INTERACTIVE = frozenset({
    "LOT_SELECTION_REQUIRED", "MONEY_CONFIRMATION_REQUIRED", "DOCUMENT_NOT_READABLE",
})
_SAFE_STATUSES = frozenset({"REPORT_READY"}) | _SAFE_INTERACTIVE
_COMPLIANCE_SEVERITY = {
    "conforming": "verde",
    "uncertain": "minore",
    "regularizable": "media",
    "non_conforming": "grave",
    "not_regularizable": "grave",
}
_VACANT_EQUIVALENTS = frozenset({"libero", "libera", "free", "vacant", "unoccupied"})
_GENERIC_FACT_FIELDS = frozenset({"", "other", "unknown", "risk", "occupancy"})


def compliance_severity(classification: Any) -> str:
    return _COMPLIANCE_SEVERITY.get(str(classification or ""), "info")


def validate_verdict(verdict: Dict[str, Any]) -> Dict[str, Any]:
    """Reject unknown canonical versions and malformed severity fail-closed."""
    if not isinstance(verdict, dict) or verdict.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported canonical verdict schema")
    if verdict.get("severity") not in SEVERITY_ORDER:
        raise ValueError("unsupported canonical verdict severity")
    if verdict.get("scope") not in {"lot", "case"}:
        raise ValueError("unsupported canonical verdict scope")
    if not isinstance(verdict.get("field_verdicts"), dict):
        raise ValueError("malformed canonical field verdicts")
    projections = verdict.get("display_projections") or {}
    if projections.get("decision_level") not in {
        None, "attenzione", "da_verificare", "pronto_con_avvertenze", "non_leggibile",
    }:
        raise ValueError("unsupported canonical decision projection")
    if projections.get("esito_level") not in {None, "rosso", "ambra", "verde"}:
        raise ValueError("unsupported canonical esito projection")
    pending: List[Any] = [verdict]
    while pending:
        value = pending.pop()
        if isinstance(value, dict):
            reason = value.get("provenance_reason")
            if reason is not None and reason not in PROVENANCE_REASONS:
                raise ValueError("unsupported canonical provenance reason")
            pending.extend(value.values())
        elif isinstance(value, list):
            pending.extend(value)
    return verdict


def try_validate_verdict(verdict: Any) -> Optional[Dict[str, Any]]:
    """Validate persisted/untrusted verdict data without breaking its consumer."""
    if not isinstance(verdict, dict):
        return None
    try:
        return validate_verdict(verdict)
    except ValueError:
        return None


def project_to_decision_level(verdict_or_severity: Any) -> str:
    if isinstance(verdict_or_severity, dict):
        verdict = validate_verdict(verdict_or_severity)
        if verdict.get("report_status") == "DOCUMENT_NOT_READABLE":
            return "non_leggibile"
        projected = (verdict.get("display_projections") or {}).get("decision_level")
        if projected:
            return str(projected)
        severity = verdict["severity"]
    else:
        severity = str(verdict_or_severity)
    return DECISION_LEVEL_BY_SEVERITY[severity]


def project_to_esito_level(verdict_or_severity: Any) -> str:
    if isinstance(verdict_or_severity, dict):
        verdict = validate_verdict(verdict_or_severity)
        projected = (verdict.get("display_projections") or {}).get("esito_level")
        if projected:
            return str(projected)
        if verdict.get("report_status") in _SAFE_INTERACTIVE:
            return "ambra"
        severity = verdict["severity"]
    else:
        severity = str(verdict_or_severity)
    return ESITO_LEVEL_BY_SEVERITY[severity]


def cross_band_contradiction(decision_level: str, esito_level: str) -> bool:
    clean = esito_level == "verde" or decision_level == "pronto_con_avvertenze"
    blocking = esito_level == "rosso" or decision_level == "attenzione"
    return clean and blocking


def same_canonical_source(left: Dict[str, Any], right: Dict[str, Any]) -> bool:
    ref = left.get("canonical_verdict_ref")
    return bool(ref and ref == right.get("canonical_verdict_ref"))


def priority_from_severity(severity: str, within_band: int = 0) -> int:
    """Presentation priority derived from the sole severity order."""
    if severity not in PRIORITY_BASE_BY_SEVERITY:
        raise ValueError("unsupported priority severity")
    return max(0, PRIORITY_BASE_BY_SEVERITY[severity] + int(within_band))


def _norm(value: Any) -> str:
    return " ".join(validator._norm(value).split())


def _semantic_fact_key(fact: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    """Return a content-stable fact dimension, or no secondary match key.

    Branch 1 deliberately uses schema fallback fields such as ``other`` for an
    untyped formality. Those values remain valid lineage/fact-ID inputs, but
    are too generic to prove that two different facts address one dimension.
    Formalities retain enough source content to recover a stable token; other
    generic category fields stay non-matchable so aggregation fails closed.
    """
    category = str(fact.get("category") or "").strip()
    field_token = validator._area_token(str(fact.get("field") or ""))
    if not category:
        return None
    if field_token not in _GENERIC_FACT_FIELDS:
        return category, field_token
    if category != "formality":
        return None
    value = fact.get("value") if isinstance(fact.get("value"), dict) else {}
    for candidate in (
        fact.get("label"), value.get("description"), value.get("type"),
    ):
        content_token = validator._area_token(str(candidate or ""))
        if content_token not in _GENERIC_FACT_FIELDS:
            return category, content_token
    return None


def _has_keyword(value: Any, *keywords: str) -> bool:
    normalized = _norm(value)
    return any(keyword in normalized for keyword in keywords)


def _decision_driver_codes(report: Dict[str, Any]) -> List[str]:
    """Legacy display reasons selected once, without carrying free narration."""
    codes: List[str] = []

    def add(code: str) -> None:
        if code not in codes:
            codes.append(code)

    occupancy = report.get("occupancy_section") or {}
    if _norm(occupancy.get("status")) == "occupato" or _has_keyword(
        occupancy.get("status_label"), "occupat"
    ):
        add("OCCUPIED")
    if occupancy.get("opponibility") or occupancy.get("registration_dates") or occupancy.get("expiry_dates"):
        add("LEASE_TITLE")
    for risk in occupancy.get("risks") or []:
        if _has_keyword(risk, "opponib", "titolo", "senza titolo"):
            add("LEASE_TITLE")
    for section in report.get("risk_sections") or []:
        for item in section.get("items") or []:
            summary = f"{item.get('area') or ''} {item.get('summary') or ''}"
            if _has_keyword(summary, "collabente", "crollo", "strutt", "pericol"):
                add("STRUCTURAL")
            if _has_keyword(summary, "amianto", "fibrocement", "fibro-cement", "eternit"):
                add("HAZARDOUS_MATERIALS")
    compliance = report.get("compliance_section") or []
    for item in compliance:
        area = f"{item.get('area') or ''} {item.get('notes') or ''}"
        classification = _norm(item.get("classification"))
        if _has_keyword(area, "agibil") and _has_keyword(area, "non", "manca"):
            add("HABITABILITY")
        if _has_keyword(area, "ape", "certificaz", "conformit") and classification in {
            "non_conforming", "not_regularizable", "uncertain",
        }:
            add("CERTIFICATIONS")
        if classification in {"regularizable", "non_conforming", "not_regularizable"}:
            add("REGULARIZATIONS")
    if any(_norm(item.get("classification")) == "regularizable" for item in compliance):
        add("REGULARIZATION_COSTS")
    money = report.get("money_sections") or {}
    if money.get("uncertain_money"):
        add("UNCERTAIN_MONEY")
    if money.get("buyer_side_costs"):
        add("BUYER_COSTS")
    try:
        if int((report.get("lot_structure") or {}).get("bene_count") or 0) > 1:
            add("MULTI_BENE")
    except (TypeError, ValueError):
        pass
    return codes


def _pages(value: Any) -> List[int]:
    out: List[int] = []
    for raw in value or []:
        try:
            page = int(raw)
        except (TypeError, ValueError):
            continue
        if page not in out:
            out.append(page)
    return sorted(out)


def _leaf(source_fact_id: Optional[str] = None, *, provenance_reason: Optional[str] = None,
          **values: Any) -> Dict[str, Any]:
    if source_fact_id:
        return {**values, "source_fact_id": str(source_fact_id)}
    reason = provenance_reason or LEGACY_ARTIFACT_NO_LEDGER
    if reason not in PROVENANCE_REASONS:
        raise ValueError("unsupported canonical provenance reason")
    return {**values, "provenance_reason": reason}


def _max_severity(values: Iterable[str]) -> str:
    return max(values, key=lambda value: SEVERITY_ORDER[value], default="verde")


def _fact_maps(*ledgers: Optional[Dict[str, Any]]) -> Tuple[Dict[str, Dict[str, Any]], Dict[Tuple[str, str], List[Dict[str, Any]]]]:
    by_id: Dict[str, Dict[str, Any]] = {}
    by_key: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for ledger in ledgers:
        for fact in (ledger or {}).get("facts") or []:
            fid = str(fact.get("fact_id") or "")
            if not fid:
                continue
            by_id[fid] = fact
            by_key.setdefault(
                (str(fact.get("category") or ""), str(fact.get("source_path") or "")), []
            ).append(fact)
    return by_id, by_key


def _projected_case_fact_ids(
    worksheet: Optional[Dict[str, Any]], case_by_id: Dict[str, Dict[str, Any]],
) -> set[str]:
    """Identify case IDs by Branch 1's projection stamp, never string overlap."""
    projected: set[str] = set()
    pending: List[Any] = [worksheet] if isinstance(worksheet, dict) else []
    while pending:
        value = pending.pop()
        if isinstance(value, dict):
            fid = str(value.get("fact_id") or "")
            if value.get("projected") is True and fid in case_by_id:
                projected.add(fid)
            pending.extend(value.values())
        elif isinstance(value, list):
            pending.extend(value)
    return projected


def _minimal_segmentation(worksheet: Dict[str, Any], lot_id: str,
                          segmentation: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out = copy.deepcopy(segmentation or {})
    evidence: set[int] = set()
    for block in (
        worksheet.get("case_identity") or {}, worksheet.get("occupancy") or {},
        worksheet.get("money") or {},
    ):
        evidence.update(_pages(block.get("evidence_pages")))
    for collection in ("lots", "technical_compliance", "legal_formalities", "risk_classification"):
        for item in worksheet.get(collection) or []:
            evidence.update(_pages(item.get("evidence_pages")))
    out["lot_ids"] = [str(lot_id)]
    out.setdefault("lot_pages", {str(lot_id): sorted(evidence)})
    out.setdefault("global_pages", [])
    out.setdefault("shared_pages", [])
    out.setdefault("page_assignments", [
        {"page": page, "method": "explicit", "assigned_lot": str(lot_id)}
        for page in sorted(evidence)
    ])
    return out


def build_reconciled_fact_ledger(
    worksheet: Dict[str, Any], lot_id: str, *,
    segmentation: Optional[Dict[str, Any]] = None,
    lot_report: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """A2 adapter: invoke Branch 1's unchanged generator on reconciled output."""
    seg = _minimal_segmentation(worksheet, str(lot_id), segmentation)
    report = copy.deepcopy(lot_report) if isinstance(lot_report, dict) else lots.build_lot_report(worksheet, None)
    report.update({"lot_ids": [str(lot_id)], "lot_count": 1, "multi_lot": False})
    return fact_lineage.build_case_fact_ledger(worksheet, seg, report)


def _source_id(item: Dict[str, Any], category: str, path: str,
               by_key: Dict[Tuple[str, str], List[Dict[str, Any]]]) -> Optional[str]:
    if item.get("fact_id"):
        return str(item["fact_id"])
    facts = by_key.get((category, path), [])
    # Case ledger is loaded first and the reconciled-lot ledger second. A
    # native leaf must therefore resolve to the latter; projected leaves have
    # already returned their stamped case fact ID above.
    return str(facts[-1]["fact_id"]) if facts else None


def _final_money(report: Dict[str, Any], worksheet: Optional[Dict[str, Any]],
                 by_key: Dict[Tuple[str, str], List[Dict[str, Any]]],
                 by_id: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    chain = ((report.get("money_sections") or {}).get("valuation_chain")) or []
    role_rank = {
        doc_signals.ROLE_MARKET_VALUE: 1,
        doc_signals.ROLE_STATE_OF_FACT_VALUE: 2,
        doc_signals.ROLE_JUDICIAL_SALE_VALUE: 3,
    }
    candidates = []
    for position, row in enumerate(chain):
        if not isinstance(row, dict) or row.get("amount") is None:
            continue
        role = doc_signals.role_for_kind(doc_signals.label_kind(row.get("label")))
        if role in role_rank:
            candidates.append((role_rank[role], position, role, row))
    if candidates:
        _, _, role, row = max(candidates)
        amount = row.get("amount")
        match = next((fact for fact in by_id.values() if fact.get("category") == "money"
                      and isinstance(fact.get("value"), dict)
                      and fact["value"].get("amount") is not None
                      and abs(float(fact["value"]["amount"]) - float(amount)) <= 1.0
                      and (fact.get("money") or {}).get("role") == role), None)
        return _leaf(str(match["fact_id"]) if match else None, amount=amount, role=role)
    for row in reversed(chain):
        if isinstance(row, dict) and row.get("amount") is not None and row.get("kind") == "value":
            amount = row.get("amount")
            role = str(row.get("kind") or "value")
            match = next((fact for fact in by_id.values() if fact.get("category") == "money"
                          and isinstance(fact.get("value"), dict)
                          and fact["value"].get("amount") is not None
                          and abs(float(fact["value"]["amount"]) - float(amount)) <= 1.0), None)
            return _leaf(str(match["fact_id"]) if match else None, amount=amount, role=role)
    money = (worksheet or {}).get("money") or {}
    for field, role in (
        ("sale_value", "JUDICIAL_SALE_VALUE"),
        ("current_state_value", "STATE_OF_FACT_VALUE"),
        ("market_value", "MARKET_VALUE"),
    ):
        if money.get(field) is not None:
            facts = by_key.get(("money", f"money.{field}"), [])
            return _leaf(str(facts[0]["fact_id"]) if facts else None,
                         amount=money[field], role=role)
    return _leaf(amount=None, role="UNKNOWN",
                 provenance_reason=LEGACY_ARTIFACT_NO_LEDGER if worksheet is None else UNRESOLVED_LOT)


def _canonical_ref(scope: str, scope_id: Any) -> str:
    return f"{SCHEMA_VERSION}:{scope}:{scope_id}"


def _base_severity(report: Dict[str, Any]) -> Tuple[str, List[Dict[str, Any]]]:
    status = str(report.get("report_status") or "REPORT_READY")
    if status not in _SAFE_STATUSES:
        return "grave", []
    if status in _SAFE_INTERACTIVE:
        return "media", []
    severities: List[str] = []
    drivers: List[Dict[str, Any]] = []
    for section in report.get("risk_sections") or []:
        for index, item in enumerate(section.get("items") or []):
            severity = str(item.get("severity") or "info")
            if severity not in SEVERITY_ORDER:
                severity = "info"
            if item.get("blocks_saleability"):
                severity = "grave"
            severities.append(severity if severity != "info" else "minore")
            drivers.append({"driver_id": f"risk:{index}", "text": str(item.get("summary") or item.get("area") or "")})
    for index, item in enumerate(report.get("compliance_section") or []):
        severity = _COMPLIANCE_SEVERITY.get(str(item.get("classification") or "uncertain"), "minore")
        if item.get("blocks_saleability"):
            severity = "grave"
        severities.append(severity)
        if severity != "verde":
            drivers.append({"driver_id": f"compliance:{index}", "text": str(item.get("notes") or item.get("area") or "")})
    occupancy = report.get("occupancy_section") or {}
    occupancy_text = _norm(occupancy.get("status_label") or occupancy.get("status"))
    if occupancy_text and occupancy_text not in _VACANT_EQUIVALENTS:
        severities.append("media")
        drivers.append({
            "driver_id": "occupancy",
            "text": str(occupancy.get("status_label") or occupancy.get("status")),
        })
    money = report.get("money_sections") or {}
    if money.get("uncertain_money"):
        severities.append("minore")
        row = next((item for item in money.get("uncertain_money") or [] if isinstance(item, dict)), {})
        drivers.append({"driver_id": "money:uncertain", "text": str(row.get("label") or "")})
    if money.get("buyer_side_costs"):
        severities.append("media")
        row = next((item for item in money.get("buyer_side_costs") or [] if isinstance(item, dict)), {})
        drivers.append({"driver_id": "money:buyer", "text": str(row.get("label") or "")})
    for index, item in enumerate(report.get("formalities_section") or []):
        if not isinstance(item, dict):
            continue
        if not item.get("cancelled_by_procedure") and not item.get("buyer_burden"):
            severities.append("media")
            drivers.append({
                "driver_id": f"formality:{index}",
                "text": str(item.get("description") or item.get("type") or ""),
            })
    return _max_severity(severities), drivers


def _semantic_report(
    report: Dict[str, Any], worksheet: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    """Expose reconciled worksheet facts in renderer shape for derivation."""
    if worksheet is None:
        return report
    money = worksheet.get("money") or {}
    return {
        "report_status": report.get("report_status"),
        "lot_structure": report.get("lot_structure") or {},
        "occupancy_section": worksheet.get("occupancy") or {},
        "compliance_section": worksheet.get("technical_compliance") or [],
        "risk_sections": [{"items": worksheet.get("risk_classification") or []}],
        "formalities_section": worksheet.get("legal_formalities") or [],
        "money_sections": {
            "uncertain_money": money.get("uncertain_money") or [],
            "buyer_side_costs": money.get("buyer_side_costs") or [],
        },
    }


def build_lot_verdict(
    report: Optional[Dict[str, Any]] = None, *,
    reconciled_worksheet: Optional[Dict[str, Any]] = None,
    case_ledger: Optional[Dict[str, Any]] = None,
    segmentation: Optional[Dict[str, Any]] = None,
    lot_report: Optional[Dict[str, Any]] = None,
    lot_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Build the sole lot verdict from reconciled facts or a legacy artifact."""
    report = report if isinstance(report, dict) else {}
    existing = report.get("canonical_verdict")
    if isinstance(existing, dict):
        validated = try_validate_verdict(existing)
        if validated is not None:
            return copy.deepcopy(validated)
    lot_structure = report.get("lot_structure") or {}
    scope_id = str(lot_id or lot_structure.get("selected_lot") or report.get("analysis_id") or "lot")
    worksheet = reconciled_worksheet if isinstance(reconciled_worksheet, dict) else None
    lot_ledger = build_reconciled_fact_ledger(
        worksheet, scope_id, segmentation=segmentation, lot_report=lot_report
    ) if worksheet else None
    case_by_id, _ = _fact_maps(case_ledger)
    lot_by_id, _ = _fact_maps(lot_ledger)
    by_id, by_key = _fact_maps(case_ledger, lot_ledger)
    projected_case_ids = _projected_case_fact_ids(worksheet, case_by_id)

    ci = (worksheet.get("case_identity") if worksheet else report.get("case_identity")) or {}
    typology_id = _source_id(ci, "identity", "case_identity.property_type", by_key) if worksheet else None
    typology = _leaf(typology_id, value=ci.get("property_type"),
                     confidence="high" if typology_id else "low")
    beni = [
        _leaf(value=bene.get("property_type"), bene_id=bene.get("bene_id"))
        for bene in report.get("beni_sections") or [] if bene.get("property_type")
    ]
    if beni:
        typology["per_bene"] = beni

    oc = (worksheet.get("occupancy") if worksheet else report.get("occupancy_section")) or {}
    occupancy_id = _source_id(oc, "occupancy", "occupancy", by_key) if worksheet else None
    occupancy_value = oc.get("status") or oc.get("status_label")
    if not occupancy_id and (_norm(occupancy_value) in _VACANT_EQUIVALENTS or worksheet is None):
        occupancy_value = "UNKNOWN"
    if occupancy_id and _norm(occupancy_value) in _VACANT_EQUIVALENTS:
        fact = by_id.get(occupancy_id) or {}
        if fact.get("declaration_status") != "explicit_declaration" or not fact.get("evidence_pages"):
            occupancy_value = "UNKNOWN"
    occupancy = _leaf(occupancy_id, value=occupancy_value or "UNKNOWN",
                      confidence="high" if occupancy_id and occupancy_value != "UNKNOWN" else "low")

    compliance_source = worksheet.get("technical_compliance") if worksheet else report.get("compliance_section")
    compliance: List[Dict[str, Any]] = []
    formalities: List[Dict[str, Any]] = []
    for index, item in enumerate(compliance_source or []):
        fid = _source_id(item, "compliance", f"technical_compliance[{index}]", by_key) if worksheet else None
        compliance.append(_leaf(fid, area=item.get("area"),
                                classification=item.get("classification") or "uncertain"))
    formalities_source = worksheet.get("legal_formalities") if worksheet else report.get("formalities_section")
    for index, item in enumerate(formalities_source or []):
        fid = _source_id(item, "formality", f"legal_formalities[{index}]", by_key) if worksheet else None
        if item.get("cancelled_by_procedure"):
            state = "cancelled_by_procedure"
        elif item.get("buyer_burden"):
            state = "buyer_burden"
        else:
            state = "to_verify"
        formalities.append(_leaf(fid, type=item.get("type"), status=state))

    semantic_report = _semantic_report(report, worksheet)
    severity, report_drivers = _base_severity(semantic_report)
    if worksheet is not None and occupancy.get("value") == "UNKNOWN":
        severity = _max_severity((severity, "media"))
    driver_ids: Dict[str, Optional[str]] = {}
    for index, item in enumerate(compliance_source or []):
        driver_ids[f"compliance:{index}"] = _source_id(
            item, "compliance", f"technical_compliance[{index}]", by_key
        ) if worksheet else None
    risk_source = worksheet.get("risk_classification") if worksheet else []
    for index, item in enumerate(risk_source or []):
        driver_ids[f"risk:{index}"] = _source_id(
            item, "risk", f"risk_classification[{index}]", by_key
        ) if worksheet else None
    driver_ids["occupancy"] = occupancy_id
    for index, item in enumerate(formalities_source or []):
        driver_ids[f"formality:{index}"] = _source_id(
            item, "formality", f"legal_formalities[{index}]", by_key
        ) if worksheet else None
    drivers = [
        _leaf(driver_ids.get(driver["driver_id"]), driver_id=driver["driver_id"], text=driver["text"])
        for driver in report_drivers if str(driver.get("text") or "").strip()
    ][:5]
    headline = SEVERITY_LABELS_IT[severity]
    readiness = {
        "grave": "TECHNICAL_REVIEW_REQUIRED", "media": "READY_FOR_REVIEW",
        "minore": "READY_FOR_REVIEW", "info": "COMPLETE_FOR_EXPORT",
        "verde": "COMPLETE_FOR_EXPORT",
    }[severity]
    if report.get("report_status") in _SAFE_INTERACTIVE:
        readiness = "CONFIRMATIONS_REQUIRED"

    conflicts = [{
        "path": row.get("path"), "case_value": row.get("case_value"),
        "lot_value": row.get("lot_value"),
        "reason_code": row.get("reason") or "CONFLICT_REQUIRES_REVIEW",
    } for row in (report.get("lot_fact_projection") or {}).get("conflicts") or []]
    used_ids = [
        value.get("source_fact_id") for value in [typology, occupancy, *compliance, *formalities]
        if value.get("source_fact_id")
    ] + [
        driver.get("source_fact_id") for driver in drivers
        if driver.get("source_fact_id")
    ]
    addressed_fact_keys = sorted({
        fact_key
        for fid in used_ids
        for fact in [
            case_by_id.get(str(fid))
            if str(fid) in projected_case_ids
            else lot_by_id.get(str(fid))
        ]
        if isinstance(fact, dict)
        for fact_key in [_semantic_fact_key(fact)]
        if fact_key is not None
    })
    case_fact_ids = list(dict.fromkeys(
        str(fid) for fid in used_ids if str(fid) in projected_case_ids
    ))
    lot_fact_ids = list(dict.fromkeys(
        str(fid) for fid in used_ids
        if str(fid) not in projected_case_ids and str(fid) in lot_by_id
    ))
    verdict = {
        "schema_version": SCHEMA_VERSION, "scope": "lot", "scope_id": scope_id,
        "canonical_ref": _canonical_ref("lot", scope_id),
        "report_status": str(report.get("report_status") or "REPORT_READY"),
        "severity": severity, "severity_label_it": SEVERITY_LABELS_IT[severity],
        "readiness": readiness, "headline": headline, "drivers": drivers,
        "field_verdicts": {
            "typology": typology, "occupancy": occupancy,
            "compliance": compliance, "formalities": formalities,
            "money": {"final_value": _final_money(report, worksheet, by_key, by_id)},
        },
        "provenance": {
            "case_fact_ids": case_fact_ids,
            "case_fact_ids_origin": PROJECTED_CASE_FACT_ORIGIN,
            "lot_fact_ids": lot_fact_ids,
            "addressed_fact_keys": [
                {"category": category, "field_token": field_token}
                for category, field_token in addressed_fact_keys
            ],
            "aggregation_rule": "RECONCILED_LOT_FACTS" if worksheet else LEGACY_ARTIFACT_NO_LEDGER,
        },
        "conflicts": conflicts,
        "generated_from": {
            "contract_schema_version": (report.get("sections_meta") or {}).get("source_contract_schema"),
            "worksheet_schema_version": worksheet.get("schema_version") if worksheet else None,
        },
        "display_projections": {
            "decision_level": DECISION_LEVEL_BY_SEVERITY[severity],
            "esito_level": ESITO_LEVEL_BY_SEVERITY[severity],
            "decision_driver_codes": _decision_driver_codes(semantic_report),
        },
    }
    return validate_verdict(verdict)


def refine_for_runtime(
    verdict: Dict[str, Any], *, report_status: str, readiness: Dict[str, Any],
    open_checks: bool = False,
) -> Dict[str, Any]:
    """Fold deterministic confirmation/checklist state into the same verdict.

    Findings remain fact-derived detail. This operation only applies the frozen
    state thresholds that previously lived in ``decision_model._build_esito``.
    """
    out = copy.deepcopy(validate_verdict(verdict))
    state = str(readiness.get("state") or "")
    if report_status not in _SAFE_STATUSES or state == "TECHNICAL_REVIEW_REQUIRED":
        out["severity"] = "grave"
    out["severity_label_it"] = SEVERITY_LABELS_IT[out["severity"]]
    out["readiness"] = state or out["readiness"]
    # These frozen thresholds are the pre-branch _build_esito behavior. They
    # live on the canonical object so both legacy views consume one result.
    if state == "TECHNICAL_REVIEW_REQUIRED":
        esito_level = "rosso"
    elif report_status != "REPORT_READY":
        esito_level = "ambra"
    elif state == "COMPLETE_FOR_EXPORT" and not open_checks:
        esito_level = "verde"
    else:
        esito_level = "ambra"
    out["display_projections"] = {
        "decision_level": DECISION_LEVEL_BY_SEVERITY[out["severity"]],
        "esito_level": esito_level,
        "decision_driver_codes": list(
            (out.get("display_projections") or {}).get("decision_driver_codes") or []
        ),
    }
    return out


def build_case_verdict(
    lot_verdicts: Sequence[Optional[Dict[str, Any]]], *, scope_id: str = "case",
    all_lot_ids: Optional[Sequence[str]] = None,
    case_global_facts: Optional[Sequence[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    completed: Dict[str, Dict[str, Any]] = {}
    for candidate in lot_verdicts or []:
        if candidate is None:
            continue
        verdict = copy.deepcopy(validate_verdict(candidate))
        if verdict.get("scope") != "lot":
            raise ValueError("case aggregation accepts canonical lot verdicts only")
        completed[str(verdict.get("scope_id"))] = verdict
    ordered_ids = [str(value) for value in (all_lot_ids or completed.keys())]
    per_lot: Dict[str, Any] = {}
    unresolved: List[str] = []
    for lot_id in ordered_ids:
        if lot_id in completed:
            per_lot[lot_id] = completed[lot_id]["field_verdicts"]
        else:
            unresolved.append(lot_id)
            per_lot[lot_id] = {"status": "UNKNOWN", "provenance_reason": UNRESOLVED_LOT}
    severity = _max_severity(v["severity"] for v in completed.values())
    aggregation_reason = AGGREGATION_FLOOR
    global_driver = None
    for fact in case_global_facts or []:
        if fact.get("applicability") not in {fact_lineage.CASE_GLOBAL, fact_lineage.ALL_LOTS}:
            continue
        value = fact.get("value") if isinstance(fact.get("value"), dict) else {}
        independently_grave = value.get("severity") == "grave" or value.get("classification") in {
            "non_conforming", "not_regularizable",
        }
        fact_key = _semantic_fact_key(fact)
        # There are exactly two legitimate positive matches: (1) the lot
        # carries Branch 1's projection stamp for this exact case fact, or
        # (2) source-backed lot evidence addresses the same stable dimension.
        # Missing/legacy origin markers and generic content keys fail closed.
        already_reflected = any(
            (
                v.get("provenance", {}).get("case_fact_ids_origin")
                == PROJECTED_CASE_FACT_ORIGIN
                and str(fact.get("fact_id"))
                in set(v.get("provenance", {}).get("case_fact_ids") or [])
            )
            or (fact_key is not None and fact_key in {
                (str(key.get("category") or ""), str(key.get("field_token") or ""))
                for key in v.get("provenance", {}).get("addressed_fact_keys") or []
                if isinstance(key, dict)
            })
            for v in completed.values()
        )
        if independently_grave and not already_reflected and severity != "grave":
            severity = "grave"
            aggregation_reason = AGGREGATION_CEILING
            global_driver = _leaf(str(fact.get("fact_id")), driver_id="case_global",
                                  text=str(value.get("summary") or fact.get("label") or ""))
            break
    conflicts: List[Dict[str, Any]] = []
    seen: Dict[str, Tuple[str, Optional[str]]] = {}
    for lot_id, verdict in completed.items():
        for driver in verdict.get("drivers") or []:
            text = _norm(driver.get("text"))
            if len(text) < 24:
                continue
            fid = driver.get("source_fact_id")
            if text in seen and fid != seen[text][1]:
                conflicts.append({
                    "path": "drivers", "case_value": seen[text][0], "lot_value": lot_id,
                    "reason_code": "POSSIBLE_CROSS_LOT_LEAKAGE",
                })
            else:
                seen[text] = (lot_id, fid)
    type_values = {_norm(v["field_verdicts"]["typology"].get("value")) for v in completed.values()}
    occupancy_values = {_norm(v["field_verdicts"]["occupancy"].get("value")) for v in completed.values()}
    return validate_verdict({
        "schema_version": SCHEMA_VERSION, "scope": "case", "scope_id": str(scope_id),
        "canonical_ref": _canonical_ref("case", scope_id), "report_status": "REPORT_READY",
        "severity": severity, "severity_label_it": SEVERITY_LABELS_IT[severity],
        "readiness": "CONFIRMATIONS_REQUIRED" if unresolved else (
            "TECHNICAL_REVIEW_REQUIRED" if severity == "grave" else "READY_FOR_REVIEW"
        ),
        "headline": SEVERITY_LABELS_IT[severity],
        "drivers": [global_driver] if global_driver else [],
        "field_verdicts": {
            "per_lot": per_lot,
            "heterogeneous": len(type_values) > 1 or len(occupancy_values) > 1,
            "aggregation": {"provenance_reason": aggregation_reason},
        },
        "provenance": {
            "case_fact_ids": [global_driver["source_fact_id"]] if global_driver else [],
            "lot_fact_ids": [fid for verdict in completed.values()
                             for fid in verdict.get("provenance", {}).get("lot_fact_ids") or []],
            "aggregation_rule": aggregation_reason,
        },
        "conflicts": conflicts, "unresolved_lot_ids": unresolved,
        "generated_from": {"contract_schema_version": None, "worksheet_schema_version": None},
    })
