"""Fail-closed disclosure classification for quality-blocked lot reports.

The classifier never changes CanonicalFacts, CanonicalVerdict, readiness,
severity, or report facts. The projection evaluates a copied verdict against
the already-blocked readiness, then adds a separate disclosure contract.
"""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional

FULL_REPORT_AVAILABLE = "FULL_REPORT_AVAILABLE"
PARTIAL_REPORT_AVAILABLE = "PARTIAL_REPORT_AVAILABLE"
REPORT_BLOCKED = "REPORT_BLOCKED"

PARTIAL_ELIGIBLE_CODES = frozenset({
    "CRITICAL_FACT_MISSING",
    "MISSING_IMPORTANT_MONEY",
})

_FIELD_BY_CATEGORY = {
    "occupancy": "occupancy",
    "property": "typology",
    "typology": "typology",
    "compliance": "compliance",
    "technical_compliance": "compliance",
    "formalities": "formalities",
    "legal_formality": "formalities",
    "money": "money",
    "sale_terms": "money",
}

_CATEGORY_LABELS_IT = {
    "occupancy": "Stato occupativo",
    "property": "Tipologia dell'immobile",
    "typology": "Tipologia dell'immobile",
    "compliance": "Conformità tecnica",
    "technical_compliance": "Conformità tecnica",
    "formalities": "Formalità",
    "legal_formality": "Formalità",
    "money": "Dato economico",
    "sale_terms": "Condizione di vendita",
}

_REASON_LABELS_IT = {
    "CRITICAL_FACT_MISSING": "Dato essenziale non risolto dalla documentazione disponibile",
    "MISSING_IMPORTANT_MONEY": "Importo essenziale non risolto dalla documentazione disponibile",
}

_MONEY_ROLE_LABELS_IT = {
    "market_value": "Valore di mercato",
    "judicial_sale_value": "Valore di vendita giudiziaria",
    "base_price": "Prezzo base",
    "minimum_bid": "Offerta minima",
    "buyer_cost": "Costo a carico dell'acquirente",
    "regularization_cost": "Costo di regolarizzazione",
    "final_value": "Valore finale",
}


def _strings(values: Any) -> List[str]:
    return sorted({str(value) for value in (values or []) if value not in (None, "")})


def _pages(values: Any) -> List[int]:
    out: List[int] = []
    for value in values or []:
        try:
            page = int(value)
        except (TypeError, ValueError):
            continue
        if page > 0 and page not in out:
            out.append(page)
    return sorted(out)


def _fact_field(omission: Dict[str, Any]) -> str:
    category = str(omission.get("category") or "").strip().lower()
    field = _FIELD_BY_CATEGORY.get(category, category or "critical_fact")
    role = str(omission.get("role") or "").strip()
    return f"money.{role}" if field == "money" and role else field


def _accounting(
    *, gate_status: str, codes: List[str], eligible: List[str], hard: List[str],
    disclosure_state: str, unresolved: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Safe metadata only: never includes appraisal prose, values, or pages."""
    return {
        "gate_outcome": str(gate_status or ""),
        "original_blocking_codes": list(codes),
        "partial_eligible_codes": list(eligible),
        "hard_blocking_codes": list(hard),
        "disclosure_state": disclosure_state,
        "unresolved_field_count": len(unresolved),
        "critical_unresolved_count": sum(
            1 for item in unresolved if item.get("severity") == "critical"
        ),
    }


def full_disclosure_accounting(gate_status: Any) -> Dict[str, Any]:
    """Return the safe, empty blocking-code ledger for a clean disclosure."""
    return _accounting(
        gate_status=str(gate_status or ""),
        codes=[],
        eligible=[],
        hard=[],
        disclosure_state=FULL_REPORT_AVAILABLE,
        unresolved=[],
    )


def classify_partial_eligibility(
    *,
    gate_status: Any,
    quality_report: Optional[Dict[str, Any]],
    coverage_audit: Optional[Dict[str, Any]],
    report: Optional[Dict[str, Any]] = None,
    lot_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Classify a failed gate using a strict omission-only allow-list.

    Unknown/malformed codes, contradictions, role conflicts, projection
    conflicts, missing structured omissions, and schema damage all block.
    """
    quality = quality_report if isinstance(quality_report, dict) else {}
    audit = coverage_audit if isinstance(coverage_audit, dict) else {}
    issues = quality.get("blocking_issues")
    omissions = audit.get("critical_omissions")
    codes = _strings(
        issue.get("code") for issue in issues if isinstance(issue, dict)
    ) if isinstance(issues, list) else []
    eligible_codes = sorted(set(codes) & PARTIAL_ELIGIBLE_CODES)
    hard_codes = sorted(set(codes) - PARTIAL_ELIGIBLE_CODES)
    unresolved: List[Dict[str, Any]] = []
    accounted_omissions = [
        {"severity": str(item.get("severity") or "")}
        for item in omissions or []
        if isinstance(item, dict)
    ] if isinstance(omissions, list) else []

    def blocked(reason: str) -> Dict[str, Any]:
        return {
            "eligible": False,
            "reason": reason,
            "disclosure_state": REPORT_BLOCKED,
            "unresolved_fields": [],
            "accounting": _accounting(
                gate_status=str(gate_status or ""), codes=codes,
                eligible=eligible_codes, hard=hard_codes,
                disclosure_state=REPORT_BLOCKED, unresolved=accounted_omissions,
            ),
        }

    if str(gate_status or "") != "FAIL":
        return blocked("gate_not_failed")
    if report is not None and not isinstance(report, dict):
        return blocked("malformed_report")
    if not isinstance(issues, list) or not issues or not codes:
        return blocked("missing_structured_blocking_issues")
    if len(codes) != len({str(i.get("code")) for i in issues if isinstance(i, dict)}):
        return blocked("malformed_blocking_issue")
    if hard_codes or any(
        not isinstance(issue, dict) or issue.get("code") not in PARTIAL_ELIGIBLE_CODES
        for issue in issues
    ):
        return blocked("hard_or_unknown_blocking_code")
    if not isinstance(omissions, list) or not omissions:
        return blocked("missing_structured_critical_omissions")
    totals = audit.get("totals") or {}
    lot_coverage = audit.get("lot_coverage") or {}
    lot_structure = (report or {}).get("lot_structure") or {}
    fact_coverage = audit.get("fact_coverage") or []
    if (
        not isinstance(totals, dict)
        or not isinstance(lot_coverage, dict)
        or not isinstance(lot_structure, dict)
        or not isinstance(fact_coverage, list)
    ):
        return blocked("malformed_structured_evidence")
    try:
        contradicted = int(totals.get("contradicted") or 0)
        unresolved_conflicts = int(lot_coverage.get("unresolved_conflicts") or 0)
    except (TypeError, ValueError):
        return blocked("malformed_structured_evidence")
    if contradicted > 0:
        return blocked("contradicted_fact_present")
    if unresolved_conflicts > 0:
        return blocked("unresolved_scope_conflict")

    report_lot = lot_structure.get("selected_lot")
    audit_lot = lot_coverage.get("lot_id")
    declared_lots = {
        str(value) for value in (lot_id, audit_lot, report_lot)
        if value not in (None, "")
    }
    if len(declared_lots) > 1:
        return blocked("ambiguous_lot_scope")

    facts = {
        str(fact.get("fact_id")): fact
        for fact in fact_coverage
        if isinstance(fact, dict) and fact.get("fact_id")
    }
    omissions_by_id = {
        str(item.get("fact_id")): item
        for item in omissions
        if isinstance(item, dict) and item.get("fact_id")
    }
    if len(omissions_by_id) != len(omissions):
        return blocked("duplicate_or_malformed_critical_omission")
    issue_by_id: Dict[str, Dict[str, Any]] = {}
    for issue in issues:
        fact_id = str(issue.get("fact_id") or "")
        if not fact_id or fact_id not in omissions_by_id:
            return blocked("blocking_issue_without_structured_omission")
        if fact_id in issue_by_id:
            return blocked("duplicate_blocking_issue")
        issue_by_id[fact_id] = issue
    if set(issue_by_id) != set(omissions_by_id):
        return blocked("unaccounted_critical_omission")

    scope_lot = str(
        lot_id or (audit.get("lot_coverage") or {}).get("lot_id") or report_lot or ""
    )
    for fact_id in sorted(omissions_by_id):
        omission = omissions_by_id[fact_id]
        issue = issue_by_id[fact_id]
        code = str(issue.get("code"))
        if omission.get("role_conflict") or omission.get("confirmation_roles"):
            return blocked("money_role_ambiguity")
        if str(omission.get("severity") or "") != "critical":
            return blocked("noncritical_omission_shape")
        if str(omission.get("match_status") or "") not in {"MISSING", "missing"}:
            return blocked("not_a_genuine_omission")
        source = facts.get(fact_id) or {}
        if not isinstance(omission.get("evidence_pages") or [], list):
            return blocked("malformed_evidence_pages")
        if not isinstance(omission.get("source_fact_ids") or [], list):
            return blocked("malformed_source_fact_ids")
        if not isinstance(source.get("source_fact_ids") or [], list):
            return blocked("malformed_source_fact_ids")
        if not isinstance(omission.get("bene_ids") or [], list):
            return blocked("malformed_bene_scope")
        if omission.get("bene_id") not in (None, "") and omission.get("bene_ids"):
            if str(omission.get("bene_id")) not in {
                str(value) for value in omission.get("bene_ids")
            }:
                return blocked("ambiguous_bene_scope")
        category = str(omission.get("category") or "").strip().lower()
        role = str(omission.get("role") or "").strip()
        if code == "MISSING_IMPORTANT_MONEY":
            if category not in {"money", "sale_terms"} or role not in _MONEY_ROLE_LABELS_IT:
                return blocked("malformed_monetary_omission")
        elif category in {"money", "sale_terms"}:
            return blocked("unclassified_monetary_omission")
        pages = _pages(omission.get("evidence_pages"))
        source_ids = _strings(
            [fact_id]
            + list(source.get("source_fact_ids") or [])
            + list(omission.get("source_fact_ids") or [])
        )
        provenance = {
            key: source.get(key)
            for key in ("source", "source_stage", "applicability", "projection_reason")
            if source.get(key) not in (None, "")
        }
        unresolved.append({
            "field": _fact_field(omission),
            "category": str(omission.get("category") or "critical_fact"),
            "lot_scope": scope_lot or None,
            "bene_scope": omission.get("bene_id") or omission.get("bene_ids"),
            "reason_code": code,
            "severity": "critical",
            "why_unresolved": str(omission.get("reason") or issue.get("detail") or ""),
            "source_pages": pages,
            "source_fact_ids": source_ids,
            "source_provenance": provenance,
            "evidence_available": bool(pages or source_ids),
            "readiness_effect": "TECHNICAL_REVIEW_REQUIRED",
            "professional_verification_required": True,
            "monetary_role": omission.get("role") if code == "MISSING_IMPORTANT_MONEY" else None,
        })

    return {
        "eligible": True,
        "reason": "omission_only",
        "disclosure_state": PARTIAL_REPORT_AVAILABLE,
        "unresolved_fields": unresolved,
        "accounting": _accounting(
            gate_status=str(gate_status), codes=codes, eligible=eligible_codes,
            hard=hard_codes, disclosure_state=PARTIAL_REPORT_AVAILABLE,
            unresolved=unresolved,
        ),
    }


def customer_unresolved_fields(items: Any) -> List[Dict[str, Any]]:
    """Map internal unresolved metadata through the Italian display boundary."""
    out: List[Dict[str, Any]] = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        category = str(item.get("category") or "")
        role = str(item.get("monetary_role") or "")
        reason_label = _REASON_LABELS_IT.get(
            str(item.get("reason_code") or ""), "Verifica professionale necessaria"
        )
        out.append({
            "field_label": _CATEGORY_LABELS_IT.get(category, "Dato essenziale"),
            "lot_scope": item.get("lot_scope"),
            "bene_scope": item.get("bene_scope"),
            "reason_label": reason_label,
            "severity_label": "Critico — verifica necessaria",
            "why_unresolved": reason_label,
            "source_pages": _pages(item.get("source_pages")),
            "source_fact_ids": _strings(item.get("source_fact_ids")),
            "source_provenance_label": (
                "Provenienza tracciata nei fatti riconciliati"
                if item.get("source_provenance") else None
            ),
            "evidence_available": bool(item.get("evidence_available")),
            "readiness_effect_label": "Report non pronto: verifica tecnica richiesta",
            "professional_verification_required": True,
            **({"monetary_role_label": _MONEY_ROLE_LABELS_IT.get(role, "Importo da verificare")} if role else {}),
        })
    return out


def build_partial_projection(
    *, report: Dict[str, Any], canonical_verdict: Dict[str, Any],
    quality_report: Dict[str, Any], coverage_audit: Dict[str, Any],
    gate_status: Any = "FAIL", lot_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Create an ephemeral partial projection from trustworthy artifacts.

    The CanonicalVerdict is refined against the underlying blocked readiness
    before the disclosure overlay is selected. This function performs no I/O.
    """
    from . import customer_report, decision_model, feature_flags, verdict_model

    if not feature_flags.partial_lot_reports_enabled():
        return None
    canonical = verdict_model.try_validate_verdict(canonical_verdict)
    if canonical is None or canonical.get("scope") != "lot":
        return None
    if not isinstance(report, dict) or not report.get("schema_version"):
        return None
    decision = classify_partial_eligibility(
        gate_status=gate_status,
        quality_report=quality_report,
        coverage_audit=coverage_audit,
        report=report,
        lot_id=lot_id,
    )
    if not decision.get("eligible"):
        return None

    # Evaluate exactly the same facts under the pre-disclosure blocked state.
    # PARTIAL vs BLOCKED therefore cannot change semantic severity/readiness.
    blocked_report = {
        **report,
        "report_status": "NEEDS_MANUAL_REVIEW",
        "canonical_verdict": canonical,
    }
    try:
        model = decision_model.build_decision_model(
            blocked_report, canonical_enabled=True
        )
        blocked_canonical = copy.deepcopy(canonical)
        blocked_canonical["report_status"] = "NEEDS_MANUAL_REVIEW"
        refined = verdict_model.refine_for_runtime(
            blocked_canonical,
            report_status="NEEDS_MANUAL_REVIEW",
            readiness=model.get("readiness") or {},
            open_checks=bool(
                ((model.get("sections") or {}).get("verifiche") or {}).get("items")
            ),
        )
    except (ValueError, KeyError, TypeError):
        return None
    overlay = customer_report.render_partial_report(
        report,
        decision.get("unresolved_fields") or [],
        decision.get("accounting") or {},
    )
    overlay["canonical_verdict"] = refined
    return {"report": overlay, "canonical_verdict": refined, "decision": decision}
