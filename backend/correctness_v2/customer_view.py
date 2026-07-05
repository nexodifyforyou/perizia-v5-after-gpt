"""
Customer-safe projection of ``customer_report.json`` for Correctness Mode v2.

The persisted ``customer_report.json`` artifact is a rich audit contract that
mixes customer facts with admin/debug machinery (quality control table, raw
evidence keys, artifact sources, internal validator flags, pipeline metadata).
It is served verbatim only by the admin-only artifact routes.

This module is the SERVER-SIDE source of truth for what a normal customer /
tester may see. ``sanitize_customer_report`` strips every admin/debug field and
keeps only presentable customer content, plus a derived executive decision box.

HARD RULES:
  * No new facts. Every retained value already exists in the input report.
  * No admin/debug leakage: quality_control, admin_evidence_index, raw
    evidence_index, sections_meta, internal manual-review flags, artifact
    paths and market/context money buckets are all removed.
  * Fail-closed: only REPORT_READY / LOT_SELECTION_REQUIRED reports are ever
    considered customer-safe; anything else yields ``available = False``.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict, List, Optional

# Report statuses a customer may see. Manual-review / validation-failed / any
# pipeline-failure status is never surfaced to a customer.
CUSTOMER_SAFE_STATUSES = frozenset({"REPORT_READY", "LOT_SELECTION_REQUIRED"})

# Money buckets a customer sees. market_comparatives / context_values are
# background/context values that read like buyer costs and are dropped here.
_CUSTOMER_MONEY_KEYS = (
    "valuation_chain",
    "auction_terms",
    "buyer_side_costs",
    "procedure_cancelled_formalities",
    "uncertain_money",
)

# Top-level keys that must NEVER reach the customer projection.
_ADMIN_ONLY_KEYS = frozenset(
    {
        "_saved_at",
        "evidence_index",
        "admin_evidence_index",
        "quality_control",
        "sections_meta",
        "surfaces_section",
        "manual_review_flags",
    }
)

# Customer-facing report status labels (never the raw internal codes).
_STATUS_LABELS = {
    "REPORT_READY": "Report pronto",
    "LOT_SELECTION_REQUIRED": "Selezione del lotto richiesta",
}

_DECISION_ATTENZIONE = "attenzione"
_DECISION_DA_VERIFICARE = "da_verificare"
_DECISION_PRONTO = "pronto_con_avvertenze"

_DECISION_LABELS = {
    _DECISION_ATTENZIONE: "Attenzione",
    _DECISION_DA_VERIFICARE: "Da verificare",
    _DECISION_PRONTO: "Pronto con avvertenze",
}


def _norm(text: Any) -> str:
    stripped = "".join(
        c
        for c in unicodedata.normalize("NFKD", str(text or ""))
        if not unicodedata.combining(c)
    )
    return stripped.lower().strip()


def _has_keyword(text: Any, *keywords: str) -> bool:
    low = _norm(text)
    return any(k in low for k in keywords)


# ---------------------------------------------------------------------------
# Executive decision box
# ---------------------------------------------------------------------------
def _occupancy_is_occupied(report: Dict[str, Any]) -> bool:
    oc = report.get("occupancy_section") or {}
    return _norm(oc.get("status")) == "occupato" or _has_keyword(
        oc.get("status_label"), "occupat"
    )


def _decision_drivers(report: Dict[str, Any]) -> List[str]:
    """Customer-language reasons behind the decision. No internal codes."""
    drivers: List[str] = []

    def add(driver: str) -> None:
        if driver and driver not in drivers:
            drivers.append(driver)

    oc = report.get("occupancy_section") or {}
    if _occupancy_is_occupied(report):
        add("immobile occupato")
    if oc.get("opponibility") or oc.get("registration_dates") or oc.get("expiry_dates"):
        add("situazione locativa e opponibilità del titolo da verificare")
    for risk in oc.get("risks") or []:
        if _has_keyword(risk, "opponib", "titolo", "senza titolo"):
            add("situazione locativa e opponibilità del titolo da verificare")

    for section in report.get("risk_sections") or []:
        for item in section.get("items") or []:
            summary = f"{item.get('area') or ''} {item.get('summary') or ''}"
            if _has_keyword(summary, "collabente", "crollo", "strutt", "pericol"):
                add("condizioni strutturali critiche")
            if _has_keyword(summary, "amianto", "fibrocement", "fibro-cement", "eternit"):
                add("possibile presenza di materiali pericolosi")

    for item in report.get("compliance_section") or []:
        area = f"{item.get('area') or ''} {item.get('notes') or ''}"
        cls = _norm(item.get("classification"))
        if _has_keyword(area, "agibil") and _has_keyword(area, "non", "manca"):
            add("agibilità/abitabilità da verificare")
        if _has_keyword(area, "ape", "certificaz", "conformit") and cls in {
            "non_conforming",
            "not_regularizable",
            "uncertain",
        }:
            add("certificazioni e conformità tecniche mancanti o da verificare")
        if cls in {"regularizable", "non_conforming", "not_regularizable"}:
            add("regolarizzazioni tecniche o catastali da valutare")

    comp = report.get("compliance_section") or []
    if any(_norm(i.get("classification")) == "regularizable" for i in comp):
        add("costi di regolarizzazione a carico dell'acquirente da valutare")

    money = report.get("money_sections") or {}
    if money.get("uncertain_money"):
        add("importi il cui ruolo non è chiaro, da verificare")
    if money.get("buyer_side_costs"):
        add("costi a carico dell'acquirente da verificare")

    lot = report.get("lot_structure") or {}
    try:
        if int(lot.get("bene_count") or 0) > 1:
            add("più beni nel lotto con situazioni tecniche differenti")
    except (TypeError, ValueError):
        pass

    return drivers


def _decision_level(report: Dict[str, Any]) -> str:
    critical = False
    for section in report.get("risk_sections") or []:
        if _norm(section.get("section_id")) == "criticita":
            critical = True
        for item in section.get("items") or []:
            if _norm(item.get("severity")) == "grave" or item.get("blocks_saleability"):
                critical = True
            if _has_keyword(
                f"{item.get('area') or ''} {item.get('summary') or ''}",
                "collabente",
                "crollo",
                "amianto",
                "fibrocement",
                "pericol",
            ):
                critical = True
    for item in report.get("compliance_section") or []:
        if _norm(item.get("classification")) in {"non_conforming", "not_regularizable"}:
            critical = True
        if item.get("blocks_saleability"):
            critical = True
    if critical:
        return _DECISION_ATTENZIONE

    money = report.get("money_sections") or {}
    needs_check = (
        _occupancy_is_occupied(report)
        or bool(money.get("uncertain_money"))
        or bool(money.get("buyer_side_costs"))
        or any(
            _norm(i.get("classification")) in {"regularizable", "uncertain"}
            for i in report.get("compliance_section") or []
        )
        or any(
            (section.get("items") or [])
            for section in report.get("risk_sections") or []
        )
    )
    if needs_check:
        return _DECISION_DA_VERIFICARE
    return _DECISION_PRONTO


def derive_decision(report: Dict[str, Any]) -> Dict[str, Any]:
    """Executive decision box derived from risks/occupancy/compliance/money.

    Returns {level, label, headline, reason, drivers}. Never exposes internal
    codes; the reason is a single customer-language sentence.
    """
    level = _decision_level(report)
    drivers = _decision_drivers(report)
    label = _DECISION_LABELS[level]

    if level == _DECISION_ATTENZIONE:
        headline = "Attenzione: sono presenti criticità che richiedono verifiche approfondite prima di ogni decisione."
    elif level == _DECISION_DA_VERIFICARE:
        headline = "Da verificare: prima di procedere è necessario controllare alcuni aspetti indicati nella perizia."
    else:
        headline = "Pronto con avvertenze: nessuna criticità bloccante, restano alcune verifiche prudenziali."

    if drivers:
        top = drivers[:3]
        reason = "Motivo principale: " + "; ".join(top) + "."
    else:
        reason = "Verificare comunque i punti segnalati nella perizia con un professionista di fiducia."

    return {
        "level": level,
        "label": label,
        "headline": headline,
        "reason": reason,
        "drivers": drivers[:5],
    }


# ---------------------------------------------------------------------------
# Sanitization
# ---------------------------------------------------------------------------
def _customer_lot_structure(lot: Any) -> Dict[str, Any]:
    """Keep only human lot facts; drop machine flags (multi_lot, bene_ids...)."""
    lot = lot if isinstance(lot, dict) else {}
    out: Dict[str, Any] = {}
    for key in ("selected_lot", "lot_count", "bene_count"):
        if lot.get(key) not in (None, "", []):
            out[key] = lot.get(key)
    return out


def _customer_money_sections(money: Any) -> Dict[str, List[Dict[str, Any]]]:
    money = money if isinstance(money, dict) else {}
    out: Dict[str, List[Dict[str, Any]]] = {}
    for key in _CUSTOMER_MONEY_KEYS:
        rows = money.get(key)
        out[key] = list(rows) if isinstance(rows, list) else []
    return out


def _customer_lot_selection(selection: Any) -> Dict[str, Any]:
    """Lot selector for the customer: no confidence, no debug money buckets."""
    selection = selection if isinstance(selection, dict) else {}
    lots: List[Dict[str, Any]] = []
    for lot in selection.get("lots") or []:
        lot = lot if isinstance(lot, dict) else {}
        lots.append(
            {
                "lot_id": lot.get("lot_id"),
                "label": lot.get("label"),
                "address": lot.get("address"),
                "property_type": lot.get("property_type"),
                "ownership_right": lot.get("ownership_right"),
                "occupancy_summary": lot.get("occupancy_summary"),
                "money_summary": list(lot.get("money_summary") or []),
                "evidence_pages": list(lot.get("evidence_pages") or []),
            }
        )
    return {"message": selection.get("message"), "lots": lots}


def is_customer_safe(report: Optional[Dict[str, Any]], job: Optional[Dict[str, Any]] = None) -> bool:
    """A report is customer-safe only if its status is REPORT_READY /
    LOT_SELECTION_REQUIRED and the job (when supplied) flags it safe."""
    if not isinstance(report, dict):
        return False
    if str(report.get("report_status")) not in CUSTOMER_SAFE_STATUSES:
        return False
    if isinstance(job, dict) and job.get("safe_to_show_customer") is False:
        # Explicit false from the pipeline is authoritative; missing key (older
        # artifacts) falls back to the status check above.
        if "safe_to_show_customer" in job:
            return False
    return True


def sanitize_customer_report(
    report: Dict[str, Any], job: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Return the customer-safe projection of a full ``customer_report`` dict.

    Strips every admin/debug field, keeps presentable customer content, and adds
    a derived executive ``decision`` box. The input dict is never mutated.
    """
    report = report if isinstance(report, dict) else {}
    decision = derive_decision(report)

    status = str(report.get("report_status") or "")
    out: Dict[str, Any] = {
        "schema_version": report.get("schema_version"),
        "analysis_id": report.get("analysis_id"),
        "job_id": report.get("job_id"),
        "report_status": status,
        "report_status_label": _STATUS_LABELS.get(status, "Report"),
        "title": report.get("title"),
        "subtitle": report.get("subtitle"),
        "decision": decision,
        "case_identity": dict(report.get("case_identity") or {}),
        "lot_structure": _customer_lot_structure(report.get("lot_structure")),
        "executive_summary": list(report.get("executive_summary") or []),
        "key_facts": list(report.get("key_facts") or []),
        "risk_sections": list(report.get("risk_sections") or []),
        "money_sections": _customer_money_sections(report.get("money_sections")),
        "beni_sections": list(report.get("beni_sections") or []),
        "occupancy_section": dict(report.get("occupancy_section") or {}),
        "compliance_section": list(report.get("compliance_section") or []),
        "formalities_section": list(report.get("formalities_section") or []),
        "buyer_checklist": list(report.get("buyer_checklist") or []),
        "customer_evidence_index": list(report.get("customer_evidence_index") or []),
        "disclaimer": report.get("disclaimer"),
    }

    if status == "LOT_SELECTION_REQUIRED" and report.get("lot_selection"):
        out["lot_selection"] = _customer_lot_selection(report.get("lot_selection"))

    # Defense in depth: guarantee no admin-only key ever slips through, even if
    # the projection above is extended incautiously later.
    for key in _ADMIN_ONLY_KEYS:
        out.pop(key, None)

    return out
