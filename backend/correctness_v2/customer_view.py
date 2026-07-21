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
from typing import Any, Dict, List, Optional, Sequence

from . import decision_model

# Report statuses a customer may see. Manual-review / validation-failed / any
# pipeline-failure status is never surfaced to a customer.
CUSTOMER_SAFE_STATUSES = frozenset(
    {
        "REPORT_READY",
        "LOT_SELECTION_REQUIRED",
        "MONEY_CONFIRMATION_REQUIRED",
        "DOCUMENT_NOT_READABLE",
    }
)

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
    "MONEY_CONFIRMATION_REQUIRED": "Conferma importi richiesta",
    "DOCUMENT_NOT_READABLE": "Perizia non leggibile",
}

_DECISION_ATTENZIONE = "attenzione"
_DECISION_DA_VERIFICARE = "da_verificare"
_DECISION_PRONTO = "pronto_con_avvertenze"
_DECISION_NON_LEGGIBILE = "non_leggibile"

_DECISION_LABELS = {
    _DECISION_ATTENZIONE: "Attenzione",
    _DECISION_DA_VERIFICARE: "Da verificare",
    _DECISION_PRONTO: "Pronto con avvertenze",
    _DECISION_NON_LEGGIBILE: "Non leggibile",
}

# Old cached reports may contain validator diagnostics appended to otherwise
# source-backed customer text.  These markers describe an internal
# classification downgrade, not a fact from the appraisal.  Match only the
# validator's known enum vocabulary so ordinary source phrases such as
# "area classificata 'AREC 2'" remain untouched.
_INTERNAL_DIAGNOSTIC_MARKER_RE = re.compile(
    r"""
    (?:
        \bdeclassat[aoei]\s+a\s+['\"]?
        (?:uncertain|conforming|non_conforming|regularizable|not_regularizable)
        |
        (?:\bconformit[aà]\s+['\"][^'\"]+['\"]\s*:\s*)?
        \bclassificat[aoei]\s+['\"]
        (?:uncertain|conforming|non_conforming|regularizable|not_regularizable)
        ['\"]
        |
        \bstato\s+impostato\s+a\s+['\"]
        (?:uncertain|conforming|non_conforming|regularizable|not_regularizable)
        ['\"]
        |
        \brichiesta\s+verifica\s+manuale\b
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Customer prose must never explain limitations of our implementation.  Match
# limitation language, not ordinary appraisal vocabulary such as "schema
# catastale" or an urban classification code.  The regex also covers the
# explicit English implementation tokens found in some legacy cached rows.
_INTERNAL_LIMITATION_MARKER_RE = re.compile(
    r"""
    (?:
        \b(?:lo\s+|il\s+|quest[oa]\s+)?schema\s+(?:intern[oa]\s+)?
        (?:non\s+(?:preved\w*|support\w*|consent\w*)|limit\w*)
        |
        \b(?:il\s+)?modello\s+dati\s+non\s+
        (?:preved\w*|support\w*|consent\w*|rappresent\w*)
        |
        \b(?:il\s+)?formato\s+interno\s+non\s+
        (?:
            rappresent\w*
            |
            (?:preved\w*|support\w*|consent\w*)
            (?=[^.]{0,100}\b(?:
                camp\w*\s+(?:separat\w*|dedicat\w*)|attribut\w*|dato|dati|
                struttur\w*\s+dati|voce\s+distint\w*|sezione\s+dedicat\w*
            )\b)
        )
        |
        \b(?:limit\w*|vincol\w*)\s+(?:intern\w*\s+)?
        (?:(?:del|dello|della|di)\s+)?
        (?:schema|parser|backend(?:[-_\s]?field)?|database(?:[-_\s]?field)?)\b
        |
        \b(?:il\s+|lo\s+|un\s+|una\s+)?
        (?:parser|backend(?:[-_\s]?field)?|database(?:[-_\s]?field)?)\b
        |
        \b(?:campo|modello)\s+intern[oa]\b
        |
        \bnormalizzazion\w*\s+(?:intern\w*|implementativ\w*)\b
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)


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
    # Non-analyzable document (images / not text-extractable): the decision box is
    # the whole customer message — what happened + upload a readable PDF.
    if str(report.get("report_status") or "") == "DOCUMENT_NOT_READABLE":
        steps = [str(s) for s in (report.get("next_steps") or []) if str(s).strip()]
        headline = str(
            report.get("reason_human")
            or "Non è stato possibile leggere la perizia caricata."
        )
        reason = str(
            report.get("troubleshoot_message")
            or (steps[0] if steps else
                "Caricare un PDF leggibile con testo selezionabile e riprovare.")
        )
        return {
            "level": _DECISION_NON_LEGGIBILE,
            "label": _DECISION_LABELS[_DECISION_NON_LEGGIBILE],
            "headline": headline,
            "reason": reason,
            "drivers": steps[:5],
        }

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
        clean_rows = list(rows) if isinstance(rows, list) else []
        if key == "uncertain_money":
            # Explicit per-Bene appraisal values belong to the decision model's
            # composition section, not the legacy "uncertain" bucket whose old
            # reason exposed an internal schema limitation.
            clean_rows = [
                row for row in clean_rows
                if not (
                    isinstance(row, dict)
                    and "valore di stima bene" in _norm(row.get("label"))
                )
            ]
        out[key] = clean_rows
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


def _customer_money_confirmation(mc: Any) -> Dict[str, Any]:
    """Money-confirmation prompt for the customer: only the presentable fields.

    Every value already exists in the report's money_confirmation block; no
    admin/evidence-index data is exposed. ``ambiguity_id``/``option_id`` are the
    opaque keys the customer's answer is submitted with."""
    mc = mc if isinstance(mc, dict) else {}
    ambiguities: List[Dict[str, Any]] = []
    for amb in mc.get("ambiguities") or []:
        amb = amb if isinstance(amb, dict) else {}
        options = [
            {"option_id": o.get("option_id"), "label": o.get("label")}
            for o in amb.get("options") or []
            if isinstance(o, dict) and o.get("option_id")
        ]
        ambiguities.append(
            {
                "ambiguity_id": amb.get("ambiguity_id"),
                "amount_display": amb.get("amount_display"),
                "page": amb.get("page"),
                "evidence_pages": list(amb.get("evidence_pages") or []),
                "excerpt": amb.get("excerpt"),
                "question": amb.get("question"),
                "options": options,
            }
        )
    return {"message": mc.get("message"), "ambiguities": ambiguities}


def _strip_internal_customer_text(value: str) -> str:
    """Retain source prose before an internal diagnostic/limitation suffix."""
    markers = [
        marker
        for marker in (
            _INTERNAL_DIAGNOSTIC_MARKER_RE.search(value),
            _INTERNAL_LIMITATION_MARKER_RE.search(value),
        )
        if marker is not None
    ]
    if not markers:
        return value
    marker = min(markers, key=lambda match: match.start())
    return value[: marker.start()].rstrip(" \t\r\n;,:\u2013\u2014-")


def _customer_content(value: Any) -> Any:
    """Deep-copy customer content while removing old validator diagnostics.

    Some reports created before the customer projection was hardened appended
    an internal downgrade explanation to a valid source-backed sentence.  The
    customer should see the sentence from the appraisal, never the validator's
    enum or manual-review explanation.  Truncating at the first diagnostic
    marker is intentionally fail-closed because the validator always appends
    that material as a suffix.
    """
    if isinstance(value, str):
        return _strip_internal_customer_text(value)
    if isinstance(value, dict):
        # Raw English classifier enums are an implementation detail. Customer
        # cards already carry an Italian status_label and the read-time decision
        # model derives its status from the unprojected report before this copy.
        return {
            key: _customer_content(item)
            for key, item in value.items()
            if key != "classification"
        }
    if isinstance(value, list):
        return [_customer_content(item) for item in value]
    if isinstance(value, tuple):
        return [_customer_content(item) for item in value]
    return value


def _customer_text_content(value: Any) -> Any:
    """Recursively scrub implementation prose while preserving field shape."""
    if isinstance(value, str):
        return _strip_internal_customer_text(value)
    if isinstance(value, dict):
        return {key: _customer_text_content(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_customer_text_content(item) for item in value]
    if isinstance(value, tuple):
        return [_customer_text_content(item) for item in value]
    return value


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
    report: Dict[str, Any],
    job: Optional[Dict[str, Any]] = None,
    confirmations: Sequence[Dict[str, Any]] = (),
    cached_pages: Sequence[Dict[str, Any]] = (),
) -> Dict[str, Any]:
    """Return the customer-safe projection of a full ``customer_report`` dict.

    Strips every admin/debug field, keeps presentable customer content, and adds
    a derived executive ``decision`` box plus the read-time ``decision_model``
    (§C). ``confirmations`` is the list of persisted user confirmations for this
    analysis (from MongoDB), joined into the decision model at read time. The
    input dict is never mutated.
    """
    report = report if isinstance(report, dict) else {}
    if cached_pages:
        # Ephemeral read-time evidence only.  Raw cached pages are never copied
        # to the customer projection or persisted back into the artifact.
        report = {**report, "_cached_input_pages": list(cached_pages)}
    decision = derive_decision(report)

    status = str(report.get("report_status") or "")
    out: Dict[str, Any] = {
        "schema_version": report.get("schema_version"),
        "analysis_id": report.get("analysis_id"),
        "job_id": report.get("job_id"),
        "report_status": status,
        "report_status_label": _STATUS_LABELS.get(status, "Report"),
        "title": _customer_content(report.get("title")),
        "subtitle": _customer_content(report.get("subtitle")),
        "decision": _customer_text_content(decision),
        "case_identity": _customer_content(report.get("case_identity") or {}),
        "lot_structure": _customer_lot_structure(report.get("lot_structure")),
        "executive_summary": _customer_content(report.get("executive_summary") or []),
        "key_facts": _customer_content(report.get("key_facts") or []),
        "risk_sections": _customer_content(report.get("risk_sections") or []),
        "money_sections": _customer_content(
            _customer_money_sections(report.get("money_sections"))
        ),
        "beni_sections": _customer_content(report.get("beni_sections") or []),
        "occupancy_section": _customer_content(report.get("occupancy_section") or {}),
        "compliance_section": _customer_content(report.get("compliance_section") or []),
        "formalities_section": _customer_content(report.get("formalities_section") or []),
        "buyer_checklist": _customer_content(report.get("buyer_checklist") or []),
        # The legacy evidence index is unvalidated and can contain a conclusion-
        # evidence mismatch.  Only decision_model.sections.fonti, built by the
        # fail-closed evidence validator, is customer-visible.
        "disclaimer": _customer_content(report.get("disclaimer")),
    }

    # Read-time customer decision model (§C). Built from the FULL stored report
    # (before admin keys are stripped above is irrelevant — the builder reads only
    # customer-safe fields) with user confirmations joined. Pure, no OpenAI.
    out["decision_model"] = decision_model.build_decision_model(report, confirmations)

    if status == "LOT_SELECTION_REQUIRED" and report.get("lot_selection"):
        out["lot_selection"] = _customer_lot_selection(report.get("lot_selection"))

    if status == "MONEY_CONFIRMATION_REQUIRED" and report.get("money_confirmation"):
        out["money_confirmation"] = _customer_money_confirmation(
            report.get("money_confirmation")
        )

    # Covers derived/optional sections too (notably uncertain-money reasons in
    # the decision model) without altering their contract shape.
    out = _customer_text_content(out)

    # Defense in depth: guarantee no admin-only key ever slips through, even if
    # the projection above is extended incautiously later.
    for key in _ADMIN_ONLY_KEYS:
        out.pop(key, None)

    return out
