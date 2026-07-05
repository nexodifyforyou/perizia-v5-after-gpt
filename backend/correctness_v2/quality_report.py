"""
Quality certificate + customer satisfaction scorecard for Correctness Mode v2.

Builds:
  * quality_standard_report.json  — internal/admin certificate proving (or
    denying) that the rendered report is fit for customer use.
  * customer_satisfaction_scorecard.json — would a real user trust and
    understand this report?

Both are deterministic functions of the persisted artifacts + coverage audit.
They never invent facts; every blocking issue carries a code and description.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set, Tuple

from . import doc_signals, lots as lots_mod
from .coverage_audit import (
    STATUS_FAIL,
    STATUS_PASS,
    STATUS_WARNING,
)
from .doc_signals import norm_text

QUALITY_REPORT_SCHEMA_VERSION = "cv2.quality_standard_report.v1"
SCORECARD_SCHEMA_VERSION = "cv2.customer_satisfaction_scorecard.v1"

# Near-exact amount identity (see coverage_audit): grounding and confusion
# checks must never confuse two adjacent-but-different amounts.
_MONEY_ABS_TOL = 0.011
_MONEY_REL_TOL = 0.0


def _approx_equal(a: float, b: float) -> bool:
    tol = max(_MONEY_ABS_TOL, _MONEY_REL_TOL * max(abs(a), abs(b)))
    return abs(a - b) <= tol


# ---------------------------------------------------------------------------
# Deterministic blocking checks (automatic FAIL conditions)
# ---------------------------------------------------------------------------
def _doc_amounts(pages: List[Dict[str, Any]]) -> List[float]:
    amounts: List[float] = []
    for page in pages or []:
        for amount, _s, _e in doc_signals.amounts_in_text(page.get("text")):
            amounts.append(amount)
    return amounts


def _amount_in_doc(amount: Any, doc_amounts: List[float]) -> bool:
    try:
        value = float(amount)
    except (TypeError, ValueError):
        return False
    return any(_approx_equal(value, d) for d in doc_amounts)


def _check_blocking(
    *,
    coverage_audit: Dict[str, Any],
    contract: Optional[Dict[str, Any]],
    customer_report: Dict[str, Any],
    validator_report: Optional[Dict[str, Any]],
    lot_report: Optional[Dict[str, Any]],
    pages: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """All automatic-FAIL conditions. Generic, deterministic."""
    blocking: List[Dict[str, Any]] = []
    contract = contract or {}
    doc_amounts = _doc_amounts(pages)

    def block(code: str, detail: str, **extra: Any) -> None:
        blocking.append({"code": code, "detail": detail, **extra})

    # 1) Critical silent omissions (incl. important money missing). An amount
    # present in the report under the WRONG money role is its own violation:
    # role-swapped money must fail the gate, never pass as "amount found".
    for omission in coverage_audit.get("critical_omissions") or []:
        if omission.get("role_conflict"):
            code = "MONEY_ROLE_MISMATCH"
        elif omission.get("category") in ("money", "sale_terms"):
            code = "MISSING_IMPORTANT_MONEY"
        else:
            code = "CRITICAL_FACT_MISSING"
        block(
            code,
            f"{omission.get('document_fact')} (pagine {omission.get('evidence_pages')})",
            fact_id=omission.get("fact_id"),
        )

    # 2) Buyer-side cost invented (amount with no support in document text).
    for row in (customer_report.get("money_sections") or {}).get("buyer_side_costs") or []:
        amount = row.get("amount")
        if amount and not _amount_in_doc(amount, doc_amounts):
            block(
                "INVENTED_BUYER_COST",
                f"Costo a carico acquirente '{row.get('label')}' ({amount}) senza "
                "riscontro nel testo del documento.",
            )

    # 3) Procedure-cancelled formalities treated as buyer debt.
    buyer_amounts = [
        row.get("amount")
        for row in (customer_report.get("money_sections") or {}).get("buyer_side_costs") or []
        if row.get("amount")
    ]
    for form in contract.get("legal_formalities") or []:
        if not form.get("cancelled_by_procedure"):
            continue
        amount = form.get("amount")
        if amount and any(_approx_equal(float(amount), float(b)) for b in buyer_amounts):
            block(
                "PROCEDURE_FORMALITY_AS_BUYER_DEBT",
                f"La formalità '{form.get('type')}' è indicata come cancellata dalla "
                f"procedura ma il suo importo ({amount}) compare come costo a carico "
                "dell'acquirente.",
            )
        if form.get("buyer_burden") and form.get("cancelled_by_procedure"):
            block(
                "PROCEDURE_FORMALITY_AS_BUYER_DEBT",
                f"La formalità '{form.get('type')}' risulta sia a carico acquirente "
                "sia cancellata dalla procedura: contraddizione.",
            )

    # 4) Fake prezzo base (derived from judicial sale value without explicit text).
    signals = ((validator_report or {}).get("checks") or {}).get("money_signals") or {}
    base_explicit = bool(signals.get("base_price_explicit_text"))
    sale_value = None
    for row in contract.get("valuation_chain") or []:
        if "vendita giudiziaria" in norm_text(row.get("label")):
            sale_value = row.get("amount")
    for row in (customer_report.get("money_sections") or {}).get("auction_terms") or []:
        if "prezzo base" not in norm_text(row.get("label")):
            continue
        if row.get("source") == "shared_summary_projection":
            continue  # deterministic document text with its own explicit support
        if not base_explicit:
            detail = (
                f"Prezzo base ({row.get('amount')}) esposto senza riscontro testuale "
                "esplicito di 'prezzo base' nel documento."
            )
            if sale_value is not None and row.get("amount") is not None and _approx_equal(
                float(row["amount"]), float(sale_value)
            ):
                detail += " L'importo coincide con il valore di vendita giudiziaria."
            block("FAKE_PREZZO_BASE", detail)

    # 5) Lot/bene integrity. Bene checks only apply once a contract exists
    # (selection mode has no contract yet: beni belong to the per-lot analysis).
    lot_report = lot_report or {}
    expected_beni = {str(b) for b in lot_report.get("bene_ids") or []}
    contract_beni = {
        str(b) for b in (contract.get("lot_summary") or {}).get("bene_ids") or []
    }
    if contract and expected_beni and not expected_beni.issubset(contract_beni):
        lost = sorted(expected_beni - contract_beni)
        block(
            "BENE_LOST",
            f"Beni rilevati nel documento ma assenti dal contratto: {lost}.",
        )
    allowed_lots = {
        lots_mod.normalize_lot_token(x) or str(x)
        for x in (lot_report.get("lot_ids") or [])
    }
    selected = (contract.get("lot_summary") or {}).get("selected_lot")
    if selected is not None and str(customer_report.get("report_status")) == "REPORT_READY":
        selected_norm = lots_mod.normalize_lot_token(selected) or str(selected)
        mentioned: Set[str] = set()
        for section in ("executive_summary", "key_facts"):
            for item in customer_report.get(section) or []:
                text = " ".join(str(v) for v in item.values() if isinstance(v, str))
                for lid in lots_mod.lot_ids_in_text(text):
                    mentioned.add(lots_mod.normalize_lot_token(lid) or lid)
        foreign = {
            m for m in mentioned
            if m != selected_norm and allowed_lots and m in allowed_lots
        }
        # Mentioning a *different known lot* in the selected-lot report = mixing.
        if foreign and len(allowed_lots) > 1:
            block(
                "LOT_CONTAMINATION",
                f"Il report del lotto '{selected}' menziona altri lotti: {sorted(foreign)}.",
            )

    # 6) Compliance marked conforming without evidence.
    for section in customer_report.get("compliance_section") or []:
        if section.get("classification") == "conforming" and not section.get("evidence_pages"):
            block(
                "CONFORMING_WITHOUT_EVIDENCE",
                f"Area '{section.get('area')}' indicata come conforme senza pagine di evidenza.",
            )
    for card in contract.get("risk_cards") or []:
        if card.get("classification") == "conforming" and not card.get("evidence_pages"):
            block(
                "CONFORMING_WITHOUT_EVIDENCE",
                f"Scheda rischio '{card.get('area')}' conforme senza evidenza.",
            )

    # 7) Contradiction between report sections: same label, different amounts.
    label_amounts: Dict[str, Set[float]] = {}
    money_sections = customer_report.get("money_sections") or {}
    for sec_name in ("valuation_chain", "auction_terms", "buyer_side_costs"):
        for row in money_sections.get(sec_name) or []:
            label = norm_text(row.get("label"))
            amount = row.get("amount")
            if label and amount is not None:
                label_amounts.setdefault(label, set()).add(round(float(amount), 2))
    for fact in customer_report.get("key_facts") or []:
        label = norm_text(fact.get("label"))
        value = fact.get("value")
        if label and isinstance(value, (int, float)) and not isinstance(value, bool):
            label_amounts.setdefault(label, set()).add(round(float(value), 2))
    for label, amounts in label_amounts.items():
        distinct = sorted(amounts)
        if len(distinct) > 1 and not all(
            _approx_equal(distinct[0], other) for other in distinct[1:]
        ):
            block(
                "SECTION_CONTRADICTION",
                f"La voce '{label}' compare con importi diversi nel report: {distinct}.",
            )

    # 8) Customer evidence excerpts must be VERBATIM document text (whitespace-
    # normalized only). A non-verbatim "Estratto perizia" is an invented quote.
    page_texts_norm: Dict[int, str] = {}
    for page in pages or []:
        pnum = doc_signals.page_number(page)
        if pnum is not None:
            page_texts_norm[pnum] = doc_signals.normalize_ws(page.get("text")).lower()
    for entry in customer_report.get("customer_evidence_index") or []:
        excerpt = entry.get("perizia_excerpt")
        if not excerpt or entry.get("coverage_status") != "covered":
            continue
        page_text = page_texts_norm.get(entry.get("page"))
        if page_text is None:
            continue
        excerpt_norm = doc_signals.normalize_ws(excerpt).lower()
        if excerpt_norm and excerpt_norm not in page_text:
            block(
                "EXCERPT_NOT_VERBATIM",
                f"L'estratto per '{entry.get('topic')}' (pag. {entry.get('page')}) "
                "non è testo letterale della pagina indicata.",
            )

    # 9) Unsupported confirmed money (grounding): confirmed ANCHOR VALUES must
    # exist verbatim in the document text. Deduction rows may legitimately be
    # the perito's implied arithmetic (e.g. "55% di € 622.970"), already tied
    # together by the validator's chain check — those become warnings, not
    # blocks (handled in _check_warnings). Shared-summary projected rows carry
    # their own deterministic textual support from the shared pages.
    for sec_name in ("valuation_chain", "auction_terms"):
        for row in money_sections.get(sec_name) or []:
            amount = row.get("amount")
            if not amount:
                continue
            if row.get("source") == "shared_summary_projection":
                continue
            if row.get("kind") not in ("value", "auction_term"):
                continue
            if not _amount_in_doc(amount, doc_amounts):
                block(
                    "UNSUPPORTED_MONEY_CLAIM",
                    f"Importo confermato '{row.get('label')}' ({amount}) non trovato "
                    "nel testo estratto del documento.",
                )
    return blocking


# ---------------------------------------------------------------------------
# Warnings (non-blocking)
# ---------------------------------------------------------------------------
# Raw internal tokens that must never surface in customer-facing display text.
_FORBIDDEN_DISPLAY_TOKENS = (
    "analyst_warning", "validator_warning", "missing_or_uncertain",
    "uncertain_money", "technical_compliance[", "risk_classification[",
    "buyer-side", "buyer side", "regularizable", "not_regularizable",
    "non_conforming", "compliance_uncertain",
)
# Fields the frontend renders as visible text for customers.
# ("perizia_excerpt" is deliberately excluded: it is verbatim document text.)
_DISPLAY_FIELDS = {
    "label", "title", "subtitle", "text", "detail", "action", "status_label",
    "severity_label", "kind_label", "summary", "notes", "value", "note",
    "reason", "message", "area", "topic", "report_section",
}


def _scan_display_text(node: Any, path: str, hits: List[Dict[str, str]]) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            if key in ("quality_control",):
                continue
            if key in _DISPLAY_FIELDS and isinstance(value, str):
                low = value.lower()
                for token in _FORBIDDEN_DISPLAY_TOKENS:
                    if token in low:
                        hits.append({"path": f"{path}.{key}", "token": token, "text": value[:120]})
            else:
                _scan_display_text(value, f"{path}.{key}", hits)
    elif isinstance(node, list):
        for i, item in enumerate(node):
            _scan_display_text(item, f"{path}[{i}]", hits)


def _check_warnings(
    *,
    coverage_audit: Dict[str, Any],
    customer_report: Dict[str, Any],
    pages: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, str]]]:
    warnings: List[Dict[str, Any]] = []

    def warn(code: str, detail: str) -> None:
        warnings.append({"code": code, "detail": detail})

    # Derived (non-verbatim) deduction amounts: legitimate perito arithmetic,
    # but flagged so a human can see they are computed, not quoted.
    if pages is not None:
        doc_amounts = _doc_amounts(pages)
        for row in (customer_report.get("money_sections") or {}).get("valuation_chain") or []:
            amount = row.get("amount")
            if amount and row.get("kind") == "deduction" and not _amount_in_doc(amount, doc_amounts):
                warn(
                    "DERIVED_AMOUNT",
                    f"L'importo '{row.get('label')}' ({amount}) è derivato dai valori "
                    "dichiarati in perizia (non citato verbatim nel testo).",
                )

    for omission in coverage_audit.get("important_warnings") or []:
        if omission.get("role_conflict"):
            warn(
                "MONEY_ROLE_CONFLICT",
                f"{omission.get('document_fact')} (pagine {omission.get('evidence_pages')}) "
                f"— {omission.get('reason')}",
            )
        else:
            warn(
                "IMPORTANT_FACT_NOT_RENDERED",
                f"{omission.get('document_fact')} (pagine {omission.get('evidence_pages')})",
            )

    # Customer evidence entries without a safe verbatim excerpt: explicit
    # coverage warning (the entry already says "Estratto non disponibile...").
    for entry in customer_report.get("customer_evidence_index") or []:
        if entry.get("coverage_status") == "excerpt_missing":
            warn(
                "EVIDENCE_EXCERPT_MISSING",
                f"Nessun estratto verbatim trovato per '{entry.get('topic')}' "
                f"(pag. {entry.get('page')}): verificare manualmente la pagina.",
            )

    uncertain_rows = (customer_report.get("money_sections") or {}).get("uncertain_money") or []
    if len(uncertain_rows) > 6:
        warn(
            "LARGE_UNCERTAINTY_SECTION",
            f"{len(uncertain_rows)} importi da verificare: sezione incertezza ampia.",
        )
    flags = customer_report.get("manual_review_flags") or []
    if len(flags) > 25:
        warn("LARGE_MANUAL_REVIEW_SECTION", f"{len(flags)} punti da verificare.")

    # Evidence pages too broad on single MONEY facts (identity facts naturally
    # recur across headers/footers, so they are not flagged).
    for fact in customer_report.get("key_facts") or []:
        pages = fact.get("evidence_pages") or []
        value = fact.get("value")
        is_money = isinstance(value, (int, float)) and not isinstance(value, bool)
        if is_money and len(pages) > 8:
            warn(
                "EVIDENCE_TOO_BROAD",
                f"Il dato '{fact.get('label')}' cita {len(pages)} pagine di evidenza.",
            )

    label_hits: List[Dict[str, str]] = []
    _scan_display_text(customer_report, "customer_report", label_hits)
    for hit in label_hits[:10]:
        warn(
            "RAW_INTERNAL_LABEL_VISIBLE",
            f"Etichetta interna '{hit['token']}' visibile in {hit['path']}.",
        )

    # Money rows must carry an Italian display string.
    for sec_name, rows in (customer_report.get("money_sections") or {}).items():
        for row in rows or []:
            if row.get("amount") is not None and not row.get("amount_display"):
                warn(
                    "MONEY_NOT_FORMATTED",
                    f"Importo '{row.get('label')}' in {sec_name} senza formato €.",
                )
    return warnings, label_hits


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------
_BLOCK_DIMENSION = {
    "MONEY_ROLE_MISMATCH": "money_integrity",
    "EXCERPT_NOT_VERBATIM": "evidence_traceability",
    "MISSING_IMPORTANT_MONEY": "money_integrity",
    "CRITICAL_FACT_MISSING": "coverage_completeness",
    "INVENTED_BUYER_COST": "money_integrity",
    "PROCEDURE_FORMALITY_AS_BUYER_DEBT": "legal_formality_integrity",
    "FAKE_PREZZO_BASE": "money_integrity",
    "BENE_LOST": "lot_bene_integrity",
    "LOT_CONTAMINATION": "lot_bene_integrity",
    "CONFORMING_WITHOUT_EVIDENCE": "compliance_integrity",
    "SECTION_CONTRADICTION": "factual_accuracy",
    "UNSUPPORTED_MONEY_CLAIM": "factual_accuracy",
}

_WARN_DIMENSION = {
    "MONEY_ROLE_CONFLICT": "money_integrity",
    "EVIDENCE_EXCERPT_MISSING": "evidence_traceability",
    "DERIVED_AMOUNT": "evidence_traceability",
    "IMPORTANT_FACT_NOT_RENDERED": "coverage_completeness",
    "LARGE_UNCERTAINTY_SECTION": "uncertainty_handling",
    "LARGE_MANUAL_REVIEW_SECTION": "uncertainty_handling",
    "EVIDENCE_TOO_BROAD": "evidence_traceability",
    "RAW_INTERNAL_LABEL_VISIBLE": "customer_clarity",
    "MONEY_NOT_FORMATTED": "customer_clarity",
}

_SCORE_DIMENSIONS = (
    "factual_accuracy", "coverage_completeness", "money_integrity",
    "lot_bene_integrity", "legal_formality_integrity", "compliance_integrity",
    "uncertainty_handling", "customer_clarity", "evidence_traceability",
)


def _compute_scores(
    blocking: List[Dict[str, Any]],
    warnings: List[Dict[str, Any]],
    coverage_audit: Dict[str, Any],
    customer_report: Dict[str, Any],
) -> Dict[str, int]:
    scores: Dict[str, float] = {dim: 100.0 for dim in _SCORE_DIMENSIONS}

    for issue in blocking:
        dim = _BLOCK_DIMENSION.get(issue.get("code"), "factual_accuracy")
        scores[dim] = min(scores[dim], 40.0) - 5.0
    for issue in warnings:
        dim = _WARN_DIMENSION.get(issue.get("code"), "customer_clarity")
        scores[dim] -= 3.0

    # A fact is ACCOUNTED if it matched, partially matched, or was explicitly
    # excluded / backgrounded with a reason. Only silent losses count against
    # completeness.
    totals = coverage_audit.get("totals") or {}
    facts = max(1, int(totals.get("facts") or 0))
    unaccounted = len(coverage_audit.get("critical_omissions") or []) + len(
        coverage_audit.get("important_warnings") or []
    )
    ratio = max(0.0, 1.0 - unaccounted / facts)
    scores["coverage_completeness"] = min(
        scores["coverage_completeness"], max(40.0, 40.0 + 60.0 * ratio)
    )

    # Evidence traceability: fraction of key facts / money rows with pages.
    with_pages = 0
    total_items = 0
    for fact in customer_report.get("key_facts") or []:
        total_items += 1
        if fact.get("evidence_pages"):
            with_pages += 1
    for rows in (customer_report.get("money_sections") or {}).values():
        for row in rows or []:
            total_items += 1
            if row.get("evidence_pages"):
                with_pages += 1
    if total_items:
        ev_ratio = with_pages / total_items
        scores["evidence_traceability"] = min(
            scores["evidence_traceability"], 40.0 + 60.0 * ev_ratio
        )

    out = {dim: int(max(0, min(100, round(value)))) for dim, value in scores.items()}
    out["overall"] = int(round(sum(out[d] for d in _SCORE_DIMENSIONS) / len(_SCORE_DIMENSIONS)))
    return out


# ---------------------------------------------------------------------------
# Public builders
# ---------------------------------------------------------------------------
def build_quality_standard_report(
    *,
    analysis_id: str,
    job_id: str,
    pages: List[Dict[str, Any]],
    coverage_audit: Dict[str, Any],
    page_audit: Dict[str, Any],
    contract: Optional[Dict[str, Any]],
    customer_report: Dict[str, Any],
    validator_report: Optional[Dict[str, Any]] = None,
    lot_report: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    blocking = _check_blocking(
        coverage_audit=coverage_audit,
        contract=contract,
        customer_report=customer_report,
        validator_report=validator_report,
        lot_report=lot_report,
        pages=pages,
    )
    warnings, _label_hits = _check_warnings(
        coverage_audit=coverage_audit, customer_report=customer_report, pages=pages
    )
    scores = _compute_scores(blocking, warnings, coverage_audit, customer_report)

    if blocking:
        overall = "FAIL"
        readiness = "NOT_READY"
        decision = "REJECT"
    elif warnings or coverage_audit.get("coverage_status") == STATUS_WARNING:
        overall = "PASS_WITH_WARNINGS"
        readiness = "READY_WITH_WARNINGS"
        decision = "APPROVE_WITH_WARNINGS"
    else:
        overall = "PASS"
        readiness = "READY"
        decision = "APPROVE"
    if scores["overall"] < 70 and overall != "FAIL":
        overall = "FAIL"
        readiness = "NOT_READY"
        decision = "REJECT"

    risks: List[str] = []
    for issue in blocking:
        risks.append(f"[{issue['code']}] {issue['detail']}")
    for omission in (coverage_audit.get("important_warnings") or [])[:10]:
        risks.append(
            f"Possibile insoddisfazione: '{omission.get('document_fact')}' non reso nel report."
        )

    next_actions: List[str] = []
    if blocking:
        next_actions.append(
            "Correggere le omissioni/violazioni bloccanti elencate prima di esporre il report."
        )
    if warnings:
        next_actions.append("Rivedere le avvertenze non bloccanti (chiarezza/completezza).")
    if not blocking and not warnings:
        next_actions.append("Nessuna azione richiesta: report conforme allo standard di qualità.")

    return {
        "schema_version": QUALITY_REPORT_SCHEMA_VERSION,
        "analysis_id": str(analysis_id),
        "job_id": str(job_id),
        "overall_quality_status": overall,
        "customer_readiness": readiness,
        "coverage_status": coverage_audit.get("coverage_status"),
        "scores": scores,
        "blocking_issues": blocking,
        "warnings": warnings,
        "customer_satisfaction_risks": risks,
        "page_by_page_summary": page_audit.get("page_summary", []),
        "report_vs_document_table": coverage_audit.get("report_vs_document_table", []),
        "final_decision": decision,
        "next_actions": next_actions,
    }


def build_customer_satisfaction_scorecard(
    *,
    quality_report: Dict[str, Any],
    coverage_audit: Dict[str, Any],
    customer_report: Dict[str, Any],
) -> Dict[str, Any]:
    blocking = quality_report.get("blocking_issues") or []
    warnings = quality_report.get("warnings") or []
    q_scores = quality_report.get("scores") or {}

    clarity = 100
    raw_label_warns = [w for w in warnings if w.get("code") == "RAW_INTERNAL_LABEL_VISIBLE"]
    money_fmt_warns = [w for w in warnings if w.get("code") == "MONEY_NOT_FORMATTED"]
    clarity -= 15 * len(raw_label_warns) + 5 * len(money_fmt_warns)

    completeness = int(q_scores.get("coverage_completeness", 100))
    if any(b.get("code") in ("MISSING_IMPORTANT_MONEY", "CRITICAL_FACT_MISSING") for b in blocking):
        completeness = min(completeness, 30)

    selection_mode = customer_report.get("report_status") == "LOT_SELECTION_REQUIRED"
    actionability = 100
    money_sections = customer_report.get("money_sections") or {}
    if selection_mode:
        # The selector's "action" is choosing a lot: it needs lots + actions,
        # not a checklist/cost table (those belong to the per-lot report).
        selection = customer_report.get("lot_selection") or {}
        if not selection.get("lots"):
            actionability -= 40
        if not selection.get("available_actions"):
            actionability -= 20
    else:
        if not customer_report.get("buyer_checklist"):
            actionability -= 25
        has_costs = bool(
            money_sections.get("buyer_side_costs") or money_sections.get("valuation_chain")
        )
        if not has_costs:
            actionability -= 20
        # Review-worthy content must be surfaced SOMEWHERE actionable, not
        # necessarily in manual_review_flags: comparatives/context values are
        # surfaced by their own background sections, and coverage warnings are
        # surfaced by the attached page-by-page quality table. Only uncertain
        # money that carries NO review flag is genuinely unsurfaced.
        unsurfaced_uncertain = bool(money_sections.get("uncertain_money")) and not any(
            f.get("kind") in ("uncertain_money", "coverage_money")
            for f in customer_report.get("manual_review_flags") or []
        )
        if unsurfaced_uncertain:
            actionability -= 15

    trust = int(q_scores.get("evidence_traceability", 100))
    if any(b.get("code") == "UNSUPPORTED_MONEY_CLAIM" for b in blocking):
        trust = min(trust, 30)

    safety = 100
    unsafe_codes = {
        "FAKE_PREZZO_BASE", "PROCEDURE_FORMALITY_AS_BUYER_DEBT",
        "CONFORMING_WITHOUT_EVIDENCE", "INVENTED_BUYER_COST",
        "SECTION_CONTRADICTION", "MONEY_ROLE_MISMATCH",
    }
    safety -= 40 * sum(1 for b in blocking if b.get("code") in unsafe_codes)
    if any(b.get("code") in ("CRITICAL_FACT_MISSING", "MISSING_IMPORTANT_MONEY") for b in blocking):
        safety = min(safety, 40)

    usability = 100
    evidence_rows = len(customer_report.get("evidence_index") or [])
    if evidence_rows > 200:
        usability -= 15
    flags = len(customer_report.get("manual_review_flags") or [])
    if flags > 25:
        usability -= 10
    qc = customer_report.get("quality_control") or {}
    if len(qc.get("rows") or []) > 800:
        usability -= 10

    scores = {
        "clarity": max(0, min(100, clarity)),
        "completeness": max(0, min(100, completeness)),
        "actionability": max(0, min(100, actionability)),
        "trust": max(0, min(100, trust)),
        "safety": max(0, min(100, safety)),
        "usability": max(0, min(100, usability)),
    }
    overall = int(round(sum(scores.values()) / len(scores)))

    customer_ready = (
        overall >= 90
        and not blocking
        and coverage_audit.get("coverage_status") != STATUS_FAIL
        and not raw_label_warns
    )
    if customer_ready:
        status = "CUSTOMER_READY"
    elif not blocking and overall >= 70:
        status = "ADMIN_PREVIEW_ONLY"
    else:
        status = "NOT_READY"

    required_fixes = [f"[{b.get('code')}] {b.get('detail')}" for b in blocking]
    for w in raw_label_warns:
        required_fixes.append(f"[{w.get('code')}] {w.get('detail')}")
    nice_to_have = [
        f"[{w.get('code')}] {w.get('detail')}"
        for w in warnings
        if w.get("code") != "RAW_INTERNAL_LABEL_VISIBLE"
    ]

    return {
        "schema_version": SCORECARD_SCHEMA_VERSION,
        "analysis_id": quality_report.get("analysis_id"),
        "job_id": quality_report.get("job_id"),
        "overall_score": overall,
        "status": status,
        "scores": scores,
        "customer_risks": quality_report.get("customer_satisfaction_risks") or [],
        "required_fixes_before_customer_release": required_fixes,
        "nice_to_have_improvements": nice_to_have,
    }


def render_quality_markdown(
    quality_report: Dict[str, Any],
    scorecard: Dict[str, Any],
    page_audit: Dict[str, Any],
) -> str:
    """Human-readable quality_standard_report.md (optional artifact)."""
    lines: List[str] = []
    lines.append(f"# Certificato qualità report — job {quality_report.get('job_id')}")
    lines.append("")
    lines.append(f"* Stato qualità: **{quality_report.get('overall_quality_status')}**")
    lines.append(f"* Prontezza cliente: **{quality_report.get('customer_readiness')}**")
    lines.append(f"* Decisione: **{quality_report.get('final_decision')}**")
    lines.append(f"* Copertura: **{quality_report.get('coverage_status')}**")
    lines.append(f"* Punteggio soddisfazione cliente: **{scorecard.get('overall_score')}** ({scorecard.get('status')})")
    lines.append("")
    lines.append("## Punteggi")
    for key, value in (quality_report.get("scores") or {}).items():
        lines.append(f"* {key}: {value}")
    lines.append("")
    blocking = quality_report.get("blocking_issues") or []
    lines.append(f"## Problemi bloccanti ({len(blocking)})")
    for issue in blocking:
        lines.append(f"* **{issue.get('code')}** — {issue.get('detail')}")
    if not blocking:
        lines.append("* Nessuno.")
    lines.append("")
    warnings = quality_report.get("warnings") or []
    lines.append(f"## Avvertenze ({len(warnings)})")
    for issue in warnings[:40]:
        lines.append(f"* {issue.get('code')} — {issue.get('detail')}")
    if not warnings:
        lines.append("* Nessuna.")
    lines.append("")
    lines.append("## Controllo qualità pagina per pagina")
    lines.append("")
    lines.append("| Pagina | Dato rilevante nella perizia | Presente nel report | Esito | Note |")
    lines.append("|---|---|---|---|---|")
    for row in (page_audit.get("rows") or [])[:300]:
        dato = str(row.get("dato_perizia") or "").replace("|", "/")
        note = str(row.get("note") or "").replace("|", "/")
        presente = "Sì" if row.get("presente_nel_report") else "No"
        lines.append(
            f"| {row.get('page')} | {dato} | {presente} | {row.get('esito')} | {note} |"
        )
    return "\n".join(lines) + "\n"
