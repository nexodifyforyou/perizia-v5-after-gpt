"""
Quality gate runner for Correctness Mode v2.

Runs, persists and evaluates the four quality artifacts for a rendered report:

    coverage_audit.json
    page_by_page_audit.json
    quality_standard_report.json
    customer_satisfaction_scorecard.json
    quality_standard_report.md (human review)

and attaches the customer-facing "Controllo qualità pagina per pagina" section
to customer_report.json. Deterministic, generic, fail-closed: a gate crash never
lets an unaudited report pass as clean (the orchestrator treats a gate error as
a quality failure, not as an excuse to skip the audit).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from . import artifacts, coverage_audit as coverage_mod, quality_report as quality_mod

GATE_PASS = "PASS"
GATE_WARNING = "WARNING"
GATE_FAIL = "FAIL"


def run_quality_gate(
    *,
    job_id: str,
    analysis_id: str,
    pages: List[Dict[str, Any]],
    worksheet: Optional[Dict[str, Any]],
    contract: Optional[Dict[str, Any]],
    customer_report: Dict[str, Any],
    validator_report: Optional[Dict[str, Any]] = None,
    lot_report: Optional[Dict[str, Any]] = None,
    lot_index: Optional[Dict[str, Any]] = None,
    artifacts_saved: Optional[Dict[str, Any]] = None,
    money_confirmations: Optional[Dict[str, str]] = None,
    persist: bool = True,
) -> Dict[str, Any]:
    """Build + persist all quality artifacts; return the gate outcome.

    Returns::

        {
          "gate_status": PASS | WARNING | FAIL,
          "coverage_audit": {...}, "page_audit": {...},
          "quality_report": {...}, "scorecard": {...},
          "customer_report": <report with quality_control attached>,
        }
    """
    audit, page_audit = coverage_mod.build_coverage_audit(
        analysis_id=analysis_id,
        job_id=job_id,
        pages=pages,
        worksheet=worksheet,
        contract=contract,
        customer_report=customer_report,
        validator_report=validator_report,
        lot_report=lot_report,
        lot_index=lot_index,
        money_confirmations=money_confirmations,
    )

    # Second chance for NON-critical page topics the report does not treat:
    # surface them as explicit manual-review flags (visible uncertainty is not
    # failure; hidden uncertainty is) and re-audit. Critical facts and money are
    # NEVER downgraded this way — they keep blocking.
    added_flags = _augment_manual_review(customer_report, audit)
    if added_flags:
        audit, page_audit = coverage_mod.build_coverage_audit(
            analysis_id=analysis_id,
            job_id=job_id,
            pages=pages,
            worksheet=worksheet,
            contract=contract,
            customer_report=customer_report,
            validator_report=validator_report,
            lot_report=lot_report,
            lot_index=lot_index,
            money_confirmations=money_confirmations,
        )

    quality = quality_mod.build_quality_standard_report(
        analysis_id=analysis_id,
        job_id=job_id,
        pages=pages,
        coverage_audit=audit,
        page_audit=page_audit,
        contract=contract,
        customer_report=customer_report,
        validator_report=validator_report,
        lot_report=lot_report,
    )
    scorecard = quality_mod.build_customer_satisfaction_scorecard(
        quality_report=quality,
        coverage_audit=audit,
        customer_report=customer_report,
    )

    report = attach_quality_section(customer_report, audit, page_audit, quality, scorecard)

    if persist:
        saved = artifacts_saved if artifacts_saved is not None else {}
        saved["coverage_audit"] = artifacts.save_coverage_audit(job_id, audit)
        saved["page_by_page_audit"] = artifacts.save_page_audit(job_id, page_audit)
        saved["quality_standard_report"] = artifacts.save_quality_report(job_id, quality)
        saved["customer_satisfaction_scorecard"] = artifacts.save_scorecard(job_id, scorecard)
        saved["quality_standard_report_md"] = artifacts.save_quality_markdown(
            job_id, quality_mod.render_quality_markdown(quality, scorecard, page_audit)
        )
        saved["customer_report"] = artifacts.save_customer_report(job_id, report)

    if quality.get("overall_quality_status") == "FAIL" or audit.get("coverage_status") == GATE_FAIL:
        gate_status = GATE_FAIL
    elif quality.get("overall_quality_status") == "PASS_WITH_WARNINGS" or audit.get(
        "coverage_status"
    ) == GATE_WARNING:
        gate_status = GATE_WARNING
    else:
        gate_status = GATE_PASS

    return {
        "gate_status": gate_status,
        "coverage_audit": audit,
        "page_audit": page_audit,
        "quality_report": quality,
        "scorecard": scorecard,
        "customer_report": report,
    }


def _augment_manual_review(
    customer_report: Dict[str, Any], audit: Dict[str, Any]
) -> int:
    """Turn missed NON-critical page topics into visible manual-review flags.

    Only deterministic page-topic signals (never worksheet facts, never money
    amounts, never critical severity) are eligible: those must be fixed at the
    source, not papered over. The flag quotes the page and a short snippet of
    the document text, so no fact is invented. Mutates ``customer_report``.
    """
    flags = customer_report.setdefault("manual_review_flags", [])
    existing = {str(f.get("detail") or "") for f in flags}
    added = 0
    for fact in audit.get("fact_coverage") or []:
        source = fact.get("source")
        if source not in ("page_topic", "page_money"):
            continue
        if fact.get("match_status") != coverage_mod.MISSING:
            continue
        if fact.get("severity") == "critical":
            continue
        if fact.get("action") in (
            coverage_mod.ACTION_EXCLUDED, coverage_mod.ACTION_BACKGROUND,
        ):
            continue  # already accounted with a reason
        pages_str = ", ".join(str(p) for p in fact.get("evidence_pages") or [])
        snippet = str(fact.get("snippet") or "")[:120]
        if source == "page_money":
            amount_display = _format_eur_it(_fact_amount(fact))
            detail = (
                f"Importo indicato in perizia a pag. {pages_str} "
                f"({amount_display}) non classificato automaticamente: da "
                f"verificare sul documento. Testo: “{snippet}…”"
            )
            kind = "coverage_money"
            kind_label = "Importo da verificare"
        else:
            detail = (
                f"La perizia tratta questo tema a pag. {pages_str} ma il report "
                f"non lo riporta in dettaglio: da verificare sul documento. "
                f"Testo: “{snippet}…”"
            )
            kind = "coverage_topic"
            kind_label = "Tema della perizia da verificare"
        if detail in existing:
            continue
        flags.append(
            {
                "kind": kind,
                "kind_label": kind_label,
                "detail": detail,
                "evidence_pages": fact.get("evidence_pages") or [],
            }
        )
        existing.add(detail)
        added += 1
    return added


def _fact_amount(fact: Dict[str, Any]) -> Any:
    """Recover the numeric amount from a page_money coverage fact."""
    text = str(fact.get("document_fact") or "")
    if ":" in text:
        tail = text.rsplit(":", 1)[-1].strip()
        try:
            return float(tail)
        except ValueError:
            pass
    return None


def _format_eur_it(amount: Any) -> str:
    try:
        value = float(amount)
    except (TypeError, ValueError):
        return "importo n.d."
    grouped = f"{abs(value):,.2f}".replace(",", "|").replace(".", ",").replace("|", ".")
    sign = "-" if value < 0 else ""
    return f"{sign}€ {grouped}"


def attach_quality_section(
    customer_report: Dict[str, Any],
    audit: Dict[str, Any],
    page_audit: Dict[str, Any],
    quality: Dict[str, Any],
    scorecard: Dict[str, Any],
) -> Dict[str, Any]:
    """Attach the customer/admin quality-control section to the report.

    Only summarized, already-audited data goes in — no new facts. Raw internal
    fact ids stay in the JSON artifacts (collapsed admin debug), not here.
    """
    report = dict(customer_report)
    rows = []
    for row in page_audit.get("rows") or []:
        view = {
            "pagina": row.get("page"),
            "dato": row.get("dato_perizia"),
            "presente": bool(row.get("presente_nel_report")),
            "esito": row.get("esito"),
            "note": row.get("note") or "",
        }
        if row.get("ruolo_label"):
            view["ruolo"] = row["ruolo_label"]
        rows.append(view)
    report["quality_control"] = {
        "title": "Controllo qualità pagina per pagina",
        "coverage_status": audit.get("coverage_status"),
        "quality_status": quality.get("overall_quality_status"),
        "customer_readiness": quality.get("customer_readiness"),
        "satisfaction_score": scorecard.get("overall_score"),
        "satisfaction_status": scorecard.get("status"),
        "blocking_issue_count": len(quality.get("blocking_issues") or []),
        "warning_count": len(quality.get("warnings") or []),
        "columns": ["Pagina", "Dato rilevante nella perizia", "Presente nel report", "Esito", "Note"],
        "rows": rows,
        "page_summary": page_audit.get("page_summary") or [],
    }
    return report
