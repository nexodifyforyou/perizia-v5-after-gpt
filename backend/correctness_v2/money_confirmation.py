"""
Customer money-confirmation: human-in-the-loop disambiguation of a small number
of GENUINE, RESOLVABLE money-role ambiguities that would OTHERWISE hard-block a
report (NEEDS_MANUAL_REVIEW).

Design (a faithful mirror of the LOT_SELECTION_REQUIRED flow):
  * A paused state (JobStatus.MONEY_CONFIRMATION_REQUIRED) analogous to
    LOT_SELECTION_REQUIRED — controlled, customer-safe, NOT a failure.
  * A ``money_confirmation`` customer-report payload analogous to
    ``lot_selection``: a short list of ambiguities, each with the formatted
    amount, 2-3 candidate interpretations (human labels), the EXACT location to
    check (page + verbatim excerpt), and a plain-Italian question.
  * A submit that records {ambiguity_id: chosen_option} and re-runs the gate
    with those answers as ground truth (see coverage_audit.money_confirmations).

Guardrails (owner: "do not make it a habit"):
  * Triggers ONLY when EVERY blocking issue is a resolvable money ambiguity
    (an amount with >=2 real candidate roles tied to a specific page). If any
    blocking issue is something else (missing critical fact, structural, an
    unresolvable pure-missing money) -> NOT eligible -> NEEDS_MANUAL_REVIEW.
  * Capped at MAX_MONEY_CONFIRMATIONS. More ambiguities than the cap -> the
    document is genuinely too uncertain -> NEEDS_MANUAL_REVIEW (no long quiz).
  * 100% deterministic. No LLM, no network. Never invents a value: every
    ambiguity carries its page + verbatim excerpt so the answer is auditable.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from . import doc_signals
from .schemas import JobStatus

MONEY_CONFIRMATION_SCHEMA_VERSION = "cv2.money_confirmation_required.v1"

# Owner cap: never quiz a customer with more than this many confirmations. A
# document with more resolvable ambiguities than the cap is genuinely too
# uncertain and falls back to manual review.
MAX_MONEY_CONFIRMATIONS = 3

# Money categories that can carry a customer-resolvable value/cost ambiguity.
_MONEY_CATEGORIES = frozenset({"money", "sale_terms"})


def _format_eur(amount: Any) -> str:
    """Italian currency formatting ('452494.0' -> '€ 452.494,00'). Deterministic."""
    try:
        value = float(amount)
    except (TypeError, ValueError):
        return str(amount)
    whole = f"{value:,.2f}"  # 452,494.00
    whole = whole.replace(",", "§").replace(".", ",").replace("§", ".")
    return f"€ {whole}"


def _role_label(role: str) -> str:
    return doc_signals.ROLE_LABELS_IT.get(role, str(role))


def _is_resolvable(omission: Dict[str, Any]) -> bool:
    """A critical omission that a customer can concretely disambiguate.

    Requires a money/sale-terms amount with a page and >=2 distinct candidate
    roles (the document-detected reading and how the report placed it)."""
    if omission.get("category") not in _MONEY_CATEGORIES:
        return False
    if omission.get("amount") is None:
        return False
    if not (omission.get("evidence_pages") or []):
        return False
    roles = [r for r in (omission.get("confirmation_roles") or []) if r]
    return len(dict.fromkeys(roles)) >= 2


def _resolvable_omissions(coverage_audit: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """fact_id -> omission for each resolvable money ambiguity (first wins)."""
    out: Dict[str, Dict[str, Any]] = {}
    for omission in coverage_audit.get("critical_omissions") or []:
        fact_id = omission.get("fact_id")
        if not fact_id or fact_id in out:
            continue
        if _is_resolvable(omission):
            out[fact_id] = omission
    return out


def _build_ambiguity(omission: Dict[str, Any]) -> Dict[str, Any]:
    roles = list(dict.fromkeys(r for r in omission.get("confirmation_roles") or [] if r))
    pages = [int(p) for p in omission.get("evidence_pages") or []]
    amount = omission.get("amount")
    excerpt = doc_signals.normalize_ws(omission.get("snippet") or "")
    options = [{"option_id": role, "label": _role_label(role)} for role in roles]
    labels = " oppure ".join(f"«{o['label']}»" for o in options)
    return {
        "ambiguity_id": omission.get("fact_id"),
        "amount": amount,
        "amount_display": _format_eur(amount),
        "page": pages[0] if pages else None,
        "evidence_pages": pages,
        "excerpt": excerpt,
        "question": (
            f"Per l'importo {_format_eur(amount)} (pag. "
            f"{pages[0] if pages else '-'}) abbiamo trovato due possibili "
            f"interpretazioni: {labels}. Quale è corretta?"
        ),
        "options": options,
    }


def eligible(
    coverage_audit: Dict[str, Any], blocking_issues: List[Dict[str, Any]]
) -> bool:
    """True when the ONLY thing blocking the report is a small, resolvable set of
    money ambiguities the customer can confirm.

    False (-> caller keeps NEEDS_MANUAL_REVIEW) when there are no blocking
    issues, when any blocking issue is not a resolvable money ambiguity, or when
    the number of ambiguities exceeds the cap.
    """
    blocking_issues = blocking_issues or []
    if not blocking_issues:
        return False
    resolvable = _resolvable_omissions(coverage_audit)
    if not resolvable:
        return False
    # Every blocking issue must be one of the resolvable money ambiguities.
    for issue in blocking_issues:
        if issue.get("fact_id") not in resolvable:
            return False
    return len(resolvable) <= MAX_MONEY_CONFIRMATIONS


def build_money_confirmation(
    *,
    analysis_id: str,
    job_id: str,
    coverage_audit: Dict[str, Any],
    blocking_issues: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Build the MONEY_CONFIRMATION_REQUIRED payload, or None when not eligible."""
    if not eligible(coverage_audit, blocking_issues):
        return None
    resolvable = _resolvable_omissions(coverage_audit)
    ambiguities = [_build_ambiguity(om) for om in resolvable.values()]
    ambiguities = [a for a in ambiguities if a["options"] and a["amount"] is not None]
    if not ambiguities:
        return None
    return {
        "schema_version": MONEY_CONFIRMATION_SCHEMA_VERSION,
        "analysis_id": str(analysis_id),
        "job_id": str(job_id),
        "status": JobStatus.MONEY_CONFIRMATION_REQUIRED,
        "reason_code": "MONEY_CONFIRMATION_REQUIRED",
        "message": (
            "Per completare il report servono una o più conferme su come "
            "interpretare alcuni importi. Il documento supporta più letture: "
            "selezioni quella corretta per ciascun importo indicato."
        ),
        "ambiguities": ambiguities,
    }


def validate_answers(
    payload: Dict[str, Any], answers: Any
) -> Dict[str, str]:
    """Validate the customer's answers against the payload.

    ``answers`` is {ambiguity_id: option_id}. Returns a clean {fact_id: role}
    map (the confirmed ground truth for the coverage audit). Raises ValueError
    with a customer-safe message when an answer is missing or not one of the
    offered options — the pipeline never accepts an unoffered role.
    """
    if not isinstance(answers, dict):
        raise ValueError("Risposte di conferma non valide.")
    ambiguities = {
        a.get("ambiguity_id"): a for a in payload.get("ambiguities") or []
    }
    if not ambiguities:
        raise ValueError("Nessuna conferma richiesta per questo report.")
    confirmed: Dict[str, str] = {}
    for ambiguity_id, ambiguity in ambiguities.items():
        chosen = answers.get(ambiguity_id)
        valid_options = {o.get("option_id") for o in ambiguity.get("options") or []}
        if chosen is None:
            raise ValueError(
                "Selezionare un'interpretazione per ogni importo indicato."
            )
        if chosen not in valid_options:
            raise ValueError(
                "Interpretazione selezionata non valida per l'importo indicato."
            )
        confirmed[str(ambiguity_id)] = str(chosen)
    return confirmed
