from __future__ import annotations

from perizia_runtime.state import RuntimeState


def _normalized_issue_dict(item):
    if isinstance(item, dict):
        return item
    return {
        "code": item.code,
        "title_it": item.title_it,
        "severity": item.severity,
        "category": item.category,
        "priority_score": item.priority_score,
        "evidence": item.evidence,
        "summary_it": item.summary_it,
        "action_it": item.action_it,
        "metadata": item.metadata,
    }


def _has_grounded_evidence(item) -> bool:
    evidence = item.get("evidence", []) if isinstance(item, dict) else []
    return bool([ev for ev in evidence if ev])


def _policy_bucket(item, *, has_surviving_legal: bool, occupancy_supported: bool, has_evidenced_cost_issue: bool) -> int:
    code = str(item.get("code") or "")
    category = str(item.get("category") or "")
    if code == "LEGAL_SURVIVING_BURDEN" or (category == "legal" and has_surviving_legal):
        return 5
    if code == "OCCUPANCY_RISK":
        return 4 if occupancy_supported else 1
    if code == "EXPLICIT_BUYER_COSTS":
        return 4 if _has_grounded_evidence(item) else 2
    if category == "legal_background":
        return 0 if has_evidenced_cost_issue else 1
    return 2


def _build_cancellable_attention_issue(legal):
    cancellable = legal.get("cancellable", []) if isinstance(legal, dict) else []
    if not cancellable:
        return None
    evidence = cancellable[0].get("evidence", []) if isinstance(cancellable[0], dict) else []
    count = len(cancellable)
    title_it = "Formalità da cancellare"
    if count > 1:
        title_it = f"Formalità da cancellare ({count})"
    return {
        "code": "LEGAL_CANCELLABLE_ATTENTION",
        "title_it": title_it,
        "severity": "AMBER",
        "category": "legal_background",
        "priority_score": 34.0,
        "evidence": evidence,
        "summary_it": "La perizia segnala formalità da cancellare con la procedura; serve un controllo legale del perimetro delle iscrizioni e trascrizioni.",
        "action_it": "Verifica che il decreto di trasferimento disponga la cancellazione delle formalità indicate.",
        "metadata": {"cancellable_count": count},
    }


def run_priority_agent(state: RuntimeState) -> None:
    issues = list(state.issues)
    legal = state.canonical_case.legal
    occupancy = state.canonical_case.occupancy
    occupancy_supported = occupancy.get("status") == "OCCUPATO" and bool(occupancy.get("evidence"))
    if occupancy_supported:
        opponibilita = str(occupancy.get("opponibilita") or "").strip().upper()
        is_non_opponibile = "NON OPPONIBILE" in opponibilita
        if is_non_opponibile:
            issues.append(
                {
                    "code": "OCCUPANCY_RISK",
                    "title_it": "Occupazione non opponibile al terzo acquirente",
                    "severity": "AMBER",
                    "category": "occupancy",
                    "priority_score": 52.0,
                    "evidence": occupancy.get("evidence", []),
                    "summary_it": "La perizia indica occupazione non opponibile ai terzi acquirenti.",
                    "action_it": "Verifica la documentazione sull'opponibilità prima dell'offerta.",
                }
            )
        else:
            issues.append(
                {
                    "code": "OCCUPANCY_RISK",
                    "title_it": "Immobile occupato",
                    "severity": "RED",
                    "category": "occupancy",
                    "priority_score": 88.0,
                    "evidence": occupancy.get("evidence", []),
                    "summary_it": "Lo stato occupativo richiede verifica immediata.",
                    "action_it": "Verifica titolo e tempi di liberazione.",
                }
            )
    normalized = [_normalized_issue_dict(item) for item in issues]
    has_surviving_legal = bool(legal.get("surviving"))
    has_evidenced_cost_issue = any(
        str(item.get("code") or "") == "EXPLICIT_BUYER_COSTS" and _has_grounded_evidence(item)
        for item in normalized
    )
    has_stronger_issue = any(
        _policy_bucket(
            item,
            has_surviving_legal=has_surviving_legal,
            occupancy_supported=occupancy_supported,
            has_evidenced_cost_issue=has_evidenced_cost_issue,
        ) >= 2
        for item in normalized
    )
    if not has_stronger_issue and legal.get("cancellable"):
        fallback_issue = _build_cancellable_attention_issue(legal)
        if fallback_issue and not any(str(item.get("code") or "") == fallback_issue["code"] for item in normalized):
            normalized.append(fallback_issue)
    normalized.sort(
        key=lambda item: (
            -_policy_bucket(
                item,
                has_surviving_legal=has_surviving_legal,
                occupancy_supported=occupancy_supported,
                has_evidenced_cost_issue=has_evidenced_cost_issue,
            ),
            -int(_has_grounded_evidence(item)),
            -float(item.get("priority_score", 0.0)),
            str(item.get("code") or ""),
        )
    )
    top_issue = normalized[0] if normalized else None
    state.canonical_case.priority = {
        "issues": normalized[:6],
        "top_issue": top_issue,
        "priority_policy": [
            "surviving_legal_before_evidenced_occupancy_or_evidenced_costs",
            "explicit_buyer_costs_with_evidence_outrank_cancellable_only_legal_background",
            "occupancy_risk_requires_valid_occupancy_evidence",
            "cancellable_formalities_cannot_auto_dominate_priority",
        ],
    }
