from __future__ import annotations

from typing import Any

from perizia_runtime.state import RuntimeState


def _append_unique(items: list[str], value: str) -> None:
    value = " ".join(str(value or "").split()).strip()
    if value and value not in items:
        items.append(value)


def _verify_next_lines(value: Any) -> list[str]:
    lines: list[str] = []
    if isinstance(value, str):
        _append_unique(lines, value)
    elif isinstance(value, list):
        for item in value:
            for line in _verify_next_lines(item):
                _append_unique(lines, line)
    elif isinstance(value, dict):
        for key in ("verify_next", "reason_unresolved"):
            for line in _verify_next_lines(value.get(key)):
                _append_unique(lines, line)
        for nested in value.values():
            if isinstance(nested, dict):
                for line in _verify_next_lines(nested):
                    _append_unique(lines, line)
    return lines


def _build_unresolved_next_steps(state: RuntimeState, caution_points_it: list[str]) -> list[str]:
    canonical = state.canonical_case
    steps: list[str] = []
    occupancy = canonical.occupancy
    opponibilita = str(occupancy.get("opponibilita") or "").strip().upper()
    if opponibilita == "NON VERIFICABILE":
        _append_unique(
            steps,
            "Chiarisci l'opponibilità dell'occupazione: la perizia non contiene un segnale decisivo riferito al bene.",
        )

    agibilita = canonical.agibilita
    if str(agibilita.get("status") or "").strip().upper() == "NON_VERIFICABILE":
        trail_lines = _verify_next_lines(agibilita.get("verification_trail"))
        if trail_lines:
            _append_unique(steps, trail_lines[0])
        else:
            _append_unique(steps, "Verifica il certificato di agibilità o abitabilità negli allegati edilizi.")

    legal = canonical.legal
    for key, label in (
        ("vincoli_status", "vincoli"),
        ("servitu_status", "servitù"),
        ("opponibilita_status", "opponibilità legale"),
    ):
        status = legal.get(key)
        if not isinstance(status, dict):
            continue
        if str(status.get("value") or "").strip().upper() != "NON_VERIFICABILE":
            continue
        trail_lines = _verify_next_lines(status.get("verification_trail"))
        if trail_lines:
            _append_unique(steps, trail_lines[0])
        else:
            _append_unique(steps, f"Verifica {label} nello stesso perimetro del bene.")

    pricing = canonical.pricing
    if pricing and pricing.get("selected_price") is None and "degraded_source_text_only" in pricing.get("guards", []):
        _append_unique(
            steps,
            "Prezzo base non confermato dal testo degradato: controlla avviso di vendita e riepilogo finale.",
        )

    if not steps:
        steps.extend(caution_points_it[:2])
    return steps


def run_summary_agent(state: RuntimeState) -> None:
    top_issue = state.canonical_case.priority.get("top_issue") or {}
    occupancy = state.canonical_case.occupancy
    costs = state.canonical_case.costs
    top_issue_it = str(top_issue.get("title_it") or "").strip()
    if not top_issue_it and costs.get("explicit_total"):
        top_issue_it = f"Costi espliciti a carico dell'acquirente: € {float(costs['explicit_total']):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    caution_points_it = []
    opponibilita = str(occupancy.get("opponibilita") or "").strip().upper()
    occ_status = str(occupancy.get("status") or "").strip().upper()
    is_non_opponibile = "NON OPPONIBILE" in opponibilita
    if is_non_opponibile and occ_status == "LIBERO":
        # Property is effectively free but has non-binding occupancy — surface the full nuance.
        caution_points_it.append(
            "Immobile libero; occupazione non opponibile all'aggiudicatario, liberazione a cura e spese della procedura"
        )
    elif opponibilita == "NON VERIFICABILE":
        caution_points_it.append("Opponibilità non verificabile dalla perizia")
    if state.canonical_case.pricing.get("absurdity_guard_triggered"):
        caution_points_it.append("Prezzo scartato per possibile contaminazione numerica")
    if costs.get("explicit_total"):
        caution_points_it.append(f"Costi espliciti rilevati: € {float(costs['explicit_total']):,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
    next_step_it = str(top_issue.get("action_it") or "").strip()
    if not next_step_it and str(top_issue.get("code") or "") == "LEGAL_CANCELLABLE_ATTENTION":
        next_step_it = "Verifica che il decreto di trasferimento disponga la cancellazione delle formalità indicate."
    if not next_step_it:
        unresolved_steps = _build_unresolved_next_steps(state, caution_points_it)
        next_step_it = " ".join(part.rstrip(".") + "." for part in unresolved_steps[:2] if part).strip()
    # When the property is LIBERO with NON_OPPONIBILE, lead with the occupancy nuance
    # rather than defaulting to a generic costs-only summary.
    if is_non_opponibile and occ_status == "LIBERO" and not str(top_issue.get("code") or "").startswith("LEGAL_SURVIVING"):
        occupancy_clause = "Immobile libero con occupazione non opponibile all'aggiudicatario (liberazione a cura e spese della procedura esecutiva)"
        summary_it_parts = [occupancy_clause]
        if top_issue_it and top_issue_it != occupancy_clause:
            summary_it_parts.append(top_issue_it)
        summary_it_parts.append(next_step_it)
    else:
        summary_it_parts = [part for part in [top_issue_it, next_step_it] if part]
    summary_it = " ".join(part.rstrip(".") + "." for part in summary_it_parts if part)[:1500] or "Analisi completata con verifiche manuali ancora necessarie."
    state.canonical_case.summary_bundle = {
        "top_issue_it": top_issue_it,
        "top_issue_en": "",
        "next_step_it": next_step_it,
        "next_step_en": "",
        "caution_points_it": caution_points_it[:2],
        "user_messages_it": [],
        "document_quality_status": str((state.result.get("document_quality") or {}).get("status") or ""),
        "semaforo_status": str((state.result.get("semaforo_generale") or {}).get("status") or ""),
        "decision_summary_it": summary_it,
        "decision_summary_en": "",
        "evidence_snippets": [
            {"page": ev.page, "quote": ev.quote}
            for ev in list(top_issue.get("evidence") or [])[:2]
        ],
        "source": "canonical_issue_bundle",
    }
