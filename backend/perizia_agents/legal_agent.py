from __future__ import annotations

from perizia_runtime.state import CanonicalIssue, Judgment, RuntimeState
from perizia_tools.evidence_span_tool import make_evidence


def run_legal_agent(state: RuntimeState) -> None:
    cancellable = []
    surviving = []
    background = []
    for idx, page in enumerate(state.pages, start=1):
        text = str((page or {}).get("text") or "")
        low = text.lower()
        page_number = int((page or {}).get("page_number") or (page or {}).get("page") or idx)
        if "saranno cancellati a cura e spese della" in low or "formalità da cancellare" in low or "cancellati con il decreto di trasferimento" in low:
            if "ipoteca" in low:
                cancellable.append({"kind": "ipoteca", "evidence": [make_evidence(page_number, text[:520], "cancellable_encumbrance", ["legal"], 0.92)]})
            if "pignoramento" in low:
                cancellable.append({"kind": "pignoramento", "evidence": [make_evidence(page_number, text[:520], "cancellable_encumbrance", ["legal"], 0.92)]})
        if "resteranno a carico dell'acquirente" in low:
            if "non note" not in low and "non noti" not in low:
                surviving.append({"kind": "surviving_burden", "evidence": [make_evidence(page_number, text[:520], "surviving_encumbrance", ["legal"], 0.9)]})
        if "ipoteca" in low and not cancellable:
            background.append({"kind": "ipoteca", "evidence": [make_evidence(page_number, text[:520], "background_legal", ["legal"], 0.65)]})
    state.canonical_case.legal = {
        "cancellable": cancellable,
        "surviving": surviving,
        "background": background,
        "top_issue_guard": "cancellable_formalities_cannot_auto_dominate_priority",
    }
    if surviving:
        item = surviving[0]
        issue = CanonicalIssue(
            code="LEGAL_SURVIVING_BURDEN",
            title_it="Vincolo che resta a carico dell'acquirente",
            severity="RED",
            category="legal",
            priority_score=95.0,
            evidence=item["evidence"],
            summary_it="Il documento indica un vincolo che resta a carico dell'acquirente.",
            action_it="Verifica legale immediata prima dell'offerta.",
        )
        state.issues.append(issue)
        state.judgments["legal_top_issue"] = Judgment("legal_top_issue", issue.title_it, "FOUND", 0.9, issue.evidence, issue.summary_it)
    else:
        state.judgments["legal_top_issue"] = Judgment(
            "legal_top_issue",
            "FORMALITA_CANCELLABILI" if cancellable else None,
            "FOUND" if cancellable else "NOT_FOUND",
            0.7 if cancellable else 0.0,
            cancellable[0]["evidence"] if cancellable else [],
            "cancellable legal items kept as background, not automatic buyer-side top risk",
        )

