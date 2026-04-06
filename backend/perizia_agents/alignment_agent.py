from __future__ import annotations

from perizia_runtime.state import RuntimeState


def run_alignment_agent(state: RuntimeState) -> None:
    bundle = state.canonical_case.summary_bundle
    top_issue_it = str(bundle.get("top_issue_it") or "").strip().lower()
    decision_summary = str(bundle.get("decision_summary_it") or "").strip().lower()
    mentions_top_issue = bool(top_issue_it) and top_issue_it in decision_summary
    state.canonical_case.qa["summary_mentions_top_issue"] = mentions_top_issue

