from __future__ import annotations

from perizia_runtime.state import Judgment, RuntimeState
from perizia_tools.quota_parser_tool import quota_candidates


def run_catasto_agent(state: RuntimeState) -> None:
    candidates = quota_candidates(state.pages, state.result)
    state.candidates["quota"] = candidates
    if candidates:
        best = sorted(candidates, key=lambda item: (-item.confidence, -len(item.evidence)))[0]
        judgment = Judgment(
            field_key="quota",
            value=best.value,
            status="FOUND",
            confidence=best.confidence,
            evidence=best.evidence,
            rationale="fraction-pattern quota extracted from rights context",
        )
        state.judgments["quota"] = judgment
        state.canonical_case.rights["quota"] = {
            "value": best.value,
            "confidence": best.confidence,
            "evidence": best.evidence,
            "guards": ["fraction_pattern_required", "date_like_tokens_rejected"],
        }
    else:
        judgment = Judgment(
            field_key="quota",
            value=None,
            status="NOT_FOUND",
            confidence=0.0,
            evidence=[],
            rationale="no quota fraction survived rights-context guards",
        )
        state.judgments["quota"] = judgment
        state.canonical_case.rights["quota"] = {
            "value": None,
            "confidence": 0.0,
            "evidence": [],
            "guards": ["fraction_pattern_required", "date_like_tokens_rejected"],
        }

