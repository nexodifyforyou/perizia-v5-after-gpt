from __future__ import annotations

from perizia_runtime.state import RuntimeState


def run_contradiction_agent(state: RuntimeState) -> None:
    contradictions = []
    pricing = state.canonical_case.pricing
    if pricing.get("absurdity_guard_triggered"):
        contradictions.append({"code": "PRICE_ABSURDITY", "severity": "ERROR"})
    invalid_occupancy = [
        cand for cand in state.canonical_case.occupancy.get("candidates", [])
        if not cand.get("valid") and "occupancy" in str(cand.get("reason") or "")
    ]
    if invalid_occupancy:
        contradictions.append({"code": "INVALID_OCCUPANCY_EVIDENCE", "severity": "WARN"})
    state.canonical_case.qa["contradictions"] = contradictions

