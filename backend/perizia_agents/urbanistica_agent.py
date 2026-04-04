from __future__ import annotations

from perizia_runtime.state import RuntimeState


def run_urbanistica_agent(state: RuntimeState) -> None:
    state.canonical_case.urbanistica.setdefault("status", "NOT_ROUTED_YET")

