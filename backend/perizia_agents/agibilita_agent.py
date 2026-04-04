from __future__ import annotations

from perizia_runtime.state import RuntimeState


def run_agibilita_agent(state: RuntimeState) -> None:
    for idx, page in enumerate(state.pages, start=1):
        text = str((page or {}).get("text") or "").lower()
        if "agibilità rilasciata" in text or "agibilità del" in text:
            state.canonical_case.agibilita = {"status": "PRESENTE", "page": int((page or {}).get("page_number") or (page or {}).get("page") or idx)}
            return
    state.canonical_case.agibilita = {"status": "NON_VERIFICABILE"}

