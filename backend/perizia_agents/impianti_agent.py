from __future__ import annotations

from perizia_runtime.state import RuntimeState


def run_impianti_agent(state: RuntimeState) -> None:
    values = []
    for idx, page in enumerate(state.pages, start=1):
        text = str((page or {}).get("text") or "").lower()
        if "conformità impianto" in text and "non verificabile" in text:
            values.append({"page": int((page or {}).get("page_number") or (page or {}).get("page") or idx), "status": "NON_VERIFICABILE"})
    state.canonical_case.impianti = {"items": values}

