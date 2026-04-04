from __future__ import annotations

from perizia_runtime.state import RuntimeState
from perizia_tools.section_router_tool import classify_section_type


def run_structure_agent(state: RuntimeState) -> None:
    page_sections = []
    for idx, page in enumerate(state.pages, start=1):
        text = str((page or {}).get("text") or "")
        page_sections.append(
            {
                "page": int((page or {}).get("page_number") or (page or {}).get("page") or idx),
                "section_type": classify_section_type(text),
            }
        )
    state.canonical_case.identity["page_sections"] = page_sections

