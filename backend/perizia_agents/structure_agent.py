from __future__ import annotations

import re

from perizia_runtime.state import RuntimeState
from perizia_tools.section_router_tool import classify_section_type

_LOT_RE = re.compile(r"\blotto\s*(?:n[°º.]?\s*)?(unico|\d+)\b", re.IGNORECASE)
_BENE_RE = re.compile(r"\bbene\s*n[°º.]?\s*(\d+)\b", re.IGNORECASE)
_BENE_HEADING_RE = re.compile(r"^(?:[•*\-]\s*)?bene\s*n[°º.]?\s*(\d+)\b", re.IGNORECASE)
_INDEX_LINE_RE = re.compile(r"\.{5,}\s*\d+\s*$")


def _normalize_lotto_id(token: str) -> str:
    token_low = str(token or "").strip().lower()
    return "lotto:unico" if token_low == "unico" else f"lotto:{token_low}"


def _register_scope_detection(
    state: RuntimeState,
    *,
    scope_id: str,
    scope_type: str,
    parent_scope_id: str | None,
    label: str,
    page: int,
    quote: str,
    ownership_method: str,
) -> None:
    scope = state.get_or_create_scope(scope_id, scope_type=scope_type, parent_scope_id=parent_scope_id, label=label)
    metadata = scope.metadata
    pages = metadata.setdefault("detected_from_pages", [])
    if page not in pages:
        pages.append(page)
    labels = metadata.setdefault("detected_labels", [])
    if label not in labels:
        labels.append(label)
    methods = metadata.setdefault("ownership_methods", [])
    if ownership_method not in methods:
        methods.append(ownership_method)
    if "detected_from_page" not in metadata:
        metadata["detected_from_page"] = page
    if "detected_label" not in metadata:
        metadata["detected_label"] = label
    if "ownership_method" not in metadata:
        metadata["ownership_method"] = ownership_method
    state.attach_evidence_ownership(
        scope_id=scope_id,
        field_target="scope.discovery",
        source_page=page,
        quote=quote,
        confidence=0.9,
        ownership_method=ownership_method,
        evidence_id=f"scope_{scope_id.replace(':', '_')}_{page}_{len(scope.evidence_ids)+1}",
    )


def run_structure_agent(state: RuntimeState) -> None:
    page_sections = []
    discovered_scopes = []
    pending_strong_lotto_scope_id: str | None = None
    for idx, page in enumerate(state.pages, start=1):
        text = str((page or {}).get("text") or "")
        page_number = int((page or {}).get("page_number") or (page or {}).get("page") or idx)
        page_sections.append(
            {
                "page": page_number,
                "section_type": classify_section_type(text),
            }
        )
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        page_lotto_ids = {
            _normalize_lotto_id(match.group(1))
            for line in lines
            for match in _LOT_RE.finditer(line)
        }
        page_bene_count = sum(1 for line in lines if _BENE_RE.search(line))
        page_has_ambiguous_lotto_context = len(page_lotto_ids) > 1
        page_is_dedicated_lotto_title_page = len(page_lotto_ids) == 1 and page_bene_count == 0 and len(lines) <= 3
        carry_lotto_scope_id = pending_strong_lotto_scope_id
        pending_strong_lotto_scope_id = None
        page_lotto_scope_id: str | None = None
        seen_page_lotto_scope_ids: set[str] = set()
        for line in lines:
            if _INDEX_LINE_RE.search(line):
                continue
            line_lotto_scope_ids: list[str] = []
            for lot_match in _LOT_RE.finditer(line):
                lot_token = lot_match.group(1)
                lot_scope_id = _normalize_lotto_id(lot_token)
                line_lotto_scope_ids.append(lot_scope_id)
                seen_page_lotto_scope_ids.add(lot_scope_id)
                label = f"Lotto {'Unico' if str(lot_token).lower() == 'unico' else lot_token}"
                _register_scope_detection(
                    state,
                    scope_id=lot_scope_id,
                    scope_type="lotto",
                    parent_scope_id="document_root",
                    label=label,
                    page=page_number,
                    quote=line,
                    ownership_method="heading_match",
                )
                discovered_scopes.append({"scope_id": lot_scope_id, "page": page_number, "label": label})
            if len(line_lotto_scope_ids) == 1:
                page_lotto_scope_id = line_lotto_scope_ids[0]
            elif len(line_lotto_scope_ids) > 1:
                # Inline multi-lot summary lines should materialize all lot scopes,
                # but they are too ambiguous to serve as bene-parent context.
                page_lotto_scope_id = None
            bene_match = _BENE_HEADING_RE.search(line)
            if bene_match:
                bene_no = bene_match.group(1)
                bene_scope_id = f"bene:{bene_no}"
                parent_scope_id = page_lotto_scope_id or carry_lotto_scope_id
                if parent_scope_id:
                    ownership_method = "nearest_lotto_heading" if page_lotto_scope_id else "previous_lotto_title_page"
                else:
                    known_lotto_scope_ids = {
                        scope_id
                        for scope_id, scope in state.scopes.items()
                        if scope.scope_type == "lotto"
                    }
                    if len(known_lotto_scope_ids) > 1:
                        continue
                    parent_scope_id = "document_root"
                    ownership_method = "weak_document_root_fallback"
                _register_scope_detection(
                    state,
                    scope_id=bene_scope_id,
                    scope_type="bene",
                    parent_scope_id=parent_scope_id,
                    label=f"Bene {bene_no}",
                    page=page_number,
                    quote=line,
                    ownership_method=ownership_method,
                )
                scope = state.scopes[bene_scope_id]
                if ownership_method == "weak_document_root_fallback":
                    scope.metadata["ownership_strength"] = "weak"
                else:
                    scope.metadata["ownership_strength"] = "strong"
                discovered_scopes.append({"scope_id": bene_scope_id, "page": page_number, "label": f"Bene {bene_no}"})
        if page_is_dedicated_lotto_title_page:
            pending_strong_lotto_scope_id = next(iter(page_lotto_ids))
    state.canonical_case.identity["page_sections"] = page_sections
    state.canonical_case.identity["discovered_scopes"] = discovered_scopes
