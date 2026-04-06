from __future__ import annotations

from typing import Any, Dict, List, Optional

from perizia_runtime.state import EvidenceSpan
from perizia_tools.section_router_tool import classify_section_type


def make_evidence(page: int, quote: str, semantic_role: str, valid_fields: Optional[List[str]] = None, confidence: float = 0.6, source: str = "pages", metadata: Optional[Dict[str, Any]] = None) -> EvidenceSpan:
    return EvidenceSpan(
        page=int(page or 0),
        quote=str(quote or "").strip()[:520],
        section_type=classify_section_type(quote),
        semantic_role=semantic_role,
        confidence=max(0.0, min(1.0, float(confidence))),
        valid_fields=list(valid_fields or []),
        source=source,
        metadata=dict(metadata or {}),
    )

