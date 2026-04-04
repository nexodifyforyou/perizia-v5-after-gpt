from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from typing import Any, Dict, List, Optional


@dataclass
class EvidenceSpan:
    page: int
    quote: str
    section_type: str = "unknown"
    semantic_role: str = "unknown"
    confidence: float = 0.0
    valid_fields: List[str] = field(default_factory=list)
    source: str = "pages"
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Candidate:
    value: Any
    field_key: str
    confidence: float
    evidence: List[EvidenceSpan] = field(default_factory=list)
    section_type: str = "unknown"
    semantic_role: str = "unknown"
    valid: bool = True
    invalid_reason: Optional[str] = None
    source: str = "unknown"
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Judgment:
    field_key: str
    value: Any
    status: str
    confidence: float
    evidence: List[EvidenceSpan] = field(default_factory=list)
    rationale: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CanonicalIssue:
    code: str
    title_it: str
    severity: str
    category: str
    priority_score: float
    evidence: List[EvidenceSpan] = field(default_factory=list)
    summary_it: str = ""
    action_it: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CanonicalCaseState:
    identity: Dict[str, Any] = field(default_factory=dict)
    rights: Dict[str, Any] = field(default_factory=dict)
    occupancy: Dict[str, Any] = field(default_factory=dict)
    urbanistica: Dict[str, Any] = field(default_factory=dict)
    catasto: Dict[str, Any] = field(default_factory=dict)
    agibilita: Dict[str, Any] = field(default_factory=dict)
    impianti: Dict[str, Any] = field(default_factory=dict)
    legal: Dict[str, Any] = field(default_factory=dict)
    costs: Dict[str, Any] = field(default_factory=dict)
    pricing: Dict[str, Any] = field(default_factory=dict)
    priority: Dict[str, Any] = field(default_factory=dict)
    summary_bundle: Dict[str, Any] = field(default_factory=dict)
    qa: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RuntimeState:
    analysis_id: str
    result: Dict[str, Any]
    pages: List[Dict[str, Any]]
    full_text: str
    candidates: Dict[str, List[Candidate]] = field(default_factory=dict)
    judgments: Dict[str, Judgment] = field(default_factory=dict)
    issues: List[CanonicalIssue] = field(default_factory=list)
    canonical_case: CanonicalCaseState = field(default_factory=CanonicalCaseState)
    metrics: Dict[str, Any] = field(default_factory=dict)
    qa_checks: List[Dict[str, Any]] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)


def to_dict(value: Any) -> Any:
    if is_dataclass(value):
        return {k: to_dict(v) for k, v in asdict(value).items()}
    if isinstance(value, dict):
        return {k: to_dict(v) for k, v in value.items()}
    if isinstance(value, list):
        return [to_dict(v) for v in value]
    return value

