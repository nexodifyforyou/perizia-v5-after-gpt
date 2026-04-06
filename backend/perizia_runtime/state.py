from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
import uuid
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
class ScopeEvidenceOwnership:
    evidence_id: str
    scope_id: str
    scope_path: List[str] = field(default_factory=list)
    field_target: str = ""
    source_page: int = 0
    quote: str = ""
    confidence: float = 0.0
    ownership_method: str = "manual"


@dataclass
class CanonicalScopeState:
    scope_id: str
    scope_type: str
    parent_scope_id: Optional[str]
    label: str = ""
    pricing: Dict[str, Any] = field(default_factory=dict)
    agibilita: Dict[str, Any] = field(default_factory=dict)
    occupancy: Dict[str, Any] = field(default_factory=dict)
    legal: Dict[str, Any] = field(default_factory=dict)
    urbanistica: Dict[str, Any] = field(default_factory=dict)
    catasto: Dict[str, Any] = field(default_factory=dict)
    costs: Dict[str, Any] = field(default_factory=dict)
    top_issue: Dict[str, Any] = field(default_factory=dict)
    confidence: Dict[str, Any] = field(default_factory=dict)
    guards: List[str] = field(default_factory=list)
    issues: List[Dict[str, Any]] = field(default_factory=list)
    contradictions: List[Dict[str, Any]] = field(default_factory=list)
    evidence_ids: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


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
    scopes: Dict[str, CanonicalScopeState] = field(default_factory=dict)
    evidence_ownership: Dict[str, ScopeEvidenceOwnership] = field(default_factory=dict)
    metrics: Dict[str, Any] = field(default_factory=dict)
    qa_checks: List[Dict[str, Any]] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.get_or_create_scope("document_root", scope_type="document_root", parent_scope_id=None, label="Document Root")

    def get_or_create_scope(
        self,
        scope_id: str,
        *,
        scope_type: str,
        parent_scope_id: Optional[str],
        label: str = "",
    ) -> CanonicalScopeState:
        scope = self.scopes.get(scope_id)
        if scope is None:
            scope = CanonicalScopeState(
                scope_id=scope_id,
                scope_type=scope_type,
                parent_scope_id=parent_scope_id,
                label=label or scope_id,
            )
            self.scopes[scope_id] = scope
            return scope
        if parent_scope_id and scope.parent_scope_id in {None, "document_root"} and parent_scope_id != scope.parent_scope_id:
            scope.parent_scope_id = parent_scope_id
        if label and not scope.label:
            scope.label = label
        return scope

    def list_child_scopes(self, parent_scope_id: str) -> List[CanonicalScopeState]:
        return [scope for scope in self.scopes.values() if scope.parent_scope_id == parent_scope_id]

    def scope_path(self, scope_id: str) -> List[str]:
        path: List[str] = []
        current = self.scopes.get(scope_id)
        seen = set()
        while current is not None and current.scope_id not in seen:
            seen.add(current.scope_id)
            path.append(current.scope_id)
            if current.parent_scope_id is None:
                break
            current = self.scopes.get(current.parent_scope_id)
        return list(reversed(path))

    def attach_evidence_ownership(
        self,
        *,
        scope_id: str,
        field_target: str,
        source_page: int,
        quote: str,
        confidence: float = 0.0,
        ownership_method: str = "manual",
        evidence_id: Optional[str] = None,
    ) -> ScopeEvidenceOwnership:
        if scope_id not in self.scopes:
            raise KeyError(f"unknown scope_id: {scope_id}")
        evidence_key = evidence_id or f"ev_{uuid.uuid4().hex[:12]}"
        ownership = ScopeEvidenceOwnership(
            evidence_id=evidence_key,
            scope_id=scope_id,
            scope_path=self.scope_path(scope_id),
            field_target=field_target,
            source_page=int(source_page),
            quote=quote,
            confidence=float(confidence),
            ownership_method=ownership_method,
        )
        self.evidence_ownership[evidence_key] = ownership
        scope = self.scopes[scope_id]
        if evidence_key not in scope.evidence_ids:
            scope.evidence_ids.append(evidence_key)
        return ownership


def to_dict(value: Any) -> Any:
    if is_dataclass(value):
        return {k: to_dict(v) for k, v in asdict(value).items()}
    if isinstance(value, dict):
        return {k: to_dict(v) for k, v in value.items()}
    if isinstance(value, list):
        return [to_dict(v) for v in value]
    return value
