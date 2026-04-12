from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional


class GateOutcome(str, Enum):
    PASS = "PASS"
    LOOP = "LOOP"
    TERMINAL = "TERMINAL"


class ArtifactName(str, Enum):
    MANIFEST = "manifest.json"
    EXTRACTION_QUALITY = "extraction_quality.json"
    STRUCTURE_HYPOTHESES = "structure_hypotheses.json"
    PLURALITY_SIGNALS = "plurality_signals.json"
    EVIDENCE_LEDGER = "evidence_ledger.json"
    AMBIGUITY_QUEUE = "ambiguity_queue.json"
    ADJUDICATIONS = "adjudications.json"
    PRECANON_QA = "precanon_qa.json"
    DOC_MAP = "doc_map.json"
    PROVISIONAL_RESULT = "provisional_result.json"
    QA_VERDICT = "qa_verdict.json"
    RETRY_REQUESTS = "retry_requests.json"
    BLOCK_REPORT = "block_report.json"
    PUBLISH_PACKAGE = "publish_package.json"
    PARITY_REPORT = "parity_report.json"
    SYNTHESIS_OUTPUT = "synthesis_output.json"


@dataclass
class CorpusCase:
    case_key: str
    label: str
    pdf_path: str
    sha256: str
    size_bytes: int
    analysis_id: Optional[str] = None
    case_id: Optional[str] = None


@dataclass
class PipelinePaths:
    backend_root: Path
    corpus_registry_path: Path
    pipeline_root: Path
    artifact_root: Path


@dataclass
class PipelineContext:
    case: CorpusCase
    paths: PipelinePaths
    run_dir: Path
    artifact_dir: Path
    notes: Dict[str, Any] = field(default_factory=dict)
