from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

from .contracts import CorpusCase

WORKING_CORPUS_REGISTRY = Path("/srv/perizia/_qa/canonical_pipeline/working_corpus_registry.json")


def load_cases() -> List[CorpusCase]:
    data = json.loads(WORKING_CORPUS_REGISTRY.read_text(encoding="utf-8"))
    return [CorpusCase(**row) for row in data]


def get_case(case_key: str) -> CorpusCase:
    for row in load_cases():
        if row.case_key == case_key:
            return row
    raise KeyError(f"Unknown case_key: {case_key}")


def list_case_keys() -> List[str]:
    return [row.case_key for row in load_cases()]


def as_dict() -> Dict[str, CorpusCase]:
    return {row.case_key: row for row in load_cases()}
