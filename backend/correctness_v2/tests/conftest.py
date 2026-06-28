"""Shared test setup for Correctness Mode v2 tests.

Puts the backend dir on sys.path so ``import correctness_v2`` works, and provides
a temp artifacts root so tests never write to the real /srv artifacts folder.
No DB / OpenAI / Gemini is touched by any test here.
"""

import sys
from pathlib import Path

import pytest

# backend/correctness_v2/tests/conftest.py -> parents[2] == backend
BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))


@pytest.fixture()
def artifacts_root(tmp_path, monkeypatch):
    root = tmp_path / "_correctness_v2"
    monkeypatch.setenv("CORRECTNESS_V2_ARTIFACTS_ROOT", str(root))
    return root
