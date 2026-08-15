"""Shared test setup for Correctness Mode v2 tests.

Puts the backend dir on sys.path so ``import correctness_v2`` works, and provides
a temp artifacts root so tests never write to the real /srv artifacts folder.
No DB / OpenAI / Gemini is touched by any test here.
"""

import sys
import os
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


@pytest.fixture(scope="session", autouse=True)
def _r305_isolated_artifact_root(tmp_path_factory):
    root = tmp_path_factory.mktemp("correctness_v2_artifacts")
    production = Path("/srv/perizia/app/_correctness_v2/jobs")
    if root.resolve() == production or (root / "jobs").resolve() == production:
        pytest.fail("R3-05: test artifact root resolved to production before any write")
    previous = os.environ.get("CORRECTNESS_V2_ARTIFACTS_ROOT")
    os.environ["CORRECTNESS_V2_ARTIFACTS_ROOT"] = str(root)
    os.environ["PERIZIA_PYTEST_ACTIVE"] = "1"
    try:
        yield root
    finally:
        if previous is None:
            os.environ.pop("CORRECTNESS_V2_ARTIFACTS_ROOT", None)
        else:
            os.environ["CORRECTNESS_V2_ARTIFACTS_ROOT"] = previous
