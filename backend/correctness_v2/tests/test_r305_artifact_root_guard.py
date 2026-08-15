from pathlib import Path

import pytest

from correctness_v2 import artifacts


PRODUCTION_JOBS = Path("/srv/perizia/app/_correctness_v2/jobs")


def test_r305_session_artifact_root_is_isolated_before_writes():
    root = artifacts.artifacts_root().resolve()
    assert root != PRODUCTION_JOBS
    assert (root / "jobs").resolve() != PRODUCTION_JOBS


def test_r305_guard_fails_before_job_directory_write(monkeypatch):
    sentinel = PRODUCTION_JOBS / "r305_guard_must_never_exist"
    assert not sentinel.exists()
    monkeypatch.setenv("PERIZIA_PYTEST_ACTIVE", "1")
    monkeypatch.setenv("CORRECTNESS_V2_ARTIFACTS_ROOT", str(PRODUCTION_JOBS.parent))
    with pytest.raises(RuntimeError, match="REFUSING TEST WRITE"):
        artifacts.ensure_job_dir("r305_guard_must_never_exist")
    assert not sentinel.exists()


def test_r305_direct_writer_guard_rejects_production_path(monkeypatch):
    target = PRODUCTION_JOBS / "r305_guard_direct.json"
    assert not target.exists()
    monkeypatch.setenv("PERIZIA_PYTEST_ACTIVE", "1")
    monkeypatch.setenv("CORRECTNESS_V2_ARTIFACTS_ROOT", str(PRODUCTION_JOBS.parent))
    with pytest.raises(RuntimeError, match="REFUSING TEST WRITE"):
        artifacts._write_json(target, {"synthetic": True})
    assert not target.exists()
