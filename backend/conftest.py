"""Project-wide pytest safety net: tests can never touch the production DB.

Rationale (found the hard way, 2026-07-20): no conftest existed, so the
correctness_v2 tests drove the real orchestrator, whose telemetry emitter built
a real pymongo client from MONGO_URL/DB_NAME and wrote 187 synthetic test events
straight into the PRODUCTION ``periziascan.v2_job_events`` collection.

Three independent layers, so no single mistake can reach production again:

1. **Name redirection (import time, before ``server`` is imported).** ``DB_NAME``
   is rewritten to a test-only database. ``server.py`` calls ``load_dotenv``,
   which does NOT override an already-set environment variable, so this wins.
   Anything that slips past the other layers writes to a scratch DB, not prod.
2. **Fail-fast assertions.** If the effective test database name ever resolves
   to the production name — or the full (URI, database) pair matches production
   — collection aborts immediately with a loud error.
3. **Telemetry sink isolation (autouse).** Every test starts with the telemetry
   emitter routed to a throwaway in-memory sink and the queue worker reset, so a
   real pymongo client is never constructed and no queued event can outlive a
   test into production.

A test that wants to assert on telemetry installs its own override (the
beta_program fakes and the telemetry tests do exactly that); these layers only
guarantee the *default* is never production.
"""

import os
import pathlib
import tempfile

import pytest
from dotenv import dotenv_values

# Pin feature flags before any test can import ``server``.  ``server`` loads
# backend/.env without overriding existing values, so this keeps the suite
# independent from mutable production rollout configuration.  Tests that need
# another state use function-scoped ``monkeypatch.setenv``.
TEST_FEATURE_FLAG_DEFAULTS = {
    "CORRECTNESS_V2_CANONICAL_VERDICT_ENABLED": "true",
    "CORRECTNESS_V2_PARTIAL_LOT_REPORTS_ENABLED": "false",
    "CORRECTNESS_V2_PDF_RETENTION_ENABLED": "false",
}


def _pin_test_feature_flags():
    os.environ.update(TEST_FEATURE_FLAG_DEFAULTS)


_pin_test_feature_flags()

_BACKEND_DIR = pathlib.Path(__file__).resolve().parent
_ENV_FILE = _BACKEND_DIR / ".env"
_ENV = dotenv_values(_ENV_FILE) if _ENV_FILE.exists() else {}

# The production identity we must never touch from a test.
PRODUCTION_DB_NAME = (_ENV.get("DB_NAME") or "").strip()
PRODUCTION_MONGO_URL = (_ENV.get("MONGO_URL") or "").strip()

TEST_DB_NAME = f"test_pytest_{PRODUCTION_DB_NAME or 'perizia'}"

# R3-05: force every backend pytest process onto a unique scratch artifact
# root before application modules can import correctness_v2.artifacts.
PRODUCTION_ARTIFACT_JOBS_ROOT = pathlib.Path("/srv/perizia/app/_correctness_v2/jobs")
TEST_ARTIFACT_ROOT = pathlib.Path(tempfile.mkdtemp(prefix="perizia_pytest_artifacts_"))
os.environ["PERIZIA_PYTEST_ACTIVE"] = "1"
os.environ["CORRECTNESS_V2_ARTIFACTS_ROOT"] = str(TEST_ARTIFACT_ROOT)


def assert_not_production_artifacts(root):
    resolved = pathlib.Path(root).resolve()
    resolved_jobs = (resolved / "jobs").resolve()
    if resolved == PRODUCTION_ARTIFACT_JOBS_ROOT or resolved_jobs == PRODUCTION_ARTIFACT_JOBS_ROOT:
        raise RuntimeError(
            "REFUSING TO RUN: tests resolved Correctness V2 artifacts to the production jobs root"
        )


assert_not_production_artifacts(os.environ["CORRECTNESS_V2_ARTIFACTS_ROOT"])


def assert_not_production(db_name, mongo_url=None):
    """Raise if (db_name[, mongo_url]) identifies the production database."""
    if PRODUCTION_DB_NAME and db_name == PRODUCTION_DB_NAME:
        raise RuntimeError(
            "REFUSING TO RUN: tests resolved to the PRODUCTION database "
            f"{db_name!r}. Tests must never target production."
        )
    if (
        mongo_url is not None
        and PRODUCTION_MONGO_URL
        and mongo_url == PRODUCTION_MONGO_URL
        and db_name == PRODUCTION_DB_NAME
    ):
        raise RuntimeError(
            "REFUSING TO RUN: tests resolved to the production "
            "(MONGO_URL, DB_NAME) pair."
        )


# --- layer 2, evaluated at import/collection time ---------------------------
assert_not_production(TEST_DB_NAME)

# --- layer 1, before any test imports `server` ------------------------------
os.environ["DB_NAME"] = TEST_DB_NAME
os.environ.setdefault(
    "MONGO_URL", PRODUCTION_MONGO_URL or "mongodb://127.0.0.1:27017"
)


class _NullEventsCollection:
    """Swallows telemetry writes; keeps them in memory for debugging."""

    def __init__(self):
        self.items = []

    def update_one(self, filt, update, upsert=False):
        self.items.append(dict(update.get("$setOnInsert", {})))

    def create_index(self, *a, **k):
        return None


def _isolate_telemetry():
    from beta_program import signals as beta_signals

    # Stop any worker left over from the previous test before swapping the sink,
    # so a late write can never land in the next test's collection.
    beta_signals.reset_for_tests()
    beta_signals.set_events_collection_override(_NullEventsCollection())
    return beta_signals


@pytest.fixture(autouse=True)
def _no_real_telemetry_writes():
    """Never let a test write telemetry to a real Mongo. Applies to ALL tests."""
    beta_signals = _isolate_telemetry()
    try:
        yield
    finally:
        # Drain into the in-memory sink, then reset, so nothing queued by this
        # test can be written later — including at interpreter shutdown.
        beta_signals.flush(2.0)
        _isolate_telemetry()


def pytest_sessionstart(session):
    """Final guard: the effective runtime config must not be production."""
    _pin_test_feature_flags()
    assert_not_production(os.environ.get("DB_NAME"), os.environ.get("MONGO_URL"))
    assert_not_production_artifacts(os.environ.get("CORRECTNESS_V2_ARTIFACTS_ROOT"))


def pytest_sessionfinish(session, exitstatus):
    """Leave no worker holding queued test events at process exit."""
    try:
        _isolate_telemetry()
    except Exception:  # pragma: no cover - teardown tolerance
        pass
