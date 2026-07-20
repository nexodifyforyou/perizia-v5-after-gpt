"""
Hard guarantees that the test suite can never touch the production database.

These are deliberately paranoid: the suite DID write 187 synthetic telemetry
events into production `periziascan.v2_job_events` before the conftest safety
net existed. Developer discipline is not an acceptable control — these tests
fail loudly if any layer of the isolation is removed.
"""

import os
import sys

import pytest
from dotenv import dotenv_values

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import conftest as backend_conftest  # noqa: E402
from beta_program import signals as beta_signals  # noqa: E402


def _production_identity():
    env = dotenv_values(
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    )
    return (env.get("MONGO_URL") or "").strip(), (env.get("DB_NAME") or "").strip()


# --- 4/5. env cannot silently point tests at production ---------------------
def test_effective_db_name_is_not_production():
    _, prod_db = _production_identity()
    assert prod_db, "production DB_NAME must be readable for this guard to mean anything"
    assert os.environ["DB_NAME"] != prod_db
    assert os.environ["DB_NAME"] == backend_conftest.TEST_DB_NAME


def test_server_module_is_bound_to_the_test_database():
    """Even the app object imported by tests points at the scratch DB."""
    import server

    _, prod_db = _production_identity()
    assert server.db.name != prod_db
    assert server.db.name == backend_conftest.TEST_DB_NAME


def test_guard_rejects_the_production_database_name():
    _, prod_db = _production_identity()
    with pytest.raises(RuntimeError, match="PRODUCTION database"):
        backend_conftest.assert_not_production(prod_db)


def test_guard_rejects_the_production_uri_and_database_pair():
    prod_url, prod_db = _production_identity()
    with pytest.raises(RuntimeError):
        backend_conftest.assert_not_production(prod_db, prod_url)


# --- 3. telemetry sink is isolated, never a real collection -----------------
def test_default_telemetry_sink_is_isolated():
    target = beta_signals._events_collection()
    assert type(target).__module__ != "pymongo.collection"
    assert not hasattr(target, "full_name")
    assert isinstance(target, backend_conftest._NullEventsCollection)


def test_emitting_never_constructs_a_real_mongo_client():
    """The lazy pymongo client must stay unbuilt for the whole session."""
    for i in range(50):
        beta_signals.emit_v2_job_event(
            beta_signals.EVENT_REPORT_READY,
            job_id=f"iso_{i}",
            analysis_id="iso_analysis",
            user_id="iso_user",
        )
    assert beta_signals.flush(5.0)
    assert beta_signals._sync_client is None, "a real pymongo client was constructed"


# --- 6. the worker is reset cleanly between tests ---------------------------
def test_worker_state_is_clean_at_test_start():
    """Counters start at zero: the previous test's worker did not leak in."""
    stats = beta_signals.telemetry_stats()
    assert stats["written"] == 0
    assert stats["dropped"] == 0
    assert stats["failed"] == 0


def test_no_worker_thread_survives_between_tests():
    import threading

    # The autouse fixture resets before each test, so at this point any worker
    # is one this test's own module-level activity started — there must be at
    # most one, and it must be the single named daemon.
    workers = [t for t in threading.enumerate() if t.name == "v2-telemetry-writer"]
    assert len(workers) <= 1
    for worker in workers:
        assert worker.daemon is True


# --- 1/2/7. nothing reaches the production collection -----------------------
def _prod_counts():
    """Read-only snapshot of the production collections tests could touch."""
    from pymongo import MongoClient

    prod_url, prod_db = _production_identity()
    client = MongoClient(prod_url, serverSelectionTimeoutMS=3000)
    try:
        db = client[prod_db]
        return {
            name: db[name].count_documents({})
            for name in (
                "v2_job_events",
                "users",
                "perizia_analyses",
                "beta_program_memberships",
                "beta_program_audit",
                "beta_feedback",
                "credit_ledger",
            )
        }
    finally:
        client.close()


@pytest.mark.parametrize("shutdown_after", [False, True])
def test_emitting_and_shutdown_write_nothing_to_production(shutdown_after):
    """Emits, flushes and even a full telemetry shutdown leave production alone.

    Covers isolation requirement 7: test shutdown must not flush queued test
    events into the production collection.
    """
    before = _prod_counts()

    for i in range(25):
        beta_signals.emit_v2_job_event(
            beta_signals.EVENT_VERIFICATION_REQUIRED,
            job_id=f"prodguard_{shutdown_after}_{i}",
            analysis_id="prodguard_analysis",
            user_id="prodguard_user",
        )
    if shutdown_after:
        beta_signals.shutdown(timeout=2.0)
    else:
        assert beta_signals.flush(5.0)

    assert _prod_counts() == before
