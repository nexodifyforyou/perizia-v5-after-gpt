"""
Beta program — the v2_job_events telemetry transport must be bounded,
best-effort and NON-BLOCKING.

Owner-mandated invariant: operational telemetry may never fail an analysis,
delay it materially, alter a job status, alter pipeline output, or alter billing
or credits. Losing a telemetry event is always preferable to delaying a
customer's analysis.

These tests exercise the real queue/worker transport (no fake short-circuit):
they block, break and saturate Mongo and assert the pipeline is unaffected.
"""

import json
import threading
import time

import pytest

import beta_program_fakes as fk  # noqa: F401  (sets sys.path)
from beta_program import signals as beta_signals

# An emit is a dict build + put_nowait. This ceiling is ~100x that cost and is
# still two orders of magnitude below any Mongo round trip, so it fails loudly
# if I/O ever creeps back onto the caller's thread.
MAX_EMIT_SECONDS = 0.05


# Every double built by a test, so teardown can unblock a stalled worker.
_LIVE_COLLECTIONS = []


@pytest.fixture()
def clean_telemetry():
    _drain_workers()
    beta_signals._analysis_user_cache.clear()
    yield
    _drain_workers()


def _drain_workers():
    """Release any simulated stall, stop the worker, and wait for it to die.

    A worker parked on a simulated hang must never leak into the next test: it
    would write that test's events into that test's collection.
    """
    for coll in _LIVE_COLLECTIONS:
        coll.release.set()
    _LIVE_COLLECTIONS.clear()
    beta_signals.reset_for_tests()
    # NB: never reset the override to None here — the conftest safety net owns
    # the default, and None would mean "build a real pymongo client".
    _await_no_workers()


class RecordingCollection:
    """Collection double that records upserts and honours $setOnInsert."""

    def __init__(self):
        self.items = []
        self.calls = 0
        self.lock = threading.Lock()
        # Set by teardown to abandon any simulated stall immediately.
        self.release = threading.Event()
        _LIVE_COLLECTIONS.append(self)

    def update_one(self, filt, update, upsert=False):
        with self.lock:
            self.calls += 1
            eid = filt.get("event_id")
            if any(d.get("event_id") == eid for d in self.items):
                return
            self.items.append(dict(update.get("$setOnInsert", {})))

    def create_index(self, *a, **k):
        return None


class HangingCollection(RecordingCollection):
    """Simulates a Mongo whose writes hang for `delay` seconds."""

    def __init__(self, delay):
        super().__init__()
        self.delay = delay

    def update_one(self, filt, update, upsert=False):
        # Interruptible sleep: teardown must not wait out the full delay.
        self.release.wait(timeout=self.delay)
        super().update_one(filt, update, upsert=upsert)


class UnavailableCollection(RecordingCollection):
    """Simulates an unreachable Mongo: every write raises."""

    def update_one(self, filt, update, upsert=False):
        self.calls += 1
        raise RuntimeError("mongo unavailable (simulated)")


class BlockedCollection(RecordingCollection):
    """Write blocks until released — used to saturate the bounded queue."""

    def update_one(self, filt, update, upsert=False):
        self.release.wait(timeout=30)
        super().update_one(filt, update, upsert=upsert)


def _worker_threads():
    return [t for t in threading.enumerate() if t.name == "v2-telemetry-writer"]


def _await_no_workers(timeout=5.0):
    deadline = time.monotonic() + timeout
    while _worker_threads() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert not _worker_threads(), "a telemetry worker outlived its test"


def _emit(job_id, event_type=None):
    beta_signals.emit_v2_job_event(
        event_type or beta_signals.EVENT_REPORT_READY,
        job_id=job_id,
        analysis_id="a1",
        user_id="u1",
    )


def _time_emit(job_id):
    start = time.perf_counter()
    _emit(job_id)
    return time.perf_counter() - start


# --- 1. a hanging Mongo must not delay the pipeline call --------------------
def test_hanging_mongo_does_not_delay_emit(clean_telemetry):
    """A write blocked for seconds must not be visible in the emit latency."""
    coll = HangingCollection(delay=3.0)
    beta_signals.set_events_collection_override(coll)

    elapsed = [_time_emit(f"cv2_hang_{i}") for i in range(20)]

    worst = max(elapsed)
    assert worst < MAX_EMIT_SECONDS, f"emit blocked for {worst:.4f}s"
    # The worker really is still stuck on the hanging write.
    assert not beta_signals.flush(0.2)


# --- 2. unavailable Mongo must not fail or change the job -------------------
def test_unavailable_mongo_does_not_fail_or_delay(clean_telemetry):
    coll = UnavailableCollection()
    beta_signals.set_events_collection_override(coll)

    elapsed = [_time_emit(f"cv2_down_{i}") for i in range(50)]

    assert max(elapsed) < MAX_EMIT_SECONDS
    assert beta_signals.flush(5.0)
    stats = beta_signals.telemetry_stats()
    assert stats["failed"] == 50      # every write failed, and was swallowed
    assert stats["written"] == 0
    assert coll.items == []


def test_unavailable_mongo_leaves_job_status_authoritative(clean_telemetry, tmp_path, monkeypatch):
    """The job_status.json artifact is written and unchanged when Mongo is down."""
    monkeypatch.setenv("CORRECTNESS_V2_ARTIFACTS_ROOT", str(tmp_path))
    from correctness_v2 import orchestrator

    beta_signals.set_events_collection_override(UnavailableCollection())
    payload = {"status": "REPORT_READY", "analysis_id": "a1", "job_id": "cv2_auth"}

    path = orchestrator._save_job_status("cv2_auth", dict(payload))

    written = json.loads(open(path).read())
    assert written["status"] == "REPORT_READY"
    assert written["analysis_id"] == "a1"
    assert beta_signals.flush(5.0)
    assert beta_signals.telemetry_stats()["written"] == 0


# --- 3. a full queue drops safely, without blocking -------------------------
def test_full_queue_drops_without_blocking(clean_telemetry, monkeypatch):
    monkeypatch.setattr(beta_signals, "_QUEUE_MAX", 8)
    beta_signals.reset_for_tests()
    coll = BlockedCollection()
    beta_signals.set_events_collection_override(coll)
    try:
        # First emit is picked up by the worker and blocks there; the next 8
        # fill the bounded queue; everything after that must be dropped.
        elapsed = [_time_emit(f"cv2_full_{i}") for i in range(200)]

        assert max(elapsed) < MAX_EMIT_SECONDS
        stats = beta_signals.telemetry_stats()
        assert stats["dropped"] > 0
        assert stats["queued"] <= 8
        # Nothing vanishes silently: every event is either queued, written, or
        # explicitly counted as dropped (±1 for the one in the worker's hand,
        # which qsize() no longer reports).
        accounted = stats["dropped"] + stats["queued"] + stats["written"]
        assert 199 <= accounted <= 200
    finally:
        coll.release.set()



# --- 4. duplicate event_id stays idempotent ---------------------------------
def test_duplicate_event_id_is_idempotent(clean_telemetry):
    coll = RecordingCollection()
    beta_signals.set_events_collection_override(coll)

    for _ in range(25):
        _emit("cv2_dup")

    assert beta_signals.flush(5.0)
    assert len(coll.items) == 1
    assert coll.items[0]["event_id"] == beta_signals._event_id(
        "cv2_dup", None, beta_signals.EVENT_REPORT_READY
    )


# --- 5. no unbounded threads / executors ------------------------------------
def test_emits_create_exactly_one_worker_thread(clean_telemetry):
    coll = RecordingCollection()
    beta_signals.set_events_collection_override(coll)
    before = threading.active_count()

    for i in range(500):
        _emit(f"cv2_thread_{i}")

    assert beta_signals.flush(10.0)
    assert threading.active_count() - before == 1
    workers = _worker_threads()
    assert len(workers) == 1
    assert workers[0].daemon is True
    assert len(coll.items) == 500


# --- 6. pipeline output identical with telemetry on and off -----------------
def test_pipeline_output_identical_with_telemetry_enabled_and_disabled(
    clean_telemetry, tmp_path, monkeypatch
):
    monkeypatch.setenv("CORRECTNESS_V2_ARTIFACTS_ROOT", str(tmp_path))
    from correctness_v2 import orchestrator

    payload = {
        "status": "NEEDS_MANUAL_REVIEW",
        "analysis_id": "a1",
        "job_id": "j",
        "reason_code": "X",
        "selected_lot_id": "L1",
    }

    coll = RecordingCollection()
    beta_signals.set_events_collection_override(coll)
    enabled_path = orchestrator._save_job_status("cv2_on", dict(payload))
    enabled_bytes = open(enabled_path, "rb").read()
    assert beta_signals.flush(5.0)
    assert coll.items, "telemetry was expected to be enabled here"

    # Telemetry fully disabled: the emitter becomes a no-op.
    monkeypatch.setattr(beta_signals, "emit_v2_job_event", lambda *a, **k: None)
    disabled_path = orchestrator._save_job_status("cv2_off", dict(payload))
    disabled_bytes = open(disabled_path, "rb").read()

    # _saved_at is the only legitimate difference between the two writes.
    def _normalize(raw):
        doc = json.loads(raw)
        doc.pop("_saved_at", None)
        return doc

    assert _normalize(enabled_bytes) == _normalize(disabled_bytes)


# --- emit latency is bounded under every Mongo condition --------------------
@pytest.mark.parametrize(
    "collection_factory",
    [
        lambda: RecordingCollection(),
        lambda: UnavailableCollection(),
        lambda: HangingCollection(delay=2.0),
    ],
    ids=["healthy", "unavailable", "hanging"],
)
def test_emit_latency_bounded_under_all_mongo_conditions(clean_telemetry, collection_factory):
    beta_signals.set_events_collection_override(collection_factory())
    elapsed = [_time_emit(f"cv2_lat_{i}") for i in range(100)]
    assert max(elapsed) < MAX_EMIT_SECONDS


# --- safety net: a test must never reach a real Mongo -----------------------
def test_default_test_telemetry_target_is_never_a_real_collection():
    """The autouse conftest fixture must keep the default sink in-memory.

    Regression guard: with no override, the emitter builds a real pymongo client
    from MONGO_URL/DB_NAME, which previously wrote test events into the
    production database.
    """
    target = beta_signals._events_collection()
    assert target is not None
    assert type(target).__module__ != "pymongo.collection"
    assert not hasattr(target, "full_name")
