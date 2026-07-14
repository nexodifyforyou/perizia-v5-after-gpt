"""Unit tests for the bounded concurrent per-lot runner (correctness_v2.lot_runner).

These test the SCHEDULER in isolation (no OpenAI, no artifacts): concurrency
ceiling, deterministic result mapping under out-of-order completion, transient
retry with bounded backoff, deterministic-failure no-retry, rate-limit
degradation to serial, duplicate suppression, and completed-lot reuse.
"""

import threading
import time

import pytest

from correctness_v2 import lot_runner
from correctness_v2.openai_client import (
    REASON_RATE_LIMITED,
    REASON_SERVER_ERROR,
    OpenAIClientError,
)


def _noop_sleep(_seconds):
    return None


# --- concurrency resolution / clamp ---------------------------------------
def test_resolve_concurrency_clamps_to_ceiling(monkeypatch):
    monkeypatch.delenv(lot_runner.CONCURRENCY_ENV, raising=False)
    assert lot_runner.resolve_concurrency() == 1
    monkeypatch.setenv(lot_runner.CONCURRENCY_ENV, "2")
    assert lot_runner.resolve_concurrency() == 2
    monkeypatch.setenv(lot_runner.CONCURRENCY_ENV, "3")
    assert lot_runner.resolve_concurrency() == 3
    # Above the hard ceiling -> clamped down, never honored blindly.
    monkeypatch.setenv(lot_runner.CONCURRENCY_ENV, "9")
    assert lot_runner.resolve_concurrency() == lot_runner.MAX_CONCURRENCY == 3
    # Invalid / sub-1 -> serial.
    monkeypatch.setenv(lot_runner.CONCURRENCY_ENV, "0")
    assert lot_runner.resolve_concurrency() == 1
    monkeypatch.setenv(lot_runner.CONCURRENCY_ENV, "nope")
    assert lot_runner.resolve_concurrency() == 1


# --- (1) concurrency ceiling ----------------------------------------------
def _concurrency_probe():
    """A call_fn that records the max number of simultaneously-active calls."""
    lock = threading.Lock()
    state = {"active": 0, "max": 0}

    def _call(key):
        with lock:
            state["active"] += 1
            state["max"] = max(state["max"], state["active"])
        time.sleep(0.05)  # widen the overlap window
        with lock:
            state["active"] -= 1
        return f"result-{key}"

    _call.state = state  # type: ignore[attr-defined]
    return _call


def test_concurrency_ceiling_two():
    call = _concurrency_probe()
    keys = ["1", "2", "3", "4", "5", "6"]
    report = lot_runner.run_lot_batch(keys, call, analysis_id="A", concurrency=2)
    assert call.state["max"] <= 2
    assert report.max_active <= 2
    assert all(report.outcomes[k].ok for k in keys)


# --- (2) serial compatibility ---------------------------------------------
def test_concurrency_one_is_serial():
    call = _concurrency_probe()
    keys = ["1", "2", "3", "4"]
    report = lot_runner.run_lot_batch(keys, call, analysis_id="A", concurrency=1)
    assert call.state["max"] == 1
    assert report.max_active == 1
    assert report.degraded_to_serial is False


# --- (3)+(4) out-of-order completion, deterministic result mapping ----------
def test_results_map_to_correct_key_under_out_of_order_completion():
    # Lot "1" is the slowest; lot "3" finishes first. Each result must still map
    # back to its own key (no cross-lot swap under concurrency).
    delays = {"1": 0.15, "2": 0.08, "3": 0.01}

    def _call(key):
        time.sleep(delays[key])
        return {"lot": key, "value": f"payload-{key}"}

    report = lot_runner.run_lot_batch(["1", "2", "3"], _call, analysis_id="A", concurrency=3)
    for key in ("1", "2", "3"):
        assert report.outcomes[key].result == {"lot": key, "value": f"payload-{key}"}
        assert report.result_or_exc(key) == {"lot": key, "value": f"payload-{key}"}


# --- (7) transient retry then success --------------------------------------
def test_transient_failure_is_retried_then_succeeds():
    attempts = {"1": 0}

    def _call(key):
        attempts[key] += 1
        if attempts[key] <= 2:  # fail twice, succeed on the third
            raise OpenAIClientError("boom", reason_code=REASON_SERVER_ERROR)
        return "ok"

    report = lot_runner.run_lot_batch(
        ["1"], _call, analysis_id="A", concurrency=1,
        sleep_fn=_noop_sleep, backoff_base=0.0, backoff_jitter=0.0,
    )
    out = report.outcomes["1"]
    assert out.ok
    assert out.result == "ok"
    assert out.attempts == 3
    assert out.retry_count == 2
    assert report.total_retries == 2


def test_transient_failure_retries_are_bounded():
    def _call(key):
        raise OpenAIClientError("always", reason_code=REASON_SERVER_ERROR)

    report = lot_runner.run_lot_batch(
        ["1"], _call, analysis_id="A", concurrency=1,
        sleep_fn=_noop_sleep, backoff_base=0.0, backoff_jitter=0.0,
    )
    out = report.outcomes["1"]
    assert not out.ok
    # MAX_RETRIES retries == MAX_RETRIES + 1 total attempts.
    assert out.attempts == lot_runner.MAX_RETRIES + 1
    assert out.retry_count == lot_runner.MAX_RETRIES
    assert isinstance(out.error, OpenAIClientError)


# --- (deterministic failures are NOT retried) ------------------------------
def test_deterministic_failure_is_not_retried():
    attempts = {"1": 0}

    def _call(key):
        attempts[key] += 1
        raise OpenAIClientError("bad request", reason_code="OPENAI_CALL_FAILED")

    report = lot_runner.run_lot_batch(
        ["1"], _call, analysis_id="A", concurrency=1, sleep_fn=_noop_sleep
    )
    out = report.outcomes["1"]
    assert not out.ok
    assert out.attempts == 1
    assert out.retry_count == 0
    assert attempts["1"] == 1


# --- (8) 429 degradation to serial -----------------------------------------
def test_rate_limit_degrades_to_serial():
    # Every lot is rate-limited on its FIRST attempt (which sets the degrade
    # flag), then succeeds. We measure concurrency ONLY among the successful
    # (post-degrade) calls: after degradation they must run strictly serially.
    lock = threading.Lock()
    state = {"succ_active": 0, "succ_max": 0}
    attempts = {}

    def _call(key):
        n = attempts.get(key, 0) + 1
        attempts[key] = n
        if n == 1:
            raise OpenAIClientError("429", reason_code=REASON_RATE_LIMITED)
        with lock:
            state["succ_active"] += 1
            state["succ_max"] = max(state["succ_max"], state["succ_active"])
        time.sleep(0.03)
        with lock:
            state["succ_active"] -= 1
        return f"ok-{key}"

    report = lot_runner.run_lot_batch(
        ["1", "2", "3", "4"], _call, analysis_id="A", concurrency=3,
        sleep_fn=_noop_sleep, backoff_base=0.0, backoff_jitter=0.0,
    )
    assert report.degraded_to_serial is True
    assert all(report.outcomes[k].ok for k in ("1", "2", "3", "4"))
    # After degradation, successful calls never overlap.
    assert state["succ_max"] == 1


def test_no_rate_limit_no_degradation():
    def _call(key):
        return f"ok-{key}"

    report = lot_runner.run_lot_batch(["1", "2"], _call, analysis_id="A", concurrency=2)
    assert report.degraded_to_serial is False


# --- (5) duplicate suppression ---------------------------------------------
def test_dedup_call_collapses_concurrent_same_key():
    count = {"n": 0}
    started = threading.Event()

    def _call():
        count["n"] += 1
        started.set()
        time.sleep(0.3)  # keep the owner busy while the second caller arrives
        return "shared"

    results = {}

    def _runner(idx):
        results[idx], _ = lot_runner._dedup_call("same-key", _call)

    t1 = threading.Thread(target=_runner, args=(1,))
    t2 = threading.Thread(target=_runner, args=(2,))
    t1.start()
    started.wait(1.0)  # ensure the owner is inside _call before starting t2
    t2.start()
    t1.join()
    t2.join()

    assert count["n"] == 1  # exactly one real call
    assert results[1] == results[2] == "shared"


def test_run_batch_dedups_same_lot_across_concurrent_batches():
    count = {"n": 0}
    lock = threading.Lock()
    inside = threading.Event()

    def _call(key):
        with lock:
            count["n"] += 1
        inside.set()
        time.sleep(0.3)
        return f"ok-{key}"

    reports = {}

    def _run(idx):
        reports[idx] = lot_runner.run_lot_batch(
            ["1"], _call, analysis_id="dup", concurrency=1
        )

    t1 = threading.Thread(target=_run, args=(1,))
    t2 = threading.Thread(target=_run, args=(2,))
    t1.start()
    inside.wait(1.0)
    t2.start()
    t1.join()
    t2.join()

    assert count["n"] == 1  # the duplicate simultaneous request made no 2nd call
    # Both batches got the same result; exactly one is flagged deduplicated.
    assert reports[1].outcomes["1"].result == reports[2].outcomes["1"].result == "ok-1"
    deduped = [reports[1].outcomes["1"].deduplicated, reports[2].outcomes["1"].deduplicated]
    assert deduped.count(True) == 1


# --- (6) completed-lot reuse (no new call) ---------------------------------
def test_reuse_fn_skips_call_for_completed_lot():
    called = []

    def _call(key):
        called.append(key)
        return f"fresh-{key}"

    def _reuse(key):
        return f"reused-{key}" if key == "1" else None

    report = lot_runner.run_lot_batch(
        ["1", "2"], _call, analysis_id="A", concurrency=2, reuse_fn=_reuse
    )
    assert report.outcomes["1"].reused is True
    assert report.outcomes["1"].result == "reused-1"
    assert report.outcomes["2"].reused is False
    assert report.outcomes["2"].result == "fresh-2"
    # Reused lot never triggered a call; only lot 2 did.
    assert called == ["2"]


def test_empty_batch_is_a_noop():
    report = lot_runner.run_lot_batch([], lambda k: k, analysis_id="A", concurrency=2)
    assert report.outcomes == {}
    assert report.max_active == 0
    assert report.degraded_to_serial is False
