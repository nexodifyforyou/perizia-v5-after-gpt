"""
Bounded concurrent per-lot analyst runner for Correctness Mode v2.

This module owns the ONLY concurrency in the v2 pipeline. It exists to overlap
the expensive, network-bound per-lot OpenAI analyst calls in ``analyze_all``
WITHOUT changing a single deterministic step. It returns, for each lot, either
an analyst result or the exception that lot hit; the orchestrator then does every
validation / contract / render / ordering step sequentially and identically to
the serial schedule. Concurrency here can therefore never change WHAT is
produced — only how long the batch of network calls takes.

Guarantees (see the mission spec):
  * Bounded concurrency — a fixed worker pool, clamped to [1, MAX_CONCURRENCY].
    No unbounded task creation, no thread explosion.
  * Isolation — a worker only ever touches its own lot key, its own call
    closure, and its own outcome record. No shared mutable analyst state.
  * Idempotency / reuse — an optional ``reuse_fn`` lets an already-completed lot
    be reused with NO new OpenAI call. A process-wide in-flight guard collapses
    duplicate simultaneous requests for the same lot into a single call.
  * Failure handling — transient API failures (429 / timeout / 5xx) are retried
    with bounded exponential backoff + jitter (``MAX_RETRIES`` retries max).
    Deterministic failures (bad request, malformed output, validation) are never
    retried. On rate limiting the batch DEGRADES TO SERIAL: no new concurrent
    call is started, remaining lots run one at a time, completed lots are kept,
    and ``degraded_to_serial`` is reported.
  * Observability — structured, PII-free log lines per lot and per batch.

Nothing here reads or writes artifacts, PDF text, or job status: the runner is a
pure scheduler over caller-supplied closures.
"""

from __future__ import annotations

import logging
import os
import random
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from . import openai_client

logger = logging.getLogger(__name__)

CONCURRENCY_ENV = "CORRECTNESS_V2_LOT_CONCURRENCY"

# Hard safety ceiling. The env var may request less, never more. Chosen to stay
# well within the OpenAI account's concurrency headroom; raise only with proven
# API limits. Invalid / out-of-range values fall back to 1 (serial).
MAX_CONCURRENCY = 3
MIN_CONCURRENCY = 1

# Bounded retries for transient API failures (per lot). Two retries == up to
# three total attempts for that lot.
MAX_RETRIES = 2

# Exponential backoff base (seconds) and jitter fraction. Overridable per call
# for tests so the retry path runs instantly.
DEFAULT_BACKOFF_BASE = 0.5
DEFAULT_BACKOFF_JITTER = 0.25


def resolve_concurrency(default: int = 1) -> int:
    """Resolve the configured per-lot concurrency, clamped to [1, MAX_CONCURRENCY].

    Reads ``CORRECTNESS_V2_LOT_CONCURRENCY``. Invalid / missing / out-of-range
    values fall back to 1 (serial — the validated default). A value above the
    hard ceiling is clamped DOWN to ``MAX_CONCURRENCY`` (never honored blindly),
    a value below 1 falls back to 1. Never raises.
    """
    raw = os.environ.get(CONCURRENCY_ENV)
    if raw is None or not str(raw).strip():
        return max(MIN_CONCURRENCY, min(int(default), MAX_CONCURRENCY))
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return 1
    if value < MIN_CONCURRENCY:
        return 1
    return min(value, MAX_CONCURRENCY)


# ---------------------------------------------------------------------------
# Process-wide in-flight guard: collapse duplicate simultaneous work for the
# same logical key (e.g. a double-clicked "analyze this lot") into ONE call.
# ---------------------------------------------------------------------------
_INFLIGHT_LOCK = threading.Lock()
_INFLIGHT: Dict[str, "_Inflight"] = {}


class _Inflight:
    __slots__ = ("event", "result", "error")

    def __init__(self) -> None:
        self.event = threading.Event()
        self.result: Any = None
        self.error: Optional[BaseException] = None


def _dedup_call(dedup_key: str, call: Callable[[], Any]) -> "tuple[Any, bool]":
    """Run ``call`` at most once per ``dedup_key`` across concurrent callers.

    Returns ``(result, was_deduplicated)``. The first caller for a key owns the
    call; concurrent callers for the same key block on its completion and reuse
    its result (or re-raise its error) WITHOUT issuing a second call.
    """
    with _INFLIGHT_LOCK:
        holder = _INFLIGHT.get(dedup_key)
        owner = holder is None
        if owner:
            holder = _Inflight()
            _INFLIGHT[dedup_key] = holder

    if not owner:
        # Someone else is already doing this exact work — wait and reuse.
        holder.event.wait()
        if holder.error is not None:
            raise holder.error
        return holder.result, True

    try:
        holder.result = call()
    except BaseException as exc:  # noqa: BLE001 — surfaced to this caller too
        holder.error = exc
        raise
    finally:
        with _INFLIGHT_LOCK:
            _INFLIGHT.pop(dedup_key, None)
        holder.event.set()
    return holder.result, False


# ---------------------------------------------------------------------------
# Outcome + batch report
# ---------------------------------------------------------------------------
@dataclass
class LotOutcome:
    """Per-lot result of the concurrent phase. Deterministic-safe: carries either
    a result or an error, never both mattering to the caller's ordered assembly."""

    key: str
    result: Any = None
    error: Optional[BaseException] = None
    reused: bool = False
    deduplicated: bool = False
    attempts: int = 0
    retry_count: int = 0
    queued_at: float = 0.0
    started_at: Optional[float] = None
    completed_at: Optional[float] = None

    @property
    def ok(self) -> bool:
        return self.error is None

    @property
    def duration(self) -> Optional[float]:
        if self.started_at is None or self.completed_at is None:
            return None
        return round(self.completed_at - self.started_at, 4)


@dataclass
class BatchReport:
    batch_id: str
    concurrency: int
    keys: List[str]
    outcomes: Dict[str, LotOutcome] = field(default_factory=dict)
    degraded_to_serial: bool = False
    max_active: int = 0
    total_retries: int = 0

    def result_or_exc(self, key: str) -> Any:
        """Return the AnalystResult for ``key`` or the exception it hit."""
        outcome = self.outcomes.get(key)
        if outcome is None:
            return KeyError(f"no outcome for lot {key}")
        return outcome.error if outcome.error is not None else outcome.result

    def summary(self) -> Dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "concurrency": self.concurrency,
            "lot_count": len(self.keys),
            "degraded_to_serial": self.degraded_to_serial,
            "max_active_concurrency": self.max_active,
            "total_retries": self.total_retries,
            "per_lot": {
                k: {
                    "status": "ok" if o.ok else "error",
                    "reused": o.reused,
                    "deduplicated": o.deduplicated,
                    "attempts": o.attempts,
                    "retry_count": o.retry_count,
                    "duration": o.duration,
                }
                for k, o in self.outcomes.items()
            },
        }


def _default_is_transient(exc: BaseException) -> bool:
    """Default transient classifier: honor an analyst/openai ``reason_code``."""
    return openai_client.is_transient_reason(getattr(exc, "reason_code", None))


def _is_rate_limit(exc: BaseException) -> bool:
    return openai_client.is_rate_limit_reason(getattr(exc, "reason_code", None))


# ---------------------------------------------------------------------------
# The runner
# ---------------------------------------------------------------------------
def run_lot_batch(
    keys: List[str],
    call_fn: Callable[[str], Any],
    *,
    analysis_id: str = "",
    batch_id: Optional[str] = None,
    concurrency: Optional[int] = None,
    reuse_fn: Optional[Callable[[str], Any]] = None,
    is_transient: Callable[[BaseException], bool] = _default_is_transient,
    is_rate_limit: Callable[[BaseException], bool] = _is_rate_limit,
    max_retries: int = MAX_RETRIES,
    backoff_base: float = DEFAULT_BACKOFF_BASE,
    backoff_jitter: float = DEFAULT_BACKOFF_JITTER,
    sleep_fn: Callable[[float], None] = time.sleep,
    rng: Optional[random.Random] = None,
    dedup_key_fn: Optional[Callable[[str], str]] = None,
) -> BatchReport:
    """Run ``call_fn(key)`` for each lot key with bounded concurrency + retries.

    ``call_fn`` must be self-contained and share NO mutable state across lots
    (it builds its own isolated inputs and OpenAI client). It returns whatever
    the caller wants per lot (an ``AnalystResult``) and raises on failure.

    ``reuse_fn(key)`` (optional) returns a cached result to reuse WITHOUT calling
    ``call_fn`` (no OpenAI call) — or ``None`` to proceed normally. It must never
    raise; a raising reuse_fn is treated as "no reuse".

    The returned :class:`BatchReport` maps every key to a :class:`LotOutcome`.
    Ordering of the caller's downstream assembly is the caller's responsibility;
    this function guarantees only that every key gets exactly one outcome.
    """
    batch_id = batch_id or f"batch_{uuid.uuid4().hex[:12]}"
    resolved_conc = concurrency if concurrency is not None else resolve_concurrency()
    resolved_conc = max(MIN_CONCURRENCY, min(int(resolved_conc), MAX_CONCURRENCY))
    rng = rng or random.Random()
    dedup_key_fn = dedup_key_fn or (lambda k: f"{analysis_id}:{k}")

    report = BatchReport(batch_id=batch_id, concurrency=resolved_conc, keys=list(keys))
    for key in keys:
        report.outcomes[key] = LotOutcome(key=key, queued_at=time.monotonic())

    if not keys:
        return report

    # Active-concurrency gauge (counts in-flight NETWORK calls) + degrade state.
    state_lock = threading.Lock()
    active = 0
    max_active = 0
    degraded = False
    total_retries = 0
    # Once degraded, workers serialize their network call through this lock so no
    # two calls overlap again (safe serial degradation without tearing the pool).
    serial_lock = threading.Lock()

    logger.info(
        "cv2.lot_batch.start batch_id=%s analysis_id=%s lots=%d concurrency=%d",
        batch_id,
        analysis_id,
        len(keys),
        resolved_conc,
    )

    def _worker(key: str) -> None:
        nonlocal active, max_active, degraded, total_retries
        outcome = report.outcomes[key]

        # 1) Reuse: an already-completed lot never triggers a new OpenAI call.
        if reuse_fn is not None:
            try:
                cached = reuse_fn(key)
            except Exception:  # noqa: BLE001 — reuse is best-effort, never fatal
                cached = None
            if cached is not None:
                outcome.reused = True
                outcome.started_at = time.monotonic()
                outcome.result = cached
                outcome.completed_at = time.monotonic()
                logger.info(
                    "cv2.lot.reused batch_id=%s analysis_id=%s lot=%s",
                    batch_id,
                    analysis_id,
                    key,
                )
                return

        dedup_key = dedup_key_fn(key)
        attempt = 0
        while True:
            attempt += 1
            outcome.attempts = attempt

            # After degradation, take the serial lock so network calls no longer
            # overlap. Before degradation the pool size alone bounds concurrency.
            with state_lock:
                degrade_now = degraded
            serial_held = False
            if degrade_now:
                serial_lock.acquire()
                serial_held = True

            # --- single bounded network attempt (active gauge held only here) ---
            with state_lock:
                active += 1
                if active > max_active:
                    max_active = active
            if outcome.started_at is None:
                outcome.started_at = time.monotonic()
            result: Any = None
            deduped = False
            err: Optional[BaseException] = None
            try:
                result, deduped = _dedup_call(dedup_key, lambda: call_fn(key))
            except BaseException as exc:  # noqa: BLE001 — recorded per lot
                err = exc
            finally:
                with state_lock:
                    active -= 1
                if serial_held:
                    serial_lock.release()
                    serial_held = False
            # --- end attempt: no pool/serial resource is held past this point ---

            if err is None:
                outcome.result = result
                outcome.deduplicated = deduped
                outcome.error = None
                outcome.completed_at = time.monotonic()
                logger.info(
                    "cv2.lot.done batch_id=%s analysis_id=%s lot=%s "
                    "attempts=%d duration=%s deduped=%s",
                    batch_id,
                    analysis_id,
                    key,
                    attempt,
                    outcome.duration,
                    deduped,
                )
                return

            transient = False
            rate_limited = False
            try:
                transient = bool(is_transient(err))
                rate_limited = bool(is_rate_limit(err))
            except Exception:  # noqa: BLE001 — classifier must never be fatal
                transient = False
            # Rate limiting means concurrency is unsafe: degrade the whole batch
            # to serial so no NEW concurrent call is started.
            if rate_limited:
                with state_lock:
                    if not degraded:
                        degraded = True
                        report.degraded_to_serial = True
                        logger.warning(
                            "cv2.lot_batch.degraded_to_serial batch_id=%s "
                            "analysis_id=%s lot=%s reason=rate_limit",
                            batch_id,
                            analysis_id,
                            key,
                        )
            if transient and attempt <= max_retries:
                with state_lock:
                    total_retries += 1
                    outcome.retry_count += 1
                delay = backoff_base * (2 ** (attempt - 1)) + rng.uniform(0, backoff_jitter)
                logger.warning(
                    "cv2.lot.retry batch_id=%s analysis_id=%s lot=%s attempt=%d "
                    "reason=%s backoff=%.3f",
                    batch_id,
                    analysis_id,
                    key,
                    attempt,
                    getattr(err, "reason_code", type(err).__name__),
                    delay,
                )
                if delay > 0:
                    sleep_fn(delay)
                continue

            # Non-transient, or retries exhausted: record and stop.
            outcome.error = err
            outcome.completed_at = time.monotonic()
            logger.warning(
                "cv2.lot.failed batch_id=%s analysis_id=%s lot=%s attempts=%d reason=%s",
                batch_id,
                analysis_id,
                key,
                attempt,
                getattr(err, "reason_code", type(err).__name__),
            )
            return

    # The pool size is the pre-degradation concurrency bound. Post-degradation the
    # serial_lock reduces effective in-flight calls to 1. We never create more than
    # ``resolved_conc`` threads regardless of lot count.
    pool_size = min(resolved_conc, len(keys))
    with ThreadPoolExecutor(max_workers=pool_size, thread_name_prefix="cv2-lot") as pool:
        list(pool.map(_worker, keys))

    report.max_active = max_active
    report.total_retries = total_retries
    logger.info(
        "cv2.lot_batch.done batch_id=%s analysis_id=%s lots=%d max_active=%d "
        "degraded_to_serial=%s total_retries=%d",
        batch_id,
        analysis_id,
        len(keys),
        max_active,
        report.degraded_to_serial,
        total_retries,
    )
    return report
