"""
Operational signals + the telemetry-only ``v2_job_events`` mirror.

TELEMETRY CONTRACT (owner-mandated):
- ``emit_v2_job_event`` is telemetry ONLY. It must never alter pipeline output,
  validation, billing, credits, report state, or customer visibility. No pipeline
  code reads it back.
- It is called only AFTER the authoritative ``job_status.json`` write succeeds.
- It never raises and never delays the analysis. ``emit_v2_job_event`` performs
  NO I/O at all on the caller's thread: it builds a small dict and does a
  non-blocking ``put_nowait`` onto a **bounded** queue. A slow, hanging or
  unreachable Mongo can therefore never block or delay an analysis. When the
  queue is full the event is DROPPED and counted — losing telemetry is always
  preferable to delaying a customer's analysis.
- Exactly ONE bounded daemon worker thread drains the queue and writes to Mongo.
  No thread-per-event, no unbounded executor. Worker exceptions are logged and
  swallowed.
- Events are idempotent: a deterministic ``event_id`` + a unique index means
  retries/restarts cannot duplicate a metric.
- Safe metadata ONLY is stored: event_id, event_type, job_id, analysis_id,
  lot_id, user_id, status, reason_code, duration_seconds, created_at. Never PDF
  text, prompts, excerpts, perizia party names, tokens, or secrets.

The worker uses a dedicated lazy **synchronous** pymongo client (short bounded
connect/serverSelection/socket timeouts) rather than the async motor client, and
it is the only place that touches it — including the ``user_id`` backfill lookup,
which is deliberately resolved off the pipeline thread. Signal/overview *reads*
run in the async admin API and use the motor ``server.db`` handle.
"""

from __future__ import annotations

import hashlib
import logging
import os
import queue
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

EVENTS_COLLECTION = "v2_job_events"

# Event vocabulary (emitted only where genuinely observed).
EVENT_REPORT_READY = "REPORT_READY"
EVENT_VERIFICATION_REQUIRED = "VERIFICATION_REQUIRED"
EVENT_SERVICE_BUSY = "SERVICE_BUSY"
EVENT_SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"
EVENT_DOCUMENT_NOT_READABLE = "DOCUMENT_NOT_READABLE"
EVENT_LOT_REPORT_REUSED = "LOT_REPORT_REUSED"
EVENT_LOT_JOB_DEDUPLICATED = "LOT_JOB_DEDUPLICATED"
EVENT_LOT_RERUN_FORCED = "LOT_RERUN_FORCED"
EVENT_FAILED_RERUN_SAFE_REPORT_PRESERVED = "FAILED_RERUN_SAFE_REPORT_PRESERVED"
EVENT_CONFIRMATION_REQUIRED = "CONFIRMATION_REQUIRED"
EVENT_CONFIRMATION_COMPLETED = "CONFIRMATION_COMPLETED"

EVENT_TYPES = frozenset(
    {
        EVENT_REPORT_READY,
        EVENT_VERIFICATION_REQUIRED,
        EVENT_SERVICE_BUSY,
        EVENT_SERVICE_UNAVAILABLE,
        EVENT_DOCUMENT_NOT_READABLE,
        EVENT_LOT_REPORT_REUSED,
        EVENT_LOT_JOB_DEDUPLICATED,
        EVENT_LOT_RERUN_FORCED,
        EVENT_FAILED_RERUN_SAFE_REPORT_PRESERVED,
        EVENT_CONFIRMATION_REQUIRED,
        EVENT_CONFIRMATION_COMPLETED,
    }
)

# Overridable collection resolver (tests inject a fake; default = sync pymongo).
_events_collection_override = None
_sync_client = None

# --- bounded non-blocking telemetry transport -------------------------------
# Sized so a long burst of jobs is absorbed while the memory ceiling stays
# trivial (~a few hundred KB of small dicts at the maximum).
_QUEUE_MAX = max(1, int(os.environ.get("V2_TELEMETRY_QUEUE_MAX", "2000")))
_TIMEOUT_SERVER_SELECTION_MS = 2000
_TIMEOUT_CONNECT_MS = 2000
_TIMEOUT_SOCKET_MS = 5000

_STOP = object()  # worker shutdown sentinel

_queue: "Optional[queue.Queue]" = None
_worker: "Optional[threading.Thread]" = None
_worker_lock = threading.Lock()
_dropped_events = 0
_written_events = 0
_failed_events = 0
_last_drop_log = 0.0
_last_fail_log = 0.0
_DROP_LOG_INTERVAL_SECONDS = 60.0
_FAIL_LOG_INTERVAL_SECONDS = 60.0


def set_events_collection_override(collection) -> None:
    """Tests only: route emits/reads to an injected collection object."""
    global _events_collection_override
    _events_collection_override = collection


def _events_collection():
    if _events_collection_override is not None:
        return _events_collection_override
    global _sync_client
    if _sync_client is None:
        from pymongo import MongoClient  # lazy: only when telemetry actually writes

        # Short, bounded timeouts everywhere: the worker must never park for
        # minutes on an unreachable or hanging Mongo.
        _sync_client = MongoClient(
            os.environ["MONGO_URL"],
            serverSelectionTimeoutMS=_TIMEOUT_SERVER_SELECTION_MS,
            connectTimeoutMS=_TIMEOUT_CONNECT_MS,
            socketTimeoutMS=_TIMEOUT_SOCKET_MS,
            maxPoolSize=2,
        )
    return _sync_client[os.environ["DB_NAME"]][EVENTS_COLLECTION]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _event_id(job_id: str, lot_id: Optional[str], event_type: str) -> str:
    raw = f"{job_id}|{lot_id or ''}|{event_type}"
    return "v2ev_" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:24]


def emit_v2_job_event(
    event_type: str,
    *,
    job_id: str,
    analysis_id: str,
    user_id: Optional[str] = None,
    lot_id: Optional[str] = None,
    status: Optional[str] = None,
    reason_code: Optional[str] = None,
    duration_seconds: Optional[float] = None,
) -> None:
    """Idempotently record one operational telemetry event. Never raises.

    NON-BLOCKING: this builds a dict and enqueues it. It performs no Mongo I/O
    and no network access on the calling (pipeline) thread, so it cannot delay
    an analysis regardless of Mongo health. If the bounded queue is full the
    event is dropped and counted.

    Safe metadata only. The worker uses a ``$setOnInsert`` upsert keyed on a
    deterministic ``event_id`` so a retry/restart can never double-count.
    """
    try:
        if event_type not in EVENT_TYPES:
            return
        if not job_id or not analysis_id:
            return
        event_id = _event_id(job_id, lot_id, event_type)
        doc = {
            "event_id": event_id,
            "event_type": event_type,
            "job_id": str(job_id),
            "analysis_id": str(analysis_id),
            "lot_id": str(lot_id) if lot_id not in (None, "") else None,
            "user_id": str(user_id) if user_id else None,
            "status": str(status) if status else None,
            "reason_code": str(reason_code) if reason_code else None,
            "duration_seconds": (
                float(duration_seconds) if duration_seconds is not None else None
            ),
            "created_at": _now(),
        }
        _enqueue(doc)
    except Exception as exc:  # never affect the pipeline
        logger.warning(
            "emit_v2_job_event failed type=%s job=%s: %s", event_type, job_id, exc
        )


def _enqueue(doc: Dict[str, Any]) -> bool:
    """Non-blocking hand-off to the telemetry worker. True if queued."""
    global _dropped_events, _last_drop_log
    q = _ensure_worker()
    try:
        q.put_nowait(doc)
        return True
    except queue.Full:
        # Bounded by design: drop, count, and log at most once a minute so a
        # sustained outage cannot turn into a logging storm.
        _dropped_events += 1
        now = time.monotonic()
        if now - _last_drop_log >= _DROP_LOG_INTERVAL_SECONDS:
            _last_drop_log = now
            logger.warning(
                "v2_job_events telemetry queue full (max=%s); dropped=%s events",
                _QUEUE_MAX,
                _dropped_events,
            )
        return False


def _ensure_worker() -> "queue.Queue":
    """Create the bounded queue and the single daemon worker exactly once."""
    global _queue, _worker
    q = _queue
    if q is not None and _worker is not None and _worker.is_alive():
        return q
    with _worker_lock:
        if _queue is None:
            _queue = queue.Queue(maxsize=_QUEUE_MAX)
        if _worker is None or not _worker.is_alive():
            _worker = threading.Thread(
                target=_worker_loop,
                name="v2-telemetry-writer",
                daemon=True,  # never holds up interpreter shutdown
            )
            _worker.start()
        return _queue


def _worker_loop() -> None:
    """Drain the queue forever, writing one event at a time. Never propagates."""
    global _written_events, _failed_events
    q = _queue
    if q is None:  # pragma: no cover - defensive
        return
    while True:
        item = q.get()
        try:
            if item is _STOP:
                return
            _write_event(item)
            _written_events += 1
        except Exception as exc:  # telemetry tolerance: log, keep draining
            _failed_events += 1
            # Throttled: a sustained Mongo outage must not become a log storm
            # (which would itself cost the process real CPU and I/O).
            global _last_fail_log
            now = time.monotonic()
            if now - _last_fail_log >= _FAIL_LOG_INTERVAL_SECONDS:
                _last_fail_log = now
                logger.warning(
                    "v2_job_events write failed type=%s job=%s (total failed=%s): %s",
                    (item or {}).get("event_type") if isinstance(item, dict) else None,
                    (item or {}).get("job_id") if isinstance(item, dict) else None,
                    _failed_events,
                    exc,
                )
        finally:
            q.task_done()


def _write_event(doc: Dict[str, Any]) -> None:
    """Worker-thread-only Mongo write (plus the deferred user_id resolution)."""
    if doc.get("user_id") is None and doc.get("analysis_id"):
        # Resolved here, never on the pipeline thread: this is a Mongo read.
        doc = dict(doc)
        doc["user_id"] = resolve_analysis_user_id(doc.get("analysis_id"))
    _events_collection().update_one(
        {"event_id": doc["event_id"]}, {"$setOnInsert": doc}, upsert=True
    )


def flush(timeout: float = 1.0) -> bool:
    """Wait (bounded) until queued events are written. True if fully drained.

    Used by tests, the offline backfill script and the service shutdown hook.
    Never used by the analysis pipeline.
    """
    q = _queue
    if q is None:
        return True
    deadline = time.monotonic() + max(0.0, timeout)
    with q.all_tasks_done:
        while q.unfinished_tasks:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            q.all_tasks_done.wait(remaining)
    return True


def shutdown(timeout: float = 2.0) -> None:
    """Best-effort bounded flush at service shutdown. Never hangs shutdown."""
    q = _queue
    if q is None:
        return
    flush(timeout)
    try:
        q.put_nowait(_STOP)
    except queue.Full:
        # Queue still saturated (worker stuck on a hanging write): make room by
        # discarding one pending event so the sentinel always lands and the
        # worker can never be leaked.
        try:
            q.get_nowait()
            q.task_done()
            q.put_nowait(_STOP)
        except (queue.Empty, queue.Full):
            return
    worker = _worker
    if worker is not None:
        worker.join(timeout=0.5)


def telemetry_stats() -> Dict[str, Any]:
    """Observability counters for the bounded telemetry transport."""
    q = _queue
    return {
        "queue_max": _QUEUE_MAX,
        "queued": q.qsize() if q is not None else 0,
        "written": _written_events,
        "dropped": _dropped_events,
        "failed": _failed_events,
        "worker_alive": bool(_worker is not None and _worker.is_alive()),
    }


def reset_for_tests() -> None:
    """Tests only: stop the worker and clear queue state/counters."""
    global _queue, _worker, _dropped_events, _written_events, _failed_events
    global _last_drop_log
    shutdown(0.5)
    with _worker_lock:
        _queue = None
        _worker = None
    _dropped_events = 0
    _written_events = 0
    _failed_events = 0
    _last_drop_log = 0.0


_analysis_user_cache: Dict[str, Optional[str]] = {}


def resolve_analysis_user_id(analysis_id: Optional[str]) -> Optional[str]:
    """Best-effort sync lookup of the owning user_id for an analysis (cached).

    Used by the telemetry hook running in the pipeline thread. Never raises.
    """
    if not analysis_id:
        return None
    if analysis_id in _analysis_user_cache:
        return _analysis_user_cache[analysis_id]
    user_id: Optional[str] = None
    try:
        coll = _events_collection()
        # Reuse the same sync client/db as the events collection.
        db = coll.database
        doc = db["perizia_analyses"].find_one(
            {"analysis_id": analysis_id}, {"_id": 0, "user_id": 1}
        )
        user_id = (doc or {}).get("user_id")
    except Exception:
        user_id = None
    _analysis_user_cache[analysis_id] = user_id
    return user_id


def ensure_indexes_sync() -> None:
    """Create the telemetry indexes using the sync client (idempotent).

    Safe to call at process start; swallows all errors. The async ``server``
    startup also mirrors these via ``ensure_indexes`` for parity in tests.
    """
    try:
        coll = _events_collection()
        coll.create_index("event_id", unique=True, name="uq_v2_event_id")
        coll.create_index([("user_id", 1), ("created_at", -1)], name="ix_v2_event_user")
        coll.create_index(
            [("analysis_id", 1), ("created_at", -1)], name="ix_v2_event_analysis"
        )
        coll.create_index(
            [("event_type", 1), ("created_at", -1)], name="ix_v2_event_type"
        )
    except Exception as exc:  # pragma: no cover - telemetry tolerance
        logger.warning("v2_job_events ensure_indexes_sync failed: %s", exc)


# ---------------------------------------------------------------------------
# Async signal/overview reads (admin API only; motor db)
# ---------------------------------------------------------------------------
def _db():
    import server  # type: ignore  # lazy

    return server.db


async def ensure_indexes() -> None:
    """Create telemetry indexes via motor (parity with the sync path)."""
    db = _db()
    await db[EVENTS_COLLECTION].create_index(
        "event_id", unique=True, name="uq_v2_event_id", background=True
    )
    await db[EVENTS_COLLECTION].create_index(
        [("user_id", 1), ("created_at", -1)], name="ix_v2_event_user", background=True
    )
    await db[EVENTS_COLLECTION].create_index(
        [("analysis_id", 1), ("created_at", -1)], name="ix_v2_event_analysis", background=True
    )
    await db[EVENTS_COLLECTION].create_index(
        [("event_type", 1), ("created_at", -1)], name="ix_v2_event_type", background=True
    )


async def list_signals(
    *,
    user_ids: Optional[List[str]] = None,
    analysis_id: Optional[str] = None,
    event_type: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
) -> Dict[str, Any]:
    db = _db()
    query: Dict[str, Any] = {}
    if user_ids is not None:
        query["user_id"] = {"$in": list(user_ids)}
    if analysis_id:
        query["analysis_id"] = analysis_id
    if event_type and event_type in EVENT_TYPES:
        query["event_type"] = event_type
    date_query: Dict[str, Any] = {}
    if date_from:
        date_query["$gte"] = date_from
    if date_to:
        date_query["$lte"] = date_to
    if date_query:
        query["created_at"] = date_query
    page = max(1, int(page or 1))
    page_size = max(1, min(200, int(page_size or 50)))
    total = await db[EVENTS_COLLECTION].count_documents(query)
    cursor = (
        db[EVENTS_COLLECTION]
        .find(query, {"_id": 0})
        .sort("created_at", -1)
        .skip((page - 1) * page_size)
        .limit(page_size)
    )
    items = await cursor.to_list(page_size)
    return {"items": items, "total": total, "page": page, "page_size": page_size}


async def event_counts(user_ids: List[str]) -> Dict[str, int]:
    """Per-event-type counts for the given tester user_ids (deterministic)."""
    db = _db()
    if not user_ids:
        return {}
    counts: Dict[str, int] = {}
    for event_type in EVENT_TYPES:
        counts[event_type] = await db[EVENTS_COLLECTION].count_documents(
            {"event_type": event_type, "user_id": {"$in": user_ids}}
        )
    return counts


async def avg_report_duration_seconds(user_ids: List[str]) -> Optional[float]:
    db = _db()
    if not user_ids:
        return None
    cursor = db[EVENTS_COLLECTION].find(
        {
            "event_type": EVENT_REPORT_READY,
            "user_id": {"$in": user_ids},
            "duration_seconds": {"$ne": None},
        },
        {"_id": 0, "duration_seconds": 1},
    )
    docs = await cursor.to_list(None)
    values = [d["duration_seconds"] for d in docs if d.get("duration_seconds") is not None]
    if not values:
        return None
    return round(sum(values) / len(values), 1)
