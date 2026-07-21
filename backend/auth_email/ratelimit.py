"""Mongo-backed rate limiting for the OTP endpoints.

The backend has no Redis and no shared in-process cache, so limits are enforced
with single-document atomic operations against ``auth_email_rate_buckets`` —
the same idiom already proven in ``beta_program.quota``. This works across
multiple workers and processes, which a frontend guard or an in-memory counter
would not.

Retention is deliberately decoupled from the challenge lifecycle: buckets outlive
the challenges that created them, so a user cannot reset an hourly limit by
letting a challenge expire or be purged.

Correctness never depends on TTL deletion. Mongo's TTL monitor is lazy (it can
lag by a minute), so every limit is decided by comparing stored timestamps, and
``purge_at`` only reclaims space afterwards.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from . import config
from .challenges import hash_identifier

logger = logging.getLogger(__name__)

RATE_COLLECTION = "auth_email_rate_buckets"

SCOPE_EMAIL_COOLDOWN = "cooldown_email"
SCOPE_EMAIL_HOUR = "hour_email"
SCOPE_IP_HOUR = "hour_ip"

# Buckets are kept well past their window so an expired challenge can never
# hand back allowance that was already spent.
_RETENTION_MARGIN_SECONDS = 3600

_indexes_ready = False


def _db():
    import server  # type: ignore  # lazy: avoid circular import with server.py

    return server.db


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.isoformat()


async def ensure_indexes() -> None:
    global _indexes_ready
    if _indexes_ready:
        return
    db = _db()
    collection = db[RATE_COLLECTION]
    await collection.create_index(
        "bucket_key", unique=True, name="uq_auth_email_rate_key", background=True
    )
    await collection.create_index(
        "purge_at", name="ttl_auth_email_rate_purge", background=True, expireAfterSeconds=0
    )
    _indexes_ready = True


def _window_start(now: datetime, window_seconds: int) -> int:
    return int(now.timestamp()) // window_seconds * window_seconds


def _bucket_key(scope: str, identity: str, window: Optional[int] = None) -> str:
    if window is None:
        return f"{scope}:{identity}"
    return f"{scope}:{identity}:{window}"


async def check_cooldown(normalized_email: str) -> Tuple[bool, int]:
    """Enforce the minimum interval between code requests for one address.

    Returns ``(allowed, retry_after_seconds)``.

    The conditional-upsert shape is what makes this atomic: the filter only
    matches a bucket whose last request is old enough, so a second concurrent
    request either fails the filter or collides on the unique key. Both mean
    "too soon", and both produce the same customer-visible response.
    """
    cooldown = config.resend_cooldown_seconds()
    identity = hash_identifier(normalized_email) or "unknown"
    key = _bucket_key(SCOPE_EMAIL_COOLDOWN, identity)
    now = _now()
    threshold = now - timedelta(seconds=cooldown)
    db = _db()

    try:
        updated = await db[RATE_COLLECTION].find_one_and_update(
            {"bucket_key": key, "last_request_at": {"$lte": _iso(threshold)}},
            {
                "$set": {
                    "last_request_at": _iso(now),
                    "purge_at": now + timedelta(seconds=cooldown + _RETENTION_MARGIN_SECONDS),
                },
                "$setOnInsert": {"bucket_key": key, "scope": SCOPE_EMAIL_COOLDOWN},
            },
            upsert=True,
            projection={"_id": 0},
            return_document=ReturnDocument.AFTER,
        )
    except DuplicateKeyError:
        # A bucket exists and is younger than the cooldown.
        existing = await db[RATE_COLLECTION].find_one({"bucket_key": key}, {"_id": 0})
        return False, _retry_after(existing, cooldown, now)

    if updated is None:
        existing = await db[RATE_COLLECTION].find_one({"bucket_key": key}, {"_id": 0})
        return False, _retry_after(existing, cooldown, now)
    return True, 0


def _retry_after(bucket: Optional[dict], cooldown: int, now: datetime) -> int:
    if not bucket:
        return cooldown
    raw = bucket.get("last_request_at")
    if not raw:
        return cooldown
    try:
        last = datetime.fromisoformat(str(raw))
    except (TypeError, ValueError):
        return cooldown
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    remaining = cooldown - int((now - last).total_seconds())
    return max(1, min(cooldown, remaining))


async def _consume_windowed(scope: str, identity: str, limit: int, window_seconds: int) -> bool:
    """Increment a fixed-window counter; return False once the limit is passed."""
    now = _now()
    window = _window_start(now, window_seconds)
    key = _bucket_key(scope, identity, window)
    db = _db()

    for _ in range(2):
        try:
            updated = await db[RATE_COLLECTION].find_one_and_update(
                {"bucket_key": key},
                {
                    "$inc": {"count": 1},
                    "$set": {"last_request_at": _iso(now)},
                    "$setOnInsert": {
                        "bucket_key": key,
                        "scope": scope,
                        "window_start": window,
                        "purge_at": datetime.fromtimestamp(window, tz=timezone.utc)
                        + timedelta(seconds=window_seconds + _RETENTION_MARGIN_SECONDS),
                    },
                },
                upsert=True,
                projection={"_id": 0},
                return_document=ReturnDocument.AFTER,
            )
        except DuplicateKeyError:
            # Concurrent insert of the same bucket; retry against the winner.
            continue
        if updated is None:
            continue
        return int(updated.get("count") or 0) <= limit
    return False


async def check_email_hourly(normalized_email: str) -> bool:
    identity = hash_identifier(normalized_email) or "unknown"
    return await _consume_windowed(
        SCOPE_EMAIL_HOUR, identity, config.max_requests_per_email_hour(), 3600
    )


async def check_ip_hourly(request_ip: object) -> bool:
    identity = hash_identifier(request_ip)
    if not identity:
        # No usable client address: do not punish the request, the per-email
        # limits still apply.
        return True
    return await _consume_windowed(
        SCOPE_IP_HOUR, identity, config.max_requests_per_ip_hour(), 3600
    )
