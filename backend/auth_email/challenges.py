"""Email OTP challenge lifecycle.

Collection: ``auth_email_challenges``.

Design invariants (mirrored by tests):

- The plaintext code exists only in the memory of the request that generated it.
  Only ``HMAC-SHA256(pepper, salt || code)`` is persisted; nothing reversible is
  stored, so a code can never be re-sent after its originating request ends.
- At most one non-terminal challenge exists per normalized email, enforced by a
  unique partial index on ``active_slot`` rather than by application checks.
- A challenge is verifiable in ``SENT`` *or* ``SEND_PENDING``. An ambiguous
  provider timeout may still have delivered the message, so possession of the
  correct code is treated as sufficient evidence of receipt. ``SEND_FAILED`` —
  a definitive provider refusal — is never verifiable.
- Successful verification is one atomic terminal write (``-> CONSUMED``), so two
  simultaneous correct submissions cannot both claim the challenge.
- ``expires_at`` bounds authentication; ``purge_at`` bounds retention. The TTL
  index is on ``purge_at``, so an expired challenge stays inspectable for
  diagnosis without ever being usable to authenticate.

Mongo here is a standalone instance (no replica set), so multi-document
transactions are unavailable. Every mutation is therefore a single-document
``find_one_and_update`` with a conditional filter, mirroring the proven pattern
in ``beta_program.quota``.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import re
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from . import config

logger = logging.getLogger(__name__)

CHALLENGES_COLLECTION = "auth_email_challenges"

# Lifecycle states.
STATUS_CREATED = "CREATED"
STATUS_SEND_PENDING = "SEND_PENDING"
STATUS_SENT = "SENT"
STATUS_SEND_FAILED = "SEND_FAILED"
STATUS_CONSUMED = "CONSUMED"
STATUS_EXPIRED = "EXPIRED"
STATUS_LOCKED = "LOCKED"
STATUS_SUPERSEDED = "SUPERSEDED"

# Possession of the code proves receipt, so an unresolved send is still
# verifiable. A definitively refused send is not.
VERIFIABLE_STATUSES = (STATUS_SENT, STATUS_SEND_PENDING)

TERMINAL_STATUSES = frozenset(
    {
        STATUS_CONSUMED,
        STATUS_EXPIRED,
        STATUS_LOCKED,
        STATUS_SEND_FAILED,
        STATUS_SUPERSEDED,
    }
)

CONSUMPTION_REASON_OTP = "OTP_VERIFIED"

# Verification outcomes.
RESULT_OK = "OK"
RESULT_INVALID = "INVALID"
RESULT_LOCKED = "LOCKED"

_MAX_EMAIL_CHARS = 254
_EMAIL_RE = re.compile(r"^[^@\s]{1,64}@[A-Za-z0-9.-]{1,190}\.[A-Za-z]{2,24}$")

_indexes_ready = False


def _db():
    import server  # type: ignore  # lazy: avoid circular import with server.py

    return server.db


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.isoformat()


def normalize_email(raw: Any) -> str:
    """Conservative normalization: trim + lowercase, nothing else.

    Plus-addressing and dots are preserved, so ``name@x.it`` and
    ``name+beta@x.it`` remain distinct identities. No Gmail-specific rewriting.
    Matches ``server._get_or_create_authenticated_user`` and
    ``beta_program.store.normalize_beta_email`` so a user and a membership
    always key on the same value.
    """
    return str(raw or "").strip().lower()


def is_valid_email(raw: Any) -> bool:
    candidate = normalize_email(raw)
    if not candidate or len(candidate) > _MAX_EMAIL_CHARS:
        return False
    # Reject control characters outright rather than sanitising them away.
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in candidate):
        return False
    if candidate.count("@") != 1:
        return False
    if ".." in candidate:
        return False
    return bool(_EMAIL_RE.match(candidate))


def generate_code() -> str:
    """Cryptographically secure six-digit code."""
    return f"{secrets.randbelow(1000000):06d}"


def hash_code(code: str, salt: str) -> str:
    """HMAC-SHA256 under a server-side pepper.

    A 10**6 keyspace makes an unpeppered digest trivially reversible, so the
    pepper — not the salt — is what protects the stored value.
    """
    pepper = config.code_pepper()
    if len(pepper) < config.MIN_PEPPER_CHARS:
        raise RuntimeError("AUTH_EMAIL_CODE_PEPPER missing or too short")
    return hmac.new(
        pepper.encode("utf-8"), f"{salt}:{code}".encode("utf-8"), hashlib.sha256
    ).hexdigest()


def verify_code_hash(code: str, salt: str, expected_hash: str) -> bool:
    try:
        candidate = hash_code(code, salt)
    except RuntimeError:
        return False
    return hmac.compare_digest(candidate, str(expected_hash or ""))


def build_idempotency_key(challenge_id: str) -> str:
    """Derived from the immutable challenge id — never the code or the address."""
    pepper = config.code_pepper()
    return hashlib.sha256(f"{pepper}:{challenge_id}".encode("utf-8")).hexdigest()


def hash_identifier(value: Any) -> Optional[str]:
    """One-way hash for IP / user-agent, so no raw client data is retained."""
    text = str(value or "").strip()
    if not text:
        return None
    pepper = config.code_pepper()
    return hashlib.sha256(f"{pepper}:id:{text}".encode("utf-8")).hexdigest()[:48]


async def ensure_indexes() -> None:
    """Additive, idempotent index creation."""
    global _indexes_ready
    if _indexes_ready:
        return
    db = _db()
    collection = db[CHALLENGES_COLLECTION]
    await collection.create_index(
        "challenge_id", unique=True, name="uq_auth_email_challenge_id", background=True
    )
    await collection.create_index(
        [("normalized_email", 1), ("created_at", -1)],
        name="ix_auth_email_challenge_email",
        background=True,
    )
    await collection.create_index(
        [("status", 1), ("expires_at", 1)],
        name="ix_auth_email_challenge_status",
        background=True,
    )
    # One live challenge per email, enforced by the database rather than by a
    # read-then-write check that two concurrent requests could both pass.
    await collection.create_index(
        "active_slot",
        unique=True,
        name="uq_auth_email_active_slot",
        background=True,
        partialFilterExpression={"active_slot": {"$type": "string"}},
    )
    # Retention, NOT authentication validity. expires_at governs whether a code
    # still works; purge_at governs when the evidence is deleted.
    await collection.create_index(
        "purge_at", name="ttl_auth_email_challenge_purge", background=True, expireAfterSeconds=0
    )
    _indexes_ready = True


async def supersede_active(normalized_email: str) -> Optional[Dict[str, Any]]:
    """Atomically retire the current live challenge for this email.

    Releases ``active_slot`` so a fresh challenge can be inserted. The old code
    becomes unusable immediately.
    """
    db = _db()
    now = _now()
    return await db[CHALLENGES_COLLECTION].find_one_and_update(
        {"active_slot": normalized_email},
        {
            "$set": {
                "status": STATUS_SUPERSEDED,
                "terminal_at": _iso(now),
                "terminal_reason": "SUPERSEDED_BY_NEW_REQUEST",
            },
            "$unset": {"active_slot": ""},
        },
        projection={"_id": 0},
        return_document=ReturnDocument.AFTER,
    )


async def get_active(normalized_email: str) -> Optional[Dict[str, Any]]:
    db = _db()
    return await db[CHALLENGES_COLLECTION].find_one(
        {"active_slot": normalized_email}, {"_id": 0}
    )


async def get(challenge_id: str) -> Optional[Dict[str, Any]]:
    db = _db()
    return await db[CHALLENGES_COLLECTION].find_one(
        {"challenge_id": str(challenge_id or "")}, {"_id": 0}
    )


async def create(
    *,
    normalized_email: str,
    request_ip: Any = None,
    user_agent: Any = None,
    supersede: bool = True,
) -> Tuple[Optional[Dict[str, Any]], str, Optional[str]]:
    """Create a live challenge and return ``(doc, plaintext_code, error)``.

    The plaintext is returned to the caller and never written anywhere. When the
    unique ``active_slot`` index rejects the insert, a concurrent request won the
    race; the caller responds with the same generic cooldown message it would
    have sent anyway, so the loser learns nothing.
    """
    db = _db()

    if supersede:
        await supersede_active(normalized_email)

    now = _now()
    ttl = config.code_ttl_seconds()
    challenge_id = f"aec_{uuid.uuid4().hex}"
    salt = secrets.token_hex(16)
    code = generate_code()

    doc: Dict[str, Any] = {
        "challenge_id": challenge_id,
        "normalized_email": normalized_email,
        "active_slot": normalized_email,
        "code_hash": hash_code(code, salt),
        "code_salt": salt,
        "status": STATUS_CREATED,
        "created_at": _iso(now),
        "expires_at": _iso(now + timedelta(seconds=ttl)),
        # Retention boundary — kept far beyond expiry on purpose.
        "purge_at": now + timedelta(seconds=config.purge_after_seconds()),
        "consumed_at": None,
        "verified_at": None,
        "attempt_count": 0,
        "request_ip_hash": hash_identifier(request_ip),
        "user_agent_hash": hash_identifier(user_agent),
        "provider": None,
        "provider_message_id": None,
        "send_attempted_at": None,
        "send_attempt_count": 0,
        "delivery_state": None,
        "failure_category": None,
        "idempotency_key": build_idempotency_key(challenge_id),
    }

    try:
        await db[CHALLENGES_COLLECTION].insert_one(dict(doc))
    except DuplicateKeyError:
        logger.info("auth_email challenge insert lost active-slot race")
        return None, "", "ACTIVE_CHALLENGE_EXISTS"

    doc.pop("_id", None)
    return doc, code, None


async def mark_send_pending(challenge_id: str, *, provider: str) -> None:
    db = _db()
    await db[CHALLENGES_COLLECTION].update_one(
        {"challenge_id": challenge_id, "status": STATUS_CREATED},
        {
            "$set": {
                "status": STATUS_SEND_PENDING,
                "provider": provider,
                "send_attempted_at": _iso(_now()),
            }
        },
    )


async def record_send_result(challenge_id: str, result: Any) -> None:
    """Persist safe delivery metadata and move the challenge to its next state.

    ``OK``        -> SENT (verifiable)
    ``AMBIGUOUS`` -> stays SEND_PENDING (still verifiable: it may have arrived)
    ``DEFINITIVE``-> SEND_FAILED, terminal, active_slot released
    """
    db = _db()
    now = _now()
    metadata: Dict[str, Any] = {
        "provider": result.provider,
        "provider_message_id": result.provider_message_id,
        "send_attempted_at": _iso(now),
        "send_attempt_count": int(getattr(result, "attempts", 1) or 1),
        "delivery_state": result.delivery_state,
        "failure_category": result.failure_category,
    }

    if result.ok:
        metadata["status"] = STATUS_SENT
        await db[CHALLENGES_COLLECTION].update_one(
            {"challenge_id": challenge_id, "status": {"$nin": list(TERMINAL_STATUSES)}},
            {"$set": metadata},
        )
        return

    if result.definitive_failure:
        metadata["status"] = STATUS_SEND_FAILED
        metadata["terminal_at"] = _iso(now)
        metadata["terminal_reason"] = "PROVIDER_DEFINITIVE_FAILURE"
        await db[CHALLENGES_COLLECTION].update_one(
            {"challenge_id": challenge_id, "status": {"$nin": list(TERMINAL_STATUSES)}},
            {"$set": metadata, "$unset": {"active_slot": ""}},
        )
        return

    # Ambiguous: leave it verifiable.
    metadata["status"] = STATUS_SEND_PENDING
    await db[CHALLENGES_COLLECTION].update_one(
        {"challenge_id": challenge_id, "status": {"$nin": list(TERMINAL_STATUSES)}},
        {"$set": metadata},
    )


async def consume(challenge_id: str, code: str) -> Tuple[str, Optional[Dict[str, Any]]]:
    """Atomically claim a challenge with the supplied code.

    Returns ``(result, doc)``. The winning caller receives ``RESULT_OK`` exactly
    once: consumption is a single terminal write, so two simultaneous correct
    submissions cannot both succeed.
    """
    db = _db()
    collection = db[CHALLENGES_COLLECTION]
    now = _now()
    max_attempts = config.max_verify_attempts()

    candidate = await collection.find_one({"challenge_id": str(challenge_id or "")}, {"_id": 0})
    if not candidate:
        return RESULT_INVALID, None

    if candidate.get("status") not in VERIFIABLE_STATUSES:
        return RESULT_INVALID, None

    if _is_expired(candidate, now):
        await _expire(challenge_id)
        return RESULT_INVALID, None

    if int(candidate.get("attempt_count") or 0) >= max_attempts:
        await _lock(challenge_id)
        return RESULT_LOCKED, None

    supplied = str(code or "").strip()
    if not verify_code_hash(supplied, candidate.get("code_salt") or "", candidate.get("code_hash") or ""):
        updated = await collection.find_one_and_update(
            {
                "challenge_id": challenge_id,
                "status": {"$in": list(VERIFIABLE_STATUSES)},
            },
            {"$inc": {"attempt_count": 1}, "$set": {"last_attempt_at": _iso(now)}},
            projection={"_id": 0},
            return_document=ReturnDocument.AFTER,
        )
        if updated and int(updated.get("attempt_count") or 0) >= max_attempts:
            await _lock(challenge_id)
            return RESULT_LOCKED, None
        return RESULT_INVALID, None

    # Correct code: one atomic terminal transition. The status/expiry/attempt
    # guards are re-applied inside the filter so the check and the claim cannot
    # be separated by a concurrent writer.
    claimed = await collection.find_one_and_update(
        {
            "challenge_id": challenge_id,
            "status": {"$in": list(VERIFIABLE_STATUSES)},
            "consumed_at": None,
            "expires_at": {"$gt": _iso(now)},
            "$expr": {"$lt": ["$attempt_count", max_attempts]},
        },
        {
            "$set": {
                "status": STATUS_CONSUMED,
                "verified_at": _iso(now),
                "consumed_at": _iso(now),
                "consumption_reason": CONSUMPTION_REASON_OTP,
                "terminal_at": _iso(now),
                "terminal_reason": CONSUMPTION_REASON_OTP,
            },
            "$unset": {"active_slot": ""},
        },
        projection={"_id": 0},
        return_document=ReturnDocument.AFTER,
    )
    if claimed is None:
        # Lost the race, or it expired between the read and the claim.
        return RESULT_INVALID, None
    return RESULT_OK, claimed


def _is_expired(doc: Dict[str, Any], now: datetime) -> bool:
    raw = doc.get("expires_at")
    if not raw:
        return True
    try:
        expires = datetime.fromisoformat(str(raw))
    except (TypeError, ValueError):
        return True
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=timezone.utc)
    return expires <= now


async def _expire(challenge_id: str) -> None:
    db = _db()
    now = _now()
    await db[CHALLENGES_COLLECTION].update_one(
        {"challenge_id": challenge_id, "status": {"$nin": list(TERMINAL_STATUSES)}},
        {
            "$set": {
                "status": STATUS_EXPIRED,
                "terminal_at": _iso(now),
                "terminal_reason": "TTL_EXPIRED",
            },
            "$unset": {"active_slot": ""},
        },
    )


async def _lock(challenge_id: str) -> None:
    db = _db()
    now = _now()
    await db[CHALLENGES_COLLECTION].update_one(
        {"challenge_id": challenge_id, "status": {"$nin": list(TERMINAL_STATUSES)}},
        {
            "$set": {
                "status": STATUS_LOCKED,
                "terminal_at": _iso(now),
                "terminal_reason": "MAX_ATTEMPTS_EXCEEDED",
            },
            "$unset": {"active_slot": ""},
        },
    )


async def expire_if_stale(normalized_email: str) -> None:
    """Release a live slot whose code has passed ``expires_at``.

    Called before issuing a new challenge so an abandoned challenge never blocks
    a legitimate retry until the TTL monitor happens to run.
    """
    active = await get_active(normalized_email)
    if active and _is_expired(active, _now()):
        await _expire(active.get("challenge_id"))
