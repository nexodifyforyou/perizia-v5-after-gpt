"""
Beta perizia allowance ("quota") -- a second, orthogonal entitlement axis on top
of the ACTIVE/PENDING/REVOKED membership status axis (see ``store.py``).

See ``docs/beta_perizia_limits_plan.md`` for the full audited design. This
module implements:

- Atomic reservation/consumption/release of a per-membership allowance using
  ONLY single-document ``find_one_and_update`` with a conditional ``$expr``
  filter -- the verified Mongo topology (a standalone instance, not a replica
  set) makes multi-document transactions unavailable, so no transaction is
  used or required anywhere here.
- A durable, append-only usage ledger (``beta_program_usage``), one row per
  analysis, keyed on the backend-generated ``analysis_id`` (never a
  browser-supplied value).
- Owner actions: set quota mode/limit, start a new phase, list historical
  phases.
- Stale-reservation crash recovery (indexed, bounded, never a full scan).

OWNER AMENDMENT (2026-07-20) -- the CONSUME/RELEASE boundary is "was a paid
processing call actually initiated for this analysis", tracked authoritatively
via ``paid_processing_started_at`` on the usage ledger row -- never inferred
from the final report status. See ``mark_paid_processing_started`` and
``finalize_on_failure``. CONSUME whenever that marker is set (including
UNREADABLE outcomes, a timeout during/after paid processing, or any later
service error); RELEASE only when the request is rejected before every paid
call site.

Never writes credits/wallet/Stripe. Never stores PDF content, extracted
evidence, party names, prompts, tokens, secrets, or session data on the usage
ledger -- only identifiers, state, timestamps, and a closed reason-code
vocabulary (mirrors the ``v2_job_events`` "safe metadata only" contract).
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from pymongo import ReturnDocument

from . import store

logger = logging.getLogger(__name__)

USAGE_COLLECTION = "beta_program_usage"

# --- quota mode / derived state vocabulary ----------------------------------
QUOTA_MODE_UNLIMITED = "UNLIMITED"
QUOTA_MODE_LIMITED = "LIMITED"
QUOTA_MODES = frozenset({QUOTA_MODE_UNLIMITED, QUOTA_MODE_LIMITED})

STATE_UNLIMITED = "UNLIMITED"
STATE_AVAILABLE = "AVAILABLE"
STATE_EXHAUSTED = "EXHAUSTED"

# --- usage ledger row states -------------------------------------------------
USAGE_RESERVED = "RESERVED"
USAGE_CONSUMED = "CONSUMED"
USAGE_RELEASED = "RELEASED"
USAGE_REJECTED = "REJECTED"

# Sane upper bound on a LIMITED allowance: generous for any real beta cohort,
# but prevents a fat-fingered owner entry from becoming a de-facto unlimited
# value that would defeat the point of "LIMITED".
MAX_ANALYSIS_LIMIT = 100_000

# --- release reason codes (one per call site) -------------------------------
REASON_INVALID_FILE_TYPE = "INVALID_FILE_TYPE"
REASON_DOCUMENT_UNREADABLE_BEFORE_PAID_ANALYSIS = "DOCUMENT_UNREADABLE_BEFORE_PAID_ANALYSIS"
REASON_PAGE_COUNT_UNSUPPORTED = "PAGE_COUNT_UNSUPPORTED"
REASON_PIPELINE_TIMEOUT = "PIPELINE_TIMEOUT"
REASON_UPLOAD_PERSISTENCE_FAILURE = "UPLOAD_PERSISTENCE_FAILURE"
REASON_JOB_CREATION_FAILURE_BEFORE_PROCESSING = "JOB_CREATION_FAILURE_BEFORE_PROCESSING"
REASON_AUTHORIZATION_RACE = "AUTHORIZATION_RACE"
REASON_DUPLICATE_REQUEST_LINKED_TO_EXISTING_ANALYSIS = "DUPLICATE_REQUEST_LINKED_TO_EXISTING_ANALYSIS"
REASON_PHASE_TRANSITION_FORCE_RELEASE = "PHASE_TRANSITION_FORCE_RELEASE"
REASON_STALE_RESERVATION_NO_ANALYSIS_FOUND = "STALE_RESERVATION_NO_ANALYSIS_FOUND"

# The one and only CONSUME reason under the owner amendment.
REASON_CONSUMED_PAID_PROCESSING_STARTED = "CONSUMED_PAID_PROCESSING_STARTED"

# --- audit actions (appended to the existing beta_program_audit stream) -----
ACTION_QUOTA_MODE_CHANGED = "QUOTA_MODE_CHANGED"
ACTION_QUOTA_LIMIT_CHANGED = "QUOTA_LIMIT_CHANGED"
ACTION_QUOTA_PHASE_STARTED = "QUOTA_PHASE_STARTED"
ACTION_QUOTA_SLOT_RESERVED = "QUOTA_SLOT_RESERVED"
ACTION_QUOTA_SLOT_CONSUMED = "QUOTA_SLOT_CONSUMED"
ACTION_QUOTA_SLOT_RELEASED = "QUOTA_SLOT_RELEASED"
ACTION_QUOTA_EXHAUSTED = "QUOTA_EXHAUSTED"
ACTION_QUOTA_AVAILABLE_AGAIN = "QUOTA_AVAILABLE_AGAIN"

ACTOR_OWNER = store.ACTOR_OWNER
ACTOR_SYSTEM_UPLOAD = "SYSTEM_UPLOAD"


def _db():
    return store._db()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_usage_id() -> str:
    return f"betause_{uuid.uuid4().hex[:16]}"


def _stale_reservation_seconds() -> int:
    """Safe duration before a RESERVED row is considered for crash recovery.

    Default 600s (10 minutes) = 5x PIPELINE_TIMEOUT_SECONDS (120s default),
    the hard ceiling on how long the synchronous analyze_perizia request (the
    only place a reservation is created or resolved) can legitimately take.
    """
    try:
        return int(os.environ.get("BETA_QUOTA_STALE_RESERVATION_SECONDS", "600"))
    except (TypeError, ValueError):
        return 600


def _quota_defaults(now: Optional[str] = None) -> Dict[str, Any]:
    """Defaults for a membership that predates this feature (additive,
    non-destructive migration target) or a freshly-added tester."""
    now = now or _now()
    return {
        "quota_mode": QUOTA_MODE_UNLIMITED,
        "analysis_limit": None,
        "analysis_consumed": 0,
        "analysis_reserved": 0,
        "quota_version": 1,
        "quota_period_started_at": now,
        "quota_updated_at": None,
        "quota_updated_by": None,
        "quota_note": None,
    }


async def ensure_indexes() -> None:
    """Create additive indexes for the usage ledger (idempotent)."""
    db = _db()
    await db[USAGE_COLLECTION].create_index(
        "analysis_id", unique=True, name="uq_beta_usage_analysis_id", background=True
    )
    await db[USAGE_COLLECTION].create_index(
        [("state", 1), ("reserved_at", 1)], name="ix_beta_usage_stale", background=True
    )
    await db[USAGE_COLLECTION].create_index(
        [("membership_id", 1), ("quota_version", 1)], name="ix_beta_usage_phase", background=True
    )
    await db[USAGE_COLLECTION].create_index(
        [("user_id", 1), ("created_at", -1)], name="ix_beta_usage_user", background=True
    )


# ---------------------------------------------------------------------------
# Derived state
# ---------------------------------------------------------------------------
def _is_exhausted(doc: Dict[str, Any]) -> bool:
    if not doc or (doc.get("quota_mode") or QUOTA_MODE_UNLIMITED) != QUOTA_MODE_LIMITED:
        return False
    limit = doc.get("analysis_limit")
    if limit is None:
        return False
    consumed = int(doc.get("analysis_consumed") or 0)
    reserved = int(doc.get("analysis_reserved") or 0)
    return (consumed + reserved) >= int(limit)


def derive_quota_state(membership: Dict[str, Any]) -> Dict[str, Any]:
    """Pure function: membership doc -> the customer/owner-safe derived quota
    block. Shape frozen for both the customer entitlement snapshot and the
    owner tester list: {mode, limit, consumed, reserved, remaining, state,
    quota_version}. Missing fields (a pre-feature membership doc) default
    exactly as the migration would (UNLIMITED / null / version 1 / 0 / 0)."""
    membership = membership or {}
    mode = membership.get("quota_mode") or QUOTA_MODE_UNLIMITED
    limit = membership.get("analysis_limit")
    consumed = int(membership.get("analysis_consumed") or 0)
    reserved = int(membership.get("analysis_reserved") or 0)
    quota_version = membership.get("quota_version") or 1
    if mode != QUOTA_MODE_LIMITED or limit is None:
        return {
            "mode": QUOTA_MODE_UNLIMITED,
            "limit": None,
            "consumed": consumed,
            "reserved": reserved,
            "remaining": None,
            "state": STATE_UNLIMITED,
            "quota_version": quota_version,
        }
    remaining = max(0, int(limit) - consumed - reserved)
    state = STATE_EXHAUSTED if (consumed + reserved) >= int(limit) else STATE_AVAILABLE
    return {
        "mode": QUOTA_MODE_LIMITED,
        "limit": int(limit),
        "consumed": consumed,
        "reserved": reserved,
        "remaining": remaining,
        "state": state,
        "quota_version": quota_version,
    }


async def _write_quota_audit(
    action: str,
    membership: Dict[str, Any],
    *,
    actor_type: str,
    actor_email: Optional[str] = None,
    actor_user_id: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    await store._write_audit(
        action=action,
        membership=membership or {},
        actor_type=actor_type,
        actor_email=actor_email,
        actor_user_id=actor_user_id,
        before_status=(membership or {}).get("status"),
        after_status=(membership or {}).get("status"),
        meta=meta or {},
    )


# ---------------------------------------------------------------------------
# Reservation (atomic, single-document conditional update -- no transaction)
# ---------------------------------------------------------------------------
async def resolve_upload_slot(user: Any, analysis_id: str) -> Dict[str, Any]:
    """Reserve (or pass through) a beta slot for one upload. Called ONCE, at
    the very top of ``analyze_perizia``, before any per-request work.

    Returns exactly one of:
    - {"mode": "NOT_BETA"} -- not an active beta member; normal path unchanged.
    - {"mode": "UNLIMITED", "membership_id"} -- active beta, UNLIMITED quota;
      no ledger row is written.
    - {"mode": "GRANTED", "usage_id", "membership_id"} -- LIMITED, slot
      reserved atomically.
    - {"mode": "FALLBACK", "membership_id"} -- LIMITED and exhausted (or a
      resolver/insert race); the caller falls through to the real credit
      check. Never silently upgraded to UNLIMITED on any failure.

    Never raises.
    """
    try:
        snapshot = getattr(user, "beta_program", None) or {}
        if not snapshot.get("active"):
            return {"mode": "NOT_BETA"}
        membership_id = snapshot.get("membership_id")
        if not membership_id:
            return {"mode": "NOT_BETA"}

        db = _db()
        # Idempotency guard: analysis_id is freshly minted per call, so this is
        # defensive (a retried background task / crash-recovery race).
        existing = await db[USAGE_COLLECTION].find_one({"analysis_id": analysis_id}, {"_id": 0})
        if existing:
            if existing.get("state") in (USAGE_RESERVED, USAGE_CONSUMED):
                return {
                    "mode": "GRANTED",
                    "usage_id": existing.get("usage_id"),
                    "membership_id": membership_id,
                    "duplicate": True,
                }
            return {"mode": "FALLBACK", "membership_id": membership_id, "duplicate": True}

        membership = await store.get_membership(membership_id)
        if not membership or membership.get("status") != store.STATUS_ACTIVE:
            return {"mode": "FALLBACK", "membership_id": membership_id}

        quota_mode = membership.get("quota_mode") or QUOTA_MODE_UNLIMITED
        if quota_mode != QUOTA_MODE_LIMITED:
            return {"mode": "UNLIMITED", "membership_id": membership_id}

        quota_version = membership.get("quota_version") or 1
        updated = await db[store.MEMBERSHIPS_COLLECTION].find_one_and_update(
            {
                "membership_id": membership_id,
                "status": store.STATUS_ACTIVE,
                "quota_mode": QUOTA_MODE_LIMITED,
                "quota_version": quota_version,
                "$expr": {
                    "$lt": [
                        {"$add": ["$analysis_consumed", "$analysis_reserved"]},
                        "$analysis_limit",
                    ]
                },
            },
            {"$inc": {"analysis_reserved": 1}},
            return_document=ReturnDocument.AFTER,
        )
        if updated is None:
            # Either genuinely exhausted, or the phase/version changed mid-flight.
            return {"mode": "FALLBACK", "membership_id": membership_id}

        now = _now()
        usage_doc = {
            "usage_id": _new_usage_id(),
            "membership_id": membership_id,
            "user_id": getattr(user, "user_id", None),
            "normalized_email": store.normalize_beta_email(getattr(user, "email", None)),
            "analysis_id": analysis_id,
            "quota_version": quota_version,
            "state": USAGE_RESERVED,
            "reserved_at": now,
            "consumed_at": None,
            "released_at": None,
            "release_reason": None,
            "paid_processing_started_at": None,
            "created_at": now,
            "updated_at": now,
        }
        try:
            await db[USAGE_COLLECTION].insert_one(dict(usage_doc))
        except Exception as exc:  # pragma: no cover - should be impossible (fresh id)
            # Compensate the reservation so a phantom slot is never leaked.
            await db[store.MEMBERSHIPS_COLLECTION].find_one_and_update(
                {"membership_id": membership_id, "analysis_reserved": {"$gte": 1}},
                {"$inc": {"analysis_reserved": -1}},
            )
            logger.warning("beta quota usage insert failed analysis_id=%s: %s", analysis_id, exc)
            return {"mode": "FALLBACK", "membership_id": membership_id}

        await _write_quota_audit(
            ACTION_QUOTA_SLOT_RESERVED,
            updated,
            actor_type=ACTOR_SYSTEM_UPLOAD,
            meta={
                "analysis_id": analysis_id,
                "quota_version": quota_version,
                "consumed": updated.get("analysis_consumed"),
                "reserved": updated.get("analysis_reserved"),
                "limit": updated.get("analysis_limit"),
            },
        )
        if _is_exhausted(updated):
            await _write_quota_audit(
                ACTION_QUOTA_EXHAUSTED,
                updated,
                actor_type=ACTOR_SYSTEM_UPLOAD,
                meta={
                    "analysis_id": analysis_id,
                    "quota_version": quota_version,
                    "consumed": updated.get("analysis_consumed"),
                    "reserved": updated.get("analysis_reserved"),
                    "limit": updated.get("analysis_limit"),
                },
            )
        return {"mode": "GRANTED", "usage_id": usage_doc["usage_id"], "membership_id": membership_id}
    except Exception as exc:  # never raises; degrade to FALLBACK, never UNLIMITED
        logger.warning("resolve_upload_slot failed analysis_id=%s: %s", analysis_id, exc)
        return {"mode": "FALLBACK"}


# ---------------------------------------------------------------------------
# The paid-processing marker (owner amendment)
# ---------------------------------------------------------------------------
async def mark_paid_processing_started(analysis_id: str) -> None:
    """Idempotent marker: "paid processing has begun for this reservation."

    Call immediately BEFORE each verified paid call site in
    ``analyze_perizia``'s ``run_pipeline`` (Document AI OCR, the QA-gate LLM
    call, the Gemini narrator). Safe (and required) to call unconditionally
    for every analysis, beta or not -- it is a no-op when there is no RESERVED
    usage row for this analysis_id (normal customers, UNLIMITED beta members,
    or a row that has already moved past RESERVED). Never raises, never
    blocks (single indexed point update).
    """
    if not analysis_id:
        return
    try:
        db = _db()
        now = _now()
        await db[USAGE_COLLECTION].update_one(
            {
                "analysis_id": analysis_id,
                "state": USAGE_RESERVED,
                "paid_processing_started_at": None,
            },
            {"$set": {"paid_processing_started_at": now, "updated_at": now}},
        )
    except Exception as exc:
        logger.warning("mark_paid_processing_started failed analysis_id=%s: %s", analysis_id, exc)


async def consume_slot(
    analysis_id: str, *, reason: str = REASON_CONSUMED_PAID_PROCESSING_STARTED
) -> bool:
    """RESERVED -> CONSUMED; membership counters move (reserved-1, consumed+1).

    No-op (returns False) when there is no RESERVED row for analysis_id (not a
    beta upload, already resolved, or a lost race against a release). Never
    raises.
    """
    if not analysis_id:
        return False
    try:
        db = _db()
        now = _now()
        row = await db[USAGE_COLLECTION].find_one_and_update(
            {"analysis_id": analysis_id, "state": USAGE_RESERVED},
            {"$set": {"state": USAGE_CONSUMED, "consumed_at": now, "updated_at": now}},
            return_document=ReturnDocument.AFTER,
        )
        if row is None:
            return False
        membership_id = row.get("membership_id")
        before = await db[store.MEMBERSHIPS_COLLECTION].find_one(
            {"membership_id": membership_id}, {"_id": 0}
        )
        updated = await db[store.MEMBERSHIPS_COLLECTION].find_one_and_update(
            {"membership_id": membership_id, "analysis_reserved": {"$gte": 1}},
            {"$inc": {"analysis_reserved": -1, "analysis_consumed": 1}},
            return_document=ReturnDocument.AFTER,
        )
        if updated is None:  # pragma: no cover - defensive (would drive negative)
            return True
        await _write_quota_audit(
            ACTION_QUOTA_SLOT_CONSUMED,
            updated,
            actor_type=ACTOR_SYSTEM_UPLOAD,
            meta={
                "analysis_id": analysis_id,
                "reason": reason,
                "quota_version": updated.get("quota_version"),
                "consumed": updated.get("analysis_consumed"),
                "reserved": updated.get("analysis_reserved"),
                "limit": updated.get("analysis_limit"),
            },
        )
        if _is_exhausted(updated) and not (before and _is_exhausted(before)):
            await _write_quota_audit(
                ACTION_QUOTA_EXHAUSTED,
                updated,
                actor_type=ACTOR_SYSTEM_UPLOAD,
                meta={
                    "analysis_id": analysis_id,
                    "quota_version": updated.get("quota_version"),
                    "consumed": updated.get("analysis_consumed"),
                    "reserved": updated.get("analysis_reserved"),
                    "limit": updated.get("analysis_limit"),
                },
            )
        return True
    except Exception as exc:
        logger.warning("consume_slot failed analysis_id=%s: %s", analysis_id, exc)
        return False


async def release_slot(analysis_id: str, *, reason: str) -> bool:
    """RESERVED -> RELEASED; membership counters move (reserved-1).

    No-op (returns False) when there is no RESERVED row for analysis_id.
    Never raises.
    """
    if not analysis_id:
        return False
    try:
        db = _db()
        now = _now()
        row = await db[USAGE_COLLECTION].find_one_and_update(
            {"analysis_id": analysis_id, "state": USAGE_RESERVED},
            {
                "$set": {
                    "state": USAGE_RELEASED,
                    "released_at": now,
                    "release_reason": reason,
                    "updated_at": now,
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        if row is None:
            return False
        membership_id = row.get("membership_id")
        before = await db[store.MEMBERSHIPS_COLLECTION].find_one(
            {"membership_id": membership_id}, {"_id": 0}
        )
        was_exhausted = bool(before and _is_exhausted(before))
        updated = await db[store.MEMBERSHIPS_COLLECTION].find_one_and_update(
            {"membership_id": membership_id, "analysis_reserved": {"$gte": 1}},
            {"$inc": {"analysis_reserved": -1}},
            return_document=ReturnDocument.AFTER,
        )
        if updated is None:  # pragma: no cover - defensive (would drive negative)
            return True
        await _write_quota_audit(
            ACTION_QUOTA_SLOT_RELEASED,
            updated,
            actor_type=ACTOR_SYSTEM_UPLOAD,
            meta={
                "analysis_id": analysis_id,
                "reason": reason,
                "quota_version": updated.get("quota_version"),
                "consumed": updated.get("analysis_consumed"),
                "reserved": updated.get("analysis_reserved"),
                "limit": updated.get("analysis_limit"),
            },
        )
        if was_exhausted and not _is_exhausted(updated):
            await _write_quota_audit(
                ACTION_QUOTA_AVAILABLE_AGAIN,
                updated,
                actor_type=ACTOR_SYSTEM_UPLOAD,
                meta={
                    "analysis_id": analysis_id,
                    "quota_version": updated.get("quota_version"),
                    "consumed": updated.get("analysis_consumed"),
                    "reserved": updated.get("analysis_reserved"),
                    "limit": updated.get("analysis_limit"),
                },
            )
        return True
    except Exception as exc:
        logger.warning("release_slot failed analysis_id=%s: %s", analysis_id, exc)
        return False


async def finalize_on_failure(analysis_id: str, *, reason_if_before_paid: str) -> Optional[str]:
    """Call at every early-exit failure point in ``analyze_perizia``.

    Owner amendment: if paid processing had already begun for this
    reservation (``paid_processing_started_at`` set), CONSUME rather than
    release -- real money was already spent. Otherwise RELEASE with the
    call-site-specific reason.

    Returns "CONSUMED" | "RELEASED" | None (no reservation existed / already
    resolved). Never raises.
    """
    if not analysis_id:
        return None
    try:
        db = _db()
        row = await db[USAGE_COLLECTION].find_one({"analysis_id": analysis_id}, {"_id": 0})
        if not row or row.get("state") != USAGE_RESERVED:
            return None
        if row.get("paid_processing_started_at"):
            await consume_slot(analysis_id, reason=REASON_CONSUMED_PAID_PROCESSING_STARTED)
            return "CONSUMED"
        await release_slot(analysis_id, reason=reason_if_before_paid)
        return "RELEASED"
    except Exception as exc:
        logger.warning("finalize_on_failure failed analysis_id=%s: %s", analysis_id, exc)
        return None


# ---------------------------------------------------------------------------
# Owner actions: set mode/limit, start a new phase, list historical phases.
# ---------------------------------------------------------------------------
async def set_quota(
    *,
    membership_id: str,
    quota_mode: str,
    analysis_limit: Optional[int],
    actor_email: str,
    actor_user_id: str,
) -> Dict[str, Any]:
    """Set quota mode/limit for a membership.

    Increasing a limit preserves ``analysis_consumed`` (only ``analysis_limit``
    changes). Lowering below ``analysis_consumed`` is allowed -- the derived
    state becomes EXHAUSTED immediately via the reservation filter's ``$expr``;
    no retroactive charge, no negative counter, no special-case code needed.
    """
    if quota_mode not in QUOTA_MODES:
        raise store.BetaProgramError(422, "INVALID_QUOTA_MODE", "Modalità quota non valida.")
    db = _db()
    existing = await db[store.MEMBERSHIPS_COLLECTION].find_one(
        {"membership_id": membership_id}, {"_id": 0}
    )
    if not existing:
        raise store.BetaProgramError(404, "MEMBERSHIP_NOT_FOUND", "Membership non trovata.")

    if quota_mode == QUOTA_MODE_UNLIMITED:
        limit_value: Optional[int] = None
    else:
        try:
            limit_value = int(analysis_limit)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            raise store.BetaProgramError(
                422, "INVALID_ANALYSIS_LIMIT", "Il limite deve essere un intero positivo."
            )
        if limit_value < 1 or limit_value > MAX_ANALYSIS_LIMIT:
            raise store.BetaProgramError(
                422,
                "INVALID_ANALYSIS_LIMIT",
                f"Il limite deve essere compreso tra 1 e {MAX_ANALYSIS_LIMIT}.",
            )

    now = _now()
    set_fields: Dict[str, Any] = {
        "quota_mode": quota_mode,
        "analysis_limit": limit_value,
        "quota_updated_at": now,
        "quota_updated_by": actor_email,
        "updated_at": now,
    }
    # Backfill any missing quota fields for a pre-feature membership document
    # (additive; never overwrites an existing counter).
    for field, default in _quota_defaults(now).items():
        if field not in existing and field not in set_fields:
            set_fields[field] = default
    if existing.get("quota_period_started_at") is None and "quota_period_started_at" not in set_fields:
        set_fields["quota_period_started_at"] = now

    await db[store.MEMBERSHIPS_COLLECTION].update_one(
        {"membership_id": membership_id}, {"$set": set_fields}
    )
    updated = await db[store.MEMBERSHIPS_COLLECTION].find_one(
        {"membership_id": membership_id}, {"_id": 0}
    )

    old_mode = existing.get("quota_mode") or QUOTA_MODE_UNLIMITED
    old_limit = existing.get("analysis_limit")
    if old_mode != quota_mode:
        await _write_quota_audit(
            ACTION_QUOTA_MODE_CHANGED,
            updated,
            actor_type=ACTOR_OWNER,
            actor_email=actor_email,
            actor_user_id=actor_user_id,
            meta={"old_mode": old_mode, "new_mode": quota_mode, "old_limit": old_limit, "new_limit": limit_value},
        )
    if old_limit != limit_value:
        await _write_quota_audit(
            ACTION_QUOTA_LIMIT_CHANGED,
            updated,
            actor_type=ACTOR_OWNER,
            actor_email=actor_email,
            actor_user_id=actor_user_id,
            meta={
                "old_limit": old_limit,
                "new_limit": limit_value,
                "consumed": updated.get("analysis_consumed"),
                "reserved": updated.get("analysis_reserved"),
            },
        )
    was_exhausted = _is_exhausted({**existing, "quota_mode": old_mode, "analysis_limit": old_limit})
    now_exhausted = _is_exhausted(updated)
    if now_exhausted and not was_exhausted:
        await _write_quota_audit(
            ACTION_QUOTA_EXHAUSTED, updated, actor_type=ACTOR_OWNER, actor_email=actor_email,
            actor_user_id=actor_user_id,
            meta={"consumed": updated.get("analysis_consumed"), "limit": updated.get("analysis_limit")},
        )
    if was_exhausted and not now_exhausted:
        await _write_quota_audit(
            ACTION_QUOTA_AVAILABLE_AGAIN, updated, actor_type=ACTOR_OWNER, actor_email=actor_email,
            actor_user_id=actor_user_id,
            meta={"consumed": updated.get("analysis_consumed"), "limit": updated.get("analysis_limit")},
        )
    return updated


async def start_new_phase(
    *, membership_id: str, actor_email: str, actor_user_id: str, force_release: bool = False
) -> Dict[str, Any]:
    """Bump ``quota_version``, reset consumed/reserved to 0, preserve history.

    Refuses to proceed while any RESERVED usage row exists for the current
    phase unless ``force_release`` is explicit (each such forced release is
    its own audited QUOTA_SLOT_RELEASED, reason PHASE_TRANSITION_FORCE_RELEASE)
    -- this avoids stranding a phantom reservation across a version bump.
    """
    db = _db()
    existing = await db[store.MEMBERSHIPS_COLLECTION].find_one(
        {"membership_id": membership_id}, {"_id": 0}
    )
    if not existing:
        raise store.BetaProgramError(404, "MEMBERSHIP_NOT_FOUND", "Membership non trovata.")

    quota_version = existing.get("quota_version") or 1
    open_reservations = await db[USAGE_COLLECTION].count_documents(
        {"membership_id": membership_id, "quota_version": quota_version, "state": USAGE_RESERVED}
    )
    if open_reservations and not force_release:
        raise store.BetaProgramError(
            409,
            "OPEN_RESERVATIONS_EXIST",
            "Esistono prenotazioni in corso per questa fase: forzare il rilascio per continuare.",
        )
    if open_reservations and force_release:
        cursor = db[USAGE_COLLECTION].find(
            {"membership_id": membership_id, "quota_version": quota_version, "state": USAGE_RESERVED},
            {"_id": 0},
        )
        stuck = await cursor.to_list(None)
        for row in stuck:
            await release_slot(row["analysis_id"], reason=REASON_PHASE_TRANSITION_FORCE_RELEASE)

    now = _now()
    new_version = quota_version + 1
    previous_consumed = int(existing.get("analysis_consumed") or 0)
    previous_reserved = int(existing.get("analysis_reserved") or 0)
    previous_limit = existing.get("analysis_limit")
    await db[store.MEMBERSHIPS_COLLECTION].update_one(
        {"membership_id": membership_id},
        {
            "$set": {
                "quota_version": new_version,
                "analysis_consumed": 0,
                "analysis_reserved": 0,
                "quota_period_started_at": now,
                "quota_updated_at": now,
                "quota_updated_by": actor_email,
                "updated_at": now,
            }
        },
    )
    updated = await db[store.MEMBERSHIPS_COLLECTION].find_one(
        {"membership_id": membership_id}, {"_id": 0}
    )
    await _write_quota_audit(
        ACTION_QUOTA_PHASE_STARTED,
        updated,
        actor_type=ACTOR_OWNER,
        actor_email=actor_email,
        actor_user_id=actor_user_id,
        meta={
            "old_version": quota_version,
            "new_version": new_version,
            "previous_phase_consumed": previous_consumed,
            "previous_phase_reserved": previous_reserved,
            "previous_limit": previous_limit,
            "limit": updated.get("analysis_limit"),
        },
    )
    return updated


async def list_phases(membership_id: str) -> Dict[str, Any]:
    """Historical phase view for the owner: ``{"items": [{quota_version,
    limit, consumed, started_at, ended_at, actor_email}]}`` -- reconstructed
    from the append-only audit trail (QUOTA_PHASE_STARTED transitions), newest
    phase last (current phase has ``ended_at: null``)."""
    membership = await store.get_membership(membership_id)
    if not membership:
        return {"items": []}

    audit = await store.list_audit(membership_id=membership_id, page=1, page_size=500)
    # list_audit sorts created_at DESC; walk chronologically.
    phase_starts = [
        a for a in reversed(audit["items"]) if a.get("action") == ACTION_QUOTA_PHASE_STARTED
    ]

    items = []
    boundary_at = membership.get("quota_period_started_at") or membership.get("added_at")
    boundary_actor = membership.get("quota_updated_by") or membership.get("added_by")
    for ev in phase_starts:
        meta = ev.get("meta") or {}
        items.append(
            {
                "quota_version": meta.get("old_version"),
                "limit": meta.get("previous_limit"),
                "consumed": meta.get("previous_phase_consumed"),
                "started_at": boundary_at,
                "ended_at": ev.get("created_at"),
                "actor_email": boundary_actor,
            }
        )
        boundary_at = ev.get("created_at")
        boundary_actor = ev.get("actor_email")

    items.append(
        {
            "quota_version": membership.get("quota_version") or 1,
            "limit": membership.get("analysis_limit"),
            "consumed": membership.get("analysis_consumed") or 0,
            "started_at": boundary_at,
            "ended_at": None,
            "actor_email": boundary_actor,
        }
    )
    return {"items": items}


# ---------------------------------------------------------------------------
# Stale reservation recovery (crash recovery sweep)
# ---------------------------------------------------------------------------
async def recover_stale_reservations(*, now: Optional[datetime] = None) -> Dict[str, Any]:
    """Indexed, bounded reconciliation for RESERVED rows past the safe
    duration. Never a full collection scan -- the query filters on ``state``
    first (a small subset), then ranges on ``reserved_at``. Age alone never
    releases a row: the sweep additionally consults the authoritative
    ``perizia_analyses`` record (if any) and the paid-processing marker.
    """
    db = _db()
    now_dt = now or datetime.now(timezone.utc)
    cutoff = (now_dt - timedelta(seconds=_stale_reservation_seconds())).isoformat()
    report = {"reconciled_to_consumed": 0, "released": 0, "scanned": 0}

    cursor = db[USAGE_COLLECTION].find(
        {"state": USAGE_RESERVED, "reserved_at": {"$lt": cutoff}}, {"_id": 0}
    )
    rows = await cursor.to_list(None)
    for row in rows:
        report["scanned"] += 1
        analysis_id = row.get("analysis_id")
        analysis = await db.perizia_analyses.find_one(
            {"analysis_id": analysis_id}, {"_id": 0, "status": 1}
        )
        if analysis and analysis.get("status") == "COMPLETED":
            await consume_slot(analysis_id, reason=REASON_CONSUMED_PAID_PROCESSING_STARTED)
            report["reconciled_to_consumed"] += 1
        elif row.get("paid_processing_started_at"):
            # Owner amendment: paid processing definitely began for this
            # reservation -- consume regardless of the analysis' own terminal
            # status (or its absence, e.g. a crash right before persistence).
            await consume_slot(analysis_id, reason=REASON_CONSUMED_PAID_PROCESSING_STARTED)
            report["reconciled_to_consumed"] += 1
        else:
            await release_slot(analysis_id, reason=REASON_STALE_RESERVATION_NO_ANALYSIS_FOUND)
            report["released"] += 1
    return report
