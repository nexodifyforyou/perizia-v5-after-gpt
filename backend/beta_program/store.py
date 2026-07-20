"""
Beta program membership store — the runtime source of truth for beta entitlement.

Collections:
- ``beta_program_memberships`` : one document per normalized email, ever
  (``PENDING`` -> ``ACTIVE`` -> ``REVOKED`` -> ``ACTIVE`` ...). Unique on
  ``normalized_email``.
- ``beta_program_audit``        : append-only history; never updated or deleted.

Design invariants (mirrored by tests):
- Beta is an entitlement, never a wallet balance. Nothing here writes credits.
- Admin/owner emails are never testers (refused at add time; admin wins in the
  resolver).
- Revocation applies on the next authenticated request (per-request resolution
  in ``server.get_current_user``); no session surgery, no restart.
- A REVOKED membership is never reactivated implicitly (login only touches
  PENDING); reactivation is one explicit owner call.
- The resolved ``beta_program`` snapshot is request-scoped and must never be
  persisted into ``db.users``.
"""

from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

MEMBERSHIPS_COLLECTION = "beta_program_memberships"
AUDIT_COLLECTION = "beta_program_audit"

# Membership status values.
STATUS_PENDING = "PENDING"
STATUS_ACTIVE = "ACTIVE"
STATUS_REVOKED = "REVOKED"
MEMBERSHIP_STATUSES = frozenset({STATUS_PENDING, STATUS_ACTIVE, STATUS_REVOKED})

# Partner types (display only; never affects entitlement).
PARTNER_TYPES = frozenset({"geometra", "avvocato", "investitore", "altro"})
DEFAULT_PARTNER_TYPE = "geometra"

# Audit actions (append-only).
ACTION_ADDED = "MEMBER_ADDED"
ACTION_ACTIVATED = "MEMBER_ACTIVATED"
ACTION_REVOKED = "MEMBER_REVOKED"
ACTION_REACTIVATED = "MEMBER_REACTIVATED"
ACTION_NOTE_UPDATED = "MEMBER_NOTE_UPDATED"
ACTION_LINKED = "MEMBER_LINKED_TO_USER"
ACTION_MIGRATION_IMPORTED = "MIGRATION_IMPORTED"
ACTION_MIGRATION_SKIPPED = "MIGRATION_SKIPPED"

# Audit actor types.
ACTOR_OWNER = "OWNER"
ACTOR_SYSTEM_LOGIN = "SYSTEM_LOGIN"
ACTOR_MIGRATION = "MIGRATION"

_MAX_NOTE_CHARS = 2000
_MAX_NAME_CHARS = 200

# Set once per process after ensure_indexes succeeds.
_indexes_ready = False


def _db():
    import server  # type: ignore  # lazy: avoid circular import with server.py

    return server.db


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_membership_id() -> str:
    return f"betam_{uuid.uuid4().hex[:16]}"


def _new_audit_id() -> str:
    return f"betaaud_{uuid.uuid4().hex[:16]}"


def normalize_beta_email(raw: Any) -> str:
    """Trim + lowercase — the single identity rule for the beta program.

    Matches ``server._parse_email_allowlist`` and the authenticated-user email
    normalization so a membership and a user always key on the same value.
    """
    return str(raw or "").strip().lower()


def _sanitize_text(value: Any, limit: int) -> Optional[str]:
    if value is None:
        return None
    text = str(value)
    # Strip control chars (never rewrite the meaning of owner-entered text).
    text = "".join(ch for ch in text if ch == "\n" or ch == "\t" or ord(ch) >= 32)
    text = text.strip()
    if not text:
        return None
    return text[:limit]


def _coerce_partner_type(value: Any) -> str:
    candidate = str(value or "").strip().lower()
    return candidate if candidate in PARTNER_TYPES else DEFAULT_PARTNER_TYPE


async def ensure_indexes() -> None:
    """Create additive indexes for memberships + audit (idempotent)."""
    global _indexes_ready
    if _indexes_ready:
        return
    db = _db()
    await db[MEMBERSHIPS_COLLECTION].create_index(
        "normalized_email", unique=True, name="uq_beta_membership_email", background=True
    )
    await db[MEMBERSHIPS_COLLECTION].create_index(
        [("status", 1), ("updated_at", -1)], name="ix_beta_membership_status", background=True
    )
    await db[MEMBERSHIPS_COLLECTION].create_index(
        "user_id", name="ix_beta_membership_user", background=True
    )
    await db[AUDIT_COLLECTION].create_index(
        [("membership_id", 1), ("created_at", -1)], name="ix_beta_audit_membership", background=True
    )
    await db[AUDIT_COLLECTION].create_index(
        [("created_at", -1)], name="ix_beta_audit_created", background=True
    )
    _indexes_ready = True


# ---------------------------------------------------------------------------
# Audit (append-only)
# ---------------------------------------------------------------------------
async def _write_audit(
    *,
    action: str,
    membership: Dict[str, Any],
    actor_type: str,
    actor_email: Optional[str] = None,
    actor_user_id: Optional[str] = None,
    before_status: Optional[str] = None,
    after_status: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    """Insert one append-only audit row. Never blocks the mutation on failure."""
    audit = {
        "audit_id": _new_audit_id(),
        "membership_id": membership.get("membership_id"),
        "normalized_email": membership.get("normalized_email"),
        "action": action,
        "actor_type": actor_type,
        "actor_email": actor_email,
        "actor_user_id": actor_user_id,
        "target_user_id": membership.get("user_id"),
        "before_status": before_status,
        "after_status": after_status,
        "entitlement_version": membership.get("entitlement_version"),
        "meta": meta or {},
        "created_at": _now(),
    }
    try:
        await _db()[AUDIT_COLLECTION].insert_one(dict(audit))
    except Exception as exc:  # pragma: no cover - telemetry tolerance
        logger.warning("beta_program audit insert failed action=%s: %s", action, exc)


# ---------------------------------------------------------------------------
# Per-request resolution
# ---------------------------------------------------------------------------
async def resolve_snapshot(email: Any) -> Dict[str, Any]:
    """Return the request-scoped beta snapshot for ``email``.

    ``{}`` when there is no ACTIVE membership; otherwise a small dict consumed by
    ``server._normalize_account_state``. This is the ONLY runtime source of beta
    entitlement — the environment allowlist is never consulted here.

    Never raises: a resolution failure degrades to "no beta" (fail-closed).
    """
    normalized = normalize_beta_email(email)
    if not normalized:
        return {}
    try:
        doc = await _db()[MEMBERSHIPS_COLLECTION].find_one(
            {"normalized_email": normalized, "status": STATUS_ACTIVE},
            {
                "_id": 0,
                "membership_id": 1,
                "display_name": 1,
                "partner_type": 1,
                "activated_at": 1,
                "entitlement_version": 1,
                "quota_mode": 1,
                "analysis_limit": 1,
                "analysis_consumed": 1,
                "analysis_reserved": 1,
                "quota_version": 1,
            },
        )
    except Exception as exc:  # pragma: no cover - fail closed
        logger.warning("beta_program resolve failed email=%s: %s", normalized, exc)
        return {}
    if not doc:
        return {}
    return {
        "active": True,
        "membership_id": doc.get("membership_id"),
        "display_name": doc.get("display_name"),
        "partner_type": doc.get("partner_type") or DEFAULT_PARTNER_TYPE,
        "activated_at": doc.get("activated_at"),
        "entitlement_version": doc.get("entitlement_version"),
        # Quota fields (additive; missing on a pre-feature doc means
        # UNLIMITED, per beta_program.quota.derive_quota_state's defaults).
        "quota_mode": doc.get("quota_mode"),
        "analysis_limit": doc.get("analysis_limit"),
        "analysis_consumed": doc.get("analysis_consumed"),
        "analysis_reserved": doc.get("analysis_reserved"),
        "quota_version": doc.get("quota_version"),
    }


async def get_membership_by_email(email: Any) -> Optional[Dict[str, Any]]:
    normalized = normalize_beta_email(email)
    if not normalized:
        return None
    return await _db()[MEMBERSHIPS_COLLECTION].find_one(
        {"normalized_email": normalized}, {"_id": 0}
    )


async def get_membership(membership_id: str) -> Optional[Dict[str, Any]]:
    return await _db()[MEMBERSHIPS_COLLECTION].find_one(
        {"membership_id": membership_id}, {"_id": 0}
    )


# ---------------------------------------------------------------------------
# Login-time activation (PENDING only — never reactivates REVOKED)
# ---------------------------------------------------------------------------
async def link_pending_membership(email: Any, user_id: str) -> Optional[Dict[str, Any]]:
    """On authentication, activate a PENDING membership for this email.

    Matches ``status == PENDING`` ONLY: a REVOKED (or already ACTIVE) membership
    is never touched here, so logging in can never restore revoked beta access.
    No-op (returns None) when there is no pending membership. Never raises.
    """
    normalized = normalize_beta_email(email)
    if not normalized or not user_id:
        return None
    try:
        db = _db()
        now = _now()
        existing = await db[MEMBERSHIPS_COLLECTION].find_one(
            {"normalized_email": normalized, "status": STATUS_PENDING}, {"_id": 0}
        )
        if not existing:
            return None
        new_version = int(existing.get("entitlement_version") or 1) + 1
        await db[MEMBERSHIPS_COLLECTION].update_one(
            {"normalized_email": normalized, "status": STATUS_PENDING},
            {
                "$set": {
                    "status": STATUS_ACTIVE,
                    "user_id": user_id,
                    "activated_at": now,
                    "updated_at": now,
                    "last_entitlement_change_at": now,
                    "entitlement_version": new_version,
                }
            },
        )
        updated = await db[MEMBERSHIPS_COLLECTION].find_one(
            {"normalized_email": normalized}, {"_id": 0}
        )
        if updated:
            await _write_audit(
                action=ACTION_ACTIVATED,
                membership=updated,
                actor_type=ACTOR_SYSTEM_LOGIN,
                actor_user_id=user_id,
                before_status=STATUS_PENDING,
                after_status=STATUS_ACTIVE,
                meta={"linked_user": True},
            )
        return updated
    except Exception as exc:  # pragma: no cover - never break login
        logger.warning("beta_program link_pending failed email=%s: %s", normalized, exc)
        return None


# ---------------------------------------------------------------------------
# Owner-driven transitions
# ---------------------------------------------------------------------------
class BetaProgramError(Exception):
    """Raised for owner-facing conflicts; carries an HTTP status + reason_code."""

    def __init__(self, status_code: int, reason_code: str, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.reason_code = reason_code
        self.message = message


async def add_tester(
    *,
    email: Any,
    display_name: Optional[str],
    partner_type: Optional[str],
    internal_note: Optional[str],
    actor_email: str,
    actor_user_id: str,
    user_lookup,
    is_admin_email,
    migration_source: Optional[str] = None,
    actor_type: str = ACTOR_OWNER,
) -> Dict[str, Any]:
    """Add a tester by email.

    - Admin/owner email -> refused (400 OWNER_CANNOT_BE_TESTER).
    - Existing REVOKED membership -> refused (409 MEMBERSHIP_REVOKED); use
      reactivate explicitly.
    - Existing PENDING/ACTIVE -> refused (409 MEMBERSHIP_EXISTS).
    - Existing user account -> membership created ACTIVE + linked.
    - No account -> membership created PENDING.

    Never writes to ``db.users`` and never touches wallets/credits.
    """
    normalized = normalize_beta_email(email)
    if not normalized or "@" not in normalized:
        raise BetaProgramError(422, "INVALID_EMAIL", "Email non valida.")
    if is_admin_email(normalized):
        raise BetaProgramError(
            400, "OWNER_CANNOT_BE_TESTER", "L'owner/admin non può essere un tester."
        )

    db = _db()
    existing = await db[MEMBERSHIPS_COLLECTION].find_one(
        {"normalized_email": normalized}, {"_id": 0}
    )
    if existing:
        status = existing.get("status")
        if status == STATUS_REVOKED:
            raise BetaProgramError(
                409,
                "MEMBERSHIP_REVOKED",
                "Esiste già una membership revocata per questa email: usare Riattiva.",
            )
        raise BetaProgramError(
            409, "MEMBERSHIP_EXISTS", "Esiste già una membership per questa email."
        )

    user_doc = await user_lookup(normalized)
    now = _now()
    is_active = bool(user_doc)
    membership = {
        "membership_id": _new_membership_id(),
        "normalized_email": normalized,
        "user_id": (user_doc.get("user_id") if user_doc else None),
        "display_name": _sanitize_text(display_name, _MAX_NAME_CHARS),
        "partner_type": _coerce_partner_type(partner_type),
        "status": STATUS_ACTIVE if is_active else STATUS_PENDING,
        "added_by": actor_email,
        "added_at": now,
        "activated_at": now if is_active else None,
        "revoked_at": None,
        "reactivated_at": None,
        "updated_at": now,
        "internal_note": _sanitize_text(internal_note, _MAX_NOTE_CHARS),
        "entitlement_version": 1,
        "last_entitlement_change_at": now,
        "migration_source": migration_source,
        # Quota (perizia allowance) defaults -- every new membership starts
        # UNLIMITED; the owner opts a tester into LIMITED+N afterward via the
        # quota API. Additive fields, see beta_program/quota.py.
        "quota_mode": "UNLIMITED",
        "analysis_limit": None,
        "analysis_consumed": 0,
        "analysis_reserved": 0,
        "quota_version": 1,
        "quota_period_started_at": now,
        "quota_updated_at": None,
        "quota_updated_by": None,
        "quota_note": None,
    }
    try:
        await db[MEMBERSHIPS_COLLECTION].insert_one(dict(membership))
    except Exception as exc:
        # Unique index is the backstop against a race.
        raise BetaProgramError(
            409, "MEMBERSHIP_EXISTS", "Esiste già una membership per questa email."
        ) from exc

    await _write_audit(
        action=ACTION_MIGRATION_IMPORTED if actor_type == ACTOR_MIGRATION else ACTION_ADDED,
        membership=membership,
        actor_type=actor_type,
        actor_email=actor_email,
        actor_user_id=actor_user_id,
        before_status=None,
        after_status=membership["status"],
        meta={"linked_user": is_active, "migration_source": migration_source},
    )
    return membership


async def _transition(
    *,
    membership_id: str,
    allowed_from: Tuple[str, ...],
    new_status: str,
    action: str,
    actor_email: str,
    actor_user_id: str,
    timestamp_field: Optional[str],
    require_user: bool = False,
) -> Dict[str, Any]:
    db = _db()
    existing = await db[MEMBERSHIPS_COLLECTION].find_one(
        {"membership_id": membership_id}, {"_id": 0}
    )
    if not existing:
        raise BetaProgramError(404, "MEMBERSHIP_NOT_FOUND", "Membership non trovata.")
    current = existing.get("status")
    if current not in allowed_from:
        raise BetaProgramError(
            409,
            "INVALID_TRANSITION",
            f"Transizione non valida da {current} a {new_status}.",
        )

    now = _now()
    new_version = int(existing.get("entitlement_version") or 1) + 1
    set_fields: Dict[str, Any] = {
        "status": new_status,
        "updated_at": now,
        "last_entitlement_change_at": now,
        "entitlement_version": new_version,
    }
    if timestamp_field:
        set_fields[timestamp_field] = now

    # Reactivation of a linked account stays ACTIVE; a never-linked one goes back
    # to PENDING so a real login is still required to attach a user_id.
    resolved_new_status = new_status
    if new_status == STATUS_ACTIVE and require_user and not existing.get("user_id"):
        resolved_new_status = STATUS_PENDING
        set_fields["status"] = STATUS_PENDING

    await db[MEMBERSHIPS_COLLECTION].update_one(
        {"membership_id": membership_id}, {"$set": set_fields}
    )
    updated = await db[MEMBERSHIPS_COLLECTION].find_one(
        {"membership_id": membership_id}, {"_id": 0}
    )
    await _write_audit(
        action=action,
        membership=updated or existing,
        actor_type=ACTOR_OWNER,
        actor_email=actor_email,
        actor_user_id=actor_user_id,
        before_status=current,
        after_status=resolved_new_status,
    )
    return updated or existing


async def revoke(*, membership_id: str, actor_email: str, actor_user_id: str) -> Dict[str, Any]:
    """ACTIVE/PENDING -> REVOKED. Preserves account/reports/feedback/credits."""
    return await _transition(
        membership_id=membership_id,
        allowed_from=(STATUS_ACTIVE, STATUS_PENDING),
        new_status=STATUS_REVOKED,
        action=ACTION_REVOKED,
        actor_email=actor_email,
        actor_user_id=actor_user_id,
        timestamp_field="revoked_at",
    )


async def reactivate(*, membership_id: str, actor_email: str, actor_user_id: str) -> Dict[str, Any]:
    """REVOKED -> ACTIVE (or PENDING if never linked to a user). The only path."""
    return await _transition(
        membership_id=membership_id,
        allowed_from=(STATUS_REVOKED,),
        new_status=STATUS_ACTIVE,
        action=ACTION_REACTIVATED,
        actor_email=actor_email,
        actor_user_id=actor_user_id,
        timestamp_field="reactivated_at",
        require_user=True,
    )


async def update_metadata(
    *,
    membership_id: str,
    display_name: Any = ...,
    partner_type: Any = ...,
    internal_note: Any = ...,
    actor_email: str,
    actor_user_id: str,
) -> Dict[str, Any]:
    """Edit display_name / partner_type / internal_note ONLY (never status)."""
    db = _db()
    existing = await db[MEMBERSHIPS_COLLECTION].find_one(
        {"membership_id": membership_id}, {"_id": 0}
    )
    if not existing:
        raise BetaProgramError(404, "MEMBERSHIP_NOT_FOUND", "Membership non trovata.")

    set_fields: Dict[str, Any] = {"updated_at": _now()}
    changed: Dict[str, bool] = {}
    if display_name is not ...:
        set_fields["display_name"] = _sanitize_text(display_name, _MAX_NAME_CHARS)
        changed["display_name"] = True
    if partner_type is not ...:
        set_fields["partner_type"] = _coerce_partner_type(partner_type)
        changed["partner_type"] = True
    if internal_note is not ...:
        set_fields["internal_note"] = _sanitize_text(internal_note, _MAX_NOTE_CHARS)
        changed["internal_note"] = True

    await db[MEMBERSHIPS_COLLECTION].update_one(
        {"membership_id": membership_id}, {"$set": set_fields}
    )
    updated = await db[MEMBERSHIPS_COLLECTION].find_one(
        {"membership_id": membership_id}, {"_id": 0}
    )
    await _write_audit(
        action=ACTION_NOTE_UPDATED,
        membership=updated or existing,
        actor_type=ACTOR_OWNER,
        actor_email=actor_email,
        actor_user_id=actor_user_id,
        before_status=existing.get("status"),
        after_status=(updated or existing).get("status"),
        meta={"fields_changed": sorted(changed.keys())},
    )
    return updated or existing


# ---------------------------------------------------------------------------
# Listing / detail (owner views)
# ---------------------------------------------------------------------------
def public_membership(doc: Dict[str, Any], *, include_note: bool = True) -> Dict[str, Any]:
    """Owner-facing membership view. Hides raw Mongo _id; keeps internal_note
    only for owner responses (never surfaced to the tester)."""
    if not doc:
        return {}
    view = {
        "membership_id": doc.get("membership_id"),
        "normalized_email": doc.get("normalized_email"),
        "user_id": doc.get("user_id"),
        "account_linked": bool(doc.get("user_id")),
        "display_name": doc.get("display_name"),
        "partner_type": doc.get("partner_type"),
        "status": doc.get("status"),
        "added_by": doc.get("added_by"),
        "added_at": doc.get("added_at"),
        "activated_at": doc.get("activated_at"),
        "revoked_at": doc.get("revoked_at"),
        "reactivated_at": doc.get("reactivated_at"),
        "updated_at": doc.get("updated_at"),
        "entitlement_version": doc.get("entitlement_version"),
        "migration_source": doc.get("migration_source"),
        # Quota (raw stored fields; the derived {mode, limit, consumed,
        # reserved, remaining, state, quota_version} block is added by the
        # API layer as "quota" -- see beta_program/quota.py:derive_quota_state,
        # not duplicated here to avoid a store<->quota import cycle).
        "quota_mode": doc.get("quota_mode") or "UNLIMITED",
        "analysis_limit": doc.get("analysis_limit"),
        "analysis_consumed": doc.get("analysis_consumed") or 0,
        "analysis_reserved": doc.get("analysis_reserved") or 0,
        "quota_version": doc.get("quota_version") or 1,
        "quota_period_started_at": doc.get("quota_period_started_at"),
        "quota_updated_at": doc.get("quota_updated_at"),
        "quota_updated_by": doc.get("quota_updated_by"),
    }
    if include_note:
        view["internal_note"] = doc.get("internal_note")
        view["quota_note"] = doc.get("quota_note")
    return view


async def list_memberships(
    *,
    status: Optional[str] = None,
    q: Optional[str] = None,
    page: int = 1,
    page_size: int = 25,
) -> Dict[str, Any]:
    db = _db()
    query: Dict[str, Any] = {}
    if status and status in MEMBERSHIP_STATUSES:
        query["status"] = status
    if q:
        pattern = re.escape(str(q).strip())
        if pattern:
            query["$or"] = [
                {"normalized_email": {"$regex": pattern, "$options": "i"}},
                {"display_name": {"$regex": pattern, "$options": "i"}},
            ]
    page = max(1, int(page or 1))
    page_size = max(1, min(100, int(page_size or 25)))
    total = await db[MEMBERSHIPS_COLLECTION].count_documents(query)
    cursor = (
        db[MEMBERSHIPS_COLLECTION]
        .find(query, {"_id": 0})
        .sort("updated_at", -1)
        .skip((page - 1) * page_size)
        .limit(page_size)
    )
    items = await cursor.to_list(page_size)
    return {"items": items, "total": total, "page": page, "page_size": page_size}


async def list_audit(
    *,
    membership_id: Optional[str] = None,
    action: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
) -> Dict[str, Any]:
    db = _db()
    query: Dict[str, Any] = {}
    if membership_id:
        query["membership_id"] = membership_id
    if action:
        query["action"] = action
    page = max(1, int(page or 1))
    page_size = max(1, min(200, int(page_size or 50)))
    total = await db[AUDIT_COLLECTION].count_documents(query)
    cursor = (
        db[AUDIT_COLLECTION]
        .find(query, {"_id": 0})
        .sort("created_at", -1)
        .skip((page - 1) * page_size)
        .limit(page_size)
    )
    items = await cursor.to_list(page_size)
    return {"items": items, "total": total, "page": page, "page_size": page_size}


async def status_counts() -> Dict[str, int]:
    """{PENDING, ACTIVE, REVOKED} counts for the overview (deterministic)."""
    db = _db()
    counts = {STATUS_PENDING: 0, STATUS_ACTIVE: 0, STATUS_REVOKED: 0}
    for status in list(counts.keys()):
        counts[status] = await db[MEMBERSHIPS_COLLECTION].count_documents({"status": status})
    return counts


async def active_and_revoked_user_ids() -> Dict[str, List[str]]:
    """Linked user_ids grouped by ACTIVE vs REVOKED (for signal scoping)."""
    db = _db()
    result: Dict[str, List[str]] = {STATUS_ACTIVE: [], STATUS_REVOKED: []}
    cursor = db[MEMBERSHIPS_COLLECTION].find(
        {"user_id": {"$ne": None}}, {"_id": 0, "user_id": 1, "status": 1}
    )
    docs = await cursor.to_list(None)
    for doc in docs:
        status = doc.get("status")
        uid = doc.get("user_id")
        if uid and status in (STATUS_ACTIVE, STATUS_REVOKED):
            result[status].append(uid)
    return result
