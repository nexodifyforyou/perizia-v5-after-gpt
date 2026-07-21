"""Canonical account identity: one user per verified normalized email.

Identity is the verified normalized email, never the authentication provider.
This module owns the field names and the uniqueness guarantee so that
``server``, the OTP endpoints and the migration script all agree on them.

The unique index lives on ``normalized_email`` rather than the historical
``email`` field: ``email`` predates normalization and is not guaranteed
canonical on older documents, so constraining it directly could fail against
real production data.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

USERS_COLLECTION = "users"
NORMALIZED_EMAIL_FIELD = "normalized_email"
NORMALIZED_EMAIL_INDEX = "uq_user_normalized_email"

# Authentication methods recordable on one account.
METHOD_GOOGLE = "google"
METHOD_EMAIL_OTP = "email_otp"
METHOD_LEGACY = "legacy"

_index_ready_cache: bool = False


def normalize_email(raw: Any) -> str:
    """Trim + lowercase — identical to the beta program's rule.

    Plus-addressing and dots survive, so ``name@x.it`` and ``name+beta@x.it``
    stay distinct accounts.
    """
    return str(raw or "").strip().lower()


async def ensure_unique_index(db) -> bool:
    """Create the unique identity index if it is safe to do so.

    Refuses when duplicate ``normalized_email`` groups exist, because creating
    the index would either fail outright or, worse, appear to succeed against a
    partially-migrated collection. Conflicts are reported for manual review; this
    never merges or deletes anything.
    """
    duplicates = await count_duplicate_groups(db)
    if duplicates:
        logger.warning(
            "auth_email identity index skipped: %s duplicate normalized_email group(s) "
            "require manual review",
            duplicates,
        )
        return False
    try:
        await db[USERS_COLLECTION].create_index(
            NORMALIZED_EMAIL_FIELD,
            unique=True,
            name=NORMALIZED_EMAIL_INDEX,
            background=True,
            partialFilterExpression={NORMALIZED_EMAIL_FIELD: {"$type": "string"}},
        )
        return True
    except Exception as exc:
        logger.warning("auth_email identity index creation failed: %s", exc)
        return False


async def count_duplicate_groups(db) -> int:
    """Number of normalized emails held by more than one user document."""
    pipeline = [
        {"$match": {NORMALIZED_EMAIL_FIELD: {"$type": "string", "$ne": ""}}},
        {"$group": {"_id": f"${NORMALIZED_EMAIL_FIELD}", "n": {"$sum": 1}}},
        {"$match": {"n": {"$gt": 1}}},
        {"$count": "groups"},
    ]
    try:
        rows = await db[USERS_COLLECTION].aggregate(pipeline).to_list(length=1)
    except Exception as exc:
        logger.warning("auth_email duplicate scan failed: %s", exc)
        # Fail closed: treat an unreadable collection as unsafe to index.
        return 1
    if not rows:
        return 0
    return int(rows[0].get("groups") or 0)


async def unique_index_ready(db, *, use_cache: bool = True) -> bool:
    """Whether the uniqueness guarantee OTP relies on is actually in place."""
    global _index_ready_cache
    if use_cache and _index_ready_cache:
        return True
    try:
        info = await db[USERS_COLLECTION].index_information()
    except Exception as exc:
        logger.warning("auth_email index_information failed: %s", exc)
        return False
    ready = False
    for name, spec in (info or {}).items():
        if name != NORMALIZED_EMAIL_INDEX:
            continue
        if not spec.get("unique"):
            continue
        keys = spec.get("key") or []
        fields = [k[0] if isinstance(k, (list, tuple)) else k for k in keys]
        if fields and fields[0] == NORMALIZED_EMAIL_FIELD:
            ready = True
            break
    if ready:
        _index_ready_cache = True
    return ready


def reset_index_cache() -> None:
    """Test hook: forget the cached index-readiness result."""
    global _index_ready_cache
    _index_ready_cache = False
