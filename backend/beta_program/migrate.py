"""
Idempotent migration from the legacy env/hardcoded beta allowlist to the
database-managed beta program.

Rules:
- Inputs: ``BETA_UNLIMITED_EMAILS`` (env), ``BETA_PARTNER_NAMES`` (in-source map,
  currently empty) and explicit ``--email`` args. Production env is empty, so the
  production run is a verified no-op — that is the desired end state.
- For each normalized email:
    * REVOKED membership -> SKIP, never override (hard bootstrap rule).
    * PENDING/ACTIVE membership -> skip (no field touched).
    * admin email -> skip.
    * else create ACTIVE+linked (if a users doc exists) or PENDING.
- Never creates/edits ``users`` docs; never touches wallets or feedback.
- Dry-run (default) writes nothing. Apply is safe to run repeatedly.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from . import quota, store

logger = logging.getLogger(__name__)


def _server():
    import server  # type: ignore  # lazy

    return server


async def apply_quota_defaults(*, dry_run: bool = True) -> Dict[str, Any]:
    """Backfill quota defaults (UNLIMITED/null/version=1/reserved=0/consumed=0)
    onto every existing membership that predates the quota feature (no
    ``quota_mode`` field at all). Idempotent: a membership that already has
    ``quota_mode`` set is skipped untouched -- safe to run repeatedly. Never
    retroactively counts historical analyses (only ever writes the additive
    defaults, never a nonzero ``analysis_consumed``).
    """
    server = _server()
    db = server.db
    report: Dict[str, Any] = {"dry_run": dry_run, "defaulted": [], "skipped_existing_quota": [], "total": 0}
    docs = await db[store.MEMBERSHIPS_COLLECTION].find({}, {"_id": 0}).to_list(None)
    report["total"] = len(docs)
    now = quota._now()
    for doc in docs:
        email = doc.get("normalized_email")
        if "quota_mode" in doc:
            report["skipped_existing_quota"].append(email)
            continue
        if dry_run:
            report["defaulted"].append(email)
            continue
        defaults = quota._quota_defaults(doc.get("added_at") or now)
        await db[store.MEMBERSHIPS_COLLECTION].update_one(
            {"membership_id": doc.get("membership_id")}, {"$set": defaults}
        )
        report["defaulted"].append(email)
    return report


async def run_migration(
    *,
    dry_run: bool = True,
    extra_emails: Optional[List[Dict[str, Any]]] = None,
    actor_email: str = "migration@system",
    actor_user_id: str = "migration",
) -> Dict[str, Any]:
    """Import legacy allowlist emails into ``beta_program_memberships``.

    Returns an explicit report. Idempotent: a second run all-skips.
    """
    server = _server()
    report: Dict[str, Any] = {
        "dry_run": dry_run,
        "migrated": [],
        "skipped_existing": [],
        "skipped_revoked": [],
        "skipped_admin": [],
        "total": 0,
    }

    # Collect candidate emails from env allowlist + in-source name map + explicit.
    candidates: Dict[str, Dict[str, Any]] = {}
    for email in getattr(server, "BETA_UNLIMITED_EMAILS", frozenset()):
        normalized = store.normalize_beta_email(email)
        if normalized:
            candidates.setdefault(normalized, {"email": normalized, "source": "env_allowlist"})
    names_map = getattr(server, "BETA_PARTNER_NAMES", {}) or {}
    for email, name in names_map.items():
        normalized = store.normalize_beta_email(email)
        if normalized:
            entry = candidates.setdefault(
                normalized, {"email": normalized, "source": "env_allowlist"}
            )
            entry.setdefault("display_name", name)
    for extra in extra_emails or []:
        normalized = store.normalize_beta_email(extra.get("email"))
        if normalized:
            entry = candidates.setdefault(
                normalized, {"email": normalized, "source": "manual_admin"}
            )
            if extra.get("name"):
                entry["display_name"] = extra["name"]
            entry["source"] = "manual_admin"

    report["total"] = len(candidates)

    db = server.db
    for normalized, entry in sorted(candidates.items()):
        if server._is_admin_email(normalized):
            report["skipped_admin"].append(normalized)
            continue
        existing = await db[store.MEMBERSHIPS_COLLECTION].find_one(
            {"normalized_email": normalized}, {"_id": 0}
        )
        if existing:
            if existing.get("status") == store.STATUS_REVOKED:
                report["skipped_revoked"].append(normalized)
            else:
                report["skipped_existing"].append(normalized)
            continue

        if dry_run:
            report["migrated"].append(normalized)
            continue

        async def _user_lookup(email: str):
            return await db.users.find_one({"email": email}, {"_id": 0})

        try:
            membership = await store.add_tester(
                email=normalized,
                display_name=entry.get("display_name"),
                partner_type=None,
                internal_note=None,
                actor_email=actor_email,
                actor_user_id=actor_user_id,
                user_lookup=_user_lookup,
                is_admin_email=server._is_admin_email,
                migration_source=entry.get("source", "env_allowlist"),
                actor_type=store.ACTOR_MIGRATION,
            )
            report["migrated"].append(membership["normalized_email"])
        except store.BetaProgramError as exc:
            # Concurrent/duplicate — treat as skipped_existing.
            logger.warning("migration add skipped email=%s: %s", normalized, exc.reason_code)
            report["skipped_existing"].append(normalized)

    report["quota_defaults"] = await apply_quota_defaults(dry_run=dry_run)
    return report
