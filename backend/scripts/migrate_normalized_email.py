"""Backfill canonical identity fields on ``users`` and create the unique index.

Identity for passwordless login is the verified normalized email, so every user
document needs a canonical ``normalized_email`` before uniqueness can be
enforced. This script establishes that, safely and idempotently.

Safety contract:

- ``--dry-run`` (the default) opens no write path at all. It reports only.
- ``--apply`` refuses to run while any duplicate ``normalized_email`` group
  exists. Conflicting historical accounts are reported for manual review and are
  never merged, deleted or renumbered.
- ``user_id`` is never changed. Credits, reports, subscriptions, beta
  memberships, quota and feedback are never read for modification and never
  written.
- ``email_verified`` is set true only where a provider actually guaranteed it
  (a recorded Google login). Legacy-provider accounts stay unverified.
- Re-running after a successful apply is a no-op, which is how idempotency is
  demonstrated.

Usage:
    backend/.venv/bin/python backend/scripts/migrate_normalized_email.py --dry-run
    backend/.venv/bin/python backend/scripts/migrate_normalized_email.py --apply
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from auth_email import identity as auth_identity  # noqa: E402


def _mask(email: str) -> str:
    """Mask an address for operator output; never print full addresses."""
    text = str(email or "")
    if "@" not in text:
        return "***"
    local, _, domain = text.partition("@")
    head = local[:2] if len(local) > 2 else local[:1]
    return f"{head}***@{domain}"


async def _scan(db) -> Dict[str, Any]:
    users = db[auth_identity.USERS_COLLECTION]

    total = await users.count_documents({})
    missing = await users.count_documents(
        {auth_identity.NORMALIZED_EMAIL_FIELD: {"$exists": False}}
    )
    blank_email = await users.count_documents(
        {"$or": [{"email": {"$exists": False}}, {"email": ""}, {"email": None}]}
    )

    # Group on the canonical value derived from whichever field is present, so
    # duplicates are detected even before the backfill has run.
    pipeline = [
        {
            "$project": {
                "user_id": 1,
                "canonical": {
                    "$toLower": {
                        "$trim": {
                            "input": {
                                "$ifNull": [
                                    f"${auth_identity.NORMALIZED_EMAIL_FIELD}",
                                    {"$ifNull": ["$email", ""]},
                                ]
                            }
                        }
                    }
                },
                "auth_methods": 1,
                "perizia_credits": 1,
                "subscription_state": 1,
            }
        },
        {"$match": {"canonical": {"$ne": ""}}},
        {
            "$group": {
                "_id": "$canonical",
                "n": {"$sum": 1},
                "user_ids": {"$push": "$user_id"},
                "auth_methods": {"$push": "$auth_methods"},
            }
        },
        {"$match": {"n": {"$gt": 1}}},
        {"$sort": {"n": -1}},
    ]
    duplicate_groups: List[Dict[str, Any]] = await users.aggregate(pipeline).to_list(length=500)

    # For each conflicting account, note whether value would be at stake in a
    # merge. This is decision support for a human, not an instruction to act.
    enriched: List[Dict[str, Any]] = []
    for group in duplicate_groups:
        detail = {
            "email_masked": _mask(group["_id"]),
            "count": group["n"],
            "user_ids": group["user_ids"],
            "auth_methods": group.get("auth_methods") or [],
            "accounts_with_value": [],
        }
        for user_id in group["user_ids"]:
            doc = await users.find_one({"user_id": user_id}, {"_id": 0}) or {}
            has_reports = await db["perizia_analyses"].count_documents({"user_id": user_id}, limit=1)
            has_beta = await db["beta_program_memberships"].count_documents(
                {"user_id": user_id}, limit=1
            )
            credits = (doc.get("perizia_credits") or {}).get("total_available") or 0
            subscription = (doc.get("subscription_state") or {}).get("status")
            if has_reports or has_beta or credits or subscription:
                detail["accounts_with_value"].append(
                    {
                        "user_id": user_id,
                        "reports": bool(has_reports),
                        "beta_membership": bool(has_beta),
                        "credits": credits,
                        "subscription": subscription,
                    }
                )
        enriched.append(detail)

    index_ready = await auth_identity.unique_index_ready(db, use_cache=False)

    return {
        "total_users": total,
        "missing_normalized_email": missing,
        "blank_email": blank_email,
        "duplicate_groups": enriched,
        "index_ready": index_ready,
    }


async def _backfill(db) -> Dict[str, int]:
    """Write canonical fields. Only touches identity fields."""
    users = db[auth_identity.USERS_COLLECTION]
    updated = 0
    skipped = 0

    cursor = users.find({}, {"_id": 0, "user_id": 1, "email": 1, "normalized_email": 1,
                             "auth_methods": 1, "email_verified": 1, "google_id": 1,
                             "last_login_method": 1})
    async for doc in cursor:
        canonical = auth_identity.normalize_email(
            doc.get(auth_identity.NORMALIZED_EMAIL_FIELD) or doc.get("email")
        )
        if not canonical:
            skipped += 1
            continue

        set_fields: Dict[str, Any] = {}
        if doc.get(auth_identity.NORMALIZED_EMAIL_FIELD) != canonical:
            set_fields[auth_identity.NORMALIZED_EMAIL_FIELD] = canonical

        if not isinstance(doc.get("auth_methods"), list) or not doc.get("auth_methods"):
            # Attribute the method only where the evidence supports it.
            if doc.get("google_id") or doc.get("last_login_method") == auth_identity.METHOD_GOOGLE:
                set_fields["auth_methods"] = [auth_identity.METHOD_GOOGLE]
            else:
                set_fields["auth_methods"] = [auth_identity.METHOD_LEGACY]

        if "email_verified" not in doc:
            # Only Google guaranteed a verified address; legacy did not.
            verified = bool(
                doc.get("google_id") or doc.get("last_login_method") == auth_identity.METHOD_GOOGLE
            )
            set_fields["email_verified"] = verified

        if not set_fields:
            skipped += 1
            continue

        await users.update_one({"user_id": doc["user_id"]}, {"$set": set_fields})
        updated += 1

    return {"updated": updated, "already_canonical": skipped}


def _print_report(scan: Dict[str, Any]) -> None:
    print("=" * 62)
    print("normalized_email migration — scan")
    print("=" * 62)
    print(f"  total users                 : {scan['total_users']}")
    print(f"  missing normalized_email    : {scan['missing_normalized_email']}")
    print(f"  blank/absent email          : {scan['blank_email']}")
    print(f"  duplicate groups            : {len(scan['duplicate_groups'])}")
    print(f"  unique index present        : {scan['index_ready']}")

    if scan["duplicate_groups"]:
        print("\n  CONFLICTS — manual review required (nothing was merged):")
        for group in scan["duplicate_groups"]:
            print(f"    {group['email_masked']}  x{group['count']}")
            print(f"      user_ids     : {', '.join(group['user_ids'])}")
            print(f"      auth_methods : {group['auth_methods']}")
            if group["accounts_with_value"]:
                print("      accounts holding value:")
                for account in group["accounts_with_value"]:
                    print(
                        f"        - {account['user_id']}: reports={account['reports']} "
                        f"beta={account['beta_membership']} credits={account['credits']} "
                        f"subscription={account['subscription']}"
                    )
            else:
                print("      accounts holding value: none")
    print()


async def main_async(args: argparse.Namespace) -> int:
    import server  # noqa: F401  # configures the Mongo handle from .env

    db = server.db
    print(f"database: {os.environ.get('DB_NAME')}")

    scan = await _scan(db)
    _print_report(scan)

    if not args.apply:
        print("DRY RUN — no writes performed.")
        if scan["duplicate_groups"]:
            print("Apply would REFUSE: resolve the conflicts above first.")
            return 2
        print("Apply is eligible to proceed.")
        return 0

    if scan["duplicate_groups"]:
        print("REFUSING TO APPLY: duplicate normalized_email groups exist.")
        print("Resolve them manually; this script will not merge or delete accounts.")
        return 2

    result = await _backfill(db)
    print(f"backfill: updated={result['updated']} already_canonical={result['already_canonical']}")

    # Re-scan before indexing: the backfill itself could surface a collision that
    # was invisible while the canonical field was absent.
    post = await _scan(db)
    if post["duplicate_groups"]:
        print("REFUSING TO INDEX: duplicates surfaced after backfill.")
        _print_report(post)
        return 2

    created = await auth_identity.ensure_unique_index(db)
    print(f"unique index on normalized_email: {'present' if created else 'NOT CREATED'}")
    if not created:
        return 2

    print("APPLY COMPLETE. Re-run to confirm idempotency (expect updated=0).")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--dry-run", action="store_true", help="report only (default)")
    group.add_argument("--apply", action="store_true", help="backfill and create the unique index")
    args = parser.parse_args()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
