#!/usr/bin/env python3
"""
CLI for the beta-program migration (see beta_program/migrate.py).

Usage:
    python -m scripts.beta_program_migrate --dry-run
    python -m scripts.beta_program_migrate --apply
    python -m scripts.beta_program_migrate --apply --email a@b.com --name "Geom. A"
    python -m scripts.beta_program_migrate --backfill-job-events

Runs from the backend/ directory (so ``import server`` resolves). Dry-run is the
default; nothing is written without ``--apply``. Prints an explicit JSON report.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

# Ensure backend/ is importable when run as a file.
BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Beta program allowlist migration")
    parser.add_argument("--apply", action="store_true", help="Write changes (default: dry-run)")
    parser.add_argument("--dry-run", action="store_true", help="Explicit dry-run (default)")
    parser.add_argument(
        "--email", action="append", default=[], help="Explicit email to import (repeatable)"
    )
    parser.add_argument(
        "--name", action="append", default=[], help="Display name paired with --email (repeatable)"
    )
    parser.add_argument(
        "--backfill-job-events",
        action="store_true",
        help="One-shot offline mirror of clearly-terminal existing jobs into v2_job_events",
    )
    return parser.parse_args()


async def _amain() -> int:
    args = _parse_args()
    dry_run = not args.apply

    import server  # noqa: F401  # loads env + db
    from beta_program import migrate

    extra = []
    names = list(args.name)
    for idx, email in enumerate(args.email):
        extra.append({"email": email, "name": names[idx] if idx < len(names) else None})

    report = await migrate.run_migration(dry_run=dry_run, extra_emails=extra)
    print(json.dumps({"migration": report}, ensure_ascii=False, indent=2))

    if args.backfill_job_events:
        from beta_program import backfill

        backfill_report = backfill.backfill_terminal_job_events(dry_run=dry_run)
        print(json.dumps({"backfill_job_events": backfill_report}, ensure_ascii=False, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_amain()))
