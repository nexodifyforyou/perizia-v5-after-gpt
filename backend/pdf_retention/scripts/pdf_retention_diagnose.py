"""Verify an original under explicit owner authorization without exporting it."""

from __future__ import annotations

import argparse
import asyncio
import hashlib

from pdf_retention.ops_access import decrypt_for_owner_diagnostic


async def _run(args) -> None:
    plaintext = await decrypt_for_owner_diagnostic(
        analysis_id=args.analysis_id,
        owner_user_id=args.owner_user_id,
        owner_email=args.owner_email,
        reason=args.reason,
    )
    print(f"verified=true sha256={hashlib.sha256(plaintext).hexdigest()} bytes={len(plaintext)}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--analysis-id", required=True)
    parser.add_argument("--owner-user-id", required=True)
    parser.add_argument("--owner-email", required=True)
    parser.add_argument("--reason", required=True)
    args = parser.parse_args()
    asyncio.run(_run(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
