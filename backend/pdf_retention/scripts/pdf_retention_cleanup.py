"""Run one idempotent TTL/reconciliation pass (for systemd)."""

from __future__ import annotations

import asyncio
import json

from pdf_retention.sweep import run_once


def main() -> int:
    result = asyncio.run(run_once())
    print(json.dumps(result, sort_keys=True))
    return 1 if result.get("failed") else 0


if __name__ == "__main__":
    raise SystemExit(main())
