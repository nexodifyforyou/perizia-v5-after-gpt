"""
Feature flags for Correctness Mode v2.

All flags are read from the process environment at call time (not import time)
so tests can monkeypatch ``os.environ`` and so a missing flag never hard-fails
app startup. Defaults are chosen to be SAFE:

- CORRECTNESS_V2_ENABLED        = false  (disabled unless explicitly enabled)
- CORRECTNESS_V2_ADMIN_ONLY     = true   (admin-only by default)
- CORRECTNESS_V2_SHADOW_MODE    = true   (shadow mode by default)
- CORRECTNESS_V2_NO_OLD_FALLBACK= true   (never fall back to the old analyzer)
- CORRECTNESS_JOB_MODE          = async  (async job model)
- CORRECTNESS_MAX_RUNTIME_SECONDS = 0    (0 == no product-level timeout)
"""

from __future__ import annotations

import os
from typing import Optional

# Canonical flag names -------------------------------------------------------
FLAG_ENABLED = "CORRECTNESS_V2_ENABLED"
FLAG_ADMIN_ONLY = "CORRECTNESS_V2_ADMIN_ONLY"
FLAG_SHADOW_MODE = "CORRECTNESS_V2_SHADOW_MODE"
FLAG_NO_OLD_FALLBACK = "CORRECTNESS_V2_NO_OLD_FALLBACK"
FLAG_JOB_MODE = "CORRECTNESS_JOB_MODE"
FLAG_MAX_RUNTIME_SECONDS = "CORRECTNESS_MAX_RUNTIME_SECONDS"

_TRUE_TOKENS = {"1", "true", "yes", "on", "y", "t"}
_FALSE_TOKENS = {"0", "false", "no", "off", "n", "f", ""}


def _env_bool(name: str, default: bool) -> bool:
    """Read a boolean flag from the environment. Never raises."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    token = str(raw).strip().lower()
    if token in _TRUE_TOKENS:
        return True
    if token in _FALSE_TOKENS:
        return False
    # Unknown/garbage value -> fall back to the safe default rather than crash.
    return default


def _env_str(name: str, default: str) -> str:
    raw = os.environ.get(name)
    if raw is None:
        return default
    token = str(raw).strip()
    return token or default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(str(raw).strip())
    except Exception:
        return default


def is_enabled() -> bool:
    return _env_bool(FLAG_ENABLED, False)


def is_admin_only() -> bool:
    return _env_bool(FLAG_ADMIN_ONLY, True)


def is_shadow_mode() -> bool:
    return _env_bool(FLAG_SHADOW_MODE, True)


def no_old_fallback() -> bool:
    return _env_bool(FLAG_NO_OLD_FALLBACK, True)


def job_mode() -> str:
    mode = _env_str(FLAG_JOB_MODE, "async").lower()
    return mode if mode in {"async", "sync"} else "async"


def max_runtime_seconds() -> int:
    """0 means no product-level timeout."""
    value = _env_int(FLAG_MAX_RUNTIME_SECONDS, 0)
    return value if value >= 0 else 0


def snapshot() -> dict:
    """Return the current resolved flag values (handy for diagnostics/artifacts)."""
    return {
        FLAG_ENABLED: is_enabled(),
        FLAG_ADMIN_ONLY: is_admin_only(),
        FLAG_SHADOW_MODE: is_shadow_mode(),
        FLAG_NO_OLD_FALLBACK: no_old_fallback(),
        FLAG_JOB_MODE: job_mode(),
        FLAG_MAX_RUNTIME_SECONDS: max_runtime_seconds(),
    }


def access_block_reason(is_admin: Optional[bool]) -> Optional[str]:
    """
    Decide whether access to a Correctness v2 endpoint/job should be blocked.

    Returns a stable reason code string when access must be blocked, or ``None``
    when access is allowed. Does not raise.

    - If the feature is disabled        -> "CORRECTNESS_V2_DISABLED"
    - If admin-only and caller is not admin -> "ADMIN_ONLY_FEATURE"
    """
    if not is_enabled():
        return "CORRECTNESS_V2_DISABLED"
    if is_admin_only() and not bool(is_admin):
        return "ADMIN_ONLY_FEATURE"
    return None
