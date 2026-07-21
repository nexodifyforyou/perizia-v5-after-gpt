"""Environment configuration for passwordless email authentication.

Every value is read from the environment at call time (not import time) so tests
and the deployment sequence can flip the feature without reimporting ``server``.

Fail-closed contract: when ``AUTH_EMAIL_ENABLED`` is true but any security
prerequisite is missing, the OTP endpoints refuse to operate. Google OAuth is
never affected by anything in this module.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Optional

# Feature flag, following the repository's string-constant + env-read idiom
# (see perizia_authority_lot_projection.FEATURE_FLAG).
FEATURE_FLAG = "AUTH_EMAIL_ENABLED"

PROVIDER_RESEND = "resend"
PROVIDER_FAKE = "fake"
PROVIDER_SINK = "sink"
KNOWN_PROVIDERS = frozenset({PROVIDER_RESEND, PROVIDER_FAKE, PROVIDER_SINK})

# A six-digit code is a 10**6 space: a bare digest is brute-forced instantly if
# the collection ever leaks. The pepper is what makes the stored hash worth
# anything, so it is a hard prerequisite rather than an optional hardening.
MIN_PEPPER_CHARS = 32

DEFAULT_CODE_TTL_SECONDS = 600
DEFAULT_PURGE_AFTER_SECONDS = 48 * 3600
DEFAULT_RESEND_COOLDOWN_SECONDS = 60
DEFAULT_MAX_REQUESTS_PER_EMAIL_HOUR = 5
DEFAULT_MAX_REQUESTS_PER_IP_HOUR = 20
DEFAULT_MAX_VERIFY_ATTEMPTS = 5
DEFAULT_RESEND_TIMEOUT_SECONDS = 10.0
DEFAULT_RESEND_MAX_INPROCESS_RETRIES = 1

RESEND_API_URL = "https://api.resend.com/emails"


def _env(name: str, default: str = "") -> str:
    return str(os.environ.get(name, default) or "").strip()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = _env(name)
    if not raw:
        return default
    return raw.lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = _env(name)
    if not raw:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value >= minimum else default


def _env_float(name: str, default: float) -> float:
    raw = _env(name)
    if not raw:
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def is_enabled() -> bool:
    return _env_bool(FEATURE_FLAG, False)


def provider_name() -> str:
    return _env("AUTH_EMAIL_PROVIDER", PROVIDER_RESEND).lower() or PROVIDER_RESEND


def code_pepper() -> str:
    # Read raw (not stripped-to-empty) so a whitespace-only value fails the
    # length check rather than silently passing.
    return str(os.environ.get("AUTH_EMAIL_CODE_PEPPER", "") or "")


def resend_api_key() -> str:
    return _env("RESEND_API_KEY")


def email_from() -> str:
    return _env("AUTH_EMAIL_FROM")


def email_reply_to() -> Optional[str]:
    return _env("AUTH_EMAIL_REPLY_TO") or None


def sender_domain_verified() -> bool:
    """Operator's explicit attestation that the sending domain is live.

    Resend silently refuses unverified senders, so this is an operational gate
    the deployment runbook flips only after checking domain status in Resend.
    """
    return _env_bool("AUTH_EMAIL_SENDER_DOMAIN_VERIFIED", False)


def code_ttl_seconds() -> int:
    return _env_int("AUTH_EMAIL_CODE_TTL_SECONDS", DEFAULT_CODE_TTL_SECONDS)


def purge_after_seconds() -> int:
    """Retention boundary — deliberately distinct from ``code_ttl_seconds``.

    A challenge stops authenticating at ``expires_at``; the record itself
    survives until ``purge_at`` so provider failures and rate-limit behaviour
    remain diagnosable.
    """
    return _env_int("AUTH_EMAIL_PURGE_AFTER_SECONDS", DEFAULT_PURGE_AFTER_SECONDS)


def resend_cooldown_seconds() -> int:
    # A zero cooldown is a legitimate configuration (the hourly caps still
    # apply), so it must not be silently replaced by the default.
    return _env_int(
        "AUTH_EMAIL_RESEND_COOLDOWN_SECONDS", DEFAULT_RESEND_COOLDOWN_SECONDS, minimum=0
    )


def max_requests_per_email_hour() -> int:
    return _env_int("AUTH_EMAIL_MAX_REQUESTS_PER_EMAIL_HOUR", DEFAULT_MAX_REQUESTS_PER_EMAIL_HOUR)


def max_requests_per_ip_hour() -> int:
    return _env_int("AUTH_EMAIL_MAX_REQUESTS_PER_IP_HOUR", DEFAULT_MAX_REQUESTS_PER_IP_HOUR)


def max_verify_attempts() -> int:
    return _env_int("AUTH_EMAIL_MAX_VERIFY_ATTEMPTS", DEFAULT_MAX_VERIFY_ATTEMPTS)


def resend_timeout_seconds() -> float:
    return _env_float("AUTH_EMAIL_RESEND_TIMEOUT_SECONDS", DEFAULT_RESEND_TIMEOUT_SECONDS)


def resend_max_inprocess_retries() -> int:
    return _env_int(
        "AUTH_EMAIL_RESEND_MAX_INPROCESS_RETRIES", DEFAULT_RESEND_MAX_INPROCESS_RETRIES
    )


@dataclass(frozen=True)
class PreflightResult:
    """Outcome of the fail-closed prerequisite check.

    ``reasons`` are operator-facing and belong in server logs only; the customer
    always receives the same generic delivery-unavailable message.
    """

    ok: bool
    reasons: List[str]

    @property
    def reason_summary(self) -> str:
        return ", ".join(self.reasons) if self.reasons else "ok"


def preflight(*, index_ready: bool) -> PreflightResult:
    """Validate every security prerequisite for serving OTP requests.

    ``index_ready`` is supplied by the caller because the unique
    ``normalized_email`` index is what guarantees one identity per verified
    email; without it, OTP could create a duplicate account.
    """
    reasons: List[str] = []

    if not is_enabled():
        reasons.append("feature_disabled")

    provider = provider_name()
    if provider not in KNOWN_PROVIDERS:
        reasons.append(f"unknown_provider:{provider}")

    if len(code_pepper()) < MIN_PEPPER_CHARS:
        reasons.append("pepper_missing_or_too_short")

    if not email_from():
        reasons.append("from_address_missing")

    if provider == PROVIDER_RESEND:
        if not resend_api_key():
            reasons.append("resend_api_key_missing")
        if not sender_domain_verified():
            reasons.append("sender_domain_not_verified")

    if not index_ready:
        reasons.append("normalized_email_unique_index_missing")

    return PreflightResult(ok=not reasons, reasons=reasons)
