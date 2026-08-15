"""Consent-gated, encrypted, TTL-bounded retention of original PDF bytes.

The package is deliberately independent from the correctness pipeline.  It is
write-dormant unless the master feature flag is enabled, while erasure and TTL
cleanup remain available so disabling creation can never strand existing data.
"""

from .config import CONSENT_VERSION, is_enabled, retention_days

__all__ = ["CONSENT_VERSION", "is_enabled", "retention_days"]
