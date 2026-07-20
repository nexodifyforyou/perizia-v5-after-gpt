"""
Beta Program (owner-managed) for PeriziaScan.

Database-managed beta membership replaces the legacy environment/hardcoded
allowlist (`BETA_UNLIMITED_EMAILS`). Beta access is resolved per request as an
ENTITLEMENT (never a wallet balance): an ACTIVE membership grants an unlimited
analysis exemption plus the beta customer display, and nothing else (no admin,
no Vista admin, no access to other users).

Package layout:
- ``store``   : membership schema, normalization, per-request resolver, status
                transitions (each append-only audited), index bootstrap.
- ``signals`` : deterministic operational signals + the telemetry-only
                ``emit_v2_job_event`` mirror (``v2_job_events``).
- ``api``     : the exact-owner-only ``/api/admin/beta-program`` router.
- ``migrate`` : idempotent import from the legacy allowlist (dry-run by default).

Nothing here imports Stripe/checkout/webhook code or calls OpenAI.
"""

from . import store  # noqa: F401
