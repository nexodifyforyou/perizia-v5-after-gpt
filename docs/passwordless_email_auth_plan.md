# Passwordless Email Authentication (six-digit OTP)

Branch: `feature-passwordless-email-auth` · base `98afae1`

Adds provider-independent email login alongside the existing Google OAuth flow.
Account identity is the **verified normalized email**, never the provider, so any
mailbox works: Microsoft 365, Outlook, Aruba, a custom corporate domain, or
Google. Google OAuth remains available and unchanged.

## Architecture

`backend/auth_email/`

| Module | Responsibility |
|---|---|
| `config.py` | Env reads, fail-closed preflight |
| `sender.py` | `EmailSender` protocol + `ResendSender` / `FakeSender` / `SinkSender` |
| `templates.py` | The OTP message (carries no account state) |
| `challenges.py` | Challenge lifecycle, hashing, atomic single-use consumption |
| `ratelimit.py` | Mongo atomic counters |
| `identity.py` | Canonical identity fields + unique-index management |
| `api.py` | `POST /api/auth/email/request-code`, `POST /api/auth/email/verify-code` |

Verification reuses `server._create_local_login` unchanged, so session cookie,
logout, session revocation, owner authorization and beta linking behave
identically to Google.

## Delivery

Resend, called directly over the httpx client already vendored for the Google
token exchange — no new backend dependency. Every send carries an
`Idempotency-Key` derived from the immutable `challenge_id` (never the code or
the address), so a retry after a lost response cannot deliver twice.

Outcomes collapse to three categories:

| Category | Trigger | Challenge result |
|---|---|---|
| `OK` | 2xx + message id | `SENT` |
| `DEFINITIVE` | 4xx (bad recipient, unverified sender, auth) | `SEND_FAILED`, terminal, unverifiable |
| `AMBIGUOUS` | timeout, connection error, 5xx, 429, 408 | stays `SEND_PENDING`, still verifiable |

Persisted per challenge: `provider`, `provider_message_id`, `send_attempted_at`,
`send_attempt_count`, `delivery_state`, `failure_category`, `idempotency_key`.
Never persisted: plaintext OTP, raw provider response, API key, full body.

## Challenge state machine

```
CREATED -> SEND_PENDING -> SENT
SEND_PENDING | SENT -> CONSUMED     (terminal, success)
SEND_PENDING        -> SEND_FAILED  (terminal, definitive refusal)
SEND_PENDING | SENT -> EXPIRED      (ttl, or superseded)
SEND_PENDING | SENT -> LOCKED       (attempts exhausted)
SEND_PENDING | SENT -> SUPERSEDED   (a new code was requested)
```

Verifiable: `SENT` **and** `SEND_PENDING`. An ambiguous provider timeout may
still have delivered the message, so possession of the correct code is treated as
sufficient evidence of receipt; requiring provider acknowledgement would lock out
users whose mail actually arrived. `SEND_FAILED` is never verifiable.

### No cross-request resend of the same code

Only `code_hash` and `code_salt` are stored, so the plaintext is unrecoverable
once its request ends.

- **In-process** (plaintext still in memory): an ambiguous result may be retried
  immediately with the same code, body and idempotency key.
- **Cross-request** (the user asks again): the previous challenge is atomically
  superseded and a **new** challenge id, code and idempotency key are minted. The
  old code dies immediately.

No reversible or encrypted copy of the code exists anywhere.

### One live code per email

`active_slot` holds the normalized email while the challenge is non-terminal and
is `$unset` on every terminal transition. A **unique partial index** on
`active_slot` enforces the invariant in the database, so two concurrent requests
resolve to one challenge and the loser receives the same generic cooldown
response. Verified at 2, 10 and 50 simultaneous requests.

### Single-write consumption

Success is one atomic `find_one_and_update`: `{SENT|SEND_PENDING, unexpired,
attempts < max}` → `CONSUMED` with `verified_at`, `consumed_at`,
`consumption_reason=OTP_VERIFIED` and `active_slot` released. There is no
intermediate `VERIFIED` state for a second caller to claim.

**Documented trade-off:** if session creation fails after consumption, the code
stays spent (security over convenience). The user requests a new one, which
succeeds because the slot was already released.

### Expiry vs retention

`expires_at` (default 600s) ends **authentication validity**. `purge_at` (default
48h) ends **retention**, and the TTL index is on `purge_at`. An expired code stops
working immediately while the record remains available for diagnosing provider
failures and verifying rate-limit behaviour.

Rate-limit buckets have their own, longer retention, so a purged or expired
challenge can never hand back spent hourly allowance.

## Identity

Unique partial index on `users.normalized_email` (not the historical `email`
field, which predates normalization and is not guaranteed canonical).
`_get_or_create_authenticated_user` is an atomic upsert matching
`normalized_email` OR `email`, with `DuplicateKeyError` resolved by loading the
winner. All login paths — Google, email OTP, legacy — share it.

Normalization is conservative: trim + lowercase only. Plus-addressing and dots
are preserved, so `name@x.it` and `name+beta@x.it` remain distinct. No
Gmail-specific rewriting. Identical to `beta_program.store.normalize_beta_email`.

Google contributes `email_verified` only behind the existing
`_google_email_is_verified` guard; the legacy provider asserts nothing and cannot
link accounts.

## Configuration

```
AUTH_EMAIL_ENABLED=false
AUTH_EMAIL_PROVIDER=resend
RESEND_API_KEY=<secret — VM only, never in git>
AUTH_EMAIL_CODE_PEPPER=<secret — VM only, never in git, >=32 chars>
AUTH_EMAIL_FROM=Perizia Scan <accesso@auth.nexodify.com>
AUTH_EMAIL_REPLY_TO=
AUTH_EMAIL_SENDER_DOMAIN_VERIFIED=false
AUTH_EMAIL_CODE_TTL_SECONDS=600
AUTH_EMAIL_PURGE_AFTER_SECONDS=172800
AUTH_EMAIL_RESEND_COOLDOWN_SECONDS=60
AUTH_EMAIL_MAX_REQUESTS_PER_EMAIL_HOUR=5
AUTH_EMAIL_MAX_REQUESTS_PER_IP_HOUR=20
AUTH_EMAIL_MAX_VERIFY_ATTEMPTS=5
```

`AUTH_EMAIL_CODE_PEPPER` is required, not optional hardening: a six-digit code is
a 10^6 keyspace, so an unpeppered digest is reversed by brute force in
milliseconds if the collection ever leaks.

### Fail-closed

With `AUTH_EMAIL_ENABLED=true`, both endpoints return a generic 503 if any of the
following is missing — the specific reason goes to the server log only:

- unique `normalized_email` index absent
- pepper absent or shorter than 32 characters
- Resend API key or From address missing
- `AUTH_EMAIL_SENDER_DOMAIN_VERIFIED` not true

**Google login is never affected** by OTP being disabled or misconfigured.

## Migration

`backend/scripts/migrate_normalized_email.py`

- `--dry-run` (default) is strictly read-only. Reports totals, users missing the
  canonical field, duplicate groups with user ids and auth methods, and which
  conflicting accounts hold credits, reports, subscriptions or beta memberships.
  Addresses are masked.
- `--apply` refuses while any duplicate group exists. It never merges, deletes or
  renumbers. On a clean scan it backfills `normalized_email`, `auth_methods` and
  `email_verified`, then creates the unique index, re-scanning first in case the
  backfill surfaced a collision.
- Re-running after a successful apply reports `updated=0`.

`email_verified=true` is backfilled only for Google-originated accounts.

## Testing

- 199 backend tests (7 files) — real Mongo on the isolated `test_pytest_*` DB
- 27 frontend jest tests — hand-rolled `createRoot` + `act`, matching repo style
- 18 Playwright specs across desktop and mobile
- `e2e/run_e2e.sh` — isolated DB, local SMTP sink, no network, no Resend

Resend is never contacted by any automated test.
