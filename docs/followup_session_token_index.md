# Follow-up: add unique/indexed session-token lookup

**Status:** open, not scheduled
**Blocking passwordless email auth?** No.
**Raised during:** review of `feature-passwordless-email-auth` (2026-07-21)

## Correction to the reported symptom

The issue was reported as a missing `users.session_token` index. That collection
and field do not exist. The real hot path is:

- `backend/server.py:1234` — `db.user_sessions.find_one({"session_token": session_token}, {"_id": 0})`

`user_sessions` is written at `backend/server.py:12689` and deleted at
`backend/server.py:12901` (also keyed by `session_token`).

## The actual defect

`ensure_indexes()` (`backend/server.py:21188`+) builds `index_specs` covering
`users`, `perizia_analyses`, `image_forensics`, `assistant_qa`,
`payment_transactions`, `credit_ledger`, `billing_records`, `admin_user_notes`,
`admin_audit_log`, and `beta_feedback`. **`user_sessions` appears nowhere in it.**

So the collection carries only the default `_id` index, and every authenticated
request — the `get_current_user` dependency — performs a full collection scan on
`user_sessions` to resolve the session cookie. Cost grows linearly with the
number of sessions ever created, and rows are only removed on explicit logout,
so the collection is effectively append-only for users who close the tab instead
of logging out.

This predates the passwordless-email-auth work. That branch adds
`backend/auth_email/api.py` as a new writer of the same collection, which
increases session-row creation rate but does not introduce or worsen the
missing index itself.

## Why it is not folded into the email-auth change

Correctness is unaffected: `find_one` returns the same document with or without
an index. Nothing in the email-auth test suite depends on lookup latency. Adding
a schema/index change to an auth feature branch would widen the blast radius of
a rollout whose whole point is to ship dark behind `AUTH_EMAIL_ENABLED=false`.

## Work required when this is picked up

1. **Duplicate pre-flight.** Before any unique index, confirm no duplicate
   `session_token` values exist:
   ```
   db.user_sessions.aggregate([
     {$group: {_id: "$session_token", n: {$sum: 1}}},
     {$match: {n: {$gt: 1}}},
     {$limit: 10}
   ])
   ```
   Tokens are generated as `f"sess_{uuid.uuid4().hex}"` (`server.py:12677`), so
   duplicates are not expected — but the index must not be the thing that
   discovers otherwise. Also count documents with a missing/null
   `session_token`, which would collide under a plain unique index.

2. **Unique index eligibility.** If the pre-flight is clean, prefer
   `create_index("session_token", unique=True)`. If null/missing tokens exist,
   either backfill/delete them first or use a partial index
   (`partialFilterExpression: {session_token: {$type: "string"}}`) rather than
   silently downgrading to a non-unique index.

3. **Query-plan comparison.** Capture `explain("executionStats")` for the
   `server.py:1234` query before and after. Expect `COLLSCAN` →
   `IXSCAN`, with `totalDocsExamined` dropping to 1. Record both plans in the PR.

4. **No session invalidation.** Index creation must not delete or rewrite any
   document. Build it in the background and verify session count is identical
   before and after; no user should be logged out by this change. Do **not**
   pair this with a TTL index in the same change — a TTL index *would* expire
   sessions and log users out, which is a separate product decision.

5. Add the spec to `index_specs` so it is created idempotently on startup, and a
   test asserting `user_sessions.session_token` is among the ensured indexes.

## Escalation condition

This becomes blocking only if testing shows the missing index causes a
correctness problem or unacceptable authentication latency under the session
volume the email-auth rollout produces. Nothing observed so far indicates that.
