# Beta Perizia Limits — Implementation Plan (feature-beta-perizia-limits)

Base: `main` @ `9741718` (merge commit "Merge owner-managed beta program"). Audit
performed read-only against the real codebase and a read-only query of the live
Mongo instance. All file:line citations are from that SHA. **No source file was
modified to produce this plan** — this document is the only artifact written.

Goal: add a configurable, software-enforced allowance of new top-level perizie per
ACTIVE beta tester (UNLIMITED or LIMITED-to-N), atomically reserved/consumed/
released around the existing upload pipeline, with owner controls in Programma
Beta → Tester and a customer-safe fallback to the tester's real paid plan when
exhausted. Beta remains an entitlement (§ of `beta_program_admin_plan.md`), never
a wallet; this feature adds a second, orthogonal entitlement axis (quota) on top
of the existing membership axis (status).

---

## Key determinations (read this first)

- **Earliest safe reservation point**: immediately after `require_auth` succeeds
  and the beta snapshot is resolved, but **before** the file-type check
  (`server.py:16283`). Reservation must happen before any per-request work
  because it is the only point that is (a) always reached for every upload
  attempt and (b) cheap enough to be the single atomic gate. Concretely: right
  after `analysis_id`/`case_id`/`run_id` are generated is too late (that already
  happens after the credit check at `server.py:16340-16342`) — the plan moves the
  ID generation and reservation to the very top of `analyze_perizia`, before
  `file.filename.lower().endswith('.pdf')` (`server.py:16283`), so `analysis_id`
  can double as the ledger's authoritative idempotency key from the first line of
  the handler (§H).
- **Exact paid-processing boundary (CONSUME)**: the same commit point the
  existing credit debit already uses — the moment `analysis_dict["status"]` is
  known (`COMPLETED` vs `UNREADABLE`, `server.py:16744`) and the record is about
  to be durably persisted (`db.perizia_analyses.insert_one`, `server.py:16759`),
  which is the exact same condition (`status == "COMPLETED"`) that gates the real
  paid OpenAI call (`correctness_v2_api.autostart_job`, `server.py:16766-16772` →
  `orchestrator.start_job` → `_run_step2` → `analyst_mod.run_analyst`,
  `correctness_v2/orchestrator.py:496-498`). Reusing this exact boundary means the
  quota system never needs to reason about the async V2 job at all (it runs in an
  untracked daemon thread, `correctness_v2/api.py:216-221`, after the HTTP
  response is already sent) — consumption is decided synchronously, deterministically,
  and it is the same boundary the money system already trusts.
- **Release conditions**: everything in the synchronous `analyze_perizia` path
  that terminates **before** that COMPLETED/insert_one point: invalid file type
  (`server.py:16283-16287`), unreadable/corrupt PDF (`server.py:16294-16298`),
  unsupported page count (`server.py:16300-16309`), `PIPELINE_TIMEOUT`
  (`server.py:16495-16508`, which persists `status=FAILED` via
  `_persist_failed_analysis`, `server.py:8860-8899`, and — like today's credit
  system — never debits), and the deterministic `UNREADABLE` outcome
  (`server.py:16744`, which is also exactly the condition that skips V2 autostart
  at `server.py:16766`). Each release records one deterministic reason code (§G).
- **Duplicate/idempotency**: there is **no existing** upload-dedup or client
  idempotency-key mechanism in this codebase today — `input_sha256`
  (`server.py:16291`) is computed and stored but never used to reject/merge a
  duplicate submission (confirmed by grep: no read of `input_sha256` before
  insert). Per the mission's counting rule, a genuinely repeated upload (same
  bytes, new click) **must** consume a second slot — so no content-hash dedup is
  introduced. The only duplicate this feature must protect against is **internal
  double-processing of the same request** (a retried background task, a crash
  recovery sweep racing the live request). The authoritative idempotency key is
  `analysis_id` itself: a fresh, backend-generated, unguessable id
  (`f"analysis_{uuid.uuid4().hex[:12]}"`, moved to the top of the handler, §D/§H)
  — never a browser-supplied value.
- **Rerun vs. new top-level upload**: the beta quota is wired into exactly **one**
  call site — the top of `analyze_perizia` (`server.py:16276`). Every other
  analysis-touching endpoint operates on an **existing** `analysis_id` and never
  creates one: `correctness_v2_generate_lot` (`correctness_v2/api.py:815-860`,
  reuse/dedup/force-rerun, "No credit debit ever happens here"),
  `correctness_v2_workspace` (`api.py:775-785`, "ZERO side effects"),
  `correctness_v2_lot_generate_preview` (`api.py:788-799`),
  `resolve_money_confirmation` (`orchestrator.py:1718`, "NO OpenAI, NO PDF"), and
  the legacy `correctness_v2_start` admin route (`api.py:306-340`, admin-only,
  reuses an existing `analysis_id`). None of these is touched by this feature —
  the separation is structural, not a new check to add.
- **Mongo topology (verified live, read-only)**: `db.admin.command("hello")`
  returns no `setName` and `replSetGetStatus` fails with
  `NoReplicationEnabled — "not running with --replSet"` (mongod 6.0.27). **This is
  a standalone instance, not a replica set.** Multi-document ACID transactions
  (`session.start_transaction()`) are therefore **unavailable**. The atomic
  mechanism in this plan uses only single-document atomicity (`find_one_and_update`
  with a conditional filter), which MongoDB guarantees on a standalone instance —
  no transaction is required or used anywhere in this design.

---

## A. Current beta entitlement flow (recap, unchanged by this feature)

- `beta_program_memberships` (`backend/beta_program/store.py:32-39`) holds
  `status ∈ {PENDING, ACTIVE, REVOKED}`, keyed on `normalized_email` (unique
  index `uq_beta_membership_email`, `store.py:117-119`).
- `get_current_user` (`server.py:1200-1245`) resolves the snapshot fresh on every
  request: `beta_program.store.resolve_snapshot(email)` (`store.py:174-210`) is a
  single indexed `find_one({"normalized_email", "status": "ACTIVE"})`, fail-closed
  on any exception (`store.py:198-200`), stamped onto `user_doc["beta_program"]`
  (`server.py:1236-1241`) and never persisted to `db.users`.
- `_user_has_active_beta(user)` (`server.py:648-658`) is the single predicate
  every call site consults; `_is_credit_exempt_user` (`server.py:661-665`) is
  `admin OR active beta`.
- This feature **adds a second predicate** alongside it: whether an ACTIVE
  member's quota is currently `UNLIMITED`, `AVAILABLE`, or `EXHAUSTED` (derived,
  never a membership status — §D). `_user_has_active_beta` keeps its current
  meaning ("is this account a beta member right now") and is not touched; a new
  function decides whether *this specific upload* gets a free slot.

## B. Current upload and credit flow (recap)

`POST` upload handler `analyze_perizia` (`server.py:16276-16805`), single
synchronous request/response, no job queue at the HTTP layer:

1. `require_auth` (`server.py:16278`) → `user` (beta snapshot already resolved).
2. File-type check (`16283-16287`), PDF parse + page count (`16290-16298`),
   page-band lookup `_get_required_perizia_credits` (`16300-16309`,
   `PERIZIA_CREDIT_BANDS` at `602-611`).
3. Credit check (`16311-16337`): `beta_unlimited = _user_has_active_beta(user)`
   logs an exemption note (`16312-16319`); the actual gate is
   `if remaining < required and not _is_credit_exempt_user(user): raise 403
   INSUFFICIENT_PERIZIA_CREDITS`. **This is the line this feature must change**
   (§Q) — today it treats every active beta member as exempt unconditionally.
4. IDs minted (`16340-16342`): `case_id`, `run_id`, `analysis_id` — **moved
   earlier in this feature**, to before step 2 (see "Key determinations").
5. `run_pipeline()` (`16346-16492`), awaited with `PIPELINE_TIMEOUT_SECONDS=120`
   (`server.py:123`, `16495`): local text extraction, optional Document AI OCR
   fallback (a real paid Google call, `16394-16431` — note for §Y), deterministic
   route-seed build. On timeout: `_persist_failed_analysis` (status `FAILED`,
   no debit) then `HTTPException(504)`.
6. Post-pipeline enrichment (candidate miner, authority resolvers, quality
   verifier, Gemini narrator `_apply_post_qa_decision_narrator`, QA gate) —
   `16510-16724`, all wrapped in try/except with deterministic fallbacks, so this
   stage effectively never raises.
7. `analysis_dict["status"] = "UNREADABLE" if ... else "COMPLETED"` (`16744`);
   `entitlement_context` stamped (`16748`, `_resolve_entitlement_context`,
   `674-688` — **needs adjustment**, see §Q); persisted
   (`db.perizia_analyses.insert_one`, `16759`).
8. If `COMPLETED`: `correctness_v2_api.autostart_job` spawns a **daemon thread**
   (`correctness_v2/api.py:178-231`) that runs the real OpenAI pipeline
   asynchronously — the HTTP response does not wait for it.
9. Credit debit (`16781-16796`, `_apply_perizia_credit_debit_with_ledger`,
   `12209-12276`): admin → skip; **`_user_has_active_beta(user)` → skip,
   unconditionally** (`12221-12229` — **this is the second line this feature
   must change**, since an EXHAUSTED beta member falling back to their real plan
   must be debited normally); otherwise real wallet debit + `credit_ledger` entry
   (`_insert_credit_ledger_entry`, `12024-12066`).
10. Response returned (`16798-16805`).

## C. Counting definition

One accepted call to `analyze_perizia` that reaches step 7 with
`status == "COMPLETED"` = one beta use. Concretely:

- A multi-lot PDF is **one** analysis record and **one** `autostart_job`
  invocation (`server.py:16770`) regardless of how many lots it contains — lot
  count is discovered later inside the V2 job. One use.
- Generating/rerunning any number of individual lot reports
  (`correctness_v2_generate_lot`) is **zero** additional uses — it never touches
  `analyze_perizia`, never mints a new `analysis_id`, and its own credit preview
  already asserts `will_consume_credit: False` / `already_paid_at_upload: True`
  (`correctness_v2/api.py:754-772`).
- Storico workspace reads (`api.py:775-785`), cached-report opens, job-status
  polling, focused confirmations, `resolve_money_confirmation`
  (`orchestrator.py:1718-1806`), and any future PDF/HTML export are all zero —
  none of them calls `analyze_perizia`.
- `status == "UNREADABLE"` does **not** consume a slot (§F/§G) — even though the
  current paid-credit debit charges for it too (`12781-12796` runs regardless of
  status). This is a deliberate, scoped divergence between the beta ledger and
  the paid-credit ledger — flagged as a product decision in the closing section.
- `PIPELINE_TIMEOUT`/`FAILED` does not consume a slot, matching the fact that the
  paid-credit system also never debits for it today (no call reaches
  `16781-16796` on that path).
- A byte-identical re-upload (new click, new `analysis_id`) consumes a new slot —
  matches existing per-analysis-id credit debit behavior; no content dedup exists
  or is introduced.

## D. Quota-period/version model

A **phase** is one interval of one membership's quota configuration. Starting a
new phase (owner action "Avvia nuova fase beta") increments `quota_version` and
resets `analysis_consumed`/`analysis_reserved` to 0 while the previous phase's
numbers remain in `beta_program_usage` rows tagged with the old `quota_version`
(history, never deleted). Changing the mode or raising/lowering the limit
**within** the same phase does not bump `quota_version` — only a new phase does
(this is what makes "increase preserves consumed" work: 5→8 with 4 consumed
stays version N, consumed stays 4, remaining becomes 4; only "Avvia nuova fase
beta" resets consumed to 0 under version N+1).

Every atomic reservation filter (§E) includes `quota_version` equal to the
membership's *current* version, so an in-flight reservation from a phase that
gets superseded mid-request can never silently succeed against the new phase's
counters (it will simply fail the filter and the request falls through to
release/fallback, §Q). "Avvia nuova fase beta" refuses to proceed while any
`RESERVED` usage rows exist for the membership (a `count_documents` check,
indexed — §L) unless the owner explicitly force-releases them first (each such
forced release is its own audited `QUOTA_SLOT_RELEASED` event, reason
`PHASE_TRANSITION_FORCE_RELEASE`); this avoids stranding a phantom reservation
across a version bump.

## E. Reservation lifecycle

**Stored counters, not derived.** `analysis_consumed` and `analysis_reserved`
live directly on the `beta_program_memberships` document (§J), not aggregated
from the ledger at read time. This is the load-bearing decision given the
verified standalone topology: the only atomic primitive available is a
single-document conditional `find_one_and_update`, and that only works if the
capacity check and the counter mutation are the *same* document operation. If
counters were derived from `beta_program_usage` via aggregation, granting a slot
would require either a transaction (unavailable) or a read-then-write in
application code (explicitly forbidden by the mission and inherently racy). The
ledger (`beta_program_usage`, §K) remains the durable, append-only, per-analysis
detail record — audit trail and reconciliation source — but it is not the
capacity gate.

Reservation, in order, at the very top of `analyze_perizia` (before the file-type
check, per "Key determinations"):

1. **Mint `analysis_id`** early (moved up from `server.py:16342`).
2. **Idempotency guard** (defensive, should never fire in normal operation):
   `beta_program_usage.find_one({"analysis_id": analysis_id})` — if found, return
   the existing outcome rather than reserving again (guarded structurally anyway
   by `analysis_id` being freshly minted per call, but cheap and closes a
   crash-recovery race, §I).
3. **UNLIMITED**: no reservation row, no counter touch — proceed exempt exactly
   as today (`quota_mode == "UNLIMITED"` short-circuits before any Mongo write).
4. **LIMITED**: one atomic conditional update on the membership document:

   ```python
   updated = await db.beta_program_memberships.find_one_and_update(
       {
           "membership_id": membership_id,
           "status": "ACTIVE",
           "quota_mode": "LIMITED",
           "quota_version": expected_version,
           "$expr": {
               "$lt": [
                   {"$add": ["$analysis_consumed", "$analysis_reserved"]},
                   "$analysis_limit",
               ]
           },
       },
       {"$inc": {"analysis_reserved": 1}},
       return_document=ReturnDocument.AFTER,
   )
   ```

   This is a single document, single round trip, and MongoDB guarantees it is
   atomic regardless of replica-set status. If `updated` is `None`, no slot was
   available (either genuinely exhausted, or the version changed mid-flight) —
   fall through to §Q. If `updated` is not `None`, the reservation succeeded;
   insert the `beta_program_usage` row (state `RESERVED`, §K) as the durable
   detail record. If that insert unexpectedly fails (duplicate `analysis_id` —
   should be impossible given step 2, but defensive), compensate with
   `$inc: {"analysis_reserved": -1}` (bounded, §L) and surface a 500 rather than
   silently leaking a phantom reservation.
5. Not a beta member at all: unchanged, normal credit path.

## F. Consumption boundary

> **OWNER AMENDMENT (2026-07-20) — SUPERSEDES §F/§G BELOW WHERE THEY CONFLICT.**
>
> The boundary is **"was a paid processing call actually initiated?"**, tracked
> authoritatively in the backend — **never inferred from the final report
> status**. The original plan released the slot on `UNREADABLE` and
> `PIPELINE_TIMEOUT`; that is now WRONG, because both routinely occur *after*
> real money has been spent on Document AI OCR.
>
> Verified paid call sites in the synchronous `analyze_perizia` path (all inside
> `run_pipeline`, all BEFORE `insert_one`):
> 1. `_extract_with_docai(...)` — Google Document AI OCR (`server.py:16405`),
>    conditional on `needs_ocr_fallback`;
> 2. the QA gate's LLM call (`qa_meta["llm_used"]`, `server.py:~16636`);
> 3. `_apply_post_qa_decision_narrator(...)` — Gemini (`server.py:16654`).
> 4. (post-persist) `correctness_v2_api.autostart_job` — OpenAI V2 pipeline.
>
> A `mark_paid_processing_started()` marker — idempotent, set on the usage-ledger
> row — MUST be set immediately BEFORE each of sites 1–3 fires. Site 4 is always
> after a `COMPLETED` persist, which already implies consumption.
>
> Resulting rule:
> - **CONSUME** (`CONSUMED_PAID_PROCESSING_STARTED`) when the marker is set —
>   including `UNREADABLE`, `PIPELINE_TIMEOUT` during/after paid processing,
>   validation failure, verification required, or any later service error that
>   yields no usable report.
> - **RELEASE** (`RELEASED_BEFORE_PAID_PROCESSING`) only when the request is
>   rejected before *every* paid call: invalid file type, invalid/corrupt PDF,
>   size/page-limit rejection, upload persistence failure, preflight failure, or
>   a timeout occurring before paid processing began.
> - Never debit normal purchased credits for a beta-covered attempt. Never call
>   Stripe.
> - Tester UI must explain an honestly-consumed failure:
>   "L'analisi non ha prodotto un report utilizzabile, ma è stata conteggiata
>   perché l'elaborazione del documento era già iniziata."
> - Tests are required for `UNREADABLE` and timeout on BOTH sides of the marker.

At the exact point described in "Key determinations" — `analysis_dict["status"]`
known, about to `insert_one` (`server.py:16744-16759`):

- `status == "COMPLETED"` **and** this request holds a reservation → atomically
  transition the usage row `RESERVED → CONSUMED`
  (`find_one_and_update({"analysis_id": analysis_id, "state": "RESERVED"},
  {"$set": {"state": "CONSUMED", "consumed_at": now}})`) **and** atomically move
  the membership counters
  (`find_one_and_update({"membership_id", "analysis_reserved": {"$gte": 1}},
  {"$inc": {"analysis_reserved": -1, "analysis_consumed": 1}})`) — net capacity
  unchanged (a reserved slot becomes a consumed slot), so no capacity re-check is
  needed here; the `$gte: 1` guard only prevents going negative under a
  duplicate/racy call.
- From this point on, usage stays `CONSUMED` regardless of what happens to the
  async V2 job (`REPORT_READY` never reached, `VERIFICATION_REQUIRED`,
  `FAILED_ANALYSIS`, any later service error) — the V2 job is untracked by this
  feature by design (§ "Key determinations"), matching how the existing paid
  credit debit already behaves identically (debited once, regardless of V2
  outcome).
- `status == "UNREADABLE"` → release, not consume (§G) — this is a deliberate,
  named divergence from paid-credit behavior (§C), flagged for owner sign-off.

## G. Release boundary

Every release is one atomic transition on the usage row
(`RESERVED → RELEASED`, `release_reason` set, `released_at` set) plus the
compensating membership decrement
(`find_one_and_update({"membership_id", "analysis_reserved": {"$gte": 1}},
{"$inc": {"analysis_reserved": -1}})`). Deterministic reasons, one per call site:

| Reason code | Call site |
|---|---|
| `INVALID_FILE_TYPE` | `server.py:16283-16287` |
| `DOCUMENT_UNREADABLE_BEFORE_PAID_ANALYSIS` | `server.py:16294-16298` (unparseable PDF) and `16744` (deterministic `UNREADABLE` outcome) |
| `PAGE_COUNT_UNSUPPORTED` | `server.py:16300-16309` |
| `PIPELINE_TIMEOUT` | `server.py:16495-16508` (`_persist_failed_analysis`) |
| `UPLOAD_PERSISTENCE_FAILURE` | `db.perizia_analyses.insert_one` (`16759`) raising |
| `JOB_CREATION_FAILURE_BEFORE_PROCESSING` | reservation succeeded but an unrelated exception aborts the handler before `run_pipeline()` starts (defensive catch-all around the new reservation block) |
| `AUTHORIZATION_RACE` | membership revoked between reservation and consumption (re-check `status == "ACTIVE"` is implicit in the consume filter — if it fails, this reason is recorded and release fires) |
| `DUPLICATE_REQUEST_LINKED_TO_EXISTING_ANALYSIS` | idempotency guard (§E.2) finds a prior row for the same `analysis_id` |
| `PHASE_TRANSITION_FORCE_RELEASE` | owner starts a new phase while a `RESERVED` row is stale (§D) |
| `STALE_RESERVATION_NO_ANALYSIS_FOUND` | crash recovery, no persisted analysis exists after the safe duration (§I) |

Never released after `CONSUMED` — the consume/release transitions are mutually
exclusive filters (`state: "RESERVED"` required by both), so a race between them
resolves to whichever atomic update wins; the loser's `find_one_and_update`
simply matches nothing and no-ops.

## H. Duplicate/idempotency behavior

- Unique index on `beta_program_usage.analysis_id` (sparse not needed — every row
  has one, minted before insertion). A second attempt to reserve/consume/release
  for the same `analysis_id` is a structural no-op (the `state` filter in every
  transition prevents a double-apply; the unique index prevents a double-insert
  at reservation time).
- There is intentionally **no** upload-content dedup (§C) — this feature does not
  read or key on `input_sha256`, and per the mission a genuinely repeated upload
  consumes a second slot.
- `upload_request_id` as a separate concept was considered and rejected: nothing
  in the current frontend or backend generates one, and introducing a
  browser-supplied key would violate "never trust a browser-generated key
  alone." `analysis_id` — backend-minted, unguessable, one-per-call — already
  satisfies every property required of an idempotency key, so it *is* the key;
  no new identifier is introduced.

## I. Crash recovery

Reconciliation job, indexed and bounded — modeled directly on the existing
`orchestrator.recover_stale_jobs` pattern (`correctness_v2/orchestrator.py:350-401`,
`_STALEABLE_STATUSES`, `_stale_job_seconds()` env-configurable,
`DEFAULT_STALE_JOB_SECONDS = 1800`, `orchestrator.py:216-230`):

- Compound index `(state, reserved_at)` on `beta_program_usage` (§L). Query:
  `find({"state": "RESERVED", "reserved_at": {"$lt": cutoff}})` — filters on
  `state` first (a small subset of the collection), then range on `reserved_at`;
  never a full collection scan.
- Safe duration: `BETA_QUOTA_STALE_RESERVATION_SECONDS`, default `600` (10
  minutes) — five times `PIPELINE_TIMEOUT_SECONDS` (`120`, `server.py:123`), the
  hard ceiling on how long the synchronous `analyze_perizia` request (which is
  the only place a reservation is created or resolved) can legitimately take.
  Age alone never releases (per mission) — the sweep additionally checks:
  - a `perizia_analyses` doc exists with this `analysis_id` and
    `status == "COMPLETED"` → **reconcile to `CONSUMED`** (paid processing
    definitely began and was durably committed; only the post-persist
    counter/ledger write must have been interrupted, e.g. process killed between
    `insert_one` and the consume transition) — idempotent, re-running the sweep
    twice on the same row is a no-op once it is `CONSUMED`.
  - a `perizia_analyses` doc exists with `status ∈ {UNREADABLE, FAILED}` →
    release with `STALE_RESERVATION_NO_ANALYSIS_FOUND`'s sibling reasons already
    covered by §G (the sweep simply applies the same rule it would have applied
    synchronously, had the process not died).
  - no `perizia_analyses` doc exists at all after the safe duration → release,
    `STALE_RESERVATION_NO_ANALYSIS_FOUND` (the request almost certainly crashed
    before reaching persistence).
- Idempotent and safe to run repeatedly / concurrently: every transition is the
  same `state: "RESERVED"`-guarded atomic update used everywhere else in this
  design: at most one runner's update matches per row.
- Never scans `beta_program_memberships` or performs any filesystem walk.

## J. Membership schema (additive fields on `beta_program_memberships`)

```jsonc
{
  // ... existing fields unchanged (store.py:32-39, 140-159 of the admin plan) ...
  "quota_mode": "UNLIMITED",          // "UNLIMITED" | "LIMITED"; default UNLIMITED (migration, §S)
  "analysis_limit": null,             // int > 0 when LIMITED; null when UNLIMITED
  "analysis_consumed": 0,             // stored counter, current phase only
  "analysis_reserved": 0,             // stored counter, current phase only (in-flight)
  "quota_version": 1,                 // ++ only on "Avvia nuova fase beta"
  "quota_period_started_at": "<iso>", // set at ACTIVE-with-quota or phase start
  "quota_updated_at": null,
  "quota_updated_by": null,           // owner email
  "quota_note": null                  // owner-only free text, never shown to tester
}
```

Rationale for **stored** `analysis_consumed`/`analysis_reserved` (not derived)
is given in §E; restated briefly: the standalone (non-replica-set) mongod means
the only available atomicity primitive is a single-document conditional update,
which requires the capacity numbers to live on the document being updated.
`beta_program_usage` remains the source of truth for *history and audit*; the
membership document is the source of truth for *current capacity*, kept in sync
by the fact that every transition that touches one also atomically touches the
other in the same request.

## K. Usage-ledger schema — new collection `beta_program_usage`

```jsonc
{
  "usage_id": "betause_<uuid4hex16>",
  "membership_id": "betam_...",
  "user_id": "user_...",
  "normalized_email": "tester@example.com",
  "analysis_id": "analysis_...",       // authoritative idempotency key (§H) — unique
  "quota_version": 1,
  "state": "RESERVED",                 // RESERVED | CONSUMED | RELEASED | REJECTED
  "reserved_at": "<iso>",
  "consumed_at": null,
  "released_at": null,
  "release_reason": null,              // one of the codes in §G, or null
  "created_at": "<iso>",
  "updated_at": "<iso>"
}
```

Explicitly **never** stores: PDF bytes/content, extracted evidence, party
names, prompt text, model tokens, secrets, or session data — only identifiers,
state, timestamps, and a closed reason-code vocabulary. Matches the
`v2_job_events` "safe metadata only" contract already shipped
(`docs/beta_program_admin_plan.md` §P.0).

Uniqueness:
- Unique index on `analysis_id` — prevents one analysis from ever being reserved
  or consumed twice (§H).
- `(membership_id, quota_version)` compound index — used by the owner detail
  view (§N) and by the phase-transition guard (§D).

## L. Indexes

New, additive, created idempotently in the existing `ensure_indexes()` pattern
(`beta_program/store.py:111-132`, mirrored by a new function in the same
module or a sibling `quota.py`):

- `beta_program_usage`: unique `analysis_id`; `(state, reserved_at)` (crash
  recovery, §I); `(membership_id, quota_version)` (owner views, §D); `(user_id,
  created_at)` (symmetry with existing patterns, not required by any query in
  this plan but cheap and consistent).
- `beta_program_memberships`: no new index required — `quota_mode`/`analysis_limit`
  ride on the existing document; the atomic reservation filter
  (`membership_id`, `status`, `quota_mode`, `quota_version`) is satisfied by the
  existing unique `membership_id`-style point lookup (membership_id is looked up
  by the existing `_id`-equivalent access pattern the store already uses —
  confirmed no separate index is needed since every membership operation in
  `store.py` already keys on `membership_id` as a point read, e.g.
  `store.py:222-225`).

## M. Audit events

Reuses the exact append-only `beta_program_audit` collection and writer pattern
already shipped (`store.py:138-168`, `_write_audit`). This feature exposes a
public wrapper (e.g. `store.write_audit_event(...)`, a thin rename/alias of the
existing private `_write_audit`) so the new `quota.py` module reuses the same
writer rather than duplicating it — one append-only audit collection for the
whole beta program, membership and quota events interleaved by `created_at`.

New actions (free-text `action` field, no schema change needed):
`QUOTA_MODE_CHANGED`, `QUOTA_LIMIT_CHANGED`, `QUOTA_PHASE_STARTED`,
`QUOTA_SLOT_RESERVED`, `QUOTA_SLOT_CONSUMED`, `QUOTA_SLOT_RELEASED`,
`QUOTA_EXHAUSTED`, `QUOTA_AVAILABLE_AGAIN`.

- `QUOTA_MODE_CHANGED`/`QUOTA_LIMIT_CHANGED`/`QUOTA_PHASE_STARTED`: `actor_type
  = OWNER`, written synchronously in the admin route handler (§N), `meta`
  carries before/after mode, limit, version.
- `QUOTA_SLOT_RESERVED`/`_CONSUMED`/`_RELEASED`: `actor_type = SYSTEM_UPLOAD`
  (new actor type, alongside existing `OWNER`/`SYSTEM_LOGIN`/`MIGRATION`),
  written from `analyze_perizia`'s new reservation/consume/release calls;
  best-effort (audit write failure never blocks the upload, same tolerance as
  every other audit write in this codebase, `store.py:165-168`).
- `QUOTA_EXHAUSTED`: written once, the first time a reservation attempt fails
  for a membership (transition detection: compare `analysis_consumed +
  analysis_reserved == analysis_limit` before vs after — recorded from the
  release/consume path that pushes it over, not from the failed reservation
  itself, to keep it a single deterministic event per exhaustion rather than one
  per blocked request).
- `QUOTA_AVAILABLE_AGAIN`: written when a limit increase or new phase brings
  `analysis_consumed + analysis_reserved < analysis_limit` for a previously
  exhausted membership.
- Telemetry-safety mirrors `v2_job_events`'s existing contract (§P.0 of the
  admin plan) but is **not** routed through that queue — quota audit writes are
  a normal awaited Mongo insert inside the already-synchronous `analyze_perizia`
  request (same cost class as the existing credit-ledger write at the same call
  site), not a hot per-lot signal.

## N. Owner actions (Programma Beta → Tester)

Extends the existing owner-only router (`beta_program/api.py`, prefix
`/admin/beta-program`, gated by `require_exact_owner_admin`) with:

| Method | Path | Purpose |
|---|---|---|
| `PATCH` | `/testers/{membership_id}/quota` | Body `{quota_mode, analysis_limit?}`. Illimitata → `quota_mode=UNLIMITED, analysis_limit=null`. Limitata+N → validates `N` is a positive int; **increase preserves `analysis_consumed`** (only `analysis_limit` changes — no counter reset, matching "5→8 with 4 consumed leaves 4 remaining"); **decrease below `analysis_consumed`** is allowed (`analysis_limit` is set to the requested value even if `< analysis_consumed` — the derived state becomes `EXHAUSTED` immediately, per mission; no retroactive charge, no negative counter — the reservation filter's `$expr` naturally yields `analysis_consumed + analysis_reserved >= analysis_limit`, blocking further reservations without any special-case code). Audit `QUOTA_MODE_CHANGED` and/or `QUOTA_LIMIT_CHANGED`. |
| `POST` | `/testers/{membership_id}/quota/new-phase` | Requires explicit confirmation body (`{"confirm": true}` — a lightweight guard against accidental resets, matching the existing revoke/reactivate friction pattern). Bumps `quota_version`, resets `analysis_consumed=0, analysis_reserved=0` (after the force-release guard in §D), sets `quota_period_started_at=now`. Audit `QUOTA_PHASE_STARTED` with before/after version and the previous phase's final consumed count preserved in `meta` (history). |
| `GET` | `/testers/{membership_id}/quota/phases` | Read-only: `beta_program_usage` grouped by `quota_version` (aggregation, `count_documents`-class cost) + the audit trail entries for that membership filtered to `QUOTA_*` actions — the historical-phases view. |

Frontend (`frontend/src/pages/admin/betaProgram/TestersTab.js`, `130-278`):
new **"Gestisci limite"** row action (alongside the existing Revoca/Riattiva
buttons, `243-251`) opens a dialog (same modal pattern as `RevokeDialog`,
`105-128`) with a mode toggle (Illimitata / Limitata) and a number input shown
only for Limitata, a "Salva" button calling `PATCH .../quota`, and — for
Limitata — a secondary **"Avvia nuova fase beta"** button with its own
confirmation dialog (explicit copy: "Questo azzera il conteggio consumato a 0
partendo da una nuova fase; la fase precedente resta visibile nello storico.")
calling `POST .../quota/new-phase`. The tester table gains a "Quota beta"
column (Illimitata / `consumed/limit` / "Esaurito") and a "Storico fasi" link
opening the phases view.

## O. Tester display (exact Italian strings, per mission)

`beta_program.store.resolve_snapshot` (`store.py:174-210`) projection is
extended to also return `quota_mode`, `analysis_limit`, `analysis_consumed`,
`analysis_reserved`, `quota_version` so `_normalize_account_state`
(`server.py:1076-1134`) and `_build_user_response` (`server.py:1167-1198`) can
derive a `beta_quota` block on the customer-safe `beta_program` response object
(`server.py:1191-1197`) without a second query:

```jsonc
"beta_program": {
  "active": true,
  "display_name": "...",
  "member_since": "...",
  "quota": {
    "state": "AVAILABLE",       // "UNLIMITED" | "AVAILABLE" | "EXHAUSTED"
    "limit": 5,                  // null when UNLIMITED
    "remaining": 3,              // max(0, limit - consumed - reserved); absent when UNLIMITED
    "in_progress": 0             // == analysis_reserved; drives "1 analisi in elaborazione"
  }
}
```

Frontend copy (`Dashboard.js:163-182`, `345-350`; `Billing.js:847-860, 897-902,
965-980`; `featureAccess.js:33-54`) branches on `quota.state`:

- `AVAILABLE` (LIMITED): badge **"Accesso Beta"**, body
  **"{remaining} perizie beta disponibili su {limit}"**.
- `EXHAUSTED`: badge **"Limite beta raggiunto"**, body
  **"0 perizie beta disponibili su {limit}"**, plus
  **"Gli eventuali crediti acquistati restano disponibili."**
- `UNLIMITED`: badge **"Accesso Beta"**, body
  **"Analisi illimitate durante il programma beta"** (today's copy, unchanged).
- `in_progress > 0`: an additional line **"1 analisi in elaborazione"** (shown
  regardless of mode, while a reservation is open — in practice a narrow window
  since the synchronous request resolves within `PIPELINE_TIMEOUT_SECONDS`).
- The real paid balance is **always** shown separately as **"Crediti
  preservati"** (`Billing.js:898`, unchanged label, now also correct for
  EXHAUSTED testers who may be spending real credits via fallback, §Q) — never
  9999, never a synthetic/infinite value; `Billing.js`'s existing
  `betaActive` conditionals (`847, 965, 974, 978`) keep hiding purchase/recharge
  CTAs for any ACTIVE member (UNLIMITED or LIMITED, any quota state) and keep
  showing "Gestisci abbonamento" whenever `hasManagedSubscription` is true
  (`Billing.js:929-931`, untouched).

## P. Exhaustion behavior

When `quota.state == "EXHAUSTED"` and the tester attempts a new upload:

1. Reservation fails (§E.4, no document matches the `$expr` filter).
2. `analyze_perizia` does **not** raise immediately — it falls through to the
   existing real-credit check (§Q) using the account's actual
   `perizia_scans_remaining`.
3. If real credits/subscription cover the required band → the upload proceeds
   as a fully normal paid upload: no beta usage row is created, the real debit
   fires at `16781-16796` exactly as for a non-beta customer, `entitlement_context`
   is stamped `PAID`/`FREE` (not `BETA`) for that analysis.
4. If real credits do not cover it → `403` with the customer-safe reason code
   `BETA_LIMIT_REACHED` (new, distinct from `INSUFFICIENT_PERIZIA_CREDITS`),
   zero OpenAI call, zero job spawn (`autostart_job` is never reached — the
   exception is raised before IDs are even used for a pipeline run), zero
   Stripe call, zero partial debit. Payload mirrors the existing
   `INSUFFICIENT_PERIZIA_CREDITS` shape (`server.py:16321-16337`) for frontend
   consistency: `{code, message_it, message_en, required_credits,
   remaining_credits, pages_count}` plus `beta_limit: {consumed, limit}`.

## Q. Normal-plan fallback (the core code change in `server.py`)

This is the only functional change to the credit-check block
(`server.py:16311-16337`) and the debit skip (`12221-12229`):

- Replace the single `beta_unlimited = _user_has_active_beta(user)` line with a
  call into the new `beta_program.quota` module, e.g.
  `beta_outcome = await quota.resolve_upload_slot(user, analysis_id)`, returning
  one of `{"mode": "UNLIMITED"}`, `{"mode": "GRANTED", "usage_id": ...}`,
  `{"mode": "FALLBACK"}` (reservation failed, proceed to real-credit check),
  `{"mode": "BLOCKED"}` (reservation failed **and** real credits also
  insufficient — only known after the existing check runs, so `FALLBACK` and
  `BLOCKED` share the same code path through the existing `if remaining <
  required` gate; `BLOCKED` is simply the case where that gate still trips).
- `_is_credit_exempt_user(user)` at the gate (`16320`) is replaced by checking
  `beta_outcome["mode"] in ("UNLIMITED", "GRANTED")` for *this request* — the
  function itself (`661-665`) is untouched (other call sites — the beta
  dashboard gate, feedback role stamping — correctly keep using "is this account
  a beta member," which is a different question from "did this upload get a
  free slot").
- The debit call (`16782`) gains an explicit parameter
  `beta_slot_granted=beta_outcome["mode"] in ("UNLIMITED", "GRANTED")`; inside
  `_apply_perizia_credit_debit_with_ledger` (`12209-12276`) the unconditional
  `if _user_has_active_beta(user): return False` (`12221`) becomes `if
  beta_slot_granted: return False` (parameter defaults to
  `_user_has_active_beta(user)` when not passed, so the one other, currently
  uncalled, reference `_decrement_quota_if_applicable` at `12461-12469` keeps
  today's behavior unchanged and needs no test coverage change).
- `_resolve_entitlement_context` (`674-688`) gains the same distinction: `BETA`
  only when `beta_slot_granted` for *this* analysis, else falls through to its
  existing `PAID`/`FREE` logic — still pure observational metadata, no billing
  read depends on it (unchanged invariant from the admin plan, §L there).

No Stripe code, no credit-band formula, no wallet-normalization function is
touched — the fallback re-enters the *existing* paid path unmodified; this
feature only decides, once per request, whether that path's exemption applies.

## R. Purchased-credit preservation

Unchanged guarantees from the admin plan (§K there) continue to hold; this
feature adds one more:

- An EXHAUSTED tester who falls back to a real paid debit is charged **exactly**
  what a non-beta customer would be charged for the same page count — no beta
  surcharge, no special band, no double counting (the beta reservation already
  failed and wrote nothing consumable before the real debit runs).
- `analysis_consumed`/`analysis_reserved` are never derived from or reconciled
  against `credit_ledger` — the two ledgers are independent; a beta use and a
  paid debit for the same tester in the same phase are mutually exclusive per
  analysis (an analysis is either `beta_slot_granted` or normally debited, never
  both, enforced by the single boolean threaded through §Q).
- Billing UI keeps showing the real purchased balance as "Crediti preservati"
  for ACTIVE members regardless of quota state (§O) — EXHAUSTED does not change
  what "preserved" means; it only means new beta-free uploads have stopped.

## S. Existing membership migration

One-shot, idempotent backend migration (new function in
`beta_program/migrate.py`, run via the existing
`backend/scripts/beta_program_migrate.py` CLI with a new `--apply-quota-defaults`
flag, or folded into the existing `--apply` path since it's equally safe to run
unconditionally):

- For every existing `beta_program_memberships` document missing `quota_mode`:
  set `quota_mode="UNLIMITED", analysis_limit=null, quota_version=1,
  analysis_consumed=0, analysis_reserved=0, quota_period_started_at=<added_at
  or now>, quota_updated_at=null, quota_updated_by=null, quota_note=null`.
- Idempotent: a document that already has `quota_mode` set is skipped
  untouched (`skipped_existing_quota` in the report) — safe to run the apply
  step twice.
- Dry-run (default) reports counts only, writes nothing — same contract as the
  existing migration script.
- **No retroactive counting**: existing `perizia_analyses`/`v2_job_events` rows
  created by beta testers before this feature are never walked to backfill
  `analysis_consumed` — per the mission's explicit instruction ("do NOT
  retroactively count historical analyses unless deterministic and reviewed"),
  and because doing so would require deciding how the FAILED/UNREADABLE
  divergence (§C) applies retroactively, which is exactly the kind of judgment
  call the mission says needs review, not automation. Every existing membership
  starts its first LIMITED phase (whenever the owner later sets one) at
  `analysis_consumed=0`, i.e. with a full fresh allowance — flagged as a product
  decision in the closing section.

## T. Active-session refresh

No new mechanism needed: `get_current_user` (`server.py:1200-1245`) already
re-resolves the beta snapshot from the database on every authenticated request
(`1236-1238`) and never persists it to `db.users`. Extending
`resolve_snapshot`'s projection (§O) to include the quota fields means a
mode/limit change made by the owner is visible to the tester on their **very
next request** — no restart, no re-login, no session mutation — identical to
how ACTIVE/REVOKED transitions already propagate (admin plan §I). A reservation
made and released/consumed within one request never needs to be "pushed" to the
client mid-flight; the response of that same request already reflects the
outcome (report success, or the `BETA_LIMIT_REACHED` error body).

## U. Tests

43 backend tests (pytest, `backend/tests/beta_program_fakes.py`'s
`FakeCollection`/`install_fake_db` pattern, `pytest.mark.anyio`, synthetic
emails only — `tests/test_beta_program_entitlement.py:1-36` is the reference
style). **One test-infrastructure change is required and is called out
explicitly rather than silently assumed**: `FakeCollection`
(`beta_program_fakes.py:115-159`) has no `find_one_and_update`, and its `_match`
helper (`55-94`) has no `$expr` support — both must be added (in that test-fakes
file, not in this plan) before the atomic-reservation tests in group 1 can run
against the fake; the real-Mongo validation in §V exercises the genuine
`find_one_and_update`/`$expr` behavior against the actual standalone mongod, so
correctness does not depend on the fake being a perfect model.

Backend test groups (component in parentheses):

1. **Atomic reservation / capacity** (8, `beta_program/quota.py` +
   `analyze_perizia` integration): two concurrent reservations for the last
   slot yield exactly one acceptance; `analysis_consumed + analysis_reserved`
   never exceeds `analysis_limit` under concurrency; UNLIMITED always grants
   without writing a usage row; PENDING/REVOKED membership never grants;
   `quota_version` mismatch fails closed; a successful reservation increments
   `analysis_reserved` by exactly 1; replaying the same `analysis_id` is a
   no-op idempotent read, not a second reservation; reservation respects a
   just-started new phase's fresh version.
2. **Consumption boundary** (6, `quota.py` + `server.py` integration): COMPLETED
   persist transitions RESERVED→CONSUMED and moves the membership counters;
   UNREADABLE outcome releases instead of consuming; a simulated post-consume
   V2 failure (FAILED_ANALYSIS/VERIFICATION_REQUIRED) does not touch the usage
   row; debit is skipped only when `beta_slot_granted` is true, not merely
   because the account is an active beta member; `entitlement_context` reflects
   `BETA` only when a slot was actually granted for that analysis; UNLIMITED
   member's analysis never creates a usage row at all.
3. **Release boundary** (7, `quota.py` + `server.py` integration): invalid file
   type releases with `INVALID_FILE_TYPE`; unsupported page count releases with
   `PAGE_COUNT_UNSUPPORTED`; simulated `PIPELINE_TIMEOUT` releases with
   `PIPELINE_TIMEOUT`; simulated persistence failure releases with
   `UPLOAD_PERSISTENCE_FAILURE`; membership revoked mid-flight releases with
   `AUTHORIZATION_RACE`; every release decrements `analysis_reserved` and never
   drives it negative; a release never fires after the corresponding row is
   already `CONSUMED` (race-order test).
4. **Duplicate/idempotency** (4, `quota.py`): unique index rejects a second
   insert for the same `analysis_id`; a replayed consume call against an
   already-CONSUMED row is a no-op; a replayed release call against an
   already-RELEASED row is a no-op; concurrent duplicate reservation attempts
   for the same `analysis_id` yield exactly one RESERVED row.
5. **Stale/crash recovery** (5, new reconciliation sweep): a RESERVED row past
   the safe duration with a COMPLETED analysis reconciles to CONSUMED; one with
   an UNREADABLE/FAILED analysis releases; one with no analysis at all releases
   with `STALE_RESERVATION_NO_ANALYSIS_FOUND`; a RESERVED row under the safe
   duration is left untouched regardless of analysis state; the sweep's query
   only ever touches documents matching `state=RESERVED` (assert via a
   call-count/filter-shape spy on the fake collection, no full-collection scan).
6. **Owner actions** (7, `beta_program/api.py` new routes): set UNLIMITED;
   set LIMITED+N; increasing the limit preserves `analysis_consumed` (5→8 with
   4 consumed leaves 4 remaining); lowering below consumed sets EXHAUSTED
   immediately with no negative counters and no retroactive charge; "Avvia
   nuova fase beta" bumps `quota_version`, resets consumed/reserved to 0, and
   preserves the prior phase's numbers in history; non-owner-admin/tester/
   customer get 403 on every new route; unauthenticated gets 401.
7. **Exhaustion / fallback** (4, `server.py` integration): EXHAUSTED tester with
   sufficient real credits gets a normal paid upload (real debit fires, no beta
   usage row); EXHAUSTED tester with insufficient real credits gets `403
   BETA_LIMIT_REACHED` with zero OpenAI/job/Stripe/debit side effects (assert
   via monkeypatched sentinels, same pattern as the admin plan's determinism
   tests); UNLIMITED tester is never blocked regardless of real credit balance;
   `QUOTA_EXHAUSTED`/`QUOTA_AVAILABLE_AGAIN` audit rows are written exactly once
   at the correct transitions.
8. **Migration** (2, `beta_program/migrate.py`): existing memberships default to
   `UNLIMITED/null/version=1/reserved=0/consumed=0`; a second apply run is a
   total no-op (idempotent).

22 frontend tests (jest/RTL, patterns from `AdminBetaProgram.test.js` and
`TestersTab.test.js`):

1. **Tester tab quota UI** (6, `TestersTab.js`): "Gestisci limite" dialog opens
   and toggles Illimitata/Limitata; Save posts the correct `PATCH .../quota`
   body; increasing the limit shows/keeps the existing consumed count in the
   UI (no reset messaging); "Avvia nuova fase beta" requires explicit
   confirmation before posting; historical phases view renders phase rows;
   limit input validation rejects non-positive numbers client-side.
2. **Tester display** (8, `Dashboard.js`/`featureAccess.js`): AVAILABLE string
   exact match; EXHAUSTED string exact match including the "crediti acquistati"
   sentence; UNLIMITED string unchanged; `in_progress > 0` shows "1 analisi in
   elaborazione"; a snapshot assertion that no numeric literal `9999` or
   `Infinity`/`∞` ever renders in any beta-quota state; sidebar credit box
   renders all three quota states correctly; a REVOKED tester sees the normal
   (non-beta) credit box; an EXHAUSTED tester who also has a real paid
   subscription sees both the beta-exhausted state and "Gestisci abbonamento".
3. **Billing page** (5, `Billing.js`): purchase/recharge CTAs stay hidden for
   ACTIVE regardless of quota state; "Crediti preservati" label and real value
   shown for EXHAUSTED same as for UNLIMITED/AVAILABLE; subscription management
   controls remain visible under EXHAUSTED with a paid sub; REVOKED immediately
   shows normal billing CTAs; no Stripe network call is triggered merely by
   quota-state changes (axios mock call-list assertion).
4. **Safety/determinism** (3): quota-management controls (Gestisci limite,
   Avvia nuova fase) never render for a non-owner viewer of any admin page
   (visibility-pattern test, matching `components/correctness-v2/visibility.test.js`);
   the Dashboard/Billing pages issue no extra network calls beyond their
   already-declared GETs when quota fields are present in the response; a
   snapshot test asserts the exact three Italian strings byte-for-byte against
   the mission's specification (regression guard against copy drift).

## V. Real-Mongo lifecycle validation (scenarios A–L)

Same isolated-environment pattern as the admin plan (§W there) and the prior
storico/decision-workflow validations: temp DB name
(`beta_perizia_limits_validation_<ts>`) on the **same local standalone mongod**
(topology confirmed in "Key determinations" — no replica set exists to spin up,
so this validation is explicitly a standalone-topology test, which is exactly
the production topology being designed for), temp backend on a spare port,
`CORRECTNESS_V2_ARTIFACTS_ROOT` pointed at a temp dir. Prod `.env`, prod DB, prod
service untouched; DB dropped afterward.

- **A** Set a membership LIMITED with limit 2 → two uploads succeed and consume;
  a third is blocked `BETA_LIMIT_REACHED` (tester has no real credits) with
  zero OpenAI/job/debit side effects (verified via artifact-dir file count/mtime
  and `credit_ledger` row count, both unchanged by the blocked attempt).
- **B** Fire N concurrent requests (`N > remaining slots`) at the reservation
  endpoint path (via a harness that calls the reservation function directly
  under real asyncio concurrency, since simulating true concurrent HTTP clients
  against a single-process test backend is not necessary to prove the Mongo
  atomicity) → exactly `remaining` reservations succeed, `analysis_consumed +
  analysis_reserved` never exceeds `analysis_limit` at any observed point.
- **C** Upload with a corrupt/non-PDF file against a LIMITED tester with 1
  remaining slot → release fires (`INVALID_FILE_TYPE`/`PAGE_COUNT_UNSUPPORTED`),
  the slot is available again for the next real upload, `analysis_consumed`
  unchanged.
- **D** Upload a document that resolves to `UNREADABLE` → release
  (`DOCUMENT_UNREADABLE_BEFORE_PAID_ANALYSIS`), no V2 job spawned (artifact job
  dir count unchanged), slot remains available, real credit ledger unaffected.
- **E** EXHAUSTED tester with real purchased credits uploads → normal paid
  debit occurs (one `credit_ledger` row, no `beta_program_usage` row), report
  proceeds normally.
- **F** Owner increases limit 5→8 with 4 consumed → remaining becomes 4
  immediately (byte-compare the membership doc before/after: only
  `analysis_limit` and `quota_updated_at/_by` change).
- **G** Owner starts a new phase → `quota_version` increments, `consumed`/
  `reserved` reset to 0, previous phase's usage rows remain queryable by their
  old `quota_version`, audit shows `QUOTA_PHASE_STARTED`.
- **H** Owner lowers limit below consumed → tester immediately reads
  `EXHAUSTED` on next `/auth/me`-equivalent call, no negative counters, no
  retroactive ledger entry.
- **I** Kill the backend process mid-reservation (simulate by directly leaving
  a `RESERVED` row with `reserved_at` older than the safe duration and a
  matching `COMPLETED` `perizia_analyses` doc) → recovery sweep reconciles to
  `CONSUMED`, not release.
- **J** Same setup but no `perizia_analyses` doc exists → recovery sweep
  releases with `STALE_RESERVATION_NO_ANALYSIS_FOUND`; a fresh (under-threshold)
  `RESERVED` row is left untouched by the same sweep run.
- **K** Full index check: `index_information()` on `beta_program_usage` shows
  the unique `analysis_id` index and the `(state, reserved_at)` compound index;
  `beta_program_audit` contains the new `QUOTA_*` actions in the expected
  before/after order for the whole scenario run.
- **L** Determinism sweep, mirroring the admin plan's §W.I: every
  quota-management GET/PATCH/POST is called with an OpenAI-key-less env; zero
  OpenAI attempts, artifacts jobs dir unchanged, `credit_ledger`/`users`
  unchanged except where explicitly expected (E, F), zero outbound Stripe.

## W. Deployment

- Merge `feature-beta-perizia-limits` → `main`; frontend auto-deploys on push
  (Vercel); backend requires one owner-initiated `systemctl restart
  periziascan-backend` (same unit as the admin-program deploy). Startup
  `ensure_indexes` creates the new `beta_program_usage` indexes idempotently.
- Run the quota-defaults migration (`--apply`, or the existing script's
  extended flag, §S) once per environment — verified idempotent, safe to
  re-run.
- No `.env` change. No Stripe product/price/webhook change. No change to
  `PERIZIA_CREDIT_BANDS`, credit debit formula, or wallet normalization beyond
  the single explicit `beta_slot_granted` parameter threaded through the
  existing debit function (§Q).
- Smoke after deploy: owner sets a test membership to LIMITED(2), confirms two
  uploads succeed and a third shows `BETA_LIMIT_REACHED`, then sets it back to
  UNLIMITED and confirms uploads resume unrestricted; a normal paying customer's
  upload+debit path is unaffected (one regression paid analysis).

## X. Rollback

- `git revert` the merge and restart. `beta_program_usage` and the new
  membership fields are additive; old code never reads `quota_mode`/
  `analysis_limit`/etc. and treats a document missing them exactly as it does
  today (no schema migration required to roll back — the fields are simply
  ignored). No user/wallet/feedback/analysis document is mutated by this
  feature outside of the single explicit debit-skip parameter, which reverts
  cleanly with the code.
- Frontend rolls back via Vercel redeploy of the previous commit; the "Gestisci
  limite"/"Avvia nuova fase" UI disappears, the tester quota strings disappear,
  the pre-existing UNLIMITED-only beta copy returns.
- Rollback does not need to touch `beta_program_usage` — it is inert (nothing
  reads it) once the code that writes it is reverted.

## Y. Owner post-deployment configuration

No hardcoded name, email, or number is introduced anywhere in reusable quota
logic (`quota.py`, `store.py` extensions, `api.py` routes) — every membership's
mode/limit is set by the owner through the admin UI, per the mission's explicit
instruction. Nothing in this plan requires the owner to configure Mauro Torchio,
AGL Aste Immobiliari, or any specific number 5 in source, tests, or migration
defaults (the migration default is UNLIMITED for everyone, §S — the owner picks
LIMITED+N per tester afterward, live, no deploy). Post-deployment, the owner:
sets whichever testers should be LIMITED via "Gestisci limite"; decides whether
the Document AI OCR paid-call edge case in §C/§F (a `PIPELINE_TIMEOUT` that may
have already incurred a Google OCR cost without consuming a beta slot) is
acceptable as designed or needs a stricter rule; decides whether newly-migrated
existing LIMITED-eligible testers should start their first phase with full
allowance (as designed, §S) or with some deterministic backfill.

## Z. Scope exclusions

Unchanged by this branch: Stripe products/prices/checkout/webhooks; the normal
credit formula (`PERIZIA_CREDIT_BANDS`, page-band lookup); the customer report
decision model and its confirmation semantics; extraction, validator, money
chains, lot segmentation, per-lot concurrency; PDF/HTML export; GDPR wording and
retention policy; feedback wording; Segnali telemetry meaning
(`v2_job_events` contract, §P.0 of the admin plan, untouched — quota audit
events use the separate `beta_program_audit` collection, not the telemetry
queue, §M); membership status lifecycle (PENDING/ACTIVE/REVOKED, admin plan
§E-I) — quota is a second, orthogonal axis layered on top, never replacing or
duplicating it.

---

## Plan self-review

- **Atomic mechanism matches the real topology** — the deployed mongod is a
  standalone instance (`replSetGetStatus` → `NoReplicationEnabled`, verified
  live), so the design uses only single-document `find_one_and_update` with a
  conditional `$expr` filter, never a transaction. This is correct on a
  standalone instance and would remain correct if the deployment were ever
  upgraded to a replica set.
- **Stored counters, justified by the atomicity constraint** — derived/
  aggregated counters were rejected specifically because they cannot be the
  subject of a single atomic conditional increment; the usage ledger
  (`beta_program_usage`) still carries the durable, auditable per-analysis
  detail, so nothing about history or reconciliation is lost by keeping the
  fast-path counters on the membership document.
- **One integration point in `server.py`** — the entire feature hooks into a
  single existing function, `analyze_perizia` (`server.py:16276-16805`), at
  three points: the credit-check gate (`16311-16337`), the debit-skip
  (`12221-12229` via one new parameter), and the entitlement-context stamp
  (`674-688`). Every rerun/lot/workspace/confirmation endpoint is verified,
  by reading its actual code, to never mint a new `analysis_id` and is
  therefore structurally untouched.
- **Consumption boundary reuses the existing money boundary** — CONSUME is
  defined at exactly the point the current code already decides to debit real
  credits and spawn the real OpenAI job (`status == "COMPLETED"`,
  `server.py:16744-16772`), so the quota system never has to reason about the
  untracked async V2 job thread at all.
- **One deliberate, named divergence from paid-credit behavior** — `UNREADABLE`
  and `PIPELINE_TIMEOUT` release the beta slot even though the existing paid
  wallet either always charges (`UNREADABLE`) or never charges
  (`PIPELINE_TIMEOUT`) today; flagged explicitly below for owner sign-off
  rather than silently assumed.
- **No hardcoded identity or number** — verified by construction: `quota.py`,
  the `store.py`/`api.py` extensions, and the migration default never
  reference an email, name, or the number 5; every limit is owner-entered at
  runtime.
- **Rollback is inert** — new collection and new membership fields, nothing
  else mutated; reverting the code makes them simply unread.

### PRODUCT decisions genuinely open for the owner

1. **`UNREADABLE` beta-slot release vs. paid-credit charge divergence (§C/§F)**
   — today a normal paying customer IS charged even when their document comes
   back `UNREADABLE` (`server.py:16744, 16782` — debit is unconditional on
   status). This plan releases the beta slot for the same outcome, so a beta
   tester whose document is unreadable does not lose a use, while a paying
   customer in the identical situation is still charged. Is this acceptable, or
   should `UNREADABLE` consume a beta slot too (matching paid-credit parity
   rather than "protect the tester's allowance")?
2. **`PIPELINE_TIMEOUT` and the Document AI OCR edge case (§C/§Y)** — a timeout
   during `run_pipeline()` can occur after a real, paid Google Document AI OCR
   call has already fired (`server.py:16394-16431`) but before the analysis is
   persisted; today's paid-credit system does not charge for this case either
   (no call reaches the debit line), so this plan releases the beta slot too,
   matching existing behavior. This is a genuine (if rare, bounded by a 120s
   timeout) place where real money could be spent without any ledger — beta or
   paid — recording it. Out of scope to fix here (§Z), but worth the owner's
   awareness since this feature makes the beta-vs-paid symmetry explicit for the
   first time.
3. **Historical/back-dated allowance for existing testers (§S)** — every
   existing membership migrates to `UNLIMITED`; the first time the owner sets
   one to LIMITED, should that phase start at `analysis_consumed=0` (full fresh
   allowance, as designed) or should any of that tester's pre-existing
   analyses count toward the very first limit? This plan defaults to a full
   fresh allowance (simplest, safest, no retroactive judgment call) but the
   mission's "unless deterministic and reviewed" caveat makes this an explicit
   owner choice, not an assumption.
