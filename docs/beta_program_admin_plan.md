# Beta Program Admin — Implementation Plan (feature-beta-program-admin)

Base: `main` @ `ea2240a`. Audit performed read-only against the real codebase and a
read-only query of the live Mongo DB. All file:line citations are from that SHA.

Goal: replace the environment/hardcoded beta allowlist with a MongoDB-managed beta
program. Beta access becomes a backend-resolved ENTITLEMENT (never a wallet balance),
managed live by the exact owner (`nexodifyforyou@gmail.com`) from a new single admin
route "Programma Beta", with add/activate/revoke requiring no deploy, no env change,
no restart.

---

## A. Current beta architecture

- Beta status is an **environment allowlist**: `BETA_UNLIMITED_EMAILS` parsed at import
  time into a frozenset (`backend/server.py:113-115`), checked by
  `_is_beta_unlimited_email()` (`server.py:645-647`). Production `.env` has
  `BETA_UNLIMITED_EMAILS=''` (verified live) — there are currently **zero** runtime beta
  users.
- `BETA_PARTNER_NAMES: Dict[str,str] = {}` (`server.py:120`) — an in-source name map,
  deliberately empty today (hardcoded names were removed); `BETA_PARTNER_DEFAULT_TYPE`
  env default `'geometra'` (`server.py:116`).
- Beta grants exactly two things today:
  1. **Credit exemption** — `_is_credit_exempt_user()` (`server.py:650-654`) returns true
     for admin OR allowlisted beta email; used to skip the insufficient-credit block on
     upload (`server.py:16240`) and to skip the debit entirely in
     `_apply_perizia_credit_debit_with_ledger` (`server.py:12143-12151`, logged as
     `beta_partner_unlimited_access ... debit=skipped`).
  2. **Beta dashboard** — `require_beta_or_admin` (`server.py:1217-1224`) gates
     `GET /api/beta/dashboard-summary` (`server.py:20660-20739`); frontend `BetaRoute`
     (`frontend/src/App.js:91-115`) gates `/beta/dashboard`
     (`frontend/src/pages/BetaDashboard.js`), and the sidebar inserts a "Beta Partner"
     item when `is_beta_partner || is_master_admin`
     (`frontend/src/pages/Dashboard.js:59-61`).
- `is_beta_partner` / `beta_partner_name` / `beta_partner_type` are computed per
  request in `_normalize_account_state` (`server.py:1060,1066-1070`) and exposed in
  `_build_user_response` (`server.py:1125-1127`); `featureAccess.js:34-36` mirrors them.
- Beta users never get an admin surface: `AdminRoute` requires `is_master_admin`
  (`App.js:84-86`); the "Vista admin" (Correctness v2 debug tab) is gated by the
  separate exact-owner flag `correctness_v2_admin_view` (`server.py:1134-1136`,
  `_is_correctness_v2_admin_view_email` at `server.py:640-642`), which beta users never
  receive.
- Changing beta membership today requires editing `.env` and restarting the backend —
  exactly the property this feature removes.

## B. Current source of beta entitlement

Single source: **process environment at import time**.

- `BETA_UNLIMITED_EMAILS` env → `_parse_email_allowlist` (trim+lowercase,
  `server.py:94-101`) → frozenset consulted by `_is_beta_unlimited_email`
  (`server.py:645-647`).
- Runtime call sites (all must stop consulting env after this feature):
  `_is_credit_exempt_user` (`server.py:654`), `_normalize_account_state`
  (`server.py:1060`), `require_beta_or_admin` (`server.py:1222`), upload logging + block
  (`server.py:16232,16240`), debit skip (`server.py:12145`), feedback role/type stamping
  (`server.py:20360,20371`), dashboard summary (`server.py:20734`).
- **The 9,999 display**: comes from the internal `enterprise` plan definition —
  `credits=9999` (`server.py:561`) and
  `quota={"perizia_scans_remaining": 9999, ...}` (`server.py:570`). Admin accounts are
  forced onto it: at signup admins get the enterprise quota
  (`server.py:12482-12484`), the legacy wallet builder folds that 9999 into
  `monthly_remaining` for admins (`server.py:868-870`), and `_normalize_account_state`
  re-asserts enterprise for any admin email (`server.py:1038-1040`). The frontend
  renders it as "Crediti disponibili" from `periziaCredits.totalAvailable`
  (`frontend/src/lib/featureAccess.js:44-50`, `frontend/src/pages/Dashboard.js:45-48,158`).
  Verified live: the owner's user doc carries `perizia_scans_remaining: 9999`.
  **No beta path writes 9999 today** (beta = exemption, not balance) and this plan keeps
  it that way; the enterprise-9999 rendering for the owner account is out of scope and
  untouched.

## C. Existing credit & plan behavior

- Plans: `SUBSCRIPTION_PLANS` dict (`server.py:5xx`, enterprise at 553-571); paid
  recurring ids `{"solo","pro","studio"}` (`server.py:611`).
- Wallet: `perizia_credits` per-user doc — `monthly_remaining` + `pack_grants[]`
  (purchased packs, each with `amount_granted/amount_remaining/source/expires_at`) +
  derived `total_available`; normalized by `_finalize_perizia_credit_wallet`
  (`server.py:793-859`), legacy migration `server.py:862-902`, admin override
  `server.py:943-982`.
- Debit path: upload charges page-banded credits (`PERIZIA_CREDIT_BANDS`,
  `server.py:602-608`; `_get_required_perizia_credits` 617-622) AFTER successful
  persistence via `_apply_perizia_credit_debit_with_ledger` (`server.py:12133-12198`),
  which writes append-only `credit_ledger` entries
  (`_insert_credit_ledger_entry`, `server.py:11948-11990`; entry types
  `server.py:584-594`). Admins (`12143`) and beta emails (`12145`) return early —
  **no ledger entry is written for exempt runs** (relevant to §L).
- Lot generation/rerun charges nothing by design
  (`backend/correctness_v2/api.py:709-740`, `_lot_credit_preview` reports
  `will_consume_credit: False`, `exempt: server._is_credit_exempt_user(user)` at 738).
- Purchases: Stripe checkout appends pack grants (`_append_pack_grant`,
  `server.py:924-940`); subscription state normalized at `server.py:695-721`;
  admin manual adjustment via `PATCH /api/admin/users/{user_id}`
  (`server.py:18577-18674`) with `admin_adjustment` ledger entries.
- Per-request refresh: `get_current_user` (`server.py:1139-1169`) re-reads the session
  doc and the **users doc on every request** and re-normalizes+persists account state —
  sessions carry no entitlement claims (`UserSession`, `server.py:245-251`). This is the
  property that makes next-request revocation work with zero session surgery.

## D. Existing feedback model

- Collection `beta_feedback` (`server.py:20080-20402`). Document fields: `id`,
  `created_at/updated_at`, `user_id`, `user_email`,
  `user_role` (`admin|beta_partner|user`, stamped at `server.py:20369`),
  `beta_partner_name/type`, `analysis_id/case_id/file_name/document_hash`,
  `feedback_level`, `section_key(+label_it)`, `item_reference{}`,
  `original_ai_output{}`, `feedback_type` (12 values, `server.py:20127-20178`),
  `priority`, `expert_comment` (verbatim, sanitized only for control chars/length,
  `server.py:20194-20206`), `expected_correction/classification`, `expert_confidence`,
  `page_reference`, deterministic `learning_label{}`
  (`_derive_beta_learning_label`, `server.py:20185-20191` — **no LLM anywhere**),
  `status` (6 values, `server.py:20122-20124`), `admin_notes`, `reviewed_by/at`,
  `permission_for_learning`, `source`.
- APIs: `POST /api/beta-feedback` (any authed user; ownership check on `analysis_id`
  at `server.py:20346-20352`), `GET /api/beta-feedback/my` (own rows only, 20405),
  admin list/patch/export gated `require_master_admin`
  (`server.py:20451-20657`), with server-side filters
  (`_build_beta_feedback_admin_query`, 20417-20448) and deterministic metrics
  (`_compute_beta_feedback_metrics`, 20494-20519).
- Admin page `frontend/src/pages/admin/AdminBetaFeedback.js` (359 lines: filters,
  metric cards, paginated table, status editor, CSV/JSONL export) reached via sidebar
  entry "Beta Feedback" (`Dashboard.js:69`) and route `/admin/beta-feedback`
  (`App.js:250`).
- **Riccardo's two historical docs — verified live**:
  `betafb_6150bea873bc4ce3` and `betafb_d4b88e3e8d8c4300`, both
  `user_email=geomazzantiriccardo@gmail.com`, `user_role=beta_partner`,
  `beta_partner_name="Geom. Riccardo Mazzanti"`, created 2026-06-14. He has **no
  `users` document** and `BETA_UNLIMITED_EMAILS` is empty, so he currently has no
  access of any kind. Nothing in this feature writes to `beta_feedback` except the
  owner status PATCH; his docs are preserved verbatim by construction.
- Indexes already exist on `beta_feedback` `user_id/created_at/analysis_id/section_key/
  feedback_type/status/user_email` (`server.py:20876-20882`).

## E. Proposed beta membership schema

Collection: **`beta_program_memberships`** — the runtime source of truth for beta
entitlement. One document per normalized email, ever.

```jsonc
{
  "membership_id": "betam_<uuid4hex16>",
  "normalized_email": "tester@example.com",   // trim + lowercase; unique
  "user_id": null,                            // null while PENDING; set on activation
  "display_name": "Geom. Example",            // owner-entered; used for feedback display
  "partner_type": "geometra",                 // geometra|avvocato|investitore|altro
  "status": "PENDING",                        // PENDING | ACTIVE | REVOKED
  "added_by": "nexodifyforyou@gmail.com",
  "added_at": "<iso8601>",
  "activated_at": null,
  "revoked_at": null,
  "reactivated_at": null,
  "updated_at": "<iso8601>",
  "internal_note": null,                      // owner-only free text
  "entitlement_version": 1,                   // ++ on every status change
  "last_entitlement_change_at": "<iso8601>",
  "migration_source": null                    // "env_allowlist" | "manual_admin" | null
}
```

Indexes (added to the startup `ensure_indexes` list, `server.py:20839-20888`, plus a
module-level `ensure_indexes()` following the `user_confirmations` pattern,
`backend/correctness_v2/user_confirmations.py:60-84`):

- `create_index("normalized_email", unique=True, name="uq_beta_membership_email")`
- `create_index([("status",1),("updated_at",-1)], name="ix_beta_membership_status")`
- `create_index("user_id", name="ix_beta_membership_user")`

Email normalization: single helper `normalize_beta_email(raw) -> str` =
`str(raw).strip().lower()` (same rule as `_parse_email_allowlist`, `server.py:94-101`,
and `_get_or_create_authenticated_user`, `server.py:12453`). Invalid/empty → 422.

Backend module layout: new package **`backend/beta_program/`**
(`__init__.py`, `store.py` (schema, normalization, resolver, index bootstrap),
`api.py` (owner-only admin router), `signals.py` (deterministic metrics/signals
queries), `migrate.py` (import logic shared by the CLI script)). The router is mounted
exactly like correctness_v2 (`server.py:20748-20755`):
`app.include_router(beta_program_router, prefix="/api")`, with lazy `import server`
for the db handle (pattern: `correctness_v2/user_confirmations.py:46-49`).

## F. Proposed beta audit-event schema

Collection: **`beta_program_audit`** — append-only, never updated or deleted
(pattern: `admin_audit_log`, `server.py:12361-12381`; `correctness_v2_confirmation_audit`).

```jsonc
{
  "audit_id": "betaaud_<uuid4hex16>",
  "membership_id": "betam_...",
  "normalized_email": "tester@example.com",
  "action": "MEMBER_ADDED",   // MEMBER_ADDED | MEMBER_ACTIVATED | MEMBER_REVOKED |
                              // MEMBER_REACTIVATED | MEMBER_NOTE_UPDATED |
                              // MIGRATION_IMPORTED | MIGRATION_SKIPPED
  "actor_type": "OWNER",      // OWNER | SYSTEM_LOGIN | MIGRATION
  "actor_email": "nexodifyforyou@gmail.com",  // null for SYSTEM_LOGIN
  "actor_user_id": "user_...",
  "before_status": null,
  "after_status": "PENDING",
  "entitlement_version": 1,
  "meta": { },                // small scalars only (note-changed flag, source, dry_run)
  "created_at": "<iso8601>"
}
```

Indexes: `[("membership_id",1),("created_at",-1)]` and `("created_at",-1)`.
Every status transition writes exactly one audit row in the same request handler
(no background tasks). Audit write failure is logged but never blocks the mutation
(same tolerance as `_write_admin_audit`, `server.py:12378-12381`).

## G. Pending-email activation flow

Owner adds a tester who has never logged in:

1. `POST /api/admin/beta-program/testers` with `{email, display_name, partner_type?,
   internal_note?}` → normalize email → `db.users.find_one({"email": normalized})`.
   No user found → insert membership `status=PENDING, user_id=null`, audit
   `MEMBER_ADDED (→PENDING)`. Duplicate email → 409 pointing at the existing
   membership (unique index is the backstop).
2. On the tester's first login, `_get_or_create_authenticated_user`
   (`server.py:12452-12496`) gains one post-upsert hook:
   `await beta_program.link_pending_membership(email, user_id)` — a single indexed
   `find_one_and_update({"normalized_email": email, "status": "PENDING"},
   {$set: status=ACTIVE, user_id, activated_at, last_entitlement_change_at,
   $inc: entitlement_version})` + audit `MEMBER_ACTIVATED (actor_type=SYSTEM_LOGIN)`.
   The filter matches **only PENDING** — a REVOKED membership can never be
   reactivated by logging in (§S).
3. From that request onward the resolver (§J) sees an ACTIVE membership.

No email is ever written into source code or env; the flow is pure DB.

## H. Existing-user activation flow

Same `POST /api/admin/beta-program/testers`; when `db.users` already has the email,
the membership is created directly `status=ACTIVE` with `user_id` linked and
`activated_at=now` (audit `MEMBER_ADDED (→ACTIVE)`). The user's `users` document is
**not modified** in any way — no plan change, no quota/wallet write, no session
change. Entitlement applies on the user's next authenticated request because
`get_current_user` re-resolves per request (§I). Re-adding an email whose membership
is REVOKED does not create a new doc: the API returns 409 with
`reason_code=MEMBERSHIP_REVOKED` and instructs to use the explicit reactivate
endpoint (deliberate friction, §S).

## I. Active-session revocation strategy

**Chosen strategy: per-request DB resolution, zero session mutation.**

Sessions are opaque tokens; `get_current_user` already performs
`db.user_sessions.find_one` + `db.users.find_one` + full re-normalization on every
authenticated request (`server.py:1150-1169`). We add one indexed
`db.beta_program_memberships.find_one({"normalized_email": email, "status": "ACTIVE"},
projection)` in the same function and stamp the result onto the user doc
(`user_doc["beta_program"] = {...}` — see §J) before `_apply_normalized_account_state`.

Consequences:
- `POST .../revoke` sets `status=REVOKED, revoked_at, entitlement_version++` — on the
  tester's **next authenticated request** the resolver finds no ACTIVE membership, so
  the credit exemption, `is_beta_partner`, beta plan display, and
  `/api/beta/dashboard-summary` access all disappear. No restart, no re-login, no
  session invalidation, no cookie change.
- Session docs, user doc, wallet, ledger, analyses, confirmations, feedback are all
  untouched by revocation. Account keeps working as a normal customer on the real
  purchased balance.
- No caching layer is introduced (matches the existing per-request `db.users` read;
  one extra indexed point-read is negligible). If load ever demands it, a TTL cache
  would need explicit invalidation on status change — out of scope now.

## J. Effective-entitlement resolution order

Resolved once per request in `get_current_user`, consumed synchronously downstream.
Exact order (documented in code next to the resolver):

1. **Admin/owner** — `_is_admin_email(email)` (`server.py:629-633`;
   MASTER_ADMIN_EMAIL or ADMIN_EMAILS). Wins over everything: enterprise plan,
   admin exemption (`server.py:12143`), admin UI. An admin email is never treated as
   a beta tester even if a membership row exists (the add API also refuses to create
   memberships for admin emails, 400 `OWNER_CANNOT_BE_TESTER`).
2. **Active beta membership** — `beta_program_memberships` doc with
   `normalized_email == user email` AND `status == "ACTIVE"`. Grants exactly:
   credit exemption on upload block + debit, `is_beta_partner=true`,
   `beta_partner_name = display_name`, `beta_partner_type = partner_type`,
   beta plan display, `/beta/dashboard` + `/api/beta/dashboard-summary`. Grants
   **nothing else**: no `is_master_admin`, no `correctness_v2_admin_view`
   (still exact-owner only, `server.py:640-642`), no admin routes, no
   assistant/forensics (`feature_access` stays admin-only, `server.py:1061-1064`),
   ownership checks on analyses unchanged (`correctness_v2/api.py:425-430`).
3. **Normal customer** — plan/wallet from the users doc via the existing
   `_normalize_account_state` logic (`server.py:1034-1083`), real credit checks.
4. **Environment allowlist `BETA_UNLIMITED_EMAILS`: consulted by NOTHING at runtime.**
   `_is_beta_unlimited_email` is deleted from all request paths; the env parse at
   `server.py:113-115` is kept only as a migration input (read by
   `beta_program/migrate.py`), and prod keeps it empty.

Concrete refactor:
- `User` model (`server.py:228-243`) gains `beta_program: Dict[str, Any] = {}`
  (resolved snapshot: `{"active": bool, "membership_id", "display_name",
  "partner_type", "activated_at", "entitlement_version"}`).
- `_normalize_account_state` (`server.py:1034`) derives
  `is_beta_partner/beta_partner_name/beta_partner_type` from
  `user_doc.get("beta_program")` instead of env (replaces lines 1060, 1066-1070).
  The `beta_program` snapshot is request-scoped only — never persisted into `db.users`
  (`_apply_normalized_account_state`'s persist block is not extended).
- `_is_credit_exempt_user` (`server.py:650-654`) becomes
  `_user_is_admin(user) or bool(user.beta_program.get("active"))`.
- `_apply_perizia_credit_debit_with_ledger` beta short-circuit (`server.py:12145`)
  checks `user.beta_program.get("active")` (log line kept, plus membership_id).
- `analyze_perizia` beta log/block (`server.py:16232,16240`) same substitution.
- `require_beta_or_admin` (`server.py:1217-1224`) checks
  `user.beta_program.get("active")`.
- Feedback stamping (`server.py:20360-20371`): `user_role="beta_partner"` and
  `beta_partner_name/type` come from the membership snapshot; non-members keep
  `user_role="user"` exactly as today.
- `_build_user_response` (`server.py:1116-1137`) additionally exposes
  `beta_program: {active, display_name, member_since}` for the frontend.

## K. Purchased-credit preservation

Invariants (each becomes a test):
- **Activate writes nothing to `users`**: no plan, quota, `perizia_credits`, or
  `subscription_state` mutation on add/activate/reactivate (§H). The wallet
  normalizer is untouched, so `pack_grants` (Stripe purchases,
  `server.py:924-940`) and `monthly_remaining` survive verbatim.
- **Beta runs debit nothing**: exemption short-circuits **before** any wallet math
  (`server.py:12143-12151`), so `total_available` and the ledger are untouched
  during the beta period — and therefore there is nothing to "claw back" on
  revocation. No code path may ever write a compensating debit for beta-period
  runs (test asserts ledger count unchanged across activate→run→revoke).
- **Revoke writes nothing to `users`** — only the membership doc changes (§I).
- **No placeholder balances**: the feature never writes 9999/∞ or any synthetic
  number into `quota`/`perizia_credits`. Unlimited-ness is expressed solely as the
  authorization exemption plus UI copy (§N).
- Subscription state: a tester on a paid plan keeps `subscription_state`
  (`server.py:695-721`) untouched throughout; Stripe is never called by any
  beta-program endpoint (none of them import or touch checkout/webhook code).

## L. Historical beta-period identification

Two mechanisms, both deterministic:

1. **Forward-looking stamp (chosen, additive)** — `analyze_perizia` stamps a new
   non-billing field on the analysis record at creation
   (`analysis_dict`, `server.py:16659-16675`):
   `entitlement_context: "OWNER" | "BETA" | "PAID" | "FREE"` —
   OWNER when `_user_is_admin(user)`; BETA when `user.beta_program.active`; PAID when
   the debit ledger entry was written against a paid plan/pack; FREE otherwise
   (free-quota debit). Pure metadata: no billing logic reads it, ever; sanitizers
   don't expose it to customers (it lives beside `user_id`, which customer responses
   already omit).
2. **Backward window join** — pre-existing runs have no stamp; a beta-period run is
   identified deterministically as: `perizia_analyses.user_id == membership.user_id`
   AND `created_at` within `[activated_at, revoked_at or now)` — computable per
   tester with the existing `user_id`/`created_at` indexes (`server.py:20848-20849`).
   Cross-check: absence of a `credit_ledger` row with
   `entry_type="perizia_upload", reference_id=analysis_id` (exempt runs skip the
   ledger, `server.py:12145-12151`; `reference_id` is indexed, `server.py:20866`).

Historical beta activity survives revocation by definition: analyses, confirmations,
feedback rows, and the membership doc itself (with its timestamps) are never deleted.

## M. Admin API design

All routes owner-only via the existing exact-owner dependency
`require_exact_owner_admin` (`server.py:1209-1214`, backed by
`_is_exact_owner_admin_email`, 1194-1206 — master-admin email or the
CORRECTNESS_V2_ADMIN_VIEW email, both resolving to the owner; a hypothetical
non-owner `ADMIN_EMAILS` member is rejected 403). Every mutation writes
`beta_program_audit` and, additionally, the generic `_write_admin_audit`
(`server.py:12361-12381`) with action prefix `BETA_PROGRAM_*` so the existing admin
audit stream stays complete.

Router prefix `/api/admin/beta-program` (mounted like correctness_v2):

| Method | Path | Purpose |
|---|---|---|
| GET | `/overview` | Panoramica payload (§Q). Read-only. |
| GET | `/testers` | List; query params `status` (PENDING/ACTIVE/REVOKED), `q` (regex-escaped substring on email/display_name), `page`, `page_size` (default 25, max 100). Returns `{items, total, page, page_size}` — same pagination contract as `admin_list_beta_feedback` (`server.py:20464-20491`). Each row includes deterministic per-tester counters (analyses_total, unreadable_total, feedback_total — three indexed `count_documents` per page row, bounded by page_size). |
| POST | `/testers` | Add (§G/§H). 409 on duplicate/revoked, 400 on admin email or invalid email. |
| GET | `/testers/{membership_id}` | Detail: membership + audit tail + per-tester signal counters (§P) + feedback summary. |
| PATCH | `/testers/{membership_id}` | Edit `display_name` / `internal_note` / `partner_type` ONLY (status is never patchable here). Audit `MEMBER_NOTE_UPDATED`. |
| POST | `/testers/{membership_id}/revoke` | ACTIVE/PENDING → REVOKED. Idempotent (revoking REVOKED → 409). |
| POST | `/testers/{membership_id}/reactivate` | REVOKED → ACTIVE (requires existing user_id; else back to PENDING). Sets `reactivated_at`. The ONLY reactivation path. |
| GET | `/audit` | Append-only log; filters `membership_id`, `action`, `date_from/to`; paginated. |
| GET | `/feedback` | Tester feedback list — same filters/metrics as today's `admin_list_beta_feedback`; handler body extracted into a shared helper and reused. |
| PATCH | `/feedback/{feedback_id}` | Status/notes update — reuses today's `admin_update_beta_feedback` body (`server.py:20522-20545`). |
| GET | `/feedback/export` | CSV/JSONL export — reuses `admin_export_beta_feedback` body (`server.py:20603-20657`). |
| GET | `/signals` | Operational signals (§P); filters `user_id`/`membership_id`, `signal`, date range; paginated. |

Route move, no duplication: the legacy `GET/PATCH /api/admin/beta-feedback*` routes
(`server.py:20451,20522,20603`) are **deleted in the same commit**; their only
consumer is `AdminBetaFeedback.js`, which this feature replaces. Their handler logic
moves into shared functions called by the new owner-gated routes (net effect: the
feedback admin surface is tightened from `require_master_admin` to exact-owner).
`POST /api/beta-feedback` and `GET /api/beta-feedback/my` (tester-facing) are
unchanged.

Read endpoints perform Mongo reads only — statically verifiable: no
`openai_chat_completion`, no `correctness_v2` job spawn, no wallet/ledger write, no
Stripe import.

## N. Admin frontend design

- **Sidebar**: `Dashboard.js:69` entry `{label: 'Beta Feedback', path:
  '/admin/beta-feedback'}` is **renamed in place** to `{label: 'Programma Beta',
  path: '/admin/beta-program'}` — same array position, single entry, no duplicate.
  Its visibility (and only its visibility) additionally requires the exact-owner
  flag already shipped to the client: `user.correctness_v2_admin_view === true`
  (`server.py:1134-1136`). Other admin items keep `is_master_admin` gating
  (`Dashboard.js:125-130`). Backend authorization remains authoritative regardless.
- **Routing** (`App.js`): `/admin/beta-feedback` route (250-252) is replaced by
  `/admin/beta-program` wrapped in `AdminRoute`; a `<Navigate to="/admin/beta-program"
  replace />` is registered on the old path so bookmarks don't 404 (redirect, not a
  second page).
- **New page** `frontend/src/pages/admin/AdminBetaProgram.js` using `AdminLayout`
  (22-line shell) with four tabs (shadcn `Tabs`, already in `components/ui`):
  1. **Panoramica** — metric cards (`MetricCard` pattern from
     `AdminBetaFeedback.js:51-56`) fed by `GET /overview`; tester status breakdown,
     feedback totals, signal counters, last-activity timestamps. Pure display.
  2. **Tester** — add form (email, display name, type, note), status filter +
     search, paginated table (pattern `AdminUsers.js`), row actions
     Revoca/Riattiva/Modifica with confirmation dialogs, per-row status badge and
     entitlement history drawer (audit tail).
  3. **Feedback** — the current `AdminBetaFeedback.js` UI (filters, metrics row,
     table, status editor, export buttons) ported into a tab component
     `admin/betaProgram/FeedbackTab.js`, calling the new
     `/admin/beta-program/feedback*` endpoints. Verbatim `expert_comment` shown
     read-only; owner writes only status/admin_notes.
  4. **Segnali** — table of operational signals (§P) with tester + signal-type +
     date filters; strictly counters/statuses, never document content or party names.
- Mobile: `AdminLayout`/sidebar Sheet behavior is inherited unchanged
  (`Dashboard.js:114-131` compact rendering); tabs stack per the existing responsive
  utilities. Axios with `withCredentials: true` and `API_URL =
  process.env.REACT_APP_BACKEND_URL` matches every existing admin page.
- **Tester-facing display** (normal customer interface, Report cliente only):
  - `featureAccess.js`: `getAccountState` gains
    `betaProgram: {active, displayName, memberSince}` from `user.beta_program`; when
    active, `planLabel` renders `'Programma Beta'` (labels map at
    `featureAccess.js:7-14` untouched — override applied after lookup).
  - Sidebar credit box (`Dashboard.js:147-162`): when `betaProgram.active`, show
    `Analisi illimitate — Programma Beta` in place of the numeric count; the real
    purchased number remains visible in Billing. **Never a fake number.**
  - Billing.js (active tester, owner-decided): HIDE "Acquista pacchetto",
    "Ricarica" and all new-credit purchase prompts; show the banner *"Programma
    Beta attivo — le analisi non consumano crediti. I crediti già acquistati
    restano salvati e saranno nuovamente utilizzabili al termine dell'accesso
    beta."*; display the REAL purchased balance under the label "Crediti
    preservati" (never a fake unlimited number); KEEP subscription-management
    controls ("Gestisci abbonamento", cancellation, payment method) available for
    users on a paid subscription so no one is trapped. On revoke everything reverts
    to normal billing automatically (conditional on `betaProgram.active`). PENDING
    and REVOKED testers see the normal billing page. No Stripe/checkout/webhook
    change — purely a frontend conditional.
  - `/beta/dashboard` continues to work for active testers via `BetaRoute`
    (`App.js:91-115`) driven by the DB-resolved `is_beta_partner`.

## O. Feedback-management design

- Storage, submission (`POST /api/beta-feedback`), and tester self-view
  (`/beta-feedback/my`) are unchanged; `expert_comment`/`expected_correction` remain
  verbatim (sanitization only strips control chars, `server.py:20194-20206`).
- Owner management moves to the Feedback tab (§M/§N): list with the existing 10
  server-side filters, deterministic metrics (`server.py:20494-20519` — switched from
  `find({}).to_list(None)` at 20481-20484 to an aggregation `$group` so the metrics
  never load every document), status transitions within `BETA_FEEDBACK_STATUSES`,
  `admin_notes`, CSV/JSONL export.
- **Three-way separation surfaced in the UI**:
  1. *Explicit tester feedback* = `beta_feedback` rows with `user_role="beta_partner"`
     (verbatim, Feedback tab).
  2. *Operational signals* = derived counters/statuses (§P, Segnali tab) — clearly
     labeled "derivato dal sistema, non dichiarato dal tester".
  3. *Owner interpretation* = `status` + `admin_notes` on feedback rows and
     `internal_note` on memberships — always displayed as owner commentary, never
     merged into the tester text.
- No classification model anywhere: `learning_label` stays the deterministic map
  (`server.py:20127-20191`); the dashboard performs zero LLM calls.

## P. Operational-signal design

### P.0 `v2_job_events` telemetry contract (owner-mandated, RESOLVED)

The mirror is **telemetry only**. Hard invariants:
- It must never alter pipeline output, validation, billing, credits, report state,
  or customer visibility. No pipeline code reads it back.
- Events are emitted **only after the authoritative job/status write succeeds**
  (i.e. after `job_status.json` is persisted). `job_status.json` remains
  authoritative; Mongo is only the queryable operational mirror.
- A Mongo event-write failure is logged and swallowed — it must never fail or
  delay the analysis. **Swallowing exceptions is not sufficient**: a slow or
  unreachable Mongo would still block the pipeline thread. The emit therefore
  performs **no I/O at all on the caller's thread** — it builds the event dict
  and does a non-blocking `put_nowait` onto a **bounded** queue
  (`V2_TELEMETRY_QUEUE_MAX`, default 2000). Exactly **one** bounded daemon
  worker thread drains that queue and writes to Mongo, using a sync pymongo
  client with short bounded connect/serverSelection/socket timeouts. When the
  queue is full the event is **dropped and counted** (throttled warning) rather
  than blocking — losing telemetry is always preferable to delaying an analysis.
  No thread-per-event, no unbounded executor. Service shutdown attempts a short
  bounded flush that can never hang shutdown. The `user_id` backfill lookup is
  resolved **inside the worker**, never on the pipeline thread, because it is
  itself a Mongo read.
- One small shared helper: `emit_v2_job_event(...)` in
  `backend/beta_program/signals.py` (imported lazily by the pipeline to avoid a
  hard dependency cycle). Signature:
  `emit_v2_job_event(event_type, *, job_id, analysis_id, lot_id=None, user_id,
  status=None, reason_code=None, duration_seconds=None) -> None` (synchronous,
  non-blocking, never raises). Companions: `flush(timeout)` (tests + offline
  backfill only), `shutdown(timeout)`, `telemetry_stats()`.
- **Idempotent**: deterministic `event_id = "v2ev_" + sha1(f"{job_id}|{lot_id}|
  {event_type}")[:24]` with a UNIQUE index on `event_id`; the insert uses
  `update_one(..., upsert=True)` (or catches DuplicateKeyError) so retries and
  restarts cannot duplicate a metric.
- **Safe metadata only** — stored fields are exactly:
  `event_id, event_type, job_id, analysis_id, lot_id, user_id, status,
  reason_code, duration_seconds, created_at`. Never PDF text, prompts, excerpts,
  perizia party names, tokens, or secrets.
- Indexes: unique `event_id`; `(user_id, created_at)`; `(analysis_id, created_at)`;
  `(event_type, created_at)`.
- Event vocabulary (emitted only where genuinely observed): `REPORT_READY`,
  `VERIFICATION_REQUIRED`, `SERVICE_BUSY`, `SERVICE_UNAVAILABLE`,
  `DOCUMENT_NOT_READABLE`, `LOT_REPORT_REUSED`, `LOT_JOB_DEDUPLICATED`,
  `LOT_RERUN_FORCED`, `FAILED_RERUN_SAFE_REPORT_PRESERVED`, `CONFIRMATION_REQUIRED`,
  `CONFIRMATION_COMPLETED`.
- **No historical full-filesystem scan at request time.** An optional one-shot
  offline `--backfill-job-events` (§R) may mirror clearly-terminal existing jobs;
  it must not backfill uncertain/ambiguous old state as fact (skip anything whose
  terminal status/reason is not unambiguous).
- The two hooks touch only `orchestrator.py` / `correctness_v2/api.py` additively;
  regression tests assert six-case reports and pipeline output are byte-for-byte
  unchanged with the mirror enabled vs a no-op emitter.

Every signal maps to a deterministic, indexed source; none reads raw document text,
none exposes counterparties, none runs an LLM, none scans the artifacts tree at
request time.

| Signal | Deterministic source |
|---|---|
| Analyses uploaded / completed / UNREADABLE | `perizia_analyses.count_documents({user_id, [status]})` — `status` set at `server.py:16664` (`COMPLETED` / `UNREADABLE`); indexes `user_id`,`created_at` (`server.py:20848-20849`); add compound `(user_id, status)`. |
| Lot count & multi-lot uploads | `pages_count` + lot metadata already summarized on the analysis record / v2 job event (below); never from artifact JSON at request time. |
| Report outcomes: REPORT_READY, LOT_SELECTION_REQUIRED, MONEY_CONFIRMATION_REQUIRED, NEEDS_MANUAL_REVIEW, FAILED_*, JOB_STALLED, CANCELLED | **New append-only mirror `v2_job_events`**: `orchestrator` writes one Mongo doc per terminal transition `{event_id, job_id, analysis_id, user_id, status, reason_code, lot_id, pages_count, created_at, finished_at, duration_seconds}` (statuses from `correctness_v2/schemas.py:26-117`). Today these exist only in per-job `job_status.json` files, and reading them means walking every job dir (`artifacts.latest_job_for_analysis`, `artifacts.py:242-259`) — forbidden per request. Fire-and-forget insert; failure never breaks the job. Indexes `(user_id, created_at)`, `(analysis_id)`, `(status)`. |
| Customer-visible degradations: SERVICE_BUSY / VERIFICATION_REQUIRED / SERVICE_UNAVAILABLE / PREPARING | Derived at query time from `v2_job_events.status` with the same mapping the API already uses (`correctness_v2/api.py:84-142`) — no new taxonomy. |
| Processing time | `v2_job_events.duration_seconds` (job payload `created_at`→`updated_at`, `job_status.py:53-70`). |
| Report reuse / dedup / forced rerun | Event rows `LOT_REPORT_REUSED` / `LOT_JOB_DEDUPLICATED` / `LOT_RERUN_FORCED` inserted at the decision points in `correctness_v2_generate_lot` (`api.py:803-838`). |
| Confirmations required / completed | `correctness_v2_confirmations` (state) + `correctness_v2_confirmation_audit` counts by `user_id` (`user_confirmations.py:33-37`, unique+owner indexes at 60-84); money confirmations via `perizia_confirmations` count (`server.py:17050`). |
| Feedback volume / priority / unresolved | `beta_feedback` counts (existing indexes, `server.py:20876-20882`). |
| Credit-free beta runs | §L: `entitlement_context="BETA"` count; historical fallback = activation-window join + missing `perizia_upload` ledger row (`credit_ledger.reference_id` indexed, `server.py:20866`). |

Historical jobs predating the mirror: one-shot **offline** backfill inside the
migration script (§R) walks `_correctness_v2/jobs/*/job_status.json` once and inserts
events (idempotent on `job_id` unique index). Request-time code never touches the
filesystem.

## Q. Dashboard metric definitions

`GET /api/admin/beta-program/overview` returns (every value a `count_documents` /
small `$group` aggregation; definitions frozen here):

- `testers.active|pending|revoked` — membership counts by status.
- `testers.last_activation_at` / `last_revocation_at` — max of the respective fields.
- `analyses.beta_total` — analyses with `entitlement_context="BETA"` plus (until
  backfill ages out) window-join count for active/revoked memberships (§L).
- `analyses.last_30d` — same, `created_at >= now-30d` (date filter pattern:
  `_date_range_query`, `server.py:12340-12359`).
- `analyses.unreadable_total` — `status="UNREADABLE"` among beta testers' user_ids.
- `reports.ready_total` / `reports.failed_total` / `reports.confirmation_required_total`
  — `v2_job_events` grouped by status for tester user_ids.
- `reports.avg_duration_seconds` — `$avg` over `v2_job_events.duration_seconds`
  (REPORT_READY only).
- `confirmations.open|resolved` — `correctness_v2_confirmations` by state for tester
  user_ids.
- `feedback.total|new|accepted|high_priority|top_error_category|top_problematic_section`
  — existing `_compute_beta_feedback_metrics` semantics (`server.py:20494-20519`)
  restricted to `user_role="beta_partner"`, via aggregation.
- `credits.beta_exempt_runs_total` — §L definition. Explicitly **not** a money value;
  no Stripe/wallet reads beyond `count_documents`.

Determinism contract (tested): serving `/overview`, `/testers`, `/signals`,
`/feedback` performs zero OpenAI calls, spawns zero jobs (artifacts dir mtime/count
unchanged), writes zero ledger/wallet/user changes, performs zero Stripe calls
(module-level: `beta_program/` never imports checkout code). Identical DB state ⇒
identical response.

## R. Migration from hardcoded/env allowlists

Script: `backend/scripts/beta_program_migrate.py` (CLI wrapper over
`beta_program/migrate.py`), run manually once per environment. Modes: `--dry-run`
(default) and `--apply`; optional `--email x --name y` pairs for manual imports
(marked `migration_source="manual_admin"`).

Rules (all idempotent; safe to run repeatedly):
1. Inputs: `BETA_UNLIMITED_EMAILS` env (parse identical to `server.py:94-115`) +
   `BETA_PARTNER_NAMES` (in-source map, currently `{}`, `server.py:120`) + explicit
   `--email` args. **Prod env is empty (verified), so the prod run is a verified
   no-op** — that is the desired end-state proof, printed in the report.
2. For each normalized email:
   - existing membership `REVOKED` → **skip, never override** (`skipped_revoked`;
     audit `MIGRATION_SKIPPED`). This is a hard rule of the bootstrap.
   - existing membership PENDING/ACTIVE → `skipped_existing` (no field is touched).
   - admin email (`_is_admin_email`) → `skipped_admin`.
   - otherwise create per §G/§H (ACTIVE+linked if a `users` doc exists, else
     PENDING); `migration_source="env_allowlist"`; audit `MIGRATION_IMPORTED
     (actor_type=MIGRATION)`.
   - never creates/edits `users` docs; never touches wallets or feedback.
3. Output: explicit JSON report `{dry_run, migrated: [], skipped_existing: [],
   skipped_revoked: [], skipped_admin: [], total}` printed to stdout.
4. `--backfill-job-events` flag runs the one-shot `v2_job_events` backfill (§P),
   idempotent via unique `job_id` index.
5. After migration is live, the env var stays supported **only** as migration input;
   runtime code paths no longer reference it (§J.4), so a stale value can never
   grant privilege. `.env` itself is not modified by this feature.

## S. Riccardo preservation / non-reactivation plan

Facts (verified live): `geomazzantiriccardo@gmail.com` has **no `users` document**,
no session, appears in no allowlist (`BETA_UNLIMITED_EMAILS=''`,
`BETA_PARTNER_NAMES={}` — `server.py:120`), and owns exactly two `beta_feedback`
docs: `betafb_6150bea873bc4ce3`, `betafb_d4b88e3e8d8c4300`.

Guarantees:
1. **No hardcoding**: his (or any tester's) email appears nowhere in source, tests,
   or fixtures — tests keep the synthetic-identity convention already documented in
   `backend/tests/test_beta_feedback.py:13-16` (`beta.partner@example.test`).
2. **No reactivation vector**: (a) migration input is empty in prod; (b) login
   auto-activation matches only `status=PENDING` (§G.2) and he has no membership doc
   at all; (c) creating one requires an explicit owner API call, which is audited;
   (d) bootstrap never overrides REVOKED (§R.2).
3. **Feedback preserved verbatim**: no code path in this feature updates
   `beta_feedback` except the owner status/notes PATCH; migration and entitlement
   code never touch the collection. Live validation G (§W) asserts his two docs are
   byte-identical before/after the full migration + entitlement test cycle.
4. **If he returns as a normal customer**: first login creates a fresh free-plan user
   (`server.py:12476-12496`); with no ACTIVE membership he resolves as a normal
   customer (§J.3) — normal plan, normal credits, no beta UI; his historical feedback
   remains attributed to his email.

## T. Authorization matrix

| Capability | Owner (exact) | Non-owner admin* | Active tester | Revoked tester | Normal customer | Unauthenticated |
|---|---|---|---|---|---|---|
| `/api/admin/beta-program/*` (all) | 200 | **403** (`require_exact_owner_admin`, `server.py:1209-1214`) | 403 | 403 | 403 | 401 |
| Sidebar "Programma Beta" visible | yes (`correctness_v2_admin_view`) | no | no | no | no | n/a |
| Other `/api/admin/*` | 200 | 200 (`require_master_admin`, 1186-1191) | 403 | 403 | 403 | 401 |
| Upload without sufficient credits | allowed (admin exemption) | allowed | **allowed** (beta exemption) | blocked 403 `INSUFFICIENT_PERIZIA_CREDITS` (`server.py:16240-16257`) | blocked 403 | 401 |
| Credit debit on upload | skipped | skipped | **skipped, no ledger row** | debited + ledger | debited + ledger | n/a |
| `/api/beta/dashboard-summary` | 200 | 200 (admin arm of `require_beta_or_admin`) | 200 | 403 | 403 | 401 |
| `POST /api/beta-feedback` | 200 (role=admin) | 200 (role=admin) | 200 (role=beta_partner) | 200 (role=user) | 200 (role=user) | 401 |
| Own analyses / Report cliente | own+any (admin) | own+any | **own only** (`correctness_v2/api.py:425-430`) | own only | own only | 401 |
| Vista admin (v2 debug tab) | yes (exact-owner flag, `server.py:640-642`) | no | **no** | no | no | no |
| Internal pipeline data / artifacts routes | yes (admin-only guard, `correctness_v2/api.py:206-237`) | yes | **no** | no | no | no |

\* Non-owner admins don't exist in prod today (`ADMIN_EMAILS` defaults to the owner,
`server.py:103-105`), but the matrix is enforced in code, not by configuration luck.

## U. Exact files expected to change

Backend — new:
- `backend/beta_program/__init__.py`
- `backend/beta_program/store.py` — schema constants, `normalize_beta_email`,
  `get_active_membership(email)`, `link_pending_membership(email, user_id)`,
  status-transition functions (each writes audit + bumps `entitlement_version`),
  `ensure_indexes()`.
- `backend/beta_program/api.py` — owner-only router (§M).
- `backend/beta_program/signals.py` — overview/signals aggregations (§P/§Q).
- `backend/beta_program/migrate.py` + `backend/scripts/beta_program_migrate.py` (§R).

Backend — modified:
- `backend/server.py` —
  `get_current_user` membership resolution (`~1162-1169`); `User.beta_program` field
  (`228-243`); `_normalize_account_state` beta fields from snapshot
  (`1060,1066-1070`); `_is_credit_exempt_user` (`650-654`);
  `_apply_perizia_credit_debit_with_ledger` (`12145`); `require_beta_or_admin`
  (`1217-1224`); `_get_or_create_authenticated_user` pending-activation hook
  (`~12472,12495`); `analyze_perizia` exemption check + `entitlement_context` stamp
  (`16232-16240`, `16659-16675`); feedback stamping (`20360-20371`); legacy
  `/admin/beta-feedback*` route removal with handler bodies extracted for reuse
  (`20451-20657`); `ensure_indexes` additions (`20844-20883`); beta_program router
  mount (beside `20748-20755`). `_is_beta_unlimited_email` deleted;
  `BETA_UNLIMITED_EMAILS` parse retained solely for `migrate.py` import.
- `backend/correctness_v2/orchestrator.py` — terminal-status `v2_job_events` insert
  (fire-and-forget helper).
- `backend/correctness_v2/api.py` — reuse/dedup/forced-rerun event inserts in
  `correctness_v2_generate_lot` (`803-838`).

Frontend — new:
- `frontend/src/pages/admin/AdminBetaProgram.js` (+ tab components under
  `frontend/src/pages/admin/betaProgram/`: `OverviewTab.js`, `TestersTab.js`,
  `FeedbackTab.js` (ported from AdminBetaFeedback), `SignalsTab.js`).

Frontend — modified:
- `frontend/src/pages/Dashboard.js` — sidebar rename + owner-only visibility (`69`,
  `125-130`); beta credit-box copy (`147-162`).
- `frontend/src/App.js` — route swap + redirect (`250-252`).
- `frontend/src/lib/featureAccess.js` — `betaProgram` state + plan label (`25-64`).
- `frontend/src/pages/Billing.js` — beta banner (display only).
- `frontend/src/pages/admin/AdminBetaFeedback.js` — **deleted** (absorbed).

Tests — new/modified (see §V):
- `backend/tests/test_beta_program_entitlement.py`,
  `backend/tests/test_beta_program_admin_api.py`,
  `backend/tests/test_beta_program_migration.py`,
  `backend/tests/test_beta_program_signals.py` (new);
  `backend/tests/test_beta_feedback.py` (route move + role stamping),
  `backend/tests/test_admin.py` (matrix additions).
- `frontend/src/pages/admin/AdminBetaProgram.test.js` (new),
  `frontend/src/lib/featureAccess.test.js` (new),
  `frontend/src/pages/Dashboard.sidebar.test.js` (new or folded into existing
  render tests).

Docs:
- `docs/beta_program_admin_plan.md` (this file).

Not touched: `.env`, Stripe/checkout/webhook code, credit formulas/bands, OpenAI
client/model code, extraction/lot pipeline behavior, legacy report gating,
`user_confirmations` store, deployment units.

## V. Tests (46 backend + 30 frontend)

Backend (pytest, `FakeCollection` async-Mongo fake per
`backend/tests/test_beta_feedback.py:20-60`, synthetic emails only) — 46:

1. Entitlement resolution (12): active membership ⇒ `is_beta_partner`/exemption;
   pending ⇒ none; revoked ⇒ none; no membership ⇒ none; admin email wins over
   membership; env var populated ⇒ still **no** runtime grant (the anti-regression
   test); normalization (case/whitespace) match; `beta_program` snapshot never
   persisted to `users`; `User` model defaults; `_build_user_response` fields;
   `require_beta_or_admin` allow/deny; `beta/dashboard-summary` role block.
2. Credit behavior (8): upload allowed at 0 credits for active tester; debit skipped
   + zero ledger rows; revoked tester blocked with exact 403 payload
   (`INSUFFICIENT_PERIZIA_CREDITS`); purchased `pack_grants` unchanged across
   activate→run→revoke; no 9999/placeholder writes anywhere (assert wallet
   invariants); paid-plan tester keeps `subscription_state`; `entitlement_context`
   stamped OWNER/BETA/PAID/FREE (4 cases in 2 tests); lot generate still charges 0.
3. Admin API (12): owner 200 on each route; non-owner-admin 403; tester/customer
   403; unauthenticated 401; add-pending; add-existing-active; duplicate 409;
   revoked-re-add 409; revoke; reactivate; PATCH restricted fields; pagination +
   filtering + `q` regex-escape.
4. Activation/audit lifecycle (6): login auto-activates PENDING (links user_id);
   login never touches REVOKED; audit row per transition with correct
   before/after/version; `entitlement_version` monotonicity; audit append-only
   (no update path); admin-email membership refused.
5. Migration (5): dry-run writes nothing; apply idempotent (second run all-skips);
   REVOKED never overridden; existing user linked vs pending; empty-env no-op report.
6. Feedback + signals (3): moved feedback routes owner-gated and legacy paths gone
   (404); metrics via aggregation match fixture counts; signals/overview endpoints
   are read-only (no inserts outside reads, no OpenAI symbol reachable — assert via
   monkeypatched sentinel).

Frontend (jest/RTL, patterns from `AdminDecisionModelPreview.test.js` etc.) — 30:

1. Sidebar (5): "Programma Beta" shown for owner flag; hidden for non-owner admin;
   hidden for tester/customer; old "Beta Feedback" label absent; single entry.
2. Routing (3): `/admin/beta-program` renders for admin; old path redirects;
   non-admin bounced to /dashboard.
3. Tabs (10): four tabs render; Panoramica cards from mock overview; Tester table
   + add form validation; revoke confirm dialog fires POST; reactivate; status
   filter/search wire query params; pagination; Feedback tab filters + status
   editor (ported assertions); Segnali table rendering; error/empty states.
4. Account display (8): `getAccountState().betaProgram` mapping; plan label
   "Programma Beta" when active; label unchanged when inactive; credit box shows
   "Analisi illimitate" copy and never a fake number; Billing banner active/inactive;
   revoked user sees normal plan; `is_beta_partner` false ⇒ no Beta Partner nav;
   BetaDashboard nav present when active.
5. Safety (4): no admin tab/Vista admin for tester (visibility test pattern,
   `components/correctness-v2/visibility.test.js`); no OpenAI/job/fetch side
   effects on dashboard mount beyond the declared GETs (axios mock call-list
   assertion); verbatim feedback text rendered unescaped-but-safe; owner-note
   fields clearly separated from tester text.

## W. Live isolated real-Mongo validation (scenarios A–J)

Environment: temp DB name (`beta_program_validation_<ts>`) on the local mongod, temp
backend on a spare port with `CORRECTNESS_V2_ARTIFACTS_ROOT` pointed at a temp dir —
same pattern as the confirmed prior validations
(`docs/storico_lot_workspace_plan.md:305`,
`docs/customer_report_decision_workflow_plan.md` §T2.6). Prod `.env`, prod DB, prod
service untouched; DB dropped afterward.

- **A** Owner adds pending email → membership PENDING + audit row; overview counts it.
- **B** Simulated first login of that email → ACTIVE, `user_id` linked, upload at 0
  credits succeeds, zero `credit_ledger` rows, `entitlement_context="BETA"` stamped.
- **C** Owner adds an already-registered user → immediately ACTIVE; user doc
  byte-identical before/after (deep compare).
- **D** Tester `/auth/me`: `is_beta_partner=true`, plan label data present,
  `correctness_v2_admin_view=false`; `/api/admin/overview` → 403;
  `/api/admin/beta-program/testers` → 403.
- **E** Revoke → immediately-next request: exemption gone (upload with 0 credits →
  403 `INSUFFICIENT_PERIZIA_CREDITS`), same session token still authenticates, no
  restart performed.
- **F** Grant a pack via `PATCH /api/admin/users/{id}` (admin_adjustment), then
  activate→run→revoke: `pack_grants`/`total_available` unchanged by the beta cycle;
  ledger contains only the admin_adjustment entries.
- **G** Seed the two real Riccardo feedback docs (copied values) + empty env →
  migration dry-run and apply: report shows zero migrated; no membership exists for
  his email; both docs byte-identical after the whole suite.
- **H** Revoke then re-run migration/bootstrap and simulate login → stays REVOKED;
  explicit `POST .../reactivate` → ACTIVE with `reactivated_at`.
- **I** Determinism sweep: call every `/api/admin/beta-program/*` GET with an
  OpenAI-key-less env and an assertion wrapper — zero OpenAI attempts, artifacts
  jobs dir unchanged (file count+mtimes), `credit_ledger`/`users` unchanged, zero
  outbound Stripe (no network beyond Mongo), repeated calls byte-identical.
- **J** Audit + pagination: full lifecycle produces the exact expected audit
  sequence; `/testers` and `/audit` pagination/filtering verified; all new indexes
  exist (`index_information()` check: unique email, status, user_id, audit, job
  events).

## X. Deployment & rollback

- Deploy: merge `feature-beta-program-admin` → `main`; frontend auto-deploys on push
  (Vercel); backend requires one owner-initiated `systemctl restart
  periziascan-backend` (service confirmed: `/srv/perizia/app/backend`,
  EnvironmentFile `.env`, uvicorn on 127.0.0.1:8081) — this is a **deploy** restart,
  after which all add/revoke operations are restart-free forever. Startup
  `ensure_indexes` creates the new indexes idempotently. Run
  `beta_program_migrate.py --dry-run` then `--apply` once (prod: verified no-op) and
  optionally `--backfill-job-events`. No `.env` edit; `BETA_UNLIMITED_EMAILS` stays
  empty.
- Rollback: `git revert` the merge and restart. The new collections
  (`beta_program_memberships`, `beta_program_audit`, `v2_job_events`) are additive
  and become inert on rollback — old code never reads them; no user/wallet/feedback
  document was mutated, so no data rollback is needed. Frontend rolls back via
  Vercel redeploy of the previous commit.
- Smoke after deploy: owner loads `/admin/beta-program`; `/auth/me` for a normal
  customer unchanged; upload+debit path regression (one paid test analysis) intact.

## Y. Deferred privacy/training-consent work

Explicitly out of scope here, tracked for a follow-up: governance of
`permission_for_learning` (it is stored, `server.py:20393`, but no enforcement
pipeline exists); GDPR export/erasure workflow for `beta_feedback` and membership
data; tester agreement/consent capture at activation; retention policy for
`beta_program_audit` and `v2_job_events`; anonymized-export mode for feedback.

## Z. Scope exclusions

Unchanged by this branch: Stripe/checkout/webhooks/pricing/packages; credit bands
and debit formulas; wallet normalization; OpenAI/Gemini model choice and pipeline
behavior (analyst, lots, validator, narrator); legacy report gating
(`require_exact_owner_admin` PDF routes); per-lot concurrency settings; user roles
beyond beta; GDPR/admin user management; `.env`; deployment topology; the owner's
enterprise-plan 9999 display (pre-existing, admin-only, not a beta artifact).

---

## Plan self-review

- **Entitlement, not wallet** — beta is resolved per request from
  `beta_program_memberships` and expressed only as authorization
  (`_is_credit_exempt_user`) + display flags; no code path writes credits (§J, §K).
- **No 9999** — the only 9999 in the system is the pre-existing enterprise plan for
  the owner (`server.py:561,570`); this feature adds no numeric placeholder anywhere
  and tests assert it (§V.2).
- **DB is the runtime source of truth** — `BETA_UNLIMITED_EMAILS` is referenced only
  by the migration; an anti-regression test proves a populated env grants nothing
  (§J.4, §V.1).
- **Session-refresh correctness** — `get_current_user` re-resolves membership every
  request; sessions carry no entitlement claims, so activate/revoke apply on the
  next authenticated request with no restart/re-login (§I, validated in W.E).
- **Purchased-credit preservation** — activate/revoke touch only the membership doc;
  exemption short-circuits before wallet math; no retroactive billing path exists
  (§K, W.F).
- **Riccardo** — no users doc, no membership, empty env ⇒ no reactivation vector;
  login-activation matches PENDING only; his two feedback docs
  (`betafb_6150bea873bc4ce3`, `betafb_d4b88e3e8d8c4300`) are never written by this
  feature and are byte-compared in W.G; no tester email in source (§S).
- **Owner-only authorization** — all `/api/admin/beta-program/*` routes use
  `require_exact_owner_admin`; sidebar visibility uses the exact-owner flag; the
  matrix (§T) covers all six actor classes including non-owner admins.
- **Deterministic dashboard** — every metric/signal maps to indexed Mongo counts or
  the new append-only `v2_job_events` mirror; zero OpenAI/jobs/credit/Stripe
  verified statically and in W.I; no request-time artifact-tree scans (§P, §Q).
- **Scope guard** — no Stripe, pipeline, pricing, `.env`, or concurrency changes
  (§Z); collections additive; rollback inert.

**PRODUCT decisions — RESOLVED by owner (2026-07-19):**

1. **Billing during ACTIVE beta — HIDE purchase/recharge CTAs** (owner chose
   option 2). While a membership is ACTIVE the Billing page must:
   - hide "Acquista pacchetto", "Ricarica", and any other new-credit purchase
     prompt;
   - show the banner (verbatim): *"Programma Beta attivo — le analisi non
     consumano crediti. I crediti già acquistati restano salvati e saranno
     nuovamente utilizzabili al termine dell'accesso beta."*;
   - continue displaying the REAL purchased-credit balance under the label
     **"Crediti preservati"** (never an artificial unlimited number);
   - keep subscription-management controls available if the user has an active
     paid subscription — "Gestisci abbonamento", cancellation, payment-method
     management — so a tester is never trapped in a paid subscription during beta;
   - on REVOCATION: banner disappears, purchase/recharge CTAs return immediately,
     the saved balance becomes active again, normal plan rules apply;
   - PENDING and REVOKED testers see the normal billing page;
   - no Stripe products/prices/webhooks/subscriptions are modified or cancelled by
     activating or revoking beta; normal credit calculations unchanged.
   Frontend-only conditional on `betaProgram.active`; backend/Stripe untouched.
   Tests: (1) ACTIVE hides purchase/recharge CTAs; (2) ACTIVE preserves+displays
   real balance as "Crediti preservati"; (3) subscription management still
   available under ACTIVE with a paid sub; (4) REVOKED immediately restores normal
   billing CTAs; (5) no Stripe op occurs merely from activate/revoke.

2. **Segnali — INCLUDE the `v2_job_events` telemetry mirror** (owner chose option
   1) under a strict telemetry-only contract (see §P, revised). The mirror is
   observability only and must never alter pipeline output, validation, billing,
   credits, report state, or customer visibility. Six-case + pipeline byte-for-byte
   invariance is asserted in tests.
