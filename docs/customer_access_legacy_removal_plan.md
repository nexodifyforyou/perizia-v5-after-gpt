# Customer Access + Legacy Frontend Removal — Implementation Plan

Plan date: 2026-07-15
Branch: `feature-customer-access-and-legacy-removal`
Base SHA: `c1827ea` (current production `main`)
Status: **PLAN ONLY — awaiting owner review. No code changes made. Working tree clean at base.**

Scope: two changes in one branch.

1. **Part 1 — Entitlement downgrade.** Remove all beta/unlimited/tester privileges from `geomazzantiriccardo@gmail.com`. He remains an **active normal user**.
2. **Part 2 — Permanent legacy frontend removal.** No legacy report surface renders, flashes, mounts, or is fetched anywhere in the customer-facing frontend.

Explicitly out of scope: visual redesign, the new admin panel, storico/lot-workspace, the V2 branded PDF/HTML export.

---

## 0. Owner Decisions Incorporated

Recorded 2026-07-15, superseding the original brief:

1. **No deactivation.** Riccardo stays an active normal user. Remove beta/unlimited/tester privileges only. If his normal wallet is empty he cannot run new analyses until he has credits — this is accepted and intended. Preserve login, history, existing reports.
2. **Fix `BETA_UNLIMITED_EMAILS` in both places:** remove him from the code default (prefer an **empty** default over a hardcoded privileged email) *and* from `backend/.env` if explicitly configured. Restart the backend and verify the **running process environment**.
3. **Do not delete the legacy print route yet** — the backend PDF renderer depends on it. Remove every customer-facing link/button/navigation to it, make the route inaccessible to normal users (internal headless renderer or tightly controlled owner/admin migration access only), and expose no legacy print/download while V2 export is unavailable. **A customer sees no download button until the V2 export feature is built.**
4. **Approved: closed-enum customer-safe `reason_code`** on the V2 customer-view endpoint (§3.3). Allowed public values are exactly `PREPARING`, `SERVICE_BUSY`, `VERIFICATION_REQUIRED`, `SERVICE_UNAVAILABLE`, `NO_REPORT`. The exact-admin diagnostic path in Vista admin is preserved unchanged. Tests must prove no raw internal reason code leaks.
5. **Approved: gate `/api/analysis/perizia/{id}/pdf` to exact owner/admin** (§3.4). Unauthorized requests must be rejected **before** any headless render starts. Backend is the authority; frontend route guards are defence-in-depth only. Endpoint is temporary — retired when the V2 export ships.
6. **Confirmed:** Riccardo currently has **no production user account, sessions, or analyses** (§1.4). This is **privilege removal, not account deactivation**. His 2 `beta_feedback` documents are preserved. Remove his email from `backend/.env` `BETA_UNLIMITED_EMAILS` so a future login does not receive unlimited credits, and also remove the inert hardcoded default from code (prefer an empty default).

**Security follow-up (owner action):** the owner session token shared during this session must be logged out / revoked after live validation.

The original brief's account-deactivation architecture (`is_active`/`disabled_at`, 403 disabled responses, session-revocation, generic account-admin CLI) is **withdrawn and must not be implemented in this branch.**

---

## 1. Verified Architecture

All anchors verified by reading code on 2026-07-15 at `c1827ea`.

### 1.1 Authentication

- **DB-backed sessions, not stateless JWT.** Local sessions live in Mongo `db.user_sessions`; users in `db.users`.
- External IdP: Emergent Auth + a backend-owned Google OAuth flow.
- `get_current_user(request)` — `backend/server.py:1140`. Reads `session_token` from cookie or `Authorization: Bearer`, looks up the session, checks `expires_at`, loads the user doc from `db.users`, calls `_apply_normalized_account_state(persist=True)`, returns `User`.
- `require_auth` — `backend/server.py:1172` (includes a loopback+token+env-gated offline-QA bypass).
- `require_master_admin` — `backend/server.py:1187`. `require_beta_or_admin` — `backend/server.py:1195`.
- Login chokepoint: `_create_local_login` — `backend/server.py:12506` → `_get_or_create_authenticated_user` — `backend/server.py:12430` (normalizes email trim+lowercase; auto-creates the user on first login) → `_create_local_session_for_user` — `backend/server.py:12477`.
- Audit infrastructure exists: `_write_admin_audit` — `backend/server.py:12339` → `db.admin_audit_log` (340 docs present).

### 1.2 Entitlements are derived per-request, never stored as a role

This is the single most important fact for Part 1.

`_normalize_account_state` (`backend/server.py:1035`) recomputes `is_beta_partner`, `plan`, `quota`, `perizia_credits` and `feature_access` **from the normalized email on every authenticated request**, via:

- `_is_admin_email` — `backend/server.py:630`
- `_is_beta_unlimited_email` — `backend/server.py:646` → membership in `BETA_UNLIMITED_EMAILS`
- `_is_credit_exempt_user` — `backend/server.py:651` = admin **or** beta-unlimited. **This is the credit bypass.**
- `_is_correctness_v2_admin_view_email` — `backend/server.py:641` (exact-email gate for Vista admin)

Every beta privilege in the product traces back to `_is_beta_unlimited_email(email)`:

| Privilege | Anchor | Source |
|---|---|---|
| Credit block bypass | `backend/server.py:16218` | `_is_credit_exempt_user` |
| `is_beta_partner` / `beta_partner_type` in `/auth/me` | `backend/server.py:1061`, `1069`, `1071`, `1126`, `1128` | `_is_beta_unlimited_email` |
| Beta-only endpoint (`require_beta_or_admin`) | `backend/server.py:20581` | `_is_beta_unlimited_email` |
| Feedback `user_role: beta_partner` | `backend/server.py:20278`, `20287`, `20289` | `_is_beta_unlimited_email` |
| `is_beta_partner` in feedback context | `backend/server.py:20652` | `_is_beta_unlimited_email` |
| Frontend `BetaRoute` → BetaDashboard | `frontend/src/App.js:112` | `user.is_beta_partner` |
| "Condividi valutazione tecnica" button | `frontend/src/pages/AnalysisResult.js:1593` | `user.is_beta_partner \|\| user.is_master_admin` |
| `featureAccess.isBetaPartner` | `frontend/src/lib/featureAccess.js:34` | `user.is_beta_partner` |

**Consequence:** removing his email from the allowlist removes *all* of the above at once, on his next request. There is no stored role to unwind.

### 1.3 Verified environment state (values not printed)

Read from `backend/.env`; the systemd unit `periziascan-backend.service` loads it via `EnvironmentFile=/srv/perizia/app/backend/.env` (`WorkingDirectory=/srv/perizia/app/backend`, `ExecStart=.venv/bin/uvicorn server:app --host 127.0.0.1 --port 8081`).

| Variable | State | Contains Riccardo | Contains owner |
|---|---|---|---|
| `BETA_UNLIMITED_EMAILS` | **SET in .env**, 1 entry | **YES** | no |
| `MASTER_ADMIN_EMAIL` | SET in .env, 1 entry | no | **YES** |
| `ADMIN_EMAILS` | NOT set → code default `nexodifyforyou@gmail.com` | no | yes |
| `CORRECTNESS_V2_ADMIN_VIEW_EMAIL` | NOT set → code default `nexodifyforyou@gmail.com` | no | yes |
| `BETA_PARTNER_DEFAULT_TYPE` | NOT set → code default `geometra` | — | — |

**Decisive:** `BETA_UNLIMITED_EMAILS` is set in `.env` and contains exactly Riccardo's email. **The code default is inert in production.** Editing only the code default would change nothing on the live service. The `.env` edit + restart is the operative fix; the code-default edit is defence-in-depth for fresh deploys.

**Owner privileges are safe:** `MASTER_ADMIN_EMAIL` (env) and `ADMIN_EMAILS` / `CORRECTNESS_V2_ADMIN_VIEW_EMAIL` (defaults) all resolve to the owner and are untouched by this plan.

**Riccardo already has no admin/Vista-admin access** — he is in none of the admin allowlists. Confirmation items 4 and 5 are therefore already true today; the change makes them *durably* true by removing the only privilege he does hold.

### 1.4 Database reality — IMPORTANT CORRECTION

Queried live Mongo (safe fields only):

- **`geomazzantiriccardo@gmail.com` has NO user document.** 14 users total; a case-insensitive regex for `mazzanti` across `db.users` returns **0 matches**.
- He therefore has **no sessions**, **no `perizia_analyses`**, and **no persisted `is_beta_partner` / `is_master_admin` / plan / wallet**.
- The owner's doc is intact: `plan=enterprise`, `is_master_admin=True`, wallet 9999, 21 active sessions, **268** `perizia_analyses`.
- `is_beta_partner` is **not a persisted field on any user doc** (`FIELD ABSENT` on the owner's doc). `_apply_normalized_account_state` (`backend/server.py:1087`) persists only `is_master_admin`, `plan`, `quota`, `perizia_credits`, `subscription_state` — never `is_beta_partner`. So beta status is purely derived.
- The only artefacts under his email are **2 `beta_feedback` docs** (`betafb_6150bea8…`, `betafb_d4b88e3e…`, created 2026-06-14) whose `user_id` is `user_live_proof_d4f0bad05d8d` — a **live-proof/test-script identity**, not a real login. `beta_feedback` holds 6 docs total.

**Implications, stated plainly:**

1. His beta entitlement is **prospective, not active**. He has never had an account. Today the allowlist means: *if he logs in, he is auto-created (`_get_or_create_authenticated_user`) and immediately granted unlimited credits.* Removing him from the allowlist closes that door before it opens.
2. **No session revocation is needed.** There is nothing to revoke, and entitlements are recomputed per-request from email regardless. This satisfies the "do not revoke sessions unless required to refresh stale cached entitlements" constraint — it is not required.
3. **No DB data fix is needed.** There is no stale persisted flag to correct.
4. "Preserve login, history and existing reports" is **vacuously satisfied** — there is no history to preserve. The 2 feedback docs will not be touched. **This should be confirmed with the owner**, because it may contradict their mental model of Riccardo having used the product.

---

## 2. Part 1 — Implementation

### 2.1 Code changes

**`backend/server.py:113-115`** — remove the hardcoded privileged email from the default:

```python
# before
BETA_UNLIMITED_EMAILS = _parse_email_allowlist(
    os.environ.get('BETA_UNLIMITED_EMAILS', 'geomazzantiriccardo@gmail.com')
)
# after
BETA_UNLIMITED_EMAILS = _parse_email_allowlist(
    os.environ.get('BETA_UNLIMITED_EMAILS', '')
)
```

**`backend/server.py:118-121`** — remove the hardcoded real name from `BETA_PARTNER_NAMES`:

```python
# after: empty by default; populate via config if a beta programme resumes.
BETA_PARTNER_NAMES: Dict[str, str] = {}
```

Rationale: it embeds a named private individual in source and only ever applied to a beta partner. Verify `_beta_partner_name_for_email` (`backend/server.py:658`) degrades safely to `None` — it already returns `None` for unmapped emails, and `_normalize_account_state:1070` passes it straight through.

**No other backend logic changes.** `_is_beta_unlimited_email`, `_is_credit_exempt_user`, `_normalize_account_state` and all gates stay exactly as they are — they are already generic and email-driven. Riccardo's email is never hardcoded into reusable auth logic; it exists only as config/operational input.

### 2.2 Test changes

`backend/tests/test_beta_feedback.py` hardcodes Riccardo's real email at lines 171, 187, 244-245, 254, 393, 441, 448, 494, 510, 555, 557 and asserts `_is_beta_unlimited_email('geomazzantiriccardo@gmail.com') is True`.

- Re-point every occurrence at a **fixture** address, e.g. `beta.partner@example.test`, and monkeypatch `BETA_UNLIMITED_EMAILS`/`BETA_PARTNER_NAMES` accordingly (the file already monkeypatches at line 171, so the pattern exists).
- Keep the normalization assertions (uppercase / mixed case / whitespace) — retarget them to the fixture email.
- Never assert privileges for a real person's address.

Add a regression test asserting `_is_beta_unlimited_email('geomazzantiriccardo@gmail.com') is False` **when the env var is unset** — this locks the code default and would catch a re-introduction.

### 2.3 Operational steps (the actual fix in production)

1. Edit `backend/.env`: set `BETA_UNLIMITED_EMAILS=` (empty). Do not delete the key — an empty explicit value is clearer and `_parse_email_allowlist` returns an empty frozenset for it (`backend/server.py:94-101`, falsy `raw` → `frozenset()`).
2. `sudo systemctl restart periziascan-backend.service`
3. **Verify the running process environment**, not just the file:
   `sudo tr '\0' '\n' < /proc/$(systemctl show -p MainPID --value periziascan-backend.service)/environ | grep -c '^BETA_UNLIMITED_EMAILS=$'`
   Expect the var present and empty. Never print the full environ.
4. Health check per `project_deploy_ops`.

`backend/.env` is gitignored (`.gitignore:114 *.env`) — the env edit is **not** part of the commit and must be applied on the host separately. **This is a manual production step the owner must approve.**

### 2.4 Verification of the six required confirmations

| # | Confirmation | How verified | Expected |
|---|---|---|---|
| 1 | Riccardo can still log in | No login gate references beta status; `_get_or_create_authenticated_user:12430` auto-creates on first login | Login works; user created with `plan=free` |
| 2 | Treated as a normal user | `_normalize_account_state:1035` → non-admin branch → `plan=free`, free quota | `is_beta_partner=false`, `is_master_admin=false` |
| 3 | No unlimited credits | `_is_credit_exempt_user:651` returns False → credit block at `:16218` applies | Blocked when wallet empty |
| 4 | No Vista admin access | Not in `ADMIN_EMAILS`/`MASTER_ADMIN_EMAIL`/`CORRECTNESS_V2_ADMIN_VIEW_EMAIL` | `correctness_v2_admin_view=false` |
| 5 | No beta-only bypasses | `require_beta_or_admin:1195` → 403; `BetaRoute` (`App.js:112`) redirects | All beta paths denied |
| 6 | Historical data unchanged | No writes/deletes in this plan; his 2 `beta_feedback` docs untouched | Byte-identical |

Test method (no real login required): seed a fixture user via the `auth_testing.md` DB-seeded session pattern, hit `/api/auth/me`, assert the flags. Assert the **owner** still returns `is_master_admin=true` and `correctness_v2_admin_view=true`.

---

## 3. Part 2 — Legacy Frontend Removal

### 3.1 Every legacy frontend mount found

| # | Mount | Anchor | Disposition |
|---|---|---|---|
| 1 | Legacy report body | `AnalysisResult.js:3972-5116` (`data-testid="legacy-report-body"`) | **DELETE** |
| 2 | Legacy reveal toggle ("Mostra report legacy (debug)") | `AnalysisResult.js:3955-3968` (`data-testid="legacy-report-reveal"`) | **DELETE** |
| 3 | `legacyReveal` state | `AnalysisResult.js:1616` | **DELETE** |
| 4 | Legacy customer fallback rule | `visibility.js:36` (`legacyFallback = v2Resolved && !hasSafeV2`) | **DELETE** |
| 5 | `showLegacyBody` / `canRevealLegacy` | `visibility.js:31,38,52-53` | **DELETE** |
| 6 | "Vista stampa" link → `/analysis/:id/print` | `AnalysisResult.js:4041-4048` (`data-testid="print-view-btn"`) | **DELETE** (inside body) |
| 7 | "Scarica Report" button | `AnalysisResult.js:4052-4057` (`data-testid="download-pdf-btn"`) | **DELETE** (inside body) |
| 8 | `handleDownloadPDF` → `/pdf` + JSON fallback | `AnalysisResult.js:1657-1719` | **DELETE** |
| 9 | `MultiLotSelector` (legacy-only) | `AnalysisResult.js:1518` | **DELETE** |
| 10 | Preparing banner text referencing the legacy preview | `AnalysisResult.js:3939` ("Nel frattempo puoi consultare l'anteprima qui sotto.") | **REWRITE** |
| 11 | Legacy print page | `pages/AnalysisPrintView.js` + route `App.js:194` | **GATE, do not delete** (§3.4) |
| 12 | Legacy derivation code (semaforo, beni, money box, red flags, headline fields, evidence) | `AnalysisResult.js` ~1600-3798 | **DELETE** (dead once body goes) |

Non-mounts (do **not** touch): `CorrectnessV2Panel.js` `legacyEvidence` / `legacyCustomerFallback` (`:508-527`, `:868`) refer to the **V2** `report.evidence_index` field naming, not the legacy report. `lib/api/perizia.js` is already clean of legacy endpoints.

### 3.2 The central design defect

`visibility.js:28` — `showV2Surface = isExactAdmin || hasSafeV2`, and `:36` — `legacyFallback = v2Resolved && !hasSafeV2`.

Today a customer with no safe V2 report is *carried by the legacy fallback*. Delete legacy without redesigning this gate and that customer gets a **blank page**. Fixing this gate is the core of Part 2, not an afterthought.

**New contract** — rewrite `visibility.js`:

```js
export const computeCorrectnessV2Visibility = ({ isExactAdmin = false, v2Resolved = false } = {}) => ({
  // The V2 surface is now the ONLY report surface: always mount it once the
  // probe resolves. CustomerReportView owns every sub-state internally.
  showV2Surface: true,
  showAdminTab: Boolean(isExactAdmin),
  showLoadingPlaceholder: Boolean(!v2Resolved),
});
```

No `showLegacyBody`, no `canRevealLegacy`, no `legacyFallback`, no `legacyReveal` input. Rewrite `visibility.test.js` to assert those keys **do not exist**.

### 3.3 State matrix → concrete implementation

**Verified backend contract** — `backend/correctness_v2/api.py:389-425` (`GET /{analysis_id}/correctness-v2/customer-view/latest`) returns exactly:

```jsonc
// unavailable
{ "available": false, "selected_lot_id": …, "preparing": bool, "reason_code": "NO_CUSTOMER_REPORT" }
// available
{ "available": true,  "selected_lot_id": …, "preparing": false, "report": <sanitized> }
```

`is_customer_safe` (`customer_view.py:335`) admits only `report_status ∈ {REPORT_READY, LOT_SELECTION_REQUIRED}` and honours `job.safe_to_show_customer is False`.

**Gap identified:** the endpoint emits **one** generic `reason_code: "NO_CUSTOMER_REPORT"` for *every* unavailable case, so the frontend cannot distinguish states 5, 6, 9 or 10. `OPENAI_QUOTA_EXHAUSTED` **does** exist at the job layer (`openai_client.py:59`, `is_quota_exhausted_reason:67`, classified from a structured `insufficient_quota` 429 at `:97-103`) — the signal exists; the customer endpoint discards it.

#### APPROVED backend change (owner decision 4)

In the `not report` branch of `correctness_v2_customer_view` (`api.py:397-419`), read the latest job (`artifacts.latest_job_for_analysis`, already used by `_has_in_progress_job:51-53`) and map its internal state to a **closed public enum**. These are the **only** permitted public values:

`PREPARING` · `SERVICE_BUSY` · `VERIFICATION_REQUIRED` · `SERVICE_UNAVAILABLE` · `NO_REPORT`

**Mapping rules (authoritative):**

| Internal state | Public `reason_code` | State |
|---|---|---|
| RUNNING / QUEUED / analysis being generated | `PREPARING` | 3 / 4 / 8 |
| `OPENAI_QUOTA_EXHAUSTED`, temporary capacity or quota failure | `SERVICE_BUSY` | 6 |
| `CONTRACT_VALIDATION_FAILED`, `NEEDS_MANUAL_REVIEW`, fail-closed correctness issue | `VERIFICATION_REQUIRED` | 5 |
| Backend dependency failure / API unavailable / unrecoverable transient service failure | `SERVICE_UNAVAILABLE` | 9 |
| Historical analysis, no V2 job and no active preparation | `NO_REPORT` | 10 |

**Hard constraints:**

1. **Never** expose internal job statuses, OpenAI error names, validator codes, stack traces, artifact paths, or raw failure messages to the customer.
2. Whitelist-map only — never pass a raw job `reason_code` through. Anything unrecognised must fall back to a safe default (`SERVICE_UNAVAILABLE`), never to the raw value.
3. **Preserve the exact-admin diagnostic path unchanged:** `nexodifyforyou@gmail.com` continues to see the internal reason and technical detail inside Vista admin (`CorrectnessV2Panel.js:182-184` renders `reason_code`/`reason_human`/`troubleshoot_message`). Do not touch it.
4. Normal customer/tester receives only the safe enum value and its public message.

**Required tests:** assert the customer payload contains no key outside `{available, selected_lot_id, preparing, reason_code, report}`; assert `reason_code` is always a member of the closed enum; and assert explicitly that raw internal codes (e.g. `OPENAI_QUOTA_EXHAUSTED`, `CONTRACT_VALIDATION_FAILED`) **never** appear in a customer response for any job state.

Does not touch the validator, money chain, lot segmentation, concurrency, or report contents.

**Mapping** (all in `CustomerReportView.js`, which already owns most of it — `:1264-1359`):

| # | State | Signal | Component / text |
|---|---|---|---|
| 1 | Safe V2 | `available && REPORT_READY` | `CustomerReportBody` (exists `:1217`); owner also sees Vista admin tab |
| 2 | Multi-lot selection | `report_status === LOT_SELECTION_REQUIRED` | `CustomerLotSelector` (exists) |
| 3 | Selected lot preparing | `lotUnavailable` | `CustomerLotPendingBox` + back-to-lots (exists) — verify copy reads **"Report del lotto in preparazione"** |
| 4 | Initial V2 preparing | `preparing` | exists `:1330` — **"Report cliente in preparazione"** ✓ |
| 5 | Validation failed | `VERIFICATION_REQUIRED` | **NEW** — "Report cliente non disponibile: verifica tecnica richiesta." |
| 6 | Quota exhausted | `SERVICE_BUSY` | **NEW** — "Il servizio è momentaneamente occupato e non disponibile. Riprova tra qualche minuto oppure contatta l'amministratore." |
| 7 | PDF unreadable | `report_status === DOCUMENT_NOT_READABLE` | `CustomerDocumentNotReadable` (exists `:1318`) ✓ |
| 8 | No job yet, V2 enabled | autostart at `api.py:402-413` | preparing state (exists) ✓ |
| 9 | Service unavailable | `SERVICE_UNAVAILABLE` / hook `error` | **NEW** — safe message + retry |
| 10 | Historical, no V2 | `NO_REPORT` | **REWORD** `:1341` → "Il nuovo report cliente non è ancora disponibile per questa analisi." |

Also rewrite the legacy-referring comments in `useCustomerView.js:9-16, 76-78` and `CorrectnessV2Tabs.js:12-15`.

### 3.4 Print route + `/pdf` coupling — per owner decision 3

**Verified coupling:** `download_perizia_print_pdf` (`server.py:16798`) → `require_auth` (**any** authenticated user, not admin) → ownership check → takes the **caller's** `session_token` (`:16807`) → `_render_print_pdf_via_frontend` (`:16705`) → spawns `frontend/scripts/render_analysis_print_pdf.mjs`, which sets that token as a cookie (`:38-42`) and navigates headless to `/analysis/{id}/print` (`:22, :66`), waiting for `window.__PERIZIA_PRINT_READY__` (`:68`). `AnalysisPrintView` fetches `/api/analysis/perizia/{id}` (`:72`). Deleting the route would break the endpoint — hence the owner's decision to keep it.

#### APPROVED plan (owner decisions 3 and 5)

1. **Keep** the route and `AnalysisPrintView.js` — the renderer depends on them. Remove **all** customer-facing navigation to it (mounts 6 and 7; they die with the legacy body). Gate: `grep -rn "/print" frontend/src --include=*.js` shows zero `<Link>`/`navigate` from the app.
2. **Gate `/api/analysis/perizia/{id}/pdf` to exact owner/admin — backend is the authority.** At `server.py:16799`, replace `require_auth` with an exact owner/admin check (`require_master_admin:1187`, consistent with the exact-email gate used elsewhere). Requirements:
   - Normal authenticated users get **403** (or 404 to avoid disclosure — pick one and apply consistently).
   - **The authorization check must precede any render.** It already would: `require_auth` runs at `:16799` and `_render_print_pdf_via_frontend` is only called at `:16812`. Keep that order — **no headless browser process may be spawned for an unauthorized request.**
   - Exact owner `nexodifyforyou@gmail.com` retains controlled access temporarily.
   - **Do not rely only on frontend route guards.**
3. **Gate the frontend `/print` route** (`App.js:194`) behind the owner/admin guard (reuse the `AdminRoute` pattern at `App.js:67`) as **defence-in-depth**, not as the authority.
4. **Internal renderer authorization path.** Verified today: `download_perizia_print_pdf:16798` passes **the caller's own** `session_token` (`:16807`) to `_render_print_pdf_via_frontend:16705`, which injects it as a cookie (`render_analysis_print_pdf.mjs:38-42`) and navigates to `/analysis/{id}/print` (`:22,:66`). Once step 2 lands, the caller is **always** an owner/admin, so the renderer's session inherently satisfies the route guard — the trusted path is *explicit and bounded*: server-initiated, admin-authorized only.
   - **Recommended hardening (owner to confirm):** decouple the renderer from a human session by minting a short-lived, single-purpose render token scoped to `analysis_id` (e.g. ~2 min TTL) instead of reusing the admin's 7-day session cookie, so a long-lived credential is never handed to a subprocess. This is a genuine improvement but **additional scope**; the caller-token path is acceptable for a temporary endpoint. **Flagged, not assumed.**
5. **No download button for customers** until the V2 export ships. No disabled/broken control — the control is *absent*.
6. **Required authorization tests:** a normal authenticated user calling `/pdf` is rejected **and no render subprocess is started** (assert `_render_print_pdf_via_frontend` is never invoked — patch and assert not-called); the owner is permitted; an unauthenticated caller is rejected.
7. **Temporary by design:** retire this endpoint, the `/print` route, `AnalysisPrintView.js` and `periziaPrintModel.js` when the V2 PDF/HTML export replaces them. Track as a follow-up.

### 3.5 `AnalysisResult.js` — lean rewrite vs surgical deletion

**Recommendation: lean rewrite**, retaining the page shell verbatim.

Rationale: the legacy body (3972-5116) is ~1150 lines of JSX, and ~2200 further lines (~1600-3798) exist *solely* to feed it. Surgical deletion of the body leaves a 3900-line file of dead derivation that still computes on every render, keeps `legacy` naming throughout, and invites re-introduction. Rewriting to ~200 lines is a larger diff but a far smaller *surface*, and it is verifiable: the file should contain zero occurrences of `legacy`.

**Keep verbatim:** `Sidebar`; `main` (`:3803`); back-to-storico link (`:3816`); `TechnicalFeedbackModal` (`:3844`) and its `canGiveTechnicalFeedback` gate (`:1593`); delete button + modal (`:3832`, `:3857-3904`); `handleDelete`; `HeadlineVerifyModal` (`:3906`); loading placeholder (`:3919`).

**Keep `fetchAnalysis`** (`:1625`, `GET /api/analysis/perizia/{id}`). It returns the legacy payload **but also** supplies `case_title`, `file_name`, `case_id`, `document_hash` used by the surviving shell. It is *not* a "call made solely to obtain legacy content", so the mission's removal rule does not apply. Consume only the shell fields; never render report content from it. See §5 residual risk.

Drop `isDebugMode`/`__DEBUG_ANALYSIS_PAYLOAD__` (`:1627`) and the `?debug=1` block (`:3804-3812`) if they only feed legacy — **verify before removing**.

### 3.7 Legacy API payload sanitization — APPROVED (owner decision 3, step D)

**Supersedes the earlier "residual risk / follow-up branch" position.** The legacy payload must not remain retrievable by normal users just because the page shell needs a few metadata fields.

**Verified surface:**

- `GET /api/analysis/perizia/{analysis_id}` — `server.py:19800-19804`, an alias of `_get_perizia_analysis_for_user` (`:19718`) → `_get_perizia_analysis_for_user_with_storage` (`:19001`), ownership-scoped (`{"analysis_id", "user_id": user.user_id}`, drops `raw_text` only). **Returns the complete legacy analysis payload to any authenticated owner.**
- Consumers: `AnalysisResult.js:1633` (shell — metadata only), `AnalysisResult.js:1663` (JSON download fallback — **being deleted**), `AnalysisPrintView.js:72` (**needs the full payload**; renderer, now owner/admin-gated).
- Shell actually needs only: `case_id`, `case_title`, `file_name`, `created_at`, `pages_count`, `document_hash`/`input_sha256` (+ `analysis_id`). Legacy content lives in `result` / `data` / `payload`.
- `GET /api/history/perizia` (list) — `History.js:96`. **Fable must verify** whether the list embeds full legacy `result` payloads; if it does, sanitize it to list metadata too.

**Design (preferred option — new minimal metadata endpoint):**

1. **Add** `GET /api/analysis/perizia/{analysis_id}/meta` — `require_auth` + existing ownership check. Returns **only** the shell fields above. **Whitelist-project the response** (build the dict key by key); never blacklist-strip, or new legacy fields leak by default.
2. **Repoint** `AnalysisResult.js` `fetchAnalysis` (`:1633`) at `/meta`. The page then never fetches legacy content.
3. **Gate** the full-payload routes — `GET /api/analysis/perizia/{analysis_id}` (`:19800`) and the equivalent `/api/history/perizia/{analysis_id}` detail route — to **exact owner/admin only** (same check as `/pdf`, §3.4). Normal customers and other admin/master users get **403/404**. The exact owner retains access because the headless renderer authenticates with their cookie (`AnalysisPrintView.js:72`), and internal migration/rollback keeps server-side access.
4. Legacy data **remains stored** in Mongo — this is an exposure change, not a data change. `DELETE` and other routes are untouched.

**Required tests:**

- The **owner of an analysis** (normal user) calling `/meta` receives metadata only — assert `result`, `data`, `payload`, `red_flags`, `summary`, `decision`, `costs`, `risks` are **absent**.
- A normal authenticated owner calling `GET /api/analysis/perizia/{id}` is **rejected** — they cannot retrieve the legacy report by direct API access.
- A non-exact admin/master user is **rejected** on the full-payload route.
- The exact owner still receives the full payload (renderer + migration must keep working).
- Frontend: the V2 page flow fetches `/meta` and **never** `/api/analysis/perizia/{id}` or `/pdf`.

**Risk:** gating the detail route may break other callers. Fable must grep the whole repo (including `frontend/scripts/`, `backend/scripts/`, tests) for consumers before changing it, and report anything found rather than breaking it silently.

### 3.6 Tests

| File | Action |
|---|---|
| `pages/AnalysisResult.legacyFallback.render.test.js` | **DELETE** — asserts the fallback we are removing |
| `pages/AnalysisResult.customerFacing.render.test.js` | **REWRITE** — no legacy body/fetch |
| `pages/AnalysisResult.freshPipeline.render.test.js` | **REWRITE** |
| `pages/AnalysisResult.canonicalIssues.render.test.js` | **REVIEW** — likely delete (legacy-tab assertions) |
| `pages/AnalysisResult.liveAcceptance.render.test.js` | **REVIEW** — likely delete |
| `components/correctness-v2/visibility.test.js` | **REWRITE** to the new contract |
| `lib/periziaPrintModel*.test.js` | **KEEP** — print model stays (route retained) |

New tests required by the mission:

- Each of the 10 states: correct copy renders, **and** `queryByTestId('legacy-report-body')` is `null`.
- **DOM assertion:** no legacy renderer mounted in any state.
- **Network assertion:** the V2 page flow issues no legacy-report fetch (assert the axios mock never sees `/pdf`; `/api/analysis/perizia/{id}` is expected once for the shell).
- Owner: Report cliente + Vista admin, **no** `legacy-report-reveal` control.
- Normal customer: no Vista admin, no legacy.
- Print/download: no `print-view-btn`, no `download-pdf-btn`.
- Backend: customer-view payload contains no key outside the closed set; no admin/validator/artifact/debug leakage.

---

## 4. Execution Order (with gates)

1. Part 1 code (§2.1) + test re-pointing (§2.2). **Gate:** `backend/.venv/bin/python -m pytest backend/tests/test_beta_feedback.py -q`.
2. Backend customer-safe `reason_code` mapping (§3.3) **— only after owner approves the backend change**. **Gate:** `pytest backend/correctness_v2/tests -q`.
3. `visibility.js` rewrite + tests (§3.2). **Gate:** `npm test -- --watchAll=false`.
4. `CustomerReportView.js` states 5/6/9/10 (§3.3). **Gate:** frontend tests.
5. `AnalysisResult.js` lean rewrite (§3.5). **Gate:** `grep -ci legacy frontend/src/pages/AnalysisResult.js` → **0**.
6. Print route gating (§3.4).
7. Full validation: `compileall`, `pytest backend/correctness_v2/tests -q`, full backend tests, `npm test -- --watchAll=false`, `npm run build`.
8. Six-case regression: Torino, Pistoia, 1859886_C, Orecchiazzi, Cairate, Codogno 6/6. Confirm concurrency stays 2, no phantom Lotto 00.
9. Pre-commit: show branch/status/diff; confirm no `backend/.env`, no secrets, `.codex/` and `.runs/` unstaged.
10. Operational env step (§2.3) — **owner-approved, manual, not in the commit**.

---

## 5. Risk Register

| Risk | Severity | Mitigation |
|---|---|---|
| `.env` edit forgotten → Riccardo still privileged in prod | **High** | §2.3 step 3 verifies the *running process* env, not the file |
| Blank page for customers with no safe V2 | **High** | §3.2 — surface always mounts; test all 10 states |
| Lean rewrite drops a shell feature (delete/feedback/headline) | Medium | Keep shell verbatim; render tests cover each control |
| Deleting `/print` route breaks `/pdf` | Medium | **Avoided** — route retained per decision 3 |
| Normal user calls `/pdf` directly → 120s headless burn → 502 | Medium | **Resolved** — §3.4 step 2 gates `/pdf` to owner/admin before any render is spawned |
| Admin's 7-day session cookie handed to the renderer subprocess | Low-Med | **Residual** — §3.4 step 4; short-lived scoped render token recommended as follow-up |
| `/api/analysis/perizia/{id}` still returns the legacy payload to its owner | **Low-Med** | **Resolved — now in scope (§3.7, step D).** New `/meta` endpoint for the shell; full-payload route gated to exact owner/admin. |
| Gating the detail route breaks an unknown caller | Medium | §3.7 — grep all consumers repo-wide first; report, don't break silently |
| Removing `BETA_PARTNER_NAMES` breaks feedback display | Low | `_beta_partner_name_for_email:658` returns `None` for unmapped; verify with a test |
| Backend `reason_code` mapping leaks internal detail | Medium | Closed whitelist enum; payload-key assertion test |

---

## 6. Must Not Change

`CORRECTNESS_V2_LOT_CONCURRENCY=2` · OpenAI model/config · V2 validator · money chain behaviour · lot segmentation · credit/package pricing · Stripe · data-retention policy · GDPR wording · the future admin panel · owner privileges for `nexodifyforyou@gmail.com` · Riccardo's account existence, login ability, and his 2 `beta_feedback` docs.

---

## 7. Decisions Resolved / Still Open

### Resolved (2026-07-15)

| # | Item | Decision |
|---|---|---|
| 1 | Riccardo has no account/history (§1.4) | **Confirmed.** Removal is *preventive*; his privilege is prospective only. Proceed. |
| 2 | Backend change for states 5/6/9/10 (§3.3) | **Approved** — closed public enum, no internal leakage, admin diagnostics preserved, leak tests required. |
| 3 | Gate `/pdf` to owner/admin (§3.4) | **Approved** — backend is the authority; no render for unauthorized requests; authorization tests required. |
| 4 | Legacy print route | **Keep and gate** — renderer depends on it. Retire with the V2 export. |
| 5 | Beta feedback docs | **Preserve** the 2 `beta_feedback` documents; no writes. |

### Still open

1. **`.env` edit + restart is a manual production step** (§2.3) — gitignored, outside the commit. **Confirm who applies it and when.** Until it is applied, Riccardo remains privileged on the live service regardless of the merged code.
2. **Renderer credential hardening** (§3.4 step 4) — the renderer currently receives the admin's 7-day session cookie. A short-lived scoped render token is recommended but is additional scope. **Confirm in-scope or follow-up.**
3. **Residual legacy API exposure** (§5) — `/api/analysis/perizia/{id}` still serves the legacy payload to the analysis owner; it is ownership-gated and required by the surviving page shell. Recommend a follow-up branch. **Confirm acceptable for now.** Do not describe legacy as "removed from the API".
4. **Owner session token revocation** — the token shared in this session must be logged out/revoked after live validation.

---

## 8. Status

**Not safe to commit** — nothing implemented.
**Not safe to deploy** — nothing implemented.
Branch clean at `c1827ea`. Implementation held pending owner review of this document (owner decision: "Hold — I'll read the plan first").
