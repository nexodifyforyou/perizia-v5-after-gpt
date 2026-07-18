# Storico Lot Workspace — Reviewed Implementation Plan

Branch: `feature-storico-lot-workspace`
Base: `main` @ `71f5d5d` (release tag `correctness-v2-access-legacy-removed-live`)
Production config preserved: `CORRECTNESS_V2_LOT_CONCURRENCY=2`
Audit basis: Fable 5 read-only audit (2026-07-16), reviewed and endorsed by Opus.

This plan turns **Storico** into the permanent workspace for every processed perizia:
reopen an analysis, see the lot overview, open already-generated lot reports with **zero
OpenAI calls / zero credit debit**, and explicitly generate or rerun lots with an
authoritative credit preview. It closes the **failed-lot re-trigger bug**.

Scope guard (Part 13): this branch is workflow / routing / reuse / job-control only. It does
**not** touch Stripe, package pricing, the credits-per-analysis formula, webhooks, checkout,
GPT price assumptions, customer report *content*, PDF/HTML export, GDPR wording, admin user
management, production concurrency, the validator, money-chain rules, extraction, lot
segmentation, or the shipped legacy-removal/access controls.

---

## A. Current routes & APIs (from audit)

### Backend — `backend/correctness_v2/api.py` (router `/api/analysis/perizia`)
| Route | Auth | Notes |
|---|---|---|
| `POST /{id}/correctness-v2/start` | admin-only (`CORRECTNESS_V2_ADMIN_ONLY=true`) | Synchronous `start_job`; **every call mints a new job, no reuse/dedup.** api.py:256 |
| `GET /{id}/correctness-v2/jobs/{job_id}` and `/jobs/{job_id}/*` artifacts | admin-only | Pure reads. api.py:293–370 |
| `GET /{id}/correctness-v2/customer-view/latest?selected_lot_id=` | ownership (not admin-only) | **The customer product endpoint. Has a job-spawning SIDE EFFECT** at api.py:469–481. |
| `POST /{id}/correctness-v2/customer-view/confirm-money` | ownership | Deterministic; no OpenAI. api.py:496 |
| `GET /{id}/correctness-v2/latest` | admin-only | Latest job status. api.py:546 |

### Backend — `backend/server.py` (billing, history, ownership)
- Credit bands `PERIZIA_CREDIT_BANDS` (server.py:602–608), `_get_required_perizia_credits(page_count)` (617–622) — **per-upload, page-banded, lot-agnostic**.
- Only debit: `analyze_perizia` after persistence, `_apply_perizia_credit_debit_with_ledger(entry_type="perizia_upload", ...)` (server.py:16697–16712); pre-check 403 `INSUFFICIENT_PERIZIA_CREDITS` (16240–16257).
- Exemption `_is_credit_exempt_user` (server.py:650–654) = admin or (now empty) beta list.
- History rows: `GET /api/history/perizia` (server.py:18946–18992) — legacy metadata + semaforo only; **no lot/V2 state**.
- Ownership: customer-view `{analysis_id, user_id}` lookup for non-admins (api.py:403–408); owner/admin = `nexodifyforyou@gmail.com`.

### Frontend
- `pages/History.js` → `GET /api/history/perizia`; cards link to `/analysis/:id`; no lot info.
- `pages/AnalysisResult.js` → loads shell meta only; mounts `useCorrectnessV2CustomerView` + `CorrectnessV2Tabs`; back = static `/history` link.
- `components/correctness-v2/useCustomerView.js` — **`selectedLotId` is React state only** (never in URL); polls `customer-view/latest` every 8s up to 225× while `preparing`.
- `components/correctness-v2/CustomerReportView.js` — state machine for preparing / unavailable / lot-selector / money-confirmation / report body.
- `lib/api/perizia.js` — `getCorrectnessV2CustomerView` (the customer GET), money-confirm, admin start; **no workspace / credits client**.
- `App.js` — routes have **no lot segment/param**.

---

## B. The bug being fixed (root cause)

`GET customer-view/latest?selected_lot_id=X` autostarts a job when no REPORT_READY exists for X
**and** `_has_in_progress_job` is false (api.py:469–481). A **terminal FAILED** latest job makes
`_has_in_progress_job` false (api.py:37–53, analysis-scoped, latest-only), so:
1. Clicking a failed lot spawns a new job (≥2 OpenAI calls: full-doc analyst + lot analyst; selected-lot path has **no reuse**, orchestrator.py:678–769).
2. The 8s preparing-poll re-hits the same URL, and each failure re-arms autostart → an **unbounded server-side retry loop** (~30 min/page view).

Secondary re-triggers with the same mechanism:
- `_find_customer_job` lot filter requires `report_status == "REPORT_READY"` **exactly** (api.py:443–446), so a lot that finished `MONEY_CONFIRMATION_REQUIRED` (customer-safe) is skipped when `selected_lot_id` is set → duplicate spawn.
- analyze_all per-lot reports (`jobs/{job_id}/lots/{lot_id}/customer_report.json`) are invisible to `_find_customer_job` → also spawn.

---

## C. Proposed workspace API shape (additive; do not overload customer-view)

All routes ownership-gated via the existing `_resolve_customer_access` (api.py:383–409). Admin/owner
retains diagnostic access through existing admin routes; non-owner → 404/established denial.
No route weakens `correctness-v2-access-legacy-removed-live`.

### 1. `GET /{analysis_id}/correctness-v2/workspace` — pure read, ZERO side effects
Customer-safe lot overview. No autostart, ever.
```json
{
  "analysis_id": "...",
  "multi_lot": true,
  "lot_count": 3,
  "analysis_state": "LOT_OVERVIEW | SINGLE_LOT | LOT_SELECTION_REQUIRED",
  "lots": [
    {
      "lot_id": "1",
      "label": "Lotto 1",
      "address": "...",
      "property_type": "...",
      "occupancy_summary": "...",
      "final_value": "€ ...",              // safe display value only
      "state": "REPORT_READY | RUNNING | MONEY_CONFIRMATION_REQUIRED | VERIFICATION_REQUIRED | FAILED | NOT_ANALYZED",
      "has_safe_report": true,
      "job_running": false,
      "latest_report_at": "2026-...T...Z",
      "report_version": 3,                 // = job attempt/version; safe integer
      "last_attempt_failed": false,        // prior-safe + newer-failed signal
      "actions": ["open_report", "rerun"], // allowed actions, server-authoritative
      "credit_preview": { ...see D... }
    }
  ]
}
```
Implementation: new thin module `backend/correctness_v2/workspace.py` folds per-lot state from
(a) job-level lot jobs (`extra.selected_lot`), (b) analyze_all `per_lot_results`
(orchestrator.py:1151) and per-lot artifacts, (c) the `LOT_SELECTION_REQUIRED` lot index
(`lot_packets.build_lot_index`) for the `NOT_ANALYZED` set. Failure statuses map through the
**closed public reason enum** already used at api.py:66–120; internal codes stay admin-only.

### 2. `POST /{analysis_id}/correctness-v2/lots/{lot_id}/generate` — the ONLY customer job-creation path
Ownership-gated (not admin-only). Body `{ "force": bool }` (`force=true` = explicit rerun).
Server applies the reuse/dedup rules in §E and the credit rules in §D. Returns:
```json
{ "job_id": "cv2_...", "state": "RUNNING", "deduplicated": false,
  "reused_report": false, "credit": { ...see D... } }
```

### 3. Make `GET customer-view/latest` side-effect-free
Delete the autostart branch (api.py:469–481). `autostart_job` remains for the **upload** path
only (server.py:16678–16695). The GET then only *reports* state; all generation moves to the
POST. This alone closes the failed-lot retrigger loop and the poll-driven respawn.

### 4. `GET /{analysis_id}/correctness-v2/lots/{lot_id}/generate/preview` (or embed in workspace)
Authoritative credit preview (§D). Read-only.

### 5. Extend `GET /api/history/perizia` rows (server.py:18946)
Add a compact, additive `v2` summary per analysis: `{ state, lot_count, ready, preparing,
confirmation_required, needs_review, not_analyzed }`. Cheap via one workspace fold per row
(bounded) — see §H on scan cost.

### 6. Optional (Part 7) `POST /{analysis_id}/correctness-v2/lots/generate-missing`
Batch "Analizza lotti mancanti": excludes READY/RUNNING lots, uses existing
`CORRECTNESS_V2_LOT_CONCURRENCY=2`, requires explicit confirm + preview. Free under existing
billing (see §D). Implemented last; deferred with documentation only if it would require
billing changes (it does not).

---

## D. Credit-preview design (Scope-Guard-compliant, no new wallet behavior)

**Decision (resolves audit §F.1/§F.2):** Under the existing billing logic, credits are charged
**once, per upload, page-banded, lot-agnostic** (server.py:602–622, 16697). There is **no per-lot
price**, and Part 13 forbids inventing one. Therefore **lot generation and lot rerun consume zero
additional credits** — the upload charge already covers the analysis. The preview reports this
truthfully; it does **not** claim a rerun costs a credit (mission Part 6).

Authoritative preview object (from the **same existing billing functions**, never re-derived in JS):
```json
{
  "can_start": true,
  "will_consume_credit": false,      // false: upload already paid; lot work is credit-free today
  "credits_required": 0,
  "available_credits": 12,           // from existing wallet totals (server.py:1056–1080)
  "already_paid_at_upload": true,
  "exempt": false,                   // _is_credit_exempt_user
  "reason": null                     // e.g. "SERVICE_BUSY" when can_start=false
}
```
Backend source of truth: a small helper that calls the **existing** `_get_required_perizia_credits`
/ wallet-total accessors and `_is_credit_exempt_user`. The frontend renders these values verbatim;
it never computes credits. If a `can_start=false` condition exists (e.g. a bound on forced reruns,
§E), `reason` carries a closed public code.

No debit is added for lot generation/rerun (existing economics preserved). The debit call
(`_apply_perizia_credit_debit_with_ledger`) is **not** invoked from any new path. Stripe / package /
webhook / formula code is untouched.

---

## E. Job-reuse & duplicate-suppression rules (server-authoritative)

For `POST .../lots/{lot_id}/generate` (and defensively inside `start_job`):

- **A. REPORT_READY exists for (analysis, lot)** (job-level **or** analyze_all per-lot) and not `force`
  → return the existing job reference, `reused_report:true`, **no new job, no analyst call**. Surface
  analyze_all per-lot reports by adding a per-lot reader (fixes the invisibility gap).
- **B. In-progress job for the same (analysis, lot)** → return that job, `deduplicated:true`, no new
  job. Requires a **lot-aware** in-progress check (candidate jobs' `selected_lot`/analyze_all), not
  only `latest_job_for_analysis` — fixes the api.py:51–53 latest-only race.
- **C. No report/job for the lot** → create exactly one job (explicit user action only).
- **D. Terminal failure exists for (analysis, lot)** and not `force` → **do NOT auto-retry**; respond
  `{reason_code:"LOT_FAILED_RERUN_REQUIRED"}` so the UI shows the failure with an explicit
  "Riprova/Rigenera" button. This is the core fix.
- **E. Two simultaneous start requests** → one job. Persistent idempotency via a lightweight registry
  keyed `(analysis_id, lot_id|"__doc__"|"__all__")` (sidecar `{ARTIFACTS_ROOT}/index/{analysis_id}.json`
  or Mongo), written by `start_job` on creation and terminal transition; plus reuse of the existing
  `lot_runner._dedup_call` for the analyze_all inner call.
- **F. Completed report reuse** → serve the exact validated artifact; never rebuild via LLM to display.
- **G. Rerun (`force:true`)** → new job/version; **never overwrites the previous safe report before the
  new one succeeds**. Bound forced reruns with a per-(analysis, lot) attempt counter + short cooldown
  in the registry (job-control, not billing) so even explicit reruns can't be abused into cost.
  `credit_exhausted` (OpenAI-quota) failures are always free to retry.
- Also fix `_find_customer_job` to accept `MONEY_CONFIRMATION_REQUIRED` reports whose
  `lot_structure.selected_lot` matches (api.py:443–446) so a paused lot resumes its prompt instead of
  respawning.

"Previous safe report + newer failed rerun": the registry keeps the last REPORT_READY job pointer
distinct from the newest (failed) attempt, so the workspace shows **"Ultimo report verificato
disponibile"** (open the safe report) **and** "L'ultimo tentativo non è stato completato."

---

## F. Route / navigation design (frontend state persistence)

- **URL-persisted lot** (audit §D.1): use `/analysis/:analysisId?lot=<lot_id>` via `useSearchParams`.
  `useCustomerView` reads/writes the param instead of `useState(null)` (useCustomerView.js:38). Refresh,
  back/forward, and deep links then preserve the selected lot. Not ephemeral component state.
- **Landing rules:** multi-lot analysis opens the **lot overview** (no `lot` param) — never a stale
  latest-lot report. Single-lot with a safe report may open its report directly, with a clear route back.
- **"Torna ai lotti"** inside a lot report → clears the `lot` param, returns to the overview, **no job**.
- **"Torna allo storico"** preserved (static `/history`).
- **Polling** no longer creates jobs (falls out of §C.3): poll the workspace/status read endpoint, which
  has no side effects.

Frontend state machine (per lot):
| State | Primary action | Behavior |
|---|---|---|
| REPORT_READY | "Apri report" | Open stored safe report — 0 jobs, 0 OpenAI, 0 debit. Secondary: "Rianalizza lotto" (explicit, separated). |
| RUNNING/PREPARING | "Report in preparazione" | Reuse existing job; poll status; no duplicates; return allowed. |
| VERIFICATION_REQUIRED | "Vedi verifica richiesta" | Show safe verification state; no auto-rerun; preserve prior safe report. Secondary "Riprova analisi" (confirm + preview). |
| FAILED | "Analisi non completata" → "Riprova analisi" | No auto-rerun; explicit retry only. |
| NOT_ANALYZED | "Genera report lotto" | Explicit start via POST generate. |
| LOT_SELECTION_REQUIRED | show selector | Never a stale latest-lot report. |
| Prior-safe + newer-failed | "Ultimo report verificato disponibile" + failure notice | Open last safe report; do not replace it with an error page. |

Rerun confirmation modal shows: lot label, current report date/version, credit preview (from backend
§D — will show "0 crediti · già incluso" today), available credits, cancel, confirm. On confirm: one
job, previous safe report preserved, running state shown. Repeated confirms do not double-charge / dup.

---

## G. Storico experience

History cards gain, using the existing dark/gold style with restrained status colors
(green ready / blue preparing / amber confirmation/verification / red failed / slate not-analyzed):
- perizia title, date, single-vs-multi, total lots, and a summary line e.g.
  `"6 lotti · 4 pronti · 1 da verificare · 1 non analizzato"` from the `v2` summary (§C.5).
- One dominant action per state; opening a multi-lot analysis lands on the lot overview.
- Legacy semaforo badge kept additive for now (removing it is a report-content concern, out of scope).

---

## H. Migration implications

- **No schema migration required.** New state derives from existing job artifacts + Mongo analyses. The
  registry index (`{ARTIFACTS_ROOT}/index/{analysis_id}.json` or a `cv2_job_index` Mongo collection) is
  **built lazily** on first workspace/generate call and updated by `start_job`; absence is handled by
  falling back to the current O(all-jobs) scan (`artifacts.list_jobs`). Existing analyses work with no
  backfill; the index just makes them cheaper over time.
- **Lot-set stability (audit §F.6):** the workspace derives a deterministic lot set from the canonical
  lot index; ordering is stable. Full pinning-with-escape-hatch is noted as a follow-up, not required
  for this branch.
- **Deletion cascade (audit §F.5): out of scope** (GDPR/admin branch). Current behavior retained
  (deleting an analysis leaves V2 artifacts); documented, not changed.
- `CORRECTNESS_V2_LOT_REUSE` stays **as-is** (worksheet reuse *within* analyze_all is independent of the
  API-level report reuse we add). Production `.env` is not modified.

---

## I. Exact files expected to change

Backend:
- `backend/correctness_v2/api.py` — remove GET autostart branch; add `workspace` GET, `lots/{id}/generate`
  POST, `generate/preview` GET; lot-aware in-progress + reuse checks; fix `_find_customer_job`
  money-confirmation lot filter.
- **new** `backend/correctness_v2/workspace.py` — per-lot state folding, keeps api.py thin.
- `backend/correctness_v2/artifacts.py` — registry/index helpers; per-lot customer report reader.
- `backend/correctness_v2/orchestrator.py` — register jobs in the index on creation/terminal write
  (no pipeline behavior change).
- `backend/server.py` — extend `/history/perizia` projection with the additive `v2` summary; expose the
  authoritative credit-preview helper (reusing existing billing functions — **no debit added**).
- `backend/correctness_v2/tests/` — new + updated (see §J).

Frontend:
- `frontend/src/lib/api/perizia.js` — `getCorrectnessV2Workspace`, `generateCorrectnessV2Lot`,
  `getCorrectnessV2LotCreditPreview`.
- `frontend/src/components/correctness-v2/useCustomerView.js` — URL-param lot; side-effect-free polling;
  generation via POST.
- `frontend/src/components/correctness-v2/CustomerReportView.js` (+ lot-overview grid / workspace view)
  — per-lot state badges, FAILED + explicit rerun UI, rerun confirmation modal with backend preview,
  "Torna ai lotti".
- `frontend/src/pages/History.js` — V2 summary per card; status colors.
- `frontend/src/pages/AnalysisResult.js`, `frontend/src/App.js` — route/search-param plumbing.
- Corresponding `*.test.js` files.

**Billing/Stripe files touched (even indirectly):** `backend/server.py` only — and only to *read*
existing credit/wallet accessors for the preview and to add the additive history `v2` summary. **No
change** to `PERIZIA_CREDIT_BANDS`, `_get_required_perizia_credits`, `_apply_perizia_credit_debit_with_ledger`,
Stripe price/product/checkout/webhook code, or package definitions.

---

## J. Tests

Backend (Part 11): (1) workspace open → 0 jobs; (2) open completed lot → 0 jobs; (3) → 0 debit;
(4) refresh → 0 jobs; (5) return to overview → 0 jobs; (6) NOT_ANALYZED explicit start → 1 job, existing
debit rules (0 additional today) applied once; (7) simultaneous duplicate start → 1 job, ≤1 debit;
(8) RUNNING lot → reuse, no dup; (9) **FAILED lot: open → 0 jobs; explicit retry → 1 job** (the fix);
(10) failed rerun with prior safe report → prior remains accessible; (11) successful rerun → new latest
only after safe completion, prior stored; (12) preview uses existing billing calc, preview==actual,
frontend can't manipulate required credits; (13) insufficient credits → safe denial, no job, no partial
debit; (14) ownership isolation; (15) no legacy/admin/debug leakage; (16) multi-lot ordering/isolation;
(17) batch missing-lots (if shipped) excludes ready/running, partial failures preserved.
Update `test_api_customer_view.py:136` (autostart-on-lot-selection) to the new POST-only contract.

Frontend (Part 11): Storico progress summary; multi-lot opens overview; single-lot safe report opens;
"Torna ai lotti" no API/job; completed → "Apri report"; missing → "Genera report lotto"; failed no
silent rerun; explicit rerun confirmation; credit from backend preview; prior safe report survives
failed rerun; refresh/back/forward preserve route; running polls existing job; no double-click dup;
normal user no Vista admin; owner sees Vista admin; no legacy DOM/fetch/print/download; mobile+desktop.

Validation commands (Part 14, isolated temp backend; do not touch prod `.env` or restart prod):
```
backend/.venv/bin/python -m compileall backend
backend/.venv/bin/python -m pytest backend/correctness_v2/tests -q
cd frontend && npm test -- --watchAll=false && npm run build
```

---

## K. Six-case regression corpus (Part 12)

For Torino `analysis_9418e2972795`, Pistoia `analysis_0acceec34340`, 1859886_C `analysis_3cad50719f75`,
Orecchiazzzi `analysis_b3d5b9e23e22`, Cairate `analysis_2aac57572b7a`, Codogno `analysis_496b17c1c778`:
record jobs-before, action, jobs-after, credits-before/after, report/status, API calls, and **whether an
OpenAI analyst call occurred**. Expected: single-lot reopens create 0 jobs; multi-lot open lands on
overview; opening all completed lots creates 0 jobs; Orecchiazzzi VERIFICATION_REQUIRED does not auto-rerun;
Codogno = 6 genuine lots, no Lotto 00, all reused, concurrency 2 for explicit batch. Values (e.g. Torino
€38.110,20) unchanged.

---

## L. Rollback

- Pure git: the branch is additive; revert the branch / reset to `71f5d5d` (tag
  `correctness-v2-access-legacy-removed-live`). No DB migration to undo. The registry index is
  regenerable/ignorable; deleting `{ARTIFACTS_ROOT}/index/` returns to scan-based lookup.
- Feature can be guarded behind a flag (e.g. `STORICO_WORKSPACE_ENABLED`) so the workspace endpoints and
  the removal of the GET autostart can be toggled if a regression appears post-deploy. Frontend falls
  back to the current customer-view behavior when the workspace endpoint 404s.
- Deploy steps mirror `project_deploy_ops`: frontend auto-deploys on push to main (Vercel); backend =
  `systemctl restart periziascan-backend.service` + health check; tag the release. **Not performed until
  final report review** (Part 15).

---

## M. Product decisions (audit §F resolved by mission Scope Guard)

1. **Extra lots cost credits?** No — existing billing is page-banded per upload, lot-agnostic; Part 13
   forbids new wallet behavior. Preview reports `credits_required:0, will_consume_credit:false`.
2. **Rerun pricing/limits?** Free (per #1); bounded by an attempt cooldown (job-control, not billing);
   OpenAI-quota (`credit_exhausted`) retries always free.
3. **MONEY_CONFIRMATION_REQUIRED reopen?** Resume the prompt (fix api.py:444 filter). Aligned with
   existing money-confirmation HITL.
4. **analyze_all "Analizza lotti mancanti"?** Implement as optional, free, concurrency 2, explicit
   confirm; ship last; defer-with-docs only if it needed billing changes (it doesn't).
5. **Deletion cascade?** Out of scope (GDPR/admin branch); retain current behavior; documented.
6. **Lot-set stability?** Deterministic lot set from canonical index; full pinning noted as follow-up.
7. **History summary source?** Additive V2 summary; keep legacy semaforo for now.
8. **Core principles list?** The mission brief's 10 CORE PRODUCT PRINCIPLES are authoritative (Fable
   lacked the doc; it exists in the mission).

No genuinely unresolved product decision blocks implementation.

---

## N. Commit / merge gate (Part 15) — not satisfied until

opening Storico creates no jobs; cached reports create no jobs/charges; failed lots no longer auto-rerun;
explicit start/rerun works; duplicate requests can't double-charge; prior safe report survives failed
rerun; ownership enforced; six-case regression passes; no legacy returns; concurrency stays 2; Stripe and
credit formula unchanged. Suggested commit: `Build storico lot workspace with safe report reuse`.
Do not merge to main until the final report is reviewed.
