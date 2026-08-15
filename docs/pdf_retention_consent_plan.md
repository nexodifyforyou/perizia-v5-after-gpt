# PDF Retention with Consent + TTL — Architecture & Implementation Plan

Branch: `feature-pdf-retention-consent` · base `main` @ `8ee2aa9` (final main; Branches 1–3 +
R3-02 all DEPLOYED LIVE). Programme task 5
(`docs/perizia_scan_quality_recovery_program.md` §P0.5). Fable 5 planning only — **no runtime
code, tests, or config were modified to produce this document.** Sol implements after owner
review.

Motivation (generic — no production identifiers): the first external beta forensic
investigation showed how valuable the true original upload is when a pipeline defect must be
reconstructed after the fact. That investigation only succeeded because a copy of the original
PDF happened to survive outside the normal flow. Today, in the *normal* flow, the original bytes
are **never retained at all** — see §1. This task adds a narrow, consent-gated, encrypted,
TTL-bounded retention path so a future incident can be reconstructed from the true original,
without ever holding a customer's document insecurely, indefinitely, or without their explicit
permission.

---

## 1. Executive summary + current behavior, traced

**Current behavior: the original uploaded PDF bytes are never persisted anywhere, under any
circumstance.**

Traced end-to-end from the single upload endpoint, `POST /api/analysis/perizia`
(`backend/server.py:16392-16393`, handler `analyze_perizia`):

1. `case_id`, `run_id`, `analysis_id` are minted up front (`server.py:16406-16408`) — backend
   generated, unguessable, never browser-supplied. `analysis_id` is the durable lineage key used
   everywhere downstream (beta quota, Correctness v2 jobs, history, deletion).
2. `contents = await file.read()` (`server.py:16429`) reads the uploaded PDF **into a Python
   `bytes` object in request-handler memory only** — no temp file, no disk write of the raw
   upload. `input_sha256 = hashlib.sha256(contents).hexdigest()` is computed immediately
   (`server.py:16430`) and logged only as a 12-char prefix (`server.py:16431`).
3. `contents` is used in-memory for: PDF structural validation (`PdfReader`, `server.py:16434`),
   digital text extraction (`_extract_pdf_text_digital`, `server.py:16540`, def at
   `server.py:16185`), and — when digital coverage is low — a Document AI OCR fallback that
   sends the bytes (or a page subset built in-memory via `_build_pdf_subset_for_pages`,
   `server.py:16296`, def at `server.py:16296`) to Google Document AI (`backend/document_ai.py`).
   No stage writes the original bytes to disk.
4. `run_pipeline()` is `await`-ed synchronously with a timeout (`server.py:16673`,
   `PIPELINE_TIMEOUT_SECONDS`, `server.py:126`) inside the same request handler, so `contents`
   stays in scope for the whole request lifetime — nothing async/backgrounded holds a separate
   reference to it.
5. Correctness v2 job artifacts (`backend/correctness_v2/artifacts.py:1-58`) persist derived
   JSON (`input_pages.json`, `analyst_worksheet.json`, `canonical_verdict.json`, etc.) under
   `{ARTIFACTS_ROOT}/jobs/{job_id}/` — **the original PDF is explicitly absent from this list**
   (`artifacts.py:1-13` enumerates every file the job folder ever contains; no PDF).
6. The `PeriziaAnalysis` Mongo record (`server.py:342-354`, inserted at `server.py:16969`)
   persists `input_sha256`, `file_name`, `pages_count`, the structured `result`, and up to
   100,000 chars of extracted `raw_text` (`server.py:16949`) — **but never the original bytes**.
7. After the response is returned, `contents` goes out of scope and is garbage-collected. There
   is no code path, feature flag, admin tool, or backup job in the current codebase that writes
   the original PDF to disk, Mongo, or any object store. `grep`-verified: no `StaticFiles`/
   `app.mount` exists anywhere in `server.py` (no web-served filesystem path exists at all
   today), and the `uploads/` directory at the repo root is dev/test fixture material never
   referenced by `server.py`.

**Net: today, "delete-after-processing" is not a policy decision anyone made — it is simply the
absence of any retention code.** This plan adds retention as an explicit, opt-in, audited,
time-bounded exception to that default, never changing the default itself.

One adjacent fact, noted for completeness and explicitly **out of scope**: `raw_text` (extracted
text, not the original bytes) is already stored in Mongo unconditionally today
(`server.py:16949`). This plan does not touch that; it governs only the original PDF *bytes*.

---

## 2. Full trace of surfaces to be touched

| Surface | File:line | Role in this plan |
|---|---|---|
| Upload handler | `server.py:16393` (`analyze_perizia`) | New optional consent form field; retention-attempt call once validity/credit gates pass |
| Ownership pattern (Mongo doc, DB-level) | `server.py:19381` (`_get_perizia_analysis_for_user_with_storage`) — `db.perizia_analyses.find_one({"analysis_id":..., "user_id": user.user_id})` | Mirrored exactly by every new ownership-scoped endpoint |
| Ownership pattern (Correctness v2 API) | `correctness_v2/api.py:433` (`_resolve_customer_access`) | Second precedent for the same ownership idiom; mirrored by the admin-vs-customer split |
| Auth | `server.py:1270` (`require_auth`), `server.py:1285` (`require_master_admin`) | Reused unchanged — no new auth mechanism |
| Feature-flag idiom | `correctness_v2/feature_flags.py:1-140` | New flag module mirrors this exactly (env-at-call-time, safe defaults, never raises) |
| Artifact-root override idiom | `correctness_v2/artifacts.py:68-77` (`CORRECTNESS_V2_ARTIFACTS_ROOT`) | Mirrored by a new `CORRECTNESS_V2_PDF_RETENTION_ROOT` |
| Atomic-write idiom | `correctness_v2/artifacts.py:88-94` (`_write_json`, temp file + `os.replace`) | Generalized to binary blob writes |
| Standalone-Mongo, no-transaction, single-doc-conditional idiom | `beta_program/quota.py:1-27` | Reused for consent/blob/deletion state transitions |
| Current-state + append-only-audit collection split | `beta_program/store.py:30-32, 96-170` (`MEMBERSHIPS_COLLECTION` + `AUDIT_COLLECTION`, `_write_audit`) | Mirrored by `pdf_retention_consents`/`pdf_retention_blobs` + `pdf_retention_audit` |
| Safe-metadata-only telemetry contract | `beta_program/signals.py:1-20` | Mirrored: audit rows never carry PDF content, plaintext keys, or full local paths beyond what's needed to locate (never serve) the ciphertext |
| Secret store outside Git | `/etc/periziascan/` (root:root, 0700) + `/etc/systemd/system/periziascan-backend.service.d/auth-email.conf` (`EnvironmentFile=/etc/periziascan/auth_email.env`) | Exact precedent reused for the encryption key |
| Existing per-analysis delete | `server.py:20279-20293` (`delete_perizia_analysis`) | Must additively cascade to any retained blob (see §7) |
| Existing bulk delete | `server.py:20327-20354` (`delete_all_history`) | Same cascade |
| Existing account-state → frontend flags | `server.py:1079` (`_normalize_account_state`), `server.py:1326` (`_feature_access_flags`) | Add one read-only boolean so the frontend knows whether to offer the consent checkbox, with no new public flags endpoint |
| Encryption library already vendored | `backend/requirements.txt` — `cryptography==46.0.3` (installed: 49.0.0) | `cryptography.hazmat.primitives.ciphers.aead.AESGCM` used directly; no new dependency |
| No existing in-process scheduler | confirmed by grep: only `@app.on_event("startup")` hooks (`server.py:21090, 21110, 21192`); no cron/APScheduler in the app | TTL sweep is a standalone script + systemd timer, not new in-process scheduling logic |
| No web-served static path | confirmed: no `StaticFiles`/`app.mount` in `server.py`; nginx (`/etc/nginx/sites-available/api-periziascan`) only reverse-proxies to `127.0.0.1:8081`, no `root`/`alias` | Storage location constraint is satisfied by construction as long as no future code adds a static mount over the blob root |
| Frontend upload | `frontend/src/pages/NewAnalysis.js:219-256` (`handleUpload`, builds `FormData`, posts to `/api/analysis/perizia`) | Consent checkbox appended to the same `FormData` |
| Frontend delete affordance precedent | `frontend/src/pages/History.js:144-158` (`handleDeletePerizia`, calls `DELETE /api/analysis/perizia/{id}`) | Pattern mirrored for the new "delete retained PDF" control |
| Frontend privacy copy | `frontend/src/pages/Privacy.js:19,30-31` — already has generic retention/deletion-request language | Extended with concrete consent/TTL wording, not redesigned |
| Frontend account page | `frontend/src/pages/Profile.js` (148 lines) | Narrow new "manage retained data" section |

---

## 3. Data model

Three new Mongo collections (standalone mongod, no transactions — see §7 for ordering), one new
disk root, one new secret file. Nothing added to any existing collection's schema in a
non-additive way.

### 3.1 `pdf_retention_consents` (current state, one doc per `analysis_id`)

```
{
  "consent_id":       "pdfc_<uuid16hex>",
  "analysis_id":       str,            # unique index
  "case_id":           str,
  "user_id":            str,            # owner, indexed
  "scope":             "DIAGNOSTIC_RETENTION_V1",   # closed vocabulary, versioned
  "policy_version":    "v1",           # text/version of the consent copy shown to the user
  "status":            "GRANTED" | "WITHDRAWN",
  "granted_at":        iso8601,
  "withdrawn_at":       iso8601 | null,
  "created_at":        iso8601,
  "updated_at":        iso8601
}
```
Lives in **Mongo**. Mirrors `beta_program_memberships` (`beta_program/store.py:30`,
`uq_beta_membership_email` unique index) — here unique on `analysis_id`.

### 3.2 `pdf_retention_consent_audit` (append-only)

```
{
  "consent_audit_id": "pdfca_<uuid16hex>",
  "analysis_id": str,
  "user_id": str,
  "action": "CONSENT_GRANTED" | "CONSENT_WITHDRAWN",
  "before_status": str | null,
  "after_status": str,
  "actor_type": "CUSTOMER" | "SYSTEM",
  "created_at": iso8601
}
```
Mirrors `beta_program_audit` (`beta_program/store.py:31,96-127`, `_write_audit` — "insert one
append-only audit row, never blocks the mutation on failure").

### 3.3 `pdf_retention_blobs` (one per retained original, unique on `analysis_id`)

```
{
  "blob_id":            "pdfb_<uuid32hex>",     # opaque, never derived from analysis_id
  "analysis_id":        str,                    # unique index
  "case_id":            str,
  "user_id":             str,                    # owner, indexed
  "correctness_v2_job_ids": [str, ...],          # appended best-effort as jobs are created; NOT
                                                  # known at upload time (see note below)
  "sha256":             str,                     # hex; must equal PeriziaAnalysis.input_sha256
  "size_bytes":         int,
  "mime_type":          "application/pdf",
  "encryption": {
    "scheme":            "AES-256-GCM-ENVELOPE-V1",
    "kek_id":             str,                    # non-secret identifier of the KEK used
    "wrapped_dek_b64":     str,                    # DEK ciphertext, wrapped by the KEK
    "wrap_nonce_b64":      str,
    "content_nonce_b64":   str
  },
  "storage": {
    "relative_path":     "blobs/<blob_id[0:2]>/<blob_id[2:4]>/<blob_id>.enc"
  },
  "consent_id":          str,                    # FK -> pdf_retention_consents
  "created_at":          iso8601,
  "ttl_days":            int,
  "expires_at":          iso8601,                # created_at + ttl_days, computed once, indexed
  "deletion_state":      "ACTIVE" | "PENDING_DELETE" | "DELETED",
  "claimed_at":          iso8601 | null,          # sweep-worker claim timestamp (stale recovery)
  "deleted_at":          iso8601 | null,
  "deletion_reason":     "TTL_EXPIRED" | "CUSTOMER_ERASURE" | "CONSENT_WITHDRAWN"
                        | "ANALYSIS_DELETED" | "ADMIN_REQUEST" | null
}
```
The *encrypted bytes themselves* live on **disk**, never in Mongo (keeps documents small,
matches the existing pattern of never putting large binary payloads in `perizia_analyses`). The
Mongo record is metadata + a pointer only.

**Note on `correctness_v2_job_ids`:** `analysis_id`/`case_id` are minted at upload time
(`server.py:16406-16408`), but the Correctness v2 `job_id` does not exist until a job starts
(`correctness_v2/api.py:307`, `correctness_v2_start`, sometimes auto-started, sometimes
admin-triggered later). Retention must not be gated on a job existing — lineage to `analysis_id`
is the only hard requirement; job IDs are attached opportunistically and are informational only.

### 3.4 `pdf_retention_audit` (append-only, lifecycle + access log)

```
{
  "audit_id":       "pdfa_<uuid16hex>",
  "blob_id":         str | null,
  "analysis_id":     str,
  "action":         "RETAINED" | "ACCESSED" | "DELETION_STARTED" | "DELETION_COMPLETED"
                    | "DELETION_FAILED" | "ORPHAN_FILE_PURGED",
  "actor_type":      "CUSTOMER" | "OWNER_ADMIN" | "SYSTEM_SWEEP" | "SYSTEM_UPLOAD",
  "actor_user_id":    str | null,
  "actor_email":      str | null,           # only for OWNER_ADMIN actions
  "reason_code":     str | null,            # deletion_reason, or admin-supplied justification
  "created_at":      iso8601
}
```
Mirrors the "safe metadata only" telemetry contract of `beta_program/signals.py:1-20`: **never**
PDF content, plaintext DEK/KEK, full absolute disk path, or any excerpt of the document. The
`relative_path` stored on the blob record is structural (hash-prefix sharded, opaque `blob_id`)
and is not itself sensitive, but it is deliberately **not** duplicated into the audit collection
— the audit log answers "who did what, when, why", not "where exactly on disk".

### 3.5 Secret store (outside Mongo, outside Git)

New file `/etc/periziascan/pdf_retention.env` (root:root, 0700 directory — reuses the existing
`/etc/periziascan/` directory created for passwordless email auth), loaded via a new systemd
drop-in `/etc/systemd/system/periziascan-backend.service.d/pdf-retention.conf`:

```
[Service]
EnvironmentFile=/etc/periziascan/pdf_retention.env
```

— an exact structural copy of the existing `auth-email.conf` drop-in (`EnvironmentFile=
/etc/periziascan/auth_email.env`). Contains:

```
PDF_RETENTION_ACTIVE_KEK_ID=k1
PDF_RETENTION_KEK_K1_B64=<32 random bytes, base64>
```

This file is an **ops action**, not a repository change (`/etc/` is outside the Git working
tree). `backend/.env` (already `.gitignore`d — confirmed: `*.env` at `.gitignore:82` etc., and
`git check-ignore -v backend/.env` matches) only ever carries the three **non-secret** flags
(§5), the same way `CORRECTNESS_V2_PARTIAL_LOT_REPORTS_ENABLED` lives in `backend/.env` today
while `RESEND_API_KEY`/`AUTH_EMAIL_PEPPER` live only in `/etc/periziascan/auth_email.env`.

### 3.6 Disk layout (new root, outside every web-served path)

```
{PDF_RETENTION_ROOT}/blobs/<blob_id[0:2]>/<blob_id[2:4]>/<blob_id>.enc
```
Default root: `/srv/perizia/app/_pdf_retention` (sibling of `_correctness_v2/`, **not** nested
inside `_correctness_v2/jobs/`). Overridable via `CORRECTNESS_V2_PDF_RETENTION_ROOT`, mirroring
`CORRECTNESS_V2_ARTIFACTS_ROOT` (`artifacts.py:68-69`), so tests point it at a temp dir. Kept as
a **separate** root from `_correctness_v2/jobs/` deliberately: that tree already has its own
per-job lifecycle, its own tooling that lists/iterates job directories, and R3-05
(`docs/perizia_scan_risk_register.md:116`) already recorded an incident where ad-hoc scripts
wrote synthetic directories into `_correctness_v2/jobs/` during review. A second, narrower,
purpose-built root for encrypted originals is easier to lock down, back up, and sweep correctly
without any interaction with job-artifact tooling.

---

## 4. Consent flow

**Opt-in only. No retention without a `GRANTED` consent record written and durable *before* any
byte of the original PDF touches disk.**

### 4.1 Capture (upload time, the only point the bytes exist to retain)

`analyze_perizia` (`server.py:16393`) gains one new optional form field:

```python
retain_original_consent: bool = Form(False)
```

Default `False` — an upload that doesn't send the field behaves exactly as today. The checkbox
is unchecked by default in the UI (§14): retention is never the default, it is something the
customer actively turns on for this particular upload.

There is no retroactive consent: once an analysis was uploaded without the flag, the original
bytes were never captured and cannot be retained after the fact — consent is only ever
meaningful at the single moment the bytes are in memory.

### 4.2 Scope

One fixed scope value, `DIAGNOSTIC_RETENTION_V1`: "retain my original uploaded PDF, encrypted,
for up to the stated TTL, so PeriziaScan's owner can use it solely to diagnose and fix a
reported or detected problem with my analysis." No broader scope (e.g., "use for model
training") is introduced by this plan — GDPR purpose limitation is satisfied by having exactly
one narrow, named purpose.

### 4.3 Where retention is actually attempted

Retention is attempted once — and only once — validity and credit gates have passed (i.e., the
paid pipeline is actually about to run: after the `PdfReader` structural check at
`server.py:16434` and after the credit/beta-quota checks at `server.py:16442-16513`), and before
`run_pipeline()` is awaited (`server.py:16673`). This is a deliberate minimization choice: a
PDF that never reached the pipeline (wrong file type, unreadable, insufficient credits) has no
diagnostic value and is not retained even if the consent field was sent, keeping the "retain
only what's needed" contract honest. A PDF that *does* enter the pipeline and later turns out
`UNREADABLE` or errors mid-processing is still retained if consented — that is precisely the
scenario retention exists for.

The call:

```python
await pdf_retention.ingest.retain_if_consented(
    analysis_id=analysis_id, case_id=case_id, user_id=user.user_id,
    contents=contents, input_sha256=input_sha256,
    consent_requested=retain_original_consent,
)
```

is wrapped in `asyncio.to_thread` + a short bounded `asyncio.wait_for` (a dedicated
`PDF_RETENTION_TIMEOUT_SECONDS`, default 10s — independent of `PIPELINE_TIMEOUT_SECONDS`,
`server.py:126`) inside a broad `try/except` that only logs. **A retention failure, timeout, or
disabled flag can never fail, delay materially, or change the outcome of the customer's
analysis** — mirroring the hard rule already stated for `emit_v2_job_event`
(`beta_program/signals.py:9-14`, "must never alter pipeline output... never delays the
analysis"). Worst case on any retention error: identical to today (no original retained).

### 4.4 Withdrawal / erasure

`DELETE /api/analysis/perizia/{analysis_id}/retained-pdf` (new, ownership-checked exactly like
`server.py:20279-20293`): sets consent `status=WITHDRAWN`, writes a
`pdf_retention_consent_audit` row, and calls the same `erasure.erase_for_analysis(...)` used by
the TTL sweep's deletion path (§7) with `reason=CONSENT_WITHDRAWN`. Idempotent: calling it when
there is no active consent/blob still returns `200 {"retained": false}` — "no retained PDF"
satisfies the withdrawal/erasure request regardless of whether one ever existed, so there is
never a reason to 404 a right-to-erasure call.

### 4.5 UI touchpoints (narrow — see §14)

- `NewAnalysis.js`: one checkbox near the upload button.
- `Profile.js`: one "manage retained data" section — status + a delete/withdraw button.
- `Privacy.js`: a few sentences of concrete policy text.

---

## 5. Encryption + key management

**Scheme: envelope encryption, AES-256-GCM at both layers, using the already-vendored
`cryptography` library (`cryptography==46.0.3` pinned, `49.0.0` installed) —
`cryptography.hazmat.primitives.ciphers.aead.AESGCM`. No new dependency.**

1. On retention: generate a fresh random 256-bit Data Encryption Key (DEK) with `os.urandom(32)`
   for **this blob only**.
2. Encrypt the PDF bytes with `AESGCM(dek).encrypt(content_nonce, contents, aad)`, where
   `content_nonce` is a fresh random 96-bit nonce and `aad = f"{blob_id}:{sha256}".encode()`
   (binds ciphertext to its own identity — defense against a swapped-metadata attack, not a
   correctness requirement).
3. Wrap the DEK: `AESGCM(kek).encrypt(wrap_nonce, dek, blob_id.encode())` using the **active**
   Key-Encryption-Key (KEK) read from `PDF_RETENTION_KEK_<active_id>_B64`
   (`/etc/periziascan/pdf_retention.env`, §3.5).
4. Persist `kek_id`, `wrapped_dek_b64`, `wrap_nonce_b64`, `content_nonce_b64` on the Mongo blob
   record (§3.3); persist only the encrypted PDF bytes (`.enc`) on disk (§3.6).
5. The plaintext DEK and the decrypted PDF exist **only transiently in process memory** during
   encrypt (retention) or decrypt (an authorized read) — never logged, never written unencrypted
   anywhere, never included in any audit row (§3.4 contract).

**Key management / rotation:** `pdf_retention/config.py` reads every
`PDF_RETENTION_KEK_<id>_B64` env var present into a small in-memory keyring
`{kek_id: raw_bytes}`, and `PDF_RETENTION_ACTIVE_KEK_ID` selects which entry wraps *new* DEKs.
Decrypting an existing blob looks up its own stored `kek_id` in the keyring — so rotating the
active KEK (add a new `PDF_RETENTION_KEK_K2_B64`, flip `PDF_RETENTION_ACTIVE_KEK_ID=k2`, restart
via the ops runbook already established for `/etc/periziascan/*.env` changes) only affects newly
retained blobs; older blobs keep decrypting under their original KEK as long as it remains in
the keyring. A future rewrap-on-rotation sweep (re-encrypt the DEK, not the PDF content, under
the new KEK for every blob still `ACTIVE`) is a natural follow-up but is **not required for v1**
and is called out as future work, not a blocker.

**No secret material ever appears in:** Git (key lives only in `/etc/periziascan/`, root-owned,
0700 — the exact precedent used for the OTP pepper/API key), `backend/.env` (repo-adjacent but
gitignored — only non-secret flags live there), Mongo (only wrapped DEK ciphertext + non-secret
`kek_id`), telemetry/audit rows (§3.4), or application logs (retention/access code paths log
`blob_id`/`analysis_id`/outcome only, mirroring `server.py:16431`'s existing practice of logging
only a truncated sha256 prefix, never raw content or key material).

---

## 6. Storage layout + the authenticated ownership-scoped audited access endpoint

Storage layout: §3.6. No `StaticFiles`/`app.mount` is ever added over `_pdf_retention/` — the
only way to reach the bytes is through FastAPI application code, which always authenticates,
always checks ownership, always decrypts server-side, and always audits.

### 6.1 Customer read path

`GET /api/analysis/perizia/{analysis_id}/retained-pdf/status` — metadata only (`retained`,
`consent status`, `expires_at`, `deletion_state`); ownership-checked exactly like
`_get_perizia_analysis_for_user_with_storage` (`server.py:19381-19400`): `404` unless
`db.perizia_analyses.find_one({"analysis_id": analysis_id, "user_id": user.user_id})` matches
(admin bypasses). No audit row needed for a metadata-only read (no bytes disclosed).

`GET /api/analysis/perizia/{analysis_id}/retained-pdf` — the actual bytes. Same ownership check,
then: look up the `pdf_retention_blobs` row, require `deletion_state == "ACTIVE"` (a
`PENDING_DELETE`/`DELETED` blob 404s exactly as if it never existed — no "tombstone" leak),
decrypt server-side, stream back `Content-Type: application/pdf`,
`Content-Disposition: attachment; filename="original.pdf"` (never the customer's real filename,
to avoid re-embedding PII into a response header unnecessarily), and write one
`pdf_retention_audit` row (`action=ACCESSED`, `actor_type=CUSTOMER`).

### 6.2 Owner/admin diagnostic path (§9 for full detail)

`GET /api/admin/pdf-retention/{analysis_id}?reason=<free text>` — `require_master_admin`
(`server.py:1285`) gated, **requires** a non-empty `reason` string (stored verbatim in the audit
row, capped length, control-char stripped mirroring `beta_program/store.py:96-104`
`_sanitize_text`). No ownership check beyond admin (by design — this is the deliberate, audited,
narrow diagnostic exception), but every access is logged with `actor_type=OWNER_ADMIN`,
`actor_email`, and the stated `reason_code`.

No path anywhere accepts a raw `blob_id` or file path from the client — the only client-supplied
identifier is `analysis_id`, which is always resolved server-side through the ownership-checked
Mongo lookup before the `blob_id`/disk path is ever touched. `blob_id` itself is an opaque
`uuid4().hex`, never derived from `analysis_id`/`case_id`/`user_id`, so even a future bug that
exposed the storage layout would not let anyone guess a blob's identity from an analysis they
don't own.

---

## 7. TTL + automatic deletion + deletion audit

**Mechanism: a standalone script (`backend/pdf_retention/scripts/pdf_retention_sweep.py`,
mirroring the existing `backend/scripts/*.py` / `correctness_v2/scripts/*.py` convention) run
periodically by a new systemd timer unit** (`periziascan-pdf-retention-sweep.timer` +
`.service`, ops-level, outside the repo) — **not** an in-process scheduler, since the app has no
existing precedent for one (confirmed §2: only `@app.on_event("startup")` hooks exist) and a
systemd timer survives backend restarts independently and is trivially observable
(`systemctl status`, `journalctl`).

### 7.1 Write-side ordering (retention)

Standalone mongod, no multi-document transactions (established precedent:
`beta_program/quota.py:8-12`, "the verified Mongo topology... makes multi-document transactions
unavailable, so no transaction is used or required anywhere here"). Rule: **disk bytes durable
before the Mongo pointer record exists — never the reverse.**

1. Confirm a `GRANTED` consent row exists for `analysis_id` (read).
2. Encrypt in memory (§5).
3. Write ciphertext to a `.tmp` path under the target shard directory, then `os.replace` to the
   final `.enc` path — the same atomic temp-file-then-`os.replace` idiom already used for JSON
   artifacts (`correctness_v2/artifacts.py:88-94`, `_write_json`), generalized to binary.
4. **Only after** step 3 succeeds: `find_one_and_update(analysis_id, upsert=True,
   $setOnInsert={...})` on `pdf_retention_blobs` — a single-document conditional upsert, the same
   idiom as `beta_program/quota.py`'s reservation calls, keyed on the unique `analysis_id` index
   so a retried retention attempt (e.g. the request handler races or is retried) cannot create a
   duplicate blob for the same analysis.
5. If step 4 fails after step 3 succeeded: the result is an orphaned `.enc` file with no Mongo
   pointer — inert ciphertext, undecryptable and unreachable through any API (no code path ever
   resolves a blob except via its Mongo record), not a leak. The sweep script's orphan pass
   (§7.3) removes any `.enc` file older than 24h with no matching `pdf_retention_blobs.storage.
   relative_path`, and audits `ORPHAN_FILE_PURGED`.
6. If step 1 fails to find a `GRANTED` consent, retention is skipped entirely — no disk write
   ever happens. This ordering makes "retained without consent" structurally impossible: the
   consent check gates step 2, and nothing before step 2 touches disk or Mongo for this feature.

### 7.2 TTL sweep (deletion-side ordering)

Rule: **never let the database claim `DELETED` before the bytes are actually gone; a transient
inconsistency must always favor "bytes might still exist a bit longer", never "DB says gone but
isn't."**

1. Indexed query: `deletion_state == "ACTIVE" AND expires_at <= now()`.
2. Atomically claim one row: `find_one_and_update({blob_id, deletion_state: "ACTIVE"},
   {$set: {deletion_state: "PENDING_DELETE", claimed_at: now()}})` — single-document
   conditional update, so two sweep runs (or a sweep racing a customer erasure call) can never
   both claim the same row.
3. Audit `DELETION_STARTED` (`actor_type=SYSTEM_SWEEP`, `reason_code=TTL_EXPIRED`).
4. `os.remove` the `.enc` file; a `FileNotFoundError` is treated as success (idempotent — the
   file may already be gone from a prior partial run).
5. `find_one_and_update({blob_id, deletion_state: "PENDING_DELETE"}, {$set:
   {deletion_state: "DELETED", deleted_at: now(), deletion_reason: "TTL_EXPIRED"}})`.
6. Audit `DELETION_COMPLETED`.
7. If step 4 raises (disk error): row stays `PENDING_DELETE`; audit `DELETION_FAILED` (never
   silently swallowed). A **stale-claim recovery pass** — indexed on `deletion_state ==
   "PENDING_DELETE" AND claimed_at <= now() - STALE_CLAIM_THRESHOLD` (default 1h, bounded,
   never a full collection scan — mirroring `beta_program/quota.py`'s documented "stale-
   reservation crash recovery (indexed, bounded, never a full scan)") — retries steps 4-6 on the
   next sweep run.

A crash between steps 2 and 5 therefore always leaves the system in `PENDING_DELETE` (bytes
possibly still present — safe, expected, self-healing on the next run), never in a state where
Mongo says `DELETED` while the ciphertext still exists on disk.

### 7.3 Erasure / withdrawal (§4.4) and the existing-delete cascade (below) reuse this exact same
state machine, only differing in the caller and `deletion_reason`. Idempotent by construction:
calling erasure on an already-`DELETED` or never-existed blob is a no-op success.

### 7.4 Cascading from the existing account-deletion surfaces

`server.py:20279-20293` (`delete_perizia_analysis`) and `server.py:20327-20354`
(`delete_all_history`) **today only delete the `perizia_analyses` Mongo document** — they do not
touch `_correctness_v2/jobs/` artifacts and, without an additive change, would not touch a
retained PDF blob either. That is a real trust gap this feature must close: a customer who
clicks "Elimina" today reasonably believes their data is gone; if a consented, still-TTL-pending
retained original existed and were left untouched, that belief would be false. This plan adds
one small, guarded call to each of those two existing endpoints:

```python
try:
    await pdf_retention.erasure.erase_for_analysis(
        analysis_id, reason="ANALYSIS_DELETED", actor_type="SYSTEM"
    )
except Exception:
    logger.warning(...)  # never blocks or fails the existing delete
```

— additive, feature-flag-safe (a no-op when the flag is off or no blob exists for that
`analysis_id`), never raising into the existing delete flow, mirroring the "never blocks the
mutation on failure" idiom already established for `beta_program/store.py`'s `_write_audit`.
This is the one place this plan touches existing endpoint *bodies* rather than only adding new
files/routes, and it is called out explicitly here and in §13/§15.

---

## 8. GDPR / data-minimization posture + right-to-erasure

- **Retention-by-default = OFF.** No original PDF is ever retained unless the customer
  affirmatively checks the box on that specific upload (§4.1). The master feature flag also
  defaults OFF (§10), so even the *option* to consent doesn't exist until explicitly enabled.
- **Purpose limitation:** one named purpose (`DIAGNOSTIC_RETENTION_V1`, §4.2) — diagnosing and
  fixing reported/detected problems. No secondary use (training, marketing, resale) is
  introduced.
- **Minimization at capture:** retention is skipped for uploads that never entered the pipeline
  (§4.3) — nothing is retained "just in case" for a rejected upload.
- **Minimization in storage:** the Mongo blob record stores identifiers and encryption metadata
  only; the customer's real filename is deliberately **not** duplicated onto the blob record
  (it already exists once, on `PeriziaAnalysis.file_name`, `server.py:350`) and is never used as
  a disk path component (§3.6, opaque `blob_id`).
- **Storage limitation:** every blob carries a mandatory, bounded `ttl_days` and a computed
  `expires_at` (§3.3) — there is no code path that creates a blob without a TTL (§10, invariant
  4).
- **Right to erasure:** `DELETE /api/analysis/perizia/{analysis_id}/retained-pdf` (§4.4) lets the
  customer remove the retained original ahead of TTL at any time, audited, idempotent. Deleting
  the analysis itself (existing endpoints, §7.4) also erases the blob.
- **Right to access/portability:** the customer can always re-download their own retained
  original via §6.1 — the same bytes they uploaded, decrypted on demand, never a proprietary
  format.
- **Transparency:** `Privacy.js` (§14) states the purpose, the TTL, and how to withdraw, in
  plain language, alongside the existing generic retention/deletion copy already present there
  (`frontend/src/pages/Privacy.js:19,30-31`).
- **No indefinite retention by default:** enforced structurally, not by policy alone — see
  invariant 4 (§10) and the TTL sweep (§7.2), which is the *only* thing standing between a
  blob and `DELETED`; there is no "keep forever" flag.

---

## 9. Owner/admin diagnostic access, with audit

Who: only `require_master_admin` (`server.py:1285`) — the exact-configured owner/admin email,
same gate already used for every other owner-only diagnostic surface in this codebase (e.g.
`correctness_v2_start`, `correctness_v2/api.py:307`, is `_resolve_user_and_guard`-gated with
`is_admin_only()` from `feature_flags.py:75-76`).

What: `GET /api/admin/pdf-retention/{analysis_id}?reason=<text>` (§6.2) — the decrypted original
PDF, streamed. No listing/browsing endpoint is added (no "show me every retained PDF" surface;
the admin must know the specific `analysis_id` they are investigating, matching how every other
admin diagnostic route in this codebase already works, e.g.
`correctness_v2/api.py:343-425`, one `analysis_id`/`job_id` at a time).

Logged how: one `pdf_retention_audit` row per access (§3.4) —
`action=ACCESSED, actor_type=OWNER_ADMIN, actor_email=<owner email>,
reason_code=<the supplied justification>, created_at=<now>`. The `reason` query parameter is
**required** (empty/whitespace-only → `422`) and capped/sanitized exactly like
`beta_program/store.py:96-104` (`_sanitize_text`, control-char stripping, length cap) before
being stored. This is the "who, what, logged how" contract required by the mission: who = the
authenticated admin's email; what = which analysis's original PDF; logged how = an immutable,
queryable Mongo audit row, never deletable by the same erasure/TTL machinery that governs the
blob itself (`pdf_retention_audit` documents are never targeted by the sweep — only
`pdf_retention_blobs` rows and their disk files are).

---

## 10. Numbered testable invariants

1. **No retention without consent.** A blob row/disk file for a given `analysis_id` can only be
   created after `pdf_retention_consents.status == "GRANTED"` for that `analysis_id` is
   confirmed (§7.1 step 1); every retention unit test asserts zero disk writes and zero Mongo
   inserts when consent is absent or `WITHDRAWN`.
2. **No plaintext at rest.** Every byte written under `{PDF_RETENTION_ROOT}/blobs/` is
   AES-256-GCM ciphertext; a test decrypts a fixture PDF and asserts the on-disk bytes are
   neither the plaintext nor a substring of it.
3. **No public/predictable path.** No `StaticFiles`/`app.mount` is added over
   `PDF_RETENTION_ROOT` (grep-enforced in CI/test); the only way to reach bytes is through an
   authenticated, ownership-checked route; `blob_id` is `uuid4().hex`, never a function of
   `analysis_id`/`case_id`/`user_id`.
4. **TTL always set + enforced.** Every `pdf_retention_blobs` row has a non-null `expires_at`
   at creation (schema/test enforced); a blob whose `expires_at` has passed is unreachable via
   the customer read path (§6.1, `deletion_state != ACTIVE` → 404) even before the sweep has run
   physically — TTL expiry gates *access*, the sweep just reclaims the bytes.
5. **Deletion is audited.** Every `deletion_state` transition to `DELETED` has a corresponding
   `pdf_retention_audit` row with `action in {DELETION_COMPLETED}` and a `reason_code`.
6. **Erasure is honored ahead of TTL.** `DELETE .../retained-pdf` on a blob with
   `expires_at` far in the future still deletes the bytes immediately (not deferred to the next
   sweep) and flips `deletion_state` to `DELETED` synchronously within the request.
7. **Tenant isolation.** A user cannot read, list-status, or erase another user's retained PDF —
   every non-admin route 404s (never a 403 that would confirm existence) exactly like
   `_get_perizia_analysis_for_user_with_storage` does today (`server.py:19399`).
8. **Flag-OFF == today's behavior.** With `CORRECTNESS_V2_PDF_RETENTION_ENABLED=false`: the
   consent field is accepted but ignored, no consent/blob/audit record is ever created, no new
   route is reachable except a `404 PDF_RETENTION_DISABLED`, and the upload endpoint's response
   is byte-for-byte identical to pre-feature output (proven the same way Branch 2's flag-OFF
   parity was proven — base golden tests unchanged).
9. **No secret material in Git, telemetry, or logs.** `grep`-enforced: no `PDF_RETENTION_KEK`
   value, wrapped or unwrapped DEK, or decrypted PDF byte ever appears in a log line, a
   `pdf_retention_audit`/`pdf_retention_consent_audit` document, or any tracked file.
10. **Disk-before-Mongo on write, Mongo-flips-to-PENDING-before-disk-delete on delete.** The
    ordering in §7.1/§7.2 is directly unit-testable by injecting a failure after each step and
    asserting the resulting state is always the "safer" one (orphan ciphertext, never a phantom
    Mongo pointer with no bytes; `PENDING_DELETE`, never a false `DELETED`).
11. **Idempotency.** Re-running the sweep, re-calling erasure, or retrying a retention attempt
    for the same `analysis_id` never creates duplicate blob rows (unique index) and never errors
    on an already-deleted blob.
12. **Existing delete cascades.** Deleting an analysis via the existing
    `DELETE /api/analysis/perizia/{analysis_id}` or `DELETE /api/history/all` endpoints (§7.4)
    always also erases any associated retained blob when the flag is on, and is a documented
    no-op when the flag is off (parity preserved).
13. **Owner access is always justified and logged.** The admin diagnostic route rejects an
    empty/whitespace `reason` (`422`) and writes exactly one audit row per successful access,
    with the real admin email attached (never a generic "system" actor for a human-triggered
    read).
14. **Correctness pipeline untouched.** `fact_lineage.py`, `lot_fact_projection.py`,
    `verdict_model.py`, `partial_report.py`, `orchestrator.py`, `lot_runner.py`,
    `CORRECTNESS_V2_LOT_CONCURRENCY` are byte-identical / untouched by this branch (grep-diff
    enforced against base, mirroring how every prior branch's plan enforced this same
    invariant).

---

## 11. Backward compatibility + flag-based rollback

- Master flag `CORRECTNESS_V2_PDF_RETENTION_ENABLED` (default `false`) gates every new route,
  every consent-capture side effect, and every retention attempt — mirrors
  `correctness_v2/feature_flags.py`'s `is_enabled()`/`_env_bool` idiom exactly
  (`feature_flags.py:34-46`).
- Rollback = flip the flag back to `false` and restart (same operational pattern already used
  for `CORRECTNESS_V2_PARTIAL_LOT_REPORTS_ENABLED`, per
  `docs/perizia_scan_quality_recovery_program.md` §P0.4 deployment record). No data migration is
  required to roll back: existing `pdf_retention_*` collections and any already-retained blobs
  are simply no longer reachable through the (now-disabled) API — they remain governed by their
  own TTL until it is safe to re-enable, or can be erased by an owner-run one-off script if the
  rollback is permanent.
- Flag-OFF customers see no UI change (§14 — the consent checkbox itself is only rendered when
  `feature_access.pdf_retention_offer_enabled` is `true`, §12) and no behavior change in the
  upload/delete endpoints beyond a no-op internal call.
- No auth mechanism, billing/Stripe code, prompt, model, provider, or concurrency setting is
  touched (`CORRECTNESS_V2_LOT_CONCURRENCY` stays untouched per the programme's standing
  constraint).

---

## 12. Test plan (offline/sanitized fixtures only — no real PDFs, no network, no paid calls, no production writes)

All new tests live under `backend/pdf_retention/tests/` and reuse the existing test-isolation
idiom (`CORRECTNESS_V2_ARTIFACTS_ROOT` pointed at a pytest `tmp_path`, mirrored here as
`CORRECTNESS_V2_PDF_RETENTION_ROOT`) plus a real-but-throwaway Mongo test database — matching how
every prior branch's suite was structured (`backend/conftest.py:1-4492`; `beta_program` tests use
the same real-Mongo-not-mocked convention per `project_beta_program_admin`/`project_beta_perizia_limits`
memory notes).

1. **Unit — crypto (`test_crypto.py`):** encrypt/decrypt round-trip with a small synthetic byte
   string (never a real PDF); tampered ciphertext fails AEAD verification; wrong KEK fails to
   unwrap; keyring rotation (old blob still decrypts under its original `kek_id` after the active
   KEK changes).
2. **Unit — consent (`test_consent.py`):** grant/withdraw state machine; withdrawal triggers
   erasure; double-withdraw is idempotent; consent-absent blocks retention (invariant 1).
3. **Unit — ordering (`test_write_ordering.py`):** injected failure after disk-write-before-
   Mongo-insert leaves an orphan file, never a phantom Mongo row without bytes (invariant 10);
   injected failure after Mongo-claim-before-disk-delete leaves `PENDING_DELETE`, never a false
   `DELETED`.
4. **Integration — retention end-to-end (`test_ingest_integration.py`):** a fake small
   in-memory "PDF" (arbitrary bytes, not a real appraisal) posted through a mocked
   `retain_if_consented` call path; asserts a `pdf_retention_blobs` row + encrypted file exist,
   `sha256` matches `PeriziaAnalysis.input_sha256`, `expires_at` is set.
5. **Deletion / TTL test (`test_ttl_sweep.py`):** create a blob with `expires_at` in the past;
   run `sweep.run_once()`; assert the `.enc` file is gone, `deletion_state == DELETED`,
   `deletion_reason == TTL_EXPIRED`, and exactly one `DELETION_COMPLETED` audit row exists.
   Second run is a no-op (idempotency, invariant 11). Stale-`PENDING_DELETE` recovery test:
   pre-seed a row claimed long ago with the file still present; assert the next sweep completes
   it.
6. **Tenant-isolation test (`test_ownership.py`):** two synthetic users, two analyses; user A's
   token against user B's `analysis_id` on every new route (status, download, erase, and the
   admin route without admin privileges) always 404s/403s, never leaks existence or bytes.
7. **Consent-absent-no-retention test (`test_no_consent_no_retention.py`):** upload flow
   simulated with `retain_original_consent=False` (and with the field omitted entirely); asserts
   zero `pdf_retention_blobs` documents, zero files under the retention root, and that the
   upload response is unchanged from the pre-feature baseline (invariant 8 support).
8. **Flag-OFF parity test (`test_flag_off_parity.py`):** with the master flag unset/false, drive
   the upload + both existing delete endpoints and assert response bytes/status codes are
   identical to a captured pre-branch baseline (mirrors the byte-for-byte flag-OFF proof used for
   Branch 2's `CORRECTNESS_V2_CANONICAL_VERDICT_ENABLED`).
9. **Cascade test (`test_delete_cascade.py`):** an analysis with an `ACTIVE` retained blob is
   deleted via the existing `DELETE /api/analysis/perizia/{analysis_id}`; assert the blob is
   erased (`deletion_reason == ANALYSIS_DELETED`) and the existing endpoint's own success
   response/behavior is unchanged.
10. **Admin-access audit test (`test_admin_access.py`):** admin download without a `reason` →
    `422`; with a `reason` → `200` + exactly one `ACCESSED`/`OWNER_ADMIN` audit row carrying the
    admin's email and the sanitized reason text.
11. **Regression:** the existing eight-case regression runner
    (`correctness_v2/scripts/run_eight_case_regression.sh`) and the full backend suite are run
    with the flag both OFF and ON to confirm zero interaction with the correctness pipeline
    (invariant 14) — no fixture PDF used for the eight-case suite is ever routed through
    retention in these regression runs (retention is exercised only by its own dedicated
    synthetic-byte tests, never real appraisal fixtures).

No test uses a real appraisal PDF, calls OpenAI/Document AI/Stripe, or writes into
`_correctness_v2/jobs/` or the production `_pdf_retention/` root (always redirected via the env
override, mirroring the R3-05 lesson recorded in the risk register).

---

## 13. Files expected to change

### New (backend)

- `backend/pdf_retention/__init__.py`
- `backend/pdf_retention/config.py` — flags, TTL default, KEK keyring reader (mirrors
  `correctness_v2/feature_flags.py` + `auth_email/config.py`)
- `backend/pdf_retention/crypto.py` — AES-256-GCM envelope encrypt/decrypt, keyring/rotation
- `backend/pdf_retention/store.py` — the three Mongo collections, indexes,
  `find_one_and_update`-based atomic transitions (mirrors `beta_program/store.py`)
- `backend/pdf_retention/blob_store.py` — disk layout, atomic temp+`os.replace` write, tolerant
  delete, orphan scan (mirrors `correctness_v2/artifacts.py`'s write idiom)
- `backend/pdf_retention/ingest.py` — `retain_if_consented(...)`, the single upload-time entry
  point, enforcing the consent-before-disk-write ordering (§7.1)
- `backend/pdf_retention/access.py` — ownership-scoped decrypt+stream+audit for the customer path
  and the admin diagnostic path
- `backend/pdf_retention/erasure.py` — `erase_for_analysis(...)`, shared by the explicit erase
  route, consent withdrawal, and the existing-delete cascade
- `backend/pdf_retention/sweep.py` — TTL sweep, stale-claim recovery, orphan-file purge; pure
  logic + a `run_once()` entrypoint
- `backend/pdf_retention/api.py` — FastAPI router: status/download/erase (customer), admin
  diagnostic download
- `backend/pdf_retention/scripts/pdf_retention_sweep.py` — thin cron entrypoint (mirrors
  `backend/scripts/*.py`)
- `backend/pdf_retention/tests/*` — §12

### Modified (backend, additive)

- `backend/server.py`:
  - `analyze_perizia` (`server.py:16393` onward): new `Form(False)` field + bounded, failure-
    tolerant retention-attempt call (§4.3).
  - `delete_perizia_analysis` (`server.py:20279-20293`) and `delete_all_history`
    (`server.py:20327-20354`): additive, guarded erasure-cascade call (§7.4).
  - `_normalize_account_state` (`server.py:1079`) / `_feature_access_flags` (`server.py:1326`):
    one additive read-only boolean (`pdf_retention_offer_enabled`) so the frontend can gate the
    consent checkbox without a new public flags endpoint.
  - `app.include_router(pdf_retention.api.router, ...)` + a startup `ensure_indexes()` call,
    mirroring the existing `correctness_v2`/`beta_program` startup wiring
    (`server.py:21056-21086`).
- `backend/.env` (gitignored, not a Git change): three new **non-secret** keys —
  `CORRECTNESS_V2_PDF_RETENTION_ENABLED` (default unset/false),
  `CORRECTNESS_V2_PDF_RETENTION_TTL_DAYS` (default e.g. 30),
  `CORRECTNESS_V2_PDF_RETENTION_ROOT` (optional override).

### New (ops, outside the Git working tree — mentioned for completeness, not a repo file)

- `/etc/periziascan/pdf_retention.env` — the KEK material (§3.5, §5).
- `/etc/systemd/system/periziascan-backend.service.d/pdf-retention.conf` — `EnvironmentFile`
  drop-in (§3.5).
- `periziascan-pdf-retention-sweep.service` + `.timer` — the TTL sweep schedule (§7).

### Explicitly not touched

`fact_lineage.py`, `lot_fact_projection.py`, `verdict_model.py`, `decision_model.py`,
`contract.py`, `customer_view.py`, `partial_report.py`, `orchestrator.py`, `lot_runner.py`,
`analyst.py`, `validator.py`, `quality_gate.py`, `coverage_audit.py`, `money_confirmation.py`,
every `perizia_canonical_pipeline/*` module, `narrator.py`, `pdf_report.py`, `document_ai.py`,
any Stripe/billing code path, `auth_email/*`, Google OAuth code,
`CORRECTNESS_V2_LOT_CONCURRENCY`, and every existing correctness_v2 job-artifact schema/file
list (`correctness_v2/artifacts.py`'s existing constants are read, never renamed or repurposed).
The historical forensic bundle
(the read-only beta-investigation bundle under `ops_backups/`, outside Git) is untouched and out of
scope.

---

## 14. Frontend scope (narrow)

- `frontend/src/pages/NewAnalysis.js` (`handleUpload`, `server.py:16393`'s client — actually
  `NewAnalysis.js:219-256`): one checkbox, rendered only when
  `user.feature_access.pdf_retention_offer_enabled` is `true` (§13), unchecked by default,
  appended to the existing `FormData` as `retain_original_consent`. A single line of explanatory
  copy ("conserva il PDF originale in forma cifrata per {TTL} giorni, solo per finalità
  diagnostiche; puoi eliminarlo in qualsiasi momento") sits next to it — no layout redesign.
- `frontend/src/pages/Profile.js` (148 lines): one new "Dati conservati" section — for each
  analysis with an `ACTIVE` retained blob (from the new status endpoint, §6.1), show the TTL
  countdown and a "Elimina PDF originale conservato" button that calls the new `DELETE`
  route, styled after the existing delete affordance already in `History.js:95-104` (`Trash2`
  icon, red button, spinner while pending).
- `frontend/src/pages/Privacy.js` (60 lines): a few sentences added near the existing generic
  retention/deletion language (`Privacy.js:19,30-31`) naming the specific purpose, the
  configurable TTL, and the self-service withdrawal control — extending, not rewriting, the
  existing copy.
- `frontend/src/lib/api/perizia.js`: two small wrapper functions
  (`getRetainedPdfStatus(analysisId)`, `deleteRetainedPdf(analysisId)`) added alongside the
  existing wrappers in that file, following its established call style.

No redesign of `History.js`, `AnalysisResult.js`, or any report-rendering component. No new page
route. No change to the upload progress/stage UI beyond the one added checkbox.

---

## 15. Red-team: how retention could leak, and how the design prevents each

| Attack / failure mode | How it could happen | Design mitigation |
|---|---|---|
| **Predictable storage path** | Blob filename derived from `analysis_id`/`case_id`/`user_id`, guessable by enumeration | `blob_id` is `uuid4().hex`, structurally independent of every tenant/lineage identifier (§3.3, §6.1); no static file mount ever exists over the root regardless (§2, §6) |
| **Missing ownership check on a new route** | A new endpoint forgets the `user_id` match and only checks auth | Every new route reuses one of the two proven ownership idioms verbatim (`server.py:19381` / `correctness_v2/api.py:433`) rather than reimplementing the check; invariant 7 + `test_ownership.py` (§12.6) exercise cross-tenant access on *every* new route, not just one |
| **Plaintext at rest** | A bug writes `contents` directly instead of the ciphertext, or the encrypt step is skipped on an error path | `blob_store.py` never accepts raw bytes — its only write function requires an already-encrypted payload from `crypto.py`; invariant 2 + `test_crypto.py`/integration test assert the on-disk bytes never equal the plaintext (§12.1, §12.4) |
| **Log/telemetry leakage** | A log line or audit `meta` field accidentally includes the PDF, the DEK, or the KEK | Mirrors the existing `beta_program/signals.py:19-20` "safe metadata only" contract exactly; `pdf_retention_audit` schema (§3.4) has a closed field set with no free-form blob for content; code review + invariant 9 grep-enforce it; upload logging already only truncates sha256 to 12 chars (`server.py:16431`) — the same discipline is required in every new log line |
| **TTL never firing** | The systemd timer is disabled, the sweep script crashes silently, or a claimed row is never retried | TTL *gates access* independently of the sweep having physically run (invariant 4 — `deletion_state`/`expires_at` checked on every read, so an overdue-but-not-yet-swept blob is already unreadable by customers); the stale-claim recovery pass (§7.2 step 7) self-heals a crashed sweep run; the sweep is a systemd `.service`+`.timer` pair, independently monitorable (`systemctl status`, `journalctl`) the same way the backend service itself already is |
| **Consent bypass** | A code path creates a blob without checking consent status first, or checks a cached/stale consent value | The consent read (§7.1 step 1) is the *first* action in `retain_if_consented`, before any encryption or I/O; there is no alternate entry point into `blob_store.py`'s write function that skips `ingest.py`; invariant 1 + `test_no_consent_no_retention.py` (§12.7) |
| **Erasure not honored** | The erase endpoint only updates Mongo state but the sweep is what actually removes bytes days later, or the existing delete endpoints don't cascade | Erasure calls the *same* delete-bytes-then-mark-deleted function the sweep uses (§7.3), synchronously, within the request — never deferred; §7.4 closes the specific gap where the two pre-existing delete endpoints (`server.py:20279`, `server.py:20327`) would otherwise silently leave a blob behind; invariant 6 + invariant 12 + `test_delete_cascade.py` (§12.9) |
| **Key exposure** | KEK committed to Git, left in `backend/.env`, or logged | KEK lives only in `/etc/periziascan/pdf_retention.env` (root-owned, 0700), loaded via a systemd `EnvironmentFile` drop-in exactly like the existing OTP pepper/Resend key (§3.5) — `backend/.env` (already gitignored) carries only non-secret flags; invariant 9 + a repo-wide grep for `PDF_RETENTION_KEK` in CI as a cheap regression guard |
| **Standalone-mongod partial states** | A crash between the disk write and the Mongo insert (or between the Mongo claim and the disk delete) leaves an inconsistent state that a naive reader trusts | §7.1/§7.2 define the exact ordering and the exact "safe" failure state for each step (orphan file over phantom pointer; `PENDING_DELETE` over false `DELETED`); orphan sweep + stale-claim recovery close both gaps automatically; invariant 10 + `test_write_ordering.py` (§12.3) inject the failure at each step and assert the safe outcome |
| **Admin path becomes a silent bulk-access backdoor** | The diagnostic route is used routinely without scrutiny because it's "just an admin tool" | `reason` is mandatory and stored (§9); no listing/enumeration endpoint exists — an admin must already know the specific `analysis_id`; every access is a separate audited event with the real admin email attached, never a shared/service credential |
| **Retention silently degrades the product** | A slow disk or Mongo write during retention delays or fails the customer's analysis response | Retention runs in a bounded `asyncio.to_thread` + `asyncio.wait_for` with its own short timeout, wrapped in a catch-all that only logs (§4.3) — mirrors the hard non-negotiable already stated for `emit_v2_job_event` (`beta_program/signals.py:9-14`) |
| **Cross-feature contamination** | Retention code touches `_correctness_v2/jobs/` and interferes with existing job-artifact tooling (echoing the R3-05 incident) | Separate storage root by design (§3.6), never nested inside `_correctness_v2/jobs/`; tests always redirect via `CORRECTNESS_V2_PDF_RETENTION_ROOT`, never write into the production artifacts tree (§12, closing remark) |

---

## Summary for the owner

Today, the original uploaded PDF is never persisted — it lives only as an in-memory `bytes`
object for the duration of one request (`server.py:16429-16673`) and is then gone. This plan
adds a fully opt-in, encrypted, TTL-bounded, audited retention path that changes nothing about
that default: flag off, or consent not given, and behavior is identical to today. The three
things most worth the owner's attention before Sol implements:

1. **§7.4 touches two existing endpoints** (`delete_perizia_analysis`,
   `delete_all_history`) — additively, to close a genuine trust gap (today's "delete my
   analysis" doesn't clean up anything on disk, and would not clean up a retained blob either
   without this hook). This is the only place the plan modifies pre-existing endpoint bodies
   rather than adding new files/routes.
2. **Key management reuses the exact `/etc/periziascan/` + systemd-`EnvironmentFile`-drop-in
   pattern already proven for passwordless email auth** — no new secret-handling mechanism is
   invented.
3. **The TTL sweep is a new systemd timer + standalone script**, not new in-process scheduling
   logic — the app has no existing scheduler precedent, and this keeps the sweep independently
   observable and restart-safe.

No product code, tests, or config were changed to produce this document — only
`docs/pdf_retention_consent_plan.md` was written.
