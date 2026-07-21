# Phase 2 runbook — install secrets, validate, then enable email OTP

**Status:** planning only. Nothing in this document has been executed.
**Preconditions (all satisfied by Phase 1, 2026-07-21):** `main` = `cbb7ab7`;
backend deployed and healthy; `uq_user_normalized_email` present; migration
applied and idempotent; capability endpoint returns `email_otp_enabled: false`;
frontend Google-only; Google login regression verified live after the migration.

**Standing constraints for the whole of Phase 2:** no secret is ever pasted into
a chat transcript, committed, or echoed to a log. Mauro is never contacted and
his membership is never read for modification or written. No release tag.

---

## 0. The ordering conflict, and how this runbook resolves it

The requested sequence asks to *verify the OTP flow* (steps 7–9) **before**
setting `AUTH_EMAIL_ENABLED=true` (step 10). Those cannot both be true of the
same process: `preflight()` refuses with `feature_disabled` until the flag is on,
so with the public backend disabled there is nothing to test.

Three ways out:

| Option | Verdict |
|---|---|
| Flip the flag in production, test, flip back | **Rejected.** Exposes a live, untested OTP endpoint to the internet during the window, and the frontend button appears for every visitor. |
| Test against a staging copy of the database | **Rejected for this purpose.** Proves the code works, but not that *production* identity data links correctly — which is the actual risk. |
| **Temporary loopback validator process** | **Recommended.** A second uvicorn on `127.0.0.1:8099`, bound to loopback only, with `AUTH_EMAIL_ENABLED=true`, against the production database. |

The recommended option gives real Resend delivery and real production identity
linking while the **public** API (`:8081`, behind nginx) stays disabled and the
deployed frontend keeps hiding the button, because the public capability
endpoint still answers `false`. Steps 7–9 run against `:8099`; step 10 then
enables the real service only after they pass.

Accept this consequence before starting: steps 7–9 **create a real production
user row** for the owner-controlled test mailbox, plus challenge and rate-limit
rows. That is the point — it is what proves account creation and linking — and
§11 covers the cleanup.

---

## 1. Install `RESEND_API_KEY` outside Git

Secrets live in `/srv/perizia/app/backend/.env`. That file is **gitignored**
(`.gitignore:114`, `*.env`), is `0600`, and is consumed twice: by systemd
(`EnvironmentFile=` in `periziascan-backend.service`) and by `load_dotenv()`
inside `server.py`.

> **Parser trap — read before generating anything.** systemd's `EnvironmentFile`
> parser is *not* dotenv. It does not understand `export`, treats `#` as a
> comment introducer, and handles quotes and `$` differently. A secret
> containing `#`, `$`, `` ` ``, `'`, `"`, or a space can silently truncate or
> fail to load, leaving preflight refusing for a reason that looks nothing like
> the real cause. **Restrict every value below to `A–Z a–z 0–9 _ - . @ < > :`
> and use no quotes.**

Run these yourself — prefix with `!` in this session so nothing is echoed back
into the transcript by me:

```bash
# Back up first; every later step's rollback depends on this.
cp -a /srv/perizia/app/backend/.env /srv/perizia/app/backend/.env.bak.$(date +%Y%m%d%H%M%S)
chmod 600 /srv/perizia/app/backend/.env.bak.*

# Append the key. Paste the value from the Resend dashboard directly into the
# editor — do not pass it as a shell argument (it would land in your history).
umask 077
nano /srv/perizia/app/backend/.env      # add: RESEND_API_KEY=re_xxxxxxxx
```

Verify without revealing it:

```bash
grep -c '^RESEND_API_KEY=' /srv/perizia/app/backend/.env          # expect 1
awk -F= '/^RESEND_API_KEY=/{print length($2)}' /srv/perizia/app/backend/.env  # expect ~30-40
grep -E '^RESEND_API_KEY=' /srv/perizia/app/backend/.env | grep -qE '[#$`"'"'"' ]' \
  && echo "UNSAFE CHARACTER — systemd may mis-parse" || echo "charset ok"
```

Use a **sending-scoped** Resend key, not a full-access one.

## 2. Generate and install `AUTH_EMAIL_CODE_PEPPER`

The pepper is what makes a stored six-digit code hash worth anything;
`MIN_PEPPER_CHARS = 32` is enforced by preflight.

```bash
# URL-safe alphabet only, so systemd cannot mis-parse it.
openssl rand -base64 48 | tr -d '/+=' | cut -c1-48
```

Append as `AUTH_EMAIL_CODE_PEPPER=<value>` in the same editor session. Verify
length only:

```bash
awk -F= '/^AUTH_EMAIL_CODE_PEPPER=/{print length($2)}' /srv/perizia/app/backend/.env  # expect 48
```

**Rotation semantics:** changing the pepper invalidates every outstanding
challenge (their hashes stop matching). Harmless now — `auth_email_challenges`
is empty — but after go-live a rotation must be treated as "every in-flight code
is dead", which is recoverable (users request a new code) but should not be done
casually. Never rotate the pepper and enable the feature in the same restart.

## 3. Install `AUTH_EMAIL_FROM` and `AUTH_EMAIL_PROVIDER`

```
AUTH_EMAIL_PROVIDER=resend
AUTH_EMAIL_FROM=Perizia Scan <accesso@auth.nexodify.com>
```

`AUTH_EMAIL_FROM` contains spaces and angle brackets. If systemd mis-parses it,
use the bare form `accesso@auth.nexodify.com` — the display name is cosmetic and
not worth a parsing risk. Confirm after the restart in §5 that
`from_address_missing` is absent from the preflight reasons.

Leave `AUTH_EMAIL_REPLY_TO` unset. Leave every tuning value
(`AUTH_EMAIL_CODE_TTL_SECONDS` 600, `AUTH_EMAIL_RESEND_COOLDOWN_SECONDS` 60,
`AUTH_EMAIL_MAX_REQUESTS_PER_EMAIL_HOUR` 5, `AUTH_EMAIL_MAX_REQUESTS_PER_IP_HOUR`
20, `AUTH_EMAIL_MAX_VERIFY_ATTEMPTS` 5) at its default — the defaults are the
tested values.

**`AUTH_EMAIL_SENDER_DOMAIN_VERIFIED` — do NOT set it yet.** The code does not
check anything; it trusts your attestation. Set it to `true` only in §6, and
only after seeing `auth.nexodify.com` reported as Verified in the Resend
dashboard (SPF/DKIM green). Resend silently refuses unverified senders, so a
premature `true` produces mail that never arrives with no local error.

## 4. Keep `AUTH_EMAIL_ENABLED=false`

Do **not** add the variable at all. Absent is the safest expression of false:
`config.is_enabled()` is `_env_bool(FEATURE_FLAG, False)`, so a missing variable
and `=false` are identical, and an absent line cannot be flipped by a typo.
It gets added exactly once, in §10.

## 5. Restart and confirm Google-only behaviour is unchanged

```bash
sudo systemctl restart periziascan-backend.service && sleep 5
sudo systemctl is-active periziascan-backend.service
sudo systemctl show periziascan-backend.service -p MainPID -p NRestarts -p ActiveState -p SubState
curl -s http://127.0.0.1:8081/api/health
curl -s https://api-periziascan.nexodify.com/api/health
```

**The key check.** Trigger one refusal and read the reason list:

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X POST \
  https://api-periziascan.nexodify.com/api/auth/email/request-code \
  -H 'Content-Type: application/json' -d '{"email":"probe@example-nonexistent.it"}'
sudo journalctl -u periziascan-backend.service --since '-2 min' | grep 'preflight refused' | tail -1
```

- Expected status: **503**.
- Expected reason list: **`feature_disabled` and nothing else.**

That single-reason line is the proof that the key, pepper and from-address all
loaded correctly through systemd. If `pepper_missing_or_too_short`,
`from_address_missing` or `resend_api_key_missing` still appears, the parser
trap in §1 bit you — fix the value, do not proceed.

Then confirm the public surface has not moved:

```bash
curl -s https://api-periziascan.nexodify.com/api/auth/capabilities
# expect exactly {"email_otp_enabled":false,"google_enabled":true}
```

and re-run the production disabled-state suite (it should still be 18/18):

```bash
cd /srv/perizia/app/frontend && E2E_BASE_URL=https://periziascan.nexodify.com \
  E2E_API_URL=https://api-periziascan.nexodify.com E2E_AUTH_EMAIL_ENABLED=false \
  npx playwright test --config ./playwright.config.js e2e/auth-email-disabled.spec.js
```

**Rollback:** restore the `.env` backup and restart. Nothing else has changed.

### Snapshot the baseline before any validation writes

Capture the pre-validation state; §13 diffs against it.

```bash
backend/.venv/bin/python /tmp/.../scratchpad/snapshot.py > /tmp/phase2_before.txt
```

(The snapshot script from Phase 1 is read-only and masks addresses. Promote it
to `backend/scripts/audit_identity_snapshot.py` if you want it version-controlled.)

## 6. Owner-controlled Resend delivery smoke

Only after the Resend dashboard shows the domain Verified, set:

```
AUTH_EMAIL_SENDER_DOMAIN_VERIFIED=true
```

Restart, then send exactly one message to an **owner-controlled** mailbox using
the application's own `ResendSender` — not `curl` — so the real code path,
payload builder and error classifier are what get exercised:

```bash
cd /srv/perizia/app/backend
.venv/bin/python - <<'PY'
import asyncio, os
from dotenv import load_dotenv; load_dotenv()
from auth_email import config, sender
s = sender.ResendSender(api_key=config.resend_api_key(),
                        from_address=config.email_from(),
                        reply_to=config.email_reply_to())
r = asyncio.run(s.send(to=os.environ["SMOKE_TO"], code="000000",
                       ttl_seconds=600, idempotency_key="phase2-smoke-1"))
print("ok:", r.ok, "state:", r.delivery_state, "status:", getattr(r, "status_code", None))
PY
```

Invoke with `SMOKE_TO=<your address> ...`. Confirm: `ok: True`, the message
arrives, sender is `accesso@auth.nexodify.com`, subject is *Il tuo codice di
accesso a Perizia Scan*, and the body contains no beta/credit/report/admin
wording. `000000` is a literal, not a real challenge — it authenticates nothing.

**If it does not arrive:** revert `AUTH_EMAIL_SENDER_DOMAIN_VERIFIED` to `false`,
restart, stop. Do not proceed to §10 on an unverified sender.

## 7. Validate the OTP flow on the loopback validator

Start the temporary validator — loopback-bound, production database, flag on:

```bash
cd /srv/perizia/app/backend
set -a; . ./.env; set +a
AUTH_EMAIL_ENABLED=true \
.venv/bin/python -m uvicorn server:app --host 127.0.0.1 --port 8099 \
  > /tmp/phase2_validator.log 2>&1 &
sleep 5
curl -s http://127.0.0.1:8099/api/auth/capabilities   # expect email_otp_enabled: true
curl -s https://api-periziascan.nexodify.com/api/auth/capabilities  # STILL false
```

Both lines matter: the validator is enabled, the public API is not.

Use an **owner-controlled non-Google mailbox** (the whole point is proving a
non-Google provider works). Request a code, read it from that mailbox, verify it:

```bash
curl -s -c /tmp/phase2.jar -X POST http://127.0.0.1:8099/api/auth/email/request-code \
  -H 'Content-Type: application/json' -d '{"email":"<owner-non-google-address>"}'
# -> {"challenge_id":"aec_...","resend_available_in":60}
curl -s -b /tmp/phase2.jar -c /tmp/phase2.jar -X POST \
  http://127.0.0.1:8099/api/auth/email/verify-code \
  -H 'Content-Type: application/json' -d '{"challenge_id":"aec_...","code":"<from mailbox>"}'
```

Check while here: a wrong code returns the generic invalid message; a replayed
code fails; the response to an unknown address is identical to a known one.
Mind the limits — 5 requests/email/hour and a 60s resend cooldown.

## 8. Validate account creation, linking, session, logout, relogin

Against `:8099`, with the address from §7:

1. **Creation** — `users` count rose by exactly 1; the new doc has
   `normalized_email` set, `auth_methods: ["email_otp"]`, `email_verified: true`,
   `plan: "free"`.
2. **Linking (the important one)** — repeat the OTP flow with the **owner's
   existing Google address**. It must resolve to the **same `user_id`** as the
   Google account, add `email_otp` to `auth_methods`, create **no** second row,
   and leave credits/plan/reports untouched. This is the atomic-upsert guarantee
   under real data.
3. **Session** — `/api/auth/me` with the cookie returns the expected user.
4. **Logout** — `POST /api/auth/logout` removes the `user_sessions` row.
5. **Relogin** — a fresh code produces a working session again.
6. **Google unaffected** — Google login on the public site still returns the same
   `user_id`.

## 9. Controlled pending beta membership with five remaining perizie

Use a **fresh owner-controlled address that has never logged in**. Never Mauro's,
and never an existing membership.

As owner, against the public API (admin routes are unrelated to the OTP flag):

```
POST  /api/beta/testers                        {"email": "<owner test address>", ...}
PATCH /api/beta/testers/{membership_id}/quota  {"quota_mode":"LIMITED","analysis_limit":5}
```

The membership is created **PENDING** (no `user_id` yet). Then OTP-login that
address on `:8099`. `_link_pending_beta_membership` runs on authentication and
activates it — it touches PENDING only, never REVOKED, and never raises into the
login path.

Verify: membership is ACTIVE and bound to the new `user_id`; `quota_mode`
`LIMITED`; `analysis_limit` 5; `analysis_consumed` 0; **remaining 5** in the UI;
Programma Beta visible; billing CTAs hidden.

Confirm Mauro's two memberships are byte-identical throughout — count still 2,
`updated_at` still `2026-07-20T19:43:04` (REVOKED) and `2026-07-20T22:14:05`
(PENDING), `beta_program_audit` grown only by your own tester's entries.

**Then stop the validator:**

```bash
pkill -f 'uvicorn server:app --host 127.0.0.1 --port 8099'
curl -s http://127.0.0.1:8099/api/health || echo "validator down (expected)"
```

## 10. Enable OTP — only after §5–§9 all pass

Add the single line, restart, nothing else:

```
AUTH_EMAIL_ENABLED=true
```

```bash
sudo systemctl restart periziascan-backend.service && sleep 5
sudo systemctl is-active periziascan-backend.service
```

No frontend redeploy. No Vercel change. No logout for existing users — sessions
live in `user_sessions` and are untouched by an env change.

## 11. Verify the public capability flips to true

```bash
curl -s https://api-periziascan.nexodify.com/api/auth/capabilities
# expect {"email_otp_enabled":true,"google_enabled":true}
```

Still exactly two booleans — no provider, key, sender, domain state, index
detail, rate limit or owner information.

## 12. Confirm the frontend shows "Continua con email"

`AuthContext` fetches the capability per mount and caches nothing, so a plain
refresh is enough. Run the enabled-mode suite against production, desktop and
mobile:

```bash
cd /srv/perizia/app/frontend
E2E_BASE_URL=https://periziascan.nexodify.com \
E2E_API_URL=https://api-periziascan.nexodify.com \
npx playwright test --config ./playwright.config.js e2e/auth-email.spec.js
```

Note this suite performs real logins and therefore real Resend sends to
`@studio-e2e-example.it` addresses that do not exist. **Prefer a manual check**
for production: load the site, confirm both buttons appear, complete one OTP
login with your own mailbox, confirm Google still works. Reserve the full spec
for the local harness with the SMTP sink.

## 13. Confirm nothing was unintentionally changed

```bash
backend/.venv/bin/python /tmp/.../scratchpad/snapshot.py > /tmp/phase2_after.txt
diff /tmp/phase2_before.txt /tmp/phase2_after.txt
```

**Expected (allow-list) deltas only:**
- `users` +1 or +2 (the §7 test mailbox, the §9 beta tester); each with
  `auth_methods` including `email_otp` and `email_verified: true`.
- The owner's existing account gains `email_otp` in `auth_methods` — **same
  `user_id`**, unchanged plan and credits.
- `user_sessions` grows and shrinks with the login/logout tests.
- `auth_email_challenges` / `auth_email_rate` become non-zero (they self-purge).
- `beta_program_memberships` +1 (your tester), `beta_program_audit` grows.

**Any of the following is a defect — stop and investigate:**
- Any pre-existing `user_id` changed, or a duplicate account for one address.
- Any change to `perizia_credits`, `plan`, or `quota` on a pre-existing user.
- Any change to `perizia_analyses` (354), `credit_ledger` (71),
  `billing_records` (9), `payment_transactions` (9), `beta_feedback` (6).
- Any change to Mauro's two memberships.

### Cleanup of validation artefacts

The §7/§9 rows are real production data. Either keep them (documented as owner
test accounts) or remove them deliberately — never half. Removing a beta tester
uses the admin revoke route, not a direct DB delete, so the audit trail stays
honest. Do not delete the owner's own linked account.

---

## Rollback summary

| Stage | Rollback |
|---|---|
| §1–§4 secrets installed | Restore `.env` backup, restart. Feature was never on. |
| §6 delivery smoke fails | `AUTH_EMAIL_SENDER_DOMAIN_VERIFIED=false`, restart, stop. |
| §7–§9 validator fails | `pkill` the validator. Public service never had the flag. Clean up any test rows. |
| §10 enabled, problem found | Remove `AUTH_EMAIL_ENABLED=true`, restart. Capability returns false within one request; the button disappears on the next page load. **No redeploy, no rollback of code, no user impact.** |

That last row is the payoff of the capability design: disabling is a one-line
`.env` edit plus a restart, and the frontend follows automatically.

## Residual risks going into Phase 2

- `AUTH_EMAIL_SENDER_DOMAIN_VERIFIED` is an unverified attestation. A wrong
  `true` yields silent non-delivery that looks like a working system.
- systemd `EnvironmentFile` parsing is the most likely cause of a confusing
  failure; §5's single-reason check is the designed detector.
- Validation writes real production rows for owner test addresses.
- Rate limits (5/email/hour) will bite during repeated manual testing.
- The `user_sessions.session_token` index is still missing
  (`docs/followup_session_token_index.md`); OTP adds session-creation volume,
  which makes that scan marginally hotter. Still not a correctness issue.
- Once enabled, OTP is a public unauthenticated endpoint that sends mail on
  demand. The rate limits are the only spend control — watch the Resend
  dashboard for the first days.
