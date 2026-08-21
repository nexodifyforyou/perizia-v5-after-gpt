# Perizia Scan — Quality Recovery Programme

Status document for the post-first-external-beta product-quality recovery programme.
Created 2026-07-22 after the forensic diagnosis of the first external beta test
(multi-lot appraisal; root cause: shared-page exclusion starving per-lot re-analysis,
plus selected-page-only quality scoring masking the loss, plus one lot blocked by the
quality gate with no partial report).

This document tracks programme state only. It is **not** runtime logic and must never
be read by product code.

Base at programme start:

- Branch base: `main` @ `5c0755743dfe1e094c9084ce65b99562aa2ac019` (= `origin/main`)
- Latest release tag: `correctness-v2-passwordless-email-auth-live`
- Backend: healthy (systemd `periziascan-backend.service`, uvicorn :8081)
- Frontend: Vercel auto-deploy on push to `main`
- `CORRECTNESS_V2_LOT_CONCURRENCY=2` (must remain 2 throughout the programme)
- Forensic bundle (read-only, outside Git, never committed):
  `/srv/perizia/ops_backups/mauro_beta_investigation_20260722T183629Z/`

Permanent golden regression cases:

1. Torino
2. Pistoia
3. 1859886_C
4. Orecchiazzi
5. Cairate
6. Codogno
7. Mantova
8. **Beta-Multilot** (sanitized fixture derived from the preserved first-external-beta
   case; added by task P0-1)

Non-hardcoding contract: runtime code must never reference the beta tester, their
company, email, filenames, production IDs, fixed page/lot numbers, or the specific
monetary values of that appraisal. Those may appear only in sanitized fixtures,
golden assertions, and forensic documentation.

---

## P0 — Correctness of information delivery

### 1. Lot-fact projection and reconciliation
- **Status:** COMPLETED — DEPLOYED (owner-approved 2026-07-23)
  - Feature commit: `c4e7cfe9056226a2c36cd6190c9ec504996c1f92`
  - Merge commit (main): `5f4cb70e664142b6bc41b50b6708c23c6d975303`
  - Release tag: `correctness-v2-lot-fact-projection-live`
  - Deployed: 2026-07-23 (backend restarted, local+public health 200,
    clean startup journal, concurrency 2 unchanged)
  - Eight-case regression on deployed main: 10 passed, exit 0
    (Torino, Pistoia, 1859886_C, Orecchiazzi, Cairate, Codogno, Mantova,
    beta_multilot)
  - External-beta fixture/replay on deployed main: Lot 1 critical-fact
    coverage 0.2857 → 1.0000; all 11 facts retained/repaired; 2 false
    uncertainties removed; 4 genuine conflicts preserved; zero cross-lot
    leakage; zero paid calls/writes/quota; production collection deltas all zero
  - Known non-blocking residual risks (safe direction, tracked under task 15):
    degree/hedging-adverb long tail in the compliance detector; topic-blind
    disqualifier over-suppression; excerpt continuation edge case; allowlist
    word-order rigidity
- **Branch:** `feature-correctness-v2-lot-fact-projection`
- **Dependency:** none (first branch)
- **Acceptance criteria:**
  - Canonical fact lineage with applicability model
    (CASE_GLOBAL / ALL_LOTS / LOT_SPECIFIC / MULTIPLE_LOTS / BENE_SPECIFIC / UNKNOWN_APPLICABILITY).
  - Shared-page/case-level facts projected into every applicable lot report; no
    silent discard of richer case-level facts; explicit conflict preservation.
  - Deterministic reconciliation with drop-reason codes for every dropped material fact.
  - Full-document lot coverage audit replaces selected-page-only scoring.
  - Deterministic lot-report input contract (structured facts, not narrative-only).
  - Eighth golden regression (sanitized beta-multilot fixture) passes; existing
    seven-case regression unchanged; aggregate eight-case runner with single command
    and zero exit code.
  - Offline replay of the preserved case shows all enumerated Lot 1 critical facts
    repaired with zero paid calls, zero production writes, zero quota/credit use.
- **Regression risk:** high (touches per-lot pipeline core); mitigated by eight-case
  runner + full Correctness V2 suite + offline historical replay.
- **Deployment gate:** Fable 5 diff review passed; pre-commit report reviewed by owner;
  no deploy before explicit approval.

### 2. Full-document lot coverage audit
- **Status:** IN PROGRESS (folded into branch 1, section D of its plan)
- **Branch:** `feature-correctness-v2-lot-fact-projection`
- **Dependency:** task 1 fact lineage
- **Acceptance criteria:** coverage measured against full-document material facts;
  selected-page-only success cannot yield a misleading completeness score; separate
  extraction / report / evidence / user-visible completeness dimensions; fail/warn
  thresholds based on fact coverage.
- **Regression risk:** medium (score semantics change; gates must not block valid runs).
- **Deployment gate:** same as task 1.

### 3. Case/lot verdict consistency
- **Status:** COMPLETED — **DEPLOYED (owner-approved 2026-08-14)**
  - Feature commit: `b085c1736292eaddae8f1e25f1d363ea755841ee`
  - Merge commit (main): `479ce2a9ad52a153964a793f981c1ca83b2b7975` (= final main / origin/main)
  - Release tag: `correctness-v2-case-verdict-consistency-live`
  - Deployed: 2026-08-14 (backend restarted PID 756484, NRestarts=0, local+public health 200,
    clean startup journal, concurrency 2 unchanged, Flag-ON proven on the live process)
  - Fable verdict: **APPROVE** after 1 audit + 4 adversarial diff-review cycles → 4 Sol repair cycles.
  - Deployed-main validation: eight-case 10 passed exit 0; focused verdict/consistency 42 passed;
    offline replay Lot-1 coverage 0.2857→1.0 (all 11 critical facts repaired), ceiling==max(media),
    selector-report gap resolved, zero cross-lot leakage / raw-enum / keyword-escalation / paid / writes;
    production semantic smoke over 83 cached reports (differential flag ON vs OFF): 0 regressions, 20
    cross-band contradictions FIXED, unknown-schema degrades with no 500; production delta audit all
    zero (0 jobs, 0 regenerations, 0 artifact changes, 0 external calls, 0 credit/quota).
  - Plan: `docs/case_verdict_consistency_plan.md` (Fable), incl. §12 amendments A1–A2.
  - One additive `verdict_model.py` (`cv2.verdict.v1` CanonicalVerdict) derived from Branch 1's
    reconciled facts; `decision`/`esito`/selector/workspace/money become projections of it. Feature
    flag `CORRECTNESS_V2_CANONICAL_VERDICT_ENABLED` (default ON); flag-OFF == byte-for-byte pre-branch
    (proven by base golden tests under flag off).
  - Review: 1 Fable audit + 4 adversarial diff-review cycles → 4 Sol repair cycles. All findings
    resolved and independently reproduced by Fable (schema-500 blocker; raw-vs-live esito; multi-lot
    money-confirmation provenance; the CASE_GLOBAL severe-suppression class closed across three
    successive routes — positional key, formality `"other"` token, cross-ledger fact_id collision;
    priority-derivation nit). §4.4 keyword-escalation carried as owner-signed-off DT-01 (flag-OFF only).
  - Verified: Correctness V2 584 · eight-case 10/exit 0 · full backend 1667 pass / 7 known-stale / 7
    skip · offline replay coverage 1.0, ceiling==max, zero leakage/enum-leak/keyword-escalation/paid/
    writes · `fact_lineage.py`/`lot_fact_projection.py` untouched · concurrency 2.
- **Branch:** `feature-correctness-v2-case-verdict-consistency`
- **Dependency:** task 1 (fact lineage + reconciliation)
- **Acceptance criteria:** case-level verdicts and lot-level verdicts derived from the
  same reconciled fact base; no contradictory case-vs-lot statements; taxonomy tests.
- **Regression risk:** medium.
- **Deployment gate:** Fable review (PASSED) + owner pre-commit review (PENDING) before any commit/deploy.

### 4. Partial lot reports
- **Status:** COMPLETED — **DEPLOYED + ENABLED (owner-approved 2026-08-14)**
  - Feature commit: `273a629d91dde4847bfd52ca39d46ff4ce77abae`
  - Merge commit: `3cb6f8bb72a74917f1f261c234cd62f9596592c3` (dormant-ready)
  - R3-02 follow-up merge (final main): `2bc9ce353a013b07f3caaaa133140dfc5b941176`
  - Tags: `correctness-v2-partial-lot-reports-ready` (dormant deploy) → `3cb6f8b`;
    `correctness-v2-partial-lot-reports-live` (enabled) → final main
  - Deployed dormant (partial flag OFF) 2026-08-14, then R3-02 deployed, then
    `CORRECTNESS_V2_PARTIAL_LOT_REPORTS_ENABLED=true` set in `backend/.env`; backend restarted
    (PID 793760, NRestarts=0, health 200 local+public, clean startup, concurrency 2, canonical=ON,
    partial=ON proven via server.py load_dotenv resolution).
  - Fable verdict: **APPROVE** (1 red-team + 1 repair + 1 confirmation cycle; all 12 mandated attacks
    defended; CanonicalVerdict authoritative, disclosure state separate; `verdict_model.py` untouched).
  - Design: new additive `partial_report.py` — 3-state disclosure {FULL / PARTIAL / BLOCKED},
    strict omission-only allow-list {CRITICAL_FACT_MISSING, MISSING_IMPORTANT_MONEY}, unknown/
    fabrication/contradiction/scope codes → REPORT_BLOCKED (fail-closed). Preserves all reliable
    facts, exposes the unresolved field (category/scope/reason/evidence/professional-verification),
    never fabricates a substitute, never increases readiness or downgrades severity. Applied at both
    suppression paths. Owner-approved fixed-safe deterministic `why_unresolved` wording (with the
    structured unresolved fields exposed separately). Feature-flagged
    `CORRECTNESS_V2_PARTIAL_LOT_REPORTS_ENABLED`.
  - Deployed validation: eight-case 10/exit 0 (partial ON and OFF), Branch-1+2 regression 65,
    Branch-3 partial 48, Correctness V2 634, full backend 1726 pass/7 known-stale/7 skip, replay
    coverage 1.0. Production partial smoke (read-only, flag ON, 83 cached reports): 0 disclosure
    changes / 0 became-partial / 0 fabrication / 0 severity-change / 0 raises — enabling is a safe
    no-op for existing customers (all cached blocked reports fail closed; only NEW eligible analyses
    produce partials). Production delta audit: artifact tree byte-identical, 0 jobs/writes/regen/
    external-calls. Eligible→PARTIAL transition + hard-block behavior proven on the beta Lot-4
    specimen (offline replay, flag ON, 19 assertions) + 10-code full-block matrix.
  - Invariants recorded: **unknown/new gate codes fail closed to REPORT_BLOCKED**; fixed-safe
    `why_unresolved` with structured unresolved fields exposed separately (owner decision 2026-08-14).

### 5. PDF retention with consent and TTL
- **Status:** COMPLETED — **DEPLOYED DORMANT (owner-approved 2026-08-14/15); retention flag OFF**
  - Feature commit: `98036334c1c941614f96550c6e81ba2299dd86b6`
  - Merge commit (final main): `8f0d3e1010cc2fce5cf3431ef7b4c33082623f88`
  - Release tag: `correctness-v2-pdf-retention-ready` (NOT `-live` — feature disabled)
  - Deployed dormant 2026-08-15 (backend PID 885233, NRestarts=0, health 200 local+public, clean
    startup, concurrency 2, canonical=ON, partial=ON, **pdf_retention=OFF** proven on live process).
  - Fable verdict: **APPROVE** (1 red-team+repair cycle; 2 MAJOR fixed — upload-retention timeout +
    unguarded erasure lookup; 2 MINOR deferred → risk register). Independent orchestrator verification
    of all fixes + a security review (no HIGH/MEDIUM findings).
  - New additive `backend/pdf_retention/` package (~1492 LOC) + `deploy/` systemd units (created, NOT
    enabled). AES-256-GCM envelope encryption (per-object DEK wrapped by versioned KEK, owner+analysis
    bound as AAD), opaque root-owned storage outside web/artifact roots, server-side ownership on every
    access, versioned consent + withdrawal-deletes-bytes, delete cascades, crash-safe create/delete
    state machines + reconciliation (standalone Mongo), standalone TTL cleanup + systemd timer,
    path-traversal/symlink defenses, ops-only (non-web) owner diagnostic access with audit. Also
    hardens R3-05 (test artifact-root guard + feature-flag pinning). Branch 1/2/3 core untouched.
  - Feature flag `CORRECTNESS_V2_PDF_RETENTION_ENABLED` (default OFF; flag-OFF == today byte-for-byte).
  - Dormant validation: eight-case 10/exit 0 (partial ON+OFF), Branch-1+2 regression 65, Branch-3
    partial 48, full backend 1765 pass/7 known-stale/7 skip; **0 retained PDFs, 0 retention Mongo rows
    (both collections empty), 0 artifact writes, 0 jobs, 0 paid/model/credit/quota** — no customer-
    visible change. Encryption + no-plaintext-at-rest + path-traversal-rejection independently verified.
  - **NOT enabled** (owner gate): needs production encryption key(s) in the secret store, root-owned
    `/srv/perizia/private` (0700), systemd timer install+enable, and owner/legal (GDPR consent-copy)
    sign-off before `CORRECTNESS_V2_PDF_RETENTION_ENABLED=true`.
- **Branch:** `feature-pdf-retention-consent`
- **Regression risk:** low-medium (upload path) — mitigated: retention failure/timeout/flag-off can
  never fail, delay, or change the analysis (timeout-bounded, additive, flag-gated).
- **Deployment gate:** Fable review (PASSED) + owner approval (PASSED for dormant) + GDPR wording review
  (PENDING, required before enablement).

## P1 — Experience and observability

### 6. Unified report summary and information architecture
- **Status:** COMPLETED — **DEPLOYED + ENABLED (owner-approved 2026-08-21)**
  - Feature commit: `c848513dc77ac2d2e0743154a7e7ba37a40dd45e`
  - Merge commit (final main): `1bf780007fa36beb0c0e2c7d3df2b810f6e6d3cb`
  - Tags: `correctness-v2-report-clarity-ready` (dormant) → `1bf7800`;
    `correctness-v2-report-clarity-bilingual-live` (enabled) → final main
  - Deployed dormant then `CORRECTNESS_V2_REPORT_CLARITY_ENABLED=true` set in `backend/.env`;
    backend PID 1399506, NRestarts=0, health 200 local+public, report_clarity=ON proven on live process,
    canonical=ON/partial=ON/pdf_retention=OFF/concurrency=2.
  - Fable verdict: **APPROVE** (1 audit + 1 revalidation + 1 red-team + 2 focused confirmations; 4 MAJOR
    found+fixed). Model routing: Opus 5 (one impl pass) + Opus 4.8 (mechanical/#3/repairs); no Sol.
  - Delivered: PARTIAL routed into the decision-oriented report (kills the "wall of Da verificare");
    six distinct states; critical/secondary split; dedup by finding-id; one financial renderer; canonical
    conflicts rendered; **presentation-only amber floor** when authoritative conflicts coexist with a green
    esito (esito/severity/readiness byte-identical). **Italian-first bilingual**: English under Italian,
    lazy-translated via the reused `narrator` Gemini client (translation-only, ownership-scoped cache,
    quota-exempt, fail-soft to Italian). Flag default OFF (flag-OFF == today). English never feeds
    semantics; Branch 1/2/3 core + PDF retention untouched.
  - Live validation: dormant parity (eight-case 10, Branch-1+2 65, Branch-3 partial 48, all deltas zero);
    ONE real Gemini smoke (fidelity: amounts/dates/negation/future-cancellation-tense preserved) + cache
    reuse (2nd call 0 additional); semantic delta audit all zero (esito/readiness/CanonicalVerdict
    unchanged flag ON vs OFF); 0 jobs/writes/regeneration/credit/quota.
- **Branch:** `feature-correctness-v2-report-clarity`
- **Regression risk:** medium (frontend-wide) — mitigated: flag-gated, re-derivation-free, English-can't-
  feed-semantics, deterministic tests + real-provider smoke.
- **Deployment gate:** Fable review (PASSED) + owner approval (PASSED) + live validation (PASSED);
  pixel-level visual/mobile browser spot-check recommended to owner.

### 7. Structured in-product beta feedback
- **Status:** NOT STARTED
- **Branch:** `feature-beta-feedback-in-product`
- **Dependency:** none hard; scheduled after 6
- **Acceptance criteria:** structured feedback capture tied to report views; no PII in
  telemetry; owner-visible aggregation.
- **Regression risk:** low.
- **Deployment gate:** Fable review + owner approval.

### 8. Artifact lineage and versioning
- **Status:** NOT STARTED (partial groundwork in branch 1 provenance chain)
- **Branch:** `feature-processing-lineage-observability`
- **Dependency:** task 1
- **Acceptance criteria:** every artifact carries producing code version, schema
  version, timestamps; historical reproduction possible without guesswork.
- **Regression risk:** low.
- **Deployment gate:** Fable review + owner approval.

### 9. Per-stage observability
- **Status:** NOT STARTED (minimum viable counters land in branch 1, section I)
- **Branch:** `feature-processing-lineage-observability`
- **Dependency:** task 8 (same branch)
- **Acceptance criteria:** per-stage fact counts, drop reasons, coverage states in
  safe metadata; no raw document content in telemetry.
- **Regression risk:** low.
- **Deployment gate:** Fable review + owner approval.

### 10. Mobile and usability pass
- **Status:** NOT STARTED
- **Branch:** part of `feature-correctness-v2-report-clarity` follow-up
- **Dependency:** task 6
- **Acceptance criteria:** report usable on common mobile viewports; no horizontal
  scroll; accordions/labels reviewed.
- **Regression risk:** low-medium.
- **Deployment gate:** Fable review + owner approval + visual smoke.

## Technical backlog

### 11. Seven stale baseline tests
- **Status:** NOT STARTED. Known pre-existing failure baseline is 7 backend tests
  (documented in beta-perizia-limits work). Fix or quarantine with reasons.
- **Branch:** TBD (`chore-stale-baseline-tests`)
- **Dependency:** none. **Risk:** low. **Gate:** green full suite.

### 12. `user_sessions.session_token` index
- **Status:** NOT STARTED (see docs/followup_session_token_index.md)
- **Branch:** TBD. **Dependency:** none. **Risk:** low (index add on standalone
  mongod). **Gate:** owner approval for prod DDL.

### 13. Artifact retention policy
- **Status:** NOT STARTED
- **Branch:** with task 5 or separate. **Dependency:** task 5 decisions.
- **Risk:** low. **Gate:** owner approval.

### 14. Browserslist update
- **Status:** NOT STARTED. **Branch:** TBD. **Risk:** trivial. **Gate:** frontend build green.

### 15. Eight-case regression hardening
- **Status:** NOT STARTED (runner created in branch 1; hardening = CI wiring, shard
  stability, timing budget)
- **Branch:** TBD. **Dependency:** task 1. **Risk:** low. **Gate:** green runner.
- **Review-discovered sub-items (from branch-1 Fable audit cycles, 2026-07-22/23):**
  - The seven historical golden-case tests build customer-report-shaped dicts
    directly and never exercise `apply_compliance_evidence_gate` /
    `validate_worksheet` / `build_contract`; only the eighth (beta-multilot)
    fixture reaches the detector. Extend golden coverage so at least a subset of
    the seven pass through the real gate/contract path.
  - Compliance disqualifier scan is deliberately topic-blind in branch 1
    (fail-safe): unrelated hedge-shaped phrases on the same evidence page
    ("in assenza di posto auto", "manca il certificato della caldaia") suppress a
    genuinely correct "conforme". Follow-up: topic co-occurrence gating to narrow
    over-suppression WITHOUT reopening unsafe holes; requires its own adversarial
    re-probe before merge.
  - Customer-excerpt continuation handling: an untagged continuation line beyond
    the paragraph-boundary rule may yield a missing excerpt while the structured
    fact and citation stay valid (safe direction; noise only).
  - Eighth fixture reuses the real case's € figures (permitted); consider varying
    values in a future fixture revision to remove a re-identification vector.
  - Widened `verification_pages` marginally increase the hallucinated-citation
    surface; a dedicated gate test exists (branch 1), keep it in the bank.
  - The compliance-detector adversarial test bank (36+ cases accumulated across
    six review cycles) is a permanent regression fixture for
    `_has_positive_compliance_statement` — never prune it; extend it whenever a
    new Italian hedging/negation pattern is discovered in production.

## P2 — Later

### 16. Cost optimization — NOT STARTED. No cost work permitted in P0 branches.
### 17. Evidence polish — NOT STARTED. Dependency: 6.
### 18. Storage/performance optimization — NOT STARTED.
### 19. Wider beta expansion — NOT STARTED. Gate: P0 complete + at least tasks 6–7 live.

---

## Execution order (one branch at a time)

1. `feature-correctness-v2-lot-fact-projection` — DEPLOYED
2. `feature-correctness-v2-case-verdict-consistency` — DEPLOYED
3. `feature-correctness-v2-partial-lot-reports` — DEPLOYED + ENABLED
4. `fix-canonical-verdict-persistence-all-lots` — DEPLOYED (R3-02: both Branch-2 singleton
   `build_case_verdict` sister sites repaired with the all-known-lots merge invariant; Fable APPROVE)
5. `feature-pdf-retention-consent` — DEPLOYED DORMANT (flag OFF; enablement gated on keys/root/timer/GDPR sign-off)
6. `feature-correctness-v2-report-clarity` ← **next** (Fable planning; addresses the 2nd external-beta complaint — report hard to understand)
7. `feature-beta-feedback-in-product`
8. `feature-processing-lineage-observability`

Per-branch protocol: Fable 5 audit + implementation plan → Sol implements → Sol runs
focused + full regressions → Fable 5 independent diff review → Sol repairs → pre-commit
report → owner review before any commit/deploy.
