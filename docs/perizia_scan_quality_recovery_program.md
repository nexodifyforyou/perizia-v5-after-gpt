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
- **Status:** NOT STARTED
- **Branch:** `feature-correctness-v2-case-verdict-consistency`
- **Dependency:** task 1 (fact lineage + reconciliation)
- **Acceptance criteria:** case-level verdicts and lot-level verdicts derived from the
  same reconciled fact base; no contradictory case-vs-lot statements; taxonomy tests.
- **Regression risk:** medium.
- **Deployment gate:** Fable review + owner approval.

### 4. Partial lot reports
- **Status:** NOT STARTED
- **Branch:** `feature-correctness-v2-partial-lot-reports`
- **Dependency:** tasks 1, 3
- **Acceptance criteria:** a quality-gate-blocked lot yields an explicit partial report
  with valid facts and a clear "incomplete" status instead of nothing; blocked-lot
  facts from the beta fixture (Lot 4) surface correctly.
- **Regression risk:** medium (gate semantics).
- **Deployment gate:** Fable review + owner approval.

### 5. PDF retention with consent and TTL
- **Status:** NOT STARTED
- **Branch:** `feature-pdf-retention-consent`
- **Dependency:** none (parallel-safe, but executed after task 4 per programme order)
- **Acceptance criteria:** original uploads retained under explicit consent with TTL;
  checksum + lineage metadata; deletion policy documented; no silent loss of the
  source document for future forensics.
- **Regression risk:** low-medium (upload path).
- **Deployment gate:** Fable review + owner approval; GDPR wording review.

## P1 — Experience and observability

### 6. Unified report summary and information architecture
- **Status:** NOT STARTED
- **Branch:** `feature-correctness-v2-report-clarity`
- **Dependency:** tasks 1, 3, 4
- **Acceptance criteria:** immediate "what matters" summary; clear hierarchy; reduced
  fragmentation; populated content never reads as empty; mobile-acceptable.
- **Regression risk:** medium (frontend-wide).
- **Deployment gate:** Fable review + owner approval + visual smoke.

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

1. `feature-correctness-v2-lot-fact-projection` ← **current**
2. `feature-correctness-v2-case-verdict-consistency`
3. `feature-correctness-v2-partial-lot-reports`
4. `feature-pdf-retention-consent`
5. `feature-correctness-v2-report-clarity`
6. `feature-beta-feedback-in-product`
7. `feature-processing-lineage-observability`

Per-branch protocol: Fable 5 audit + implementation plan → Sol implements → Sol runs
focused + full regressions → Fable 5 independent diff review → Sol repairs → pre-commit
report → owner review before any commit/deploy.
