# Perizia Scan — Risk Register

Living register of correctness/product risks discovered during the post-first-external-beta
quality-recovery programme (see `perizia_scan_quality_recovery_program.md`). This document tracks
risk state only; it is **not** runtime logic and must never be read by product code.

Severity: **S1** customer-visible wrong/contradictory information · **S2** degraded correctness or
completeness · **S3** hygiene / latent.
Status: OPEN · MITIGATED (compensating control in place) · CLOSED (fixed + regression-locked).

---

## Branch 1 — lot-fact projection (DEPLOYED 2026-07-23, tag `correctness-v2-lot-fact-projection-live`)

| ID | Sev | Risk | Status | Control / note |
|----|-----|------|--------|----------------|
| R1-01 | S1 | Per-lot re-analysis excluded shared pages → case-level facts vanished from lot reports | CLOSED | Shared-page projection + reconciliation; offline replay Lot 1 coverage 0.2857→1.0 |
| R1-02 | S1 | Coverage audit scored only selected pages, masking the loss | CLOSED | Full-document coverage audit (`coverage_audit.lot_coverage`) |
| R1-03 | S2 | Compliance detector negation-blind ("conforme" affirmed under negation) | CLOSED | Allowlist + whole-text disqualifier scan; 58-item adversarial bank (permanent) |
| R1-04 | S3 | 7 golden cases never exercise the real compliance gate / contract path | OPEN | Programme task 15; extend golden coverage through real gate |
| R1-05 | S3 | Compliance disqualifier scan topic-blind → over-suppresses (safe direction) | MITIGATED | Fail-safe accepted; topic co-occurrence gating deferred (task 15) |

## Branch 2 — case/lot verdict consistency (DEPLOYED LIVE 2026-08-14, tag `correctness-v2-case-verdict-consistency-live`, main `479ce2a`)

Deployment record: feature `b085c17`, merge `479ce2a` (= final main/origin), Fable **APPROVE** after 4
adversarial cycles. Backend restarted (PID 756484, NRestarts=0), local+public health 200, Flag-ON proven
on the live process, concurrency 2. Validation: eight-case 10/exit 0, focused verdict 42, replay
coverage 1.0 / ceiling==max / selector-gap resolved / zero leakage/enum/keyword/paid/writes, production
semantic smoke 83 reports differential 0 regressions + 20 contradictions fixed, delta audit all zero.



Source: `docs/case_verdict_consistency_plan.md` (Fable 5 audit). Verified against production code this cycle.

| ID | Sev | Risk | Status | Control / note |
|----|-----|------|--------|----------------|
| R2-01 | S1 | Two disagreeing verdict engines (`customer_view.derive_decision` vs `decision_model._build_esito`) both attached to every payload | OPEN → mitigating | Unify behind one `CanonicalVerdict`; both become projections (plan §3.A, invariant 1) |
| R2-02 | S1 | Lot selector / case overview reads pre-reconciliation `lot_index.json`; can contradict the reconciled lot report | OPEN → mitigating | Selector/Storico prefer canonical verdict `field_verdicts` for reconciled lots (plan §3.D.2, invariant 2) |
| R2-03 | S1 | Raw English classification enum can leak into customer text (`contract.py:163` fallback) | OPEN → mitigating | Route through `_classification_it`/canonical label table (invariant 3) |
| R2-04 | S1 | Keyword severity-escalation in `customer_view._decision_level` has no negation guard (same bug class as R1-03) | MITIGATED (Flag-ON) | Flag-ON never invokes the keyword scanner (canonical severity only); legacy path retained Flag-OFF only for rollback — see **DT-01** (owner-signed-off 2026-08-13) |
| R2-05 | S2 | Four independent severity taxonomies with no shared source | OPEN → mitigating | One `SEVERITY_LABELS_IT`; others become derived views (plan §3.B) |
| R2-06 | S2 | No case-level aggregation exists; "case overview" = un-reconciled snapshot | OPEN → mitigating | `build_case_verdict` ceiling/floor aggregation (plan §3.E/F) |
| R2-07 | S1 | Occupancy could render vacant by omission on paths Branch 1 didn't cover (selector/Storico/`_occupancy_is_occupied`) | CLOSED | Generalized no-omission-to-LIBERO invariant to all paths (plan §3.H, invariant 9); replay confirms occupancy retained |

_R2-01…R2-07 all CLOSED at implementation (Fable APPROVE 2026-08-14, pending owner pre-commit review). Verdict-model unification + §12 amendments; each item locked by regression tests + offline replay._

### Branch 2 review-cycle findings (Fable diff red-team, all CLOSED)

Four adversarial cycles; every finding independently reproduced by Fable then reproduced fixed:

| ID | Sev | Finding | Resolution |
|----|-----|---------|------------|
| RC-01 | S1 (BLOCKER) | Unknown canonical `schema_version` raised uncaught → 500 outage (Gate 9/10) | `try_validate_verdict` + isolation in every consumer → degrade to legacy; one bad lot can't 500 the Storico page |
| RC-02 | S1 | Persisted raw verdict could be cleaner than live esito (`_base_severity` ignored formalities) → Gate 8 contradiction | Persist/aggregate the runtime-refined verdict; `_base_severity` now scans open formalities |
| RC-03 | S1 | Multi-lot money-confirmation never attached `canonical_verdict` → legacy path forced occupancy UNKNOWN on a live report | Resolve specific selected lot; reconstruct worksheet/segmentation/ledger without page re-selection; guard missing context |
| RC-04 | S1 | CASE_GLOBAL severe-fact suppression class (3 successive routes) | (a) positional `source_path` key → content-stable `(category, field)`; (b) formality `"other"` token collapse → content-derived token, fail-closed; (c) cross-ledger `fact_id` collision → case attribution requires Branch-1 `projected: true` stamp, lot-native IDs isolated to `lot_fact_ids`, fail-closed on missing origin |
| RC-05 | S3 (NIT) | `priority_from_severity` didn't derive from severity | Severity base + in-band offset; UI ordering byte-identical |

All fixes are additive and confined to Branch 2 modules; `fact_lineage.py`/`lot_fact_projection.py` untouched. See DT-01 for the §4.4 flag-OFF-only accepted debt.

### Branch 2 plan amendments (2026-08-13)

During plan-validation Sol found two internal contradictions between the plan and Branch 1's real
persisted contract; both verified against code and resolved in `case_verdict_consistency_plan.md` §12,
in the only direction consistent with the owner's non-negotiables + Gate 7:

- **A1 (R2-01):** `decision.level` and `esito.level` use disjoint, test-enforced enums; literal
  equality is impossible without breaking Gate 9. Resolved: both become deterministic projections of
  ONE canonical severity; consistency = semantic non-contradiction via a frozen equivalence table, not
  string equality.
- **A2 (R2-07/provenance):** Branch 1 does not persist a fact ledger and does not stamp fact IDs on
  lot-native reconciled facts; plan §4.11 was stricter than owner Gate 7. Resolved: reuse Branch 1's
  pure `build_case_fact_ledger` generator (recompute-on-read) to mint IDs over the reconciled
  worksheet; legacy/unresolvable leaves carry an explicit deterministic `provenance_reason` (fail-safe)
  per Gate 7. No parallel model, no Branch-1 core change, no fact suppression.

### Deprecated technical debt (owner-signed-off)

**DT-01 — Legacy keyword-based severity escalation (`customer_view._decision_level`, `customer_view.py:241-249`).**
Free-text keyword scan (`"collabente","crollo","amianto","fibrocement","pericol"`) with **no negation
guard** — it can escalate on a *negated* danger phrase (e.g. "non presenta rischio di crollo"). Same
false-certainty bug class Branch 1 spent seven cycles hardening in `validator.py`. Discovered by Fable
in the Branch 2 red-team (finding #5).

- **Status:** DEPRECATED — retained **only** on the emergency rollback path.
- **Owner sign-off (2026-08-13):** Flag-OFF may deliberately preserve complete pre-Branch-2 behavior,
  *including this bug*, strictly as an emergency rollback/parity path. Approved under non-negotiables:
  1. Shipping production state is **Flag-ON** (`CORRECTNESS_V2_CANONICAL_VERDICT_ENABLED` default ON).
  2. Flag-ON **must never** invoke the legacy keyword severity scanner (severity comes only from
     structured canonical/reconciled facts). Proven by `test_verdict_consistency.py` matrix item 10
     (forces flag ON; structured `minore` + "non presenta rischio di crollo" does **not** escalate).
  3. **No new runtime code may depend on Flag-OFF semantics.**
  4. This entry is the required deprecation record.
  5. Flag-ON canonical behavior is proven by regression tests (matrix + eight-case + repair suite).
  6. Flag-OFF preserves **byte-for-byte** pre-branch parity (proven: base golden tests pass under
     `CORRECTNESS_V2_CANONICAL_VERDICT_ENABLED=false`, 10/10).
- **Exit plan:** remove the legacy `_decision_level` keyword path (and the flag) once Branch 2 has
  soaked in production and rollback is no longer needed; tracked as future cleanup.

_Register updated as Branch 2 review cycles close each item; final states recorded in the pre-commit report._

## Branch 3 — partial lot reports (DEPLOYED + ENABLED 2026-08-14, tags `correctness-v2-partial-lot-reports-ready` + `-live`, main `2bc9ce3`)

Deployment record: feature `273a629`, dormant merge `3cb6f8b`, R3-02 merge `2bc9ce3` (= final main).
Deployed dormant (partial OFF), R3-02 deployed, then partial ENABLED via `backend/.env`. Backend PID
793760, NRestarts=0, health 200, canonical=ON/partial=ON/concurrency=2. Fable APPROVE. Production partial
smoke (read-only, flag ON, 83 cached reports): 0 disclosure changes / 0 fabrication / 0 severity change /
0 raises — safe no-op for existing customers; delta audit byte-identical (0 jobs/writes/regen/external).

| ID | Sev | Risk | Status | Control / note |
|----|-----|------|--------|----------------|
| R3-01 | S2 | Branch-3 partial path built `case_verdict.json` with `build_case_verdict([current_lot], all_lot_ids=...)`, silently turning untouched lots into `UNKNOWN` | CLOSED | Branch-3 `_finish_partial_report_available` reconstructs the complete input set from persisted `lots/<id>/canonical_verdict.json` via `_reconstruct_case_lot_verdicts`, existing case entry as conservative fallback; regression-locked by the 3-lot corruption test. Deployed 2026-08-14. |
| R3-02 | S1 | **Pre-existing (Branch-2) data-integrity defect:** the SAME singleton-list pattern at `_build_single_lot_contract` (single-lot success path) and `resolve_money_confirmation` could persist a `case_verdict.json` that downgrades unrelated untouched lots to `UNKNOWN`. | CLOSED | Fixed in follow-up branch `fix-canonical-verdict-persistence-all-lots` (merge `2bc9ce3`): both sister sites now merge every known persisted per-lot verdict (fresh verdict for the affected lot), self-healing prior corruption; `verdict_model.py`/Branch-1/2/3 semantics unchanged. Fable APPROVE (reproduced fixed on both paths). 9 R3-02 tests. DEPLOYED + production-verified 2026-08-14. |
| R3-03 | S1 | **Disclosure invariant:** unknown/new quality-gate codes must never become partial | CLOSED (design invariant) | Strict allow-list {CRITICAL_FACT_MISSING, MISSING_IMPORTANT_MONEY}; every other/unknown code → `REPORT_BLOCKED` (fail-closed, no prefix/substring escape). Locked by the full-block matrix incl. an unknown-future-code case. |
| R3-04 | S3 | Fixed-safe deterministic `why_unresolved` wording could hide which field is unresolved | CLOSED | Owner decision 2026-08-14: keep fixed-safe wording (avoids narrative drift); customer payload exposes the structured unresolved item **separately** (field/category, lot/Bene scope, reason, source page/evidence, professional-verification) via `partial_report.customer_unresolved_fields` — the sentence never hides the field. |
| R3-05 | S3 | **Process hygiene:** review/repro harnesses (Fable ad-hoc scripts) wrote synthetic job dirs into the production artifacts root (`_correctness_v2/jobs/`) during the Branch-3/R3-02 reviews | CLOSED | 5 synthetic dirs (0 PDFs, no PII) removed 2026-08-14; production restored to 119 real `cv2_` jobs. **Hard guard added (task 5, deployed):** `backend/conftest.py` forces a temp `CORRECTNESS_V2_ARTIFACTS_ROOT` + `test_r305_artifact_root_guard.py` fails immediately if any test resolves the artifact root to production; also pins Correctness-V2 feature flags for tests so they never inherit production `backend/.env` values. |

## Branch 5 — PDF retention with consent (DEPLOYED DORMANT 2026-08-15, tag `correctness-v2-pdf-retention-ready`, main `8f0d3e1`; retention flag OFF)

Deployment record: feature `9803633`, merge `8f0d3e1`. Fable APPROVE (red-team + repairs, credit-constrained so Fable implemented; orchestrator independently verified + security review, no HIGH/MEDIUM). Backend PID 885233, NRestarts=0, health 200, canonical=ON/partial=ON/**pdf_retention=OFF**/concurrency=2. Dormant deltas all zero (0 retained PDFs, 0 retention Mongo rows, 0 artifact writes/jobs/paid/credit).

| ID | Sev | Risk | Status | Control / note |
|----|-----|------|--------|----------------|
| R5-01 | S1 | Retaining an original PDF at all (customer document at rest) could leak/PII if mishandled | MITIGATED | Retention is OPT-IN (consent default OFF); flag `CORRECTNESS_V2_PDF_RETENTION_ENABLED` default OFF (deployed dormant); AES-256-GCM envelope encryption, no plaintext at rest (verified on-disk), opaque root-owned `0600` storage outside web/artifact roots. |
| R5-02 | S1 | Cross-tenant access to a retained PDF | MITIGATED | Every read/delete query binds the authenticated server-side `user_id`; ciphertext AAD binds owner+analysis lineage (undecryptable without the owner-scoped record); ops diagnostic requires exact-owner-admin + lineage verify + audit and is NOT web-mounted. Tenant-isolation tests pass. |
| R5-03 | S1 | Right-to-erasure / delete cascade not honored → orphaned PDF | MITIGATED | Withdrawal + analysis-delete + delete-all cascade to encrypted-blob deletion; crash-safe state machine (ACTIVE→DELETE_PENDING→delete→tombstone), idempotent, fail-closed 503-not-500 on genuine fault; reconciliation sweep for partial states. |
| R5-04 | S2 | Encryption-key handling / rotation | MITIGATED | Keys only in the secret store (never Git/logs/telemetry/responses); versioned (`key_version` v1/v2…), fail-closed on unknown version; envelope DEK-wrapping supports rotation. Production keys NOT yet provisioned (enablement gate). |
| R5-05 | S2 | TTL never fires / deletes early / follows arbitrary paths | MITIGATED | Standalone cleanup + systemd timer (created, NOT enabled); deletes only expired ACTIVE; path resolution confined to the retention root via validated opaque storage IDs (`O_NOFOLLOW`, symlink `lstat`); tested both directions + traversal/symlink. |
| R5-06 | S3 | (MINOR, deferred — owner) No dedicated `pdf_retention/tests/conftest.py`; the isolation fixture activates only via explicit per-file import | OPEN | Defense-in-depth only (blob store fails closed on the default root anyway). Scheduled for a later security-hardening pass; do NOT spend a cycle now (owner 2026-08-15). |
| R5-07 | S3 | (MINOR, deferred — owner) No explicit regression test tampering with the AAD-contributing record fields (analysis_id/user_id/retention_id/consent_version) | OPEN | AEAD + `ciphertext_sha256` structurally guarantee fail-closed (verified by code read + wrong-owner/wrong-analysis AAD tests by the orchestrator). Add a dedicated regression in the later security-hardening pass (owner 2026-08-15). |
| R5-08 | S2 | Enablement without safeguards (keys/root/timer/legal) | OPEN (enablement gate) | `-ready` tag only, NOT `-live`. Before enabling: provision production key(s) in the secret store, create root-owned `/srv/perizia/private` (0700), install+enable the systemd cleanup timer, obtain owner/legal GDPR consent-copy sign-off. Then flip the flag + restart, snapshot deltas, run the read-only partial/retention smoke. |

## Branch 6 — report clarity + Italian-first bilingual (DEPLOYED + ENABLED LIVE 2026-08-21, tag `correctness-v2-report-clarity-bilingual-live`, main `1bf7800`)

Deployment record: feature `c848513`, merge `1bf7800`. Fable APPROVE. Deployed dormant then ENABLED via `backend/.env`; PID 1399506, health 200, report_clarity=ON/canonical=ON/partial=ON/pdf_retention=OFF/concurrency=2. Live validation: real Gemini smoke (fidelity preserved) + cache reuse (0 additional calls) + semantic delta audit all zero. Presentation/IA + bilingual layer over Branches 1–3; no semantic re-derivation; English is translation-only, never feeds severity/status/readiness/disclosure.

| ID | Sev | Risk | Status | Control / note |
|----|-----|------|--------|----------------|
| R6-00 | S2 | UX defects: partial fallback renderer / "wall of Da verificare" / duplicate findings / hidden canonical conflicts / divergent financial renderers / six-state ambiguity / bilingual absence | CLOSED | Fixed + regression-locked (report_clarity 31 backend + 106 frontend tests, Fable APPROVE, live smoke). PARTIAL now uses the decision renderer; findings deduped by finding-id; conflicts rendered; one financial renderer; six distinct states; Italian-first + English-under. |
| R6-01 | S2 | Material CanonicalVerdict conflict classes may deserve to influence canonical severity/readiness/esito, not just presentation | **OPEN (follow-up, owner 2026-08-15; retained after deploy)** | Fable finding #3. This UX branch applies a **presentation-only** amber floor: REPORT_READY + non-empty `canonical_verdict.conflicts` never shows an unconditional green summary; instead a deterministic qualified bilingual message with a ≥amber displayed tone. **CanonicalVerdict/esito/severity/readiness/REPORT_READY are NOT changed.** Whether specific material conflict classes should change canonical severity/esito is a separate SEMANTIC-quality decision — do NOT change that policy inside this UX branch. |
| R6-02 | S3 | Translation semantic drift (Gemini could subtly alter meaning despite constraints) | OPEN (monitored) | Constraint-checked prompt + post-translation validator (numbers/dates/refs multiset + negation incl. contractions + language + untranslated guards) → Italian fallback; fail-soft (English omitted on any miss). Ongoing monitored risk; extend the validator as new drift patterns surface. |
| R6-03 | S3 | Per-Bene critical-count presentation not implemented | OPEN (deferred) | Plan §3.6 feasibility caveat (not every finding carries `bene_id`); Bene accordion shipped without per-Bene counts. Follow-up improvement. |
| R6-04 | S1 | Final tenant-isolation / security hardening still mandatory before wider beta | OPEN (programme gate) | Translate endpoint + cache are ownership-scoped (verified: `_resolve_customer_access`, per-job cache, no global-by-hash reuse), but the comprehensive tenant-isolation/security audit remains a required later programme step before wider professional beta. |
