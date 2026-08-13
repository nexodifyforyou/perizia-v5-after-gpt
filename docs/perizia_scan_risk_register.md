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
