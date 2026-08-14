# Partial Lot Reports — Architecture & Remediation Plan

Branch: `feature-correctness-v2-partial-lot-reports`
Base: `main` @ `9dd32b4` (final main; includes Branch 1 fact-lineage/projection/reconciliation
AND Branch 2 canonical verdict — both DEPLOYED LIVE, tags
`correctness-v2-lot-fact-projection-live` and `correctness-v2-case-verdict-consistency-live`).
This document is a **plan only** — no product code, tests, or configuration were changed while
producing it. Sol implements after owner review. All file:line citations below were read against
the current checked-out tree this session.

Scope guard (unchanged from Branches 1–2): no auth/beta/quota/credits/Stripe/billing/PDF-retention/
concurrency/model-provider/prompt changes; concurrency stays 2. No document-specific values (page
numbers, € amounts, lot counts) are hardcoded anywhere below or proposed for runtime code; the
sanitized fixture is referred to as "the beta multi-lot case" / "the blocked beta lot" throughout.

---

## 1. Executive summary — the suppression mechanism, traced end-to-end

**The counter-intuitive finding: nothing is actually lost today.** For the failure mode this branch
targets (a rendered report that the quality gate then fails), the full customer report — every
section, every fact, every compliance/occupancy/money row, plus the page-by-page quality-control
detail explaining exactly what is missing and why — **is fully computed and persisted to disk**. The
suppression is not a data-loss bug; it is a single boolean AND-gate that throws the entire payload
away at serve time, all-or-nothing, with no partial-content path.

The chain, traced through the real code:

1. **The report is fully rendered before the gate ever runs.** `customer_report_mod.render_success_report(contract, ...)` (`orchestrator.py:728`) builds the complete customer report — sections, compliance, occupancy, money, formalities, checklist — and unconditionally sets `report_status = REPORT_READY` (`customer_report.py:1333-1365`, `_empty_report(..., REPORT_READY, ...)`). This happens **before** any quality-gate verdict exists.
2. **The Branch-2 canonical verdict is also computed unconditionally, before the gate runs**, at `orchestrator.py:733-758` (`verdict_model_mod.build_lot_verdict(...)` then `_refine_canonical_for_report(...)`) and is **persisted regardless of outcome**: `artifacts_saved["lot_verdict"] = artifacts.save_lot_verdict(job_id, canonical_lot_id, canonical_verdict)` at `orchestrator.py:750-752`. A correct, severity-elevated (see §2.7 below) `canonical_verdict.json` for the blocked lot already exists on disk in every case this branch targets.
3. **The full report (without the canonical verdict attached) is saved to disk** at `orchestrator.py:772-774`, then handed to `quality_gate_mod.run_quality_gate(...)` (`orchestrator.py:780-796`). Inside the gate, `quality_gate.py:122-133` builds `attach_quality_section(customer_report, audit, page_audit, quality, scorecard)` — a **shallow copy** (`quality_gate.py:247`, `report = dict(customer_report)`) carrying the full fact content **plus** the page-by-page `quality_control` block — and persists it again: `saved["customer_report"] = artifacts.save_customer_report(job_id, report)` (`quality_gate.py:133`). This save happens **unconditionally**, whether the gate result is PASS, WARNING, or FAIL.
4. **On gate FAIL, the orchestrator branches on money-resolvability only** (`orchestrator.py:802-824`). If every blocking issue is a resolvable money-role ambiguity, `_finish_money_confirmation_required` is called and the canonical verdict IS attached (`orchestrator.py:816-817`, `customer_report["canonical_verdict"] = canonical_verdict`, mutating the dict in place — this happens to work only because Python dict mutation precedes the money-confirmation renderer call). **Otherwise**, `_finish_quality_gate_failed(job_id, analysis_id, gate, artifacts_saved, created_at, admin_only)` is called (`orchestrator.py:822-824`) — note `canonical_verdict` is **not even passed as an argument**. `_finish_quality_gate_failed` (`orchestrator.py:1796-1865`) does not touch `customer_report` at all; it only writes a `job_status.json` with `customer_report_generated=True` (`orchestrator.py:1837`) **and `safe_to_show_customer=False`** (`orchestrator.py:1838`), plus an `error.json` diagnostic (`orchestrator.py:1818-1830`).
5. **The serving gate is one boolean field.** `customer_view.is_customer_safe(report, job)` (`customer_view.py:526-538`): `if isinstance(job, dict) and job.get("safe_to_show_customer") is False: ... return False` — this check is **authoritative over the report's own `report_status`**, which at this point still literally says `"REPORT_READY"` inside the persisted `customer_report.json` (nothing ever changed it back). Independently, `CUSTOMER_SAFE_STATUSES` (`customer_view.py:32-38`) does **not** include `NEEDS_MANUAL_REVIEW` — belt-and-braces, but either check alone already suppresses everything.
6. **All three customer-facing API surfaces enforce the same single gate and return nothing on failure**: `/customer-view/latest` (`api.py:550-585`, `report:None` → `{"available": false, "reason_code": "VERIFICATION_REQUIRED", ...}` via `_public_unavailable_reason` at `api.py:139-170`), `/confirm-finding` (`api.py:634-635`, raises 404), `/confirm-money` (`api.py:763-771`, `"report": None`).
7. **Storico (workspace.py) enforces it a fourth time, independently.** `_state_from_status` (`workspace.py:111-123`) maps `NEEDS_MANUAL_REVIEW` into `_VERIFICATION_STATUSES` (`workspace.py:89-95`) → display state `VERIFICATION_REQUIRED`, which is **not** in `_SAFE_STATES` (`workspace.py:98`, `{REPORT_READY, MONEY_CONFIRMATION_REQUIRED}`). Consequently `_records_for_job` sets `report_filename=None` (`workspace.py:223`, `243`), `_fold_lot` sets `has_safe_report=False` (`workspace.py:262-265`), `_persisted_lot_verdict` **refuses to read the already-saved `canonical_verdict.json`** because it short-circuits on `not folded.get("has_safe_report")` (`workspace.py:432-434`), and `_actions_for` (`workspace.py:298-306`) returns only `[ACTION_RERUN]` — no `open_report`. The one partial mercy: `entry["property_type"]`/`entry["occupancy_summary"]` (`workspace.py:376,378`) fall back to the **raw, pre-reconciliation** `lot_index.json` snapshot (`meta_by_lot`, populated at `workspace.py:327-337` for every lot regardless of state) — so today's blocked-lot Storico card shows a coarse, unexplained, potentially-stale typology/occupancy string (the exact un-reconciled-snapshot risk Branch 2 R2-02/§1.2 already flagged) while the rich, reconciled, evidenced content sitting in `customer_report.json`/`canonical_verdict.json` two files away is completely unreachable.
8. **The frontend confirms total content loss.** `CustomerReportView.js:1382-1392` renders a fixed, generic "verification required" box for `reason_code === 'VERIFICATION_REQUIRED'` with zero report content. `LotWorkspace.js:397-410` renders only a retry button for `state === 'VERIFICATION_REQUIRED'`.
9. **The blocked-lot facts are structurally already provenance-tagged and ready to reuse.** `coverage_audit.py`'s `critical_omissions` list (surfaced via `_omission_view`, `coverage_audit.py:1244-1265`) already carries, per unresolved fact: `fact_id`, `category`, `document_fact`, `evidence_pages`, `severity`, `match_status`, `reason`, and (for money) `role`/`amount`/`snippet`/`confirmation_roles`. `quality_report.py`'s `blocking_issues` (`_check_blocking`, `quality_report.py:60-104` money/structural checks) already carry a closed, deterministic `code` (`CRITICAL_FACT_MISSING`, `MISSING_IMPORTANT_MONEY`, `MONEY_ROLE_MISMATCH`, `INVENTED_BUYER_COST`, `PROCEDURE_FORMALITY_AS_BUYER_DEBT`, `FAKE_PREZZO_BASE`, `BENE_LOST`, plus a lot/selection-mismatch check) with a ready-made Italian `detail` sentence, `evidence_pages` embedded in the text. Branch 1's `lot_fact_projection.REASON_CODES` (`lot_fact_projection.py:18-27`: `NOT_APPLICABLE_TO_LOT`, `DUPLICATE_EQUIVALENT`, `CONFLICT_REQUIRES_REVIEW`, `SOURCE_EVIDENCE_INSUFFICIENT`, `CUSTOMER_SAFETY_SUPPRESSION`, `SCHEMA_UNREPRESENTABLE`, `LOW_CONFIDENCE_NONCRITICAL`, `INVALIDATED_BY_STRONGER_SOURCE`) already exist for facts dropped during reconciliation. Branch 2's `verdict_model.PROVENANCE_REASONS` (`verdict_model.py:51-60`) is a closed, fail-closed-enforced set (`validate_verdict`, `verdict_model.py:82-109`, walks the whole verdict tree and rejects any `provenance_reason` not in the set). **All of the raw material this branch needs — reason, evidence, category, severity — already exists, already structured, already evidenced. No re-derivation, no re-selection of pages, no new extraction is required.**

**Worst mechanism, in one sentence:** a fully-formed, fact-complete, already-quality-audited report is computed and written to disk twice, and then a single `if job.get("safe_to_show_customer") is False: return False` (`customer_view.py:533-537`) throws the entire thing away — the architecture has no notion of "show some of it," only "show all of it" or "show none of it."

---

## 2. Full trace of every audited surface

### 2.1 Quality gate (`quality_gate.py`)
`run_quality_gate` (`quality_gate.py:29-151`) is generic and deterministic: builds `coverage_audit`
(delegates to Branch 1's `coverage_audit.build_coverage_audit`), gives non-critical page-topic
omissions a "second chance" as visible manual-review flags (`_augment_manual_review`,
`quality_gate.py:154-210` — explicitly **never** touches critical facts or money,
`quality_gate.py:173-174`), then computes `gate_status` (`quality_gate.py:135-142`: FAIL if
`quality.overall_quality_status == "FAIL"` or `audit.coverage_status == GATE_FAIL`; WARNING;
else PASS). The gate itself makes no distinction today between "nothing at all is trustworthy" and
"everything except one field is trustworthy" — `gate_status` is a single three-value enum for the
**entire report**. This branch does not change `quality_gate.py`'s PASS/WARNING/FAIL computation
(the fail-closed decision must stay exactly as strict as it is); it adds a **downstream** classifier
that looks at *why* FAIL fired (§3.B) without altering whether it fires.

### 2.2 The `NEEDS_MANUAL_REVIEW` path
`NEEDS_MANUAL_REVIEW` is emitted from at least seven distinct call sites in `orchestrator.py`
(`api.py:133` also references it for admin job listing). They fall into two materially different
classes, and this branch targets **only the first**:

- **Content-bearing FAIL** (a full report was rendered, only the quality gate rejected it):
  `_finish_quality_gate_failed` (`orchestrator.py:1796-1865`) — `customer_report_generated=True`
  (`orchestrator.py:1837`). This is the **only** entry point this branch changes.
- **Content-less FAIL** (no renderable report ever existed): `_finish_lot_ambiguous`
  (`orchestrator.py:1709-1759`, `customer_report_generated=False` at `orchestrator.py:1736`),
  `_finish_validation_failed`, `_finish_contract_build_failed`, `_finish_report_render_failed`,
  `_finish_quality_gate_error` (gate itself crashed — fail-closed, no audit to reuse). For all of
  these, `customer_report_generated=False`: there is no fact content to make partial, and this
  branch explicitly leaves them untouched (§7 "not touched").

### 2.3 Report suppression point
`customer_view.is_customer_safe` (`customer_view.py:526-538`) and `CUSTOMER_SAFE_STATUSES`
(`customer_view.py:32-38`, currently `{REPORT_READY, LOT_SELECTION_REQUIRED,
MONEY_CONFIRMATION_REQUIRED, DOCUMENT_NOT_READABLE}`). This is the single choke point; every
consumer (§2.4) calls through it.

### 2.4 Customer payload generation
`sanitize_customer_report` (`customer_view.py:541+`) is only ever invoked **after**
`is_customer_safe` has already passed (`api.py:509` gates `_find_customer_job`'s candidate set;
`api.py:634` gates `/confirm-finding`; `api.py:763` gates `/confirm-money`). A blocked lot's report
never reaches `sanitize_customer_report`, `decision_model.build_decision_model`, or any of their
projection logic today — those functions are semantically ready to render a partial report (their
`TECHNICAL_REVIEW_REQUIRED` readiness state, `decision_model.py:1987`, and `rosso`/`attenzione`
projections already exist for exactly this severity) but are simply never invoked for this case.

### 2.5 Storico lot workspace
Traced in full in §1.7 above (`workspace.py:70-123, 89-98, 223-306, 364-388, 432-439`). Key
structural fact for the design: `_persisted_lot_verdict` (`workspace.py:432-439`) already knows how
to read `LOT_VERDICT_FILE` (`canonical_verdict.json`) for a lot — it just gates on
`folded.get("has_safe_report")`, which this branch's new state must satisfy.

### 2.6 Lot selector
`orchestrator._finish_lot_selection_required` (cited in `docs/case_verdict_consistency_plan.md`
§1.2/§2.1-2.2, verified unchanged in this tree) builds `available_lots` from `lot_index.json`
(case-level, pre-reconciliation) — this is a **different** lot's-worth-of-choices screen (shown
*before* any lot is picked) and is out of scope for this branch: it does not suppress a completed
lot's content, it lists lots not yet analyzed. Not touched.

### 2.7 Decision model / readiness / checklist
`decision_model._build_readiness` (`decision_model.py:1972-2005`) already computes
`TECHNICAL_REVIEW_REQUIRED` whenever `report_status not in _CUSTOMER_SAFE_STATUSES`
(`decision_model.py:1984`, module-local set at `decision_model.py:134-137` — itself a **second,
independently-defined** copy of the same concept `customer_view.CUSTOMER_SAFE_STATUSES` encodes;
noted, not fixed, since fixing that duplication is Branch 2 territory and out of this branch's
scope) **or** any finding carries `status == "verifica_tecnica_richiesta"`
(`decision_model.py:1977`, `1986-1987`). `_build_esito` (`decision_model.py:2008-2034`) defers to
`verdict_model.project_to_esito_level(canonical_verdict)` whenever the canonical-verdict flag is on
and a verdict is present (`decision_model.py:2024-2025`) — this is the path this branch relies on:
**no new decision_model logic is needed**, only feeding it a canonical verdict with the correct
severity (see §3).

The buyer checklist (`contract._buyer_action_checklist`, `contract.py:608-650`, surfaced as
`report["buyer_checklist"]`, `customer_report.py:1361`) is a pass-through of deterministic contract
rows and is unaffected either way — it already renders whatever checklist rows the (partial)
contract produced; no change needed.

### 2.8 Cached reports (back-compat)
Every artifact this branch reads/writes is either brand-new (a `completeness`/`unresolved_fields`
key on the verdict) or already-optional (`report.get("canonical_verdict")`,
`try_validate_verdict` returns `None` on anything malformed/absent, `verdict_model.py:112-119`). A
pre-branch cached `customer_report.json`/`canonical_verdict.json` has no `completeness` key; every
new read site must default absent `completeness` to `"FULL"` (§5).

### 2.9 Monetary vs non-monetary critical findings
Already structurally distinguished in the existing code, reused (not reinvented) by this branch:
- `money_confirmation.eligible`/`_is_resolvable` (`money_confirmation.py:60-130`) already separates
  a **resolvable** money ambiguity (paused, customer-safe, deterministic re-render on answer) from
  everything else.
- `quality_report._check_blocking` (`quality_report.py:60-104`+) already tags money-shaped
  violations with distinct, closed codes: `MISSING_IMPORTANT_MONEY` (a value is simply absent —
  omission-shaped), vs. `MONEY_ROLE_MISMATCH`, `INVENTED_BUYER_COST`,
  `PROCEDURE_FORMALITY_AS_BUYER_DEBT`, `FAKE_PREZZO_BASE` (all **contradiction/fabrication-shaped**:
  a number exists in the report but is wrong, invented, or contested). This distinction is exactly
  the line this branch's eligibility rule uses (§3.B): an *omission* can become an explicit
  "unresolved" field; a *contradiction/fabrication* about money must never be softened into a
  quietly-displayed partial value — it stays full manual review, unchanged.

### 2.10 Branch 1 / Branch 2 interaction points
- **Branch 1** (`fact_lineage.py`, `lot_fact_projection.py`): consumed, not modified. Applicability
  enum (`fact_lineage.py:17-22`), drop-reason codes (`lot_fact_projection.py:18-27`), and the
  no-omission-to-LIBERO occupancy rule (already generalized product-wide by Branch 2 invariant §3.H)
  are reused verbatim as the evidence/reason vocabulary for unresolved fields.
- **Branch 2** (`verdict_model.py`): the canonical verdict is the delivery vehicle. Key facts
  established by tracing `build_lot_verdict` (`verdict_model.py:487-649`):
  - It is **already called unconditionally before the gate outcome is known**
    (`orchestrator.py:733-751`), so a correct verdict object for the blocked lot already exists.
  - Its occupancy handling (`verdict_model.py:524-534`) already implements
    no-omission-to-LIBERO correctly: a missing occupancy fact (the blocked beta lot's exact shape,
    §6.2) resolves to `value="UNKNOWN"`, never a guessed status, and (`verdict_model.py:556-557`)
    already bumps `severity` to at least `"media"` when occupancy is `UNKNOWN`. **No change needed
    to this logic.**
  - **A found, real gotcha this branch must account for** (not a defect to fix elsewhere, a fact to
    design around): `_refine_canonical_for_report` (`orchestrator.py:627-643`) is called at
    `orchestrator.py:747-749`, **before** the quality gate has run, using
    `report_status=str(report.get("report_status") or "")` — which at that point is still
    `"REPORT_READY"` (§1.1). `refine_for_runtime` (`verdict_model.py:652-684`) only forces
    `severity="grave"` when `report_status not in _SAFE_STATUSES` **or**
    `readiness.state == "TECHNICAL_REVIEW_REQUIRED"` (`verdict_model.py:663-664`). Because the
    pre-gate `report_status` is `REPORT_READY` (which *is* in `_SAFE_STATUSES`,
    `verdict_model.py:66`), the persisted `canonical_verdict.json` for a to-be-blocked lot reflects
    a **pre-gate, would-be-clean world**, not the true post-gate outcome. §3.C's design refines the
    verdict a **second time**, after the true status is known, reusing the exact same
    `_refine_canonical_for_report`/`refine_for_runtime` functions with the corrected
    `report_status` — this is additive reuse, not a change to either function.
  - `validate_verdict` (`verdict_model.py:82-109`) is permissive to new, unrecognized top-level keys
    (it only checks `schema_version`, `severity`, `scope`, `field_verdicts` is a dict, the two
    projection enums, and that every `provenance_reason` anywhere in the tree belongs to the closed
    `PROVENANCE_REASONS` set) — confirmed by reading the function body. This means the new
    `completeness`/`unresolved_fields` keys (§3.A) can be added **without bumping
    `SCHEMA_VERSION`**, so long as any new `provenance_reason` literal used is added to the closed
    set first (§3.C — the one justified edit to `verdict_model.py`).

---

## 3. Design: the PARTIAL / VERIFICATION_REQUIRED state

### 3.A Data model — additive extension of the Branch-2 CanonicalVerdict

Two new top-level keys on `CanonicalVerdict` (schema_version stays `cv2.verdict.v1`; both keys
default to their "nothing partial happened" values so an unrelated verdict is byte-identical):

```python
{
  # ... all existing cv2.verdict.v1 keys, unchanged ...
  "completeness": "FULL" | "PARTIAL",       # new, default "FULL" when absent
  "unresolved_fields": [                     # new, default [] when absent
    {
      "field": str,                # e.g. "occupancy", "field_verdicts.money.final_value"
      "category": str,             # from coverage_audit's fact category, verbatim
      "reason_code": str,          # the quality_report blocking-issue code, verbatim
                                    #   (e.g. "CRITICAL_FACT_MISSING", "MISSING_IMPORTANT_MONEY")
      "reason_human": str,         # the quality_report blocking-issue "detail" string, verbatim
                                    #   (already Italian, already page-cited — no new copy authored)
      "evidence_pages": [int, ...],# straight from coverage_audit's critical_omissions, verbatim
      "severity": "critical",      # always "critical" — only critical omissions reach this list
      "readiness_impact": str,     # fixed sentence keyed off severity, see §3.D
      "provenance_reason": "CRITICAL_FACT_UNRESOLVED",  # new, closed-set member (§3.C)
    },
    ...
  ],
}
```

The corresponding `field_verdicts` leaf (e.g. `field_verdicts.occupancy`) gains one additive key,
`"status": "UNRESOLVED"` (absent/`"RESOLVED"` = today's implicit meaning), so a consumer can tag the
specific field inline as well as read the flat `unresolved_fields` summary list. **The leaf's
`value` itself is never changed by this branch** — it is already correctly `"UNKNOWN"` for the
missing-occupancy shape (§2.10), never a fabricated guess.

### 3.B Eligibility classifier (new, pure, additive module)

New file `backend/correctness_v2/partial_report.py`, one pure function:
`classify_partial_eligibility(gate: Dict, coverage_audit: Dict) -> Optional[PartialEligibility]`.

A lot is **eligible** for partial disclosure only when **every** `blocking_issues[].code`
(`quality_report.py`'s catalog, §2.9) is one of the omission-shaped codes:
`{"CRITICAL_FACT_MISSING", "MISSING_IMPORTANT_MONEY"}`. If **any** blocking issue carries a
contradiction/fabrication-shaped code — `MONEY_ROLE_MISMATCH`, `INVENTED_BUYER_COST`,
`PROCEDURE_FORMALITY_AS_BUYER_DEBT`, `FAKE_PREZZO_BASE`, `BENE_LOST`, or the selected-lot-mismatch
check (`quality_report.py`, lot/bene-integrity block) — the lot is **ineligible** and falls through
to today's unchanged `_finish_quality_gate_failed` (full `NEEDS_MANUAL_REVIEW`, no partial content).
Rationale: an omission ("this fact is simply absent from the document") is safe to disclose
alongside everything else that *is* verified; a contradiction ("the document supports two different
numbers for this" / "an invented/mismatched value made it into the draft") means the report's
**integrity**, not just its completeness, is in question — disclosing "everything except the
contested part" would misrepresent how trustworthy the surrounding content is. This is the
mission's "monetary criticals handled at least as strictly as today" requirement, made mechanical.

When eligible, `unresolved_fields` (§3.A) is built as a **straight, lossless projection** of
`coverage_audit["critical_omissions"]` filtered to the fact_ids referenced by the eligible blocking
issues — reusing `_omission_view`'s existing shape (`coverage_audit.py:1244-1265`) and the matching
`blocking_issues[].detail` string (`quality_report.py`) for `reason_human`. No new extraction, no
new page selection, no new narrative synthesis (satisfies §3.G-equivalent no-promotional-language:
every word in `reason_human` already existed in an audited artifact).

### 3.C The one justified, minimal edit to `verdict_model.py`

Add one literal to the closed `PROVENANCE_REASONS` frozenset (`verdict_model.py:51-60`):
`CRITICAL_FACT_UNRESOLVED`. This is the mechanism the module's own design already prescribes for
extending fail-safe provenance (Branch 2's own A2 amendment did the same for
`LEGACY_ARTIFACT_NO_LEDGER`/`UNRESOLVED_LOT`/`AGGREGATION_CEILING`/`AGGREGATION_FLOOR`). No other
line of `verdict_model.py`'s existing logic changes: `validate_verdict`, `build_lot_verdict`,
`build_case_verdict`, `refine_for_runtime`, and the severity/readiness/projection tables are all
**reused as-is** (§3.D explains why they already produce the correct, conservative output once fed
the corrected `report_status`).

Everything else is new, additive code in `partial_report.py` and a small number of new call sites
(§7) — no other existing function body in `verdict_model.py`, `fact_lineage.py`,
`lot_fact_projection.py`, `validator.py`, or `quality_gate.py` is modified.

### 3.D How this flows through CanonicalVerdict, not around it

This is the load-bearing design decision: **the partial report is built by re-deriving nothing**.
The sequence, added to the existing gate-FAIL branch (`orchestrator.py:802-824`):

1. `classify_partial_eligibility(gate, gate["coverage_audit"])` (§3.B). If ineligible or the new
   feature flag (§3.E) is off, fall through to today's unchanged `_finish_quality_gate_failed` —
   byte-identical to current behavior.
2. If eligible: take the **already-computed** `canonical_verdict` (built at `orchestrator.py:739`,
   currently discarded on this path) and call the **existing**
   `_refine_canonical_for_report`-equivalent a **second time**, now passing the **true** final
   `report_status` (the new `PARTIAL_REPORT_AVAILABLE`, §3.E) instead of the stale pre-gate
   `REPORT_READY` (§2.10's gotcha). Because `PARTIAL_REPORT_AVAILABLE` is deliberately **not** added
   to `verdict_model._SAFE_INTERACTIVE`/`_SAFE_STATUSES` (§3.E), `refine_for_runtime`
   (`verdict_model.py:663-664`) unconditionally forces `severity="grave"` — every partial report
   reuses the module's own existing "not a safe status" branch to pin itself at the most serious
   band. This requires **zero new severity logic**.
3. Attach `completeness="PARTIAL"` and the `unresolved_fields` list (§3.A) to the refined verdict —
   the **only** new field-population logic in this branch.
4. `verdict_model.validate_verdict` (called internally by `refine_for_runtime`,
   `verdict_model.py:661`) runs over the result exactly as it does for every other verdict today —
   the new `CRITICAL_FACT_UNRESOLVED` provenance reason passes because of §3.C; anything else
   malformed still fails closed exactly as before.
5. Persist via the **existing** `artifacts.save_lot_verdict`/`save_case_verdict` (no new artifact
   file, no new save function). `workspace._persisted_lot_verdict` (`workspace.py:432-439`) then
   reads the identical file it already knows how to read once `has_safe_report=True` (§3.F).
6. Attach the refined verdict to `customer_report["canonical_verdict"]` and call `decision_model.
   build_decision_model` exactly as the existing REPORT_READY/MONEY_CONFIRMATION_REQUIRED paths do
   (`decision_model.py:2024-2025` already defers to `verdict_model.project_to_esito_level` whenever
   a canonical verdict is present) — **no decision_model.py code changes**.

At no point does this design re-select pages, re-run reconciliation, re-decide applicability, or
invent a value for the unresolved field. It is a pure re-labeling and re-packaging of data that
Branches 1 and 2 already computed correctly.

### 3.E New JobStatus + feature flag (mirrors the existing `MONEY_CONFIRMATION_REQUIRED` pattern)

- `schemas.py`: add `JobStatus.PARTIAL_REPORT_AVAILABLE = "PARTIAL_REPORT_AVAILABLE"` to the class
  body, `ALL_STATUSES` (`schemas.py:52-75`), and `TERMINAL_STATUSES` (`schemas.py:100-117`) —
  mirroring exactly how `MONEY_CONFIRMATION_REQUIRED` is listed in both. **Deliberately not added**
  to `FAILURE_STATUSES` (`schemas.py:79-87`) — this is not a failure, exactly like
  `NEEDS_MANUAL_REVIEW` is also excluded from that list today despite being customer-unsafe.
- `customer_view.py`: add `PARTIAL_REPORT_AVAILABLE` to `CUSTOMER_SAFE_STATUSES`
  (`customer_view.py:32-38`) — this is the single line that unblocks `is_customer_safe` and, through
  it, all three API endpoints (§2.3-2.4) with no further code change to `api.py`.
- `workspace.py`: add a new workspace-public state (e.g. `STATE_PARTIAL_REPORT_AVAILABLE =
  "PARTIAL_REPORT_AVAILABLE"`), map it in `_state_from_status` (`workspace.py:111-123`) **before**
  the generic `_VERIFICATION_STATUSES` branch, and add it to `_SAFE_STATES`
  (`workspace.py:98`) — mirroring exactly how `MONEY_CONFIRMATION_REQUIRED` is already a member.
  This alone makes `has_safe_report=True`, `report_filename` populated (`workspace.py:223,243`), and
  `_persisted_lot_verdict` (`workspace.py:432-439`) start reading the already-correct
  `canonical_verdict.json`. `_actions_for` (`workspace.py:298-306`) then naturally returns
  `[ACTION_OPEN_REPORT, ACTION_RERUN]` via its existing `_SAFE_STATES` branch — no new logic there
  either.
- `verdict_model.py`: **deliberately do not** add `PARTIAL_REPORT_AVAILABLE` to `_SAFE_INTERACTIVE`
  (`verdict_model.py:63-65`) — this is what keeps `refine_for_runtime` pinning severity to `"grave"`
  (§3.D step 2). This is a load-bearing *omission*, called out explicitly so a future refactor does
  not "helpfully" add it there and silently soften every partial report's severity.
- New flag in `feature_flags.py`, mirroring `FLAG_CANONICAL_VERDICT`/`canonical_verdict_enabled()`
  (`feature_flags.py`): `FLAG_PARTIAL_LOT_REPORTS = "CORRECTNESS_V2_PARTIAL_LOT_REPORTS_ENABLED"`,
  `partial_lot_reports_enabled() -> bool`. **Recommended default: OFF** (`_env_bool(FLAG, False)`),
  unlike Branch 2's canonical-verdict flag which defaulted ON because it was a pure bugfix with no
  new customer-visible status. This branch introduces a **new** status value and a **new** payload
  shape the frontend must recognize (§9); shipping it default-OFF lets the owner flip it on after a
  frontend smoke pass, consistent with the programme's staged-rollout discipline. The flag also
  requires `feature_flags.canonical_verdict_enabled()` to be true (a partial report has no meaning
  without the canonical verdict layer it is built on) — `partial_lot_reports_enabled()` should
  itself check `canonical_verdict_enabled()` and return `False` if that flag is off, so the two
  flags cannot be misconfigured into an inconsistent state.

### 3.F Customer report rendering (mirrors `render_money_confirmation_report`)

New function `customer_report.render_partial_report(base_report, unresolved_fields)`
(`customer_report.py`, sibling to `render_money_confirmation_report` at `customer_report.py:1445-
1467`, same pattern): `report = dict(base_report)`; `report["report_status"] =
PARTIAL_REPORT_AVAILABLE`; sets a customer-facing `title`/`subtitle` explaining a partial report;
attaches `report["partial_status"] = {"message": ..., "unresolved_fields": [...]}` — a **new**,
additive top-level key, exactly analogous to how `money_confirmation` is attached today
(`customer_report.py:1463-1466`) and to how `quality_gate.attach_quality_section`
(`quality_gate.py:235-273`) already attaches `quality_control`. Every existing section
(`risk_sections`, `money_sections`, `occupancy_section`, `compliance_section`,
`formalities_section`, `buyer_checklist`, ...) is preserved **verbatim** from the already-rendered
`render_success_report` output — this function does not re-render or re-filter a single section; it
only overlays the status/partial-status metadata, exactly as the money-confirmation renderer does.

### 3.G Case-level aggregation interaction

A case containing one partial lot must not let the case-level `build_case_verdict`
(`verdict_model.py:687+`) silently present the case as clean. Because this branch pins the lot's
`severity="grave"` (§3.D step 2), Branch 2's existing ceiling/floor invariants
(`case_verdict_consistency_plan.md` §3.F, already implemented and regression-locked) already ensure
`case.severity >= max(lot.severity)` includes this lot's `grave` — **no change needed to
`build_case_verdict`**. The only new case-level consideration: `build_case_verdict` should surface,
additively, a case-level `partial_lots: [lot_id, ...]` list (derived by filtering
`lot_verdicts` for `completeness == "PARTIAL"`) so a case overview can say "3 of 4 lots complete, 1
partial" rather than only a flat severity ceiling. This is optional polish, not required for
correctness (the ceiling/ floor invariants already make a partial lot's severity visible at the
case level); flagged in §7 as an optional addition, not a required one.

---

## 4. Invariants (numbered, testable)

1. **Never fabricate a substitute value.** No unresolved field's `field_verdicts` leaf `value` is
   ever changed by this branch; it remains whatever Branch 1/2's existing reconciliation already
   produced (`"UNKNOWN"` for occupancy, absent for a missing compliance/formality row). Test:
   assert the leaf `value` before and after partial-report construction is byte-identical.
2. **Uncertainty is always visible.** Every entry in `unresolved_fields` is rendered in the
   customer payload (`report["partial_status"]["unresolved_fields"]`) — none is ever silently
   dropped from the list between `coverage_audit.critical_omissions` and the customer payload. Test:
   count parity between eligible `critical_omissions` and rendered `unresolved_fields`.
3. **Fail-closed is preserved.** A lot is eligible for `PARTIAL_REPORT_AVAILABLE` only through the
   narrow `classify_partial_eligibility` gate (§3.B); any contradiction/fabrication-shaped blocking
   code forces the existing, unchanged `NEEDS_MANUAL_REVIEW` path. Test: one fixture per
   ineligible code (`MONEY_ROLE_MISMATCH`, `INVENTED_BUYER_COST`,
   `PROCEDURE_FORMALITY_AS_BUYER_DEBT`, `FAKE_PREZZO_BASE`, `BENE_LOST`) asserts the job status is
   still `NEEDS_MANUAL_REVIEW` with `safe_to_show_customer=False`, unchanged from today.
4. **A partial lot never inflates readiness.** `refine_for_runtime` always yields `severity="grave"`
   for a partial verdict (§3.D step 2); `readiness` is always `TECHNICAL_REVIEW_REQUIRED`;
   `esito_level` is always `"rosso"`; `decision_level` is always `"attenzione"`. Test: assert these
   four values regardless of which/how-few fields are unresolved.
5. **A partial lot never inflates case-level severity/readiness.** Branch 2's existing ceiling/floor
   (unchanged) already guarantees `case.severity >= grave` whenever any completed lot is partial.
   Test: reuse Branch 2's `test_verdict_consistency.py` matrix items 3/5/6 with one lot forced
   partial.
6. **Monetary criticals are handled at least as strictly as today.** No blocking issue with a
   contradiction/fabrication-shaped code ever reaches `unresolved_fields`; a `MISSING_IMPORTANT_MONEY`
   omission surfaces as an explicit unresolved field with **no amount rendered**, never an estimate.
   Test: assert `unresolved_fields` entries derived from `MISSING_IMPORTANT_MONEY` never carry a
   non-null `amount`/`amount_display` key.
7. **Every `unresolved_fields` entry is provenance-complete.** `reason_code`, `reason_human`, and
   `evidence_pages` (or an explicit `[]` with a `reason_code` explaining absence, never a silently
   missing key) are always present. Test: schema assertion on every entry.
8. **`try_validate_verdict`/`validate_verdict` still fail closed on anything malformed**, including
   the new keys: an `unresolved_fields` entry with a `provenance_reason` outside the closed set is
   rejected exactly like any other malformed verdict today. Test: corrupt one entry's
   `provenance_reason`, assert `try_validate_verdict` returns `None`.
9. **No cross-lot leakage.** `unresolved_fields`/`completeness` are computed strictly from the
   single lot's own `coverage_audit`/`gate` output, never from another lot's or the case-level
   ledger. Test: multi-lot fixture, assert lot A's partial verdict never references lot B's
   `fact_id`s or evidence pages.
10. **Flag-OFF (or ineligible) reproduces today's behavior exactly.** With
    `CORRECTNESS_V2_PARTIAL_LOT_REPORTS_ENABLED=false`, every code path in `orchestrator.py`,
    `customer_view.py`, `workspace.py`, `schemas.py` behaves byte-identically to pre-branch
    `main@9dd32b4`. Test: the eight-case + beta-Lot-4 regression run twice (flag on/off), assert the
    flag-off run's outputs are unchanged from the pre-branch baseline.
11. **Old cached reports still render.** A `customer_report.json`/`canonical_verdict.json` persisted
    before this branch (no `completeness`/`partial_status` key) is read as `completeness="FULL"`
    everywhere, with no crash and no behavior change. Test: load a pre-branch fixture through every
    new read site.

---

## 5. Backward compatibility

- `completeness` absent on a verdict is always treated as `"FULL"` (helper:
  `verdict.get("completeness", "FULL")`, never a bare `verdict["completeness"]` access anywhere new
  code reads it).
- `unresolved_fields` absent is always treated as `[]`.
- `report["partial_status"]` absent is simply not rendered by the frontend (§9) — no crash, exactly
  like `report["money_confirmation"]` being absent today causes no issue for a `REPORT_READY`
  report.
- `schema_version` stays `"cv2.verdict.v1"` — no migration, no version-gate needed for the new keys
  (§2.10, `validate_verdict` is permissive to unknown top-level keys).
- The new `JobStatus.PARTIAL_REPORT_AVAILABLE` is a **new** terminal status; it never replaces or
  aliases `NEEDS_MANUAL_REVIEW` for any job that predates this branch — a pre-branch job's persisted
  `job_status.json` still says `NEEDS_MANUAL_REVIEW` and is read exactly as before (`is_customer_safe`
  still returns `False` for it, since `job.safe_to_show_customer` was persisted `False` and
  `report_status` inside the old `customer_report.json` is unaffected).
- With the flag off, `classify_partial_eligibility` is never called and the gate-FAIL branch
  (`orchestrator.py:802-824`) is byte-identical to today.

---

## 6. Test plan

### 6.1 Generic matrix (new, `test_partial_lot_reports.py`)
1. Pure non-monetary critical omission (occupancy missing, mirrors the beta Lot-4 shape): eligible,
   `completeness="PARTIAL"`, one `unresolved_fields` entry, `severity="grave"`,
   `readiness="TECHNICAL_REVIEW_REQUIRED"`.
2. `MISSING_IMPORTANT_MONEY` only, no resolvable roles (money-confirmation `eligible()` already
   returned `False` upstream): eligible for partial, the money row renders with no amount, an
   explicit unresolved-field entry with `reason_code="MISSING_IMPORTANT_MONEY"`.
3. `MONEY_ROLE_MISMATCH` present alongside an otherwise-eligible occupancy omission: **ineligible**
   — falls through to unchanged `NEEDS_MANUAL_REVIEW`, zero partial content, per invariant 3.
4. `INVENTED_BUYER_COST` alone: ineligible, unchanged `NEEDS_MANUAL_REVIEW`.
5. `BENE_LOST`: ineligible, unchanged `NEEDS_MANUAL_REVIEW`.
6. Two independent non-monetary critical omissions in the same lot: both appear in
   `unresolved_fields`, both evidenced, no cross-contamination between their `evidence_pages`.
7. Flag off: identical fixture to (1), assert output equals pre-branch `_finish_quality_gate_failed`
   behavior exactly (job status, `safe_to_show_customer=False`, no `customer_report` change).
8. Old cached verdict (no `completeness` key) read through every new consumer: renders as FULL, no
   crash.
9. Case-level: one partial lot + three clean lots — case severity/readiness ceiling reflects the
   partial lot's forced `"grave"`, per invariant 5.
10. Storico: a partial lot's workspace entry has `state="PARTIAL_REPORT_AVAILABLE"`,
    `has_safe_report=True`, `actions=[open_report, rerun]`, and its `property_type`/
    `occupancy_summary` come from the **canonical** `field_verdicts` (reconciled), not the raw
    `lot_index` snapshot — closing the exact provenance gap noted in §1.7/§2.5.
11. API: `/customer-view/latest` for a partial lot returns `available=True` with
    `report.report_status == "PARTIAL_REPORT_AVAILABLE"` and a populated `partial_status` block.
12. `try_validate_verdict` rejects a verdict with an out-of-set `provenance_reason` on an
    `unresolved_fields` entry (invariant 8).
13. No PII / no hardcoded historical-case values in the new module or its tests (string-scan
    assertion, matching the pattern already used by `test_verdict_consistency.py` item 19).

### 6.2 Eight-case no-regression
Run the existing eight-case golden regression (Torino, Pistoia, 1859886_C, Orecchiazzi, Cairate,
Codogno, Mantova, beta-multilot) with the flag **on**: none of the eight cases' seven "clean" lots
should be affected (this branch only changes behavior for a lot that reaches
`_finish_quality_gate_failed`, which by construction none of the seven single-lot golden fixtures
do). Then run with the flag **off**: byte-identical to the pre-branch eight-case baseline (10
passed, exit 0, per the programme doc's Branch-2 deployment record).

### 6.3 Beta Lot-4 partial-report acceptance (the primary regression specimen)
The sanitized beta multi-lot fixture already ships a ready-made blocked-lot builder:
`tests/beta_fixture.py::build_lot("4")` (`backend/correctness_v2/tests/beta_fixture.py`), exercised
today by `test_beta_multilot_case_regression.py::test_beta_multilot_cross_lot_and_blocked_lot_facts`
(`backend/correctness_v2/tests/test_beta_multilot_case_regression.py:61-71`), which already asserts:
`lot4["case"]["blocked_lot_id"] == "4"`, `lot4["worksheet"]["legal_formalities"] == []` (a genuinely
valid, verified fact — no legal formalities on this lot), and `"Stato occupativo critico mancante"
in lot4["worksheet"]["missing_or_uncertain"]` (the missing critical occupancy fact). This is the
exact "reliable-facts-present, one-critical-field-missing" shape the mission specifies — **no new
fixture is needed**.

New acceptance test (extends `test_beta_multilot_case_regression.py`, or a new
`test_beta_multilot_partial_report.py`): run `build_lot("4")`'s rendered `customer_report`/
`worksheet` through the **real** `quality_gate.run_quality_gate` → `classify_partial_eligibility` →
partial-verdict-refinement chain (not just the worksheet-level assertions the existing test already
makes) and assert:
- The lot is classified eligible (its blocking issue is the missing-occupancy critical omission —
  no money contradiction/fabrication code present in this fixture's Lot 4).
- Address (`"Via del Tiglio 2, Borgo Esempio"`, already asserted line 68) and the empty
  `legal_formalities` finding are present, verbatim, in the rendered partial customer report.
- Exactly one `unresolved_fields` entry, `field="occupancy"`, non-empty `reason_human`, and
  `evidence_pages` matching the worksheet's own evidence for that omission.
- `severity="grave"`, `readiness="TECHNICAL_REVIEW_REQUIRED"`, `esito_level="rosso"`,
  `decision_level="attenzione"` — the partial report is still visibly the most serious band, per
  invariant 4.
- Zero fabricated occupancy status anywhere in the rendered output (string-scan for any of the
  vacant-equivalent tokens, `verdict_model._VACANT_EQUIVALENTS`, `verdict_model.py:74`, appearing
  without the `UNKNOWN`/"da verificare" qualifier).

Reuses the **offline replay pattern** already established by Branch 1/2
(`backend/correctness_v2/scripts/offline_historical_replay.py`, `--bundle` required with no default,
no network, no paid calls, no production writes) for any assertion that needs the full preserved
forensic bundle rather than the sanitized fixture; the acceptance test above, however, needs only
the already-checked-in sanitized fixture (`tests/fixtures/beta_multilot_case_sanitized.json` +
`..._cached_pages_sanitized.json`), consistent with how the existing Branch-1/2 beta-fixture tests
already run without `--bundle`.

---

## 7. Files expected to change

### New (backend)
- `backend/correctness_v2/partial_report.py` — `classify_partial_eligibility`, the
  omission→`unresolved_fields` projection (§3.A/§3.B).
- `backend/correctness_v2/tests/test_partial_lot_reports.py` — the matrix in §6.1.
- (Optional, extends existing file) `backend/correctness_v2/tests/
  test_beta_multilot_partial_report.py` — §6.3.

### Modified (backend, additive wherever possible)
- `backend/correctness_v2/schemas.py` — new `JobStatus.PARTIAL_REPORT_AVAILABLE`,
  `ALL_STATUSES`/`TERMINAL_STATUSES` membership (§3.E).
- `backend/correctness_v2/feature_flags.py` — new `FLAG_PARTIAL_LOT_REPORTS`,
  `partial_lot_reports_enabled()` (§3.E).
- `backend/correctness_v2/customer_view.py` — `CUSTOMER_SAFE_STATUSES` gains the new status
  (`customer_view.py:32-38`); no other line changes.
- `backend/correctness_v2/workspace.py` — new `STATE_PARTIAL_REPORT_AVAILABLE`, one new branch in
  `_state_from_status`, one new member in `_SAFE_STATES` (`workspace.py:98,111-123`).
- `backend/correctness_v2/verdict_model.py` — **one line**: add `CRITICAL_FACT_UNRESOLVED` to
  `PROVENANCE_REASONS` (`verdict_model.py:51-60`). No other line changes (§3.C).
- `backend/correctness_v2/customer_report.py` — new `render_partial_report` function, sibling to
  `render_money_confirmation_report` (`customer_report.py:1445-1467`) (§3.F).
- `backend/correctness_v2/orchestrator.py` — the gate-FAIL branch (`orchestrator.py:802-824`) gains
  the eligibility check + new `_finish_partial_report_available` function (new, sibling to
  `_finish_money_confirmation_required` at `orchestrator.py:1868-1920`); the second
  `refine_for_runtime` call with the corrected `report_status` (§3.D).
- `backend/correctness_v2/artifacts.py` — no new file constants required; reuses
  `LOT_VERDICT_FILE`/`CASE_VERDICT_FILE`/`save_lot_verdict`/`save_case_verdict` verbatim.

### Modified (frontend, narrow — see §9)
- `frontend/src/components/correctness-v2/CustomerReportView.js` — new branch alongside the existing
  `reason_code === 'VERIFICATION_REQUIRED'` check (`CustomerReportView.js:1382-1392`) for
  `report_status === 'PARTIAL_REPORT_AVAILABLE'`, rendering the existing report body (all sections
  already render generically) plus a new banner reading `report.partial_status`.
- `frontend/src/components/correctness-v2/LotWorkspace.js` — new case in the state-label map
  (`LotWorkspace.js:29,57`) for `PARTIAL_REPORT_AVAILABLE`, and the existing `open_report`/`rerun`
  action rendering (`LotWorkspace.js:397-410`) already generalizes once the backend reports the new
  state in `actions` — no new action-handling code needed there.

### Explicitly not touched
`fact_lineage.py`, `lot_fact_projection.py` (core reconciliation — consumed only),
`validator.py`, `quality_gate.py` (PASS/WARNING/FAIL computation — consumed only, `run_quality_gate`
itself is not modified), `coverage_audit.py`, `quality_report.py` (blocking-issue catalog reused,
not modified), `money_confirmation.py`, `lot_runner.py`, `decision_model.py` (relies entirely on its
existing canonical-verdict-aware `_build_esito` path, `decision_model.py:2024-2025`),
`contract.py`, `lot_packets.py`, auth/beta/billing/Stripe/concurrency files, `analyst.py`
(prompt/schema — no prompt change, §8), `perizia_authority_lot_projection.py`,
`perizia_canonical_pipeline/*`, any visual styling/layout files.

---

## 8. Prompt-change verdict

**No prompt change is required or proven necessary.** Every mechanism traced in §1-2 is a
serve-time/aggregation-time suppression problem: the underlying facts are already correctly
extracted (Branch 1), reconciled (Branch 1), and verdict-computed (Branch 2) before the suppression
ever happens. This branch's entire job is to stop throwing away already-correct data, not to extract
anything new or differently. No change to `analyst.py`'s prompt, schema, or model call is proposed
or needed.

---

## 9. Frontend scope (narrow, explicit)

Frontend changes are limited to **recognizing two new server-computed values** the backend already
fully assembles — no new client-side derivation, no visual redesign:

1. `CustomerReportView.js` gains a branch for `report_status === 'PARTIAL_REPORT_AVAILABLE'` that
   renders the **same** report body components used for `REPORT_READY` (sections already render
   generically from server-provided data) plus a small, new banner component that lists
   `report.partial_status.unresolved_fields` (field name, reason, page evidence) verbatim — no
   client-side severity/readiness computation, mirroring the existing rule that this codebase's
   frontend never re-derives severity (`case_verdict_consistency_plan.md` §2.10's finding that the
   existing components are display-only).
2. `LotWorkspace.js` gains one label-map entry for the new state
   (`STATE_PARTIAL_REPORT_AVAILABLE` → e.g. "Report parziale — verifica richiesta"); the existing
   action-rendering code (`open_report`/`rerun` buttons) already generalizes over whatever the
   backend's `actions` array contains, so no new button-handling logic is needed.
3. No other component changes. No layout, spacing, or visual-design work is in scope for this
   branch (that is Programme task 6, `feature-correctness-v2-report-clarity`). If the owner wants
   the partial banner co-designed with that later branch instead of shipped now, the backend change
   still stands alone: with the flag off, no frontend change is even reachable.

---

## 10. Rollback plan

Single feature flag, `CORRECTNESS_V2_PARTIAL_LOT_REPORTS_ENABLED` (default OFF, §3.E). Flipping it
off:
- Reverts the gate-FAIL branch (`orchestrator.py:802-824`) to calling
  `_finish_quality_gate_failed` unconditionally, byte-identical to pre-branch behavior (invariant
  10).
- No stored artifact needs migration in either direction: a `canonical_verdict.json` written with
  `completeness="PARTIAL"` while the flag was on is simply never read as partial again once the flag
  is off (workspace/customer_view fall back to their unchanged pre-branch status checks, which never
  look at `completeness`).
- No schema version bump, so no artifact becomes unreadable in either direction.
- If a deeper rollback is needed, the change set is small enough (one new module, ~6 modified files,
  each a narrow additive diff) for a straightforward branch-commit revert with no data cleanup.

---

## 11. Red-team section

**(a) Could a partial report accidentally fabricate or mislead?**
Risk: a future edit could set `field_verdicts.<field>.value` to something other than the existing
`UNKNOWN`/absent sentinel when building the partial verdict, "to make the card look nicer." Mitigated
by invariant 1 (byte-identical leaf value, tested explicitly) and by design: §3.D's flow **never
touches `field_verdicts` leaf values**, only adds the sibling `status`/`unresolved_fields` metadata.
A code reviewer checking this branch's diff for any line that assigns into an existing
`field_verdicts.<x>.value` is the concrete review gate.

Secondary risk: `reason_human` text drifts from being a verbatim quote of an audited
`blocking_issues[].detail` string into hand-authored narrative that could overstate or understate the
problem. Mitigated by design: §3.B explicitly reuses the existing string verbatim, and invariant 7's
test can include a string-equality assertion (not just non-empty) against the source
`blocking_issues[].detail`.

**(b) Could this reopen fail-open?**
Risk: the eligibility classifier (§3.B) is too permissive and lets a contradiction/fabrication-shaped
blocking code slip into "partial" territory (e.g., a future new blocking code added to
`quality_report.py` that is contradiction-shaped but not added to the classifier's ineligible list).
Mitigated by inverting the check: `classify_partial_eligibility` should be an **allow-list** (only
`{"CRITICAL_FACT_MISSING", "MISSING_IMPORTANT_MONEY"}` are eligible; everything else — including any
new code added later — is ineligible by default), not a deny-list of currently-known bad codes. This
is the single most important implementation detail from this section; the plan's §3.B is written as
an allow-list for exactly this reason, and the test matrix (§6.1 items 3-5) should include a
synthetic "unknown future code" case asserting it defaults to ineligible.

**(c) Could a partial report inflate readiness?**
Risk: someone "fixes" §3.E's deliberate omission (not adding `PARTIAL_REPORT_AVAILABLE` to
`verdict_model._SAFE_INTERACTIVE`) because it looks inconsistent with how
`MONEY_CONFIRMATION_REQUIRED`/`LOT_SELECTION_REQUIRED` are handled. Mitigated by invariant 4's test
(hard-asserts `severity=grave`/`readiness=TECHNICAL_REVIEW_REQUIRED`/`esito=rosso` regardless of how
few fields are unresolved) plus the explicit callout in §3.E and §3.D step 2 explaining *why* the
omission is load-bearing — any PR that adds the membership will fail invariant 4's test immediately.

**(d) Could this leak facts across lots?**
Risk: `unresolved_fields`/evidence pages computed from the wrong lot's `coverage_audit`, or a shared
CASE_GLOBAL fact's evidence pages bleeding into another lot's unresolved-field entry. Mitigated
because `classify_partial_eligibility` and the projection in §3.A operate strictly on the **single
lot's own** `gate`/`coverage_audit` object already scoped by the orchestrator's existing per-lot
call (`orchestrator.py:780-796`, `lot_id=lot_id` already threaded through) — no new cross-lot read is
introduced. Invariant 9's multi-lot test directly checks this.

**(e) Could this break the CanonicalVerdict schema/provenance contract?**
Risk: `unresolved_fields` entries missing a `provenance_reason`, or using a reason string not in the
closed set, silently passing validation. Mitigated by §3.C's design: `CRITICAL_FACT_UNRESOLVED` is
added to `PROVENANCE_REASONS` *before* any code emits it, and `validate_verdict`
(`verdict_model.py:82-109`) already walks the **entire** verdict tree recursively checking every
`provenance_reason` key it finds (confirmed by reading the function — it is not scoped to
`field_verdicts` only, so a `provenance_reason` on an `unresolved_fields` list entry is caught by the
same existing walk with zero additional validation code). Invariant 8's test corrupts one entry and
asserts `try_validate_verdict` returns `None`, proving the existing fail-closed walk covers the new
key without any new validation logic being written (and therefore without a new place for that
logic to be wrong).

Additional risk not in the mission's named list but found during this trace: **schema-version drift
across a mid-flight deploy** (the same class of risk Branch 2's red-team §11.3 already flagged) — a
partial verdict built by an older `verdict_model.py` (pre-`CRITICAL_FACT_UNRESOLVED`) queued
alongside code that now expects the new provenance reason. Because `SCHEMA_VERSION` is unchanged
(§2.10), `try_validate_verdict` cannot distinguish "old code, no partial support" from "new code,
partial support" purely by version string. Mitigated in practice because `unresolved_fields`/
`completeness` are only ever *written* by the new `_finish_partial_report_available` path, which
does not exist in older code — an in-flight job started before deploy either completes through the
old `_finish_quality_gate_failed` path (no partial keys written) or is queued/retried after deploy
(new code, consistent). No verdict is ever partially written by two different code versions in this
design, since the whole partial-verdict construction happens synchronously within one orchestrator
call. Flagged for Sol to confirm during implementation with a targeted test (build a verdict with
today's `verdict_model.py`, assert an `unresolved_fields`-using consumer degrades gracefully — falls
back to `completeness="FULL"` behavior — when handed a verdict that has no such keys at all, which is
exactly invariant 11's test).

---

## 12. Summary of the worst suppression mechanisms found (for the executive review)

1. **The all-or-nothing boolean gate** (`customer_view.is_customer_safe`, `customer_view.py:526-
   538`, driven by `job.safe_to_show_customer` set at `orchestrator.py:1838`) discards a fully
   rendered, fully quality-audited, already-fact-complete report because of a single unresolved
   field — with no partial-content code path anywhere in the architecture to fall back to.
2. **The canonical verdict is computed correctly and persisted to disk for every blocked lot today,
   and then never used** — `orchestrator.py:733-758` builds it, `orchestrator.py:750-752` saves it,
   and `orchestrator.py:822-824` (the plain quality-gate-FAIL path) discards it without ever passing
   it to the finish function. The fix this branch proposes is almost entirely "stop throwing this
   away," not "compute something new."
3. **Storico quietly shows an unreconciled, unexplained snapshot instead of the reconciled, evidenced
   one** for a blocked lot (`workspace.py:376,378` falling back to raw `lot_index` when
   `has_safe_report=False`) — the one place today's system already leaks *some* partial information,
   but from the wrong (pre-reconciliation) source and with zero explanation, which is arguably worse
   than either full suppression or a correctly-sourced partial report.

No product code, tests, or configuration were modified while producing this plan. All reads were
against the current checked-out tree (`main@9dd32b4` state on this branch) using `Read`/`grep` only.

---

## 13. Owner mandatory amendments (2026-08-14) — BINDING, supersede any looser wording above

These amendments are binding and take precedence wherever the plan above is looser.

**13.1 Three-state disclosure model (separate from severity).** Introduce an explicit disclosure
state distinct from CanonicalVerdict severity: `FULL_REPORT_AVAILABLE` / `PARTIAL_REPORT_AVAILABLE` /
`REPORT_BLOCKED` (repository-appropriate names, but preserve this separation). Flow is: Branch 1
reconciled CanonicalFacts → Branch 2 CanonicalVerdict → quality/readiness evaluation → **disclosure
state**. `PARTIAL_REPORT_AVAILABLE` is NOT a severity, NOT a new verdict engine, NOT a replacement for
CanonicalVerdict, NOT permission to fabricate, NOT permission to increase readiness. CanonicalVerdict
remains authoritative. Severity example: `severity=TECHNICAL_REVIEW_REQUIRED` +
`disclosure_state=PARTIAL_REPORT_AVAILABLE` — never `severity=PARTIAL`.

**13.2 Allow-list + fail-closed default.** Partial-disclosable classes (initial): `CRITICAL_FACT_MISSING`,
`MISSING_IMPORTANT_MONEY` — and only when the failure is genuine omission/incompleteness while the rest
of the report is internally trustworthy. Remain fully blocked: `MONEY_ROLE_MISMATCH`,
`INVENTED_BUYER_COST`, `FAKE_PREZZO_BASE`, `BENE_LOST`, all contradiction/integrity/fabrication-shaped
failures, cross-lot contamination, ownership/scope ambiguity, unresolvable schema corruption, and **any
code whose safety classification is unknown**. **Unknown/new blocking codes MUST default to
`REPORT_BLOCKED`** — never default a newly introduced code to partial.

**13.3 Field-level unresolved contract.** Each unresolved item carries (where available): field/category,
lot/Bene scope, reason code, severity, why-unresolved, source page(s), source fact IDs/provenance,
evidence availability, effect on decision readiness, professional-verification-required flag, monetary
role when applicable. Never send internal enums to customer text — map through the established display
boundary.

**13.4 Never fabricate a substitute.** For a missing monetary critical: do not infer/borrow a value,
relabel market value as auction base, compute a minimum bid unless explicitly supported, use €/m² as the
missing total, or silently drop the field. Show reliable values, show the missing value explicitly as
unresolved, state what must be verified, preserve evidence. Non-money absence stays absence/unknown.

**13.5 Readiness fail-closed.** `PARTIAL_REPORT_AVAILABLE` can never increase readiness vs the underlying
blocked state; never green/ready merely because most of the report is available; case-level aggregation
stays conservative — a partial lot must not make the case more ready than before.

**13.6 Preserve all reliable content — fix the discard directly.** Do not rebuild from a reduced schema,
do not rerun analysis, reselect pages, or re-decide applicability. Preserve CanonicalVerdict, Branch-1
projected facts, validated values, occupancy, typology, compliance, formalities, evidence, and conflicts
unrelated to the blocking field; add unresolved-field info.

**13.7 Customer endpoint consistency.** Every path gated by `safe_to_show_customer` (customer report
endpoint, Storico, lot workspace/selector, cached reopen, customer decision report, report detail,
checklist/readiness payload, any download/export, any summary endpoint) must derive the SAME disclosure
state. No "readable here / gone there" divergence.

**13.8 Cached/historical read-time exposure.** Where the reconciled report + CanonicalVerdict already
exist in stored artifacts, the disclosure classifier may expose an eligible historical partial at read
time — with no regeneration, model/Document-AI call, credit debit, beta consumption, or artifact rewrite.
When the stored artifact lacks trustworthy info, fail closed. Never invent compatibility data from prose.

**13.9 Tenant isolation.** Any new endpoint/read path must preserve ownership scoping — never expose a
partial report merely because a caller knows case/analysis/job/lot/report IDs. Authorization stays tied
to authenticated ownership. Record any discovered risk in `docs/perizia_scan_risk_register.md`.

**13.10 Quality-gate accounting (safe metadata only).** Preserve the gate-failure reason; add safe
diagnostics: gate outcome, original blocking codes, which were partial-eligible, which stayed
hard-blocking, disclosure state, unresolved-field count, critical-unresolved count. No raw appraisal
content in telemetry.

**13.11 Test mandates.** Beta Lot-4 golden acceptance (15 assertions incl. reliable facts visible,
unresolved money explicit, no substitute, full-readiness false, severity not downgraded, case readiness
not improved, no cross-lot leak, no raw enums). Full-block regression matrix (10 codes stay blocked:
invented buyer cost, fake auction base, money role mismatch, lost Bene, severe contradiction, cross-lot
contamination, schema corruption, ambiguous ownership/scope, fabricated evidence, unknown future code).
Generic partial matrix (24 cases). Eight-case corpus unchanged (Orecchiazzi/Cairate fail-closed NOT
weakened). Branch-1 (Lot-1 coverage 100%, zero leakage) and Branch-2 (CanonicalVerdict authoritative, no
omission→LIBERO, no unsupported minor→severe, selector/report consistency, severity unchanged by
disclosure) invariants explicitly proven.

**13.12 Fable red-team (12 attacks).** Fabrication→partial; unknown-code→partial; hide unresolved
critical; unresolved field disappears between Storico and report; partial increases readiness; partial
downgrades severity; valid fields lost on blocked→partial switch; cross-lot leak; missing money acquires
substitute; cached/legacy overconfident; ownership bypass via new read path; CanonicalVerdict vs
disclosure state becoming conflicting authorities. Repair cycles until no confirmed blocker/high finding.

---

## 14. Amendment (2026-08-14) — SECOND suppression path + spec completions (BINDING)

Sol's plan-validation (correctly) found the §1/§2 trace incomplete. Verified against code:

**14.1 There are TWO content-bearing suppression paths; both are in scope.**
- **Single-lot / selected-lot:** `_finish_quality_gate_failed` (`orchestrator.py:1796`, called from
  `:822` and `:2108`). Covered by §1–§13.
- **Multi-lot analyze-all:** `_run_analyze_all` (`orchestrator.py:1018+`) persists each lot's full
  `render_success_report` + `save_lot_verdict` + runs `run_quality_gate`, then at **`orchestrator.py:1390-1394`**
  suppresses a failed lot INLINE (`gate_status == GATE_FAIL → entry.status = NEEDS_MANUAL_REVIEW,
  reason "quality_gate_failed"`) **without ever calling `_finish_quality_gate_failed`**. The overall
  analysis status is set at `:1453` (`REPORT_READY if all_ok else NEEDS_MANUAL_REVIEW`).
  **The sanitized external-beta case is MULTI-LOT, so its blocked Lot-4 travels THIS path** — the
  mandatory §8/§13.11 beta Lot-4 acceptance cannot pass unless the disclosure classifier + partial
  construction are applied here too.
- **Binding:** the disclosure-state classifier and partial-report construction (§3, §13) must be applied
  at BOTH suppression sites identically — each eligible failed lot (per the §1/§13.2 allow-list, unknown
  → BLOCKED) is routed to `PARTIAL_REPORT_AVAILABLE`, reusing the ALREADY-persisted per-lot report +
  CanonicalVerdict (no rebuild, no rerun, no reselect, no re-decide-applicability), plus the
  unresolved-field contract. Case/analysis-level status (`:1453`) and case-level readiness aggregation
  stay conservative (§13.5): a partial lot never makes the overall analysis look more ready than before.
  Storico/selector for a blocked-or-partial lot must read the reconciled report/verdict, never the
  pre-reconciliation `lot_index` snapshot (fixing the §2.5/§2.8 bug).

**14.2 Full 24-case generic partial matrix (supersedes the 13 enumerated in §6.1).**
1. One missing critical non-money fact + otherwise valid → partial. 2. One missing important monetary
value + otherwise valid → partial. 3. Multiple omission-shaped unresolved fields → partial when
independently safe. 4. Unknown gate reason → BLOCKED. 5. Fabrication + missing fact → BLOCKED.
6. Contradiction + missing fact → BLOCKED. 7. Valid sections preserved exactly. 8. Unresolved field never
silently disappears. 9. No substitute money generated. 10. Partial report cannot be READY. 11. Partial
lot cannot inflate case readiness. 12. Existing FULL reports remain FULL. 13. Existing unsafe reports
remain blocked. 14. Cached historical eligible report becomes partial without regeneration. 15. Old
cached report with insufficient structured evidence remains BLOCKED. 16. Customer endpoints agree on
disclosure state. 17. Storico agrees with opened report. 18. Checklist agrees with report.
19. CanonicalVerdict unchanged by the disclosure classifier. 20. Branch-1 facts/provenance unchanged.
21. Branch-2 semantic result unchanged. 22. No model/API call needed. 23. No credit/beta consumption.
24. No production writes during replay.

**14.3 Spec-reference clarification.** The binding sources are this plan (§1–§14) plus Sol's brief.
Any "§N" cross-reference that points beyond §14 (e.g. an owner-message "§17 validation", "§8/§9/§10")
refers to the OWNER MESSAGE's own section numbering, now folded into §13 (mandatory amendments) and
§14 here and the brief's validation/test sections — not to a plan section. There is no plan §15–§17.

**14.4 Baseline correction.** The correct full-backend baseline on the untouched base (`main@9dd32b4`,
includes Branch 2) is **1667 passed / 7 known-stale failed / 7 skipped** (matches the deployment record;
the brief's "1665" was the stale Branch-2-branch count). Do not add failures to 1667.
