# Case/Lot Verdict Consistency — Architecture & Remediation Plan

Branch: `feature-correctness-v2-case-verdict-consistency`
Base: `main` @ `31be7f5` (tag `correctness-v2-lot-fact-projection-live` — includes all Branch 1
fact-lineage/projection/reconciliation infrastructure: `fact_lineage.py`, `lot_fact_projection.py`,
the widened validator/coverage-audit page sets, the hardened compliance-evidence-gate detector).
This document is a **plan only** — no product code was changed while producing it; Sol implements
after review.

> **AMENDMENTS A1–A2 (2026-08-13, verified against production code).** During the required
> plan-validation step Sol identified two internal contradictions between this plan's requirements and
> Branch 1's *actual persisted contract*. Both were verified against code by the orchestrator and are
> resolved in **§12 (Amendments)** below, in the only direction consistent with the owner's
> non-negotiable invariants and Gate 7. **§12 supersedes any conflicting wording in §3.C, §4.1, §4.11,
> and §6.1 item 1.** Read §12 before implementing those sections.

Scope guard: no auth/beta/quota/credits/Stripe/billing/PDF-retention/concurrency/model-provider
changes. Prompt changes only with proven necessity (none found — see §8). Frontend touched **only**
where a verdict/severity/status value is independently re-derived client-side, never for visual
redesign (that is Branch 5). No document-specific values (page numbers, € amounts, lot counts,
wording unique to the beta case) are hardcoded anywhere below; the historical case is referred to
as "the beta multi-lot case" and cited only to prove a mechanism exists.

---

## 1. Executive summary of what was traced

Six semantic values (typology, occupancy, severity, compliance status, value/money, formality
status) and their "verdict" derivatives (executive decision, esito, badges, checklists) currently
have **between two and five independently-coded sources of truth each**, built by different modules
at different pipeline stages, with **no shared precedence rule and no cross-check that they agree**.
Branch 1 fixed the specific mechanism by which a lot's *own* re-analysis lost facts that lived on
shared pages; it did **not** touch the fact that the *customer-facing verdict layer* (executive
decision box, esito semaphore, case overview/selector, Storico workspace) is built by several
independent re-derivations of the same underlying facts, each with its own rules. This is a
structurally different class of contradiction, and the two confirmed worst mechanisms below
reproduce end-to-end, this cycle, using the actual production code:

1. **Two independent, disagreeing verdict computations for the same lot report.**
   `customer_view.derive_decision` (`customer_view.py:277`, using `_decision_level` at
   `customer_view.py:233-274`) and `decision_model.build_decision_model`'s `_build_esito`
   (`decision_model.py:2007-2060`, via `_build_readiness` at `decision_model.py:1971-2004`) are two
   **entirely separate functions**, each re-deriving a semaphore-like verdict from the same
   `customer_report` dict, using materially different rules. Reproduced directly against the real
   functions with one constructed report (single `compliance_section` item classified `"uncertain"`,
   otherwise clean — no risk items, no occupancy risk, no money issues):
   ```
   customer_view.derive_decision(report)["level"]              -> "da_verificare"  ("needs check")
   decision_model.build_decision_model(report, [])["esito"]["level"] -> "verde"    ("nothing blocking found")
   ```
   Both fields are unconditionally computed and attached to every sanitized customer payload
   (`customer_view.py:505,516,539` — `sanitize_customer_report` always sets both `"decision"` and
   `"decision_model"`), so both values exist simultaneously for every report. The frontend currently
   shows only one of the two per screen state (`CustomerReportView.js:1267-1279` picks
   `CustomerDecisionReport` — which reads `decision_model.esito` — whenever `decision_model` is
   present **and** `report_status === 'REPORT_READY'`; otherwise it falls back to
   `V2CustomerReportFallback`, which reads `report.decision` at `CustomerReportView.js:1241`), but
   `report.decision` is served to **every** consumer of the sanitized payload regardless of which
   screen the current frontend chooses to render it on — any other surface (a future case overview,
   an export, an admin preview, a mobile client) that reads `report.decision` independently would
   show a different verdict than what `CustomerDecisionReport` shows for the identical report.

2. **The customer-facing lot selector / case overview is built from a code path that never goes
   through Branch 1's reconciliation.** `orchestrator._finish_lot_selection_required`
   (`orchestrator.py:1404-1439`) builds `available_lots` directly from `lot_index.get("lots")` —
   i.e. `lot_packets.build_lot_index`'s output, itself built once from the **case-level** (full
   document, unreconciled) worksheet before any lot is selected. `customer_report.
   render_lot_selection_report` (`customer_report.py:1470-1564`) renders this into the customer's
   first-seen screen (`report["lot_selection"]["lots"][].occupancy_summary`/`property_type`), and
   `workspace.build_workspace` (`workspace.py:305-406`, `meta_by_lot` built at `workspace.py:320-330`
   from `_best_lot_index` at `workspace.py:172-178`) feeds the same case-level, unreconciled
   `occupancy_summary`/`property_type` into the Storico case overview
   (`frontend/src/components/correctness-v2/LotWorkspace.js:324-325`), while
   `workspace._final_value_display` (`workspace.py:409-421`) pulls its **money** figure from the
   fully-reconciled, per-lot `customer_report` for the *same* lot card. One card therefore mixes two
   provenances: typology/occupancy from the never-reconciled case-level snapshot, value from the
   fully-reconciled report. Verified against the beta case's real `lot_index.json`
   (the preserved beta case's `job_artifacts/<job_id>/lot_index.json` in the read-only
   forensic bundle):
   the case-overview/selector already showed a specific, correct occupancy detail for one lot
   ("locato a terzi con contratto 4+4") **before** Branch 1's fix, at the exact same time that lot's
   own dedicated report showed a vague, detail-free "incerto" (the bug Branch 1 fixed) — i.e. the
   selector and the lot report were **already telling the customer two different things about the
   same lot**, just in the safe direction that time. Nothing in the code enforces that they must
   agree; only coincidence made them agree once Branch 1 landed. A future change to either path can
   silently reopen this without any test catching it, because no test compares the two.

3. **A raw internal enum can leak into a customer-facing summary sentence, unlocalized.**
   `contract._risk_cards` (`contract.py:136-193`), when a `technical_compliance` item's `notes` field
   is empty, falls back to `f"{item.get('area')}: {classification}"` at `contract.py:163` — embedding
   the **raw English enum token** (`"non_conforming"`, `"regularizable"`, `"not_regularizable"`,
   `"uncertain"`) directly into the customer-facing risk-card `summary` string, bypassing the
   canonical Italian label mapper `_classification_it` (`contract.py:599-600`) used everywhere else
   (`_compliance_overview`'s `classification_label` at `contract.py:674`,
   `_buyer_action_checklist` at `contract.py:609`). This is a live, reachable narration path that is
   not itself severity-escalating, but demonstrates there is **no single place** where a
   classification enum becomes customer-facing text — exactly the drift surface a unified verdict
   model must close.

4. **Independent, ad hoc "is this a real problem" text-scanning outside the structured severity
   fields.** `customer_view._decision_level` (`customer_view.py:233-256`) escalates to
   `_DECISION_ATTENZIONE` when free-text `area`/`summary` content matches a hardcoded keyword list —
   `"collabente", "crollo", "amianto", "fibrocement", "pericol"` (`customer_view.py:241-249`) —
   **independent of** the structured `severity`/`classification` fields on the same item. This is a
   genuine "narration can move severity independent of structured facts" path (trace duty 9): if the
   analyst's structured `severity` says `"minore"` but the free-text `summary` happens to contain one
   of these words in an unrelated sense (e.g. quoting a *ruled-out* risk — "non presenta rischio di
   crollo"), the escalation still fires, because the keyword scan does not check for negation the way
   `validator._has_negative` (`validator.py:111-112`) does for compliance text. This is the same
   *class* of false-certainty bug Branch 1 spent seven cycles hardening in `validator.py`, now found
   independently reimplemented, unhardened, in a different module.

**Severity vocabulary count.** Four independently-defined severity/priority taxonomies coexist:
worksheet `risk_classification[].severity` (`grave|media|minore|info`, `analyst.py:43`);
`contract._risk_cards`'s own classification→severity re-mapping (`contract.py:152-159`, a *second*
independent mapping to the same four words); `decision_model`'s numeric UI-ordering priority
`_SEV_FINAL_VALUE(1)...`.`_SEV_CONTEXT(7)` (`decision_model.py:50-56`, assigned per finding-builder
function at nine different call sites); and `doc_signals.SEV_CRITICAL/IMPORTANT/USEFUL/BACKGROUND`
(Branch 1, used only for coverage-audit purposes today). None of the four are defined in terms of
any other, and nothing prevents them from disagreeing about the same fact.

---

## 2. Full trace (file:line) per requested source

### 2.1 Typology (`property_type`)
- Analyst output: `analyst.py:442` (`case_identity.property_type`), `analyst.py:527`
  (`lots[].property_type`).
- Fact ledger / projection (Branch 1): `fact_lineage.py:214,221` (case-identity and per-lot identity
  facts), `lot_fact_projection.py:264-284` (`_reconcile_identity`, the only place typology is
  actually **reconciled** across case/lot).
- Case-level per-lot index (pre-reconciliation, "case overview" source): `lot_packets.py:760,831`
  (`build_per_lot_packets`/`build_lot_index` reading `ws_lot.get("property_type")` from the
  case-level worksheet).
- Contract (per-lot, post-reconciliation): `contract.py:119-120` (`_executive_summary_facts`, labeled
  "Tipologia").
- Customer report: `customer_report.py:492,565,573,649,691-698,1174,1300,1515` (title/subtitle
  construction, beni-section titles, "real property" detection, evidence scanning).
- Decision model: `decision_model.py:380,396` (`"tipologia"` field in `acquisto`/`numeri` sections).
- Case overview / selector: `orchestrator.py:1430`, `customer_report.py:1512-ish` (lot_selection
  view), `workspace.py:327,364` (Storico), `LotWorkspace.js:324` (frontend render).

### 2.2 Occupancy
- Analyst: `analyst.py` occupancy block (`case_identity`-adjacent `occupancy{status,title_info,...}`
  and `lots[].occupancy_status`), normalized in `analyst.normalize_worksheet`.
- Fact ledger / projection (Branch 1): `fact_lineage.py:220-232` (occupancy + risk facts),
  `lot_fact_projection.py:177-224` (`_reconcile_occupancy` — the one place case-level and lot-level
  occupancy are actually merged, including the "never silently convert to LIBERO" rule at
  `lot_fact_projection.py:209`).
- Case-level per-lot summary (pre-reconciliation): `lot_packets.py:201-202` (`_occupancy_summary`),
  surfaced into `lot_index.json` and hence into the selector/Storico paths.
- Contract: `contract.py:648-659` (`_occupancy_view`, reads the worksheet's `occupancy` block
  verbatim — whatever worksheet it is handed, reconciled or not).
- Customer report: `customer_report.py:861-889ish` (`_occupancy_section`).
- Decision model: `decision_model.py:910-964` (`_occupancy_why`, `_build_occupancy`) — its OWN
  wording/severity assignment (`_SEV_OCCUPANCY` at `decision_model.py:51,1450,2127`).
- **Independent re-derivation #1**: `customer_view._occupancy_is_occupied`
  (`customer_view.py:167-171`) — a separate boolean test (`status == "occupato"` or a keyword match
  on `status_label`) used only by `derive_decision`, not shared with `decision_model._build_occupancy`.
- Case overview: `lot_packets.py` `occupancy_summary` in `lot_index.json` → `workspace.py:329,366` →
  `LotWorkspace.js:325`.

### 2.3 Severity (of a risk/compliance/formality finding)
- Worksheet enum: `analyst.py:43` (`SEVERITY_LEVELS = {grave, media, minore, info}`),
  `risk_classification[].severity`.
- Contract's independent re-mapping: `contract.py:152-159` (`_risk_cards`, classification →
  severity: `non_conforming`→`grave`, `not_regularizable`→`grave`, `regularizable`→`media`,
  `uncertain`→`minore`).
- Decision model's independent numeric priority: `decision_model.py:50-56` (`_SEV_FINAL_VALUE`
  through `_SEV_CONTEXT`), assigned ad hoc at `decision_model.py:890,1246,1370,1427,1434,1450,1464,
  1494,1523,1536,1828,2127` — nine+ separate assignment sites, several with inline literals
  (`"severity": 0`, `"severity": 3`, `"severity": 5`) not drawn from the named constants at all.
- Branch 1's severity taxonomy (audit-only today): `doc_signals.SEV_CRITICAL/IMPORTANT/USEFUL/
  BACKGROUND`, consumed by `coverage_audit.py` and `fact_lineage.py` — not currently wired to any
  customer-facing severity.
- **Independent re-derivation #2**: `customer_view._decision_level` (`customer_view.py:233-256`) —
  its own `critical` boolean from `severity == "grave"` **OR** `blocks_saleability` **OR** a raw
  keyword match on free text (`customer_view.py:241-249`, no negation guard — see §1 finding 4).

### 2.4 Compliance status
- Enum + gate: `analyst.py:35-41` (`COMPLIANCE_CLASSES`), `validator.apply_compliance_evidence_gate`
  (`validator.py:727+`, hardened over Branch 1's seven cycles).
- Reconciliation (Branch 1): `lot_fact_projection.py:146-174` (`_reconcile_compliance`).
- Label mapping: `contract.py:590-600` (`CLASSIFICATION_LABELS_IT`, `_classification_it`) — the
  **one correct, canonical** place a classification becomes Italian customer text, bypassed at
  `contract.py:163` (§1 finding 3).
- Views: `contract.py:662-682` (`_compliance_overview`), `customer_report.py`
  `_compliance_section`/`_risk_sections`.
- **Independent re-derivation #3**: `customer_view._decision_drivers` (`customer_view.py:199-215`)
  re-reads `compliance_section[].classification` directly (its own `cls in {...}` checks) to build
  free-text "driver" sentences, a third consumer of the same enum with its own rules, separate from
  both `contract._risk_cards` and `decision_model`'s conformity-finding builder
  (`decision_model.py:965-1266`, `_build_conformity_findings`).

### 2.5 Value / money labels
- Worksheet + reconciliation: `analyst.py` `money` block; Branch 1's `lot_packets.build_lot_money`
  and `lot_packets.contract_rows_from_lot_money` (label-aware merge, hardened in Branch 1).
- Contract: `contract.py:396-573` (`_money_sections`), `contract.py:762-846`
  (`_merge_shared_summary_rows`, label-aware since Branch 1 cycle 2).
- Customer report: `customer_report.py` `_money_sections_view`, `format_eur`.
- Decision model: `decision_model.py:488-591` (`_build_numeri`), `decision_model.py:367-419`
  (`_build_acquisto`).
- **Independent re-derivation #4**: `workspace._final_value_display` (`workspace.py:409-421`) reads
  the *rendered customer_report's* `money_sections.valuation_chain` directly (last row with an
  `amount_display`) — a fourth, separate value-extraction path used only for the Storico case-overview
  card, with its own "last row wins" heuristic rather than any canonical "final value" field.

### 2.6 Formality status
- Worksheet: `analyst.py` `legal_formalities[]` (`type`, `cancelled_by_procedure`, `buyer_burden`).
- Reconciliation (Branch 1): `lot_fact_projection.py:226-245` (`_reconcile_formality`).
- Contract: `contract.py:706-719` (`_legal_formalities_view`).
- Customer report: `customer_report.py` `_formalities_section`.
- Decision model: `decision_model.py:1288-1396` (`_build_formalita`).
No independent re-derivation was found for formalities specifically (the smallest-surface source of
the six), but it shares the same "each layer re-reads the raw worksheet enum with its own logic"
shape as the other five.

### 2.7 Transformations into case- and lot-level summaries
`lots.build_lot_report` (case-level lot detection) → `lot_packets.segment_pages` +
`lot_packets.build_lot_index`/`build_per_lot_packets` (case-level per-lot snapshot; feeds the
selector and Storico) → **[selection point, no reconciliation yet]** →
`analyst.run_analyst(selected_pages, target_lot=...)` (fresh, isolated lot re-analysis) →
`fact_lineage.build_case_fact_ledger` + `lot_fact_projection.project_and_reconcile` (Branch 1,
per-lot reconciliation) → `contract.build_contract` (per-lot canonical contract) →
`customer_report.render_success_report` (per-lot customer report) →
`decision_model.build_decision_model` (per-lot esito/findings) **and independently**
`customer_view.derive_decision` (per-lot executive decision) — both attached to the same
`sanitize_customer_report` payload (`customer_view.py:486-541`). There is currently **no case-level
aggregation step at all** beyond the pre-reconciliation `lot_index`/selector/Storico views — i.e.
"case overview" today literally means "the un-reconciled snapshot", not "an aggregate of the
reconciled per-lot verdicts". This is itself a design gap this branch must close.

### 2.8 Precedence rules today
None exist across views. Branch 1 introduced ONE precedence rule, and it is scoped narrowly to
*within* `lot_fact_projection.project_and_reconcile` (case-fact vs. this-lot's-own-re-analysis-fact,
§5.C of the Branch 1 plan). No rule exists between: `decision` vs. `decision_model.esito`; the
case-level `lot_index`/selector vs. the reconciled per-lot report; `contract._risk_cards`'s severity
vs. `decision_model`'s numeric priority; or any of the "independent re-derivation" sites in §2.1-2.4.

### 2.9 Narration / promotional severity paths (trace duty 9)
- `contract.py:163` — raw enum leak (§1.3).
- `customer_view.py:241-249` — keyword-based severity escalation with no negation guard (§1.4).
- `decision_model.py`'s finding-builder functions construct `customer_summary`/`buyer_impact`
  narrative strings largely from structured fields verbatim (`item.get("notes")`,
  `item.get("summary")`) rather than synthesizing new severity language — this is the safer pattern
  and should become the template the unified model enforces everywhere (see invariant §4.7).
- No evidence was found of narration *softening* a structured severe finding (the risk found runs in
  the escalating direction only, in `customer_view.py`).

### 2.10 Frontend mappings (`frontend/src`)
- `components/correctness-v2/shared.js:8-26,30-50` — `compactText`/`StatusChip`: pure display
  formatting and a `tone → CSS class` map (`CHIP_TONES`). Takes a server-computed tone string; does
  **not** re-derive severity itself. Safe.
- `components/correctness-v2/CustomerReportView.js:149-171` (`CustomerDecisionBox`, reads
  `decision.level`/`decision.label`/`decision.headline`/`decision.drivers` verbatim),
  `CustomerReportView.js:1239-1252` (`V2CustomerReportFallback`, backward-compat renderer using
  `report.decision`), `CustomerReportView.js:1254-1287` (`CustomerReportBody`, the `? :` branch that
  picks `CustomerDecisionReport` (decision_model/esito) vs. the fallback (decision) — §1.1's dual
  surface). No client-side severity computation; the risk is entirely about **which of two
  server-computed verdicts** gets shown, and only one non-server consumer of `report.decision`
  needs to exist for the divergence to become customer-visible (§1.1).
- `components/correctness-v2/LotWorkspace.js:298-328` — renders `lot.property_type`,
  `lot.occupancy_summary`, `lot.final_value` verbatim from `workspace.build_workspace()`'s per-lot
  entry; no re-derivation, but renders the mixed-provenance card described in §1.2.
- `components/correctness-v2/CustomerDecisionReport.js` — renders `decision_model` fields only
  (esito, findings, sections); does not reference `report.decision` at all (grep-confirmed).
- `lib/periziaPrintModel.js` — **out of scope**: this is the legacy/old-pipeline print-model (fields
  like `decision_rapida_client`, `section_2_decisione_rapida`, `semaforo`), used only by
  `pages/AnalysisPrintView.js`. It does not reference `decision_model`, `compliance_section`,
  `occupancy_section`, or `money_sections` at all (grep-confirmed) — it is unrelated to
  correctness_v2 and was not touched or analyzed further, consistent with the branch's frontend
  scope guard.

---

## 3. Design: one canonical verdict contract

### 3.A Canonical verdict object (new, additive)

A single, versioned `CanonicalVerdict` shape, produced by ONE new deterministic function per level
(lot and case), consumed by every existing view instead of each view re-deriving its own:

```python
{
  "schema_version": "cv2.verdict.v1",
  "scope": "lot" | "case",
  "scope_id": str,                 # lot_id, or the analysis_id for a case-level verdict
  "severity": "grave" | "media" | "minore" | "info" | "verde",  # ONE closed enum, see §3.B
  "severity_label_it": str,        # from ONE label table, never inlined elsewhere
  "readiness": "TECHNICAL_REVIEW_REQUIRED" | "CONFIRMATIONS_REQUIRED" |
               "READY_FOR_REVIEW" | "COMPLETE_FOR_EXPORT",   # reuses decision_model's existing enum
  "headline": str,                 # single customer sentence, structured-field-derived only
  "drivers": [{"driver_id": str, "source_fact_id": str, "text": str}],  # every driver traceable
  "field_verdicts": {               # per-dimension detail, always present, never omitted
    "typology": {"value": str, "confidence": str, "source_fact_id": str},
    "occupancy": {"value": str, "confidence": str, "source_fact_id": str},
    "compliance": [{"area": str, "classification": str, "source_fact_id": str}],
    "formalities": [{"type": str, "status": str, "source_fact_id": str}],
    "money": {"final_value": {"amount": float, "role": str, "source_fact_id": str}},
  },
  "provenance": {"case_fact_ids": [...], "lot_fact_ids": [...], "aggregation_rule": str},
  "conflicts": [{"path": str, "case_value": Any, "lot_value": Any, "reason_code": str}],
  "generated_from": {"contract_schema_version": str, "worksheet_schema_version": str},
}
```

This is **additive**: it does not replace `decision`, `decision_model`, `lot_index`, or
`customer_report` — it becomes the single computation both `decision` and `decision_model.esito`
delegate to, and the single source `lot_index`/selector/Storico read for typology/occupancy instead
of the raw case-level worksheet snapshot.

### 3.B One severity enum, one label table

Retire the four independent taxonomies (§1's severity-vocabulary-count finding) behind **one**
mapping table, `verdict_model.SEVERITY_LABELS_IT` (new), built from the *existing* four-value
worksheet vocabulary (`grave|media|minore|info`) plus `"verde"` for "nothing found" — this is the
vocabulary already used by `analyst.py`/`contract._risk_cards`, so no worksheet schema change is
needed. `contract._risk_cards`'s own classification→severity mapping (`contract.py:152-159`) and
`decision_model`'s `_SEV_*` numeric constants become two **derived views** of this one table (the
numeric constants may stay, for UI ordering purposes only, but are computed FROM the canonical
severity, never assigned independently per finding-builder call site as they are today).

### 3.C Field-level provenance

Every leaf value in `field_verdicts` carries a `source_fact_id` pointing into the Branch 1
`case_fact_ledger`/`lot_fact_projection_report` fact-id space (`fact_lineage.py`'s `fact_id` format,
already stable and deterministic) — this is deliberately the **same** provenance mechanism Branch 1
already built, extended to cover severity/typology/formality-status facts the same way it already
covers compliance/occupancy/money facts, rather than inventing a second provenance format.

### 3.D Deterministic precedence rules (new, generalizing Branch 1's §5.C)

1. A **lot-level** verdict is authoritative for that lot's own report — it wins over anything a
   case-level view might otherwise compute, whenever a lot has completed reconciliation
   (Branch 1's `project_and_reconcile` has run for it).
2. A **case-level** verdict (new — see §3.E) is an explicit **aggregation** of completed lot
   verdicts, never an independent re-derivation from the case-level (pre-reconciliation) worksheet.
   Until a lot's reconciliation has completed, its contribution to the case verdict is marked
   `UNRESOLVED`, never silently omitted and never guessed from the raw case-level snapshot.
3. Within `field_verdicts`, the SAME reconciliation precedence Branch 1 already defined for
   worksheet fields (exact lot-specific evidence > explicit declarations > generic case inference;
   `lot_fact_projection.py:146-283`) is reused verbatim — this branch does not invent a second
   precedence system, it plugs the *verdict* layer into the *fact* layer's existing one.
4. Any of the "independent re-derivation" sites found in §1/§2 (`customer_view._decision_level`,
   `customer_view._occupancy_is_occupied`, `customer_view._decision_drivers`,
   `contract._risk_cards`'s severity mapping, `workspace._final_value_display`) is rewritten to
   **read** the canonical verdict's corresponding `field_verdicts` entry instead of re-scanning the
   underlying report — this is the single largest concrete code change this branch makes (§7).

### 3.E Case-level aggregation (new)

`verdict_model.build_case_verdict(lot_verdicts: List[CanonicalVerdict]) -> CanonicalVerdict`:
- `severity` = the **ceiling rule** (§3.F) applied over all completed lots' severities.
- `field_verdicts.typology`/`occupancy` are **not** a single scalar at case level (a multi-lot case
  legitimately has different typology/occupancy per lot) — the case verdict's `field_verdicts`
  instead carries a `per_lot: {lot_id: field_verdicts}` map plus an explicit `heterogeneous: bool`
  flag, so a UI never has to guess "one" case-wide typology/occupancy the way today's
  `lot_index`-based selector implicitly does when it shows a flat list without any warning that the
  four lots disagree.
- `conflicts` at the case level include, additively, any place two lots' OWN verdicts materially
  disagree in a way that suggests cross-lot contamination (e.g. two lots both independently reporting
  the exact same specific compliance defect wording, which would suggest a projection leak rather
  than two genuinely identical defects) — a new, case-level-only contamination check, not present in
  Branch 1's per-lot reconciliation.

### 3.F Severity ceilings and floors (mandatory invariant)

- **Ceiling**: `case.severity` may never exceed `max(lot.severity for lot in completed_lots)`
  **unless** the case verdict has its own `field_verdicts` entry with a `source_fact_id` pointing to
  a genuinely `CASE_GLOBAL`/`ALL_LOTS` fact (Branch 1's applicability enum) that is *itself* severe
  and does not appear, downgraded, in any lot's own verdict. In other words: the case can only be
  "worse than every lot" when it has its own independent, source-backed evidence for that — never as
  an artifact of aggregation logic alone.
- **Floor**: `case.severity` may never be *lower* than `max(lot.severity for lot in completed_lots)`
  either, symmetrically — a case overview can never quietly present as "less serious" than its worst
  completed lot. (This directly targets contradiction class 1: "case overview says severe... while
  lot source says minor" is the ceiling violation; the *dual*, less obvious risk — "case overview
  says fine while a lot is actually severe" — is the floor violation, and is exactly the shape of the
  `lot_index`/selector-vs-reconciled-report gap found in §1.2.)
- A lot still `UNRESOLVED` (not yet reconciled/generated) contributes `UNKNOWN`, not `verde`/`info`,
  to both the ceiling and the floor computation — an un-generated lot must never make the case look
  artificially safe.

### 3.G No-promotional-language invariant

Every `headline`/`drivers[].text`/narrative string in the canonical verdict is generated **only**
from: (a) verbatim `notes`/`summary` text already present on a structured fact, or (b) the ONE
`SEVERITY_LABELS_IT` table (§3.B). No function in the verdict-building path may introduce a
severity-carrying word (`grave`, `critico`, `pericoloso`, `urgente`, etc.) that is not already
present on the structured fact it is describing — this closes both §1.3 (raw enum leak) and §1.4
(keyword-triggered escalation independent of structured severity) by construction: the *only*
severity-classifying logic left in the system is the canonical mapper, and every other site becomes
a passthrough of its output.

### 3.H No-omission-to-LIBERO invariant (across ALL paths, not just Branch 1's lot projection)

Formalized as: for any occupancy field surfaced anywhere in the product (per-lot
`occupancy_section`, `decision_model` occupancy finding, `customer_view._occupancy_is_occupied`,
case-level `lot_index`/selector/Storico `occupancy_summary`), the value may only be a
"vacant/unoccupied"-equivalent string when a source fact with `declaration_status` in
`{explicit_declaration}` (Branch 1's `fact_lineage` vocabulary) and non-empty `evidence_pages`
supports it. Absence of an occupancy fact must render as `UNKNOWN`/`"da verificare"`, never inferred
as vacant, *whichever code path renders it* — this generalizes Branch 1's §9 invariant 10 (which was
scoped to `lot_fact_projection._reconcile_occupancy` only) to also cover the case-level
`lot_index`/selector/Storico path (§1.2) and `customer_view._occupancy_is_occupied` (§2.2), neither
of which Branch 1 touched.

### 3.I No minor-to-severe escalation without explicit source evidence

A `field_verdicts` entry's severity may only be `grave` when its `source_fact_id` resolves to a fact
whose own worksheet-level classification/severity is independently `grave`/`non_conforming`/
`not_regularizable` (or an occupancy/formality fact meeting the equivalent bar) — never as a result
of a *narration-layer* keyword match (closes §1.4) or a *aggregation-layer* ceiling miscalculation
(closes the inverse of §3.F's ceiling rule).

---

## 4. Invariants (numbered, testable)

1. `decision.level` and `decision_model.esito.level` are never independently computed for the same
   report; both are projections of the one canonical verdict.
2. The case-level lot selector (`lot_selection_required`/`render_lot_selection_report`) and the
   Storico case overview (`workspace.build_workspace`) read `field_verdicts` from the canonical
   verdict for any lot that has completed reconciliation — never the raw case-level
   `lot_index`/worksheet snapshot for a lot once that lot has a reconciled report.
3. `contract._risk_cards`'s summary fallback never emits a raw classification enum token; every
   classification-derived string passes through `_classification_it`/`SEVERITY_LABELS_IT`.
4. `customer_view._decision_level`'s keyword-based escalation is removed in favor of reading the
   canonical verdict's severity; if retained transitionally, it must apply the same negation guard
   as `validator._has_negative`.
5. Case severity never exceeds the max of completed lots' severities without an explicit,
   source-backed `CASE_GLOBAL`/`ALL_LOTS` fact of its own (ceiling, §3.F).
6. Case severity never falls below the max of completed lots' severities (floor, §3.F).
7. An unresolved/not-yet-generated lot contributes `UNKNOWN`, never a safe default, to case
   aggregation.
8. No narrative string anywhere in the verdict path introduces a severity word absent from the
   structured fact it describes (§3.G).
9. Occupancy never renders as vacant/unoccupied without an explicit, evidenced source fact, in any
   of: per-lot report, decision model, `customer_view` executive decision, case-level selector,
   Storico workspace (§3.H).
10. A compliance/risk/formality classification is never escalated to `grave` without a
    source-backed, independently-`grave`-equivalent fact (§3.I).
11. Every `field_verdicts` leaf carries a non-empty `source_fact_id` resolvable in the fact ledger.
12. Conflicts between case and lot verdicts are always represented explicitly in
    `conflicts[]`, never silently resolved by picking one side.
13. Old stored `customer_report`/`decision_model`/`lot_index` artifacts (no canonical verdict block)
    still load and render via the existing fields, unchanged, exactly as before this branch.
14. The eight-case regression suite (seven original + Branch 1's beta-multi-lot fixture) is
    unaffected in output for every case that has no cross-view divergence today (i.e. this branch is
    a no-op for single-view-consistent reports).
15. No runtime module references the historical beta case's specific page numbers, lot count, or
    € figures; they may appear only in fixtures/golden assertions/docs.

---

## 5. Backward compatibility

- The canonical verdict is a **new, additive** artifact/field; every existing `customer_report`/
  `decision_model`/`lot_index`/`workspace` JSON shape is unchanged in its existing keys. A stored
  artifact from before this branch has no canonical verdict block; every consumer falls back to
  computing it on read (a pure function of the already-stored data), not by requiring a rewrite of
  historical artifacts.
- `decision`/`decision_model.esito` remain present with their existing shapes (label/level/headline
  fields unchanged) — they become **derived views** of the canonical verdict rather than
  independent computations, so any frontend code reading them today needs no change to keep working;
  only their *values* become consistent with each other going forward.
- `lot_index.json`'s existing keys (`occupancy_summary`, `property_type`, etc.) are preserved; the
  selector/Storico code paths are changed to *prefer* the canonical verdict's `field_verdicts` when a
  reconciled lot report exists, falling back to the existing raw snapshot only for a lot that has
  never been generated — no schema removal.

---

## 6. Test plan

### 6.1 Generic matrix (new, `test_verdict_consistency.py`)
1. Single lot, clean report: `decision.level == decision_model.esito.level` derived, both from one
   canonical computation.
2. Single lot, one uncertain compliance area, otherwise clean: canonical severity is consistent
   across `decision`/`esito`/case overview (regression test for §1.1's exact reproduction).
3. Multi-lot, one lot severe / others clean: case ceiling equals the severe lot's severity, not an
   average or the first lot's.
4. Multi-lot, all lots clean, one lot still unresolved (not yet generated): case verdict shows
   `UNKNOWN` contribution for the unresolved lot, not `verde`.
5. Case-level `CASE_GLOBAL` fact independently severe, no lot reflects it yet: case severity may
   exceed lot max ONLY with this fact's `source_fact_id` populated and visible.
6. Occupancy explicit lease fact present in case ledger, lot's own re-analysis silent: verdict
   occupancy is not vacant anywhere (selector, Storico, decision, esito) — direct regression for
   §1.2's historical mechanism.
7. Compliance area classified `grave` at case level but the specific lot's reconciled fact is
   `regularizable`/`sanabile`: lot verdict shows `regularizable`, never re-escalated to `grave` by the
   case rollup (direct regression for the mission's named contradiction class).
8. Typology mismatch between case-level worksheet and a lot's reconciled identity fact: lot verdict
   wins (per precedence rule §3.D.1); conflict recorded, not silently dropped.
9. `contract._risk_cards` with empty `notes`: summary contains the Italian label, never the raw
   enum token (regression for §1.3).
10. Keyword-escalation regression: a risk item with structured `severity == "minore"` whose free text
    contains a negated danger word ("non presenta rischio di crollo") does not escalate to
    `attenzione`/`grave` (regression for §1.4).
11. Case overview / selector rendering for a lot with a completed reconciled report shows the SAME
    occupancy/typology as that lot's own report (direct assertion the two views cannot diverge).
12. Case overview rendering for a never-generated lot shows the existing raw case-level snapshot,
    clearly marked as unconfirmed/pre-analysis (not silently presented as equally authoritative).
13. Old stored artifact (no canonical verdict key) still renders via existing fields, byte-identical
    to pre-branch behavior when read through the unchanged paths.
14. Formality status: a formality reconciled at the lot level as `cancelled_by_procedure=true`
    renders consistently in the lot report, decision model, and (if referenced) any case-level
    formalities rollup.
15. Value/money: `workspace._final_value_display`'s figure and the canonical verdict's
    `field_verdicts.money.final_value` agree for the same lot (regression for the fourth
    value-extraction path found in §2.5).
16. Multi-Bene lot: per-Bene typology stays distinct within one lot's verdict, never blended into a
    single lot-wide typology (generalization of Branch 1's Bene-specific applicability).
17. Case-level contamination check: two lots' verdicts both citing the exact same specific,
    narrowly-worded defect text is flagged as a conflict/possible-leak for manual review, not
    silently accepted as "two lots coincidentally have the identical problem."
18. Schema-version guard: a canonical verdict with an unrecognized `schema_version` is rejected
    fail-closed (never rendered as if valid) by every consumer.
19. No PII/no hardcoded historical-case values anywhere in the new verdict-building code or its
    tests (string-scan assertion).
20. Offline replay (extending Branch 1's harness): the historical beta case, replayed, shows zero
    remaining severity/typology/occupancy divergence between its case overview and its per-lot
    reports, and the previously-observed selector-vs-lot-report gap (§1.2) no longer exists —
    verified against the sanitized fixture, not the raw forensic bundle, exactly as Branch 1's
    harness already does.

### 6.2 Eight-case regression expansion
For each of the seven original golden cases (Torino, Pistoia, 1859886_C, Orecchiazzi, Cairate,
Codogno, Mantova) plus Branch 1's beta-multi-lot fixture, add one assertion per case: **the
canonical verdict's severity/occupancy/typology for that case's report agrees with the existing
`decision`/`esito` output already asserted by the seven-case regression test** — i.e. this branch
must prove it does not change any of the eight cases' existing, already-correct output, only
guarantees consistency for cases where the current code *could* diverge (none of the eight currently
exercise a divergence, by construction of those fixtures — this is a **no-regression** assertion set,
not a new-behavior one, for the existing eight).

### 6.3 Offline historical-case acceptance criteria
Reusing Branch 1's `offline_historical_replay.py` harness/pattern (no network, no paid calls, no
production writes, `--bundle` required with no default): the replayed beta multi-lot case must show,
for the lot whose selector-vs-report gap was found in §1.2: (a) the selector/case-overview's
occupancy/typology for that lot matches its own reconciled report's occupancy/typology; (b) the
case-level severity ceiling equals the max of the four lots' severities (not less, not silently
higher without evidence); (c) zero raw-enum leaks in any rendered summary text; (d) zero
keyword-triggered severity escalations without a matching structured-severity fact.

---

## 7. Files expected to change

### New (backend)
- `backend/correctness_v2/verdict_model.py` — `CanonicalVerdict` builder (`build_lot_verdict`,
  `build_case_verdict`), `SEVERITY_LABELS_IT`, the ceiling/floor aggregation (§3.E/F), reusing
  Branch 1's `fact_lineage`/`lot_fact_projection` fact-id space for provenance.
- `backend/correctness_v2/tests/test_verdict_model.py`, `test_verdict_consistency.py` — the 20-item
  matrix (§6.1).

### Modified (backend, additive wherever possible)
- `backend/correctness_v2/customer_view.py` — `derive_decision` becomes a thin projection of
  `verdict_model.build_lot_verdict(...)`'s `severity`/`headline`/`drivers`; `_decision_level`,
  `_occupancy_is_occupied`, `_decision_drivers`'s independent classification re-reads are replaced by
  reads of the canonical verdict's `field_verdicts` (closes §1.1, §1.4, and the §2.2/§2.4 independent
  re-derivations).
- `backend/correctness_v2/decision_model.py` — `_build_esito` becomes a thin projection of the same
  canonical verdict (same function, same call sites, now backed by one shared computation instead of
  its own `readiness`-only logic); `_SEV_*` numeric constants become derived from canonical severity
  rather than assigned ad hoc per finding-builder call site.
- `backend/correctness_v2/contract.py` — `_risk_cards`'s summary fallback (`contract.py:163`) routed
  through `_classification_it`/the canonical label table instead of an f-string of the raw enum
  (closes §1.3).
- `backend/correctness_v2/lot_packets.py` — `build_lot_index` gains an optional parameter to accept
  an already-reconciled lot verdict (when available) and prefer it over the raw
  `ws_lot.get("occupancy_status")`/`property_type` snapshot (closes §1.2 at the source).
- `backend/correctness_v2/customer_report.py` — `render_lot_selection_report` prefers the canonical
  verdict's `field_verdicts` for any lot with a completed reconciled report; unresolved lots keep
  today's raw-snapshot rendering, explicitly marked unconfirmed.
- `backend/correctness_v2/workspace.py` — `build_workspace`'s `meta_by_lot` construction
  (`workspace.py:320-330`) and `_final_value_display` (`workspace.py:409-421`) both read the
  canonical verdict when a reconciled report exists (closes §1.2/§2.5's mixed-provenance card).
- `backend/correctness_v2/orchestrator.py` — after a lot's reconciliation completes, persist/attach
  the canonical verdict artifact (new save function in `artifacts.py`) so downstream reads (selector
  for OTHER still-pending lots, Storico) can find it without recomputing from scratch each time.
- `backend/correctness_v2/artifacts.py` — new `CASE_VERDICT_FILE`/`LOT_VERDICT_FILE` constants and
  `save_lot_verdict`/`save_case_verdict` wrappers, following the existing per-artifact-type
  convention.

### Modified (frontend, narrowly — see §8 for the explicit scope statement)
- `frontend/src/components/correctness-v2/CustomerReportView.js` — no visual change; the `? :`
  branch at `CustomerReportView.js:1267` may be simplified once `decision` and `decision_model.esito`
  are guaranteed consistent (optional, low-risk — the existing branch continues to work unchanged
  either way since both fields still exist and now agree).
- `frontend/src/components/correctness-v2/LotWorkspace.js` — no code change expected; it already
  renders whatever `workspace.build_workspace()` returns verbatim, so fixing the backend fixes what
  this component shows with zero frontend edits.

### Explicitly not touched
`analyst.py` (prompt/schema — no prompt change proven necessary, see §8's verdict below),
`fact_lineage.py`/`lot_fact_projection.py` core reconciliation logic (reused, not modified),
`validator.py`, `quality_gate.py`, `coverage_audit.py`, `money_confirmation.py`, `lot_runner.py`,
auth/beta/billing/Stripe/concurrency files, `perizia_authority_lot_projection.py`,
`perizia_canonical_pipeline/*`, `lib/periziaPrintModel.js` (legacy, unrelated — §2.10), any visual
styling/layout files (Branch 5's scope).

---

## 8. Prompt-change verdict

**No prompt change is required or proven necessary.** Every contradiction mechanism traced in this
document is a **downstream aggregation/re-derivation problem** — the underlying facts (occupancy,
compliance classification, severity, typology) are already correctly extracted and reconciled by
Branch 1's pipeline; the bug is that four-plus independent modules each re-derive a "verdict" from
those facts with their own, undocumented rules, and nothing enforces they agree. This is fixable
entirely with deterministic Python-side unification (one canonical verdict function, everything else
becomes a projection of it) and requires no change to what the model is asked to produce.

## 9. Frontend scope statement (explicit, as requested)

Frontend changes in this branch are **narrow and optional**, not required for the backend fix to be
correct: every frontend component identified in §2.10 either (a) purely displays server-computed
tone/label strings (`shared.js` — no change needed), or (b) picks between two already-existing,
now-consistent server fields (`CustomerReportView.js` — no change strictly required, an optional
simplification once both fields agree), or (c) renders whatever the backend workspace payload
contains verbatim (`LotWorkspace.js` — automatically fixed once `workspace.build_workspace` reads
the canonical verdict, zero frontend edit needed). **No visual redesign, no new component, no
layout change is in scope for this branch** — that is Branch 5's mandate. The only frontend
"change" this branch might make is deleting now-redundant fallback branching logic once the two
server fields are guaranteed to agree, which is cosmetic-neutral (no visual difference) and can be
deferred entirely to a later branch without blocking this one's correctness goal.

---

## 10. Rollback plan

Every change is additive (new module, new optional parameters defaulting to today's behavior, new
artifact files) except the four "independent re-derivation" call sites explicitly rewritten to read
the canonical verdict (§7's `customer_view.py`/`contract.py` changes) — those are the only places
with behavioral risk, and each is a small, isolated function body swap with a 1:1 existing test
(`test_compliance_gate.py`-style unit coverage) to catch any regression immediately. Rollback is a
straightforward revert of the branch commit(s): no stored artifact needs migration in either
direction, since the canonical verdict is optional/additive and every existing key/shape is
preserved. A feature flag (e.g. `CORRECTNESS_V2_CANONICAL_VERDICT_ENABLED`, mirroring the existing
`feature_flags.py` pattern) around the four rewritten call sites allows instant disablement without
a code revert if needed.

---

## 11. Red-team section

1. **New contamination risk**: the case-level "two lots citing the identical specific defect
   wording" contamination check (§3.E) could itself misfire on a genuinely `ALL_LOTS`/`CASE_GLOBAL`
   fact correctly projected into multiple lots (Branch 1's intended, correct behavior) — the check
   must exclude facts sharing the same `source_fact_id`/`provenance.case_fact_ids` entry (a legitimate
   shared projection) from being flagged as a "leak," flagging only textual duplication with
   *different* fact ids or no shared provenance at all.
2. **New false-certainty risk**: the ceiling/floor severity aggregation (§3.F) must not be
   implemented as a simple "look at the lot's OWN esito string" comparison, because that string is
   itself currently inconsistent (§1.1) — the aggregation must read the canonical per-lot severity
   directly, not either of the two legacy verdict fields, or it inherits their disagreement one level
   up.
3. **Schema-version risk**: a canonical verdict computed by an OLDER version of `verdict_model.py`
   (e.g. from a cached/queued job that started before a mid-flight deploy) must never be blended with
   case-level aggregation logic from a NEWER version without a schema-version check; invariant 18
   (§4) requires fail-closed rejection of an unrecognized version, not a best-effort merge.
4. **Frontend cache risk**: `CustomerReportView.js`'s `useCorrectnessV2CustomerView` hook (or
   equivalent client cache) could hold a STALE `decision`/`decision_model` pair fetched before this
   branch's backend fix ships, while a freshly-fetched OTHER lot in the same session reflects the
   fixed, consistent pair — this is a plain cache-invalidation concern (bump the sanitized payload's
   `schema_version` so any client-side cache keyed on it naturally misses and refetches), not a new
   architectural risk, but must be checked before rollout.
5. **Evidence/applicability risk**: the case-level `CASE_GLOBAL` exception to the ceiling rule
   (§3.F) is the one place this branch allows a case verdict to exceed its lots' severities — this is
   exactly the kind of rule that, if implemented loosely, reopens contradiction class 1 ("case
   overview says severe while lot says minor") in the OTHER direction (case now wrongly overrides a
   lot's correct, less-severe finding). It must require the case-level fact to be independently
   verified as NOT already reflected (even in weaker form) in the lot's own verdict, or the lot's
   verdict wins per precedence rule §3.D.1.
6. **Backward-compat risk**: an old stored `decision_model.json`/`customer_report.json` with no
   canonical verdict computed **and** a report_status that is no longer in the customer-safe set by
   the time this branch ships (an edge case if the safe-status set itself ever changes) must still
   fail closed exactly as it does today — the fallback-to-recompute path (§5) must preserve today's
   `is_customer_safe`/`report_status` gating untouched, never inferring safety from the presence of a
   verdict object alone.

---

## 12. Amendments (2026-08-13) — resolutions to two verified plan/contract contradictions

Sol, during plan validation, found two places where this plan's literal wording is unsatisfiable
against Branch 1's real persisted contract. Both were re-verified against code by the orchestrator
(cited below). Each is resolved **only** in the direction already mandated by the plan's own design
intent and the owner's non-negotiable invariants + Gate 7. **This section is authoritative and
supersedes the conflicting sentences it names.**

### A1 — Cross-view verdict consistency is *semantic projection*, not literal string equality

**Contradiction.** §6.1 item 1 (and a literal reading of invariant §4.1) requires
`decision.level == decision_model.esito.level`. But the two fields use **disjoint, test-enforced
vocabularies**:
- `decision.level` ∈ {`attenzione`, `da_verificare`, `pronto_con_avvertenze`, `non_leggibile`}
  (`customer_view.py:72-75`; asserted `test_customer_view.py:304-342`).
- `esito.level` ∈ {`rosso`, `ambra`, `verde`} (`decision_model.py:2022-2029`; asserted
  `test_decision_model.py:97-122`).
Making them literally equal would break owner **Gate 9** (old cached reports readable), invariant
§4.13, and existing tests. No value satisfies literal equality while keeping both legacy enums.

**Resolution (matches the plan's own §3.A/§3.B intent).** Both `decision.level` and `esito.level`
become **deterministic display projections of the ONE canonical `severity`** (§3.B), via two frozen
mapping tables in `verdict_model.py`. Neither field is ever computed by its own independent rules
again; each is `project_to_decision_level(canonical.severity)` /
`project_to_esito_level(canonical.severity)`. The tables MUST be chosen so every existing golden and
unit case's current output is **unchanged** (invariant §4.14 no-regression); Sol derives the exact
per-threshold mapping from current behavior, then locks it.

Consistency is redefined as **semantic non-contradiction**. The projection tables MUST be
reverse-engineered from **current behavior** (not hand-authored) so every existing output is
unchanged; the table below is the **observed** current mapping, recorded here only to define the
compatibility relation — it is **not** a license to change any output.

| canonical `severity` / status                                   | `esito.level`         | `decision.level`        |
|-----------------------------------------------------------------|-----------------------|-------------------------|
| `verde` (nothing actionable, REPORT_READY)                      | `verde`               | `pronto_con_avvertenze` |
| `info` / `minore` (non-blocking notes)                          | `verde` or `ambra` (per current thresholds) | `pronto_con_avvertenze` / `da_verificare` |
| `media` (needs verification, REPORT_READY with open actions)    | `ambra`               | `da_verificare`         |
| interactive safe non-ready (`LOT_SELECTION_REQUIRED`, `MONEY_CONFIRMATION_REQUIRED`) | `ambra` | `da_verificare`         |
| **unreadable (`DOCUMENT_NOT_READABLE`)**                        | **`ambra`**           | **`non_leggibile`**     |
| `grave` blocking / fail-closed technical review (`TECHNICAL_REVIEW_REQUIRED`) | `rosso` | `attenzione`            |

> **A1 correction (2026-08-13):** an earlier draft of this table wrongly grouped
> `DOCUMENT_NOT_READABLE` with fail-closed as `rosso`. Verified against code, unreadable is an
> **interactive safe** status → `esito=ambra`, `decision=non_leggibile` (`decision_model.py:2019-2029`
> maps every non-`REPORT_READY` non-technical state to `ambra`; `customer_view.py:285-298` sets
> `non_leggibile`; enforced by `test_decision_model.py:479-485`
> `test_interactive_statuses_are_amber_not_red`). **`rosso` is reserved for `TECHNICAL_REVIEW_REQUIRED`
> / genuinely fail-closed reports only** — never for unreadable or other interactive-safe statuses.

- §6.1 item 1's assertion changes from `decision.level == esito.level` to:
  **`assert same_canonical_source(decision, esito) and not cross_band_contradiction(decision.level,
  esito.level)`**. A pair is a **forbidden cross-band contradiction** *only* when one side asserts
  "clean / go" (`esito == verde` **or** `decision == pronto_con_avvertenze`) while the other asserts
  "blocking / stop" (`esito == rosso` **or** `decision == attenzione`). Middle-band pairings
  (`ambra` ↔ {`da_verificare`, `non_leggibile`}) are **compatible**, not contradictions — mere label
  difference across the two legacy vocabularies is never a violation.
- Invariant §4.1 stands as written in meaning ("never independently computed; both projections of one
  canonical verdict"); only §6.1.1's literal `==` is amended.

### A2 — Field provenance: reuse Branch 1's fact_lineage generator; Gate-7 legacy exception

**Contradiction.** §3.C and invariant §4.11 require every `field_verdicts` leaf to carry a non-empty
`source_fact_id` *resolvable in the Branch 1 fact ledger*. But per code:
- `fact_id` (format `f"{category}:{token}:{ordinal}"`, `fact_lineage.py:182`) is minted **only over the
  case-level worksheet** by `build_case_fact_ledger`.
- Only **projected** facts get that id **stamped** onto the reconciled lot worksheet
  (`lot_fact_projection.py:150,212,230,248,304`). **Lot-native reconciled facts** (kept by
  reconciliation from the lot's own re-analysis) receive **no** `fact_id`.
- The **ledger is never persisted** — only the projection *report* (`lot_fact_projection.json`,
  which stores fact-id *references*, not full facts). `artifacts.py` has no ledger constant/saver.
- Old cached artifacts predate fact_lineage entirely (no ids, no ledger).
So §4.11 as written is unsatisfiable for lot-native facts and legacy artifacts — **and it is stricter
than the owner's own Gate 7**, which already permits "a Branch 1 fact ID **or an explicit
deterministic aggregation reason**."

**Resolution (consume Branch 1; no parallel model; no core-reconciliation change).**
`build_case_fact_ledger(worksheet, segmentation, lot_report)` is a **pure, deterministic** function
(Branch-1 plan §5, returns `{"schema_version":"cv2.fact_ledger.v1","facts":[...]}`). Provenance is
resolved as follows, all reusing that one generator and id format:

1. **Projected facts:** use the `fact_id` already stamped on the reconciled worksheet item (points into
   the case ledger). Unchanged from Branch 1.
2. **Lot-native reconciled facts (no stamped id):** mint a deterministic Branch-1-format id by
   invoking the **existing** `fact_lineage.build_case_fact_ledger` over the **reconciled lot
   worksheet** (same pure function, same `category:token:ordinal` format). This is *reuse of Branch 1's
   generator*, **not** a new id namespace and **not** a change to `fact_lineage.py`/
   `lot_fact_projection.py` logic — `verdict_model.py` merely calls the function on the reconciled
   worksheet. IDs are stable across reruns of the same worksheet.
3. **Resolution strategy = recompute-on-read (preferred), persist optional.** Because the ledger is a
   pure function of stored data (`analyst_worksheet` + `segmentation` + `lot_report`, all already
   persisted for live reports), a `source_fact_id` is resolved by recomputing the ledger and looking it
   up — matching §5's "compute on read from already-stored data." Persisting is an **optional additive
   optimization**: if added, use a new `FACT_LEDGER_FILE` constant + `save_*_fact_ledger` wrapper in
   `artifacts.py` following the existing convention (no schema of any existing artifact changes).
4. **Legacy artifacts with no recoverable worksheet/segmentation:** the leaf carries an explicit
   deterministic `provenance_reason` from a **closed set** instead of a `source_fact_id`. This aligns
   §4.11 to Gate 7. Consumers treat a reason-only leaf **fail-safe**: never upgrade
   severity/occupancy/certainty from it (occupancy stays `UNKNOWN`/"da verificare", never LIBERO;
   severity is not escalated).

**Invariant §4.11 is amended to:** *"Every `field_verdicts` leaf carries EITHER a non-empty
`source_fact_id` resolvable via `fact_lineage.build_case_fact_ledger` over the (stored or recomputed)
reconciled worksheet, OR an explicit deterministic `provenance_reason` drawn from the closed set
{`AGGREGATION_CEILING`, `AGGREGATION_FLOOR`, `UNRESOLVED_LOT`, `LEGACY_ARTIFACT_NO_LEDGER`}. No leaf
is ever unattributed; a reason-only leaf is always rendered fail-safe."* This satisfies owner Gate 7
verbatim and preserves fail-closed behavior (Gate 10).

**Scope confirmation.** A2 adds **no** modification to `fact_lineage.py` or `lot_fact_projection.py`
reconciliation logic (they are only *called*), introduces **no** second provenance format (the id
format and the generator are Branch 1's), and suppresses **no** fact — an unattributable leaf is
surfaced with a reason, never dropped. Consistent with the owner's non-negotiable invariant.
