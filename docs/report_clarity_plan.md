# Report Clarity — Implementation Plan

Branch: `feature-correctness-v2-report-clarity` · Base: `main` @ `e46b865`
(Branches 1-3 + R3-02 LIVE; PDF retention deployed dormant)
Status: PLANNING ONLY -- no product code changed. This document is the only
file written on this branch. Sol/Fable implement after owner review.

Addresses the second direct external-beta complaint: the report was hard
to understand. Branch 1 (fact lineage), Branch 2 (CanonicalVerdict) and
Branch 3 (partial disclosure) already made the underlying facts and verdict
correct. This branch is presentation/IA only.

---

## 1. Problem statement

The codebase already contains a well-designed, decision-oriented customer
report: `decision_model.build_decision_model()` (backend) plus
`CustomerDecisionReport` (frontend, `frontend/src/components/correctness-v2/CustomerDecisionReport.js`)
implement almost exactly the 11-section, priority-ranked, deduplicated,
evidence-anchored flow the mission asks for: grouped conformity findings,
a single capped "Cosa verificare prima di procedere" list sorted by severity,
one financial value chain with reconciliation, a dedicated confirmations
section, and a readiness/esito summary. This is not a green-field design
problem: it is a routing and coverage problem. The good renderer is
reachable only in one narrow condition, and everywhere else the customer
still sees an older, flatter surface that produces the "wall of Da
verificare" complaint.

Concrete, code-grounded clarity problems, worst first.

### P1 -- The good IA never activates for a partial report; the flat legacy renderer takes over

`decision_model.build_decision_model()` only builds `sections.acquisto`,
`sections.numeri`, `sections.occupazione`, `sections.conformita`,
`sections.formalita`, `sections.verifiche`, `sections.altri`,
`sections.fonti` inside one gate:

```
backend/correctness_v2/decision_model.py:2128
    if report_status == "REPORT_READY":
```

For `PARTIAL_REPORT_AVAILABLE` (Branch 3's own status -- exactly the state
the beta tester's blocked lot now produces) this block is skipped, so
`decision_model.sections` comes back essentially empty (only
`stato_verifiche`, always attached outside the gate). The frontend correctly
detects this and falls back:

```
frontend/src/components/correctness-v2/CustomerReportView.js:1297
    {report?.decision_model && report?.report_status === 'REPORT_READY' ? (
      <CustomerDecisionReport ... />
    ) : (
      <V2CustomerReportFallback report={report} />
    )}
```

`V2CustomerReportFallback` (`CustomerReportView.js:1269-1282`) is the
pre-decision-model renderer: it prints `compliance_section`,
`risk_sections` and `buyer_checklist` as independent, undeduplicated,
unprioritized lists (`CustomerComplianceSection` at `CustomerReportView.js:675-718`,
`CustomerOtherFindings` at `:722-764`, `CustomerChecklistSection` at
`:777-830`). A partial report -- the case that most needs a calm, prioritized
explanation of what's missing -- is exactly the case that gets the flattest,
most repetitive presentation. This is proven by the sanitized beta fixture:
`backend/correctness_v2/tests/test_beta_multilot_partial_report.py` builds a
`PARTIAL_REPORT_AVAILABLE` customer payload and asserts `compliance_section`,
`occupancy_section`, `money_sections.valuation_chain` are all preserved
verbatim -- i.e. all the raw material the good renderer needs is present in
the payload -- but nothing routes it there.

### P2 -- The same finding is shown up to three times in the fallback path, with no severity grouping

In `V2CustomerReportFallback`, a single compliance issue can appear as: (a) a
card in `CustomerComplianceSection` (`CustomerReportView.js:675-718`,
un-capped, one card per `compliance_section` row, no critical/secondary
split), (b) a line in `CustomerOtherFindings` if its risk-section counterpart
wasn't matched by area (`:722-764`), and (c) a numbered step in
`CustomerChecklistSection` (`:777-830`, `buyer_checklist`, severity-blind
`CHECKLIST_PREVIEW_LIMIT = 8` truncation with no critical/secondary
distinction). By contrast `decision_model._build_verifiche()`
(`decision_model.py:1406-1545`) already does the right thing once reachable:
it builds ONE checklist, links each item back to its finding by
`finding_id` so a confirmation can never leave a stale duplicate, sorts by a
severity-derived `priority_from_severity` integer, and caps at 8
(`decision_model.py:1544`, `displayed_items = items[:8]`) -- but there is
still no CRITICAL vs secondary visual split even in the good path; it's one
flat list of at most 8 items.

### P3 -- The "IN CONFLITTO" state exists on the server and is never shown

`verdict_model.build_lot_verdict()` computes `conflicts` -- an array of
`{path, case_value, lot_value, reason_code}` rows describing case-vs-lot
contradictory facts (`verdict_model.py:586-590`), and
`verdict_model.build_case_verdict()` computes cross-lot conflicts the same
way (`verdict_model.py:744-758`, reason code `POSSIBLE_CROSS_LOT_LEAKAGE`).
This field is attached to the customer payload unconditionally whenever a
canonical verdict exists:

```
backend/correctness_v2/customer_view.py:652
    if canonical is not None:
        out["canonical_verdict"] = canonical
```

No frontend component reads `canonical_verdict.conflicts` (confirmed by
grepping `conflicts|canonical_verdict` across
`frontend/src/components/correctness-v2/*.js`: zero non-test matches). A
buyer today has no way to see that two sources in the same appraisal
disagree, even though the fact and its two disagreeing values are already
computed, validated, and shipped.

### P4 -- Two independent, unequal "financial summary" implementations exist

`V2CustomerReportFallback` renders money via `CustomerMoneySection`
(`CustomerReportView.js:490-524`) and `CustomerCostsSection`
(`:529-586`), reading `money_sections.valuation_chain` /
`buyer_side_costs` / `uncertain_money` directly with no reordering, no
component-value reconciliation, and no "Importi da chiarire" separation.
`CustomerDecisionReport`'s `NumeriPrincipali` (`CustomerDecisionReport.js:130-243`)
reads the richer, already-computed `decision_model.sections.numeri` (built
by `decision_model._build_numeri`, `decision_model.py:489-586`), which
canonically reorders the value chain, reconciles component sums against
percentage deductions (`_valuation_reconciliation`, `:804-862`), and
separates "Importi da chiarire" from genuine buyer costs. Whichever path a
customer lands on, they see a materially different financial summary for
the same underlying report -- this is the direct cause of "clearer financial
summary" being an explicit mission target.

### P5 -- The 5-state vocabulary exists but is visually collapsed to three tones

`decision_model._build_conformity_findings()`
(`decision_model.py:966-1264`) already computes a fine-grained status token
per compliance finding -- `dichiarato_perizia`, `non_dichiarato`,
`da_chiarire`, `non_determinabile`, `da_verificare`, `conforme` -- derived
from `_CLASSIFICATION_STATUS` (`:88-95`) plus per-topic regex heuristics
inside the `uncertain` branch (`:990-1137`). The frontend then squashes four
of those distinct states into a single visual tone:

```
frontend/src/components/correctness-v2/CustomerDecisionReport.js:319-333
    const CONFORMITY_TONE = {
      ...
      da_chiarire: 'slate',
      non_dichiarato: 'slate',
      non_determinabile: 'slate',
      non_verificato: 'slate',
    };
```

"Not declared at all," "genuinely undeterminable from this appraisal," and
"declared but ambiguous wording" render as the identical grey chip. A buyer
cannot tell these apart even though the backend already can.

### P6 -- A defensive label fallback can silently mislabel a conforming item as "Da verificare"

In the raw (non-decision-model) `compliance_section` builder:

```
backend/correctness_v2/customer_report.py:897-898
    "status_label": item.get("classification_label")
        or _CLASSIFICATION_LABELS.get(item.get("classification"), "Da verificare"),
```

`_CLASSIFICATION_LABELS` (`customer_report.py:134-139`) has no `"conforming"`
key. `contract.py:687` normally sets `classification_label` explicitly via
`_classification_it()` (which does map `"conforming"` correctly, `contract.py:595-601`),
so in the common case this fallback is never hit -- but any legacy/edge-case
report missing `classification_label` upstream will show a fully-conforming
item as "Da verificare" in the raw section that `V2CustomerReportFallback`
reads. This directly manufactures instances of the "wall of Da verificare."

None of P1-P6 requires re-deriving any fact, verdict, severity, or
disclosure state. Every fix is either (a) reaching an authoritative field
that already exists but is unrouted/unrendered, or (b) grouping/labeling
an existing field more precisely than today's collapsed tone map.

---

## 2. Current data contract -- full trace

### 2.1 What the customer payload authoritatively contains

`customer_view.sanitize_customer_report()` (`customer_view.py:562-688`) is
the single choke point. Every customer-safe field originates here:

- `report_status` / `report_status_label` -- `_STATUS_LABELS` (`:66-72`), closed enum, Italian labels only.
- `decision` -- `derive_decision()` (`:304-384`); legacy executive box; still the hero for interactive/non-ready statuses.
- `case_identity`, `lot_structure`, `beni_sections` -- raw report, `_customer_content`-scrubbed identity facts.
- `money_sections` -- `_customer_money_sections()` (`:400-418`): `valuation_chain` / `auction_terms` / `buyer_side_costs` / `procedure_cancelled_formalities` / `uncertain_money` only.
- `risk_sections`, `compliance_section`, `formalities_section`, `buyer_checklist` -- raw report, scrubbed; the "flat" surface `V2CustomerReportFallback` reads.
- `disclosure_state` -- `partial_report.PARTIAL_REPORT_AVAILABLE` / `FULL_REPORT_AVAILABLE`, only when `feature_flags.partial_lot_reports_enabled()` (`:646-651`).
- `canonical_verdict` -- `verdict_model.refine_for_runtime()` output; includes `conflicts[]`, unused by any frontend component (P3).
- `decision_model` -- `decision_model.build_decision_model()`; the good IA; section-populated only when `report_status == REPORT_READY` (P1).
- `lot_selection` -- only when status is `LOT_SELECTION_REQUIRED`.
- `money_confirmation` -- only when status is `MONEY_CONFIRMATION_REQUIRED`.
- `partial_status` -- only when status is `PARTIAL_REPORT_AVAILABLE`; `unresolved_fields` via `partial_report.customer_unresolved_fields()`. Branch 3, fail-closed, `full_readiness: False`, `professional_verification_required: True` always.

`is_customer_safe()` (`:528-559`) is the fail-closed gate: for
`PARTIAL_REPORT_AVAILABLE` it additionally requires
`partial.unresolved_fields` non-empty, `full_readiness is False`,
`professional_verification_required is True`, and a valid canonical verdict
-- any violation makes the whole report unsafe (`available: False`), never a
degraded partial view. This gate is upstream of everything in this plan and
is not touched.

### 2.2 `decision_model` shape (the authoritative view-model already built)

`decision_model.build_decision_model()` (`decision_model.py:2092-2271`)
returns a dict with: `schema_version`, `analysis_id`, `job_id`, `lot_id`,
`report_status`, `readiness` (`state`, `label`, `confirmations_total`,
`confirmations_done`, `professional_checks_open`, `confirmations_open`,
`information_declared`, `information_confirmed`, `information_resolved`),
`esito` (`level`, `headline`, `sentence`, `drivers[]`,
`canonical_verdict_ref`, `canonical_severity`), `sections` (`acquisto`,
`numeri`, `occupazione`, `verifiche`, `conformita`, `formalita`, `altri`,
`fonti`, `conferme`, `stato_verifiche`), `findings[]` (each with
`finding_id`, `section`, `topic`, `title`, `status`, `status_label`, `tone`,
`severity`, `customer_summary`, `buyer_impact`, `recommended_action`,
`amount`, `amount_display`, `pages`, `page`, `evidence`, `blocking`,
`confirm_class`, optional `confirmation`), and `confirmations[]`.

Every `finding` carries `severity` (an int from
`verdict_model.priority_from_severity`; bands: grave to 0-2, media to 3-4,
minore to 5, info to 6-7, verde to 7) and `blocking` (bool). These two
fields are already authoritative and sufficient to build a
CRITICAL/secondary split without inventing anything (see section 3.3).

### 2.3 `canonical_verdict` shape (CanonicalVerdict, Branch 2)

`verdict_model.build_lot_verdict()` (`verdict_model.py:487-649`) returns
`severity` (verde/info/minore/media/grave), `readiness`, `field_verdicts`
(`typology`, `occupancy`, `compliance[]`, `formalities[]`,
`money.final_value`, each a "leaf" with `value`, `confidence` and either
`source_fact_id` or `provenance_reason`), `conflicts[]`, `drivers[]`, and
`display_projections` (`decision_level`, `esito_level`,
`decision_driver_codes`). This is the only authority for severity and
verdict; nothing in this plan re-derives it.

### 2.4 `partial_status` shape (Branch 3)

`partial_report.customer_unresolved_fields()` (`partial_report.py:300-329`)
maps each unresolved field to `field_label`, `reason_label`,
`severity_label` (always "Critico -- verifica necessaria" -- Branch 3 only
discloses `critical` omissions, `partial_report.py:234-235`),
`source_pages`, `evidence_available`, and optionally `monetary_role_label`.
This is a closed, already-Italian, already-page-anchored contract.

### 2.5 Current frontend rendering

Two parallel renderers exist under `CustomerReportBody`
(`CustomerReportView.js:1284-1310`):

- `CustomerDecisionReport` (`CustomerDecisionReport.js`, entirely
  data-driven off `report.decision_model`, 11 sections, order matches the
  `Part 3` doc comment at `:17-19`) -- used only when
  `report.report_status === 'REPORT_READY'` and `decision_model` present.
- `V2CustomerReportFallback` (`CustomerReportView.js:1269-1282`) -- used
  for every other customer-safe status (`PARTIAL_REPORT_AVAILABLE`,
  `MONEY_CONFIRMATION_REQUIRED`, `LOT_SELECTION_REQUIRED`, and any legacy
  cached report without a `decision_model`), reading the raw
  `compliance_section` / `risk_sections` / `buyer_checklist` /
  `money_sections` fields directly.

`CustomerReportView` top-level routing (`:1443-1489`) additionally special-
cases `DOCUMENT_NOT_READABLE` (own screen, `CustomerDocumentNotReadable`,
`:1200-1250`), `LOT_SELECTION_REQUIRED` (`CustomerLotSelector`), and prepends
`CustomerPartialReportBanner` (`:93-120`) above the body for
`PARTIAL_REPORT_AVAILABLE`.

`LotWorkspace.js` (multi-lot list/status board, entered before any single
report) is already reasonably coherent: one status badge per lot
(`LOT_STATE_META`, `:42-78`), one dominant action per state, an aggregate
summary line (`buildLotSummaryLine`, `:82-...`). This plan does not redesign
lot selection; it focuses on the report itself.

---

## 3. Design

### 3.1 Architectural decision: extend the existing backend view-model, do not build a new one

`decision_model` is already the correct place for the buyer-facing
view-model -- it is pure, deterministic, takes no network/OpenAI, and is
explicitly documented as a presentation projection (`decision_model.py:1-19`).
This plan extends it in two additive, narrowly-scoped ways, both flag-gated
by a new `CORRECTNESS_V2_REPORT_CLARITY_ENABLED` flag (mirroring
`feature_flags.py:20-29`'s `FLAG_CANONICAL_VERDICT` /
`FLAG_PARTIAL_LOT_REPORTS` pattern, default `False`).

**(a) Broaden the section-building gate to cover `PARTIAL_REPORT_AVAILABLE`.**

Change the condition at `decision_model.py:2128` from
`if report_status == "REPORT_READY":` to include
`PARTIAL_REPORT_AVAILABLE` when `feature_flags.report_clarity_enabled()` is
true, with the existing body of that block left otherwise unchanged.

This is safe because of what Branch 3 already guarantees:
`render_partial_report` (invoked from `partial_report.build_partial_projection`,
`partial_report.py:384-390`) is an overlay -- it does not touch any
already-resolved section of the report; it only adds `partial_status`. This
is proven by the existing golden test
(`test_beta_multilot_partial_report.py`, assertions 6/7/9): after the
partial overlay, `occupancy_section`, `compliance_section`, and
`money_sections.valuation_chain` are byte-identical to the pre-partial
customer projection. Running the same, unmodified `_build_acquisto` /
`_build_numeri` / `_build_occupancy` / `_build_conformity_findings` /
`_build_formalita` / `_build_verifiche` / `_build_altri` / `_build_sources`
functions against a `PARTIAL_REPORT_AVAILABLE` report therefore produces
sections built from exactly the same already-certified data they would
produce for `REPORT_READY` -- it computes nothing new.

`_build_readiness` and `_build_esito` are not touched: their own local
`_CUSTOMER_SAFE_STATUSES` set (`decision_model.py:137`) does not include
`PARTIAL_REPORT_AVAILABLE`, so `readiness.state` correctly stays
`TECHNICAL_REVIEW_REQUIRED` and `esito.level` stays governed by
`verdict_model.refine_for_runtime()` (which independently forces
`severity = "grave"` whenever `report_status not in _SAFE_STATUSES`,
`verdict_model.py:663-664`, and `_SAFE_STATUSES` also excludes PARTIAL). A
partial report will therefore show full, richly organized content and keep
its correct "verifica tecnica richiesta" red/amber posture -- this is
exactly the target "show partial-report status clearly without
overwhelming."

**(b) Add one new projection function surfacing `canonical_verdict.conflicts`
and low-confidence/`UNKNOWN` field-verdicts as findings, closing P3.**

A new function, e.g. `decision_model._build_conflicts(report,
canonical_verdict, lot_id)`, projects `canonical_verdict["conflicts"]`
(`verdict_model.py:586-590` / `:744-758`) into findings with status
`in_conflitto`, and any `field_verdicts.*` leaf whose `confidence == "low"`
or whose `occupancy.value == "UNKNOWN"` (the exact condition
`verdict_model.py:527-534` already uses to represent "we could not
determine this") into findings with status `non_determinabile` (INCERTO).
It reads only fields already computed by `verdict_model`; it invents
nothing. Findings from `_build_conflicts` are appended to `findings[]` with
the same shape as every other finding (so they participate in
`_build_verifiche`'s existing severity sort and the CRITICAL/secondary
split, section 3.3) and are rendered by the existing generic finding-card
component -- no new card type, just a new status token and its tone.

No other backend function changes. Everything else in this plan is frontend
grouping/labeling/consolidation of fields that already exist.

### 3.2 The 5(6)-state mapping -- FROM existing authoritative fields only

- DICHIARATO DALLA PERIZIA -> backend token `dichiarato_perizia` -> source: `decision_model._build_conformity_findings`, positive-declaration branch, `decision_model.py:1111-1137` (reads `compliance_section[].notes` via the existing fail-closed regex classifier -- not re-derived by this branch, only consumed).
- CONFERMATO -> backend token `confermato_utente` -> source: `decision_model._apply_confirmations()`, `decision_model.py:1953-1957`, joined from persisted Mongo `user_confirmations` at read time.
- DA VERIFICARE -> backend tokens `da_verificare` / `da_chiarire` -> source: `_CLASSIFICATION_STATUS` (`decision_model.py:88-95`) for `regularizable`/`non_conforming`/`uncertain`-with-ambiguous-wording classifications.
- NON DICHIARATO -> backend token `non_dichiarato` -> source: `decision_model.py:1098-1102`, negated-declaration regex branch.
- IN CONFLITTO -> new backend token `in_conflitto` (section 3.1b) -> source: `canonical_verdict.conflicts[]`, `verdict_model.py:586-590` / `:744-758`.
- INCERTO -> backend token `non_determinabile` -> source: `decision_model.py:1105-1108` (ownership) and the new field-verdict-confidence branch (section 3.1b) reading `field_verdicts.*.confidence == "low"` / `occupancy.value == "UNKNOWN"`.

This table is the enforceable contract for invariant #3 (section 4): every
one of these six labels must trace to exactly one of the rows above; the
frontend maps `status` -> label/tone via a static dictionary, never a
heuristic.

Frontend: replace the collapsed `CONFORMITY_TONE` map
(`CustomerDecisionReport.js:319-333`) with a table giving each of the six
tokens its own label plus icon plus tone (verde/ambra/slate/rosso as today,
but no two distinct states share both the same icon and the same label --
`slate` tone may still be shared by INCERTO/NON DICHIARATO since both are
legitimately neutral-toned, but their text label and icon must differ so
they remain visually distinguishable).

### 3.3 Prioritized-checks model: CRITICAL vs secondary

Source fields only -- no new severity computation. CRITICAL is defined as
`finding.blocking is True` OR `finding.severity < 3` (the grave priority
band, `verdict_model.PRIORITY_BASE_BY_SEVERITY["grave"] == 0`, band width 3
per `decision_model.py:51-57`'s existing `_SEV_FINAL_VALUE` (1) /
`_SEV_OCCUPANCY` (2) constants). Secondary is everything else in
`sections.verifiche.items` / the flattened `findings[]` list.

This is a pure client-side (or, equivalently, a `decision_model`
output-time) partition of the existing `sections.verifiche.items` array --
no new field, no new backend computation, same 8-item cap
(`decision_model.py:1544`) but rendered as two headed groups instead of one
flat list: "Verifiche essenziali prima di procedere" (CRITICAL, always
expanded) and "Altre verifiche" (secondary, collapsed by default, the
`<details>` pattern already used elsewhere, e.g. `shared.js:60-72`
`DetailBlock`).

### 3.4 Financial summary consolidation (closes P4)

Retire the independent `CustomerMoneySection` / `CustomerCostsSection`
implementation path once `decision_model.sections.numeri` is reachable for
both `REPORT_READY` and `PARTIAL_REPORT_AVAILABLE` (section 3.1a).
`NumeriPrincipali` (`CustomerDecisionReport.js:130-243`) becomes the single
financial-summary renderer for every status that has a `decision_model`.
`CustomerMoneySection` / `CustomerCostsSection` are kept only as the
last-resort path for the shrinking population of legacy cached reports with
no `decision_model` key at all (pre-dates the decision-model rollout --
`LOT_SELECTION_REQUIRED` / `MONEY_CONFIRMATION_REQUIRED` interactive prompts
also keep their own existing, deliberately minimal, money display since
those statuses show a prompt, not a report).

### 3.5 One "what am I buying / what matters / what's the verdict" summary (Q1-Q3)

Currently a buyer must read `EsitoOperativoCard` then scroll through
`AcquistoSection` then `NumeriPrincipali` to answer "what am I buying, what
are the numbers, what's the verdict." Add one compact hero block above
`EsitoOperativoCard` combining: identity one-liner (address + tipologia,
from `sections.acquisto.identity`, already computed), the terminal value row
(`sections.numeri.catena` row with `terminal: true`, already computed at
`decision_model.py:509-515`), and the esito headline/chip (already
computed). This is a pure recombination of three already-rendered fields
into one scannable block; `EsitoOperativoCard`, `AcquistoSection`,
`NumeriPrincipali` remain below it in full for anyone who wants detail
(nothing is removed, the hero is additive).

### 3.6 Multi-Bene and multi-lot IA

Multi-Bene: keep the existing accordion pattern
(`CustomerDecisionReport.js:98-124`, one `<details>` per Bene). Add, on each
Bene's summary row, a small count badge of open CRITICAL checks that
reference that Bene, sourced from existing per-component evidence linkage
(`decision_model._component_value_view` / `_find_cached_component_excerpt`
already resolve a Bene label per money row, `decision_model.py:625-725`).
Caveat, to be resolved during implementation and not this plan: not every
`finding` in the current schema carries an explicit `bene_id`; Sol must
verify feasibility against the real fixture set before committing to
per-Bene check counts, and if infeasible, ship the Bene accordion unchanged
and defer per-Bene counts to a follow-up.

Multi-lot: unchanged. `LotWorkspace.js` already gives one state badge, one
action, and an aggregate summary line per lot before any report is opened;
this plan only touches the report body reached after a lot is opened.

### 3.7 Partial-status presentation

Keep `CustomerPartialReportBanner` (`CustomerReportView.js:93-120`) as the
single, compact disclosure surface: one paragraph plus one card per
unresolved field (already capped by however many fields Branch 3 discloses
-- typically one or two, never a wall, per the fail-closed allow-list in
`partial_report.classify_partial_eligibility`). Once section 3.1a lands,
this banner sits above a now-fully-populated `CustomerDecisionReport`
instead of above the flat fallback -- the partial notice stays exactly as
informative as today, but the customer additionally gets the full organized
report for everything that is resolved, instead of the flat wall.

### 3.8 Evidence/page-reference preservation

No plan item changes `evidence.excerpt` / `evidence.page` / `pages` /
`Pages` / `PageRefs` plumbing. Every new or regrouped component must
continue threading the exact same `evidence` object already attached to
each finding (`decision_model._find_excerpt` / `_missing_evidence`,
unchanged). The `Pages` component (`CustomerDecisionReport.js:39-43`) and
`PageRefs` (`CustomerReportView.js:87-91`) are reused as-is.

### 3.9 Visual hierarchy / duplication removal

`CustomerOtherFindings` (`CustomerReportView.js:722-764`) becomes dead code
once the decision-model path covers `PARTIAL_REPORT_AVAILABLE` -- its sole
purpose was to catch risk-section items the flat compliance renderer missed;
`decision_model._build_altri` (`decision_model.py:1608-1632`) already does
this deduplication correctly (`used_area_tokens` exclusion) inside the good
path. Remove the redundant component once its last caller (the fallback) is
no longer reached in normal operation; keep it only if a legacy-artifact
code path still needs it (verify during implementation).

### 3.10 Mobile

No new breakpoints. Existing Tailwind responsive classes
(`grid-cols-1 sm:grid-cols-2`, `xl:grid-cols-2`) are reused throughout; the
new CRITICAL/secondary split and hero block use the same single-column-first
patterns already present in every section component in this file.

---

## 4. Testable invariants

1. With `CORRECTNESS_V2_REPORT_CLARITY_ENABLED=false`, `sanitize_customer_report()` output is byte-identical to current `main` for every one of the eight golden regression cases (flag-OFF equals today).
2. The frontend never computes `severity`, `status`, `classification`, `confidence`, or any verdict/disclosure value from raw report text -- every visible tone/label is read from a `status`/`tone`/`severity`/`blocking` field already present on the payload. Grep-enforceable: no new regex/keyword classifier added to any `frontend/src/components/correctness-v2/*.js` file.
3. No raw English/internal enum token (`conforming`, `non_conforming`, `regularizable`, `not_regularizable`, `uncertain`, `UNKNOWN`, any `_status`/`reason_code` machine token) reaches rendered text; every visible string is one of the fixed Italian labels in `decision_model.py` / `partial_report.py` / `customer_view.py` string tables or this plan's new section-3.2 table.
4. Every one of the six states in section 3.2 maps to exactly the source field listed in that table; a unit test asserts the mapping function fails closed on an unknown `status` token rather than guessing a tone.
5. The `sections.verifiche` CRITICAL/secondary partition never drops an item: count(critical) + count(secondary) == count(sections.verifiche.items) for every golden case.
6. `canonical_verdict.conflicts` is non-empty in a payload if and only if at least one `in_conflitto` finding is present in `findings[]` (closes P3 with a regression test, not just a manual check).
7. Every finding's `evidence.page` / `pages` / `evidence.excerpt` present before this branch is present, unchanged, after this branch, for all eight golden cases (an evidence-diff test).
8. No omission is ever presented as reassuring/conforming: any finding whose backing fact is `UNKNOWN`/missing renders as INCERTO or NON DICHIARATO, never as a green/verde tone (unit test over `verdict_model` fixtures with a deliberately missing occupancy fact).
9. `PARTIAL_REPORT_AVAILABLE` reports always render `readiness.state == "TECHNICAL_REVIEW_REQUIRED"` and `esito.level` in `{rosso, ambra}` regardless of how many sections are now populated (readiness/esito computation untouched, section 3.1a).
10. `partial_status.unresolved_fields` count and content are unchanged by this branch (Branch 3 contract frozen; assert byte-identical `partial_status` before/after for the beta fixture).
11. Flag-gated: `decision_model.sections` for `PARTIAL_REPORT_AVAILABLE` is empty (as today) whenever `CORRECTNESS_V2_REPORT_CLARITY_ENABLED` is false, and populated only when true.
12. The financial value-chain terminal amount (`sections.numeri.catena[terminal=true].amount`) is identical whether reached via the hero summary (section 3.5) or the full `NumeriPrincipali` section -- no duplicated computation, one source read twice.

---

## 5. Backward compatibility and flag-based rollback

New flag `CORRECTNESS_V2_REPORT_CLARITY_ENABLED` (default `False`), read via
a new `feature_flags.report_clarity_enabled()` helper mirroring
`canonical_verdict_enabled()` / `partial_lot_reports_enabled()`.

Flag OFF: `decision_model.py:2128`'s gate is unchanged
(`report_status == "REPORT_READY"` only); section 3.1b's new
`_build_conflicts` call is itself gated by the same flag and simply not
invoked; frontend components are additive -- new hero block, new
CRITICAL/secondary grouping, new 5-state tone table -- all read from
`decision_model` output that, when the flag is off, is structurally
identical to today's, so the new components render nothing new and old
components are untouched.

No existing field is renamed, removed, or reshaped. `decision`,
`disclosure_state`, `canonical_verdict`, `partial_status`, `lot_selection`,
`money_confirmation` keep their exact current shape.

Rollback: flip the flag off (env var only, no redeploy of the fix itself
needed if the flag check is already deployed) -- identical to the Branch 3
rollback pattern (`CORRECTNESS_V2_PARTIAL_LOT_REPORTS_ENABLED`).

---

## 6. Test plan

1. Backend projection unit tests (`backend/correctness_v2/tests/test_decision_model.py`, extended):
   - `PARTIAL_REPORT_AVAILABLE` input yields `sections.acquisto` / `sections.numeri` / `sections.occupazione` / `sections.conformita` populated identically to the equivalent `REPORT_READY` input with the same underlying report content, flag ON; empty, flag OFF.
   - `readiness.state` / `esito.level` unaffected by the flag for `PARTIAL_REPORT_AVAILABLE` (invariant 9).
   - `_build_conflicts`: given a `canonical_verdict.conflicts` fixture with 1 and with 0 entries, assert 1 and 0 `in_conflitto` findings respectively; given a `field_verdicts.occupancy.value == "UNKNOWN"` fixture, assert exactly one INCERTO finding, no duplicate.
   - Malformed/unvalidatable `canonical_verdict` makes `_build_conflicts` return `[]` (fail closed, never raises into the caller).
2. Eight-case no-regression: existing runner (`backend/scripts/regression_gate_v2_customer_surface.py` and the eight-case golden set referenced in `docs/perizia_scan_quality_recovery_program.md`) run flag-OFF (must be byte-identical to `main`) and flag-ON (must still pass all existing assertions: DOM never blank, no legacy DOM, customer-safe reason codes preserved, exact-owner diagnostics unaffected, legacy PDF endpoint still unauthorized).
3. Beta-fixture clarity acceptance (new test, extending `test_beta_multilot_partial_report.py`, offline/sanitized only):
   - Build the sanitized `PARTIAL_REPORT_AVAILABLE` beta-multilot customer payload (existing fixture path, `backend/correctness_v2/tests/fixtures/beta_multilot_case_sanitized.json`) with the flag ON.
   - Assert the eight buyer questions are answerable from `decision_model` alone: (1) `sections.acquisto.identity`/`beni` present; (2) `sections.numeri.catena` non-empty with a `terminal` row; (3) `esito.headline`/`level` present; (4) `sections.verifiche` CRITICAL partition non-empty when a blocking finding exists; (5) `sections.conformita`/`sections.formalita` present; (6) `partial_status.unresolved_fields` present and every 5-state finding's `status` is one of the six section-3.2 tokens; (7) `sections.formalita` addresses formalities/mortgages; (8) every finding in `findings[]` has a non-null `page` or `evidence.note` fallback (never silently absent).
   - Assert every finding `status` in the payload is a key in the section-3.2 table (no unmapped token reaches the projected output).
4. Frontend component tests (Jest, extending `CustomerDecisionReport.test.js`, `CustomerReportView.test.js`):
   - Assert that `PARTIAL_REPORT_AVAILABLE` plus `decision_model` with sections renders `CustomerDecisionReport`, not `V2CustomerReportFallback`, once the payload shape reflects populated sections (component behavior keys off `report.decision_model.sections` non-emptiness plus status, not a raw flag -- the frontend never reads env flags directly, consistent with the existing pattern, section 2.5).
   - CRITICAL/secondary split renders two headed groups summing to the same item count as today's flat list (invariant 5).
   - Five-/six-state tone table: one test per token in section 3.2 asserting a distinct label (icon may repeat tone, never repeats label).
   - `in_conflitto` finding renders with the new status token and does not crash when `conflicts` is absent (backward compatible with older payloads).
   - Evidence/page props unchanged: existing `Pages`/`PageRefs` snapshot tests continue passing unmodified.
5. All new/changed tests run fully offline against sanitized fixtures only -- no network, no OpenAI, no production data, no case/tester identifiers outside `fixtures/` and `docs/`.

---

## 7. Files expected to change

This branch is intentionally frontend-heavy, but it does add one small,
justified backend view-model extension (section 3.1): the gate broadening
at `decision_model.py:2128` and the new `_build_conflicts` projection are
both required because the frontend has nothing to render if `decision_model`
itself never computes the sections/findings for a partial report or for
conflicts -- the frontend cannot legitimately re-derive that content itself
(that would violate "no client-side re-derivation of meaning"). Everything
else is frontend grouping/labeling of already-shipped data.

Backend, additive and flag-gated:
- `backend/correctness_v2/feature_flags.py` -- new `FLAG_REPORT_CLARITY` plus `report_clarity_enabled()`.
- `backend/correctness_v2/decision_model.py` -- broaden the section gate (section 3.1a); add `_build_conflicts()` (section 3.1b), wired into `build_decision_model()`'s `findings` list.
- `backend/correctness_v2/tests/test_decision_model.py`, `test_beta_multilot_partial_report.py` -- new assertions (section 6).

Frontend:
- `frontend/src/components/correctness-v2/CustomerDecisionReport.js` -- hero summary block (section 3.5), CRITICAL/secondary split of `VerificheSection` (section 3.3), six-state tone table replacing `CONFORMITY_TONE` (section 3.2).
- `frontend/src/components/correctness-v2/CustomerReportView.js` -- widen the `CustomerDecisionReport` routing condition (section 2.5) to include `PARTIAL_REPORT_AVAILABLE` once `decision_model.sections` is populated for it; retire/branch off `V2CustomerReportFallback`'s money duplication (section 3.4); evaluate removing `CustomerOtherFindings` once unreachable (section 3.9).
- `frontend/src/components/correctness-v2/shared.js` -- reuse only (a new tone/icon table lives with the component that needs it, not here, unless a second component needs the same table).
- Corresponding `*.test.js` files for the above.

Not touched, explicitly:
- `backend/correctness_v2/verdict_model.py` (CanonicalVerdict computation) -- read-only consumer.
- `backend/correctness_v2/partial_report.py` (Branch 3 disclosure contract) -- read-only consumer; `partial_status` shape frozen (invariant 10).
- `backend/correctness_v2/quality_gate.py`, `coverage_audit.py`, `fact_lineage.py`, `contract.py`'s fact-extraction logic -- no reconciliation or extraction change of any kind.
- `backend/correctness_v2/user_confirmations.py`, `workspace.py`, `job_status.py`, `orchestrator.py` -- no job/confirmation-flow change.
- Auth, beta/entitlement, credits/Stripe, PDF retention, concurrency settings -- untouched, unrelated to presentation.
- `LotWorkspace.js` -- multi-lot list/status board unchanged (section 3.6).
- `customer_report.py` -- the P6 label-fallback fix (`:897-898`, adding a `"conforming"` entry to `_CLASSIFICATION_LABELS`) is noted as a candidate minor correction for Sol to make in the same branch since it is a one-line, clearly-scoped defect directly contributing to the "wall of Da verificare" complaint, but it is not required for the flag-gated rollout to be safe (the decision-model path already reads `classification_label` correctly upstream); Sol should treat it as optional cleanup, tested separately from the flag.

---

## 8. Prompt-change verdict: NO

No OpenAI prompt, extraction instruction, or model call changes in any way.
Every field this plan touches is already computed by `verdict_model.py`,
`decision_model.py`, `partial_report.py`, or `contract.py`'s existing
deterministic (non-LLM) projection logic -- confirmed by
`decision_model.py:1-19`'s own module docstring ("never calls OpenAI, never
touches the network, never reads the PDF") and `partial_report.py:1-6`
("never changes CanonicalFacts, CanonicalVerdict, readiness, severity"). This
plan's only backend change (section 3.1) calls the same deterministic
functions against a status they weren't previously invoked for, and adds
one new pure projection function reading only already-computed verdict
fields. If implementation discovers a genuine gap requiring new extracted
facts (it should not), that is out of scope for this branch and must go
back to Branch 1/2 planning, not be patched in here.

---

## 9. Frontend scope

This branch is legitimately frontend-heavy: most design items (sections
3.3, 3.4, 3.5, 3.6, 3.7, 3.9, 3.10) are pure frontend grouping, labeling,
layout, and duplication removal. Confirmed no client-side re-derivation of
meaning:

- All tone/label lookups are static dictionaries keyed by a `status`/`tone` token already present on the payload (section 3.2's table is the spec for the one new dictionary needed).
- The CRITICAL/secondary split (section 3.3) partitions an existing array by existing `severity`/`blocking` fields -- a sort/filter, not a classifier.
- The hero summary (section 3.5) reads three fields that are already independently rendered elsewhere on the page -- recombination, not computation.
- No frontend file gains a new regex, keyword list, or heuristic over raw report text (contrast with the existing `CustomerReportView.js` helpers `normText`/`shortExcerpt`/`complianceTone` in the legacy fallback path, which do pattern-match text -- those are pre-existing and being retired, not extended).

---

## 10. Rollback plan

1. Set `CORRECTNESS_V2_REPORT_CLARITY_ENABLED=false` in `backend/.env` (gitignored, mirrors the Branch 3 `.env` pattern) -- takes effect on next backend restart, no code revert needed.
2. If a regression is found post-flip-ON in production, flip the flag back off; `decision_model.py`'s gate reverts to `REPORT_READY`-only behavior and the frontend automatically falls back to `V2CustomerReportFallback` for `PARTIAL_REPORT_AVAILABLE` exactly as it does today (section 2.5's existing conditional is the fallback path, not removed, only widened under the flag).
3. Because no existing field is renamed/removed (section 5), a full revert of the frontend commits is also safe at any point without a corresponding backend revert, and vice versa -- the two sides degrade independently.
4. No database migration, no Mongo schema change, no credit/quota impact -- rollback carries zero data-consistency risk.

---

## 11. Red-team

(a) Could a clarity redesign accidentally hide a critical finding? Risk: the
CRITICAL/secondary split (section 3.3) or the hero summary (section 3.5)
could let a `blocking: true` finding get collapsed into "Altre verifiche" or
omitted from the hero. Mitigation: section 3.3's CRITICAL predicate
(`blocking is True OR severity < 3`) is a superset test, never a cap --
invariant 5 asserts the split is a strict partition of the full
`sections.verifiche.items` array (nothing dropped, only regrouped); the
hero (section 3.5) is explicitly additive (full sections remain below it,
never replaced), and a unit test asserts every `blocking` finding's title
appears in the CRITICAL group.

(b) Could it re-derive or contradict the CanonicalVerdict? Risk: a new
frontend tone table could compute its own "how bad is this" logic that
disagrees with `canonical_verdict.severity`/`esito.level`. Mitigation:
invariant 2 forbids any new classifier; the six-state table (section 3.2)
maps `status` tokens 1:1 to fixed labels, never inputs to a severity
decision -- `esito`/`readiness` remain the sole severity authority and are
untouched by this branch (section 3.1a explicitly does not touch
`_build_readiness`/`_build_esito`).

(c) Could it drop evidence/page references? Risk: consolidating two
financial-summary renderers (section 3.4) or removing `CustomerOtherFindings`
(section 3.9) could lose a `page`/`evidence` prop along the way. Mitigation:
invariant 7 is a direct before/after diff test over `evidence`/`pages`/`page`
across all eight golden cases; `NumeriPrincipali` already threads every
evidence field the legacy `CustomerMoneySection` does (cross-checked in
section 2.5/3.4) so no field is lost in the consolidation, only the
presentation logic around it.

(d) Could it present an omission as reassuring? Risk: INCERTO/NON DICHIARATO
findings render in a neutral `slate` tone, which could visually read as
"fine" (like verde) to an inattentive user. Mitigation: invariant 8 forbids
verde tone for any `UNKNOWN`/missing-backed finding; the six-state table
keeps `slate` visually distinct from `verde` (different background/border
per existing `CHIP_TONES`, `shared.js:30-38`) and each state additionally
gets a distinct icon plus label (section 3.2) so "not declared" cannot be
mistaken for "declared and fine" even at a glance -- green is reserved
exclusively for `dichiarato_perizia`/`confermato_utente`.

(e) Could it leak a raw enum? Risk: the new `_build_conflicts` projection
(section 3.1b) reads `canonical_verdict.conflicts[].reason_code` (e.g.
`CONFLICT_REQUIRES_REVIEW`, `POSSIBLE_CROSS_LOT_LEAKAGE`) -- internal
machine tokens. Mitigation: `_build_conflicts` must map `reason_code`
through a fixed Italian label table before attaching it to a finding
(mirroring `partial_report._REASON_LABELS_IT`, `partial_report.py:46-49`),
never attach the raw code; invariant 3 is a grep-enforceable test scanning
rendered output for known internal token strings.

(f) Could it mislead on partial/blocked status? Risk: once section 3.1a
makes a `PARTIAL_REPORT_AVAILABLE` report look as rich and organized as a
`REPORT_READY` one, a buyer could mistake it for a complete, ready report
and miss that professional verification is mandatory. Mitigation:
`CustomerPartialReportBanner` stays pinned above the full report body,
unchanged in content (section 3.7); invariant 9 pins `readiness.state ==
TECHNICAL_REVIEW_REQUIRED` and `esito.level` to red/amber for every partial
report regardless of section richness, so the hero summary (section 3.5) --
which surfaces `esito` first, above everything -- will always show the
"verification required" posture before the buyer ever reaches the detailed
sections; invariant 10 freezes the `partial_status` contract so Branch 3's
disclosure guarantee cannot silently weaken as a side effect of this
branch's grouping changes.

---

## 12. Amendment — Italian-first bilingual + Gemini translation (owner-approved 2026-08-15)

Adds a bilingual presentation layer to this plan. Italian is authoritative + visually primary;
English is smaller/muted translation directly underneath (not two columns, not duplicated cards).
Fable revalidated the fit; the two data-handling decisions below were made by the owner.

### 12.A Owner decisions
- **Dynamic translation cache = OWNERSHIP-SCOPED.** Dynamic (Gemini) translations can contain
  customer-specific appraisal content (verbatim excerpts with addresses/cadastral/financials), so they
  are isolated by authenticated ownership + analysis/job context and reachable only through the same
  owner-gated path as the report. A dynamic translation is NEVER reused cross-user merely because a
  source-text hash matches. The STATIC UI/glossary dictionary (no customer data, never through Gemini)
  MAY be globally reusable.
- **Gemini translation = QUOTA-EXEMPT INFRASTRUCTURE.** Translation is a presentation/UX cost borne by
  Perizia Scan; it MUST NOT consume customer credits, beta analysis allowance, or report quota.
  Minimize cost via static deterministic translations + ownership-scoped caching + no repeat Gemini
  calls for identical (source_text_hash, prompt/glossary version). Translation failure falls back to
  Italian and MUST NEVER affect analysis entitlement or report availability.

### 12.B Architecture (translation OUTSIDE the synchronous report path)
`decision_model`/`customer_view` are pure/deterministic/no-network (`decision_model.py:1-19`); a Gemini
call inside them would hang/corrupt every report read. Therefore translation is a separate additive
layer via **lazy fetch-after-render**: the Italian report renders first from the existing synchronous
path; English is progressive enhancement fetched from a dedicated owner-scoped translate path AFTER the
Italian report is already shown. English is presentation metadata attached alongside a
`status`/`finding_id`; it NEVER feeds back into severity/status/readiness/disclosure_state/
canonical_verdict. **New invariant (grep-enforceable, same pattern as invariant 2):** no function that
computes severity/status/readiness/disclosure ever reads English text.

### 12.C Gemini client — REUSE, do not create a second provider
Reuse `backend/narrator.py:1166 _call_gemini_narrator_llm(*, api_key, model, prompt, timeout_seconds)`
(env already wired: `GEMINI_API_KEY`/`GOOGLE_API_KEY`, `GEMINI_DECISION_MODEL` default gemini-2.5-flash,
timeout). Add a backward-compatible `system_instruction: str` parameter (extract from the existing
hardcoded `_GEMINI_NARRATOR_SYSTEM_PROMPT` constant as the default) — a signature-only change that does
NOT alter narrator semantics when called the existing way. Do NOT fork or add a new client. Correctness
V2 has zero Gemini plumbing today (`orchestrator.py:18`, `__init__.py:12`).

### 12.D Three-way translation split
- **STATIC** (deterministic reviewed bilingual dictionary, NO Gemini, globally cacheable, versioned):
  the six state labels, section titles, `_STATUS_LABELS`, `_DECISION_LABELS`/`_ESITO_WORDING` fixed
  strings, `_READINESS_LABEL`, `_CONFIRM_OPTIONS`, `_CONFORMITY_WHY*` templates, and the professional
  glossary (perizia, pignoramento, ipoteca, decreto di trasferimento, formalità, sanabile,
  opponibilità, agibilità, conformità catastale/edilizia, diritto di usufrutto, prezzo base d'asta,
  offerta minima, procedura esecutiva, etc.).
- **PASSTHROUGH** (NEVER translated, identical in both languages): `case_identity` (address, tribunale,
  procedura_rge), Bene/lot labels, cadastral references, amounts/`amount_display`, percentages, dates,
  law/article numbers, page numbers, `evidence.page`, RGE/procedure refs.
- **DYNAMIC** (Gemini, cached, fail-soft — the high-stakes bucket): `evidence.excerpt` verbatim quotes,
  `customer_summary`/`notes`-derived text, `occupazione.dettaglio`, `partial_status.message`, formality
  `card["details"]`, and the assembled `decision.reason` sentence. Treat as fact-adjacent legal text.

### 12.E Constraint-checked prompt + post-translation validation
The translation prompt must isolate EXACTLY the free-text span to translate (never a whole finding JSON
node — otherwise Gemini could translate an embedded address/cadastral code). It must carry the §6
preservation constraints: negation, uncertainty, declaration status, conflict status, cancellation
TENSE, and leave amounts/percentages/dates/lot/Bene/RGE/cadastral/law-article/page refs unchanged.
Semantic guards: "Non dichiarato"≠"non compliant"; "Da verificare"≠"incorrect"; "Da cancellare a cura
della procedura"≠"already cancelled"; "Dichiarato"≠"independently verified". Provide the glossary as
guidance. Add a post-translation validator (reuse the PATTERN of `narrator.py`'s
`validate_gemini_decision_payload`, narrator.py:1651): reject suspicious output (numbers/dates/refs
changed, empty, wrong-language) and fall back to Italian rather than trust it blindly.

### 12.F Cache contract
Key = `(analysis_id or job_id, source_text_hash, glossary_prompt_version)`; stored with the same access
control as `customer_report.json` artifacts (owner-scoped, never a public/global lookup table); NOT
inside CanonicalVerdict; presentation metadata only. Static glossary is the only globally-cached layer.
Record the cache-isolation posture in the risk register.

### 12.G Failure-harmless + test discipline
Gemini timeout/failure/empty/invalid → Italian report fully available, English omitted or safe
deterministic fallback; never fails analysis/blocks report/changes readiness/severity/disclosure/hides
Italian warnings/causes regeneration/decrements quota. Automated tests MOCK Gemini (no real paid
calls). Any optional real-translation smoke must be reported (what/why) BEFORE running. Feature-flagged
`CORRECTNESS_V2_REPORT_CLARITY_ENABLED` (default OFF); flag-OFF == today (no bilingual, no translate
path, no Gemini). PDF retention is NOT touched here (stays dormant, flag OFF). No Branch-1/2/3 semantic
changes; concurrency stays 2.
