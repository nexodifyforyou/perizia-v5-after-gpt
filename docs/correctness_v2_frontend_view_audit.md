# Correctness V2 Frontend View Audit

Audit date: 2026-07-05

Scope: audit only. No code changes, no commits, no artifact mutation. I inspected the frontend code, API helpers, auth/admin gating, tests, and persisted `customer_report.json` artifacts for:

- Torino: `analysis_9418e2972795`
- Pistoia: `analysis_0acceec34340`
- 1859886_C: `analysis_3cad50719f75`

Local API note: `GET /api/analysis/perizia/{analysis_id}/correctness-v2/latest` returned `401` without an authenticated `session_token` cookie. I therefore used the local persisted Correctness V2 artifacts under `_correctness_v2/jobs/`, which are the same JSON files served by the admin-only artifact routes.

## Source Files Inspected

- `frontend/src/components/correctness-v2/CorrectnessV2Panel.js`
- `frontend/src/components/correctness-v2/CorrectnessV2Panel.test.js`
- `frontend/src/lib/api/perizia.js`
- `frontend/src/pages/AnalysisResult.js`
- `frontend/src/context/AuthContext.js`
- `frontend/src/lib/featureAccess.js`
- `backend/correctness_v2/api.py`
- `backend/correctness_v2/feature_flags.py`
- `backend/server.py`

Important current gating:

- `AnalysisResult.js` sets `canPreviewCorrectnessV2 = Boolean(user?.is_master_admin)`.
- `CorrectnessV2Panel` returns `null` unless `isAdmin` is true.
- Backend admin logic derives admin from configured email allowlists. `ADMIN_EMAILS` defaults to `nexodifyforyou@gmail.com`; `MASTER_ADMIN_EMAIL` defaults to `admin@nexodify.com`.
- Business requirement says Admin View must be visible only to `nexodifyforyou@gmail.com`. Current frontend does not check that exact email; it trusts `user.is_master_admin`.

## A) Current Frontend Sections

### Current Render Order

`CorrectnessV2Panel` renders this outer sequence:

1. Admin preview header: `Admin preview`, status badge, `Correctness Mode V2`, explanatory copy, run button.
2. Loading latest job line, while loading.
3. Job/status box, if a job or error exists.
4. Lot selector, only when job status is `LOT_SELECTION_REQUIRED`.
5. `Contract ready` box, only when status is `CONTRACT_READY` and no report is available.
6. Manual review box, only for `NEEDS_MANUAL_REVIEW`, `CONTRACT_VALIDATION_FAILED`, or failed statuses.
7. Quality control table for manual-review/failed states when `report.quality_control` exists.
8. Coverage-failed red box and quality table when `report.quality_control.coverage_status === "FAIL"`.
9. Full report article only when job status is `REPORT_READY`, report status is `REPORT_READY`, and coverage did not fail.
10. Report loading / missing report warning states.
11. No-job-found state.
12. Running-job duplicate-start warning.

Inside the full report article, the current order is:

1. Header: `Correctness Mode V2 preview/admin`, report title/subtitle, report status badge.
2. `Dati principali`
3. `Struttura lotto / beni`
4. `Beni`
5. `Sintesi esecutiva`
6. `Dati chiave`
7. `Stato di occupazione`
8. `Risk sections`
9. `Money sections`
10. `Conformità e documenti tecnici`
11. `Formalità e cancellazioni`
12. `Superfici e dati catastali`
13. `Checklist acquirente`
14. `Punti da verificare`
15. `Controllo qualità pagina per pagina`
16. `Indice delle evidenze`
17. `Debug evidenze (admin)`
18. Disclaimer

### Section Classification

| Frontend section/title | Data source | Customer-safe | Admin/debug only | Confusing now | Recommendation |
|---|---|---:|---:|---:|---|
| `Admin preview` / `Correctness Mode V2` | job from `/latest` and `/jobs/{job_id}` | No | Yes | Yes | Move to Admin View only. Never show to customer/tester. |
| Status badge values like `REPORT_READY`, `LOT_SELECTION_REQUIRED`, `NEEDS_MANUAL_REVIEW` | `job.status`, `report.report_status` | No | Yes | Yes | Hide from customer. Translate to `Ready`, `Needs review`, or omit. |
| Run button: `Run Correctness V2`, `Run again`, `Analyze this lot` | `startCorrectnessV2` action | No | Yes | Yes | Admin View only. Customer should not see pipeline controls. |
| Job status box with stage/reason/next steps | `job.current_stage`, `job.reason_code`, `job.reason_human`, `job.troubleshoot_message`, `job.next_steps` | No | Yes | Yes | Admin View only. |
| Lot selector | `report.lot_selection.lots`; fallback `job.available_lots` | Partly | No | Slightly | Keep. Rename actions to customer language, e.g. `Vedi report lotto`. Hide confidence/debug. |
| Lot selector details | lot `ownership_right`, `occupancy_summary`, `evidence_pages`, money rows | Yes if simplified | No | Medium | Keep lot label, address, type, ownership, occupancy, base price. Hide `confidence`; keep page refs only if presented as proof. |
| `Dati principali` | `report.case_identity` | Yes | No | Low | Keep, but merge into Customer `Property summary`. |
| `Struttura lotto / beni` | `report.lot_structure` | Partly | No | Medium | Keep only `lot`, `number of beni/accessories`, multi-lot flag. Hide machine-ish fields like `multi_lot`, `multi_bene`, `bene_ids` labels. |
| `Beni` | `report.beni_sections` | Partly | No | High | Keep only when multi-bene or meaningful accessories exist. Must show human labels/types, not only `Bene 1`. |
| `Sintesi esecutiva` | `report.executive_summary` | Partly | No | Medium | Replace with decision box plus 3-5 critical facts. Current count-based lines are too abstract. |
| `Dati chiave` | `report.key_facts` | Yes | No | Medium | Merge into Property summary and Economic summary; avoid duplicate facts already in `Dati principali`. |
| `Stato di occupazione` | `report.occupancy_section` | Yes | No | Medium | Keep. Needs clearer explanation of lease/opponibility/title implications. |
| `Risk sections` | `report.risk_sections` | Partly | No | High | Rename to customer language. Merge with technical risks and decision box. Current English title and dense grouping are not customer-ready. |
| `Money sections` | `report.money_sections` | Partly | No | High | Replace with final money chain and explicit buyer-cost notes. Move comparatives/context/uncertain buckets to admin or collapsed explanation. |
| `Catena di valore` | `report.money_sections.valuation_chain` | Yes | No | Medium | Keep, simplified as final chain. |
| `Condizioni di vendita` | `report.money_sections.auction_terms` | Yes | No | Low | Keep if present. |
| `Costi a carico dell'acquirente` | `report.money_sections.buyer_side_costs` | Yes with warnings | No | High | Keep only explicit buyer burden. If already included in valuation, show a strong note to avoid double counting. |
| `Formalità cancellate dalla procedura` money bucket | `report.money_sections.procedure_cancelled_formalities` | Partly | No | High | Merge into `Formalities`; do not show as money bucket by default. |
| `Comparativi di mercato` | `report.money_sections.market_comparatives` | No by default | Mostly admin/context | High | Hide from default customer view; optional collapsed proof/context. |
| `Dati economici di contesto` | `report.money_sections.context_values` | No by default | Mostly admin/context | High | Hide by default; values like mortgage principal/rendita can be mistaken for buyer costs. |
| `Importi da verificare` | `report.money_sections.uncertain_money` | Yes if small and explained | No | Medium | Keep only as explicit `Manual review needed` items. |
| `Conformità e documenti tecnici` | `report.compliance_section` | Yes if rewritten | No | Medium | Keep simplified customer sentences grouped by urbanistica/catastale/impianti/APE/agibilità. |
| `Formalità e cancellazioni` | `report.formalities_section` | Yes if explained | No | High | Keep only with plain explanation: cancelled by procedure vs explicit buyer burden. |
| `Superfici e dati catastali` | `report.surfaces_section` | Partly | No | High | Hide full grid by default. Keep key surface/catasto only when unambiguous. Duplicate values should be admin/manual-review. |
| `Checklist acquirente` | `report.buyer_checklist` | Yes | No | Medium | Keep, but limit to 5-8 clear actions. Current lists can be too long and technical. |
| `Punti da verificare` | `report.manual_review_flags` | Partly | Mixed | High | Customer view should show curated review items only. Move validator/debug/coverage flags to Admin View. |
| `Controllo qualità pagina per pagina` | `report.quality_control` | No | Yes | Very high | Admin View only. Never default customer-visible. |
| `Indice delle evidenze` | `report.customer_evidence_index`; fallback humanized `report.evidence_index` | Yes | No | Medium | Keep as `Evidence/proof`, but cap and show only page + topic + exact excerpt. |
| `Debug evidenze (admin)` | `report.admin_evidence_index`; fallback raw `report.evidence_index` | No | Yes | Very high | Admin View only. |
| Disclaimer | `report.disclaimer` | Yes | No | Low | Keep. |

## B) Per-Report Audit

### 1. Torino: `analysis_9418e2972795`

Artifact used: `cv2_f9dc9c8e774d49608e724adf76aeed17`

Status: `REPORT_READY`; quality `PASS`, `PASS_WITH_WARNINGS`, score `96`; quality rows `79`; customer evidence rows `43`; admin evidence rows `14`; manual flags `22`.

#### Top 10 Customer-Visible Facts

1. Tribunal/procedure/lot: Tribunale Ordinario di Torino, `292/2025`, `LOTTO 1`.
2. Property: apartment in Torino, Via Marchese Visconti 6, Borgo Vittoria / Circoscrizione 5.
3. Ownership: `1/1 di piena proprietà`.
4. Beni display: one main property, `Bene principale: appartamento`.
5. Accessories shown under the main property: autorimessa, cantina, soffitta, solaio.
6. Occupancy: occupied by tenant; lease type `4+4`, stipulated and registered on `30/12/2016`.
7. Lease warning: report says the lease predates pignoramento/fallimento; expiry shown as `31/12/2020`, but renewal/current enforceability is not clear.
8. Additional occupancy issue: attic/soffitta was not inspected and may be occupied without title by another condominium resident.
9. Money chain: market value `€ 43.654,20`; regularization costs `€ 5.250,00`; value as-is `€ 38.404,20`; formalities cancellation costs `€ 294,00`; final judicial sale value `€ 38.110,20`.
10. Technical risks: edilizia and catastale are regularizable; gas/electric systems lack certification; gas appears sealed/disconnected; APE data incomplete.

#### Top 10 Admin/Debug Facts

1. Job `cv2_f9dc9c8e774d49608e724adf76aeed17`, stage `step3:report_ready`, created `2026-07-05T11:51:22Z`.
2. `safe_to_show_customer: true`, but the current component still wraps it in an admin preview.
3. Quality score `96`, readiness `READY_WITH_WARNINGS`, `8` warning count.
4. Page-by-page quality table has `79` rows.
5. Manual flags: `22`, including missing auction terms, unclear lease renewal, incomplete APE, unquantified formalities, and validator warnings.
6. Validator/debug flags include `ZERO_AMOUNT_BUYER_COST`, `DUPLICATE_MONEY_ROW`, and `SAME_AMOUNT_CONFLICTING_KIND`.
7. Evidence: `43` customer rows, `14` admin raw-key rows, `14` legacy rows.
8. Money buckets include `9` market comparatives and `6` context values.
9. Surfaces/catasto include duplicate/conflicting values: e.g. commercial surface `46,95` and `23`; foglio `1125` and `51`; particella `22` and `1705`.
10. No auction terms are present in `auction_terms`, while the report has final judicial sale value.

#### Confusing/Redundant Sections

- `Dati principali` and `Dati chiave` repeat tribunal, procedure, lot, address, type, ownership.
- `Risk sections`, `Conformità e documenti tecnici`, `Checklist acquirente`, and `Punti da verificare` repeat the same technical issues in different formats.
- `Money sections` shows valuation, buyer-side cost, formalities, comparatives, context values, and uncertain money as equal-looking grids.
- `Superfici e dati catastali` exposes duplicate/conflicting catasto fields without a customer explanation.
- `Controllo qualità pagina per pagina` and `Debug evidenze (admin)` are mixed into the same report body.

#### Dangerous Sections for Customer Misunderstanding

- The `€ 294,00` cancellation cost appears both in the valuation chain and buyer-side costs. It says already included, but this still invites double counting.
- Large mortgage/formality amounts (`€ 150.000,00`, `€ 75.000,00`) can be mistaken for debts owed by the buyer despite the note.
- Market comparatives can be mistaken for the actual asset value or offer price.
- `PASS`/`CUSTOMER_READY` quality language can overstate certainty even though there are 22 verification points.
- Lease/opponibility is legally important but currently buried in paragraph text.

#### Missing Customer Explanation

- A one-line decision: `Attention / Manual review needed because the asset is occupied, lease status needs legal review, and technical/impianti issues require verification`.
- Plain explanation of whether `€ 294,00` is extra cash due after award or already embedded in the final value.
- Plain explanation of lease expiry vs opponibility.
- A short formalities explanation: procedure-cancelled formalities are not buyer debt unless the perizia explicitly says otherwise.
- A visible "what to do before bidding" list with max 5-8 actions.

### 2. Pistoia: `analysis_0acceec34340`

Artifacts used:

- No selected lot / selection screen: `cv2_ec9706562f254291b14f41c32d64abcb`
- Selected lot 1: `cv2_a7988b8035fe4639bed6af96929d4289`
- Selected lot 2: `cv2_a951b28219724151809507afa24977c6`
- Selected lot 3: `cv2_509bbb68c01346c19dc82a7e773637f9`

#### Lot Selector Content

The latest lot-selection report has `report_status: LOT_SELECTION_REQUIRED`, title `Selezione del lotto richiesta`, and lot IDs `1, 2, 3`.

The frontend shows:

- Admin preview/status first.
- Status box with `LOT_SELECTION_REQUIRED`, stage `step3:lot_selection_required`, and reason text.
- `Selezione lotto` section.
- Search box `Filtra lotti`.
- Three lot cards, each with an `Analyze this lot` button.
- Card fields: label, address, type, confidence, key money, hidden details for ownership, occupancy, and pages.

Actual lot cards:

| Lot | Label | Address | Type | Ownership | Occupancy | Key money |
|---|---|---|---|---|---|---|
| 1 | `Lotto 1 - Bene N° 1` | Montecatini-Terme (PT), via Giuseppe Garibaldi n.c. 23 | Fabbricato civile terra-tetto | Piena proprietà 1/1 | Libero | Prezzo base `€ 64.198,00` |
| 2 | `Lotto 2 - Bene N° 2` | Pieve a Nievole (PT), via Colonna s.n.c. | Magazzino with parking/area urbana | Piena proprietà 1/1 plus 1/4 stradella | Libero | Prezzo base `€ 84.000,00` |
| 3 | `Lotto 3 - Bene N° 3` | Pieve a Nievole (PT), via Colonna s.n.c. | Porzione di fabbricato in costruzione al grezzo and lastrico solare | Proprietà 1/1 plus 1/4 stradella | Libero | Prezzo base `€ 224.268,00` |

#### What Is Shown Before Lot Selection

The full `customer_report.json` body is not rendered for `LOT_SELECTION_REQUIRED`; only the admin preview/status and the lot selector are shown. This is good structurally, but the labels are admin-oriented:

- `LOT_SELECTION_REQUIRED`
- `step3:lot_selection_required`
- `Analyze this lot`
- `Confidence: high`

Customer copy should be "Scegli il lotto da consultare" and "Vedi report lotto"; hide confidence.

#### Selected Lot 1 Report

Frontend shows the full report body because status is `REPORT_READY`.

Customer-relevant facts:

- Title: Fabbricato civile terra-tetto, Montecatini-Terme, via Giuseppe Garibaldi 23.
- Lot structure says `bene_count: 3`, `multi_bene: true`, but cards are only `Bene 1`, `Bene 2`, `Bene 3`.
- Occupancy: Libero; no registered lease/comodato; keys held by a company member.
- Market value `€ 80.248,00`; auction/base term shown as `€ 64.198,00`.
- Critical risks: non-agibile; no APE; no electrical/thermal/water conformity declarations.
- Structural danger: abandoned/collabente, solai and terrace collapse risk, many solai propped.
- Hazardous material: fibro-cement items in attic, to be removed/bonified at buyer cost.
- Catastale and edilizia regularization needed; costs not quantified.
- Formalities cancelled by procedure, but cancellation costs not quantified.
- Quality: `PASS_WITH_WARNINGS`, score `98`, rows `60`, manual flags `18`, customer evidence `34`, admin evidence `16`.

Clarity judgment: not customer-clear enough. The top risk is severe, but it appears as one risk section among many. The decision box should say `Manual review needed / high attention: structurally unsafe collabente asset, non-agibile, missing certifications, possible hazardous material`.

#### Selected Lot 2 Report

Customer-relevant facts:

- Title: magazzino with area urbana/parcheggio and 1/4 stradella access rights.
- Lot structure says `bene_count: 3`, `multi_bene: true`, but again shows generic `Bene 1`, `Bene 2`, `Bene 3`.
- Occupancy section has no clear `status_label`; it contains a general risk about transfer in current condition with servitù and no warranty.
- Market value `€ 105.000,00`; auction/base term shown as `€ 84.000,00`.
- Critical risk: non-agibile.
- Catastale, edilizia/urbanistica, structural/seismic practices, servitù/access, and condition risks.
- APE and impianti are uncertain because the scope is unclear for Lotto 2.
- No buyer-side costs bucket, but regularization costs are not quantified.
- Formalities bucket has many large mortgage amounts, all procedure-side/cancellation context.
- Quality: `PASS_WITH_WARNINGS`, score `92`, rows `94`, manual flags `35`, customer evidence `40`, admin evidence `17`.

Clarity judgment: selected-lot data is partially clear for title/value, but not clear enough for occupancy, servitù/access, and missing costs. Admin needs all current detail; customer needs a shorter risk hierarchy.

#### Selected Lot 3 Report

Customer-relevant facts:

- Title: porzione di fabbricato in costruzione, Pieve a Nievole, via Colonna s.n.c.
- Subtitle mentions `69/2024 R.G.E.` and possible extension/joinder with `232/2024` for the stradella quota.
- Lot structure says `bene_count: 3`, `multi_bene: true`, but cards are generic.
- Occupancy: Libero.
- Market/completion value `€ 622.970,00`.
- Depreciation `55%` for unfinished/rough state: `€ 342.634,00`.
- Additional `20%` depreciation: `€ 56.068,00`.
- As-is value `€ 280.336,00`.
- Auction/base price `€ 224.268,00`.
- Risks: non-agibile, edilizia/catastale regularizable, unfinished rough building, servitù/access on stradella, IVA caveat.
- Quality: `WARNING`, `PASS_WITH_WARNINGS`, score `94`, rows `126`, manual flags `27`, customer evidence `22`, admin evidence `17`.

Clarity judgment: this is the clearest money chain of the Pistoia lots, but the `WARNING` quality state is admin-visible only as a badge/table. Customer should see "Manual review needed because the building is unfinished, non-agibile, has access/servitù issues, and cost amounts are not fully explicit."

#### Whether Selected-Lot Data Is Clear for Customer

Partly. The lot title and main money values are useful. The lot selector is good and should stay. The selected-lot reports are not customer-ready because:

- `Bene 1/2/3` cards do not explain what each bene is.
- Critical risks are not promoted into a decision box.
- Formalities and mortgage amounts are too prominent.
- Missing-cost and uncertain-scope items are mixed with regular facts.
- Admin quality/debug evidence is rendered in the same article.

#### Whether Admin Needs More Detail

Admin needs the current detail, and probably more structured access to:

- quality rows,
- raw evidence keys,
- coverage gaps,
- validator warnings,
- selected-lot routing decisions,
- artifact links/names,
- lot-selection metadata.

That detail should be preserved in Admin View, not shown in Customer Report.

### 3. 1859886_C: `analysis_3cad50719f75`

Artifact used: `cv2_cfa9134d230c430b8d24c7186985db1c`

Status: `REPORT_READY`; quality `PASS_WITH_WARNINGS`, score `93`; quality rows `195`; customer evidence rows `51`; admin evidence rows `34`; manual flags `28`.

#### Lotto Unico and 4 Beni Display

The report title is useful: `Ufficio, due garage e villetta - San Giorgio Bigarello (MN), Via Sordello n. 5`.

The frontend then displays:

- `Lotto Unico`
- `bene_count: 4`
- `multi_bene: true`
- four cards titled only `Bene 1`, `Bene 2`, `Bene 3`, `Bene 4`

This is not understandable enough. The title says the composition, but the cards do not map:

- Bene 1 appears to be the office.
- Bene 2 appears to be garage piano terra.
- Bene 3 appears to be garage seminterrato.
- Bene 4 appears to be the villetta.

The customer should not have to infer that from risk/checklist text. Each card needs a human title from the perizia, e.g. `Bene 1 - Ufficio`, `Bene 2 - Garage piano terra`, etc.

#### Money / Formalities / Customer Risks

Visible money chain:

- Market value: `€ 419.849,00`
- Regularization costs: `€ 23.000,00`
- Risk assumed for lack of warranty: `€ 5.000,00`
- As-is/final judicial sale value: `€ 391.849,00`
- Auction/base price: `€ 391.849,00`

Buyer-side costs shown:

- Bene 1 completion works: `€ 15.000,00`
- Bene 1 habitability practices: `€ 5.000,00`
- Bene 3 sanatoria estimate: `€ 3.000,00`

Risk: these buyer-side costs appear related to the `€ 23.000,00` regularization costs in the valuation chain. The customer needs one explicit sentence saying whether these are already deducted in the final value or additional cash items.

Formalities:

- Three mortgage/ipoteca entries with amounts `€ 216.000,00`, `€ 70.000,00`, `€ 183.406,92`
- One pignoramento
- All shown as procedure-cancelled / not buyer debt unless the perizia says otherwise

Customer risks:

- Occupied by debtor and spouse.
- No contracts/title dates/opponibility shown.
- All beni: no APE and no electrical/thermal/water conformity declarations.
- Catastale regularization for all beni.
- Urbanistica/PGT only as destination/CDU, no explicit conformance statement.
- Bene 1 and Bene 2 not agibile; Bene 3 has sanatoria; Bene 4 appears more regular.
- Access/parts issue: access via other mappali/properties; pedestrian entrance/stair for villetta on adjacent property.
- Shared utilities for villetta: water/gas meters shared with adjacent dwelling.
- Uncertain `€ 1.032,00` oblazione art. 36 bis.

#### Confusing/Redundant Sections

- `Beni` is present but not semantically useful because cards are generic.
- `Risk sections`, `Compliance`, `Checklist`, and per-bene risks repeat the same issues.
- `Money sections` splits the same cost story across valuation chain, buyer-side costs, and uncertain money.
- `Formalities` and procedure-cancelled money bucket duplicate each other.
- `Superfici e dati catastali` has `31` rows; this is admin/audit detail, not customer presentation.
- Quality table has `195` rows and should not be customer-visible.

## C) Role Split Proposal

### Tab 1: Customer Report

Visible to normal customer/tester by default. It should be short, decisive, and built from `customer_report.json` after sanitization.

Recommended sections:

1. **Executive Decision Box**
   - Status: `Safe`, `Attention`, or `Manual review needed`.
   - One-sentence reason.
   - Examples:
     - Torino: `Manual review needed: occupied asset with lease/opponibility uncertainty and technical regularization costs.`
     - Pistoia lot 1: `Manual review needed: structurally unsafe/collabente building, non-agibile, missing certifications, possible hazardous material.`
     - 1859886_C: `Attention/manual review: occupied by debtor, four beni with technical/catasto issues and buyer-side works to verify.`

2. **Property Summary**
   - Tribunal/RGE.
   - Lot.
   - Property type.
   - Address.
   - Ownership.
   - Number of beni/accessories.
   - Data source: `case_identity`, `lot_structure`, `beni_sections`.

3. **Auction / Economic Summary**
   - Market value.
   - Regularization costs.
   - Judicial sale value / base price.
   - Buyer-side costs only if explicit.
   - Important note when a cost is already included in valuation.
   - Data source: `money_sections.valuation_chain`, `auction_terms`, `buyer_side_costs`, `uncertain_money`.

4. **Occupancy / Title Status**
   - Free/occupied/lease.
   - Lease dates/opponibility warning when relevant.
   - Data source: `occupancy_section`.

5. **Technical / Document Risks**
   - Urbanistica.
   - Catastale.
   - Impianti.
   - APE/agibilità if present.
   - Customer-readable sentences only.
   - Data source: `compliance_section`, curated `risk_sections`.

6. **Formalities**
   - Procedure-cancelled formalities.
   - Buyer burden only if explicit.
   - Data source: `formalities_section`, `money_sections.procedure_cancelled_formalities`.

7. **Beni / Accessories**
   - Only if multi-bene or important accessory exists.
   - Must show human titles/types, not just `Bene 1`.
   - Data source: `beni_sections`, plus known title/type fields when available.

8. **Buyer Checklist**
   - Max 5-8 clear actions.
   - Data source: curated `buyer_checklist` plus selected `manual_review_flags`.

9. **Evidence / Proof**
   - Page number + exact perizia excerpt.
   - No raw internal keys.
   - Prefer the most important evidence only; full evidence index can be expandable but not enormous.
   - Data source: `customer_evidence_index`.

10. **Disclaimer**
   - Data source: `disclaimer`.

### Tab 2: Admin View

Visible only to `nexodifyforyou@gmail.com`.

Admin View can include:

- Job status.
- Run/rerun button.
- Lot analyze controls.
- Raw artifact links/names.
- Quality score.
- Page-by-page audit.
- Coverage audit.
- Manual review flags.
- Raw evidence keys.
- Money role audit.
- Extracted sections.
- Debug warnings.
- Source artifact names.
- API/job IDs and stages.

## D) Tester View

Tester should see the same Customer Report by default.

Recommendation: Customer Report + a small `Testing notes` box only when the user is a tester/beta partner. That box may include:

- report generated timestamp,
- non-technical feedback prompt,
- "This report is being tested; please report unclear facts."

Do not expose raw debug by default. Debug should require Admin View.

## E) What To Remove From Customer View

Remove or hide by default:

- `Admin preview`.
- `Correctness Mode V2 preview/admin`.
- Internal status names: `REPORT_READY`, `LOT_SELECTION_REQUIRED`, `NEEDS_MANUAL_REVIEW`, `CONTRACT_VALIDATION_FAILED`.
- Pipeline stage names such as `step3:report_ready`.
- Run/rerun/analyze buttons.
- Raw artifact names and paths.
- Quality score and `Controllo qualità pagina per pagina`.
- Full page-by-page audit table.
- `Debug evidenze (admin)`.
- Raw evidence keys such as `technical_compliance[2]`, `risk_classification[5]`.
- Validator/debug flags and `debug_detail`.
- Full `market_comparatives` and `context_values` money buckets.
- Full formalities amounts presented as ordinary money rows without a buyer/non-buyer explanation.
- Full `surfaces_section` when it contains duplicate/conflicting catasto rows.
- Full manual flags list when it includes internal validation/coverage messages.
- Confidence labels in lot selector.

## F) What To Keep Customer-Visible

Keep because it builds trust:

- Lot selector with lot labels, addresses, type, ownership, occupancy, and base price.
- Selected lot details.
- Final money chain.
- Explicit buyer-side costs, with `already included in valuation` note when applicable.
- Occupancy status and lease/opponibility warnings.
- Technical risks in customer-readable language.
- Formalities explanation: procedure-cancelled vs buyer burden.
- Buyer checklist, capped and clear.
- Evidence/proof: page number + exact perizia excerpt.
- Examples of useful proof currently available:
  - Torino p. 8, conformità urbanistica: customer evidence includes the excerpt that the property is conforming under PRGC.
  - Torino p. 19, cancellation costs: customer evidence supports `€ 294,00`.
  - 1859886_C p. 4, auction/base price: evidence includes `Lotto Unico - Prezzo base d'asta: € 391.849,00`.
  - 1859886_C p. 5, composition: evidence lists `Bene N° 1 - Ufficio...`.
  - Pistoia lot selector: page-supported lot cards for lots 1, 2, and 3.
- Disclaimer.

## G) Implementation Recommendation

Do not redesign data extraction yet. First split presentation and access.

### Files To Change

- `frontend/src/pages/AnalysisResult.js`
  - Replace single admin preview mount with role-aware tabs.
  - Pass user email/role explicitly to Correctness V2 area.

- `frontend/src/components/correctness-v2/CorrectnessV2Panel.js`
  - Split into:
    - `CorrectnessV2Tabs`
    - `CustomerReportView`
    - `AdminCorrectnessV2View`
    - `CorrectnessV2LotSelector`
    - `CustomerDecisionBox`
    - `CustomerMoneySummary`
    - `CustomerEvidenceList`
    - `AdminQualityPanel`
    - `AdminEvidenceDebug`

- `frontend/src/lib/api/perizia.js`
  - Keep admin routes.
  - Add or clearly name a customer-safe report route if backend exposes one.

- `backend/correctness_v2/api.py`
  - Keep admin artifact routes admin-only.
  - Add a sanitized customer-report route only if product wants customers/testers to use Correctness V2 output directly.
  - Strip `admin_evidence_index`, raw artifact paths, quality rows, debug flags, and internal status/stage data from any customer route.

- `backend/server.py`
  - Enforce exact admin email requirement for Admin View/API access if the business rule is strict: `nexodifyforyou@gmail.com`.
  - Current `ADMIN_EMAILS` default includes that email, but frontend should not be the only guard.

### Suggested Tab Names

- `Customer Report`
- `Admin View`

If the UI should stay Italian:

- `Report cliente`
- `Vista admin`

### Admin-Only Enforcement

- Server must remain the source of truth.
- For the specific business rule, Admin View should require:
  - authenticated user,
  - normalized email exactly `nexodifyforyou@gmail.com`,
  - backend permission check before serving raw admin artifacts.
- Frontend can hide the tab with:
  - `user?.email?.toLowerCase() === 'nexodifyforyou@gmail.com'`
  - optionally combined with `user?.is_master_admin`
- Do not rely on frontend-only gating.

### Tests Needed

- Customer render test:
  - hides admin preview/status/run button/stage/raw artifact/quality table/debug evidence/raw keys.
  - shows decision box, summary, money chain, occupancy, risks, checklist, evidence, disclaimer.

- Admin render test:
  - shows status, run button, quality table, admin evidence keys, artifacts.

- Role/access tests:
  - normal user sees Customer Report only.
  - tester sees Customer Report plus small testing notes.
  - `nexodifyforyou@gmail.com` sees Admin View.
  - other admin/master email behavior should match product decision.

- Lot selector tests:
  - no selected lot shows only selector/customer-friendly copy.
  - selecting lot 1/2/3 sends `selected_lot_id`.
  - selected lot reports keep lot-specific facts.

- Fixture tests using the three audited analyses:
  - Torino: occupied/lease warning and `€ 294,00` inclusion note.
  - Pistoia: lot selection and generic beni issue fixed.
  - 1859886_C: four beni receive understandable labels.

- API tests:
  - customer-safe route strips admin/debug fields.
  - admin route preserves them.
  - non-admin cannot fetch admin artifacts.

### Risks

- `customer_report.json` is not yet a clean presentation model. It is a rich audit contract with customer, admin, evidence, and quality concerns mixed together.
- Quality `PASS` does not mean the UI is customer-ready.
- Money can be double-counted unless included/excluded semantics are explicit.
- Multi-bene labels are weak in current payloads.
- If admin-only is exact-email business logic, current `is_master_admin` abstraction may be too broad.
- Existing backend Correctness V2 API is admin-only; exposing Customer Report requires a sanitized route, not just un-hiding the existing component.

## H) Final Judgment

- Is current frontend customer-ready? **No.**
- Is current frontend admin-useful? **Yes, but only as an admin/debug preview.**
- Biggest confusion risk: money/formalities/debug are mixed with customer facts, so a customer can misread mortgage/formality amounts or already-included costs as new buyer debt, while internal quality/status/debug labels create false confidence or confusion.
- Best next build step: split the current component into `Customer Report` and `Admin View`, enforce exact admin-only access for `nexodifyforyou@gmail.com`, and build the Customer Report around a decision box, property summary, final money chain, occupancy/title status, concise risks, buyer checklist, and proof excerpts.
