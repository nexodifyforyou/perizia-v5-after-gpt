# Customer Report Decision Workflow — Implementation Plan

Branch: `feature-customer-report-decision-workflow` (base `eff4abe48c96b5dddf4c472c2df2c594d1f9d163`)
Scope: transform the Correctness V2 customer report from an extraction display into a
calm decision workflow. Backend builds the customer decision model; frontend renders it.
Zero OpenAI calls, zero credit charges, zero artifact rewrites: the decision model is a
deterministic READ-TIME transform of the existing `customer_report.json`, and user
confirmations are the only new *mutable* state (persisted authoritatively in MongoDB,
never written back into any artifact).

> **Human decisions locked in before implementation (2026-07-18):**
> 1. **Cairate** — accept **fail-closed** as the acceptance criterion. Preserve the current
>    `VERIFICATION_REQUIRED` result with a customer-safe explanation; do NOT fabricate or
>    display the € 67.264,50 → € 59.923,16 chain as a validated report; keep that historical
>    chain only as an admin/regression reference clearly marked "non validato su main"; no
>    regeneration, no credits, no OpenAI in this branch.
> 2. **Confirmation storage** — **MongoDB is the authoritative source of truth** (not a
>    file store). See §L (rewritten). Confirmations are mutable, ownership-scoped, and
>    audit-tracked; a read-only snapshot may later ride into the V2 export artifact, but
>    Mongo remains authoritative.
> 3. **Value-chain reorder** — **presentation-only**, inside the decision model at read
>    time. Amounts/roles byte-for-byte unchanged; validator/contract untouched; original
>    extracted order preserved for Vista admin; ambiguous rows go to "Importi da chiarire"/
>    "Conferma necessaria", never force-inserted into the chain.
> 4. **Green Esito wording** — headline **"Nessuna verifica bloccante emersa dalla perizia"**
>    with the restrained supporting sentence in §D.1; green is never a guarantee of legal
>    conformity, investment safety, vacant possession, or absence of hidden defects.

---

## A. Current problems (grounded in the six real cases)

Audited artifacts (newest job per analysis under `/srv/perizia/app/_correctness_v2/jobs/`):

| Case | Job | Status | Visible load today |
|---|---|---|---|
| Torino (`…9418e2972795`) | `cv2_f61e35c5065a4f75a57a005f7409b9e4` | REPORT_READY, Lotto 1 | 42 customer-evidence entries (5 without excerpt), 9 key_facts, 6 exec-summary lines, 10 risk items in 3 sections, 8 compliance cards, 9 checklist items, 4 formality rows, 5-row chain + 1 buyer cost + 3 cancelled-formality refs + 14 comparatives + 2 context values |
| Pistoia (`…0acceec34340`) | `cv2_509bbb68c01346c19dc82a7e773637f9` | REPORT_READY, Lotto 3 | 22 evidence (6 missing excerpt), 10 risk items, 6 compliance, 10 formalities (duplicate "Pignoramento" pair), 6 checklist, chain of 4 rows **without a terminal sale-value row** |
| 1859886_C (`…3cad50719f75`) | `cv2_cfa9134d230c430b8d24c7186985db1c` | REPORT_READY, Lotto unico | 51 evidence (10 missing excerpt), 31 surface rows, 11 risk items, 9 compliance, 11 checklist, 4 beni ("Bene 1..4"), 1 genuinely uncertain amount (Oblazione € 1.032,00) |
| Orecchiazzzi (`…b3d5b9e23e22`) | `cv2_f731db8e1aca4b019e2783a43d94c8a8` | CONTRACT_VALIDATION_FAILED (`MONEY_CHAIN_INCONSISTENT`) | correct fail-closed safe report; customer view returns `VERIFICATION_REQUIRED` |
| Cairate (`…2aac57572b7a`) | `cv2_92f13a53a8b14834b5fbe69084d41d13` (+7 identical retries) | CONTRACT_VALIDATION_FAILED (`MONEY_CHAIN_INCONSISTENT`) per-lot; newest safe artifact is LOT_SELECTION_REQUIRED `cv2_7f3fe1ea1e814a1ea5b747848048524a` | per-lot money summaries carry duplicate rows ("Lotto 1 - spese condominiali insolute ai sensi dell'art. 568 c.p.c." and "…scadute e insolute alla data della perizia", both € 3.978,12) |
| Codogno (`…496b17c1c778`) | `cv2_09ead33b27d84e01adb149f98b0fa8ee` | REPORT_READY, Lotto 1 (of exactly 6, no Lotto 00) | chain lists BOTH deductions before "Valore nello stato di fatto"; € 14.000 regularization echoed as an amber buyer-cost card; ipoteche € 600.000/€ 615.000 with prominent amounts |

Concrete failure modes (each maps to a rule in this plan):

1. **Triple duplication of compliance facts** (Torino): the same 6 areas appear as
   `compliance_section` cards, again as `risk_sections` items (contract's `_risk_cards`
   copies `technical_compliance`), and a third time as `buyer_checklist` rows
   ("Verificare/regolarizzare conformità | conformità edilizia: …"). One fact, three cards.
2. **Occupancy repeated four times** (Torino): exec summary line, `key_facts`
   "Stato occupazione", `occupancy_section`, and two checklist rows "Valutare rischio occupazione".
3. **Money repeated**: `key_facts` carries "Valore di mercato"/"Valore di vendita
   giudiziaria" that also head the chain; exec summary restates both as sentences.
4. **Generic counts as headline** ("La perizia segnala 10 punti di attenzione.",
   "15 aspetti non sono stati verificati automaticamente…") — exactly the validator-screen
   tone the decision card must not have (`customer_report._executive_summary`).
5. **Page-reference smear**: every Torino chain row cites `[2, 3, 7, 8, 16, 17, 18, 19]`
   because `contract._money_sections` stamps the worksheet-level `money.evidence_pages`
   on every row. The decisive pages (18 chain, 19 final+cancellation) are indistinguishable.
6. **Evidence wall**: 42–51 `customer_evidence_index` entries, dominated by
   "Superfici e dati catastali" micro-topics (`_evidence_sources` adds one source per
   `surface_cadastral` fact — 31 in 1859886_C), several topically wrong excerpts
   ("Valore nello stato di fatto :: 46,95 m² Consistenza commerciale…" quotes a surface
   sentence for a valuation topic) and 5–10 "excerpt_missing" rows rendered by default.
7. **Chain ordering bug (presentation)**: `_money_sections` emits
   `chain.extend(precurrent_rows)` before "Valore nello stato di fatto", so Codogno shows
   `market − 15% − regolarizzazione → state → sale` where arithmetically the 15% produces
   the state value and the € 14.000 produces the sale value. Pistoia lot 3 additionally
   has no sale-value row at all (the € 224.268 lives only in `auction_terms`), so the
   chain visually "ends" at € 280.336.
8. **Formality duplication/emphasis**: Pistoia shows 10 formality rows including two
   indistinguishable "Pignoramento" rows (`_formalities_section` dedup key includes the
   description, and the two descriptions differ trivially); registered mortgage amounts
   (€ 850.000, € 600.000…) render as the visually dominant number of each card.
9. **Frontend infers conclusions from raw arrays** (violates non-negotiable rule 1):
   `CustomerReportView.js` computes `occupancyMeaning`, `complianceTone`,
   `findSupportExcerpt`, `dedupedExtraFacts`, `buildEvidencePreview`, `shortExcerpt`,
   and `CustomerOtherFindings` leftover logic client-side.
10. **No focused confirmation workflow**: the only HITL is the pre-report
    `MONEY_CONFIRMATION_REQUIRED` gate; a READY report with a genuinely unclear item
    (1859886_C "Oblazione indicativa art. 36 bis", € 1.032,00, reason already present in
    the row) offers the customer no way to resolve it.
11. **No readiness status**: nothing tells the customer "verifiche completate /
    conferme necessarie"; `manual_review_flags` (admin-only) is the closest thing.
12. **English/debug leakage risk**: the sanitized view is clean (verified: `customer_view`
    drops `manual_review_flags`, `surfaces_section`, `sections_meta`, `admin_evidence_index`,
    `evidence_index`, `quality_control`), but classification tokens (`conforming`,
    `regularizable`, `uncertain`) and kinds (`deduction`, `buyer_side`) still ride into the
    customer payload and the frontend keys logic off them (`complianceTone`, `ChainRow`).

What already works and must be preserved: fail-closed behavior (Orecchiazzzi/Cairate),
lot isolation (Pistoia lot 3, Codogno lot 1 of 6), comparative/context money separation
(`_split_uncertain_rows` + `doc_signals.COMPARATIVE_LABEL_KINDS`/`CONTEXT_LABEL_KINDS`),
the `included_in_valuation` echo with "Già considerato nella catena di valore.",
amount-free procedure-cancelled reference rows, the `amount_note` on non-buyer formality
amounts, and the verbatim-excerpt gate in `_find_verbatim_excerpt`.

## B. Current report data flow

```
orchestrator.start_job
  └─ analyst worksheet ─ validator ─ contract.build_contract → verified_report_contract.json
  └─ customer_report.render_success_report(contract, input_pages) → customer_report.json
       (or render_lot_selection_report / render_money_confirmation_report /
        render_safe_report / render_not_readable_report)
API read path (no recompute of facts):
  GET /api/analysis/perizia/{id}/correctness-v2/customer-view/latest   (api.correctness_v2_customer_view)
    └─ api._find_customer_job → artifacts.read_json(CUSTOMER_REPORT_FILE)
    └─ (NEW) await user_confirmations.list_for_analysis(analysis_id, owner_user_id)   [Mongo]
    └─ customer_view.is_customer_safe → customer_view.sanitize_customer_report(report, job, confirmations)
         └─ (NEW) decision_model.build_decision_model(report, confirmations)   [pure, sync]
         └─ customer_view.derive_decision (decision box, keyword heuristics)
Frontend:
  useCorrectnessV2CustomerView (pages/AnalysisResult.js lifts one hook instance)
    └─ CorrectnessV2Tabs → CustomerReportView → CustomerReportBody renders, in order:
       CustomerDecisionBox, CustomerPropertySection, CustomerOccupancySection,
       CustomerMoneySection, CustomerCostsSection, CustomerFormalitiesSection,
       CustomerComplianceSection, CustomerOtherFindings, CustomerChecklistSection,
       CustomerEvidence, disclaimer.
Admin: CorrectnessV2Panel (unchanged) via admin-only artifact routes in api.py.
Existing HITL: money_confirmation.build_money_confirmation (pre-report gate) →
  POST customer-view/confirm-money → orchestrator.resolve_money_confirmation
  (validate_answers → re-gate with money_confirmations as ground truth; answers persisted
  in job_status.json under "money_confirmations").
```

Key insertion point: **`customer_view.sanitize_customer_report` is the single choke point
every customer render passes through**. The decision model is attached there, derived from
the FULL stored report (before admin keys are stripped), so it works for every existing
artifact with no regeneration and no artifact rewrite.

## C. Proposed customer decision schema

New top-level key `decision_model` inside the sanitized customer payload (and previewable
raw in Vista admin). Built by a new module `backend/correctness_v2/decision_model.py`
(pure function; see §P). Schema version `cv2.customer_decision.v1`.

```jsonc
{
  "schema_version": "cv2.customer_decision.v1",
  "analysis_id": "analysis_…",          // from report.analysis_id
  "job_id": "cv2_…",
  "lot_id": "1",                         // report.lot_structure.selected_lot (or null)
  "report_status": "REPORT_READY",       // pass-through, already customer-safe statuses only
  "readiness": {                          // §M — internal enum NEVER shown raw
    "state": "CONFIRMATIONS_REQUIRED",   // READY_FOR_REVIEW | CONFIRMATIONS_REQUIRED |
                                          // TECHNICAL_REVIEW_REQUIRED | COMPLETE_FOR_EXPORT
    "label": "Conferme necessarie",      // fixed Italian label map
    "confirmations_total": 2, "confirmations_done": 1,
    "professional_checks_open": 3
  },
  "esito": {                              // §D.1
    "level": "ambra",                    // verde | ambra | rosso
    "headline": "Verifiche necessarie prima di procedere",
    "sentence": "…one plain-Italian sentence…",
    "drivers": [ {"finding_id": "…", "title": "…", "section": "verifiche"} ]  // max 5
  },
  "sections": { /* one key per §D section; a key is ABSENT when it has no content */ },
  "findings": [ Finding, … ],            // flat list; sections reference finding_ids
  "sources": [ Source, … ],              // §J decisive sources (max 8 primary + rest collapsed)
  "confirmations": [ ConfirmationView, … ] // §L user confirmations joined at read time
}
```

**Finding** (the atom everything renders from):

```jsonc
{
  "finding_id": "cmp-edilizia-l1-p7-2500.00",  // stable: see derivation below
  "section": "conformita",                     // esito|acquisto|numeri|occupazione|verifiche|
                                               // conformita|formalita|altri
  "topic": "conformita_edilizia",              // contract._area_token / doc_signals.label_kind
  "title": "Conformità edilizia",              // fixed Italian, from existing labels
  "status": "regolarizzabile",                 // closed enum, Italian tokens:
                                               // conforme|regolarizzabile|non_conforme|
                                               // non_verificato|conferma_necessaria|
                                               // da_verificare|completato|confermato_utente
  "status_label": "Regolarizzabile secondo la perizia",  // reuse _CLASSIFICATION_LABELS
  "severity": 3,                               // 1..7 priority class of §E (int, internal sort only)
  "customer_summary": "…",                    // from card.notes / summary (already Italian)
  "buyer_impact": "…",                        // fixed template per topic class ("Perché conta")
  "recommended_action": "…",                  // fixed template ("Cosa fare / Cosa verificare")
  "amount": 2500.0, "currency": "EUR",         // optional
  "amount_display": "€ 2.500,00",              // customer_report.format_eur
  "included_in_valuation": false,              // from roles / included_in_valuation flag
  "timing": "6 mesi",                          // optional, only when explicit
  "pages": [7],                                // CANONICAL page first (see §J), full list kept
  "evidence": {                                // optional; only when a verbatim excerpt exists
    "page": 7, "excerpt": "…verbatim…", "verbatim": true
  },
  "confirmation": {                            // §K; present only when eligible
    "eligible": true,
    "question": "…", "options": [ {"option_id": "…", "label": "…"}, … ],  // 2–4 + "unsure"
    "unsure_option": {"option_id": "non_sicuro", "label": "Non sono sicuro"}
  },
  "links": ["numeri"]                          // sections where the same fact is referenced,
}                                              // never duplicated as a full card
```

`finding_id` derivation (stable across re-reads of the same artifact, no randomness):
`sha1("|".join([section, topic, lot_id or "-", str(canonical_page or "-"), f"{amount:.2f}" if amount is not None else "-"]))[:12]`
prefixed by a 3-letter section code (`cmp-`, `mon-`, `occ-`, `frm-`, `ver-`, `alt-`).
The raw components come only from the stored artifact, so the same artifact always yields
the same ids; a rerun that changes the underlying fact intentionally changes the id.

**Source** (Fonti decisive):

```jsonc
{
  "source_id": "src-p18-catena",
  "page": 18,
  "title": "Catena di valutazione",              // human topic, fixed vocabulary
  "excerpt": "…verbatim sentence…",              // or null
  "excerpt_status": "covered" | "excerpt_missing", // from customer_evidence_index.coverage_status
  "why": "Perché conta: …",                      // one fixed-template line
  "priority": 2,                                  // §J ranking
  "finding_ids": ["mon-…"]
}
```

Field provenance — what today's schema already supplies vs. what is derived:

| Decision-model field | Exists today | Where |
|---|---|---|
| identity, lot, beni, accessories | yes | `case_identity`, `lot_structure`, `beni_sections` (incl. `accessories`, `bene_count` floor-at-1) |
| chain rows + roles | yes | `money_sections.valuation_chain` (`kind`, `roles` on contract rows) |
| buyer costs + included flag | yes | `money_sections.buyer_side_costs` (`included_in_valuation`, notes) |
| cancelled formalities (fact rows) | yes | `money_sections.procedure_cancelled_formalities` + `formalities_section` (`cancelled_by_procedure`, `buyer_burden`, `amount_note`) |
| unclear amounts | yes | `money_sections.uncertain_money` (with `reason`) |
| comparatives/context (for collapsed line + admin) | yes in FULL report | `money_sections.market_comparatives` / `context_values` (currently stripped by `_CUSTOMER_MONEY_KEYS`; decision model reads them BEFORE stripping and emits only `{count, pages}`) |
| compliance status/cost/timing/notes | yes | `compliance_section` (`classification`, `status_label`, `cost_display`, `timing`, `blocks_saleability`) |
| occupancy status/title/opponibility/dates/risks | yes | `occupancy_section` |
| verbatim excerpts + coverage status | yes | `customer_evidence_index` (`perizia_excerpt`, `coverage_status`, `report_section`) |
| checklist raw actions | yes | `buyer_checklist` |
| canonical page per topic | **derived** | §J: intersection/priority over `pages` + evidence entries |
| statuses/buyer_impact/recommended_action | **derived** | fixed Italian template per (topic class, status) — deterministic string tables, no free text |
| readiness | **derived** | §M from findings + confirmations + report_status |
| confirmations | **new Mongo store** | §L (`correctness_v2_confirmations` collection; fetched async by the route, passed as plain data into the pure builder) |

Hard properties: pure function, no OpenAI, no network, no PDF access; unknown/missing
input fields ⇒ the finding/section is omitted (never fabricated); the builder never
mutates its input dict; customer strings come only from (a) values already in the
artifact, (b) fixed Italian string tables in the module.

## D. Final section architecture (11 sections)

Order and mapping (backend key → frontend component). A section with no meaningful
content is ABSENT from `sections` and the frontend renders nothing (no "0 beni" cards).

| # | Section (Italian title) | `sections` key | Backend source | Frontend component |
|---|---|---|---|---|
| 1 | ESITO OPERATIVO | `esito` (top-level) | replaces `customer_view.derive_decision`: level from report_status + findings (verde: no open verifiche/conferme; ambra: any open verification/confirmation; rosso: only fail-closed, which customers today see as `VERIFICATION_REQUIRED` reason — kept) | `EsitoOperativoCard` (evolution of `CustomerDecisionBox`) |

**Esito wording table** (fixed Italian, in `decision_model.py`; max 3–5 drivers, no counts):

| level | headline | supporting sentence |
|---|---|---|
| `verde` | "Nessuna verifica bloccante emersa dalla perizia" | "Il report non rileva elementi bloccanti tra quelli espressamente indicati nella perizia. Restano consigliate le verifiche ordinarie prima di procedere." |
| `ambra` | "Verifiche necessarie prima di procedere" | one plain-Italian sentence naming the driver classes (occupazione/valore/conformità/…), no numeric counts |
| `rosso` | "Verifica tecnica richiesta" | customer-safe `VERIFICATION_REQUIRED` reason (fail-closed only) |

`verde` is explicitly **not** a guarantee of legal conformity, investment safety, vacant
possession, or absence of hidden defects; if any manageable warning remains the esito is
`ambra`, never `verde`. No buy/don't-buy or bidding advice in any state.
| 2 | COSA STAI ACQUISTANDO | `acquisto` | `case_identity` + `lot_structure` + `beni_sections` + one occupancy summary line; `key_facts` de-duplication moves server-side (delete `dedupedExtraFacts` from JS); "Lotto" and "Lotto selezionato" collapse to one row | `CustomerPropertySection` (modified) |
| 3 | NUMERI PRINCIPALI | `numeri` | (A) `catena`: `valuation_chain` re-ordered canonically (see §F) with role labels; (B) `costi_potenziali`: `buyer_side_costs` (+`included_in_valuation` note); (C) `scenari`: alternative valid rows (`source == "shared_summary_projection"` rows, second deduction paths) presented as scenario, not contradiction; (D) `da_chiarire`: `uncertain_money` only; plus `comparatives_summary: {count, pages}` one collapsed line | `CustomerMoneySection` + `CustomerCostsSection` (merged into one `NumeriPrincipali` component) |
| 4 | STATO DI OCCUPAZIONE | `occupazione` | `occupancy_section` + backend `perche_conta` / `cosa_verificare` (moves `occupancyMeaning` server-side); `opponibilita_label` = "Opponibilità da verificare" when the perizia is silent (Codogno's explicit text passes through verbatim) | `CustomerOccupancySection` (modified) |
| 5 | COSA VERIFICARE PRIMA DI PROCEDERE | `verifiche` | findings with `status ∈ {da_verificare, conferma_necessaria, regolarizzabile-without-cost-…}` from `buyer_checklist` + uncertain risk cards + regularizable compliance, deduped by topic (§E); max 8 visible, each with action/why/what-perizia-says/canonical page/status/next-check/optional confirmation; other-section facts are LINKS not cards | `ChecklistSection` (rewrite of `CustomerChecklistSection`) |
| 6 | CONFORMITÀ E DOCUMENTI TECNICI | `conformita` | `compliance_section` grouped via `contract._area_token` classes (edilizia/catastale/urbanistica/corrispondenza/impianto_gas/impianto_elettrico/impianti/agibilita + APE via label match); statuses from `_CLASSIFICATION_LABELS` + "Non verificato o non dichiarato" for absent, "Conferma necessaria" when a confirmation is attached; excerpt chosen server-side (moves `findSupportExcerpt`) | `CustomerComplianceSection` (modified) |
| 7 | FORMALITÀ E CANCELLAZIONI | `formalita` | three sub-lists from `formalities_section` + `money_sections.procedure_cancelled_formalities`: `cancellate` (fixed sentence pair, amount collapsed with existing `amount_note`), `costi_cancellazione` (explicit buyer rows, "Già considerato nella catena di valore." when `included_in_valuation`), `da_verificare` (unclear treatment only); summary+detail dedup (§E) | `CustomerFormalitiesSection` (modified) |
| 8 | ALTRI ELEMENTI DA CONOSCERE | `altri` | server-side leftover computation (replaces `CustomerOtherFindings` JS logic): `risk_sections` items whose topic is not already a finding in sections 3–7 (Torino: stato manutentivo, soffitta non ispezionata; excluded: comparatives, counts, repeated money/occupancy/formalities) | `AltriElementiSection` (renamed/modified `CustomerOtherFindings`) |
| 9 | FONTI DECISIVE DALLA PERIZIA | `fonti` | `sources` (§J) — 5–8 primary + "Mostra tutte le fonti (N)" collapsed rest | `FontiDecisiveSection` (rewrite of `CustomerEvidence`; deletes `buildEvidencePreview` JS heuristic) |
| 10 | CONFERME FORNITE DALL'UTENTE | `conferme` | confirmations store joined read-time; wording "Confermato dall'utente sulla base della pagina N."; original perizia finding always shown alongside | `ConfermeUtenteSection` (new) |
| 11 | STATO DELLE VERIFICHE | `stato_verifiche` | `readiness` (§M); customer labels only; NO download button | `StatoVerificheSection` (new) |

`executive_summary`, `key_facts` and `risk_sections` remain in the sanitized payload for
backward compatibility (older frontend bundles) but the new frontend renders exclusively
from `decision_model`; the count-sentences of `_executive_summary` are not shown anywhere.

## E. Deduplication rules

All server-side, in `decision_model.py`, applied when building `findings`:

1. **Canonical key**: `(topic_token, lot_id, bene_id or "-", rounded_amount or "-", status-class)`
   where `topic_token` = `contract._area_token` for compliance/risk areas,
   `doc_signals.label_kind` for money labels, formality `type` for formalities.
2. **One canonical card**: the first source in priority order below wins; every later
   duplicate contributes only its pages (union) and a `links` entry.
   Priority of *source arrays* for the same key: `compliance_section` >
   `risk_sections` > `buyer_checklist` (kills the Torino ×3 duplication: the checklist
   row "Verificare/regolarizzare conformità | conformità edilizia…" becomes the
   `recommended_action` of the compliance finding, not a separate card).
3. **Money**: keep `customer_report._dedup_key` (label, amount) global guard; additionally
   a chain row and a buyer-cost row with the same key render once in the chain and once
   as the "Già considerato nel valore finale: non sommare nuovamente." echo — never as a
   third card in `verifiche`.
4. **Occupancy**: exactly one finding (section 4); exec-summary line, key_fact and
   checklist "Valutare rischio occupazione" rows fold into it (`recommended_action`
   from checklist detail; distinct checklist details become `next_checks[]` bullets).
5. **Formalities**: dedup key drops the free-text description; two rows of the same
   `(type, amount, cancelled, buyer)` collapse with descriptions joined as detail lines
   (fixes the Pistoia double "Pignoramento"); genuinely distinct amounts (Torino's two
   € 150.000 ipoteche: original + rinnovazione) stay as two detail lines under ONE
   "Ipoteca" card.
6. **Identity**: "Lotto" (case_identity.lotto) and `lot_structure.selected_lot` render
   once (identity value wins; selected lot shown only if it differs).
7. **Priority classes** (drives `severity` int and esito drivers, in order):
   1 final valuation & buyer impact, 2 occupancy/title/opponibility, 3 technical/legal
   action items, 4 explicit buyer costs, 5 uninspected/uncertain elements,
   6 explicit conformity confirmations, 7 context.

## F. Money-role presentation rules

1. **Canonical chain order** (presentation-only reorder in `decision_model.py`; amounts and
   semantic roles byte-for-byte unchanged; `contract._money_sections`, the stored
   `customer_report.json`, and the validator stay untouched). Generic **role-based** order,
   never sorted by amount:
   `market_value` → rows whose roles ∩ {deduction, regularization} → `current_state_value`
   → rows with role cancellation (or condominium-per-art.568-type deductions that the
   document nets AFTER the state value, detected by arithmetic fit) → `sale_value`.
   Deterministic arithmetic-fit pass: given the two anchor values present, a deduction row
   is placed in the segment whose subtraction it satisfies within `_MONEY_ABS_TOL`/
   `_MONEY_REL_TOL` (same tolerances as `contract._approx_equal`). This fixes
   Codogno (15% → segment 1, € 14.000 → segment 2) and Pistoia (55% → segment 1,
   20% → segment 2) without touching validator math.
   **Ambiguous rows are never force-inserted**: a deduction whose role is unknown AND that
   fits no segment within tolerance is NOT guessed into the chain — it moves to "Importi da
   chiarire (D)" (or, when confirmation-eligible per §K, carries a "Conferma necessaria").
   The chain therefore contains only rows with a determinate position.
1b. **Original order preserved for admin**: the decision model carries the untouched
   document-order chain as `numeri.catena_ordine_originale` (admin-only projection, surfaced
   in the Vista admin decision-model preview and the admin `decision-model` route) so the
   reorder is fully traceable; the customer surface shows only the canonical order.
   The customer chain and the alternative scenarios (C) render as separate chains — an
   ambiguous/alternative row is never merged into the primary chain to invent one order.
2. **Terminal row**: when `sale_value` is absent but `auction_terms.prezzo_base` equals
   `state − remaining deductions` (Pistoia lot 3), the chain does NOT fabricate a sale
   row; it ends at the last grounded value and the prezzo base renders in its own
   auction block with the note it carries. No invented roles, ever.
3. **Buyer costs (B)**: only rows from `buyer_side_costs`; `included_in_valuation` rows
   show "Già considerato nel valore finale: non sommare nuovamente." (existing note text
   normalized to this single sentence) and are styled slate/informational, not amber.
4. **Scenari alternativi (C)**: rows with `source == "shared_summary_projection"` or a
   second internally-consistent deduction path render under "Scenari alternativi indicati
   dalla perizia" as parallel readings, never as warnings.
5. **Importi da chiarire (D)**: exactly `money_sections.uncertain_money`. The renderer
   already routes rendita/canone/spese condominiali/formality capital to `context_values`
   and OMI/borsino/annunci to `market_comparatives` (`doc_signals.COMPARATIVE_LABEL_KINDS`,
   `CONTEXT_LABEL_KINDS`); the decision model additionally asserts (defense in depth)
   that no row whose `label_kind` is comparative/context ever lands in `da_chiarire`.
6. **Comparatives**: one collapsed line "La perizia riporta N riferimenti di mercato
   (pagine …)" from the FULL report's `market_comparatives` (count + pages only);
   full rows remain admin/appendix (unchanged `customer_view` stripping).
7. **Mortgage amounts**: never in any cost bucket unless `buyer_burden` is true on the
   formality (existing `_formalities_section` contract honored); registered amounts stay
   collapsed inside the formality card detail with the existing `amount_note`.
8. **Amount emphasis**: only chain terminal value + explicit buyer costs use the gold
   numeric treatment; formality capitals, context values and scenario rows use slate.

## G. Occupancy presentation rules

Card structure: STATO / PERCHÉ CONTA / COSA VERIFICARE / PAGINE.

1. `stato`: `occupancy_section.status_label` verbatim ("Occupato", "Libero",
   "Occupato dal debitore e dal coniuge" pass-through).
2. `perche_conta`: fixed template selected by status class (the three sentences currently
   in `occupancyMeaning` in `CustomerReportView.js` move verbatim into the backend table).
3. `cosa_verificare`: derived from `title_info`/`opponibility`/`risks` — each risk becomes
   one bullet; when `opponibility` is null/empty AND status is occupied, emit exactly
   "Opponibilità del titolo da verificare: la perizia non si esprime espressamente."
   Never synthesize an opponibility conclusion (Codogno's explicit sentence — registration
   after pignoramento notification — passes through verbatim as document text).
4. `pagine`: canonical page (§J) + full list.
5. Lease facts (`registration_dates`, `expiry_dates`, rent from `context_values` label
   kind "canone") render as neutral blue context lines, never as costs or risks.

## H. Technical conformity rules

1. **Grouping** into fixed classes via `contract._area_token` on `compliance_section[].area`:
   Edilizia, Catastale, Urbanistica, Corrispondenza catastale/atto (token from label match
   "corrispondenza"), Impianti (gas/elettrico/general kept as separate cards inside the
   group), APE/energia (label match "ape"/"energetic"), Altro.
2. **Statuses** (closed map, from `classification`):
   `conforming` → "Conforme secondo la perizia" (green);
   `regularizable` → "Regolarizzabile secondo la perizia" (amber);
   `non_conforming`/`not_regularizable` → "Non conforme secondo la perizia" (amber card,
   red reserved for fail-closed reports);
   `uncertain`/absent → "Non verificato o non dichiarato" (slate);
   confirmation attached → "Conferma necessaria".
3. **Card fields**: Stato, "Cosa dice la perizia" (existing `notes`, clamped server-side to
   the first sentence(s) ≤260 chars — moves `shortExcerpt` clamping server-side),
   Costo (`cost_display` only when explicit), Tempistica (`timing` only when explicit),
   "Perché conta" + "Cosa fare" from fixed per-class templates, Pagina + one verbatim
   excerpt when a `customer_evidence_index` entry with `coverage_status == "covered"`
   exists on one of the card's pages with a matching topic (server-side port of
   `findSupportExcerpt`).
4. **Never invert**: "NESSUNA DIFFORMITÀ" (Torino corrispondenza card) stays green;
   missing info stays "Non verificato", never "Non conforme"; missing certification
   (Torino "certificazione impianti in generale") renders as a verification action,
   never as a safety warning.

## I. Formality presentation rules

1. **(A) Formalità cancellate dalla procedura**: one card per formality type-group with
   the two fixed sentences: "Formalità indicata come cancellata a cura della procedura."
   and "L'importo iscritto non è un debito da sommare al prezzo, salvo diversa indicazione
   espressa nella perizia." Detail rows (dates, notary, amounts) collapsed by default;
   amounts slate, never gold/red.
2. **(B) Costi di cancellazione a carico dell'acquirente**: only explicit rows
   (`buyer_burden == true` formalities, or the buyer-side cancellation cost row —
   Torino € 294,00); when `included_in_valuation`, append "Già considerato nella catena
   di valore."
3. **(C) Formalità da verificare**: only rows that are neither `cancelled_by_procedure`
   nor `buyer_burden` (today's status "Formalità rilevata; verificare le condizioni di
   cancellazione").
4. **Dedup**: §E rule 5 (summary reference rows from
   `money_sections.procedure_cancelled_formalities` and detail rows from
   `formalities_section` merge into one card per type: the Torino "Ipoteca: cancellazione
   a cura della procedura" reference + two € 150.000 detail rows = one Ipoteca card with
   two detail lines).
5. Section renders GREEN/blue when everything is cancelled by the procedure and no buyer
   cost exists; it is never amber merely because formalities exist.

## J. Evidence selection rules (Fonti decisive)

Input: `customer_evidence_index` (already verbatim-gated by `_find_verbatim_excerpt`:
topic-anchored, redaction-preserving, number-only overlaps rejected). The decision model
RANKS and PRUNES; it never re-extracts and never rewrites excerpt text.

1. **Priority order** (first match per topic wins, one source per topic):
   1 property identity, 2 occupancy/title, 3 valuation chain (canonical chain page),
   4 explicit buyer costs, 5 key conformity items, 6 formalities/cancellations,
   7 top checklist item. Ranking key: (priority, has excerpt, page).
2. **Canonical page per topic**: the page where the excerpt was actually found
   (`entry.page`, already unique per (page, topic)); for money rows with smeared page
   lists, the canonical page is the excerpt's page — the full list stays available under
   the finding, not the source card. (Torino: identity p.2/8, chain p.18, final+
   cancellation p.19 — matching the regression expectation — because those are the pages
   `_build_evidence_views` finds the topic-anchored excerpts on.)
3. **Pruning**: sources whose `report_section == "Superfici e dati catastali"` are
   excluded from the primary list (they collapse into ONE "Dati catastali (pagine …)"
   source at priority 7 max) — removes 11–31 micro-entries per case.
4. **Card format**: "p. X — {title}" + "Cosa dice la perizia:" excerpt + "Perché conta:"
   fixed line per priority class.
5. **Missing excerpt**: entries with `coverage_status == "excerpt_missing"` never enter
   the primary list; when such a topic is priority ≤ 4, the card shows page + topic +
   "Estratto da verificare" and links the matching checklist finding. Never fabricated.
6. **Cap**: 8 primary; the rest behind "Mostra tutte le fonti (N)" (collapsed component
   state, no extra fetch).

## K. Confirmation eligibility rules

A finding gets `confirmation.eligible = true` only when ALL hold:

1. The finding's status is `da_verificare` (or an uncertain money row with a `reason`),
   report_status is REPORT_READY, and the item does not block via validator (a
   deterministic validator failure is NEVER confirmable — those reports are fail-closed
   and never reach the customer anyway).
2. A canonical page exists AND a verbatim excerpt exists for the finding's topic
   (no excerpt ⇒ no form; show "Verifica pagina X con un professionista" instead).
3. A deterministic option set of 2–4 choices exists for the finding type:
   * **money-role** (uncertain_money rows): options from `doc_signals.ROLE_LABELS_IT`
     for the roles the label kind supports (reuses `money_confirmation._role_label`
     pattern), e.g. "È un costo che dovrò sostenere io" / "È già compreso nei valori
     indicati" / "È solo un dato informativo";
   * **occupancy**: "L'immobile è occupato con contratto opponibile" / "…con contratto da
     verificare" / "L'immobile è libero";
   * **conformity uncertain**: "La documentazione indicata è disponibile" / "Non è
     disponibile: servirà una verifica tecnica";
   * **formality unclear treatment**: "La cancellazione è a cura della procedura" /
     "Il costo resta a mio carico" / "Non è indicato".
   Every set is closed and defined in a fixed table keyed by (section, topic class);
   "Non sono sicuro" is always appended.
4. Global cap: at most 5 eligible confirmations per report (beyond that, lower-priority
   findings show the professional-check line instead of a form). Never a global
   questionnaire; each panel is one question + page + excerpt + options + cancel/confirm.

## L. Confirmation storage and audit

**Storage location (DECISION #2): MongoDB is the authoritative store.** Confirmations are
mutable, ownership-scoped, audit-tracked user actions — not artifacts — so they live in the
app's existing Motor/async Mongo database (`server.db`), reached the same way every other
`correctness_v2/api.py` route reaches it (lazy `import server; server.db`). They are NEVER
written into `customer_report.json` or any artifact; React state is presentation-only and
never a source of truth.

Two additive collections (no migration of existing collections; no schema change to
`perizia_analyses`/`users`/billing):

`correctness_v2_confirmations` — **current state**, one document per confirmed finding:

```jsonc
{
  "confirmation_id": "cnf_<uuid4hex>",
  "analysis_id": "analysis_…",
  "lot_id": "1",                          // selected_lot_id; "-" when single-lot
  "finding_id": "mon-…",
  "report_version": "cv2.customer_report.v1",     // report/pipeline version of the source
  "decision_version": "cv2.customer_decision.v1",
  "job_id": "cv2_…",
  "user_id": "…",                         // authenticated owner who submitted
  "selected_option": "gia_compreso",      // normalized option_id, or "non_sicuro"
  "selected_label": "È già compreso nei valori indicati",  // customer-facing label
  "page": 12,
  "evidence_hash": "sha1(normalized excerpt)",   // detects stale confirmations after rerun
  "status": "confermato_utente",          // current status: confermato_utente | non_sicuro
  "note": "…optional, ≤500 chars, sanitized…",
  "source": "USER_CONFIRMED",             // constant; NEVER merged into perizia facts
  "stale": false,                          // set true when evidence_hash no longer matches
  "created_at": "…iso…", "updated_at": "…iso…"
}
```

**Uniqueness / indexes** (created idempotently at first write inside `user_confirmations.py`
via `create_index(..., background=True)` — no `server.py` startup change required):
* **unique** compound index on `(analysis_id, lot_id, finding_id, report_version, user_id)`
  — the identity tuple the human mandated; one live confirmation per (owner, finding,
  report version). Submitting again on the same tuple is an **update** (upsert), never a
  second row.
* non-unique index on `(analysis_id, user_id)` for the owner list query.

`correctness_v2_confirmation_audit` — **append-only history**, never updated/deleted:

```jsonc
{
  "audit_id": "aud_<uuid4hex>", "confirmation_id": "cnf_…",
  "analysis_id": "…", "lot_id": "1", "finding_id": "…", "user_id": "…",
  "report_version": "…",
  "action": "created" | "updated" | "cleared",
  "from_option": null | "…", "to_option": "…",
  "from_status": null | "…", "to_status": "…",
  "at": "…iso…"
}
```

Every create/update/clear inserts one audit document **before/with** the state upsert, so the
previous answer is never destroyed (history is reconstructable). The state collection holds
the latest answer; the audit collection holds the full trail. All writes are `await`ed Motor
ops; there is no file IO and no artifact mutation. Zero OpenAI, zero jobs, zero credits on
every confirmation write.

**Stale handling on rerun** (the human's requirement #12): when a report is regenerated and
the finding/evidence identity changes, the stored `evidence_hash` (and, for a changed fact,
the recomputed `finding_id`) no longer matches the freshly built decision model. Such a
confirmation is marked `stale: true` at read time and rendered as "Conferma precedente da
rivedere"; it is **not** silently applied to the new report and never auto-resolves a
finding. `finding_id` includes `report_version`-affecting components only through the
artifact’s own fields, so a genuine fact change yields a new id and the old confirmation
simply no longer joins.

**Routes** (in `api.py`, mirroring the confirm-money pattern):

* `POST /{analysis_id}/correctness-v2/customer-view/confirm-finding`
  body `{job_id, finding_id, option_id, note?}`. Guards: `_resolve_customer_access`
  (owner or admin); job must belong to the analysis (`artifacts.read_job_status`
  cross-check, same as confirm-money — a confirmation can never target another
  analysis/lot); `finding_id` must exist in the decision model rebuilt server-side for
  that job's report AND be eligible; `option_id` must be one of the offered options or
  `non_sicuro` (mirror of `money_confirmation.validate_answers` — unoffered answers are
  rejected with a customer-safe ValueError → 400 INVALID_CONFIRMATION).
  On success the handler `await user_confirmations.submit(...)` performs a Mongo upsert on
  the unique identity tuple **plus** an append to the audit collection (never a second live
  row, never a destroyed prior answer). Only the analysis **owner** may create or change a
  confirmation (admin inspects, never authors). Response: the refreshed sanitized report
  (decision model re-derived with the updated confirmation set joined). Zero OpenAI, zero
  jobs, zero credits.
* `GET /{analysis_id}/correctness-v2/customer-view/confirmations` — owner view (their
  own confirmations from `correctness_v2_confirmations`, customer projection only).
* `GET /{analysis_id}/correctness-v2/jobs/{job_id}/decision-model` — **admin-only**
  (`_resolve_user_and_guard`): raw decision model + confirmations + original-finding vs
  user-confirmation diff + readiness + evidence identity, for Vista admin. Technical
  inspection stays behind the exact-email gate exactly as today
  (`CORRECTNESS_V2_ADMIN_VIEW_EMAIL` = nexodifyforyou@gmail.com;
  `server._user_is_admin` / `user_response["correctness_v2_admin_view"]`).

**Effect of a confirmation** (all read-time, in `decision_model.py`):
resolves the ambiguity presentation (finding status → `confermato_utente`, wording
"Confermato dall'utente sulla base della pagina N."); re-buckets a money item into the
confirmed subsection; updates checklist + readiness. It must NOT and cannot: weaken a
validator failure (fail-closed reports never render findings), convert
CONTRACT_VALIDATION_FAILED into READY (report_status is read-only input), alter the
worksheet/report artifacts (store is a separate file), remove contradictory evidence
(original finding + evidence always render alongside), call OpenAI, or touch billing.
If `evidence_hash` no longer matches the current artifact's excerpt (rerun changed the
fact), the confirmation is shown as "Conferma precedente da rivedere" and does not apply.
For critical conflicts (user confirms against an explicit document statement — detectable
only as: confirmation on a finding whose status was NOT `da_verificare` — the API refuses;
plus any `non_sicuro` answer) the finding keeps/gets "Verifica tecnica richiesta" and the
stored confirmation is visible in Vista admin.

**"Confermato dall'utente" vs perizia**: findings confirmed by the user always carry the
`source: USER_CONFIRMED` marker and render in section 10 + as a status chip — never inside
"Cosa dice la perizia" text, never as "La perizia conferma".

## M. Report readiness rules

Computed in `decision_model.py`; internal enum + fixed label map:

| Internal state | Condition (evaluated in order) | Customer label |
|---|---|---|
| `TECHNICAL_REVIEW_REQUIRED` | report_status not REPORT_READY-family, or any finding forced to "Verifica tecnica richiesta" (incl. stale/critical-conflict confirmations, `non_sicuro` answers on priority ≤ 3 findings) | "Verifica tecnica richiesta" |
| `CONFIRMATIONS_REQUIRED` | ≥1 eligible confirmation unanswered | "Conferme necessarie" |
| `READY_FOR_REVIEW` | no open confirmations, ≥1 open professional check | "Verifiche completate" (with open-checks count) |
| `COMPLETE_FOR_EXPORT` | no open confirmations and no open professional checks | "Pronto per l'esportazione" |

The raw enum never reaches the customer DOM (label only; enum stays in the JSON payload
for the admin preview and the future `feature-v2-report-export` branch, which will consume
`readiness.state == "COMPLETE_FOR_EXPORT"`). No download button in this branch.
Section 11 shows: confirmations done/total, mandatory open confirmations, professional
checks open, readiness label. Fail-closed paths are untouched: `is_customer_safe`,
`CUSTOMER_SAFE_STATUSES`, `_public_unavailable_reason` keep their exact behavior.

## N. Backward compatibility

1. **Read-time only**: `sanitize_customer_report(report, job)` gains
   `decision_model = build_decision_model(report, confirmations)` computed from the FULL
   stored dict before admin-key stripping. Existing artifacts (all six audited jobs)
   render through it with zero regeneration; old artifacts are NEVER rewritten.
2. **Missing fields ⇒ safe defaults**: absent `customer_evidence_index` (pre-3B jobs) ⇒
   sources list empty + "Estratto da verificare" on priority topics; absent
   `formalities_section` ⇒ formality section built from money reference rows alone (the
   existing `CustomerFormalitiesSection` fallback moves server-side); absent
   `beni_sections` ⇒ identity card only. Nothing fabricated.
3. **Old payload keys kept**: every key `sanitize_customer_report` emits today continues
   to be emitted unchanged (additive change), so a stale frontend bundle keeps working
   during deploy overlap.
4. **LOT_SELECTION_REQUIRED / MONEY_CONFIRMATION_REQUIRED / DOCUMENT_NOT_READABLE**:
   unchanged flows; the decision model attaches only `esito` + `readiness` for these
   statuses (no findings), so the selector/prompt/upload screens render exactly as today.
5. No change to Storico reuse (`workspace.find_lot_safe_report`, generate route rules
   A–D), no change to job spawning, no change to `_find_customer_job` selection.

## O. Frontend components

All under `frontend/src/components/correctness-v2/`. The customer surface renders ONLY
from `report.decision_model` when present (it always is, server-side), with the current
rendering kept as the fallback path for safety during rollout.

Modified:
* `CustomerReportView.js` — new body order per §D; delete client-side inference helpers
  (`occupancyMeaning`, `dedupedExtraFacts`, `findSupportExcerpt`, `buildEvidencePreview`,
  `CustomerOtherFindings` leftover logic; `shortExcerpt` retained only as a display clamp);
  `EsitoOperativoCard` replaces `CustomerDecisionBox` mapping `esito.level`
  verde/ambra/rosso to the existing tone system (green card allowed for the first time:
  headline "Nessuna verifica bloccante emersa dalla perizia" + the §D.1 supporting
  sentence; the card renders only backend-provided `headline`/`sentence`/`drivers`, no
  client-side wording); section components consume decision-model props.
  Prop contracts (all plain data, no fetching):
  * `EsitoOperativoCard({esito, readiness, onJumpToSection})`
  * `NumeriPrincipali({numeri})` — `{catena[], costi_potenziali[], scenari[], da_chiarire[], comparatives_summary}`
  * `ChecklistSection({verifiche, onOpenConfirmation})`
  * `FontiDecisiveSection({fonti})` — `{primary[], all_count}`
  * `ConfermeUtenteSection({conferme})`, `StatoVerificheSection({stato_verifiche})`
* `shared.js` — add `StatusChip` (closed status→tone map: verde/conferma/blu/oro/slate;
  red only for fail-closed) and `SectionAnchor` helpers.
* `useCustomerView.js` — add `submitFindingConfirmation(findingId, optionId, note)`
  mirroring `submitMoneyConfirmation` (POST confirm-finding, swap refreshed report in;
  no polling, no job).
* `CorrectnessV2Panel.js` — Vista admin ADDITIVE block only: "Decision model (anteprima)"
  + confirmations list + original-vs-confirmed diff + readiness state + evidence identity,
  fed by the new admin route. No redesign of existing panels.
* `CorrectnessV2Tabs.js` — unchanged except passing the confirmation handler through
  (likely zero changes; listed for verification).

New:
* `ConfirmationDialog.js` — focused panel: one question, page + verbatim excerpt,
  2–4 radio options + "Non sono sicuro", cancel/confirm; props
  `{finding, submitting, error, onSubmit, onClose}`. Reuses the interaction pattern of
  `CustomerMoneyConfirmation` (radio + gold submit) without the multi-ambiguity list.

Untouched: `LotWorkspace.js`, `useLotWorkspace.js`, `visibility.js` (workspace states and
visibility rules are out of scope).

Style: keep the dark premium tone system (`BADGE_TONES`, gold accents); color discipline
per rule 9 (green explicit conformity, amber action required, blue neutral, gold primary
values, slate supporting, red only fail-closed); no monospace ids in customer DOM; all
headings Italian.

## P. Backend modules

Modified:
* `customer_view.py` — `sanitize_customer_report(report, job, confirmations=())` gains an
  optional confirmations arg (default empty tuple, so every existing caller and every legacy
  render still works) and attaches `decision_model = build_decision_model(report,
  confirmations)`; `derive_decision` delegates its headline/level to the model's `esito` for
  REPORT_READY (kept as-is for the other statuses); everything else unchanged.
* `api.py` — three routes of §L; the customer-view route additionally `await`s
  `user_confirmations.list_for_analysis` and threads the result into
  `sanitize_customer_report`; no changes to existing routes' behavior.

New (justified):
* `decision_model.py` — the pure builder: `build_decision_model(report, confirmations,
  *, now=None) -> dict`, plus the fixed Italian string tables (status labels,
  buyer_impact / recommended_action / why templates, confirmation option tables) and the
  finding/dedup/ordering/evidence-ranking logic of §§D–K. Justification: this is a new
  read-time projection layer distinct from generation-time rendering
  (`customer_report.py`) and from sanitization (`customer_view.py`); folding ~600 lines
  into either would blur their single responsibilities and their test suites.
* `user_confirmations.py` — **async Mongo** store operations against `server.db`
  (`correctness_v2_confirmations` + `correctness_v2_confirmation_audit`): `async submit(
  analysis_id, lot_id, finding, option_id, user_id, note)` (validate option against the
  offered set, compute `evidence_hash`, upsert current-state doc on the unique tuple, insert
  audit doc), `async list_for_analysis(analysis_id, user_id)`, `async ensure_indexes()`
  (idempotent, called on first write). Reaches the db via lazy `import server` exactly like
  `api.py`; option validation mirrors `money_confirmation.validate_answers` semantics.
  Justification: isolates the only stateful/mutating (Mongo) surface of the feature so the
  builder and the routes stay thin, and so tests can inject a fake async collection.

Explicitly NOT touched: `validator.py`, `contract.py` (chain reorder is presentation-side
in `decision_model.py` — the contract artifact and validator math stay byte-identical),
`orchestrator.py`, `analyst.py`, `lots.py`, `lot_packets.py`, `lot_runner.py`,
`coverage_audit.py`, `quality_report.py`, `quality_gate.py`, `doc_signals.py` (read-only
reuse of `label_kind`, `ROLE_LABELS_IT`, kind sets), `workspace.py`, `feature_flags.py`,
`openai_client.py`, `pdf_quality.py`, `money_confirmation.py` (pattern reused, module
unchanged), `customer_report.py` (generation stays identical so serial==parallel and
Storico reuse equalities hold; ALL new behavior is read-time).

## Q. Exact files expected to change

Backend:
```
backend/correctness_v2/decision_model.py          (new)
backend/correctness_v2/user_confirmations.py      (new)
backend/correctness_v2/customer_view.py           (modified)
backend/correctness_v2/api.py                     (modified)
backend/correctness_v2/tests/test_decision_model.py        (new)
backend/correctness_v2/tests/test_user_confirmations.py    (new)
backend/correctness_v2/tests/test_customer_view.py         (modified)
backend/correctness_v2/tests/test_api_customer_view.py     (modified)
```
Frontend:
```
frontend/src/components/correctness-v2/CustomerReportView.js        (modified)
frontend/src/components/correctness-v2/ConfirmationDialog.js        (new)
frontend/src/components/correctness-v2/shared.js                    (modified)
frontend/src/components/correctness-v2/useCustomerView.js           (modified)
frontend/src/components/correctness-v2/CorrectnessV2Panel.js        (modified, additive)
frontend/src/components/correctness-v2/CustomerReportView.test.js   (modified)
frontend/src/components/correctness-v2/ConfirmationDialog.test.js   (new)
frontend/src/components/correctness-v2/useCustomerView.test.js      (new — hook currently untested directly)
frontend/src/components/correctness-v2/CorrectnessV2Panel.test.js   (modified)
frontend/src/lib/api/perizia.js                                     (modified: 3 client fns)
docs/customer_report_decision_workflow_plan.md                      (this file)
```

**Files touched outside customer-report/evidence/confirmation modules** (scope check):
* `frontend/src/lib/api/perizia.js` — 3 additive client functions for the new routes;
  no existing function changes. Justified: it is the single API-client module.
* **MongoDB**: two new *additive* collections (`correctness_v2_confirmations`,
  `correctness_v2_confirmation_audit`) with indexes created idempotently by
  `user_confirmations.ensure_indexes()` on first write. No migration of, or change to, any
  existing collection (`perizia_analyses`, `users`, `credit_ledger`, `billing_records`,
  `payment_transactions`, …). No change to `.env` (reuses `MONGO_URL`/`DB_NAME`).
* Nothing else. In particular NO changes to `server.py` (routes stay in `api.py`; the db is
  reached via the existing lazy `import server` pattern, and indexes are created lazily so
  no startup hook is added), `.env`, Stripe/billing modules, beta/entitlement code,
  `orchestrator.py`, `workspace.py`, `artifacts.py`, or any extraction/validation module.
  `pages/AnalysisResult.js` is expected to need zero changes (it passes `customerState`
  through); if a prop must be threaded, that one-line change must be called out in the PR.

## R. Tests

Backend — 32 tests (pytest, deterministic fixtures in `tests/sample_perizia.py` style;
six-case regression uses stored artifacts as fixtures copied into test data, never
hardcoded assertions against production paths):

`test_decision_model.py` (20):
1. schema envelope: version, section keys omitted when empty, no fabricated sections
2. finding_id stability: same artifact ⇒ same ids across two builds
3. esito verde when no open verifications; 4. esito ambra with open confirmations;
5. esito rosso never emitted for REPORT_READY (fail-closed only)
6. esito drivers ≤5, no counts, no internal codes/enums in any customer string
7. chain canonical reorder by arithmetic fit (Codogno-shaped fixture)
8. chain with missing terminal (Pistoia-shaped): no fabricated sale row
9. buyer cost included_in_valuation ⇒ "non sommare nuovamente" note, no third card
10. comparatives ⇒ single {count, pages} summary; none in da_chiarire (defense assert)
11. context kinds (rendita/canone/spese/capitale) never in da_chiarire
12. mortgage amount never a buyer cost without buyer_burden
13. compliance grouping + status map incl. "Non verificato o non dichiarato";
    "nessuna difformità" stays green
14. formality three-way split + summary/detail dedup (double-Pignoramento fixture);
    two distinct €150k ipoteche preserved as detail lines of one card
15. occupancy card: perche_conta template, "Opponibilità da verificare" when silent,
    explicit opponibility text passes through verbatim
16. dedup: compliance>risk>checklist single canonical card; occupancy folded once
17. sources: priority ranking, surfaces pruned/collapsed, cap 8, excerpt_missing never
    primary, excerpts verbatim from customer_evidence_index (string identity)
18. confirmation eligibility: excerpt required, option tables closed, cap 5,
    professional-check fallback line
19. readiness state machine all four states + label map; enum absent from any *_label
20. sanitizer safety: decision model output contains none of the forbidden tokens
    (LOW_CONFIDENCE, USER_PROVIDED, MONEY_ROLE_CONFLICT, MANUAL_REVIEW, confidence,
    manual_review, raw classification tokens in labels, artifact paths, provider names)

`test_user_confirmations.py` (6) — run against an injected in-memory async Mongo fake
(fake `server.db` with `correctness_v2_confirmations`/`correctness_v2_confirmation_audit`
collections supporting `update_one(upsert=True)`, `insert_one`, `find`; asserts index spec):
21. submit persists full state doc (all §L fields incl. `status`, `report_version`,
    `selected_label`) + inserts one audit doc; upsert on the unique tuple, not a second row
22. re-submit on the same identity tuple UPDATES the live doc (still one row), appends a
    second audit doc, preserves `created_at`, never rewrites/deletes prior audit history
23. unoffered option rejected; finding not eligible rejected; cross-analysis/cross-lot
    rejected; non-owner rejected (owner-only authoring)
24. evidence_hash mismatch after a rerun ⇒ confirmation flagged `stale`, not applied,
    rendered as "Conferma precedente da rivedere"
25. non_sicuro ⇒ finding to "Verifica tecnica richiesta", stored, visible to admin path
26. confirmation applied ⇒ money row re-bucketed + readiness updated; original finding
    and its evidence still present (never overwritten); no artifact file mutated

`test_customer_view.py` additions (3):
27. sanitize attaches decision_model for all six regression artifacts without error;
    old keys unchanged (snapshot of key set)
28. LOT_SELECTION/MONEY_CONFIRMATION/NOT_READABLE payloads unchanged except esito/readiness
29. defense-in-depth: _ADMIN_ONLY_KEYS still absent with decision model attached

`test_api_customer_view.py` additions (3):
30. POST confirm-finding: owner ok, non-owner 404, admin ok; response carries refreshed
    report; zero jobs spawned (artifacts dir job count unchanged); zero OpenAI
    (openai_caller sentinel never invoked)
31. GET confirmations owner-only projection; GET decision-model admin-only (403 non-admin)
32. six-case regression fold: for each stored fixture, customer view renders, chain
    values match the documented regression numbers (Torino 43.654,20→38.110,20 with
    €294 included; Codogno 6 lots + lot-1 isolation; Pistoia lot-3 isolation;
    1859886_C 4 beni; Orecchiazzzi/Cairate remain VERIFICATION_REQUIRED with zero findings)

Frontend — 29 tests (jest/RTL):

`CustomerReportView.test.js` (14): 1 esito verde/ambra render + no counts; 2 section
order matches §D; 3 empty sections absent (no "0 beni"/"Nessun dato" cards); 4 identity
rendered once (no double Lotto); 5 chain render with final gold row only; 6 "non sommare
nuovamente" note; 7 scenari block as neutral; 8 da_chiarire only from payload bucket;
9 comparatives single line; 10 occupancy STATO/PERCHÉ/COSA/PAGINE; 11 conformità groups +
green nessuna-difformità; 12 formalità three groups + collapsed amounts; 13 fonti: max 8
primary + "Mostra tutte le fonti (N)"; 14 altri elementi excludes duplicated topics.
`ConfirmationDialog.test.js` (6): 15 renders question+page+excerpt; 16 options 2–4 +
"Non sono sicuro"; 17 confirm disabled until selection; 18 submit calls handler with
option_id; 19 error state; 20 cancel closes without submit.
`useCustomerView.test.js` (4): 21 submitFindingConfirmation posts and swaps report;
22 failure sets error, keeps report; 23 no polling/job side effects on confirm;
24 legacy payload without decision_model still renders (fallback path).
`CorrectnessV2Panel.test.js` (3): 25 admin preview block renders decision model;
26 original-vs-confirmed diff visible; 27 readiness state shown raw only in admin.
Static/sanitization (2): 28 customer DOM contains no English headings / internal enums
(scan rendered output for forbidden tokens across all fixtures); 29 status chips map:
red only for fail-closed fixture.

## S. Real-case acceptance criteria (measurable, from stored artifacts)

* **Torino**: chain renders exactly 43.654,20 − 5.250,00 = 38.404,20 − 294,00 = 38.110,20;
  € 294,00 marked "già considerato"; edilizia/catastale regolarizzabili (amber),
  urbanistica + corrispondenza conformi (green), gas/elettrico regolarizzabili;
  formality section: cancelled-by-procedure, mortgage amounts collapsed, ONE Ipoteca card
  (two € 150.000 detail lines); primary sources ≤8 and include p.8 (conformità),
  p.18 (chain), p.19 (final+cancellation); each fact appears in exactly one section
  (occupancy once, compliance areas once); no "N punti di attenzione" sentence anywhere;
  visible cards above the fold ≤ ~12 vs today's 40+.
* **Pistoia lot 3**: chain deductions ordered by arithmetic fit (55% before state value,
  20% after), no fabricated sale row; formality list shows no duplicate Pignoramento;
  zero cross-lot money/evidence (all pages within lot-3 packet pages).
* **1859886_C**: 4 beni preserved; "Oblazione indicativa art. 36 bis" € 1.032,00 is the
  single da_chiarire row AND carries an eligible confirmation with a money-role option
  set; surfaces collapsed out of fonti (≤8 primary from 51 entries).
* **Orecchiazzzi**: customer view still returns `available:false` /
  `VERIFICATION_REQUIRED`; decision model attaches nothing beyond esito; no findings,
  no confirmations possible.
* **Cairate** (DECISION #1 — accept fail-closed): the latest per-lot artifacts on `main`
  are `CONTRACT_VALIDATION_FAILED` (`MONEY_CHAIN_INCONSISTENT`) and MUST stay fail-closed.
  Acceptance criterion = the customer sees a clear, customer-safe explanation of the
  fail-closed state (esito `rosso` "Verifica tecnica richiesta", no findings), the
  `LOT_SELECTION_REQUIRED` selector renders unchanged, and the per-lot money-summary dedup
  (the two € 3.978,12 rows) collapses in the selector display. The report redesign must
  improve **how a failed case is explained**, not convert it into a passing report.
  The € 67.264,50 − 3.363,23 = 63.901,28 − 3.978,12 = 59.923,16 chain is **not** displayed
  as a validated customer report; it is retained **only** as an admin/regression reference
  fixture explicitly labelled "non validato su main" (used to prove the reorder/dedup logic
  in isolation, never asserted against a live customer render). No regeneration, no credits,
  no OpenAI in this branch. Part 23 (Cairate) is hereby updated to this production truth.
* **Codogno**: exactly 6 lots in the selector, no Lotto 00; lot-1 chain ordered
  452.494,00 − 67.874,10 = 384.619,90 − 14.000,00 = 370.619,90; € 14.000 echo styled as
  included (slate, "non sommare nuovamente"); explicit opponibility sentence passes
  through verbatim; prezzo base € 370.619,90 in its own block; ipoteca amounts collapsed.
* **All six**: rendering, opening evidence, and saving a confirmation perform zero OpenAI
  calls, spawn zero jobs, charge zero credits (asserted in tests via sentinel caller +
  job-dir count + no billing imports in new modules).

## T. Rollback

The feature is additive and read-time:
1. Frontend rollback: revert the frontend commit — old bundle renders the unchanged
   legacy keys (which the sanitizer still emits).
2. Backend rollback: revert `customer_view.py`/`api.py` changes — `decision_model`
   disappears from the payload; no artifact was ever modified, so nothing to migrate back.
3. Confirmations store: the two additive Mongo collections
   (`correctness_v2_confirmations`, `correctness_v2_confirmation_audit`) become inert data
   ignored by the reverted code — left in place (audit preserved) or archived. No existing
   collection was altered, so there is nothing to migrate back.
4. No `.env` change, no destructive DB migration (collections are additive-only), no Stripe,
   no systemd changes anywhere in this branch, so rollback is exactly `git revert` +
   frontend redeploy + backend restart.

## T2. Post-review corrections (2026-07-18, after human review + Fable code review)

Applied after the "approved in principle" review and Fable's correctness pass:

1. **"Non sono sicuro" severity (human gate 1)** — an unsure answer no longer escalates
   by itself. Severity now derives from `report_status` / a deterministic per-finding
   `blocking` flag (from `blocks_saleability` or a non-conforming classification) / status,
   never from the option string. Ordinary unsure → finding stays `da_verificare` with its
   confirmation still OPEN → readiness `CONFIRMATIONS_REQUIRED`, esito **amber**. Blocking →
   `verifica_tecnica_richiesta` → **red**. Fail-closed stays red with zero findings.
   Resolved→unsure reopens the confirmation.
2. **Interactive statuses are amber, not red (Fable B2)** — `MONEY_CONFIRMATION_REQUIRED` /
   `LOT_SELECTION_REQUIRED` / `DOCUMENT_NOT_READABLE` are customer-safe interactive prompts,
   not fail-closed; the decision model maps them to amber and the frontend renders the
   decision report ONLY for `REPORT_READY` (interactive states keep the informative V2
   customer body).
3. **`numeri.da_chiarire` carries chain-excluded ambiguous rows (Fable B1)** — uncertain
   money rows surface as confirmation-eligible findings; the presentation-only reorder's
   excluded deductions (e.g. Pistoia €56.068) now render in "Importi da chiarire" so no
   perizia amount is ever dropped. `_reorder_chain`'s all-failed branch is a pure passthrough
   (no row is both in the chain and flagged ambiguous — Fable B3).
4. **Fallback renamed (human gate 2)** — `CustomerReportLegacyBody` → `V2CustomerReportFallback`:
   the previous *sanitized Correctness V2 customer renderer* (not the legacy analysis report),
   used only for old artifacts lacking `decision_model`; no legacy DOM, no network, no job.
5. **Concurrency (Fable 8a)** — `user_confirmations.submit` retries a racing upsert as a plain
   update on `DuplicateKeyError`; the unique index guarantees exactly one active record.
6. **Real-Mongo isolated smoke** — full lifecycle (create/update/audit/non_sicuro/lot-isolation/
   stale/authorization/indexes/concurrency) verified against a real mongod temp DB, then dropped.

## U. Explicit deferred scope

* **feature-beta-program-admin**: beta tester admin, Programma Beta, Beta Feedback,
  beta entitlement, `BETA_UNLIMITED_EMAILS` — untouched by this branch.
* **feature-v2-report-export**: PDF/HTML export and any download button. This branch only
  produces the `readiness.state == "COMPLETE_FOR_EXPORT"` signal that branch will consume.
  A read-only confirmation snapshot may later be embedded into the export artifact, but the
  `correctness_v2_confirmations` Mongo collection remains the authoritative source of truth.
* Also unchanged here (other branches/owners): user roles, Stripe/packages/pricing/
  checkout/webhooks, credit formulas and usage metering, GDPR/admin user management,
  cybersecurity hardening, OpenAI model choice, extraction, lot segmentation, validator
  acceptance rules, Storico generation/reuse behavior, production concurrency, legacy
  frontend restoration.
