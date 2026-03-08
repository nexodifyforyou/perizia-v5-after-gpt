# PERIZIA 1859886 — UI/Data Contract Audit (Audit-Only)

## Run Artifacts (fresh run, current working tree)
- RUN_DIR: `/srv/perizia/_qa/runs/2026-03-08_201919_ui_contract_1859886`
- analysis_id: `analysis_9ce1db1d7208`
- Backend JSON: `/srv/perizia/_qa/runs/2026-03-08_201919_ui_contract_1859886/system.pretty.json`
- Frontend snapshot: `/srv/perizia/_qa/runs/2026-03-08_201919_ui_contract_1859886/frontend_snapshot.json`
- Frontend render: `/srv/perizia/_qa/runs/2026-03-08_201919_ui_contract_1859886/frontend_render.png`
- Tab captures:
  - Panoramica: `/srv/perizia/_qa/runs/2026-03-08_201919_ui_contract_1859886/tab_panoramica.png`
  - Costi: `/srv/perizia/_qa/runs/2026-03-08_201919_ui_contract_1859886/tab_costi.png`
  - Legal Killers: `/srv/perizia/_qa/runs/2026-03-08_201919_ui_contract_1859886/tab_legal_killers.png`
  - Dettagli: `/srv/perizia/_qa/runs/2026-03-08_201919_ui_contract_1859886/tab_dettagli.png`
  - Red Flags: `/srv/perizia/_qa/runs/2026-03-08_201919_ui_contract_1859886/tab_red_flags.png`
- Tab text captures (for comparison):
  - `/srv/perizia/_qa/runs/2026-03-08_201919_ui_contract_1859886/tab_*_text.txt`
  - Note: all 5 tab text files are byte-identical (`sha256=0df1709fe2412f8a0564bd2c359b7dd836f9ee3552c8fffea34c4916b347ab0a`).

---

## Perizia-Backed Contract (Expected for this specific file)

## A) Procedure / Lot Level (single lot, multi-beni)
- Tribunale: `TRIBUNALE DI MANTOVA` (pag. 1).
- Procedura: `Esecuzione Immobiliare 62/2024 del R.G.E.` (pag. 1).
- Lot count: `1`.
- Lot label: `Lotto Unico` (pag. 45; sommario pag. 2/4).
- Lot composition: lotto unico composto da 4 beni (pag. 37, 42, 45-46).
- Prezzo base: `€ 391.849,00` (pag. 44-45).
- Total estimate (`Valore di stima`): `€ 419.849,00` (pag. 39).
- Final estimate (`Valore finale di stima`): `€ 391.849,00` (pag. 40).
- Valuation adjustments (pag. 40):
  - Oneri regolarizzazione urbanistica: `€ 23.000,00`
  - Rischio mancata garanzia: `€ 5.000,00`

## B) Bene Cards (must show all beni; no flattening)
- Bene 1
  - Numero: `1`
  - Tipo: `Ufficio`
  - Ubicazione/piano: `Via Sordello n. 5, piano Terra-primo`
  - Diritto reale: `Proprietà, quota 1/1`
  - Superficie convenzionale: `116,39 mq`
  - Valore stima bene: `€ 104.751,00`
  - Occupazione: `Occupato da debitore + coniuge`
  - Stato conservativo: piano terra buono; piano primo al grezzo
  - Catasto: `Fg 20, Part 433, Sub 301, Cat A10`
  - Urbanistica/agibilità: incongruenze rilevate; assenza abitabilità riportata nel blocco regolarità
  - APE/dichiarazioni: APE assente; dichiarazioni conformità impianti non presenti
- Bene 2
  - Numero: `2`
  - Tipo: `Garage`
  - Ubicazione/piano: `Via Sordello n. 5, piano Terra`
  - Diritto reale: `Proprietà, quota 1/1`
  - Superficie convenzionale: `38,50 mq`
  - Valore stima bene: `€ 34.650,00`
  - Occupazione: `Occupato da debitore + coniuge`
  - Stato conservativo: buono
  - Catasto: `Fg 20, Part 433, Sub 302, Cat C6`
  - Urbanistica/agibilità: incongruenze riportate; non presente abitabilità
  - APE/dichiarazioni: APE assente; dichiarazioni conformità non presenti
- Bene 3
  - Numero: `3`
  - Tipo: `Garage`
  - Ubicazione/piano: `Via Sordello n. 5, piano Seminterrato`
  - Diritto reale: `Proprietà, quota 1/1`
  - Superficie convenzionale: `35,22 mq`
  - Valore stima bene: `€ 49.308,00`
  - Occupazione: `Occupato da debitore + coniuge`
  - Stato conservativo: buono
  - Catasto: `Fg 20, Part 600, Sub 4, Cat C6`
  - Urbanistica/agibilità: immobile non regolare; non presente abitabilità
  - APE/dichiarazioni: APE assente; dichiarazioni conformità non presenti
- Bene 4
  - Numero: `4`
  - Tipo: `Villetta`
  - Ubicazione/piano: `Via Sordello n. 5, piano Piano terra rialzato-primo`
  - Diritto reale: `Proprietà, quota 1/1`
  - Superficie convenzionale: `165,10 mq`
  - Valore stima bene: `€ 231.140,00`
  - Occupazione: `Occupato da debitore + coniuge`
  - Stato conservativo: prevalente buono; area laterale in stato trascurato
  - Catasto: `Fg 20, Part 600, Sub 3, Cat A7`
  - Urbanistica/agibilità: difformità citata (ripostiglio); situazione da verificare per abitabilità
  - APE/dichiarazioni: APE assente; dichiarazioni conformità non presenti

## C) Legal Killers Candidates (material, evidence-backed only)
- Pignoramento trascritto (esecuzione in corso) (pag. 47-50).
- Ipoteca giudiziale (pag. 47-50).
- Ipoteca della riscossione (pag. 47-50).
- Difformità urbanistico-catastali con indicazione di incongruenze (pag. 34-36).
- Assenza documentale APE/conformità impianti (pag. 34-36).
- Stato di occupazione da debitore/coniuge (pag. 21-22, 45-46).

## D) Cost Buckets (contractual separation)
- Perizia valuation adjustments (non automaticamente “extra cash out”):
  - `Oneri regolarizzazione urbanistica € 23.000`
  - `Rischio mancata garanzia € 5.000`
- Explicit cost mentions from text:
  - `Completamento lavori € 15.000`
  - `Pratiche abitabilità € 5.000`
  - `Sanatoria (spese di massima) € 3.000`
- Nexodify estimates (must be labeled as assumptions):
  - voci market-estimate (`B,D,E,F,H,...`) separate and clearly marked as non-perizia.
- Unspecified / unverifiable:
  - spese condominiali arretrate (non provate in estrazione corrente)
  - dati asta (mancanti)

---

## Current Frontend vs Expected Contract

## Panoramica
- Should be there:
  - block procedura+lotto corretto
  - composizione lotto (4 beni) visibile
  - KPI lotto: valore di stima, deprezzamenti, valore finale, prezzo base
- Currently shown:
  - Case Summary + semaforo + pochi field_states
  - no explicit lot-composition card with all 4 beni
- Verdict: `PARTIAL`
- Mismatch fields/components:
  - missing explicit `lot_composition`
  - missing `valuation waterfall` (419.849 -> -28.000 -> 391.849)

## Costi
- Should be there:
  - separazione netta: `deprezzamenti perizia` vs `costi espliciti` vs `stime Nexodify`
  - no mixing with non-cost facts (e.g., prezzo base)
- Currently shown:
  - summary range + mixed buckets; includes derived/estimated lines and candidate artifacts
- Verdict: `CONFUSING`
- Mismatch fields/components:
  - bucket semantics non deterministic
  - “voci rilevate automaticamente ... non incluse” + range shown together
  - potential inclusion of non-cost candidate class in money pipeline

## Legal Killers
- Should be there:
  - ipoteche/pignoramento/difformità/occupazione con evidenze materiali (pag. 47-50, 34-36, 21-22)
- Currently shown:
  - tab content identical to Panoramica (no dedicated killer list rendered)
  - backend killer evidence in this run includes TOC-like lines (non material)
- Verdict: `WRONG`
- Mismatch fields/components:
  - missing legal-killer dedicated list/card
  - evidence quality mismatch (TOC snippets vs substantive formalità pages)

## Dettagli
- Should be there:
  - 4 bene cards complete (numero, tipologia, ubicazione, diritto, superficie, valore, occupazione, stato, catasto, urbanistica/agibilità, APE)
- Currently shown:
  - same content as Panoramica; no per-bene detailed rendering
- Verdict: `MISSING`
- Mismatch fields/components:
  - missing `bene_cards[]` rendering for all 4 beni

## Red Flags
- Should be there:
  - evidence-backed risks only, separated by confidence and impact
- Currently shown:
  - same content as other tabs; generic AMBER summary/checklist
- Verdict: `PARTIAL`
- Mismatch fields/components:
  - no tab-specific red-flag matrix
  - no confidence tiering per red flag

---

## Logical Leaps to Flag (product rule)
- Using TOC/index evidence as legal-killer proof (non-material, non-contextual).
- Risk of interpreting valuation/deprezzamento lines as guaranteed buyer-side extra cost without qualification.
- Potential lot-level vs bene-level blending in one summary block (occupazione/agibilità rendered as single global fact).
- Spese condominiali: any definitive state must require direct evidence in spese section; `incidenza condominiale 0,00%` is not proof of arrears absence.
- Tabs currently not differentiating semantic payload (all five tabs rendering equivalent text snapshot).

---

## Normalized Generic Data Model Proposal (scalable)
- `Document`
  - `document_id`, `source_file`, `pages_count`, `quality_metrics`
- `Procedure`
  - `tribunale`, `procedura_id`, `case_meta`
- `Lots[]`
  - `lot_id`, `lot_label`, `prezzo_base`, `valore_stima`, `valore_finale`, `adjustments[]`, `beni_ids[]`
- `Beni[]`
  - `bene_id`, `lot_id`, `bene_number`, `type`, `address`, `piano`, `diritto_reale`, `quota`, `superficie_convenzionale`, `valore_stima_bene`, `occupancy`, `stato_conservativo`, `catasto`, `urbanistica`, `agibilita`, `ape`, `dichiarazioni_impianti`
- `Findings[]`
  - `finding_id`, `scope` (`lot|bene|procedure`), `category` (`legal|cost|occupancy|doc_missing|technical`), `severity`, `confidence`, `materiality`, `statement`, `evidence[]`
- `CostItems[]`
  - `cost_id`, `bucket` (`perizia_adjustment|explicit_text|nexodify_estimate|unspecified`), `amount_min`, `amount_max`, `currency`, `assumption_flag`, `source_ref`
- `Evidence[]`
  - `page`, `quote`, `search_hint`, `source_hash`, `quality_flags`

Scalability by document shape:
- Single lot / single bene: `Lots[1]`, `Beni[1]`, simple mapping.
- Single lot / multiple beni: `Lots[1]`, `Beni[n]`, lot summary + per-bene cards mandatory.
- Multiple lotti / multiple beni: `Lots[n]` with strict lot/bene scoping; no cross-lot aggregation without explicit rule.

---

## Recommended Gate Rules

## Field Presence Gates
- For each active tab, require minimum field set:
  - Panoramica: `tribunale, procedura, lot_label, lot_count, prezzo_base`
  - Costi: at least one item per enabled bucket with bucket labels
  - Legal Killers: >=1 material finding OR explicit “none found” with search coverage evidence
  - Dettagli: one rendered card per bene in lot
  - Red Flags: findings list with severity/confidence

## Evidence Quality Gates
- Reject TOC/index-only evidence for legal/cost conclusions.
- Require quote-to-claim lexical alignment and section alignment.
- Enforce page evidence presence for all non-user-provided critical fields.

## No-Logical-Leap Gates
- Forbid deriving definitive states from proxy phrases (e.g., `incidenza condominiale` -> arrears status).
- Forbid converting valuation adjustments into guaranteed buyer cash-out without assumption label.
- Forbid collapsing bene-specific facts into lot-global claims without scope tag.

## Document-Shape Gates
- Detect shape (`single_lot_multi_beni` here) from schema + PDF evidence.
- If `beni_count > 1`, Dettagli must render all bene cards.
- If tab payload hashes are identical across all tabs, fail UI contract gate.

