# DEFECT LIST — PeriziaScan

Date: 2026-02-12
QA Run: OFFLINE_QA (analysis_id: analysis_a4423cfb545d)
Artifacts: `/tmp/perizia_qa_run/response.json`, `/tmp/perizia_qa_run/analysis.json`, `/tmp/perizia_qa_run/evidence_anchors.json`

**P0 — Evidence Missing (Non‑Negotiable Violation)**
- None observed in this fixture run. All critical fields populated (prezzo base, ubicazione, superficie, tribunale/procedure) now include evidence anchors, or are explicitly marked `LOW_CONFIDENCE — USER MUST VERIFY`.

**P0 — Invented Numeric Estimates (Non‑Negotiable Violation)**
- None observed. `summary_for_client` contains no numeric estimates; `money_box.total_extra_costs` and `indice_di_convenienza` estimate fields are set to `TBD` unless directly evidenced.

**P1 — Lotto Extraction Failure (Incorrect Summary / Multi‑lot Handling Risk)**
- Resolved. `result.lots_count = 1` with `Lotto Unico` evidence, and lot fields use schema‑anchored evidence.

**P1 — Evidence Anchors List Sparse**
- Resolved. Evidence anchors extracted: 19 total (`/tmp/perizia_qa_run/evidence_anchors.json`).

**Observations (Non‑blocking for this QA pass)**
- OFFLINE_QA bypasses DB persistence and stores analysis locally in `/tmp/perizia_qa_run/analysis.json`.
