# Repo Audit — PeriziaScan

Date: 2026-02-12
Repo root: /srv/perizia/app

**Backend Map**
- FastAPI app, models, auth, perizia analysis pipeline, report download and history endpoints: `backend/server.py` (models at `backend/server.py:60`-`backend/server.py:138`, auth helpers/endpoints at `backend/server.py:175`-`backend/server.py:437`, perizia analysis endpoint at `backend/server.py:1944`-`backend/server.py:2085`, report download at `backend/server.py:2091`-`backend/server.py:2116`, report HTML generator at `backend/server.py:2118`-`backend/server.py:2265`, history endpoints at `backend/server.py:3575`-`backend/server.py:3602`).
- OCR extraction with Google Document AI, PDF splitting, tables + form fields extraction: `backend/document_ai.py` (configuration and extractor class at `backend/document_ai.py:16`-`backend/document_ai.py:208`).

**Request/Response Schemas (Key)**
- Perizia analysis storage schema: `PeriziaAnalysis` model includes `analysis_id`, `user_id`, `case_id`, `run_id`, `revision`, `case_title`, `file_name`, `input_sha256`, `pages_count`, `result`, `created_at` (`backend/server.py:105`-`backend/server.py:118`).
- `/api/analysis/perizia` response includes `analysis_id`, `case_id`, `run_id`, and full `result` payload (`backend/server.py:2079`-`backend/server.py:2085`).

**Perizia Pipeline (Upload → OCR → Analysis → Storage → Report Download)**
1. Frontend upload: `NewAnalysis` posts multipart form-data to `/api/analysis/perizia` (`frontend/src/pages/NewAnalysis.js:76`-`frontend/src/pages/NewAnalysis.js:118`).
2. Backend validation + OCR: `analyze_perizia` validates PDF, reads bytes, runs Google Document AI (`extract_pdf_with_google_docai`), and falls back to `pdfplumber` when needed (`backend/server.py:1944`-`backend/server.py:2049`).
3. LLM analysis: `analyze_perizia_with_llm(...)` is called with full text + per-page content (`backend/server.py:2055`-`backend/server.py:2056`).
4. Storage: result is stored in MongoDB `db.perizia_analyses` as `PeriziaAnalysis` and `raw_text` is saved (truncated) (`backend/server.py:2058`-`backend/server.py:2074`).
5. Report download: `GET /api/analysis/perizia/{analysis_id}/pdf` loads analysis result and generates HTML report via `generate_report_html(...)` (`backend/server.py:2091`-`backend/server.py:2118`).

**Frontend Pages / Components (Perizia Flow)**
- Upload page: `frontend/src/pages/NewAnalysis.js` (file validation + POST to `/api/analysis/perizia`) (`frontend/src/pages/NewAnalysis.js:53`-`frontend/src/pages/NewAnalysis.js:118`).
- Result page: `frontend/src/pages/AnalysisResult.js` (loads analysis via `/api/history/perizia/{analysis_id}`, download report via `/api/analysis/perizia/{analysis_id}/pdf`) (`frontend/src/pages/AnalysisResult.js:344`-`frontend/src/pages/AnalysisResult.js:385`).
- History page: `frontend/src/pages/History.js` (lists perizia history via `/api/history/perizia`) (`frontend/src/pages/History.js` around `:94`-`:100`).
- Routing and auth callback interception: `frontend/src/App.js` (session_id detection and protected routes) (`frontend/src/App.js:76`-`frontend/src/App.js:108`).
- Login/auth context: `frontend/src/context/AuthContext.js` (login redirect, session exchange, /api/auth/me) (`frontend/src/context/AuthContext.js:16`-`frontend/src/context/AuthContext.js:42`).

**"Download Report" JSON Location + Schema**
- There is no JSON report endpoint for the download itself; the download endpoint returns HTML (`text/html`) built by `generate_report_html(...)` (`backend/server.py:2091`-`backend/server.py:2116`).
- The JSON used for the report view is the analysis record returned by `GET /api/history/perizia/{analysis_id}` (`backend/server.py:3589`-`backend/server.py:3602`), which returns the stored `PeriziaAnalysis` document (see schema at `backend/server.py:105`-`backend/server.py:118`).
- The `result` JSON structure is defined by the NEXODIFY ROMA STANDARD prompt (12 sections) in `PERIZIA_SYSTEM_PROMPT` (`backend/server.py:596`-`backend/server.py:760`).
- Fallback result schema (used when LLM fails) includes `schema_version`, `run`, `lots`, `lots_count`, `is_multi_lot`, `case_header`, `report_header`, `lot_index`, `page_coverage_log`, `semaforo_generale`, `decision_rapida_client`, `money_box`, `section_9_legal_killers`, `dati_certi_del_lotto`, `abusi_edilizi_conformita`, `stato_occupativo`, `stato_conservativo`, `formalita`, `legal_killers_checklist`, `indice_di_convenienza`, `red_flags_operativi`, `checklist_pre_offerta`, `summary_for_client`, `qa_pass` (see `backend/server.py:1803`-`backend/server.py:1939`).
