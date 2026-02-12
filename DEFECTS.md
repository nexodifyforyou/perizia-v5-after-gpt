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

## P0 Incident: OFFLINE AUTH BYPASS VIA HEADER

Status: `PARTIALLY FIXED (Gate 1 + Gate 2 proven, Gate 3 requires token-enabled local run precondition)`

### Root cause
- `backend/server.py` previously bypassed auth when `X-OFFLINE-QA` header existed.

### Backend fix shipped
- Strict offline gating is now enforced by `is_offline_qa_request(request)` in `backend/server.py`.
- Offline fixture mode is allowed only when all checks pass:
  - `ALLOW_OFFLINE_QA=1`
  - `OFFLINE_QA_TOKEN` configured
  - `X-OFFLINE-QA: 1`
  - `X-OFFLINE-QA-TOKEN` matches
  - client IP is loopback only
- Rejected offline attempts are audited without token logging.

### Proof commands and outputs

Command (local unauth exploit):
`curl -sS -D /tmp/g1.h -o /tmp/g1.json -X POST http://127.0.0.1:8081/api/analysis/perizia -H "X-OFFLINE-QA: 1" -F "file=@/srv/perizia/app/perizia_test.pdf;type=application/pdf"`

Observed output:
`/tmp/g1.h`
```
HTTP/1.1 100 Continue

HTTP/1.1 401 Unauthorized
date: Thu, 12 Feb 2026 23:54:05 GMT
server: uvicorn
content-length: 30
content-type: application/json
```
`/tmp/g1.json`
```
{"detail":"Not authenticated"}
```

Command (public unauth exploit):
`curl -sS -D /tmp/g2.h -o /tmp/g2.json -X POST https://api-periziascan.nexodify.com/api/analysis/perizia -H "X-OFFLINE-QA: 1" -F "file=@/srv/perizia/app/perizia_test.pdf;type=application/pdf"`

Observed output:
`/tmp/g2.h`
```
HTTP/1.1 100 Continue

HTTP/1.1 401 Unauthorized
Server: nginx/1.18.0 (Ubuntu)
Date: Thu, 12 Feb 2026 23:54:05 GMT
Content-Type: application/json
Content-Length: 30
Connection: keep-alive
```
`/tmp/g2.json`
```
{"detail":"Not authenticated"}
```

### Gate script status
- Script added: `scripts/security_gate_tests.sh`
- Gate 1/2 logic passes with the outputs above.
- Gate 3 requires a localhost backend instance started with `ALLOW_OFFLINE_QA=1` and `OFFLINE_QA_TOKEN=devtoken`; in this VM tool session, spawning that temporary instance for in-script curl was not reproducible due runtime isolation. Gate 3 command is included in script and ready for direct VM execution.
