#!/usr/bin/env bash
set -euo pipefail

BASE_URL="https://api-periziascan.nexodify.com"
export BASE_URL
PERIZIA_PDF="/srv/perizia/app/uploads/1859886_c_perizia.pdf"
ESTRATTO_PDF="/srv/perizia/app/uploads/estratto_agency.pdf"

mkdir -p /tmp/estratto_compare

echo "[1] Upload perizia -> create NEW analysis"
code="$(curl -sS -o /tmp/estratto_compare/new_upload.body -D /tmp/estratto_compare/new_upload.h \
  -w "%{http_code}" -X POST \
  -H "Cookie: session_token=${SESSION_TOKEN}" \
  -F "file=@${PERIZIA_PDF}" \
  "${BASE_URL}/api/analysis/perizia")"
echo "HTTP=$code"
if [[ "$code" != "200" && "$code" != "201" ]]; then
  echo "Upload failed. Headers:"
  sed -n '1,30p' /tmp/estratto_compare/new_upload.h || true
  echo "Body (first 400 chars):"
  head -c 400 /tmp/estratto_compare/new_upload.body || true; echo
  exit 1
fi

NEW_AID="$(python3 - <<'PY'
import json
p="/tmp/estratto_compare/new_upload.body"
j=json.load(open(p))
aid = j.get("analysis_id") or j.get("id") or (j.get("analysis") or {}).get("analysis_id") or ""
print(aid)
PY
)"
echo "NEW_AID=$NEW_AID"
test -n "$NEW_AID" || (echo "Failed to extract NEW_AID. Body:"; head -c 600 /tmp/estratto_compare/new_upload.body; echo; exit 1)

echo "[2] Run regressions step2->step5"
cd /srv/perizia/app/backend
./.venv/bin/python scripts/regression_step2_contract.py --analysis-id "$NEW_AID"
./.venv/bin/python scripts/regression_step3_semiforo_and_fields.py --analysis-id "$NEW_AID"
./.venv/bin/python scripts/regression_step4_pdf_content.py --analysis-id "$NEW_AID"
./.venv/bin/python scripts/regression_step5_estratto_compare.py --analysis-id "$NEW_AID" --estratto-pdf "$ESTRATTO_PDF"

echo "ALL PASS for $NEW_AID"
