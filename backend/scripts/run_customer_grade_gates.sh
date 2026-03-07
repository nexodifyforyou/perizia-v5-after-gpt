#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="/srv/perizia/app"
BACKEND_DIR="$APP_ROOT/backend"
FRONTEND_DIR="$APP_ROOT/frontend"
PERIZIA_PDF="${PERIZIA_PDF:-/srv/perizia/app/uploads/1859886_c_perizia.pdf}"
ESTRATTO_PDF="${ESTRATTO_PDF:-/srv/perizia/app/uploads/estratto_agency.pdf}"
BASE_URL="${BASE_URL:-https://api-periziascan.nexodify.com}"
SESSION_TOKEN="${SESSION_TOKEN:-${session_token:-}}"
OFFLINE_QA="${OFFLINE_QA:-0}"

# Safety: if running against localhost, never skip step2-4
if [[ "$BASE_URL" =~ ^http://(127\.0\.0\.1|localhost)(:|/|$) ]]; then
  OFFLINE_QA=0
fi

OFFLINE_QA_TOKEN="${OFFLINE_QA_TOKEN:-}"

if [[ -z "$SESSION_TOKEN" ]]; then
  echo "SESSION_TOKEN missing"
  exit 2
fi

RUN_DIR="/srv/perizia/_qa/runs/$(date +%F_%H%M%S)_customer_grade"
mkdir -p "$RUN_DIR"
echo "$RUN_DIR"

echo "[1/8] Upload fresh perizia..."
if [[ "$OFFLINE_QA" == "1" ]]; then
  curl -sS -D "$RUN_DIR/upload.h" -o "$RUN_DIR/upload.body" \
    -X POST -H "Cookie: session_token=${SESSION_TOKEN}" \
    -H "X-OFFLINE-QA: 1" -H "X-OFFLINE-QA-TOKEN: ${OFFLINE_QA_TOKEN}" \
    -F "file=@${PERIZIA_PDF}" \
    "${BASE_URL}/api/analysis/perizia"
else
  curl -sS -D "$RUN_DIR/upload.h" -o "$RUN_DIR/upload.body" \
    -X POST -H "Cookie: session_token=${SESSION_TOKEN}" \
    -F "file=@${PERIZIA_PDF}" \
    "${BASE_URL}/api/analysis/perizia"
fi

NEW_AID="$(python3 - <<PY
import json
j=json.load(open("$RUN_DIR/upload.body"))
print(j.get("analysis_id") or j.get("id") or (j.get("analysis") or {}).get("analysis_id") or "")
PY
)"
if [[ -z "$NEW_AID" ]]; then
  echo "Failed to create analysis id"
  exit 1
fi
echo "$NEW_AID" > "$RUN_DIR/NEW_AID.txt"

echo "[2/8] Fetch system JSON..."
if [[ "$OFFLINE_QA" == "1" ]]; then
  curl -sS -o "$RUN_DIR/system.json" \
    -H "Cookie: session_token=${SESSION_TOKEN}" \
    -H "X-OFFLINE-QA: 1" -H "X-OFFLINE-QA-TOKEN: ${OFFLINE_QA_TOKEN}" \
    "${BASE_URL}/api/analysis/perizia/${NEW_AID}"
else
  curl -sS -o "$RUN_DIR/system.json" \
    -H "Cookie: session_token=${SESSION_TOKEN}" \
    "${BASE_URL}/api/analysis/perizia/${NEW_AID}"
fi
python3 -m json.tool "$RUN_DIR/system.json" > "$RUN_DIR/system.pretty.json"

echo "[3/8] Existing regressions step2-4..."
cd "$BACKEND_DIR"
if [[ "$OFFLINE_QA" == "1" ]]; then
  echo "Skipping step2-4 HTTP-auth regressions in OFFLINE_QA mode"
else
  BASE_URL="$BASE_URL" SESSION_TOKEN="$SESSION_TOKEN" ./.venv/bin/python scripts/regression_step2_contract.py --analysis-id "$NEW_AID"
  BASE_URL="$BASE_URL" SESSION_TOKEN="$SESSION_TOKEN" ./.venv/bin/python scripts/regression_step3_semiforo_and_fields.py --analysis-id "$NEW_AID"
  BASE_URL="$BASE_URL" SESSION_TOKEN="$SESSION_TOKEN" ./.venv/bin/python scripts/regression_step4_pdf_content.py --analysis-id "$NEW_AID"
fi

echo "[4/8] Build estratto reference..."
./.venv/bin/python scripts/estratto_ref_build.py --pdf "$ESTRATTO_PDF" --out "$RUN_DIR/estratto_ref.json"

echo "[5/8] Gate A: strict estratto parity..."
BASE_URL="$BASE_URL" SESSION_TOKEN="$SESSION_TOKEN" OFFLINE_QA="$OFFLINE_QA" OFFLINE_QA_TOKEN="$OFFLINE_QA_TOKEN" ./.venv/bin/python scripts/regression_gate_estratto_parity_strict.py \
  --analysis-id "$NEW_AID" --estratto-pdf "$ESTRATTO_PDF" --run-dir "$RUN_DIR"

echo "[6/8] Capture frontend snapshot..."
cd "$FRONTEND_DIR"
HOST=127.0.0.1 PORT=5180 npm start > "$RUN_DIR/frontend_gate.log" 2>&1 &
FEPID=$!
cleanup() {
  kill "$FEPID" >/dev/null 2>&1 || true
}
trap cleanup EXIT
for _ in $(seq 1 80); do
  curl -sS -I http://127.0.0.1:5180 >/dev/null 2>&1 && break
  sleep 1
done
NEW_AID="$NEW_AID" RUN_DIR="$RUN_DIR" FRONTEND_URL="http://127.0.0.1:5180/analysis/${NEW_AID}?debug=1" SESSION_TOKEN="$SESSION_TOKEN" \
  node scripts/capture_ui_snapshot.mjs

echo "[7/8] Gate B: frontend parity..."
cd "$BACKEND_DIR"
SESSION_TOKEN="$SESSION_TOKEN" ./.venv/bin/python scripts/regression_gate_frontend_parity.py --run-dir "$RUN_DIR"

echo "[8/8] Gate C: decisione rapida specificity..."
BASE_URL="$BASE_URL" SESSION_TOKEN="$SESSION_TOKEN" OFFLINE_QA="$OFFLINE_QA" OFFLINE_QA_TOKEN="$OFFLINE_QA_TOKEN" ./.venv/bin/python scripts/regression_gate_decisione_rapida_specific.py --analysis-id "$NEW_AID"

echo "GREEN: all customer-grade gates passed"
echo "RUN_DIR=$RUN_DIR"
