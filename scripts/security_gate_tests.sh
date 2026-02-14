#!/usr/bin/env bash
set -euo pipefail

ROOT="/srv/perizia/app"
PDF="$ROOT/perizia_test.pdf"
OFFLINE_QA_PORT="${OFFLINE_QA_PORT:-8082}"
OFFLINE_QA_TOKEN="${OFFLINE_QA_TOKEN:-}"
OFFLINE_QA_FILE="${OFFLINE_QA_FILE:-$PDF}"

rm -f /tmp/g1.h /tmp/g1.json /tmp/g2.h /tmp/g2.json /tmp/g3.h /tmp/g3.json /tmp/g3_notoken.h /tmp/g3_notoken.json

fail() {
  echo "FAIL: $1" >&2
  exit 1
}

assert_401_not_authenticated() {
  local headers_file="$1"
  local body_file="$2"
  grep -q " 401 " "$headers_file" || fail "Expected HTTP 401 in $headers_file"
  grep -q "Not authenticated" "$body_file" || fail "Expected Not authenticated body in $body_file"
}

echo "[GATE 1] Local unauth exploit must fail (401)"
if ! curl -sS -D /tmp/g1.h -o /tmp/g1.json \
  -X POST http://127.0.0.1:8081/api/analysis/perizia \
  -H "X-OFFLINE-QA: 1" \
  -F "file=@$PDF;type=application/pdf"; then
  fail "Backend not running on 127.0.0.1:8081 — start periziascan-backend.service and re-run"
fi
assert_401_not_authenticated /tmp/g1.h /tmp/g1.json
echo "PASS: Gate 1"

echo "[GATE 2] Public unauth exploit must fail (401)"
curl -sS -D /tmp/g2.h -o /tmp/g2.json \
  -X POST https://api-periziascan.nexodify.com/api/analysis/perizia \
  -H "X-OFFLINE-QA: 1" \
  -F "file=@$PDF;type=application/pdf" || fail "Gate 2 curl failed"
assert_401_not_authenticated /tmp/g2.h /tmp/g2.json
echo "PASS: Gate 2"

echo "[GATE 3] Offline QA only with env+token on localhost"
if [[ -z "$OFFLINE_QA_TOKEN" ]]; then
  fail "OFFLINE_QA_TOKEN is empty. Run: OFFLINE_QA_PORT=8082 OFFLINE_QA_TOKEN=devtoken bash scripts/security_gate_tests.sh"
fi
echo "Prereq: backend on 127.0.0.1:${OFFLINE_QA_PORT} started with ALLOW_OFFLINE_QA=1 OFFLINE_QA_TOKEN=<token>"
curl -sS -D /tmp/g3.h -o /tmp/g3.json \
  -X POST "http://127.0.0.1:${OFFLINE_QA_PORT}/api/analysis/perizia" \
  -H "X-OFFLINE-QA: 1" \
  -H "X-OFFLINE-QA-TOKEN: ${OFFLINE_QA_TOKEN}" \
  -F "file=@${OFFLINE_QA_FILE};type=application/pdf" || fail "Gate 3 curl failed"
[[ -s /tmp/g3.json ]] || fail "Expected fresh non-empty /tmp/g3.json from Gate 3"
grep -q " 200 " /tmp/g3.h || fail "Expected HTTP 200 in /tmp/g3.h"
python3 - <<'PY' || fail "Expected ok:true in /tmp/g3.json"
import json
with open("/tmp/g3.json", "r", encoding="utf-8") as f:
    payload = json.load(f)
if payload.get("ok") is not True:
    raise SystemExit(1)
PY
python3 - <<'PY' || fail "Expected result.offset_mode == PAGE_LOCAL in /tmp/g3.json"
import json
with open("/tmp/g3.json", "r", encoding="utf-8") as f:
    payload = json.load(f)
result = payload.get("result")
if not isinstance(result, dict) or result.get("offset_mode") != "PAGE_LOCAL":
    raise SystemExit(1)
PY
curl -sS -D /tmp/g3_notoken.h -o /tmp/g3_notoken.json \
  -X POST "http://127.0.0.1:${OFFLINE_QA_PORT}/api/analysis/perizia" \
  -H "X-OFFLINE-QA: 1" \
  -F "file=@${OFFLINE_QA_FILE};type=application/pdf" || fail "Gate 3 no-token curl failed"
assert_401_not_authenticated /tmp/g3_notoken.h /tmp/g3_notoken.json
echo "PASS: Gate 3"

echo "ALL SECURITY GATES PASSED"
