#!/usr/bin/env bash
set -euo pipefail

ROOT="/srv/perizia/app"
PDF="$ROOT/perizia_test.pdf"

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
curl -sS -D /tmp/g1.h -o /tmp/g1.json \
  -X POST http://127.0.0.1:8081/api/analysis/perizia \
  -H "X-OFFLINE-QA: 1" \
  -F "file=@$PDF;type=application/pdf" || fail "Gate 1 curl failed"
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
echo "Prereq: backend on 127.0.0.1:8081 started with ALLOW_OFFLINE_QA=1 OFFLINE_QA_TOKEN=devtoken"
curl -sS -D /tmp/g3.h -o /tmp/g3.json \
  -X POST http://127.0.0.1:8081/api/analysis/perizia \
  -H "X-OFFLINE-QA: 1" \
  -H "X-OFFLINE-QA-TOKEN: devtoken" \
  -F "file=@$PDF;type=application/pdf" || fail "Gate 3 curl failed"
grep -q " 200 " /tmp/g3.h || fail "Expected HTTP 200 in /tmp/g3.h"
grep -q "\"ok\":true" /tmp/g3.json || fail "Expected ok:true in /tmp/g3.json"
grep -q "\"offset_mode\"" /tmp/g3.json || fail "Expected offset_mode in /tmp/g3.json"
grep -q "\"evidence\"" /tmp/g3.json || fail "Expected evidence in /tmp/g3.json"
echo "PASS: Gate 3"

echo "ALL SECURITY GATES PASSED"
