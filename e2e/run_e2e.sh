#!/usr/bin/env bash
# End-to-end harness for passwordless email authentication.
#
# Everything is local and disposable:
#   - an isolated Mongo database (never the production one)
#   - a backend on a throwaway port with OTP enabled
#   - the SMTP sink from e2e/smtp_sink.py, so no mail leaves the machine
#   - a frontend build pointed at the local backend
#
# Resend is never contacted and no production configuration is read.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="${E2E_WORK_DIR:-/tmp/perizia_e2e}"
BACKEND_PORT="${E2E_BACKEND_PORT:-8099}"
FRONTEND_PORT="${E2E_FRONTEND_PORT:-3099}"
SMTP_PORT="${E2E_SMTP_PORT:-1099}"
MAIL_FILE="$WORK/mail.jsonl"
# Both rollout states are exercised against the SAME frontend build, which is
# the whole point: the build carries no flag, so only the backend decides.
AUTH_EMAIL_ENABLED="${E2E_AUTH_EMAIL_ENABLED:-true}"

mkdir -p "$WORK"
: > "$WORK/backend.log"

# A leftover process from an aborted run would silently serve the tests with
# stale configuration (and a dead mail sink), so reclaim the ports first.
for port in "$BACKEND_PORT" "$FRONTEND_PORT" "$SMTP_PORT"; do
  stale="$(ss -lptn "sport = :$port" 2>/dev/null | grep -oP 'pid=\K[0-9]+' | sort -u || true)"
  if [ -n "$stale" ]; then
    echo "==> reclaiming port $port from pid(s): $stale"
    kill $stale 2>/dev/null || true
  fi
done
sleep 1

PROD_DB="$(grep '^DB_NAME=' "$ROOT/backend/.env" | cut -d= -f2- | tr -d '"' | tr -d "'")"
E2E_DB="test_e2e_${PROD_DB}"
if [ "$E2E_DB" = "$PROD_DB" ]; then
  echo "REFUSING: e2e database resolved to production" >&2
  exit 1
fi
export DB_NAME="$E2E_DB"

cleanup() {
  for pidfile in "$WORK"/*.pid; do
    [ -f "$pidfile" ] || continue
    kill "$(cat "$pidfile")" 2>/dev/null || true
    rm -f "$pidfile"
  done
}
trap cleanup EXIT

echo "==> SMTP sink on $SMTP_PORT"
"$ROOT/backend/.venv/bin/python" "$ROOT/e2e/smtp_sink.py" --port "$SMTP_PORT" --out "$MAIL_FILE" \
  > "$WORK/smtp.log" 2>&1 &
echo $! > "$WORK/smtp.pid"

# FRONTEND_URL below is what puts the harness origin into CORS_ORIGINS. Without
# it the browser blocks every cross-origin POST and the specs fail with no
# request ever reaching the backend.
echo "==> Backend on $BACKEND_PORT (db=$E2E_DB, AUTH_EMAIL_ENABLED=$AUTH_EMAIL_ENABLED)"
(
  cd "$ROOT/backend"
  AUTH_EMAIL_ENABLED="$AUTH_EMAIL_ENABLED" \
  AUTH_EMAIL_PROVIDER=sink \
  AUTH_EMAIL_FROM="Perizia Scan <accesso@auth.nexodify.com>" \
  AUTH_EMAIL_CODE_PEPPER="e2e-pepper-value-0123456789-0123456789-abcdef" \
  AUTH_EMAIL_SENDER_DOMAIN_VERIFIED=true \
  AUTH_EMAIL_RESEND_COOLDOWN_SECONDS=0 \
  AUTH_EMAIL_MAX_REQUESTS_PER_EMAIL_HOUR=50 \
  AUTH_EMAIL_MAX_REQUESTS_PER_IP_HOUR=200 \
  AUTH_EMAIL_SINK_HOST=127.0.0.1 \
  AUTH_EMAIL_SINK_PORT="$SMTP_PORT" \
  DB_NAME="$E2E_DB" \
  FRONTEND_URL="http://127.0.0.1:$FRONTEND_PORT" \
  .venv/bin/python -m uvicorn server:app --host 127.0.0.1 --port "$BACKEND_PORT"
) > "$WORK/backend.log" 2>&1 &
echo $! > "$WORK/backend.pid"

echo -n "    waiting for backend"
backend_up=0
for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:$BACKEND_PORT/api/health" >/dev/null 2>&1; then
    backend_up=1; echo " ok"; break
  fi
  echo -n "."; sleep 1
done
if [ "$backend_up" -ne 1 ]; then
  echo ""
  echo "BACKEND FAILED TO START — refusing to run tests against a stale server." >&2
  tail -20 "$WORK/backend.log" >&2
  exit 1
fi

echo "==> Frontend build for e2e"
if [ ! -d "$WORK/build" ] || [ "${E2E_REBUILD:-0}" = "1" ]; then
  (
    cd "$ROOT/frontend"
    REACT_APP_BACKEND_URL="http://127.0.0.1:$BACKEND_PORT" \
    BUILD_PATH="$WORK/build" \
    CI=true npx craco build
  ) > "$WORK/frontend_build.log" 2>&1
fi

echo "==> Serving frontend on $FRONTEND_PORT"
(cd "$WORK/build" && "$ROOT/backend/.venv/bin/python" -m http.server "$FRONTEND_PORT" --bind 127.0.0.1) \
  > "$WORK/frontend.log" 2>&1 &
echo $! > "$WORK/frontend.pid"
sleep 2

echo "==> Playwright"
cd "$ROOT/frontend"
E2E_BASE_URL="http://127.0.0.1:$FRONTEND_PORT" \
E2E_API_URL="http://127.0.0.1:$BACKEND_PORT" \
E2E_MAIL_FILE="$MAIL_FILE" \
E2E_AUTH_EMAIL_ENABLED="$AUTH_EMAIL_ENABLED" \
npx playwright test --config "$ROOT/frontend/playwright.config.js" "$@"
