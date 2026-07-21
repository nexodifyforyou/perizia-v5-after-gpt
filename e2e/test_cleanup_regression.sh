#!/usr/bin/env bash
# Deterministic regression proof for the run_e2e.sh cleanup fix.
#
# The bug: a server launched as `( ... ) &` records the SUBSHELL pid in `$!`,
# not the real server pid. Killing that subshell pid orphaned the backend on
# every e2e run (found live during the passwordless-auth rollout: an 8099
# backend survived 50 minutes past its harness).
#
# The fix: launch each server with `setsid` (own process group) and have cleanup
# kill the process GROUP (`kill -- -PGID`), which reaches the real child.
#
# This test reproduces the exact launch/cleanup shapes with a dummy long-lived
# child and asserts the child is actually dead afterwards. No Mongo, no build,
# no network — it runs anywhere in a second.
set -u

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
fail=0

# --- the fixed pattern: setsid + exec, record group-leader pid --------------
E2E_MARK="cleanup_regression_child_$$"
E2E_MARK="$E2E_MARK" setsid bash -c 'exec sleep 300 # '"$E2E_MARK" \
  > "$WORK/child.log" 2>&1 &
child_pgid=$!
echo "$child_pgid" > "$WORK/child.pid"

sleep 0.5
if ! kill -0 "$child_pgid" 2>/dev/null; then
  echo "SETUP FAIL: child did not start"
  exit 1
fi
echo "started child group leader pid=$child_pgid"

# --- the fixed cleanup: kill the process group ------------------------------
cleanup() {
  local pidfile pgid
  for pidfile in "$WORK"/*.pid; do
    [ -f "$pidfile" ] || continue
    pgid="$(cat "$pidfile")"
    [ -n "$pgid" ] && kill -TERM -- "-$pgid" 2>/dev/null || true
  done
  sleep 1
  for pidfile in "$WORK"/*.pid; do
    [ -f "$pidfile" ] || continue
    pgid="$(cat "$pidfile")"
    [ -n "$pgid" ] && kill -KILL -- "-$pgid" 2>/dev/null || true
    rm -f "$pidfile"
  done
}
cleanup

# --- assertions -------------------------------------------------------------
if kill -0 "$child_pgid" 2>/dev/null; then
  echo "FAIL: child pid $child_pgid still alive after cleanup"
  fail=1
fi
# Belt and braces: no process carrying our unique marker survives.
if pgrep -f "$E2E_MARK" >/dev/null 2>&1; then
  echo "FAIL: a process still matches marker $E2E_MARK"
  pgrep -af "$E2E_MARK"
  fail=1
fi

if [ "$fail" -eq 0 ]; then
  echo "PASS: process-group cleanup terminated the child, no orphan survived"
fi
exit "$fail"
