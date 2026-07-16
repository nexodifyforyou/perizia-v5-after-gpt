#!/usr/bin/env python3
"""V2 customer-surface safety gate.

REPLACES `regression_gate_frontend_parity.py`, which asserted that the LEGACY
report body displayed a fixed set of legacy fields. That legacy body was removed
permanently (branch: feature-customer-access-and-legacy-removal), so the old gate
could never pass again and was deleted rather than left failing.

This gate protects the properties that matter now:

  1. the V2 customer surface renders and the page is NEVER blank;
  2. the page shell is fed by the metadata-only endpoint (/meta);
  3. NO legacy report payload is fetched by the frontend;
  4. NO legacy DOM exists (body / reveal toggle / print / download controls);
  5. customer-safe reason codes are preserved (closed public enum, no internal
     codes, no admin/debug/artifact leakage);
  6. exact-owner diagnostics remain available (Vista admin data source);
  7. the legacy PDF endpoint remains unauthorized for normal users.

DOM/network assertions (1-4) come from
`frontend/scripts/capture_ui_snapshot.mjs`, which writes
`{run_dir}/v2_surface_snapshot.json`. API assertions (5-7) are made directly.

Usage:
  regression_gate_v2_customer_surface.py --run-dir DIR [--analysis-id ID]
                                         [--frontend-url URL] [--base-url URL]

Env:
  SESSION_TOKEN         owner/admin session (required for capture + owner checks)
  NORMAL_SESSION_TOKEN  optional; enables the normal-user denial checks (7)
Tokens are never printed by this script.
"""
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

# The ONLY reason_code values a customer may ever receive.
PUBLIC_REASON_CODES = {
    "PREPARING",
    "SERVICE_BUSY",
    "VERIFICATION_REQUIRED",
    "SERVICE_UNAVAILABLE",
    "NO_REPORT",
}

# Keys the customer-view payload may contain. Anything else is a leak.
ALLOWED_PAYLOAD_KEYS = {"available", "selected_lot_id", "preparing", "reason_code", "report"}

# Internal codes / admin machinery that must never reach a customer response.
FORBIDDEN_SUBSTRINGS = [
    "OPENAI_QUOTA_EXHAUSTED",
    "OPENAI_CALL_FAILED",
    "OPENAI_RATE_LIMITED",
    "CONTRACT_VALIDATION_FAILED",
    "NEEDS_MANUAL_REVIEW",
    "FAILED_ANALYSIS",
    "FAILED_CONTRACT_BUILD",
    "JOB_STALLED",
    "MONEY_CHAIN_INCONSISTENT",
    "REPORT_QUALITY_GATE_FAILED",
    "NO_CUSTOMER_REPORT",
    "reason_human",
    "troubleshoot_message",
    "artifacts_saved",
    "Traceback",
]

# Admin-only report keys (exact match, at any depth).
ADMIN_ONLY_KEYS = {
    "evidence_index",
    "admin_evidence_index",
    "quality_control",
    "sections_meta",
    "surfaces_section",
    "manual_review_flags",
    "_saved_at",
}

FAILURES: List[str] = []
PASSES: List[str] = []


def _ok(msg: str) -> None:
    PASSES.append(msg)


def _bad(msg: str) -> None:
    FAILURES.append(msg)


def _deep_keys(obj: Any, acc=None) -> set:
    acc = acc if acc is not None else set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            acc.add(k)
            _deep_keys(v, acc)
    elif isinstance(obj, list):
        for v in obj:
            _deep_keys(v, acc)
    return acc


def _check_dom_and_network(snap: Dict[str, Any], analysis_id: str) -> None:
    dom = snap.get("dom") or {}
    net = snap.get("network") or {}

    # 1. surface renders, never blank
    if dom.get("v2_surface_mounted"):
        _ok("V2 surface mounted")
    else:
        _bad("V2 surface did NOT mount (blank-page regression)")
    if dom.get("page_is_blank"):
        _bad("page rendered blank")
    else:
        _ok(f"page not blank ({dom.get('body_text_len')} chars)")
    states = dom.get("v2_states_present") or []
    if states:
        _ok(f"V2 customer state rendered: {states}")
    else:
        _bad("no V2 customer state rendered (surface mounted but empty)")

    # 4. no legacy DOM
    legacy = dom.get("legacy_testids_present") or []
    if legacy:
        _bad(f"LEGACY DOM PRESENT: {legacy}")
    else:
        _ok("no legacy DOM (body/reveal/print/download all absent)")

    # 2. metadata endpoint feeds the shell
    metas = net.get("meta_fetches") or []
    if metas:
        _ok(f"metadata endpoint used ({len(metas)} fetch(es))")
    else:
        _bad(f"/api/analysis/perizia/{analysis_id}/meta was never fetched")

    # 3. no legacy payload fetched
    legacy_payload = net.get("legacy_payload_fetches") or []
    if legacy_payload:
        _bad(f"LEGACY PAYLOAD FETCHED: {legacy_payload}")
    else:
        _ok("no legacy report payload fetched")
    legacy_render = net.get("legacy_render_fetches") or []
    if legacy_render:
        _bad(f"LEGACY RENDER ENDPOINT FETCHED: {legacy_render}")
    else:
        _ok("no legacy /pdf|/pdf-html|/html fetched")

    cv = net.get("customer_view_fetches") or []
    if cv:
        _ok(f"sanitized customer-view fetched ({len(cv)})")
    else:
        _bad("customer-view endpoint never fetched")


def _check_api(base_url: str, analysis_id: str) -> None:
    try:
        import requests  # noqa: F401
    except Exception:
        _bad("python 'requests' not available for API checks")
        return
    import requests

    owner = os.environ.get("SESSION_TOKEN") or os.environ.get("session_token")
    if not owner:
        _bad("SESSION_TOKEN required for API checks")
        return
    oh = {"Authorization": f"Bearer {owner}"}
    base = base_url.rstrip("/")

    # 5. customer-safe reason codes / payload shape
    url = f"{base}/api/analysis/perizia/{analysis_id}/correctness-v2/customer-view/latest"
    try:
        r = requests.get(url, headers=oh, timeout=60)
    except Exception as exc:
        _bad(f"customer-view request failed: {exc}")
        return
    if r.status_code != 200:
        _bad(f"customer-view returned {r.status_code}")
        return
    body = r.text
    data = r.json()
    extra = set(data.keys()) - ALLOWED_PAYLOAD_KEYS
    if extra:
        _bad(f"customer payload has non-allowed keys: {sorted(extra)}")
    else:
        _ok("customer payload keys within the allowed set")
    if not data.get("available"):
        rc = data.get("reason_code")
        if rc in PUBLIC_REASON_CODES:
            _ok(f"reason_code in closed public enum: {rc}")
        else:
            _bad(f"reason_code NOT in public enum: {rc!r}")
    else:
        _ok("customer report available (report_status=%s)" % ((data.get("report") or {}).get("report_status")))
    leaked = [s for s in FORBIDDEN_SUBSTRINGS if s in body]
    if leaked:
        _bad(f"internal detail leaked to customer: {leaked}")
    else:
        _ok("no internal codes/admin machinery in customer payload")
    admin_keys = sorted(_deep_keys(data.get("report") or {}) & ADMIN_ONLY_KEYS)
    if admin_keys:
        _bad(f"admin-only keys in customer report: {admin_keys}")
    else:
        _ok("no admin-only keys in customer report")

    # 6. exact-owner diagnostics remain available
    diag = f"{base}/api/analysis/perizia/{analysis_id}/correctness-v2/latest"
    try:
        rd = requests.get(diag, headers=oh, timeout=60)
        if rd.status_code == 200:
            _ok("exact-owner diagnostics available (Vista admin data source)")
        elif rd.status_code == 404:
            _ok("owner diagnostics reachable (404 = no V2 job for this analysis)")
        else:
            _bad(f"owner diagnostics returned {rd.status_code}")
    except Exception as exc:
        _bad(f"owner diagnostics request failed: {exc}")

    # 7. normal-user denial (optional: needs a normal session)
    normal = os.environ.get("NORMAL_SESSION_TOKEN")
    if not normal:
        PASSES.append(
            "SKIP: NORMAL_SESSION_TOKEN not set - normal-user denial checks not run "
            "(covered by backend/tests/test_legacy_access_gating.py)"
        )
        return
    nh = {"Authorization": f"Bearer {normal}"}
    for label, path, expect in [
        ("legacy PDF", f"/api/analysis/perizia/{analysis_id}/pdf", (401, 403, 404)),
        ("legacy print PDF", f"/api/analysis/perizia/{analysis_id}/pdf-html", (401, 403, 404)),
        ("legacy payload", f"/api/analysis/perizia/{analysis_id}", (401, 403, 404)),
        ("legacy history detail", f"/api/history/perizia/{analysis_id}", (401, 403, 404)),
    ]:
        try:
            rr = requests.get(f"{base}{path}", headers=nh, timeout=60)
            if rr.status_code in expect:
                _ok(f"normal user denied on {label} ({rr.status_code})")
            else:
                _bad(f"normal user NOT denied on {label}: {rr.status_code}")
        except Exception as exc:
            _bad(f"{label} check failed: {exc}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--analysis-id", required=False)
    ap.add_argument("--frontend-url", required=False)
    ap.add_argument("--base-url", required=False, default=os.environ.get("BASE_URL", ""))
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)

    if args.frontend_url and args.analysis_id:
        node = Path("/srv/perizia/app/frontend/scripts/capture_ui_snapshot.mjs")
        if not node.exists():
            _bad("capture_ui_snapshot.mjs not found")
        else:
            env = os.environ.copy()
            env["NEW_AID"] = args.analysis_id
            env["RUN_DIR"] = str(run_dir)
            env["FRONTEND_URL"] = args.frontend_url
            if not env.get("SESSION_TOKEN"):
                _bad("SESSION_TOKEN required to run the Playwright capture")
            else:
                subprocess.run(["node", str(node)], check=True, env=env, cwd="/srv/perizia/app/frontend")

    snap_path = run_dir / "v2_surface_snapshot.json"
    if snap_path.exists():
        snap = json.loads(snap_path.read_text(encoding="utf-8"))
        if not isinstance(snap, dict):
            _bad("v2_surface_snapshot.json is null/non-object")
        else:
            _check_dom_and_network(snap, snap.get("analysis_id") or args.analysis_id or "")
    else:
        _bad(f"{snap_path} not found (run with --frontend-url/--analysis-id to capture)")

    if args.base_url and args.analysis_id:
        _check_api(args.base_url, args.analysis_id)
    else:
        PASSES.append("SKIP: --base-url/--analysis-id not given - API checks not run")

    for p in PASSES:
        print(f"PASS: {p}" if not p.startswith("SKIP") else p)
    if FAILURES:
        print("\nFAIL:")
        for f in FAILURES:
            print(f"- {f}")
        sys.exit(1)
    print("\nGREEN: V2 customer-surface gate passed")


if __name__ == "__main__":
    main()
