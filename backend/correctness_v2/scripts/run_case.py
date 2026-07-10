"""
Full-from-start stability run for one real perizia (Correctness Mode v2).

Drives orchestrator.start_job with the REAL OpenAI caller (analyst -> lot routing
-> validator -> contract -> customer report -> quality gate), exactly like the
live /correctness-v2/start endpoint, but offline. Pages come from the analysis's
saved extract (same source _load_pages_for_analysis uses).

For a multi-lot perizia it first runs the no-selection request; if that returns
LOT_SELECTION_REQUIRED it then runs every offered lot via selected_lot_id, so
"all their lotti" are exercised.

Writes a compact machine-readable summary (+ full reports) under OUT and prints
it. Never touches the live job store when CORRECTNESS_V2_ARTIFACTS_ROOT is set.

Usage:
    python -m correctness_v2.scripts.run_case ANALYSIS_ID [--label NAME] [--out DIR]
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

QA = "/srv/perizia/_qa/runs/{aid}/extract/pages_raw.json"
RAW_KEY_RE = re.compile(r"(technical_compliance|risk_classification|legal_formalities)\[\d+\]")
COMPARATIVE_TOKENS = ("comparativ", "omi", "borsin", "annunc")
# Admin-only keys that must never survive into the customer-facing sanitized view.
ADMIN_LEAK_KEYS = (
    "quality_control", "evidence_index", "admin_evidence_index", "sections_meta",
    "surfaces_section", "manual_review_flags",
)
ADMIN_MONEY_KEYS = ("market_comparatives", "context_values")


def _load_env() -> None:
    env = Path(__file__).resolve().parents[2] / ".env"
    for line in env.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def _page_loader(aid: str):
    path = QA.format(aid=aid)
    if not os.path.exists(path):
        raise SystemExit(f"no extract pages for {aid}: {path}")
    pages = json.load(open(path, encoding="utf-8"))
    out = []
    for idx, p in enumerate(pages, start=1):
        pn = p.get("page_number", p.get("page", idx))
        try:
            pn = int(pn)
        except Exception:
            pn = idx
        out.append({"page_number": pn, "text": str(p.get("text", "") or "")})
    return out


def _report_path(job_id: str):
    from correctness_v2 import artifacts
    return artifacts.jobs_root() / job_id / "customer_report.json"


def _load_report(job_id: str):
    p = _report_path(job_id)
    return json.load(open(p, encoding="utf-8")) if p.exists() else None


def _invariants(label: str, status: dict, san: dict | None, warnings: list) -> list[dict]:
    """Customer-facing invariants, evaluated on the SANITIZED customer view (what
    the customer actually sees). Soft issues are appended to ``warnings``."""
    out = []

    def chk(name, ok, detail=""):
        out.append({"check": name, "ok": bool(ok), "detail": str(detail)[:220]})

    def warn(name, detail=""):
        warnings.append({"warn": name, "detail": str(detail)[:220]})

    st = status.get("status")
    gate = status.get("quality_gate_status")
    chk("terminal_ok", st in ("REPORT_READY", "LOT_SELECTION_REQUIRED"),
        f"status={st} reason={status.get('reason_code')}")
    chk("gate_not_fail", gate in (None, "PASS", "WARNING"), f"gate={gate}")

    if san is None:
        chk("customer_view_present", False, "sanitize returned None (not customer-safe)")
        return out
    chk("customer_view_present", True)

    blob = json.dumps(san, ensure_ascii=False)
    leaked_admin = [k for k in ADMIN_LEAK_KEYS if k in san]
    leaked_money = [k for k in ADMIN_MONEY_KEYS if k in (san.get("money_sections") or {})]
    chk("no_admin_leak", not leaked_admin and not leaked_money,
        f"top={leaked_admin} money={leaked_money}")
    chk("no_raw_bracket_keys", not RAW_KEY_RE.search(blob),
        (RAW_KEY_RE.search(blob) or [""])[0] if RAW_KEY_RE.search(blob) else "")

    if st == "LOT_SELECTION_REQUIRED":
        lots = (san.get("lot_selection") or {}).get("lots") or []
        chk("selector_has_lots", len(lots) >= 2, f"lots={[l.get('lot_id') for l in lots]}")
        return out

    # REPORT_READY invariants (sanitized)
    ls = san.get("lot_structure") or {}
    chk("bene_count_ge_1", (ls.get("bene_count") or 0) >= 1, str(ls))
    chk("beni_sections_nonempty", bool(san.get("beni_sections")),
        f"{len(san.get('beni_sections') or [])} sections")

    ms = san.get("money_sections") or {}
    unc = ms.get("uncertain_money") or []
    leaked = [r for r in unc if any(t in str(r.get("label", "")).lower() for t in COMPARATIVE_TOKENS)]
    chk("no_comparatives_in_uncertain", not leaked, f"leaked={[r.get('label') for r in leaked]}")

    cei = san.get("customer_evidence_index") or []
    chk("evidence_present", bool(cei), f"{len(cei)} rows")
    missing_excerpt = [e for e in cei if not (e.get("perizia_excerpt") or "").strip()]
    if missing_excerpt:
        warn("evidence_rows_missing_excerpt",
             f"{len(missing_excerpt)}/{len(cei)}: " +
             "; ".join(f"p{e.get('page')} {e.get('topic')}" for e in missing_excerpt[:8]))
    return out


def _run(aid: str, label: str, **kw):
    from correctness_v2 import orchestrator, openai_client, customer_view
    status = orchestrator.start_job(
        aid, _page_loader, openai_caller=openai_client.call_openai_json, **kw
    )
    report = _load_report(status["job_id"])
    san = None
    if report is not None:
        try:
            san = customer_view.sanitize_customer_report(report, status)
        except Exception as e:  # sanitizer refusing = itself a signal
            san = None
            report = report  # keep raw for debugging
    warnings: list = []
    inv = _invariants(label, status, san, warnings)
    return {
        "label": label,
        "analysis_id": aid,
        "selection": kw,
        "job_id": status["job_id"],
        "status": status.get("status"),
        "gate_status": status.get("quality_gate_status"),
        "coverage_status": status.get("coverage_status"),
        "quality_status": status.get("quality_status"),
        "score": status.get("satisfaction_score"),
        "reason_code": status.get("reason_code"),
        "reason_human": status.get("reason_human"),
        "invariants": inv,
        "failed_invariants": [i for i in inv if not i["ok"]],
        "warnings": warnings,
    }


def main() -> int:
    _load_env()
    ap = argparse.ArgumentParser()
    ap.add_argument("analysis_id")
    ap.add_argument("--label", default=None)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    label = args.label or args.analysis_id
    out_dir = Path(args.out or (Path(os.environ.get(
        "CORRECTNESS_V2_REPLAY_ROOT",
        "/tmp/claude-1001/-srv-perizia-app/d7d2a1fa-ddcc-417e-a62a-6afbd5658f5b/scratchpad/runs")) / label))
    out_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("CORRECTNESS_V2_ARTIFACTS_ROOT", str(out_dir / "_jobs"))

    results = []
    base = _run(args.analysis_id, label)
    results.append(base)

    if base["status"] == "LOT_SELECTION_REQUIRED":
        rep = _load_report(base["job_id"]) or {}
        lots = (rep.get("lot_selection") or {}).get("lots") or []
        for l in lots:
            lot_id = str(l.get("lot_id"))
            r = _run(args.analysis_id, f"{label}::lot_{lot_id}", selected_lot_id=lot_id)
            results.append(r)

    summary = {
        "label": label,
        "analysis_id": args.analysis_id,
        "runs": results,
        "all_ok": all(not r["failed_invariants"] for r in results),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    # Compact stdout
    for r in results:
        flag = "OK " if not r["failed_invariants"] else "XX "
        print(f"{flag}{r['label']}: {r['status']} gate={r['gate_status']} "
              f"cov={r['coverage_status']} qual={r['quality_status']} score={r['score']} "
              f"job={r['job_id']}")
        if r["reason_code"]:
            print(f"     reason: {r['reason_code']} | {r['reason_human']}")
        for i in r["failed_invariants"]:
            print(f"     FAIL {i['check']}: {i['detail']}")
        for w in r.get("warnings", []):
            print(f"     warn {w['warn']}: {w['detail']}")
    print(f"\nALL_OK={summary['all_ok']}  summary={out_dir/'summary.json'}")
    return 0 if summary["all_ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
