#!/usr/bin/env python3
"""Compare legacy customer Money Box outputs with authority money roles.

Phase 3E is shadow-only: this script reports where authority classification
would avoid unsafe cost labels, but it does not mutate saved analyses or
customer-facing payloads.
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

try:
    from scripts.audit_authority_corpus import _extract_pdf_pages
except Exception:  # pragma: no cover
    from audit_authority_corpus import _extract_pdf_pages

from customer_decision_contract import sanitize_customer_facing_result, separate_internal_runtime_from_customer_result
from perizia_authority_resolvers import build_authority_shadow_resolvers
from perizia_section_authority import build_section_authority_map


RUNS_ROOT = Path("/srv/perizia/_qa/runs")
FIXTURE_PATH = BACKEND_DIR / "tests" / "fixtures" / "perizia_authority_golden_cases.json"
SAMPLE_ROOTS = [RUNS_ROOT, Path("/srv/perizia/app/uploads"), Path("/home/syedtajmeelshah")]

STALE_VIA_UMBRIA_RE = re.compile(r"regolarizzazion\w*\s*:\s*(?:€|\beuro\b)?\s*(?:31|6)(?:[,\.]00)?\b", re.I)
PLACEHOLDER_RE = re.compile(r"\b(?:TBD|TODO|NOT_SPECIFIED|INTERNAL\s+DIRTY)\b|\{\{[^{}]+\}\}", re.I)
MONEY_AMOUNT_RE = re.compile(
    r"(?:€|\beuro\b)\s*\d{1,3}(?:\.\d{3})*(?:,\d{2})?|"
    r"\d{1,3}(?:\.\d{3})*(?:,\d{2})?\s*(?:€|\beuro\b)",
    re.I,
)

FORBIDDEN_CUSTOMER_KEYS = {
    "authority_money",
    "shadow_money",
    "authority_score",
    "authority_level",
    "section_zone",
    "domain_hints",
    "debug",
    "internal_runtime",
    "reason_for_authority",
}

TABLE_HEADERS = [
    "file",
    "analysis_id",
    "legacy_money_item_count",
    "legacy_buyer_cost_like_count",
    "authority_role_counts",
    "unsafe_legacy_signals",
    "authority_customer_safe_cost_count",
    "component_total_double_count_risk",
    "stale_via_umbria_money_risk",
    "placeholder_leak_risk",
    "customer_leak_count",
    "comparison_verdict",
    "notes",
]


def _read_json(path: Path, fallback: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return fallback


def _page_number(page: Dict[str, Any], default: int) -> int:
    for key in ("page_number", "page", "page_num"):
        try:
            parsed = int(page.get(key))
            if parsed > 0:
                return parsed
        except Exception:
            continue
    return default


def _normalize_pages(pages: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for idx, page in enumerate(pages or [], start=1):
        if isinstance(page, dict):
            out.append({"page_number": _page_number(page, idx), "text": str(page.get("text") or "")})
    out.sort(key=lambda row: row["page_number"])
    return out


def _all_text(pages: Sequence[Dict[str, Any]]) -> str:
    return "\n\n".join(str(page.get("text") or "") for page in pages if isinstance(page, dict))


def _flatten_text(value: Any) -> str:
    if isinstance(value, dict):
        return " ".join(_flatten_text(child) for child in value.values())
    if isinstance(value, list):
        return " ".join(_flatten_text(item) for item in value)
    return str(value or "")


def _customer_result_from_analysis(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    result = payload.get("result")
    return result if isinstance(result, dict) else {}


def _money_items_from_result(result: Dict[str, Any]) -> List[Any]:
    paths = [
        ("customer_decision_contract", "money_box", "items"),
        ("customer_decision_contract", "section_3_money_box", "items"),
        ("money_box", "items"),
        ("section_3_money_box", "items"),
    ]
    for path in paths:
        current: Any = result
        for part in path:
            current = current.get(part) if isinstance(current, dict) else None
        if isinstance(current, list):
            return current
    return []


def legacy_money_summary_from_result(result: Dict[str, Any]) -> Dict[str, Any]:
    items = _money_items_from_result(result)
    text = _flatten_text({"items": items, "money_box": result.get("money_box"), "section_3_money_box": result.get("section_3_money_box")})
    buyer_like = len(items)
    return {
        "legacy_money_item_count": len(items),
        "legacy_buyer_cost_like_count": buyer_like,
        "text": text,
        "source": "customer_result",
        "stale_via_umbria_money_risk": bool(STALE_VIA_UMBRIA_RE.search(text)),
        "placeholder_leak_risk": bool(PLACEHOLDER_RE.search(text)),
    }


def legacy_money_summary_from_pages(pages: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    text = _all_text(pages)
    buyer_like = len(re.findall(r"\b(spese|costi|oneri|sanzion|regolarizzazion|sanator|docfa|ripristin|condominial)\w*\b", text, re.I))
    return {
        "legacy_money_item_count": len(MONEY_AMOUNT_RE.findall(text)),
        "legacy_buyer_cost_like_count": buyer_like,
        "text": text,
        "source": "pages",
        "stale_via_umbria_money_risk": False,
        "placeholder_leak_risk": bool(PLACEHOLDER_RE.search(text)),
    }


def collect_customer_leaks(value: Any, path: str = "result") -> List[str]:
    hits: List[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            key_text = str(key)
            child_path = f"{path}.{key_text}"
            if key_text in FORBIDDEN_CUSTOMER_KEYS or key_text.startswith("authority_") or "shadow_" in key_text:
                hits.append(child_path)
            hits.extend(collect_customer_leaks(child, child_path))
    elif isinstance(value, list):
        for idx, item in enumerate(value):
            hits.extend(collect_customer_leaks(item, f"{path}[{idx}]"))
    return hits


def sanitized_customer_result(result: Dict[str, Any]) -> Dict[str, Any]:
    clone = json.loads(json.dumps(result, ensure_ascii=False, default=str))
    sanitize_customer_facing_result(clone)
    separate_internal_runtime_from_customer_result(clone)
    return clone


def _role_counts(money_value: Dict[str, Any]) -> Dict[str, int]:
    counts = money_value.get("money_role_counts")
    if isinstance(counts, dict):
        return {str(key): int(value or 0) for key, value in counts.items()}
    return {}


def _authority_money_value(shadow: Dict[str, Any]) -> Dict[str, Any]:
    row = shadow.get("money_roles") if isinstance(shadow, dict) else {}
    value = row.get("value") if isinstance(row, dict) else {}
    return value if isinstance(value, dict) else {}


def _unsafe_legacy_signals(
    legacy_text: str,
    money_value: Dict[str, Any],
    legacy_buyer_like_count: int,
    *,
    stale_label_check: bool = True,
) -> List[str]:
    text = legacy_text or ""
    role_counts = _role_counts(money_value)
    classes: List[str] = []
    if legacy_buyer_like_count <= 0:
        return classes
    if re.search(r"\brendita\s+catastale\b", text, re.I) and role_counts.get("cadastral_rendita", 0):
        classes.append("rendita_as_buyer_cost")
    if re.search(r"\bprezzo\s+base|base\s+d[' ]asta|offerta\s+minima\b", text, re.I) and (
        role_counts.get("base_auction", 0) or role_counts.get("price", 0)
    ):
        classes.append("price_as_buyer_cost")
    if re.search(r"\bvalore\s+(?:di\s+stima|finale|venale|commerciale)|\bstima\b", text, re.I) and (
        role_counts.get("market_value", 0) or role_counts.get("final_value", 0)
    ):
        classes.append("valuation_as_buyer_cost")
    if re.search(r"\bdeprezzament|decurtazion|abbattimento\b", text, re.I) and role_counts.get("valuation_deduction", 0):
        classes.append("deprezzamento_as_extra_cost")
    if re.search(r"\bformalita|ipotec|pignorament|trascrizion|iscrizion\b", text, re.I) and role_counts.get("formalities_procedural_amount", 0):
        classes.append("formalita_as_buyer_cost")
    if bool((money_value.get("summary") or {}).get("double_count_risk")):
        classes.append("component_total_double_count")
    if stale_label_check and STALE_VIA_UMBRIA_RE.search(text):
        classes.append("stale_money_projection")
    if PLACEHOLDER_RE.search(text):
        classes.append("placeholder_cost_leak")
    safe_count = int((money_value.get("summary") or {}).get("authority_customer_safe_cost_count") or 0)
    if legacy_buyer_like_count > safe_count and classes:
        classes.append("unsupported_buyer_cost_certainty")
    return list(dict.fromkeys(classes))


def _authority_warnings(money_value: Dict[str, Any]) -> List[str]:
    warnings: List[str] = []
    for candidate in money_value.get("money_candidates") or []:
        if isinstance(candidate, dict):
            warnings.extend(str(item) for item in candidate.get("warnings") or [])
            if candidate.get("reason_code") == "GENERIC_MONEY_BOILERPLATE":
                warnings.append("GENERIC_MONEY_BOILERPLATE")
    return list(dict.fromkeys(warnings))


def verdict_for_money(
    *,
    money_status: str,
    unsafe_signals: Sequence[str],
    customer_leaks: Sequence[str],
    money_value: Dict[str, Any],
) -> str:
    if customer_leaks:
        return "AUTHORITY_WORSE_THAN_LEGACY"
    candidates = money_value.get("money_candidates") if isinstance(money_value.get("money_candidates"), list) else []
    unsafe_authority_surface = [
        candidate
        for candidate in candidates
        if isinstance(candidate, dict)
        and candidate.get("should_surface_in_money_box")
        and candidate.get("role") not in {"buyer_cost_signal_to_verify", "condominium_arrears", "total_candidate"}
    ]
    unsafe_authority_sum = [
        candidate
        for candidate in candidates
        if isinstance(candidate, dict)
        and candidate.get("should_sum")
        and (candidate.get("parent_total_candidate_id") or candidate.get("role") == "component_of_total")
    ]
    if unsafe_authority_surface or unsafe_authority_sum:
        return "AUTHORITY_WORSE_THAN_LEGACY"
    if str(money_status) == "FAIL_OPEN":
        return "FAIL_OPEN_ACCEPTABLE"
    if unsafe_signals:
        return "AUTHORITY_BETTER_THAN_LEGACY"
    safe_count = int((money_value.get("summary") or {}).get("authority_customer_safe_cost_count") or 0)
    if safe_count == 0 and not candidates:
        return "INSUFFICIENT_EXPECTED_TRUTH"
    return "AUTHORITY_SAME_AS_LEGACY"


def _load_candidates_for_analysis(analysis_id: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    candidates_dir = RUNS_ROOT / analysis_id / "candidates"
    for key, filename in (("money", "candidates_money.json"), ("triggers", "candidates_triggers.json")):
        payload = _read_json(candidates_dir / filename)
        if isinstance(payload, list):
            out[key] = payload
        elif payload is not None:
            out[key] = payload
    return out


def _load_saved_analysis_from_files(analysis_id: str) -> Optional[Dict[str, Any]]:
    for path in (RUNS_ROOT / analysis_id / "analysis.json", RUNS_ROOT / analysis_id / "system.json", Path("/tmp/perizia_qa_run/analysis.json")):
        payload = _read_json(path)
        if isinstance(payload, dict) and str(payload.get("analysis_id") or "") == analysis_id:
            return payload
    return None


def _load_saved_analysis_from_mongo(analysis_id: str) -> Optional[Dict[str, Any]]:
    try:
        from dotenv import dotenv_values
        from pymongo import MongoClient

        cfg = dotenv_values(BACKEND_DIR / ".env")
        client = MongoClient(cfg.get("MONGO_URL"), serverSelectionTimeoutMS=1200)
        db = client[cfg.get("DB_NAME")]
        payload = db.perizia_analyses.find_one({"analysis_id": analysis_id}, {"_id": 0, "raw_text": 0})
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def load_saved_analysis(analysis_id: str) -> Optional[Dict[str, Any]]:
    return _load_saved_analysis_from_files(analysis_id) or _load_saved_analysis_from_mongo(analysis_id)


def _build_shadow_for_pages(
    pages: Sequence[Dict[str, Any]],
    *,
    analysis_id: str = "",
    notes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    notes = notes if notes is not None else []
    section_path = RUNS_ROOT / analysis_id / "extract" / "section_authority.json" if analysis_id else None
    section_map = _read_json(section_path) if section_path else None
    if not isinstance(section_map, dict):
        section_map = build_section_authority_map(list(pages))
        if analysis_id:
            notes.append("rebuilt_section_authority_from_pages")
    candidates = _load_candidates_for_analysis(analysis_id) if analysis_id else {}
    return build_authority_shadow_resolvers(pages, section_map, candidates=candidates)


def _row(
    *,
    file_label: str,
    analysis_id: str,
    pages: Sequence[Dict[str, Any]],
    result: Optional[Dict[str, Any]],
    notes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    notes = list(notes or [])
    shadow = _build_shadow_for_pages(pages, analysis_id=analysis_id, notes=notes)
    money_row = shadow.get("money_roles") if isinstance(shadow.get("money_roles"), dict) else {}
    money_value = _authority_money_value(shadow)
    legacy = legacy_money_summary_from_result(result or {}) if isinstance(result, dict) and result else legacy_money_summary_from_pages(pages)
    customer_result = sanitized_customer_result(result or {}) if isinstance(result, dict) else {}
    leaks = collect_customer_leaks(customer_result)
    unsafe = _unsafe_legacy_signals(
        str(legacy.get("text") or ""),
        money_value,
        int(legacy.get("legacy_buyer_cost_like_count") or 0),
        stale_label_check=str(legacy.get("source") or "") == "customer_result",
    )
    warnings = _authority_warnings(money_value)
    if "GENERIC_MONEY_BOILERPLATE" in warnings:
        unsafe.append("generic_money_boilerplate")
    if str(money_row.get("status") or "") == "FAIL_OPEN":
        unsafe.append("no_money_authority_fail_open")
    unsafe = list(dict.fromkeys(unsafe))
    verdict = verdict_for_money(
        money_status=str(money_row.get("status") or ""),
        unsafe_signals=unsafe,
        customer_leaks=leaks,
        money_value=money_value,
    )
    summary = money_value.get("summary") if isinstance(money_value.get("summary"), dict) else {}
    role_counts = _role_counts(money_value)
    all_notes = notes + [str(note) for note in money_row.get("notes") or []]
    if leaks:
        all_notes.append("customer_leaks=" + "|".join(leaks[:5]))
    return {
        "file": file_label,
        "analysis_id": analysis_id,
        "legacy_money_item_count": int(legacy.get("legacy_money_item_count") or 0),
        "legacy_buyer_cost_like_count": int(legacy.get("legacy_buyer_cost_like_count") or 0),
        "authority_role_counts": role_counts,
        "unsafe_legacy_signals": unsafe,
        "authority_customer_safe_cost_count": int(summary.get("authority_customer_safe_cost_count") or 0),
        "component_total_double_count_risk": bool(summary.get("double_count_risk")),
        "stale_via_umbria_money_risk": bool(legacy.get("stale_via_umbria_money_risk")),
        "placeholder_leak_risk": bool(legacy.get("placeholder_leak_risk")),
        "customer_leak_count": len(leaks),
        "customer_leak_paths": leaks,
        "money_status": str(money_row.get("status") or ""),
        "authority_safe_amounts": [
            candidate.get("amount_eur")
            for candidate in money_value.get("money_candidates") or []
            if isinstance(candidate, dict) and candidate.get("is_customer_safe_cost")
        ][:12],
        "authority_amounts_by_role": {
            role: [
                candidate.get("amount_eur")
                for candidate in money_value.get("money_candidates") or []
                if isinstance(candidate, dict) and candidate.get("role") == role
            ][:12]
            for role in sorted(role_counts)
        },
        "comparison_verdict": verdict,
        "notes": ";".join(dict.fromkeys(note for note in all_notes if note)),
    }


def compare_analysis_id(analysis_id: str) -> Dict[str, Any]:
    extract_dir = RUNS_ROOT / analysis_id / "extract"
    pages = _normalize_pages(_read_json(extract_dir / "pages_raw.json", []))
    saved = load_saved_analysis(analysis_id)
    result = _customer_result_from_analysis(saved)
    label = str((saved or {}).get("file_name") or (saved or {}).get("case_title") or analysis_id)
    notes = [] if pages else ["missing_extract_pages"]
    return _row(file_label=label, analysis_id=analysis_id, pages=pages, result=result, notes=notes)


def compare_pdf(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {
            "file": str(path),
            "analysis_id": "",
            "legacy_money_item_count": 0,
            "legacy_buyer_cost_like_count": 0,
            "authority_role_counts": {},
            "unsafe_legacy_signals": ["missing_pdf"],
            "authority_customer_safe_cost_count": 0,
            "component_total_double_count_risk": False,
            "stale_via_umbria_money_risk": False,
            "placeholder_leak_risk": False,
            "customer_leak_count": 0,
            "customer_leak_paths": [],
            "money_status": "FAIL_OPEN",
            "comparison_verdict": "FAIL_OPEN_ACCEPTABLE",
            "notes": f"missing_pdf:{path}",
        }
    pages = _normalize_pages(_extract_pdf_pages(path))
    return _row(file_label=str(path), analysis_id="", pages=pages, result=None, notes=[])


def _fixture_cases() -> List[Dict[str, Any]]:
    payload = _read_json(FIXTURE_PATH, [])
    return payload if isinstance(payload, list) else []


def _resolve_case_path(case: Dict[str, Any]) -> Optional[Path]:
    for raw in case.get("paths") or []:
        path = Path(str(raw)).expanduser()
        if path.exists() and path.is_file():
            return path.resolve()
    return None


def compare_corpus() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for case in _fixture_cases():
        analysis_ids = [str(item) for item in case.get("analysis_ids") or [] if str(item or "").strip()]
        if analysis_ids:
            rows.extend(compare_analysis_id(analysis_id) for analysis_id in analysis_ids)
            continue
        path = _resolve_case_path(case)
        if path is not None:
            rows.append(compare_pdf(path))
        else:
            rows.append(compare_pdf(Path(str((case.get("paths") or [case.get("label") or "missing"])[0]))))
    return rows


def _sample_sources(limit: int, seed: int) -> List[Tuple[str, str]]:
    sources: List[Tuple[str, str]] = []
    for path in sorted(RUNS_ROOT.glob("analysis_*/extract/pages_raw.json")):
        sources.append(("analysis", path.parents[1].name))
    for root in SAMPLE_ROOTS[1:]:
        if root.exists():
            for path in sorted(root.rglob("*.pdf")):
                sources.append(("file", str(path)))
    random.Random(seed).shuffle(sources)
    return sources[: max(0, limit)]


def compare_sample(limit: int, seed: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for kind, value in _sample_sources(limit, seed):
        if kind == "analysis":
            rows.append(compare_analysis_id(value))
        else:
            rows.append(compare_pdf(Path(value)))
    return rows


def _public_payload(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    verdict_counts: Dict[str, int] = {}
    error_counts: Dict[str, int] = {}
    for row in rows:
        verdict = str(row.get("comparison_verdict") or "")
        verdict_counts[verdict] = verdict_counts.get(verdict, 0) + 1
        for error in row.get("unsafe_legacy_signals") or []:
            error_counts[str(error)] = error_counts.get(str(error), 0) + 1
    return {"summary": {"total": len(rows), "verdict_counts": verdict_counts, "error_counts": error_counts}, "rows": rows}


def _print_table(rows: List[Dict[str, Any]]) -> None:
    print("\t".join(TABLE_HEADERS))
    for row in rows:
        printable = dict(row)
        printable["authority_role_counts"] = json.dumps(row.get("authority_role_counts") or {}, ensure_ascii=False, sort_keys=True)
        printable["unsafe_legacy_signals"] = ",".join(str(item) for item in row.get("unsafe_legacy_signals") or [])
        print("\t".join(str(printable.get(header, "")) for header in TABLE_HEADERS))


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Compare authority money roles against legacy Money Box output.")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--corpus", action="store_true")
    mode.add_argument("--file", dest="file_path")
    mode.add_argument("--analysis-id")
    mode.add_argument("--sample", type=int)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--json-out")
    args = parser.parse_args(argv)

    if args.corpus:
        rows = compare_corpus()
    elif args.file_path:
        rows = [compare_pdf(Path(args.file_path).expanduser())]
    elif args.analysis_id:
        rows = [compare_analysis_id(str(args.analysis_id).strip())]
    else:
        rows = compare_sample(int(args.sample or 0), int(args.seed))

    payload = _public_payload(rows)
    if args.json or args.json_out:
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        if args.json_out:
            out = Path(args.json_out).expanduser().resolve()
            out.parent.mkdir(parents=True, exist_ok=True)
            with open(out, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
    else:
        _print_table(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
