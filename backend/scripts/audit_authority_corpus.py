#!/usr/bin/env python3
import argparse
import json
import random
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from perizia_authority_resolvers import build_authority_shadow_resolvers  # noqa: E402
from perizia_section_authority import (  # noqa: E402
    AUTH_HIGH,
    AUTH_LOW,
    ZONE_CONTEXT,
    ZONE_FINAL_LOT,
    ZONE_FINAL_VALUATION,
    ZONE_FORMALITIES,
    ZONE_INSTRUCTION,
    ZONE_QUESTION,
    ZONE_TOC,
    build_section_authority_map,
    summarize_authority_map,
)


DEFAULT_DISCOVERY_PATHS = [
    Path("/home/syedtajmeelshah"),
    Path("/srv/perizia/app/uploads"),
    Path("/srv/perizia/app/backend/tests/fixtures/perizie"),
]
FIXTURE_PATH = BACKEND_DIR / "tests" / "fixtures" / "perizia_authority_golden_cases.json"

TABLE_HEADERS = [
    "file",
    "expected_lot_mode",
    "shadow_lot_mode",
    "legacy_lot_mode_if_available",
    "shadow_occupancy",
    "shadow_opponibilita",
    "money_role_counts",
    "formalities_table_detected",
    "instruction_leak_count",
    "fail_open",
    "warnings",
    "verdict",
    "pages_total",
    "instruction_pages_count",
    "answer_pages_count",
    "final_lot_formation_count",
    "final_valuation_count",
    "formalities_count",
    "high_authority_lotto_unico",
    "high_authority_multilot",
    "chapter_lot_numbers",
    "chapter_lot_start_pages",
    "contextual_lotto_mentions_count",
    "money_cost_signal_count",
    "money_valuation_count",
    "money_rendita_catastale_count",
    "instruction_false_positive_suspects_count",
    "status",
    "notes",
]


def _read_json(path: Path, fallback: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return fallback


def _load_golden_cases() -> List[Dict[str, Any]]:
    payload = _read_json(FIXTURE_PATH, [])
    return payload if isinstance(payload, list) else []


def _expected_lot_mode_from_case(case: Dict[str, Any]) -> str:
    expectations = case.get("expectations") if isinstance(case.get("expectations"), dict) else {}
    expected = str(expectations.get("expected_lot_mode") or "")
    if expected:
        return expected
    expected_class = str(case.get("expected_class") or "")
    if expectations.get("requires_high_lotto_unico") or "SINGLE_LOT" in expected_class:
        return "single_lot"
    if (
        len(expectations.get("requires_high_lot_numbers") or []) >= 2
        or len(expectations.get("requires_chapter_lot_numbers") or []) >= 2
        or int(expectations.get("expected_min_lots_detected") or 0) >= 2
        or "MULTI_LOT" in expected_class
    ):
        return "multi_lot"
    return ""


def _run_text_command(cmd: List[str]) -> Optional[str]:
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
    except Exception:
        return None
    return proc.stdout


def _extract_pdf_pages_with_cli(pdf_path: Path) -> List[Dict[str, Any]]:
    info = _run_text_command(["pdfinfo", str(pdf_path)])
    if not info:
        raise RuntimeError("Could not import PyPDF2 and pdfinfo is unavailable")
    match = re.search(r"^Pages:\s+(\d+)\s*$", info, flags=re.MULTILINE)
    if not match:
        raise RuntimeError("Could not parse page count from pdfinfo output")
    total_pages = int(match.group(1))
    pages: List[Dict[str, Any]] = []
    for idx in range(1, total_pages + 1):
        text = _run_text_command(["pdftotext", "-f", str(idx), "-l", str(idx), "-raw", str(pdf_path), "-"])
        pages.append({"page_number": idx, "text": text or "", "char_count": len(text or "")})
    return pages


def _extract_pdf_pages(pdf_path: Path) -> List[Dict[str, Any]]:
    try:
        from PyPDF2 import PdfReader

        reader = PdfReader(str(pdf_path))
    except Exception:
        return _extract_pdf_pages_with_cli(pdf_path)
    pages: List[Dict[str, Any]] = []
    for idx, page in enumerate(reader.pages, start=1):
        try:
            text = page.extract_text() or ""
        except Exception:
            text = ""
        pages.append({"page_number": idx, "text": text, "char_count": len(text)})
    return pages


def _path_pdfs(path: Path) -> List[Path]:
    expanded = path.expanduser()
    if expanded.is_file() and expanded.suffix.lower() == ".pdf":
        return [expanded.resolve()]
    if expanded.is_dir():
        return sorted((p.resolve() for p in expanded.rglob("*.pdf") if p.is_file()), key=lambda p: str(p).lower())
    if any(ch in str(expanded) for ch in "*?["):
        return sorted((p.resolve() for p in expanded.parent.glob(expanded.name) if p.is_file() and p.suffix.lower() == ".pdf"), key=lambda p: str(p).lower())
    return []


def _resolve_input_paths(paths: Optional[Sequence[str]]) -> List[Path]:
    roots = [Path(p) for p in paths] if paths else DEFAULT_DISCOVERY_PATHS
    found: Dict[str, Path] = {}
    for root in roots:
        for pdf in _path_pdfs(root):
            found[str(pdf)] = pdf
    return sorted(found.values(), key=lambda p: str(p).lower())


def _sample_paths(paths: List[Path], sample: Optional[int], seed: int) -> List[Path]:
    if sample is None or sample >= len(paths):
        return paths
    rng = random.Random(seed)
    shuffled = list(paths)
    rng.shuffle(shuffled)
    return sorted(shuffled[:sample], key=lambda p: str(p).lower())


def _page_num(page: Dict[str, Any], default: int) -> int:
    try:
        return int(page.get("page") or page.get("page_number") or default)
    except Exception:
        return default


def _page_text(page: Dict[str, Any]) -> str:
    return str(page.get("text") or "")


def _contains(text: str, pattern: str) -> bool:
    return bool(re.search(pattern, text or "", flags=re.IGNORECASE | re.UNICODE))


def _page_text_by_number(pages: List[Dict[str, Any]]) -> Dict[int, str]:
    return {_page_num(page, idx): _page_text(page) for idx, page in enumerate(pages, start=1) if isinstance(page, dict)}


def _as_int_list(value: Any) -> List[int]:
    if not isinstance(value, list):
        return []
    out: List[int] = []
    for item in value:
        try:
            out.append(int(item))
        except Exception:
            continue
    return out


def _lot_numbers_from_text(text: str) -> List[int]:
    numbers: List[int] = []
    for match in re.finditer(r"\blotto\s*(?:n\.?|nr\.?|numero)?\s*([1-9]\d*)\b", text or "", flags=re.IGNORECASE | re.UNICODE):
        try:
            number = int(match.group(1))
        except Exception:
            continue
        if number not in numbers:
            numbers.append(number)
    return numbers


def _expected_lot_mode_for_path(path: Path) -> str:
    resolved = str(path.resolve()) if path.exists() else str(path)
    for case in _load_golden_cases():
        for raw in case.get("paths") or []:
            candidate = Path(str(raw)).expanduser()
            if str(candidate) == resolved or (candidate.exists() and str(candidate.resolve()) == resolved):
                return _expected_lot_mode_from_case(case)
    name = path.name.lower()
    if "1859886" in name:
        return "single_lot"
    if "multilot" in name or "69-2024" in name or "69_2024" in name:
        return "multi_lot"
    return ""


def _legacy_lot_mode_if_available(high_lotto_unico: bool, high_lot_numbers: List[int]) -> str:
    if len(high_lot_numbers) >= 2:
        return "multi_lot"
    if high_lotto_unico:
        return "single_lot"
    return ""


def _shadow_value(shadow: Dict[str, Any], domain: str) -> Dict[str, Any]:
    row = shadow.get(domain) if isinstance(shadow, dict) else {}
    value = row.get("value") if isinstance(row, dict) else {}
    return value if isinstance(value, dict) else {}


def _shadow_fail_open(shadow: Dict[str, Any]) -> bool:
    if not isinstance(shadow, dict):
        return True
    if bool(shadow.get("fail_open")):
        return True
    for key in ("lot_structure", "occupancy", "opponibilita", "legal_formalities", "money_roles"):
        row = shadow.get(key)
        if isinstance(row, dict) and bool(row.get("fail_open")):
            return True
    return False


def _shadow_warnings(shadow: Dict[str, Any]) -> List[str]:
    if not isinstance(shadow, dict):
        return ["shadow_unavailable"]
    warnings = [str(item) for item in shadow.get("warnings") or [] if str(item or "").strip()]
    for key in ("lot_structure", "occupancy", "opponibilita", "legal_formalities", "money_roles"):
        row = shadow.get(key)
        if not isinstance(row, dict):
            warnings.append(f"{key}:missing")
            continue
        if row.get("status") not in {"OK", None}:
            warnings.append(f"{key}:{row.get('status')}")
    return list(dict.fromkeys(warnings))


def _instruction_leak_count(shadow: Dict[str, Any]) -> int:
    opp = _shadow_value(shadow, "opponibilita")
    legal = _shadow_value(shadow, "legal_formalities")
    return len(opp.get("instruction_only_mentions") or []) + len(legal.get("instruction_only_legal_mentions") or [])


def _shadow_verdict(base_status: str, expected_lot_mode: str, shadow: Dict[str, Any]) -> str:
    if base_status == "FAIL":
        return "FAIL"
    warnings = _shadow_warnings(shadow)
    lot_mode = str(_shadow_value(shadow, "lot_structure").get("shadow_lot_mode") or "")
    if expected_lot_mode and lot_mode and lot_mode != "unknown" and lot_mode != expected_lot_mode:
        return "FAIL"
    if expected_lot_mode and lot_mode == expected_lot_mode and not _shadow_fail_open(shadow) and base_status == "PASS":
        return "PASS"
    if _shadow_fail_open(shadow) or warnings or base_status == "WARN":
        return "WARN"
    return "PASS"


def _authority_status(
    pages_total: int,
    unknown_count: int,
    major_section_count: int,
    unsafe_low_zone_high_count: int,
    notes: List[str],
) -> str:
    if pages_total <= 0:
        notes.append("no_pages")
        return "FAIL"
    if unsafe_low_zone_high_count:
        notes.append(f"low_zone_high_authority={unsafe_low_zone_high_count}")
        return "FAIL"
    if pages_total and unknown_count / pages_total > 0.75:
        notes.append("mostly_unknown")
        return "WARN"
    if major_section_count == 0:
        notes.append("no_major_authority_sections")
        return "WARN"
    return "PASS"


def _audit_pdf(path: Path) -> Dict[str, Any]:
    try:
        pages = _extract_pdf_pages(path)
    except Exception as exc:
        return {
            "file": str(path),
            "expected_lot_mode": _expected_lot_mode_for_path(path),
            "shadow_lot_mode": "",
            "legacy_lot_mode_if_available": "",
            "shadow_occupancy": "",
            "shadow_opponibilita": "",
            "money_role_counts": "{}",
            "formalities_table_detected": "NO",
            "instruction_leak_count": 0,
            "fail_open": "YES",
            "warnings": f"extract_failed:{str(exc)[:120]}",
            "verdict": "FAIL",
            "pages_total": 0,
            "instruction_pages_count": 0,
            "answer_pages_count": 0,
            "final_lot_formation_count": 0,
            "final_valuation_count": 0,
        "formalities_count": 0,
        "high_authority_lotto_unico": "NO",
        "high_authority_multilot": "NO",
        "chapter_lot_numbers": "[]",
        "chapter_lot_start_pages": "[]",
        "contextual_lotto_mentions_count": 0,
            "money_cost_signal_count": 0,
            "money_valuation_count": 0,
            "money_rendita_catastale_count": 0,
            "instruction_false_positive_suspects_count": 0,
            "status": "FAIL",
            "notes": f"extract_failed:{str(exc)[:120]}",
        }

    section_map = build_section_authority_map(pages)
    summary = summarize_authority_map(section_map)
    section_pages = [row for row in section_map.get("pages", []) if isinstance(row, dict)]
    page_texts = _page_text_by_number(pages)

    high_lotto_unico = False
    high_lot_numbers: List[int] = []
    contextual_lotto_mentions = 0
    money_cost_signal_pages: List[int] = []
    money_valuation_pages: List[int] = []
    money_rendita_pages: List[int] = []
    instruction_false_positive_suspects = 0
    unsafe_low_zone_high = 0

    for row in section_pages:
        page_num = int(row.get("page") or 0)
        text = page_texts.get(page_num, "")
        zone = str(row.get("zone") or "")
        level = str(row.get("authority_level") or "")
        hints = set(row.get("domain_hints") or [])

        if zone in {ZONE_TOC, ZONE_INSTRUCTION, ZONE_QUESTION} and level != AUTH_LOW:
            unsafe_low_zone_high += 1
        if row.get("is_instruction_like") and zone not in {ZONE_INSTRUCTION, ZONE_QUESTION} and level == AUTH_HIGH:
            instruction_false_positive_suspects += 1
        if _contains(text, r"\blotto\s+unico\b") and zone == ZONE_FINAL_LOT and level == AUTH_HIGH:
            high_lotto_unico = True
        if zone == ZONE_FINAL_LOT and level == AUTH_HIGH:
            for number in _lot_numbers_from_text(text):
                if number not in high_lot_numbers:
                    high_lot_numbers.append(number)
        if _contains(text, r"\blotto\b") and (zone == ZONE_CONTEXT or "procedure_context" in hints):
            contextual_lotto_mentions += 1
        if "money_cost_signal" in hints:
            money_cost_signal_pages.append(page_num)
        if "money_valuation" in hints or "money_price" in hints:
            money_valuation_pages.append(page_num)
        if "money_rendita_catastale" in hints:
            money_rendita_pages.append(page_num)

    pages_total = int(summary.get("pages_total") or 0)
    final_lot_pages = _as_int_list(summary.get("final_lot_formation_pages"))
    final_valuation_pages = _as_int_list(summary.get("final_valuation_pages"))
    formalities_pages = _as_int_list(summary.get("formalities_pages"))
    major_section_count = (
        len(summary.get("instruction_pages") or [])
        + len(summary.get("answer_pages") or [])
        + len(final_lot_pages)
        + len(final_valuation_pages)
        + len(formalities_pages)
    )
    notes_parts: List[str] = []
    if pages_total and not final_lot_pages:
        notes_parts.append("no_final_lot_formation")
    if pages_total and not final_valuation_pages:
        notes_parts.append("no_final_valuation")
    status = _authority_status(
        pages_total,
        len(summary.get("unknown_pages") or []),
        major_section_count,
        unsafe_low_zone_high,
        notes_parts,
    )
    if instruction_false_positive_suspects:
        notes_parts.append(f"instruction_suspects={instruction_false_positive_suspects}")

    high_lot_numbers.sort()
    shadow = build_authority_shadow_resolvers(pages, section_map)
    shadow_lot_value = _shadow_value(shadow, "lot_structure")
    shadow_occupancy_value = _shadow_value(shadow, "occupancy")
    shadow_opponibilita_value = _shadow_value(shadow, "opponibilita")
    shadow_legal_value = _shadow_value(shadow, "legal_formalities")
    shadow_money_value = _shadow_value(shadow, "money_roles")
    expected_lot_mode = _expected_lot_mode_for_path(path)
    shadow_warnings = _shadow_warnings(shadow)
    verdict = _shadow_verdict(status, expected_lot_mode, shadow)
    row = {
        "file": str(path),
        "expected_lot_mode": expected_lot_mode,
        "shadow_lot_mode": shadow_lot_value.get("shadow_lot_mode", ""),
        "legacy_lot_mode_if_available": _legacy_lot_mode_if_available(high_lotto_unico, high_lot_numbers),
        "shadow_occupancy": shadow_occupancy_value.get("shadow_occupancy_status", ""),
        "shadow_opponibilita": shadow_opponibilita_value.get("shadow_opponibilita_status", ""),
        "money_role_counts": json.dumps(shadow_money_value.get("money_role_counts") or {}, ensure_ascii=False, sort_keys=True),
        "formalities_table_detected": "YES"
        if shadow_legal_value.get("formalities_to_cancel") or shadow_legal_value.get("surviving_formalities")
        else "NO",
        "instruction_leak_count": _instruction_leak_count(shadow),
        "fail_open": "YES" if _shadow_fail_open(shadow) else "NO",
        "warnings": ",".join(shadow_warnings),
        "verdict": verdict,
        "pages_total": pages_total,
        "instruction_pages_count": len(summary.get("instruction_pages") or []),
        "answer_pages_count": len(summary.get("answer_pages") or []),
        "final_lot_formation_count": len(final_lot_pages),
        "final_valuation_count": len(final_valuation_pages),
        "formalities_count": len(formalities_pages),
        "high_authority_lotto_unico": "YES" if high_lotto_unico else "NO",
        "high_authority_multilot": "YES" if shadow_lot_value.get("has_high_authority_multilot") else "NO",
        "chapter_lot_numbers": json.dumps(shadow_lot_value.get("chapter_lot_numbers") or [], ensure_ascii=False),
        "chapter_lot_start_pages": json.dumps(shadow_lot_value.get("chapter_lot_start_pages") or [], ensure_ascii=False),
        "contextual_lotto_mentions_count": contextual_lotto_mentions,
        "money_cost_signal_count": len(set(money_cost_signal_pages)),
        "money_valuation_count": len(set(money_valuation_pages)),
        "money_rendita_catastale_count": len(set(money_rendita_pages)),
        "instruction_false_positive_suspects_count": instruction_false_positive_suspects,
        "status": status,
        "notes": ",".join(dict.fromkeys(part for part in notes_parts if part)),
        "final_lot_formation_pages": final_lot_pages,
        "final_valuation_pages": final_valuation_pages,
        "formalities_pages": formalities_pages,
        "high_authority_lot_numbers": high_lot_numbers,
        "money_cost_signal_pages": sorted(set(money_cost_signal_pages)),
        "money_valuation_pages": sorted(set(money_valuation_pages)),
        "money_rendita_catastale_pages": sorted(set(money_rendita_pages)),
        "authority_shadow_resolvers": shadow,
    }
    return row


def _print_table(rows: List[Dict[str, Any]]) -> None:
    print("\t".join(TABLE_HEADERS))
    for row in rows:
        print("\t".join(str(row.get(header, "")) for header in TABLE_HEADERS))


def _write_json(path: Path, rows: List[Dict[str, Any]]) -> None:
    summary = {
        "total": len(rows),
        "pass": sum(1 for row in rows if row.get("status") == "PASS"),
        "warn": sum(1 for row in rows if row.get("status") == "WARN"),
        "fail": sum(1 for row in rows if row.get("status") == "FAIL"),
    }
    payload = {"summary": summary, "rows": rows}
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Corpus audit for Perizia section authority maps.")
    parser.add_argument("files", nargs="*", help="Explicit PDF files to audit.")
    parser.add_argument("--file", dest="single_file", help="Audit one explicit PDF file.")
    parser.add_argument("--paths", nargs="*", help="PDF files or directories to scan. Defaults to the known local corpus roots.")
    parser.add_argument("--sample", type=int, default=None, help="Deterministic sample size after path discovery.")
    parser.add_argument("--seed", type=int, default=42, help="Seed used with --sample.")
    parser.add_argument("--limit", type=int, default=None, help="Backwards-compatible alias for auditing the first N discovered PDFs.")
    parser.add_argument("--json-out", help="Optional JSON output path.")
    args = parser.parse_args()

    requested_paths: List[str] = []
    if args.single_file:
        requested_paths.append(args.single_file)
    if args.paths:
        requested_paths.extend(args.paths)
    if args.files:
        requested_paths.extend(args.files)
    pdfs = _resolve_input_paths(requested_paths or None)
    if args.sample is not None:
        pdfs = _sample_paths(pdfs, args.sample, args.seed)
    elif args.limit is not None:
        pdfs = pdfs[: args.limit]

    rows = [_audit_pdf(path) for path in pdfs]
    _print_table(rows)
    if args.json_out:
        _write_json(Path(args.json_out).expanduser().resolve(), rows)

    if any(row.get("status") == "FAIL" for row in rows):
        sys.exit(1)


if __name__ == "__main__":
    main()
