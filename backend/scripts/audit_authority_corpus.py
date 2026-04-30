#!/usr/bin/env python3
import argparse
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from perizia_section_authority import (  # noqa: E402
    build_section_authority_map,
    summarize_authority_map,
)


DISCOVERY_GLOBS = [
    "/home/syedtajmeelshah/*.pdf",
    "/srv/perizia/app/uploads/*.pdf",
    "/srv/perizia/app/backend/tests/fixtures/perizie/*.pdf",
]


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
        text = text or ""
        pages.append({"page_number": idx, "text": text, "char_count": len(text)})
    return pages


def _discover_pdfs(limit: int | None = None) -> List[Path]:
    found: Dict[str, Path] = {}
    for pattern in DISCOVERY_GLOBS:
        for path in Path("/").glob(pattern.lstrip("/")):
            if path.is_file():
                found[str(path.resolve())] = path.resolve()
    ordered = sorted(found.values(), key=lambda p: str(p).lower())
    if limit is not None:
        return ordered[:limit]
    return ordered


def _page_text(page: Dict[str, Any]) -> str:
    return str(page.get("text") or "")


def _contains(text: str, pattern: str) -> bool:
    return bool(re.search(pattern, text or "", flags=re.IGNORECASE | re.UNICODE))


def _page_num(page: Dict[str, Any], default: int) -> int:
    try:
        return int(page.get("page") or page.get("page_number") or default)
    except Exception:
        return default


def _audit_pdf(path: Path) -> Dict[str, Any]:
    try:
        pages = _extract_pdf_pages(path)
    except Exception as exc:
        return {
            "file": str(path),
            "pages": 0,
            "instruction_pages_count": 0,
            "answer_pages_count": 0,
            "final_valuation_pages": "",
            "final_lot_formation_pages": "",
            "formalities_pages": "",
            "has_lotto_unico_high_authority": "NO",
            "contextual_lotto_mentions_count": 0,
            "occupancy_answer_pages": "",
            "money_answer_pages": "",
            "money_cost_signal_pages": "",
            "money_valuation_pages": "",
            "money_rendita_pages": "",
            "answer_point_count": 0,
            "instruction_false_positive_suspects": 0,
            "authority_map_status": "FAIL",
            "notes": f"extract_failed:{str(exc)[:80]}",
        }

    section_map = build_section_authority_map(pages)
    summary = summarize_authority_map(section_map)
    section_pages = section_map.get("pages") if isinstance(section_map.get("pages"), list) else []
    page_text_by_num = {_page_num(page, idx): _page_text(page) for idx, page in enumerate(pages, start=1)}

    high_lotto_unico = False
    contextual_lotto_mentions = 0
    occupancy_answer_pages: List[int] = []
    money_answer_pages: List[int] = []
    money_cost_signal_pages: List[int] = []
    money_valuation_pages: List[int] = []
    money_rendita_pages: List[int] = []
    instruction_false_positive_suspects = 0
    answer_point_count = 0

    for row in section_pages:
        if not isinstance(row, dict):
            continue
        page_num = int(row.get("page") or 0)
        text = page_text_by_num.get(page_num, "")
        zone = str(row.get("zone") or "")
        level = str(row.get("authority_level") or "")
        hints = set(row.get("domain_hints") or [])
        if row.get("answer_point") is not None:
            answer_point_count += 1
        if row.get("is_instruction_like") and zone not in {"INSTRUCTION_BLOCK", "QUESTION_BLOCK"} and level == "HIGH_FACTUAL":
            instruction_false_positive_suspects += 1

        if _contains(text, r"\blotto\s+unico\b") and level == "HIGH_FACTUAL":
            high_lotto_unico = True
        if _contains(text, r"\blotto\s+(n\.?\s*)?\d+\b") and ("procedure_context" in hints or zone == "ANNEX_OR_CONTEXT"):
            contextual_lotto_mentions += 1
        if "occupancy" in hints and zone in {"ANSWER_BLOCK", "FINAL_LOT_FORMATION", "FINAL_VALUATION"}:
            occupancy_answer_pages.append(page_num)
        if any(hint.startswith("money_") for hint in hints) and zone in {"ANSWER_BLOCK", "FINAL_LOT_FORMATION", "FINAL_VALUATION"}:
            money_answer_pages.append(page_num)
        if "money_cost_signal" in hints:
            money_cost_signal_pages.append(page_num)
        if "money_valuation" in hints or "money_price" in hints:
            money_valuation_pages.append(page_num)
        if "money_rendita_catastale" in hints:
            money_rendita_pages.append(page_num)

    unknown_count = len(summary.get("unknown_pages") or [])
    pages_total = int(summary.get("pages_total") or 0)
    notes_parts: List[str] = []
    if pages_total and not summary.get("final_valuation_pages"):
        notes_parts.append("no_final_valuation")
    if pages_total and not summary.get("final_lot_formation_pages"):
        notes_parts.append("no_final_lot_formation")
    if instruction_false_positive_suspects:
        notes_parts.append(f"instruction_suspects={instruction_false_positive_suspects}")

    if pages_total <= 0:
        status = "FAIL"
        notes = "no_pages"
    elif pages_total and unknown_count / pages_total > 0.75:
        status = "WARN"
        notes_parts.append("mostly_unknown")
        notes = ",".join(notes_parts)
    elif not (
        summary.get("instruction_pages")
        or summary.get("answer_pages")
        or summary.get("final_valuation_pages")
        or summary.get("final_lot_formation_pages")
        or summary.get("formalities_pages")
    ):
        status = "WARN"
        notes_parts.append("no_major_authority_sections")
        notes = ",".join(notes_parts)
    elif notes_parts and ("no_final_valuation" in notes_parts and "no_final_lot_formation" in notes_parts):
        status = "WARN"
        notes = ",".join(notes_parts)
    else:
        status = "PASS"
        notes = ",".join(notes_parts)

    return {
        "file": str(path),
        "pages": pages_total,
        "instruction_pages_count": len(summary.get("instruction_pages") or []),
        "answer_pages_count": len(summary.get("answer_pages") or []),
        "final_valuation_pages": ",".join(str(p) for p in summary.get("final_valuation_pages") or []),
        "final_lot_formation_pages": ",".join(str(p) for p in summary.get("final_lot_formation_pages") or []),
        "formalities_pages": ",".join(str(p) for p in summary.get("formalities_pages") or []),
        "has_lotto_unico_high_authority": "YES" if high_lotto_unico else "NO",
        "contextual_lotto_mentions_count": contextual_lotto_mentions,
        "occupancy_answer_pages": ",".join(str(p) for p in sorted(set(occupancy_answer_pages))),
        "money_answer_pages": ",".join(str(p) for p in sorted(set(money_answer_pages))),
        "money_cost_signal_pages": ",".join(str(p) for p in sorted(set(money_cost_signal_pages))),
        "money_valuation_pages": ",".join(str(p) for p in sorted(set(money_valuation_pages))),
        "money_rendita_pages": ",".join(str(p) for p in sorted(set(money_rendita_pages))),
        "answer_point_count": answer_point_count,
        "instruction_false_positive_suspects": instruction_false_positive_suspects,
        "authority_map_status": status,
        "notes": notes,
    }


def _print_table(rows: List[Dict[str, Any]]) -> None:
    headers = [
        "file",
        "pages",
        "instruction_pages_count",
        "answer_pages_count",
        "final_valuation_pages",
        "final_lot_formation_pages",
        "formalities_pages",
        "has_lotto_unico_high_authority",
        "contextual_lotto_mentions_count",
        "occupancy_answer_pages",
        "money_answer_pages",
        "money_cost_signal_pages",
        "money_valuation_pages",
        "money_rendita_pages",
        "answer_point_count",
        "instruction_false_positive_suspects",
        "authority_map_status",
        "notes",
    ]
    print("\t".join(headers))
    for row in rows:
        print("\t".join(str(row.get(header, "")) for header in headers))


def main() -> None:
    parser = argparse.ArgumentParser(description="Corpus audit for Perizia section authority maps.")
    parser.add_argument("--limit", type=int, default=None, help="Optional maximum number of PDFs to audit.")
    args = parser.parse_args()

    pdfs = _discover_pdfs(limit=args.limit)
    rows = [_audit_pdf(path) for path in pdfs]
    _print_table(rows)


if __name__ == "__main__":
    main()
