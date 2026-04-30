#!/usr/bin/env python3
import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from perizia_section_authority import build_section_authority_map, summarize_authority_map  # noqa: E402


RUNS_DIR = Path("/srv/perizia/_qa/runs")


def _fail(message: str) -> None:
    print(f"FAIL: {message}")
    sys.exit(1)


def _load_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _run_text_command(cmd: List[str]) -> Optional[str]:
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
    except Exception:
        return None
    return proc.stdout


def _extract_pdf_pages_with_cli(pdf_path: Path) -> List[Dict[str, Any]]:
    info = _run_text_command(["pdfinfo", str(pdf_path)])
    if not info:
        _fail("Could not import PyPDF2 and pdfinfo is unavailable")
    match = re.search(r"^Pages:\s+(\d+)\s*$", info, flags=re.MULTILINE)
    if not match:
        _fail("Could not parse page count from pdfinfo output")
    total_pages = int(match.group(1))
    pages: List[Dict[str, Any]] = []
    for idx in range(1, total_pages + 1):
        text = _run_text_command(["pdftotext", "-f", str(idx), "-l", str(idx), "-raw", str(pdf_path), "-"])
        text = text or ""
        pages.append({"page_number": idx, "text": text, "char_count": len(text)})
    return pages


def _extract_pdf_pages(pdf_path: Path) -> List[Dict[str, Any]]:
    try:
        from PyPDF2 import PdfReader

        reader = PdfReader(str(pdf_path))
    except Exception as exc:
        return _extract_pdf_pages_with_cli(pdf_path)
    pages: List[Dict[str, Any]] = []
    for idx, page in enumerate(reader.pages, start=1):
        try:
            text = page.extract_text() or ""
        except Exception:
            text = ""
        pages.append({"page_number": idx, "text": text, "char_count": len(text)})
    return pages


def _load_pages_from_analysis(analysis_id: str) -> List[Dict[str, Any]]:
    path = RUNS_DIR / analysis_id / "extract" / "pages_raw.json"
    if not path.exists():
        _fail(f"pages_raw.json not found for analysis_id={analysis_id}: {path}")
    data = _load_json(path)
    if not isinstance(data, list):
        _fail(f"pages_raw.json must contain a list: {path}")
    return [row for row in data if isinstance(row, dict)]


def _page_texts(pages: List[Dict[str, Any]]) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for idx, page in enumerate(pages, start=1):
        try:
            page_num = int(page.get("page") or page.get("page_number") or idx)
        except Exception:
            page_num = idx
        out[page_num] = str(page.get("text") or "")
    return out


def _find_pages(page_texts: Dict[int, str], pattern: str) -> List[int]:
    rx = re.compile(pattern, flags=re.IGNORECASE | re.UNICODE)
    return [page for page, text in sorted(page_texts.items()) if rx.search(text or "")]


def _default_output(args: argparse.Namespace) -> Path:
    if args.output:
        return Path(args.output).expanduser().resolve()
    if args.analysis_id:
        suffix = args.analysis_id
    else:
        suffix = Path(args.pdf).stem
    return Path("/tmp") / f"section_authority_{suffix}.json"


def _print_summary(section_map: Dict[str, Any], pages: List[Dict[str, Any]]) -> None:
    summary = summarize_authority_map(section_map)
    texts = _page_texts(pages)
    lotto_unico_pages = _find_pages(texts, r"\blotto\s+unico\b")
    lotto_n_pages = _find_pages(texts, r"\blotto\s+(n\.?\s*)?\d+\b")
    occupancy_pages = _find_pages(texts, r"\b(stato\s+di\s+possesso|occupat[oaie]|liber[oaie]|debitore|debitori)\b")
    money_pages = _find_pages(texts, r"(€|\beuro\b|\bspes[ae]\b|\bcost[oi]\b|\boneri?\b|\bsanzion[ei]\b)")
    section_pages = [p for p in section_map.get("pages", []) if isinstance(p, dict)]
    answer_point_pages = [int(p.get("page")) for p in section_pages if p.get("answer_point") is not None]
    money_role_pages = {
        role: [
            int(p.get("page"))
            for p in section_pages
            if role in (p.get("domain_hints") or [])
        ]
        for role in (
            "money_cost_signal",
            "money_valuation",
            "money_rendita_catastale",
            "money_formalities",
            "money_price",
            "money_unknown",
        )
    }
    instruction_false_positive_suspects = [
        int(p.get("page"))
        for p in section_pages
        if p.get("is_instruction_like")
        and p.get("zone") not in {"INSTRUCTION_BLOCK", "QUESTION_BLOCK"}
        and p.get("authority_level") == "HIGH_FACTUAL"
    ]

    print("Authority summary")
    print(f"- pages_total: {summary.get('pages_total')}")
    print(f"- instruction_pages: {summary.get('instruction_pages')}")
    print(f"- answer_pages: {summary.get('answer_pages')}")
    print(f"- final_valuation_pages: {summary.get('final_valuation_pages')}")
    print(f"- final_lot_formation_pages: {summary.get('final_lot_formation_pages')}")
    print(f"- formalities_pages: {summary.get('formalities_pages')}")
    print(f"- pages_mentioning_lotto_unico: {lotto_unico_pages}")
    print(f"- pages_mentioning_lotto_n: {lotto_n_pages}")
    print(f"- pages_mentioning_occupancy_terms: {occupancy_pages}")
    print(f"- pages_mentioning_money_cost_terms: {money_pages}")
    print(f"- answer_point_pages: {answer_point_pages}")
    for role, pages_for_role in money_role_pages.items():
        print(f"- {role}_pages: {pages_for_role}")
    print(f"- instruction_false_positive_suspects: {instruction_false_positive_suspects}")
    print("")
    print("Page zones")
    for page in section_map.get("pages", []):
        if not isinstance(page, dict):
            continue
        hints = ",".join(page.get("domain_hints") or [])
        print(
            f"{int(page.get('page') or 0):>3}  "
            f"{str(page.get('zone') or ''):<24} "
            f"{str(page.get('authority_level') or ''):<16} "
            f"{float(page.get('authority_score') or 0.0):.2f}  "
            f"{hints}  "
            f"{page.get('reason')}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit Perizia section authority map for a PDF or saved analysis.")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--pdf", help="Path to a PDF file.")
    source.add_argument("--analysis-id", dest="analysis_id", help="Analysis id with saved pages_raw.json.")
    parser.add_argument("--output", "-o", help="Optional JSON output path.")
    args = parser.parse_args()

    if args.pdf:
        pdf_path = Path(args.pdf).expanduser().resolve()
        if not pdf_path.exists() or not pdf_path.is_file():
            _fail(f"PDF not found: {pdf_path}")
        pages = _extract_pdf_pages(pdf_path)
    else:
        pages = _load_pages_from_analysis(args.analysis_id)

    section_map = build_section_authority_map(pages)
    out_path = _default_output(args)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(section_map, f, ensure_ascii=False, indent=2)

    _print_summary(section_map, pages)
    print("")
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
