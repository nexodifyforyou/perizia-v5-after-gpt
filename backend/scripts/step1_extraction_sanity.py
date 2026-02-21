#!/usr/bin/env python3
import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from pymongo import MongoClient


SEARCH_DIRS = [
    Path("/srv/perizia/app/backend/uploads"),
    Path("/srv/perizia/app/uploads"),
    Path("/srv/perizia/uploads"),
    Path("/tmp"),
]

ANCHOR_PATTERNS = {
    "tribunale": re.compile(r"\bTRIBUNALE\s+DI\s+[A-ZÀ-Ü\s]{3,40}\b"),
    "rge_procedura": re.compile(
        r"\bR\.?\s*G\.?\s*E\.?\s*\d+/\d{4}\b|\bEsecuzione\s+Immobiliare\s+\d+/\d{4}\b",
        re.IGNORECASE,
    ),
    "prezzo_base": re.compile(r"Prezzo\s+base[^\n]{0,80}€\s*[\d\.\,]+", re.IGNORECASE),
    "address": re.compile(
        r"\b(Via|Viale|Piazza|Corso|Strada|Vicolo|Largo|Localit[aà])\b[^\n]{5,120}",
        re.IGNORECASE,
    ),
}


def _fail(message: str) -> None:
    print(f"FAIL: {message}")
    sys.exit(1)


def _run_checked(cmd: List[str]) -> str:
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
    except FileNotFoundError:
        _fail(f"Required tool not found: {cmd[0]}")
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        stdout = (exc.stdout or "").strip()
        details = stderr or stdout or "no stderr/stdout"
        _fail(f"Command failed ({' '.join(cmd)}): {details}")
    return proc.stdout


def _parse_pdf_pages(pdf_path: Path) -> int:
    out = _run_checked(["pdfinfo", str(pdf_path)])
    match = re.search(r"^Pages:\s+(\d+)\s*$", out, re.MULTILINE)
    if not match:
        _fail("Could not parse page count from pdfinfo output")
    pages = int(match.group(1))
    if pages <= 0:
        _fail(f"Invalid page count from pdfinfo: {pages}")
    return pages


def _load_backend_env() -> None:
    env_path = Path("/srv/perizia/app/backend/.env")
    if env_path.exists():
        load_dotenv(env_path)


def _resolve_db_collection():
    _load_backend_env()
    mongo_url = os.environ.get("MONGO_URL")
    db_name = os.environ.get("DB_NAME", "periziascan")
    if not mongo_url:
        _fail("MONGO_URL not set and required for --analysis-id lookup")
    client = MongoClient(mongo_url, serverSelectionTimeoutMS=4000)
    try:
        client.admin.command("ping")
    except Exception as exc:
        _fail(f"Cannot connect to MongoDB at MONGO_URL: {exc}")
    return client, client[db_name]["perizia_analyses"]


def _safe_path_candidate(value: Any) -> Optional[Path]:
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    p = Path(raw)
    if p.exists() and p.is_file():
        return p.resolve()
    return None


def _extract_path_from_doc(doc: Dict[str, Any]) -> Optional[Path]:
    candidates: List[Any] = []
    for key in (
        "pdf_path",
        "file_path",
        "input_path",
        "document_path",
        "source_pdf_path",
        "source_path",
        "upload_path",
        "path",
    ):
        if key in doc:
            candidates.append(doc.get(key))

    input_obj = doc.get("input")
    if isinstance(input_obj, dict):
        for key in ("pdf_path", "file_path", "input_path", "path"):
            candidates.append(input_obj.get(key))

    for item in candidates:
        p = _safe_path_candidate(item)
        if p is not None:
            return p
    return None


def _search_pdf_by_filename(file_name: str) -> Optional[Path]:
    target = Path(file_name).name
    matches: List[Tuple[float, Path]] = []
    for base_dir in SEARCH_DIRS:
        if not base_dir.exists():
            continue
        try:
            for found in base_dir.rglob(target):
                if found.is_file():
                    matches.append((found.stat().st_mtime, found.resolve()))
        except Exception:
            continue
    if not matches:
        return None
    matches.sort(key=lambda item: item[0], reverse=True)
    return matches[0][1]


def _resolve_pdf_path(args: argparse.Namespace) -> Tuple[Path, Optional[str]]:
    if args.pdf:
        p = Path(args.pdf).expanduser().resolve()
        if not p.exists() or not p.is_file():
            _fail(f"--pdf path not found: {p}")
        return p, Path(p).name

    analysis_doc: Optional[Dict[str, Any]] = None
    if args.analysis_id:
        client, collection = _resolve_db_collection()
        try:
            analysis_doc = collection.find_one({"analysis_id": args.analysis_id})
        finally:
            client.close()
        if not analysis_doc:
            _fail(f"analysis_id not found in perizia_analyses: {args.analysis_id}")

        doc_path = _extract_path_from_doc(analysis_doc)
        if doc_path is not None:
            return doc_path, analysis_doc.get("file_name")

        db_file_name = analysis_doc.get("file_name")
        if isinstance(db_file_name, str) and db_file_name.strip():
            found = _search_pdf_by_filename(db_file_name.strip())
            if found is not None:
                return found, db_file_name.strip()

    if args.file_name:
        found = _search_pdf_by_filename(args.file_name)
        if found is not None:
            return found, Path(args.file_name).name

    if args.analysis_id and analysis_doc is not None:
        _fail(
            "Could not resolve PDF from analysis_id. "
            "No valid stored path key found and filename search returned no matches."
        )
    _fail("Could not locate PDF. Provide --pdf, --analysis-id, or --file-name with a discoverable file.")


def _default_out_dir(args: argparse.Namespace, source_name: Optional[str]) -> Path:
    if args.out:
        return Path(args.out).expanduser().resolve()
    suffix = args.analysis_id or (Path(source_name).stem if source_name else "unknown_pdf")
    return Path("/srv/perizia/_qa/extract") / suffix


def _extract_page_text(pdf_path: Path, page_num: int) -> str:
    out = _run_checked(
        [
            "pdftotext",
            "-f",
            str(page_num),
            "-l",
            str(page_num),
            "-raw",
            str(pdf_path),
            "-",
        ]
    )
    return out.replace("\r\n", "\n").replace("\r", "\n")


def _quote_from_match(page_text: str, start: int, end: int, max_len: int = 200) -> str:
    left_nl = page_text.rfind("\n", 0, start)
    right_nl = page_text.find("\n", end)
    q_start = 0 if left_nl < 0 else left_nl + 1
    q_end = len(page_text) if right_nl < 0 else right_nl
    quote = page_text[q_start:q_end].strip()
    if len(quote) <= max_len:
        return quote
    center = max(0, start - q_start)
    half = max_len // 2
    s = max(0, center - half)
    e = min(len(quote), s + max_len)
    return quote[s:e].strip()


def _build_anchors(page_texts: List[str]) -> List[Dict[str, Any]]:
    anchors: List[Dict[str, Any]] = []
    for anchor_name, pattern in ANCHOR_PATTERNS.items():
        found = False
        for idx, text in enumerate(page_texts, start=1):
            match = pattern.search(text)
            if not match:
                continue
            start = match.start()
            end = match.end()
            anchors.append(
                {
                    "name": anchor_name,
                    "page": idx,
                    "quote": _quote_from_match(text, start, end, max_len=200),
                    "start_offset": start,
                    "end_offset": end,
                    "offset_mode": "PAGE_LOCAL",
                }
            )
            found = True
            break
        if not found:
            anchors.append(
                {
                    "name": anchor_name,
                    "page": None,
                    "quote": "",
                    "start_offset": None,
                    "end_offset": None,
                    "offset_mode": "PAGE_LOCAL",
                    "error": "NOT_FOUND",
                }
            )
    return anchors


def _build_metrics(page_texts: List[str]) -> Dict[str, Any]:
    chars_per_page: List[int] = []
    bad_pages: List[Dict[str, Any]] = []
    pages_ge_500 = 0
    pages_ge_200 = 0

    for idx, text in enumerate(page_texts, start=1):
        trimmed = text.strip()
        char_count = len(trimmed)
        chars_per_page.append(char_count)
        if char_count >= 500:
            pages_ge_500 += 1
        if char_count >= 200:
            pages_ge_200 += 1
        if char_count < 200:
            snippet = re.sub(r"\s+", " ", trimmed[:140]).strip()
            bad_pages.append({"page": idx, "chars": char_count, "head_snippet": snippet})

    total_pages = len(page_texts)
    coverage_ratio = (pages_ge_200 / total_pages) if total_pages else 0.0
    return {
        "total_pages": total_pages,
        "chars_per_page": chars_per_page,
        "coverage_ratio": coverage_ratio,
        "pages_ge_500": pages_ge_500,
        "bad_pages": bad_pages,
    }


def _write_json(path: Path, payload: Dict[str, Any] | List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Step1 extraction sanity: per-page digital text + metrics + anchors.")
    parser.add_argument("--analysis-id", dest="analysis_id")
    parser.add_argument("--pdf")
    parser.add_argument("--file-name", dest="file_name")
    parser.add_argument("--out")
    args = parser.parse_args()

    pdf_path, source_name = _resolve_pdf_path(args)
    out_dir = _default_out_dir(args, source_name)
    pages_dir = out_dir / "pages"
    pages_dir.mkdir(parents=True, exist_ok=True)

    total_pages_pdfinfo = _parse_pdf_pages(pdf_path)
    page_texts: List[str] = []
    for page_num in range(1, total_pages_pdfinfo + 1):
        page_text = _extract_page_text(pdf_path, page_num)
        page_texts.append(page_text)
        page_file = pages_dir / f"{page_num:03d}.txt"
        page_file.write_text(page_text, encoding="utf-8")

    metrics = _build_metrics(page_texts)
    anchors = _build_anchors(page_texts)
    _write_json(out_dir / "metrics.json", metrics)
    _write_json(out_dir / "anchors.json", anchors)

    anchors_by_name = {a.get("name"): a for a in anchors if isinstance(a, dict)}
    tribunale_found = bool(anchors_by_name.get("tribunale", {}).get("page"))
    total_pages_matches = metrics["total_pages"] == total_pages_pdfinfo
    coverage_ok = float(metrics.get("coverage_ratio", 0.0)) >= 0.95

    reasons: List[str] = []
    if not coverage_ok:
        reasons.append(
            f"coverage_ratio {metrics.get('coverage_ratio', 0.0):.4f} < 0.95 "
            f"(bad_pages={len(metrics.get('bad_pages', []))})"
        )
    if not tribunale_found:
        reasons.append("anchors.tribunale not found")
    if not total_pages_matches:
        reasons.append(
            f"total_pages mismatch: metrics={metrics.get('total_pages')} pdfinfo={total_pages_pdfinfo}"
        )

    print(f"PDF_PATH: {pdf_path}")
    print(f"OUT_FOLDER: {out_dir}")
    print(
        "SUMMARY: "
        + json.dumps(
            {
                "total_pages": metrics.get("total_pages"),
                "coverage_ratio": metrics.get("coverage_ratio"),
                "pages_ge_500": metrics.get("pages_ge_500"),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    print("ANCHORS: " + json.dumps(anchors, ensure_ascii=False))

    if reasons:
        print("FAIL: " + "; ".join(reasons))
        sys.exit(1)

    print("PASS: coverage_ratio>=0.95 AND anchors.tribunale exists AND total_pages matches pdfinfo")
    sys.exit(0)


if __name__ == "__main__":
    main()
