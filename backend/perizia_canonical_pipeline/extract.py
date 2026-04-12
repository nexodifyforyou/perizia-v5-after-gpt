from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Dict, List, Tuple

import fitz
from PyPDF2 import PdfReader

from .runner import build_context
from .corpus_registry import list_case_keys


ITALIAN_HINT_WORDS = {
    "tribunale", "esecuzione", "immobile", "immobili", "lotto", "lotti", "bene", "beni",
    "corpo", "corpi", "foglio", "mappale", "sub", "categoria", "rendita", "valore",
    "stima", "prezzo", "occupazione", "proprietà", "proprieta", "diritto", "quota",
    "ubicazione", "indirizzo", "condominio", "catastale", "urbanistica", "edilizia",
    "procedura", "debitore", "creditore", "pignoramento", "vendita"
}


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _extract_with_pypdf2(pdf_path: Path) -> List[str]:
    reader = PdfReader(str(pdf_path))
    out: List[str] = []
    for page in reader.pages:
        text = page.extract_text() or ""
        out.append(text)
    return out


def _extract_with_fitz(pdf_path: Path) -> List[str]:
    doc = fitz.open(str(pdf_path))
    out: List[str] = []
    for page in doc:
        text = page.get_text("text") or ""
        out.append(text)
    doc.close()
    return out


def _choose_page_text(p1: str, p2: str) -> Tuple[str, str]:
    s1 = (p1 or "").strip()
    s2 = (p2 or "").strip()
    if len(s2) > len(s1):
        return p2 or "", "fitz"
    return p1 or "", "PyPDF2"


def _readability_metrics(text: str) -> Dict[str, float | int]:
    raw = text or ""
    stripped = raw.strip()

    total = len(stripped)
    if total == 0:
        return {
            "text_length": 0,
            "alpha_ratio": 0.0,
            "digit_ratio": 0.0,
            "space_ratio": 0.0,
            "printable_ratio": 0.0,
            "italian_hint_hits": 0,
            "weird_symbol_ratio": 1.0,
            "gibberish_score": 1.0,
        }

    alpha = sum(ch.isalpha() for ch in stripped)
    digits = sum(ch.isdigit() for ch in stripped)
    spaces = sum(ch.isspace() for ch in stripped)
    printable = sum(ch.isprintable() for ch in stripped)

    weird = 0
    for ch in stripped:
        if ch.isalnum() or ch.isspace():
            continue
        if ch in ".,;:!?%€/\\-–—()[]{}\"'°àèéìòùÀÈÉÌÒÙ":
            continue
        weird += 1

    words = re.findall(r"[A-Za-zÀ-ÖØ-öø-ÿ]{3,}", stripped.lower())
    hint_hits = sum(1 for w in words if w in ITALIAN_HINT_WORDS)

    alpha_ratio = alpha / total
    digit_ratio = digits / total
    space_ratio = spaces / total
    printable_ratio = printable / total
    weird_symbol_ratio = weird / total

    gibberish_score = 0.0
    if alpha_ratio < 0.45:
        gibberish_score += 0.35
    if printable_ratio < 0.98:
        gibberish_score += 0.20
    if weird_symbol_ratio > 0.18:
        gibberish_score += 0.30
    if total >= 80 and hint_hits == 0:
        gibberish_score += 0.25

    return {
        "text_length": total,
        "alpha_ratio": round(alpha_ratio, 4),
        "digit_ratio": round(digit_ratio, 4),
        "space_ratio": round(space_ratio, 4),
        "printable_ratio": round(printable_ratio, 4),
        "italian_hint_hits": hint_hits,
        "weird_symbol_ratio": round(weird_symbol_ratio, 4),
        "gibberish_score": round(min(gibberish_score, 1.0), 4),
    }


def _quality_tier(text: str, metrics: Dict[str, float | int]) -> str:
    n = int(metrics["text_length"])
    gib = float(metrics["gibberish_score"])

    if n == 0:
        return "UNREADABLE"
    if gib >= 0.70:
        return "UNREADABLE"
    if gib >= 0.45:
        return "LOW"
    if n < 40:
        return "LOW"
    if n < 200:
        return "MEDIUM"
    return "HIGH"


def extract_case(case_key: str) -> Dict[str, object]:
    ctx = build_context(case_key)
    pdf_path = Path(ctx.case.pdf_path)

    pypdf_pages = _extract_with_pypdf2(pdf_path)
    fitz_pages = _extract_with_fitz(pdf_path)

    if len(pypdf_pages) != len(fitz_pages):
        raise RuntimeError(
            f"Page count mismatch for {pdf_path}: PyPDF2={len(pypdf_pages)} fitz={len(fitz_pages)}"
        )

    raw_pages: List[Dict[str, object]] = []
    source_counts = {"PyPDF2": 0, "fitz": 0}
    unreadable_pages: List[int] = []
    low_pages: List[int] = []
    total_chars = 0

    for idx, (t1, t2) in enumerate(zip(pypdf_pages, fitz_pages), start=1):
        chosen_text, source = _choose_page_text(t1, t2)
        source_counts[source] += 1

        metrics = _readability_metrics(chosen_text)
        tier = _quality_tier(chosen_text, metrics)

        if tier == "UNREADABLE":
            unreadable_pages.append(idx)
        elif tier == "LOW":
            low_pages.append(idx)

        total_chars += len(chosen_text)
        raw_pages.append(
            {
                "page_number": idx,
                "text": chosen_text,
                "text_sha256": _sha256_text(chosen_text),
                "source_engine": source,
                "quality_tier": tier,
                "readability_metrics": metrics,
            }
        )

    if len(unreadable_pages) == len(raw_pages):
        global_quality = "UNREADABLE"
    elif unreadable_pages:
        global_quality = "LOW"
    elif low_pages:
        global_quality = "MEDIUM"
    elif total_chars < 500:
        global_quality = "MEDIUM"
    else:
        global_quality = "HIGH"

    raw_pages_fp = ctx.artifact_dir / "raw_pages.json"
    metrics_fp = ctx.artifact_dir / "extract_metrics.json"

    metrics = {
        "case_key": ctx.case.case_key,
        "pdf_path": ctx.case.pdf_path,
        "pdf_sha256_expected": ctx.case.sha256,
        "pages_count": len(raw_pages),
        "total_chars": total_chars,
        "source_counts": source_counts,
        "unreadable_pages": unreadable_pages,
        "low_pages": low_pages,
        "global_quality_tier": global_quality,
    }

    raw_pages_fp.write_text(json.dumps(raw_pages, ensure_ascii=False, indent=2), encoding="utf-8")
    metrics_fp.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "case_key": ctx.case.case_key,
        "pages_count": len(raw_pages),
        "total_chars": total_chars,
        "global_quality_tier": global_quality,
        "unreadable_pages": unreadable_pages,
        "low_pages": low_pages,
        "raw_pages_fp": str(raw_pages_fp),
        "metrics_fp": str(metrics_fp),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Canonical pipeline extraction adapter")
    parser.add_argument("--case", required=True, choices=list_case_keys())
    args = parser.parse_args()

    out = extract_case(args.case)
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
