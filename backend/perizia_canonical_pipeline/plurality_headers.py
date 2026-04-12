from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Dict, List

from .runner import build_context
from .corpus_registry import load_cases, list_case_keys


STRICT_LOT_HEADER = re.compile(r"^\s*lotto\s*(?:n[°\.\s]*)?(\d{1,3}|unico|[A-Z])\s*$", re.I)
STRICT_BENE_HEADER = re.compile(r"^\s*bene\s*(?:n[°\.\s]*)?(\d{1,3}|primo|secondo|terzo|quarto|quinto)\s*$", re.I)
STRICT_CORPO_HEADER = re.compile(r"^\s*corpo\s*([A-Z]|\d{1,3})\s*$", re.I)

SOFT_LOT_HEADER = re.compile(r"^\s*(?:lotto\s*(?:n[°\.\s]*)?(\d{1,3}|unico)|(?:primo|secondo|terzo|quarto|quinto)\s+lotto)\b", re.I)
SOFT_BENE_HEADER = re.compile(r"^\s*(?:bene\s*(?:n[°\.\s]*)?(\d{1,3}|primo|secondo|terzo|quarto|quinto)|(?:primo|secondo|terzo|quarto|quinto)\s+bene)\b", re.I)
SOFT_CORPO_HEADER = re.compile(r"^\s*corpo\s*([A-Z]|\d{1,3})\b", re.I)

MONEY_RE = re.compile(r"(?:€|eur(?:o)?)\s*[0-9]", re.I)
ALLCAPS_RE = re.compile(r"^[A-ZÀ-ÖØ-Ý0-9\s\.\-–—\"'“”‘’°/()]+$")


def _clean(line: str) -> str:
    return re.sub(r"\s+", " ", (line or "")).strip()


def _line_class(line: str, kind: str) -> tuple[str, str | None]:
    clean = _clean(line)
    if not clean:
        return "GENERIC_NOISE", None

    if kind == "lot":
        m = STRICT_LOT_HEADER.match(clean)
        if m:
            return "HEADER_GRADE", (m.group(1) if m.lastindex else clean)
        m = SOFT_LOT_HEADER.match(clean)
        if m and len(clean) <= 80:
            if MONEY_RE.search(clean):
                return "TABLE_GRADE", (m.group(1) if m.lastindex else clean)
            if ALLCAPS_RE.match(clean.upper()) or clean.upper().startswith("LOTTO"):
                return "HEADER_GRADE", (m.group(1) if m.lastindex else clean)
            return "REFERENCE_ONLY", (m.group(1) if m.lastindex else clean)
        if "lotto" in clean.lower():
            return "REFERENCE_ONLY", None
        return "GENERIC_NOISE", None

    if kind == "bene":
        m = STRICT_BENE_HEADER.match(clean)
        if m:
            return "HEADER_GRADE", (m.group(1) if m.lastindex else clean)
        m = SOFT_BENE_HEADER.match(clean)
        if m and len(clean) <= 80:
            if MONEY_RE.search(clean):
                return "TABLE_GRADE", (m.group(1) if m.lastindex else clean)
            if clean.upper().startswith("BENE") or ALLCAPS_RE.match(clean.upper()):
                return "HEADER_GRADE", (m.group(1) if m.lastindex else clean)
            return "REFERENCE_ONLY", (m.group(1) if m.lastindex else clean)
        if "bene" in clean.lower():
            return "REFERENCE_ONLY", None
        return "GENERIC_NOISE", None

    if kind == "corpo":
        m = STRICT_CORPO_HEADER.match(clean)
        if m:
            return "HEADER_GRADE", m.group(1)
        m = SOFT_CORPO_HEADER.match(clean)
        if m and len(clean) <= 80:
            if clean.upper().startswith("CORPO"):
                return "HEADER_GRADE", m.group(1)
            return "REFERENCE_ONLY", m.group(1)
        if re.search(r"\bcorpi\b", clean, re.I):
            return "GENERIC_NOISE", None
        if re.search(r"\bcorpo\b", clean, re.I):
            return "REFERENCE_ONLY", None
        return "GENERIC_NOISE", None

    return "GENERIC_NOISE", None


def classify_case(case_key: str) -> Dict[str, object]:
    ctx = build_context(case_key)
    raw_fp = ctx.artifact_dir / "raw_pages.json"
    raw_pages = json.loads(raw_fp.read_text(encoding="utf-8"))

    out = {
        "case_key": case_key,
        "lot_signals": [],
        "bene_signals": [],
        "corpo_signals": [],
        "summary": {},
    }

    lot_header_ids = set()
    bene_header_ids = set()
    corpo_header_ids = set()

    occurrence_index = 0
    for page_obj in raw_pages:
        page = int(page_obj.get("page_number", 0) or 0)
        text = str(page_obj.get("text", "") or "")
        for line_index, raw_line in enumerate(text.splitlines(), start=1):
            line = _clean(raw_line)
            if not line:
                continue

            for kind, bucket_name, id_set in [
                ("lot", "lot_signals", lot_header_ids),
                ("bene", "bene_signals", bene_header_ids),
                ("corpo", "corpo_signals", corpo_header_ids),
            ]:
                label, value = _line_class(line, kind)
                if label == "GENERIC_NOISE":
                    continue
                occurrence_index += 1
                row = {
                    "page": page,
                    "line_index": line_index,
                    "occurrence_index": occurrence_index,
                    "quote": line[:300],
                    "class": label,
                    "value": value,
                }
                out[bucket_name].append(row)
                if label in {"HEADER_GRADE", "TABLE_GRADE"} and value:
                    id_set.add(str(value).lower())

    def _counts(rows: List[Dict[str, object]]) -> Dict[str, int]:
        return {
            "header_grade": sum(1 for r in rows if r["class"] == "HEADER_GRADE"),
            "table_grade": sum(1 for r in rows if r["class"] == "TABLE_GRADE"),
            "reference_only": sum(1 for r in rows if r["class"] == "REFERENCE_ONLY"),
        }

    out["summary"] = {
        "lot_counts": _counts(out["lot_signals"]),
        "bene_counts": _counts(out["bene_signals"]),
        "corpo_counts": _counts(out["corpo_signals"]),
        "unique_lot_header_ids": sorted(lot_header_ids),
        "unique_bene_header_ids": sorted(bene_header_ids),
        "unique_corpo_header_ids": sorted(corpo_header_ids),
    }

    fp = ctx.artifact_dir / "plurality_headers.json"
    fp.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Header-grade plurality classifier")
    parser.add_argument("--case", required=True, choices=list_case_keys())
    args = parser.parse_args()

    out = classify_case(args.case)
    print(json.dumps(out["summary"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
