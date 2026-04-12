from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Set

from .runner import build_context
from .corpus_registry import list_case_keys


LOT_PATTERNS = [
    re.compile(r"\blotto\s*(?:n[°\.\s]*)?(\d{1,3}|unico)\b", re.I),
    re.compile(r"\b(primo|secondo|terzo|quarto|quinto)\s+lotto\b", re.I),
]

BENE_PATTERNS = [
    re.compile(r"\bbene\s*(?:n[°\.\s]*)?(\d{1,3}|primo|secondo|terzo|quarto|quinto)\b", re.I),
    re.compile(r"\b(primo|secondo|terzo|quarto|quinto)\s+bene\b", re.I),
]

CORPO_PATTERNS = [
    re.compile(r"\bcorpo\s*([A-Z]|\d{1,3})\b", re.I),
    re.compile(r"\bcorpi\b", re.I),
]

VALUATION_PATTERNS = [
    re.compile(r"\bprezzo\s+base\b", re.I),
    re.compile(r"\bofferta\s+minima\b", re.I),
    re.compile(r"\bvalore\s+(?:di\s+mercato|commerciale|venale|di\s+stima)\b", re.I),
]

EURO_PATTERN = re.compile(r"(?:€|eur(?:o)?)\s*([0-9]{1,3}(?:[.\s][0-9]{3})*(?:,\d{2})?)", re.I)


def _load_raw_pages(case_key: str) -> List[Dict[str, object]]:
    ctx = build_context(case_key)
    fp = ctx.artifact_dir / "raw_pages.json"
    if not fp.exists():
        raise FileNotFoundError(f"Missing raw_pages.json for {case_key}: {fp}")
    return json.loads(fp.read_text(encoding="utf-8"))


def _clean_line(line: str) -> str:
    return re.sub(r"\s+", " ", line or "").strip()


def _append_signal(bucket: List[Dict[str, object]], page: int, line: str, pattern: str, value: str | None = None) -> None:
    bucket.append(
        {
            "page": page,
            "quote": _clean_line(line)[:300],
            "pattern_matched": pattern,
            "value": value,
        }
    )


def scan_case(case_key: str) -> Dict[str, object]:
    ctx = build_context(case_key)
    raw_pages = _load_raw_pages(case_key)

    lot_header_signals: List[Dict[str, object]] = []
    bene_header_signals: List[Dict[str, object]] = []
    corpo_markers: List[Dict[str, object]] = []
    valuation_amount_signals: List[Dict[str, object]] = []
    table_plurality_signals: List[Dict[str, object]] = []
    narrative_plurality_signals: List[Dict[str, object]] = []

    unique_lot_ids: Set[str] = set()
    unique_bene_ids: Set[str] = set()
    unique_corpo_ids: Set[str] = set()
    unique_valuation_quotes: Set[str] = set()

    for page_obj in raw_pages:
        page = int(page_obj.get("page_number", 0) or 0)
        text = str(page_obj.get("text", "") or "")
        lines = text.splitlines()

        for line in lines:
            clean = _clean_line(line)
            if not clean:
                continue

            # Lot markers
            for pat in LOT_PATTERNS:
                m = pat.search(clean)
                if m:
                    val = (m.group(1) if m.lastindex else m.group(0)).strip()
                    unique_lot_ids.add(val.lower())
                    _append_signal(lot_header_signals, page, clean, pat.pattern, val)

                    if "€" in clean or "eur" in clean.lower():
                        _append_signal(table_plurality_signals, page, clean, "lot_line_with_money", val)
                    break

            # Bene markers
            for pat in BENE_PATTERNS:
                m = pat.search(clean)
                if m:
                    val = (m.group(1) if m.lastindex else m.group(0)).strip()
                    unique_bene_ids.add(val.lower())
                    _append_signal(bene_header_signals, page, clean, pat.pattern, val)
                    break

            # Corpo markers
            for pat in CORPO_PATTERNS:
                m = pat.search(clean)
                if m:
                    val = (m.group(1) if m.lastindex else m.group(0)).strip()
                    unique_corpo_ids.add(val.lower())
                    _append_signal(corpo_markers, page, clean, pat.pattern, val)
                    break

            # Valuation signals
            valuation_hit = False
            for pat in VALUATION_PATTERNS:
                if pat.search(clean):
                    euro = EURO_PATTERN.search(clean)
                    euro_val = euro.group(1) if euro else None
                    key = f"{page}:{clean}"
                    if key not in unique_valuation_quotes:
                        unique_valuation_quotes.add(key)
                        _append_signal(valuation_amount_signals, page, clean, pat.pattern, euro_val)
                    valuation_hit = True
                    break

            # Narrative plurality hints
            lowered = clean.lower()
            if any(token in lowered for token in ["fabbricato", "terreno", "garage", "cantina", "appartamento", "deposito"]):
                if any(token in lowered for token in ["primo", "secondo", "terzo", "bene", "corpo", "lotto"]):
                    _append_signal(narrative_plurality_signals, page, clean, "narrative_plurality_hint")

            # Table-like plurality hints
            if not valuation_hit and ("lotto" in lowered and ("prezzo" in lowered or "valore" in lowered)):
                _append_signal(table_plurality_signals, page, clean, "lotto_table_like_row")

    out = {
        "case_key": ctx.case.case_key,
        "source_raw_pages": str(ctx.artifact_dir / "raw_pages.json"),
        "lot_header_signals": lot_header_signals[:200],
        "bene_header_signals": bene_header_signals[:200],
        "corpo_markers": corpo_markers[:200],
        "valuation_amount_signals": valuation_amount_signals[:300],
        "table_plurality_signals": table_plurality_signals[:200],
        "narrative_plurality_signals": narrative_plurality_signals[:200],
        "summary": {
            "total_lot_header_signals": len(lot_header_signals),
            "total_bene_header_signals": len(bene_header_signals),
            "total_corpo_markers": len(corpo_markers),
            "total_valuation_amount_signals": len(valuation_amount_signals),
            "unique_lot_ids_detected": sorted(unique_lot_ids),
            "unique_bene_ids_detected": sorted(unique_bene_ids),
            "unique_corpo_ids_detected": sorted(unique_corpo_ids),
        },
    }

    fp = ctx.artifact_dir / "plurality_signals.json"
    fp.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Canonical pipeline plurality scanner")
    parser.add_argument("--case", required=True, choices=list_case_keys())
    args = parser.parse_args()

    out = scan_case(args.case)
    print(json.dumps(out["summary"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
