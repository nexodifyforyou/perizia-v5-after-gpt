#!/usr/bin/env python3
import argparse
import json
import re
import subprocess
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Pattern, Tuple

from PyPDF2 import PdfReader


@dataclass
class Hit:
    page: int
    quote: str
    match: re.Match


def _clean_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def _extract_pages(pdf_path: str) -> List[Dict[str, Any]]:
    reader = PdfReader(pdf_path)
    pages: List[Dict[str, Any]] = []
    for idx, page in enumerate(reader.pages, start=1):
        text = ""
        try:
            proc = subprocess.run(
                ["pdftotext", "-f", str(idx), "-l", str(idx), pdf_path, "-"],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode == 0 and proc.stdout:
                text = proc.stdout
        except Exception:
            text = ""
        if not text:
            text = page.extract_text() or ""
        pages.append({"page": idx, "text": text})
    return pages


def _find_first(pages: List[Dict[str, Any]], pattern: Pattern[str], quote_len: int = 220) -> Optional[Hit]:
    for page in pages:
        text = page["text"]
        m = pattern.search(text)
        if not m:
            continue
        start = max(0, m.start() - 50)
        end = min(len(text), m.end() + quote_len)
        quote = _clean_spaces(text[start:end])
        return Hit(page=page["page"], quote=quote, match=m)
    return None


def _find_all_lines(pages: List[Dict[str, Any]], pattern: Pattern[str], max_items: int = 8) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for page in pages:
        for line in (page["text"] or "").splitlines():
            if pattern.search(line):
                out.append({"page": page["page"], "quote": _clean_spaces(line)})
                if len(out) >= max_items:
                    return out
    return out


def _nf(searched_patterns: List[str]) -> Dict[str, Any]:
    return {
        "value": "NOT_FOUND",
        "evidence": [],
        "searched_in": searched_patterns,
    }


def _money_to_float(raw: str) -> Optional[float]:
    if not raw:
        return None
    m = re.search(r"([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)", raw)
    if not m:
        return None
    try:
        return float(m.group(1).replace(".", "").replace(",", "."))
    except Exception:
        return None


def build_ref(pdf_path: str) -> Dict[str, Any]:
    pages = _extract_pages(pdf_path)
    out: Dict[str, Any] = {
        "source_pdf": pdf_path,
        "pages_count": len(pages),
        "fields": {},
    }

    tribunal_hit = _find_first(pages, re.compile(r"Tribunale\s+di\s+([^\n]+)", re.I))
    if tribunal_hit:
        out["fields"]["tribunale"] = {
            "value": f"Tribunale di {_clean_spaces(tribunal_hit.match.group(1))}",
            "evidence": [{"page": tribunal_hit.page, "quote": tribunal_hit.quote}],
        }
    else:
        out["fields"]["tribunale"] = _nf([r"TRIBUNALE\s+DI"])

    proc_hit = _find_first(pages, re.compile(r"Esecuzione\s+Immobiliare\s+(\d+\s*/\s*\d+)", re.I))
    if proc_hit:
        proc_num = re.sub(r"\s+", "", proc_hit.match.group(1))
        out["fields"]["procedure_id"] = {
            "value": f"Esecuzione Immobiliare {proc_num}",
            "evidence": [{"page": proc_hit.page, "quote": proc_hit.quote}],
        }
    else:
        out["fields"]["procedure_id"] = _nf([r"Esecuzione\s+Immobiliare\s+\d+/\d+", r"R\.?G\.?E\.?"])

    occ_hit = _find_first(pages, re.compile(r"occupat[oa][\s\S]{0,160}(debitor|esecutat|liber[oa]|non\s+occupat)", re.I))
    if occ_hit:
        occ_quote = _clean_spaces(occ_hit.quote)
        if re.search(r"debitore", occ_quote, re.I):
            occ_value = "OCCUPATO DAL DEBITORE"
        elif re.search(r"liber[oa]|non\s+occupat", occ_quote, re.I):
            occ_value = "LIBERO"
        else:
            occ_value = "OCCUPATO"
        out["fields"]["occupancy"] = {
            "value": occ_value,
            "evidence": [{"page": occ_hit.page, "quote": occ_quote}],
        }
    else:
        out["fields"]["occupancy"] = _nf([r"STATO DI OCCUPAZIONE", r"Occupat"])

    beni_lines = _find_all_lines(pages, re.compile(r"\bBene\s+(?:N[°º]\s*)?(\d+)\b", re.I), max_items=20)
    bene_map: Dict[int, str] = {}
    for item in beni_lines:
        q = item["quote"]
        m = re.search(r"\bBene\s+(?:N[°º]\s*)?(\d+)\b", q, re.I)
        if not m:
            continue
        num = int(m.group(1))
        if num not in bene_map:
            bene_map[num] = q
    if bene_map:
        descriptors = [{"bene_number": k, "text": bene_map[k]} for k in sorted(bene_map.keys())]
        evid = [{"page": x["page"], "quote": x["quote"]} for x in beni_lines[:8]]
        out["fields"]["beni_count"] = {"value": len(bene_map), "evidence": evid}
        out["fields"]["beni_descriptors"] = {"value": descriptors, "evidence": evid}
    else:
        out["fields"]["beni_count"] = _nf([r"\bBene\s+N?\s*\d+\b"])
        out["fields"]["beni_descriptors"] = _nf([r"\bBene\s+N?\s*\d+\b"])

    ape_hit = _find_first(pages, re.compile(r"(non\s+esiste|non\s+presente|assente)[\s\S]{0,120}(APE|certificato energetico)", re.I))
    if not ape_hit:
        ape_hit = _find_first(pages, re.compile(r"(APE|certificato energetico)[\s\S]{0,120}(presente)", re.I))
    if ape_hit:
        q = _clean_spaces(ape_hit.quote)
        if re.search(r"non\s+esiste|non\s+presente|assente", q, re.I):
            ape_value = "NON PRESENTE"
        else:
            ape_value = "PRESENTE"
        out["fields"]["ape_status"] = {"value": ape_value, "evidence": [{"page": ape_hit.page, "quote": q}]}
    else:
        out["fields"]["ape_status"] = _nf([r"APE", r"certificato energetico"])

    spese_hit = _find_first(pages, re.compile(r"spese\s+condominiali[\s\S]{0,180}(non\s+presenti|assent|non\s+risult)", re.I))
    if spese_hit:
        out["fields"]["spese_condominiali_arretrate"] = {
            "value": "NON PRESENTI",
            "evidence": [{"page": spese_hit.page, "quote": _clean_spaces(spese_hit.quote)}],
        }
    else:
        out["fields"]["spese_condominiali_arretrate"] = _nf([r"spese\s+condominiali"])

    sanatoria_hit = _find_first(pages, re.compile(r"sanabil\w+[\s\S]{0,180}?€\s*([0-9\.\,]+)", re.I))
    if not sanatoria_hit:
        sanatoria_hit = _find_first(pages, re.compile(r"spese[\s\S]{0,180}sanare[\s\S]{0,180}€\s*([0-9\.\,]+)", re.I))
    if not sanatoria_hit:
        sanatoria_hit = _find_first(pages, re.compile(r"spese\s+di\s+massima\s+presunte:\s*€\s*([0-9\.\,]+)", re.I))
    if sanatoria_hit:
        raw = sanatoria_hit.match.group(1)
        out["fields"]["sanatoria_estimate_eur"] = {
            "value": _money_to_float(raw),
            "value_raw": raw,
            "evidence": [{"page": sanatoria_hit.page, "quote": _clean_spaces(sanatoria_hit.quote)}],
        }
    else:
        out["fields"]["sanatoria_estimate_eur"] = _nf([r"spese\s+di\s+massima\s+presunte", r"sanatoria"])

    prezzo_hit = _find_first(pages, re.compile(r"Prezzo\s+base\s+d[’']asta.*?€\s*([0-9\.\,]+)", re.I))
    if prezzo_hit:
        raw = prezzo_hit.match.group(1)
        out["fields"]["prezzo_base_eur"] = {
            "value": _money_to_float(raw),
            "value_raw": raw,
            "evidence": [{"page": prezzo_hit.page, "quote": _clean_spaces(prezzo_hit.quote)}],
        }
    else:
        out["fields"]["prezzo_base_eur"] = _nf([r"Prezzo\s+base\s+d[’']asta"])

    asta_hit = _find_first(pages, re.compile(r"(\d{1,2}/\d{1,2}/\d{4}).{0,80}?ore\s+(\d{1,2}[:\.]\d{2})", re.I))
    if asta_hit:
        out["fields"]["dati_asta"] = {
            "value": {"data": asta_hit.match.group(1), "ora": asta_hit.match.group(2).replace(".", ":")},
            "evidence": [{"page": asta_hit.page, "quote": _clean_spaces(asta_hit.quote)}],
        }
    else:
        out["fields"]["dati_asta"] = _nf([r"\d{1,2}/\d{1,2}/\d{4}", r"ore\s+\d{1,2}[:\.]\d{2}"])

    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Build deterministic estratto reference with evidence.")
    parser.add_argument("--pdf", required=True, help="Path to agency estratto PDF")
    parser.add_argument("--out", required=True, help="Output json path")
    args = parser.parse_args()

    ref = build_ref(args.pdf)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(ref, f, ensure_ascii=False, indent=2)
    print(args.out)


if __name__ == "__main__":
    main()
