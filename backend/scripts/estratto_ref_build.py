#!/usr/bin/env python3
import argparse
import json
import re
import subprocess
import unicodedata
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Pattern, Tuple

from PyPDF2 import PdfReader


@dataclass
class Hit:
    page: int
    quote: str
    match: re.Match


SECTION_ORDER = [
    "STATO OCCUPAZIONALE",
    "ABUSI / CONFORMITA",
    "STATO DEGLI IMPIANTI",
    "APE",
    "SPESE CONDOMINIALI",
    "DETTAGLI ASTA",
    "ALTRE SPESE",
    "DATI CATASTALI",
]

SECTION_ALIASES = {
    "STATO OCCUPAZIONALE": ["STATO OCCUPAZIONALE", "STATO DI OCCUPAZIONE", "STATO OCCUPATIVO"],
    "ABUSI / CONFORMITA": ["ABUSI", "CONFORMITA", "CONFORMITÀ", "ABUSI / CONFORMITA"],
    "STATO DEGLI IMPIANTI": ["STATO DEGLI IMPIANTI", "IMPIANTI"],
    "APE": ["APE", "ATTESTATO DI PRESTAZIONE ENERGETICA", "CERTIFICATO ENERGETICO"],
    "SPESE CONDOMINIALI": ["SPESE CONDOMINIALI"],
    "DETTAGLI ASTA": ["DETTAGLI ASTA", "VENDITA", "DATA ASTA"],
    "ALTRE SPESE": ["ALTRE SPESE"],
    "DATI CATASTALI": ["DATI CATASTALI", "CATASTALI"],
}

MONEY_RE = re.compile(r"€\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)", re.I)


def _clean_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def _normalize(text: str) -> str:
    x = (text or "").upper()
    x = "".join(ch for ch in unicodedata.normalize("NFD", x) if unicodedata.category(ch) != "Mn")
    return _clean_spaces(x)


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
        start = max(0, m.start() - 70)
        end = min(len(text), m.end() + quote_len)
        quote = _clean_spaces(text[start:end])
        return Hit(page=page["page"], quote=quote, match=m)
    return None


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


def _nf(searched_patterns: List[str]) -> Dict[str, Any]:
    return {
        "value": "NOT_FOUND",
        "evidence": [],
        "searched_in": searched_patterns,
    }


def _match_heading(line: str) -> Optional[str]:
    nline = _normalize(line)
    if not nline:
        return None
    for canonical in SECTION_ORDER:
        for alias in SECTION_ALIASES.get(canonical, []):
            if _normalize(alias) in nline:
                return canonical
    return None


def _collect_lines(pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for page in pages:
        for raw in (page.get("text") or "").splitlines():
            line = _clean_spaces(raw)
            if line:
                out.append({"page": page["page"], "line": line})
    return out


def _section_blocks(lines: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    starts: List[Tuple[int, str]] = []
    for i, row in enumerate(lines):
        heading = _match_heading(row["line"])
        if heading:
            starts.append((i, heading))

    blocks: Dict[str, List[Dict[str, Any]]] = {k: [] for k in SECTION_ORDER}
    if not starts:
        return blocks

    starts_sorted = sorted(starts, key=lambda x: x[0])
    for idx, (start_i, name) in enumerate(starts_sorted):
        end_i = starts_sorted[idx + 1][0] if idx + 1 < len(starts_sorted) else len(lines)
        chunk = lines[start_i + 1 : end_i]
        if chunk:
            blocks[name].extend(chunk)
    return blocks


def _line_evidence(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [{"page": row["page"], "quote": _clean_spaces(row["line"])}]


def _first_line(lines: List[Dict[str, Any]], pattern: Pattern[str]) -> Optional[Dict[str, Any]]:
    for row in lines:
        if pattern.search(row["line"]):
            return row
    return None


def _add_item(section_items: Dict[str, List[Dict[str, Any]]], section: str, item: Dict[str, Any]) -> None:
    items = section_items.setdefault(section, [])
    key = str(item.get("key") or "").strip()
    if key and any(str(x.get("key")) == key for x in items):
        return
    items.append(item)


def _extract_cost_items(all_lines: List[Dict[str, Any]], section_items: Dict[str, List[Dict[str, Any]]]) -> Dict[str, float]:
    found: Dict[str, float] = {}

    for idx, row in enumerate(all_lines):
        line = row["line"]
        for m in MONEY_RE.finditer(line):
            value = _money_to_float(m.group(1))
            if value is None:
                continue
            ctx = line
            if idx - 1 >= 0 and all_lines[idx - 1]["page"] == row["page"]:
                ctx = f"{all_lines[idx - 1]['line']} {ctx}"
            if idx + 1 < len(all_lines) and all_lines[idx + 1]["page"] == row["page"]:
                ctx = f"{ctx} {all_lines[idx + 1]['line']}"
            ctx_l = _normalize(ctx)

            chosen = None
            if "costo_completamento_lavori" not in found and "COMPLET" in ctx_l and "LAVOR" in ctx_l:
                chosen = "costo_completamento_lavori"
            elif "costo_richiesta_abitabilita" not in found and "RICHIEST" in ctx_l and "ABITABIL" in ctx_l:
                chosen = "costo_richiesta_abitabilita"
            elif "sanatoria_bene3" not in found and ("SANABIL" in ctx_l or "SANATORIA" in ctx_l):
                chosen = "sanatoria_bene3"
            if not chosen:
                continue

            found[chosen] = value
            _add_item(
                section_items,
                "ABUSI / CONFORMITA",
                {
                    "key": chosen,
                    "value_eur": value,
                    "type": "cost",
                    "evidence": _line_evidence(row),
                },
            )

    return found


def _extract_impianti_items(impianti_lines: List[Dict[str, Any]], section_items: Dict[str, List[Dict[str, Any]]]) -> None:
    wanted = ["UFFICIO", "RUSTICO", "GARAGE", "ABITAZIONE"]
    for label in wanted:
        rx = re.compile(rf"\b{label}\b\s*[:\-]?\s*(.*)$", re.I)
        best_value = None
        best_ev: List[Dict[str, Any]] = []

        for i, row in enumerate(impianti_lines):
            m = rx.search(row["line"])
            if not m:
                continue
            tail = _clean_spaces(m.group(1))
            if tail:
                best_value = tail
                best_ev = _line_evidence(row)
                break
            if i + 1 < len(impianti_lines):
                nxt = impianti_lines[i + 1]
                if nxt["page"] == row["page"] and _clean_spaces(nxt["line"]):
                    best_value = _clean_spaces(nxt["line"])
                    best_ev = [{"page": row["page"], "quote": f"{row['line']} {nxt['line']}"}]
                    break

        if best_value:
            _add_item(
                section_items,
                "STATO DEGLI IMPIANTI",
                {
                    "key": f"impianti_{label.lower()}",
                    "value": best_value,
                    "type": "fact",
                    "evidence": best_ev,
                },
            )


def build_ref(pdf_path: str) -> Dict[str, Any]:
    pages = _extract_pages(pdf_path)
    all_lines = _collect_lines(pages)
    blocks = _section_blocks(all_lines)

    section_items: Dict[str, List[Dict[str, Any]]] = {k: [] for k in SECTION_ORDER}

    occ_lines = blocks.get("STATO OCCUPAZIONALE", []) or all_lines
    occ_row = _first_line(occ_lines, re.compile(r"occupat|liber", re.I))
    occ_value = None
    if occ_row:
        q = _normalize(occ_row["line"])
        if re.search(r"DEBITOR|ESECUTAT|FAMILIAR", q):
            occ_value = "OCCUPATO (debitore/familiari)"
        elif re.search(r"LIBER|NON OCCUPAT", q):
            occ_value = "LIBERO"
        else:
            occ_value = "OCCUPATO"
        _add_item(
            section_items,
            "STATO OCCUPAZIONALE",
            {"key": "occupancy", "value": occ_value, "type": "fact", "evidence": _line_evidence(occ_row)},
        )

    abusi_lines = blocks.get("ABUSI / CONFORMITA", [])
    incongr_row = _first_line(abusi_lines, re.compile(r"incongruen|catast|catasto", re.I))
    if incongr_row:
        abusi_join = _normalize(" ".join(x["line"] for x in abusi_lines[:8]))
        if "UFFICIO" in abusi_join and "GARAGE" in abusi_join:
            incongr_val = "ufficio e garage"
        else:
            incongr_val = _clean_spaces(incongr_row["line"])
        _add_item(
            section_items,
            "ABUSI / CONFORMITA",
            {
                "key": "incongruenze_catasto",
                "value": incongr_val,
                "type": "risk",
                "evidence": _line_evidence(incongr_row),
            },
        )

    non_agibile_row = _first_line(
        abusi_lines or all_lines,
        re.compile(r"non\s+agibil|non\s+risulta\s+agibil|non\s+abitabil|mancanza\s+abitabil|assenza\s+abitabil", re.I),
    )
    if non_agibile_row:
        _add_item(
            section_items,
            "ABUSI / CONFORMITA",
            {
                "key": "non_agibile",
                "value": True,
                "type": "risk",
                "evidence": _line_evidence(non_agibile_row),
            },
        )

    costs = _extract_cost_items(abusi_lines or all_lines, section_items)

    _extract_impianti_items(blocks.get("STATO DEGLI IMPIANTI", []), section_items)

    ape_block = blocks.get("APE", [])
    ape_row = _first_line(ape_block, re.compile(r"non\s+presente|assente|non\s+esiste|presente", re.I))
    if not ape_row:
        ape_row = _first_line(ape_block or all_lines, re.compile(r"APE|energetic|prestazione", re.I))
    if ape_row:
        q = _normalize(ape_row["line"])
        ape_value = "NON PRESENTE" if re.search(r"NON\s+PRESENT|ASSENT|NON\s+ESIST", q) else "PRESENTE"
        _add_item(section_items, "APE", {"key": "ape", "value": ape_value, "type": "fact", "evidence": _line_evidence(ape_row)})

    spese_block = blocks.get("SPESE CONDOMINIALI", [])
    spese_row = _first_line(spese_block, re.compile(r"non\s+present|assent|non\s+risult", re.I))
    if not spese_row:
        spese_row = _first_line(spese_block or all_lines, re.compile(r"spese\s+condominial", re.I))
    if spese_row:
        q = _normalize(spese_row["line"])
        sval = "NON PRESENTI" if re.search(r"NON\s+PRESENT|ASSENT|NON\s+RISULT", q) else _clean_spaces(spese_row["line"])
        _add_item(
            section_items,
            "SPESE CONDOMINIALI",
            {
                "key": "spese_condominiali_arretrate",
                "value": sval,
                "type": "fact",
                "evidence": _line_evidence(spese_row),
            },
        )

    asta_hit = _find_first(pages, re.compile(r"(\d{1,2}/\d{1,2}/\d{4}).{0,90}?ore\s+(\d{1,2}[:\.]\d{2})", re.I))
    if asta_hit:
        _add_item(
            section_items,
            "DETTAGLI ASTA",
            {
                "key": "dati_asta",
                "value": {"data": asta_hit.match.group(1), "ora": asta_hit.match.group(2).replace(".", ":")},
                "type": "fact",
                "evidence": [{"page": asta_hit.page, "quote": _clean_spaces(asta_hit.quote)}],
            },
        )

    sections_out: List[Dict[str, Any]] = []
    for name in SECTION_ORDER:
        items = section_items.get(name, [])
        if items or blocks.get(name):
            sections_out.append({"name": name, "items": items})

    out: Dict[str, Any] = {
        "source_pdf": pdf_path,
        "pages_count": len(pages),
        "sections": sections_out,
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

    if occ_value:
        occ_scalar = "OCCUPATO DAL DEBITORE" if "debitore" in occ_value.lower() else occ_value
        out["fields"]["occupancy"] = {
            "value": occ_scalar,
            "evidence": section_items.get("STATO OCCUPAZIONALE", [{}])[0].get("evidence", []) if section_items.get("STATO OCCUPAZIONALE") else [],
        }
    else:
        out["fields"]["occupancy"] = _nf([r"STATO DI OCCUPAZIONE", r"Occupat"])

    bene_rows = [row for row in all_lines if re.search(r"\bBene\s+(?:N[°º]\s*)?(\d+)\b", row["line"], re.I)]
    bene_map: Dict[int, str] = {}
    for row in bene_rows:
        m = re.search(r"\bBene\s+(?:N[°º]\s*)?(\d+)\b", row["line"], re.I)
        if not m:
            continue
        n = int(m.group(1))
        if n not in bene_map:
            bene_map[n] = row["line"]
    if bene_map:
        out["fields"]["beni_count"] = {"value": len(bene_map), "evidence": [{"page": r["page"], "quote": r["line"]} for r in bene_rows[:8]]}
        out["fields"]["beni_descriptors"] = {
            "value": [{"bene_number": k, "text": bene_map[k]} for k in sorted(bene_map.keys())],
            "evidence": [{"page": r["page"], "quote": r["line"]} for r in bene_rows[:8]],
        }
    else:
        out["fields"]["beni_count"] = _nf([r"\bBene\s+N?\s*\d+\b"])
        out["fields"]["beni_descriptors"] = _nf([r"\bBene\s+N?\s*\d+\b"])

    ape_item = next((x for x in section_items.get("APE", []) if x.get("key") == "ape"), None)
    if ape_item:
        out["fields"]["ape_status"] = {"value": ape_item.get("value"), "evidence": ape_item.get("evidence", [])}
    else:
        out["fields"]["ape_status"] = _nf([r"APE", r"certificato energetico"])

    spese_item = next((x for x in section_items.get("SPESE CONDOMINIALI", []) if x.get("key") == "spese_condominiali_arretrate"), None)
    if spese_item:
        out["fields"]["spese_condominiali_arretrate"] = {"value": spese_item.get("value"), "evidence": spese_item.get("evidence", [])}
    else:
        out["fields"]["spese_condominiali_arretrate"] = _nf([r"spese\s+condominiali"])

    if "sanatoria_bene3" in costs:
        out["fields"]["sanatoria_estimate_eur"] = {
            "value": costs["sanatoria_bene3"],
            "value_raw": f"{costs['sanatoria_bene3']:.2f}",
            "evidence": next((x.get("evidence", []) for x in section_items.get("ABUSI / CONFORMITA", []) if x.get("key") == "sanatoria_bene3"), []),
        }
    else:
        sanatoria_hit = _find_first(pages, re.compile(r"sanabil\w+[\s\S]{0,180}?€\s*([0-9\.\,]+)", re.I))
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

    dati_asta_item = next((x for x in section_items.get("DETTAGLI ASTA", []) if x.get("key") == "dati_asta"), None)
    if dati_asta_item:
        out["fields"]["dati_asta"] = {"value": dati_asta_item.get("value"), "evidence": dati_asta_item.get("evidence", [])}
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
