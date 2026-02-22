#!/usr/bin/env python3
import argparse
import os
import re
import shutil
import subprocess
import sys
from typing import Any, Dict, List, Optional, Tuple

import requests


def _fail_many(reasons: List[str]) -> None:
    if not reasons:
        reasons = ["unknown failure"]
    print("FAIL:")
    for reason in reasons:
        print(f"- {reason}")
    sys.exit(1)


def _extract_text_from_pdf(path: str) -> str:
    pdftotext = shutil.which("pdftotext")
    if pdftotext:
        proc = subprocess.run([pdftotext, path, "-"], capture_output=True, text=True)
        if proc.returncode == 0 and proc.stdout:
            return proc.stdout
    try:
        from pypdf import PdfReader  # type: ignore
    except Exception:
        _fail_many([f"pdftotext failed and pypdf unavailable for {path}"])
    reader = PdfReader(path)
    parts = []
    for page in reader.pages:
        try:
            parts.append(page.extract_text() or "")
        except Exception:
            continue
    return "\n".join(parts)


def _parse_euro_number(raw: str) -> Optional[float]:
    if not raw:
        return None
    m = re.search(r"([0-9]{1,3}(?:[\\.,][0-9]{3})*(?:[\\.,][0-9]{2})?)", raw)
    if not m:
        return None
    cleaned = m.group(1).replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except Exception:
        return None


def _norm(s: Any) -> str:
    return " ".join(str(s or "").strip().lower().split())


def _parse_estratto(text: str) -> Dict[str, Any]:
    ref: Dict[str, Any] = {}
    text_norm = "\n".join([line.rstrip() for line in text.splitlines()])

    # Tribunale
    m = re.search(r"Tribunale\\s+di\\s+([A-Za-zÀ-Ù'\\s]+)", text_norm, re.I)
    if m:
        ref["tribunale"] = f"Tribunale di {m.group(1).strip()}"

    # Procedura
    m = re.search(r"Esecuzione\\s+Immobiliare\\s+(\\d+\\s*/\\s*\\d+)", text_norm, re.I)
    if m:
        ref["procedura"] = f"Esecuzione Immobiliare {m.group(1).replace(' ', '')}"

    # Address / city
    m = re.search(r"^([A-ZÀ-Ù'\\s]+\\([A-Z]{2}\\),\\s*VIA\\s+[A-ZÀ-Ù0-9'\\s]+)", text_norm, re.M)
    if m:
        ref["address_full"] = m.group(1).strip()
        city_m = re.search(r"^([A-ZÀ-Ù'\\s]+)\\([A-Z]{2}\\)", ref["address_full"])
        if city_m:
            ref["address_city"] = city_m.group(1).strip()

    # Beni count
    bene_nums = set(int(n) for n in re.findall(r"\\bBene\\s+(\\d+)\\b", text_norm, re.I))
    if bene_nums:
        ref["beni_count"] = len(bene_nums)

    # Occupancy
    occ_m = re.search(r"STATO\\s+OCCUPAZIONALE[^\\n]*\\n([^\\n]+)", text_norm, re.I)
    if occ_m:
        occ_line = occ_m.group(1)
        if re.search(r"occupat", occ_line, re.I):
            ref["occupancy"] = "OCCUPATO DAL DEBITORE" if re.search(r"debit", occ_line, re.I) else "OCCUPATO"
        elif re.search(r"liber|non\\s+occupato", occ_line, re.I):
            ref["occupancy"] = "LIBERO"

    # APE
    ape_block = re.search(r"APE\\s*\\n([^\\n]+)", text_norm, re.I)
    if ape_block:
        ape_line = ape_block.group(1)
        if re.search(r"non\\s+presente|assente", ape_line, re.I):
            ref["ape_present"] = False
        elif re.search(r"presente", ape_line, re.I):
            ref["ape_present"] = True

    # Spese condominiali
    spese_block = re.search(r"SPESE\\s+CONDOMINIALI\\s*\\n([^\\n]+)", text_norm, re.I)
    if spese_block:
        spese_line = spese_block.group(1)
        if re.search(r"non\\s+presenti|assen", spese_line, re.I):
            ref["spese_condominiali"] = "NON PRESENTI"

    # Sanatoria estimate
    sanatoria_m = re.search(r"sanabile[^\\n]{0,120}spesa\\s+stimata[^\\n]{0,80}€\\s*([\\d\\.,]+)", text_norm, re.I)
    if sanatoria_m:
        ref["sanatoria_estimate_eur"] = _parse_euro_number(sanatoria_m.group(0))

    # Asta date/time
    asta_m = re.search(r"(\\d{1,2}/\\d{1,2}/\\d{4}).{0,80}?ore\\s+(\\d{1,2}:\\d{2})", text_norm, re.I)
    if asta_m:
        ref["asta_date"] = asta_m.group(1)
        ref["asta_time"] = asta_m.group(2)

    return ref


def _get_system_value(result: Dict[str, Any], *paths: Tuple[str, ...]) -> Optional[Any]:
    for path in paths:
        cur: Any = result
        ok = True
        for key in path:
            if not isinstance(cur, dict) or key not in cur:
                ok = False
                break
            cur = cur[key]
        if ok:
            return cur
    return None


def _find_money_box_item(result: Dict[str, Any], code: str, label_fragment: str) -> Optional[Dict[str, Any]]:
    for key in ("money_box", "section_3_money_box"):
        box = result.get(key)
        if not isinstance(box, dict):
            continue
        items = box.get("items", [])
        if not isinstance(items, list):
            continue
        for item in items:
            label = str(item.get("label_it", "") or item.get("voce", "") or "")
            if item.get("code") == code or label_fragment.lower() in label.lower():
                return item
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Regression Step5: compare estratto agency vs system JSON.")
    parser.add_argument("--analysis-id", dest="analysis_id", required=True)
    parser.add_argument("--estratto-pdf", dest="estratto_pdf", default="/srv/perizia/app/uploads/estratto_agency.pdf")
    args = parser.parse_args()

    base_url = (os.environ.get("BASE_URL") or "").strip()
    session_token = (
        os.environ.get("SESSION_TOKEN")
        or os.environ.get("session_token")
        or ""
    ).strip()
    if not base_url or not session_token:
        _fail_many(["BASE_URL and SESSION_TOKEN/session_token are required"])

    headers = {"Cookie": f"session_token={session_token}"}
    detail_url = f"{base_url.rstrip('/')}/api/analysis/perizia/{args.analysis_id}"
    resp = requests.get(detail_url, headers=headers, timeout=30)
    if resp.status_code != 200:
        _fail_many([f"GET {detail_url} failed: {resp.status_code} {resp.text[:180]}"])
    result = resp.json().get("result") or {}

    text = _extract_text_from_pdf(args.estratto_pdf)
    ref = _parse_estratto(text)

    failures: List[str] = []
    matched: List[str] = []

    # Tribunale
    if ref.get("tribunale"):
        sys_val = _get_system_value(result, ("case_header", "tribunale")) or _get_system_value(result, ("report_header", "tribunale", "value"))
        if not sys_val or "mantova" not in _norm(sys_val):
            failures.append(f"tribunale mismatch: estratto={ref['tribunale']} system={sys_val}")
        else:
            matched.append(f"tribunale={sys_val}")

    # Procedura
    if ref.get("procedura"):
        sys_val = _get_system_value(result, ("case_header", "procedure_id")) or _get_system_value(result, ("report_header", "procedure", "value"))
        proc_num = re.search(r"(\\d+\\s*/\\s*\\d+)", ref["procedura"])
        if not sys_val or not proc_num or proc_num.group(1).replace(" ", "") not in _norm(sys_val):
            failures.append(f"procedura mismatch: estratto={ref['procedura']} system={sys_val}")
        else:
            matched.append(f"procedura={sys_val}")

    # Address
    if ref.get("address_full"):
        sys_val = _get_system_value(result, ("case_header", "address", "full")) or _get_system_value(result, ("report_header", "address", "value"))
        addr_ok = sys_val and _norm(ref["address_full"].split(",")[0]) in _norm(sys_val)
        if not addr_ok:
            failures.append(f"address mismatch: estratto={ref['address_full']} system={sys_val}")
        else:
            matched.append(f"address={sys_val}")

    # Beni count
    if ref.get("beni_count"):
        beni = _get_system_value(result, ("beni",)) or None
        if beni is None:
            lots = result.get("lots")
            if isinstance(lots, list) and lots:
                beni = lots[0].get("beni") if isinstance(lots[0], dict) else None
        if not isinstance(beni, list) or len(beni) != ref["beni_count"]:
            failures.append(f"beni_count mismatch: estratto={ref['beni_count']} system={len(beni) if isinstance(beni, list) else None}")
        else:
            matched.append(f"beni_count={len(beni)}")

    # Occupancy
    if ref.get("occupancy"):
        occ = _get_system_value(result, ("stato_occupativo", "status")) or ""
        occ_it = _get_system_value(result, ("stato_occupativo", "status_it")) or ""
        occ_norm = f"{occ} {occ_it}".upper()
        if ref["occupancy"] == "OCCUPATO DAL DEBITORE":
            ok = "OCCUPATO" in occ_norm and "DEBITORE" in occ_norm
        else:
            ok = ref["occupancy"] in occ_norm
        if not ok:
            failures.append(f"occupancy mismatch: estratto={ref['occupancy']} system={occ}")
        else:
            matched.append(f"occupancy={occ}")

    # APE
    if "ape_present" in ref:
        ape_status = None
        ape_obj = _get_system_value(result, ("abusi_edilizi_conformita", "ape")) or _get_system_value(result, ("section_5_abusi_conformita", "ape"))
        if isinstance(ape_obj, dict):
            ape_status = ape_obj.get("status")
        if ref["ape_present"] is False:
            if not ape_status or _norm(ape_status) not in {"assente", "non presente", "no"}:
                failures.append(f"ape mismatch: estratto=NON PRESENTE system={ape_status}")
            else:
                matched.append(f"ape={ape_status}")

    # Spese condominiali
    if ref.get("spese_condominiali"):
        spese_state = _get_system_value(result, ("field_states", "spese_condominiali_arretrate", "value"))
        spese_note = None
        item_e = _find_money_box_item(result, "E", "Spese condominiali")
        if item_e:
            spese_note = item_e.get("stima_nota")
        if not spese_state and spese_note:
            spese_state = spese_note
        if not spese_state or "non" not in _norm(spese_state):
            failures.append(f"spese_condominiali mismatch: estratto=NON PRESENTI system={spese_state}")
        else:
            matched.append(f"spese_condominiali={spese_state}")

    # Sanatoria estimate
    if ref.get("sanatoria_estimate_eur") is not None:
        item_a = _find_money_box_item(result, "A", "Regolarizzazione urbanistica")
        sys_val = None
        if item_a:
            sys_val = item_a.get("stima_euro") or item_a.get("value")
        sys_num = None
        if isinstance(sys_val, (int, float)):
            sys_num = float(sys_val)
        elif isinstance(sys_val, str):
            sys_num = _parse_euro_number(sys_val)
        if sys_num is None or abs(sys_num - float(ref["sanatoria_estimate_eur"])) > 1:
            failures.append(f"sanatoria_estimate mismatch: estratto={ref['sanatoria_estimate_eur']} system={sys_val}")
        else:
            matched.append(f"sanatoria_estimate={sys_num}")

    # Asta date/time
    if ref.get("asta_date") and ref.get("asta_time"):
        dati_asta = _get_system_value(result, ("dati_asta",)) or {}
        if not isinstance(dati_asta, dict):
            dati_asta = {}
        if dati_asta.get("data") != ref["asta_date"] or dati_asta.get("ora") != ref["asta_time"]:
            failures.append(f"dati_asta mismatch: estratto={ref['asta_date']} {ref['asta_time']} system={dati_asta}")
        else:
            matched.append(f"dati_asta={dati_asta.get('data')} {dati_asta.get('ora')}")

    if failures:
        _fail_many(failures)

    print(f"PASS: regression_step5_estratto_compare analysis_id={args.analysis_id}")
    if matched:
        print("Matched:")
        for item in matched:
            print(f"- {item}")


if __name__ == "__main__":
    main()
