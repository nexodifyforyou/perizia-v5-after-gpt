import json
import hashlib
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from evidence_utils import normalize_evidence_quote

RUNS_ROOT = Path("/srv/perizia/_qa/runs")

REQUIRED_HEADINGS = [
    "occupancy",
    "ape",
    "abusi_agibilita",
    "impianti",
    "catasto",
    "formalita",
    "dati_asta",
    "legal",
]

HEADINGS_DICT = {
    "occupancy": ("Stato occupativo", "Occupancy"),
    "ape": ("APE / Energia", "Energy certificate"),
    "abusi_agibilita": ("Abusi e agibilità", "Building compliance and habitability"),
    "impianti": ("Impianti", "Systems"),
    "catasto": ("Catasto", "Cadastre"),
    "formalita": ("Formalità pregiudizievoli", "Liens and encumbrances"),
    "dati_asta": ("Dati asta", "Auction details"),
    "legal": ("Vincoli legali", "Legal constraints"),
}

FORMALITA_TERMS = ("ipoteca", "pignor", "capitale", "importo", "formalit")
FORMALITA_DATE_TERMS = ("trascrizion", "notifica")
ASTA_TERMS = ("asta", "vendita", "delegato")
ABUSI_COST_TERMS = ("sanatoria", "abitabil", "agibil", "lavor", "regolarizz")
COST_TERMS = ("sanatoria", "regolarizz", "agibil", "abitabil", "lavor", "spese", "oneri", "costo")
BENE_TERMS = (
    ("BENE 1", re.compile(r"\bBENE\s*N[°º\.]?\s*1\b", re.IGNORECASE)),
    ("BENE 2", re.compile(r"\bBENE\s*N[°º\.]?\s*2\b", re.IGNORECASE)),
    ("BENE 3", re.compile(r"\bBENE\s*N[°º\.]?\s*3\b", re.IGNORECASE)),
    ("BENE 4", re.compile(r"\bBENE\s*N[°º\.]?\s*4\b", re.IGNORECASE)),
    ("UFFICIO", re.compile(r"\bufficio\b", re.IGNORECASE)),
    ("RUSTICO", re.compile(r"\brustic\w*\b", re.IGNORECASE)),
    ("GARAGE", re.compile(r"\bgarage\b|\bautorimess\w*\b", re.IGNORECASE)),
    ("ABITAZIONE", re.compile(r"\babitazion\w*\b", re.IGNORECASE)),
)

IMPIANTI_LABEL_PATTERNS: Dict[str, re.Pattern] = {
    "impianti_ufficio": re.compile(r"(?im)^\s*UFFICIO\s*:", re.MULTILINE),
    "impianti_rustico": re.compile(r"(?im)^\s*RUSTICO\s*:", re.MULTILINE),
    "impianti_garage": re.compile(
        r"(?im)^\s*(?:GARAGE(?:\s*\(.*?\))?|AUTORIMESSA)\s*:|^\s*BENE\s*N[°º\.]?\s*[23]\b[^\n]*GARAGE[^\n]*$",
        re.MULTILINE,
    ),
    "impianti_abitazione": re.compile(r"(?im)^\s*ABITAZIONE(?:\s*-\s*PIANO\s*[A-ZÀ-Ù ]+)?\s*:", re.MULTILINE),
}

NON_AGIBILE_PATTERN = re.compile(
    r"non\s+risulta\s+agibil|non\s+agibil|non\s+[èe]\s+presente\s+l[’']abitabilit|assenza\s+di\s+abitabilit",
    re.IGNORECASE,
)


def _read_json(path: Path, fallback: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return fallback


def _clean_quote(q: Any) -> str:
    quote = str(q or "")
    quote = re.sub(r"\s+", " ", quote).strip()
    return quote


def _contains_any(text: str, terms: Tuple[str, ...]) -> bool:
    t = text.lower()
    return any(term in t for term in terms)


def _evidence_from_candidate(item: Dict[str, Any]) -> List[Dict[str, Any]]:
    page = item.get("page")
    quote = _clean_quote(item.get("quote"))
    if not isinstance(page, int) or not quote:
        return []
    normalized_quote, search_hint = normalize_evidence_quote(quote, 0, len(quote), max_len=520)
    if not normalized_quote:
        return []
    payload = {"page": page, "quote": normalized_quote}
    if search_hint:
        payload["search_hint"] = search_hint
    return [payload]


def _add_item(
    target: List[Dict[str, Any]],
    item_id: str,
    label_it: str,
    label_en: str,
    candidate_ids: List[str],
    evidence: List[Dict[str, Any]],
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    if not candidate_ids or not evidence:
        return
    payload: Dict[str, Any] = {
        "item_id": item_id,
        "label_it": label_it,
        "label_en": label_en,
        "candidate_ids": candidate_ids,
        "evidence": evidence,
    }
    if isinstance(extra, dict):
        payload.update(extra)
    target.append(payload)


def _section(section_key: str, items: List[Dict[str, Any]]) -> Dict[str, Any]:
    heading_it, heading_en = HEADINGS_DICT[section_key]
    return {
        "heading_key": section_key,
        "heading_it": heading_it,
        "heading_en": heading_en,
        "items": items,
    }


def _normalized_page_rows(pages_raw: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for idx, row in enumerate(pages_raw):
        if not isinstance(row, dict):
            continue
        page = row.get("page")
        if not isinstance(page, int):
            page = row.get("page_number")
        if not isinstance(page, int):
            page = idx + 1
        text = str(row.get("text", "") or "")
        rows.append(
            {
                "page": int(page),
                "text": text,
                "page_text_hash": hashlib.sha256(text.encode("utf-8")).hexdigest(),
            }
        )
    rows.sort(key=lambda x: x["page"])
    return rows


def _major_heading_hits(pages: List[Dict[str, Any]]) -> List[Tuple[int, int, str]]:
    known = [
        re.compile(r"^\s*STATO\s+DEGLI\s+IMPIANTI\b", re.IGNORECASE),
        re.compile(r"^\s*REGOLARIT[ÀA]\s+EDILIZIA\b", re.IGNORECASE),
        re.compile(r"^\s*CONFORMIT[ÀA]\s+EDILIZIA\b", re.IGNORECASE),
        re.compile(r"^\s*REGOLARIT[ÀA]\s+URBANISTICA\b", re.IGNORECASE),
        re.compile(r"^\s*ABUSI\b", re.IGNORECASE),
    ]
    caps_line = re.compile(r"^\s*[A-ZÀ-Ù0-9][A-ZÀ-Ù0-9\s\-/\(\)\.'’]{6,}\s*$")
    hits: List[Tuple[int, int, str]] = []
    for page_row in pages:
        page = int(page_row["page"])
        lines = str(page_row["text"]).splitlines()
        for line_idx, raw_line in enumerate(lines):
            line = raw_line.strip()
            if not line:
                continue
            if any(rx.search(line) for rx in known) or caps_line.match(line):
                hits.append((page, line_idx, line))
    hits.sort(key=lambda x: (x[0], x[1]))
    return hits


def _is_toc_like_heading(line: str) -> bool:
    s = str(line or "")
    return bool(re.search(r"\.{3,}\s*\d+\s*$", s)) or "................................" in s


def _find_section_window(
    pages: List[Dict[str, Any]],
    target_heading_patterns: List[re.Pattern],
) -> Optional[Dict[str, Any]]:
    heading_hits = _major_heading_hits(pages)
    candidates: List[Tuple[int, int, str]] = []
    for page, line_idx, line in heading_hits:
        if any(rx.search(line) for rx in target_heading_patterns):
            candidates.append((page, line_idx, line))
    if not candidates:
        return None

    non_toc = [c for c in candidates if not _is_toc_like_heading(c[2])]
    start: Optional[Tuple[int, int, str]] = non_toc[0] if non_toc else candidates[0]
    if not start:
        return None

    start_page, start_line_idx, start_line = start
    end_page = int(pages[-1]["page"]) if pages else start_page
    for page, line_idx, line in heading_hits:
        if (page, line_idx) <= (start_page, start_line_idx):
            continue
        end_page = page - 1 if page > start_page else start_page
        break
    if end_page < start_page:
        end_page = start_page

    return {
        "start_page": start_page,
        "end_page": end_page,
        "heading": start_line,
    }


def _fallback_impianti_window_from_labels(pages: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    page_hits: List[int] = []
    label_rx = re.compile(r"(?im)^\s*(UFFICIO|RUSTICO|GARAGE|AUTORIMESSA|ABITAZIONE)\s*:")
    for p in pages:
        text = str(p.get("text", "") or "")
        label_count = len(list(label_rx.finditer(text)))
        impianto_count = len(re.findall(r"\bimpianto\b", text, flags=re.IGNORECASE))
        if label_count >= 1 and impianto_count >= 2:
            page_hits.append(int(p["page"]))
    if not page_hits:
        return None
    return {
        "start_page": min(page_hits),
        "end_page": max(page_hits),
        "heading": "FALLBACK_LABEL_WINDOW",
    }


def _pages_in_window(pages: List[Dict[str, Any]], window: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not window:
        return []
    sp = int(window.get("start_page", 0) or 0)
    ep = int(window.get("end_page", 0) or 0)
    return [p for p in pages if sp <= int(p["page"]) <= ep]


def _build_page_local_evidence(page_row: Dict[str, Any], start: int, end: int) -> Dict[str, Any]:
    text = str(page_row["text"])
    text_len = len(text)
    s = max(0, min(int(start), text_len))
    e = max(s, min(int(end), text_len))
    normalized_quote, search_hint = normalize_evidence_quote(text, s, e, max_len=520)
    return {
        "page": int(page_row["page"]),
        "quote": normalized_quote,
        "start_offset": s,
        "end_offset": e,
        "page_text_hash": page_row["page_text_hash"],
        "offset_mode": "PAGE_LOCAL",
        "search_hint": search_hint,
    }


def _extract_label_blocks(
    pages_window: List[Dict[str, Any]],
    label_patterns: Dict[str, re.Pattern],
) -> Dict[str, Dict[str, Any]]:
    if not pages_window:
        return {}

    page_offsets: Dict[int, int] = {}
    absolute_text_parts: List[str] = []
    cursor = 0
    for page_row in pages_window:
        page = int(page_row["page"])
        text = str(page_row["text"])
        page_offsets[page] = cursor
        absolute_text_parts.append(text)
        cursor += len(text) + 1
    absolute_text = "\n".join(absolute_text_parts)

    all_matches: List[Dict[str, Any]] = []
    for field_key, rx in label_patterns.items():
        for page_row in pages_window:
            page = int(page_row["page"])
            text = str(page_row["text"])
            for m in rx.finditer(text):
                all_matches.append(
                    {
                        "field_key": field_key,
                        "page": page,
                        "start": m.start(),
                        "end": m.end(),
                        "abs_start": page_offsets[page] + m.start(),
                    }
                )
    all_matches.sort(key=lambda x: x["abs_start"])
    if not all_matches:
        return {}

    extracted: Dict[str, List[Dict[str, Any]]] = {}
    for idx, hit in enumerate(all_matches):
        field_key = hit["field_key"]
        abs_start = int(hit["abs_start"])
        next_abs = len(absolute_text)
        if idx + 1 < len(all_matches):
            next_abs = int(all_matches[idx + 1]["abs_start"])
        abs_end = min(next_abs, abs_start + 350)
        if abs_end <= abs_start:
            continue

        pieces: List[Dict[str, Any]] = []
        for page_row in pages_window:
            page = int(page_row["page"])
            text = str(page_row["text"])
            p_start_abs = page_offsets[page]
            p_end_abs = p_start_abs + len(text)
            if abs_end <= p_start_abs or abs_start >= p_end_abs:
                continue
            local_start = max(0, abs_start - p_start_abs)
            local_end = min(len(text), abs_end - p_start_abs)
            if local_end <= local_start:
                continue
            ev = _build_page_local_evidence(page_row, local_start, local_end)
            if ev["quote"]:
                pieces.append(ev)
        if not pieces:
            continue
        raw_combined = " ".join(p["quote"] for p in pieces).strip()
        normalized_value = re.sub(r"\s+", " ", raw_combined).strip()
        if not normalized_value:
            continue
        extracted.setdefault(field_key, []).append(
            {
                "value": normalized_value,
                "evidence": pieces,
            }
        )

    out: Dict[str, Dict[str, Any]] = {}
    for field_key, blocks in extracted.items():
        if field_key == "impianti_abitazione":
            joined_value = " | ".join(b["value"] for b in blocks if b.get("value"))
            joined_evidence: List[Dict[str, Any]] = []
            for b in blocks:
                joined_evidence.extend(b.get("evidence", []))
            out[field_key] = {"value": joined_value if joined_value else "NOT_FOUND", "evidence": joined_evidence}
        else:
            out[field_key] = blocks[0]
    return out


def _extract_non_agibile(pages_window: List[Dict[str, Any]]) -> Dict[str, Any]:
    for page_row in pages_window:
        text = str(page_row["text"])
        m = NON_AGIBILE_PATTERN.search(text)
        if not m:
            continue
        start = max(0, m.start() - 90)
        end = min(len(text), m.end() + 90)
        ev = _build_page_local_evidence(page_row, start, end)
        return {"value": True, "evidence": [ev]}
    return {"value": False, "evidence": []}


def _detect_bene_bucket(text: str) -> str:
    for label, pattern in BENE_TERMS:
        if pattern.search(text or ""):
            return label
    return "GENERIC"


def _select_cost_money_candidates(money_candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    picked: List[Dict[str, Any]] = []
    seen: Set[Tuple[float, str]] = set()

    for item in money_candidates:
        if not isinstance(item, dict):
            continue
        amount = item.get("amount_eur")
        if not isinstance(amount, (int, float)):
            continue
        text = f"{item.get('context','')} {item.get('quote','')}".lower()
        if not _contains_any(text, COST_TERMS):
            continue
        quote = _clean_quote(item.get("quote"))
        key = (float(amount), quote[:120])
        if key in seen:
            continue
        seen.add(key)
        picked.append(item)

    # Deterministic order by page then amount
    picked.sort(key=lambda x: (int(x.get("page") or 0), float(x.get("amount_eur") or 0.0)))
    return picked


def _integrate_money_box_cost_items(result: Dict[str, Any], money_candidates: List[Dict[str, Any]]) -> None:
    money_box = result.get("money_box") if isinstance(result.get("money_box"), dict) else {}
    items = money_box.get("items") if isinstance(money_box.get("items"), list) else []

    selected = _select_cost_money_candidates(money_candidates)
    existing_pairs: Set[Tuple[float, int, str]] = set()
    for it in items:
        if not isinstance(it, dict):
            continue
        stima = it.get("stima_euro")
        ev = (((it.get("fonte_perizia") or {}).get("evidence")) if isinstance(it.get("fonte_perizia"), dict) else [])
        if isinstance(stima, (int, float)) and isinstance(ev, list) and ev:
            e0 = ev[0] if isinstance(ev[0], dict) else {}
            p = e0.get("page") if isinstance(e0, dict) else None
            q = _clean_quote((e0 or {}).get("quote", "")) if isinstance(e0, dict) else ""
            if isinstance(p, int) and q:
                existing_pairs.add((float(stima), int(p), q[:120]))

    added = 0
    for idx, cand in enumerate(selected, 1):
        amount = float(cand.get("amount_eur") or 0.0)
        page = cand.get("page")
        quote = _clean_quote(cand.get("quote"))
        if not isinstance(page, int) or not quote:
            continue
        normalized_quote, search_hint = normalize_evidence_quote(quote, 0, len(quote), max_len=520)
        if not normalized_quote:
            continue
        key = (amount, int(page), quote[:120])
        if key in existing_pairs:
            continue
        existing_pairs.add(key)
        evidence_payload = {"page": page, "quote": normalized_quote}
        if search_hint:
            evidence_payload["search_hint"] = search_hint
        items.append(
            {
                "code": f"S3C{idx:02d}",
                "label_it": "Costo rilevato da perizia",
                "label_en": "Cost extracted from appraisal",
                "type": "ESTIMATE",
                "stima_euro": amount,
                "stima_nota": "Deterministic candidate-based cost",
                "fonte_perizia": {
                    "value": "Perizia",
                    "evidence": [evidence_payload],
                },
                "action_required_it": "Verifica documentale",
                "action_required_en": "Document check",
                "source": "step3_candidates",
                "candidate_ids": [cand.get("id")],
            }
        )
        added += 1

    money_box["items"] = items

    has_unknown = False
    total_known = 0.0
    for it in items:
        if not isinstance(it, dict):
            continue
        stima = it.get("stima_euro")
        if isinstance(stima, (int, float)):
            total_known += float(stima)
        else:
            has_unknown = True

    if has_unknown:
        money_box["total_extra_costs"] = {
            "range": {"min": "TBD", "max": "TBD"},
            "max_is_open": True,
            "note": "TBD because one or more cost items are unspecified",
        }
    else:
        money_box["total_extra_costs"] = {
            "range": {"min": round(total_known, 2), "max": round(total_known, 2)},
            "max_is_open": False,
        }

    result["money_box"] = money_box


def build_estratto_quality(analysis_id: str, result: Dict[str, Any]) -> Dict[str, Any]:
    candidates_dir = RUNS_ROOT / analysis_id / "candidates"
    extract_dir = RUNS_ROOT / analysis_id / "extract"

    money_candidates = _read_json(candidates_dir / "candidates_money.json", [])
    date_candidates = _read_json(candidates_dir / "candidates_dates.json", [])
    trigger_candidates = _read_json(candidates_dir / "candidates_triggers.json", [])
    candidates_index = _read_json(candidates_dir / "candidates_index.json", {})
    pages_raw = _read_json(extract_dir / "pages_raw.json", [])

    if not isinstance(money_candidates, list):
        money_candidates = []
    if not isinstance(date_candidates, list):
        date_candidates = []
    if not isinstance(trigger_candidates, list):
        trigger_candidates = []
    if not isinstance(candidates_index, dict):
        candidates_index = {}
    if not isinstance(pages_raw, list):
        pages_raw = []

    # occupancy
    occupancy_items: List[Dict[str, Any]] = []
    for t in trigger_candidates:
        if not isinstance(t, dict) or t.get("family") != "occupancy":
            continue
        cid = t.get("id")
        ev = _evidence_from_candidate(t)
        kw = str(t.get("keyword") or "occupancy")
        _add_item(
            occupancy_items,
            item_id=f"occ_{cid}",
            label_it=f"Indicatore occupazione: {kw}",
            label_en=f"Occupancy indicator: {kw}",
            candidate_ids=[cid] if isinstance(cid, str) else [],
            evidence=ev,
        )

    # APE
    ape_items: List[Dict[str, Any]] = []
    for t in trigger_candidates:
        if not isinstance(t, dict) or t.get("family") != "energy":
            continue
        cid = t.get("id")
        ev = _evidence_from_candidate(t)
        kw = str(t.get("keyword") or "energy")
        _add_item(
            ape_items,
            item_id=f"ape_{cid}",
            label_it=f"Riferimento energetico: {kw}",
            label_en=f"Energy reference: {kw}",
            candidate_ids=[cid] if isinstance(cid, str) else [],
            evidence=ev,
        )

    # abusi + agibilita + costi correlati
    abusi_items: List[Dict[str, Any]] = []
    for t in trigger_candidates:
        if not isinstance(t, dict) or t.get("family") not in {"agibilita", "abusi"}:
            continue
        cid = t.get("id")
        ev = _evidence_from_candidate(t)
        fam = t.get("family")
        kw = str(t.get("keyword") or fam)
        _add_item(
            abusi_items,
            item_id=f"ab_{cid}",
            label_it=f"Rilievo {fam}: {kw}",
            label_en=f"Finding {fam}: {kw}",
            candidate_ids=[cid] if isinstance(cid, str) else [],
            evidence=ev,
        )

    for m in money_candidates:
        if not isinstance(m, dict):
            continue
        text = f"{m.get('context','')} {m.get('quote','')}".lower()
        if not _contains_any(text, ABUSI_COST_TERMS):
            continue
        cid = m.get("id")
        amount = m.get("amount_raw")
        ev = _evidence_from_candidate(m)
        _add_item(
            abusi_items,
            item_id=f"ab_cost_{cid}",
            label_it=f"Costo collegato ad abusi/agibilità: {amount}",
            label_en=f"Cost linked to compliance/habitability: {amount}",
            candidate_ids=[cid] if isinstance(cid, str) else [],
            evidence=ev,
            extra={"amount_eur": m.get("amount_eur")},
        )

    # impianti grouped by bene
    imp_buckets: Dict[str, List[Dict[str, Any]]] = {}
    for t in trigger_candidates:
        if not isinstance(t, dict) or t.get("family") != "impianti":
            continue
        cid = t.get("id")
        if not isinstance(cid, str):
            continue
        combo = f"{t.get('quote','')} {t.get('context','')}"
        bucket = _detect_bene_bucket(combo)
        imp_buckets.setdefault(bucket, []).append(t)

    impianti_items: List[Dict[str, Any]] = []
    for bucket in sorted(imp_buckets.keys()):
        bucket_items = imp_buckets[bucket]
        candidate_ids = [str(x.get("id")) for x in bucket_items if isinstance(x.get("id"), str)]
        evidence = []
        for x in bucket_items[:4]:
            evidence.extend(_evidence_from_candidate(x))
        _add_item(
            impianti_items,
            item_id=f"imp_{bucket.lower().replace(' ', '_')}",
            label_it=f"Impianti - {bucket}",
            label_en=f"Systems - {bucket}",
            candidate_ids=candidate_ids,
            evidence=evidence,
        )

    # catasto
    catasto_items: List[Dict[str, Any]] = []
    for t in trigger_candidates:
        if not isinstance(t, dict) or t.get("family") != "catasto":
            continue
        cid = t.get("id")
        ev = _evidence_from_candidate(t)
        kw = str(t.get("keyword") or "catasto")
        _add_item(
            catasto_items,
            item_id=f"cat_{cid}",
            label_it=f"Riferimento catastale: {kw}",
            label_en=f"Cadastre reference: {kw}",
            candidate_ids=[cid] if isinstance(cid, str) else [],
            evidence=ev,
        )

    # formalita (money + dates)
    formalita_items: List[Dict[str, Any]] = []
    for m in money_candidates:
        if not isinstance(m, dict):
            continue
        text = f"{m.get('context','')} {m.get('quote','')}".lower()
        if not _contains_any(text, FORMALITA_TERMS):
            continue
        cid = m.get("id")
        ev = _evidence_from_candidate(m)
        _add_item(
            formalita_items,
            item_id=f"for_m_{cid}",
            label_it=f"Importo formalità: {m.get('amount_raw')}",
            label_en=f"Encumbrance amount: {m.get('amount_raw')}",
            candidate_ids=[cid] if isinstance(cid, str) else [],
            evidence=ev,
            extra={"amount_eur": m.get("amount_eur")},
        )

    for d in date_candidates:
        if not isinstance(d, dict):
            continue
        text = f"{d.get('context','')} {d.get('quote','')}".lower()
        if not _contains_any(text, FORMALITA_DATE_TERMS):
            continue
        cid = d.get("id")
        ev = _evidence_from_candidate(d)
        _add_item(
            formalita_items,
            item_id=f"for_d_{cid}",
            label_it=f"Data formalità: {d.get('date') or d.get('time')}",
            label_en=f"Encumbrance date: {d.get('date') or d.get('time')}",
            candidate_ids=[cid] if isinstance(cid, str) else [],
            evidence=ev,
            extra={"date": d.get("date"), "time": d.get("time")},
        )

    # dati asta
    asta_items: List[Dict[str, Any]] = []
    for d in date_candidates:
        if not isinstance(d, dict):
            continue
        text = f"{d.get('context','')} {d.get('quote','')}".lower()
        if not _contains_any(text, ASTA_TERMS):
            continue
        cid = d.get("id")
        ev = _evidence_from_candidate(d)
        label_it = f"Data/ora asta: {d.get('date') or ''} {d.get('time') or ''}".strip()
        label_en = f"Auction date/time: {d.get('date') or ''} {d.get('time') or ''}".strip()
        _add_item(
            asta_items,
            item_id=f"asta_{cid}",
            label_it=label_it,
            label_en=label_en,
            candidate_ids=[cid] if isinstance(cid, str) else [],
            evidence=ev,
            extra={"date": d.get("date"), "time": d.get("time"), "relevance": d.get("relevance")},
        )

    # legal
    legal_items: List[Dict[str, Any]] = []
    for t in trigger_candidates:
        if not isinstance(t, dict) or t.get("family") != "legal":
            continue
        cid = t.get("id")
        ev = _evidence_from_candidate(t)
        kw = str(t.get("keyword") or "legal")
        _add_item(
            legal_items,
            item_id=f"leg_{cid}",
            label_it=f"Vincolo legale: {kw}",
            label_en=f"Legal constraint: {kw}",
            candidate_ids=[cid] if isinstance(cid, str) else [],
            evidence=ev,
        )

    sections = [
        _section("occupancy", occupancy_items),
        _section("ape", ape_items),
        _section("abusi_agibilita", abusi_items),
        _section("impianti", impianti_items),
        _section("catasto", catasto_items),
        _section("formalita", formalita_items),
        _section("dati_asta", asta_items),
        _section("legal", legal_items),
    ]

    # HARD WAY section-bounded extraction from Step1 pages_raw (not estratto)
    page_rows = _normalized_page_rows(pages_raw)
    impianti_window = _find_section_window(
        page_rows,
        [re.compile(r"^\s*STATO\s+DEGLI\s+IMPIANTI\b", re.IGNORECASE)],
    )
    if not impianti_window:
        impianti_window = _fallback_impianti_window_from_labels(page_rows)
    abusi_window = _find_section_window(
        page_rows,
        [
            re.compile(r"^\s*REGOLARIT[ÀA]\s+EDILIZIA\b", re.IGNORECASE),
            re.compile(r"^\s*CONFORMIT[ÀA]\s+EDILIZIA\b", re.IGNORECASE),
            re.compile(r"^\s*REGOLARIT[ÀA]\s+URBANISTICA\b", re.IGNORECASE),
            re.compile(r"^\s*ABUSI\b", re.IGNORECASE),
        ],
    )
    impianti_pages = _pages_in_window(page_rows, impianti_window)
    abusi_pages = _pages_in_window(page_rows, abusi_window)

    impianti_extracted = _extract_label_blocks(impianti_pages, IMPIANTI_LABEL_PATTERNS)
    non_agibile_extracted = _extract_non_agibile(abusi_pages)

    def _upsert_quality_item(
        section_key: str,
        field_key: str,
        label_it: str,
        label_en: str,
        value_obj: Dict[str, Any],
    ) -> None:
        sec = next((s for s in sections if s.get("heading_key") == section_key), None)
        if not sec:
            sec = _section(section_key, [])
            sections.append(sec)
        items = sec.get("items")
        if not isinstance(items, list):
            items = []
            sec["items"] = items
        existing = next((x for x in items if isinstance(x, dict) and x.get("field_key") == field_key), None)
        payload = {
            "item_id": f"bp_{field_key}",
            "field_key": field_key,
            "key": field_key,
            "label_it": label_it,
            "label_en": label_en,
            "value_it": str(value_obj.get("value")),
            "value_en": str(value_obj.get("value")),
            "evidence": value_obj.get("evidence", []),
        }
        if existing:
            existing.update(payload)
        else:
            items.append(payload)

    blueprint_abusi = {
        "non_agibile": {
            "value": bool(non_agibile_extracted.get("value")),
            "evidence": non_agibile_extracted.get("evidence", []),
        }
    }
    if not blueprint_abusi["non_agibile"]["evidence"]:
        blueprint_abusi["non_agibile"]["searched_in"] = [p.get("page") for p in abusi_pages[:8]]

    blueprint_impianti: Dict[str, Dict[str, Any]] = {}
    for field_key in (
        "impianti_ufficio",
        "impianti_rustico",
        "impianti_garage",
        "impianti_abitazione",
    ):
        found = impianti_extracted.get(field_key)
        if found and found.get("value"):
            blueprint_impianti[field_key] = {
                "value": found.get("value"),
                "evidence": found.get("evidence", []),
            }
        else:
            blueprint_impianti[field_key] = {
                "value": "NOT_FOUND",
                "evidence": [],
                "searched_in": [p.get("page") for p in impianti_pages[:8]],
            }

    result["estratto_blueprint"] = {
        "abusi": blueprint_abusi,
        "impianti": blueprint_impianti,
    }

    _upsert_quality_item(
        "abusi_agibilita",
        "non_agibile",
        "Non agibile",
        "Not habitable",
        blueprint_abusi["non_agibile"],
    )
    _upsert_quality_item(
        "impianti",
        "impianti_ufficio",
        "Impianti ufficio",
        "Office systems",
        blueprint_impianti["impianti_ufficio"],
    )
    _upsert_quality_item(
        "impianti",
        "impianti_rustico",
        "Impianti rustico",
        "Rustic systems",
        blueprint_impianti["impianti_rustico"],
    )
    _upsert_quality_item(
        "impianti",
        "impianti_garage",
        "Impianti garage",
        "Garage systems",
        blueprint_impianti["impianti_garage"],
    )
    _upsert_quality_item(
        "impianti",
        "impianti_abitazione",
        "Impianti abitazione",
        "Home systems",
        blueprint_impianti["impianti_abitazione"],
    )

    mirror_sections: List[Dict[str, Any]] = []
    for sec in sections:
        if not isinstance(sec, dict):
            continue
        sec_name = sec.get("heading_it") or sec.get("heading_key") or "SECTION"
        out_items: List[Dict[str, Any]] = []
        for it in sec.get("items", []) if isinstance(sec.get("items"), list) else []:
            if not isinstance(it, dict):
                continue
            key = it.get("field_key") or it.get("key") or it.get("item_id")
            if not key:
                continue
            val = it.get("value_it")
            if val is None:
                val = it.get("value")
            out_items.append({"key": str(key), "value": val, "evidence": it.get("evidence", [])})
        mirror_sections.append({"name": sec_name, "items": out_items})
    result["estratto_mirror"] = {"sections": mirror_sections}

    _integrate_money_box_cost_items(result, money_candidates)

    low_quality_pages = candidates_index.get("low_quality_pages", [])
    if not isinstance(low_quality_pages, list):
        low_quality_pages = []

    build_meta = {
        "analysis_id": analysis_id,
        "candidate_counts": {
            "money": len(money_candidates),
            "dates": len(date_candidates),
            "triggers": len(trigger_candidates),
            "pages": len(pages_raw),
        },
        "low_quality_pages": [int(x) for x in low_quality_pages if isinstance(x, int)],
        "section_windows": {
            "impianti": impianti_window,
            "abusi": abusi_window,
        },
    }

    return {
        "sections": sections,
        "build_meta": build_meta,
    }
