import hashlib
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from perizia_section_authority import classify_quote_authority

RUNS_ROOT = Path("/srv/perizia/_qa/runs")

_AMOUNT_TOKEN_RE = re.compile(r"\d{1,3}(?:\.\d{3})*(?:,\d{2})?|\d+(?:,\d{2})?")
_EURO_WORD_RE = re.compile(r"\beuro\b", flags=re.IGNORECASE)
_DATE_RE = re.compile(r"\b([0-3]\d/[01]\d/\d{4})\b")
_TIME_RE = re.compile(r"\b(?:ore\s*)?([01]\d|2[0-3])[:.]([0-5]\d)\b", flags=re.IGNORECASE)

_UNIT_GUARD_TERMS = (
    "mq",
    "vani",
    "sub",
    "foglio",
    "particella",
    "catasto",
    "cl.",
    "cons.",
)

_HIGH_RELEVANCE_TERMS = ("asta", "vendita", "delegato", "pignoramento", "trascrizione", "notifica")
_LOW_RELEVANCE_TERMS = ("pdg", "pubblicazione eseguita")

_TRIGGER_DEFS: List[Tuple[str, str, re.Pattern[str]]] = [
    ("occupancy", "occupato", re.compile(r"\boccupat\w*\b", re.IGNORECASE)),
    ("occupancy", "debitore", re.compile(r"\bdebitor\w*\b", re.IGNORECASE)),
    ("occupancy", "libero", re.compile(r"\bliber\w*\b", re.IGNORECASE)),
    ("occupancy", "familia", re.compile(r"\bfamili\w*\b", re.IGNORECASE)),
    ("energy", "APE", re.compile(r"\bAPE\b", re.IGNORECASE)),
    ("energy", "attestato di prestazione energetica", re.compile(r"\battestato\s+di\s+prestazione\s+energetica\b", re.IGNORECASE)),
    ("energy", "certificato energetico", re.compile(r"\bcertificat\w*\s+energetic\w*\b", re.IGNORECASE)),
    ("agibilita", "agibilità", re.compile(r"\bagibilit(?:à|a)\b", re.IGNORECASE)),
    ("agibilita", "abitabilità", re.compile(r"\babitabilit(?:à|a)\b", re.IGNORECASE)),
    ("agibilita", "non agibile", re.compile(r"\bnon\s+agibil\w*\b", re.IGNORECASE)),
    ("impianti", "impianto elettrico", re.compile(r"\bimpiant\w*\s+elettric\w*\b", re.IGNORECASE)),
    ("impianti", "idrico", re.compile(r"\bidric\w*\b", re.IGNORECASE)),
    ("impianti", "gas", re.compile(r"\bgas\b", re.IGNORECASE)),
    ("impianti", "riscaldamento", re.compile(r"\briscaldament\w*\b", re.IGNORECASE)),
    ("impianti", "contatore", re.compile(r"\bcontator\w*\b", re.IGNORECASE)),
    ("impianti", "caldaia", re.compile(r"\bcaldai\w*\b", re.IGNORECASE)),
    ("catasto", "dati catastali", re.compile(r"\bdati\s+catastal\w*\b", re.IGNORECASE)),
    ("catasto", "foglio", re.compile(r"\bfogli\w*\b", re.IGNORECASE)),
    ("catasto", "particella", re.compile(r"\bparticell\w*\b", re.IGNORECASE)),
    ("catasto", "sub", re.compile(r"\bsub\b", re.IGNORECASE)),
    ("catasto", "categoria", re.compile(r"\bcategori\w*\b", re.IGNORECASE)),
    ("catasto", "rendita", re.compile(r"\brendit\w*\b", re.IGNORECASE)),
    ("catasto", "vani", re.compile(r"\bvani\b", re.IGNORECASE)),
    ("abusi", "difformità", re.compile(r"\bdifformit(?:à|a)\b", re.IGNORECASE)),
    ("abusi", "incongruenza", re.compile(r"\bincongruenz\w*\b", re.IGNORECASE)),
    ("abusi", "sanatoria", re.compile(r"\bsanatori\w*\b", re.IGNORECASE)),
    ("abusi", "regolarizzazione", re.compile(r"\bregolarizzazion\w*\b", re.IGNORECASE)),
    ("abusi", "abuso", re.compile(r"\babus\w*\b", re.IGNORECASE)),
    ("abusi", "oblazione", re.compile(r"\boblazion\w*\b", re.IGNORECASE)),
    ("legal", "servitù", re.compile(r"\bservit(?:ù|u)\b", re.IGNORECASE)),
    ("legal", "usi civici", re.compile(r"\busi\s+civic\w*\b", re.IGNORECASE)),
    ("legal", "ipoteca", re.compile(r"\bipotec\w*\b", re.IGNORECASE)),
    ("legal", "pignoramento", re.compile(r"\bpignorament\w*\b", re.IGNORECASE)),
    ("legal", "vincolo", re.compile(r"\bvincol\w*\b", re.IGNORECASE)),
    ("legal", "trascrizione", re.compile(r"\btrascrizion\w*\b", re.IGNORECASE)),
]


def _read_json(path: Path, fallback: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return fallback


def _write_json(path: Path, payload: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _context(text: str, start: int, end: int, radius: int) -> str:
    left = max(0, start - radius)
    right = min(len(text), end + radius)
    return text[left:right].strip()


def _quote(text: str, start: int, end: int, radius: int = 70) -> str:
    q = _context(text, start, end, radius)
    return q if q else text[start:end]


def _parse_it_amount_to_eur(raw_amount: str) -> Optional[float]:
    match = _AMOUNT_TOKEN_RE.search(raw_amount)
    if not match:
        return None
    number_text = match.group(0)
    normalized = number_text.replace(".", "").replace(",", ".")
    try:
        return float(normalized)
    except Exception:
        return None


def _normalize_text_bucket(text: str, max_len: int = 160) -> str:
    lowered = text.lower()
    lowered = re.sub(r"\d+", "#", lowered)
    lowered = re.sub(r"\s+", " ", lowered).strip()
    return lowered[:max_len]


def _context_bucket_for_span(text: str, start: int, end: int) -> str:
    snippet = _context(text, start, end, 80).lower()
    snippet = re.sub(r"\d+", "#", snippet)
    snippet = re.sub(r"[^a-z0-9àèéìòù#]+", " ", snippet)
    snippet = re.sub(r"\s+", " ", snippet).strip()
    tokens = snippet.split(" ")
    return " ".join(tokens[:24])


def _normalize_quote_hash(quote: str) -> str:
    norm = _normalize_text_bucket(quote, max_len=240)
    return hashlib.sha1(norm.encode("utf-8")).hexdigest()[:16]


def _load_step1_inputs(analysis_id: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    extract_dir = RUNS_ROOT / analysis_id / "extract"
    pages_raw = _read_json(extract_dir / "pages_raw.json", [])
    metrics = _read_json(extract_dir / "metrics.json", [])
    ocr_plan = _read_json(extract_dir / "ocr_plan.json", [])
    return pages_raw if isinstance(pages_raw, list) else [], metrics if isinstance(metrics, list) else [], ocr_plan if isinstance(ocr_plan, list) else []


def _load_section_authority_map(analysis_id: str) -> Dict[str, Any]:
    extract_dir = RUNS_ROOT / analysis_id / "extract"
    path = extract_dir / "section_authority.json"
    if not path.exists():
        return {"_authority_tagging_status": "missing_map"}
    try:
        with open(path, "r", encoding="utf-8") as f:
            section_map = json.load(f)
    except Exception as exc:
        return {
            "_authority_tagging_status": "corrupt_map",
            "_authority_tagging_error": str(exc)[:240],
        }
    if not isinstance(section_map, dict):
        return {
            "_authority_tagging_status": "corrupt_map",
            "_authority_tagging_error": "section_authority.json root is not an object",
        }
    return section_map


def _low_quality_pages(metrics: List[Dict[str, Any]], ocr_plan: List[Dict[str, Any]]) -> List[int]:
    flagged: Set[int] = set()
    for row in metrics:
        if not isinstance(row, dict):
            continue
        page = row.get("page")
        if not isinstance(page, int):
            continue
        try:
            garbage_ratio = float(row.get("garbage_ratio", 0.0) or 0.0)
        except Exception:
            garbage_ratio = 0.0
        if garbage_ratio > 0.25:
            flagged.add(page)
    for row in ocr_plan:
        if isinstance(row, dict) and isinstance(row.get("page"), int):
            flagged.add(int(row["page"]))
    return sorted(flagged)


def _reject_unit_context(text: str, token_start: int, token_end: int) -> bool:
    snippet = text[max(0, token_start - 10): min(len(text), token_end + 10)].lower()
    return any(term in snippet for term in _UNIT_GUARD_TERMS)


def _closest_amount_for_anchor(text: str, anchor_start: int, anchor_end: int) -> Optional[Tuple[int, int, str]]:
    window_left = max(0, anchor_start - 25)
    window_right = min(len(text), anchor_end + 25)
    best: Optional[Tuple[int, int, str, int]] = None

    for m in _AMOUNT_TOKEN_RE.finditer(text, window_left, window_right):
        token_start, token_end = m.start(), m.end()
        if _reject_unit_context(text, token_start, token_end):
            continue
        token_center = (token_start + token_end) // 2
        anchor_center = (anchor_start + anchor_end) // 2
        distance = abs(token_center - anchor_center)
        if best is None or distance < best[3] or (distance == best[3] and token_start < best[0]):
            best = (token_start, token_end, m.group(0), distance)

    if best is None:
        return None
    return best[0], best[1], best[2]


def _build_amount_raw(text: str, anchor_start: int, anchor_end: int, token_start: int, token_end: int) -> str:
    left = min(anchor_start, token_start)
    right = max(anchor_end, token_end)
    raw = text[left:right].strip()
    raw = re.sub(r"\s+", " ", raw)
    return raw


def _merge_occurrence(occurrences: List[Dict[str, Any]], page: int, quote: str) -> None:
    for occ in occurrences:
        if occ.get("page") == page and occ.get("quote") == quote:
            return
    occurrences.append({"page": page, "quote": quote})


def _mine_money(pages_raw: List[Dict[str, Any]], low_pages: Set[int]) -> List[Dict[str, Any]]:
    merged: Dict[Tuple[float, str], Dict[str, Any]] = {}

    for page_obj in pages_raw:
        if not isinstance(page_obj, dict):
            continue
        page = page_obj.get("page")
        text = page_obj.get("text")
        if not isinstance(page, int) or not isinstance(text, str) or not text:
            continue

        anchors: List[Tuple[int, int]] = []
        for m in re.finditer(r"€", text):
            anchors.append((m.start(), m.end()))
        for m in _EURO_WORD_RE.finditer(text):
            anchors.append((m.start(), m.end()))
        anchors.sort(key=lambda x: x[0])

        for anchor_start, anchor_end in anchors:
            closest = _closest_amount_for_anchor(text, anchor_start, anchor_end)
            if closest is None:
                continue
            token_start, token_end, _token = closest

            amount_raw = _build_amount_raw(text, anchor_start, anchor_end, token_start, token_end)
            amount_eur = _parse_it_amount_to_eur(amount_raw)
            if amount_eur is None:
                continue

            item_start = min(anchor_start, token_start)
            item_end = max(anchor_end, token_end)
            quote = _quote(text, item_start, item_end)
            context = _context(text, item_start, item_end, 200)
            quote_hash = _normalize_quote_hash(quote)
            dedupe_key = (amount_eur, quote_hash)

            existing = merged.get(dedupe_key)
            if existing is None:
                merged[dedupe_key] = {
                    "page": page,
                    "amount_raw": amount_raw,
                    "amount_eur": amount_eur,
                    "quote": quote,
                    "context": context,
                    "low_quality_page": page in low_pages,
                    "source": "pages_raw",
                    "occurrences": [{"page": page, "quote": quote}],
                }
            else:
                _merge_occurrence(existing["occurrences"], page, quote)
                existing["low_quality_page"] = bool(existing.get("low_quality_page")) or (page in low_pages)

    out = sorted(merged.values(), key=lambda x: (x.get("page", 0), float(x.get("amount_eur", 0.0) or 0.0), x.get("quote", "")))
    for i, row in enumerate(out, 1):
        row["id"] = f"m_{i:06d}"
    return out


def _nearest_time(text: str, start: int, end: int) -> Optional[str]:
    left = max(0, start - 60)
    right = min(len(text), end + 60)
    window = text[left:right]
    best: Optional[Tuple[int, str]] = None
    for tm in _TIME_RE.finditer(window):
        hhmm = f"{tm.group(1)}:{tm.group(2)}"
        dist = min(abs((left + tm.start()) - start), abs((left + tm.end()) - end))
        if best is None or dist < best[0]:
            best = (dist, hhmm)
    return best[1] if best else None


def _relevance_from_context(context: str) -> str:
    c = context.lower()
    if any(term in c for term in _LOW_RELEVANCE_TERMS):
        return "LOW"
    if any(term in c for term in _HIGH_RELEVANCE_TERMS):
        return "HIGH"
    return "MEDIUM"


def _mine_dates(pages_raw: List[Dict[str, Any]], low_pages: Set[int]) -> List[Dict[str, Any]]:
    merged: Dict[Tuple[Optional[str], Optional[str], str], Dict[str, Any]] = {}

    for page_obj in pages_raw:
        if not isinstance(page_obj, dict):
            continue
        page = page_obj.get("page")
        text = page_obj.get("text")
        if not isinstance(page, int) or not isinstance(text, str) or not text:
            continue

        for m in _DATE_RE.finditer(text):
            start, end = m.start(), m.end()
            date_text = m.group(1)
            time_text = _nearest_time(text, start, end)
            quote = _quote(text, start, end)
            context = _context(text, start, end, 200)
            bucket = _context_bucket_for_span(text, start, end)
            key = (date_text, time_text, bucket)

            existing = merged.get(key)
            if existing is None:
                merged[key] = {
                    "page": page,
                    "date": date_text,
                    "time": time_text,
                    "quote": quote,
                    "context": context,
                    "relevance": _relevance_from_context(context),
                    "low_quality_page": page in low_pages,
                    "source": "pages_raw",
                    "occurrences": [{"page": page, "quote": quote}],
                }
            else:
                _merge_occurrence(existing["occurrences"], page, quote)
                existing["low_quality_page"] = bool(existing.get("low_quality_page")) or (page in low_pages)

        for m in _TIME_RE.finditer(text):
            start, end = m.start(), m.end()
            time_text = f"{m.group(1)}:{m.group(2)}"
            quote = _quote(text, start, end)
            context = _context(text, start, end, 200)
            bucket = _context_bucket_for_span(text, start, end)
            key = (None, time_text, bucket)

            existing = merged.get(key)
            if existing is None:
                merged[key] = {
                    "page": page,
                    "date": None,
                    "time": time_text,
                    "quote": quote,
                    "context": context,
                    "relevance": _relevance_from_context(context),
                    "low_quality_page": page in low_pages,
                    "source": "pages_raw",
                    "occurrences": [{"page": page, "quote": quote}],
                }
            else:
                _merge_occurrence(existing["occurrences"], page, quote)
                existing["low_quality_page"] = bool(existing.get("low_quality_page")) or (page in low_pages)

    out = sorted(
        merged.values(),
        key=lambda x: (
            x.get("page", 0),
            x.get("date") or "",
            x.get("time") or "",
            x.get("relevance") or "",
            x.get("quote") or "",
        ),
    )
    for i, row in enumerate(out, 1):
        row["id"] = f"d_{i:06d}"
    return out


def _mine_triggers(pages_raw: List[Dict[str, Any]], low_pages: Set[int]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    spans: List[Tuple[int, int, int, str, str, str, str, bool]] = []
    for page_obj in pages_raw:
        if not isinstance(page_obj, dict):
            continue
        page = page_obj.get("page")
        text = page_obj.get("text")
        if not isinstance(page, int) or not isinstance(text, str) or not text:
            continue

        for family, keyword, pattern in _TRIGGER_DEFS:
            for m in pattern.finditer(text):
                start, end = m.start(), m.end()
                spans.append(
                    (
                        page,
                        start,
                        end,
                        family,
                        keyword,
                        _quote(text, start, end, radius=55),
                        _context(text, start, end, 125),
                        page in low_pages,
                    )
                )

    spans.sort(key=lambda x: (x[0], x[1], x[2], x[3], x[4]))
    for i, item in enumerate(spans, 1):
        out.append(
            {
                "id": f"t_{i:06d}",
                "page": item[0],
                "family": item[3],
                "keyword": item[4],
                "quote": item[5],
                "context": item[6],
                "low_quality_page": item[7],
                "source": "pages_raw",
            }
        )
    return out


def _authority_domain_for_trigger_family(family: str) -> str:
    mapping = {
        "occupancy": "occupancy",
        "energy": "agibilita",
        "agibilita": "agibilita",
        "impianti": "urbanistica",
        "catasto": "catasto",
        "abusi": "urbanistica",
        "legal": "legal_formalities",
    }
    return mapping.get(str(family or ""), str(family or "") or "unknown")


def _attach_authority_shadow(
    candidates: List[Dict[str, Any]],
    section_map: Dict[str, Any],
    *,
    default_domain: Optional[str] = None,
) -> Dict[str, Any]:
    status = str(section_map.get("_authority_tagging_status") or "").strip() if isinstance(section_map, dict) else ""
    if status == "missing_map":
        return {"enabled": False, "status": "missing_map", "tagged_count": 0, "missing_map": True}
    if status == "corrupt_map":
        return {
            "enabled": False,
            "status": "corrupt_map",
            "tagged_count": 0,
            "missing_map": False,
            "corrupt_map": True,
            "error": str(section_map.get("_authority_tagging_error") or "")[:240],
        }
    if not isinstance(section_map, dict) or not isinstance(section_map.get("pages"), list):
        return {"enabled": False, "status": "invalid_map", "tagged_count": 0, "missing_map": False, "invalid_map": True}

    tagged = 0
    failed = 0
    first_error: Optional[str] = None
    authority_levels: Dict[str, int] = {}
    zones: Dict[str, int] = {}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        try:
            page = int(candidate.get("page"))
        except Exception:
            continue
        quote = str(candidate.get("quote") or candidate.get("context") or "")
        if not quote.strip():
            continue
        domain = default_domain
        if default_domain is None and candidate.get("family"):
            domain = _authority_domain_for_trigger_family(str(candidate.get("family") or ""))
        try:
            authority = classify_quote_authority(page, quote, section_map, domain=domain)
        except Exception as exc:
            failed += 1
            if first_error is None:
                first_error = str(exc)[:240]
            candidate["authority_tagging_error"] = "authority_shadow_failed_open"
            continue
        for key in (
            "section_zone",
            "authority_level",
            "authority_score",
            "domain_hints",
            "answer_point",
            "is_instruction_like",
            "is_answer_like",
            "reason_for_authority",
        ):
            candidate[key] = authority.get(key)
        if authority.get("domain_hint") is not None:
            candidate["domain_hint"] = authority.get("domain_hint")
        tagged += 1
        level = str(candidate.get("authority_level") or "UNKNOWN")
        zone = str(candidate.get("section_zone") or "UNKNOWN_FACTUAL")
        authority_levels[level] = authority_levels.get(level, 0) + 1
        zones[zone] = zones.get(zone, 0) + 1

    summary = {
        "enabled": True,
        "status": "partial" if failed else "tagged",
        "tagged_count": tagged,
        "failed_count": failed,
        "missing_map": False,
        "authority_level_counts": dict(sorted(authority_levels.items())),
        "section_zone_counts": dict(sorted(zones.items())),
    }
    if first_error:
        summary["first_error"] = first_error
    return summary


def run_candidate_miner_for_analysis(analysis_id: str) -> Dict[str, Any]:
    pages_raw, metrics, ocr_plan = _load_step1_inputs(analysis_id)
    low_quality_list = _low_quality_pages(metrics, ocr_plan)
    low_quality_set = set(low_quality_list)

    money = _mine_money(pages_raw, low_quality_set)
    dates = _mine_dates(pages_raw, low_quality_set)
    triggers = _mine_triggers(pages_raw, low_quality_set)
    section_map = _load_section_authority_map(analysis_id)
    money_authority_summary = _attach_authority_shadow(money, section_map, default_domain="money")
    trigger_authority_summary = _attach_authority_shadow(triggers, section_map)

    candidates_dir = RUNS_ROOT / analysis_id / "candidates"
    if candidates_dir.exists():
        for file_path in candidates_dir.glob("*.json"):
            try:
                file_path.unlink()
            except Exception:
                pass
    candidates_dir.mkdir(parents=True, exist_ok=True)

    index_payload = {
        "analysis_id": analysis_id,
        "money_count": len(money),
        "date_count": len(dates),
        "trigger_count": len(triggers),
        "low_quality_pages": low_quality_list,
        "authority_tagging": {
            "money": money_authority_summary,
            "triggers": trigger_authority_summary,
        },
        "files": {
            "candidates_money": str(candidates_dir / "candidates_money.json"),
            "candidates_dates": str(candidates_dir / "candidates_dates.json"),
            "candidates_triggers": str(candidates_dir / "candidates_triggers.json"),
            "candidates_index": str(candidates_dir / "candidates_index.json"),
            "candidates_full": str(candidates_dir / "candidates_full.json"),
        },
    }

    _write_json(candidates_dir / "candidates_money.json", money)
    _write_json(candidates_dir / "candidates_dates.json", dates)
    _write_json(candidates_dir / "candidates_triggers.json", triggers)
    _write_json(candidates_dir / "candidates_index.json", index_payload)
    _write_json(
        candidates_dir / "candidates_full.json",
        {
            "analysis_id": analysis_id,
            "money": money,
            "dates": dates,
            "triggers": triggers,
            "authority_tagging": {
                "money": money_authority_summary,
                "triggers": trigger_authority_summary,
            },
        },
    )

    return {
        "money_count": len(money),
        "date_count": len(dates),
        "trigger_count": len(triggers),
        "low_quality_pages": low_quality_list,
        "candidates_folder": f"{candidates_dir}/",
        "authority_tagging": {
            "money": money_authority_summary,
            "triggers": trigger_authority_summary,
        },
    }
