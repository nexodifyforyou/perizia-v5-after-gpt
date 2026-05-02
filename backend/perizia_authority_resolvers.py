"""Shadow-only authority-aware resolvers for PeriziaScan.

These resolvers answer "what would the authority-aware result be?" without
writing to customer-facing fields. They intentionally return structured debug
payloads only, so Phase 3A can be compared before any replacement is enabled.
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from perizia_section_authority import (
    AUTH_HIGH,
    AUTH_LOW,
    AUTH_MEDIUM,
    AUTH_UNKNOWN,
    ZONE_ANSWER,
    ZONE_CONTEXT,
    ZONE_FINAL_LOT,
    ZONE_FINAL_VALUATION,
    ZONE_FORMALITIES,
    ZONE_INSTRUCTION,
    ZONE_QUESTION,
    ZONE_TOC,
    ZONE_UNKNOWN,
    classify_quote_authority,
)


SCHEMA_VERSION = "perizia_authority_resolvers_v1"

STATUS_OK = "OK"
STATUS_WARN = "WARN"
STATUS_PARTIAL = "PARTIAL"
STATUS_FAIL_OPEN = "FAIL_OPEN"
STATUS_INSUFFICIENT = "INSUFFICIENT_EVIDENCE"

LOT_FINAL_ZONES = {ZONE_FINAL_LOT, ZONE_FINAL_VALUATION, ZONE_ANSWER}
FACTUAL_ZONES = {ZONE_FINAL_LOT, ZONE_FINAL_VALUATION, ZONE_FORMALITIES, ZONE_ANSWER}
LOW_AUTHORITY_ZONES = {ZONE_TOC, ZONE_INSTRUCTION, ZONE_QUESTION, ZONE_CONTEXT}

LOT_UNICO_RE = re.compile(
    r"\b(?:lotto\s+unico|unico\s+lotto|vendibil[ei]\s+in\s+un\s+unico\s+lotto)\b",
    flags=re.IGNORECASE | re.UNICODE,
)
LOT_NUMBER_RE = re.compile(
    r"\blotto\s*(?:n\.?|nr\.?|numero)?\s*([1-9]\d*)\b",
    flags=re.IGNORECASE | re.UNICODE,
)
CHAPTER_LOT_HEADING_RE = re.compile(
    r"(?im)^\s*lotto\s*(?:n\.?|nr\.?|numero)?\s*([1-9]\d*)\s*$",
    flags=re.IGNORECASE | re.UNICODE | re.MULTILINE,
)
CHAPTER_IDENTIFICATION_RE = re.compile(
    r"\b1\s*[\.\)]\s*identificazione\s+dei\s+beni\s+immobili\s+oggetto\s+di\s+vendita\b",
    flags=re.IGNORECASE | re.UNICODE,
)
CHAPTER_ASSET_QUOTA_RE = re.compile(
    r"\b(appartamento|abitazione|box|garage|autorimessa|cantina|deposito|fabbricato|terreno|locale|unita\s+immobiliare|negozio|ufficio|magazzino)\b"
    r".{0,260}\bquota\s+di\b",
    flags=re.IGNORECASE | re.UNICODE | re.DOTALL,
)
CHAPTER_SUPPORT_PATTERNS = {
    "valuation": re.compile(
        r"\b(valutazione|valore\s+di\s+mercato|valore\s+di\s+vendita\s+giudiziaria|valore\s+di\s+stima|data\s+della\s+valutazione)\b",
        flags=re.IGNORECASE | re.UNICODE,
    ),
    "possession": re.compile(
        r"\b(stato\s+di\s+possesso|al\s+momento\s+del\s+sopralluogo|sopralluogo|occupat\w*|liber[oaie])\b",
        flags=re.IGNORECASE | re.UNICODE,
    ),
    "conformity": re.compile(
        r"\b(conformit[àa]|regolarit[àa]\s+(?:urbanistica|edilizia|catastale)|identificazione\s+catastale|catasto)\b",
        flags=re.IGNORECASE | re.UNICODE,
    ),
    "legal": re.compile(
        r"\b(vincoli\s+ed\s+oneri\s+giuridici|formalit[àa]|pignorament\w*|ipotec\w*)\b",
        flags=re.IGNORECASE | re.UNICODE,
    ),
}
MONEY_AMOUNT_RE = re.compile(
    r"(?:\u20ac|\beuro\b)\s*\d{1,3}(?:\.\d{3})*(?:,\d{2})?|"
    r"\d{1,3}(?:\.\d{3})*(?:,\d{2})?\s*(?:\u20ac|\beuro\b)|"
    r"(?:\u20ac|\beuro\b)\s*\d+(?:[\.,]\d{2})?",
    flags=re.IGNORECASE | re.UNICODE,
)
MONEY_ROLES = [
    "buyer_cost_signal_to_verify",
    "valuation_deduction",
    "price",
    "base_auction",
    "final_value",
    "market_value",
    "cadastral_rendita",
    "formalities_procedural_amount",
    "component_of_total",
    "total_candidate",
    "condominium_arrears",
    "unknown_money",
]


def _normalize_text(text: Any) -> str:
    raw = str(text or "")
    raw = raw.replace("’", "'").replace("`", "'").replace("´", "'")
    decomposed = unicodedata.normalize("NFKD", raw)
    without_marks = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return without_marks.lower()


def _page_number(page: Dict[str, Any], default: int) -> int:
    for key in ("page", "page_number", "page_num"):
        try:
            value = int(page.get(key))
            if value > 0:
                return value
        except Exception:
            continue
    return default


def _page_text(page: Dict[str, Any]) -> str:
    return str(page.get("text") or "")


def _page_rows(section_map: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    rows: Dict[int, Dict[str, Any]] = {}
    for row in section_map.get("pages") or []:
        if not isinstance(row, dict):
            continue
        try:
            rows[int(row.get("page"))] = row
        except Exception:
            continue
    return rows


def _as_pages(pages: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for idx, page in enumerate(pages or [], start=1):
        if not isinstance(page, dict):
            continue
        out.append({"page": _page_number(page, idx), "text": _page_text(page)})
    out.sort(key=lambda row: row["page"])
    return out


def _section_map_fail_reason(section_map: Any) -> Optional[str]:
    if not isinstance(section_map, dict):
        return "missing_or_invalid_section_authority_map"
    status = str(section_map.get("_authority_tagging_status") or "").strip()
    if status == "missing_map":
        return "missing_section_authority_map"
    if status == "corrupt_map":
        return "corrupt_section_authority_map"
    pages = section_map.get("pages")
    if not isinstance(pages, list):
        return "invalid_section_authority_map"
    return None


def _mostly_unknown_authority(section_map: Dict[str, Any]) -> bool:
    pages = [row for row in section_map.get("pages") or [] if isinstance(row, dict)]
    if not pages:
        return True
    unknown = sum(1 for row in pages if str(row.get("authority_level") or AUTH_UNKNOWN) == AUTH_UNKNOWN)
    return unknown / max(len(pages), 1) > 0.75


def _safe_float(value: Any, default: float = 0.3) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _line_snippet(text: str, start: int, end: int, radius: int = 170) -> str:
    left_break = text.rfind("\n", 0, start)
    right_break = text.find("\n", end)
    if left_break == -1 or start - left_break > radius:
        left = max(0, start - radius)
    else:
        left = left_break + 1
    if right_break == -1 or right_break - end > radius:
        right = min(len(text), end + radius)
    else:
        right = right_break
    snippet = re.sub(r"\s+", " ", text[left:right]).strip()
    return snippet[:520]


def _dedupe_evidence(items: Iterable[Dict[str, Any]], limit: int = 12) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        sig = (item.get("page"), item.get("quote"), item.get("signal"), item.get("status"), item.get("role"))
        if sig in seen:
            continue
        seen.add(sig)
        out.append(item)
        if len(out) >= limit:
            break
    return out


def _basis(winning: Sequence[Dict[str, Any]], rules: Sequence[str]) -> Dict[str, Any]:
    zones: List[str] = []
    levels: List[str] = []
    pages: List[int] = []
    for ev in winning:
        zone = str(ev.get("section_zone") or "")
        level = str(ev.get("authority_level") or "")
        if zone and zone not in zones:
            zones.append(zone)
        if level and level not in levels:
            levels.append(level)
        try:
            page = int(ev.get("page"))
        except Exception:
            continue
        if page not in pages:
            pages.append(page)
    pages.sort()
    return {
        "zones_used": zones,
        "authority_levels_used": levels,
        "pages_used": pages,
        "rules_triggered": list(dict.fromkeys(rule for rule in rules if rule)),
    }


def _result(
    domain: str,
    *,
    status: str,
    value: Dict[str, Any],
    confidence: float,
    winning_evidence: Optional[List[Dict[str, Any]]] = None,
    rejected_conflicts: Optional[List[Dict[str, Any]]] = None,
    rules: Optional[List[str]] = None,
    fail_open: bool = False,
    notes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    winning = _dedupe_evidence(winning_evidence or [], limit=16)
    rejected = _dedupe_evidence(rejected_conflicts or [], limit=20)
    return {
        "domain": domain,
        "status": status,
        "value": value,
        "confidence": round(max(0.0, min(1.0, float(confidence))), 4),
        "winning_evidence": winning,
        "rejected_conflicts": rejected,
        "authority_basis": _basis(winning, rules or []),
        "fail_open": bool(fail_open),
        "notes": list(dict.fromkeys(note for note in (notes or []) if note)),
    }


def _domain_fail_open(domain: str, value: Dict[str, Any], reason: str) -> Dict[str, Any]:
    return _result(
        domain,
        status=STATUS_FAIL_OPEN,
        value=value,
        confidence=0.0,
        fail_open=True,
        notes=[reason],
    )


def _fallback_authority(page: int, rows: Dict[int, Dict[str, Any]]) -> Dict[str, Any]:
    row = rows.get(int(page), {})
    return {
        "page": int(page),
        "section_zone": str(row.get("zone") or ZONE_UNKNOWN),
        "authority_level": str(row.get("authority_level") or AUTH_UNKNOWN),
        "authority_score": round(_safe_float(row.get("authority_score"), 0.3), 4),
        "domain_hints": list(row.get("domain_hints") or []) if isinstance(row.get("domain_hints"), list) else [],
        "answer_point": row.get("answer_point"),
        "is_instruction_like": bool(row.get("is_instruction_like")),
        "is_answer_like": bool(row.get("is_answer_like")),
    }


def _quote_authority(
    page: int,
    quote: str,
    section_map: Dict[str, Any],
    rows: Dict[int, Dict[str, Any]],
    *,
    domain: Optional[str] = None,
) -> Tuple[Dict[str, Any], bool, Optional[str]]:
    try:
        authority = classify_quote_authority(page, quote, section_map, domain=domain)
        return (
            {
                "page": int(page),
                "section_zone": str(authority.get("section_zone") or ZONE_UNKNOWN),
                "authority_level": str(authority.get("authority_level") or AUTH_UNKNOWN),
                "authority_score": round(_safe_float(authority.get("authority_score"), 0.3), 4),
                "domain_hints": list(authority.get("domain_hints") or [])
                if isinstance(authority.get("domain_hints"), list)
                else [],
                "answer_point": authority.get("answer_point"),
                "is_instruction_like": bool(authority.get("is_instruction_like")),
                "is_answer_like": bool(authority.get("is_answer_like")),
            },
            False,
            None,
        )
    except Exception as exc:
        fallback = _fallback_authority(page, rows)
        return fallback, True, str(exc)[:160]


def _make_evidence(
    page: int,
    quote: str,
    authority: Dict[str, Any],
    *,
    signal: Optional[str] = None,
    status: Optional[str] = None,
    role: Optional[str] = None,
    amount_eur: Optional[float] = None,
    lot_number: Optional[int] = None,
) -> Dict[str, Any]:
    ev: Dict[str, Any] = {
        "page": int(page),
        "quote": re.sub(r"\s+", " ", str(quote or "")).strip()[:520],
        "section_zone": authority.get("section_zone") or ZONE_UNKNOWN,
        "authority_level": authority.get("authority_level") or AUTH_UNKNOWN,
        "authority_score": round(_safe_float(authority.get("authority_score"), 0.3), 4),
        "domain_hints": list(authority.get("domain_hints") or []),
        "answer_point": authority.get("answer_point"),
        "is_instruction_like": bool(authority.get("is_instruction_like")),
        "is_answer_like": bool(authority.get("is_answer_like")),
    }
    if signal:
        ev["signal"] = signal
    if status:
        ev["status"] = status
    if role:
        ev["role"] = role
    if amount_eur is not None:
        ev["amount_eur"] = amount_eur
    if lot_number is not None:
        ev["lot_number"] = lot_number
    return ev


def _is_high_factual(ev: Dict[str, Any], zones: Optional[set[str]] = None) -> bool:
    wanted_zones = zones or FACTUAL_ZONES
    return (
        str(ev.get("authority_level") or "") == AUTH_HIGH
        and str(ev.get("section_zone") or "") in wanted_zones
        and not bool(ev.get("is_instruction_like"))
    )


def _is_low_or_context(ev: Dict[str, Any]) -> bool:
    return (
        str(ev.get("authority_level") or "") in {AUTH_LOW, AUTH_UNKNOWN}
        or str(ev.get("section_zone") or "") in LOW_AUTHORITY_ZONES
        or bool(ev.get("is_instruction_like"))
    )


def _looks_like_toc_lot_reference(ev: Dict[str, Any]) -> bool:
    quote = str(ev.get("quote") or "")
    normalized = _normalize_text(quote)
    return bool(
        re.search(r"\.{3,}\s*\d+\s*$", quote)
        or re.search(r"\b(indice|sommario)\b", normalized)
        or re.search(r"\blotto\b.{0,80}\.{3,}\s*\d+\b", quote, flags=re.IGNORECASE | re.UNICODE)
    )


def _is_high_final_lot_evidence(ev: Dict[str, Any]) -> bool:
    if not _is_high_factual(ev, LOT_FINAL_ZONES) or _looks_like_toc_lot_reference(ev):
        return False
    zone = str(ev.get("section_zone") or "")
    if zone == ZONE_FINAL_LOT:
        return True
    normalized = _normalize_text(ev.get("quote"))
    if zone == ZONE_ANSWER and re.search(r"\b(formazione\s+lott[oi]|schema\s+riassuntivo|riepilogo\s+lott[oi]|lotto\s+unico)\b", normalized):
        return True
    if zone == ZONE_FINAL_VALUATION and re.search(
        r"\b(formazione\s+lott[oi]|schema\s+riassuntivo|riepilogo\s+lott[oi]|identificativo\s+lotto|prezzo\s+base\s+d[' ]asta\s+per\s+lotto)\b",
        normalized,
    ):
        return True
    return False


def _empty_lot_value() -> Dict[str, Any]:
    return {
        "shadow_lot_mode": "unknown",
        "detected_lot_numbers": [],
        "has_high_authority_lotto_unico": False,
        "has_high_authority_multilot": False,
        "final_lot_formation_pages": [],
        "schema_riassuntivo_pages": [],
        "riepilogo_bando_pages": [],
        "chapter_lot_numbers": [],
        "chapter_lot_start_pages": [],
        "chapter_lot_evidence": [],
        "chapter_topology_conflicts": [],
    }


def _summary_pages(section_map: Dict[str, Any], key: str, zone: str) -> List[int]:
    summary = section_map.get("summary") if isinstance(section_map.get("summary"), dict) else {}
    pages = summary.get(key)
    out: List[int] = []
    if isinstance(pages, list):
        for item in pages:
            try:
                page = int(item)
            except Exception:
                continue
            if page not in out:
                out.append(page)
    if out:
        return sorted(out)
    for row in section_map.get("pages") or []:
        if not isinstance(row, dict) or row.get("zone") != zone:
            continue
        try:
            page = int(row.get("page"))
        except Exception:
            continue
        if page not in out:
            out.append(page)
    return sorted(out)


def _pages_matching(pages: Sequence[Dict[str, Any]], pattern: str) -> List[int]:
    out: List[int] = []
    for page in pages:
        if re.search(pattern, page.get("text", ""), flags=re.IGNORECASE | re.UNICODE):
            out.append(int(page["page"]))
    return sorted(dict.fromkeys(out))


def _chapter_local_window(pages: Sequence[Dict[str, Any]], index: int, start: int, max_next_pages: int = 2) -> str:
    chunks: List[str] = []
    current = pages[index]
    chunks.append(str(current.get("text") or "")[start:])
    for next_page in pages[index + 1 : index + 1 + max_next_pages]:
        chunks.append(str(next_page.get("text") or ""))
    window = "\n".join(chunks)
    next_heading = CHAPTER_LOT_HEADING_RE.search(window, pos=8)
    if next_heading:
        window = window[: next_heading.start()]
    return window[:12000]


def _chapter_support_signals(window: str) -> List[str]:
    signals: List[str] = []
    if CHAPTER_IDENTIFICATION_RE.search(window):
        signals.append("identificazione_beni_oggetto_di_vendita")
    if CHAPTER_ASSET_QUOTA_RE.search(_normalize_text(window)):
        signals.append("asset_type_address_quota")
    for key, pattern in CHAPTER_SUPPORT_PATTERNS.items():
        if pattern.search(window):
            signals.append(key)
    return list(dict.fromkeys(signals))


def _chapter_snippet(window: str) -> str:
    return re.sub(r"\s+", " ", str(window or "")).strip()[:520]


def _is_toc_like_text(text: str) -> bool:
    normalized = _normalize_text(text)
    if re.search(r"\b(indice|sommario|elenco\s+lott[oi])\b", normalized[:600]):
        return True
    lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]
    dotted_refs = sum(1 for line in lines if re.search(r"\.{3,}\s*\d+\s*$", line))
    return dotted_refs >= 3


def _scan_chapter_lot_topology(
    pages: Sequence[Dict[str, Any]],
    section_map: Dict[str, Any],
    rows: Dict[int, Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[str]]:
    strong: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    failures: List[str] = []
    seen_numbers: set[int] = set()
    for idx, page in enumerate(pages):
        page_num = int(page["page"])
        text = str(page.get("text") or "")
        page_row = rows.get(page_num, {})
        page_zone = str(page_row.get("zone") or ZONE_UNKNOWN)
        if page_zone in {ZONE_TOC, ZONE_INSTRUCTION, ZONE_QUESTION} or _is_toc_like_text(text):
            continue
        for match in CHAPTER_LOT_HEADING_RE.finditer(text):
            try:
                lot_number = int(match.group(1))
            except Exception:
                continue
            window = _chapter_local_window(pages, idx, match.start())
            snippet = _chapter_snippet(window)
            if _looks_like_toc_lot_reference({"quote": snippet}):
                continue
            authority, failed, err = _quote_authority(page_num, snippet, section_map, rows, domain="lots")
            if failed and err:
                failures.append(f"page_{page_num}:{err}")
            ev = _make_evidence(
                page_num,
                snippet,
                authority,
                signal="chapter_lot_start",
                lot_number=lot_number,
            )
            ev["supporting_signals"] = _chapter_support_signals(window)
            ev["chapter_start_page"] = page_num
            if ev["supporting_signals"]:
                if lot_number in seen_numbers:
                    rejected.append(ev)
                    continue
                seen_numbers.add(lot_number)
                strong.append(ev)
            else:
                rejected.append(ev)
    return _dedupe_evidence(strong, limit=30), _dedupe_evidence(rejected, limit=30), failures


def resolve_lot_structure_shadow(pages: Sequence[Dict[str, Any]], section_map: Any) -> Dict[str, Any]:
    domain = "lot_structure"
    fail_reason = _section_map_fail_reason(section_map)
    value = _empty_lot_value()
    if fail_reason:
        return _domain_fail_open(domain, value, fail_reason)

    normalized_pages = _as_pages(pages)
    rows = _page_rows(section_map)
    notes: List[str] = []
    if _mostly_unknown_authority(section_map):
        notes.append("mostly_unknown_authority_map")

    value["final_lot_formation_pages"] = _summary_pages(section_map, "final_lot_formation_pages", ZONE_FINAL_LOT)
    value["schema_riassuntivo_pages"] = _pages_matching(normalized_pages, r"\bschema\s+riassuntivo\b")
    value["riepilogo_bando_pages"] = _pages_matching(normalized_pages, r"\briepilogo\s+(?:bando|avviso)\s+d[' ]?asta\b")

    single_evidence: List[Dict[str, Any]] = []
    number_evidence: List[Dict[str, Any]] = []
    partial_failures: List[str] = []
    for page in normalized_pages:
        text = page["text"]
        for match in LOT_UNICO_RE.finditer(text):
            quote = _line_snippet(text, match.start(), match.end())
            authority, failed, err = _quote_authority(page["page"], quote, section_map, rows, domain="lots")
            if failed and err:
                partial_failures.append(f"page_{page['page']}:{err}")
            single_evidence.append(_make_evidence(page["page"], quote, authority, signal="lotto_unico"))
        for match in LOT_NUMBER_RE.finditer(text):
            try:
                lot_number = int(match.group(1))
            except Exception:
                continue
            quote = _line_snippet(text, match.start(), match.end())
            authority, failed, err = _quote_authority(page["page"], quote, section_map, rows, domain="lots")
            if failed and err:
                partial_failures.append(f"page_{page['page']}:{err}")
            number_evidence.append(
                _make_evidence(
                    page["page"],
                    quote,
                    authority,
                    signal="lot_number",
                    lot_number=lot_number,
                )
            )

    chapter_evidence, weak_chapter_evidence, chapter_failures = _scan_chapter_lot_topology(normalized_pages, section_map, rows)
    partial_failures.extend(chapter_failures)
    high_single = [ev for ev in single_evidence if _is_high_final_lot_evidence(ev)]
    high_numbers = [ev for ev in number_evidence if _is_high_final_lot_evidence(ev)]
    high_lot_numbers = sorted({int(ev["lot_number"]) for ev in high_numbers if ev.get("lot_number")})
    chapter_lot_numbers = sorted({int(ev["lot_number"]) for ev in chapter_evidence if ev.get("lot_number")})
    all_lot_numbers = sorted({int(ev["lot_number"]) for ev in number_evidence if ev.get("lot_number")})

    value["chapter_lot_numbers"] = chapter_lot_numbers
    value["chapter_lot_start_pages"] = sorted(
        {
            int(ev["chapter_start_page"])
            for ev in chapter_evidence
            if ev.get("chapter_start_page")
        }
    )
    value["chapter_lot_evidence"] = _dedupe_evidence(chapter_evidence, limit=20)
    if len(high_lot_numbers) >= 2:
        value["detected_lot_numbers"] = high_lot_numbers
    elif len(chapter_lot_numbers) >= 2:
        value["detected_lot_numbers"] = chapter_lot_numbers
    else:
        value["detected_lot_numbers"] = high_lot_numbers or chapter_lot_numbers or all_lot_numbers
    value["has_high_authority_lotto_unico"] = bool(high_single)
    value["has_high_authority_multilot"] = len(high_lot_numbers) >= 2 or len(chapter_lot_numbers) >= 2

    rejected: List[Dict[str, Any]] = []
    rules: List[str] = []
    fail_open = bool(partial_failures)
    if partial_failures:
        notes.append("partial_authority_classification_failure")

    if len(high_lot_numbers) >= 2:
        value["shadow_lot_mode"] = "multi_lot"
        winning = [ev for ev in high_numbers if ev.get("lot_number") in high_lot_numbers]
        rejected = high_single + weak_chapter_evidence + [ev for ev in single_evidence if _is_low_or_context(ev) or _looks_like_toc_lot_reference(ev)]
        rules = ["high_authority_multilot_beats_toc_context_and_generic_lot_mentions"]
        status = STATUS_WARN if fail_open else STATUS_OK
        confidence = 0.92
    elif len(chapter_lot_numbers) >= 2 and high_single:
        value["shadow_lot_mode"] = "unknown"
        value["chapter_topology_conflicts"] = [
            {
                "type": "chapter_multilot_conflicts_with_high_lotto_unico",
                "chapter_lot_numbers": chapter_lot_numbers,
                "chapter_lot_start_pages": value["chapter_lot_start_pages"],
                "lotto_unico_pages": sorted({int(ev.get("page")) for ev in high_single if ev.get("page")}),
            }
        ]
        winning = chapter_evidence + high_single
        rejected = weak_chapter_evidence
        rules = ["chapter_based_multi_lot_topology_conflicts_with_high_lotto_unico"]
        notes.append("chapter_based_multilot_conflicts_with_high_lotto_unico")
        status = STATUS_WARN
        confidence = 0.45
    elif len(chapter_lot_numbers) >= 2:
        value["shadow_lot_mode"] = "multi_lot"
        winning = chapter_evidence
        rejected = high_single + weak_chapter_evidence + [ev for ev in number_evidence if _is_low_or_context(ev) or _looks_like_toc_lot_reference(ev)]
        rules = ["chapter_based_multi_lot_topology"]
        status = STATUS_WARN if fail_open else STATUS_OK
        confidence = 0.9 if len(chapter_lot_numbers) >= 3 else 0.88
    elif high_single:
        value["shadow_lot_mode"] = "single_lot"
        winning = high_single
        rejected = weak_chapter_evidence + [ev for ev in number_evidence if _is_low_or_context(ev) or not _is_high_final_lot_evidence(ev)]
        rules = ["high_authority_lotto_unico_beats_toc_context_and_generic_lot_mentions"]
        status = STATUS_WARN if fail_open else STATUS_OK
        confidence = 0.9
    else:
        winning = []
        rejected = single_evidence + number_evidence + weak_chapter_evidence
        if single_evidence or number_evidence or weak_chapter_evidence:
            notes.append("only_low_or_nonfinal_lot_evidence")
        else:
            notes.append("no_lot_structure_evidence")
        if "mostly_unknown_authority_map" in notes:
            fail_open = True
        status = STATUS_FAIL_OPEN if fail_open else STATUS_INSUFFICIENT
        confidence = 0.25 if (single_evidence or number_evidence) else 0.0
        rules = ["insufficient_high_authority_lot_evidence"]

    return _result(
        domain,
        status=status,
        value=value,
        confidence=confidence,
        winning_evidence=winning,
        rejected_conflicts=rejected,
        rules=rules,
        fail_open=fail_open,
        notes=notes,
    )


def _empty_occupancy_value() -> Dict[str, Any]:
    return {"shadow_occupancy_status": "UNKNOWN"}


def _occupancy_status_from_text(text: str) -> Optional[str]:
    normalized = _normalize_text(text)
    if re.search(r"\b(non\s+verificabile|non\s+accertabile|da\s+verificare|non\s+e\s+stato\s+possibile)\b", normalized):
        return "NON_VERIFICABILE"
    if re.search(r"\b(occupat[oaie]\s+da\s+terzi|terzi\s+senza\s+titolo|senza\s+titolo)\b", normalized):
        return "OCCUPATO_DA_TERZI"
    if re.search(r"\boccupat[oaie]\s+(?:dal|dalla|dai|dagli|dalle)\s+(?:debitore|debitori|esecutat[oaie])\b", normalized):
        return "OCCUPATO_DA_DEBITORE"
    if re.search(r"\boccupat[oaie]\b", normalized):
        return "OCCUPATO"
    if re.search(r"\bnon\s+occupat[oaie]\b", normalized):
        return "LIBERO"
    if re.search(r"\bliber[oaie]\b", normalized):
        if re.search(r"\bliber[oaie]\s+da\s+(?:iscrizioni|trascrizioni|ipoteche|pignoramenti|vincoli|gravami)\b", normalized):
            return None
        if re.search(r"\b(stato\s+di\s+(?:occupazione|possesso)|occupazione|possesso|immobile|bene|persone|cose)\b", normalized):
            return "LIBERO"
    return None


def _scan_occupancy_evidence(
    pages: Sequence[Dict[str, Any]],
    section_map: Dict[str, Any],
    rows: Dict[int, Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[str]]:
    out: List[Dict[str, Any]] = []
    failures: List[str] = []
    pattern = re.compile(
        r"stato\s+di\s+(?:occupazione|possesso)|occupazion[ei]|occupat\w*|liber\w*|debit\w*|terzi|senza\s+titolo",
        flags=re.IGNORECASE | re.UNICODE,
    )
    for page in pages:
        text = page["text"]
        for match in pattern.finditer(text):
            quote = _line_snippet(text, match.start(), match.end(), radius=220)
            status = _occupancy_status_from_text(quote)
            if not status:
                continue
            authority, failed, err = _quote_authority(page["page"], quote, section_map, rows, domain="occupancy")
            if failed and err:
                failures.append(f"page_{page['page']}:{err}")
            out.append(_make_evidence(page["page"], quote, authority, signal="occupancy", status=status))
    return _dedupe_evidence(out, limit=80), failures


def _evidence_rank(ev: Dict[str, Any], *, exact_heading_bonus: bool = False) -> Tuple[float, int, int]:
    zone = str(ev.get("section_zone") or "")
    level = str(ev.get("authority_level") or "")
    score = _safe_float(ev.get("authority_score"), 0.3)
    if level == AUTH_HIGH:
        score += 0.45
    elif level == AUTH_MEDIUM:
        score += 0.18
    if zone in {ZONE_FINAL_LOT, ZONE_FINAL_VALUATION, ZONE_FORMALITIES}:
        score += 0.18
    elif zone == ZONE_ANSWER:
        score += 0.14
    elif zone in LOW_AUTHORITY_ZONES:
        score -= 0.25
    if bool(ev.get("is_instruction_like")) and not bool(ev.get("is_answer_like")):
        score -= 0.5
    if exact_heading_bonus and re.search(r"\bstato\s+di\s+(?:occupazione|possesso)\b", _normalize_text(ev.get("quote"))):
        score += 0.12
    page = int(ev.get("page") or 0)
    specificity = {
        "OCCUPATO_DA_TERZI": 5,
        "OCCUPATO_DA_DEBITORE": 4,
        "OCCUPATO": 3,
        "LIBERO": 3,
        "NON_OPPONIBILE": 3,
        "OPPONIBILE": 3,
        "NON_VERIFICABILE": 1,
        "UNKNOWN": 0,
    }.get(str(ev.get("status") or ""), 0)
    return (score, specificity, page)


def resolve_occupancy_shadow(pages: Sequence[Dict[str, Any]], section_map: Any) -> Dict[str, Any]:
    domain = "occupancy"
    fail_reason = _section_map_fail_reason(section_map)
    value = _empty_occupancy_value()
    if fail_reason:
        return _domain_fail_open(domain, value, fail_reason)

    normalized_pages = _as_pages(pages)
    rows = _page_rows(section_map)
    notes: List[str] = []
    if _mostly_unknown_authority(section_map):
        notes.append("mostly_unknown_authority_map")

    evidence, failures = _scan_occupancy_evidence(normalized_pages, section_map, rows)
    if failures:
        notes.append("partial_authority_classification_failure")
    factual = [
        ev
        for ev in evidence
        if not (bool(ev.get("is_instruction_like")) and not bool(ev.get("is_answer_like")))
        and str(ev.get("section_zone") or "") not in {ZONE_TOC, ZONE_INSTRUCTION, ZONE_QUESTION}
        and str(ev.get("authority_level") or "") in {AUTH_HIGH, AUTH_MEDIUM}
    ]
    high = [ev for ev in factual if _is_high_factual(ev)]
    winner_pool = high or factual

    if winner_pool:
        winner = sorted(winner_pool, key=lambda ev: _evidence_rank(ev, exact_heading_bonus=True), reverse=True)[0]
        chosen_status = str(winner.get("status") or "UNKNOWN")
        value["shadow_occupancy_status"] = chosen_status
        rejected = [ev for ev in evidence if ev is not winner and ev.get("status") != chosen_status]
        confidence = 0.86 if _is_high_factual(winner) else 0.58
        status = STATUS_WARN if failures else STATUS_OK
        return _result(
            domain,
            status=status,
            value=value,
            confidence=confidence,
            winning_evidence=[winner],
            rejected_conflicts=rejected,
            rules=["high_authority_occupancy_status_wins"] if _is_high_factual(winner) else ["medium_factual_occupancy_used_as_shadow_only"],
            fail_open=bool(failures),
            notes=notes,
        )

    if evidence:
        value["shadow_occupancy_status"] = "NON_VERIFICABILE"
        notes.append("only_weak_or_instruction_occupancy_evidence")
        fail_open = bool(failures) or "mostly_unknown_authority_map" in notes
        return _result(
            domain,
            status=STATUS_FAIL_OPEN if fail_open else STATUS_INSUFFICIENT,
            value=value,
            confidence=0.25,
            rejected_conflicts=evidence,
            rules=["weak_occupancy_evidence_cannot_create_factual_status"],
            fail_open=fail_open,
            notes=notes,
        )

    fail_open = "mostly_unknown_authority_map" in notes
    return _result(
        domain,
        status=STATUS_FAIL_OPEN if fail_open else STATUS_INSUFFICIENT,
        value=value,
        confidence=0.0,
        rules=["no_occupancy_evidence"],
        fail_open=fail_open,
        notes=notes + ["no_occupancy_evidence"],
    )


def _empty_opponibilita_value() -> Dict[str, Any]:
    return {
        "shadow_opponibilita_status": "UNKNOWN",
        "lease_title_evidence": [],
        "instruction_only_mentions": [],
    }


def _opponibilita_status_from_text(text: str) -> Optional[str]:
    normalized = _normalize_text(text)
    if re.search(r"\b(non\s+opponibil|inopponibil|senza\s+titolo|posteriore\s+al\s+pignoramento|non\s+registrat[oa])\b", normalized):
        return "NON_OPPONIBILE"
    if re.search(r"\bopponibil", normalized) and not re.search(r"\b(non\s+opponibil|inopponibil)\b", normalized):
        return "OPPONIBILE"
    if re.search(r"\b(anterior[ei]\s+al\s+pignoramento|registrat[oa]\s+in\s+data\s+anteriore)\b", normalized):
        return "OPPONIBILE"
    if re.search(r"\b(non\s+risultan[oa]\s+(?:contratti|locazioni)|nessun\s+contratto|da\s+verificare|non\s+verificabile)\b", normalized):
        return "NON_VERIFICABILE"
    return None


def resolve_opponibilita_shadow(pages: Sequence[Dict[str, Any]], section_map: Any) -> Dict[str, Any]:
    domain = "opponibilita"
    fail_reason = _section_map_fail_reason(section_map)
    value = _empty_opponibilita_value()
    if fail_reason:
        return _domain_fail_open(domain, value, fail_reason)

    normalized_pages = _as_pages(pages)
    rows = _page_rows(section_map)
    notes: List[str] = []
    if _mostly_unknown_authority(section_map):
        notes.append("mostly_unknown_authority_map")

    pattern = re.compile(
        r"opponibil\w*|locazion\w*|contratto\s+di\s+locazione|comodato|assegnazione\s+casa|senza\s+titolo|titolo\s+occupativo",
        flags=re.IGNORECASE | re.UNICODE,
    )
    factual: List[Dict[str, Any]] = []
    instruction_only: List[Dict[str, Any]] = []
    failures: List[str] = []
    for page in normalized_pages:
        text = page["text"]
        for match in pattern.finditer(text):
            quote = _line_snippet(text, match.start(), match.end(), radius=240)
            status = _opponibilita_status_from_text(quote)
            authority, failed, err = _quote_authority(page["page"], quote, section_map, rows, domain="opponibilita")
            if failed and err:
                failures.append(f"page_{page['page']}:{err}")
            ev = _make_evidence(page["page"], quote, authority, signal="opponibilita", status=status or "UNKNOWN")
            if bool(ev.get("is_instruction_like")) and not bool(ev.get("is_answer_like")):
                instruction_only.append(ev)
                continue
            normalized_quote = _normalize_text(quote)
            has_lease_basis = bool(
                re.search(
                    r"\b(locazion\w*|contratto|comodato|assegnazione|titolo\s+occupativo|senza\s+titolo|occupazion\w*)\b",
                    normalized_quote,
                )
            )
            if has_lease_basis and status and str(ev.get("section_zone") or "") not in {ZONE_TOC, ZONE_INSTRUCTION, ZONE_QUESTION}:
                factual.append(ev)
            elif status == "NON_VERIFICABILE" and str(ev.get("authority_level") or "") in {AUTH_HIGH, AUTH_MEDIUM}:
                factual.append(ev)

    value["instruction_only_mentions"] = _dedupe_evidence(instruction_only, limit=12)
    if failures:
        notes.append("partial_authority_classification_failure")

    if factual:
        winner = sorted(factual, key=lambda ev: _evidence_rank(ev), reverse=True)[0]
        status_value = str(winner.get("status") or "UNKNOWN")
        value["shadow_opponibilita_status"] = status_value
        value["lease_title_evidence"] = _dedupe_evidence(factual, limit=8)
        confidence = 0.82 if _is_high_factual(winner) else 0.55
        rejected = [ev for ev in factual + instruction_only if ev is not winner and ev.get("status") != status_value]
        return _result(
            domain,
            status=STATUS_WARN if failures else STATUS_OK,
            value=value,
            confidence=confidence,
            winning_evidence=[winner],
            rejected_conflicts=rejected,
            rules=["only_factual_lease_or_title_discussion_can_support_opponibilita"],
            fail_open=bool(failures),
            notes=notes,
        )

    if instruction_only:
        value["shadow_opponibilita_status"] = "NON_VERIFICABILE"
        notes.append("instruction_only_mentions_ignored")
        fail_open = bool(failures) or "mostly_unknown_authority_map" in notes
        return _result(
            domain,
            status=STATUS_FAIL_OPEN if fail_open else STATUS_INSUFFICIENT,
            value=value,
            confidence=0.2,
            rejected_conflicts=instruction_only,
            rules=["instruction_text_cannot_create_opponibilita_risk"],
            fail_open=fail_open,
            notes=notes,
        )

    fail_open = "mostly_unknown_authority_map" in notes
    value["shadow_opponibilita_status"] = "NON_VERIFICABILE" if not fail_open else "UNKNOWN"
    return _result(
        domain,
        status=STATUS_FAIL_OPEN if fail_open else STATUS_INSUFFICIENT,
        value=value,
        confidence=0.0,
        rules=["no_factual_lease_or_title_basis"],
        fail_open=fail_open,
        notes=notes + ["no_factual_lease_or_title_basis"],
    )


def _empty_legal_value() -> Dict[str, Any]:
    return {
        "formalities_to_cancel": [],
        "surviving_formalities": [],
        "legal_killer_candidates": [],
        "instruction_only_legal_mentions": [],
        "generic_legal_mentions": [],
    }


def _is_cancellable_formality(text: str) -> bool:
    normalized = _normalize_text(text)
    return bool(
        re.search(
            r"\b(cancellabil\w*|da\s+cancellare|cancellazione|saranno\s+cancellat\w*|ordine\s+di\s+cancellazione|purgazione)\b",
            normalized,
        )
    )


def _is_surviving_formality(text: str) -> bool:
    normalized = _normalize_text(text)
    return bool(
        re.search(
            r"\b(non\s+cancellabil\w*|non\s+soggett\w*\s+a\s+cancellazione|resta\w*\s+a\s+carico\s+(?:dell'?aggiudicatario|dell'?acquirente)|"
            r"permane\w*|continua\w*\s+a\s+gravare|opponibil\w*\s+(?:all'?aggiudicatario|all'?acquirente)|servitu\s+non\s+cancellabil\w*)\b",
            normalized,
        )
    )


def resolve_legal_formalities_shadow(pages: Sequence[Dict[str, Any]], section_map: Any) -> Dict[str, Any]:
    domain = "legal_formalities"
    fail_reason = _section_map_fail_reason(section_map)
    value = _empty_legal_value()
    if fail_reason:
        return _domain_fail_open(domain, value, fail_reason)

    normalized_pages = _as_pages(pages)
    rows = _page_rows(section_map)
    notes: List[str] = []
    if _mostly_unknown_authority(section_map):
        notes.append("mostly_unknown_authority_map")

    pattern = re.compile(
        r"formalita|ipotec\w*|pignorament\w*|trascrizion\w*|iscrizion\w*|conservatoria|domand[ae]\s+giudizial\w*|servitu|vincol\w*",
        flags=re.IGNORECASE | re.UNICODE,
    )
    winning: List[Dict[str, Any]] = []
    failures: List[str] = []
    for page in normalized_pages:
        text = page["text"]
        for match in pattern.finditer(text):
            quote = _line_snippet(text, match.start(), match.end(), radius=260)
            authority, failed, err = _quote_authority(page["page"], quote, section_map, rows, domain="legal_formalities")
            if failed and err:
                failures.append(f"page_{page['page']}:{err}")
            ev = _make_evidence(page["page"], quote, authority, signal="legal_formality")
            high_formalities = _is_high_factual(ev, {ZONE_FORMALITIES, ZONE_ANSWER})
            if bool(ev.get("is_instruction_like")) and not bool(ev.get("is_answer_like")):
                value["instruction_only_legal_mentions"].append(ev)
            elif high_formalities and _is_surviving_formality(quote):
                value["surviving_formalities"].append(ev)
                value["legal_killer_candidates"].append(ev)
                winning.append(ev)
            elif high_formalities and _is_cancellable_formality(quote):
                value["formalities_to_cancel"].append(ev)
                winning.append(ev)
            elif high_formalities and re.search(r"\b(ipotec\w*|pignorament\w*|trascrizion\w*|iscrizion\w*)\b", _normalize_text(quote)):
                # Factual formalities are tracked, but not blockers without survival/buyer-action language.
                value["formalities_to_cancel"].append(ev)
                winning.append(ev)
            else:
                value["generic_legal_mentions"].append(ev)

    for key in list(value.keys()):
        if isinstance(value[key], list):
            value[key] = _dedupe_evidence(value[key], limit=14)
    if failures:
        notes.append("partial_authority_classification_failure")
    if value["formalities_to_cancel"]:
        notes.append("cancellable_formalities_separated_from_buyer_costs")
    if value["generic_legal_mentions"]:
        notes.append("generic_legal_mentions_not_promoted_to_killers")

    if value["legal_killer_candidates"] or value["formalities_to_cancel"] or value["surviving_formalities"]:
        confidence = 0.84 if winning else 0.5
        return _result(
            domain,
            status=STATUS_WARN if failures else STATUS_OK,
            value=value,
            confidence=confidence,
            winning_evidence=winning,
            rejected_conflicts=value["instruction_only_legal_mentions"] + value["generic_legal_mentions"],
            rules=[
                "formalities_table_is_valid_authority",
                "cancellable_formalities_are_not_buyer_blockers_without_survival_language",
            ],
            fail_open=bool(failures),
            notes=notes,
        )

    fail_open = bool(failures) or "mostly_unknown_authority_map" in notes
    return _result(
        domain,
        status=STATUS_FAIL_OPEN if fail_open else STATUS_INSUFFICIENT,
        value=value,
        confidence=0.2 if value["generic_legal_mentions"] else 0.0,
        rejected_conflicts=value["instruction_only_legal_mentions"] + value["generic_legal_mentions"],
        rules=["generic_or_instruction_legal_mentions_cannot_create_killers"],
        fail_open=fail_open,
        notes=notes or ["no_factual_formalities_basis"],
    )


def _empty_money_value() -> Dict[str, Any]:
    roles = {role: [] for role in MONEY_ROLES}
    roles["money_candidates"] = []
    roles["money_role_counts"] = {key: 0 for key in roles.keys()}
    roles["counting_policy"] = "components_are_not_summed_when_total_candidate_is_present"
    roles["summary"] = {
        "buyer_cost_signal_count": 0,
        "valuation_amount_count": 0,
        "formalities_amount_count": 0,
        "rendita_count": 0,
        "unknown_count": 0,
        "authority_customer_safe_cost_count": 0,
        "double_count_risk": False,
    }
    return roles


def _parse_amount(raw: Any) -> Optional[float]:
    text = str(raw or "")
    match = re.search(r"\d{1,3}(?:\.\d{3})*(?:,\d{2})?|\d+(?:,\d{2})?", text)
    if not match:
        return None
    normalized = match.group(0).replace(".", "").replace(",", ".")
    try:
        return float(normalized)
    except Exception:
        return None


def _money_role_reason_from_text(text: str) -> Tuple[str, str]:
    normalized = _normalize_text(text)
    if re.search(r"\b(verifich\w*|accert\w*|provved\w*|indichi|riferisca|determini)\b.{0,120}\b(spese|costi|oneri|importi|euro)\b", normalized):
        return "unknown_money", "INSTRUCTION_OR_BOILERPLATE_AMOUNT"
    if re.search(r"\brendita\s+catastale\b|\brendita\b.{0,80}\b(catasto|catastale|categoria|classe|vani|foglio|particella|subalterno)\b", normalized):
        return "cadastral_rendita", "RENDITA_CATASTALE_AMOUNT"
    if re.search(r"\b(prezzo\s+base|base\s+d[' ]asta|offerta\s+minima)\b", normalized):
        return "base_auction", "PREZZO_BASE_ASTA_AMOUNT"
    if re.search(r"\bvalore\s+finale\s+(?:di\s+)?stima\b|\bvalore\s+finale\b", normalized):
        return "final_value", "VALORE_FINALE_STIMA_AMOUNT"
    if re.search(r"\bvalore\s+di\s+(?:stima|mercato)|valore\s+venale|valore\s+cauzionale|valore\s+commerciale\b", normalized):
        return "market_value", "VALORE_STIMA_MERCATO_AMOUNT"
    if re.search(r"\b(deprezzament\w*|decurtazion\w*|abbattimento|riduzione|adeguament\w*\s+e\s+correzion\w*)\b", normalized):
        return "valuation_deduction", "VALUATION_DEDUCTION_AMOUNT"
    if re.search(r"\b(ipotec\w*|pignorament\w*|formalita|trascrizion\w*|iscrizion\w*|registro\s+(?:generale|particolare)|procedura)\b", normalized):
        return "formalities_procedural_amount", "FORMALITA_PROCEDURAL_AMOUNT"
    if re.search(r"\b(spese\s+condominiali|condominio|condominial\w*)\b.{0,120}\b(arretrat\w*|insolut\w*|scadut\w*|morosit\w*|debito|debitoria)\b", normalized):
        return "condominium_arrears", "CONDOMINIUM_ARREARS_AMOUNT"
    if re.search(
        r"\b(spese\s+tecniche|regolarizzazion\w*|sanatori\w*|oblazion\w*|sanzion\w*|docfa|tipo\s+mappale|"
        r"ripristin\w*|demolizion\w*|fiscalizzazion\w*|oneri?\s+(?:a\s+carico|di\s+regolarizzazione)|"
        r"costo\s+(?:di\s+)?(?:sanatoria|regolarizzazione|ripristino|demolizione|fiscalizzazione))\b",
        normalized,
    ):
        return "buyer_cost_signal_to_verify", "EXPLICIT_BUYER_COST_SIGNAL"
    if re.search(r"\b(prezzo|valore|stima)\b", normalized):
        return "price", "GENERIC_PRICE_OR_VALUE_AMOUNT"
    if re.search(r"\b(legge|norma|articolo|procedura|tribunale|decreto|asta|bando)\b", normalized):
        return "unknown_money", "GENERIC_MONEY_BOILERPLATE"
    return "unknown_money", "UNKNOWN_MONEY_AMOUNT"


def _is_total_context(text: str, amount_raw: Any = None) -> bool:
    normalized = _normalize_text(text)
    raw_amount = _normalize_text(amount_raw)
    if raw_amount:
        idx = normalized.find(raw_amount)
        if idx >= 0:
            window_before = normalized[max(0, idx - 90) : idx]
            return bool(re.search(r"\b(totale|complessiv[oa]|sommano|somma|importo\s+totale)\b", window_before))
    return bool(re.search(r"\b(totale|complessiv[oa]|sommano|somma|importo\s+totale)\b", normalized))


def _candidate_id(page: int, amount: Any, quote: str, index: int) -> str:
    seed = f"{page}|{amount}|{quote}|{index}".encode("utf-8", errors="ignore")
    return "money_" + hashlib.sha1(seed).hexdigest()[:12]


def _money_authority_supported(authority: Dict[str, Any]) -> bool:
    return (
        str(authority.get("authority_level") or "") in {AUTH_HIGH, AUTH_MEDIUM}
        and str(authority.get("section_zone") or "") not in LOW_AUTHORITY_ZONES
        and not bool(authority.get("is_instruction_like"))
    )


def _money_confidence(authority: Dict[str, Any], role: str, supported: bool) -> float:
    score = _safe_float(authority.get("authority_score"), 0.3)
    if role == "unknown_money":
        return min(score, 0.35)
    if supported:
        return max(score, 0.75)
    return min(score, 0.55)


def _money_candidate_record(
    item: Dict[str, Any],
    authority: Dict[str, Any],
    *,
    role: str,
    reason_code: str,
    index: int,
    base_role: Optional[str] = None,
    base_reason_code: Optional[str] = None,
) -> Dict[str, Any]:
    page = int(item["page"])
    quote = re.sub(r"\s+", " ", str(item.get("quote") or "")).strip()[:800]
    amount = item.get("amount_eur")
    supported = _money_authority_supported(authority)
    buyer_signal = role in {"buyer_cost_signal_to_verify", "condominium_arrears"} or (
        role == "total_candidate" and (base_role or "") in {"buyer_cost_signal_to_verify", "condominium_arrears"}
    )
    explicit_buyer_obligation = bool(
        re.search(
            r"\b(a\s+carico\s+(?:dell[' ]?)?(?:aggiudicatario|acquirente|parte\s+acquirente)|"
            r"aggiudicatario|acquirente|da\s+sostenere|restano?\s+a\s+carico)\b",
            _normalize_text(quote),
        )
    )
    safe_cost = bool(buyer_signal and supported and explicit_buyer_obligation)
    warnings: List[str] = []
    if str(authority.get("authority_level") or "") in {AUTH_LOW, AUTH_UNKNOWN} or bool(authority.get("is_instruction_like")):
        warnings.append("WEAK_OR_INSTRUCTION_AUTHORITY")
    if role == "unknown_money" and reason_code in {"GENERIC_MONEY_BOILERPLATE", "INSTRUCTION_OR_BOILERPLATE_AMOUNT"}:
        warnings.append(reason_code)
    return {
        "candidate_id": _candidate_id(page, amount, quote, index),
        "amount_eur": amount,
        "raw_text": quote,
        "page": page,
        "role": role,
        "confidence": round(_money_confidence(authority, role, supported), 4),
        "authority_zone": authority.get("section_zone") or ZONE_UNKNOWN,
        "authority_level": authority.get("authority_level") or AUTH_UNKNOWN,
        "reason_code": reason_code,
        "is_customer_safe_cost": safe_cost,
        "should_surface_in_money_box": bool(buyer_signal and supported),
        "should_sum": bool(role == "total_candidate" and safe_cost),
        "parent_total_candidate_id": None,
        "warnings": warnings,
        "source": item.get("source") or "",
        "semantic_base_role": base_role or role,
        "semantic_base_reason_code": base_reason_code or reason_code,
    }


def _candidate_money_items(
    pages: Sequence[Dict[str, Any]],
    candidates: Optional[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[str]]:
    out: List[Dict[str, Any]] = []
    warnings: List[str] = []
    money_candidates = candidates.get("money") if isinstance(candidates, dict) else None
    if isinstance(money_candidates, list):
        for row in money_candidates:
            if not isinstance(row, dict):
                warnings.append("malformed_money_candidate_row")
                continue
            try:
                page = int(row.get("page"))
            except Exception:
                warnings.append("malformed_money_candidate_page")
                continue
            quote = str(row.get("context") or row.get("quote") or "").strip()
            if not quote:
                warnings.append("malformed_money_candidate_quote")
                continue
            amount = row.get("amount_eur")
            if amount is None:
                amount = _parse_amount(row.get("amount_raw") or quote)
            try:
                amount_f = float(amount)
            except Exception:
                amount_f = None
            out.append(
                {
                    "page": page,
                    "quote": quote,
                    "amount_eur": amount_f,
                    "amount_raw": row.get("amount_raw"),
                    "source": "candidate_miner",
                }
            )
    elif money_candidates is not None:
        warnings.append("malformed_candidate_money_file")

    for page in pages:
        text = page["text"]
        for match in MONEY_AMOUNT_RE.finditer(text):
            quote = _line_snippet(text, match.start(), match.end(), radius=220)
            amount = _parse_amount(match.group(0))
            if amount is None:
                continue
            out.append(
                {
                    "page": page["page"],
                    "quote": quote,
                    "amount_eur": amount,
                    "amount_raw": match.group(0),
                    "source": "page_scan",
                }
            )

    deduped: List[Dict[str, Any]] = []
    seen = set()
    for item in out:
        sig = (item.get("page"), round(float(item.get("amount_eur") or 0.0), 2), _normalize_text(item.get("quote"))[:120])
        if sig in seen:
            continue
        seen.add(sig)
        deduped.append(item)
    return deduped, list(dict.fromkeys(warnings))


def resolve_money_roles_shadow(
    pages: Sequence[Dict[str, Any]],
    section_map: Any,
    candidates: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    domain = "money_roles"
    fail_reason = _section_map_fail_reason(section_map)
    value = _empty_money_value()
    if fail_reason:
        return _domain_fail_open(domain, value, fail_reason)

    normalized_pages = _as_pages(pages)
    rows = _page_rows(section_map)
    notes: List[str] = []
    if _mostly_unknown_authority(section_map):
        notes.append("mostly_unknown_authority_map")

    items, candidate_warnings = _candidate_money_items(normalized_pages, candidates)
    notes.extend(candidate_warnings)
    failures: List[str] = []
    money_candidates: List[Dict[str, Any]] = []
    for idx, item in enumerate(items, start=1):
        page = int(item["page"])
        quote = str(item["quote"])
        authority, failed, err = _quote_authority(page, quote, section_map, rows, domain="money")
        if failed and err:
            failures.append(f"page_{page}:{err}")
        base_role, reason_code = _money_role_reason_from_text(quote)
        if _is_total_context(quote, item.get("amount_raw")):
            total_reason = "EXPLICIT_TOTAL_CANDIDATE"
            if base_role in {"buyer_cost_signal_to_verify", "condominium_arrears"}:
                total_reason = "EXPLICIT_TOTAL_BUYER_COST_SIGNAL"
            candidate = _money_candidate_record(
                item,
                authority,
                role="total_candidate",
                reason_code=total_reason,
                index=idx,
                base_role=base_role,
                base_reason_code=reason_code,
            )
        else:
            candidate = _money_candidate_record(item, authority, role=base_role, reason_code=reason_code, index=idx)
        money_candidates.append(candidate)

    totals_by_page: Dict[int, List[Dict[str, Any]]] = {}
    for candidate in money_candidates:
        if candidate.get("role") != "total_candidate":
            continue
        try:
            totals_by_page.setdefault(int(candidate["page"]), []).append(candidate)
        except Exception:
            continue

    for candidate in money_candidates:
        if candidate.get("role") == "total_candidate":
            continue
        child_base_role = str(candidate.get("semantic_base_role") or candidate.get("role") or "")
        try:
            page = int(candidate.get("page"))
            amount = float(candidate.get("amount_eur") or 0.0)
        except Exception:
            continue
        parent = None
        for total in sorted(totals_by_page.get(page) or [], key=lambda row: float(row.get("amount_eur") or 0.0)):
            parent_base_role = str(total.get("semantic_base_role") or "")
            compatible_component = (
                child_base_role == parent_base_role
                or {child_base_role, parent_base_role}.issubset({"buyer_cost_signal_to_verify", "condominium_arrears"})
                or {child_base_role, parent_base_role}.issubset({"formalities_procedural_amount"})
                or {child_base_role, parent_base_role}.issubset({"valuation_deduction", "market_value", "final_value"})
            )
            if not compatible_component:
                continue
            try:
                total_amount = float(total.get("amount_eur") or 0.0)
            except Exception:
                continue
            if amount and total_amount and amount < total_amount:
                parent = total
                break
        if parent is None:
            continue
        candidate["semantic_base_role"] = candidate.get("role")
        candidate["semantic_base_reason_code"] = candidate.get("reason_code")
        candidate["role"] = "component_of_total"
        candidate["reason_code"] = "COMPONENT_OF_EXPLICIT_TOTAL"
        candidate["is_customer_safe_cost"] = False
        candidate["should_surface_in_money_box"] = False
        candidate["should_sum"] = False
        candidate["parent_total_candidate_id"] = parent.get("candidate_id")
        candidate.setdefault("warnings", []).append("COMPONENT_TOTAL_DOUBLE_COUNT_RISK")

    value["money_candidates"] = money_candidates[:120]
    for candidate in money_candidates:
        role = str(candidate.get("role") or "unknown_money")
        if role not in MONEY_ROLES:
            role = "unknown_money"
        value[role].append(
            {
                "page": candidate.get("page"),
                "quote": candidate.get("raw_text"),
                "signal": "money",
                "role": role,
                "amount_eur": candidate.get("amount_eur"),
                "section_zone": candidate.get("authority_zone"),
                "authority_level": candidate.get("authority_level"),
                "authority_score": candidate.get("confidence"),
                "reason_code": candidate.get("reason_code"),
            }
        )

    for key in MONEY_ROLES:
        value[key] = _dedupe_evidence(value.get(key) or [], limit=20)
    value["money_role_counts"] = {
        key: len(items)
        for key, items in value.items()
        if isinstance(items, list)
        and key != "money_candidates"
    }
    valuation_roles = {"valuation_deduction", "price", "base_auction", "final_value", "market_value"}
    value["summary"] = {
        "buyer_cost_signal_count": int(value["money_role_counts"].get("buyer_cost_signal_to_verify") or 0)
        + int(value["money_role_counts"].get("condominium_arrears") or 0),
        "valuation_amount_count": sum(int(value["money_role_counts"].get(role) or 0) for role in valuation_roles),
        "formalities_amount_count": int(value["money_role_counts"].get("formalities_procedural_amount") or 0),
        "rendita_count": int(value["money_role_counts"].get("cadastral_rendita") or 0),
        "unknown_count": int(value["money_role_counts"].get("unknown_money") or 0),
        "authority_customer_safe_cost_count": sum(1 for candidate in money_candidates if candidate.get("is_customer_safe_cost")),
        "double_count_risk": any(candidate.get("parent_total_candidate_id") for candidate in money_candidates),
        "candidate_count": len(money_candidates),
    }
    classified = [
        {
            "page": candidate.get("page"),
            "quote": candidate.get("raw_text"),
            "signal": "money",
            "role": candidate.get("role"),
            "amount_eur": candidate.get("amount_eur"),
            "section_zone": candidate.get("authority_zone"),
            "authority_level": candidate.get("authority_level"),
            "authority_score": candidate.get("confidence"),
            "reason_code": candidate.get("reason_code"),
        }
        for candidate in money_candidates
    ]

    if any(note.startswith("malformed_") for note in candidate_warnings):
        notes.append("partial_malformed_money_candidates")
    if failures:
        notes.append("partial_authority_classification_failure")
    notes.extend(
        [
            "rendita_price_valuation_and_formalities_amounts_are_not_buyer_costs",
            "buyer_cost_certainty_requires_explicit_buyer_obligation",
        ]
    )

    if "mostly_unknown_authority_map" in notes and classified:
        return _result(
            domain,
            status=STATUS_FAIL_OPEN,
            value=value,
            confidence=0.0,
            winning_evidence=classified[:16],
            rules=["mostly_unknown_authority_map_cannot_support_money_certainty"],
            fail_open=True,
            notes=notes,
        )

    if classified:
        confidence = max(_safe_float(ev.get("authority_score"), 0.3) for ev in classified)
        return _result(
            domain,
            status=STATUS_PARTIAL if failures or any(note.startswith("malformed_") for note in notes) else STATUS_OK,
            value=value,
            confidence=confidence,
            winning_evidence=classified[:16],
            rules=[
                "money_amounts_are_classified_by_semantic_role",
                "no_extra_buyer_cost_without_explicit_buyer_obligation",
                "components_are_not_double_counted_with_totals",
            ],
            fail_open=bool(failures),
            notes=notes,
        )

    fail_open = "mostly_unknown_authority_map" in notes
    return _result(
        domain,
        status=STATUS_FAIL_OPEN if fail_open else STATUS_INSUFFICIENT,
        value=value,
        confidence=0.0,
        rules=["no_money_candidates"],
        fail_open=fail_open,
        notes=notes + ["no_money_candidates"],
    )


def load_shadow_candidates(candidates_folder: Optional[Any]) -> Dict[str, Any]:
    if not candidates_folder:
        return {}
    folder = Path(str(candidates_folder))
    out: Dict[str, Any] = {}
    for key, filename in (("money", "candidates_money.json"), ("triggers", "candidates_triggers.json")):
        path = folder / filename
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception:
            continue
        if isinstance(payload, list):
            out[key] = payload
    return out


def build_authority_shadow_resolvers(
    pages: Sequence[Dict[str, Any]],
    section_map: Any,
    *,
    candidates: Optional[Dict[str, Any]] = None,
    candidates_folder: Optional[Any] = None,
) -> Dict[str, Any]:
    candidate_payload = dict(candidates or {})
    if candidates_folder:
        loaded = load_shadow_candidates(candidates_folder)
        for key, value in loaded.items():
            candidate_payload.setdefault(key, value)

    lot = resolve_lot_structure_shadow(pages, section_map)
    occupancy = resolve_occupancy_shadow(pages, section_map)
    opponibilita = resolve_opponibilita_shadow(pages, section_map)
    legal = resolve_legal_formalities_shadow(pages, section_map)
    money = resolve_money_roles_shadow(pages, section_map, candidate_payload)
    domains = {
        "lot_structure": lot,
        "occupancy": occupancy,
        "opponibilita": opponibilita,
        "legal_formalities": legal,
        "money_roles": money,
    }
    status_order = {
        STATUS_OK: 0,
        STATUS_INSUFFICIENT: 1,
        STATUS_WARN: 2,
        STATUS_PARTIAL: 2,
        STATUS_FAIL_OPEN: 3,
    }
    overall_status = max((str(row.get("status") or STATUS_WARN) for row in domains.values()), key=lambda s: status_order.get(s, 2))
    warnings: List[str] = []
    for name, row in domains.items():
        if row.get("status") != STATUS_OK:
            warnings.append(f"{name}:{row.get('status')}")
        for note in row.get("notes") or []:
            warnings.append(f"{name}:{note}")

    return {
        "schema_version": SCHEMA_VERSION,
        "status": overall_status,
        "fail_open": any(bool(row.get("fail_open")) for row in domains.values()),
        "warnings": list(dict.fromkeys(warnings)),
        **domains,
    }
