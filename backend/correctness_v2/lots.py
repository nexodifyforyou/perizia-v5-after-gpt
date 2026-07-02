"""
Generic multi-lot detection for Correctness Mode v2.

An Italian perizia for a judicial sale often describes SEVERAL lots ("LOTTO 1",
"LOTTO 2", ...). Each lot is a distinct property with its own address, ownership,
occupancy, compliance issues, formalities and money figures. Blending two lots
into one customer report is a correctness disaster ("lot contamination"): an
address from one lot next to a price from another.

This module is GENERIC: it never hardcodes a tribunale, città or document. It
detects lots structurally from (a) the worksheet's own per-claim text and (b) the
extracted page text, then builds a per-lot index that preserves each lot's
evidence so the orchestrator can fail closed to manual review instead of guessing.

Design choice (fail closed): when the worksheet mixes two or more lots into its
flat fields, we do NOT try to pick one. We surface a multi-lot report and let the
job stop at NEEDS_MANUAL_REVIEW. Picking/serving a single lot is a deliberate
future step that needs a target-lot input from the API/UX.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict, List, Optional

# "lotto 3", "lotti 1", "lotto n. 2", "lotto unico". Accent/case handled via _norm.
_LOT_RE = re.compile(r"\blott[oi]\s+(?:n[.°ºo]*\s*)?(\d{1,3}|unico)\b")
# Enumerations like "lotti 1, 2 e 3" -> capture the trailing number run too.
_LOT_ENUM_RE = re.compile(r"\blott[oi]\s+((?:\d{1,3})(?:\s*[,e]\s*\d{1,3})+)\b")
# "bene 2", "bene n. 3". A bene is a single asset; several beni can belong to ONE
# lot (apartment + garage + cantina sold together). Multi-bene is NORMAL and must
# NEVER trigger the multi-lot manual-review gate — it is tracked for transparency
# only. Requires an explicit number so the ubiquitous bare word "bene" is ignored.
_BENE_RE = re.compile(r"\bben[ei]\s+(?:n[.°ºo]*\s*)?(\d{1,3})\b")


def _strip_accents(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c)
    )


def _norm(text: Any) -> str:
    return _strip_accents(str(text or "")).lower()


def lot_ids_in_text(text: Any) -> List[str]:
    """Return the lot identifiers mentioned in a single string (order-preserving)."""
    n = _norm(text)
    out: List[str] = []
    for m in _LOT_ENUM_RE.finditer(n):
        for num in re.findall(r"\d{1,3}", m.group(1)):
            out.append(num)
    for m in _LOT_RE.finditer(n):
        tok = m.group(1)
        out.append("unico" if tok == "unico" else tok)
    # Order-preserving de-dup: an enumeration and the single-lot pattern can both
    # match the same leading lot id ("lotti 1, 2 e 3").
    return _distinct(out)


def bene_ids_in_text(text: Any) -> List[str]:
    """Return the bene (single-asset) identifiers mentioned in a string."""
    n = _norm(text)
    return [m.group(1) for m in _BENE_RE.finditer(n)]


# Ordinal words sometimes used instead of digits ("LOTTO PRIMO"). Generic Italian,
# not tied to any document.
_ORDINAL_WORDS = {
    "primo": "1", "secondo": "2", "terzo": "3", "quarto": "4", "quinto": "5",
    "sesto": "6", "settimo": "7", "ottavo": "8", "nono": "9", "decimo": "10",
    "unico": "unico", "unica": "unico",
}
_ROMAN_RE = re.compile(r"^(?=[ivxlcdm])(m{0,3}(cm|cd|d?c{0,3})(xc|xl|l?x{0,3})(ix|iv|v?i{0,3}))$")


def normalize_lot_token(raw: Any) -> Optional[str]:
    """Normalize an arbitrary lot identifier to a canonical token (wording-agnostic).

    Handles the many ways a perizia can name a lot WITHOUT hardcoding any document:
    a number ("Lotto 2" -> "2"), an ordinal word ("Lotto Primo" -> "1"), "unico",
    a single letter label ("Lotto A" -> "a"), or a roman numeral ("Lotto III" ->
    "iii"). Returns None when nothing identifier-like is present.
    """
    n = _norm(raw).strip()
    if not n:
        return None
    digits = re.search(r"\d{1,3}", n)
    if digits:
        return digits.group(0)
    # Pull the token right after a 'lotto'/'lotti' word if present, else use n.
    m = re.search(r"\blott[oi]\b\s*(?:n[.°ºo]*\s*)?([a-z]+)", n)
    token = m.group(1) if m else n
    token = token.strip(" .)-:")
    if token in _ORDINAL_WORDS:
        return _ORDINAL_WORDS[token]
    if _ROMAN_RE.match(token):
        return token
    if len(token) == 1 and token.isalpha():
        return token
    return None


def _distinct(seq: List[str]) -> List[str]:
    seen: Dict[str, None] = {}
    for s in seq:
        seen.setdefault(s, None)
    return list(seen.keys())


def _numeric_ids(ids: List[str]) -> List[str]:
    return [i for i in ids if i.isdigit()]


# ---------------------------------------------------------------------------
# Worksheet scanning
# ---------------------------------------------------------------------------
def _worksheet_strings(worksheet: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Yield {path, text, evidence_pages} for every lot-bearing worksheet string."""
    out: List[Dict[str, Any]] = []

    def add(path: str, text: Any, ev: Any) -> None:
        if text:
            out.append({"path": path, "text": str(text), "evidence_pages": list(ev or [])})

    ci = worksheet.get("case_identity") or {}
    ci_ev = ci.get("evidence_pages", [])
    for key in ("lotto", "address", "property_type", "ownership_right"):
        add(f"case_identity.{key}", ci.get(key), ci_ev)

    oc = worksheet.get("occupancy") or {}
    add("occupancy.status", oc.get("status"), oc.get("evidence_pages", []))
    add("occupancy.title_info", oc.get("title_info"), oc.get("evidence_pages", []))

    for i, item in enumerate(worksheet.get("technical_compliance") or []):
        add(f"technical_compliance[{i}].area", item.get("area"), item.get("evidence_pages", []))
    for i, item in enumerate(worksheet.get("risk_classification") or []):
        add(f"risk_classification[{i}].area", item.get("area"), item.get("evidence_pages", []))
    for i, item in enumerate(worksheet.get("legal_formalities") or []):
        add(f"legal_formalities[{i}].description", item.get("description"), item.get("evidence_pages", []))

    money = worksheet.get("money") or {}
    for coll in ("deductions", "buyer_side_costs", "procedure_cancelled_costs", "uncertain_money"):
        for i, item in enumerate(money.get(coll) or []):
            add(f"money.{coll}[{i}]", item.get("label"), item.get("evidence_pages", []))

    for i, item in enumerate(worksheet.get("lots") or []):
        bits = " ".join(
            str(item.get(k) or "")
            for k in ("lot_id", "label", "address", "property_type", "ownership_right")
        )
        add(f"lots[{i}]", bits, item.get("evidence_pages", []))

    return out


def _money_rows_for_lot(worksheet: Dict[str, Any], lot_id: str) -> List[Dict[str, Any]]:
    money = worksheet.get("money") or {}
    rows: List[Dict[str, Any]] = []
    for coll in ("uncertain_money", "buyer_side_costs", "deductions", "procedure_cancelled_costs"):
        for item in money.get(coll) or []:
            label = item.get("label")
            if lot_id in _numeric_ids(lot_ids_in_text(label)):
                rows.append(
                    {
                        "label": label,
                        "amount": item.get("amount"),
                        "source": coll,
                        "evidence_pages": list(item.get("evidence_pages") or []),
                    }
                )
    return rows


def build_lot_report(
    worksheet: Dict[str, Any],
    pages: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Detect lots and build a per-lot index.

    A document is multi-lot when two or more DISTINCT numbered lots are evidenced
    by the worksheet's structured fields, or appear repeatedly in the page text.
    The page text alone needs a lot id to appear at least twice to count, which
    avoids a single stray table-of-contents mention tripping detection.
    """
    ws_strings = _worksheet_strings(worksheet)

    ws_hits: Dict[str, List[Dict[str, Any]]] = {}
    contaminated_fields: List[Dict[str, Any]] = []
    for entry in ws_strings:
        ids = _numeric_ids(_distinct(lot_ids_in_text(entry["text"])))
        for lid in ids:
            ws_hits.setdefault(lid, []).append(entry)
        if len(ids) >= 2:
            contaminated_fields.append({"path": entry["path"], "lot_ids": ids})

    ws_ids = set(ws_hits.keys())

    # Page-text ids: require a lot id to appear at least twice to be counted.
    text_counts: Dict[str, int] = {}
    for p in pages or []:
        text = p.get("text") if isinstance(p, dict) else p
        for lid in _numeric_ids(lot_ids_in_text(text)):
            text_counts[lid] = text_counts.get(lid, 0) + 1
    text_ids = {lid for lid, c in text_counts.items() if c >= 2}

    # Semantic ids from the analyst's structured lots[] array. This is the
    # WORDING-AGNOSTIC signal: the model reads "Lotto Primo", "Lotto A", a roman
    # numeral, etc., and emits a lot_id, which we normalize to a canonical token.
    # It is the primary multi-lot detector for documents whose lot labels are not
    # plain digits in the flat text; the regex above is the deterministic backstop.
    semantic_lots: Dict[str, Dict[str, Any]] = {}
    for item in worksheet.get("lots") or []:
        tok = normalize_lot_token(item.get("lot_id") or item.get("label") or item.get("id"))
        if tok:
            semantic_lots.setdefault(tok, item)
    semantic_ids = set(semantic_lots.keys())

    all_ids = sorted(ws_ids | text_ids | semantic_ids, key=_lot_sort_key)

    lots: List[Dict[str, Any]] = []
    for lid in all_ids:
        hits = ws_hits.get(lid, [])
        ev: List[int] = []
        for h in hits:
            for pg in h["evidence_pages"]:
                if pg not in ev:
                    ev.append(pg)
        ws_lot = semantic_lots.get(lid, {})
        for pg in ws_lot.get("evidence_pages") or []:
            if pg not in ev:
                ev.append(pg)
        identifiers = [
            h["text"][:160]
            for h in hits
            if h["path"].startswith("case_identity") or h["path"].startswith("lots")
        ]
        ident_bits = " ".join(
            str(ws_lot.get(k) or "")
            for k in ("lot_id", "label", "address", "property_type", "ownership_right")
        ).strip()
        if ident_bits and ident_bits not in identifiers:
            identifiers.append(ident_bits[:160])
        lots.append(
            {
                "lot_id": lid,
                "claim_paths": [h["path"] for h in hits],
                "identifiers": identifiers,
                "money": _money_rows_for_lot(worksheet, lid),
                "evidence_pages": sorted(ev),
            }
        )

    # Bene detection is informational only. The four real-world combinations —
    # multi-lot/multi-bene, multi-lot/single-bene, single-lot/multi-bene,
    # single-lot/single-bene — are all handled by gating ONLY on lot count:
    # several beni inside one lot are sold together and are never contamination.
    bene_ids: List[str] = []
    for entry in ws_strings:
        bene_ids.extend(bene_ids_in_text(entry["text"]))
    for p in pages or []:
        text = p.get("text") if isinstance(p, dict) else p
        bene_ids.extend(bene_ids_in_text(text))
    bene_ids = _distinct(bene_ids)

    multi_lot = len(all_ids) >= 2
    return {
        "multi_lot": multi_lot,
        "lot_count": len(all_ids),
        "lot_ids": all_ids,
        "lots": lots,
        "bene_count": len(bene_ids),
        "multi_bene": len(bene_ids) >= 2,
        "bene_ids": sorted(bene_ids, key=lambda s: int(s)),
        "contaminated_fields": contaminated_fields,
        "detection": {
            "worksheet_lot_ids": sorted(ws_ids, key=_lot_sort_key),
            "page_text_lot_ids": sorted(text_ids, key=_lot_sort_key),
            "semantic_lot_ids": sorted(semantic_ids, key=_lot_sort_key),
        },
    }


def _lot_sort_key(lid: str):
    """Sort numeric lots numerically, non-numeric (roman/letter/'unico') after, by text."""
    s = str(lid)
    return (0, int(s)) if s.isdigit() else (1, s)


def worksheet_lot_ids(worksheet: Dict[str, Any]) -> List[str]:
    """Distinct numbered lot ids evidenced by the worksheet's structured fields."""
    ids: List[str] = []
    for entry in _worksheet_strings(worksheet):
        ids.extend(_numeric_ids(lot_ids_in_text(entry["text"])))
    return sorted(_distinct(ids), key=lambda s: int(s))


def distinct_lots(worksheet: Dict[str, Any]) -> List[str]:
    """All distinct lot tokens (wording-agnostic): numbered flat-field ids PLUS the
    analyst's structured lots[] tokens (which capture ordinal/letter/roman labels).

    This is the count the multi-lot gate should trust regardless of how a specific
    perizia spells its lots.
    """
    ids = set(worksheet_lot_ids(worksheet))
    for item in worksheet.get("lots") or []:
        tok = normalize_lot_token(item.get("lot_id") or item.get("label") or item.get("id"))
        if tok:
            ids.add(tok)
    return sorted(ids, key=_lot_sort_key)


def contaminated_flat_fields(worksheet: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Flat identity/money fields that mix two or more distinct lot ids."""
    out: List[Dict[str, Any]] = []
    ci = worksheet.get("case_identity") or {}
    for key in ("address", "property_type", "ownership_right", "lotto"):
        ids = _numeric_ids(_distinct(lot_ids_in_text(ci.get(key))))
        if len(ids) >= 2:
            out.append({"path": f"case_identity.{key}", "lot_ids": ids})
    return out


def contaminated_worksheet_fields(worksheet: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Any worksheet string (identity, occupancy, money, compliance, formalities)
    that internally mixes two or more distinct lot ids.

    Strictly stronger than :func:`contaminated_flat_fields` (which only scans
    case_identity): this catches a money label, occupancy status or compliance
    area that splices two lots together. Generic — keyed only on numbered lots,
    never on beni or any specific document.
    """
    out: List[Dict[str, Any]] = []
    for entry in _worksheet_strings(worksheet):
        ids = _numeric_ids(_distinct(lot_ids_in_text(entry["text"])))
        if len(ids) >= 2:
            out.append({"path": entry["path"], "lot_ids": ids})
    return out
