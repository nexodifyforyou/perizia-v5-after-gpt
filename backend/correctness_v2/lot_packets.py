"""
Generic lot segmentation + per-lot packet builder for Correctness Mode v2.

Multi-lot handling evolves beyond the old "stop at manual review" gate: when a
perizia describes several lots, we now segment the document into per-lot page
ranges and build inspectable artifacts so a caller can either pick a lot or ask
to analyze all lots. NOTHING here blends lots: each lot keeps its own pages,
identity and money, and pages that belong to no specific lot stay "global".

Design (all generic — never branches on a specific tribunale / città / document):

  * Page→lot assignment walks the pages in order and uses the same numbered-lot
    regex as :mod:`lots`. A page that explicitly names exactly one lot anchors the
    "current lot". A page with no lot mention inherits the current lot
    (carry-forward) — perizie describe a lot across several consecutive pages.
    Pages before the first lot mention are GLOBAL (frontespizio, metodologia,
    premesse common to all lots). A page naming two or more lots is SHARED
    (typically a summary table) and is deliberately NOT fed into a single-lot
    re-analysis, to avoid re-introducing contamination.

  * ``select_lot_pages`` returns the safe page subset to RE-ANALYZE for one lot:
    global pages + that lot's own (single-lot / carry-forward) pages. Shared
    multi-lot pages are excluded on purpose (uncertainty over contamination).

Bene-awareness: several beni (apartment + box + cantina) inside ONE lot are
normal and are tracked per lot, never treated as separate lots.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from . import lots as lots_mod

LOT_INDEX_SCHEMA_VERSION = "cv2.lot_index.v1"
PER_LOT_PACKETS_SCHEMA_VERSION = "cv2.per_lot_packets.v1"
SELECTED_LOT_CONTEXT_SCHEMA_VERSION = "cv2.selected_lot_context.v1"

_SNIPPET_CHARS = 240


def _page_number(entry: Any, fallback: int) -> int:
    if isinstance(entry, dict):
        try:
            return int(entry.get("page_number", fallback))
        except Exception:
            return fallback
    return fallback


def _page_text(entry: Any) -> str:
    if isinstance(entry, dict):
        return str(entry.get("text") or "")
    return str(entry or "")


def _numeric_lot_ids(text: Any) -> List[str]:
    ids = lots_mod.lot_ids_in_text(text)
    return [i for i in ids if i.isdigit()]


# ---------------------------------------------------------------------------
# Segmentation
# ---------------------------------------------------------------------------
def segment_pages(
    pages: Optional[List[Dict[str, Any]]],
    lot_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Assign every page to a lot (or to the global/shared buckets).

    Returns a dict with:
      * ``page_assignments``: per page -> {page, explicit_lots, assigned_lot, method}
      * ``lot_pages``: {lot_id: [page_number, ...]} (single-lot + carry-forward)
      * ``global_pages``: pages that belong to all lots (preamble / common)
      * ``shared_pages``: pages naming two or more lots (excluded from re-analysis)
      * ``lot_ids``: lots actually seen in the page text
    """
    page_assignments: List[Dict[str, Any]] = []
    lot_pages: Dict[str, List[int]] = {}
    global_pages: List[int] = []
    shared_pages: List[int] = []
    seen_lot_ids: List[int] = []

    current_lot: Optional[str] = None
    for idx, entry in enumerate(pages or [], start=1):
        num = _page_number(entry, idx)
        explicit = _dedup(_numeric_lot_ids(_page_text(entry)))
        for lid in explicit:
            if lid not in [str(x) for x in seen_lot_ids]:
                seen_lot_ids.append(lid)

        if len(explicit) == 1:
            current_lot = explicit[0]
            assigned = current_lot
            method = "explicit"
            lot_pages.setdefault(assigned, [])
            if num not in lot_pages[assigned]:
                lot_pages[assigned].append(num)
        elif len(explicit) >= 2:
            # Page references multiple lots -> shared (e.g. a summary table). It is
            # added to every referenced lot's index pages but NOT carried forward
            # and NOT used as safe single-lot re-analysis input.
            assigned = None
            method = "shared"
            shared_pages.append(num)
            for lid in explicit:
                lot_pages.setdefault(lid, [])
        else:
            # No explicit lot on this page.
            if current_lot is None:
                assigned = None
                method = "global"
                global_pages.append(num)
            else:
                assigned = current_lot
                method = "carry_forward"
                lot_pages.setdefault(assigned, [])
                if num not in lot_pages[assigned]:
                    lot_pages[assigned].append(num)

        page_assignments.append(
            {
                "page": num,
                "explicit_lots": explicit,
                "assigned_lot": assigned,
                "method": method,
            }
        )

    # If the caller knows the canonical lot set (from lot_report), make sure each
    # known lot at least exists as a key (possibly with no own pages).
    for lid in lot_ids or []:
        if str(lid).isdigit():
            lot_pages.setdefault(str(lid), [])

    ordered_ids = sorted(lot_pages.keys(), key=lambda s: int(s) if s.isdigit() else 1_000_000)
    return {
        "page_assignments": page_assignments,
        "lot_pages": {lid: sorted(lot_pages[lid]) for lid in ordered_ids},
        "global_pages": sorted(global_pages),
        "shared_pages": sorted(set(shared_pages)),
        "lot_ids": [str(x) for x in seen_lot_ids],
    }


def _dedup(seq: List[str]) -> List[str]:
    seen: Dict[str, None] = {}
    for s in seq:
        seen.setdefault(s, None)
    return list(seen.keys())


def select_lot_pages(
    pages: Optional[List[Dict[str, Any]]],
    segmentation: Dict[str, Any],
    lot_id: str,
) -> List[Dict[str, Any]]:
    """Return the SAFE page subset to re-analyze for a single lot.

    Global (common) pages + that lot's own single-lot/carry-forward pages, in
    document order. Shared multi-lot pages are intentionally excluded so a single
    lot's re-analysis can never absorb another lot's data.
    """
    lot_id = str(lot_id)
    own = set(segmentation.get("lot_pages", {}).get(lot_id, []))
    glob = set(segmentation.get("global_pages", []))
    shared = set(segmentation.get("shared_pages", []))
    keep = (own | glob) - shared
    out: List[Dict[str, Any]] = []
    for idx, entry in enumerate(pages or [], start=1):
        num = _page_number(entry, idx)
        if num in keep:
            out.append(entry)
    return out


# ---------------------------------------------------------------------------
# Lot index + per-lot packets
# ---------------------------------------------------------------------------
def _worksheet_lot_entry(worksheet: Dict[str, Any], lot_id: str) -> Dict[str, Any]:
    for item in worksheet.get("lots") or []:
        if str(item.get("lot_id") or "").strip() == str(lot_id):
            return item
    return {}


def _occupancy_summary(ws_lot: Dict[str, Any]) -> Optional[str]:
    return ws_lot.get("occupancy_status")


def _lot_money(ws_lot: Dict[str, Any], lot_report_lot: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Lot-specific money: structured per-lot fields plus any label-tagged rows."""
    money: List[Dict[str, Any]] = []
    for field, label in (("prezzo_base_asta", "Prezzo base d'asta"), ("sale_value", "Valore di vendita giudiziaria")):
        amount = ws_lot.get(field)
        if amount is not None:
            money.append(
                {
                    "label": label,
                    "amount": amount,
                    "source": f"lots.{field}",
                    "evidence_pages": list(ws_lot.get("evidence_pages") or []),
                }
            )
    for row in lot_report_lot.get("money") or []:
        money.append(dict(row))
    return money


def _snippet(pages: Optional[List[Dict[str, Any]]], page_number: int) -> str:
    for idx, entry in enumerate(pages or [], start=1):
        if _page_number(entry, idx) == page_number:
            text = _page_text(entry).strip().replace("\n", " ")
            return text[:_SNIPPET_CHARS]
    return ""


def _bene_ids_for_pages(pages: Optional[List[Dict[str, Any]]], page_numbers: List[int]) -> List[str]:
    out: List[str] = []
    wanted = set(page_numbers)
    for idx, entry in enumerate(pages or [], start=1):
        if _page_number(entry, idx) in wanted:
            out.extend(lots_mod.bene_ids_in_text(_page_text(entry)))
    return sorted(_dedup(out), key=lambda s: int(s) if s.isdigit() else 1_000_000)


# ---------------------------------------------------------------------------
# Shared-summary row projection (lot-tagged rows on shared pages -> lot packets)
# ---------------------------------------------------------------------------
# A shared multi-lot page (summary table, TOC, "schema riassuntivo") is excluded
# wholesale from single-lot re-analysis to prevent contamination — but its ROWS
# are often clearly lot-tagged ("LOTTO 1 - PREZZO BASE D'ASTA: € 64.198,00").
# Those rows are projected deterministically into the matching lot's money so the
# information is preserved without ever re-entering the model. A row whose lot
# association is unclear is preserved under uncertain_money (manual review),
# never blended into a lot.

# Euro amounts in Italian notation: "€ 64.198,00", "€1.234", "Euro 950,50".
_AMOUNT_RE = re.compile(
    r"(?:€|\beuro\b)\s*(\d{1,3}(?:[.\s]\d{3})+(?:,\d{1,2})?|\d+(?:,\d{1,2})?)",
    re.IGNORECASE,
)
# Lot tag with position info (works on the raw line; the pattern is accent-free).
_LOT_TAG_RE = re.compile(r"\blott[oi]\s+(?:n[.°ºo]*\s*)?(\d{1,3})\b", re.IGNORECASE)
# Trailing TOC dot-leaders ("....... 41") stripped from row labels.
_DOT_LEADER_RE = re.compile(r"\.{3,}\s*\d*\s*$")

_LABEL_CHARS = 140

# Generic canonical money fields recognized from a row's own words (keyword ->
# field). Checked in order; never document-specific.
_ROW_FIELD_KEYWORDS = [
    ("offerta minima", "offerta_minima"),
    ("rialzo minimo", "rialzo_minimo"),
    ("cauzione", "cauzione"),
    ("prezzo base", "prezzo_base_asta"),
    ("base d'asta", "prezzo_base_asta"),
    ("base dasta", "prezzo_base_asta"),
    ("valore di mercato", "market_value"),
    ("valore di vendita", "sale_value"),
    ("stato di fatto", "current_state_value"),
]


def _parse_it_amount(raw: str) -> Optional[float]:
    try:
        return float(raw.replace(" ", "").replace(".", "").replace(",", "."))
    except (TypeError, ValueError):
        return None


def _row_field(label: str) -> Optional[str]:
    n = lots_mod._norm(label)
    for keyword, field in _ROW_FIELD_KEYWORDS:
        if keyword in n:
            return field
    return None


def _clean_label(text: str) -> str:
    label = _DOT_LEADER_RE.sub("", text.strip())
    label = _AMOUNT_RE.sub("", label).strip(" .:;-–")
    return label[:_LABEL_CHARS] or "Importo da tabella riassuntiva"


def project_shared_summary_rows(
    pages: Optional[List[Dict[str, Any]]],
    segmentation: Dict[str, Any],
) -> Dict[str, Any]:
    """Project lot-tagged money rows found on shared multi-lot pages.

    Scans every shared page line by line. An amount is assigned to a lot ONLY
    when its row is clearly tagged with exactly that one lot (single lot tag on
    the line, or — on a line naming several lots — the nearest lot tag BEFORE the
    amount). Everything else is preserved as uncertain. Rows repeated across
    shared pages (TOC + summary) are deduplicated with unioned evidence.

    Returns ``{"projected": {lot_id: [row, ...]}, "uncertain": [row, ...]}``
    where each row is ``{label, amount, field, evidence_pages, source}``
    (``field`` is a canonical money field or None).
    """
    shared = set(segmentation.get("shared_pages", []))
    projected: Dict[str, Dict[Any, Dict[str, Any]]] = {}
    uncertain: Dict[Any, Dict[str, Any]] = {}

    def add(bucket: Dict[Any, Dict[str, Any]], key: Any, row: Dict[str, Any], page: int) -> None:
        existing = bucket.get(key)
        if existing is None:
            bucket[key] = row
        elif page not in existing["evidence_pages"]:
            existing["evidence_pages"].append(page)

    for idx, entry in enumerate(pages or [], start=1):
        num = _page_number(entry, idx)
        if num not in shared:
            continue
        for line in _page_text(entry).splitlines():
            amounts = list(_AMOUNT_RE.finditer(line))
            if not amounts:
                continue
            tags = list(_LOT_TAG_RE.finditer(line))
            for m in amounts:
                amount = _parse_it_amount(m.group(1))
                if amount is None:
                    continue
                lot_id: Optional[str] = None
                segment = line
                if len(tags) == 1:
                    lot_id = tags[0].group(1)
                elif len(tags) >= 2:
                    # Nearest lot tag BEFORE the amount; its segment ends at the
                    # next lot tag so labels never mix two lots.
                    before = [t for t in tags if t.start() < m.start()]
                    if before:
                        tag = before[-1]
                        nxt = next((t for t in tags if t.start() > tag.start()), None)
                        if nxt is None or m.start() < nxt.start():
                            lot_id = tag.group(1)
                            segment = line[tag.start(): nxt.start() if nxt else len(line)]
                label = _clean_label(segment)
                field = _row_field(segment)
                if lot_id is not None:
                    row = {
                        "label": label,
                        "amount": amount,
                        "field": field,
                        "evidence_pages": [num],
                        "source": "shared_summary_projection",
                    }
                    add(projected.setdefault(lot_id, {}),
                        (field or lots_mod._norm(label), round(amount, 2)), row, num)
                else:
                    row = {
                        "label": label,
                        "amount": amount,
                        "field": field,
                        "evidence_pages": [num],
                        "source": "shared_summary_projection",
                        "manual_review": True,
                        "reason": (
                            "Importo su pagina riassuntiva multi-lotto senza riga "
                            "chiaramente associata a un lotto."
                        ),
                    }
                    add(uncertain, (lots_mod._norm(label), round(amount, 2)), row, num)

    return {
        "projected": {lid: list(rows.values()) for lid, rows in sorted(projected.items())},
        "uncertain": list(uncertain.values()),
    }


# ---------------------------------------------------------------------------
# Strict per-lot money assignment (evidence-page -> lot, never blended)
# ---------------------------------------------------------------------------
_GLOBAL = "__global__"
_SHARED = "__shared__"

# The value (scalar) money fields a lot keeps separate, in display order.
_VALUE_FIELDS = [
    ("market_value", "Valore di mercato"),
    ("current_state_value", "Valore nello stato di fatto"),
    ("sale_value", "Valore di vendita giudiziaria"),
    ("regularization_costs", "Costi di regolarizzazione"),
    ("cancellation_costs", "Costi di cancellazione formalità"),
]
_AUCTION_FIELDS = [
    ("prezzo_base_asta", "Prezzo base d'asta"),
    ("offerta_minima", "Offerta minima"),
    ("rialzo_minimo", "Rialzo minimo"),
    ("cauzione", "Cauzione"),
]


def _page_lot_map(segmentation: Dict[str, Any]) -> Dict[int, str]:
    """page_number -> assigned lot id, or _GLOBAL / _SHARED."""
    out: Dict[int, str] = {}
    for pa in segmentation.get("page_assignments", []):
        method = pa.get("method")
        if method in ("explicit", "carry_forward"):
            out[pa["page"]] = str(pa.get("assigned_lot"))
        elif method == "global":
            out[pa["page"]] = _GLOBAL
        elif method == "shared":
            out[pa["page"]] = _SHARED
    return out


def _assign_lot(evidence_pages: Optional[List[int]], page_lot: Dict[int, str]) -> Optional[str]:
    """Return the lot id an amount belongs to, '__global__', or None (ambiguous).

    An amount is assigned to a lot ONLY if every one of its evidence pages maps to
    that single lot. All-global pages -> global. No evidence, mixed lots, or any
    shared multi-lot page -> None (must be preserved as uncertain).
    """
    if not evidence_pages:
        return None
    seen = {page_lot.get(int(p)) for p in evidence_pages if page_lot.get(int(p)) is not None}
    if not seen:
        return None
    if seen == {_GLOBAL}:
        return _GLOBAL
    real = {s for s in seen if s not in (_GLOBAL, _SHARED)}
    if _SHARED in seen:
        return None
    if len(real) == 1 and (seen - real) <= {_GLOBAL}:
        # Single real lot, possibly alongside global preamble pages -> that lot.
        return next(iter(real))
    return None


def classify_compliance_scope(
    worksheet: Dict[str, Any],
    segmentation: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Classify each whole-document compliance section as lot-specific, global
    or unclear, by mapping its evidence pages through the page segmentation.

    Part of the whole-document map: a compliance statement is only usable for a
    selected lot when its scope is that lot or clearly global; 'unclear' scope
    must stay an uncertainty, never a conformity claim.
    """
    page_lot = _page_lot_map(segmentation)
    out: List[Dict[str, Any]] = []
    for i, item in enumerate(worksheet.get("technical_compliance") or []):
        ev = list(item.get("evidence_pages") or [])
        target = _assign_lot(ev, page_lot)
        if target == _GLOBAL:
            scope, lot_id = "global", None
        elif target is None:
            scope, lot_id = "unclear", None
        else:
            scope, lot_id = "lot", str(target)
        out.append(
            {
                "path": f"technical_compliance[{i}]",
                "area": item.get("area"),
                "classification": item.get("classification"),
                "scope": scope,
                "lot_id": lot_id,
                "evidence_pages": ev,
            }
        )
    return out


def _empty_lot_money() -> Dict[str, Any]:
    section: Dict[str, Any] = {f: None for f, _ in _VALUE_FIELDS}
    section.update({f: None for f, _ in _AUCTION_FIELDS})
    section["deductions"] = []
    section["buyer_side_costs"] = []
    section["procedure_cancelled_formalities"] = []
    section["shared_summary_rows"] = []
    return section


def _row(label: str, amount: Any, evidence_pages: Any, **extra) -> Dict[str, Any]:
    row = {"label": label, "amount": amount, "evidence_pages": list(evidence_pages or [])}
    row.update(extra)
    return row


def build_lot_money(
    worksheet: Dict[str, Any],
    segmentation: Dict[str, Any],
    pages: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Assign every monetary value to a lot, to global/common, or to uncertain_money.

    STRICTLY per-lot: an amount lands in a lot's section only when its evidence
    pages map unambiguously to that one lot. Amounts on shared multi-lot pages, on
    mixed pages, or with no evidence are NEVER blended into a lot — they are
    preserved under ``uncertain_money`` with their evidence and a manual_review
    flag. No significant amount is dropped.

    When ``pages`` is provided, clearly lot-tagged rows on shared summary pages
    are additionally projected into the matching lot (see
    :func:`project_shared_summary_rows`) — never blended, unclear rows stay
    uncertain.
    """
    page_lot = _page_lot_map(segmentation)
    money = worksheet.get("money") or {}
    money_ev = list(money.get("evidence_pages") or [])

    by_lot: Dict[str, Dict[str, Any]] = {}
    global_money = _empty_lot_money()
    uncertain: List[Dict[str, Any]] = []

    def bucket(target: Optional[str]) -> Dict[str, Any]:
        if target == _GLOBAL:
            return global_money
        return by_lot.setdefault(target, _empty_lot_money())

    def place_value(field: str, label: str, amount: Any, evidence: List[int], reason_prefix: str) -> None:
        if amount is None:
            return
        target = _assign_lot(evidence, page_lot)
        if target in (None,):
            uncertain.append(
                _row(label, amount, evidence, kind="uncertain", manual_review=True,
                     reason=f"{reason_prefix}: associazione al lotto non determinabile dall'evidenza.")
            )
            return
        bucket(target)[field] = {"amount": amount, "evidence_pages": list(evidence or [])}

    def place_row(section_key: str, label: str, amount: Any, evidence: List[int], reason_prefix: str) -> None:
        if amount is None:
            return
        target = _assign_lot(evidence, page_lot)
        if target is None:
            uncertain.append(
                _row(label, amount, evidence, kind="uncertain", manual_review=True,
                     reason=f"{reason_prefix}: associazione al lotto non determinabile dall'evidenza.")
            )
            return
        bucket(target)[section_key].append(_row(label, amount, evidence))

    # 1) Model-linked per-lot values (worksheet.lots[]) — these are explicitly tied
    #    to a lot by the analyst, so they go straight to that lot.
    for item in worksheet.get("lots") or []:
        tok = lots_mod.normalize_lot_token(item.get("lot_id") or item.get("label") or item.get("id"))
        if not tok:
            continue
        ev = list(item.get("evidence_pages") or [])
        section = by_lot.setdefault(tok, _empty_lot_money())
        if item.get("prezzo_base_asta") is not None and section["prezzo_base_asta"] is None:
            section["prezzo_base_asta"] = {"amount": item["prezzo_base_asta"], "evidence_pages": ev}
        if item.get("sale_value") is not None and section["sale_value"] is None:
            section["sale_value"] = {"amount": item["sale_value"], "evidence_pages": ev}

    # 2) Document-level scalar values (assigned by their evidence pages).
    for field, label in _VALUE_FIELDS:
        place_value(field, label, money.get(field), money_ev, label)

    # 3) Auction terms.
    at = money.get("auction_terms") or {}
    at_ev = list(at.get("evidence_pages") or []) or money_ev
    base = at.get("prezzo_base_asta")
    if base is None:
        base = money.get("base_auction_value")
    place_value("prezzo_base_asta", "Prezzo base d'asta", base, at_ev, "Prezzo base d'asta")
    for field, label in _AUCTION_FIELDS[1:]:
        place_value(field, label, at.get(field), at_ev, label)

    # 4) Itemized cost rows.
    for d in money.get("deductions") or []:
        place_row("deductions", d.get("label") or "Deprezzamento", d.get("amount"),
                  d.get("evidence_pages") or [], "Deduzione")
    for c in money.get("buyer_side_costs") or []:
        place_row("buyer_side_costs", c.get("label") or "Costo a carico acquirente", c.get("amount"),
                  c.get("evidence_pages") or [], "Costo a carico acquirente")
    for c in money.get("procedure_cancelled_costs") or []:
        place_row("procedure_cancelled_formalities", c.get("label") or "Formalità cancellata",
                  c.get("amount"), c.get("evidence_pages") or [], "Formalità cancellata")

    # 5) Amounts the analyst already flagged uncertain stay uncertain (evidence kept).
    for u in money.get("uncertain_money") or []:
        if u.get("amount") is None:
            continue
        uncertain.append(
            _row(u.get("label") or "Importo da verificare", u.get("amount"),
                 u.get("evidence_pages") or [], kind="uncertain", manual_review=True,
                 reason=u.get("reason") or "Importo segnalato come incerto dall'analista.")
        )

    # 6) Shared-summary projection: clearly lot-tagged rows on shared multi-lot
    #    pages land in their lot (rows + canonical field fill-in when the lot has
    #    no value yet); unclear rows are preserved as uncertain (deduplicated
    #    against amounts already listed there).
    if pages:
        proj = project_shared_summary_rows(pages, segmentation)
        for lid, rows in proj["projected"].items():
            section = by_lot.setdefault(lid, _empty_lot_money())
            for row in rows:
                section["shared_summary_rows"].append(dict(row))
                field = row.get("field")
                if field and section.get(field) is None:
                    section[field] = {
                        "amount": row["amount"],
                        "evidence_pages": list(row["evidence_pages"]),
                        "source": "shared_summary_projection",
                    }
        for row in proj["uncertain"]:
            if any(_amount_close(row["amount"], u.get("amount")) for u in uncertain):
                continue
            uncertain.append(dict(row, kind="uncertain"))

    return {
        "by_lot": by_lot,
        "global": global_money,
        "uncertain_money": uncertain,
        "needs_manual_review_money": bool(uncertain),
    }


def _amount_close(a: Any, b: Any) -> bool:
    try:
        return abs(float(a) - float(b)) <= 1.0
    except (TypeError, ValueError):
        return False


def build_lot_index(
    worksheet: Dict[str, Any],
    pages: Optional[List[Dict[str, Any]]],
    lot_report: Dict[str, Any],
    segmentation: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build ``lot_index.json``: a per-lot summary with evidence and confidence.

    Pure function of the worksheet + page text + deterministic lot_report. Lists
    each lot's identity, occupancy summary, key money values and page evidence so a
    caller (or human) can choose a target lot.
    """
    segmentation = segmentation or segment_pages(pages, lot_report.get("lot_ids"))
    report_lots = {str(L.get("lot_id")): L for L in lot_report.get("lots") or []}
    lot_ids = lot_report.get("lot_ids") or sorted(
        segmentation.get("lot_pages", {}).keys(), key=lambda s: int(s) if s.isdigit() else 1_000_000
    )
    lot_money = build_lot_money(worksheet, segmentation, pages)

    lots_out: List[Dict[str, Any]] = []
    for lid in lot_ids:
        ws_lot = _worksheet_lot_entry(worksheet, lid)
        rep_lot = report_lots.get(str(lid), {})
        seg_pages = segmentation.get("lot_pages", {}).get(str(lid), [])
        evidence_pages = sorted(_dedup_int(list(ws_lot.get("evidence_pages") or []) + list(rep_lot.get("evidence_pages") or []) + seg_pages))

        # Confidence: an explicit worksheet identity is high; segmentation-only is medium;
        # carry-forward-only / empty is low.
        if ws_lot.get("address") or ws_lot.get("label") or rep_lot.get("identifiers"):
            confidence = "high"
        elif seg_pages:
            confidence = "medium"
        else:
            confidence = "low"

        notes: List[str] = []
        if str(lid) in [str(x) for x in segmentation.get("lot_ids", [])] and not seg_pages:
            notes.append("Lotto citato nel testo ma senza pagine assegnate in modo univoco.")
        if not ws_lot:
            notes.append("Nessuna voce strutturata 'lots[]' dall'analista per questo lotto.")

        lots_out.append(
            {
                "lot_id": str(lid),
                "lot_number": str(lid),
                "label": ws_lot.get("label"),
                "address": ws_lot.get("address"),
                "property_type": ws_lot.get("property_type"),
                "ownership_right": ws_lot.get("ownership_right"),
                "occupancy_summary": _occupancy_summary(ws_lot),
                # STRICT per-lot money: a dedicated section per lot (never shared).
                "money": lot_money["by_lot"].get(str(lid), _empty_lot_money()),
                # Legacy flat list kept for back-compat (model-linked + tagged rows).
                "key_money": _lot_money(ws_lot, rep_lot),
                "bene_ids": _bene_ids_for_pages(pages, seg_pages),
                "page_evidence": evidence_pages,
                "segmentation_pages": seg_pages,
                "confidence": confidence,
                "notes": notes,
            }
        )

    return {
        "schema_version": LOT_INDEX_SCHEMA_VERSION,
        "multi_lot": bool(lot_report.get("multi_lot")),
        "lot_count": lot_report.get("lot_count", len(lots_out)),
        "lot_ids": [str(x) for x in lot_ids],
        "global_pages": segmentation.get("global_pages", []),
        "shared_pages": segmentation.get("shared_pages", []),
        # Whole-document compliance map: each compliance section's scope
        # (lot-specific / global / unclear) derived from its evidence pages.
        "compliance_sections": classify_compliance_scope(worksheet, segmentation),
        "lots": lots_out,
        # Money that applies to the whole procedure, and amounts that could not be
        # safely tied to a single lot (preserved with evidence, never dropped).
        "global_money": lot_money["global"],
        "uncertain_money": lot_money["uncertain_money"],
        "needs_manual_review_money": lot_money["needs_manual_review_money"],
    }


def build_per_lot_packets(
    worksheet: Dict[str, Any],
    pages: Optional[List[Dict[str, Any]]],
    lot_report: Dict[str, Any],
    segmentation: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build ``per_lot_packets.json``: per-lot pages/snippets + lot-specific data.

    Preserves each lot's pages, bene list and lot-tagged money separately, keeps
    global pages apart, and flags where deep per-lot detail (occupancy, compliance,
    formalities) still requires a per-lot re-analysis (so nothing is faked).
    """
    segmentation = segmentation or segment_pages(pages, lot_report.get("lot_ids"))
    report_lots = {str(L.get("lot_id")): L for L in lot_report.get("lots") or []}
    lot_ids = lot_report.get("lot_ids") or sorted(
        segmentation.get("lot_pages", {}).keys(), key=lambda s: int(s) if s.isdigit() else 1_000_000
    )
    lot_money = build_lot_money(worksheet, segmentation, pages)

    global_pages = segmentation.get("global_pages", [])
    shared_pages = segmentation.get("shared_pages", [])

    packets: List[Dict[str, Any]] = []
    for lid in lot_ids:
        ws_lot = _worksheet_lot_entry(worksheet, lid)
        rep_lot = report_lots.get(str(lid), {})
        own_pages = segmentation.get("lot_pages", {}).get(str(lid), [])
        analysis_pages = sorted(set(own_pages) | set(global_pages))

        snippets = [{"page": p, "text": _snippet(pages, p)} for p in own_pages]
        packets.append(
            {
                "lot_id": str(lid),
                "lot_number": str(lid),
                "identity": {
                    "label": ws_lot.get("label"),
                    "address": ws_lot.get("address"),
                    "property_type": ws_lot.get("property_type"),
                    "ownership_right": ws_lot.get("ownership_right"),
                    "occupancy_status": ws_lot.get("occupancy_status"),
                    "evidence_pages": list(ws_lot.get("evidence_pages") or []),
                },
                "bene_ids": _bene_ids_for_pages(pages, own_pages),
                "lot_specific_pages": own_pages,
                "global_pages": list(global_pages),
                "shared_multi_lot_pages": list(shared_pages),
                "reanalysis_input_pages": analysis_pages,
                # STRICT per-lot money sections (req: lot_money per lot).
                "lot_money": lot_money["by_lot"].get(str(lid), _empty_lot_money()),
                "lot_specific_money": _lot_money(ws_lot, rep_lot),  # legacy flat list
                "snippets": snippets,
                "identifiers": list(rep_lot.get("identifiers") or []),
                # Deep per-lot fields are only safe to populate from a per-lot
                # re-analysis on reanalysis_input_pages; we never copy the blended
                # document-level occupancy/compliance/formalities into a lot packet.
                "lot_specific_detail_requires_analysis": True,
                "uncertainty": (
                    [] if own_pages else
                    ["Nessuna pagina assegnata in modo univoco a questo lotto: assegnazione incerta."]
                ),
            }
        )

    return {
        "schema_version": PER_LOT_PACKETS_SCHEMA_VERSION,
        "lot_count": lot_report.get("lot_count", len(packets)),
        "lot_ids": [str(x) for x in lot_ids],
        "global_pages": list(global_pages),
        "shared_pages": list(shared_pages),
        "packets": packets,
        "global_money": lot_money["global"],
        "uncertain_money": lot_money["uncertain_money"],
        "needs_manual_review_money": lot_money["needs_manual_review_money"],
    }


def build_document_map(
    lot_report: Dict[str, Any],
    segmentation: Dict[str, Any],
    lot_index: Dict[str, Any],
    selected_lot_id: str,
) -> Dict[str, Any]:
    """Compact whole-document map handed to the selected-lot re-analysis prompt.

    Built from the full-document pass (lot detection + segmentation + lot index)
    so the second pass is not blind to the document structure. It carries ONLY
    structure (lot ids, page buckets, beni, compliance-section scopes) — never
    another lot's content. Compliance sections of OTHER lots are excluded; the
    selected lot's own, global and unclear ones are kept (unclear stays an
    uncertainty signal, never usable evidence).
    """
    lot_id = str(selected_lot_id)
    lot_entry = next(
        (L for L in lot_index.get("lots", []) if str(L.get("lot_id")) == lot_id), {}
    )
    return {
        "lot_ids": [str(x) for x in lot_report.get("lot_ids", [])],
        "selected_lot": lot_id,
        "lot_pages": segmentation.get("lot_pages", {}).get(lot_id, []),
        "global_pages": segmentation.get("global_pages", []),
        "excluded_shared_pages": segmentation.get("shared_pages", []),
        "bene_ids": list(lot_entry.get("bene_ids") or []),
        "compliance_sections": [
            c
            for c in lot_index.get("compliance_sections", [])
            if c.get("scope") in ("global", "unclear") or str(c.get("lot_id")) == lot_id
        ],
    }


def build_selected_lot_context(
    pages: Optional[List[Dict[str, Any]]],
    segmentation: Dict[str, Any],
    lot_id: str,
    lot_index: Optional[Dict[str, Any]] = None,
    worksheet: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build ``selected_lot_context.json`` for the chosen lot.

    Records exactly which pages will be (re)analyzed for the selected lot and why,
    so the resulting single-lot contract is fully traceable to a safe page subset.
    Includes ONLY the selected lot's money plus clearly global/common money — never
    another lot's money (req: per-lot money isolation).
    """
    lot_id = str(lot_id)
    selected_pages = select_lot_pages(pages, segmentation, lot_id)
    selected_page_numbers = [
        _page_number(p, i) for i, p in enumerate(selected_pages, start=1)
    ]
    lot_summary = None
    if lot_index:
        lot_summary = next((L for L in lot_index.get("lots", []) if str(L.get("lot_id")) == lot_id), None)

    lot_money = _empty_lot_money()
    global_money = _empty_lot_money()
    if worksheet is not None:
        money = build_lot_money(worksheet, segmentation, pages)
        lot_money = money["by_lot"].get(lot_id, _empty_lot_money())
        global_money = money["global"]

    return {
        "schema_version": SELECTED_LOT_CONTEXT_SCHEMA_VERSION,
        "selected_lot_id": lot_id,
        "analysis_pages": selected_page_numbers,
        "global_pages": segmentation.get("global_pages", []),
        "lot_specific_pages": segmentation.get("lot_pages", {}).get(lot_id, []),
        "excluded_shared_pages": segmentation.get("shared_pages", []),
        "lot_summary": lot_summary,
        # Money limited to THIS lot + global/common; other lots' money is excluded.
        "lot_money": lot_money,
        "global_money": global_money,
        "note": (
            "Contesto isolato per il lotto selezionato: solo pagine globali + pagine "
            "del lotto. Le pagine multi-lotto condivise sono escluse per evitare "
            "contaminazione tra lotti. La sezione money contiene solo importi del "
            "lotto selezionato e importi chiaramente globali/comuni."
        ),
    }


def _dedup_int(seq: List[int]) -> List[int]:
    seen: Dict[int, None] = {}
    for s in seq:
        try:
            seen.setdefault(int(s), None)
        except Exception:
            continue
    return list(seen.keys())
