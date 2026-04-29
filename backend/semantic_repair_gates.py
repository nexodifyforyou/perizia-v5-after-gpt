"""
semantic_repair_gates.py
------------------------
Repair-first deterministic reconciliation for customer-facing money and asset
inventory projections.

These gates intentionally run late in the backend pipeline, after the initial
customer contract is built and before the final consistency sweep mirrors root
sections into the customer_decision_contract.
"""
from __future__ import annotations

import copy
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


_EXPLICIT_EURO_RE = re.compile(
    r"(?i)(?:"
    r"(?:€\.?\s*|(?:euro|eur)\s+)"
    r"(?P<prefix>[0-9]{1,3}(?:[.\s][0-9]{3})*(?:,[0-9]{1,2})?|[0-9]{4,}(?:,[0-9]{1,2})?|[0-9]{1,3}(?:,[0-9]{1,2})?)"
    r"|(?P<suffix>[0-9]{1,3}(?:[.\s][0-9]{3})*(?:,[0-9]{1,2})?|[0-9]{4,}(?:,[0-9]{1,2})?|[0-9]{1,3}(?:,[0-9]{1,2})?)"
    r"\s*(?:€|(?:euro|eur)\b)"
    r")",
    re.I,
)

_VISIBLE_EURO_RE = re.compile(
    r"(?i)(?:"
    r"€\.?\s*(?P<prefix>[0-9]{1,3}(?:[.\s][0-9]{3})*(?:,[0-9]{1,2})?|[0-9]{1,3}(?:,[0-9]{1,2})?|[0-9]{4,}(?:,[0-9]{1,2})?)"
    r"|(?P<suffix>[0-9]{1,3}(?:[.\s][0-9]{3})*(?:,[0-9]{1,2})?|[0-9]{1,3}(?:,[0-9]{1,2})?|[0-9]{4,}(?:,[0-9]{1,2})?)\s*€"
    r"|\b(?:euro|eur)\s+(?P<word>[0-9]{1,3}(?:[.\s][0-9]{3})*(?:,[0-9]{1,2})?|[0-9]{1,3}(?:,[0-9]{1,2})?|[0-9]{4,}(?:,[0-9]{1,2})?)"
    r")",
    re.I,
)

_LABEL_AMOUNT_TAIL_RE = re.compile(
    r"(?i)\s*(?:[:\-–]\s*)?"
    r"(?:(?:€\.?\s*[0-9][0-9.\s]*(?:,[0-9]{1,2})?)|(?:[0-9][0-9.\s]*(?:,[0-9]{1,2})?\s*€)|(?:\b(?:euro|eur)\s+[0-9][0-9.\s]*(?:,[0-9]{1,2})?))"
    r"\s*$"
)

_IDENTIFIER_CONTEXT_RE = re.compile(
    r"(?i)"
    r"\b(?:sub\.?|subaltern[oi]|fg\.?|foglio|part\.?|particella|mapp\.?|mappale|"
    r"lotto|bene\s*(?:n\.?|n[°º])?|pag\.?|pagina|pagg\.?|"
    r"cat(?:egoria)?\.?|classe|consistenza|scheda|protocollo|n\.?\s*)\b"
)

_CATASTAL_CATEGORY_RE = re.compile(r"(?i)\b[A-Z]\s*/\s*\d+\b")
_DATE_LIKE_RE = re.compile(r"\b\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\b")
_COST_CONTEXT_RE = re.compile(
    r"(?i)\b(?:regolarizzazione|sanatoria|condono|oblazione|urbanistic|catastal|"
    r"oneri|spese|costi|costo|ripristino|demolizione|completamento|agibilit|"
    r"abitabilit|conformit|impianti|ape|deprezz|detrazion|rischio|garanzia)\b"
)
_VALUATION_BLOCKER_RE = re.compile(
    r"(?i)\b(?:prezzo\s+base|offerta\s+minima|valore\s+di\s+mercato|valore\s+commerciale|"
    r"rendita\s+catastale|capitale|ipoteca|trascrizione|formalita)\b"
)

_MONEY_LIST_KEYS = (
    "items",
    "cost_signals_to_verify",
    "valuation_deductions",
    "qualitative_burdens",
)
_MONEY_BOX_KEYS = ("money_box", "section_3_money_box")
_CUSTOMER_MONEY_LABEL_FIELDS = (
    "label_it",
    "label_en",
    "label",
    "title",
    "title_it",
    "title_en",
    "headline_it",
    "headline_en",
    "display_label",
)
_CUSTOMER_MONEY_LABEL_KEY_RE = re.compile(r"(?i)(?:label|title|headline|display)")
_CUSTOMER_MONEY_LABEL_EXCLUDE_RE = re.compile(r"(?i)(?:evidence|quote|source|fonte)")

_MONEY_MANUAL_HINTS = [
    "Verificare la sezione regolarità urbanistica/conformità.",
    "Verificare eventuale tabella di stima/deprezzamenti.",
    "Non usare numeri catastali, subalterni, fogli, particelle, date o numeri pagina come importi.",
]

_ASSET_MANUAL_HINTS = [
    "Verificare indice iniziale della perizia.",
    "Verificare sezioni Lotto/Bene.",
    "Verificare tabella di stima finale.",
    "Controllare se garage/cantina sono beni separati o pertinenze dello stesso lotto.",
]

_ASSET_TYPE_PATTERNS: Tuple[Tuple[str, re.Pattern], ...] = (
    ("appartamento", re.compile(r"(?i)\bappartament[oi]\b|\babitazion[ei]\b|\balloggio\b")),
    ("ufficio", re.compile(r"(?i)\buffici[oi]\b")),
    ("garage", re.compile(r"(?i)\bgarage\b|\bautorimess[ae]\b")),
    ("box", re.compile(r"(?i)\bbox\b")),
    ("cantina", re.compile(r"(?i)\bcantin[ae]\b")),
    ("terreno", re.compile(r"(?i)\bterren[oi]\b")),
    ("locale", re.compile(r"(?i)\blocal[ei]\b")),
    ("magazzino", re.compile(r"(?i)\bmagazzin[oi]\b|\bdeposit[oi]\b")),
    ("negozio", re.compile(r"(?i)\bnegozi[oi]\b|\bbotteg[ae]\b")),
    ("fabbricato", re.compile(r"(?i)\bfabbricat[oi]\b|\bvillett[ae]\b|\bvillino\b")),
)

_LOT_RE = re.compile(
    r"(?i)\b(?:lotto\s*(?:n\.?|n[°º])?\s*(?P<num>\d+)|(?P<unico>lotto\s+unico))\b"
)
_BENE_RE = re.compile(r"(?i)\bbene\s*(?:n\.?|n[°º])?\s*(?P<num>\d+)\b")
_SURFACE_RE = re.compile(r"(?i)\b(?:superficie|sup\.?|mq|m²)\D{0,24}(?P<mq>\d{1,4}(?:[,.]\d{1,2})?)")
_CATASTO_RE = re.compile(
    r"(?i)\b(?:foglio|fg\.?)\s*[\w./-]+.{0,80}?(?:particella|part\.?|mappale|mapp\.?)\s*[\w./-]+(?:.{0,80}?\bsub(?:alterno)?\.?\s*[\w./-]+)?"
)


def apply_semantic_repair_gates(
    result: Dict[str, Any],
    raw_text: Optional[Any] = None,
    page_map: Optional[Dict[int, str]] = None,
) -> Dict[str, Any]:
    """Run money and asset repair gates in-place and return Mongo-safe metadata."""
    pages = _coerce_page_map(page_map=page_map, raw_text=raw_text)
    money_meta = apply_money_semantic_repair_gate(result, pages)
    asset_meta = apply_asset_inventory_repair_gate(result, pages)
    return {
        "changed": bool(money_meta.get("changed") or asset_meta.get("changed")),
        "money": money_meta,
        "asset_inventory": asset_meta,
    }


# ---------------------------------------------------------------------------
# Money Semantic Repair Gate
# ---------------------------------------------------------------------------


def apply_money_semantic_repair_gate(result: Dict[str, Any], page_map: Dict[int, str]) -> Dict[str, Any]:
    meta: Dict[str, Any] = {
        "changed": False,
        "repairs": [],
        "fallbacks": [],
        "validated": 0,
    }
    repair_records: List[Dict[str, Any]] = []

    for box_key in _MONEY_BOX_KEYS:
        box = result.get(box_key)
        if not isinstance(box, dict):
            continue
        for list_key in _MONEY_LIST_KEYS:
            items = box.get(list_key)
            if not isinstance(items, list):
                continue
            for index, item in enumerate(items):
                if not isinstance(item, dict):
                    continue
                outcome = _repair_or_downgrade_money_item(item, page_map, box_key, list_key, index)
                if outcome.get("action") == "validated":
                    meta["validated"] += 1
                elif outcome.get("action") == "repaired":
                    meta["changed"] = True
                    meta["repairs"].append(_money_meta_summary(outcome))
                    repair_records.append(outcome)
                elif outcome.get("action") == "fallback":
                    meta["changed"] = True
                    meta["fallbacks"].append(_money_meta_summary(outcome))
                    repair_records.append(outcome)
        if _sanitize_money_totals(box, page_map, box_key, meta):
            meta["changed"] = True

    if repair_records:
        _rewrite_customer_money_texts(result, repair_records)
        _sync_money_box_aliases(result)
        _sync_money_display_labels_after_repair(result)
    if meta["changed"]:
        result["money_semantic_repair"] = {
            "status": "APPLIED",
            "repairs_count": len(meta["repairs"]),
            "fallbacks_count": len(meta["fallbacks"]),
        }
    return meta


def _repair_or_downgrade_money_item(
    item: Dict[str, Any],
    page_map: Dict[int, str],
    box_key: str,
    list_key: str,
    index: int,
) -> Dict[str, Any]:
    displayed = _extract_displayed_amount(item)
    if displayed is None:
        return {"action": "skip"}
    amount, amount_fields = displayed
    evidence = _normalize_evidence(item.get("evidence") or _nested_evidence(item))
    item_context = _item_context_text(item)
    search_terms = _money_search_terms(item_context)

    if _amount_is_explicitly_anchored(amount, evidence):
        item["amount_status"] = "ANCHORED_EXPLICIT_EURO"
        return {"action": "validated"}

    pages_to_search = _money_relevant_pages(page_map, evidence, search_terms)
    repair = _find_repair_amount(amount, page_map, evidence, pages_to_search, search_terms, item_context)
    old_amount = int(round(float(amount)))
    if repair is not None:
        repaired_amount = int(round(float(repair["amount"])))
        _apply_repaired_money_item(item, amount_fields, repaired_amount, repair)
        return {
            "action": "repaired",
            "box_key": box_key,
            "list_key": list_key,
            "index": index,
            "old_amount": old_amount,
            "new_amount": repaired_amount,
            "searched_pages": repair.get("searched_pages", []),
            "evidence": repair.get("evidence", []),
            "reason_it": item.get("reason_it"),
        }

    fallback = _money_fallback_context(page_map, evidence, pages_to_search, search_terms, item_context)
    _apply_money_fallback_item(item, amount_fields, fallback)
    return {
        "action": "fallback",
        "box_key": box_key,
        "list_key": list_key,
        "index": index,
        "old_amount": old_amount,
        "new_amount": None,
        "searched_pages": fallback.get("searched_pages", []),
        "evidence": fallback.get("evidence", []),
        "reason_it": item.get("reason_it"),
    }


def _extract_displayed_amount(item: Dict[str, Any]) -> Optional[Tuple[float, List[str]]]:
    fields: List[str] = []
    candidates: List[float] = []
    for field in ("amount_eur", "stima_euro"):
        value = item.get(field)
        if isinstance(value, (int, float)) and float(value) > 0:
            fields.append(field)
            candidates.append(float(value))
    for field in _money_customer_label_fields(item):
        value = item.get(field)
        if not isinstance(value, str):
            continue
        for match in _VISIBLE_EURO_RE.finditer(value):
            parsed = _parse_it_money(match.group("prefix") or match.group("suffix") or match.group("word"))
            if parsed is not None:
                fields.append(field)
                candidates.append(parsed)
                break
    if not candidates:
        return None
    return candidates[0], sorted(set(fields))


def _amount_is_explicitly_anchored(amount: float, evidence: Sequence[Dict[str, Any]]) -> bool:
    for ev in evidence:
        quote = str(ev.get("quote") or "")
        if not quote:
            continue
        for candidate in _explicit_euro_amounts(quote):
            if _money_amount_equal(amount, float(candidate["amount"])):
                return True
    return False


def _find_repair_amount(
    old_amount: float,
    page_map: Dict[int, str],
    evidence: Sequence[Dict[str, Any]],
    pages_to_search: Sequence[int],
    search_terms: Sequence[str],
    item_context: str,
) -> Optional[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    searched_pages: List[int] = []

    for ev in evidence:
        quote = str(ev.get("quote") or "")
        page = _safe_int(ev.get("page"))
        if page is not None and page not in searched_pages:
            searched_pages.append(page)
        if quote:
            candidates.extend(_score_explicit_amounts_in_text(quote, page, search_terms, old_amount, item_context))

    for page in pages_to_search:
        text = page_map.get(page) or ""
        if not text:
            continue
        if page not in searched_pages:
            searched_pages.append(page)
        for snippet in _money_context_snippets(text, search_terms):
            candidates.extend(_score_explicit_amounts_in_text(snippet, page, search_terms, old_amount, item_context))

    candidates = [c for c in candidates if c.get("amount") and not _money_amount_equal(old_amount, float(c["amount"]))]
    if not candidates:
        return None
    candidates.sort(key=lambda c: (-int(c.get("score") or 0), len(str(c.get("quote") or "")), int(c.get("page") or 9999)))
    best = candidates[0]
    best["searched_pages"] = sorted({p for p in searched_pages if isinstance(p, int)})
    best["evidence"] = [{"page": best.get("page"), "quote": best.get("quote")}]
    return best


def _score_explicit_amounts_in_text(
    text: str,
    page: Optional[int],
    search_terms: Sequence[str],
    old_amount: float,
    item_context: str,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not text:
        return out
    normalized = _text_key(text)
    old_token_re = re.compile(rf"(?<!\d){re.escape(str(int(round(float(old_amount)))))}(?!\d)")
    old_positions = [m.start() for m in old_token_re.finditer(text)]
    for candidate in _explicit_euro_amounts(text):
        start = int(candidate["start"])
        amount = float(candidate["amount"])
        score = 1
        around = text[max(0, start - 140): min(len(text), start + 140)]
        around_key = _text_key(around)
        if _COST_CONTEXT_RE.search(around):
            score += 4
        if any(term in normalized or term in around_key for term in search_terms):
            score += 4
        if _text_key(item_context) and any(term in around_key for term in _money_search_terms(item_context)):
            score += 2
        if old_positions and any(0 <= start - pos <= 180 for pos in old_positions):
            score += 3
        if _VALUATION_BLOCKER_RE.search(around) and not _has_valuation_money_context(item_context):
            score -= 3
        out.append(
            {
                "amount": amount,
                "page": page,
                "quote": _compact_quote(text),
                "raw": candidate.get("raw"),
                "score": score,
            }
        )
    return out


def _apply_repaired_money_item(
    item: Dict[str, Any],
    amount_fields: Sequence[str],
    repaired_amount: int,
    repair: Dict[str, Any],
) -> None:
    base_it, base_en = _money_item_label_base(item)
    item["amount_eur"] = repaired_amount
    if "stima_euro" in amount_fields:
        item["stima_euro"] = repaired_amount
    else:
        item["stima_euro"] = item.get("stima_euro") if isinstance(item.get("stima_euro"), (int, float)) else None
    item["label_it"] = f"{base_it}: {_format_euro_it(repaired_amount)}"
    item["label_en"] = f"{base_en}: {_format_euro_it(repaired_amount)}"
    item["classification"] = item.get("classification") or "cost_signal_to_verify"
    item["amount_status"] = "ANCHORED_EXPLICIT_EURO"
    item["reason_it"] = (
        "Importo riparato perché il valore precedente derivava da un identificativo "
        "catastale/subalterno; l'importo euro corretto è presente nella stessa citazione."
    )
    item["searched_pages"] = sorted({p for p in repair.get("searched_pages", []) if isinstance(p, int)})
    item["evidence"] = _normalize_evidence(repair.get("evidence"))
    item["fonte_perizia"] = {"value": "Perizia", "evidence": copy.deepcopy(item["evidence"])}
    item["note_it"] = item.get("note_it") or item["reason_it"]
    _normalize_money_item_display_labels(item, base_it=base_it, base_en=base_en)


def _apply_money_fallback_item(
    item: Dict[str, Any],
    amount_fields: Sequence[str],
    fallback: Dict[str, Any],
) -> None:
    base_it, base_en = _money_item_label_base(item)
    item["label_it"] = base_it
    item["label_en"] = base_en
    item["classification"] = item.get("classification") or "cost_signal_to_verify"
    item["amount_eur"] = None
    item["stima_euro"] = None
    for field in amount_fields:
        if field not in ("amount_eur", "stima_euro"):
            item[field] = _strip_money_amount_from_label(item.get(field))
    item["amount_status"] = "NON_QUANTIFICATO_IN_MODO_DIFENDIBILE"
    item["reason_it"] = (
        "Trovato contesto di regolarizzazione, ma nessun importo euro affidabile è stato "
        "ancorato dopo la verifica."
    )
    item["searched_pages"] = fallback.get("searched_pages", [])
    item["manual_check_hint_it"] = list(_MONEY_MANUAL_HINTS)
    item["evidence"] = _normalize_evidence(fallback.get("evidence"))
    item["fonte_perizia"] = {"value": "Perizia", "evidence": copy.deepcopy(item["evidence"])}
    item["note_it"] = "Importo non quantificato in modo difendibile."
    item["stima_nota"] = item["note_it"]
    _normalize_money_item_display_labels(item, base_it=base_it, base_en=base_en)


def _money_fallback_context(
    page_map: Dict[int, str],
    evidence: Sequence[Dict[str, Any]],
    pages_to_search: Sequence[int],
    search_terms: Sequence[str],
    item_context: str,
) -> Dict[str, Any]:
    searched = sorted({p for p in pages_to_search if isinstance(p, int)})
    fallback_evidence = _normalize_evidence(evidence, limit=2)
    if not fallback_evidence:
        for page in searched:
            text = page_map.get(page) or ""
            snippets = _money_context_snippets(text, search_terms or _money_search_terms(item_context))
            for snippet in snippets[:2]:
                fallback_evidence.append({"page": page, "quote": _compact_quote(snippet)})
            if fallback_evidence:
                break
    return {
        "searched_pages": searched,
        "evidence": fallback_evidence,
    }


def _sanitize_money_totals(
    box: Dict[str, Any],
    page_map: Dict[int, str],
    box_key: str,
    meta: Dict[str, Any],
) -> bool:
    changed = False
    for total_key in ("total_extra_costs", "totale_extra_budget"):
        total = box.get(total_key)
        if not isinstance(total, dict):
            continue
        values = []
        for field in ("min", "max"):
            value = total.get(field)
            if isinstance(value, (int, float)) and float(value) > 0:
                values.append(float(value))
        value_range = total.get("range")
        if isinstance(value_range, dict):
            for field in ("min", "max"):
                value = value_range.get(field)
                if isinstance(value, (int, float)) and float(value) > 0:
                    values.append(float(value))
        if not values:
            continue
        evidence = _normalize_evidence(total.get("evidence"))
        if all(_amount_is_explicitly_anchored(value, evidence) for value in values):
            continue
        for field in ("min", "max"):
            if isinstance(total.get(field), (int, float)):
                total[field] = None
        if isinstance(value_range, dict):
            value_range["min"] = None
            value_range["max"] = None
        total["note"] = (
            "Totale extra non quantificato in modo difendibile: nessun importo cliente "
            "può restare visibile senza pagina, citazione e importo euro esplicito."
        )
        total["amount_status"] = "NON_QUANTIFICATO_IN_MODO_DIFENDIBILE"
        total["searched_pages"] = _money_relevant_pages(page_map, evidence, ["costi", "spese", "oneri"])[:8]
        changed = True
        meta["fallbacks"].append(
            {
                "box_key": box_key,
                "list_key": total_key,
                "old_amount": int(round(values[0])),
                "new_amount": None,
                "searched_pages": total.get("searched_pages", []),
                "reason_it": total["note"],
            }
        )
    return changed


def _rewrite_customer_money_texts(result: Dict[str, Any], repair_records: Sequence[Dict[str, Any]]) -> None:
    keys = (
        "issues",
        "red_flags_operativi",
        "section_11_red_flags",
        "summary_for_client",
        "summary_for_client_bundle",
        "section_2_decisione_rapida",
        "decision_rapida_client",
        "semaforo_generale",
        "section_1_semaforo_generale",
        "section_9_legal_killers",
    )

    def rewrite_text(text: str) -> str:
        out = text
        if not _COST_CONTEXT_RE.search(out):
            return out
        for rec in repair_records:
            old_amount = rec.get("old_amount")
            if not isinstance(old_amount, int):
                continue
            old_re = re.compile(rf"(?i)(?:€\.?\s*{old_amount}\b|{old_amount}\s*€|euro\s+{old_amount}\b)")
            if rec.get("new_amount") is None:
                out = old_re.sub("importo non quantificato in modo difendibile", out)
            else:
                out = old_re.sub(_format_euro_it(int(rec["new_amount"])), out)
        return out

    def walk(value: Any) -> Any:
        if isinstance(value, str):
            return rewrite_text(value)
        if isinstance(value, list):
            return [walk(v) for v in value]
        if isinstance(value, dict):
            return {k: walk(v) for k, v in value.items()}
        return value

    for key in keys:
        if key in result:
            result[key] = walk(result[key])


def _sync_money_box_aliases(result: Dict[str, Any]) -> None:
    mb = result.get("money_box")
    s3 = result.get("section_3_money_box")
    if isinstance(s3, dict) and isinstance(mb, dict):
        # Keep both public aliases internally consistent while preserving any section-specific keys.
        for list_key in _MONEY_LIST_KEYS + ("total_extra_costs", "totale_extra_budget"):
            if list_key in s3:
                mb[list_key] = copy.deepcopy(s3[list_key])
            elif list_key in mb:
                s3[list_key] = copy.deepcopy(mb[list_key])


# ---------------------------------------------------------------------------
# Asset Inventory Repair Gate
# ---------------------------------------------------------------------------


def apply_asset_inventory_repair_gate(result: Dict[str, Any], page_map: Dict[int, str]) -> Dict[str, Any]:
    inventory = _build_source_asset_inventory(page_map)
    meta: Dict[str, Any] = {
        "changed": False,
        "detected_lotto_count": len(inventory.get("lot_numbers") or []),
        "detected_bene_count": len(inventory.get("bene_candidates") or []),
        "likely_multi_lot": bool(inventory.get("likely_multi_lot")),
        "likely_multi_bene": bool(inventory.get("likely_multi_bene")),
        "status": "NO_ACTION",
    }
    if not page_map:
        return meta

    if inventory.get("ambiguous"):
        _apply_asset_inventory_fallback(result, inventory)
        meta["changed"] = True
        meta["status"] = "FALLBACK_AMBIGUOUS"
        return meta

    if _needs_multi_lot_repair(result, inventory):
        _apply_multi_lot_repair(result, inventory)
        meta["changed"] = True
        meta["status"] = "REPAIRED_MULTI_LOT"
    elif _needs_single_lot_asset_repair(result, inventory):
        _apply_single_lot_asset_repair(result, inventory)
        meta["changed"] = True
        meta["status"] = "REPAIRED_SINGLE_LOT_ASSETS"

    if meta["changed"]:
        result["asset_inventory_repair"] = {
            "asset_inventory_status": meta["status"],
            "searched_pages": inventory.get("searched_pages", []),
            "detected_candidates": inventory.get("detected_candidates", [])[:12],
        }
    return meta


def _build_source_asset_inventory(page_map: Dict[int, str]) -> Dict[str, Any]:
    lot_candidates: List[Dict[str, Any]] = []
    bene_candidates: List[Dict[str, Any]] = []
    detected_candidates: List[Dict[str, Any]] = []
    searched_pages: List[int] = []
    current_lot: Optional[int] = None
    has_lotto_unico = False

    for page in sorted(page_map):
        text = page_map.get(page) or ""
        if not text.strip():
            continue
        searched_pages.append(page)
        lines = _meaningful_lines(text)
        for line_index, line in enumerate(lines):
            line_quote = _line_context(lines, line_index)
            lot_match = _LOT_RE.search(line)
            if lot_match:
                if lot_match.group("unico"):
                    has_lotto_unico = True
                    detected_candidates.append({"page": page, "quote": _compact_quote(line_quote)})
                else:
                    lot_num = _safe_int(lot_match.group("num"))
                    if lot_num is not None:
                        current_lot = lot_num
                        candidate = {"lot_number": lot_num, "page": page, "quote": _compact_quote(line_quote)}
                        lot_candidates.append(candidate)
                        detected_candidates.append({"page": page, "quote": candidate["quote"]})
            bene_match = _BENE_RE.search(line)
            asset_types = _asset_types_in_text(line)
            if bene_match or asset_types:
                if not (bene_match or _line_has_asset_inventory_signal(line)):
                    continue
                bene_num = _safe_int(bene_match.group("num")) if bene_match else None
                lot_num = current_lot or _lot_for_page(lot_candidates, page)
                components = asset_types or _asset_types_in_text(line_quote)
                if not components:
                    continue
                candidate = {
                    "lot_number": lot_num,
                    "bene_number": bene_num or _next_bene_number_for_lot(bene_candidates, lot_num),
                    "asset_type": components[0],
                    "asset_components": components,
                    "page": page,
                    "quote": _compact_quote(line_quote),
                    "superficie_mq": _extract_surface(line_quote),
                    "catasto": _extract_catasto(line_quote),
                    "stima_euro": _extract_first_explicit_euro(line_quote),
                }
                bene_candidates.append(candidate)
                detected_candidates.append({"page": page, "quote": candidate["quote"]})

    lot_numbers = sorted({c["lot_number"] for c in lot_candidates if isinstance(c.get("lot_number"), int)})
    asset_types = sorted({t for c in bene_candidates for t in (c.get("asset_components") or [])})
    return {
        "searched_pages": searched_pages,
        "lot_candidates": _dedup_asset_candidates(lot_candidates, ("lot_number", "page")),
        "bene_candidates": _dedup_bene_candidates(bene_candidates),
        "detected_candidates": _dedup_asset_candidates(detected_candidates, ("page", "quote")),
        "lot_numbers": lot_numbers,
        "asset_types": asset_types,
        "has_lotto_unico": has_lotto_unico,
        "likely_multi_lot": len(lot_numbers) >= 2,
        "likely_multi_bene": len(bene_candidates) >= 2 or len(asset_types) >= 2,
        "ambiguous": bool(has_lotto_unico and len(lot_numbers) >= 2),
    }


def _needs_multi_lot_repair(result: Dict[str, Any], inventory: Dict[str, Any]) -> bool:
    lot_numbers = inventory.get("lot_numbers") or []
    if len(lot_numbers) < 2:
        return False
    current_count = _current_lots_count(result)
    headers_text = _text_key(
        " ".join(
            str(v)
            for v in (
                result.get("detail_scope"),
                result.get("case_header"),
                result.get("report_header"),
            )
        )
    )
    if current_count < len(lot_numbers):
        return True
    if "lotto unico" in headers_text:
        return True
    current_types = _current_asset_types(result)
    source_types = set(inventory.get("asset_types") or [])
    return bool(source_types and not source_types.issubset(current_types))


def _needs_single_lot_asset_repair(result: Dict[str, Any], inventory: Dict[str, Any]) -> bool:
    if inventory.get("likely_multi_lot"):
        return False
    source_types = set(inventory.get("asset_types") or [])
    if len(source_types) < 2:
        return False
    current_types = _current_asset_types(result)
    return not source_types.issubset(current_types)


def _apply_multi_lot_repair(result: Dict[str, Any], inventory: Dict[str, Any]) -> None:
    existing_by_lot = _existing_lots_by_number(result.get("lots"))
    bene_by_lot = _source_beni_by_lot(inventory)
    repaired_lots: List[Dict[str, Any]] = []
    repaired_beni: List[Dict[str, Any]] = []

    for lot_num in inventory.get("lot_numbers") or []:
        lot = copy.deepcopy(existing_by_lot.get(lot_num) or {})
        lot["lot_number"] = lot_num
        lot["lot"] = lot_num
        lot["lot_id"] = lot.get("lot_id") or f"lotto_{lot_num}"
        lot["titolo"] = lot.get("titolo") or f"Lotto {lot_num}"
        lot["detail_scope"] = "LOT_FIRST"
        source_beni = bene_by_lot.get(lot_num) or []
        if source_beni:
            lot_beni = [_source_bene_to_result_bene(bene, lot_num) for bene in source_beni]
            lot["beni"] = lot_beni
            repaired_beni.extend(copy.deepcopy(lot_beni))
            lot["tipologia"] = _asset_type_label([t for bene in source_beni for t in (bene.get("asset_components") or [])])
        else:
            lot["beni"] = lot.get("beni") if isinstance(lot.get("beni"), list) else []
        lot["asset_inventory_status"] = "RICOSTRUITO_DA_TESTO"
        lot["evidence"] = {"lotto": _lot_evidence_for_number(inventory, lot_num)}
        repaired_lots.append(lot)

    result["lots"] = repaired_lots
    result["beni"] = repaired_beni
    result["lot_index"] = [_lot_index_entry(lot) for lot in repaired_lots]
    result["lots_count"] = len(repaired_lots)
    result["is_multi_lot"] = len(repaired_lots) > 1
    result["detail_scope"] = "LOT_FIRST" if len(repaired_lots) > 1 else "BENE_FIRST"
    _sync_asset_headers(result, len(repaired_lots))


def _apply_single_lot_asset_repair(result: Dict[str, Any], inventory: Dict[str, Any]) -> None:
    lot_num = 1
    existing_lots = result.get("lots") if isinstance(result.get("lots"), list) else []
    lot = copy.deepcopy(existing_lots[0]) if existing_lots and isinstance(existing_lots[0], dict) else {}
    lot["lot_number"] = lot.get("lot_number") or lot.get("lot") or lot_num
    lot["lot"] = lot.get("lot") or lot_num
    lot["lot_id"] = lot.get("lot_id") or "lotto_1"
    lot["titolo"] = lot.get("titolo") or "Lotto unico"
    lot["detail_scope"] = "BENE_FIRST"

    source_beni = inventory.get("bene_candidates") or []
    repaired_beni = [_source_bene_to_result_bene(bene, int(lot["lot_number"])) for bene in source_beni]
    current_beni = result.get("beni") if isinstance(result.get("beni"), list) else []
    merged = _merge_beni_by_asset_type(current_beni, repaired_beni)
    lot["beni"] = merged
    lot["tipologia"] = _asset_type_label([t for bene in source_beni for t in (bene.get("asset_components") or [])])
    lot["asset_inventory_status"] = "RICOSTRUITO_DA_TESTO"

    result["lots"] = [lot]
    result["beni"] = merged
    result["lot_index"] = [_lot_index_entry(lot)]
    result["lots_count"] = 1
    result["is_multi_lot"] = False
    result["detail_scope"] = "BENE_FIRST"
    _sync_asset_headers(result, 1)


def _apply_asset_inventory_fallback(result: Dict[str, Any], inventory: Dict[str, Any]) -> None:
    fallback = {
        "asset_inventory_status": "NON_RISOLTO_IN_MODO_DIFENDIBILE",
        "reason_it": (
            "La struttura Lotto/Bene risulta incoerente o non ricostruibile con sicurezza "
            "dopo la verifica del testo estratto/OCR."
        ),
        "searched_pages": inventory.get("searched_pages", []),
        "detected_candidates": inventory.get("detected_candidates", [])[:12],
        "manual_check_hint_it": list(_ASSET_MANUAL_HINTS),
    }
    result["asset_inventory_repair"] = fallback


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _coerce_page_map(page_map: Optional[Dict[int, str]], raw_text: Optional[Any]) -> Dict[int, str]:
    if isinstance(page_map, dict) and page_map:
        out: Dict[int, str] = {}
        for key, value in page_map.items():
            page = _safe_int(key)
            if page is not None and isinstance(value, str) and value.strip():
                out[page] = value
        if out:
            return dict(sorted(out.items()))
    if isinstance(raw_text, list):
        out = {}
        for idx, item in enumerate(raw_text):
            if isinstance(item, str):
                out[idx + 1] = item
            elif isinstance(item, dict):
                page = _safe_int(item.get("page") or item.get("page_number") or idx + 1)
                text = item.get("text") or item.get("content") or item.get("quote")
                if page is not None and isinstance(text, str):
                    out[page] = text
        return dict(sorted(out.items()))
    if isinstance(raw_text, str) and raw_text.strip():
        if "\f" in raw_text:
            return {idx + 1: part for idx, part in enumerate(raw_text.split("\f")) if part.strip()}
        return {1: raw_text}
    return {}


def _parse_it_money(value: Any) -> Optional[float]:
    text = str(value or "").strip()
    if not text:
        return None
    cleaned = re.sub(r"[^\d,\.]", "", text)
    if not cleaned:
        return None
    if "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(" ", "").replace(",", ".")
    elif cleaned.count(".") > 1:
        cleaned = cleaned.replace(".", "")
    else:
        cleaned = cleaned.replace(" ", "")
    try:
        amount = float(cleaned)
    except Exception:
        return None
    if amount <= 0:
        return None
    return amount


def _explicit_euro_amounts(text: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for match in _EXPLICIT_EURO_RE.finditer(text or ""):
        raw_amount = match.group("prefix") or match.group("suffix")
        amount = _parse_it_money(raw_amount)
        if amount is None:
            continue
        out.append(
            {
                "amount": amount,
                "raw": match.group(0),
                "start": match.start(),
                "end": match.end(),
            }
        )
    return out


def _extract_first_explicit_euro(text: str) -> Optional[int]:
    amounts = _explicit_euro_amounts(text)
    if not amounts:
        return None
    return int(round(float(amounts[0]["amount"])))


def _money_amount_equal(left: float, right: float) -> bool:
    if abs(float(left) - float(right)) <= 0.01:
        return True
    return int(round(float(left))) == int(round(float(right)))


def _format_euro_it(amount: int) -> str:
    return f"€ {int(round(float(amount))):,}".replace(",", ".")


def _normalize_evidence(value: Any, limit: int = 4) -> List[Dict[str, Any]]:
    if not isinstance(value, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        page = _safe_int(item.get("page"))
        quote = str(item.get("quote") or item.get("text") or "").strip()
        if page is None or not quote:
            continue
        out.append({"page": page, "quote": _compact_quote(quote)})
        if len(out) >= limit:
            break
    return out


def _nested_evidence(item: Dict[str, Any]) -> Any:
    fonte = item.get("fonte_perizia")
    if isinstance(fonte, dict):
        return fonte.get("evidence")
    return None


def _money_relevant_pages(
    page_map: Dict[int, str],
    evidence: Sequence[Dict[str, Any]],
    search_terms: Sequence[str],
    limit: int = 12,
) -> List[int]:
    pages: List[int] = []
    for ev in evidence:
        page = _safe_int(ev.get("page"))
        if page is not None and page in page_map and page not in pages:
            pages.append(page)
    terms = [term for term in search_terms if term]
    for page, text in page_map.items():
        if page in pages:
            continue
        key = _text_key(text)
        if any(term in key for term in terms) or _COST_CONTEXT_RE.search(text or ""):
            pages.append(page)
        if len(pages) >= limit:
            break
    return pages


def _money_context_snippets(text: str, search_terms: Sequence[str], limit: int = 6) -> List[str]:
    if not text:
        return []
    snippets: List[str] = []
    key = _text_key(text)
    terms = [term for term in search_terms if term]
    for term in terms:
        start = key.find(term)
        if start >= 0:
            approx = max(0, min(len(text), start))
            snippets.append(text[max(0, approx - 220): min(len(text), approx + 360)])
    if not snippets and _COST_CONTEXT_RE.search(text):
        for match in _COST_CONTEXT_RE.finditer(text):
            snippets.append(text[max(0, match.start() - 180): min(len(text), match.end() + 360)])
            if len(snippets) >= limit:
                break
    return [_compact_quote(s, limit=520) for s in snippets[:limit]]


def _money_search_terms(text: str) -> List[str]:
    normalized = _text_key(text)
    terms: List[str] = []
    term_map = (
        ("regolarizzazione", ("regolarizzazione", "urbanistic", "sanatoria")),
        ("sanatoria", ("sanatoria", "condono", "oblazione")),
        ("spese", ("spese", "costi", "oneri")),
        ("condominiali", ("condominiali",)),
        ("deprezz", ("deprezz", "detrazion", "rischio", "garanzia")),
        ("agibilita", ("agibilita", "abitabilita")),
        ("impianti", ("impianti", "conformita")),
        ("ape", ("ape", "prestazione energetica")),
    )
    for trigger, values in term_map:
        if trigger in normalized:
            terms.extend(values)
    if not terms:
        for token in re.findall(r"[a-zA-Zàèéìòù]{5,}", text.lower()):
            clean = _text_key(token)
            if clean and clean not in terms:
                terms.append(clean)
            if len(terms) >= 4:
                break
    return sorted(set(terms))


def _item_context_text(item: Dict[str, Any]) -> str:
    pieces: List[str] = []
    for key in tuple(_CUSTOMER_MONEY_LABEL_FIELDS) + ("classification", "type", "code", "note_it", "stima_nota"):
        value = item.get(key)
        if isinstance(value, str):
            pieces.append(value)
    for ev in _normalize_evidence(item.get("evidence") or _nested_evidence(item), limit=3):
        pieces.append(ev.get("quote") or "")
    return " ".join(pieces)


def _money_customer_label_fields(item: Dict[str, Any]) -> List[str]:
    fields: List[str] = []
    for field in _CUSTOMER_MONEY_LABEL_FIELDS:
        if field in item and field not in fields:
            fields.append(field)
    for key, value in item.items():
        if not isinstance(key, str) or key in fields or not isinstance(value, str):
            continue
        if _CUSTOMER_MONEY_LABEL_EXCLUDE_RE.search(key):
            continue
        if _CUSTOMER_MONEY_LABEL_KEY_RE.search(key):
            fields.append(key)
    return fields


def _money_item_label_base(item: Dict[str, Any]) -> Tuple[str, str]:
    context_key = _text_key(_item_context_text(item))
    classification_key = _text_key(item.get("classification"))
    code_key = _text_key(item.get("code"))
    joined = " ".join(part for part in (context_key, classification_key, code_key) if part)

    if any(term in joined for term in ("regolar", "urbanistic", "sanatoria", "condono", "oblazione")):
        return "Regolarizzazione urbanistica", "Urban regularization"
    if any(term in joined for term in ("condomin", "arretrat")):
        return "Spese condominiali da verificare", "Condominium costs to verify"
    if any(term in joined for term in ("agibil", "abitabil")):
        return "Pratiche agibilita/abitabilita da verificare", "Habitability certification costs to verify"
    if any(term in joined for term in ("impiant", "ape", "energetic")):
        return "Conformita impianti/APE da verificare", "Systems/energy compliance costs to verify"
    if any(term in joined for term in ("deprezz", "detrazion", "garanzia", "rischio")):
        return "Detrazione/deprezzamento da verificare", "Valuation deduction to verify"
    if any(term in joined for term in ("tecnic", "istruttor")):
        return "Oneri tecnici da verificare", "Technical fees to verify"
    if any(term in joined for term in ("liberaz", "occupaz")):
        return "Costo liberazione da verificare", "Liberation cost to verify"
    return "Segnale economico da verificare", "Economic signal to verify"


def _valid_money_amount(value: Any) -> Optional[int]:
    if isinstance(value, (int, float)) and float(value) > 0:
        return int(round(float(value)))
    return None


def _money_label_for_field(field: str, label_it: str, label_en: str) -> str:
    key = str(field or "").lower()
    if key.endswith("_en") or key in ("label_en", "title_en", "headline_en"):
        return label_en
    return label_it


def _normalize_money_item_display_labels(
    item: Dict[str, Any],
    base_it: Optional[str] = None,
    base_en: Optional[str] = None,
) -> None:
    if base_it is None or base_en is None:
        base_it, base_en = _money_item_label_base(item)

    status = str(item.get("amount_status") or "")
    amount = _valid_money_amount(item.get("amount_eur"))
    if status == "ANCHORED_EXPLICIT_EURO" and amount is not None:
        formatted = _format_euro_it(amount)
        label_it = f"{base_it}: {formatted}"
        label_en = f"{base_en}: {formatted}"
    elif status == "NON_QUANTIFICATO_IN_MODO_DIFENDIBILE" or ("amount_eur" in item and item.get("amount_eur") is None):
        label_it = f"{base_it}: importo non quantificato in modo difendibile"
        label_en = f"{base_en}: amount not defensibly quantified"
    else:
        return

    item["label_it"] = label_it
    item["label_en"] = label_en
    for field in _money_customer_label_fields(item):
        if not isinstance(item.get(field), str):
            continue
        item[field] = _money_label_for_field(field, label_it, label_en)


def _sync_money_display_labels_after_repair(result: Dict[str, Any]) -> None:
    containers: List[Dict[str, Any]] = []
    if isinstance(result, dict):
        containers.append(result)
        cdc = result.get("customer_decision_contract")
        if isinstance(cdc, dict):
            containers.append(cdc)

    for container in containers:
        for box_key in _MONEY_BOX_KEYS:
            box = container.get(box_key)
            if not isinstance(box, dict):
                continue
            for list_key in _MONEY_LIST_KEYS:
                items = box.get(list_key)
                if not isinstance(items, list):
                    continue
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    if str(item.get("amount_status") or "") in (
                        "ANCHORED_EXPLICIT_EURO",
                        "NON_QUANTIFICATO_IN_MODO_DIFENDIBILE",
                    ):
                        _normalize_money_item_display_labels(item)


def _base_money_label(item: Dict[str, Any]) -> str:
    return _money_item_label_base(item)[0]


def _strip_money_amount_from_label(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = _LABEL_AMOUNT_TAIL_RE.sub("", text).strip(" :-–")
    return re.sub(r"\s+", " ", text).strip()


def _has_valuation_money_context(text: str) -> bool:
    key = _text_key(text)
    return any(term in key for term in ("deprezz", "detrazion", "rischio", "garanzia", "valore finale"))


def _money_meta_summary(outcome: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "box_key": outcome.get("box_key"),
        "list_key": outcome.get("list_key"),
        "index": outcome.get("index"),
        "old_amount": outcome.get("old_amount"),
        "new_amount": outcome.get("new_amount"),
        "searched_pages": outcome.get("searched_pages", []),
        "reason_it": outcome.get("reason_it"),
    }


def _meaningful_lines(text: str) -> List[str]:
    lines = []
    for raw in (text or "").splitlines():
        line = re.sub(r"\s+", " ", raw).strip()
        if len(line) >= 3:
            lines.append(line)
    return lines


def _line_context(lines: Sequence[str], index: int) -> str:
    start = max(0, index - 1)
    end = min(len(lines), index + 2)
    return _compact_quote(" ".join(lines[start:end]), limit=420)


def _line_has_asset_inventory_signal(line: str) -> bool:
    return bool(_LOT_RE.search(line) or _BENE_RE.search(line) or re.search(r"(?i)\b(?:catasto|foglio|sub\.?|superficie|stima|valore)\b", line))


def _asset_types_in_text(text: str) -> List[str]:
    out: List[str] = []
    for label, pattern in _ASSET_TYPE_PATTERNS:
        if pattern.search(text or "") and label not in out:
            out.append(label)
    return out


def _extract_surface(text: str) -> Optional[float]:
    match = _SURFACE_RE.search(text or "")
    if not match:
        return None
    raw = match.group("mq")
    try:
        return float(str(raw).replace(",", "."))
    except Exception:
        return None


def _extract_catasto(text: str) -> Optional[str]:
    match = _CATASTO_RE.search(text or "")
    if not match:
        return None
    return _compact_quote(match.group(0), limit=180)


def _lot_for_page(lot_candidates: Sequence[Dict[str, Any]], page: int) -> Optional[int]:
    current: Optional[int] = None
    for candidate in sorted(lot_candidates, key=lambda c: (int(c.get("page") or 0), int(c.get("lot_number") or 0))):
        cand_page = _safe_int(candidate.get("page"))
        lot_number = _safe_int(candidate.get("lot_number"))
        if cand_page is not None and lot_number is not None and cand_page <= page:
            current = lot_number
    return current


def _next_bene_number_for_lot(candidates: Sequence[Dict[str, Any]], lot_num: Optional[int]) -> int:
    values = [
        int(c.get("bene_number"))
        for c in candidates
        if _safe_int(c.get("lot_number")) == lot_num and isinstance(c.get("bene_number"), int)
    ]
    return (max(values) + 1) if values else 1


def _dedup_asset_candidates(candidates: Sequence[Dict[str, Any]], keys: Sequence[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen = set()
    for candidate in candidates:
        marker = tuple(candidate.get(key) for key in keys)
        if marker in seen:
            continue
        seen.add(marker)
        out.append(dict(candidate))
    return out


def _dedup_bene_candidates(candidates: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen = set()
    for candidate in candidates:
        marker = (
            candidate.get("lot_number"),
            candidate.get("bene_number"),
            tuple(candidate.get("asset_components") or []),
            candidate.get("page"),
        )
        if marker in seen:
            continue
        seen.add(marker)
        out.append(dict(candidate))
    return out


def _current_lots_count(result: Dict[str, Any]) -> int:
    value = result.get("lots_count")
    if isinstance(value, int) and value >= 0:
        return value
    lots = result.get("lots")
    if isinstance(lots, list):
        return len([lot for lot in lots if isinstance(lot, dict)])
    return 0


def _current_asset_types(result: Dict[str, Any]) -> set:
    serialized = _text_key(
        " ".join(text for key in ("lots", "beni", "lot_index") for text in _walk_strings(result.get(key)))
    )
    return {label for label, pattern in _ASSET_TYPE_PATTERNS if pattern.search(serialized)}


def _walk_strings(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, list):
        for item in value:
            yield from _walk_strings(item)
    elif isinstance(value, dict):
        for item in value.values():
            yield from _walk_strings(item)


def _existing_lots_by_number(lots: Any) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
    if not isinstance(lots, list):
        return out
    for index, lot in enumerate(lots, start=1):
        if not isinstance(lot, dict):
            continue
        lot_num = _safe_int(lot.get("lot_number") or lot.get("lot") or lot.get("numero") or index)
        if lot_num is not None and lot_num not in out:
            out[lot_num] = lot
    return out


def _source_beni_by_lot(inventory: Dict[str, Any]) -> Dict[int, List[Dict[str, Any]]]:
    out: Dict[int, List[Dict[str, Any]]] = {}
    for bene in inventory.get("bene_candidates") or []:
        lot_num = _safe_int(bene.get("lot_number"))
        if lot_num is None:
            continue
        out.setdefault(lot_num, []).append(bene)
    return out


def _source_bene_to_result_bene(candidate: Dict[str, Any], lot_num: int) -> Dict[str, Any]:
    bene_num = _safe_int(candidate.get("bene_number")) or 1
    components = candidate.get("asset_components") or [candidate.get("asset_type") or "bene"]
    bene: Dict[str, Any] = {
        "lot_number": lot_num,
        "bene_number": bene_num,
        "composite_key": f"lotto:{lot_num}/bene:{bene_num}",
        "bene_label": f"Lotto {lot_num} - Bene {bene_num}",
        "tipologia": _asset_type_label(components),
        "asset_type": components[0],
        "asset_components": components,
        "repair_origin": "asset_inventory_repair_gate",
        "evidence": [{"page": candidate.get("page"), "quote": candidate.get("quote")}],
    }
    if candidate.get("superficie_mq") is not None:
        bene["superficie_mq"] = candidate.get("superficie_mq")
    if candidate.get("catasto"):
        bene["catasto"] = candidate.get("catasto")
    if candidate.get("stima_euro") is not None:
        bene["stima_euro"] = candidate.get("stima_euro")
    return bene


def _merge_beni_by_asset_type(current_beni: Any, repaired_beni: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = [copy.deepcopy(b) for b in current_beni if isinstance(b, dict)]
    current_types = _text_key(" ".join(_walk_strings(merged)))
    for bene in repaired_beni:
        components = bene.get("asset_components") or []
        if components and all(component in current_types for component in components):
            continue
        merged.append(copy.deepcopy(bene))
    if not merged:
        merged.extend(copy.deepcopy(list(repaired_beni)))
    return merged


def _asset_type_label(types: Sequence[Any]) -> str:
    labels = []
    for value in types:
        text = str(value or "").strip()
        if text and text not in labels:
            labels.append(text)
    if not labels:
        return "Bene immobiliare"
    return " + ".join(label.capitalize() for label in labels)


def _lot_evidence_for_number(inventory: Dict[str, Any], lot_num: int) -> List[Dict[str, Any]]:
    evidence = [
        {"page": c.get("page"), "quote": c.get("quote")}
        for c in inventory.get("lot_candidates") or []
        if c.get("lot_number") == lot_num
    ]
    if evidence:
        return evidence[:3]
    return [
        {"page": c.get("page"), "quote": c.get("quote")}
        for c in inventory.get("detected_candidates") or []
    ][:2]


def _lot_index_entry(lot: Dict[str, Any]) -> Dict[str, Any]:
    lot_num = _safe_int(lot.get("lot_number") or lot.get("lot")) or 1
    beni = lot.get("beni") if isinstance(lot.get("beni"), list) else []
    return {
        "lot": lot_num,
        "lot_number": lot_num,
        "label": f"Lotto {lot_num}",
        "tipologia": lot.get("tipologia") or _asset_type_label([b.get("asset_type") for b in beni if isinstance(b, dict)]),
        "beni_count": len([b for b in beni if isinstance(b, dict)]),
        "evidence": lot.get("evidence", {}).get("lotto") if isinstance(lot.get("evidence"), dict) else [],
    }


def _sync_asset_headers(result: Dict[str, Any], lots_count: int) -> None:
    if lots_count > 1:
        value = f"Lotti multipli ({lots_count})"
    else:
        value = "Lotto unico"
    case_header = result.get("case_header")
    if not isinstance(case_header, dict):
        case_header = {}
        result["case_header"] = case_header
    case_header["lotto"] = value

    report_header = result.get("report_header")
    if not isinstance(report_header, dict):
        report_header = {}
        result["report_header"] = report_header
    lotto = report_header.get("lotto")
    if isinstance(lotto, dict):
        lotto["value"] = value
        lotto["status"] = "REPAIRED" if lots_count > 1 else lotto.get("status", "FOUND")
    else:
        report_header["lotto"] = {"value": value, "status": "REPAIRED" if lots_count > 1 else "FOUND"}
    report_header["lots_count"] = lots_count
    report_header["is_multi_lot"] = lots_count > 1


def _safe_int(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value or "").strip()
    if not text:
        return None
    match = re.search(r"-?\d+", text)
    if not match:
        return None
    try:
        return int(match.group(0))
    except Exception:
        return None


def _compact_quote(text: Any, limit: int = 360) -> str:
    quote = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(quote) <= limit:
        return quote
    return quote[:limit].rsplit(" ", 1)[0].rstrip(" ,;:.") + "."


def _text_key(value: Any) -> str:
    text = str(value or "").lower()
    replacements = {
        "à": "a",
        "è": "e",
        "é": "e",
        "ì": "i",
        "ò": "o",
        "ù": "u",
        "’": "'",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    return re.sub(r"\s+", " ", text).strip()
