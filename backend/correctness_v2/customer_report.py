"""
Deterministic customer-facing report renderer for Correctness Mode v2 (Step 3B).

``customer_report.json`` is a pure function of the already-persisted artifacts:

  * verified_report_contract.json           -> full customer report (REPORT_READY)
  * lot_selection_required.json + lot_index -> lot-selection report (no blending)
  * failure/manual-review job outcome       -> safe report with zero fake certainty

HARD RULES:
  * NO LLM call, NO PDF access, NO old analyzer, NO new facts. Every value in the
    output exists verbatim in the input artifacts (labels/titles are fixed Italian
    strings, never derived from unverified content).
  * Money rows are never duplicated and never hidden: rows are passed through from
    the contract's five money sections with a global (label, amount) dedup guard.
  * Prezzo base appears ONLY if the contract carries it in ``auction_terms`` — the
    renderer never synthesizes auction terms from other values.
  * Uncertain money renders under "Importi da verificare", never as confirmed cost.
  * Uncertain/unknown compliance renders as "Da verificare", never as conforming.
"""

from __future__ import annotations

import unicodedata
from typing import Any, Dict, List, Optional

from . import lots as lots_mod

CUSTOMER_REPORT_SCHEMA_VERSION = "cv2.customer_report.v1"

REPORT_READY = "REPORT_READY"
LOT_SELECTION_REQUIRED = "LOT_SELECTION_REQUIRED"
NEEDS_MANUAL_REVIEW = "NEEDS_MANUAL_REVIEW"
CONTRACT_VALIDATION_FAILED = "CONTRACT_VALIDATION_FAILED"

DISCLAIMER = (
    "Questo report è generato automaticamente a partire dalla perizia depositata. "
    "Tutte le informazioni provengono esclusivamente dal documento analizzato, con "
    "le pagine di riferimento indicate. Gli importi e le valutazioni riportati sono "
    "quelli dichiarati dal perito, non una valutazione indipendente. Il report non "
    "costituisce consulenza legale o tecnica: si raccomanda di verificare i punti "
    "segnalati con un professionista di fiducia prima di qualsiasi decisione."
)

UNCERTAIN_MONEY_TITLE = "Importi da verificare"

_SEVERITY_LABELS = {
    "grave": "Critico",
    "media": "Medio",
    "minore": "Minore",
    "info": "Informativo",
}

_CLASSIFICATION_LABELS = {
    "regularizable": "Regolarizzabile secondo la perizia",
    "non_conforming": "Non conforme secondo la perizia",
    "not_regularizable": "Non regolarizzabile secondo la perizia",
    "uncertain": "Da verificare",
}

# Per-lot money field -> customer label (kept in lockstep with lot_packets).
_LOT_MONEY_VALUE_FIELDS = [
    ("market_value", "Valore di mercato"),
    ("current_state_value", "Valore nello stato di fatto"),
    ("sale_value", "Valore di vendita giudiziaria"),
    ("regularization_costs", "Costi di regolarizzazione"),
    ("cancellation_costs", "Costi di cancellazione formalità"),
]
_LOT_MONEY_AUCTION_FIELDS = [
    ("prezzo_base_asta", "Prezzo base d'asta"),
    ("offerta_minima", "Offerta minima"),
    ("rialzo_minimo", "Rialzo minimo"),
    ("cauzione", "Cauzione"),
]


# ---------------------------------------------------------------------------
# Small deterministic helpers
# ---------------------------------------------------------------------------
def format_eur(amount: Any) -> Optional[str]:
    """Format a number as Italian-style euros: 1234567.8 -> '€ 1.234.567,80'."""
    if amount is None:
        return None
    try:
        value = float(amount)
    except (TypeError, ValueError):
        return None
    sign = "-" if value < 0 else ""
    grouped = f"{abs(value):,.2f}".replace(",", "|").replace(".", ",").replace("|", ".")
    return f"{sign}€ {grouped}"


def _norm(text: Any) -> str:
    stripped = "".join(
        c
        for c in unicodedata.normalize("NFKD", str(text or ""))
        if not unicodedata.combining(c)
    )
    return stripped.lower().strip()


def _pages(value: Any) -> List[int]:
    return [int(p) for p in (value or []) if isinstance(p, (int, float)) or str(p).isdigit()]


def _empty_report(
    analysis_id: str, job_id: str, report_status: str, title: str, subtitle: str
) -> Dict[str, Any]:
    """The full customer_report envelope with every content section empty."""
    return {
        "schema_version": CUSTOMER_REPORT_SCHEMA_VERSION,
        "analysis_id": analysis_id,
        "job_id": job_id,
        "report_status": report_status,
        "title": title,
        "subtitle": subtitle,
        "case_identity": {},
        "lot_structure": {},
        "executive_summary": [],
        "key_facts": [],
        "risk_sections": [],
        "money_sections": {
            "valuation_chain": [],
            "auction_terms": [],
            "buyer_side_costs": [],
            "procedure_cancelled_formalities": [],
            "uncertain_money": [],
        },
        "beni_sections": [],
        "buyer_checklist": [],
        "manual_review_flags": [],
        "evidence_index": [],
        "disclaimer": DISCLAIMER,
    }


def _money_row_view(row: Dict[str, Any], *, uncertain: bool = False) -> Dict[str, Any]:
    """Pass a contract money row through to the customer view (no new facts)."""
    view: Dict[str, Any] = {
        "label": row.get("label"),
        "amount": row.get("amount"),
        "amount_display": format_eur(row.get("amount")),
        "kind": row.get("kind"),
        "evidence_pages": _pages(row.get("evidence_pages")),
    }
    if row.get("notes"):
        view["notes"] = row["notes"]
    if row.get("source"):
        view["source"] = row["source"]
    if uncertain or row.get("kind") == "uncertain":
        view["status"] = "da_verificare"
        view["status_label"] = "Importo da verificare"
        if row.get("reason"):
            view["reason"] = row["reason"]
    return view


def _dedup_key(row: Dict[str, Any]) -> Optional[tuple]:
    amount = row.get("amount")
    if amount is None:
        return None
    try:
        return (_norm(row.get("label")), round(float(amount), 2))
    except (TypeError, ValueError):
        return None


def _money_sections_view(contract: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """Render the five money sections with a global (label, amount) dedup guard.

    Rows only ever come from the contract's own sections. Dedup drops a row only
    when the SAME normalized label + amount was already rendered, so distinct
    amounts (and distinct concepts sharing an amount) are never hidden.
    """
    seen: set = set()

    def render(rows: Any, *, uncertain: bool = False) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for row in rows or []:
            key = _dedup_key(row)
            if key is not None:
                if key in seen:
                    continue
                seen.add(key)
            out.append(_money_row_view(row, uncertain=uncertain))
        return out

    return {
        "valuation_chain": render(contract.get("valuation_chain")),
        "auction_terms": render(contract.get("auction_terms")),
        "buyer_side_costs": render(contract.get("buyer_side_costs")),
        "procedure_cancelled_formalities": render(
            contract.get("procedure_cancelled_formalities")
        ),
        "uncertain_money": render(contract.get("uncertain_money"), uncertain=True),
    }


def _risk_item_view(card: Dict[str, Any]) -> Dict[str, Any]:
    classification = card.get("classification")
    uncertain = classification == "uncertain" or card.get("severity") not in _SEVERITY_LABELS
    if classification in _CLASSIFICATION_LABELS:
        status_label = _CLASSIFICATION_LABELS[classification]
    elif uncertain:
        status_label = "Da verificare"
    else:
        status_label = _SEVERITY_LABELS.get(card.get("severity"), "Segnalazione")
    view: Dict[str, Any] = {
        "area": card.get("area"),
        "severity": "da_verificare" if classification == "uncertain" else card.get("severity"),
        "severity_label": (
            "Da verificare"
            if classification == "uncertain"
            else _SEVERITY_LABELS.get(card.get("severity"), "Segnalazione")
        ),
        "status_label": status_label,
        "summary": card.get("summary"),
        "regularizable": bool(card.get("regularizable")),
        "blocks_saleability": bool(card.get("blocks_saleability")),
        "evidence_pages": _pages(card.get("evidence_pages")),
    }
    if classification:
        view["classification"] = classification
    if card.get("cost") is not None:
        view["cost"] = card["cost"]
        view["cost_display"] = format_eur(card["cost"])
    if card.get("timing"):
        view["timing"] = card["timing"]
    return view


def _risk_sections(contract: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Group risk cards into fixed customer sections; uncertain NEVER as confirmed."""
    critical: List[Dict[str, Any]] = []
    manageable: List[Dict[str, Any]] = []
    minor: List[Dict[str, Any]] = []
    to_verify: List[Dict[str, Any]] = []

    for card in contract.get("risk_cards") or []:
        item = _risk_item_view(card)
        if card.get("classification") == "uncertain":
            to_verify.append(item)
        elif card.get("severity") == "grave":
            critical.append(item)
        elif card.get("severity") == "media":
            manageable.append(item)
        elif card.get("severity") in ("minore", "info"):
            minor.append(item)
        else:
            # Unknown severity is uncertainty, never silently promoted or dropped.
            to_verify.append(item)

    sections: List[Dict[str, Any]] = []
    if critical:
        sections.append(
            {"section_id": "criticita", "title": "Criticità rilevanti", "items": critical}
        )
    if manageable:
        sections.append(
            {
                "section_id": "rischi_gestibili",
                "title": "Difformità e rischi indicati come gestibili",
                "items": manageable,
            }
        )
    if minor:
        sections.append(
            {"section_id": "segnalazioni_minori", "title": "Segnalazioni minori", "items": minor}
        )
    if to_verify:
        sections.append(
            {
                "section_id": "da_verificare",
                "title": "Aspetti da verificare (non confermati dalla perizia)",
                "items": to_verify,
            }
        )
    return sections


def _key_facts(contract: Dict[str, Any]) -> List[Dict[str, Any]]:
    facts: List[Dict[str, Any]] = []
    for fact in contract.get("executive_summary_facts") or []:
        value = fact.get("value")
        view = {
            "label": fact.get("label"),
            "value": value,
            "evidence_pages": _pages(fact.get("evidence_pages")),
        }
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            view["value_display"] = format_eur(value)
        facts.append(view)
    return facts


def _fact_by_label(contract: Dict[str, Any], label: str) -> Optional[Dict[str, Any]]:
    for fact in contract.get("executive_summary_facts") or []:
        if fact.get("label") == label:
            return fact
    return None


def _executive_summary(contract: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Short factual Italian sentences, each derived from one contract fact/count."""
    out: List[Dict[str, Any]] = []
    ci = contract.get("case_identity") or {}
    ci_pages = _pages(ci.get("evidence_pages"))

    identity_bits = [str(v) for v in (ci.get("property_type"), ci.get("address")) if v]
    if identity_bits:
        out.append(
            {"text": f"Immobile oggetto della perizia: {', '.join(identity_bits)}.", "evidence_pages": ci_pages}
        )

    occ = _fact_by_label(contract, "Stato occupazione")
    if occ and occ.get("value"):
        out.append(
            {
                "text": f"Stato di occupazione dichiarato in perizia: {occ['value']}.",
                "evidence_pages": _pages(occ.get("evidence_pages")),
            }
        )

    sale = _fact_by_label(contract, "Valore di vendita giudiziaria")
    if sale and sale.get("value") is not None:
        out.append(
            {
                "text": (
                    "Valore di vendita giudiziaria indicato in perizia: "
                    f"{format_eur(sale['value'])}."
                ),
                "evidence_pages": _pages(sale.get("evidence_pages")),
            }
        )

    market = _fact_by_label(contract, "Valore di mercato")
    if market and market.get("value") is not None:
        out.append(
            {
                "text": f"Valore di mercato stimato dal perito: {format_eur(market['value'])}.",
                "evidence_pages": _pages(market.get("evidence_pages")),
            }
        )

    cards = contract.get("risk_cards") or []
    if cards:
        graves = sum(1 for c in cards if c.get("severity") == "grave")
        text = f"La perizia segnala {len(cards)} punti di attenzione"
        if graves:
            text += f", di cui {graves} classificati come critici"
        out.append({"text": text + ".", "evidence_pages": []})

    uncertain_money = contract.get("uncertain_money") or []
    if uncertain_money:
        out.append(
            {
                "text": (
                    f"Sono presenti {len(uncertain_money)} importi il cui ruolo non è "
                    "chiaro dal documento: vanno verificati prima di ogni valutazione."
                ),
                "evidence_pages": [],
            }
        )

    flags = contract.get("uncertainty_flags") or []
    if flags:
        out.append(
            {
                "text": (
                    f"{len(flags)} aspetti non sono stati verificati automaticamente "
                    "e restano da controllare."
                ),
                "evidence_pages": [],
            }
        )
    return out


def _lot_structure(contract: Dict[str, Any]) -> Dict[str, Any]:
    lot = contract.get("lot_summary") or {}
    return {
        "multi_lot": bool(lot.get("multi_lot")),
        "lot_count": lot.get("lot_count"),
        "selected_lot": lot.get("selected_lot"),
        "bene_count": lot.get("bene_count", 0),
        "multi_bene": bool(lot.get("multi_bene")),
        "bene_ids": [str(b) for b in lot.get("bene_ids") or []],
    }


def _beni_sections(contract: Dict[str, Any]) -> List[Dict[str, Any]]:
    """One section per bene of the (single) lot, populated by generic bene-token
    matching over the contract's own risk cards and checklist. Never invents
    per-bene detail: a bene with no explicitly tagged content gets empty lists."""
    lot = contract.get("lot_summary") or {}
    bene_ids = [str(b) for b in lot.get("bene_ids") or []]
    if len(bene_ids) < 2:
        return []

    sections: Dict[str, Dict[str, Any]] = {
        b: {
            "bene_id": b,
            "title": f"Bene {b}",
            "risks": [],
            "checklist": [],
            "note": None,
        }
        for b in bene_ids
    }

    for card in contract.get("risk_cards") or []:
        text = f"{card.get('area') or ''} {card.get('summary') or ''}"
        for b in lots_mod.bene_ids_in_text(text):
            if b in sections:
                sections[b]["risks"].append(_risk_item_view(card))

    for item in contract.get("buyer_action_checklist") or []:
        text = f"{item.get('action') or ''} {item.get('detail') or ''}"
        for b in lots_mod.bene_ids_in_text(text):
            if b in sections:
                sections[b]["checklist"].append(dict(item))

    for section in sections.values():
        if not section["risks"] and not section["checklist"]:
            section["note"] = (
                "La perizia non riporta segnalazioni specifiche riferite espressamente "
                "a questo bene; valgono le sezioni generali del lotto."
            )
    return [sections[b] for b in bene_ids]


def _buyer_checklist(contract: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Pass-through of the contract checklist (already zero-value-free)."""
    out: List[Dict[str, Any]] = []
    for item in contract.get("buyer_action_checklist") or []:
        detail = str(item.get("detail") or "")
        # Defense-in-depth for rule 9: never render a zero-value cost action.
        if _norm(detail).endswith((": 0", ": 0.0", ": 0,0", ": 0,00")):
            continue
        view = {
            "action": item.get("action"),
            "detail": item.get("detail"),
            "evidence_pages": _pages(item.get("evidence_pages")),
        }
        if item.get("blocks_saleability") is not None:
            view["blocks_saleability"] = bool(item.get("blocks_saleability"))
        out.append(view)
    return out


def _manual_review_flags(contract: Dict[str, Any]) -> List[Dict[str, Any]]:
    flags: List[Dict[str, Any]] = []
    for flag in contract.get("uncertainty_flags") or []:
        view = {"kind": flag.get("kind"), "detail": flag.get("detail")}
        if flag.get("code"):
            view["code"] = flag["code"]
        if flag.get("evidence_pages"):
            view["evidence_pages"] = _pages(flag.get("evidence_pages"))
        flags.append(view)
    for row in contract.get("uncertain_money") or []:
        flags.append(
            {
                "kind": "uncertain_money",
                "detail": (
                    f"Importo da verificare: {row.get('label')} "
                    f"({format_eur(row.get('amount'))})."
                ),
                "evidence_pages": _pages(row.get("evidence_pages")),
            }
        )
    for card in contract.get("risk_cards") or []:
        if card.get("classification") == "uncertain":
            flags.append(
                {
                    "kind": "compliance_uncertain",
                    "detail": (
                        f"Conformità non verificabile automaticamente: {card.get('area')}."
                    ),
                    "evidence_pages": _pages(card.get("evidence_pages")),
                }
            )
    return flags


def _evidence_index(contract: Dict[str, Any]) -> List[Dict[str, Any]]:
    index = contract.get("evidence_index") or {}
    out: List[Dict[str, Any]] = []
    for page_key in index:
        try:
            page = int(page_key)
        except (TypeError, ValueError):
            continue
        out.append({"page": page, "referenced_by": list(index[page_key] or [])})
    return sorted(out, key=lambda e: e["page"])


def _title_from_identity(ci: Dict[str, Any]) -> str:
    prop = ci.get("property_type")
    addr = ci.get("address")
    if prop and addr:
        return f"{prop} – {addr}"
    if prop:
        return str(prop)
    if addr:
        return str(addr)
    return "Report di analisi della perizia"


def _lot_label(value: Any) -> Optional[str]:
    if value is None or str(value).strip() == "":
        return None
    text = str(value).strip()
    return text if _norm(text).startswith("lott") else f"Lotto {text}"


def _subtitle_from_identity(ci: Dict[str, Any], selected_lot: Any) -> str:
    bits: List[str] = []
    if ci.get("tribunale"):
        bits.append(str(ci["tribunale"]))
    if ci.get("procedura_rge"):
        bits.append(str(ci["procedura_rge"]))
    lot_label = _lot_label(selected_lot) or _lot_label(ci.get("lotto"))
    if lot_label:
        bits.append(lot_label)
    return " · ".join(bits)


# ---------------------------------------------------------------------------
# Public renderers
# ---------------------------------------------------------------------------
def render_success_report(contract: Dict[str, Any]) -> Dict[str, Any]:
    """Render the customer report for a validated single-lot contract."""
    ci = dict(contract.get("case_identity") or {})
    lot_structure = _lot_structure(contract)
    report = _empty_report(
        str(contract.get("analysis_id")),
        str(contract.get("job_id")),
        REPORT_READY,
        _title_from_identity(ci),
        _subtitle_from_identity(ci, lot_structure.get("selected_lot")),
    )
    report["case_identity"] = {k: v for k, v in ci.items() if v not in (None, [], "")}
    report["lot_structure"] = lot_structure
    report["executive_summary"] = _executive_summary(contract)
    report["key_facts"] = _key_facts(contract)
    report["risk_sections"] = _risk_sections(contract)
    report["money_sections"] = _money_sections_view(contract)
    report["beni_sections"] = _beni_sections(contract)
    report["buyer_checklist"] = _buyer_checklist(contract)
    report["manual_review_flags"] = _manual_review_flags(contract)
    report["evidence_index"] = _evidence_index(contract)
    report["sections_meta"] = {
        "uncertain_money_title": UNCERTAIN_MONEY_TITLE,
        "source_contract_schema": contract.get("schema_version"),
        "source_pdf_quality_status": contract.get("source_pdf_quality_status"),
        "validation_status": contract.get("validation_status"),
    }
    return report


def _lot_money_summary(
    selection_lot: Dict[str, Any], index_lot: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Per-lot money summary for the selector: strict lot money first, then any
    legacy key_money rows not already listed (dedup by label+amount)."""
    rows: List[Dict[str, Any]] = []
    seen: set = set()

    def add(label: Any, amount: Any, evidence_pages: Any, kind: str) -> None:
        if amount is None:
            return
        key = (_norm(label), round(float(amount), 2)) if label else None
        if key is not None:
            if key in seen:
                return
            # Same amount whose label merely contains (or is contained by) an
            # already-listed one is the same row restated with a lot prefix
            # (e.g. "Lotto 1 - Prezzo base d'asta" vs "Prezzo base d'asta").
            norm_label = _norm(label)
            for existing in rows:
                try:
                    same_amount = round(float(existing["amount"]), 2) == key[1]
                except (TypeError, ValueError):
                    continue
                existing_label = _norm(existing.get("label"))
                if same_amount and existing_label and (
                    norm_label in existing_label or existing_label in norm_label
                ):
                    return
            seen.add(key)
        rows.append(
            {
                "label": label,
                "amount": amount,
                "amount_display": format_eur(amount),
                "kind": kind,
                "evidence_pages": _pages(evidence_pages),
            }
        )

    strict = index_lot.get("money") or {}
    for field, label in _LOT_MONEY_VALUE_FIELDS:
        value = strict.get(field)
        if isinstance(value, dict):
            add(label, value.get("amount"), value.get("evidence_pages"), "value")
        elif value is not None:
            add(label, value, strict.get("evidence_pages"), "value")
    for field, label in _LOT_MONEY_AUCTION_FIELDS:
        value = strict.get(field)
        if isinstance(value, dict):
            add(label, value.get("amount"), value.get("evidence_pages"), "auction_term")
        elif value is not None:
            add(label, value, strict.get("evidence_pages"), "auction_term")
    for section, kind in (
        ("deductions", "deduction"),
        ("buyer_side_costs", "buyer_side"),
        ("procedure_cancelled_formalities", "procedure_cancelled"),
        ("shared_summary_rows", "lot_summary_value"),
    ):
        for row in strict.get(section) or []:
            add(row.get("label"), row.get("amount"), row.get("evidence_pages"), kind)

    for row in selection_lot.get("key_money") or []:
        add(row.get("label"), row.get("amount"), row.get("evidence_pages"), "lot_summary_value")
    return rows


def render_lot_selection_report(
    selection: Dict[str, Any], lot_index: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Render the LOT_SELECTION_REQUIRED customer report (a selector, NOT a blend).

    Content comes only from lot_selection_required.json + lot_index.json. All the
    normal report sections stay empty: no blended facts, risks or money are shown.
    """
    lot_index = lot_index or {}
    lot_ids = [str(x) for x in selection.get("lot_ids") or []]
    lot_count = selection.get("lot_count", len(lot_ids))
    index_lots = {str(L.get("lot_id")): L for L in lot_index.get("lots") or []}

    report = _empty_report(
        str(selection.get("analysis_id")),
        str(selection.get("job_id")),
        LOT_SELECTION_REQUIRED,
        "Selezione del lotto richiesta",
        f"La perizia contiene {lot_count} lotti distinti",
    )
    message = selection.get("message") or (
        f"Rilevati {lot_count} lotti distinti. Selezionare un lotto da analizzare "
        "oppure richiedere l'analisi di tutti i lotti. I lotti non vengono mai fusi."
    )
    report["lot_structure"] = {
        "multi_lot": True,
        "lot_count": lot_count,
        "lot_ids": lot_ids,
        "selected_lot": None,
    }
    report["executive_summary"] = [{"text": message, "evidence_pages": []}]

    lots_view: List[Dict[str, Any]] = []
    evidence: Dict[int, List[str]] = {}
    for lot in selection.get("available_lots") or []:
        lot_id = str(lot.get("lot_id"))
        index_lot = index_lots.get(lot_id, {})
        pages = _pages(lot.get("page_evidence"))
        for p in pages:
            evidence.setdefault(p, []).append(f"lotto {lot_id}")
        lots_view.append(
            {
                "lot_id": lot_id,
                "label": lot.get("label"),
                "address": lot.get("address"),
                "property_type": lot.get("property_type"),
                "ownership_right": lot.get("ownership_right"),
                "occupancy_summary": lot.get("occupancy_summary"),
                "money_summary": _lot_money_summary(lot, index_lot),
                "evidence_pages": pages,
                "confidence": lot.get("confidence"),
                "notes": list(lot.get("notes") or []),
            }
        )

    # Money that could not be safely attributed to one lot stays visible as
    # "da verificare" — it is never blended into a lot and never hidden.
    uncertain_rows = [
        _money_row_view(row, uncertain=True)
        for row in lot_index.get("uncertain_money") or []
        if row.get("amount") is not None
    ]
    global_money = lot_index.get("global_money") or {}
    global_rows: List[Dict[str, Any]] = []
    for field, label in _LOT_MONEY_VALUE_FIELDS + _LOT_MONEY_AUCTION_FIELDS:
        value = global_money.get(field)
        if isinstance(value, dict) and value.get("amount") is not None:
            global_rows.append(
                _money_row_view(
                    {
                        "label": label,
                        "amount": value.get("amount"),
                        "kind": "global",
                        "evidence_pages": value.get("evidence_pages"),
                    }
                )
            )
        elif value is not None and not isinstance(value, dict):
            global_rows.append(
                _money_row_view({"label": label, "amount": value, "kind": "global"})
            )

    report["lot_selection"] = {
        "message": message,
        "lots": lots_view,
        "available_actions": [dict(a) for a in selection.get("available_actions") or []],
        "global_money": global_rows,
        "uncertain_money": uncertain_rows,
    }
    if uncertain_rows:
        report["manual_review_flags"] = [
            {
                "kind": "uncertain_money",
                "detail": (
                    f"Importo non attribuibile con certezza a un singolo lotto: "
                    f"{row.get('label')} ({row.get('amount_display')})."
                ),
                "evidence_pages": row.get("evidence_pages", []),
            }
            for row in uncertain_rows
        ]
    report["evidence_index"] = [
        {"page": p, "referenced_by": evidence[p]} for p in sorted(evidence)
    ]
    return report


_SAFE_TITLES = {
    NEEDS_MANUAL_REVIEW: "Revisione manuale necessaria",
    CONTRACT_VALIDATION_FAILED: "Report non disponibile: verifica non superata",
}

_SAFE_SUMMARY = (
    "Nessun dato della perizia è stato confermato automaticamente: il report non "
    "riporta valori, rischi o costi perché non è stato possibile verificarli in "
    "modo affidabile."
)


def render_safe_report(
    *,
    analysis_id: str,
    job_id: str,
    report_status: str,
    job_status_value: Optional[str] = None,
    reason_code: Optional[str] = None,
    reason_human: Optional[str] = None,
    next_steps: Optional[List[str]] = None,
    violation_codes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Render the fail-closed customer report: uncertainty only, zero fake facts.

    ``report_status`` must be NEEDS_MANUAL_REVIEW or CONTRACT_VALIDATION_FAILED;
    the precise job status (e.g. FAILED_ANALYSIS) is carried in ``job_status``.
    """
    if report_status not in _SAFE_TITLES:
        report_status = NEEDS_MANUAL_REVIEW
    report = _empty_report(
        str(analysis_id),
        str(job_id),
        report_status,
        _SAFE_TITLES[report_status],
        reason_human or "L'analisi automatica non ha prodotto un report verificato.",
    )
    if job_status_value:
        report["job_status"] = job_status_value

    summary = [{"text": _SAFE_SUMMARY, "evidence_pages": []}]
    if reason_human:
        summary.insert(0, {"text": str(reason_human), "evidence_pages": []})
    report["executive_summary"] = summary

    flags: List[Dict[str, Any]] = [
        {
            "kind": "status",
            "code": reason_code or report_status,
            "detail": reason_human or _SAFE_TITLES[report_status],
        }
    ]
    for code in violation_codes or []:
        flags.append(
            {
                "kind": "validation_violation",
                "code": str(code),
                "detail": f"Verifica non superata: {code}.",
            }
        )
    for step in next_steps or []:
        flags.append({"kind": "next_step", "detail": str(step)})
    report["manual_review_flags"] = flags
    return report
