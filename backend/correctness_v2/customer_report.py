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

import re
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

from . import doc_signals, lots as lots_mod

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
MARKET_COMPARATIVES_TITLE = "Comparativi di mercato"
CONTEXT_VALUES_TITLE = "Dati economici di contesto"

# Uncertain-money rows whose LABEL clearly states a background/context role are
# rendered in dedicated sections instead of the scary "Importi da verificare":
# market comparables (OMI/borsino/annunci) and context values (rendita, canone,
# spese condominiali, capitale di formalità). Rows whose role stays unclear
# remain uncertain — never promoted to confirmed costs. The kind sets live in
# doc_signals so the renderer and the coverage audit can never drift.
_COMPARATIVE_KINDS = doc_signals.COMPARATIVE_LABEL_KINDS
_CONTEXT_KINDS = doc_signals.CONTEXT_LABEL_KINDS

_CONTEXT_NOTES = {
    "rendita": "Rendita catastale: dato di contesto, non è un costo a carico dell'acquirente.",
    "canone": "Canone/locazione dichiarato in perizia: dato di contesto, non è un costo di acquisto.",
    "spese_condominiali": "Spese condominiali indicate in perizia: verificare l'eventuale quota a carico dell'acquirente.",
    "formalita_capitale": (
        "Importo della formalità iscritta (es. capitale di ipoteca/mutuo): non è un "
        "debito a carico dell'acquirente salvo diversa indicazione della perizia."
    ),
}

_COMPARATIVE_NOTE = (
    "Valore comparativo usato dal perito come riferimento di mercato: dato di "
    "contesto della valutazione, non un costo né un valore di vendita."
)


# Fixed Italian labels for formality types (raw machine tokens like "other"
# must never reach the customer view).
_FORMALITY_TYPE_LABELS = {
    "ipoteca": "Ipoteca",
    "pignoramento": "Pignoramento",
    "sequestro": "Sequestro",
    "domanda_giudiziale": "Domanda giudiziale",
    "trascrizione": "Trascrizione",
    "iscrizione": "Iscrizione",
    "other": "Altra formalità",
}


def _formality_type_label(raw_type: Any) -> str:
    key = _norm(raw_type).replace(" ", "_")
    if key in _FORMALITY_TYPE_LABELS:
        return _FORMALITY_TYPE_LABELS[key]
    text = str(raw_type or "Formalità").strip()
    return text[:1].upper() + text[1:] if text else "Formalità"


def _uncertain_row_bucket(row: Dict[str, Any]) -> str:
    """'comparatives' | 'context' | 'uncertain' from the row's own label."""
    kind = doc_signals.label_kind(row.get("label"))
    if kind in _COMPARATIVE_KINDS:
        return "comparatives"
    if kind in _CONTEXT_KINDS:
        return "context"
    return "uncertain"


def _split_uncertain_rows(
    contract: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    comparatives: List[Dict[str, Any]] = []
    context: List[Dict[str, Any]] = []
    uncertain: List[Dict[str, Any]] = []
    for row in contract.get("uncertain_money") or []:
        bucket = _uncertain_row_bucket(row)
        if bucket == "comparatives":
            comparatives.append(row)
        elif bucket == "context":
            context.append(row)
        else:
            uncertain.append(row)
    return comparatives, context, uncertain

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
            "market_comparatives": [],
            "context_values": [],
            "uncertain_money": [],
        },
        "beni_sections": [],
        "occupancy_section": {},
        "compliance_section": [],
        "formalities_section": [],
        "surfaces_section": [],
        "buyer_checklist": [],
        "manual_review_flags": [],
        "evidence_index": [],
        "customer_evidence_index": [],
        "admin_evidence_index": [],
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
    """Render the money sections with a global (label, amount) dedup guard.

    Rows only ever come from the contract's own sections. Dedup drops a row only
    when the SAME normalized label + amount was already rendered, so distinct
    amounts (and distinct concepts sharing an amount) are never hidden.

    Consistency rules (generic, no document-specific branching):
      * A valuation-chain cost that is ALSO explicitly buyer-side is echoed once
        in ``buyer_side_costs`` with ``included_in_valuation=True`` — the buyer
        section is never empty while a buyer-relevant cost sits in the chain.
      * If no cancellation COSTS exist but the perizia lists formalities
        cancelled by the procedure, ``procedure_cancelled_formalities`` carries
        amount-free reference rows (the formality is a fact, not a buyer cost).
      * Uncertain rows with a clear background role move to ``market_comparatives``
        / ``context_values``; only genuinely unclear amounts stay uncertain.
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

    comparatives_src, context_src, uncertain_src = _split_uncertain_rows(contract)

    sections = {
        "valuation_chain": render(contract.get("valuation_chain")),
        "auction_terms": render(contract.get("auction_terms")),
        "buyer_side_costs": render(contract.get("buyer_side_costs")),
        "procedure_cancelled_formalities": render(
            contract.get("procedure_cancelled_formalities")
        ),
        "market_comparatives": [],
        "context_values": [],
        "uncertain_money": render(uncertain_src, uncertain=True),
    }

    # Comparables / context values: shown with their role stated, outside the
    # uncertainty bucket, and NEVER as confirmed costs (dedup guard still on).
    def render_background(rows: List[Dict[str, Any]], status: str,
                          status_label: str, note_for) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for row in rows:
            key = _dedup_key(row)
            if key is not None:
                if key in seen:
                    continue
                seen.add(key)
            view = _money_row_view(row)
            view["status"] = status
            view["status_label"] = status_label
            note = note_for(row)
            if note:
                view["notes"] = note
            out.append(view)
        return out

    sections["market_comparatives"] = render_background(
        comparatives_src, "comparativo", "Comparativo di mercato (dato di contesto)",
        lambda _row: _COMPARATIVE_NOTE,
    )
    sections["context_values"] = render_background(
        context_src, "contesto", "Dato economico di contesto (non è un costo)",
        lambda row: _CONTEXT_NOTES.get(doc_signals.label_kind(row.get("label"))),
    )

    # Buyer-side costs already counted in the valuation chain: echoed once with
    # an explicit included_in_valuation marker, never as an extra cost.
    buyer_keys = {
        _dedup_key(r) for r in sections["buyer_side_costs"] if _dedup_key(r) is not None
    }
    for row in contract.get("valuation_chain") or []:
        roles = set(row.get("roles") or [])
        if "buyer_side" not in roles:
            continue
        key = _dedup_key(row)
        if key is not None and key in buyer_keys:
            continue
        view = _money_row_view(row)
        view["included_in_valuation"] = True
        view["notes"] = "Già considerato nella catena di valore."
        view["status_label"] = "Costo a carico dell'acquirente già incluso nei valori"
        sections["buyer_side_costs"].append(view)
        if key is not None:
            buyer_keys.add(key)

    # Cancelled-by-procedure formalities ALWAYS visible where the customer
    # expects them: one amount-free reference row per formality TYPE (a
    # formality is a fact, not a buyer cost), regardless of whether the
    # section already carries cancellation COST rows — a €294 cancellation
    # cost must never suppress the ipoteca/pignoramento references.
    seen_forms: set = set()
    for item in contract.get("legal_formalities") or []:
        if not item.get("cancelled_by_procedure"):
            continue
        form_key = _norm(item.get("type"))
        if form_key in seen_forms:
            continue
        seen_forms.add(form_key)
        type_label = _formality_type_label(item.get("type"))
        sections["procedure_cancelled_formalities"].append(
            {
                "label": f"{type_label}: cancellazione a cura della procedura",
                "amount": None,
                "amount_display": None,
                "kind": "procedure_cancelled_reference",
                "informational": True,
                "notes": (
                    "Formalità cancellata dalla procedura, non è un costo per "
                    "l'acquirente: i dettagli sono nella sezione 'Formalità e "
                    "cancellazioni'."
                ),
                "evidence_pages": _pages(item.get("evidence_pages")),
            }
        )

    return sections


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

    # Only genuinely unclear amounts count as "da verificare": comparables and
    # context values (rendita, canone, ...) have a clear background role.
    _comp, _ctx, uncertain_money = _split_uncertain_rows(contract)
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


def _has_real_property(ci: Dict[str, Any]) -> bool:
    return bool(ci.get("property_type") or ci.get("address"))


def _lot_structure(contract: Dict[str, Any]) -> Dict[str, Any]:
    lot = contract.get("lot_summary") or {}
    ci = contract.get("case_identity") or {}
    detected = int(lot.get("bene_count") or 0)
    bene_count = detected
    # A real property (property_type/address present) is never "zero beni":
    # when explicit beni were not extracted, the lot still contains its main
    # property. No extra beni are ever faked — the count only floors at 1.
    if bene_count == 0 and _has_real_property(ci):
        bene_count = 1
    view = {
        "multi_lot": bool(lot.get("multi_lot")),
        "lot_count": lot.get("lot_count"),
        "selected_lot": lot.get("selected_lot"),
        "bene_count": bene_count,
        "multi_bene": bool(lot.get("multi_bene")),
        "bene_ids": [str(b) for b in lot.get("bene_ids") or []],
    }
    if bene_count != detected:
        view["detected_bene_count"] = detected
        view["bene_count_source"] = "bene_principale_da_tipologia"
    return view


# Accessory/pertinenza terms (generic Italian): rendered as accessories of the
# main bene, never as extra fake beni.
_ACCESSORY_RE = re.compile(
    r"\b(soffitt\w*|cantin\w*|garage|box\s+auto|autorimess\w*|posto\s+auto|"
    r"solai\w*|magazzin\w*|tettoi\w*|pertinenz\w*|accessori\w*)\b",
    re.IGNORECASE,
)

# Canonical display form per accessory stem (matched term is normalized so
# "SOFFITTA"/"soffitte" collapse into one entry).
def _accessory_canonical(term: str) -> str:
    n = _norm(term)
    for stem, label in (
        ("soffitt", "soffitta"), ("cantin", "cantina"), ("garage", "garage"),
        ("box", "box auto"), ("autorimess", "autorimessa"),
        ("posto", "posto auto"), ("solai", "solaio"), ("magazzin", "magazzino"),
        ("tettoi", "tettoia"),
    ):
        if n.startswith(stem):
            return label
    return n


_GENERIC_ACCESSORY_STEMS = ("pertinenz", "accessori")


def _detect_accessories(
    contract: Dict[str, Any], pages: Optional[List[Dict[str, Any]]]
) -> List[Dict[str, Any]]:
    """Accessory/pertinenza units named by the document (soffitta, cantina...).

    Sources, in order: the contract's own text fields, then the page text of
    THIS analysis (document truth, same precedent as surfaces). Only concrete
    accessory nouns are reported; bare 'pertinenza/accessorio' wording alone is
    not an accessory name and is ignored.
    """
    found: Dict[str, Dict[str, Any]] = {}

    def scan(text: Any, pages_ev: Any) -> None:
        for m in _ACCESSORY_RE.finditer(str(text or "")):
            canonical = _accessory_canonical(m.group(1))
            if any(canonical.startswith(s) for s in _GENERIC_ACCESSORY_STEMS):
                continue
            entry = found.setdefault(
                canonical, {"label": canonical, "evidence_pages": []}
            )
            for p in _pages(pages_ev):
                if p not in entry["evidence_pages"]:
                    entry["evidence_pages"].append(p)

    ci = contract.get("case_identity") or {}
    ci_pages = ci.get("evidence_pages")
    scan(ci.get("property_type"), ci_pages)
    scan(ci.get("address"), ci_pages)
    for card in contract.get("risk_cards") or []:
        scan(f"{card.get('area') or ''} {card.get('summary') or ''}", card.get("evidence_pages"))
    for item in contract.get("compliance_overview") or []:
        scan(f"{item.get('area') or ''} {item.get('notes') or ''}", item.get("evidence_pages"))
    for item in contract.get("buyer_action_checklist") or []:
        scan(f"{item.get('action') or ''} {item.get('detail') or ''}", item.get("evidence_pages"))

    for page in pages or []:
        pnum = doc_signals.page_number(page)
        if pnum is None:
            continue
        scan(page.get("text"), [pnum])

    out = [found[k] for k in sorted(found)]
    for entry in out:
        entry["evidence_pages"] = sorted(entry["evidence_pages"])
        entry["note"] = "Accessorio/pertinenza del bene principale secondo la perizia."
    return out


def _beni_sections(
    contract: Dict[str, Any], pages: Optional[List[Dict[str, Any]]] = None
) -> List[Dict[str, Any]]:
    """One section per bene of the (single) lot, populated by generic bene-token
    matching over the contract's own risk cards and checklist. Never invents
    per-bene detail: a bene with no explicitly tagged content gets empty lists.

    When NO explicit beni were extracted but the perizia clearly describes one
    property, a single "Bene principale" section is rendered from case_identity
    (with document-named accessories such as soffitta/cantina/garage). Multiple
    beni are never faked."""
    lot = contract.get("lot_summary") or {}
    bene_ids = [str(b) for b in lot.get("bene_ids") or []]
    if len(bene_ids) < 2:
        ci = contract.get("case_identity") or {}
        if not _has_real_property(ci):
            return []
        title = f"Bene principale: {ci.get('property_type')}" if ci.get(
            "property_type"
        ) else "Bene principale"
        section: Dict[str, Any] = {
            "bene_id": bene_ids[0] if bene_ids else "principale",
            "title": title,
            "is_main_property": True,
            "property_type": ci.get("property_type"),
            "address": ci.get("address"),
            "evidence_pages": _pages(ci.get("evidence_pages")),
            "risks": [],
            "checklist": [],
            "note": None,
        }
        accessories = _detect_accessories(contract, pages)
        if accessories:
            section["accessories"] = accessories
        return [section]

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


# Italian labels for manual-review flag kinds: raw machine kinds stay in the
# JSON for admin debug, but the frontend must render kind_label only.
_FLAG_KIND_LABELS = {
    "missing_or_uncertain": "Dato mancante o incerto",
    "analyst_warning": "Segnalazione dell'analisi",
    "validator_warning": "Verifica automatica",
    "uncertain_money": "Importo da verificare",
    "compliance_uncertain": "Conformità da verificare",
    "status": "Stato dell'analisi",
    "validation_violation": "Verifica non superata",
    "next_step": "Passo suggerito",
}


def _flag_view(kind: str, detail: Any, evidence_pages: Any = None, code: Any = None) -> Dict[str, Any]:
    view: Dict[str, Any] = {
        "kind": kind,
        "kind_label": _FLAG_KIND_LABELS.get(kind, "Punto da verificare"),
        "detail": detail,
    }
    if code:
        view["code"] = code
    pages = _pages(evidence_pages)
    if pages:
        view["evidence_pages"] = pages
    return view


# Customer-safe Italian wording for validator warning codes. The raw English
# detail stays in debug_detail (admin-only); customers see only these.
_VALIDATOR_WARNING_LABELS = {
    "ZERO_AMOUNT_BUYER_COST": (
        "Una voce di costo a carico dell'acquirente ha importo pari a zero in "
        "perizia e non è stata considerata un costo effettivo."
    ),
    "MONEY_ROW_EVIDENCE_VIA_MERGE": (
        "Un importo senza pagina di riferimento propria è stato unito alla voce "
        "equivalente con riferimento di pagina."
    ),
    "SAME_AMOUNT_CONFLICTING_KIND": (
        "Uno stesso importo compare in perizia con ruoli diversi: verificare il "
        "ruolo corretto sul documento."
    ),
}


def _validator_warning_detail(code: Any, raw_detail: Any) -> str:
    label = _VALIDATOR_WARNING_LABELS.get(str(code or ""))
    if label:
        return label
    return (
        f"Verifica automatica: segnalazione tecnica (codice {code}). "
        "Dettagli disponibili nel report di validazione."
    )


def _manual_review_flags(contract: Dict[str, Any]) -> List[Dict[str, Any]]:
    flags: List[Dict[str, Any]] = []
    for flag in contract.get("uncertainty_flags") or []:
        kind = flag.get("kind")
        detail = flag.get("detail")
        view = _flag_view(kind, detail, flag.get("evidence_pages"), flag.get("code"))
        if kind == "validator_warning":
            # Validator details are internal English: keep them for admin debug
            # but expose only the Italian customer-safe wording.
            view["debug_detail"] = detail
            view["detail"] = _validator_warning_detail(flag.get("code"), detail)
        flags.append(view)
    # Comparables/context rows are NOT flagged: their role is clear from the
    # document and they render in their own background sections.
    _comp, _ctx, true_uncertain = _split_uncertain_rows(contract)
    for row in true_uncertain:
        flags.append(
            _flag_view(
                "uncertain_money",
                (
                    f"Importo da verificare: {row.get('label')} "
                    f"({format_eur(row.get('amount'))})."
                ),
                row.get("evidence_pages"),
            )
        )
    for card in contract.get("risk_cards") or []:
        if card.get("classification") == "uncertain":
            flags.append(
                _flag_view(
                    "compliance_uncertain",
                    f"Conformità non verificabile automaticamente: {card.get('area')}.",
                    card.get("evidence_pages"),
                )
            )
    return flags


_OCCUPANCY_STATUS_LABELS = {
    "occupato": "Occupato",
    "libero": "Libero",
}


def _occupancy_section(contract: Dict[str, Any]) -> Dict[str, Any]:
    """Full occupancy view: status, lease details, opponibility, risks."""
    oc = contract.get("occupancy") or {}
    if not any(
        oc.get(k)
        for k in ("status", "title_info", "opponibility", "registration_dates",
                  "expiry_dates", "risks")
    ):
        return {}
    status = oc.get("status")
    section: Dict[str, Any] = {
        "title": "Stato di occupazione",
        "status": status,
        "status_label": _OCCUPANCY_STATUS_LABELS.get(_norm(status), status),
        "evidence_pages": _pages(oc.get("evidence_pages")),
    }
    if oc.get("title_info"):
        section["title_info"] = oc["title_info"]
    if oc.get("opponibility"):
        section["opponibility"] = oc["opponibility"]
    if oc.get("registration_dates"):
        section["registration_dates"] = [str(d) for d in oc["registration_dates"]]
    if oc.get("expiry_dates"):
        section["expiry_dates"] = [str(d) for d in oc["expiry_dates"]]
    if oc.get("risks"):
        section["risks"] = [str(r) for r in oc["risks"]]
    return section


def _compliance_section(contract: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Every compliance area including conforming ones — nothing disappears."""
    out: List[Dict[str, Any]] = []
    for item in contract.get("compliance_overview") or []:
        view: Dict[str, Any] = {
            "area": item.get("area"),
            "classification": item.get("classification"),
            "status_label": item.get("classification_label")
            or _CLASSIFICATION_LABELS.get(item.get("classification"), "Da verificare"),
            "evidence_pages": _pages(item.get("evidence_pages")),
        }
        if item.get("notes"):
            view["notes"] = item["notes"]
        if item.get("cost") is not None:
            view["cost"] = item["cost"]
            view["cost_display"] = format_eur(item["cost"])
        if item.get("timing"):
            view["timing"] = item["timing"]
        view["blocks_saleability"] = bool(item.get("blocks_saleability"))
        out.append(view)
    return out


def _formalities_section(contract: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Formalità e cancellazioni: never rendered as buyer debt unless explicit.

    Identical rows (same type, description, amount and flags) are shown once."""
    out: List[Dict[str, Any]] = []
    seen: set = set()
    for item in contract.get("legal_formalities") or []:
        row_key = (
            _norm(item.get("type")),
            _norm(item.get("description")),
            item.get("amount"),
            bool(item.get("cancelled_by_procedure")),
            bool(item.get("buyer_burden")),
        )
        if row_key in seen:
            continue
        seen.add(row_key)
        cancelled = bool(item.get("cancelled_by_procedure"))
        buyer = bool(item.get("buyer_burden"))
        if cancelled:
            status_label = "Formalità rilevata; cancellazione indicata a cura della procedura"
        elif buyer:
            status_label = "A carico dell'acquirente secondo la perizia"
        else:
            status_label = "Formalità rilevata; verificare le condizioni di cancellazione"
        view: Dict[str, Any] = {
            "type": item.get("type"),
            "type_label": _formality_type_label(item.get("type")),
            "description": item.get("description"),
            "status_label": status_label,
            "cancelled_by_procedure": cancelled,
            "buyer_burden": buyer,
            "evidence_pages": _pages(item.get("evidence_pages")),
        }
        if item.get("amount") is not None:
            view["amount"] = item["amount"]
            view["amount_display"] = format_eur(item["amount"])
            if not buyer:
                view["amount_note"] = (
                    "Importo della formalità iscritta: non è un debito a carico "
                    "dell'acquirente salvo diversa indicazione della perizia."
                )
        out.append(view)
    return out


def _surfaces_section(contract: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Superfici e dati catastali (deterministic document facts, verbatim)."""
    out: List[Dict[str, Any]] = []
    for fact in contract.get("surface_cadastral") or []:
        view: Dict[str, Any] = {
            "label": fact.get("label"),
            "value": fact.get("value"),
            "evidence_pages": _pages(fact.get("evidence_pages")),
        }
        if fact.get("multiple_values"):
            view["note"] = (
                "In perizia compaiono più valori per questo dato: da verificare."
            )
            view["status"] = "da_verificare"
        out.append(view)
    return out


# ---------------------------------------------------------------------------
# Evidence index: customer view (page + human topic + VERBATIM excerpt) vs
# admin/debug view (raw claim keys). Customers never see raw internal keys.
# ---------------------------------------------------------------------------
EXCERPT_MISSING_NOTE = "Estratto non disponibile automaticamente; verificare pagina {page}."

_BREAK_RE = re.compile(r"[.;!?]\s")


# Single sanctioned excerpt normalization (shared with the verbatim gate).
_normalize_ws = doc_signals.normalize_ws


def _amount_variants(amount: Any) -> List[str]:
    """Italian textual forms of an amount, most specific first.

    Zero amounts return no variants: '0,00' occurs all over a perizia and any
    match would quote an unrelated sentence."""
    try:
        value = float(amount)
    except (TypeError, ValueError):
        return []
    if value == 0:
        return []
    grouped = f"{abs(value):,.2f}".replace(",", "|").replace(".", ",").replace("|", ".")
    ungrouped = f"{abs(value):.2f}".replace(".", ",")
    variants = [grouped]
    if ungrouped != grouped:
        variants.append(ungrouped)
    # Documents often omit the cents ("€ 100.000", "€ 294").
    if value == int(abs(value)) or abs(value) == int(abs(value)):
        int_part = grouped.rsplit(",", 1)[0]
        variants.append(int_part)
        bare = str(int(abs(value)))
        if bare != int_part:
            variants.append(bare)
    return variants


def _find_amount_spans(text_lower: str, variant: str) -> List[int]:
    """Occurrences of an amount variant with digit boundaries: '294' must not
    match inside '1294', '294,50' or '1.294', but a sentence period right
    after the amount ("Importo ipoteca: 150.000.") is a legitimate boundary."""
    pattern = re.compile(
        r"(?<!\d)(?<!\d[.,])" + re.escape(variant.lower()) + r"(?!\d)(?![.,]\d)"
    )
    return [m.start() for m in pattern.finditer(text_lower)]


def _last_break_before(text: str, pos: int, min_gap: int, max_span: int) -> int:
    """Start of the sentence containing ``pos``: last break at least ``min_gap``
    chars before pos (label separators like ': €. ' right before an amount are
    not sentence ends), bounded to ``max_span`` chars."""
    lo = max(0, pos - max_span)
    best = lo
    for m in _BREAK_RE.finditer(text, lo, pos):
        if pos - m.end() >= min_gap:
            best = m.end()
    # Never start mid-word: skip forward to the next word boundary.
    if best > 0 and best < len(text) and text[best - 1] not in " \t":
        space = text.find(" ", best)
        if 0 <= space < pos:
            best = space + 1
    return best


def _excerpt_for_amount(text: str, s: int, e: int) -> str:
    lo = _last_break_before(text, s, min_gap=20, max_span=160)
    return text[lo:e].strip()


def _excerpt_for_needle(text: str, s: int, e: int, max_after: int = 180) -> str:
    m = _BREAK_RE.search(text, e, min(len(text), e + max_after))
    hi = (m.start() + 1) if m else min(len(text), e + max_after)
    return text[s:hi].strip()


def _topic_words(needles: Optional[List[str]], role: Optional[str]) -> set:
    """Content words that anchor an excerpt to ITS topic (label + role label)."""
    words: set = set()
    for needle in needles or []:
        words |= set(re.findall(r"[a-zà-ù]{5,}", _norm(needle)))
    if role:
        words |= set(
            re.findall(r"[a-zà-ù]{5,}", _norm(doc_signals.ROLE_LABELS_IT.get(role, "")))
        )
    return words


def _find_verbatim_excerpt(
    text: str, *, amount: Any = None, needles: Optional[List[str]] = None,
    role: Optional[str] = None
) -> Optional[str]:
    """A short VERBATIM excerpt (whitespace-normalized only) or None.

    ``text`` must already be whitespace-normalized (the caller normalizes each
    page exactly once). Never rewrites, never paraphrases: the returned string
    is a substring of that normalized page text.

    Topic-aware: an excerpt is only returned when it is anchored to ITS claim —
    for amounts, the excerpt must contain a topic/role word (or be the page's
    only occurrence of that amount); the sentence fallback requires (near-)full
    coverage of the topic words, so a verbatim-but-wrong-topic sentence is
    rejected and the entry honestly reports "estratto non disponibile"."""
    if not text:
        return None
    lower = text.lower()
    topic = _topic_words(needles, role)

    occurrences: List[Tuple[int, int]] = []
    for variant in _amount_variants(amount):
        for idx in _find_amount_spans(lower, variant):
            occurrences.append((idx, idx + len(variant)))
        if occurrences:
            break  # variants are ordered most-specific first
    if occurrences:
        best: Optional[Tuple[int, str]] = None
        for s, e in occurrences:
            excerpt = _excerpt_for_amount(text, s, e)
            hits = sum(1 for w in topic if w in _norm(excerpt))
            if hits and (best is None or hits > best[0]):
                best = (hits, excerpt)
        if best:
            return best[1]
        if len(occurrences) == 1:
            # Unique amount on the page: the amount itself is the anchor.
            s, e = occurrences[0]
            return _excerpt_for_amount(text, s, e)
        # Several occurrences, none near the topic: too ambiguous to quote.

    for needle in needles or []:
        clean = _normalize_ws(needle)
        if len(clean) < 10:
            continue
        idx = lower.find(clean.lower())
        if idx >= 0:
            return _excerpt_for_needle(text, idx, idx + len(clean))

    # Sentence fallback: the sentence must cover the topic words (ALL of them
    # for short topics, >= 3/4 for long ones) — never a half-matching sentence.
    best_sentence: Optional[str] = None
    best_score = 0.0
    sentences = re.split(r"(?<=[.!?;])\s+", text)
    for needle in needles or []:
        words = set(re.findall(r"[a-zà-ù]{5,}", _norm(needle)))
        if len(words) < 2:
            continue
        threshold = 1.0 if len(words) <= 3 else 0.75
        for sentence in sentences:
            if len(sentence) < 25:
                continue
            sent_norm = _norm(sentence)
            hits = sum(1 for w in words if w in sent_norm)
            score = hits / len(words)
            if score >= threshold and score > best_score:
                best_sentence, best_score = sentence, score
    if best_sentence:
        return best_sentence[:240].strip()
    return None


def _shorten_excerpt(excerpt: str, limit: int = 240) -> Tuple[str, bool]:
    if len(excerpt) <= limit:
        return excerpt, False
    cut = excerpt[:limit]
    if " " in cut:
        cut = cut.rsplit(" ", 1)[0]
    return cut.strip(), True


def _evidence_sources(contract: Dict[str, Any]) -> List[Dict[str, Any]]:
    """(topic, report_section, evidence_pages, needles, amount) per report item."""
    sources: List[Dict[str, Any]] = []

    def add(topic: Any, section: str, pages: Any, needles: List[Any],
            amount: Any = None) -> None:
        pages_list = _pages(pages)
        topic_text = _normalize_ws(topic)
        if not topic_text or not pages_list:
            return
        role = None
        if amount is not None:
            kind = doc_signals.label_kind(topic_text)
            if kind != "importo_generico":
                role = doc_signals.role_for_kind(kind)
        sources.append({
            "topic": topic_text,
            "report_section": section,
            "evidence_pages": pages_list,
            "needles": [str(n) for n in needles if n],
            "amount": amount,
            "role": role,
        })

    ci = contract.get("case_identity") or {}
    add(
        "Identificazione dell'immobile", "Dati principali", ci.get("evidence_pages"),
        [ci.get("address"), ci.get("property_type")],
    )
    oc = contract.get("occupancy") or {}
    if oc.get("status"):
        add(
            "Stato di occupazione", "Stato di occupazione", oc.get("evidence_pages"),
            [oc.get("title_info"), oc.get("status")],
        )
    for item in contract.get("compliance_overview") or []:
        add(
            item.get("area"), "Conformità e documenti tecnici",
            item.get("evidence_pages"), [item.get("area"), item.get("notes")],
        )
    for row in (contract.get("valuation_chain") or []) + (contract.get("auction_terms") or []):
        add(row.get("label"), "Valori e costi", row.get("evidence_pages"),
            [row.get("label")], row.get("amount"))
    for row in contract.get("buyer_side_costs") or []:
        add(row.get("label"), "Costi a carico dell'acquirente", row.get("evidence_pages"),
            [row.get("label")], row.get("amount"))
    for row in contract.get("procedure_cancelled_formalities") or []:
        add(row.get("label"), "Formalità e cancellazioni", row.get("evidence_pages"),
            [row.get("label")], row.get("amount"))
    for row in contract.get("uncertain_money") or []:
        add(row.get("label"), "Importi e valori di contesto", row.get("evidence_pages"),
            [row.get("label")], row.get("amount"))
    for item in contract.get("legal_formalities") or []:
        add(_formality_type_label(item.get("type")), "Formalità e cancellazioni",
            item.get("evidence_pages"),
            [item.get("description"), item.get("type")], item.get("amount"))
    for fact in contract.get("surface_cadastral") or []:
        label = str(fact.get("label") or "")
        value = str(fact.get("value") or "")
        # "Categoria catastale A/3" is written "categoria A/3" in the document:
        # try label-word + value combinations; numeric values also go through
        # the amount path ("46,95" formats) which recovers the preceding label.
        needles = [f"{label} {value}"]
        for word in re.split(r"[\s/()]+", label):
            if len(word) >= 4:
                needles.append(f"{word} {value}")
        add(label, "Superfici e dati catastali", fact.get("evidence_pages"),
            needles, doc_signals.parse_amount(value))
    return sources


def _build_evidence_views(
    contract: Dict[str, Any], pages: Optional[List[Dict[str, Any]]]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """(customer_evidence_index, admin_evidence_index).

    Customer entries carry page + human topic + verbatim perizia excerpt (from
    the extracted page text, whitespace-normalized only). When no safe excerpt
    exists the entry says so explicitly and is flagged as a coverage warning by
    the quality gate. Raw claim keys live ONLY in the admin view."""
    # Each page is whitespace-normalized exactly once, up front.
    page_texts: Dict[int, str] = {}
    for page in pages or []:
        pnum = doc_signals.page_number(page)
        if pnum is not None:
            page_texts[pnum] = _normalize_ws(page.get("text"))

    customer: List[Dict[str, Any]] = []
    seen: set = set()
    for source in _evidence_sources(contract):
        excerpt: Optional[str] = None
        excerpt_page: Optional[int] = None
        for pnum in source["evidence_pages"]:
            text = page_texts.get(pnum)
            if not text:
                continue
            excerpt = _find_verbatim_excerpt(
                text, amount=source["amount"], needles=source["needles"],
                role=source.get("role"),
            )
            if excerpt:
                excerpt_page = pnum
                break
        page = excerpt_page if excerpt_page is not None else source["evidence_pages"][0]
        key = (page, _norm(source["topic"]))
        if key in seen:
            continue
        seen.add(key)
        entry: Dict[str, Any] = {
            "page": page,
            "topic": source["topic"],
            "report_section": source["report_section"],
        }
        if excerpt:
            short, truncated = _shorten_excerpt(excerpt)
            entry["perizia_excerpt"] = short
            entry["excerpt_truncated"] = truncated
            entry["coverage_status"] = "covered"
        else:
            entry["perizia_excerpt"] = None
            entry["note"] = EXCERPT_MISSING_NOTE.format(page=page)
            entry["coverage_status"] = "excerpt_missing"
        customer.append(entry)
    customer.sort(key=lambda e: (e["page"], _norm(e["topic"])))

    admin: List[Dict[str, Any]] = []
    for page_key, refs in (contract.get("evidence_index") or {}).items():
        try:
            page = int(page_key)
        except (TypeError, ValueError):
            continue
        admin.append({
            "page": page,
            "raw_keys": list(refs or []),
            "artifact_source": "verified_report_contract.json",
        })
    admin.sort(key=lambda e: e["page"])
    return customer, admin


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
def render_success_report(
    contract: Dict[str, Any], pages: Optional[List[Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """Render the customer report for a validated single-lot contract.

    ``pages`` (input_pages of THIS analysis) is optional and used only for
    document-truth lookups: verbatim customer evidence excerpts and accessory
    detection. No fact is ever invented from it."""
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
    report["beni_sections"] = _beni_sections(contract, pages)
    report["occupancy_section"] = _occupancy_section(contract)
    report["compliance_section"] = _compliance_section(contract)
    report["formalities_section"] = _formalities_section(contract)
    report["surfaces_section"] = _surfaces_section(contract)
    report["buyer_checklist"] = _buyer_checklist(contract)
    report["manual_review_flags"] = _manual_review_flags(contract)
    report["evidence_index"] = _evidence_index(contract)
    customer_evidence, admin_evidence = _build_evidence_views(contract, pages)
    report["customer_evidence_index"] = customer_evidence
    report["admin_evidence_index"] = admin_evidence
    report["sections_meta"] = {
        "uncertain_money_title": UNCERTAIN_MONEY_TITLE,
        "market_comparatives_title": MARKET_COMPARATIVES_TITLE,
        "context_values_title": CONTEXT_VALUES_TITLE,
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
