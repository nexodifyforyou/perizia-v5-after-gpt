"""
Deterministic customer DECISION MODEL — a read-time projection of the stored
``customer_report.json`` into a calm, decision-oriented information architecture.

This module is a PURE function layer. It:
  * never calls OpenAI, never touches the network, never reads the PDF;
  * never mutates its input;
  * never fabricates a fact — every value comes either from the stored artifact
    or from the fixed Italian string tables in this module;
  * omits any section/finding it cannot support (no empty "0 beni" cards).

It is invoked by ``customer_view.sanitize_customer_report`` (the single customer
choke point) and attached as ``decision_model`` on the sanitized payload. User
confirmations (persisted authoritatively in MongoDB — see ``user_confirmations``)
are passed in as plain data and joined here at read time; they NEVER rewrite an
artifact and NEVER weaken a validator failure.

See docs/customer_report_decision_workflow_plan.md (§§C–M).
"""

from __future__ import annotations

import hashlib
import unicodedata
from itertools import combinations
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .contract import _area_token  # canonical compliance/risk area tokenizer (reused)

SCHEMA_VERSION = "cv2.customer_decision.v1"

# Money tolerances — mirror contract._approx_equal so the presentation-only
# chain reorder agrees with the validator's own arithmetic.
_MONEY_ABS_TOL = 1.0
_MONEY_REL_TOL = 0.01

# Section codes for stable finding ids.
_SECTION_CODE = {
    "acquisto": "acq",
    "numeri": "mon",
    "occupazione": "occ",
    "verifiche": "ver",
    "conformita": "cmp",
    "formalita": "frm",
    "altri": "alt",
}

# Priority classes (§E rule 7) → severity int (lower = more important).
_SEV_FINAL_VALUE = 1
_SEV_OCCUPANCY = 2
_SEV_TECH_ACTION = 3
_SEV_BUYER_COST = 4
_SEV_UNCERTAIN = 5
_SEV_CONFORMITY = 6
_SEV_CONTEXT = 7

# ---------------------------------------------------------------------------
# Fixed Italian string tables (never free text; never internal codes)
# ---------------------------------------------------------------------------
_ESITO_WORDING = {
    "verde": {
        "headline": "Nessuna verifica bloccante emersa dalla perizia",
        "sentence": (
            "Il report non rileva elementi bloccanti tra quelli espressamente "
            "indicati nella perizia. Restano consigliate le verifiche ordinarie "
            "prima di procedere."
        ),
    },
    "ambra": {
        "headline": "Verifiche necessarie prima di procedere",
        "sentence": (
            "La perizia segnala alcuni aspetti da controllare prima di procedere; "
            "sono riportati qui sotto con la pagina di riferimento."
        ),
    },
    "rosso": {
        "headline": "Verifica tecnica richiesta",
        "sentence": (
            "Non è stato possibile produrre un report cliente affidabile da questa "
            "perizia: è necessaria una verifica tecnica prima di procedere."
        ),
    },
}

# classification token → (status token, customer label, tone)
_CLASSIFICATION_STATUS = {
    "conforming": ("conforme", "Conforme secondo la perizia", "verde"),
    "regularizable": ("regolarizzabile", "Regolarizzabile secondo la perizia", "ambra"),
    "non_conforming": ("non_conforme", "Non conforme secondo la perizia", "ambra"),
    "not_regularizable": ("non_conforme", "Non conforme secondo la perizia", "ambra"),
    "uncertain": ("non_verificato", "Non verificato o non dichiarato", "slate"),
}
_DEFAULT_STATUS = ("non_verificato", "Non verificato o non dichiarato", "slate")

# area token → customer group title
_AREA_GROUP_LABEL = {
    "edilizia": "Edilizia",
    "catastale": "Catastale",
    "urbanistica": "Urbanistica",
    "corrispondenza": "Corrispondenza catastale/atto",
    "impianto_gas": "Impianti — gas",
    "impianto_elettrico": "Impianti — elettrico",
    "impianti": "Impianti",
    "agibilita": "Agibilità/abitabilità",
    "ape": "APE/energia",
}

# per (area group) → "Perché conta" / "Cosa fare" templates
_CONFORMITY_WHY = {
    "edilizia": "Le difformità edilizie possono comportare costi e pratiche di regolarizzazione a carico dell'acquirente.",
    "catastale": "Le difformità catastali vanno allineate prima o dopo l'acquisto, con costi e tempi dedicati.",
    "urbanistica": "La conformità urbanistica incide sulla commerciabilità e su eventuali sanatorie.",
    "corrispondenza": "La corrispondenza tra stato di fatto, catasto e atto evita contestazioni sull'immobile.",
    "impianto_gas": "Un impianto non a norma richiede adeguamento e certificazione prima dell'uso.",
    "impianto_elettrico": "Un impianto non a norma richiede adeguamento e certificazione prima dell'uso.",
    "impianti": "Gli impianti non a norma richiedono adeguamento e certificazione prima dell'uso.",
    "agibilita": "L'agibilità incide sull'utilizzo e sul valore dell'immobile.",
    "ape": "La certificazione energetica è documentazione richiesta per la vendita.",
}
_CONFORMITY_WHY_DEFAULT = "L'aspetto tecnico indicato può richiedere verifiche o costi prima di procedere."

_READINESS_LABEL = {
    "TECHNICAL_REVIEW_REQUIRED": "Verifica tecnica richiesta",
    "CONFIRMATIONS_REQUIRED": "Conferme necessarie",
    "READY_FOR_REVIEW": "Verifiche completate",
    "COMPLETE_FOR_EXPORT": "Pronto per l'esportazione",
}

# Customer-safe non-READY statuses are interactive prompts (pick a lot, confirm a
# money role, upload a readable PDF) — NOT fail-closed. Only a status OUTSIDE the
# customer-safe set is a genuine fail-closed report that warrants the red esito.
_SAFE_NON_READY_STATUSES = frozenset(
    {"MONEY_CONFIRMATION_REQUIRED", "LOT_SELECTION_REQUIRED", "DOCUMENT_NOT_READABLE"}
)
_CUSTOMER_SAFE_STATUSES = frozenset({"REPORT_READY"}) | _SAFE_NON_READY_STATUSES

# Deterministic confirmation option sets, keyed by finding class.
_CONFIRM_OPTIONS = {
    "money_role": [
        {"option_id": "costo_acquirente", "label": "È un costo che dovrò sostenere io"},
        {"option_id": "gia_compreso", "label": "È già compreso nei valori indicati"},
        {"option_id": "solo_informativo", "label": "È solo un dato informativo"},
    ],
    "occupancy": [
        {"option_id": "occupato_opponibile", "label": "Occupato con contratto opponibile"},
        {"option_id": "occupato_da_verificare", "label": "Occupato con contratto da verificare"},
        {"option_id": "libero", "label": "L'immobile è libero"},
    ],
    "conformity": [
        {"option_id": "disponibile", "label": "La documentazione indicata è disponibile"},
        {"option_id": "non_disponibile", "label": "Non è disponibile: servirà una verifica tecnica"},
    ],
    "formality": [
        {"option_id": "cancellata_procedura", "label": "La cancellazione è a cura della procedura"},
        {"option_id": "costo_acquirente", "label": "Il costo resta a mio carico"},
        {"option_id": "non_indicato", "label": "Non è indicato"},
    ],
}
_UNSURE_OPTION = {"option_id": "non_sicuro", "label": "Non sono sicuro"}

# Global cap on how many confirmation panels a report may offer (§K rule 4).
MAX_ELIGIBLE_CONFIRMATIONS = 5


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def _norm(text: Any) -> str:
    stripped = "".join(
        c
        for c in unicodedata.normalize("NFKD", str(text or ""))
        if not unicodedata.combining(c)
    )
    return stripped.lower().strip()


def _as_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _pages(value: Any) -> List[int]:
    out: List[int] = []
    for p in value or []:
        try:
            out.append(int(p))
        except (TypeError, ValueError):
            continue
    return out


def format_eur(amount: Any) -> Optional[str]:
    value = _as_float(amount)
    if value is None:
        return None
    whole = f"{value:,.2f}".replace(",", "§").replace(".", ",").replace("§", ".")
    return f"€ {whole}"


def _approx(a: Optional[float], b: Optional[float]) -> bool:
    if a is None or b is None:
        return False
    return abs(a - b) <= max(_MONEY_ABS_TOL, _MONEY_REL_TOL * max(abs(a), abs(b)))


def _first_sentence(text: Any, limit: int = 260) -> str:
    raw = " ".join(str(text or "").split())
    if not raw:
        return ""
    if len(raw) <= limit:
        return raw
    cut = raw[:limit]
    dot = cut.rfind(". ")
    if dot >= 60:
        return cut[: dot + 1]
    return cut.rstrip() + "…"


def evidence_hash(excerpt: Any) -> str:
    """Stable hash of a verbatim excerpt — detects stale confirmations after a rerun."""
    normalized = " ".join(str(excerpt or "").split())
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()


def _finding_id(
    section: str, topic: str, lot_id: Optional[str], page: Optional[int], amount: Optional[float]
) -> str:
    code = _SECTION_CODE.get(section, "fnd")
    parts = [
        section,
        topic or "-",
        str(lot_id) if lot_id not in (None, "") else "-",
        str(page) if page is not None else "-",
        f"{amount:.2f}" if amount is not None else "-",
    ]
    digest = hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:12]
    return f"{code}-{digest}"


# ---------------------------------------------------------------------------
# Evidence index
# ---------------------------------------------------------------------------
def _evidence_lookup(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [e for e in (report.get("customer_evidence_index") or []) if isinstance(e, dict)]


def _find_excerpt(
    evidence: Sequence[Dict[str, Any]], pages: Iterable[int], topic_keywords: Sequence[str]
) -> Optional[Dict[str, Any]]:
    """Return a covered verbatim excerpt on one of ``pages`` whose topic matches.

    The customer_evidence_index is already verbatim-gated upstream; here we only
    SELECT — never rewrite. Number-only / topically-wrong entries are avoided by
    requiring a topic-keyword match.
    """
    page_set = set(_pages(pages))
    keys = [k for k in topic_keywords if k]
    for entry in evidence:
        if entry.get("coverage_status") != "covered":
            continue
        if not entry.get("perizia_excerpt"):
            continue
        try:
            page = int(entry.get("page"))
        except (TypeError, ValueError):
            continue
        if page_set and page not in page_set:
            continue
        topic_l = _norm(entry.get("topic")) + " " + _norm(entry.get("report_section"))
        if keys and not any(k in topic_l for k in keys):
            continue
        return {"page": page, "excerpt": str(entry.get("perizia_excerpt")), "verbatim": True}
    return None


def _canonical_page(evidence_pages: Iterable[int], excerpt: Optional[Dict[str, Any]]) -> Optional[int]:
    if excerpt and excerpt.get("page") is not None:
        return int(excerpt["page"])
    pgs = _pages(evidence_pages)
    return pgs[0] if pgs else None


# ---------------------------------------------------------------------------
# §2 COSA STAI ACQUISTANDO
# ---------------------------------------------------------------------------
def _build_acquisto(report: Dict[str, Any], lot_id: Optional[str]) -> Optional[Dict[str, Any]]:
    ci = report.get("case_identity") or {}
    beni = [b for b in (report.get("beni_sections") or []) if isinstance(b, dict)]
    occ = report.get("occupancy_section") or {}
    has_identity = any(ci.get(k) for k in ("tribunale", "procedura_rge", "address", "property_type"))
    if not has_identity and not beni:
        return None

    identity = {
        "tribunale": ci.get("tribunale"),
        "procedura_rge": ci.get("procedura_rge"),
        "lotto": ci.get("lotto"),
        "indirizzo": ci.get("address"),
        "tipologia": ci.get("property_type"),
        "diritto_quota": ci.get("ownership_right"),
        "pagine": _pages(ci.get("evidence_pages")),
    }
    # Selected lot only when it adds information beyond case_identity.lotto.
    lot_struct = report.get("lot_structure") or {}
    selected = lot_struct.get("selected_lot")
    identity_lot = _norm(ci.get("lotto"))
    if selected and _norm(f"lotto {selected}") not in identity_lot and str(selected) not in identity_lot:
        identity["lotto_selezionato"] = selected

    beni_out: List[Dict[str, Any]] = []
    for b in beni:
        beni_out.append(
            {
                "titolo": b.get("title"),
                "tipologia": b.get("property_type"),
                "indirizzo": b.get("address"),
                "principale": bool(b.get("is_main_property")),
                "pertinenze": [
                    {"label": a.get("label"), "nota": a.get("note"), "pagine": _pages(a.get("evidence_pages"))}
                    for a in (b.get("accessories") or [])
                    if isinstance(a, dict) and a.get("label")
                ],
                "pagine": _pages(b.get("evidence_pages")),
            }
        )

    return {
        "identity": identity,
        "beni": beni_out,
        "occupazione_sintesi": occ.get("status_label"),
    }


# ---------------------------------------------------------------------------
# §3 NUMERI PRINCIPALI — value chain (canonical reorder), buyer costs, scenari,
#     da_chiarire, comparatives summary
# ---------------------------------------------------------------------------
def _chain_row_view(row: Dict[str, Any], *, canonical_page: Optional[int] = None) -> Dict[str, Any]:
    return {
        "label": row.get("label"),
        "amount": _as_float(row.get("amount")),
        "amount_display": row.get("amount_display") or format_eur(row.get("amount")),
        "kind": row.get("kind"),
        "pagina": canonical_page,
    }


def _reorder_chain(rows: Sequence[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Canonical, role-based, presentation-only reorder (§F).

    Returns (canonical_rows, ambiguous_deductions). Amounts/roles are untouched.
    Anchors are ``value`` rows in document order; each ``deduction`` is placed in
    the segment whose subtraction it satisfies within tolerance. Deductions that
    fit no segment are NOT force-inserted — they are returned as ambiguous (they
    become "Importi da chiarire", §F rule 1).
    """
    rows = [r for r in rows if isinstance(r, dict)]
    anchors = [r for r in rows if r.get("kind") == "value" and _as_float(r.get("amount")) is not None]
    deductions = [r for r in rows if r.get("kind") == "deduction" and _as_float(r.get("amount")) is not None]
    other = [r for r in rows if r.get("kind") not in ("value", "deduction")]

    # Not enough structure to reason about → passthrough (document order).
    if len(anchors) < 2 or not deductions:
        return list(rows), []

    remaining = list(deductions)
    segments: List[List[Dict[str, Any]]] = []
    for i in range(len(anchors) - 1):
        top = _as_float(anchors[i]["amount"])
        bottom = _as_float(anchors[i + 1]["amount"])
        needed = (top or 0.0) - (bottom or 0.0)
        placed: List[Dict[str, Any]] = []
        if needed > 0 and remaining:
            found = _subset_matching(remaining, needed)
            if found:
                placed = found
                for d in found:
                    remaining.remove(d)
        segments.append(placed)

    # Rebuild in canonical order: anchor, its segment deductions, next anchor…
    ordered: List[Dict[str, Any]] = []
    for i, anchor in enumerate(anchors):
        ordered.append(anchor)
        if i < len(segments):
            ordered.extend(segments[i])
    # Any non-value/deduction rows keep their relative tail position.
    ordered.extend(other)
    # If nothing could be placed, fall back to original document order and keep
    # EVERY row in the chain — nothing is excluded, so nothing is ambiguous
    # (a pure passthrough must not also flag its own rows as "da chiarire").
    if all(not seg for seg in segments):
        return list(rows), []
    return ordered, remaining


def _subset_matching(rows: List[Dict[str, Any]], needed: float) -> Optional[List[Dict[str, Any]]]:
    """Smallest subset of ``rows`` whose amounts sum to ``needed`` within tol."""
    for size in range(1, min(len(rows), 3) + 1):
        for combo in combinations(rows, size):
            total = sum(_as_float(r.get("amount")) or 0.0 for r in combo)
            if _approx(total, needed):
                return list(combo)
    return None


def _build_numeri(report: Dict[str, Any], full_money: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    money = report.get("money_sections") or {}
    chain_src = [r for r in (money.get("valuation_chain") or []) if isinstance(r, dict)]
    buyer_src = [r for r in (money.get("buyer_side_costs") or []) if isinstance(r, dict)]
    uncertain_src = [r for r in (money.get("uncertain_money") or []) if isinstance(r, dict)]
    auction_src = [r for r in (money.get("auction_terms") or []) if isinstance(r, dict)]

    if not (chain_src or buyer_src or uncertain_src):
        return None

    canonical, ambiguous = _reorder_chain(chain_src)
    catena = [_chain_row_view(r) for r in canonical]
    # Mark the terminal value row for gold emphasis.
    last_value_idx = None
    for i, r in enumerate(catena):
        if r.get("kind") == "value":
            last_value_idx = i
    if last_value_idx is not None:
        catena[last_value_idx]["terminal"] = True

    costi = []
    for r in buyer_src:
        included = bool(r.get("included_in_valuation"))
        costi.append(
            {
                "label": r.get("label"),
                "amount_display": r.get("amount_display") or format_eur(r.get("amount")),
                "included_in_valuation": included,
                "nota": "Già considerato nel valore finale: non sommare nuovamente." if included else r.get("notes"),
            }
        )

    # "Importi da chiarire" carries ONLY the chain-excluded ambiguous deductions
    # (a value stated in the perizia that does not fit the valuation arithmetic).
    # Genuinely uncertain money rows (uncertain_money) surface separately as
    # confirmation-eligible findings, so they are NOT duplicated here.
    da_chiarire = []
    for r in ambiguous:
        da_chiarire.append(
            {
                "label": r.get("label"),
                "amount_display": r.get("amount_display") or format_eur(r.get("amount")),
                "motivo": r.get("reason")
                or "Importo indicato nella perizia il cui punto nella catena di valore non è determinato con certezza.",
            }
        )

    # Comparatives: one collapsed line only (count + pages) from the FULL report.
    comparatives = [c for c in (full_money.get("market_comparatives") or []) if isinstance(c, dict)]
    comparatives_summary = None
    if comparatives:
        pgs = sorted({p for c in comparatives for p in _pages(c.get("evidence_pages"))})
        comparatives_summary = {"count": len(comparatives), "pages": pgs}

    auction = None
    if auction_src:
        a0 = auction_src[0]
        auction = {
            "label": a0.get("label"),
            "amount_display": a0.get("amount_display") or format_eur(a0.get("amount")),
            "nota": a0.get("notes"),
        }

    out: Dict[str, Any] = {"catena": catena}
    if costi:
        out["costi_potenziali"] = costi
    if da_chiarire:
        out["da_chiarire"] = da_chiarire
    if comparatives_summary:
        out["comparatives_summary"] = comparatives_summary
    if auction:
        out["auction"] = auction
    # Original extracted order preserved for Vista admin (§F rule 1b).
    out["catena_ordine_originale"] = [_chain_row_view(r) for r in chain_src]
    return out


def _build_money_findings(
    report: Dict[str, Any], evidence: Sequence[Dict[str, Any]], lot_id: Optional[str]
) -> List[Dict[str, Any]]:
    """Confirmation-eligible findings for genuinely uncertain money rows (§K money-role)."""
    money = report.get("money_sections") or {}
    findings: List[Dict[str, Any]] = []
    for row in money.get("uncertain_money") or []:
        if not isinstance(row, dict):
            continue
        pages = _pages(row.get("evidence_pages"))
        amount = _as_float(row.get("amount"))
        label = str(row.get("label") or "")
        excerpt = _find_excerpt(evidence, pages, tuple(_norm(label).split()[:2]) + ("importo", "costo"))
        page = _canonical_page(pages, excerpt)
        fid = _finding_id("numeri", _norm(label) or "importo", lot_id, page, amount)
        findings.append(
            {
                "finding_id": fid,
                "section": "numeri",
                "topic": _norm(label) or "importo",
                "title": label or "Importo da chiarire",
                "status": "da_verificare",
                "status_label": "Da chiarire",
                "tone": "ambra",
                "severity": _SEV_UNCERTAIN,
                "customer_summary": row.get("reason"),
                "buyer_impact": "Il ruolo di questo importo (costo, dato informativo o scenario) non è indicato con chiarezza.",
                "recommended_action": "Chiarire il ruolo dell'importo con il delegato o un professionista.",
                "amount": amount,
                "amount_display": row.get("amount_display") or format_eur(amount),
                "included_in_valuation": False,
                "pages": pages,
                "page": page,
                "evidence": excerpt,
                "blocking": bool(row.get("blocks_saleability")),
                "confirm_class": "money_role",
            }
        )
    return findings


# ---------------------------------------------------------------------------
# §4 STATO DI OCCUPAZIONE
# ---------------------------------------------------------------------------
def _occupancy_why(status: str) -> str:
    s = _norm(status)
    if "occupat" in s:
        return (
            "Se l'immobile è occupato, i tempi e le modalità di rilascio e "
            "l'eventuale opponibilità del contratto incidono sull'acquisto."
        )
    if "liber" in s:
        return "Un immobile libero non presenta vincoli di rilascio verso terzi occupanti."
    return "Lo stato di occupazione incide su tempi e modalità di disponibilità dell'immobile."


def _build_occupancy(report: Dict[str, Any], evidence: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    occ = report.get("occupancy_section") or {}
    if not (occ.get("status") or occ.get("status_label") or occ.get("title_info")):
        return None
    status_label = occ.get("status_label") or occ.get("status")
    cosa_verificare: List[str] = []
    for risk in occ.get("risks") or []:
        if str(risk).strip():
            cosa_verificare.append(str(risk).strip())
    is_occupied = "occupat" in _norm(status_label)
    if is_occupied and not occ.get("opponibility"):
        cosa_verificare.insert(
            0,
            "Opponibilità del titolo da verificare: la perizia non si esprime espressamente.",
        )
    pages = _pages(occ.get("evidence_pages"))
    excerpt = _find_excerpt(evidence, pages, ("occupa", "contratt", "locazione", "conduttore", "affitt"))
    return {
        "stato": status_label,
        "dettaglio": occ.get("title_info"),
        "perche_conta": _occupancy_why(status_label),
        "cosa_verificare": cosa_verificare[:4],
        "date_registrazione": list(occ.get("registration_dates") or []),
        "date_scadenza": list(occ.get("expiry_dates") or []),
        "pagina": _canonical_page(pages, excerpt),
        "pagine": pages,
        "evidence": excerpt,
        "blocking": bool(occ.get("blocks_saleability")),
    }


# ---------------------------------------------------------------------------
# §6 CONFORMITÀ E DOCUMENTI TECNICI  (also feeds findings)
# ---------------------------------------------------------------------------
def _build_conformity_findings(
    report: Dict[str, Any], evidence: Sequence[Dict[str, Any]], lot_id: Optional[str]
) -> List[Dict[str, Any]]:
    findings: List[Dict[str, Any]] = []
    for item in report.get("compliance_section") or []:
        if not isinstance(item, dict):
            continue
        area = item.get("area")
        token = _area_token(area)
        cls = _norm(item.get("classification"))
        status, status_label, tone = _CLASSIFICATION_STATUS.get(cls, _DEFAULT_STATUS)
        pages = _pages(item.get("evidence_pages"))
        group = _AREA_GROUP_LABEL.get(token, str(area or "").strip().capitalize() or "Altro")
        excerpt = _find_excerpt(evidence, pages, (token, _norm(area), "conform"))
        page = _canonical_page(pages, excerpt)
        amount = _as_float(item.get("cost"))
        fid = _finding_id("conformita", token, lot_id, page, amount)
        blocking = bool(item.get("blocks_saleability")) or status == "non_conforme"
        findings.append(
            {
                "finding_id": fid,
                "section": "conformita",
                "topic": token,
                "group": group,
                "title": group,
                "status": status,
                "status_label": status_label,
                "tone": tone,
                "blocking": blocking,
                "severity": _SEV_CONFORMITY if status == "conforme" else _SEV_TECH_ACTION,
                "customer_summary": _first_sentence(item.get("notes")),
                "buyer_impact": _CONFORMITY_WHY.get(token, _CONFORMITY_WHY_DEFAULT),
                "recommended_action": (
                    "Verificare/regolarizzare secondo quanto indicato nella perizia."
                    if status in ("regolarizzabile", "non_conforme")
                    else "Nessuna azione richiesta secondo la perizia."
                ),
                "amount": amount,
                "amount_display": item.get("cost_display") or format_eur(amount),
                "included_in_valuation": False,
                "timing": item.get("timing"),
                "pages": pages,
                "page": page,
                "evidence": excerpt,
            }
        )
    return findings


def _build_conformita_section(findings: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    groups: Dict[str, Dict[str, Any]] = {}
    for f in findings:
        if f["section"] != "conformita":
            continue
        g = groups.setdefault(f["group"], {"group": f["group"], "items": []})
        g["items"].append(f["finding_id"])
    if not groups:
        return None
    return {"groups": list(groups.values())}


# ---------------------------------------------------------------------------
# §7 FORMALITÀ E CANCELLAZIONI
# ---------------------------------------------------------------------------
_CANCELLED_SENTENCE = "Formalità indicata come cancellata a cura della procedura."
_CANCELLED_NOTE = (
    "L'importo iscritto non è un debito da sommare al prezzo, salvo diversa "
    "indicazione espressa nella perizia."
)


def _build_formalita(report: Dict[str, Any], lot_id: Optional[str]) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
    rows = [f for f in (report.get("formalities_section") or []) if isinstance(f, dict)]
    if not rows:
        return None, []

    # Dedup by (type, amount, cancelled, buyer) — descriptions become detail lines.
    grouped: Dict[Tuple, Dict[str, Any]] = {}
    for r in rows:
        amount = _as_float(r.get("amount"))
        key = (
            _norm(r.get("type")),
            round(amount, 2) if amount is not None else None,
            bool(r.get("cancelled_by_procedure")),
            bool(r.get("buyer_burden")),
        )
        card = grouped.get(key)
        if card is None:
            card = {
                "type": r.get("type"),
                "type_label": r.get("type_label") or (r.get("type") or "").capitalize(),
                "cancelled_by_procedure": bool(r.get("cancelled_by_procedure")),
                "buyer_burden": bool(r.get("buyer_burden")),
                "amount_display": r.get("amount_display") or format_eur(amount),
                "amount_note": r.get("amount_note"),
                "details": [],
                "pages": [],
            }
            grouped[key] = card
        if r.get("description"):
            card["details"].append(r["description"])
        card["pages"] = sorted(set(card["pages"]) | set(_pages(r.get("evidence_pages"))))

    cancellate: List[Dict[str, Any]] = []
    costi: List[Dict[str, Any]] = []
    da_verificare: List[Dict[str, Any]] = []
    findings: List[Dict[str, Any]] = []
    for card in grouped.values():
        view = {
            "type_label": card["type_label"],
            "amount_display": card["amount_display"],
            "amount_note": card["amount_note"],
            "details": card["details"],
            "pages": card["pages"],
        }
        if card["cancelled_by_procedure"] and not card["buyer_burden"]:
            view["statement"] = _CANCELLED_SENTENCE
            view["note"] = _CANCELLED_NOTE
            cancellate.append(view)
        elif card["buyer_burden"]:
            costi.append(view)
        else:
            # Unclear treatment → verify + confirmation-eligible finding.
            page = card["pages"][0] if card["pages"] else None
            fid = _finding_id("formalita", _norm(card["type"]), lot_id, page, None)
            view["finding_id"] = fid
            da_verificare.append(view)
            findings.append(
                {
                    "finding_id": fid,
                    "section": "formalita",
                    "topic": _norm(card["type"]) or "formalita",
                    "title": card["type_label"],
                    "status": "da_verificare",
                    "status_label": "Formalità da verificare",
                    "tone": "ambra",
                    "severity": _SEV_TECH_ACTION,
                    "customer_summary": card["details"][0] if card["details"] else card["type_label"],
                    "buyer_impact": "Il trattamento di cancellazione o l'eventuale onere per l'acquirente non è indicato con chiarezza.",
                    "recommended_action": "Verificare le condizioni di cancellazione con il delegato o un professionista.",
                    "amount": None,
                    "pages": card["pages"],
                    "page": page,
                    "evidence": None,
                    "blocking": False,
                    "confirm_class": "formality",
                }
            )

    section = {}
    if cancellate:
        section["cancellate"] = cancellate
    if costi:
        section["costi_cancellazione"] = costi
    if da_verificare:
        section["da_verificare"] = da_verificare
    return (section or None), findings


# ---------------------------------------------------------------------------
# §5 COSA VERIFICARE — checklist findings (dedup vs compliance/formalita)
# ---------------------------------------------------------------------------
def _build_verifiche(
    report: Dict[str, Any],
    conformity_findings: List[Dict[str, Any]],
    formality_findings: List[Dict[str, Any]],
    occupancy_section: Optional[Dict[str, Any]],
    numeri: Optional[Dict[str, Any]],
    lot_id: Optional[str],
) -> Optional[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []

    # 1. Occupancy/title first (§E priority). Linked to the occupancy finding by
    #    finding_id so its state is reconciled after a confirmation is applied.
    if occupancy_section and occupancy_section.get("cosa_verificare"):
        items.append(
            {
                "title": "Verificare la situazione di occupazione e l'opponibilità del titolo",
                "why": occupancy_section["cosa_verificare"][0],
                "status": "da_verificare",
                "page": occupancy_section.get("pagina"),
                "link": "occupazione",
                "finding_id": _finding_id(
                    "occupazione", "occupazione", lot_id, occupancy_section.get("pagina"), None
                ),
                "severity": _SEV_OCCUPANCY,
            }
        )

    # 2. Chain-excluded ambiguous amounts (informational; not confirmation-driven).
    if numeri and numeri.get("da_chiarire"):
        items.append(
            {
                "title": "Chiarire alcuni importi indicati nella perizia",
                "why": "Alcuni importi non hanno un ruolo chiaro nella catena di valore.",
                "status": "da_verificare",
                "page": None,
                "link": "numeri",
                "severity": _SEV_FINAL_VALUE,
            }
        )

    # 3. Technical action items (regularizable / non-conforming) — link, don't repeat.
    for f in conformity_findings:
        if f["status"] in ("regolarizzabile", "non_conforme"):
            items.append(
                {
                    "title": f"Verificare/regolarizzare: {f['title'].lower()}",
                    "why": f["buyer_impact"],
                    "status": "da_verificare",
                    "page": f.get("page"),
                    "link": "conformita",
                    "finding_id": f["finding_id"],
                    "severity": _SEV_TECH_ACTION,
                }
            )

    # 5. Formalities with unclear treatment.
    for f in formality_findings:
        items.append(
            {
                "title": f"Verificare la formalità: {f['title'].lower()}",
                "why": f["buyer_impact"],
                "status": "da_verificare",
                "page": f.get("page"),
                "link": "formalita",
                "finding_id": f["finding_id"],
                "severity": _SEV_TECH_ACTION,
            }
        )

    if not items:
        return None
    items.sort(key=lambda it: it.get("severity", 9))
    return {"items": items[:8], "total": len(items)}


# Checklist status taxonomy (customer Italian) + which statuses count as OPEN.
_CHECKLIST_STATUS_LABEL = {
    "da_verificare": "Da verificare",
    "conferma_necessaria": "Conferma necessaria",
    "verifica_tecnica_richiesta": "Verifica tecnica richiesta",
    "confermato_utente": "Confermato dall'utente",
    "completato": "Completato",
}
_OPEN_CHECKLIST_STATUSES = frozenset(
    {"da_verificare", "conferma_necessaria", "verifica_tecnica_richiesta"}
)


def _checklist_status_from_finding(finding: Dict[str, Any]) -> str:
    """Map a (post-confirmation) finding to its checklist status."""
    status = finding.get("status")
    if status == "confermato_utente":
        return "confermato_utente"
    if status == "verifica_tecnica_richiesta":
        return "verifica_tecnica_richiesta"
    if status == "completato":
        return "completato"
    if finding.get("confirmation"):
        return "conferma_necessaria"
    return "da_verificare"


def _reconcile_verifiche(
    verifiche: Optional[Dict[str, Any]], findings: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Reconcile the checklist AGAINST the findings AFTER confirmations are applied.

    finding_id is the authoritative link: a checklist item's state is recomputed
    from its finding so the checklist can never contradict the detailed card (e.g.
    show "Da verificare" for a finding the user already confirmed). The canonical
    item is updated in place — no second row is ever created. Adds open/completed
    counts so the readiness summary and the checklist agree.
    """
    if not verifiche:
        return verifiche
    by_id = {f["finding_id"]: f for f in findings}
    open_count = 0
    completed_count = 0
    for item in verifiche.get("items") or []:
        fid = item.get("finding_id")
        if fid and fid in by_id:
            item["status"] = _checklist_status_from_finding(by_id[fid])
        item["status_label"] = _CHECKLIST_STATUS_LABEL.get(item.get("status"), "Da verificare")
        if item.get("status") in _OPEN_CHECKLIST_STATUSES:
            open_count += 1
        else:
            completed_count += 1
    verifiche["open_count"] = open_count
    verifiche["completed_count"] = completed_count
    return verifiche


# ---------------------------------------------------------------------------
# §8 ALTRI ELEMENTI DA CONOSCERE
# ---------------------------------------------------------------------------
def _build_altri(report: Dict[str, Any], used_area_tokens: set) -> Optional[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    seen: set = set()
    for section in report.get("risk_sections") or []:
        for item in (section.get("items") or []):
            if not isinstance(item, dict):
                continue
            token = _area_token(item.get("area"))
            if token in used_area_tokens:
                continue  # already a conformity/other finding
            summary = _first_sentence(item.get("summary"))
            key = _norm(summary)[:80]
            if not summary or key in seen:
                continue
            seen.add(key)
            items.append(
                {
                    "title": (item.get("area") or "").strip().capitalize() or "Elemento",
                    "summary": summary,
                    "pages": _pages(item.get("evidence_pages")),
                }
            )
    if not items:
        return None
    return {"items": items}


# ---------------------------------------------------------------------------
# §9 FONTI DECISIVE
# ---------------------------------------------------------------------------
_SOURCE_PRIORITY = [
    (_SEV_FINAL_VALUE, ("valore", "vendita", "stima", "prezzo")),
    (_SEV_OCCUPANCY, ("occupa", "locazione", "contratt", "conduttore")),
    (_SEV_TECH_ACTION, ("conform", "edifiz", "catast", "urbanist", "impiant")),
    (_SEV_CONTEXT, ("formalit", "ipoteca", "pignoram")),
]


def _source_priority(topic: str, section: str) -> int:
    text = _norm(topic) + " " + _norm(section)
    for pri, keys in _SOURCE_PRIORITY:
        if any(k in text for k in keys):
            return pri
    return _SEV_CONTEXT


def _build_sources(report: Dict[str, Any], evidence: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    # One source per (topic), prune the surface micro-topics into a single line.
    primary: List[Dict[str, Any]] = []
    seen_topics: set = set()
    surface_pages: set = set()
    total = 0
    for entry in evidence:
        section = str(entry.get("report_section") or "")
        topic = str(entry.get("topic") or "")
        if _norm(section) == _norm("Superfici e dati catastali"):
            surface_pages |= set(_pages([entry.get("page")]))
            continue
        # Market comparatives (OMI / listings) are NOT decisive evidence — they are
        # already represented by the single collapsed "comparatives" line in the
        # Numeri section (§6/§J). Keep them out of the decisive-sources list so
        # they never crowd out identity / valuation-chain / conformity evidence.
        if _norm(topic).startswith("comparativ"):
            continue
        key = _norm(topic)
        if key in seen_topics:
            continue
        seen_topics.add(key)
        total += 1
        pri = _source_priority(topic, section)
        excerpt = entry.get("perizia_excerpt") if entry.get("coverage_status") == "covered" else None
        try:
            page = int(entry.get("page"))
        except (TypeError, ValueError):
            page = None
        primary.append(
            {
                "source_id": _finding_id("acquisto", key, None, page, None).replace("acq-", "src-"),
                "page": page,
                "title": topic,
                "excerpt": excerpt,
                "excerpt_status": "covered" if excerpt else "excerpt_missing",
                "priority": pri,
            }
        )
    primary.sort(key=lambda s: (s["priority"], 0 if s["excerpt"] else 1, s["page"] or 999))
    if surface_pages:
        primary.append(
            {
                "source_id": "src-superfici",
                "page": min(surface_pages),
                "title": "Superfici e dati catastali",
                "excerpt": None,
                "excerpt_status": "collapsed",
                "priority": _SEV_CONTEXT,
            }
        )
        total += 1
    if not primary:
        return None
    return {"primary": primary[:8], "all_count": total}


# ---------------------------------------------------------------------------
# Confirmation eligibility + join
# ---------------------------------------------------------------------------
def _attach_confirmation(finding: Dict[str, Any]) -> None:
    """Attach a deterministic confirmation panel when the finding is eligible (§K)."""
    if finding.get("status") not in ("da_verificare",):
        return
    if not finding.get("evidence") and not finding.get("page"):
        return
    confirm_class = finding.get("confirm_class")
    if not confirm_class:
        # infer from section
        confirm_class = {
            "conformita": "conformity",
            "occupazione": "occupancy",
            "numeri": "money_role",
            "formalita": "formality",
        }.get(finding["section"])
    options = _CONFIRM_OPTIONS.get(confirm_class)
    if not options:
        return
    if not finding.get("evidence"):
        # No verbatim excerpt → professional-check line instead of a form.
        finding["professional_check"] = (
            f"Verifica pagina {finding.get('page')} con un professionista"
            if finding.get("page")
            else "Verifica con un professionista"
        )
        return
    finding["confirmation"] = {
        "eligible": True,
        "question": _confirm_question(confirm_class, finding),
        "options": list(options),
        "unsure_option": dict(_UNSURE_OPTION),
    }


def _confirm_question(confirm_class: str, finding: Dict[str, Any]) -> str:
    page = finding.get("page")
    if confirm_class == "money_role":
        return f"Secondo la pagina {page}, l'importo indicato rappresenta:"
    if confirm_class == "occupancy":
        return f"Secondo la pagina {page}, l'immobile risulta:"
    if confirm_class == "conformity":
        return f"Secondo la pagina {page}, la perizia descrive questa situazione come:"
    if confirm_class == "formality":
        return f"Secondo la pagina {page}, la formalità risulta:"
    return "Come interpretare questa indicazione?"


def _apply_confirmations(
    findings: List[Dict[str, Any]], confirmations: Sequence[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Join user confirmations (from Mongo) onto findings at read time.

    A confirmation never overwrites the perizia fact; it adds a USER_CONFIRMED
    marker and moves the finding status. Stale confirmations are shown but not
    applied. Returns the list of confirmation view objects (§10 CONFERME).
    """
    by_finding = {c.get("finding_id"): c for c in confirmations if isinstance(c, dict)}
    views: List[Dict[str, Any]] = []
    for f in findings:
        conf = by_finding.get(f["finding_id"])
        if not conf:
            continue
        # Stale when the underlying excerpt changed since the confirmation (rerun).
        current_hash = evidence_hash((f.get("evidence") or {}).get("excerpt"))
        stored_hash = conf.get("evidence_hash")
        stale = bool(stored_hash) and stored_hash != current_hash
        option = conf.get("selected_option")
        unsure = option == "non_sicuro"
        page = conf.get("page")

        if stale:
            wording = "Conferma precedente da rivedere"
            view_status = "da_rivedere"
        elif unsure:
            wording = (
                f"Hai indicato «Non sono sicuro» in base alla pagina {page}: "
                "la verifica resta aperta."
            )
            view_status = "non_sicuro"
        else:
            wording = f"Confermato dall'utente sulla base della pagina {page}."
            view_status = "confermato_utente"

        views.append(
            {
                "finding_id": f["finding_id"],
                "title": f.get("title"),
                "selected_label": conf.get("selected_label"),
                "page": page,
                "status": view_status,
                "stale": stale,
                "updated_at": conf.get("updated_at"),
                "wording": wording,
            }
        )

        # A stale confirmation is shown but NEVER applied to the current report.
        if stale:
            continue

        if unsure:
            # "Non sono sicuro" NEVER escalates severity by itself. Red is reserved
            # for a finding that is critical/technically blocking on its own; an
            # ordinary finding simply stays OPEN (amber, CONFIRMATIONS_REQUIRED) with
            # the confirmation still offered so it counts and can be changed.
            if f.get("blocking"):
                f["status"] = "verifica_tecnica_richiesta"
                f["status_label"] = "Verifica tecnica richiesta"
                f.pop("confirmation", None)
            else:
                f["user_unsure"] = True
                # status stays "da_verificare"; confirmation stays attached (open).
        else:
            f["status"] = "confermato_utente"
            f["status_label"] = "Confermato dall'utente"
            f["user_confirmed"] = True
            f.pop("confirmation", None)
    return views


# ---------------------------------------------------------------------------
# §11 STATO DELLE VERIFICHE + §1 ESITO
# ---------------------------------------------------------------------------
# Statuses that represent an open action the buyer still has to resolve.
_ACTION_STATUSES = frozenset({"da_verificare", "regolarizzabile", "non_conforme", "conferma_necessaria"})


def _is_actionable(finding: Dict[str, Any]) -> bool:
    return finding.get("status") in _ACTION_STATUSES


def _build_readiness(
    report_status: str, findings: List[Dict[str, Any]], confirmations_views: List[Dict[str, Any]]
) -> Dict[str, Any]:
    open_confirmations = sum(1 for f in findings if f.get("confirmation"))
    done = sum(1 for c in confirmations_views if not c.get("stale") and c.get("status") == "confermato_utente")
    tech_required = any(f.get("status") == "verifica_tecnica_richiesta" for f in findings)
    # Professional/technical checks that remain open: actionable findings that are
    # not themselves an offered confirmation (those are counted separately).
    professional_open = sum(
        1 for f in findings if _is_actionable(f) and not f.get("confirmation")
    )

    fail_closed = report_status not in _CUSTOMER_SAFE_STATUSES
    interactive = report_status in _SAFE_NON_READY_STATUSES
    if fail_closed or tech_required:
        state = "TECHNICAL_REVIEW_REQUIRED"
    elif interactive:
        # A pending customer interaction (lot selection / money confirmation /
        # upload) — not fail-closed, not a technical failure.
        state = "CONFIRMATIONS_REQUIRED"
    elif open_confirmations:
        state = "CONFIRMATIONS_REQUIRED"
    elif professional_open:
        state = "READY_FOR_REVIEW"
    else:
        state = "COMPLETE_FOR_EXPORT"

    return {
        "state": state,
        "label": _READINESS_LABEL[state],
        "confirmations_total": open_confirmations + done,
        "confirmations_done": done,
        "professional_checks_open": professional_open,
    }


def _build_esito(
    report_status: str, findings: List[Dict[str, Any]], readiness: Dict[str, Any]
) -> Dict[str, Any]:
    open_confirmations = any(f.get("confirmation") for f in findings)
    # Red is reserved for a genuinely fail-closed report or a report that a
    # blocking finding pushed into technical review. Interactive safe statuses
    # (lot selection / money confirmation) are amber, never red.
    if readiness["state"] == "TECHNICAL_REVIEW_REQUIRED":
        level = "rosso"
    elif report_status != "REPORT_READY":
        level = "ambra"
    elif not any(_is_actionable(f) for f in findings) and not open_confirmations:
        level = "verde"
    else:
        level = "ambra"

    wording = _ESITO_WORDING[level]
    drivers: List[Dict[str, Any]] = []
    seen_titles: set = set()
    for f in sorted(findings, key=lambda x: x.get("severity", 9)):
        if not (_is_actionable(f) or f.get("status") == "verifica_tecnica_richiesta"):
            continue
        title = f.get("title")
        if _norm(title) in seen_titles:
            continue
        seen_titles.add(_norm(title))
        drivers.append({"finding_id": f["finding_id"], "title": title, "section": f["section"]})
        if len(drivers) >= 5:
            break
    return {
        "level": level,
        "headline": wording["headline"],
        "sentence": wording["sentence"],
        "drivers": drivers,
    }


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def build_decision_model(
    report: Dict[str, Any], confirmations: Sequence[Dict[str, Any]] = ()
) -> Dict[str, Any]:
    """Build the customer decision model from a stored ``customer_report`` dict.

    Pure, deterministic, no OpenAI/network/PDF. ``confirmations`` is the list of
    persisted user confirmations for this analysis (from MongoDB), joined here at
    read time. The input dict is never mutated.
    """
    report = report if isinstance(report, dict) else {}
    confirmations = list(confirmations or [])
    report_status = str(report.get("report_status") or "")
    lot_struct = report.get("lot_structure") or {}
    lot_id = str(lot_struct.get("selected_lot")) if lot_struct.get("selected_lot") not in (None, "") else None
    evidence = _evidence_lookup(report)
    full_money = report.get("money_sections") or {}

    sections: Dict[str, Any] = {}
    findings: List[Dict[str, Any]] = []

    # Fail-closed / non-ready statuses: attach only esito + readiness, no findings.
    if report_status == "REPORT_READY":
        acquisto = _build_acquisto(report, lot_id)
        if acquisto:
            sections["acquisto"] = acquisto

        numeri = _build_numeri(report, full_money)
        if numeri:
            sections["numeri"] = numeri
        findings.extend(_build_money_findings(report, evidence, lot_id))

        occupazione = _build_occupancy(report, evidence)
        if occupazione:
            sections["occupazione"] = occupazione
            findings.append(
                {
                    "finding_id": _finding_id("occupazione", "occupazione", lot_id, occupazione.get("pagina"), None),
                    "section": "occupazione",
                    "topic": "occupazione",
                    "title": "Stato di occupazione",
                    "status": "da_verificare" if occupazione.get("cosa_verificare") else "completato",
                    "status_label": "Da verificare" if occupazione.get("cosa_verificare") else "Completato",
                    "tone": "ambra" if occupazione.get("cosa_verificare") else "verde",
                    "severity": _SEV_OCCUPANCY,
                    "customer_summary": occupazione.get("dettaglio"),
                    "buyer_impact": occupazione.get("perche_conta"),
                    "recommended_action": (occupazione.get("cosa_verificare") or [None])[0],
                    "amount": None,
                    "pages": occupazione.get("pagine"),
                    "page": occupazione.get("pagina"),
                    "evidence": occupazione.get("evidence"),
                    "blocking": bool(occupazione.get("blocking")),
                    "confirm_class": "occupancy",
                }
            )

        conformity_findings = _build_conformity_findings(report, evidence, lot_id)
        findings.extend(conformity_findings)
        conformita = _build_conformita_section(conformity_findings)
        if conformita:
            sections["conformita"] = conformita

        formalita, formality_findings = _build_formalita(report, lot_id)
        findings.extend(formality_findings)
        if formalita:
            sections["formalita"] = formalita

        verifiche = _build_verifiche(
            report, conformity_findings, formality_findings, occupazione, sections.get("numeri"), lot_id
        )
        if verifiche:
            sections["verifiche"] = verifiche

        used_tokens = {f["topic"] for f in conformity_findings}
        altri = _build_altri(report, used_tokens)
        if altri:
            sections["altri"] = altri

        fonti = _build_sources(report, evidence)
        if fonti:
            sections["fonti"] = fonti

        # Confirmation eligibility (before joining user answers).
        eligible_count = 0
        for f in sorted(findings, key=lambda x: x.get("severity", 9)):
            if eligible_count >= MAX_ELIGIBLE_CONFIRMATIONS:
                break
            _attach_confirmation(f)
            if f.get("confirmation"):
                eligible_count += 1

    # Join user confirmations (also for non-ready → empty since no findings).
    confirmation_views = _apply_confirmations(findings, confirmations)
    if confirmation_views:
        sections["conferme"] = {"items": confirmation_views}

    # Reconcile the checklist AFTER confirmations are applied, so the "Cosa
    # verificare" summary can never contradict the detailed finding cards.
    if "verifiche" in sections:
        sections["verifiche"] = _reconcile_verifiche(sections["verifiche"], findings)

    readiness = _build_readiness(report_status, findings, confirmation_views)
    sections["stato_verifiche"] = {
        "label": readiness["label"],
        "confirmations_total": readiness["confirmations_total"],
        "confirmations_done": readiness["confirmations_done"],
        "professional_checks_open": readiness["professional_checks_open"],
    }
    esito = _build_esito(report_status, findings, readiness)

    return {
        "schema_version": SCHEMA_VERSION,
        "analysis_id": report.get("analysis_id"),
        "job_id": report.get("job_id"),
        "lot_id": lot_id,
        "report_status": report_status,
        "readiness": readiness,
        "esito": esito,
        "sections": sections,
        "findings": findings,
        "confirmations": confirmation_views,
    }
