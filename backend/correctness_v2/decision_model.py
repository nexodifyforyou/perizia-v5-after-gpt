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
import re
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
    "uncertain": ("da_chiarire", "Da chiarire", "slate"),
}
_DEFAULT_STATUS = ("non_determinabile", "Non determinabile dalla sola perizia", "slate")

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
    "impianti": "L'assenza di alcune dichiarazioni non prova da sola la non conformità: occorre verificare stato, certificabilità ed eventuali adeguamenti.",
    "agibilita": "L'agibilità incide sull'utilizzo e sul valore dell'immobile.",
    "ape": "La certificazione energetica è documentazione richiesta per la vendita.",
}
_CONFORMITY_WHY_DEFAULT = "L'aspetto tecnico indicato può richiedere verifiche o costi prima di procedere."

_READINESS_LABEL = {
    "TECHNICAL_REVIEW_REQUIRED": "Verifica tecnica richiesta",
    "CONFIRMATIONS_REQUIRED": "Conferme necessarie",
    "READY_FOR_REVIEW": "Verifiche professionali aperte",
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


def _semantic_norm(text: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", _norm(text)).strip()


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
    evidence: Sequence[Dict[str, Any]], pages: Iterable[int], topic_keywords: Sequence[str],
    *, expected_amount: Optional[float] = None, expected_bene: Optional[str] = None,
    expected_asset: Optional[str] = None,
    excerpt_keywords: Optional[Sequence[str]] = None,
    required_fact_keywords: Sequence[str] = (),
) -> Optional[Dict[str, Any]]:
    """Return a covered verbatim excerpt on one of ``pages`` whose topic matches.

    The customer_evidence_index is already verbatim-gated upstream; here we only
    SELECT — never rewrite. Number-only / topically-wrong entries are avoided by
    requiring a topic-keyword match.
    """
    page_set = set(_pages(pages))
    keys = [_semantic_norm(k) for k in topic_keywords if _semantic_norm(k)]
    default_excerpt_keywords: Sequence[str] = topic_keywords
    if excerpt_keywords is None and expected_bene and any(
        "valore di stima" in _semantic_norm(k) for k in topic_keywords
    ):
        # Component row wording commonly reverses metadata order:
        # "Bene N° 3 ... Valore di stima" versus topic "Valore ... Bene N° 3".
        default_excerpt_keywords = ("valore di stima",)
    excerpt_keys = [
        _semantic_norm(k) for k in (excerpt_keywords if excerpt_keywords is not None else default_excerpt_keywords)
        if _semantic_norm(k)
    ]
    fact_keys = [_semantic_norm(k) for k in required_fact_keywords if _semantic_norm(k)]
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
        # Match the evidence topic itself.  The shared report-section label (for
        # example "Conformità e documenti tecnici") is deliberately excluded:
        # it made an APE excerpt eligible for every neighbouring category.
        topic_l = _semantic_norm(entry.get("topic"))
        if keys and not any(k in topic_l for k in keys):
            continue
        excerpt = str(entry.get("perizia_excerpt"))
        excerpt_l = _norm(excerpt)
        excerpt_semantic = _semantic_norm(excerpt)
        # Metadata says what an index row was intended to prove; the displayed
        # source span must independently contain the category itself.
        if excerpt_keys and not any(k in excerpt_semantic for k in excerpt_keys):
            continue
        # Some declared conclusions require a decisive fact, not merely the
        # section heading (for example "completezza" versus "risulta completa").
        if fact_keys and not any(k in excerpt_semantic for k in fact_keys):
            continue
        # A Bene label in metadata is not proof that the displayed source span
        # belongs to that asset.  Require the target Bene in the excerpt itself;
        # otherwise fail closed (or let the cached-page resolver recover a
        # decisive span containing both Bene and amount).
        if expected_bene and _semantic_norm(expected_bene) not in _semantic_norm(excerpt_l):
            continue
        if expected_asset:
            asset_terms = _component_asset_terms(expected_asset)
            bene_number = re.search(r"bene\s+n[°ºo.]?\s*(\d+)", expected_bene or "", re.I)
            row_match = re.search(
                rf"Bene\s+N[°ºo.]?\s*{bene_number.group(1)}\b([^\n;:]{{0,100}})",
                excerpt,
                re.I,
            ) if bene_number else None
            declared_zone = _semantic_norm(row_match.group(1) if row_match else "")
            if not asset_terms or not any(term in declared_zone.split() for term in asset_terms):
                continue
        if expected_amount is not None and not _excerpt_contains_amount(excerpt, expected_amount):
            continue
        return {"page": page, "excerpt": excerpt, "verbatim": True}
    return None


def _excerpt_contains_amount(excerpt: Any, expected: float) -> bool:
    """Currency/locale-normalized monetary match; fail closed on ambiguity."""
    for raw in re.findall(r"(?:EUR|€)?\s*(-?\d[\d. ]*(?:,\d{1,2})?)", str(excerpt or ""), re.I):
        compact = raw.replace(" ", "").replace(".", "").replace(",", ".")
        try:
            if abs(float(compact) - float(expected)) < 0.005:
                return True
        except ValueError:
            pass
    return False


def _missing_evidence(pages: Iterable[int]) -> Dict[str, Any]:
    """Customer-safe fail-closed marker, retaining only a reliable page hint."""
    pgs = _pages(pages)
    return {
        "page": pgs[0] if pgs else None,
        "excerpt": None,
        "note": "Estratto decisivo non disponibile",
        "verbatim": False,
    }


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
    component_src = [r for r in uncertain_src if _is_component_value(r)]
    uncertain_src = [r for r in uncertain_src if not _is_component_value(r)]
    auction_src = [r for r in (money.get("auction_terms") or []) if isinstance(r, dict)]

    if not (chain_src or buyer_src or uncertain_src):
        return None

    canonical, ambiguous = _reorder_chain(chain_src)
    catena = [_chain_row_view(r) for r in canonical]
    if component_src and catena:
        catena[0]["label"] = "Valore di stima prima dei deprezzamenti"
        for row in reversed(catena):
            if row.get("kind") == "value":
                row["label"] = "Valore finale dichiarato"
                break
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
    if component_src:
        out["composizione_valore"] = {
            "title": "Composizione del valore di stima",
            "items": [_component_value_view(report, r) for r in component_src],
            "total": sum((_as_float(r.get("amount")) or 0.0) for r in component_src),
            "total_display": format_eur(sum((_as_float(r.get("amount")) or 0.0) for r in component_src)),
        }
    reconciliation = _valuation_reconciliation(report, chain_src, component_src)
    if reconciliation:
        out["riconciliazione"] = reconciliation
        out.setdefault("da_chiarire", []).append({
            "label": "Calcolo da chiarire",
            "amount_display": reconciliation["difference_display"],
            "motivo": reconciliation["explanation"],
        })
    # Original extracted order preserved for Vista admin (§F rule 1b).
    out["catena_ordine_originale"] = [_chain_row_view(r) for r in chain_src]
    return out


_COMPONENT_VALUE_RE = re.compile(r"valore\s+di\s+stima\s+bene\s+n[°ºo.]?\s*(\d+)\s*-?\s*(.*)", re.I)


def _is_component_value(row: Dict[str, Any]) -> bool:
    return bool(_COMPONENT_VALUE_RE.search(str(row.get("label") or ""))) and _as_float(row.get("amount")) is not None


def _component_label(row: Dict[str, Any]) -> str:
    match = _COMPONENT_VALUE_RE.search(str(row.get("label") or ""))
    return (match.group(2).strip() if match and match.group(2).strip() else f"Bene {match.group(1)}") if match else "Bene"


def _component_asset_terms(label: Any) -> Tuple[str, ...]:
    """Return source-facing aliases for a component's declared asset type.

    A Bene number is not unique enough to recover evidence from legacy cached
    pages: a malformed table may repeat the same number beside a different asset.
    Keep a deliberately small, generic alias set and otherwise require one of the
    meaningful words in the declared component label.
    """
    normalized = _semantic_norm(label)
    alias_groups = (
        (("garage", "autorimessa", "box"), ("garage", "autorimessa", "box")),
        (("magazzino", "rustico", "deposito"), ("magazzino", "rustico", "deposito")),
        (("appartamento", "abitazione", "alloggio"), ("appartamento", "abitazione", "alloggio")),
    )
    for triggers, aliases in alias_groups:
        if any(trigger in normalized.split() for trigger in triggers):
            return aliases
    ignored = {"bene", "immobile", "unita", "numero", "lotto", "valore", "stima"}
    return tuple(
        word for word in normalized.split()
        if len(word) >= 4 and word not in ignored and not word.isdigit()
    )


def _component_value_view(report: Dict[str, Any], row: Dict[str, Any]) -> Dict[str, Any]:
    match = _COMPONENT_VALUE_RE.search(str(row.get("label") or ""))
    bene = f"Bene N° {match.group(1)}" if match else None
    asset_label = _component_label(row)
    amount = _as_float(row.get("amount"))
    pages = _pages(row.get("evidence_pages"))
    excerpt = _find_excerpt(
        _evidence_lookup(report), pages, (_semantic_norm(row.get("label")),),
        expected_amount=amount, expected_bene=bene, expected_asset=asset_label,
    )
    if not excerpt:
        excerpt = _find_cached_component_excerpt(report, pages, bene, amount, asset_label)
    return {
        "label": asset_label, "amount": amount,
        "amount_display": row.get("amount_display") or format_eur(amount),
        "pages": pages, "evidence": excerpt or _missing_evidence(pages),
    }


def _find_cached_component_excerpt(
    report: Dict[str, Any], pages: Iterable[int], bene: Optional[str], amount: Optional[float],
    expected_asset: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Recover a narrow decisive span from already-cached text pages.

    Used only when the old customer evidence index is wrong/missing.  Both the
    target Bene, declared asset type and normalized amount must occur in the same
    bounded table span.
    """
    asset_terms = _component_asset_terms(expected_asset)
    if not bene or amount is None or not asset_terms:
        return None
    allowed = set(_pages(pages))
    bene_norm = _semantic_norm(bene)
    for item in report.get("_cached_input_pages") or []:
        if not isinstance(item, dict):
            continue
        try:
            page = int(item.get("page_number") or item.get("page"))
        except (TypeError, ValueError):
            continue
        if allowed and page not in allowed:
            continue
        text = str(item.get("text") or "")
        semantic = _semantic_norm(text)
        start_sem = semantic.find(bene_norm)
        if start_sem < 0:
            continue
        # Work on the original text: locate Bene number/type permissively, then
        # require the expected amount before the next Bene/table row.
        number_match = re.search(r"bene\s+n[°ºo.]?\s*(\d+)", bene, re.I)
        if not number_match:
            continue
        pattern = re.compile(rf"Bene\s+N[°ºo.]?\s*{number_match.group(1)}\b", re.I)
        for match in pattern.finditer(text):
            tail = text[match.start(): match.start() + 500]
            next_bene = re.search(r"\n\s*Bene\s+N[°ºo.]?\s*\d+", tail[match.end() - match.start():], re.I)
            if next_bene:
                tail = tail[: match.end() - match.start() + next_bene.start()]
            amount_end = _matching_amount_end(tail, amount)
            if amount_end is None:
                continue
            # End exactly at the decisive amount.  A fixed-length tail can copy
            # unrelated contact/admin text following an otherwise valid row.
            compact = " ".join(tail[:amount_end].split())
            relative_match_end = match.end() - match.start()
            before_amount = tail[relative_match_end:amount_end]
            # The asset type must be the row label immediately following the Bene
            # number (or an explicit Tipologia field), not a later incidental
            # mention such as "magazzino con accesso dal garage".
            immediate_label = re.match(
                r"\s*[-–—:]?\s*([^\n;:]{1,100})", before_amount, re.I
            )
            declared_label = re.split(
                r"\b(?:valore\s+di\s+stima|accesso|ubicat\w*|sito|indirizzo|comune|via)\b",
                immediate_label.group(1) if immediate_label else "",
                maxsplit=1,
                flags=re.I,
            )[0]
            explicit_type = re.search(
                r"\btipologia\s*:?\s*([^\n;:]{1,80})", before_amount, re.I
            )
            asset_zone = _semantic_norm(
                f"{declared_label} {explicit_type.group(1) if explicit_type else ''}"
            )
            if not any(term in asset_zone.split() for term in asset_terms):
                continue
            identity = report.get("case_identity") or {}
            allowed_identity = " ".join(str(value or "") for value in (
                identity.get("address"), identity.get("property_type"), bene, expected_asset,
                *(
                    field
                    for asset in (report.get("beni_sections") or []) if isinstance(asset, dict)
                    for field in (asset.get("address"), asset.get("property_type"))
                    if field
                ),
            ))
            if _contains_sensitive_identity(compact, allowed_identity):
                continue
            return {"page": page, "excerpt": compact, "verbatim": True, "source": "cached_page"}
    return None


def _matching_amount_end(text: str, expected: float) -> Optional[int]:
    """Return the end offset of the first locale-normalized expected amount."""
    pattern = re.compile(r"(?:EUR|€)?\s*(-?\d[\d. ]*(?:,\d{1,2})?)", re.I)
    for match in pattern.finditer(text or ""):
        compact = match.group(1).replace(" ", "").replace(".", "").replace(",", ".")
        try:
            if abs(float(compact) - float(expected)) < 0.005:
                return match.end()
        except ValueError:
            continue
    return None


_EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
_ITALIAN_CF_RE = re.compile(r"\b[A-Z]{6}\d{2}[A-Z]\d{2}[A-Z]\d{3}[A-Z]\b", re.I)
_CONTACT_MARKER_RE = re.compile(
    r"\b(?:telefono|tel\.?|cellulare|cell\.?|pec|e-?mail|contatto|codice\s+fiscale|"
    r"c\.?\s*f\.?|carta\s+(?:d['’]\s*)?identit[aà]|documento\s+(?:d['’]\s*)?identit[aà]|"
    r"passaport\w*|patente(?:\s+di\s+guida)?|tessera\s+sanitaria|"
    r"permesso\s+di\s+soggiorno|documento\s+(?:personale|di\s+riconoscimento)|"
    r"estremi\s+(?:del\s+)?documento|data\s+(?:di\s+)?nascita|"
    r"luogo\s+(?:di\s+)?nascita|nato\s+(?:a|il)|nata\s+(?:a|il)|"
    r"nome\s+e\s+cognome|signor[ae]?|sig\.?|"
    r"esecutato|debitore|intestatario)\b",
    re.I,
)
_PHONE_RE = re.compile(r"(?:\+\s*39\s*)?(?:\d[ .-]*){9,12}")
_COMMON_GIVEN_NAMES = frozenset({
    "alessandro", "andrea", "anna", "antonio", "carlo", "chiara", "davide",
    "elena", "francesca", "francesco", "giovanni", "giulia", "giuseppe",
    "laura", "luca", "luigi", "marco", "maria", "mario", "matteo", "mauro",
    "paolo", "pietro", "roberto", "sara", "simone", "stefano", "syed",
})


def _contains_sensitive_identity(text: Any, allowed_identity_text: Any = "") -> bool:
    value = str(text or "")
    if _EMAIL_RE.search(value) or _ITALIAN_CF_RE.search(value) or _CONTACT_MARKER_RE.search(value):
        return True
    # A bare international/telephone-length sequence is not a property-table
    # identifier.  Decimal/cadastral values remain below this threshold.
    if _PHONE_RE.search(value):
        return True
    # Catch an unlabelled person name conservatively without rejecting the
    # report's declared property type/address. Address-labelled spans are also
    # allowed for legacy reports that lack case_identity.address.
    proper = list(re.finditer(r"\b[A-ZÀ-Ý][a-zà-ÿ]{2,}\b", value))
    if any(_norm(match.group(0)) in _COMMON_GIVEN_NAMES for match in proper):
        return True
    allowed_words = set(_semantic_norm(allowed_identity_text).split())
    allowed_words.update({
        "bene", "garage", "autorimessa", "box", "magazzino", "rustico",
        "deposito", "appartamento", "abitazione", "alloggio", "comune",
        "via", "viale", "piazza", "localita", "san", "santa", "santo",
        "valore", "stima", "foglio", "particella", "subalterno", "categoria",
        "catasto", "sezione", "mappale",
    })
    for address in re.finditer(
        r"\b(?:comune|via|viale|piazza|localit[aà])\b\s*:?[ -]*"
        r"(.{0,120}?)(?=\b(?:valore|superficie|consistenza)\b|€|$)",
        value,
        re.I,
    ):
        allowed_words.update(_semantic_norm(address.group(1)).split())
    for first, second in zip(proper, proper[1:]):
        between = value[first.end():second.start()]
        if not between.isspace():
            continue
        if (
            _norm(first.group(0)) not in allowed_words
            or _norm(second.group(0)) not in allowed_words
        ):
            return True
    return False


def _valuation_reconciliation(report, chain, components):
    """Reconcile explicit component values, listed percentages and declared final."""
    if not components:
        return None
    component_sum = sum((_as_float(r.get("amount")) or 0.0) for r in components)
    values = [_as_float(r.get("amount")) for r in chain if r.get("kind") == "value"]
    values = [v for v in values if v is not None]
    if len(values) < 2 or not _approx(values[0], component_sum):
        return None
    percentages: List[float] = []
    seen_spans: set = set()
    for entry in _evidence_lookup(report):
        text = str(entry.get("perizia_excerpt") or "")
        if "valore finale" not in _norm(text) and "deprezz" not in _norm(text):
            continue
        span_key = _semantic_norm(text)
        if not span_key or span_key in seen_spans:
            continue
        seen_spans.add(span_key)
        # Attribute a percentage only to a reduction-labelled segment.  This
        # excludes adjacent VAT, ownership quota and other unrelated percentages.
        previous_end = 0
        reduction_context = "deprezz" in _norm(text[:80])
        for match in re.finditer(r"(\d{1,2}(?:,\d+)?)\s*%", text):
            segment = _norm(text[previous_end:match.start()])
            has_reduction_label = any(k in segment for k in (
                "rischio", "garanzia", "stato d'uso", "manutenzione",
                "regolarizzazione", "riduzione", "deprezz", "abbattimento", "oneri",
            ))
            has_unrelated_label = any(k in segment for k in (
                "iva", "quota", "proprieta", "interesse", "provvigione",
            ))
            if has_reduction_label or (reduction_context and not has_unrelated_label):
                percentages.append(float(match.group(1).replace(",", ".")))
                reduction_context = True
            else:
                reduction_context = False
            previous_end = match.end()
    percentages = percentages[:8]
    if not percentages:
        return None
    pct_sum = sum(percentages)
    expected = round(component_sum * (1.0 - pct_sum / 100.0), 2)
    declared = values[-1]
    difference = round(abs(expected - declared), 2)
    if difference <= 1.0:
        return None
    pct_text = " + ".join(f"{p:g}%" for p in percentages)
    return {
        "component_sum": component_sum, "percentage_sum": pct_sum,
        "expected_final": expected, "declared_final": declared, "difference": difference,
        "component_sum_display": format_eur(component_sum), "expected_final_display": format_eur(expected),
        "declared_final_display": format_eur(declared), "difference_display": format_eur(difference),
        "explanation": (
            f"I deprezzamenti indicati del {pct_text} porterebbero matematicamente da "
            f"{format_eur(component_sum)} a {format_eur(expected)}, mentre la perizia dichiara "
            f"{format_eur(declared)}. La differenza di {format_eur(difference)} non è separatamente spiegata."
        ),
    }


def _build_money_findings(
    report: Dict[str, Any], evidence: Sequence[Dict[str, Any]], lot_id: Optional[str]
) -> List[Dict[str, Any]]:
    """Confirmation-eligible findings for genuinely uncertain money rows (§K money-role)."""
    money = report.get("money_sections") or {}
    findings: List[Dict[str, Any]] = []
    for row in money.get("uncertain_money") or []:
        if not isinstance(row, dict):
            continue
        if _is_component_value(row):
            continue
        pages = _pages(row.get("evidence_pages"))
        amount = _as_float(row.get("amount"))
        label = str(row.get("label") or "")
        excerpt = _find_excerpt(evidence, pages, tuple(_norm(label).split()[:2]), expected_amount=amount)
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
        if any(
            "locazion" in _norm(r)
            and any(k in _norm(r) for k in (
                "non indic", "non riport", "non sono riport", "non risult",
            ))
            for r in occ.get("risks") or []
        ):
            cosa_verificare.insert(0, "Verificare lo stato della liberazione e l’eventuale esistenza di titoli opponibili. La perizia riporta la residenza del nucleo dell’esecutato ma non indica contratti di locazione.")
        else:
            cosa_verificare.insert(0, "Opponibilità del titolo da verificare: la perizia non si esprime espressamente.")
    pages = _pages(occ.get("evidence_pages"))
    excerpt = _find_excerpt(
        evidence, pages,
        ("occupa", "contratt", "locazione", "conduttore", "affitt", "residen", "esecutat"),
    )
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
        notes_norm = _norm(item.get("notes"))
        area_norm = _norm(area)
        ownership_area = any(k in area_norm for k in (
            "titolarita", "proprieta", "diritti posti in vendita",
        ))
        public_housing_constraints = any(k in area_norm for k in (
            "edilizia residenziale pubblica", "edilizia convenzionata",
            "alloggio pubblico", "alloggi pubblici",
        ))
        if public_housing_constraints:
            token = "vincoli_edilizia_pubblica"
        if "ape" in area_norm or "certificazione energetica" in area_norm:
            token = "ape"
        if cls == "uncertain":
            # Negative/qualified statements must win over substring matches such
            # as "dichiarato" inside "non dichiarato" and "completa" inside
            # "incompleta".
            incomplete_documentation = (
                ("document" in area_norm or "document" in notes_norm)
                and (
                    "incompleta" in notes_norm
                    or re.search(r"\bnon\s+(?:risulta\s+|e\s+|appare\s+)?complet", notes_norm)
                )
            )
            ownership_term = r"(?:diritto\s+di\s+proprieta|proprieta|titolarita)"
            declaration_term = (
                r"(?:dichiarat|attestat|accertat|indicat|riportat|provat|"
                r"dimostrat|documentat)\w*"
            )
            declaration_auxiliary = (
                r"(?:(?:e|era|risulta|risultava|viene|veniva)"
                r"(?:\s+stat[ao])?\s+)?"
            )
            negated_declaration_after = (
                re.search(
                    rf"\b{ownership_term}\b.{{0,60}}\bnon\s+"
                    rf"{declaration_auxiliary}(?P<verb>{declaration_term})\b",
                    notes_norm,
                )
                if ownership_area else None
            )
            negated_declaration_before = (
                re.search(
                    rf"\bnon\s+{declaration_auxiliary}"
                    rf"(?P<verb>{declaration_term})\b.{{0,60}}\b{ownership_term}\b",
                    notes_norm,
                )
                if ownership_area else None
            )
            negated_ownership_declaration = (
                negated_declaration_after or negated_declaration_before
            )
            negated_declaration_verb = _norm(
                negated_ownership_declaration.group("verb")
                if negated_ownership_declaration else ""
            )
            ownership_declaration_absent = negated_declaration_verb.startswith(
                ("dichiarat", "indicat", "riportat")
            )
            negated_continuity = (
                "continuita" in area_norm
                and (
                    re.search(r"\bnon\s+(?:sussiste|risulta|e|appare)(?:\s+la)?\s+continuita\b", notes_norm)
                    or re.search(r"\bcontinuita\b.{0,40}\bnon\s+(?:sussiste|risulta|e|appare)\b", notes_norm)
                )
            )
            negated_ownership = (
                ownership_area
                and (
                    re.search(r"\bnon\s+(?:risulta|sussiste|e|appare)(?:\s+la)?\s+(?:proprieta|titolarita)\b", notes_norm)
                    or re.search(r"\bnon\s+appart", notes_norm)
                    or re.search(r"\b(?:proprieta|titolarita)\b.{0,40}\bnon\s+(?:risulta|sussiste|e|appare)\b", notes_norm)
                )
            )
            ownership_needs_documentary_check = (
                ownership_area
                and (
                    re.search(
                        r"\b(?:proprieta|titolarita)\b.{0,50}"
                        r"\bda\s+verificare\s+documentalmente\b",
                        notes_norm,
                    )
                    or re.search(
                        r"\b(?:proprieta|titolarita|diritto\s+di\s+proprieta)\b.{0,50}"
                        r"\b(?:da\s+verificare|deve\s+essere\s+verificat\w*)\b",
                        notes_norm,
                    )
                )
            )
            ownership_not_determinable = (
                ownership_area
                and re.search(
                    r"\b(?:proprieta|titolarita|diritto\s+di\s+proprieta)\b.{0,50}"
                    r"\bnon\s+determinabil\w*(?:\s+dalla\s+sola\s+perizia)?\b",
                    notes_norm,
                )
            )
            ownership_uncertain = (
                ownership_area
                and re.search(
                    r"\b(?:proprieta|titolarita|diritto\s+di\s+proprieta)\b.{0,50}"
                    r"\b(?:incert\w*|dubbi\w*|da\s+chiarire)\b",
                    notes_norm,
                )
            )
            ownership_explicitly_declared = (
                ownership_area
                and not negated_ownership_declaration
                and (
                    re.search(
                        rf"\b{ownership_term}\b.{{0,60}}\b{declaration_term}\b",
                        notes_norm,
                    )
                    or re.search(
                        rf"\b{declaration_term}\b.{{0,60}}\b{ownership_term}\b",
                        notes_norm,
                    )
                    or re.search(r"\brisult\w*\s+(?:di\s+)?proprieta\b", notes_norm)
                    or re.search(r"\bben[ei]\s+appartengono\b", notes_norm)
                )
            )
            if (
                any(k in notes_norm for k in ("non indic", "non dichiar", "non riport"))
                or ownership_declaration_absent
            ):
                status, status_label, tone = "non_dichiarato", "Non dichiarato dalla perizia", "slate"
            elif incomplete_documentation:
                status, status_label, tone = "da_chiarire", "Dichiarata incompleta dalla perizia", "ambra"
            elif ownership_not_determinable:
                status, status_label, tone = (
                    "non_determinabile", "Non determinabile dalla sola perizia", "slate"
                )
            elif ownership_needs_documentary_check:
                status, status_label, tone = "da_verificare", "Da verificare documentalmente", "ambra"
            elif negated_ownership_declaration:
                status, status_label, tone = "da_verificare", "Da verificare documentalmente", "ambra"
            elif negated_continuity or negated_ownership or ownership_uncertain:
                status, status_label, tone = "da_chiarire", "Da chiarire", "ambra"
            elif ownership_explicitly_declared:
                status, status_label, tone = "dichiarato_perizia", "Dichiarato dalla perizia", "verde"
                if re.search(r"\b1\s*/\s*1\b", notes_norm):
                    status_label = "Proprietà 1/1 dichiarata dalla perizia"
            elif ownership_area:
                # An ownership noun alone is not a declaration.  Unknown legacy
                # wording stays fail-closed instead of being upgraded by the
                # presence of the word "proprietà".
                status, status_label, tone = "da_chiarire", "Da chiarire", "slate"
            elif any(k in notes_norm for k in (
                "dichiarata", "dichiarato", "attestato", "sussiste", "risulta completa",
            )):
                status, status_label, tone = "dichiarato_perizia", "Dichiarato dalla perizia", "verde"
                if "completezza documentazione" in area_norm and any(
                    k in notes_norm for k in ("risulta completa", "dichiarata completa", "documentazione completa")
                ):
                    status_label = "Dichiarata completa dalla perizia"
                elif "continuita" in area_norm and any(
                    k in notes_norm for k in ("sussistenza della continuita", "continuita delle trascrizioni", "continuita nelle trascrizioni")
                ):
                    status_label = "Continuità delle trascrizioni dichiarata dalla perizia"
            elif public_housing_constraints and "non risulta realizzato" in notes_norm:
                status, status_label, tone = "dichiarato_perizia", "Dichiarato dalla perizia", "verde"
        pages = _pages(item.get("evidence_pages"))
        if public_housing_constraints:
            group = "Vincoli di edilizia convenzionata o pubblica"
        else:
            group = _AREA_GROUP_LABEL.get(token, str(area or "").strip().capitalize() or "Altro")
        category_keys = {
            "ape": ("ape", "certificazione energetica"),
            "agibilita": ("agibilita", "abitabilita"),
            "urbanistica": ("urbanistica", "destinazione urbanistica", "pgt"),
            "impianti": ("impianti", "dichiarazioni di conformita"),
            "impianto_gas": ("impianto gas", "gas"),
            "impianto_elettrico": ("impianto elettrico", "elettrico"),
            "edilizia": (
                "difformita", "regolarita edilizia", "conformita edilizia",
                "titolo edilizio", "titolo abilitativo", "sanatoria", "ripristino",
            ),
            "catastale": ("catastale", "catastali", "docfa"),
            "vincoli_edilizia_pubblica": (
                "edilizia residenziale pubblica", "edilizia convenzionata",
                "alloggio pubblico", "alloggi pubblici",
            ),
        }.get(token, (token, _norm(area)))
        excerpt_keys = category_keys
        fact_keys: Tuple[str, ...] = ()
        if "completezza documentazione" in area_norm:
            category_keys = ("completezza", "documentazione", "art 567")
            excerpt_keys = ("documentazione", "art 567")
            if status_label == "Dichiarata completa dalla perizia":
                fact_keys = ("risulta completa", "documentazione completa", "dichiarata completa")
        elif ownership_area:
            category_keys = ("titolarita", "diritti posti in vendita", "proprieta")
            excerpt_keys = ("proprieta", "diritto")
            if status_label == "Proprietà 1/1 dichiarata dalla perizia":
                fact_keys = ("proprieta 1 1", "diritto di proprieta 1 1")
        elif "continuita" in area_norm:
            category_keys = ("continuita", "trascrizioni")
            excerpt_keys = ("continuita", "trascrizioni")
            if status_label == "Continuità delle trascrizioni dichiarata dalla perizia":
                fact_keys = (
                    "sussistenza della continuita", "continuita nelle trascrizioni",
                    "continuita delle trascrizioni dichiarata",
                )
        elif token == "urbanistica":
            # A generic section heading does not prove a specific classification
            # such as AREC 2. Require a distinctive alphanumeric code when one is
            # explicitly stated in the normalized fact.
            code = re.search(r"\b([a-z]{2,}\s*\d+[a-z0-9.-]*)\b", notes_norm)
            if code:
                fact_keys = (code.group(1),)

        explicit_plant_noncompliance = any(k in notes_norm for k in (
            "impianto non conforme", "impianto non a norma", "non conformita dell'impianto",
            "impianti non conformi", "impianti non a norma",
        ))
        missing_plant_declarations = (
            (token == "impianti" or token.startswith("impianto_"))
            and any(k in notes_norm for k in (
                "assenza della dichiarazione", "assenza delle dichiarazioni",
                "non contiene una dichiarazione", "dichiarazioni non disponibili",
            ))
            and not explicit_plant_noncompliance
        )
        if token == "agibilita" and "non risulta agibile" in notes_norm:
            status, status_label, tone = "da_verificare", "Da verificare per singolo Bene", "ambra"
        if missing_plant_declarations:
            status, status_label, tone = "da_verificare", "Dichiarazioni non disponibili secondo la perizia", "ambra"
        if (
            token == "catastale" and cls == "uncertain" and "docfa" in notes_norm
            and any(k in notes_norm for k in ("difform", "non sussiste corrispondenza"))
        ):
            status, status_label, tone = "da_verificare", "Da verificare documentalmente", "ambra"
        excerpt = _find_excerpt(
            evidence, pages, category_keys, excerpt_keywords=excerpt_keys,
            required_fact_keywords=fact_keys,
        )
        evidence_view = excerpt or _missing_evidence(pages)
        page = _canonical_page(pages, excerpt)
        amount = _as_float(item.get("cost"))
        customer_summary = _first_sentence(item.get("notes"))
        buyer_impact = _CONFORMITY_WHY.get(token, _CONFORMITY_WHY_DEFAULT)
        if missing_plant_declarations:
            customer_summary = (
                "Non risultano disponibili alcune dichiarazioni di conformità degli impianti; "
                "verificare stato, certificabilità ed eventuali adeguamenti necessari."
            )
            buyer_impact = (
                "L’assenza delle dichiarazioni non prova da sola che gli impianti siano non conformi; "
                "servono verifiche sullo stato e sulla certificabilità."
            )
        elif token == "catastale" and "docfa" in notes_norm and any(
            k in notes_norm for k in ("redatte", "predispost", "preparat")
        ):
            customer_summary = (
                "Difformità rilevate; aggiornamenti DOCFA predisposti dal perito. "
                "Verificare l’avvenuta registrazione e la situazione catastale definitiva."
            )
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
                "customer_summary": customer_summary,
                "buyer_impact": buyer_impact,
                "recommended_action": (
                    "Verificare/regolarizzare secondo quanto indicato nella perizia."
                    if status in ("regolarizzabile", "non_conforme", "da_verificare")
                    else "Nessuna azione richiesta secondo la perizia."
                ),
                "amount": amount,
                "amount_display": item.get("cost_display") or format_eur(amount),
                "included_in_valuation": False,
                "timing": item.get("timing"),
                "pages": pages,
                "page": page,
                "evidence": evidence_view,
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
_CANCELLED_SENTENCE = "Da cancellare a cura della procedura con il decreto di trasferimento."
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
            description_blob = _norm(" ".join(card["details"]))
            future_or_negated = any(k in description_blob for k in (
                "da cancell", "a cura della procedura", "con decreto di trasferimento",
                "non risulta gia cancell", "non risulta cancell",
            ))
            explicitly_already = (
                not future_or_negated
                and bool(re.search(
                    r"(?:^gia\s+cancellat|(?:risulta|formalita|e)\s+(?:gia\s+)?cancellat)",
                    description_blob,
                ))
            )
            if explicitly_already:
                view["statement"] = "Già cancellata secondo la perizia."
                view["cancellation_state"] = "already_cancelled"
            else:
                view["statement"] = _CANCELLED_SENTENCE
                view["cancellation_state"] = "to_be_cancelled"
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
def _access_action_title(area: Any, summary: Any) -> str:
    target = _norm(f"{area} {summary}")
    if "magazzino" in target or "rustico" in target:
        return "Verificare il titolo di accesso al magazzino/rustico"
    if "garage" in target or "autorimessa" in target:
        return "Verificare il titolo di accesso al garage/autorimessa"
    return "Verificare il titolo di accesso all’immobile"


def _build_verifiche(
    report: Dict[str, Any],
    conformity_findings: List[Dict[str, Any]],
    formality_findings: List[Dict[str, Any]],
    occupancy_section: Optional[Dict[str, Any]],
    numeri: Optional[Dict[str, Any]],
    lot_id: Optional[str],
) -> Optional[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []

    # Risks not represented by compliance cards still need an authoritative,
    # buyer-impact-ranked professional action (access, condition, etc.).
    for section in report.get("risk_sections") or []:
        for risk in section.get("items") or []:
            if not isinstance(risk, dict):
                continue
            blob = _norm(f"{risk.get('area')} {risk.get('summary')}")
            if "access" in blob and any(k in blob for k in ("altra proprieta", "altra proprietà", "terz")):
                items.append({
                    "title": _access_action_title(risk.get("area"), risk.get("summary")),
                    "why": "L’accesso descritto avviene attraverso un immobile di altra proprietà non compreso nella procedura. Verificare servitù, titolo opponibile e futura utilizzabilità dell’accesso.",
                    "status": "da_verificare", "page": (_pages(risk.get("evidence_pages")) or [None])[0],
                    "link": "altri", "severity": 0,
                })
            elif any(k in blob for k in ("copertura dannegg", "tetto dannegg")):
                items.append({
                    "title": "Verificare la copertura danneggiata del magazzino",
                    "why": "Il danno può incidere sull’utilizzabilità e comportare costi non quantificati.",
                    "status": "da_verificare", "page": (_pages(risk.get("evidence_pages")) or [None])[0],
                    "link": "altri", "severity": 5,
                })

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
        reconciliation = numeri.get("riconciliazione")
        items.append(
            {
                "title": "Calcolo da chiarire" if reconciliation else "Chiarire alcuni importi indicati nella perizia",
                "why": reconciliation.get("explanation") if reconciliation else "Alcuni importi non hanno un ruolo chiaro nella catena di valore.",
                "status": "da_verificare",
                "page": None,
                "link": "numeri",
                "severity": 3,
            }
        )

    # 3. Technical actions. Building irregularities and asset-level agibilità
    # form one buyer decision, while their detailed findings remain distinct.
    actionable = [
        f for f in conformity_findings
        if f["status"] in ("regolarizzabile", "non_conforme", "da_verificare")
    ]
    building = next((f for f in actionable if f.get("topic") == "edilizia"), None)
    agibilita = next((f for f in actionable if f.get("topic") == "agibilita"), None)
    consumed: set = set()
    if building or agibilita:
        anchor = building or agibilita
        if building and agibilita:
            title = "Verificare le difformità edilizie e l’agibilità del garage"
            why = (
                "Confermare con il Comune modalità, ammissibilità e costi delle regolarizzazioni, "
                "oltre a possibilità e condizioni di regolarizzazione del garage non agibile."
            )
        elif building:
            title = "Verificare le difformità edilizie e i titoli abilitativi"
            why = building["buyer_impact"]
        else:
            title = "Verificare l’agibilità per singolo Bene"
            why = agibilita["buyer_impact"]
        items.append({
            "title": title, "why": why, "status": "da_verificare",
            "page": anchor.get("page"), "link": "conformita",
            "finding_id": anchor["finding_id"], "severity": 1,
        })
        consumed.update(f["finding_id"] for f in (building, agibilita) if f)

    technical_priority = {
        "catastale": 4,
        "impianti": 6,
        "impianto_gas": 6,
        "impianto_elettrico": 6,
        "ape": 7,
    }
    technical_title = {
        "catastale": "Verificare registrazione DOCFA e allineamento catastale definitivo",
        "impianti": "Verificare dichiarazioni e stato degli impianti",
        "impianto_gas": "Verificare dichiarazioni e stato dell’impianto gas",
        "impianto_elettrico": "Verificare dichiarazioni e stato dell’impianto elettrico",
        "ape": "Verificare la disponibilità dell’APE",
    }
    for f in actionable:
        if f["finding_id"] in consumed:
            continue
        topic = f.get("topic")
        # Unknown technical topics remain visible after the eight principal
        # buyer-impact checks; they never displace a more material named risk.
        priority = technical_priority.get(topic, 8)
        items.append({
            "title": technical_title.get(topic, f"Verificare/regolarizzare: {f['title'].lower()}"),
            "why": f["buyer_impact"], "status": "da_verificare",
            "page": f.get("page"), "link": "conformita",
            "finding_id": f["finding_id"], "severity": priority,
        })

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
    displayed_items = items[:8]
    return {"items": displayed_items, "total": len(displayed_items)}


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


def _source_excerpt_supports_topic(
    report: Dict[str, Any], topic: str, section: str, excerpt: Any
) -> bool:
    """Fail closed unless a decisive-source excerpt proves its normalized topic."""
    text = _semantic_norm(excerpt)
    topic_norm = _norm(topic)
    if not text:
        return False

    matching_item = next((
        item for item in report.get("compliance_section") or []
        if isinstance(item, dict) and _semantic_norm(item.get("area")) == _semantic_norm(topic)
    ), None)
    notes = _norm((matching_item or {}).get("notes"))
    if "completezza documentazione" in topic_norm:
        return (
            ("documentazione" in text or "art 567" in text)
            and any(k in text for k in (
                "risulta completa", "documentazione completa", "dichiarata completa",
            ))
        )
    if "titolarita" in topic_norm:
        if re.search(r"\b1\s*/\s*1\b", notes):
            return "proprieta 1 1" in text or "diritto di proprieta 1 1" in text
        return "proprieta" in text or "diritto" in text
    if "continuita" in topic_norm:
        return any(k in text for k in (
            "sussistenza della continuita", "continuita nelle trascrizioni",
            "continuita delle trascrizioni dichiarata",
        ))
    if any(k in topic_norm for k in (
        "edilizia residenziale pubblica", "edilizia convenzionata",
        "alloggio pubblico", "alloggi pubblici",
    )):
        return any(k in text for k in (
            "edilizia residenziale pubblica", "edilizia convenzionata",
            "alloggio pubblico", "alloggi pubblici",
        ))
    if any(k in topic_norm for k in ("urbanistica", "destinazione urbanistica", "pgt")):
        code = re.search(r"\b([a-z]{2,}\s*\d+[a-z0-9.-]*)\b", notes)
        if code and _semantic_norm(code.group(1)) not in text:
            return False
        return any(k in text for k in ("urbanistica", "destinazione urbanistica", "pgt"))
    if "ape" in topic_norm or "energetic" in topic_norm:
        return "ape" in text or "certificat" in text and "energetic" in text
    if "agibil" in topic_norm or "abitabil" in topic_norm:
        return "agibil" in text or "abitabil" in text
    if "impiant" in topic_norm and "gas" in topic_norm:
        return "gas" in text and ("impiant" in text or "dichiaraz" in text)
    if "impiant" in topic_norm and "elettric" in topic_norm:
        return "elettric" in text and ("impiant" in text or "dichiaraz" in text)
    if "impiant" in topic_norm:
        return "impiant" in text and any(k in text for k in ("conform", "dichiaraz", "stato"))
    if any(k in topic_norm for k in ("edilizia", "regolarita edilizia")):
        return any(k in text for k in (
            "conformita edilizia", "regolarita edilizia", "difform", "titolo",
            "sanatori", "ripristin",
        ))
    if "vincol" in topic_norm:
        return any(k in text for k in ("vincol", "demanial", "usi civici"))
    if "catastal" in topic_norm or "docfa" in topic_norm:
        return any(k in text for k in ("catastal", "docfa", "corrispondenza"))
    if any(k in topic_norm for k in ("valore", "prezzo", "stima")):
        return "valore" in text or "prezzo" in text or "stima" in text
    if "ipoteca" in topic_norm:
        return "ipoteca" in text or "capitale" in text
    if "pignoram" in topic_norm:
        return "pignoram" in text
    if "occupa" in topic_norm:
        return any(k in text for k in ("occupa", "residen", "locazion", "esecutat"))

    words = [w for w in _semantic_norm(topic).split() if len(w) >= 5]
    return bool(words and any(w in text for w in words[:4]))


def _build_sources(report: Dict[str, Any], evidence: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    # One source per topic, but validate every candidate before marking the topic
    # seen: an early wrong excerpt must not starve a later decisive one.
    primary: List[Dict[str, Any]] = []
    surface_pages: set = set()
    total = 0
    component_rows = {
        _semantic_norm(row.get("label")): row
        for row in (report.get("money_sections") or {}).get("uncertain_money") or []
        if isinstance(row, dict) and _is_component_value(row)
    }
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    topic_order: List[str] = []
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
        if key not in grouped:
            grouped[key] = []
            topic_order.append(key)
        grouped[key].append(entry)

    for key in topic_order:
        candidates = grouped[key]
        first = candidates[0]
        section = str(first.get("report_section") or "")
        topic = str(first.get("topic") or "")
        total += 1
        pri = _source_priority(topic, section)
        page = next((p for p in (
            (_pages([entry.get("page")]) or [None])[0] for entry in candidates
        ) if p is not None), None)
        excerpt = None
        component = component_rows.get(_semantic_norm(topic))
        if component:
            match = _COMPONENT_VALUE_RE.search(str(component.get("label") or ""))
            bene = f"Bene N° {match.group(1)}" if match else None
            amount = _as_float(component.get("amount"))
            valid = _find_excerpt(
                candidates, component.get("evidence_pages") or [entry.get("page") for entry in candidates],
                (_semantic_norm(topic),), expected_amount=amount, expected_bene=bene,
                expected_asset=_component_label(component),
            )
            if valid:
                excerpt, page = valid["excerpt"], valid["page"]
            else:
                recovered = _find_cached_component_excerpt(
                    report,
                    component.get("evidence_pages") or [entry.get("page") for entry in candidates],
                    bene,
                    amount,
                    _component_label(component),
                )
                excerpt = recovered.get("excerpt") if recovered else None
                if recovered:
                    page = recovered.get("page")
        else:
            valid_candidates: List[Tuple[int, int, str]] = []
            for entry in candidates:
                candidate_excerpt = entry.get("perizia_excerpt") if entry.get("coverage_status") == "covered" else None
                if not _source_excerpt_supports_topic(report, topic, section, candidate_excerpt):
                    continue
                candidate_page = (_pages([entry.get("page")]) or [999])[0]
                valid_candidates.append((len(str(candidate_excerpt)), candidate_page, str(candidate_excerpt)))
            if valid_candidates:
                _, page, excerpt = min(valid_candidates, key=lambda row: (row[0], row[1]))
        display_topic = topic
        if (
            _norm(topic) == "valore di mercato" and excerpt
            and "valore di stima" in _norm(excerpt) and component_rows
        ):
            display_topic = "Valore di stima prima dei deprezzamenti"
        primary.append(
            {
                "source_id": _finding_id("acquisto", key, None, page, None).replace("acq-", "src-"),
                "page": page,
                "title": display_topic,
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
    if not (finding.get("evidence") or {}).get("excerpt"):
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
    report_status: str, findings: List[Dict[str, Any]], readiness: Dict[str, Any],
    report: Optional[Dict[str, Any]] = None,
    verifiche: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    report = report or {}
    verifiche = verifiche or {}
    open_confirmations = any(f.get("confirmation") for f in findings)
    open_checks = [
        item for item in verifiche.get("items") or []
        if isinstance(item, dict) and item.get("status") in _OPEN_CHECKLIST_STATUSES
    ]
    # Red is reserved for a genuinely fail-closed report or a report that a
    # blocking finding pushed into technical review. Interactive safe statuses
    # (lot selection / money confirmation) are amber, never red.
    if readiness["state"] == "TECHNICAL_REVIEW_REQUIRED":
        level = "rosso"
    elif report_status != "REPORT_READY":
        level = "ambra"
    elif not any(_is_actionable(f) for f in findings) and not open_confirmations and not open_checks:
        level = "verde"
    else:
        level = "ambra"

    wording = dict(_ESITO_WORDING[level])
    has_access_dependency = any(
        "access" in _norm(f"{item.get('area')} {item.get('summary')}")
        and "altra proprieta" in _norm(f"{item.get('area')} {item.get('summary')}")
        for section in report.get("risk_sections") or []
        for item in section.get("items") or [] if isinstance(item, dict)
    )
    if level == "ambra" and has_access_dependency:
        wording["headline"] = "ATTENZIONE — Verifiche tecniche e legali necessarie prima di procedere"
    drivers: List[Dict[str, Any]] = []
    seen_titles: set = set()
    candidates: List[Dict[str, Any]] = []
    for item in open_checks:
        title = item.get("title")
        candidates.append({
            "finding_id": item.get("finding_id") or _finding_id(
                "altri", _norm(title), None, item.get("page"), None
            ),
            "title": title,
            "section": item.get("link") or "verifiche",
            "severity": item.get("severity", 9),
        })
    for finding in findings:
        if _is_actionable(finding) or finding.get("status") == "verifica_tecnica_richiesta":
            candidates.append({
                "finding_id": finding["finding_id"],
                "title": finding.get("title"),
                "section": finding["section"],
                "severity": finding.get("severity", 9),
            })
    for candidate in sorted(candidates, key=lambda x: x.get("severity", 9)):
        title = candidate.get("title")
        if _norm(title) in seen_titles:
            continue
        seen_titles.add(_norm(title))
        drivers.append({
            "finding_id": candidate["finding_id"],
            "title": title,
            "section": candidate["section"],
        })
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
    if sections.get("verifiche"):
        checklist = sections["verifiche"]
        readiness["professional_checks_open"] = sum(
            1 for item in checklist.get("items") or []
            if item.get("status") in _OPEN_CHECKLIST_STATUSES
            and item.get("status") != "conferma_necessaria"
        )
        # Some authoritative checklist actions (for example a third-party
        # access dependency) are sourced from risk_sections and intentionally do
        # not fabricate a conformity finding.  Their open count must still drive
        # readiness state and its customer label.
        if (
            readiness["state"] == "COMPLETE_FOR_EXPORT"
            and readiness["professional_checks_open"] > 0
        ):
            readiness["state"] = "READY_FOR_REVIEW"
            readiness["label"] = _READINESS_LABEL["READY_FOR_REVIEW"]
    readiness["confirmations_open"] = max(
        0, readiness["confirmations_total"] - readiness["confirmations_done"]
    )
    readiness["information_declared"] = sum(
        1 for f in findings if f.get("status") in {"dichiarato_perizia", "conforme"}
    )
    readiness["information_confirmed"] = readiness["confirmations_done"]
    readiness["information_resolved"] = sum(
        1 for f in findings if f.get("status") in {"completato", "confermato_utente"}
    )
    sections["stato_verifiche"] = {
        "label": readiness["label"],
        "confirmations_total": readiness["confirmations_total"],
        "confirmations_done": readiness["confirmations_done"],
        "professional_checks_open": readiness["professional_checks_open"],
        "confirmations_open": readiness["confirmations_open"],
        "information_declared": readiness["information_declared"],
        "information_confirmed": readiness["information_confirmed"],
        "information_resolved": readiness["information_resolved"],
    }
    esito = _build_esito(
        report_status, findings, readiness, report, sections.get("verifiche")
    )

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
