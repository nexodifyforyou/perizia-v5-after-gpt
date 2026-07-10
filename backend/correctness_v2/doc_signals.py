"""
Deterministic material-fact signal extraction from perizia page text.

This module reads the extracted document truth (input_pages) and produces
page-level "signals": compact, categorized markers of material facts that a
customer-grade report must account for. It is the document side of the
no-silent-omissions coverage gate (coverage_audit.py compares these signals
against the software output).

HARD RULES:
  * 100% deterministic: regex/keyword detection only. NO LLM, NO network.
  * GENERIC: never branches on a specific tribunale, city or document.
  * NEVER invents facts: every signal carries the page number and a short
    snippet of the source text so it can be audited by a human.

The same detectors also power the deterministic "Superfici e dati catastali"
extraction used by the contract builder (document text read deterministically,
same precedent as the shared-summary projection).
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

# Severity levels (kept in lockstep with coverage_audit.json schema).
SEV_CRITICAL = "critical"
SEV_IMPORTANT = "important"
SEV_USEFUL = "useful"
SEV_BACKGROUND = "background"


def _strip_accents(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c)
    )


def norm_text(text: Any) -> str:
    return _strip_accents(str(text or "")).lower()


def normalize_ws(text: Any) -> str:
    """Collapse all whitespace runs to single spaces (verbatim otherwise).

    The ONLY normalization allowed on customer evidence excerpts: the excerpt
    builder (customer_report) and the verbatim gate (quality_report) MUST use
    this same function so the substring check can never drift."""
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _clean_snippet(text: str, limit: int = 160) -> str:
    snippet = re.sub(r"\s+", " ", str(text or "")).strip()
    return snippet[:limit]


def page_number(page: Dict[str, Any]) -> Optional[int]:
    try:
        return int(page.get("page_number"))
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Money amount parsing (Italian formats)
# ---------------------------------------------------------------------------
# "€ 43.654,20", "Euro 150.000,00", "38.110,20", "3720,00", "€150000".
# PDF text extraction sometimes inserts a single space-like char after the
# thousands-separator dot ("452. 494,00" instead of "452.494,00"). Tolerate one
# optional horizontal space (incl. nbsp / narrow-nbsp / thin space) inside a
# thousands group so grouped amounts are read whole, not split into fragments.
_THOU = r"\.[ \u00a0\u202f\u2009]?\d{3}"
_AMOUNT_WITH_CURRENCY_RE = re.compile(
    r"(?:€|\beuro\b|\beur\b)\s*:?\s*(\d{1,3}(?:" + _THOU + r")+(?:,\d{1,2})?|\d+(?:,\d{1,2})?)",
    re.IGNORECASE,
)
# Decimal-comma or dot-grouped numbers even without a currency marker.
_AMOUNT_BARE_RE = re.compile(r"\b(\d{1,3}(?:" + _THOU + r")+(?:,\d{1,2})?|\d{2,9},\d{2})\b")
_DATE_RE = re.compile(r"\d{1,2}/\d{1,2}/\d{2,4}")
_LAW_REF_RE = re.compile(r"\b(?:n\.?|nn\.?|legge|l\.|d\.?p\.?r\.?|art\.?)\s*\d", re.IGNORECASE)

# A bare number counts as money only if a money word appears close to it.
_MONEY_CONTEXT_WORDS = (
    "€", "euro", "importo", "valore", "prezzo", "costo", "costi", "spese", "spesa",
    "canone", "rendita", "capitale", "cauzione", "offerta", "rialzo", "deposito",
    "oneri", "indennit", "debito", "credito", "mutuo", "stima", "deprezzament",
    "riduzione", "detrazion", "arretrat", "insolut",
)


def parse_amount(raw: str) -> Optional[float]:
    """Parse an Italian-formatted number string to float ('43.654,20' -> 43654.2)."""
    if not raw:
        return None
    # Drop any whitespace (incl. nbsp/thin space PDFs insert between digit groups)
    # before stripping the Italian thousands dot and normalizing the decimal comma.
    cleaned = re.sub(r"\s", "", raw).replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _is_datelike(text: str, start: int, end: int) -> bool:
    window = text[max(0, start - 12) : min(len(text), end + 12)]
    return bool(_DATE_RE.search(window))


def _has_money_context(text_norm: str, start: int, end: int, span: int = 90) -> bool:
    window = text_norm[max(0, start - span) : min(len(text_norm), end + span)]
    return any(word in window for word in _MONEY_CONTEXT_WORDS)


def amounts_in_text(text: Any) -> List[Tuple[float, int, int]]:
    """All plausible money amounts in a text with their spans (deterministic).

    Conservative by design: bare numbers only count when money context words are
    nearby, date-like and law-reference tokens are skipped, and plain years are
    ignored. Used both for document signals and for grounding report amounts.
    """
    raw = str(text or "")
    n = norm_text(raw)
    found: List[Tuple[float, int, int]] = []
    seen_spans: List[Tuple[int, int]] = []

    def overlaps(s: int, e: int) -> bool:
        return any(not (e <= s0 or s >= e0) for s0, e0 in seen_spans)

    for m in _AMOUNT_WITH_CURRENCY_RE.finditer(raw):
        amount = parse_amount(m.group(1))
        if amount is None:
            continue
        found.append((amount, m.start(1), m.end(1)))
        seen_spans.append((m.start(1), m.end(1)))

    for m in _AMOUNT_BARE_RE.finditer(raw):
        s, e = m.start(1), m.end(1)
        if overlaps(s, e):
            continue
        if _is_datelike(raw, s, e):
            continue
        prefix = raw[max(0, s - 8) : s]
        if _LAW_REF_RE.search(prefix):
            continue
        amount = parse_amount(m.group(1))
        if amount is None:
            continue
        # Bare integers that look like years or serial numbers need money context.
        if "," not in m.group(1) and not _has_money_context(n, s, e):
            continue
        if 1900 <= amount <= 2100 and "," not in m.group(1):
            continue
        found.append((amount, s, e))
        seen_spans.append((s, e))
    return found


# ---------------------------------------------------------------------------
# Money kind classification (by nearby context, generic Italian)
# ---------------------------------------------------------------------------
# Ordered: first match wins. (kind, severity, label_it, context regex)
#
# Ordering is semantic, not cosmetic:
#   * auction terms first (most specific wording);
#   * market COMPARABLES (OMI/borsino/annunci/listini) before the value kinds,
#     so a listing price near "valore di mercato" wording stays a comparable;
#   * among the value kinds: judicial sale BEFORE market BEFORE state-of-fact.
#     Italian perizie append the boilerplate qualifier "nello stato di fatto e
#     di diritto in cui si trova" to essentially EVERY value line, so an
#     explicit head value phrase must win over that trailing qualifier:
#     "Valore di vendita giudiziaria ... nello stato di fatto" is the judicial
#     sale value, "Valore di Mercato ... nello stato di fatto" is the market
#     value. "Stato di fatto"/"valore di realizzo" (net realizable value in
#     the current state, NOT the judicial sale) is the current/state value and
#     is matched last among the value kinds.
_MONEY_KINDS: List[Tuple[str, str, str, re.Pattern]] = [
    ("prezzo_base", SEV_CRITICAL, "Prezzo base d'asta", re.compile(r"prezzo\s+base|base\s+d'?asta")),
    ("offerta_minima", SEV_CRITICAL, "Offerta minima", re.compile(r"offerta\s+minima")),
    ("rialzo_minimo", SEV_IMPORTANT, "Rialzo minimo", re.compile(r"rialzo\s+minimo|aumento\s+minimo")),
    ("cauzione", SEV_IMPORTANT, "Cauzione", re.compile(r"cauzione")),
    ("comparativo", SEV_BACKGROUND, "Comparativo di mercato", re.compile(r"\bomi\b|borsin|annunc|comparabil|comparativ|listino|quotazion")),
    ("valore_vendita", SEV_CRITICAL, "Valore di vendita giudiziaria", re.compile(r"vendita\s+giudiziari|valore\s+di\s+vendita|prezzo\s+di\s+vendita|valore\s+giudiziario")),
    ("valore_mercato", SEV_CRITICAL, "Valore di mercato", re.compile(r"valore\s+di\s+mercato|piu\s+probabile\s+valore|valore\s+commerciale")),
    ("valore_stato", SEV_CRITICAL, "Valore nello stato di fatto", re.compile(r"stato\s+di\s+fatto|valore\s+di\s+realizzo")),
    ("rendita", SEV_IMPORTANT, "Rendita catastale", re.compile(r"rendita")),
    ("canone", SEV_IMPORTANT, "Canone / importo di locazione", re.compile(r"canone|affitto|locazion")),
    ("spese_condominiali", SEV_IMPORTANT, "Spese condominiali", re.compile(r"condomini|millesim|arretrat|insolut")),
    ("costo_regolarizzazione", SEV_CRITICAL, "Costi di regolarizzazione", re.compile(r"regolarizz|sanator|ripristin|adeguament|messa\s+a\s+norma|docfa|pratica\s+edilizia")),
    ("cancellazione", SEV_CRITICAL, "Costi di cancellazione formalità", re.compile(r"cancellaz")),
    ("formalita_capitale", SEV_BACKGROUND, "Importo formalità (ipoteca/pignoramento)", re.compile(r"ipotec|pignoram|mutuo|capitale|iscrizion|trascrizion")),
    ("deprezzamento", SEV_IMPORTANT, "Deprezzamento / riduzione", re.compile(r"deprezzament|riduzione|decurtazion|abbattiment|detrazion")),
    ("oneri", SEV_IMPORTANT, "Oneri / spese", re.compile(r"oneri|notaril|provvigion|tribut")),
]


_VALUE_KIND_NAMES = ("valore_vendita", "valore_stato", "valore_mercato")


def classify_money_context(context_norm: str) -> Tuple[str, str, str]:
    """Return (kind, severity, label_it) for the text around an amount.

    An explicit value-role phrase beats comparable/methodology wording: 'valore
    di mercato determinato con procedimento comparativo / dalle quotazioni OMI'
    names the ROLE (market value) — the comparable words only describe how the
    perito derived it. Listing rows (annunci/borsino) don't carry the explicit
    value phrases, so they still classify as comparables.
    """
    for kind, severity, label, pattern in _MONEY_KINDS:
        if not pattern.search(context_norm):
            continue
        if kind == "comparativo":
            for v_kind, v_sev, v_label, v_pattern in _MONEY_KINDS:
                if v_kind in _VALUE_KIND_NAMES and v_pattern.search(context_norm):
                    return v_kind, v_sev, v_label
        return kind, severity, label
    return "importo_generico", SEV_USEFUL, "Importo indicato in perizia"


# Money kinds that describe THE VALUE CONCEPTS a customer report must carry.
VALUE_KINDS = {
    "prezzo_base", "offerta_minima", "rialzo_minimo", "cauzione",
    "valore_mercato", "valore_stato", "valore_vendita",
    "costo_regolarizzazione", "cancellazione",
}

# ---------------------------------------------------------------------------
# Money ROLE taxonomy: the semantic role a money fact plays. The page audit is
# role-aware — an amount only covers a document fact when BOTH the amount and
# the role match (never amount-only matching).
# ---------------------------------------------------------------------------
ROLE_MARKET_VALUE = "market_value"
ROLE_REGULARIZATION_COST = "regularization_cost"
ROLE_STATE_OF_FACT_VALUE = "state_of_fact_value"
ROLE_JUDICIAL_SALE_VALUE = "judicial_sale_value"
ROLE_AUCTION_BASE_PRICE = "auction_base_price"
ROLE_MINIMUM_BID = "minimum_bid"
ROLE_BUYER_SIDE_COST = "buyer_side_cost"
ROLE_PROCEDURE_CANCELLED_FORMALITY = "procedure_cancelled_formality"
ROLE_COMPARABLE_MARKET_VALUE = "comparable_market_value"
ROLE_CADASTRAL_INCOME = "cadastral_income"
ROLE_RENT = "rent"
ROLE_CONDOMINIUM_EXPENSE = "condominium_expense"
ROLE_UNCERTAIN_MONEY = "uncertain_money"
# Extra internal roles (still generic; not in the customer-facing taxonomy).
ROLE_AUCTION_INCREMENT = "auction_increment"
ROLE_AUCTION_DEPOSIT = "auction_deposit"
ROLE_DEPRECIATION = "depreciation"
ROLE_GENERIC_CHARGE = "generic_charge"

ROLE_BY_KIND = {
    "prezzo_base": ROLE_AUCTION_BASE_PRICE,
    "offerta_minima": ROLE_MINIMUM_BID,
    "rialzo_minimo": ROLE_AUCTION_INCREMENT,
    "cauzione": ROLE_AUCTION_DEPOSIT,
    "comparativo": ROLE_COMPARABLE_MARKET_VALUE,
    "valore_vendita": ROLE_JUDICIAL_SALE_VALUE,
    "valore_stato": ROLE_STATE_OF_FACT_VALUE,
    "valore_mercato": ROLE_MARKET_VALUE,
    "rendita": ROLE_CADASTRAL_INCOME,
    "canone": ROLE_RENT,
    "spese_condominiali": ROLE_CONDOMINIUM_EXPENSE,
    "costo_regolarizzazione": ROLE_REGULARIZATION_COST,
    "cancellazione": ROLE_BUYER_SIDE_COST,
    "formalita_capitale": ROLE_PROCEDURE_CANCELLED_FORMALITY,
    "deprezzamento": ROLE_DEPRECIATION,
    "oneri": ROLE_GENERIC_CHARGE,
    "importo_generico": ROLE_UNCERTAIN_MONEY,
}

# Italian display names for roles (page audit / quality tables).
ROLE_LABELS_IT = {
    ROLE_MARKET_VALUE: "valore di mercato",
    ROLE_REGULARIZATION_COST: "costo di regolarizzazione",
    ROLE_STATE_OF_FACT_VALUE: "valore nello stato di fatto",
    ROLE_JUDICIAL_SALE_VALUE: "valore di vendita giudiziaria",
    ROLE_AUCTION_BASE_PRICE: "prezzo base d'asta",
    ROLE_MINIMUM_BID: "offerta minima",
    ROLE_BUYER_SIDE_COST: "costo a carico dell'acquirente",
    ROLE_PROCEDURE_CANCELLED_FORMALITY: "formalità cancellata dalla procedura",
    ROLE_COMPARABLE_MARKET_VALUE: "comparativo di mercato",
    ROLE_CADASTRAL_INCOME: "rendita catastale",
    ROLE_RENT: "canone di locazione",
    ROLE_CONDOMINIUM_EXPENSE: "spesa condominiale",
    ROLE_UNCERTAIN_MONEY: "importo da verificare",
    ROLE_AUCTION_INCREMENT: "rialzo minimo",
    ROLE_AUCTION_DEPOSIT: "cauzione",
    ROLE_DEPRECIATION: "deprezzamento/riduzione",
    ROLE_GENERIC_CHARGE: "oneri/spese",
}


# Label-kinds whose role is CLEAR background context even when the analyst
# filed the amount as "uncertain": comparables get their own section, context
# kinds (rendita/canone/condominio/capitale di formalità) are informational.
# Shared by the renderer (bucketing) and the coverage audit (expectations) so
# the two can never drift apart.
COMPARATIVE_LABEL_KINDS = frozenset({"comparativo"})
CONTEXT_LABEL_KINDS = frozenset({
    "rendita", "canone", "spese_condominiali", "formalita_capitale",
})

# ---------------------------------------------------------------------------
# Role compatibility (single source of truth for the whole pipeline).
#
# CORE roles state what the buyer pays or what the property is worth: showing
# an amount under the wrong core role (or presenting a core value as mere
# background, or background data as a confirmed value) MISLEADS the buyer.
# BACKGROUND roles are informational context: two background roles disagreeing
# about the same amount is a same-economic-fact/different-safe-bucket case.
# ---------------------------------------------------------------------------
CORE_MONEY_ROLES = frozenset({
    ROLE_MARKET_VALUE, ROLE_STATE_OF_FACT_VALUE, ROLE_JUDICIAL_SALE_VALUE,
    ROLE_AUCTION_BASE_PRICE, ROLE_MINIMUM_BID, ROLE_AUCTION_INCREMENT,
    ROLE_AUCTION_DEPOSIT, ROLE_REGULARIZATION_COST, ROLE_BUYER_SIDE_COST,
    ROLE_DEPRECIATION, ROLE_GENERIC_CHARGE,
})
BACKGROUND_MONEY_ROLES = frozenset({
    ROLE_COMPARABLE_MARKET_VALUE, ROLE_CADASTRAL_INCOME, ROLE_RENT,
    ROLE_CONDOMINIUM_EXPENSE, ROLE_PROCEDURE_CANCELLED_FORMALITY,
    ROLE_UNCERTAIN_MONEY,
})

# Roles that are different names for the SAME economic fact: all four are
# "a cost the perito deducted/charged" (a merged chain row may carry any of
# them, and generic 'oneri e spese' wording covers each). Swaps INSIDE this
# family never mislead about value-vs-cost-vs-background; swaps across
# families (value roles, background roles) stay strict.
_COMPATIBLE_ROLE_GROUPS = (
    frozenset({
        ROLE_REGULARIZATION_COST, ROLE_DEPRECIATION, ROLE_BUYER_SIDE_COST,
        ROLE_GENERIC_CHARGE,
    }),
)


def roles_compatible(role_a: str, role_b: str) -> bool:
    """True when two roles describe the same economic fact."""
    if role_a == role_b:
        return True
    if ROLE_UNCERTAIN_MONEY in (role_a, role_b):
        return True  # explicit uncertainty accounts for any role
    return any(
        role_a in group and role_b in group for group in _COMPATIBLE_ROLE_GROUPS
    )


def conflict_is_misleading(doc_role: str, entry_roles: Any) -> bool:
    """Is showing this amount under ``entry_roles`` misleading for ``doc_role``?

    Misleading whenever a CORE role is involved on either side (a value/cost
    presented under the wrong role, a core value demoted to background, or
    background data promoted to a confirmed value). Background-vs-background
    disagreements are safe-bucket differences, not deception.
    """
    if doc_role in CORE_MONEY_ROLES:
        return True
    return any(r in CORE_MONEY_ROLES for r in entry_roles or ())


def role_for_kind(kind: Any) -> str:
    return ROLE_BY_KIND.get(str(kind or ""), ROLE_UNCERTAIN_MONEY)


def label_kind(label: Any) -> str:
    """Classify a report row label into a money kind (same detector as pages)."""
    kind, _sev, _lab = classify_money_context(norm_text(label))
    return kind


# ---------------------------------------------------------------------------
# Topic detectors (non-money page facts)
# ---------------------------------------------------------------------------
# (category, kind, severity, label_it, page-text regex, report-match tokens)
# report-match tokens: ANY of them appearing in the report text pool counts as
# topic coverage (accent-stripped, lowercase matching).
_TOPICS: List[Tuple[str, str, str, str, re.Pattern, Tuple[str, ...]]] = [
    (
        "occupancy", "stato_occupazione", SEV_CRITICAL, "Stato di occupazione",
        re.compile(r"occupat[oa]|\blocato\b|conduttore|inquilin|libero\s+da\s+persone|liber[oa]\s+e\s+disponibile"),
        ("occupat", "locato", "libero", "conduttore", "occupazione"),
    ),
    (
        "occupancy", "contratto_locazione", SEV_CRITICAL, "Contratto di locazione/affitto",
        re.compile(r"contratto\s+di\s+(locazione|affitto)|contratto\s+.{0,20}4\s*\+\s*4"),
        ("contratto di locazione", "contratto di affitto", "affitto", "locazione"),
    ),
    (
        "occupancy", "registrazione_contratto", SEV_IMPORTANT, "Registrazione del contratto",
        re.compile(r"registrat[oa]\s+(il|in\s+data|presso|a)\b"),
        ("registrat",),
    ),
    (
        "occupancy", "opponibilita", SEV_IMPORTANT, "Opponibilità / data certa del titolo",
        re.compile(r"opponibil|antecedente\s+il\s+pignoramento|anteriore\s+al\s+pignoramento|data\s+certa"),
        ("opponibil", "antecedente il pignoramento", "anteriore al pignoramento", "data certa"),
    ),
    (
        "occupancy", "occupazione_senza_titolo", SEV_IMPORTANT, "Possibile occupazione senza titolo",
        re.compile(r"senza\s+titolo|abusivamente\s+occupat"),
        ("senza titolo",),
    ),
    (
        "compliance", "urbanistica", SEV_IMPORTANT, "Conformità urbanistica",
        re.compile(r"urbanistic"),
        ("urbanistic",),
    ),
    (
        "compliance", "edilizia", SEV_IMPORTANT, "Conformità edilizia",
        re.compile(r"conformita\s+edilizia|difformita|abus[oi]\s+ediliz|sanatoria|accertamento\s+di\s+conformita|permesso\s+di\s+costruire|concessione\s+edilizia"),
        ("edilizia", "difformita", "sanatoria", "regolarizz"),
    ),
    (
        "compliance", "catastale", SEV_IMPORTANT, "Conformità catastale",
        re.compile(r"planimetria\s+catastale|conformita\s+catastale|difformita\s+catastale|variazione\s+catastale|docfa"),
        ("catastale",),
    ),
    (
        "compliance", "agibilita", SEV_IMPORTANT, "Agibilità / abitabilità",
        re.compile(r"agibilit|abitabilit"),
        ("agibilit", "abitabilit"),
    ),
    (
        "compliance", "ape", SEV_USEFUL, "APE / prestazione energetica",
        re.compile(r"\bape\b|prestazione\s+energetica|attestato\s+di\s+prestazione|classe\s+energetica"),
        ("ape", "energetic"),
    ),
    (
        "compliance", "impianti", SEV_IMPORTANT, "Impianti (gas/elettrico/idraulico)",
        re.compile(r"impiant[oi]\s+(elettric|gas|idraulic|termic|di\s+riscaldament)|certificazione\s+impiant|dichiarazione\s+di\s+conformita\s+impiant"),
        ("impiant",),
    ),
    (
        "formalities", "ipoteca", SEV_IMPORTANT, "Ipoteca",
        re.compile(r"ipotec"),
        ("ipotec",),
    ),
    (
        "formalities", "pignoramento", SEV_IMPORTANT, "Pignoramento",
        re.compile(r"pignoram"),
        ("pignoram",),
    ),
    (
        "formalities", "sequestro", SEV_IMPORTANT, "Sequestro",
        re.compile(r"\bsequestr"),
        ("sequestr",),
    ),
    (
        "formalities", "domanda_giudiziale", SEV_IMPORTANT, "Domanda giudiziale",
        re.compile(r"domanda\s+giudiziale"),
        ("domanda giudiziale",),
    ),
    (
        "formalities", "cancellazione_procedura", SEV_CRITICAL, "Cancellazione a cura della procedura",
        re.compile(r"a\s+cura\s+(e\s+spese\s+)?della\s+procedura|cancellazione\s+.{0,60}(procedura|decreto\s+di\s+trasferimento)"),
        ("cura della procedura", "cancellazione", "cancellati dalla procedura", "cancellate dalla procedura"),
    ),
    (
        "surface", "superficie", SEV_IMPORTANT, "Superficie / consistenza",
        re.compile(r"superficie\s+(commerciale|catastale|lorda|utile|complessiva)|consistenza\s+(commerciale|catastale)?|\bvani\b|\bm²\b|\bmq\b"),
        ("superficie", "consistenza", "vani", "mq", "m2"),
    ),
    (
        "cadastral", "rendita_catastale", SEV_IMPORTANT, "Rendita catastale",
        re.compile(r"rendita\s+catastale|rendita\s*[:€]"),
        ("rendita",),
    ),
    (
        "cadastral", "identificativi_catastali", SEV_USEFUL, "Identificativi catastali",
        re.compile(r"foglio\s+\d+|particella\s+\d+|mappale\s+\d+|subalterno\s+\d+|sub\.\s*\d+|categoria\s+[a-f]\s*/?\s*\d"),
        ("foglio", "particella", "mappale", "subalterno", "categoria"),
    ),
    (
        "expenses", "spese_condominiali", SEV_IMPORTANT, "Spese condominiali / arretrati",
        re.compile(r"spese\s+condominiali|millesim|arretrat|insolut[ei]"),
        ("condominial", "arretrat", "insolut", "millesim"),
    ),
    (
        "maintenance", "stato_manutentivo", SEV_USEFUL, "Stato di manutenzione",
        re.compile(r"manutenzion|degrado|vetust|cattivo\s+stato|pessimo\s+stato|buono\s+stato|discreto\s+stato"),
        ("manutenzion", "degrado", "stato"),
    ),
    (
        "access", "accesso_sopralluogo", SEV_USEFUL, "Accesso / sopralluogo",
        re.compile(r"non\s+(e\s+stato\s+)?possibile\s+accedere|non\s+accessibil|chiavi\s+non|mancanza\s+delle?\s+chiavi|senza\s+ascensore|privo\s+di\s+ascensore"),
        ("accessibil", "accedere", "chiavi", "ascensore", "sopralluogo"),
    ),
]


def topic_detectors() -> List[Tuple[str, str, str, str, re.Pattern, Tuple[str, ...]]]:
    return list(_TOPICS)


# ---------------------------------------------------------------------------
# Page signal extraction
# ---------------------------------------------------------------------------
def _context_window(text: str, start: int, end: int, before: int = 90, after: int = 45) -> str:
    lo = max(0, start - before)
    hi = min(len(text), end + after)
    return text[lo:hi]


def classification_window(text: str, spans: List[Tuple[float, int, int]], idx: int,
                          max_before: int = 120) -> str:
    """The text used to classify the role of amount ``idx``.

    Italian perizia labels PRECEDE their amount ("Valore di ...: €. 43.654,20"),
    so only the text BEFORE the amount is used, truncated at the end of the
    previous amount: without truncation, chained value lines ("Valore di
    mercato: € A ... Valore nello stato di fatto: € B") bleed the previous
    label into the next amount's window and swap money roles.
    """
    _amount, s, _e = spans[idx]
    lo = max(0, s - max_before)
    if idx > 0:
        prev_end = spans[idx - 1][2]
        lo = max(lo, prev_end)
    return text[lo:s]


def classify_after_fallback(text: str, spans: List[Tuple[float, int, int]], idx: int,
                            max_after: int = 80) -> Tuple[str, str, str]:
    """Fallback classification from the text AFTER the amount.

    Used ONLY when the before-window is inconclusive (importo_generico), for
    layouts where the label follows the amount ("EUR 75.000,00 prezzo base").
    The window stops at the next amount. Fail-safe: if a ':' follows the
    matched phrase anywhere in the window, the phrase is the heading of the
    NEXT line ("€ 43.654,20 € VALORE DI VENDITA GIUDIZIARIA (FJV): ...") — the
    amount stays generic rather than risking a swapped money role.
    """
    _amount, _s, e = spans[idx]
    hi = min(len(text), e + max_after)
    if idx + 1 < len(spans):
        hi = min(hi, spans[idx + 1][1])
    window = text[e:hi]
    n = norm_text(window)
    for kind, severity, label, pattern in _MONEY_KINDS:
        m = pattern.search(n)
        if not m:
            continue
        if ":" in n[m.start():]:
            continue  # heading of the NEXT value, not this amount's label
        return kind, severity, label
    return "importo_generico", SEV_USEFUL, "Importo indicato in perizia"


def extract_money_signals(pages: List[Dict[str, Any]], *, min_amount: float = 50.0) -> List[Dict[str, Any]]:
    """Per-page money signals, deduplicated by (page, kind, amount)."""
    signals: List[Dict[str, Any]] = []
    seen: set = set()
    for page in pages or []:
        pnum = page_number(page)
        text = str(page.get("text") or "")
        if pnum is None or not text.strip():
            continue
        spans = sorted(amounts_in_text(text), key=lambda t: t[1])
        for i, (amount, s, e) in enumerate(spans):
            if amount < min_amount:
                continue
            kind, severity, label = classify_money_context(
                norm_text(classification_window(text, spans, i))
            )
            if kind == "importo_generico":
                kind, severity, label = classify_after_fallback(text, spans, i)
            key = (pnum, kind, round(amount, 2))
            if key in seen:
                continue
            seen.add(key)
            signals.append(
                {
                    "signal_type": "money",
                    "page": pnum,
                    "category": _money_category(kind),
                    "kind": kind,
                    "severity": severity,
                    "label": label,
                    "amount": round(amount, 2),
                    "role": role_for_kind(kind),
                    "snippet": _clean_snippet(_context_window(text, s, e)),
                }
            )
    return signals


def _money_category(kind: str) -> str:
    if kind in {"prezzo_base", "offerta_minima", "rialzo_minimo", "cauzione"}:
        return "sale_terms"
    if kind == "rendita":
        return "cadastral"
    if kind == "canone":
        return "occupancy"
    if kind == "spese_condominiali":
        return "expenses"
    if kind == "formalita_capitale":
        return "formalities"
    if kind == "comparativo":
        return "comparables"
    return "money"


def extract_topic_signals(pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Per-page topic signals (one per topic per page)."""
    signals: List[Dict[str, Any]] = []
    for page in pages or []:
        pnum = page_number(page)
        text = str(page.get("text") or "")
        if pnum is None or not text.strip():
            continue
        n = norm_text(text)
        for category, kind, severity, label, pattern, report_tokens in _TOPICS:
            m = pattern.search(n)
            if not m:
                continue
            lo = max(0, m.start() - 40)
            snippet = _clean_snippet(text[lo : m.start() + 120])
            signals.append(
                {
                    "signal_type": "topic",
                    "page": pnum,
                    "category": category,
                    "kind": kind,
                    "severity": severity,
                    "label": label,
                    "amount": None,
                    "snippet": snippet,
                    "report_tokens": list(report_tokens),
                }
            )
    return signals


def extract_page_signals(pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """All deterministic page signals (money + topics), with stable signal ids."""
    signals = extract_money_signals(pages) + extract_topic_signals(pages)
    signals.sort(key=lambda s: (s["page"], s["signal_type"], s["kind"], s.get("amount") or 0))
    for idx, sig in enumerate(signals):
        sig["signal_id"] = f"p{sig['page']}:{sig['kind']}:{idx}"
    return signals


# ---------------------------------------------------------------------------
# Deterministic surface / cadastral fact extraction (for the customer report)
# ---------------------------------------------------------------------------
_SURFACE_FIELD_RES: List[Tuple[str, str, re.Pattern]] = [
    (
        "superficie_commerciale", "Superficie commerciale",
        re.compile(r"(?:superficie|consistenza)\s+commerciale[^\d]{0,60}?([\d.,]+)\s*(?:m|mq)", re.IGNORECASE),
    ),
    (
        "superficie_catastale", "Superficie catastale",
        re.compile(r"superficie\s+catastale[^\d]{0,60}?([\d.,]+)\s*(?:m|mq)", re.IGNORECASE),
    ),
    (
        "vani", "Consistenza (vani)",
        re.compile(r"(?:consistenza[^\d]{0,30}?|\b)([\d.,]+)\s*van[oi]", re.IGNORECASE),
    ),
    (
        "rendita_catastale", "Rendita catastale",
        re.compile(r"rendita(?:\s+catastale)?\s*[:\s]{0,4}(?:€|euro|eur)?\.?\s*([\d.,]+)", re.IGNORECASE),
    ),
]

_CADASTRAL_ID_RES: List[Tuple[str, str, re.Pattern]] = [
    ("foglio", "Foglio", re.compile(r"foglio\s*(?:n[.°]?\s*)?(\d+)", re.IGNORECASE)),
    ("particella", "Particella/Mappale", re.compile(r"(?:particella|mappale)\s*(?:n[.°]?\s*)?(\d+)", re.IGNORECASE)),
    ("subalterno", "Subalterno", re.compile(r"(?:subalterno|sub\.?)\s*(?:n[.°]?\s*)?(\d+)", re.IGNORECASE)),
    ("categoria", "Categoria catastale", re.compile(r"categoria\s*[:\s]?\s*([A-F]\s*/?\s*\d{1,2})", re.IGNORECASE)),
    ("classe", "Classe catastale", re.compile(r"classe\s*[:\s]?\s*(\d{1,2}|U)\b", re.IGNORECASE)),
]


def extract_surface_cadastral(pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Deterministic surface/cadastral facts read verbatim from page text.

    Each distinct value of a field becomes one fact with its evidence pages.
    Conflicting values are all kept (never guessed away) and flagged
    ``multiple_values=True`` so the renderer shows them as "da verificare".
    """
    values: Dict[str, Dict[str, Dict[str, Any]]] = {}

    def record(field: str, label: str, raw_value: str, pnum: int, numeric: bool) -> None:
        display = raw_value.strip().replace(" ", "")
        if numeric:
            amount = parse_amount(display)
            if amount is None or amount == 0:
                return
            display = raw_value.strip()
        bucket = values.setdefault(field, {})
        entry = bucket.setdefault(
            display,
            {"field": field, "label": label, "value": display, "evidence_pages": []},
        )
        if pnum not in entry["evidence_pages"]:
            entry["evidence_pages"].append(pnum)

    for page in pages or []:
        pnum = page_number(page)
        text = str(page.get("text") or "")
        if pnum is None or not text.strip():
            continue
        for field, label, pattern in _SURFACE_FIELD_RES:
            for m in pattern.finditer(text):
                record(field, label, m.group(1), pnum, numeric=True)
        for field, label, pattern in _CADASTRAL_ID_RES:
            for m in pattern.finditer(text):
                record(field, label, m.group(1), pnum, numeric=False)

    facts: List[Dict[str, Any]] = []
    for field in ("superficie_commerciale", "superficie_catastale", "vani",
                  "rendita_catastale", "foglio", "particella", "subalterno",
                  "categoria", "classe"):
        bucket = values.get(field) or {}
        multiple = len(bucket) > 1
        for entry in bucket.values():
            fact = dict(entry)
            fact["multiple_values"] = multiple
            fact["evidence_pages"] = sorted(fact["evidence_pages"])
            facts.append(fact)
    return facts
