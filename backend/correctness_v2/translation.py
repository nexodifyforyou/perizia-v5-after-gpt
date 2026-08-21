"""
Report-clarity bilingual translation layer (Italian-first + English).

Design contract (docs/report_clarity_plan.md §12):

  * Italian is AUTHORITATIVE and visually primary. English is a smaller/muted
    translation shown underneath — a PROGRESSIVE ENHANCEMENT fetched lazily
    AFTER the Italian report renders. It NEVER feeds back into
    severity/status/readiness/disclosure_state/canonical_verdict. No function in
    this module is ever read by any of those computations.
  * Three-way split (§12.D): STATIC (deterministic reviewed dictionary, no
    Gemini, globally cacheable), PASSTHROUGH (never translated — amounts, dates,
    cadastral/RGE refs, page numbers, addresses), DYNAMIC (Gemini, cached,
    fail-soft — free-text spans only).
  * Gemini is REUSED via ``narrator._call_gemini_narrator_llm`` with a bilingual
    ``system_instruction`` — no second client. QUOTA-EXEMPT: translation never
    decrements credits/beta/quota. Failure → Italian fallback, never affects
    entitlement/report availability/readiness/severity/disclosure.
  * Cache is OWNER-SCOPED: stored in the job directory alongside
    customer_report.json, keyed by (source_text_hash, glossary_prompt_version),
    reachable only through the owner-gated customer-view endpoint. A dynamic
    translation is NEVER reused cross-user by hash alone (the job directory is
    the ownership boundary).

This module is otherwise PURE and deterministic apart from the injected async
``translator`` callable (which the API wires to Gemini; tests inject a mock).
"""

from __future__ import annotations

import hashlib
import os
import re
import unicodedata
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Tuple

# Versioning: bump either when the glossary OR the prompt changes so stale
# cached translations are transparently invalidated.
GLOSSARY_VERSION = "cv2.clarity.glossary.v1"
PROMPT_VERSION = "cv2.clarity.prompt.v1"
GLOSSARY_PROMPT_VERSION = f"{GLOSSARY_VERSION}+{PROMPT_VERSION}"

# Target language marker returned to the client.
TARGET_LANGUAGE = "en"


# ---------------------------------------------------------------------------
# STATIC reviewed bilingual dictionary (NO Gemini, globally cacheable)
# ---------------------------------------------------------------------------
# Section titles + IA headers + six-state labels + status labels + fixed esito
# wording + professional glossary. Keys are the EXACT Italian strings emitted by
# decision_model.py / partial_report.py / customer_view.py string tables and the
# frontend section headers. Reviewed, never machine-generated.
STATIC_GLOSSARY: Dict[str, str] = {
    # --- IA section headers (frontend) -------------------------------------
    "Cosa stai acquistando": "What you are buying",
    "Nessun blocco automatico rilevato, ma sono presenti informazioni in conflitto da verificare.":
        "No automatic blocking issue was detected, but conflicting information requires verification.",
    "Numeri principali": "Key figures",
    "Valutazione sintetica": "Summary assessment",
    "Stato di occupazione": "Occupancy status",
    "Cosa verificare prima di procedere": "What to verify before proceeding",
    "Verifiche essenziali prima di procedere": "Essential checks before proceeding",
    "Altre verifiche": "Other checks",
    "Conformità e documenti tecnici": "Compliance and technical documents",
    "Formalità e cancellazioni": "Encumbrances and cancellations",
    "Altri elementi da conoscere": "Other things to know",
    "Fonti decisive dalla perizia": "Key evidence from the appraisal",
    "Conferme fornite dall'utente": "Confirmations you provided",
    "Stato delle verifiche": "Verification status",
    "Incertezze e conflitti": "Uncertainties and conflicts",
    "Report parziale": "Partial report",
    # --- Six state labels (§3.2) -------------------------------------------
    "Dichiarato dalla perizia": "Declared by the appraisal",
    "Confermato dall'utente": "Confirmed by you",
    "Da verificare": "To be verified",
    "Da chiarire": "To be clarified",
    "Non dichiarato": "Not declared",
    "In conflitto tra le fonti": "Conflicting between sources",
    "Non determinabile dalla sola perizia": "Not determinable from the appraisal alone",
    # --- Conformity/status labels ------------------------------------------
    "Conforme secondo la perizia": "Compliant according to the appraisal",
    "Regolarizzabile secondo la perizia": "Regularizable according to the appraisal",
    "Non conforme secondo la perizia": "Non-compliant according to the appraisal",
    "Completato": "Completed",
    "Conferma necessaria": "Confirmation required",
    "Verifica tecnica richiesta": "Technical review required",
    "Da rivedere": "To review",
    "Non sono sicuro": "I am not sure",
    # --- Readiness labels ---------------------------------------------------
    "Conferme necessarie": "Confirmations required",
    "Verifiche professionali aperte": "Open professional checks",
    "Pronto per l'esportazione": "Ready for export",
    # --- Esito wording (fixed tables) --------------------------------------
    "Nessuna verifica bloccante emersa dalla perizia":
        "No blocking issues emerged from the appraisal",
    "Verifiche necessarie prima di procedere": "Checks required before proceeding",
    "Verifica tecnica richiesta": "Technical review required",
    "Nessun elemento bloccante": "No blocking issues",
    "Verifiche necessarie": "Checks required",
    # --- Field labels (frontend rows) --------------------------------------
    "Tribunale": "Court",
    "Procedura/RGE": "Procedure/RGE",
    "Lotto": "Lot",
    "Lotto selezionato": "Selected lot",
    "Indirizzo": "Address",
    "Tipologia": "Property type",
    "Diritto/quota": "Right/share",
    "Occupazione": "Occupancy",
    "Stato": "Status",
    "Perché conta": "Why it matters",
    "Cosa verificare": "What to verify",
    "Costo": "Cost",
    "Tempistica": "Timing",
    "Totale": "Total",
    "Importo iscritto": "Registered amount",
    "Importi da chiarire": "Amounts to clarify",
    "Costi potenzialmente a carico dell'acquirente":
        "Costs potentially borne by the buyer",
    "Scenari alternativi indicati dalla perizia":
        "Alternative scenarios indicated by the appraisal",
    "Prezzo base d'asta": "Auction base price",
    "Già cancellata": "Already cancelled",
    "Da cancellare a cura della procedura":
        "To be cancelled by the procedure",
    # --- Professional glossary (surfaced within card labels) ----------------
    "perizia": "appraisal",
    "procedura esecutiva": "enforcement procedure",
    "decreto di trasferimento": "transfer decree",
    "formalità": "encumbrances/formalities",
    "pignoramento": "seizure (pignoramento)",
    "ipoteca": "mortgage (ipoteca)",
    "agibilità": "habitability certificate (agibilità)",
    "conformità catastale": "cadastral compliance",
    "conformità edilizia": "building compliance",
    "sanabile": "regularizable (sanabile)",
    "diritto di usufrutto": "usufruct right",
    "opponibilità": "enforceability against third parties (opponibilità)",
    "offerta minima": "minimum bid",
    # --- Enum-sourced fixed labels (area groups, conflict/incerto topics &
    #     reasons). These are deterministic Italian from decision_model's fixed
    #     tables; they resolve here (never Gemini) so English wording stays
    #     consistent across every report and no paid call is spent on them.
    "Edilizia": "Building regulations",
    "Catastale": "Cadastral",
    "Urbanistica": "Urban planning",
    "Vincoli di edilizia convenzionata o pubblica": "Subsidised or public-housing restrictions",
    "Corrispondenza catastale/atto": "Cadastral/deed correspondence",
    "Impianti — gas": "Systems — gas",
    "Impianti — elettrico": "Systems — electrical",
    "Impianti": "Systems",
    "Agibilità/abitabilità": "Habitability/fitness for use",
    "APE/energia": "EPC/energy",
    "Elemento segnalato dalla perizia": "Element flagged by the appraisal",
    "Tipologia dell'immobile": "Property type",
    "Valore finale della perizia": "Final appraisal value",
    "Dato economico": "Financial datum",
    "Conformità tecnica": "Technical compliance",
    "Formalità": "Encumbrances/formalities",
    "Elemento della perizia": "Element of the appraisal",
    "Le fonti della perizia riportano valori discordanti: è necessaria una verifica.":
        "The appraisal's sources report conflicting values: verification is required.",
    "Possibile sovrapposizione di dati tra lotti diversi: da verificare a quale lotto si riferisce il dato.":
        "Possible overlap of data between different lots: verify which lot the datum refers to.",
    "Elemento discordante tra le fonti della perizia: da verificare.":
        "Conflicting element between the appraisal's sources: to be verified.",
    "La perizia non consente di determinare con certezza lo stato di occupazione dell'immobile.":
        "The appraisal does not allow the property's occupancy status to be determined with certainty.",
    "La tipologia dell'immobile non è determinabile con certezza dalla sola perizia.":
        "The property type is not determinable with certainty from the appraisal alone.",
    "Il valore finale non è determinabile con certezza dalla sola perizia.":
        "The final value is not determinable with certainty from the appraisal alone.",
    "Estratto decisivo non disponibile": "Key excerpt not available",
}

# Normalized-key lookup so casing/whitespace differences still hit the glossary.


def _normalize(text: Any) -> str:
    stripped = "".join(
        c
        for c in unicodedata.normalize("NFKD", str(text or ""))
        if not unicodedata.combining(c)
    )
    return re.sub(r"\s+", " ", stripped).strip().lower()


_STATIC_BY_NORM: Dict[str, str] = {_normalize(k): v for k, v in STATIC_GLOSSARY.items()}


def static_translate(text: Any) -> Optional[str]:
    """Deterministic reviewed translation, or ``None`` when not a known term."""
    key = _normalize(text)
    if not key:
        return None
    return _STATIC_BY_NORM.get(key)


def source_text_hash(text: Any) -> str:
    """Stable hash of a source span (cache key component)."""
    return hashlib.sha256(_normalize(text).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# DYNAMIC bucket collection (free-text spans only — §12.D)
# ---------------------------------------------------------------------------
# Only these free-text fields are eligible for Gemini translation. Passthrough
# fields (amount_display, page numbers, cadastral/RGE refs, identity labels) are
# intentionally NOT collected here and are shown identically in both languages.
def _add(strings: List[str], seen: set, value: Any) -> None:
    text = str(value or "").strip()
    if not text:
        return
    if len(text) < 3:  # single tokens / punctuation are passthrough
        return
    if static_translate(text) is not None:
        return  # handled deterministically, never sent to Gemini
    key = _normalize(text)
    if key in seen:
        return
    seen.add(key)
    strings.append(text)


def collect_dynamic_strings(
    decision_model: Optional[Dict[str, Any]],
    partial_status: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """Return the de-duplicated list of DYNAMIC free-text spans to translate.

    Reads a built ``decision_model`` dict (+ optional ``partial_status``). Never
    collects amounts/dates/refs/labels (those are passthrough).
    """
    strings: List[str] = []
    seen: set = set()
    model = decision_model if isinstance(decision_model, dict) else {}
    sections = model.get("sections") or {}

    occ = sections.get("occupazione") or {}
    _add(strings, seen, occ.get("stato"))
    _add(strings, seen, occ.get("dettaglio"))
    _add(strings, seen, occ.get("perche_conta"))
    for c in occ.get("cosa_verificare") or []:
        _add(strings, seen, c)

    numeri = sections.get("numeri") or {}
    _add(strings, seen, (numeri.get("composizione_valore") or {}).get("title"))
    for r in numeri.get("da_chiarire") or []:
        _add(strings, seen, (r or {}).get("motivo"))
    for c in numeri.get("costi_potenziali") or []:
        _add(strings, seen, (c or {}).get("nota"))
    for s in numeri.get("scenari") or []:
        _add(strings, seen, (s or {}).get("label"))
    _add(strings, seen, (numeri.get("auction") or {}).get("nota"))
    recon = numeri.get("riconciliazione") or {}
    _add(strings, seen, recon.get("explanation"))

    verifiche = sections.get("verifiche") or {}
    for it in verifiche.get("items") or []:
        _add(strings, seen, (it or {}).get("title"))
        _add(strings, seen, (it or {}).get("why"))

    altri = sections.get("altri") or {}
    for it in altri.get("items") or []:
        _add(strings, seen, (it or {}).get("title"))
        _add(strings, seen, (it or {}).get("summary"))

    formalita = sections.get("formalita") or {}
    for bucket in ("cancellate", "costi_cancellazione", "da_verificare"):
        for card in formalita.get(bucket) or []:
            card = card or {}
            _add(strings, seen, card.get("type_label"))
            _add(strings, seen, card.get("statement"))
            _add(strings, seen, card.get("note"))
            _add(strings, seen, card.get("details"))

    fonti = sections.get("fonti") or {}
    for s in fonti.get("primary") or []:
        _add(strings, seen, (s or {}).get("title"))
        _add(strings, seen, (s or {}).get("excerpt"))

    for f in model.get("findings") or []:
        f = f or {}
        _add(strings, seen, f.get("title"))
        _add(strings, seen, f.get("customer_summary"))
        _add(strings, seen, f.get("buyer_impact"))
        ev = f.get("evidence") or {}
        _add(strings, seen, ev.get("excerpt"))

    ps = partial_status if isinstance(partial_status, dict) else (model.get("partial_status") or {})
    _add(strings, seen, (ps or {}).get("message"))
    for uf in (ps or {}).get("unresolved_fields") or []:
        _add(strings, seen, (uf or {}).get("why_unresolved"))

    return strings


# ---------------------------------------------------------------------------
# Constraint-checked prompt + post-translation validation (§12.E)
# ---------------------------------------------------------------------------
TRANSLATION_SYSTEM_INSTRUCTION = (
    "You are a precise legal-technical translator for Italian judicial real-estate "
    "appraisals (perizie immobiliari). Translate the given Italian span into clear, "
    "professional English for a property buyer. HARD RULES:\n"
    "- Translate ONLY the text provided; output nothing else.\n"
    "- Leave every amount, percentage, date, lot/Bene number, RGE/procedure "
    "reference, cadastral identifier, law/article number and page reference EXACTLY "
    "as written (do not convert, reformat, or localise numbers).\n"
    "- Preserve negation, uncertainty and declaration status precisely. "
    "'Non dichiarato' means 'Not declared' (NOT 'non compliant'). "
    "'Da verificare' means 'To be verified' (NOT 'incorrect'). "
    "'Da cancellare a cura della procedura' means it will be cancelled by the "
    "procedure (NOT 'already cancelled'). 'Dichiarato' means 'Declared' (NOT "
    "'independently verified').\n"
    "- Do not add facts, warnings, caveats, or commentary. Do not summarise.\n"
    "- Keep Italian legal terms in parentheses where helpful, but do not invent.\n"
    "Return ONLY the English translation as plain text."
)


def build_translation_prompt(source_it: str) -> str:
    """Isolate EXACTLY the free-text span (never a whole JSON node)."""
    return (
        "Translate the following Italian span to English, following every rule. "
        "Output only the English translation.\n\n"
        "ITALIAN:\n"
        f"{source_it}"
    )


# Any run of digits (ignoring thousands/decimal separators) — captures amounts,
# dates, RGE/procedure numbers, cadastral sheet/parcel, article/law numbers, page
# refs. Preservation of these is the core safety invariant.
_DIGIT_RUN_RE = re.compile(r"\d+")


def _number_multiset(text: Any) -> List[str]:
    return sorted(_DIGIT_RUN_RE.findall(str(text or "")))


_EN_STOPWORDS = {
    "the", "of", "and", "to", "is", "are", "in", "for", "with", "this", "be",
    "has", "have", "not", "that", "a", "an", "or", "on", "by", "as", "it", "will",
}


def validate_translation(source_it: str, candidate_en: Any) -> List[str]:
    """Return a list of rejection reasons; empty list means the translation is safe.

    Rejects: empty output, numbers/dates/refs changed (added or dropped), and
    output that is not plausibly English (still Italian / untranslated). On any
    rejection the caller falls back to the Italian source.
    """
    errors: List[str] = []
    candidate = str(candidate_en or "").strip()
    if not candidate:
        return ["empty"]
    # Numbers / dates / refs must be preserved exactly (multiset equality).
    if _number_multiset(source_it) != _number_multiset(candidate):
        errors.append("numbers_changed")
    # Wrong-language guard: a non-trivial output must read as English.
    words = re.findall(r"[a-zA-Z']+", candidate.lower())
    if len(words) >= 5 and not any(w in _EN_STOPWORDS for w in words):
        errors.append("wrong_language")
    # Untranslated passthrough: identical to the Italian source is not a
    # translation (fall back rather than show Italian labelled as English).
    if _normalize(candidate) == _normalize(source_it) and len(words) >= 3:
        errors.append("untranslated")
    # Negation preservation (backstops attacks #14/#16): if the Italian source
    # carries a standalone negation ("non"/"senza") the English candidate must
    # carry one too. This is a mechanical fail-soft trigger (falls back to Italian
    # on a miss), not a full tense/scope guarantee — that stays LLM-dependent.
    if re.search(r"(?<![a-z])(non|senza)(?![a-z])", _normalize(source_it)):
        low = candidate.lower()
        # Standalone negation words OR any contraction (isn't/doesn't/wasn't/
        # can't/won't/…). Contractions always have a letter before "n't", so they
        # must be matched WITHOUT the leading non-letter boundary.
        has_en_negation = bool(
            re.search(r"(?<![a-z])(not|no|without|never|nor|cannot|none|neither)(?![a-z])", low)
            or re.search(r"n['’]t\b", low)
        )
        if not has_en_negation:
            errors.append("negation_dropped")
    return errors


# ---------------------------------------------------------------------------
# Orchestration: static → cache → Gemini (fail-soft)
# ---------------------------------------------------------------------------
# The translator callable is injected so tests MOCK Gemini (no real paid calls).
Translator = Callable[[str, str], Awaitable[str]]


def _cache_entries(cache: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Return the usable cache entries, honouring the version fence."""
    if not isinstance(cache, dict):
        return {}
    if str(cache.get("glossary_prompt_version") or "") != GLOSSARY_PROMPT_VERSION:
        return {}  # stale cache for a different glossary/prompt version
    entries = cache.get("entries")
    return entries if isinstance(entries, dict) else {}


def new_cache() -> Dict[str, Any]:
    return {"glossary_prompt_version": GLOSSARY_PROMPT_VERSION, "entries": {}}


async def translate_texts(
    sources: Sequence[str],
    *,
    translator: Optional[Translator],
    cache: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, str]], Dict[str, Any], Dict[str, int]]:
    """Translate ``sources`` (IT→EN) via static → cache → Gemini, fail-soft.

    Returns ``(translations, updated_cache, stats)`` where ``translations`` is a
    list of ``{"source": it, "en": en}`` for spans that resolved to a trustworthy
    English string. A span that fails validation / has no translator / errors is
    simply OMITTED (Italian stays authoritative). ``stats`` counts resolution
    paths (``static``/``cache``/``gemini``/``failed``) for diagnostics — it never
    influences report availability.
    """
    entries = dict(_cache_entries(cache))
    translations: List[Dict[str, str]] = []
    stats = {"static": 0, "cache": 0, "gemini": 0, "failed": 0}
    emitted: set = set()

    for raw in sources:
        source = str(raw or "").strip()
        if not source:
            continue
        norm = _normalize(source)
        if norm in emitted:
            continue

        static_en = static_translate(source)
        if static_en:
            translations.append({"source": source, "en": static_en})
            emitted.add(norm)
            stats["static"] += 1
            continue

        h = source_text_hash(source)
        cached = entries.get(h)
        if isinstance(cached, dict) and cached.get("en"):
            translations.append({"source": source, "en": str(cached["en"])})
            emitted.add(norm)
            stats["cache"] += 1
            continue

        if translator is None:
            stats["failed"] += 1
            continue

        try:
            candidate = await translator(
                build_translation_prompt(source), TRANSLATION_SYSTEM_INSTRUCTION
            )
        except Exception:
            # Gemini timeout/HTTP/empty → Italian fallback. NEVER raises up.
            stats["failed"] += 1
            continue

        if validate_translation(source, candidate):
            stats["failed"] += 1
            continue

        english = str(candidate).strip()
        entries[h] = {"source": source, "en": english}
        translations.append({"source": source, "en": english})
        emitted.add(norm)
        stats["gemini"] += 1

    updated_cache = {"glossary_prompt_version": GLOSSARY_PROMPT_VERSION, "entries": entries}
    return translations, updated_cache, stats


async def default_gemini_translator(prompt: str, system_instruction: str) -> str:
    """Default translator: REUSES the shared Gemini client (no second provider).

    Reads the already-wired Gemini env (GEMINI_API_KEY/GOOGLE_API_KEY,
    GEMINI_DECISION_MODEL, translation timeout). Raises on any failure so
    ``translate_texts`` treats it as a fail-soft Italian fallback. Imported
    lazily to avoid pulling the large server/narrator module at import time.
    """
    from narrator import _call_gemini_narrator_llm  # lazy shared-client reuse

    model = str(os.environ.get("GEMINI_DECISION_MODEL") or "gemini-2.5-flash").strip()
    api_key = str(
        os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY") or ""
    ).strip()
    if not api_key:
        raise RuntimeError("gemini_api_key_missing")
    try:
        timeout = float(os.environ.get("CORRECTNESS_V2_TRANSLATION_TIMEOUT_SECONDS", "30") or 30)
    except Exception:
        timeout = 30.0
    return await _call_gemini_narrator_llm(
        api_key=api_key,
        model=model,
        prompt=prompt,
        timeout_seconds=timeout,
        system_instruction=system_instruction,
    )
