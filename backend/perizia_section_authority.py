"""Document section authority map for Perizia/CTU PDFs.

This module is intentionally shadow-only. It classifies source pages and
evidence quotes so later resolvers can distinguish CTU factual answers from
court questions, procedural context, final valuation, lot formation and
formalities sections.
"""

from __future__ import annotations

import re
import unicodedata
from collections import Counter
from typing import Any, Dict, Iterable, List, Optional, Tuple


SCHEMA_VERSION = "perizia_section_authority_v1"

ZONE_TOC = "TOC"
ZONE_INSTRUCTION = "INSTRUCTION_BLOCK"
ZONE_QUESTION = "QUESTION_BLOCK"
ZONE_ANSWER = "ANSWER_BLOCK"
ZONE_FINAL_LOT = "FINAL_LOT_FORMATION"
ZONE_FINAL_VALUATION = "FINAL_VALUATION"
ZONE_FORMALITIES = "FORMALITIES_TABLE"
ZONE_LOT_BODY = "LOT_BODY"
ZONE_BENE_BODY = "BENE_BODY"
ZONE_CONTEXT = "ANNEX_OR_CONTEXT"
ZONE_UNKNOWN = "UNKNOWN_FACTUAL"

AUTH_HIGH = "HIGH_FACTUAL"
AUTH_MEDIUM = "MEDIUM_FACTUAL"
AUTH_LOW = "LOW_CONTEXT_ONLY"
AUTH_UNKNOWN = "UNKNOWN"


INSTRUCTION_HEADING_PATTERNS = [
    r"\bindicazioni\s+peritali\b",
    r"\bquesit[oi]\b",
    r"\bmandato\b",
    r"\bincarico\s+(conferito|affidato|al\s+ctu|all'?esperto|peritale)\b",
    r"\bil\s+giudice\s+(dispone|chiede|ordina|invita)\b",
    r"\bl'?esperto\s+dovra\b",
]

TASK_VERB_PATTERNS = [
    r"\b(verifichi|accerti|indichi|descriva|determini|provveda|specifichi)\b",
    r"\bdica\s+se\b",
    r"\bproceda\b",
]

INSTRUCTION_PATTERNS = INSTRUCTION_HEADING_PATTERNS + TASK_VERB_PATTERNS

ANSWER_PATTERNS = [
    r"\brisposte\s+alle\s+indicazioni\s+peritali\b",
    r"\brisposte\s+ai\s+quesiti\b",
    r"\brisposta\s+al\s+punto\s+n(?:\.|r\.?|°)?\s*\d+\b",
    r"\brisposta\s+al\s+punto\s+\d+\b",
    r"\brisposta\s+al\s+quesito\s+n(?:\.|r\.?|°)?\s*\d+\b",
    r"\brisposta\s+al\s+quesito\s+\d+\b",
    r"\brisposta\s+n(?:\.|r\.?|°)?\s*\d+\b",
    r"\baccertamenti\s+e\s+risposte\b",
    r"\bsvolgimento\s+delle\s+operazioni\s+peritali\b",
    r"\bsviluppo\s+delle\s+operazioni\s+peritali\b",
    r"\boperazioni\s+peritali\b",
    r"\bconclusioni\b",
]

FINAL_LOT_PATTERNS = [
    r"\bformazione\s+lott[oi]\b",
    r"\blotto\s+unico\b",
    r"\bunico\s+lotto\b",
    r"\bvendibil[ei]\s+in\s+un\s+unico\s+lotto\b",
    r"\bschema\s+riassuntivo\b",
    r"\briepilogo\s+(dei\s+)?lott[oi]\b",
    r"\bprezzo\s+base\s+d[' ]asta\s+per\s+lotto\b",
    r"\bvalore\s+finale\s+lotto\b",
    r"\bidentificativo\s+lotto\b",
]

FINAL_VALUATION_PATTERNS = [
    r"\bvalore\s+finale\s+di\s+stima\b",
    r"\bvalore\s+finale\b",
    r"\bprezzo\s+base\s+d[' ]asta\b",
    r"\bbase\s+d[' ]asta\b",
    r"\bprezzo\s+base\b",
    r"\bvalore\s+di\s+stima\b",
    r"\bstima\s+(finale|del\s+bene|dell'?immobile)\b",
    r"\badeguamenti\s+e\s+correzioni\s+della\s+stima\b",
    r"\bdeprezzament[oi]\b",
    r"\bsuperficie\s+commerciale\b",
    r"\bvalore\s+unitario\b",
    r"\bvalore\s+complessivo\b",
    r"\briepilogo\s+stima\b",
    r"\briepilogo\s+valutazione\b",
    r"\bdeterminazione\s+del\s+valore\b",
    r"\bvalore\s+di\s+mercato\b",
    r"\bvalore\s+cauzionale\b",
    r"\bvalore\s+venale\b",
    r"\bvalore\s+di\s+vendita\s+giudiziaria\b",
    r"\bvalore\s+vendita\s+giudiziaria\b",
    r"\bcalcolo\s+del\s+valore\s+di\s+mercato\b",
    r"\bdecurtazioni\s+ed\s+adeguamenti\s+del\s+valore\b",
    r"\bvalutazione\s+complessiva\s+del\s+lotto\b",
]

FORMALITIES_PATTERNS = [
    r"\bformalita\s+pregiudizievol[ei]\b",
    r"\bformalita\s+gravanti\b",
    r"\bformalita\s+da\s+cancellare\b",
    r"\bcancellazione\s+formalita\b",
    r"\biscrizion[ei]\b",
    r"\btrascrizion[ei]\b",
    r"\bannotazion[ei]\b",
    r"\bipotec[ah]e?\b",
    r"\bipoteca\s+(volontaria|giudiziale|legale)\b",
    r"\bpignorament[oi]\b",
    r"\bdomand[ae]\s+giudizial[ei]\b",
    r"\bconservatoria\b",
    r"\bregistro\s+(particolare|generale)\b",
]

PROCEDURE_CONTEXT_PATTERNS = [
    r"\bprocedura\s+portante\b",
    r"\bprecedente\s+perizia\b",
    r"\bprecedente\s+lotto\b",
    r"\bvecchio\s+lotto\b",
    r"\baltra\s+procedura\b",
    r"\briunion[ei]\s+con\s+altra\s+procedura\b",
    r"\bseparazion[ei]\s+procedura\b",
    r"\brichiamo\s+(storico|procedural[ei])\b",
    r"\briferimento\s+storico\b",
    r"\brichiamo\b",
    r"\bgia\s+(indicato|identificato)\b",
    r"\bprocedura\s+riunita\b",
]

TOC_PATTERNS = [
    r"\bsommario\b",
    r"\bindice\b",
    r"\bpagina\s+\d+\b",
]

MONEY_COST_PATTERNS = [
    r"\bspes[ae]\b",
    r"\bcost[oi]\b",
    r"\boneri?\b",
    r"\bsanzion[ei]\b",
    r"\bcila\b",
    r"\bdocfa\b",
    r"\btipo\s+mappale\b",
    r"\bregolarizzazion[ei]\b",
    r"\bsanatoria\b",
    r"\bspese\s+tecniche\b",
]

MONEY_RENDITA_PATTERNS = [
    r"\brendita\s+catastale\b",
    r"\brendita\b.{0,80}\b(catasto|catastale|categoria|classe|vani|foglio|particella|subalterno)\b",
    r"\b(catasto|catastale|categoria|classe|vani|foglio|particella|subalterno)\b.{0,80}\brendita\b",
]

MONEY_PRICE_PATTERNS = [
    r"\bprezzo\s+base\b",
    r"\bbase\s+d[' ]asta\b",
    r"\bofferta\s+minima\b",
    r"\bvalore\s+di\s+vendita\s+giudiziaria\b",
    r"\bvalore\s+vendita\s+giudiziaria\b",
]

MONEY_VALUATION_PATTERNS = FINAL_VALUATION_PATTERNS + [
    r"\bvalore\s+intero\b",
    r"\bvalore\s+diritto\b",
    r"\bvalore\s+mercato\b",
    r"\bdecurtazion[ei]\b",
    r"\babbattimento\b",
]

MONEY_FORMALITIES_PATTERNS = [
    r"\bspese\s+di\s+cancellazione\b",
    r"\bcancellazione\s+(delle\s+)?(trascrizioni|iscrizioni|formalita)\b",
    r"\bformalita\b.{0,120}(euro|€|importo|spese|costo)\b",
    r"\b(iscrizioni|trascrizioni|pignoramento|ipoteca)\b.{0,120}(euro|€|importo|spese|costo)\b",
]

DOMAIN_PATTERNS: List[Tuple[str, List[str]]] = [
    ("lots", [r"\blotto\b", r"\blotti\b", r"\bformazione\s+lott[oi]\b", r"\bschema\s+riassuntivo\b"]),
    ("beni", [r"\bbene\s+n", r"\bbene\s+numero\b", r"\bappartamento\b", r"\bimmobile\b", r"\bautorimessa\b", r"\bgarage\b"]),
    ("occupancy", [r"\bstato\s+di\s+possesso\b", r"\boccupazion[ei]\b", r"\boccupat[oaie]\b", r"\bliber[oaie]\b", r"\bdebitore\b", r"\bdebitori\b"]),
    ("opponibilita", [r"\bopponibil", r"\blocazion[ei]\b", r"\bcontratto\s+di\s+locazione\b", r"\bassegnazione\s+casa\s+coniugale\b"]),
    ("legal_formalities", FORMALITIES_PATTERNS),
    ("urbanistica", [r"\burbanistic", r"\bregolarita\s+urbanistica\b", r"\bcila\b", r"\bscia\b", r"\bsanatoria\b", r"\babusi?\b"]),
    ("agibilita", [r"\bagibilita\b", r"\babitabilita\b", r"\bcertificato\s+di\s+agibilita\b"]),
    ("catasto", [r"\bcatast", r"\bdocfa\b", r"\btipo\s+mappale\b", r"\bplanimetria\b", r"\bsubalterno\b", r"\bfoglio\b", r"\bparticella\b"]),
    ("money_cost_signal", MONEY_COST_PATTERNS),
    ("money_rendita_catastale", MONEY_RENDITA_PATTERNS),
    ("money_formalities", MONEY_FORMALITIES_PATTERNS),
    ("money_price", MONEY_PRICE_PATTERNS),
    ("money_valuation", MONEY_VALUATION_PATTERNS),
    ("money_unknown", [r"\beuro\b", r"\bimporto\b", r"EUR_SYMBOL"]),
    ("valuation", FINAL_VALUATION_PATTERNS + [r"\bdecurtazion[ei]\b", r"\babbattimento\b"]),
    ("deprezzamenti", [r"\bdeprezzament[oi]\b", r"\bdecurtazion[ei]\b", r"\bribasso\b"]),
    ("condominio", [r"\bcondomini", r"\bspese\s+condominial", r"\bamministrator[ei]\b"]),
    ("procedure_context", PROCEDURE_CONTEXT_PATTERNS + [r"\bprocedura\s+esecutiva\b", r"\br\.?\s*g\.?\s*e\.?\b"]),
]


def _normalize_text(text: Any) -> str:
    raw = str(text or "")
    raw = raw.replace("’", "'").replace("`", "'").replace("´", "'")
    decomposed = unicodedata.normalize("NFKD", raw)
    without_marks = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return without_marks.lower()


def _compile_match(patterns: Iterable[str], normalized_text: str) -> bool:
    for pattern in patterns:
        if pattern == r"EUR_SYMBOL":
            if "€" in normalized_text:
                return True
            continue
        if re.search(pattern, normalized_text, flags=re.IGNORECASE | re.UNICODE):
            return True
    return False


def _matching_patterns(patterns: Iterable[str], normalized_text: str) -> List[str]:
    hits: List[str] = []
    for pattern in patterns:
        if pattern == r"EUR_SYMBOL":
            if "€" in normalized_text:
                hits.append("€")
            continue
        if re.search(pattern, normalized_text, flags=re.IGNORECASE | re.UNICODE):
            hits.append(pattern)
    return hits


def _has_money_token(normalized_text: str) -> bool:
    return "€" in normalized_text or bool(re.search(r"\beuro\b|\bimporto\b|\b\d{1,3}(?:\.\d{3})*,\d{2}\b", normalized_text))


def _contains_factual_professional_cost_context(normalized_text: str) -> bool:
    return bool(
        re.search(
            r"\b(incarico|consulenz[ae]|prestazion[ei])\s+(a\s+)?(professionista|tecnico|professionisti|tecnici)\b",
            normalized_text,
        )
        or re.search(r"\bspese\s+tecniche\b|\bprofessionista\s+abilitato\b|\btecnico\s+abilitato\b", normalized_text)
    )


def _is_instruction_like(normalized_text: str, *, high_factual_context: bool = False) -> bool:
    if high_factual_context and _contains_factual_professional_cost_context(normalized_text):
        return False
    if _compile_match(INSTRUCTION_HEADING_PATTERNS, normalized_text):
        return not _contains_factual_professional_cost_context(normalized_text)
    if re.search(r"\bquesit[oi]\b.{0,160}\b(verifichi|accerti|indichi|descriva|determini|provveda|specifichi|dica\s+se|proceda)\b", normalized_text):
        return True
    if re.search(r"\b(verifichi|accerti|indichi|descriva|determini|provveda|specifichi|dica\s+se|proceda)\b", normalized_text[:700]):
        if _contains_factual_professional_cost_context(normalized_text):
            return False
        return bool(
            re.search(r"\b(se|che|la|il|lo|gli|le|l'|l’immobile|l'immobile|bene|diritto|stato|regolarita)\b", normalized_text[:760])
        )
    return False


def detect_money_role_hints(text: str) -> List[str]:
    normalized = _normalize_text(text)
    if not _has_money_token(normalized) and not any(
        _compile_match(patterns, normalized)
        for patterns in (MONEY_COST_PATTERNS, MONEY_RENDITA_PATTERNS, MONEY_PRICE_PATTERNS, MONEY_VALUATION_PATTERNS, MONEY_FORMALITIES_PATTERNS)
    ):
        return []

    roles: List[str] = []
    role_patterns = [
        ("money_rendita_catastale", MONEY_RENDITA_PATTERNS),
        ("money_formalities", MONEY_FORMALITIES_PATTERNS),
        ("money_price", MONEY_PRICE_PATTERNS),
        ("money_valuation", MONEY_VALUATION_PATTERNS),
        ("money_cost_signal", MONEY_COST_PATTERNS),
    ]
    for role, patterns in role_patterns:
        if _compile_match(patterns, normalized):
            roles.append(role)
    if not roles and _has_money_token(normalized):
        roles.append("money_unknown")
    return roles


def _is_formalities_section(normalized_text: str) -> bool:
    if re.search(r"\bformalita\s+(pregiudizievol[ei]|gravanti|da\s+cancellare)\b", normalized_text):
        return True
    if re.search(r"\b(conservatoria|registro\s+(particolare|generale))\b", normalized_text):
        return True
    if re.search(r"\bcancellazione\s+(formalita|delle\s+trascrizioni|delle\s+iscrizioni)\b", normalized_text):
        return True
    registration_hits = sum(
        1
        for pattern in (
            r"\biscrizion[ei]\b",
            r"\btrascrizion[ei]\b",
            r"\bannotazion[ei]\b",
            r"\bipoteca\s+(volontaria|giudiziale|legale)\b",
            r"\bpignorament[oi]\b",
            r"\bdomand[ae]\s+giudizial[ei]\b",
        )
        if re.search(pattern, normalized_text)
    )
    return registration_hits >= 2


def _is_final_valuation_section(normalized_text: str) -> bool:
    strong = [
        r"\bvalore\s+finale\s+di\s+stima\b",
        r"\bvalore\s+di\s+vendita\s+giudiziaria\b",
        r"\bvalore\s+vendita\s+giudiziaria\b",
        r"\bprezzo\s+base\s+d[' ]asta\b",
        r"\badeguamenti\s+e\s+correzioni\s+della\s+stima\b",
        r"\bcalcolo\s+del\s+valore\s+di\s+mercato\b",
        r"\bdecurtazioni\s+ed\s+adeguamenti\s+del\s+valore\b",
        r"\bvalutazione\s+complessiva\s+del\s+lotto\b",
    ]
    if _compile_match(strong, normalized_text):
        return True
    hits = 0
    for pattern in (
        r"\bvalore\s+di\s+mercato\b",
        r"\bvalore\s+di\s+stima\b",
        r"\bvalore\s+finale\b",
        r"\bvalore\s+unitario\b",
        r"\bvalore\s+complessivo\b",
        r"\bsuperficie\s+commerciale\b",
        r"\bdeprezzament[oi]\b",
        r"\bdecurtazion[ei]\b",
        r"\briepilogo\s+(stima|valutazione)\b",
        r"\bvalore\s+(cauzionale|venale)\b",
        r"\bbase\s+d[' ]asta\b",
        r"\btotale\b",
    ):
        if re.search(pattern, normalized_text):
            hits += 1
    return hits >= 2 and _has_money_token(normalized_text)


def _is_final_lot_section(normalized_text: str, *, final_valuation: bool = False) -> bool:
    if _compile_match(FINAL_LOT_PATTERNS, normalized_text):
        return True
    if re.search(r"\b(lotto\s+n\.?\s*1|lotto\s+1)\b", normalized_text) and final_valuation:
        return True
    combo_hits = sum(
        1
        for pattern in (
            r"\blotto\b",
            r"\bcorpo\b",
            r"\bbene\b",
            r"\bschema\s+riassuntivo\b",
            r"\bidentificativo\s+lotto\b",
            r"\bprezzo\s+base\b",
            r"\bvalore\s+finale\b",
        )
        if re.search(pattern, normalized_text)
    )
    return combo_hits >= 3 and final_valuation


def _is_numbered_answer_heading(normalized_text: str) -> bool:
    return bool(
        re.search(
            r"(?:^|\n)\s*(\d{1,2})\s*[\.\-–]\s*(stato\s+di\s+possesso|identificazione|descrizione|valutazione|regolarita|conformita|formalita|vincoli|catasto|abitabilita|agibilita)\b",
            normalized_text,
        )
    )


def _page_number(page: Dict[str, Any], default: int) -> int:
    for key in ("page", "page_number", "page_num"):
        try:
            value = int(page.get(key))
            if value > 0:
                return value
        except Exception:
            continue
    return default


def _extract_headings(text: str, max_headings: int = 8) -> List[str]:
    headings: List[str] = []
    for raw_line in str(text or "").splitlines():
        line = " ".join(raw_line.strip().split())
        if not line or len(line) > 140:
            continue
        normalized = _normalize_text(line)
        if (
            _compile_match(INSTRUCTION_PATTERNS, normalized)
            or _compile_match(ANSWER_PATTERNS, normalized)
            or _compile_match(FINAL_LOT_PATTERNS, normalized)
            or _compile_match(FINAL_VALUATION_PATTERNS, normalized)
            or _compile_match(FORMALITIES_PATTERNS, normalized)
            or re.search(r"\blotto\s+(unico|n\.?\s*\d+|\d+)\b", normalized)
            or re.search(r"\bbene\s+n\.?\s*\d+\b", normalized)
        ):
            headings.append(line)
        elif line.upper() == line and len(line) >= 8 and re.search(r"[A-Z]", line):
            headings.append(line)
        if len(headings) >= max_headings:
            break
    return headings


def detect_answer_point(text: str) -> Optional[int]:
    """Return the explicit CTU answer point number, if present."""
    normalized = _normalize_text(text)
    patterns = [
        r"\brisposta\s+al\s+punto\s+n(?:\.|r\.?|°)?\s*(\d+)\b",
        r"\brisposta\s+al\s+punto\s+(\d+)\b",
        r"\brisposta\s+al\s+quesito\s+n(?:\.|r\.?|°)?\s*(\d+)\b",
        r"\brisposta\s+al\s+quesito\s+(\d+)\b",
        r"\brisposta\s+n(?:\.|r\.?|°)?\s*(\d+)\b",
        r"\bpunto\s+n(?:\.|r\.?|°)?\s*(\d+)\b",
        r"\bquesito\s+n(?:\.|r\.?|°)?\s*(\d+)\b",
        r"(?:^|\n)\s*(\d{1,2})\s*[\.\-–]\s*(stato\s+di\s+possesso|identificazione|descrizione|valutazione|regolarita|conformita|formalita|vincoli|catasto|abitabilita|agibilita)\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, normalized, flags=re.IGNORECASE | re.UNICODE)
        if match:
            try:
                return int(match.group(1))
            except Exception:
                return None
    return None


def detect_domain_hints(text: str) -> List[str]:
    """Detect broad extraction domains mentioned in text."""
    normalized = _normalize_text(text)
    hints: List[str] = []
    for domain, patterns in DOMAIN_PATTERNS:
        if _compile_match(patterns, normalized):
            hints.append(domain)
    specific_money_roles = [hint for hint in hints if hint.startswith("money_") and hint != "money_unknown"]
    if specific_money_roles and "money_unknown" in hints:
        hints = [hint for hint in hints if hint != "money_unknown"]
    for role in detect_money_role_hints(text):
        if role not in hints:
            hints.append(role)
    if "money_formalities" in hints and "money_cost_signal" in hints:
        hints = [hint for hint in hints if hint != "money_cost_signal"]
    return hints


def _is_toc_like(normalized: str) -> bool:
    if not _compile_match(TOC_PATTERNS, normalized):
        return False
    lines = [line.strip() for line in normalized.splitlines() if line.strip()]
    if not lines:
        return False
    dotted_or_page_refs = sum(1 for line in lines if re.search(r"\.{3,}\s*\d+\s*$|\s+\d+\s*$", line))
    return dotted_or_page_refs >= 3 or ("indice" in normalized[:300] or "sommario" in normalized[:300])


def _classify_zone(
    normalized: str,
    *,
    context: Optional[Dict[str, Any]] = None,
) -> Tuple[str, str, float, str, bool, bool]:
    context = context or {}
    answer_continuation = int(context.get("answer_continuation_pages", 0) or 0)
    seen_answer = bool(context.get("seen_answer")) or answer_continuation > 0
    explicit_answer = _compile_match(ANSWER_PATTERNS, normalized) or _is_numbered_answer_heading(normalized)
    final_valuation = _is_final_valuation_section(normalized)
    final_lot = _is_final_lot_section(normalized, final_valuation=final_valuation)
    high_factual_candidate = explicit_answer or final_lot or final_valuation or _is_formalities_section(normalized)
    explicit_instruction = _is_instruction_like(normalized, high_factual_context=high_factual_candidate)
    formalities = _is_formalities_section(normalized)
    procedural_context = _compile_match(PROCEDURE_CONTEXT_PATTERNS, normalized)
    lot_body = bool(re.search(r"\blotto\s+(unico|n\.?\s*\d+|\d+)\b", normalized))
    bene_body = bool(re.search(r"\bbene\s+n\.?\s*\d+\b", normalized))
    factual_phrasing = bool(
        re.search(
            r"\b(risulta|si\s+rileva|si\s+e\s+accertato|il\s+bene\s+e|l'immobile\s+e|occupato\s+dai|libero\s+da)\b",
            normalized,
        )
    )

    if _is_toc_like(normalized) and not explicit_answer and not final_lot and not final_valuation:
        return ZONE_TOC, AUTH_LOW, 0.05, "table of contents or index-like page", explicit_instruction, False

    if procedural_context and lot_body and not final_lot and not explicit_answer:
        return ZONE_CONTEXT, AUTH_LOW, 0.18, "lot reference appears procedural or historical", explicit_instruction, False

    if final_lot and not (procedural_context and not explicit_answer):
        return ZONE_FINAL_LOT, AUTH_HIGH, 0.95, "final lot formation indicator", explicit_instruction, True

    if formalities and (explicit_answer or seen_answer or not explicit_instruction):
        return ZONE_FORMALITIES, AUTH_HIGH, 0.9, "formalities section indicators", explicit_instruction, explicit_answer or seen_answer

    if final_valuation and not (procedural_context and not explicit_answer):
        return ZONE_FINAL_VALUATION, AUTH_HIGH, 0.9, "final valuation or auction price indicator", explicit_instruction, explicit_answer or seen_answer

    if explicit_answer:
        return ZONE_ANSWER, AUTH_HIGH, 0.88, "explicit CTU answer heading", explicit_instruction, True

    if explicit_instruction and not seen_answer:
        zone = ZONE_QUESTION if _compile_match([r"\bquesit[oi]\b"], normalized) else ZONE_INSTRUCTION
        return zone, AUTH_LOW, 0.15, "court/question/task wording before answer section", True, False

    if procedural_context:
        return ZONE_CONTEXT, AUTH_LOW, 0.22, "procedural or historical context indicators", explicit_instruction, explicit_answer

    if (answer_continuation > 0 or bool(context.get("seen_answer"))) and factual_phrasing:
        return ZONE_ANSWER, AUTH_HIGH, 0.82, "factual wording after answer section began", explicit_instruction, True

    if factual_phrasing:
        return ZONE_UNKNOWN, AUTH_MEDIUM, 0.55, "factual wording without explicit section heading", explicit_instruction, False

    if lot_body:
        return ZONE_LOT_BODY, AUTH_MEDIUM, 0.5, "lot heading/body indicator without final formation cue", explicit_instruction, explicit_answer or seen_answer

    if bene_body:
        return ZONE_BENE_BODY, AUTH_MEDIUM, 0.48, "bene heading/body indicator", explicit_instruction, explicit_answer or seen_answer

    return ZONE_UNKNOWN, AUTH_UNKNOWN, 0.3, "no strong authority indicators", explicit_instruction, explicit_answer or seen_answer


def classify_page_authority(
    page_number: int,
    text: str,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Classify one page into a source-authority zone."""
    normalized = _normalize_text(text)
    zone, level, score, reason, is_instruction_like, is_answer_like = _classify_zone(normalized, context=context)
    hints = detect_domain_hints(text)
    answer_point = detect_answer_point(text)

    if zone == ZONE_CONTEXT and "procedure_context" not in hints:
        hints.append("procedure_context")
    if re.search(r"\blotto\s+(unico|n\.?\s*\d+|\d+)\b", normalized) and "lots" not in hints:
        hints.append("lots")

    return {
        "page": int(page_number),
        "zone": zone,
        "authority_level": level,
        "authority_score": round(float(score), 4),
        "headings": _extract_headings(text),
        "answer_point": answer_point,
        "domain_hints": hints,
        "is_instruction_like": bool(is_instruction_like),
        "is_answer_like": bool(is_answer_like),
        "reason": reason,
    }


def summarize_authority_map(section_map: Dict[str, Any]) -> Dict[str, Any]:
    pages = section_map.get("pages") if isinstance(section_map, dict) else []
    if not isinstance(pages, list):
        pages = []
    zone_counts = Counter(str(p.get("zone") or ZONE_UNKNOWN) for p in pages if isinstance(p, dict))
    hint_counts: Counter[str] = Counter()
    for page in pages:
        if not isinstance(page, dict):
            continue
        for hint in page.get("domain_hints") or []:
            hint_counts[str(hint)] += 1
    return {
        "pages_total": len(pages),
        "instruction_pages": [int(p["page"]) for p in pages if isinstance(p, dict) and p.get("zone") in {ZONE_INSTRUCTION, ZONE_QUESTION}],
        "answer_pages": [int(p["page"]) for p in pages if isinstance(p, dict) and p.get("zone") == ZONE_ANSWER],
        "final_valuation_pages": [int(p["page"]) for p in pages if isinstance(p, dict) and p.get("zone") == ZONE_FINAL_VALUATION],
        "final_lot_formation_pages": [int(p["page"]) for p in pages if isinstance(p, dict) and p.get("zone") == ZONE_FINAL_LOT],
        "formalities_pages": [int(p["page"]) for p in pages if isinstance(p, dict) and p.get("zone") == ZONE_FORMALITIES],
        "unknown_pages": [int(p["page"]) for p in pages if isinstance(p, dict) and p.get("zone") == ZONE_UNKNOWN],
        "zone_counts": dict(sorted(zone_counts.items())),
        "domain_hint_counts": dict(sorted(hint_counts.items())),
    }


def _first_page_for_zone(pages: List[Dict[str, Any]], zones: Iterable[str]) -> Optional[int]:
    wanted = set(zones)
    for page in pages:
        if isinstance(page, dict) and page.get("zone") in wanted:
            try:
                return int(page.get("page"))
            except Exception:
                return None
    return None


def build_section_authority_map(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Build a document-wide authority map from extracted page text."""
    normalized_pages: List[Tuple[int, str]] = []
    for idx, page in enumerate(pages or [], start=1):
        if not isinstance(page, dict):
            continue
        normalized_pages.append((_page_number(page, idx), str(page.get("text") or "")))
    normalized_pages.sort(key=lambda item: item[0])

    classified_pages: List[Dict[str, Any]] = []
    answer_continuation_pages = 0
    for page_num, text in normalized_pages:
        context: Dict[str, Any] = {"answer_continuation_pages": answer_continuation_pages}
        page_info = classify_page_authority(page_num, text, context=context)
        classified_pages.append(page_info)
        if page_info.get("zone") == ZONE_ANSWER and page_info.get("reason") == "explicit CTU answer heading":
            answer_continuation_pages = 1
        else:
            answer_continuation_pages = max(0, answer_continuation_pages - 1)

    boundaries = {
        "first_instruction_page": _first_page_for_zone(classified_pages, [ZONE_INSTRUCTION, ZONE_QUESTION]),
        "first_answer_page": _first_page_for_zone(classified_pages, [ZONE_ANSWER]),
        "first_final_valuation_page": _first_page_for_zone(classified_pages, [ZONE_FINAL_VALUATION]),
        "first_lot_formation_page": _first_page_for_zone(classified_pages, [ZONE_FINAL_LOT]),
    }
    section_map = {
        "schema_version": SCHEMA_VERSION,
        "pages": classified_pages,
        "boundaries": boundaries,
        "summary": {},
    }
    section_map["summary"] = summarize_authority_map(section_map)
    return section_map


def _page_entry(section_map: Dict[str, Any], page_number: int) -> Optional[Dict[str, Any]]:
    pages = section_map.get("pages") if isinstance(section_map, dict) else []
    if not isinstance(pages, list):
        return None
    for page in pages:
        if not isinstance(page, dict):
            continue
        try:
            if int(page.get("page")) == int(page_number):
                return page
        except Exception:
            continue
    return None


def classify_quote_authority(
    page_number: int,
    quote: str,
    section_map: Dict[str, Any],
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """Return authority metadata for a specific evidence quote."""
    page = _page_entry(section_map, page_number)
    if page is None:
        page = classify_page_authority(page_number, quote, context=None)

    normalized_quote = _normalize_text(quote)
    high_factual_page = str(page.get("authority_level") or "") == AUTH_HIGH
    quote_instruction = _is_instruction_like(normalized_quote, high_factual_context=high_factual_page)
    quote_answer = _compile_match(ANSWER_PATTERNS, normalized_quote)
    quote_context = _compile_match(PROCEDURE_CONTEXT_PATTERNS, normalized_quote)
    quote_hints = detect_domain_hints(quote)

    section_zone = str(page.get("zone") or ZONE_UNKNOWN)
    level = str(page.get("authority_level") or AUTH_UNKNOWN)
    try:
        score = float(page.get("authority_score", 0.3) or 0.3)
    except Exception:
        score = 0.3
    reason = str(page.get("reason") or "page authority")

    if quote_context and section_zone not in {ZONE_FINAL_LOT, ZONE_FINAL_VALUATION, ZONE_ANSWER}:
        section_zone = ZONE_CONTEXT
        level = AUTH_LOW
        score = min(score, 0.18)
        reason = "quote appears procedural or historical"
    elif quote_instruction and not bool(page.get("is_answer_like")):
        section_zone = ZONE_INSTRUCTION
        level = AUTH_LOW
        score = min(score, 0.15)
        reason = "quote contains task wording before answer section"
    elif quote_answer and level in {AUTH_LOW, AUTH_UNKNOWN}:
        section_zone = ZONE_ANSWER
        level = AUTH_HIGH
        score = max(score, 0.85)
        reason = "quote contains explicit answer heading"

    domain_hint = domain or (quote_hints[0] if quote_hints else None)
    if domain_hint is None:
        page_hints = page.get("domain_hints") if isinstance(page.get("domain_hints"), list) else []
        domain_hint = str(page_hints[0]) if page_hints else None

    return {
        "page": int(page_number),
        "section_zone": section_zone,
        "authority_level": level,
        "authority_score": round(float(score), 4),
        "domain_hint": domain_hint,
        "domain_hints": quote_hints or list(page.get("domain_hints") or []),
        "is_instruction_like": bool(quote_instruction or page.get("is_instruction_like")),
        "is_answer_like": bool(quote_answer or page.get("is_answer_like")),
        "answer_point": detect_answer_point(quote) or page.get("answer_point"),
        "reason_for_authority": reason,
    }
