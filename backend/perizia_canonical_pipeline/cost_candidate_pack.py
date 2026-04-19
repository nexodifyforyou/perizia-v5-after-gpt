"""
Costi / Oneri bounded field shell — candidate harvesting only.

Produces: cost_candidate_pack.json

Quantified field types:
  cost_regolarizzazione_raw          — explicit regularization cost total / aggregate
  cost_sanatoria_raw                 — sanatoria-specific costs (oblazione, oneri costruzione
                                       raddoppiato art.36, pratica sanatoria)
  cost_demolizione_raw               — demolition intervention costs
  cost_ripristino_raw                — ripristino / reinstatement intervention costs
  cost_condominiali_arretrati_raw    — overdue / unpaid condominium charges
  cost_spese_tecniche_raw            — professional fees (onorari professionisti, DOCFA,
                                       catasto versamenti)
  cost_altri_oneri_quantificati_raw  — other explicit buyer-side charges (sanzioni
                                       amministrative, diritti di segreteria)

Non-quantified context field types (extracted_value = None):
  onere_non_quantificato_context          — explicitly unquantified buyer burden
  condominiali_non_quantificati_context   — condominium burden without stated amount
  urbanistica_non_quantificata_context    — urbanistic regularisation burden without amount
  ripristino_non_quantificato_context     — ripristino burden without stated amount

What is NOT built here:
  - cost aggregation or summation
  - deprezzamento decomposition or waterfall
  - final "money box" synthesis
  - LLM conflict-resolution
  - doc_map freeze

Root risks mitigated:
  - valuation reductions (riduzione %, deprezzamento) are NOT activated as buyer costs
  - "da verificare" / "da quantificare" warnings are NOT converted to amounts
  - formalità / ipoteche / pignoramenti are excluded (cancelled by the procedure)
  - OMI / methodology prose is excluded
  - SCHEMA RIASSUNTIVO summary pages are blocked as COST_SUMMARY_DUPLICATE_UNSAFE
  - bene-local costs are NOT leaked to lot level
  - same-scope, same-field-type conflicts surface all candidates but emit no ACTIVE winner
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from .runner import build_context
from .corpus_registry import load_cases, list_case_keys
from .bene_scope_map import build_bene_scope_map
from .cadastral_candidate_pack import _build_lookup_tables, _determine_scope as _cadat_determine_scope


# ---------------------------------------------------------------------------
# Monetary amount helpers
# ---------------------------------------------------------------------------

# Matches €, €., Euro followed by a numeric amount.
# OCR may insert spaces inside thousands separators.
_AMOUNT_PAT = re.compile(
    r"(?:€\.?\s*|Euro\s+)"
    r"([\d]{1,3}(?:[\s.]+\d{2,3})*(?:,\d{1,2})?)",
    re.IGNORECASE,
)

# A standalone amount-only line
_AMOUNT_ONLY_LINE_PAT = re.compile(
    r"^\s*(?:€\.?\s*|Euro\s+)"
    r"([\d]{1,3}(?:[\s.]+\d{2,3})*(?:,\d{1,2})?)\s*$",
    re.IGNORECASE,
)

_SUFFIX_AMOUNT_PAT = re.compile(
    r"\b(\d+(?:[\s.]+\d{3})*(?:,\d{1,2})?)\s*€",
    re.IGNORECASE,
)


def _extract_amount_inline(line: str) -> Optional[str]:
    """Return raw matched amount token (including € prefix) or None."""
    m = _AMOUNT_PAT.search(line)
    if not m:
        return None
    return m.group(0).strip()


def _extract_suffix_amount_inline(line: str) -> Optional[str]:
    m = _SUFFIX_AMOUNT_PAT.search(line)
    if not m:
        return None
    return f"€ {m.group(1).strip()}"


# ---------------------------------------------------------------------------
# Exclusion patterns — lines that must never activate cost evidence
# ---------------------------------------------------------------------------

# Table-of-contents dot-leader lines
_DOT_LEADER_PAT = re.compile(r"\.{5,}|(?:\s*\.\s*){6,}")

# SCHEMA RIASSUNTIVO page marker (summary / riepilogo pages)
_SCHEMA_RIASSUNTIVO_PAT = re.compile(r"\bSCHEMA\s+RIASSUNTIVO\b", re.IGNORECASE)

# Per-unit rate (€/mq) — methodology, not a cost total
_PER_UNIT_RATE_PAT = re.compile(
    r"€\s*/\s*mq|€\s*[\d.,]+\s*/\s*mq|\d+[\s,.]?\d*\s*€\s*/\s*mq|€\s*/\s*m²",
    re.IGNORECASE,
)

# Valuation reduction / deprezzamento sentences — NOT buyer-side costs;
# these reflect valuation methodology, not direct cost burdens.
_VALUATION_REDUCTION_PAT = re.compile(
    r"(?:"
    r"riduzione\s+del\s+\d+[\s,.]*%"
    r"|deprezzamento\s+del\s+\d+[\s,.]*%"
    r"|valore\s+finale\s+di\s+stima"
    r"|coefficiente\s+di\s+vetust"
    r"|riduzione\s+per\s+lo\s+stato\s+di\s+occupazione"
    r"|riduzione.*\boccupazione\b"
    r"|decurtazion"
    r")",
    re.IGNORECASE,
)

# Riduzione/Deprezzamento line prefix — valuation reduction, not cost
_RIDUZIONE_PREFIX_PAT = re.compile(
    r"^\s*(?:Riduzione|Deprezzamento)\s+del\b",
    re.IGNORECASE,
)

# Formalità pregiudizievoli / ipoteca / pignoramento — legal encumbrances
# cancelled by the procedure; these are NOT buyer-side cost burdens.
_FORMALITA_PAT = re.compile(
    r"(?:"
    r"\bipoteca\s+(?:volontaria|giudiziale|legale)\b"
    r"|\bverbale\s+di\s+pignoramento\b"
    r"|\bpignoramento\s+immobil"
    r"|\bFormali(?:tà|ta')\s+(?:pregiudizievol|a\s+carico\s+della\s+procedura)"
    r"|\bIscrizioni\b\s*$"
    r"|\bRegistr[oa]\s+(?:Partic|General)"
    r"|\btrascrizione\s+contro\b"
    r"|\bFormalit"
    r")",
    re.IGNORECASE,
)

# Valuation result lines — these belong to the valuation shell, NOT costs
_VALUATION_RESULT_PAT = re.compile(
    r"(?:"
    r"\bvalore\s+di\s+stima\s+del\s+bene\b"
    r"|\bvalore\s+di\s+stima\b"
    r"|\bvalore\s+di\s+mercato\b"
    r"|\bprezzo\s+base\s+d['.']asta\b"
    r"|\bvalore\s+venale\b"
    r"|\bvalore\s+commerciale\b"
    r"|\bvalore\s+di\s+vendita\s+giudiziaria\b"
    r")",
    re.IGNORECASE,
)

# Deprezzamento inline — valuation result, not a cost
_DEPREZZAMENTO_INLINE_PAT = re.compile(r"\bdeprezzam", re.IGNORECASE)

# OMI / comparables / market analysis prose
_OMI_COMPARABLE_PAT = re.compile(
    r"(?:"
    r"\bOMI\b.*quotazion"
    r"|\bquotazioni\s+OMI\b"
    r"|\bvalore\s+(?:medio|unitario)\s+di\s+mercato\s+di\s+€"
    r"|\bprezzo\s+(?:medio|unitario)\s+di\s+(?:mercato|compravendita)\s+"
    r")",
    re.IGNORECASE,
)

# Ordinary annual management costs — NOT buyer-side burden at point of sale
_SPESE_ORDINARIE_PAT = re.compile(
    r"\bspese\s+ordinarie\s+annue\s+di\s+gestione\b",
    re.IGNORECASE,
)

# Methodology / explanatory prose — keyword appears in narrative, not as field assignment
_METHOD_PROSE_PAT = re.compile(
    r"(?:"
    r"la\s+dottrina\s+estimale"
    r"|determinare\s+il\s+(?:valore|pi[uù]\s+probabile)"
    r"|pi[uù]\s+probabile\s+valore\s+di"
    r"|in\s+condizioni\s+di\s+libero\s+mercato"
    r"|banca\s+dati\s+delle\s+quotazioni"
    r"|osservatorio\s+(?:del\s+mercato|immobiliare)\b(?!\s*:)"
    r"|ha\s+condotto\s+alla\s+determinazione"
    r"|l[\x27\u2018\u2019\W]iter\s+di\s+rilascio"
    r"|stima\s+dell[ae]\s+(?:sanzione|fiscalizzazione)"
    r"|fiscalizzazione\s+dell[\x27\u2018\u2019\W]abuso"
    r")",
    re.IGNORECASE,
)

# Label ending with colon (split-pattern lookahead trigger)
_LABEL_ENDS_COLON_PAT = re.compile(r":\s*$")

# Section stop signals: new unrelated section headers encountered during lookahead
_SECTION_STOP_PAT = re.compile(
    r"^\s*(?:"
    r"Tempi\s+necessari\s+per\s+la\s+regolarizzazione"
    r"|VALORE\s+(?:DI\s+)?(?:STIMA|MERCATO|ATTUALE)"
    r"|PREZZO\s+BASE"
    r"|STATO\s+DI\s+POSSESSO"
    r"|CONFORMIT"
    r"|RIEPILOGO\s+VALUTAZIONE"
    r"|SCHEMA\s+RIASSUNTIVO"
    r")",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Quantified cost trigger patterns
# ---------------------------------------------------------------------------

# 1. cost_regolarizzazione_raw — total/aggregate regularization cost
_COSTO_REGOLAR_PAT = re.compile(
    r"(?:"
    r"costi?\s+per\s+regolarizzare\s+l[\x27\u2018\u2019\W]immobile"
    r"|spese?\s+di\s+regolarizzazione"
    r"|spese\s+tecniche\s+di\s+regolarizzazione"
    r"|costi?\s+di\s+regolarizzazione\s*:"   # section header → needs lookahead
    r"|per\s+un\s+totale\s+preventivabile"
    r"|totale\s+costi?\s+(?:per\s+la\s+)?regolarizzazione"
    r"|totale\s+preventivabile"
    r"|si\s+quantificano\s+le\s+spese\s+di\s+massima"  # mantova-style estimate header
    r"|le\s+spese\s+di\s+massima\s+per\b"              # "le spese di massima per ..."
    r")",
    re.IGNORECASE,
)

# Detect section-header triggers that require sub-type lookahead
_COSTO_REGOLAR_HEADER_PAT = re.compile(
    r"(?:"
    r"costi?\s+di\s+regolarizzazione\s*:"
    r"|si\s+quantificano\s+le\s+spese\s+di\s+massima"
    r"|le\s+spese\s+di\s+massima\s+per\b"
    r")",
    re.IGNORECASE,
)

# 2. cost_sanatoria_raw — sanatoria-specific costs
_COSTO_SANATORIA_PAT = re.compile(
    r"(?:"
    r"oneri?\s+per\s+(?:il\s+costo\s+di\s+costruzione|la\s+sanatoria)"
    r"|onorari\w*\s+per\s+(?:la\s+)?redazione\s+pratica\s+sanatoria"
    r"|oblazione\s+per\s+la\s+sanatoria"
    r"|progettazione\s+in\s+sanatoria"
    r"|pratica\s+sanatoria\s+onerosa"
    r"|permesso\s+di\s+costruire\s+in\s+sanatoria"
    r"|spese\s+di\s+massima\s+(?:presunte|della\s+sanatoria)"   # mantova p38
    r"|le\s+spese\s+di\s+massima\s+della\s+sanatoria"
    r")",
    re.IGNORECASE,
)

# 3. cost_demolizione_raw
_COSTO_DEMOLIZIONE_PAT = re.compile(
    r"(?:"
    r"costi?\s+di\s+demolizione"
    r"|intervento\s+di\s+demolizione"
    r"|lavori\s+di\s+demolizione"
    r"|demolizione\s+e\s+(?:ricostruzione|ripristino)"
    r")",
    re.IGNORECASE,
)

# 4. cost_ripristino_raw
_COSTO_RIPRISTINO_PAT = re.compile(
    r"(?:"
    r"costo\s+prev\w*(?:\s+\w+)?\s+dell"  # handles OCR splits in "preventivato" + "dell'inte rvento"
    r"|interventi?\s+di\s+ripristino"
    r"|costi?\s+di\s+ripristino"
    r"|lavori\s+di\s+ripristino"
    r")",
    re.IGNORECASE,
)

# 5. cost_condominiali_arretrati_raw
_COSTO_CONDOMINIALI_PAT = re.compile(
    r"(?:"
    r"spese\s+condominiali\s+scadute(?:\s+ed?\s+insolute)?"
    r"|condominiali\s+scadute\s+ed?\s+insolute"
    r"|spese\s+condominiali\s+insolute"
    r"|arretrati\s+condominiali"
    r"|spese\s+straordinarie\s+di\s+gestione\s+gi[aà]\s+deliberate"
    r")",
    re.IGNORECASE,
)

# 6. cost_spese_tecniche_raw — professional fees
_COSTO_SPESE_TECNICHE_PAT = re.compile(
    r"(?:"
    r"onorari\w*\s+in\s+favore\s+di\s+professionista"
    r"|onorari\w*\s+per\s+(?:la\s+)?redazione\s+pratica\s+edilizia"
    r"|classamento,?\s+oneri\s+e\s+spese"
    r"|onorari\w*\s+per\s+la?\s+(?:redazione|progettazione|presentazione)"
    r"|parcella\s+(?:del\s+)?(?:perito|tecnico|professionista)"
    r"|redazione\s+pratica\s+edilizia"
    r")",
    re.IGNORECASE,
)

# 7. cost_altri_oneri_quantificati_raw — other explicit buyer-side charges
_COSTO_ALTRI_ONERI_PAT = re.compile(
    r"(?:"
    r"sanzione\s+amministrativa"
    r"|diritti\s+di\s+segreteria"
    r")",
    re.IGNORECASE,
)

# "a carico dell'aggiudicatario" WITH an explicit € on same line
_A_CARICO_AGGIUDICATARIO_QUANT_PAT = re.compile(
    r"a\s+carico\s+dell[\x27\u2018\u2019\W]aggiudicatario\b[^€\n]*€",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Non-quantified burden / context trigger patterns
# ---------------------------------------------------------------------------

# onere_non_quantificato_context
_ONERE_NON_QUANT_PAT = re.compile(
    r"(?:"
    r"\bda\s+quantificare\b"
    r"|\bnon\s+(?:è\s+)?quantificat[oa]\b"
    r"|\bda\s+verificare\b"
    r"|\bda\s+sostenere\b"
    r"|\ba\s+(?:cura\s+e\s+)?spese\s+del\s+futuro\s+aggiudicatario\b"
    r"|\ba\s+totale\s+ed\s+esclusivo\s+carico\s+del\s+soggetto.*aggiudicatari"
    r"|\bpossibili\s+costi\b"
    r"|\bpotr[aà]\s+sostenere\b"
    r"|\bdovr[aà]\s+sostenere\b"
    r"|\bdovrà\s+essere\s+provveduto.*spese"
    r")",
    re.IGNORECASE,
)

# condominiali_non_quantificati_context
_CONDOMINIALI_NON_QUANT_PAT = re.compile(
    r"(?:"
    r"\bil\s+CTU\s+non\s+ha\s+i\s+mezzi\s+per"
    r"|\bnon\s+[èe]\s+stato\s+possibile\s+(?:definire|verificare).*condominial"
    r"|\bsituazione\s+debitoria.*condominial"
    r"|\bmillesimi\s+di\s+propriet"
    r")",
    re.IGNORECASE,
)

# urbanistica_non_quantificata_context
_URBANISTICA_NON_QUANT_PAT = re.compile(
    r"(?:"
    r"\bnecessit[aà]\s+di\s+regolarizzazione\s+urbanistica\b"
    r"|\bdovr[aà]\s+essere\s+regolarizzat[oa]\s+urbanisticamente\b"
    r"|\bl[\x27\u2018\u2019\W]immobile\s+(?:non\s+risulta|risulta\s+non)\s+conforme.*urbanisticamente"
    r"|\bnecessit[aà]\s+di\s+sanatoria\s+urbanistica\b"
    r")",
    re.IGNORECASE,
)

# ripristino_non_quantificato_context
_RIPRISTINO_NON_QUANT_PAT = re.compile(
    r"(?:"
    r"\binterventi?\s+di\s+ripristino\s+da\s+quantificare\b"
    r"|\bsaranno\s+necessari\s+interventi\s+di\s+ripristino\b"
    r"|\bin\s+pessime\s+condizioni\s+(?:statiche|di\s+conservazione)\b"
    r"|\bnecessit[aà]\s+di\s+interventi\s+di\s+ripristino\b"
    r")",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Scope helpers (HEADER_GRADE mismatch for multi-lot documents)
# ---------------------------------------------------------------------------

_EXPLICIT_LOT_LABEL_PAT = re.compile(
    r"\bLOTTO\s+([A-Z0-9]+)\b",
    re.IGNORECASE,
)


def _build_lot_header_grade_lookup(plurality_headers: Dict) -> Dict[int, List[Dict]]:
    result: Dict[int, List[Dict]] = {}
    for sig in plurality_headers.get("lot_signals", []):
        if sig.get("class") != "HEADER_GRADE":
            continue
        if sig.get("value") is None:
            continue
        page = int(sig["page"])
        result.setdefault(page, []).append(sig)
    return result


def _check_local_lot_mismatch(
    page: int,
    trigger_line_index: int,
    page_scope_lot_id: str,
    hg_lookup: Dict[int, List[Dict]],
    winner: str,
) -> Optional[str]:
    if winner not in ("H2_EXPLICIT_MULTI_LOT", "H4_CANDIDATE_MULTI_LOT_MULTI_BENE"):
        return None
    scope_lot_norm = str(page_scope_lot_id).strip().lower()
    for sig in hg_lookup.get(page, []):
        sig_lot = str(sig["value"]).strip().lower()
        sig_line = int(sig.get("line_index", 0))
        if sig_line < trigger_line_index and sig_lot != scope_lot_norm:
            return sig_lot
    return None


def _find_local_lot_in_context(
    lines: List[str],
    trigger_idx: int,
    known_lot_ids: Set[str],
    window: int = 5,
) -> Optional[str]:
    found: Optional[str] = None
    start = max(0, trigger_idx - window)
    for line in lines[start:trigger_idx]:
        m = _EXPLICIT_LOT_LABEL_PAT.search(line)
        if m:
            candidate = m.group(1).strip().lower()
            if candidate in known_lot_ids:
                found = candidate
    return found


def _find_forward_lot_in_window(
    lines: List[str],
    trigger_idx: int,
    known_lot_ids: Set[str],
    window: int = 5,
) -> Optional[str]:
    """
    Scan the next `window` non-empty lines forward from trigger_idx for an
    explicit LOTTO X header. Returns the lot_id if one is found within the
    window and it is a known lot, else None.

    Used to detect lot-transition zones: if a different LOTTO header appears
    just after the trigger, the trigger is at the seam between two lot sections
    and the attribution is unsafe.
    """
    end = min(len(lines), trigger_idx + window + 1)
    for line in lines[trigger_idx + 1:end]:
        stripped = line.strip()
        if not stripped:
            continue
        m = _EXPLICIT_LOT_LABEL_PAT.search(stripped)
        if m:
            lot = m.group(1).strip().lower()
            if lot in known_lot_ids:
                return lot
    return None


# Detects backward deprezzamento/valuation-explanation windows.
# When one of these markers appears in the N lines preceding a non-quantified
# trigger, the trigger is inside a CTU valuation reasoning block, not a direct
# buyer-side cost statement.
_DEPR_WINDOW_PAT = re.compile(
    r"(?:"
    r"\battribuire,?\s+tra\s+l\Waltro\b"
    r"|\blo\s+scrivente\s+precisa\s+che\s+il\s+deprezzamento\b"
    r"|\bvalore\s+finale\s+di\s+stima\b"
    r"|\bdeprezzamento\s+del\s+\d"
    r")",
    re.IGNORECASE,
)

_DEPR_REGOLAR_TABLE_ROW_PAT = re.compile(
    r"\bOneri\s+di\s+regolarizzazione\s+urbanistica\b",
    re.IGNORECASE,
)

_DEPR_TABLE_HEADER_PAT = re.compile(
    r"\bTipologia\s+deprezzamento\b.*\bValore\b.*\bTipo\b",
    re.IGNORECASE,
)


def _is_explicit_depr_regolar_table_row(lines: List[str], trigger_idx: int) -> bool:
    if not _DEPR_REGOLAR_TABLE_ROW_PAT.search(lines[trigger_idx]):
        return False
    if not _extract_suffix_amount_inline(lines[trigger_idx]):
        return False
    start = max(0, trigger_idx - 6)
    return any(_DEPR_TABLE_HEADER_PAT.search(line) for line in lines[start:trigger_idx])


def _is_in_depr_window(lines: List[str], trigger_idx: int, window: int = 8) -> bool:
    """
    Return True if the trigger appears to be inside a valuation/deprezzamento
    explanation block. Checks the preceding `window` lines for deprezzamento
    boilerplate markers.
    """
    start = max(0, trigger_idx - window)
    for line in lines[start:trigger_idx]:
        if _DEPR_WINDOW_PAT.search(line):
            return True
    return False


# ---------------------------------------------------------------------------
# Schema page detection
# ---------------------------------------------------------------------------

def _find_schema_pages(raw_pages: List[Dict]) -> Set[int]:
    schema_pages: Set[int] = set()
    for page_data in raw_pages:
        pn = int(page_data["page_number"])
        text = page_data.get("text", "") or ""
        if _SCHEMA_RIASSUNTIVO_PAT.search(text):
            schema_pages.add(pn)
            schema_pages.add(pn + 1)
            schema_pages.add(pn + 2)
    return schema_pages


# Auction-summary recap pages — "RIEPILOGO BANDO D'ASTA" sections repeat lot descriptions
# from other parts of the document; they must not generate fresh cost truth.
_RECAP_ZONE_PAT = re.compile(
    r"RIEPILOGO\s+BANDO\s+D[\x27\u2018\u2019\W]ASTA",
    re.IGNORECASE,
)


def _find_recap_pages(raw_pages: List[Dict], winner: str) -> Set[int]:
    """
    Detect auction-summary recap pages (RIEPILOGO BANDO D'ASTA).
    Only relevant for multi-lot documents; returns empty set for single-lot winners.
    """
    if winner not in ("H2_EXPLICIT_MULTI_LOT", "H4_CANDIDATE_MULTI_LOT_MULTI_BENE"):
        return set()
    recap: Set[int] = set()
    for page_data in raw_pages:
        pn = int(page_data["page_number"])
        text = page_data.get("text", "") or ""
        # Inspect only the first 10 lines to avoid false matches deep in body text
        first_lines = "\n".join(text.split("\n")[:10])
        if _RECAP_ZONE_PAT.search(first_lines):
            recap.add(pn)
    return recap


# ---------------------------------------------------------------------------
# Trigger classification
# ---------------------------------------------------------------------------

def _classify_cost_trigger(line: str) -> Tuple[Optional[str], bool]:
    """
    Classify the line for a quantified cost trigger.

    Returns (field_type_or_None, is_section_header).
    is_section_header: True when the trigger is a section header requiring
    sub-type refinement and longer lookahead.
    """
    # Hard exclusions — checked first
    if _RIDUZIONE_PREFIX_PAT.match(line):
        return None, False
    if _DEPREZZAMENTO_INLINE_PAT.search(line):
        return None, False
    if _VALUATION_REDUCTION_PAT.search(line):
        return None, False
    if _VALUATION_RESULT_PAT.search(line):
        return None, False
    if _FORMALITA_PAT.search(line):
        return None, False
    if _OMI_COMPARABLE_PAT.search(line):
        return None, False
    if _SPESE_ORDINARIE_PAT.search(line):
        return None, False
    if _METHOD_PROSE_PAT.search(line):
        return None, False
    if _PER_UNIT_RATE_PAT.search(line):
        return None, False

    # Quantified trigger patterns (in specificity order)
    if _COSTO_REGOLAR_PAT.search(line):
        is_header = bool(_COSTO_REGOLAR_HEADER_PAT.search(line))
        return "cost_regolarizzazione_raw", is_header

    if _COSTO_SANATORIA_PAT.search(line):
        return "cost_sanatoria_raw", False

    if _COSTO_DEMOLIZIONE_PAT.search(line):
        return "cost_demolizione_raw", False

    if _COSTO_RIPRISTINO_PAT.search(line):
        return "cost_ripristino_raw", False

    if _COSTO_CONDOMINIALI_PAT.search(line):
        return "cost_condominiali_arretrati_raw", False

    if _COSTO_SPESE_TECNICHE_PAT.search(line):
        return "cost_spese_tecniche_raw", False

    if _COSTO_ALTRI_ONERI_PAT.search(line):
        return "cost_altri_oneri_quantificati_raw", False

    if _A_CARICO_AGGIUDICATARIO_QUANT_PAT.search(line):
        return "cost_altri_oneri_quantificati_raw", False

    return None, False


def _sub_classify_regolar_header(description_line: str) -> str:
    """
    Given the first description sub-line under a 'Costi di regolarizzazione:'
    header, determine the most specific quantified field type.
    Priority: sanatoria > spese_tecniche > demolizione > ripristino > regolarizzazione.
    """
    dl = description_line.lower()

    if any(kw in dl for kw in ["sanatoria", "oblazione", "condono"]):
        return "cost_sanatoria_raw"

    if any(kw in dl for kw in [
        "onorario", "onorari", "professionista", "progettazione",
        "classamento", "parcella", "cila", "scia", "docfa", "catasto",
        "redazione", "versamenti al catasto",
    ]):
        return "cost_spese_tecniche_raw"

    if any(kw in dl for kw in ["demolizione", "demolire"]):
        return "cost_demolizione_raw"

    if any(kw in dl for kw in [
        "opere edili", "lavori", "intervento", "chiusura", "ripristino",
        "costruzione", "muratura", "struttura",
    ]):
        return "cost_ripristino_raw"

    return "cost_regolarizzazione_raw"


def _classify_nonquant_trigger(line: str) -> Optional[str]:
    """
    Classify the line for a non-quantified burden context trigger.
    Returns field_type or None.
    Only fires if the line contains NO monetary amount (prevent overlap
    with quantified triggers).
    """
    # If the line has an amount, it belongs to a quantified path
    if _extract_amount_inline(line):
        return None

    # Hard exclusions
    if _FORMALITA_PAT.search(line):
        return None
    if _METHOD_PROSE_PAT.search(line):
        return None

    if _CONDOMINIALI_NON_QUANT_PAT.search(line):
        return "condominiali_non_quantificati_context"
    if _URBANISTICA_NON_QUANT_PAT.search(line):
        return "urbanistica_non_quantificata_context"
    if _RIPRISTINO_NON_QUANT_PAT.search(line):
        return "ripristino_non_quantificato_context"
    if _ONERE_NON_QUANT_PAT.search(line):
        return "onere_non_quantificato_context"

    return None


# ---------------------------------------------------------------------------
# Amount extraction helpers
# ---------------------------------------------------------------------------

def _try_extract_cost_amount(
    trigger_line: str,
    lines: List[str],
    trigger_idx: int,
    is_header: bool = False,
) -> Tuple[Optional[str], int, Optional[str]]:
    """
    Try to extract an amount from the trigger line or subsequent lines.

    For section headers (is_header=True), uses a longer lookahead (up to 5
    non-empty content lines) and may detect a sub-type from the first description
    sub-line.

    Returns (raw_amount_string_or_None, line_offset, description_subline_or_None).
    line_offset == 0  → inline on trigger line
    line_offset >= 1  → found on a subsequent line
    line_offset == -1 → not found
    description_subline is the first non-empty content line after the header
    (used for sub-classification of the 'Costi di regolarizzazione:' header).
    """
    # Inline amount
    inline = _extract_amount_inline(trigger_line)
    if inline:
        return inline, 0, None

    # Only look ahead if line ends with ":"
    if not _LABEL_ENDS_COLON_PAT.search(trigger_line):
        return None, -1, None

    max_nonempty = 5 if is_header else 3
    nonempty_seen = 0
    first_description_line: Optional[str] = None

    for offset in range(1, min(12, len(lines) - trigger_idx)):
        candidate_line = lines[trigger_idx + offset]
        stripped = candidate_line.strip()
        if not stripped:
            continue

        nonempty_seen += 1

        # Capture the first non-empty sub-line (for header sub-classification)
        if first_description_line is None:
            first_description_line = stripped

        # Stop at section-boundary signals
        if _SECTION_STOP_PAT.match(stripped):
            break

        # Skip amounts explicitly flagged as already counted in a previous estimate
        if not re.search(r"\bgià\s+(?:conteggi|compr)", candidate_line, re.IGNORECASE):
            amount = _extract_amount_inline(candidate_line)
            if amount:
                return amount, offset, first_description_line

        if nonempty_seen >= max_nonempty:
            break

    return None, -1, first_description_line


# ---------------------------------------------------------------------------
# Context window helper
# ---------------------------------------------------------------------------

def _make_context_window(lines: List[str], idx: int, back: int = 4, fwd: int = 3) -> str:
    start = max(0, idx - back)
    end = min(len(lines), idx + fwd + 1)
    parts = [ln.strip() for ln in lines[start:end] if ln.strip()]
    return " | ".join(parts)


# ---------------------------------------------------------------------------
# Candidate ID
# ---------------------------------------------------------------------------

def _make_candidate_id(
    field_type: str,
    lot_id: Optional[str],
    bene_id: Optional[str],
    page: int,
    line_index: int,
    match_idx: int,
) -> str:
    lot_part = lot_id or "unknown"
    bene_part = bene_id or "na"
    return f"{field_type}::{lot_part}::{bene_part}::p{page}::l{line_index}::m{match_idx}"


# ---------------------------------------------------------------------------
# Scope attribution → cost block types
# ---------------------------------------------------------------------------

_CADAT_TO_COST_BLOCK: Dict[str, str] = {
    "CADASTRAL_IN_GLOBAL_PRE_LOT_ZONE":      "COST_IN_GLOBAL_PRE_LOT_ZONE",
    "CADASTRAL_IN_SAME_PAGE_LOT_COLLISION":  "COST_IN_SAME_PAGE_LOT_COLLISION",
    "CADASTRAL_IN_SAME_PAGE_BENE_COLLISION": "COST_IN_SAME_PAGE_BENE_COLLISION",
    "CADASTRAL_SCOPE_AMBIGUOUS":             "COST_SCOPE_FIELD_CONFLICT",
    "CADASTRAL_IN_BLOCKED_UNREADABLE":       "COST_SCOPE_FIELD_CONFLICT",
}

_COST_SAFE_ATTRIBUTIONS = {
    "CONFIRMED",
    "ATTRIBUTED_BY_SCOPE",
    "LOT_LEVEL_ONLY",
    "LOT_LEVEL_ONLY_PRE_BENE_CONTEXT",
}

_LOT_LEVEL_ONLY_ATTRIBUTIONS = {
    "LOT_LEVEL_ONLY",
    "LOT_LEVEL_ONLY_PRE_BENE_CONTEXT",
}


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_cost_candidate_pack(case_key: str) -> Dict[str, object]:
    ctx = build_context(case_key)

    hyp_fp   = ctx.artifact_dir / "structure_hypotheses.json"
    scope_fp = ctx.artifact_dir / "lot_scope_map.json"
    pages_fp = ctx.artifact_dir / "raw_pages.json"
    plh_fp   = ctx.artifact_dir / "plurality_headers.json"

    hyp    = json.loads(hyp_fp.read_text(encoding="utf-8"))
    scope  = json.loads(scope_fp.read_text(encoding="utf-8"))
    raw_pages: List[Dict] = json.loads(pages_fp.read_text(encoding="utf-8"))
    plurality_headers: Dict = (
        json.loads(plh_fp.read_text(encoding="utf-8")) if plh_fp.exists() else {}
    )

    winner = hyp.get("winner")

    out: Dict[str, object] = {
        "case_key": case_key,
        "winner": winner,
        "status": "OK",
        "candidates": [],
        "blocked_or_ambiguous": [],
        "warnings": [],
        "coverage": {
            "pages_scanned": len(raw_pages),
            "candidates_harvested": 0,
            "blocked_or_ambiguous_count": 0,
            "cost_packet_count": 0,
            "cost_fields_present": [],
            "cost_scope_keys": [],
            "cost_context_count": 0,
        },
        "summary": {},
        "source_artifacts": {
            "structure_hypotheses": str(hyp_fp),
            "lot_scope_map": str(scope_fp),
            "raw_pages": str(pages_fp),
            "plurality_headers": str(plh_fp),
        },
    }

    dst = ctx.artifact_dir / "cost_candidate_pack.json"

    # Early exit: unreadable document
    if winner == "BLOCKED_UNREADABLE":
        out["status"] = "BLOCKED_UNREADABLE"
        out["summary"]["note"] = (
            "Cost/onere harvesting blocked: document quality is BLOCKED_UNREADABLE."
        )
        dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        return out

    # Build scope lookup tables
    bsm = build_bene_scope_map(case_key)
    lut = _build_lookup_tables(scope, bsm)
    hg_lookup = _build_lot_header_grade_lookup(plurality_headers)
    schema_pages = _find_schema_pages(raw_pages)
    recap_pages = _find_recap_pages(raw_pages, winner)

    known_lot_ids: Set[str] = {
        str(ls["lot_id"]).strip().lower()
        for ls in (scope.get("lot_scopes") or [])
    }

    candidates: List[Dict] = []
    blocked_or_ambiguous: List[Dict] = []
    match_counter = 0

    # -----------------------------------------------------------------------
    # Page scan
    # -----------------------------------------------------------------------
    for page_data in raw_pages:
        page = int(page_data["page_number"])
        text = page_data.get("text", "") or ""
        lines = text.split("\n")
        is_schema_page = page in schema_pages
        is_recap_page = page in recap_pages

        for i, line in enumerate(lines):
            stripped = line.strip()
            if not stripped:
                continue

            # Basic line-level exclusions
            if _DOT_LEADER_PAT.search(line):
                continue

            if _is_explicit_depr_regolar_table_row(lines, i):
                amount_raw = _extract_suffix_amount_inline(line)
                context_win = _make_context_window(lines, i)
                match_counter += 1

                if is_schema_page:
                    blocked_or_ambiguous.append({
                        "type": "COST_SUMMARY_DUPLICATE_UNSAFE",
                        "reason": (
                            "Explicit depreciation-table regolarizzazione row found "
                            "on SCHEMA RIASSUNTIVO page; treated as non-authoritative "
                            "summary duplicate."
                        ),
                        "field_type": "cost_regolarizzazione_raw",
                        "page": page,
                        "line_index": i,
                        "quote": stripped,
                        "extracted_value": amount_raw,
                    })
                    continue

                if is_recap_page:
                    blocked_or_ambiguous.append({
                        "type": "COST_SUMMARY_DUPLICATE_UNSAFE",
                        "reason": (
                            "Explicit depreciation-table regolarizzazione row found "
                            "on RIEPILOGO BANDO D'ASTA recap page; treated as "
                            "non-authoritative auction-summary duplicate."
                        ),
                        "field_type": "cost_regolarizzazione_raw",
                        "page": page,
                        "line_index": i,
                        "quote": stripped,
                        "extracted_value": amount_raw,
                    })
                    continue

                scope_result = _cadat_determine_scope(page, winner, lut)
                attr = scope_result.get("attribution", "CADASTRAL_SCOPE_AMBIGUOUS")
                lot_id = scope_result.get("lot_id")
                bene_id = scope_result.get("bene_id")
                is_blocked = scope_result.get("blocked", False)

                if is_blocked:
                    block_type = _CADAT_TO_COST_BLOCK.get(attr, "COST_SCOPE_FIELD_CONFLICT")
                    blocked_or_ambiguous.append({
                        "type": block_type,
                        "reason": f"Page {page} falls in blocked geometric zone: {attr}.",
                        "field_type": "cost_regolarizzazione_raw",
                        "page": page,
                        "line_index": i,
                        "quote": stripped,
                        "extracted_value": amount_raw,
                        "scope_attribution": attr,
                    })
                    continue

                if attr == "LOT_LEVEL_ONLY_PRE_BENE_CONTEXT":
                    blocked_or_ambiguous.append({
                        "type": "COST_IN_PRE_BENE_CONTEXT",
                        "reason": (
                            "Explicit depreciation-table regolarizzazione row falls in "
                            "pre-bene context; blocked to avoid lot-level contamination."
                        ),
                        "field_type": "cost_regolarizzazione_raw",
                        "page": page,
                        "line_index": i,
                        "quote": stripped,
                        "extracted_value": amount_raw,
                        "lot_id": lot_id,
                        "scope_attribution": attr,
                    })
                    continue

                if bene_id is not None:
                    blocked_or_ambiguous.append({
                        "type": "COST_BENE_SCOPED_DEPREZZAMENTO_TABLE_NOT_PROMOTABLE",
                        "reason": (
                            "Explicit depreciation-table regolarizzazione row resolved "
                            "to a bene scope; not promoted to lot-level cost."
                        ),
                        "field_type": "cost_regolarizzazione_raw",
                        "page": page,
                        "line_index": i,
                        "quote": stripped,
                        "extracted_value": amount_raw,
                        "lot_id": lot_id,
                        "bene_id": bene_id,
                        "scope_attribution": attr,
                    })
                    continue

                local_lot_override = False
                if (
                    winner in ("H2_EXPLICIT_MULTI_LOT", "H4_CANDIDATE_MULTI_LOT_MULTI_BENE")
                    and lot_id is not None
                    and known_lot_ids
                ):
                    local_lot = _find_local_lot_in_context(lines, i, known_lot_ids)
                    if local_lot is not None and local_lot != str(lot_id).strip().lower():
                        lot_id = local_lot
                        attr = "LOT_LOCAL_CONTEXT_OVERRIDE"
                        local_lot_override = True

                if not local_lot_override:
                    mismatch_lot = _check_local_lot_mismatch(page, i, lot_id or "", hg_lookup, winner)
                    if mismatch_lot is not None:
                        blocked_or_ambiguous.append({
                            "type": "COST_LOCAL_SCOPE_HEADER_MISMATCH",
                            "reason": (
                                f"A HEADER_GRADE lot-label signal for lot '{mismatch_lot}' appears "
                                f"before line {i} on page {page}, which falls in lot '{lot_id}' "
                                "by page-scope geometry. Cross-lot attribution is not safe."
                            ),
                            "field_type": "cost_regolarizzazione_raw",
                            "page": page,
                            "line_index": i,
                            "quote": stripped,
                            "extracted_value": amount_raw,
                            "page_scope_lot_id": lot_id,
                            "conflicting_lot_label": mismatch_lot,
                        })
                        continue

                cid = _make_candidate_id(
                    "cost_regolarizzazione_raw", lot_id, None, page, i, match_counter
                )
                candidates.append({
                    "candidate_id": cid,
                    "field_type": "cost_regolarizzazione_raw",
                    "is_quantified": True,
                    "extracted_value": amount_raw,
                    "raw_amount_str": amount_raw,
                    "page": page,
                    "line_index": i,
                    "quote": stripped,
                    "context_window": context_win,
                    "extraction_method": "REGEX_COST_DEPREZZAMENTO_REGOLARIZZAZIONE_TABLE",
                    "lot_id": lot_id,
                    "bene_id": None,
                    "corpo_id": None,
                    "attribution": attr,
                    "scope_basis": (
                        "Explicit lot-level deprezzamento table row "
                        f"'{stripped}' on p{page}l{i}; scope={attr}; lot={lot_id}."
                    ),
                    "candidate_status": "ACTIVE",
                    "amount_line_offset": 0,
                    "source_trigger_field_type": "cost_regolarizzazione_raw",
                    "description_subline": None,
                })
                continue

            # -------------------------------------------------------------------
            # A. Try quantified cost trigger
            # -------------------------------------------------------------------
            field_type, is_header = _classify_cost_trigger(line)
            if field_type:
                # Try to extract amount
                amount_raw, amount_offset, description_subline = _try_extract_cost_amount(
                    line, lines, i, is_header=is_header
                )

                # Sub-classify "Costi di regolarizzazione:" headers
                effective_field_type = field_type
                if is_header and description_subline:
                    effective_field_type = _sub_classify_regolar_header(description_subline)

                context_win = _make_context_window(lines, i)
                match_counter += 1

                # --- SCHEMA RIASSUNTIVO page handling ---
                if is_schema_page:
                    if amount_raw:
                        blocked_or_ambiguous.append({
                            "type": "COST_SUMMARY_DUPLICATE_UNSAFE",
                            "reason": (
                                "Candidate found on SCHEMA RIASSUNTIVO page; "
                                "treated as non-authoritative summary duplicate."
                            ),
                            "field_type": effective_field_type,
                            "page": page,
                            "line_index": i,
                            "quote": stripped,
                            "extracted_value": amount_raw,
                        })
                    continue

                # --- RIEPILOGO BANDO D'ASTA recap page handling ---
                if is_recap_page:
                    if amount_raw:
                        blocked_or_ambiguous.append({
                            "type": "COST_SUMMARY_DUPLICATE_UNSAFE",
                            "reason": (
                                "Candidate found on RIEPILOGO BANDO D'ASTA recap page; "
                                "treated as non-authoritative auction-summary duplicate."
                            ),
                            "field_type": effective_field_type,
                            "page": page,
                            "line_index": i,
                            "quote": stripped,
                            "extracted_value": amount_raw,
                        })
                    continue

                # --- No amount found ---
                if amount_raw is None:
                    # A section header with no amount found is non-quantified context.
                    # A non-header trigger with no amount is noted as such.
                    if is_header:
                        # Non-quantified section context
                        scope_result = _cadat_determine_scope(page, winner, lut)
                        attr = scope_result.get("attribution", "CADASTRAL_SCOPE_AMBIGUOUS")
                        lot_id = scope_result.get("lot_id")
                        bene_id = scope_result.get("bene_id")
                        is_blocked = scope_result.get("blocked", False)

                        if is_blocked:
                            block_type = _CADAT_TO_COST_BLOCK.get(attr, "COST_SCOPE_FIELD_CONFLICT")
                            blocked_or_ambiguous.append({
                                "type": block_type,
                                "reason": f"Non-quantified cost section header on page {page}: scope blocked ({attr}).",
                                "field_type": "onere_non_quantificato_context",
                                "page": page,
                                "line_index": i,
                                "quote": stripped,
                                "scope_attribution": attr,
                            })
                        else:
                            cid = _make_candidate_id(
                                "onere_non_quantificato_context", lot_id, bene_id, page, i, match_counter
                            )
                            candidates.append({
                                "candidate_id": cid,
                                "field_type": "onere_non_quantificato_context",
                                "is_quantified": False,
                                "extracted_value": None,
                                "raw_amount_str": None,
                                "page": page,
                                "line_index": i,
                                "quote": stripped,
                                "context_window": context_win,
                                "extraction_method": "REGEX_COST_NONQUANT_HEADER",
                                "lot_id": lot_id,
                                "bene_id": bene_id,
                                "corpo_id": None,
                                "attribution": attr,
                                "scope_basis": (
                                    f"Non-quantified cost section header on p{page}; "
                                    f"scope={attr}"
                                ),
                                "candidate_status": "ACTIVE",
                                "description_subline": description_subline,
                                "source_trigger_field_type": field_type,
                            })
                    else:
                        blocked_or_ambiguous.append({
                            "type": "COST_NON_QUANTIFIED_CONTEXT_ONLY",
                            "reason": (
                                "Cost trigger found but no associated monetary amount "
                                "on this or next content lines."
                            ),
                            "field_type": effective_field_type,
                            "page": page,
                            "line_index": i,
                            "quote": stripped,
                        })
                    continue

                # --- Determine scope ---
                scope_result = _cadat_determine_scope(page, winner, lut)
                attr = scope_result.get("attribution", "CADASTRAL_SCOPE_AMBIGUOUS")
                lot_id = scope_result.get("lot_id")
                bene_id = scope_result.get("bene_id")
                is_blocked = scope_result.get("blocked", False)

                # --- Blocked by geometric scope ---
                if is_blocked:
                    block_type = _CADAT_TO_COST_BLOCK.get(attr, "COST_SCOPE_FIELD_CONFLICT")
                    blocked_or_ambiguous.append({
                        "type": block_type,
                        "reason": f"Page {page} falls in blocked geometric zone: {attr}.",
                        "field_type": effective_field_type,
                        "page": page,
                        "line_index": i,
                        "quote": stripped,
                        "extracted_value": amount_raw,
                        "scope_attribution": attr,
                    })
                    continue

                # --- LOT_LEVEL_ONLY_PRE_BENE_CONTEXT ---
                if attr == "LOT_LEVEL_ONLY_PRE_BENE_CONTEXT":
                    blocked_or_ambiguous.append({
                        "type": "COST_IN_PRE_BENE_CONTEXT",
                        "reason": (
                            f"Cost candidate on page {page} falls in pre-bene context zone "
                            f"for lot '{lot_id}'. Attribution is LOT_LEVEL_ONLY_PRE_BENE_CONTEXT; "
                            "emitting as blocked to avoid lot-level contamination from pre-bene prose."
                        ),
                        "field_type": effective_field_type,
                        "page": page,
                        "line_index": i,
                        "quote": stripped,
                        "extracted_value": amount_raw,
                        "lot_id": lot_id,
                        "scope_attribution": attr,
                    })
                    continue

                # --- Local lot context override (multi-lot only) ---
                local_lot_override = False
                if (
                    winner in ("H2_EXPLICIT_MULTI_LOT", "H4_CANDIDATE_MULTI_LOT_MULTI_BENE")
                    and lot_id is not None
                    and known_lot_ids
                ):
                    local_lot = _find_local_lot_in_context(lines, i, known_lot_ids)
                    if local_lot is not None and local_lot != str(lot_id).strip().lower():
                        lot_id = local_lot
                        bene_id = None
                        attr = "LOT_LOCAL_CONTEXT_OVERRIDE"
                        local_lot_override = True

                # --- HEADER_GRADE lot-label mismatch (multi-lot only) ---
                if not local_lot_override:
                    mismatch_lot = _check_local_lot_mismatch(page, i, lot_id or "", hg_lookup, winner)
                    if mismatch_lot is not None:
                        blocked_or_ambiguous.append({
                            "type": "COST_LOCAL_SCOPE_HEADER_MISMATCH",
                            "reason": (
                                f"A HEADER_GRADE lot-label signal for lot '{mismatch_lot}' appears "
                                f"before line {i} on page {page}, which falls in lot '{lot_id}' "
                                "by page-scope geometry. Cross-lot attribution is not safe."
                            ),
                            "field_type": effective_field_type,
                            "page": page,
                            "line_index": i,
                            "quote": stripped,
                            "extracted_value": amount_raw,
                            "page_scope_lot_id": lot_id,
                            "conflicting_lot_label": mismatch_lot,
                        })
                        continue

                # --- Emit ACTIVE candidate ---
                cid = _make_candidate_id(
                    effective_field_type, lot_id, bene_id, page, i, match_counter
                )
                candidates.append({
                    "candidate_id": cid,
                    "field_type": effective_field_type,
                    "is_quantified": True,
                    "extracted_value": amount_raw,
                    "raw_amount_str": amount_raw,
                    "page": page,
                    "line_index": i,
                    "quote": stripped,
                    "context_window": context_win,
                    "extraction_method": "REGEX_COST_INLINE" if amount_offset == 0 else "REGEX_COST_SPLIT",
                    "lot_id": lot_id,
                    "bene_id": bene_id,
                    "corpo_id": None,
                    "attribution": attr,
                    "scope_basis": (
                        f"Scope={attr}; lot={lot_id}; bene={bene_id}; p{page}l{i}"
                    ),
                    "candidate_status": "ACTIVE",
                    "amount_line_offset": amount_offset,
                    "source_trigger_field_type": field_type,
                    "description_subline": description_subline if is_header else None,
                })
                continue

            # -------------------------------------------------------------------
            # B. Try non-quantified burden/context trigger
            # -------------------------------------------------------------------
            nonquant_type = _classify_nonquant_trigger(line)
            if nonquant_type:
                context_win = _make_context_window(lines, i)
                match_counter += 1

                if is_schema_page:
                    continue  # Skip schema/riepilogo pages for non-quantified too

                if is_recap_page:
                    continue  # Skip RIEPILOGO BANDO D'ASTA pages for non-quantified too

                scope_result = _cadat_determine_scope(page, winner, lut)
                attr = scope_result.get("attribution", "CADASTRAL_SCOPE_AMBIGUOUS")
                lot_id = scope_result.get("lot_id")
                bene_id = scope_result.get("bene_id")
                is_blocked = scope_result.get("blocked", False)

                if is_blocked:
                    block_type = _CADAT_TO_COST_BLOCK.get(attr, "COST_SCOPE_FIELD_CONFLICT")
                    blocked_or_ambiguous.append({
                        "type": block_type,
                        "reason": (
                            f"Non-quantified context trigger on page {page} "
                            f"falls in blocked geometric zone: {attr}."
                        ),
                        "field_type": nonquant_type,
                        "page": page,
                        "line_index": i,
                        "quote": stripped,
                        "scope_attribution": attr,
                    })
                    continue

                # --- Deprezzamento / valuation-explanation window exclusion ---
                # Lines like "- ai costi da sostenere per la bonifica..." appear inside
                # CTU deprezzamento boilerplate that explains WHY a percentage reduction
                # was applied. These are valuation reasoning, not direct buyer-side costs.
                if _is_in_depr_window(lines, i):
                    blocked_or_ambiguous.append({
                        "type": "COST_VALUATION_REDUCTION_EXCLUDED",
                        "reason": (
                            f"Non-quantified context on p{page}l{i} falls inside a "
                            "valuation/deprezzamento explanation window "
                            "(markers: 'attribuire tra l'altro', 'deprezzamento del', "
                            "'valore finale di stima'). Not safe to activate as "
                            "direct buyer-side cost truth."
                        ),
                        "field_type": nonquant_type,
                        "page": page,
                        "line_index": i,
                        "quote": stripped,
                    })
                    continue

                # --- Local lot context cross-check for multi-lot non-quantified ---
                # (mirrors the same guard in the quantified path A)
                if (
                    winner in ("H2_EXPLICIT_MULTI_LOT", "H4_CANDIDATE_MULTI_LOT_MULTI_BENE")
                    and lot_id is not None
                    and known_lot_ids
                ):
                    local_lot = _find_local_lot_in_context(lines, i, known_lot_ids)
                    if local_lot is not None and local_lot != str(lot_id).strip().lower():
                        # A "LOTTO X" label in the preceding lines contradicts the page-scope
                        # attribution. Block rather than re-attribute to avoid duplicate noise.
                        blocked_or_ambiguous.append({
                            "type": "COST_IN_SAME_PAGE_LOT_COLLISION",
                            "reason": (
                                f"Non-quantified context on p{page}l{i}: local "
                                f"'LOTTO {local_lot.upper()}' label contradicts "
                                f"page-scope lot '{lot_id}'. "
                                "Cross-lot recap contamination. Blocking."
                            ),
                            "field_type": nonquant_type,
                            "page": page,
                            "line_index": i,
                            "quote": stripped,
                            "local_lot_label": local_lot,
                            "page_scope_lot_id": lot_id,
                        })
                        continue

                # --- Forward lot-transition guard for multi-lot non-quantified ---
                # If a different LOTTO X header appears in the next few lines, this
                # trigger is at a lot-section seam. Attribution to the current page-scope
                # lot is not safe.
                if (
                    winner in ("H2_EXPLICIT_MULTI_LOT", "H4_CANDIDATE_MULTI_LOT_MULTI_BENE")
                    and lot_id is not None
                    and known_lot_ids
                ):
                    forward_lot = _find_forward_lot_in_window(lines, i, known_lot_ids)
                    if forward_lot is not None and forward_lot != str(lot_id).strip().lower():
                        blocked_or_ambiguous.append({
                            "type": "COST_IN_SAME_PAGE_LOT_COLLISION",
                            "reason": (
                                f"Forward 'LOTTO {forward_lot.upper()}' header appears "
                                f"within 5 lines after p{page}l{i} (page-scope lot "
                                f"'{lot_id}'). Lot-transition zone; blocking."
                            ),
                            "field_type": nonquant_type,
                            "page": page,
                            "line_index": i,
                            "quote": stripped,
                            "forward_lot_label": forward_lot,
                            "page_scope_lot_id": lot_id,
                        })
                        continue

                cid = _make_candidate_id(
                    nonquant_type, lot_id, bene_id, page, i, match_counter
                )
                candidates.append({
                    "candidate_id": cid,
                    "field_type": nonquant_type,
                    "is_quantified": False,
                    "extracted_value": None,
                    "raw_amount_str": None,
                    "page": page,
                    "line_index": i,
                    "quote": stripped,
                    "context_window": context_win,
                    "extraction_method": "REGEX_COST_NONQUANT",
                    "lot_id": lot_id,
                    "bene_id": bene_id,
                    "corpo_id": None,
                    "attribution": attr,
                    "scope_basis": (
                        f"Non-quantified context; scope={attr}; lot={lot_id}; bene={bene_id}"
                    ),
                    "candidate_status": "ACTIVE",
                    "amount_line_offset": None,
                    "source_trigger_field_type": nonquant_type,
                    "description_subline": None,
                })

    # -----------------------------------------------------------------------
    # Finalize
    # -----------------------------------------------------------------------
    out["candidates"] = candidates
    out["blocked_or_ambiguous"] = blocked_or_ambiguous
    out["coverage"]["candidates_harvested"] = len(candidates)
    out["coverage"]["blocked_or_ambiguous_count"] = len(blocked_or_ambiguous)

    active_quant = [c for c in candidates if c.get("candidate_status") == "ACTIVE" and c.get("is_quantified")]
    active_nonquant = [c for c in candidates if c.get("candidate_status") == "ACTIVE" and not c.get("is_quantified")]

    out["coverage"]["cost_packet_count"] = len(active_quant)
    out["coverage"]["cost_fields_present"] = sorted({c["field_type"] for c in active_quant})
    out["coverage"]["cost_scope_keys"] = sorted({
        (
            f"{c['lot_id']}/{c['bene_id']}"
            if c.get("bene_id")
            else f"lot:{c['lot_id']}"
        )
        for c in active_quant
        if c.get("lot_id")
    })
    out["coverage"]["cost_context_count"] = len(active_nonquant)

    out["summary"] = {
        "active_quantified_candidates": len(active_quant),
        "active_context_candidates": len(active_nonquant),
        "blocked_or_ambiguous": len(blocked_or_ambiguous),
        "quantified_field_types": sorted({c["field_type"] for c in active_quant}),
        "context_field_types": sorted({c["field_type"] for c in active_nonquant}),
        "scope_keys_with_quantified_cost": sorted({
            (
                f"{c['lot_id']}/{c['bene_id']}"
                if c.get("bene_id")
                else f"lot:{c.get('lot_id', 'unknown')}"
            )
            for c in active_quant
        }),
    }

    dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Cost / oneri bounded field shell")
    parser.add_argument("--case", required=True, choices=list(list_case_keys()))
    args = parser.parse_args()

    out = build_cost_candidate_pack(args.case)
    print(json.dumps({
        "case_key": out["case_key"],
        "status": out["status"],
        "winner": out["winner"],
        "active_quantified": out["coverage"]["cost_packet_count"],
        "cost_fields_present": out["coverage"]["cost_fields_present"],
        "cost_scope_keys": out["coverage"]["cost_scope_keys"],
        "active_context": out["coverage"]["cost_context_count"],
        "blocked_or_ambiguous_count": out["coverage"]["blocked_or_ambiguous_count"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
