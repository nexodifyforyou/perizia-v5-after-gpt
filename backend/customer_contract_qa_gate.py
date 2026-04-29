"""
customer_contract_qa_gate.py
----------------------------
LLM-powered QA adjudicator for PeriziaScan customer-facing results.

Pipeline position:
  ... → apply_customer_decision_contract() → apply_customer_contract_qa_gate() → save

The gate challenges conclusions, detects contradictions, applies structured corrections,
then enforces deterministic safety invariants.  Failure of the LLM call never crashes
the analysis — it degrades gracefully to status=WARN with the safety sweep still running.
"""
from __future__ import annotations

import copy
import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from semantic_repair_gates import apply_semantic_repair_gates

# ---------------------------------------------------------------------------
# Environment configuration (all overridable via env vars)
# ---------------------------------------------------------------------------
_DEFAULT_MODEL = "gpt-4o"
_ENABLED_DEFAULT = True

QA_GATE_ENABLED = os.environ.get("QA_GATE_ENABLED", "1").strip() not in ("0", "false", "False", "no")
QA_GATE_MODEL = (
    os.environ.get("QA_GATE_MODEL")
    or os.environ.get("CANONICAL_LLM_MODEL")
    or os.environ.get("OPENAI_MODEL")
    or os.environ.get("NARRATOR_MODEL")
    or _DEFAULT_MODEL
)
QA_GATE_TIMEOUT_SECONDS = int(os.environ.get("QA_GATE_TIMEOUT_SECONDS", "45"))
# Increased from 40k to 120k — a typical 20–40 page perizia is 60–100k chars.
# 40k caused blind-spot failures in later-page evidence for longer documents.
QA_GATE_MAX_CONTEXT_CHARS = int(os.environ.get("QA_GATE_MAX_CONTEXT_CHARS", "120000"))
QA_GATE_CONTEXT_MODE = os.environ.get("QA_GATE_CONTEXT_MODE", "auto").lower()
QA_GATE_MIN_CONFIDENCE = float(os.environ.get("QA_GATE_MIN_CONFIDENCE", "0.65"))
QA_GATE_VERSION = "customer_contract_qa_gate_v2_smart_pages"

# ---------------------------------------------------------------------------
# Keyword groups for page tier-2 selection
# ---------------------------------------------------------------------------
_KEYWORD_GROUPS: Dict[str, List[str]] = {
    "keyword_urbanistica": [
        "urbanistica", "difformità", "difformita", "illegittima", "non conforme",
        "non autorizzata", "abusiva", "abuso", "condono", "sanatoria",
        "fiscalizzazione", "ripristino", "rimessa in pristino", "demolizione",
    ],
    "keyword_beni_details": [
        "compendio", "lotto", "bene", "foglio", "fg.", "mappale", "mapp.",
        "particella", "subalterno", "sub.", "categoria", "a/2", "a/3", "a/4",
        "c/6", "consistenza", "superficie", "scoperti",
    ],
    "keyword_occupancy": [
        "occupato", "occupazione", "possesso", "locazione", "canone",
        "conduttore", "4+4", "registrato", "opponibile", "opponibilità",
        "opponibilita",
    ],
    "keyword_agibilita": [
        "agibilità", "agibilita", "abitabilità", "abitabilita", "non agibile",
        "non abitabile", "non accessibile", "certificato di agibilità",
    ],
    "keyword_money": [
        "costi", "oneri", "spese", "condominiali", "deprezzamento", "detrazione",
        "valore", "stima", "vdm", "regolarizzazione", "fiscalizzazione",
        "ripristino",
    ],
    "keyword_formalities": [
        "pignoramento", "ipoteca", "trascrizione", "servitù", "servitu",
        "vincolo", "ipoteche",
    ],
}

# Severe urbanistica evidence terms (INV-5)
_SEVERE_URBANISTICA_TERMS: List[str] = [
    "illegittima", "non conforme", "non autorizzata", "abusiva", "abuso",
    "condono", "sanatoria", "fiscalizzazione", "ripristino",
    "rimessa in pristino", "demolizione",
]

# ---------------------------------------------------------------------------
# Valuation narrative markers (same list as customer_decision_contract)
# ---------------------------------------------------------------------------
_VALUATION_NARRATIVE_MARKERS = [
    "valore commerciale dei beni pignorati",
    "determinato sulla base",
    "caratteristiche e peculiarità",
    "caratteristiche e peculiarita",
    "domanda e offerta",
    "facilità di raggiungimento",
    "facilita di raggiungimento",
]

# Phrases that signal a fake buyer-side total
_FAKE_COST_PHRASES = [
    "costi espliciti a carico dell",
    "costi espliciti a carico del",
]

# Phrases that indicate a lot is falsely marked libero from irrelevant text
_FALSE_LIBERO_MARKERS = [
    "libero professionista",
    "canone libero",
    "mercato libero",
    "libera professione",
]

# ── Rule 2: buyer-side label patterns ────────────────────────────────────────
_RE_BUYER_SIDE_LABEL = re.compile(r"Costo buyer-side esplicito|buyer.side esplicito", re.I)
_RE_COSTI_ESPLICITI_LABEL = re.compile(
    r"(?:La perizia indica\s+)?[Cc]osti espliciti a carico dell['’' ]acquirente",
    re.I,
)
_SAFE_COST_BLOCKER = "Costi/oneri da verificare: totale extra non quantificato in modo difendibile."
_SAFE_SIGNAL_LABEL_IT = "Segnale economico da verificare"
_SAFE_SIGNAL_LABEL_EN = "Economic signal to verify"
_SAFE_SIGNAL_NOTE = (
    "Segnale economico ancorato alla perizia; non trattato come costo extra "
    "buyer-side certo senza verifica di separata debenza."
)

# ── Rule 3: agibilità assente replacement ────────────────────────────────────
_AGIBILITA_TERM_PATTERN = r"(?:agibilit[àa](?:/abitabilit[àa])?|abitabilit[àa])"
_RE_AGIBILITA_ASSENTE = re.compile(
    rf"(?:\bl['’]\s*)?{_AGIBILITA_TERM_PATTERN}\s*(?::\s*)?"
    r"(?:risulta\s+)?assente(?:\s*(?:/|o)\s*non\s+rilasciat[ao])?\b",
    re.I,
)
_RE_AGIBILITA_NON_RILASCIATA_NEAR = re.compile(
    rf"{_AGIBILITA_TERM_PATTERN}.{{0,80}}\bnon\s+rilasciat[ao]\b|"
    rf"\bnon\s+rilasciat[ao]\b.{{0,80}}{_AGIBILITA_TERM_PATTERN}",
    re.I | re.S,
)
_AGIBILITA_DA_VERIFICARE_SAFE = "Agibilità/abitabilità: DA VERIFICARE"
_AGIBILITA_DA_VERIFICARE_EXPLANATION = (
    "La perizia segnala porzioni/volumi non agibili, non accessibili o non autorizzati; "
    "questo non prova da solo l'assenza globale del certificato di agibilità dell'intera unità."
)
_AGIBILITA_DA_VERIFICARE_SAFE_TEXT = (
    f"{_AGIBILITA_DA_VERIFICARE_SAFE}. {_AGIBILITA_DA_VERIFICARE_EXPLANATION}"
)

_OCCUPIED_HEADLINE_IT = "Stato occupativo: OCCUPATO."
_OCCUPIED_EXPLANATION_IT = (
    "La perizia indica che l'immobile risulta occupato. "
    "L'opponibilità del titolo deve essere verificata separatamente."
)
_OCCUPIED_ACTION_IT = (
    "Verificare separatamente opponibilità del titolo e tempi di liberazione prima dell'offerta."
)
_OCCUPANCY_STALE_UNRESOLVED_RE = re.compile(
    r"resta\s+irrisolt[oa]|campo\s+resta\s+aperto|"
    r"non\s+attribuisce\s+un\s+dato\s+finale|"
    r"rinvio\s+o\s+contesto\s+locale\s+senza\s+valore\s+candidato",
    re.I,
)
_OPPONIBILITA_UNKNOWN_SAFE_TEXT = (
    "L'immobile risulta occupato; l'opponibilità del titolo non è determinabile in modo "
    "difendibile dalle evidenze disponibili. Verificare titolo di occupazione, data certa, "
    "registrazione e opponibilità verso la procedura."
)
_OPPONIBILITA_UNKNOWN_WHY_NOT_RESOLVED = (
    "La perizia conferma l'occupazione, ma non basta per stabilire l'opponibilità del titolo. "
    "Servono titolo, data certa, registrazione e rapporto con la procedura."
)

_URBANISTICA_GRAVE_HEADLINE_IT = "Regolarità urbanistica: NON CONFORME / GRAVE."
_URBANISTICA_GRAVE_ACTION_IT = (
    "Verificare sanabilità, costi di regolarizzazione/ripristino e conformità urbanistica "
    "con tecnico prima dell'offerta."
)
_RE_URBANISTICA_DA_VERIFICARE = re.compile(
    r"(?:Regolarit[àa]\s+urbanistica|urbanistica)\s*:\s*DA\s+VERIFICARE\.?",
    re.I,
)
_RE_URBANISTICA_DA_VERIFICARE_SIMILAR = re.compile(
    r"(?:Regolarit[àa]\s+urbanistica|urbanistica)\s+(?:resta\s+)?DA\s+VERIFICARE\.?",
    re.I,
)

# ── Rule 6: customer-facing scan keys (excluding qa_gate) ────────────────────
_CUSTOMER_FACING_SCAN_KEYS = (
    "issues",
    "field_states",
    "summary_for_client",
    "summary_for_client_bundle",
    "section_2_decisione_rapida",
    "decision_rapida_client",
    "section_3_money_box",
    "money_box",
    "section_9_legal_killers",
    "red_flags_operativi",
    "section_11_red_flags",
    "semaforo_generale",
    "section_1_semaforo_generale",
    "lots",
    "lot_index",
    "beni",
    "checklist_pre_offerta",
    "qa_pass",
    "qa_checks",
    "customer_decision_contract",
)

_BAD_TEXT_PATTERNS = {
    "fake_528": re.compile(r"528[\.\s]?123|528123"),
    "costi_espliciti": re.compile(r"Costi espliciti a carico|costi espliciti a carico"),
    "buyer_side_label": re.compile(r"Costo buyer-side esplicito|buyer-side esplicito", re.I),
}

# ---------------------------------------------------------------------------
# System prompt for the LLM QA adjudicator
# ---------------------------------------------------------------------------
_QA_SYSTEM_PROMPT = """\
You are the final QA adjudicator for PeriziaScan, an evidence-anchored analyzer of Italian \
real-estate auction perizie/CTU PDFs.

Your job is NOT to summarize the document.
Your job is to challenge the generated customer-facing result.

Treat every extracted field as a claim, not as truth.

Compare:
- document page context provided
- current result JSON sections
- field states, issues, Money Box, Legal Killers, Red Flags
- evidence snippets already anchored
- CLAIMS TO CHALLENGE block (explicit contradiction candidates to resolve)

Detect:
- unsafe exact numbers: valuation/deprezzamento/regolarizzazione numbers wrongly treated as payable \
buyer-side costs
- fake buyer-side cost totals summed from periodic condominium amounts (annual average + year total)
- contradictions between Money Box and summary/issues
- occupancy presence mixed with opponibility uncertainty (occupato ≠ opponibile)
- local non-agile/non-authorized part promoted to global agibilità absence
- severe urbanistic issues softened (illegittima/non conforme/non autorizzata/condono/sanatoria → \
NON CONFORME / GRAVE, not merely PRESENTI DIFFORMITA)
- visible property identity facts missing from beni details
- duplicate legal-killer cards
- formalities/vincoli hidden or underreported

Prefer honest uncertainty over fake precision.
Never invent facts not present in the provided context.
Every correction MUST cite evidence_pages and evidence_quotes from the provided context.
Return STRICT JSON ONLY — no commentary, no markdown fences.

Required JSON schema:
{
  "qa_status": "PASS|WARN|FAIL_CORRECTED|BLOCK",
  "overall_verdict_it": "string",
  "context_used": {
    "mode": "FULL_DOCUMENT|PAGE_PACK",
    "pages_reviewed": [1],
    "limitations_it": "string"
  },
  "contradictions_detected": [
    {
      "id": "string",
      "severity": "LOW|MEDIUM|HIGH|CRITICAL",
      "problem_it": "string",
      "current_wrong_claim": "string",
      "evidence_pages": [1],
      "evidence_quotes": ["short quote"],
      "recommended_action": "string"
    }
  ],
  "corrections": [
    {
      "id": "string",
      "target": "money_box|summary|occupancy|opponibility|agibilita|urbanistica|beni_details|legal_killers|red_flags|legal_constraints|duplicates",
      "action": "REMOVE_EXACT_TOTAL|DOWNGRADE_TO_VERIFY|UPGRADE_SEVERITY|SPLIT_OCCUPANCY_OPPONIBILITY|BACKFILL_DETAILS|MERGE_DUPLICATES|AGGREGATE_CONSTRAINTS|REWRITE_SAFE_SUMMARY",
      "safe_value_it": "string",
      "reason_it": "string",
      "evidence_pages": [1],
      "evidence_quotes": ["short quote"],
      "confidence": 0.0,
      "backfill_data": {
        "bene_label": "string",
        "address": "string",
        "tipologia": "string",
        "categoria": "string",
        "superficie": "string",
        "catasto": {"foglio": "", "mappale": "", "sub": ""}
      }
    }
  ],
  "section_verdicts": {
    "money_box": {"ok": true, "note_it": ""},
    "occupancy": {"ok": true, "note_it": ""},
    "opponibility": {"ok": true, "note_it": ""},
    "agibilita": {"ok": true, "note_it": ""},
    "urbanistica": {"ok": true, "note_it": ""},
    "beni_details": {"ok": true, "note_it": ""},
    "duplicates": {"ok": true, "note_it": ""},
    "legal_constraints": {"ok": true, "note_it": ""}
  }
}
"""

# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def apply_customer_contract_qa_gate(
    result: Dict[str, Any],
    raw_text: Optional[str] = None,
    internal_runtime: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Run the QA Gate on the customer-facing result.

    Mutates result in-place: applies corrections and attaches result["qa_gate"] metadata.
    Returns the qa_gate metadata dict.
    Always runs deterministic safety sweep regardless of LLM outcome.
    """
    if not isinstance(result, dict):
        return _empty_qa_gate("WARN", "result is not a dict")

    if not QA_GATE_ENABLED:
        meta = _empty_qa_gate("PASS", "QA_GATE_ENABLED=0 — gate disabled by configuration")
        meta["llm_used"] = False
        attach_qa_gate_metadata(result, meta)
        apply_final_safety_invariants(result, meta, raw_text=raw_text)
        return meta

    qa_report: Dict[str, Any] = _empty_qa_gate("PASS", "")
    page_map: Dict[int, str] = {}

    try:
        context = build_customer_qa_context(result, raw_text=raw_text, internal_runtime=internal_runtime)
        page_map = context.get("_page_map", {})
        qa_report["context_debug"] = context.get("debug", {})

        llm_response_raw = call_customer_qa_llm(context)
        llm_response = validate_customer_qa_response(llm_response_raw)
        qa_report["llm_used"] = True
        qa_report["model"] = QA_GATE_MODEL
        qa_report["context_mode"] = context.get("mode", "PAGE_PACK")
        qa_report["pages_reviewed"] = context.get("pages_reviewed", [])
        qa_report["contradictions_detected"] = llm_response.get("contradictions_detected", [])
        qa_report["section_verdicts"] = llm_response.get("section_verdicts", {})

        llm_status = str(llm_response.get("qa_status") or "WARN").upper()
        if llm_status in ("PASS", "WARN", "FAIL_CORRECTED", "BLOCK"):
            qa_report["status"] = llm_status
        else:
            qa_report["status"] = "WARN"

        corrections = llm_response.get("corrections") or []
        applied = apply_customer_qa_corrections(result, corrections, qa_report)
        qa_report["corrections_applied"] = applied

        if applied and qa_report["status"] in ("PASS", "WARN"):
            qa_report["status"] = "FAIL_CORRECTED"

    except Exception as exc:
        qa_report["status"] = "WARN"
        qa_report["llm_used"] = False
        qa_report["errors"].append(f"LLM call failed: {type(exc).__name__}: {str(exc)[:300]}")

    # Deterministic safety sweep always runs — even if LLM failed.
    apply_final_safety_invariants(result, qa_report, raw_text=raw_text, page_map=page_map)
    attach_qa_gate_metadata(result, qa_report)
    return qa_report


# ---------------------------------------------------------------------------
# Page map builder
# ---------------------------------------------------------------------------

def _normalize_raw_text_to_page_map(raw: Any) -> Dict[int, str]:
    """Convert any raw_text format into {page_number: page_text} dict."""
    if isinstance(raw, str):
        return _split_string_into_page_map(raw)
    if isinstance(raw, list):
        page_map: Dict[int, str] = {}
        for i, item in enumerate(raw):
            if isinstance(item, str):
                page_map[i + 1] = item
            elif isinstance(item, dict):
                pg_num = item.get("page") or item.get("page_number") or (i + 1)
                txt = item.get("text") or item.get("content") or json.dumps(item, ensure_ascii=False)
                try:
                    page_map[int(pg_num)] = txt
                except (TypeError, ValueError):
                    page_map[i + 1] = txt
        return page_map
    if isinstance(raw, dict):
        page_map = {}
        for i, (k, v) in enumerate(raw.items()):
            try:
                page_map[int(k)] = str(v)
            except (TypeError, ValueError):
                page_map[i + 1] = str(v)
        return page_map
    return {}


def _split_string_into_page_map(text: str) -> Dict[int, str]:
    """Split a raw text string into page-numbered chunks."""
    # Form feed pages (most common in PDFMiner-extracted text)
    ff_parts = text.split("\f")
    if len(ff_parts) > 1:
        return {i + 1: p for i, p in enumerate(ff_parts)}

    # PAGINA N / Page N headers with separator lines
    pagina_pattern = re.compile(
        r"(?:^|\n)(?:={10,}|-{10,})\s*\n(?:PAGINA|Page|PAG\.?)\s+(\d+)\s*\n(?:={10,}|-{10,})",
        re.I,
    )
    splits = pagina_pattern.split(text)
    if len(splits) > 2:
        page_map: Dict[int, str] = {}
        if splits[0].strip():
            page_map[0] = splits[0]
        for idx in range(1, len(splits), 2):
            try:
                pg = int(splits[idx])
                content = splits[idx + 1] if idx + 1 < len(splits) else ""
                page_map[pg] = content
            except (IndexError, ValueError):
                pass
        if page_map:
            return page_map

    # Fallback: chunk by ~2500 chars
    chunk_size = 2500
    return {
        i + 1: text[i * chunk_size: (i + 1) * chunk_size]
        for i in range(max(1, (len(text) + chunk_size - 1) // chunk_size))
    }


# ---------------------------------------------------------------------------
# Context builder
# ---------------------------------------------------------------------------

def build_customer_qa_context(
    result: Dict[str, Any],
    raw_text: Optional[Any] = None,
    internal_runtime: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build the context dict passed to the LLM.

    Always scans ALL pages before deciding what to include.
    Adds context_debug and _page_map to the returned dict.
    """
    page_map: Dict[int, str] = {}
    if raw_text is not None:
        page_map = _normalize_raw_text_to_page_map(raw_text)

    result_snapshot = _build_result_snapshot(result)
    claims = _build_claims_to_challenge(result, page_map)

    if not page_map:
        ev_pack = _build_evidence_pack(result)
        debug = {
            "detected_page_count": 0,
            "selected_pages": ev_pack.get("pages_reviewed", []),
            "selected_pages_by_reason": {},
            "context_char_count": 0,
            "mode": "EVIDENCE_ONLY",
        }
        return {
            "mode": "EVIDENCE_ONLY",
            "pages_reviewed": ev_pack.get("pages_reviewed", []),
            "text_pack": ev_pack,
            "result_snapshot": result_snapshot,
            "claims_to_challenge": claims,
            "debug": debug,
            "_page_map": page_map,
            "limitations_it": (
                "Nessun testo grezzo disponibile: contesto limitato alle evidenze "
                "ancorate nel risultato."
            ),
        }

    total_raw_chars = sum(len(t) for t in page_map.values())

    # FULL_DOCUMENT when everything fits within budget
    if total_raw_chars <= QA_GATE_MAX_CONTEXT_CHARS:
        full_text = (
            raw_text
            if isinstance(raw_text, str)
            else "\n\n".join(page_map[p] for p in sorted(page_map))
        )
        pages_reviewed = sorted(page_map.keys())
        selected_by_reason = {p: ["full_document"] for p in pages_reviewed}
        mode = "FULL_DOCUMENT"
        context_chars = len(full_text)
    else:
        # PAGE_PACK: smart tier-based selection
        pack = build_page_text_pack(page_map, result, internal_runtime)
        full_text = pack["full_text"]
        pages_reviewed = pack["pages_reviewed"]
        selected_by_reason = pack.get("selected_by_reason", {})
        mode = "PAGE_PACK"
        context_chars = len(full_text)

    debug: Dict[str, Any] = {
        "detected_page_count": len(page_map),
        "selected_pages": pages_reviewed,
        "selected_pages_by_reason": selected_by_reason,
        "context_char_count": context_chars,
        "mode": mode,
        "total_raw_chars": total_raw_chars,
        "budget_chars": QA_GATE_MAX_CONTEXT_CHARS,
    }

    return {
        "mode": mode,
        "pages_reviewed": pages_reviewed,
        "text_pack": {"mode": mode, "full_text": full_text},
        "result_snapshot": result_snapshot,
        "claims_to_challenge": claims,
        "debug": debug,
        "_page_map": page_map,
        "limitations_it": "" if mode == "FULL_DOCUMENT" else (
            f"Documento troncato: incluse {len(pages_reviewed)}/{len(page_map)} pagine "
            "per priorità (evidenze + parole chiave)."
        ),
    }


def build_page_text_pack(
    page_map: Dict[int, str],
    result: Dict[str, Any],
    internal_runtime: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Select pages by tier priority when total chars exceed budget.

    Tier 1: pages cited by field_states/issues/money_box/legal_killers/red_flags evidence.
    Tier 2: pages containing contradiction-relevant keywords.
    Tier 3: first 2 pages + final 3 pages.
    """
    selected: Dict[int, Set[str]] = {}  # page → set of reasons

    def _add(pg: int, reason: str) -> None:
        if isinstance(pg, int) and pg in page_map:
            selected.setdefault(pg, set()).add(reason)

    # Tier 1 — evidence pages
    for pg in _collect_evidence_pages(result):
        _add(pg, "evidence_page")

    # Tier 1 extras — field_state issue / money_box / legal_killer page refs
    for issue in (result.get("issues") or []):
        for ev in (issue.get("evidence") or [] if isinstance(issue, dict) else []):
            if isinstance(ev, dict) and isinstance(ev.get("page"), int):
                _add(ev["page"], "issue_page")
    for item in ((result.get("section_3_money_box") or {}).get("items") or []):
        if isinstance(item, dict) and isinstance(item.get("page"), int):
            _add(item["page"], "money_box_page")
    for item in ((result.get("section_9_legal_killers") or {}).get("items") or []):
        if isinstance(item, dict):
            for ev in (item.get("evidence") or []):
                if isinstance(ev, dict) and isinstance(ev.get("page"), int):
                    _add(ev["page"], "legal_killer_page")
    for flag in (result.get("red_flags_operativi") or []):
        if isinstance(flag, dict):
            for ev in (flag.get("evidence") or []):
                if isinstance(ev, dict) and isinstance(ev.get("page"), int):
                    _add(ev["page"], "red_flag_page")

    # Tier 2 — keyword pages (scan all pages)
    for pg, txt in page_map.items():
        tl = txt.lower()
        for group, keywords in _KEYWORD_GROUPS.items():
            if any(kw.lower() in tl for kw in keywords):
                _add(pg, group)

    # Tier 3 — first 2 and final 3 pages
    all_pages = sorted(page_map.keys())
    for pg in all_pages[:2]:
        _add(pg, "first_pages")
    for pg in all_pages[-3:]:
        _add(pg, "final_pages")

    # Build page pack within budget — Tier 1 first, then 2, then 3
    def _tier_order(pg_reasons: Tuple[int, Set[str]]) -> int:
        reasons = pg_reasons[1]
        if any(r in reasons for r in ("evidence_page", "issue_page", "money_box_page",
                                       "legal_killer_page", "red_flag_page")):
            return 0
        if any(r.startswith("keyword_") for r in reasons):
            return 1
        return 2

    ordered = sorted(selected.items(), key=_tier_order)

    included_pages: List[int] = []
    parts: List[str] = []
    running_chars = 0

    for pg, reasons in ordered:
        pg_text = page_map[pg]
        if running_chars + len(pg_text) > QA_GATE_MAX_CONTEXT_CHARS:
            break
        included_pages.append(pg)
        parts.append(pg_text)
        running_chars += len(pg_text)

    pages_reviewed = sorted(included_pages)
    full_text = "\n\n".join(page_map[p] for p in pages_reviewed)
    selected_by_reason = {pg: sorted(reasons) for pg, reasons in selected.items() if pg in set(pages_reviewed)}

    return {
        "mode": "PAGE_PACK",
        "full_text": full_text,
        "pages_reviewed": pages_reviewed,
        "selected_by_reason": selected_by_reason,
    }


def _build_evidence_pack(result: Dict[str, Any]) -> Dict[str, Any]:
    """When raw_text is unavailable, collect all evidence snippets from the result."""
    snippets: List[Dict[str, Any]] = []
    pages: List[int] = []

    def _collect(obj: Any) -> None:
        if isinstance(obj, dict):
            if "quote" in obj and "page" in obj:
                snippets.append({"page": obj["page"], "quote": str(obj.get("quote") or "")[:300]})
                pg = obj.get("page")
                if isinstance(pg, int):
                    pages.append(pg)
            for v in obj.values():
                _collect(v)
        elif isinstance(obj, list):
            for item in obj:
                _collect(item)

    _collect(result)
    unique_pages = sorted(set(pages))

    return {
        "mode": "EVIDENCE_ONLY",
        "pages_reviewed": unique_pages[:20],
        "evidence_snippets": snippets[:80],
    }


def _build_result_snapshot(result: Dict[str, Any]) -> Dict[str, Any]:
    """Extract the key customer-facing sections for the LLM context."""
    def _safe(key: str) -> Any:
        return result.get(key)

    return {
        "field_states": _safe("field_states"),
        "issues": (_safe("issues") or [])[:8],
        "money_box_policy": (_safe("section_3_money_box") or {}).get("policy"),
        "money_box_total": (_safe("section_3_money_box") or {}).get("total_extra_costs"),
        "money_box_signals": (_safe("section_3_money_box") or {}).get("cost_signals_to_verify"),
        "money_box_items": [
            {"code": i.get("code"), "label_it": i.get("label_it"), "stima_euro": i.get("stima_euro")}
            for i in ((_safe("section_3_money_box") or {}).get("items") or [])[:10]
        ],
        "summary_it": ((_safe("section_2_decisione_rapida") or {}).get("summary_it") or ""),
        "legal_killers_items": [
            {"killer": i.get("killer"), "status": i.get("status")}
            for i in ((_safe("section_9_legal_killers") or {}).get("items") or [])[:10]
        ],
        "red_flags": (_safe("red_flags_operativi") or [])[:8],
        "lots": [
            {"stato_occupativo": l.get("stato_occupativo"), "ubicazione": l.get("ubicazione")}
            for l in (_safe("lots") or [])[:3]
        ],
        "beni": (_safe("beni") or [])[:4],
        "lot_index": (_safe("lot_index") or [])[:3],
    }


# ---------------------------------------------------------------------------
# Claims-to-challenge builder (Stage 11.3)
# ---------------------------------------------------------------------------

def _build_claims_to_challenge(
    result: Dict[str, Any],
    page_map: Optional[Dict[int, str]] = None,
) -> List[Dict[str, Any]]:
    """Build a compact list of claims the LLM should actively challenge."""
    claims: List[Dict[str, Any]] = []
    field_states = result.get("field_states") or {}

    # Field state contradiction candidates
    for key, state in field_states.items():
        if not isinstance(state, dict):
            continue
        value = str(state.get("value") or "")
        evidence = state.get("evidence") or []
        quotes = [str(ev.get("quote") or "") for ev in evidence if isinstance(ev, dict)]
        all_quotes_lower = " ".join(quotes).lower()

        claim: Dict[str, Any] = {
            "field": key,
            "current_value": value,
            "evidence_quotes_sample": quotes[:3],
        }

        if key == "regolarita_urbanistica":
            severe = any(t in all_quotes_lower for t in _SEVERE_URBANISTICA_TERMS)
            if severe and value.upper() in ("PRESENTI DIFFORMITA", "PRESENTI DIFFORMITÀ",
                                             "CONFORME", "NON VERIFICABILE"):
                claim["contradiction_flag"] = "EVIDENCE_SUGGESTS_SEVERE_URBANISTICA"
                claim["challenge_it"] = (
                    f"regolarita_urbanistica currently = {value!r}; "
                    "evidence quotes contain illegittima/non conforme/non autorizzata — "
                    "likely NON CONFORME / GRAVE, not merely PRESENTI DIFFORMITA."
                )

        elif key == "agibilita":
            local_markers = ["terrapieno", "pertinenza", "locale", "cantina", "box",
                             "scantinato", "parzialmente"]
            if value.upper() == "ASSENTE" and any(m in all_quotes_lower for m in local_markers):
                claim["contradiction_flag"] = "MAY_BE_LOCAL_NOT_GLOBAL_AGIBILITA"
                claim["challenge_it"] = (
                    f"agibilita currently = {value!r}; evidence may be scoped to a single "
                    "part/pertinence, not the whole unit. Consider DOWNGRADE_TO_VERIFY."
                )

        elif key == "stato_occupativo":
            occupied_markers = ["occupato", "locazione", "conduttore", "canone", "4+4",
                                "contratto", "affittuario", "inquilino"]
            if value.upper() in ("NON_VERIFICABILE", "NON VERIFICABILE") and \
                    any(m in all_quotes_lower for m in occupied_markers):
                claim["contradiction_flag"] = "EVIDENCE_SUGGESTS_OCCUPATO"
                claim["challenge_it"] = (
                    f"stato_occupativo currently = {value!r}; "
                    "evidence quotes suggest occupied/locazione — likely OCCUPATO."
                )

        claims.append(claim)

    # Money box: exact total is suspicious
    mb = result.get("section_3_money_box") or {}
    total = mb.get("total_extra_costs") or {}
    mb_min = total.get("min")
    mb_max = total.get("max")
    if isinstance(mb_min, (int, float)) or isinstance(mb_max, (int, float)):
        claims.append({
            "field": "money_box.total_extra_costs",
            "current_value": f"min={mb_min} max={mb_max}",
            "contradiction_flag": "VERIFY_EXACT_BUYER_SIDE_TOTAL",
            "challenge_it": (
                "Money box has an exact numeric total. Verify it is NOT derived from "
                "deprezzamento/regolarizzazione/condo-periodic sums that are NOT payable "
                "buyer-side costs. If uncertain, use REMOVE_EXACT_TOTAL."
            ),
        })

    # Missing beni
    beni = result.get("beni") or []
    has_real_beni = any(
        isinstance(b, dict) and (b.get("address") or b.get("bene_label") or b.get("tipologia"))
        for b in beni
    )
    if not has_real_beni:
        lot_index = result.get("lot_index") or []
        lot_address = ""
        if lot_index and isinstance(lot_index[0], dict):
            lot_address = str(lot_index[0].get("ubicazione") or "")
        claims.append({
            "field": "beni",
            "current_value": "MISSING_OR_EMPTY",
            "contradiction_flag": "BENI_DETAILS_MISSING",
            "challenge_it": (
                "beni is missing/empty. If the document contains property identity facts "
                "(Compendio/Lotto, Via/Locality, Fg/mapp/sub, categoria, superficie mq), "
                "use BACKFILL_DETAILS with backfill_data. "
                + (f"lot_index address hint: {lot_address!r}" if lot_address else "")
            ),
        })

    return claims


# ---------------------------------------------------------------------------
# LLM call
# ---------------------------------------------------------------------------

def call_customer_qa_llm(context: Dict[str, Any]) -> Dict[str, Any]:
    """Call the LLM QA adjudicator. Raises on failure (caller catches)."""
    from perizia_canonical_pipeline.llm_resolution_pack import discover_openai_config

    config = discover_openai_config()
    api_key = config.get("api_key")
    if not api_key:
        raise RuntimeError("No OpenAI API key available for QA Gate")

    model = QA_GATE_MODEL

    # Document text section
    text_section = ""
    text_pack = context.get("text_pack", {})
    full_text = text_pack.get("full_text") or ""
    if full_text:
        mode_label = text_pack.get("mode", "DOCUMENT TEXT")
        text_section = f"\n\n=== {mode_label} ===\n{full_text}"
    elif text_pack.get("evidence_snippets"):
        snippets_str = json.dumps(text_pack["evidence_snippets"], ensure_ascii=False)[:12000]
        text_section = f"\n\n=== EVIDENCE SNIPPETS (no raw text) ===\n{snippets_str}"

    snapshot_str = json.dumps(context.get("result_snapshot", {}), ensure_ascii=False)[:16000]

    claims = context.get("claims_to_challenge", [])
    claims_section = ""
    if claims:
        claims_str = json.dumps(claims, ensure_ascii=False)[:6000]
        claims_section = (
            f"\n\n=== CLAIMS TO CHALLENGE ===\n"
            f"The following fields have flagged contradictions. Address each one explicitly.\n"
            f"{claims_str}"
        )

    user_message = (
        f"=== CUSTOMER RESULT SNAPSHOT ===\n{snapshot_str}"
        f"{claims_section}"
        f"{text_section}"
        f"\n\n=== CONTEXT MODE ===\n{context.get('mode', 'UNKNOWN')}"
        f"\nPages reviewed: {context.get('pages_reviewed', [])}"
        f"\n\nAnalyze the above and return the required QA JSON. "
        f"Pay special attention to CLAIMS TO CHALLENGE — each flagged contradiction "
        f"must be resolved with an explicit correction or a justified PASS verdict."
    )

    try:
        from openai import OpenAI
    except ImportError:
        raise RuntimeError("openai package not available")

    client = OpenAI(api_key=api_key, timeout=QA_GATE_TIMEOUT_SECONDS)
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": _QA_SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        temperature=0,
        response_format={"type": "json_object"},
    )
    raw = response.choices[0].message.content or "{}"
    return _parse_json_response(raw)


def _parse_json_response(text: str) -> Dict[str, Any]:
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, flags=re.S)
        if match:
            return json.loads(match.group(0))
        raise


# ---------------------------------------------------------------------------
# Response validation
# ---------------------------------------------------------------------------

def validate_customer_qa_response(response: Any) -> Dict[str, Any]:
    """Validate and normalize the LLM response. Raises ValueError for critically malformed."""
    if not isinstance(response, dict):
        raise ValueError(f"QA response is not a dict: {type(response)}")

    status = str(response.get("qa_status") or "WARN").upper()
    if status not in ("PASS", "WARN", "FAIL_CORRECTED", "BLOCK"):
        response["qa_status"] = "WARN"

    if not isinstance(response.get("corrections"), list):
        response["corrections"] = []
    if not isinstance(response.get("contradictions_detected"), list):
        response["contradictions_detected"] = []
    if not isinstance(response.get("context_used"), dict):
        response["context_used"] = {}
    if not isinstance(response.get("section_verdicts"), dict):
        response["section_verdicts"] = {}

    return response


# ---------------------------------------------------------------------------
# Correction application
# ---------------------------------------------------------------------------

_KNOWN_TARGETS = {
    "money_box", "summary", "occupancy", "opponibility", "agibilita",
    "urbanistica", "beni_details", "legal_killers", "red_flags",
    "legal_constraints", "duplicates",
}

_KNOWN_ACTIONS = {
    "REMOVE_EXACT_TOTAL", "DOWNGRADE_TO_VERIFY", "UPGRADE_SEVERITY",
    "SPLIT_OCCUPANCY_OPPONIBILITY", "BACKFILL_DETAILS", "MERGE_DUPLICATES",
    "AGGREGATE_CONSTRAINTS", "REWRITE_SAFE_SUMMARY",
}


def apply_customer_qa_corrections(
    result: Dict[str, Any],
    corrections: List[Dict[str, Any]],
    qa_report: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Apply LLM correction instructions to result. Returns list of applied correction ids."""
    applied: List[Dict[str, Any]] = []
    for corr in corrections:
        if not isinstance(corr, dict):
            continue
        target = str(corr.get("target") or "").lower()
        action = str(corr.get("action") or "").upper()
        confidence = float(corr.get("confidence") or 0.0)
        evidence_pages = corr.get("evidence_pages") or []
        evidence_quotes = corr.get("evidence_quotes") or []
        corr_id = str(corr.get("id") or action)

        if target not in _KNOWN_TARGETS or action not in _KNOWN_ACTIONS:
            qa_report["errors"].append(
                f"Skipping unknown correction target={target!r} action={action!r}"
            )
            continue

        if confidence < QA_GATE_MIN_CONFIDENCE:
            qa_report["errors"].append(
                f"Skipping low-confidence correction id={corr_id} confidence={confidence}"
            )
            continue

        if action != "MERGE_DUPLICATES" and not (evidence_pages or evidence_quotes):
            qa_report["errors"].append(
                f"Skipping correction id={corr_id} — no evidence_pages/quotes provided"
            )
            continue

        safe_value = corr.get("safe_value_it") or ""

        try:
            if action == "REMOVE_EXACT_TOTAL":
                _apply_remove_exact_total(result)
                applied.append({"id": corr_id, "action": action, "target": target})

            elif action == "SPLIT_OCCUPANCY_OPPONIBILITY":
                _apply_split_occupancy_opponibility(result, safe_value, evidence_pages, evidence_quotes)
                applied.append({"id": corr_id, "action": action, "target": target})

            elif action == "DOWNGRADE_TO_VERIFY" and target == "agibilita":
                _apply_downgrade_agibilita(result, safe_value)
                applied.append({"id": corr_id, "action": action, "target": target})

            elif action == "UPGRADE_SEVERITY" and target == "urbanistica":
                _apply_upgrade_urbanistica(result, safe_value)
                applied.append({"id": corr_id, "action": action, "target": target})

            elif action == "BACKFILL_DETAILS" and target == "beni_details":
                backfill = corr.get("backfill_data") or {}
                _apply_backfill_details(result, backfill, evidence_pages, evidence_quotes)
                applied.append({"id": corr_id, "action": action, "target": target})

            elif action == "MERGE_DUPLICATES":
                _apply_merge_duplicates(result)
                applied.append({"id": corr_id, "action": action, "target": target})

            elif action == "AGGREGATE_CONSTRAINTS":
                _apply_aggregate_constraints(result, safe_value)
                applied.append({"id": corr_id, "action": action, "target": target})

            elif action == "REWRITE_SAFE_SUMMARY" and safe_value:
                _apply_rewrite_summary(result, safe_value)
                applied.append({"id": corr_id, "action": action, "target": target})

        except Exception as exc:
            qa_report["errors"].append(
                f"Error applying correction id={corr_id}: {type(exc).__name__}: {str(exc)[:200]}"
            )

    return applied


# ── Correction implementations ───────────────────────────────────────────────

_FAKE_TOTAL_PATTERN = re.compile(r"Costi espliciti a carico dell['']acquirente:\s*€\s*[\d\.,]+", re.I)
_NON_ADDITIVE_NOTE = (
    "Costi/oneri da verificare: totale extra non quantificato in modo difendibile. "
    "Verificare importi e separata debenza con tecnico/delegato prima dell'offerta."
)


def _strip_fake_total_from_text(text: Any) -> str:
    if not isinstance(text, str):
        return str(text or "")
    text = _FAKE_TOTAL_PATTERN.sub(_NON_ADDITIVE_NOTE, text)
    # Also remove generic costi/buyer-side phrases that have no amount suffix
    text = _RE_COSTI_ESPLICITI_LABEL.sub(_SAFE_COST_BLOCKER, text)
    text = _RE_BUYER_SIDE_LABEL.sub(_SAFE_SIGNAL_LABEL_IT, text)
    return text.strip()


def _apply_remove_exact_total(result: Dict[str, Any]) -> None:
    """Remove fake buyer-side total from all customer-facing sections."""
    for mb_key in ("section_3_money_box", "money_box"):
        mb = result.get(mb_key)
        if not isinstance(mb, dict):
            continue
        for total_key in ("total_extra_costs", "totale_extra_budget"):
            total = mb.get(total_key)
            if isinstance(total, dict):
                total["min"] = None
                total["max"] = None
                if "range" in total and isinstance(total["range"], dict):
                    total["range"]["min"] = None
                    total["range"]["max"] = None
                if not total.get("note"):
                    total["note"] = _NON_ADDITIVE_NOTE

    _strip_fake_total_from_result_text(result)

    cdc = result.get("customer_decision_contract")
    if isinstance(cdc, dict):
        _strip_fake_total_from_result_text(cdc)
        for mb_key in ("money_box",):
            mb = cdc.get(mb_key)
            if isinstance(mb, dict):
                for total_key in ("total_extra_costs", "totale_extra_budget"):
                    total = mb.get(total_key)
                    if isinstance(total, dict):
                        total["min"] = None
                        total["max"] = None


def _strip_fake_total_from_result_text(container: Dict[str, Any]) -> None:
    def _walk(obj: Any) -> Any:
        if isinstance(obj, str):
            return _strip_fake_total_from_text(obj)
        if isinstance(obj, list):
            return [_walk(item) for item in obj]
        if isinstance(obj, dict):
            return {k: _walk(v) for k, v in obj.items()}
        return obj

    for key in (
        "issues", "summary_for_client", "summary_for_client_bundle",
        "section_2_decisione_rapida", "decision_rapida_client",
        "section_9_legal_killers", "red_flags_operativi", "section_11_red_flags",
        "semaforo_generale", "section_1_semaforo_generale",
    ):
        if key in container:
            container[key] = _walk(container[key])


def _apply_split_occupancy_opponibility(
    result: Dict[str, Any],
    safe_value: str,
    evidence_pages: List[int],
    evidence_quotes: List[str],
) -> None:
    """Separate occupancy (OCCUPATO) from opponibility (DA VERIFICARE) in field_states and lots."""
    evidence = [{"page": p, "quote": q} for p, q in zip(evidence_pages, evidence_quotes)]

    field_states = result.get("field_states")
    if isinstance(field_states, dict):
        occ = field_states.get("stato_occupativo")
        if isinstance(occ, dict) and str(occ.get("value") or "").upper() in (
            "NON_VERIFICABILE", "NON VERIFICABILE", ""
        ):
            occ["value"] = "OCCUPATO"
            occ["status"] = "FOUND"
            occ["headline_it"] = "Stato occupativo: OCCUPATO."
            occ["explanation_it"] = (
                "La perizia indica che l'immobile risulta occupato. "
                "L'opponibilità del titolo deve essere verificata separatamente."
            )
            if evidence:
                occ["evidence"] = evidence

        oppon = field_states.get("opponibilita_occupazione")
        if isinstance(oppon, dict) and str(oppon.get("value") or "").upper() in (
            "OPPONIBILE", "NON_VERIFICABILE"
        ):
            if not _has_explicit_opponibility_evidence(oppon):
                oppon["value"] = "DA VERIFICARE"
                oppon["status"] = "LOW_CONFIDENCE"
                oppon["headline_it"] = "Opponibilità occupazione: DA VERIFICARE."
                oppon["explanation_it"] = (
                    "La perizia segnala un contratto di locazione, ma l'opponibilità effettiva "
                    "dipende da registrazione, data, rinnovi e procedura. Verificare con il delegato."
                )

    for lot in (result.get("lots") or []):
        if not isinstance(lot, dict):
            continue
        if _lot_libero_from_false_marker(lot):
            lot["stato_occupativo"] = "DA VERIFICARE"
            lot["occupancy_status"] = "DA VERIFICARE"

    cdc = result.get("customer_decision_contract")
    if isinstance(cdc, dict) and isinstance(cdc.get("field_states"), dict):
        _apply_split_occupancy_opponibility(cdc, safe_value, evidence_pages, evidence_quotes)


def _has_explicit_opponibility_evidence(state: Dict[str, Any]) -> bool:
    evidence = state.get("evidence") or []
    if not evidence:
        return False
    for ev in evidence:
        quote = str(ev.get("quote") or "").lower()
        if "opponibile" in quote:
            return True
    return False


def _lot_libero_from_false_marker(lot: Dict[str, Any]) -> bool:
    if str(lot.get("stato_occupativo") or "").upper() not in ("LIBERO", "FREE"):
        return False
    evidence = lot.get("evidence") or {}
    occ_ev = evidence.get("occupancy_status") or []
    for ev in (occ_ev if isinstance(occ_ev, list) else []):
        quote = str(ev.get("quote") or "").lower()
        if any(m in quote for m in _FALSE_LIBERO_MARKERS):
            return True
    return False


def _apply_downgrade_agibilita(result: Dict[str, Any], safe_value: str) -> None:
    _update_field_state(
        result, "agibilita",
        value="DA VERIFICARE",
        status="LOW_CONFIDENCE",
        headline_it="Agibilità/abitabilità: DA VERIFICARE.",
        explanation_it=(
            safe_value or
            "Una parte o pertinenza risulta non agibile/non accessibile nella perizia, "
            "ma l'assenza del certificato globale non è confermata esplicitamente."
        ),
    )


def _apply_upgrade_urbanistica(result: Dict[str, Any], safe_value: str) -> None:
    _update_field_state(
        result, "regolarita_urbanistica",
        value="NON CONFORME / GRAVE",
        status="FOUND",
        headline_it="Regolarità urbanistica: NON CONFORME / GRAVE.",
        explanation_it=(
            safe_value or
            "La perizia segnala opere/porzioni illegittime, non conformi o non autorizzate; "
            "sanatoria/condono/ripristino da verificare."
        ),
    )


def _update_field_state(
    result: Dict[str, Any], key: str, *, value: str, status: str,
    headline_it: str, explanation_it: str
) -> None:
    for container_key in ("field_states", None):
        if container_key:
            fs = result.get(container_key)
        else:
            cdc = result.get("customer_decision_contract")
            fs = cdc.get("field_states") if isinstance(cdc, dict) else None

        if not isinstance(fs, dict):
            continue
        state = fs.get(key)
        if not isinstance(state, dict):
            state = {}
            fs[key] = state
        state["value"] = value
        state["status"] = status
        state["headline_it"] = headline_it
        state["explanation_it"] = explanation_it


_PLACEHOLDER_LOCATIONS: frozenset = frozenset({
    "", "indirizzo da verificare", "da verificare", "non disponibile", "n/d", "nd",
})


def _is_placeholder_location(value: Any) -> bool:
    """Return True if value is None, empty, or a known placeholder string."""
    if value is None:
        return True
    return str(value).strip().lower() in _PLACEHOLDER_LOCATIONS


def _apply_backfill_details(
    result: Dict[str, Any],
    backfill: Dict[str, Any],
    evidence_pages: List[int],
    evidence_quotes: List[str],
) -> None:
    if not backfill:
        return
    evidence = [{"page": p, "quote": q} for p, q in zip(evidence_pages, evidence_quotes)]
    beni = result.get("beni")
    if not isinstance(beni, list) or not beni:
        result["beni"] = [{}]
        beni = result["beni"]
    bene = beni[0]
    if not isinstance(bene, dict):
        beni[0] = {}
        bene = beni[0]

    for field in ("bene_label", "address", "tipologia", "categoria"):
        if backfill.get(field):
            bene[field] = backfill[field]
    if backfill.get("superficie"):
        bene["superficie_mq"] = backfill["superficie"]
    if backfill.get("catasto"):
        bene["catasto"] = backfill["catasto"]
    if evidence and not bene.get("evidence"):
        bene["evidence"] = {"note": evidence}

    address = backfill.get("address")
    if address:
        # Overwrite lot_index[*].ubicazione when current value is placeholder/null
        for li in (result.get("lot_index") or []):
            if isinstance(li, dict) and _is_placeholder_location(li.get("ubicazione")):
                li["ubicazione"] = address

        # Overwrite lots[*].ubicazione when current value is placeholder/null
        for lot in (result.get("lots") or []):
            if isinstance(lot, dict) and _is_placeholder_location(lot.get("ubicazione")):
                lot["ubicazione"] = address

        # Mirror into customer_decision_contract if present
        cdc = result.get("customer_decision_contract")
        if isinstance(cdc, dict):
            for li in (cdc.get("lot_index") or []):
                if isinstance(li, dict) and _is_placeholder_location(li.get("ubicazione")):
                    li["ubicazione"] = address
            for lot in (cdc.get("lots") or []):
                if isinstance(lot, dict) and _is_placeholder_location(lot.get("ubicazione")):
                    lot["ubicazione"] = address


def _apply_merge_duplicates(result: Dict[str, Any]) -> None:
    from customer_decision_contract import _dedup_legal_killer_items

    for key in ("issues", "red_flags_operativi", "section_11_red_flags"):
        items = result.get(key)
        if isinstance(items, list):
            result[key] = _dedup_by_headline(items)

    lk = result.get("section_9_legal_killers")
    if isinstance(lk, dict):
        if isinstance(lk.get("items"), list):
            lk["items"] = _dedup_legal_killer_items(lk["items"])
        if isinstance(lk.get("top_items"), list):
            lk["top_items"] = _dedup_legal_killer_items(lk["top_items"])

    cdc = result.get("customer_decision_contract")
    if isinstance(cdc, dict):
        _apply_merge_duplicates(cdc)


def _dedup_by_headline(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: set = set()
    out: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        headline = re.sub(r"\s+", " ", str(
            item.get("headline_it") or item.get("flag_it") or item.get("killer") or ""
        )).strip().lower()
        family = str(item.get("family") or item.get("category") or "").lower()
        key = f"{headline}|{family}"
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _apply_aggregate_constraints(result: Dict[str, Any], safe_value: str) -> None:
    lk = result.get("section_9_legal_killers")
    if not isinstance(lk, dict):
        return
    resolver_meta = lk.get("resolver_meta")
    if not isinstance(resolver_meta, dict):
        resolver_meta = {}
        lk["resolver_meta"] = resolver_meta
    if safe_value:
        resolver_meta["aggregated_constraints_note_it"] = safe_value


def _apply_rewrite_summary(result: Dict[str, Any], safe_value: str) -> None:
    if not safe_value:
        return
    s2 = result.get("section_2_decisione_rapida")
    if isinstance(s2, dict):
        s2["summary_it"] = safe_value
    sc = result.get("summary_for_client")
    if isinstance(sc, dict):
        sc["summary_it"] = safe_value
    elif isinstance(sc, str):
        result["summary_for_client"] = {"summary_it": safe_value, "generation_mode": "qa_gate_rewrite"}
    cdc = result.get("customer_decision_contract")
    if isinstance(cdc, dict):
        dr = cdc.get("decision_rapida_client")
        if isinstance(dr, dict):
            dr["summary_it"] = safe_value
        sfcb = cdc.get("summary_for_client_bundle")
        if isinstance(sfcb, dict):
            sfcb["decision_summary_it"] = safe_value


# ---------------------------------------------------------------------------
# Deterministic safety sweep (INV-1 through INV-6)
# ---------------------------------------------------------------------------

def apply_final_safety_invariants(
    result: Dict[str, Any],
    qa_report: Optional[Dict[str, Any]] = None,
    raw_text: Optional[str] = None,
    page_map: Optional[Dict[int, str]] = None,
) -> None:
    """Enforce non-negotiable post-correction safety invariants.

    INV-1: No fake buyer-side total phrase.
    INV-2: CONSERVATIVE policy → null totals.
    INV-3: No false LIBERO from false markers.
    INV-4: CDC field_states mirrors root field_states.
    INV-5: Severe urbanistica evidence → NON CONFORME / GRAVE (never just PRESENTI DIFFORMITA).
    INV-6: Missing beni → attempt regex backfill from document text.
    """
    if qa_report is None:
        qa_report = _empty_qa_gate("WARN", "safety_sweep_only")
    invariants_checked: List[str] = list(qa_report.get("invariants_checked") or [])

    # INV-1
    if _scan_for_fake_total_phrases(result):
        _apply_remove_exact_total(result)
        _record_safety_correction(qa_report, "INV-1", "REMOVE_EXACT_TOTAL",
                                  "Fake buyer-side total phrase detected by safety sweep and removed.")
    invariants_checked.append("INV-1:no_fake_total_phrase")

    # INV-2
    mb = result.get("section_3_money_box") or {}
    if str(mb.get("policy") or "").upper() == "CONSERVATIVE":
        total = mb.get("total_extra_costs") or {}
        if isinstance(total.get("min"), (int, float)) or isinstance(total.get("max"), (int, float)):
            total["min"] = None
            total["max"] = None
            _record_safety_correction(qa_report, "INV-2", "REMOVE_EXACT_TOTAL",
                                      "Money box CONSERVATIVE policy but numeric total survived — cleared.")
    invariants_checked.append("INV-2:conservative_no_numeric_total")

    # INV-3
    for lot in (result.get("lots") or []):
        if isinstance(lot, dict) and _lot_libero_from_false_marker(lot):
            lot["stato_occupativo"] = "DA VERIFICARE"
            lot["occupancy_status"] = "DA VERIFICARE"
            _record_safety_correction(qa_report, "INV-3", "SPLIT_OCCUPANCY_OPPONIBILITY",
                                      "Lot stato_occupativo=LIBERO derived from false marker — corrected.")
    invariants_checked.append("INV-3:no_false_libero")

    # INV-4
    _sync_cdc_field_states(result, qa_report)
    invariants_checked.append("INV-4:cdc_mirrors_root_field_states")

    # INV-5 — severe urbanistica backstop
    _inv5_severe_urbanistica(result, qa_report, page_map=page_map)
    invariants_checked.append("INV-5:severe_urbanistica_upgrade")

    # INV-6 — missing beni backfill
    _inv6_backfill_beni(result, qa_report, raw_text=raw_text, page_map=page_map)
    invariants_checked.append("INV-6:beni_backfill")

    # INV-7 — semantic repair gates (repair-first money and asset inventory reconciliation)
    semantic_meta = apply_semantic_repair_gates(result, raw_text=raw_text, page_map=page_map)
    if semantic_meta.get("changed"):
        qa_report["semantic_repair_gates"] = semantic_meta
        _record_safety_correction(
            qa_report,
            "INV-7",
            "SEMANTIC_REPAIR_GATES",
            "Money and/or asset inventory projections were repaired from source evidence before final CDC sync.",
        )
        if qa_report.get("status") in ("PASS", "WARN"):
            qa_report["status"] = "FAIL_CORRECTED"
    invariants_checked.append("INV-7:semantic_repair_gates")

    # Customer-facing consistency sweep (Rules 2–5)
    apply_customer_facing_consistency_sweep(result, qa_report)
    invariants_checked.append("INV-8:customer_facing_consistency_sweep")

    qa_report["invariants_checked"] = invariants_checked


def _inv5_severe_urbanistica(
    result: Dict[str, Any],
    qa_report: Dict[str, Any],
    page_map: Optional[Dict[int, str]] = None,
) -> None:
    """INV-5: If urbanistica evidence contains severe terms, upgrade to NON CONFORME / GRAVE."""
    field_states = result.get("field_states") or {}
    urb = field_states.get("regolarita_urbanistica")
    if not isinstance(urb, dict):
        return

    current_value = str(urb.get("value") or "").upper()
    if current_value in ("NON CONFORME / GRAVE", "NON_CONFORME_GRAVE", "NON CONFORME"):
        return  # already upgraded — no action

    # Only upgrade if currently signalling some issues (avoid upgrading CONFORME/ASSENTE)
    if current_value not in (
        "PRESENTI DIFFORMITA", "PRESENTI DIFFORMITÀ", "PRESENTI DIFFORMITA'",
        "NON VERIFICABILE", "DA VERIFICARE",
    ):
        return

    # Check field_states evidence quotes first
    evidence = urb.get("evidence") or []
    quotes_text = " ".join(str(ev.get("quote") or "") for ev in evidence if isinstance(ev, dict)).lower()

    has_severe = any(t in quotes_text for t in _SEVERE_URBANISTICA_TERMS)

    # Also scan urbanistica keyword pages from page_map (narrowly scoped)
    if not has_severe and page_map:
        for pg, txt in page_map.items():
            tl = txt.lower()
            if any(kw.lower() in tl for kw in _KEYWORD_GROUPS["keyword_urbanistica"]):
                # Only scan pages that are already flagged as urbanistica-relevant
                if any(t in tl for t in _SEVERE_URBANISTICA_TERMS):
                    has_severe = True
                    break

    if has_severe:
        _apply_upgrade_urbanistica(
            result,
            "La perizia segnala opere/porzioni illegittime, non conformi o non autorizzate; "
            "sanatoria/condono/ripristino da verificare.",
        )
        _record_safety_correction(
            qa_report, "INV-5", "UPGRADE_SEVERITY",
            "Severe urbanistica terms (illegittima/non conforme/non autorizzata/condono/sanatoria) "
            "found in evidence or urbanistica keyword pages — upgraded from "
            f"{current_value!r} to NON CONFORME / GRAVE.",
        )


# Regex patterns for beni extraction (INV-6)
_RE_COMPENDIO = re.compile(
    r"(?:Compendio|Lotto|Bene)\s+([A-Z](?:\d+)?)", re.I
)
_RE_ADDRESS = re.compile(
    r"(?:Via|Viale|Piazza|Largo|Corso|Località|Loc\.?|Strada)\s+[A-ZÀ-Ùa-zà-ù0-9,'\s/]+?"
    r"(?=\s*[,\n(]|\s+\d{5}|\s+Fg\.|\s+mapp|\s*$)",
    re.I | re.M,
)
_RE_FOGLIO = re.compile(r"(?:Fg\.|Foglio)\s*(\d+)", re.I)
_RE_MAPPALE = re.compile(r"(?:mapp\.|mappale|particella)\s*(\d+)", re.I)
_RE_SUB = re.compile(r"(?:sub\.|sub\b|subalterno)\s*(\d+)", re.I)
_RE_CATEGORIA = re.compile(r"categoria\s+([A-Z]/\d+)", re.I)
_RE_SUPERFICIE = re.compile(r"(\d+)\s*(?:mq|m²|m2)\b", re.I)
_RE_SCOPERTI = re.compile(r"(\d+)\s*(?:mq|m²|m2)\s*(?:scoperti|scoped)", re.I)


def _regex_extract_beni(text: str) -> Dict[str, Any]:
    """Extract property identity fields from text using regex. Returns partial dict."""
    extracted: Dict[str, Any] = {}

    m = _RE_COMPENDIO.search(text)
    if m:
        extracted["bene_label"] = m.group(0).strip()

    addr_matches = _RE_ADDRESS.findall(text)
    if addr_matches:
        addr = max(addr_matches, key=len).strip().rstrip(",")
        if len(addr) >= 8:
            extracted["address"] = addr

    foglio = _RE_FOGLIO.search(text)
    mappale = _RE_MAPPALE.search(text)
    sub = _RE_SUB.search(text)
    catasto: Dict[str, str] = {}
    if foglio:
        catasto["foglio"] = foglio.group(1)
    if mappale:
        catasto["mappale"] = mappale.group(1)
    if sub:
        catasto["sub"] = sub.group(1)
    if catasto:
        extracted["catasto"] = catasto

    cat = _RE_CATEGORIA.search(text)
    if cat:
        extracted["categoria"] = cat.group(1)

    # Superficie: take largest plausible value (avoid picking up irrelevant numbers)
    sup_matches = _RE_SUPERFICIE.findall(text)
    if sup_matches:
        plausible = [int(v) for v in sup_matches if 10 <= int(v) <= 2000]
        if plausible:
            extracted["superficie"] = f"{max(plausible)} mq"

    scoperti = _RE_SCOPERTI.search(text)
    if scoperti:
        extracted["scoperti_mq"] = f"{scoperti.group(1)} mq"

    return extracted


def _inv6_backfill_beni(
    result: Dict[str, Any],
    qa_report: Dict[str, Any],
    raw_text: Optional[str] = None,
    page_map: Optional[Dict[int, str]] = None,
) -> None:
    """INV-6: If beni is empty, attempt regex backfill from document text."""
    beni = result.get("beni") or []
    has_real_beni = any(
        isinstance(b, dict) and (b.get("address") or b.get("bene_label") or b.get("catasto"))
        for b in beni
    )
    if has_real_beni:
        return

    # Build search text — prefer beni keyword pages, fall back to full raw_text
    search_text = ""
    if page_map:
        beni_kws = _KEYWORD_GROUPS["keyword_beni_details"]
        for pg in sorted(page_map.keys()):
            txt = page_map[pg]
            if any(kw.lower() in txt.lower() for kw in beni_kws):
                search_text += txt + "\n"
    elif raw_text:
        search_text = raw_text

    if not search_text:
        return

    extracted = _regex_extract_beni(search_text)
    if not extracted:
        return

    # Require at least two fields to avoid noisy partial extractions
    if len(extracted) < 2:
        return

    if not result.get("beni"):
        result["beni"] = [{}]
    bene = result["beni"][0]
    if not isinstance(bene, dict):
        result["beni"][0] = {}
        bene = result["beni"][0]

    bene.update(extracted)
    bene.setdefault("source", "INV6_regex_backfill")

    _record_safety_correction(
        qa_report, "INV-6", "BACKFILL_DETAILS",
        f"Beni was empty; regex backfill extracted: {list(extracted.keys())}",
    )


# ---------------------------------------------------------------------------
# Customer-facing consistency sweep (Rules 2–5)
# ---------------------------------------------------------------------------

def _relabel_buyer_side_money_box_items(container: Dict[str, Any]) -> None:
    """Rule 2B: Relabel money_box items whose label/note carries a fake buyer-side claim."""
    for mb_key in ("section_3_money_box", "money_box"):
        mb = container.get(mb_key)
        if not isinstance(mb, dict):
            continue
        items = mb.get("items")
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            # Only match on label fields — stima_nota can say "buyer-side" on legitimate items
            combined = " ".join(str(item.get(f) or "") for f in ("label_it", "label_en"))
            if _RE_BUYER_SIDE_LABEL.search(combined) or _RE_COSTI_ESPLICITI_LABEL.search(combined):
                item["label_it"] = _SAFE_SIGNAL_LABEL_IT
                item["label_en"] = _SAFE_SIGNAL_LABEL_EN
                item["type"] = "NON_ADDITIVE_SIGNAL"
                item["classification"] = "cost_signal_to_verify"
                item["stima_nota"] = _SAFE_SIGNAL_NOTE
                item["additive_to_extra_total"] = False


def _clean_semaforo_top_blockers(container: Dict[str, Any]) -> None:
    """Rule 2D: Replace fake buyer-side cost blockers in semaforo_generale top_blockers."""
    for sem_key in ("semaforo_generale", "section_1_semaforo_generale"):
        sem = container.get(sem_key)
        if not isinstance(sem, dict):
            continue
        reason = sem.get("reason_it")
        if isinstance(reason, str) and (
            _RE_BUYER_SIDE_LABEL.search(reason) or _RE_COSTI_ESPLICITI_LABEL.search(reason)
        ):
            sem["reason_it"] = _SAFE_COST_BLOCKER
        blockers = sem.get("top_blockers")
        if not isinstance(blockers, list):
            continue
        for b in blockers:
            if not isinstance(b, dict):
                continue
            for field in ("label_it", "label", "text", "description_it"):
                val = b.get(field)
                if not isinstance(val, str):
                    continue
                if _RE_BUYER_SIDE_LABEL.search(val) or _RE_COSTI_ESPLICITI_LABEL.search(val):
                    b[field] = _SAFE_COST_BLOCKER


def _find_agibilita_absence_overclaim(text: Any) -> Optional[re.Match[str]]:
    if not isinstance(text, str):
        return None
    return (
        _RE_AGIBILITA_ASSENTE.search(text)
        or _RE_AGIBILITA_NON_RILASCIATA_NEAR.search(text)
    )


def _is_agibilita_absence_overclaim(text: Any) -> bool:
    return _find_agibilita_absence_overclaim(text) is not None


def _rewrite_agibilita_absence_overclaim(text: str) -> str:
    if _is_agibilita_absence_overclaim(text):
        return _AGIBILITA_DA_VERIFICARE_SAFE_TEXT
    return text


def _propagate_agibilita_downgrade(container: Dict[str, Any]) -> None:
    """Rule 3: Replace all ASSENTE agibilità claims in customer-facing text sections."""
    def _walk(obj: Any) -> Any:
        if isinstance(obj, str):
            return _rewrite_agibilita_absence_overclaim(obj)
        if isinstance(obj, list):
            return [_walk(item) for item in obj]
        if isinstance(obj, dict):
            return {
                k: (v if str(k) == "qa_gate" else _walk(v))
                for k, v in obj.items()
            }
        return obj

    for key in _CUSTOMER_FACING_SCAN_KEYS:
        if key in container:
            container[key] = _walk(container[key])


def _propagate_occupancy_to_lots(result: Dict[str, Any]) -> None:
    """Rule 4: If field_states.stato_occupativo=OCCUPATO, propagate to lots.

    Skips lots whose evidence contains false libero markers — those were already
    handled by INV-3 and should stay at DA VERIFICARE until manual review.
    """
    field_states = result.get("field_states") or {}
    occ_state = field_states.get("stato_occupativo")
    if not isinstance(occ_state, dict):
        return
    if str(occ_state.get("value") or "").upper() != "OCCUPATO":
        return
    for lot in (result.get("lots") or []):
        if not isinstance(lot, dict):
            continue
        if str(lot.get("stato_occupativo") or "").upper() == "OCCUPATO":
            continue
        # Don't promote lots that have false libero marker evidence
        lot_evidence = lot.get("evidence") or {}
        occ_ev = lot_evidence.get("occupancy_status") or [] if isinstance(lot_evidence, dict) else []
        has_false_marker = any(
            any(m in str(ev.get("quote") or "").lower() for m in _FALSE_LIBERO_MARKERS)
            for ev in (occ_ev if isinstance(occ_ev, list) else [])
        )
        if not has_false_marker:
            lot["stato_occupativo"] = "OCCUPATO"
            lot["occupancy_status"] = "OCCUPATO"


def _is_occupancy_status_related_text(text: Any, path: str = "") -> bool:
    body = str(text or "").lower()
    path_lower = str(path or "").lower()
    if "stato_occupativo" in path_lower:
        return True
    if "stato occupativo" in body or "stato di occupazione" in body:
        return True
    if "immobile occupato" in body:
        return True
    if "opponibil" in body:
        return False
    return bool(re.search(r"\boccupat[oaie]?\b|\boccupazione\b", body, flags=re.I))


def _normalize_occupied_field_state(result: Dict[str, Any]) -> None:
    """Normalize stale customer-facing occupancy text after root stato_occupativo=OCCUPATO."""
    field_states = result.get("field_states") or {}
    occ = field_states.get("stato_occupativo")
    if not isinstance(occ, dict):
        return
    if str(occ.get("value") or "").upper() != "OCCUPATO":
        return

    occ["headline_it"] = _OCCUPIED_HEADLINE_IT
    occ["explanation_it"] = _OCCUPIED_EXPLANATION_IT
    occ["why_not_resolved"] = None

    def _replacement(path: str) -> str:
        tail = path.rsplit(".", 1)[-1].lower()
        if "action" in tail or "verify" in tail:
            return _OCCUPIED_ACTION_IT
        if "headline" in tail or "flag" in tail or "killer" in tail or "label" in tail:
            return _OCCUPIED_HEADLINE_IT
        return _OCCUPIED_EXPLANATION_IT

    def _walk(obj: Any, path: str) -> Any:
        path_lower = path.lower()
        if "qa_gate" in path_lower or "opponibilita_occupazione" in path_lower:
            return obj
        if isinstance(obj, str):
            if (
                _OCCUPANCY_STALE_UNRESOLVED_RE.search(obj)
                and _is_occupancy_status_related_text(obj, path)
            ):
                return _replacement(path)
            return obj
        if isinstance(obj, list):
            return [_walk(item, f"{path}[{idx}]") for idx, item in enumerate(obj)]
        if isinstance(obj, dict):
            return {k: _walk(v, f"{path}.{k}") for k, v in obj.items()}
        return obj

    for key in _CUSTOMER_FACING_SCAN_KEYS:
        if key in result:
            result[key] = _walk(result[key], key)

    for list_key in ("issues", "red_flags_operativi", "section_11_red_flags"):
        items = result.get(list_key)
        if not isinstance(items, list):
            continue
        for idx, item in enumerate(items):
            if not isinstance(item, dict):
                continue
            title = _card_title_text(item)
            if not _is_occupancy_status_related_text(title, f"{list_key}[{idx}]"):
                continue
            for field in (
                "explanation_it", "explanation", "action_it", "action",
                "verify_next_it", "why_not_resolved",
            ):
                val = item.get(field)
                if not isinstance(val, str) or not _OCCUPANCY_STALE_UNRESOLVED_RE.search(val):
                    continue
                if field == "why_not_resolved":
                    item[field] = None
                elif "action" in field or "verify" in field:
                    item[field] = _OCCUPIED_ACTION_IT
                else:
                    item[field] = _OCCUPIED_EXPLANATION_IT


def _opponibilita_is_unknown(state: Dict[str, Any]) -> bool:
    value = re.sub(r"[\s_]+", " ", str(state.get("value") or "").strip().upper())
    status = re.sub(r"[\s_]+", " ", str(state.get("status") or "").strip().upper())
    return (
        value in ("NON VERIFICABILE", "DA VERIFICARE", "LOW CONFIDENCE", "")
        or status in ("LOW CONFIDENCE", "NON VERIFICABILE", "DA VERIFICARE")
    )


def _normalize_opponibilita_when_occupied(result: Dict[str, Any]) -> None:
    """Clean stale occupancy-resolution wording from opponibility when occupancy is confirmed."""
    field_states = result.get("field_states") or {}
    occ = field_states.get("stato_occupativo")
    oppon = field_states.get("opponibilita_occupazione")
    if not isinstance(occ, dict) or not isinstance(oppon, dict):
        return
    if str(occ.get("value") or "").upper() != "OCCUPATO":
        return
    if not _opponibilita_is_unknown(oppon):
        return

    for field in ("explanation", "explanation_it", "verify_next_it"):
        value = oppon.get(field)
        if isinstance(value, str) and _OCCUPANCY_STALE_UNRESOLVED_RE.search(value):
            oppon[field] = _OPPONIBILITA_UNKNOWN_SAFE_TEXT

    why_not_resolved = oppon.get("why_not_resolved")
    if (
        isinstance(why_not_resolved, str)
        and _OCCUPANCY_STALE_UNRESOLVED_RE.search(why_not_resolved)
    ):
        oppon["why_not_resolved"] = _OPPONIBILITA_UNKNOWN_WHY_NOT_RESOLVED


def _rewrite_urbanistica_projection_text(text: str) -> str:
    rewritten = _RE_URBANISTICA_DA_VERIFICARE.sub(_URBANISTICA_GRAVE_HEADLINE_IT, text)
    rewritten = _RE_URBANISTICA_DA_VERIFICARE_SIMILAR.sub(
        _URBANISTICA_GRAVE_HEADLINE_IT,
        rewritten,
    )
    return rewritten


def _is_urbanistica_card(item: Any) -> bool:
    if not isinstance(item, dict):
        return False
    structured = " ".join(
        str(item.get(key) or "")
        for key in ("family", "theme", "category", "code", "issue_id")
    ).lower()
    if "urbanistica" in structured or "conformita_urbanistica" in structured:
        return True
    labels = " ".join(
        str(item.get(key) or "")
        for key in ("headline_it", "flag_it", "killer", "title_it", "label_it", "check_it")
    ).lower()
    return any(
        token in labels
        for token in (
            "regolarità urbanistica", "regolarita urbanistica", "urbanistica",
            "difform", "sanatoria", "condono", "ripristino",
        )
    )


def _rewrite_urbanistica_strings(obj: Any) -> Any:
    if isinstance(obj, str):
        return _rewrite_urbanistica_projection_text(obj)
    if isinstance(obj, list):
        return [_rewrite_urbanistica_strings(item) for item in obj]
    if isinstance(obj, dict):
        return {
            k: (v if str(k) == "qa_gate" else _rewrite_urbanistica_strings(v))
            for k, v in obj.items()
        }
    return obj


def _mark_urbanistica_card_red(item: Any) -> None:
    if not isinstance(item, dict) or not _is_urbanistica_card(item):
        return
    item["severity"] = "RED"
    if "status" in item or "killer" in item:
        item["status"] = "RED"
    item["action_it"] = _URBANISTICA_GRAVE_ACTION_IT
    if "action" in item:
        item["action"] = _URBANISTICA_GRAVE_ACTION_IT


def _sync_urbanistica_checks(container: Dict[str, Any]) -> None:
    def _rewrite_checks(section: Any) -> None:
        if not isinstance(section, dict):
            return
        checks = section.get("checks_it")
        if not isinstance(checks, list):
            return
        section["checks_it"] = [
            _rewrite_urbanistica_projection_text(str(check))
            if isinstance(check, str) else check
            for check in checks
        ]

    _rewrite_checks(container.get("section_2_decisione_rapida"))
    _rewrite_checks(container.get("summary_for_client_bundle"))
    _rewrite_checks(container.get("decision_rapida_client"))


def _propagate_urbanistica_severity(result: Dict[str, Any]) -> None:
    """Project NON CONFORME / GRAVE urbanistica root state into customer-facing cards."""
    field_states = result.get("field_states") or {}
    urb = field_states.get("regolarita_urbanistica")
    if not isinstance(urb, dict):
        return
    if str(urb.get("value") or "").upper() != "NON CONFORME / GRAVE":
        return

    urb["headline_it"] = _URBANISTICA_GRAVE_HEADLINE_IT

    for key in ("issues", "red_flags_operativi", "section_11_red_flags"):
        items = result.get(key)
        if not isinstance(items, list):
            continue
        rewritten_items = _rewrite_urbanistica_strings(items)
        if isinstance(rewritten_items, list):
            result[key] = rewritten_items
        for item in result.get(key) or []:
            _mark_urbanistica_card_red(item)

    legal_killers = result.get("section_9_legal_killers")
    if isinstance(legal_killers, dict):
        result["section_9_legal_killers"] = _rewrite_urbanistica_strings(legal_killers)
        legal_killers = result.get("section_9_legal_killers")
        if isinstance(legal_killers, dict):
            for list_key in ("items", "top_items"):
                for item in (legal_killers.get(list_key) or []):
                    _mark_urbanistica_card_red(item)
            resolver_meta = legal_killers.get("resolver_meta")
            if isinstance(resolver_meta, dict):
                for theme in (resolver_meta.get("themes") or []):
                    if isinstance(theme, dict) and _is_urbanistica_card(theme):
                        theme["severity"] = "RED"
                        theme["action_it"] = _URBANISTICA_GRAVE_ACTION_IT

    for key in ("section_2_decisione_rapida", "summary_for_client_bundle", "decision_rapida_client"):
        section = result.get(key)
        if isinstance(section, dict):
            result[key] = _rewrite_urbanistica_strings(section)
    _sync_urbanistica_checks(result)


def _card_title_text(item: Dict[str, Any]) -> str:
    return str(
        item.get("headline_it")
        or item.get("flag_it")
        or item.get("killer")
        or item.get("title_it")
        or item.get("title")
        or item.get("label_it")
        or item.get("label")
        or ""
    ).strip()


def _semantic_text_key(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = (
        text.replace("à", "a")
        .replace("è", "e")
        .replace("é", "e")
        .replace("ì", "i")
        .replace("ò", "o")
        .replace("ù", "u")
    )
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^\w\s:/€.-]", "", text)
    return text.strip().rstrip(" .")


def _normalized_card_title(item: Dict[str, Any]) -> str:
    return _semantic_text_key(_card_title_text(item))


def _is_exact_primary_occupancy_card(item: Dict[str, Any]) -> bool:
    return _normalized_card_title(item) == "immobile occupato"


def _is_exact_occupancy_status_card(item: Dict[str, Any]) -> bool:
    title = _normalized_card_title(item)
    return re.fullmatch(r"stato\s+occupativo\s*:\s*occupato", title, flags=re.I) is not None


def _is_opponibility_non_verifiable_card(item: Dict[str, Any]) -> bool:
    return re.fullmatch(
        r"Opponibilit[àa]\s+occupazione\s*:\s*(?:NON\s+VERIFICABILE|DA\s+VERIFICARE)\.?",
        _card_title_text(item),
        flags=re.I,
    ) is not None


def _title_has_per_bene_details(item: Dict[str, Any]) -> bool:
    title = _normalized_card_title(item)
    return re.search(
        r"\b(?:bene|lotto|lot)\s*(?:n\.?|n[°º]|numero)?\s*\d+\b",
        title,
        flags=re.I,
    ) is not None


def _has_per_bene_details(item: Dict[str, Any]) -> bool:
    scope = item.get("scope")
    if isinstance(scope, dict):
        level = str(scope.get("level") or "").lower()
        if level in ("bene", "lot", "lotto"):
            return True
        if scope.get("bene_number") or scope.get("lot_number"):
            return True
    return (
        any(item.get(key) for key in ("bene_number", "lot_number", "bene_label", "lot_label"))
        or _title_has_per_bene_details(item)
    )


_SEMANTIC_CARD_SEVERITY_RANK = {
    "BLOCKER": 50,
    "CRITICAL": 50,
    "RED": 40,
    "ROSSO": 40,
    "HIGH": 40,
    "GRAVE": 40,
    "AMBER": 30,
    "YELLOW": 30,
    "MEDIUM": 30,
    "ATTENZIONE": 30,
    "LOW": 20,
    "INFO": 10,
    "GREEN": 0,
    "VERDE": 0,
}


def _semantic_card_severity_rank(value: Any) -> Optional[int]:
    key = _semantic_text_key(value).replace(" ", "_").upper()
    return _SEMANTIC_CARD_SEVERITY_RANK.get(key)


def _semantic_card_family(item: Dict[str, Any]) -> str:
    title = _semantic_text_key(_card_title_text(item))
    structured = _semantic_text_key(
        " ".join(
            str(item.get(key) or "")
            for key in ("family", "category", "theme", "type", "code", "issue_id")
        )
    )
    blob = f"{structured} {title}"
    if "opponibil" in blob:
        return "opponibilita_occupazione"
    if "agibil" in blob or "abitabil" in blob:
        return "agibilita"
    if "urbanistica" in blob or "difform" in blob or "sanatoria" in blob or "condono" in blob:
        return "urbanistica"
    if "occupat" in blob or "stato occupativo" in blob or "occupazione" in blob:
        return "occupancy"
    if "catastal" in blob or "catasto" in blob:
        return "catastale"
    explicit = _semantic_text_key(item.get("family") or item.get("category") or item.get("theme"))
    return "" if explicit in ("", "legal", "issue", "red_flag", "redflag") else explicit


def _is_money_or_cost_semantic_card(item: Dict[str, Any]) -> bool:
    family = _semantic_text_key(item.get("family") or item.get("category") or item.get("theme") or item.get("type"))
    title = _semantic_text_key(_card_title_text(item))
    code = _semantic_text_key(item.get("code") or item.get("issue_id"))
    blob = f"{family} {title} {code}"
    return any(
        token in blob
        for token in (
            "money", "cost", "costi", "costo", "oneri", "spese", "euro", "eur",
            "€", "stima", "prezzo", "valore", "pricing", "valuation",
        )
    )


def _coerce_page(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if re.fullmatch(r"\d+", stripped):
            return int(stripped)
    return None


def _merge_pages_from_value(value: Any, pages: List[int]) -> None:
    page = _coerce_page(value)
    if page is not None:
        pages.append(page)
        return
    if isinstance(value, list):
        for child in value:
            _merge_pages_from_value(child, pages)
    elif isinstance(value, dict):
        for key, child in value.items():
            if str(key).lower() in ("page", "pagina", "page_number"):
                _merge_pages_from_value(child, pages)


def _semantic_card_page_set(item: Dict[str, Any]) -> Tuple[int, ...]:
    pages: List[int] = []
    for key in ("page", "pagina", "page_number", "supporting_pages", "evidence_pages", "pages", "searched_pages"):
        if key in item:
            _merge_pages_from_value(item.get(key), pages)
    evidence = item.get("evidence")
    if isinstance(evidence, (list, dict)):
        _merge_pages_from_value(evidence, pages)
    return tuple(sorted({page for page in pages if page > 0}))


def _semantic_card_asset_signature(item: Dict[str, Any]) -> Tuple[str, ...]:
    refs: Set[str] = set()
    scope = item.get("scope")
    if isinstance(scope, dict):
        for key, prefix in (("bene_number", "bene"), ("lot_number", "lotto")):
            value = scope.get(key)
            if value not in (None, ""):
                refs.add(f"{prefix}:{value}")
        scope_key = str(scope.get("scope_key") or "")
        for prefix, number in re.findall(r"\b(bene|lotto|lot)\s*:?\s*(\d+)\b", scope_key, flags=re.I):
            refs.add(f"{'lotto' if prefix.lower() == 'lot' else prefix.lower()}:{number}")

    for key, prefix in (("bene_number", "bene"), ("lot_number", "lotto")):
        value = item.get(key)
        if value not in (None, ""):
            refs.add(f"{prefix}:{value}")

    texts: List[str] = [_card_title_text(item)]
    for ev in item.get("evidence") or []:
        if isinstance(ev, dict):
            texts.append(str(ev.get("quote") or ev.get("text") or ev.get("snippet") or ""))
    blob = _semantic_text_key(" ".join(texts))
    for prefix, number in re.findall(
        r"\b(bene|lotto|lot)\s*(?:n\.?|n[°º]|numero)?\s*(\d+)\b",
        blob,
        flags=re.I,
    ):
        refs.add(f"{'lotto' if prefix.lower() == 'lot' else prefix.lower()}:{number}")
    return tuple(sorted(refs))


def _semantic_card_status_partition(item: Dict[str, Any]) -> str:
    if _semantic_card_severity_rank(item.get("severity")) is not None:
        return ""
    if _semantic_card_severity_rank(item.get("status")) is not None:
        return ""
    return _semantic_text_key(item.get("status"))


def _semantic_card_dedup_key(item: Dict[str, Any]) -> Optional[Tuple[str, str, Tuple[int, ...], Tuple[str, ...], str]]:
    title = _normalized_card_title(item)
    if not title or _is_money_or_cost_semantic_card(item):
        return None
    return (
        _semantic_card_family(item),
        title,
        _semantic_card_page_set(item),
        _semantic_card_asset_signature(item),
        _semantic_card_status_partition(item),
    )


def _semantic_card_quality(item: Dict[str, Any]) -> Tuple[int, int, int, int]:
    evidence_count = len(item.get("evidence") or []) if isinstance(item.get("evidence"), list) else 0
    page_count = len(_semantic_card_page_set(item))
    action_score = max(_semantic_card_action_quality(item, key) for key in ("action_it", "action", "verify_next_it"))
    detail_len = max(
        len(str(item.get(key) or ""))
        for key in ("explanation_it", "explanation", "detail_it", "reason_it")
    )
    return (evidence_count, page_count, action_score, detail_len)


def _semantic_card_action_quality(item: Dict[str, Any], key: str) -> int:
    value = str(item.get(key) or "").strip()
    if not value:
        return 0
    title = _normalized_card_title(item)
    action = _semantic_text_key(value)
    if not action:
        return 0
    if action == title:
        return 1
    if len(action) <= 12:
        return 1
    if action in title or title in action:
        return 2
    return 3


def _merge_semantic_evidence(first: Any, second: Any) -> List[Any]:
    merged: List[Any] = []
    seen: Set[Tuple[Any, str]] = set()
    first_items = first if isinstance(first, list) else []
    second_items = second if isinstance(second, list) else []
    for ev in first_items + second_items:
        if not isinstance(ev, dict):
            key = (None, _semantic_text_key(ev))
        else:
            key = (
                _coerce_page(ev.get("page") or ev.get("pagina") or ev.get("page_number")),
                _semantic_text_key(ev.get("quote") or ev.get("text") or ev.get("snippet") or ev),
            )
        if key in seen:
            continue
        seen.add(key)
        merged.append(copy.deepcopy(ev))
    return merged


def _merged_page_list(first: Any, second: Any) -> List[int]:
    pages: List[int] = []
    _merge_pages_from_value(first, pages)
    _merge_pages_from_value(second, pages)
    out: List[int] = []
    seen: Set[int] = set()
    for page in pages:
        if page <= 0 or page in seen:
            continue
        seen.add(page)
        out.append(page)
    return out


def _merge_string_list(first: Any, second: Any) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    first_items = first if isinstance(first, list) else []
    second_items = second if isinstance(second, list) else []
    for value in first_items + second_items:
        if not isinstance(value, str):
            continue
        key = _semantic_text_key(value)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _stronger_semantic_severity(first: Any, second: Any) -> Any:
    first_rank = _semantic_card_severity_rank(first)
    second_rank = _semantic_card_severity_rank(second)
    if first_rank is None:
        return second if second_rank is not None else first
    if second_rank is None:
        return first
    return second if second_rank > first_rank else first


def _merge_semantic_duplicate_card(existing: Dict[str, Any], duplicate: Dict[str, Any]) -> Dict[str, Any]:
    if _semantic_card_quality(duplicate) > _semantic_card_quality(existing):
        merged = copy.deepcopy(duplicate)
        other = existing
    else:
        merged = copy.deepcopy(existing)
        other = duplicate

    if "severity" in existing or "severity" in duplicate:
        merged["severity"] = _stronger_semantic_severity(existing.get("severity"), duplicate.get("severity"))
    if "status" in existing or "status" in duplicate:
        stronger_status = _stronger_semantic_severity(existing.get("status"), duplicate.get("status"))
        if stronger_status not in (None, ""):
            merged["status"] = stronger_status

    if isinstance(existing.get("evidence"), list) or isinstance(duplicate.get("evidence"), list):
        merged["evidence"] = _merge_semantic_evidence(existing.get("evidence"), duplicate.get("evidence"))
    for key in ("supporting_pages", "evidence_pages", "pages", "searched_pages"):
        if key in existing or key in duplicate:
            merged[key] = _merged_page_list(existing.get(key), duplicate.get(key))
    for key in ("evidence_quotes", "quotes"):
        if key in existing or key in duplicate:
            merged[key] = _merge_string_list(existing.get(key), duplicate.get(key))

    for key in ("action_it", "action", "verify_next_it", "verify_next"):
        if _semantic_card_action_quality(other, key) > _semantic_card_action_quality(merged, key):
            merged[key] = other.get(key)

    return merged


def _dedup_customer_semantic_card_list(items: List[Any]) -> List[Any]:
    out: List[Any] = []
    index_by_key: Dict[Tuple[str, str, Tuple[int, ...], Tuple[str, ...], str], int] = {}
    for item in items:
        if not isinstance(item, dict):
            out.append(item)
            continue
        key = _semantic_card_dedup_key(item)
        if key is None:
            out.append(item)
            continue
        existing_index = index_by_key.get(key)
        if existing_index is None:
            index_by_key[key] = len(out)
            out.append(item)
            continue
        existing = out[existing_index]
        if isinstance(existing, dict):
            out[existing_index] = _merge_semantic_duplicate_card(existing, item)
    return out


def _dedup_final_customer_semantic_cards(result: Dict[str, Any]) -> None:
    for key in ("issues", "red_flags_operativi", "section_11_red_flags"):
        items = result.get(key)
        if isinstance(items, list):
            result[key] = _dedup_customer_semantic_card_list(items)

    legal_killers = result.get("section_9_legal_killers")
    if isinstance(legal_killers, dict):
        for list_key in ("items", "top_items"):
            items = legal_killers.get(list_key)
            if isinstance(items, list):
                legal_killers[list_key] = _dedup_customer_semantic_card_list(items)


def _set_card_title(item: Dict[str, Any], title: str) -> None:
    for key in ("headline_it", "flag_it", "killer", "title_it", "label_it"):
        if key in item:
            item[key] = title
            return
    item["headline_it"] = title


def _dedup_occupancy_cards(items: List[Any]) -> List[Any]:
    """Deduplicate generic occupancy cards by title while preserving distinct issue families."""
    has_primary = any(
        isinstance(item, dict)
        and _is_exact_primary_occupancy_card(item)
        and not _title_has_per_bene_details(item)
        for item in items
    )
    out: List[Any] = []
    for item in items:
        if not isinstance(item, dict):
            out.append(item)
            continue
        if _is_exact_primary_occupancy_card(item):
            _set_card_title(item, "Immobile occupato.")
            item["severity"] = "RED"
            out.append(item)
            continue
        if _is_opponibility_non_verifiable_card(item):
            item["severity"] = "AMBER"
            out.append(item)
            continue
        if (
            has_primary
            and _is_exact_occupancy_status_card(item)
            and not _title_has_per_bene_details(item)
        ):
            continue
        out.append(item)
    return out


def _dedup_semantic_issue_cards(result: Dict[str, Any]) -> None:
    """Remove duplicate customer-facing semantic cards without collapsing distinct legal facts."""
    for key in ("issues", "red_flags_operativi", "section_11_red_flags"):
        items = result.get(key)
        if isinstance(items, list):
            result[key] = _dedup_occupancy_cards(items)
    _dedup_final_customer_semantic_cards(result)


def _sync_cdc_full_sections(result: Dict[str, Any]) -> None:
    """Rule 5: Deep-sync customer_decision_contract with corrected root sections."""
    cdc = result.get("customer_decision_contract")
    if not isinstance(cdc, dict):
        return
    direct_mirror = (
        "issues", "money_box", "section_3_money_box",
        "red_flags_operativi", "section_11_red_flags",
        "section_9_legal_killers",
        "semaforo_generale", "section_1_semaforo_generale",
        "lots", "lot_index", "beni", "lots_count", "is_multi_lot", "detail_scope",
        "case_header", "report_header", "asset_inventory_repair", "money_semantic_repair",
        "summary_for_client", "summary_for_client_bundle",
    )
    for key in direct_mirror:
        if key in result and result[key] is not None:
            cdc[key] = copy.deepcopy(result[key])
    # CDC decision_rapida_client: prefer summary_for_client (most corrected), fall back to section_2
    sfc = result.get("summary_for_client")
    s2 = result.get("section_2_decisione_rapida")
    if sfc is not None:
        decision = copy.deepcopy(sfc)
        if isinstance(decision, dict) and isinstance(s2, dict):
            for key in ("checks_it", "main_risk_it"):
                if key not in decision and key in s2:
                    decision[key] = copy.deepcopy(s2[key])
        cdc["decision_rapida_client"] = decision
    elif s2 is not None:
        cdc["decision_rapida_client"] = copy.deepcopy(s2)
    # Full field_states mirror
    root_fs = result.get("field_states")
    if isinstance(root_fs, dict):
        cdc["field_states"] = copy.deepcopy(root_fs)


def apply_customer_facing_consistency_sweep(
    result: Dict[str, Any],
    qa_report: Optional[Dict[str, Any]] = None,
) -> None:
    """Purge stale derived projections from customer-facing sections after QA corrections.

    Rule 2: relabel fake buyer-side money_box items; clean semaforo blockers.
    Rule 3: replace ASSENTE agibilità claims when field state is DA VERIFICARE.
    Rule 4: normalize OCCUPATO projections and propagate OCCUPATO to lots.
    Rule 4B: normalize opponibility text when occupancy is known but opponibility is unknown.
    Rule 4C: project severe urbanistica and deduplicate final customer-facing semantic cards.
    Rule 5: deep-sync customer_decision_contract with corrected root sections.
    """
    # Rule 2: buyer-side label cleanup in money_box items
    _relabel_buyer_side_money_box_items(result)
    # Rule 2D: semaforo blocker cleanup
    _clean_semaforo_top_blockers(result)

    # Rule 3: agibilità downgrade propagation
    field_states = result.get("field_states") or {}
    agib = field_states.get("agibilita")
    if isinstance(agib, dict) and str(agib.get("value") or "").upper() == "DA VERIFICARE":
        _propagate_agibilita_downgrade(result)

    # Rule 4: root-state projection cleanup for customer-facing occupancy cards
    _normalize_occupied_field_state(result)

    # Rule 4B: opponibility text cleanup after occupied root state is final
    _normalize_opponibilita_when_occupied(result)

    # Existing lot projection remains before CDC sync.
    _propagate_occupancy_to_lots(result)

    # Rule 4C: remaining root-state projection cleanup
    _propagate_urbanistica_severity(result)
    _dedup_semantic_issue_cards(result)

    # Rule 5: full CDC sync (must be last so it captures all prior corrections)
    _sync_cdc_full_sections(result)

    if qa_report is not None and qa_report.get("status") in ("PASS",):
        qa_report["status"] = "FAIL_CORRECTED"


# ---------------------------------------------------------------------------
# Bad-text scan helper (Rule 6)
# ---------------------------------------------------------------------------

def _collect_customer_facing_bad_text_hits(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Scan customer-facing sections (excluding qa_gate) for known bad text patterns.

    Returns list of {"key": path, "pattern": pattern_name, "text_excerpt": str}.
    """
    field_states = result.get("field_states") or {}

    active_patterns: Dict[str, Any] = dict(_BAD_TEXT_PATTERNS)

    agib = field_states.get("agibilita")
    if isinstance(agib, dict) and str(agib.get("value") or "").upper() == "DA VERIFICARE":
        active_patterns["agibilita_assente_after_downgrade"] = None

    occ = field_states.get("stato_occupativo")
    if isinstance(occ, dict) and str(occ.get("value") or "").upper() == "OCCUPATO":
        active_patterns["stato_non_verificabile_after_occupied"] = re.compile(
            r"Stato occupativo:\s*NON|stato occupativo.*NON_VERIFICABILE", re.I
        )
        active_patterns["occupancy_stale_unresolved_after_occupied"] = None

    urb = field_states.get("regolarita_urbanistica")
    if isinstance(urb, dict) and str(urb.get("value") or "").upper() == "NON CONFORME / GRAVE":
        active_patterns["urbanistica_da_verificare_after_grave"] = re.compile(
            r"Regolarit[àa]\s+urbanistica\s*:\s*DA\s+VERIFICARE|urbanistica\s*:\s*DA\s+VERIFICARE",
            re.I,
        )

    hits: List[Dict[str, Any]] = []

    def _scan_str(text: str, path: str) -> None:
        for name, pat in active_patterns.items():
            if name == "agibilita_assente_after_downgrade":
                m = _find_agibilita_absence_overclaim(text)
            elif name == "occupancy_stale_unresolved_after_occupied":
                m = (
                    _OCCUPANCY_STALE_UNRESOLVED_RE.search(text)
                    if _is_occupancy_status_related_text(text, path)
                    else None
                )
            else:
                m = pat.search(text)
            if m:
                start = max(0, m.start() - 20)
                hits.append({
                    "key": path,
                    "pattern": name,
                    "text_excerpt": text[start: m.end() + 20],
                })

    def _walk(obj: Any, path: str) -> None:
        if isinstance(obj, str):
            _scan_str(obj, path)
        elif isinstance(obj, dict):
            for k, v in obj.items():
                if str(k) == "qa_gate":
                    continue
                _walk(v, f"{path}.{k}")
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                _walk(item, f"{path}[{i}]")

    for key in _CUSTOMER_FACING_SCAN_KEYS:
        val = result.get(key)
        if val is not None:
            _walk(val, key)

    return hits


_EXTENDED_FAKE_PHRASES = list(_FAKE_COST_PHRASES) + [
    "costo buyer-side esplicito",
    "buyer-side esplicito",
]


def _scan_for_fake_total_phrases(result: Dict[str, Any]) -> bool:
    customer_keys = (
        "issues", "summary_for_client", "summary_for_client_bundle",
        "section_2_decisione_rapida", "decision_rapida_client",
        "section_9_legal_killers", "red_flags_operativi", "section_11_red_flags",
        "semaforo_generale", "section_1_semaforo_generale",
        "money_box", "section_3_money_box",
    )
    for key in customer_keys:
        val = result.get(key)
        if val is None:
            continue
        text = json.dumps(val, ensure_ascii=False)
        if any(phrase in text.lower() for phrase in _EXTENDED_FAKE_PHRASES):
            return True

    cdc = result.get("customer_decision_contract")
    if isinstance(cdc, dict):
        return _scan_for_fake_total_phrases(cdc)
    return False


def _sync_cdc_field_states(result: Dict[str, Any], qa_report: Dict[str, Any]) -> None:
    root_fs = result.get("field_states")
    cdc = result.get("customer_decision_contract")
    if not isinstance(root_fs, dict) or not isinstance(cdc, dict):
        return
    cdc_fs = cdc.get("field_states")
    if not isinstance(cdc_fs, dict):
        cdc["field_states"] = {}
        cdc_fs = cdc["field_states"]
    for critical_key in ("stato_occupativo", "opponibilita_occupazione", "agibilita",
                          "regolarita_urbanistica"):
        root_state = root_fs.get(critical_key)
        cdc_state = cdc_fs.get(critical_key)
        if isinstance(root_state, dict) and isinstance(cdc_state, dict):
            if root_state.get("value") != cdc_state.get("value"):
                cdc_fs[critical_key] = copy.deepcopy(root_state)
                _record_safety_correction(
                    qa_report, "INV-4", "MERGE_DUPLICATES",
                    f"CDC field_states.{critical_key} diverged from root — synced."
                )


def _record_safety_correction(
    qa_report: Dict[str, Any],
    inv_id: str,
    action: str,
    reason: str,
) -> None:
    corrections = qa_report.get("corrections_applied")
    if not isinstance(corrections, list):
        corrections = []
        qa_report["corrections_applied"] = corrections
    corrections.append({"id": inv_id, "action": action, "reason": reason, "source": "safety_sweep"})
    if qa_report.get("status") in ("PASS", "WARN", None):
        qa_report["status"] = "FAIL_CORRECTED"


# ---------------------------------------------------------------------------
# Mongo key sanitization
# ---------------------------------------------------------------------------

def _mongo_safe(value: Any) -> Any:
    """Recursively convert all dict keys to strings so BSON encoding never fails."""
    if isinstance(value, dict):
        return {str(k): _mongo_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_mongo_safe(v) for v in value]
    return value


# ---------------------------------------------------------------------------
# Metadata attachment
# ---------------------------------------------------------------------------

def attach_qa_gate_metadata(result: Dict[str, Any], qa_report: Dict[str, Any]) -> None:
    """Write qa_gate dict to result["qa_gate"] with all dict keys stringified for Mongo."""
    result["qa_gate"] = _mongo_safe({
        "version": QA_GATE_VERSION,
        "status": qa_report.get("status", "WARN"),
        "llm_used": qa_report.get("llm_used", False),
        "model": qa_report.get("model", ""),
        "context_mode": qa_report.get("context_mode", ""),
        "pages_reviewed": qa_report.get("pages_reviewed", []),
        "corrections_applied": qa_report.get("corrections_applied", []),
        "contradictions_detected": qa_report.get("contradictions_detected", []),
        "invariants_checked": qa_report.get("invariants_checked", []),
        "semantic_repair_gates": qa_report.get("semantic_repair_gates", {}),
        "section_verdicts": qa_report.get("section_verdicts", {}),
        "errors": qa_report.get("errors", []),
        "context_debug": qa_report.get("context_debug", {}),
    })


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _empty_qa_gate(status: str, reason: str) -> Dict[str, Any]:
    return {
        "status": status,
        "llm_used": False,
        "model": "",
        "context_mode": "",
        "pages_reviewed": [],
        "corrections_applied": [],
        "contradictions_detected": [],
        "invariants_checked": [],
        "section_verdicts": {},
        "errors": [reason] if reason else [],
        "context_debug": {},
    }


def _collect_evidence_pages(result: Dict[str, Any]) -> List[int]:
    pages: List[int] = []

    def _walk(obj: Any) -> None:
        if isinstance(obj, dict):
            if "page" in obj and isinstance(obj["page"], int):
                pages.append(obj["page"])
            for v in obj.values():
                _walk(v)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item)

    _walk(result)
    return pages
