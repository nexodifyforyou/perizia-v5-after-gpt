"""
Generic synthetic perizia fixtures for Step 2 tests.

Deliberately NOT a real document (uses Tribunale di Esempio) so nothing here is
overfit to Torino/Rome/Ostuni. Provides:
  * GENERIC_PERIZIA_PAGES — passes the PDF-quality gate and carries the tokens
    the validator inspects (conforme / a carico dell'aggiudicatario / etc).
  * base_raw_worksheet() — a model-style raw worksheet that validates clean.
  * fake callers for the OpenAI seam.
"""

from __future__ import annotations

import copy
import json
import re
from typing import Any, Dict, List

# Two-page generic perizia: all 6 key sections present, money on both pages,
# consistent internal pagination -> PDF_QUALITY_OK.
GENERIC_PERIZIA_PAGES: List[Dict[str, Any]] = [
    {
        "page_number": 1,
        "text": (
            "TRIBUNALE DI ESEMPIO - Procedura esecutiva R.G.E. 123/2024 - LOTTO 1. "
            "Identificazione dei beni e oggetto della vendita: appartamento ad uso "
            "abitativo con pertinenze. Stato di possesso: l'immobile risulta libero "
            "da persone e cose. Sotto il profilo urbanistico l'immobile risulta "
            "urbanisticamente conforme agli strumenti vigenti. Prezzo base indicato "
            "negli atti della procedura esecutiva: EUR 100.000,00. Pagina 1 di 2."
        ),
    },
    {
        "page_number": 2,
        "text": (
            "Vincoli e oneri: risultano iscritta ipoteca e trascritto pignoramento, "
            "formalità che saranno cancellate dalla procedura con il decreto di "
            "trasferimento e non restano a carico dell'aggiudicatario. Giudizio di "
            "conformità: la difformità edilizia è regolarizzabile con pratica in "
            "sanatoria; la conformità catastale è regolarizzabile. Valutazione e "
            "stima: valore di mercato EUR 100.000,00. Costi di regolarizzazione "
            "stimati in EUR 5.000,00, valore nello stato di fatto EUR 95.000,00. "
            "Costi di cancellazione delle formalità EUR 300,00; valore di vendita "
            "giudiziaria EUR 94.700,00. Pagina 2 di 2."
        ),
    },
]


def base_raw_worksheet() -> Dict[str, Any]:
    """A model-style raw worksheet (pre-normalization) that validates clean."""
    return {
        "case_identity": {
            "tribunale": "Tribunale di Esempio",
            "procedura_rge": "R.G.E. 123/2024",
            "lotto": "1",
            "address": "Via Esempio 1",
            "property_type": "Appartamento",
            "ownership_right": "Piena proprietà",
            "evidence_pages": [1],
        },
        "occupancy": {
            "status": "Libero",
            "title_info": None,
            "opponibility": None,
            "registration_dates": [],
            "expiry_dates": [],
            "risks": [],
            "evidence_pages": [1],
        },
        "technical_compliance": [
            {
                "area": "urbanistica",
                "classification": "conforming",
                "blocks_saleability": False,
                "cost": None,
                "timing": None,
                "notes": "Immobile urbanisticamente conforme.",
                "evidence_pages": [1],
            },
            {
                "area": "edilizia",
                "classification": "regularizable",
                "blocks_saleability": False,
                "cost": 5000.0,
                "timing": "60 giorni",
                "notes": "Difformità edilizia regolarizzabile in sanatoria.",
                "evidence_pages": [2],
            },
            {
                "area": "catastale",
                "classification": "regularizable",
                "blocks_saleability": False,
                "cost": None,
                "timing": None,
                "notes": "Conformità catastale regolarizzabile.",
                "evidence_pages": [2],
            },
        ],
        "money": {
            "market_value": 100000.0,
            "deductions": [],
            "regularization_costs": 5000.0,
            "current_state_value": 95000.0,
            "auction_terms": {
                "prezzo_base_asta": 75000.0,
                "offerta_minima": 56250.0,
                "rialzo_minimo": 1000.0,
                "cauzione": 7500.0,
                "evidence_pages": [1],
            },
            "buyer_side_costs": [],
            "procedure_cancelled_costs": [
                {"label": "Cancellazione ipoteca e pignoramento", "amount": 300.0, "evidence_pages": [2]}
            ],
            "uncertain_money": [],
            "cancellation_costs": 300.0,
            "sale_value": 94700.0,
            "evidence_pages": [2],
        },
        "legal_formalities": [
            {
                "type": "ipoteca",
                "description": "Ipoteca iscritta",
                "cancelled_by_procedure": True,
                "buyer_burden": False,
                "amount": None,
                "evidence_pages": [2],
            },
            {
                "type": "pignoramento",
                "description": "Pignoramento trascritto",
                "cancelled_by_procedure": True,
                "buyer_burden": False,
                "amount": None,
                "evidence_pages": [2],
            },
        ],
        "risk_classification": [
            {
                "area": "edilizia",
                "severity": "media",
                "summary": "Difformità edilizia regolarizzabile.",
                "regularizable": True,
                "evidence_pages": [2],
            }
        ],
        "warnings": [],
        "missing_or_uncertain": [],
    }


def make_worksheet(**overrides) -> Dict[str, Any]:
    """Deep-copy the base raw worksheet and shallow-override top-level sections."""
    ws = base_raw_worksheet()
    for key, value in overrides.items():
        ws[key] = value
    return copy.deepcopy(ws)


def make_multilot_worksheet() -> Dict[str, Any]:
    """A raw worksheet that blends TWO distinct lots into its flat fields."""
    ws = make_worksheet()
    ws["case_identity"]["lotto"] = "Lotti 1 e 2"
    ws["case_identity"]["address"] = "Lotto 1: Via Uno 1; Lotto 2: Via Due 2"
    ws["technical_compliance"] = [
        {
            "area": "Lotto 1 - regolarità edilizia",
            "classification": "regularizable",
            "blocks_saleability": False,
            "cost": 1000.0,
            "timing": "30 giorni",
            "notes": "Difformità del Lotto 1.",
            "evidence_pages": [2],
        },
        {
            "area": "Lotto 2 - regolarità edilizia",
            "classification": "regularizable",
            "blocks_saleability": False,
            "cost": 2000.0,
            "timing": "30 giorni",
            "notes": "Difformità del Lotto 2.",
            "evidence_pages": [2],
        },
    ]
    return ws


def make_multibene_single_lot_worksheet() -> Dict[str, Any]:
    """A SINGLE-lot worksheet whose lot contains several beni (apartment + box).

    Multiple beni inside one lot are normal and must NOT be treated as multi-lot.
    """
    ws = make_worksheet()
    ws["technical_compliance"] = [
        {
            "area": "Bene 1 - regolarità edilizia",
            "classification": "regularizable",
            "blocks_saleability": False,
            "cost": 1000.0,
            "timing": "30 giorni",
            "notes": "Bene 1 (appartamento).",
            "evidence_pages": [2],
        },
        {
            "area": "Bene 2 - regolarità edilizia",
            "classification": "regularizable",
            "blocks_saleability": False,
            "cost": 500.0,
            "timing": "30 giorni",
            "notes": "Bene 2 (box auto) dello stesso lotto.",
            "evidence_pages": [2],
        },
    ]
    return ws


# ---------------------------------------------------------------------------
# Multi-lot, page-segmentable fixture (each lot lives on its own pages)
# ---------------------------------------------------------------------------
# One self-contained lot block carrying ALL the tokens/numbers the validator needs
# (identity, possesso, vincoli/oneri cancellate, conformità, valutazione, prezzo
# base). Built generically from the single-lot GENERIC content — NOT a real city.
_LOT_BLOCK = GENERIC_PERIZIA_PAGES[0]["text"] + " " + GENERIC_PERIZIA_PAGES[1]["text"]

# 5 pages: a global preamble, then two pages per lot so each lot id appears >=2
# times (page-text detection) and segmentation gets clean per-lot anchors.
MULTI_LOT_PAGES: List[Dict[str, Any]] = [
    {
        "page_number": 1,
        "text": (
            "TRIBUNALE DI ESEMPIO - Procedura esecutiva. Premessa, metodologia di "
            "stima e criteri generali. Identificazione dei beni, possesso, vincoli e "
            "oneri, conformita, valutazione e costi sono dettagliati nei singoli lotti."
        ),
    },
    {"page_number": 2, "text": _LOT_BLOCK},  # LOTTO 1 (explicit, full content)
    {"page_number": 3, "text": "Segue la descrizione del LOTTO 1 e relativi allegati."},
    {"page_number": 4, "text": _LOT_BLOCK.replace("LOTTO 1", "LOTTO 2")},  # LOTTO 2 full
    {"page_number": 5, "text": "Segue la descrizione del LOTTO 2 e relativi allegati."},
]


def _remap_evidence(obj: Any, page: int) -> Any:
    if isinstance(obj, dict):
        return {
            k: ([page] if k == "evidence_pages" else _remap_evidence(v, page))
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [_remap_evidence(x, page) for x in obj]
    return obj


def single_lot_worksheet_on_page(page: int, lot_id: str = "1") -> Dict[str, Any]:
    """A clean SINGLE-lot raw worksheet whose every evidence_pages points at ``page``.

    Used as the re-analysis result for a selected lot: it cites only pages inside
    the lot's isolated page subset, so it validates against that subset.
    """
    ws = base_raw_worksheet()
    ws["case_identity"]["lotto"] = lot_id
    ws["case_identity"]["address"] = f"Via del Lotto {lot_id}"
    ws = _remap_evidence(ws, page)
    return ws


# ---------------------------------------------------------------------------
# OpenAI seam fakes
# ---------------------------------------------------------------------------
def fake_caller_returning(worksheet_raw: Dict[str, Any]):
    """Return an openai_caller that yields the given raw worksheet as JSON."""

    calls: List[Dict[str, Any]] = []

    def _caller(messages, *, model=None, timeout=None):
        calls.append({"model": model, "messages": messages})
        return {
            "content": json.dumps(worksheet_raw, ensure_ascii=False),
            "model": model or "fake-model",
            "finish_reason": "stop",
            "usage": {"total_tokens": 1},
            "response_id": "resp_fake",
        }

    _caller.calls = calls  # type: ignore[attr-defined]
    return _caller


def fake_caller_raising(reason_code: str = "OPENAI_CALL_FAILED"):
    """Return an openai_caller that raises OpenAIClientError (records calls)."""
    from correctness_v2.openai_client import OpenAIClientError

    calls: List[Dict[str, Any]] = []

    def _caller(messages, *, model=None, timeout=None):
        calls.append({"model": model})
        raise OpenAIClientError("simulated OpenAI failure", reason_code=reason_code)

    _caller.calls = calls  # type: ignore[attr-defined]
    return _caller


def fake_sequence_caller(worksheets_raw: List[Dict[str, Any]]):
    """A caller that returns each given raw worksheet in order across calls.

    Records the pages it was asked to analyze so a test can assert that a selected
    lot re-analysis only saw that lot's isolated page subset.
    """
    calls: List[Dict[str, Any]] = []
    seq = list(worksheets_raw)

    def _caller(messages, *, model=None, timeout=None):
        idx = len(calls)
        ws = seq[idx] if idx < len(seq) else seq[-1]
        # The user message embeds the page blocks ("=== PAGINA n ===").
        user = next((m.get("content", "") for m in messages if m.get("role") == "user"), "")
        pages_seen = [int(n) for n in re.findall(r"=== PAGINA (\d+) ===", user)]
        calls.append({"model": model, "pages_seen": pages_seen, "user_text": user})
        return {
            "content": json.dumps(ws, ensure_ascii=False),
            "model": model or "fake-model",
            "finish_reason": "stop",
            "usage": {"total_tokens": 1},
            "response_id": f"resp_fake_{idx}",
        }

    _caller.calls = calls  # type: ignore[attr-defined]
    return _caller


def recording_caller():
    """A caller that must never be invoked (asserted via .calls)."""
    calls: List[Dict[str, Any]] = []

    def _caller(messages, *, model=None, timeout=None):
        calls.append({"model": model})
        return {"content": "{}", "model": model or "fake"}

    _caller.calls = calls  # type: ignore[attr-defined]
    return _caller
