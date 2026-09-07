"""Sanitized synthetic multi-lot *fresh-case* fixture — STRUCTURE ONLY.

Reproduces the production shape that over-blocked (coverage-only hard block):

  * multi-lot case (lots "1" and "2");
  * every DECISIVE monetary value present and correctly role-labelled in the
    customer report (market value / judicial sale value / auction base price);
  * many legitimately-"parziale" subordinate/detail money rows whose CONCLUSIVE
    value is already in the report -> they depress the covered-ratio below 0.60;
  * optionally one NON-critical money-role caution (a deprezzamento amount not
    exposed) -> an ``important_warning``, money-integrity-style, never the ratio
    driver;
  * ZERO hard blocking issues, ZERO critical omissions, customer report fully
    generated (internally REPORT_READY), APPROVE_WITH_WARNINGS.

NO real address / filename / cadastral / production-id / user-identity /
verbatim customer text: every label and amount below is synthetic and
case-agnostic. Nothing here is hard-coded into the runtime pipeline.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from correctness_v2 import doc_signals, fact_lineage

# Distinct synthetic amounts per lot so the report pool never cross-matches.
_DECISIVE = {
    "1": {"market_value": 250000.0, "judicial_sale_value": 180000.0, "base_price": 144000.0},
    "2": {"market_value": 320000.0, "judicial_sale_value": 240000.0, "base_price": 192000.0},
}

# Seven subordinate/detail amounts per lot (component/intermediate values whose
# conclusive figure is already the decisive value above). Absent from the
# report on purpose: honest "parziale" detail, never a silent loss.
_DETAIL = {
    "1": [5100.0, 5200.0, 5300.0, 5400.0, 5500.0, 5600.0, 5700.0],
    "2": [6100.0, 6200.0, 6300.0, 6400.0, 6500.0, 6600.0, 6700.0],
}

# A single non-critical "deprezzamento/riduzione" caution amount per lot.
_CAUTION = {"1": 9100.0, "2": 9200.0}


def _money_section_row(label: str, amount: float, pages: List[int]) -> Dict[str, Any]:
    return {"label": label, "amount": amount, "evidence_pages": pages}


def build_customer_report(lot_id: str, *, high_coverage: bool = False) -> Dict[str, Any]:
    """A fully-rendered (internally READY) customer report for one lot.

    ``high_coverage`` additionally renders the detail rows (used to model a
    CLEAN sibling lot whose covered-ratio stays high).
    """
    dec = _DECISIVE[lot_id]
    valuation = [
        _money_section_row("Valore di mercato", dec["market_value"], [3]),
        _money_section_row("Valore di vendita giudiziaria", dec["judicial_sale_value"], [4]),
    ]
    auction = [
        _money_section_row("Prezzo base d'asta", dec["base_price"], [5]),
    ]
    if high_coverage:
        for i, amt in enumerate(_DETAIL[lot_id]):
            valuation.append(
                _money_section_row(f"Componente di valore {i + 1}", amt, [6])
            )
    return {
        "schema_version": "cv2.customer_report.v1",
        "report_status": "REPORT_READY",
        "title": f"Perizia sintetica — Lotto {lot_id}",
        "lot_structure": {"selected_lot": lot_id, "lot_ids": [lot_id]},
        "executive_summary": [
            {"text": f"Lotto {lot_id}: immobile valutato per la vendita giudiziaria."}
        ],
        "key_facts": [
            {"label": "Valore di mercato", "value": str(dec["market_value"]),
             "evidence_pages": [3]},
        ],
        "money_sections": {
            "valuation_chain": valuation,
            "auction_terms": auction,
        },
    }


def _ledger_fact(
    fact_id: str, amount: float, role: str, severity: str, lot_id: str,
    pages: List[int],
) -> Dict[str, Any]:
    return {
        "fact_id": fact_id,
        "category": "money",
        "severity": severity,
        "value": None,
        "money": {"amount": amount, "role": role},
        "evidence_pages": pages,
        "applicability": fact_lineage.LOT_SPECIFIC,
        "applicability_lot_ids": [lot_id],
    }


def build_case_ledger(lot_id: str) -> Dict[str, Any]:
    """A case ledger whose CRITICAL facts are all present in the report and whose
    many NON-critical detail facts are absent (depressing the covered-ratio)."""
    dec = _DECISIVE[lot_id]
    facts: List[Dict[str, Any]] = [
        _ledger_fact(f"lot{lot_id}.market", dec["market_value"],
                     doc_signals.ROLE_MARKET_VALUE, doc_signals.SEV_CRITICAL, lot_id, [3]),
        _ledger_fact(f"lot{lot_id}.judicial", dec["judicial_sale_value"],
                     doc_signals.ROLE_JUDICIAL_SALE_VALUE, doc_signals.SEV_CRITICAL, lot_id, [4]),
        _ledger_fact(f"lot{lot_id}.base", dec["base_price"],
                     doc_signals.ROLE_AUCTION_BASE_PRICE, doc_signals.SEV_CRITICAL, lot_id, [5]),
    ]
    for i, amt in enumerate(_DETAIL[lot_id]):
        facts.append(
            _ledger_fact(f"lot{lot_id}.detail{i}", amt, doc_signals.ROLE_DEPRECIATION,
                         doc_signals.SEV_USEFUL, lot_id, [6])
        )
    return {"facts": facts}


def build_worksheet(lot_id: str, *, with_caution: bool = False) -> Dict[str, Any]:
    """A reconciled worksheet whose decisive money is matched by the report.

    ``with_caution`` adds a single non-critical deprezzamento deduction absent
    from the report -> surfaces as an ``important_warning`` (a money-role
    caution), never a blocking issue and never the ratio driver.
    """
    dec = _DECISIVE[lot_id]
    money: Dict[str, Any] = {
        "evidence_pages": [3],
        "market_value": dec["market_value"],
        "sale_value": dec["judicial_sale_value"],
        "auction_terms": {"evidence_pages": [5], "prezzo_base_asta": dec["base_price"]},
    }
    if with_caution:
        money["deductions"] = [
            {"label": "Riduzione per stato manutentivo", "amount": _CAUTION[lot_id],
             "evidence_pages": [7]}
        ]
    return {
        "case_identity": {"lotto": lot_id, "evidence_pages": [1]},
        "lots": [{"lot_id": lot_id, "label": f"Lotto {lot_id}", "evidence_pages": [1]}],
        "money": money,
    }


def build_inputs(
    lot_id: str = "1", *, with_caution: bool = False, high_coverage: bool = False
) -> Dict[str, Any]:
    """All kwargs for ``coverage_audit.build_coverage_audit`` / the quality gate.

    ``pages`` is intentionally empty: the audit's page-signal channel adds no
    critical omissions here — the coverage pressure comes purely from the
    ledger's subordinate/detail facts, exactly like the production case.
    """
    return {
        "analysis_id": "fresh_case_synth",
        "job_id": f"fresh_case_job_lot_{lot_id}",
        "pages": [],
        "worksheet": build_worksheet(lot_id, with_caution=with_caution),
        "contract": {"schema_version": "cv2.contract.v1", "valuation_chain": []},
        "customer_report": build_customer_report(lot_id, high_coverage=high_coverage),
        # The auction base price is explicitly supported in the source (as in a
        # real perizia) so the FAKE_PREZZO_BASE hard-block is not tripped: this
        # fixture models an over-block driven ONLY by non-critical coverage.
        "validator_report": {
            "validation_status": "OK",
            "warning_count": 0,
            "checks": {"money_signals": {"base_price_explicit_text": True}},
        },
        "full_document_pages": [],
        "lot_id": lot_id,
        "segmentation": {},
        "case_ledger": build_case_ledger(lot_id),
        "lot_fact_projection_report": {},
        "selected_analysis_pages": [3, 4, 5],
    }
