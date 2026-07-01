"""
Deterministic verified_report_contract.json builder for Correctness v2.

Pure function of (worksheet + validator_report + job metadata). NO LLM, NO
randomness, NO old-analyzer fallback. The contract is renderer-ready: a frontend
can render it directly without re-deriving facts. It is only ever built AFTER the
validator returns VALIDATED, so by construction it carries no rejected claims.

Every fact in the contract keeps its ``evidence_pages`` so the renderer can show
provenance and the whole thing stays inspectable.
"""

from __future__ import annotations

import unicodedata
from typing import Any, Dict, List, Optional

CONTRACT_SCHEMA_VERSION = "cv2.contract.v1"

# Money equality tolerance (kept in lockstep with the validator).
_MONEY_ABS_TOL = 1.0
_MONEY_REL_TOL = 0.005


def _approx_equal(a: float, b: float) -> bool:
    tol = max(_MONEY_ABS_TOL, _MONEY_REL_TOL * max(abs(a), abs(b)))
    return abs(a - b) <= tol


def _strip_accents(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c)
    )


def _norm(text: Any) -> str:
    return _strip_accents(str(text or "")).lower().strip()


def _area_token(area: str) -> str:
    """Normalize a compliance/risk area to a canonical token for dedup.

    Generic (never branches on a specific document). Impianto gas vs elettrico
    are kept distinct on purpose so they are not collapsed into one card.
    """
    n = _norm(area)
    if "gas" in n:
        return "impianto_gas"
    if "elettric" in n:
        return "impianto_elettrico"
    if "ediliz" in n:
        return "edilizia"
    if "catast" in n:
        return "catastale"
    if "urbanistic" in n:
        return "urbanistica"
    if "agibil" in n or "abitabil" in n:
        return "agibilita"
    if "impiant" in n:
        return "impianti"
    return n


def _fact(label: str, value: Any, evidence_pages: List[int]) -> Dict[str, Any]:
    return {"label": label, "value": value, "evidence_pages": list(evidence_pages or [])}


def _money_row(label: str, amount: Optional[float], kind: str, evidence_pages: List[int]) -> Dict[str, Any]:
    return {
        "label": label,
        "amount": amount,
        "kind": kind,
        "evidence_pages": list(evidence_pages or []),
    }


def _executive_summary_facts(worksheet: Dict[str, Any]) -> List[Dict[str, Any]]:
    ci = worksheet["case_identity"]
    ev = ci.get("evidence_pages", [])
    facts: List[Dict[str, Any]] = []
    if ci.get("tribunale"):
        facts.append(_fact("Tribunale", ci["tribunale"], ev))
    if ci.get("procedura_rge"):
        facts.append(_fact("Procedura / RGE", ci["procedura_rge"], ev))
    if ci.get("lotto"):
        facts.append(_fact("Lotto", ci["lotto"], ev))
    if ci.get("address"):
        facts.append(_fact("Indirizzo", ci["address"], ev))
    if ci.get("property_type"):
        facts.append(_fact("Tipologia", ci["property_type"], ev))
    if ci.get("ownership_right"):
        facts.append(_fact("Diritto", ci["ownership_right"], ev))

    oc = worksheet["occupancy"]
    if oc.get("status"):
        facts.append(_fact("Stato occupazione", oc["status"], oc.get("evidence_pages", [])))

    money = worksheet["money"]
    if money.get("market_value") is not None:
        facts.append(_fact("Valore di mercato", money["market_value"], money.get("evidence_pages", [])))
    if money.get("sale_value") is not None:
        facts.append(_fact("Valore di vendita giudiziaria", money["sale_value"], money.get("evidence_pages", [])))
    return facts


def _risk_cards(worksheet: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Build deduplicated risk cards.

    The detailed ``technical_compliance`` card for an area is more precise than a
    generic ``risk_classification`` card for the same area, so when both cover the
    same area we keep ONLY the detailed one. Generic risk cards survive only for
    areas not already covered by a detailed compliance card (e.g. occupazione,
    soffitta, stato manutentivo). Areas are matched generically by token, never by
    hardcoded document content.
    """
    cards: List[Dict[str, Any]] = []
    covered_areas: set = set()

    # Detailed (technical_compliance) cards first — these win on conflict.
    for item in worksheet["technical_compliance"]:
        classification = item.get("classification")
        if classification in {"conforming"}:
            continue
        severity = {
            "non_conforming": "grave",
            "not_regularizable": "grave",
            "regularizable": "media",
            "uncertain": "minore",
        }.get(classification, "info")
        cards.append(
            {
                "area": item.get("area"),
                "severity": severity,
                "summary": item.get("notes") or f"{item.get('area')}: {classification}",
                "regularizable": classification == "regularizable",
                "classification": classification,
                "blocks_saleability": bool(item.get("blocks_saleability")),
                "cost": item.get("cost"),
                "timing": item.get("timing"),
                "source": "technical_compliance",
                "evidence_pages": list(item.get("evidence_pages", [])),
            }
        )
        covered_areas.add(_area_token(item.get("area")))

    # Generic risk cards only for areas not already covered by a detailed card.
    for item in worksheet["risk_classification"]:
        token = _area_token(item.get("area"))
        if token in covered_areas:
            continue
        covered_areas.add(token)
        cards.append(
            {
                "area": item.get("area"),
                "severity": item.get("severity"),
                "summary": item.get("summary"),
                "regularizable": bool(item.get("regularizable")),
                "source": "risk_classification",
                "evidence_pages": list(item.get("evidence_pages", [])),
            }
        )

    return cards


def _merge_cost_rows(raw_costs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Collapse cost rows that share the same amount into one evidenced row.

    Generic de-duplication: a deduction breakdown line, the equivalent scalar
    (regularization_costs / cancellation_costs) and a buyer-side line that all
    carry the same amount represent the SAME money and become a single row. The
    label is taken from whichever input row carries the most evidence, evidence
    pages are unioned, and every contributing role is recorded so a single amount
    that is both part of the valuation chain and a buyer-side cost is shown once
    with a clear note instead of as confusing duplicates.
    """
    merged: Dict[float, Dict[str, Any]] = {}
    order: List[float] = []
    for r in raw_costs:
        amount = r["amount"]
        if amount is None or amount == 0:
            continue
        key = round(float(amount), 2)
        ev = list(r.get("evidence_pages") or [])
        if key not in merged:
            merged[key] = {
                "label": r["label"],
                "amount": float(amount),
                "roles": set(),
                "evidence_pages": [],
                "_label_ev": -1,
            }
            order.append(key)
        m = merged[key]
        m["roles"].add(r["role"])
        for p in ev:
            if p not in m["evidence_pages"]:
                m["evidence_pages"].append(p)
        if len(ev) > m["_label_ev"]:
            m["label"] = r["label"]
            m["_label_ev"] = len(ev)

    rows: List[Dict[str, Any]] = []
    for key in order:
        m = merged[key]
        roles = m["roles"]
        chain_roles = roles & {"deduction", "regularization", "cancellation"}
        is_buyer = "buyer_side" in roles
        row = {
            "label": m["label"],
            "amount": m["amount"],
            "kind": "deduction" if chain_roles else "buyer_side",
            "roles": sorted(roles),
            "evidence_pages": m["evidence_pages"],
        }
        if chain_roles and is_buyer:
            row["notes"] = "Anche a carico dell'acquirente."
        rows.append(row)
    return rows


def _uncertain_row(label: str, amount: Optional[float], evidence_pages: List[int], reason: Optional[str]) -> Dict[str, Any]:
    return {
        "label": label,
        "amount": amount,
        "kind": "uncertain",
        "manual_review": True,
        "reason": reason,
        "evidence_pages": list(evidence_pages or []),
    }


def _money_sections(
    worksheet: Dict[str, Any], validator_report: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Build the normalized money view as five clearly separated sections.

    No significant amount is ever dropped:
      * ``valuation_chain``: market value -> deductions/regularization ->
        current-state value -> cancellation deduction -> judicial sale value.
      * ``auction_terms``: prezzo base d'asta (ONLY with explicit text support),
        offerta minima, rialzo minimo, cauzione.
      * ``buyer_side_costs``: costs that are ONLY buyer-side.
      * ``procedure_cancelled_formalities``: ipoteca/pignoramento/etc. cancelled
        by the procedure.
      * ``uncertain_money``: any significant amount whose role is unclear, kept
        with its evidence and flagged for manual review.

    The judicial sale value is ALWAYS shown and is never relabeled as prezzo base.
    Whether a prezzo base candidate has explicit textual support is decided by the
    validator (which has the page text) and read from ``money_signals`` here.
    """
    money = worksheet["money"]
    money_ev = list(money.get("evidence_pages") or [])
    signals = ((validator_report or {}).get("checks") or {}).get("money_signals") or {}
    base_explicit = bool(signals.get("base_price_explicit_text"))

    # Collect every cost-type figure (mergeable by amount).
    raw_costs: List[Dict[str, Any]] = []
    for d in money.get("deductions", []):
        raw_costs.append(
            {
                "role": "deduction",
                "label": d.get("label") or "Deprezzamento",
                "amount": d.get("amount"),
                "evidence_pages": d.get("evidence_pages") or [],
            }
        )
    if money.get("regularization_costs"):
        raw_costs.append(
            {
                "role": "regularization",
                "label": "Costi di regolarizzazione",
                "amount": money["regularization_costs"],
                "evidence_pages": money_ev,
            }
        )
    if money.get("cancellation_costs"):
        raw_costs.append(
            {
                "role": "cancellation",
                "label": "Costi di cancellazione formalità",
                "amount": money["cancellation_costs"],
                "evidence_pages": money_ev,
            }
        )
    for c in money.get("buyer_side_costs", []):
        raw_costs.append(
            {
                "role": "buyer_side",
                "label": c.get("label") or "Costo a carico acquirente",
                "amount": c.get("amount"),
                "evidence_pages": c.get("evidence_pages") or [],
            }
        )

    cost_rows = _merge_cost_rows(raw_costs)

    cancellation_rows = [r for r in cost_rows if "cancellation" in r["roles"]]
    precurrent_rows = [
        r
        for r in cost_rows
        if "cancellation" not in r["roles"]
        and ({"deduction", "regularization"} & set(r["roles"]))
    ]
    buyer_only_rows = [
        r
        for r in cost_rows
        if not ({"deduction", "regularization", "cancellation"} & set(r["roles"]))
    ]

    # Valuation chain (ordered) — prezzo base is NOT part of this chain; the
    # judicial sale value is always present here.
    chain: List[Dict[str, Any]] = []
    if money.get("market_value") is not None:
        chain.append(_money_row("Valore di mercato", money["market_value"], "value", money_ev))
    chain.extend(precurrent_rows)
    if money.get("current_state_value") is not None:
        chain.append(_money_row("Valore nello stato di fatto", money["current_state_value"], "value", money_ev))
    chain.extend(cancellation_rows)
    sale = money.get("sale_value")
    if sale is not None:
        chain.append(_money_row("Valore di vendita giudiziaria", sale, "value", money_ev))

    # Auction terms + uncertain money.
    at = money.get("auction_terms") or {}
    at_ev = list(at.get("evidence_pages") or []) or money_ev
    auction_terms: List[Dict[str, Any]] = []
    uncertain: List[Dict[str, Any]] = []

    for u in money.get("uncertain_money", []):
        if u.get("amount") is None:
            continue
        uncertain.append(
            _uncertain_row(
                u.get("label") or "Importo da verificare",
                u.get("amount"),
                u.get("evidence_pages") or [],
                u.get("reason"),
            )
        )

    # Prezzo base candidate: prefer the structured auction term, fall back to the
    # legacy field. Honor it as prezzo base ONLY with explicit text support.
    base = at.get("prezzo_base_asta")
    if base is None:
        base = money.get("base_auction_value")
    if base is not None:
        if base_explicit:
            auction_terms.append(_money_row("Prezzo base d'asta", base, "auction_term", at_ev))
        else:
            shown = [
                v
                for v in (
                    money.get("market_value"),
                    money.get("current_state_value"),
                    sale,
                )
                if v is not None
            ]
            already_visible = any(_approx_equal(float(base), float(v)) for v in shown)
            if not already_visible:
                # Significant amount with an unclear role -> never dropped.
                uncertain.append(
                    _uncertain_row(
                        "Importo indicato come base/asta (ruolo da verificare)",
                        float(base),
                        at_ev,
                        "Valore senza riferimento testuale esplicito a 'prezzo base'/'base d'asta'.",
                    )
                )

    for field, label in (
        ("offerta_minima", "Offerta minima"),
        ("rialzo_minimo", "Rialzo minimo"),
        ("cauzione", "Cauzione"),
    ):
        amt = at.get(field)
        if amt:  # present and non-zero
            auction_terms.append(_money_row(label, amt, "auction_term", at_ev))

    procedure_cancelled: List[Dict[str, Any]] = []
    for c in money.get("procedure_cancelled_costs", []):
        procedure_cancelled.append(
            _money_row(
                c.get("label") or "Formalità cancellata dalla procedura",
                c.get("amount"),
                "procedure_cancelled",
                c.get("evidence_pages") or [],
            )
        )

    money_table = (
        list(chain)
        + list(auction_terms)
        + list(buyer_only_rows)
        + list(procedure_cancelled)
        + list(uncertain)
    )

    return {
        "valuation_chain": chain,
        "auction_terms": auction_terms,
        "buyer_side_costs": buyer_only_rows,
        "procedure_cancelled_formalities": procedure_cancelled,
        "uncertain_money": uncertain,
        "money_table": money_table,
        "needs_manual_review_money": bool(uncertain),
    }


def _buyer_action_checklist(worksheet: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Actionable items the buyer should consider — only from supported facts."""
    checklist: List[Dict[str, Any]] = []

    for item in worksheet["technical_compliance"]:
        if item.get("classification") in {"regularizable", "non_conforming", "not_regularizable", "uncertain"}:
            parts = [f"{item.get('area')}: {item.get('classification')}"]
            if item.get("cost") is not None:
                parts.append(f"costo stimato {item.get('cost')}")
            if item.get("timing"):
                parts.append(f"tempistica {item.get('timing')}")
            checklist.append(
                {
                    "action": "Verificare/regolarizzare conformità",
                    "detail": "; ".join(parts),
                    "blocks_saleability": bool(item.get("blocks_saleability")),
                    "evidence_pages": list(item.get("evidence_pages", [])),
                }
            )

    oc = worksheet["occupancy"]
    for risk in oc.get("risks", []):
        checklist.append(
            {
                "action": "Valutare rischio occupazione",
                "detail": risk,
                "evidence_pages": list(oc.get("evidence_pages", [])),
            }
        )

    for c in worksheet["money"].get("buyer_side_costs", []):
        amount = c.get("amount")
        # No buyer action for a zero/absent amount — it is not a meaningful cost.
        if amount is None or amount == 0:
            continue
        checklist.append(
            {
                "action": "Considerare costo a carico acquirente",
                "detail": f"{c.get('label')}: {amount}",
                "evidence_pages": list(c.get("evidence_pages", [])),
            }
        )
    return checklist


def _evidence_index(worksheet: Dict[str, Any]) -> Dict[str, List[str]]:
    """Map page_number(str) -> list of claim paths citing it (renderer provenance)."""
    index: Dict[str, List[str]] = {}

    def add(pages: Any, path: str) -> None:
        for p in pages or []:
            index.setdefault(str(p), []).append(path)

    add(worksheet["case_identity"].get("evidence_pages"), "case_identity")
    add(worksheet["occupancy"].get("evidence_pages"), "occupancy")
    add(worksheet["money"].get("evidence_pages"), "money")
    for i, item in enumerate(worksheet["technical_compliance"]):
        add(item.get("evidence_pages"), f"technical_compliance[{i}]:{item.get('area')}")
    for i, item in enumerate(worksheet["legal_formalities"]):
        add(item.get("evidence_pages"), f"legal_formalities[{i}]:{item.get('type')}")
    for i, item in enumerate(worksheet["risk_classification"]):
        add(item.get("evidence_pages"), f"risk_classification[{i}]:{item.get('area')}")
    # Sort page keys numerically for deterministic output.
    return {k: index[k] for k in sorted(index, key=lambda s: int(s) if s.isdigit() else 1_000_000)}


def _legal_formalities_view(worksheet: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for item in worksheet["legal_formalities"]:
        out.append(
            {
                "type": item.get("type"),
                "description": item.get("description"),
                "cancelled_by_procedure": bool(item.get("cancelled_by_procedure")),
                "buyer_burden": bool(item.get("buyer_burden")),
                "amount": item.get("amount"),
                "evidence_pages": list(item.get("evidence_pages", [])),
            }
        )
    return out


def _uncertainty_flags(worksheet: Dict[str, Any], validator_report: Dict[str, Any]) -> List[Dict[str, Any]]:
    flags: List[Dict[str, Any]] = []
    for text in worksheet.get("missing_or_uncertain", []):
        flags.append({"kind": "missing_or_uncertain", "detail": text})
    for w in worksheet.get("warnings", []):
        flags.append({"kind": "analyst_warning", "detail": w.get("text"), "evidence_pages": w.get("evidence_pages", [])})
    for w in validator_report.get("warnings", []):
        flags.append({"kind": "validator_warning", "detail": w.get("detail"), "code": w.get("code")})
    return flags


def _lot_summary(lot_report: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Single-lot contract lot block: always states the lot situation explicitly.

    The contract is only ever built on the single-lot path (the orchestrator
    diverts multi-lot documents to manual review), so this records the selected
    lot and asserts no manual review is required. ``bene`` counts are carried for
    transparency: several beni inside ONE lot are normal and never a contamination.
    """
    lot_report = lot_report or {}
    lot_ids = lot_report.get("lot_ids", [])
    return {
        "multi_lot": False,
        "lot_count": lot_report.get("lot_count", len(lot_ids)),
        "selected_lot": lot_ids[0] if lot_ids else None,
        "bene_count": lot_report.get("bene_count", 0),
        "multi_bene": bool(lot_report.get("multi_bene")),
        "manual_review_required": False,
    }


_AUCTION_TERM_FIELDS = {
    "prezzo_base_asta": "Prezzo base d'asta",
    "offerta_minima": "Offerta minima",
    "rialzo_minimo": "Rialzo minimo",
    "cauzione": "Cauzione",
}


def _merge_shared_summary_rows(
    money: Dict[str, Any], shared_summary_rows: List[Dict[str, Any]]
) -> None:
    """Merge the selected lot's deterministic shared-summary rows into the money view.

    These rows come from :func:`lot_packets.project_shared_summary_rows`: money
    lines on shared multi-lot pages that are clearly tagged with THIS lot's id
    (e.g. "LOTTO 1 - PREZZO BASE D'ASTA: € 64.198,00"). They are document text
    read deterministically — not model claims — so they carry their own explicit
    textual support. A row is only added when no approximately-equal amount is
    already visible in the same section, so nothing is double-listed.
    """
    for row in shared_summary_rows or []:
        amount = row.get("amount")
        if amount is None:
            continue
        field = row.get("field")
        out_row = {
            "label": row.get("label") or "Importo da tabella riassuntiva",
            "amount": float(amount),
            "kind": "auction_term" if field in _AUCTION_TERM_FIELDS else "lot_summary_value",
            "source": "shared_summary_projection",
            "evidence_pages": list(row.get("evidence_pages") or []),
        }
        if field in _AUCTION_TERM_FIELDS:
            section = money["auction_terms"]
        else:
            section = money["valuation_chain"]
        already = any(
            r.get("amount") is not None and _approx_equal(float(r["amount"]), float(amount))
            for r in section
        )
        if already:
            continue
        section.append(out_row)
        money["money_table"].append(out_row)


def build_contract(
    *,
    worksheet: Dict[str, Any],
    validator_report: Dict[str, Any],
    analysis_id: str,
    job_id: str,
    source_pdf_quality_status: str,
    lot_report: Optional[Dict[str, Any]] = None,
    shared_summary_rows: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Build the deterministic, renderer-ready verified report contract."""
    money = _money_sections(worksheet, validator_report)
    _merge_shared_summary_rows(money, shared_summary_rows or [])
    return {
        "schema_version": CONTRACT_SCHEMA_VERSION,
        "analysis_id": analysis_id,
        "job_id": job_id,
        "source_pdf_quality_status": source_pdf_quality_status,
        "lot_summary": _lot_summary(lot_report),
        "case_identity": dict(worksheet["case_identity"]),
        "executive_summary_facts": _executive_summary_facts(worksheet),
        "risk_cards": _risk_cards(worksheet),
        "money_table": money["money_table"],
        "valuation_chain": money["valuation_chain"],
        "auction_terms": money["auction_terms"],
        "buyer_side_costs": money["buyer_side_costs"],
        "procedure_cancelled_formalities": money["procedure_cancelled_formalities"],
        "uncertain_money": money["uncertain_money"],
        "needs_manual_review_money": money["needs_manual_review_money"],
        # Raw lot-tagged rows projected from shared multi-lot summary pages
        # (deterministic; this lot only). Kept verbatim for provenance.
        "shared_summary_money": [dict(r) for r in shared_summary_rows or []],
        "legal_formalities": _legal_formalities_view(worksheet),
        "buyer_action_checklist": _buyer_action_checklist(worksheet),
        "evidence_index": _evidence_index(worksheet),
        "validation_status": validator_report.get("validation_status"),
        "uncertainty_flags": _uncertainty_flags(worksheet, validator_report),
    }
