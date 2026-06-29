"""
Deterministic, rule-based validator for the Correctness v2 analyst worksheet.

This is the grounding gate between the (LLM-produced) analyst worksheet and the
renderer-ready contract. It is GENERIC and never branches on a specific perizia.
It uses ONLY the worksheet plus the extracted page text as evidence.

If it finds any hard violation, validation_status is ``VALIDATION_FAILED`` and the
orchestrator MUST NOT build a customer-facing contract (fail closed).

Rules enforced (see task spec):
  1. Important claims carry page evidence; cited pages must exist.
  2. Money chains are internally consistent where enough data is present.
  3. Buyer-side costs are separated from procedure-cancelled formalities.
  4. Ipoteca / pignoramento are not auto-treated as buyer-side costs.
  5. A 'conforming' area cannot be promoted to grave/non-conforming without
     stronger contradictory evidence in the cited text.
  6. A 'regularizable' issue with cost/timing must not be marked as blocking
     saleability.
  7. Unsupported claims (evidence pages that don't exist) are rejected.
  8. Contradictions => VALIDATION_FAILED.
"""

from __future__ import annotations

import unicodedata
from typing import Any, Dict, List, Optional, Tuple

from . import lots as lots_mod
from .analyst import CANCELLABLE_FORMALITY_TYPES

VALIDATOR_SCHEMA_VERSION = "cv2.validator.v1"

STATUS_VALIDATED = "VALIDATED"
STATUS_FAILED = "VALIDATION_FAILED"

# Relative tolerance for money-chain equality checks.
MONEY_ABS_TOLERANCE = 1.0
MONEY_REL_TOLERANCE = 0.005

# Text markers (accent-insensitive, lowercased).
_NEGATIVE_TOKENS = [
    "non conform",
    "difform",
    "abus",            # abuso / abusivo / abusiva
    "illecit",
    "insanabil",
    "non sanabil",
    "non regolarizzabil",
    "irregolarit",
]
_BUYER_BURDEN_TOKENS = [
    "a carico dell'aggiudicatario",
    "a carico dell aggiudicatario",
    "a carico dell'acquirente",
    "a carico dell acquirente",
    "a carico del compratore",
    "a carico dell'aggiudicatar",
    "restano a carico dell",
    "carico dell'aggiudicatario",
]
_CANCELLABLE_LABEL_TOKENS = ["ipotec", "pignorament", "sequestr"]

# Canonical compliance-area anchors used to match risks to compliance entries.
_AREA_ANCHORS = {
    "urbanistic": "urbanistica",
    "ediliz": "edilizia",
    "catast": "catastale",
    "impiant": "impianti",
    "agibil": "agibilita",
    "abitabil": "agibilita",
}


def _strip_accents(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c)
    )


def _norm(text: Any) -> str:
    return _strip_accents(str(text or "")).lower()


def _area_token(area: str) -> str:
    n = _norm(area)
    for anchor, token in _AREA_ANCHORS.items():
        if anchor in n:
            return token
    return n.strip()


def _has_negative(text_norm: str) -> bool:
    return any(tok in text_norm for tok in _NEGATIVE_TOKENS)


def _has_buyer_burden(text_norm: str) -> bool:
    return any(tok in text_norm for tok in _BUYER_BURDEN_TOKENS)


def _approx_equal(a: float, b: float) -> bool:
    tol = max(MONEY_ABS_TOLERANCE, MONEY_REL_TOLERANCE * max(abs(a), abs(b)))
    return abs(a - b) <= tol


class _Report:
    def __init__(self) -> None:
        self.violations: List[Dict[str, Any]] = []
        self.warnings: List[Dict[str, Any]] = []
        self.checks: Dict[str, Any] = {}

    def error(self, code: str, path: str, detail: str) -> None:
        self.violations.append(
            {"code": code, "severity": "error", "path": path, "detail": detail}
        )

    def warn(self, code: str, path: str, detail: str) -> None:
        self.warnings.append(
            {"code": code, "severity": "warning", "path": path, "detail": detail}
        )


def _page_text_index(pages: List[Dict[str, Any]]) -> Dict[int, str]:
    index: Dict[int, str] = {}
    for i, entry in enumerate(pages or [], start=1):
        if not isinstance(entry, dict):
            entry = {"page_number": i, "text": str(entry or "")}
        try:
            num = int(entry.get("page_number", i))
        except Exception:
            num = i
        index[num] = _norm(entry.get("text"))
    return index


def _evidence_text(evidence_pages: List[int], page_index: Dict[int, str]) -> str:
    return "\n".join(page_index.get(p, "") for p in (evidence_pages or []))


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------
def _check_evidence_pages_exist(
    worksheet: Dict[str, Any], valid_pages: set, report: _Report
) -> None:
    """Every cited evidence page must exist (rule 1/7)."""

    def _scan(pages: Any, path: str) -> None:
        for p in pages or []:
            if p not in valid_pages:
                report.error(
                    "UNSUPPORTED_EVIDENCE_PAGE",
                    path,
                    f"evidence page {p} does not exist (valid pages: 1..{max(valid_pages) if valid_pages else 0})",
                )

    _scan(worksheet["case_identity"].get("evidence_pages"), "case_identity.evidence_pages")
    _scan(worksheet["occupancy"].get("evidence_pages"), "occupancy.evidence_pages")
    _scan(worksheet["money"].get("evidence_pages"), "money.evidence_pages")
    for i, item in enumerate(worksheet["technical_compliance"]):
        _scan(item.get("evidence_pages"), f"technical_compliance[{i}].evidence_pages")
    for i, item in enumerate(worksheet["legal_formalities"]):
        _scan(item.get("evidence_pages"), f"legal_formalities[{i}].evidence_pages")
    for i, item in enumerate(worksheet["risk_classification"]):
        _scan(item.get("evidence_pages"), f"risk_classification[{i}].evidence_pages")
    for coll in ("buyer_side_costs", "procedure_cancelled_costs"):
        for i, item in enumerate(worksheet["money"].get(coll, [])):
            _scan(item.get("evidence_pages"), f"money.{coll}[{i}].evidence_pages")


def _check_important_claims_have_evidence(worksheet: Dict[str, Any], report: _Report) -> None:
    """Important claims must carry page evidence (rule 1)."""
    for i, item in enumerate(worksheet["technical_compliance"]):
        if not item.get("evidence_pages"):
            report.error(
                "MISSING_EVIDENCE",
                f"technical_compliance[{i}]",
                f"compliance area '{item.get('area')}' has no evidence_pages",
            )
    for i, item in enumerate(worksheet["legal_formalities"]):
        if not item.get("evidence_pages"):
            report.error(
                "MISSING_EVIDENCE",
                f"legal_formalities[{i}]",
                f"formality '{item.get('type')}' has no evidence_pages",
            )
    for i, item in enumerate(worksheet["risk_classification"]):
        if item.get("severity") in {"grave", "media"} and not item.get("evidence_pages"):
            report.error(
                "MISSING_EVIDENCE",
                f"risk_classification[{i}]",
                f"risk '{item.get('area')}' severity={item.get('severity')} has no evidence_pages",
            )
    money = worksheet["money"]
    has_money_value = any(
        money.get(k) is not None
        for k in ("market_value", "base_auction_value", "current_state_value", "sale_value", "regularization_costs", "cancellation_costs")
    )
    if has_money_value and not money.get("evidence_pages"):
        report.error("MISSING_EVIDENCE", "money", "money values present but money.evidence_pages is empty")

    # Softer claims -> warnings only.
    ci = worksheet["case_identity"]
    if any(ci.get(k) for k in ("tribunale", "procedura_rge", "address")) and not ci.get("evidence_pages"):
        report.warn("MISSING_EVIDENCE_SOFT", "case_identity", "case_identity has values but no evidence_pages")
    oc = worksheet["occupancy"]
    if oc.get("status") and not oc.get("evidence_pages"):
        report.warn("MISSING_EVIDENCE_SOFT", "occupancy", "occupancy status present but no evidence_pages")


def _check_money_chains(worksheet: Dict[str, Any], report: _Report) -> None:
    """Money values internally consistent where enough data is present (rule 2)."""
    money = worksheet["money"]
    market = money.get("market_value")
    regularization = money.get("regularization_costs")
    current = money.get("current_state_value")
    cancellation = money.get("cancellation_costs")
    sale = money.get("sale_value")

    chains_checked: List[str] = []

    # current_state_value == market_value - regularization_costs
    if market is not None and regularization is not None and current is not None:
        chains_checked.append("market-regularization=current")
        if not _approx_equal(current, market - regularization):
            report.error(
                "MONEY_CHAIN_INCONSISTENT",
                "money",
                f"current_state_value ({current}) != market_value ({market}) - "
                f"regularization_costs ({regularization}) = {market - regularization}",
            )

    # sale_value == current_state_value - cancellation_costs
    if current is not None and cancellation is not None and sale is not None:
        chains_checked.append("current-cancellation=sale")
        if not _approx_equal(sale, current - cancellation):
            report.error(
                "MONEY_CHAIN_INCONSISTENT",
                "money",
                f"sale_value ({sale}) != current_state_value ({current}) - "
                f"cancellation_costs ({cancellation}) = {current - cancellation}",
            )

    # Non-negativity sanity.
    for key in ("market_value", "base_auction_value", "current_state_value", "sale_value"):
        val = money.get(key)
        if val is not None and val < 0:
            report.error("MONEY_NEGATIVE_VALUE", f"money.{key}", f"{key} is negative ({val})")

    report.checks["money_chains_checked"] = chains_checked


_BASE_PRICE_TOKENS = ["prezzo base", "base d'asta", "base d asta", "base asta", "prezzo a base"]


def _check_money_rows(
    worksheet: Dict[str, Any], page_index: Dict[int, str], report: _Report
) -> None:
    """Money rows are evidenced, non-duplicated and not mislabeled (rules per spec).

    Generic — never branches on a specific perizia. The contract builder merges /
    drops the offending rows; the validator surfaces WHY so a clean contract is
    never produced silently. An unevidenced amount that cannot be merged into an
    evidenced row is a hard error (fail closed); the rest are warnings.
    """
    money = worksheet["money"]
    money_ev = money.get("evidence_pages") or []

    # Amounts that DO carry evidence somewhere (used to decide mergeability).
    evidenced_amounts: List[float] = []
    if money_ev:
        for key in (
            "market_value",
            "base_auction_value",
            "regularization_costs",
            "current_state_value",
            "cancellation_costs",
            "sale_value",
        ):
            val = money.get(key)
            if val:
                evidenced_amounts.append(float(val))
    for coll in ("deductions", "buyer_side_costs", "procedure_cancelled_costs"):
        for item in money.get(coll, []):
            amt = item.get("amount")
            if amt and item.get("evidence_pages"):
                evidenced_amounts.append(float(amt))

    def _has_evidenced_match(amount: float) -> bool:
        return any(_approx_equal(amount, e) for e in evidenced_amounts)

    # 1) Cost rows (deductions / buyer-side) must be evidenced or mergeable.
    for coll in ("deductions", "buyer_side_costs"):
        for i, item in enumerate(money.get(coll, [])):
            amt = item.get("amount")
            if not amt:  # None / 0 handled separately
                continue
            if item.get("evidence_pages"):
                continue
            path = f"money.{coll}[{i}]"
            if _has_evidenced_match(float(amt)):
                report.warn(
                    "MONEY_ROW_EVIDENCE_VIA_MERGE",
                    path,
                    f"money row '{item.get('label')}' ({amt}) has no evidence_pages but "
                    "matches an evidenced amount; it will be merged into the evidenced row",
                )
            else:
                report.error(
                    "MONEY_ROW_MISSING_EVIDENCE",
                    path,
                    f"money row '{item.get('label')}' ({amt}) has no evidence_pages and no "
                    "evidenced row with the same amount to merge into",
                )

    # 2) Zero-amount buyer-side costs are not meaningful buyer actions.
    for i, item in enumerate(money.get("buyer_side_costs", [])):
        if item.get("amount") == 0:
            report.warn(
                "ZERO_AMOUNT_BUYER_COST",
                f"money.buyer_side_costs[{i}]",
                f"buyer-side cost '{item.get('label')}' has amount 0; it is not a buyer action",
            )

    # 3) Duplicate labelled money rows (same normalized label + amount).
    seen: Dict[Tuple[str, float], str] = {}
    for coll in ("deductions", "buyer_side_costs", "procedure_cancelled_costs"):
        for i, item in enumerate(money.get(coll, [])):
            amt = item.get("amount")
            if amt is None:
                continue
            label = _norm(item.get("label"))
            if not label:
                continue
            dkey = (label, round(float(amt), 2))
            if dkey in seen:
                report.warn(
                    "DUPLICATE_MONEY_ROW",
                    f"money.{coll}[{i}]",
                    f"duplicate money row '{item.get('label')}' ({amt}) also at {seen[dkey]}",
                )
            else:
                seen[dkey] = f"money.{coll}[{i}]"

    # 4) Same amount under a chain-cost role AND buyer-side role (justified-but-flagged).
    chain_cost_amounts: List[float] = []
    for key in ("regularization_costs", "cancellation_costs"):
        val = money.get(key)
        if val:
            chain_cost_amounts.append(float(val))
    for item in money.get("deductions", []):
        amt = item.get("amount")
        if amt:
            chain_cost_amounts.append(float(amt))
    for i, item in enumerate(money.get("buyer_side_costs", [])):
        amt = item.get("amount")
        if amt and any(_approx_equal(float(amt), c) for c in chain_cost_amounts):
            report.warn(
                "SAME_AMOUNT_CONFLICTING_KIND",
                f"money.buyer_side_costs[{i}]",
                f"amount {amt} appears both in the valuation chain and as a buyer-side cost; "
                "it must be represented once with a clear note",
            )

    # 5) Auction base-price gating. The judicial sale value is NEVER relabeled as
    # prezzo base; a prezzo base candidate is only honored with explicit text. The
    # explicit-text verdict is published in checks.money_signals for the contract
    # builder (which has no page text) to route the amount correctly.
    at = money.get("auction_terms") or {}
    base = at.get("prezzo_base_asta")
    if base is None:
        base = money.get("base_auction_value")
    base_ev = list(at.get("evidence_pages") or []) + list(money_ev)
    base_text = _evidence_text(base_ev, page_index)
    base_explicit = any(tok in base_text for tok in _BASE_PRICE_TOKENS)
    report.checks["money_signals"] = {
        "base_price_candidate": base,
        "base_price_explicit_text": bool(base_explicit),
    }
    sale = money.get("sale_value")
    if (
        base is not None
        and sale is not None
        and _approx_equal(float(base), float(sale))
        and not base_explicit
    ):
        report.warn(
            "BASE_PRICE_MISLABELED",
            "money.auction_terms.prezzo_base_asta",
            "prezzo base candidate equals sale_value and the cited text has no explicit "
            "'prezzo base'/'base d'asta' statement; do not label the judicial sale value "
            "as auction base price",
        )

    # 6) Page evidence is unavoidable: every significant amount must cite a page.
    #    Auction terms and uncertain-money amounts without evidence are hard errors.
    for field in ("prezzo_base_asta", "offerta_minima", "rialzo_minimo", "cauzione"):
        amt = at.get(field)
        if amt and not (at.get("evidence_pages") or money_ev):
            report.error(
                "MONEY_ROW_MISSING_EVIDENCE",
                f"money.auction_terms.{field}",
                f"auction term {field} ({amt}) has no evidence_pages",
            )
    for i, item in enumerate(money.get("uncertain_money", [])):
        if item.get("amount") and not item.get("evidence_pages"):
            report.error(
                "MONEY_ROW_MISSING_EVIDENCE",
                f"money.uncertain_money[{i}]",
                f"uncertain money '{item.get('label')}' ({item.get('amount')}) has no "
                "evidence_pages (page evidence is mandatory for every amount)",
            )


def _check_buyer_vs_procedure(
    worksheet: Dict[str, Any], page_index: Dict[int, str], report: _Report
) -> None:
    """Buyer costs separated from cancellable formalities (rules 3 & 4)."""
    money = worksheet["money"]

    # 3: a buyer-side cost must not actually be a cancellable formality (ipoteca/pignoramento/sequestro).
    for i, item in enumerate(money.get("buyer_side_costs", [])):
        label_norm = _norm(item.get("label"))
        if any(tok in label_norm for tok in _CANCELLABLE_LABEL_TOKENS):
            report.error(
                "BUYER_SIDE_INCLUDES_CANCELLABLE_FORMALITY",
                f"money.buyer_side_costs[{i}]",
                f"buyer-side cost '{item.get('label')}' looks like a procedure-cancelled "
                "formality (ipoteca/pignoramento/sequestro)",
            )

    # 4: ipoteca/pignoramento/sequestro must not be auto buyer-burden.
    for i, item in enumerate(worksheet["legal_formalities"]):
        if item.get("type") not in CANCELLABLE_FORMALITY_TYPES:
            continue
        buyer_burden = bool(item.get("buyer_burden"))
        cancelled = bool(item.get("cancelled_by_procedure"))
        if not buyer_burden:
            continue
        if cancelled:
            report.error(
                "FORMALITY_BUYER_BURDEN_CONTRADICTION",
                f"legal_formalities[{i}]",
                f"formality '{item.get('type')}' is both cancelled_by_procedure and buyer_burden",
            )
        # buyer_burden claimed -> require explicit textual support.
        ev_text = _evidence_text(item.get("evidence_pages", []), page_index)
        if not _has_buyer_burden(ev_text):
            report.error(
                "FORMALITY_BUYER_BURDEN_UNSUPPORTED",
                f"legal_formalities[{i}]",
                f"formality '{item.get('type')}' marked buyer_burden but cited text does not say "
                "the buyer/aggiudicatario pays it",
            )


def _check_compliance_contradictions(
    worksheet: Dict[str, Any], page_index: Dict[int, str], report: _Report
) -> None:
    """Conforming areas can't be promoted to grave/non-conforming (rules 5 & 8)."""
    # Map canonical area token -> classification (conforming wins for the check).
    conforming_areas: Dict[str, int] = {}
    for i, item in enumerate(worksheet["technical_compliance"]):
        if item.get("classification") == "conforming":
            conforming_areas[_area_token(item.get("area"))] = i

    # 5a: ground each compliance classification against its cited text.
    # NOTE: perizia pages mix multiple areas (e.g. urbanistica conforme AND
    # difformità edilizia on the same page), so we must NOT reject a 'conforming'
    # claim merely because a negative marker for a DIFFERENT area appears on the
    # cited page. We therefore require POSITIVE conformity language for a
    # conforming claim, and a contradictory marker for a non-conforming claim.
    for i, item in enumerate(worksheet["technical_compliance"]):
        classification = item.get("classification")
        ev_text = _evidence_text(item.get("evidence_pages", []), page_index)
        if not ev_text:
            continue
        has_conform = "conform" in ev_text
        has_negative = _has_negative(ev_text)
        if classification == "conforming" and not has_conform:
            report.error(
                "UNSUPPORTED_COMPLIANCE_CLAIM",
                f"technical_compliance[{i}]",
                f"area '{item.get('area')}' classified conforming but cited text contains no "
                "conformity statement",
            )
        if classification in {"non_conforming", "not_regularizable"} and has_conform and not has_negative:
            report.error(
                "COMPLIANCE_EVIDENCE_CONTRADICTION",
                f"technical_compliance[{i}]",
                f"area '{item.get('area')}' classified {classification} but cited text says conforme "
                "with no contradictory marker",
            )

    # 5b: a grave/non-conforming RISK on an area the worksheet itself calls conforming,
    # without stronger contradictory evidence in the risk's cited text.
    for i, item in enumerate(worksheet["risk_classification"]):
        if item.get("severity") != "grave":
            continue
        token = _area_token(item.get("area"))
        if token in conforming_areas:
            ev_text = _evidence_text(item.get("evidence_pages", []), page_index)
            if not _has_negative(ev_text):
                report.error(
                    "COMPLIANCE_CONTRADICTION",
                    f"risk_classification[{i}]",
                    f"area '{item.get('area')}' is classified conforming elsewhere but flagged as "
                    "grave with no stronger contradictory evidence",
                )


def _check_lots(worksheet: Dict[str, Any], report: _Report) -> None:
    """Reject blended multi-lot worksheets and flat-field lot contamination.

    Generic and bene-agnostic: keys ONLY on distinct numbered LOTS, never on beni
    (several beni inside one lot are normal). A worksheet that mixes two or more
    lots cannot be turned into one safe customer contract — the orchestrator
    diverts such jobs to manual review, and this check is the defense-in-depth
    net for any direct validation.
    """
    lot_ids = lots_mod.worksheet_lot_ids(worksheet)
    if len(lot_ids) >= 2:
        report.error(
            "MULTI_LOT_SELECTION_UNCLEAR",
            "worksheet",
            f"worksheet mixes {len(lot_ids)} distinct lots ({', '.join(lot_ids)}); a single "
            "lot must be selected before a customer contract can be built (no lot blending)",
        )
    for field in lots_mod.contaminated_flat_fields(worksheet):
        report.error(
            "LOT_CONTAMINATION",
            field["path"],
            f"flat field '{field['path']}' mixes data from lots {field['lot_ids']}",
        )


def _check_regularizable_not_blocking(worksheet: Dict[str, Any], report: _Report) -> None:
    """A regularizable issue with cost/timing must not be marked blocking (rule 6)."""
    for i, item in enumerate(worksheet["technical_compliance"]):
        if item.get("classification") != "regularizable":
            continue
        has_path = (item.get("cost") is not None) or bool(item.get("timing"))
        if has_path and item.get("blocks_saleability"):
            report.error(
                "REGULARIZABLE_MARKED_BLOCKING",
                f"technical_compliance[{i}]",
                f"area '{item.get('area')}' is regularizable with cost/timing but marked as "
                "blocking saleability",
            )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def validate_worksheet(
    worksheet: Dict[str, Any],
    pages: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Validate a normalized worksheet against the extracted pages.

    Returns a validator_report dict with ``validation_status`` of
    ``VALIDATED`` or ``VALIDATION_FAILED``.
    """
    report = _Report()
    page_index = _page_text_index(pages)
    valid_pages = set(page_index.keys())

    _check_evidence_pages_exist(worksheet, valid_pages, report)
    _check_important_claims_have_evidence(worksheet, report)
    _check_money_chains(worksheet, report)
    _check_money_rows(worksheet, page_index, report)
    _check_buyer_vs_procedure(worksheet, page_index, report)
    _check_compliance_contradictions(worksheet, page_index, report)
    _check_regularizable_not_blocking(worksheet, report)
    _check_lots(worksheet, report)

    status = STATUS_FAILED if report.violations else STATUS_VALIDATED

    return {
        "schema_version": VALIDATOR_SCHEMA_VERSION,
        "validation_status": status,
        "violation_count": len(report.violations),
        "warning_count": len(report.warnings),
        "violations": report.violations,
        "warnings": report.warnings,
        "checks": report.checks,
        "valid_page_count": len(valid_pages),
    }
