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

import copy
import itertools
import re
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

from . import lots as lots_mod
from .analyst import CANCELLABLE_FORMALITY_TYPES

VALIDATOR_SCHEMA_VERSION = "cv2.validator.v1"
COMPLIANCE_GATE_SCHEMA_VERSION = "cv2.compliance_gate.v1"

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
    "non risulta regolare",
    "non risulta rispettata",
    "non e stata verificata",
    "non viene verificata",
    "senza verifica",
    "non risulta agibile",
    "non sussiste corrispondenza",
    "non esiste il certificato",
    "non esiste la dichiarazione",
    "assenza di certificato",
    "assenza delle dichiarazioni",
    "riscontrate incongruenze",
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
_CANCELLATION_MONEY_LABEL_TOKENS = [
    "cancellaz",
    "trascrizion",
    "iscrizion",
    "formalita",
]

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


_AREA_WORDS = r"(?:edilizi[oa]\s+urbanistic[oa]|urbanistic[oa]|catastale)"
_REGULARITY_SUBJECT = (
    rf"la\s+regolarita\s+{_AREA_WORDS}\s+"
    r"(?:dell\s+immobile|della\s+costruzione|rispetto\s+allo\s+stato)"
)
_SMALL_AFFIRMATIVE_GAP = r"(?:[a-z0-9]+\s+){0,3}"
_AFFIRMATIVE_COMPLIANCE_PATTERNS = (
    re.compile(
        rf"\b{_REGULARITY_SUBJECT}\s+(?:risulta|e)\s+"
        rf"{_SMALL_AFFIRMATIVE_GAP}(?:rispettata|verificata|accertata|conforme)\b"
    ),
    re.compile(
        rf"\bla\s+verifica\s+{_AREA_WORDS}\s+(?:risulta|e)\s+"
        rf"{_SMALL_AFFIRMATIVE_GAP}(?:conforme|verificata|positiva)\b"
    ),
    re.compile(
        rf"\bla\s+corrispondenza\s+catastale\s+(?:risulta|e)\s+"
        rf"{_SMALL_AFFIRMATIVE_GAP}conforme\b"
    ),
    re.compile(
        rf"\bl\s+immobile\s+(?:risulta|e)\s+{_SMALL_AFFIRMATIVE_GAP}"
        r"(?:conforme|regolare|agibile)\b"
    ),
    re.compile(
        rf"\bl\s+impianto(?:\s+\w+){{0,2}}\s+(?:risulta|e)\s+"
        rf"{_SMALL_AFFIRMATIVE_GAP}conforme\b"
    ),
    re.compile(
        rf"\bla\s+planimetria(?:\s+catastale)?\s+(?:risulta|e)\s+"
        rf"{_SMALL_AFFIRMATIVE_GAP}conforme\b"
    ),
    re.compile(r"\bsussiste\s+corrispondenza\s+catastale\b"),
    # Checkbox/declaration form used by CTU reports: DICHIARA + the positive
    # regularity noun phrase. The negative form contains "non regolarita" and
    # cannot match this closed shape.
    re.compile(rf"\bdichiara(?:\s+\w+){{0,2}}\s+{_REGULARITY_SUBJECT}\b"),
)
_NEGATIVE_FINDING_ABSENCE_PATTERNS = (
    re.compile(r"\bnon\s+sono\s+state\s+riscontrate\s+incongruenze\b"),
    re.compile(r"\bnon\s+risultano\s+difformita\b"),
    re.compile(r"\bnon\s+sono\s+presenti\s+difformita\b"),
    re.compile(r"\bnon\s+sono\s+presenti\s+abusi\b"),
)
_NEGATED_COMPLIANCE_PREDICATE = re.compile(
    r"\b(?:verific\w*|controll\w*|accert\w*|conform\w*|"
    r"regolar(?:e|i|izz\w*)|rispett\w*|attest\w*|document\w*|risult\w*|"
    r"disponibil\w*|reperibil\w*|esibit\w*|prodott\w*|consegnat\w*|"
    r"acced\w*|sopralluog\w*|ispezion\w*|vision\w*)\b"
)
# This list is deliberately topic-blind across the cited evidence text. It can
# over-suppress a clean compliance declaration when an unrelated sentence says
# e.g. "in assenza di posto auto", "manca il certificato di garanzia della
# caldaia", or "privo di cantina". That safe-direction cost is accepted here;
# topic co-occurrence gating is a follow-up because narrowing this scan risks
# reopening false customer-facing conformity findings.
_COMPLIANCE_DISQUALIFIERS = (
    re.compile(r"\bsenza\b"),
    re.compile(r"\bpriv[oaie]\s+di\b"),
    re.compile(r"\bsprovvist[oaie]\s+di\b"),
    re.compile(r"\bsfornit[oaie]\s+di\b"),
    re.compile(r"\bin\s+assenza\s+di\b"),
    re.compile(r"\bcaren(?:te|ti|z[ae])\b"),
    re.compile(r"\b(?:manca|mancant[ei]|mancanz[ae])\b"),
    re.compile(r"\bincomplet[oaie]\b"),
    # Degree-qualified conformity is not a clean affirmative declaration.
    re.compile(r"\b(?:solo\s+)?parzial(?:e|i|mente)\b"),
    re.compile(r"\bin\s+parte\b"),
    re.compile(r"\bquasi\b"),
    re.compile(r"\bsostanzialmente\b"),
    re.compile(r"\bin\s+larga\s+parte\b"),
    re.compile(r"\bin\s+misura\s+parziale\b"),
    re.compile(r"\bprevalentemente\b"),
    re.compile(r"\bpressoche\b"),
    re.compile(r"\btendenzialmente\b"),
    re.compile(r"\bperlopiu\b"),
    re.compile(r"\bnel\s+complesso\b"),
    re.compile(r"\bgrosso\s+modo\b"),
    # A named exception co-located with "conforme" is partial compliance.
    re.compile(r"\bsalvo\b"),
    re.compile(r"\bfatta?\s+eccezione\s+per\b"),
    re.compile(r"\bad?\s+eccezione\s+di\b"),
    re.compile(r"\beccezion(?:e)?\s+fatta\s+per\b"),
    re.compile(r"\bcon\s+esclusione\s+di\b"),
    re.compile(r"\btranne\b"),
    re.compile(r"\bda\s+verific(?:are|at[aoei])\b"),
    re.compile(r"\b(?:dovra|sara)(?:\s+\w+){0,3}\s+verific(?:are|at[aoei])\b"),
    re.compile(r"\bda\s+riconfermare\b"),
    re.compile(r"\bin\s+attesa\s+di\b"),
    re.compile(r"\briserva\b"),
)


def _has_compliance_disqualifier(text_norm: str) -> bool:
    """Scan all evidence, without fixed lookahead windows, for a hedge."""
    compact = " ".join(_norm(text_norm).split())
    lexical = re.sub(r"[^a-z0-9]+", " ", compact).strip()
    for pattern in _NEGATIVE_FINDING_ABSENCE_PATTERNS:
        lexical = pattern.sub(" ", lexical)
    if any(pattern.search(lexical) for pattern in _COMPLIANCE_DISQUALIFIERS):
        return True

    # Within each hard-delimited statement, any compliance/verification
    # predicate after "non" disqualifies, regardless of nesting or distance.
    # This catches long subordinate clauses without allowing negation to bleed
    # across an unrelated semicolon-delimited CTU declaration.
    for statement in re.split(r"[.;:]", compact):
        statement_lexical = re.sub(r"[^a-z0-9]+", " ", statement).strip()
        for pattern in _NEGATIVE_FINDING_ABSENCE_PATTERNS:
            statement_lexical = pattern.sub(" ", statement_lexical)
        for negation in re.finditer(r"\bnon\b", statement_lexical):
            if _NEGATED_COMPLIANCE_PREDICATE.search(statement_lexical, negation.end()):
                return True
    return False


def _has_positive_compliance_statement(text_norm: str) -> bool:
    """Require a closed affirmative shape and no whole-evidence hedge.

    Fail-safe asymmetry is intentional: ambiguous evidence is not affirmative.
    A false manual-review flag is safer than a false customer-facing conformity
    finding. Topic-shifted negations remain harmless only when they do not use a
    compliance/verification predicate (for example, an uncensused garden).
    """
    lexical = re.sub(r"[^a-z0-9]+", " ", _norm(text_norm)).strip()
    absence_positive = any(pattern.search(lexical) for pattern in _NEGATIVE_FINDING_ABSENCE_PATTERNS)
    affirmative_shape = absence_positive or any(
        pattern.search(lexical) for pattern in _AFFIRMATIVE_COMPLIANCE_PATTERNS
    )
    if not affirmative_shape:
        return False

    return not _has_compliance_disqualifier(text_norm)


def _has_buyer_burden(text_norm: str) -> bool:
    return any(tok in text_norm for tok in _BUYER_BURDEN_TOKENS)


def _is_cancellation_money_label(label: Any) -> bool:
    n = _norm(label)
    return any(tok in n for tok in _CANCELLATION_MONEY_LABEL_TOKENS)


def _approx_equal(a: float, b: float) -> bool:
    tol = max(MONEY_ABS_TOLERANCE, MONEY_REL_TOLERANCE * max(abs(a), abs(b)))
    return abs(a - b) <= tol


def _deduction_subset_for_delta(amounts: List[float], delta: float) -> Optional[List[int]]:
    """Return indexes of deduction rows that explain a valuation delta.

    Perizie sometimes present valuation as staged math, e.g. market value at
    completed works -> current condition -> judicial sale value. In those cases
    all rows are legitimate deductions, but only a subset belongs to the
    market-to-current step.
    """
    if _approx_equal(delta, 0.0):
        return []
    for size in range(1, len(amounts) + 1):
        for indexes in itertools.combinations(range(len(amounts)), size):
            if _approx_equal(sum(amounts[i] for i in indexes), delta):
                return list(indexes)
    return None


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
    """Important claims must carry page evidence (rule 1).

    Exception: a compliance entry already classified ``uncertain`` is an
    uncertainty flag, not a claim — it may legitimately lack evidence (that is
    WHY it is uncertain) and must not fail validation. It is surfaced as a
    warning so the contract still carries the manual-review signal.
    """
    for i, item in enumerate(worksheet["technical_compliance"]):
        if not item.get("evidence_pages"):
            if item.get("classification") == "uncertain":
                report.warn(
                    "MISSING_EVIDENCE_SOFT",
                    f"technical_compliance[{i}]",
                    f"uncertain compliance area '{item.get('area')}' has no evidence_pages "
                    "(kept as manual-review uncertainty, not a claim)",
                )
            else:
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

    deduction_amounts = [
        float(item.get("amount"))
        for item in (money.get("deductions") or [])
        if item.get("amount") is not None and not _is_cancellation_money_label(item.get("label"))
    ]

    # Deductions split across two steps: some bridge market_value -> current_state
    # (e.g. deprezzamento/regularization), the rest bridge current_state -> sale
    # (e.g. the judicial-sale reduction "-15% per vendita giudiziaria"). Track how
    # much of the deductions table applies to the sale step so the sale can fall
    # below current even with zero cancellation costs.
    remaining_deductions = 0.0

    # current_state_value == market_value - explicit deductions, when the
    # worksheet has a deprezzamenti/deductions table. If no explicit deductions
    # exist, fall back to the older regularization-only chain.
    if market is not None and current is not None and deduction_amounts:
        total_deductions = sum(deduction_amounts)
        chains_checked.append("market-deductions=current")
        if not _approx_equal(current, market - total_deductions):
            current_step = _deduction_subset_for_delta(deduction_amounts, market - current)
            if current_step is None:
                report.error(
                    "MONEY_CHAIN_INCONSISTENT",
                    "money",
                    f"current_state_value ({current}) != market_value ({market}) - "
                    f"deductions ({total_deductions}) = {market - total_deductions}",
                )
            else:
                remaining_deductions = sum(
                    amount
                    for i, amount in enumerate(deduction_amounts)
                    if i not in set(current_step)
                )
                chains_checked[-1] = "market-deduction-subset=current"
    elif market is not None and regularization is not None and current is not None:
        chains_checked.append("market-regularization=current")
        if not _approx_equal(current, market - regularization):
            report.error(
                "MONEY_CHAIN_INCONSISTENT",
                "money",
                f"current_state_value ({current}) != market_value ({market}) - "
                f"regularization_costs ({regularization}) = {market - regularization}",
            )

    # sale_value == current_state_value - (sale-step deductions) - cancellation_costs.
    # Accounting for the sale-step deductions is what lets a legitimate judicial-sale
    # reduction lower the sale below current without tripping the chain.
    if current is not None and sale is not None and (cancellation is not None or remaining_deductions):
        cancellation_val = cancellation or 0.0
        expected_sale = current - remaining_deductions - cancellation_val
        chains_checked.append("current-deductions-cancellation=sale")
        if not _approx_equal(sale, expected_sale):
            report.error(
                "MONEY_CHAIN_INCONSISTENT",
                "money",
                f"sale_value ({sale}) != current_state_value ({current}) - "
                f"remaining deductions ({remaining_deductions}) - "
                f"cancellation_costs ({cancellation_val}) = {expected_sale}",
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
    chain_cost_rows: List[Tuple[float, str]] = []
    for key in ("regularization_costs", "cancellation_costs"):
        val = money.get(key)
        if val:
            chain_cost_rows.append((float(val), key))
    for item in money.get("deductions", []):
        amt = item.get("amount")
        if amt:
            chain_cost_rows.append((float(amt), str(item.get("label") or "")))
    for i, item in enumerate(money.get("buyer_side_costs", [])):
        amt = item.get("amount")
        label_norm = _norm(item.get("label"))
        if not amt:
            continue
        conflicting = False
        for chain_amt, chain_label in chain_cost_rows:
            chain_label_norm = _norm(chain_label)
            if not _approx_equal(float(amt), chain_amt):
                continue
            if label_norm and chain_label_norm and (
                label_norm in chain_label_norm or chain_label_norm in label_norm
            ):
                conflicting = True
                break
        if conflicting:
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
        has_conform = _has_positive_compliance_statement(ev_text)
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
    lot_ids = lots_mod.distinct_lots(worksheet)
    if len(lot_ids) >= 2:
        report.error(
            "MULTI_LOT_SELECTION_UNCLEAR",
            "worksheet",
            f"worksheet mixes {len(lot_ids)} distinct lots ({', '.join(lot_ids)}); a single "
            "lot must be selected before a customer contract can be built (no lot blending)",
        )
    # Any single worksheet string that splices two lots together is contamination —
    # this covers address/ownership AND money/occupancy/compliance/formality fields.
    for field in lots_mod.contaminated_worksheet_fields(worksheet):
        report.error(
            "LOT_CONTAMINATION",
            field["path"],
            f"field '{field['path']}' mixes data from lots {field['lot_ids']}",
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


def check_selected_lot_present(lot_ids: List[str], selected_lot_id: Any) -> Optional[Dict[str, Any]]:
    """Return a violation dict if ``selected_lot_id`` is not among the document's lots.

    Generic helper used by the orchestrator before re-analyzing a chosen lot: a
    selected lot that does not exist in a multi-lot document must fail closed rather
    than silently analyzing the wrong (or no) lot.
    """
    from . import lots as _lots
    want = _lots.normalize_lot_token(selected_lot_id) or str(selected_lot_id or "").strip()
    known = {str(x) for x in (lot_ids or [])}
    if want and want in known:
        return None
    return {
        "code": "SELECTED_LOT_NOT_FOUND",
        "severity": "error",
        "path": "selected_lot_id",
        "detail": (
            f"selected lot '{selected_lot_id}' (normalized '{want}') is not among the "
            f"document's lots ({sorted(known)})"
        ),
    }


# ---------------------------------------------------------------------------
# Compliance evidence gate (deterministic downgrade, runs BEFORE validation)
# ---------------------------------------------------------------------------
def apply_compliance_evidence_gate(
    worksheet: Dict[str, Any],
    pages: List[Dict[str, Any]],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Downgrade unsupported compliance claims to 'uncertain' (manual review).

    Generic, document-agnostic rule (never defaults to conforming):
      * 'conforming' is honored ONLY when the cited page text contains an
        explicit conformity statement. Missing evidence, generic/administrative
        text, or text not containing conformity language -> 'uncertain'.
      * Any other classification with NO evidence pages at all -> 'uncertain'
        (a claim that cannot be verified is preserved as an uncertainty flag,
        never as a fact).

    In the selected-lot pipeline the ``pages`` are already the isolated
    lot+global subset, so evidence "not clearly tied to the selected lot"
    naturally yields empty cited text here and is downgraded.

    Returns ``(gated_worksheet, gate_report)``. The input worksheet is not
    mutated. Downgrades are also appended to ``missing_or_uncertain`` so the
    contract's uncertainty flags carry them. The validator's
    UNSUPPORTED_COMPLIANCE_CLAIM check stays active as defense-in-depth for any
    path that skips this gate.
    """
    gated = copy.deepcopy(worksheet)
    page_index = _page_text_index(pages)
    downgrades: List[Dict[str, Any]] = []

    for i, item in enumerate(gated.get("technical_compliance") or []):
        classification = item.get("classification")
        if classification == "uncertain":
            continue
        ev_pages = list(item.get("evidence_pages") or [])
        ev_text = _evidence_text(ev_pages, page_index)

        reason: Optional[str] = None
        if classification == "conforming":
            if not ev_pages:
                reason = (
                    "classificata 'conforming' senza alcuna pagina di evidenza"
                )
            elif not _has_positive_compliance_statement(ev_text):
                reason = (
                    "classificata 'conforming' ma il testo citato non contiene una "
                    "dichiarazione esplicita di conformità per il contesto analizzato"
                )
        elif not ev_pages:
            reason = (
                f"classificata '{classification}' senza alcuna pagina di evidenza"
            )

        if reason is None:
            continue

        downgrades.append(
            {
                "path": f"technical_compliance[{i}]",
                "area": item.get("area"),
                "from": classification,
                "to": "uncertain",
                "reason": reason,
                "evidence_pages": ev_pages,
            }
        )
        item["classification"] = "uncertain"
        item["needs_manual_review"] = True
        # A downgraded entry is an uncertainty flag, not a claim: citations to
        # pages outside the analyzed context (e.g. another lot's pages) are
        # dropped here (they are preserved in the gate report above) so the
        # page-existence check does not fail a claim we already neutralized.
        item["evidence_pages"] = [p for p in ev_pages if p in page_index]
        note = (
            f"Declassata a 'uncertain' (verifica manuale): {reason}."
        )
        item["notes"] = f"{item.get('notes')} {note}".strip() if item.get("notes") else note
        gated.setdefault("missing_or_uncertain", []).append(
            f"Conformità '{item.get('area')}': {reason}; stato impostato a "
            "'uncertain' e richiesta verifica manuale."
        )

    gate_report = {
        "schema_version": COMPLIANCE_GATE_SCHEMA_VERSION,
        "checked_count": len(gated.get("technical_compliance") or []),
        "downgrade_count": len(downgrades),
        "downgrades": downgrades,
    }
    return gated, gate_report


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
