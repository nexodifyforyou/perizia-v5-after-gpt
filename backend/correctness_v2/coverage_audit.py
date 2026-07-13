"""
No-silent-omissions coverage gate for Correctness Mode v2.

Builds ``coverage_audit.json`` and ``page_by_page_audit.json`` by comparing the
extracted document truth (input_pages + deterministic doc_signals) and the
analyst worksheet against the software output (customer_report + contract).

HARD RULES:
  * Never invents facts: every entry points at the document page(s) and at the
    report location (or explains why the fact is not rendered).
  * Generic: no tribunale/city/document-specific branching.
  * Deterministic: pure function of the persisted artifacts.

Every material fact ends up classified as exactly one action:
  rendered_in_customer_report | rendered_as_uncertainty | rendered_as_manual_review
  | rendered_as_background_with_reason | excluded_as_non_material_with_reason
  | blocked_due_to_coverage_failure
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set, Tuple

from . import doc_signals
from .doc_signals import (
    SEV_BACKGROUND,
    SEV_CRITICAL,
    SEV_IMPORTANT,
    SEV_USEFUL,
    norm_text,
)

COVERAGE_AUDIT_SCHEMA_VERSION = "cv2.coverage_audit.v1"
PAGE_AUDIT_SCHEMA_VERSION = "cv2.page_by_page_audit.v1"

STATUS_PASS = "PASS"
STATUS_WARNING = "WARNING"
STATUS_FAIL = "FAIL"

ACTION_INCLUDED = "included"
ACTION_UNCERTAINTY = "uncertainty"
ACTION_MANUAL_REVIEW = "manual_review"
ACTION_BACKGROUND = "background"
ACTION_EXCLUDED = "excluded_with_reason"
ACTION_BLOCKED = "blocked"

MATCH = "match"
PARTIAL = "partial"
MISSING = "missing"
CONTRADICTED = "contradicted"
UNCLEAR = "unclear"

# Esito labels for the customer-facing page-by-page table.
ESITO_COPERTO = "Coperto"
ESITO_PARZIALE = "Parziale"
ESITO_MANCANTE = "Mancante"
ESITO_DA_VERIFICARE = "Da verificare"
ESITO_NON_MATERIALE = "Non materiale"

# Identity matching of amounts must be near-exact: a 0.5% relative tolerance
# would make adjacent chain values (e.g. 94.700 vs 95.000) "equal" and hide
# real omissions. Only sub-euro formatting differences are tolerated.
_MONEY_ABS_TOL = 0.011
_MONEY_REL_TOL = 0.0

# customer_report top-level keys whose content is customer-visible "main" output.
_MAIN_SECTIONS = (
    "case_identity", "lot_structure", "executive_summary", "key_facts",
    "risk_sections", "money_sections.valuation_chain", "money_sections.auction_terms",
    "money_sections.buyer_side_costs", "money_sections.procedure_cancelled_formalities",
    "money_sections.market_comparatives", "money_sections.context_values",
    "beni_sections", "buyer_checklist", "occupancy_section", "compliance_section",
    "formalities_section", "surfaces_section", "lot_selection", "title", "subtitle",
)
_UNCERTAIN_SECTIONS = ("money_sections.uncertain_money", "risk_sections.da_verificare")
_MANUAL_SECTIONS = ("manual_review_flags",)

_SKIP_KEYS = {
    "schema_version", "analysis_id", "job_id", "_saved_at", "disclaimer",
    "sections_meta", "report_status", "evidence_pages", "kind", "signal_id",
    "quality_control", "customer_evidence_index", "admin_evidence_index",
    # The money-confirmation prompt echoes ambiguous amounts (with page excerpts)
    # only to ASK the customer — never a report placement; never index it.
    "money_confirmation",
    # Pure formatting duplicates of structured amounts: indexing them would
    # create role-less wildcard entries that defeat role-aware matching.
    "amount_display", "value_display", "cost_display",
}

# Section-implied money roles for the report pool (role-aware matching).
_SECTION_ROLES = {
    "money_sections.buyer_side_costs": doc_signals.ROLE_BUYER_SIDE_COST,
    "money_sections.procedure_cancelled_formalities": doc_signals.ROLE_PROCEDURE_CANCELLED_FORMALITY,
    "money_sections.market_comparatives": doc_signals.ROLE_COMPARABLE_MARKET_VALUE,
    "money_sections.uncertain_money": doc_signals.ROLE_UNCERTAIN_MONEY,
    "formalities_section": doc_signals.ROLE_PROCEDURE_CANCELLED_FORMALITY,
}

# Contract row "roles" tokens -> money roles.
_ROW_ROLE_TOKENS = {
    "buyer_side": doc_signals.ROLE_BUYER_SIDE_COST,
    "cancellation": doc_signals.ROLE_BUYER_SIDE_COST,
    "regularization": doc_signals.ROLE_REGULARIZATION_COST,
    "deduction": doc_signals.ROLE_DEPRECIATION,
}

# Role-mismatch severity is decided by the SHARED taxonomy in doc_signals
# (CORE_MONEY_ROLES / BACKGROUND_MONEY_ROLES / roles_compatible /
# conflict_is_misleading) — never by a local list that could drift.


def _approx_equal(a: float, b: float) -> bool:
    tol = max(_MONEY_ABS_TOL, _MONEY_REL_TOL * max(abs(a), abs(b)))
    return abs(a - b) <= tol


def _pages_list(value: Any) -> List[int]:
    out: List[int] = []
    for p in value or []:
        try:
            out.append(int(p))
        except (TypeError, ValueError):
            continue
    return sorted(set(out))


# ---------------------------------------------------------------------------
# Report pool: where every text/amount of the software output lives
# ---------------------------------------------------------------------------
class ReportPool:
    """Searchable index of the customer report (text + amounts per section).

    Every amount carries the set of money ROLES it plays in the report, so the
    audit can require BOTH amount and role to match. An empty role set means
    "role unknown" (free text) and matches any role — unknown must never create
    false contradictions, only explicit conflicting roles do.
    """

    def __init__(self) -> None:
        self.section_text: Dict[str, List[str]] = {}
        # (amount, section, roles)
        self.amounts: List[Tuple[float, str, frozenset]] = []
        # cent-keyed index over self.amounts, built lazily on first lookup
        # (the pool is fully populated before evaluation starts).
        self._index: Optional[Dict[int, List[Tuple[float, str, frozenset]]]] = None
        self._indexed_count = 0

    def add_text(self, section: str, text: Any) -> None:
        n = norm_text(text)
        if not n.strip():
            return
        self.section_text.setdefault(section, []).append(n)
        raw = str(text)
        spans = sorted(doc_signals.amounts_in_text(raw), key=lambda t: t[1])
        for i, (amount, _s, _e) in enumerate(spans):
            # Role from the text preceding the amount (same detector as pages).
            window_norm = norm_text(doc_signals.classification_window(raw, spans, i))
            kind, _sev, _lab = doc_signals.classify_money_context(window_norm)
            # Ambiguous value wording plays EVERY role its text supports.
            roles = _roles_for_kind(section, kind) | doc_signals.value_role_alternatives(
                window_norm, kind
            )
            self.amounts.append((amount, section, roles))

    def add_amount(self, section: str, amount: Any, roles: Any = None) -> None:
        if amount is None or isinstance(amount, bool):
            return
        try:
            value = float(amount)
        except (TypeError, ValueError):
            return
        self.amounts.append((value, section, frozenset(roles or ())))

    @staticmethod
    def _entry_matches_role(entry_roles: frozenset, role: Optional[str]) -> bool:
        if not role or role == doc_signals.ROLE_UNCERTAIN_MONEY:
            return True  # document role unclear -> any placement accounts for it
        if not entry_roles:
            return True  # report role unknown (free text) -> never a conflict
        return any(doc_signals.roles_compatible(role, r) for r in entry_roles)

    def _candidates(self, amount: float) -> List[Tuple[float, str, frozenset]]:
        """Amount-equal entries via the cent-keyed index (one linear pass total).

        The absolute tolerance (0.011) spans just over one cent, so checking
        the neighbouring cent buckets covers every _approx_equal match.
        """
        if self._index is None or self._indexed_count != len(self.amounts):
            index: Dict[int, List[Tuple[float, str, frozenset]]] = {}
            for entry in self.amounts:
                index.setdefault(round(entry[0] * 100), []).append(entry)
            self._index = index
            self._indexed_count = len(self.amounts)
        key = round(amount * 100)
        out: List[Tuple[float, str, frozenset]] = []
        for k in (key - 1, key, key + 1):
            for entry in self._index.get(k, ()):  # type: ignore[union-attr]
                if _approx_equal(entry[0], amount):
                    out.append(entry)
        return out

    def lookup_amount(
        self, amount: float, role: Optional[str] = None,
        role_alternatives: Any = (),
    ) -> Tuple[List[str], List[str], frozenset]:
        """(match_sections, conflict_sections, conflict_roles) in ONE pass.

        ``role_alternatives`` are other roles the DOCUMENT wording equally
        supports (doc_signals.value_role_alternatives): a placement under any
        supported reading is a match, never a conflict. conflict_* describe
        entries where the amount exists but only under an incompatible role."""
        acceptable = [role] + [r for r in role_alternatives or () if r]
        matches: set = set()
        conflict_secs: set = set()
        conflict_roles: set = set()
        for _val, sec, entry_roles in self._candidates(amount):
            if any(self._entry_matches_role(entry_roles, r) for r in acceptable):
                matches.add(sec)
            else:
                conflict_secs.add(sec)
                conflict_roles.update(entry_roles)
        conflict_secs -= matches
        return sorted(matches), sorted(conflict_secs), frozenset(conflict_roles)

    def find_amount(self, amount: float, role: Optional[str] = None) -> List[str]:
        return self.lookup_amount(amount, role)[0]

    def find_text(self, needle: Any) -> List[str]:
        n = norm_text(needle).strip()
        if len(n) < 3:
            return []
        return sorted(
            sec for sec, texts in self.section_text.items()
            if any(n in t for t in texts)
        )

    def find_tokens(self, tokens: List[str]) -> List[str]:
        toks = [norm_text(t) for t in tokens or [] if str(t).strip()]
        if not toks:
            return []
        return sorted(
            sec for sec, texts in self.section_text.items()
            if any(any(tok in t for tok in toks) for t in texts)
        )

    def find_token_overlap(self, text: Any, threshold: float = 0.6) -> List[str]:
        """Sections containing >= threshold of the content words of ``text``."""
        words = [w for w in re.findall(r"[a-z0-9]{4,}", norm_text(text))]
        if not words:
            return []
        needed = max(1, int(len(words) * threshold))
        out = []
        for sec, texts in self.section_text.items():
            joined = " \n".join(texts)
            hits = sum(1 for w in set(words) if w in joined)
            if hits >= min(needed, len(set(words))):
                out.append(sec)
        return sorted(out)


def _roles_for_kind(section: str, kind: Optional[str]) -> frozenset:
    """Single role-derivation for a money kind in a report section (used both
    for free-text amounts and structured rows — never forked)."""
    roles = set()
    if kind and kind != "importo_generico":
        roles.add(doc_signals.role_for_kind(kind))
    sec_role = _SECTION_ROLES.get(section)
    if sec_role:
        roles.add(sec_role)
    return frozenset(roles)


def _row_roles(section: str, node: Dict[str, Any]) -> frozenset:
    """Money roles a report row plays: label semantics + contract roles + section."""
    label = node.get("label") or node.get("area")
    label_kind = doc_signals.label_kind(label) if label else None
    roles = set(_roles_for_kind(section, label_kind))
    # A row keeping the document's ambiguous value wording plays every role
    # that wording supports (same rule as the page-side signals).
    roles |= doc_signals.value_role_alternatives(norm_text(label), label_kind)
    for tok in node.get("roles") or []:
        mapped = _ROW_ROLE_TOKENS.get(str(tok))
        if mapped:
            roles.add(mapped)
    if node.get("included_in_valuation"):
        roles.add(doc_signals.ROLE_BUYER_SIDE_COST)
    return frozenset(roles)


def _walk(pool: ReportPool, node: Any, path: str) -> None:
    if isinstance(node, dict):
        section_id = node.get("section_id")
        for key, value in node.items():
            if key in _SKIP_KEYS:
                continue
            child = f"{path}.{key}" if path else str(key)
            # risk_sections items keep their section_id so "da_verificare" items
            # are classified as uncertainty, not as confirmed output.
            if path.startswith("risk_sections") and section_id:
                child = f"risk_sections.{section_id}"
            if key in ("amount", "value", "cost") and isinstance(value, (int, float)):
                section = _top_section(child)
                if key == "cost":
                    # Compliance/risk card costs are regularization estimates.
                    roles = frozenset({doc_signals.ROLE_REGULARIZATION_COST})
                else:
                    roles = _row_roles(section, node)
                pool.add_amount(section, value, roles)
                continue
            _walk(pool, value, child)
    elif isinstance(node, list):
        for item in node:
            _walk(pool, item, path)
    elif isinstance(node, str):
        pool.add_text(_top_section(path), node)
    elif isinstance(node, (int, float)) and not isinstance(node, bool):
        pool.add_amount(_top_section(path), node)


def _top_section(path: str) -> str:
    parts = path.split(".")
    if not parts:
        return path
    if parts[0] == "money_sections" and len(parts) > 1:
        return f"money_sections.{parts[1]}"
    if parts[0] == "risk_sections" and len(parts) > 1 and parts[1] == "da_verificare":
        return "risk_sections.da_verificare"
    return parts[0]


def build_report_pool(customer_report: Dict[str, Any]) -> ReportPool:
    pool = ReportPool()
    _walk(pool, customer_report or {}, "")
    return pool


def _classify_sections(sections: List[str]) -> Tuple[str, str]:
    """(action, report_location) for the sections where a fact was found."""
    main = [s for s in sections if s in _MAIN_SECTIONS or s.split(".")[0] in _MAIN_SECTIONS]
    # risk_sections.<other ids> are confirmed output too.
    main += [
        s for s in sections
        if s.startswith("risk_sections") and s not in _UNCERTAIN_SECTIONS
    ]
    uncertain = [s for s in sections if s in _UNCERTAIN_SECTIONS]
    manual = [s for s in sections if s in _MANUAL_SECTIONS]
    if main:
        return ACTION_INCLUDED, ", ".join(sorted(set(main)))
    if uncertain:
        return ACTION_UNCERTAINTY, ", ".join(sorted(set(uncertain)))
    if manual:
        return ACTION_MANUAL_REVIEW, ", ".join(sorted(set(manual)))
    return "", ""


# ---------------------------------------------------------------------------
# Worksheet fact enumeration (what the analyst captured must not be dropped)
# ---------------------------------------------------------------------------
def _worksheet_facts(worksheet: Dict[str, Any]) -> List[Dict[str, Any]]:
    facts: List[Dict[str, Any]] = []
    ws = worksheet or {}

    def add(fact_id: str, category: str, document_fact: str, ev: Any, severity: str,
            check_kind: str, check_value: Any, expect: Optional[str] = None,
            fallback_tokens: Optional[List[str]] = None,
            preset_reason: Optional[str] = None,
            role: Optional[str] = None) -> None:
        if document_fact is None or str(document_fact).strip() == "":
            return
        facts.append({
            "fact_id": fact_id,
            "category": category,
            "document_fact": str(document_fact),
            "evidence_pages": _pages_list(ev),
            "severity": severity,
            "source": "worksheet",
            "check_kind": check_kind,   # 'text' | 'amount' | 'tokens' | 'overlap'
            "check_value": check_value,
            "expected_action": expect,  # e.g. uncertainty for uncertain_money rows
            "fallback_tokens": fallback_tokens,
            "preset_reason": preset_reason,
            "role": role,               # money role for role-aware amount checks
        })

    ci = ws.get("case_identity") or {}
    ev = ci.get("evidence_pages")
    for field, label in (
        ("tribunale", "Tribunale"), ("procedura_rge", "Procedura / RGE"),
        ("address", "Indirizzo"),
        ("property_type", "Tipologia"), ("ownership_right", "Diritto"),
    ):
        value = ci.get(field)
        if value:
            add(f"identity.{field}", "case_identity", f"{label}: {value}", ev,
                SEV_CRITICAL, "text", value)
    if ci.get("lotto"):
        # Lot ids are often bare digits ("1"): match the normalized lot token,
        # never a raw substring search.
        from . import lots as _lots_mod
        norm_lot = _lots_mod.normalize_lot_token(ci["lotto"]) or str(ci["lotto"])
        add("identity.lotto", "case_identity", f"Lotto: {ci['lotto']}", ev,
            SEV_CRITICAL, "tokens",
            [f"lotto {norm_lot}", f"lotto {str(ci['lotto']).strip()}", str(ci["lotto"])
             if len(str(ci["lotto"]).strip()) >= 3 else f"lotto {norm_lot}"])

    for i, lot in enumerate(ws.get("lots") or []):
        lid = lot.get("lot_id")
        if lid is not None:
            # Lot ids may arrive already prefixed ("Lotto Unico"): normalize
            # first so the search token is never "lotto lotto unico".
            from . import lots as _lots_mod
            norm_lid = _lots_mod.normalize_lot_token(lid) or str(lid).strip()
            add(f"lots[{i}].lot_id", "lot_bene", f"Lotto {lid}",
                lot.get("evidence_pages"), SEV_CRITICAL, "tokens",
                [f"lotto {norm_lid}", f"lotto {str(lid).strip()}",
                 str(lot.get('label') or '')])

    oc = ws.get("occupancy") or {}
    oev = oc.get("evidence_pages")
    if oc.get("status"):
        add("occupancy.status", "occupancy", f"Stato di occupazione: {oc['status']}",
            oev, SEV_CRITICAL, "text", oc["status"])
    if oc.get("title_info"):
        add("occupancy.title_info", "occupancy", f"Titolo di occupazione: {oc['title_info']}",
            oev, SEV_IMPORTANT, "overlap", oc["title_info"])
    if oc.get("opponibility"):
        add("occupancy.opponibility", "occupancy", f"Opponibilità: {oc['opponibility']}",
            oev, SEV_IMPORTANT, "overlap", oc["opponibility"])
    for i, d in enumerate(oc.get("registration_dates") or []):
        add(f"occupancy.registration[{i}]", "occupancy", f"Registrazione contratto: {d}",
            oev, SEV_IMPORTANT, "text", d)
    for i, d in enumerate(oc.get("expiry_dates") or []):
        add(f"occupancy.expiry[{i}]", "occupancy", f"Scadenza contratto: {d}",
            oev, SEV_IMPORTANT, "text", d)
    for i, risk in enumerate(oc.get("risks") or []):
        add(f"occupancy.risks[{i}]", "occupancy", f"Rischio occupazione: {risk}",
            oev, SEV_IMPORTANT, "overlap", risk)

    for i, item in enumerate(ws.get("technical_compliance") or []):
        area = item.get("area")
        cls = item.get("classification")
        sev = SEV_CRITICAL if cls in ("non_conforming", "not_regularizable") else SEV_IMPORTANT
        add(f"technical_compliance[{i}]", "compliance",
            f"{area}: {cls}" + (f" (costo {item.get('cost')})" if item.get("cost") else ""),
            item.get("evidence_pages"), sev, "tokens", [area])
        if item.get("cost"):
            add(f"technical_compliance[{i}].cost", "money",
                f"Costo {area}: {item.get('cost')}", item.get("evidence_pages"),
                SEV_IMPORTANT, "amount", item.get("cost"),
                role=doc_signals.ROLE_REGULARIZATION_COST)

    money = ws.get("money") or {}
    mev = money.get("evidence_pages")
    for field, label, role in (
        ("market_value", "Valore di mercato", doc_signals.ROLE_MARKET_VALUE),
        ("current_state_value", "Valore nello stato di fatto", doc_signals.ROLE_STATE_OF_FACT_VALUE),
        ("sale_value", "Valore di vendita giudiziaria", doc_signals.ROLE_JUDICIAL_SALE_VALUE),
        ("regularization_costs", "Costi di regolarizzazione", doc_signals.ROLE_REGULARIZATION_COST),
        ("cancellation_costs", "Costi di cancellazione formalità", doc_signals.ROLE_BUYER_SIDE_COST),
    ):
        if money.get(field) is not None and money.get(field) != 0:
            add(f"money.{field}", "money", f"{label}: {money[field]}", mev,
                SEV_CRITICAL, "amount", money[field], role=role)
    at = money.get("auction_terms") or {}
    for field, label, role in (
        ("prezzo_base_asta", "Prezzo base d'asta", doc_signals.ROLE_AUCTION_BASE_PRICE),
        ("offerta_minima", "Offerta minima", doc_signals.ROLE_MINIMUM_BID),
        ("rialzo_minimo", "Rialzo minimo", doc_signals.ROLE_AUCTION_INCREMENT),
        ("cauzione", "Cauzione", doc_signals.ROLE_AUCTION_DEPOSIT),
    ):
        if at.get(field):
            add(f"money.auction_terms.{field}", "sale_terms", f"{label}: {at[field]}",
                at.get("evidence_pages") or mev, SEV_CRITICAL, "amount", at[field],
                role=role)
    for coll, label, sev, role in (
        ("deductions", "Deprezzamento/deduzione", SEV_IMPORTANT,
         doc_signals.ROLE_DEPRECIATION),
        ("buyer_side_costs", "Costo a carico acquirente", SEV_CRITICAL,
         doc_signals.ROLE_BUYER_SIDE_COST),
        ("procedure_cancelled_costs", "Formalità cancellata dalla procedura", SEV_IMPORTANT,
         doc_signals.ROLE_PROCEDURE_CANCELLED_FORMALITY),
    ):
        for i, row in enumerate(money.get(coll) or []):
            amt = row.get("amount")
            if amt:
                add(f"money.{coll}[{i}]", "money", f"{label}: {row.get('label')} = {amt}",
                    row.get("evidence_pages"), sev, "amount", amt, role=role)
            elif row.get("label"):
                zero_reason = None
                if amt == 0:
                    zero_reason = (
                        "Importo pari a zero dichiarato in perizia (nessun costo "
                        "applicato): non è un costo da esporre."
                    )
                add(f"money.{coll}[{i}]", "money" if coll != "procedure_cancelled_costs" else "formalities",
                    f"{label}: {row.get('label')}", row.get("evidence_pages"), SEV_USEFUL,
                    "overlap", row.get("label"), preset_reason=zero_reason)
    for i, row in enumerate(money.get("uncertain_money") or []):
        amt = row.get("amount")
        if amt:
            # A row the analyst could not role-classify may still have a CLEAR
            # role from its own label (comparables, rendita, canone, spese
            # condominiali, capitale di formalità): those render as background/
            # context sections, not as scary "importi da verificare". The kind
            # sets are shared with the renderer (doc_signals) by construction.
            label_kind = doc_signals.label_kind(row.get("label"))
            contextual = label_kind in (
                doc_signals.COMPARATIVE_LABEL_KINDS | doc_signals.CONTEXT_LABEL_KINDS
            )
            add(f"money.uncertain_money[{i}]", "money",
                f"Importo incerto: {row.get('label')} = {amt}",
                row.get("evidence_pages"), SEV_CRITICAL, "amount", amt,
                expect=None if contextual else ACTION_UNCERTAINTY,
                role=doc_signals.role_for_kind(label_kind) if contextual else None)

    for i, item in enumerate(ws.get("legal_formalities") or []):
        add(f"legal_formalities[{i}]", "formalities",
            f"{item.get('type')}: {str(item.get('description') or '')[:140]}",
            item.get("evidence_pages"), SEV_IMPORTANT, "tokens",
            [item.get("type"), "formalit", "cancellazion"])
        if item.get("amount"):
            add(f"legal_formalities[{i}].amount", "formalities",
                f"Importo formalità {item.get('type')}: {item.get('amount')}",
                item.get("evidence_pages"), SEV_USEFUL, "amount", item.get("amount"),
                role=doc_signals.ROLE_PROCEDURE_CANCELLED_FORMALITY)

    for i, item in enumerate(ws.get("risk_classification") or []):
        # A generic risk card deduped into the same-area detailed compliance
        # card is still covered: fall back to matching the area's word stems
        # ("difformità edilizie" ~ "conformità edilizia" via 'ediliz').
        area_stems = [
            w[:6] for w in re.findall(r"[a-z]{5,}", norm_text(item.get("area")))
        ] or [item.get("area")]
        add(f"risk_classification[{i}]", "other",
            f"Rischio ({item.get('area')}): {item.get('summary')}",
            item.get("evidence_pages"), SEV_IMPORTANT, "overlap",
            f"{item.get('area')} {item.get('summary')}",
            fallback_tokens=area_stems)

    for i, w in enumerate(ws.get("warnings") or []):
        add(f"warnings[{i}]", "uncertainty", f"Avvertenza analisi: {w.get('text')}",
            w.get("evidence_pages"), SEV_USEFUL, "overlap", w.get("text"),
            expect=ACTION_MANUAL_REVIEW)
    for i, t in enumerate(ws.get("missing_or_uncertain") or []):
        add(f"missing_or_uncertain[{i}]", "uncertainty", f"Dato mancante/incerto: {t}",
            None, SEV_USEFUL, "overlap", t, expect=ACTION_MANUAL_REVIEW)

    return facts


# ---------------------------------------------------------------------------
# Fact evaluation against the report pool
# ---------------------------------------------------------------------------
def _confirmation_role_candidates(doc_role: str, conflict_roles: Any) -> List[str]:
    """Distinct candidate roles a customer could confirm for one amount.

    The document-detected role first, then the CORE roles the report actually
    placed the same amount under. These are the ">=2 real interpretations tied
    to a specific amount+page" that make a money ambiguity customer-resolvable
    (money_confirmation.py builds the prompt from them). Background/uncertain
    roles never seed a confirmation — they are already safe placements."""
    candidates: List[str] = []
    for role in [doc_role, *(conflict_roles or ())]:
        if not role:
            continue
        if role not in doc_signals.CORE_MONEY_ROLES:
            continue
        if role not in candidates:
            candidates.append(role)
    return candidates


def _role_conflict_update(
    out: Dict[str, Any], role: str, conflicts: List[str], conflict_roles: Any
) -> Dict[str, Any]:
    """Amount present in the report but ONLY under an incompatible money role.

    Tiers (shared taxonomy in doc_signals):
      * MISLEADING (a CORE value/cost role on either side): CONTRADICTED —
        blocks when the document fact is critical, warns otherwise.
      * Background-vs-background: the same economic fact shown in a different
        safe context bucket -> PARTIAL, visibly noted, never silently covered.
    """
    role_label = doc_signals.ROLE_LABELS_IT.get(role, role)
    misleading = doc_signals.conflict_is_misleading(role, conflict_roles)
    if misleading:
        candidates = _confirmation_role_candidates(role, conflict_roles)
        out.update({
            "match_status": CONTRADICTED,
            "report_location": ", ".join(conflicts),
            "action": "",
            "role_conflict": True,
            # >=2 candidate roles make this a customer-resolvable ambiguity.
            "confirmation_roles": candidates if len(candidates) >= 2 else [],
            "software_output": (
                f"Importo presente nel report ma con un ruolo diverso ({', '.join(conflicts)})"
            ),
            "reason": (
                f"Il documento indica questo importo come '{role_label}', ma il "
                "report lo espone con un ruolo che può fuorviare sul significato "
                "economico: importo E ruolo devono coincidere."
            ),
        })
        return out
    out.update({
        "match_status": PARTIAL,
        "report_location": ", ".join(conflicts),
        "action": ACTION_BACKGROUND,
        "role_conflict": True,
        "software_output": (
            f"Importo presente in una sezione di contesto diversa ({', '.join(conflicts)})"
        ),
        "reason": (
            f"Stesso dato economico ('{role_label}') esposto in una sezione di "
            "contesto diversa: nessun costo o valore viene attribuito in modo "
            "fuorviante."
        ),
    })
    return out


def _evaluate_fact(fact: Dict[str, Any], pool: ReportPool) -> Dict[str, Any]:
    kind = fact["check_kind"]
    value = fact["check_value"]
    role = fact.get("role")
    role_conflicts: List[str] = []
    conflict_roles: Any = frozenset()
    if kind == "amount":
        try:
            amount = float(value)
        except (TypeError, ValueError):
            sections = []
        else:
            sections, conflicts, conflict_roles = pool.lookup_amount(amount, role)
            if role:
                role_conflicts = conflicts
    elif kind == "tokens":
        sections = pool.find_tokens([v for v in (value or []) if v])
    elif kind == "overlap":
        sections = pool.find_token_overlap(value)
    else:
        sections = pool.find_text(value)
        if not sections:
            sections = pool.find_token_overlap(value, threshold=0.75)
    via_fallback = False
    if not sections and fact.get("fallback_tokens"):
        sections = pool.find_tokens([t for t in fact["fallback_tokens"] if t])
        via_fallback = bool(sections)
    action, location = _classify_sections(sections)

    out = dict(fact)
    out.pop("check_kind", None)
    out.pop("check_value", None)
    out.pop("fallback_tokens", None)
    preset_reason = out.pop("preset_reason", None)
    expected = out.pop("expected_action", None)
    if not action and role_conflicts:
        return _role_conflict_update(out, role, role_conflicts, conflict_roles)
    # An uncertainty/manual-review match never launders a MISLEADING role
    # placement in the main report body (see _evaluate_money_signal).
    if action in (ACTION_UNCERTAINTY, ACTION_MANUAL_REVIEW) and role_conflicts and (
        doc_signals.conflict_is_misleading(role, conflict_roles)
    ):
        return _role_conflict_update(out, role, role_conflicts, conflict_roles)
    if via_fallback and action:
        out.update({
            "match_status": PARTIAL,
            "report_location": location,
            "action": action,
            "software_output": f"Coperto tramite la sezione dell'area corrispondente ({location})",
            "reason": (
                "Il contenuto è rappresentato dalla scheda dedicata alla stessa "
                "area nel report."
            ),
        })
        return out

    if action:
        out["match_status"] = MATCH
        out["report_location"] = location
        out["action"] = action
        out["software_output"] = f"Presente nel report ({location})"
        out["reason"] = ""
        if expected == ACTION_UNCERTAINTY and action == ACTION_INCLUDED:
            # An uncertain amount surfacing as confirmed output is a problem.
            uncertain_hit = any(s in _UNCERTAIN_SECTIONS for s in sections)
            if not uncertain_hit:
                out["match_status"] = CONTRADICTED
                out["reason"] = (
                    "Importo indicato come incerto dal worksheet ma reso come "
                    "dato confermato nel report."
                )
    else:
        out["match_status"] = MISSING
        out["report_location"] = ""
        out["action"] = ""
        out["software_output"] = "Non trovato nel report"
        out["reason"] = preset_reason or ""
    return out


def _finalize_missing(fact: Dict[str, Any]) -> Dict[str, Any]:
    """Assign the final action/reason for a fact the report does not carry."""
    severity = fact.get("severity")
    if severity in (SEV_CRITICAL,):
        fact["action"] = ACTION_BLOCKED
        fact["reason"] = fact.get("reason") or (
            "Fatto materiale non riscontrato in alcuna sezione del report: "
            "omissione critica."
        )
    elif severity == SEV_IMPORTANT:
        fact["action"] = ACTION_BLOCKED
        fact["reason"] = fact.get("reason") or (
            "Fatto importante non riscontrato nel report."
        )
    elif severity == SEV_BACKGROUND:
        fact["action"] = ACTION_EXCLUDED
        fact["reason"] = fact.get("reason") or (
            "Dato di contesto (non è un costo né un valore di vendita); "
            "escluso dal report cliente."
        )
    else:
        fact["action"] = ACTION_EXCLUDED
        fact["reason"] = fact.get("reason") or (
            "Dato utile ma non essenziale, non riscontrato nel report."
        )
    return fact


# ---------------------------------------------------------------------------
# Page signals evaluation (document -> report)
# ---------------------------------------------------------------------------
def _evaluate_money_signal(
    sig: Dict[str, Any], pool: ReportPool, role_covered: Dict[str, bool],
    confirmations: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    role = sig.get("role") or doc_signals.role_for_kind(sig.get("kind"))
    alternatives = list(sig.get("role_alternatives") or ())
    # Customer-confirmed ground truth (human-in-the-loop money confirmation):
    # once the customer confirms which reading of this amount is correct, that
    # role becomes an ACCEPTED placement, so a report row under the confirmed
    # role is a match and the previous role conflict is resolved. Confirming a
    # role the report does NOT carry leaves the block in place (fail-closed).
    confirmed_role = (confirmations or {}).get(sig["signal_id"])
    if confirmed_role and confirmed_role not in alternatives and confirmed_role != role:
        alternatives.append(confirmed_role)
    sections, role_conflicts, conflict_roles = pool.lookup_amount(
        sig["amount"], role, alternatives
    )
    action, location = _classify_sections(sections)
    fact = {
        "fact_id": sig["signal_id"],
        "category": sig["category"],
        "document_fact": f"{sig['label']}: {sig['amount']}",
        "amount": sig["amount"],
        "evidence_pages": [sig["page"]],
        "severity": sig["severity"],
        "source": "page_money",
        "role": role,
        "snippet": sig.get("snippet"),
    }
    # A match that lives ONLY in uncertainty/manual-review sections never
    # launders a MISLEADING role placement in the main report body: the
    # customer still sees the wrong role prominently, so the conflict wins.
    if action in (ACTION_UNCERTAINTY, ACTION_MANUAL_REVIEW) and role_conflicts and (
        doc_signals.conflict_is_misleading(role, conflict_roles)
    ):
        return _role_conflict_update(fact, role, role_conflicts, conflict_roles)
    if action:
        fact.update({
            "match_status": MATCH, "action": action, "report_location": location,
            "software_output": f"Importo presente nel report ({location})",
            "reason": "",
        })
        return fact
    # Amount present in the report but ONLY under an incompatible role: never
    # counted as covered (misleading conflicts contradict, safe-bucket
    # differences stay PARTIAL — see _role_conflict_update).
    if role_conflicts:
        return _role_conflict_update(fact, role, role_conflicts, conflict_roles)
    # Amount not in report. If the same money concept IS covered with another
    # (the perito's final) amount, this is a detail line, not a silent loss.
    if role_covered.get(role):
        fact.update({
            "match_status": PARTIAL, "action": ACTION_BACKGROUND, "report_location": "",
            "software_output": "Il report riporta il valore finale del perito per questa grandezza",
            "reason": (
                "Importo di dettaglio/parziale della stessa grandezza; il valore "
                "conclusivo della perizia è riportato nel report."
            ),
        })
        return fact
    if sig["severity"] == SEV_BACKGROUND:
        fact.update({
            "match_status": MISSING, "action": ACTION_EXCLUDED, "report_location": "",
            "software_output": "Non riportato",
            "reason": (
                "Importo descrittivo di contesto (es. capitale di una formalità), "
                "non un costo o valore a carico dell'acquirente."
            ),
        })
        return fact
    if sig["kind"] == "importo_generico":
        fact.update({
            "match_status": MISSING, "action": ACTION_EXCLUDED, "report_location": "",
            "software_output": "Non riportato",
            "reason": "Importo senza contesto monetario chiaro nella pagina.",
        })
        return fact
    fact.update({
        "match_status": MISSING, "action": "", "report_location": "",
        "software_output": "Non riportato", "reason": "",
    })
    return _finalize_missing(fact)


def _evaluate_topic_signal(sig: Dict[str, Any], pool: ReportPool) -> Dict[str, Any]:
    sections = pool.find_tokens(sig.get("report_tokens") or [])
    action, location = _classify_sections(sections)
    fact = {
        "fact_id": sig["signal_id"],
        "category": sig["category"],
        "document_fact": f"{sig['label']} (pag. {sig['page']})",
        "evidence_pages": [sig["page"]],
        "severity": sig["severity"],
        "source": "page_topic",
        "snippet": sig.get("snippet"),
    }
    if action:
        fact.update({
            "match_status": MATCH, "action": action, "report_location": location,
            "software_output": f"Tema trattato nel report ({location})", "reason": "",
        })
        return fact
    fact.update({
        "match_status": MISSING, "action": "", "report_location": "",
        "software_output": "Tema non trattato nel report", "reason": "",
    })
    return _finalize_missing(fact)


def _money_role_coverage(signals: List[Dict[str, Any]], pool: ReportPool) -> Dict[str, bool]:
    """role -> True if at least one document amount of that role is in the report
    UNDER that same role (role-aware, never amount-only)."""
    covered: Dict[str, bool] = {}
    for sig in signals:
        if sig["signal_type"] != "money":
            continue
        role = sig.get("role") or doc_signals.role_for_kind(sig.get("kind"))
        if covered.get(role):
            continue
        if pool.find_amount(sig["amount"], role):
            covered[role] = True
        else:
            covered.setdefault(role, False)
    return covered


# ---------------------------------------------------------------------------
# Lot-selection mode (multi-lot, no lot chosen): structure + money preservation
# ---------------------------------------------------------------------------
def _selection_facts(
    lot_report: Optional[Dict[str, Any]],
    lot_index: Optional[Dict[str, Any]],
    customer_report: Dict[str, Any],
) -> List[Dict[str, Any]]:
    facts: List[Dict[str, Any]] = []
    # Lot presence is checked STRUCTURALLY on the selector (documents may call
    # their lots "bene", "unità" etc. — the id is the invariant, not the word).
    report_lot_ids = {
        str(l.get("lot_id"))
        for l in (customer_report.get("lot_selection") or {}).get("lots") or []
    }
    report_lot_ids |= {
        str(x) for x in (customer_report.get("lot_structure") or {}).get("lot_ids") or []
    }
    for lid in (lot_report or {}).get("lot_ids") or []:
        present = str(lid) in report_lot_ids
        facts.append({
            "fact_id": f"selection.lot[{lid}]",
            "category": "lot_bene",
            "document_fact": f"Lotto {lid} presente nel documento",
            "evidence_pages": [],
            "severity": SEV_CRITICAL,
            "source": "selection",
            "match_status": MATCH if present else MISSING,
            "action": ACTION_INCLUDED if present else "",
            "report_location": "lot_selection" if present else "",
            "software_output": (
                "Lotto presente nel selettore" if present else "Lotto assente dal selettore"
            ),
            "reason": "" if present else (
                "Lotto rilevato nel documento ma non offerto nel selettore: "
                "perdita di lotto."
            ),
            "_pre_evaluated": True,
        })
    for lot in (lot_index or {}).get("lots") or []:
        lid = lot.get("lot_id")
        money = lot.get("money") or {}
        for field, label, role in (
            ("market_value", "Valore di mercato", doc_signals.ROLE_MARKET_VALUE),
            ("current_state_value", "Valore nello stato di fatto", doc_signals.ROLE_STATE_OF_FACT_VALUE),
            ("sale_value", "Valore di vendita giudiziaria", doc_signals.ROLE_JUDICIAL_SALE_VALUE),
            ("prezzo_base_asta", "Prezzo base d'asta", doc_signals.ROLE_AUCTION_BASE_PRICE),
            ("offerta_minima", "Offerta minima", doc_signals.ROLE_MINIMUM_BID),
            ("cauzione", "Cauzione", doc_signals.ROLE_AUCTION_DEPOSIT),
        ):
            value = money.get(field)
            amount = value.get("amount") if isinstance(value, dict) else value
            if amount:
                facts.append({
                    "fact_id": f"selection.lot[{lid}].{field}",
                    "category": "money",
                    "document_fact": f"Lotto {lid} – {label}: {amount}",
                    "evidence_pages": _pages_list(
                        value.get("evidence_pages") if isinstance(value, dict) else []
                    ),
                    "severity": SEV_CRITICAL,
                    "source": "selection",
                    "check_kind": "amount",
                    "check_value": amount,
                    "expected_action": None,
                    "role": role,
                })
    return facts


# ---------------------------------------------------------------------------
# Public builder
# ---------------------------------------------------------------------------
def build_coverage_audit(
    *,
    analysis_id: str,
    job_id: str,
    pages: List[Dict[str, Any]],
    worksheet: Optional[Dict[str, Any]],
    contract: Optional[Dict[str, Any]],
    customer_report: Dict[str, Any],
    validator_report: Optional[Dict[str, Any]] = None,
    lot_report: Optional[Dict[str, Any]] = None,
    lot_index: Optional[Dict[str, Any]] = None,
    money_confirmations: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Build (coverage_audit, page_by_page_audit) for a rendered report.

    Selection mode (report_status == LOT_SELECTION_REQUIRED) audits lot
    structure + per-lot money preservation; full mode audits everything.

    ``money_confirmations`` maps a money signal's fact_id -> the role the
    customer confirmed as correct (human-in-the-loop disambiguation). A
    confirmed role is treated as an accepted placement, so a previously-blocking
    role ambiguity the customer resolved no longer contradicts.
    """
    pool = build_report_pool(customer_report)
    selection_mode = (customer_report or {}).get("report_status") == "LOT_SELECTION_REQUIRED"

    signals = doc_signals.extract_page_signals(pages or [])
    role_covered = _money_role_coverage(signals, pool)

    fact_coverage: List[Dict[str, Any]] = []

    if selection_mode:
        for fact in _selection_facts(lot_report, lot_index, customer_report):
            if fact.pop("_pre_evaluated", False):
                fact_coverage.append(_maybe_finalize(fact))
            else:
                fact_coverage.append(_maybe_finalize(_evaluate_fact(fact, pool)))
        # In selection mode only VALUE money signals are audited page-side: the
        # detailed analysis happens after a lot is chosen (never blended). A
        # page value that is absent from the SELECTOR is not a loss — it is
        # analyzed in the per-lot report after selection (whose own gate then
        # requires it) — so it defers visibly instead of blocking. Lot-index
        # money preservation (selection facts above) stays CRITICAL.
        for sig in signals:
            if sig["signal_type"] == "money" and sig["kind"] in doc_signals.VALUE_KINDS:
                fact = _evaluate_money_signal(sig, pool, role_covered)
                if fact["match_status"] in (MISSING, CONTRADICTED) and fact.get(
                    "action"
                ) in ("", ACTION_BLOCKED):
                    fact.update({
                        "match_status": PARTIAL,
                        "action": ACTION_BACKGROUND,
                        "software_output": (
                            "Valore analizzato in dettaglio dopo la selezione del lotto"
                        ),
                        "reason": (
                            "Valore di dettaglio presente nel documento: l'analisi "
                            "completa avviene dopo la selezione del lotto (nessun "
                            "dato viene fuso tra lotti)."
                        ),
                    })
                fact_coverage.append(fact)
            else:
                fact_coverage.append({
                    "fact_id": sig["signal_id"],
                    "category": sig["category"],
                    "document_fact": f"{sig['label']} (pag. {sig['page']})",
                    "evidence_pages": [sig["page"]],
                    "severity": sig["severity"],
                    "snippet": sig.get("snippet"),
                    "match_status": UNCLEAR,
                    "action": ACTION_EXCLUDED,
                    "report_location": "lot_selection",
                    "software_output": "Selettore lotti (nessun report fuso)",
                    "reason": (
                        "Analisi di dettaglio disponibile dopo la selezione del "
                        "lotto; nessun dato viene fuso tra lotti."
                    ),
                })
    else:
        for fact in _worksheet_facts(worksheet or {}):
            fact_coverage.append(_maybe_finalize(_evaluate_fact(fact, pool)))
        for sig in signals:
            if sig["signal_type"] == "money":
                fact_coverage.append(
                    _evaluate_money_signal(sig, pool, role_covered, money_confirmations)
                )
            else:
                fact_coverage.append(_evaluate_topic_signal(sig, pool))

    # ------------------------------------------------------------------ rollup
    critical_omissions: List[Dict[str, Any]] = []
    important_warnings: List[Dict[str, Any]] = []
    useful_background: List[Dict[str, Any]] = []

    for fact in fact_coverage:
        problem = fact["match_status"] in (MISSING, CONTRADICTED) and fact["action"] in (
            ACTION_BLOCKED, "",
        )
        if fact["match_status"] == CONTRADICTED:
            # A contradiction blocks when the document fact is critical;
            # weaker facts (e.g. an important background amount shown under a
            # misleading role) surface as explicit warnings.
            if fact["severity"] == SEV_CRITICAL:
                critical_omissions.append(_omission_view(fact))
            else:
                important_warnings.append(_omission_view(fact))
        elif problem and fact["severity"] == SEV_CRITICAL:
            critical_omissions.append(_omission_view(fact))
        elif problem and fact["severity"] == SEV_IMPORTANT:
            important_warnings.append(_omission_view(fact))
        elif fact["match_status"] in (MISSING, PARTIAL) and fact["severity"] in (
            SEV_USEFUL, SEV_BACKGROUND,
        ):
            useful_background.append(_omission_view(fact))

    coverage_status = STATUS_PASS
    if critical_omissions:
        coverage_status = STATUS_FAIL
    elif important_warnings:
        coverage_status = STATUS_WARNING

    page_audit = _build_page_audit(
        analysis_id, job_id, pages, signals, fact_coverage, customer_report
    )

    report_vs_document = [
        {
            "document_text_says": f["document_fact"],
            "software_output_says": f.get("software_output") or "",
            "match": f["match_status"] == MATCH,
            "evidence_pages": f.get("evidence_pages") or [],
            "severity": f["severity"],
            "notes": f.get("reason") or "",
        }
        for f in fact_coverage
        if f["severity"] in (SEV_CRITICAL, SEV_IMPORTANT)
    ]

    audit = {
        "schema_version": COVERAGE_AUDIT_SCHEMA_VERSION,
        "analysis_id": str(analysis_id),
        "job_id": str(job_id),
        "coverage_status": coverage_status,
        "selection_mode": selection_mode,
        "critical_omissions": critical_omissions,
        "important_warnings": important_warnings,
        "useful_background_omissions": useful_background,
        "page_coverage": page_audit.get("page_summary", []),
        "fact_coverage": fact_coverage,
        "report_vs_document_table": report_vs_document,
        "totals": {
            "facts": len(fact_coverage),
            "match": sum(1 for f in fact_coverage if f["match_status"] == MATCH),
            "partial": sum(1 for f in fact_coverage if f["match_status"] == PARTIAL),
            "missing": sum(1 for f in fact_coverage if f["match_status"] == MISSING),
            "contradicted": sum(1 for f in fact_coverage if f["match_status"] == CONTRADICTED),
        },
    }
    return audit, page_audit


def _maybe_finalize(fact: Dict[str, Any]) -> Dict[str, Any]:
    if fact["match_status"] in (MISSING,) and not fact.get("action"):
        return _finalize_missing(fact)
    return fact


def _omission_view(fact: Dict[str, Any]) -> Dict[str, Any]:
    view = {
        "fact_id": fact["fact_id"],
        "category": fact["category"],
        "document_fact": fact["document_fact"],
        "evidence_pages": fact.get("evidence_pages") or [],
        "severity": fact["severity"],
        "match_status": fact["match_status"],
        "reason": fact.get("reason") or "",
    }
    if fact.get("role"):
        view["role"] = fact["role"]
    if fact.get("role_conflict"):
        view["role_conflict"] = True
    # Carried for the money-confirmation builder (customer disambiguation): the
    # amount, its verbatim page excerpt, and the >=2 candidate roles. Present
    # only for resolvable money ambiguities; absent everywhere else.
    if fact.get("amount") is not None:
        view["amount"] = fact["amount"]
    if fact.get("snippet"):
        view["snippet"] = fact["snippet"]
    if fact.get("confirmation_roles"):
        view["confirmation_roles"] = list(fact["confirmation_roles"])
    return view


# ---------------------------------------------------------------------------
# Page-by-page audit table
# ---------------------------------------------------------------------------
def _esito_for(fact: Dict[str, Any]) -> str:
    status = fact["match_status"]
    action = fact.get("action")
    if status == MATCH:
        if action == ACTION_UNCERTAINTY or action == ACTION_MANUAL_REVIEW:
            return ESITO_DA_VERIFICARE
        return ESITO_COPERTO
    if status == PARTIAL:
        return ESITO_PARZIALE
    if status == CONTRADICTED:
        return ESITO_MANCANTE
    if action in (ACTION_EXCLUDED, ACTION_BACKGROUND):
        return ESITO_NON_MATERIALE
    if status == UNCLEAR:
        return ESITO_NON_MATERIALE
    return ESITO_MANCANTE


def _build_page_audit(
    analysis_id: str,
    job_id: str,
    pages: List[Dict[str, Any]],
    signals: List[Dict[str, Any]],
    fact_coverage: List[Dict[str, Any]],
    customer_report: Dict[str, Any],
) -> Dict[str, Any]:
    facts_by_id = {f["fact_id"]: f for f in fact_coverage}
    rows: List[Dict[str, Any]] = []
    per_page: Dict[int, Dict[str, int]] = {}

    for sig in signals:
        fact = facts_by_id.get(sig["signal_id"])
        if fact is None:
            continue
        esito = _esito_for(fact)
        page = sig["page"]
        counters = per_page.setdefault(
            page, {"totale": 0, "coperti": 0, "parziali": 0, "mancanti": 0,
                   "da_verificare": 0, "non_materiali": 0},
        )
        counters["totale"] += 1
        counters[{
            ESITO_COPERTO: "coperti", ESITO_PARZIALE: "parziali",
            ESITO_MANCANTE: "mancanti", ESITO_DA_VERIFICARE: "da_verificare",
            ESITO_NON_MATERIALE: "non_materiali",
        }[esito]] += 1
        dato = fact["document_fact"]
        if sig.get("amount") is not None:
            dato = f"{sig['label']}: € {sig['amount']:,.2f}".replace(
                ",", "|").replace(".", ",").replace("|", ".")
        row = {
            "page": page,
            "dato_perizia": dato,
            "snippet": sig.get("snippet") or "",
            "presente_nel_report": fact["match_status"] in (MATCH, PARTIAL),
            "report_location": fact.get("report_location") or "",
            "esito": esito,
            "severity": fact["severity"],
            "note": fact.get("reason") or "",
        }
        if fact.get("role"):
            row["ruolo"] = fact["role"]
            row["ruolo_label"] = doc_signals.ROLE_LABELS_IT.get(fact["role"], fact["role"])
        rows.append(row)

    page_summary = []
    known_pages = sorted(per_page)
    all_pages = sorted(
        {p for p in (doc_signals.page_number(pg) for pg in pages or []) if p is not None}
    )
    for page in all_pages:
        counters = per_page.get(page)
        if counters is None:
            page_summary.append({
                "page": page, "totale": 0, "coperti": 0, "parziali": 0,
                "mancanti": 0, "da_verificare": 0, "non_materiali": 0,
                "status": "nessun_dato_materiale",
            })
            continue
        status = "coperta"
        if counters["mancanti"]:
            status = "con_omissioni"
        elif counters["parziali"] or counters["da_verificare"]:
            status = "parziale"
        page_summary.append({"page": page, **counters, "status": status})

    rows.sort(key=lambda r: (r["page"], r["esito"] != ESITO_MANCANTE, r["dato_perizia"]))
    return {
        "schema_version": PAGE_AUDIT_SCHEMA_VERSION,
        "analysis_id": str(analysis_id),
        "job_id": str(job_id),
        "title": "Controllo qualità pagina per pagina",
        "columns": ["Pagina", "Dato rilevante nella perizia", "Presente nel report", "Esito", "Note"],
        "rows": rows,
        "page_summary": page_summary,
        "known_pages": known_pages,
    }
