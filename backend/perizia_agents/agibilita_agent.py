from __future__ import annotations

from dataclasses import asdict, dataclass, field
import re
from collections import defaultdict
import uuid

from perizia_runtime.state import CanonicalIssue, EvidenceSpan, RuntimeState
from perizia_tools.evidence_span_tool import make_evidence


_NEGATIVE_PATTERNS = [
    "non risulta rilasciato il certificato di agibilità",
    "non risulta rilasciato il certificato di agibilita",
    "agibilità assente",
    "agibilita assente",
    "abitabilità assente",
    "abitabilita assente",
    "non è presente l'abitabilità",
    "non e presente l'abitabilita",
    "non è presente l'agibilità",
    "non e presente l'agibilita",
    "non risulta agibile",
    "non risultano agibili",
    "non agibile",
]

_POSITIVE_PATTERNS = [
    "agibilità rilasciata",
    "agibilita rilasciata",
    "immobile risulta agibile",
    "l'immobile risulta agibile",
    "risultano agibili",
    "risulta agibile",
    "agibile",
]

_CONSTRUCTION_NEGATIVE_PATTERNS = [
    "in costruzione",
    "in corso di costruzione",
    "lavori sospesi",
    "non ancora ultimato",
    "completamente al grezzo",
    "realizzato al grezzo",
]

_LOT_REGEX = re.compile(r"\blotto\s*n?[°º]?\s*(unico|\d+)\b", re.IGNORECASE)
_BENE_REGEX = re.compile(r"\bbene\s*n[°º.]?\s*(\d+)\b", re.IGNORECASE)
_UNIVERSAL_BENI_RE = re.compile(r"\btutti\s+i\s+beni\b", re.IGNORECASE)
_UNIVERSAL_LOTTI_RE = re.compile(r"\btutti\s+i\s+lotti\b", re.IGNORECASE)
_INTERO_LOTTO_RE = re.compile(r"\bl[' ]intero\s+lotto\b|\bintero\s+lotto\b", re.IGNORECASE)
_SECTION_LABEL_RE = re.compile(r"regolarità edilizia|regolarita edilizia|pratiche edilizie|agibilit|abitabilit", re.IGNORECASE)

_TIER1_NEGATIVE_PATTERNS = {
    "non risulta rilasciato il certificato di agibilità",
    "non risulta rilasciato il certificato di agibilita",
    "agibilità assente",
    "agibilita assente",
    "abitabilità assente",
    "abitabilita assente",
    "non è presente l'abitabilità",
    "non e presente l'abitabilita",
    "non è presente l'agibilità",
    "non e presente l'agibilita",
}

_TIER2_NEGATIVE_PATTERNS = {
    "non risulta agibile",
    "non risultano agibili",
    "non agibile",
}

_TIER3_POSITIVE_PATTERNS = {
    "immobile risulta agibile",
    "l'immobile risulta agibile",
    "risultano agibili",
    "risulta agibile",
    "agibilità rilasciata",
    "agibilita rilasciata",
}

_OWNERSHIP_STRENGTH = {
    "DIRECT_ASSIGNMENT": 5,
    "WEAK_ASSIGNMENT": 3,
    "INHERITANCE": 1,
    "UNRESOLVED": 0,
    "explicit_bene_quote": 5,
    "explicit_lotto_quote": 5,
    "active_bene_section": 4,
    "active_lotto_universal": 3,
    "page_unique_bene": 4,
    "page_unique_lotto": 4,
    "single_scope_document": 3,
    "single_lotto_document": 3,
    "scope_inheritance": 1,
    "unresolved_scope": 0,
}

_SIGNAL_WEIGHTS = {
    "EXPLICIT_SCOPE_REF": 0.95,
    "TABLE_CELL_SCOPE": 0.85,
    "SECTION_HEADING_BENE": 0.80,
    "INLINE_REFERENCE": 0.75,
    "UNIVERSAL_MARKER": 0.90,
    "SECTION_HEADING_LOTTO": 0.60,
    "PROXIMITY_STICKY": 0.50,
    "DEFAULT_ROOT": 0.20,
}


@dataclass
class OwnershipSignal:
    signal_type: str
    target_scope: str
    score: float
    detail: str = ""


@dataclass
class OwnershipDecision:
    evidence_id: str
    raw_text: str
    page: int
    scope_candidates: list[dict] = field(default_factory=list)
    winning_scope: str | None = None
    confidence: str | None = None
    ownership_method: str = "UNRESOLVED"
    competing_signals: list[dict] = field(default_factory=list)
    unresolved_reason: str | None = None
    inherited: bool = False
    inherited_from_scope_id: str | None = None
    inheritance_reason: str | None = None


def _pattern_hits(text: str, patterns: list[str]) -> list[tuple[int, str]]:
    hits: list[tuple[int, str]] = []
    for pattern in patterns:
        start = 0
        while True:
            idx = text.find(pattern, start)
            if idx == -1:
                break
            hits.append((idx, pattern))
            start = idx + len(pattern)
    hits.sort(key=lambda item: item[0])
    return hits


def _lot_label_from_quote(quote: str) -> str | None:
    match = _LOT_REGEX.search(quote)
    if not match:
        return None
    token = str(match.group(1)).lower()
    return f"Lotto {'Unico' if token == 'unico' else token}"


def _negative_issue_title(quote: str) -> str:
    lot_label = _lot_label_from_quote(quote)
    if lot_label:
        return f"{lot_label}: Non agibile"
    if "lavori sospesi" in quote.lower() or "in costruzione" in quote.lower():
        return "Fabbricato in costruzione / lavori sospesi"
    return "Agibilità assente / non rilasciata"


def _build_negative_agibilita_issue(
    page_number: int,
    quote: str,
    confidence: float,
    reason: str,
    *,
    metadata: dict | None = None,
) -> CanonicalIssue:
    return CanonicalIssue(
        code="AGIBILITA_NEGATIVE",
        title_it=_negative_issue_title(quote),
        severity="RED",
        category="agibilita",
        priority_score=82.0 if reason != "construction_negative" else 76.0,
        evidence=[make_evidence(page_number, quote, "agibilita", ["agibilita"], confidence)],
        summary_it="L'agibilità risulta assente o non rilasciata e richiede verifica immediata prima dell'offerta.",
        action_it="Verifica titoli edilizi, agibilità/abitabilità e costi necessari per la regolarizzazione.",
        metadata={"reason": reason, **(metadata or {})},
    )


def _page_scope_ids(state: RuntimeState, page_number: int, scope_type: str) -> list[str]:
    out = []
    for scope_id, scope in state.scopes.items():
        if scope.scope_type != scope_type:
            continue
        if page_number in scope.metadata.get("detected_from_pages", []):
            out.append(scope_id)
    return sorted(out)


def _last_heading_scope(text: str, regex: re.Pattern[str], kind: str) -> tuple[int, str | None]:
    last_pos = -1
    last_scope_id = None
    for match in regex.finditer(text):
        token = str(match.group(1)).lower()
        if kind == "bene":
            scope_id = f"bene:{token}"
        else:
            scope_id = "lotto:unico" if token == "unico" else f"lotto:{token}"
        last_pos = match.start()
        last_scope_id = scope_id
    return last_pos, last_scope_id


def _has_multiple_bene_scopes(state: RuntimeState) -> bool:
    return len([scope_id for scope_id, scope in state.scopes.items() if scope.scope_type == "bene"]) > 1


def _single_scope_fallback_candidate(state: RuntimeState, page_number: int) -> tuple[str | None, str | None, float]:
    page_benes = _page_scope_ids(state, page_number, "bene")
    if len(page_benes) == 1:
        return page_benes[0], "page_unique_bene", 0.55
    page_lotti = _page_scope_ids(state, page_number, "lotto")
    if len(page_lotti) == 1 and not _has_multiple_bene_scopes(state):
        return page_lotti[0], "page_unique_lotto", 0.55
    nearest_lotto_scope = None
    nearest_lotto_page = -1
    for scope_id, scope in state.scopes.items():
        if scope.scope_type != "lotto":
            continue
        for detected_page in scope.metadata.get("detected_from_pages", []):
            try:
                detected_page_num = int(detected_page)
            except Exception:
                continue
            if detected_page_num <= page_number and detected_page_num > nearest_lotto_page:
                nearest_lotto_scope = scope_id
                nearest_lotto_page = detected_page_num
    if nearest_lotto_scope:
        child_benes = sorted(
            child.scope_id
            for child in state.list_child_scopes(nearest_lotto_scope)
            if child.scope_type == "bene"
        )
        if len(child_benes) == 1:
            return child_benes[0], "active_single_bene_lotto_fallback", 0.58
    bene_scope_ids = sorted(scope_id for scope_id, scope in state.scopes.items() if scope.scope_type == "bene")
    lotto_scope_ids = sorted(scope_id for scope_id, scope in state.scopes.items() if scope.scope_type == "lotto")
    if len(bene_scope_ids) == 1 and len(lotto_scope_ids) <= 1:
        return bene_scope_ids[0], "single_scope_document", 0.55
    if len(lotto_scope_ids) == 1 and not bene_scope_ids:
        return lotto_scope_ids[0], "single_lotto_document", 0.55
    if not bene_scope_ids and not lotto_scope_ids:
        return "document_root", "document_root_fallback", 0.55
    return None, None, 0.0


def _ownership_context(raw_text: str, start: int) -> str:
    line_start = raw_text.rfind("\n", 0, start) + 1
    line_end = raw_text.find("\n", start)
    if line_end == -1:
        line_end = len(raw_text)
    current_line = raw_text[line_start:line_end].strip()
    previous_line = ""
    previous_end = line_start - 1
    if previous_end > 0:
        previous_start = raw_text.rfind("\n", 0, previous_end)
        previous_start = 0 if previous_start == -1 else previous_start + 1
        previous_line = raw_text[previous_start:previous_end].strip()
    if previous_line and (_BENE_REGEX.search(previous_line) or _LOT_REGEX.search(previous_line)):
        return f"{previous_line}\n{current_line}".strip()
    return current_line


def _collect_ownership_signals(
    state: RuntimeState,
    quote: str,
    expanded_quote: str,
    ownership_context: str,
    page_number: int,
    raw_text: str,
    start: int,
) -> tuple[list[OwnershipSignal], bool]:
    signals: list[OwnershipSignal] = [
        OwnershipSignal("DEFAULT_ROOT", "document_root", _SIGNAL_WEIGHTS["DEFAULT_ROOT"], "no stronger scope yet")
    ]
    universal = False
    low_context = ownership_context.lower()
    prefix = raw_text[:start]
    bene_pos, active_bene = _last_heading_scope(prefix, _BENE_REGEX, "bene")
    lotto_pos, active_lotto = _last_heading_scope(prefix, _LOT_REGEX, "lotto")
    has_active_local_bene = bool(active_bene and bene_pos >= lotto_pos and active_bene in state.scopes)
    bene_match = _BENE_REGEX.search(ownership_context)
    if bene_match and not has_active_local_bene:
        scope_id = f"bene:{bene_match.group(1)}"
        if scope_id in state.scopes:
            signals.append(OwnershipSignal("EXPLICIT_SCOPE_REF", scope_id, _SIGNAL_WEIGHTS["EXPLICIT_SCOPE_REF"], "explicit bene reference in ownership context"))
    lot_match = _LOT_REGEX.search(ownership_context)
    if _UNIVERSAL_BENI_RE.search(low_context) or _UNIVERSAL_LOTTI_RE.search(low_context) or _INTERO_LOTTO_RE.search(low_context):
        universal = True
    if lot_match:
        token = str(lot_match.group(1)).lower()
        scope_id = "lotto:unico" if token == "unico" else f"lotto:{token}"
        if scope_id in state.scopes:
            signals.append(OwnershipSignal("EXPLICIT_SCOPE_REF", scope_id, _SIGNAL_WEIGHTS["EXPLICIT_SCOPE_REF"], "explicit lotto reference in ownership context"))
            if universal:
                signals.append(OwnershipSignal("UNIVERSAL_MARKER", scope_id, _SIGNAL_WEIGHTS["UNIVERSAL_MARKER"], "explicit universal lot wording in ownership context"))
    if has_active_local_bene:
        signals.append(OwnershipSignal("SECTION_HEADING_BENE", active_bene, _SIGNAL_WEIGHTS["SECTION_HEADING_BENE"], "active bene section heading"))
        signals.append(OwnershipSignal("PROXIMITY_STICKY", active_bene, _SIGNAL_WEIGHTS["PROXIMITY_STICKY"], "sticky bene ownership"))
    if active_lotto and active_lotto in state.scopes:
        if universal:
            signals.append(OwnershipSignal("UNIVERSAL_MARKER", active_lotto, _SIGNAL_WEIGHTS["UNIVERSAL_MARKER"], "active lotto universal wording"))
            signals.append(OwnershipSignal("SECTION_HEADING_LOTTO", active_lotto, _SIGNAL_WEIGHTS["SECTION_HEADING_LOTTO"], "active lotto heading"))
        elif not active_bene:
            signals.append(OwnershipSignal("SECTION_HEADING_LOTTO", active_lotto, _SIGNAL_WEIGHTS["SECTION_HEADING_LOTTO"], "lotto container heading"))
            signals.append(OwnershipSignal("PROXIMITY_STICKY", active_lotto, _SIGNAL_WEIGHTS["PROXIMITY_STICKY"], "sticky lotto ownership"))
    inline_bene = _BENE_REGEX.search(ownership_context)
    if inline_bene and not has_active_local_bene:
        scope_id = f"bene:{inline_bene.group(1)}"
        if scope_id in state.scopes:
            signals.append(OwnershipSignal("INLINE_REFERENCE", scope_id, _SIGNAL_WEIGHTS["INLINE_REFERENCE"], "inline bene reference in ownership context"))
    inline_lotto = _LOT_REGEX.search(ownership_context)
    if inline_lotto:
        token = str(inline_lotto.group(1)).lower()
        scope_id = "lotto:unico" if token == "unico" else f"lotto:{token}"
        if scope_id in state.scopes and universal:
            signals.append(OwnershipSignal("INLINE_REFERENCE", scope_id, _SIGNAL_WEIGHTS["INLINE_REFERENCE"], "inline lotto reference in ownership context"))
    fallback_scope, fallback_method, fallback_score = _single_scope_fallback_candidate(state, page_number)
    if fallback_scope and fallback_method:
        signals.append(OwnershipSignal("PROXIMITY_STICKY", fallback_scope, fallback_score, fallback_method))
    return signals, universal


def _child_candidate_exists(state: RuntimeState, container_scope: str, aggregated: dict[str, float]) -> bool:
    for scope_id, score in aggregated.items():
        if score <= 0.35 or scope_id == container_scope:
            continue
        scope = state.scopes.get(scope_id)
        if not scope:
            continue
        if container_scope == "document_root":
            if scope.scope_type in {"lotto", "bene"}:
                return True
        elif scope.parent_scope_id == container_scope:
            return True
    return False


def _ownership_method_from_signals(signals: list[str], inherited: bool = False) -> str:
    if inherited:
        return "INHERITANCE"
    if any(sig in {"EXPLICIT_SCOPE_REF", "SECTION_HEADING_BENE", "INLINE_REFERENCE", "UNIVERSAL_MARKER"} for sig in signals):
        return "DIRECT_ASSIGNMENT"
    if any(sig in {"SECTION_HEADING_LOTTO", "PROXIMITY_STICKY", "DEFAULT_ROOT"} for sig in signals):
        return "WEAK_ASSIGNMENT"
    return "UNRESOLVED"


def _expanded_quote_has_multiple_bene_refs(expanded_quote: str) -> bool:
    return len(set(_BENE_REGEX.findall(expanded_quote))) > 1


def _build_ownership_decision(
    state: RuntimeState,
    *,
    quote: str,
    page_number: int,
    raw_text: str,
    expanded_quote: str,
    ownership_context: str | None = None,
    start: int,
    inherited: bool = False,
    inherited_from_scope_id: str | None = None,
    inheritance_reason: str | None = None,
) -> OwnershipDecision:
    ownership_context_provided = ownership_context is not None
    ownership_context = ownership_context or _ownership_context(raw_text, start)
    signals, universal = _collect_ownership_signals(state, quote, expanded_quote, ownership_context, page_number, raw_text, start)
    aggregated: dict[str, float] = defaultdict(float)
    by_scope_signals: dict[str, list[OwnershipSignal]] = defaultdict(list)
    for signal in signals:
        aggregated[signal.target_scope] += float(signal.score)
        by_scope_signals[signal.target_scope].append(signal)
    ranked = sorted(aggregated.items(), key=lambda item: item[1], reverse=True)
    scope_candidates = [
        {
            "scope_id": scope_id,
            "score": round(score, 3),
            "signals": [signal.signal_type for signal in by_scope_signals[scope_id]],
        }
        for scope_id, score in ranked
    ]
    decision = OwnershipDecision(
        evidence_id=f"own_{uuid.uuid4().hex[:12]}",
        raw_text=quote,
        page=page_number,
        scope_candidates=scope_candidates,
        inherited=inherited,
        inherited_from_scope_id=inherited_from_scope_id,
        inheritance_reason=inheritance_reason,
    )
    if not ranked:
        decision.unresolved_reason = "no_scope_signals_found"
        decision.competing_signals = [asdict(signal) for signal in signals]
        return decision
    top_scope, top_score = ranked[0]
    second_score = ranked[1][1] if len(ranked) > 1 else 0.0
    gap = top_score - second_score
    top_signals = [signal.signal_type for signal in by_scope_signals[top_scope]]
    if top_score >= 0.75 and gap >= 0.30:
        confidence = "HIGH"
    elif top_score >= 0.50 and gap >= 0.15:
        confidence = "LOW"
    else:
        confidence = None
    top_scope_type = state.scopes.get(top_scope).scope_type if top_scope in state.scopes else None
    if (
        confidence
        and top_scope_type == "bene"
        and not ownership_context_provided
        and _expanded_quote_has_multiple_bene_refs(expanded_quote)
        and not universal
        and set(top_signals).issubset({"SECTION_HEADING_BENE", "PROXIMITY_STICKY", "DEFAULT_ROOT"})
    ):
        confidence = None
        decision.unresolved_reason = "multiple_bene_refs_without_explicit_anchor"
    if confidence and top_scope_type in {"lotto", "document_root"} and _child_candidate_exists(state, top_scope, aggregated) and not universal:
        confidence = None
        decision.unresolved_reason = "container_scope_blocked_by_child_signal"
    if confidence is None:
        if decision.unresolved_reason is None:
            decision.unresolved_reason = "ownership_signals_too_close_or_weak"
        decision.competing_signals = [asdict(signal) for signal in signals]
        return decision
    decision.winning_scope = top_scope
    decision.confidence = confidence
    decision.ownership_method = _ownership_method_from_signals(top_signals, inherited=inherited)
    decision.competing_signals = [asdict(signal) for signal in by_scope_signals[top_scope]]
    return decision


def _inheritance_targets(state: RuntimeState, quote: str, owning_scope_id: str | None) -> tuple[list[str], str | None]:
    low = quote.lower()
    if _UNIVERSAL_BENI_RE.search(low):
        return sorted(scope_id for scope_id, scope in state.scopes.items() if scope.scope_type == "bene"), "tutti_i_beni"
    if _UNIVERSAL_LOTTI_RE.search(low):
        return sorted(scope_id for scope_id, scope in state.scopes.items() if scope.scope_type == "lotto"), "tutti_i_lotti"
    if _INTERO_LOTTO_RE.search(low) and owning_scope_id and owning_scope_id.startswith("lotto:"):
        children = [scope.scope_id for scope in state.list_child_scopes(owning_scope_id)]
        return [owning_scope_id, *sorted(children)], "intero_lotto"
    if owning_scope_id == "lotto:unico":
        children = [scope.scope_id for scope in state.list_child_scopes(owning_scope_id)]
        if children:
            return [owning_scope_id, *sorted(children)], "lotto_unico_root_equivalent"
    return [], None


def _make_hit(
    *,
    page_number: int,
    quote: str,
    expanded_quote: str,
    confidence: float,
    reason: str,
    evidence_type: str,
    matched_pattern: str,
    scope_id: str,
    ownership_method: str,
    inherited: bool = False,
    inherited_from_scope_id: str | None = None,
    inheritance_reason: str | None = None,
) -> dict:
    return {
        "page": page_number,
        "quote": quote,
        "expanded_quote": expanded_quote,
        "confidence": confidence,
        "reason": reason,
        "evidence_type": evidence_type,
        "matched_pattern": matched_pattern,
        "scope_id": scope_id,
        "ownership_method": ownership_method,
        "inherited": inherited,
        "inherited_from_scope_id": inherited_from_scope_id,
        "inheritance_reason": inheritance_reason,
    }


def _attach_scope_hit(state: RuntimeState, hit: dict) -> dict:
    ownership = state.attach_evidence_ownership(
        scope_id=hit["scope_id"],
        field_target="agibilita",
        source_page=int(hit["page"]),
        quote=str(hit["quote"]),
        confidence=float(hit["confidence"]),
        ownership_method=str(hit["ownership_method"]),
    )
    attached = dict(hit)
    attached["evidence_id"] = ownership.evidence_id
    return attached


def _scope_evidence_span(hit: dict):
    return make_evidence(int(hit["page"]), str(hit["quote"]), "agibilita", ["agibilita"], float(hit["confidence"]))


def _readable_scope_ref(scope_id: str) -> str:
    if scope_id == "document_root":
        return "Documento"
    if scope_id.startswith("bene:"):
        return f"Bene {scope_id.split(':', 1)[1]}"
    if scope_id.startswith("lotto:"):
        token = scope_id.split(":", 1)[1]
        return f"Lotto {'Unico' if token == 'unico' else token}"
    return scope_id


def _evidence_page(evidence: EvidenceSpan | dict) -> int:
    if isinstance(evidence, dict):
        return int(evidence.get("page") or 0)
    return int(getattr(evidence, "page", 0) or 0)


def _evidence_quote(evidence: EvidenceSpan | dict) -> str:
    if isinstance(evidence, dict):
        return str(evidence.get("quote") or "")
    return str(getattr(evidence, "quote", "") or "")


def _section_labels_from_text(text: str) -> list[str]:
    labels = []
    low = text.lower()
    if "regolarità edilizia" in low or "regolarita edilizia" in low:
        labels.append("Regolarità edilizia")
    if "pratiche edilizie" in low:
        labels.append("Pratiche edilizie")
    lot_match = _LOT_REGEX.search(text)
    if lot_match:
        token = str(lot_match.group(1)).lower()
        labels.append(f"Lotto {'Unico' if token == 'unico' else token}")
    bene_match = _BENE_REGEX.search(text)
    if bene_match:
        labels.append(f"Bene N° {bene_match.group(1)}")
    deduped = []
    for label in labels:
        if label not in deduped:
            deduped.append(label)
    return deduped


def _trail_evidence(hit: dict) -> dict:
    quote = " ".join(str(hit.get("quote") or "").split())
    if len(quote) > 180:
        quote = f"{quote[:177]}..."
    return {"page": int(hit.get("page") or 0), "quote": quote}


def _verification_trail_for_scope(scope_id: str, hits: list[dict], unresolved_reason: str) -> dict:
    checked_pages = sorted({int(hit.get("page") or 0) for hit in hits if hit.get("page")})
    checked_sections = []
    for hit in hits:
        for label in _section_labels_from_text(str(hit.get("expanded_quote") or hit.get("quote") or "")):
            if label not in checked_sections:
                checked_sections.append(label)
    sorted_hits = sorted(hits, key=lambda item: _hit_sort_key(scope_id, item), reverse=True)
    reason_map = {
        "same_scope_conflict_survives_expanded_reading": "same-scope evidence remains genuinely conflicting",
        "different_scopes_have_different_resolved_truth": "truth differs across scopes",
        "no_agibilita_evidence": "no decisive certificate or explicit same-scope statement found",
    }
    verify_map = {
        "same_scope_conflict_survives_expanded_reading": "Verify certificate of agibilità / abitabilità and the Regolarità edilizia / Pratiche edilizie attachments.",
        "different_scopes_have_different_resolved_truth": "Verify each Lotto/Bene separately instead of relying on a single document-wide agibilità conclusion.",
        "no_agibilita_evidence": "Verify certificate of agibilità / abitabilità in the edilizia attachments.",
    }
    return {
        "checked_scope_label": _readable_scope_ref(scope_id),
        "checked_pages": checked_pages,
        "checked_sections": checked_sections,
        "key_evidence_found": [_trail_evidence(hit) for hit in sorted_hits[:3]],
        "reason_unresolved": reason_map.get(unresolved_reason, "no decisive certificate or explicit same-scope statement found"),
        "verify_next": verify_map.get(unresolved_reason, "Verify certificate of agibilità / abitabilità and scope ownership of the statements."),
    }


def _verification_trail_for_root(scopes: list, unresolved_reason: str) -> dict:
    checked_pages = []
    key_evidence = []
    checked_sections = []
    for scope in scopes:
        for evidence in scope.agibilita.get("evidence", [])[:1]:
            page = _evidence_page(evidence)
            if page and page not in checked_pages:
                checked_pages.append(page)
            scope_label = scope.label or scope.scope_id
            quote = " ".join(_evidence_quote(evidence).split())
            if len(quote) > 150:
                quote = f"{quote[:147]}..."
            key_evidence.append({"page": page, "quote": f"{scope_label}: {quote}"})
            for label in _section_labels_from_text(_evidence_quote(evidence)):
                if label not in checked_sections:
                    checked_sections.append(label)
    return {
        "checked_scope_label": _readable_scope_ref("document_root"),
        "checked_pages": sorted(checked_pages),
        "checked_sections": checked_sections,
        "key_evidence_found": key_evidence[:3],
        "reason_unresolved": "truth differs by scope",
        "verify_next": "Verify each Lotto/Bene separately instead of relying on a single document-wide agibilità conclusion.",
    }


def _hit_tier(hit: dict) -> int:
    pattern = str(hit.get("matched_pattern") or "").lower()
    evidence_type = str(hit.get("evidence_type") or "")
    if evidence_type.startswith("inherited_"):
        return 5
    if evidence_type == "construction_negative":
        return 4
    if pattern in _TIER1_NEGATIVE_PATTERNS:
        return 1
    if pattern in _TIER2_NEGATIVE_PATTERNS:
        return 2
    if pattern in _TIER3_POSITIVE_PATTERNS:
        return 3
    return 5


def _section_strength(text: str) -> int:
    low = text.lower()
    if _SECTION_LABEL_RE.search(low):
        return 2
    if _LOT_REGEX.search(low) or _BENE_REGEX.search(low):
        return 1
    return 0


def _hit_specificity(scope_id: str, hit: dict) -> int:
    expanded = str(hit.get("expanded_quote") or hit.get("quote") or "")
    low = expanded.lower()
    score = _OWNERSHIP_STRENGTH.get(str(hit.get("ownership_method") or ""), 0)
    if scope_id.startswith("bene:"):
        token = scope_id.split(":", 1)[1]
        if re.search(rf"\bbene\s*n[°º.]?\s*{re.escape(token)}\b", low, re.IGNORECASE):
            score += 3
    if scope_id.startswith("lotto:"):
        token = scope_id.split(":", 1)[1]
        if token == "unico":
            if "lotto unico" in low:
                score += 3
        elif re.search(rf"\blotto\s*n?[°º]?\s*{re.escape(token)}\b", low, re.IGNORECASE):
            score += 3
    score += _section_strength(expanded)
    if hit.get("inherited"):
        score -= 3
    return score


def _hit_sort_key(scope_id: str, hit: dict) -> tuple[int, int, float, int]:
    return (
        -_hit_tier(hit),
        _hit_specificity(scope_id, hit),
        float(hit.get("confidence", 0.0)),
        -len(str(hit.get("expanded_quote") or hit.get("quote") or "")),
    )


def _best_hit(scope_id: str, hits: list[dict]) -> dict | None:
    if not hits:
        return None
    return sorted(hits, key=lambda item: _hit_sort_key(scope_id, item), reverse=True)[0]


def _resolver_meta(
    *,
    raw_conflict_detected: bool,
    competing_hits: list[dict],
    resolution_method: str,
    resolution_reason: str,
    winner: dict | None = None,
    loser: dict | None = None,
    unresolved_reason: str | None = None,
) -> dict:
    return {
        "raw_conflict_detected": raw_conflict_detected,
        "competing_evidence_ids": [hit.get("evidence_id") for hit in competing_hits if hit.get("evidence_id")],
        "resolution_method": resolution_method,
        "winning_evidence_tier": _hit_tier(winner) if winner else None,
        "losing_evidence_tier": _hit_tier(loser) if loser else None,
        "resolution_reason": resolution_reason,
        "unresolved_reason": unresolved_reason,
        "winner_inherited": bool(winner.get("inherited")) if winner else False,
        "winner_inherited_from_scope_id": winner.get("inherited_from_scope_id") if winner else None,
    }


def _resolve_scope_hits(scope_id: str, positive_hits: list[dict], negative_hits: list[dict]) -> dict:
    best_positive = _best_hit(scope_id, positive_hits)
    best_negative = _best_hit(scope_id, negative_hits)
    if best_negative is None and best_positive is None:
        return {}
    if best_negative and best_positive:
        conflict_hits = [best_negative, best_positive]
        if scope_id == "document_root":
            return {
                "status": "NON_VERIFICABILE",
                "winner": None,
                "loser": None,
                "resolver_meta": _resolver_meta(
                    raw_conflict_detected=True,
                    competing_hits=conflict_hits,
                    resolution_method="A+B+C+D",
                    resolution_reason="document_root_conflict_not_collapsible",
                    unresolved_reason="same_scope_conflict_survives_expanded_reading",
                ),
                "evidence": [_scope_evidence_span(best_negative), _scope_evidence_span(best_positive)],
            }
        negative_tier = _hit_tier(best_negative)
        positive_tier = _hit_tier(best_positive)
        if negative_tier < positive_tier:
            return {
                "status": "ASSENTE",
                "winner": best_negative,
                "loser": best_positive,
                "resolver_meta": _resolver_meta(
                    raw_conflict_detected=True,
                    competing_hits=conflict_hits,
                    resolution_method="A+B+C+D",
                    resolution_reason="higher_tier_negative_beats_positive",
                    winner=best_negative,
                    loser=best_positive,
                ),
            }
        if positive_tier < negative_tier:
            return {
                "status": "PRESENTE",
                "winner": best_positive,
                "loser": best_negative,
                "resolver_meta": _resolver_meta(
                    raw_conflict_detected=True,
                    competing_hits=conflict_hits,
                    resolution_method="A+B+C+D",
                    resolution_reason="higher_tier_positive_beats_negative",
                    winner=best_positive,
                    loser=best_negative,
                ),
            }
        negative_specificity = _hit_specificity(scope_id, best_negative)
        positive_specificity = _hit_specificity(scope_id, best_positive)
        if negative_specificity > positive_specificity:
            return {
                "status": "ASSENTE",
                "winner": best_negative,
                "loser": best_positive,
                "resolver_meta": _resolver_meta(
                    raw_conflict_detected=True,
                    competing_hits=conflict_hits,
                    resolution_method="A+B+C+D",
                    resolution_reason="same_tier_negative_more_specific",
                    winner=best_negative,
                    loser=best_positive,
                ),
            }
        if positive_specificity > negative_specificity:
            return {
                "status": "PRESENTE",
                "winner": best_positive,
                "loser": best_negative,
                "resolver_meta": _resolver_meta(
                    raw_conflict_detected=True,
                    competing_hits=conflict_hits,
                    resolution_method="A+B+C+D",
                    resolution_reason="same_tier_positive_more_specific",
                    winner=best_positive,
                    loser=best_negative,
                ),
            }
        return {
            "status": "NON_VERIFICABILE",
            "winner": None,
            "loser": None,
            "resolver_meta": _resolver_meta(
                raw_conflict_detected=True,
                competing_hits=conflict_hits,
                resolution_method="A+B+C+D",
                resolution_reason="non_collapsible_same_scope_conflict",
                unresolved_reason="same_scope_conflict_survives_expanded_reading",
            ),
            "evidence": [_scope_evidence_span(best_negative), _scope_evidence_span(best_positive)],
        }
    winner = best_negative or best_positive
    return {
        "status": "ASSENTE" if best_negative else "PRESENTE",
        "winner": winner,
        "loser": None,
        "resolver_meta": _resolver_meta(
            raw_conflict_detected=False,
            competing_hits=[winner] if winner else [],
            resolution_method="single_polarity_resolution",
            resolution_reason="negative_only" if best_negative else "positive_only",
            winner=winner,
        ),
    }


def _write_scope_agibilita(state: RuntimeState, scope_id: str, positive_hits: list[dict], negative_hits: list[dict]) -> None:
    scope = state.scopes[scope_id]
    resolved = _resolve_scope_hits(scope_id, positive_hits, negative_hits)
    if not resolved:
        return
    resolver_meta = resolved.get("resolver_meta", {})
    if resolver_meta:
        scope.metadata["agibilita_internal"] = resolver_meta
    if resolver_meta.get("raw_conflict_detected"):
        scope.contradictions.append(
            {
                "field": "agibilita",
                "kind": "same_scope_conflict_internal",
                "positive_evidence_ids": [hit.get("evidence_id") for hit in positive_hits if hit.get("evidence_id")],
                "negative_evidence_ids": [hit.get("evidence_id") for hit in negative_hits if hit.get("evidence_id")],
                "resolver_meta": resolver_meta,
            }
        )
    winner = resolved.get("winner")
    if resolved["status"] == "ASSENTE" and winner:
        issue = _build_negative_agibilita_issue(
            int(winner["page"]),
            str(winner["quote"]),
            float(winner["confidence"]),
            str(winner["reason"]),
            metadata={
                "scope_id": scope_id,
                "evidence_id": winner.get("evidence_id"),
                "inherited": bool(winner.get("inherited")),
            },
        )
        scope.issues.append(
            {
                "code": issue.code,
                "title_it": issue.title_it,
                "severity": issue.severity,
                "category": issue.category,
                "priority_score": issue.priority_score,
                "summary_it": issue.summary_it,
                "action_it": issue.action_it,
                "metadata": issue.metadata,
                "evidence": issue.evidence,
            }
        )
        scope.agibilita = {
            "status": "ASSENTE",
            "confidence": float(winner["confidence"]),
            "evidence": issue.evidence,
            "issue_code": issue.code,
            "guards": ["negative_agibilita_requires_document_evidence"],
        }
        return
    if resolved["status"] == "PRESENTE" and winner:
        scope.agibilita = {
            "status": "PRESENTE",
            "page": int(winner["page"]),
            "confidence": float(winner["confidence"]),
            "evidence": [_scope_evidence_span(winner)],
            "guards": [],
        }
        return
    scope.agibilita = {
        "status": "NON_VERIFICABILE",
        "confidence": 0.0,
        "evidence": resolved.get("evidence", []),
        "guards": ["negative_agibilita_requires_document_evidence", "agibilita_same_scope_non_collapsible"],
        "verification_trail": _verification_trail_for_scope(
            scope_id,
            positive_hits + negative_hits,
            str(resolver_meta.get("unresolved_reason") or "same_scope_conflict_survives_expanded_reading"),
        ),
    }


def _root_negative_issue(scope) -> CanonicalIssue | None:
    for item in scope.issues:
        if item.get("code") == "AGIBILITA_NEGATIVE":
            return CanonicalIssue(
                code=item["code"],
                title_it=item["title_it"],
                severity=item["severity"],
                category=item["category"],
                priority_score=item["priority_score"],
                evidence=item.get("evidence", []),
                summary_it=item.get("summary_it", ""),
                action_it=item.get("action_it", ""),
                metadata=item.get("metadata", {}),
            )
    return None


def _has_resolved_bene_descendants(state: RuntimeState, scope_id: str) -> bool:
    for scope in state.scopes.values():
        if scope.scope_type != "bene" or not scope.agibilita:
            continue
        if scope.agibilita.get("status") not in {"ASSENTE", "PRESENTE", "NON_VERIFICABILE"}:
            continue
        current = scope
        while current.parent_scope_id is not None:
            if current.parent_scope_id == scope_id:
                return True
            current = state.scopes.get(current.parent_scope_id)
            if current is None:
                break
    return False


def _derive_root_from_scopes(state: RuntimeState) -> tuple[dict, CanonicalIssue | None]:
    bene_scopes = [
        scope
        for scope in state.scopes.values()
        if scope.scope_type == "bene" and scope.agibilita and scope.agibilita.get("status") in {"ASSENTE", "PRESENTE", "NON_VERIFICABILE"}
    ]
    if bene_scopes:
        scoped = bene_scopes
    else:
        scoped = [
            scope
            for scope_id, scope in state.scopes.items()
            if scope_id != "document_root"
            and scope.agibilita
            and not _has_resolved_bene_descendants(state, scope_id)
        ]
    negatives = [scope for scope in scoped if scope.agibilita.get("status") == "ASSENTE"]
    positives = [scope for scope in scoped if scope.agibilita.get("status") == "PRESENTE"]
    non_verifiable = [scope for scope in scoped if scope.agibilita.get("status") == "NON_VERIFICABILE"]
    effective = negatives + positives + non_verifiable
    if len(effective) == 1:
        only = effective[0]
        root = dict(only.agibilita)
        return root, (_root_negative_issue(only) if root.get("status") == "ASSENTE" else None)
    if negatives and not positives and not non_verifiable:
        best_negative = max(negatives, key=lambda scope: float(scope.agibilita.get("confidence", 0.0)))
        return (
            {
                "status": "ASSENTE",
                "confidence": float(best_negative.agibilita.get("confidence", 0.0)),
                "evidence": best_negative.agibilita.get("evidence", []),
                "issue_code": "AGIBILITA_NEGATIVE",
                "guards": ["negative_agibilita_requires_document_evidence"],
            },
            _root_negative_issue(best_negative),
        )
    if positives and not negatives and not non_verifiable:
        best_positive = max(positives, key=lambda scope: float(scope.agibilita.get("confidence", 0.0)))
        return (
            {
                "status": "PRESENTE",
                "page": best_positive.agibilita.get("page"),
                "confidence": float(best_positive.agibilita.get("confidence", 0.0)),
                "evidence": best_positive.agibilita.get("evidence", []),
                "guards": [],
            },
            None,
        )
    if effective:
        evidence = []
        for scope in effective:
            evidence.extend(scope.agibilita.get("evidence", [])[:1])
        return (
            {
                "status": "NON_VERIFICABILE",
                "confidence": 0.0,
                "evidence": evidence[:2],
                "guards": ["negative_agibilita_requires_document_evidence", "mixed_scope_agibilita_non_collapsible"],
                "verification_trail": _verification_trail_for_root(effective, "different_scopes_have_different_resolved_truth"),
            },
            None,
        )
    return (
        {
            "status": "NON_VERIFICABILE",
            "guards": ["negative_agibilita_requires_document_evidence"],
            "verification_trail": {
                "checked_scope_label": "Documento",
                "checked_pages": [],
                "checked_sections": [],
                "key_evidence_found": [],
                "reason_unresolved": "no decisive certificate or explicit same-scope statement found",
                "verify_next": "Verify certificate of agibilità / abitabilità in the edilizia attachments.",
            },
        },
        None,
    )


def run_agibilita_agent(state: RuntimeState) -> None:
    hits_by_scope: dict[str, dict[str, list[dict]]] = defaultdict(lambda: {"positive": [], "negative": []})
    ownership_decisions = state.metrics.setdefault("agibilita_ownership_decisions", [])
    for idx, page in enumerate(state.pages, start=1):
        raw_text = str((page or {}).get("text") or "")
        text = raw_text.lower()
        page_number = int((page or {}).get("page_number") or (page or {}).get("page") or idx)
        match_kinds = []
        negative_hits = _pattern_hits(text, _NEGATIVE_PATTERNS)
        positive_hits = _pattern_hits(text, _POSITIVE_PATTERNS)
        construction_hits = _pattern_hits(text, _CONSTRUCTION_NEGATIVE_PATTERNS)
        for start, pattern in negative_hits:
            match_kinds.append(("negative", start, pattern, "explicit_negative", 0.95, "explicit_negative"))
        for start, pattern in positive_hits:
            if any(neg_start <= start < neg_start + len(neg_pattern) for neg_start, neg_pattern in negative_hits):
                continue
            if "non " in text[max(0, start - 12): start]:
                continue
            match_kinds.append(("positive", start, pattern, "explicit_positive", 0.9, "explicit_positive"))
        for start, pattern in construction_hits:
            match_kinds.append(("negative", start, pattern, "construction_negative", 0.78, "construction_negative"))
        match_kinds.sort(key=lambda item: item[1])
        for polarity, start, pattern, evidence_type, confidence, reason in match_kinds:
            pad_before = 160 if evidence_type == "construction_negative" else 140
            quote = raw_text[max(0, start - pad_before): min(len(raw_text), start + len(pattern) + 260)].strip()
            expanded_quote = raw_text[max(0, start - 400): min(len(raw_text), start + len(pattern) + 600)].strip()
            ownership_context = _ownership_context(raw_text, start)
            decision = _build_ownership_decision(
                state,
                quote=quote,
                page_number=page_number,
                raw_text=raw_text,
                expanded_quote=expanded_quote,
                ownership_context=ownership_context,
                start=start,
            )
            if decision.winning_scope is None:
                ownership_decisions.append(asdict(decision))
                continue
            base_hit = _attach_scope_hit(
                state,
                _make_hit(
                    page_number=page_number,
                    quote=quote,
                    expanded_quote=expanded_quote,
                    confidence=confidence,
                    reason=reason,
                    evidence_type=evidence_type,
                    matched_pattern=pattern,
                    scope_id=decision.winning_scope,
                    ownership_method=decision.ownership_method,
                ),
            )
            decision.evidence_id = str(base_hit.get("evidence_id") or decision.evidence_id)
            ownership_decisions.append(asdict(decision))
            hits_by_scope[decision.winning_scope][polarity].append(base_hit)
            inherited_targets, inheritance_reason = _inheritance_targets(state, quote, decision.winning_scope)
            for target_scope_id in inherited_targets:
                if target_scope_id == decision.winning_scope:
                    continue
                inherited_decision = OwnershipDecision(
                    evidence_id=f"own_{uuid.uuid4().hex[:12]}",
                    raw_text=quote,
                    page=page_number,
                    scope_candidates=[{"scope_id": target_scope_id, "score": 0.9, "signals": ["UNIVERSAL_MARKER"]}],
                    winning_scope=target_scope_id,
                    confidence="HIGH",
                    ownership_method="INHERITANCE",
                    competing_signals=[asdict(OwnershipSignal("UNIVERSAL_MARKER", target_scope_id, _SIGNAL_WEIGHTS["UNIVERSAL_MARKER"], str(inheritance_reason or "")))],
                    inherited=True,
                    inherited_from_scope_id=decision.winning_scope,
                    inheritance_reason=inheritance_reason,
                )
                inherited_hit = _attach_scope_hit(
                    state,
                    _make_hit(
                        page_number=page_number,
                        quote=quote,
                        expanded_quote=expanded_quote,
                        confidence=confidence,
                        reason=reason,
                        evidence_type="inherited_negative" if polarity == "negative" else "inherited_positive",
                        matched_pattern=pattern,
                        scope_id=target_scope_id,
                        ownership_method="INHERITANCE",
                        inherited=True,
                        inherited_from_scope_id=decision.winning_scope,
                        inheritance_reason=inheritance_reason,
                    ),
                )
                inherited_hit["original_evidence_id"] = base_hit.get("evidence_id")
                inherited_decision.evidence_id = str(inherited_hit.get("evidence_id") or inherited_decision.evidence_id)
                ownership_decisions.append(asdict(inherited_decision))
                hits_by_scope[target_scope_id][polarity].append(inherited_hit)

    for scope_id, polar_hits in hits_by_scope.items():
        _write_scope_agibilita(state, scope_id, polar_hits["positive"], polar_hits["negative"])

    root_scope = state.scopes["document_root"]
    if root_scope.agibilita:
        state.canonical_case.agibilita = root_scope.agibilita
        if root_scope.agibilita.get("status") == "ASSENTE":
            root_issue = next((item for item in root_scope.issues if item.get("code") == "AGIBILITA_NEGATIVE"), None)
            if root_issue:
                state.issues.append(
                    CanonicalIssue(
                        code=root_issue["code"],
                        title_it=root_issue["title_it"],
                        severity=root_issue["severity"],
                        category=root_issue["category"],
                        priority_score=root_issue["priority_score"],
                        evidence=root_issue.get("evidence", []),
                        summary_it=root_issue.get("summary_it", ""),
                        action_it=root_issue.get("action_it", ""),
                        metadata=root_issue.get("metadata", {}),
                    )
                )
        return

    root_agibilita, root_issue = _derive_root_from_scopes(state)
    root_scope.agibilita = root_agibilita
    if root_agibilita.get("status") == "NON_VERIFICABILE":
        root_scope.metadata["agibilita_internal"] = {
            "resolution_reason": "root_non_verificabile_rollup",
            "unresolved_reason": root_agibilita.get("verification_trail", {}).get("reason_unresolved"),
        }
    state.canonical_case.agibilita = root_agibilita
    if root_issue:
        root_scope.issues.append(
            {
                "code": root_issue.code,
                "title_it": root_issue.title_it,
                "severity": root_issue.severity,
                "category": root_issue.category,
                "priority_score": root_issue.priority_score,
                "summary_it": root_issue.summary_it,
                "action_it": root_issue.action_it,
                "metadata": root_issue.metadata,
                "evidence": root_issue.evidence,
            }
        )
        state.issues.append(root_issue)
