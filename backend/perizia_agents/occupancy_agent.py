from __future__ import annotations

import copy
import re
from typing import Any

from perizia_runtime.state import CanonicalScopeState, Judgment, RuntimeState
from perizia_tools.evidence_span_tool import make_evidence
from perizia_tools.section_router_tool import classify_section_type


_PROPERTY_OCCUPANCY_ANCHORS = [
    "stato di occupazione",
    "stato occupativo",
    "occupazione dell'immobile",
    "occupazione del bene",
    "immobile",
    "bene",
    "unità",
    "unita",
    "porzione",
]

_STRONG_FREE_OCCUPANCY_PHRASES = [
    "non appariva occupato",
    "non risultava occupato",
    "non risulta occupato",
    "non occupato",
    "immobile libero",
    "l'immobile risulta libero",
    "risulta libero",
    "bene libero",
    "unità libera",
    "unita libera",
    "libero da persone",
    "libero da cose",
]

_TENURE_SIGNALS = [
    "contratto di locazione",
    "locazione registrata",
    "condotto in locazione",
    "comodato",
    "occupato da",
    "locato",
    "locazione",
]

_TENURE_NEGATIONS = [
    "non risultano registrati atti di locazione",
    "non risultano registrati atti di locazione e/o comodato",
    "assenza di contratto di locazione",
    "assenza di locazione",
]

_TENURE_REGEX = re.compile(r"\b(?:locaz\w*|locat\w*|comodat\w*)\b", re.IGNORECASE)
_BENE_REGEX = re.compile(r"\bbene\s*n[°º.]?\s*(\d+)\b", re.IGNORECASE)
_LOTTO_REGEX = re.compile(r"\blotto\s*(?:n[°º.]?\s*)?(\d+|unico)\b", re.IGNORECASE)

_BASE_OCCUPANCY_GUARDS = [
    "valuation_coefficient_not_valid_occupancy",
    "public_space_occupancy_not_property_occupancy",
    "non_property_libero_noise",
    "bare_nessuno_requires_property_anchor",
    "tenure_signals_block_weak_libero_inference",
    "non_verificabile_not_assente",
]

_UNIVERSAL_SCOPE_MARKERS = [
    "tutti i beni",
    "tutti gli immobili",
    "entrambi i beni",
    "tutte le unità",
    "intero lotto",
    "intero immobile",
    "beni del lotto",
    "immobili del lotto",
]

_SIGNAL_RANK = {
    "occupied_explicit": 3,
    "free_strong": 2,
    "occupied_tenure": 1,
}


def _is_non_property_libero_noise(quote: str) -> bool:
    low = quote.lower()
    return any(
        marker in low
        for marker in [
            "@libero.it",
            ".libero.it",
            "stato libero",
            "regime patrimoniale",
            "separazione legale dei beni",
        ]
    )


def _has_tenure_signal(quote: str) -> bool:
    low = quote.lower()
    return any(marker in low for marker in _TENURE_SIGNALS)


def _has_negated_tenure_signal(quote: str) -> bool:
    low = quote.lower()
    return any(marker in low for marker in _TENURE_NEGATIONS)


def _has_property_occupancy_anchor(quote: str) -> bool:
    low = quote.lower()
    return any(marker in low for marker in _PROPERTY_OCCUPANCY_ANCHORS)


def _has_valid_free_occupancy_anchor(quote: str) -> bool:
    low = quote.lower()
    return (
        any(marker in low for marker in _STRONG_FREE_OCCUPANCY_PHRASES)
        or ("nessuno" in low and _has_property_occupancy_anchor(quote))
        or ("liber" in low and _has_property_occupancy_anchor(quote))
    )


def _infer_opponibilita(value: str, quote: str) -> str:
    if value != "OCCUPATO":
        return "NON VERIFICABILE"
    low = quote.lower()
    if "occupato da" in low or "senza titolo" in low:
        return "NON VERIFICABILE"
    if _has_tenure_signal(quote):
        return "LOCAZIONE DA VERIFICARE"
    return "NON VERIFICABILE"


def _occupancy_confidence(quote: str, value: str) -> float:
    low = quote.lower()
    if value == "OCCUPATO":
        if "stato di occupazione" in low or "occupato da" in low:
            return 0.96
        if _has_tenure_signal(quote):
            return 0.84
        return 0.7
    if "non appariva occupato" in low or "nessuno" in low:
        return 0.92
    if "stato di occupazione" in low or "immobile libero" in low or "bene libero" in low:
        return 0.9
    return 0.58


def _signal_type_for_quote(value: str, quote: str) -> str:
    if value == "LIBERO":
        return "free_strong"
    low = quote.lower()
    if "occupato da" in low or "stato di occupazione" in low:
        return "occupied_explicit"
    if _has_tenure_signal(quote):
        return "occupied_tenure"
    return "occupied_explicit"


def _quote_window(text: str, start: int, end: int) -> str:
    lines = text.splitlines()
    if not lines:
        return text[max(0, start - 80):min(len(text), end + 120)].strip()
    cursor = 0
    match_line_index = 0
    for idx, line in enumerate(lines):
        next_cursor = cursor + len(line) + 1
        if start < next_cursor:
            match_line_index = idx
            break
        cursor = next_cursor
    selected = [lines[match_line_index].strip()]
    heading_lines = []
    probe = match_line_index - 1
    while probe >= 0 and len(heading_lines) < 2:
        candidate = lines[probe].strip()
        if not candidate:
            break
        if _BENE_REGEX.search(candidate) or _LOTTO_REGEX.search(candidate):
            heading_lines.insert(0, candidate)
        elif heading_lines:
            break
        probe -= 1
    selected = heading_lines + selected
    return "\n".join(part for part in selected if part)


def _normalize_lotto_token(token: str) -> str:
    low = str(token or "").strip().lower()
    return "unico" if low == "unico" else re.sub(r"\D+", "", low)


def _available_scopes(state: RuntimeState, scope_type: str) -> list[CanonicalScopeState]:
    return sorted(
        [scope for scope in state.scopes.values() if scope.scope_type == scope_type],
        key=lambda scope: scope.scope_id,
    )


def _matching_scope_ids(state: RuntimeState, text: str, *, scope_type: str) -> list[str]:
    regex = _BENE_REGEX if scope_type == "bene" else _LOTTO_REGEX
    matches = []
    available = {scope.scope_id for scope in _available_scopes(state, scope_type)}
    for token in regex.findall(text or ""):
        normalized = token if scope_type == "bene" else _normalize_lotto_token(token)
        if not normalized:
            continue
        scope_id = f"{scope_type}:{normalized}"
        if scope_id in available and scope_id not in matches:
            matches.append(scope_id)
    return matches


def _is_universal_scope_statement(text: str) -> bool:
    low = str(text or "").lower()
    return any(marker in low for marker in _UNIVERSAL_SCOPE_MARKERS)


def _smallest_fallback_scope_id(state: RuntimeState) -> tuple[str, str]:
    bene_scopes = _available_scopes(state, "bene")
    if len(bene_scopes) == 1:
        return bene_scopes[0].scope_id, "single_bene_fallback"
    lotto_scopes = _available_scopes(state, "lotto")
    if len(lotto_scopes) == 1:
        return lotto_scopes[0].scope_id, "single_lotto_fallback"
    return "document_root", "document_root_fallback"


def _ownership_decision(state: RuntimeState, quote: str) -> tuple[str, str, bool]:
    bene_scope_ids = _matching_scope_ids(state, quote, scope_type="bene")
    lotto_scope_ids = _matching_scope_ids(state, quote, scope_type="lotto")
    universal = _is_universal_scope_statement(quote)
    if len(bene_scope_ids) == 1:
        return bene_scope_ids[0], "explicit_bene", False
    if len(bene_scope_ids) > 1:
        if universal and len(lotto_scope_ids) == 1:
            return lotto_scope_ids[0], "explicit_lotto_universal", True
        return "document_root", "ambiguous_multi_bene_reference", universal
    if len(lotto_scope_ids) == 1:
        return lotto_scope_ids[0], "explicit_lotto_universal" if universal else "explicit_lotto", universal
    scope_id, method = _smallest_fallback_scope_id(state)
    return scope_id, method, universal and scope_id != "document_root"


def _descendant_bene_scope_ids(state: RuntimeState, scope_id: str) -> list[str]:
    descendants = []
    queue = [scope_id]
    seen = set()
    while queue:
        current = queue.pop(0)
        if current in seen:
            continue
        seen.add(current)
        for child in state.list_child_scopes(current):
            if child.scope_type == "bene":
                descendants.append(child.scope_id)
            queue.append(child.scope_id)
    return descendants


def _make_hit(
    *,
    state: RuntimeState,
    index: int,
    page_number: int,
    quote: str,
    value: str,
    confidence: float,
    signal_type: str,
) -> dict[str, Any]:
    evidence = make_evidence(page_number, quote, "occupancy_statement", ["stato_occupativo", "occupancy"], confidence)
    scope_id, ownership_method, universal = _ownership_decision(state, quote)
    ownership = state.attach_evidence_ownership(
        scope_id=scope_id,
        field_target="occupancy",
        source_page=page_number,
        quote=quote,
        confidence=confidence,
        ownership_method=ownership_method,
        evidence_id=f"occupancy_{page_number}_{index}",
    )
    return {
        "value": value,
        "confidence": confidence,
        "valid": True,
        "evidence": [evidence],
        "opponibilita": _infer_opponibilita(value, quote),
        "page": page_number,
        "quote": quote,
        "signal_type": signal_type,
        "scope_id": scope_id,
        "ownership_method": ownership_method,
        "universal": universal,
        "inherited": False,
        "inherited_from_scope_id": None,
        "evidence_id": ownership.evidence_id,
    }


def _propagate_universal_hits(state: RuntimeState, direct_hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    propagated = list(direct_hits)
    for hit in direct_hits:
        if not hit.get("universal"):
            continue
        scope_id = str(hit.get("scope_id") or "")
        if scope_id.startswith("bene:"):
            continue
        for bene_scope_id in _descendant_bene_scope_ids(state, scope_id):
            inherited = copy.deepcopy(hit)
            inherited["scope_id"] = bene_scope_id
            inherited["inherited"] = True
            inherited["inherited_from_scope_id"] = scope_id
            inherited["ownership_method"] = "inherited_universal_scope_statement"
            propagated.append(inherited)
    return propagated


def _hit_sort_key(hit: dict[str, Any]) -> tuple[int, int, float, int]:
    return (
        0 if hit.get("inherited") else 1,
        _SIGNAL_RANK.get(str(hit.get("signal_type") or ""), 0),
        float(hit.get("confidence", 0.0)),
        len(str(hit.get("quote") or "")),
    )


def _best_hit(hits: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not hits:
        return None
    return sorted(hits, key=_hit_sort_key, reverse=True)[0]


def _resolver_meta(
    *,
    raw_conflict_detected: bool,
    competing_hits: list[dict[str, Any]],
    resolution_reason: str,
    winner: dict[str, Any] | None = None,
    unresolved_reason: str | None = None,
) -> dict[str, Any]:
    return {
        "raw_conflict_detected": raw_conflict_detected,
        "competing_evidence_ids": [hit.get("evidence_id") for hit in competing_hits if hit.get("evidence_id")],
        "resolution_reason": resolution_reason,
        "unresolved_reason": unresolved_reason,
        "winner_inherited": bool(winner.get("inherited")) if winner else False,
        "winner_inherited_from_scope_id": winner.get("inherited_from_scope_id") if winner else None,
        "competing_values": sorted({str(hit.get("value")) for hit in competing_hits if hit.get("value")}),
    }


def _resolve_scope_hits(scope_id: str, hits: list[dict[str, Any]]) -> dict[str, Any]:
    libres = [hit for hit in hits if hit.get("value") == "LIBERO"]
    occupati = [hit for hit in hits if hit.get("value") == "OCCUPATO"]
    best_libero = _best_hit(libres)
    best_occupato = _best_hit(occupati)
    if best_libero is None and best_occupato is None:
        return {}
    if best_libero is None:
        return {
            "status": "OCCUPATO",
            "winner": best_occupato,
            "resolver_meta": _resolver_meta(raw_conflict_detected=False, competing_hits=hits, resolution_reason="single_polarity_occupied"),
        }
    if best_occupato is None:
        return {
            "status": "LIBERO",
            "winner": best_libero,
            "resolver_meta": _resolver_meta(raw_conflict_detected=False, competing_hits=hits, resolution_reason="single_polarity_libero"),
        }
    if best_libero and best_occupato:
        competing_hits = [best_libero, best_occupato]
        if scope_id == "document_root":
            return {
                "status": "NON_VERIFICABILE",
                "winner": None,
                "resolver_meta": _resolver_meta(
                    raw_conflict_detected=True,
                    competing_hits=competing_hits,
                    resolution_reason="document_root_same_scope_not_collapsible",
                    unresolved_reason="same_scope_conflict_survives_resolution",
                ),
                "evidence": [best_libero["evidence"][0], best_occupato["evidence"][0]],
            }
        if bool(best_libero.get("inherited")) != bool(best_occupato.get("inherited")):
            winner = best_libero if not best_libero.get("inherited") else best_occupato
            return {
                "status": winner["value"],
                "winner": winner,
                "resolver_meta": _resolver_meta(
                    raw_conflict_detected=True,
                    competing_hits=competing_hits,
                    resolution_reason="direct_scope_statement_beats_inherited_statement",
                    winner=winner,
                ),
            }
        libero_rank = _SIGNAL_RANK.get(str(best_libero.get("signal_type") or ""), 0)
        occupato_rank = _SIGNAL_RANK.get(str(best_occupato.get("signal_type") or ""), 0)
        if occupato_rank > libero_rank:
            return {
                "status": "OCCUPATO",
                "winner": best_occupato,
                "resolver_meta": _resolver_meta(
                    raw_conflict_detected=True,
                    competing_hits=competing_hits,
                    resolution_reason="occupied_signal_stronger_than_free_signal",
                    winner=best_occupato,
                ),
            }
        if libero_rank > occupato_rank:
            return {
                "status": "LIBERO",
                "winner": best_libero,
                "resolver_meta": _resolver_meta(
                    raw_conflict_detected=True,
                    competing_hits=competing_hits,
                    resolution_reason="free_signal_stronger_than_occupied_signal",
                    winner=best_libero,
                ),
            }
        libero_conf = float(best_libero.get("confidence", 0.0))
        occupato_conf = float(best_occupato.get("confidence", 0.0))
        if occupato_conf - libero_conf >= 0.08:
            return {
                "status": "OCCUPATO",
                "winner": best_occupato,
                "resolver_meta": _resolver_meta(
                    raw_conflict_detected=True,
                    competing_hits=competing_hits,
                    resolution_reason="occupied_confidence_beats_free_confidence",
                    winner=best_occupato,
                ),
            }
        if libero_conf - occupato_conf >= 0.08:
            return {
                "status": "LIBERO",
                "winner": best_libero,
                "resolver_meta": _resolver_meta(
                    raw_conflict_detected=True,
                    competing_hits=competing_hits,
                    resolution_reason="free_confidence_beats_occupied_confidence",
                    winner=best_libero,
                ),
            }
        return {
            "status": "NON_VERIFICABILE",
            "winner": None,
            "resolver_meta": _resolver_meta(
                raw_conflict_detected=True,
                competing_hits=competing_hits,
                resolution_reason="same_scope_occupancy_not_collapsible",
                unresolved_reason="same_scope_conflict_survives_resolution",
            ),
            "evidence": [best_libero["evidence"][0], best_occupato["evidence"][0]],
        }
    return {}


def _guards_for_status(status: str | None, *, raw_conflict_detected: bool, mixed_scope: bool) -> list[str]:
    guards = list(_BASE_OCCUPANCY_GUARDS)
    if raw_conflict_detected:
        guards.append("same_scope_occupancy_not_collapsible")
    if mixed_scope:
        guards.append("mixed_scope_occupancy_non_collapsible")
    return guards


def _write_scope_occupancy(scope: CanonicalScopeState, resolved: dict[str, Any], *, mixed_scope: bool = False) -> None:
    if not resolved:
        return
    resolver_meta = resolved.get("resolver_meta") if isinstance(resolved.get("resolver_meta"), dict) else {}
    scope.metadata["occupancy_internal"] = resolver_meta
    status = resolved.get("status")
    if status == "NON_VERIFICABILE":
        evidence = resolved.get("evidence", [])
        scope.occupancy = {
            "status": "NON_VERIFICABILE",
            "opponibilita": "NON VERIFICABILE",
            "confidence": 0.0,
            "evidence": evidence,
            "guards": _guards_for_status("NON_VERIFICABILE", raw_conflict_detected=bool(resolver_meta.get("raw_conflict_detected")), mixed_scope=mixed_scope),
        }
        return
    winner = resolved.get("winner") or {}
    scope.occupancy = {
        "status": winner.get("value"),
        "opponibilita": winner.get("opponibilita", "NON VERIFICABILE"),
        "confidence": float(winner.get("confidence", 0.0)),
        "evidence": winner.get("evidence", []),
        "guards": _guards_for_status(status, raw_conflict_detected=bool(resolver_meta.get("raw_conflict_detected")), mixed_scope=mixed_scope),
    }


def _derive_scope_from_children(children: list[CanonicalScopeState]) -> dict[str, Any]:
    occupied = [scope for scope in children if scope.occupancy.get("status") == "OCCUPATO"]
    free = [scope for scope in children if scope.occupancy.get("status") == "LIBERO"]
    non_verifiable = [scope for scope in children if scope.occupancy.get("status") == "NON_VERIFICABILE"]
    if not occupied and not free and not non_verifiable:
        return {}
    if occupied and not free and not non_verifiable:
        best = max(occupied, key=lambda scope: float(scope.occupancy.get("confidence", 0.0)))
        opponibilita_values = {scope.occupancy.get("opponibilita") for scope in occupied}
        return {
            "status": "OCCUPATO",
            "winner": {
                "value": "OCCUPATO",
                "opponibilita": occupied[0].occupancy.get("opponibilita") if len(opponibilita_values) == 1 else "NON VERIFICABILE",
                "confidence": float(best.occupancy.get("confidence", 0.0)),
                "evidence": best.occupancy.get("evidence", []),
            },
            "resolver_meta": _resolver_meta(raw_conflict_detected=False, competing_hits=[], resolution_reason="leaf_first_uniform_occupied"),
        }
    if free and not occupied and not non_verifiable:
        best = max(free, key=lambda scope: float(scope.occupancy.get("confidence", 0.0)))
        return {
            "status": "LIBERO",
            "winner": {
                "value": "LIBERO",
                "opponibilita": "NON VERIFICABILE",
                "confidence": float(best.occupancy.get("confidence", 0.0)),
                "evidence": best.occupancy.get("evidence", []),
            },
            "resolver_meta": _resolver_meta(raw_conflict_detected=False, competing_hits=[], resolution_reason="leaf_first_uniform_free"),
        }
    evidence = []
    for scope in occupied + free + non_verifiable:
        evidence.extend(scope.occupancy.get("evidence", [])[:1])
    return {
        "status": "NON_VERIFICABILE",
        "winner": None,
        "evidence": evidence[:3],
        "resolver_meta": _resolver_meta(
            raw_conflict_detected=False,
            competing_hits=[],
            resolution_reason="leaf_first_mixed_scope_truth",
            unresolved_reason="different_scopes_have_different_resolved_truth",
        ),
    }


def _collect_candidates_and_hits(state: RuntimeState) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidates = []
    valid_hits = []
    hit_index = 0
    for idx, page in enumerate(state.pages, start=1):
        text = str((page or {}).get("text") or "")
        low = text.lower()
        page_number = int((page or {}).get("page_number") or (page or {}).get("page") or idx)
        if "coefficiente di locazione" in low:
            candidates.append(
                {
                    "value": "INVALID_OCCUPANCY_SIGNAL",
                    "confidence": 0.0,
                    "valid": False,
                    "reason": "valuation_coefficient_not_valid_occupancy",
                    "evidence": [make_evidence(page_number, text[:520], "valuation_coefficient", [], 0.0)],
                }
            )
        for match in re.finditer(r"\boccupat\w*\b|\bliber\w*\b|\bnessuno\b", text, re.IGNORECASE):
            quote = _quote_window(text, match.start(), match.end())
            section_type = classify_section_type(quote)
            match_text = match.group(0).lower()
            has_tenure_signal = _has_tenure_signal(quote)
            if "suolo pubblico occupato" in quote.lower():
                candidates.append(
                    {
                        "value": "INVALID_OCCUPANCY_SIGNAL",
                        "confidence": 0.0,
                        "valid": False,
                        "reason": "public_space_occupancy_not_property_occupancy",
                        "evidence": [make_evidence(page_number, quote, "public_space_occupancy", [], 0.0)],
                    }
                )
                continue
            if "pubblico occupato" in quote.lower():
                continue
            if section_type == "valuation" or "coefficiente" in quote.lower():
                candidates.append(
                    {
                        "value": "INVALID_OCCUPANCY_SIGNAL",
                        "confidence": 0.0,
                        "valid": False,
                        "reason": "valuation_table_not_valid_occupancy",
                        "evidence": [make_evidence(page_number, quote, "valuation_noise", [], 0.0)],
                    }
                )
                continue
            if _is_non_property_libero_noise(quote):
                candidates.append(
                    {
                        "value": "INVALID_OCCUPANCY_SIGNAL",
                        "confidence": 0.0,
                        "valid": False,
                        "reason": "non_property_libero_noise",
                        "evidence": [make_evidence(page_number, quote, "non_property_noise", [], 0.0)],
                    }
                )
                continue
            if match_text == "nessuno" and not _has_property_occupancy_anchor(quote):
                continue
            if has_tenure_signal and ("nessuno" in quote.lower() or "liber" in quote.lower()) and not _has_valid_free_occupancy_anchor(quote):
                candidates.append(
                    {
                        "value": "INVALID_OCCUPANCY_SIGNAL",
                        "confidence": 0.0,
                        "valid": False,
                        "reason": "weak_libero_blocked_by_tenure_signal",
                        "evidence": [make_evidence(page_number, quote, "tenure_blocks_weak_free", [], 0.0)],
                    }
                )
                continue
            value = None
            if _has_valid_free_occupancy_anchor(quote):
                value = "LIBERO"
            elif "occupato" in quote.lower():
                value = "OCCUPATO"
            elif "nessuno" in quote.lower() or "liber" in quote.lower():
                continue
            if value:
                confidence = _occupancy_confidence(quote, value)
                hit_index += 1
                hit = _make_hit(
                    state=state,
                    index=hit_index,
                    page_number=page_number,
                    quote=quote,
                    value=value,
                    confidence=confidence,
                    signal_type=_signal_type_for_quote(value, quote),
                )
                valid_hits.append(hit)
                candidates.append(
                    {
                        "value": hit["value"],
                        "confidence": hit["confidence"],
                        "valid": True,
                        "evidence": hit["evidence"],
                        "opponibilita": hit["opponibilita"],
                    }
                )
        for match in _TENURE_REGEX.finditer(text):
            quote = _quote_window(text, match.start(), match.end())
            section_type = classify_section_type(quote)
            if "coefficiente di locazione" in quote.lower():
                continue
            if section_type == "valuation" or "coefficiente" in quote.lower():
                candidates.append(
                    {
                        "value": "INVALID_OCCUPANCY_SIGNAL",
                        "confidence": 0.0,
                        "valid": False,
                        "reason": "valuation_table_not_valid_occupancy",
                        "evidence": [make_evidence(page_number, quote, "valuation_noise", [], 0.0)],
                    }
                )
                continue
            if _has_negated_tenure_signal(quote):
                continue
            if not _has_tenure_signal(quote):
                continue
            if not _has_property_occupancy_anchor(quote):
                continue
            confidence = _occupancy_confidence(quote, "OCCUPATO")
            hit_index += 1
            hit = _make_hit(
                state=state,
                index=hit_index,
                page_number=page_number,
                quote=quote,
                value="OCCUPATO",
                confidence=confidence,
                signal_type="occupied_tenure",
            )
            valid_hits.append(hit)
            candidates.append(
                {
                    "value": hit["value"],
                    "confidence": hit["confidence"],
                    "valid": True,
                    "evidence": hit["evidence"],
                    "opponibilita": hit["opponibilita"],
                }
            )
    return candidates, valid_hits


def run_occupancy_agent(state: RuntimeState) -> None:
    candidates, direct_hits = _collect_candidates_and_hits(state)
    state.canonical_case.occupancy["candidates"] = candidates
    if not direct_hits:
        state.judgments["stato_occupativo_verifier"] = Judgment(
            "stato_occupativo_verifier",
            None,
            "NOT_FOUND",
            0.0,
            [],
            "no valid occupancy evidence survived verifier guards",
        )
        state.canonical_case.occupancy.update(
            {
                "status": None,
                "opponibilita": "NON VERIFICABILE",
                "confidence": 0.0,
                "evidence": [],
                "guards": list(_BASE_OCCUPANCY_GUARDS),
            }
        )
        return

    all_hits = _propagate_universal_hits(state, direct_hits)
    hits_by_scope: dict[str, list[dict[str, Any]]] = {}
    for hit in all_hits:
        hits_by_scope.setdefault(str(hit.get("scope_id") or "document_root"), []).append(hit)

    for scope in _available_scopes(state, "bene"):
        resolved = _resolve_scope_hits(scope.scope_id, hits_by_scope.get(scope.scope_id, []))
        if resolved:
            _write_scope_occupancy(scope, resolved)

    for scope in _available_scopes(state, "lotto"):
        child_beni = [child for child in state.list_child_scopes(scope.scope_id) if child.scope_type == "bene" and child.occupancy]
        if child_beni:
            _write_scope_occupancy(scope, _derive_scope_from_children(child_beni), mixed_scope=True)
            continue
        resolved = _resolve_scope_hits(scope.scope_id, hits_by_scope.get(scope.scope_id, []))
        if resolved:
            _write_scope_occupancy(scope, resolved)

    root_scope = state.scopes["document_root"]
    bene_truths = [scope for scope in _available_scopes(state, "bene") if scope.occupancy]
    lotto_truths = [scope for scope in _available_scopes(state, "lotto") if scope.occupancy]
    if bene_truths:
        _write_scope_occupancy(root_scope, _derive_scope_from_children(bene_truths), mixed_scope=True)
    elif lotto_truths:
        _write_scope_occupancy(root_scope, _derive_scope_from_children(lotto_truths), mixed_scope=True)
    else:
        resolved = _resolve_scope_hits("document_root", hits_by_scope.get("document_root", []))
        if resolved:
            _write_scope_occupancy(root_scope, resolved)

    root_payload = dict(root_scope.occupancy) if root_scope.occupancy else {
        "status": None,
        "opponibilita": "NON VERIFICABILE",
        "confidence": 0.0,
        "evidence": [],
        "guards": list(_BASE_OCCUPANCY_GUARDS),
    }
    root_payload["candidates"] = candidates
    state.canonical_case.occupancy = root_payload

    status = root_payload.get("status")
    if status:
        state.judgments["stato_occupativo_verifier"] = Judgment(
            "stato_occupativo_verifier",
            status,
            "FOUND" if status != "NON_VERIFICABILE" else "LOW_CONFIDENCE",
            float(root_payload.get("confidence", 0.0)),
            root_payload.get("evidence", []),
            "occupancy resolved with scoped ownership, internal conflict resolution, and leaf-first rollup",
        )
    else:
        state.judgments["stato_occupativo_verifier"] = Judgment(
            "stato_occupativo_verifier",
            None,
            "NOT_FOUND",
            0.0,
            [],
            "no valid occupancy evidence survived verifier guards",
        )
