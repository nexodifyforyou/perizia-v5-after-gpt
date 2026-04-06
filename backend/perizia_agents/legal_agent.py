from __future__ import annotations

import copy
import re
from collections import defaultdict
from typing import Any

from perizia_runtime.state import CanonicalIssue, Judgment, RuntimeState
from perizia_tools.evidence_span_tool import make_evidence


_BENE_REGEX = re.compile(r"\bbene\s*n[°º.]?\s*(\d+)\b", re.IGNORECASE)
_LOTTO_REGEX = re.compile(r"\blotto\s*(?:n[°º.]?\s*)?(\d+|unico)\b", re.IGNORECASE)
_TOC_DOTS_RE = re.compile(r"\.{5,}\s*\d+\s*$")
_AMBIGUOUS_RE = re.compile(
    r"\b(?:potrebbe|parrebbe|sembrerebbe|eventuale|presunt\w+|ove|qualora|da\s+verificare|da\s+accertare)\b",
    re.IGNORECASE,
)
_HISTORICAL_RE = re.compile(r"\b(?:storic\w+|pregress\w+|in\s+passato|gi[aà]\s+estint\w+)\b", re.IGNORECASE)
_WEAK_BURDEN_BOILERPLATE_RE = re.compile(
    r"\b(?:"
    r"eventual\w+\s+vincol\w+\s+e\s+servit[ùu]\w*|"
    r"usi,\s*diritti,\s*ragioni\s+e\s+servit[ùu]\w*\s+attiv\w+\s+e\s+passiv\w+|"
    r"servit[ùu]\w*\s+attiv\w+\s+e\s+passiv\w+,\s+comunque\s+costituite|"
    r"nascenti\s+dallo\s+stato\s+dei\s+luoghi|"
    r"vincol\w+\s+derivant\w+\s+da\s+diritti\s+personal\w+\s+di\s+godimento"
    r")\b",
    re.IGNORECASE,
)
_VINCOLO_ACT_RE = re.compile(
    r"\b(?:"
    r"atto\s+di\s+vincolo|"
    r"atto\s+d['’]obbligo|"
    r"[èe]\s+stata\s+vincolat\w+|"
    r"sono\s+state\s+vincolat\w+"
    r")\b",
    re.IGNORECASE,
)
_SERVITU_ACT_RE = re.compile(
    r"\b(?:"
    r"atto\s+di\s+costituzione\s+servit[ùu]\w*|"
    r"costituit\w+\s+servit[ùu]\w*|"
    r"servit[ùu]\s+di\s+(?:passo|attraversamento)|"
    r"servit[ùu]\s+passiv\w+"
    r")\b",
    re.IGNORECASE,
)

_UNIVERSAL_SCOPE_MARKERS = [
    "tutti i beni",
    "tutti gli immobili",
    "tutte le unità",
    "intero lotto",
    "intero immobile",
    "beni del lotto",
    "immobili del lotto",
]

_FIELD_GUARDS = {
    "vincoli_status": [
        "scoped_legal_burdens_resolution",
        "weak_legal_burden_wording_stays_non_verificabile",
        "root_legal_leaf_first_rollup",
    ],
    "servitu_status": [
        "scoped_legal_burdens_resolution",
        "historical_servitu_mentions_not_auto_truth",
        "root_legal_leaf_first_rollup",
    ],
    "opponibilita_status": [
        "scoped_legal_burdens_resolution",
        "explicit_opponibilita_required_for_decisive_truth",
        "root_legal_leaf_first_rollup",
    ],
}

_VINCOLI_PRESENTE_RE = re.compile(
    r"\b(?:"
    r"sono\s+presenti\s+vincol\w+|"
    r"sussiston\w+\s+vincol\w+|"
    r"gravat\w+\s+da\s+vincol\w+|"
    r"soggett\w+\s+a\s+vincol\w+|"
    r"assoggettat\w+\s+a\s+vincol\w+|"
    r"vincol\w+[^\n]{0,40}\bpresente\b"
    r")\b",
    re.IGNORECASE,
)
_VINCOLI_ASSENTE_RE = re.compile(
    r"\b(?:"
    r"non\s+(?:sono\s+presenti|risultan\w+)\s+vincol\w+|"
    r"assenza\s+di\s+vincol\w+|"
    r"liber\w+\s+da\s+vincol\w+"
    r")\b",
    re.IGNORECASE,
)
_SERVITU_PRESENTE_RE = re.compile(
    r"\b(?:"
    r"sono\s+presenti\s+servit[ùu]\w*|"
    r"gravat\w+\s+da\s+servit[ùu]\w*|"
    r"soggett\w+\s+a\s+servit[ùu]\w*|"
    r"servit[ùu]\w*\s+(?:attiv\w+|passiv\w+)|"
    r"esiste\s+servit[ùu]\w*"
    r")\b",
    re.IGNORECASE,
)
_SERVITU_ASSENTE_RE = re.compile(
    r"\b(?:"
    r"non\s+(?:risultan\w+|sono\s+presenti)\s+servit[ùu]\w*|"
    r"assenza\s+di\s+servit[ùu]\w*|"
    r"liber\w+\s+da\s+servit[ùu]\w*"
    r")\b",
    re.IGNORECASE,
)
_OPPONIBILE_RE = re.compile(
    r"\b(?:"
    r"opponibil\w+[^\n]{0,30}(?:alla|alla\s+procedura|alla\s+procedura\s+esecutiva|all'esecuzione)|"
    r"[èe]\s+opponibil\w+"
    r")\b",
    re.IGNORECASE,
)
_NON_OPPONIBILE_RE = re.compile(
    r"\b(?:"
    r"non\s+opponibil\w+|"
    r"inopponibil\w+"
    r")\b",
    re.IGNORECASE,
)
_OPPONIBILITA_DA_VERIFICARE_RE = re.compile(
    r"\b(?:"
    r"opponibil\w+[^\n]{0,30}da\s+verificare|"
    r"da\s+verificare[^\n]{0,30}opponibil\w+"
    r")\b",
    re.IGNORECASE,
)

_SCOPE_LABELS = {
    "document_root": "Documento",
}

_ROLLUP_REASON_MIXED = "truth differs by scope"
_ROLLUP_REASON_INCOMPLETE = "not every leaf scope has decisive truth"
_ROLLUP_REASON_ABSENT = "no decisive same-scope statement found"


def _normalize_lotto_token(token: str) -> str:
    low = str(token or "").strip().lower()
    return "unico" if low == "unico" else re.sub(r"\D+", "", low)


def _available_scopes(state: RuntimeState, scope_type: str) -> list[str]:
    return sorted(scope.scope_id for scope in state.scopes.values() if scope.scope_type == scope_type)


def _matching_scope_ids(state: RuntimeState, text: str, *, scope_type: str) -> list[str]:
    regex = _BENE_REGEX if scope_type == "bene" else _LOTTO_REGEX
    available = set(_available_scopes(state, scope_type))
    matches: list[str] = []
    for token in regex.findall(text or ""):
        normalized = token if scope_type == "bene" else _normalize_lotto_token(token)
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
        return bene_scopes[0], "single_bene_fallback"
    lotto_scopes = _available_scopes(state, "lotto")
    if len(lotto_scopes) == 1:
        return lotto_scopes[0], "single_lotto_fallback"
    return "document_root", "document_root_fallback"


def _descendant_bene_scope_ids(state: RuntimeState, scope_id: str) -> list[str]:
    descendants: list[str] = []
    queue = [scope_id]
    seen: set[str] = set()
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


def _single_descendant_bene_scope_id(state: RuntimeState, scope_id: str) -> str | None:
    descendants = sorted(_descendant_bene_scope_ids(state, scope_id))
    if len(descendants) == 1:
        return descendants[0]
    return None


def _scope_label(state: RuntimeState, scope_id: str) -> str:
    if scope_id in _SCOPE_LABELS:
        return _SCOPE_LABELS[scope_id]
    scope = state.scopes.get(scope_id)
    if scope and scope.label:
        return str(scope.label)
    return scope_id


def _assign_scope(
    state: RuntimeState,
    *,
    quote: str,
    active_bene_scope_id: str | None,
    active_lotto_scope_id: str | None,
) -> tuple[str | None, str, bool]:
    bene_scope_ids = _matching_scope_ids(state, quote, scope_type="bene")
    lotto_scope_ids = _matching_scope_ids(state, quote, scope_type="lotto")
    universal = _is_universal_scope_statement(quote)

    if len(bene_scope_ids) == 1:
        return bene_scope_ids[0], "explicit_bene", False
    if len(bene_scope_ids) > 1:
        if universal and len(lotto_scope_ids) == 1:
            return lotto_scope_ids[0], "explicit_lotto_universal", True
        return None, "ambiguous_multi_bene_reference", False
    if len(lotto_scope_ids) == 1:
        if not universal:
            single_bene_scope_id = _single_descendant_bene_scope_id(state, lotto_scope_ids[0])
            if single_bene_scope_id:
                return single_bene_scope_id, "explicit_lotto_single_bene_section", False
        return lotto_scope_ids[0], "explicit_lotto_universal" if universal else "explicit_lotto", universal
    if universal and active_lotto_scope_id:
        return active_lotto_scope_id, "active_lotto_universal", True
    if active_bene_scope_id:
        return active_bene_scope_id, "active_bene_section", False
    if active_lotto_scope_id:
        if universal:
            return active_lotto_scope_id, "active_lotto_universal", True
        single_bene_scope_id = _single_descendant_bene_scope_id(state, active_lotto_scope_id)
        if single_bene_scope_id:
            return single_bene_scope_id, "active_lotto_single_bene_section", False
        return active_lotto_scope_id, "active_lotto_section", False
    scope_id, method = _smallest_fallback_scope_id(state)
    if scope_id == "document_root" and (_available_scopes(state, "bene") or _available_scopes(state, "lotto")):
        return None, "unresolved_unscoped_multi_scope_statement", False
    return scope_id, method, False


def _hit_sort_key(hit: dict[str, Any]) -> tuple[int, int, float, int]:
    return (
        int(hit.get("tier") or 0),
        0 if not hit.get("inherited") else 1,
        float(hit.get("confidence") or 0.0),
        -int(hit.get("page") or 0),
    )


def _field_payload(
    field_key: str,
    *,
    value: str,
    confidence: float,
    evidence: list[dict[str, Any]],
    source_scope_id: str | None = None,
    inherited: bool = False,
    verification_trail: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "value": value,
        "confidence": float(confidence),
        "evidence": evidence,
        "guards": list(_FIELD_GUARDS[field_key]),
    }
    if source_scope_id:
        payload["source_scope_id"] = source_scope_id
    if inherited:
        payload["inherited"] = True
    if verification_trail:
        payload["verification_trail"] = verification_trail
    return payload


def _verification_trail(
    state: RuntimeState,
    *,
    scope_id: str,
    reason_unresolved: str,
    hits: list[dict[str, Any]],
) -> dict[str, Any]:
    pages = sorted({int(hit.get("page") or 0) for hit in hits if int(hit.get("page") or 0) > 0})
    return {
        "checked_scope_label": _scope_label(state, scope_id),
        "checked_pages": pages,
        "key_evidence_found": [str(hit.get("quote") or "") for hit in hits[:2]],
        "reason_unresolved": reason_unresolved,
        "verify_next": [
            "Verifica il titolo o il vincolo richiamato nella sezione oneri e formalita.",
            "Conferma se la frase si riferisce al singolo bene o all'intero lotto.",
        ],
    }


def _detect_vincoli_status(line: str) -> tuple[str, int, float, str] | None:
    if _WEAK_BURDEN_BOILERPLATE_RE.search(line):
        return None
    if _VINCOLO_ACT_RE.search(line):
        return "PRESENTE", 4, 0.97, "explicit_vincolo_instrument"
    ambiguous = bool(_AMBIGUOUS_RE.search(line) or _HISTORICAL_RE.search(line))
    has_present = bool(_VINCOLI_PRESENTE_RE.search(line))
    has_absent = bool(_VINCOLI_ASSENTE_RE.search(line))
    if has_absent and not ambiguous:
        return "ASSENTE", 2, 0.88, "explicit_vincoli_assente"
    if has_present and not ambiguous:
        return "PRESENTE", 2, 0.9, "explicit_vincoli_presente"
    if has_present and has_absent:
        return "NON_VERIFICABILE", 1, 0.5, "same_line_mixed_vincoli_signal"
    return None


def _detect_servitu_status(line: str) -> tuple[str, int, float, str] | None:
    if _WEAK_BURDEN_BOILERPLATE_RE.search(line):
        return None
    if _SERVITU_ACT_RE.search(line):
        return "PRESENTE", 4, 0.97, "explicit_servitu_instrument"
    ambiguous = bool(_AMBIGUOUS_RE.search(line) or _HISTORICAL_RE.search(line))
    has_present = bool(_SERVITU_PRESENTE_RE.search(line))
    has_absent = bool(_SERVITU_ASSENTE_RE.search(line))
    if has_absent and not ambiguous:
        return "ASSENTE", 2, 0.88, "explicit_servitu_assente"
    if has_present and not ambiguous:
        return "PRESENTE", 2, 0.9, "explicit_servitu_presente"
    if has_present and has_absent:
        return "NON_VERIFICABILE", 1, 0.5, "same_line_mixed_servitu_signal"
    return None


def _detect_opponibilita_status(line: str) -> tuple[str, int, float, str] | None:
    ambiguous = bool(_AMBIGUOUS_RE.search(line) or _HISTORICAL_RE.search(line))
    if _NON_OPPONIBILE_RE.search(line) and not ambiguous:
        return "NON_OPPONIBILE", 3, 0.92, "explicit_non_opponibile"
    if _OPPONIBILITA_DA_VERIFICARE_RE.search(line):
        return "DA_VERIFICARE", 2, 0.78, "explicit_da_verificare"
    if _OPPONIBILE_RE.search(line) and not ambiguous:
        return "OPPONIBILE", 3, 0.9, "explicit_opponibile"
    return None


def _collect_scoped_hits(state: RuntimeState) -> dict[str, list[dict[str, Any]]]:
    collected: dict[str, list[dict[str, Any]]] = defaultdict(list)
    hit_index = 0
    state.candidates["vincoli_status"] = []
    state.candidates["servitu_status"] = []
    state.candidates["opponibilita_status"] = []

    for idx, page in enumerate(state.pages, start=1):
        text = str((page or {}).get("text") or "")
        page_number = int((page or {}).get("page_number") or (page or {}).get("page") or idx)
        active_lotto_scope_id: str | None = None
        active_bene_scope_id: str | None = None

        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line or _TOC_DOTS_RE.search(line):
                continue

            lotto_matches = _matching_scope_ids(state, line, scope_type="lotto")
            bene_matches = _matching_scope_ids(state, line, scope_type="bene")
            if len(lotto_matches) == 1:
                active_lotto_scope_id = lotto_matches[0]
            if len(bene_matches) == 1:
                active_bene_scope_id = bene_matches[0]

            detections = {
                "vincoli_status": _detect_vincoli_status(line),
                "servitu_status": _detect_servitu_status(line),
                "opponibilita_status": _detect_opponibilita_status(line),
            }
            for field_key, detection in detections.items():
                if detection is None:
                    continue
                value, tier, confidence, signal = detection
                scope_id, ownership_method, universal = _assign_scope(
                    state,
                    quote=line,
                    active_bene_scope_id=active_bene_scope_id,
                    active_lotto_scope_id=active_lotto_scope_id,
                )
                if not scope_id:
                    continue
                hit_index += 1
                ownership = state.attach_evidence_ownership(
                    scope_id=scope_id,
                    field_target=f"legal.{field_key}",
                    source_page=page_number,
                    quote=line,
                    confidence=confidence,
                    ownership_method=ownership_method,
                    evidence_id=f"{field_key}_{page_number}_{hit_index}",
                )
                evidence = make_evidence(page_number, line, field_key, ["legal"], confidence)
                hit = {
                    "field_key": field_key,
                    "value": value,
                    "tier": tier,
                    "confidence": confidence,
                    "page": page_number,
                    "quote": line,
                    "signal": signal,
                    "scope_id": scope_id,
                    "ownership_method": ownership_method,
                    "universal": universal,
                    "inherited": False,
                    "inherited_from_scope_id": None,
                    "evidence_id": ownership.evidence_id,
                    "evidence": [evidence],
                }
                collected[field_key].append(hit)
                state.candidates[field_key].append(
                    {
                        "value": value,
                        "page": page_number,
                        "quote": line,
                        "scope_id": scope_id,
                        "ownership_method": ownership_method,
                        "signal": signal,
                        "confidence": confidence,
                    }
                )
    return collected


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


def _resolve_scope_hits(
    state: RuntimeState,
    *,
    scope_id: str,
    field_key: str,
    hits: list[dict[str, Any]],
) -> dict[str, Any]:
    ordered = sorted(hits, key=_hit_sort_key, reverse=True)
    internal = {
        "raw_conflict_detected": False,
        "resolution_reason": None,
        "unresolved_reason": None,
        "competing_evidence_ids": [],
        "winner_inherited": False,
        "winner_inherited_from_scope_id": None,
    }
    if not ordered:
        return {"payload": None, "internal": internal}

    unique_values = {str(hit.get("value") or "") for hit in ordered}
    internal["raw_conflict_detected"] = len(unique_values) > 1

    top_hit = ordered[0]
    best_value = str(top_hit.get("value") or "")
    best_tier = int(top_hit.get("tier") or 0)
    best_hits = [hit for hit in ordered if int(hit.get("tier") or 0) == best_tier]
    best_values = {str(hit.get("value") or "") for hit in best_hits}
    internal["competing_evidence_ids"] = [str(hit.get("evidence_id") or "") for hit in best_hits[1:3]]

    if len(best_values) > 1 or best_value == "NON_VERIFICABILE":
        internal["unresolved_reason"] = "same_scope_conflict_survives_expanded_reading"
        payload = _field_payload(
            field_key,
            value="NON_VERIFICABILE",
            confidence=0.45,
            evidence=[copy.deepcopy(item) for hit in best_hits[:2] for item in hit.get("evidence", [])],
            source_scope_id=scope_id,
            verification_trail=_verification_trail(
                state,
                scope_id=scope_id,
                reason_unresolved=internal["unresolved_reason"],
                hits=best_hits,
            ),
        )
        return {"payload": payload, "internal": internal}

    internal["resolution_reason"] = "highest_tier_signal_wins"
    internal["winner_inherited"] = bool(top_hit.get("inherited"))
    internal["winner_inherited_from_scope_id"] = top_hit.get("inherited_from_scope_id")
    payload = _field_payload(
        field_key,
        value=best_value,
        confidence=float(top_hit.get("confidence") or 0.0),
        evidence=[copy.deepcopy(item) for item in top_hit.get("evidence", [])],
        source_scope_id=str(top_hit.get("scope_id") or scope_id),
        inherited=bool(top_hit.get("inherited")),
    )
    return {"payload": payload, "internal": internal}


def _leaf_scope_ids(state: RuntimeState) -> list[str]:
    bene_scopes = _available_scopes(state, "bene")
    if bene_scopes:
        return bene_scopes
    lotto_scopes = _available_scopes(state, "lotto")
    if lotto_scopes:
        return lotto_scopes
    return ["document_root"]


def _root_rollup_payload(
    state: RuntimeState,
    *,
    field_key: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    leaf_scope_ids = _leaf_scope_ids(state)
    internal = {
        "resolved_from_leaf_scopes": leaf_scope_ids,
        "unresolved_reason": None,
    }

    if leaf_scope_ids == ["document_root"]:
        direct_root = state.scopes["document_root"].legal.get(field_key)
        if isinstance(direct_root, dict):
            return copy.deepcopy(direct_root), internal

    decisive = []
    missing_scope_ids = []
    for scope_id in leaf_scope_ids:
        scope = state.scopes.get(scope_id)
        payload = scope.legal.get(field_key) if scope else None
        if not isinstance(payload, dict) or str(payload.get("value") or "") == "NON_VERIFICABILE":
            missing_scope_ids.append(scope_id)
            continue
        decisive.append((scope_id, payload))

    if not decisive:
        internal["unresolved_reason"] = _ROLLUP_REASON_ABSENT
        payload = _field_payload(
            field_key,
            value="NON_VERIFICABILE",
            confidence=0.0,
            evidence=[],
            source_scope_id="document_root",
            verification_trail={
                "checked_scope_label": "Documento",
                "checked_scope_count": len(leaf_scope_ids),
                "reason_unresolved": _ROLLUP_REASON_ABSENT,
                "verify_next": [
                    "Verifica vincoli, servitu e opponibilita separatamente per ciascun bene.",
                ],
            },
        )
        return payload, internal

    decisive_values = {str(payload.get("value") or "") for _, payload in decisive}
    if len(decisive_values) > 1:
        internal["unresolved_reason"] = _ROLLUP_REASON_MIXED
        payload = _field_payload(
            field_key,
            value="NON_VERIFICABILE",
            confidence=max(float(payload.get("confidence") or 0.0) for _, payload in decisive),
            evidence=[copy.deepcopy(item) for _, payload in decisive[:2] for item in payload.get("evidence", [])[:1]],
            source_scope_id="document_root",
            verification_trail={
                "checked_scope_label": "Documento",
                "checked_scope_count": len(leaf_scope_ids),
                "reason_unresolved": _ROLLUP_REASON_MIXED,
                "verify_next": [
                    "Verifica il titolo richiamato separatamente per ciascun bene.",
                ],
            },
        )
        return payload, internal

    if missing_scope_ids:
        internal["unresolved_reason"] = _ROLLUP_REASON_INCOMPLETE
        payload = _field_payload(
            field_key,
            value="NON_VERIFICABILE",
            confidence=max(float(payload.get("confidence") or 0.0) for _, payload in decisive),
            evidence=[copy.deepcopy(item) for _, payload in decisive[:1] for item in payload.get("evidence", [])[:1]],
            source_scope_id="document_root",
            verification_trail={
                "checked_scope_label": "Documento",
                "checked_scope_count": len(leaf_scope_ids),
                "reason_unresolved": _ROLLUP_REASON_INCOMPLETE,
                "verify_next": [
                    "Verifica i beni senza evidenza legale esplicita.",
                ],
            },
        )
        return payload, internal

    winner_scope_id, winner_payload = decisive[0]
    payload = copy.deepcopy(winner_payload)
    payload["source_scope_id"] = winner_scope_id
    return payload, internal


def _legacy_priority_legal_items(state: RuntimeState) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    cancellable = []
    surviving = []
    background = []
    for idx, page in enumerate(state.pages, start=1):
        text = str((page or {}).get("text") or "")
        low = text.lower()
        page_number = int((page or {}).get("page_number") or (page or {}).get("page") or idx)
        if (
            "saranno cancellati a cura e spese della" in low
            or "formalità da cancellare" in low
            or "cancellati con il decreto di trasferimento" in low
        ):
            if "ipoteca" in low:
                cancellable.append(
                    {"kind": "ipoteca", "evidence": [make_evidence(page_number, text[:520], "cancellable_encumbrance", ["legal"], 0.92)]}
                )
            if "pignoramento" in low:
                cancellable.append(
                    {"kind": "pignoramento", "evidence": [make_evidence(page_number, text[:520], "cancellable_encumbrance", ["legal"], 0.92)]}
                )
        if "resteranno a carico dell'acquirente" in low and "non note" not in low and "non noti" not in low:
            surviving.append(
                {"kind": "surviving_burden", "evidence": [make_evidence(page_number, text[:520], "surviving_encumbrance", ["legal"], 0.9)]}
            )
        if "ipoteca" in low and not cancellable:
            background.append({"kind": "ipoteca", "evidence": [make_evidence(page_number, text[:520], "background_legal", ["legal"], 0.65)]})
    return cancellable, surviving, background


def run_legal_agent(state: RuntimeState) -> None:
    cancellable, surviving, background = _legacy_priority_legal_items(state)

    collected = _collect_scoped_hits(state)
    scope_internal = defaultdict(dict)
    for field_key, direct_hits in collected.items():
        propagated_hits = _propagate_universal_hits(state, direct_hits)
        hits_by_scope: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for hit in propagated_hits:
            hits_by_scope[str(hit.get("scope_id") or "document_root")].append(hit)

        for scope_id, hits in hits_by_scope.items():
            resolved = _resolve_scope_hits(state, scope_id=scope_id, field_key=field_key, hits=hits)
            payload = resolved["payload"]
            internal = resolved["internal"]
            if payload:
                state.scopes[scope_id].legal[field_key] = payload
            scope_internal[scope_id][field_key] = internal

    for scope_id, internal in scope_internal.items():
        scope = state.scopes.get(scope_id)
        if not scope:
            continue
        metadata = scope.metadata.setdefault("legal_internal", {})
        metadata.update(copy.deepcopy(internal))

    state.canonical_case.legal = {
        "guards": sorted({guard for guards in _FIELD_GUARDS.values() for guard in guards}),
        "cancellable": cancellable,
        "surviving": surviving,
        "background": background,
        "top_issue_guard": "cancellable_formalities_cannot_auto_dominate_priority",
    }

    root_scope = state.scopes["document_root"]
    root_internal = root_scope.metadata.setdefault("legal_internal", {})
    for field_key in _FIELD_GUARDS:
        payload, internal = _root_rollup_payload(state, field_key=field_key)
        state.canonical_case.legal[field_key] = payload
        root_internal[field_key] = internal

    if surviving:
        item = surviving[0]
        issue = CanonicalIssue(
            code="LEGAL_SURVIVING_BURDEN",
            title_it="Vincolo che resta a carico dell'acquirente",
            severity="RED",
            category="legal",
            priority_score=95.0,
            evidence=item["evidence"],
            summary_it="Il documento indica un vincolo che resta a carico dell'acquirente.",
            action_it="Verifica legale immediata prima dell'offerta.",
        )
        state.issues.append(issue)
        state.judgments["legal_top_issue"] = Judgment("legal_top_issue", issue.title_it, "FOUND", 0.9, issue.evidence, issue.summary_it)
    else:
        state.judgments["legal_top_issue"] = Judgment(
            "legal_top_issue",
            "FORMALITA_CANCELLABILI" if cancellable else None,
            "FOUND" if cancellable else "NOT_FOUND",
            0.7 if cancellable else 0.0,
            cancellable[0]["evidence"] if cancellable else [],
            "cancellable legal items kept as background, not automatic buyer-side top risk",
        )
