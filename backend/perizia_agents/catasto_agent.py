from __future__ import annotations

import copy
from dataclasses import asdict, is_dataclass
import re
from typing import Any

from perizia_runtime.state import CanonicalScopeState, Judgment, RuntimeState
from perizia_tools.catasto_parser_tool import catasto_candidates
from perizia_tools.quota_parser_tool import quota_candidates


_BENE_REGEX = re.compile(r"\bbene\s*n[°º.]?\s*(\d+)\b", re.IGNORECASE)
_LOTTO_REGEX = re.compile(r"\blotto\s*(?:n[°º.]?\s*)?(\d+|unico)\b", re.IGNORECASE)
_UNIVERSAL_SCOPE_MARKERS = [
    "tutti i beni",
    "tutti gli immobili",
    "intero lotto",
    "intero immobile",
    "beni del lotto",
    "immobili del lotto",
]
_QUOTA_GUARDS = [
    "fraction_pattern_required",
    "date_like_tokens_rejected",
    "scoped_rights_resolution",
    "root_quota_leaf_first_rollup",
]
_CAT_FIELD_KEYS = ("foglio", "particella", "subalterno", "categoria")
_CATASTO_GUARDS = [
    "scoped_catasto_resolution",
    "root_catasto_leaf_first_rollup",
]
_SCOPE_RIGHTS_TO_DICT_PATCHED = False


def _candidate_quote(candidate: Any) -> str:
    if candidate.evidence:
        return str(candidate.evidence[0].quote or "")
    return ""


def _candidate_page(candidate: Any) -> int:
    if candidate.evidence:
        return int(candidate.evidence[0].page or 0)
    return 0


def _candidate_priority(candidate: Any) -> int:
    metadata = candidate.metadata if isinstance(getattr(candidate, "metadata", None), dict) else {}
    try:
        return int(metadata.get("priority", 2))
    except Exception:
        return 2


def _candidate_match_start(candidate: Any) -> int | None:
    metadata = candidate.metadata if isinstance(getattr(candidate, "metadata", None), dict) else {}
    value = metadata.get("match_start")
    try:
        return int(value) if value is not None else None
    except Exception:
        return None


def _normalize_lotto_token(token: str) -> str:
    low = str(token or "").strip().lower()
    return "unico" if low == "unico" else re.sub(r"\D+", "", low)


def _available_scope_ids(state: RuntimeState, scope_type: str) -> set[str]:
    return {scope.scope_id for scope in state.scopes.values() if scope.scope_type == scope_type}


def _matching_scope_ids(state: RuntimeState, text: str, *, scope_type: str) -> list[str]:
    regex = _BENE_REGEX if scope_type == "bene" else _LOTTO_REGEX
    available = _available_scope_ids(state, scope_type)
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


def _single_scope_fallback(state: RuntimeState) -> tuple[str, str]:
    bene_scope_ids = sorted(_available_scope_ids(state, "bene"))
    if len(bene_scope_ids) == 1:
        return bene_scope_ids[0], "single_bene_fallback"
    lotto_scope_ids = sorted(_available_scope_ids(state, "lotto"))
    if len(lotto_scope_ids) == 1:
        return lotto_scope_ids[0], "single_lotto_fallback"
    return "document_root", "document_root_fallback"


def _nearest_scope_from_page_context(state: RuntimeState, candidate: Any) -> tuple[str, str, bool] | None:
    page_number = _candidate_page(candidate)
    anchor = _candidate_match_start(candidate)
    if anchor is None:
        return None
    page_text = ""
    for idx, page in enumerate(state.pages or [], start=1):
        page_no = int((page or {}).get("page_number") or (page or {}).get("page") or idx)
        if page_no == page_number:
            page_text = str((page or {}).get("text") or "")
            break
    if not page_text:
        return None

    nearest: tuple[int, str, str] | None = None
    for match in _BENE_REGEX.finditer(page_text):
        scope_id = f"bene:{match.group(1)}"
        if scope_id not in state.scopes:
            continue
        distance = anchor - match.start() if match.start() <= anchor else match.start() - anchor + 500
        candidate_scope = (distance, scope_id, "page_context_bene_heading")
        if nearest is None or candidate_scope < nearest:
            nearest = candidate_scope
    for match in _LOTTO_REGEX.finditer(page_text):
        scope_id = f"lotto:{_normalize_lotto_token(match.group(1))}"
        if scope_id not in state.scopes:
            continue
        distance = anchor - match.start() if match.start() <= anchor else match.start() - anchor + 500
        candidate_scope = (distance, scope_id, "page_context_lotto_heading")
        if nearest is None or candidate_scope < nearest:
            nearest = candidate_scope
    if nearest is None:
        return None
    _, scope_id, ownership_method = nearest
    return scope_id, ownership_method, False


def _scope_id_for_candidate(state: RuntimeState, candidate: Any) -> tuple[str, str, bool]:
    metadata = candidate.metadata if isinstance(getattr(candidate, "metadata", None), dict) else {}
    scope_hint = str(metadata.get("scope_hint") or "").strip()
    if scope_hint in state.scopes:
        return scope_hint, "parser_heading_scope_hint", False
    quote = _candidate_quote(candidate)
    value = str(candidate.value or "")
    anchor_end = quote.lower().find(value.lower())
    before = quote[: anchor_end + len(value)] if anchor_end >= 0 else quote
    after = quote[anchor_end:] if anchor_end >= 0 else ""

    before_benes = list(_BENE_REGEX.finditer(before))
    if before_benes:
        scope_id = f"bene:{before_benes[-1].group(1)}"
        if scope_id in state.scopes:
            return scope_id, "explicit_bene_local_match", False

    before_lotti = list(_LOTTO_REGEX.finditer(before))
    if before_lotti:
        scope_id = f"lotto:{_normalize_lotto_token(before_lotti[-1].group(1))}"
        if scope_id in state.scopes:
            universal = _is_universal_scope_statement(quote)
            return scope_id, "explicit_lotto_universal" if universal else "explicit_lotto_local_match", universal

    next_bene = _BENE_REGEX.search(after)
    if next_bene:
        scope_id = f"bene:{next_bene.group(1)}"
        if scope_id in state.scopes:
            return scope_id, "forward_bene_local_match", False

    next_lotto = _LOTTO_REGEX.search(after)
    if next_lotto:
        scope_id = f"lotto:{_normalize_lotto_token(next_lotto.group(1))}"
        if scope_id in state.scopes:
            universal = _is_universal_scope_statement(quote)
            return scope_id, "forward_lotto_universal" if universal else "forward_lotto_local_match", universal

    bene_scope_ids = _matching_scope_ids(state, quote, scope_type="bene")
    lotto_scope_ids = _matching_scope_ids(state, quote, scope_type="lotto")
    universal = _is_universal_scope_statement(quote)
    if len(bene_scope_ids) == 1:
        return bene_scope_ids[0], "explicit_bene_quote", False
    if len(bene_scope_ids) > 1:
        if universal and len(lotto_scope_ids) == 1:
            return lotto_scope_ids[0], "explicit_lotto_universal", True
        return "document_root", "ambiguous_multi_bene_reference", False
    if len(lotto_scope_ids) == 1:
        return lotto_scope_ids[0], "explicit_lotto_universal" if universal else "explicit_lotto_quote", universal
    page_context_match = _nearest_scope_from_page_context(state, candidate)
    if page_context_match is not None:
        return page_context_match
    scope_id, ownership_method = _single_scope_fallback(state)
    return scope_id, ownership_method, False


def _quota_payload(
    value: str | None,
    confidence: float,
    evidence: list[Any],
    *,
    source_scope_id: str | None = None,
    inherited: bool = False,
) -> dict[str, Any]:
    payload = {
        "value": value,
        "confidence": float(confidence),
        "evidence": evidence,
        "guards": list(_QUOTA_GUARDS),
    }
    if source_scope_id:
        payload["source_scope_id"] = source_scope_id
    if inherited:
        payload["inherited"] = True
    return payload


def _catasto_payload(
    value: str | None,
    confidence: float,
    evidence: list[Any],
    *,
    source_scope_id: str | None = None,
    inherited: bool = False,
) -> dict[str, Any]:
    payload = {
        "value": value,
        "confidence": float(confidence),
        "evidence": evidence,
        "guards": list(_CATASTO_GUARDS),
    }
    if source_scope_id:
        payload["source_scope_id"] = source_scope_id
    if inherited:
        payload["inherited"] = True
    return payload


def _collect_quota_candidates(state: RuntimeState) -> list[dict[str, Any]]:
    grouped: list[dict[str, Any]] = []
    index = 0
    raw_candidates = quota_candidates(state.pages, state.result)
    state.candidates["quota"] = raw_candidates
    for candidate in raw_candidates:
        quote = _candidate_quote(candidate)
        page = _candidate_page(candidate)
        scope_id, ownership_method, universal = _scope_id_for_candidate(state, candidate)
        index += 1
        ownership = state.attach_evidence_ownership(
            scope_id=scope_id,
            field_target="rights.quota",
            source_page=page,
            quote=quote,
            confidence=float(candidate.confidence),
            ownership_method=ownership_method,
            evidence_id=f"quota_{page}_{index}",
        )
        grouped.append(
            {
                "field_key": "quota",
                "value": str(candidate.value),
                "confidence": float(candidate.confidence),
                "evidence": list(candidate.evidence),
                "page": page,
                "quote": quote,
                "scope_id": scope_id,
                "ownership_method": ownership_method,
                "universal": universal,
                "evidence_id": ownership.evidence_id,
                "priority": _candidate_priority(candidate),
            }
        )
    return grouped


def _collect_structured_catasto_candidates(state: RuntimeState) -> dict[str, dict[str, list[dict[str, Any]]]]:
    by_field: dict[str, dict[str, list[dict[str, Any]]]] = {field_key: {} for field_key in _CAT_FIELD_KEYS}
    counters = {field_key: 0 for field_key in _CAT_FIELD_KEYS}
    raw_candidates = catasto_candidates(state.pages)
    for field_key in _CAT_FIELD_KEYS:
        state.candidates[field_key] = [candidate for candidate in raw_candidates if candidate.field_key == field_key]
    for candidate in raw_candidates:
        if candidate.field_key not in _CAT_FIELD_KEYS:
            continue
        quote = _candidate_quote(candidate)
        page = _candidate_page(candidate)
        scope_id, ownership_method, universal = _scope_id_for_candidate(state, candidate)
        counters[candidate.field_key] += 1
        ownership = state.attach_evidence_ownership(
            scope_id=scope_id,
            field_target=f"catasto.{candidate.field_key}",
            source_page=page,
            quote=quote,
            confidence=float(candidate.confidence),
            ownership_method=ownership_method,
            evidence_id=f"{candidate.field_key}_{page}_{counters[candidate.field_key]}",
        )
        by_field.setdefault(candidate.field_key, {}).setdefault(scope_id, []).append(
            {
                "field_key": candidate.field_key,
                "value": str(candidate.value),
                "confidence": float(candidate.confidence),
                "evidence": list(candidate.evidence),
                "page": page,
                "quote": quote,
                "scope_id": scope_id,
                "ownership_method": ownership_method,
                "universal": universal,
                "evidence_id": ownership.evidence_id,
                "priority": _candidate_priority(candidate),
            }
        )
    return by_field


def _resolve_scope_value(scope_id: str, candidates: list[dict[str, Any]], *, conflict_reason: str) -> dict[str, Any]:
    if not candidates:
        return {"value": None, "winner": None, "conflict": False, "scope_id": scope_id}
    max_priority = max(int(item.get("priority", 2)) for item in candidates)
    ranked_candidates = [item for item in candidates if int(item.get("priority", 2)) == max_priority]
    distinct_values = sorted({str(item["value"]) for item in ranked_candidates})
    if len(distinct_values) > 1:
        return {
            "value": None,
            "winner": None,
            "conflict": True,
            "reason": conflict_reason,
            "competing_values": distinct_values,
            "candidate_ids": [item["evidence_id"] for item in ranked_candidates],
            "scope_id": scope_id,
            "priority": max_priority,
        }
    winner = sorted(
        ranked_candidates,
        key=lambda item: (float(item["confidence"]), -len(str(item["quote"] or "")), -int(item["page"])),
        reverse=True,
    )[0]
    return {
        "value": str(winner["value"]),
        "winner": winner,
        "conflict": False,
        "candidate_ids": [item["evidence_id"] for item in ranked_candidates],
        "scope_id": scope_id,
        "priority": max_priority,
    }


def _resolve_scope_quota(scope_id: str, candidates: list[dict[str, Any]]) -> dict[str, Any]:
    return _resolve_scope_value(scope_id, candidates, conflict_reason="same_scope_quota_conflict")


def _resolve_scope_catasto_field(scope_id: str, field_key: str, candidates: list[dict[str, Any]]) -> dict[str, Any]:
    return _resolve_scope_value(scope_id, candidates, conflict_reason=f"same_scope_{field_key}_conflict")


def _patched_to_dict(value: Any) -> Any:
    if is_dataclass(value):
        data = {k: _patched_to_dict(v) for k, v in asdict(value).items()}
        if isinstance(value, CanonicalScopeState):
            rights = getattr(value, "rights", None)
            if isinstance(rights, dict):
                data["rights"] = _patched_to_dict(rights)
        return data
    if isinstance(value, dict):
        return {k: _patched_to_dict(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_patched_to_dict(v) for v in value]
    return value


def _ensure_scope_rights_serialization_patch() -> None:
    global _SCOPE_RIGHTS_TO_DICT_PATCHED
    if _SCOPE_RIGHTS_TO_DICT_PATCHED:
        return
    import perizia_runtime.runtime as runtime_module
    import perizia_runtime.state as state_module

    runtime_module.to_dict = _patched_to_dict
    state_module.to_dict = _patched_to_dict
    _SCOPE_RIGHTS_TO_DICT_PATCHED = True


def _scope_rights(scope: Any) -> dict[str, Any]:
    rights = getattr(scope, "rights", None)
    if not isinstance(rights, dict):
        rights = {}
        setattr(scope, "rights", rights)
    return rights


def _child_scopes_with_field(state: RuntimeState, parent_scope_id: str, field_key: str) -> list[Any]:
    return [
        child
        for child in state.list_child_scopes(parent_scope_id)
        if isinstance(child.catasto, dict)
        and isinstance(child.catasto.get(field_key), dict)
        and child.catasto[field_key].get("value") is not None
    ]


def _child_scopes_with_quota(state: RuntimeState, parent_scope_id: str) -> list[Any]:
    return [
        child
        for child in state.list_child_scopes(parent_scope_id)
        if isinstance(_scope_rights(child).get("quota"), dict)
        and _scope_rights(child)["quota"].get("value") is not None
    ]


def _apply_inherited_quota_to_children(state: RuntimeState, parent_scope_id: str) -> None:
    parent_scope = state.scopes[parent_scope_id]
    quota = _scope_rights(parent_scope).get("quota")
    if not isinstance(quota, dict) or quota.get("value") is None:
        return
    internal = parent_scope.metadata.get("rights_internal", {}).get("quota", {})
    if not internal.get("inherits_to_children"):
        return
    for child in state.list_child_scopes(parent_scope_id):
        if child.scope_type != "bene" or _scope_rights(child).get("quota"):
            continue
        inherited_payload = _quota_payload(
            str(quota["value"]),
            float(quota.get("confidence", 0.0)),
            copy.deepcopy(quota.get("evidence", [])),
            source_scope_id=parent_scope_id,
            inherited=True,
        )
        _scope_rights(child)["quota"] = inherited_payload
        child.metadata["rights_internal"] = {
            "quota": {
                "value": str(quota["value"]),
                "winner": {
                    "scope_id": parent_scope_id,
                    "evidence": copy.deepcopy(quota.get("evidence", [])),
                    "confidence": float(quota.get("confidence", 0.0)),
                },
                "conflict": False,
                "derived_from_scope_id": parent_scope_id,
                "resolution_reason": "scope_inheritance",
            }
        }


def _apply_inherited_field_to_children(state: RuntimeState, parent_scope_id: str, field_key: str) -> None:
    parent_scope = state.scopes[parent_scope_id]
    payload = parent_scope.catasto.get(field_key) if isinstance(parent_scope.catasto, dict) else None
    if not isinstance(payload, dict) or payload.get("value") is None:
        return
    internal = parent_scope.metadata.get("catasto_internal", {}).get(field_key, {})
    if not internal.get("inherits_to_children"):
        return
    for child in state.list_child_scopes(parent_scope_id):
        if child.scope_type != "bene" or child.catasto.get(field_key):
            continue
        inherited_payload = _catasto_payload(
            str(payload["value"]),
            float(payload.get("confidence", 0.0)),
            copy.deepcopy(payload.get("evidence", [])),
            source_scope_id=parent_scope_id,
            inherited=True,
        )
        child.catasto[field_key] = inherited_payload
        child.metadata.setdefault("catasto_internal", {})[field_key] = {
            "value": str(payload["value"]),
            "winner": {
                "scope_id": parent_scope_id,
                "evidence": copy.deepcopy(payload.get("evidence", [])),
                "confidence": float(payload.get("confidence", 0.0)),
            },
            "conflict": False,
            "derived_from_scope_id": parent_scope_id,
            "resolution_reason": "scope_inheritance",
        }


def _derive_root_field(state: RuntimeState, field_key: str, direct_root_resolution: dict[str, Any]) -> dict[str, Any]:
    leaf_scopes = [
        scope
        for scope in state.scopes.values()
        if scope.scope_type == "bene"
        and isinstance(scope.catasto.get(field_key), dict)
        and scope.catasto[field_key].get("value") is not None
    ]
    if not leaf_scopes:
        leaf_scopes = [
            scope
            for scope in state.scopes.values()
            if scope.scope_type == "lotto"
            and not any(child.scope_type == "bene" for child in state.list_child_scopes(scope.scope_id))
            and isinstance(scope.catasto.get(field_key), dict)
            and scope.catasto[field_key].get("value") is not None
        ]

    if leaf_scopes:
        values = [str(scope.catasto[field_key]["value"]) for scope in leaf_scopes]
        unique_values = sorted(set(values))
        if len(unique_values) == 1:
            best = max(leaf_scopes, key=lambda scope: float(scope.catasto[field_key].get("confidence", 0.0)))
            return {
                "value": unique_values[0],
                "confidence": float(best.catasto[field_key].get("confidence", 0.0)),
                "evidence": copy.deepcopy(best.catasto[field_key].get("evidence", [])),
                "source_scope_id": best.scope_id,
                "internal": {
                    "derived_from_scopes": [scope.scope_id for scope in leaf_scopes],
                    "resolution_reason": "uniform_leaf_scope_collapse",
                },
            }
        return {
            "value": None,
            "confidence": 0.0,
            "evidence": [],
            "internal": {
                "derived_from_scopes": [scope.scope_id for scope in leaf_scopes],
                "unresolved_reason": "different_scopes_have_different_resolved_truth",
            },
        }

    if direct_root_resolution.get("value") is not None:
        winner = direct_root_resolution.get("winner") or {}
        return {
            "value": str(direct_root_resolution["value"]),
            "confidence": float(winner.get("confidence", 0.0)),
            "evidence": copy.deepcopy(winner.get("evidence", [])),
            "source_scope_id": "document_root",
            "internal": {
                "candidate_ids": direct_root_resolution.get("candidate_ids", []),
                "resolution_reason": "direct_root_resolution",
            },
        }

    return {
        "value": None,
        "confidence": 0.0,
        "evidence": [],
        "internal": {
            "unresolved_reason": direct_root_resolution.get("reason") or f"no_{field_key}_candidates",
            "candidate_ids": direct_root_resolution.get("candidate_ids", []),
        },
    }


def _derive_root_quota(state: RuntimeState, direct_root_resolution: dict[str, Any]) -> dict[str, Any]:
    leaf_scopes = [
        scope
        for scope in state.scopes.values()
        if scope.scope_type == "bene"
        and isinstance(_scope_rights(scope).get("quota"), dict)
        and _scope_rights(scope)["quota"].get("value") is not None
    ]
    if not leaf_scopes:
        leaf_scopes = [
            scope
            for scope in state.scopes.values()
            if scope.scope_type == "lotto"
            and not any(child.scope_type == "bene" for child in state.list_child_scopes(scope.scope_id))
            and isinstance(_scope_rights(scope).get("quota"), dict)
            and _scope_rights(scope)["quota"].get("value") is not None
        ]

    if leaf_scopes:
        values = [str(_scope_rights(scope)["quota"]["value"]) for scope in leaf_scopes]
        unique_values = sorted(set(values))
        if len(unique_values) == 1:
            best = max(leaf_scopes, key=lambda scope: float(_scope_rights(scope)["quota"].get("confidence", 0.0)))
            return {
                "value": unique_values[0],
                "confidence": float(_scope_rights(best)["quota"].get("confidence", 0.0)),
                "evidence": copy.deepcopy(_scope_rights(best)["quota"].get("evidence", [])),
                "source_scope_id": best.scope_id,
                "internal": {
                    "derived_from_scopes": [scope.scope_id for scope in leaf_scopes],
                    "resolution_reason": "uniform_leaf_scope_collapse",
                },
            }
        return {
            "value": None,
            "confidence": 0.0,
            "evidence": [],
            "internal": {
                "derived_from_scopes": [scope.scope_id for scope in leaf_scopes],
                "unresolved_reason": "different_scopes_have_different_resolved_truth",
            },
        }

    if direct_root_resolution.get("value") is not None:
        winner = direct_root_resolution.get("winner") or {}
        return {
            "value": str(direct_root_resolution["value"]),
            "confidence": float(winner.get("confidence", 0.0)),
            "evidence": copy.deepcopy(winner.get("evidence", [])),
            "source_scope_id": "document_root",
            "internal": {
                "candidate_ids": direct_root_resolution.get("candidate_ids", []),
                "resolution_reason": "direct_root_resolution",
            },
        }

    return {
        "value": None,
        "confidence": 0.0,
        "evidence": [],
        "internal": {
            "unresolved_reason": direct_root_resolution.get("reason") or "no_quota_candidates",
            "candidate_ids": direct_root_resolution.get("candidate_ids", []),
        },
    }


def _write_scoped_catasto_field(state: RuntimeState, field_key: str, by_scope: dict[str, list[dict[str, Any]]]) -> None:
    for scope in [scope for scope in state.scopes.values() if scope.scope_type == "bene"]:
        resolution = _resolve_scope_catasto_field(scope.scope_id, field_key, by_scope.get(scope.scope_id, []))
        if resolution.get("value") is not None:
            winner = resolution["winner"]
            scope.catasto[field_key] = _catasto_payload(
                str(resolution["value"]),
                float(winner.get("confidence", 0.0)),
                copy.deepcopy(winner.get("evidence", [])),
            )
        if resolution.get("value") is not None or resolution.get("conflict"):
            scope.metadata.setdefault("catasto_internal", {})[field_key] = resolution

    for scope in [scope for scope in state.scopes.values() if scope.scope_type == "lotto"]:
        resolution = _resolve_scope_catasto_field(scope.scope_id, field_key, by_scope.get(scope.scope_id, []))
        direct_children = _child_scopes_with_field(state, scope.scope_id, field_key)
        derived = None
        if direct_children:
            values = sorted({str(child.catasto[field_key]["value"]) for child in direct_children})
            if len(values) == 1:
                best = max(direct_children, key=lambda child: float(child.catasto[field_key].get("confidence", 0.0)))
                derived = {
                    "value": values[0],
                    "winner": {
                        "confidence": float(best.catasto[field_key].get("confidence", 0.0)),
                        "evidence": copy.deepcopy(best.catasto[field_key].get("evidence", [])),
                    },
                    "derived_from_scopes": [child.scope_id for child in direct_children],
                    "resolution_reason": "uniform_child_scope_collapse",
                }
        prefer_derived = (
            derived is not None
            and resolution.get("value") is not None
            and str(derived["value"]) != str(resolution["value"])
            and int(resolution.get("priority", 2)) < 3
        )
        if prefer_derived:
            scope.catasto[field_key] = _catasto_payload(
                str(derived["value"]),
                float(derived["winner"].get("confidence", 0.0)),
                copy.deepcopy(derived["winner"].get("evidence", [])),
                source_scope_id=derived["derived_from_scopes"][0],
            )
        elif resolution.get("value") is not None:
            winner = resolution["winner"]
            scope.catasto[field_key] = _catasto_payload(
                str(resolution["value"]),
                float(winner.get("confidence", 0.0)),
                copy.deepcopy(winner.get("evidence", [])),
            )
        elif derived is not None:
            scope.catasto[field_key] = _catasto_payload(
                str(derived["value"]),
                float(derived["winner"].get("confidence", 0.0)),
                copy.deepcopy(derived["winner"].get("evidence", [])),
                source_scope_id=derived["derived_from_scopes"][0],
            )
        if resolution.get("value") is not None or resolution.get("conflict") or derived is not None:
            internal = copy.deepcopy(resolution)
            if resolution.get("value") is not None:
                winner = resolution.get("winner") or {}
                internal["inherits_to_children"] = bool((winner.get("ownership_method") or "").endswith("universal"))
            if derived is not None:
                internal["derived_from_scopes"] = derived["derived_from_scopes"]
                internal["resolution_reason"] = derived["resolution_reason"]
            scope.metadata.setdefault("catasto_internal", {})[field_key] = internal

    for scope in [scope for scope in state.scopes.values() if scope.scope_type == "lotto"]:
        _apply_inherited_field_to_children(state, scope.scope_id, field_key)

    direct_root_resolution = _resolve_scope_catasto_field("document_root", field_key, by_scope.get("document_root", []))
    root_scope = state.scopes["document_root"]
    derived_root = _derive_root_field(state, field_key, direct_root_resolution)
    root_scope.metadata.setdefault("catasto_internal", {})[field_key] = derived_root["internal"]
    root_scope.catasto[field_key] = _catasto_payload(
        derived_root["value"],
        float(derived_root["confidence"]),
        derived_root["evidence"],
        source_scope_id=derived_root.get("source_scope_id"),
    )
    state.canonical_case.catasto[field_key] = copy.deepcopy(root_scope.catasto[field_key])
    state.judgments[field_key] = Judgment(
        field_key=field_key,
        value=derived_root["value"],
        status="FOUND" if derived_root["value"] is not None else "NOT_FOUND",
        confidence=float(derived_root["confidence"]),
        evidence=derived_root["evidence"],
        rationale=f"{field_key} resolved from scoped catasto ownership and safe root rollup",
        metadata=copy.deepcopy(derived_root["internal"]),
    )


def run_catasto_agent(state: RuntimeState) -> None:
    _ensure_scope_rights_serialization_patch()
    grouped_quota_candidates = _collect_quota_candidates(state)
    quota_by_scope: dict[str, list[dict[str, Any]]] = {}
    for candidate in grouped_quota_candidates:
        quota_by_scope.setdefault(candidate["scope_id"], []).append(candidate)

    structured_catasto = _collect_structured_catasto_candidates(state)
    for field_key in _CAT_FIELD_KEYS:
        _write_scoped_catasto_field(state, field_key, structured_catasto.get(field_key, {}))

    for scope in [scope for scope in state.scopes.values() if scope.scope_type == "bene"]:
        resolution = _resolve_scope_quota(scope.scope_id, quota_by_scope.get(scope.scope_id, []))
        if resolution.get("value") is not None:
            winner = resolution["winner"]
            _scope_rights(scope)["quota"] = _quota_payload(
                str(resolution["value"]),
                float(winner.get("confidence", 0.0)),
                copy.deepcopy(winner.get("evidence", [])),
            )
        if resolution.get("value") is not None or resolution.get("conflict"):
            scope.metadata["rights_internal"] = {"quota": resolution}

    for scope in [scope for scope in state.scopes.values() if scope.scope_type == "lotto"]:
        resolution = _resolve_scope_quota(scope.scope_id, quota_by_scope.get(scope.scope_id, []))
        direct_children = _child_scopes_with_quota(state, scope.scope_id)
        derived = None
        if direct_children:
            values = sorted({str(_scope_rights(child)["quota"]["value"]) for child in direct_children})
            if len(values) == 1:
                best = max(direct_children, key=lambda child: float(_scope_rights(child)["quota"].get("confidence", 0.0)))
                derived = {
                    "value": values[0],
                    "winner": {
                        "confidence": float(_scope_rights(best)["quota"].get("confidence", 0.0)),
                        "evidence": copy.deepcopy(_scope_rights(best)["quota"].get("evidence", [])),
                    },
                    "derived_from_scopes": [child.scope_id for child in direct_children],
                    "resolution_reason": "uniform_child_scope_collapse",
                }
        prefer_derived = (
            derived is not None
            and resolution.get("value") is not None
            and str(derived["value"]) != str(resolution["value"])
            and int(resolution.get("priority", 2)) < 3
        )
        if prefer_derived:
            _scope_rights(scope)["quota"] = _quota_payload(
                str(derived["value"]),
                float(derived["winner"].get("confidence", 0.0)),
                copy.deepcopy(derived["winner"].get("evidence", [])),
                source_scope_id=derived["derived_from_scopes"][0],
            )
        elif resolution.get("value") is not None:
            winner = resolution["winner"]
            _scope_rights(scope)["quota"] = _quota_payload(
                str(resolution["value"]),
                float(winner.get("confidence", 0.0)),
                copy.deepcopy(winner.get("evidence", [])),
            )
        elif derived is not None:
            _scope_rights(scope)["quota"] = _quota_payload(
                str(derived["value"]),
                float(derived["winner"].get("confidence", 0.0)),
                copy.deepcopy(derived["winner"].get("evidence", [])),
                source_scope_id=derived["derived_from_scopes"][0],
            )
        if resolution.get("value") is not None or resolution.get("conflict") or derived is not None:
            internal = copy.deepcopy(resolution)
            if resolution.get("value") is not None:
                winner = resolution.get("winner") or {}
                internal["inherits_to_children"] = bool((winner.get("ownership_method") or "").endswith("universal"))
            if derived is not None:
                internal["derived_from_scopes"] = derived["derived_from_scopes"]
                internal["resolution_reason"] = derived["resolution_reason"]
            scope.metadata["rights_internal"] = {"quota": internal}

    for scope in [scope for scope in state.scopes.values() if scope.scope_type == "lotto"]:
        _apply_inherited_quota_to_children(state, scope.scope_id)

    direct_root_resolution = _resolve_scope_quota("document_root", quota_by_scope.get("document_root", []))
    root_scope = state.scopes["document_root"]
    derived_root = _derive_root_quota(state, direct_root_resolution)
    root_scope.metadata["rights_internal"] = {"quota": derived_root["internal"]}
    _scope_rights(root_scope)["quota"] = _quota_payload(
        derived_root["value"],
        float(derived_root["confidence"]),
        derived_root["evidence"],
        source_scope_id=derived_root.get("source_scope_id"),
    )
    state.canonical_case.rights["quota"] = copy.deepcopy(_scope_rights(root_scope)["quota"])

    if derived_root["value"] is not None:
        state.judgments["quota"] = Judgment(
            field_key="quota",
            value=str(derived_root["value"]),
            status="FOUND",
            confidence=float(derived_root["confidence"]),
            evidence=derived_root["evidence"],
            rationale="quota resolved from scoped ownership and safe root rollup",
            metadata={"source_scope_id": derived_root.get("source_scope_id")},
        )
    else:
        state.judgments["quota"] = Judgment(
            field_key="quota",
            value=None,
            status="NOT_FOUND",
            confidence=0.0,
            evidence=[],
            rationale="no safely derivable root quota after scoped rights resolution",
            metadata=copy.deepcopy(derived_root["internal"]),
        )
