from __future__ import annotations

import copy
import re
from typing import Any

from perizia_runtime.state import Candidate, Judgment, RuntimeState
from perizia_tools.evidence_span_tool import make_evidence
from perizia_tools.valuation_table_tool import valuation_candidates


_DIRECT_SELECTED_PATTERNS = [
    re.compile(
        r"prezzo\s+base\s+d[' ]asta[\s\S]{0,260}?€\.?\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)",
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r"prezzo\s+a\s+base\s+d[' ]asta[\s\S]{0,260}?€\.?\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)",
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r"valore\s+di\s+vendita\s+giudiziaria[\s\S]{0,260}?€\.?\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)",
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r"valore\s+al\s+netto\s+dei\s+costi\s+di\s+regolarizzazione\s+e\s+della\s+riduzione\s+cautelativa[\s\S]{0,260}?€\.?\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)",
        re.IGNORECASE | re.DOTALL,
    ),
]

_DIRECT_ADJUSTED_PATTERNS = [
    re.compile(
        r"valore\s+di\s+mercato\s+dell[' ]immobile\s+nello\s+stato\s+di\s+fatto\s+e\s+di\s+diritto\s+in\s+cui\s+si\s+trova[\s\S]{0,260}?€\.?\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)",
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r"valore\s+al\s+netto\s+dei\s+costi\s+di\s+regolarizzazione(?!\s+e\s+della\s+riduzione\s+cautelativa)[\s\S]{0,260}?€\.?\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)",
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r"valore\s+al\s+netto\s+delle\s+decurtazioni(?![\s\S]{0,80}vendita\s+giudiziaria)[\s\S]{0,260}?€\.?\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)",
        re.IGNORECASE | re.DOTALL,
    ),
]

_DIRECT_BENCHMARK_PATTERNS = [
    re.compile(
        r"valore\s+di\s+mercato\s*\(1000/1000[\s\S]{0,200}?€\.?\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)",
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r"valore\s+di\s+stima\s+del\s+bene[\s\S]{0,180}?€\.?\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)",
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r"valore\s+complessivo\s*\(vc\)[\s\S]{0,120}?€\.?\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)",
        re.IGNORECASE | re.DOTALL,
    ),
]

_LOTTO_REGEX = re.compile(r"\blotto\s*(?:n[°º.]?\s*)?(\d+|unico)\b", re.IGNORECASE)
_BENE_REGEX = re.compile(r"\bbene\s*n[°º.]?\s*(\d+)\b", re.IGNORECASE)
_PRICING_GUARDS = [
    "price_absurdity_guard",
    "nearby_number_contamination_rejected",
    "multi_lot_scalar_price_suppressed",
    "multi_lot_scalar_benchmark_suppressed",
]
_SELECTED_ROLE_PRIORITY = {
    "direct_selected": 3,
    "auction_price": 2,
    "net_valuation": 1,
}


def _parse_it_amount(raw: str) -> float:
    return float(str(raw).replace(".", "").replace(",", "."))


def _normalize_lotto_token(token: str) -> str:
    low = str(token or "").strip().lower()
    return "unico" if low == "unico" else re.sub(r"\D+", "", low)


def _single_scope_fallback(state: RuntimeState) -> tuple[str, str]:
    bene_scope_ids = sorted(scope.scope_id for scope in state.scopes.values() if scope.scope_type == "bene")
    if len(bene_scope_ids) == 1:
        return bene_scope_ids[0], "single_bene_fallback"
    lotto_scope_ids = sorted(scope.scope_id for scope in state.scopes.values() if scope.scope_type == "lotto")
    if len(lotto_scope_ids) == 1:
        return lotto_scope_ids[0], "single_lotto_fallback"
    return "document_root", "document_root_fallback"


def _scope_id_from_text(state: RuntimeState, text: str) -> tuple[str, str]:
    bene_scope_ids = []
    for token in _BENE_REGEX.findall(text or ""):
        scope_id = f"bene:{token}"
        if scope_id in state.scopes and scope_id not in bene_scope_ids:
            bene_scope_ids.append(scope_id)
    lotto_scope_ids = []
    for token in _LOTTO_REGEX.findall(text or ""):
        scope_id = f"lotto:{_normalize_lotto_token(token)}"
        if scope_id in state.scopes and scope_id not in lotto_scope_ids:
            lotto_scope_ids.append(scope_id)
    if bene_scope_ids:
        return bene_scope_ids[-1], "explicit_bene"
    if lotto_scope_ids:
        return lotto_scope_ids[-1], "explicit_lotto"
    return _single_scope_fallback(state)


def _nearest_preceding_local_scope_match(
    state: RuntimeState,
    text: str,
    *,
    anchor_pos: int,
    window: int = 220,
) -> tuple[str | None, str | None]:
    start = max(0, anchor_pos - window)
    local = text[start:anchor_pos]
    nearest: tuple[int, int, str, str] | None = None

    for match in _LOTTO_REGEX.finditer(local):
        scope_id = f"lotto:{_normalize_lotto_token(match.group(1))}"
        if scope_id not in state.scopes:
            continue
        distance = abs((start + match.start()) - anchor_pos)
        candidate = (distance, 0, scope_id, "nearest_preceding_lotto_local_match")
        if nearest is None or candidate < nearest:
            nearest = candidate

    for match in _BENE_REGEX.finditer(local):
        scope_id = f"bene:{match.group(1)}"
        if scope_id not in state.scopes:
            continue
        distance = abs((start + match.start()) - anchor_pos)
        candidate = (distance, 1, scope_id, "nearest_preceding_bene_local_match")
        if nearest is None or candidate < nearest:
            nearest = candidate

    if nearest is None:
        return None, None
    return nearest[2], nearest[3]


def _scope_id_for_local_match(state: RuntimeState, text: str, start: int, end: int) -> tuple[str, str]:
    anchor_pos = max(0, start)
    nearest_scope_id, nearest_method = _nearest_preceding_local_scope_match(state, text, anchor_pos=anchor_pos)
    if nearest_scope_id and nearest_method:
        return nearest_scope_id, nearest_method
    return _scope_id_from_text(state, text[max(0, start - 120):end])


def _candidate_page(candidate: Candidate) -> int:
    if candidate.evidence:
        return int(candidate.evidence[0].page or 0)
    return 0


def _candidate_quote(candidate: Candidate) -> str:
    if candidate.evidence:
        return str(candidate.evidence[0].quote or "")
    return ""


def _make_layer_candidate(
    state: RuntimeState,
    *,
    layer: str,
    amount: float,
    page: int,
    quote: str,
    confidence: float,
    source_role: str,
    evidence: list[Any],
    scope_id: str,
    ownership_method: str,
    index: int,
) -> dict[str, Any]:
    ownership = state.attach_evidence_ownership(
        scope_id=scope_id,
        field_target=f"pricing.{layer}",
        source_page=page,
        quote=quote,
        confidence=confidence,
        ownership_method=ownership_method,
        evidence_id=f"pricing_{layer}_{page}_{index}",
    )
    return {
        "layer": layer,
        "amount": round(float(amount), 2),
        "page": int(page),
        "quote": quote,
        "confidence": float(confidence),
        "source_role": source_role,
        "evidence": evidence,
        "scope_id": scope_id,
        "ownership_method": ownership_method,
        "evidence_id": ownership.evidence_id,
    }


def _candidate_scope_for_pricing(state: RuntimeState, candidate: Candidate) -> tuple[str, str]:
    metadata = candidate.metadata or {}
    anchor = str(metadata.get("structural_anchor") or "")
    if anchor in state.scopes:
        return anchor, "structural_anchor"
    normalized = str(metadata.get("normalized_ownership") or "")
    if normalized == "document_root_effective":
        return "document_root", "document_root_effective"
    quote = _candidate_quote(candidate)
    if quote:
        return _scope_id_from_text(state, quote)
    return _single_scope_fallback(state)


def _collect_table_layer_candidates(state: RuntimeState) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    layer_candidates: list[dict[str, Any]] = []
    invalid_candidates: list[dict[str, Any]] = []
    index = 0
    for candidate in valuation_candidates(state.analysis_id):
        if not candidate.valid:
            invalid_candidates.append(
                {
                    "value": candidate.value,
                    "reason": candidate.invalid_reason,
                    "evidence": candidate.evidence,
                }
            )
            continue
        layer = None
        source_role = str(candidate.semantic_role or "")
        if source_role == "auction_price":
            layer = "selected_price"
        elif source_role == "net_valuation":
            layer = "selected_price"
        elif source_role == "valuation_total":
            layer = "benchmark_value"
        if not layer:
            continue
        scope_id, ownership_method = _candidate_scope_for_pricing(state, candidate)
        index += 1
        layer_candidates.append(
            _make_layer_candidate(
                state,
                layer=layer,
                amount=float(candidate.value),
                page=_candidate_page(candidate),
                quote=_candidate_quote(candidate),
                confidence=float(candidate.confidence),
                source_role=source_role,
                evidence=list(candidate.evidence),
                scope_id=scope_id,
                ownership_method=ownership_method,
                index=index,
            )
        )
    return layer_candidates, invalid_candidates


def _collect_direct_matches(
    state: RuntimeState,
    *,
    patterns: list[re.Pattern[str]],
    layer: str,
    source_role: str,
    confidence: float,
    quote_padding: int,
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    index = 0
    for idx, page in enumerate(state.pages, start=1):
        text = str((page or {}).get("text") or "")
        page_number = int((page or {}).get("page_number") or (page or {}).get("page") or idx)
        for pattern in patterns:
            for match in pattern.finditer(text):
                amount = _parse_it_amount(match.group(1))
                start = max(0, match.start() - quote_padding)
                end = min(len(text), match.end() + quote_padding)
                quote = text[start:end].strip()
                scope_id, ownership_method = _scope_id_for_local_match(state, text, match.start(), match.end())
                index += 1
                matches.append(
                    _make_layer_candidate(
                        state,
                        layer=layer,
                        amount=amount,
                        page=page_number,
                        quote=quote,
                        confidence=confidence,
                        source_role=source_role,
                        evidence=[make_evidence(page_number, quote, source_role, ["pricing"], confidence)],
                        scope_id=scope_id,
                        ownership_method=ownership_method,
                        index=index,
                    )
                )
    return matches


def _prune_contaminated_direct_matches(matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for match in matches:
        grouped.setdefault((str(match["scope_id"]), str(match["layer"])), []).append(match)
    cleaned: list[dict[str, Any]] = []
    for _, group in grouped.items():
        clean = [item for item in group if "subalterno" not in str(item.get("quote") or "").lower()]
        cleaned.extend(clean if clean else group)
    return cleaned


def _consolidate_direct_selected_matches(matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for match in matches:
        grouped.setdefault((str(match["scope_id"]), int(match["page"])), []).append(match)
    consolidated: list[dict[str, Any]] = []
    for _, group in grouped.items():
        cautelative = [item for item in group if "riduzione cautelativa" in str(item.get("quote") or "").lower()]
        if cautelative:
            consolidated.append(min(cautelative, key=lambda item: float(item["amount"])))
        else:
            consolidated.extend(group)
    return consolidated


def _is_absurd_selected_price(amount: float | None, benchmark_value: float | None) -> bool:
    if amount is None or benchmark_value is None:
        return False
    return float(amount) < 1000 and float(benchmark_value) >= 10000


def _resolve_selected_candidates(scope_id: str, candidates: list[dict[str, Any]]) -> dict[str, Any]:
    if not candidates:
        return {"value": None, "winner": None, "conflict": False}
    tier_groups: dict[int, list[dict[str, Any]]] = {}
    for candidate in candidates:
        tier = _SELECTED_ROLE_PRIORITY.get(str(candidate.get("source_role") or ""), 0)
        tier_groups.setdefault(tier, []).append(candidate)
    top_tier = max(tier_groups)
    top_candidates = tier_groups[top_tier]
    distinct_amounts = sorted({round(float(item["amount"]), 2) for item in top_candidates})
    if len(distinct_amounts) > 1:
        return {
            "value": None,
            "winner": None,
            "conflict": True,
            "reason": "same_scope_selected_price_conflict",
            "competing_amounts": distinct_amounts,
            "candidate_ids": [item.get("evidence_id") for item in top_candidates if item.get("evidence_id")],
            "scope_id": scope_id,
        }
    winner = sorted(top_candidates, key=lambda item: (float(item["confidence"]), -item["page"]), reverse=True)[0]
    return {
        "value": winner["amount"],
        "winner": winner,
        "conflict": False,
        "candidate_ids": [item.get("evidence_id") for item in top_candidates if item.get("evidence_id")],
    }


def _resolve_unique_amount_layer(scope_id: str, layer: str, candidates: list[dict[str, Any]]) -> dict[str, Any]:
    if not candidates:
        return {"value": None, "winner": None, "conflict": False}
    distinct_amounts = sorted({round(float(item["amount"]), 2) for item in candidates})
    if len(distinct_amounts) > 1:
        return {
            "value": None,
            "winner": None,
            "conflict": True,
            "reason": f"same_scope_{layer}_conflict",
            "competing_amounts": distinct_amounts,
            "candidate_ids": [item.get("evidence_id") for item in candidates if item.get("evidence_id")],
            "scope_id": scope_id,
        }
    winner = sorted(candidates, key=lambda item: (float(item["confidence"]), -item["page"]), reverse=True)[0]
    return {
        "value": winner["amount"],
        "winner": winner,
        "conflict": False,
        "candidate_ids": [item.get("evidence_id") for item in candidates if item.get("evidence_id")],
    }


def _child_scopes_with_pricing(state: RuntimeState, scope_id: str) -> list[Any]:
    return [
        child
        for child in state.list_child_scopes(scope_id)
        if isinstance(child.pricing, dict) and any(child.pricing.get(layer) is not None for layer in ("selected_price", "benchmark_value", "adjusted_market_value"))
    ]


def _collapsible_child_layer(children: list[Any], layer: str) -> dict[str, Any] | None:
    if not children:
        return None
    values = [child.pricing.get(layer) for child in children if child.pricing.get(layer) is not None]
    if not values:
        return None
    if len(children) == 1 and len(values) == 1:
        only = children[0]
        return {
            "value": only.pricing.get(layer),
            "evidence": only.pricing.get("evidence", []),
            "confidence": only.pricing.get("confidence_map", {}).get(layer, 0.0),
            "source_scope_id": only.scope_id,
            "reason": "single_child_scope_collapse",
        }
    if len(values) == len(children):
        unique = sorted({round(float(value), 2) for value in values})
        if len(unique) == 1:
            best = max(children, key=lambda scope: float(scope.pricing.get("confidence_map", {}).get(layer, 0.0)))
            return {
                "value": unique[0],
                "evidence": best.pricing.get("evidence", []),
                "confidence": best.pricing.get("confidence_map", {}).get(layer, 0.0),
                "source_scope_id": best.scope_id,
                "reason": "uniform_child_scope_collapse",
            }
    return None


def _write_scope_pricing(
    scope: Any,
    *,
    selected: dict[str, Any],
    benchmark: dict[str, Any],
    adjusted: dict[str, Any],
    derived_layers: dict[str, Any] | None = None,
) -> None:
    derived_layers = derived_layers or {}
    selected_derived = derived_layers.get("selected_price") or {}
    benchmark_derived = derived_layers.get("benchmark_value") or {}
    adjusted_derived = derived_layers.get("adjusted_market_value") or {}
    selected_value = selected.get("value")
    benchmark_value = benchmark.get("value")
    adjusted_value = adjusted.get("value")
    confidence_map = {
        "selected_price": float((selected.get("winner") or {}).get("confidence", selected_derived.get("confidence", 0.0)) or 0.0),
        "benchmark_value": float((benchmark.get("winner") or {}).get("confidence", benchmark_derived.get("confidence", 0.0)) or 0.0),
        "adjusted_market_value": float((adjusted.get("winner") or {}).get("confidence", adjusted_derived.get("confidence", 0.0)) or 0.0),
    }
    pricing_payload = {
        "selected_price": selected_value if selected_value is not None else selected_derived.get("value"),
        "benchmark_value": benchmark_value if benchmark_value is not None else benchmark_derived.get("value"),
        "adjusted_market_value": adjusted_value if adjusted_value is not None else adjusted_derived.get("value"),
        "absurdity_guard_triggered": False,
        "guards": list(_PRICING_GUARDS),
        "confidence_map": confidence_map,
        "evidence": (
            (selected.get("winner") or {}).get("evidence")
            or (benchmark.get("winner") or {}).get("evidence")
            or (adjusted.get("winner") or {}).get("evidence")
            or selected_derived.get("evidence")
            or benchmark_derived.get("evidence")
            or adjusted_derived.get("evidence")
            or []
        ),
    }
    if _is_absurd_selected_price(pricing_payload["selected_price"], pricing_payload["benchmark_value"]):
        pricing_payload["selected_price"] = None
        pricing_payload["absurdity_guard_triggered"] = True
    scope.pricing = pricing_payload
    scope.metadata["pricing_internal"] = {
        "selected_price": selected,
        "benchmark_value": benchmark,
        "adjusted_market_value": adjusted,
        "derived_layers": derived_layers,
    }


def run_pricing_agent(state: RuntimeState) -> None:
    table_layer_candidates, invalid_candidates = _collect_table_layer_candidates(state)
    direct_selected = _collect_direct_matches(
        state,
        patterns=_DIRECT_SELECTED_PATTERNS,
        layer="selected_price",
        source_role="direct_selected",
        confidence=0.99,
        quote_padding=100,
    )
    direct_adjusted = _collect_direct_matches(
        state,
        patterns=_DIRECT_ADJUSTED_PATTERNS,
        layer="adjusted_market_value",
        source_role="direct_adjusted",
        confidence=0.95,
        quote_padding=100,
    )
    direct_benchmark = _collect_direct_matches(
        state,
        patterns=_DIRECT_BENCHMARK_PATTERNS,
        layer="benchmark_value",
        source_role="direct_benchmark",
        confidence=0.94,
        quote_padding=100,
    )
    direct_selected = _consolidate_direct_selected_matches(_prune_contaminated_direct_matches(direct_selected))
    direct_adjusted = _prune_contaminated_direct_matches(direct_adjusted)

    all_layer_candidates = table_layer_candidates + direct_selected + direct_adjusted + direct_benchmark
    by_scope: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for candidate in all_layer_candidates:
        scope_layers = by_scope.setdefault(candidate["scope_id"], {})
        scope_layers.setdefault(candidate["layer"], []).append(candidate)

    for scope in [scope for scope in state.scopes.values() if scope.scope_type == "bene"]:
        selected = _resolve_selected_candidates(scope.scope_id, by_scope.get(scope.scope_id, {}).get("selected_price", []))
        benchmark = _resolve_unique_amount_layer(scope.scope_id, "benchmark_value", by_scope.get(scope.scope_id, {}).get("benchmark_value", []))
        adjusted = _resolve_unique_amount_layer(scope.scope_id, "adjusted_market_value", by_scope.get(scope.scope_id, {}).get("adjusted_market_value", []))
        if any(item.get("value") is not None or item.get("conflict") for item in [selected, benchmark, adjusted]):
            _write_scope_pricing(scope, selected=selected, benchmark=benchmark, adjusted=adjusted)

    for scope in [scope for scope in state.scopes.values() if scope.scope_type == "lotto"]:
        direct_selected_resolution = _resolve_selected_candidates(scope.scope_id, by_scope.get(scope.scope_id, {}).get("selected_price", []))
        direct_benchmark_resolution = _resolve_unique_amount_layer(scope.scope_id, "benchmark_value", by_scope.get(scope.scope_id, {}).get("benchmark_value", []))
        direct_adjusted_resolution = _resolve_unique_amount_layer(scope.scope_id, "adjusted_market_value", by_scope.get(scope.scope_id, {}).get("adjusted_market_value", []))
        child_layers = {
            layer: _collapsible_child_layer(_child_scopes_with_pricing(state, scope.scope_id), layer)
            for layer in ("selected_price", "benchmark_value", "adjusted_market_value")
        }
        if any(
            item.get("value") is not None or item.get("conflict")
            for item in [direct_selected_resolution, direct_benchmark_resolution, direct_adjusted_resolution]
        ) or any(value is not None for value in child_layers.values()):
            _write_scope_pricing(
                scope,
                selected=direct_selected_resolution,
                benchmark=direct_benchmark_resolution,
                adjusted=direct_adjusted_resolution,
                derived_layers=child_layers,
            )

    root_scope = state.scopes["document_root"]
    root_direct_selected = _resolve_selected_candidates("document_root", by_scope.get("document_root", {}).get("selected_price", []))
    root_direct_benchmark = _resolve_unique_amount_layer("document_root", "benchmark_value", by_scope.get("document_root", {}).get("benchmark_value", []))
    root_direct_adjusted = _resolve_unique_amount_layer("document_root", "adjusted_market_value", by_scope.get("document_root", {}).get("adjusted_market_value", []))
    pricing_children = _child_scopes_with_pricing(state, "document_root")
    if not pricing_children:
        pricing_children = [scope for scope in state.scopes.values() if scope.scope_type == "bene" and scope.pricing]
    root_derived_layers = {
        layer: _collapsible_child_layer(pricing_children, layer)
        for layer in ("selected_price", "benchmark_value", "adjusted_market_value")
    }
    _write_scope_pricing(
        root_scope,
        selected=root_direct_selected,
        benchmark=root_direct_benchmark,
        adjusted=root_direct_adjusted,
        derived_layers=root_derived_layers,
    )

    root_pricing = copy.deepcopy(root_scope.pricing)
    non_root_selected_values = []
    for scope in state.scopes.values():
        if scope.scope_id == "document_root" or not scope.pricing:
            continue
        if scope.pricing.get("selected_price") is not None:
            non_root_selected_values.append(round(float(scope.pricing["selected_price"]), 2))
    if root_pricing.get("selected_price") is not None and len(set(non_root_selected_values)) > 1:
        root_pricing["selected_price"] = None

    if root_pricing.get("absurdity_guard_triggered"):
        invalid_candidates.append({"value": "direct_price_candidate", "reason": "direct_price_absurdity_guard", "evidence": []})

    if root_pricing.get("selected_price") is None:
        raw_selected_values = [
            round(float(candidate["amount"]), 2)
            for candidate in all_layer_candidates
            if candidate.get("layer") == "selected_price"
        ]
        if len(set(non_root_selected_values)) > 1 or len(set(raw_selected_values)) > 1:
            invalid_candidates.append({"value": "multiple_auction_prices", "reason": "multi_lot_scalar_price_suppressed", "evidence": []})

    if root_pricing.get("benchmark_value") is None:
        all_benchmark_scope_values = []
        for scope in state.scopes.values():
            if scope.scope_id == "document_root" or not scope.pricing:
                continue
            if scope.pricing.get("benchmark_value") is not None:
                all_benchmark_scope_values.append(round(float(scope.pricing["benchmark_value"]), 2))
        has_multi_lot_structure = len([scope for scope in state.scopes.values() if scope.scope_type == "lotto"]) > 1
        has_scoped_benchmark_candidates = any(
            candidate.get("layer") == "benchmark_value" and candidate.get("scope_id") != "document_root"
            for candidate in all_layer_candidates
        )
        raw_benchmark_values = [
            round(float(candidate["amount"]), 2)
            for candidate in all_layer_candidates
            if candidate.get("layer") == "benchmark_value"
        ]
        if len(set(all_benchmark_scope_values)) > 1 or (has_multi_lot_structure and has_scoped_benchmark_candidates) or len(set(raw_benchmark_values)) > 1:
            invalid_candidates.append({"value": "multiple_benchmark_values", "reason": "multi_lot_scalar_benchmark_suppressed", "evidence": []})

    seen_invalid_keys = set()
    deduped_invalid = []
    for item in invalid_candidates:
        key = (str(item.get("reason")), str(item.get("value")))
        if key in seen_invalid_keys:
            continue
        seen_invalid_keys.add(key)
        deduped_invalid.append(item)

    root_pricing["candidate_count"] = len(all_layer_candidates)
    root_pricing["invalid_candidates"] = deduped_invalid
    state.canonical_case.pricing = root_pricing

    judgment_confidence = float(root_pricing.get("confidence_map", {}).get("selected_price", 0.0))
    if root_pricing.get("selected_price") is not None:
        state.judgments["pricing"] = Judgment(
            "pricing",
            float(root_pricing["selected_price"]),
            "FOUND",
            judgment_confidence,
            root_pricing.get("evidence", []),
            "pricing selected from scoped ownership and safe root collapse",
            {
                "benchmark_value": root_pricing.get("benchmark_value"),
                "adjusted_market_value": root_pricing.get("adjusted_market_value"),
            },
        )
    else:
        state.judgments["pricing"] = Judgment(
            "pricing",
            None,
            "LOW_CONFIDENCE" if root_pricing.get("absurdity_guard_triggered") else "NOT_FOUND",
            0.0,
            root_pricing.get("evidence", []),
            "price rejected by absurdity guard" if root_pricing.get("absurdity_guard_triggered") else "no safely derivable root selected price",
            {
                "benchmark_value": root_pricing.get("benchmark_value"),
                "adjusted_market_value": root_pricing.get("adjusted_market_value"),
                "absurdity_guard_triggered": root_pricing.get("absurdity_guard_triggered"),
            },
        )
