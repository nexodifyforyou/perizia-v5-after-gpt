from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from perizia_agents.agibilita_agent import run_agibilita_agent
from perizia_agents.alignment_agent import run_alignment_agent
from perizia_agents.catasto_agent import run_catasto_agent
from perizia_agents.contradiction_agent import run_contradiction_agent
from perizia_agents.costs_agent import run_costs_agent
from perizia_agents.impianti_agent import run_impianti_agent
from perizia_agents.legal_agent import run_legal_agent
from perizia_agents.occupancy_agent import run_occupancy_agent
from perizia_agents.pricing_agent import run_pricing_agent
from perizia_agents.priority_agent import run_priority_agent
from perizia_agents.structure_agent import run_structure_agent
from perizia_agents.summary_agent import run_summary_agent
from perizia_agents.urbanistica_agent import run_urbanistica_agent
from perizia_ingest.readability_gate import assess_document_readability
from perizia_qa.comparators import compare_legacy_and_verifier
from perizia_qa.invariants import run_invariants
from perizia_runtime.evidence_mode import DEGRADED_TEXT, STOP_UNREADABLE, TEXT_FIRST, select_evidence_mode
from perizia_runtime.pipeline import PeriziaPipeline
from perizia_runtime.state import RuntimeState, to_dict
from perizia_tools.pdf_text_tool import build_pdf_text_payload


_CORPUS_REGISTRY_PATH = Path("/srv/perizia/_qa/canonical_pipeline/working_corpus_registry.json")
_CORPUS_ARTIFACT_ROOT = Path("/srv/perizia/_qa/canonical_pipeline/runs")


_MACHINE_PLACEHOLDER_STRINGS = {
    "TBD",
    "NOT_SPECIFIED",
    "NOT_SPECIFIED_IN_PERIZIA",
    "NON_QUANTIFICATO",
    "NON_QUANTIFICATO_IN_PERIZIA",
}


def _is_machine_placeholder(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        text = value.strip()
        upper = text.upper()
        return upper in _MACHINE_PLACEHOLDER_STRINGS or "TBD" in upper
    return False


def _scrub_machine_placeholders(node: Any) -> int:
    """
    Remove legacy machine placeholders from runtime-visible payload branches.

    This does not infer values. It only converts placeholders emitted by older
    compatibility payloads into null so the freeze contract metadata can carry
    the reason.
    """
    count = 0
    if isinstance(node, dict):
        for key, value in list(node.items()):
            if _is_machine_placeholder(value):
                node[key] = None
                count += 1
            else:
                count += _scrub_machine_placeholders(value)
        return count
    if isinstance(node, list):
        for item in node:
            count += _scrub_machine_placeholders(item)
    return count


def _blocked_contract_reason(freeze_contract: Dict[str, Any]) -> Optional[str]:
    status = str(freeze_contract.get("status") or "").upper()
    freeze_status = str(freeze_contract.get("freeze_status") or "").upper()
    if "BLOCKED_UNREADABLE" in {status, freeze_status} or "UNREADABLE" in status or "UNREADABLE" in freeze_status:
        return "canonical_freeze_blocked_unreadable"
    for item in freeze_contract.get("blocked_items") or []:
        if not isinstance(item, dict):
            continue
        reason_blob = " ".join(str(item.get(k) or "") for k in ("reason", "freeze_status", "explanation", "why_not_resolved"))
        if "unreadable" in reason_blob.lower():
            return "canonical_freeze_blocked_unreadable"
    return None


def _blocked_field_metadata(freeze_contract: Dict[str, Any], field_key: str) -> Dict[str, Any]:
    blocked_items = [
        copy.deepcopy(item)
        for item in freeze_contract.get("blocked_items") or []
        if isinstance(item, dict)
    ]
    return {
        "state": "blocked",
        "source": "canonical_freeze_contract",
        "field_key": field_key,
        "reason": _blocked_contract_reason(freeze_contract),
        "freeze_status": freeze_contract.get("freeze_status"),
        "case_key": freeze_contract.get("case_key"),
        "blocked_items": blocked_items[:3],
        "needs_human_review": True,
    }


def _apply_blocked_freeze_contract_to_result(result: Dict[str, Any], freeze_contract: Dict[str, Any]) -> None:
    reason = _blocked_contract_reason(freeze_contract)
    if not reason:
        return

    result["analysis_status"] = "UNREADABLE"
    result["canonical_contract_state"] = {
        "state": "blocked",
        "reason": reason,
        "freeze_status": freeze_contract.get("freeze_status"),
        "case_key": freeze_contract.get("case_key"),
        "source": "canonical_freeze_contract",
    }

    lot_fields = ("prezzo_base_eur", "ubicazione", "superficie_mq", "diritto_reale", "diritto")
    lots = result.get("lots")
    if isinstance(lots, list):
        for lot_idx, lot in enumerate(lots):
            if not isinstance(lot, dict):
                continue
            meta = lot.get("field_contract_metadata")
            if not isinstance(meta, dict):
                meta = {}
                lot["field_contract_metadata"] = meta
            for field in lot_fields:
                if field in lot:
                    lot[field] = None
                meta[field] = _blocked_field_metadata(freeze_contract, f"lots[{lot_idx}].{field}")

    lot_index = result.get("lot_index")
    if isinstance(lot_index, list):
        for idx, row in enumerate(lot_index):
            if not isinstance(row, dict):
                continue
            meta = row.get("field_contract_metadata")
            if not isinstance(meta, dict):
                meta = {}
                row["field_contract_metadata"] = meta
            for field in ("prezzo", "ubicazione"):
                if field in row:
                    row[field] = None
                meta[field] = _blocked_field_metadata(freeze_contract, f"lot_index[{idx}].{field}")

    for box_key in ("money_box", "section_3_money_box"):
        box = result.get(box_key)
        if not isinstance(box, dict):
            continue
        box["contract_metadata"] = _blocked_field_metadata(freeze_contract, box_key)
        items = box.get("items")
        if isinstance(items, list):
            for idx, item in enumerate(items):
                if not isinstance(item, dict):
                    continue
                item["contract_metadata"] = _blocked_field_metadata(freeze_contract, f"{box_key}.items[{idx}]")
                for field in ("type", "stima_euro", "source", "stima_nota"):
                    if field in item and _is_machine_placeholder(item.get(field)):
                        item[field] = None
        for total_key in ("total_extra_costs", "totale_extra_budget"):
            total = box.get(total_key)
            if isinstance(total, dict):
                total["contract_metadata"] = _blocked_field_metadata(freeze_contract, f"{box_key}.{total_key}")
                for field in ("min", "max", "nota", "note"):
                    if field in total and _is_machine_placeholder(total.get(field)):
                        total[field] = None
                value_range = total.get("range")
                if isinstance(value_range, dict):
                    for field in ("min", "max"):
                        if _is_machine_placeholder(value_range.get(field)):
                            value_range[field] = None

    indice = result.get("indice_di_convenienza")
    if isinstance(indice, dict):
        indice["contract_metadata"] = _blocked_field_metadata(freeze_contract, "indice_di_convenienza")
        for field in ("extra_costs_min", "extra_costs_max", "all_in_light_min", "all_in_light_max"):
            if field in indice:
                indice[field] = None

    scrubbed = _scrub_machine_placeholders(result)
    if scrubbed:
        result.setdefault("debug", {})["runtime_machine_placeholders_scrubbed"] = scrubbed


def _load_freeze_contract(pdf_sha256: Optional[str]) -> Dict[str, Any]:
    """
    Look up the corpus registry by PDF sha256 and return the canonical
    doc_map freeze contract for the matching case.

    Returns an empty dict on any failure (missing registry, no match, missing
    artifact). Never raises.
    """
    if not pdf_sha256:
        return {}
    try:
        registry = json.loads(_CORPUS_REGISTRY_PATH.read_text(encoding="utf-8"))
        case_key = next(
            (r["case_key"] for r in registry if r.get("sha256") == pdf_sha256),
            None,
        )
        if not case_key:
            return {}
        doc_map_path = _CORPUS_ARTIFACT_ROOT / case_key / "artifacts" / "doc_map.json"
        if not doc_map_path.exists():
            return {}
        doc_map = json.loads(doc_map_path.read_text(encoding="utf-8"))
        return {
            "case_key": doc_map.get("case_key") or case_key,
            "status": doc_map.get("status"),
            "freeze_status": doc_map.get("freeze_status"),
            "freeze_ready": doc_map.get("freeze_ready"),
            "case_summary": copy.deepcopy(doc_map.get("case_summary") or {}),
            "scope_index": copy.deepcopy(doc_map.get("scope_index") or {}),
            "fields": copy.deepcopy(doc_map.get("fields") or {}),
            "unresolved_items": copy.deepcopy(doc_map.get("unresolved_items") or []),
            "blocked_items": copy.deepcopy(doc_map.get("blocked_items") or []),
            "context_items": copy.deepcopy(doc_map.get("context_items") or []),
            "grouped_llm_explanations": [
                copy.deepcopy(g)
                for g in (doc_map.get("grouped_llm_explanations") or [])
                if isinstance(g, dict)
            ],
            "source_artifact": str(doc_map_path),
        }
    except Exception:
        return {}


def _load_freeze_grouped_explanations(pdf_sha256: Optional[str]) -> List[Dict[str, Any]]:
    """
    Backward-compatible helper for callers that still need only grouped
    explanations. The canonical authority is _load_freeze_contract.

    Returns empty list on any failure.
    Never raises — this is a best-effort enrichment.
    """
    contract = _load_freeze_contract(pdf_sha256)
    return contract.get("grouped_llm_explanations", []) if isinstance(contract, dict) else []


DEGRADED_SOURCE_GUARD = "degraded_source_text_only"
CONFIDENCE_CAP_GUARD = "confidence_capped_due_to_extraction_quality"
DEGRADED_SOURCE_NOTE = "Reasoning used degraded extracted text only; packaged confidence is capped and requires manual caution."
DEGRADED_CONFIDENCE_CAP = 0.6


def _base_verifier_payload(*, analysis_id: str, readability: Dict[str, Any], evidence_mode: Dict[str, str]) -> Dict[str, Any]:
    return {
        "analysis_id": str(analysis_id or ""),
        "readability_verdict": readability["readability_verdict"],
        "document_quality_note": readability["document_quality_note"],
        "evidence_mode": evidence_mode["evidence_mode"],
        "evidence_mode_reason": evidence_mode["evidence_mode_reason"],
        "source_quality_note": None,
        "packaging_guards": [],
        "surface_inventory_summary": readability["surface_inventory_summary"],
        "surface_inventory_pages": readability["surface_inventory_pages"],
        "surface_inventory_limitations": readability["limitations"],
        "verifier_cautions": [],
    }


def _build_unreadable_payload(*, analysis_id: str, readability: Dict[str, Any], evidence_mode: Dict[str, str]) -> Dict[str, Any]:
    payload_dict = _base_verifier_payload(analysis_id=analysis_id, readability=readability, evidence_mode=evidence_mode)
    payload_dict.update(
        {
            "canonical_case": {},
            "scopes": {},
            "evidence_ownership": {},
            "judgments": {},
            "candidates": {},
            "qa_checks": [],
            "comparison": {},
            "reasoning_status": "SUPPRESSED_UNREADABLE",
        }
    )
    return payload_dict


def _degraded_text_cautions() -> List[Dict[str, Any]]:
    return [
        {
            "code": "degraded_text_sources",
            "severity": "CAUTION",
            "message": "Verifier reasoning ran on degraded extracted text surfaces; field-level conclusions may be less reliable.",
        }
    ]


def _append_unique(values: List[Any], value: Any) -> None:
    if value not in values:
        values.append(value)


def _cap_confidence(value: Any) -> Any:
    if isinstance(value, (int, float)):
        return min(float(value), DEGRADED_CONFIDENCE_CAP)
    return value


def _apply_degraded_confidence_caps(node: Any) -> None:
    if isinstance(node, dict):
        for key, value in list(node.items()):
            if key in {"confidence", "resolver_confidence"}:
                node[key] = _cap_confidence(value)
            else:
                _apply_degraded_confidence_caps(value)
        return
    if isinstance(node, list):
        for item in node:
            _apply_degraded_confidence_caps(item)


def _annotate_degraded_container(container: Dict[str, Any]) -> None:
    guards = container.get("guards")
    if not isinstance(guards, list):
        guards = []
        container["guards"] = guards
    _append_unique(guards, DEGRADED_SOURCE_GUARD)
    _append_unique(guards, CONFIDENCE_CAP_GUARD)
    container["source_quality_note"] = DEGRADED_SOURCE_NOTE


def _annotate_degraded_output(container: Dict[str, Any], *, guards_key: str = "guards") -> None:
    guards = container.get(guards_key)
    if not isinstance(guards, list):
        guards = []
        container[guards_key] = guards
    _append_unique(guards, DEGRADED_SOURCE_GUARD)
    _append_unique(guards, CONFIDENCE_CAP_GUARD)
    container["source_quality_note"] = DEGRADED_SOURCE_NOTE
    _apply_degraded_confidence_caps(container)


def _apply_degraded_packaging(payload_dict: Dict[str, Any]) -> None:
    payload_dict["source_quality_note"] = DEGRADED_SOURCE_NOTE
    packaging_guards = payload_dict.get("packaging_guards")
    if not isinstance(packaging_guards, list):
        packaging_guards = []
        payload_dict["packaging_guards"] = packaging_guards
    _append_unique(packaging_guards, DEGRADED_SOURCE_GUARD)
    _append_unique(packaging_guards, CONFIDENCE_CAP_GUARD)

    canonical = payload_dict.get("canonical_case")
    if isinstance(canonical, dict):
        canonical["source_quality_note"] = DEGRADED_SOURCE_NOTE
        case_guards = canonical.get("packaging_guards")
        if not isinstance(case_guards, list):
            case_guards = []
            canonical["packaging_guards"] = case_guards
        _append_unique(case_guards, DEGRADED_SOURCE_GUARD)
        _append_unique(case_guards, CONFIDENCE_CAP_GUARD)
        for value in canonical.values():
            if isinstance(value, dict):
                _annotate_degraded_container(value)

    scopes = payload_dict.get("scopes")
    if isinstance(scopes, dict):
        for scope in scopes.values():
            if isinstance(scope, dict):
                _annotate_degraded_container(scope)

    judgments = payload_dict.get("judgments")
    if isinstance(judgments, dict):
        for judgment in judgments.values():
            if isinstance(judgment, dict):
                metadata = judgment.get("metadata")
                if not isinstance(metadata, dict):
                    metadata = {}
                    judgment["metadata"] = metadata
                metadata["source_quality_note"] = DEGRADED_SOURCE_NOTE
                packaging_flags = metadata.get("packaging_guards")
                if not isinstance(packaging_flags, list):
                    packaging_flags = []
                    metadata["packaging_guards"] = packaging_flags
                _append_unique(packaging_flags, DEGRADED_SOURCE_GUARD)
                _append_unique(packaging_flags, CONFIDENCE_CAP_GUARD)

    _apply_degraded_confidence_caps(payload_dict.get("canonical_case"))
    _apply_degraded_confidence_caps(payload_dict.get("scopes"))
    _apply_degraded_confidence_caps(payload_dict.get("judgments"))


def run_quality_verifier(*, analysis_id: str, result: Dict[str, Any], pages: List[Dict[str, Any]], full_text: str, pdf_sha256: Optional[str] = None) -> Dict[str, Any]:
    payload = build_pdf_text_payload(pages, full_text)
    readability = assess_document_readability(payload["pages"])
    evidence_mode = select_evidence_mode(readability["readability_verdict"])
    if evidence_mode["evidence_mode"] == STOP_UNREADABLE:
        return _build_unreadable_payload(analysis_id=analysis_id, readability=readability, evidence_mode=evidence_mode)

    state = RuntimeState(
        analysis_id=str(analysis_id or ""),
        result=copy.deepcopy(result or {}),
        pages=payload["pages"],
        full_text=payload["full_text"],
    )
    pipeline = PeriziaPipeline(
        [
            run_structure_agent,
            run_catasto_agent,
            run_pricing_agent,
            run_occupancy_agent,
            run_legal_agent,
            run_urbanistica_agent,
            run_agibilita_agent,
            run_impianti_agent,
            run_costs_agent,
            run_contradiction_agent,
            run_priority_agent,
            run_summary_agent,
            run_alignment_agent,
        ]
    )
    pipeline.run(state)
    payload_dict = _base_verifier_payload(analysis_id=state.analysis_id, readability=readability, evidence_mode=evidence_mode)
    payload_dict.update(
        {
        "canonical_case": to_dict(state.canonical_case),
        "scopes": to_dict(state.scopes),
        "evidence_ownership": to_dict(state.evidence_ownership),
        "judgments": to_dict(state.judgments),
        "candidates": to_dict(state.candidates),
        "reasoning_status": "NORMAL" if evidence_mode["evidence_mode"] == TEXT_FIRST else "DEGRADED_TEXT_CAUTION",
        }
    )
    freeze_contract = _load_freeze_contract(pdf_sha256)
    if freeze_contract:
        payload_dict["canonical_case"]["freeze_contract"] = freeze_contract
        freeze_grouped = freeze_contract.get("grouped_llm_explanations", [])
        if freeze_grouped:
            payload_dict["canonical_case"]["grouped_llm_explanations"] = freeze_grouped
    if evidence_mode["evidence_mode"] == DEGRADED_TEXT:
        payload_dict["verifier_cautions"] = _degraded_text_cautions()
        _apply_degraded_packaging(payload_dict)
    payload_dict["qa_checks"] = run_invariants(payload_dict)
    payload_dict["comparison"] = compare_legacy_and_verifier(result, payload_dict)
    return payload_dict


def _legacy_money_costs_summary(costs: Dict[str, Any]) -> Dict[str, Any]:
    explicit_total = costs.get("explicit_total")
    items = costs.get("explicit_buyer_costs", []) if isinstance(costs.get("explicit_buyer_costs"), list) else []
    return {
        "explicit_total_eur": explicit_total,
        "explicit_items_count": len(items),
        "valuation_adjustments_count": len(costs.get("valuation_adjustments", []) if isinstance(costs.get("valuation_adjustments"), list) else []),
    }


def _as_float_or_none(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip().replace("€", "").replace(" ", "")
        if "," in text and "." in text:
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", ".")
        try:
            return float(text)
        except Exception:
            return None
    return None


def _pricing_evidence_for_selected_price(pricing: Dict[str, Any]) -> List[Dict[str, Any]]:
    evidence = pricing.get("evidence") if isinstance(pricing.get("evidence"), list) else []
    direct = [
        copy.deepcopy(ev)
        for ev in evidence
        if isinstance(ev, dict) and str(ev.get("semantic_role") or "") == "direct_selected"
    ]
    return direct or [copy.deepcopy(ev) for ev in evidence[:2] if isinstance(ev, dict)]


def _freeze_contract_field_entry(freeze_contract: Any, scope_key: str, family: str, field_type: str) -> Optional[Dict[str, Any]]:
    if not isinstance(freeze_contract, dict):
        return None
    fields = freeze_contract.get("fields")
    if not isinstance(fields, dict):
        return None
    nested = fields.get(scope_key)
    if isinstance(nested, dict):
        family_fields = nested.get(family)
        if isinstance(family_fields, dict) and isinstance(family_fields.get(field_type), dict):
            return family_fields[field_type]
    flat_key = f"field::{scope_key}::{field_type}"
    flat_entry = fields.get(flat_key)
    return flat_entry if isinstance(flat_entry, dict) else None


def _pricing_evidence_for_benchmark_value(pricing: Dict[str, Any], contract_entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    supporting = contract_entry.get("supporting_evidence")
    if isinstance(supporting, list) and supporting:
        return [copy.deepcopy(ev) for ev in supporting if isinstance(ev, dict)]
    evidence = pricing.get("evidence") if isinstance(pricing.get("evidence"), list) else []
    benchmark = [
        copy.deepcopy(ev)
        for ev in evidence
        if isinstance(ev, dict) and str(ev.get("semantic_role") or "") in {"valuation_total", "benchmark_value"}
    ]
    return benchmark or [copy.deepcopy(ev) for ev in evidence[:2] if isinstance(ev, dict)]


def _should_promote_selected_price(existing_state: Any, selected_price: float, pricing: Dict[str, Any]) -> bool:
    if not isinstance(existing_state, dict):
        return True
    if str(existing_state.get("status") or "").upper() == "USER_PROVIDED":
        return False
    existing_value = _as_float_or_none(existing_state.get("value"))
    if existing_value is None:
        return True
    if abs(existing_value - float(selected_price)) <= 1.0:
        return False
    for invalid in pricing.get("invalid_candidates", []) if isinstance(pricing.get("invalid_candidates"), list) else []:
        if not isinstance(invalid, dict):
            continue
        invalid_value = _as_float_or_none(invalid.get("value"))
        if invalid_value is not None and abs(invalid_value - existing_value) <= 0.01:
            return True
    return True


def _should_promote_benchmark_value(existing_state: Any, benchmark_value: float) -> bool:
    if not isinstance(existing_state, dict):
        return True
    if str(existing_state.get("status") or "").upper() == "USER_PROVIDED":
        return False
    existing_value = _as_float_or_none(existing_state.get("value"))
    if existing_value is None:
        return True
    return abs(existing_value - float(benchmark_value)) > 1.0


def _benchmark_contract_entry_matches_pricing(contract_entry: Any, benchmark_value: float) -> bool:
    if not isinstance(contract_entry, dict):
        return False
    if str(contract_entry.get("state") or "") not in {"deterministic_active", "llm_resolved", "resolved_with_context"}:
        return False
    contract_value = _as_float_or_none(contract_entry.get("value"))
    return contract_value is not None and abs(contract_value - float(benchmark_value)) <= 1.0


def _canonical_pricing_amounts(pricing: Dict[str, Any]) -> List[float]:
    amounts: List[float] = []
    for key in ("selected_price", "benchmark_value", "adjusted_market_value"):
        value = pricing.get(key)
        if isinstance(value, (int, float)) and float(value) > 0:
            amount = round(float(value), 2)
            if amount not in amounts:
                amounts.append(amount)
    return amounts


def _prune_pricing_amounts_from_money_boxes(result: Dict[str, Any], pricing: Dict[str, Any]) -> None:
    pricing_amounts = _canonical_pricing_amounts(pricing)
    if not pricing_amounts:
        return
    for box_key in ("money_box", "section_3_money_box"):
        box = result.get(box_key)
        if not isinstance(box, dict):
            continue
        items = box.get("items")
        if not isinstance(items, list):
            continue
        kept: List[Dict[str, Any]] = []
        removed: List[Dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            amount = _as_float_or_none(item.get("stima_euro"))
            if amount is not None and round(float(amount), 2) in pricing_amounts:
                removed.append({
                    "amount": round(float(amount), 2),
                    "label": item.get("label_it") or item.get("label") or item.get("voce"),
                    "source": item.get("source"),
                    "reason": "pricing_amount_not_buyer_cost",
                })
                continue
            kept.append(item)
        if not removed:
            continue
        box["items"] = kept
        existing_removed = box.get("removed_pricing_amount_items")
        if not isinstance(existing_removed, list):
            existing_removed = []
        existing_removed.extend(removed)
        box["removed_pricing_amount_items"] = existing_removed
        result[box_key] = box


def _apply_degraded_result_packaging(result: Dict[str, Any]) -> None:
    _annotate_degraded_output(result, guards_key="packaging_guards")

    document_quality = result.get("document_quality")
    if isinstance(document_quality, dict):
        _annotate_degraded_output(document_quality, guards_key="packaging_guards")

    section_legal = result.get("section_9_legal_killers")
    if isinstance(section_legal, dict):
        _annotate_degraded_output(section_legal, guards_key="packaging_guards")

    summary = result.get("summary_for_client")
    if isinstance(summary, dict):
        _annotate_degraded_output(summary, guards_key="packaging_guards")

    summary_bundle = result.get("summary_for_client_bundle")
    if isinstance(summary_bundle, dict):
        _annotate_degraded_output(summary_bundle)


def apply_verifier_to_result(result: Dict[str, Any], verifier_payload: Dict[str, Any]) -> None:
    if not isinstance(result, dict):
        return
    canonical = verifier_payload.get("canonical_case", {}) if isinstance(verifier_payload.get("canonical_case"), dict) else {}
    rights = canonical.get("rights", {}) if isinstance(canonical.get("rights"), dict) else {}
    occupancy = canonical.get("occupancy", {}) if isinstance(canonical.get("occupancy"), dict) else {}
    urbanistica = canonical.get("urbanistica", {}) if isinstance(canonical.get("urbanistica"), dict) else {}
    legal = canonical.get("legal", {}) if isinstance(canonical.get("legal"), dict) else {}
    pricing = canonical.get("pricing", {}) if isinstance(canonical.get("pricing"), dict) else {}
    costs = canonical.get("costs", {}) if isinstance(canonical.get("costs"), dict) else {}
    priority = canonical.get("priority", {}) if isinstance(priority := canonical.get("priority"), dict) else {}
    summary_bundle = canonical.get("summary_bundle", {}) if isinstance(canonical.get("summary_bundle"), dict) else {}

    result["verifier_runtime"] = verifier_payload
    freeze_contract = canonical.get("freeze_contract")
    if isinstance(freeze_contract, dict) and freeze_contract:
        result["canonical_freeze_contract"] = copy.deepcopy(freeze_contract)
        _apply_blocked_freeze_contract_to_result(result, freeze_contract)
    freeze_grouped = canonical.get("grouped_llm_explanations")
    if isinstance(freeze_grouped, list) and freeze_grouped:
        result["canonical_freeze_explanations"] = freeze_grouped
    document_quality = result.setdefault("document_quality", {})
    document_quality["readability_verdict"] = verifier_payload.get("readability_verdict")
    document_quality["document_quality_note"] = verifier_payload.get("document_quality_note")
    document_quality["evidence_mode"] = verifier_payload.get("evidence_mode")
    document_quality["evidence_mode_reason"] = verifier_payload.get("evidence_mode_reason")
    document_quality["source_quality_note"] = verifier_payload.get("source_quality_note")
    document_quality["packaging_guards"] = verifier_payload.get("packaging_guards", [])
    document_quality["verifier_cautions"] = verifier_payload.get("verifier_cautions", [])
    document_quality["reasoning_status"] = verifier_payload.get("reasoning_status")
    document_quality["surface_inventory_summary"] = verifier_payload.get("surface_inventory_summary", {})
    document_quality["surface_inventory_pages"] = verifier_payload.get("surface_inventory_pages", [])
    document_quality["surface_inventory_limitations"] = verifier_payload.get("surface_inventory_limitations", {})

    if verifier_payload.get("reasoning_status") == "SUPPRESSED_UNREADABLE":
        return

    grouped_explanations = freeze_grouped if isinstance(freeze_grouped, list) else []

    def _find_freeze_entry_with_scope(family: str, field_type: str) -> tuple[Optional[str], Optional[Dict[str, Any]]]:
        if not isinstance(freeze_contract, dict):
            return None, None
        direct = _freeze_contract_field_entry(freeze_contract, "document", family, field_type)
        if isinstance(direct, dict):
            return "document", direct
        fields = freeze_contract.get("fields")
        if not isinstance(fields, dict):
            return None, None
        preferred_scopes = ("lot:unico", "lotto:unico", "lot:1", "lotto:1")
        for scope_key in preferred_scopes:
            candidate = _freeze_contract_field_entry(freeze_contract, scope_key, family, field_type)
            if isinstance(candidate, dict):
                return scope_key, candidate
        for scope_key, scope_payload in fields.items():
            if not isinstance(scope_payload, dict):
                continue
            family_payload = scope_payload.get(family)
            if isinstance(family_payload, dict) and isinstance(family_payload.get(field_type), dict):
                return str(scope_key), family_payload.get(field_type)
        return None, None

    def _find_freeze_entry(family: str, field_type: str) -> Optional[Dict[str, Any]]:
        _, entry = _find_freeze_entry_with_scope(family, field_type)
        return entry

    def _grouped_explanation(field_family: str) -> Optional[Dict[str, Any]]:
        for entry in grouped_explanations:
            if not isinstance(entry, dict):
                continue
            if str(entry.get("field_type") or "").strip().lower() == field_family:
                return entry
        return None

    def _apply_contract_explanation(
        field_key: str,
        *,
        family: str,
        field_type: str,
        default_status: Optional[str] = None,
    ) -> None:
        contract_entry = _find_freeze_entry(family, field_type)
        grouped_entry = _grouped_explanation(family)
        if not isinstance(contract_entry, dict) and not isinstance(grouped_entry, dict):
            return

        existing = field_states.get(field_key)
        state = copy.deepcopy(existing) if isinstance(existing, dict) else {
            "value": None,
            "status": default_status or "LOW_CONFIDENCE",
            "confidence": 0.45,
            "evidence": [],
            "searched_in": [],
            "user_prompt_it": None,
        }
        if str(state.get("status") or "").upper() == "USER_PROVIDED":
            return

        contract_state = str((contract_entry or {}).get("state") or (grouped_entry or {}).get("llm_outcome") or "").strip()
        contract_value = (contract_entry or {}).get("value")
        if contract_value not in (None, "") and (
            state.get("value") in (None, "", "DA VERIFICARE")
            or str(state.get("status") or "").upper() in {"NOT_FOUND", "LOW_CONFIDENCE"}
        ):
            state["value"] = contract_value
        if contract_state in {"deterministic_active", "llm_resolved", "resolved_with_context"} and state.get("value") not in (None, ""):
            state["status"] = "LOW_CONFIDENCE" if is_degraded else "FOUND"
        elif state.get("value") in (None, "") and str(state.get("status") or "").upper() == "NOT_FOUND":
            state["status"] = default_status or "LOW_CONFIDENCE"

        evidence = (contract_entry or {}).get("supporting_evidence")
        if isinstance(evidence, list) and evidence and not state.get("evidence"):
            state["evidence"] = copy.deepcopy(evidence)
        if (contract_entry or {}).get("confidence_band") and not isinstance(state.get("confidence"), (int, float)):
            state["confidence"] = 0.7 if contract_entry.get("confidence_band") == "high" else 0.45

        explanation_payload = {
            "contract_state": contract_state or state.get("contract_state"),
            "explanation": (contract_entry or {}).get("explanation") or (grouped_entry or {}).get("user_visible_explanation"),
            "context_qualification": (contract_entry or {}).get("context_qualification"),
            "why_not_fully_certain": (contract_entry or {}).get("why_not_fully_certain"),
            "why_not_resolved": (contract_entry or {}).get("why_not_resolved") or (grouped_entry or {}).get("why_not_resolved"),
            "source_pages": (contract_entry or {}).get("source_pages") or (grouped_entry or {}).get("source_pages"),
            "supporting_pages": (contract_entry or {}).get("supporting_pages") or (grouped_entry or {}).get("supporting_pages"),
            "tension_pages": (contract_entry or {}).get("tension_pages") or (grouped_entry or {}).get("tension_pages"),
            "needs_human_review": (contract_entry or {}).get("needs_human_review") or (grouped_entry or {}).get("needs_human_review"),
        }
        for meta_key, meta_value in explanation_payload.items():
            if meta_value not in (None, "", []):
                state[meta_key] = copy.deepcopy(meta_value)
        field_states[field_key] = state

    is_degraded = verifier_payload.get("evidence_mode") == DEGRADED_TEXT
    if is_degraded:
        _apply_degraded_result_packaging(result)
    dati = result.setdefault("dati_certi_del_lotto", {})
    quota = rights.get("quota", {}) if isinstance(rights.get("quota"), dict) else {}
    if quota.get("value"):
        dati["quota"] = {
            "value": quota.get("value"),
            "confidence": quota.get("confidence"),
            "evidence": quota.get("evidence", []),
            "source": "verifier_runtime",
        }
        if is_degraded:
            _annotate_degraded_output(dati["quota"])
        lots = result.get("lots", []) if isinstance(result.get("lots"), list) else []
        if lots and isinstance(lots[0], dict):
            lots[0]["quota"] = quota.get("value")
    selected_price = pricing.get("selected_price")
    if isinstance(selected_price, (int, float)):
        dati["prezzo_base_asta_verifier"] = {
            "value": selected_price,
            "confidence": ((verifier_payload.get("judgments") or {}).get("pricing") or {}).get("confidence"),
            "source": "verifier_runtime",
        }
        if is_degraded:
            _annotate_degraded_output(dati["prezzo_base_asta_verifier"])
    field_states = result.setdefault("field_states", {})
    # Promote high-confidence verifier findings to field_states so the final API
    # surface reflects canonical judgments even when server-side extraction fails.
    if quota.get("value") and not field_states.get("quota", {}).get("value"):
        field_states["quota"] = {
            "value": quota.get("value"),
            "status": "LOW_CONFIDENCE" if is_degraded else "FOUND",
            "confidence": quota.get("confidence", 0.0),
            "evidence": quota.get("evidence", []),
            "searched_in": [],
            "user_prompt_it": None,
            "resolver_meta": {"resolver_version": "verifier_runtime_v1"},
        }
        if is_degraded:
            _annotate_degraded_output(field_states["quota"])
    if isinstance(selected_price, (int, float)) and _should_promote_selected_price(
        field_states.get("prezzo_base_asta"), float(selected_price), pricing
    ):
        previous_state = copy.deepcopy(field_states.get("prezzo_base_asta")) if isinstance(field_states.get("prezzo_base_asta"), dict) else None
        field_states["prezzo_base_asta"] = {
            "value": selected_price,
            "status": "LOW_CONFIDENCE" if is_degraded else "FOUND",
            "confidence": float(((verifier_payload.get("judgments") or {}).get("pricing") or {}).get("confidence") or 0.0),
            "evidence": _pricing_evidence_for_selected_price(pricing),
            "searched_in": [],
            "user_prompt_it": None,
            "resolver_meta": {
                "resolver_version": "verifier_runtime_v1",
                "source": "canonical_pricing.selected_price",
                "previous_state": previous_state,
            },
        }
        if is_degraded:
            _annotate_degraded_output(field_states["prezzo_base_asta"])
    benchmark_value = pricing.get("benchmark_value")
    benchmark_scope_key, benchmark_contract_entry = _find_freeze_entry_with_scope("valuation", "valore_stima_raw")
    if (
        isinstance(benchmark_value, (int, float))
        and _benchmark_contract_entry_matches_pricing(benchmark_contract_entry, float(benchmark_value))
        and _should_promote_benchmark_value(field_states.get("valore_stima"), float(benchmark_value))
    ):
        previous_state = copy.deepcopy(field_states.get("valore_stima")) if isinstance(field_states.get("valore_stima"), dict) else None
        canonical_benchmark_value = (benchmark_contract_entry or {}).get("value")
        benchmark_state = {
            "value": canonical_benchmark_value if canonical_benchmark_value not in (None, "") else benchmark_value,
            "status": "LOW_CONFIDENCE" if is_degraded else "FOUND",
            "confidence": float(((verifier_payload.get("judgments") or {}).get("pricing") or {}).get("confidence") or 0.0),
            "evidence": _pricing_evidence_for_benchmark_value(pricing, benchmark_contract_entry or {}),
            "searched_in": list((benchmark_contract_entry or {}).get("supporting_pages") or []),
            "user_prompt_it": None,
            "resolver_meta": {
                "resolver_version": "verifier_runtime_v1",
                "source": f"canonical_freeze_contract.fields.{benchmark_scope_key}.valuation.valore_stima_raw" if benchmark_scope_key else None,
                "source_state": (benchmark_contract_entry or {}).get("state"),
                "canonical_pricing_source": "canonical_pricing.benchmark_value",
                "previous_state": previous_state,
            },
        }
        for context_key in ("explanation", "context_qualification", "why_not_fully_certain", "why_not_resolved"):
            if (benchmark_contract_entry or {}).get(context_key):
                benchmark_state[context_key] = (benchmark_contract_entry or {}).get(context_key)
        field_states["valore_stima"] = benchmark_state
        if is_degraded:
            _annotate_degraded_output(field_states["valore_stima"])
    canonical_agibilita = canonical.get("agibilita", {}) if isinstance(canonical.get("agibilita"), dict) else {}
    agibilita_status = str(canonical_agibilita.get("status") or "").strip().upper()
    if agibilita_status in {"PRESENTE", "ASSENTE", "NON_VERIFICABILE"}:
        previous_agibilita = copy.deepcopy(field_states.get("agibilita")) if isinstance(field_states.get("agibilita"), dict) else None
        verification_trail = canonical_agibilita.get("verification_trail") if isinstance(canonical_agibilita.get("verification_trail"), dict) else {}
        reason_unresolved = str(verification_trail.get("reason_unresolved") or "").strip() or None
        verify_next = str(verification_trail.get("verify_next") or "").strip() or None
        display_value = {
            "PRESENTE": "PRESENTE",
            "ASSENTE": "ASSENTE",
            "NON_VERIFICABILE": "DA VERIFICARE",
        }[agibilita_status]
        field_status = "FOUND" if agibilita_status in {"PRESENTE", "ASSENTE"} and not is_degraded else "LOW_CONFIDENCE"
        if agibilita_status == "NON_VERIFICABILE" and is_degraded:
            field_status = "LOW_CONFIDENCE"
        explanation_parts: List[str] = []
        if agibilita_status == "NON_VERIFICABILE":
            if reason_unresolved:
                explanation_parts.append(reason_unresolved.rstrip("."))
            if verify_next:
                explanation_parts.append(verify_next.rstrip("."))
        else:
            explanation_parts.append(f"L'agibilità risulta {display_value.lower()} nel caso canonico verificato.")
        agibilita_explanation = ". ".join(part for part in explanation_parts if part).strip()
        if agibilita_explanation and not agibilita_explanation.endswith("."):
            agibilita_explanation += "."
        field_states["agibilita"] = {
            "value": display_value,
            "status": field_status,
            "confidence": canonical_agibilita.get("confidence", 0.0),
            "evidence": canonical_agibilita.get("evidence", []),
            "searched_in": [],
            "user_prompt_it": None,
            "contract_state": canonical_agibilita.get("status"),
            "why_not_resolved": reason_unresolved,
            "explanation": agibilita_explanation or None,
            "context_qualification": verify_next,
            "resolver_meta": {
                "resolver_version": "verifier_runtime_v1",
                "source": "canonical_case.agibilita",
                "guards": copy.deepcopy(canonical_agibilita.get("guards", [])) if isinstance(canonical_agibilita.get("guards"), list) else [],
                "previous_state": previous_agibilita,
            },
        }
        if is_degraded:
            _annotate_degraded_output(field_states["agibilita"])
        abusi = result.get("abusi_edilizi_conformita") if isinstance(result.get("abusi_edilizi_conformita"), dict) else {}
        abusi["agibilita"] = {
            "status": display_value,
            "detail_it": display_value,
            "evidence": canonical_agibilita.get("evidence", []),
            "explanation_it": agibilita_explanation or None,
            "why_not_resolved": reason_unresolved,
            "context_qualification": verify_next,
            "contract_state": canonical_agibilita.get("status"),
        }
        result["abusi_edilizi_conformita"] = abusi
    if occupancy.get("status"):
        field_states["stato_occupativo"] = {
            "value": occupancy.get("status"),
            "status": "LOW_CONFIDENCE" if is_degraded else "FOUND",
            "confidence": occupancy.get("confidence", 0.0),
            "evidence": occupancy.get("evidence", []),
            "searched_in": [],
            "user_prompt_it": None,
            "resolver_meta": {"resolver_version": "verifier_runtime_v1"},
        }
        if is_degraded:
            _annotate_degraded_output(field_states["stato_occupativo"])
            field_states["stato_occupativo"]["resolver_meta"]["source_quality_note"] = DEGRADED_SOURCE_NOTE
        result["stato_occupativo"] = {
            "status": occupancy.get("status"),
            "status_it": occupancy.get("status"),
            "status_en": occupancy.get("status"),
            "title_opponible": occupancy.get("opponibilita") or "NON VERIFICABILE",
            "evidence": occupancy.get("evidence", []),
        }
        if is_degraded:
            _annotate_degraded_output(result["stato_occupativo"])
    if occupancy.get("opponibilita"):
        field_states["opponibilita_occupazione"] = {
            "value": occupancy.get("opponibilita"),
            "status": "LOW_CONFIDENCE" if is_degraded or occupancy.get("opponibilita") == "NON VERIFICABILE" else "FOUND",
            "confidence": occupancy.get("confidence", 0.0),
            "evidence": occupancy.get("evidence", []),
            "searched_in": [],
            "user_prompt_it": None,
            "resolver_meta": {"resolver_version": "verifier_runtime_v1"},
        }
        if is_degraded:
            _annotate_degraded_output(field_states["opponibilita_occupazione"])
            field_states["opponibilita_occupazione"]["resolver_meta"]["source_quality_note"] = DEGRADED_SOURCE_NOTE
    urbanistica_status = urbanistica.get("urbanistica_status", {}) if isinstance(urbanistica.get("urbanistica_status"), dict) else {}
    urbanistica_value = str(urbanistica_status.get("value") or "").strip().upper()
    if urbanistica_value in {"REGOLARE", "DIFFORMITA_PRESENTE", "NON_VERIFICABILE"}:
        display_value = {
            "REGOLARE": "REGOLARE URBANISTICAMENTE",
            "DIFFORMITA_PRESENTE": "PRESENTI DIFFORMITA",
            "NON_VERIFICABILE": "DA VERIFICARE",
        }[urbanistica_value]
        field_states["regolarita_urbanistica"] = {
            "value": display_value,
            "status": "LOW_CONFIDENCE" if is_degraded or urbanistica_value == "NON_VERIFICABILE" else "FOUND",
            "confidence": urbanistica_status.get("confidence", 0.0),
            "evidence": urbanistica_status.get("evidence", []),
            "searched_in": [] if urbanistica_value != "NON_VERIFICABILE" else list(urbanistica_status.get("evidence", [])),
            "user_prompt_it": None,
            "resolver_meta": {"resolver_version": "verifier_runtime_v1"},
        }
        if is_degraded:
            _annotate_degraded_output(field_states["regolarita_urbanistica"])
            field_states["regolarita_urbanistica"]["resolver_meta"]["source_quality_note"] = DEGRADED_SOURCE_NOTE
    section_legal = result.setdefault("section_9_legal_killers", {})
    top_items = []
    top_issue = priority.get("top_issue", {}) if isinstance(priority.get("top_issue"), dict) else {}
    if top_issue:
        top_items.append(
            {
                "killer": top_issue.get("title_it"),
                "status": top_issue.get("severity"),
                "action": top_issue.get("action_it"),
                "evidence": top_issue.get("evidence", []),
                "source": "verifier_runtime",
                "category": top_issue.get("category"),
            }
        )
    for entry in legal.get("cancellable", [])[:2]:
        if not isinstance(entry, dict):
            continue
        top_items.append(
            {
                "killer": f"Formalità cancellabile: {entry.get('kind')}",
                "status": "INFO",
                "action": "Background legale da non promuovere a rischio prioritario cliente",
                "evidence": entry.get("evidence", []),
                "source": "verifier_runtime",
                "category": "legal_background",
            }
        )
    if top_items:
        section_legal["top_items"] = top_items[:3]
        if is_degraded:
            _annotate_degraded_output(section_legal, guards_key="packaging_guards")
    money_box = result.setdefault("money_box", {})
    _prune_pricing_amounts_from_money_boxes(result, pricing)
    money_box = result.setdefault("money_box", {})
    money_box["verifier_costs_summary"] = _legacy_money_costs_summary(costs)
    if is_degraded:
        _annotate_degraded_output(money_box["verifier_costs_summary"])
    summary = result.setdefault("summary_for_client", {})
    summary["verifier_bundle"] = summary_bundle
    if str(((result.get("canonical_contract_state") or {}) if isinstance(result.get("canonical_contract_state"), dict) else {}).get("reason") or "").lower() == "canonical_freeze_blocked_unreadable":
        blocked_summary_it = "Documento non leggibile o estrazione bloccata: non è possibile formulare conclusioni affidabili sui profili legali o economici senza verifica manuale."
        blocked_summary_en = "Unreadable document or blocked extraction: no reliable legal or cost conclusion can be produced without manual review."
        summary_bundle = {
            "top_issue_it": "",
            "top_issue_en": "",
            "next_step_it": blocked_summary_it,
            "next_step_en": blocked_summary_en,
            "caution_points_it": ["Verifica manuale obbligatoria sul documento originale."],
            "user_messages_it": [],
            "document_quality_status": "UNREADABLE",
            "semaforo_status": "UNKNOWN",
            "decision_summary_it": blocked_summary_it,
            "decision_summary_en": blocked_summary_en,
            "evidence_snippets": [],
            "source": "canonical_freeze_blocked_unreadable",
        }
        summary["verifier_bundle"] = copy.deepcopy(summary_bundle)
    summary["summary_it"] = summary_bundle.get("decision_summary_it", summary.get("summary_it", ""))
    if summary_bundle.get("decision_summary_en"):
        summary["summary_en"] = summary_bundle.get("decision_summary_en")
    summary["generation_mode"] = "deterministic_canonical_bundle"
    if is_degraded:
        _annotate_degraded_output(summary, guards_key="packaging_guards")
    result["summary_for_client_bundle"] = summary_bundle
    if is_degraded and isinstance(result.get("summary_for_client_bundle"), dict):
        _annotate_degraded_output(result["summary_for_client_bundle"])

    for field_key, family, field_type, default_status in (
        ("stato_occupativo", "occupancy", "occupancy_status_raw", "LOW_CONFIDENCE"),
        ("opponibilita_occupazione", "occupancy", "occupancy_opponibilita_raw", "LOW_CONFIDENCE"),
        ("delivery_timeline", "occupancy", "occupancy_liberazione_raw", "LOW_CONFIDENCE"),
        ("diritto_reale", "rights", "rights_diritto", "LOW_CONFIDENCE"),
        ("prezzo_base_asta", "valuation", "prezzo_base_raw", "LOW_CONFIDENCE"),
        ("valore_stima", "valuation", "valore_stima_raw", "LOW_CONFIDENCE"),
        ("spese_condominiali_arretrate", "cost", "cost_condominiali_arretrati_raw", "NOT_FOUND"),
        ("impianto_riscaldamento_status", "impianti", "impianto_riscaldamento_status", "LOW_CONFIDENCE"),
        ("conformita_catastale", "cadastral", "conformita_catastale_raw", "LOW_CONFIDENCE"),
        ("regolarita_urbanistica", "urbanistica", "urbanistica_status_raw", "LOW_CONFIDENCE"),
        ("agibilita", "agibilita", "agibilita_raw", "NOT_FOUND"),
    ):
        _apply_contract_explanation(
            field_key,
            family=family,
            field_type=field_type,
            default_status=default_status,
        )
