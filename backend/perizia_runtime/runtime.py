from __future__ import annotations

import copy
from typing import Any, Dict, List

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


def run_quality_verifier(*, analysis_id: str, result: Dict[str, Any], pages: List[Dict[str, Any]], full_text: str) -> Dict[str, Any]:
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
    legal = canonical.get("legal", {}) if isinstance(canonical.get("legal"), dict) else {}
    pricing = canonical.get("pricing", {}) if isinstance(canonical.get("pricing"), dict) else {}
    costs = canonical.get("costs", {}) if isinstance(canonical.get("costs"), dict) else {}
    priority = canonical.get("priority", {}) if isinstance(priority := canonical.get("priority"), dict) else {}
    summary_bundle = canonical.get("summary_bundle", {}) if isinstance(canonical.get("summary_bundle"), dict) else {}

    result["verifier_runtime"] = verifier_payload
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
    money_box["verifier_costs_summary"] = _legacy_money_costs_summary(costs)
    if is_degraded:
        _annotate_degraded_output(money_box["verifier_costs_summary"])
    summary = result.setdefault("summary_for_client", {})
    summary["verifier_bundle"] = summary_bundle
    summary["summary_it"] = summary_bundle.get("decision_summary_it", summary.get("summary_it", ""))
    if summary_bundle.get("decision_summary_en"):
        summary["summary_en"] = summary_bundle.get("decision_summary_en")
    summary["generation_mode"] = "deterministic_canonical_bundle"
    if is_degraded:
        _annotate_degraded_output(summary, guards_key="packaging_guards")
    result["summary_for_client_bundle"] = summary_bundle
    if is_degraded and isinstance(result.get("summary_for_client_bundle"), dict):
        _annotate_degraded_output(result["summary_for_client_bundle"])
