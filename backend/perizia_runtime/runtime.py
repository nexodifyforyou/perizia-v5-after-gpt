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
from perizia_qa.comparators import compare_legacy_and_verifier
from perizia_qa.invariants import run_invariants
from perizia_runtime.pipeline import PeriziaPipeline
from perizia_runtime.state import RuntimeState, to_dict
from perizia_tools.pdf_text_tool import build_pdf_text_payload


def run_quality_verifier(*, analysis_id: str, result: Dict[str, Any], pages: List[Dict[str, Any]], full_text: str) -> Dict[str, Any]:
    payload = build_pdf_text_payload(pages, full_text)
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
    payload_dict = {
        "analysis_id": state.analysis_id,
        "canonical_case": to_dict(state.canonical_case),
        "judgments": to_dict(state.judgments),
        "candidates": to_dict(state.candidates),
    }
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

    dati = result.setdefault("dati_certi_del_lotto", {})
    quota = rights.get("quota", {}) if isinstance(rights.get("quota"), dict) else {}
    if quota.get("value"):
        dati["quota"] = {
            "value": quota.get("value"),
            "confidence": quota.get("confidence"),
            "evidence": quota.get("evidence", []),
            "source": "verifier_runtime",
        }
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
    field_states = result.setdefault("field_states", {})
    if occupancy.get("status"):
        field_states["stato_occupativo"] = {
            "value": occupancy.get("status"),
            "status": "FOUND",
            "confidence": occupancy.get("confidence", 0.0),
            "evidence": occupancy.get("evidence", []),
            "searched_in": [],
            "user_prompt_it": None,
            "resolver_meta": {"resolver_version": "verifier_runtime_v1"},
        }
        result["stato_occupativo"] = {
            "status": occupancy.get("status"),
            "status_it": occupancy.get("status"),
            "status_en": occupancy.get("status"),
            "title_opponible": occupancy.get("opponibilita") or "NON VERIFICABILE",
            "evidence": occupancy.get("evidence", []),
        }
    if occupancy.get("opponibilita"):
        field_states["opponibilita_occupazione"] = {
            "value": occupancy.get("opponibilita"),
            "status": "LOW_CONFIDENCE" if occupancy.get("opponibilita") == "NON VERIFICABILE" else "FOUND",
            "confidence": occupancy.get("confidence", 0.0),
            "evidence": occupancy.get("evidence", []),
            "searched_in": [],
            "user_prompt_it": None,
            "resolver_meta": {"resolver_version": "verifier_runtime_v1"},
        }
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
    money_box = result.setdefault("money_box", {})
    money_box["verifier_costs_summary"] = _legacy_money_costs_summary(costs)
    summary = result.setdefault("summary_for_client", {})
    summary["verifier_bundle"] = summary_bundle
    summary["summary_it"] = summary_bundle.get("decision_summary_it", summary.get("summary_it", ""))
    if summary_bundle.get("decision_summary_en"):
        summary["summary_en"] = summary_bundle.get("decision_summary_en")
    summary["generation_mode"] = "deterministic_canonical_bundle"
    result["summary_for_client_bundle"] = summary_bundle
