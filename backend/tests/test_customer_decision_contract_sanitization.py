import json
import copy
import sys
import unittest.mock as mock
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from customer_decision_contract import (
    apply_customer_decision_contract,
    sanitize_customer_facing_result,
    separate_internal_runtime_from_customer_result,
    _explicit_total_is_condo_periodic_sum,
    _is_valuation_narrative,
    _sanitize_address_contamination,
    _sanitize_address_evidence,
    _project_certification_block_to_beni,
    _dedup_legal_killer_items,
    _build_legal_killers,
    _extract_amount_after_term,
)


BANNED_FIELDS = {
    "contract_state",
    "explanation_fallback_reason",
    "explanation_mode",
    "llm_explanation_used",
    "customer_visible_amount_status",
    "source_path",
    "driver_field",
    "theme_resolution",
    "llm_outcome",
    "raw",
    "debug",
    "candidate",
    "candidates",
    "step3_candidates",
}

BANNED_VALUES = {"unresolved_explained", "no_packet"}

CUSTOMER_KEYS = {
    "issues",
    "field_states",
    "section_3_money_box",
    "money_box",
    "section_9_legal_killers",
    "abusi_edilizi_conformita",
    "red_flags_operativi",
    "section_11_red_flags",
    "customer_decision_contract",
}


def _collect_customer_hits(value: Any, path: str = "root") -> list[tuple[str, str, Any]]:
    hits: list[tuple[str, str, Any]] = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}"
            if key in BANNED_FIELDS:
                hits.append((child_path, "field", child))
            hits.extend(_collect_customer_hits(child, child_path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            hits.extend(_collect_customer_hits(child, f"{path}[{index}]"))
    elif isinstance(value, str) and value.strip() in BANNED_VALUES:
        hits.append((path, "value", value))
    return hits


def test_sanitize_customer_facing_result_strips_internal_controls_only_from_customer_structures():
    result = {
        "issues": [
            {
                "headline_it": "Opponibilità occupazione: NON VERIFICABILE.",
                "explanation_it": "La perizia segnala occupazione, ma non chiarisce il titolo.",
                "why_not_resolved": "Manca il titolo opponibile.",
                "verify_next_it": "Verificare contratto, data e registrazione.",
                "contract_state": "unresolved_explained",
                "explanation_fallback_reason": "no_packet",
                "llm_explanation_used": False,
                "explanation_mode": "conflict_explained",
                "source_path": "field_states.opponibilita_occupazione",
                "scope": {"level": "bene", "scope_key": "bene:1", "bene_number": 1},
                "status": "LOW_CONFIDENCE",
                "severity": "AMBER",
                "theme": "occupancy",
                "evidence": [{"page": 21, "quote": "Occupato da debitore."}],
                "supporting_pages": [21],
                "tension_pages": [],
            }
        ],
        "field_states": {
            "opponibilita_occupazione": {
                "headline_it": "Opponibilità occupazione: NON VERIFICABILE.",
                "why_not_resolved": "Manca il titolo opponibile.",
                "verify_next_it": "Verificare contratto, data e registrazione.",
                "contract_state": "unresolved_explained",
                "explanation_fallback_reason": "no_packet",
                "llm_explanation_used": False,
                "evidence": [{"page": 21, "quote": "Occupato da debitore."}],
                "supporting_pages": [21],
                "tension_pages": [],
            }
        },
        "section_3_money_box": {
            "total_extra_costs": {
                "min": None,
                "max": None,
                "note": "Serve verifica manuale.",
                "contract_state": "unresolved_explained",
            }
        },
        "money_box": {
            "items": [
                {
                    "label_it": "Costo buyer-side",
                    "stima_nota": "Importo non chiuso.",
                    "customer_visible_amount_status": "unresolved_explained",
                }
            ]
        },
        "section_9_legal_killers": {
            "resolver_meta": {
                "themes": [
                    {
                        "theme": "occupazione_titolo_opponibilita",
                        "theme_resolution": "unresolved_explained",
                        "driver_field": "field_states.opponibilita_occupazione",
                        "driver_status": "LOW_CONFIDENCE",
                        "driver_value": "NON VERIFICABILE",
                    }
                ]
            }
        },
        "dati_certi_del_lotto": {
            "quota": {
                "value": "1/1",
                "source": "verifier_runtime",
            },
            "diritto_reale": {
                "value": "Proprietà 1/1",
                "source": "Perizia",
            },
        },
        "abusi_edilizi_conformita": {
            "agibilita": {
                "status": "ASSENTE",
                "detail_it": "Agibilità assente.",
                "contract_state": "deterministic_active",
            }
        },
        "verifier_runtime": {
            "contract_state": "unresolved_explained",
            "source_path": "internal.runtime.path",
        },
        "resolver_meta": {"resolver_version": "verifier_runtime_v1"},
    }
    result["customer_decision_contract"] = {
        key: result[key]
        for key in ("issues", "field_states", "money_box", "section_9_legal_killers")
    }

    sanitize_customer_facing_result(result)

    hits = []
    for key in CUSTOMER_KEYS:
        if key in result:
            hits.extend(_collect_customer_hits(result[key], f"result.{key}"))
    assert hits == []

    issue = result["issues"][0]
    assert issue["headline_it"] == "Opponibilità occupazione: NON VERIFICABILE."
    assert issue["why_not_resolved"] == "Manca il titolo opponibile."
    assert issue["verify_next_it"] == "Verificare contratto, data e registrazione."
    assert issue["evidence"][0]["page"] == 21
    assert issue["scope"]["scope_key"] == "bene:1"
    assert issue["status"] == "LOW_CONFIDENCE"
    assert issue["severity"] == "AMBER"
    assert issue["theme"] == "occupancy"
    assert "source" not in result["dati_certi_del_lotto"]["quota"]
    assert result["dati_certi_del_lotto"]["diritto_reale"]["source"] == "Perizia"
    assert "resolver_meta" not in result
    assert result["verifier_runtime"]["contract_state"] == "unresolved_explained"


def test_separate_internal_runtime_removes_runtime_keys_from_customer_result():
    result = {
        "issues": [{"headline_it": "Issue", "evidence": [{"page": 1, "quote": "Quote"}]}],
        "summary_for_client_bundle": {"decision_summary_it": "Issue"},
        "verifier_runtime": {"canonical_case": {"freeze_contract": {"state": "unresolved_explained"}}},
        "canonical_freeze_contract": {"fields": {"field::document::x": {"state": "unresolved_explained"}}},
        "canonical_freeze_explanations": [{"explanation_fallback_reason": "no_packet"}],
        "debug": {"candidate_summary": {"total_candidates": 3}},
    }

    internal_runtime = separate_internal_runtime_from_customer_result(result)

    assert "verifier_runtime" not in result
    assert "canonical_freeze_contract" not in result
    assert "canonical_freeze_explanations" not in result
    assert "debug" not in result
    assert result["issues"][0]["headline_it"] == "Issue"
    assert result["summary_for_client_bundle"]["decision_summary_it"] == "Issue"
    assert internal_runtime["verifier_runtime"]["canonical_case"]["freeze_contract"]["state"] == "unresolved_explained"
    assert internal_runtime["canonical_freeze_explanations"][0]["explanation_fallback_reason"] == "no_packet"


def test_apply_customer_contract_surfaces_anchored_money_signals_without_fake_additive_total():
    result = {
        "field_states": {},
        "estratto_quality": {
            "sections": [
                {
                    "heading_key": "abusi_agibilita",
                    "items": [
                        {
                            "item_id": "ab_cost_m_000049",
                            "label_it": "Costo collegato ad abusi/agibilità: 23000,00 €",
                            "candidate_ids": ["m_000049"],
                            "amount_eur": 0,
                            "evidence": [
                                {
                                    "page": 40,
                                    "quote": "Deprezzamenti Oneri di regolarizzazione urbanistica 23000,00 € Rischio assunto per mancata garanzia 5000,00 € Valore finale di stima: € 391.849,00",
                                }
                            ],
                        },
                        {
                            "item_id": "ab_cost_m_000027",
                            "label_it": "Costo collegato ad abusi/agibilità: € 15.000,00",
                            "candidate_ids": ["m_000027"],
                            "amount_eur": 15000,
                            "evidence": [
                                {
                                    "page": 37,
                                    "quote": "Si quantificano le spese di massima per completare l'immobile e per le pratiche dell'abitabilità: - Completamento lavori € 15.000,00; - pratiche per abitabilità € 5.000,00",
                                }
                            ],
                        },
                        {
                            "item_id": "ab_cost_m_000031",
                            "label_it": "Costo collegato ad abusi/agibilità: € 5.000,00",
                            "candidate_ids": ["m_000031"],
                            "amount_eur": 5000,
                            "evidence": [
                                {
                                    "page": 38,
                                    "quote": "pratiche per abitabilità € 5.000,00 (già conteggiate)",
                                }
                            ],
                        },
                    ],
                }
            ]
        },
        "verifier_runtime": {
            "canonical_case": {
                "costs": {
                    "explicit_buyer_costs": [],
                    "valuation_adjustments": [],
                    "explicit_total": None,
                },
                "priority": {},
            }
        },
    }

    apply_customer_decision_contract(result)

    money_box = result["section_3_money_box"]
    assert money_box["items"]
    serialized_money_box = json.dumps(money_box, ensure_ascii=False).lower()

    assert "oneri di regolarizzazione urbanistica" in serialized_money_box
    assert "23000" in serialized_money_box or "23.000" in serialized_money_box
    assert "rischio assunto per mancata garanzia" in serialized_money_box
    assert "completamento lavori" in serialized_money_box
    assert "15000" in serialized_money_box or "15.000" in serialized_money_box
    assert "pratiche per abitabilità" in serialized_money_box
    assert "5000" in serialized_money_box or "5.000" in serialized_money_box
    assert "già conteggiate" in serialized_money_box

    valuation_deductions = money_box["valuation_deductions"]
    assert {
        item["classification"]
        for item in valuation_deductions
    } == {"valuation_deduction", "valuation_risk_deduction"}
    assert all(item["additive_to_extra_total"] is False for item in valuation_deductions)
    assert {ev["page"] for item in valuation_deductions for ev in item["evidence"]} == {40}

    cost_signals = money_box["cost_signals_to_verify"]
    assert cost_signals
    assert all(item["additive_to_extra_total"] is False for item in cost_signals)
    assert {37, 38}.issubset({ev["page"] for item in cost_signals for ev in item["evidence"]})

    total = money_box["total_extra_costs"]
    assert total["min"] is None
    assert total["max"] is None
    assert "48.000" not in json.dumps(total, ensure_ascii=False)
    assert "28.000" not in json.dumps(total, ensure_ascii=False)
    assert "non sommati" in total["note"]

    hits = []
    for key in CUSTOMER_KEYS:
        if key in result:
            hits.extend(_collect_customer_hits(result[key], f"result.{key}"))
    assert hits == []

    customer_payload = {
        "section_3_money_box": result["section_3_money_box"],
        "money_box": result["money_box"],
        "customer_decision_contract": result["customer_decision_contract"],
    }
    serialized_customer = json.dumps(customer_payload, ensure_ascii=False)
    for forbidden in (
        "ab_cost_m_",
        "raw",
        "debug",
        "candidate",
        "unresolved_explained",
        "source_path",
        "verifier_runtime",
    ):
        assert forbidden not in serialized_customer


def test_apply_customer_contract_respects_explicit_opponible_lease():
    result = {
        "field_states": {
            "stato_occupativo": {
                "value": "OCCUPATO",
                "status": "FOUND",
                "confidence": 0.96,
                "headline_it": "Stato occupativo: OCCUPATO.",
                "evidence": [
                    {
                        "page": 14,
                        "quote": "Stato di occupazione: Occupato da terzi con contratto di locazione opponibile",
                    }
                ],
            },
            "opponibilita_occupazione": {
                "value": "NON VERIFICABILE",
                "status": "LOW_CONFIDENCE",
                "confidence": 0.6,
                "headline_it": "Opponibilità occupazione: NON VERIFICABILE.",
                "evidence": [
                    {
                        "page": 14,
                        "quote": "Stato di occupazione: Occupato da terzi con contratto di locazione opponibile",
                    }
                ],
                "why_not_resolved": "Vecchia logica troppo prudente.",
            },
        },
        "verifier_runtime": {
            "canonical_case": {
                "priority": {
                    "issues": [
                        {
                            "family": "occupancy",
                            "severity": "RED",
                            "headline_it": "Immobile occupato.",
                            "evidence": [
                                {
                                    "page": 14,
                                    "quote": "Stato di occupazione: Occupato da terzi con contratto di locazione opponibile",
                                }
                            ],
                        }
                    ]
                },
                "grouped_llm_explanations": [],
            },
            "scopes": {},
        },
    }

    apply_customer_decision_contract(result)

    opp = result["field_states"]["opponibilita_occupazione"]
    assert opp["value"] == "OPPONIBILE"
    assert opp["status"] == "FOUND"
    assert "contratto di locazione opponibile" in opp["explanation_it"]

    headlines = [issue["headline_it"] for issue in result["issues"]]
    assert headlines[0] == "Occupato da terzi con contratto di locazione opponibile."
    assert "Opponibilità occupazione: NON VERIFICABILE." not in headlines
    assert "Stato occupativo: OCCUPATO." not in headlines
    assert sum(1 for issue in result["issues"] if issue.get("family") == "occupancy") == 1

    section2 = result["section_2_decisione_rapida"]
    assert "contratto di locazione opponibile" in section2["summary_it"]


def test_apply_customer_contract_separates_ape_absent_from_present_impianto_declarations():
    quote = (
        "Certificazioni energetiche e dichiarazioni di conformità\n"
        "•Non esiste il certificato energetico dell'immobile / APE.\n"
        "•Esiste la dichiarazione di conformità dell'impianto elettrico.\n"
        "•Esiste la dichiarazione di conformità dell'impianto termico.\n"
        "•Esiste la dichiarazione di conformità dell'impianto idrico."
    )
    result = {
        "field_states": {},
        "section_9_legal_killers": {
            "top_items": [
                {
                    "killer": "Vincolo che resta a carico dell'acquirente",
                    "status": "RED",
                    "category": "legal",
                    "action": "Verifica legale immediata prima dell'offerta.",
                    "evidence": [{"page": 10, "quote": quote}],
                }
            ]
        },
        "verifier_runtime": {
            "canonical_case": {
                "priority": {},
                "grouped_llm_explanations": [],
            }
        },
    }

    apply_customer_decision_contract(result)

    legal_killers = result["section_9_legal_killers"]
    serialized = json.dumps(legal_killers, ensure_ascii=False).lower()

    assert "ape assente; dichiarazioni impianti indicate come presenti" in serialized
    assert "non considerare mancanti le dichiarazioni elettrica, termica e idrica" in serialized
    assert "vincolo che resta a carico dell'acquirente" not in serialized
    assert "dichiarazione impianto elettrico: non esiste" not in serialized
    assert "dichiarazione impianto termico: non esiste" not in serialized
    assert "dichiarazione impianto idrico: non esiste" not in serialized


def test_apply_customer_contract_rejects_date_fragment_money_and_dedupes_regolarizzazione():
    result = {
        "field_states": {},
        "estratto_quality": {
            "sections": [
                {
                    "heading_key": "abusi_agibilita",
                    "items": [
                        {
                            "item_id": "ab_t_date_sanatoria",
                            "label_it": "Sanatoria edilizia",
                            "evidence": [
                                {
                                    "page": 9,
                                    "quote": "concessione A SANATORIA per opere edilizie emessa il 20/06/2000 al n. 9197/93",
                                }
                            ],
                        },
                        {
                            "item_id": "ab_cost_regolarizzazione_generic",
                            "label_it": "Costo della regolarizzazione urbanistica in € 2.500,00",
                            "evidence": [
                                {
                                    "page": 10,
                                    "quote": "lo scrivente perito stima il costo della regolarizzazione urbanistica in € 2.500,00",
                                }
                            ],
                        },
                        {
                            "item_id": "ab_cost_regolarizzazione_specific",
                            "label_it": "Oneri di regolarizzazione urbanistica 2500,00 €",
                            "evidence": [
                                {
                                    "page": 11,
                                    "quote": "Spese condominiali insolute 6500,00 € Oneri di regolarizzazione urbanistica 2500,00 €",
                                }
                            ],
                        },
                    ],
                }
            ]
        },
        "verifier_runtime": {
            "canonical_case": {
                "costs": {
                    "explicit_buyer_costs": [],
                    "valuation_adjustments": [],
                    "explicit_total": None,
                },
                "priority": {},
                "grouped_llm_explanations": [],
            }
        },
    }

    apply_customer_decision_contract(result)

    money_box = result["section_3_money_box"]
    serialized = json.dumps(money_box, ensure_ascii=False).lower()

    assert "oblazione / sanatoria: € 20" not in serialized
    assert '"amount_eur": 20' not in serialized
    assert "oblazione / sanatoria" not in serialized

    labels = [item.get("label_it") for item in money_box.get("items", [])]
    assert "Oneri di regolarizzazione urbanistica: € 2.500" in labels
    assert "Spese condominiali insolute: € 6.500" in labels
    assert "Regolarizzazione: € 2.500" not in labels

    total = money_box["total_extra_costs"]
    assert total["min"] is None
    assert total["max"] is None
    assert "non sommati" in total["note"]


def test_apply_customer_contract_surfaces_condominium_arrears_without_fake_sum():
    result = {
        "field_states": {},
        "estratto_quality": {
            "sections": [
                {
                    "heading_key": "costi_condominiali",
                    "items": [
                        {
                            "item_id": "condo_context",
                            "label_it": "Spese condominiali",
                            "evidence": [
                                {
                                    "page": 10,
                                    "quote": (
                                        "Importo medio annuo delle spese condominiali: € 900,00 "
                                        "Totale spese per l'anno in corso e precedente: € 5.777,09 "
                                        "Importo spese straordinarie già deliberate: € 0,00"
                                    ),
                                }
                            ],
                        },
                        {
                            "item_id": "condo_arrears",
                            "label_it": "Spese condominiali insolute 6500,00 €",
                            "evidence": [
                                {
                                    "page": 11,
                                    "quote": "Spese condominiali insolute 6500,00 € Oneri di regolarizzazione urbanistica 2500,00 €",
                                }
                            ],
                        },
                    ],
                }
            ]
        },
        "verifier_runtime": {
            "canonical_case": {
                "costs": {
                    "explicit_buyer_costs": [],
                    "valuation_adjustments": [],
                    "explicit_total": None,
                },
                "priority": {},
                "grouped_llm_explanations": [],
            }
        },
    }

    apply_customer_decision_contract(result)

    money_box = result["section_3_money_box"]
    serialized = json.dumps(money_box, ensure_ascii=False).lower()

    assert "spese condominiali insolute: € 6.500" in serialized
    assert "oneri di regolarizzazione urbanistica: € 2.500" in serialized
    assert "6.677" not in serialized
    assert "6677" not in serialized

    for item in money_box.get("items", []):
        assert item.get("additive_to_extra_total") is False
        assert item.get("stima_euro") is None

    total = money_box["total_extra_costs"]
    assert total["min"] is None
    assert total["max"] is None
    assert "non sommati" in total["note"]


# ── Stage 1: Condo periodic sum guard ─────────────────────────────────────────

def _make_condo_periodic_costs(include_priority_explicit_buyer=True):
    """Build a canonical_case.costs fixture matching the Campogalliani pattern."""
    priority = {}
    if include_priority_explicit_buyer:
        priority = {
            "issues": [
                {
                    "code": "EXPLICIT_BUYER_COSTS",
                    "title_it": "Costi espliciti a carico dell'acquirente: € 6.677,09",
                    "severity": "AMBER",
                    "category": "costs",
                    "priority_score": 70.0,
                    "evidence": [
                        {
                            "page": 10,
                            "quote": (
                                "Importo medio annuo delle spese condominiali: € 900,00\n"
                                "Totale spese per l'anno in corso e precedente: € 5.777,09"
                            ),
                        }
                    ],
                    "summary_it": "La perizia riporta spese condominiali annue (€900) e totale anno corrente/precedente (€5.777,09).",
                    "action_it": "Verificare con amministratore.",
                }
            ]
        }
    return {
        "explicit_buyer_costs": [
            {
                "amount": 0.0,
                "evidence": [{"page": 10, "quote": "Totale spese per l'anno in corso e precedente: € 5.777,09"}],
                "label": "Totale spese per l'anno in corso e precedente: € 5.777,09",
            },
            {
                "amount": 900.0,
                "evidence": [{"page": 10, "quote": "Importo medio annuo delle spese condominiali: € 900,00"}],
                "label": "Importo medio annuo delle spese condominiali: € 900,00",
            },
            {
                "amount": 5777.09,
                "evidence": [{"page": 10, "quote": "Totale spese per l'anno in corso e precedente: € 5.777,09"}],
                "label": "Totale spese per l'anno in corso e precedente: € 5.777,09",
            },
        ],
        "valuation_adjustments": [],
        "explicit_total": 6677.09,
        "explicit_total_low_confidence": None,
        "guards": [],
        "lines": [],
        "priority": priority,
    }


def test_explicit_total_is_condo_periodic_sum_detects_annual_plus_year_total():
    costs = _make_condo_periodic_costs()["costs"] if "costs" in _make_condo_periodic_costs() else _make_condo_periodic_costs()
    # Build the costs dict directly
    costs_dict = {
        "explicit_buyer_costs": [
            {"amount": 900.0, "evidence": [{"page": 10, "quote": "Importo medio annuo delle spese condominiali: € 900,00"}], "label": "Importo medio annuo..."},
            {"amount": 5777.09, "evidence": [{"page": 10, "quote": "Totale spese per l'anno in corso e precedente: € 5.777,09"}], "label": "Totale spese anno..."},
        ],
        "explicit_total": 6677.09,
    }
    assert _explicit_total_is_condo_periodic_sum(costs_dict) is True


def test_explicit_total_is_condo_periodic_sum_allows_real_buyer_cost():
    costs_dict = {
        "explicit_buyer_costs": [
            {"amount": 2500.0, "evidence": [{"page": 10, "quote": "Oneri di regolarizzazione urbanistica: €2.500"}], "label": "Oneri regolarizzazione"},
            {"amount": 6500.0, "evidence": [{"page": 11, "quote": "Spese condominiali insolute: €6.500"}], "label": "Spese condominiali insolute"},
        ],
        "explicit_total": 9000.0,
    }
    assert _explicit_total_is_condo_periodic_sum(costs_dict) is False


def test_apply_contract_drops_fake_6677_from_issues_and_money_box():
    """€6.677,09 derived from annual avg + year total must not appear in customer output."""
    costs_data = _make_condo_periodic_costs()
    priority_data = costs_data.pop("priority")
    result = {
        "field_states": {},
        "estratto_quality": {"sections": [
            {"heading_key": "costi", "items": [
                {"item_id": "arrears", "label_it": "Spese condominiali insolute: €6.500",
                 "evidence": [{"page": 11, "quote": "Spese condominiali insolute 6500,00 €"}]},
                {"item_id": "reg", "label_it": "Oneri di regolarizzazione urbanistica: €2.500",
                 "evidence": [{"page": 11, "quote": "Oneri di regolarizzazione urbanistica 2500,00 €"}]},
            ]}
        ]},
        "verifier_runtime": {
            "canonical_case": {
                "costs": costs_data,
                "priority": priority_data,
            }
        },
    }

    apply_customer_decision_contract(result)

    # Only check customer-facing sections (verifier_runtime still holds raw data)
    customer_payload = {
        k: result[k]
        for k in ("issues", "section_3_money_box", "money_box", "red_flags_operativi",
                  "section_9_legal_killers", "section_2_decisione_rapida", "summary_for_client",
                  "summary_for_client_bundle", "customer_decision_contract")
        if k in result
    }
    serialized = json.dumps(customer_payload, ensure_ascii=False)
    # No fake total anywhere in customer output
    assert "6.677" not in serialized
    assert "6677" not in serialized
    # No "Costi espliciti a carico" issue in customer issues
    assert "Costi espliciti a carico dell" not in serialized

    # Money box must be CONSERVATIVE (no total)
    mb = result.get("section_3_money_box", {})
    assert mb.get("policy") == "CONSERVATIVE"
    total = mb.get("total_extra_costs", {})
    assert total.get("min") is None
    assert total.get("max") is None

    # Real signals still present
    items_labels = [item.get("label_it", "") for item in mb.get("items", [])]
    assert any("condominiali" in lbl.lower() or "regolarizzazione" in lbl.lower() for lbl in items_labels)


# ── Stage 2: Certification projection to beni ─────────────────────────────────

def test_project_certification_block_corrects_wrong_electric_declaration():
    """When evidence says 'Esiste la dichiarazione di conformità dell'impianto elettrico',
    a wrong 'Non esiste' in beni.dichiarazioni_impianti.elettrico must be corrected."""
    result = {
        "beni": [
            {
                "bene_number": 1,
                "dichiarazioni_impianti": {"elettrico": "Non esiste", "termico": "Presente", "idrico": "Presente"},
                "dichiarazioni": {
                    "dichiarazione_impianto_elettrico": "Non esiste",
                    "dichiarazione_impianto_termico": "Presente",
                    "dichiarazione_impianto_idrico": "Presente",
                },
            }
        ],
        "lots": [],
    }
    issues = [
        {
            "severity": "AMBER",
            "family": "legal",
            "evidence": [
                {
                    "page": 10,
                    "quote": (
                        "Non esiste il certificato energetico dell'immobile / APE.\n"
                        "Esiste la dichiarazione di conformità dell'impianto elettrico.\n"
                        "Esiste la dichiarazione di conformità dell'impianto termico.\n"
                        "Esiste la dichiarazione di conformità dell'impianto idrico."
                    ),
                }
            ],
        }
    ]
    _project_certification_block_to_beni(result, issues)

    bene = result["beni"][0]
    assert bene["dichiarazioni_impianti"]["elettrico"] == "Presente"
    assert bene["dichiarazioni_impianti"]["termico"] == "Presente"
    assert bene["dichiarazioni_impianti"]["idrico"] == "Presente"
    assert bene["dichiarazioni"]["dichiarazione_impianto_elettrico"] == "Presente"
    assert bene["dichiarazioni"]["dichiarazione_impianto_termico"] == "Presente"
    assert bene["dichiarazioni"]["dichiarazione_impianto_idrico"] == "Presente"


def test_project_certification_block_does_not_overwrite_correct_values():
    """When beni already shows 'Presente', the projection must not change it."""
    result = {
        "beni": [
            {"bene_number": 1, "dichiarazioni_impianti": {"elettrico": "Presente"}, "dichiarazioni": {}}
        ],
        "lots": [],
    }
    issues = [
        {
            "evidence": [
                {"page": 10, "quote": "Esiste la dichiarazione di conformità dell'impianto elettrico."}
            ]
        }
    ]
    _project_certification_block_to_beni(result, issues)
    assert result["beni"][0]["dichiarazioni_impianti"]["elettrico"] == "Presente"


# ── Stage 3: Address contamination sanitization ────────────────────────────────

def test_is_valuation_narrative_detects_marker_phrases():
    assert _is_valuation_narrative("Il valore commerciale dei beni pignorati è stato determinato...")
    assert _is_valuation_narrative("determinato sulla base delle seguenti variabili")
    assert not _is_valuation_narrative("VIA GARIBALDI 10, MANTOVA")
    assert not _is_valuation_narrative(None)


def test_sanitize_address_contamination_replaces_narrative_with_beni_address():
    result = {
        "report_header": {
            "address": {
                "value": "Il valore commerciale dei beni pignorati è stato determinato sulla base",
                "full": "Il valore commerciale dei beni pignorati è stato determinato sulla base",
                "evidence": [],
            }
        },
        "case_header": {
            "address": {
                "value": "Il valore commerciale dei beni pignorati è stato determinato sulla base",
                "full": "Il valore commerciale dei beni pignorati è stato determinato sulla base",
            }
        },
        "beni": [
            {"bene_number": 1, "short_location": "AppartamentoMantova (MN) - VIA CAMPOGALLIANI N. 12"}
        ],
        "lots": [],
    }
    _sanitize_address_contamination(result)

    rh_addr = result["report_header"]["address"]["value"]
    ch_addr = result["case_header"]["address"]["value"]
    assert "valore commerciale" not in rh_addr.lower()
    assert "valore commerciale" not in ch_addr.lower()
    # Should use the address from beni short_location
    assert "CAMPOGALLIANI" in rh_addr.upper() or rh_addr == "Indirizzo da verificare"


def test_sanitize_address_contamination_fallback_when_no_beni_address():
    result = {
        "report_header": {
            "address": {"value": "Il valore commerciale dei beni pignorati è stato determinato."}
        },
        "case_header": {"address": {"value": "determinato sulla base delle variabili"}},
        "beni": [],
        "lots": [],
    }
    _sanitize_address_contamination(result)
    assert result["report_header"]["address"]["value"] == "Indirizzo da verificare"
    assert result["case_header"]["address"]["value"] == "Indirizzo da verificare"


def test_sanitize_address_normalizes_camel_joined_short_location():
    result = {
        "report_header": {},
        "case_header": {},
        "beni": [{"short_location": "AppartamentoMantova (MN) - VIA GARIBALDI 10"}],
        "lots": [],
    }
    _sanitize_address_contamination(result)
    assert result["beni"][0]["short_location"] == "Appartamento Mantova (MN) - VIA GARIBALDI 10"


# ── Stage 1-F: Address evidence sanitization ──────────────────────────────────

def test_sanitize_address_evidence_removes_valuation_narrative_quotes():
    """Evidence entries whose quote contains valuation narrative must be stripped from address evidence."""
    addr = {
        "value": "VIA GARIBALDI 10, MANTOVA",
        "evidence": [
            {
                "page": 10,
                "quote": "Il valore commerciale dei beni pignorati è stato determinato sulla base delle seguenti variabili",
            },
            {
                "page": 5,
                "quote": "VIA GARIBALDI 10",
            },
        ],
    }
    _sanitize_address_evidence(addr)
    assert len(addr["evidence"]) == 1
    assert addr["evidence"][0]["quote"] == "VIA GARIBALDI 10"


def test_sanitize_address_contamination_removes_narrative_evidence_when_value_is_correct():
    """When address value is already correct, narrative evidence quotes must still be stripped."""
    result = {
        "report_header": {
            "address": {
                "value": "VIA CAMPOGALLIANI N. 12",
                "evidence": [
                    {
                        "page": 10,
                        "quote": "Il valore commerciale dei beni pignorati è stato determinato sulla base delle seguenti variabili: ubicazione dell'immobile",
                    }
                ],
            }
        },
        "case_header": {"address": {"value": "VIA CAMPOGALLIANI N. 12", "evidence": []}},
        "beni": [{"short_location": "Appartamento Mantova (MN) - VIA CAMPOGALLIANI N. 12"}],
        "lots": [],
    }
    _sanitize_address_contamination(result)
    # Value must remain correct
    assert result["report_header"]["address"]["value"] == "VIA CAMPOGALLIANI N. 12"
    # Valuation narrative evidence must be stripped
    assert result["report_header"]["address"]["evidence"] == []


# ── Stage 1-F2: Legal killers deduplication ───────────────────────────────────

def test_dedup_legal_killer_items_removes_duplicates_by_killer_and_status():
    """Duplicate legal killer items (same killer + status) must be collapsed to one."""
    items = [
        {"killer": "Difformità da regolarizzare", "status": "SI", "action": "Verifica"},
        {"killer": "Usi civici", "status": "SI"},
        {"killer": "Difformità da regolarizzare", "status": "SI", "action": "Verifica (dup)"},
    ]
    deduped = _dedup_legal_killer_items(items)
    killers = [i["killer"] for i in deduped]
    assert killers.count("Difformità da regolarizzare") == 1
    assert "Usi civici" in killers
    assert len(deduped) == 2


def test_build_legal_killers_deduplicates_existing_items():
    """_build_legal_killers must not expose duplicate items to the customer contract."""
    existing = {
        "items": [
            {"killer": "Difformità da regolarizzare", "status": "SI", "evidence": []},
            {"killer": "Usi civici", "status": "SI", "evidence": []},
            {"killer": "Difformità da regolarizzare", "status": "SI", "evidence": []},
            {"killer": "Servitù rilevata", "status": "GIALLO", "evidence": []},
        ],
        "top_items": [],
        "resolver_meta": {"themes": [{"theme": "urbanistica"}]},
    }
    result = _build_legal_killers(existing, issues=[])
    killers = [i.get("killer") for i in result["items"]]
    assert killers.count("Difformità da regolarizzare") == 1


# ── Stage 1-A: Zero-amount VR_COST items must not appear in money box ─────────

def test_apply_contract_zero_amount_vr_cost_items_excluded_from_money_box():
    """Verifier items with amount=0 must not produce VR_COST items in money box."""
    result = {
        "field_states": {},
        "verifier_runtime": {
            "canonical_case": {
                "costs": {
                    "explicit_buyer_costs": [
                        {
                            "code": "VR_COST_01",
                            "amount": 0,
                            "label": "raw ocr text fragment that should not appear",
                            "evidence": [{"page": 5, "quote": "raw ocr text"}],
                        }
                    ],
                    "explicit_total": None,
                },
                "priority": {},
            }
        },
    }
    apply_customer_decision_contract(result)
    mb = result.get("section_3_money_box", {})
    items = mb.get("items", [])
    vr_cost_items = [i for i in items if str(i.get("code", "")).startswith("VR_COST")]
    assert len(vr_cost_items) == 0, "Zero-amount VR_COST items must not appear in money box"


def test_apply_contract_positive_amount_vr_cost_items_appear_with_clean_label():
    """VR_COST items with a positive amount and short label appear correctly in money box."""
    result = {
        "field_states": {},
        "verifier_runtime": {
            "canonical_case": {
                "costs": {
                    "explicit_buyer_costs": [
                        {
                            "code": "VR_COST_01",
                            "amount": 2500,
                            "label": "Oneri di regolarizzazione",
                            "evidence": [{"page": 5, "quote": "Oneri di regolarizzazione 2500 euro"}],
                        }
                    ],
                    "explicit_total": 2500,
                },
                "priority": {},
            }
        },
    }
    apply_customer_decision_contract(result)
    mb = result.get("section_3_money_box", {})
    items = mb.get("items", [])
    vr_items = [i for i in items if str(i.get("code", "")).startswith("VR_COST")]
    assert len(vr_items) == 1
    assert vr_items[0]["stima_euro"] == 2500
    assert vr_items[0]["label_it"] == "Oneri di regolarizzazione"


def test_apply_contract_long_ocr_label_replaced_with_generic():
    """VR_COST items with a raw OCR fragment (> 120 chars or newline) must use generic label."""
    raw_label = "spese anno corrente\n€5.777,09\nImporto spese straordinarie già deliberate: € 0,00\nCon mail del 08/11/2024"
    result = {
        "field_states": {},
        "verifier_runtime": {
            "canonical_case": {
                "costs": {
                    "explicit_buyer_costs": [
                        {
                            "code": "VR_COST_01",
                            "amount": 5777,
                            "label": raw_label,
                            "evidence": [{"page": 10, "quote": raw_label[:80]}],
                        }
                    ],
                    "explicit_total": 5777,
                },
                "priority": {},
            }
        },
    }
    apply_customer_decision_contract(result)
    mb = result.get("section_3_money_box", {})
    items = mb.get("items", [])
    vr_items = [i for i in items if str(i.get("code", "")).startswith("VR_COST")]
    if vr_items:
        assert "\n" not in vr_items[0]["label_it"]
        assert len(vr_items[0]["label_it"]) <= 120


# ── Stage 7 (QA Gate): Tests with mocked LLM ─────────────────────────────────

from customer_contract_qa_gate import (
    apply_customer_contract_qa_gate,
    apply_final_safety_invariants,
    apply_customer_qa_corrections,
    validate_customer_qa_response,
    _apply_remove_exact_total,
    _scan_for_fake_total_phrases,
    _lot_libero_from_false_marker,
    build_customer_qa_context,
    build_page_text_pack,
    _normalize_raw_text_to_page_map,
    _build_claims_to_challenge,
    _regex_extract_beni,
    _is_placeholder_location,
    _apply_backfill_details,
    attach_qa_gate_metadata,
    _mongo_safe,
    apply_customer_facing_consistency_sweep,
    _collect_customer_facing_bad_text_hits,
)


def _make_base_result_for_qa(**overrides) -> dict:
    """Minimal customer-facing result suitable for QA Gate tests."""
    base = {
        "field_states": {
            "stato_occupativo": {"value": "OCCUPATO", "status": "FOUND", "headline_it": "Stato occupativo: OCCUPATO.", "evidence": []},
            "opponibilita_occupazione": {"value": "OPPONIBILE", "status": "FOUND", "headline_it": "Opponibilità occupazione: OPPONIBILE.", "evidence": []},
            "agibilita": {"value": "PRESENTE", "status": "FOUND", "headline_it": "Agibilità: PRESENTE.", "evidence": []},
            "regolarita_urbanistica": {"value": "PRESENTI DIFFORMITA", "status": "FOUND", "headline_it": "Regolarità urbanistica: PRESENTI DIFFORMITA.", "evidence": []},
        },
        "issues": [],
        "section_3_money_box": {
            "policy": "CONSERVATIVE",
            "items": [],
            "cost_signals_to_verify": [],
            "total_extra_costs": {"min": None, "max": None, "note": "Non quantificato."},
        },
        "money_box": {
            "policy": "CONSERVATIVE",
            "items": [],
            "cost_signals_to_verify": [],
            "total_extra_costs": {"min": None, "max": None},
        },
        "section_2_decisione_rapida": {"summary_it": "Sintesi corrente."},
        "summary_for_client": {"summary_it": "Sintesi corrente."},
        "summary_for_client_bundle": {"decision_summary_it": "Sintesi corrente."},
        "section_9_legal_killers": {"items": [], "top_items": [], "resolver_meta": {"themes": []}},
        "red_flags_operativi": [],
        "section_11_red_flags": [],
        "lots": [],
        "beni": [],
        "customer_decision_contract": {
            "field_states": {},
            "issues": [],
            "money_box": {"policy": "CONSERVATIVE", "items": [], "total_extra_costs": {"min": None, "max": None}},
            "section_9_legal_killers": {"items": [], "top_items": []},
            "red_flags_operativi": [],
            "decision_rapida_client": {"summary_it": "Sintesi."},
            "summary_for_client_bundle": {"decision_summary_it": "Sintesi."},
        },
    }
    base.update(overrides)
    return base


def _money_customer_label_values(item: dict) -> list[str]:
    values = []
    for key, value in item.items():
        if not isinstance(key, str) or not isinstance(value, str):
            continue
        if any(blocked in key.lower() for blocked in ("evidence", "quote", "source", "fonte")):
            continue
        if any(marker in key.lower() for marker in ("label", "title", "headline", "display")):
            values.append(value)
    return values


def _assert_money_labels_contain_only_amount(item: dict, expected_amount: str, *stale_amounts: str) -> None:
    values = _money_customer_label_values(item)
    assert values
    for value in values:
        assert expected_amount in value
        for stale_amount in stale_amounts:
            assert stale_amount not in value


def _assert_money_labels_have_no_amount(item: dict, *stale_amounts: str) -> None:
    values = _money_customer_label_values(item)
    assert values
    for value in values:
        for stale_amount in stale_amounts:
            assert stale_amount not in value
        assert "€" not in value


def test_upstream_money_extraction_prefers_explicit_euro_over_identifier_numbers():
    text = "Regolarizzazione urbanistica sub. 6, foglio 31, particella 433: € 3.000,00."
    assert _extract_amount_after_term(text, r"regolarizzazione\s+urbanistica") == 3000

    text = "Oneri di regolarizzazione urbanistica sub. 31: Euro 5.032,00."
    assert _extract_amount_after_term(text, r"oneri\s+di\s+regolarizzazione\s+urbanistica") == 5032

    text = "Regolarizzazione urbanistica: sub. 6, foglio 31, categoria A/2, pag. 16."
    assert _extract_amount_after_term(text, r"regolarizzazione\s+urbanistica") is None


def test_money_semantic_repair_gate_repairs_sub31_identifier_amount_and_all_display_labels():
    result = _make_base_result_for_qa(
        lots=[{"lot_number": 1, "titolo": "Lotto unico"}],
        lot_index=[{"lot": 1, "label": "Lotto unico"}],
        lots_count=1,
        is_multi_lot=False,
    )
    quote = "Oneri di regolarizzazione urbanistica relativi al sub. 31: €.5.032,00."
    bad_item = {
        "label_it": "Regolarizzazione: € 31",
        "label_en": "Regolarizzazione: € 31",
        "label": "Regolarizzazione: € 31",
        "title": "Regolarizzazione: € 31",
        "title_it": "Regolarizzazione: € 31",
        "title_en": "Regolarizzazione: € 31",
        "headline_it": "Regolarizzazione: € 31",
        "headline_en": "Regolarizzazione: € 31",
        "display_label": "Regolarizzazione: € 31",
        "customer_display_label": "Regolarizzazione: € 31",
        "classification": "cost_signal_to_verify",
        "amount_eur": 31,
        "stima_euro": None,
        "additive_to_extra_total": False,
        "evidence": [{"page": 3, "quote": quote}],
    }
    result["section_3_money_box"]["items"] = [copy.deepcopy(bad_item)]
    result["section_3_money_box"]["cost_signals_to_verify"] = [copy.deepcopy(bad_item)]
    result["money_box"]["items"] = [copy.deepcopy(bad_item)]
    result["money_box"]["cost_signals_to_verify"] = [copy.deepcopy(bad_item)]
    result["customer_decision_contract"]["money_box"]["items"] = [copy.deepcopy(bad_item)]
    result["customer_decision_contract"]["section_3_money_box"] = {"items": [copy.deepcopy(bad_item)]}

    qa_report = {"status": "PASS", "corrections_applied": [], "errors": []}
    apply_final_safety_invariants(
        result,
        qa_report,
        page_map={3: quote},
    )

    for container in (result, result["customer_decision_contract"]):
        for box_key in ("section_3_money_box", "money_box"):
            item = container[box_key]["items"][0]
            assert item["amount_eur"] == 5032
            assert item["amount_status"] == "ANCHORED_EXPLICIT_EURO"
            assert item["searched_pages"] == [3]
            assert item["evidence"][0]["quote"] == quote
            _assert_money_labels_contain_only_amount(item, "€ 5.032", "€ 31", "€31")

    assert qa_report["status"] == "FAIL_CORRECTED"


def test_money_semantic_repair_gate_repairs_sub6_identifier_amount_and_all_display_labels():
    result = _make_base_result_for_qa()
    quote = "Oneri di regolarizzazione urbanistica relativi al sub. 6 pari a € 3.000,00."
    bad_item = {
        "label_it": "Regolarizzazione: € 6",
        "label_en": "Regolarizzazione: € 6",
        "title": "Regolarizzazione: € 6",
        "headline_en": "Regolarizzazione: € 6",
        "display_label": "Regolarizzazione: € 6",
        "classification": "cost_signal_to_verify",
        "amount_eur": 6,
        "stima_euro": None,
        "additive_to_extra_total": False,
        "evidence": [{"page": 3, "quote": quote}],
    }
    result["section_3_money_box"]["items"] = [copy.deepcopy(bad_item)]
    result["section_3_money_box"]["qualitative_burdens"] = [copy.deepcopy(bad_item)]
    result["money_box"]["items"] = [copy.deepcopy(bad_item)]
    result["money_box"]["qualitative_burdens"] = [copy.deepcopy(bad_item)]

    apply_final_safety_invariants(
        result,
        {"status": "PASS", "corrections_applied": [], "errors": []},
        page_map={3: quote},
    )

    for container in (result, result["customer_decision_contract"]):
        for box_key in ("section_3_money_box", "money_box"):
            for list_key in ("items", "qualitative_burdens"):
                item = container[box_key][list_key][0]
                assert item["amount_eur"] == 3000
                assert item["amount_status"] == "ANCHORED_EXPLICIT_EURO"
                assert item["evidence"][0]["quote"] == quote
                _assert_money_labels_contain_only_amount(item, "€ 3.000", "€ 6", "€6")


def test_money_semantic_repair_gate_downgrades_identifier_without_defensible_euro_anchor():
    result = _make_base_result_for_qa()
    quote = "Regolarizzazione da verificare: foglio 31, particella 433, sub. 6, categoria C/6, pag. 16."
    bad_item = {
        "label_it": "Regolarizzazione urbanistica: € 6",
        "label_en": "Regolarizzazione urbanistica: € 6",
        "title": "Regolarizzazione urbanistica: € 6",
        "title_en": "Regolarizzazione urbanistica: € 6",
        "headline_it": "Regolarizzazione urbanistica: € 6",
        "headline_en": "Regolarizzazione urbanistica: € 6",
        "display_label": "Regolarizzazione urbanistica: € 6",
        "classification": "cost_signal_to_verify",
        "amount_eur": 6,
        "evidence": [{"page": 7, "quote": quote}],
    }
    result["section_3_money_box"]["items"] = [copy.deepcopy(bad_item)]
    result["section_3_money_box"]["cost_signals_to_verify"] = [copy.deepcopy(bad_item)]
    result["money_box"]["items"] = [copy.deepcopy(bad_item)]
    result["money_box"]["cost_signals_to_verify"] = [copy.deepcopy(bad_item)]

    apply_final_safety_invariants(
        result,
        {"status": "PASS", "corrections_applied": [], "errors": []},
        page_map={7: f"{quote} Data 01.05.2003."},
    )

    item = result["section_3_money_box"]["items"][0]
    assert item["amount_eur"] is None
    assert item["stima_euro"] is None
    assert item["amount_status"] == "NON_QUANTIFICATO_IN_MODO_DIFENDIBILE"
    assert item["searched_pages"] == [7]
    assert item["manual_check_hint_it"]
    assert item["evidence"][0]["page"] == 7
    assert item["evidence"][0]["quote"] == quote
    assert item["reason_it"]
    for container in (result, result["customer_decision_contract"]):
        for box_key in ("section_3_money_box", "money_box"):
            for list_key in ("items", "cost_signals_to_verify"):
                _assert_money_labels_have_no_amount(container[box_key][list_key][0], "€ 6", "€6")


def test_money_semantic_repair_gate_keeps_valid_explicit_euro_amount():
    result = _make_base_result_for_qa()
    valid_item = {
        "label_it": "Regolarizzazione urbanistica: € 5.032",
        "classification": "cost_signal_to_verify",
        "amount_eur": 5032,
        "evidence": [{"page": 4, "quote": "Regolarizzazione urbanistica Euro 5.032,00."}],
    }
    result["section_3_money_box"]["items"] = [copy.deepcopy(valid_item)]
    result["money_box"]["items"] = [copy.deepcopy(valid_item)]

    apply_final_safety_invariants(
        result,
        {"status": "PASS", "corrections_applied": [], "errors": []},
        page_map={4: "Regolarizzazione urbanistica Euro 5.032,00."},
    )

    item = result["section_3_money_box"]["items"][0]
    assert item["amount_eur"] == 5032
    assert item["amount_status"] == "ANCHORED_EXPLICIT_EURO"
    assert "manual_check_hint_it" not in item


def test_asset_inventory_repair_gate_rebuilds_multilot_apartment_and_garage_inventory():
    result = _make_base_result_for_qa(
        lots=[
            {
                "lot_number": 1,
                "lot": 1,
                "titolo": "Lotto Unico",
                "tipologia": "Garage",
                "beni": [{"bene_number": 1, "tipologia": "Garage"}],
            }
        ],
        beni=[{"bene_number": 1, "tipologia": "Garage"}],
        lot_index=[{"lot": 1, "label": "Lotto Unico", "tipologia": "Garage"}],
        lots_count=1,
        is_multi_lot=False,
        detail_scope="SINGLE_ASSET",
        case_header={"lotto": "Lotto Unico"},
        report_header={"lotto": {"value": "Lotto Unico"}},
    )

    page_map = {
        1: "LOTTO N. 1\nBene N° 1 - Appartamento in Via Roma, superficie mq 80, foglio 10 particella 20 sub. 6.",
        2: "LOTTO N. 2\nBene N° 1 - Garage in Via Roma, superficie mq 18, foglio 10 particella 20 sub. 31.",
    }
    apply_final_safety_invariants(result, {"status": "PASS", "corrections_applied": [], "errors": []}, page_map=page_map)

    assert result["lots_count"] == 2
    assert result["is_multi_lot"] is True
    assert result["detail_scope"] == "LOT_FIRST"
    assert "unico" not in str(result["case_header"]["lotto"]).lower()
    serialized_assets = json.dumps({"lots": result["lots"], "beni": result["beni"], "lot_index": result["lot_index"]}, ensure_ascii=False).lower()
    assert "appartamento" in serialized_assets
    assert "garage" in serialized_assets
    assert len(result["lot_index"]) == 2
    assert result["customer_decision_contract"]["lots_count"] == 2
    assert result["customer_decision_contract"]["is_multi_lot"] is True


def test_asset_inventory_repair_gate_keeps_single_lot_pertinenze_in_same_lot():
    result = _make_base_result_for_qa(
        lots=[{"lot_number": 1, "lot": 1, "titolo": "Lotto unico", "beni": [{"tipologia": "Appartamento"}]}],
        beni=[{"tipologia": "Appartamento"}],
        lot_index=[{"lot": 1, "label": "Lotto unico", "tipologia": "Appartamento"}],
        lots_count=1,
        is_multi_lot=False,
        detail_scope="SINGLE_ASSET",
    )
    page_map = {
        1: "LOTTO UNICO\nBene N° 1 - Appartamento con garage e cantina pertinenziale, superficie mq 90.",
    }

    apply_final_safety_invariants(result, {"status": "PASS", "corrections_applied": [], "errors": []}, page_map=page_map)

    assert result["lots_count"] == 1
    assert result["is_multi_lot"] is False
    assert result["detail_scope"] == "BENE_FIRST"
    serialized_assets = json.dumps({"lots": result["lots"], "beni": result["beni"]}, ensure_ascii=False).lower()
    assert "appartamento" in serialized_assets
    assert "garage" in serialized_assets
    assert "cantina" in serialized_assets


def test_asset_inventory_repair_gate_falls_back_when_lot_structure_is_ambiguous():
    result = _make_base_result_for_qa(lots=[], beni=[], lot_index=[], lots_count=0, is_multi_lot=False)
    page_map = {
        1: "LOTTO UNICO - descrizione iniziale del compendio.",
        2: "LOTTO N. 1 - Bene N° 1 Appartamento.",
        3: "LOTTO N. 2 - Bene N° 1 Box.",
    }

    apply_final_safety_invariants(result, {"status": "PASS", "corrections_applied": [], "errors": []}, page_map=page_map)

    fallback = result["asset_inventory_repair"]
    assert fallback["asset_inventory_status"] == "NON_RISOLTO_IN_MODO_DIFENDIBILE"
    assert fallback["searched_pages"] == [1, 2, 3]
    assert fallback["detected_candidates"]
    assert fallback["manual_check_hint_it"]


def test_qa_gate_metadata_preserves_semantic_repair_details():
    result = {}
    attach_qa_gate_metadata(
        result,
        {
            "status": "FAIL_CORRECTED",
            "semantic_repair_gates": {
                "changed": True,
                "asset_inventory": {"status": "REPAIRED_SINGLE_LOT_ASSETS"},
            },
        },
    )

    assert result["qa_gate"]["semantic_repair_gates"]["changed"] is True
    assert result["qa_gate"]["semantic_repair_gates"]["asset_inventory"]["status"] == "REPAIRED_SINGLE_LOT_ASSETS"


# Test 1: LLM detects fake buyer-side total
def test_qa_gate_llm_removes_fake_buyer_side_total():
    """When LLM returns REMOVE_EXACT_TOTAL, fake total must be stripped everywhere."""
    result = _make_base_result_for_qa()
    result["section_2_decisione_rapida"]["summary_it"] = (
        "Costi espliciti a carico dell'acquirente: € 528.123,68. Immobile occupato."
    )
    result["section_3_money_box"]["total_extra_costs"]["min"] = 528124
    result["section_3_money_box"]["total_extra_costs"]["max"] = 528124

    mocked_response = {
        "qa_status": "FAIL_CORRECTED",
        "overall_verdict_it": "Totale falso rilevato.",
        "context_used": {"mode": "EVIDENCE_ONLY", "pages_reviewed": [5], "limitations_it": ""},
        "contradictions_detected": [],
        "corrections": [
            {
                "id": "fake_total_1",
                "target": "money_box",
                "action": "REMOVE_EXACT_TOTAL",
                "safe_value_it": "Totale non quantificabile.",
                "reason_it": "Importo è VdM / deprezzamento, non costo buyer-side.",
                "evidence_pages": [5],
                "evidence_quotes": ["Valore commerciale determinato in euro 528.123,68"],
                "confidence": 0.95,
            }
        ],
        "section_verdicts": {},
    }

    with mock.patch("customer_contract_qa_gate.call_customer_qa_llm", return_value=mocked_response):
        qa_meta = apply_customer_contract_qa_gate(result, raw_text=None)

    assert qa_meta["status"] == "FAIL_CORRECTED"
    assert qa_meta["llm_used"] is True

    serialized = json.dumps({
        k: result[k] for k in (
            "section_2_decisione_rapida", "section_3_money_box", "money_box",
            "red_flags_operativi", "section_9_legal_killers"
        ) if k in result
    }, ensure_ascii=False)
    assert "528.123" not in serialized
    assert "528123" not in serialized
    assert "Costi espliciti a carico" not in serialized

    total = result["section_3_money_box"]["total_extra_costs"]
    assert total["min"] is None
    assert total["max"] is None

    assert "qa_gate" in result
    assert any(c.get("action") == "REMOVE_EXACT_TOTAL" for c in result["qa_gate"]["corrections_applied"])


# Test 2: LLM separates occupancy from opponibility
def test_qa_gate_llm_splits_occupancy_from_opponibility():
    """When occupancy is NON_VERIFICABILE but evidence says occupied, gate must correct it."""
    result = _make_base_result_for_qa()
    result["field_states"]["stato_occupativo"]["value"] = "NON_VERIFICABILE"
    result["field_states"]["stato_occupativo"]["status"] = "NOT_FOUND"

    mocked_response = {
        "qa_status": "FAIL_CORRECTED",
        "overall_verdict_it": "Occupazione confermata in perizia.",
        "context_used": {"mode": "EVIDENCE_ONLY", "pages_reviewed": [14], "limitations_it": ""},
        "contradictions_detected": [],
        "corrections": [
            {
                "id": "split_occ_1",
                "target": "occupancy",
                "action": "SPLIT_OCCUPANCY_OPPONIBILITY",
                "safe_value_it": "Stato occupativo: OCCUPATO. Opponibilità: DA VERIFICARE.",
                "reason_it": "Perizia dice occupato da terzi con contratto 4+4.",
                "evidence_pages": [14],
                "evidence_quotes": ["Stato di occupazione: Occupato da terzi con contratto di locazione"],
                "confidence": 0.9,
            }
        ],
        "section_verdicts": {},
    }

    with mock.patch("customer_contract_qa_gate.call_customer_qa_llm", return_value=mocked_response):
        apply_customer_contract_qa_gate(result, raw_text=None)

    occ = result["field_states"]["stato_occupativo"]
    assert occ["value"] == "OCCUPATO"
    assert occ["status"] == "FOUND"

    oppon = result["field_states"]["opponibilita_occupazione"]
    assert oppon["value"] in ("DA VERIFICARE", "NON_VERIFICABILE", "LOW_CONFIDENCE")
    assert oppon["value"] != "OPPONIBILE"


# Test 3: False libero guard — safety sweep rejects LIBERO from false markers
def test_qa_gate_safety_rejects_libero_from_false_marker():
    """Lot stato_occupativo=LIBERO derived from 'libero professionista' must be corrected."""
    result = _make_base_result_for_qa()
    result["lots"] = [
        {
            "lot_number": 1,
            "stato_occupativo": "LIBERO",
            "occupancy_status": "LIBERO",
            "evidence": {
                "occupancy_status": [
                    {"page": 5, "quote": "Il venditore è un libero professionista iscritto all'albo"}
                ]
            },
        }
    ]

    apply_final_safety_invariants(result)

    lot = result["lots"][0]
    assert lot["stato_occupativo"] != "LIBERO"
    assert lot["stato_occupativo"] == "DA VERIFICARE"


# Test 4: LLM downgrades agibilità overclaim
def test_qa_gate_llm_downgrades_agibilita_from_local_scope():
    """When evidence is only for a local part, agibilità must be downgraded to DA VERIFICARE."""
    result = _make_base_result_for_qa()
    result["field_states"]["agibilita"]["value"] = "ASSENTE"
    result["field_states"]["agibilita"]["status"] = "FOUND"

    mocked_response = {
        "qa_status": "FAIL_CORRECTED",
        "overall_verdict_it": "Agibilità ASSENTE sembra locale.",
        "context_used": {"mode": "EVIDENCE_ONLY", "pages_reviewed": [8], "limitations_it": ""},
        "contradictions_detected": [],
        "corrections": [
            {
                "id": "agib_down_1",
                "target": "agibilita",
                "action": "DOWNGRADE_TO_VERIFY",
                "safe_value_it": "Solo una parte/pertinenza risulta non agibile; certificato globale da verificare.",
                "reason_it": "Il terrapieno è non agibile, non l'unità principale.",
                "evidence_pages": [8],
                "evidence_quotes": ["il terrapieno risulta non accessibile e non agibile"],
                "confidence": 0.85,
            }
        ],
        "section_verdicts": {},
    }

    with mock.patch("customer_contract_qa_gate.call_customer_qa_llm", return_value=mocked_response):
        apply_customer_contract_qa_gate(result, raw_text=None)

    agib = result["field_states"]["agibilita"]
    assert agib["value"] == "DA VERIFICARE"


# Test 5: Global agibilità absence must remain ASSENTE
def test_qa_gate_preserves_global_agibilita_absence():
    """When evidence explicitly says global certificate not issued, ASSENTE must not be downgraded."""
    result = _make_base_result_for_qa()
    result["field_states"]["agibilita"]["value"] = "ASSENTE"

    mocked_response = {
        "qa_status": "PASS",
        "overall_verdict_it": "Agibilità ASSENTE confermata.",
        "context_used": {"mode": "EVIDENCE_ONLY", "pages_reviewed": [9], "limitations_it": ""},
        "contradictions_detected": [],
        "corrections": [],
        "section_verdicts": {},
    }

    with mock.patch("customer_contract_qa_gate.call_customer_qa_llm", return_value=mocked_response):
        apply_customer_contract_qa_gate(result, raw_text=None)

    # No correction → must remain ASSENTE
    assert result["field_states"]["agibilita"]["value"] == "ASSENTE"


# Test 6: LLM upgrades severe urbanistica
def test_qa_gate_llm_upgrades_severe_urbanistica():
    """When evidence says 'illegittima, non conforme e non autorizzata', severity must upgrade."""
    result = _make_base_result_for_qa()

    mocked_response = {
        "qa_status": "FAIL_CORRECTED",
        "overall_verdict_it": "Urbanistica grave.",
        "context_used": {"mode": "EVIDENCE_ONLY", "pages_reviewed": [3], "limitations_it": ""},
        "contradictions_detected": [],
        "corrections": [
            {
                "id": "urban_up_1",
                "target": "urbanistica",
                "action": "UPGRADE_SEVERITY",
                "safe_value_it": "Regolarità urbanistica: NON CONFORME / GRAVE.",
                "reason_it": "Perizia dice 'illegittima, non conforme e non autorizzata'.",
                "evidence_pages": [3],
                "evidence_quotes": ["porzione rilevata illegittima, non conforme e non autorizzata"],
                "confidence": 0.88,
            }
        ],
        "section_verdicts": {},
    }

    with mock.patch("customer_contract_qa_gate.call_customer_qa_llm", return_value=mocked_response):
        apply_customer_contract_qa_gate(result, raw_text=None)

    urb = result["field_states"]["regolarita_urbanistica"]
    assert urb["value"] == "NON CONFORME / GRAVE"


# Test 7: LLM backfills beni details
def test_qa_gate_llm_backfills_beni_details():
    """When beni is empty, BACKFILL_DETAILS must create beni entry with evidence-supported fields."""
    result = _make_base_result_for_qa()
    result["beni"] = []
    result["lot_index"] = [{"lot": 1, "ubicazione": None}]

    mocked_response = {
        "qa_status": "FAIL_CORRECTED",
        "overall_verdict_it": "Beni mancanti.",
        "context_used": {"mode": "EVIDENCE_ONLY", "pages_reviewed": [2], "limitations_it": ""},
        "contradictions_detected": [],
        "corrections": [
            {
                "id": "beni_fill_1",
                "target": "beni_details",
                "action": "BACKFILL_DETAILS",
                "safe_value_it": "Beni creati da evidenza.",
                "reason_it": "Beni assenti nel risultato.",
                "evidence_pages": [2],
                "evidence_quotes": ["Appartamento VIA GARIBALDI 10, MANTOVA"],
                "confidence": 0.8,
                "backfill_data": {
                    "tipologia": "Appartamento",
                    "address": "VIA GARIBALDI 10, MANTOVA",
                },
            }
        ],
        "section_verdicts": {},
    }

    with mock.patch("customer_contract_qa_gate.call_customer_qa_llm", return_value=mocked_response):
        apply_customer_contract_qa_gate(result, raw_text=None)

    assert len(result["beni"]) == 1
    assert result["beni"][0].get("tipologia") == "Appartamento"
    # lot_index address may also be filled
    li = result.get("lot_index") or []
    if li:
        assert li[0].get("ubicazione") == "VIA GARIBALDI 10, MANTOVA"


# Test 8: LLM unavailable — gate degrades gracefully
def test_qa_gate_llm_unavailable_does_not_crash():
    """When LLM call raises, qa_gate.status=WARN and analysis continues."""
    result = _make_base_result_for_qa()

    with mock.patch(
        "customer_contract_qa_gate.call_customer_qa_llm",
        side_effect=RuntimeError("timeout"),
    ):
        qa_meta = apply_customer_contract_qa_gate(result, raw_text=None)

    assert qa_meta["status"] == "WARN"
    assert qa_meta["llm_used"] is False
    assert any("LLM call failed" in e for e in qa_meta["errors"])
    assert "qa_gate" in result


# Test 9: Root / CDC consistency after correction
def test_qa_gate_corrections_synced_to_customer_decision_contract():
    """After LLM correction applied to root, CDC field_states must mirror via safety sweep."""
    result = _make_base_result_for_qa()
    # Root has DA VERIFICARE, CDC has ASSENTE — safety sweep must sync
    result["field_states"]["agibilita"]["value"] = "DA VERIFICARE"
    result["customer_decision_contract"]["field_states"]["agibilita"] = {
        "value": "ASSENTE", "status": "FOUND"
    }

    apply_final_safety_invariants(result)

    cdc_agib = result["customer_decision_contract"]["field_states"].get("agibilita", {})
    assert cdc_agib.get("value") == "DA VERIFICARE"


# Test 10: Validate QA response rejects unknown schema gracefully
def test_validate_customer_qa_response_normalizes_bad_status():
    resp = {"qa_status": "GARBAGE_VALUE", "corrections": [], "contradictions_detected": []}
    validated = validate_customer_qa_response(resp)
    assert validated["qa_status"] == "WARN"


# Test 11: Safety sweep removes fake total phrase that survived corrections
def test_safety_sweep_removes_surviving_fake_total_phrase():
    result = _make_base_result_for_qa()
    result["section_2_decisione_rapida"]["summary_it"] = (
        "Occupato. Costi espliciti a carico dell'acquirente: € 6.677,09."
    )
    result["section_3_money_box"]["total_extra_costs"]["min"] = 6677
    result["section_3_money_box"]["total_extra_costs"]["max"] = 6677

    apply_final_safety_invariants(result)

    summary = result["section_2_decisione_rapida"]["summary_it"]
    assert "6.677" not in summary
    assert "6677" not in summary
    assert "Costi espliciti a carico" not in summary

    total = result["section_3_money_box"]["total_extra_costs"]
    # Safety sweep CONSERVATIVE invariant clears numeric totals
    assert total["min"] is None
    assert total["max"] is None


# ── Stage 11.2: Smart page pack builder ──────────────────────────────────────

def _make_long_raw_text(urbanistica_page: int = 18, beni_page: int = 3, total_pages: int = 25) -> str:
    """Build a synthetic perizia raw_text with pages separated by form feed.

    urbanistica_page contains 'illegittima, non conforme e non autorizzata'.
    beni_page contains Compendio A / Fg. / mapp. / superficie.
    All other pages are filler text.
    """
    pages = []
    for i in range(1, total_pages + 1):
        if i == urbanistica_page:
            content = (
                f"PAGINA {i}\n"
                "La porzione di immobile rilevata è illegittima, non conforme e non autorizzata.\n"
                "Si rimanda a sanatoria/condono da verificare.\n"
                "Fiscalizzazione o ripristino obbligatorio secondo normativa vigente.\n"
            )
        elif i == beni_page:
            content = (
                f"PAGINA {i}\n"
                "Compendio A — Appartamento.\n"
                "Via Nuova 19, località Carozzo, Vezzano Ligure (SP).\n"
                "Fg. 12, mapp. 237, sub. 7\n"
                "Categoria A/2, consistenza 6 vani, superficie 172 mq, 8 mq scoperti.\n"
            )
        else:
            content = (
                f"PAGINA {i}\n"
                "Testo di riempimento della pagina senza parole chiave rilevanti.\n" * 5
            )
        pages.append(content)
    return "\f".join(pages)


def test_page_pack_includes_urbanistica_severe_page_beyond_40k():
    """build_page_text_pack must include the page with severe urbanistica terms even if it
    falls after what the old 40k-char naive truncation would have included."""
    # Place severe urbanistica terms on page 18, filler on earlier pages.
    raw_text = _make_long_raw_text(urbanistica_page=18, total_pages=25)
    page_map = _normalize_raw_text_to_page_map(raw_text)

    # Verify page 18 actually has the severe term
    assert "illegittima" in page_map[18].lower()

    # Simulate a budget that would cut off at page ~13 (old 40k limit)
    # We test the tier-based selector, not the budget, by calling build_page_text_pack directly.
    result = _make_base_result_for_qa()  # no evidence pages for urbanistica

    pack = build_page_text_pack(page_map, result, internal_runtime=None)

    assert 18 in pack["pages_reviewed"], (
        f"Page 18 (urbanistica severe terms) must be in page pack; got {pack['pages_reviewed']}"
    )
    reasons_18 = pack.get("selected_by_reason", {}).get(18, [])
    assert any("keyword_urbanistica" in r for r in reasons_18), (
        f"Page 18 reason should include keyword_urbanistica; got {reasons_18}"
    )
    assert "illegittima" in pack["full_text"].lower()


def test_page_pack_includes_beni_detail_page():
    """build_page_text_pack must include pages with Compendio/Fg/mapp/superficie regardless
    of where they fall in the document."""
    raw_text = _make_long_raw_text(beni_page=22, total_pages=25)
    page_map = _normalize_raw_text_to_page_map(raw_text)

    assert "fg." in page_map[22].lower() or "172 mq" in page_map[22].lower()

    result = _make_base_result_for_qa()
    pack = build_page_text_pack(page_map, result, internal_runtime=None)

    assert 22 in pack["pages_reviewed"], (
        f"Page 22 (beni detail terms) must be in page pack; got {pack['pages_reviewed']}"
    )
    reasons_22 = pack.get("selected_by_reason", {}).get(22, [])
    assert any("keyword_beni_details" in r for r in reasons_22)


def test_context_debug_lists_selected_pages_and_reasons():
    """build_customer_qa_context must return context_debug with detected_page_count,
    selected_pages, selected_pages_by_reason, and context_char_count."""
    raw_text = _make_long_raw_text(total_pages=10)
    result = _make_base_result_for_qa()

    context = build_customer_qa_context(result, raw_text=raw_text)

    debug = context.get("debug", {})
    assert "detected_page_count" in debug, "debug must include detected_page_count"
    assert debug["detected_page_count"] == 10
    assert "selected_pages" in debug
    assert isinstance(debug["selected_pages"], list)
    assert "context_char_count" in debug
    assert debug["context_char_count"] > 0


# ── Stage 11.3: Claims-to-challenge ──────────────────────────────────────────

def test_claims_to_challenge_flags_weak_urbanistica_with_severe_evidence():
    """_build_claims_to_challenge must flag urbanistica PRESENTI DIFFORMITA when evidence
    contains severe terms like illegittima/non conforme/non autorizzata."""
    result = _make_base_result_for_qa()
    result["field_states"]["regolarita_urbanistica"]["evidence"] = [
        {"page": 15, "quote": "La porzione è illegittima, non conforme e non autorizzata."}
    ]

    claims = _build_claims_to_challenge(result)

    urb_claims = [c for c in claims if c.get("field") == "regolarita_urbanistica"]
    assert urb_claims, "Must have a claim for regolarita_urbanistica"
    assert urb_claims[0].get("contradiction_flag") == "EVIDENCE_SUGGESTS_SEVERE_URBANISTICA", (
        f"Expected EVIDENCE_SUGGESTS_SEVERE_URBANISTICA, got {urb_claims[0].get('contradiction_flag')}"
    )


def test_claims_to_challenge_flags_missing_beni():
    """_build_claims_to_challenge must flag BENI_DETAILS_MISSING when beni is empty."""
    result = _make_base_result_for_qa()
    result["beni"] = []

    claims = _build_claims_to_challenge(result)

    beni_claims = [c for c in claims if c.get("field") == "beni"]
    assert beni_claims, "Must have a claim for missing beni"
    assert beni_claims[0].get("contradiction_flag") == "BENI_DETAILS_MISSING"


# ── Stage 11.4: INV-5 and INV-6 deterministic backstops ──────────────────────

def test_inv5_upgrades_severe_urbanistica_from_evidence_quotes():
    """INV-5: When field_states evidence contains severe terms, safety sweep must upgrade
    urbanistica from PRESENTI DIFFORMITA to NON CONFORME / GRAVE — even if LLM returns nothing."""
    result = _make_base_result_for_qa()
    result["field_states"]["regolarita_urbanistica"]["value"] = "PRESENTI DIFFORMITA"
    result["field_states"]["regolarita_urbanistica"]["evidence"] = [
        {"page": 15, "quote": "La porzione è illegittima, non conforme e non autorizzata."},
    ]

    # LLM returns PASS with no corrections — deterministic backstop must still fire
    mocked_response = {
        "qa_status": "PASS",
        "overall_verdict_it": "Tutto ok.",
        "context_used": {"mode": "EVIDENCE_ONLY", "pages_reviewed": [], "limitations_it": ""},
        "contradictions_detected": [],
        "corrections": [],
        "section_verdicts": {},
    }

    with mock.patch("customer_contract_qa_gate.call_customer_qa_llm", return_value=mocked_response):
        qa_meta = apply_customer_contract_qa_gate(result, raw_text=None)

    urb = result["field_states"]["regolarita_urbanistica"]
    assert urb["value"] == "NON CONFORME / GRAVE", (
        f"INV-5 must upgrade urbanistica; got {urb['value']!r}"
    )
    inv5_applied = any(
        c.get("id") == "INV-5" for c in qa_meta.get("corrections_applied", [])
    )
    assert inv5_applied, "INV-5 correction must appear in corrections_applied"


def test_inv6_backfills_beni_from_raw_text_when_beni_empty():
    """INV-6: When beni is empty and raw_text contains Compendio/Fg/mapp/superficie,
    safety sweep must create a beni entry — even if LLM returns nothing."""
    raw_text_with_beni = (
        "PAGINA 1\n"
        "Compendio A — Appartamento.\n"
        "Via Nuova 19, località Carozzo, Vezzano Ligure.\n"
        "Fg. 12, mapp. 237, sub. 7\n"
        "Categoria A/2, superficie 172 mq, 8 mq scoperti.\n"
    )

    result = _make_base_result_for_qa()
    result["beni"] = []

    # LLM returns PASS with no BACKFILL_DETAILS — INV-6 must still fire
    mocked_response = {
        "qa_status": "PASS",
        "overall_verdict_it": "Beni ok.",
        "context_used": {"mode": "FULL_DOCUMENT", "pages_reviewed": [1], "limitations_it": ""},
        "contradictions_detected": [],
        "corrections": [],
        "section_verdicts": {},
    }

    with mock.patch("customer_contract_qa_gate.call_customer_qa_llm", return_value=mocked_response):
        qa_meta = apply_customer_contract_qa_gate(result, raw_text=raw_text_with_beni)

    beni = result.get("beni") or []
    assert len(beni) >= 1, "INV-6 must create at least one beni entry"
    bene = beni[0]
    assert bene.get("catasto") or bene.get("superficie_mq") or bene.get("address"), (
        f"INV-6 beni entry must have at least catasto/superficie/address; got {bene}"
    )
    inv6_applied = any(
        c.get("id") == "INV-6" for c in qa_meta.get("corrections_applied", [])
    )
    assert inv6_applied, "INV-6 correction must appear in corrections_applied"


# ── Stage 12: lot_index / lots placeholder overwrite ─────────────────────────

def test_backfill_details_overwrites_placeholder_lot_index_location():
    """BACKFILL_DETAILS must overwrite lot_index[0].ubicazione when it holds a placeholder."""
    result = _make_base_result_for_qa()
    result["beni"] = []
    result["lots"] = []
    result["lot_index"] = [{"lot": 1, "ubicazione": "Indirizzo da verificare"}]

    _apply_backfill_details(
        result,
        backfill={"address": "Via Roma 10, Milano", "tipologia": "Appartamento"},
        evidence_pages=[2],
        evidence_quotes=["Via Roma 10"],
    )

    assert result["lot_index"][0]["ubicazione"] == "Via Roma 10, Milano", (
        f"Placeholder must be overwritten; got {result['lot_index'][0]['ubicazione']!r}"
    )


def test_backfill_details_does_not_overwrite_real_lot_index_location():
    """BACKFILL_DETAILS must NOT overwrite lot_index[0].ubicazione when it holds a real address."""
    result = _make_base_result_for_qa()
    result["beni"] = []
    result["lots"] = []
    result["lot_index"] = [{"lot": 1, "ubicazione": "Via Garibaldi 5, Torino"}]

    _apply_backfill_details(
        result,
        backfill={"address": "Via Roma 10, Milano"},
        evidence_pages=[2],
        evidence_quotes=["Via Roma 10"],
    )

    assert result["lot_index"][0]["ubicazione"] == "Via Garibaldi 5, Torino", (
        "Real address must not be overwritten by backfill"
    )


def test_backfill_details_updates_lots_location_when_placeholder():
    """BACKFILL_DETAILS must overwrite lots[*].ubicazione when current value is placeholder/None."""
    result = _make_base_result_for_qa()
    result["beni"] = []
    result["lots"] = [
        {"lot_number": 1, "ubicazione": None},
        {"lot_number": 2, "ubicazione": "Da verificare"},
        {"lot_number": 3, "ubicazione": "Via Esistente 7, Roma"},
    ]
    result["lot_index"] = []

    _apply_backfill_details(
        result,
        backfill={"address": "Via Roma 10, Milano"},
        evidence_pages=[3],
        evidence_quotes=["Via Roma 10"],
    )

    assert result["lots"][0]["ubicazione"] == "Via Roma 10, Milano", "None must be overwritten"
    assert result["lots"][1]["ubicazione"] == "Via Roma 10, Milano", "'Da verificare' must be overwritten"
    assert result["lots"][2]["ubicazione"] == "Via Esistente 7, Roma", "Real address must be preserved"


def test_is_placeholder_location_identifies_all_known_placeholders():
    assert _is_placeholder_location(None)
    assert _is_placeholder_location("")
    assert _is_placeholder_location("Indirizzo da verificare")
    assert _is_placeholder_location("Da verificare")
    assert _is_placeholder_location("Non disponibile")
    assert _is_placeholder_location("N/D")
    assert _is_placeholder_location("ND")
    assert not _is_placeholder_location("Via Roma 10, Milano")
    assert not _is_placeholder_location("VIA GARIBALDI 5")


# ── Stage 13: Mongo key sanitization (BSON safety) ───────────────────────────

def _all_dict_keys_are_strings(obj: Any) -> bool:
    """Recursively verify every dict key in obj is a string."""
    if isinstance(obj, dict):
        return all(isinstance(k, str) and _all_dict_keys_are_strings(v) for k, v in obj.items())
    if isinstance(obj, list):
        return all(_all_dict_keys_are_strings(item) for item in obj)
    return True


def test_qa_gate_metadata_is_mongo_safe():
    """After attach_qa_gate_metadata, result['qa_gate'] must have only string dict keys
    and BSON.encode must succeed (int page keys in context_debug must be stringified)."""
    from bson import BSON

    qa_report = {
        "status": "PASS",
        "llm_used": False,
        "model": "",
        "context_mode": "PAGE_PACK",
        "pages_reviewed": [1, 15],
        "corrections_applied": [],
        "contradictions_detected": [],
        "invariants_checked": [],
        "section_verdicts": {},
        "errors": [],
        "context_debug": {
            "detected_page_count": 20,
            "selected_pages": [1, 15],
            "selected_pages_by_reason": {1: ["x"], 15: ["keyword_urbanistica"]},
            "context_char_count": 5000,
            "mode": "PAGE_PACK",
        },
    }
    result: dict = {}
    attach_qa_gate_metadata(result, qa_report)

    qa_gate = result["qa_gate"]
    assert _all_dict_keys_are_strings(qa_gate), (
        "All dict keys in result['qa_gate'] must be strings for Mongo compatibility"
    )

    by_reason = qa_gate["context_debug"]["selected_pages_by_reason"]
    assert "1" in by_reason, "Integer page key 1 must become string '1'"
    assert "15" in by_reason, "Integer page key 15 must become string '15'"
    assert 1 not in by_reason, "Original integer key 1 must not remain"

    BSON.encode({"qa_gate": qa_gate})


def test_qa_gate_full_result_is_bson_safe_after_gate():
    """After apply_customer_contract_qa_gate runs, BSON.encode on the full result must not raise
    even when context_debug contains integer page keys in selected_pages_by_reason."""
    from bson import BSON

    raw_text = _make_long_raw_text(total_pages=5)
    result = _make_base_result_for_qa()

    mocked_response = {
        "qa_status": "PASS",
        "overall_verdict_it": "Tutto ok.",
        "context_used": {"mode": "FULL_DOCUMENT", "pages_reviewed": [1, 2, 3, 4, 5], "limitations_it": ""},
        "contradictions_detected": [],
        "corrections": [],
        "section_verdicts": {},
    }

    with mock.patch("customer_contract_qa_gate.call_customer_qa_llm", return_value=mocked_response):
        apply_customer_contract_qa_gate(result, raw_text=raw_text)

    assert "qa_gate" in result
    assert _all_dict_keys_are_strings(result["qa_gate"]), (
        "All dict keys in result['qa_gate'] must be strings"
    )
    BSON.encode({"result": result})


# ── Stage 14: Customer-facing consistency sweep ───────────────────────────────

def _make_stale_528_result() -> dict:
    """Result with €528.123,68 fake total scattered across customer-facing and CDC sections."""
    result = _make_base_result_for_qa()
    fake_label = "Costi espliciti a carico dell'acquirente: € 528.123,68."
    result["semaforo_generale"] = {
        "colore": "ROSSO",
        "reason_it": fake_label,
        "top_blockers": [
            {"label_it": "Immobile occupato.", "severity": "HIGH"},
            {"label_it": fake_label, "severity": "HIGH"},
        ],
    }
    result["section_1_semaforo_generale"] = result["semaforo_generale"].copy()
    result["issues"].append({
        "headline_it": "Costi espliciti.",
        "explanation_it": "La perizia indica costi espliciti a carico dell'acquirente.",
        "severity": "AMBER",
    })
    result["customer_decision_contract"]["semaforo_generale"] = result["semaforo_generale"].copy()
    result["customer_decision_contract"]["issues"] = result["issues"].copy()
    return result


def test_remove_exact_total_purges_customer_decision_contract_semaforo():
    """REMOVE_EXACT_TOTAL must clear fake 528 total from CDC semaforo and issues."""
    result = _make_stale_528_result()
    result["section_3_money_box"]["total_extra_costs"]["min"] = 528124
    result["section_3_money_box"]["total_extra_costs"]["max"] = 528124

    mocked_response = {
        "qa_status": "FAIL_CORRECTED",
        "overall_verdict_it": "Totale falso rilevato.",
        "context_used": {"mode": "EVIDENCE_ONLY", "pages_reviewed": [5], "limitations_it": ""},
        "contradictions_detected": [
            {
                "id": "fake_528_contradiction",
                "severity": "CRITICAL",
                "problem_it": "Totale buyer-side falso: è VdM, non costo.",
                "current_wrong_claim": "€ 528.123,68",
                "evidence_pages": [5],
                "evidence_quotes": ["Valore commerciale determinato in euro 528.123,68"],
                "recommended_action": "REMOVE_EXACT_TOTAL",
            }
        ],
        "corrections": [
            {
                "id": "fake_total_528",
                "target": "money_box",
                "action": "REMOVE_EXACT_TOTAL",
                "safe_value_it": "Totale non quantificabile.",
                "reason_it": "Importo è VdM.",
                "evidence_pages": [5],
                "evidence_quotes": ["Valore commerciale determinato in euro 528.123,68"],
                "confidence": 0.95,
            }
        ],
        "section_verdicts": {},
    }

    with mock.patch("customer_contract_qa_gate.call_customer_qa_llm", return_value=mocked_response):
        qa_meta = apply_customer_contract_qa_gate(result, raw_text=None)

    assert qa_meta["status"] == "FAIL_CORRECTED"

    # qa_gate metadata is ALLOWED to keep the wrong claim
    assert any(
        "528" in json.dumps(c, ensure_ascii=False)
        for c in result.get("qa_gate", {}).get("contradictions_detected", [])
    ), "qa_gate contradictions_detected must preserve the original wrong claim"

    # Customer-facing sections must have no fake 528
    hits = _collect_customer_facing_bad_text_hits(result)
    fake_hits = [h for h in hits if h["pattern"] == "fake_528"]
    assert fake_hits == [], f"Fake 528 must not appear outside qa_gate: {fake_hits}"

    costi_hits = [h for h in hits if h["pattern"] == "costi_espliciti"]
    assert costi_hits == [], f"costi_espliciti must not appear outside qa_gate: {costi_hits}"

    total = result["section_3_money_box"]["total_extra_costs"]
    assert total["min"] is None
    assert total["max"] is None


def test_money_box_buyer_side_labels_are_relabelled_after_remove_exact_total():
    """Money_box items with buyer-side explicit label must be relabelled as NON_ADDITIVE_SIGNAL."""
    result = _make_base_result_for_qa()
    stale_item = {
        "code": "VR_COST_99",
        "label_it": "Costo buyer-side esplicito da perizia",
        "label_en": "Costo buyer-side esplicito da perizia",
        "stima_nota": "Costo buyer-side esplicito rilevato nella perizia.",
        "stima_euro": None,
        "type": "BUYER_SIDE_COST",
        "classification": "explicit_buyer_cost",
        "additive_to_extra_total": True,
    }
    result["section_3_money_box"]["items"] = [stale_item.copy()]
    result["money_box"]["items"] = [stale_item.copy()]
    result["customer_decision_contract"]["money_box"]["items"] = [stale_item.copy()]

    apply_customer_facing_consistency_sweep(result)

    hits = _collect_customer_facing_bad_text_hits(result)
    buyer_hits = [h for h in hits if h["pattern"] == "buyer_side_label"]
    assert buyer_hits == [], f"buyer_side_label must not appear after sweep: {buyer_hits}"

    for mb_key in ("section_3_money_box", "money_box"):
        items = result[mb_key]["items"]
        assert items, f"{mb_key}.items must not be empty"
        item = items[0]
        assert item["type"] == "NON_ADDITIVE_SIGNAL", f"{mb_key} item type must be NON_ADDITIVE_SIGNAL"
        assert item["classification"] == "cost_signal_to_verify"
        assert item["additive_to_extra_total"] is False
        assert "buyer-side esplicito" not in item["label_it"].lower()


def test_agibilita_downgrade_purges_summaries_redflags_legal_killers():
    """When agibilita=DA VERIFICARE, all ASSENTE claims must be replaced in customer-facing sections."""
    result = _make_base_result_for_qa()
    result["field_states"]["agibilita"]["value"] = "DA VERIFICARE"
    result["field_states"]["agibilita"]["status"] = "LOW_CONFIDENCE"

    stale_text = "Agibilità: ASSENTE — certificato non rilasciato."
    result["summary_for_client"]["summary_it"] = stale_text
    result["section_2_decisione_rapida"]["summary_it"] = "Agibilità assente / non rilasciata."
    result["red_flags_operativi"] = [
        {"flag_it": "Agibilità: ASSENTE", "severity": "RED"}
    ]
    result["section_9_legal_killers"]["resolver_meta"]["themes"] = [
        {"theme": "agibilita", "note": "Agibilità assente — non rilasciata."}
    ]
    result["semaforo_generale"] = {
        "colore": "ROSSO",
        "reason_it": "Agibilità: ASSENTE.",
        "top_blockers": [{"label_it": "Agibilità assente / non rilasciata.", "severity": "HIGH"}],
    }

    apply_customer_facing_consistency_sweep(result)

    hits = _collect_customer_facing_bad_text_hits(result)
    agib_hits = [h for h in hits if h["pattern"] == "agibilita_assente_after_downgrade"]
    assert agib_hits == [], f"No ASSENTE agibilità must survive after downgrade: {agib_hits}"

    full_text = json.dumps({
        k: result[k] for k in (
            "summary_for_client", "section_2_decisione_rapida",
            "red_flags_operativi", "semaforo_generale", "section_9_legal_killers",
        )
    }, ensure_ascii=False)
    assert "DA VERIFICARE" in full_text, "DA VERIFICARE replacement must be present"


def test_agibilita_downgrade_purges_explanation_and_action_text():
    """Downgraded agibilita must purge stale explanation/action absence claims outside qa_gate."""
    result = _make_base_result_for_qa()
    result["field_states"]["agibilita"].update({
        "value": "DA VERIFICARE",
        "status": "LOW_CONFIDENCE",
        "explanation": "L'agibilità risulta assente nel caso canonico verificato.",
        "explanation_it": (
            "L'agibilità risulta assente o non rilasciata e richiede verifica immediata "
            "prima dell'offerta."
        ),
    })
    result["issues"] = [
        {
            "headline_it": "Agibilità assente.",
            "explanation_it": "L'agibilità risulta assente nel caso canonico verificato.",
            "action_it": "L'agibilità risulta assente nel caso canonico verificato.",
            "severity": "RED",
        }
    ]
    result["red_flags_operativi"] = [
        {
            "flag_it": "Agibilità: ASSENTE",
            "flag_en": "Agibilità: ASSENTE",
            "action_it": "L'agibilità risulta assente nel caso canonico verificato.",
            "severity": "RED",
        }
    ]
    result["section_11_red_flags"] = [
        {
            "flag_it": "Agibilità assente o non rilasciata.",
            "flag_en": "Agibilità: ASSENTE",
            "action_it": "L'agibilità risulta assente nel caso canonico verificato.",
            "severity": "RED",
        }
    ]
    result["summary_for_client_bundle"].update({
        "top_issue_it": "Agibilità assente o non rilasciata.",
        "main_risk_it": "L'agibilità risulta assente nel caso canonico verificato.",
        "caution_points_it": ["Agibilità: ASSENTE"],
        "checks_it": ["Verificare agibilità non rilasciata prima dell'offerta."],
        "decision_summary_it": "L'agibilità risulta assente nel caso canonico verificato.",
    })
    result["semaforo_generale"] = {
        "colore": "ROSSO",
        "reason_it": "L'agibilità risulta assente nel caso canonico verificato.",
        "top_blockers": [{"label_it": "Agibilità: ASSENTE.", "severity": "HIGH"}],
    }
    result["section_9_legal_killers"]["resolver_meta"]["themes"] = [
        {"theme": "agibilita", "note": "Agibilità assente — non rilasciata."}
    ]
    result["customer_decision_contract"].update({
        "field_states": {"agibilita": result["field_states"]["agibilita"].copy()},
        "issues": [issue.copy() for issue in result["issues"]],
        "red_flags_operativi": [flag.copy() for flag in result["red_flags_operativi"]],
        "section_11_red_flags": [flag.copy() for flag in result["section_11_red_flags"]],
        "summary_for_client_bundle": result["summary_for_client_bundle"].copy(),
    })
    result["qa_gate"] = {
        "contradictions_detected": [
            {"current_wrong_claim": "L'agibilità risulta assente nel caso canonico verificato."}
        ]
    }

    apply_customer_facing_consistency_sweep(result)

    hits = _collect_customer_facing_bad_text_hits(result)
    agib_hits = [h for h in hits if h["pattern"] == "agibilita_assente_after_downgrade"]
    assert agib_hits == [], f"No stale agibilita absence claim must survive outside qa_gate: {agib_hits}"

    customer_text = json.dumps({
        "issues": result["issues"],
        "field_states": result["field_states"],
        "red_flags_operativi": result["red_flags_operativi"],
        "section_11_red_flags": result["section_11_red_flags"],
        "summary_for_client_bundle": result["summary_for_client_bundle"],
        "semaforo_generale": result["semaforo_generale"],
        "section_9_legal_killers": result["section_9_legal_killers"],
        "customer_decision_contract": result["customer_decision_contract"],
    }, ensure_ascii=False)
    assert "Agibilità/abitabilità: DA VERIFICARE" in customer_text
    assert "questo non prova da solo l'assenza globale" in customer_text
    assert "L'agibilità risulta assente" in json.dumps(result["qa_gate"], ensure_ascii=False)


def test_occupancy_correction_propagates_to_lots():
    """When field_states.stato_occupativo=OCCUPATO, lots must be updated to OCCUPATO."""
    result = _make_base_result_for_qa()
    result["field_states"]["stato_occupativo"]["value"] = "OCCUPATO"
    result["lots"] = [
        {"lot_number": 1, "stato_occupativo": "DA VERIFICARE", "occupancy_status": "DA VERIFICARE"},
        {"lot_number": 2, "stato_occupativo": "OCCUPATO", "occupancy_status": "OCCUPATO"},
    ]
    result["customer_decision_contract"]["lots"] = [
        {"lot_number": 1, "stato_occupativo": "DA VERIFICARE", "occupancy_status": "DA VERIFICARE"},
    ]

    apply_customer_facing_consistency_sweep(result)

    assert result["lots"][0]["stato_occupativo"] == "OCCUPATO", "DA VERIFICARE lot must become OCCUPATO"
    assert result["lots"][0]["occupancy_status"] == "OCCUPATO"
    assert result["lots"][1]["stato_occupativo"] == "OCCUPATO", "Already OCCUPATO lot must stay OCCUPATO"

    # CDC lots must also be synced (Rule 5 sync)
    cdc_lots = result["customer_decision_contract"].get("lots") or []
    assert cdc_lots, "CDC lots must be synced"
    assert cdc_lots[0]["stato_occupativo"] == "OCCUPATO", "CDC lot must be OCCUPATO after sync"

    hits = _collect_customer_facing_bad_text_hits(result)
    occ_hits = [h for h in hits if h["pattern"] == "stato_non_verificabile_after_occupied"]
    assert occ_hits == [], f"No NON_VERIFICABILE occupancy must survive: {occ_hits}"


def test_customer_decision_contract_mirrors_corrected_root_sections():
    """After consistency sweep, CDC must mirror root money_box, summaries, field_states, lots, beni."""
    result = _make_base_result_for_qa()
    result["section_3_money_box"]["items"] = [
        {"code": "SIG_001", "label_it": "Segnale economico da verificare", "stima_euro": None}
    ]
    result["money_box"]["items"] = [
        {"code": "SIG_001", "label_it": "Segnale economico da verificare", "stima_euro": None}
    ]
    result["lots"] = [{"lot_number": 1, "stato_occupativo": "OCCUPATO", "ubicazione": "Via Roma 1"}]
    result["beni"] = [{"bene_label": "Lotto A", "address": "Via Roma 1"}]
    result["summary_for_client"]["summary_it"] = "Immobile occupato da verificare."
    result["red_flags_operativi"] = [{"flag_it": "Urbanistica da verificare.", "severity": "AMBER"}]

    # CDC starts stale / empty
    result["customer_decision_contract"]["money_box"]["items"] = []
    result["customer_decision_contract"]["lots"] = []
    result["customer_decision_contract"]["beni"] = []

    apply_customer_facing_consistency_sweep(result)

    cdc = result["customer_decision_contract"]

    # money_box synced
    cdc_mb_items = cdc.get("money_box", {}).get("items") or []
    assert len(cdc_mb_items) == 1, "CDC money_box.items must be synced from root"
    assert cdc_mb_items[0]["label_it"] == "Segnale economico da verificare"

    # lots synced
    cdc_lots = cdc.get("lots") or []
    assert cdc_lots, "CDC lots must be synced"
    assert cdc_lots[0]["stato_occupativo"] == "OCCUPATO"

    # beni synced
    cdc_beni = cdc.get("beni") or []
    assert cdc_beni, "CDC beni must be synced"
    assert cdc_beni[0]["address"] == "Via Roma 1"

    # summary synced
    cdc_s2 = cdc.get("decision_rapida_client") or {}
    assert "Immobile occupato" in cdc_s2.get("summary_it", ""), "CDC decision_rapida_client must mirror root summary"

    # field_states synced
    cdc_fs = cdc.get("field_states") or {}
    assert cdc_fs.get("stato_occupativo", {}).get("value") == "OCCUPATO"


def test_occupied_field_state_purges_stale_unresolved_text():
    result = _make_base_result_for_qa()
    result["field_states"]["stato_occupativo"].update({
        "value": "OCCUPATO",
        "headline_it": "Stato occupativo: DA VERIFICARE.",
        "explanation_it": "Lo stato di occupazione resta irrisolto.",
        "why_not_resolved": "Il campo resta aperto.",
        "action_it": "Il campo resta aperto perché la perizia non attribuisce un dato finale.",
    })
    original_opponibilita = {
        "value": "NON VERIFICABILE",
        "status": "LOW_CONFIDENCE",
        "headline_it": "Opponibilità occupazione: NON VERIFICABILE.",
        "explanation_it": "Titolo e opponibilità devono essere verificati separatamente.",
    }
    result["field_states"]["opponibilita_occupazione"] = original_opponibilita.copy()
    result["issues"] = [
        {
            "headline_it": "Stato occupativo: OCCUPATO.",
            "explanation_it": "Lo stato di occupazione resta irrisolto.",
            "action_it": "Il campo resta aperto prima dell'offerta.",
            "severity": "RED",
        }
    ]
    result["red_flags_operativi"] = [
        {
            "flag_it": "Stato occupativo: OCCUPATO.",
            "action_it": "La perizia non attribuisce un dato finale sullo stato di occupazione.",
            "severity": "RED",
        }
    ]

    apply_customer_facing_consistency_sweep(result)

    occ = result["field_states"]["stato_occupativo"]
    assert occ["headline_it"] == "Stato occupativo: OCCUPATO."
    assert occ["explanation_it"] == (
        "La perizia indica che l'immobile risulta occupato. "
        "L'opponibilità del titolo deve essere verificata separatamente."
    )
    assert occ["why_not_resolved"] is None
    assert result["field_states"]["opponibilita_occupazione"] == original_opponibilita

    customer_text = json.dumps({
        "field_states": result["field_states"],
        "issues": result["issues"],
        "red_flags_operativi": result["red_flags_operativi"],
        "customer_decision_contract": result["customer_decision_contract"],
    }, ensure_ascii=False).lower()
    assert "resta irrisolto" not in customer_text
    assert "campo resta aperto" not in customer_text
    assert "non attribuisce un dato finale" not in customer_text


def test_urbanistica_non_conforme_propagates_to_issue_cards():
    result = _make_base_result_for_qa()
    result["field_states"]["regolarita_urbanistica"].update({
        "value": "NON CONFORME / GRAVE",
        "status": "FOUND",
        "headline_it": "Regolarità urbanistica: NON CONFORME / GRAVE.",
    })
    result["issues"] = [
        {
            "family": "urbanistica",
            "headline_it": "Regolarità urbanistica: DA VERIFICARE.",
            "explanation_it": "Regolarità urbanistica: DA VERIFICARE.",
            "severity": "AMBER",
            "action_it": "Verificare genericamente.",
        }
    ]
    result["red_flags_operativi"] = [
        {
            "category": "urbanistica",
            "flag_it": "Regolarità urbanistica: DA VERIFICARE.",
            "severity": "AMBER",
        }
    ]
    result["section_11_red_flags"] = [
        {
            "code": "URBANISTICA_VERIFY",
            "flag_it": "urbanistica: DA VERIFICARE.",
            "severity": "AMBER",
        }
    ]
    result["section_9_legal_killers"]["items"] = [
        {
            "category": "urbanistica",
            "killer": "Regolarità urbanistica: DA VERIFICARE.",
            "status": "AMBER",
        }
    ]
    result["section_2_decisione_rapida"]["checks_it"] = [
        "Regolarità urbanistica: DA VERIFICARE.",
        "Verificare occupazione.",
    ]
    result["summary_for_client_bundle"]["checks_it"] = [
        "urbanistica: DA VERIFICARE.",
    ]

    apply_customer_facing_consistency_sweep(result)

    assert result["issues"][0]["headline_it"] == "Regolarità urbanistica: NON CONFORME / GRAVE."
    assert result["issues"][0]["severity"] == "RED"
    assert result["issues"][0]["action_it"] == (
        "Verificare sanabilità, costi di regolarizzazione/ripristino e conformità urbanistica "
        "con tecnico prima dell'offerta."
    )
    assert result["red_flags_operativi"][0]["severity"] == "RED"
    assert result["section_11_red_flags"][0]["flag_it"] == "Regolarità urbanistica: NON CONFORME / GRAVE."
    assert result["section_9_legal_killers"]["items"][0]["status"] == "RED"
    assert result["section_2_decisione_rapida"]["checks_it"][0] == (
        "Regolarità urbanistica: NON CONFORME / GRAVE."
    )
    assert result["summary_for_client_bundle"]["checks_it"][0] == (
        "Regolarità urbanistica: NON CONFORME / GRAVE."
    )

    hits = _collect_customer_facing_bad_text_hits(result)
    assert [h for h in hits if h["pattern"] == "urbanistica_da_verificare_after_grave"] == []


def test_opponibilita_text_cleaned_when_occupied_but_opponibility_unknown():
    result = _make_base_result_for_qa()
    result["field_states"]["stato_occupativo"]["value"] = "OCCUPATO"
    result["field_states"]["opponibilita_occupazione"].update({
        "value": "NON VERIFICABILE",
        "status": "LOW_CONFIDENCE",
        "headline_it": "Opponibilità occupazione: NON VERIFICABILE.",
        "explanation": "Lo stato di occupazione resta irrisolto.",
        "explanation_it": "Il campo resta aperto perché la perizia non attribuisce un dato finale.",
        "verify_next_it": "Rinvio o contesto locale senza valore candidato.",
        "why_not_resolved": "Lo stato di occupazione resta irrisolto.",
    })
    result["customer_decision_contract"]["field_states"] = {
        "stato_occupativo": {"value": "DA VERIFICARE"},
        "opponibilita_occupazione": {
            "value": "NON VERIFICABILE",
            "status": "LOW_CONFIDENCE",
            "explanation": "Lo stato di occupazione resta irrisolto.",
        },
    }

    apply_customer_facing_consistency_sweep(result)

    oppon = result["field_states"]["opponibilita_occupazione"]
    assert oppon["value"] == "NON VERIFICABILE"
    assert oppon["status"] == "LOW_CONFIDENCE"
    safe_text = (
        "L'immobile risulta occupato; l'opponibilità del titolo non è determinabile in modo "
        "difendibile dalle evidenze disponibili. Verificare titolo di occupazione, data certa, "
        "registrazione e opponibilità verso la procedura."
    )
    assert oppon["explanation"] == safe_text
    assert oppon["explanation_it"] == safe_text
    assert oppon["verify_next_it"] == safe_text
    assert oppon["why_not_resolved"] == (
        "La perizia conferma l'occupazione, ma non basta per stabilire l'opponibilità del titolo. "
        "Servono titolo, data certa, registrazione e rapporto con la procedura."
    )
    assert result["customer_decision_contract"]["field_states"]["opponibilita_occupazione"] == oppon
    assert _collect_customer_facing_bad_text_hits(result) == []


def test_semantic_dedup_keeps_occupato_and_opponibilita_separate():
    result = _make_base_result_for_qa()
    duplicate_cards = [
        {"headline_it": "Immobile occupato.", "severity": "AMBER", "family": "occupancy"},
        {"headline_it": "Stato occupativo: OCCUPATO.", "severity": "RED", "family": "occupancy"},
        {
            "headline_it": "Opponibilità occupazione: NON VERIFICABILE.",
            "severity": "RED",
            "family": "occupancy",
        },
        {
            "headline_it": "Stato occupativo: OCCUPATO - Bene 2.",
            "severity": "RED",
            "family": "occupancy",
            "scope": {"level": "bene", "bene_number": 2},
        },
        {"headline_it": "Regolarità urbanistica: DA VERIFICARE.", "severity": "AMBER", "family": "urbanistica"},
    ]
    result["issues"] = [card.copy() for card in duplicate_cards]
    result["red_flags_operativi"] = [
        {"flag_it": "Immobile occupato.", "severity": "AMBER", "category": "occupancy"},
        {"flag_it": "Stato occupativo: OCCUPATO.", "severity": "RED", "category": "occupancy"},
        {"flag_it": "Opponibilità occupazione: NON VERIFICABILE.", "severity": "RED", "category": "occupancy"},
    ]
    result["section_11_red_flags"] = [
        {"flag_it": "Immobile occupato.", "severity": "AMBER", "category": "occupancy"},
        {"flag_it": "Stato occupativo: OCCUPATO.", "severity": "RED", "category": "occupancy"},
    ]

    apply_customer_facing_consistency_sweep(result)

    issue_headlines = [issue["headline_it"] for issue in result["issues"]]
    assert issue_headlines.count("Immobile occupato.") == 1
    assert "Stato occupativo: OCCUPATO." not in issue_headlines
    assert "Opponibilità occupazione: NON VERIFICABILE." in issue_headlines
    assert "Stato occupativo: OCCUPATO - Bene 2." in issue_headlines
    assert "Regolarità urbanistica: DA VERIFICARE." in issue_headlines
    assert result["issues"][0]["severity"] == "RED"
    opp_issue = next(i for i in result["issues"] if i["headline_it"].startswith("Opponibilità"))
    assert opp_issue["severity"] == "AMBER"

    red_flag_titles = [flag["flag_it"] for flag in result["red_flags_operativi"]]
    assert red_flag_titles == [
        "Immobile occupato.",
        "Opponibilità occupazione: NON VERIFICABILE.",
    ]
    assert result["red_flags_operativi"][0]["severity"] == "RED"
    assert result["red_flags_operativi"][1]["severity"] == "AMBER"

    section_11_titles = [flag["flag_it"] for flag in result["section_11_red_flags"]]
    assert section_11_titles == ["Immobile occupato."]


def test_issues_occupancy_duplicate_removed_but_opponibilita_kept():
    result = _make_base_result_for_qa()
    result["issues"] = [
        {
            "headline_it": "Immobile occupato.",
            "severity": "RED",
        },
        {"headline_it": "Stato occupativo: OCCUPATO.", "severity": "RED"},
        {
            "headline_it": "Opponibilità occupazione: NON VERIFICABILE.",
            "severity": "AMBER",
        },
        {"headline_it": "Agibilità/abitabilità: DA VERIFICARE.", "severity": "AMBER"},
        {"headline_it": "Regolarità urbanistica: DA VERIFICARE.", "severity": "AMBER"},
    ]

    apply_customer_facing_consistency_sweep(result)

    headlines = [issue["headline_it"] for issue in result["issues"]]
    assert headlines == [
        "Immobile occupato.",
        "Opponibilità occupazione: NON VERIFICABILE.",
        "Agibilità/abitabilità: DA VERIFICARE.",
        "Regolarità urbanistica: DA VERIFICARE.",
    ]
    cdc_headlines = [
        issue["headline_it"]
        for issue in result["customer_decision_contract"]["issues"]
    ]
    assert cdc_headlines == headlines


def test_plain_issue_cards_dedup_occupancy_by_title_ignores_evidence_bene():
    result = _make_base_result_for_qa()
    result["issues"] = [
        {
            "headline_it": "Immobile occupato.",
            "severity": "RED",
            "evidence": [{"page": 1, "quote": "BENE N° 1 - immobile occupato."}],
        },
        {
            "headline_it": "Stato occupativo: OCCUPATO.",
            "severity": "RED",
            "evidence": [{"page": 1, "quote": "BENE N° 1 - stato occupativo occupato."}],
        },
        {"headline_it": "Agibilità: DA VERIFICARE.", "severity": "AMBER"},
        {"headline_it": "Opponibilità occupazione: NON VERIFICABILE.", "severity": "AMBER"},
        {"headline_it": "Regolarità urbanistica: NON CONFORME / GRAVE.", "severity": "RED"},
    ]

    apply_customer_facing_consistency_sweep(result)

    headlines = [issue["headline_it"] for issue in result["issues"]]
    occupancy_headlines = [
        headline
        for headline in headlines
        if headline in ("Immobile occupato.", "Stato occupativo: OCCUPATO.")
    ]
    assert occupancy_headlines == ["Immobile occupato."]
    assert "Opponibilità occupazione: NON VERIFICABILE." in headlines
    assert "Agibilità: DA VERIFICARE." in headlines
    assert "Regolarità urbanistica: NON CONFORME / GRAVE." in headlines


def test_plain_issue_cards_keep_true_per_bene_titled_occupancy_cards():
    result = _make_base_result_for_qa()
    result["issues"] = [
        {"headline_it": "Immobile occupato.", "severity": "RED"},
        {"headline_it": "Bene N° 2 occupato.", "severity": "RED"},
    ]

    apply_customer_facing_consistency_sweep(result)

    headlines = [issue["headline_it"] for issue in result["issues"]]
    assert "Immobile occupato." in headlines
    assert "Bene N° 2 occupato." in headlines


def test_issues_occupancy_duplicate_removed_even_without_code_or_category():
    result = _make_base_result_for_qa()
    result["issues"] = [
        {"headline_it": "Immobile occupato.", "severity": "AMBER"},
        {"headline_it": "Stato occupativo: OCCUPATO.", "severity": "RED"},
        {"headline_it": "Opponibilità occupazione: NON VERIFICABILE.", "severity": "RED"},
        {"headline_it": "Stato occupativo: OCCUPATO - Bene 2.", "severity": "RED"},
    ]

    apply_customer_facing_consistency_sweep(result)

    headlines = [issue["headline_it"] for issue in result["issues"]]
    assert headlines == [
        "Immobile occupato.",
        "Opponibilità occupazione: NON VERIFICABILE.",
        "Stato occupativo: OCCUPATO - Bene 2.",
    ]
    assert result["issues"][0]["severity"] == "RED"
    assert result["issues"][1]["severity"] == "AMBER"
    cdc_headlines = [
        issue["headline_it"]
        for issue in result["customer_decision_contract"]["issues"]
    ]
    assert cdc_headlines == headlines


def test_final_semantic_dedup_collapses_duplicate_agibilita_issues_same_title_page_family():
    result = _make_base_result_for_qa()
    title = "Agibilità/abitabilità: DA VERIFICARE..."
    result["issues"] = [
        {
            "issue_id": "agibilita_43af1d2775db",
            "family": "agibilita",
            "headline_it": title,
            "severity": "AMBER",
            "action_it": title,
            "evidence": [{"page": 46, "quote": "Agibilità da verificare."}],
            "supporting_pages": [46],
        },
        {
            "issue_id": "agibilita_9187f8133f07",
            "family": "agibilita",
            "headline_it": title,
            "severity": "AMBER",
            "action_it": title,
            "evidence": [{"page": 46, "quote": "Abitabilità da verificare."}],
            "supporting_pages": [46],
        },
    ]

    apply_customer_facing_consistency_sweep(result)

    agibilita_issues = [
        issue for issue in result["issues"]
        if issue.get("family") == "agibilita" and issue.get("headline_it") == title
    ]
    assert len(agibilita_issues) == 1
    assert agibilita_issues[0]["issue_id"] == "agibilita_43af1d2775db"
    assert agibilita_issues[0]["supporting_pages"] == [46]
    assert [ev["quote"] for ev in agibilita_issues[0]["evidence"]] == [
        "Agibilità da verificare.",
        "Abitabilità da verificare.",
    ]


def test_final_semantic_dedup_collapses_duplicate_agibilita_red_flags_and_section_11():
    result = _make_base_result_for_qa()
    title = "Agibilità/abitabilità: DA VERIFICARE..."
    duplicate_flags = [
        {
            "flag_it": title,
            "category": "agibilita",
            "severity": "AMBER",
            "evidence": [{"page": 46, "quote": "Agibilità da verificare."}],
        },
        {
            "flag_it": title,
            "category": "agibilita",
            "severity": "RED",
            "evidence": [{"page": 46, "quote": "Abitabilità da verificare."}],
        },
    ]
    result["red_flags_operativi"] = [copy.deepcopy(flag) for flag in duplicate_flags]
    result["section_11_red_flags"] = [copy.deepcopy(flag) for flag in duplicate_flags]

    apply_customer_facing_consistency_sweep(result)

    assert [flag["flag_it"] for flag in result["red_flags_operativi"]].count(title) == 1
    assert [flag["flag_it"] for flag in result["section_11_red_flags"]].count(title) == 1
    assert result["red_flags_operativi"][0]["severity"] == "RED"
    assert result["section_11_red_flags"][0]["severity"] == "RED"
    assert len(result["red_flags_operativi"][0]["evidence"]) == 2
    assert len(result["section_11_red_flags"][0]["evidence"]) == 2


def test_final_semantic_dedup_preserves_useful_action_over_copied_title_action():
    result = _make_base_result_for_qa()
    title = "Agibilità/abitabilità: DA VERIFICARE..."
    useful_action = "Richiedere al custode o al tecnico il certificato di agibilità/abitabilità prima dell'offerta."
    result["issues"] = [
        {
            "family": "agibilita",
            "headline_it": title,
            "severity": "AMBER",
            "action_it": title,
            "evidence": [{"page": 46, "quote": "Agibilità da verificare."}],
        },
        {
            "family": "agibilita",
            "headline_it": title,
            "severity": "AMBER",
            "action_it": useful_action,
            "evidence": [{"page": 46, "quote": "Agibilità da verificare."}],
        },
    ]

    apply_customer_facing_consistency_sweep(result)

    agibilita_issue = next(issue for issue in result["issues"] if issue.get("family") == "agibilita")
    assert agibilita_issue["action_it"] == useful_action


def test_final_semantic_dedup_keeps_occupancy_and_opponibility_separate():
    result = _make_base_result_for_qa()
    result["issues"] = [
        {
            "headline_it": "Immobile occupato.",
            "family": "occupancy",
            "severity": "RED",
            "evidence": [{"page": 12, "quote": "L'immobile risulta occupato."}],
        },
        {
            "headline_it": "Opponibilità occupazione: NON VERIFICABILE.",
            "family": "occupancy",
            "severity": "AMBER",
            "evidence": [{"page": 12, "quote": "Titolo di occupazione da verificare."}],
        },
    ]

    apply_customer_facing_consistency_sweep(result)

    headlines = [issue["headline_it"] for issue in result["issues"]]
    assert "Immobile occupato." in headlines
    assert "Opponibilità occupazione: NON VERIFICABILE." in headlines
    assert len(headlines) == 2


def test_final_semantic_dedup_keeps_per_bene_agibilita_cards_separate():
    result = _make_base_result_for_qa()
    title = "Agibilità/abitabilità: DA VERIFICARE..."
    result["issues"] = [
        {
            "family": "agibilita",
            "headline_it": title,
            "severity": "AMBER",
            "evidence": [{"page": 46, "quote": "Bene N° 1: agibilità da verificare."}],
        },
        {
            "family": "agibilita",
            "headline_it": title,
            "severity": "AMBER",
            "evidence": [{"page": 46, "quote": "Bene N° 2: agibilità da verificare."}],
        },
    ]

    apply_customer_facing_consistency_sweep(result)

    agibilita_issues = [issue for issue in result["issues"] if issue.get("family") == "agibilita"]
    assert len(agibilita_issues) == 2
    assert {issue["evidence"][0]["quote"] for issue in agibilita_issues} == {
        "Bene N° 1: agibilità da verificare.",
        "Bene N° 2: agibilità da verificare.",
    }


def test_final_semantic_dedup_syncs_customer_decision_contract_mirrors():
    result = _make_base_result_for_qa()
    title = "Agibilità/abitabilità: DA VERIFICARE..."
    result["issues"] = [
        {"family": "agibilita", "headline_it": title, "severity": "AMBER", "evidence": [{"page": 46, "quote": "Agibilità da verificare."}]},
        {"family": "agibilita", "headline_it": title, "severity": "RED", "evidence": [{"page": 46, "quote": "Abitabilità da verificare."}]},
    ]
    result["red_flags_operativi"] = [
        {"category": "agibilita", "flag_it": title, "severity": "AMBER", "evidence": [{"page": 46, "quote": "Agibilità da verificare."}]},
        {"category": "agibilita", "flag_it": title, "severity": "RED", "evidence": [{"page": 46, "quote": "Abitabilità da verificare."}]},
    ]
    result["section_11_red_flags"] = [copy.deepcopy(flag) for flag in result["red_flags_operativi"]]
    result["section_9_legal_killers"]["items"] = [
        {"category": "agibilita", "killer": title, "status": "AMBER", "action": title, "evidence": [{"page": 46, "quote": "Agibilità da verificare."}]},
        {"category": "agibilita", "killer": title, "status": "RED", "action": "Verificare agibilità/abitabilità.", "evidence": [{"page": 46, "quote": "Abitabilità da verificare."}]},
    ]
    result["section_9_legal_killers"]["top_items"] = [copy.deepcopy(item) for item in result["section_9_legal_killers"]["items"]]
    result["customer_decision_contract"].update({
        "issues": [copy.deepcopy(issue) for issue in result["issues"]],
        "red_flags_operativi": [copy.deepcopy(flag) for flag in result["red_flags_operativi"]],
        "section_11_red_flags": [copy.deepcopy(flag) for flag in result["section_11_red_flags"]],
        "section_9_legal_killers": copy.deepcopy(result["section_9_legal_killers"]),
    })

    apply_customer_facing_consistency_sweep(result)

    cdc = result["customer_decision_contract"]
    assert cdc["issues"] == result["issues"]
    assert cdc["red_flags_operativi"] == result["red_flags_operativi"]
    assert cdc["section_11_red_flags"] == result["section_11_red_flags"]
    assert cdc["section_9_legal_killers"] == result["section_9_legal_killers"]
    assert len(cdc["issues"]) == 1
    assert len(cdc["red_flags_operativi"]) == 1
    assert len(cdc["section_11_red_flags"]) == 1
    assert len(cdc["section_9_legal_killers"]["items"]) == 1
    assert len(cdc["section_9_legal_killers"]["top_items"]) == 1


def test_customer_bad_hits_detects_stale_occupancy_and_urbanistica_projection():
    result = _make_base_result_for_qa()
    result["field_states"]["stato_occupativo"]["value"] = "OCCUPATO"
    result["field_states"]["regolarita_urbanistica"]["value"] = "NON CONFORME / GRAVE"
    result["issues"] = [
        {
            "headline_it": "Stato occupativo.",
            "explanation_it": "Lo stato di occupazione resta irrisolto.",
            "severity": "RED",
        },
        {
            "headline_it": "Regolarità urbanistica: DA VERIFICARE.",
            "severity": "AMBER",
        },
    ]

    hits = _collect_customer_facing_bad_text_hits(result)
    patterns = {hit["pattern"] for hit in hits}
    assert "occupancy_stale_unresolved_after_occupied" in patterns
    assert "urbanistica_da_verificare_after_grave" in patterns


def test_full_consistency_sweep_syncs_cdc_after_projection_fixes():
    result = _make_base_result_for_qa()
    result["field_states"]["stato_occupativo"].update({
        "value": "OCCUPATO",
        "explanation_it": "Lo stato di occupazione resta irrisolto.",
        "why_not_resolved": "Campo resta aperto.",
    })
    result["field_states"]["opponibilita_occupazione"].update({
        "value": "NON VERIFICABILE",
        "status": "LOW_CONFIDENCE",
        "explanation_it": "Lo stato di occupazione resta irrisolto.",
        "why_not_resolved": "Rinvio o contesto locale senza valore candidato.",
    })
    result["field_states"]["regolarita_urbanistica"].update({
        "value": "NON CONFORME / GRAVE",
        "status": "FOUND",
    })
    result["issues"] = [
        {"headline_it": "Immobile occupato.", "severity": "AMBER", "family": "occupancy"},
        {"headline_it": "Stato occupativo: OCCUPATO.", "severity": "RED", "family": "occupancy"},
        {"headline_it": "Regolarità urbanistica: DA VERIFICARE.", "severity": "AMBER", "family": "urbanistica"},
    ]
    result["red_flags_operativi"] = [
        {"flag_it": "Regolarità urbanistica: DA VERIFICARE.", "severity": "AMBER", "category": "urbanistica"}
    ]
    result["section_2_decisione_rapida"]["checks_it"] = ["Regolarità urbanistica: DA VERIFICARE."]
    result["summary_for_client_bundle"]["checks_it"] = ["Regolarità urbanistica: DA VERIFICARE."]
    result["customer_decision_contract"].update({
        "field_states": {
            "stato_occupativo": {"value": "DA VERIFICARE", "explanation_it": "Stale"},
            "opponibilita_occupazione": {
                "value": "NON VERIFICABILE",
                "status": "LOW_CONFIDENCE",
                "explanation_it": "Lo stato di occupazione resta irrisolto.",
            },
            "regolarita_urbanistica": {"value": "DA VERIFICARE"},
        },
        "issues": [{"headline_it": "Stato occupativo: OCCUPATO.", "severity": "RED"}],
        "red_flags_operativi": [{"flag_it": "Regolarità urbanistica: DA VERIFICARE.", "severity": "AMBER"}],
        "summary_for_client_bundle": {"checks_it": ["Regolarità urbanistica: DA VERIFICARE."]},
    })

    apply_customer_facing_consistency_sweep(result)

    cdc = result["customer_decision_contract"]
    assert cdc["field_states"]["stato_occupativo"]["explanation_it"] == (
        "La perizia indica che l'immobile risulta occupato. "
        "L'opponibilità del titolo deve essere verificata separatamente."
    )
    assert cdc["field_states"]["regolarita_urbanistica"]["value"] == "NON CONFORME / GRAVE"
    assert cdc["field_states"]["opponibilita_occupazione"]["explanation_it"] == (
        "L'immobile risulta occupato; l'opponibilità del titolo non è determinabile in modo "
        "difendibile dalle evidenze disponibili. Verificare titolo di occupazione, data certa, "
        "registrazione e opponibilità verso la procedura."
    )
    assert cdc["field_states"]["opponibilita_occupazione"]["why_not_resolved"] == (
        "La perizia conferma l'occupazione, ma non basta per stabilire l'opponibilità del titolo. "
        "Servono titolo, data certa, registrazione e rapporto con la procedura."
    )
    cdc_issue_titles = [issue["headline_it"] for issue in cdc["issues"]]
    assert cdc_issue_titles == [
        "Immobile occupato.",
        "Regolarità urbanistica: NON CONFORME / GRAVE.",
    ]
    assert cdc["red_flags_operativi"][0]["severity"] == "RED"
    assert cdc["summary_for_client_bundle"]["checks_it"] == [
        "Regolarità urbanistica: NON CONFORME / GRAVE."
    ]
    assert cdc["decision_rapida_client"]["checks_it"] == [
        "Regolarità urbanistica: NON CONFORME / GRAVE."
    ]

    hits = _collect_customer_facing_bad_text_hits(result)
    assert hits == []


def test_live_via_nuova_style_payload_has_no_customer_facing_bad_hits():
    """Full analysis_f55750bc3f91-style payload must have zero bad hits after the gate sweep."""
    result = _make_base_result_for_qa()

    # Simulate stale customer-facing state matching the live bad hits
    result["field_states"]["agibilita"]["value"] = "DA VERIFICARE"
    result["field_states"]["agibilita"]["status"] = "LOW_CONFIDENCE"
    result["field_states"]["agibilita"]["explanation"] = (
        "L'agibilità risulta assente nel caso canonico verificato."
    )
    result["field_states"]["stato_occupativo"]["value"] = "OCCUPATO"

    fake_label = "Costi espliciti a carico dell'acquirente: € 528.123,68."
    buyer_label = "Costo buyer-side esplicito da perizia"
    buyer_nota = "Costo buyer-side esplicito rilevato nella perizia."

    result["semaforo_generale"] = {
        "colore": "ROSSO",
        "reason_it": "Problemi rilevati.",
        "top_blockers": [
            {"label_it": "Immobile occupato.", "severity": "HIGH"},
            {"label_it": "Agibilità: ASSENTE.", "severity": "HIGH"},
            {"label_it": fake_label, "severity": "HIGH"},
        ],
    }
    result["issues"] = [
        {
            "headline_it": "Agibilità assente.",
            "explanation_it": (
                "L'agibilità risulta assente o non rilasciata e richiede verifica immediata "
                "prima dell'offerta."
            ),
            "action_it": "L'agibilità risulta assente nel caso canonico verificato.",
            "severity": "RED",
        },
        {
            "headline_it": "Agibilità.",
            "explanation_it": "L'agibilità risulta assente nel caso canonico verificato.",
            "action_it": "L'agibilità risulta assente nel caso canonico verificato.",
            "severity": "RED",
        },
        {"headline_it": "Occupazione.", "explanation_it": "Immobile occupato.", "severity": "RED"},
        {
            "headline_it": "Costi.",
            "explanation_it": "La perizia indica costi espliciti a carico dell'acquirente.",
            "severity": "AMBER",
        },
    ]
    result["section_3_money_box"]["items"] = [
        {
            "code": "VR_COST_01",
            "label_it": buyer_label,
            "label_en": buyer_label,
            "stima_nota": buyer_nota,
            "stima_euro": None,
            "type": "BUYER_SIDE_COST",
            "additive_to_extra_total": True,
        }
    ]
    result["money_box"]["items"] = list(result["section_3_money_box"]["items"])
    result["red_flags_operativi"] = [
        {
            "flag_it": "Agibilità assente / non rilasciata.",
            "flag_en": "Agibilità: ASSENTE",
            "action_it": "L'agibilità risulta assente nel caso canonico verificato.",
            "severity": "RED",
        },
    ]
    result["section_11_red_flags"] = [
        {
            "flag_it": "Agibilità assente / non rilasciata.",
            "flag_en": "Agibilità: ASSENTE",
            "action_it": "L'agibilità risulta assente nel caso canonico verificato.",
            "severity": "RED",
        },
    ]
    result["section_9_legal_killers"]["resolver_meta"]["themes"] = [
        {
            "theme": "agibilita",
            "note": "L'agibilità risulta assente nel caso canonico verificato.",
        }
    ]
    result["summary_for_client_bundle"].update({
        "top_issue_it": "Agibilità assente o non rilasciata.",
        "main_risk_it": "L'agibilità risulta assente nel caso canonico verificato.",
        "caution_points_it": ["Agibilità: ASSENTE"],
        "checks_it": ["Verificare agibilità non rilasciata prima dell'offerta."],
        "decision_summary_it": "L'agibilità risulta assente nel caso canonico verificato.",
    })
    result["section_2_decisione_rapida"]["main_risk_it"] = (
        "L'agibilità risulta assente nel caso canonico verificato."
    )
    result["section_2_decisione_rapida"]["checks_it"] = ["Agibilità: ASSENTE"]
    result["decision_rapida_client"] = {
        "summary_it": "L'agibilità risulta assente nel caso canonico verificato.",
        "main_risk_it": "Agibilità assente o non rilasciata.",
        "checks_it": ["Verificare agibilità non rilasciata prima dell'offerta."],
    }
    result["lots"] = [
        {"lot_number": 1, "stato_occupativo": "DA VERIFICARE", "occupancy_status": "DA VERIFICARE"}
    ]
    result["customer_decision_contract"].update({
        "semaforo_generale": result["semaforo_generale"].copy(),
        "issues": [i.copy() for i in result["issues"]],
        "lots": [lot.copy() for lot in result["lots"]],
        "field_states": {"agibilita": result["field_states"]["agibilita"].copy()},
        "red_flags_operativi": [flag.copy() for flag in result["red_flags_operativi"]],
        "section_11_red_flags": [flag.copy() for flag in result["section_11_red_flags"]],
        "summary_for_client_bundle": result["summary_for_client_bundle"].copy(),
        "decision_rapida_client": result["decision_rapida_client"].copy(),
    })

    # qa_gate contradictions are allowed to retain wrong claims — simulate them
    # (applied AFTER the sweep to confirm they're excluded from the scan)

    mocked_response = {
        "qa_status": "FAIL_CORRECTED",
        "overall_verdict_it": "Multiple issues corrected.",
        "context_used": {"mode": "EVIDENCE_ONLY", "pages_reviewed": [5, 8, 14], "limitations_it": ""},
        "contradictions_detected": [
            {
                "id": "c1",
                "severity": "CRITICAL",
                "problem_it": "Totale buyer-side falso.",
                "current_wrong_claim": "Costi espliciti a carico dell'acquirente: € 528.123,68",
                "evidence_pages": [5],
                "evidence_quotes": ["Valore di mercato 528.123,68"],
                "recommended_action": "REMOVE_EXACT_TOTAL",
            },
            {
                "id": "c2",
                "severity": "HIGH",
                "problem_it": "Agibilità overclaim.",
                "current_wrong_claim": "Agibilità: ASSENTE",
                "evidence_pages": [8],
                "evidence_quotes": ["terrapieno non agibile"],
                "recommended_action": "DOWNGRADE_TO_VERIFY",
            },
            {
                "id": "c3",
                "severity": "HIGH",
                "problem_it": "Occupancy split.",
                "current_wrong_claim": "DA VERIFICARE",
                "evidence_pages": [14],
                "evidence_quotes": ["immobile occupato"],
                "recommended_action": "SPLIT_OCCUPANCY_OPPONIBILITY",
            },
        ],
        "corrections": [
            {
                "id": "fake_total_528",
                "target": "money_box",
                "action": "REMOVE_EXACT_TOTAL",
                "safe_value_it": "Totale non quantificabile.",
                "reason_it": "VdM non è costo buyer-side.",
                "evidence_pages": [5],
                "evidence_quotes": ["Valore di mercato 528.123,68"],
                "confidence": 0.95,
            },
            {
                "id": "agib_down",
                "target": "agibilita",
                "action": "DOWNGRADE_TO_VERIFY",
                "safe_value_it": "Solo terrapieno non agibile; certificato globale da verificare.",
                "reason_it": "Scope locale.",
                "evidence_pages": [8],
                "evidence_quotes": ["terrapieno non agibile"],
                "confidence": 0.88,
            },
        ],
        "section_verdicts": {},
    }

    with mock.patch("customer_contract_qa_gate.call_customer_qa_llm", return_value=mocked_response):
        qa_meta = apply_customer_contract_qa_gate(result, raw_text=None)

    assert qa_meta["status"] == "FAIL_CORRECTED"
    assert qa_meta["llm_used"] is True

    # qa_gate contradictions_detected must preserve the original wrong claims (audit metadata)
    qa_contradictions_text = json.dumps(result["qa_gate"]["contradictions_detected"], ensure_ascii=False)
    assert "528" in qa_contradictions_text or "ASSENTE" in qa_contradictions_text, (
        "qa_gate.contradictions_detected must preserve old wrong claims"
    )

    # Customer-facing sections must have zero bad hits
    hits = _collect_customer_facing_bad_text_hits(result)
    assert hits == [], (
        f"Expected 0 customer-facing bad hits, got {len(hits)}:\n"
        + "\n".join(f"  {h['pattern']} at {h['key']}: ...{h['text_excerpt']}..." for h in hits)
    )

    # Specific structural assertions
    assert result["field_states"]["stato_occupativo"]["value"] == "OCCUPATO"
    assert result["lots"][0]["stato_occupativo"] == "OCCUPATO"
    assert result["lots"][0]["occupancy_status"] == "OCCUPATO"

    total = result["section_3_money_box"]["total_extra_costs"]
    assert total["min"] is None
    assert total["max"] is None
