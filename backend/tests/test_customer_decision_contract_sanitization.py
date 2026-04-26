import json
import sys
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
    _project_certification_block_to_beni,
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
