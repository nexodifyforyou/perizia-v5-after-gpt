import json
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
