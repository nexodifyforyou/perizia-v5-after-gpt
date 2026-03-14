import os
import sys
from copy import deepcopy

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import server as server


def _result_with_items(*items):
    return {
        "section_9_legal_killers": {"items": list(items)},
        "section_1_semaforo_generale": {},
    }


def _normalize(result, pages):
    server._normalize_legal_killers(result, pages)
    states = result.get("field_states", {})
    server._ensure_semaforo_top_blockers(result, states, pages)
    server._recompute_semaforo_status(result)
    return result


def _killer_by_name(result, name):
    for item in result["section_9_legal_killers"]["items"]:
        if str(item.get("killer") or "").strip().lower() == name.lower():
            return item
    return None


def test_real_blocker_survives_near_page_number_and_stale_offsets():
    pages = [
        {
            "page_number": 7,
            "text": (
                "7 di 15\n"
                "A favore di BPER Credit Management S.C.p.A\n"
                "NORMATIVA URBANISTICA\n"
                "L'immobile non risulta regolare per la legge n° 47/1985. "
                "La costruzione è successiva al 01/09/1967.\n"
            ),
        }
    ]
    result = _result_with_items(
        {
            "killer": "Immobile non regolare ediliziamente",
            "status": "ROSSO",
            "reason_it": "Criticità urbanistico-edilizia determinante",
            "evidence": [
                {
                    "page": 7,
                    "quote": "L'immobile non risulta regolare per la legge n° 47/1985. La costruzione è successiva al 01/09/1967.",
                    "start_offset": 0,
                    "end_offset": 43,
                }
            ],
        }
    )

    normalized = _normalize(result, pages)
    killer = _killer_by_name(normalized, "Immobile non regolare ediliziamente")

    assert killer is not None
    assert killer["status"] == "ROSSO"
    assert normalized["section_9_legal_killers"]["top_items"][0]["killer"] == "Immobile non regolare ediliziamente"
    assert normalized["section_1_semaforo_generale"]["top_blockers"][0]["label_it"] == "Immobile non regolare ediliziamente"


def test_real_blocker_survives_near_header_footer_noise():
    pages = [
        {
            "page_number": 8,
            "text": (
                "TRIBUNALE DI ROMA\n"
                "Perizia dell'Esperto\n"
                "La Concessione in Sanatoria non è stata ad oggi rilasciata, "
                "nè risulta l'emissione di un diniego da parte del Comune di Roma.\n"
                "Documento firmato digitalmente\n"
            ),
        }
    ]
    result = _result_with_items(
        {
            "killer": "Sanatoria / condono non perfezionati",
            "status": "ROSSO",
            "reason_it": "Sanatoria/condono non perfezionati",
            "evidence": [
                {
                    "page": 8,
                    "quote": "La Concessione in Sanatoria non è stata ad oggi rilasciata, nè risulta l'emissione di un diniego da parte",
                    "start_offset": 0,
                    "end_offset": 30,
                }
            ],
        }
    )

    normalized = _normalize(result, pages)
    killer = _killer_by_name(normalized, "Sanatoria / condono non perfezionati")

    assert killer is not None
    assert len(killer["evidence"]) == 1
    assert normalized["section_9_legal_killers"]["top_items"][0]["killer"] == "Sanatoria / condono non perfezionati"


def test_pure_toc_entry_is_filtered():
    pages = [
        {
            "page_number": 2,
            "text": "SOMMARIO\nAgibilità assente / non rilasciata ........ 7\nUso residenziale non legittimato ........ 9\n",
        }
    ]
    result = _result_with_items(
        {
            "killer": "Agibilità assente / non rilasciata",
            "status": "ROSSO",
            "reason_it": "Agibilità/abitabilità assente o non rilasciata",
            "evidence": [
                {
                    "page": 2,
                    "quote": "Agibilità assente / non rilasciata ........ 7",
                    "start_offset": 9,
                    "end_offset": 52,
                }
            ],
        }
    )

    normalized = _normalize(result, pages)

    killer = _killer_by_name(normalized, "Agibilità assente / non rilasciata")

    assert killer is not None
    assert killer["status"] == "DA_VERIFICARE"
    assert killer["evidence"] == []
    assert normalized["section_9_legal_killers"]["top_items"] == []


def test_schema_summary_page_start_real_evidence_survives():
    pages = [
        {
            "page_number": 14,
            "text": (
                "14 di 15\n"
                "SCHEMA RIASSUNTIVO\n"
                "LOTTO UNICO - PREZZO BASE D'ASTA: € 172.000,00\n"
                "occupazione: Occupato da terzi senza titolo\n"
            ),
        }
    ]
    result = _result_with_items(
        {
            "killer": "Occupato da terzi senza titolo",
            "status": "ROSSO",
            "reason_it": "Occupazione da terzi senza titolo opponibile",
            "evidence": [
                {
                    "page": 14,
                    "quote": "occupazione: Occupato da terzi senza titolo",
                    "start_offset": 0,
                    "end_offset": 43,
                }
            ],
        }
    )

    normalized = _normalize(result, pages)

    assert _killer_by_name(normalized, "Occupato da terzi senza titolo") is not None
    assert normalized["section_9_legal_killers"]["top_items"][0]["killer"] == "Occupato da terzi senza titolo"


def test_real_blocker_survives_with_whitespace_drift_between_quote_and_page_text():
    pages = [
        {
            "page_number": 14,
            "text": (
                "14 di 15\n"
                "SCHEMA RIASSUNTIVO\n"
                "occupazione:\n"
                "Occupato da terzi senza titolo\n"
            ),
        }
    ]
    result = _result_with_items(
        {
            "killer": "Occupato da terzi senza titolo",
            "status": "ROSSO",
            "reason_it": "Occupazione da terzi senza titolo opponibile",
            "evidence": [
                {
                    "page": 14,
                    "quote": "occupazione: Occupato da terzi senza titolo",
                    "search_hint": "Occupato da terzi senza titolo",
                    "start_offset": 0,
                    "end_offset": 20,
                }
            ],
        }
    )

    normalized = _normalize(result, pages)

    killer = _killer_by_name(normalized, "Occupato da terzi senza titolo")
    assert killer is not None
    assert len(killer["evidence"]) == 1
    assert normalized["section_9_legal_killers"]["top_items"][0]["killer"] == "Occupato da terzi senza titolo"


def test_refresh_path_is_deterministic_for_same_input():
    pages = [
        {
            "page_number": 7,
            "text": (
                "7 di 15\n"
                "L'immobile non risulta regolare per la legge n° 47/1985. La costruzione è successiva al 01/09/1967.\n"
                "Non risulta rilasciato il certificato di agibilità.\n"
            ),
        }
    ]
    base = _result_with_items(
        {
            "killer": "Immobile non regolare ediliziamente",
            "status": "ROSSO",
            "reason_it": "Criticità urbanistico-edilizia determinante",
            "evidence": [
                {
                    "page": 7,
                    "quote": "L'immobile non risulta regolare per la legge n° 47/1985. La costruzione è successiva al 01/09/1967.",
                    "start_offset": 0,
                    "end_offset": 20,
                }
            ],
        },
        {
            "killer": "Agibilità assente / non rilasciata",
            "status": "ROSSO",
            "reason_it": "Agibilità/abitabilità assente o non rilasciata",
            "evidence": [
                {
                    "page": 7,
                    "quote": "Non risulta rilasciato il certificato di agibilità.",
                    "start_offset": 0,
                    "end_offset": 10,
                }
            ],
        },
    )

    first = _normalize(deepcopy(base), pages)
    second = _normalize(deepcopy(base), pages)

    assert first["section_9_legal_killers"]["items"] == second["section_9_legal_killers"]["items"]
    assert first["section_9_legal_killers"]["top_items"] == second["section_9_legal_killers"]["top_items"]
    assert first["section_1_semaforo_generale"]["top_blockers"] == second["section_1_semaforo_generale"]["top_blockers"]
    assert first["section_1_semaforo_generale"]["status"] == second["section_1_semaforo_generale"]["status"]
