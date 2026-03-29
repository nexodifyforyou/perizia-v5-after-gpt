import io
import os
import sys
import zipfile
import copy
from datetime import datetime, timezone

import httpx
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import server as server
from test_admin import FakeDB, _seed_session


@pytest.fixture()
def fake_db(monkeypatch):
    fake_db = FakeDB()
    monkeypatch.setattr(server, "db", fake_db)
    server.MASTER_ADMIN_EMAIL = "admin@example.com"
    return fake_db


def _user(email: str = "user@example.com", *, is_master_admin: bool = False) -> server.User:
    return server.User(
        user_id="user_1" if not is_master_admin else "admin_1",
        email=email,
        name="User" if not is_master_admin else "Admin",
        plan="pro",
        is_master_admin=is_master_admin,
        quota={"perizia_scans_remaining": 10, "image_scans_remaining": 0, "assistant_messages_remaining": 0},
    )


def _seed_analysis(fake_db, *, analysis_id: str, user_id: str, result: dict, headline_overrides=None, field_overrides=None):
    fake_db.perizia_analyses.items.append(
        {
            "analysis_id": analysis_id,
            "user_id": user_id,
            "case_id": "case_1",
            "run_id": "run_1",
            "case_title": "perizia.pdf",
            "file_name": "perizia.pdf",
            "pages_count": 2,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "result": result,
            "headline_overrides": headline_overrides or {},
            "field_overrides": field_overrides or {},
        }
    )


def _build_result_for_pages(pages):
    result = server.create_fallback_analysis("perizia.pdf", "case_1", "run_1", pages, server._build_full_text_from_pages(pages))
    server._apply_headline_field_states(result, pages)
    server._apply_decision_field_states(result, pages)
    server._normalize_legal_killers(result, pages)
    server._apply_market_ranges_to_money_box(result)
    server._normalize_evidence_offsets(result, pages)
    result["panoramica_contract"] = server._build_panoramica_contract(result, pages)
    return result


def _build_result_with_legal_items(pages, items):
    result = server.create_fallback_analysis("perizia.pdf", "case_1", "run_1", pages, server._build_full_text_from_pages(pages))
    result["section_9_legal_killers"] = {"items": items}
    server._apply_headline_field_states(result, pages)
    server._apply_decision_field_states(result, pages)
    server._normalize_legal_killers(result, pages)
    server._ensure_semaforo_top_blockers(result, result.get("field_states", {}), pages)
    return result


def _build_result_with_money_box_items(pages, mutate_items):
    result = server.create_fallback_analysis("perizia.pdf", "case_1", "run_1", pages, server._build_full_text_from_pages(pages))
    items = copy.deepcopy(result["money_box"]["items"])
    mutate_items(items)
    result["money_box"]["items"] = copy.deepcopy(items)
    result["section_3_money_box"] = copy.deepcopy(result["money_box"])
    server._apply_headline_field_states(result, pages)
    server._apply_decision_field_states(result, pages)
    server._normalize_legal_killers(result, pages)
    server._apply_market_ranges_to_money_box(result)
    return result


def test_occupazione_uncertain_case_produces_confirmation_metadata():
    pages = [
        {"page_number": 4, "text": "STATO OCCUPATIVO\nL'immobile risulta libero e nella disponibilità della procedura.\n"},
        {"page_number": 5, "text": "STATO OCCUPATIVO\nL'immobile risulta occupato dal debitore alla data del sopralluogo.\n"},
    ]
    state = server._extract_stato_occupativo_state(pages)
    assert state["status"] == "LOW_CONFIDENCE"
    assert state["review_required"] is True
    assert state["needs_user_confirmation"] is True
    assert len(state["top_candidates"]) == 2
    assert {state["top_candidates"][0]["value"], state["top_candidates"][1]["value"]} == {"LIBERO", "OCCUPATO DAL DEBITORE"}


def test_later_authoritative_libero_beats_earlier_weak_occupato():
    pages = [
        {"page_number": 1, "text": "RIEPILOGO: stato occupativo: occupato | prezzo base: € 100.000\n"},
        {"page_number": 7, "text": "STATO OCCUPATIVO\nL'immobile risulta libero e nella disponibilità della procedura.\n"},
    ]
    state = server._extract_stato_occupativo_state(pages)
    assert state["status"] == "FOUND"
    assert state["value"] == "LIBERO"
    assert state["chosen_candidate"]["page"] == 7


def test_later_authoritative_terzi_senza_titolo_beats_earlier_libero():
    pages = [
        {"page_number": 2, "text": "Nota storica: immobile libero al precedente accesso del custode.\n"},
        {"page_number": 8, "text": "STATO OCCUPATIVO\nL'immobile risulta occupato da terzi senza titolo opponibile.\n"},
    ]
    state = server._extract_stato_occupativo_state(pages)
    assert state["status"] == "FOUND"
    assert state["value"] == "OCCUPATO DA TERZI SENZA TITOLO"
    assert state["chosen_candidate"]["page"] == 8


def test_strong_unresolved_conflict_yields_review_required_output():
    pages = [
        {"page_number": 9, "text": "STATO OCCUPATIVO\nL'immobile risulta libero.\n"},
        {"page_number": 10, "text": "STATO OCCUPATIVO\nL'immobile risulta occupato dal debitore alla data del sopralluogo.\n"},
    ]
    state = server._extract_stato_occupativo_state(pages)
    assert state["value"] == "DA VERIFICARE"
    assert state["status"] == "LOW_CONFIDENCE"
    assert state["conflicts"]


def test_noisy_table_like_mention_is_downweighted_against_narrative():
    pages = [
        {"page_number": 1, "text": "Tabella riepilogo | occupato | € 120.000 | mq 90\n"},
        {"page_number": 3, "text": "STATO OCCUPATIVO\nDalla perizia emerge che il bene risulta libero.\n"},
    ]
    state = server._extract_stato_occupativo_state(pages)
    assert state["value"] == "LIBERO"
    assert state["chosen_candidate"]["is_table_like"] is False


def test_explicit_prezzo_base_extraction_from_strong_clause():
    pages = [
        {"page_number": 2, "text": "Il prezzo base d'asta del lotto è pari a € 85.000,00.\n"},
    ]
    state = server._extract_prezzo_base_asta_state(pages)
    assert state["status"] == "FOUND"
    assert state["value"] == 85000.0


def test_prezzo_base_not_confused_with_valore_di_stima():
    pages = [
        {"page_number": 2, "text": "Valore di stima € 120.000,00.\nPrezzo base d'asta € 85.000,00.\n"},
    ]
    state = server._extract_prezzo_base_asta_state(pages)
    assert state["status"] == "FOUND"
    assert state["value"] == 85000.0


def test_prezzo_base_not_confused_with_offerta_minima():
    pages = [
        {"page_number": 3, "text": "Offerta minima € 63.750,00.\nPrezzo base asta € 85.000,00.\n"},
    ]
    state = server._extract_prezzo_base_asta_state(pages)
    assert state["status"] == "FOUND"
    assert state["value"] == 85000.0


def test_prezzo_base_not_confused_with_rilancio_minimo():
    pages = [
        {"page_number": 3, "text": "Rilancio minimo € 2.000,00.\nPrezzo base asta € 85.000,00.\n"},
    ]
    state = server._extract_prezzo_base_asta_state(pages)
    assert state["status"] == "FOUND"
    assert state["value"] == 85000.0


def test_prezzo_base_not_confused_with_nearby_cost_numbers():
    pages = [
        {"page_number": 4, "text": "Prezzo base del lotto € 85.000,00. Oneri di regolarizzazione urbanistica € 12.000,00.\n"},
    ]
    state = server._extract_prezzo_base_asta_state(pages)
    assert state["status"] == "FOUND"
    assert state["value"] == 85000.0


def test_later_authoritative_urbanistica_compliant_beats_earlier_weak_negative():
    pages = [
        {"page_number": 1, "text": "Tabella costi | oneri di regolarizzazione urbanistica | sanatoria € 5.000 | valore finale € 120.000\n"},
        {"page_number": 7, "text": "REGOLARITA URBANISTICA\nNon risultano abusi edilizi e il bene risulta conforme urbanisticamente.\n"},
    ]
    state = server._extract_regolarita_urbanistica_state(pages)
    assert state["status"] == "FOUND"
    assert state["value"] == "NON EMERGONO ABUSI"
    assert state["chosen_candidate"]["page"] == 7


def test_later_authoritative_urbanistica_non_compliant_beats_earlier_weak_positive():
    pages = [
        {"page_number": 1, "text": "Nota storica: in passato non emergevano abusi edilizi sul bene.\n"},
        {"page_number": 8, "text": "ABUSI EDILIZI E CONFORMITA URBANISTICA\nSi rilevano difformità urbanistiche e opere abusive da sanare.\n"},
    ]
    state = server._extract_regolarita_urbanistica_state(pages)
    assert state["status"] == "FOUND"
    assert state["value"] == "PRESENTI DIFFORMITÀ"
    assert state["chosen_candidate"]["page"] == 8


def test_urbanistica_strong_unresolved_conflict_yields_review_required_output():
    pages = [
        {"page_number": 4, "text": "REGOLARITA URBANISTICA\nNon risultano abusi edilizi e il bene appare conforme urbanisticamente.\n"},
        {"page_number": 5, "text": "CONFORMITA URBANISTICA\nSi rilevano difformità urbanistiche e opere abusive da sanare.\n"},
    ]
    state = server._extract_regolarita_urbanistica_state(pages)
    assert state["value"] == "DA VERIFICARE"
    assert state["status"] == "LOW_CONFIDENCE"
    assert state["review_required"] is True
    assert state["needs_user_confirmation"] is True
    assert state["conflicts"]


def test_urbanistica_table_like_mentions_are_downweighted_against_narrative():
    pages = [
        {"page_number": 1, "text": "ONERI DI REGOLARIZZAZIONE URBANISTICA | condono € 7.000 | valore finale € 120.000\n"},
        {"page_number": 3, "text": "ABUSI EDILIZI E CONFORMITA URBANISTICA\nNon risultano difformità urbanistiche né abusi edilizi.\n"},
    ]
    state = server._extract_regolarita_urbanistica_state(pages)
    assert state["value"] == "NON EMERGONO ABUSI"
    assert state["chosen_candidate"]["is_table_like"] is False


def test_urbanistica_uncertain_case_exposes_top_two_candidates():
    pages = [
        {"page_number": 6, "text": "REGOLARITA URBANISTICA\nNon risultano abusi edilizi e il bene risulta conforme urbanisticamente.\n"},
        {"page_number": 7, "text": "ABUSI EDILIZI\nSono presenti difformità urbanistiche e irregolarità da sanare.\n"},
    ]
    state = server._extract_regolarita_urbanistica_state(pages)
    assert state["review_required"] is True
    assert len(state["top_candidates"]) == 2
    assert {state["top_candidates"][0]["value"], state["top_candidates"][1]["value"]} == {"NON EMERGONO ABUSI", "PRESENTI DIFFORMITÀ"}


def test_later_authoritative_catastale_compliant_beats_earlier_weak_negative():
    pages = [
        {"page_number": 1, "text": "Tabella riepilogo | aggiornamento catastale | planimetria | note da verificare\n"},
        {"page_number": 7, "text": "CONFORMITA CATASTALE\nLa planimetria risulta conforme e vi e corrispondenza catastale con lo stato di fatto.\n"},
    ]
    state = server._extract_conformita_catastale_state(pages)
    assert state["status"] == "FOUND"
    assert state["value"] == "CONFORME"
    assert state["chosen_candidate"]["page"] == 7


def test_later_authoritative_catastale_non_compliant_beats_earlier_weak_positive():
    pages = [
        {"page_number": 1, "text": "Nota storica: la planimetria risultava conforme al catasto al precedente accesso.\n"},
        {"page_number": 8, "text": "CONFORMITA CATASTALE\nSi rilevano difformita catastali e planimetria non conforme allo stato di fatto.\n"},
    ]
    state = server._extract_conformita_catastale_state(pages)
    assert state["status"] == "FOUND"
    assert state["value"] == "PRESENTI DIFFORMITÀ"
    assert state["chosen_candidate"]["page"] == 8


def test_catastale_strong_unresolved_conflict_yields_review_required_output():
    pages = [
        {"page_number": 4, "text": "CONFORMITA CATASTALE\nLa planimetria risulta conforme e vi e piena corrispondenza catastale con lo stato di fatto.\n"},
        {"page_number": 5, "text": "PLANIMETRIA CATASTALE\nSi rilevano difformita catastali e mancata corrispondenza con lo stato di fatto.\n"},
    ]
    state = server._extract_conformita_catastale_state(pages)
    assert state["value"] == "DA VERIFICARE"
    assert state["status"] == "LOW_CONFIDENCE"
    assert state["review_required"] is True
    assert state["needs_user_confirmation"] is True
    assert state["conflicts"]


def test_catastale_table_like_mentions_are_downweighted_against_narrative():
    pages = [
        {"page_number": 1, "text": "Riepilogo | aggiornamento catastale | planimetria | catasto | note\n"},
        {"page_number": 3, "text": "CONFORMITA CATASTALE\nLa planimetria risulta conforme al catasto e coerente con lo stato di fatto.\n"},
    ]
    state = server._extract_conformita_catastale_state(pages)
    assert state["value"] == "CONFORME"
    assert state["chosen_candidate"]["is_table_like"] is False


def test_catastale_uncertain_case_exposes_top_two_candidates():
    pages = [
        {"page_number": 6, "text": "CONFORMITA CATASTALE\nLa planimetria risulta conforme al catasto e coerente con lo stato di fatto.\n"},
        {"page_number": 7, "text": "PLANIMETRIA CATASTALE\nSono presenti difformita catastali e planimetria non conforme.\n"},
    ]
    state = server._extract_conformita_catastale_state(pages)
    assert state["review_required"] is True
    assert len(state["top_candidates"]) == 2
    assert {state["top_candidates"][0]["value"], state["top_candidates"][1]["value"]} == {"CONFORME", "PRESENTI DIFFORMITÀ"}


def test_explicit_non_opponibile_clause_beats_generic_occupancy_mention():
    pages = [
        {"page_number": 1, "text": "STATO OCCUPATIVO\nImmobile occupato da terzi conduttori.\n"},
        {"page_number": 7, "text": "TITOLO OPPONIBILE\nIl contratto di locazione non opponibile alla procedura esecutiva.\n"},
    ]
    state = server._extract_opponibilita_occupazione_state(pages)
    assert state["status"] == "FOUND"
    assert state["value"] == "TITOLO NON OPPONIBILE"
    assert state["chosen_candidate"]["page"] == 7


def test_explicit_opponibile_clause_beats_weak_contrary_implication():
    pages = [
        {"page_number": 1, "text": "STATO OCCUPATIVO\nImmobile locato a terzi.\n"},
        {"page_number": 8, "text": "LOCAZIONE OPPONIBILE\nContratto di locazione opponibile alla procedura esecutiva.\n"},
    ]
    state = server._extract_opponibilita_occupazione_state(pages)
    assert state["status"] == "FOUND"
    assert state["value"] == "TITOLO OPPONIBILE"
    assert state["chosen_candidate"]["page"] == 8


def test_occupazione_senza_titolo_is_recognized_distinctly():
    pages = [
        {"page_number": 4, "text": "STATO OCCUPATIVO\nL'immobile risulta occupato da terzi senza titolo opponibile.\n"},
    ]
    state = server._extract_opponibilita_occupazione_state(pages)
    assert state["status"] == "FOUND"
    assert state["value"] == "OCCUPAZIONE SENZA TITOLO"


def test_opponibilita_strong_unresolved_conflict_yields_review_required_output():
    pages = [
        {"page_number": 4, "text": "TITOLO OPPONIBILE\nContratto di locazione opponibile alla procedura.\n"},
        {"page_number": 5, "text": "TITOLO OPPONIBILE\nContratto di locazione non opponibile alla procedura.\n"},
    ]
    state = server._extract_opponibilita_occupazione_state(pages)
    assert state["value"] == "DA VERIFICARE"
    assert state["status"] == "LOW_CONFIDENCE"
    assert state["review_required"] is True
    assert state["needs_user_confirmation"] is True
    assert state["conflicts"]


def test_weak_table_like_opponibilita_mention_is_downweighted_against_narrative():
    pages = [
        {"page_number": 1, "text": "Riepilogo | locazione | conduttore | canone\n"},
        {"page_number": 4, "text": "TITOLO OPPONIBILE\nIl contratto di locazione non opponibile alla procedura esecutiva.\n"},
    ]
    state = server._extract_opponibilita_occupazione_state(pages)
    assert state["value"] == "TITOLO NON OPPONIBILE"
    assert state["chosen_candidate"]["is_table_like"] is False


def test_delivery_liberazione_timing_appears_only_when_explicitly_evidenced():
    explicit_pages = [
        {"page_number": 6, "text": "LIBERAZIONE\nL'immobile deve essere rilasciato entro 120 giorni dal decreto di trasferimento.\n"},
    ]
    explicit_state = server._extract_delivery_timeline_state(explicit_pages)
    assert explicit_state["status"] == "FOUND"
    assert "entro 120 giorni" in explicit_state["value"].lower()

    implicit_pages = [
        {"page_number": 2, "text": "STATO OCCUPATIVO\nImmobile occupato da terzi conduttori.\n"},
    ]
    implicit_state = server._extract_delivery_timeline_state(implicit_pages)
    assert implicit_state["status"] == "NOT_FOUND"


def test_contradictory_raw_opponibilita_hits_do_not_collapse_into_false_blocker():
    pages = [
        {"page_number": 2, "text": "STATO OCCUPATIVO\nImmobile occupato da terzi conduttori.\n"},
        {"page_number": 8, "text": "LOCAZIONE OPPONIBILE\nContratto di locazione opponibile alla procedura esecutiva.\n"},
    ]
    result = _build_result_with_legal_items(
        pages,
        [
            {
                "killer": "Occupato da terzi senza titolo",
                "status": "ROSSO",
                "reason_it": "Segnale grezzo da verificare",
                "evidence": [{"page": 2, "quote": "Immobile occupato da terzi conduttori.", "search_hint": "occupato da terzi"}],
            }
        ],
    )
    item = result["section_9_legal_killers"]["items"][0]
    assert item["theme"] == "occupazione_titolo_opponibilita"
    assert item["theme_resolution"] == "SAFE_CLEAR"
    assert item["decision_bucket"] == "BACKGROUND_NOTE"
    assert result["section_9_legal_killers"]["top_items"] == []
    assert not any(b["label_it"] == "Occupato da terzi senza titolo" for b in result["semaforo_generale"]["top_blockers"])


def test_strong_field_backed_opponibilita_blocker_survives_normalization():
    pages = [
        {"page_number": 1, "text": "STATO OCCUPATIVO\nImmobile occupato da terzi conduttori.\n"},
        {"page_number": 7, "text": "TITOLO OPPONIBILE\nIl contratto di locazione non opponibile alla procedura esecutiva.\n"},
    ]
    result = _build_result_with_legal_items(
        pages,
        [
            {
                "killer": "Occupato da terzi senza titolo",
                "status": "ROSSO",
                "reason_it": "Segnale grezzo coerente con il titolo",
                "evidence": [{"page": 7, "quote": "Il contratto di locazione non opponibile alla procedura esecutiva.", "search_hint": "non opponibile"}],
            }
        ],
    )
    item = result["section_9_legal_killers"]["items"][0]
    assert item["theme_resolution"] == "BLOCKER_CLEAR"
    assert result["section_9_legal_killers"]["top_items"][0]["killer"] == "Occupato da terzi senza titolo"
    assert any(b["label_it"] == "Occupato da terzi senza titolo" for b in result["semaforo_generale"]["top_blockers"])


def test_contradictory_raw_urbanistica_hits_do_not_produce_fake_blocker():
    pages = [
        {"page_number": 4, "text": "REGOLARITA URBANISTICA\nNon risultano abusi edilizi e il bene appare conforme urbanisticamente.\n"},
        {"page_number": 5, "text": "CONFORMITA URBANISTICA\nSi rilevano difformità urbanistiche e opere abusive da sanare.\n"},
    ]
    result = _build_result_with_legal_items(
        pages,
        [
            {
                "killer": "Abuso edilizio",
                "status": "ROSSO",
                "reason_it": "Segnale grezzo da verificare",
                "evidence": [{"page": 5, "quote": "Si rilevano difformità urbanistiche e opere abusive da sanare.", "search_hint": "difformita urbanistiche"}],
            }
        ],
    )
    item = result["section_9_legal_killers"]["items"][0]
    assert item["theme"] == "urbanistica"
    assert item["theme_resolution"] == "REVIEW_REQUIRED"
    assert item["decision_bucket"] == "BACKGROUND_NOTE"
    assert result["section_9_legal_killers"]["top_items"] == []


def test_strong_urbanistica_blocker_survives_when_evidence_is_clear():
    pages = [
        {"page_number": 8, "text": "ABUSI EDILIZI E CONFORMITA URBANISTICA\nSi rilevano difformità urbanistiche e opere abusive da sanare.\n"},
    ]
    result = _build_result_with_legal_items(
        pages,
        [
            {
                "killer": "Abuso edilizio",
                "status": "ROSSO",
                "reason_it": "Criticita urbanistica supportata",
                "evidence": [{"page": 8, "quote": "Si rilevano difformità urbanistiche e opere abusive da sanare.", "search_hint": "abusive da sanare"}],
            }
        ],
    )
    item = result["section_9_legal_killers"]["items"][0]
    assert item["theme_resolution"] == "BLOCKER_CLEAR"
    assert result["section_9_legal_killers"]["top_items"][0]["killer"] == "Abuso edilizio"


def test_contradictory_raw_catastale_hits_do_not_produce_fake_blocker():
    pages = [
        {"page_number": 4, "text": "CONFORMITA CATASTALE\nLa planimetria risulta conforme e vi e piena corrispondenza catastale con lo stato di fatto.\n"},
        {"page_number": 5, "text": "PLANIMETRIA CATASTALE\nSi rilevano difformita catastali e mancata corrispondenza con lo stato di fatto.\n"},
    ]
    result = _build_result_with_legal_items(
        pages,
        [
            {
                "killer": "Difformità catastale rilevata",
                "status": "ROSSO",
                "reason_it": "Segnale grezzo catastale da verificare",
                "evidence": [{"page": 5, "quote": "Si rilevano difformita catastali e mancata corrispondenza con lo stato di fatto.", "search_hint": "difformita catastali"}],
            }
        ],
    )
    item = result["section_9_legal_killers"]["items"][0]
    assert item["theme"] == "catastale"
    assert item["theme_resolution"] == "REVIEW_REQUIRED"
    assert item["decision_bucket"] == "BACKGROUND_NOTE"
    assert result["section_9_legal_killers"]["top_items"] == []


def test_strong_catastale_blocker_survives_when_evidence_is_clear():
    pages = [
        {"page_number": 8, "text": "CONFORMITA CATASTALE\nSi rilevano difformita catastali e planimetria non conforme allo stato di fatto.\n"},
    ]
    result = _build_result_with_legal_items(
        pages,
        [
            {
                "killer": "Difformità catastale rilevata",
                "status": "ROSSO",
                "reason_it": "Criticita catastale supportata",
                "evidence": [{"page": 8, "quote": "Si rilevano difformita catastali e planimetria non conforme allo stato di fatto.", "search_hint": "planimetria non conforme"}],
            }
        ],
    )
    item = result["section_9_legal_killers"]["items"][0]
    assert item["theme_resolution"] == "BLOCKER_CLEAR"
    assert result["section_9_legal_killers"]["top_items"][0]["killer"] == "Difformità catastale rilevata"


def test_legal_killers_section_shape_remains_usable_with_theme_metadata():
    pages = [
        {"page_number": 8, "text": "ABUSI EDILIZI E CONFORMITA URBANISTICA\nSi rilevano difformità urbanistiche e opere abusive da sanare.\n"},
    ]
    result = _build_result_with_legal_items(
        pages,
        [
            {
                "killer": "Abuso edilizio",
                "status": "ROSSO",
                "reason_it": "Criticita urbanistica supportata",
                "evidence": [{"page": 8, "quote": "Si rilevano difformità urbanistiche e opere abusive da sanare.", "search_hint": "abusive da sanare"}],
            }
        ],
    )
    section = result["section_9_legal_killers"]
    assert isinstance(section["items"], list)
    assert isinstance(section["top_items"], list)
    assert isinstance(section["resolver_meta"], dict)
    assert isinstance(section["resolver_meta"]["themes"], list)
    item = section["items"][0]
    assert item["theme"] == "urbanistica"
    assert item["theme_resolution"] == "BLOCKER_CLEAR"
    assert item["source_priority"] == "field_state"


def test_semaforo_top_items_respect_theme_level_normalization():
    pages = [
        {"page_number": 4, "text": "REGOLARITA URBANISTICA\nNon risultano abusi edilizi e il bene appare conforme urbanisticamente.\n"},
        {"page_number": 5, "text": "CONFORMITA URBANISTICA\nSi rilevano difformità urbanistiche e opere abusive da sanare.\n"},
        {"page_number": 8, "text": "TITOLO OPPONIBILE\nIl contratto di locazione non opponibile alla procedura esecutiva.\n"},
    ]
    result = _build_result_with_legal_items(
        pages,
        [
            {
                "killer": "Abuso edilizio",
                "status": "ROSSO",
                "reason_it": "Segnale urbanistico contraddittorio",
                "evidence": [{"page": 5, "quote": "Si rilevano difformità urbanistiche e opere abusive da sanare.", "search_hint": "difformita urbanistiche"}],
            },
            {
                "killer": "Occupato da terzi senza titolo",
                "status": "ROSSO",
                "reason_it": "Titolo non opponibile supportato",
                "evidence": [{"page": 8, "quote": "Il contratto di locazione non opponibile alla procedura esecutiva.", "search_hint": "non opponibile"}],
            },
        ],
    )
    top_items = result["section_9_legal_killers"]["top_items"]
    assert len(top_items) == 1
    assert top_items[0]["killer"] == "Occupato da terzi senza titolo"
    blocker_labels = [b["label_it"] for b in result["semaforo_generale"]["top_blockers"]]
    assert "Occupato da terzi senza titolo" in blocker_labels
    assert "Abuso edilizio" not in blocker_labels


def test_vetted_quantified_burden_survives_to_final_money_box_output():
    pages = [
        {"page_number": 3, "text": "Oneri di regolarizzazione urbanistica € 12.000.\n"},
    ]
    result = _build_result_with_money_box_items(
        pages,
        lambda items: items.__setitem__(
            0,
            {
                **items[0],
                "stima_euro": 12000,
                "type": "ESTIMATE",
                "stima_nota": "Oneri di regolarizzazione urbanistica € 12.000.",
                "fonte_perizia": {"value": "Perizia", "evidence": [{"page": 3, "quote": "Oneri di regolarizzazione urbanistica € 12.000.", "search_hint": "regolarizzazione urbanistica"}]},
            },
        ),
    )
    item_a = next(item for item in result["money_box"]["items"] if item["code"] == "A")
    assert item_a["stima_euro"] == 12000
    assert result["money_box"]["total_extra_costs"]["range"]["min"] == 12000
    assert result["money_box"]["total_extra_costs"]["range"]["max"] == 12000


def test_secondary_internal_burden_is_mapped_into_canonical_money_box_slot():
    pages = [
        {"page_number": 4, "text": "Spese tecniche per regolarizzazione urbanistica da quantificare.\n"},
    ]
    def mutate(items):
        items.append(
            {
                "code": "S3C_URB_1",
                "label_it": "Spese tecniche per regolarizzazione urbanistica",
                "label_en": "Technical fees for urban regularization",
                "type": "QUALITATIVE",
                "stima_euro": "TBD",
                "stima_nota": "Spese tecniche per regolarizzazione urbanistica da quantificare",
                "fonte_perizia": {"value": "Perizia", "evidence": [{"page": 4, "quote": "Spese tecniche per regolarizzazione urbanistica da quantificare.", "search_hint": "spese tecniche"}]},
            }
        )
    result = _build_result_with_money_box_items(pages, mutate)
    item_b = next(item for item in result["money_box"]["items"] if item["code"] == "B")
    assert item_b["type"] == "QUALITATIVE"
    assert "Spese tecniche per regolarizzazione urbanistica" in item_b["stima_nota"]
    assert item_b["fonte_perizia"]["evidence"]


def test_qualitative_only_burden_remains_qualitative_when_amount_is_unsupported():
    pages = [
        {"page_number": 2, "text": "Morosità condominiale da verificare presso l'amministratore.\n"},
    ]
    def mutate(items):
        items.append(
            {
                "code": "S3C_CONDO_1",
                "label_it": "Morosità condominiale",
                "label_en": "Condo arrears",
                "type": "QUALITATIVE",
                "stima_euro": "TBD",
                "stima_nota": "Morosità condominiale da verificare presso l'amministratore",
                "fonte_perizia": {"value": "Perizia", "evidence": [{"page": 2, "quote": "Morosità condominiale da verificare presso l'amministratore.", "search_hint": "morosita condominiale"}]},
            }
        )
    result = _build_result_with_money_box_items(pages, mutate)
    item_e = next(item for item in result["money_box"]["items"] if item["code"] == "E")
    assert item_e["type"] == "QUALITATIVE"
    assert item_e["stima_euro"] == "TBD"
    assert result["money_box"]["total_extra_costs"]["min"] == "NON_QUANTIFICATO_IN_PERIZIA"


def test_weak_evidence_does_not_generate_fake_numeric_total():
    pages = [
        {"page_number": 1, "text": "Tabella costi | sanatoria | € 5.000 | valore finale € 120.000\n"},
    ]
    result = _build_result_with_money_box_items(
        pages,
        lambda items: items.__setitem__(
            0,
            {
                **items[0],
                "stima_euro": 5000,
                "type": "ESTIMATE",
                "stima_nota": "Tabella costi | sanatoria | € 5.000 | valore finale € 120.000",
                "fonte_perizia": {"value": "Perizia", "evidence": [{"page": 1, "quote": "Tabella costi | sanatoria | € 5.000 | valore finale € 120.000", "search_hint": "sanatoria"}]},
            },
        ),
    )
    item_a = next(item for item in result["money_box"]["items"] if item["code"] == "A")
    assert item_a["stima_euro"] == "TBD"
    assert result["money_box"]["total_extra_costs"]["min"] == "NON_QUANTIFICATO_IN_PERIZIA"


def test_irrelevant_or_seller_side_amount_does_not_mix_into_buyer_money_box():
    pages = [
        {"page_number": 6, "text": "Valore finale di stima € 120.000.\n"},
    ]
    result = _build_result_with_money_box_items(
        pages,
        lambda items: items.__setitem__(
            0,
            {
                **items[0],
                "stima_euro": 120000,
                "type": "ESTIMATE",
                "stima_nota": "Valore finale di stima € 120.000.",
                "fonte_perizia": {"value": "Perizia", "evidence": [{"page": 6, "quote": "Valore finale di stima € 120.000.", "search_hint": "valore finale"}]},
            },
        ),
    )
    item_a = next(item for item in result["money_box"]["items"] if item["code"] == "A")
    assert item_a["stima_euro"] == "TBD"
    assert result["money_box"]["total_extra_costs"]["min"] == "NON_QUANTIFICATO_IN_PERIZIA"


def test_broad_wording_variants_are_recognized_when_linkage_is_strong():
    pages = [
        {"page_number": 5, "text": "Morosità condominiale pari a € 3.200 a carico dell'acquirente.\n"},
    ]
    def mutate(items):
        items.append(
            {
                "code": "S3C_CONDO_2",
                "label_it": "Morosità condominiale",
                "label_en": "Condo arrears",
                "type": "ESTIMATE",
                "stima_euro": 3200,
                "stima_nota": "Morosità condominiale pari a € 3.200",
                "fonte_perizia": {"value": "Perizia", "evidence": [{"page": 5, "quote": "Morosità condominiale pari a € 3.200 a carico dell'acquirente.", "search_hint": "morosita condominiale"}]},
            }
        )
    result = _build_result_with_money_box_items(pages, mutate)
    item_e = next(item for item in result["money_box"]["items"] if item["code"] == "E")
    assert item_e["stima_euro"] == 3200
    assert result["money_box"]["total_extra_costs"]["range"]["min"] == 3200


def test_broad_wording_variants_do_not_create_false_numeric_mapping_when_linkage_is_weak():
    pages = [
        {"page_number": 2, "text": "Riepilogo | morosità condominiale | € 3.200 | valore lotto € 98.000\n"},
    ]
    def mutate(items):
        items.append(
            {
                "code": "S3C_CONDO_3",
                "label_it": "Morosità condominiale",
                "label_en": "Condo arrears",
                "type": "ESTIMATE",
                "stima_euro": 3200,
                "stima_nota": "Riepilogo morosità condominiale",
                "fonte_perizia": {"value": "Perizia", "evidence": [{"page": 2, "quote": "Riepilogo | morosità condominiale | € 3.200 | valore lotto € 98.000", "search_hint": "morosita condominiale"}]},
            }
        )
    result = _build_result_with_money_box_items(pages, mutate)
    item_e = next(item for item in result["money_box"]["items"] if item["code"] == "E")
    assert item_e["stima_euro"] == "TBD"
    assert result["money_box"]["total_extra_costs"]["min"] == "NON_QUANTIFICATO_IN_PERIZIA"


def test_customer_facing_money_box_structure_remains_usable():
    pages = [
        {"page_number": 3, "text": "Spese tecniche per regolarizzazione urbanistica da quantificare.\n"},
    ]
    def mutate(items):
        items.append(
            {
                "code": "S3C_URB_2",
                "label_it": "Spese tecniche per regolarizzazione urbanistica",
                "label_en": "Technical fees for urban regularization",
                "type": "QUALITATIVE",
                "stima_euro": "TBD",
                "stima_nota": "Spese tecniche per regolarizzazione urbanistica da quantificare",
                "fonte_perizia": {"value": "Perizia", "evidence": [{"page": 3, "quote": "Spese tecniche per regolarizzazione urbanistica da quantificare.", "search_hint": "spese tecniche"}]},
            }
        )
    result = _build_result_with_money_box_items(pages, mutate)
    codes = [item["code"] for item in result["money_box"]["items"] if isinstance(item, dict) and item.get("code")]
    assert {"A", "B", "C", "D", "E", "F", "G", "H"}.issubset(set(codes))
    assert isinstance(result["money_box"]["qualitative_burdens"], list)
    assert isinstance(result["section_3_money_box"]["items"], list)
    assert isinstance(result["section_3_money_box"]["totale_extra_budget"], dict)


@pytest.mark.anyio
async def test_occupazione_user_confirmation_is_stored_and_applied(fake_db, monkeypatch):
    pages = [
        {"page_number": 4, "text": "STATO OCCUPATIVO\nL'immobile risulta libero e nella disponibilità della procedura.\n"},
        {"page_number": 5, "text": "STATO OCCUPATIVO\nL'immobile risulta occupato dal debitore alla data del sopralluogo.\n"},
    ]
    result = _build_result_for_pages(pages)
    _seed_analysis(fake_db, analysis_id="analysis_occ", user_id="user_1", result=result)

    async def fake_require_auth(_request):
        return _user()

    monkeypatch.setattr(server, "require_auth", fake_require_auth)
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/analysis/perizia/analysis_occ/confirmations",
            json={"check_type": "stato_occupativo", "value": "LIBERO", "notes": "Confermato da utente"},
        )
        assert resp.status_code == 200
        assert len(fake_db.perizia_confirmations.items) == 1
        record = fake_db.perizia_confirmations.items[0]
        assert record["check_type"] == "stato_occupativo"
        assert record["user_confirmed_value"] == "LIBERO"
        assert record["candidate_1_value"]
        detail = await client.get("/api/analysis/perizia/analysis_occ")
        assert detail.status_code == 200
        field_state = detail.json()["result"]["field_states"]["stato_occupativo"]
        assert field_state["status"] == "USER_PROVIDED"
        assert field_state["value"] == "LIBERO"


@pytest.mark.anyio
async def test_urbanistica_user_confirmation_is_stored_and_applied_without_breaking_consumers(fake_db, monkeypatch):
    pages = [
        {"page_number": 4, "text": "REGOLARITA URBANISTICA\nNon risultano abusi edilizi e il bene appare conforme urbanisticamente.\n"},
        {"page_number": 5, "text": "CONFORMITA URBANISTICA\nSi rilevano difformità urbanistiche e opere abusive da sanare.\n"},
    ]
    result = _build_result_for_pages(pages)
    _seed_analysis(fake_db, analysis_id="analysis_urb", user_id="user_1", result=result)

    async def fake_require_auth(_request):
        return _user()

    monkeypatch.setattr(server, "require_auth", fake_require_auth)
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/analysis/perizia/analysis_urb/confirmations",
            json={"check_type": "regolarita_urbanistica", "value": "NON EMERGONO ABUSI", "notes": "Confermato da tecnico"},
        )
        assert resp.status_code == 200
        assert len(fake_db.perizia_confirmations.items) == 1
        record = fake_db.perizia_confirmations.items[0]
        assert record["check_type"] == "regolarita_urbanistica"
        assert record["field_key"] == "field_states.regolarita_urbanistica"
        assert record["user_confirmed_value"] == "NON EMERGONO ABUSI"
        assert record["notes"] == "Confermato da tecnico"
        assert record["candidate_1_value"]
        detail = await client.get("/api/analysis/perizia/analysis_urb")
        assert detail.status_code == 200
        payload = detail.json()["result"]
        field_state = payload["field_states"]["regolarita_urbanistica"]
        assert field_state["status"] == "USER_PROVIDED"
        assert field_state["value"] == "NON EMERGONO ABUSI"
        assert payload["abusi_edilizi_conformita"]["conformita_urbanistica"]["status"] == "CONFORME"
        assert payload["abusi_edilizi_conformita"]["conformita_urbanistica"]["detail_it"] == "NON EMERGONO ABUSI"


@pytest.mark.anyio
async def test_catastale_user_confirmation_is_stored_and_applied_without_breaking_consumers(fake_db, monkeypatch):
    pages = [
        {"page_number": 4, "text": "CONFORMITA CATASTALE\nLa planimetria risulta conforme e vi e piena corrispondenza catastale con lo stato di fatto.\n"},
        {"page_number": 5, "text": "PLANIMETRIA CATASTALE\nSi rilevano difformita catastali e mancata corrispondenza con lo stato di fatto.\n"},
    ]
    result = _build_result_for_pages(pages)
    _seed_analysis(fake_db, analysis_id="analysis_cat", user_id="user_1", result=result)

    async def fake_require_auth(_request):
        return _user()

    monkeypatch.setattr(server, "require_auth", fake_require_auth)
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/analysis/perizia/analysis_cat/confirmations",
            json={"check_type": "conformita_catastale", "value": "CONFORME", "notes": "Confermato da geometra"},
        )
        assert resp.status_code == 200
        assert len(fake_db.perizia_confirmations.items) == 1
        record = fake_db.perizia_confirmations.items[0]
        assert record["check_type"] == "conformita_catastale"
        assert record["field_key"] == "field_states.conformita_catastale"
        assert record["user_confirmed_value"] == "CONFORME"
        assert record["notes"] == "Confermato da geometra"
        assert record["candidate_1_value"]
        detail = await client.get("/api/analysis/perizia/analysis_cat")
        assert detail.status_code == 200
        payload = detail.json()["result"]
        field_state = payload["field_states"]["conformita_catastale"]
        assert field_state["status"] == "USER_PROVIDED"
        assert field_state["value"] == "CONFORME"
        assert payload["abusi_edilizi_conformita"]["conformita_catastale"]["status"] == "CONFORME"
        assert payload["abusi_edilizi_conformita"]["conformita_catastale"]["detail_it"] == "CONFORME"


@pytest.mark.anyio
async def test_opponibilita_user_confirmation_is_stored_and_applied_without_breaking_consumers(fake_db, monkeypatch):
    pages = [
        {"page_number": 1, "text": "STATO OCCUPATIVO\nImmobile occupato da terzi conduttori.\n"},
        {"page_number": 7, "text": "TITOLO OPPONIBILE\nIl contratto di locazione non opponibile alla procedura esecutiva.\n"},
    ]
    result = _build_result_for_pages(pages)
    _seed_analysis(fake_db, analysis_id="analysis_opp", user_id="user_1", result=result)

    async def fake_require_auth(_request):
        return _user()

    monkeypatch.setattr(server, "require_auth", fake_require_auth)
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/analysis/perizia/analysis_opp/confirmations",
            json={"check_type": "opponibilita_occupazione", "value": "TITOLO NON OPPONIBILE", "notes": "Confermato da legale"},
        )
        assert resp.status_code == 200
        assert len(fake_db.perizia_confirmations.items) == 1
        record = fake_db.perizia_confirmations.items[0]
        assert record["check_type"] == "opponibilita_occupazione"
        assert record["field_key"] == "field_states.opponibilita_occupazione"
        assert record["user_confirmed_value"] == "TITOLO NON OPPONIBILE"
        assert record["notes"] == "Confermato da legale"
        detail = await client.get("/api/analysis/perizia/analysis_opp")
        assert detail.status_code == 200
        payload = detail.json()["result"]
        field_state = payload["field_states"]["opponibilita_occupazione"]
        assert field_state["status"] == "USER_PROVIDED"
        assert field_state["value"] == "TITOLO NON OPPONIBILE"
        assert payload["stato_occupativo"]["status"]


@pytest.mark.anyio
async def test_existing_address_confirmation_is_logged(fake_db, monkeypatch):
    pages = [{"page_number": 1, "text": "TRIBUNALE DI ROMA\nUbicazione Via Roma 10, Roma\n"}]
    result = _build_result_for_pages(pages)
    _seed_analysis(fake_db, analysis_id="analysis_addr", user_id="user_1", result=result)

    async def fake_require_auth(_request):
        return _user()

    monkeypatch.setattr(server, "require_auth", fake_require_auth)
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.patch(
            "/api/analysis/perizia/analysis_addr/headline",
            json={"address": "Via Roma 10, Roma"},
        )
        assert resp.status_code == 200
        assert len(fake_db.perizia_confirmations.items) == 1
        record = fake_db.perizia_confirmations.items[0]
        assert record["check_type"] == "address"
        assert record["field_key"] == "field_states.address"


@pytest.mark.anyio
async def test_address_confirmation_endpoint_preserves_notes_and_export_includes_them(fake_db, monkeypatch):
    pages = [{"page_number": 1, "text": "TRIBUNALE DI ROMA\nUbicazione Via Roma 10, Roma\n"}]
    result = _build_result_for_pages(pages)
    _seed_analysis(fake_db, analysis_id="analysis_addr_confirm", user_id="user_1", result=result)
    fake_db.users.items.append(
        {
            "user_id": "user_admin",
            "email": "admin@example.com",
            "name": "Admin",
            "plan": "enterprise",
            "is_master_admin": True,
            "quota": {},
        }
    )
    admin_session = _seed_session(
        fake_db,
        {
            "user_id": "user_admin",
            "email": "admin@example.com",
            "name": "Admin",
            "plan": "enterprise",
            "is_master_admin": True,
            "quota": {},
        },
        session_token="sess_admin_export",
    )

    original_require_auth = server.require_auth

    async def fake_require_auth(_request):
        return _user()

    monkeypatch.setattr(server, "require_auth", fake_require_auth)
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        confirm_resp = await client.post(
            "/api/analysis/perizia/analysis_addr_confirm/confirmations",
            json={"check_type": "address", "value": "Via Roma 10, Roma", "notes": "indirizzo verificato manualmente"},
        )
        assert confirm_resp.status_code == 200
        assert len(fake_db.perizia_confirmations.items) == 1
        record = fake_db.perizia_confirmations.items[0]
        assert record["check_type"] == "address"
        assert record["notes"] == "indirizzo verificato manualmente"

        monkeypatch.setattr(server, "require_auth", original_require_auth)
        export_resp = await client.get(
            "/api/admin/perizia-confirmations/export.xlsx",
            headers={"Authorization": f"Bearer {admin_session}"},
        )
        assert export_resp.status_code == 200
        zf = zipfile.ZipFile(io.BytesIO(export_resp.content))
        sheet_xml = zf.read("xl/worksheets/sheet1.xml").decode("utf-8")
        assert "indirizzo verificato manualmente" in sheet_xml


@pytest.mark.anyio
async def test_admin_export_includes_urbanistica_confirmation_rows(fake_db, monkeypatch):
    pages = [
        {"page_number": 4, "text": "REGOLARITA URBANISTICA\nNon risultano abusi edilizi e il bene appare conforme urbanisticamente.\n"},
        {"page_number": 5, "text": "CONFORMITA URBANISTICA\nSi rilevano difformità urbanistiche e opere abusive da sanare.\n"},
    ]
    result = _build_result_for_pages(pages)
    _seed_analysis(fake_db, analysis_id="analysis_urb_export", user_id="user_1", result=result)
    admin_session = _seed_session(
        fake_db,
        {
            "user_id": "user_admin",
            "email": "admin@example.com",
            "name": "Admin",
            "plan": "enterprise",
            "is_master_admin": True,
            "quota": {},
        },
        session_token="sess_admin_urbanistica_export",
    )

    original_require_auth = server.require_auth

    async def fake_require_auth(_request):
        return _user()

    monkeypatch.setattr(server, "require_auth", fake_require_auth)
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        confirm_resp = await client.post(
            "/api/analysis/perizia/analysis_urb_export/confirmations",
            json={"check_type": "regolarita_urbanistica", "value": "PRESENTI DIFFORMITÀ", "notes": "Difformita confermate"},
        )
        assert confirm_resp.status_code == 200

        monkeypatch.setattr(server, "require_auth", original_require_auth)
        export_resp = await client.get(
            "/api/admin/perizia-confirmations/export.xlsx",
            headers={"Authorization": f"Bearer {admin_session}"},
        )
        assert export_resp.status_code == 200
        zf = zipfile.ZipFile(io.BytesIO(export_resp.content))
        sheet_xml = zf.read("xl/worksheets/sheet1.xml").decode("utf-8")
        assert "regolarita_urbanistica" in sheet_xml
        assert "PRESENTI DIFFORMITÀ" in sheet_xml
        assert "Difformita confermate" in sheet_xml


@pytest.mark.anyio
async def test_admin_export_includes_catastale_confirmation_rows(fake_db, monkeypatch):
    pages = [
        {"page_number": 4, "text": "CONFORMITA CATASTALE\nLa planimetria risulta conforme e vi e piena corrispondenza catastale con lo stato di fatto.\n"},
        {"page_number": 5, "text": "PLANIMETRIA CATASTALE\nSi rilevano difformita catastali e mancata corrispondenza con lo stato di fatto.\n"},
    ]
    result = _build_result_for_pages(pages)
    _seed_analysis(fake_db, analysis_id="analysis_cat_export", user_id="user_1", result=result)
    admin_session = _seed_session(
        fake_db,
        {
            "user_id": "user_admin",
            "email": "admin@example.com",
            "name": "Admin",
            "plan": "enterprise",
            "is_master_admin": True,
            "quota": {},
        },
        session_token="sess_admin_catastale_export",
    )

    original_require_auth = server.require_auth

    async def fake_require_auth(_request):
        return _user()

    monkeypatch.setattr(server, "require_auth", fake_require_auth)
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        confirm_resp = await client.post(
            "/api/analysis/perizia/analysis_cat_export/confirmations",
            json={"check_type": "conformita_catastale", "value": "PRESENTI DIFFORMITÀ", "notes": "Catastale confermato"},
        )
        assert confirm_resp.status_code == 200

        monkeypatch.setattr(server, "require_auth", original_require_auth)
        export_resp = await client.get(
            "/api/admin/perizia-confirmations/export.xlsx",
            headers={"Authorization": f"Bearer {admin_session}"},
        )
        assert export_resp.status_code == 200
        zf = zipfile.ZipFile(io.BytesIO(export_resp.content))
        sheet_xml = zf.read("xl/worksheets/sheet1.xml").decode("utf-8")
        assert "conformita_catastale" in sheet_xml
        assert "PRESENTI DIFFORMITÀ" in sheet_xml
        assert "Catastale confermato" in sheet_xml


@pytest.mark.anyio
async def test_admin_export_includes_opponibilita_confirmation_rows(fake_db, monkeypatch):
    pages = [
        {"page_number": 1, "text": "STATO OCCUPATIVO\nImmobile occupato da terzi conduttori.\n"},
        {"page_number": 7, "text": "TITOLO OPPONIBILE\nIl contratto di locazione non opponibile alla procedura esecutiva.\n"},
    ]
    result = _build_result_for_pages(pages)
    _seed_analysis(fake_db, analysis_id="analysis_opp_export", user_id="user_1", result=result)
    admin_session = _seed_session(
        fake_db,
        {
            "user_id": "user_admin",
            "email": "admin@example.com",
            "name": "Admin",
            "plan": "enterprise",
            "is_master_admin": True,
            "quota": {},
        },
        session_token="sess_admin_opponibilita_export",
    )

    original_require_auth = server.require_auth

    async def fake_require_auth(_request):
        return _user()

    monkeypatch.setattr(server, "require_auth", fake_require_auth)
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        confirm_resp = await client.post(
            "/api/analysis/perizia/analysis_opp_export/confirmations",
            json={"check_type": "opponibilita_occupazione", "value": "TITOLO NON OPPONIBILE", "notes": "Opponibilita confermata"},
        )
        assert confirm_resp.status_code == 200

        monkeypatch.setattr(server, "require_auth", original_require_auth)
        export_resp = await client.get(
            "/api/admin/perizia-confirmations/export.xlsx",
            headers={"Authorization": f"Bearer {admin_session}"},
        )
        assert export_resp.status_code == 200
        zf = zipfile.ZipFile(io.BytesIO(export_resp.content))
        sheet_xml = zf.read("xl/worksheets/sheet1.xml").decode("utf-8")
        assert "opponibilita_occupazione" in sheet_xml
        assert "TITOLO NON OPPONIBILE" in sheet_xml
        assert "Opponibilita confermata" in sheet_xml


def test_catastale_resolver_backed_field_does_not_leak_human_phrase_into_status():
    pages = [
        {"page_number": 4, "text": "CONFORMITA CATASTALE\nLa planimetria risulta conforme e vi e piena corrispondenza catastale con lo stato di fatto.\n"},
    ]
    result = _build_result_for_pages(pages)
    field_state = result["field_states"]["conformita_catastale"]
    legacy_cat = result["abusi_edilizi_conformita"]["conformita_catastale"]
    assert field_state["status"] == "FOUND"
    assert field_state["value"] == "CONFORME"
    assert legacy_cat["status"] == "CONFORME"
    assert legacy_cat["detail_it"] == "CONFORME"


def test_opponibilita_cluster_resolver_backed_fields_do_not_leak_human_phrase_into_status():
    pages = [
        {"page_number": 6, "text": "LIBERAZIONE\nL'immobile deve essere rilasciato entro 120 giorni dal decreto di trasferimento.\n"},
        {"page_number": 7, "text": "TITOLO OPPONIBILE\nIl contratto di locazione non opponibile alla procedura esecutiva.\n"},
    ]
    result = _build_result_for_pages(pages)
    opp_state = result["field_states"]["opponibilita_occupazione"]
    delivery_state = result["field_states"]["delivery_timeline"]
    assert opp_state["status"] == "FOUND"
    assert opp_state["value"] == "TITOLO NON OPPONIBILE"
    assert delivery_state["status"] == "FOUND"
    assert "entro 120 giorni" in delivery_state["value"].lower()


@pytest.mark.anyio
async def test_admin_export_generates_xlsx_with_expected_rows_and_columns(fake_db):
    pages = [{"page_number": 1, "text": "TRIBUNALE DI ROMA\nUbicazione Via Roma 10, Roma\nSTATO OCCUPATIVO\nL'immobile risulta libero.\n"}]
    result = _build_result_for_pages(pages)
    _seed_analysis(
        fake_db,
        analysis_id="analysis_export",
        user_id="user_1",
        result=result,
        headline_overrides={"address": "Via Roma 10, Roma"},
        field_overrides={"stato_occupativo": "LIBERO"},
    )
    fake_db.users.items.append({"user_id": "user_1", "email": "user@example.com"})
    session_token = _seed_session(
        fake_db,
        {
            "user_id": "user_admin",
            "email": "admin@example.com",
            "name": "Admin",
            "plan": "enterprise",
            "is_master_admin": True,
            "quota": {},
        },
        session_token="sess_export_ok",
    )
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/api/admin/perizia-confirmations/export.xlsx",
            headers={"Authorization": f"Bearer {session_token}"},
        )
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        sheet_xml = zf.read("xl/worksheets/sheet1.xml").decode("utf-8")
        assert "confirmation_id" in sheet_xml
        assert "check_type" in sheet_xml
        assert "Via Roma 10, Roma" in sheet_xml
        assert "stato_occupativo" in sheet_xml


@pytest.mark.anyio
async def test_admin_export_allows_configured_master_admin_email(fake_db):
    session_token = _seed_session(
        fake_db,
        {
            "user_id": "user_admin",
            "email": "admin@example.com",
            "name": "Admin",
            "plan": "enterprise",
            "is_master_admin": True,
            "quota": {},
        },
        session_token="sess_export_configured_admin",
    )
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/api/admin/perizia-confirmations/export.xlsx",
            headers={"Authorization": f"Bearer {session_token}"},
        )
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_admin_export_forbidden_for_non_master_admin(fake_db):
    session_token = _seed_session(
        fake_db,
        {
            "user_id": "user_2",
            "email": "user@example.com",
            "name": "User",
            "plan": "pro",
            "is_master_admin": False,
            "quota": {},
        },
        session_token="sess_export_forbidden",
    )
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/api/admin/perizia-confirmations/export.xlsx",
            headers={"Authorization": f"Bearer {session_token}"},
        )
    assert resp.status_code == 403
