import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from perizia_section_authority import (  # noqa: E402
    build_section_authority_map,
    classify_page_authority,
    classify_quote_authority,
    detect_answer_point,
    detect_domain_hints,
)


def _page(section_map, page_number):
    for row in section_map["pages"]:
        if row["page"] == page_number:
            return row
    raise AssertionError(f"missing page {page_number}")


def test_instruction_answer_boundary_detects_low_context_then_high_factual_answer():
    pages = [
        {
            "page_number": 1,
            "text": "INDICAZIONI PERITALI\nIl giudice dispone che l'esperto verifichi lo stato di occupazione.",
        },
        {"page_number": 2, "text": "RISPOSTE ALLE INDICAZIONI PERITALI"},
        {
            "page_number": 3,
            "text": "RISPOSTA AL PUNTO N. 12: Il bene e occupato dai debitori esecutati.",
        },
    ]

    section_map = build_section_authority_map(pages)

    p1 = _page(section_map, 1)
    assert p1["zone"] == "INSTRUCTION_BLOCK"
    assert p1["authority_level"] == "LOW_CONTEXT_ONLY"
    assert p1["is_instruction_like"] is True

    p3 = _page(section_map, 3)
    assert p3["zone"] == "ANSWER_BLOCK"
    assert p3["authority_level"] == "HIGH_FACTUAL"
    assert p3["answer_point"] == 12
    assert "occupancy" in p3["domain_hints"]


def test_generic_instruction_occupancy_before_answer_is_low_context_only():
    row = classify_page_authority(
        1,
        "Quesito: accerti se l'immobile sia libero od occupato e dica se il titolo sia opponibile.",
    )

    assert row["authority_level"] == "LOW_CONTEXT_ONLY"
    assert row["zone"] in {"QUESTION_BLOCK", "INSTRUCTION_BLOCK"}
    assert row["is_instruction_like"] is True
    assert row["is_answer_like"] is False
    assert "occupancy" in row["domain_hints"]
    assert "opponibilita" in row["domain_hints"]


def test_factual_occupancy_in_answer_context_is_high_factual():
    row = classify_page_authority(
        4,
        "L'immobile risulta occupato dai debitori esecutati.",
        context={"seen_answer": True},
    )

    assert row["zone"] == "ANSWER_BLOCK"
    assert row["authority_level"] == "HIGH_FACTUAL"
    assert "occupancy" in row["domain_hints"]


def test_historical_procedural_lot_reference_is_contextual_not_final_lot():
    row = classify_page_authority(
        5,
        "Si richiama il Lotto 2 della procedura portante, precedente lotto di altra procedura riunita.",
    )

    assert row["zone"] == "ANNEX_OR_CONTEXT"
    assert row["authority_level"] == "LOW_CONTEXT_ONLY"
    assert "lots" in row["domain_hints"]
    assert "procedure_context" in row["domain_hints"]


def test_final_lotto_unico_is_high_authority_lot_formation():
    row = classify_page_authority(
        9,
        "FORMAZIONE LOTTI\nLOTTO UNICO\nValore finale di stima euro 120.000,00.",
    )

    assert row["zone"] == "FINAL_LOT_FORMATION"
    assert row["authority_level"] == "HIGH_FACTUAL"
    assert "lots" in row["domain_hints"]
    assert "valuation" in row["domain_hints"]
    assert "money_valuation" in row["domain_hints"]


def test_final_valuation_adequamenti_and_valore_finale_is_high_factual():
    row = classify_page_authority(
        12,
        "Adeguamenti e correzioni della stima\nValore finale di stima: Euro 182.800,00",
    )

    assert row["zone"] == "FINAL_VALUATION"
    assert row["authority_level"] == "HIGH_FACTUAL"
    assert "valuation" in row["domain_hints"]
    assert "money_valuation" in row["domain_hints"]


def test_final_lot_vendibile_unico_lotto_is_high_factual():
    row = classify_page_authority(
        20,
        "FORMAZIONE LOTTI\nLOTTO UNICO\nIl compendio e vendibile in un unico lotto.",
    )

    assert row["zone"] == "FINAL_LOT_FORMATION"
    assert row["authority_level"] == "HIGH_FACTUAL"
    assert "lots" in row["domain_hints"]


def test_formalities_section_gets_legal_formalities_hint():
    row = classify_page_authority(
        11,
        "Formalita pregiudizievoli\nIscrizioni ipotecarie, trascrizioni, ipoteca e pignoramento.",
    )

    assert row["zone"] == "FORMALITIES_TABLE"
    assert row["authority_level"] == "HIGH_FACTUAL"
    assert "legal_formalities" in row["domain_hints"]


def test_generic_ipoteca_alone_does_not_create_formalities_table():
    row = classify_page_authority(8, "Il bene risulta gravato da ipoteca secondo gli atti disponibili.")

    assert row["zone"] != "FORMALITIES_TABLE"
    assert row["authority_level"] in {"MEDIUM_FACTUAL", "UNKNOWN"}


def test_money_cost_section_gets_money_and_technical_hints():
    hints = detect_domain_hints(
        "Spese tecniche per CILA tardiva, sanzione, Docfa e tipo mappale: € 2.000,00."
    )

    assert "money_cost_signal" in hints
    assert "urbanistica" in hints
    assert "catasto" in hints


def test_rendita_catastale_is_not_cost_signal():
    hints = detect_domain_hints(
        "categoria A/3, classe 2, vani 5, rendita catastale 387,34 Euro, foglio 1 particella 2."
    )

    assert "money_rendita_catastale" in hints
    assert "money_cost_signal" not in hints


def test_incarico_professionista_cost_context_is_not_instruction_like():
    row = classify_page_authority(
        77,
        "Spese tecniche: incarico a professionista abilitato e versamenti da effettuare in favore del Catasto - Agenzia. Euro 800,00",
        context={"seen_answer": True},
    )

    assert row["is_instruction_like"] is False
    assert "money_cost_signal" in row["domain_hints"]


def test_quote_classification_uses_page_zone_for_same_word():
    section_map = build_section_authority_map(
        [
            {
                "page_number": 1,
                "text": "INDICAZIONI PERITALI\nverifichi lo stato di occupazione e opponibilita.",
            },
            {
                "page_number": 3,
                "text": "RISPOSTA AL PUNTO N. 12\nL'immobile risulta occupato dai debitori esecutati.",
            },
        ]
    )

    instruction_quote = classify_quote_authority(1, "occupazione", section_map)
    answer_quote = classify_quote_authority(3, "occupazione", section_map)

    assert instruction_quote["section_zone"] == "INSTRUCTION_BLOCK"
    assert instruction_quote["authority_level"] == "LOW_CONTEXT_ONLY"
    assert instruction_quote["domain_hint"] == "occupancy"
    assert instruction_quote["is_instruction_like"] is True

    assert answer_quote["section_zone"] == "ANSWER_BLOCK"
    assert answer_quote["authority_level"] == "HIGH_FACTUAL"
    assert answer_quote["domain_hint"] == "occupancy"
    assert answer_quote["is_answer_like"] is True


def test_answer_point_detection_is_generic():
    assert detect_answer_point("RISPOSTA AL QUESITO N. 7 - Catasto") == 7
    assert detect_answer_point("RISPOSTA N. 15") == 15
    assert detect_answer_point("RISPOSTA AL PUNTO 12") == 12
    assert detect_answer_point("RISPOSTA AL PUNTO N° 12") == 12
    assert detect_answer_point("RISPOSTA AL PUNTO NR. 12") == 12
    assert detect_answer_point("12. STATO DI POSSESSO") == 12
    assert detect_answer_point("12 - STATO DI POSSESSO") == 12
    assert detect_answer_point("Senza punto esplicito") is None


def test_answer_heading_does_not_turn_all_later_pages_into_answer_block():
    section_map = build_section_authority_map(
        [
            {"page": 1, "text": "RISPOSTA AL PUNTO N. 1\nIl bene risulta occupato."},
            {"page": 2, "text": "La relazione continua con un accertamento fattuale."},
            {"page": 3, "text": "Pagina con allegati e dati catastali senza risposta diretta."},
        ]
    )

    assert _page(section_map, 1)["zone"] == "ANSWER_BLOCK"
    assert _page(section_map, 2)["zone"] == "UNKNOWN_FACTUAL"
    assert _page(section_map, 3)["zone"] != "ANSWER_BLOCK"


def test_section_authority_map_is_json_serializable_for_mongo_and_persistence():
    section_map = build_section_authority_map(
        [
            {"page": 1, "text": "INDICAZIONI PERITALI verifichi occupazione."},
            {"page": 2, "text": "RISPOSTA AL PUNTO N. 1 Il bene e libero."},
        ]
    )

    encoded = json.dumps(section_map, ensure_ascii=False)
    decoded = json.loads(encoded)
    assert decoded["schema_version"] == "perizia_section_authority_v1"
    assert isinstance(decoded["boundaries"]["first_instruction_page"], int)
    assert isinstance(decoded["summary"]["pages_total"], int)
    assert all(isinstance(key, str) for key in decoded.keys())


def test_quote_authority_metadata_is_candidate_tagging_safe():
    section_map = build_section_authority_map(
        [{"page": 1, "text": "Valore finale di stima: Euro 100.000,00"}]
    )
    meta = classify_quote_authority(1, "Valore finale di stima: Euro 100.000,00", section_map)
    encoded = json.dumps(meta, ensure_ascii=False)
    decoded = json.loads(encoded)

    for key in (
        "section_zone",
        "authority_level",
        "authority_score",
        "domain_hints",
        "answer_point",
        "is_instruction_like",
        "is_answer_like",
        "reason_for_authority",
    ):
        assert key in decoded
