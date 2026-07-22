"""Tests for the deterministic compliance evidence gate (never default conforming).

The gate runs BEFORE validation: a 'conforming' claim without an explicit
conformity statement in its cited text — or any compliance claim with no
evidence at all — is downgraded to 'uncertain' + manual review instead of
failing the whole job. Explicitly supported claims are untouched.
"""

import json
from pathlib import Path

import pytest

from correctness_v2 import validator
from correctness_v2.analyst import build_messages, normalize_worksheet

from .sample_perizia import GENERIC_PERIZIA_PAGES, make_worksheet

# A page with no conformity/negativity wording at all (generic filler).
_NEUTRAL_PAGE = {"page_number": 3, "text": "Documentazione amministrativa depositata agli atti. " * 5}
_PAGES_WITH_NEUTRAL = GENERIC_PERIZIA_PAGES + [_NEUTRAL_PAGE]


def _codes(report):
    return {v["code"] for v in report["violations"]}


def test_gate_keeps_supported_conforming_claim():
    ws = normalize_worksheet(make_worksheet())
    gated, gate = validator.apply_compliance_evidence_gate(ws, GENERIC_PERIZIA_PAGES)
    assert gate["downgrade_count"] == 0
    assert gated["technical_compliance"][0]["classification"] == "conforming"


def test_gate_downgrades_conforming_without_conformity_text():
    raw = make_worksheet()
    # Point the conforming urbanistica claim at a neutral/administrative page.
    raw["technical_compliance"][0]["evidence_pages"] = [3]
    ws = normalize_worksheet(raw)
    gated, gate = validator.apply_compliance_evidence_gate(ws, _PAGES_WITH_NEUTRAL)

    assert gate["downgrade_count"] == 1
    item = gated["technical_compliance"][0]
    assert item["classification"] == "uncertain"
    assert item["needs_manual_review"] is True
    assert "uncertain" in (item["notes"] or "")
    assert any("urbanistica" in s for s in gated["missing_or_uncertain"])
    # The original worksheet is not mutated.
    assert ws["technical_compliance"][0]["classification"] == "conforming"

    # The gated worksheet validates clean (no unsupported-conforming violation).
    report = validator.validate_worksheet(gated, _PAGES_WITH_NEUTRAL)
    assert report["validation_status"] == validator.STATUS_VALIDATED
    assert "UNSUPPORTED_COMPLIANCE_CLAIM" not in _codes(report)


def test_gate_downgrades_conforming_on_absent_declaration_only():
    pages = GENERIC_PERIZIA_PAGES + [
        {
            "page_number": 3,
            "text": (
                "Certificazioni energetiche e dichiarazioni di conformità. "
                "Non esiste la dichiarazione di conformità dell'impianto elettrico."
            ),
        }
    ]
    raw = make_worksheet()
    raw["technical_compliance"][0]["evidence_pages"] = [3]
    ws = normalize_worksheet(raw)
    gated, gate = validator.apply_compliance_evidence_gate(ws, pages)

    assert gate["downgrade_count"] == 1
    assert gated["technical_compliance"][0]["classification"] == "uncertain"


@pytest.mark.parametrize("statement", [
    "La regolarita edilizio-urbanistica dell immobile non risulta rispettata: sono state riscontrate difformita.",
    "La regolarità urbanistica dell'immobile non è stata verificata.",
    "La regolarità catastale rispetto allo stato non viene attestata.",
    "Senza verifica, la regolarità edilizia della costruzione resta indeterminata.",
])
def test_gate_never_treats_negated_regolarita_as_affirmative(statement):
    raw = make_worksheet()
    raw["technical_compliance"] = [dict(raw["technical_compliance"][0], evidence_pages=[3])]
    pages = [{"page_number": 3, "text": statement}]
    gated, gate = validator.apply_compliance_evidence_gate(normalize_worksheet(raw), pages)

    assert gate["downgrade_count"] == 1
    assert gated["technical_compliance"][0]["classification"] == "uncertain"
    assert validator._has_positive_compliance_statement(validator._norm(statement)) is False


def test_affirmative_regolarita_is_not_cancelled_by_separate_negative_clause():
    statement = (
        "La regolarità edilizio-urbanistica dell'immobile risulta rispettata; "
        "la non regolarità della costruzione per un diverso ambito."
    )
    assert validator._has_positive_compliance_statement(validator._norm(statement)) is True


def test_affirmative_regolarita_ignores_negation_in_subordinate_clause():
    statement = (
        "La regolarità edilizio-urbanistica dell'immobile risulta rispettata, "
        "sebbene i box auto non siano oggetto di perizia separata."
    )
    assert validator._has_positive_compliance_statement(validator._norm(statement)) is True


@pytest.mark.parametrize("statement", [
    "La regolarità edilizio-urbanistica dell'immobile è accertata.",
    "La regolarità catastale rispetto allo stato risulta rispettata.",
    (
        "La regolarità edilizio-urbanistica dell'immobile risulta rispettata, "
        "sebbene i box auto non siano oggetto di perizia separata."
    ),
    (
        "La regolarità catastale rispetto allo stato risulta rispettata, "
        "anche se il giardino non è censito separatamente."
    ),
    (
        "La regolarità edilizio-urbanistica dell'immobile risulta rispettata. "
        "Il giardino non è censito separatamente."
    ),
    "Non risultano difformità.",
    "Non sono presenti abusi.",
    "L'immobile risulta regolarmente accatastato e conforme alla normativa urbanistica vigente.",
    (
        "La regolarità edilizio-urbanistica dell'immobile risulta rispettata per i lotti 1 e 4; "
        "la non regolarità della costruzione per un diverso ambito."
    ),
])
def test_adversarial_compliance_bank_affirmative(statement):
    assert validator._has_positive_compliance_statement(validator._norm(statement)) is True


@pytest.mark.parametrize("statement", [
    "La regolarita edilizio-urbanistica dell immobile non risulta rispettata: sono state riscontrate difformita.",
    "La regolarità urbanistica dell'immobile non è stata verificata.",
    "La regolarità catastale rispetto allo stato non viene attestata.",
    "Senza verifica, la regolarità edilizia della costruzione resta indeterminata.",
    (
        "La regolarità edilizio-urbanistica dell'immobile risulta dichiarata "
        "mentre non è stata effettivamente verificata dal tecnico incaricato."
    ),
    (
        "La regolarità edilizio-urbanistica dell'immobile risulta rispettata "
        "pur non essendo stata controllata di persona."
    ),
    (
        "La regolarità edilizio-urbanistica dell'immobile risulta rispettata "
        "benché non risulti alcuna documentazione a supporto."
    ),
    "La regolarità edilizio-urbanistica dell'immobile risulta dichiarata senza che sia stata verificata.",
    "Non è stata accertata la regolarità edilizio-urbanistica dell'immobile.",
    "La regolarità edilizio-urbanistica dell'immobile dovrà essere verificata.",
    "La regolarità catastale rispetto allo stato sarà da verificare.",
    (
        "La regolarità edilizio-urbanistica dell'immobile risulta rispettata, "
        "sebbene si segnali che, pur essendo stata dichiarata, essa non risulti "
        "in realtà verificata."
    ),
    (
        "La regolarità edilizio-urbanistica dell'immobile risulta rispettata, "
        "pur essendo priva di alcuna documentazione a supporto."
    ),
    (
        "La regolarità edilizio-urbanistica dell'immobile risulta dichiarata conforme, "
        "in assenza di verifica diretta da parte del tecnico."
    ),
    "La regolarità edilizio-urbanistica dell'immobile risulta rispettata, pur non essendola mai stata verificata.",
    (
        "La regolarità edilizio-urbanistica dell'immobile risulta rispettata. "
        "Tuttavia si segnala successivamente che tale regolarità non è mai stata "
        "effettivamente verificata."
    ),
    (
        "La regolarità edilizio-urbanistica dell'immobile risulta rispettata, "
        "sebbene sia sprovvisto di certificato di agibilità."
    ),
    (
        "La regolarità edilizio-urbanistica dell'immobile risulta rispettata, "
        "sebbene sia sfornita di documentazione tecnica."
    ),
    (
        "La regolarità edilizio-urbanistica dell'immobile risulta rispettata, "
        "pur con documentazione incompleta agli atti."
    ),
    (
        "La regolarità edilizio-urbanistica dell'immobile risulta rispettata; "
        "il certificato non è disponibile agli atti."
    ),
    (
        "La regolarità edilizio-urbanistica dell'immobile risulta rispettata, "
        "sebbene il tecnico non abbia potuto accedere alle unità per il sopralluogo."
    ),
    "La regolarità catastale rispetto allo stato risulta conforme; il certificato non è reperibile.",
    "La regolarità catastale rispetto allo stato risulta conforme; il certificato non è stato esibito.",
    "La regolarità catastale rispetto allo stato risulta conforme; il certificato non è stato prodotto.",
    "La regolarità catastale rispetto allo stato risulta conforme; il certificato non è stato consegnato.",
    "La regolarità catastale rispetto allo stato risulta conforme, ma non è verificabile.",
    "La regolarità catastale rispetto allo stato risulta solo parzialmente conforme.",
    "La regolarità catastale rispetto allo stato risulta solo in parte conforme.",
    "La regolarità catastale rispetto allo stato risulta quasi conforme.",
    "La regolarità catastale rispetto allo stato risulta sostanzialmente conforme.",
    "La regolarità catastale rispetto allo stato risulta in larga parte conforme.",
    "La regolarità catastale rispetto allo stato risulta in misura parziale conforme.",
    "La regolarità catastale rispetto allo stato risulta non del tutto conforme.",
    "La regolarità catastale rispetto allo stato risulta prevalentemente conforme.",
    "La regolarità catastale rispetto allo stato risulta conforme; risultano allegati mancanti agli atti.",
    "La regolarità catastale rispetto allo stato risulta conforme; permangono carenze documentali agli atti.",
    "La regolarità catastale rispetto allo stato risulta conforme salvo una difformità minore nel sottotetto.",
    "La regolarità catastale rispetto allo stato risulta conforme, fatta eccezione per il locale caldaia.",
    "La regolarità catastale rispetto allo stato risulta conforme, ad eccezione di una tramezza interna.",
    "La regolarità catastale rispetto allo stato risulta conforme, eccezion fatta per il sottotetto.",
    "La regolarità catastale rispetto allo stato risulta conforme, con esclusione di un locale accessorio.",
    "La regolarità catastale rispetto allo stato risulta conforme, tranne il vano tecnico.",
    "La regolarità catastale rispetto allo stato risulta conforme, a eccezione di una parete interna.",
    "La regolarità catastale rispetto allo stato risulta pressoché conforme.",
    "La regolarità catastale rispetto allo stato risulta tendenzialmente conforme.",
    "La regolarità catastale rispetto allo stato risulta perlopiù conforme.",
    "La regolarità catastale rispetto allo stato risulta nel complesso conforme.",
    "La regolarità catastale rispetto allo stato risulta grosso modo conforme.",
    # A bare CTU noun phrase has no affirmative predicate and must fail closed.
    "La regolarità edilizio-urbanistica dell'immobile per i Lotti 1 e 4.",
])
def test_adversarial_compliance_bank_not_affirmative(statement):
    raw = make_worksheet()
    raw["technical_compliance"] = [dict(raw["technical_compliance"][0], evidence_pages=[3])]
    pages = [{"page_number": 3, "text": statement}]
    gated, gate = validator.apply_compliance_evidence_gate(normalize_worksheet(raw), pages)

    assert validator._has_positive_compliance_statement(validator._norm(statement)) is False
    assert gate["downgrade_count"] == 1
    assert gated["technical_compliance"][0]["classification"] == "uncertain"


def test_beta_fixture_quantifies_topic_blind_disqualifier_tradeoff():
    fixture_path = (
        Path(__file__).parent / "fixtures" / "beta_multilot_case_cached_pages_sanitized.json"
    )
    pages = json.loads(fixture_path.read_text(encoding="utf-8"))["pages"]

    # The intentionally broad scan flags 3/12 non-compliance pages (safe noise),
    # but suppresses 0/1 clean affirmative declaration pages in this fixture.
    disqualified_pages = [
        page["page_number"]
        for page in pages
        if validator._has_compliance_disqualifier(page["text"])
    ]
    assert disqualified_pages == [3, 7, 11]
    compliance_page = next(page for page in pages if page["page_number"] == 2)
    assert validator._has_compliance_disqualifier(compliance_page["text"]) is False
    assert validator._has_positive_compliance_statement(compliance_page["text"]) is True


def test_gate_keeps_supported_regolare_agibile_claim_without_conforme_word():
    pages = GENERIC_PERIZIA_PAGES + [
        {
            "page_number": 3,
            "text": (
                "L'immobile risulta regolare per la legge n. 47/1985. "
                "L'immobile risulta agibile. "
                "Durante il sopralluogo non sono state riscontrate incongruenze."
            ),
        }
    ]
    raw = make_worksheet()
    raw["technical_compliance"][0]["evidence_pages"] = [3]
    ws = normalize_worksheet(raw)
    gated, gate = validator.apply_compliance_evidence_gate(ws, pages)

    assert gate["downgrade_count"] == 0
    assert gated["technical_compliance"][0]["classification"] == "conforming"
    report = validator.validate_worksheet(gated, pages)
    assert report["validation_status"] == validator.STATUS_VALIDATED
    assert "UNSUPPORTED_COMPLIANCE_CLAIM" not in _codes(report)


def test_gate_downgrades_any_claim_without_evidence():
    raw = make_worksheet()
    raw["technical_compliance"][1]["evidence_pages"] = []  # regularizable claim
    ws = normalize_worksheet(raw)
    gated, gate = validator.apply_compliance_evidence_gate(ws, GENERIC_PERIZIA_PAGES)
    assert gate["downgrade_count"] == 1
    assert gated["technical_compliance"][1]["classification"] == "uncertain"

    # Uncertain-without-evidence is a warning, never a hard failure.
    report = validator.validate_worksheet(gated, GENERIC_PERIZIA_PAGES)
    assert report["validation_status"] == validator.STATUS_VALIDATED
    assert any(w["code"] == "MISSING_EVIDENCE_SOFT" for w in report["warnings"])


def test_gate_drops_out_of_context_pages_on_downgrade():
    # A conforming claim citing a page OUTSIDE the analyzed context (e.g. another
    # lot's page in the selected-lot pipeline) is downgraded and its dangling
    # citation removed, so the job does not fail on a neutralized claim.
    raw = make_worksheet()
    raw["technical_compliance"][0]["evidence_pages"] = [99]
    ws = normalize_worksheet(raw)
    gated, gate = validator.apply_compliance_evidence_gate(ws, GENERIC_PERIZIA_PAGES)
    assert gate["downgrade_count"] == 1
    assert gate["downgrades"][0]["evidence_pages"] == [99]  # preserved in the report
    assert gated["technical_compliance"][0]["evidence_pages"] == []

    report = validator.validate_worksheet(gated, GENERIC_PERIZIA_PAGES)
    assert report["validation_status"] == validator.STATUS_VALIDATED


def test_gate_keeps_supported_negative_claims():
    ws = normalize_worksheet(make_worksheet())
    gated, gate = validator.apply_compliance_evidence_gate(ws, GENERIC_PERIZIA_PAGES)
    # regularizable edilizia/catastale claims with evidence are untouched.
    assert gated["technical_compliance"][1]["classification"] == "regularizable"
    assert gated["technical_compliance"][2]["classification"] == "regularizable"
    assert gate["downgrade_count"] == 0


def test_uncertain_without_evidence_is_not_a_violation():
    raw = make_worksheet()
    raw["technical_compliance"].append(
        {
            "area": "Vincoli o oneri condominiali",
            "classification": "uncertain",
            "blocks_saleability": False,
            "cost": None,
            "timing": None,
            "notes": None,
            "evidence_pages": [],
        }
    )
    ws = normalize_worksheet(raw)
    report = validator.validate_worksheet(ws, GENERIC_PERIZIA_PAGES)
    assert report["validation_status"] == validator.STATUS_VALIDATED
    assert "MISSING_EVIDENCE" not in _codes(report)
    assert any(w["code"] == "MISSING_EVIDENCE_SOFT" for w in report["warnings"])


def test_selected_lot_prompt_includes_document_map():
    document_map = {
        "lot_ids": ["1", "2", "3"],
        "selected_lot": "2",
        "lot_pages": [15, 16, 17],
        "global_pages": [1, 2],
        "excluded_shared_pages": [40],
        "bene_ids": ["1", "2"],
        "compliance_sections": [
            {"area": "urbanistica", "scope": "lot", "lot_id": "2"},
            {"area": "catastale", "scope": "unclear", "lot_id": None},
        ],
    }
    messages = build_messages(
        GENERIC_PERIZIA_PAGES, target_lot="2", document_map=document_map
    )
    user = next(m["content"] for m in messages if m["role"] == "user")
    assert "MAPPA DEL DOCUMENTO" in user
    assert "Lotto selezionato per questa analisi: 2" in user
    assert "DA IGNORARE: 1, 3" in user
    assert "ESCLUSE da questo contesto: 40" in user
    assert "scope=unclear" in user
    # Never-default-conforming rule is present for the selected-lot pass.
    assert "uncertain" in user
