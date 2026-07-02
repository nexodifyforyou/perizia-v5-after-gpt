"""Tests for the deterministic compliance evidence gate (never default conforming).

The gate runs BEFORE validation: a 'conforming' claim without an explicit
conformity statement in its cited text — or any compliance claim with no
evidence at all — is downgraded to 'uncertain' + manual review instead of
failing the whole job. Explicitly supported claims are untouched.
"""

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
