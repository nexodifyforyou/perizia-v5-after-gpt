from correctness_v2 import fact_lineage, lot_packets

from .beta_fixture import prepare


def _classify(text, pages, segmentation, category="compliance", bene=None):
    return fact_lineage._classify_applicability(
        text, pages, segmentation, ["1", "2", "3", "4"], bene or {}, category=category
    )


def test_matrix_01_global_fact_projects_to_every_lot():
    _, _, _, segmentation, ledger = prepare()
    facts = [f for f in ledger["facts"] if f["category"] == "compliance" and f["applicability"] == fact_lineage.ALL_LOTS]
    assert len(facts) == 2
    assert all(f["applicability_lot_ids"] == ["1", "2", "3", "4"] for f in facts)


def test_matrix_02_shared_explicit_fact_is_scoped_to_named_lots():
    _, _, _, segmentation, _ = prepare()
    scope, ids, basis = _classify("Ipoteca sui Lotti 1 e 2", [3], segmentation, category="formality")
    assert (scope, ids, basis) == (fact_lineage.MULTIPLE_LOTS, ["1", "2"], "explicit_multi_lot_enumeration")


def test_compact_range_expansion_includes_intermediate_lots():
    _, _, _, segmentation, _ = prepare()
    scope, ids, _ = _classify("Dichiarazione per Lotti 1-4", [2], segmentation)
    assert scope == fact_lineage.ALL_LOTS
    assert ids == ["1", "2", "3", "4"]


def test_matrix_04_bene_specific_fact_stays_with_mapped_lot():
    segmentation = {"page_assignments": [], "bene_lot_map": {"7": "3"}}
    assert _classify("Difformità del Bene 7", [9], segmentation, bene={"7": "3"})[:2] == (fact_lineage.BENE_SPECIFIC, ["3"])


def test_matrix_08_shared_untagged_fact_is_unknown():
    _, _, _, segmentation, _ = prepare()
    assert _classify("Annotazione priva di richiamo", [3], segmentation)[0] == fact_lineage.UNKNOWN_APPLICABILITY


def test_all_lots_never_inferred_from_shared_page_alone():
    _, _, _, segmentation, _ = prepare()
    scope, _, basis = _classify("Conformità indicata nel riepilogo", [2], segmentation)
    assert scope == fact_lineage.UNKNOWN_APPLICABILITY
    assert basis == "shared_page_no_explicit_lot_reference"


def test_global_preamble_identity_is_case_global_and_substance_is_all_lots():
    _, _, _, segmentation, _ = prepare()
    assert _classify("Tribunale dimostrativo", [1], segmentation, "identity")[0] == fact_lineage.CASE_GLOBAL
    assert _classify("Occupazione dichiarata", [1], segmentation, "occupancy")[0] == fact_lineage.ALL_LOTS


def test_checkbox_summary_has_low_computed_confidence():
    declaration, quality, _ = fact_lineage._declaration_metadata("Conformità edilizia □ Sì ■ No")
    assert (declaration, quality) == ("checkbox_summary", "checkbox_only")


def test_ledger_ids_are_stable_and_shared_money_reuses_existing_projection():
    _, _, _, segmentation, ledger = prepare()
    again = fact_lineage.build_case_fact_ledger(prepare()[0]["case_worksheet"], segmentation, prepare()[2])
    assert [f["fact_id"] for f in ledger["facts"]] == [f["fact_id"] for f in again["facts"]]
    assert any(f["source_stage"] == "shared_summary_projection" and f["money"]["amount"] == 124000 for f in ledger["facts"])
