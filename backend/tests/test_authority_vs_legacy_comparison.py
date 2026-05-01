import io
import json
import sys
from contextlib import redirect_stdout
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from scripts import compare_authority_vs_legacy as compare  # noqa: E402


SINGLE_LOT_PDF = BACKEND_DIR / "tests" / "fixtures" / "perizie" / "1859886_c_perizia.pdf"
MULTILOT_PDF = Path("/srv/perizia/app/uploads/perizia_multilot_69_2024.pdf")
OSTUNI_PDF = Path("/home/syedtajmeelshah/Ostuni, Via Viterbo 2.pdf")


def test_comparison_script_produces_stable_table_and_json_row():
    row = compare.compare_pdf(
        SINGLE_LOT_PDF,
        expected={"expected_lot_mode": "single_lot", "expected_occupancy": "OCCUPATO", "expected_opponibilita": ""},
        analysis_payload=None,
    )

    for key in compare.TABLE_HEADERS:
        assert key in row
    assert isinstance(row["legacy_money_summary"], dict)
    assert isinstance(row["authority_money_role_counts"], dict)
    json.dumps(row, ensure_ascii=False, sort_keys=True)

    out = io.StringIO()
    with redirect_stdout(out):
        compare._print_table([row])
    lines = out.getvalue().splitlines()
    assert lines[0].split("\t") == compare.TABLE_HEADERS
    assert len(lines) == 2


def test_known_single_lot_case_compares_correctly():
    row = compare.compare_pdf(
        SINGLE_LOT_PDF,
        expected={"expected_lot_mode": "single_lot", "expected_occupancy": "OCCUPATO", "expected_opponibilita": ""},
        analysis_payload=None,
    )

    assert row["expected_lot_mode"] == "single_lot"
    assert row["authority_shadow_lot_mode"] == "single_lot"
    assert row["authority_shadow_lot_mode"] != "multi_lot"
    assert row["customer_authority_key_leak"] == 0


def test_known_multilot_case_compares_correctly():
    if not MULTILOT_PDF.exists():
        pytest.skip(f"multilot PDF not available: {MULTILOT_PDF}")

    row = compare.compare_pdf(
        MULTILOT_PDF,
        expected={"expected_lot_mode": "multi_lot", "expected_occupancy": "LIBERO", "expected_opponibilita": ""},
        analysis_payload=None,
    )

    assert row["expected_lot_mode"] == "multi_lot"
    assert row["authority_shadow_lot_mode"] == "multi_lot"
    assert row["authority_shadow_lot_mode"] != "single_lot"
    assert row["customer_authority_key_leak"] == 0


def test_chapter_based_multilot_case_compares_correctly():
    if not OSTUNI_PDF.exists():
        pytest.skip(f"Ostuni PDF not available: {OSTUNI_PDF}")

    row = compare.compare_pdf(
        OSTUNI_PDF,
        expected={"expected_lot_mode": "multi_lot", "expected_occupancy": "", "expected_opponibilita": ""},
        analysis_payload=None,
    )

    assert row["expected_lot_mode"] == "multi_lot"
    assert row["authority_shadow_lot_mode"] == "multi_lot"
    assert row["customer_authority_key_leak"] == 0


def test_casa_ai_venti_saved_extraction_stays_single_lot_when_available():
    extract_pages = Path("/srv/perizia/_qa/runs/analysis_6b3ab6865dca/extract/pages_raw.json")
    if not extract_pages.exists():
        pytest.skip("Casa ai Venti saved extraction not available")

    row = compare.compare_analysis_id(
        "analysis_6b3ab6865dca",
        expected={"expected_lot_mode": "single_lot", "expected_occupancy": "", "expected_opponibilita": ""},
    )

    assert row["expected_lot_mode"] == "single_lot"
    assert row["authority_shadow_lot_mode"] == "single_lot"
    assert row["authority_shadow_lot_mode"] != "multi_lot"


def test_unknown_incomplete_cases_do_not_fail_only_because_authority_is_unknown():
    verdict, classes, notes = compare.compare_outputs(
        expected_lot_mode="",
        legacy_lot_mode="unknown",
        authority_shadow_lot_mode="unknown",
        expected_occupancy="",
        legacy_occupancy="UNKNOWN",
        authority_shadow_occupancy="UNKNOWN",
        expected_opponibilita="",
        legacy_opponibilita="UNKNOWN",
        authority_shadow_opponibilita="UNKNOWN",
        legacy_money_summary={},
        authority_money_role_counts={},
        legacy_legal_killers_count=0,
        authority_legal_candidates_count=0,
        instruction_leak_suspects=0,
        placeholder_leak_suspects=0,
        customer_authority_key_leak=0,
    )

    assert verdict == "INSUFFICIENT_EXPECTED_TRUTH"
    assert "insufficient_expected_truth" in classes
    assert "authority_worse_than_legacy" not in classes
    assert notes == []


def test_customer_payload_authority_keys_are_detected_but_internal_debug_is_ignored():
    debug_only = {
        "summary_for_client": {"summary_it": "ok"},
        "debug": {
            "authority_shadow_resolvers": {
                "lot_structure": {
                    "winning_evidence": [
                        {"section_zone": "FINAL_LOT_FORMATION", "authority_level": "HIGH_FACTUAL"}
                    ]
                }
            }
        },
    }
    visible_leak = {
        "summary_for_client": {
            "summary_it": "bad",
            "authority_shadow_resolvers": {"section_zone": "FINAL_LOT_FORMATION"},
        }
    }

    assert compare.collect_authority_key_leaks(debug_only) == []
    assert compare.collect_authority_key_leaks(visible_leak)


def test_comparison_identifies_authority_better_and_worse_synthetic_cases():
    better_verdict, better_classes, _ = compare.compare_outputs(
        expected_lot_mode="multi_lot",
        legacy_lot_mode="single_lot",
        authority_shadow_lot_mode="multi_lot",
        expected_occupancy="",
        legacy_occupancy="UNKNOWN",
        authority_shadow_occupancy="UNKNOWN",
        expected_opponibilita="",
        legacy_opponibilita="UNKNOWN",
        authority_shadow_opponibilita="UNKNOWN",
        legacy_money_summary={},
        authority_money_role_counts={},
        legacy_legal_killers_count=0,
        authority_legal_candidates_count=0,
        instruction_leak_suspects=0,
        placeholder_leak_suspects=0,
        customer_authority_key_leak=0,
    )
    worse_verdict, worse_classes, _ = compare.compare_outputs(
        expected_lot_mode="single_lot",
        legacy_lot_mode="single_lot",
        authority_shadow_lot_mode="multi_lot",
        expected_occupancy="",
        legacy_occupancy="UNKNOWN",
        authority_shadow_occupancy="UNKNOWN",
        expected_opponibilita="",
        legacy_opponibilita="UNKNOWN",
        authority_shadow_opponibilita="UNKNOWN",
        legacy_money_summary={},
        authority_money_role_counts={},
        legacy_legal_killers_count=0,
        authority_legal_candidates_count=0,
        instruction_leak_suspects=0,
        placeholder_leak_suspects=0,
        customer_authority_key_leak=0,
    )

    assert better_verdict == "AUTHORITY_BETTER_THAN_LEGACY"
    assert "authority_better_than_legacy" in better_classes
    assert "false_single_lot" in better_classes
    assert worse_verdict == "AUTHORITY_WORSE_THAN_LEGACY"
    assert "authority_worse_than_legacy" in worse_classes
    assert "false_multilot" in worse_classes


def test_missing_pdf_is_skipped_clearly():
    missing = Path("/tmp/perizia_missing_for_phase_3b.pdf")
    row = compare.compare_pdf(missing, expected={"expected_lot_mode": "single_lot"})

    assert row["comparison_verdict"] == "SKIPPED_MISSING_PDF"
    assert "missing_pdf:" in row["notes"]


def test_via_umbria_stale_money_check_is_included_when_payload_has_stale_label():
    verdict, classes, notes = compare.compare_outputs(
        expected_lot_mode="",
        legacy_lot_mode="unknown",
        authority_shadow_lot_mode="unknown",
        expected_occupancy="",
        legacy_occupancy="UNKNOWN",
        authority_shadow_occupancy="UNKNOWN",
        expected_opponibilita="",
        legacy_opponibilita="UNKNOWN",
        authority_shadow_opponibilita="UNKNOWN",
        legacy_money_summary={
            "buyer_cost_like": 1,
            "valuation_mentions": 1,
            "stale_via_umbria_regolarizzazione": True,
        },
        authority_money_role_counts={"market_value": 1},
        legacy_legal_killers_count=0,
        authority_legal_candidates_count=0,
        instruction_leak_suspects=0,
        placeholder_leak_suspects=0,
        customer_authority_key_leak=0,
    )

    assert verdict == "AUTHORITY_BETTER_THAN_LEGACY"
    assert "valuation_as_buyer_cost" in classes
    assert "stale_via_umbria_regolarizzazione_label_detected" in notes
