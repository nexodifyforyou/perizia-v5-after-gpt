import copy
import os
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from customer_decision_contract import (  # noqa: E402
    sanitize_customer_facing_result,
    separate_internal_runtime_from_customer_result,
)
from perizia_authority_lot_projection import (  # noqa: E402
    FEATURE_FLAG,
    apply_authority_lot_projection_if_enabled,
)
from perizia_authority_resolvers import build_authority_shadow_resolvers  # noqa: E402
from perizia_section_authority import build_section_authority_map  # noqa: E402
from scripts.audit_authority_corpus import _extract_pdf_pages  # noqa: E402
from scripts.compare_authority_vs_legacy import (  # noqa: E402
    RUNS_ROOT,
    add_projection_columns,
    compare_analysis_id,
    _load_candidates_for_analysis,
    _read_json,
)


SINGLE_LOT_PDF = BACKEND_DIR / "tests" / "fixtures" / "perizie" / "1859886_c_perizia.pdf"
MULTILOT_PDF = Path("/srv/perizia/app/uploads/perizia_multilot_69_2024.pdf")
OSTUNI_PDF = Path("/home/syedtajmeelshah/Ostuni, Via Viterbo 2.pdf")
VIA_NUOVA_PDF = Path("/home/syedtajmeelshah/Via Nuova 19_1.pdf")
VIA_DEL_MARE_PDF = Path("/home/syedtajmeelshah/Via del Mare 4591-4593.pdf")
CASA_ANALYSIS_ID = "analysis_6b3ab6865dca"

FORBIDDEN_CUSTOMER_KEYS = {
    "authority_lot_projection",
    "authority_shadow_resolvers",
    "authority_shadow",
    "shadow_authority",
    "authority_resolver",
    "section_zone",
    "authority_score",
    "authority_level",
    "domain_hints",
    "reason_for_authority",
    "debug",
    "internal_runtime",
}


@lru_cache(maxsize=None)
def _shadow_for_pdf(path_str: str) -> Dict[str, Any]:
    path = Path(path_str)
    if not path.exists():
        pytest.skip(f"PDF not available: {path}")
    pages = _extract_pdf_pages(path)
    section_map = build_section_authority_map(pages)
    return build_authority_shadow_resolvers(pages, section_map)


@lru_cache(maxsize=None)
def _shadow_for_analysis(analysis_id: str) -> Dict[str, Any]:
    extract_dir = RUNS_ROOT / analysis_id / "extract"
    pages = _read_json(extract_dir / "pages_raw.json", [])
    if not isinstance(pages, list) or not pages:
        pytest.skip(f"saved extraction pages not available for {analysis_id}")
    section_map = _read_json(extract_dir / "section_authority.json")
    if not isinstance(section_map, dict):
        section_map = build_section_authority_map(pages)
    return build_authority_shadow_resolvers(
        pages,
        section_map,
        candidates=_load_candidates_for_analysis(analysis_id),
    )


def _mode_from_shadow(shadow: Dict[str, Any]) -> str:
    lot = shadow.get("lot_structure") if isinstance(shadow.get("lot_structure"), dict) else {}
    value = lot.get("value") if isinstance(lot.get("value"), dict) else {}
    return str(value.get("shadow_lot_mode") or "unknown")


def _detected_numbers(shadow: Dict[str, Any]) -> List[int]:
    lot = shadow.get("lot_structure") if isinstance(shadow.get("lot_structure"), dict) else {}
    value = lot.get("value") if isinstance(lot.get("value"), dict) else {}
    return [int(item) for item in value.get("detected_lot_numbers") or []]


def _false_multilot_result() -> Dict[str, Any]:
    return {
        "lots": [
            {"lot_number": 1, "lot_id": "1", "beni": [{"bene_id": "a", "descrizione": "Bene A"}]},
            {"lot_number": 2, "lot_id": "2", "beni": [{"bene_id": "b", "descrizione": "Bene B"}]},
        ],
        "lots_count": 2,
        "lot_count": 2,
        "is_multi_lot": True,
        "case_header": {"lotto": "Lotti 1, 2"},
        "report_header": {"lotto": {"value": "Lotti 1, 2"}, "is_multi_lot": True},
        "field_states": {
            "stato_occupativo": {"status": "UNKNOWN"},
            "opponibilita_occupazione": {"status": "UNKNOWN"},
            "prezzo_base_asta": {"value": 100000},
        },
        "money_box": {"items": [{"label": "prezzo", "stima_euro": 100000}]},
        "section_3_money_box": {"items": [{"label": "prezzo", "stima_euro": 100000}]},
        "section_9_legal_killers": {"items": [{"title_it": "unchanged"}]},
        "red_flags_operativi": [{"title_it": "unchanged"}],
        "summary_for_client": {"summary_it": "unchanged"},
        "decision_rapida_client": {"headline_it": "unchanged"},
        "section_2_decisione_rapida": {"headline_it": "unchanged"},
        "decision_rapida_narrated": {"headline_it": "unchanged"},
    }


def _false_single_lot_result() -> Dict[str, Any]:
    return {
        "lots": [{"lot_number": 1, "lot_id": "1", "beni": [{"bene_id": "a"}]}],
        "lots_count": 1,
        "lot_count": 1,
        "is_multi_lot": False,
        "case_header": {"lotto": "Lotto Unico"},
        "report_header": {"lotto": {"value": "Lotto Unico"}, "is_multi_lot": False},
        "money_box": {"items": []},
        "section_3_money_box": {"items": []},
        "field_states": {"stato_occupativo": {"status": "UNKNOWN"}},
    }


def _collect_forbidden_keys(value: Any, path: str = "result") -> List[str]:
    hits: List[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}"
            if str(key) in FORBIDDEN_CUSTOMER_KEYS or str(key).startswith("authority_"):
                hits.append(child_path)
            hits.extend(_collect_forbidden_keys(child, child_path))
    elif isinstance(value, list):
        for idx, item in enumerate(value):
            hits.extend(_collect_forbidden_keys(item, f"{path}[{idx}]"))
    return hits


def test_feature_flag_disabled_does_not_change_customer_lots(monkeypatch):
    monkeypatch.delenv(FEATURE_FLAG, raising=False)
    result = _false_multilot_result()
    before = copy.deepcopy(result)
    meta = apply_authority_lot_projection_if_enabled(result, _shadow_for_pdf(str(SINGLE_LOT_PDF)))

    assert meta["status"] == "DISABLED"
    assert meta["applied"] is False
    assert result == before


def test_casa_ai_venti_saved_extraction_projects_single_lot(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    shadow = _shadow_for_analysis(CASA_ANALYSIS_ID)
    assert _mode_from_shadow(shadow) == "single_lot"

    result = _false_multilot_result()
    meta = apply_authority_lot_projection_if_enabled(result, shadow)

    assert meta["status"] == "APPLIED_AUTHORITY_SINGLE_LOT"
    assert meta["applied"] is True
    assert result["lots_count"] == 1
    assert result["lot_count"] == 1
    assert result["is_multi_lot"] is False
    assert result["case_header"]["lotto"] == "Lotto Unico"
    assert len(result["lots"][0]["beni"]) == 2


def test_ostuni_chapter_topology_projects_multi_lot(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    shadow = _shadow_for_pdf(str(OSTUNI_PDF))
    assert _mode_from_shadow(shadow) == "multi_lot"
    assert set(_detected_numbers(shadow)) >= {1, 2, 3, 4, 5, 6, 7}

    result = _false_single_lot_result()
    meta = apply_authority_lot_projection_if_enabled(result, shadow)

    assert meta["status"] == "APPLIED_AUTHORITY_MULTI_LOT"
    assert result["is_multi_lot"] is True
    assert result["lots_count"] >= 7
    assert [lot["lot_number"] for lot in result["lots"][:7]] == [1, 2, 3, 4, 5, 6, 7]


def test_1859886_projects_or_preserves_single_lot(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    shadow = _shadow_for_pdf(str(SINGLE_LOT_PDF))
    assert _mode_from_shadow(shadow) == "single_lot"

    result = _false_multilot_result()
    meta = apply_authority_lot_projection_if_enabled(result, shadow)

    assert meta["authority_lot_mode"] == "single_lot"
    assert result["is_multi_lot"] is False
    assert result["lots_count"] == 1


def test_perizia_multilot_69_projects_multi_lot(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    shadow = _shadow_for_pdf(str(MULTILOT_PDF))
    assert _mode_from_shadow(shadow) == "multi_lot"
    assert set(_detected_numbers(shadow)) >= {1, 2, 3}

    result = _false_single_lot_result()
    meta = apply_authority_lot_projection_if_enabled(result, shadow)

    assert meta["authority_lot_mode"] == "multi_lot"
    assert result["is_multi_lot"] is True
    assert result["lots_count"] >= 3
    assert [lot["lot_number"] for lot in result["lots"][:3]] == [1, 2, 3]


@pytest.mark.parametrize("pdf_path", [VIA_NUOVA_PDF, VIA_DEL_MARE_PDF])
def test_incomplete_or_mostly_unknown_maps_fail_open_without_override(monkeypatch, pdf_path: Path):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    shadow = _shadow_for_pdf(str(pdf_path))
    result = _false_multilot_result()
    before = copy.deepcopy(result)

    meta = apply_authority_lot_projection_if_enabled(result, shadow)

    assert meta["applied"] is False
    assert meta["status"] in {"FAIL_OPEN", "NOT_APPLIED"}
    assert result == before


def test_low_confidence_authority_conflict_preserves_legacy(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _false_multilot_result()
    before = copy.deepcopy(result)
    weak_shadow = {
        "status": "OK",
        "fail_open": False,
        "lot_structure": {
            "status": "OK",
            "fail_open": False,
            "confidence": 0.4,
            "value": {"shadow_lot_mode": "single_lot", "has_high_authority_lotto_unico": True},
            "winning_evidence": [{"page": 2, "quote": "LOTTO UNICO"}],
            "authority_basis": {
                "rules_triggered": ["high_authority_lotto_unico_beats_toc_context_and_generic_lot_mentions"],
                "pages_used": [2],
            },
        },
    }

    meta = apply_authority_lot_projection_if_enabled(result, weak_shadow)

    assert meta["applied"] is False
    assert meta["reason"] == "low_confidence"
    assert result == before


def test_projection_debug_stays_internal_after_customer_sanitization(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _false_multilot_result()
    shadow = _shadow_for_pdf(str(SINGLE_LOT_PDF))
    meta = apply_authority_lot_projection_if_enabled(result, shadow)
    result["debug"] = {
        "authority_lot_projection": meta,
        "authority_shadow_resolvers": shadow,
        "candidate": {"section_zone": "FINAL_LOT_FORMATION", "authority_score": 0.9},
    }

    sanitize_customer_facing_result(result)
    internal_runtime = separate_internal_runtime_from_customer_result(result)

    assert _collect_forbidden_keys(result) == []
    assert "debug" in internal_runtime
    assert "authority_lot_projection" in internal_runtime["debug"]
    assert "authority_shadow_resolvers" in internal_runtime["debug"]


def test_feature_flag_on_changes_only_lot_structure_fields(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _false_multilot_result()
    protected_before = {
        key: copy.deepcopy(result.get(key))
        for key in (
            "field_states",
            "money_box",
            "section_3_money_box",
            "section_9_legal_killers",
            "red_flags_operativi",
            "summary_for_client",
            "decision_rapida_client",
            "section_2_decisione_rapida",
            "decision_rapida_narrated",
        )
    }

    meta = apply_authority_lot_projection_if_enabled(result, _shadow_for_pdf(str(SINGLE_LOT_PDF)))

    assert meta["applied"] is True
    for key, before in protected_before.items():
        assert result.get(key) == before


def test_comparison_harness_can_simulate_authority_lot_projection():
    extract_pages = RUNS_ROOT / CASA_ANALYSIS_ID / "extract" / "pages_raw.json"
    if not extract_pages.exists():
        pytest.skip("Casa ai Venti saved extraction not available")

    row = compare_analysis_id(CASA_ANALYSIS_ID, expected={"expected_lot_mode": "single_lot"})
    projected = add_projection_columns([row])[0]

    assert projected["authority_shadow_lot_mode"] == "single_lot"
    assert projected["projected_lot_mode"] == "single_lot"
    assert projected["projection_status"] in {"APPLIED_AUTHORITY_SINGLE_LOT", "ALREADY_MATCHES"}
