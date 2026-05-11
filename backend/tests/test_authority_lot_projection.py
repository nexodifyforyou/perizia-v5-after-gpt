import copy
import json
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
    sanitize_lot_field_consistency_for_customer,
    sanitize_stale_lot_narratives_after_projection,
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


def _applied_lot_meta(mode: str, numbers: List[int]) -> Dict[str, Any]:
    return {
        "enabled": True,
        "status": "APPLIED_AUTHORITY_SINGLE_LOT" if mode == "single_lot" else "APPLIED_AUTHORITY_MULTI_LOT",
        "applied": True,
        "authority_lot_mode": mode,
        "detected_lot_numbers": numbers,
        "changed_fields": ["lots_count", "is_multi_lot", "case_header.lotto"],
    }


def _customer_lot_narrative_result() -> Dict[str, Any]:
    legal_item = {
        "killer": "Lotto 2: Non regolare / difformità",
        "detail": "Lotto 2 da verificare",
        "evidence": [{"quote": "Non-lot evidence remains irrelevant to active label"}],
    }
    return {
        "lots": [{"lot_number": 1, "lot_id": "1", "beni": [{"bene_id": "a"}]}],
        "lots_count": 1,
        "lot_count": 1,
        "is_multi_lot": False,
        "case_header": {"lotto": "Lotto Unico"},
        "report_header": {"lotto": {"value": "Lotto Unico"}, "is_multi_lot": False},
        "summary_for_client": {
            "summary_it": "La perizia riguarda 2 lotti. Rischi documentati: verificare difformità e costi."
        },
        "summary_for_client_bundle": {
            "factual_summary_it": "La perizia riguarda 2 lotti. Rischi documentati: verificare difformità e costi.",
            "checks_it": ["Verificare difformità e costi"],
        },
        "decision_rapida_narrated": {
            "summary_it": "La perizia riguarda 2 lotti. Stato e costi da verificare.",
            "decisione_rapida_it": "Verificare stato e costi prima dell'offerta.",
        },
        "section_9_legal_killers": {
            "items": [copy.deepcopy(legal_item), {"killer": "Occupazione da verificare", "detail": "Titolo non chiaro"}],
        },
        "red_flags_operativi": [
            {"flag_it": "Lotto 2: difformità da verificare", "action_it": "Verificare la regolarità"},
            {"flag_it": "Stato occupativo da verificare", "action_it": "Controllare titolo"},
        ],
        "customer_decision_contract": {
            "summary_for_client": {
                "summary_it": "La perizia riguarda 2 lotti. Rischi documentati: verificare difformità e costi."
            },
            "summary_for_client_bundle": {
                "factual_summary_it": "La perizia riguarda 2 lotti. Rischi documentati: verificare difformità e costi."
            },
            "decision_rapida_narrated": {
                "summary_it": "La perizia riguarda 2 lotti. Stato e costi da verificare."
            },
            "section_9_legal_killers": {
                "items": [copy.deepcopy(legal_item), {"killer": "Occupazione da verificare", "detail": "Titolo non chiaro"}],
            },
        },
        "other_customer_text": "Campo non-lot preservato",
    }


def _lot_contradiction_result() -> Dict[str, Any]:
    return {
        "lots_count": 2,
        "lot_count": 2,
        "is_multi_lot": True,
        "case_header": {"lotto": "Lotto Unico"},
        "report_header": {"lotto": {"value": "Lotto Unico"}, "is_multi_lot": True},
        "field_states": {"lotto": {"value": "Lotto Unico"}},
        "summary_for_client": {"summary_it": "Rischi non collegati ai lotti."},
        "customer_decision_contract": {
            "lots_count": 2,
            "lot_count": 2,
            "is_multi_lot": True,
            "case_header": {"lotto": "Lotto Unico"},
            "report_header": {"lotto": {"value": "Lotto Unico"}, "is_multi_lot": True},
            "field_states": {"lotto": {"value": "Lotto Unico"}},
        },
    }


def _weak_unknown_single_lot_result() -> Dict[str, Any]:
    return {
        "lots": [{"lot_number": 1, "lot_id": "1", "titolo": "Lotto 1"}],
        "lots_count": 1,
        "is_multi_lot": False,
        "lot_index": [{"lot": 1, "ubicazione": "Via non certa"}],
        "case_header": {"lotto": "Lotto Unico"},
        "report_header": {"lotto": {"value": "Lotto Unico"}, "is_multi_lot": False},
        "field_states": {"lotto": {"value": "Lotto Unico"}},
        "customer_decision_contract": {
            "case_header": {"lotto": "Lotto Unico"},
            "report_header": {"lotto": {"value": "Lotto Unico"}, "is_multi_lot": False},
            "field_states": {"lotto": {"value": "Lotto Unico"}},
        },
    }


def _stale_multilot_header_result(numbers: List[int]) -> Dict[str, Any]:
    lots = [
        {
            "lot_number": number,
            "lot_id": str(number),
            "titolo": f"Lotto {number}",
            "beni": [{"bene_id": f"bene_{number}", "descrizione": f"Bene Lotto {number}"}],
        }
        for number in numbers
    ]
    result = {
        "lots": copy.deepcopy(lots),
        "lots_count": len(numbers),
        "lot_count": len(numbers),
        "is_multi_lot": True,
        "lot_index": [{"lot": number, "ubicazione": f"Lotto {number}"} for number in numbers],
        "case_header": {"lotto": "DA VERIFICARE"},
        "report_header": {"lotto": {"value": "Lotto 1", "evidence": [{"page": 1, "quote": "LOTTO 1"}]}, "is_multi_lot": False},
        "field_states": {"lotto": {"value": "DA VERIFICARE"}},
        "section_9_legal_killers": {
            "items": [{"killer": f"Lotto {number}: verifica specifica preservata"} for number in numbers]
        },
    }
    result["customer_decision_contract"] = {
        "lots": copy.deepcopy(lots),
        "lots_count": len(numbers),
        "lot_count": len(numbers),
        "is_multi_lot": True,
        "lot_index": copy.deepcopy(result["lot_index"]),
        "case_header": {"lotto": "DA VERIFICARE"},
        "report_header": {"lotto": {"value": "Lotto 1", "evidence": [{"page": 1, "quote": "LOTTO 1"}]}, "is_multi_lot": False},
        "field_states": {"lotto": {"value": "DA VERIFICARE"}},
    }
    return result


def _assert_lot_headers(result: Dict[str, Any], label: str, is_multi_lot: bool) -> None:
    assert result["case_header"]["lotto"] == label
    assert result["report_header"]["lotto"]["value"] == label
    assert result["report_header"]["is_multi_lot"] is is_multi_lot
    if isinstance(result.get("field_states"), dict) and isinstance(result["field_states"].get("lotto"), dict):
        assert result["field_states"]["lotto"]["value"] == label


def _assert_top_level_and_cdc_lot_fields_match(result: Dict[str, Any]) -> None:
    cdc = result["customer_decision_contract"]
    for key in (
        "lots",
        "lots_count",
        "lot_count",
        "is_multi_lot",
        "lot_index",
        "case_header",
        "report_header",
        "lot_verification_hint",
        "lot_verification_pages",
        "lot_verification_sections",
    ):
        assert cdc.get(key) == result.get(key)


def _assert_lot_verification_guidance(result: Dict[str, Any], *, expects_pages: bool) -> None:
    hint = result.get("lot_verification_hint")
    pages = result.get("lot_verification_pages")
    sections = result.get("lot_verification_sections")
    assert isinstance(hint, str) and hint
    assert "lot structure" in hint
    assert "lotto" in hint
    assert "numero lotti" in hint
    assert "Identificazione del lotto" in hint
    assert "Descrizione dei beni" in hint
    assert "Formazione dei lotti" in hint
    assert "Quesito / risposta dell'esperto" in hint
    assert "Valutazione / riepilogo finale" in hint
    assert isinstance(sections, list)
    assert "Formazione dei lotti" in sections
    assert isinstance(pages, dict)
    if expects_pages:
        assert pages.get("exact_pages") or pages.get("searched_page_ranges")
        assert "pagine non determinate automaticamente" not in hint
    else:
        assert pages.get("exact_pages") == []
        assert pages.get("searched_page_ranges") == []
        assert pages.get("warning") == "lot_pages_not_determined"
        assert "pagine non determinate automaticamente; verificare nelle sezioni iniziali e nella formazione lotti della perizia" in hint
    cdc = result.get("customer_decision_contract")
    if isinstance(cdc, dict):
        assert cdc.get("lot_verification_hint") == result.get("lot_verification_hint")
        assert cdc.get("lot_verification_pages") == result.get("lot_verification_pages")
        assert cdc.get("lot_verification_sections") == result.get("lot_verification_sections")
    lotto = result["report_header"]["lotto"]
    assert lotto["verification_hint"] == hint
    assert lotto["verification_pages"] == pages
    assert lotto["verification_sections"] == sections


def _assert_no_lot_verification_guidance(result: Dict[str, Any]) -> None:
    for key in ("lot_verification_hint", "lot_verification_pages", "lot_verification_sections"):
        assert key not in result
    lotto = result.get("report_header", {}).get("lotto") if isinstance(result.get("report_header"), dict) else None
    if isinstance(lotto, dict):
        for key in ("verification_hint", "verification_pages", "verification_sections"):
            assert key not in lotto


def _flatten_text(value: Any) -> str:
    if isinstance(value, dict):
        return " ".join(_flatten_text(child) for child in value.values())
    if isinstance(value, list):
        return " ".join(_flatten_text(item) for item in value)
    return str(value or "")


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


def test_lot_consistency_sanitizer_flag_off_valid_lotto_unico_unchanged(monkeypatch):
    monkeypatch.delenv(FEATURE_FLAG, raising=False)
    result = _false_single_lot_result()
    result["customer_decision_contract"] = copy.deepcopy(result)
    before = copy.deepcopy(result)

    meta = sanitize_lot_field_consistency_for_customer(
        result,
        {"enabled": False, "status": "DISABLED", "authority_lot_mode": "unknown"},
    )

    assert meta["enabled"] is False
    assert meta["status"] == "SKIPPED"
    assert meta["changed"] is False
    assert meta["changed_fields"] == []
    assert result == before
    assert result["case_header"]["lotto"] == "Lotto Unico"
    assert "DA VERIFICARE" not in _flatten_text(result)


def test_lot_consistency_sanitizer_flag_off_upload_path_disabled_meta_noop(monkeypatch):
    monkeypatch.delenv(FEATURE_FLAG, raising=False)
    result = _false_single_lot_result()
    result["customer_decision_contract"] = copy.deepcopy(result)
    before = copy.deepcopy(result)

    projection_meta = apply_authority_lot_projection_if_enabled(result, {"lot_structure": {}})
    consistency_meta = sanitize_lot_field_consistency_for_customer(result, projection_meta)

    assert projection_meta["enabled"] is False
    assert projection_meta["status"] == "DISABLED"
    assert consistency_meta["enabled"] is False
    assert consistency_meta["status"] == "SKIPPED"
    assert consistency_meta["changed"] is False
    assert consistency_meta["changed_fields"] == []
    assert result == before
    assert result["case_header"]["lotto"] == "Lotto Unico"
    assert "DA VERIFICARE" not in _flatten_text(result)


def test_lot_consistency_sanitizer_flag_off_weak_fake_lotto_unico_unchanged(monkeypatch):
    monkeypatch.delenv(FEATURE_FLAG, raising=False)
    result = _weak_unknown_single_lot_result()
    before = copy.deepcopy(result)

    meta = sanitize_lot_field_consistency_for_customer(
        result,
        {"enabled": False, "status": "DISABLED", "authority_lot_mode": "unknown"},
    )

    assert meta["enabled"] is False
    assert meta["status"] == "SKIPPED"
    assert meta["changed"] is False
    assert meta["changed_fields"] == []
    assert result == before
    assert result["case_header"]["lotto"] == "Lotto Unico"
    assert "DA VERIFICARE" not in _flatten_text(result)


def test_lot_consistency_sanitizer_meta_disabled_noop_even_when_flag_on(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _weak_unknown_single_lot_result()
    before = copy.deepcopy(result)

    meta = sanitize_lot_field_consistency_for_customer(
        result,
        {"enabled": False, "status": "DISABLED", "authority_lot_mode": "unknown"},
    )

    assert meta["enabled"] is False
    assert meta["status"] == "SKIPPED"
    assert meta["changed"] is False
    assert meta["changed_fields"] == []
    assert result == before


def test_lot_consistency_sanitizer_missing_meta_noop_even_when_flag_on(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _weak_unknown_single_lot_result()
    before = copy.deepcopy(result)

    meta = sanitize_lot_field_consistency_for_customer(result)

    assert meta["enabled"] is False
    assert meta["status"] == "SKIPPED"
    assert meta["changed"] is False
    assert meta["changed_fields"] == []
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


def test_stale_single_lot_narrative_is_removed_after_authority_projection(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _customer_lot_narrative_result()
    meta = sanitize_stale_lot_narratives_after_projection(result, _applied_lot_meta("single_lot", [1]))
    serialized = _flatten_text(result)

    assert result["lots_count"] == 1
    assert result["is_multi_lot"] is False
    assert result["case_header"]["lotto"] == "Lotto Unico"
    assert "2 lotti" not in serialized
    assert "Lotto 2" not in serialized
    assert result["other_customer_text"] == "Campo non-lot preservato"
    assert result["section_9_legal_killers"]["items"] == [
        {"killer": "Occupazione da verificare", "detail": "Titolo non chiaro"}
    ]
    assert meta["removed_stale_lot_narrative_count"] > 0


def test_lot_consistency_sanitizer_downgrades_fail_open_contradiction(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _lot_contradiction_result()

    meta = sanitize_lot_field_consistency_for_customer(
        result,
        {
            "enabled": True,
            "status": "NOT_APPLIED",
            "applied": False,
            "authority_lot_mode": "unknown",
            "reason": "low_confidence",
        },
    )

    assert meta["status"] == "APPLIED_CONSERVATIVE_CONSISTENCY"
    assert result["is_multi_lot"] is True
    assert result["lots_count"] == 2
    _assert_lot_headers(result, "DA VERIFICARE", True)
    _assert_top_level_and_cdc_lot_fields_match(result)
    assert "Lotto Unico" not in _flatten_text(result["case_header"])
    assert "Lotto Unico" not in _flatten_text(result["report_header"])


def test_lot_consistency_sanitizer_not_applied_removes_fake_lotto_unico(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _weak_unknown_single_lot_result()

    meta = sanitize_lot_field_consistency_for_customer(
        result,
        {
            "enabled": True,
            "status": "NOT_APPLIED",
            "applied": False,
            "authority_lot_mode": "unknown",
            "reason": "low_confidence",
        },
    )

    assert meta["status"] == "APPLIED_CONSERVATIVE_UNCERTAIN_CONSISTENCY"
    assert "Lotto Unico" not in _flatten_text(result)
    _assert_lot_headers(result, "DA VERIFICARE", False)
    assert result["is_multi_lot"] is False
    assert result["lots_count"] == 1
    assert "Lotti 1, 2" not in _flatten_text(result)
    _assert_lot_verification_guidance(result, expects_pages=False)


def test_lot_consistency_sanitizer_not_applied_syncs_top_level_and_cdc(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _weak_unknown_single_lot_result()
    result["customer_decision_contract"].pop("lots_count", None)
    result["customer_decision_contract"].pop("is_multi_lot", None)
    result["customer_decision_contract"].pop("lots", None)
    result["customer_decision_contract"].pop("lot_index", None)

    sanitize_lot_field_consistency_for_customer(
        result,
        {"enabled": True, "status": "NOT_APPLIED", "applied": False, "authority_lot_mode": "unknown"},
    )

    _assert_lot_headers(result, "DA VERIFICARE", False)
    _assert_top_level_and_cdc_lot_fields_match(result)
    assert result["customer_decision_contract"]["lots_count"] == 1
    assert result["customer_decision_contract"]["is_multi_lot"] is False
    assert result["customer_decision_contract"]["lots"] == result["lots"]
    assert result["customer_decision_contract"]["lot_index"] == result["lot_index"]
    _assert_lot_verification_guidance(result, expects_pages=False)


def test_lot_consistency_sanitizer_not_applied_da_verificare_includes_page_guidance(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _weak_unknown_single_lot_result()

    sanitize_lot_field_consistency_for_customer(
        result,
        {
            "enabled": True,
            "status": "NOT_APPLIED",
            "applied": False,
            "authority_lot_mode": "unknown",
            "source_pages": [12],
            "searched_page_ranges": ["2-8"],
        },
    )

    _assert_lot_headers(result, "DA VERIFICARE", False)
    _assert_lot_verification_guidance(result, expects_pages=True)
    assert result["lot_verification_pages"]["exact_pages"] == [12]
    assert result["lot_verification_pages"]["searched_page_ranges"] == ["2-8"]
    assert "Ancore pagina: 12" in result["lot_verification_hint"]


def test_lot_consistency_sanitizer_not_applied_da_verificare_no_page_evidence_uses_fallback(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _weak_unknown_single_lot_result()

    sanitize_lot_field_consistency_for_customer(
        result,
        {"enabled": True, "status": "FAIL_OPEN", "applied": False, "authority_lot_mode": "unknown"},
    )

    _assert_lot_headers(result, "DA VERIFICARE", False)
    _assert_lot_verification_guidance(result, expects_pages=False)
    assert "pagina 1" not in result["lot_verification_hint"].lower()


def test_lot_consistency_sanitizer_applies_authority_single_lot(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _false_multilot_result()
    result["customer_decision_contract"] = copy.deepcopy(result)

    meta = sanitize_lot_field_consistency_for_customer(result, _applied_lot_meta("single_lot", [1]))

    assert meta["status"] == "APPLIED_SINGLE_LOT_CONSISTENCY"
    assert result["lots_count"] == 1
    assert result["lot_count"] == 1
    assert result["is_multi_lot"] is False
    _assert_lot_headers(result, "Lotto Unico", False)
    _assert_top_level_and_cdc_lot_fields_match(result)
    _assert_no_lot_verification_guidance(result)


def test_lot_consistency_sanitizer_applies_authority_multi_lot(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _false_single_lot_result()
    result["customer_decision_contract"] = copy.deepcopy(result)

    meta = sanitize_lot_field_consistency_for_customer(result, _applied_lot_meta("multi_lot", [1, 2, 3]))

    assert meta["status"] == "APPLIED_MULTI_LOT_CONSISTENCY"
    assert result["lots_count"] == 3
    assert result["is_multi_lot"] is True
    _assert_lot_headers(result, "Lotti 1, 2, 3", True)
    _assert_top_level_and_cdc_lot_fields_match(result)
    assert "Lotto Unico" not in _flatten_text(result["case_header"])
    assert "Lotto Unico" not in _flatten_text(result["report_header"])
    _assert_no_lot_verification_guidance(result)


def test_lot_consistency_sanitizer_applied_multilot_three_syncs_stale_headers_and_cdc(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _stale_multilot_header_result([1, 2, 3])

    meta = sanitize_lot_field_consistency_for_customer(result, _applied_lot_meta("multi_lot", [1, 2, 3]))

    assert meta["status"] == "APPLIED_MULTI_LOT_CONSISTENCY"
    assert result["lots_count"] == 3
    assert result["is_multi_lot"] is True
    _assert_lot_headers(result, "Lotti 1, 2, 3", True)
    _assert_top_level_and_cdc_lot_fields_match(result)
    assert result["customer_decision_contract"]["case_header"]["lotto"] == "Lotti 1, 2, 3"
    assert result["customer_decision_contract"]["report_header"]["lotto"]["value"] == "Lotti 1, 2, 3"
    assert result["customer_decision_contract"]["report_header"]["is_multi_lot"] is True
    assert result["case_header"]["lotto"] not in {"DA VERIFICARE", "Lotto Unico", "Lotto 1"}
    assert result["report_header"]["lotto"]["value"] not in {"DA VERIFICARE", "Lotto Unico", "Lotto 1"}
    _assert_no_lot_verification_guidance(result)
    assert "Lotto 1: verifica specifica preservata" in _flatten_text(result["section_9_legal_killers"])
    assert "Lotto 2: verifica specifica preservata" in _flatten_text(result["section_9_legal_killers"])
    assert "Lotto 3: verifica specifica preservata" in _flatten_text(result["section_9_legal_killers"])


def test_lot_consistency_sanitizer_applied_multilot_creates_missing_cdc_mirror(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _stale_multilot_header_result([1, 2, 3])
    result.pop("customer_decision_contract", None)

    sanitize_lot_field_consistency_for_customer(result, _applied_lot_meta("multi_lot", [1, 2, 3]))

    assert isinstance(result.get("customer_decision_contract"), dict)
    _assert_lot_headers(result, "Lotti 1, 2, 3", True)
    _assert_top_level_and_cdc_lot_fields_match(result)


def test_lot_consistency_sanitizer_applied_multilot_two_syncs_without_single_contradiction(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _stale_multilot_header_result([1, 2])
    result["case_header"]["lotto"] = "Lotto Unico"
    result["report_header"]["lotto"]["value"] = "Lotto Unico"
    result["customer_decision_contract"]["case_header"]["lotto"] = "Lotto Unico"
    result["customer_decision_contract"]["report_header"]["lotto"]["value"] = "Lotto Unico"

    sanitize_lot_field_consistency_for_customer(result, _applied_lot_meta("multi_lot", [1, 2]))

    _assert_lot_headers(result, "Lotti 1, 2", True)
    _assert_top_level_and_cdc_lot_fields_match(result)
    assert "Lotto Unico" not in _flatten_text(result["case_header"])
    assert "Lotto Unico" not in _flatten_text(result["report_header"])
    assert result["case_header"]["lotto"] != "DA VERIFICARE"
    assert result["report_header"]["lotto"]["value"] != "DA VERIFICARE"
    _assert_no_lot_verification_guidance(result)


def test_lot_consistency_sanitizer_applied_multilot_seven_syncs_full_header(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    numbers = list(range(1, 8))
    result = _stale_multilot_header_result(numbers)

    sanitize_lot_field_consistency_for_customer(result, _applied_lot_meta("multi_lot", numbers))

    label = "Lotti 1, 2, 3, 4, 5, 6, 7"
    _assert_lot_headers(result, label, True)
    _assert_top_level_and_cdc_lot_fields_match(result)
    assert [lot["lot_number"] for lot in result["lots"]] == numbers
    assert [row["lot"] for row in result["lot_index"]] == numbers
    assert not any(lot["lot_number"] not in numbers for lot in result["lots"])
    _assert_no_lot_verification_guidance(result)


def test_lot_consistency_sanitizer_applied_single_lot_keeps_lotto_unico_regression(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _stale_multilot_header_result([1, 2, 3])

    sanitize_lot_field_consistency_for_customer(result, _applied_lot_meta("single_lot", [1]))

    assert result["lots_count"] == 1
    assert result["is_multi_lot"] is False
    _assert_lot_headers(result, "Lotto Unico", False)
    _assert_top_level_and_cdc_lot_fields_match(result)
    _assert_no_lot_verification_guidance(result)


def test_lot_consistency_sanitizer_not_applied_weak_case_keeps_conservative_unknown(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _weak_unknown_single_lot_result()

    sanitize_lot_field_consistency_for_customer(
        result,
        {"enabled": True, "status": "NOT_APPLIED", "applied": False, "authority_lot_mode": "unknown"},
    )

    _assert_lot_headers(result, "DA VERIFICARE", False)
    _assert_top_level_and_cdc_lot_fields_match(result)
    assert "Lotto Unico" not in _flatten_text(result)
    assert "Lotti 1, 2" not in _flatten_text(result)
    _assert_lot_verification_guidance(result, expects_pages=False)


def test_lot_consistency_sanitizer_flag_off_preserves_valid_and_weak_lot_labels(monkeypatch):
    monkeypatch.delenv(FEATURE_FLAG, raising=False)
    valid = _false_single_lot_result()
    valid["customer_decision_contract"] = copy.deepcopy(valid)
    weak = _weak_unknown_single_lot_result()
    before_valid = copy.deepcopy(valid)
    before_weak = copy.deepcopy(weak)

    disabled_meta = {"enabled": False, "status": "DISABLED", "authority_lot_mode": "unknown"}
    valid_meta = sanitize_lot_field_consistency_for_customer(valid, disabled_meta)
    weak_meta = sanitize_lot_field_consistency_for_customer(weak, disabled_meta)

    assert valid_meta["status"] == "SKIPPED"
    assert weak_meta["status"] == "SKIPPED"
    assert valid == before_valid
    assert weak == before_weak
    assert valid["case_header"]["lotto"] == "Lotto Unico"
    assert weak["case_header"]["lotto"] == "Lotto Unico"


def test_lot_consistency_sanitizer_applied_multilot_metadata_does_not_leak(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _stale_multilot_header_result([1, 2, 3])
    meta = sanitize_lot_field_consistency_for_customer(result, _applied_lot_meta("multi_lot", [1, 2, 3]))
    result["debug"] = {
        "authority_lot_projection": _applied_lot_meta("multi_lot", [1, 2, 3]),
        "authority_lot_field_consistency": meta,
        "internal_runtime": {"removed_paths": ["result.report_header.lotto.value"]},
        "section_zone": "FINAL_LOT_FORMATION",
        "authority_score": 0.98,
        "shadow_authority": {"lot": "multi"},
    }

    sanitize_customer_facing_result(result)
    internal_runtime = separate_internal_runtime_from_customer_result(result)
    serialized = json.dumps(result, ensure_ascii=False)

    assert _collect_forbidden_keys(result) == []
    for forbidden in (
        "debug",
        "internal_runtime",
        "authority_lot_projection",
        "removed_paths",
        "section_zone",
        "authority_score",
        "shadow_authority",
    ):
        assert forbidden not in serialized
    assert "debug" in internal_runtime


def test_lot_consistency_sanitizer_applied_multilot_mutates_response_copy_only(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    source = _stale_multilot_header_result([1, 2, 3])
    response_copy = copy.deepcopy(source)

    sanitize_lot_field_consistency_for_customer(response_copy, _applied_lot_meta("multi_lot", [1, 2, 3]))

    assert source["case_header"]["lotto"] == "DA VERIFICARE"
    assert source["report_header"]["lotto"]["value"] == "Lotto 1"
    assert response_copy["case_header"]["lotto"] == "Lotti 1, 2, 3"
    assert response_copy["report_header"]["lotto"]["value"] == "Lotti 1, 2, 3"


def test_lot_consistency_sanitizer_preserves_known_authority_multilot(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = {
        "lots": [{"lot_number": 1}, {"lot_number": 2}, {"lot_number": 3}],
        "lots_count": 3,
        "is_multi_lot": True,
        "lot_index": [{"lot": 1}, {"lot": 2}, {"lot": 3}],
        "case_header": {"lotto": "Lotti 1, 2, 3"},
        "report_header": {"lotto": {"value": "Lotti 1, 2, 3"}, "is_multi_lot": True},
        "customer_decision_contract": {},
    }

    sanitize_lot_field_consistency_for_customer(result, _applied_lot_meta("multi_lot", [1, 2, 3]))

    assert result["lots_count"] == 3
    assert result["is_multi_lot"] is True
    _assert_lot_headers(result, "Lotti 1, 2, 3", True)
    _assert_top_level_and_cdc_lot_fields_match(result)


def test_lot_consistency_sanitizer_aligns_cdc_and_top_level(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _false_single_lot_result()
    result["customer_decision_contract"] = _false_multilot_result()

    meta = sanitize_lot_field_consistency_for_customer(result, _applied_lot_meta("single_lot", [1]))

    assert meta["changed_fields"]
    _assert_lot_headers(result, "Lotto Unico", False)
    _assert_top_level_and_cdc_lot_fields_match(result)
    assert result["customer_decision_contract"]["is_multi_lot"] is False


def test_lot_consistency_sanitizer_weak_unknown_stays_conservative(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = {
        "is_multi_lot": True,
        "case_header": {"lotto": "Lotto Unico"},
        "report_header": {"lotto": {"value": "Lotto Unico"}, "is_multi_lot": True},
        "customer_decision_contract": {
            "is_multi_lot": True,
            "case_header": {"lotto": "Lotto Unico"},
            "report_header": {"lotto": {"value": "Lotto Unico"}, "is_multi_lot": True},
        },
    }

    sanitize_lot_field_consistency_for_customer(
        result,
        {"enabled": True, "status": "FAIL_OPEN", "applied": False, "authority_lot_mode": "unknown"},
    )

    assert "Lotto Unico" not in _flatten_text(result)
    assert "DA VERIFICARE" in _flatten_text(result)
    assert result.get("lots_count") is None
    _assert_top_level_and_cdc_lot_fields_match(result)


def test_lot_consistency_sanitizer_metadata_does_not_leak(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _lot_contradiction_result()
    meta = sanitize_lot_field_consistency_for_customer(
        result,
        {"enabled": True, "status": "NOT_APPLIED", "authority_lot_mode": "unknown"},
    )
    result["debug"] = {
        "authority_lot_field_consistency": meta,
        "removed_paths": ["result.case_header.lotto"],
        "authority_score": 0.9,
        "shadow_authority": {"lot": "unknown"},
    }

    sanitize_customer_facing_result(result)
    internal_runtime = separate_internal_runtime_from_customer_result(result)
    serialized = json.dumps(result, ensure_ascii=False)

    assert _collect_forbidden_keys(result) == []
    for forbidden in ("debug", "authority_lot_field_consistency", "removed_paths", "authority_score", "shadow_authority"):
        assert forbidden not in serialized
    assert "debug" in internal_runtime
    assert "authority" not in result["lot_verification_hint"].lower()
    assert "internal" not in result["lot_verification_hint"].lower()
    assert "debug" not in result["lot_verification_hint"].lower()


def test_lot_consistency_sanitizer_mutates_response_copy_only(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    source = _lot_contradiction_result()
    response_copy = copy.deepcopy(source)

    sanitize_lot_field_consistency_for_customer(
        response_copy,
        {"enabled": True, "status": "NOT_APPLIED", "authority_lot_mode": "unknown"},
    )

    assert "Lotto Unico" in _flatten_text(source)
    assert "Lotto Unico" not in _flatten_text(response_copy["case_header"])
    assert "DA VERIFICARE" in _flatten_text(response_copy)


def test_lot_consistency_sanitizer_weak_case_source_copy_only(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    source = _weak_unknown_single_lot_result()
    response_copy = copy.deepcopy(source)

    sanitize_lot_field_consistency_for_customer(
        response_copy,
        {"enabled": True, "status": "NOT_APPLIED", "authority_lot_mode": "unknown"},
    )

    assert source["case_header"]["lotto"] == "Lotto Unico"
    assert source["customer_decision_contract"].get("lots") is None
    assert response_copy["case_header"]["lotto"] == "DA VERIFICARE"
    assert response_copy["customer_decision_contract"]["lots"] == response_copy["lots"]


def test_stale_single_lot_narrative_syncs_top_level_and_cdc(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _customer_lot_narrative_result()

    sanitize_stale_lot_narratives_after_projection(result, _applied_lot_meta("single_lot", [1]))

    assert "2 lotti" not in _flatten_text(result["summary_for_client"])
    assert "2 lotti" not in _flatten_text(result["summary_for_client_bundle"])
    assert "2 lotti" not in _flatten_text(result["decision_rapida_narrated"])
    cdc = result["customer_decision_contract"]
    assert "2 lotti" not in _flatten_text(cdc["summary_for_client"])
    assert "2 lotti" not in _flatten_text(cdc["summary_for_client_bundle"])
    assert "2 lotti" not in _flatten_text(cdc["decision_rapida_narrated"])
    assert "Lotto 2" not in _flatten_text(cdc["section_9_legal_killers"])


def test_non_lot_summary_is_preserved_after_lot_narrative_sync(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = {
        "lots": [{"lot_number": 1}],
        "lots_count": 1,
        "is_multi_lot": False,
        "summary_for_client": {"summary_it": "Rischi documentati: verificare occupazione e costi."},
        "customer_decision_contract": {
            "summary_for_client": {"summary_it": "Rischi documentati: verificare occupazione e costi."}
        },
    }
    before = copy.deepcopy(result)

    meta = sanitize_stale_lot_narratives_after_projection(result, _applied_lot_meta("single_lot", [1]))

    assert result == before
    assert meta["status"] == "NO_STALE_LOT_NARRATIVE"


def test_valid_multilot_references_are_preserved_after_lot_narrative_sync(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = {
        "lots": [{"lot_number": 1}, {"lot_number": 2}, {"lot_number": 3}],
        "lots_count": 3,
        "is_multi_lot": True,
        "summary_for_client": {"summary_it": "La perizia riguarda 3 lotti. Lotto 1, Lotto 2 e Lotto 3 restano distinti."},
        "section_9_legal_killers": {
            "items": [
                {"killer": "Lotto 1: occupazione da verificare"},
                {"killer": "Lotto 2: difformità da verificare"},
                {"killer": "Lotto 3: formalità da verificare"},
            ]
        },
    }
    before = copy.deepcopy(result)

    meta = sanitize_stale_lot_narratives_after_projection(result, _applied_lot_meta("multi_lot", [1, 2, 3]))

    assert result == before
    assert "Lotto 1" in _flatten_text(result)
    assert "Lotto 2" in _flatten_text(result)
    assert "Lotto 3" in _flatten_text(result)
    assert meta["status"] == "NO_STALE_LOT_NARRATIVE"


def test_out_of_range_multilot_reference_is_removed_after_lot_narrative_sync(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = {
        "lots": [{"lot_number": 1}, {"lot_number": 2}, {"lot_number": 3}],
        "lots_count": 3,
        "is_multi_lot": True,
        "summary_for_client": {"summary_it": "La perizia riguarda 3 lotti. Lotto 4 contiene difformità."},
        "section_9_legal_killers": {
            "items": [
                {"killer": "Lotto 1: occupazione da verificare"},
                {"killer": "Lotto 4: difformità da verificare"},
            ]
        },
    }

    meta = sanitize_stale_lot_narratives_after_projection(result, _applied_lot_meta("multi_lot", [1, 2, 3]))

    serialized = _flatten_text(result)
    assert "Lotto 1" in serialized
    assert "Lotto 4" not in serialized
    assert result["section_9_legal_killers"]["items"] == [{"killer": "Lotto 1: occupazione da verificare"}]
    assert meta["removed_stale_lot_narrative_count"] > 0


def test_lot_narrative_sync_is_flag_off_invariant(monkeypatch):
    monkeypatch.delenv(FEATURE_FLAG, raising=False)
    result = _customer_lot_narrative_result()
    before = copy.deepcopy(result)

    meta = sanitize_stale_lot_narratives_after_projection(result, _applied_lot_meta("single_lot", [1]))

    assert result == before
    assert meta["status"] == "SKIPPED"


def test_lot_narrative_sync_mutates_response_copy_only(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    source = _customer_lot_narrative_result()
    response_copy = copy.deepcopy(source)

    sanitize_stale_lot_narratives_after_projection(response_copy, _applied_lot_meta("single_lot", [1]))

    assert "2 lotti" in _flatten_text(source)
    assert "Lotto 2" in _flatten_text(source)
    assert "2 lotti" not in _flatten_text(response_copy)
    assert "Lotto 2" not in _flatten_text(response_copy)


def test_lot_narrative_sync_metadata_does_not_leak_to_customer_response(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    result = _customer_lot_narrative_result()
    meta = sanitize_stale_lot_narratives_after_projection(result, _applied_lot_meta("single_lot", [1]))
    result["debug"] = {
        "authority_lot_projection": _applied_lot_meta("single_lot", [1]),
        "authority_lot_narrative_sync": meta,
        "section_zone": "FINAL_LOT_FORMATION",
        "authority_score": 0.99,
        "shadow_authority": {"lot": "single"},
    }

    sanitize_customer_facing_result(result)
    internal_runtime = separate_internal_runtime_from_customer_result(result)
    serialized = json.dumps(result, ensure_ascii=False)

    assert _collect_forbidden_keys(result) == []
    for forbidden in (
        "debug",
        "internal_runtime",
        "authority_lot_projection",
        "removed_paths",
        "section_zone",
        "authority_score",
        "shadow_authority",
    ):
        assert forbidden not in serialized
    assert "debug" in internal_runtime


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
