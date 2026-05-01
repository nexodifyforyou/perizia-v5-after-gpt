import copy
import json
import re
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import candidate_miner  # noqa: E402
from customer_decision_contract import (  # noqa: E402
    sanitize_customer_facing_result,
    separate_internal_runtime_from_customer_result,
)
from perizia_authority_resolvers import build_authority_shadow_resolvers  # noqa: E402
from perizia_section_authority import (  # noqa: E402
    AUTH_HIGH,
    AUTH_LOW,
    ZONE_FINAL_LOT,
    ZONE_FINAL_VALUATION,
    ZONE_FORMALITIES,
    ZONE_INSTRUCTION,
    ZONE_QUESTION,
    ZONE_TOC,
    build_section_authority_map,
    classify_page_authority,
    detect_domain_hints,
    detect_money_role_hints,
    summarize_authority_map,
)
from scripts.audit_authority_corpus import _audit_pdf, _extract_pdf_pages  # noqa: E402


FIXTURE_PATH = BACKEND_DIR / "tests" / "fixtures" / "perizia_authority_golden_cases.json"

AUTHORITY_INTERNAL_KEYS = {
    "section_zone",
    "authority_level",
    "authority_score",
    "domain_hint",
    "domain_hints",
    "answer_point",
    "reason_for_authority",
    "is_instruction_like",
    "is_answer_like",
    "source_stage",
    "extractor_version",
    "shadow_authority",
    "authority_resolver",
    "authority_shadow_resolvers",
}


def _load_cases() -> List[Dict[str, Any]]:
    with open(FIXTURE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _resolve_case_path(case: Dict[str, Any]) -> Optional[Path]:
    for raw_path in case.get("paths") or []:
        path = Path(raw_path).expanduser()
        if path.exists() and path.is_file():
            return path.resolve()
    return None


def _page_num(page: Dict[str, Any], default: int) -> int:
    try:
        return int(page.get("page") or page.get("page_number") or default)
    except Exception:
        return default


def _page_texts(pages: List[Dict[str, Any]]) -> Dict[int, str]:
    return {
        _page_num(page, idx): str(page.get("text") or "")
        for idx, page in enumerate(pages, start=1)
        if isinstance(page, dict)
    }


def _row_by_page(section_map: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    rows: Dict[int, Dict[str, Any]] = {}
    for row in section_map.get("pages") or []:
        if not isinstance(row, dict):
            continue
        try:
            rows[int(row.get("page"))] = row
        except Exception:
            continue
    return rows


@lru_cache(maxsize=None)
def _case_state(path_str: str) -> Dict[str, Any]:
    path = Path(path_str)
    pages = _extract_pdf_pages(path)
    section_map = build_section_authority_map(pages)
    return {
        "path": path,
        "pages": pages,
        "section_map": section_map,
        "summary": summarize_authority_map(section_map),
        "audit": _audit_pdf(path),
        "texts": _page_texts(pages),
        "rows": _row_by_page(section_map),
    }


def _shadow_for_state(state: Dict[str, Any]) -> Dict[str, Any]:
    return build_authority_shadow_resolvers(state["pages"], state["section_map"])


def _high_final_lotto_unico(state: Dict[str, Any]) -> bool:
    for page, row in state["rows"].items():
        if row.get("zone") != ZONE_FINAL_LOT or row.get("authority_level") != AUTH_HIGH:
            continue
        if re.search(r"\blotto\s+unico\b", state["texts"].get(page, ""), flags=re.IGNORECASE | re.UNICODE):
            return True
    return False


def _high_final_lot_numbers(state: Dict[str, Any]) -> set[int]:
    numbers: set[int] = set()
    for page, row in state["rows"].items():
        if row.get("zone") != ZONE_FINAL_LOT or row.get("authority_level") != AUTH_HIGH:
            continue
        text = state["texts"].get(page, "")
        for match in re.finditer(r"\blotto\s*(?:n\.?|nr\.?|numero)?\s*([1-9]\d*)\b", text, flags=re.IGNORECASE | re.UNICODE):
            numbers.add(int(match.group(1)))
    return numbers


def _collect_internal_key_hits(value: Any, path: str = "result") -> List[str]:
    hits: List[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            key_text = str(key)
            child_path = f"{path}.{key_text}"
            if key_text.startswith("authority_") or key_text in AUTHORITY_INTERNAL_KEYS:
                hits.append(child_path)
            hits.extend(_collect_internal_key_hits(child, child_path))
    elif isinstance(value, list):
        for idx, item in enumerate(value):
            hits.extend(_collect_internal_key_hits(item, f"{path}[{idx}]"))
    return hits


def _tainted_leaf() -> Dict[str, Any]:
    return {
        "headline_it": "Voce cliente",
        "authority_level": "HIGH_FACTUAL",
        "authority_score": 0.95,
        "section_zone": "FINAL_VALUATION",
        "domain_hints": ["money_valuation"],
        "answer_point": 12,
        "reason_for_authority": "shadow-only",
        "is_instruction_like": False,
        "is_answer_like": True,
        "source_stage": "candidate_miner",
        "extractor_version": "shadow-test",
        "authority_shadow": {"page": 1},
        "shadow_authority": {"page": 1},
        "authority_resolver": {"domain": "money"},
        "authority_shadow_resolvers": {"domain": "debug"},
    }


@pytest.mark.parametrize("case", _load_cases(), ids=lambda case: case["id"])
def test_golden_corpus_authority_expectations(case: Dict[str, Any]):
    pdf_path = _resolve_case_path(case)
    if pdf_path is None:
        pytest.skip(f"golden PDF not available for {case['id']}: {case.get('paths')}")

    state = _case_state(str(pdf_path))
    expectations = case.get("expectations") or {}
    summary = state["summary"]
    audit = state["audit"]

    assert summary["pages_total"] >= expectations.get("pages_total_min", 1)
    if expectations.get("expected_status"):
        assert audit["status"] == expectations["expected_status"]
    else:
        assert audit["status"] == "PASS"

    for row in state["rows"].values():
        if row.get("zone") in {ZONE_TOC, ZONE_INSTRUCTION, ZONE_QUESTION}:
            assert row.get("authority_level") == AUTH_LOW

    for page in summary.get("final_valuation_pages") or []:
        row = state["rows"][int(page)]
        assert row["zone"] == ZONE_FINAL_VALUATION
        assert row["authority_level"] == AUTH_HIGH
    for page in summary.get("formalities_pages") or []:
        row = state["rows"][int(page)]
        assert row["zone"] == ZONE_FORMALITIES
        assert row["authority_level"] == AUTH_HIGH

    if expectations.get("requires_high_lotto_unico"):
        assert _high_final_lotto_unico(state) is True
    if expectations.get("forbids_high_lotto_unico"):
        assert _high_final_lotto_unico(state) is False
    if expectations.get("forbids_high_lotto_n"):
        assert _high_final_lot_numbers(state) == set()
    if expectations.get("requires_high_lot_numbers"):
        assert set(expectations["requires_high_lot_numbers"]).issubset(_high_final_lot_numbers(state))

    assert len(summary.get("final_lot_formation_pages") or []) >= expectations.get("min_final_lot_formation_pages", 0)
    assert len(summary.get("final_valuation_pages") or []) >= expectations.get("min_final_valuation_pages", 0)
    assert len(summary.get("formalities_pages") or []) >= expectations.get("min_formalities_pages", 0)
    assert audit["money_rendita_catastale_count"] >= expectations.get("min_money_rendita_catastale_pages", 0)
    assert audit["money_valuation_count"] >= expectations.get("min_money_valuation_pages", 0)

    if "max_instruction_false_positive_suspects" in expectations:
        assert audit["instruction_false_positive_suspects_count"] <= expectations["max_instruction_false_positive_suspects"]
    if "max_answer_pages" in expectations:
        assert len(summary.get("answer_pages") or []) <= expectations["max_answer_pages"]

    high_price_or_value_pages = [
        page
        for page, row in state["rows"].items()
        if row.get("authority_level") == AUTH_HIGH
        and row.get("zone") in {ZONE_FINAL_LOT, ZONE_FINAL_VALUATION}
        and re.search(
            r"\b(prezzo\s+base|valore\s+finale|valore\s+di\s+stima|valutazione|stima)\b",
            state["texts"].get(page, ""),
            flags=re.IGNORECASE | re.UNICODE,
        )
    ]
    if expectations.get("min_final_valuation_pages", 0) or expectations.get("min_final_lot_formation_pages", 0):
        assert high_price_or_value_pages


@pytest.mark.parametrize("case", _load_cases(), ids=lambda case: case["id"])
def test_golden_corpus_shadow_resolver_outputs(case: Dict[str, Any]):
    pdf_path = _resolve_case_path(case)
    if pdf_path is None:
        pytest.skip(f"golden PDF not available for {case['id']}: {case.get('paths')}")

    state = _case_state(str(pdf_path))
    shadow = _shadow_for_state(state)
    for domain in ("lot_structure", "occupancy", "opponibilita", "legal_formalities", "money_roles"):
        assert isinstance(shadow.get(domain), dict)
        assert shadow[domain]["domain"]
        assert shadow[domain]["status"] in {"OK", "WARN", "FAIL_OPEN", "INSUFFICIENT_EVIDENCE"}
        assert isinstance(shadow[domain]["value"], dict)
        assert 0.0 <= float(shadow[domain]["confidence"]) <= 1.0
        assert isinstance(shadow[domain]["authority_basis"], dict)

    lot = shadow["lot_structure"]["value"]
    case_id = case["id"]
    if case_id == "1859886_c_perizia":
        assert lot["shadow_lot_mode"] == "single_lot"
        assert lot["has_high_authority_lotto_unico"] is True
        assert lot["has_high_authority_multilot"] is False
        assert shadow["lot_structure"]["confidence"] >= 0.85
    elif case_id == "multilot_69_2024":
        assert lot["shadow_lot_mode"] == "multi_lot"
        assert set(lot["detected_lot_numbers"]).issuperset({1, 2, 3})
        assert lot["has_high_authority_lotto_unico"] is False
    elif case_id == "ostuni_via_viterbo_2":
        assert shadow["status"] in {"OK", "WARN", "INSUFFICIENT_EVIDENCE", "FAIL_OPEN"}
        for domain in ("occupancy", "opponibilita", "legal_formalities", "money_roles"):
            assert shadow[domain]["status"] in {"OK", "WARN", "INSUFFICIENT_EVIDENCE", "FAIL_OPEN"}
    elif case_id in {"via_nuova_19_1", "via_del_mare_4591_4593"}:
        assert shadow["fail_open"] is True or shadow["lot_structure"]["confidence"] <= 0.35
        assert lot["shadow_lot_mode"] in {"unknown", "single_lot", "multi_lot"}
        if lot["shadow_lot_mode"] != "unknown":
            assert shadow["lot_structure"]["confidence"] < 0.5
    elif case_id == "via_umbria":
        money_counts = shadow["money_roles"]["value"].get("money_role_counts") or {}
        assert "buyer_cost" not in money_counts


def test_error_class_instruction_context_lot_money_and_formalities_rules():
    instruction = classify_page_authority(
        1,
        "Quesito: verifichi, accerti e indichi se l'immobile sia libero e se vi siano ipoteche.",
    )
    assert instruction["zone"] in {ZONE_INSTRUCTION, ZONE_QUESTION}
    assert instruction["authority_level"] == AUTH_LOW
    assert instruction["is_instruction_like"] is True

    toc = classify_page_authority(2, "INDICE\nLotto 1 ........ 10\nLotto 2 ........ 20\nFormalita ........ 30")
    assert toc["zone"] == ZONE_TOC
    assert toc["authority_level"] == AUTH_LOW

    procedural_lot = classify_page_authority(
        3,
        "Si richiama il Lotto 2 della precedente perizia e della procedura portante riunita.",
    )
    assert procedural_lot["authority_level"] == AUTH_LOW
    assert "procedure_context" in procedural_lot["domain_hints"]

    final_lot = classify_page_authority(
        4,
        "FORMAZIONE LOTTI\nLOTTO UNICO\nIl compendio e vendibile in un unico lotto.",
    )
    assert final_lot["zone"] == ZONE_FINAL_LOT
    assert final_lot["authority_level"] == AUTH_HIGH

    rendita_roles = detect_money_role_hints("Rendita catastale Euro 387,34, foglio 1, particella 2, subalterno 3.")
    assert "money_rendita_catastale" in rendita_roles
    assert "money_cost_signal" not in rendita_roles

    valuation_roles = detect_money_role_hints("Valore finale di stima: Euro 391.849,00 con deprezzamento applicato.")
    assert "money_valuation" in valuation_roles
    assert "money_cost_signal" not in valuation_roles

    formalities = classify_page_authority(
        5,
        "Formalita pregiudizievoli: iscrizione ipotecaria, trascrizione del pignoramento e registro particolare.",
    )
    assert formalities["zone"] == ZONE_FORMALITIES
    assert formalities["authority_level"] == AUTH_HIGH

    generic_legal = classify_page_authority(6, "Nel fascicolo e citata una ipoteca.")
    assert generic_legal["zone"] != ZONE_FORMALITIES
    assert generic_legal["authority_level"] != AUTH_HIGH

    hints = detect_domain_hints("Spese di cancellazione formalita Euro 500,00.")
    assert "money_formalities" in hints
    assert "money_cost_signal" not in hints


def test_payload_invariance_strips_authority_internals_from_customer_result():
    tainted = _tainted_leaf()
    result = {
        "authority_layer": "shadow",
        "authority_score": 1.0,
        "source_stage": "root",
        "issues": [copy.deepcopy(tainted)],
        "legal_killers": [copy.deepcopy(tainted)],
        "section_9_legal_killers": {"items": [copy.deepcopy(tainted)], "top_items": [copy.deepcopy(tainted)]},
        "red_flags_operativi": [copy.deepcopy(tainted)],
        "section_11_red_flags": [copy.deepcopy(tainted)],
        "summary_for_client": {"summary_it": "Sintesi", **copy.deepcopy(tainted)},
        "summary_for_client_bundle": {"decision_summary_it": "Sintesi", **copy.deepcopy(tainted)},
        "section_2_decisione_rapida": {"summary_it": "Decisione", **copy.deepcopy(tainted)},
        "section_3_money_box": {"items": [copy.deepcopy(tainted)], **copy.deepcopy(tainted)},
        "money_box": {"items": [copy.deepcopy(tainted)], **copy.deepcopy(tainted)},
        "customer_decision_contract": {
            "issues": [copy.deepcopy(tainted)],
            "legal_killers": [copy.deepcopy(tainted)],
            "section_9_legal_killers": {"items": [copy.deepcopy(tainted)]},
            "red_flags_operativi": [copy.deepcopy(tainted)],
            "section_11_red_flags": [copy.deepcopy(tainted)],
            "summary_for_client": {"summary_it": "Sintesi", **copy.deepcopy(tainted)},
            "summary_for_client_bundle": {"decision_summary_it": "Sintesi", **copy.deepcopy(tainted)},
            "section_2_decisione_rapida": {"summary_it": "Decisione", **copy.deepcopy(tainted)},
            "section_3_money_box": {"items": [copy.deepcopy(tainted)]},
            "money_box": {"items": [copy.deepcopy(tainted)]},
        },
        "debug": {"candidate": copy.deepcopy(tainted)},
    }

    sanitize_customer_facing_result(result)
    internal_runtime = separate_internal_runtime_from_customer_result(result)

    assert _collect_internal_key_hits(result) == []
    assert internal_runtime["debug"]["candidate"]["authority_level"] == "HIGH_FACTUAL"


def _write_miner_extract(tmp_path: Path, analysis_id: str, *, section_authority: Any = None, corrupt: bool = False) -> Path:
    extract_dir = tmp_path / analysis_id / "extract"
    extract_dir.mkdir(parents=True)
    pages_raw = [
        {
            "page": 1,
            "text": "Valore finale di stima Euro 100.000,00. Il bene risulta occupato dal debitore.",
        },
        {
            "page": 2,
            "text": "Prezzo base Euro 80.000,00. Formalita: pignoramento e ipoteca.",
        },
    ]
    for name, payload in {
        "pages_raw.json": pages_raw,
        "metrics.json": [],
        "ocr_plan.json": [],
    }.items():
        with open(extract_dir / name, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
    if corrupt:
        with open(extract_dir / "section_authority.json", "w", encoding="utf-8") as f:
            f.write("{ not valid json")
    elif section_authority is not None:
        with open(extract_dir / "section_authority.json", "w", encoding="utf-8") as f:
            json.dump(section_authority, f, ensure_ascii=False)
    return extract_dir


def test_candidate_miner_missing_section_authority_map_fails_open(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    analysis_id = "analysis_missing_authority"
    _write_miner_extract(tmp_path, analysis_id)
    monkeypatch.setattr(candidate_miner, "RUNS_ROOT", tmp_path)

    result = candidate_miner.run_candidate_miner_for_analysis(analysis_id)

    assert result["money_count"] > 0
    assert result["trigger_count"] > 0
    assert result["authority_tagging"]["money"]["enabled"] is False
    assert result["authority_tagging"]["money"]["status"] == "missing_map"
    assert result["authority_tagging"]["triggers"]["status"] == "missing_map"


def test_candidate_miner_corrupt_section_authority_map_fails_open(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    analysis_id = "analysis_corrupt_authority"
    _write_miner_extract(tmp_path, analysis_id, corrupt=True)
    monkeypatch.setattr(candidate_miner, "RUNS_ROOT", tmp_path)

    result = candidate_miner.run_candidate_miner_for_analysis(analysis_id)

    assert result["money_count"] > 0
    assert result["trigger_count"] > 0
    assert result["authority_tagging"]["money"]["enabled"] is False
    assert result["authority_tagging"]["money"]["status"] == "corrupt_map"
    assert result["authority_tagging"]["triggers"]["status"] == "corrupt_map"


def test_candidate_miner_bad_authority_classification_on_one_page_fails_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    analysis_id = "analysis_partial_authority"
    section_map = build_section_authority_map(
        [
            {"page_number": 1, "text": "Valore finale di stima Euro 100.000,00."},
            {"page_number": 2, "text": "Prezzo base Euro 80.000,00."},
        ]
    )
    _write_miner_extract(tmp_path, analysis_id, section_authority=section_map)
    monkeypatch.setattr(candidate_miner, "RUNS_ROOT", tmp_path)

    original = candidate_miner.classify_quote_authority

    def flaky_classifier(page_number: int, quote: str, section_map: Dict[str, Any], domain: Optional[str] = None):
        if int(page_number) == 1:
            raise ValueError("bad page authority row")
        return original(page_number, quote, section_map, domain=domain)

    monkeypatch.setattr(candidate_miner, "classify_quote_authority", flaky_classifier)

    result = candidate_miner.run_candidate_miner_for_analysis(analysis_id)

    assert result["money_count"] >= 2
    assert result["authority_tagging"]["money"]["enabled"] is True
    assert result["authority_tagging"]["money"]["status"] == "partial"
    assert result["authority_tagging"]["money"]["failed_count"] >= 1
    assert result["authority_tagging"]["money"]["tagged_count"] >= 1
