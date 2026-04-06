from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from perizia_qa.comparators import compare_expected_to_actual, compare_legacy_and_verifier, extract_fixture_actuals
from perizia_qa.invariants import run_invariants
from perizia_qa.reports import build_report
from perizia_runtime.runtime import run_quality_verifier

FIXTURES_ROOT = Path(__file__).resolve().parent / "fixtures"


def _load_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _fixture_dir(name: str) -> Path:
    fixture_dir = FIXTURES_ROOT / name.strip().lower()
    if not fixture_dir.exists():
        available = sorted(path.name for path in FIXTURES_ROOT.iterdir() if path.is_dir()) if FIXTURES_ROOT.exists() else []
        raise ValueError(f"Unknown fixture: {name}. Available fixtures: {', '.join(available)}")
    return fixture_dir


def _build_named_fixture(name: str) -> Dict[str, Any]:
    fixture_dir = _fixture_dir(name)
    metadata = _load_json(fixture_dir / "metadata.json")
    result = _load_json(fixture_dir / "result_seed.json")
    raw_pages = _load_json(fixture_dir / "pages_raw.json")
    pages = [
        {
            "page_number": int(row.get("page_number") or row.get("page") or idx),
            "text": str(row.get("text") or ""),
        }
        for idx, row in enumerate(raw_pages or [], start=1)
        if isinstance(row, dict)
    ]
    full_text = "\n\n".join(page["text"] for page in pages)
    return {
        "analysis_id": str(metadata.get("analysis_id") or name.strip().lower()),
        "metadata": metadata,
        "expected": _load_json(fixture_dir / "expected.json"),
        "result": result,
        "pages": pages,
        "full_text": full_text,
    }


def run_named_fixture(name: str) -> Dict[str, Any]:
    fixture = _build_named_fixture(name)
    payload = run_quality_verifier(
        analysis_id=fixture["analysis_id"],
        result=fixture["result"],
        pages=fixture["pages"],
        full_text=fixture.get("full_text") or "\n\n".join(str(page.get("text") or "") for page in fixture["pages"]),
    )
    invariant_results = run_invariants(payload)
    legacy_vs_verifier = compare_legacy_and_verifier(fixture["result"], payload)
    expected_actual = extract_fixture_actuals(payload)
    expected_actual["fixture_name"] = fixture["metadata"].get("fixture_name")
    expected_actual["source_analysis_id"] = fixture["metadata"].get("source_analysis_id")
    expected_actual["seed_semaforo_status"] = (
        ((fixture["result"].get("semaforo_generale") or {}).get("status"))
        if isinstance(fixture["result"].get("semaforo_generale"), dict)
        else None
    )
    expected_actual["tags"] = fixture["metadata"].get("tags") or []
    expected_results = compare_expected_to_actual(fixture["expected"], expected_actual)
    return build_report(payload, invariant_results, legacy_vs_verifier, expected_results)
