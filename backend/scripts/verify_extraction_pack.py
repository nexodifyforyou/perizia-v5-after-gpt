#!/usr/bin/env python3
import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from pymongo import MongoClient
from PyPDF2 import PdfReader


REQUIRED_FILES = [
    "pages_raw.json",
    "full_raw.txt",
    "words_raw.json",
    "metrics.json",
    "ocr_plan.json",
    "quality.json",
]

SEARCH_DIRS = [
    Path("/tmp"),
    Path("/srv/perizia/app/uploads"),
    Path("/srv/perizia/app/backend/uploads"),
]


def _fail(messages: List[str]) -> None:
    print("FAIL:")
    for msg in messages:
        print(f"- {msg}")
    sys.exit(1)


def _load_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_analysis_doc(analysis_id: str) -> Optional[Dict[str, Any]]:
    env_path = Path("/srv/perizia/app/backend/.env")
    if env_path.exists():
        load_dotenv(env_path)

    mongo_url = os.environ.get("MONGO_URL")
    db_name = os.environ.get("DB_NAME")
    if mongo_url and db_name:
        client = None
        try:
            client = MongoClient(mongo_url, serverSelectionTimeoutMS=3000)
            client.admin.command("ping")
            doc = client[db_name]["perizia_analyses"].find_one({"analysis_id": analysis_id}, {"_id": 0})
            if isinstance(doc, dict):
                return doc
        except Exception:
            pass
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass

    offline_path = Path("/tmp/perizia_qa_run/analysis.json")
    if offline_path.exists():
        try:
            data = _load_json(offline_path)
            if isinstance(data, dict) and data.get("analysis_id") == analysis_id:
                return data
        except Exception:
            return None
    return None


def _resolve_expected_pages(doc: Optional[Dict[str, Any]]) -> Optional[int]:
    if not isinstance(doc, dict):
        return None

    file_name = doc.get("file_name")
    if isinstance(file_name, str) and file_name.strip():
        target_name = Path(file_name.strip()).name
        for base in SEARCH_DIRS:
            if not base.exists():
                continue
            try:
                for candidate in base.rglob(target_name):
                    if candidate.exists() and candidate.is_file():
                        try:
                            return len(PdfReader(str(candidate)).pages)
                        except Exception:
                            continue
            except Exception:
                continue

    result = doc.get("result")
    if isinstance(result, dict):
        run = result.get("run")
        if isinstance(run, dict):
            inp = run.get("input")
            if isinstance(inp, dict) and isinstance(inp.get("pages_total"), int) and inp.get("pages_total") > 0:
                return int(inp.get("pages_total"))

    if isinstance(doc.get("pages_count"), int) and doc.get("pages_count") > 0:
        return int(doc.get("pages_count"))

    for key in ("source_pdf_path", "pdf_path", "file_path", "input_path", "upload_path"):
        path_value = doc.get(key)
        if isinstance(path_value, str) and path_value.strip():
            pdf_path = Path(path_value.strip())
            if pdf_path.exists() and pdf_path.is_file():
                try:
                    return len(PdfReader(str(pdf_path)).pages)
                except Exception:
                    continue
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify extraction pack created under _qa/runs/<analysis_id>/extract.")
    parser.add_argument("--analysis-id", required=True, dest="analysis_id")
    args = parser.parse_args()

    analysis_id = args.analysis_id.strip()
    extract_dir = Path("/srv/perizia/_qa/runs") / analysis_id / "extract"
    errors: List[str] = []

    if not extract_dir.exists() or not extract_dir.is_dir():
        _fail([f"Missing extraction folder: {extract_dir}"])

    for name in REQUIRED_FILES:
        file_path = extract_dir / name
        if not file_path.exists() or not file_path.is_file():
            errors.append(f"Missing required file: {file_path}")

    if errors:
        _fail(errors)

    pages_raw = _load_json(extract_dir / "pages_raw.json")
    metrics = _load_json(extract_dir / "metrics.json")
    ocr_plan = _load_json(extract_dir / "ocr_plan.json")
    quality = _load_json(extract_dir / "quality.json")

    if not isinstance(pages_raw, list):
        errors.append("pages_raw.json must contain a list")
        pages_raw = []
    if not isinstance(metrics, list):
        errors.append("metrics.json must contain a list")
        metrics = []
    if not isinstance(ocr_plan, list):
        errors.append("ocr_plan.json must contain a list")
        ocr_plan = []
    if not isinstance(quality, dict):
        errors.append("quality.json must contain an object")

    pages_count_from_pack = len(pages_raw)
    doc = _load_analysis_doc(analysis_id)
    expected_pages = _resolve_expected_pages(doc)
    if expected_pages is None:
        errors.append("Could not resolve expected PDF page count from stored metadata or source path")
    elif pages_count_from_pack != expected_pages:
        errors.append(
            f"pages_raw.json page count mismatch: got {pages_count_from_pack}, expected {expected_pages}"
        )

    metric_pages = sorted(
        int(item.get("page"))
        for item in metrics
        if isinstance(item, dict) and isinstance(item.get("page"), int)
    )
    expected_range = list(range(1, pages_count_from_pack + 1))
    if metric_pages != expected_range:
        errors.append(
            f"metrics.json page coverage mismatch: got {metric_pages[:10]}{'...' if len(metric_pages) > 10 else ''}, expected 1..{pages_count_from_pack}"
        )

    ocr_plan_pages = []
    for item in ocr_plan:
        if isinstance(item, dict) and isinstance(item.get("page"), int):
            ocr_plan_pages.append(int(item["page"]))
    invalid_ocr_pages = sorted(set(p for p in ocr_plan_pages if p < 1 or p > pages_count_from_pack))
    if invalid_ocr_pages:
        errors.append(f"ocr_plan.json has pages out of range: {invalid_ocr_pages}")

    if errors:
        _fail(errors)

    print(
        "PASS: "
        f"analysis_id={analysis_id} "
        f"pages={pages_count_from_pack} "
        f"metrics_entries={len(metrics)} "
        f"ocr_pages={sorted(set(ocr_plan_pages))}"
    )


if __name__ == "__main__":
    main()
