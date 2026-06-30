"""
Artifact storage for Correctness Mode v2 jobs.

Each job gets an isolated folder under the artifacts root:

    {ARTIFACTS_ROOT}/jobs/{job_id}/
        job_status.json
        input_pages.json
        pdf_quality_report.json
        error.json            (only when failed)

The default root is ``/srv/perizia/app/_correctness_v2`` but is overridable via
the ``CORRECTNESS_V2_ARTIFACTS_ROOT`` env var (used by tests to write to a temp
dir). Writes are atomic-ish (temp file + os.replace) and JSON is timestamped.

Local absolute paths are debugging info for admins only; the job_status
sanitizer (see job_status.sanitize_for_customer) is responsible for hiding them
from non-admin/customer responses.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

_DEFAULT_ROOT = "/srv/perizia/app/_correctness_v2"

JOB_STATUS_FILE = "job_status.json"
INPUT_PAGES_FILE = "input_pages.json"
PDF_QUALITY_FILE = "pdf_quality_report.json"
ERROR_FILE = "error.json"
# Step 2 artifacts.
ANALYST_WORKSHEET_FILE = "analyst_worksheet.json"
VERIFIED_CONTRACT_FILE = "verified_report_contract.json"
OPENAI_REQUEST_FILE = "openai_request.json"
OPENAI_RESPONSE_FILE = "openai_response.json"
VALIDATOR_REPORT_FILE = "validator_report.json"
LOT_REPORT_FILE = "lot_report.json"
# Step 3 lot-aware artifacts.
LOT_INDEX_FILE = "lot_index.json"
PER_LOT_PACKETS_FILE = "per_lot_packets.json"
SELECTED_LOT_CONTEXT_FILE = "selected_lot_context.json"
LOT_SELECTION_REQUIRED_FILE = "lot_selection_required.json"
ANALYZE_ALL_RESULT_FILE = "analyze_all_result.json"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def artifacts_root() -> Path:
    raw = os.environ.get("CORRECTNESS_V2_ARTIFACTS_ROOT")
    return Path(raw.strip()) if raw and raw.strip() else Path(_DEFAULT_ROOT)


def jobs_root() -> Path:
    return artifacts_root() / "jobs"


def job_dir(job_id: str) -> Path:
    return jobs_root() / str(job_id)


def ensure_job_dir(job_id: str) -> Path:
    """Create (if needed) and return the job folder."""
    path = job_dir(job_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_json(path: Path, data: Any) -> None:
    """Write JSON safely (temp file + replace)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    os.replace(tmp, path)


def save_json(job_id: str, filename: str, data: Dict[str, Any]) -> str:
    """
    Save a JSON artifact for the job and return its absolute path string.

    A ``_saved_at`` timestamp is injected if the payload is a dict and does not
    already carry one.
    """
    ensure_job_dir(job_id)
    path = job_dir(job_id) / filename
    if isinstance(data, dict) and "_saved_at" not in data:
        data = {**data, "_saved_at": _now_iso()}
    _write_json(path, data)
    return str(path)


def save_job_status(job_id: str, status_payload: Dict[str, Any]) -> str:
    return save_json(job_id, JOB_STATUS_FILE, status_payload)


def save_input_pages(job_id: str, pages: List[Dict[str, Any]]) -> str:
    payload = {
        "_saved_at": _now_iso(),
        "page_count": len(pages or []),
        "pages": pages or [],
    }
    return save_json(job_id, INPUT_PAGES_FILE, payload)


def save_pdf_quality_report(job_id: str, report: Dict[str, Any]) -> str:
    return save_json(job_id, PDF_QUALITY_FILE, report)


def save_error(job_id: str, error_payload: Dict[str, Any]) -> str:
    return save_json(job_id, ERROR_FILE, error_payload)


def save_analyst_worksheet(job_id: str, worksheet: Dict[str, Any]) -> str:
    return save_json(job_id, ANALYST_WORKSHEET_FILE, worksheet)


def save_verified_contract(job_id: str, contract: Dict[str, Any]) -> str:
    return save_json(job_id, VERIFIED_CONTRACT_FILE, contract)


def save_openai_request(job_id: str, request_payload: Dict[str, Any]) -> str:
    """Persist the redacted OpenAI request (must NOT contain secrets)."""
    return save_json(job_id, OPENAI_REQUEST_FILE, request_payload)


def save_openai_response(job_id: str, response_payload: Dict[str, Any]) -> str:
    return save_json(job_id, OPENAI_RESPONSE_FILE, response_payload)


def save_validator_report(job_id: str, report: Dict[str, Any]) -> str:
    return save_json(job_id, VALIDATOR_REPORT_FILE, report)


def save_lot_report(job_id: str, report: Dict[str, Any]) -> str:
    return save_json(job_id, LOT_REPORT_FILE, report)


def save_lot_index(job_id: str, index: Dict[str, Any]) -> str:
    return save_json(job_id, LOT_INDEX_FILE, index)


def save_per_lot_packets(job_id: str, packets: Dict[str, Any]) -> str:
    return save_json(job_id, PER_LOT_PACKETS_FILE, packets)


def save_selected_lot_context(job_id: str, context: Dict[str, Any]) -> str:
    return save_json(job_id, SELECTED_LOT_CONTEXT_FILE, context)


def save_lot_selection_required(job_id: str, payload: Dict[str, Any]) -> str:
    return save_json(job_id, LOT_SELECTION_REQUIRED_FILE, payload)


def save_analyze_all_result(job_id: str, payload: Dict[str, Any]) -> str:
    return save_json(job_id, ANALYZE_ALL_RESULT_FILE, payload)


def save_lot_subartifact(job_id: str, lot_id: str, filename: str, data: Dict[str, Any]) -> str:
    """Save a per-lot artifact under jobs/{job_id}/lots/{lot_id}/{filename} (analyze_all)."""
    safe_lot = str(lot_id).replace("/", "_").replace("..", "_")
    rel = os.path.join("lots", safe_lot, filename)
    return save_json(job_id, rel, data)


def read_json(job_id: str, filename: str) -> Optional[Dict[str, Any]]:
    path = job_dir(job_id) / filename
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def read_job_status(job_id: str) -> Optional[Dict[str, Any]]:
    return read_json(job_id, JOB_STATUS_FILE)


def list_jobs() -> List[str]:
    root = jobs_root()
    if not root.exists():
        return []
    return sorted(p.name for p in root.iterdir() if p.is_dir())


def latest_job_for_analysis(analysis_id: str) -> Optional[Dict[str, Any]]:
    """
    Return the most recently updated job_status.json whose analysis_id matches.

    Sorted by updated_at (falling back to created_at). Returns None if none.
    """
    candidates: List[Dict[str, Any]] = []
    for jid in list_jobs():
        status = read_job_status(jid)
        if isinstance(status, dict) and str(status.get("analysis_id")) == str(analysis_id):
            candidates.append(status)
    if not candidates:
        return None
    candidates.sort(
        key=lambda s: str(s.get("updated_at") or s.get("created_at") or ""),
        reverse=True,
    )
    return candidates[0]
