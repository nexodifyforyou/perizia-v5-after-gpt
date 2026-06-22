from __future__ import annotations

import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


_INTERNAL_META_KEYS = ("_rejected_payload", "_rejected_text")


def _safe_component(value: Any, fallback: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "").strip()).strip("._")
    return cleaned[:120] or fallback


def pop_rejected_narration_data(meta: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(meta, dict):
        return {}
    rejected: Dict[str, Any] = {}
    for key in _INTERNAL_META_KEYS:
        value = meta.pop(key, None)
        if value not in (None, "", {}, []):
            rejected[key.lstrip("_")] = value
    return rejected


def store_rejected_narration_artifact(
    *,
    analysis_id: str,
    case_id: Optional[str],
    run_id: Optional[str],
    provider: Optional[str],
    model: Optional[str],
    narrator_meta: Dict[str, Any],
    rejected_data: Dict[str, Any],
    artifact_root: Optional[Path] = None,
) -> Optional[Path]:
    """Persist a rejected narration attempt outside all customer response models."""
    if not rejected_data:
        return None
    root = artifact_root or Path(os.environ.get("PERIZIA_QA_RUNS_ROOT", "/srv/perizia/_qa/runs"))
    analysis_component = _safe_component(analysis_id, "analysis_unknown")
    run_dir = root / analysis_component
    run_dir.mkdir(parents=True, exist_ok=True)
    path = run_dir / "rejected_narration.json"
    payload = {
        "analysis_id": str(analysis_id or ""),
        "case_id": str(case_id or ""),
        "run_id": str(run_id or ""),
        "provider": str(provider or ""),
        "model": str(model or ""),
        "validation_error": narrator_meta.get("error") or ((narrator_meta.get("errors") or [None])[0]),
        "validation_errors": list(narrator_meta.get("errors") or [])[:20],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "fallback_applied": True,
        "generation_mode_attempted": "gemini_clean_contract",
        "final_fallback_generation_mode": "deterministic_separated_fallback",
        **rejected_data,
    }
    fd, temporary = tempfile.mkstemp(prefix=".rejected_narration.", suffix=".json", dir=str(run_dir))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return path
