"""
LLM clarification resolution pack builder.

Produces: llm_resolution_pack.json

The LLM is only a bounded clarification layer over issue packets produced from
deterministic artifacts. Unsupported resolved values are downgraded after the
call rather than accepted.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import ssl
import urllib.error
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional, Set

from .corpus_registry import list_case_keys
from .llm_clarification_issue_pack import build_clarification_issue_pack, select_issues
from .runner import build_context


PROVIDER = "openai"
DEFAULT_MODEL = "gpt-4o-mini"
CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"
ALLOWED_OUTCOMES = {"resolved", "unresolved_explained", "upgraded_context"}


class LLMResolutionUnavailable(RuntimeError):
    pass


def _load_dotenv_value(path: Path, name: str) -> Optional[str]:
    if not path.exists():
        return None
    for line in path.read_text(errors="ignore").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        if key.strip() == name:
            return value.strip().strip('"').strip("'") or None
    return None


def discover_openai_config() -> Dict[str, Optional[str]]:
    key = os.environ.get("OPENAI_API_KEY")
    source = "environment" if key else None
    if not key:
        dotenv = Path("/srv/perizia/app/backend/.env")
        key = _load_dotenv_value(dotenv, "OPENAI_API_KEY")
        if key:
            source = str(dotenv)
    model = (
        os.environ.get("CANONICAL_LLM_MODEL")
        or os.environ.get("OPENAI_MODEL")
        or os.environ.get("NARRATOR_MODEL")
        or _load_dotenv_value(Path("/srv/perizia/app/backend/.env"), "NARRATOR_MODEL")
        or DEFAULT_MODEL
    )
    return {
        "provider": PROVIDER,
        "env_var": "OPENAI_API_KEY",
        "key_source": source,
        "key_found": bool(key),
        "api_key": key,
        "model": model,
    }


def _json_from_response(text: str) -> Dict:
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, flags=re.S)
        if not match:
            raise
        return json.loads(match.group(0))


def _issue_payload(issue: Dict) -> Dict:
    return {
        "issue_id": issue.get("issue_id"),
        "case_key": issue.get("case_key"),
        "field_family": issue.get("field_family"),
        "field_type": issue.get("field_type"),
        "lot_id": issue.get("lot_id"),
        "bene_id": issue.get("bene_id"),
        "issue_type": issue.get("issue_type"),
        "deterministic_status": issue.get("deterministic_status"),
        "reason_codes": issue.get("reason_codes"),
        "candidate_values": issue.get("candidate_values"),
        "blocked_values": issue.get("blocked_values"),
        "source_line_indices": issue.get("source_line_indices"),
        "shell_quotes": issue.get("shell_quotes"),
        "supporting_candidates": issue.get("supporting_candidates"),
        "supporting_blocked_entries": issue.get("supporting_blocked_entries"),
        "source_pages": issue.get("source_pages"),
        "local_text_windows": issue.get("local_text_windows"),
        "table_zone_types": issue.get("table_zone_types"),
        "scope_metadata": issue.get("scope_metadata"),
        "needs_llm": issue.get("needs_llm"),
        "shell_sources": issue.get("shell_sources"),
    }


def _system_prompt() -> str:
    return (
        "You are a bounded clarification layer for PeriziaScan canonical artifacts. "
        "Use only the supplied issue packet evidence. Do not re-extract the document, "
        "do not summarize the perizia, do not make investment recommendations, and do "
        "not invent values. Resolve to one value only when a supplied candidate/text "
        "window strongly supports it. Otherwise keep it unresolved and explain why. "
        "Return only valid JSON."
    )


def _user_prompt(issue: Dict) -> str:
    schema = {
        "issue_id": "same issue id",
        "llm_outcome": "resolved | unresolved_explained | upgraded_context",
        "resolved_value": "string or null",
        "resolved_value_type": "field value type or null",
        "confidence_band": "high | medium | low",
        "user_visible_explanation": "short Italian explanation grounded in evidence",
        "supporting_evidence": [{"page": 0, "quote": "short quote from supplied evidence", "reason": "why it supports outcome"}],
        "rejected_alternatives": [{"value": "string", "reason": "why rejected"}],
        "why_not_resolved": "required when unresolved_explained or upgraded_context, else null",
        "needs_human_review": True,
        "source_pages": [0],
    }
    return json.dumps(
        {
            "task": "Resolve this one deterministic issue packet if, and only if, evidence is strong.",
            "strict_rules": [
                "Never output a resolved_value that is not present in supporting candidates or local text windows.",
                "For FIELD_CONFLICT choose resolved only if the supplied snippets clearly say one value is the final/current value.",
                "For SUSPICIOUS_SILENCE or GROUPED_CONTEXT_NEEDS_EXPLANATION prefer upgraded_context when the text gives context but not a fixed field value.",
                "For SCOPE_AMBIGUITY prefer unresolved_explained unless the supplied windows clearly assign scope.",
                "For TABLE_RECAP_DUPLICATE_UNCLEAR prefer unresolved_explained unless the recap and source value agree on the same value and scope.",
                "For upgraded_context, explain why the context should be surfaced but not treated as a final normalized field value.",
                "For unresolved_explained, why_not_resolved must explain the conflict, ambiguity, or unsafe scope.",
                "Keep needs_human_review true unless confidence is high and resolved_value is directly quoted.",
            ],
            "output_schema": schema,
            "issue_packet": _issue_payload(issue),
        },
        ensure_ascii=False,
    )


def _call_openai_json(api_key: str, model: str, issue: Dict, timeout_seconds: int = 45) -> Dict:
    body = {
        "model": model,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": _system_prompt()},
            {"role": "user", "content": _user_prompt(issue)},
        ],
    }
    req = urllib.request.Request(
        CHAT_COMPLETIONS_URL,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": "Bearer " + api_key,
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds, context=ssl.create_default_context()) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise LLMResolutionUnavailable(f"OpenAI HTTP error status={exc.code}") from exc
    except Exception as exc:
        raise LLMResolutionUnavailable(f"OpenAI request failed type={type(exc).__name__}") from exc
    content = payload["choices"][0]["message"]["content"]
    return _json_from_response(content)


def _all_evidence_text(issue: Dict) -> str:
    parts: List[str] = []
    for candidate in issue.get("supporting_candidates") or []:
        for key in ("extracted_value", "quote", "context", "context_window"):
            if candidate.get(key):
                parts.append(str(candidate[key]))
    for blocked in issue.get("supporting_blocked_entries") or []:
        for key in ("extracted_value", "quote", "line_quote", "context", "reason", "occupancy_status_raw"):
            if blocked.get(key):
                parts.append(str(blocked[key]))
        for value in blocked.get("distinct_values") or []:
            parts.append(str(value))
    for window in issue.get("local_text_windows") or []:
        parts.append(str(window.get("text") or ""))
    return "\n".join(parts)


def _candidate_values(issue: Dict) -> Set[str]:
    values: Set[str] = set()
    for value in issue.get("candidate_values") or []:
        values.add(str(value).strip())
    for value in issue.get("blocked_values") or []:
        values.add(str(value).strip())
    for candidate in issue.get("supporting_candidates") or []:
        if candidate.get("extracted_value") is not None:
            values.add(str(candidate["extracted_value"]).strip())
    for blocked in issue.get("supporting_blocked_entries") or []:
        if blocked.get("extracted_value") is not None:
            values.add(str(blocked["extracted_value"]).strip())
        for value in blocked.get("distinct_values") or []:
            values.add(str(value).strip())
    return {v for v in values if v}


def _normalize_for_match(text: str) -> str:
    return re.sub(r"\s+", " ", text).casefold().strip()


def _issue_pages(issue: Dict) -> Set[int]:
    return {int(p) for p in issue.get("source_pages") or [] if isinstance(p, int)}


def _evidence_page_warnings(issue: Dict, supporting_evidence: List[Dict]) -> List[str]:
    warnings: List[str] = []
    pages = _issue_pages(issue)
    for idx, evidence in enumerate(supporting_evidence):
        page = evidence.get("page") if isinstance(evidence, dict) else None
        if page not in pages:
            warnings.append(f"supporting_evidence[{idx}] page {page!r} not in issue source_pages")
    return warnings


def _validate_resolution(issue: Dict, raw_resolution: Dict, provider: str, model: str) -> Dict:
    warnings: List[str] = []
    outcome = raw_resolution.get("llm_outcome")
    if outcome not in ALLOWED_OUTCOMES:
        warnings.append(f"Invalid llm_outcome {outcome!r}; downgraded to unresolved_explained.")
        outcome = "unresolved_explained"
    resolved_value = raw_resolution.get("resolved_value")
    evidence_text = _normalize_for_match(_all_evidence_text(issue))
    candidate_values = _candidate_values(issue)
    if outcome == "resolved":
        value_text = "" if resolved_value is None else str(resolved_value).strip()
        value_ok = bool(value_text) and (
            value_text in candidate_values or _normalize_for_match(value_text) in evidence_text
        )
        if not value_ok:
            warnings.append("Resolved value was not present in bounded evidence; downgraded.")
            outcome = "unresolved_explained"
            resolved_value = None
    else:
        resolved_value = None

    source_pages = raw_resolution.get("source_pages") or issue.get("source_pages") or []
    if not isinstance(source_pages, list):
        source_pages = issue.get("source_pages") or []
    source_pages = [p for p in source_pages if p in _issue_pages(issue)]
    if not source_pages:
        source_pages = issue.get("source_pages") or []

    supporting_evidence = raw_resolution.get("supporting_evidence") if isinstance(raw_resolution.get("supporting_evidence"), list) else []
    warnings.extend(_evidence_page_warnings(issue, supporting_evidence))
    resolved_value_type = raw_resolution.get("resolved_value_type")
    if outcome == "resolved":
        if not resolved_value_type:
            resolved_value_type = issue.get("field_type")
    else:
        resolved_value_type = None

    why_not_resolved = raw_resolution.get("why_not_resolved")
    if outcome in {"unresolved_explained", "upgraded_context"} and not why_not_resolved:
        why_not_resolved = (
            "Il pacchetto non contiene evidenza abbastanza forte per emettere un valore fisso."
            if outcome == "unresolved_explained"
            else "Il pacchetto contiene contesto utile, ma non un valore normalizzato sicuro."
        )

    resolution = {
        "issue_id": issue["issue_id"],
        "llm_outcome": outcome,
        "resolved_value": resolved_value,
        "resolved_value_type": resolved_value_type,
        "confidence_band": raw_resolution.get("confidence_band") if raw_resolution.get("confidence_band") in {"high", "medium", "low"} else "low",
        "user_visible_explanation": str(raw_resolution.get("user_visible_explanation") or "").strip(),
        "supporting_evidence": supporting_evidence,
        "rejected_alternatives": raw_resolution.get("rejected_alternatives") if isinstance(raw_resolution.get("rejected_alternatives"), list) else [],
        "why_not_resolved": why_not_resolved if outcome in {"unresolved_explained", "upgraded_context"} else None,
        "needs_human_review": bool(raw_resolution.get("needs_human_review", True)),
        "provider": provider,
        "model": model,
        "source_pages": source_pages,
    }
    if warnings:
        resolution["validation_warnings"] = warnings
        resolution["needs_human_review"] = True
    if not resolution["user_visible_explanation"]:
        resolution["user_visible_explanation"] = "Caso lasciato irrisolto: il pacchetto non contiene evidenza sufficiente per mostrare un valore certo."
        resolution["needs_human_review"] = True
    if resolution["llm_outcome"] == "upgraded_context" and not resolution["why_not_resolved"]:
        resolution["why_not_resolved"] = "Contesto utile, ma non valore finale normalizzabile."
    return resolution


def build_llm_resolution_pack(
    case_key: str,
    *,
    issue_type: Optional[str] = None,
    field_family: Optional[str] = None,
    field_type: Optional[str] = None,
    limit: int = 1,
) -> Dict[str, object]:
    config = discover_openai_config()
    if not config["key_found"]:
        raise LLMResolutionUnavailable("OPENAI_API_KEY missing")
    ctx = build_context(case_key)
    issue_pack = build_clarification_issue_pack(case_key)
    selected = select_issues(
        issue_pack,
        issue_type=issue_type,
        field_family=field_family,
        field_type=field_type,
        limit=limit,
    )
    if not selected:
        raise LLMResolutionUnavailable("No matching clarification issues")

    resolutions: List[Dict] = []
    warnings: List[str] = []
    for issue in selected:
        raw = _call_openai_json(str(config["api_key"]), str(config["model"]), issue)
        resolutions.append(_validate_resolution(issue, raw, PROVIDER, str(config["model"])))

    out = {
        "case_key": case_key,
        "status": "OK",
        "issue_count": len(selected),
        "issues": selected,
        "resolutions": resolutions,
        "warnings": warnings,
        "source_artifacts": issue_pack.get("source_artifacts", {}),
        "provider": PROVIDER,
        "model": config["model"],
        "key_source": config["key_source"],
        "key_env_var": config["env_var"],
    }
    dst = ctx.artifact_dir / "llm_resolution_pack.json"
    dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Build canonical LLM resolution pack")
    parser.add_argument("--case", required=True, choices=list_case_keys())
    parser.add_argument("--issue-type", choices=sorted({"FIELD_CONFLICT", "SUSPICIOUS_SILENCE", "SCOPE_AMBIGUITY", "GROUPED_CONTEXT_NEEDS_EXPLANATION", "OCR_VARIANT_COLLISION", "TABLE_RECAP_DUPLICATE_UNCLEAR"}))
    parser.add_argument("--field-family")
    parser.add_argument("--field-type")
    parser.add_argument("--limit", type=int, default=1)
    args = parser.parse_args()
    out = build_llm_resolution_pack(
        args.case,
        issue_type=args.issue_type,
        field_family=args.field_family,
        field_type=args.field_type,
        limit=args.limit,
    )
    summary = {
        "case_key": out["case_key"],
        "status": out["status"],
        "issue_count": out["issue_count"],
        "outcomes": [r["llm_outcome"] for r in out["resolutions"]],
        "provider": out["provider"],
        "model": out["model"],
        "key_env_var": out["key_env_var"],
        "key_source": out["key_source"],
    }
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
