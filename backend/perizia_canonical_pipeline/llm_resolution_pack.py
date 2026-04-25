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
import random
import re
import ssl
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from .corpus_registry import list_case_keys
from .llm_clarification_issue_pack import build_clarification_issue_pack
from .runner import build_context


PROVIDER = "openai"
DEFAULT_MODEL = "gpt-4o-mini"
DEFAULT_ISSUE_LIMIT = 8
CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"
MAX_OPENAI_RETRIES = 2
DEFAULT_RETRY_DELAY_SECONDS = 1.0
MAX_RETRY_DELAY_SECONDS = 8.0
ALLOWED_OUTCOMES = {"resolved", "unresolved_explained", "upgraded_context"}
ALLOWED_RESOLUTION_MODES = {
    "clean_resolution",
    "qualified_resolution",
    "true_unresolved",
    "blocked",
}
FAMILY_PRIORITY = {
    "valuation": 0,
    "occupancy": 1,
    "rights": 2,
    "cadastral": 3,
    "cost": 4,
    "location": 5,
}
ISSUE_TYPE_PRIORITY = {
    "SCOPE_AMBIGUITY": 0,
    "FIELD_CONFLICT": 1,
    "GROUPED_CONTEXT_NEEDS_EXPLANATION": 2,
    "TABLE_RECAP_DUPLICATE_UNCLEAR": 3,
    "SUSPICIOUS_SILENCE": 4,
    "OCR_VARIANT_COLLISION": 5,
}
GENERIC_EXPLANATION_PATTERNS = [
    "the system found conflicting evidence",
    "the value could not be resolved safely",
    "the document contains ambiguity",
    "no fixed value is shown due to conflicting information",
    "il sistema ha trovato evidenze contrastanti",
    "il valore non può essere risolto in modo sicuro",
    "il documento contiene ambiguità",
    "non viene mostrato un valore fisso",
    "caso lasciato irrisolto",
]
USER_VISIBLE_MACHINE_CODE_RE = re.compile(
    r"\b(?:NON_QUANTIFICATO_IN_PERIZIA|NON_QUANTIFICATO|MISSING_EVIDENCE|"
    r"SCOPE_AMBIGUITY|SCOPE AMBIGUITY|OCR_NOISE|FIELD_CONFLICT|FIELD CONFLICT|"
    r"SUSPICIOUS_SILENCE|SUSPICIOUS SILENCE|GROUPED_CONTEXT_NEEDS_EXPLANATION|"
    r"GROUPED CONTEXT NEEDS EXPLANATION|OCR_VARIANT_COLLISION|OCR VARIANT COLLISION|"
    r"TABLE_RECAP_DUPLICATE_UNCLEAR|TABLE RECAP DUPLICATE UNCLEAR)\b",
    re.IGNORECASE,
)


class LLMResolutionUnavailable(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        error_type: Optional[str] = None,
        retry_after: Optional[float] = None,
        retryable: bool = False,
        detail: Optional[str] = None,
    ) -> None:
        self.status_code = status_code
        self.error_type = error_type
        self.retry_after = retry_after
        self.retryable = retryable
        self.detail = detail
        super().__init__(self._format_message(message))

    def _format_message(self, message: str) -> str:
        parts = [message]
        if self.error_type:
            parts.append(f"type={self.error_type}")
        if self.retry_after is not None:
            parts.append(f"retry_after={self.retry_after:g}")
        parts.append(f"retryable={str(self.retryable).lower()}")
        if self.detail:
            cleaned = re.sub(r"\s+", " ", self.detail).strip()
            if cleaned:
                parts.append(f"detail={cleaned[:300]}")
        return " ".join(parts)

    def to_error_details(self) -> Dict[str, Any]:
        return {
            "status_code": self.status_code,
            "error_type": self.error_type,
            "retry_after": self.retry_after,
            "retryable": self.retryable,
            "detail": self.detail,
        }

    @property
    def hard_provider_failure(self) -> bool:
        return self.status_code in {400, 401, 403, 404, 429} and not self.retryable


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
        "target_case_key": issue.get("target_case_key") or issue.get("case_key"),
        "target_field": issue.get("target_field") or issue.get("field_type"),
        "target_scope": issue.get("target_scope") or issue.get("scope_metadata"),
        "scope_status": issue.get("scope_status"),
        "target_scope_kind": issue.get("target_scope_kind"),
        "field_family": issue.get("field_family"),
        "field_type": issue.get("field_type"),
        "lot_id": issue.get("lot_id"),
        "bene_id": issue.get("bene_id"),
        "issue_type": issue.get("issue_type"),
        "deterministic_status": issue.get("deterministic_status"),
        "current_ambiguity_summary": issue.get("current_ambiguity_summary"),
        "reason_codes": issue.get("reason_codes"),
        "candidate_values": issue.get("candidate_values"),
        "blocked_values": issue.get("blocked_values"),
        "known_candidates": issue.get("known_candidates"),
        "blocked_reasons": issue.get("blocked_reasons"),
        "source_line_indices": issue.get("source_line_indices"),
        "shell_quotes": issue.get("shell_quotes"),
        "supporting_evidence_snippets": issue.get("supporting_evidence_snippets"),
        "supporting_candidates": issue.get("supporting_candidates"),
        "supporting_blocked_entries": issue.get("supporting_blocked_entries"),
        "source_pages": issue.get("source_pages"),
        "relevant_pages": issue.get("relevant_pages"),
        "target_section_entry_pages": issue.get("target_section_entry_pages"),
        "anchor_pages": issue.get("anchor_pages"),
        "recap_pages": issue.get("recap_pages"),
        "page_selection": issue.get("page_selection"),
        "contamination_class": issue.get("contamination_class"),
        "contamination_disposition": issue.get("contamination_disposition"),
        "admissibility_status": issue.get("admissibility_status"),
        "admissibility_reason_codes": issue.get("admissibility_reason_codes"),
        "quality_label": issue.get("quality_label"),
        "reason_for_label": issue.get("reason_for_label"),
        "local_text_windows": issue.get("local_text_windows"),
        "table_zone_types": issue.get("table_zone_types"),
        "scope_metadata": issue.get("scope_metadata"),
        "needs_llm": issue.get("needs_llm"),
        "shell_sources": issue.get("shell_sources"),
    }


def _system_prompt() -> str:
    return (
        "You are analyzing a real-estate auction appraisal issue for a single target "
        "field within a specific scope. Your job is not to summarize the whole "
        "document and not to invent certainty. Reason only about the supplied bounded "
        "issue pack. You may return only resolved, upgraded_context, or "
        "unresolved_explained. Classify the case as clean_resolution, "
        "qualified_resolution, true_unresolved, or blocked. Use only the supplied "
        "evidence, respect the target scope, do not merge across lots/beni unless the "
        "issue pack proves it is safe, treat target_section_entry_pages and anchor_pages "
        "as stronger scope anchors than recap pages, never treat summary_or_index pages "
        "or transition pages as primary evidence on their own, mention exact pages worth "
        "checking, and never put a fake value into unresolved output. If one best "
        "conclusion is safe but needs mandatory qualification, return "
        "qualified_resolution with the value and context qualification. "
        "Write customer-facing Italian only. Never use backend jargon or internal labels such as "
        "“Per scope document”, “problema contesto raggruppato”, “bounded packet”, "
        "or “valori concorrenti” as a standalone technical phrase. "
        "For unresolved or context-only output, always explain in this order: "
        "1) what the perizia says, 2) why that is insufficient or conflicted, "
        "3) the exact document or check to verify next. "
        "Return only valid JSON."
    )


def _user_prompt(issue: Dict) -> str:
    schema = {
        "issue_id": "same issue id",
        "llm_outcome": "resolved | unresolved_explained | upgraded_context",
        "outcome": "resolved | unresolved_explained | upgraded_context",
        "resolution_mode": "clean_resolution | qualified_resolution | true_unresolved | blocked",
        "resolved_value": "string or null",
        "resolved_value_type": "field value type or null",
        "context_qualification": "Italian qualification text or null",
        "why_not_fully_certain": "Italian text or null",
        "confidence_band": "high | medium | low",
        "user_visible_explanation": "short Italian explanation grounded in evidence",
        "supporting_evidence": [{"page": 0, "quote": "short quote from supplied evidence", "reason": "why it supports outcome"}],
        "evidence_pages": [0],
        "supporting_pages": [0],
        "tension_pages": [0],
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
                "For clean_resolution, provide resolved_value and rationale without material qualification.",
                "For qualified_resolution, provide resolved_value, context_qualification, why_not_fully_certain, supporting_pages, and tension_pages.",
                "For true_unresolved, leave resolved_value null and provide why_not_resolved plus user_visible_explanation.",
                "For blocked, leave resolved_value null and explain the structural blockage.",
                "For FIELD_CONFLICT choose resolved only if the supplied snippets clearly say one value is the final/current value.",
                "For SUSPICIOUS_SILENCE or GROUPED_CONTEXT_NEEDS_EXPLANATION prefer upgraded_context when the text gives context but not a fixed field value.",
                "For SCOPE_AMBIGUITY prefer unresolved_explained unless the supplied windows clearly assign scope.",
                "For TABLE_RECAP_DUPLICATE_UNCLEAR prefer unresolved_explained unless the recap and source value agree on the same value and scope.",
                "For upgraded_context, explain why the context should be surfaced but not treated as a final normalized field value.",
                "Use target_section_entry_pages and anchor_pages to confirm scope before trusting source_pages.",
                "Do not resolve from summary_or_index pages alone.",
                "Do not choose a value from a transition_page unless target_section_entry_pages prove the same lot/bene scope.",
                "For unresolved_explained, why_not_resolved must explain the conflict, ambiguity, or unsafe scope.",
                "For unresolved_explained or upgraded_context, context_qualification or why_not_fully_certain must name the exact next document or check.",
                "user_visible_explanation must be plain customer-grade Italian and must not contain backend wording.",
                "Keep needs_human_review true unless confidence is high and resolved_value is directly quoted.",
            ],
            "output_schema": schema,
            "issue_packet": _issue_payload(issue),
        },
        ensure_ascii=False,
    )


def _parse_retry_after(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        delay = float(value)
    except ValueError:
        return None
    if delay < 0:
        return None
    return delay


def _openai_error_from_body(text: str) -> Dict[str, Optional[str]]:
    if not text:
        return {"error_type": None, "detail": None}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {"error_type": None, "detail": text.strip() or None}
    error = payload.get("error") if isinstance(payload, dict) else None
    if not isinstance(error, dict):
        return {"error_type": None, "detail": text.strip() or None}
    detail = error.get("message") or error.get("detail") or text.strip() or None
    error_type = error.get("type") or error.get("code")
    return {
        "error_type": str(error_type) if error_type else None,
        "detail": str(detail) if detail else None,
    }


def _is_hard_quota_error(status_code: int, error_type: Optional[str], detail: Optional[str]) -> bool:
    haystack = " ".join(part for part in [error_type, detail] if part).casefold()
    hard_markers = (
        "insufficient_quota",
        "billing",
        "quota exhausted",
        "exceeded your current quota",
        "check your plan and billing",
    )
    return status_code == 429 and any(marker in haystack for marker in hard_markers)


def _build_http_error(exc: urllib.error.HTTPError) -> LLMResolutionUnavailable:
    try:
        body_text = exc.read().decode("utf-8", errors="replace")
    except Exception:
        body_text = ""
    parsed = _openai_error_from_body(body_text)
    retry_after = _parse_retry_after(exc.headers.get("Retry-After") if exc.headers else None)
    status_code = int(exc.code)
    error_type = parsed.get("error_type")
    detail = parsed.get("detail")

    if status_code == 429 and not _is_hard_quota_error(status_code, error_type, detail):
        retryable = True
    else:
        retryable = False

    return LLMResolutionUnavailable(
        f"OpenAI HTTP error status={status_code}",
        status_code=status_code,
        error_type=error_type,
        retry_after=retry_after,
        retryable=retryable,
        detail=detail,
    )


def _retry_delay(exc: LLMResolutionUnavailable, attempt_index: int) -> float:
    if exc.retry_after is not None:
        return min(exc.retry_after, MAX_RETRY_DELAY_SECONDS)
    base = DEFAULT_RETRY_DELAY_SECONDS * (2 ** max(attempt_index - 1, 0))
    return min(base + random.uniform(0, 0.25), MAX_RETRY_DELAY_SECONDS)


def _call_openai_json_once(api_key: str, model: str, issue: Dict, timeout_seconds: int = 45) -> Dict:
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
        raise _build_http_error(exc) from exc
    except Exception as exc:
        raise LLMResolutionUnavailable(f"OpenAI request failed type={type(exc).__name__}") from exc
    content = payload["choices"][0]["message"]["content"]
    return _json_from_response(content)


def _call_openai_json(api_key: str, model: str, issue: Dict, timeout_seconds: int = 45) -> Dict:
    for attempt_index in range(MAX_OPENAI_RETRIES + 1):
        try:
            return _call_openai_json_once(api_key, model, issue, timeout_seconds=timeout_seconds)
        except LLMResolutionUnavailable as exc:
            if not exc.retryable or attempt_index >= MAX_OPENAI_RETRIES:
                raise
            time.sleep(_retry_delay(exc, attempt_index + 1))
    raise LLMResolutionUnavailable("OpenAI request failed after retry loop")


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


def _optional_text(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _field_label(issue: Dict) -> str:
    labels = {
        "impianto_idrico_status": "impianto idrico",
        "impianto_elettrico_status": "impianto elettrico",
        "impianto_riscaldamento_status": "impianto di riscaldamento",
        "impianto_gas_status": "impianto gas",
        "impianto_ascensore_status": "impianto ascensore",
        "valuation_market_raw": "valore di mercato",
        "cost_sanatoria_raw": "costi o indicazioni di sanatoria",
        "cost_regolarizzazione_raw": "costi o indicazioni di regolarizzazione",
        "occupancy": "stato di occupazione",
    }
    field_type = str(issue.get("field_type") or issue.get("field_family") or "campo")
    return labels.get(field_type, field_type.replace("_raw", "").replace("_status", "").replace("_", " "))


def _scope_label(issue: Dict) -> str:
    parts: List[str] = []
    lot_id = issue.get("lot_id")
    bene_id = issue.get("bene_id")
    if lot_id not in {None, ""}:
        parts.append(f"Lotto {lot_id}")
    if bene_id not in {None, ""}:
        parts.append(f"Bene {bene_id}")
    if parts:
        return ", ".join(parts)
    scope = (issue.get("scope_metadata") or {}).get("scope_key")
    if scope:
        return f"scope {scope}"
    pages = issue.get("source_pages") or []
    if pages:
        label = "pagina" if len(pages) == 1 else "pagine"
        return f"la finestra di {label} {', '.join(str(p) for p in pages)}"
    return "la finestra locale del documento"


def _short_quote(text: object, max_len: int = 140) -> Optional[str]:
    quote = _optional_text(text)
    if not quote:
        return None
    quote = re.sub(r"\s+", " ", quote)
    if len(quote) > max_len:
        quote = quote[: max_len - 1].rstrip() + "…"
    return quote


def _issue_quotes(issue: Dict, limit: int = 2) -> List[str]:
    quotes: List[str] = []
    for quote in issue.get("shell_quotes") or []:
        short = _short_quote(quote)
        if short and any(marker in short for marker in ["cross-lot scope guard", "Schema riassuntivo page"]):
            continue
        if short and short not in quotes:
            quotes.append(short)
    for candidate in issue.get("supporting_candidates") or []:
        short = _short_quote(candidate.get("quote"))
        if short and short not in quotes:
            quotes.append(short)
    for blocked in issue.get("supporting_blocked_entries") or []:
        for candidate in blocked.get("candidates") or []:
            short = _short_quote(candidate.get("quote"))
            if short and short not in quotes:
                quotes.append(short)
    if not quotes:
        field_words = [w for w in _field_label(issue).split() if len(w) > 3]
        best_short: Optional[str] = None
        for window in issue.get("local_text_windows") or []:
            for line in str(window.get("text") or "").splitlines():
                short = _short_quote(line)
                if short and field_words and all(word.casefold() in short.casefold() for word in field_words):
                    quotes.append(short)
                    break
                if short and not best_short and any(word.casefold() in short.casefold() for word in field_words):
                    best_short = short
            if quotes:
                break
        if not quotes and best_short:
            quotes.append(best_short)
    if not quotes:
        for window in issue.get("local_text_windows") or []:
            for line in str(window.get("text") or "").splitlines():
                short = _short_quote(line)
                if short and not re.fullmatch(r"\d+\s+di\s+\d+", short, flags=re.IGNORECASE):
                    quotes.append(short)
                    break
            if quotes:
                break
    return quotes[:limit]


def _quote_phrase(quotes: List[str]) -> str:
    if not quotes:
        return "gli estratti forniti non riportano una formulazione locale abbastanza selettiva"
    if len(quotes) == 1:
        return f'“{quotes[0]}”'
    return " e ".join(f'“{quote}”' for quote in quotes)


def _values_phrase(issue: Dict) -> str:
    values = sorted(_candidate_values(issue))
    if not values:
        return "nessun valore candidato normalizzabile"
    if len(values) == 1:
        return values[0]
    return " / ".join(values)


def _issue_type_label(issue_type: object) -> str:
    labels = {
        "FIELD_CONFLICT": "frasi che indicano esiti diversi",
        "SUSPICIOUS_SILENCE": "mancanza di un dato conclusivo",
        "SCOPE_AMBIGUITY": "ambito non chiaro tra bene e lotto",
        "GROUPED_CONTEXT_NEEDS_EXPLANATION": "contesto utile ma non conclusivo",
        "OCR_VARIANT_COLLISION": "varianti testuali dovute alla qualità OCR",
        "TABLE_RECAP_DUPLICATE_UNCLEAR": "riepilogo e tabella non allineati in modo sicuro",
    }
    return labels.get(str(issue_type or "").upper(), "ambiguita deterministica")


def _is_generic_explanation(text: str) -> bool:
    norm = _normalize_for_match(text)
    return any(pattern in norm for pattern in GENERIC_EXPLANATION_PATTERNS)


def _explanation_has_case_shape(issue: Dict, text: str, outcome: str, resolved_value: object) -> bool:
    norm = _normalize_for_match(text)
    if not norm or _is_generic_explanation(text):
        return False
    field = _field_label(issue)
    if _normalize_for_match(field) not in norm:
        return False
    lot_id = issue.get("lot_id")
    bene_id = issue.get("bene_id")
    if lot_id not in {None, ""} and _normalize_for_match(f"Lotto {lot_id}") not in norm:
        return False
    if bene_id not in {None, ""} and _normalize_for_match(f"Bene {bene_id}") not in norm:
        return False
    quotes = _issue_quotes(issue)
    values = sorted(_candidate_values(issue))
    has_evidence = any(_normalize_for_match(value) in norm for value in values)
    has_evidence = has_evidence or any(_normalize_for_match(quote) in norm for quote in quotes)
    if outcome == "resolved" and resolved_value is not None:
        has_evidence = has_evidence and _normalize_for_match(str(resolved_value)) in norm
    return has_evidence


def _evidence_shaped_explanation(issue: Dict, raw_resolution: Dict, outcome: str, resolved_value: object) -> str:
    scope = _scope_label(issue)
    field = _field_label(issue)
    quotes = _issue_quotes(issue)
    evidence = _quote_phrase(quotes)
    values = _values_phrase(issue)
    issue_type = _issue_type_label(issue.get("issue_type"))

    if outcome == "resolved":
        alternatives = raw_resolution.get("rejected_alternatives")
        if isinstance(alternatives, list) and alternatives:
            rejected = "; ".join(
                f"{alt.get('value')}: {alt.get('reason')}"
                for alt in alternatives[:2]
                if isinstance(alt, dict) and alt.get("value") and alt.get("reason")
            )
            if rejected:
                return (
                    f"Per {scope}, il valore “{resolved_value}” è usato per {field} perché il passaggio {evidence} "
                    f"lo sostiene più direttamente delle alternative scartate ({rejected})."
                )
        return (
            f"Per {scope}, il valore “{resolved_value}” è usato per {field} perché il passaggio {evidence} "
            "lo collega direttamente allo scope del pacchetto e non lascia un'alternativa più forte."
        )

    if outcome == "upgraded_context":
        if not _candidate_values(issue):
            return (
                f"Per {scope}, il documento dà contesto su {field} con il passaggio {evidence}. "
                f"Il contenuto conta perché spiega la condizione da mostrare all'utente, ma resta contesto: "
                f"il problema {issue_type} è raggruppato e non separa un valore finale normalizzato per il singolo campo."
            )
        return (
            f"Per {scope}, il documento dà contesto su {field} con il passaggio {evidence}. "
            f"Il contenuto è rilevante per interpretare {values}, ma resta contesto perché il problema {issue_type} "
            "non isola un valore finale normalizzato per il singolo campo."
        )

    if not _candidate_values(issue):
        return (
            f"Per {scope}, {field} resta irrisolto: il documento riporta {evidence}, "
            f"ma quel passaggio è un rinvio o contesto locale senza valore candidato normalizzabile. "
            f"Il problema {issue_type} non attribuisce un dato finale a un lotto o bene specifico, "
            "quindi il campo resta aperto invece di inventare uno stato."
        )

    return (
        f"Per {scope}, {field} resta irrisolto: il documento riporta {evidence}, cioè valori o indicazioni concorrenti "
        f"({values}). Il problema {issue_type} non chiarisce quale formulazione valga come dato finale per questo scope, "
        f"quindi il campo resta aperto invece di scegliere tra {values}."
    )


def _fallback_why_not_resolved(issue: Dict, raw_resolution: Dict, outcome: str) -> str:
    explanation = _optional_text(raw_resolution.get("user_visible_explanation"))
    if explanation:
        return explanation

    if outcome == "upgraded_context":
        return "Il pacchetto contiene contesto utile, ma non un valore normalizzato sicuro."

    issue_type = _issue_type_label(issue.get("issue_type"))
    field_type = issue.get("field_type") or issue.get("field_family") or "campo"
    values = sorted(_candidate_values(issue))
    if values:
        return (
            f"Il pacchetto {issue_type} per {field_type} contiene valori concorrenti "
            f"({', '.join(values)}) senza evidenza abbastanza forte per scegliere un valore finale."
        )
    return (
        f"Il pacchetto {issue_type} per {field_type} non contiene evidenza abbastanza forte "
        "per emettere un valore finale."
    )


def _validate_resolution(issue: Dict, raw_resolution: Dict, provider: str, model: str) -> Dict:
    warnings: List[str] = []
    outcome = raw_resolution.get("llm_outcome") or raw_resolution.get("outcome")
    if outcome not in ALLOWED_OUTCOMES:
        warnings.append(f"Invalid llm_outcome {outcome!r}; downgraded to unresolved_explained.")
        outcome = "unresolved_explained"
    resolution_mode = raw_resolution.get("resolution_mode")
    if resolution_mode not in ALLOWED_RESOLUTION_MODES:
        if outcome == "resolved":
            resolution_mode = "clean_resolution"
        elif outcome == "upgraded_context":
            resolution_mode = "qualified_resolution" if raw_resolution.get("resolved_value") else "true_unresolved"
        else:
            resolution_mode = "true_unresolved"
        warnings.append("resolution_mode missing or invalid; inferred from outcome and value.")
    resolved_value = raw_resolution.get("resolved_value")
    evidence_text = _normalize_for_match(_all_evidence_text(issue))
    candidate_values = _candidate_values(issue)
    if outcome == "upgraded_context" and resolution_mode == "qualified_resolution" and resolved_value is not None:
        value_text = str(resolved_value).strip()
        value_ok = bool(value_text) and (
            value_text in candidate_values or _normalize_for_match(value_text) in evidence_text
        )
        if not value_ok:
            warnings.append("Qualified resolved value was not present in bounded evidence; downgraded.")
            outcome = "unresolved_explained"
            resolution_mode = "true_unresolved"
            resolved_value = None
    elif outcome == "resolved":
        value_text = "" if resolved_value is None else str(resolved_value).strip()
        value_ok = bool(value_text) and (
            value_text in candidate_values or _normalize_for_match(value_text) in evidence_text
        )
        if not value_ok:
            warnings.append("Resolved value was not present in bounded evidence; downgraded.")
            outcome = "unresolved_explained"
            resolution_mode = "true_unresolved"
            resolved_value = None
    else:
        resolved_value = None
        if resolution_mode == "clean_resolution":
            resolution_mode = "true_unresolved"

    source_pages = raw_resolution.get("source_pages") or issue.get("source_pages") or []
    if not isinstance(source_pages, list):
        source_pages = issue.get("source_pages") or []
    source_pages = [p for p in source_pages if p in _issue_pages(issue)]
    if not source_pages:
        source_pages = issue.get("source_pages") or []

    supporting_evidence = raw_resolution.get("supporting_evidence") if isinstance(raw_resolution.get("supporting_evidence"), list) else []
    warnings.extend(_evidence_page_warnings(issue, supporting_evidence))
    resolved_value_type = raw_resolution.get("resolved_value_type")
    if outcome == "resolved" or (outcome == "upgraded_context" and resolution_mode == "qualified_resolution" and resolved_value is not None):
        if not resolved_value_type:
            resolved_value_type = issue.get("field_type")
    else:
        resolved_value_type = None

    why_not_resolved = _optional_text(raw_resolution.get("why_not_resolved"))
    if outcome in {"unresolved_explained", "upgraded_context"} and not why_not_resolved:
        warnings.append(f"{outcome} response omitted why_not_resolved; populated from bounded issue packet.")
        why_not_resolved = _fallback_why_not_resolved(issue, raw_resolution, outcome)
    elif why_not_resolved and USER_VISIBLE_MACHINE_CODE_RE.search(why_not_resolved):
        warnings.append(f"{outcome} response exposed internal machine codes in why_not_resolved; populated from bounded issue packet.")
        why_not_resolved = _fallback_why_not_resolved(issue, {}, outcome)
    user_visible_explanation = _optional_text(raw_resolution.get("user_visible_explanation"))
    if not user_visible_explanation or not _explanation_has_case_shape(issue, user_visible_explanation, outcome, resolved_value):
        warnings.append("user_visible_explanation was missing or too generic; populated from bounded issue packet.")
        user_visible_explanation = _evidence_shaped_explanation(issue, raw_resolution, outcome, resolved_value)
    elif USER_VISIBLE_MACHINE_CODE_RE.search(user_visible_explanation):
        warnings.append("user_visible_explanation exposed internal machine codes; populated from bounded issue packet.")
        user_visible_explanation = _evidence_shaped_explanation(issue, {}, outcome, resolved_value)

    resolution = {
        "issue_id": issue["issue_id"],
        "llm_outcome": outcome,
        "resolution_mode": resolution_mode,
        "resolved_value": resolved_value,
        "resolved_value_type": resolved_value_type,
        "context_qualification": _optional_text(raw_resolution.get("context_qualification")),
        "why_not_fully_certain": _optional_text(raw_resolution.get("why_not_fully_certain")),
        "confidence_band": raw_resolution.get("confidence_band") if raw_resolution.get("confidence_band") in {"high", "medium", "low"} else "low",
        "user_visible_explanation": user_visible_explanation,
        "supporting_evidence": supporting_evidence,
        "evidence_pages": [p for p in (raw_resolution.get("evidence_pages") or source_pages) if p in _issue_pages(issue)],
        "supporting_pages": [p for p in (raw_resolution.get("supporting_pages") or source_pages) if p in _issue_pages(issue)],
        "tension_pages": [p for p in (raw_resolution.get("tension_pages") or []) if p in _issue_pages(issue)],
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
    if resolution["llm_outcome"] == "upgraded_context" and not resolution["why_not_resolved"]:
        resolution["why_not_resolved"] = "Contesto utile, ma non valore finale normalizzabile."
    if resolution["resolution_mode"] == "qualified_resolution":
        if not resolution["context_qualification"]:
            resolution["context_qualification"] = resolution["user_visible_explanation"]
        if not resolution["why_not_fully_certain"]:
            resolution["why_not_fully_certain"] = resolution["why_not_resolved"] or "Il valore richiede contesto qualificante dalle pagine del pacchetto."
    return resolution


def resolve_single_issue(
    issue: Dict,
    api_key: str,
    model: str,
) -> Dict:
    """
    Run bounded LLM clarification for a single pre-built issue packet.

    Returns a validated resolution dict (same schema as llm_resolution_pack resolutions).
    Raises LLMResolutionUnavailable or an HTTP/network exception on failure.
    Intended for use by the missing-slot escalation path in doc_map_freeze.
    """
    raw = _call_openai_json(api_key, model, issue)
    return _validate_resolution(issue, raw, PROVIDER, model)


def _has_resolution_evidence(issue: Dict) -> bool:
    return bool(
        issue.get("candidate_values")
        or issue.get("blocked_values")
        or issue.get("supporting_candidates")
        or issue.get("supporting_blocked_entries")
        or issue.get("local_text_windows")
        or issue.get("shell_quotes")
    )


def _priority_key(indexed_issue: tuple[int, Dict]) -> tuple[int, int, int, int, int, str]:
    original_index, issue = indexed_issue
    family = str(issue.get("field_family") or "")
    issue_type = str(issue.get("issue_type") or "")
    has_values = bool(issue.get("candidate_values") or issue.get("blocked_values"))
    primary_issue_bucket = 0 if issue_type in {
        "SCOPE_AMBIGUITY",
        "FIELD_CONFLICT",
        "GROUPED_CONTEXT_NEEDS_EXPLANATION",
    } else 1
    return (
        primary_issue_bucket,
        FAMILY_PRIORITY.get(family, 99),
        ISSUE_TYPE_PRIORITY.get(issue_type, 99),
        0 if has_values else 1,
        original_index,
        str(issue.get("issue_id") or ""),
    )


def select_prioritized_issues(
    pack: Dict[str, object],
    *,
    issue_type: Optional[str] = None,
    field_family: Optional[str] = None,
    field_type: Optional[str] = None,
    limit: int = DEFAULT_ISSUE_LIMIT,
) -> List[Dict]:
    """
    Select a bounded material batch for the LLM clarification layer.

    Explicit filters still narrow the candidate set, but the default path no
    longer sends only the first packet in artifact order.
    """
    if limit < 1:
        raise ValueError("limit must be >= 1")

    indexed: List[tuple[int, Dict]] = []
    for original_index, issue in enumerate(pack.get("issues", [])):
        if issue_type and issue.get("issue_type") != issue_type:
            continue
        if field_family and issue.get("field_family") != field_family:
            continue
        if field_type and issue.get("field_type") != field_type:
            continue
        if issue.get("needs_llm") is False:
            continue
        if issue.get("admissibility_status") != "admissible_clean":
            continue
        if (issue.get("page_selection") or {}).get("llm_safe") is False:
            continue
        if not _has_resolution_evidence(issue):
            continue
        indexed.append((original_index, issue))

    return [issue for _, issue in sorted(indexed, key=_priority_key)[:limit]]


def build_llm_resolution_pack(
    case_key: str,
    *,
    issue_type: Optional[str] = None,
    field_family: Optional[str] = None,
    field_type: Optional[str] = None,
    limit: int = DEFAULT_ISSUE_LIMIT,
) -> Dict[str, object]:
    config = discover_openai_config()
    if not config["key_found"]:
        raise LLMResolutionUnavailable("OPENAI_API_KEY missing")
    ctx = build_context(case_key)
    issue_pack = build_clarification_issue_pack(case_key)
    selected = select_prioritized_issues(
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
    parser.add_argument("--limit", type=int, default=DEFAULT_ISSUE_LIMIT)
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
