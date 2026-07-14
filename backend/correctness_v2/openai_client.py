"""
OpenAI client wrapper for Correctness Mode v2 (step 2).

This is the ONLY place in the v2 subsystem that talks to OpenAI. It is built to
fail closed and to be fully inspectable:

  * The model is read from ``CORRECTNESS_V2_OPENAI_MODEL`` and falls back to a
    safe app default (the most recent accurate model available to this app).
  * The API key is discovered via the existing app config helper
    (``discover_openai_config``) — never hard-coded, never written to artifacts.
  * Requests use structured JSON output (``response_format=json_object``).
  * Timeouts and any transport/parse error raise :class:`OpenAIClientError`.
    There is NO silent fallback and NO old-analyzer fallback.

The redacted request payload (``redacted_request``) is what gets persisted to
``openai_request.json`` — it deliberately omits the api key and any auth header.

Newer reasoning models (gpt-5.x) reject a non-default ``temperature``. We
therefore only send ``temperature`` when ``CORRECTNESS_V2_OPENAI_TEMPERATURE`` is
explicitly set; otherwise we omit it and let the model use its default.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

# Most recent accurate model available to this app at build time. Overridable via
# CORRECTNESS_V2_OPENAI_MODEL. Kept as a constant (not a secret) on purpose.
DEFAULT_MODEL = "gpt-5.5"

MODEL_ENV = "CORRECTNESS_V2_OPENAI_MODEL"
TIMEOUT_ENV = "CORRECTNESS_V2_OPENAI_TIMEOUT_SECONDS"
TEMPERATURE_ENV = "CORRECTNESS_V2_OPENAI_TEMPERATURE"
MAX_CONTEXT_CHARS_ENV = "CORRECTNESS_V2_MAX_CONTEXT_CHARS"

DEFAULT_TIMEOUT_SECONDS = 180.0
DEFAULT_MAX_CONTEXT_CHARS = 120_000


class OpenAIClientError(RuntimeError):
    """Raised for any OpenAI configuration/transport/parse failure (fail closed)."""

    def __init__(self, message: str, *, reason_code: str = "OPENAI_CALL_FAILED"):
        super().__init__(message)
        self.reason_code = reason_code


# Reason codes for API failures that are transient and MAY be safely retried
# (idempotent read-shaped analyst call). Everything else — bad request, malformed
# response, missing key, empty content — is deterministic and must NOT be retried.
REASON_RATE_LIMITED = "OPENAI_RATE_LIMITED"
REASON_TIMEOUT = "OPENAI_TIMEOUT"
REASON_SERVER_ERROR = "OPENAI_SERVER_ERROR"
# Account credit/quota exhausted (a 429 with error code 'insufficient_quota').
# This is NOT transient — retrying cannot help until billing is topped up — so it
# is deliberately kept OUT of TRANSIENT_REASON_CODES (fail fast, no retry) and is
# surfaced to admins as an unmistakable "recharge required" signal.
REASON_QUOTA_EXHAUSTED = "OPENAI_QUOTA_EXHAUSTED"

# The subset of reason codes that indicate a transient, retryable failure.
TRANSIENT_REASON_CODES = frozenset(
    {REASON_RATE_LIMITED, REASON_TIMEOUT, REASON_SERVER_ERROR}
)


def is_quota_exhausted_reason(reason_code: Any) -> bool:
    """True when ``reason_code`` denotes exhausted account credit/quota."""
    return str(reason_code or "") == REASON_QUOTA_EXHAUSTED


def is_transient_reason(reason_code: Any) -> bool:
    """True when ``reason_code`` denotes a transient (retryable) API failure."""
    return str(reason_code or "") in TRANSIENT_REASON_CODES


def is_rate_limit_reason(reason_code: Any) -> bool:
    """True when ``reason_code`` denotes an API rate-limit (429) failure."""
    return str(reason_code or "") == REASON_RATE_LIMITED


def classify_openai_exception(exc: BaseException) -> str:
    """Map a raw OpenAI SDK / transport exception to a stable reason code.

    Purely structural (type name + optional ``status_code``) so it never imports
    the openai package and never inspects message text. Transient failures (429,
    timeout, 5xx, connection reset) get a retryable code; anything else stays the
    deterministic ``OPENAI_CALL_FAILED`` and is never retried.
    """
    name = type(exc).__name__
    status = getattr(exc, "status_code", None)
    try:
        status_int = int(status) if status is not None else None
    except (TypeError, ValueError):
        status_int = None

    # A 429 caused by exhausted account credit carries the structured error code
    # 'insufficient_quota' — distinguish it from a transient rate-limit 429 so it
    # is never retried and is surfaced as a distinct "recharge required" signal.
    code = getattr(exc, "code", None)
    err_type = getattr(exc, "type", None)
    if str(code or "") == "insufficient_quota" or str(err_type or "") == "insufficient_quota":
        return REASON_QUOTA_EXHAUSTED

    if status_int == 429 or "RateLimit" in name:
        return REASON_RATE_LIMITED
    if "Timeout" in name:
        return REASON_TIMEOUT
    if (status_int is not None and status_int >= 500) or name in {
        "InternalServerError",
        "APIConnectionError",
        "APIConnectionResetError",
    }:
        return REASON_SERVER_ERROR
    return "OPENAI_CALL_FAILED"


# ---------------------------------------------------------------------------
# Config resolution
# ---------------------------------------------------------------------------
def resolve_model() -> str:
    """Resolve the model: env override first, then the safe app default."""
    raw = os.environ.get(MODEL_ENV)
    if raw and raw.strip():
        return raw.strip()
    # Fall back to the existing app config (NARRATOR_MODEL / OPENAI_MODEL / ...)
    # if available, otherwise our constant default.
    try:
        from perizia_canonical_pipeline.llm_resolution_pack import discover_openai_config

        cfg = discover_openai_config()
        model = cfg.get("model")
        if model and str(model).strip():
            return str(model).strip()
    except Exception:
        pass
    return DEFAULT_MODEL


def resolve_timeout() -> float:
    raw = os.environ.get(TIMEOUT_ENV)
    if raw and raw.strip():
        try:
            value = float(raw.strip())
            if value > 0:
                return value
        except Exception:
            pass
    return DEFAULT_TIMEOUT_SECONDS


def resolve_temperature() -> Optional[float]:
    """Only return a temperature if explicitly configured (newer models reject non-default)."""
    raw = os.environ.get(TEMPERATURE_ENV)
    if raw is None or not raw.strip():
        return None
    try:
        return float(raw.strip())
    except Exception:
        return None


def resolve_max_context_chars() -> int:
    raw = os.environ.get(MAX_CONTEXT_CHARS_ENV)
    if raw and raw.strip():
        try:
            value = int(raw.strip())
            if value > 0:
                return value
        except Exception:
            pass
    return DEFAULT_MAX_CONTEXT_CHARS


def _discover_api_key() -> Optional[str]:
    try:
        from perizia_canonical_pipeline.llm_resolution_pack import discover_openai_config

        cfg = discover_openai_config()
        key = cfg.get("api_key")
        return key if key else None
    except Exception:
        # Last resort: environment only.
        return os.environ.get("OPENAI_API_KEY") or None


# ---------------------------------------------------------------------------
# Request shaping (redacted for artifacts) + the live call
# ---------------------------------------------------------------------------
def build_request(
    messages: List[Dict[str, str]],
    *,
    model: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build the chat-completions request body (NO secrets).

    This exact structure is what we persist to ``openai_request.json``. The api
    key lives only in the client object at call time and is never included here.
    """
    resolved_model = model or resolve_model()
    body: Dict[str, Any] = {
        "model": resolved_model,
        "response_format": {"type": "json_object"},
        "messages": messages,
    }
    temperature = resolve_temperature()
    if temperature is not None:
        body["temperature"] = temperature
    return body


def redacted_request(
    messages: List[Dict[str, str]],
    *,
    model: Optional[str] = None,
    extra_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Return an artifact-safe view of the request: secrets omitted, key markers explicit."""
    body = build_request(messages, model=model)
    out: Dict[str, Any] = {
        "provider": "openai",
        "model": body["model"],
        "response_format": body["response_format"],
        "timeout_seconds": resolve_timeout(),
        "secrets_included": False,
        "api_key": "<omitted>",
        "messages": body["messages"],
    }
    if "temperature" in body:
        out["temperature"] = body["temperature"]
    if extra_meta:
        for k, v in extra_meta.items():
            if k not in out:
                out[k] = v
    return out


def call_openai_json(
    messages: List[Dict[str, str]],
    *,
    model: Optional[str] = None,
    timeout: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Call OpenAI chat-completions in JSON mode and return a structured result.

    Returns a dict:
        {
          "content": "<raw json string>",
          "model": "<resolved model>",
          "finish_reason": "...",
          "usage": {...} | None,
          "response_id": "..." | None,
        }

    Raises :class:`OpenAIClientError` on missing key, transport error, timeout,
    or empty content. Never falls back.
    """
    api_key = _discover_api_key()
    if not api_key:
        raise OpenAIClientError(
            "No OpenAI API key available for Correctness v2.",
            reason_code="OPENAI_API_KEY_MISSING",
        )

    try:
        from openai import OpenAI
    except Exception as exc:  # pragma: no cover - import guard
        raise OpenAIClientError(
            f"openai package not available: {exc}",
            reason_code="OPENAI_SDK_UNAVAILABLE",
        )

    resolved_timeout = timeout if (timeout and timeout > 0) else resolve_timeout()
    body = build_request(messages, model=model)

    try:
        client = OpenAI(api_key=api_key, timeout=resolved_timeout)
        response = client.chat.completions.create(**body)
    except Exception as exc:
        # Includes APITimeoutError, RateLimitError, APIError, BadRequestError, etc.
        # Classify transient failures (429/timeout/5xx) with a retryable reason
        # code so the per-lot runner can back off and, on rate limiting, degrade
        # to serial. Deterministic failures keep OPENAI_CALL_FAILED (no retry).
        raise OpenAIClientError(
            f"OpenAI call failed: {type(exc).__name__}: {exc}",
            reason_code=classify_openai_exception(exc),
        )

    try:
        choice = response.choices[0]
        content = (choice.message.content or "").strip()
        finish_reason = getattr(choice, "finish_reason", None)
        usage = getattr(response, "usage", None)
        usage_dict = usage.model_dump() if hasattr(usage, "model_dump") else (
            dict(usage) if usage else None
        )
        response_id = getattr(response, "id", None)
        resolved_model = getattr(response, "model", body["model"])
    except Exception as exc:
        raise OpenAIClientError(
            f"Malformed OpenAI response: {type(exc).__name__}: {exc}",
            reason_code="OPENAI_RESPONSE_MALFORMED",
        )

    if not content:
        raise OpenAIClientError(
            "OpenAI returned empty content.",
            reason_code="OPENAI_EMPTY_CONTENT",
        )

    return {
        "content": content,
        "model": resolved_model,
        "finish_reason": finish_reason,
        "usage": usage_dict,
        "response_id": response_id,
    }
