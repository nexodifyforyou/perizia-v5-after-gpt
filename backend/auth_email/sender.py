"""Provider-neutral email delivery for authentication codes.

Auth logic depends only on :class:`EmailSender`; no provider-specific detail
leaks past this module. ``ResendSender`` talks to the Resend HTTPS API directly
through the httpx client already vendored for the Google token exchange, so no
new dependency is introduced.

Delivery outcomes are collapsed into three categories because the challenge
lifecycle only ever needs to distinguish them:

``OK``          the provider accepted the message (2xx + message id)
``DEFINITIVE``  the provider refused it and always will (4xx)
``AMBIGUOUS``   we do not know (timeout, connection error, 5xx)

``AMBIGUOUS`` is the interesting one: the message may well have been delivered
even though the response was lost, so the caller must leave the challenge
verifiable rather than failing it.

Nothing here logs the code, the API key, the full body, or the raw provider
response.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol

import httpx

from . import config, templates

logger = logging.getLogger(__name__)

# Delivery categories.
CATEGORY_OK = "OK"
CATEGORY_DEFINITIVE = "DEFINITIVE"
CATEGORY_AMBIGUOUS = "AMBIGUOUS"

# Persisted delivery states (a projection of the category).
DELIVERY_SENT = "SENT"
DELIVERY_FAILED = "FAILED"
DELIVERY_PENDING = "PENDING"

_CATEGORY_TO_DELIVERY = {
    CATEGORY_OK: DELIVERY_SENT,
    CATEGORY_DEFINITIVE: DELIVERY_FAILED,
    CATEGORY_AMBIGUOUS: DELIVERY_PENDING,
}


@dataclass(frozen=True)
class SendResult:
    """Safe, persistable summary of one delivery attempt."""

    category: str
    provider: str
    provider_message_id: Optional[str] = None
    failure_category: Optional[str] = None
    attempts: int = 1

    @property
    def delivery_state(self) -> str:
        return _CATEGORY_TO_DELIVERY.get(self.category, DELIVERY_PENDING)

    @property
    def ok(self) -> bool:
        return self.category == CATEGORY_OK

    @property
    def definitive_failure(self) -> bool:
        return self.category == CATEGORY_DEFINITIVE


class EmailSender(Protocol):
    """The only surface auth logic is allowed to depend on."""

    name: str

    async def send_login_code(
        self, *, to: str, code: str, idempotency_key: str, ttl_seconds: int
    ) -> SendResult: ...


def _classify_status(status_code: int) -> str:
    if 200 <= status_code < 300:
        return CATEGORY_OK
    # 408/429 are retryable rather than permanent refusals.
    if status_code in (408, 429):
        return CATEGORY_AMBIGUOUS
    if 400 <= status_code < 500:
        return CATEGORY_DEFINITIVE
    return CATEGORY_AMBIGUOUS


def _failure_label(status_code: int) -> str:
    """A coarse, non-identifying reason code safe to persist and log."""
    if status_code in (401, 403):
        return "provider_auth_rejected"
    if status_code == 422:
        return "provider_rejected_payload"
    if status_code == 429:
        return "provider_rate_limited"
    if 400 <= status_code < 500:
        return "provider_rejected_request"
    if status_code >= 500:
        return "provider_server_error"
    return "provider_unknown"


class ResendSender:
    """Resend adapter — ``POST /emails`` with a delivery idempotency key."""

    name = config.PROVIDER_RESEND

    def __init__(self, *, api_key: str, from_address: str, reply_to: Optional[str] = None):
        self._api_key = api_key
        self._from = from_address
        self._reply_to = reply_to

    def _payload(self, *, to: str, code: str, ttl_seconds: int) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "from": self._from,
            "to": [to],
            "subject": templates.SUBJECT,
            "text": templates.render_text(code, ttl_seconds),
            "html": templates.render_html(code, ttl_seconds),
        }
        if self._reply_to:
            payload["reply_to"] = self._reply_to
        return payload

    async def send_login_code(
        self, *, to: str, code: str, idempotency_key: str, ttl_seconds: int
    ) -> SendResult:
        payload = self._payload(to=to, code=code, ttl_seconds=ttl_seconds)
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            # Resend deduplicates on this key, so an in-process retry after a
            # lost response cannot deliver a second copy.
            "Idempotency-Key": idempotency_key,
        }

        max_attempts = 1 + max(0, config.resend_max_inprocess_retries())
        timeout = config.resend_timeout_seconds()
        last: Optional[SendResult] = None

        for attempt in range(1, max_attempts + 1):
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    response = await client.post(
                        config.RESEND_API_URL, json=payload, headers=headers
                    )
            except Exception as exc:
                # Transport-level failure: genuinely ambiguous. The plaintext
                # code is still in memory here, which is the only moment a
                # same-code retry is possible at all.
                logger.warning(
                    "auth_email resend transport error attempt=%s/%s type=%s",
                    attempt,
                    max_attempts,
                    type(exc).__name__,
                )
                last = SendResult(
                    category=CATEGORY_AMBIGUOUS,
                    provider=self.name,
                    failure_category="provider_unreachable",
                    attempts=attempt,
                )
                continue

            category = _classify_status(response.status_code)
            if category == CATEGORY_OK:
                message_id = None
                try:
                    body = response.json()
                    if isinstance(body, dict):
                        raw_id = body.get("id")
                        if isinstance(raw_id, str):
                            message_id = raw_id[:120]
                except Exception:
                    # A 2xx without a parseable body is still an acceptance.
                    message_id = None
                logger.info(
                    "auth_email resend accepted status=%s attempt=%s message_id=%s",
                    response.status_code,
                    attempt,
                    message_id or "unknown",
                )
                return SendResult(
                    category=CATEGORY_OK,
                    provider=self.name,
                    provider_message_id=message_id,
                    attempts=attempt,
                )

            label = _failure_label(response.status_code)
            logger.warning(
                "auth_email resend rejected status=%s category=%s reason=%s attempt=%s/%s",
                response.status_code,
                category,
                label,
                attempt,
                max_attempts,
            )
            last = SendResult(
                category=category,
                provider=self.name,
                failure_category=label,
                attempts=attempt,
            )
            if category == CATEGORY_DEFINITIVE:
                return last

        return last or SendResult(
            category=CATEGORY_AMBIGUOUS,
            provider=self.name,
            failure_category="provider_unreachable",
            attempts=max_attempts,
        )


@dataclass
class SentMessage:
    """What a test double records — never the recipient's account state."""

    to: str
    code: str
    subject: str
    text: str
    html: str
    idempotency_key: str


class FakeSender:
    """In-memory sender for the automated suite. Never touches the network."""

    name = config.PROVIDER_FAKE

    def __init__(self, *, outcomes: Optional[List[str]] = None, from_address: str = ""):
        # A queue of categories to return; the last one repeats once drained.
        self._outcomes = list(outcomes or [CATEGORY_OK])
        self._from = from_address
        self.messages: List[SentMessage] = []
        self.calls: List[Dict[str, Any]] = []

    @property
    def from_address(self) -> str:
        return self._from

    def _next_category(self) -> str:
        if len(self._outcomes) > 1:
            return self._outcomes.pop(0)
        return self._outcomes[0] if self._outcomes else CATEGORY_OK

    async def send_login_code(
        self, *, to: str, code: str, idempotency_key: str, ttl_seconds: int
    ) -> SendResult:
        category = self._next_category()
        self.calls.append(
            {"to": to, "idempotency_key": idempotency_key, "category": category}
        )
        if category == CATEGORY_OK:
            # Deduplicate exactly as Resend would, so an in-process retry after
            # an ambiguous result records one delivered message, not two.
            already = any(m.idempotency_key == idempotency_key for m in self.messages)
            if not already:
                self.messages.append(
                    SentMessage(
                        to=to,
                        code=code,
                        subject=templates.SUBJECT,
                        text=templates.render_text(code, ttl_seconds),
                        html=templates.render_html(code, ttl_seconds),
                        idempotency_key=idempotency_key,
                    )
                )
            return SendResult(
                category=CATEGORY_OK,
                provider=self.name,
                provider_message_id=f"fake_{idempotency_key[:16]}",
            )
        if category == CATEGORY_DEFINITIVE:
            return SendResult(
                category=CATEGORY_DEFINITIVE,
                provider=self.name,
                failure_category="provider_rejected_request",
            )
        return SendResult(
            category=CATEGORY_AMBIGUOUS,
            provider=self.name,
            failure_category="provider_unreachable",
        )


class SinkSender:
    """SMTP sink (Mailpit/MailHog) for isolated end-to-end validation."""

    name = config.PROVIDER_SINK

    def __init__(self, *, host: str, port: int, from_address: str):
        self._host = host
        self._port = port
        self._from = from_address

    async def send_login_code(
        self, *, to: str, code: str, idempotency_key: str, ttl_seconds: int
    ) -> SendResult:
        import asyncio
        import smtplib
        from email.message import EmailMessage

        def _deliver() -> None:
            message = EmailMessage()
            message["Subject"] = templates.SUBJECT
            message["From"] = self._from
            message["To"] = to
            message["Idempotency-Key"] = idempotency_key
            message.set_content(templates.render_text(code, ttl_seconds))
            message.add_alternative(templates.render_html(code, ttl_seconds), subtype="html")
            with smtplib.SMTP(self._host, self._port, timeout=10) as smtp:
                smtp.send_message(message)

        try:
            await asyncio.get_running_loop().run_in_executor(None, _deliver)
        except Exception as exc:
            logger.warning("auth_email sink delivery failed type=%s", type(exc).__name__)
            return SendResult(
                category=CATEGORY_AMBIGUOUS,
                provider=self.name,
                failure_category="provider_unreachable",
            )
        return SendResult(
            category=CATEGORY_OK,
            provider=self.name,
            provider_message_id=f"sink_{idempotency_key[:16]}",
        )


# Process-wide override used by tests; production leaves this None.
_sender_override: Optional[EmailSender] = None


def set_sender_override(sender: Optional[EmailSender]) -> None:
    global _sender_override
    _sender_override = sender


def get_sender() -> Optional[EmailSender]:
    """Build the configured sender, or None when configuration is unusable."""
    if _sender_override is not None:
        return _sender_override

    provider = config.provider_name()
    from_address = config.email_from()

    if provider == config.PROVIDER_RESEND:
        api_key = config.resend_api_key()
        if not (api_key and from_address):
            return None
        return ResendSender(
            api_key=api_key,
            from_address=from_address,
            reply_to=config.email_reply_to(),
        )
    if provider == config.PROVIDER_SINK:
        import os

        return SinkSender(
            host=os.environ.get("AUTH_EMAIL_SINK_HOST", "127.0.0.1"),
            port=int(os.environ.get("AUTH_EMAIL_SINK_PORT", "1025")),
            from_address=from_address or "Perizia Scan <accesso@auth.nexodify.com>",
        )
    if provider == config.PROVIDER_FAKE:
        return FakeSender(from_address=from_address)
    return None
