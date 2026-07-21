"""Resend delivery contract.

Resend is never contacted: every request is intercepted by an httpx
``MockTransport``. These tests pin the wire contract (endpoint, From identity,
recipient, idempotency header, body content) and the classification of each
provider outcome into OK / DEFINITIVE / AMBIGUOUS.
"""

import logging
import os
import sys

import httpx
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from auth_email import config, templates  # noqa: E402
from auth_email import sender as sender_module  # noqa: E402
from tests.auth_email_helpers import CORP_EMAIL, apply_test_env  # noqa: E402

FROM_ADDRESS = "Perizia Scan <accesso@auth.nexodify.com>"
API_KEY = "re_test_key_never_real"


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    apply_test_env(monkeypatch, AUTH_EMAIL_PROVIDER="resend", RESEND_API_KEY=API_KEY)


def _install_transport(monkeypatch, handler):
    """Route every ResendSender request into an in-process handler."""
    captured = []

    def _handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return handler(request)

    original = httpx.AsyncClient

    def _factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(_handler)
        return original(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", _factory)
    return captured


def _sender():
    return sender_module.ResendSender(api_key=API_KEY, from_address=FROM_ADDRESS)


def _ok(request):
    return httpx.Response(200, json={"id": "msg_resend_123"})


# ---------------------------------------------------------------------------
# Wire contract
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_posts_to_resend_emails_endpoint(monkeypatch):
    captured = _install_transport(monkeypatch, _ok)
    await _sender().send_login_code(
        to=CORP_EMAIL, code="123456", idempotency_key="key-abc", ttl_seconds=600
    )
    assert str(captured[0].url) == config.RESEND_API_URL
    assert captured[0].method == "POST"


@pytest.mark.anyio
async def test_uses_configured_from_identity(monkeypatch):
    captured = _install_transport(monkeypatch, _ok)
    await _sender().send_login_code(
        to=CORP_EMAIL, code="123456", idempotency_key="key-abc", ttl_seconds=600
    )
    import json

    payload = json.loads(captured[0].content)
    assert payload["from"] == FROM_ADDRESS
    assert "auth.nexodify.com" in payload["from"]
    assert "resend.dev" not in payload["from"]


@pytest.mark.anyio
async def test_sends_to_the_requested_recipient_only(monkeypatch):
    captured = _install_transport(monkeypatch, _ok)
    await _sender().send_login_code(
        to=CORP_EMAIL, code="123456", idempotency_key="key-abc", ttl_seconds=600
    )
    import json

    payload = json.loads(captured[0].content)
    assert payload["to"] == [CORP_EMAIL]
    assert len(payload["to"]) == 1
    assert "cc" not in payload and "bcc" not in payload


@pytest.mark.anyio
async def test_includes_idempotency_key_header(monkeypatch):
    captured = _install_transport(monkeypatch, _ok)
    await _sender().send_login_code(
        to=CORP_EMAIL, code="123456", idempotency_key="key-abc", ttl_seconds=600
    )
    assert captured[0].headers["Idempotency-Key"] == "key-abc"


@pytest.mark.anyio
async def test_subject_and_expiry_text(monkeypatch):
    captured = _install_transport(monkeypatch, _ok)
    await _sender().send_login_code(
        to=CORP_EMAIL, code="123456", idempotency_key="key-abc", ttl_seconds=600
    )
    import json

    payload = json.loads(captured[0].content)
    assert payload["subject"] == templates.SUBJECT
    assert "123456" in payload["text"]
    assert "10 minuti" in payload["text"]


@pytest.mark.anyio
async def test_body_carries_no_account_state(monkeypatch):
    captured = _install_transport(monkeypatch, _ok)
    await _sender().send_login_code(
        to=CORP_EMAIL, code="123456", idempotency_key="key-abc", ttl_seconds=600
    )
    import json

    payload = json.loads(captured[0].content)
    blob = (payload["text"] + payload["html"]).lower()
    for forbidden in ("beta", "credit", "report", "admin", "quota", "abbonamento"):
        assert forbidden not in blob


@pytest.mark.anyio
async def test_reply_to_omitted_when_not_configured(monkeypatch):
    captured = _install_transport(monkeypatch, _ok)
    await _sender().send_login_code(
        to=CORP_EMAIL, code="1", idempotency_key="k", ttl_seconds=600
    )
    import json

    assert "reply_to" not in json.loads(captured[0].content)


@pytest.mark.anyio
async def test_reply_to_included_when_configured(monkeypatch):
    captured = _install_transport(monkeypatch, _ok)
    with_reply = sender_module.ResendSender(
        api_key=API_KEY, from_address=FROM_ADDRESS, reply_to="supporto@nexodify.com"
    )
    await with_reply.send_login_code(
        to=CORP_EMAIL, code="1", idempotency_key="k", ttl_seconds=600
    )
    import json

    assert json.loads(captured[0].content)["reply_to"] == "supporto@nexodify.com"


# ---------------------------------------------------------------------------
# Outcome classification
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_success_returns_ok_with_message_id(monkeypatch):
    _install_transport(monkeypatch, _ok)
    result = await _sender().send_login_code(
        to=CORP_EMAIL, code="123456", idempotency_key="k", ttl_seconds=600
    )
    assert result.ok is True
    assert result.provider_message_id == "msg_resend_123"
    assert result.delivery_state == sender_module.DELIVERY_SENT


@pytest.mark.parametrize("status", [400, 403, 422])
@pytest.mark.anyio
async def test_client_errors_are_definitive(monkeypatch, status):
    _install_transport(monkeypatch, lambda r: httpx.Response(status, json={"message": "no"}))
    result = await _sender().send_login_code(
        to=CORP_EMAIL, code="123456", idempotency_key="k", ttl_seconds=600
    )
    assert result.definitive_failure is True
    assert result.delivery_state == sender_module.DELIVERY_FAILED


@pytest.mark.parametrize("status", [500, 502, 503, 429, 408])
@pytest.mark.anyio
async def test_server_and_throttle_errors_are_ambiguous(monkeypatch, status):
    """These may still be delivered, or be retryable; never treat as definitive."""
    _install_transport(monkeypatch, lambda r: httpx.Response(status, json={"message": "x"}))
    result = await _sender().send_login_code(
        to=CORP_EMAIL, code="123456", idempotency_key="k", ttl_seconds=600
    )
    assert result.category == sender_module.CATEGORY_AMBIGUOUS
    assert result.definitive_failure is False


@pytest.mark.anyio
async def test_network_timeout_is_ambiguous(monkeypatch):
    def _boom(request):
        raise httpx.ConnectTimeout("timed out")

    _install_transport(monkeypatch, _boom)
    result = await _sender().send_login_code(
        to=CORP_EMAIL, code="123456", idempotency_key="k", ttl_seconds=600
    )
    assert result.category == sender_module.CATEGORY_AMBIGUOUS
    assert result.failure_category == "provider_unreachable"


# ---------------------------------------------------------------------------
# In-process retry (correction 2A)
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_ambiguous_timeout_retries_in_process_with_same_code_and_key(monkeypatch):
    """The plaintext still exists in this frame, so a same-code retry is safe.

    Both attempts must carry the identical body and Idempotency-Key, so Resend
    deduplicates and the user cannot receive two different live codes.
    """
    monkeypatch.setenv("AUTH_EMAIL_RESEND_MAX_INPROCESS_RETRIES", "1")
    attempts = {"n": 0}

    def _flaky(request):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise httpx.ReadTimeout("lost response")
        return httpx.Response(200, json={"id": "msg_second"})

    captured = _install_transport(monkeypatch, _flaky)
    result = await _sender().send_login_code(
        to=CORP_EMAIL, code="123456", idempotency_key="stable-key", ttl_seconds=600
    )

    assert result.ok is True
    assert result.attempts == 2
    assert len(captured) == 2
    assert captured[0].headers["Idempotency-Key"] == captured[1].headers["Idempotency-Key"] == "stable-key"
    assert captured[0].content == captured[1].content


@pytest.mark.anyio
async def test_definitive_failure_is_not_retried(monkeypatch):
    calls = {"n": 0}

    def _refuse(request):
        calls["n"] += 1
        return httpx.Response(422, json={"message": "invalid recipient"})

    monkeypatch.setenv("AUTH_EMAIL_RESEND_MAX_INPROCESS_RETRIES", "3")
    _install_transport(monkeypatch, _refuse)
    result = await _sender().send_login_code(
        to=CORP_EMAIL, code="123456", idempotency_key="k", ttl_seconds=600
    )
    assert result.definitive_failure is True
    assert calls["n"] == 1


@pytest.mark.anyio
async def test_repeated_provider_retry_delivers_once(monkeypatch):
    """The fake models Resend's dedupe: one key, one delivered message."""
    fake = sender_module.FakeSender(from_address=FROM_ADDRESS)
    for _ in range(4):
        await fake.send_login_code(
            to=CORP_EMAIL, code="123456", idempotency_key="same-key", ttl_seconds=600
        )
    assert len(fake.messages) == 1
    assert len(fake.calls) == 4


# ---------------------------------------------------------------------------
# Secret hygiene
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_api_key_and_code_never_logged(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)

    def _fail(request):
        return httpx.Response(500, json={"message": "boom", "trace": "internal"})

    monkeypatch.setenv("AUTH_EMAIL_RESEND_MAX_INPROCESS_RETRIES", "1")
    _install_transport(monkeypatch, _fail)
    await _sender().send_login_code(
        to=CORP_EMAIL, code="987654", idempotency_key="k", ttl_seconds=600
    )

    logged = caplog.text
    assert API_KEY not in logged
    assert "987654" not in logged
    assert "Bearer" not in logged
    # Raw provider payloads must not be echoed either.
    assert "internal" not in logged


@pytest.mark.anyio
async def test_success_log_contains_no_code(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    _install_transport(monkeypatch, _ok)
    await _sender().send_login_code(
        to=CORP_EMAIL, code="424242", idempotency_key="k", ttl_seconds=600
    )
    assert "424242" not in caplog.text


# ---------------------------------------------------------------------------
# Selection
# ---------------------------------------------------------------------------
def test_get_sender_returns_none_without_api_key(monkeypatch):
    apply_test_env(monkeypatch, AUTH_EMAIL_PROVIDER="resend", RESEND_API_KEY="")
    sender_module.set_sender_override(None)
    assert sender_module.get_sender() is None


def test_get_sender_builds_resend_when_configured(monkeypatch):
    apply_test_env(monkeypatch, AUTH_EMAIL_PROVIDER="resend", RESEND_API_KEY=API_KEY)
    sender_module.set_sender_override(None)
    built = sender_module.get_sender()
    assert isinstance(built, sender_module.ResendSender)


def test_unknown_provider_yields_no_sender(monkeypatch):
    apply_test_env(monkeypatch, AUTH_EMAIL_PROVIDER="carrier-pigeon")
    sender_module.set_sender_override(None)
    assert sender_module.get_sender() is None
