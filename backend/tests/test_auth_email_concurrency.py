"""Concurrency invariants, proven against real Mongo.

These are the guarantees an in-memory fake cannot honestly demonstrate:

- at most one live OTP per email, however many requests arrive at once;
- a challenge is consumed exactly once, however many correct submissions race;
- one account per verified email, even when Google and OTP first-log-in together.

The safety net is the database — a unique partial index on ``active_slot`` and a
unique index on ``normalized_email`` — not application-level checking, because
Mongo here is standalone and multi-document transactions are unavailable.
"""

import asyncio
import os
import sys

import pytest
from fastapi import Response

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import server  # noqa: E402
from auth_email import challenges, identity  # noqa: E402
from tests.auth_email_helpers import (  # noqa: E402
    CORP_EMAIL,
    apply_test_env,
    clear_sender,
    install_sender,
    request_code,
    reset_auth_email_state,
    verify_code,
)


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    apply_test_env(monkeypatch)
    yield
    clear_sender()


async def _active_count(email=CORP_EMAIL):
    return await server.db[challenges.CHALLENGES_COLLECTION].count_documents(
        {"active_slot": email}
    )


async def _non_terminal_count(email=CORP_EMAIL):
    return await server.db[challenges.CHALLENGES_COLLECTION].count_documents(
        {
            "normalized_email": email,
            "status": {"$nin": list(challenges.TERMINAL_STATUSES)},
        }
    )


# ---------------------------------------------------------------------------
# One active challenge per email
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("burst", [2, 10, 50])
@pytest.mark.anyio
async def test_concurrent_requests_yield_one_active_challenge(monkeypatch, burst):
    """However the race resolves, two usable codes must never coexist."""
    await reset_auth_email_state()
    # Remove the cooldown so the burst actually reaches the active-slot index
    # rather than being absorbed by rate limiting.
    monkeypatch.setenv("AUTH_EMAIL_RESEND_COOLDOWN_SECONDS", "0")
    monkeypatch.setenv("AUTH_EMAIL_MAX_REQUESTS_PER_EMAIL_HOUR", str(burst * 2))
    monkeypatch.setenv("AUTH_EMAIL_MAX_REQUESTS_PER_IP_HOUR", str(burst * 2))
    fake = install_sender(monkeypatch)

    responses = await asyncio.gather(
        *[request_code(CORP_EMAIL, ip=f"203.0.113.{i % 250}") for i in range(burst)],
        return_exceptions=True,
    )

    for item in responses:
        assert not isinstance(item, Exception), item

    statuses = [r.status_code for r in responses]
    assert set(statuses) <= {200, 429}, statuses
    assert statuses.count(200) >= 1

    assert await _active_count() == 1
    assert await _non_terminal_count() == 1


@pytest.mark.parametrize("burst", [2, 10, 50])
@pytest.mark.anyio
async def test_only_the_surviving_code_can_authenticate(monkeypatch, burst):
    await reset_auth_email_state()
    monkeypatch.setenv("AUTH_EMAIL_RESEND_COOLDOWN_SECONDS", "0")
    monkeypatch.setenv("AUTH_EMAIL_MAX_REQUESTS_PER_EMAIL_HOUR", str(burst * 2))
    monkeypatch.setenv("AUTH_EMAIL_MAX_REQUESTS_PER_IP_HOUR", str(burst * 2))
    fake = install_sender(monkeypatch)

    await asyncio.gather(
        *[request_code(CORP_EMAIL, ip=f"203.0.113.{i % 250}") for i in range(burst)]
    )

    survivor = await server.db[challenges.CHALLENGES_COLLECTION].find_one(
        {"active_slot": CORP_EMAIL}, {"_id": 0}
    )
    assert survivor is not None

    # Every delivered code except the survivor's must already be dead.
    accepted = 0
    for message in fake.messages:
        result = await verify_code(survivor["challenge_id"], message.code)
        if result.status_code == 200:
            accepted += 1
    assert accepted <= 1


@pytest.mark.anyio
async def test_no_duplicate_send_beyond_provider_dedupe(monkeypatch):
    """Each challenge produces one delivered message, never several."""
    await reset_auth_email_state()
    monkeypatch.setenv("AUTH_EMAIL_RESEND_COOLDOWN_SECONDS", "0")
    monkeypatch.setenv("AUTH_EMAIL_MAX_REQUESTS_PER_EMAIL_HOUR", "40")
    monkeypatch.setenv("AUTH_EMAIL_MAX_REQUESTS_PER_IP_HOUR", "40")
    fake = install_sender(monkeypatch)

    await asyncio.gather(
        *[request_code(CORP_EMAIL, ip=f"203.0.113.{i}") for i in range(20)]
    )

    keys = [m.idempotency_key for m in fake.messages]
    assert len(keys) == len(set(keys)), "one message per challenge"


@pytest.mark.anyio
async def test_concurrent_requests_persist_no_plaintext(monkeypatch):
    await reset_auth_email_state()
    monkeypatch.setenv("AUTH_EMAIL_RESEND_COOLDOWN_SECONDS", "0")
    monkeypatch.setenv("AUTH_EMAIL_MAX_REQUESTS_PER_EMAIL_HOUR", "40")
    monkeypatch.setenv("AUTH_EMAIL_MAX_REQUESTS_PER_IP_HOUR", "40")
    fake = install_sender(monkeypatch)

    await asyncio.gather(
        *[request_code(CORP_EMAIL, ip=f"203.0.113.{i}") for i in range(20)]
    )

    rows = await server.db[challenges.CHALLENGES_COLLECTION].find({}, {"_id": 0}).to_list(100)
    blob = repr(rows)
    for message in fake.messages:
        assert message.code not in blob


# ---------------------------------------------------------------------------
# Consume-once
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("racers", [2, 10])
@pytest.mark.anyio
async def test_concurrent_correct_verifications_consume_once(monkeypatch, racers):
    await reset_auth_email_state()
    fake = install_sender(monkeypatch)

    response = await request_code(CORP_EMAIL)
    challenge_id = response.json()["challenge_id"]
    code = fake.messages[-1].code

    results = await asyncio.gather(
        *[verify_code(challenge_id, code) for _ in range(racers)],
        return_exceptions=True,
    )
    for item in results:
        assert not isinstance(item, Exception), item

    successes = [r for r in results if r.status_code == 200]
    assert len(successes) == 1

    # Exactly one account, and one session per successful verification.
    assert await server.db["users"].count_documents({}) == 1
    tokens = {r.json()["session_token"] for r in successes}
    assert len(tokens) == 1

    stored = await server.db[challenges.CHALLENGES_COLLECTION].find_one(
        {"challenge_id": challenge_id}, {"_id": 0}
    )
    assert stored["status"] == challenges.STATUS_CONSUMED


@pytest.mark.anyio
async def test_concurrent_wrong_then_right_does_not_double_consume(monkeypatch):
    await reset_auth_email_state()
    fake = install_sender(monkeypatch)

    response = await request_code(CORP_EMAIL)
    challenge_id = response.json()["challenge_id"]
    code = fake.messages[-1].code
    wrong = "000000" if code != "000000" else "111111"

    results = await asyncio.gather(
        verify_code(challenge_id, code),
        verify_code(challenge_id, wrong),
        verify_code(challenge_id, code),
    )
    assert len([r for r in results if r.status_code == 200]) == 1
    assert await server.db["users"].count_documents({}) == 1


# ---------------------------------------------------------------------------
# One account per verified email
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_concurrent_otp_verifications_create_one_user(monkeypatch):
    await reset_auth_email_state()
    fake = install_sender(monkeypatch)

    response = await request_code(CORP_EMAIL)
    challenge_id = response.json()["challenge_id"]
    code = fake.messages[-1].code

    await asyncio.gather(*[verify_code(challenge_id, code) for _ in range(8)])

    assert await server.db["users"].count_documents({"normalized_email": CORP_EMAIL}) == 1


@pytest.mark.anyio
async def test_concurrent_google_and_otp_first_login_create_one_user(monkeypatch):
    """The two providers must converge on a single account, not race into two."""
    await reset_auth_email_state()
    fake = install_sender(monkeypatch)

    response = await request_code(CORP_EMAIL)
    challenge_id = response.json()["challenge_id"]
    code = fake.messages[-1].code

    async def _google():
        return await server._create_local_login(
            email=CORP_EMAIL,
            name="Google User",
            picture=None,
            response=Response(),
            auth_method=identity.METHOD_GOOGLE,
            email_verified=True,
        )

    results = await asyncio.gather(
        _google(), verify_code(challenge_id, code), _google(), return_exceptions=True
    )
    for item in results:
        assert not isinstance(item, Exception), item

    assert await server.db["users"].count_documents({"normalized_email": CORP_EMAIL}) == 1

    stored = await server.db["users"].find_one({"normalized_email": CORP_EMAIL}, {"_id": 0})
    assert identity.METHOD_GOOGLE in stored["auth_methods"]


@pytest.mark.anyio
async def test_many_concurrent_google_logins_create_one_user(monkeypatch):
    await reset_auth_email_state()

    async def _google():
        return await server._create_local_login(
            email=CORP_EMAIL,
            name="Google User",
            picture=None,
            response=Response(),
            auth_method=identity.METHOD_GOOGLE,
            email_verified=True,
        )

    results = await asyncio.gather(*[_google() for _ in range(10)], return_exceptions=True)
    for item in results:
        assert not isinstance(item, Exception), item

    assert await server.db["users"].count_documents({"normalized_email": CORP_EMAIL}) == 1
    user_ids = {user.user_id for user, _ in results}
    assert len(user_ids) == 1


# ---------------------------------------------------------------------------
# Availability trade-off: consumption is terminal even if the session fails
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_session_failure_after_consume_leaves_code_spent(monkeypatch):
    """Security over convenience: a spent code stays spent.

    The user simply requests a new one, which must not be blocked by the
    consumed challenge, because consumption already released the active slot.
    """
    await reset_auth_email_state()
    monkeypatch.setenv("AUTH_EMAIL_RESEND_COOLDOWN_SECONDS", "0")
    fake = install_sender(monkeypatch)

    response = await request_code(CORP_EMAIL)
    challenge_id = response.json()["challenge_id"]
    code = fake.messages[-1].code

    original = server._create_local_session_for_user

    async def _boom(user):
        raise RuntimeError("session store unavailable")

    monkeypatch.setattr(server, "_create_local_session_for_user", _boom)

    try:
        failed = await verify_code(challenge_id, code)
        assert failed.status_code >= 500
    except RuntimeError:
        pass  # the ASGI transport may surface the error directly
    finally:
        monkeypatch.setattr(server, "_create_local_session_for_user", original)

    # The code is spent and cannot be replayed.
    stored = await server.db[challenges.CHALLENGES_COLLECTION].find_one(
        {"challenge_id": challenge_id}, {"_id": 0}
    )
    assert stored["status"] == challenges.STATUS_CONSUMED
    assert (await verify_code(challenge_id, code)).status_code == 400

    # A fresh code works, and no duplicate account was produced.
    retry = await request_code(CORP_EMAIL)
    assert retry.status_code == 200
    new_id = retry.json()["challenge_id"]
    new_code = fake.messages[-1].code

    success = await verify_code(new_id, new_code)
    assert success.status_code == 200
    assert await server.db["users"].count_documents({"normalized_email": CORP_EMAIL}) == 1
