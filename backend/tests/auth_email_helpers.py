"""Shared fixtures for the passwordless email auth suites.

These tests run against a REAL Mongo instance, isolated to the test database
that ``backend/conftest.py`` forces before ``server`` is imported. Real Mongo is
deliberate rather than convenient: the guarantees under test — a unique partial
index on ``active_slot``, a unique index on ``normalized_email``, and
single-document atomicity under genuine concurrency — are database behaviours.
An in-memory fake would assert that the fake works.

Synthetic identities only. No real tester address appears anywhere here, and the
sender is always a fake or a local sink, so the suite can never contact Resend
or deliver mail.
"""

import os
import sys
from typing import Any, Dict, List, Optional

import httpx

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import server  # noqa: E402
from auth_email import challenges, config, identity, ratelimit  # noqa: E402
from auth_email import sender as sender_module  # noqa: E402

# Non-Google corporate domains, mirroring the real beta situation without ever
# naming a real tester.
CORP_EMAIL = "test.user@aglaste-example-corp.it"
CORP_EMAIL_2 = "second.user@example-immobiliare.it"
MS365_EMAIL = "utente@example-ms365.onmicrosoft.com"
OWNER_EMAIL = "nexodifyforyou@gmail.com"

TEST_PEPPER = "test-pepper-value-for-auth-email-otp-suite-0123456789"


def apply_test_env(monkeypatch, **overrides: Any) -> None:
    """Enable OTP with safe, fully local settings."""
    env: Dict[str, str] = {
        "AUTH_EMAIL_ENABLED": "true",
        "AUTH_EMAIL_PROVIDER": "fake",
        "AUTH_EMAIL_FROM": "Perizia Scan <accesso@auth.nexodify.com>",
        "AUTH_EMAIL_REPLY_TO": "",
        "AUTH_EMAIL_CODE_PEPPER": TEST_PEPPER,
        "AUTH_EMAIL_SENDER_DOMAIN_VERIFIED": "true",
        "AUTH_EMAIL_CODE_TTL_SECONDS": "600",
        "AUTH_EMAIL_PURGE_AFTER_SECONDS": "172800",
        "AUTH_EMAIL_RESEND_COOLDOWN_SECONDS": "60",
        "AUTH_EMAIL_MAX_REQUESTS_PER_EMAIL_HOUR": "5",
        "AUTH_EMAIL_MAX_REQUESTS_PER_IP_HOUR": "20",
        "AUTH_EMAIL_MAX_VERIFY_ATTEMPTS": "5",
        # Never let a test reach the network even if a provider were selected.
        "RESEND_API_KEY": "",
    }
    env.update({k: str(v) for k, v in overrides.items()})
    for key, value in env.items():
        monkeypatch.setenv(key, value)


def rebind_db() -> None:
    """Rebind the Motor handle to the currently running event loop.

    Motor binds a client to the loop that is running when it is created, and
    pytest-anyio gives each test a fresh loop. The module-level client built at
    ``server`` import time therefore belongs to a dead loop by the second test.
    Rebuilding it per test is what lets these suites use real Mongo at all.
    """
    from motor.motor_asyncio import AsyncIOMotorClient

    mongo_url = os.environ["MONGO_URL"]
    db_name = os.environ["DB_NAME"]

    # Layer-4 guard: never let a rebind escape the isolated test database.
    from conftest import assert_not_production

    assert_not_production(db_name, mongo_url)

    previous = getattr(server, "client", None)
    server.client = AsyncIOMotorClient(mongo_url)
    server.db = server.client[db_name]
    if previous is not None:
        try:
            previous.close()
        except Exception:
            pass


async def reset_auth_email_state(*, drop_users: bool = True) -> None:
    """Clear every collection these suites touch, then rebuild indexes."""
    rebind_db()
    db = server.db
    await db[challenges.CHALLENGES_COLLECTION].delete_many({})
    await db[ratelimit.RATE_COLLECTION].delete_many({})
    if drop_users:
        await db["users"].delete_many({})
        await db["user_sessions"].delete_many({})
        await db["beta_program_memberships"].delete_many({})
        await db["beta_program_audit"].delete_many({})
        await db["perizia_analyses"].delete_many({})

    # Index state is process-cached; force a genuine rebuild each time.
    challenges._indexes_ready = False
    ratelimit._indexes_ready = False
    identity.reset_index_cache()
    await challenges.ensure_indexes()
    await ratelimit.ensure_indexes()
    await identity.ensure_unique_index(db)


async def drop_identity_index() -> None:
    """Remove the unique identity index to exercise the fail-closed path."""
    try:
        await server.db["users"].drop_index(identity.NORMALIZED_EMAIL_INDEX)
    except Exception:
        pass
    identity.reset_index_cache()


def install_sender(monkeypatch, outcomes: Optional[List[str]] = None) -> sender_module.FakeSender:
    fake = sender_module.FakeSender(
        outcomes=outcomes, from_address="Perizia Scan <accesso@auth.nexodify.com>"
    )
    sender_module.set_sender_override(fake)
    return fake


def clear_sender() -> None:
    sender_module.set_sender_override(None)


# Captured before any test can monkeypatch httpx.AsyncClient to intercept
# outbound provider calls; the test client itself must never be intercepted.
_REAL_ASYNC_CLIENT = httpx.AsyncClient


async def api_request(method: str, path: str, **kwargs) -> httpx.Response:
    transport = httpx.ASGITransport(app=server.app)
    async with _REAL_ASYNC_CLIENT(transport=transport, base_url="http://test") as client:
        return await client.request(method, path, **kwargs)


async def request_code(email: str, ip: str = "203.0.113.10") -> httpx.Response:
    return await api_request(
        "POST",
        "/api/auth/email/request-code",
        json={"email": email},
        headers={"x-forwarded-for": ip},
    )


async def verify_code(challenge_id: str, code: str) -> httpx.Response:
    return await api_request(
        "POST",
        "/api/auth/email/verify-code",
        json={"challenge_id": challenge_id, "code": code},
    )


async def request_and_capture(
    fake: sender_module.FakeSender, email: str, ip: str = "203.0.113.10"
):
    """Request a code and read the plaintext back out of the fake mailbox.

    This is the only place a test learns a code: it comes from the delivered
    message, exactly as a real user would receive it, never from the database.
    """
    before = len(fake.messages)
    response = await request_code(email, ip=ip)
    code = fake.messages[-1].code if len(fake.messages) > before else None
    challenge_id = response.json().get("challenge_id") if response.status_code == 200 else None
    return response, challenge_id, code


async def stored_challenge(challenge_id: str) -> Optional[Dict[str, Any]]:
    return await server.db[challenges.CHALLENGES_COLLECTION].find_one(
        {"challenge_id": challenge_id}, {"_id": 0}
    )
