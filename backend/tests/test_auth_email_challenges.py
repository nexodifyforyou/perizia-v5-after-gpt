"""Challenge lifecycle, delivery contract, and the expiry/retention split.

Runs against real Mongo (isolated test DB) because the invariants under test —
the unique partial index on ``active_slot``, TTL placement, and atomic terminal
consumption — are database behaviours.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import server  # noqa: E402
from auth_email import challenges, config  # noqa: E402
from auth_email import sender as sender_module  # noqa: E402
from tests.auth_email_helpers import (  # noqa: E402
    CORP_EMAIL,
    apply_test_env,
    install_sender,
    reset_auth_email_state,
    stored_challenge,
)


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    apply_test_env(monkeypatch)


async def _fresh():
    await reset_auth_email_state()


async def _create(email=CORP_EMAIL, **kwargs):
    doc, code, error = await challenges.create(normalized_email=email, **kwargs)
    assert error is None
    return doc, code


async def _create_sent(email=CORP_EMAIL):
    """Create a challenge and mark it delivered — the normal post-send state."""
    doc, code = await _create(email)
    await challenges.record_send_result(
        doc["challenge_id"],
        sender_module.SendResult(category=sender_module.CATEGORY_OK, provider="fake"),
    )
    return doc, code


# ---------------------------------------------------------------------------
# Storage: hashed only, never plaintext
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_code_is_stored_hashed_never_plaintext():
    await _fresh()
    doc, code = await _create()
    persisted = await stored_challenge(doc["challenge_id"])

    assert persisted["code_hash"]
    assert persisted["code_hash"] != code
    assert len(persisted["code_hash"]) == 64  # sha256 hex
    # The plaintext must not appear in ANY field of the document.
    blob = repr(persisted)
    assert code not in blob


@pytest.mark.anyio
async def test_no_reversible_code_field_exists():
    """Guards against a future 'encrypted code' shortcut for resending."""
    await _fresh()
    doc, _ = await _create()
    persisted = await stored_challenge(doc["challenge_id"])
    for forbidden in ("code", "plaintext", "code_plain", "code_encrypted", "code_cipher"):
        assert forbidden not in persisted


@pytest.mark.anyio
async def test_no_raw_client_data_retained():
    await _fresh()
    doc, _ = await _create(
        CORP_EMAIL, request_ip="203.0.113.7", user_agent="Mozilla/5.0 probe"
    )
    persisted = await stored_challenge(doc["challenge_id"])
    blob = repr(persisted)
    assert "203.0.113.7" not in blob
    assert "Mozilla" not in blob
    assert persisted["request_ip_hash"]
    assert persisted["user_agent_hash"]


# ---------------------------------------------------------------------------
# Verifiable status set (correction 1)
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_sent_challenge_can_be_verified():
    await _fresh()
    doc, code = await _create()
    await challenges.record_send_result(
        doc["challenge_id"],
        sender_module.SendResult(category=sender_module.CATEGORY_OK, provider="fake"),
    )
    assert (await stored_challenge(doc["challenge_id"]))["status"] == challenges.STATUS_SENT

    result, claimed = await challenges.consume(doc["challenge_id"], code)
    assert result == challenges.RESULT_OK
    assert claimed["status"] == challenges.STATUS_CONSUMED


@pytest.mark.anyio
async def test_send_pending_challenge_can_be_verified():
    """An ambiguous provider timeout may still have delivered the message.

    Possession of the correct code is proof of receipt, so requiring provider
    acknowledgement would lock out users whose mail actually arrived.
    """
    await _fresh()
    doc, code = await _create()
    await challenges.mark_send_pending(doc["challenge_id"], provider="fake")
    await challenges.record_send_result(
        doc["challenge_id"],
        sender_module.SendResult(
            category=sender_module.CATEGORY_AMBIGUOUS,
            provider="fake",
            failure_category="provider_unreachable",
        ),
    )
    assert (
        await stored_challenge(doc["challenge_id"])
    )["status"] == challenges.STATUS_SEND_PENDING

    result, claimed = await challenges.consume(doc["challenge_id"], code)
    assert result == challenges.RESULT_OK
    assert claimed["status"] == challenges.STATUS_CONSUMED


@pytest.mark.anyio
async def test_created_challenge_is_not_yet_verifiable():
    """Before a send is even attempted there is nothing the user could hold."""
    await _fresh()
    doc, code = await _create()
    assert (await stored_challenge(doc["challenge_id"]))["status"] == challenges.STATUS_CREATED

    result, _ = await challenges.consume(doc["challenge_id"], code)
    assert result == challenges.RESULT_INVALID


@pytest.mark.anyio
async def test_send_failed_challenge_cannot_be_verified():
    """A definitive provider refusal means the mail will never arrive."""
    await _fresh()
    doc, code = await _create()
    await challenges.record_send_result(
        doc["challenge_id"],
        sender_module.SendResult(
            category=sender_module.CATEGORY_DEFINITIVE,
            provider="fake",
            failure_category="provider_rejected_request",
        ),
    )
    persisted = await stored_challenge(doc["challenge_id"])
    assert persisted["status"] == challenges.STATUS_SEND_FAILED
    assert "active_slot" not in persisted  # slot released

    result, claimed = await challenges.consume(doc["challenge_id"], code)
    assert result == challenges.RESULT_INVALID
    assert claimed is None


@pytest.mark.anyio
async def test_definitive_failure_persists_safe_metadata_only():
    await _fresh()
    doc, code = await _create()
    await challenges.record_send_result(
        doc["challenge_id"],
        sender_module.SendResult(
            category=sender_module.CATEGORY_DEFINITIVE,
            provider="resend",
            failure_category="provider_rejected_request",
        ),
    )
    persisted = await stored_challenge(doc["challenge_id"])
    assert persisted["provider"] == "resend"
    assert persisted["delivery_state"] == sender_module.DELIVERY_FAILED
    assert persisted["failure_category"] == "provider_rejected_request"
    assert code not in repr(persisted)


@pytest.mark.anyio
async def test_successful_send_records_provider_message_id():
    await _fresh()
    doc, _ = await _create()
    await challenges.record_send_result(
        doc["challenge_id"],
        sender_module.SendResult(
            category=sender_module.CATEGORY_OK,
            provider="resend",
            provider_message_id="msg_abc123",
        ),
    )
    persisted = await stored_challenge(doc["challenge_id"])
    assert persisted["provider_message_id"] == "msg_abc123"
    assert persisted["delivery_state"] == sender_module.DELIVERY_SENT
    assert persisted["send_attempted_at"]


# ---------------------------------------------------------------------------
# Single use, attempts, supersession
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_correct_code_succeeds_once_and_cannot_be_reused():
    await _fresh()
    doc, code = await _create()
    await challenges.record_send_result(
        doc["challenge_id"],
        sender_module.SendResult(category=sender_module.CATEGORY_OK, provider="fake"),
    )

    assert (await challenges.consume(doc["challenge_id"], code))[0] == challenges.RESULT_OK
    again, _ = await challenges.consume(doc["challenge_id"], code)
    assert again == challenges.RESULT_INVALID


@pytest.mark.anyio
async def test_consumption_is_one_terminal_write():
    """No intermediate VERIFIED state that a second caller could still claim."""
    await _fresh()
    doc, code = await _create()
    await challenges.record_send_result(
        doc["challenge_id"],
        sender_module.SendResult(category=sender_module.CATEGORY_OK, provider="fake"),
    )
    await challenges.consume(doc["challenge_id"], code)

    persisted = await stored_challenge(doc["challenge_id"])
    assert persisted["status"] == challenges.STATUS_CONSUMED
    assert persisted["consumed_at"]
    assert persisted["verified_at"]
    assert persisted["consumption_reason"] == challenges.CONSUMPTION_REASON_OTP
    assert "active_slot" not in persisted


@pytest.mark.anyio
async def test_incorrect_code_fails_and_increments_attempts():
    await _fresh()
    doc, code = await _create_sent()
    wrong = "000000" if code != "000000" else "111111"

    result, _ = await challenges.consume(doc["challenge_id"], wrong)
    assert result == challenges.RESULT_INVALID
    assert (await stored_challenge(doc["challenge_id"]))["attempt_count"] == 1


@pytest.mark.anyio
async def test_five_failed_attempts_lock_the_challenge():
    await _fresh()
    doc, code = await _create_sent()
    wrong = "000000" if code != "000000" else "111111"

    for _ in range(4):
        assert (await challenges.consume(doc["challenge_id"], wrong))[0] == challenges.RESULT_INVALID

    assert (await challenges.consume(doc["challenge_id"], wrong))[0] == challenges.RESULT_LOCKED

    persisted = await stored_challenge(doc["challenge_id"])
    assert persisted["status"] == challenges.STATUS_LOCKED
    assert "active_slot" not in persisted

    # Even the correct code is refused once locked.
    assert (await challenges.consume(doc["challenge_id"], code))[0] == challenges.RESULT_INVALID


@pytest.mark.anyio
async def test_new_code_supersedes_the_previous_one():
    await _fresh()
    first, first_code = await _create()
    second, second_code = await _create()

    assert first["challenge_id"] != second["challenge_id"]
    assert first["idempotency_key"] != second["idempotency_key"]

    old = await stored_challenge(first["challenge_id"])
    assert old["status"] == challenges.STATUS_SUPERSEDED
    assert "active_slot" not in old

    # The old code is dead immediately.
    assert (await challenges.consume(first["challenge_id"], first_code))[0] == challenges.RESULT_INVALID

    await challenges.record_send_result(
        second["challenge_id"],
        sender_module.SendResult(category=sender_module.CATEGORY_OK, provider="fake"),
    )
    assert (await challenges.consume(second["challenge_id"], second_code))[0] == challenges.RESULT_OK


@pytest.mark.anyio
async def test_resend_generates_a_different_code_not_the_original():
    """The plaintext is unrecoverable after its request ends, by design."""
    await _fresh()
    codes = set()
    for _ in range(12):
        _, code = await _create()
        codes.add(code)
    # A reconstructed original would produce one repeated value.
    assert len(codes) > 1


@pytest.mark.anyio
async def test_only_one_active_challenge_per_email():
    await _fresh()
    await _create()
    await _create()
    live = await server.db[challenges.CHALLENGES_COLLECTION].count_documents(
        {"active_slot": CORP_EMAIL}
    )
    assert live == 1


@pytest.mark.anyio
async def test_second_challenge_without_supersede_is_refused():
    """The DB index, not application logic, is what forbids two live codes."""
    await _fresh()
    await _create()
    doc, code, error = await challenges.create(normalized_email=CORP_EMAIL, supersede=False)
    assert error == "ACTIVE_CHALLENGE_EXISTS"
    assert doc is None


# ---------------------------------------------------------------------------
# Expiry vs retention (correction 5)
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_expired_code_is_refused():
    await _fresh()
    doc, code = await _create()
    past = datetime.now(timezone.utc) - timedelta(seconds=5)
    await server.db[challenges.CHALLENGES_COLLECTION].update_one(
        {"challenge_id": doc["challenge_id"]},
        {"$set": {"status": challenges.STATUS_SENT, "expires_at": past.isoformat()}},
    )

    result, _ = await challenges.consume(doc["challenge_id"], code)
    assert result == challenges.RESULT_INVALID

    persisted = await stored_challenge(doc["challenge_id"])
    assert persisted["status"] == challenges.STATUS_EXPIRED
    assert "active_slot" not in persisted


@pytest.mark.anyio
async def test_expired_record_survives_for_diagnosis_until_purge_at():
    """expires_at ends authentication; purge_at ends retention."""
    await _fresh()
    doc, code = await _create()
    past = datetime.now(timezone.utc) - timedelta(seconds=5)
    await server.db[challenges.CHALLENGES_COLLECTION].update_one(
        {"challenge_id": doc["challenge_id"]},
        {"$set": {"status": challenges.STATUS_SENT, "expires_at": past.isoformat()}},
    )
    await challenges.consume(doc["challenge_id"], code)

    persisted = await stored_challenge(doc["challenge_id"])
    assert persisted is not None, "evidence must outlive authentication validity"
    assert persisted["status"] == challenges.STATUS_EXPIRED

    purge = persisted["purge_at"]
    if purge.tzinfo is None:
        purge = purge.replace(tzinfo=timezone.utc)
    # Authentication is already invalid, yet the record is still retained.
    assert purge > datetime.now(timezone.utc)


@pytest.mark.anyio
async def test_purge_at_is_far_beyond_expires_at():
    await _fresh()
    doc, _ = await _create()
    persisted = await stored_challenge(doc["challenge_id"])

    expires = datetime.fromisoformat(persisted["expires_at"])
    purge = persisted["purge_at"]
    if purge.tzinfo is None:
        purge = purge.replace(tzinfo=timezone.utc)
    assert purge > expires
    assert (purge - expires).total_seconds() > 3600


@pytest.mark.anyio
async def test_ttl_index_is_on_purge_at_not_expires_at():
    """Deleting on expires_at would destroy the diagnostic window."""
    await _fresh()
    info = await server.db[challenges.CHALLENGES_COLLECTION].index_information()

    ttl_fields = []
    for spec in info.values():
        if "expireAfterSeconds" not in spec:
            continue
        keys = spec.get("key") or []
        ttl_fields.extend(k[0] if isinstance(k, (list, tuple)) else k for k in keys)

    assert "purge_at" in ttl_fields
    assert "expires_at" not in ttl_fields


@pytest.mark.anyio
async def test_expire_if_stale_releases_an_abandoned_slot():
    await _fresh()
    doc, _ = await _create()
    past = datetime.now(timezone.utc) - timedelta(seconds=5)
    await server.db[challenges.CHALLENGES_COLLECTION].update_one(
        {"challenge_id": doc["challenge_id"]},
        {"$set": {"expires_at": past.isoformat()}},
    )

    await challenges.expire_if_stale(CORP_EMAIL)
    assert await challenges.get_active(CORP_EMAIL) is None


@pytest.mark.anyio
async def test_required_indexes_exist():
    await _fresh()
    info = await server.db[challenges.CHALLENGES_COLLECTION].index_information()
    names = set(info)
    assert "uq_auth_email_challenge_id" in names
    assert info["uq_auth_email_challenge_id"].get("unique") is True
    assert "uq_auth_email_active_slot" in names
    assert info["uq_auth_email_active_slot"].get("unique") is True
    assert "ix_auth_email_challenge_email" in names
    assert "ix_auth_email_challenge_status" in names
