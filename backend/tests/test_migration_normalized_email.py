"""Identity migration: dry-run reporting, conflict refusal, idempotency.

The migration must never merge or delete a historical account, and must never
touch credits, reports, subscriptions or beta memberships.
"""

import importlib.util
import os
import sys

import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import server  # noqa: E402
from auth_email import identity  # noqa: E402
from tests.auth_email_helpers import (  # noqa: E402
    CORP_EMAIL,
    CORP_EMAIL_2,
    apply_test_env,
    rebind_db,
    reset_auth_email_state,
)

_SCRIPT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "scripts",
    "migrate_normalized_email.py",
)
_spec = importlib.util.spec_from_file_location("migrate_normalized_email", _SCRIPT)
migration = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(migration)


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    apply_test_env(monkeypatch)


async def _clean(drop_index=True):
    await reset_auth_email_state()
    if drop_index:
        try:
            await server.db["users"].drop_index(identity.NORMALIZED_EMAIL_INDEX)
        except Exception:
            pass
        identity.reset_index_cache()


async def _legacy_user(user_id, email, **extra):
    """A user document as it existed before the identity fields were added."""
    doc = {"user_id": user_id, "email": email, "name": "Legacy", "plan": "free"}
    doc.update(extra)
    await server.db["users"].insert_one(doc)
    return doc


# ---------------------------------------------------------------------------
# Scan / dry run
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_scan_counts_users_missing_canonical_field():
    await _clean()
    await _legacy_user("u1", CORP_EMAIL)
    await _legacy_user("u2", CORP_EMAIL_2)

    scan = await migration._scan(server.db)

    assert scan["total_users"] == 2
    assert scan["missing_normalized_email"] == 2
    assert scan["duplicate_groups"] == []
    assert scan["index_ready"] is False


@pytest.mark.anyio
async def test_dry_run_writes_nothing():
    await _clean()
    await _legacy_user("u1", CORP_EMAIL)

    await migration._scan(server.db)

    stored = await server.db["users"].find_one({"user_id": "u1"}, {"_id": 0})
    assert "normalized_email" not in stored
    assert "auth_methods" not in stored
    assert await identity.unique_index_ready(server.db, use_cache=False) is False


@pytest.mark.anyio
async def test_scan_detects_duplicates_before_backfill():
    """Case-different addresses are the same identity and must be flagged."""
    await _clean()
    await _legacy_user("u1", CORP_EMAIL)
    await _legacy_user("u2", CORP_EMAIL.upper())

    scan = await migration._scan(server.db)

    assert len(scan["duplicate_groups"]) == 1
    group = scan["duplicate_groups"][0]
    assert group["count"] == 2
    assert sorted(group["user_ids"]) == ["u1", "u2"]


@pytest.mark.anyio
async def test_conflict_report_masks_addresses():
    await _clean()
    await _legacy_user("u1", CORP_EMAIL)
    await _legacy_user("u2", CORP_EMAIL.upper())

    scan = await migration._scan(server.db)
    masked = scan["duplicate_groups"][0]["email_masked"]

    assert CORP_EMAIL not in masked
    assert "***" in masked
    assert masked.endswith("@aglaste-example-corp.it")


@pytest.mark.anyio
async def test_conflict_report_flags_accounts_holding_value():
    """Decision support for a human, not an instruction to merge."""
    await _clean()
    await _legacy_user("u1", CORP_EMAIL)
    await _legacy_user(
        "u2", CORP_EMAIL.upper(), perizia_credits={"total_available": 4}
    )
    await server.db["perizia_analyses"].insert_one({"analysis_id": "a1", "user_id": "u1"})

    scan = await migration._scan(server.db)
    with_value = {a["user_id"] for a in scan["duplicate_groups"][0]["accounts_with_value"]}

    assert with_value == {"u1", "u2"}


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_backfill_populates_canonical_fields():
    await _clean()
    await _legacy_user("u1", "  Mario.Rossi@Example-Corp.IT  ")

    result = await migration._backfill(server.db)

    stored = await server.db["users"].find_one({"user_id": "u1"}, {"_id": 0})
    assert result["updated"] == 1
    assert stored["normalized_email"] == "mario.rossi@example-corp.it"
    assert stored["auth_methods"] == [identity.METHOD_LEGACY]
    assert stored["email_verified"] is False


@pytest.mark.anyio
async def test_backfill_marks_google_users_verified():
    """Only where the provider actually guaranteed a verified address."""
    await _clean()
    await _legacy_user("u1", CORP_EMAIL, last_login_method=identity.METHOD_GOOGLE)
    await _legacy_user("u2", CORP_EMAIL_2)

    await migration._backfill(server.db)

    google_user = await server.db["users"].find_one({"user_id": "u1"}, {"_id": 0})
    legacy_user = await server.db["users"].find_one({"user_id": "u2"}, {"_id": 0})

    assert google_user["email_verified"] is True
    assert google_user["auth_methods"] == [identity.METHOD_GOOGLE]
    assert legacy_user["email_verified"] is False


@pytest.mark.anyio
async def test_backfill_preserves_user_ids_and_value():
    await _clean()
    await _legacy_user(
        "u_stable",
        CORP_EMAIL,
        perizia_credits={"total_available": 9},
        subscription_state={"status": "active"},
        quota={"perizia_scans_remaining": 9},
    )
    await server.db["perizia_analyses"].insert_one({"analysis_id": "a1", "user_id": "u_stable"})
    await server.db["beta_program_memberships"].insert_one(
        {"membership_id": "b1", "normalized_email": CORP_EMAIL, "status": "ACTIVE",
         "user_id": "u_stable", "analysis_limit": 5, "analysis_consumed": 2}
    )

    await migration._backfill(server.db)

    stored = await server.db["users"].find_one({"user_id": "u_stable"}, {"_id": 0})
    assert stored["user_id"] == "u_stable"
    assert stored["perizia_credits"]["total_available"] == 9
    assert stored["subscription_state"]["status"] == "active"
    assert stored["quota"]["perizia_scans_remaining"] == 9

    membership = await server.db["beta_program_memberships"].find_one({"membership_id": "b1"})
    assert membership["analysis_consumed"] == 2
    assert membership["analysis_limit"] == 5
    assert membership["status"] == "ACTIVE"
    assert await server.db["perizia_analyses"].count_documents({"user_id": "u_stable"}) == 1


@pytest.mark.anyio
async def test_backfill_is_idempotent():
    await _clean()
    await _legacy_user("u1", CORP_EMAIL)
    await _legacy_user("u2", CORP_EMAIL_2)

    first = await migration._backfill(server.db)
    second = await migration._backfill(server.db)

    assert first["updated"] == 2
    assert second["updated"] == 0
    assert second["already_canonical"] == 2


@pytest.mark.anyio
async def test_index_created_after_clean_backfill():
    await _clean()
    await _legacy_user("u1", CORP_EMAIL)
    await _legacy_user("u2", CORP_EMAIL_2)

    await migration._backfill(server.db)
    created = await identity.ensure_unique_index(server.db)

    assert created is True
    assert await identity.unique_index_ready(server.db, use_cache=False) is True


@pytest.mark.anyio
async def test_index_refused_and_nothing_deleted_when_duplicates_exist():
    await _clean()
    await _legacy_user("u1", CORP_EMAIL)
    await _legacy_user("u2", CORP_EMAIL.upper())

    await migration._backfill(server.db)
    created = await identity.ensure_unique_index(server.db)

    assert created is False
    assert await server.db["users"].count_documents({}) == 2
    ids = sorted(d["user_id"] for d in await server.db["users"].find({}, {"_id": 0}).to_list(10))
    assert ids == ["u1", "u2"]


@pytest.mark.anyio
async def test_users_without_email_are_skipped_not_deleted():
    await _clean()
    await server.db["users"].insert_one({"user_id": "u_no_email", "name": "Orphan"})
    await _legacy_user("u1", CORP_EMAIL)

    result = await migration._backfill(server.db)

    assert result["updated"] == 1
    assert await server.db["users"].count_documents({"user_id": "u_no_email"}) == 1


@pytest.mark.anyio
async def test_partial_index_tolerates_users_without_canonical_field():
    """A document lacking normalized_email must not collide under the index."""
    await _clean()
    await _legacy_user("u1", CORP_EMAIL)
    await migration._backfill(server.db)
    assert await identity.ensure_unique_index(server.db) is True

    await server.db["users"].insert_one({"user_id": "u_no_email_a"})
    await server.db["users"].insert_one({"user_id": "u_no_email_b"})

    assert await server.db["users"].count_documents({}) == 3
