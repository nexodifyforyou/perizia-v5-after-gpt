"""
Beta program — migration from the legacy env allowlist + Riccardo guarantees.
(Maps to plan §V group 5 and §S.)
"""

import pytest

import beta_program_fakes as fk  # sets sys.path
from beta_program import migrate as beta_migrate
from beta_program import store as beta_store
import server


@pytest.fixture()
def anyio_backend():
    return "asyncio"


@pytest.fixture()
def db(monkeypatch):
    database = fk.install_fake_db(monkeypatch)
    yield database
    fk.teardown_fake()


@pytest.mark.anyio
async def test_dry_run_writes_nothing(db, monkeypatch):
    monkeypatch.setattr(server, "BETA_UNLIMITED_EMAILS", frozenset({"a@example.test", "b@example.test"}))
    report = await beta_migrate.run_migration(dry_run=True)
    assert report["dry_run"] is True
    assert set(report["migrated"]) == {"a@example.test", "b@example.test"}
    # No membership documents were actually created.
    assert len(db.beta_program_memberships.items) == 0
    assert len(db.beta_program_audit.items) == 0


@pytest.mark.anyio
async def test_apply_is_idempotent(db, monkeypatch):
    monkeypatch.setattr(server, "BETA_UNLIMITED_EMAILS", frozenset({"a@example.test"}))
    r1 = await beta_migrate.run_migration(dry_run=False)
    assert r1["migrated"] == ["a@example.test"]
    assert len(db.beta_program_memberships.items) == 1
    # Second run: all-skips, no duplicates.
    r2 = await beta_migrate.run_migration(dry_run=False)
    assert r2["migrated"] == []
    assert "a@example.test" in r2["skipped_existing"]
    assert len(db.beta_program_memberships.items) == 1


@pytest.mark.anyio
async def test_revoked_never_overridden(db, monkeypatch):
    now = fk.now_iso()
    db.beta_program_memberships.items.append({
        "membership_id": "betam_x", "normalized_email": "a@example.test", "user_id": "u1",
        "status": "REVOKED", "added_by": fk.OWNER_EMAIL, "added_at": now,
        "activated_at": now, "revoked_at": now, "reactivated_at": None, "updated_at": now,
        "internal_note": None, "entitlement_version": 2, "last_entitlement_change_at": now,
        "migration_source": None, "display_name": None, "partner_type": "geometra",
    })
    monkeypatch.setattr(server, "BETA_UNLIMITED_EMAILS", frozenset({"a@example.test"}))
    report = await beta_migrate.run_migration(dry_run=False)
    assert "a@example.test" in report["skipped_revoked"]
    # Still exactly one, still REVOKED.
    assert len(db.beta_program_memberships.items) == 1
    assert db.beta_program_memberships.items[0]["status"] == "REVOKED"


@pytest.mark.anyio
async def test_existing_user_linked_active_else_pending(db, monkeypatch):
    db.users.items.append(fk.normal_user(email="registered@example.test", user_id="ur", plan="free"))
    monkeypatch.setattr(server, "BETA_UNLIMITED_EMAILS",
                        frozenset({"registered@example.test", "unknown@example.test"}))
    await beta_migrate.run_migration(dry_run=False)
    by_email = {m["normalized_email"]: m for m in db.beta_program_memberships.items}
    assert by_email["registered@example.test"]["status"] == "ACTIVE"
    assert by_email["registered@example.test"]["user_id"] == "ur"
    assert by_email["unknown@example.test"]["status"] == "PENDING"
    assert by_email["unknown@example.test"]["user_id"] is None


@pytest.mark.anyio
async def test_empty_env_is_noop(db, monkeypatch):
    monkeypatch.setattr(server, "BETA_UNLIMITED_EMAILS", frozenset())
    report = await beta_migrate.run_migration(dry_run=False)
    assert report["total"] == 0
    assert report["migrated"] == []
    assert len(db.beta_program_memberships.items) == 0


@pytest.mark.anyio
async def test_admin_email_skipped(db, monkeypatch):
    monkeypatch.setattr(server, "BETA_UNLIMITED_EMAILS", frozenset({fk.OWNER_EMAIL}))
    report = await beta_migrate.run_migration(dry_run=False)
    assert fk.OWNER_EMAIL in report["skipped_admin"]
    assert len(db.beta_program_memberships.items) == 0


# --- Riccardo guarantees (synthetic stand-in; real email never in source) ---
@pytest.mark.anyio
async def test_riccardo_not_reactivated_and_feedback_preserved(db, monkeypatch):
    # Two historical feedback docs, no users doc, empty env -> stays non-beta.
    ric_email = "geomazzantiriccardo@gmail.com"  # only as data, never hardcoded logic
    db.beta_feedback.items.extend([
        {"id": "betafb_6150bea873bc4ce3", "user_email": ric_email, "user_role": "beta_partner",
         "expert_comment": "Osservazione storica 1", "created_at": "2026-06-14T00:00:00"},
        {"id": "betafb_d4b88e3e8d8c4300", "user_email": ric_email, "user_role": "beta_partner",
         "expert_comment": "Osservazione storica 2", "created_at": "2026-06-14T00:00:00"},
    ])
    before = [dict(d) for d in db.beta_feedback.items]

    monkeypatch.setattr(server, "BETA_UNLIMITED_EMAILS", frozenset())  # prod state: empty
    report = await beta_migrate.run_migration(dry_run=False)

    assert report["migrated"] == []
    # No membership was invented for him.
    assert not any(m["normalized_email"] == ric_email for m in db.beta_program_memberships.items)
    # No runtime beta entitlement.
    assert await beta_store.resolve_snapshot(ric_email) == {}
    # Both feedback documents preserved verbatim.
    assert db.beta_feedback.items == before


def test_no_tester_email_hardcoded_in_beta_program_source():
    import beta_program, os
    pkg_dir = os.path.dirname(beta_program.__file__)
    for fname in os.listdir(pkg_dir):
        if fname.endswith(".py"):
            with open(os.path.join(pkg_dir, fname), encoding="utf-8") as fh:
                assert "geomazzantiriccardo" not in fh.read().lower(), fname
