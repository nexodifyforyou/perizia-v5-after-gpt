from __future__ import annotations

import base64
import os

import pytest
from motor.motor_asyncio import AsyncIOMotorClient

from pdf_retention import store
from pdf_retention.tests.loop import run


@pytest.fixture(autouse=True)
def isolated_retention(tmp_path, monkeypatch):
    monkeypatch.setenv("CORRECTNESS_V2_PDF_RETENTION_ENABLED", "true")
    monkeypatch.setenv("CORRECTNESS_V2_PDF_RETENTION_DAYS", "30")
    monkeypatch.setenv("CORRECTNESS_V2_PDF_RETENTION_ROOT", str(tmp_path / "private_retention"))
    monkeypatch.setenv("CORRECTNESS_V2_PDF_RETENTION_ACTIVE_KEY_VERSION", "v1")
    monkeypatch.setenv(
        "CORRECTNESS_V2_PDF_RETENTION_KEY_V1",
        base64.b64encode(os.urandom(32)).decode("ascii"),
    )
    monkeypatch.setenv(
        "CORRECTNESS_V2_PDF_RETENTION_DIAGNOSTIC_TMP_ROOT",
        str(tmp_path / "diagnostic_tmp"),
    )
    import server

    # Never bind server.client/server.db to this suite's persistent event loop:
    # later pytest-anyio tests intentionally use fresh loops. A disposable
    # client keeps this suite hermetic and server.db is restored by monkeypatch.
    retention_client = AsyncIOMotorClient(os.environ["MONGO_URL"])
    retention_db = retention_client[os.environ["DB_NAME"]]
    monkeypatch.setattr(server, "db", retention_db)
    store.set_database_override(retention_db)

    async def clean():
        for name in (store.CONSENTS_COLLECTION, store.RECORDS_COLLECTION, store.AUDIT_COLLECTION):
            await retention_db[name].delete_many({})
        await retention_db.perizia_analyses.delete_many({"user_id": {"$regex": "^ret_test_"}})
        await retention_db.users.delete_many({"user_id": {"$regex": "^ret_test_"}})

    run(clean())
    try:
        yield
    finally:
        run(clean())
        store.clear_database_override()
        retention_client.close()
