from __future__ import annotations

import hashlib
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

import server
from pdf_retention import api, blob_store, ingest, ops_access, store
from pdf_retention.tests.fixtures import isolated_retention  # noqa: F401
from pdf_retention.tests.loop import run


async def _retain(analysis_id: str, user_id: str, payload: bytes):
    digest = hashlib.sha256(payload).hexdigest()
    await store.database().perizia_analyses.insert_one({
        "analysis_id": analysis_id, "user_id": user_id, "input_sha256": digest,
        "result": {}, "created_at": store.now_iso(),
    })
    result = await ingest.retain_if_consented(
        analysis_id=analysis_id, user_id=user_id, contents=payload,
        input_sha256=digest, consent_requested=True,
    )
    assert result["retained"]
    return await store.database()[store.RECORDS_COLLECTION].find_one(
        {"analysis_id": analysis_id, "user_id": user_id}, {"_id": 0}
    )


def test_tenant_list_and_withdrawal_do_not_disclose_other_owner(monkeypatch):
    async def scenario():
        own = await _retain("analysis_ret_owner_a", "ret_test_owner_a", b"owner A")
        other = await _retain("analysis_ret_owner_b", "ret_test_owner_b", b"owner B")

        async def user_a(_request):
            return SimpleNamespace(user_id="ret_test_owner_a")

        monkeypatch.setattr(api, "_server_user", user_a)
        listed = await api.list_my_retained_pdfs(None)
        assert [row["analysis_id"] for row in listed["retained_pdfs"]] == ["analysis_ret_owner_a"]
        assert "retention_id" not in listed["retained_pdfs"][0]
        assert "encrypted_storage_id" not in listed["retained_pdfs"][0]

        with pytest.raises(HTTPException) as denied:
            await api.withdraw_pdf_retention("analysis_ret_owner_b", None)
        assert denied.value.status_code == 404
        assert blob_store.object_path(other["encrypted_storage_id"]).exists()

        erased = await api.withdraw_pdf_retention("analysis_ret_owner_a", None)
        assert erased == {"ok": True, "retained": False}
        assert not blob_store.object_path(own["encrypted_storage_id"]).exists()
        consent = await store.database()[store.CONSENTS_COLLECTION].find_one({
            "analysis_id": "analysis_ret_owner_a", "user_id": "ret_test_owner_a"
        })
        assert consent["status"] == "WITHDRAWN"
    run(scenario())


def test_no_customer_or_admin_raw_pdf_route_exists():
    routes = [(getattr(route, "path", ""), set(getattr(route, "methods", set()))) for route in server.app.routes]
    assert not any(
        (path.endswith("/retained-pdf") and "GET" in methods)
        or "/admin/pdf-retention" in path
        or "/download/" in path
        or "/files/" in path
        for path, methods in routes
    )
    assert not any("retention_id" in path or "storage_id" in path for path, _methods in routes)


def test_beta_user_cannot_invoke_owner_ops_action():
    async def scenario():
        with pytest.raises(ops_access.DiagnosticAccessDenied, match="OWNER_AUTHORIZATION_REQUIRED"):
            await ops_access.decrypt_for_owner_diagnostic(
                analysis_id="analysis_guessed",
                owner_user_id="ret_test_beta",
                owner_email="beta@example.invalid",
                reason="INC-100 diagnostic",
            )
    run(scenario())


def test_owner_ops_access_requires_context_reason_audits_and_cleans_temp():
    async def scenario():
        record = await _retain("analysis_ret_ops", "ret_test_customer_ops", b"owner diagnostic bytes")
        owner_email = server.CORRECTNESS_V2_ADMIN_VIEW_EMAIL
        owner_user_id = "ret_test_exact_owner"
        await store.database().users.insert_one({
            "user_id": owner_user_id, "email": owner_email, "name": "Owner"
        })
        with pytest.raises(ops_access.DiagnosticAccessDenied, match="REASON_REQUIRED"):
            await ops_access.decrypt_for_owner_diagnostic(
                analysis_id="analysis_ret_ops", owner_user_id=owner_user_id,
                owner_email=owner_email, reason="",
            )
        plaintext = await ops_access.decrypt_for_owner_diagnostic(
            analysis_id="analysis_ret_ops", owner_user_id=owner_user_id,
            owner_email=owner_email, reason="INC-101 reproduce parser defect",
        )
        assert plaintext == b"owner diagnostic bytes"
        materialized_path = None
        async with ops_access.materialize_for_owner_diagnostic(
            analysis_id="analysis_ret_ops", owner_user_id=owner_user_id,
            owner_email=owner_email, reason="INC-101 inspect locally",
        ) as path:
            materialized_path = path
            assert path.read_bytes() == plaintext
            assert path.stat().st_mode & 0o777 == 0o600
        assert materialized_path is not None and not materialized_path.exists()
        audit = await store.database()[store.AUDIT_COLLECTION].find_one({
            "retention_id": record["retention_id"],
            "event": "PDF_RETENTION_ACCESSED",
            "reason_detail": "INC-101 reproduce parser defect",
        })
        assert audit and audit["actor_user_id"] == owner_user_id
    run(scenario())


def test_existing_single_analysis_delete_cascades_before_metadata_delete(monkeypatch):
    async def scenario():
        record = await _retain("analysis_ret_cascade", "ret_test_cascade", b"cascade")

        async def auth(_request):
            return SimpleNamespace(user_id="ret_test_cascade")

        monkeypatch.setattr(server, "require_auth", auth)
        response = await server.delete_perizia_analysis("analysis_ret_cascade", None)
        assert response == {"ok": True, "message": "Analisi eliminata / Analysis deleted"}
        assert not blob_store.object_path(record["encrypted_storage_id"]).exists()
        assert await store.database().perizia_analyses.count_documents({
            "analysis_id": "analysis_ret_cascade", "user_id": "ret_test_cascade"
        }) == 0
        row = await store.database()[store.RECORDS_COLLECTION].find_one({
            "retention_id": record["retention_id"], "user_id": "ret_test_cascade"
        })
        assert row["deletion_state"] == store.STATE_DELETED
    run(scenario())


def test_analysis_delete_does_not_claim_success_when_ciphertext_delete_fails(monkeypatch):
    async def scenario():
        await _retain("analysis_ret_delete_fail", "ret_test_delete_fail", b"must remain tracked")

        async def auth(_request):
            return SimpleNamespace(user_id="ret_test_delete_fail")

        monkeypatch.setattr(server, "require_auth", auth)
        monkeypatch.setattr(
            blob_store,
            "delete_ciphertext",
            lambda _storage_id: (_ for _ in ()).throw(OSError("injected")),
        )
        with pytest.raises(HTTPException) as failed:
            await server.delete_perizia_analysis("analysis_ret_delete_fail", None)
        assert failed.value.status_code == 503
        assert await store.database().perizia_analyses.count_documents({
            "analysis_id": "analysis_ret_delete_fail", "user_id": "ret_test_delete_fail"
        }) == 1
        row = await store.database()[store.RECORDS_COLLECTION].find_one({
            "analysis_id": "analysis_ret_delete_fail", "user_id": "ret_test_delete_fail"
        })
        assert row["deletion_state"] == store.STATE_DELETE_PENDING
    run(scenario())


def test_analysis_delete_surfaces_internal_erasure_fault_as_503_not_500(monkeypatch):
    """A fault in the retention lookup itself (not a ciphertext-delete
    failure) must still degrade to the same safe, retryable 503 -- never an
    unhandled 500 -- so an unrelated internal error in this new subsystem can
    never crash the pre-existing delete-analysis endpoint."""
    async def scenario():
        await _retain("analysis_ret_lookup_500", "ret_test_lookup_500", b"lookup 500")

        async def auth(_request):
            return SimpleNamespace(user_id="ret_test_lookup_500")

        monkeypatch.setattr(server, "require_auth", auth)
        collection_cls = type(store.database()[store.RECORDS_COLLECTION])
        real_find_one = collection_cls.find_one

        async def _boom_only_for_records(self, *args, **kwargs):
            if getattr(self, "name", None) == store.RECORDS_COLLECTION:
                raise RuntimeError("injected transient lookup fault")
            return await real_find_one(self, *args, **kwargs)

        monkeypatch.setattr(collection_cls, "find_one", _boom_only_for_records)
        with pytest.raises(HTTPException) as failed:
            await server.delete_perizia_analysis("analysis_ret_lookup_500", None)
        assert failed.value.status_code == 503
        assert await store.database().perizia_analyses.count_documents({
            "analysis_id": "analysis_ret_lookup_500", "user_id": "ret_test_lookup_500"
        }) == 1
    run(scenario())


def test_flag_off_metadata_listing_is_not_reachable(monkeypatch):
    async def scenario():
        monkeypatch.setenv("CORRECTNESS_V2_PDF_RETENTION_ENABLED", "false")
        with pytest.raises(HTTPException) as disabled:
            await api.list_my_retained_pdfs(None)
        assert disabled.value.status_code == 404
        assert disabled.value.detail == "PDF_RETENTION_DISABLED"
    run(scenario())


def test_existing_delete_all_removes_only_authenticated_users_pdfs(monkeypatch):
    async def scenario():
        own1 = await _retain("analysis_ret_bulk_1", "ret_test_bulk", b"bulk 1")
        own2 = await _retain("analysis_ret_bulk_2", "ret_test_bulk", b"bulk 2")
        other = await _retain("analysis_ret_bulk_other", "ret_test_bulk_other", b"other")

        async def auth(_request):
            return SimpleNamespace(user_id="ret_test_bulk")

        monkeypatch.setattr(server, "require_auth", auth)
        response = await server.delete_all_history(None)
        assert response["ok"] is True
        assert response["deleted"]["perizia"] == 2
        assert not blob_store.object_path(own1["encrypted_storage_id"]).exists()
        assert not blob_store.object_path(own2["encrypted_storage_id"]).exists()
        assert blob_store.object_path(other["encrypted_storage_id"]).exists()
        assert await store.database().perizia_analyses.count_documents({
            "analysis_id": "analysis_ret_bulk_other", "user_id": "ret_test_bulk_other"
        }) == 1
    run(scenario())
