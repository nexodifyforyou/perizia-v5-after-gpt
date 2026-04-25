import os
import sys
from datetime import datetime, timezone

import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import server as server
from test_admin import FakeDB


@pytest.fixture()
def anyio_backend():
    return "asyncio"


@pytest.fixture()
def fake_db(monkeypatch):
    fake = FakeDB()
    monkeypatch.setattr(server, "db", fake)
    return fake


def _test_user() -> server.User:
    return server.User(user_id="user_test", email="user@test.local", name="Test User")


@pytest.mark.anyio
async def test_perizia_history_list_is_lightweight_and_never_refreshes(fake_db, monkeypatch):
    async def fake_require_auth(_request):
        return _test_user()

    def forbidden_refresh(*_args, **_kwargs):
        raise AssertionError("history list must not refresh customer-facing result")

    def forbidden_page_load(*_args, **_kwargs):
        raise AssertionError("history list must not load page artifacts")

    monkeypatch.setattr(server, "require_auth", fake_require_auth)
    monkeypatch.setattr(server, "_refresh_customer_facing_result_on_read", forbidden_refresh)
    monkeypatch.setattr(server, "_load_pages_for_analysis", forbidden_page_load)

    fake_db.perizia_analyses.items.append(
        {
            "analysis_id": "analysis_lightweight",
            "user_id": "user_test",
            "case_id": "case_123",
            "case_title": "perizia.pdf",
            "file_name": "perizia.pdf",
            "created_at": datetime(2026, 4, 25, tzinfo=timezone.utc),
            "status": "COMPLETED",
            "pages_count": 81,
            "semaforo_status": "red",
            "raw_text": "large raw text",
            "result": {
                "section_1_semaforo_generale": {"status": "RED", "reason_it": "heavy"},
                "field_states": {"large": {"value": "must not be returned"}},
                "debug": {"large": True},
            },
        }
    )

    response = await server.get_perizia_history(object(), limit=100, skip=-5)

    assert response["limit"] == 50
    assert response["skip"] == 0
    assert response["total"] == 1
    assert response["analyses"] == [
        {
            "analysis_id": "analysis_lightweight",
            "case_id": "case_123",
            "case_title": "perizia.pdf",
            "file_name": "perizia.pdf",
            "created_at": datetime(2026, 4, 25, tzinfo=timezone.utc),
            "status": "COMPLETED",
            "pages_count": 81,
            "semaforo_status": "RED",
            "result": {"section_1_semaforo_generale": {"status": "RED"}},
        }
    ]


@pytest.mark.anyio
async def test_perizia_detail_skips_refresh_for_persisted_customer_contract(fake_db, monkeypatch):
    def forbidden_refresh(*_args, **_kwargs):
        raise AssertionError("persisted customer contract detail read must not refresh")

    def forbidden_page_load(*_args, **_kwargs):
        raise AssertionError("persisted customer contract detail read must not load page artifacts")

    monkeypatch.setattr(server, "_refresh_customer_facing_result_on_read", forbidden_refresh)
    monkeypatch.setattr(server, "_load_pages_for_analysis", forbidden_page_load)

    persisted_result = {
        "customer_decision_contract": {"version": "customer_decision_contract_v1"},
        "issues": [],
        "summary_for_client_bundle": {"semaforo_status": "GREEN"},
        "section_1_semaforo_generale": {"status": "GREEN"},
        "section_3_money_box": {"items": []},
        "section_9_legal_killers": {"items": []},
        "section_11_red_flags": [],
        "field_states": {"superficie_catastale": {"value": "100 mq"}},
    }
    fake_db.perizia_analyses.items.append(
        {
            "analysis_id": "analysis_persisted",
            "user_id": "user_test",
            "case_id": "case_456",
            "case_title": "persisted.pdf",
            "file_name": "persisted.pdf",
            "created_at": datetime(2026, 4, 25, tzinfo=timezone.utc),
            "status": "COMPLETED",
            "pages_count": 50,
            "raw_text": "large raw text",
            "result": persisted_result,
        }
    )

    response = await server._get_perizia_analysis_for_user("analysis_persisted", _test_user())

    assert response["analysis_id"] == "analysis_persisted"
    assert response["result"] == persisted_result
    assert response["result"]["field_states"] == {"superficie_catastale": {"value": "100 mq"}}
