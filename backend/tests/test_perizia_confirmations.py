import io
import os
import sys
import zipfile
from datetime import datetime, timezone

import httpx
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import server as server
from test_admin import FakeDB, _seed_session


@pytest.fixture()
def fake_db(monkeypatch):
    fake_db = FakeDB()
    monkeypatch.setattr(server, "db", fake_db)
    server.MASTER_ADMIN_EMAIL = "admin@example.com"
    return fake_db


def _user(email: str = "user@example.com", *, is_master_admin: bool = False) -> server.User:
    return server.User(
        user_id="user_1" if not is_master_admin else "admin_1",
        email=email,
        name="User" if not is_master_admin else "Admin",
        plan="pro",
        is_master_admin=is_master_admin,
        quota={"perizia_scans_remaining": 10, "image_scans_remaining": 0, "assistant_messages_remaining": 0},
    )


def _seed_analysis(fake_db, *, analysis_id: str, user_id: str, result: dict, headline_overrides=None, field_overrides=None):
    fake_db.perizia_analyses.items.append(
        {
            "analysis_id": analysis_id,
            "user_id": user_id,
            "case_id": "case_1",
            "run_id": "run_1",
            "case_title": "perizia.pdf",
            "file_name": "perizia.pdf",
            "pages_count": 2,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "result": result,
            "headline_overrides": headline_overrides or {},
            "field_overrides": field_overrides or {},
        }
    )


def _build_result_for_pages(pages):
    result = server.create_fallback_analysis("perizia.pdf", "case_1", "run_1", pages, server._build_full_text_from_pages(pages))
    server._normalize_legal_killers(result, pages)
    server._apply_headline_field_states(result, pages)
    server._apply_decision_field_states(result, pages)
    server._apply_market_ranges_to_money_box(result)
    server._normalize_evidence_offsets(result, pages)
    result["panoramica_contract"] = server._build_panoramica_contract(result, pages)
    return result


def test_occupazione_uncertain_case_produces_confirmation_metadata():
    pages = [
        {"page_number": 4, "text": "STATO OCCUPATIVO\nL'immobile risulta libero e nella disponibilità della procedura.\n"},
        {"page_number": 5, "text": "STATO OCCUPATIVO\nL'immobile risulta occupato dal debitore alla data del sopralluogo.\n"},
    ]
    state = server._extract_stato_occupativo_state(pages)
    assert state["status"] == "LOW_CONFIDENCE"
    assert state["review_required"] is True
    assert state["needs_user_confirmation"] is True
    assert len(state["top_candidates"]) == 2
    assert {state["top_candidates"][0]["value"], state["top_candidates"][1]["value"]} == {"LIBERO", "OCCUPATO DAL DEBITORE"}


def test_later_authoritative_libero_beats_earlier_weak_occupato():
    pages = [
        {"page_number": 1, "text": "RIEPILOGO: stato occupativo: occupato | prezzo base: € 100.000\n"},
        {"page_number": 7, "text": "STATO OCCUPATIVO\nL'immobile risulta libero e nella disponibilità della procedura.\n"},
    ]
    state = server._extract_stato_occupativo_state(pages)
    assert state["status"] == "FOUND"
    assert state["value"] == "LIBERO"
    assert state["chosen_candidate"]["page"] == 7


def test_later_authoritative_terzi_senza_titolo_beats_earlier_libero():
    pages = [
        {"page_number": 2, "text": "Nota storica: immobile libero al precedente accesso del custode.\n"},
        {"page_number": 8, "text": "STATO OCCUPATIVO\nL'immobile risulta occupato da terzi senza titolo opponibile.\n"},
    ]
    state = server._extract_stato_occupativo_state(pages)
    assert state["status"] == "FOUND"
    assert state["value"] == "OCCUPATO DA TERZI SENZA TITOLO"
    assert state["chosen_candidate"]["page"] == 8


def test_strong_unresolved_conflict_yields_review_required_output():
    pages = [
        {"page_number": 9, "text": "STATO OCCUPATIVO\nL'immobile risulta libero.\n"},
        {"page_number": 10, "text": "STATO OCCUPATIVO\nL'immobile risulta occupato dal debitore alla data del sopralluogo.\n"},
    ]
    state = server._extract_stato_occupativo_state(pages)
    assert state["value"] == "DA VERIFICARE"
    assert state["status"] == "LOW_CONFIDENCE"
    assert state["conflicts"]


def test_noisy_table_like_mention_is_downweighted_against_narrative():
    pages = [
        {"page_number": 1, "text": "Tabella riepilogo | occupato | € 120.000 | mq 90\n"},
        {"page_number": 3, "text": "STATO OCCUPATIVO\nDalla perizia emerge che il bene risulta libero.\n"},
    ]
    state = server._extract_stato_occupativo_state(pages)
    assert state["value"] == "LIBERO"
    assert state["chosen_candidate"]["is_table_like"] is False


def test_later_authoritative_urbanistica_compliant_beats_earlier_weak_negative():
    pages = [
        {"page_number": 1, "text": "Tabella costi | oneri di regolarizzazione urbanistica | sanatoria € 5.000 | valore finale € 120.000\n"},
        {"page_number": 7, "text": "REGOLARITA URBANISTICA\nNon risultano abusi edilizi e il bene risulta conforme urbanisticamente.\n"},
    ]
    state = server._extract_regolarita_urbanistica_state(pages)
    assert state["status"] == "FOUND"
    assert state["value"] == "NON EMERGONO ABUSI"
    assert state["chosen_candidate"]["page"] == 7


def test_later_authoritative_urbanistica_non_compliant_beats_earlier_weak_positive():
    pages = [
        {"page_number": 1, "text": "Nota storica: in passato non emergevano abusi edilizi sul bene.\n"},
        {"page_number": 8, "text": "ABUSI EDILIZI E CONFORMITA URBANISTICA\nSi rilevano difformità urbanistiche e opere abusive da sanare.\n"},
    ]
    state = server._extract_regolarita_urbanistica_state(pages)
    assert state["status"] == "FOUND"
    assert state["value"] == "PRESENTI DIFFORMITÀ"
    assert state["chosen_candidate"]["page"] == 8


def test_urbanistica_strong_unresolved_conflict_yields_review_required_output():
    pages = [
        {"page_number": 4, "text": "REGOLARITA URBANISTICA\nNon risultano abusi edilizi e il bene appare conforme urbanisticamente.\n"},
        {"page_number": 5, "text": "CONFORMITA URBANISTICA\nSi rilevano difformità urbanistiche e opere abusive da sanare.\n"},
    ]
    state = server._extract_regolarita_urbanistica_state(pages)
    assert state["value"] == "DA VERIFICARE"
    assert state["status"] == "LOW_CONFIDENCE"
    assert state["review_required"] is True
    assert state["needs_user_confirmation"] is True
    assert state["conflicts"]


def test_urbanistica_table_like_mentions_are_downweighted_against_narrative():
    pages = [
        {"page_number": 1, "text": "ONERI DI REGOLARIZZAZIONE URBANISTICA | condono € 7.000 | valore finale € 120.000\n"},
        {"page_number": 3, "text": "ABUSI EDILIZI E CONFORMITA URBANISTICA\nNon risultano difformità urbanistiche né abusi edilizi.\n"},
    ]
    state = server._extract_regolarita_urbanistica_state(pages)
    assert state["value"] == "NON EMERGONO ABUSI"
    assert state["chosen_candidate"]["is_table_like"] is False


def test_urbanistica_uncertain_case_exposes_top_two_candidates():
    pages = [
        {"page_number": 6, "text": "REGOLARITA URBANISTICA\nNon risultano abusi edilizi e il bene risulta conforme urbanisticamente.\n"},
        {"page_number": 7, "text": "ABUSI EDILIZI\nSono presenti difformità urbanistiche e irregolarità da sanare.\n"},
    ]
    state = server._extract_regolarita_urbanistica_state(pages)
    assert state["review_required"] is True
    assert len(state["top_candidates"]) == 2
    assert {state["top_candidates"][0]["value"], state["top_candidates"][1]["value"]} == {"NON EMERGONO ABUSI", "PRESENTI DIFFORMITÀ"}


@pytest.mark.anyio
async def test_occupazione_user_confirmation_is_stored_and_applied(fake_db, monkeypatch):
    pages = [
        {"page_number": 4, "text": "STATO OCCUPATIVO\nL'immobile risulta libero e nella disponibilità della procedura.\n"},
        {"page_number": 5, "text": "STATO OCCUPATIVO\nL'immobile risulta occupato dal debitore alla data del sopralluogo.\n"},
    ]
    result = _build_result_for_pages(pages)
    _seed_analysis(fake_db, analysis_id="analysis_occ", user_id="user_1", result=result)

    async def fake_require_auth(_request):
        return _user()

    monkeypatch.setattr(server, "require_auth", fake_require_auth)
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/analysis/perizia/analysis_occ/confirmations",
            json={"check_type": "stato_occupativo", "value": "LIBERO", "notes": "Confermato da utente"},
        )
        assert resp.status_code == 200
        assert len(fake_db.perizia_confirmations.items) == 1
        record = fake_db.perizia_confirmations.items[0]
        assert record["check_type"] == "stato_occupativo"
        assert record["user_confirmed_value"] == "LIBERO"
        assert record["candidate_1_value"]
        detail = await client.get("/api/analysis/perizia/analysis_occ")
        assert detail.status_code == 200
        field_state = detail.json()["result"]["field_states"]["stato_occupativo"]
        assert field_state["status"] == "USER_PROVIDED"
        assert field_state["value"] == "LIBERO"


@pytest.mark.anyio
async def test_urbanistica_user_confirmation_is_stored_and_applied_without_breaking_consumers(fake_db, monkeypatch):
    pages = [
        {"page_number": 4, "text": "REGOLARITA URBANISTICA\nNon risultano abusi edilizi e il bene appare conforme urbanisticamente.\n"},
        {"page_number": 5, "text": "CONFORMITA URBANISTICA\nSi rilevano difformità urbanistiche e opere abusive da sanare.\n"},
    ]
    result = _build_result_for_pages(pages)
    _seed_analysis(fake_db, analysis_id="analysis_urb", user_id="user_1", result=result)

    async def fake_require_auth(_request):
        return _user()

    monkeypatch.setattr(server, "require_auth", fake_require_auth)
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/analysis/perizia/analysis_urb/confirmations",
            json={"check_type": "regolarita_urbanistica", "value": "NON EMERGONO ABUSI", "notes": "Confermato da tecnico"},
        )
        assert resp.status_code == 200
        assert len(fake_db.perizia_confirmations.items) == 1
        record = fake_db.perizia_confirmations.items[0]
        assert record["check_type"] == "regolarita_urbanistica"
        assert record["field_key"] == "field_states.regolarita_urbanistica"
        assert record["user_confirmed_value"] == "NON EMERGONO ABUSI"
        assert record["notes"] == "Confermato da tecnico"
        assert record["candidate_1_value"]
        detail = await client.get("/api/analysis/perizia/analysis_urb")
        assert detail.status_code == 200
        payload = detail.json()["result"]
        field_state = payload["field_states"]["regolarita_urbanistica"]
        assert field_state["status"] == "USER_PROVIDED"
        assert field_state["value"] == "NON EMERGONO ABUSI"
        assert payload["abusi_edilizi_conformita"]["conformita_urbanistica"]["status"] == "CONFORME"
        assert payload["abusi_edilizi_conformita"]["conformita_urbanistica"]["detail_it"] == "NON EMERGONO ABUSI"


@pytest.mark.anyio
async def test_existing_address_confirmation_is_logged(fake_db, monkeypatch):
    pages = [{"page_number": 1, "text": "TRIBUNALE DI ROMA\nUbicazione Via Roma 10, Roma\n"}]
    result = _build_result_for_pages(pages)
    _seed_analysis(fake_db, analysis_id="analysis_addr", user_id="user_1", result=result)

    async def fake_require_auth(_request):
        return _user()

    monkeypatch.setattr(server, "require_auth", fake_require_auth)
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.patch(
            "/api/analysis/perizia/analysis_addr/headline",
            json={"address": "Via Roma 10, Roma"},
        )
        assert resp.status_code == 200
        assert len(fake_db.perizia_confirmations.items) == 1
        record = fake_db.perizia_confirmations.items[0]
        assert record["check_type"] == "address"
        assert record["field_key"] == "field_states.address"


@pytest.mark.anyio
async def test_address_confirmation_endpoint_preserves_notes_and_export_includes_them(fake_db, monkeypatch):
    pages = [{"page_number": 1, "text": "TRIBUNALE DI ROMA\nUbicazione Via Roma 10, Roma\n"}]
    result = _build_result_for_pages(pages)
    _seed_analysis(fake_db, analysis_id="analysis_addr_confirm", user_id="user_1", result=result)
    fake_db.users.items.append(
        {
            "user_id": "user_admin",
            "email": "admin@example.com",
            "name": "Admin",
            "plan": "enterprise",
            "is_master_admin": True,
            "quota": {},
        }
    )
    admin_session = _seed_session(
        fake_db,
        {
            "user_id": "user_admin",
            "email": "admin@example.com",
            "name": "Admin",
            "plan": "enterprise",
            "is_master_admin": True,
            "quota": {},
        },
        session_token="sess_admin_export",
    )

    original_require_auth = server.require_auth

    async def fake_require_auth(_request):
        return _user()

    monkeypatch.setattr(server, "require_auth", fake_require_auth)
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        confirm_resp = await client.post(
            "/api/analysis/perizia/analysis_addr_confirm/confirmations",
            json={"check_type": "address", "value": "Via Roma 10, Roma", "notes": "indirizzo verificato manualmente"},
        )
        assert confirm_resp.status_code == 200
        assert len(fake_db.perizia_confirmations.items) == 1
        record = fake_db.perizia_confirmations.items[0]
        assert record["check_type"] == "address"
        assert record["notes"] == "indirizzo verificato manualmente"

        monkeypatch.setattr(server, "require_auth", original_require_auth)
        export_resp = await client.get(
            "/api/admin/perizia-confirmations/export.xlsx",
            headers={"Authorization": f"Bearer {admin_session}"},
        )
        assert export_resp.status_code == 200
        zf = zipfile.ZipFile(io.BytesIO(export_resp.content))
        sheet_xml = zf.read("xl/worksheets/sheet1.xml").decode("utf-8")
        assert "indirizzo verificato manualmente" in sheet_xml


@pytest.mark.anyio
async def test_admin_export_includes_urbanistica_confirmation_rows(fake_db, monkeypatch):
    pages = [
        {"page_number": 4, "text": "REGOLARITA URBANISTICA\nNon risultano abusi edilizi e il bene appare conforme urbanisticamente.\n"},
        {"page_number": 5, "text": "CONFORMITA URBANISTICA\nSi rilevano difformità urbanistiche e opere abusive da sanare.\n"},
    ]
    result = _build_result_for_pages(pages)
    _seed_analysis(fake_db, analysis_id="analysis_urb_export", user_id="user_1", result=result)
    admin_session = _seed_session(
        fake_db,
        {
            "user_id": "user_admin",
            "email": "admin@example.com",
            "name": "Admin",
            "plan": "enterprise",
            "is_master_admin": True,
            "quota": {},
        },
        session_token="sess_admin_urbanistica_export",
    )

    original_require_auth = server.require_auth

    async def fake_require_auth(_request):
        return _user()

    monkeypatch.setattr(server, "require_auth", fake_require_auth)
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        confirm_resp = await client.post(
            "/api/analysis/perizia/analysis_urb_export/confirmations",
            json={"check_type": "regolarita_urbanistica", "value": "PRESENTI DIFFORMITÀ", "notes": "Difformita confermate"},
        )
        assert confirm_resp.status_code == 200

        monkeypatch.setattr(server, "require_auth", original_require_auth)
        export_resp = await client.get(
            "/api/admin/perizia-confirmations/export.xlsx",
            headers={"Authorization": f"Bearer {admin_session}"},
        )
        assert export_resp.status_code == 200
        zf = zipfile.ZipFile(io.BytesIO(export_resp.content))
        sheet_xml = zf.read("xl/worksheets/sheet1.xml").decode("utf-8")
        assert "regolarita_urbanistica" in sheet_xml
        assert "PRESENTI DIFFORMITÀ" in sheet_xml
        assert "Difformita confermate" in sheet_xml


@pytest.mark.anyio
async def test_admin_export_generates_xlsx_with_expected_rows_and_columns(fake_db):
    pages = [{"page_number": 1, "text": "TRIBUNALE DI ROMA\nUbicazione Via Roma 10, Roma\nSTATO OCCUPATIVO\nL'immobile risulta libero.\n"}]
    result = _build_result_for_pages(pages)
    _seed_analysis(
        fake_db,
        analysis_id="analysis_export",
        user_id="user_1",
        result=result,
        headline_overrides={"address": "Via Roma 10, Roma"},
        field_overrides={"stato_occupativo": "LIBERO"},
    )
    fake_db.users.items.append({"user_id": "user_1", "email": "user@example.com"})
    session_token = _seed_session(
        fake_db,
        {
            "user_id": "user_admin",
            "email": "admin@example.com",
            "name": "Admin",
            "plan": "enterprise",
            "is_master_admin": True,
            "quota": {},
        },
        session_token="sess_export_ok",
    )
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/api/admin/perizia-confirmations/export.xlsx",
            headers={"Authorization": f"Bearer {session_token}"},
        )
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        sheet_xml = zf.read("xl/worksheets/sheet1.xml").decode("utf-8")
        assert "confirmation_id" in sheet_xml
        assert "check_type" in sheet_xml
        assert "Via Roma 10, Roma" in sheet_xml
        assert "stato_occupativo" in sheet_xml


@pytest.mark.anyio
async def test_admin_export_allows_configured_master_admin_email(fake_db):
    session_token = _seed_session(
        fake_db,
        {
            "user_id": "user_admin",
            "email": "admin@example.com",
            "name": "Admin",
            "plan": "enterprise",
            "is_master_admin": True,
            "quota": {},
        },
        session_token="sess_export_configured_admin",
    )
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/api/admin/perizia-confirmations/export.xlsx",
            headers={"Authorization": f"Bearer {session_token}"},
        )
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_admin_export_forbidden_for_non_master_admin(fake_db):
    session_token = _seed_session(
        fake_db,
        {
            "user_id": "user_2",
            "email": "user@example.com",
            "name": "User",
            "plan": "pro",
            "is_master_admin": False,
            "quota": {},
        },
        session_token="sess_export_forbidden",
    )
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/api/admin/perizia-confirmations/export.xlsx",
            headers={"Authorization": f"Bearer {session_token}"},
        )
    assert resp.status_code == 403
