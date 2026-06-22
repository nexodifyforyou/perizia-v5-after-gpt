import copy
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


def _persisted_lot_result(*, mode: str, lots_count: int) -> dict:
    is_multi = mode == "multi_lot"
    lots = [{"lot_number": idx, "lot_id": str(idx), "beni": [{"bene_id": f"bene_{idx}"}]} for idx in range(1, lots_count + 1)]
    label = "Lotto Unico" if not is_multi else "Lotti " + ", ".join(str(idx) for idx in range(1, lots_count + 1))
    lot_index = [{"lot": idx, "ubicazione": f"Lotto {idx}"} for idx in range(1, lots_count + 1)]
    lot_contract = {
        "version": "customer_decision_contract_v1",
        "lots": copy.deepcopy(lots),
        "lots_count": lots_count,
        "lot_count": lots_count,
        "is_multi_lot": is_multi,
        "case_header": {"lotto": label},
        "report_header": {"lotto": {"value": label}, "is_multi_lot": is_multi},
        "lot_index": copy.deepcopy(lot_index),
    }
    return {
        "customer_decision_contract": lot_contract,
        "issues": [],
        "summary_for_client_bundle": {"semaforo_status": "GREEN"},
        "section_1_semaforo_generale": {"status": "GREEN"},
        "section_3_money_box": {"items": [{"label": "unchanged"}]},
        "section_9_legal_killers": {"items": [{"title_it": "unchanged"}]},
        "section_11_red_flags": [],
        "field_states": {"stato_occupativo": {"status": "UNKNOWN"}},
        "money_box": {"items": [{"label": "unchanged"}]},
        "summary_for_client": {"summary_it": "unchanged"},
        "decision_rapida_client": {"headline_it": "unchanged"},
        "lots": lots,
        "lots_count": lots_count,
        "lot_count": lots_count,
        "is_multi_lot": is_multi,
        "case_header": {"lotto": label},
        "report_header": {"lotto": {"value": label}, "is_multi_lot": is_multi},
        "lot_index": lot_index,
    }


def _authority_lot_shadow(mode: str, *, numbers=None, confidence: float = 0.92, rule: str = "") -> dict:
    numbers = list(numbers or [])
    if not rule:
        rule = (
            "high_authority_lotto_unico_beats_toc_context_and_generic_lot_mentions"
            if mode == "single_lot"
            else "high_authority_multilot_beats_toc_context_and_generic_lot_mentions"
        )
    return {
        "schema_version": "perizia_authority_resolvers_v1",
        "status": "OK",
        "fail_open": False,
        "warnings": [],
        "lot_structure": {
            "domain": "lot_structure",
            "status": "OK",
            "value": {
                "shadow_lot_mode": mode,
                "detected_lot_numbers": numbers,
                "has_high_authority_lotto_unico": mode == "single_lot",
                "has_high_authority_multilot": mode == "multi_lot",
            },
            "confidence": confidence,
            "winning_evidence": [{"page": 2, "quote": "LOTTO UNICO" if mode == "single_lot" else "LOTTO 1"}],
            "rejected_conflicts": [],
            "authority_basis": {
                "zones_used": ["FINAL_LOT_FORMATION"],
                "authority_levels_used": ["HIGH_FACTUAL"],
                "pages_used": [2],
                "rules_triggered": [rule],
            },
            "fail_open": False,
            "notes": [],
        },
    }


def _collect_forbidden_customer_keys(value, path="response"):
    forbidden = {
        "authority_lot_projection",
        "authority_shadow_resolvers",
        "authority_shadow",
        "shadow_authority",
        "authority_resolver",
        "section_zone",
        "authority_score",
        "authority_level",
        "domain_hints",
        "answer_point",
        "reason_for_authority",
        "is_instruction_like",
        "is_answer_like",
        "source_stage",
        "extractor_version",
        "debug",
        "internal_runtime",
    }
    hits = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}"
            if str(key) in forbidden or str(key).startswith("authority_") or "shadow_" in str(key):
                hits.append(child_path)
            hits.extend(_collect_forbidden_customer_keys(child, child_path))
    elif isinstance(value, list):
        for idx, item in enumerate(value):
            hits.extend(_collect_forbidden_customer_keys(item, f"{path}[{idx}]"))
    return hits


def _collect_forbidden_customer_strings(value, path="response"):
    hits = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}"
            if isinstance(child, str) and "AUTH_" in child:
                hits.append(child_path)
            hits.extend(_collect_forbidden_customer_strings(child, child_path))
    elif isinstance(value, list):
        for idx, item in enumerate(value):
            item_path = f"{path}[{idx}]"
            if isinstance(item, str) and "AUTH_" in item:
                hits.append(item_path)
            hits.extend(_collect_forbidden_customer_strings(item, item_path))
    elif isinstance(value, str) and "AUTH_" in value:
        hits.append(path)
    return hits


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
    monkeypatch.delenv(server.AUTHORITY_LOT_PROJECTION_FLAG, raising=False)

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


@pytest.mark.anyio
async def test_persisted_detail_authority_lot_projection_is_response_only_for_saved_casa(fake_db, monkeypatch):
    monkeypatch.setenv(server.AUTHORITY_LOT_PROJECTION_FLAG, "1")
    monkeypatch.setattr(
        server,
        "_load_authority_shadow_for_detail_read",
        lambda _analysis_id, _analysis: _authority_lot_shadow("single_lot"),
    )

    persisted_result = _persisted_lot_result(mode="multi_lot", lots_count=2)
    stored = {
        "analysis_id": "analysis_6b3ab6865dca",
        "user_id": "user_test",
        "case_id": "case_casa",
        "case_title": "Casa ai Venti.pdf",
        "file_name": "Casa ai Venti.pdf",
        "created_at": datetime(2026, 4, 25, tzinfo=timezone.utc),
        "status": "COMPLETED",
        "pages_count": 50,
        "internal_runtime": {"debug": {"existing": True}},
        "result": persisted_result,
    }
    fake_db.perizia_analyses.items.append(stored)
    before_stored = copy.deepcopy(stored)

    response = await server._get_perizia_analysis_for_user("analysis_6b3ab6865dca", _test_user())
    result = response["result"]
    cdc = result["customer_decision_contract"]

    assert cdc["lots_count"] == 1
    assert cdc["lot_count"] == 1
    assert cdc["is_multi_lot"] is False
    assert cdc["case_header"]["lotto"] == "Lotto Unico"
    assert cdc["report_header"]["lotto"]["value"] == "Lotto Unico"
    assert len(cdc["lot_index"]) == 1
    assert "Lotti 1, 2" not in str(cdc)
    assert result["lots_count"] == 1
    assert result["lot_count"] == 1
    assert result["is_multi_lot"] is False
    assert result["case_header"]["lotto"] == "Lotto Unico"
    assert result["report_header"]["lotto"]["value"] == "Lotto Unico"
    assert len(result["lot_index"]) == 1
    assert "Lotti 1, 2" not in str(result["case_header"]["lotto"])
    assert fake_db.perizia_analyses.items[0] == before_stored
    assert _collect_forbidden_customer_keys(response) == []
    assert result["money_box"] == before_stored["result"]["money_box"]
    assert result["section_9_legal_killers"] == before_stored["result"]["section_9_legal_killers"]
    assert result["summary_for_client"] == before_stored["result"]["summary_for_client"]
    assert result["decision_rapida_client"] == before_stored["result"]["decision_rapida_client"]


@pytest.mark.anyio
async def test_persisted_detail_authority_lot_projection_flag_off_keeps_saved_payload(fake_db, monkeypatch):
    monkeypatch.delenv(server.AUTHORITY_LOT_PROJECTION_FLAG, raising=False)

    def forbidden_loader(*_args, **_kwargs):
        raise AssertionError("flag-off persisted read must not load authority projection inputs")

    monkeypatch.setattr(server, "_load_authority_shadow_for_detail_read", forbidden_loader)
    persisted_result = _persisted_lot_result(mode="multi_lot", lots_count=2)
    stored = {
        "analysis_id": "analysis_6b3ab6865dca",
        "user_id": "user_test",
        "case_id": "case_casa",
        "case_title": "Casa ai Venti.pdf",
        "file_name": "Casa ai Venti.pdf",
        "created_at": datetime(2026, 4, 25, tzinfo=timezone.utc),
        "status": "COMPLETED",
        "pages_count": 50,
        "internal_runtime": {"debug": {"authority_lot_projection": {"applied": True}}},
        "result": persisted_result,
    }
    fake_db.perizia_analyses.items.append(stored)
    before_stored = copy.deepcopy(stored)

    response = await server._get_perizia_analysis_for_user("analysis_6b3ab6865dca", _test_user())

    assert response["result"] == persisted_result
    assert response["result"]["lots_count"] == 2
    assert response["result"]["is_multi_lot"] is True
    assert response["result"]["customer_decision_contract"]["lots_count"] == 2
    assert response["result"]["customer_decision_contract"]["is_multi_lot"] is True
    assert fake_db.perizia_analyses.items[0] == before_stored
    assert _collect_forbidden_customer_keys(response) == []


@pytest.mark.anyio
async def test_persisted_detail_outbound_sanitizer_strips_full_response_internal_keys(fake_db, monkeypatch):
    monkeypatch.delenv(server.AUTHORITY_LOT_PROJECTION_FLAG, raising=False)

    def forbidden_refresh(*_args, **_kwargs):
        raise AssertionError("persisted customer contract detail read must not refresh")

    monkeypatch.setattr(server, "_refresh_customer_facing_result_on_read", forbidden_refresh)

    persisted_result = _persisted_lot_result(mode="single_lot", lots_count=1)
    persisted_result["debug"] = {"authority_shadow_resolvers": {"leak": True}}
    persisted_result["qa_gate"] = {"status": "PASS", "context_debug": {"internal": True}}
    persisted_result["money_box"] = {
        "valuation_references": [
            {
                "code": "AUTH_VAL_REF_01",
                "customer_title_it": "Valore di stima",
                "label_it": "Valore stimato",
                "amount_eur": 391849,
                "evidence": [{"page": 40, "quote": "Valore finale di stima"}],
            }
        ]
    }
    persisted_result["customer_decision_contract"]["authority_shadow_resolvers"] = {"leak": True}
    persisted_result["customer_decision_contract"]["field_states"] = {
        "stato_occupativo": {
            "value": "LIBERO",
            "status": "OK",
            "evidence": [{"page": 12, "quote": "Libero"}],
            "chosen_candidate": {"value": "LIBERO", "authority_score": 0.91},
        }
    }
    stored = {
        "analysis_id": "analysis_persisted_leaky",
        "user_id": "user_test",
        "case_id": "case_persisted_leaky",
        "case_title": "persisted-leaky.pdf",
        "file_name": "persisted-leaky.pdf",
        "created_at": datetime(2026, 4, 25, tzinfo=timezone.utc),
        "status": "COMPLETED",
        "pages_count": 50,
        "internal_runtime": {"debug": {"authority_lot_projection": {"applied": True}}},
        "result": persisted_result,
    }
    fake_db.perizia_analyses.items.append(stored)
    before_stored = copy.deepcopy(stored)

    response = await server._get_perizia_analysis_for_user("analysis_persisted_leaky", _test_user())

    assert _collect_forbidden_customer_keys(response) == []
    assert _collect_forbidden_customer_strings(response) == []
    money_item = response["result"]["money_box"]["valuation_references"][0]
    assert money_item == {
        "customer_title_it": "Valore di stima",
        "label_it": "Valore stimato",
        "amount_eur": 391849,
        "evidence": [{"page": 40, "quote": "Valore finale di stima"}],
    }
    # qa_gate is an internal QA container (model name, corrections, critique) and
    # is moved to the internal runtime sidecar, never exposed in customer output.
    assert "qa_gate" not in response["result"]
    field_state = response["result"]["customer_decision_contract"]["field_states"]["stato_occupativo"]
    assert field_state["value"] == "LIBERO"
    assert field_state["status"] == "OK"
    assert field_state["evidence"] == [{"page": 12, "quote": "Libero"}]
    assert field_state["chosen_candidate"] == {"value": "LIBERO"}
    assert fake_db.perizia_analyses.items[0] == before_stored


@pytest.mark.anyio
async def test_legacy_refresh_detail_outbound_sanitizer_strips_authority_candidate_keys(fake_db, monkeypatch):
    monkeypatch.delenv(server.AUTHORITY_LOT_PROJECTION_FLAG, raising=False)
    monkeypatch.setattr(server, "_load_pages_for_analysis", lambda *_args, **_kwargs: [])

    def no_refresh(result, *_args, **_kwargs):
        result.setdefault("report_header", {"lotto": {"value": "Lotto Unico"}})

    monkeypatch.setattr(server, "_refresh_customer_facing_result_on_read", no_refresh)

    leaky_field_state = {
        "value": "OCCUPATO",
        "status": "OK",
        "evidence": [{"page": 9, "quote": "Occupato"}],
        "chosen_candidate": {"value": "OCCUPATO", "authority_score": 0.94},
        "all_candidates": [{"value": "OCCUPATO", "authority_score": 0.94}],
        "top_candidates": [{"value": "OCCUPATO", "authority_score": 0.94}],
    }
    stored = {
        "analysis_id": "analysis_legacy_refresh_leaky",
        "user_id": "user_test",
        "case_id": "case_legacy_refresh_leaky",
        "case_title": "legacy-refresh-leaky.pdf",
        "file_name": "legacy-refresh-leaky.pdf",
        "created_at": datetime(2026, 4, 25, tzinfo=timezone.utc),
        "status": "COMPLETED",
        "pages_count": 50,
        "internal_runtime": {"debug": {"authority_lot_projection": {"applied": True}}},
        "result": {
            "debug": {"authority_shadow_resolvers": {"leak": True}},
            "field_states": {"stato_occupativo": leaky_field_state},
            "case_header": {"lotto": "Lotto Unico"},
            "report_header": {"lotto": {"value": "Lotto Unico"}},
            "lots_count": 1,
            "is_multi_lot": False,
            "summary_for_client": {"summary_it": "unchanged"},
            "decision_rapida_client": {"headline_it": "unchanged"},
        },
    }
    fake_db.perizia_analyses.items.append(stored)
    before_stored = copy.deepcopy(stored)

    response = await server._get_perizia_analysis_for_user("analysis_legacy_refresh_leaky", _test_user())

    assert _collect_forbidden_customer_keys(response) == []
    field_state = response["result"]["field_states"]["stato_occupativo"]
    assert field_state["value"] == "OCCUPATO"
    assert field_state["status"] == "OK"
    assert field_state["evidence"] == [{"page": 9, "quote": "Occupato"}]
    assert field_state["chosen_candidate"] == {"value": "OCCUPATO"}
    assert field_state["all_candidates"] == [{"value": "OCCUPATO"}]
    assert field_state["top_candidates"] == [{"value": "OCCUPATO"}]
    assert response["result"]["lots_count"] == 1
    assert response["result"]["is_multi_lot"] is False
    assert response["result"]["case_header"]["lotto"] == "Lotto Unico"
    assert response["result"]["report_header"]["lotto"]["value"] == "Lotto Unico"
    assert fake_db.perizia_analyses.items[0] == before_stored


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("analysis_id", "saved_result", "shadow", "expected_count", "expected_multi"),
    [
        ("analysis_1859886", _persisted_lot_result(mode="single_lot", lots_count=1), _authority_lot_shadow("single_lot"), 1, False),
        ("analysis_multilot_69", _persisted_lot_result(mode="multi_lot", lots_count=3), _authority_lot_shadow("multi_lot", numbers=[1, 2, 3]), 3, True),
        (
            "analysis_ostuni",
            _persisted_lot_result(mode="multi_lot", lots_count=7),
            _authority_lot_shadow("multi_lot", numbers=[1, 2, 3, 4, 5, 6, 7], rule="chapter_based_multi_lot_topology"),
            7,
            True,
        ),
    ],
)
async def test_persisted_detail_authority_lot_projection_preserves_passing_lot_cases(
    fake_db,
    monkeypatch,
    analysis_id,
    saved_result,
    shadow,
    expected_count,
    expected_multi,
):
    monkeypatch.setenv(server.AUTHORITY_LOT_PROJECTION_FLAG, "1")
    monkeypatch.setattr(server, "_load_authority_shadow_for_detail_read", lambda _analysis_id, _analysis: shadow)
    fake_db.perizia_analyses.items.append(
        {
            "analysis_id": analysis_id,
            "user_id": "user_test",
            "case_id": f"case_{analysis_id}",
            "case_title": f"{analysis_id}.pdf",
            "file_name": f"{analysis_id}.pdf",
            "created_at": datetime(2026, 4, 25, tzinfo=timezone.utc),
            "status": "COMPLETED",
            "pages_count": 80,
            "result": saved_result,
        }
    )

    response = await server._get_perizia_analysis_for_user(analysis_id, _test_user())

    assert response["result"]["lots_count"] == expected_count
    assert response["result"]["is_multi_lot"] is expected_multi
    assert response["result"]["customer_decision_contract"]["lots_count"] == expected_count
    assert response["result"]["customer_decision_contract"]["is_multi_lot"] is expected_multi
    assert _collect_forbidden_customer_keys(response) == []


@pytest.mark.anyio
async def test_persisted_detail_preserves_ostuni_lot_occupancy_and_syncs_overview(fake_db, monkeypatch):
    monkeypatch.setenv(server.AUTHORITY_LOT_PROJECTION_FLAG, "1")
    monkeypatch.setattr(
        server,
        "_load_authority_shadow_for_detail_read",
        lambda _analysis_id, _analysis: _authority_lot_shadow("multi_lot", numbers=[1, 2, 3, 4, 5, 6, 7]),
    )
    saved_result = _persisted_lot_result(mode="multi_lot", lots_count=7)
    statuses = [
        "OCCUPATO DAL DEBITORE",
        "OCCUPATO DAL DEBITORE",
        "OCCUPATO DAL DEBITORE",
        "OCCUPATO DAL DEBITORE",
        "LOCATO",
        "OCCUPATO DAL DEBITORE",
        "LIBERO",
    ]
    saved_result["field_states"] = {
        "stato_occupativo": {"value": "OCCUPATO", "status": "FOUND"},
        "superficie": {"value": 123.6, "unit": "mq", "status": "FOUND"},
    }
    for idx, status in enumerate(statuses):
        saved_result["lots"][idx].update(
            {
                "stato_occupativo": status,
                "occupancy_status": status,
                "superficie_mq": None,
            }
        )
    saved_result["customer_decision_contract"]["lots"] = copy.deepcopy(saved_result["lots"])
    saved_result["customer_decision_contract"]["field_states"] = copy.deepcopy(saved_result["field_states"])
    saved_result["panoramica_contract"] = {"lots_overview": []}
    saved_result["customer_decision_contract"]["panoramica_contract"] = {"lots_overview": []}
    stored = {
        "analysis_id": "analysis_1127db41e705",
        "user_id": "user_test",
        "case_id": "case_b6f579e0",
        "case_title": "Ostuni, Via Viterbo 2.pdf",
        "file_name": "Ostuni, Via Viterbo 2.pdf",
        "created_at": datetime(2026, 4, 25, tzinfo=timezone.utc),
        "status": "COMPLETED",
        "pages_count": 80,
        "result": saved_result,
    }
    fake_db.perizia_analyses.items.append(stored)
    before_stored = copy.deepcopy(stored)

    response = await server._get_perizia_analysis_for_user("analysis_1127db41e705", _test_user())
    lots = response["result"]["lots"]
    overview = response["result"]["panoramica_contract"]["lots_overview"]
    cdc_overview = response["result"]["customer_decision_contract"]["panoramica_contract"]["lots_overview"]

    assert lots[4]["stato_occupativo"] == "LOCATO"
    assert lots[6]["stato_occupativo"] == "LIBERO"
    assert {lot["stato_occupativo"] for lot in lots} != {"OCCUPATO"}
    assert [row["stato_occupativo"] for row in overview] == statuses
    assert [row["stato_occupativo"] for row in cdc_overview] == statuses
    assert all(row.get("superficie_mq") is None for row in overview)
    assert fake_db.perizia_analyses.items[0] == before_stored
    assert _collect_forbidden_customer_keys(response) == []


@pytest.mark.anyio
async def test_persisted_detail_multilot_lots_overview_preserves_lot_surfaces(fake_db, monkeypatch):
    monkeypatch.setenv(server.AUTHORITY_LOT_PROJECTION_FLAG, "1")
    monkeypatch.setattr(
        server,
        "_load_authority_shadow_for_detail_read",
        lambda _analysis_id, _analysis: _authority_lot_shadow("multi_lot", numbers=[1, 2, 3]),
    )
    saved_result = _persisted_lot_result(mode="multi_lot", lots_count=3)
    lots = [
        {
            "lot_number": 1,
            "superficie_mq": 169.3,
            "diritto_reale": "Proprietà 1/1",
            "quota": "1/1",
            "stato_occupativo": "LOCATO",
            "ubicazione": "Montecatini-Terme (PT)-via Giuseppe Garibaldi n.c. 23",
        },
        {
            "lot_number": 2,
            "superficie_mq": 258.5,
            "diritto_reale": "Proprietà 1/1 + Quota 1/4 della stradella privata di accesso",
            "quota": "1/1",
            "stato_occupativo": "LIBERO",
            "ubicazione": "Pieve a Nievole (PT)-via Colonna senza numero civico",
        },
        {
            "lot_number": 3,
            "superficie_mq": 411.85,
            "diritto_reale": "Proprietà 1/1 + Quota 1/4 della stradella privata di accesso",
            "quota": "1/1",
            "stato_occupativo": "LOCATO",
            "ubicazione": "Pieve a Nievole (PT) - via Colonna senza numero civico",
        },
    ]
    saved_result["lots"] = copy.deepcopy(lots)
    saved_result["customer_decision_contract"]["lots"] = copy.deepcopy(lots)
    saved_result["panoramica_contract"] = {}
    saved_result["customer_decision_contract"]["panoramica_contract"] = {}
    stored = {
        "analysis_id": "analysis_996bb0474af9",
        "user_id": "user_test",
        "case_id": "case_4c5cad8c",
        "case_title": "perizia_multilot_69_2024.pdf",
        "file_name": "perizia_multilot_69_2024.pdf",
        "created_at": datetime(2026, 4, 25, tzinfo=timezone.utc),
        "status": "COMPLETED",
        "pages_count": 80,
        "result": saved_result,
    }
    fake_db.perizia_analyses.items.append(stored)
    before_stored = copy.deepcopy(stored)

    response = await server._get_perizia_analysis_for_user("analysis_996bb0474af9", _test_user())
    overview = response["result"]["panoramica_contract"]["lots_overview"]

    assert len(response["result"]["lots"]) == 3
    assert len(overview) == 3
    assert [row["superficie_mq"] for row in overview] == [169.3, 258.5, 411.85]
    assert [row["diritto_reale"] for row in overview] == [lot["diritto_reale"] for lot in lots]
    assert [row["quota"] for row in overview] == ["1/1", "1/1", "1/1"]
    assert [row["stato_occupativo"] for row in overview] == ["LOCATO", "LIBERO", "LOCATO"]
    assert fake_db.perizia_analyses.items[0] == before_stored
    assert _collect_forbidden_customer_keys(response) == []


@pytest.mark.anyio
async def test_persisted_detail_single_lot_lots_overview_preserves_surface(fake_db, monkeypatch):
    monkeypatch.setenv(server.AUTHORITY_LOT_PROJECTION_FLAG, "1")
    monkeypatch.setattr(
        server,
        "_load_authority_shadow_for_detail_read",
        lambda _analysis_id, _analysis: _authority_lot_shadow("single_lot", numbers=[1]),
    )
    saved_result = _persisted_lot_result(mode="single_lot", lots_count=1)
    saved_result["lots"][0].update(
        {
            "superficie_mq": 116.39,
            "diritto_reale": "Proprietà",
            "quota": "1/1",
            "stato_occupativo": "OCCUPATO",
            "ubicazione": "San Giorgio Bigarello (MN) - Via Sordello n. 5, piano Seminterrato",
        }
    )
    saved_result["customer_decision_contract"]["lots"] = copy.deepcopy(saved_result["lots"])
    saved_result["panoramica_contract"] = {}
    saved_result["customer_decision_contract"]["panoramica_contract"] = {}
    stored = {
        "analysis_id": "analysis_98d2cb078503",
        "user_id": "user_test",
        "case_id": "case_c8b8d881",
        "case_title": "1859886_c_perizia.pdf",
        "file_name": "1859886_c_perizia.pdf",
        "created_at": datetime(2026, 4, 25, tzinfo=timezone.utc),
        "status": "COMPLETED",
        "pages_count": 50,
        "result": saved_result,
    }
    fake_db.perizia_analyses.items.append(stored)
    before_stored = copy.deepcopy(stored)

    response = await server._get_perizia_analysis_for_user("analysis_98d2cb078503", _test_user())
    overview = response["result"]["panoramica_contract"]["lots_overview"]

    assert len(response["result"]["lots"]) == 1
    assert len(overview) == 1
    assert overview[0]["superficie_mq"] == 116.39
    assert overview[0]["diritto_reale"] == "Proprietà"
    assert overview[0]["quota"] == "1/1"
    assert fake_db.perizia_analyses.items[0] == before_stored
    assert _collect_forbidden_customer_keys(response) == []


def test_lots_overview_does_not_promote_global_surface_to_multilot():
    result = {
        "field_states": {"superficie": {"value": 123.6, "unit": "mq", "status": "FOUND"}},
        "lots": [
            {"lot_number": 1, "stato_occupativo": "OCCUPATO"},
            {"lot_number": 2, "stato_occupativo": "LOCATO"},
            {"lot_number": 3, "stato_occupativo": "LIBERO"},
        ],
        "customer_decision_contract": {},
    }
    source = copy.deepcopy(result)

    overview = server._sync_lots_overview_from_result_lots(result)

    assert [row.get("superficie_mq") for row in overview] == [None, None, None]
    assert result["customer_decision_contract"]["panoramica_contract"]["lots_overview"] == overview
    assert source.get("panoramica_contract") is None


@pytest.mark.anyio
async def test_persisted_detail_authority_lot_projection_missing_pack_fails_open(fake_db, monkeypatch):
    monkeypatch.setenv(server.AUTHORITY_LOT_PROJECTION_FLAG, "1")
    persisted_result = _persisted_lot_result(mode="multi_lot", lots_count=2)
    fake_db.perizia_analyses.items.append(
        {
            "analysis_id": "analysis_missing_read_path_projection_pack",
            "user_id": "user_test",
            "case_id": "case_missing",
            "case_title": "missing.pdf",
            "file_name": "missing.pdf",
            "created_at": datetime(2026, 4, 25, tzinfo=timezone.utc),
            "status": "COMPLETED",
            "pages_count": 1,
            "result": persisted_result,
        }
    )
    before_stored = copy.deepcopy(fake_db.perizia_analyses.items[0])

    response = await server._get_perizia_analysis_for_user("analysis_missing_read_path_projection_pack", _test_user())

    assert response["result"]["lots_count"] == 2
    assert response["result"]["is_multi_lot"] is True
    assert response["result"]["customer_decision_contract"]["lots_count"] == 2
    assert response["result"]["customer_decision_contract"]["is_multi_lot"] is True
    assert len(response["result"]["panoramica_contract"]["lots_overview"]) == 2
    assert len(response["result"]["customer_decision_contract"]["panoramica_contract"]["lots_overview"]) == 2
    assert fake_db.perizia_analyses.items[0] == before_stored
    assert _collect_forbidden_customer_keys(response) == []


@pytest.mark.anyio
async def test_legacy_refresh_combined_flags_sync_authority_multilot_headers_without_mutating_source(fake_db, monkeypatch):
    monkeypatch.setenv(server.AUTHORITY_LOT_PROJECTION_FLAG, "1")
    monkeypatch.setenv(server.AUTHORITY_MONEY_PROJECTION_FLAG, "1")
    monkeypatch.setattr(
        server,
        "_load_authority_shadow_for_detail_read",
        lambda _analysis_id, _analysis: _authority_lot_shadow("multi_lot", numbers=[1, 2, 3]),
    )
    saved_result = {
        "lots": [{"lot_number": 1}, {"lot_number": 2}, {"lot_number": 3}],
        "lots_count": 3,
        "lot_count": 3,
        "is_multi_lot": True,
        "case_header": {"lotto": "DA VERIFICARE"},
        "report_header": {"lotto": {"value": "Lotto 1"}, "is_multi_lot": False},
        "field_states": {"lotto": {"value": "DA VERIFICARE"}},
        "money_box": {"items": []},
        "section_3_money_box": {"items": []},
    }
    stored = {
        "analysis_id": "analysis_combined_legacy_multilot_headers",
        "user_id": "user_test",
        "case_id": "case_combined_legacy_multilot_headers",
        "case_title": "combined-legacy-multilot.pdf",
        "file_name": "combined-legacy-multilot.pdf",
        "created_at": datetime(2026, 4, 25, tzinfo=timezone.utc),
        "status": "COMPLETED",
        "pages_count": 80,
        "result": saved_result,
    }
    fake_db.perizia_analyses.items.append(stored)
    before_stored = copy.deepcopy(stored)

    response = await server._get_perizia_analysis_for_user("analysis_combined_legacy_multilot_headers", _test_user())

    result = response["result"]
    assert result["lots_count"] == 3
    assert result["is_multi_lot"] is True
    assert result["case_header"]["lotto"] == "Lotti 1, 2, 3"
    assert result["report_header"]["lotto"]["value"] == "Lotti 1, 2, 3"
    assert result["report_header"]["is_multi_lot"] is True
    cdc = result["customer_decision_contract"]
    assert cdc["case_header"]["lotto"] == "Lotti 1, 2, 3"
    assert cdc["report_header"]["lotto"]["value"] == "Lotti 1, 2, 3"
    assert cdc["report_header"]["is_multi_lot"] is True
    assert "lot_verification_hint" not in result
    assert fake_db.perizia_analyses.items[0] == before_stored
    assert _collect_forbidden_customer_keys(response) == []
