import sys
from io import BytesIO
from pathlib import Path

import pytest
from PyPDF2 import PdfReader

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pdf_report
import server
from pdf_report import NO_DEFENSIBLE_TOTAL_TEXT, money_report_payload_from_result


@pytest.fixture()
def anyio_backend():
    return "asyncio"


def _flatten_text(value):
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return " ".join(_flatten_text(v) for v in value.values())
    if isinstance(value, list):
        return " ".join(_flatten_text(v) for v in value)
    return str(value)


def _authority_entry(code, title, amount="EUR 1.336", page=63):
    return {
        "code": code,
        "label_it": f"{code} - {title}",
        "customer_title_it": title,
        "customer_amount_label": amount,
        "customer_context_it": "Spese condominiali scadute ed insolute alla data della perizia.",
        "evidence": [{"page": page, "quote": "Spese condominiali scadute ed insolute: EUR 1.336,31"}],
    }


def _money_box_with_groups():
    return {
        "items": [
            {
                "code": "A",
                "label_it": "STALE LEGACY COST ITEM",
                "stima_euro": 999999,
                "fonte_perizia": "stale",
            }
        ],
        "buyer_cost_signals_to_verify": [
            _authority_entry("AUTH_BUYER_VERIFY_01", "Spese condominiali pregresse: EUR 1.336")
        ],
        "cost_signals_to_verify": [
            _authority_entry("AUTH_BUYER_VERIFY_DUP", "Duplicato da non usare se buyer_cost_signals esiste")
        ],
        "valuation_references": [
            _authority_entry("AUTH_VAL_REF_01", "Valore di stima / mercato: EUR 193.043", "EUR 193.043", 3)
        ],
        "valuation_reference_amounts": [
            _authority_entry("AUTH_VAL_REF_01", "Valore di stima / mercato: EUR 193.043", "EUR 193.043", 3)
        ],
        "other_monetary_mentions": [
            _authority_entry("AUTH_MONEY_MENTION_01", "Importo monetario: EUR 27.000", "EUR 27.000", 62),
            _authority_entry("AUTH_MONEY_MENTION_02", "Importo monetario: EUR 652", "EUR 652", 63),
        ],
        "totale_extra_budget": {
            "min": None,
            "max": None,
            "nota": "Nessun costo extra buyer-side certo ricavabile dalla perizia.",
        },
        "customer_summary": {
            "line_it": "Nessun totale buyer-side difendibile e stato ricavato dalla perizia.",
            "total_status": {
                "status_code": "no_defensible_total",
                "label_it": "Non quantificato in modo difendibile",
            },
        },
    }


def test_pdf_money_model_uses_section_3_authority_groups_before_legacy_items():
    payload = money_report_payload_from_result(
        {
            "section_3_money_box": _money_box_with_groups(),
            "money_box": {"items": [{"label_it": "OLD MONEY BOX ITEM", "stima_euro": 42}]},
        }
    )

    text = _flatten_text(payload)
    assert payload["source"] == "section_3_money_box"
    assert payload["uses_authority_groups"] is True
    assert "Spese condominiali pregresse" in text
    assert "STALE LEGACY COST ITEM" not in text
    assert "OLD MONEY BOX ITEM" not in text


def test_pdf_money_model_does_not_fabricate_null_total():
    payload = money_report_payload_from_result({"section_3_money_box": _money_box_with_groups()})

    assert payload["total"] == NO_DEFENSIBLE_TOTAL_TEXT
    assert "EUR 0" not in _flatten_text(payload)
    assert "999999" not in _flatten_text(payload)


def test_pdf_money_model_hides_internal_auth_codes():
    payload = money_report_payload_from_result({"section_3_money_box": _money_box_with_groups()})

    assert "AUTH_" not in _flatten_text(payload)


def test_pdf_money_model_dedupes_valuation_reference_alias_groups():
    payload = money_report_payload_from_result({"section_3_money_box": _money_box_with_groups()})
    text = _flatten_text(payload)

    assert text.count("Valore di stima / mercato: EUR 193.043") == 1


def test_pdf_money_model_preserves_evidence_page_references():
    payload = money_report_payload_from_result({"section_3_money_box": _money_box_with_groups()})
    text = _flatten_text(payload)

    assert "p.63" in text
    assert "Spese condominiali scadute" in text


def test_pdf_money_model_uses_cdc_groups_when_top_level_legacy_is_empty():
    payload = money_report_payload_from_result(
        {
            "section_3_money_box": {"items": []},
            "money_box": {"items": []},
            "customer_decision_contract": {"money_box": _money_box_with_groups()},
        }
    )

    assert payload["source"] == "customer_decision_contract.money_box"
    assert payload["uses_authority_groups"] is True
    assert "Spese condominiali pregresse" in _flatten_text(payload)


def test_generate_report_html_uses_authority_money_payload():
    html = server.generate_report_html(
        {"analysis_id": "analysis_html_render", "file_name": "test.pdf"},
        {
            "section_3_money_box": _money_box_with_groups(),
            "money_box": {"items": [{"label_it": "OLD MONEY BOX ITEM", "stima_euro": 42}]},
        },
    )

    assert "Spese condominiali pregresse" in html
    assert NO_DEFENSIBLE_TOTAL_TEXT in html
    assert "AUTH_" not in html
    assert "STALE LEGACY COST ITEM" not in html
    assert "OLD MONEY BOX ITEM" not in html


def test_build_pdf_bytes_uses_authority_money_payload():
    pdf_bytes = pdf_report.build_perizia_pdf_bytes(
        {"analysis_id": "analysis_pdf_render", "file_name": "test.pdf"},
        {
            "section_3_money_box": _money_box_with_groups(),
            "money_box": {"items": [{"label_it": "OLD MONEY BOX ITEM", "stima_euro": 42}]},
        },
    )
    text = "\n".join(page.extract_text() or "" for page in PdfReader(BytesIO(pdf_bytes)).pages)
    normalized_text = " ".join(text.split())

    assert "Spese condominiali pregresse" in normalized_text
    assert NO_DEFENSIBLE_TOTAL_TEXT in normalized_text
    assert "AUTH_" not in text
    assert "STALE LEGACY COST ITEM" not in text
    assert "OLD MONEY BOX ITEM" not in text


@pytest.mark.anyio
async def test_pdf_endpoint_renders_customer_facing_detail_read_model(monkeypatch):
    captured = {}
    customer_analysis = {
        "analysis_id": "analysis_pdf",
        "result": {"section_3_money_box": _money_box_with_groups()},
    }

    async def fake_require_auth(_request):
        return server.User(user_id="user_test", email="user@test.local", name="Test User")

    async def fake_get_perizia_analysis_for_user(analysis_id, user):
        captured["analysis_id"] = analysis_id
        captured["user_id"] = user.user_id
        return customer_analysis

    def fake_build_pdf_bytes(analysis, result):
        captured["analysis"] = analysis
        captured["result"] = result
        return b"%PDF-branch-proof"

    monkeypatch.setattr(server, "require_auth", fake_require_auth)
    monkeypatch.setattr(server, "_get_perizia_analysis_for_user", fake_get_perizia_analysis_for_user)
    monkeypatch.setattr(pdf_report, "build_perizia_pdf_bytes", fake_build_pdf_bytes)

    response = await server.download_perizia_pdf("analysis_pdf", object())

    assert response.body == b"%PDF-branch-proof"
    assert captured["analysis_id"] == "analysis_pdf"
    assert captured["user_id"] == "user_test"
    assert captured["analysis"] is customer_analysis
    assert captured["result"] is customer_analysis["result"]


@pytest.mark.anyio
async def test_html_endpoint_renders_customer_facing_detail_read_model(monkeypatch):
    captured = {}
    customer_analysis = {
        "analysis_id": "analysis_html",
        "result": {"section_3_money_box": _money_box_with_groups()},
    }

    async def fake_require_auth(_request):
        return server.User(user_id="user_test", email="user@test.local", name="Test User")

    async def fake_get_perizia_analysis_for_user(analysis_id, user):
        captured["analysis_id"] = analysis_id
        captured["user_id"] = user.user_id
        return customer_analysis

    def fake_generate_report_html(analysis, result):
        captured["analysis"] = analysis
        captured["result"] = result
        return "<html>money-map</html>"

    monkeypatch.setattr(server, "require_auth", fake_require_auth)
    monkeypatch.setattr(server, "_get_perizia_analysis_for_user", fake_get_perizia_analysis_for_user)
    monkeypatch.setattr(server, "generate_report_html", fake_generate_report_html)

    response = await server.download_perizia_html("analysis_html", object())

    assert response.body == b"<html>money-map</html>"
    assert captured["analysis_id"] == "analysis_html"
    assert captured["user_id"] == "user_test"
    assert captured["analysis"] is customer_analysis
    assert captured["result"] is customer_analysis["result"]
