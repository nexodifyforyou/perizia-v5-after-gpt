"""customer_view decision-model integration (plan Part 24 #27-29)."""

from correctness_v2 import customer_view


def _full_report(status="REPORT_READY"):
    return {
        "schema_version": "cv2.customer_report.v1",
        "analysis_id": "analysis_x",
        "job_id": "cv2_x",
        "report_status": status,
        "title": "Report cliente",
        "case_identity": {"tribunale": "T", "address": "Via 1", "property_type": "casa",
                          "evidence_pages": [1]},
        "lot_structure": {"selected_lot": "1", "multi_lot": False, "bene_ids": []},
        "beni_sections": [{"bene_id": "principale", "title": "Bene", "is_main_property": True,
                           "evidence_pages": [1], "accessories": []}],
        "occupancy_section": {}, "compliance_section": [], "formalities_section": [],
        "buyer_checklist": [], "risk_sections": [], "customer_evidence_index": [],
        "money_sections": {"valuation_chain": [], "buyer_side_costs": [],
            "procedure_cancelled_formalities": [], "market_comparatives": [],
            "context_values": [], "uncertain_money": [], "auction_terms": []},
        # admin-only machinery that must never leak
        "quality_control": {"rows": []},
        "admin_evidence_index": [{"page": 1, "raw_keys": "x"}],
        "evidence_index": [{"page": 1}],
        "manual_review_flags": [{"code": "LOW_CONFIDENCE"}],
        "sections_meta": {"x": 1},
        "surfaces_section": [{"m2": 10}],
    }


# 27. sanitize attaches decision_model; the legacy customer key set is unchanged
def test_sanitize_attaches_decision_model_and_keeps_legacy_keys():
    report = _full_report()
    out = customer_view.sanitize_customer_report(report, {"safe_to_show_customer": True})
    assert out["decision_model"]["schema_version"] == "cv2.customer_decision.v1"
    # Every legacy customer key still present (additive change).
    for key in ("decision", "case_identity", "money_sections", "beni_sections",
                "occupancy_section", "compliance_section", "formalities_section",
                "buyer_checklist", "customer_evidence_index", "risk_sections", "key_facts"):
        assert key in out


# 28. non-ready statuses attach only esito + readiness (no findings), flow unchanged
def test_non_ready_statuses_minimal_decision_model():
    for status in ("LOT_SELECTION_REQUIRED", "MONEY_CONFIRMATION_REQUIRED", "DOCUMENT_NOT_READABLE"):
        out = customer_view.sanitize_customer_report(_full_report(status), {"safe_to_show_customer": True})
        dm = out["decision_model"]
        assert dm["findings"] == []
        assert dm["esito"]["level"] in ("verde", "ambra", "rosso")
        assert list(dm["sections"].keys()) == ["stato_verifiche"]


# 29. admin-only keys stay absent even with the decision model attached
def test_admin_keys_absent_with_decision_model():
    out = customer_view.sanitize_customer_report(_full_report(), {"safe_to_show_customer": True})
    for key in customer_view._ADMIN_ONLY_KEYS:
        assert key not in out
    blob = str(out)
    assert "LOW_CONFIDENCE" not in blob
    assert "raw_keys" not in blob
