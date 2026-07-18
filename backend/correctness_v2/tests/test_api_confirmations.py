"""API integration tests for focused confirmations (plan Part 24 #30-32)."""

import asyncio
import types

import httpx
from fastapi import FastAPI, HTTPException
import pytest

from correctness_v2 import api, artifacts, decision_model, user_confirmations


def _report(analysis_id, job_id):
    return {
        "schema_version": "cv2.customer_report.v1",
        "analysis_id": analysis_id,
        "job_id": job_id,
        "report_status": "REPORT_READY",
        "title": "Report cliente",
        "case_identity": {"tribunale": "T", "address": "Via 1", "property_type": "appartamento",
                          "evidence_pages": [1]},
        "lot_structure": {"selected_lot": "1"},
        "beni_sections": [{"bene_id": "principale", "title": "Bene", "is_main_property": True,
                           "evidence_pages": [1], "accessories": []}],
        "occupancy_section": {}, "compliance_section": [], "formalities_section": [],
        "buyer_checklist": [], "risk_sections": [],
        "customer_evidence_index": [{"page": 38, "topic": "Oblazione", "report_section": "Costi",
            "perizia_excerpt": "oblazione indicativa art 36 bis 1032 euro", "coverage_status": "covered"}],
        "money_sections": {
            "valuation_chain": [{"label": "Valore", "amount": 100.0, "amount_display": "€ 100,00",
                                 "kind": "value", "evidence_pages": [1]}],
            "buyer_side_costs": [], "procedure_cancelled_formalities": [], "market_comparatives": [],
            "context_values": [],
            "uncertain_money": [{"label": "Oblazione art 36 bis", "amount": 1032.0,
                "amount_display": "€ 1.032,00", "kind": "uncertain", "evidence_pages": [38],
                "reason": "Importo indicativo da verificare."}],
            "auction_terms": [],
        },
    }


class _Store:
    """In-memory async stand-in for the Mongo store, wired via monkeypatch."""
    def __init__(self):
        self.rows = []

    async def submit(self, *, analysis_id, lot_id, finding, option_id, user_id, report_version,
                     job_id=None, note=None, decision_version=None):
        label = None
        for o in finding["confirmation"]["options"]:
            if o["option_id"] == option_id:
                label = o["label"]
        if option_id == "non_sicuro":
            label = "Non sono sicuro"
        if label is None:
            raise ValueError("Opzione non valida")
        ev = finding.get("evidence") or {}
        doc = {"analysis_id": analysis_id, "lot_id": str(lot_id), "finding_id": finding["finding_id"],
               "user_id": user_id, "selected_option": option_id, "selected_label": label,
               "page": ev.get("page"), "status": "non_sicuro" if option_id == "non_sicuro" else "confermato_utente",
               "evidence_hash": decision_model.evidence_hash(ev.get("excerpt")),
               "report_version": report_version, "updated_at": "now"}
        self.rows = [r for r in self.rows if not (r["finding_id"] == finding["finding_id"] and r["user_id"] == user_id)]
        self.rows.append(doc)
        return doc

    async def list_for_analysis(self, analysis_id, user_id):
        return [r for r in self.rows if r["analysis_id"] == analysis_id and r["user_id"] == user_id]

    async def list_all_for_analysis(self, analysis_id):
        return [r for r in self.rows if r["analysis_id"] == analysis_id]

    async def audit_for_analysis(self, analysis_id):
        return []


@pytest.fixture()
def app_env(monkeypatch, artifacts_root):
    store = _Store()
    state = {"user_id": "owner", "is_admin": False, "admin_ok": False}

    async def _access(request, analysis_id):
        if state["user_id"] is None:
            raise HTTPException(status_code=404, detail="Analysis not found")
        return types.SimpleNamespace(user_id=state["user_id"]), state["is_admin"]

    async def _guard(request):
        if not state["admin_ok"]:
            raise HTTPException(status_code=403, detail="admin only")
        return types.SimpleNamespace(user_id="admin"), True

    monkeypatch.setattr(api, "_resolve_customer_access", _access)
    monkeypatch.setattr(api, "_resolve_user_and_guard", _guard)
    monkeypatch.setattr(user_confirmations, "submit", store.submit)
    monkeypatch.setattr(user_confirmations, "list_for_analysis", store.list_for_analysis)
    monkeypatch.setattr(user_confirmations, "list_all_for_analysis", store.list_all_for_analysis)
    monkeypatch.setattr(user_confirmations, "audit_for_analysis", store.audit_for_analysis)

    app = FastAPI()
    app.include_router(api.router, prefix="/api")
    return app, state, store


def _save(job_id, analysis_id):
    artifacts.save_job_status(job_id, {"job_id": job_id, "analysis_id": analysis_id,
        "status": "REPORT_READY", "safe_to_show_customer": True, "artifacts_saved": {}})
    artifacts.save_customer_report(job_id, _report(analysis_id, job_id))


def _req(app, method, path, json=None):
    async def _do():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            return await c.request(method, path, json=json)
    return asyncio.run(_do())


def _eligible_finding_id(analysis_id, job_id):
    rep = _report(analysis_id, job_id)
    model = decision_model.build_decision_model(rep, [])
    for f in model["findings"]:
        if (f.get("confirmation") or {}).get("eligible"):
            return f["finding_id"]
    raise AssertionError("no eligible finding in fixture")


# 30. confirm-finding: owner ok; zero jobs spawned; response carries refreshed report
def test_confirm_finding_owner_ok_zero_jobs(app_env):
    app, state, store = app_env
    analysis_id, job_id = "analysis_conf", "cv2_conf"
    _save(job_id, analysis_id)
    fid = _eligible_finding_id(analysis_id, job_id)
    jobs_before = set(artifacts.list_jobs())

    resp = _req(app, "POST",
        f"/api/analysis/perizia/{analysis_id}/correctness-v2/customer-view/confirm-finding",
        json={"job_id": job_id, "finding_id": fid, "option_id": "gia_compreso"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["available"] is True
    # confirmation joined into the refreshed decision model
    conferme = data["report"]["decision_model"]["sections"].get("conferme")
    assert conferme and conferme["items"][0]["finding_id"] == fid
    assert "Confermato dall'utente" in conferme["items"][0]["wording"]
    # PURE: opening/confirming spawned no new job
    assert set(artifacts.list_jobs()) == jobs_before
    assert len(store.rows) == 1


# 30b. invalid option rejected 400; unknown finding rejected 400
def test_confirm_finding_invalid(app_env):
    app, state, store = app_env
    analysis_id, job_id = "analysis_conf2", "cv2_conf2"
    _save(job_id, analysis_id)
    fid = _eligible_finding_id(analysis_id, job_id)
    bad_opt = _req(app, "POST",
        f"/api/analysis/perizia/{analysis_id}/correctness-v2/customer-view/confirm-finding",
        json={"job_id": job_id, "finding_id": fid, "option_id": "bogus"})
    assert bad_opt.status_code == 400
    bad_find = _req(app, "POST",
        f"/api/analysis/perizia/{analysis_id}/correctness-v2/customer-view/confirm-finding",
        json={"job_id": job_id, "finding_id": "nope-000", "option_id": "gia_compreso"})
    assert bad_find.status_code == 400


# 31. non-owner denied; GET confirmations owner-only; decision-model admin-only
def test_ownership_and_admin_gates(app_env):
    app, state, store = app_env
    analysis_id, job_id = "analysis_gate", "cv2_gate"
    _save(job_id, analysis_id)
    fid = _eligible_finding_id(analysis_id, job_id)

    # non-owner (access resolver denies) -> 404
    state["user_id"] = None
    denied = _req(app, "POST",
        f"/api/analysis/perizia/{analysis_id}/correctness-v2/customer-view/confirm-finding",
        json={"job_id": job_id, "finding_id": fid, "option_id": "gia_compreso"})
    assert denied.status_code == 404

    # owner GET confirmations
    state["user_id"] = "owner"
    _req(app, "POST",
        f"/api/analysis/perizia/{analysis_id}/correctness-v2/customer-view/confirm-finding",
        json={"job_id": job_id, "finding_id": fid, "option_id": "gia_compreso"})
    got = _req(app, "GET",
        f"/api/analysis/perizia/{analysis_id}/correctness-v2/customer-view/confirmations")
    assert got.status_code == 200
    assert got.json()["confirmations"][0]["finding_id"] == fid

    # decision-model route: non-admin 403, admin ok
    forbidden = _req(app, "GET",
        f"/api/analysis/perizia/{analysis_id}/correctness-v2/jobs/{job_id}/decision-model")
    assert forbidden.status_code == 403
    state["admin_ok"] = True
    admin = _req(app, "GET",
        f"/api/analysis/perizia/{analysis_id}/correctness-v2/jobs/{job_id}/decision-model")
    assert admin.status_code == 200
    assert admin.json()["decision_model"]["schema_version"] == "cv2.customer_decision.v1"


# 32. cross-analysis job rejected (a confirmation can never target another analysis)
def test_cross_analysis_rejected(app_env):
    app, state, store = app_env
    _save("cv2_a", "analysis_a")
    fid = _eligible_finding_id("analysis_a", "cv2_a")
    # job cv2_a belongs to analysis_a, but URL claims analysis_b
    resp = _req(app, "POST",
        "/api/analysis/perizia/analysis_b/correctness-v2/customer-view/confirm-finding",
        json={"job_id": "cv2_a", "finding_id": fid, "option_id": "gia_compreso"})
    assert resp.status_code == 404
