"""Report-clarity + Italian-first bilingual branch — focused offline tests.

All tests are offline/deterministic. Gemini is MOCKED — no real paid calls.
"""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest
from fastapi import FastAPI

from correctness_v2 import api, artifacts, decision_model, feature_flags, translation


# ---------------------------------------------------------------------------
# Feature flag
# ---------------------------------------------------------------------------
def test_report_clarity_flag_default_off(monkeypatch):
    monkeypatch.delenv(feature_flags.FLAG_REPORT_CLARITY, raising=False)
    monkeypatch.setenv(feature_flags.FLAG_CANONICAL_VERDICT, "true")
    assert feature_flags.report_clarity_enabled() is False


def test_report_clarity_requires_canonical(monkeypatch):
    monkeypatch.setenv(feature_flags.FLAG_REPORT_CLARITY, "true")
    monkeypatch.setenv(feature_flags.FLAG_CANONICAL_VERDICT, "false")
    assert feature_flags.report_clarity_enabled() is False
    monkeypatch.setenv(feature_flags.FLAG_CANONICAL_VERDICT, "true")
    assert feature_flags.report_clarity_enabled() is True


# ---------------------------------------------------------------------------
# _build_conflicts — projection of authoritative verdict fields (P3)
# ---------------------------------------------------------------------------
def _verdict(conflicts=None, occupancy_value="occupato", occupancy_conf="high",
             typology_conf="high"):
    return {
        "conflicts": conflicts or [],
        "field_verdicts": {
            "occupancy": {"value": occupancy_value, "confidence": occupancy_conf},
            "typology": {"value": "Appartamento", "confidence": typology_conf},
            "money": {"final_value": {"value": 100000, "confidence": "high"}},
        },
    }


def test_conflicts_projected_to_in_conflitto_findings():
    v = _verdict(conflicts=[
        {"path": "drivers", "case_value": "1", "lot_value": "2",
         "reason_code": "POSSIBLE_CROSS_LOT_LEAKAGE"},
    ])
    findings = decision_model._build_conflicts({}, v, "4", set())
    conflicts = [f for f in findings if f["status"] == "in_conflitto"]
    assert len(conflicts) == 1
    assert conflicts[0]["status_label"] == "In conflitto tra le fonti"
    # No raw reason_code / path enum leaks into any rendered field.
    blob = json.dumps(conflicts[0], ensure_ascii=False)
    assert "POSSIBLE_CROSS_LOT_LEAKAGE" not in blob
    assert "drivers" not in conflicts[0]["title"]


def test_no_conflicts_yields_no_in_conflitto():
    findings = decision_model._build_conflicts({}, _verdict(conflicts=[]), "4", set())
    assert not [f for f in findings if f["status"] == "in_conflitto"]


def test_unknown_occupancy_yields_single_incerto_no_duplicate():
    v = _verdict(occupancy_value="UNKNOWN", occupancy_conf="low")
    findings = decision_model._build_conflicts({}, v, "4", set())
    incerti = [f for f in findings if f["topic"] == "occupancy"]
    assert len(incerti) == 1
    assert incerti[0]["status"] == "non_determinabile"


def test_incerto_skipped_when_topic_already_present():
    v = _verdict(occupancy_value="UNKNOWN", occupancy_conf="low")
    findings = decision_model._build_conflicts({}, v, "4", {"occupancy"})
    assert not [f for f in findings if f["topic"] == "occupancy"]


def test_low_confidence_leaf_is_incerto_never_green():
    v = _verdict(typology_conf="low")
    findings = decision_model._build_conflicts({}, v, "4", set())
    typ = [f for f in findings if f["topic"] == "typology"]
    assert len(typ) == 1
    assert typ[0]["status"] == "non_determinabile"
    assert typ[0]["tone"] != "verde"  # invariant 8: omission never reassuring


def test_build_conflicts_fail_closed_on_malformed():
    assert decision_model._build_conflicts({}, None, "4", set()) == []
    assert decision_model._build_conflicts({}, {"conflicts": "garbage"}, "4", set()) == []
    assert decision_model._build_conflicts({}, {"field_verdicts": 5}, "4", set()) == []


# ---------------------------------------------------------------------------
# Translation — static / cache / passthrough / failure-harmless (Gemini MOCKED)
# ---------------------------------------------------------------------------
def test_static_translate_deterministic():
    assert translation.static_translate("Da verificare") == "To be verified"
    assert translation.static_translate("  da  verificare ") == "To be verified"
    assert translation.static_translate("qualcosa di non censito") is None


def test_validate_translation_preserves_numbers_and_language():
    assert translation.validate_translation("€ 1.000 pagina 3", "€ 1.000 page 3") == []
    assert "numbers_changed" in translation.validate_translation("€ 1.000", "€ 2.000 total")
    assert "empty" in translation.validate_translation("ciao", "")
    assert "wrong_language" in translation.validate_translation(
        "non dichiarato dalla perizia oggi", "non dichiarato dalla perizia oggi valore"
    )


async def _english_mock(calls):
    async def _mock(prompt, system_instruction):
        calls["n"] += 1
        span = prompt.split("ITALIAN:\n")[-1]
        # Deterministic pseudo-English that preserves digits (passes validator).
        import re
        digits = re.findall(r"\d+", span)
        return "the verified item " + " ".join(digits)
    return _mock


def test_translation_cache_reuse_no_second_gemini_call():
    calls = {"n": 0}
    mock = asyncio.run(_english_mock(calls))
    sources = ["La perizia segnala una difformita 2 da chiarire con il tecnico"]
    tr, cache, stats = asyncio.run(
        translation.translate_texts(sources, translator=mock, cache=None)
    )
    assert stats["gemini"] == 1 and len(tr) == 1
    first_calls = calls["n"]
    tr2, cache2, stats2 = asyncio.run(
        translation.translate_texts(sources, translator=mock, cache=cache)
    )
    assert stats2["cache"] == 1 and stats2["gemini"] == 0
    assert calls["n"] == first_calls  # no additional Gemini call for same source+version


def test_translation_cache_version_fence():
    stale = {"glossary_prompt_version": "OLD", "entries": {"x": {"source": "s", "en": "e"}}}
    assert translation._cache_entries(stale) == {}


def test_translation_failure_harmless_italian_intact():
    async def _boom(prompt, system_instruction):
        raise RuntimeError("gemini_timeout")
    sources = ["Una frase lunga che richiede una traduzione dinamica dal modello"]
    tr, cache, stats = asyncio.run(
        translation.translate_texts(sources, translator=_boom, cache=None)
    )
    assert tr == []                      # English omitted
    assert stats["failed"] == 1          # counted, never raised
    assert cache["entries"] == {}        # nothing cached on failure


def test_translation_static_never_calls_gemini():
    calls = {"n": 0}

    async def _mock(prompt, system_instruction):
        calls["n"] += 1
        return "should-not-be-called"

    tr, cache, stats = asyncio.run(
        translation.translate_texts(["Da verificare"], translator=_mock, cache=None)
    )
    assert stats["static"] == 1 and calls["n"] == 0
    assert tr == [{"source": "Da verificare", "en": "To be verified"}]


def test_collect_dynamic_strings_excludes_passthrough():
    model = {
        "sections": {
            "numeri": {
                "catena": [{"label": "Prezzo", "amount_display": "€ 1.000"}],
                "da_chiarire": [{"label": "X", "amount_display": "€ 2.000",
                                 "motivo": "Motivo da tradurre lungo abbastanza"}],
            },
            "occupazione": {"dettaglio": "Immobile occupato senza titolo opponibile noto"},
        },
        "findings": [
            {"title": "Difformita edilizia", "customer_summary": "Sintesi cliente da tradurre",
             "buyer_impact": "", "evidence": {"excerpt": "€ 1.234 citazione verbatim di pagina"}},
        ],
    }
    strings = translation.collect_dynamic_strings(model, None)
    # amount_display strings are passthrough and never collected.
    assert "€ 1.000" not in strings
    assert "Motivo da tradurre lungo abbastanza" in strings
    assert "Sintesi cliente da tradurre" in strings
    assert any("citazione verbatim" in s for s in strings)


# ---------------------------------------------------------------------------
# narrator signature backward compatibility (§12.C)
# ---------------------------------------------------------------------------
def test_narrator_gemini_signature_backward_compatible():
    import inspect
    import narrator
    sig = inspect.signature(narrator._call_gemini_narrator_llm)
    assert "system_instruction" in sig.parameters
    default = sig.parameters["system_instruction"].default
    assert default == narrator._GEMINI_NARRATOR_SYSTEM_PROMPT


# ---------------------------------------------------------------------------
# Grep-enforceable invariant: English/translation NEVER feeds any
# severity/status/readiness/disclosure computation (§12.B). The modules that
# compute those must not import the translation layer.
# ---------------------------------------------------------------------------
def test_severity_modules_never_import_translation():
    from pathlib import Path
    import correctness_v2
    base = Path(correctness_v2.__file__).parent
    for module_name in ("verdict_model", "decision_model", "customer_view", "partial_report"):
        src = (base / f"{module_name}.py").read_text(encoding="utf-8")
        assert "import translation" not in src, module_name
        assert "translation." not in src, module_name


# ---------------------------------------------------------------------------
# Translate endpoint — auth/ownership gate, flag gating, quota-exempt, fail-soft
# ---------------------------------------------------------------------------
@pytest.fixture()
def translate_app(monkeypatch):
    async def _allow(request, analysis_id):
        return type("U", (), {"user_id": "owner"})(), False

    monkeypatch.setattr(api, "_resolve_customer_access", _allow)
    app = FastAPI()
    app.include_router(api.router, prefix="/api")
    return app


def _post(app, path, body):
    async def _go():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.post(path, json=body)
    return asyncio.run(_go())


def _save_ready(job_id, analysis_id):
    artifacts.save_job_status(job_id, {
        "job_id": job_id, "analysis_id": analysis_id, "status": "REPORT_READY",
        "safe_to_show_customer": True, "artifacts_saved": {},
    })
    artifacts.save_customer_report(job_id, {
        "schema_version": "cv2.customer_report.v1",
        "analysis_id": analysis_id, "job_id": job_id,
        "report_status": "REPORT_READY", "title": "Report",
        "occupancy_section": {"status": "Occupato", "notes": "Immobile occupato senza titolo opponibile registrato"},
        "money_sections": {"valuation_chain": [{"label": "Valore", "amount_display": "€ 100,00"}]},
    })


_PATH = "/api/analysis/perizia/{aid}/correctness-v2/customer-view/translate"


def test_translate_flag_off_returns_empty_no_gemini(translate_app, artifacts_root, monkeypatch):
    monkeypatch.setenv(feature_flags.FLAG_CANONICAL_VERDICT, "true")
    monkeypatch.delenv(feature_flags.FLAG_REPORT_CLARITY, raising=False)
    _save_ready("clarity_job_off", "aid_off")

    called = {"n": 0}

    async def _tr(prompt, si):
        called["n"] += 1
        return "x"
    monkeypatch.setattr(translation, "default_gemini_translator", _tr)

    resp = _post(translate_app, _PATH.format(aid="aid_off"), {"job_id": "clarity_job_off"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["available"] is False and data["reason_code"] == "REPORT_CLARITY_DISABLED"
    assert data["translations"] == []
    assert called["n"] == 0  # no Gemini path when flag off


def test_translate_flag_on_returns_translations_and_caches(translate_app, artifacts_root, monkeypatch):
    monkeypatch.setenv(feature_flags.FLAG_CANONICAL_VERDICT, "true")
    monkeypatch.setenv(feature_flags.FLAG_REPORT_CLARITY, "true")
    _save_ready("clarity_job_on", "aid_on")

    called = {"n": 0}

    async def _tr(prompt, si):
        called["n"] += 1
        import re
        span = prompt.split("ITALIAN:\n")[-1]
        # A realistic mock preserves negation (the validator now enforces it).
        neg = "not " if re.search(r"(?i)(?<![a-z])(non|senza)(?![a-z])", span) else ""
        return "the property is " + neg + "occupied " + " ".join(re.findall(r"\d+", span))
    monkeypatch.setattr(translation, "default_gemini_translator", _tr)

    resp = _post(translate_app, _PATH.format(aid="aid_on"), {"job_id": "clarity_job_on"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["available"] is True and data["language"] == "en"
    assert data["glossary_prompt_version"] == translation.GLOSSARY_PROMPT_VERSION
    # Cache persisted → a second request makes no additional Gemini call.
    first = called["n"]
    assert first >= 1
    resp2 = _post(translate_app, _PATH.format(aid="aid_on"), {"job_id": "clarity_job_on"})
    assert resp2.status_code == 200
    assert called["n"] == first  # served from the owner-scoped cache


def test_translate_wrong_job_owner_mismatch_404(translate_app, artifacts_root, monkeypatch):
    monkeypatch.setenv(feature_flags.FLAG_CANONICAL_VERDICT, "true")
    monkeypatch.setenv(feature_flags.FLAG_REPORT_CLARITY, "true")
    _save_ready("clarity_job_x", "aid_real")
    # job belongs to a different analysis than the path → 404
    resp = _post(translate_app, _PATH.format(aid="other_aid"), {"job_id": "clarity_job_x"})
    assert resp.status_code == 404


def test_translate_gemini_failure_is_harmless(translate_app, artifacts_root, monkeypatch):
    monkeypatch.setenv(feature_flags.FLAG_CANONICAL_VERDICT, "true")
    monkeypatch.setenv(feature_flags.FLAG_REPORT_CLARITY, "true")
    _save_ready("clarity_job_fail", "aid_fail")

    async def _boom(prompt, si):
        raise RuntimeError("gemini down")
    monkeypatch.setattr(translation, "default_gemini_translator", _boom)

    resp = _post(translate_app, _PATH.format(aid="aid_fail"), {"job_id": "clarity_job_fail"})
    assert resp.status_code == 200  # never 500 — Italian report unaffected
    data = resp.json()
    assert data["available"] is True
    # No cache file was written on an all-failed translation.
    assert artifacts.read_translation_cache("clarity_job_fail") is None


# ---------------------------------------------------------------------------
# Repair-cycle: static glossary completeness (enum labels never hit Gemini)
# ---------------------------------------------------------------------------
def test_enum_labels_resolve_statically_not_dynamic():
    # Area-group + conflict/incerto fixed labels must resolve from the reviewed
    # dictionary (deterministic), never fall through to the Gemini batch.
    for it in (
        "Edilizia", "Catastale", "Urbanistica", "Impianti — gas",
        "Impianti — elettrico", "Agibilità/abitabilità", "APE/energia",
        "Le fonti della perizia riportano valori discordanti: è necessaria una verifica.",
        "Elemento discordante tra le fonti della perizia: da verificare.",
        "La tipologia dell'immobile non è determinabile con certezza dalla sola perizia.",
    ):
        assert translation.static_translate(it), f"missing static: {it}"

    # A decision_model whose findings carry these fixed labels must NOT surface
    # them in the dynamic (Gemini) collection.
    model = {
        "sections": {},
        "findings": [
            {"title": "Edilizia", "customer_summary": "Le fonti della perizia riportano valori discordanti: è necessaria una verifica."},
            {"title": "APE/energia", "customer_summary": ""},
        ],
    }
    dynamic = translation.collect_dynamic_strings(model)
    for label in ("Edilizia", "APE/energia",
                  "Le fonti della perizia riportano valori discordanti: è necessaria una verifica."):
        assert label not in dynamic


# ---------------------------------------------------------------------------
# Repair-cycle: negation-preservation heuristic (backstops attacks #14/#16)
# ---------------------------------------------------------------------------
def test_validate_translation_rejects_dropped_negation():
    # Italian negation with no English negation → rejected (→ Italian fallback).
    errs = translation.validate_translation(
        "L'immobile non è conforme alle norme urbanistiche.",
        "The property is compliant with urban planning rules.",
    )
    assert "negation_dropped" in errs

    errs2 = translation.validate_translation(
        "Immobile senza agibilità.",
        "Property with valid habitability certificate.",
    )
    assert "negation_dropped" in errs2


def test_validate_translation_accepts_preserved_negation():
    errs = translation.validate_translation(
        "L'immobile non è conforme alle norme urbanistiche.",
        "The property is not compliant with urban planning rules.",
    )
    assert "negation_dropped" not in errs
    # A source without negation is unaffected by the heuristic.
    errs2 = translation.validate_translation(
        "Valore di mercato indicato dalla perizia.",
        "Market value indicated by the appraisal.",
    )
    assert errs2 == []


# ---------------------------------------------------------------------------
# Presentation-only conflict floor (Fable finding #3, owner-approved)
# esito.level / readiness / severity / CanonicalVerdict are NEVER changed —
# only a DISPLAY hint (+ deterministic qualified bilingual message) is added.
# ---------------------------------------------------------------------------
_FLOOR_IT = ("Nessun blocco automatico rilevato, ma sono presenti informazioni "
             "in conflitto da verificare.")


def _model_with_conflicts(monkeypatch, *, conflicts, clarity=True):
    monkeypatch.setenv(feature_flags.FLAG_CANONICAL_VERDICT, "true")
    if clarity:
        monkeypatch.setenv(feature_flags.FLAG_REPORT_CLARITY, "true")
    else:
        monkeypatch.delenv(feature_flags.FLAG_REPORT_CLARITY, raising=False)
    v = _verdict(conflicts=conflicts)
    v["severity"] = "verde"
    v["canonical_ref"] = "cv:test"
    monkeypatch.setattr(decision_model.verdict_model, "try_validate_verdict", lambda x: v)
    monkeypatch.setattr(decision_model.verdict_model, "build_lot_verdict", lambda report: v)
    monkeypatch.setattr(decision_model.verdict_model, "refine_for_runtime", lambda base, **k: v)
    monkeypatch.setattr(decision_model.verdict_model, "project_to_esito_level", lambda cv: "verde")
    report = {
        "report_status": "REPORT_READY",
        "lot_structure": {"selected_lot": "1"},
        "canonical_verdict": {"schema_version": "x"},
        "compliance_section": [], "money_sections": {}, "occupancy_section": {},
        "formalities_section": [], "risk_sections": [],
    }
    return decision_model.build_decision_model(report, [])


_CONFLICT_ROW = [{"path": "compliance.edilizia", "case_value": "conforme",
                  "lot_value": "non_conforme", "reason_code": "CONFLICT_REQUIRES_REVIEW"}]


def test_floor_1_no_conflicts_keeps_green(monkeypatch):
    m = _model_with_conflicts(monkeypatch, conflicts=[])
    assert m["esito"]["level"] == "verde"
    assert "clarity_conflict_floor" not in m["esito"]  # green summary allowed


def test_floor_2_conflict_shows_qualified_amber(monkeypatch):
    m = _model_with_conflicts(monkeypatch, conflicts=_CONFLICT_ROW)
    esito = m["esito"]
    assert esito["clarity_conflict_floor"] is True          # amber floor displayed
    assert esito["clarity_summary_it"] == _FLOOR_IT          # deterministic qualified message


def test_floor_3_in_conflitto_card_still_visible(monkeypatch):
    m = _model_with_conflicts(monkeypatch, conflicts=_CONFLICT_ROW)
    assert "conflitti" in m["sections"]                      # card remains authoritative


def test_floor_4_5_6_7_semantics_unchanged(monkeypatch):
    m = _model_with_conflicts(monkeypatch, conflicts=_CONFLICT_ROW)
    assert m["report_status"] == "REPORT_READY"              # 4 REPORT_READY unchanged
    # 5 readiness unchanged vs the no-conflict baseline
    base = _model_with_conflicts(monkeypatch, conflicts=[])
    assert m["readiness"] == base["readiness"]
    assert m["esito"]["canonical_severity"] == "verde"       # 6 canonical severity unchanged
    assert m["esito"]["level"] == "verde"                    # 7/esito.level never mutated


def test_floor_11_message_is_static_zero_gemini(monkeypatch):
    # The fixed bilingual message resolves via the deterministic glossary — no Gemini.
    assert translation.static_translate(_FLOOR_IT) == (
        "No automatic blocking issue was detected, but conflicting information requires verification.")


def test_floor_12_flag_off_no_floor(monkeypatch):
    m = _model_with_conflicts(monkeypatch, conflicts=_CONFLICT_ROW, clarity=False)
    assert "clarity_conflict_floor" not in m["esito"]        # flag OFF == today
    assert "clarity_enabled" not in m


def test_validate_translation_accepts_english_contraction_negation():
    # Real contractions (isn't/doesn't/wasn't/can't) must NOT be flagged as
    # negation_dropped (Fable focused-confirmation blocker).
    for en in ("The property isn't compliant.", "The document doesn't result available.",
               "It wasn't declared.", "The buyer can't proceed without checks."):
        assert "negation_dropped" not in translation.validate_translation(
            "L'immobile non è conforme.", en)
    # A genuinely dropped negation is still caught.
    assert "negation_dropped" in translation.validate_translation(
        "L'immobile non è conforme.", "The property is compliant.")
