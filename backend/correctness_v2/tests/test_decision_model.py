"""Tests for the deterministic customer decision model (§C–M, plan Part 24 #1-20)."""

import json

import pytest

from correctness_v2 import decision_model as dm

_FORBIDDEN_TOKENS = (
    "LOW_CONFIDENCE",
    "USER_PROVIDED",
    "MONEY_ROLE_CONFLICT",
    "MANUAL_REVIEW",
    "manual_review",
    "confidence",
    "quality_control",
    "admin_evidence_index",
    "openai",
    "gpt",
    "regularizable",  # raw English classification token must not leak as a label
)


def _report(**overrides):
    base = {
        "schema_version": "cv2.customer_report.v1",
        "analysis_id": "analysis_test",
        "job_id": "cv2_test",
        "report_status": "REPORT_READY",
        "case_identity": {
            "tribunale": "Tribunale di Esempio",
            "procedura_rge": "1/2025",
            "lotto": "LOTTO 1",
            "address": "Via Esempio 1",
            "property_type": "appartamento",
            "ownership_right": "1/1 piena proprietà",
            "evidence_pages": [1, 2],
        },
        "lot_structure": {"selected_lot": "1", "lot_count": 1, "bene_count": 1},
        "beni_sections": [
            {"bene_id": "principale", "title": "Bene principale: appartamento",
             "is_main_property": True, "property_type": "appartamento",
             "address": "Via Esempio 1", "evidence_pages": [1], "accessories": []}
        ],
        "occupancy_section": {},
        "compliance_section": [],
        "formalities_section": [],
        "buyer_checklist": [],
        "risk_sections": [],
        "customer_evidence_index": [],
        "money_sections": {
            "valuation_chain": [],
            "buyer_side_costs": [],
            "procedure_cancelled_formalities": [],
            "market_comparatives": [],
            "context_values": [],
            "uncertain_money": [],
            "auction_terms": [],
        },
    }
    base.update(overrides)
    return base


def _chain(rows):
    return [
        {"label": lbl, "amount": amt, "amount_display": dm.format_eur(amt), "kind": kind,
         "evidence_pages": pages}
        for (lbl, amt, kind, pages) in rows
    ]


# 1. schema envelope: version, section keys omitted when empty
def test_schema_envelope_and_empty_sections_omitted():
    m = dm.build_decision_model(_report(), [])
    assert m["schema_version"] == "cv2.customer_decision.v1"
    # No money/occupancy/formalities content -> those keys are absent.
    assert "numeri" not in m["sections"]
    assert "occupazione" not in m["sections"]
    assert "formalita" not in m["sections"]
    # A ready report with real identity always has an acquisto section.
    assert "acquisto" in m["sections"]
    assert "stato_verifiche" in m["sections"]


# 2. finding_id stability across two builds of the same artifact
def test_finding_id_stable():
    rep = _report(compliance_section=[
        {"area": "conformità edilizia", "classification": "regularizable",
         "evidence_pages": [7], "notes": "x", "cost": 100.0}])
    a = dm.build_decision_model(rep, [])
    b = dm.build_decision_model(rep, [])
    assert [f["finding_id"] for f in a["findings"]] == [f["finding_id"] for f in b["findings"]]
    assert a["findings"][0]["finding_id"].startswith("cmp-")


# 3. esito verde when nothing is actionable
def test_esito_verde_when_nothing_actionable():
    rep = _report(compliance_section=[
        {"area": "conformità urbanistica", "classification": "conforming", "evidence_pages": [8],
         "notes": "Nessuna difformità."}])
    m = dm.build_decision_model(rep, [])
    assert m["esito"]["level"] == "verde"
    assert m["esito"]["headline"] == "Nessuna verifica bloccante emersa dalla perizia"


# 4. esito ambra with an open action
def test_esito_ambra_with_open_action():
    rep = _report(compliance_section=[
        {"area": "conformità edilizia", "classification": "regularizable", "evidence_pages": [7],
         "notes": "difformità", "cost": 500.0}])
    m = dm.build_decision_model(rep, [])
    assert m["esito"]["level"] == "ambra"
    assert m["esito"]["headline"] == "Verifiche necessarie prima di procedere"


# 5. esito rosso only for fail-closed, never for REPORT_READY
def test_esito_rosso_only_fail_closed():
    ready = dm.build_decision_model(_report(), [])
    assert ready["esito"]["level"] != "rosso"
    failed = dm.build_decision_model(_report(report_status="CONTRACT_VALIDATION_FAILED"), [])
    assert failed["esito"]["level"] == "rosso"
    assert failed["findings"] == []
    assert list(failed["sections"].keys()) == ["stato_verifiche"]


# 6. drivers <=5, deduped, no counts/enums in any customer string
def test_drivers_capped_and_clean():
    comp = [{"area": f"conformità edilizia {i}", "classification": "regularizable",
             "evidence_pages": [7], "notes": "x", "cost": 100.0} for i in range(8)]
    m = dm.build_decision_model(_report(compliance_section=comp), [])
    assert len(m["esito"]["drivers"]) <= 5
    blob = json.dumps(m["esito"], ensure_ascii=False)
    assert "punti di attenzione" not in blob and "evidenze" not in blob


# 7. chain canonical reorder by arithmetic fit (Codogno-shaped)
def test_chain_reorder_arithmetic_fit():
    rep = _report(money_sections={**_report()["money_sections"], "valuation_chain": _chain([
        ("Valore di mercato", 452494.0, "value", [1]),
        ("Riduzione 15%", 67874.10, "deduction", [2]),
        ("Costo cancellazione", 14000.0, "deduction", [3]),
        ("Valore nello stato di fatto", 384619.90, "value", [2]),
        ("Valore di vendita", 370619.90, "value", [3]),
    ])})
    m = dm.build_decision_model(rep, [])
    kinds = [(r["kind"], r["amount"]) for r in m["sections"]["numeri"]["catena"]]
    assert kinds == [
        ("value", 452494.0), ("deduction", 67874.10), ("value", 384619.90),
        ("deduction", 14000.0), ("value", 370619.90),
    ]
    assert m["sections"]["numeri"]["catena"][-1]["terminal"] is True


# 8. chain with missing terminal (Pistoia-shaped): no fabricated sale row
def test_chain_missing_terminal_no_fabrication():
    rep = _report(money_sections={**_report()["money_sections"], "valuation_chain": _chain([
        ("Valore di mercato", 280336.0, "value", [1]),
        ("Riduzione", 56067.0, "deduction", [2]),
        ("Valore nello stato di fatto", 224269.0, "value", [2]),
    ])})
    m = dm.build_decision_model(rep, [])
    catena = m["sections"]["numeri"]["catena"]
    assert len(catena) == 3
    assert all(r["kind"] in ("value", "deduction") for r in catena)


# 9. included buyer cost -> "non sommare nuovamente", no third card
def test_included_buyer_cost_note():
    rep = _report(money_sections={**_report()["money_sections"],
        "valuation_chain": _chain([("Valore", 100.0, "value", [1])]),
        "buyer_side_costs": [{"label": "Costo cancellazione", "amount": 10.0,
            "amount_display": "€ 10,00", "included_in_valuation": True}]})
    m = dm.build_decision_model(rep, [])
    costi = m["sections"]["numeri"]["costi_potenziali"]
    assert costi[0]["included_in_valuation"] is True
    assert "non sommare nuovamente" in costi[0]["nota"]


# 10. comparatives -> single {count, pages} summary; none in da_chiarire
def test_comparatives_single_line():
    rep = _report(money_sections={**_report()["money_sections"],
        "valuation_chain": _chain([("Valore", 100.0, "value", [1])]),
        "market_comparatives": [{"label": "OMI", "evidence_pages": [5]},
                                {"label": "Annuncio", "evidence_pages": [6]}]})
    m = dm.build_decision_model(rep, [])
    numeri = m["sections"]["numeri"]
    assert numeri["comparatives_summary"] == {"count": 2, "pages": [5, 6]}
    assert "da_chiarire" not in numeri


# 11. uncertain money row -> da_chiarire + confirmation-eligible money finding
def test_uncertain_money_confirmation():
    rep = _report(
        customer_evidence_index=[{"page": 38, "topic": "Oblazione", "report_section": "Costi",
            "perizia_excerpt": "oblazione indicativa art. 36 bis pari a 1032 euro",
            "coverage_status": "covered"}],
        money_sections={**_report()["money_sections"],
            "valuation_chain": _chain([("Valore", 100.0, "value", [1])]),
            "uncertain_money": [{"label": "Oblazione art. 36 bis", "amount": 1032.0,
                "amount_display": "€ 1.032,00", "kind": "uncertain", "evidence_pages": [38],
                "reason": "Importo indicativo da verificare."}]})
    m = dm.build_decision_model(rep, [])
    money_findings = [f for f in m["findings"] if f["confirm_class"] == "money_role"]
    assert len(money_findings) == 1
    conf = money_findings[0]["confirmation"]
    assert conf["eligible"] is True
    assert 2 <= len(conf["options"]) <= 4
    assert conf["unsure_option"]["option_id"] == "non_sicuro"


# 12. mortgage/registered amount never a buyer cost without buyer_burden
def test_mortgage_not_buyer_cost():
    rep = _report(formalities_section=[
        {"type": "ipoteca", "type_label": "Ipoteca", "description": "Ipoteca 150000",
         "cancelled_by_procedure": True, "buyer_burden": False, "amount": 150000.0,
         "amount_display": "€ 150.000,00", "evidence_pages": [3],
         "amount_note": "Importo iscritto, non è un debito."}])
    m = dm.build_decision_model(rep, [])
    formalita = m["sections"]["formalita"]
    assert "cancellate" in formalita
    assert "costi_cancellazione" not in formalita
    # No money finding treats the mortgage as a buyer cost.
    assert all(f.get("amount") != 150000.0 for f in m["findings"])


# 13. compliance grouping + status map; "nessuna difformità" stays green
def test_compliance_status_map():
    rep = _report(compliance_section=[
        {"area": "conformità catastale", "classification": "regularizable", "evidence_pages": [7],
         "notes": "difformità", "cost": 750.0, "cost_display": "€ 750,00", "timing": "3 mesi"},
        {"area": "conformità urbanistica", "classification": "conforming", "evidence_pages": [8],
         "notes": "Nessuna difformità."},
        {"area": "impianto gas", "classification": "uncertain", "evidence_pages": [9], "notes": ""}])
    m = dm.build_decision_model(rep, [])
    by_topic = {f["topic"]: f for f in m["findings"] if f["section"] == "conformita"}
    assert by_topic["catastale"]["status_label"] == "Regolarizzabile secondo la perizia"
    assert by_topic["urbanistica"]["status_label"] == "Conforme secondo la perizia"
    assert by_topic["urbanistica"]["tone"] == "verde"
    assert by_topic["impianto_gas"]["status_label"] == "Non verificato o non dichiarato"


# 14. formality three-way split + summary/detail dedup; distinct amounts preserved
def test_formality_dedup_and_split():
    rep = _report(formalities_section=[
        {"type": "pignoramento", "type_label": "Pignoramento", "description": "Pignoramento A",
         "cancelled_by_procedure": True, "buyer_burden": False, "amount": None, "evidence_pages": [3]},
        {"type": "pignoramento", "type_label": "Pignoramento", "description": "Pignoramento B",
         "cancelled_by_procedure": True, "buyer_burden": False, "amount": None, "evidence_pages": [4]},
        {"type": "ipoteca", "type_label": "Ipoteca", "description": "Ipoteca 150k orig",
         "cancelled_by_procedure": True, "buyer_burden": False, "amount": 150000.0, "evidence_pages": [3]},
        {"type": "ipoteca", "type_label": "Ipoteca", "description": "Ipoteca 150k rinnovo",
         "cancelled_by_procedure": True, "buyer_burden": False, "amount": 150000.0, "evidence_pages": [3]}])
    m = dm.build_decision_model(rep, [])
    cancellate = m["sections"]["formalita"]["cancellate"]
    pign = [c for c in cancellate if c["type_label"] == "Pignoramento"]
    ipo = [c for c in cancellate if c["type_label"] == "Ipoteca"]
    assert len(pign) == 1  # two trivially-different descriptions collapse
    assert len(pign[0]["details"]) == 2
    assert len(ipo) == 1
    assert len(ipo[0]["details"]) == 2  # two distinct 150k formalities kept as detail lines


# 15. occupancy: "Opponibilità da verificare" when silent, no invented conclusion
def test_occupancy_opponibilita_default():
    rep = _report(occupancy_section={"status": "occupato", "status_label": "Occupato",
        "evidence_pages": [2], "title_info": "occupato con contratto", "risks": []})
    m = dm.build_decision_model(rep, [])
    occ = m["sections"]["occupazione"]
    assert any("Opponibilità" in c for c in occ["cosa_verificare"])
    assert occ["stato"] == "Occupato"


# 16. dedup: compliance>risk>checklist single canonical card; occupancy folded once
def test_dedup_single_occupancy_finding():
    rep = _report(occupancy_section={"status": "occupato", "status_label": "Occupato",
        "evidence_pages": [2], "title_info": "x", "risks": ["opponibilità"]},
        buyer_checklist=[{"action": "Valutare rischio occupazione", "detail": "y", "evidence_pages": [2]}])
    m = dm.build_decision_model(rep, [])
    occ_findings = [f for f in m["findings"] if f["section"] == "occupazione"]
    assert len(occ_findings) == 1


# 17. sources: surfaces pruned/collapsed, cap 8, excerpts verbatim from index
def test_sources_prune_and_cap():
    evidence = [{"page": p, "topic": f"Superficie {p}", "report_section": "Superfici e dati catastali",
                 "perizia_excerpt": f"mq {p}", "coverage_status": "covered"} for p in range(1, 12)]
    evidence.append({"page": 18, "topic": "Valore di vendita", "report_section": "Valutazione",
                     "perizia_excerpt": "valore di vendita giudiziaria 38110", "coverage_status": "covered"})
    rep = _report(customer_evidence_index=evidence)
    m = dm.build_decision_model(rep, [])
    fonti = m["sections"]["fonti"]
    assert len(fonti["primary"]) <= 8
    # the 11 surface micro-entries collapse into ONE line
    surface = [s for s in fonti["primary"] if s["title"] == "Superfici e dati catastali"]
    assert len(surface) == 1
    val = [s for s in fonti["primary"] if s["title"] == "Valore di vendita"][0]
    assert val["excerpt"] == "valore di vendita giudiziaria 38110"


# 17b. market comparatives are excluded from decisive evidence (fonti), not just
#      collapsed — they never crowd out identity/valuation/conformity sources.
def test_comparatives_excluded_from_decisive_evidence():
    evidence = [
        {"page": 2, "topic": "Valore di mercato", "report_section": "Valutazione",
         "perizia_excerpt": "valore di mercato 43654", "coverage_status": "covered"},
        {"page": 7, "topic": "conformità edilizia", "report_section": "Conformità",
         "perizia_excerpt": "difformità edilizia regolarizzabile", "coverage_status": "covered"},
    ] + [
        {"page": 16, "topic": f"Comparativo {i} OMI - prezzo unitario", "report_section": "Comparativi",
         "perizia_excerpt": f"comparativo {i} 1500/mq", "coverage_status": "covered"}
        for i in range(1, 6)
    ]
    m = dm.build_decision_model(_report(customer_evidence_index=evidence), [])
    fonti = m["sections"]["fonti"]
    assert not any(s["title"].lower().startswith("comparativ") for s in fonti["primary"])
    titles = " ".join(s["title"].lower() for s in fonti["primary"])
    assert "valore di mercato" in titles and "conformità edilizia" in titles


# 18. confirmation eligibility: excerpt required (else professional check), cap 5
def test_confirmation_requires_excerpt():
    # occupancy with a risk but NO matching evidence excerpt -> professional check, not a form
    rep = _report(occupancy_section={"status": "occupato", "status_label": "Occupato",
        "evidence_pages": [2], "title_info": "x", "risks": ["opponibilità"]})
    m = dm.build_decision_model(rep, [])
    occ = [f for f in m["findings"] if f["section"] == "occupazione"][0]
    assert "confirmation" not in occ
    assert occ.get("professional_check")


# 19. readiness state machine + label map; enum absent from any *_label
def test_readiness_states():
    ready_complete = dm.build_decision_model(_report(), [])
    assert ready_complete["readiness"]["state"] == "COMPLETE_FOR_EXPORT"
    assert ready_complete["readiness"]["label"] == "Pronto per l'esportazione"
    rep = _report(compliance_section=[{"area": "conformità edilizia", "classification": "regularizable",
        "evidence_pages": [7], "notes": "x", "cost": 100.0}])
    m = dm.build_decision_model(rep, [])
    assert m["readiness"]["state"] == "READY_FOR_REVIEW"
    # raw enum never appears in the customer label
    assert "READY_FOR_REVIEW" not in m["sections"]["stato_verifiche"]["label"]


def _occupancy_report(blocking=False):
    """A REPORT_READY report whose occupancy finding is confirmation-eligible."""
    return _report(
        customer_evidence_index=[{"page": 3, "topic": "occupazione", "report_section": "Occupazione",
            "perizia_excerpt": "immobile occupato con contratto di locazione", "coverage_status": "covered"}],
        occupancy_section={"status": "occupato", "status_label": "Occupato", "evidence_pages": [3],
            "title_info": "occupato con contratto", "risks": ["opponibilità da verificare"],
            **({"blocks_saleability": True} if blocking else {})},
    )


def _confirm(finding, option):
    ev = finding.get("evidence") or {}
    return [{
        "finding_id": finding["finding_id"], "selected_option": option, "selected_label": "scelta",
        "page": ev.get("page"), "status": "non_sicuro" if option == "non_sicuro" else "confermato_utente",
        "evidence_hash": dm.evidence_hash(ev.get("excerpt")), "updated_at": "t",
    }]


def _eligible(model):
    return next(f for f in model["findings"] if f.get("confirmation"))


# G1.1 ordinary "Non sono sicuro" -> amber, stays open (CONFIRMATIONS_REQUIRED)
def test_unsure_ordinary_stays_amber_and_open():
    base = dm.build_decision_model(_occupancy_report(), [])
    finding = _eligible(base)
    assert finding.get("blocking") is False
    m = dm.build_decision_model(_occupancy_report(), _confirm(finding, "non_sicuro"))
    f = next(x for x in m["findings"] if x["finding_id"] == finding["finding_id"])
    assert f["status"] == "da_verificare"        # unresolved, not escalated
    assert f.get("confirmation")                  # still offered -> counts as open + changeable
    assert m["readiness"]["state"] == "CONFIRMATIONS_REQUIRED"
    assert m["esito"]["level"] == "ambra"
    view = next(c for c in m["sections"]["conferme"]["items"] if c["finding_id"] == finding["finding_id"])
    assert "Non sono sicuro" in view["wording"] and view["status"] == "non_sicuro"


# G1.2 critical/blocking "Non sono sicuro" -> red (TECHNICAL_REVIEW_REQUIRED)
def test_unsure_blocking_stays_red():
    base = dm.build_decision_model(_occupancy_report(blocking=True), [])
    finding = next(f for f in base["findings"] if f.get("confirmation") and f.get("blocking"))
    m = dm.build_decision_model(_occupancy_report(blocking=True), _confirm(finding, "non_sicuro"))
    f = next(x for x in m["findings"] if x["finding_id"] == finding["finding_id"])
    assert f["status"] == "verifica_tecnica_richiesta"
    assert m["readiness"]["state"] == "TECHNICAL_REVIEW_REQUIRED"
    assert m["esito"]["level"] == "rosso"


# G1.3 fail-closed report stays red regardless of any confirmation
def test_unsure_fail_closed_stays_red():
    rep = _occupancy_report()
    rep["report_status"] = "CONTRACT_VALIDATION_FAILED"
    m = dm.build_decision_model(rep, [{"finding_id": "occ-x", "selected_option": "non_sicuro",
        "selected_label": "x", "page": 3, "status": "non_sicuro", "evidence_hash": "", "updated_at": "t"}])
    assert m["esito"]["level"] == "rosso"
    assert m["findings"] == []  # no fabricated customer report


# G1.4 changing a resolved answer back to "Non sono sicuro" reopens the confirmation
def test_change_resolved_back_to_unsure_reopens():
    finding = _eligible(dm.build_decision_model(_occupancy_report(), []))
    resolved = dm.build_decision_model(_occupancy_report(), _confirm(finding, "occupato_opponibile"))
    fr = next(x for x in resolved["findings"] if x["finding_id"] == finding["finding_id"])
    assert fr["status"] == "confermato_utente" and not fr.get("confirmation")
    reopened = dm.build_decision_model(_occupancy_report(), _confirm(finding, "non_sicuro"))
    fo = next(x for x in reopened["findings"] if x["finding_id"] == finding["finding_id"])
    assert fo["status"] == "da_verificare" and fo.get("confirmation")


# G1.5 red/amber never determined by the selected option alone (pure, no side effects)
def test_unsure_severity_not_from_option_and_pure():
    import ast
    finding = _eligible(dm.build_decision_model(_occupancy_report(), []))
    ordinary = dm.build_decision_model(_occupancy_report(), _confirm(finding, "non_sicuro"))["esito"]["level"]
    blocking_finding = next(f for f in dm.build_decision_model(_occupancy_report(True), []) ["findings"]
                            if f.get("confirmation") and f.get("blocking"))
    blocking = dm.build_decision_model(_occupancy_report(True), _confirm(blocking_finding, "non_sicuro"))["esito"]["level"]
    assert ordinary == "ambra" and blocking == "rosso"  # same option, different severity
    # builder performs no OpenAI/job/credit/network work (pure imports only)
    tree = ast.parse(open(dm.__file__).read())
    mods = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Import):
            mods.update(a.name for a in n.names)
        elif isinstance(n, ast.ImportFrom):
            mods.add(n.module or "")
    assert not any(x in m for m in mods for x in ("openai", "server", "stripe", "orchestrator", "requests", "httpx"))


# B1: a chain-excluded ambiguous deduction stays visible in numeri.da_chiarire,
#     and uncertain_money rows are NOT duplicated there (they are findings).
def test_da_chiarire_carries_excluded_rows_not_uncertain():
    rep = _report(
        customer_evidence_index=[{"page": 38, "topic": "Oblazione", "report_section": "Costi",
            "perizia_excerpt": "oblazione 1032", "coverage_status": "covered"}],
        money_sections={**_report()["money_sections"], "valuation_chain": _chain([
            # Pistoia-shaped: two deductions fit the segments, a trailing one does not.
            ("Valore di mercato", 622970.0, "value", [1]),
            ("Riduzione 55%", 280336.0, "deduction", [2]),   # fits gap0 (622970-342634)
            ("Valore intermedio", 342634.0, "value", [2]),
            ("Riduzione", 62298.0, "deduction", [3]),        # fits gap1 (342634-280336)
            ("Valore nello stato di fatto", 280336.0, "value", [3]),
            ("Deprezzamento 20%", 56068.0, "deduction", [4]),  # fits nothing -> excluded
        ]),
        "uncertain_money": [{"label": "Oblazione", "amount": 1032.0, "amount_display": "€ 1.032,00",
            "kind": "uncertain", "evidence_pages": [38], "reason": "indicativo"}]})
    m = dm.build_decision_model(rep, [])
    numeri = m["sections"]["numeri"]
    # the 56068 deduction fits no segment (the other two do), so it is excluded from
    # the chain and MUST remain visible in da_chiarire
    dc_amounts = {r["amount_display"] for r in numeri.get("da_chiarire", [])}
    assert "€ 56.068,00" in dc_amounts
    # the uncertain Oblazione is a confirmable finding, NOT duplicated into da_chiarire
    assert "€ 1.032,00" not in dc_amounts
    assert any(f["confirm_class"] == "money_role" for f in m["findings"])


# B3: an all-failed reorder is a pure passthrough — no row is both in the chain
#     and flagged ambiguous.
def test_reorder_all_failed_no_double_report():
    rows = _chain([
        ("V", 100000.0, "value", [1]),
        ("Ded", 5000.0, "deduction", [2]),   # 100000-90000=10000 != 5000
        ("V2", 90000.0, "value", [3]),
    ])
    ordered, ambiguous = dm._reorder_chain(rows)
    in_chain = {r["label"] for r in ordered}
    in_amb = {r["label"] for r in ambiguous}
    assert not (in_chain & in_amb)  # no row double-reported
    assert "Ded" in in_chain and not in_amb  # passthrough keeps it in the chain


# B2: interactive customer-safe statuses are amber (not the red technical-review
#     verdict); only a genuinely fail-closed status is red.
def test_interactive_statuses_are_amber_not_red():
    for status in ("MONEY_CONFIRMATION_REQUIRED", "LOT_SELECTION_REQUIRED", "DOCUMENT_NOT_READABLE"):
        m = dm.build_decision_model(_report(report_status=status), [])
        assert m["esito"]["level"] == "ambra", status
        assert m["readiness"]["state"] != "TECHNICAL_REVIEW_REQUIRED"
    failed = dm.build_decision_model(_report(report_status="NEEDS_MANUAL_REVIEW"), [])
    assert failed["esito"]["level"] == "rosso"


def _checklist_item(model, finding_id):
    for it in model["sections"].get("verifiche", {}).get("items", []):
        if it.get("finding_id") == finding_id:
            return it
    return None


def _consistency_holds(model):
    """No finding may be 'done' in one place and 'open' in the checklist."""
    by_id = {f["finding_id"]: f for f in model["findings"]}
    done = {"confermato_utente", "completato"}
    for it in model["sections"].get("verifiche", {}).get("items", []):
        fid = it.get("finding_id")
        if fid and fid in by_id:
            if (by_id[fid]["status"] in done) != (it["status"] in done):
                return False
    return True


# CK1: checklist is reconciled AFTER confirmations — a confirmed finding is
#      "Confermato dall'utente" in BOTH the checklist and the detail/conferme.
def test_checklist_confirmed_completed_in_both_places():
    base = dm.build_decision_model(_occupancy_report(), [])
    occ = next(f for f in base["findings"] if f["section"] == "occupazione")
    m = dm.build_decision_model(_occupancy_report(), _confirm(occ, "occupato_opponibile"))
    item = _checklist_item(m, occ["finding_id"])
    finding = next(f for f in m["findings"] if f["finding_id"] == occ["finding_id"])
    conferme = next(c for c in m["sections"]["conferme"]["items"] if c["finding_id"] == occ["finding_id"])
    assert finding["status"] == "confermato_utente"
    assert item["status"] == "confermato_utente" and item["status_label"] == "Confermato dall'utente"
    assert conferme["status"] == "confermato_utente"
    assert _consistency_holds(m)


# CK2: uncertain answer keeps both the checklist item and the finding OPEN.
def test_checklist_unsure_remains_open():
    base = dm.build_decision_model(_occupancy_report(), [])
    occ = next(f for f in base["findings"] if f["section"] == "occupazione")
    m = dm.build_decision_model(_occupancy_report(), _confirm(occ, "non_sicuro"))
    item = _checklist_item(m, occ["finding_id"])
    assert item["status"] in ("da_verificare", "conferma_necessaria")  # still open
    assert _consistency_holds(m)


# CK3: a blocking finding stays "Verifica tecnica richiesta" in the checklist,
#      a confirmation never marks it completed.
def test_checklist_blocking_stays_technical_review():
    base = dm.build_decision_model(_occupancy_report(blocking=True), [])
    occ = next(f for f in base["findings"] if f.get("confirmation") and f.get("blocking"))
    m = dm.build_decision_model(_occupancy_report(blocking=True), _confirm(occ, "non_sicuro"))
    item = _checklist_item(m, occ["finding_id"])
    assert item["status"] == "verifica_tecnica_richiesta"  # never completed by a confirmation
    assert _consistency_holds(m)


# CK4: a stale confirmation never marks the checklist complete.
def test_checklist_stale_remains_open():
    base = dm.build_decision_model(_occupancy_report(), [])
    occ = next(f for f in base["findings"] if f["section"] == "occupazione")
    stale = _confirm(occ, "occupato_opponibile")
    stale[0]["evidence_hash"] = "MISMATCH"
    m = dm.build_decision_model(_occupancy_report(), stale)
    item = _checklist_item(m, occ["finding_id"])
    assert item["status"] in ("da_verificare", "conferma_necessaria")  # open, not completed
    assert _consistency_holds(m)


# CK5: readiness counts agree with the checklist open/completed counts.
def test_checklist_counts_match_readiness():
    base = dm.build_decision_model(_occupancy_report(), [])
    occ = next(f for f in base["findings"] if f["section"] == "occupazione")
    m = dm.build_decision_model(_occupancy_report(), _confirm(occ, "occupato_opponibile"))
    ver = m["sections"]["verifiche"]
    assert ver["open_count"] + ver["completed_count"] == len(ver["items"])
    # the confirmed occupancy item is completed, so it is not in the open set
    assert ver["completed_count"] >= 1
    # readiness no longer counts the confirmed finding as an open confirmation
    assert m["readiness"]["state"] in ("COMPLETE_FOR_EXPORT", "READY_FOR_REVIEW", "CONFIRMATIONS_REQUIRED")
    assert not any(
        f.get("confirmation") for f in m["findings"] if f["finding_id"] == occ["finding_id"]
    )


# 20. sanitizer safety: no forbidden tokens anywhere in the decision model
def test_no_forbidden_tokens():
    rep = _report(
        compliance_section=[{"area": "conformità edilizia", "classification": "regularizable",
            "evidence_pages": [7], "notes": "difformità", "cost": 100.0}],
        occupancy_section={"status": "occupato", "status_label": "Occupato", "evidence_pages": [2],
            "title_info": "x", "risks": ["opponibilità"]},
        formalities_section=[{"type": "ipoteca", "type_label": "Ipoteca", "description": "d",
            "cancelled_by_procedure": True, "buyer_burden": False, "amount": 1.0, "evidence_pages": [3]}])
    m = dm.build_decision_model(rep, [])
    blob = json.dumps(m, ensure_ascii=False)
    for token in _FORBIDDEN_TOKENS:
        assert token not in blob, f"forbidden token leaked: {token}"
