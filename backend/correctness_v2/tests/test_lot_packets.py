"""Unit tests for generic lot segmentation + per-lot packet building."""

from correctness_v2 import analyst, lot_packets, lots

from .sample_perizia import MULTI_LOT_PAGES, make_multilot_worksheet


def _ws():
    return analyst.normalize_worksheet(make_multilot_worksheet())


def test_segment_pages_assigns_global_and_per_lot():
    seg = lot_packets.segment_pages(MULTI_LOT_PAGES, ["1", "2"])
    # Page 1 is preamble (no lot) -> global. Pages 2,3 -> lot1; 4,5 -> lot2.
    assert seg["global_pages"] == [1]
    assert seg["lot_pages"]["1"] == [2, 3]
    assert seg["lot_pages"]["2"] == [4, 5]
    assert seg["shared_pages"] == []


def test_select_lot_pages_isolates_one_lot_plus_global():
    seg = lot_packets.segment_pages(MULTI_LOT_PAGES, ["1", "2"])
    lot1 = lot_packets.select_lot_pages(MULTI_LOT_PAGES, seg, "1")
    nums = sorted(p["page_number"] for p in lot1)
    # Lot 1 sees only global + its own pages; lot 2's pages are excluded (no blend).
    assert nums == [1, 2, 3]
    assert 4 not in nums and 5 not in nums


def test_build_lot_index_lists_each_lot_with_evidence():
    ws = _ws()
    rep = lots.build_lot_report(ws, MULTI_LOT_PAGES)
    idx = lot_packets.build_lot_index(ws, MULTI_LOT_PAGES, rep)
    assert idx["multi_lot"] is True
    assert {L["lot_id"] for L in idx["lots"]} == {"1", "2"}
    for L in idx["lots"]:
        assert "page_evidence" in L
        assert "confidence" in L
        assert L["confidence"] in {"high", "medium", "low"}


def test_per_lot_packets_keep_pages_separate_and_flag_reanalysis():
    ws = _ws()
    rep = lots.build_lot_report(ws, MULTI_LOT_PAGES)
    packets = lot_packets.build_per_lot_packets(ws, MULTI_LOT_PAGES, rep)
    assert packets["global_pages"] == [1]
    by_lot = {p["lot_id"]: p for p in packets["packets"]}
    assert by_lot["1"]["lot_specific_pages"] == [2, 3]
    assert by_lot["2"]["lot_specific_pages"] == [4, 5]
    # Deep per-lot detail must be flagged as requiring a per-lot re-analysis (never
    # copied/blended from the document-level worksheet).
    assert by_lot["1"]["lot_specific_detail_requires_analysis"] is True
    # Reanalysis input is the lot's own pages + global, never the other lot's pages.
    assert 4 not in by_lot["1"]["reanalysis_input_pages"]


def test_selected_lot_context_excludes_other_lot_pages():
    seg = lot_packets.segment_pages(MULTI_LOT_PAGES, ["1", "2"])
    ctx = lot_packets.build_selected_lot_context(MULTI_LOT_PAGES, seg, "2")
    assert ctx["selected_lot_id"] == "2"
    assert set(ctx["analysis_pages"]) == {1, 4, 5}
    assert 2 not in ctx["analysis_pages"] and 3 not in ctx["analysis_pages"]


# ---------------------------------------------------------------------------
# Wording-agnostic detection (no hardcoded document / city)
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Strict per-lot money assignment
# ---------------------------------------------------------------------------
def _money_ws():
    """A multi-lot worksheet with money clearly tied to different lot pages."""
    raw = {
        "case_identity": {"tribunale": "Tribunale di Esempio", "lotto": "Lotti 1 e 2", "evidence_pages": [1]},
        "lots": [
            {"lot_id": "1", "label": "Lotto 1", "address": "Via Uno",
             "prezzo_base_asta": 50000.0, "sale_value": 60000.0, "evidence_pages": [2]},
            {"lot_id": "2", "label": "Lotto 2", "address": "Via Due",
             "prezzo_base_asta": 70000.0, "sale_value": 80000.0, "evidence_pages": [4]},
        ],
        "money": {
            "market_value": 999999.0,  # cited on a SHARED page -> must be uncertain
            "evidence_pages": [2, 4],
            "buyer_side_costs": [
                {"label": "Spese lotto 1", "amount": 294.0, "evidence_pages": [2]},  # -> lot 1
            ],
            "deductions": [
                {"label": "Deprezzamento", "amount": 1000.0, "evidence_pages": [4]},  # -> lot 2
            ],
        },
    }
    return analyst.normalize_worksheet(raw)


def test_build_lot_money_assigns_strictly_per_lot():
    ws = _money_ws()
    seg = lot_packets.segment_pages(MULTI_LOT_PAGES, ["1", "2"])
    money = lot_packets.build_lot_money(ws, seg)

    lot1 = money["by_lot"]["1"]
    lot2 = money["by_lot"]["2"]
    # Model-linked per-lot values land on the right lot, never the other.
    assert lot1["prezzo_base_asta"]["amount"] == 50000.0
    assert lot1["sale_value"]["amount"] == 60000.0
    assert lot2["prezzo_base_asta"]["amount"] == 70000.0
    # Lot-1 buyer cost (page 2) is on lot 1 only; lot-2 deduction (page 4) on lot 2.
    assert [r["amount"] for r in lot1["buyer_side_costs"]] == [294.0]
    assert [r["amount"] for r in lot2["deductions"]] == [1000.0]
    assert lot2["buyer_side_costs"] == []  # lot 1's cost never leaks into lot 2


def test_build_lot_money_preserves_ambiguous_as_uncertain():
    ws = _money_ws()
    seg = lot_packets.segment_pages(MULTI_LOT_PAGES, ["1", "2"])
    money = lot_packets.build_lot_money(ws, seg)
    # market_value cited on pages [2,4] (two different lots) cannot be assigned ->
    # preserved under uncertain_money with evidence + manual_review, never dropped.
    amounts = [r["amount"] for r in money["uncertain_money"]]
    assert 999999.0 in amounts
    flagged = next(r for r in money["uncertain_money"] if r["amount"] == 999999.0)
    assert flagged["manual_review"] is True
    assert flagged["evidence_pages"] == [2, 4]
    assert money["needs_manual_review_money"] is True


def test_money_label_lot_tag_cannot_override_exclusive_evidence_lot():
    ws = _money_ws()
    ws["money"]["deductions"] = [{
        "label": "Confronto con Lotto 2 - costo proprio",
        "amount": 4321.0,
        "evidence_pages": [2],
    }]
    seg = lot_packets.segment_pages(MULTI_LOT_PAGES, ["1", "2"])
    money = lot_packets.build_lot_money(ws, seg)

    assert [row["amount"] for row in money["by_lot"]["1"]["deductions"]] == [4321.0]
    assert money["by_lot"]["2"]["deductions"] == []
    row = money["by_lot"]["1"]["deductions"][0]
    assert row["allocation_conflict"] is True
    assert row["label_lot_ids"] == ["2"]


def test_contract_money_keeps_distinct_equal_amount_deductions():
    rows = lot_packets.contract_rows_from_lot_money({
        "deductions": [
            {"label": "Deprezzamento per vetustà", "amount": 5000, "evidence_pages": [2]},
            {"label": "Costi di regolarizzazione impianto elettrico", "amount": 5000, "evidence_pages": [2]},
        ]
    })
    assert [(row["label"], row["amount"]) for row in rows] == [
        ("Deprezzamento per vetustà", 5000),
        ("Costi di regolarizzazione impianto elettrico", 5000),
    ]


def test_lot_index_money_is_per_lot_not_shared():
    ws = _money_ws()
    rep = lots.build_lot_report(ws, MULTI_LOT_PAGES)
    seg = lot_packets.segment_pages(MULTI_LOT_PAGES, rep["lot_ids"])
    idx = lot_packets.build_lot_index(ws, MULTI_LOT_PAGES, rep, seg)
    by_lot = {L["lot_id"]: L["money"] for L in idx["lots"]}
    assert by_lot["1"]["prezzo_base_asta"]["amount"] == 50000.0
    assert by_lot["2"]["prezzo_base_asta"]["amount"] == 70000.0
    # The shared/ambiguous market value is NOT placed in any lot.
    assert by_lot["1"]["market_value"] is None
    assert by_lot["2"]["market_value"] is None
    assert idx["needs_manual_review_money"] is True


def test_selected_lot_context_money_excludes_other_lot():
    ws = _money_ws()
    seg = lot_packets.segment_pages(MULTI_LOT_PAGES, ["1", "2"])
    ctx = lot_packets.build_selected_lot_context(MULTI_LOT_PAGES, seg, "1", worksheet=ws)
    # Only lot 1's money is present; lot 2's prezzo base (70000) must be absent.
    assert ctx["lot_money"]["prezzo_base_asta"]["amount"] == 50000.0
    blob = json_dumps(ctx["lot_money"])
    assert "70000" not in blob and "80000" not in blob


def json_dumps(obj):
    import json
    return json.dumps(obj)


def test_analyst_target_lot_prompt_focuses_one_lot():
    from correctness_v2 import analyst
    msgs = analyst.build_messages(MULTI_LOT_PAGES, target_lot="2")
    user = next(m["content"] for m in msgs if m["role"] == "user")
    assert "ESCLUSIVAMENTE IL LOTTO 2" in user
    # Without a target lot, no single-lot focus instruction is injected.
    msgs2 = analyst.build_messages(MULTI_LOT_PAGES)
    user2 = next(m["content"] for m in msgs2 if m["role"] == "user")
    assert "ESCLUSIVAMENTE IL LOTTO" not in user2


def test_trailing_valuation_page_stays_with_the_lot_it_concludes():
    # Regression (Codogno): a lot's CONCLUDING valuation page carries a boilerplate
    # "Lotto 00" footer artifact. "00" must NOT be read as a real lot (lots number
    # from 1) — otherwise it spawns a phantom lot that hijacks p2/p4 (the stima /
    # "valore nello stato di fatto" pages) away from lots 1/2.
    pages = [
        {"page_number": 1, "text": "Lotto 1 - descrizione dell'immobile e consistenza."},
        {"page_number": 2, "text": (
            "Valore nello stato di fatto: € 100.000,00. "
            "E.I. 123/2024 - Lotto 00 - Astalegale.net")},
        {"page_number": 3, "text": "Lotto 2 - descrizione del secondo immobile."},
        {"page_number": 4, "text": (
            "Valore nello stato di fatto: € 80.000,00. Lotto 00")},
    ]
    seg = lot_packets.segment_pages(pages, ["1", "2"])
    # No phantom "0"/"00" lot; the valuation pages carry forward to their lot.
    assert "00" not in seg["lot_pages"] and "0" not in seg["lot_pages"]
    assert seg["lot_ids"] == ["1", "2"]
    assert seg["lot_pages"]["1"] == [1, 2]
    assert seg["lot_pages"]["2"] == [3, 4]
    # The concluding valuation page is now in the lot's isolated context.
    assert 2 in {p["page_number"] for p in lot_packets.select_lot_pages(pages, seg, "1")}
    assert 4 in {p["page_number"] for p in lot_packets.select_lot_pages(pages, seg, "2")}


def test_zero_lot_ids_are_never_real_lots():
    assert lot_packets._numeric_lot_ids("Lotto 00 in calce alla pagina") == []
    assert lot_packets._numeric_lot_ids("Lotto 0") == []
    # A genuine numbered lot is still detected.
    assert lot_packets._numeric_lot_ids("Lotto 3 - immobile") == ["3"]


def test_multi_lot_boundary_page_stays_shared_not_stolen():
    # A page that explicitly names TWO real lots (a summary/boundary table) stays
    # SHARED and is excluded from single-lot re-analysis — the trailing-page fix
    # must not turn such a page into one lot's owned page.
    pages = [
        {"page_number": 1, "text": "Lotto 1 - primo immobile."},
        {"page_number": 2, "text": "Riepilogo: Lotto 1 e Lotto 2 - tabella comparativa."},
        {"page_number": 3, "text": "Lotto 2 - secondo immobile."},
    ]
    seg = lot_packets.segment_pages(pages, ["1", "2"])
    assert seg["shared_pages"] == [2]
    assert 2 not in seg["lot_pages"]["1"]
    assert 2 not in {p["page_number"] for p in lot_packets.select_lot_pages(pages, seg, "1")}


def test_normalize_lot_token_handles_varied_wordings():
    assert lots.normalize_lot_token("Lotto 2") == "2"
    assert lots.normalize_lot_token("LOTTO PRIMO") == "1"
    assert lots.normalize_lot_token("Lotto A") == "a"
    assert lots.normalize_lot_token("Lotto III") == "iii"
    assert lots.normalize_lot_token("lotto unico") == "unico"


def test_semantic_lots_array_drives_multi_lot_without_digits_in_flat_text():
    # A document whose lots are labelled with letters in the analyst lots[] array
    # (no digit "lotto N" in flat fields) is still detected as multi-lot.
    raw = {
        "case_identity": {"tribunale": "Tribunale di Esempio", "evidence_pages": [1]},
        "lots": [
            {"lot_id": "A", "label": "Lotto A", "address": "Via Uno", "evidence_pages": [1]},
            {"lot_id": "B", "label": "Lotto B", "address": "Via Due", "evidence_pages": [2]},
        ],
        "money": {},
    }
    ws = analyst.normalize_worksheet(raw)
    # Pages carry NO "lotto N" text, so detection rests purely on the semantic
    # lots[] array (letter labels). It must still flag multi-lot.
    plain_pages = [{"page_number": 1, "text": "Relazione di stima."}, {"page_number": 2, "text": "Allegati."}]
    rep = lots.build_lot_report(ws, plain_pages)
    assert rep["multi_lot"] is True
    assert set(rep["lot_ids"]) == {"a", "b"}
