"""Tests for the Storico lot workspace (pure-read lot state folding).

Everything here runs against a temp artifacts root (see conftest.py) and never
touches the DB, OpenAI or the real /srv artifacts folder. The central contract
under test: reading workspace state NEVER spawns a job.
"""

from correctness_v2 import artifacts, orchestrator, workspace


T0 = "2026-07-16T09:00:00+00:00"
T1 = "2026-07-16T10:00:00+00:00"
T2 = "2026-07-16T11:00:00+00:00"
T3 = "2026-07-16T12:00:00+00:00"


def _job_dirs():
    root = artifacts.jobs_root()
    if not root.exists():
        return set()
    return {p.name for p in root.iterdir() if p.is_dir()}


def _save_status(job_id, analysis_id, status, *, updated_at=T1, safe=None, **extra):
    payload = {
        "job_id": job_id,
        "analysis_id": analysis_id,
        "status": status,
        "created_at": updated_at,
        "updated_at": updated_at,
        "artifacts_saved": {},
    }
    if safe is not None:
        payload["safe_to_show_customer"] = bool(safe)
    payload.update(extra)
    artifacts.save_job_status(job_id, payload)
    return payload


def _ready_report(analysis_id, job_id, lot_id, amount="EUR 38.110,20"):
    return {
        "schema_version": "cv2.customer_report.v1",
        "analysis_id": analysis_id,
        "job_id": job_id,
        "report_status": "REPORT_READY",
        "title": f"Lotto {lot_id}",
        "lot_structure": {"selected_lot": str(lot_id)},
        "money_sections": {
            "valuation_chain": [{"label": "Valore finale", "amount_display": amount}]
        },
    }


def _setup_multi_lot_analysis(analysis_id):
    """Multi-lot analysis: lot 1 READY, lot 2 FAILED, lot 3 never analyzed."""
    # The lot-selection job carries the canonical lot index (3 genuine lots).
    _save_status(
        f"cv2_{analysis_id}_sel", analysis_id, "LOT_SELECTION_REQUIRED", updated_at=T0
    )
    artifacts.save_lot_index(
        f"cv2_{analysis_id}_sel",
        {
            "multi_lot": True,
            "lots": [
                {"lot_id": "1", "label": "Lotto 1", "address": "Via Roma 1"},
                {"lot_id": "2", "label": "Lotto 2", "address": "Via Roma 2"},
                {"lot_id": "3", "label": "Lotto 3", "address": "Via Roma 3"},
            ],
        },
    )
    # Lot 1: a completed selected-lot job with a customer-safe report.
    _save_status(
        f"cv2_{analysis_id}_lot1",
        analysis_id,
        "REPORT_READY",
        updated_at=T1,
        safe=True,
        selected_lot="1",
        multi_lot=True,
        lot_count=3,
    )
    artifacts.save_customer_report(
        f"cv2_{analysis_id}_lot1", _ready_report(analysis_id, f"cv2_{analysis_id}_lot1", "1")
    )
    # Lot 2: a failed selected-lot attempt.
    _save_status(
        f"cv2_{analysis_id}_lot2",
        analysis_id,
        "FAILED_ANALYSIS",
        updated_at=T2,
        safe=False,
        selected_lot="2",
        multi_lot=True,
        lot_count=3,
        reason_code="OPENAI_CALL_FAILED",
    )
    # Lot 3: never analyzed (index-only).


# ---------------------------------------------------------------------------
# (a) + (b): the overview folds correctly and building it is a pure read
# ---------------------------------------------------------------------------
def test_build_workspace_folds_ready_failed_and_not_analyzed(artifacts_root):
    analysis_id = "analysis_ws_overview"
    _setup_multi_lot_analysis(analysis_id)

    before = _job_dirs()
    ws = workspace.build_workspace(analysis_id)
    assert _job_dirs() == before  # zero side effects: no new job dirs

    assert ws["analysis_id"] == analysis_id
    assert ws["multi_lot"] is True
    assert ws["lot_count"] == 3
    assert ws["analysis_state"] == "LOT_OVERVIEW"

    by_id = {lot["lot_id"]: lot for lot in ws["lots"]}
    assert set(by_id) == {"1", "2", "3"}

    lot1 = by_id["1"]
    assert lot1["state"] == workspace.STATE_REPORT_READY
    assert lot1["has_safe_report"] is True
    assert lot1["job_running"] is False
    assert lot1["last_attempt_failed"] is False
    assert lot1["final_value"] == "EUR 38.110,20"
    assert lot1["actions"] == [workspace.ACTION_OPEN_REPORT, workspace.ACTION_RERUN]

    lot2 = by_id["2"]
    assert lot2["state"] == workspace.STATE_FAILED
    assert lot2["has_safe_report"] is False
    assert lot2["actions"] == [workspace.ACTION_RERUN]

    lot3 = by_id["3"]
    assert lot3["state"] == workspace.STATE_NOT_ANALYZED
    assert lot3["has_safe_report"] is False
    assert lot3["actions"] == [workspace.ACTION_GENERATE]

    assert ws["summary"] == {
        "lot_count": 3,
        "ready": 1,
        "preparing": 0,
        "confirmation_required": 0,
        "verification_required": 0,
        "failed": 1,
        "not_analyzed": 1,
    }


# ---------------------------------------------------------------------------
# (c): safe-report lookup — READY lot found, not-analyzed lot yields nothing
# ---------------------------------------------------------------------------
def test_find_lot_safe_report_ready_and_not_analyzed(artifacts_root):
    analysis_id = "analysis_ws_find"
    _setup_multi_lot_analysis(analysis_id)

    before = _job_dirs()
    status, report = workspace.find_lot_safe_report(analysis_id, "1")
    assert _job_dirs() == before
    assert isinstance(status, dict) and status["job_id"] == f"cv2_{analysis_id}_lot1"
    assert isinstance(report, dict) and report["report_status"] == "REPORT_READY"
    assert report["lot_structure"]["selected_lot"] == "1"

    status, report = workspace.find_lot_safe_report(analysis_id, "3")
    assert status is None and report is None

    # The FAILED lot has no safe report either.
    status, report = workspace.find_lot_safe_report(analysis_id, "2")
    assert status is None and report is None


# ---------------------------------------------------------------------------
# (d): latest terminal outcome for a failed lot
# ---------------------------------------------------------------------------
def test_latest_lot_outcome_reports_failed(artifacts_root):
    analysis_id = "analysis_ws_outcome"
    _setup_multi_lot_analysis(analysis_id)

    assert workspace.latest_lot_outcome(analysis_id, "2") == workspace.STATE_FAILED
    assert workspace.latest_lot_outcome(analysis_id, "1") == workspace.STATE_REPORT_READY
    assert workspace.latest_lot_outcome(analysis_id, "3") is None


# ---------------------------------------------------------------------------
# (e): a newer failed rerun never hides the prior safe report
# ---------------------------------------------------------------------------
def test_prior_safe_report_survives_newer_failed_rerun(artifacts_root):
    analysis_id = "analysis_ws_safe_survives"
    _save_status(
        f"cv2_{analysis_id}_ok",
        analysis_id,
        "REPORT_READY",
        updated_at=T1,
        safe=True,
        selected_lot="1",
        multi_lot=True,
        lot_count=3,
    )
    artifacts.save_customer_report(
        f"cv2_{analysis_id}_ok", _ready_report(analysis_id, f"cv2_{analysis_id}_ok", "1")
    )
    # A newer explicit rerun that failed.
    _save_status(
        f"cv2_{analysis_id}_retry",
        analysis_id,
        "FAILED_ANALYSIS",
        updated_at=T2,
        safe=False,
        selected_lot="1",
        multi_lot=True,
        lot_count=3,
    )

    ws = workspace.build_workspace(analysis_id)
    lot1 = next(lot for lot in ws["lots"] if lot["lot_id"] == "1")
    assert lot1["state"] == workspace.STATE_REPORT_READY  # folded state stays READY
    assert lot1["has_safe_report"] is True
    assert lot1["last_attempt_failed"] is True

    # The stored safe report itself stays reachable.
    status, report = workspace.find_lot_safe_report(analysis_id, "1")
    assert status["job_id"] == f"cv2_{analysis_id}_ok"
    assert report["report_status"] == "REPORT_READY"


# ---------------------------------------------------------------------------
# (f): generation slot — one claim at a time, and never over a running job
# ---------------------------------------------------------------------------
def test_begin_generation_claims_slot_once(artifacts_root):
    analysis_id = "analysis_ws_slot"
    try:
        assert workspace.begin_generation(analysis_id, "1") is True
        # Second simultaneous claim loses the race.
        assert workspace.begin_generation(analysis_id, "1") is False
    finally:
        workspace.finish_generation(analysis_id, "1")
    # After release the slot can be claimed again.
    try:
        assert workspace.begin_generation(analysis_id, "1") is True
    finally:
        workspace.finish_generation(analysis_id, "1")


def test_begin_generation_refuses_when_job_running(artifacts_root):
    analysis_id = "analysis_ws_slot_running"
    _save_status(
        f"cv2_{analysis_id}_run",
        analysis_id,
        "RUNNING",
        updated_at=T1,
        safe=False,
        selected_lot="9",
        multi_lot=True,
        lot_count=3,
    )
    try:
        assert workspace.lot_in_progress(analysis_id, "9") is not None
        assert workspace.begin_generation(analysis_id, "9") is False
        # A different lot of the same analysis is NOT blocked (lot-aware).
        assert workspace.begin_generation(analysis_id, "5") is True
    finally:
        workspace.finish_generation(analysis_id, "9")
        workspace.finish_generation(analysis_id, "5")


# ---------------------------------------------------------------------------
# (g): analyze_all per-lot reports are discoverable (no invisibility gap)
# ---------------------------------------------------------------------------
def test_find_lot_safe_report_covers_analyze_all_per_lot_results(artifacts_root):
    analysis_id = "analysis_ws_all"
    job_id = f"cv2_{analysis_id}_all"
    _save_status(
        job_id,
        analysis_id,
        "REPORT_READY",
        updated_at=T1,
        safe=True,
        analyze_all=True,
        lot_ids=["2", "3"],
        per_lot_results=[
            {"lot_id": "2", "status": "REPORT_READY"},
            {"lot_id": "3", "status": "FAILED_ANALYSIS"},
        ],
    )
    artifacts.save_lot_subartifact(
        job_id,
        "2",
        artifacts.CUSTOMER_REPORT_FILE,
        _ready_report(analysis_id, job_id, "2", amount="EUR 12.000,00"),
    )

    status, report = workspace.find_lot_safe_report(analysis_id, "2")
    assert status["job_id"] == job_id
    assert report["report_status"] == "REPORT_READY"
    assert report["title"] == "Lotto 2"

    # The failed sibling lot has no safe report and folds to FAILED.
    status, report = workspace.find_lot_safe_report(analysis_id, "3")
    assert status is None and report is None
    assert workspace.latest_lot_outcome(analysis_id, "3") == workspace.STATE_FAILED

    ws = workspace.build_workspace(analysis_id)
    by_id = {lot["lot_id"]: lot for lot in ws["lots"]}
    assert by_id["2"]["state"] == workspace.STATE_REPORT_READY
    assert by_id["2"]["final_value"] == "EUR 12.000,00"
    assert by_id["3"]["state"] == workspace.STATE_FAILED


# ---------------------------------------------------------------------------
# Fold bug A: a stale abandoned RUNNING job must never mask a newer safe report
# ---------------------------------------------------------------------------
def test_stale_running_job_does_not_mask_newer_safe_report(artifacts_root):
    analysis_id = "analysis_ws_stale_running"
    # Old abandoned RUNNING job for lot 1 ...
    _save_status(
        f"cv2_{analysis_id}_stale",
        analysis_id,
        "RUNNING",
        updated_at=T0,
        safe=False,
        selected_lot="1",
        multi_lot=True,
        lot_count=3,
    )
    # ... then a newer completed run of the same lot.
    _save_status(
        f"cv2_{analysis_id}_ok",
        analysis_id,
        "REPORT_READY",
        updated_at=T1,
        safe=True,
        selected_lot="1",
        multi_lot=True,
        lot_count=3,
    )
    artifacts.save_customer_report(
        f"cv2_{analysis_id}_ok", _ready_report(analysis_id, f"cv2_{analysis_id}_ok", "1")
    )

    ws = workspace.build_workspace(analysis_id)
    lot1 = next(lot for lot in ws["lots"] if lot["lot_id"] == "1")
    assert lot1["state"] == workspace.STATE_REPORT_READY  # not RUNNING
    assert lot1["job_running"] is False
    assert lot1["has_safe_report"] is True
    assert lot1["last_attempt_failed"] is False


# ---------------------------------------------------------------------------
# Fold bug B: a single-lot analysis stays single-lot with no phantom lot "1"
# ---------------------------------------------------------------------------
def test_single_lot_analysis_has_no_phantom_lot(artifacts_root):
    analysis_id = "analysis_ws_single_unico"
    # An older failed attempt that never produced a customer report (so it has
    # no explicit lot id): it must fold into the sole lot, not invent lot "1".
    _save_status(
        f"cv2_{analysis_id}_fail",
        analysis_id,
        "FAILED_ANALYSIS",
        updated_at=T0,
        safe=False,
    )
    # The successful run went through the selected-lot path for the sole
    # "unico" lot (selected_lot alone must NOT flip the analysis to multi-lot).
    _save_status(
        f"cv2_{analysis_id}_ok",
        analysis_id,
        "REPORT_READY",
        updated_at=T1,
        safe=True,
        selected_lot="unico",
        lot_count=1,
    )
    artifacts.save_customer_report(
        f"cv2_{analysis_id}_ok",
        _ready_report(analysis_id, f"cv2_{analysis_id}_ok", "unico", amount="EUR 391.849,00"),
    )

    ws = workspace.build_workspace(analysis_id)
    assert ws["multi_lot"] is False
    assert ws["analysis_state"] == "SINGLE_LOT"
    assert ws["lot_count"] == 1
    assert [lot["lot_id"] for lot in ws["lots"]] == ["unico"]  # no phantom "1"
    sole = ws["lots"][0]
    assert sole["state"] == workspace.STATE_REPORT_READY
    assert sole["has_safe_report"] is True
    assert sole["final_value"] == "EUR 391.849,00"
    assert sole["last_attempt_failed"] is False  # the failure predates the report
    assert ws["summary"]["ready"] == 1 and ws["summary"]["failed"] == 0

    # Single-lot lookups resolve the sole lot however the caller names it.
    _status, report = workspace.find_lot_safe_report(analysis_id, "unico")
    assert report["report_status"] == "REPORT_READY"
    _status, report = workspace.find_lot_safe_report(analysis_id, "1")
    assert report is not None and report["report_status"] == "REPORT_READY"
    assert workspace.latest_lot_outcome(analysis_id, "unico") == workspace.STATE_REPORT_READY


# ---------------------------------------------------------------------------
# A terminal step-1-only PDF_QUALITY_* job must never read as in-progress:
# it carries no lot outcome and must never dedup-block a legitimate rerun
# (live regression: Torino force-rerun returned spawned=false forever).
# ---------------------------------------------------------------------------
def test_terminal_pdf_quality_job_does_not_block_generation(artifacts_root):
    analysis_id = "analysis_ws_quality_only"
    # Step-1-only admin job that stopped at the quality gate (terminal).
    _save_status(
        f"cv2_{analysis_id}_q",
        analysis_id,
        "PDF_QUALITY_OK",
        updated_at=T0,
        safe=False,
    )
    # A newer completed customer run of the sole lot.
    _save_status(
        f"cv2_{analysis_id}_ok",
        analysis_id,
        "REPORT_READY",
        updated_at=T1,
        safe=True,
    )
    artifacts.save_customer_report(
        f"cv2_{analysis_id}_ok", _ready_report(analysis_id, f"cv2_{analysis_id}_ok", "1")
    )

    # Not in progress: the terminal quality job never reads as RUNNING.
    assert workspace.lot_in_progress(analysis_id, "1") is None
    ws = workspace.build_workspace(analysis_id)
    sole = ws["lots"][0]
    assert sole["state"] == workspace.STATE_REPORT_READY
    assert sole["job_running"] is False

    # A (forced) rerun can claim the generation slot: nothing blocks it.
    try:
        assert workspace.begin_generation(analysis_id, "1") is True
    finally:
        workspace.finish_generation(analysis_id, "1")


def test_lone_terminal_pdf_quality_job_presents_as_not_analyzed(artifacts_root):
    analysis_id = "analysis_ws_quality_lone"
    _save_status(
        f"cv2_{analysis_id}_q",
        analysis_id,
        "PDF_QUALITY_WARNING",
        updated_at=T0,
        safe=False,
    )

    # No analysis was ever attempted: the lot is NOT_ANALYZED (not FAILED, not
    # RUNNING), a plain generate is allowed, and no outcome gates a rerun.
    assert workspace.latest_lot_outcome(analysis_id, "1") is None
    assert workspace.lot_in_progress(analysis_id, "1") is None
    ws = workspace.build_workspace(analysis_id)
    assert ws["lot_count"] == 1
    sole = ws["lots"][0]
    assert sole["state"] == workspace.STATE_NOT_ANALYZED
    assert sole["actions"] == [workspace.ACTION_GENERATE]
    assert sole["last_attempt_failed"] is False
    try:
        assert workspace.begin_generation(analysis_id, "1") is True
    finally:
        workspace.finish_generation(analysis_id, "1")


# ---------------------------------------------------------------------------
# No import-time or read-time job spawning, ever
# ---------------------------------------------------------------------------
def test_workspace_reads_never_spawn_jobs(artifacts_root, monkeypatch):
    analysis_id = "analysis_ws_no_spawn"
    _setup_multi_lot_analysis(analysis_id)

    def _forbidden(*args, **kwargs):
        raise AssertionError("workspace reads must never call start_job")

    # If any read path reached the orchestrator, the test would blow up here.
    monkeypatch.setattr(orchestrator, "start_job", _forbidden)

    # The module itself must not even hold a job-creation entry point.
    assert not hasattr(workspace, "start_job")
    assert not hasattr(workspace, "autostart_job")

    before = _job_dirs()
    workspace.build_workspace(analysis_id)
    workspace.find_lot_safe_report(analysis_id, "1")
    workspace.find_lot_safe_report(analysis_id, "3")
    workspace.lot_in_progress(analysis_id, "2")
    workspace.latest_lot_outcome(analysis_id, "2")
    workspace.is_multi_lot(analysis_id)
    assert _job_dirs() == before  # zero new job dirs from any read
