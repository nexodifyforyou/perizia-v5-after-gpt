"""
The optional, opt-in PDF retention attempt inside ``POST /api/analysis/perizia``
(``analyze_perizia``, server.py) must never hang or materially delay the
customer's analysis response -- mirroring the non-negotiable rule already
proven for the pipeline itself (``PIPELINE_TIMEOUT_SECONDS``,
``test_beta_perizia_limits_server.py::test_timeout_before_marker_releases_and_returns_504``)
and for beta telemetry (``beta_program/signals.py``: "must never alter
pipeline output... never delays the analysis").

Regression for a gap found during red-team review: the upload-time retention
call was wrapped in a broad ``try/except`` but had no bounded timeout, so a
stalled retention I/O (Mongo or disk) could hang the whole request instead of
degrading to "not retained" within ``PDF_RETENTION_TIMEOUT_SECONDS``.
"""

import asyncio
import io
import time

import pytest

import beta_program_fakes as fk  # sets sys.path
import server
from pdf_retention import ingest as pdf_retention_ingest

try:
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    _HAVE_REPORTLAB = True
except Exception:  # pragma: no cover - environment guard
    _HAVE_REPORTLAB = False

pytestmark = pytest.mark.skipif(not _HAVE_REPORTLAB, reason="reportlab not available")


def _make_pdf(num_lines=40):
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    y = 800
    for i in range(num_lines):
        c.drawString(50, y, f"Riga di testo numero {i} della perizia immobiliare di prova.")
        y -= 20
        if y < 50:
            c.showPage()
            y = 800
    c.showPage()
    c.save()
    return buf.getvalue()


SAMPLE_PDF = _make_pdf(40)


@pytest.fixture()
def anyio_backend():
    return "asyncio"


@pytest.fixture()
def db(monkeypatch):
    database = fk.install_fake_db(monkeypatch)

    def _fake_qa_gate(result, raw_text=None, internal_runtime=None):
        return {"status": "PASS", "llm_used": False, "corrections_applied": []}

    async def _fake_narrator(*args, **kwargs):
        raise RuntimeError("narrator disabled for test")

    async def _fake_docai(contents, mime_type, request_id):
        return [], "", None

    monkeypatch.setattr(server, "apply_customer_contract_qa_gate", _fake_qa_gate)
    monkeypatch.setattr(server, "_apply_post_qa_decision_narrator", _fake_narrator)
    monkeypatch.setattr(server, "_extract_with_docai", _fake_docai)

    from correctness_v2 import api as correctness_v2_api

    monkeypatch.setattr(correctness_v2_api, "autostart_job", lambda *a, **k: False)

    user_doc = fk.normal_user(email="retention_timeout@example.test", user_id="user_ret_timeout")
    fk.seed_session(database, user_doc, "s1")

    yield database
    fk.teardown_fake()


@pytest.mark.anyio
async def test_hanging_retention_attempt_is_bounded_and_never_delays_response(db, monkeypatch):
    async def _hang_forever(**_kwargs):
        await asyncio.sleep(10)
        return {"attempted": True, "retained": True}

    monkeypatch.setattr(pdf_retention_ingest, "retain_if_consented", _hang_forever)
    monkeypatch.setattr(server, "PDF_RETENTION_TIMEOUT_SECONDS", 0.1)

    started = time.monotonic()
    resp = await fk.client_request(
        "POST", "/api/analysis/perizia", token="s1",
        files={"file": ("test.pdf", SAMPLE_PDF, "application/pdf")},
        data={"retain_original_consent": "true"},
    )
    elapsed = time.monotonic() - started

    # Bounded well under the injected 10s hang -- proves the analysis response
    # is never held hostage by a stalled retention attempt.
    assert elapsed < 5.0
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["pdf_retention"] == {
        "attempted": True,
        "retained": False,
        "failure_code": "RETENTION_TIMEOUT",
    }
