"""
Correctness Mode v2 — isolated subsystem.

This package lives BESIDE the existing (old) perizia pipeline and never replaces
or weakens it. It is feature-flagged and admin-only by default.

Critical product rule: Correctness Mode FAILS CLOSED.
If Correctness Mode fails, it must NOT fall back to the old analyzer.
A wrong report is worse than no report.

Step 1 scope (this module): job shell, strict failure/diagnostic contract,
artifact storage, and a PDF quality blocker. No OpenAI analysis, no Gemini
narration, no customer report generation here.
"""

from . import feature_flags, schemas, job_status, artifacts, pdf_quality, orchestrator

__all__ = [
    "feature_flags",
    "schemas",
    "job_status",
    "artifacts",
    "pdf_quality",
    "orchestrator",
]
