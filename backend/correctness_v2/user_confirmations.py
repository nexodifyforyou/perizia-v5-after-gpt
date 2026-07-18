"""
Authoritative MongoDB store for focused user confirmations (DECISION #2).

A confirmation is a mutable, ownership-scoped, audit-tracked user action on a
single decision-model finding — NOT an artifact. It is therefore persisted in the
app's existing async Mongo database (``server.db``), never in ``customer_report.json``
and never in React state.

Two additive collections (no migration of any existing collection):
  * ``correctness_v2_confirmations``        — current state, one doc per finding,
    unique on (analysis_id, lot_id, finding_id, report_version, user_id).
  * ``correctness_v2_confirmation_audit``   — append-only history, never updated.

Guarantees mirrored from the money-confirmation flow:
  * option validation against the OFFERED set only (unoffered answers rejected);
  * zero OpenAI / zero jobs / zero credits on every write;
  * a confirmation NEVER rewrites a perizia fact and NEVER weakens a validator
    failure (fail-closed reports expose no findings, so nothing is confirmable).

The db handle is reached lazily (``import server``) exactly like ``api.py`` to
avoid a circular import with the very large ``server.py`` module.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pymongo.errors import DuplicateKeyError

from . import decision_model

CONFIRMATIONS_COLLECTION = "correctness_v2_confirmations"
AUDIT_COLLECTION = "correctness_v2_confirmation_audit"
CONFIRMATION_SCHEMA_VERSION = "cv2.user_confirmations.v1"
CONFIRMATION_SOURCE = "USER_CONFIRMED"
UNSURE_OPTION_ID = "non_sicuro"

_MAX_NOTE_CHARS = 500

# Set once per process after ensure_indexes succeeds.
_indexes_ready = False


def _db():
    import server  # type: ignore  # lazy: avoid circular import with server.py

    return server.db


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _lot_key(lot_id: Any) -> str:
    return str(lot_id) if lot_id not in (None, "") else "-"


async def ensure_indexes() -> None:
    """Create the unique identity index + owner-list index (idempotent)."""
    global _indexes_ready
    if _indexes_ready:
        return
    db = _db()
    await db[CONFIRMATIONS_COLLECTION].create_index(
        [
            ("analysis_id", 1),
            ("lot_id", 1),
            ("finding_id", 1),
            ("report_version", 1),
            ("user_id", 1),
        ],
        unique=True,
        name="uq_confirmation_identity",
        background=True,
    )
    await db[CONFIRMATIONS_COLLECTION].create_index(
        [("analysis_id", 1), ("user_id", 1)],
        name="ix_confirmation_owner",
        background=True,
    )
    _indexes_ready = True


def _option_label(finding: Dict[str, Any], option_id: str) -> str:
    """Return the customer label for ``option_id``, or raise if not offered.

    Mirrors ``money_confirmation.validate_answers``: the store never accepts an
    answer that was not one of the offered options (or the always-available
    "Non sono sicuro").
    """
    confirmation = finding.get("confirmation") or {}
    if not confirmation.get("eligible"):
        raise ValueError("Conferma non disponibile per questo elemento.")
    for option in confirmation.get("options") or []:
        if option.get("option_id") == option_id:
            return str(option.get("label"))
    unsure = confirmation.get("unsure_option") or {}
    if option_id == unsure.get("option_id") or option_id == UNSURE_OPTION_ID:
        return str(unsure.get("label") or "Non sono sicuro")
    raise ValueError("Opzione selezionata non valida per questo elemento.")


def _clean_note(note: Any) -> Optional[str]:
    if note in (None, ""):
        return None
    text = " ".join(str(note).split())
    return text[:_MAX_NOTE_CHARS] or None


async def submit(
    *,
    analysis_id: str,
    lot_id: Any,
    finding: Dict[str, Any],
    option_id: str,
    user_id: str,
    report_version: Any,
    decision_version: Any = decision_model.SCHEMA_VERSION,
    job_id: Any = None,
    note: Any = None,
) -> Dict[str, Any]:
    """Upsert a confirmation on the unique identity tuple + append one audit doc.

    ``finding`` is the eligible decision-model finding. Raises ValueError (→ 400)
    when the finding is not confirmable or the option was not offered. The prior
    answer is never destroyed: every create/update appends to the audit log.
    """
    label = _option_label(finding, option_id)  # validates eligibility + option
    await ensure_indexes()
    db = _db()

    finding_id = str(finding.get("finding_id"))
    evidence = finding.get("evidence") or {}
    page = evidence.get("page") if evidence.get("page") is not None else finding.get("page")
    ehash = decision_model.evidence_hash(evidence.get("excerpt"))
    status = "non_sicuro" if option_id == UNSURE_OPTION_ID else "confermato_utente"
    now = _now()
    lot_key = _lot_key(lot_id)
    report_version = str(report_version or "")

    query = {
        "analysis_id": str(analysis_id),
        "lot_id": lot_key,
        "finding_id": finding_id,
        "report_version": report_version,
        "user_id": str(user_id),
    }
    existing = await db[CONFIRMATIONS_COLLECTION].find_one(query, {"_id": 0})

    if existing:
        confirmation_id = existing["confirmation_id"]
        action = "updated"
        created_at = existing.get("created_at", now)
        from_option = existing.get("selected_option")
        from_status = existing.get("status")
    else:
        confirmation_id = "cnf_" + uuid.uuid4().hex
        action = "created"
        created_at = now
        from_option = None
        from_status = None

    doc = {
        "schema_version": CONFIRMATION_SCHEMA_VERSION,
        "confirmation_id": confirmation_id,
        "analysis_id": str(analysis_id),
        "lot_id": lot_key,
        "finding_id": finding_id,
        "report_version": report_version,
        "decision_version": str(decision_version or ""),
        "job_id": str(job_id) if job_id is not None else None,
        "user_id": str(user_id),
        "selected_option": option_id,
        "selected_label": label,
        "page": page,
        "evidence_hash": ehash,
        "status": status,
        "note": _clean_note(note),
        "source": CONFIRMATION_SOURCE,
        "created_at": created_at,
        "updated_at": now,
    }

    # Append-only audit FIRST, so history survives even if the upsert races.
    await db[AUDIT_COLLECTION].insert_one(
        {
            "audit_id": "aud_" + uuid.uuid4().hex,
            "confirmation_id": confirmation_id,
            "analysis_id": str(analysis_id),
            "lot_id": lot_key,
            "finding_id": finding_id,
            "report_version": report_version,
            "user_id": str(user_id),
            "action": action,
            "from_option": from_option,
            "to_option": option_id,
            "from_status": from_status,
            "to_status": status,
            "at": now,
        }
    )
    try:
        await db[CONFIRMATIONS_COLLECTION].update_one(query, {"$set": doc}, upsert=True)
    except DuplicateKeyError:
        # A concurrent create raced us on the unique identity tuple; the row now
        # exists, so converge with a plain update (last write wins). The unique
        # index guarantees exactly one active record; the audit trail is intact.
        await db[CONFIRMATIONS_COLLECTION].update_one(query, {"$set": doc})
    return doc


async def list_for_analysis(analysis_id: str, user_id: str) -> List[Dict[str, Any]]:
    """All of ``user_id``'s confirmations for ``analysis_id`` (owner projection)."""
    db = _db()
    cursor = db[CONFIRMATIONS_COLLECTION].find(
        {"analysis_id": str(analysis_id), "user_id": str(user_id)}, {"_id": 0}
    )
    return await cursor.to_list(None)


async def list_all_for_analysis(analysis_id: str) -> List[Dict[str, Any]]:
    """Every confirmation for ``analysis_id`` — admin (Vista admin) inspection only."""
    db = _db()
    cursor = db[CONFIRMATIONS_COLLECTION].find({"analysis_id": str(analysis_id)}, {"_id": 0})
    return await cursor.to_list(None)


async def audit_for_analysis(analysis_id: str) -> List[Dict[str, Any]]:
    """Append-only audit trail for ``analysis_id`` — admin inspection only."""
    db = _db()
    cursor = db[AUDIT_COLLECTION].find({"analysis_id": str(analysis_id)}, {"_id": 0})
    return await cursor.to_list(None)
