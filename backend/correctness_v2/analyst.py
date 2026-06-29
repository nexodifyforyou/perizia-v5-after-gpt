"""
Generic analyst worksheet generation for Correctness Mode v2 (step 2).

The analyst turns page-by-page extracted text of an ITALIAN perizia (court
appraisal for a judicial sale) into a strict, evidence-anchored worksheet. It is
deliberately GENERIC: it never branches on a specific tribunale, città or
document. The only knowledge baked in is the structure of an Italian perizia.

Hard rules pushed onto the model AND enforced on normalization:
  * Extract ONLY what the PDF text supports. Unknown -> null / empty.
  * Every non-trivial claim carries ``evidence_pages`` (page numbers).
  * Compliance areas are classified with a fixed enum.
  * Buyer-side costs are kept SEPARATE from procedure-cancelled formalities.
  * Ipoteca / pignoramento are NOT buyer costs unless the text explicitly says so.

The model output is parsed and normalized into a canonical shape so the
deterministic validator and contract builder always see the same structure.

This module performs NO old-analyzer fallback. On any failure it raises
:class:`AnalystError` and the orchestrator fails the job closed.
"""

from __future__ import annotations

import json
import re
from typing import Any, Callable, Dict, List, Optional

from . import openai_client
from .openai_client import OpenAIClientError

WORKSHEET_SCHEMA_VERSION = "cv2.worksheet.v1"

# Classification enum for technical compliance areas (and used by the validator).
COMPLIANCE_CLASSES = {
    "conforming",
    "non_conforming",
    "regularizable",
    "not_regularizable",
    "uncertain",
}

SEVERITY_LEVELS = {"grave", "media", "minore", "info"}

FORMALITY_TYPES = {
    "ipoteca",
    "pignoramento",
    "sequestro",
    "domanda_giudiziale",
    "other",
}

# Formality types that are normally wiped by the judicial sale (decreto di
# trasferimento) and therefore must NOT be treated as buyer costs by default.
CANCELLABLE_FORMALITY_TYPES = {"ipoteca", "pignoramento", "sequestro"}


# A caller compatible with openai_client.call_openai_json.
OpenAICaller = Callable[..., Dict[str, Any]]


class AnalystError(RuntimeError):
    def __init__(self, message: str, *, reason_code: str = "ANALYST_FAILED"):
        super().__init__(message)
        self.reason_code = reason_code


_SYSTEM_PROMPT = (
    "Sei un perito estimatore esperto in perizie immobiliari per vendite "
    "giudiziarie italiane. Ricevi il testo estratto, pagina per pagina, di una "
    "perizia. Devi produrre un FOGLIO DI LAVORO strutturato in JSON.\n\n"
    "REGOLE ASSOLUTE:\n"
    "1. Estrai SOLO ciò che è supportato dal testo. Se un dato non è presente, "
    "usa null oppure lista vuota. NON inventare nulla.\n"
    "2. Ogni affermazione non banale deve indicare 'evidence_pages': la lista "
    "dei numeri di pagina (interi) da cui proviene.\n"
    "3. Classifica ogni area di conformità tecnica SOLO con uno di questi valori: "
    "'conforming', 'non_conforming', 'regularizable', 'not_regularizable', "
    "'uncertain'. Se il testo dice che l'immobile è conforme, usa 'conforming' "
    "e NON marcarlo come grave.\n"
    "4. Non promuovere un problema minore o regolarizzabile a problema grave se "
    "il testo non lo supporta.\n"
    "5. Tieni SEPARATI i costi a carico dell'acquirente ('buyer_side_costs') "
    "dalle formalità cancellate dalla procedura ('procedure_cancelled_costs').\n"
    "6. Ipoteca e pignoramento sono normalmente cancellati dalla procedura con il "
    "decreto di trasferimento: NON trattarli come costi a carico dell'acquirente "
    "a meno che il testo dica esplicitamente che l'acquirente li paga.\n"
    "7. 'auction_terms.prezzo_base_asta' va valorizzato SOLO se il testo dice "
    "ESPLICITAMENTE 'prezzo base', 'base d'asta' o equivalente. NON usare il "
    "valore di vendita giudiziaria come prezzo base d'asta. 'valore di vendita "
    "giudiziaria' va SEMPRE in money.sale_value e NON va mai rietichettato.\n"
    "8. NON scartare alcun importo monetario significativo. Se il ruolo di un "
    "importo non è chiaro, mettilo in 'uncertain_money' con la pagina di evidenza "
    "e una breve motivazione.\n"
    "9. Se la perizia descrive PIÙ LOTTI distinti (LOTTO 1, LOTTO 2, ...), compila "
    "l'array 'lots' con una voce per ciascun lotto e NON fondere dati di lotti "
    "diversi nei campi piatti: lascia 'case_identity.address' a null (o indica più "
    "lotti) e metti gli indirizzi per-lotto in 'lots'. Un singolo lotto può "
    "contenere più 'beni' (es. appartamento + box): questo NON è multi-lotto.\n"
    "10. Restituisci ESCLUSIVAMENTE JSON valido, senza testo aggiuntivo.\n\n"
    "SCHEMA JSON RICHIESTO:\n"
    "{\n"
    '  "case_identity": {"tribunale": str|null, "procedura_rge": str|null, '
    '"lotto": str|null, "address": str|null, "property_type": str|null, '
    '"ownership_right": str|null, "evidence_pages": [int]},\n'
    '  "lots": [{"lot_id": str, "label": str|null, "address": str|null, '
    '"property_type": str|null, "ownership_right": str|null, '
    '"prezzo_base_asta": number|null, "sale_value": number|null, '
    '"occupancy_status": str|null, "evidence_pages": [int]}],\n'
    '  "occupancy": {"status": str|null, "title_info": str|null, '
    '"opponibility": str|null, "registration_dates": [str], '
    '"expiry_dates": [str], "risks": [str], "evidence_pages": [int]},\n'
    '  "technical_compliance": [{"area": str, "classification": '
    '"conforming|non_conforming|regularizable|not_regularizable|uncertain", '
    '"blocks_saleability": bool, "cost": number|null, "timing": str|null, '
    '"notes": str|null, "evidence_pages": [int]}],\n'
    '  "money": {"market_value": number|null, '
    '"deductions": [{"label": str, "amount": number, "evidence_pages": [int]}], '
    '"regularization_costs": number|null, "current_state_value": number|null, '
    '"cancellation_costs": number|null, "sale_value": number|null, '
    '"auction_terms": {"prezzo_base_asta": number|null, "offerta_minima": number|null, '
    '"rialzo_minimo": number|null, "cauzione": number|null, "evidence_pages": [int]}, '
    '"buyer_side_costs": [{"label": str, "amount": number|null, "evidence_pages": [int]}], '
    '"procedure_cancelled_costs": [{"label": str, "amount": number|null, "evidence_pages": [int]}], '
    '"uncertain_money": [{"label": str, "amount": number, "evidence_pages": [int], "reason": str|null}], '
    '"evidence_pages": [int]},\n'
    '  "legal_formalities": [{"type": '
    '"ipoteca|pignoramento|sequestro|domanda_giudiziale|other", '
    '"description": str|null, "cancelled_by_procedure": bool, '
    '"buyer_burden": bool, "amount": number|null, "evidence_pages": [int]}],\n'
    '  "risk_classification": [{"area": str, "severity": '
    '"grave|media|minore|info", "summary": str, "regularizable": bool, '
    '"evidence_pages": [int]}],\n'
    '  "warnings": [{"text": str, "evidence_pages": [int]}],\n'
    '  "missing_or_uncertain": [str]\n'
    "}\n"
    "Gli importi monetari ('market_value', 'amount', ...) devono essere numeri "
    "(es. 43654.20), non stringhe."
)


# ---------------------------------------------------------------------------
# Prompt building
# ---------------------------------------------------------------------------
def _pages_text(pages: List[Dict[str, Any]], max_chars: int) -> str:
    """Render pages as '=== PAGINA n ===' blocks, truncated to a char budget."""
    parts: List[str] = []
    used = 0
    for entry in pages or []:
        if not isinstance(entry, dict):
            entry = {"text": str(entry or "")}
        try:
            num = int(entry.get("page_number"))
        except Exception:
            num = len(parts) + 1
        text = str(entry.get("text") or "")
        header = f"=== PAGINA {num} ===\n"
        block = header + text + "\n"
        if used + len(block) > max_chars:
            remaining = max_chars - used
            if remaining > len(header):
                parts.append(header + text[: remaining - len(header)])
            parts.append("\n[...TESTO TRONCATO PER LIMITE DI CONTESTO...]")
            break
        parts.append(block)
        used += len(block)
    return "\n".join(parts)


def build_messages(
    pages: List[Dict[str, Any]],
    *,
    max_context_chars: Optional[int] = None,
) -> List[Dict[str, str]]:
    budget = max_context_chars or openai_client.resolve_max_context_chars()
    doc = _pages_text(pages, budget)
    user = (
        "Analizza la seguente perizia ed estrai il foglio di lavoro JSON secondo "
        "lo schema. Cita sempre le pagine in 'evidence_pages'.\n\n"
        f"{doc}"
    )
    return [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": user},
    ]


# ---------------------------------------------------------------------------
# JSON parsing + number coercion
# ---------------------------------------------------------------------------
def _parse_json(text: str) -> Dict[str, Any]:
    text = (text or "").strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, flags=re.S)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError as exc:
                raise AnalystError(
                    f"Worksheet JSON parse failed: {exc}",
                    reason_code="ANALYST_JSON_INVALID",
                )
        raise AnalystError(
            "Worksheet response was not valid JSON.",
            reason_code="ANALYST_JSON_INVALID",
        )


_ITALIAN_NUMBER_RE = re.compile(r"^-?\d{1,3}(\.\d{3})*(,\d+)?$")


def _to_number(value: Any) -> Optional[float]:
    """Coerce a value to float, tolerating Italian-formatted numeric strings."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        raw = value.strip().replace("€", "").replace("EUR", "").replace("eur", "").strip()
        if not raw:
            return None
        if _ITALIAN_NUMBER_RE.match(raw):
            raw = raw.replace(".", "").replace(",", ".")
        else:
            # Plain "43654.20" or "43654,20"
            raw = raw.replace(",", ".") if raw.count(",") == 1 and "." not in raw else raw
        try:
            return float(raw)
        except Exception:
            return None
    return None


def _as_int_list(value: Any) -> List[int]:
    out: List[int] = []
    if isinstance(value, list):
        for v in value:
            try:
                out.append(int(v))
            except Exception:
                continue
    elif value is not None:
        try:
            out.append(int(value))
        except Exception:
            pass
    # Stable, de-duplicated.
    seen = set()
    deduped = []
    for n in out:
        if n not in seen:
            seen.add(n)
            deduped.append(n)
    return deduped


def _as_str_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(v) for v in value if v is not None and str(v).strip()]
    if value is None:
        return []
    return [str(value)] if str(value).strip() else []


def _str_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "si", "sì", "y"}
    if isinstance(value, (int, float)):
        return bool(value)
    return default


# ---------------------------------------------------------------------------
# Normalization into the canonical worksheet shape
# ---------------------------------------------------------------------------
def _normalize_money_items(items: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not isinstance(items, list):
        return out
    for it in items:
        if not isinstance(it, dict):
            continue
        out.append(
            {
                "label": _str_or_none(it.get("label")) or "",
                "amount": _to_number(it.get("amount")),
                "evidence_pages": _as_int_list(it.get("evidence_pages")),
            }
        )
    return out


def _normalize_uncertain_money(items: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not isinstance(items, list):
        return out
    for it in items:
        if not isinstance(it, dict):
            continue
        amount = _to_number(it.get("amount"))
        if amount is None:
            continue
        out.append(
            {
                "label": _str_or_none(it.get("label")) or "",
                "amount": amount,
                "reason": _str_or_none(it.get("reason")),
                "evidence_pages": _as_int_list(it.get("evidence_pages")),
            }
        )
    return out


def _normalize_auction_terms(value: Any) -> Dict[str, Any]:
    at = value or {}
    if not isinstance(at, dict):
        at = {}
    return {
        "prezzo_base_asta": _to_number(
            at.get("prezzo_base_asta") or at.get("prezzo_base") or at.get("base_asta")
        ),
        "offerta_minima": _to_number(at.get("offerta_minima") or at.get("offerta_min")),
        "rialzo_minimo": _to_number(at.get("rialzo_minimo") or at.get("rilancio_minimo")),
        "cauzione": _to_number(at.get("cauzione")),
        "evidence_pages": _as_int_list(at.get("evidence_pages")),
    }


def normalize_worksheet(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Coerce a raw model dict into the canonical worksheet shape (generic)."""
    if not isinstance(raw, dict):
        raise AnalystError(
            "Worksheet root is not an object.",
            reason_code="ANALYST_SCHEMA_INVALID",
        )

    ci = raw.get("case_identity") or {}
    case_identity = {
        "tribunale": _str_or_none(ci.get("tribunale")),
        "procedura_rge": _str_or_none(ci.get("procedura_rge") or ci.get("procedura") or ci.get("rge")),
        "lotto": _str_or_none(ci.get("lotto")),
        "address": _str_or_none(ci.get("address") or ci.get("indirizzo")),
        "property_type": _str_or_none(ci.get("property_type") or ci.get("tipologia")),
        "ownership_right": _str_or_none(ci.get("ownership_right") or ci.get("diritto")),
        "evidence_pages": _as_int_list(ci.get("evidence_pages")),
    }

    oc = raw.get("occupancy") or {}
    occupancy = {
        "status": _str_or_none(oc.get("status") or oc.get("stato")),
        "title_info": _str_or_none(oc.get("title_info")),
        "opponibility": _str_or_none(oc.get("opponibility") or oc.get("opponibilita")),
        "registration_dates": _as_str_list(oc.get("registration_dates")),
        "expiry_dates": _as_str_list(oc.get("expiry_dates")),
        "risks": _as_str_list(oc.get("risks")),
        "evidence_pages": _as_int_list(oc.get("evidence_pages")),
    }

    tech_in = raw.get("technical_compliance")
    technical_compliance: List[Dict[str, Any]] = []
    if isinstance(tech_in, list):
        for item in tech_in:
            if not isinstance(item, dict):
                continue
            classification = str(item.get("classification") or "uncertain").strip().lower()
            if classification not in COMPLIANCE_CLASSES:
                classification = "uncertain"
            technical_compliance.append(
                {
                    "area": _str_or_none(item.get("area")) or "",
                    "classification": classification,
                    "blocks_saleability": _as_bool(item.get("blocks_saleability"), False),
                    "cost": _to_number(item.get("cost")),
                    "timing": _str_or_none(item.get("timing")),
                    "notes": _str_or_none(item.get("notes")),
                    "evidence_pages": _as_int_list(item.get("evidence_pages")),
                }
            )

    mo = raw.get("money") or {}
    money = {
        "market_value": _to_number(mo.get("market_value")),
        # Legacy field kept for back-compat; prezzo base now lives in auction_terms.
        "base_auction_value": _to_number(mo.get("base_auction_value")),
        "deductions": _normalize_money_items(mo.get("deductions")),
        "regularization_costs": _to_number(mo.get("regularization_costs")),
        "current_state_value": _to_number(mo.get("current_state_value")),
        "auction_terms": _normalize_auction_terms(mo.get("auction_terms")),
        "buyer_side_costs": _normalize_money_items(mo.get("buyer_side_costs")),
        "procedure_cancelled_costs": _normalize_money_items(mo.get("procedure_cancelled_costs")),
        "uncertain_money": _normalize_uncertain_money(mo.get("uncertain_money")),
        "cancellation_costs": _to_number(mo.get("cancellation_costs")),
        "sale_value": _to_number(mo.get("sale_value")),
        "evidence_pages": _as_int_list(mo.get("evidence_pages")),
    }

    legal_in = raw.get("legal_formalities")
    legal_formalities: List[Dict[str, Any]] = []
    if isinstance(legal_in, list):
        for item in legal_in:
            if not isinstance(item, dict):
                continue
            ftype = str(item.get("type") or "other").strip().lower()
            if ftype not in FORMALITY_TYPES:
                ftype = "other"
            legal_formalities.append(
                {
                    "type": ftype,
                    "description": _str_or_none(item.get("description")),
                    "cancelled_by_procedure": _as_bool(item.get("cancelled_by_procedure"), False),
                    "buyer_burden": _as_bool(item.get("buyer_burden"), False),
                    "amount": _to_number(item.get("amount")),
                    "evidence_pages": _as_int_list(item.get("evidence_pages")),
                }
            )

    lots_in = raw.get("lots")
    lots_out: List[Dict[str, Any]] = []
    if isinstance(lots_in, list):
        for item in lots_in:
            if not isinstance(item, dict):
                continue
            lots_out.append(
                {
                    "lot_id": _str_or_none(item.get("lot_id") or item.get("lotto") or item.get("id")),
                    "label": _str_or_none(item.get("label") or item.get("titolo")),
                    "address": _str_or_none(item.get("address") or item.get("indirizzo")),
                    "property_type": _str_or_none(item.get("property_type") or item.get("tipologia")),
                    "ownership_right": _str_or_none(item.get("ownership_right") or item.get("diritto")),
                    "prezzo_base_asta": _to_number(item.get("prezzo_base_asta") or item.get("prezzo_base")),
                    "sale_value": _to_number(item.get("sale_value")),
                    "occupancy_status": _str_or_none(item.get("occupancy_status") or item.get("stato_occupativo")),
                    "evidence_pages": _as_int_list(item.get("evidence_pages")),
                }
            )

    risk_in = raw.get("risk_classification")
    risk_classification: List[Dict[str, Any]] = []
    if isinstance(risk_in, list):
        for item in risk_in:
            if not isinstance(item, dict):
                continue
            severity = str(item.get("severity") or "info").strip().lower()
            if severity not in SEVERITY_LEVELS:
                severity = "info"
            risk_classification.append(
                {
                    "area": _str_or_none(item.get("area")) or "",
                    "severity": severity,
                    "summary": _str_or_none(item.get("summary")) or "",
                    "regularizable": _as_bool(item.get("regularizable"), False),
                    "evidence_pages": _as_int_list(item.get("evidence_pages")),
                }
            )

    warnings_in = raw.get("warnings")
    warnings: List[Dict[str, Any]] = []
    if isinstance(warnings_in, list):
        for item in warnings_in:
            if isinstance(item, dict):
                text = _str_or_none(item.get("text"))
                if text:
                    warnings.append(
                        {"text": text, "evidence_pages": _as_int_list(item.get("evidence_pages"))}
                    )
            elif item is not None and str(item).strip():
                warnings.append({"text": str(item).strip(), "evidence_pages": []})

    return {
        "schema_version": WORKSHEET_SCHEMA_VERSION,
        "case_identity": case_identity,
        "lots": lots_out,
        "occupancy": occupancy,
        "technical_compliance": technical_compliance,
        "money": money,
        "legal_formalities": legal_formalities,
        "risk_classification": risk_classification,
        "warnings": warnings,
        "missing_or_uncertain": _as_str_list(raw.get("missing_or_uncertain")),
    }


# ---------------------------------------------------------------------------
# Orchestration entry point
# ---------------------------------------------------------------------------
class AnalystResult:
    def __init__(
        self,
        *,
        worksheet: Dict[str, Any],
        redacted_request: Dict[str, Any],
        response_artifact: Dict[str, Any],
        model: str,
    ):
        self.worksheet = worksheet
        self.redacted_request = redacted_request
        self.response_artifact = response_artifact
        self.model = model


def run_analyst(
    pages: List[Dict[str, Any]],
    *,
    openai_caller: Optional[OpenAICaller] = None,
    model: Optional[str] = None,
    max_context_chars: Optional[int] = None,
) -> AnalystResult:
    """
    Generate and normalize the analyst worksheet.

    ``openai_caller`` defaults to :func:`openai_client.call_openai_json` and is
    injected in tests with a fake. It must return a dict with a ``content`` JSON
    string (and may include model/usage/finish_reason).

    Raises :class:`AnalystError` (which the orchestrator maps to a fail-closed
    status) on any OpenAI or parsing failure.
    """
    caller = openai_caller or openai_client.call_openai_json
    resolved_model = model or openai_client.resolve_model()
    messages = build_messages(pages, max_context_chars=max_context_chars)
    redacted = openai_client.redacted_request(messages, model=resolved_model)

    try:
        result = caller(messages, model=resolved_model)
    except OpenAIClientError as exc:
        raise AnalystError(str(exc), reason_code=getattr(exc, "reason_code", "OPENAI_CALL_FAILED"))
    except Exception as exc:
        raise AnalystError(
            f"Unexpected OpenAI caller error: {type(exc).__name__}: {exc}",
            reason_code="OPENAI_CALL_FAILED",
        )

    if not isinstance(result, dict) or "content" not in result:
        raise AnalystError(
            "OpenAI caller returned an unexpected result shape.",
            reason_code="OPENAI_RESPONSE_MALFORMED",
        )

    raw_content = result.get("content") or ""
    parsed = _parse_json(raw_content)
    worksheet = normalize_worksheet(parsed)

    response_artifact = {
        "model": result.get("model") or resolved_model,
        "finish_reason": result.get("finish_reason"),
        "usage": result.get("usage"),
        "response_id": result.get("response_id"),
        "raw_content": raw_content,
    }

    return AnalystResult(
        worksheet=worksheet,
        redacted_request=redacted,
        response_artifact=response_artifact,
        model=str(result.get("model") or resolved_model),
    )
