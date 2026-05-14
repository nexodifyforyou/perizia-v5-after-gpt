"""Feature-flagged authority projection for customer Money Box output.

This module is intentionally narrow: it only rewrites customer-facing money
structures when AUTHORITY_MONEY_PROJECTION_ENABLED is exactly "1". Authority
classification remains the source of truth for what may be surfaced, while all
debug/projection metadata is attached under result["debug"] for the existing
customer sanitizer to remove from outbound API payloads.
"""

from __future__ import annotations

import copy
import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from perizia_authority_resolvers import build_authority_shadow_resolvers
from perizia_section_authority import build_section_authority_map


FEATURE_FLAG = "AUTHORITY_MONEY_PROJECTION_ENABLED"

SAFE_MONEY_STATUSES = {"OK", "PARTIAL"}
NON_BUYER_COST_ROLES = {
    "valuation_deduction",
    "price",
    "base_auction",
    "final_value",
    "market_value",
    "cadastral_rendita",
    "formalities_procedural_amount",
}
BUYER_SIGNAL_ROLES = {"buyer_cost_signal_to_verify", "condominium_arrears"}
STALE_REGOLARIZZAZIONE_RE = re.compile(
    r"(?<!tempi necessari per la )(?<!tempo necessario per la )regolarizzazion\w*\s*:\s*(?:€|\beuro\b)?\s*(?:31|6)(?:[,\.]00)?\b(?!\s*(?:mesi?|giorni?|anni?)\b)",
    re.IGNORECASE,
)
GENERIC_REGOLARIZZAZIONE_CERTAINTY_RE = re.compile(
    r"\b(?<!tempi necessari per la )(?<!tempo necessario per la )regolarizzazion\w*\s*:\s*(?:€|\beuro\b)?\s*\d+(?:[\.,]\d+)?\b(?!\s*(?:mesi?|giorni?|anni?)\b)",
    re.IGNORECASE,
)
MONEY_AMOUNT_RE = re.compile(r"(?:€|\beuro\b)\s*\d|\d[\d\.\s]*,\d{2}\b", re.IGNORECASE)
MONEY_QA_TOPIC_RE = re.compile(
    r"\b(?:costi?|spese?|oneri?|import[oi]|regolarizzazion\w*|sanatori\w*|ripristin\w*|"
    r"fiscalizzazion\w*|formal(?:it|i)à?|ipotec\w*|pignorament\w*|rendita\s+catastal\w*|"
    r"prezzo\s+base|base\s+d['’]?\s*asta|valore\s+(?:di\s+)?stima|valore\s+finale|"
    r"market\s+value|deprezzament\w*|totale\s+(?:stimato|costi?|extra|oneri?|spese?))\b",
    re.IGNORECASE,
)
BUYER_COST_CERTAINTY_RE = re.compile(
    r"\b(?:costo\s+certo|costi?\s+(?:extra|espliciti|a\s+carico)|a\s+carico\s+(?:dell['’]?)?"
    r"(?:acquirente|aggiudicatario)|buyer[-\s]?side|extra\s+cost|totale\s+(?:stimato|costi?|extra|oneri?|spese?))\b",
    re.IGNORECASE,
)
NON_BUYER_COST_AS_BUYER_RE = re.compile(
    r"\b(?:formal(?:it|i)à?|ipotec\w*|pignorament\w*|rendita\s+catastal\w*|prezzo\s+base|"
    r"base\s+d['’]?\s*asta|valore\s+(?:di\s+)?stima|valore\s+finale|market\s+value|"
    r"deprezzament\w*)\b.*\b(?:costi?|spese?|oneri?|a\s+carico|acquirente|aggiudicatario|extra)\b",
    re.IGNORECASE,
)
QA_MONEY_TEXT_FIELDS = {
    "current_wrong_claim",
    "claim",
    "message",
    "text",
    "problem_it",
    "description",
    "detail",
    "details",
}
QA_CLAIM_LIST_MARKERS = {"contradiction", "warning", "warn", "claim", "qa_gate"}


def _base_meta(enabled: bool) -> Dict[str, Any]:
    return {
        "enabled": enabled,
        "status": "DISABLED" if not enabled else "NOT_EVALUATED",
        "applied": False,
        "reason": "feature_flag_disabled" if not enabled else "",
        "money_status": "unknown",
        "authority_confidence": 0.0,
        "candidate_count": 0,
        "projected_items_count": 0,
        "cost_signals_to_verify_count": 0,
        "excluded_non_buyer_cost_count": 0,
        "valuation_reference_count": 0,
        "component_total_double_count_prevented": False,
        "stale_money_removed": False,
        "changed_fields": [],
        "notes": [],
    }


def _deepclone(value: Any) -> Any:
    return copy.deepcopy(value)


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _read_json(path: Path) -> Any:
    try:
        import json

        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _load_candidate_artifacts(candidate_artifacts: Any) -> Dict[str, Any]:
    if isinstance(candidate_artifacts, dict):
        return copy.deepcopy(candidate_artifacts)
    if not candidate_artifacts:
        return {}
    folder = Path(str(candidate_artifacts))
    out: Dict[str, Any] = {}
    for key, filename in (("money", "candidates_money.json"), ("triggers", "candidates_triggers.json")):
        payload = _read_json(folder / filename)
        if isinstance(payload, list):
            out[key] = payload
        elif payload is not None:
            out[key] = payload
    return out


def _shadow_from_inputs(
    pages_raw: Optional[Sequence[Dict[str, Any]]],
    section_authority_map: Any,
    candidate_artifacts: Any,
    authority_shadow: Optional[Dict[str, Any]],
) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    if isinstance(authority_shadow, dict) and isinstance(authority_shadow.get("money_roles"), dict):
        return copy.deepcopy(authority_shadow), ["reused_authority_shadow"]

    if isinstance(section_authority_map, dict):
        status = str(section_authority_map.get("_authority_tagging_status") or "").strip()
        if status in {"missing_map", "corrupt_map"}:
            return None, [f"section_authority_{status}"]
    if not isinstance(pages_raw, Sequence) or isinstance(pages_raw, (str, bytes)) or not pages_raw:
        return None, ["missing_pages_raw"]

    try:
        section_map = section_authority_map if isinstance(section_authority_map, dict) else build_section_authority_map(list(pages_raw))
        shadow = build_authority_shadow_resolvers(
            list(pages_raw),
            section_map,
            candidates=_load_candidate_artifacts(candidate_artifacts),
        )
        notes = ["built_authority_shadow_from_inputs"]
        if not isinstance(section_authority_map, dict):
            notes.append("rebuilt_section_authority_from_pages")
        return shadow, notes
    except Exception as exc:
        return None, [f"authority_shadow_build_failed:{str(exc)[:160]}"]


def _money_row(authority_shadow: Dict[str, Any]) -> Dict[str, Any]:
    row = authority_shadow.get("money_roles") if isinstance(authority_shadow, dict) else {}
    return row if isinstance(row, dict) else {}


def _money_value(money_row: Dict[str, Any]) -> Dict[str, Any]:
    value = money_row.get("value") if isinstance(money_row, dict) else {}
    return value if isinstance(value, dict) else {}


def _candidate_amount(candidate: Dict[str, Any]) -> Optional[float]:
    try:
        amount = float(candidate.get("amount_eur"))
    except Exception:
        return None
    if amount <= 0:
        return None
    return amount


def _amount_label(amount: Optional[float]) -> str:
    if amount is None:
        return ""
    rounded = int(round(float(amount)))
    return f"€ {rounded:,.0f}".replace(",", ".")


def _evidence_from_candidate(candidate: Dict[str, Any]) -> List[Dict[str, Any]]:
    quote = _normalize_text(candidate.get("raw_text"))
    evidence: Dict[str, Any] = {}
    try:
        page = int(candidate.get("page"))
        if page > 0:
            evidence["page"] = page
    except Exception:
        pass
    if quote:
        evidence["quote"] = quote[:500]
    return [evidence] if evidence else []


def _candidate_sort_key(candidate: Dict[str, Any]) -> Tuple[int, float, str]:
    try:
        page = int(candidate.get("page") or 0)
    except Exception:
        page = 0
    amount = _candidate_amount(candidate) or 0.0
    return page, amount, _normalize_text(candidate.get("raw_text"))[:80]


def _dedupe_candidates(candidates: Iterable[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen = set()
    for candidate in sorted((item for item in candidates if isinstance(item, dict)), key=_candidate_sort_key):
        sig = (
            str(candidate.get("role") or ""),
            round(float(candidate.get("amount_eur") or 0.0), 2),
            _normalize_text(candidate.get("raw_text"))[:120],
        )
        if sig in seen:
            continue
        seen.add(sig)
        out.append(candidate)
        if len(out) >= limit:
            break
    return out


def _consolidate_repeated_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Group items with the same (amount, label_role, lot_label) across multiple
    pages into a single visible item carrying the union of evidence pages.

    This prevents the same mortgage / cadastral value / formality from rendering
    six times when it is repeated on consecutive pages.
    """
    if not items:
        return items
    grouped: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    order: List[Tuple[Any, ...]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        try:
            amount_key = round(float(item.get("amount_eur") or 0.0), 2)
        except Exception:
            amount_key = 0.0
        key = (
            amount_key,
            str(item.get("label_role") or ""),
            str(item.get("lot_label") or ""),
        )
        existing = grouped.get(key)
        if existing is None:
            grouped[key] = copy.deepcopy(item)
            order.append(key)
            continue
        # Merge evidence pages
        ev_list = existing.get("evidence") or []
        if not isinstance(ev_list, list):
            ev_list = []
        for ev in item.get("evidence") or []:
            if isinstance(ev, dict) and ev not in ev_list:
                ev_list.append(ev)
        existing["evidence"] = ev_list
        pages_seen: List[int] = []
        for ev in ev_list:
            page = ev.get("page") if isinstance(ev, dict) else None
            if isinstance(page, int) and page > 0 and page not in pages_seen:
                pages_seen.append(page)
        if pages_seen:
            existing["pages"] = pages_seen
            if existing.get("page") is None:
                existing["page"] = pages_seen[0]
        existing.setdefault("merged_count", 1)
        existing["merged_count"] = int(existing.get("merged_count") or 1) + 1
    return [grouped[key] for key in order]


def _role_label(candidate: Dict[str, Any]) -> str:
    role = str(candidate.get("role") or "")
    base_role = str(candidate.get("semantic_base_role") or "")
    if role == "condominium_arrears" or base_role == "condominium_arrears":
        return "Spese condominiali da verificare"
    if role in {"total_candidate", "buyer_cost_signal_to_verify"} or base_role == "buyer_cost_signal_to_verify":
        return "Costo da verificare"
    return "Importo segnalato in perizia, debenza da verificare"


def _cost_signal_payload(candidate: Dict[str, Any], index: int, *, safe_cost: bool) -> Dict[str, Any]:
    amount = _candidate_amount(candidate)
    label = _role_label(candidate)
    amount_text = _amount_label(amount)
    if amount_text:
        label = f"{label}: {amount_text}"
    note = (
        "Importo segnalato in perizia, debenza da verificare prima dell'offerta."
        if not safe_cost
        else "Importo indicato con obbligo buyer-side esplicito in perizia; verificare comunque con tecnico/delegato."
    )
    payload = {
        "code": f"AUTH_COST_VERIFY_{index:02d}",
        "label_it": label,
        "label_en": label,
        "type": "SIGNAL_TO_VERIFY" if not safe_cost else "ESTIMATE",
        "stima_euro": int(round(amount)) if amount is not None else None,
        "stima_nota": note,
        "note_it": note,
        "additive_to_extra_total": False,
        "contract_state": "cost_signal_to_verify" if not safe_cost else "quantified_estimate",
        "customer_visible_amount_status": "to_verify" if not safe_cost else "explicit_buyer_obligation",
        "evidence": _evidence_from_candidate(candidate),
        "fonte_perizia": {"value": "Perizia", "evidence": _evidence_from_candidate(candidate)},
    }
    return payload


def _excluded_payload(candidate: Dict[str, Any], index: int) -> Dict[str, Any]:
    role = str(candidate.get("role") or "")
    amount = _candidate_amount(candidate)
    amount_text = _amount_label(amount)
    if role == "cadastral_rendita":
        label = "Rendita catastale: dato fiscale, non costo per l'acquirente"
    elif role == "formalities_procedural_amount":
        label = "Formalita/cancellazione: importo procedurale, non trattato come costo extra certo"
    else:
        label = "Importo valutativo, non costo extra"
    if amount_text:
        label = f"{label}: {amount_text}"
    return {
        "code": f"AUTH_EXCLUDED_{index:02d}",
        "label_it": label,
        "label_en": label,
        "amount_eur": int(round(amount)) if amount is not None else None,
        "role": role,
        "note_it": "Non trattato come costo extra certo per l'acquirente.",
        "evidence": _evidence_from_candidate(candidate),
    }


def _legacy_money_text(result: Dict[str, Any]) -> str:
    pieces: List[str] = []

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for item in value:
                walk(item)
        else:
            pieces.append(str(value or ""))

    for path in (
        ("money_box",),
        ("section_3_money_box",),
        ("customer_decision_contract", "money_box"),
        ("customer_decision_contract", "section_3_money_box"),
    ):
        cur: Any = result
        for part in path:
            cur = cur.get(part) if isinstance(cur, dict) else None
        walk(cur)
    return " ".join(pieces)


def _money_box_has_projected_downgrades(money_box: Dict[str, Any]) -> bool:
    if not isinstance(money_box, dict):
        return False
    if money_box.get("policy") != "AUTHORITY_CONSERVATIVE":
        return False
    downgrade_keys = (
        "cost_signals_to_verify",
        "buyer_cost_signals_to_verify",
        "qualitative_burdens",
        "valuation_reference_amounts",
        "valuation_references",
        "valuation_deductions",
        "price_references",
        "cadastral_values",
        "formalities_and_procedural_amounts",
        "other_monetary_mentions",
        "excluded_non_buyer_cost_amounts",
        "unsupported_or_unknown_amounts",
    )
    return any(isinstance(money_box.get(key), list) and bool(money_box.get(key)) for key in downgrade_keys)


def _qa_list_path_is_customer_warning_or_contradiction(path: str) -> bool:
    path_text = str(path or "").lower()
    return any(marker in path_text for marker in QA_CLAIM_LIST_MARKERS)


def _qa_money_claim_texts(item: Dict[str, Any]) -> List[str]:
    texts: List[str] = []
    if not isinstance(item, dict):
        return texts
    for key, value in item.items():
        key_text = str(key or "")
        if key_text in QA_MONEY_TEXT_FIELDS or key_text.endswith("_claim") or key_text.endswith("_message"):
            if isinstance(value, (dict, list)):
                continue
            normalized = _normalize_text(value)
            if normalized:
                texts.append(normalized)
    return texts


def _is_stale_money_qa_claim(item: Dict[str, Any], projected_money_box: Dict[str, Any]) -> bool:
    if not _money_box_has_projected_downgrades(projected_money_box):
        return False
    for text in _qa_money_claim_texts(item):
        if GENERIC_REGOLARIZZAZIONE_CERTAINTY_RE.search(text):
            return True
        if re.search(r"\btotale\s+stimato\s+in\s+perizia\s*:\s*(?:€|\beuro\b)?\s*\d", text, re.IGNORECASE):
            return True
        if NON_BUYER_COST_AS_BUYER_RE.search(text):
            return True
        if MONEY_AMOUNT_RE.search(text) and MONEY_QA_TOPIC_RE.search(text) and BUYER_COST_CERTAINTY_RE.search(text):
            return True
    return False


def _sanitize_stale_money_qa_claims_after_projection(result: Dict[str, Any], projected_money_box: Dict[str, Any]) -> Dict[str, Any]:
    meta: Dict[str, Any] = {
        "removed_money_qa_claims_count": 0,
        "removed_paths": [],
        "reason_codes": [],
    }
    if not isinstance(result, dict) or not _money_box_has_projected_downgrades(projected_money_box):
        return meta

    def walk(value: Any, path: str) -> Any:
        if isinstance(value, dict):
            for key in list(value.keys()):
                value[key] = walk(value.get(key), f"{path}.{key}")
            return value
        if isinstance(value, list):
            cleaned: List[Any] = []
            claim_list = _qa_list_path_is_customer_warning_or_contradiction(path)
            for idx, item in enumerate(value):
                item_path = f"{path}[{idx}]"
                if claim_list and isinstance(item, dict) and _is_stale_money_qa_claim(item, projected_money_box):
                    meta["removed_money_qa_claims_count"] += 1
                    meta["removed_paths"].append(item_path)
                    meta["reason_codes"].append("STALE_MONEY_QA_CLAIM_REMOVED")
                    continue
                cleaned.append(walk(item, item_path))
            return cleaned
        return value

    walk(result.get("qa_gate"), "result.qa_gate")
    customer_contract = result.get("customer_decision_contract")
    if isinstance(customer_contract, dict):
        walk(customer_contract.get("qa_gate"), "result.customer_decision_contract.qa_gate")
    for key in list(result.keys()):
        key_text = str(key or "").lower()
        if key_text == "qa_gate" or key_text == "customer_decision_contract":
            continue
        if any(marker in key_text for marker in ("contradiction", "warning", "warn", "claim")):
            result[key] = walk(result.get(key), f"result.{key}")

    meta["removed_paths"] = list(dict.fromkeys(str(path) for path in meta["removed_paths"]))
    meta["reason_codes"] = list(dict.fromkeys(str(code) for code in meta["reason_codes"]))
    return meta


_TEXT_NORMALIZE_TR = str.maketrans({"’": "'", "‘": "'", "`": "'", "´": "'"})


def _money_norm(text: Any) -> str:
    raw = str(text or "").translate(_TEXT_NORMALIZE_TR)
    return re.sub(r"\s+", " ", raw).strip().lower()


_BUYER_OBLIGATION_PATTERNS = (
    re.compile(r"\ba\s+carico\s+(?:dell['\s]?\s*)?(?:aggiudicatario|acquirente|parte\s+acquirente|nuovo\s+proprietario)\b", re.IGNORECASE),
    re.compile(r"\b(?:onere|oneri|spese|costi|importi)\s+a\s+carico\s+(?:dell['\s]?)?(?:aggiudicatario|acquirente)", re.IGNORECASE),
    re.compile(r"\b(?:dovr[aà]|dovranno|tenuto\s+a|tenuti\s+a)\s+sostenere\b", re.IGNORECASE),
    re.compile(r"\b(?:da|deve|devono)\s+sostenere\b", re.IGNORECASE),
    re.compile(r"\brestano?\s+a\s+carico\s+(?:dell['\s]?)?(?:aggiudicatario|acquirente)", re.IGNORECASE),
    re.compile(r"\bsono\s+a\s+carico\s+(?:dell['\s]?)?(?:aggiudicatario|acquirente)", re.IGNORECASE),
    re.compile(r"\bspese\s+condominiali\s+(?:insolute|arretrate|scadute|morose|non\s+pagate)\b", re.IGNORECASE),
)
_BUYER_OBLIGATION_PROXIMITY = re.compile(
    r"\b(?:sanatoria|oblazione|sanzione|diritti)\b[^.\n]{0,80}\b"
    r"(?:a\s+carico\s+(?:dell['\s]?)?(?:aggiudicatario|acquirente)|aggiudicatario|acquirente|sostenere)\b",
    re.IGNORECASE,
)
_VALUATION_ARITHMETIC_PATTERNS = (
    re.compile(r"\d[\d\.\s]*(?:,\d{1,3})?\s*(?:mq|m²|metri\s+quadr\w*|ha|ettar\w*)\b", re.IGNORECASE),
    re.compile(r"€\s*/\s*(?:mq|m²|ha|ettaro)\b", re.IGNORECASE),
    re.compile(r"\b(?:€/mq|€/ha|euro/mq|euro/ha)\b", re.IGNORECASE),
    re.compile(r"€?\s*\d[\d\.\s]*(?:,\d{1,3})?\s*[xX×\*]\s*€?\s*[\d/]", re.IGNORECASE),
    re.compile(r"€?\s*\d[\d\.\s]*(?:,\d{1,3})?\s*[-−]\s*€?\s*\d[\d\.\s]*(?:,\d{1,3})?\s*=\s*€?\s*\d", re.IGNORECASE),
    re.compile(r"€?\s*\d[\d\.\s]*(?:,\d{1,3})?\s*\+\s*€?\s*\d[\d\.\s]*(?:,\d{1,3})?\s*=\s*€?\s*\d", re.IGNORECASE),
)
_VALUATION_DEDUCTION_RE = re.compile(
    r"\b(?:valore\s+(?:viene|e['\s]?\s+stato|sara['\s]?|risulta)\s+(?:decurtat|abbattut|ridott|deprezzat|adeguat|corret)\w*|"
    r"decurtazion\w*|decurtat[oaie]|abbattiment\w*|deprezzament\w*|deprezzat[oaie]|"
    r"detrazion\w*|detratt[oaie]|riduzion\w*\s+(?:di|del)\s+valore|"
    r"adeguament\w*\s+e\s+correzion\w*|arrotondat[oaie]\s+a)\b",
    re.IGNORECASE,
)
_PRICE_REFERENCE_RE = re.compile(
    r"\b(?:prezzo\s+base|base\s+d['\s]?\s*asta|offerta\s+minima|prezzo\s+(?:di\s+)?vendita|"
    r"valore\s+(?:di\s+)?(?:stima|mercato|venale|cauzionale|commerciale|finale|complessivo)|"
    r"valore\s+arrotondat\w*|market\s+value|valore\s+(?:dell['\s]?immobile|dei?\s+beni?))\b",
    re.IGNORECASE,
)
_CADASTRAL_RE = re.compile(
    r"\b(?:rendita\s+catastal\w*|rendita\b[^.\n]{0,80}\b(?:catasto|catastale|foglio|particella|subalterno|"
    r"mappale|categoria|classe|vani)|valore\s+catastal\w*|valore\s+ai\s+fini\s+catastal\w*)\b",
    re.IGNORECASE,
)
_FORMALITY_RE = re.compile(
    r"\b(?:formal(?:it|i)\w*|ipotec\w*|pignorament\w*|cancellazion\w*\s+(?:di\s+)?(?:ipotec|trascriz|formal)|"
    r"trascrizion\w*\s+pregiudizievol|iscrizion\w*\s+ipotecari|registro\s+(?:generale|particolare)|"
    r"procedura\s+(?:esecutiva|concorsual)|spese\s+procedural\w*)\b",
    re.IGNORECASE,
)
_REGOLARIZZAZIONE_MENTION_RE = re.compile(
    r"\b(?:regolarizzazion\w*|sanatori\w*|oblazion\w*|sanzion\w*|fiscalizzazion\w*|"
    r"docfa|tipo\s+mappale|ripristin\w*|demolizion\w*|spese\s+tecnich\w*)\b",
    re.IGNORECASE,
)
_LAVORI_ULTIMATI_RE = re.compile(
    r"\b(?:valore\s+(?:considerato\s+)?a\s+lavori\s+ultimat\w*|a\s+lavori\s+ultimat\w*|"
    r"valore\s+come\s+sopra\s+determinat\w*|valore\s+ipotetic\w*\s+a\s+lavori)\b",
    re.IGNORECASE,
)

# Strong economic/legal keywords that justify keeping a small amount as a real money
# mention rather than demoting it as OCR/regex noise.
_STRONG_MONEY_KEYWORD_RE = re.compile(
    r"\b(?:imposta|bollo|tassa\w*|diritt\w*|spes\w*|oner\w*|cancellazion\w*|"
    r"ipotec\w*|formal(?:it|i)\w*|tribut\w*|sanzion\w*|sanatori\w*|oblazion\w*|"
    r"cost\w*|pagament\w*|debit\w*|prezzo|valor\w*|stima|deprezzament\w*|"
    r"decurtazion\w*|rendita|extra|condominial\w*|insolut\w*|arretrat\w*|"
    r"morosit\w*|esecutiv\w*|procedural\w*)\b",
    re.IGNORECASE,
)

# Cadastral/table identification terms — a candidate dominated by these terms with
# no economic/legal keyword is most likely OCR noise from the cadastral table rows.
_CADASTRAL_TABLE_TERMS_RE = re.compile(
    r"\b(?:foglio|particell\w*|part\.|sub\.?|subaltern\w*|categori\w*|classe|"
    r"consistenz\w*|superficie\s+catastal\w*|zona\s+censuari\w*|graffat\w*|"
    r"vani|mappal\w*|dati\s+identificativ\w*|sezione)\b",
    re.IGNORECASE,
)

# Cadastral row HEADER patterns (table row signatures) — when present without
# strong money keywords the amount is almost certainly OCR/regex noise.
_CADASTRAL_TABLE_HEADER_RE = re.compile(
    r"(?:dati\s+identificativ\w*[^.\n]{0,160}part(?:\.|icella)|"
    r"foglio[^.\n]{0,40}part(?:\.|icella)|"
    r"part(?:\.|icella)[^.\n]{0,30}sub(?:\.|alterno)|"
    r"sub(?:\.|alterno)[^.\n]{0,30}zona\s+cens|"
    r"categori\w*[^.\n]{0,40}classe[^.\n]{0,40}consistenz|"
    r"superficie\s+catastal\w*[^.\n]{0,40}rendita)",
    re.IGNORECASE,
)

# Broken token patterns (e.g., "€ T-1", "T-1", "F1", "A/3", "mappale", isolated "€1")
# — common OCR/regex fragments that should not be promoted as money.
_BROKEN_FRAGMENT_RE = re.compile(
    r"^\s*€?\s*(?:[TtFfABCDE]\s*-\s*\d{1,3}|A\s*/\s*\d{1,3}|F\d{1,3}|"
    r"mappal\w*|sub(?:\.|alterno)?|particell\w*)\s*[,;.\s]*$",
    re.IGNORECASE,
)

# Standalone tiny euro shards: "€1", "€ 2" with no other money context.
_ISOLATED_EURO_SHARD_RE = re.compile(r"^\s*€\s*\d{1,2}\s*$")


def is_explicit_buyer_obligation(quote: Any) -> bool:
    """True iff the quote contains explicit buyer-side obligation/exposure language.

    Strict rule: a mention of "regolarizzazione/sanzione/sanatoria" alone is NOT enough.
    The phrase must tie the cost to the buyer (aggiudicatario / acquirente) or to
    explicit "sostenere/a carico" language. Condominium arrears require the
    "insolute/arretrate/scadute" qualifier.
    """
    norm = _money_norm(quote)
    if not norm:
        return False
    for pattern in _BUYER_OBLIGATION_PATTERNS:
        if pattern.search(norm):
            return True
    return bool(_BUYER_OBLIGATION_PROXIMITY.search(norm))


def is_valuation_arithmetic_context(quote: Any, amount_raw: Any = None) -> bool:
    """True iff the quote shows valuation arithmetic (mq×€/mq, +/- = formulas)."""
    norm = _money_norm(quote)
    if not norm:
        return False
    return any(pattern.search(norm) for pattern in _VALUATION_ARITHMETIC_PATTERNS)


def is_price_reference_context(quote: Any) -> bool:
    return bool(_PRICE_REFERENCE_RE.search(_money_norm(quote)))


def is_valuation_deduction_context(quote: Any) -> bool:
    return bool(_VALUATION_DEDUCTION_RE.search(_money_norm(quote)))


def is_cadastral_context(quote: Any) -> bool:
    return bool(_CADASTRAL_RE.search(_money_norm(quote)))


def is_formality_procedural_context(quote: Any) -> bool:
    return bool(_FORMALITY_RE.search(_money_norm(quote)))


def _has_page_evidence(candidate: Dict[str, Any]) -> bool:
    try:
        page = int(candidate.get("page") or 0)
    except Exception:
        page = 0
    quote = _normalize_text(candidate.get("raw_text"))
    return page > 0 and bool(quote)


# Amounts that are commonly OCR fragments / table cell mis-reads when no clear
# tax/fee/legal context surrounds them. The list is intentionally narrow: any
# such amount is only demoted when the surrounding quote lacks economic/legal
# keywords like tassa, bollo, diritti, imposta, costo, ipoteca, cancellazione.
_SUSPICIOUS_TINY_AMOUNTS = {1, 2, 3, 5, 9, 10, 12, 24, 35, 59, 100}

_UNIT_PRICE_CONTEXT_RE = re.compile(
    r"€\s*/\s*(?:mq|m²|m2|ha|ettar|mc|m3)\b|\b(?:euro\s+per|al)\s+(?:mq|m²|m2|ha|mc)\b|"
    r"\d+(?:[,\.]\d+)?\s*(?:mq|m²|m2|ha|ettar|mc|m3)\s*[xX×]\s*€?\s*\d",
    re.IGNORECASE,
)


def is_likely_ocr_noise(candidate: Dict[str, Any]) -> bool:
    """Return True iff the candidate is likely OCR/regex noise rather than a real money mention.

    Hard guard: candidates already classified by the resolver into a known money role
    (price/base_auction/final_value/market_value/valuation_deduction/cadastral_rendita/
    condominium_arrears/judicial_sale_value/final_valuation_after_deductions/
    unit_price_reference/rent_or_income/formalities_procedural_amount) are never treated as noise.

    A candidate is flagged as noise when at least one of the following holds:
      - The quote is empty.
      - The full quote is a broken table/cadastral fragment (e.g. "T-1", "F1", "A/3", "mappale", isolated "€1").
      - The context is dominated by cadastral identification tokens (Foglio, Part., Sub., Zona Cens., Categoria,
        Classe, Consistenza, Superficie catastale, ...) AND has no strong economic/legal keyword.
      - The amount is < €50 AND the context has no strong economic/legal keyword.
      - The amount is in the suspicious-tiny list (1, 2, 3, 5, 9, 10, 12, 24, 35, 59, 100) AND has no
        explicit tax/fee/cost/legal keyword.
      - The normalized quote is very short (< 12 chars) AND has no strong economic/legal keyword.
    """
    amount = _candidate_amount(candidate)
    quote_raw = _normalize_text(candidate.get("raw_text") or candidate.get("quote") or "")
    norm = _money_norm(quote_raw)
    role = str(candidate.get("role") or "")
    base_role = str(candidate.get("semantic_base_role") or "")
    effective_role = base_role if role in {"component_of_total", "total_candidate", "unknown_money"} else role
    if not effective_role:
        effective_role = role

    # Hard guard — known authoritative money roles always survive.
    if effective_role in {
        "price",
        "base_auction",
        "final_value",
        "market_value",
        "valuation_deduction",
        "cadastral_rendita",
        "condominium_arrears",
        "judicial_sale_value",
        "final_valuation_after_deductions",
        "unit_price_reference",
        "rent_or_income",
        "formalities_procedural_amount",
    }:
        return False

    if not norm:
        return True

    if _BROKEN_FRAGMENT_RE.match(quote_raw):
        return True
    if _ISOLATED_EURO_SHARD_RE.match(quote_raw):
        return True

    amount_raw_text = _normalize_text(candidate.get("amount_raw") or "")
    if amount_raw_text:
        if _BROKEN_FRAGMENT_RE.match(amount_raw_text):
            return True
        if _ISOLATED_EURO_SHARD_RE.match(amount_raw_text):
            return True

    has_strong_kw = bool(_STRONG_MONEY_KEYWORD_RE.search(norm))
    cadastral_hits = len(_CADASTRAL_TABLE_TERMS_RE.findall(norm))
    in_cadastral_header = bool(_CADASTRAL_TABLE_HEADER_RE.search(norm))
    in_unit_price_context = bool(_UNIT_PRICE_CONTEXT_RE.search(quote_raw))

    # Tiny amounts (≤€5) inside cadastral table HEADER rows or heavily
    # cadastral context are practically always OCR artifacts (e.g. "€ T-1"
    # mis-read as "€1"). Demote regardless of column-header keywords like
    # "Rendita" because they describe the table column, not a money amount.
    if amount is not None and amount <= 5 and (in_cadastral_header or cadastral_hits >= 3):
        return True

    if in_cadastral_header and not has_strong_kw:
        return True

    if cadastral_hits >= 3 and not has_strong_kw:
        return True

    # Very small amount (<€50) with no economic/legal keyword in context.
    if amount is not None and amount < 50 and not has_strong_kw:
        return True

    # Suspicious round/tiny amounts that frequently come from broken OCR rows.
    if (
        amount is not None
        and int(round(amount)) in _SUSPICIOUS_TINY_AMOUNTS
        and not has_strong_kw
        and not in_unit_price_context
    ):
        return True

    # Very short context with no money keyword.
    if len(norm) < 12 and not has_strong_kw:
        return True

    return False


_RESOLVER_NON_BUYER_ROLE_TO_GROUP = {
    "cadastral_rendita": "cadastral_values",
    "formalities_procedural_amount": "formalities_and_procedural_amounts",
    "valuation_deduction": "valuation_deductions",
    "base_auction": "price_references",
    "final_value": "price_references",
    "market_value": "price_references",
    "price": "price_references",
    "judicial_sale_value": "price_references",
    "final_valuation_after_deductions": "price_references",
    "unit_price_reference": "valuation_references",
    "regularization_cost": "other_monetary_mentions",
    "rent_or_income": "other_monetary_mentions",
}

# Map resolver semantic role to a customer-facing label_role / sub-role so the
# Money Box can show "Prezzo base d'asta", "Valore di stima", "Valore finale dopo
# decurtazioni", etc. instead of one generic "Riferimento di prezzo".
_RESOLVER_ROLE_TO_LABEL_ROLE = {
    "base_auction": "auction_base_price",
    "judicial_sale_value": "judicial_sale_value",
    "market_value": "market_value",
    "final_value": "valuation_value",
    "final_valuation_after_deductions": "final_valuation_after_deductions",
    "price": "valuation_value",
    "valuation_deduction": "valuation_deduction",
    "cadastral_rendita": "cadastral_rendita",
    "formalities_procedural_amount": "mortgage_formality",
    "buyer_cost_signal_to_verify": "buyer_cost_signal",
    "condominium_arrears": "condominium_arrears",
    "regularization_cost": "regularization_cost",
    "rent_or_income": "rent_or_income",
    "unit_price_reference": "unit_price_reference",
    "unknown_money": "raw_or_low_confidence",
}

_RESOLVER_ROLE_TO_TITLE_IT = {
    "base_auction": "Prezzo base d'asta",
    "judicial_sale_value": "Valore di vendita giudiziaria",
    "market_value": "Valore di stima / mercato",
    "final_value": "Valore finale di stima",
    "final_valuation_after_deductions": "Valore finale (netto delle decurtazioni)",
    "price": "Valore di stima",
    "valuation_deduction": "Decurtazione nella stima",
    "cadastral_rendita": "Rendita catastale",
    "formalities_procedural_amount": "Formalità / ipoteca / pignoramento",
    "buyer_cost_signal_to_verify": "Costo a carico dell'acquirente (da verificare)",
    "condominium_arrears": "Spese condominiali pregresse",
    "regularization_cost": "Costo di regolarizzazione (da verificare)",
    "rent_or_income": "Canone / reddito locativo",
    "unit_price_reference": "Riferimento unitario (€/mq / €/ha)",
    "unknown_money": "Importo monetario",
}


def _classification_template(group: str, buyer_relevance: str, explanation: str, *, additive: bool = False, label_role: str = "", verification_note: Optional[str] = None) -> Dict[str, Any]:
    return {
        "group": group,
        "buyer_relevance": buyer_relevance,
        "additive_to_extra_total": additive,
        "explanation_it": explanation,
        "verification_note_it": verification_note,
        "label_role": label_role or group,
    }


def classify_money_context(candidate: Dict[str, Any]) -> Dict[str, Any]:
    """Map a money candidate into one customer-facing group + relevance metadata.

    Decision order (highest priority first):
      1. Arithmetic context (mq×€/mq, +/- = formulas) ALWAYS routes to valuation_references.
      2. Resolver-assigned non-buyer role (rendita / formalities / valuation_deduction /
         base_auction / final_value / market_value / price) routes to the corresponding
         non-buyer group, regardless of obligation language elsewhere in the quote window.
      3. Quote pattern fallback (cadastral / formality / deduction / price_ref) for amounts
         the resolver left as buyer_cost_signal_to_verify / unknown_money / total_candidate.
      4. Explicit buyer obligation language → buyer_cost_signals_to_verify (or
         buyer_costs_confirmed for safe explicit totals).
      5. Condominium arrears → buyer_cost_signals_to_verify.
      6. Plain regolarizzazione/sanatoria mention without obligation → other_monetary_mentions.

    Regolarizzazione/sanatoria mentions alone never promote to buyer cost.
    """
    quote = candidate.get("raw_text") or candidate.get("quote") or ""
    amount_raw = candidate.get("amount_raw") or quote
    explicit_buyer = is_explicit_buyer_obligation(quote)
    role = str(candidate.get("role") or "")
    base_role = str(candidate.get("semantic_base_role") or "")
    confidence = float(candidate.get("confidence") or 0.0)

    effective_role = base_role if role in {"component_of_total", "total_candidate"} else role
    if not effective_role:
        effective_role = role

    arithmetic = is_valuation_arithmetic_context(quote, amount_raw)
    deduction = is_valuation_deduction_context(quote)
    price_ref = is_price_reference_context(quote)
    cadastral = is_cadastral_context(quote)
    formality = is_formality_procedural_context(quote)
    regolarizzazione = bool(_REGOLARIZZAZIONE_MENTION_RE.search(_money_norm(quote)))
    lavori_ultimati = bool(_LAVORI_ULTIMATI_RE.search(_money_norm(quote)))

    # "Valore a lavori ultimati" / "valore considerato a lavori ultimati" /
    # "valore come sopra determinato" — hypothetical completed-state value:
    # always a valuation reference, never a buyer-side cost or a deduction.
    if lavori_ultimati and effective_role not in {"buyer_cost_signal_to_verify", "condominium_arrears"}:
        classification = _classification_template(
            "valuation_references",
            "none",
            "Valore considerato a lavori ultimati: stima ipotetica del bene a regolarizzazione/lavori completati, non un costo per l'acquirente.",
            label_role="valuation_reference",
        )
        return _finalize_classification(classification, candidate, regolarizzazione, explicit_buyer)

    # Resolver-assigned condominium_arrears (the position-aware resolver
    # already determined this amount sits under a "spese condominiali insolute /
    # debito pregresso condominio" label) MUST surface as a buyer signal, even
    # when the wider quote contains valuation/price keywords from neighbouring
    # table cells.
    if effective_role == "condominium_arrears":
        classification = _classification_template(
            "buyer_cost_signals_to_verify",
            "to_verify",
            "Spese condominiali potenzialmente esigibili dal nuovo proprietario: verificare esposizione esatta.",
            label_role="condominium_arrears",
            verification_note="Richiedere lettera dell'amministratore condominiale e verificare esposizione residua biennale.",
        )
        return _finalize_classification(classification, candidate, regolarizzazione, explicit_buyer)

    if arithmetic and not (
        explicit_buyer and effective_role in {"buyer_cost_signal_to_verify", "condominium_arrears"}
    ):
        classification = _classification_template(
            "valuation_references",
            "none",
            "Calcolo valutativo (es. mq × €/mq oppure somma/differenza nella stima): valore di riferimento, non un costo extra per l'acquirente.",
            label_role="valuation_reference",
        )
        return _finalize_classification(classification, candidate, regolarizzazione, explicit_buyer)

    resolver_group = _RESOLVER_NON_BUYER_ROLE_TO_GROUP.get(effective_role)
    if resolver_group:
        explanations = {
            "cadastral_values": "Valore catastale (rendita / valore fiscale): dato di riferimento, non un costo a carico dell'acquirente.",
            "formalities_and_procedural_amounts": "Importo procedurale (formalità/ipoteca/cancellazione/trascrizione): non automaticamente a carico dell'acquirente.",
            "valuation_deductions": "Decurtazione/deprezzamento applicato dal perito alla stima del valore: non è un costo a carico dell'acquirente.",
            "price_references": "Valore di stima / prezzo base / valore di mercato: riferimento di prezzo, non un costo extra per l'acquirente.",
            "valuation_references": "Calcolo o riferimento valutativo: importo unitario o ipotetico usato dal perito, non un costo per l'acquirente.",
            "other_monetary_mentions": "Importo monetario rilevato in perizia: classificazione conservativa.",
        }
        explanation_text = explanations.get(
            resolver_group, "Importo monetario classificato in modo conservativo."
        )
        classification = _classification_template(
            resolver_group,
            "none",
            explanation_text,
            label_role=resolver_group,
        )
        return _finalize_classification(classification, candidate, regolarizzazione, explicit_buyer)

    if cadastral:
        classification = _classification_template(
            "cadastral_values",
            "none",
            "Valore catastale (rendita / valore fiscale): dato di riferimento, non un costo a carico dell'acquirente.",
            label_role="cadastral",
        )
        return _finalize_classification(classification, candidate, regolarizzazione, explicit_buyer)

    if formality and not explicit_buyer:
        classification = _classification_template(
            "formalities_and_procedural_amounts",
            "none",
            "Importo procedurale (formalità/ipoteca/cancellazione/trascrizione): non automaticamente a carico dell'acquirente.",
            label_role="formality",
        )
        return _finalize_classification(classification, candidate, regolarizzazione, explicit_buyer)

    if deduction:
        classification = _classification_template(
            "valuation_deductions",
            "none",
            "Decurtazione/deprezzamento applicato dal perito alla stima del valore: non è un costo a carico dell'acquirente.",
            label_role="valuation_deduction",
        )
        return _finalize_classification(classification, candidate, regolarizzazione, explicit_buyer)

    if price_ref and not explicit_buyer:
        classification = _classification_template(
            "price_references",
            "none",
            "Valore di stima / prezzo base / valore di mercato: riferimento di prezzo, non un costo extra per l'acquirente.",
            label_role="price_reference",
        )
        return _finalize_classification(classification, candidate, regolarizzazione, explicit_buyer)

    if explicit_buyer:
        if confidence >= 0.75 and role == "total_candidate" and candidate.get("should_sum"):
            classification = _classification_template(
                "buyer_costs_confirmed",
                "explicit",
                "Costo a carico dell'acquirente esplicitamente dichiarato in perizia.",
                additive=True,
                label_role="buyer_cost_confirmed",
                verification_note="Verificare comunque importo e modalità con tecnico/delegato prima dell'offerta.",
            )
        else:
            classification = _classification_template(
                "buyer_cost_signals_to_verify",
                "to_verify",
                "Indicato come obbligo dell'acquirente in perizia: confermare importo e applicabilità prima dell'offerta.",
                label_role="buyer_cost_signal_to_verify",
                verification_note="Verificare importo esatto, applicabilità e tempistiche con tecnico/delegato prima dell'offerta.",
            )
        return _finalize_classification(classification, candidate, regolarizzazione, explicit_buyer)

    if base_role == "condominium_arrears" or role == "condominium_arrears":
        classification = _classification_template(
            "buyer_cost_signals_to_verify",
            "to_verify",
            "Spese condominiali potenzialmente esigibili dal nuovo proprietario: verificare esposizione esatta.",
            label_role="buyer_cost_signal_to_verify",
            verification_note="Richiedere lettera dell'amministratore condominiale e verificare esposizione residua biennale.",
        )
        return _finalize_classification(classification, candidate, regolarizzazione, explicit_buyer)

    if regolarizzazione:
        classification = _classification_template(
            "other_monetary_mentions",
            "none",
            "Menzione di regolarizzazione/sanatoria senza esplicita responsabilità dell'acquirente: importo riportato ma non promosso a costo certo.",
            label_role="monetary_mention",
            verification_note="Verificare con tecnico/delegato se l'importo è effettivamente a carico dell'acquirente.",
        )
        return _finalize_classification(classification, candidate, regolarizzazione, explicit_buyer)

    classification = _classification_template(
        "other_monetary_mentions",
        "none",
        "Importo monetario rilevato in perizia: classificazione conservativa.",
        label_role="monetary_mention",
    )
    return _finalize_classification(classification, candidate, regolarizzazione, explicit_buyer)


def _finalize_classification(
    classification: Dict[str, Any],
    candidate: Dict[str, Any],
    regolarizzazione: bool,
    explicit_buyer: bool,
) -> Dict[str, Any]:
    if not _has_page_evidence(candidate):
        classification = dict(classification)
        classification["group"] = "unsupported_or_unknown_amounts"
        classification["buyer_relevance"] = "none"
        classification["additive_to_extra_total"] = False
        classification["explanation_it"] = "Importo rilevato ma pagine non determinate automaticamente: non promosso a costo certo."
        classification["verification_note_it"] = "Verificare la pagina e il contesto direttamente nella perizia originale."
        classification["label_role"] = "unsupported_amount"
    # OCR/regex noise demotion — never override an explicit buyer obligation, but
    # demote any cadastral/table/broken-token/very-small fragment to the low-confidence
    # bucket so it stops appearing as a primary valuation reference.
    current_group = str(classification.get("group") or "")
    if current_group not in {"buyer_costs_confirmed", "buyer_cost_signals_to_verify"} and is_likely_ocr_noise(candidate):
        classification = dict(classification)
        classification["group"] = "unsupported_or_unknown_amounts"
        classification["buyer_relevance"] = "none"
        classification["additive_to_extra_total"] = False
        classification["explanation_it"] = (
            "Frammento monetario probabilmente derivato da OCR/tabella catastale: "
            "non promosso come riferimento monetario."
        )
        classification["verification_note_it"] = (
            "Verificare manualmente nella perizia originale se l'importo è effettivamente rilevante."
        )
        classification["label_role"] = "ocr_noise"
        classification["is_likely_ocr_noise"] = True
    else:
        classification["is_likely_ocr_noise"] = bool(classification.get("is_likely_ocr_noise"))
    return classification


_GROUP_LABEL_PREFIX = {
    "buyer_costs_confirmed": "Costo a carico dell'acquirente",
    "buyer_cost_signals_to_verify": "Costo da verificare",
    "valuation_references": "Riferimento valutativo (non a carico dell'acquirente)",
    "price_references": "Riferimento di prezzo (non a carico dell'acquirente)",
    "valuation_deductions": "Decurtazione nella stima (non a carico dell'acquirente)",
    "cadastral_values": "Valore catastale (non a carico dell'acquirente)",
    "formalities_and_procedural_amounts": "Importo procedurale (non a carico dell'acquirente)",
    "other_monetary_mentions": "Importo monetario citato in perizia",
    "unsupported_or_unknown_amounts": "Importo senza evidenza paginata",
}

_CODE_PREFIX = {
    "buyer_costs_confirmed": "AUTH_BUYER_COST",
    "buyer_cost_signals_to_verify": "AUTH_BUYER_VERIFY",
    "valuation_references": "AUTH_VAL_REF",
    "price_references": "AUTH_PRICE_REF",
    "valuation_deductions": "AUTH_VAL_DED",
    "cadastral_values": "AUTH_CADASTRAL",
    "formalities_and_procedural_amounts": "AUTH_FORMALITY",
    "other_monetary_mentions": "AUTH_MONEY_MENTION",
    "unsupported_or_unknown_amounts": "AUTH_UNSUPPORTED",
}

_CUSTOMER_TITLE_PREFIX = {
    "buyer_costs_confirmed": "Costo a carico dell'acquirente",
    "buyer_cost_signals_to_verify": "Costo da verificare",
    "valuation_references": "Riferimento valutativo",
    "price_references": "Riferimento di prezzo",
    "valuation_deductions": "Decurtazione nella stima",
    "cadastral_values": "Valore catastale",
    "formalities_and_procedural_amounts": "Importo procedurale / formalità",
    "other_monetary_mentions": "Importo monetario citato in perizia",
    "unsupported_or_unknown_amounts": "Importo senza pagina certa",
}

_CUSTOMER_BADGE_LABEL = {
    "buyer_costs_confirmed": "Costo acquirente",
    "buyer_cost_signals_to_verify": "Da verificare",
    "valuation_references": "Stima",
    "price_references": "Prezzo",
    "valuation_deductions": "Decurtazione",
    "cadastral_values": "Catastale",
    "formalities_and_procedural_amounts": "Procedurale",
    "other_monetary_mentions": "Altro importo",
    "unsupported_or_unknown_amounts": "Da verificare",
}

_CUSTOMER_BADGE_TONE = {
    "buyer_costs_confirmed": "buyer_confirmed",
    "buyer_cost_signals_to_verify": "buyer_verify",
    "valuation_references": "info_neutral",
    "price_references": "info_neutral",
    "valuation_deductions": "info_neutral",
    "cadastral_values": "info_neutral",
    "formalities_and_procedural_amounts": "info_neutral",
    "other_monetary_mentions": "info_neutral",
    "unsupported_or_unknown_amounts": "low_confidence",
}

# Display importance / default-expand mapping used by the customer Money Map UI
# to render collapsible/dropdown groups instead of a giant wall of cards.
_GROUP_DISPLAY_IMPORTANCE = {
    "buyer_costs_confirmed": ("primary", True),
    "buyer_cost_signals_to_verify": ("primary", True),
    "valuation_references": ("primary", True),
    "price_references": ("primary", True),
    "valuation_deductions": ("primary", True),
    "cadastral_values": ("secondary", False),
    "formalities_and_procedural_amounts": ("secondary", False),
    "other_monetary_mentions": ("secondary", False),
    "unsupported_or_unknown_amounts": ("raw", False),
}


def make_customer_money_item(
    candidate: Dict[str, Any],
    classification: Dict[str, Any],
    index: int,
) -> Dict[str, Any]:
    """Build a customer-safe money item carrying actionable DA VERIFICARE metadata."""
    group = str(classification.get("group") or "other_monetary_mentions")
    amount = _candidate_amount(candidate)
    amount_text = _amount_label(amount)
    # Resolver-driven label_role overrides the group default when available,
    # so the customer sees "Prezzo base d'asta" rather than a generic
    # "Riferimento di prezzo".
    resolver_role = str(candidate.get("semantic_base_role") or candidate.get("role") or "")
    role_label_role = _RESOLVER_ROLE_TO_LABEL_ROLE.get(resolver_role)
    role_title = _RESOLVER_ROLE_TO_TITLE_IT.get(resolver_role)
    base_label = role_title or _GROUP_LABEL_PREFIX.get(group, "Importo monetario")
    label = f"{base_label}: {amount_text}" if amount_text else base_label
    evidence = _evidence_from_candidate(candidate)
    contract_state = (
        "quantified_estimate"
        if group == "buyer_costs_confirmed"
        else "cost_signal_to_verify"
        if group == "buyer_cost_signals_to_verify"
        else "info_only"
    )
    visible_status = (
        "explicit_buyer_obligation"
        if group == "buyer_costs_confirmed"
        else "to_verify"
        if group == "buyer_cost_signals_to_verify"
        else "info_only"
    )
    code_prefix = _CODE_PREFIX.get(group, "AUTH_MONEY")
    explanation = str(classification.get("explanation_it") or "")
    verification_note = classification.get("verification_note_it")
    note_pieces: List[str] = []
    if explanation:
        note_pieces.append(explanation)
    if verification_note:
        note_pieces.append(str(verification_note))
    note_text = " ".join(note_pieces).strip() or "Importo monetario classificato in modo conservativo."
    page_value: Optional[int] = None
    if evidence and isinstance(evidence[0], dict):
        page_candidate = evidence[0].get("page")
        if isinstance(page_candidate, int):
            page_value = page_candidate
    customer_title_base = role_title or _CUSTOMER_TITLE_PREFIX.get(group, "Importo monetario")
    customer_title = f"{customer_title_base}: {amount_text}" if amount_text else customer_title_base
    customer_badge = _CUSTOMER_BADGE_LABEL.get(group, "Da verificare")
    badge_tone = _CUSTOMER_BADGE_TONE.get(group, "info_neutral")
    customer_context = ""
    if evidence and isinstance(evidence[0], dict):
        quote_text = _normalize_text(evidence[0].get("quote"))
        if quote_text:
            customer_context = quote_text
    is_noise = bool(classification.get("is_likely_ocr_noise"))
    importance, default_expand = _GROUP_DISPLAY_IMPORTANCE.get(group, ("raw", False))
    if is_noise:
        importance, default_expand = "raw", False
    lot_label = candidate.get("lot_label") if isinstance(candidate.get("lot_label"), str) else None
    payload: Dict[str, Any] = {
        "code": f"{code_prefix}_{index:02d}",
        "label_it": label,
        "label_en": label,
        "customer_title_it": customer_title,
        "customer_badge_it": customer_badge,
        "customer_badge_tone": badge_tone,
        "customer_amount_label": amount_text,
        "customer_context_it": customer_context,
        "type": "ESTIMATE" if group == "buyer_costs_confirmed" else "SIGNAL_TO_VERIFY" if group == "buyer_cost_signals_to_verify" else "INFO",
        "group": group,
        "display_group": group,
        "importance": importance,
        "should_expand_by_default": bool(default_expand),
        "is_likely_ocr_noise": bool(is_noise),
        "role": resolver_role or "unknown_money",
        "label_role": role_label_role or classification.get("label_role") or group,
        "lot_label": lot_label,
        "lot_id": lot_label,
        "buyer_relevance": classification.get("buyer_relevance") or "none",
        "additive_to_extra_total": bool(classification.get("additive_to_extra_total")),
        "additive_to_buyer_total": bool(classification.get("additive_to_extra_total")),
        "amount_eur": int(round(amount)) if amount is not None else None,
        "stima_euro": int(round(amount)) if amount is not None else None,
        "raw_value": candidate.get("amount_raw") or _normalize_text(candidate.get("raw_text"))[:120],
        "page": page_value,
        "stima_nota": note_text,
        "note_it": note_text,
        "explanation_it": explanation,
        "verification_note_it": str(verification_note) if verification_note else None,
        "contract_state": contract_state,
        "customer_visible_amount_status": visible_status,
        "evidence": evidence,
        "fonte_perizia": {"value": "Perizia", "evidence": evidence},
    }
    return payload


def _customer_total_status(money_box: Dict[str, Any]) -> Dict[str, Any]:
    total_extra_cost = money_box.get("total_extra_cost_eur")
    total_block = money_box.get("total_extra_costs") if isinstance(money_box.get("total_extra_costs"), dict) else {}
    if isinstance(total_extra_cost, (int, float)) and total_extra_cost > 0:
        amount_text = _amount_label(float(total_extra_cost))
        return {
            "status_code": "explicit_total",
            "label_it": amount_text or "Totale buyer-side esplicito",
            "explanation_it": str(total_block.get("note") or "Totale buyer-side esplicitamente supportato in perizia."),
        }
    return {
        "status_code": "no_defensible_total",
        "label_it": "Non quantificato in modo difendibile",
        "explanation_it": str(
            total_block.get("note")
            or "Nessun totale buyer-side certo: usare le voci sotto come checklist di verifica."
        ),
    }


def _customer_summary_pages(money_box: Dict[str, Any], group_keys: Sequence[str], limit: int = 4) -> List[int]:
    pages: List[int] = []
    seen = set()
    for key in group_keys:
        for item in money_box.get(key, []) or []:
            if not isinstance(item, dict):
                continue
            page = item.get("page")
            if not isinstance(page, int) or page <= 0:
                continue
            if page in seen:
                continue
            seen.add(page)
            pages.append(page)
            if len(pages) >= limit:
                return pages
    return pages


def _top_amount_phrase(money_box: Dict[str, Any], key: str, limit: int = 2) -> str:
    items = money_box.get(key) or []
    if not isinstance(items, list):
        return ""
    candidates: List[Tuple[int, Optional[int]]] = []
    seen_amounts = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        amount = item.get("amount_eur") or item.get("stima_euro")
        try:
            amount_int = int(round(float(amount)))
        except Exception:
            continue
        if amount_int <= 0 or amount_int in seen_amounts:
            continue
        seen_amounts.add(amount_int)
        page = item.get("page") if isinstance(item.get("page"), int) else None
        candidates.append((amount_int, page))
    if not candidates:
        return ""
    candidates.sort(key=lambda pair: pair[0], reverse=True)
    formatted = []
    for amount_int, page in candidates[:limit]:
        amount_text = f"€{amount_int:,.0f}".replace(",", ".")
        if page:
            formatted.append(f"{amount_text} (p.{page})")
        else:
            formatted.append(amount_text)
    return ", ".join(formatted)


def _build_customer_summary(money_box: Dict[str, Any]) -> Dict[str, Any]:
    """Deterministic, case-specific Money Map summary for the customer UI.

    The frontend renders this verbatim; if Gemini is wired upstream the
    `line_it` field can be replaced while the counts/pages/total remain.
    """
    counts = {
        "buyer_costs_confirmed": len(money_box.get("buyer_costs_confirmed") or []),
        "buyer_cost_signals_to_verify": len(money_box.get("buyer_cost_signals_to_verify") or []),
        "valuation_references": len(money_box.get("valuation_references") or []),
        "price_references": len(money_box.get("price_references") or []),
        "valuation_deductions": len(money_box.get("valuation_deductions") or []),
        "cadastral_values": len(money_box.get("cadastral_values") or []),
        "formalities_and_procedural_amounts": len(money_box.get("formalities_and_procedural_amounts") or []),
        "other_monetary_mentions": len(money_box.get("other_monetary_mentions") or []),
        "unsupported_or_unknown_amounts": len(money_box.get("unsupported_or_unknown_amounts") or []),
    }
    total_status = _customer_total_status(money_box)
    pages = _customer_summary_pages(
        money_box,
        (
            "buyer_costs_confirmed",
            "buyer_cost_signals_to_verify",
            "valuation_references",
            "valuation_deductions",
            "price_references",
            "formalities_and_procedural_amounts",
            "cadastral_values",
            "other_monetary_mentions",
        ),
        limit=4,
    )

    parts: List[str] = []
    if total_status["status_code"] == "explicit_total":
        parts.append(
            f"Totale buyer-side dichiarato in perizia: {total_status['label_it']}."
        )
    else:
        parts.append("Nessun totale buyer-side difendibile è stato ricavato dalla perizia.")

    if counts["buyer_costs_confirmed"] > 0:
        top_buyer = _top_amount_phrase(money_box, "buyer_costs_confirmed", limit=2)
        if top_buyer:
            parts.append(
                f"{counts['buyer_costs_confirmed']} costi a carico dell'acquirente confermati (es. {top_buyer})."
            )
        else:
            parts.append(f"{counts['buyer_costs_confirmed']} costi a carico dell'acquirente confermati.")
    if counts["buyer_cost_signals_to_verify"] > 0:
        top_signal = _top_amount_phrase(money_box, "buyer_cost_signals_to_verify", limit=2)
        if top_signal:
            parts.append(
                f"{counts['buyer_cost_signals_to_verify']} segnali da verificare prima dell'offerta (es. {top_signal})."
            )
        else:
            parts.append(
                f"{counts['buyer_cost_signals_to_verify']} segnali da verificare prima dell'offerta."
            )

    classified_total = (
        counts["valuation_references"]
        + counts["price_references"]
        + counts["valuation_deductions"]
        + counts["cadastral_values"]
        + counts["formalities_and_procedural_amounts"]
    )
    if classified_total > 0:
        breakdown = []
        if counts["valuation_references"]:
            top = _top_amount_phrase(money_box, "valuation_references", limit=1)
            breakdown.append(
                f"{counts['valuation_references']} riferimenti di stima"
                + (f" (es. {top})" if top else "")
            )
        if counts["price_references"]:
            top = _top_amount_phrase(money_box, "price_references", limit=1)
            breakdown.append(
                f"{counts['price_references']} riferimenti di prezzo"
                + (f" (es. {top})" if top else "")
            )
        if counts["valuation_deductions"]:
            top = _top_amount_phrase(money_box, "valuation_deductions", limit=1)
            breakdown.append(
                f"{counts['valuation_deductions']} decurtazioni nella stima"
                + (f" (es. {top})" if top else "")
            )
        if counts["cadastral_values"]:
            top = _top_amount_phrase(money_box, "cadastral_values", limit=1)
            breakdown.append(
                f"{counts['cadastral_values']} valori catastali"
                + (f" (es. {top})" if top else "")
            )
        if counts["formalities_and_procedural_amounts"]:
            top = _top_amount_phrase(money_box, "formalities_and_procedural_amounts", limit=1)
            breakdown.append(
                f"{counts['formalities_and_procedural_amounts']} importi procedurali"
                + (f" (es. {top})" if top else "")
            )
        parts.append(
            "Importi classificati come non a carico dell'acquirente: "
            + ", ".join(breakdown)
            + "."
        )
    if counts["other_monetary_mentions"]:
        parts.append(
            f"{counts['other_monetary_mentions']} altri importi monetari rilevati senza obbligo esplicito."
        )
    if counts["unsupported_or_unknown_amounts"]:
        parts.append(
            f"{counts['unsupported_or_unknown_amounts']} importi senza pagina certa: verificare manualmente nella perizia."
        )
    if pages:
        parts.append("Pagine principali da controllare: " + ", ".join(f"p.{p}" for p in pages) + ".")

    line_it = " ".join(parts).strip()
    if not line_it:
        line_it = "Nessun importo monetario classificato in questa perizia."

    focus_pieces: List[str] = []
    if counts["buyer_cost_signals_to_verify"]:
        focus_pieces.append("Concentrarsi sui segnali da verificare prima dell'offerta.")
    if counts["valuation_references"] or counts["valuation_deductions"]:
        focus_pieces.append("Stime e decurtazioni sono riferimenti del perito, non costi extra.")
    if counts["formalities_and_procedural_amounts"]:
        focus_pieces.append("Le formalità procedurali non sono automaticamente a carico dell'acquirente.")
    focus_it = " ".join(focus_pieces).strip()

    why_pieces: List[str] = []
    if counts["valuation_references"] or counts["price_references"]:
        why_pieces.append(
            "I valori di stima/prezzo sono calcoli del perito (es. mq × €/mq) e non rappresentano cassa extra a carico dell'acquirente."
        )
    if counts["valuation_deductions"]:
        why_pieces.append(
            "Le decurtazioni applicate alla stima riducono il valore del bene ma non sono costi che l'acquirente paga in più."
        )
    if counts["cadastral_values"]:
        why_pieces.append(
            "I valori catastali sono dati fiscali, non costi a carico dell'acquirente."
        )
    if counts["formalities_and_procedural_amounts"]:
        why_pieces.append(
            "Le formalità (ipoteche, cancellazioni, trascrizioni) sono importi procedurali; verificare nel dispositivo di vendita chi è tenuto a sostenerli."
        )
    why_not_buyer_it = " ".join(why_pieces).strip()

    return {
        "version": "money_map_summary_v1",
        "line_it": line_it,
        "focus_it": focus_it,
        "why_not_buyer_it": why_not_buyer_it,
        "counts": counts,
        "total_status": total_status,
        "primary_pages": pages,
    }


def _section3_from_money_box(money_box: Dict[str, Any]) -> Dict[str, Any]:
    section3 = copy.deepcopy(money_box)
    total = money_box.get("total_extra_costs") if isinstance(money_box.get("total_extra_costs"), dict) else {}
    if isinstance(total.get("range"), dict):
        min_value = total["range"].get("min")
        max_value = total["range"].get("max")
    else:
        min_value = total.get("min")
        max_value = total.get("max")
    section3["totale_extra_budget"] = {
        "min": min_value,
        "max": max_value,
        "nota": total.get("note") or total.get("nota"),
        "contract_state": total.get("contract_state"),
        "evidence": copy.deepcopy(total.get("evidence", [])),
    }
    return section3


_GROUP_LIMITS = {
    "buyer_costs_confirmed": 12,
    "buyer_cost_signals_to_verify": 16,
    "valuation_references": 24,
    "price_references": 16,
    "valuation_deductions": 16,
    "cadastral_values": 12,
    "formalities_and_procedural_amounts": 16,
    "other_monetary_mentions": 24,
    "unsupported_or_unknown_amounts": 16,
}

_NON_BUYER_GROUPS = (
    "valuation_references",
    "price_references",
    "valuation_deductions",
    "cadastral_values",
    "formalities_and_procedural_amounts",
)


_ESTRATTO_AMOUNT_RE = re.compile(
    r"(?:€|\beuro\b)\s*\d{1,3}(?:\.\d{3})*(?:,\d{1,2})?|"
    r"\d{1,3}(?:\.\d{3})*(?:,\d{1,2})?\s*(?:€|\beuro\b)|"
    r"\b\d{1,3}(?:\.\d{3})+(?:,\d{1,2})?\b",
    flags=re.IGNORECASE | re.UNICODE,
)


def _parse_amount_eur(text: str) -> Optional[float]:
    """Parse a single Italian-formatted money fragment into a float, or None."""
    if not text:
        return None
    match = re.search(r"\d{1,3}(?:\.\d{3})*(?:,\d{1,2})?|\d+(?:,\d{1,2})?", text)
    if not match:
        return None
    normalized = match.group(0).replace(".", "").replace(",", ".")
    try:
        value = float(normalized)
    except Exception:
        return None
    return value if value > 0 else None


def _iter_estratto_quality_items(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Yield every item dict found under result["estratto_quality"], either
    directly via "items" or nested under "sections[].items"."""
    out: List[Dict[str, Any]] = []
    estratto = result.get("estratto_quality") if isinstance(result.get("estratto_quality"), dict) else {}
    sections = estratto.get("sections") if isinstance(estratto.get("sections"), list) else []
    for section in sections:
        if not isinstance(section, dict):
            continue
        for item in section.get("items") or []:
            if isinstance(item, dict):
                out.append(item)
    for item in estratto.get("items") or []:
        if isinstance(item, dict):
            out.append(item)
    return out


def _estratto_evidence_page(item: Dict[str, Any]) -> Tuple[Optional[int], str]:
    evidence = item.get("evidence") if isinstance(item.get("evidence"), list) else []
    for ev in evidence:
        if not isinstance(ev, dict):
            continue
        try:
            page = int(ev.get("page"))
            quote = _normalize_text(ev.get("quote"))
            if page > 0:
                return page, quote
        except Exception:
            continue
    return None, ""


def _candidate_from_estratto_amount(
    item: Dict[str, Any],
    amount_text: str,
    amount_eur: float,
    base_text: str,
    page: int,
    quote: str,
) -> Dict[str, Any]:
    role, reason = _resolve_estratto_role(item, base_text, amount_text)
    seed = f"estratto|{page}|{round(amount_eur, 2)}|{amount_text}".encode("utf-8")
    import hashlib

    candidate_id = "money_eq_" + hashlib.sha1(seed).hexdigest()[:12]
    candidate = {
        "candidate_id": candidate_id,
        "amount_eur": amount_eur,
        "raw_text": quote or base_text,
        "amount_raw": amount_text,
        "page": int(page),
        "role": role,
        "confidence": 0.78,
        "authority_zone": "FINAL_VALUATION",
        "authority_level": "HIGH",
        "reason_code": reason,
        "is_customer_safe_cost": False,
        "should_surface_in_money_box": role in {"buyer_cost_signal_to_verify", "condominium_arrears"},
        "should_sum": False,
        "parent_total_candidate_id": None,
        "warnings": [],
        "source": "estratto_quality",
        "semantic_base_role": role,
        "semantic_base_reason_code": reason,
        "lot_label": item.get("lot_label") if isinstance(item.get("lot_label"), str) else None,
    }
    return candidate


_ESTRATTO_LABEL_ROLE_RULES = (
    (re.compile(r"\b(?:valore\s+arrotondat\w*\s+final\w*\s+(?:per\s+)?vendita\s+giudiziaria|"
                r"valore\s+(?:di\s+|esatto\s+)?vendita\s+giudiziaria|vendita\s+giudiziaria)\b", re.IGNORECASE),
     "judicial_sale_value", "ESTRATTO_VENDITA_GIUDIZIARIA"),
    (re.compile(r"\b(?:lotto\s+unico|valore\s+(?:complessivo\s+)?(?:di\s+stima|finale|"
                r"arrotondat\w*)\s*(?:dell['\s]?immobile|del\s+compendio|del\s+lotto)?)\b", re.IGNORECASE),
     "market_value", "ESTRATTO_VALORE_STIMA"),
    (re.compile(r"\b(?:prezzo\s+base|base\s+d['\s]?\s*asta|offerta\s+minima)\b", re.IGNORECASE),
     "base_auction", "ESTRATTO_PREZZO_BASE"),
    (re.compile(r"\b(?:valore\s+di\s+(?:mercato|venale|stima|complessivo)|piu\s+probabile\s+valore|market\s+value)\b", re.IGNORECASE),
     "market_value", "ESTRATTO_VALORE_MERCATO"),
    (re.compile(r"\b(?:rendita\s+catastal\w*|valore\s+catastal\w*)\b", re.IGNORECASE),
     "cadastral_rendita", "ESTRATTO_RENDITA"),
    (re.compile(r"\b(?:cancellazion\w*|formal(?:it|i)\w*|ipotec\w*|pignorament\w*|"
                r"iscrizione\s+ipotecari|trascrizion\w*)\b", re.IGNORECASE),
     "formalities_procedural_amount", "ESTRATTO_FORMALITA"),
    (re.compile(r"\b(?:spese\s+condominiali\s+(?:insolut|arretrat|scadut|morosit|pregress)|"
                r"debito\s+pregress\w*\s+condomini|condominial\w*\s+(?:insolut|arretrat|scadut|morosit|pregress))\w*",
                re.IGNORECASE),
     "condominium_arrears", "ESTRATTO_CONDOMINIUM_ARREARS"),
    (re.compile(r"\b(?:decurtazion\w*|deprezzament\w*|abbattiment\w*|detrazion\w*|"
                r"oneri\s+di\s+regolarizzazion\w*|rischio\s+(?:assunto\s+)?per\s+(?:la\s+)?mancata\s+garanzi\w*)\b",
                re.IGNORECASE),
     "valuation_deduction", "ESTRATTO_DECURTAZIONE"),
    (re.compile(r"\b(?:sanatori\w*|oblazion\w*|sanzion\w*|completamento\s+lavori|"
                r"abitabilit[aà]|spese\s+tecnich\w*|fiscalizzazion\w*|ripristin\w*|demolizion\w*|"
                r"regolarizzazion\w*)\b", re.IGNORECASE),
     "buyer_cost_signal_to_verify", "ESTRATTO_BUYER_SIGNAL"),
)


def _resolve_estratto_role(item: Dict[str, Any], context_text: str, amount_text: str) -> Tuple[str, str]:
    label_parts = [
        item.get("label_it"),
        item.get("label_en"),
        item.get("detail_it"),
        item.get("note_it"),
        item.get("title_it"),
        context_text,
    ]
    joined = " ".join(str(part or "") for part in label_parts if part)
    for pattern, role, reason in _ESTRATTO_LABEL_ROLE_RULES:
        if pattern.search(joined):
            return role, reason
    return "unknown_money", "ESTRATTO_GENERIC"


def _augment_with_estratto_quality_candidates(
    money_value: Dict[str, Any],
    result: Dict[str, Any],
) -> int:
    """Promote money amounts found inside result["estratto_quality"] items into
    the resolver's money_candidates list. Returns the number of new entries
    added. Duplicates (same page + same amount) are skipped.
    """
    items = _iter_estratto_quality_items(result)
    if not items:
        return 0
    candidates = money_value.get("money_candidates")
    if not isinstance(candidates, list):
        candidates = []
        money_value["money_candidates"] = candidates
    seen = set()
    for existing in candidates:
        if not isinstance(existing, dict):
            continue
        try:
            seen.add((int(existing.get("page") or 0), round(float(existing.get("amount_eur") or 0.0), 2)))
        except Exception:
            continue
    added = 0
    for item in items:
        page, quote = _estratto_evidence_page(item)
        if not page:
            continue
        text_parts = [
            item.get("label_it"),
            item.get("label_en"),
            item.get("detail_it"),
            item.get("note_it"),
            item.get("title_it"),
            item.get("value"),
            quote,
        ]
        joined = " ".join(str(part or "") for part in text_parts if part)
        # Look for an explicit amount_eur first; otherwise scan the joined text.
        direct_amount = item.get("amount_eur") or item.get("stima_euro") or item.get("value_eur")
        try:
            direct_value = float(direct_amount) if direct_amount is not None else None
        except Exception:
            direct_value = None
        if direct_value and direct_value > 0:
            key = (int(page), round(direct_value, 2))
            if key not in seen:
                seen.add(key)
                amount_label = _amount_label(direct_value)
                candidates.append(
                    _candidate_from_estratto_amount(item, amount_label, direct_value, joined, page, quote)
                )
                added += 1
        for match in _ESTRATTO_AMOUNT_RE.finditer(joined):
            amount_eur = _parse_amount_eur(match.group(0))
            if amount_eur is None or amount_eur < 50:
                continue
            key = (int(page), round(amount_eur, 2))
            if key in seen:
                continue
            seen.add(key)
            candidates.append(
                _candidate_from_estratto_amount(item, match.group(0), amount_eur, joined, page, quote)
            )
            added += 1
    if added:
        money_value.setdefault("summary", {})
        money_value["summary"]["candidate_count"] = (
            int(money_value["summary"].get("candidate_count") or 0) + added
        )
    return added


def _augment_with_evidence_quote_amounts(money_value: Dict[str, Any]) -> int:
    """For every existing money candidate, scan its evidence quote for OTHER
    money amounts that did not surface as standalone candidates and add them
    with a role inferred from the surrounding phrase. This is the
    "evidence-quote amount extraction" rule from the brief: e.g. when a quote
    contains 'netto delle decurtazioni € 191.273,57', surface 191274 even if
    only 5932 was the original candidate.
    """
    candidates = money_value.get("money_candidates")
    if not isinstance(candidates, list):
        return 0
    seen = set()
    for existing in candidates:
        if not isinstance(existing, dict):
            continue
        try:
            seen.add((int(existing.get("page") or 0), round(float(existing.get("amount_eur") or 0.0), 2)))
        except Exception:
            continue
    added = 0
    promote_patterns = (
        (re.compile(r"\bnetto\s+delle\s+decurtazion\w*\b[^.\n]{0,40}(?:€|\beuro\b)?\s*(\d{1,3}(?:\.\d{3})*(?:,\d{1,2})?)",
                    re.IGNORECASE), "final_valuation_after_deductions", "EVIDENCE_NETTO_DECURTAZIONI"),
        (re.compile(r"\bvalore\s+final\w*\b[^.\n]{0,40}(?:€|\beuro\b)?\s*(\d{1,3}(?:\.\d{3})*(?:,\d{1,2})?)",
                    re.IGNORECASE), "final_value", "EVIDENCE_VALORE_FINALE"),
        (re.compile(r"\bvendita\s+giudiziari\w*\b[^.\n]{0,60}(?:€|\beuro\b)?\s*(\d{1,3}(?:\.\d{3})*(?:,\d{1,2})?)",
                    re.IGNORECASE), "judicial_sale_value", "EVIDENCE_VENDITA_GIUDIZIARIA"),
        (re.compile(r"\bvalore\s+di\s+mercato\b[^.\n]{0,40}(?:€|\beuro\b)?\s*(\d{1,3}(?:\.\d{3})*(?:,\d{1,2})?)",
                    re.IGNORECASE), "market_value", "EVIDENCE_VALORE_DI_MERCATO"),
        (re.compile(r"\b(?:debito\s+pregress\w*\s+condomini|spese\s+condominial\w*\s+(?:insolut|arretrat|scadut|morosit|pregress))\w*"
                    r"\b[^.\n]{0,60}?(\d{1,3}(?:\.\d{3})*(?:,\d{1,2})?)\s*€?",
                    re.IGNORECASE), "condominium_arrears", "EVIDENCE_CONDOMINIUM_ARREARS"),
    )
    base = list(candidates)
    for existing in base:
        if not isinstance(existing, dict):
            continue
        page = existing.get("page")
        quote = _normalize_text(existing.get("raw_text"))
        if not page or not quote:
            continue
        for pattern, role, reason in promote_patterns:
            for match in pattern.finditer(quote):
                amount_eur = _parse_amount_eur(match.group(1))
                if amount_eur is None or amount_eur < 100:
                    continue
                key = (int(page), round(amount_eur, 2))
                if key in seen:
                    continue
                seen.add(key)
                import hashlib
                seed = f"evquote|{page}|{round(amount_eur, 2)}|{reason}".encode("utf-8")
                cand = {
                    "candidate_id": "money_evq_" + hashlib.sha1(seed).hexdigest()[:12],
                    "amount_eur": amount_eur,
                    "raw_text": quote,
                    "amount_raw": match.group(1),
                    "page": int(page),
                    "role": role,
                    "confidence": float(existing.get("confidence") or 0.7),
                    "authority_zone": existing.get("authority_zone") or "FINAL_VALUATION",
                    "authority_level": existing.get("authority_level") or "HIGH",
                    "reason_code": reason,
                    "is_customer_safe_cost": False,
                    "should_surface_in_money_box": role in {"buyer_cost_signal_to_verify", "condominium_arrears"},
                    "should_sum": False,
                    "parent_total_candidate_id": None,
                    "warnings": [],
                    "source": "evidence_quote",
                    "semantic_base_role": role,
                    "semantic_base_reason_code": reason,
                    "lot_label": existing.get("lot_label"),
                }
                candidates.append(cand)
                added += 1
    return added


def _build_projected_money_box(money_value: Dict[str, Any], legacy_result: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    raw_candidates = money_value.get("money_candidates") if isinstance(money_value.get("money_candidates"), list) else []
    candidates = [candidate for candidate in raw_candidates if isinstance(candidate, dict)]

    eligible = [
        candidate
        for candidate in candidates
        if _candidate_amount(candidate) is not None
    ]
    eligible = _dedupe_candidates(eligible, limit=400)

    # Process larger amounts and authoritative roles first so the per-group
    # limits do not silently drop headline valuations (e.g. €503.930 LOTTO
    # UNICO) in favor of OCR-fragment noise that happens to come earlier in
    # the page ordering.
    _ROLE_PRIORITY = {
        "judicial_sale_value": 0,
        "final_value": 1,
        "market_value": 1,
        "final_valuation_after_deductions": 1,
        "base_auction": 2,
        "price": 3,
        "valuation_deduction": 4,
        "cadastral_rendita": 5,
        "formalities_procedural_amount": 6,
        "condominium_arrears": 7,
        "buyer_cost_signal_to_verify": 8,
        "total_candidate": 9,
        "component_of_total": 10,
        "unknown_money": 11,
    }

    def _priority(candidate: Dict[str, Any]) -> Tuple[int, float]:
        role_key = str(candidate.get("role") or "")
        if role_key in {"component_of_total", "total_candidate", "unknown_money"}:
            role_key = str(candidate.get("semantic_base_role") or candidate.get("role") or "")
        rank = _ROLE_PRIORITY.get(role_key, 12)
        try:
            amount = float(candidate.get("amount_eur") or 0.0)
        except Exception:
            amount = 0.0
        return (rank, -amount)

    eligible = sorted(eligible, key=_priority)

    grouped: Dict[str, List[Dict[str, Any]]] = {key: [] for key in _GROUP_LIMITS}
    counters: Dict[str, int] = {key: 0 for key in _GROUP_LIMITS}
    classified_total_candidate: Optional[Dict[str, Any]] = None

    for candidate in eligible:
        is_component = candidate.get("role") == "component_of_total"
        classification = classify_money_context(candidate)
        group = str(classification.get("group") or "other_monetary_mentions")
        if is_component and group in {"buyer_costs_confirmed", "buyer_cost_signals_to_verify"}:
            group = "other_monetary_mentions"
            classification = dict(classification)
            classification["group"] = group
            classification["buyer_relevance"] = "none"
            classification["additive_to_extra_total"] = False
        if group not in grouped:
            group = "other_monetary_mentions"
            classification["group"] = group
        if counters[group] >= _GROUP_LIMITS[group]:
            continue
        counters[group] += 1
        item = make_customer_money_item(candidate, classification, counters[group])
        if is_component:
            item["is_component_of_total"] = True
            item["additive_to_extra_total"] = False
            item["parent_total_candidate_id"] = candidate.get("parent_total_candidate_id")
        grouped[group].append(item)
        if (
            group == "buyer_costs_confirmed"
            and classified_total_candidate is None
            and candidate.get("role") == "total_candidate"
            and candidate.get("should_sum")
        ):
            classified_total_candidate = {"candidate": candidate, "item": item}

    summary = money_value.get("summary") if isinstance(money_value.get("summary"), dict) else {}
    double_count_risk = bool(summary.get("double_count_risk")) or any(candidate.get("parent_total_candidate_id") for candidate in candidates)
    stale_removed = bool(STALE_REGOLARIZZAZIONE_RE.search(_legacy_money_text(legacy_result)))

    any_items = any(grouped[key] for key in grouped)
    if not (any_items or stale_removed or double_count_risk):
        return None, {
            "safe_cost_count": 0,
            "signal_count": 0,
            "excluded_count": 0,
            "valuation_count": 0,
            "double_count_risk": double_count_risk,
            "stale_removed": stale_removed,
        }

    buyer_confirmed = _consolidate_repeated_items(grouped["buyer_costs_confirmed"])
    buyer_signals = _consolidate_repeated_items(grouped["buyer_cost_signals_to_verify"])
    valuation_refs = _consolidate_repeated_items(grouped["valuation_references"])
    price_refs = _consolidate_repeated_items(grouped["price_references"])
    valuation_deds = _consolidate_repeated_items(grouped["valuation_deductions"])
    cadastral_vals = _consolidate_repeated_items(grouped["cadastral_values"])
    formalities = _consolidate_repeated_items(grouped["formalities_and_procedural_amounts"])
    other_mentions = _consolidate_repeated_items(grouped["other_monetary_mentions"])
    unsupported = _consolidate_repeated_items(grouped["unsupported_or_unknown_amounts"])

    excluded_non_buyer_cost_amounts: List[Dict[str, Any]] = []
    for key in _NON_BUYER_GROUPS:
        for item in grouped[key]:
            excluded_non_buyer_cost_amounts.append(copy.deepcopy(item))

    qualitative_burdens = [
        copy.deepcopy(item)
        for item in buyer_signals
        if item.get("amount_eur") is None or not item.get("additive_to_extra_total")
    ] or copy.deepcopy(buyer_signals)

    total: Dict[str, Any]
    total_extra_cost_eur: Optional[int]
    if classified_total_candidate:
        candidate = classified_total_candidate["candidate"]
        amount = int(round(float(candidate.get("amount_eur"))))
        total_extra_cost_eur = amount
        total = {
            "range": {"min": amount, "max": amount},
            "max_is_open": False,
            "note": "Totale buyer-side esplicitamente supportato in perizia; componenti non sommate una seconda volta.",
            "contract_state": "quantified_estimate",
            "evidence": _evidence_from_candidate(candidate),
        }
    else:
        total_extra_cost_eur = None
        if buyer_confirmed or buyer_signals:
            note = (
                "Oneri non quantificati in modo difendibile come totale unico: usare le voci sotto come checklist "
                "di verifica; nessun totale economico certo è indicato."
            )
            contract_state = "unresolved_explained"
        else:
            note = (
                "Nessun costo extra buyer-side certo ricavabile dalla perizia; gli importi valutativi/procedurali "
                "sono mostrati come riferimenti e non concorrono al totale dei costi extra."
            )
            contract_state = "info_only"
        first_evidence_source: Optional[Dict[str, Any]] = None
        for bucket in (buyer_confirmed, buyer_signals, valuation_refs, valuation_deds, cadastral_vals, formalities, price_refs, other_mentions):
            if bucket:
                first_evidence_source = bucket[0]
                break
        total = {
            "min": None,
            "max": None,
            "max_is_open": False,
            "note": note,
            "contract_state": contract_state,
            "evidence": copy.deepcopy((first_evidence_source or {}).get("evidence", [])),
        }

    money_box = {
        "policy": "AUTHORITY_CONSERVATIVE",
        "items": copy.deepcopy(buyer_confirmed),
        "buyer_costs_confirmed": buyer_confirmed,
        "buyer_cost_signals_to_verify": buyer_signals,
        "cost_signals_to_verify": copy.deepcopy(buyer_signals),
        "valuation_references": valuation_refs,
        "valuation_reference_amounts": copy.deepcopy(valuation_refs),
        "price_references": price_refs,
        "valuation_deductions": valuation_deds,
        "cadastral_values": cadastral_vals,
        "formalities_and_procedural_amounts": formalities,
        "other_monetary_mentions": other_mentions,
        "qualitative_burdens": qualitative_burdens,
        "excluded_non_buyer_cost_amounts": excluded_non_buyer_cost_amounts,
        "unsupported_or_unknown_amounts": unsupported,
        "total_extra_costs": total,
        "total_extra_cost_eur": total_extra_cost_eur,
        # Canonical display groups — UI should render these and treat any other
        # legacy list (cost_signals_to_verify, valuation_reference_amounts,
        # excluded_non_buyer_cost_amounts) as an alias for backwards compat only.
        "canonical_display_groups": [
            "buyer_costs_confirmed",
            "buyer_cost_signals_to_verify",
            "valuation_references",
            "price_references",
            "valuation_deductions",
            "cadastral_values",
            "formalities_and_procedural_amounts",
            "other_monetary_mentions",
            "unsupported_or_unknown_amounts",
        ],
        "deprecated_alias_groups": {
            "cost_signals_to_verify": "buyer_cost_signals_to_verify",
            "valuation_reference_amounts": "valuation_references",
            "excluded_non_buyer_cost_amounts": "valuation_references+price_references+valuation_deductions+cadastral_values+formalities_and_procedural_amounts",
        },
    }
    money_box["customer_summary"] = _build_customer_summary(money_box)
    if double_count_risk:
        money_box["component_total_policy"] = "componenti_non_sommate_con_totale"
    return money_box, {
        "safe_cost_count": len(buyer_confirmed),
        "signal_count": len(buyer_signals),
        "excluded_count": len(excluded_non_buyer_cost_amounts),
        "valuation_count": len(valuation_refs) + len(valuation_deds),
        "price_reference_count": len(price_refs),
        "cadastral_count": len(cadastral_vals),
        "formality_count": len(formalities),
        "other_mention_count": len(other_mentions),
        "unsupported_count": len(unsupported),
        "double_count_risk": double_count_risk,
        "stale_removed": stale_removed,
    }


def _set_money_boxes(result: Dict[str, Any], money_box: Dict[str, Any]) -> List[str]:
    changed: List[str] = []
    section3 = _section3_from_money_box(money_box)
    cdc = result.get("customer_decision_contract") if isinstance(result.get("customer_decision_contract"), dict) else None
    if isinstance(cdc, dict):
        if cdc.get("money_box") != money_box:
            changed.append("customer_decision_contract.money_box")
        cdc["money_box"] = copy.deepcopy(money_box)
        if cdc.get("section_3_money_box") != section3:
            changed.append("customer_decision_contract.section_3_money_box")
        cdc["section_3_money_box"] = copy.deepcopy(section3)
        if "money_box" in result:
            if result.get("money_box") != money_box:
                changed.append("money_box")
            result["money_box"] = copy.deepcopy(money_box)
        if "section_3_money_box" in result:
            if result.get("section_3_money_box") != section3:
                changed.append("section_3_money_box")
            result["section_3_money_box"] = copy.deepcopy(section3)
        return changed

    if result.get("money_box") != money_box:
        changed.append("money_box")
    result["money_box"] = copy.deepcopy(money_box)
    if result.get("section_3_money_box") != section3:
        changed.append("section_3_money_box")
    result["section_3_money_box"] = copy.deepcopy(section3)
    return changed


def apply_authority_money_projection_if_enabled(
    result: Dict[str, Any],
    pages_raw: Optional[Sequence[Dict[str, Any]]] = None,
    section_authority_map: Any = None,
    candidate_artifacts: Any = None,
    *,
    analysis_id: Optional[str] = None,
    authority_shadow: Optional[Dict[str, Any]] = None,
    request_id: Optional[str] = None,
) -> Dict[str, Any]:
    enabled = os.environ.get(FEATURE_FLAG) == "1"
    meta = _base_meta(enabled)
    if analysis_id:
        meta["analysis_id"] = str(analysis_id)
    if request_id:
        meta["request_id"] = str(request_id)
    if not enabled:
        return meta
    if not isinstance(result, dict):
        meta.update({"status": "FAIL_OPEN", "reason": "invalid_result", "fail_open": True})
        return meta

    shadow, build_notes = _shadow_from_inputs(pages_raw, section_authority_map, candidate_artifacts, authority_shadow)
    meta["notes"].extend(build_notes)
    if not isinstance(shadow, dict):
        meta.update({"status": "FAIL_OPEN", "reason": "missing_or_invalid_authority_shadow", "fail_open": True})
        _attach_meta(result, meta)
        return meta

    money_row = _money_row(shadow)
    money_value = _money_value(money_row)
    money_status = str(money_row.get("status") or "unknown")
    confidence = float(money_row.get("confidence") or 0.0)
    notes = [str(note) for note in (money_row.get("notes") or [])]
    candidates = money_value.get("money_candidates") if isinstance(money_value.get("money_candidates"), list) else []
    meta.update(
        {
            "money_status": money_status,
            "authority_confidence": round(confidence, 4),
            "candidate_count": len(candidates),
        }
    )
    meta["notes"].extend(notes)

    if bool(money_row.get("fail_open")) or money_status == "FAIL_OPEN" or "mostly_unknown_authority_map" in notes:
        meta.update({"status": "FAIL_OPEN", "reason": "authority_money_fail_open", "fail_open": True})
        _attach_meta(result, meta)
        return meta
    if money_status not in SAFE_MONEY_STATUSES:
        meta.update({"status": "INSUFFICIENT_EVIDENCE", "reason": "authority_money_not_projectable"})
        _attach_meta(result, meta)
        return meta
    if confidence < 0.55:
        meta.update({"status": "NOT_APPLIED_LOW_CONFIDENCE", "reason": "authority_money_low_confidence"})
        _attach_meta(result, meta)
        return meta

    # Augment candidate pool with reliable economic values found in
    # result["estratto_quality"] sections and with amounts only present inside
    # the evidence-quote field of an existing candidate (rule from the brief:
    # estratto_quality bridge + evidence-quote amount extraction).
    estratto_added = _augment_with_estratto_quality_candidates(money_value, result)
    evidence_added = _augment_with_evidence_quote_amounts(money_value)
    if estratto_added:
        meta["estratto_quality_candidates_added"] = estratto_added
        meta["notes"].append("estratto_quality_bridge_applied")
    if evidence_added:
        meta["evidence_quote_candidates_added"] = evidence_added
        meta["notes"].append("evidence_quote_amount_extraction_applied")
    if estratto_added or evidence_added:
        meta["candidate_count"] = int(meta.get("candidate_count") or 0) + estratto_added + evidence_added

    projected, stats = _build_projected_money_box(money_value, result)
    meta["component_total_double_count_prevented"] = bool(stats.get("double_count_risk"))
    meta["stale_money_removed"] = bool(stats.get("stale_removed"))
    if not isinstance(projected, dict):
        meta.update({"status": "NOT_APPLIED_NO_ACTIONABLE_AUTHORITY", "reason": "no_projectable_money_roles"})
        _attach_meta(result, meta)
        return meta

    changed_fields = _set_money_boxes(result, projected)
    meta.update(
        {
            "status": "APPLIED" if changed_fields else "ALREADY_MATCHES",
            "applied": bool(changed_fields),
            "reason": "authority_money_projection_applied" if changed_fields else "authority_money_projection_already_matches",
            "projected_items_count": len(projected.get("items") or []),
            "cost_signals_to_verify_count": int(stats.get("signal_count") or 0),
            "excluded_non_buyer_cost_count": int(stats.get("excluded_count") or 0),
            "valuation_reference_count": int(stats.get("valuation_count") or 0),
            "changed_fields": changed_fields,
        }
    )
    if changed_fields:
        qa_sanitize_meta = _sanitize_stale_money_qa_claims_after_projection(result, projected)
        removed_count = int(qa_sanitize_meta.get("removed_money_qa_claims_count") or 0)
        meta["removed_money_qa_claims_count"] = removed_count
        meta["removed_paths"] = qa_sanitize_meta.get("removed_paths") or []
        meta["qa_money_claim_sanitizer_reason_codes"] = qa_sanitize_meta.get("reason_codes") or []
        if removed_count:
            meta["notes"].append("stale_money_qa_claims_removed")
    _attach_meta(result, meta)
    return meta


def _attach_meta(result: Dict[str, Any], meta: Dict[str, Any]) -> None:
    if not isinstance(result, dict):
        return
    debug = result.get("debug") if isinstance(result.get("debug"), dict) else {}
    debug["authority_money_projection"] = copy.deepcopy(meta)
    result["debug"] = debug
