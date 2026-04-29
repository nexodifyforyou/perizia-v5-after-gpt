import hashlib
import json
import os
import re
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import httpx


def _safe_text(value: Any, limit: int = 400) -> str:
    text = str(value or "").strip()
    return text[:limit]


def _extract_top_blockers(semaforo: Dict[str, Any], limit: int = 2) -> List[str]:
    blockers = semaforo.get("top_blockers")
    if not isinstance(blockers, list):
        return []
    out: List[str] = []
    for item in blockers:
        label = ""
        if isinstance(item, dict):
            label = str(item.get("label_it") or item.get("key") or item.get("code") or "").strip()
        else:
            label = str(item or "").strip()
        if not label:
            continue
        out.append(label[:120])
        if len(out) >= limit:
            break
    return out


def _append_evidence_pool(
    evidence: Any,
    dedupe: Dict[Tuple[int, str], str],
    pool: List[Dict[str, Any]],
) -> List[str]:
    if not isinstance(evidence, list):
        return []
    refs: List[str] = []
    for item in evidence:
        if not isinstance(item, dict):
            continue
        page = item.get("page")
        quote = str(item.get("quote") or "").strip()
        if not isinstance(page, int) or not quote:
            continue
        quote_hash = hashlib.sha256(quote.encode("utf-8")).hexdigest()[:16]
        sig = (page, quote_hash)
        ev_id = dedupe.get(sig)
        if not ev_id:
            ev_id = f"ev_{len(pool) + 1:03d}"
            dedupe[sig] = ev_id
            payload: Dict[str, Any] = {
                "ev_id": ev_id,
                "page": page,
                "quote": quote[:520],
            }
            hint = str(item.get("search_hint") or "").strip()
            if hint:
                payload["search_hint"] = hint[:180]
            pool.append(payload)
        if ev_id not in refs:
            refs.append(ev_id)
    return refs


def build_fact_pack(result: Dict[str, Any]) -> Dict[str, Any]:
    dedupe: Dict[Tuple[int, str], str] = {}
    evidence_pool: List[Dict[str, Any]] = []

    semaforo = result.get("semaforo_generale", {}) if isinstance(result.get("semaforo_generale"), dict) else {}
    semaforo_status = _safe_text(semaforo.get("status"), 60)
    top_blockers = _extract_top_blockers(semaforo, limit=2)
    semaforo_fact = {
        "status": semaforo_status,
        "top_blockers": top_blockers,
        "reason_it": _safe_text(semaforo.get("reason_it"), 240),
        "reason_en": _safe_text(semaforo.get("reason_en"), 240),
    }

    messages_fact: List[Dict[str, Any]] = []
    user_messages = result.get("user_messages", [])
    if isinstance(user_messages, list):
        for msg in user_messages[:6]:
            if not isinstance(msg, dict):
                continue
            refs = _append_evidence_pool(msg.get("evidence"), dedupe, evidence_pool)
            messages_fact.append(
                {
                    "code": _safe_text(msg.get("code"), 80),
                    "severity": _safe_text(msg.get("severity"), 40),
                    "title_it": _safe_text(msg.get("title_it"), 160),
                    "body_it": _safe_text(msg.get("body_it"), 240),
                    "evidence_refs": refs,
                }
            )

    blueprint_fact: Dict[str, Any] = {"non_agibile": None, "impianti": []}
    blueprint = result.get("estratto_blueprint", {}) if isinstance(result.get("estratto_blueprint"), dict) else {}
    abusi = blueprint.get("abusi", {}) if isinstance(blueprint.get("abusi"), dict) else {}
    non_agibile = abusi.get("non_agibile", {}) if isinstance(abusi.get("non_agibile"), dict) else {}
    if non_agibile:
        blueprint_fact["non_agibile"] = {
            "value": bool(non_agibile.get("value")),
            "evidence_refs": _append_evidence_pool(non_agibile.get("evidence"), dedupe, evidence_pool),
        }
    impianti = blueprint.get("impianti", {}) if isinstance(blueprint.get("impianti"), dict) else {}
    for key in sorted(impianti.keys()):
        obj = impianti.get(key)
        if not isinstance(obj, dict):
            continue
        value = obj.get("value")
        if value in (None, "", "NOT_FOUND"):
            continue
        blueprint_fact["impianti"].append(
            {
                "field": key,
                "value": _safe_text(value, 180),
                "evidence_refs": _append_evidence_pool(obj.get("evidence"), dedupe, evidence_pool),
            }
        )

    money_fact: List[Dict[str, Any]] = []
    money_box = result.get("money_box", {}) if isinstance(result.get("money_box"), dict) else {}
    items = money_box.get("items", [])
    if isinstance(items, list):
        for item in items[:20]:
            if not isinstance(item, dict):
                continue
            if str(item.get("type") or "").upper() != "ESTIMATE":
                continue
            stima = item.get("stima_euro")
            if not isinstance(stima, (int, float)):
                continue
            fonte = item.get("fonte_perizia", {}) if isinstance(item.get("fonte_perizia"), dict) else {}
            refs = _append_evidence_pool(fonte.get("evidence"), dedupe, evidence_pool)
            if not refs:
                continue
            money_fact.append(
                {
                    "code": _safe_text(item.get("code"), 20),
                    "label_it": _safe_text(item.get("label_it"), 120),
                    "stima_euro": float(stima),
                    "evidence_refs": refs,
                }
            )

    legal_fact: List[Dict[str, Any]] = []
    section_legal = result.get("section_9_legal_killers", {}) if isinstance(result.get("section_9_legal_killers"), dict) else {}
    legal_items = section_legal.get("items", [])
    if isinstance(legal_items, list):
        for item in legal_items[:6]:
            if not isinstance(item, dict):
                continue
            refs = _append_evidence_pool(item.get("evidence"), dedupe, evidence_pool)
            legal_fact.append(
                {
                    "title": _safe_text(item.get("killer"), 140),
                    "status": _safe_text(item.get("status"), 20),
                    "evidence_refs": refs,
                }
            )

    document_quality = result.get("document_quality", {}) if isinstance(result.get("document_quality"), dict) else {}
    fact_pack = {
        "document_quality": {
            "status": _safe_text(document_quality.get("status"), 30),
        },
        "semaforo_generale": semaforo_fact,
        "user_messages": messages_fact,
        "estratto_blueprint": blueprint_fact,
        "money_box_estimates": money_fact,
        "legal_signals": legal_fact,
        "evidence_pool": evidence_pool,
    }
    serialized = json.dumps(fact_pack, ensure_ascii=False, separators=(",", ":"))
    if len(serialized) > 12000:
        for ev in fact_pack["evidence_pool"]:
            if isinstance(ev, dict):
                ev["quote"] = _safe_text(ev.get("quote"), 220)
                if "search_hint" in ev:
                    ev["search_hint"] = _safe_text(ev.get("search_hint"), 80)
        serialized = json.dumps(fact_pack, ensure_ascii=False, separators=(",", ":"))
        if len(serialized) > 12000 and isinstance(fact_pack.get("user_messages"), list):
            fact_pack["user_messages"] = fact_pack["user_messages"][:3]
    return fact_pack


def _first_non_empty(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _truncate_sentence(text: Any, limit: int = 220) -> str:
    value = str(text or "").strip()
    if not value:
        return ""
    return value[:limit]


def _canonical_issue_it_to_en(text: Any) -> str:
    value = _safe_text(text, 180)
    normalized = value.lower()
    if "formalità da cancellare" in normalized or "formalita da cancellare" in normalized:
        return "Encumbrances to be cancelled"
    if "servitù" in normalized or "servitu" in normalized:
        return "Easement detected"
    if "occupazione" in normalized:
        return "Occupancy issue"
    if "agibilità" in normalized or "agibilita" in normalized or "abitabilità" in normalized or "abitabilita" in normalized:
        return "Habitability issue"
    if "difformità" in normalized or "difformita" in normalized or "catastal" in normalized or "urbanistic" in normalized:
        return "Urban / cadastral discrepancies"
    if "pignoramento" in normalized or "esecuzione" in normalized:
        return "Foreclosure / execution context"
    if "vincolo" in normalized or "accesso" in normalized:
        return "Access / binding restriction to verify"
    return ""


def build_summary_for_client_bundle(result: Dict[str, Any]) -> Dict[str, Any]:
    verifier_runtime = result.get("verifier_runtime", {}) if isinstance(result.get("verifier_runtime"), dict) else {}
    canonical_case = verifier_runtime.get("canonical_case", {}) if isinstance(verifier_runtime.get("canonical_case"), dict) else {}
    verifier_bundle = canonical_case.get("summary_bundle", {}) if isinstance(canonical_case.get("summary_bundle"), dict) else {}
    document_quality = result.get("document_quality", {}) if isinstance(result.get("document_quality"), dict) else {}
    canonical_contract_state = result.get("canonical_contract_state", {}) if isinstance(result.get("canonical_contract_state"), dict) else {}
    blocked_unreadable = (
        str(result.get("analysis_status") or "").upper() == "UNREADABLE"
        or str(document_quality.get("status") or "").upper() == "UNREADABLE"
        or str(canonical_contract_state.get("reason") or "").lower() == "canonical_freeze_blocked_unreadable"
    )
    if blocked_unreadable:
        blocked_it = _truncate_sentence(
            _first_non_empty(
                document_quality.get("customer_message_it"),
                "Documento non leggibile o estrazione bloccata: non è possibile formulare conclusioni affidabili senza verifica manuale.",
            ),
            320,
        )
        blocked_en = _truncate_sentence(
            _first_non_empty(
                document_quality.get("customer_message_en"),
                "Unreadable document or blocked extraction: no reliable conclusion can be produced without manual review.",
            ),
            320,
        )
        return {
            "top_issue_it": "",
            "top_issue_en": "",
            "next_step_it": blocked_it,
            "next_step_en": blocked_en,
            "caution_points_it": ["Verifica manuale obbligatoria sul documento originale."],
            "user_messages_it": [],
            "document_quality_status": _truncate_sentence(document_quality.get("status"), 40),
            "semaforo_status": "UNKNOWN",
            "decision_summary_it": blocked_it,
            "decision_summary_en": blocked_en,
            "evidence_snippets": [],
        }
    if verifier_bundle:
        return verifier_bundle
    decision = result.get("decision_rapida_client", {}) if isinstance(result.get("decision_rapida_client"), dict) else {}
    section2 = result.get("section_2_decisione_rapida", {}) if isinstance(result.get("section_2_decisione_rapida"), dict) else {}
    narrated = result.get("decision_rapida_narrated", {}) if isinstance(result.get("decision_rapida_narrated"), dict) else {}
    semaforo = result.get("semaforo_generale", {}) if isinstance(result.get("semaforo_generale"), dict) else {}
    section_legal = result.get("section_9_legal_killers", {}) if isinstance(result.get("section_9_legal_killers"), dict) else {}
    legal_items = section_legal.get("top_items", [])
    if not isinstance(legal_items, list) or not legal_items:
        legal_items = section_legal.get("items", []) if isinstance(section_legal.get("items"), list) else []
    user_messages = result.get("user_messages", []) if isinstance(result.get("user_messages"), list) else []

    top_issue_it = ""
    top_issue_en = ""
    top_issue_evidence: List[Dict[str, Any]] = []
    if legal_items:
        first = legal_items[0] if isinstance(legal_items[0], dict) else {}
        top_issue_it = _truncate_sentence(first.get("killer"), 140)
        top_issue_en = _truncate_sentence(
            _first_non_empty(
                first.get("killer_en"),
                first.get("title_en"),
                first.get("label_en"),
                _canonical_issue_it_to_en(first.get("killer")),
            ),
            140,
        )
        if isinstance(first.get("evidence"), list):
            top_issue_evidence = [ev for ev in first.get("evidence", []) if isinstance(ev, dict)][:2]

    bullets_it = narrated.get("bullets_it", []) if isinstance(narrated.get("bullets_it"), list) else []
    bullets_en = narrated.get("bullets_en", []) if isinstance(narrated.get("bullets_en"), list) else []
    next_step_it = _truncate_sentence(_first_non_empty(
        bullets_it[0] if bullets_it else "",
        decision.get("summary_it"),
        section2.get("summary_it"),
    ))
    next_step_en = _truncate_sentence(_first_non_empty(
        bullets_en[0] if bullets_en else "",
        decision.get("summary_en"),
        section2.get("summary_en"),
    ))

    caution_points_it: List[str] = []
    for bullet in bullets_it[1:3]:
        text = _truncate_sentence(bullet, 180)
        if text:
            caution_points_it.append(text)
    if not caution_points_it:
        for item in legal_items[1:3]:
            if not isinstance(item, dict):
                continue
            text = _truncate_sentence(item.get("killer"), 140)
            if text and text not in caution_points_it:
                caution_points_it.append(text)

    messages_it: List[str] = []
    for item in user_messages[:3]:
        if not isinstance(item, dict):
            continue
        text = _truncate_sentence(item.get("title_it") or item.get("body_it"), 180)
        if text:
            messages_it.append(text)

    evidence_snippets: List[Dict[str, Any]] = []
    for ev in top_issue_evidence[:2]:
        page = ev.get("page")
        quote = _truncate_sentence(ev.get("quote"), 240)
        if isinstance(page, int) and quote:
            evidence_snippets.append({"page": page, "quote": quote})

    return {
        "top_issue_it": top_issue_it,
        "top_issue_en": top_issue_en,
        "next_step_it": next_step_it,
        "next_step_en": next_step_en,
        "caution_points_it": caution_points_it[:2],
        "user_messages_it": messages_it[:2],
        "document_quality_status": _truncate_sentence(document_quality.get("status"), 40),
        "semaforo_status": _truncate_sentence(semaforo.get("status"), 20),
        "decision_summary_it": _truncate_sentence(_first_non_empty(narrated.get("it"), decision.get("summary_it"), section2.get("summary_it")), 320),
        "decision_summary_en": _truncate_sentence(_first_non_empty(narrated.get("en"), decision.get("summary_en"), section2.get("summary_en")), 320),
        "evidence_snippets": evidence_snippets,
    }


def build_deterministic_summary_for_client(result: Dict[str, Any]) -> Dict[str, str]:
    bundle = build_summary_for_client_bundle(result)
    top_issue_it = bundle.get("top_issue_it", "")
    top_issue_en = bundle.get("top_issue_en", "")
    next_step_it = bundle.get("next_step_it", "")
    next_step_en = bundle.get("next_step_en", "")
    caution_points_it = bundle.get("caution_points_it", []) if isinstance(bundle.get("caution_points_it"), list) else []

    summary_it_parts: List[str] = []
    if top_issue_it:
        summary_it_parts.append(str(top_issue_it))
    decision_it = str(bundle.get("decision_summary_it") or "").strip()
    if not top_issue_it and decision_it:
        summary_it_parts.append(decision_it)
    if next_step_it:
        summary_it_parts.append(next_step_it)
    elif caution_points_it:
        summary_it_parts.append(str(caution_points_it[0]))
    if not summary_it_parts:
        summary_it_parts.append("Analisi completata con verifiche manuali ancora necessarie.")

    summary_en_parts: List[str] = []
    decision_en = str(bundle.get("decision_summary_en") or "").strip()
    if top_issue_en:
        summary_en_parts.append(f"Key issue: {top_issue_en}.")
    elif decision_en:
        summary_en_parts.append(decision_en)
    if next_step_en:
        summary_en_parts.append(next_step_en)
    elif next_step_it:
        summary_en_parts.append(f"Next check: {next_step_it}.")
    elif caution_points_it:
        summary_en_parts.append(f"Next check: {caution_points_it[0]}.")
    if not summary_en_parts:
        summary_en_parts.append("Analysis completed with manual checks still required.")

    return {
        "summary_it": " ".join(part.strip().rstrip(".") + "." for part in summary_it_parts if str(part).strip())[:1500],
        "summary_en": " ".join(part.strip().rstrip(".") + "." for part in summary_en_parts if str(part).strip())[:1500],
    }


def _extract_json_payload(raw: str) -> Dict[str, Any]:
    payload = str(raw or "").strip()
    if payload.startswith("```json"):
        payload = payload[7:]
    if payload.startswith("```"):
        payload = payload[3:]
    if payload.endswith("```"):
        payload = payload[:-3]
    parsed = json.loads(payload.strip())
    if not isinstance(parsed, dict):
        raise ValueError("Narrator response is not an object")
    return parsed


def _extract_number_tokens(text: str) -> List[str]:
    pattern = re.compile(r"\b\d{2}/\d{2}/\d{4}\b|\b\d{1,2}:\d{2}\b|\b\d{1,3}(?:\.\d{3})*(?:,\d+)?\b|\b\d+\b")
    return sorted(set(pattern.findall(text)))


def _validate_narrated_payload(
    payload: Dict[str, Any],
    fact_pack: Dict[str, Any],
    required_status: str,
    top_blockers: List[str],
) -> List[str]:
    errors: List[str] = []
    required_keys = ("it", "en", "bullets_it", "bullets_en", "evidence_refs")
    for key in required_keys:
        if key not in payload:
            errors.append(f"missing_key:{key}")
    if errors:
        return errors

    if not isinstance(payload.get("it"), str) or not payload["it"].strip():
        errors.append("invalid:it")
    if not isinstance(payload.get("en"), str) or not payload["en"].strip():
        errors.append("invalid:en")
    if not isinstance(payload.get("bullets_it"), list):
        errors.append("invalid:bullets_it")
    if not isinstance(payload.get("bullets_en"), list):
        errors.append("invalid:bullets_en")
    refs = payload.get("evidence_refs")
    if not isinstance(refs, list) or not refs:
        errors.append("invalid:evidence_refs")

    evidence_ids = {str(ev.get("ev_id")) for ev in fact_pack.get("evidence_pool", []) if isinstance(ev, dict)}
    if isinstance(refs, list):
        for ref in refs:
            if str(ref) not in evidence_ids:
                errors.append("invalid:evidence_ref_unknown")
                break

    it_text = str(payload.get("it") or "")
    lower_it = it_text.lower()
    if required_status and required_status.lower() not in lower_it:
        errors.append("missing:semaforo_status_in_it")
    for blocker in top_blockers[:2]:
        if blocker and blocker.lower() not in lower_it:
            errors.append("missing:blocker_in_it")

    combined = " ".join(
        [str(payload.get("it") or ""), str(payload.get("en") or "")]
        + [str(x) for x in (payload.get("bullets_it") or [])]
        + [str(x) for x in (payload.get("bullets_en") or [])]
    )
    combined_lower = combined.lower()
    doc_quality = fact_pack.get("document_quality", {}) if isinstance(fact_pack.get("document_quality"), dict) else {}
    doc_status = str(doc_quality.get("status") or "").upper()
    if doc_status == "TEXT_OK" and ("parziale" in combined_lower or "ocr" in combined_lower):
        errors.append("invalid:text_ok_forbidden_partial_or_ocr")
    if "estratto" in combined_lower:
        errors.append("invalid:forbidden_word_estratto")

    fact_text = json.dumps(fact_pack, ensure_ascii=False)
    for token in _extract_number_tokens(combined):
        if token not in fact_text:
            errors.append("invalid:number_token_not_in_fact_pack")
            break
    return errors


def _as_float(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("€", "").replace("euro", "").replace("EUR", "").strip()
    normalized = re.sub(r"\s+", "", normalized)
    if "," in normalized and "." in normalized:
        normalized = normalized.replace(".", "").replace(",", ".")
    elif "," in normalized:
        normalized = normalized.replace(",", ".")
    elif normalized.count(".") == 1 and len(normalized.rsplit(".", 1)[-1]) == 3:
        normalized = normalized.replace(".", "")
    try:
        return float(normalized)
    except Exception:
        return None


def _first_float(*values: Any) -> Optional[float]:
    for value in values:
        parsed = _as_float(value)
        if parsed is not None:
            return parsed
    return None


def _compact_evidence_refs(value: Any, limit: int = 2) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not isinstance(value, list):
        return out
    for item in value:
        if not isinstance(item, dict):
            continue
        page = item.get("page") or item.get("page_number")
        try:
            page_int = int(page)
        except Exception:
            page_int = None
        quote = _safe_text(item.get("quote") or item.get("text") or item.get("snippet"), 240)
        if page_int is None and not quote:
            continue
        ev: Dict[str, Any] = {}
        if page_int is not None:
            ev["page"] = page_int
        if quote:
            ev["quote"] = quote
        out.append(ev)
        if len(out) >= limit:
            break
    return out


_OCCUPANCY_EVIDENCE_SUPPORT_RE = re.compile(
    r"\b("
    r"liber[oaie]?|libero\s+da\s+occupazioni|occupat[oaie]?|occupazione|"
    r"stato\s+(?:di\s+)?(?:possesso|occupazione)|disponibil[ei]?"
    r"|detenut[oaie]?|detentor[ei]|locat[oaie]?|locazione|conduttor[ei]|rilascio"
    r")\b",
    re.I,
)


def _evidence_quote_text(evidence: Any) -> str:
    if not isinstance(evidence, list):
        return ""
    return " ".join(
        str(item.get("quote") or item.get("text") or item.get("snippet") or "")
        for item in evidence
        if isinstance(item, dict)
    )


def _occupancy_evidence_supports_claim(value: Dict[str, Any]) -> bool:
    evidence_text = _evidence_quote_text(value.get("evidence"))
    return bool(_OCCUPANCY_EVIDENCE_SUPPORT_RE.search(evidence_text))


def _compact_field_state(value: Any, field_key: str = "") -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    out = {
        "value": _safe_text(value.get("value"), 140),
        "status": _safe_text(value.get("status"), 60),
        "headline_it": _safe_text(value.get("headline_it"), 180),
        "explanation_it": _safe_text(value.get("explanation_it"), 260),
        "verify_next_it": _safe_text(value.get("verify_next_it"), 220),
        "evidence": _compact_evidence_refs(value.get("evidence"), limit=2),
    }
    if field_key == "stato_occupativo" and out.get("value"):
        supports_claim = _occupancy_evidence_supports_claim(value)
        out["evidence_supports_claim"] = supports_claim
        if not supports_claim:
            out["wording_instruction"] = "needs_cautious_wording"
    return {k: v for k, v in out.items() if v not in ("", [], None)}


def _compact_issue(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    out = {
        "severity": _safe_text(value.get("severity") or value.get("status"), 40),
        "family": _safe_text(value.get("family") or value.get("category") or value.get("theme"), 80),
        "headline_it": _safe_text(
            value.get("headline_it") or value.get("title_it") or value.get("killer") or value.get("flag_it"),
            180,
        ),
        "action_it": _safe_text(value.get("action_it") or value.get("action") or value.get("verify_next_it"), 220),
        "evidence": _compact_evidence_refs(value.get("evidence"), limit=2),
    }
    return {k: v for k, v in out.items() if v not in ("", [], None)}


def _money_item_amount(value: Dict[str, Any]) -> Optional[float]:
    return _first_float(
        value.get("amount_eur"),
        value.get("stima_euro"),
        value.get("amount"),
        value.get("min"),
        value.get("max"),
    )


def _money_item_confirmed_buyer_side_obligation(value: Dict[str, Any]) -> bool:
    explicit_bool_keys = (
        "confirmed_buyer_side_obligation",
        "buyer_side_obligation_confirmed",
        "is_confirmed_buyer_side_obligation",
        "confirmed_buyer_side",
        "buyer_side_confirmed",
    )
    if any(value.get(key) is True for key in explicit_bool_keys):
        return True
    marker_text = " ".join(
        str(value.get(key) or "")
        for key in (
            "classification",
            "amount_status",
            "customer_visible_amount_status",
            "obligation_status",
            "buyer_side_obligation_status",
        )
    ).lower()
    return "confirmed_buyer_side_obligation" in marker_text or "buyer_side_obligation_confirmed" in marker_text


def _compact_money_item(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    amount = _money_item_amount(value)
    evidence = _compact_evidence_refs(value.get("evidence"), limit=2)
    if not evidence and isinstance(value.get("fonte_perizia"), dict):
        evidence = _compact_evidence_refs(value.get("fonte_perizia", {}).get("evidence"), limit=2)
    out: Dict[str, Any] = {
        "label_it": _safe_text(_scrub_stale_money_text(value.get("label_it") or value.get("label") or value.get("voce")), 180),
        "reason_it": _safe_text(_scrub_stale_money_text(value.get("reason_it") or value.get("note") or value.get("description")), 220),
        "amount_status": _safe_text(value.get("amount_status") or value.get("customer_visible_amount_status"), 80),
        "classification": _safe_text(value.get("classification") or value.get("type") or value.get("category"), 80),
        "evidence": evidence,
    }
    if amount is not None:
        out["amount_eur"] = round(amount, 2)
    if value.get("additive_to_extra_total") is not None:
        out["additive_to_extra_total"] = bool(value.get("additive_to_extra_total"))
    if _money_item_confirmed_buyer_side_obligation(value):
        out["confirmed_buyer_side_obligation"] = True
    return {k: v for k, v in out.items() if v not in ("", [], None)}


def _money_box_note_has_component_total_hint(note: Any) -> bool:
    normalized = _normalize_for_similarity(note)
    return any(
        phrase in normalized
        for phrase in (
            "componenti del totale",
            "componente del totale",
            "non un secondo totale",
            "secondo totale autonomo",
            "non sommati",
            "non sommare",
            "non sommarle",
            "non sommato",
        )
    )


def _compact_money_box(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    out: Dict[str, Any] = {
        "policy": _safe_text(value.get("policy"), 80),
        "items": [],
        "cost_signals_to_verify": [],
        "valuation_deductions": [],
        "qualitative_burdens": [],
    }
    total = value.get("total_extra_costs") if isinstance(value.get("total_extra_costs"), dict) else {}
    total_note = _safe_text(total.get("note"), 360) if total else ""
    if total:
        out["total_extra_costs"] = {
            "min": _first_float(total.get("min"), (total.get("range") or {}).get("min") if isinstance(total.get("range"), dict) else None),
            "max": _first_float(total.get("max"), (total.get("range") or {}).get("max") if isinstance(total.get("range"), dict) else None),
            "note": total_note,
            "evidence": _compact_evidence_refs(total.get("evidence"), limit=2),
        }
    for key, limit in (
        ("items", 8),
        ("cost_signals_to_verify", 8),
        ("valuation_deductions", 6),
        ("qualitative_burdens", 6),
    ):
        items = value.get(key)
        if not isinstance(items, list):
            continue
        compacted = [_compact_money_item(item) for item in items[:limit]]
        out[key] = [item for item in compacted if item]
    all_compact_items: List[Dict[str, Any]] = []
    for key in ("items", "cost_signals_to_verify", "valuation_deductions", "qualitative_burdens"):
        if isinstance(out.get(key), list):
            all_compact_items.extend([item for item in out[key] if isinstance(item, dict)])
    total_amounts: Set[float] = set()
    if isinstance(out.get("total_extra_costs"), dict):
        for key in ("min", "max"):
            amount = _as_float(out["total_extra_costs"].get(key))
            if amount is not None and amount > 0:
                total_amounts.add(round(amount, 2))
        total_amounts.update(_extract_euro_amounts_from_text(total_note))
    component_amounts = {
        round(float(item["amount_eur"]), 2)
        for item in all_compact_items
        if isinstance(item.get("amount_eur"), (int, float))
    }
    has_non_additive_items = any(item.get("additive_to_extra_total") is False for item in all_compact_items)
    has_component_hint = _money_box_note_has_component_total_hint(total_note)
    out["money_interpretation"] = {
        "total_note": total_note,
        "has_non_additive_items": has_non_additive_items,
        "has_components_of_total": has_component_hint,
        "total_amounts_eur": sorted(total_amounts),
        "component_amounts_eur": sorted(component_amounts),
        "confirmed_buyer_side_obligation_exists": any(
            item.get("confirmed_buyer_side_obligation") is True for item in all_compact_items
        ),
    }
    return {k: v for k, v in out.items() if v not in ("", [], None)}


def _lot_value(lot: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in lot and lot.get(key) not in ("", None):
            return lot.get(key)
    return None


def _compact_lot(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    out: Dict[str, Any] = {
        "lot_number": _safe_text(_lot_value(value, "lot_number", "numero_lotto", "lotto", "id"), 40),
        "tipologia": _safe_text(_lot_value(value, "tipologia", "type", "asset_type", "categoria"), 140),
        "ubicazione": _safe_text(_lot_value(value, "ubicazione", "location", "address", "short_location"), 180),
        "stato_occupativo": _safe_text(_lot_value(value, "stato_occupativo", "occupancy_status"), 120),
    }
    prezzo_base = _first_float(
        _lot_value(value, "prezzo_base_eur", "prezzo_base_asta", "prezzo_base", "base_asta", "base_price"),
    )
    valore_stima = _first_float(
        _lot_value(value, "valore_stima_eur", "valore_stima", "market_value", "stima"),
    )
    deprezzamento = _first_float(
        _lot_value(value, "deprezzamento_percentuale", "deprezzamento_pct", "deprezzamento"),
    )
    if prezzo_base is not None:
        out["prezzo_base_eur"] = round(prezzo_base, 2)
    if valore_stima is not None:
        out["valore_stima_eur"] = round(valore_stima, 2)
    if deprezzamento is not None:
        out["deprezzamento_percentuale"] = round(deprezzamento, 2)
    risk_notes = value.get("risk_notes")
    if isinstance(risk_notes, list):
        out["risk_notes"] = [_safe_text(item, 120) for item in risk_notes[:4] if _safe_text(item, 120)]
    return {k: v for k, v in out.items() if v not in ("", [], None)}


def _compact_summary_bundle(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    out = {
        "decision_summary_it": _safe_text(value.get("decision_summary_it"), 420),
        "top_issue_it": _safe_text(value.get("top_issue_it"), 180),
        "next_step_it": _safe_text(value.get("next_step_it"), 260),
        "main_risk_it": _safe_text(value.get("main_risk_it"), 220),
        "checks_it": [_safe_text(item, 180) for item in value.get("checks_it", [])[:5] if _safe_text(item, 180)]
        if isinstance(value.get("checks_it"), list) else [],
        "before_offer_it": [_safe_text(item, 180) for item in value.get("before_offer_it", [])[:5] if _safe_text(item, 180)]
        if isinstance(value.get("before_offer_it"), list) else [],
    }
    return {k: v for k, v in out.items() if v not in ("", [], None)}


def _collect_numeric_amounts(value: Any, parent_key: str = "") -> Set[float]:
    amounts: Set[float] = set()
    amount_key = bool(re.search(r"(eur|euro|amount|stima|prezzo|valore|costo|spes|oneri|min|max)", parent_key, re.I))
    if isinstance(value, dict):
        for key, child in value.items():
            amounts.update(_collect_numeric_amounts(child, str(key)))
    elif isinstance(value, list):
        for child in value:
            amounts.update(_collect_numeric_amounts(child, parent_key))
    elif amount_key:
        parsed = _as_float(value)
        if parsed is not None and parsed > 0:
            amounts.add(round(parsed, 2))
    elif isinstance(value, str):
        amounts.update(_extract_euro_amounts_from_text(value))
    return amounts


def build_clean_customer_decision_fact_pack(result: Dict[str, Any]) -> Dict[str, Any]:
    """Build the bounded Gemini input from final customer-facing contract data only."""
    cdc = result.get("customer_decision_contract") if isinstance(result.get("customer_decision_contract"), dict) else {}
    source = cdc if cdc else result
    root_lots = result.get("lots") if isinstance(result.get("lots"), list) else []
    cdc_lots = source.get("lots") if isinstance(source.get("lots"), list) else []
    lots = root_lots or cdc_lots
    compact_lots = [_compact_lot(lot) for lot in lots[:12]]
    compact_lots = [lot for lot in compact_lots if lot]
    lots_count = _first_float(result.get("lots_count"), source.get("lots_count"))
    if lots_count is None and compact_lots:
        lots_count = float(len(compact_lots))
    is_multi_lot = bool(result.get("is_multi_lot") or source.get("is_multi_lot") or (lots_count or 0) > 1)

    field_states_src = source.get("field_states") if isinstance(source.get("field_states"), dict) else {}
    wanted_field_states = {}
    for key in ("stato_occupativo", "opponibilita_occupazione", "agibilita", "regolarita_urbanistica"):
        compact = _compact_field_state(field_states_src.get(key), field_key=key)
        if compact:
            wanted_field_states[key] = compact
    occupancy_state = wanted_field_states.get("stato_occupativo")
    if isinstance(occupancy_state, dict) and occupancy_state.get("evidence_supports_claim") is False:
        for lot in compact_lots:
            if isinstance(lot, dict) and lot.get("stato_occupativo"):
                lot["stato_occupativo_reported_value"] = lot.pop("stato_occupativo")
                lot["stato_occupativo_evidence_supports_claim"] = False
                lot["stato_occupativo_wording_instruction"] = "needs_cautious_wording"

    issues_src = source.get("issues") if isinstance(source.get("issues"), list) else result.get("issues", [])
    if not isinstance(issues_src, list):
        issues_src = []
    red_flags_src = source.get("red_flags_operativi") if isinstance(source.get("red_flags_operativi"), list) else result.get("red_flags_operativi", [])
    if not isinstance(red_flags_src, list):
        red_flags_src = []
    legal_src = source.get("section_9_legal_killers") if isinstance(source.get("section_9_legal_killers"), dict) else {}
    legal_items = legal_src.get("top_items") if isinstance(legal_src.get("top_items"), list) else []
    if not legal_items:
        legal_items = legal_src.get("items") if isinstance(legal_src.get("items"), list) else []

    money_box = source.get("money_box") if isinstance(source.get("money_box"), dict) else {}
    if not money_box:
        money_box = result.get("section_3_money_box") if isinstance(result.get("section_3_money_box"), dict) else {}

    fact_pack: Dict[str, Any] = {
        "lots_count": int(lots_count) if lots_count is not None else 0,
        "is_multi_lot": bool(is_multi_lot),
        "lots": compact_lots,
        "field_states": wanted_field_states,
        "issues": [compact for compact in (_compact_issue(item) for item in issues_src[:8]) if compact],
        "red_flags": [compact for compact in (_compact_issue(item) for item in red_flags_src[:6]) if compact],
        "legal_killers": [compact for compact in (_compact_issue(item) for item in legal_items[:6]) if compact],
        "money_box": _compact_money_box(money_box),
        "asset_inventory_repair": result.get("asset_inventory_repair")
        if isinstance(result.get("asset_inventory_repair"), dict) else source.get("asset_inventory_repair", {}),
        "summary_for_client_bundle": _compact_summary_bundle(
            source.get("summary_for_client_bundle") if isinstance(source.get("summary_for_client_bundle"), dict)
            else result.get("summary_for_client_bundle")
        ),
        "semaforo": {
            "status": _safe_text((source.get("semaforo_generale") or {}).get("status") if isinstance(source.get("semaforo_generale"), dict) else "", 40),
            "reason_it": _safe_text((source.get("semaforo_generale") or {}).get("reason_it") if isinstance(source.get("semaforo_generale"), dict) else "", 220),
            "top_blockers": _extract_top_blockers(
                source.get("semaforo_generale") if isinstance(source.get("semaforo_generale"), dict) else {},
                limit=4,
            ),
        },
    }
    fact_pack["allowed_amounts_eur"] = sorted(_collect_numeric_amounts(fact_pack))
    serialized = json.dumps(fact_pack, ensure_ascii=False, separators=(",", ":"))
    if len(serialized) > 14000:
        for collection_key in ("issues", "red_flags", "legal_killers"):
            if isinstance(fact_pack.get(collection_key), list):
                fact_pack[collection_key] = fact_pack[collection_key][:4]
        if isinstance(fact_pack.get("lots"), list):
            fact_pack["lots"] = fact_pack["lots"][:6]
    return fact_pack


_GEMINI_NARRATOR_SYSTEM_PROMPT = """Sei un assistente specializzato nella lettura di perizie immobiliari per aste giudiziarie italiane.

Devi scrivere una sintesi cliente e una Decisione Rapida usando SOLO i dati strutturati forniti nel JSON.

Regole assolute:
- Non inventare fatti.
- Non inventare importi.
- Non inventare rischi.
- Non inventare il numero di lotti o beni.
- Non aggiungere conclusioni legali non presenti nei dati.
- Non dire che un importo è a carico dell'acquirente se il JSON non lo classifica come costo/segnale economico buyer-relevant.
- Non usare "a carico dell'acquirente", "esborso effettivo", "deve pagare", "costo certo" o "costi certi" salvo esplicita marcatura confirmed_buyer_side_obligation=true.
- Preferisci formule caute come "eventuale incidenza economica per l'acquirente", "eventuale esposizione economica" o "verificare se e in quale misura tali importi possano incidere sull'acquirente".
- Non trasformare prezzo base, valore di stima, valore finale, deprezzamenti o formalità/ipoteche in costi extra buyer-side.
- Se una informazione è DA VERIFICARE o NON VERIFICABILE, mantieni l'incertezza.
- Non dire "libero da occupazioni", "immobile libero", "occupato" o "locato" se field_states.stato_occupativo.evidence_supports_claim=false.
- Se lo stato occupativo è marcato needs_cautious_wording, omettilo oppure scrivi che va verificato nella sezione stato di possesso/stato occupativo.
- Scrivi in italiano naturale, chiaro e professionale.
- Non usare markdown.
- Non usare frasi generiche tipo "verificare tutti i dati" come contenuto principale.
- Non copiare la stessa frase in summary_it e decisione_rapida_it.
- summary_it deve spiegare cosa contiene il caso.
- decisione_rapida_it deve dire come il compratore dovrebbe procedere prima dell'offerta.

Differenza obbligatoria:
summary_it = descrizione fattuale compressa.
decisione_rapida_it = indicazione operativa prudenziale per il compratore.

Se il caso è multi-lotto, devi menzionare che la lettura deve essere lotto-per-lotto.
Se ci sono costi di regolarizzazione/sanatoria/ripristino ancorati, puoi menzionarli come segnali economici da verificare.
Se Money Box è vuoto o conservativo senza importi, non parlare di costi certi.
Se Money Box indica che singole voci sono componenti di un totale, non presentare totale e componenti come somme separate o additive.
Se cost_signals_to_verify contiene un totale più componenti interne, descrivi "totale + componente", non "totale e altro costo".
Se ci sono deprezzamenti, spiega che sono componenti estimative/valutative, non esborsi automatici.
Se ci sono formalità/ipoteche, spiega che sono segnali procedurali/legali da verificare o da cancellare secondo procedura, non costi extra automatici dell'acquirente.

Restituisci SOLO JSON valido, senza markdown, senza testo extra.

Schema JSON obbligatorio:
{
  "summary_it": "max 70 parole, descrizione fattuale del caso",
  "decisione_rapida_it": "max 90 parole, consiglio operativo prudenziale e specifico per il compratore",
  "main_risk_it": "rischio principale, se presente; altrimenti 'Rischio principale non determinabile automaticamente'",
  "why_it_matters_it": "perché il rischio principale incide sulla decisione d'offerta",
  "before_offer_it": [
    "controllo concreto 1",
    "controllo concreto 2",
    "controllo concreto 3"
  ],
  "not_to_confuse_it": "spiegazione breve di cosa non va confuso, ad esempio valori di stima/deprezzamenti/formalità vs costi buyer-side"
}"""


def _build_gemini_prompt(fact_pack: Dict[str, Any], request_id: str) -> str:
    return (
        f"request_id: {request_id}\n"
        "Usa SOLO questo JSON pulito:\n"
        f"{json.dumps(fact_pack, ensure_ascii=False, separators=(',', ':'))}"
    )


async def _call_gemini_narrator_llm(
    *,
    api_key: str,
    model: str,
    prompt: str,
    timeout_seconds: float,
) -> str:
    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    payload = {
        "systemInstruction": {"parts": [{"text": _GEMINI_NARRATOR_SYSTEM_PROMPT}]},
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.2,
            "topP": 0.8,
            "responseMimeType": "application/json",
        },
    }
    async with httpx.AsyncClient(timeout=timeout_seconds) as client:
        resp = await client.post(endpoint, params={"key": api_key}, json=payload)
    if resp.status_code >= 400:
        raise RuntimeError(f"gemini_http_{resp.status_code}")
    body = resp.json()
    candidates = body.get("candidates") if isinstance(body.get("candidates"), list) else []
    content = candidates[0].get("content") if candidates and isinstance(candidates[0], dict) else {}
    parts = content.get("parts") if isinstance(content, dict) and isinstance(content.get("parts"), list) else []
    text_parts = [str(part.get("text") or "") for part in parts if isinstance(part, dict) and str(part.get("text") or "").strip()]
    raw = "\n".join(text_parts).strip()
    if not raw:
        raise RuntimeError("gemini_empty_content")
    return raw


def _normalize_for_similarity(value: Any) -> str:
    text = str(value or "").lower()
    text = re.sub(r"[^\w\sàèéìòù]", " ", text, flags=re.UNICODE)
    return re.sub(r"\s+", " ", text).strip()


def _near_identical_text(first: Any, second: Any) -> bool:
    a = _normalize_for_similarity(first)
    b = _normalize_for_similarity(second)
    if not a or not b:
        return False
    if a == b:
        return True
    ratio = SequenceMatcher(None, a, b).ratio()
    tokens_a = set(a.split())
    tokens_b = set(b.split())
    jaccard = len(tokens_a & tokens_b) / max(1, len(tokens_a | tokens_b))
    return ratio >= 0.88 or (jaccard >= 0.82 and min(len(tokens_a), len(tokens_b)) >= 8)


_EURO_AMOUNT_RE = re.compile(
    r"(?:€\s*(?P<prefix>\d{1,3}(?:[\.\s]\d{3})*(?:,\d{1,2})?|\d+(?:,\d{1,2})?))"
    r"|(?P<suffix>\d{1,3}(?:[\.\s]\d{3})*(?:,\d{1,2})?|\d+(?:,\d{1,2})?)\s*(?:€|euro|eur)\b",
    re.I,
)


def _extract_euro_amounts_from_text(text: Any) -> Set[float]:
    amounts: Set[float] = set()
    for match in _EURO_AMOUNT_RE.finditer(str(text or "")):
        raw = match.group("prefix") or match.group("suffix")
        parsed = _as_float(raw)
        if parsed is not None and parsed > 0:
            amounts.add(round(parsed, 2))
    return amounts


def _amount_allowed(amount: float, allowed_amounts: Iterable[float]) -> bool:
    return any(abs(float(amount) - float(allowed)) <= 0.01 for allowed in allowed_amounts)


_ITALIAN_COUNT_WORDS = {
    "un": 1,
    "uno": 1,
    "una": 1,
    "due": 2,
    "tre": 3,
    "quattro": 4,
    "cinque": 5,
    "sei": 6,
    "sette": 7,
    "otto": 8,
    "nove": 9,
    "dieci": 10,
    "undici": 11,
    "dodici": 12,
}


def _extract_lot_counts(text: Any) -> Set[int]:
    counts: Set[int] = set()
    pattern = re.compile(
        r"\b(?P<count>\d{1,2}|un|uno|una|due|tre|quattro|cinque|sei|sette|otto|nove|dieci|undici|dodici)\s+lott[oi]\b",
        re.I,
    )
    for match in pattern.finditer(str(text or "").lower()):
        raw = match.group("count")
        if raw.isdigit():
            counts.add(int(raw))
        elif raw in _ITALIAN_COUNT_WORDS:
            counts.add(_ITALIAN_COUNT_WORDS[raw])
    return counts


def _combined_payload_text(payload: Dict[str, Any]) -> str:
    parts: List[str] = []
    for key in ("summary_it", "decisione_rapida_it", "main_risk_it", "why_it_matters_it", "not_to_confuse_it"):
        parts.append(str(payload.get(key) or ""))
    before = payload.get("before_offer_it")
    if isinstance(before, list):
        parts.extend(str(item or "") for item in before)
    return " ".join(parts)


def _field_value(fact_pack: Dict[str, Any], key: str) -> str:
    states = fact_pack.get("field_states") if isinstance(fact_pack.get("field_states"), dict) else {}
    state = states.get(key) if isinstance(states.get(key), dict) else {}
    return str(state.get("value") or state.get("status") or "").upper()


def _money_box_has_signals(fact_pack: Dict[str, Any]) -> bool:
    box = fact_pack.get("money_box") if isinstance(fact_pack.get("money_box"), dict) else {}
    for key in ("items", "cost_signals_to_verify", "valuation_deductions", "qualitative_burdens"):
        if isinstance(box.get(key), list) and box.get(key):
            return True
    total = box.get("total_extra_costs") if isinstance(box.get("total_extra_costs"), dict) else {}
    return bool(total.get("min") or total.get("max"))


def _fact_pack_has_confirmed_buyer_side_obligation(fact_pack: Dict[str, Any]) -> bool:
    box = fact_pack.get("money_box") if isinstance(fact_pack.get("money_box"), dict) else {}
    interpretation = box.get("money_interpretation") if isinstance(box.get("money_interpretation"), dict) else {}
    if interpretation.get("confirmed_buyer_side_obligation_exists") is True:
        return True
    for key in ("items", "cost_signals_to_verify", "valuation_deductions", "qualitative_burdens"):
        items = box.get(key)
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, dict) and item.get("confirmed_buyer_side_obligation") is True:
                return True
    return False


def _has_unsupported_buyer_cost_claim(text: str, fact_pack: Dict[str, Any]) -> bool:
    normalized = _normalize_for_similarity(text)
    if not _fact_pack_has_confirmed_buyer_side_obligation(fact_pack):
        forbidden_without_confirmation = (
            r"\ba\s+carico\s+dell\s+acquirente\b",
            r"\ba\s+carico\s+del\s+compratore\b",
            r"\besborso\s+effettivo\b",
            r"\bdeve\s+pagare\b",
            r"\bdovra\s+pagare\b",
            r"\bdovrà\s+pagare\b",
            r"\bdeve\s+versare\b",
            r"\bcosto\s+certo\b",
            r"\bcosti\s+certi\b",
            r"\besborso\s+certo\b",
            r"\bextra\s+certo\b",
        )
        if any(re.search(pattern, normalized) for pattern in forbidden_without_confirmation):
            return True
    certainty_patterns = (
        r"\besborso\s+certo\b",
        r"\bextra\s+certo\b",
    )
    if any(re.search(pattern, normalized) for pattern in certainty_patterns):
        return True
    if not _money_box_has_signals(fact_pack):
        unsupported_without_money = (
            "a carico dell acquirente",
            "carico dell acquirente",
            "buyer side",
            "costi extra",
            "extra budget",
            "esborso",
        )
        return any(phrase in normalized for phrase in unsupported_without_money)
    return False


def _money_box_has_component_total_hint(fact_pack: Dict[str, Any]) -> bool:
    box = fact_pack.get("money_box") if isinstance(fact_pack.get("money_box"), dict) else {}
    interpretation = box.get("money_interpretation") if isinstance(box.get("money_interpretation"), dict) else {}
    if interpretation.get("has_components_of_total") is True:
        return True
    total = box.get("total_extra_costs") if isinstance(box.get("total_extra_costs"), dict) else {}
    return _money_box_note_has_component_total_hint(total.get("note") or interpretation.get("total_note"))


def _money_box_total_and_component_amounts(fact_pack: Dict[str, Any]) -> Tuple[Set[float], Set[float]]:
    box = fact_pack.get("money_box") if isinstance(fact_pack.get("money_box"), dict) else {}
    interpretation = box.get("money_interpretation") if isinstance(box.get("money_interpretation"), dict) else {}
    totals = {
        round(float(amount), 2)
        for amount in interpretation.get("total_amounts_eur", [])
        if isinstance(amount, (int, float))
    }
    components = {
        round(float(amount), 2)
        for amount in interpretation.get("component_amounts_eur", [])
        if isinstance(amount, (int, float))
    }
    total = box.get("total_extra_costs") if isinstance(box.get("total_extra_costs"), dict) else {}
    for key in ("min", "max"):
        amount = _as_float(total.get(key))
        if amount is not None and amount > 0:
            totals.add(round(amount, 2))
    totals.update(_extract_euro_amounts_from_text(total.get("note")))
    for key in ("items", "cost_signals_to_verify"):
        items = box.get(key)
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, dict) and isinstance(item.get("amount_eur"), (int, float)):
                components.add(round(float(item["amount_eur"]), 2))
    return totals, components


def _euro_amount_mentions(text: str) -> List[Tuple[float, int, int]]:
    mentions: List[Tuple[float, int, int]] = []
    for match in _EURO_AMOUNT_RE.finditer(str(text or "")):
        raw = match.group("prefix") or match.group("suffix")
        amount = _as_float(raw)
        if amount is not None and amount > 0:
            mentions.append((round(amount, 2), match.start(), match.end()))
    return mentions


def _has_additive_double_counting_language(text: str, fact_pack: Dict[str, Any]) -> bool:
    if not _money_box_has_component_total_hint(fact_pack):
        return False
    totals, components = _money_box_total_and_component_amounts(fact_pack)
    components = {amount for amount in components if amount not in totals}
    if not totals or not components:
        return False
    mentions = _euro_amount_mentions(text)
    additive_re = re.compile(r"\b(e|ed|oltre\s+a|piu|più|si\s+aggiung\w*|aggiunt\w*|somm\w*)\b", re.I)
    safe_component_re = re.compile(r"\b(component\w*|interna|interno|compres\w*|parte\s+del\s+totale|totale\s+stimato)\b", re.I)
    for total_amount, total_start, total_end in mentions:
        if total_amount not in totals:
            continue
        for component_amount, component_start, component_end in mentions:
            if component_amount not in components:
                continue
            start = min(total_end, component_end)
            end = max(total_start, component_start)
            segment = text[start:end]
            if len(segment) > 140:
                continue
            context_start = max(0, min(total_start, component_start) - 80)
            context_end = min(len(text), max(total_end, component_end) + 80)
            context = text[context_start:context_end]
            if additive_re.search(segment) and not (safe_component_re.search(segment) or safe_component_re.search(context)):
                return True
    return False


def _occupancy_evidence_is_weak(fact_pack: Dict[str, Any]) -> bool:
    states = fact_pack.get("field_states") if isinstance(fact_pack.get("field_states"), dict) else {}
    occupancy = states.get("stato_occupativo") if isinstance(states.get("stato_occupativo"), dict) else {}
    return bool(occupancy.get("value")) and occupancy.get("evidence_supports_claim") is False


def _has_confident_occupancy_claim_with_weak_evidence(text: str, fact_pack: Dict[str, Any]) -> bool:
    if not _occupancy_evidence_is_weak(fact_pack):
        return False
    normalized = _normalize_for_similarity(text)
    confident_patterns = (
        r"\blibero\s+da\s+occupazioni\b",
        r"\bimmobile\s+libero\b",
        r"\bbene\s+libero\b",
        r"\blotto\s+libero\b",
        r"\boccupat[oaie]?\b",
        r"\blocato\b",
        r"\blocat[oaie]\b",
    )
    return any(re.search(pattern, normalized) for pattern in confident_patterns)


_RISK_KEYWORD_GROUPS = {
    "amianto": ("amianto", "fibrocemento", "fibro-cemento"),
    "servitu": ("servitù", "servitu"),
    "formalita": ("ipoteca", "ipoteche", "formalità", "formalita", "trascrizione"),
    "occupazione": ("occupato", "occupazione", "locazione", "libero", "liberazione"),
    "urbanistica": ("urbanistic", "difform", "sanatoria", "abuso", "regolarizz", "ripristino"),
    "agibilita": ("agibilità", "agibilita", "abitabilità", "abitabilita", "agibile"),
    "catastale": ("catastal", "planimetr"),
    "condominio": ("condomin", "arretrat"),
    "accesso": ("accesso", "stradella"),
    "idrogeologico": ("frana", "alluvion", "idrogeolog"),
}


def _unsupported_risk_keywords(text: str, fact_pack: Dict[str, Any]) -> List[str]:
    fact_blob = json.dumps(fact_pack, ensure_ascii=False).lower()
    text_lower = text.lower()
    unsupported: List[str] = []
    for group, terms in _RISK_KEYWORD_GROUPS.items():
        if any(term in text_lower for term in terms) and not any(term in fact_blob for term in terms):
            unsupported.append(group)
    return unsupported


def _contrary_field_claims(text: str, fact_pack: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    normalized = _normalize_for_similarity(text)
    occupancy = _field_value(fact_pack, "stato_occupativo")
    if "OCCUPATO" in occupancy and re.search(r"\blibero\b", normalized):
        errors.append("contrary:occupancy_libero")
    if "LIBERO" in occupancy and re.search(r"\boccupat[oaie]?\b", normalized):
        errors.append("contrary:occupancy_occupato")
    if occupancy in {"DA VERIFICARE", "NON VERIFICABILE", "NOT_FOUND"} and re.search(r"\b(immobile\s+)?(libero|occupato)\b", normalized):
        errors.append("contrary:occupancy_definitive_unknown")

    agibilita = _field_value(fact_pack, "agibilita")
    if agibilita in {"DA VERIFICARE", "NON VERIFICABILE", "NOT_FOUND"} and re.search(
        r"\b(non\s+agibile|agibile|agibilita\s+assente|agibilità\s+assente|abitabilita\s+assente|abitabilità\s+assente)\b",
        normalized,
    ):
        errors.append("contrary:agibilita_definitive_unknown")

    urbanistica = _field_value(fact_pack, "regolarita_urbanistica")
    if any(marker in urbanistica for marker in ("NON CONFORME", "PRESENTI DIFFORMITA", "PRESENTI DIFFORMITÀ")) and re.search(
        r"\b(conforme\s+urbanisticamente|urbanisticamente\s+conforme|regolare\s+urbanisticamente)\b",
        normalized,
    ):
        errors.append("contrary:urbanistica_conforme")
    if urbanistica in {"DA VERIFICARE", "NON VERIFICABILE", "NOT_FOUND"} and re.search(
        r"\b(non\s+conforme|conforme\s+urbanisticamente|urbanisticamente\s+conforme)\b",
        normalized,
    ):
        errors.append("contrary:urbanistica_definitive_unknown")
    return errors


def validate_gemini_decision_payload(payload: Dict[str, Any], fact_pack: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    required_string_keys = (
        "summary_it",
        "decisione_rapida_it",
        "main_risk_it",
        "why_it_matters_it",
        "not_to_confuse_it",
    )
    for key in required_string_keys:
        if not isinstance(payload.get(key), str) or not payload.get(key, "").strip():
            errors.append(f"missing_or_invalid:{key}")
    before_offer = payload.get("before_offer_it")
    if not isinstance(before_offer, list) or len([x for x in before_offer if str(x or "").strip()]) < 3:
        errors.append("missing_or_invalid:before_offer_it")
    if errors:
        return errors

    summary_it = str(payload.get("summary_it") or "").strip()
    decisione_it = str(payload.get("decisione_rapida_it") or "").strip()
    if _near_identical_text(summary_it, decisione_it):
        errors.append("invalid:summary_decision_near_identical")

    combined = _combined_payload_text(payload)
    output_amounts = _extract_euro_amounts_from_text(combined)
    allowed_amounts = set(fact_pack.get("allowed_amounts_eur") or [])
    for amount in output_amounts:
        if not _amount_allowed(amount, allowed_amounts):
            errors.append(f"invalid:unsupported_euro_amount:{amount:g}")
            break

    expected_lots_count = int(fact_pack.get("lots_count") or 0)
    mentioned_lot_counts = _extract_lot_counts(combined)
    for count in mentioned_lot_counts:
        if expected_lots_count and count != expected_lots_count:
            errors.append(f"invalid:unsupported_lot_count:{count}")
            break
    if bool(fact_pack.get("is_multi_lot")) and "multi" not in combined.lower() and "lotto" not in combined.lower() and "lotti" not in combined.lower():
        errors.append("invalid:missing_multi_lot_reference")

    errors.extend(_contrary_field_claims(combined, fact_pack))
    if _has_unsupported_buyer_cost_claim(combined, fact_pack):
        errors.append("invalid:unsupported_buyer_cost_claim")
    if _has_additive_double_counting_language(combined, fact_pack):
        errors.append("invalid:money_box_double_counting_language")
    if _has_confident_occupancy_claim_with_weak_evidence(combined, fact_pack):
        errors.append("invalid:unsupported_confident_occupancy_claim")
    unsupported_risks = _unsupported_risk_keywords(combined, fact_pack)
    if unsupported_risks:
        errors.append("invalid:unsupported_risk:" + ",".join(unsupported_risks[:3]))
    return errors


_STALE_REGOLARIZZAZIONE_MONEY_RE = re.compile(
    r"\bRegolarizzazione(?:\s+urbanistica)?\s*:\s*€\s*(?:6|31)\b(?![\d.,])",
    re.I,
)
_CUSTOMER_FACING_SCRUB_KEYS = (
    "money_box",
    "section_3_money_box",
    "summary_for_client",
    "summary_for_client_bundle",
    "section_2_decisione_rapida",
    "decision_rapida_client",
    "decision_rapida_narrated",
)
_SOURCE_TEXT_KEYS = {
    "quote",
    "search_hint",
    "source_quote",
    "raw_quote",
    "raw_text",
    "source_text",
    "text",
    "context",
    "full_text",
}
_UNSAFE_CUSTOMER_FACING_PHRASE_RE = re.compile(
    r"\b("
    r"esborso\s+effettivo|"
    r"deve\s+pagare|"
    r"costo\s+certo|"
    r"costi\s+certi|"
    r"costo\s+a\s+carico\s+dell['’]acquirente|"
    r"a\s+carico\s+dell['’]acquirente"
    r")\b",
    re.I,
)


def _fallback_compact_sentence(text: str, limit: int = 520) -> str:
    text = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(text) <= limit:
        return text
    truncated = text[:limit].rsplit(" ", 1)[0].rstrip(" ,;:")
    return truncated + "."


def _fallback_lot_phrase(fact_pack: Dict[str, Any]) -> str:
    lots = fact_pack.get("lots") if isinstance(fact_pack.get("lots"), list) else []
    lots_count = int(fact_pack.get("lots_count") or len(lots) or 0)
    is_multi_lot = bool(fact_pack.get("is_multi_lot"))
    asset_types: List[str] = []
    for lot in lots:
        if not isinstance(lot, dict):
            continue
        label = str(lot.get("tipologia") or "").strip()
        if label and label.lower() not in {x.lower() for x in asset_types}:
            asset_types.append(label)
        if len(asset_types) >= 3:
            break
    asset_phrase = ", ".join(asset_types)
    if is_multi_lot or lots_count > 1:
        base = f"La perizia riguarda {lots_count or len(lots)} lotti"
        if asset_phrase:
            base += f" con beni indicati come {asset_phrase}"
        return base + "."
    base = "La perizia riguarda un lotto"
    if asset_phrase:
        base += f" con bene indicato come {asset_phrase}"
    return base + "."


def _fallback_issue_headlines(fact_pack: Dict[str, Any], *, limit: int = 3) -> List[str]:
    out: List[str] = []
    weak_occupancy = _occupancy_evidence_is_weak(fact_pack)
    for collection_key in ("legal_killers", "red_flags", "issues"):
        items = fact_pack.get(collection_key) if isinstance(fact_pack.get(collection_key), list) else []
        for item in items:
            if not isinstance(item, dict):
                continue
            headline = _safe_text(
                item.get("headline_it")
                or item.get("flag_it")
                or item.get("title_it")
                or item.get("label_it")
                or item.get("code"),
                180,
            )
            if weak_occupancy and re.search(r"\b(occupat[oaie]?|libero|locat[oaie]?|occupazione)\b", _normalize_for_similarity(headline)):
                headline = "Stato occupativo da verificare nella sezione stato di possesso"
            if headline and headline.lower() not in {x.lower() for x in out}:
                out.append(headline.rstrip("."))
            if len(out) >= limit:
                return out
    semaforo = fact_pack.get("semaforo") if isinstance(fact_pack.get("semaforo"), dict) else {}
    for blocker in semaforo.get("top_blockers") or []:
        text = _safe_text(blocker, 140)
        if weak_occupancy and re.search(r"\b(occupat[oaie]?|libero|locat[oaie]?|occupazione)\b", _normalize_for_similarity(text)):
            text = "Stato occupativo da verificare nella sezione stato di possesso"
        if text and text.lower() not in {x.lower() for x in out}:
            out.append(text.rstrip("."))
        if len(out) >= limit:
            break
    return out


def _fallback_money_box_has_signals(fact_pack: Dict[str, Any]) -> bool:
    box = fact_pack.get("money_box") if isinstance(fact_pack.get("money_box"), dict) else {}
    if not box:
        return False
    for key in ("items", "cost_signals_to_verify", "qualitative_burdens"):
        if isinstance(box.get(key), list) and box.get(key):
            return True
    interpretation = box.get("money_interpretation") if isinstance(box.get("money_interpretation"), dict) else {}
    return bool(interpretation.get("total_amounts_eur") or interpretation.get("component_amounts_eur"))


def _fallback_money_box_non_additive(fact_pack: Dict[str, Any]) -> bool:
    box = fact_pack.get("money_box") if isinstance(fact_pack.get("money_box"), dict) else {}
    interpretation = box.get("money_interpretation") if isinstance(box.get("money_interpretation"), dict) else {}
    return bool(interpretation.get("has_non_additive_items") or interpretation.get("has_components_of_total"))


def _fallback_has_valuation_deductions(fact_pack: Dict[str, Any]) -> bool:
    box = fact_pack.get("money_box") if isinstance(fact_pack.get("money_box"), dict) else {}
    return isinstance(box.get("valuation_deductions"), list) and bool(box.get("valuation_deductions"))


def _fallback_field_topics(fact_pack: Dict[str, Any]) -> List[str]:
    states = fact_pack.get("field_states") if isinstance(fact_pack.get("field_states"), dict) else {}
    topics: List[str] = []
    urban = states.get("regolarita_urbanistica")
    if isinstance(urban, dict) and urban.get("value"):
        topics.append("regolarità urbanistica")
    agibilita = states.get("agibilita")
    if isinstance(agibilita, dict) and agibilita.get("value"):
        topics.append("agibilità")
    occupancy = states.get("stato_occupativo")
    if isinstance(occupancy, dict) and occupancy.get("value"):
        if occupancy.get("evidence_supports_claim") is False:
            topics.append("stato occupativo nella sezione stato di possesso")
        else:
            topics.append("stato occupativo e titolo di occupazione")
    opponibilita = states.get("opponibilita_occupazione")
    if isinstance(opponibilita, dict) and opponibilita.get("value"):
        topics.append("opponibilità dell'occupazione")
    return topics


def build_deterministic_separated_fallback_payload(
    result: Dict[str, Any],
    narrator_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build safe separated fallback copy from the final cleaned customer contract."""
    fact_pack = build_clean_customer_decision_fact_pack(result)
    issue_headlines = _fallback_issue_headlines(fact_pack)
    has_money_signals = _fallback_money_box_has_signals(fact_pack)
    non_additive_money = _fallback_money_box_non_additive(fact_pack)
    has_valuation_deductions = _fallback_has_valuation_deductions(fact_pack)
    is_multi_lot = bool(fact_pack.get("is_multi_lot")) or int(fact_pack.get("lots_count") or 0) > 1

    summary_parts = [_fallback_lot_phrase(fact_pack)]
    if issue_headlines:
        summary_parts.append("Rischi documentati: " + "; ".join(issue_headlines[:3]) + ".")
    else:
        summary_parts.append("I campi puliti non isolano un rischio principale determinabile automaticamente.")
    if has_money_signals:
        if non_additive_money:
            summary_parts.append("Il Money Box contiene segnali economici con eventuali componenti interne non additive.")
        else:
            summary_parts.append("Il Money Box contiene segnali economici da verificare.")
    if has_valuation_deductions:
        summary_parts.append("Sono presenti componenti valutative o deprezzamenti separati dai costi da verificare.")
    factual_summary = _fallback_compact_sentence(" ".join(summary_parts), 650)

    topics = _fallback_field_topics(fact_pack)
    if has_money_signals:
        topics.append("eventuale incidenza economica degli importi segnalati")
    if has_valuation_deductions:
        topics.append("deprezzamenti e valori estimativi")
    if not topics:
        topics.append("coerenza dei dati essenziali della perizia")
    topic_text = ", ".join(topics[:5])
    decision_prefix = "Prima dell'offerta procedi lotto per lotto" if is_multi_lot else "Prima dell'offerta procedi con verifiche mirate"
    decisione = (
        f"{decision_prefix} su {topic_text}. "
        "Chiudi i punti tecnici e documentali prima di fissare il rilancio, separando valori di stima, deprezzamenti e formalità dai possibili segnali economici da verificare."
    )
    decisione = _fallback_compact_sentence(decisione, 760)
    if _near_identical_text(factual_summary, decisione):
        decisione = _fallback_compact_sentence(
            "Prima dell'offerta usa la perizia come lista di controlli: chiarisci i punti critici indicati nei campi puliti e definisci il margine economico solo dopo verifica tecnica e documentale.",
            760,
        )

    before_offer: List[str] = []
    if is_multi_lot:
        before_offer.append("Leggere dati, rischi e valori separatamente per ciascun lotto.")
    for topic in topics:
        if "urbanistica" in topic:
            before_offer.append("Verificare regolarità urbanistica, sanabilità e documenti tecnici richiamati.")
        elif "agibil" in topic:
            before_offer.append("Chiarire agibilità o abitabilità con tecnico e documentazione disponibile.")
        elif "occup" in topic or "possesso" in topic:
            before_offer.append("Verificare stato di possesso, titolo di occupazione e tempi pratici di disponibilità.")
        elif "incidenza economica" in topic:
            if non_additive_money:
                before_offer.append("Leggere il Money Box distinguendo totale stimato e componenti interne, senza sommarle due volte.")
            else:
                before_offer.append("Verificare se e in quale misura gli importi segnalati possano incidere sull'offerta.")
        elif "deprezzamenti" in topic or "estimativi" in topic:
            before_offer.append("Separare valori di stima e deprezzamenti dai segnali economici da verificare.")
    if issue_headlines:
        before_offer.append("Confrontare i rischi documentati con tecnico, delegato o professionista prima del rilancio.")
    before_offer.append("Definire il margine d'offerta solo dopo aver chiuso le verifiche specifiche emerse.")
    deduped_checks: List[str] = []
    for item in before_offer:
        clean = _fallback_compact_sentence(item, 260)
        if clean and clean.lower() not in {x.lower() for x in deduped_checks}:
            deduped_checks.append(clean)
        if len(deduped_checks) >= 5:
            break
    while len(deduped_checks) < 3:
        fallback_check = "Verificare la coerenza tra dati principali, allegati e avviso di vendita prima dell'offerta."
        if fallback_check.lower() not in {x.lower() for x in deduped_checks}:
            deduped_checks.append(fallback_check)
        else:
            deduped_checks.append("Conservare margine prudenziale finché i punti non documentati restano aperti.")

    main_risk = issue_headlines[0] if issue_headlines else "Rischio principale non determinabile automaticamente"
    why = (
        "Il punto incide su tempi, margine di offerta e verifiche tecniche o documentali necessarie prima del rilancio."
        if issue_headlines
        else "L'assenza di un rischio principale automatico richiede comunque verifica dei campi non chiusi prima dell'offerta."
    )
    if has_money_signals and non_additive_money:
        not_to_confuse = (
            "Totali e componenti interne del Money Box non vanno sommati due volte; valori di stima, deprezzamenti e formalità non sono costi automatici."
        )
    elif has_money_signals:
        not_to_confuse = (
            "Gli importi del Money Box sono segnali da verificare per eventuale incidenza economica; valori di stima, deprezzamenti e formalità restano categorie distinte."
        )
    else:
        not_to_confuse = (
            "Valori di stima, deprezzamenti e formalità non vanno trasformati in costi automatici senza una classificazione strutturata."
        )

    return {
        "summary_it": factual_summary,
        "decisione_rapida_it": decisione,
        "main_risk_it": _fallback_compact_sentence(main_risk, 420),
        "why_it_matters_it": _fallback_compact_sentence(why, 520),
        "before_offer_it": deduped_checks[:5],
        "not_to_confuse_it": _fallback_compact_sentence(not_to_confuse, 520),
        "generation_mode": "deterministic_separated_fallback",
        "provider": "deterministic",
        "model": None,
    }


def _normalize_gemini_payload(payload: Dict[str, Any], *, provider: str, model: str) -> Dict[str, Any]:
    before_offer = payload.get("before_offer_it") if isinstance(payload.get("before_offer_it"), list) else []
    normalized = {
        "summary_it": _safe_text(payload.get("summary_it"), 900),
        "decisione_rapida_it": _safe_text(payload.get("decisione_rapida_it"), 1100),
        "main_risk_it": _safe_text(payload.get("main_risk_it"), 420),
        "why_it_matters_it": _safe_text(payload.get("why_it_matters_it"), 520),
        "before_offer_it": [_safe_text(item, 260) for item in before_offer if _safe_text(item, 260)][:5],
        "not_to_confuse_it": _safe_text(payload.get("not_to_confuse_it"), 520),
        "generation_mode": "gemini_clean_contract",
        "provider": provider,
        "model": model,
    }
    return normalized


def apply_narrated_payload_to_result(
    result: Dict[str, Any],
    payload: Dict[str, Any],
    narrator_meta: Optional[Dict[str, Any]] = None,
) -> None:
    if not isinstance(result, dict) or not isinstance(payload, dict):
        return
    summary_it = _safe_text(payload.get("summary_it"), 1500)
    decisione_it = _safe_text(payload.get("decisione_rapida_it"), 1500)
    if not summary_it or not decisione_it:
        return
    generation_mode = _safe_text(payload.get("generation_mode") or "gemini_clean_contract", 120)

    summary_for_client = result.get("summary_for_client") if isinstance(result.get("summary_for_client"), dict) else {}
    summary_for_client["summary_it"] = summary_it
    summary_for_client["generation_mode"] = generation_mode
    summary_for_client.setdefault("disclaimer_it", "Documento informativo. Non costituisce consulenza legale. Consultare un professionista qualificato.")
    summary_for_client.setdefault("disclaimer_en", "Informational document. Not legal advice. Consult a qualified professional.")
    result["summary_for_client"] = summary_for_client

    decision_client = result.get("decision_rapida_client") if isinstance(result.get("decision_rapida_client"), dict) else {}
    decision_client["decisione_rapida_it"] = decisione_it
    decision_client["summary_it"] = decisione_it
    decision_client["main_risk_it"] = _safe_text(payload.get("main_risk_it"), 700)
    decision_client["why_it_matters_it"] = _safe_text(payload.get("why_it_matters_it"), 700)
    decision_client["before_offer_it"] = list(payload.get("before_offer_it") or [])
    decision_client["not_to_confuse_it"] = _safe_text(payload.get("not_to_confuse_it"), 700)
    decision_client["generation_mode"] = generation_mode
    result["decision_rapida_client"] = decision_client

    section2 = result.get("section_2_decisione_rapida") if isinstance(result.get("section_2_decisione_rapida"), dict) else {}
    section2["summary_it"] = decisione_it
    section2["decisione_rapida_it"] = decisione_it
    section2["main_risk_it"] = _safe_text(payload.get("main_risk_it"), 700)
    section2["why_it_matters_it"] = _safe_text(payload.get("why_it_matters_it"), 700)
    section2["before_offer_it"] = list(payload.get("before_offer_it") or [])
    section2["not_to_confuse_it"] = _safe_text(payload.get("not_to_confuse_it"), 700)
    section2["generation_mode"] = generation_mode
    result["section_2_decisione_rapida"] = section2

    bundle = result.get("summary_for_client_bundle") if isinstance(result.get("summary_for_client_bundle"), dict) else {}
    bundle["factual_summary_it"] = summary_it
    bundle["decision_summary_it"] = decisione_it
    bundle["main_risk_it"] = _safe_text(payload.get("main_risk_it"), 700)
    bundle["why_it_matters_it"] = _safe_text(payload.get("why_it_matters_it"), 700)
    bundle["before_offer_it"] = list(payload.get("before_offer_it") or [])
    bundle["not_to_confuse_it"] = _safe_text(payload.get("not_to_confuse_it"), 700)
    bundle["generation_mode"] = generation_mode
    result["summary_for_client_bundle"] = bundle

    result["decision_rapida_narrated"] = dict(payload)
    if narrator_meta is not None:
        result["narrator_meta"] = dict(narrator_meta)

    cdc = result.get("customer_decision_contract") if isinstance(result.get("customer_decision_contract"), dict) else {}
    if cdc:
        cdc["summary_for_client"] = json.loads(json.dumps(result["summary_for_client"], ensure_ascii=False))
        cdc["decision_rapida_client"] = json.loads(json.dumps(result["decision_rapida_client"], ensure_ascii=False))
        cdc["section_2_decisione_rapida"] = json.loads(json.dumps(result["section_2_decisione_rapida"], ensure_ascii=False))
        cdc["summary_for_client_bundle"] = json.loads(json.dumps(result["summary_for_client_bundle"], ensure_ascii=False))
        cdc["decision_rapida_narrated"] = json.loads(json.dumps(result["decision_rapida_narrated"], ensure_ascii=False))
        if narrator_meta is not None:
            cdc["narrator_meta"] = json.loads(json.dumps(narrator_meta, ensure_ascii=False))
        result["customer_decision_contract"] = cdc

    scrub_customer_facing_stale_money_labels(result)


def _scrub_stale_money_text(value: str) -> str:
    return _STALE_REGOLARIZZAZIONE_MONEY_RE.sub(
        "Regolarizzazione urbanistica: importo da verificare",
        str(value or ""),
    )


def _scrub_customer_facing_value(value: Any, *, parent_key: str = "") -> Tuple[Any, int]:
    if isinstance(value, dict):
        changed = 0
        out: Dict[str, Any] = {}
        for key, child in value.items():
            if str(key) in _SOURCE_TEXT_KEYS:
                out[key] = child
                continue
            scrubbed, child_changed = _scrub_customer_facing_value(child, parent_key=str(key))
            out[key] = scrubbed
            changed += child_changed
        return out, changed
    if isinstance(value, list):
        changed = 0
        out_list = []
        for child in value:
            scrubbed, child_changed = _scrub_customer_facing_value(child, parent_key=parent_key)
            out_list.append(scrubbed)
            changed += child_changed
        return out_list, changed
    if isinstance(value, str):
        scrubbed_text = _scrub_stale_money_text(value)
        return scrubbed_text, int(scrubbed_text != value)
    return value, 0


def scrub_customer_facing_stale_money_labels(result: Dict[str, Any]) -> List[str]:
    """Remove stale identifier-as-money regolarizzazione labels from customer-facing projections."""
    if not isinstance(result, dict):
        return []
    changed_paths: List[str] = []
    for key in _CUSTOMER_FACING_SCRUB_KEYS:
        if key not in result:
            continue
        scrubbed, changed = _scrub_customer_facing_value(result.get(key), parent_key=key)
        if changed:
            result[key] = scrubbed
            changed_paths.append(key)
    cdc = result.get("customer_decision_contract") if isinstance(result.get("customer_decision_contract"), dict) else {}
    if cdc:
        for key in _CUSTOMER_FACING_SCRUB_KEYS:
            if key not in cdc:
                continue
            scrubbed, changed = _scrub_customer_facing_value(cdc.get(key), parent_key=key)
            if changed:
                cdc[key] = scrubbed
                changed_paths.append(f"customer_decision_contract.{key}")
        result["customer_decision_contract"] = cdc
    return changed_paths


def _iter_customer_facing_strings(value: Any, *, path: str = "") -> Iterable[Tuple[str, str]]:
    if isinstance(value, dict):
        for key, child in value.items():
            if str(key) in _SOURCE_TEXT_KEYS:
                continue
            child_path = f"{path}.{key}" if path else str(key)
            yield from _iter_customer_facing_strings(child, path=child_path)
    elif isinstance(value, list):
        for idx, child in enumerate(value):
            yield from _iter_customer_facing_strings(child, path=f"{path}[{idx}]")
    elif isinstance(value, str):
        yield path, value


def scan_customer_facing_narrator_issues(result: Dict[str, Any]) -> List[str]:
    """Debug/test helper for stale repaired money labels and unsafe narrator phrases."""
    if not isinstance(result, dict):
        return []
    issues: List[str] = []
    roots: List[Tuple[str, Any]] = [(key, result.get(key)) for key in _CUSTOMER_FACING_SCRUB_KEYS if key in result]
    cdc = result.get("customer_decision_contract") if isinstance(result.get("customer_decision_contract"), dict) else {}
    roots.extend(
        (f"customer_decision_contract.{key}", cdc.get(key))
        for key in _CUSTOMER_FACING_SCRUB_KEYS
        if key in cdc
    )
    for root_path, value in roots:
        for path, text in _iter_customer_facing_strings(value, path=root_path):
            if _STALE_REGOLARIZZAZIONE_MONEY_RE.search(text):
                issues.append(f"{path}:stale_regolarizzazione_money_label")
            if _UNSAFE_CUSTOMER_FACING_PHRASE_RE.search(text):
                issues.append(f"{path}:unsafe_buyer_obligation_phrase")
    return issues


async def _call_narrator_llm(
    *,
    api_key: str,
    model: str,
    prompt: str,
    timeout_seconds: float = 12.0,
) -> str:
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "temperature": 0,
        "messages": [
            {
                "role": "system",
                "content": "Return ONLY strict JSON with keys: it,en,bullets_it,bullets_en,evidence_refs.",
            },
            {"role": "user", "content": prompt},
        ],
    }
    async with httpx.AsyncClient(timeout=timeout_seconds) as client:
        resp = await client.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
    if resp.status_code >= 400:
        raise RuntimeError(f"openai_http_{resp.status_code}")
    body = resp.json()
    content = (((body.get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip()
    if not content:
        raise RuntimeError("openai_empty_content")
    return content


async def build_decisione_rapida_narration(
    *,
    result: Dict[str, Any],
    request_id: str,
    enabled: bool,
    model: Optional[str],
    api_key: Optional[str],
    provider: Optional[str] = None,
    timeout_seconds: Optional[float] = None,
) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    provider_name = str(provider or os.environ.get("DECISIONE_RAPIDA_PROVIDER") or "").strip().lower()
    if not provider_name:
        provider_name = "openai" if api_key else "disabled"
    meta = {
        "enabled": bool(enabled),
        "status": "SKIPPED",
        "provider": provider_name,
        "model": model if model else None,
        "errors": [],
        "error": None,
    }
    if not enabled:
        return None, meta

    if provider_name in {"gemini", "google", "google_gemini"}:
        gemini_model = str(model or os.environ.get("GEMINI_DECISION_MODEL") or "gemini-2.5-flash").strip()
        gemini_key = str(api_key or os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY") or "").strip()
        gemini_timeout = timeout_seconds
        if gemini_timeout is None:
            try:
                gemini_timeout = float(os.environ.get("DECISIONE_RAPIDA_TIMEOUT_SECONDS", "45") or 45)
            except Exception:
                gemini_timeout = 45.0
        meta["provider"] = "gemini"
        meta["model"] = gemini_model
        if not gemini_key:
            meta["status"] = "ERROR"
            meta["error"] = "GEMINI_API_KEY missing"
            meta["errors"].append("GEMINI_API_KEY missing")
            return None, meta
        fact_pack = build_clean_customer_decision_fact_pack(result)
        prompt = _build_gemini_prompt(fact_pack, request_id)
        try:
            raw = await _call_gemini_narrator_llm(
                api_key=gemini_key,
                model=gemini_model,
                prompt=prompt,
                timeout_seconds=float(gemini_timeout),
            )
            parsed = _extract_json_payload(raw)
        except httpx.TimeoutException as e:
            meta["status"] = "TIMEOUT"
            meta["error"] = f"timeout:{str(e)[:120]}"
            meta["errors"].append(meta["error"])
            return None, meta
        except Exception as e:
            meta["status"] = "ERROR"
            meta["error"] = f"gemini_error:{str(e)[:160]}"
            meta["errors"].append(meta["error"])
            return None, meta

        validation_errors = validate_gemini_decision_payload(parsed, fact_pack)
        if validation_errors:
            meta["status"] = "REJECTED_VALIDATION"
            meta["error"] = validation_errors[0]
            meta["errors"].extend(validation_errors[:8])
            return None, meta

        narrated = _normalize_gemini_payload(parsed, provider="gemini", model=gemini_model)
        meta["status"] = "OK"
        return narrated, meta

    if provider_name in {"disabled", "none", "off", "0", "false"}:
        return None, meta

    meta["provider"] = "openai"
    if not api_key:
        meta["status"] = "ERROR"
        meta["error"] = "OPENAI_API_KEY missing"
        meta["errors"].append("OPENAI_API_KEY missing")
        return None, meta
    if not model:
        meta["status"] = "ERROR"
        meta["error"] = "NARRATOR_MODEL missing"
        meta["errors"].append("NARRATOR_MODEL missing")
        return None, meta

    fact_pack = build_fact_pack(result)
    semaforo = fact_pack.get("semaforo_generale", {}) if isinstance(fact_pack.get("semaforo_generale"), dict) else {}
    semaforo_status = str(semaforo.get("status") or "").strip()
    top_blockers = semaforo.get("top_blockers", []) if isinstance(semaforo.get("top_blockers"), list) else []
    prompt = (
        "Write Decisione Rapida narration in Italian and English.\n"
        "Rules:\n"
        "- Return STRICT JSON only with keys it,en,bullets_it,bullets_en,evidence_refs.\n"
        "- No invented numbers/dates/times.\n"
        "- IT text must mention semaforo status and top 2 blockers by name.\n"
        "- bullets_it and bullets_en must be arrays of short actionable items.\n"
        "- Never use source wording 'dall'estratto' / 'dallo estratto'. Use 'dal documento analizzato' or 'dalla perizia'.\n"
        f"- request_id: {request_id}\n"
        "FACT_PACK_JSON:\n"
        f"{json.dumps(fact_pack, ensure_ascii=False)}"
    )
    doc_quality = fact_pack.get("document_quality", {}) if isinstance(fact_pack.get("document_quality"), dict) else {}
    if str(doc_quality.get("status") or "").upper() == "TEXT_OK":
        prompt = (
            prompt
            + "\nADDITIONAL TEXT_OK CONSTRAINTS:\n"
            + "- Do NOT say 'analisi automatica è parziale', 'documento non leggibile', or 'OCR necessario'.\n"
            + "- Prefer wording like 'alcuni dati richiedono verifica manuale' and 'verifiche consigliate'.\n"
        )
    try:
        raw = await _call_narrator_llm(api_key=api_key, model=model, prompt=prompt, timeout_seconds=float(timeout_seconds or 12.0))
        parsed = _extract_json_payload(raw)
    except Exception as e:
        meta["status"] = "FALLBACK"
        meta["error"] = f"llm_error:{str(e)[:120]}"
        meta["errors"].append(meta["error"])
        return None, meta

    # Deterministic guardrail: ensure IT mentions semaforo status + top blockers.
    it_text = str(parsed.get("it") or "").strip()
    lower_it = it_text.lower()
    additions: List[str] = []
    if semaforo_status and semaforo_status.lower() not in lower_it:
        additions.append(f"Semaforo {semaforo_status}.")
    missing_blockers = [str(b).strip() for b in top_blockers[:2] if str(b).strip() and str(b).strip().lower() not in lower_it]
    if missing_blockers:
        additions.append("Blocchi principali: " + "; ".join(missing_blockers) + ".")
    if additions:
        parsed["it"] = (" ".join(additions) + " " + it_text).strip()

    errors = _validate_narrated_payload(parsed, fact_pack, semaforo_status, [str(x) for x in top_blockers])
    if errors:
        meta["status"] = "FALLBACK"
        meta["error"] = errors[0]
        meta["errors"].extend(errors[:6])
        return None, meta

    refs: List[str] = []
    for ref in parsed.get("evidence_refs", []):
        ref_text = str(ref or "").strip()
        if ref_text and ref_text not in refs:
            refs.append(ref_text)
    if not refs:
        meta["status"] = "FALLBACK"
        meta["error"] = "invalid:evidence_refs_empty"
        meta["errors"].append(meta["error"])
        return None, meta

    narrated = {
        "it": _safe_text(parsed.get("it"), 1200),
        "en": _safe_text(parsed.get("en"), 1200),
        "bullets_it": [str(x).strip()[:220] for x in parsed.get("bullets_it", []) if str(x).strip()][:8],
        "bullets_en": [str(x).strip()[:220] for x in parsed.get("bullets_en", []) if str(x).strip()][:8],
        "evidence_refs": refs[:12],
    }
    if not narrated["bullets_it"] or not narrated["bullets_en"]:
        meta["status"] = "FALLBACK"
        meta["error"] = "invalid:empty_bullets"
        meta["errors"].append(meta["error"])
        return None, meta

    meta["status"] = "OK"
    return narrated, meta
