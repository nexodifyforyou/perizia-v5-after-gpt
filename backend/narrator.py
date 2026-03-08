import hashlib
import json
import re
from typing import Any, Dict, List, Optional, Tuple

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
) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    meta = {
        "enabled": bool(enabled),
        "status": "SKIPPED",
        "model": model if model else None,
        "errors": [],
    }
    if not enabled:
        return None, meta
    if not api_key:
        meta["errors"].append("OPENAI_API_KEY missing")
        return None, meta
    if not model:
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
        raw = await _call_narrator_llm(api_key=api_key, model=model, prompt=prompt, timeout_seconds=12.0)
        parsed = _extract_json_payload(raw)
    except Exception as e:
        meta["status"] = "FALLBACK"
        meta["errors"].append(f"llm_error:{str(e)[:120]}")
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
        meta["errors"].extend(errors[:6])
        return None, meta

    refs: List[str] = []
    for ref in parsed.get("evidence_refs", []):
        ref_text = str(ref or "").strip()
        if ref_text and ref_text not in refs:
            refs.append(ref_text)
    if not refs:
        meta["status"] = "FALLBACK"
        meta["errors"].append("invalid:evidence_refs_empty")
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
        meta["errors"].append("invalid:empty_bullets")
        return None, meta

    meta["status"] = "OK"
    return narrated, meta
