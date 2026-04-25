from __future__ import annotations

import copy
import hashlib
import os
import re
from typing import Any, Dict, List, Optional

from perizia_canonical_pipeline.llm_resolution_pack import (
    LLMResolutionUnavailable,
    discover_openai_config,
    resolve_single_issue,
)


_FIELD_FAMILY = {
    "agibilita": "agibilita",
    "conformita_catastale": "catastale",
    "regolarita_urbanistica": "urbanistica",
    "stato_occupativo": "occupancy",
    "opponibilita_occupazione": "occupancy",
    "spese_condominiali_arretrate": "costs",
    "prezzo_base_asta": "valuation",
    "valore_stima": "valuation",
    "diritto_reale": "rights",
    "quota": "rights",
    "impianto_riscaldamento_status": "impianti",
}

_FIELD_HEADLINE = {
    "agibilita": "Agibilità",
    "conformita_catastale": "Conformità catastale",
    "regolarita_urbanistica": "Regolarità urbanistica",
    "stato_occupativo": "Stato occupativo",
    "opponibilita_occupazione": "Opponibilità occupazione",
    "spese_condominiali_arretrate": "Spese condominiali arretrate",
    "prezzo_base_asta": "Prezzo base asta",
    "valore_stima": "Valore di stima",
    "diritto_reale": "Diritto reale",
    "quota": "Quota",
    "impianto_riscaldamento_status": "Impianto di riscaldamento",
}

_NEGATIVE_VALUE_HINTS = {
    "agibilita": {"ASSENTE", "DA VERIFICARE"},
    "conformita_catastale": {"DA VERIFICARE"},
    "regolarita_urbanistica": {"PRESENTI DIFFORMITA", "DA VERIFICARE"},
    "stato_occupativo": {"OCCUPATO", "OCCUPATO DAL DEBITORE", "OCCUPATO DA TERZI SENZA TITOLO"},
    "opponibilita_occupazione": {"OPPONIBILE", "NON VERIFICABILE"},
}

_EN_TO_IT = (
    (r"\bverify certificate of agibilit[aà]\b", "Verificare il certificato di agibilità"),
    (r"\bverify\b", "Verificare"),
    (r"\bcertificate\b", "certificato"),
    (r"\battachments\b", "allegati"),
    (r"\bmanual review\b", "verifica manuale"),
    (r"\bno decisive\b", "manca un riscontro decisivo"),
    (r"\bnot found\b", "non trovato"),
    (r"\bunresolved\b", "non risolto"),
    (r"\bconflict\b", "conflitto"),
    (r"\bsame-scope\b", "stesso ambito"),
    (r"\bmixed-scope\b", "ambiti diversi"),
)

_BACKEND_JARGON_PHRASES = (
    "Per scope document",
    "problema contesto raggruppato",
    "valori concorrenti",
    "pacchetto bounded",
    "scope document",
    "contesto raggruppato",
)

_NON_CUSTOMER_PHRASES = (
    "truth differs by scope",
    "scope",
    "bounded",
    "freeze-safe",
    "human review",
    "same-scope",
    "mixed-scope",
)

_FAMILY_INCLUDE_TERMS = {
    "agibilita": ("agibil", "abitabil", "certificat", "licenza"),
    "catastale": ("catast", "planimetr", "subaltern", "foglio", "particella"),
    "urbanistica": ("urban", "difform", "sanator", "condon", "permess", "ediliz", "abuso", "regolarizz"),
    "occupancy": ("occup", "opponib", "liber", "locat", "comod", "debitor", "terzi", "contratto", "rilasc", "custode", "senza titolo"),
    "costs": ("spes", "cost", "oner", "condomin", "regolarizz", "sanator", "riprist", "demol", "smalt", "bonific", "lavor", "complet", "arretrat", "tecnic", "tribut", "impost", "tassa"),
    "valuation": ("prezzo base", "valore di stima", "valore", "stima", "euro", "€", "deprezz", "riduz", "abbatt"),
    "rights": ("propriet", "quota", "usufrutto", "nuda propriet", "diritto reale"),
    "impianti": ("impiant", "riscald", "elettric", "idric", "termic", "conformità", "conformita", "dichiarazione"),
}

_FAMILY_EXCLUDE_TERMS = {
    "occupancy": ("piazza", "corso", "stazione", "negozi", "boutique", "istituti bancari", "alberghi", "urbanizzata", "viabilità", "viabilita", "casello", "zona prossima"),
}


def _normalize_evidence_item(item: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None
    try:
        page = int(item.get("page"))
    except Exception:
        return None
    quote = str(item.get("quote") or "").strip()
    if not quote:
        return None
    out = {
        "page": page,
        "quote": quote[:520],
        "start_offset": int(item.get("start_offset") or 0),
        "end_offset": int(item.get("end_offset") or 0),
    }
    if out["end_offset"] < out["start_offset"]:
        out["end_offset"] = out["start_offset"]
    return out


def _normalize_evidence_list(*sources: Any, limit: int = 4) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen = set()
    for source in sources:
        if not isinstance(source, list):
            continue
        for item in source:
            normalized = _normalize_evidence_item(item)
            if not normalized:
                continue
            sig = (normalized["page"], normalized["quote"])
            if sig in seen:
                continue
            seen.add(sig)
            out.append(normalized)
            if len(out) >= limit:
                return out
    return out


def _pages_from_evidence(evidence: List[Dict[str, Any]]) -> List[int]:
    pages: List[int] = []
    for item in evidence:
        page = item.get("page")
        if isinstance(page, int) and page not in pages:
            pages.append(page)
    return pages


def _normalize_page_list(value: Any) -> List[int]:
    pages: List[int] = []
    if not isinstance(value, list):
        return pages
    for item in value:
        try:
            page = int(item)
        except Exception:
            continue
        if page not in pages:
            pages.append(page)
    pages.sort()
    return pages


def _explanation_mode(supporting_pages: List[int], tension_pages: List[int], blocked: bool) -> str:
    if blocked:
        return "blocked"
    if tension_pages:
        return "conflict_explained"
    if len(supporting_pages) > 1:
        return "multi_source"
    return "single_source"


def _clean_it_text(text: Any) -> str:
    value = str(text or "").strip()
    if not value:
        return ""
    cleaned = value.replace("_", " ").strip()
    for pattern, repl in _EN_TO_IT:
        cleaned = re.sub(pattern, repl, cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if cleaned and cleaned[-1] not in ".!?":
        cleaned += "."
    return cleaned


def _contains_backend_jargon(text: Any) -> bool:
    cleaned = _clean_it_text(text).lower()
    if not cleaned:
        return False
    return any(phrase.lower() in cleaned for phrase in _BACKEND_JARGON_PHRASES)


def _contains_non_customer_language(text: Any) -> bool:
    cleaned = _clean_it_text(text).lower()
    if not cleaned:
        return False
    return any(phrase in cleaned for phrase in _NON_CUSTOMER_PHRASES)


def _quote_is_relevant_to_family(quote: str, field_family: str, field_type: str) -> bool:
    text = _clean_it_text(quote).lower()
    if not text:
        return False
    include_terms = list(_FAMILY_INCLUDE_TERMS.get(field_family, ()))
    include_terms.extend(_FAMILY_INCLUDE_TERMS.get(field_type, ()))
    exclude_terms = _FAMILY_EXCLUDE_TERMS.get(field_family, ())
    has_include = any(term in text for term in include_terms) if include_terms else True
    has_exclude = any(term in text for term in exclude_terms)
    if field_family == "valuation":
        has_amount = "€" in text or bool(re.search(r"\b\d[\d\.,]*\b", text))
        return has_include and has_amount
    if field_family == "occupancy":
        return has_include and not has_exclude
    if field_family == "costs":
        return has_include
    return has_include and not (has_exclude and not has_include)


def _family_specific_evidence(obj: Dict[str, Any], field_family: str, field_type: str) -> List[Dict[str, Any]]:
    evidence = _normalize_evidence_list(obj.get("evidence"))
    filtered = [item for item in evidence if _quote_is_relevant_to_family(str(item.get("quote") or ""), field_family, field_type)]
    if filtered:
        return filtered
    if field_family in {"occupancy", "valuation", "costs"}:
        return []
    return evidence


def _field_label_it(field_type: str) -> str:
    aliases = {
        "valuation": "Valore di stima",
        "occupancy": "Stato occupativo",
        "costs": "Costi a carico dell'acquirente",
        "rights": "Diritti reali",
        "catastale": "Conformità catastale",
        "urbanistica": "Regolarità urbanistica",
        "agibilita": "Agibilità",
        "impianti": "Impianti",
    }
    return _FIELD_HEADLINE.get(field_type, aliases.get(field_type, field_type.replace("_", " ")))


def _best_explanation_quote(obj: Dict[str, Any], field_family: str, field_type: str) -> str:
    evidence = _family_specific_evidence(obj, field_family, field_type)
    if not evidence:
        return ""
    quote = _clean_it_text(evidence[0].get("quote"))
    return quote[:220]


def _default_verify_next_it(field_family: str, field_type: str) -> str:
    if field_family == "agibilita":
        return "Verificare certificato di agibilità/abitabilità e titoli edilizi richiamati in perizia."
    if field_family == "catastale":
        return "Verificare visura, planimetria e coerenza tra stato di fatto e dati catastali."
    if field_family == "urbanistica":
        return "Verificare titoli edilizi, eventuali sanatorie e stato delle difformità indicate in perizia."
    if field_family == "occupancy" and field_type == "opponibilita_occupazione":
        return "Verificare titolo di occupazione, data del contratto, registrazione e opponibilità verso la procedura."
    if field_family == "occupancy":
        return "Verificare stato di occupazione, titolo del detentore e tempi di liberazione presso perizia, allegati e custode."
    if field_family == "valuation" and field_type == "prezzo_base_asta":
        return "Verificare nell'avviso di vendita o nel prospetto finale il prezzo base d'asta del lotto."
    if field_family == "valuation":
        return "Verificare nel capitolo di stima e nel riepilogo finale il valore di stima conclusivo del lotto o del bene."
    if field_family == "costs":
        return "Verificare oneri, spese condominiali, sanatorie e preventivi richiamati in perizia per quantificare il costo a carico dell'acquirente."
    if field_family == "rights":
        return "Verificare titolo di provenienza, quota e diritti reali richiamati in perizia."
    if field_family == "impianti":
        return "Verificare dichiarazioni di conformità, libretti e stato reale dell'impianto richiamati in perizia."
    return f"Verificare i documenti richiamati in perizia per chiudere il punto su {_field_label_it(field_type).lower()}."


def _ensure_verify_next_it(obj: Dict[str, Any], field_family: str, field_type: str, verify_next_it: str) -> str:
    cleaned = _clean_it_text(verify_next_it)
    if cleaned and not _contains_backend_jargon(cleaned) and re.search(
        r"\b(verificare|controllare|acquisire|richiedere|consultare|recuperare|esaminare)\b",
        cleaned,
        flags=re.IGNORECASE,
    ):
        return cleaned
    return _default_verify_next_it(field_family, field_type)


def _ensure_why_not_resolved(obj: Dict[str, Any], field_family: str, field_type: str, why_not_resolved: str) -> str:
    cleaned = _clean_it_text(why_not_resolved)
    if cleaned and not _contains_backend_jargon(cleaned) and not _contains_non_customer_language(cleaned):
        return cleaned
    if str(obj.get("tension_pages") or "").strip("[]"):
        return f"Le frasi raccolte sulla {_field_label_it(field_type).lower()} non coincidono tra loro e non permettono una chiusura sicura."
    return f"La frase trovata sulla {_field_label_it(field_type).lower()} non basta, da sola, per chiudere il punto in modo difendibile."


def _build_customer_grade_unresolved_explanation(
    obj: Dict[str, Any],
    *,
    field_family: str,
    field_type: str,
    verify_next_it: str,
    why_not_resolved: str,
) -> str:
    quote = _best_explanation_quote(obj, field_family, field_type)
    field_label = _field_label_it(field_type).lower()
    if quote:
        return (
            f"La perizia, sul tema {field_label}, riporta: “{quote}”. "
            f"Questo però non basta perché {why_not_resolved.rstrip('.').lower()}. "
            f"Verifica successiva: {verify_next_it}"
        )
    return (
        f"La perizia contiene un riferimento su {field_label}, ma non basta per chiudere il punto in modo sicuro. "
        f"Il motivo è questo: {why_not_resolved} "
        f"Verifica successiva: {verify_next_it}"
    )


def _explanation_is_too_thin(text: Any) -> bool:
    cleaned = _clean_it_text(text)
    if not cleaned:
        return True
    words = re.findall(r"\w+", cleaned, flags=re.UNICODE)
    return len(words) < 12


def _text_is_relevant_to_family(text: Any, field_family: str, field_type: str) -> bool:
    cleaned = _clean_it_text(text).lower()
    if not cleaned:
        return False
    include_terms = list(_FAMILY_INCLUDE_TERMS.get(field_family, ()))
    include_terms.extend(_FAMILY_INCLUDE_TERMS.get(field_type, ()))
    exclude_terms = _FAMILY_EXCLUDE_TERMS.get(field_family, ())
    has_include = any(term in cleaned for term in include_terms) if include_terms else True
    has_exclude = any(term in cleaned for term in exclude_terms)
    if field_family == "occupancy":
        return has_include and not has_exclude
    if field_family == "costs":
        return has_include
    return has_include and not (has_exclude and not has_include)


def _existing_llm_copy_needs_regeneration(obj: Dict[str, Any], field_family: str, field_type: str) -> bool:
    explanation_it = _clean_it_text(obj.get("explanation_it"))
    verify_next_it = _clean_it_text(obj.get("verify_next_it"))
    contract_state = str(obj.get("contract_state") or "").strip()

    if _contains_backend_jargon(explanation_it) or _contains_non_customer_language(explanation_it):
        return True
    if _explanation_is_too_thin(explanation_it):
        return True
    if _is_explanatory_contract_state(contract_state) and not verify_next_it:
        return True
    if explanation_it and not _text_is_relevant_to_family(explanation_it, field_family, field_type):
        return True
    return False


def _sanitize_explanatory_fields(
    obj: Dict[str, Any],
    *,
    field_family: str,
    field_type: str,
) -> None:
    contract_state = str(obj.get("contract_state") or "").strip()
    verify_next_it = _ensure_verify_next_it(obj, field_family, field_type, str(obj.get("verify_next_it") or ""))
    why_not_resolved = _clean_it_text(obj.get("why_not_resolved"))
    if contract_state in {"unresolved_explained", "context_only", "conflict_explained"}:
        why_not_resolved = _ensure_why_not_resolved(obj, field_family, field_type, why_not_resolved)
        obj["why_not_resolved"] = why_not_resolved
        obj["verify_next_it"] = verify_next_it
        explanation_it = _clean_it_text(obj.get("explanation_it"))
        if (
            not explanation_it
            or _contains_backend_jargon(explanation_it)
            or _contains_non_customer_language(explanation_it)
            or "Verifica successiva:" not in explanation_it
        ):
            explanation_it = _build_customer_grade_unresolved_explanation(
                obj,
                field_family=field_family,
                field_type=field_type,
                verify_next_it=verify_next_it,
                why_not_resolved=why_not_resolved,
            )
        obj["explanation_it"] = _clean_it_text(explanation_it)
        return
    if obj.get("verify_next_it"):
        obj["verify_next_it"] = verify_next_it
    if _contains_backend_jargon(obj.get("explanation_it")) or _contains_non_customer_language(obj.get("explanation_it")):
        obj["explanation_it"] = _build_customer_grade_unresolved_explanation(
            obj,
            field_family=field_family,
            field_type=field_type,
            verify_next_it=verify_next_it,
            why_not_resolved=_ensure_why_not_resolved(obj, field_family, field_type, why_not_resolved),
        )


def _default_headline(field_key: str, value: Any, status: str) -> str:
    label = _FIELD_HEADLINE.get(field_key, field_key.replace("_", " ").title())
    value_text = str(value or "").strip()
    if value_text:
        return f"{label}: {value_text}"
    if status == "BLOCKED":
        return f"{label}: bloccato"
    if status == "NOT_FOUND":
        return f"{label}: non trovato"
    return f"{label}: da verificare"


def _default_unresolved_explanation(field_key: str, state: Dict[str, Any], evidence: List[Dict[str, Any]]) -> str:
    pages = _pages_from_evidence(evidence)
    label = _FIELD_HEADLINE.get(field_key, field_key.replace("_", " "))
    if field_key == "agibilita":
        return f"La perizia non consente di chiudere il tema {label.lower()}; le pagine {pages} richiedono verifica documentale." if pages else f"La perizia non consente di chiudere il tema {label.lower()}."
    if field_key == "conformita_catastale":
        return f"La perizia segnala elementi catastali non decisivi; le pagine {pages} non permettono una conformità catastale difendibile." if pages else "La perizia segnala elementi catastali non decisivi."
    if field_key == "regolarita_urbanistica":
        return f"La perizia richiede un approfondimento urbanistico; le pagine {pages} non consentono una chiusura più forte." if pages else "La perizia richiede un approfondimento urbanistico."
    return f"Lo stato {label.lower()} resta da verificare sulla base delle pagine {pages}." if pages else f"Lo stato {label.lower()} resta da verificare."


def _fallback_theme_name(item: Dict[str, Any]) -> str:
    text = " ".join(
        str(item.get(key) or "")
        for key in ("killer", "action", "category", "theme", "status")
    ).lower()
    if "occup" in text or "opponib" in text or "titolo" in text:
        return "occupazione_titolo_opponibilita"
    if "urban" in text or "difform" in text or "sanatoria" in text:
        return "urbanistica"
    if "catast" in text:
        return "catastale"
    return "legal"


def _map_contract_state(raw_state: Any, status: str) -> str:
    value = str(raw_state or "").strip()
    if value in {
        "deterministic_active",
        "resolved_with_context",
        "context_only",
        "unresolved_explained",
        "blocked_unreadable",
        "info_only",
    }:
        return value
    upper = value.upper()
    if upper in {"PRESENTE", "ASSENTE"} and status == "FOUND":
        return "deterministic_active"
    if upper in {"NON_VERIFICABILE", "NOT_FOUND"}:
        return "unresolved_explained"
    if status == "BLOCKED":
        return "blocked_unreadable"
    if status == "FOUND":
        return "deterministic_active"
    if status == "NOT_FOUND":
        return "context_only"
    return "unresolved_explained"


def _is_explanatory_contract_state(contract_state: Any) -> bool:
    return str(contract_state or "").strip() in {
        "resolved_with_context",
        "context_only",
        "unresolved_explained",
        "conflict_explained",
    }


def _normalize_field_state(field_key: str, state: Dict[str, Any], blocked_unreadable: bool) -> Dict[str, Any]:
    status = str(state.get("status") or "NOT_FOUND").upper().strip()
    evidence = _normalize_evidence_list(
        state.get("evidence"),
        state.get("supporting_evidence"),
        state.get("searched_in"),
    )
    evidence_pages = _pages_from_evidence(evidence)
    supporting_pages = evidence_pages
    tension_pages = [page for page in _normalize_page_list(state.get("tension_pages")) if page not in evidence_pages]
    explanation_it = _clean_it_text(state.get("explanation_it") or state.get("explanation"))
    why_not_resolved = _clean_it_text(state.get("why_not_resolved") or state.get("why_not_fully_certain"))
    verify_next_it = _clean_it_text(state.get("verify_next_it") or state.get("context_qualification"))
    if blocked_unreadable and status != "FOUND":
        status = "BLOCKED"
    contract_state = _map_contract_state(state.get("contract_state"), status)
    if blocked_unreadable and contract_state != "deterministic_active":
        contract_state = "blocked_unreadable"
    if status != "FOUND" and not explanation_it and evidence:
        explanation_it = _default_unresolved_explanation(field_key, state, evidence)
    if explanation_it and not evidence:
        explanation_it = ""
    if status != "FOUND" and not explanation_it and blocked_unreadable:
        explanation_it = "Documento non leggibile in modo difendibile; il sistema si ferma a revisione manuale."
    if explanation_it and not evidence and blocked_unreadable:
        evidence = []
    normalized = copy.deepcopy(state)
    normalized["status"] = status
    normalized["contract_state"] = contract_state
    normalized["headline_it"] = _clean_it_text(state.get("headline_it")) or _clean_it_text(_default_headline(field_key, state.get("value"), status))
    normalized["explanation_it"] = explanation_it or None
    normalized["why_not_resolved"] = why_not_resolved or None
    normalized["verify_next_it"] = verify_next_it or None
    normalized["evidence"] = evidence
    normalized["supporting_pages"] = supporting_pages
    normalized["tension_pages"] = tension_pages
    normalized["explanation_mode"] = (
        str(state.get("explanation_mode") or "").strip()
        or _explanation_mode(supporting_pages, tension_pages, blocked_unreadable and status != "FOUND")
    )
    normalized["llm_explanation_used"] = bool(state.get("llm_explanation_used"))
    normalized["explanation_fallback_reason"] = state.get("explanation_fallback_reason")
    return normalized


def _coerce_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        return None


def _scope_rank(scope: Dict[str, Any]) -> int:
    level = str(scope.get("level") or "")
    return {"bene": 3, "lot": 2, "document": 1}.get(level, 0)


def _build_scope(level: str, scope_key: str, lot_number: Any = None, bene_number: Any = None) -> Dict[str, Any]:
    return {
        "level": level,
        "scope_key": scope_key,
        "lot_number": _coerce_int(lot_number),
        "bene_number": _coerce_int(bene_number),
    }


def _scope_from_identifier(scope_id: Any) -> Optional[Dict[str, Any]]:
    value = str(scope_id or "").strip().lower()
    if not value:
        return None
    match = re.search(r"\bbene[:_ -]?(\d+)\b", value)
    if match:
        bene_number = int(match.group(1))
        lot_match = re.search(r"\blot(?:to)?[:_ -]?(\d+)\b", value)
        return _build_scope("bene", f"bene:{bene_number}", lot_match.group(1) if lot_match else None, bene_number)
    match = re.search(r"\blot(?:to)?[:_ -]?(\d+)\b", value)
    if match:
        lot_number = int(match.group(1))
        return _build_scope("lot", f"lotto:{lot_number}", lot_number, None)
    return None


def _scope_from_metadata(metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    scope_candidates = (
        metadata.get("scope_id"),
        metadata.get("source_scope_id"),
        metadata.get("target_scope_id"),
        metadata.get("scope_key"),
    )
    for candidate in scope_candidates:
        parsed = _scope_from_identifier(candidate)
        if parsed:
            return parsed
    target_scope = metadata.get("target_scope") if isinstance(metadata.get("target_scope"), dict) else {}
    for candidate in (target_scope.get("scope_key"), target_scope.get("scope_id")):
        parsed = _scope_from_identifier(candidate)
        if parsed:
            return parsed
    bene_number = _coerce_int(metadata.get("bene_number"))
    lot_number = _coerce_int(metadata.get("lot_number"))
    if bene_number is not None:
        return _build_scope("bene", f"bene:{bene_number}", lot_number, bene_number)
    if lot_number is not None:
        return _build_scope("lot", f"lotto:{lot_number}", lot_number, None)
    return None


def _scope_from_source_path(source_path: str) -> Optional[Dict[str, Any]]:
    return _scope_from_identifier(source_path)


def _scope_from_runtime_pages(
    evidence_pages: List[int],
    runtime_scopes: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    if not evidence_pages or not isinstance(runtime_scopes, dict):
        return None
    best_scope: Optional[Dict[str, Any]] = None
    best_cover = 0
    wanted_pages = set(evidence_pages)
    for scope_id, scope_payload in runtime_scopes.items():
        if not isinstance(scope_payload, dict):
            continue
        metadata = scope_payload.get("metadata") if isinstance(scope_payload.get("metadata"), dict) else {}
        detected_pages = set(_normalize_page_list(metadata.get("detected_from_pages")))
        if not detected_pages:
            continue
        cover = len(wanted_pages & detected_pages)
        if cover <= 0:
            continue
        candidate = _scope_from_identifier(scope_id) or _scope_from_metadata(metadata)
        if not candidate:
            continue
        if best_scope is None or _scope_rank(candidate) > _scope_rank(best_scope) or (
            _scope_rank(candidate) == _scope_rank(best_scope) and cover > best_cover
        ):
            best_scope = candidate
            best_cover = cover
    return best_scope


def _resolve_issue_scope(
    raw: Dict[str, Any],
    evidence: List[Dict[str, Any]],
    runtime_scopes: Dict[str, Any],
    source_path: str = "",
) -> Dict[str, Any]:
    metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
    for candidate in (
        _scope_from_metadata(metadata),
        _scope_from_source_path(source_path),
        _scope_from_runtime_pages(_pages_from_evidence(evidence), runtime_scopes),
    ):
        if candidate:
            return candidate
    return _build_scope("document", "document", None, None)


def _severity_from_field(field_key: str, state: Dict[str, Any]) -> str:
    status = str(state.get("status") or "").upper()
    value = str(state.get("value") or "").upper().strip()
    if status == "BLOCKED":
        return "BLOCKER"
    if status in {"LOW_CONFIDENCE", "NOT_FOUND"}:
        return "AMBER"
    if value and value in _NEGATIVE_VALUE_HINTS.get(field_key, set()):
        return "RED"
    if field_key == "regolarita_urbanistica" and "DIFFORM" in value:
        return "RED"
    if field_key == "agibilita" and value == "ASSENTE":
        return "RED"
    if field_key == "stato_occupativo" and value.startswith("OCCUPATO"):
        return "RED"
    return "INFO"


def _issue_id(family: str, headline: str, pages: List[int]) -> str:
    digest = hashlib.sha1(f"{family}|{headline}|{pages}".encode("utf-8")).hexdigest()[:12]
    return f"{family}_{digest}"


def _normalize_family(value: Any) -> str:
    family = str(value or "").strip().lower()
    aliases = {
        "delivery_timeline": "agibilita",
        "abitabilita": "agibilita",
        "agibilità": "agibilita",
        "conformita_catastale": "catastale",
        "catastale": "catastale",
        "urbanistica": "urbanistica",
        "occupazione": "occupancy",
        "occupancy": "occupancy",
        "legal": "legal",
        "rights": "rights",
        "valuation": "valuation",
        "costi": "costs",
        "costs": "costs",
        "impianti": "impianti",
    }
    return aliases.get(family, family or "legal")


def _infer_family_from_text(*parts: Any) -> Optional[str]:
    text = " ".join(str(part or "") for part in parts).lower()
    if "agibil" in text or "abitabil" in text or "certificato di agibil" in text:
        return "agibilita"
    if "catast" in text:
        return "catastale"
    if "urban" in text or "difform" in text or "sanatoria" in text or "condono" in text or "ripristino" in text:
        return "urbanistica"
    if "occup" in text or "libero" in text or "debitore" in text or "opponib" in text:
        return "occupancy"
    if "spes" in text or "condomin" in text or "oneri" in text or "costo" in text:
        return "costs"
    if "valore" in text or "prezzo base" in text or "asta" in text:
        return "valuation"
    if "diritto" in text or "quota" in text or "usufrutto" in text:
        return "rights"
    if "impiant" in text or "riscald" in text:
        return "impianti"
    return None


def _canonical_family(raw_family: str, *parts: Any) -> str:
    inferred = _infer_family_from_text(*parts)
    if inferred and raw_family in {"legal", "delivery_timeline", ""}:
        return inferred
    if inferred == "agibilita" and raw_family == "delivery_timeline":
        return inferred
    return _normalize_family(raw_family or inferred)


def _normalize_issue_pages(issue: Dict[str, Any]) -> Dict[str, Any]:
    evidence = _normalize_evidence_list(issue.get("evidence"))
    supporting_pages = _pages_from_evidence(evidence)
    tension_pages = [page for page in _normalize_page_list(issue.get("tension_pages")) if page not in supporting_pages]
    issue["evidence"] = evidence
    issue["supporting_pages"] = supporting_pages
    issue["tension_pages"] = tension_pages
    issue["explanation_mode"] = str(issue.get("explanation_mode") or _explanation_mode(supporting_pages, tension_pages, str(issue.get("status") or "").upper() == "BLOCKED"))
    issue["llm_explanation_used"] = bool(issue.get("llm_explanation_used"))
    issue["explanation_fallback_reason"] = issue.get("explanation_fallback_reason")
    return issue


_CUSTOMER_INTERNAL_CONTROL_KEYS = {
    "contract_state",
    "explanation_fallback_reason",
    "explanation_mode",
    "llm_explanation_used",
    "customer_visible_amount_status",
    "source_path",
    "driver_field",
    "theme_resolution",
    "llm_outcome",
    "raw",
    "debug",
    "candidate",
    "candidates",
    "step3_candidates",
}

_CUSTOMER_INTERNAL_MARKER_VALUES = {
    "unresolved_explained",
    "no_packet",
    "deterministic_active",
    "resolved_with_context",
    "context_only",
    "conflict_explained",
    "blocked_unreadable",
    "info_only",
    "quantified_estimate",
    "qualitative_burden",
    "llm_timeout",
    "llm_error",
    "TBD",
    "NOT_SPECIFIED",
    "NON SPECIFICATO IN PERIZIA",
    "INTERNAL DIRTY",
}

_CUSTOMER_INTERNAL_VALUE_KEYS = {
    "state",
    "source_state",
    "resolution",
    "resolution_state",
    "reason",
    "fallback_reason",
}

_CUSTOMER_INTERNAL_PROVENANCE_VALUES = {
    "verifier_runtime",
    "verifier_runtime_v1",
}

_CUSTOMER_INTERNAL_PROVENANCE_KEYS = {
    "source",
    "resolver_version",
}

_CUSTOMER_FACING_RESULT_KEYS = {
    "issues",
    "field_states",
    "dati_certi_del_lotto",
    "section_3_money_box",
    "money_box",
    "section_9_legal_killers",
    "abusi_edilizi_conformita",
    "red_flags_operativi",
    "section_11_red_flags",
    "customer_decision_contract",
}

_CUSTOMER_RESULT_INTERNAL_RUNTIME_KEYS = {
    "verifier_runtime",
    "canonical_freeze_contract",
    "canonical_freeze_explanations",
    "debug",
}


def _is_customer_internal_marker(value: Any) -> bool:
    return isinstance(value, str) and value.strip() in _CUSTOMER_INTERNAL_MARKER_VALUES


def _is_customer_internal_provenance(key: str, value: Any) -> bool:
    return (
        key in _CUSTOMER_INTERNAL_PROVENANCE_KEYS
        and isinstance(value, str)
        and value.strip() in _CUSTOMER_INTERNAL_PROVENANCE_VALUES
    )


def _strip_customer_internal_controls(value: Any) -> Any:
    if isinstance(value, dict):
        cleaned: Dict[str, Any] = {}
        for key, child in value.items():
            key_text = str(key)
            if key_text in _CUSTOMER_INTERNAL_CONTROL_KEYS:
                continue
            if _is_customer_internal_provenance(key_text, child):
                continue
            stripped = _strip_customer_internal_controls(child)
            if key_text == "resolver_meta" and isinstance(stripped, dict) and not stripped:
                continue
            if _is_customer_internal_marker(stripped) and (
                key_text in _CUSTOMER_INTERNAL_VALUE_KEYS
                or key_text.endswith("_state")
                or key_text.endswith("_reason")
                or key_text.endswith("_resolution")
            ):
                continue
            cleaned[key] = stripped
        return cleaned
    if isinstance(value, list):
        cleaned_items: List[Any] = []
        for item in value:
            stripped = _strip_customer_internal_controls(item)
            if _is_customer_internal_marker(stripped):
                continue
            cleaned_items.append(stripped)
        return cleaned_items
    return value


def _strip_customer_internal_provenance(value: Any) -> Any:
    if isinstance(value, dict):
        cleaned: Dict[str, Any] = {}
        for key, child in value.items():
            key_text = str(key)
            if _is_customer_internal_provenance(key_text, child):
                continue
            stripped = _strip_customer_internal_provenance(child)
            if key_text == "resolver_meta" and isinstance(stripped, dict) and not stripped:
                continue
            cleaned[key] = stripped
        return cleaned
    if isinstance(value, list):
        return [_strip_customer_internal_provenance(item) for item in value]
    return value


def sanitize_customer_facing_result(result: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(result, dict):
        return result
    for key in _CUSTOMER_FACING_RESULT_KEYS:
        if key in result:
            result[key] = _strip_customer_internal_controls(result[key])
    cleaned_result = _strip_customer_internal_provenance(result)
    result.clear()
    result.update(cleaned_result)
    return result


def separate_internal_runtime_from_customer_result(result: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(result, dict):
        return {}
    internal_runtime: Dict[str, Any] = {}
    for key in _CUSTOMER_RESULT_INTERNAL_RUNTIME_KEYS:
        if key in result:
            internal_runtime[key] = copy.deepcopy(result.pop(key))
    return internal_runtime


def _scope_identity(scope: Dict[str, Any]) -> str:
    return "|".join(
        [
            str(scope.get("level") or ""),
            str(scope.get("scope_key") or ""),
            str(scope.get("lot_number") or ""),
            str(scope.get("bene_number") or ""),
        ]
    )


def _normalized_text_key(value: Any) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip().lower())
    return re.sub(r"[^\w\s:€]", "", text)


def _issue_richness(issue: Dict[str, Any]) -> tuple:
    return (
        len(issue.get("evidence") or []),
        _scope_rank(issue.get("scope") or {}),
        1 if issue.get("verify_next_it") else 0,
        1 if issue.get("why_not_resolved") else 0,
        len(str(issue.get("explanation_it") or "")),
    )


def _issue_from_priority_item(
    raw: Dict[str, Any],
    runtime_scopes: Dict[str, Any],
    family_hint: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    evidence = _normalize_evidence_list(raw.get("evidence"))
    if not evidence:
        return None
    headline = _clean_it_text(raw.get("title_it")) or _clean_it_text(raw.get("killer")) or "Issue"
    source_path = f"verifier_runtime.canonical_case.priority.issues.{raw.get('code') or 'item'}"
    family = _canonical_family(
        family_hint or raw.get("category") or raw.get("theme") or "legal",
        headline,
        raw.get("summary_it"),
        raw.get("action_it"),
        " ".join(str(item.get("quote") or "") for item in evidence),
        source_path,
    )
    supporting_pages = _pages_from_evidence(evidence)
    tension_pages = [page for page in _normalize_page_list(raw.get("tension_pages")) if page not in supporting_pages]
    explanation_it = _clean_it_text(raw.get("summary_it")) or headline
    verify_next_it = _clean_it_text(raw.get("action_it"))
    status = "FOUND"
    severity = str(raw.get("severity") or "AMBER").upper().strip()
    if severity not in {"INFO", "AMBER", "RED", "BLOCKER"}:
        severity = "AMBER"
    issue = {
        "issue_id": _issue_id(family, headline, supporting_pages),
        "family": family,
        "scope": _resolve_issue_scope(raw, evidence, runtime_scopes, source_path),
        "status": status,
        "contract_state": "deterministic_active",
        "severity": severity,
        "headline_it": headline,
        "explanation_it": explanation_it,
        "why_not_resolved": None,
        "verify_next_it": verify_next_it or None,
        "evidence": evidence,
        "supporting_pages": supporting_pages,
        "tension_pages": tension_pages,
        "explanation_mode": _explanation_mode(supporting_pages, tension_pages, False),
        "source_path": source_path,
        "theme": family,
    }
    return _normalize_issue_pages(issue)


def _issue_from_field_state(field_key: str, state: Dict[str, Any], runtime_scopes: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    evidence = _normalize_evidence_list(state.get("evidence"))
    if not evidence:
        return None
    status = str(state.get("status") or "").upper()
    severity = _severity_from_field(field_key, state)
    value = str(state.get("value") or "").upper().strip()
    negative = severity in {"AMBER", "RED", "BLOCKER"} or status != "FOUND"
    if not negative and value not in _NEGATIVE_VALUE_HINTS.get(field_key, set()):
        return None
    family = _FIELD_FAMILY.get(field_key, "legal")
    headline = _clean_it_text(state.get("headline_it")) or _default_headline(field_key, state.get("value"), status)
    supporting_pages = _pages_from_evidence(evidence)
    tension_pages = [page for page in _normalize_page_list(state.get("tension_pages")) if page not in supporting_pages]
    source_path = f"field_states.{field_key}"
    issue = {
        "issue_id": _issue_id(family, headline, supporting_pages),
        "family": family,
        "scope": _resolve_issue_scope(
            {"metadata": state.get("resolver_meta") if isinstance(state.get("resolver_meta"), dict) else {}},
            evidence,
            runtime_scopes,
            source_path,
        ),
        "status": status,
        "contract_state": str(state.get("contract_state") or "unresolved_explained"),
        "severity": severity,
        "headline_it": headline,
        "explanation_it": _clean_it_text(state.get("explanation_it")) or headline,
        "why_not_resolved": _clean_it_text(state.get("why_not_resolved")) or None,
        "verify_next_it": _clean_it_text(state.get("verify_next_it")) or None,
        "evidence": evidence,
        "supporting_pages": supporting_pages,
        "tension_pages": tension_pages,
        "explanation_mode": str(state.get("explanation_mode") or _explanation_mode(supporting_pages, tension_pages, status == "BLOCKED")),
        "source_path": source_path,
        "theme": family,
        "llm_explanation_used": bool(state.get("llm_explanation_used")),
        "explanation_fallback_reason": state.get("explanation_fallback_reason"),
    }
    return _normalize_issue_pages(issue)


def _dedupe_issues(issues: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out_map: Dict[tuple, Dict[str, Any]] = {}
    for issue in issues:
        if not isinstance(issue, dict):
            continue
        issue = _normalize_issue_pages(copy.deepcopy(issue))
        evidence = issue.get("evidence") or []
        primary_quote = evidence[0].get("quote") if evidence and isinstance(evidence[0], dict) else ""
        sig = (
            _normalize_family(issue.get("family")),
            _normalized_text_key(issue.get("headline_it")),
            tuple(issue.get("supporting_pages") or []),
            _normalized_text_key(primary_quote),
            _scope_identity(issue.get("scope") or {}),
        )
        existing = out_map.get(sig)
        if existing is None or _issue_richness(issue) > _issue_richness(existing):
            out_map[sig] = issue
    out = list(out_map.values())
    out.sort(key=lambda item: ({"BLOCKER": 0, "RED": 1, "AMBER": 2, "INFO": 3}.get(str(item.get("severity") or ""), 9), str(item.get("headline_it") or "")))
    return out


def _blocked_issue(document_quality: Dict[str, Any]) -> Dict[str, Any]:
    explanation = _clean_it_text(
        document_quality.get("customer_message_it")
        or "Documento non leggibile in modo affidabile; il sistema non produce decisioni più forti."
    )
    return {
        "issue_id": "document_blocked_unreadable",
        "family": "legal",
        "scope": {"level": "document", "scope_key": "document", "lot_number": None, "bene_number": None},
        "status": "BLOCKED",
        "contract_state": "blocked_unreadable",
        "severity": "BLOCKER",
        "headline_it": "Documento non leggibile o estrazione bloccata.",
        "explanation_it": explanation,
        "why_not_resolved": "Manca una base testuale affidabile e anchor-bound per decidere oltre.",
        "verify_next_it": "Verifica manuale obbligatoria sul documento originale.",
        "evidence": [],
        "supporting_pages": [],
        "tension_pages": [],
        "explanation_mode": "blocked",
        "source_path": "document_quality",
        "theme": "legal",
    }


def _build_semaforo(issues: List[Dict[str, Any]], blocked_unreadable: bool, document_quality: Dict[str, Any]) -> Dict[str, Any]:
    if blocked_unreadable:
        reason_it = _clean_it_text(document_quality.get("customer_message_it")) or "Documento non valutabile automaticamente."
        return {
            "status": "UNKNOWN",
            "status_it": "NON VALUTABILE",
            "status_en": "NOT ASSESSABLE",
            "reason_it": reason_it,
            "top_blockers": ["DOCUMENT_UNREADABLE"],
        }
    top = issues[:3]
    if any(item.get("severity") in {"BLOCKER", "RED"} for item in issues):
        status = "RED"
    elif any(item.get("severity") == "AMBER" for item in issues):
        status = "AMBER"
    else:
        status = "GREEN"
    status_it_map = {"GREEN": "VERDE", "AMBER": "ATTENZIONE", "RED": "CRITICO"}
    status_en_map = {"GREEN": "GREEN", "AMBER": "AMBER", "RED": "RED"}
    top_blockers = [
        {
            "issue_id": item.get("issue_id"),
            "key": item.get("family"),
            "label_it": item.get("headline_it"),
            "status": item.get("status"),
            "severity": item.get("severity"),
            "supporting_pages": item.get("supporting_pages", []),
            "tension_pages": item.get("tension_pages", []),
            "evidence": item.get("evidence", []),
        }
        for item in top
    ]
    reason_it = _clean_it_text(top[0].get("headline_it")) if top else "Non emergono criticità materialmente ancorate."
    return {
        "status": status,
        "status_it": status_it_map[status],
        "status_en": status_en_map[status],
        "reason_it": reason_it,
        "top_blockers": top_blockers,
    }


def _build_legal_killers(existing: Dict[str, Any], issues: List[Dict[str, Any]]) -> Dict[str, Any]:
    section = copy.deepcopy(existing) if isinstance(existing, dict) else {}
    items = section.get("items") if isinstance(section.get("items"), list) else []
    top_items = section.get("top_items") if isinstance(section.get("top_items"), list) else []
    projected_items = [copy.deepcopy(item) for item in items if isinstance(item, dict)]
    projected_top = [copy.deepcopy(item) for item in top_items if isinstance(item, dict)]
    if not projected_top:
        projected_top = [
            {
                "killer": issue.get("headline_it"),
                "status": issue.get("severity"),
                "action": issue.get("verify_next_it"),
                "evidence": copy.deepcopy(issue.get("evidence", [])),
                "source": "customer_decision_contract",
                "category": issue.get("family"),
            }
            for issue in issues[:3]
        ]
    if not projected_items:
        projected_items = copy.deepcopy(projected_top)
    resolver_meta = section.get("resolver_meta") if isinstance(section.get("resolver_meta"), dict) else {}
    if "themes" not in resolver_meta:
        resolver_meta["themes"] = [
            {
                "theme": issue.get("family"),
                "theme_resolution": issue.get("contract_state"),
                "driver_field": issue.get("source_path"),
                "driver_status": issue.get("status"),
                "driver_value": issue.get("headline_it"),
            }
            for issue in issues[:6]
        ]
    if not resolver_meta.get("themes"):
        fallback_themes: List[Dict[str, Any]] = []
        for item in projected_top[:3] or projected_items[:3]:
            if not isinstance(item, dict):
                continue
            fallback_themes.append(
                {
                    "theme": _fallback_theme_name(item),
                    "theme_resolution": str(item.get("status") or ""),
                    "driver_field": "section_9_legal_killers",
                    "driver_status": str(item.get("status") or ""),
                    "driver_value": str(item.get("killer") or ""),
                }
            )
        resolver_meta["themes"] = fallback_themes
    return {"items": projected_items, "top_items": projected_top, "resolver_meta": resolver_meta}


def _build_red_flags(issues: List[Dict[str, Any]], blocked_unreadable: bool) -> List[Dict[str, Any]]:
    if blocked_unreadable:
        return [
            {
                "code": "MANUAL_REVIEW",
                "severity": "BLOCKER",
                "flag_it": "Documento non leggibile o estrazione bloccata",
                "flag_en": "Unreadable document or blocked extraction",
                "action_it": "Nessuna conclusione affidabile automatica: verifica manuale obbligatoria sul documento originale.",
            }
        ]
    flags: List[Dict[str, Any]] = []
    for issue in issues:
        if issue.get("severity") not in {"BLOCKER", "RED", "AMBER"}:
            continue
        flags.append(
            {
                "code": str(issue.get("issue_id") or "").upper()[:60],
                "severity": issue.get("severity"),
                "flag_it": issue.get("headline_it"),
                "flag_en": issue.get("headline_it"),
                "action_it": issue.get("verify_next_it") or issue.get("explanation_it"),
                "evidence": copy.deepcopy(issue.get("evidence", [])),
            }
        )
    return flags[:6]


def _build_money_box(result: Dict[str, Any], issues: List[Dict[str, Any]], blocked_unreadable: bool) -> Dict[str, Any]:
    existing_money_box = result.get("money_box") if isinstance(result.get("money_box"), dict) else {}
    verifier_runtime = result.get("verifier_runtime") if isinstance(result.get("verifier_runtime"), dict) else {}
    canonical_case = verifier_runtime.get("canonical_case") if isinstance(verifier_runtime.get("canonical_case"), dict) else {}
    costs = canonical_case.get("costs") if isinstance(canonical_case.get("costs"), dict) else {}
    raw_items = costs.get("explicit_buyer_costs") if isinstance(costs.get("explicit_buyer_costs"), list) else []
    items: List[Dict[str, Any]] = []
    for idx, raw in enumerate(raw_items, start=1):
        if not isinstance(raw, dict):
            continue
        amount = raw.get("amount")
        if not isinstance(amount, (int, float)):
            continue
        evidence = _normalize_evidence_list(raw.get("evidence"), limit=2)
        if not evidence:
            continue
        label = str(raw.get("label") or raw.get("label_it") or "").strip() or "Costo buyer-side esplicito da perizia"
        items.append(
            {
                "code": str(raw.get("code") or f"VR_COST_{idx:02d}"),
                "label_it": label,
                "label_en": label,
                "type": "ESTIMATE",
                "stima_euro": int(round(float(amount))),
                "stima_nota": "Costo buyer-side esplicito rilevato nella perizia.",
                "evidence": evidence,
                "fonte_perizia": {"value": "Perizia", "evidence": evidence},
                "contract_state": "quantified_estimate",
                "customer_visible_amount_status": "quantified_estimate",
            }
        )
    if blocked_unreadable:
        blocked_note = "Documento/perizia non leggibile o estrazione bloccata: nessun totale extra difendibile può essere ricavato automaticamente; verifica manuale obbligatoria."
        return {
            "policy": "BLOCKED_UNREADABLE",
            "items": [],
            "total_extra_costs": {
                "min": None,
                "max": None,
                "max_is_open": False,
                "note": blocked_note,
                "contract_state": "blocked_unreadable",
                "evidence": [],
            },
            "removed_pricing_amount_items": copy.deepcopy(existing_money_box.get("removed_pricing_amount_items", [])),
        }
    explicit_total = costs.get("explicit_total")
    if isinstance(explicit_total, (int, float)) and items:
        total_note = f"Totale stimato in perizia: € {int(round(float(explicit_total)))}."
        if items:
            total_note += " Le singole voci quantificate sotto sono componenti del totale e non un secondo totale autonomo."
        return {
            "policy": "CANONICAL_RUNTIME",
            "items": items,
            "total_extra_costs": {
                "range": {"min": int(round(float(explicit_total))), "max": int(round(float(explicit_total)))},
                "max_is_open": False,
                "note": total_note,
                "contract_state": "quantified_estimate",
                "evidence": copy.deepcopy(items[0].get("evidence", [])),
            },
            "removed_pricing_amount_items": copy.deepcopy(existing_money_box.get("removed_pricing_amount_items", [])),
        }
    cost_issue = next((issue for issue in issues if issue.get("family") == "costs" and _normalize_evidence_list(issue.get("evidence"))), None)
    unresolved_note = None
    unresolved_evidence: List[Dict[str, Any]] = []
    if cost_issue:
        unresolved_note = _clean_it_text(cost_issue.get("explanation_it")) or "La perizia segnala oneri buyer-side senza un totale numerico difendibile."
        unresolved_evidence = copy.deepcopy(cost_issue.get("evidence", []))
    elif items:
        unresolved_note = "La perizia riporta voci di costo buyer-side ancorate, ma non consente di difendere un totale extra unico."
        unresolved_evidence = copy.deepcopy(items[0].get("evidence", []))
    if unresolved_note is None:
        unresolved_note = "La perizia non riporta un totale extra buyer-side numericamente difendibile; serve verifica manuale delle voci ancorate."
    return {
        "policy": "CONSERVATIVE",
        "items": items,
        "total_extra_costs": {
            "min": None,
            "max": None,
            "max_is_open": False,
            "note": unresolved_note,
            "contract_state": "unresolved_explained",
            "evidence": unresolved_evidence,
        },
        "removed_pricing_amount_items": copy.deepcopy(existing_money_box.get("removed_pricing_amount_items", [])),
    }


def _build_summary_bundle(issues: List[Dict[str, Any]], semaforo: Dict[str, Any], blocked_unreadable: bool, document_quality: Dict[str, Any]) -> Dict[str, Any]:
    if blocked_unreadable:
        blocked_it = _clean_it_text(document_quality.get("customer_message_it")) or "Documento non leggibile o estrazione bloccata: non è possibile formulare conclusioni affidabili senza verifica manuale."
        return {
            "top_issue_it": "",
            "top_issue_en": "",
            "next_step_it": blocked_it,
            "next_step_en": "Manual review required.",
            "caution_points_it": ["Verifica manuale obbligatoria sul documento originale."],
            "user_messages_it": [],
            "document_quality_status": str(document_quality.get("status") or ""),
            "semaforo_status": "UNKNOWN",
            "decision_summary_it": blocked_it,
            "decision_summary_en": "Unreadable document or blocked extraction.",
            "evidence_snippets": [],
            "summary_trace": [{"sentence": blocked_it, "issue_ids": ["document_blocked_unreadable"]}],
        }
    top_issue = issues[0] if issues else None
    top_issue_it = str(top_issue.get("headline_it") or "") if top_issue else ""
    next_step_it = str(top_issue.get("verify_next_it") or "") if top_issue else ""
    if not next_step_it and top_issue:
        next_step_it = str(top_issue.get("explanation_it") or "")
    decision_summary_it = top_issue_it or "Non emergono criticità materialmente ancorate."
    evidence_snippets = []
    if top_issue:
        for ev in list(top_issue.get("evidence") or [])[:2]:
            if isinstance(ev, dict) and isinstance(ev.get("page"), int) and str(ev.get("quote") or "").strip():
                evidence_snippets.append({"page": ev["page"], "quote": str(ev["quote"])[:240]})
    summary_trace = []
    if decision_summary_it:
        summary_trace.append({"sentence": decision_summary_it, "issue_ids": [top_issue.get("issue_id")] if top_issue else []})
    if next_step_it:
        summary_trace.append({"sentence": next_step_it, "issue_ids": [top_issue.get("issue_id")] if top_issue else []})
    return {
        "top_issue_it": top_issue_it,
        "top_issue_en": "",
        "next_step_it": next_step_it,
        "next_step_en": "",
        "caution_points_it": [str(item.get("headline_it") or "") for item in issues[1:3]],
        "user_messages_it": [],
        "document_quality_status": str(document_quality.get("status") or ""),
        "semaforo_status": str(semaforo.get("status") or ""),
        "decision_summary_it": decision_summary_it,
        "decision_summary_en": "",
        "evidence_snippets": evidence_snippets,
        "summary_trace": summary_trace,
    }


def _build_abusi_projection(field_states: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for field_key, alias in (
        ("agibilita", "agibilita"),
        ("regolarita_urbanistica", "conformita_urbanistica"),
        ("conformita_catastale", "conformita_catastale"),
    ):
        state = field_states.get(field_key) if isinstance(field_states.get(field_key), dict) else None
        if not state:
            continue
        out[alias] = {
            "status": state.get("value") or state.get("status"),
            "detail_it": state.get("headline_it"),
            "evidence": copy.deepcopy(state.get("evidence", [])),
            "explanation_it": state.get("explanation_it"),
            "why_not_resolved": state.get("why_not_resolved"),
            "verify_next_it": state.get("verify_next_it"),
            "contract_state": state.get("contract_state"),
        }
    return out


def _packet_issue_type(contract_state: str, explanation_mode: str) -> str:
    if explanation_mode == "conflict_explained" or contract_state == "conflict_explained":
        return "FIELD_CONFLICT"
    if contract_state in {"resolved_with_context", "context_only"}:
        return "GROUPED_CONTEXT_NEEDS_EXPLANATION"
    return "SUSPICIOUS_SILENCE"


def _build_llm_packet(
    obj: Dict[str, Any],
    *,
    issue_id: str,
    field_family: str,
    field_type: str,
    scope: Dict[str, Any],
    case_key: str,
) -> Optional[Dict[str, Any]]:
    evidence = _family_specific_evidence(obj, field_family, field_type)
    supporting_pages = _pages_from_evidence(evidence)
    if not evidence or not supporting_pages:
        return None
    contract_state = str(obj.get("contract_state") or "").strip()
    issue_type = _packet_issue_type(contract_state, str(obj.get("explanation_mode") or ""))
    value = str(obj.get("value") or "").strip()
    candidate_values = []
    if value and value.upper() not in {"DA VERIFICARE", "NON TROVATO", "BLOCKED", "BLOCCATO"}:
        candidate_values = [value]
    return {
        "issue_id": issue_id,
        "case_key": case_key,
        "target_case_key": case_key,
        "field_family": field_family,
        "field_type": field_type,
        "target_field": field_type,
        "issue_type": issue_type,
        "candidate_values": candidate_values,
        "supporting_candidates": [
            {
                "extracted_value": value or None,
                "page": item["page"],
                "quote": item["quote"],
                "context": item["quote"],
            }
            for item in evidence
        ],
        "supporting_blocked_entries": [],
        "local_text_windows": [{"page": item["page"], "text": item["quote"]} for item in evidence],
        "shell_quotes": [item["quote"] for item in evidence],
        "source_pages": supporting_pages + [page for page in _normalize_page_list(obj.get("tension_pages")) if page not in supporting_pages],
        "supporting_pages": supporting_pages,
        "tension_pages": _normalize_page_list(obj.get("tension_pages")),
        "anchor_pages": supporting_pages,
        "evidence_pages": supporting_pages,
        "lot_id": scope.get("lot_number"),
        "bene_id": scope.get("bene_number"),
        "scope_metadata": {
            "scope_key": scope.get("scope_key"),
            "scope_level": scope.get("level"),
        },
        "target_scope": {
            "scope_key": scope.get("scope_key"),
            "scope_level": scope.get("level"),
        },
    }


def _cached_llm_resolution(packet: Dict[str, Any], grouped_llm_explanations: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    packet_pages = sorted(set(packet.get("source_pages") or []))
    packet_family = str(packet.get("field_family") or "")
    packet_scope = str((packet.get("scope_metadata") or {}).get("scope_key") or "")
    for item in grouped_llm_explanations:
        if not isinstance(item, dict):
            continue
        family = str(item.get("field_family") or item.get("field_type") or "")
        scope = str(item.get("scope_key") or "")
        pages = sorted(set(_normalize_page_list(item.get("source_pages"))))
        if family == packet_family and scope == packet_scope and pages == packet_pages:
            return item
    return None


def _llm_failure_reason(exc: Exception) -> str:
    text = str(exc or "").lower()
    if "timeout" in text or "timed out" in text:
        return "llm_timeout"
    return "llm_error"


def _apply_explanatory_resolution(
    obj: Dict[str, Any],
    *,
    issue_id: str,
    field_family: str,
    field_type: str,
    scope: Dict[str, Any],
    case_key: str,
    blocked_unreadable: bool,
    grouped_llm_explanations: List[Dict[str, Any]],
) -> Dict[str, Any]:
    contract_state = str(obj.get("contract_state") or "").strip()
    if blocked_unreadable:
        obj["llm_explanation_used"] = False
        obj["explanation_fallback_reason"] = "blocked_unreadable"
        return obj
    already_has_llm_copy = obj.get("llm_explanation_used") is True
    existing_copy_dirty = already_has_llm_copy and _existing_llm_copy_needs_regeneration(
        obj,
        field_family,
        field_type,
    )
    if (not already_has_llm_copy) and obj.get("explanation_fallback_reason") in {
        "no_packet",
        "llm_timeout",
        "llm_error",
        "blocked_unreadable",
    }:
        return obj
    if not _is_explanatory_contract_state(contract_state):
        obj["llm_explanation_used"] = False
        obj["explanation_fallback_reason"] = None
        return obj
    packet = _build_llm_packet(
        obj,
        issue_id=issue_id,
        field_family=field_family,
        field_type=field_type,
        scope=scope,
        case_key=case_key,
    )
    if already_has_llm_copy and not existing_copy_dirty:
        resolution = {
            "user_visible_explanation": obj.get("explanation_it"),
            "why_not_resolved": obj.get("why_not_resolved"),
            "context_qualification": obj.get("verify_next_it"),
            "why_not_fully_certain": obj.get("why_not_resolved"),
        }
    elif not packet:
        obj["llm_explanation_used"] = False
        obj["explanation_fallback_reason"] = "no_packet"
        _sanitize_explanatory_fields(obj, field_family=field_family, field_type=field_type)
        return obj
    else:
        cached = _cached_llm_resolution(packet, grouped_llm_explanations)
        if cached:
            resolution = cached
        else:
            config = discover_openai_config()
            api_key = config.get("api_key")
            if not api_key:
                obj["llm_explanation_used"] = False
                obj["explanation_fallback_reason"] = "llm_error"
                _sanitize_explanatory_fields(obj, field_family=field_family, field_type=field_type)
                return obj
            if os.environ.get("PYTEST_CURRENT_TEST") and str(config.get("model") or "") != "fake":
                obj["llm_explanation_used"] = False
                obj["explanation_fallback_reason"] = "llm_error"
                _sanitize_explanatory_fields(obj, field_family=field_family, field_type=field_type)
                return obj
            try:
                resolution = resolve_single_issue(packet, str(api_key), str(config.get("model") or "gpt-4o-mini"))
            except Exception as exc:
                obj["llm_explanation_used"] = False
                obj["explanation_fallback_reason"] = _llm_failure_reason(exc)
                _sanitize_explanatory_fields(obj, field_family=field_family, field_type=field_type)
                return obj
    explanation_it = _clean_it_text(resolution.get("user_visible_explanation"))
    why_not_resolved = _clean_it_text(resolution.get("why_not_resolved"))
    verify_next_it = _clean_it_text(
        resolution.get("context_qualification")
        or resolution.get("why_not_fully_certain")
    )
    if explanation_it:
        obj["explanation_it"] = explanation_it
    if why_not_resolved:
        obj["why_not_resolved"] = why_not_resolved
    if verify_next_it:
        obj["verify_next_it"] = verify_next_it
    obj["llm_explanation_used"] = True
    obj["explanation_fallback_reason"] = None
    _sanitize_explanatory_fields(obj, field_family=field_family, field_type=field_type)
    return obj


def apply_customer_decision_contract(result: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(result, dict):
        return {}
    document_quality = result.get("document_quality") if isinstance(result.get("document_quality"), dict) else {}
    canonical_contract_state = result.get("canonical_contract_state") if isinstance(result.get("canonical_contract_state"), dict) else {}
    blocked_unreadable = (
        str(result.get("analysis_status") or "").upper() == "UNREADABLE"
        or str(document_quality.get("status") or "").upper() == "UNREADABLE"
        or str(canonical_contract_state.get("reason") or "").lower() == "canonical_freeze_blocked_unreadable"
    )
    verifier_runtime = result.get("verifier_runtime") if isinstance(result.get("verifier_runtime"), dict) else {}
    canonical_case = verifier_runtime.get("canonical_case") if isinstance(verifier_runtime.get("canonical_case"), dict) else {}
    existing_states = result.get("field_states") if isinstance(result.get("field_states"), dict) else {}
    grouped_llm_explanations = canonical_case.get("grouped_llm_explanations") if isinstance(canonical_case.get("grouped_llm_explanations"), list) else []
    normalized_states = {
        field_key: _normalize_field_state(field_key, state, blocked_unreadable)
        for field_key, state in existing_states.items()
        if isinstance(state, dict)
    }
    all_normalized_states = copy.deepcopy(normalized_states)
    runtime_scopes = verifier_runtime.get("scopes") if isinstance(verifier_runtime.get("scopes"), dict) else {}
    normalized_states = {
        field_key: state
        for field_key, state in normalized_states.items()
        if str(state.get("status") or "").upper() == "FOUND" or state.get("evidence")
    }
    priority = canonical_case.get("priority") if isinstance(canonical_case.get("priority"), dict) else {}
    issues: List[Dict[str, Any]] = []
    for raw in priority.get("issues") if isinstance(priority.get("issues"), list) else []:
        issue = _issue_from_priority_item(raw, runtime_scopes)
        if issue:
            issues.append(issue)
    top_issue = priority.get("top_issue") if isinstance(priority.get("top_issue"), dict) else None
    if top_issue:
        issue = _issue_from_priority_item(top_issue, runtime_scopes)
        if issue:
            issues.append(issue)
    for field_key, state in normalized_states.items():
        issue = _issue_from_field_state(field_key, state, runtime_scopes)
        if issue:
            issues.append(issue)
    if blocked_unreadable:
        issues.insert(0, _blocked_issue(document_quality))
    issues = _dedupe_issues(issues)
    for field_key, state in normalized_states.items():
        if str(state.get("status") or "").upper() == "FOUND":
            continue
        if state.get("evidence"):
            continue
        family = _FIELD_FAMILY.get(field_key)
        borrowed_issue = next(
            (
                issue
                for issue in issues
                if issue.get("family") == family and isinstance(issue.get("evidence"), list) and issue.get("evidence")
            ),
            None,
        )
        if not borrowed_issue:
            continue
        state["evidence"] = copy.deepcopy(borrowed_issue.get("evidence", []))
        state["supporting_pages"] = copy.deepcopy(borrowed_issue.get("supporting_pages", []))
        state["tension_pages"] = copy.deepcopy(borrowed_issue.get("tension_pages", []))
        state["explanation_mode"] = borrowed_issue.get("explanation_mode")
        if not state.get("explanation_it"):
            state["explanation_it"] = borrowed_issue.get("explanation_it")
        if not state.get("explanation_it"):
            state["explanation_it"] = _clean_it_text(state.get("context_qualification"))
    for field_key, state in normalized_states.items():
        _apply_explanatory_resolution(
            state,
            issue_id=f"field_state.{field_key}",
            field_family=_FIELD_FAMILY.get(field_key, "legal"),
            field_type=field_key,
            scope=_resolve_issue_scope(
                {"metadata": state.get("resolver_meta") if isinstance(state.get("resolver_meta"), dict) else {}},
                _normalize_evidence_list(state.get("evidence")),
                runtime_scopes,
                f"field_states.{field_key}",
            ),
            case_key=str(result.get("analysis_id") or result.get("case_id") or result.get("pdf_sha256") or "customer-contract"),
            blocked_unreadable=blocked_unreadable,
            grouped_llm_explanations=grouped_llm_explanations,
        )
        extra_issue = _issue_from_field_state(field_key, state, runtime_scopes)
        if extra_issue:
            issues.append(extra_issue)
    issues = _dedupe_issues(issues)
    for issue in issues:
        _apply_explanatory_resolution(
            issue,
            issue_id=str(issue.get("issue_id") or "issue"),
            field_family=str(issue.get("family") or "legal"),
            field_type=str(issue.get("theme") or issue.get("family") or "legal"),
            scope=issue.get("scope") if isinstance(issue.get("scope"), dict) else _build_scope("document", "document", None, None),
            case_key=str(result.get("analysis_id") or result.get("case_id") or result.get("pdf_sha256") or "customer-contract"),
            blocked_unreadable=blocked_unreadable,
            grouped_llm_explanations=grouped_llm_explanations,
        )
    semaforo = _build_semaforo(issues, blocked_unreadable, document_quality)
    legal_killers = _build_legal_killers(
        result.get("section_9_legal_killers") if isinstance(result.get("section_9_legal_killers"), dict) else {},
        issues,
    )
    legal_meta = legal_killers.get("resolver_meta") if isinstance(legal_killers.get("resolver_meta"), dict) else {}
    themes = legal_meta.get("themes") if isinstance(legal_meta.get("themes"), list) else []
    theme_names = {item.get("theme") for item in themes if isinstance(item, dict)}
    if "opponibilita_occupazione" in all_normalized_states and "occupazione_titolo_opponibilita" not in theme_names:
        themes.append(
            {
                "theme": "occupazione_titolo_opponibilita",
                "theme_resolution": all_normalized_states["opponibilita_occupazione"].get("contract_state"),
                "driver_field": "field_states.opponibilita_occupazione",
                "driver_status": all_normalized_states["opponibilita_occupazione"].get("status"),
                "driver_value": all_normalized_states["opponibilita_occupazione"].get("value"),
            }
        )
    if "regolarita_urbanistica" in all_normalized_states and "urbanistica" not in theme_names:
        themes.append(
            {
                "theme": "urbanistica",
                "theme_resolution": all_normalized_states["regolarita_urbanistica"].get("contract_state"),
                "driver_field": "field_states.regolarita_urbanistica",
                "driver_status": all_normalized_states["regolarita_urbanistica"].get("status"),
                "driver_value": all_normalized_states["regolarita_urbanistica"].get("value"),
            }
        )
    if "conformita_catastale" in all_normalized_states and "catastale" not in theme_names:
        themes.append(
            {
                "theme": "catastale",
                "theme_resolution": all_normalized_states["conformita_catastale"].get("contract_state"),
                "driver_field": "field_states.conformita_catastale",
                "driver_status": all_normalized_states["conformita_catastale"].get("status"),
                "driver_value": all_normalized_states["conformita_catastale"].get("value"),
            }
        )
    legal_meta["themes"] = themes
    legal_killers["resolver_meta"] = legal_meta
    money_box = _build_money_box(result, issues, blocked_unreadable)
    summary_bundle = _build_summary_bundle(issues, semaforo, blocked_unreadable, document_quality)
    summary_for_client = {
        "summary_it": summary_bundle.get("decision_summary_it"),
        "summary_en": summary_bundle.get("decision_summary_en"),
        "generation_mode": "customer_decision_contract_v1",
        "disclaimer_it": "Documento informativo. Non costituisce consulenza legale. Consultare un professionista qualificato.",
        "disclaimer_en": "Informational document. Not legal advice. Consult a qualified professional.",
    }
    decision = {
        "risk_level": str(semaforo.get("status") or ""),
        "risk_level_it": str(semaforo.get("status_it") or ""),
        "risk_level_en": str(semaforo.get("status_en") or ""),
        "summary_it": summary_bundle.get("decision_summary_it"),
        "summary_en": summary_bundle.get("decision_summary_en"),
        "driver_rosso": copy.deepcopy(semaforo.get("top_blockers", [])),
        "issue_ids": [item.get("issue_id") for item in issues[:3]],
    }
    section3 = copy.deepcopy(money_box)
    total = section3.get("total_extra_costs") if isinstance(section3.get("total_extra_costs"), dict) else {}
    section3["totale_extra_budget"] = {
        "min": (total.get("range") or {}).get("min") if isinstance(total.get("range"), dict) else total.get("min"),
        "max": (total.get("range") or {}).get("max") if isinstance(total.get("range"), dict) else total.get("max"),
        "nota": total.get("note"),
        "contract_state": total.get("contract_state"),
        "evidence": copy.deepcopy(total.get("evidence", [])),
    }
    red_flags = _build_red_flags(issues, blocked_unreadable)
    abusi_projection = _build_abusi_projection(normalized_states)
    customer_field_states = _strip_customer_internal_controls(normalized_states)
    customer_issues = _strip_customer_internal_controls(issues)
    customer_money_box = _strip_customer_internal_controls(money_box)
    customer_section3 = _strip_customer_internal_controls(section3)
    customer_red_flags = _strip_customer_internal_controls(red_flags)
    customer_legal_killers = _strip_customer_internal_controls(legal_killers)
    customer_abusi_projection = _strip_customer_internal_controls(abusi_projection)
    contract = {
        "version": "customer_decision_contract_v1",
        "field_states": copy.deepcopy(customer_field_states),
        "issues": copy.deepcopy(customer_issues),
        "semaforo_generale": copy.deepcopy(semaforo),
        "summary_for_client_bundle": copy.deepcopy(summary_bundle),
        "money_box": copy.deepcopy(customer_money_box),
        "red_flags_operativi": copy.deepcopy(customer_red_flags),
        "section_9_legal_killers": copy.deepcopy(customer_legal_killers),
        "decision_rapida_client": copy.deepcopy(decision),
    }
    result["customer_decision_contract"] = copy.deepcopy(contract)
    result["field_states"] = copy.deepcopy(customer_field_states)
    result["issues"] = copy.deepcopy(customer_issues)
    result["semaforo_generale"] = copy.deepcopy(semaforo)
    result["section_1_semaforo_generale"] = copy.deepcopy(semaforo)
    result["summary_for_client_bundle"] = copy.deepcopy(summary_bundle)
    result["summary_for_client"] = summary_for_client
    result["money_box"] = copy.deepcopy(customer_money_box)
    result["section_3_money_box"] = copy.deepcopy(customer_section3)
    result["red_flags_operativi"] = copy.deepcopy(contract["red_flags_operativi"])
    result["section_11_red_flags"] = copy.deepcopy(contract["red_flags_operativi"])
    result["section_9_legal_killers"] = copy.deepcopy(customer_legal_killers)
    result["decision_rapida_client"] = decision
    result["section_2_decisione_rapida"] = {
        "summary_it": decision.get("summary_it"),
        "summary_en": decision.get("summary_en"),
        "issue_ids": decision.get("issue_ids", []),
    }
    result["abusi_edilizi_conformita"] = copy.deepcopy(customer_abusi_projection)
    sanitize_customer_facing_result(result)
    return contract
