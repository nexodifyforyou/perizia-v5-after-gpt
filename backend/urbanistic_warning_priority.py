from __future__ import annotations

import copy
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


ISSUE_ID = "urbanistica_customer_priority"

_SCAN_ROOTS: Sequence[str] = (
    "abusi_edilizi_conformita",
    "section_5_abusi_conformita",
    "field_states",
    "issues",
    "section_1_semaforo_generale",
    "semaforo_generale",
    "section_2_decisione_rapida",
    "decision_rapida_client",
    "decision_rapida_narrated",
    "section_9_legal_killers",
    "legal_killers_checklist",
    "section_11_red_flags",
    "red_flags_operativi",
    "customer_decision_contract",
    "qa_gate",
    "lots",
    "panoramica_contract",
    "estratto_blueprint",
    "estratto_mirror",
)

_PRIMARY_EVIDENCE_ROOTS: Sequence[str] = (
    "abusi_edilizi_conformita",
    "section_5_abusi_conformita",
    "field_states",
    "qa_gate",
    "lots",
    "estratto_blueprint",
    "estratto_mirror",
)

_SKIP_KEYS = {
    "action",
    "action_it",
    "recommended_action_it",
    "verify_next_it",
    "before_offer_it",
    "checks_it",
    "debug",
    "internal_runtime",
    "errors",
    "error",
    "prompt",
    "raw",
    "rejected_payload",
    "rejected_text",
}

_EVIDENCE_KEYS = {"evidence", "supporting_evidence", "source_evidence"}

_CRITICAL_PATTERNS: Sequence[Tuple[str, re.Pattern[str]]] = (
    ("non_commerciabile", re.compile(r"\bnon\s+(?:e\s+|è\s+|risulta\s+)?commerciabil\w*\b", re.I)),
    ("forced_sale_only", re.compile(r"\b(?:solo|soltanto|esclusivamente|possibile\s+solo)\b.{0,90}\bvendita\s+forzat\w*\b|\bvendita\s+forzat\w*\b.{0,90}\b(?:solo|soltanto|esclusivamente)\b", re.I | re.S)),
    ("limited_marketability", re.compile(r"\bcommerciabilit[aà]\s+limitat\w*\b|\bnon\s+liberamente\s+commerciabil\w*\b", re.I)),
    ("not_remediable", re.compile(r"\b(?:non\s+sanabil\w*|insanabil\w*|non\s+regolarizzabil\w*)\b", re.I)),
)

_SEVERE_PATTERNS: Sequence[Tuple[str, re.Pattern[str]]] = (
    ("sanatoria_unresolved", re.compile(r"\b(?:concessione\s+in\s+)?sanatoria\s+(?:non\s+rilasciat\w*|non\s+conclus\w*|pendente)\b|\bistanza\s+di\s+sanatoria\s+non\s+conclus\w*\b", re.I)),
    ("condono_unresolved", re.compile(r"\bcondono\s+(?:pendente|non\s+definit\w*|non\s+rilasciat\w*)\b|\bdomanda\s+di\s+condono\s+(?:(?:e|è|risulta)\s+)?(?:ancora\s+)?apert\w*\b", re.I)),
    ("building_abuse", re.compile(r"\babuso\s+edilizi\w*\b|\babusi\s+edilizi\w*\b", re.I)),
    ("grave_nonconformity", re.compile(r"\bnon\s+conforme\s*/\s*grave\b|\bdifformit[aà]\s+grav\w*\b|\birregolarit[aà]\s+urbanistic\w*\s+grav\w*\b|\baccertamento\s+di\s+conformit[aà]\s+(?:negativ\w*|impossibil\w*)\b", re.I)),
    ("use_mismatch", re.compile(r"\bdestinazione\s+(?:d['’]\s*uso\s+)?(?:legittimat\w*\s+)?non\s+residenzial\w*\b|\bdestinazione\s+legittimat\w*\s+(?:(?:e|è|risulta)\s+)?(?:a\s+)?(?:cantina|locale\s+accessorio)\b|\b(?:cantina|locale\s+accessorio)\b.{0,100}\b(?:usat\w*|utilizzat\w*)\b.{0,60}\b(?:abitazione|residenz\w*)\b|\bdiversa\s+destinazione\s+d['’]\s*uso\b", re.I | re.S)),
    ("fiscalizzazione", re.compile(r"\bfiscalizzazion\w*\b", re.I)),
    ("irreversible_outcome", re.compile(r"\b(?:solo|unico\s+esito|unica\s+soluzione|necessari\w*|obbligator\w*|dovr[aà])\b.{0,80}\b(?:ripristin\w*|demolizion\w*)\b|\b(?:ripristin\w*|demolizion\w*)\b.{0,80}\b(?:unico\s+esito|unica\s+soluzione|necessari\w*|obbligator\w*)\b", re.I | re.S)),
)

_WEAK_PATTERNS: Sequence[Tuple[str, re.Pattern[str]]] = (
    ("regularization_required", re.compile(r"\b(?:accertamento\s+di\s+conformit[aà]|regolarizzazion\w*)\s+(?:richiest\w*|necessari\w*|da\s+verificare)\b", re.I)),
)

_AGIBILITA_RE = re.compile(r"\b(?:agibilit[aà]|abitabilit[aà])\b.{0,70}\b(?:assent\w*|non\s+rilasciat\w*|mancant\w*)\b|\bnon\s+risulta\s+rilasciat\w*\b.{0,70}\bcertificat\w*\s+di\s+agibilit[aà]\b", re.I | re.S)
_OCCUPANCY_RE = re.compile(r"\b(?:occupazion\w*|occupat\w*|opponibil\w*)\b", re.I)
_COST_RE = re.compile(r"\b(?:costi?\s+acquirente|spese?\s+condominial\w*|oneri?\s+economici?)\b", re.I)
_FORMALITY_RE = re.compile(r"\b(?:pignorament\w*|ipotec\w*|formalit[aà]|servit[uù]|accesso)\b", re.I)
_URBAN_TOPIC_RE = re.compile(r"\b(?:urbanistic\w*|ediliz\w*|difformit[aà]|sanatori\w*|condono|commerciabil\w*|accertamento\s+di\s+conformit[aà]|regolarizzazion\w*|destinazione\s+d['’]\s*uso|cantina|locale\s+accessorio|fiscalizzazion\w*|ripristin\w*|demolizion\w*)\b", re.I)

_FALSE_POSITIVE_PATTERNS: Sequence[re.Pattern[str]] = (
    re.compile(r"\b(?:nessun|nessuno|assenza\s+di|non\s+sono\s+presenti|non\s+risultano)\b.{0,80}\babusi?\s+edilizi\w*\b", re.I | re.S),
    re.compile(r"\b(?:pienamente|risulta|e|è)\s+conforme\b", re.I),
    re.compile(r"\bsanatoria\s+(?:rilasciat\w*|conclus\w*|definit\w*)\b", re.I),
)


# Roots whose text may inform the severity decision but must never surface as
# customer-facing evidence (internal QA critique lives here).
_SIGNAL_ONLY_ROOTS = frozenset({"qa_gate"})

# Defensive guard: drop any bullet text that reads like internal QA/validator
# reasoning rather than document-backed evidence, regardless of source root.
_INTERNAL_CRITIQUE_RE = re.compile(
    r"alzare\s+severit|"
    r"classificazione\s+troppo\s+morbid|troppo\s+morbid|"
    r"sostituire\s+(?:l['’]\s*)?evidenz|"
    r"recommended_action|"
    r"section_verdicts|"
    r"\bcontradiction\b|contraddizion\w*\s+rilevat|"
    r"\bvalidator\b|"
    r"\bqa[_\s]?gate\b|"
    r"\bcorrection\b|"
    r"\bdebug\b",
    re.I,
)


def _is_internal_critique_text(value: Any) -> bool:
    return bool(_INTERNAL_CRITIQUE_RE.search(str(value or "")))


def _clean(value: Any, limit: int = 900) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()[:limit]


def _normalized(value: Any) -> str:
    text = _clean(value).lower()
    return text.replace("à", "a").replace("è", "e").replace("é", "e").replace("ì", "i").replace("ò", "o").replace("ù", "u")


def _normalize_evidence(values: Iterable[Any], limit: int = 12) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen = set()
    for item in values:
        if not isinstance(item, dict):
            continue
        quote = _clean(item.get("quote"), 700)
        if not quote:
            continue
        try:
            page = int(item.get("page"))
        except Exception:
            page = None
        sig = (page, quote)
        if sig in seen:
            continue
        seen.add(sig)
        normalized = copy.deepcopy(item)
        normalized["quote"] = quote
        if page is not None:
            normalized["page"] = page
        out.append(normalized)
        if len(out) >= limit:
            break
    return out


def _dict_evidence(value: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw: List[Any] = []
    for key in _EVIDENCE_KEYS:
        candidate = value.get(key)
        if isinstance(candidate, list):
            raw.extend(candidate)
    return _normalize_evidence(raw)


def _walk_structured(value: Any, path: str, inherited_evidence: Optional[List[Dict[str, Any]]] = None) -> Iterable[Tuple[str, str, List[Dict[str, Any]]]]:
    inherited_evidence = inherited_evidence or []
    if isinstance(value, dict):
        local_evidence = _normalize_evidence([*inherited_evidence, *_dict_evidence(value)])
        for key, child in value.items():
            key_text = str(key)
            if key_text in _SKIP_KEYS or key_text.startswith("_") or "debug" in key_text.lower():
                continue
            if key_text in _EVIDENCE_KEYS and isinstance(child, list):
                for index, evidence in enumerate(child):
                    if isinstance(evidence, dict) and evidence.get("quote"):
                        yield f"{path}.{key_text}[{index}].quote", _clean(evidence.get("quote")), _normalize_evidence([evidence])
                continue
            yield from _walk_structured(child, f"{path}.{key_text}", local_evidence)
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            yield from _walk_structured(item, f"{path}[{index}]", inherited_evidence)
        return
    if isinstance(value, str):
        text = _clean(value)
        if text:
            yield path, text, inherited_evidence


def _matches(patterns: Sequence[Tuple[str, re.Pattern[str]]], text: str) -> List[str]:
    if any(pattern.search(text) for pattern in _FALSE_POSITIVE_PATTERNS):
        return []
    return [name for name, pattern in patterns if pattern.search(text)]


def _append_unique(values: List[str], value: str, limit: int = 12) -> None:
    key = _normalized(value)
    if not key or any(_normalized(existing) == key for existing in values):
        return
    if len(values) < limit:
        values.append(_clean(value, 360))


def detect_urbanistic_customer_issue(result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(result, dict):
        return None
    critical: List[str] = []
    severe: List[str] = []
    weak: List[str] = []
    evidence: List[Dict[str, Any]] = []
    bullets: List[str] = []
    sources: List[str] = []
    signal_names: List[str] = []

    existing_issue: Optional[Dict[str, Any]] = None
    for candidate in result.get("issues") if isinstance(result.get("issues"), list) else []:
        if isinstance(candidate, dict) and candidate.get("issue_id") == ISSUE_ID:
            existing_issue = copy.deepcopy(candidate)
            break
    scan_roots = _PRIMARY_EVIDENCE_ROOTS if existing_issue else _SCAN_ROOTS

    for root in scan_roots:
        if root not in result:
            continue
        # qa_gate carries internal QA critique (recommended_action, section
        # verdicts, contradiction notes). It may inform the severity decision but
        # its text must never become customer-facing evidence.
        signal_only_root = root in _SIGNAL_ONLY_ROOTS
        for path, text, local_evidence in _walk_structured(result[root], root):
            critical_names = _matches(_CRITICAL_PATTERNS, text)
            severe_names = _matches(_SEVERE_PATTERNS, text)
            weak_names = _matches(_WEAK_PATTERNS, text)
            if not critical_names and not severe_names and not weak_names:
                continue
            bucket = critical if critical_names else severe if severe_names else weak
            _append_unique(bucket, text)
            for name in [*critical_names, *severe_names, *weak_names]:
                if name not in signal_names:
                    signal_names.append(name)
            # Visible evidence bullets/quotes/sources only from document-backed roots,
            # and never internal-critique wording.
            if signal_only_root or _is_internal_critique_text(text):
                continue
            _append_unique(bullets, text)
            sources.append(path)
            evidence = _normalize_evidence([*evidence, *local_evidence])

    if not critical and not severe and not weak:
        return existing_issue

    is_critical = bool(critical)
    is_severe = bool(critical or severe)
    classification = "blocker" if is_critical else "severe_risk_to_verify" if severe else "risk_to_verify"
    badge = "Blocco critico" if is_critical else "Rischio grave" if severe else "Rischio da verificare"
    severity = "BLOCKER" if is_critical else "RED" if severe else "AMBER"
    title = (
        "Urbanistica: non conformità grave / commerciabilità limitata"
        if is_critical
        else "Urbanistica: non conformità grave"
        if severe
        else "Urbanistica: regolarizzazione da verificare"
    )

    detail_sentences: List[str] = []
    combined = " ".join([*critical, *severe, *weak])
    if re.search(r"\bnon\s+(?:e\s+|è\s+|risulta\s+)?commerciabil", combined, re.I):
        detail_sentences.append("La perizia indica che l'immobile non è commerciabile al di fuori della vendita forzata.")
    elif "forced_sale_only" in signal_names:
        detail_sentences.append("La perizia indica una commerciabilità limitata alla vendita forzata.")
    if "sanatoria_unresolved" in signal_names:
        detail_sentences.append("La concessione/sanatoria risulta non rilasciata o non conclusa.")
    if "condono_unresolved" in signal_names:
        detail_sentences.append("La pratica di condono risulta pendente o non definita.")
    if "use_mismatch" in signal_names:
        detail_sentences.append("La destinazione legittimata risulta non residenziale, cantina o locale accessorio.")
    if "building_abuse" in signal_names:
        detail_sentences.append("La perizia segnala un abuso edilizio.")
    if "grave_nonconformity" in signal_names:
        detail_sentences.append("La perizia qualifica la situazione come NON CONFORME / GRAVE.")

    if is_severe:
        summary = " ".join(detail_sentences) or "La perizia segnala una criticità urbanistica rilevante e potenzialmente incidente sulla commerciabilità."
        summary += " Verificare con tecnico e legale prima dell'offerta."
    else:
        summary = "La perizia richiede una verifica di regolarizzazione o conformità urbanistica prima dell'offerta."

    pages = sorted({int(item["page"]) for item in evidence if isinstance(item.get("page"), int)})
    return {
        "issue_id": ISSUE_ID,
        "family": "urbanistica",
        "theme": "urbanistica",
        "status": "FOUND",
        "severity": severity,
        "classification": classification,
        "badge_it": badge,
        "severity_label_it": badge,
        "status_it": badge,
        "fact_status_it": "Fatto dichiarato dal perito",
        "priority": 0 if is_critical else 10 if severe else 50,
        "headline_it": title,
        "killer": title,
        "flag_it": title,
        "explanation_it": summary,
        "summary_it": summary,
        "action": "Verificare con tecnico e legale prima dell'offerta.",
        "action_it": "Verificare con tecnico e legale prima dell'offerta.",
        "verify_next_it": "Verificare titoli, sanabilità, destinazione d'uso e commerciabilità prima dell'offerta.",
        "evidence": evidence,
        "evidence_bullets": bullets,
        "supporting_pages": pages,
        "pages": pages,
        "evidence_note_it": None if evidence else "Fonte da verificare nel documento.",
        "signal_groups": signal_names,
        "source_paths": sorted(set(sources))[:20],
        "is_blocker": is_critical,
        "is_legal_killer": is_critical,
    }


def _blob(item: Any) -> str:
    if isinstance(item, dict):
        parts = []
        for key in ("family", "theme", "category", "headline_it", "killer", "flag_it", "title_it", "explanation_it", "summary_it", "action", "action_it"):
            if item.get(key):
                parts.append(str(item[key]))
        for evidence in item.get("evidence") if isinstance(item.get("evidence"), list) else []:
            if isinstance(evidence, dict):
                parts.append(str(evidence.get("quote") or ""))
        return " ".join(parts)
    return str(item or "")


def _is_urban_card(item: Any) -> bool:
    if not isinstance(item, dict):
        return False
    if item.get("issue_id") == ISSUE_ID:
        return True
    family = _normalized(item.get("family") or item.get("theme") or item.get("category"))
    if family in {"agibilita", "occupancy", "occupazione", "opponibilita", "costs", "formalities", "legal_background"}:
        return False
    return family in {"urbanistica", "urbanistic"} or bool(_URBAN_TOPIC_RE.search(_blob(item)))


def customer_issue_priority(item: Any) -> Tuple[int, int, str]:
    blob = _blob(item)
    classification = _normalized(item.get("classification") if isinstance(item, dict) else "")
    severity = str(item.get("severity") or item.get("status") or "").upper() if isinstance(item, dict) else ""
    if _is_urban_card(item) and (classification in {"critical_blocker", "blocker"} or severity == "BLOCKER" or _matches(_CRITICAL_PATTERNS, blob)):
        rank = 0
    elif _is_urban_card(item) and (classification == "severe_risk_to_verify" or severity == "RED" or _matches(_SEVERE_PATTERNS, blob)):
        rank = 1
    elif _AGIBILITA_RE.search(blob):
        rank = 2
    elif _OCCUPANCY_RE.search(blob):
        rank = 3
    elif _COST_RE.search(blob):
        rank = 4
    elif _FORMALITY_RE.search(blob):
        rank = 5
    else:
        rank = 6
    severity_rank = {"BLOCKER": 0, "RED": 1, "AMBER": 2, "GIALLO": 2, "INFO": 3}.get(severity, 4)
    return rank, severity_rank, _normalized(blob)[:120]


def _merge_existing_evidence(issue: Dict[str, Any], cards: Iterable[Any]) -> Dict[str, Any]:
    evidence = list(issue.get("evidence") or [])
    bullets = list(issue.get("evidence_bullets") or [])
    for card in cards:
        if not isinstance(card, dict) or not _is_urban_card(card):
            continue
        if isinstance(card.get("evidence"), list):
            evidence.extend(card["evidence"])
        for key in ("headline_it", "killer", "flag_it", "explanation_it"):
            if card.get(key) and not _is_internal_critique_text(card[key]):
                _append_unique(bullets, str(card[key]))
    evidence = [
        item for item in evidence
        if not (isinstance(item, dict) and _is_internal_critique_text(item.get("quote")))
    ]
    issue["evidence"] = _normalize_evidence(evidence)
    issue["evidence_bullets"] = [b for b in bullets if not _is_internal_critique_text(b)][:12]
    issue["supporting_pages"] = sorted({int(item["page"]) for item in issue["evidence"] if isinstance(item.get("page"), int)})
    issue["pages"] = list(issue["supporting_pages"])
    if issue["evidence"]:
        issue["evidence_note_it"] = None
    return issue


def _replace_urban_card(items: Any, card: Dict[str, Any], *, kind: str) -> List[Dict[str, Any]]:
    source = [copy.deepcopy(item) for item in items if isinstance(item, dict)] if isinstance(items, list) else []
    card = _merge_existing_evidence(copy.deepcopy(card), source)
    kept = [item for item in source if not _is_urban_card(item)]
    if kind == "legal":
        promoted = copy.deepcopy(card)
        promoted["category"] = "urbanistica"
        promoted["finding_status"] = "FOUND"
        promoted["status"] = "RED" if card["severity"] == "BLOCKER" else card["severity"]
    elif kind == "flag":
        promoted = {
            "code": "URBANISTICA_CUSTOMER_PRIORITY",
            "severity": card["severity"],
            "classification": card["classification"],
            "badge_it": card["badge_it"],
            "fact_status_it": card["fact_status_it"],
            "flag_it": card["headline_it"],
            "action_it": card["summary_it"],
            "evidence": copy.deepcopy(card["evidence"]),
            "evidence_bullets": copy.deepcopy(card["evidence_bullets"]),
            "category": "urbanistica",
            "issue_id": ISSUE_ID,
        }
    else:
        promoted = copy.deepcopy(card)
    return sorted([promoted, *kept], key=customer_issue_priority)


def _prepend(existing: Any, prefix: str, limit: int = 1800) -> str:
    current = _clean(existing, limit)
    if _normalized(prefix) in _normalized(current):
        return current
    return _clean(f"{prefix} {current}", limit)


def _inject_root_surfaces(result: Dict[str, Any], issue: Dict[str, Any]) -> None:
    result["issues"] = _replace_urban_card(result.get("issues"), issue, kind="issue")

    section9 = copy.deepcopy(result.get("section_9_legal_killers")) if isinstance(result.get("section_9_legal_killers"), dict) else {}
    combined = []
    for key in ("items", "top_items"):
        if isinstance(section9.get(key), list):
            combined.extend(section9[key])
    section9["items"] = _replace_urban_card(combined, issue, kind="legal")
    section9["top_items"] = copy.deepcopy(section9["items"][:3])
    section9["section_label_it"] = "Rischi e punti critici"
    result["section_9_legal_killers"] = section9

    flags_source = result.get("section_11_red_flags") if isinstance(result.get("section_11_red_flags"), list) else result.get("red_flags_operativi")
    flags = _replace_urban_card(flags_source, issue, kind="flag")
    result["section_11_red_flags"] = copy.deepcopy(flags)
    result["red_flags_operativi"] = copy.deepcopy(flags)

    top_blocker = {
        "issue_id": ISSUE_ID,
        "key": "urbanistica",
        "label_it": issue["headline_it"],
        "status": "FOUND",
        "severity": issue["severity"],
        "classification": issue["classification"],
        "badge_it": issue["badge_it"],
        "fact_status_it": issue["fact_status_it"],
        "supporting_pages": copy.deepcopy(issue["supporting_pages"]),
        "evidence": copy.deepcopy(issue["evidence"]),
    }
    semaforo = copy.deepcopy(result.get("section_1_semaforo_generale")) if isinstance(result.get("section_1_semaforo_generale"), dict) else copy.deepcopy(result.get("semaforo_generale")) if isinstance(result.get("semaforo_generale"), dict) else {}
    existing_top = semaforo.get("top_blockers") if isinstance(semaforo.get("top_blockers"), list) else []
    semaforo["top_blockers"] = sorted([top_blocker, *[item for item in existing_top if isinstance(item, dict) and item.get("issue_id") != ISSUE_ID]], key=customer_issue_priority)[:6]
    if issue["severity"] in {"BLOCKER", "RED"}:
        semaforo.update({"status": "RED", "status_it": "CRITICO", "status_en": "RED", "reason_it": issue["headline_it"]})
    result["section_1_semaforo_generale"] = copy.deepcopy(semaforo)
    result["semaforo_generale"] = copy.deepcopy(semaforo)

    summary = issue["summary_it"]
    section2 = copy.deepcopy(result.get("section_2_decisione_rapida")) if isinstance(result.get("section_2_decisione_rapida"), dict) else {}
    section2["summary_it"] = _prepend(section2.get("summary_it"), summary)
    section2["decisione_rapida_it"] = _prepend(section2.get("decisione_rapida_it"), summary)
    section2["main_risk_it"] = issue["headline_it"]
    ids = [str(value) for value in section2.get("issue_ids", []) if value]
    section2["issue_ids"] = [ISSUE_ID, *[value for value in ids if value != ISSUE_ID]]
    result["section_2_decisione_rapida"] = section2

    decision = copy.deepcopy(result.get("decision_rapida_client")) if isinstance(result.get("decision_rapida_client"), dict) else {}
    decision["summary_it"] = _prepend(decision.get("summary_it"), summary)
    decision["decisione_rapida_it"] = _prepend(decision.get("decisione_rapida_it"), summary)
    decision["main_risk_it"] = issue["headline_it"]
    decision["risk_level"] = "RED" if issue["severity"] in {"BLOCKER", "RED"} else decision.get("risk_level", "AMBER")
    decision["risk_level_it"] = "CRITICO" if issue["severity"] in {"BLOCKER", "RED"} else decision.get("risk_level_it", "ATTENZIONE")
    driver = decision.get("driver_rosso") if isinstance(decision.get("driver_rosso"), list) else []
    decision["driver_rosso"] = [top_blocker, *[item for item in driver if not isinstance(item, dict) or item.get("issue_id") != ISSUE_ID]][:6]
    ids = [str(value) for value in decision.get("issue_ids", []) if value]
    decision["issue_ids"] = [ISSUE_ID, *[value for value in ids if value != ISSUE_ID]]
    result["decision_rapida_client"] = decision

    bundle = copy.deepcopy(result.get("summary_for_client_bundle")) if isinstance(result.get("summary_for_client_bundle"), dict) else {}
    bundle["main_risk_it"] = issue["headline_it"]
    bundle["decision_summary_it"] = _prepend(bundle.get("decision_summary_it"), summary)
    bundle["summary_it"] = _prepend(bundle.get("summary_it"), summary)
    result["summary_for_client_bundle"] = bundle
    summary_for_client = copy.deepcopy(result.get("summary_for_client")) if isinstance(result.get("summary_for_client"), dict) else {}
    summary_for_client["summary_it"] = _prepend(summary_for_client.get("summary_it"), summary)
    result["summary_for_client"] = summary_for_client

    panoramica = result.get("panoramica_contract")
    if isinstance(panoramica, dict):
        panoramica["main_risk_it"] = issue["headline_it"]
        panoramica["priority_issue"] = copy.deepcopy(top_blocker)


def promote_severe_urbanistic_customer_warning(result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    issue = detect_urbanistic_customer_issue(result)
    if not issue:
        for key in ("issues", "section_9_legal_killers", "section_11_red_flags", "red_flags_operativi"):
            value = result.get(key)
            if isinstance(value, list):
                result[key] = sorted(value, key=customer_issue_priority)
            elif isinstance(value, dict):
                for list_key in ("items", "top_items"):
                    if isinstance(value.get(list_key), list):
                        value[list_key] = sorted(value[list_key], key=customer_issue_priority)
        cdc = result.get("customer_decision_contract")
        if isinstance(cdc, dict):
            for key in ("issues", "section_9_legal_killers", "section_11_red_flags", "red_flags_operativi"):
                if key in result and key in cdc:
                    cdc[key] = copy.deepcopy(result[key])
        return None

    _inject_root_surfaces(result, issue)
    cdc = result.get("customer_decision_contract")
    if isinstance(cdc, dict):
        cdc["issues"] = copy.deepcopy(result["issues"])
        cdc["semaforo_generale"] = copy.deepcopy(result["semaforo_generale"])
        cdc["section_1_semaforo_generale"] = copy.deepcopy(result["section_1_semaforo_generale"])
        cdc["section_2_decisione_rapida"] = copy.deepcopy(result["section_2_decisione_rapida"])
        cdc["decision_rapida_client"] = copy.deepcopy(result["decision_rapida_client"])
        cdc["summary_for_client_bundle"] = copy.deepcopy(result["summary_for_client_bundle"])
        cdc["section_9_legal_killers"] = copy.deepcopy(result["section_9_legal_killers"])
        cdc["red_flags_operativi"] = copy.deepcopy(result["red_flags_operativi"])
        cdc["section_11_red_flags"] = copy.deepcopy(result["section_11_red_flags"])
    return issue
