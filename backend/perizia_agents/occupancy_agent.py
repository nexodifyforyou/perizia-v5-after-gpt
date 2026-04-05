from __future__ import annotations

import re

from perizia_runtime.state import Judgment, RuntimeState
from perizia_tools.evidence_span_tool import make_evidence
from perizia_tools.section_router_tool import classify_section_type


_PROPERTY_OCCUPANCY_ANCHORS = [
    "stato di occupazione",
    "stato occupativo",
    "occupazione dell'immobile",
    "occupazione del bene",
    "immobile",
    "bene",
    "unità",
    "unita",
    "porzione",
]

_STRONG_FREE_OCCUPANCY_PHRASES = [
    "non appariva occupato",
    "non risultava occupato",
    "non risulta occupato",
    "non occupato",
    "immobile libero",
    "l'immobile risulta libero",
    "risulta libero",
    "bene libero",
    "unità libera",
    "unita libera",
    "libero da persone",
    "libero da cose",
]

_TENURE_SIGNALS = [
    "contratto di locazione",
    "locazione registrata",
    "condotto in locazione",
    "comodato",
    "occupato da",
    "locato",
    "locazione",
]

_TENURE_NEGATIONS = [
    "non risultano registrati atti di locazione",
    "non risultano registrati atti di locazione e/o comodato",
    "assenza di contratto di locazione",
    "assenza di locazione",
]

_TENURE_REGEX = re.compile(r"\b(?:locaz\w*|locat\w*|comodat\w*)\b", re.IGNORECASE)


def _is_non_property_libero_noise(quote: str) -> bool:
    low = quote.lower()
    return any(
        marker in low
        for marker in [
            "@libero.it",
            ".libero.it",
            "stato libero",
            "regime patrimoniale",
            "separazione legale dei beni",
        ]
    )


def _has_tenure_signal(quote: str) -> bool:
    low = quote.lower()
    return any(marker in low for marker in _TENURE_SIGNALS)


def _has_negated_tenure_signal(quote: str) -> bool:
    low = quote.lower()
    return any(marker in low for marker in _TENURE_NEGATIONS)


def _has_property_occupancy_anchor(quote: str) -> bool:
    low = quote.lower()
    return any(marker in low for marker in _PROPERTY_OCCUPANCY_ANCHORS)


def _has_valid_free_occupancy_anchor(quote: str) -> bool:
    low = quote.lower()
    return any(marker in low for marker in _STRONG_FREE_OCCUPANCY_PHRASES) or (
        "nessuno" in low and _has_property_occupancy_anchor(quote)
    )


def _infer_opponibilita(value: str, quote: str) -> str:
    if value != "OCCUPATO":
        return "NON VERIFICABILE"
    low = quote.lower()
    if "occupato da" in low or "senza titolo" in low:
        return "NON VERIFICABILE"
    if _has_tenure_signal(quote):
        return "LOCAZIONE DA VERIFICARE"
    return "NON VERIFICABILE"


def _occupancy_confidence(quote: str, value: str) -> float:
    low = quote.lower()
    if value == "OCCUPATO":
        if "stato di occupazione" in low or "occupato da" in low:
            return 0.96
        if _has_tenure_signal(quote):
            return 0.84
        return 0.7
    if "non appariva occupato" in low or "nessuno" in low:
        return 0.92
    if "stato di occupazione" in low or "immobile libero" in low or "bene libero" in low:
        return 0.9
    return 0.58


def run_occupancy_agent(state: RuntimeState) -> None:
    candidates = []
    for idx, page in enumerate(state.pages, start=1):
        text = str((page or {}).get("text") or "")
        low = text.lower()
        page_number = int((page or {}).get("page_number") or (page or {}).get("page") or idx)
        if "coefficiente di locazione" in low:
            candidates.append(
                {
                    "value": "INVALID_OCCUPANCY_SIGNAL",
                    "confidence": 0.0,
                    "valid": False,
                    "reason": "valuation_coefficient_not_valid_occupancy",
                    "evidence": [make_evidence(page_number, text[:520], "valuation_coefficient", [], 0.0)],
                }
            )
        for match in re.finditer(r"\boccupat\w*\b|\bliber\w*\b|\bnessuno\b", text, re.IGNORECASE):
            start = max(0, match.start() - 120)
            end = min(len(text), match.end() + 120)
            quote = text[start:end].strip()
            section_type = classify_section_type(quote)
            match_text = match.group(0).lower()
            has_tenure_signal = _has_tenure_signal(quote)
            if "suolo pubblico occupato" in quote.lower():
                candidates.append(
                    {
                        "value": "INVALID_OCCUPANCY_SIGNAL",
                        "confidence": 0.0,
                        "valid": False,
                        "reason": "public_space_occupancy_not_property_occupancy",
                        "evidence": [make_evidence(page_number, quote, "public_space_occupancy", [], 0.0)],
                    }
                )
                continue
            if section_type == "valuation" or "coefficiente" in quote.lower():
                candidates.append(
                    {
                        "value": "INVALID_OCCUPANCY_SIGNAL",
                        "confidence": 0.0,
                        "valid": False,
                        "reason": "valuation_table_not_valid_occupancy",
                        "evidence": [make_evidence(page_number, quote, "valuation_noise", [], 0.0)],
                    }
                )
                continue
            if _is_non_property_libero_noise(quote):
                candidates.append(
                    {
                        "value": "INVALID_OCCUPANCY_SIGNAL",
                        "confidence": 0.0,
                        "valid": False,
                        "reason": "non_property_libero_noise",
                        "evidence": [make_evidence(page_number, quote, "non_property_noise", [], 0.0)],
                    }
                )
                continue
            if match_text == "nessuno" and not _has_property_occupancy_anchor(quote):
                # Ignore bare "Nessuno" fragments unless the local text clearly ties
                # them to property occupancy; these frequently appear in unrelated tables.
                continue
            if has_tenure_signal and ("nessuno" in quote.lower() or "liber" in quote.lower()) and not _has_valid_free_occupancy_anchor(quote):
                candidates.append(
                    {
                        "value": "INVALID_OCCUPANCY_SIGNAL",
                        "confidence": 0.0,
                        "valid": False,
                        "reason": "weak_libero_blocked_by_tenure_signal",
                        "evidence": [make_evidence(page_number, quote, "tenure_blocks_weak_free", [], 0.0)],
                    }
                )
                continue
            value = None
            confidence = 0.0
            if _has_valid_free_occupancy_anchor(quote):
                value = "LIBERO"
            elif "occupato" in quote.lower():
                value = "OCCUPATO"
            elif "nessuno" in quote.lower() or "liber" in quote.lower():
                # Ignore generic "libero"/"nessuno" fragments that are not clearly tied
                # to property occupancy; they should not become either evidence or errors.
                continue
            if value:
                confidence = _occupancy_confidence(quote, value)
            if value:
                candidates.append(
                    {
                        "value": value,
                        "confidence": confidence,
                        "valid": True,
                        "evidence": [make_evidence(page_number, quote, "occupancy_statement", ["stato_occupativo", "occupancy"], confidence)],
                        "opponibilita": _infer_opponibilita(value, quote),
                    }
                )
        for match in _TENURE_REGEX.finditer(text):
            start = max(0, match.start() - 120)
            end = min(len(text), match.end() + 120)
            quote = text[start:end].strip()
            section_type = classify_section_type(quote)
            if "coefficiente di locazione" in quote.lower():
                continue
            if section_type == "valuation" or "coefficiente" in quote.lower():
                candidates.append(
                    {
                        "value": "INVALID_OCCUPANCY_SIGNAL",
                        "confidence": 0.0,
                        "valid": False,
                        "reason": "valuation_table_not_valid_occupancy",
                        "evidence": [make_evidence(page_number, quote, "valuation_noise", [], 0.0)],
                    }
                )
                continue
            if _has_negated_tenure_signal(quote):
                continue
            if not _has_tenure_signal(quote):
                continue
            if not _has_property_occupancy_anchor(quote):
                continue
            confidence = _occupancy_confidence(quote, "OCCUPATO")
            candidates.append(
                {
                    "value": "OCCUPATO",
                    "confidence": confidence,
                    "valid": True,
                    "evidence": [make_evidence(page_number, quote, "tenure_occupancy_statement", ["stato_occupativo", "occupancy"], confidence)],
                    "opponibilita": _infer_opponibilita("OCCUPATO", quote),
                }
            )
    state.canonical_case.occupancy["candidates"] = candidates
    valid_candidates = [c for c in candidates if c.get("valid")]
    if valid_candidates:
        best = sorted(valid_candidates, key=lambda item: (-float(item.get("confidence", 0.0)), str(item.get("value"))))[0]
        state.judgments["stato_occupativo_verifier"] = Judgment(
            "stato_occupativo_verifier",
            best["value"],
            "FOUND",
            float(best["confidence"]),
            best["evidence"],
            "occupancy resolved from direct state-of-fact evidence only",
        )
        state.canonical_case.occupancy.update(
            {
                "status": best["value"],
                "opponibilita": best.get("opponibilita", "NON VERIFICABILE"),
                "confidence": float(best["confidence"]),
                "evidence": best["evidence"],
                "guards": [
                    "valuation_coefficient_not_valid_occupancy",
                    "public_space_occupancy_not_property_occupancy",
                    "non_property_libero_noise",
                    "bare_nessuno_requires_property_anchor",
                    "tenure_signals_block_weak_libero_inference",
                    "non_verificabile_not_assente",
                ],
            }
        )
    else:
        state.judgments["stato_occupativo_verifier"] = Judgment(
            "stato_occupativo_verifier",
            None,
            "NOT_FOUND",
            0.0,
            [],
            "no valid occupancy evidence survived verifier guards",
        )
        state.canonical_case.occupancy.update(
            {
                "status": None,
                "opponibilita": "NON VERIFICABILE",
                "confidence": 0.0,
                "evidence": [],
                "guards": [
                    "valuation_coefficient_not_valid_occupancy",
                    "public_space_occupancy_not_property_occupancy",
                    "non_property_libero_noise",
                    "bare_nessuno_requires_property_anchor",
                    "tenure_signals_block_weak_libero_inference",
                    "non_verificabile_not_assente",
                ],
            }
        )
