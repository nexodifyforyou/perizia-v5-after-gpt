from __future__ import annotations

import re

from perizia_runtime.state import Judgment, RuntimeState
from perizia_tools.evidence_span_tool import make_evidence
from perizia_tools.section_router_tool import classify_section_type


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


def _occupancy_confidence(quote: str, value: str) -> float:
    low = quote.lower()
    if value == "OCCUPATO":
        if "stato di occupazione" in low or "occupato da" in low:
            return 0.96
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
            value = None
            confidence = 0.0
            if "non appariva occupato" in quote.lower() or "nessuno" in quote.lower() or "liber" in quote.lower():
                value = "LIBERO"
            elif "occupato" in quote.lower():
                value = "OCCUPATO"
            if value:
                confidence = _occupancy_confidence(quote, value)
            if value:
                candidates.append(
                    {
                        "value": value,
                        "confidence": confidence,
                        "valid": True,
                        "evidence": [make_evidence(page_number, quote, "occupancy_statement", ["stato_occupativo", "occupancy"], confidence)],
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
                "opponibilita": "NON VERIFICABILE",
                "confidence": float(best["confidence"]),
                "evidence": best["evidence"],
                "guards": [
                    "valuation_coefficient_not_valid_occupancy",
                    "public_space_occupancy_not_property_occupancy",
                    "non_property_libero_noise",
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
                    "non_verificabile_not_assente",
                ],
            }
        )
