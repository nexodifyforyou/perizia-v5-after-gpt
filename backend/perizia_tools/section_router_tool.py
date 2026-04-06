from __future__ import annotations


def classify_section_type(text: str) -> str:
    low = str(text or "").lower()
    if "coefficiente" in low or "valutazione complessiva" in low or "prezzo a base d'asta" in low:
        return "valuation"
    if "vincoli ed oneri giuridici" in low or "formalità" in low or "pignoramento" in low or "ipoteca" in low:
        return "legal"
    if "diritti reali" in low or "proprietà" in low or "usufrutto" in low or "quota" in low:
        return "rights"
    if "stato dei luoghi" in low or "occupato" in low or "libero" in low or "sopralluogo" in low:
        return "occupancy"
    if "agibilità" in low or "abitabilità" in low:
        return "agibilita"
    if "conformità urbanistico" in low or "regolarizzazione urbanistico" in low:
        return "urbanistica"
    if "conformità catastale" in low or "catasto" in low:
        return "catasto"
    if "spese" in low or "costi" in low or "riduzione cautelativa" in low:
        return "costs"
    return "unknown"

