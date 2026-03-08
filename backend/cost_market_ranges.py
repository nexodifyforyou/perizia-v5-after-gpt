from typing import Dict, Optional


BASE_MARKET_RANGES: Dict[str, Dict[str, float]] = {
    "B": {"min": 800.0, "max": 2500.0},
    "C": {"min": 0.0, "max": 15000.0},
    "D": {"min": 500.0, "max": 2500.0},
    "F": {"min": 2000.0, "max": 8000.0},
    "G": {"min": 0.0, "max": 2000.0},
}


def market_range_for_item(
    *,
    code: str,
    occupancy_status: str,
    spese_status: str,
) -> Optional[Dict[str, float]]:
    item_code = str(code or "").strip().upper()
    if item_code in BASE_MARKET_RANGES:
        return dict(BASE_MARKET_RANGES[item_code])
    if item_code == "H":
        return {"min": 3000.0, "max": 15000.0} if "OCCUP" in occupancy_status else {"min": 0.0, "max": 0.0}
    if item_code == "E":
        return {"min": 0.0, "max": 0.0} if "NON PRESENTI" in spese_status else {"min": 0.0, "max": 3000.0}
    return None
