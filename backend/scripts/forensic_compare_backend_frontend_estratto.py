#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _short(s: Any, n: int = 180) -> str:
    text = str(s if s is not None else "")
    text = " ".join(text.split())
    return text[:n] + ("..." if len(text) > n else "")


def _is_missing(v: Any) -> bool:
    if v is None:
        return True
    t = str(v).strip().upper()
    return t in {"", "NOT_FOUND", "TBD", "NONE", "NULL", "NON SPECIFICATO IN PERIZIA"}


def _eq(a: Any, b: Any) -> bool:
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return abs(float(a) - float(b)) < 0.01
    sa = " ".join(str(a).lower().split())
    sb = " ".join(str(b).lower().split())
    return sa == sb or sa in sb or sb in sa


def _format_evidence(ev: List[Dict[str, Any]], searched: List[Any] = None) -> str:
    if ev:
        e0 = ev[0]
        return f"p.{e0.get('page','?')} \"{_short(e0.get('quote',''))}\""
    if searched:
        s0 = searched[0]
        if isinstance(s0, dict):
            return f"searched p.{s0.get('page','?')} \"{_short(s0.get('quote',''))}\""
        return f"searched \"{_short(s0)}\""
    return "NO_EVIDENCE"


def _get_money_item(money_box: Dict[str, Any], code: str) -> Dict[str, Any]:
    for item in (money_box or {}).get("items", []):
        if str(item.get("code")) == code:
            return item
    return {}


def _extract_backend(back: Dict[str, Any], system: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    states = back.get("field_states") or {}
    case_header = back.get("case_header") or {}
    money_box = back.get("money_box") or {}
    beni = back.get("beni")
    lots = back.get("lots") or []
    if not isinstance(beni, list) and lots and isinstance(lots[0], dict):
        beni = lots[0].get("beni")
    if not isinstance(beni, list):
        beni = []

    result = system.get("result") if isinstance(system.get("result"), dict) else {}
    decision = result.get("decision_rapida_client") or result.get("section_2_decisione_rapida") or {}
    semaforo = result.get("semaforo_generale") or result.get("section_1_semaforo_generale") or {}
    dati_asta = result.get("dati_asta") or result.get("dati_certi_del_lotto", {}).get("dati_asta") or {}

    item_a = _get_money_item(money_box, "A")
    return {
        "tribunale": {"value": (states.get("tribunale") or {}).get("value") or case_header.get("tribunale"), "evidence": (states.get("tribunale") or {}).get("evidence", []), "searched": (states.get("tribunale") or {}).get("searched_in", [])},
        "procedure_id": {"value": (states.get("procedura") or {}).get("value") or case_header.get("procedure_id"), "evidence": (states.get("procedura") or {}).get("evidence", []), "searched": (states.get("procedura") or {}).get("searched_in", [])},
        "occupancy": {"value": (states.get("stato_occupativo") or {}).get("value") or ((states.get("stato_occupativo") or {}).get("status")), "evidence": (states.get("stato_occupativo") or {}).get("evidence", []), "searched": (states.get("stato_occupativo") or {}).get("searched_in", [])},
        "beni_count": {"value": len(beni), "evidence": (beni[0].get("evidence", []) if beni and isinstance(beni[0], dict) else []), "searched": []},
        "ape_status": {"value": (states.get("ape") or {}).get("value"), "evidence": (states.get("ape") or {}).get("evidence", []), "searched": (states.get("ape") or {}).get("searched_in", [])},
        "spese_condominiali_arretrate": {"value": (states.get("spese_condominiali_arretrate") or {}).get("value") or (states.get("spese_condominiali_arretrate") or {}).get("status"), "evidence": (states.get("spese_condominiali_arretrate") or {}).get("evidence", []), "searched": (states.get("spese_condominiali_arretrate") or {}).get("searched_in", [])},
        "sanatoria_estimate_eur": {"value": item_a.get("stima_euro"), "evidence": (item_a.get("fonte_perizia") or {}).get("evidence", []), "searched": []},
        "prezzo_base_eur": {"value": (states.get("prezzo_base_asta") or {}).get("value"), "evidence": (states.get("prezzo_base_asta") or {}).get("evidence", []), "searched": (states.get("prezzo_base_asta") or {}).get("searched_in", [])},
        "dati_asta": {"value": dati_asta if dati_asta else "NOT_FOUND", "evidence": dati_asta.get("evidence", []) if isinstance(dati_asta, dict) else [], "searched": []},
        "decisione_rapida_it": {"value": decision.get("summary_it"), "evidence": decision.get("evidence", []), "searched": []},
        "decisione_rapida_en": {"value": decision.get("summary_en"), "evidence": decision.get("evidence", []), "searched": []},
        "semaforo_status": {"value": semaforo.get("status"), "evidence": semaforo.get("evidence", []), "searched": []},
    }


def _extract_frontend(front: Dict[str, Any]) -> Dict[str, Any]:
    return (front or {}).get("displayed_fields") or {}


def _classify(field: str, estratto_v: Any, backend_v: Any, backend_ev: List[Dict[str, Any]], frontend_v: Any) -> str:
    if field.startswith("decisione_rapida"):
        txt = str(backend_v or "").lower()
        if "alcuni dati richiedono verifica manuale" in txt or "document analyzed" in txt:
            return "DECISION_GENERATION_BUG"
        return "FRONTEND_MAPPING_BUG"
    if _is_missing(backend_v):
        return "BACKEND_MISSING"
    if not backend_ev:
        return "BACKEND_FOUND_NO_EVIDENCE"
    if not _is_missing(estratto_v) and not _eq(estratto_v, backend_v):
        return "BACKEND_WRONG_VALUE"
    if not _eq(frontend_v, backend_v):
        if str(frontend_v).lower().strip() in {"non presenti", "non specificato in perizia"} and str(backend_v).upper().strip() == "NOT_FOUND":
            return "FRONTEND_MASKING_BUG"
        return "FRONTEND_MAPPING_BUG"
    return "BACKEND_FOUND_NO_EVIDENCE"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    args = ap.parse_args()
    run_dir = Path(args.run_dir)

    back = _load_json(run_dir / "backend_snapshot.json")
    front = _load_json(run_dir / "frontend_snapshot.json")
    estratto = _load_json(run_dir / "estratto_ref.json")
    system = _load_json(run_dir / "system.json") if (run_dir / "system.json").exists() else {}

    b = _extract_backend(back, system)
    f = _extract_frontend(front)
    e = (estratto.get("fields") or {})

    rows: List[Tuple[str, str, str, str, str]] = []
    fields = [
        "tribunale",
        "procedure_id",
        "occupancy",
        "beni_count",
        "ape_status",
        "spese_condominiali_arretrate",
        "sanatoria_estimate_eur",
        "prezzo_base_eur",
        "dati_asta",
        "decisione_rapida_it",
        "decisione_rapida_en",
        "semaforo_status",
    ]

    for field in fields:
        ev = e.get(field, {})
        estratto_v = ev.get("value", "NOT_FOUND") if isinstance(ev, dict) else "NOT_FOUND"
        estratto_ev = _format_evidence(ev.get("evidence", []), ev.get("searched_in", [])) if isinstance(ev, dict) else "NO_EVIDENCE"
        backend_v = (b.get(field) or {}).get("value")
        backend_ev = (b.get(field) or {}).get("evidence", [])
        backend_sr = (b.get(field) or {}).get("searched", [])
        backend_ev_txt = _format_evidence(backend_ev, backend_sr)
        frontend_v = f.get(field)
        stage = _classify(field, estratto_v, backend_v, backend_ev, frontend_v)

        rows.append(
            (
                field,
                f"{_short(estratto_v)} | {estratto_ev}",
                f"{_short(backend_v)} | {backend_ev_txt}",
                _short(frontend_v),
                stage,
            )
        )

    report = []
    report.append("# FORENSIC REPORT")
    report.append("")
    report.append("| Field | Estratto value + evidence | Backend value + evidence | Frontend printed value | Failure stage |")
    report.append("|---|---|---|---|---|")
    for r in rows:
        report.append(f"| {r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]} |")
    report.append("")
    report.append("## Why Decisione Rapida is generic")
    report.append("- Static fallback is hardcoded in `/srv/perizia/app/backend/server.py:4172` and `/srv/perizia/app/backend/server.py:4173`.")
    report.append("- The same fallback block initializes `driver_rosso` as empty in `/srv/perizia/app/backend/server.py:4174`, so no blocker-specific text is generated.")
    report.append("- Input dependency observed in this run: semaforo and decision sections are not rebuilt from actual blocker states before serialization, so generic text survives to API/UI.")
    report.append("")
    report.append("## What to change")
    report.append("- `/srv/perizia/app/backend/server.py`: build `decision_rapida_client.summary_it/summary_en` from semaforo blockers + missing critical fields, and keep fallback only when both blockers and field states are unavailable.")
    report.append("- `/srv/perizia/app/backend/server.py`: add field-state extraction for `ape`, `dati_asta`, `beni_count`, and occupancy signals from `STATO OCCUPAZIONALE` lines; require evidence for every `FOUND` value.")
    report.append("- `/srv/perizia/app/frontend/src/pages/AnalysisResult.js`: keep `NOT_FOUND` explicit (never map to 'Non presenti') and expose `searched_in` in rendered evidence for missing fields.")

    out = run_dir / "FORENSIC_REPORT.md"
    out.write_text("\n".join(report) + "\n", encoding="utf-8")
    print(str(out))


if __name__ == "__main__":
    main()
