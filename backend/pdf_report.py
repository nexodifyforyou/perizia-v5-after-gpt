from __future__ import annotations

from datetime import datetime, timezone
from functools import lru_cache
from io import BytesIO
import json
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import KeepTogether, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
from xml.sax.saxutils import escape


TEMPLATE_VERSION = "nexodify_pdf_v2_2026_03"
BRAND_ICON_SOURCE_PATH = "/srv/perizia/app/backend/assets/brand_scale.js"

PLACEHOLDER_VALUES = {
    "",
    "NONE",
    "N/A",
    "NULL",
    "UNDEFINED",
    "UNKNOWN",
    "TBD",
    "NOT_SPECIFIED",
    "NOT_SPECIFIED_IN_PERIZIA",
    "NON SPECIFICATO IN PERIZIA",
    "{}",
    "[]",
}

REQUIRED_SECTIONS = [
    "Header / Cover",
    "Panoramica",
    "Lots / Composizione Lotto",
    "Decisione Rapida",
    "Costi",
    "Legal Killers",
    "Dettagli per bene",
    "Red Flags",
    "Disclaimer / Footer",
]

LEGACY_SECTION_MARKERS = [
    "Checklist Pre-Offerta",
    "Summary for Client",
    "Section 12",
    "Money Box",
]

PALETTE = {
    "bg": colors.HexColor("#0A0A0D"),
    "panel": colors.HexColor("#13141B"),
    "panel_soft": colors.HexColor("#1B1D27"),
    "text": colors.HexColor("#F5F6F9"),
    "muted": colors.HexColor("#B8BDC9"),
    "gold": colors.HexColor("#D6B66A"),
    "gold_soft": colors.HexColor("#6A5A31"),
    "ok": colors.HexColor("#22C55E"),
    "warn": colors.HexColor("#F59E0B"),
    "risk": colors.HexColor("#EF4444"),
}


def _try_register_font() -> str:
    candidates = [
        ("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", "DejaVuSans"),
        ("/usr/share/fonts/truetype/dejavu/DejaVuSansCondensed.ttf", "DejaVuSansCondensed"),
    ]
    for path, name in candidates:
        try:
            pdfmetrics.registerFont(TTFont(name, path))
            return name
        except Exception:
            continue
    return "Helvetica"


def _upper_text(value: Any) -> str:
    return str(value or "").strip().upper()


def _is_placeholder(value: Any) -> bool:
    text = _upper_text(value)
    if text in PLACEHOLDER_VALUES:
        return True
    if text.startswith("LOW_CONFIDENCE"):
        return True
    return False


def _missing_text(context: str) -> str:
    mapping = {
        "money": "Non quantificabile dal documento",
        "address": "Non indicato nel documento",
        "legal": "Informazione non ricavabile con certezza dalla perizia",
        "status": "Da verificare",
        "estimate": "Stima indicativa",
        "action": "Verifica manuale consigliata",
    }
    return mapping.get(context, "Dato non presente nel documento analizzato")


def sanitize_value(value: Any, context: str = "generic") -> str:
    if isinstance(value, dict):
        if "value" in value:
            return sanitize_value(value.get("value"), context=context)
        if "formatted" in value:
            return sanitize_value(value.get("formatted"), context=context)
        if "full" in value:
            return sanitize_value(value.get("full"), context=context)
        parts: List[str] = []
        for key in ("street", "number", "city", "province", "cap", "text"):
            v = value.get(key)
            if v and not _is_placeholder(v):
                parts.append(str(v).strip())
        if parts:
            return " ".join(parts)
        return _missing_text(context)

    if isinstance(value, (list, tuple, set)):
        parts = [sanitize_value(v, context=context) for v in value]
        compact = [p for p in parts if p and not _is_placeholder(p)]
        return "; ".join(compact) if compact else _missing_text(context)

    if value is None:
        return _missing_text(context)

    text = str(value).strip()
    if _is_placeholder(text):
        return _missing_text(context)
    if re.search(r"\b(TBD|UNKNOWN|NOT_SPECIFIED(?:_IN_PERIZIA)?|NULL|UNDEFINED)\b", text, flags=re.IGNORECASE):
        return _missing_text(context)
    return text


def parse_money_value(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if _is_placeholder(text):
        return None
    cleaned = re.sub(r"[^0-9,.-]", "", text)
    if not cleaned:
        return None
    if "," in cleaned and "." in cleaned:
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    elif "," in cleaned:
        cleaned = cleaned.replace(",", ".")
    else:
        parts = cleaned.split(".")
        if len(parts) > 1 and all(p.isdigit() for p in parts) and len(parts[-1]) == 3:
            cleaned = "".join(parts)
    try:
        return float(cleaned)
    except Exception:
        return None


def format_euro(value: Optional[float], *, allow_missing: bool = True) -> str:
    if value is None:
        return _missing_text("money") if allow_missing else ""
    formatted = f"{value:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"EUR {formatted}"


def _safe_paragraph(text: Any, style: ParagraphStyle, context: str = "generic") -> Paragraph:
    normalized = sanitize_value(text, context=context)
    # Insert soft breaks for very long tokens to avoid overflow.
    normalized = re.sub(r"([A-Za-z0-9_/-]{40})(?=[A-Za-z0-9_/-])", r"\1&#8203;", normalized)
    return Paragraph(escape(normalized), style)


def _status_color(status: str) -> colors.Color:
    u = _upper_text(status)
    if any(x in u for x in ["RED", "ROSSO", "CRITICO", "HIGH"]):
        return PALETTE["risk"]
    if any(x in u for x in ["GREEN", "VERDE", "OK", "LOW"]):
        return PALETTE["ok"]
    return PALETTE["warn"]


def _extract_evidence_snippets(evidence: Any, max_items: int = 2) -> List[str]:
    if not isinstance(evidence, list):
        return []
    out: List[str] = []
    for item in evidence:
        if not isinstance(item, dict):
            continue
        page = item.get("page") or item.get("page_number")
        quote = str(item.get("quote") or item.get("text") or item.get("snippet") or "").strip().replace("\n", " ")
        quote = re.sub(r"\s+", " ", quote)
        if len(quote) > 150:
            quote = quote[:147] + "..."
        if page and quote:
            out.append(f"p.{page} - {quote}")
        elif quote:
            out.append(quote)
        elif page:
            out.append(f"p.{page}")
        if len(out) >= max_items:
            break
    return out


def _first_non_empty(*values: Any, context: str = "generic") -> str:
    for v in values:
        t = sanitize_value(v, context=context)
        if t and not _is_placeholder(t):
            return t
    return _missing_text(context)


def _decision_payload(result: Dict[str, Any]) -> Dict[str, str]:
    section = result.get("decision_rapida_client") or result.get("section_2_decisione_rapida") or {}
    bundle = result.get("summary_for_client_bundle") if isinstance(result.get("summary_for_client_bundle"), dict) else {}
    risk = _first_non_empty(section.get("risk_level_it"), section.get("risk_level"), context="status")
    summary_it = sanitize_value(
        bundle.get("summary_it") or bundle.get("decision_summary_it") or section.get("summary_it") or section.get("summary") or section.get("value"),
        context="generic",
    )
    summary_en = sanitize_value(section.get("summary_en") or "", context="generic")
    return {
        "risk": risk,
        "summary_it": summary_it,
        "summary_en": "" if _is_placeholder(summary_en) else summary_en,
    }


def _lots_payload(result: Dict[str, Any]) -> List[Dict[str, str]]:
    lots_raw = result.get("lots") or []
    if not isinstance(lots_raw, list):
        return []
    rows: List[Dict[str, str]] = []
    for idx, lot in enumerate(lots_raw, start=1):
        if not isinstance(lot, dict):
            continue
        rows.append(
            {
                "lotto": _first_non_empty(lot.get("lot_number"), lot.get("lotto"), idx, context="generic"),
                "prezzo": _first_non_empty(lot.get("prezzo_base_eur"), lot.get("prezzo_base_value"), context="money"),
                "ubicazione": sanitize_value(lot.get("ubicazione"), context="address"),
                "superficie": sanitize_value(lot.get("superficie_mq"), context="generic"),
                "diritto": sanitize_value(lot.get("diritto_reale"), context="generic"),
                "tipologia": sanitize_value(lot.get("tipologia"), context="generic"),
            }
        )
    return rows


def _money_items_payload(result: Dict[str, Any]) -> Tuple[List[Dict[str, str]], str]:
    money_box = result.get("section_3_money_box") or result.get("money_box") or {}
    items = money_box.get("items") if isinstance(money_box, dict) else []
    if not isinstance(items, list):
        items = []

    canonical_codes = {"A", "B", "C", "D", "E", "F", "G", "H"}
    output: List[Dict[str, str]] = []
    numeric_total = 0.0
    has_unknown_cost = False

    for item in items:
        if not isinstance(item, dict):
            continue
        code = _upper_text(item.get("code") or item.get("voce"))
        if code.startswith("S3C"):
            continue

        label = sanitize_value(item.get("label_it") or item.get("voce") or item.get("label") or code, context="generic")
        if _is_placeholder(label):
            continue

        stima = parse_money_value(item.get("stima_euro"))
        if stima is None:
            has_unknown_cost = True
        else:
            numeric_total += stima

        if code and code not in canonical_codes and stima is None:
            # Drop noisy non-canonical candidate rows without deterministic amount.
            continue

        nota = sanitize_value(item.get("stima_nota") or item.get("note") or "", context="estimate")
        fonte = sanitize_value(item.get("fonte_perizia") or item.get("source") or "", context="generic")

        output.append(
            {
                "code": code or "-",
                "label": label,
                "stima": format_euro(stima),
                "nota": "" if _is_placeholder(nota) else nota,
                "fonte": "" if _is_placeholder(fonte) else fonte,
            }
        )

    money_total = money_box.get("totale_extra_budget") or money_box.get("total_extra_costs") or {}
    total_min = parse_money_value(money_total.get("min") if isinstance(money_total, dict) else None)
    total_text = ""
    if total_min is not None and not has_unknown_cost:
        total_text = format_euro(total_min)
    elif numeric_total > 0 and not has_unknown_cost:
        total_text = format_euro(numeric_total)
    else:
        total_text = _missing_text("money")

    if not output:
        output.append(
            {
                "code": "-",
                "label": "Costi non quantificabili dal documento",
                "stima": _missing_text("money"),
                "nota": "Verifica manuale consigliata",
                "fonte": "",
            }
        )

    return output, total_text


def _legal_killers_payload(result: Dict[str, Any]) -> List[Dict[str, str]]:
    section = result.get("section_9_legal_killers") or result.get("legal_killers_checklist") or {}
    items = section.get("items") if isinstance(section, dict) else []
    if not isinstance(items, list):
        return []

    out: List[Dict[str, str]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        killer = sanitize_value(item.get("killer") or item.get("label_it") or item.get("label") or "", context="legal")
        if _is_placeholder(killer):
            continue
        status = sanitize_value(item.get("status_it") or item.get("status"), context="status")
        action = sanitize_value(
            item.get("explanation_it") or item.get("reason_it") or item.get("action_required_it") or item.get("action"),
            context="action",
        )
        verify_next_raw = item.get("verify_next_it")
        verify_next = sanitize_value(verify_next_raw, context="action") if verify_next_raw not in (None, "") else ""
        if verify_next and not _is_placeholder(verify_next):
            action = f"{action}\nVerifica: {verify_next}"
        evidence = " | ".join(_extract_evidence_snippets(item.get("evidence"), max_items=2))
        out.append(
            {
                "killer": killer,
                "status": status,
                "action": action,
                "evidence": evidence,
            }
        )

    return out


def _details_per_bene_payload(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    lots = result.get("lots") if isinstance(result.get("lots"), list) else []
    details: List[Dict[str, Any]] = []

    for lot_idx, lot in enumerate(lots, start=1):
        beni = lot.get("beni") if isinstance(lot, dict) and isinstance(lot.get("beni"), list) else []
        for bene_idx, bene in enumerate(beni, start=1):
            if not isinstance(bene, dict):
                continue
            fields = [
                ("Tipologia", bene.get("tipologia")),
                ("Piano", bene.get("piano")),
                ("Superficie", bene.get("superficie")),
                ("Ubicazione", bene.get("ubicazione") or bene.get("short_location")),
                ("Stato", bene.get("stato")),
                ("Note", bene.get("note")),
            ]
            rows = []
            for label, value in fields:
                rendered = sanitize_value(value, context="generic")
                if not _is_placeholder(rendered):
                    rows.append((label, rendered))

            if not rows:
                rows.append(("Dettaglio", _missing_text("generic")))

            details.append(
                {
                    "title": f"Lotto {lot.get('lot_number', lot_idx)} - Bene {bene.get('bene_number', bene_idx)}",
                    "rows": rows,
                }
            )

    if details:
        return details

    # Fallback when structured beni are unavailable.
    dati = result.get("section_4_dati_certi") or result.get("dati_certi_del_lotto") or {}
    fallback_rows = [
        ("Composizione lotto", sanitize_value(dati.get("composizione_lotto"), context="generic")),
        ("Superficie", sanitize_value(dati.get("superficie_catastale") or dati.get("superficie"), context="generic")),
        ("Diritto reale", sanitize_value(dati.get("diritto_reale"), context="generic")),
    ]
    cleaned = [(k, v) for k, v in fallback_rows if not _is_placeholder(v)]
    if not cleaned:
        cleaned = [("Dettaglio", _missing_text("generic"))]
    return [{"title": "Bene principale", "rows": cleaned}]


def _red_flags_payload(result: Dict[str, Any]) -> List[Dict[str, str]]:
    red_flags = result.get("section_11_red_flags") or result.get("red_flags_operativi") or []
    if not isinstance(red_flags, list):
        red_flags = []
    out: List[Dict[str, str]] = []
    for idx, item in enumerate(red_flags, start=1):
        if isinstance(item, str):
            text = sanitize_value(item, context="action")
            if _is_placeholder(text):
                continue
            out.append({"title": f"Red Flag {idx}", "detail": text, "severity": "AMBER"})
            continue
        if not isinstance(item, dict):
            continue
        title = sanitize_value(item.get("label") or item.get("title_it") or item.get("title") or f"Red Flag {idx}", context="action")
        detail = sanitize_value(
            item.get("action_it") or item.get("explanation_it") or item.get("explanation") or item.get("detail") or item.get("reason_it"),
            context="action",
        )
        severity = _upper_text(item.get("severity") or item.get("status") or "AMBER")
        out.append({"title": title, "detail": detail, "severity": severity})
    return out


def _payload_from_result(analysis: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
    report_header = result.get("report_header") or {}
    case_header = result.get("case_header") or report_header
    semaforo = result.get("section_1_semaforo_generale") or result.get("semaforo_generale") or {}
    summary = result.get("summary_for_client") or {}
    bundle = result.get("summary_for_client_bundle") if isinstance(result.get("summary_for_client_bundle"), dict) else {}
    lots = _lots_payload(result)
    costs, costs_total = _money_items_payload(result)

    created_at = analysis.get("created_at") or datetime.now(timezone.utc).isoformat()
    generated_at = (result.get("run") or {}).get("generated_at_utc") or created_at

    return {
        "analysis_id": sanitize_value(analysis.get("analysis_id"), context="generic"),
        "case_id": sanitize_value(analysis.get("case_id"), context="generic"),
        "file_name": sanitize_value(analysis.get("file_name") or analysis.get("case_title"), context="generic"),
        "generated_at": sanitize_value(generated_at, context="generic"),
        "header": {
            "procedura": _first_non_empty(case_header.get("procedure", {}).get("value") if isinstance(case_header.get("procedure"), dict) else case_header.get("procedure"), case_header.get("procedure_id"), context="generic"),
            "tribunale": _first_non_empty(case_header.get("tribunale", {}).get("value") if isinstance(case_header.get("tribunale"), dict) else case_header.get("tribunale"), context="generic"),
            "lotto": _first_non_empty(case_header.get("lotto", {}).get("value") if isinstance(case_header.get("lotto"), dict) else case_header.get("lotto"), context="generic"),
            "indirizzo": _first_non_empty(case_header.get("address", {}).get("value") if isinstance(case_header.get("address"), dict) else case_header.get("address"), context="address"),
        },
        "panoramica": {
            "semaforo": sanitize_value(semaforo.get("status") or semaforo.get("status_it"), context="status"),
            "driver": sanitize_value((semaforo.get("driver") or {}).get("value") if isinstance(semaforo.get("driver"), dict) else semaforo.get("reason_it"), context="action"),
            "summary_it": sanitize_value(
                bundle.get("summary_it") or bundle.get("decision_summary_it") or summary.get("summary_it") or summary.get("raccomandazione"),
                context="generic",
            ),
            "summary_en": sanitize_value(summary.get("summary_en"), context="generic"),
        },
        "decisione": _decision_payload(result),
        "lots": lots,
        "costi": costs,
        "costi_totale": costs_total,
        "legal_killers": _legal_killers_payload(result),
        "dettagli_beni": _details_per_bene_payload(result),
        "red_flags": _red_flags_payload(result),
        "disclaimer_it": sanitize_value(summary.get("disclaimer_it"), context="action"),
        "disclaimer_en": sanitize_value(summary.get("disclaimer_en"), context="action"),
    }


def _extract_texts(obj: Any) -> Iterable[str]:
    if isinstance(obj, str):
        yield obj
        return
    if isinstance(obj, dict):
        for val in obj.values():
            yield from _extract_texts(val)
        return
    if isinstance(obj, (list, tuple, set)):
        for item in obj:
            yield from _extract_texts(item)


def _layout_preflight(payload: Dict[str, Any]) -> Dict[str, Any]:
    long_tokens: List[str] = []
    for text in _extract_texts(payload):
        for tok in re.findall(r"[A-Za-z0-9_/-]{60,}", text):
            if tok not in long_tokens:
                long_tokens.append(tok[:90])
    return {
        "has_long_unbroken_tokens": bool(long_tokens),
        "long_unbroken_tokens": long_tokens[:20],
    }


def _build_styles(font_name: str) -> Dict[str, ParagraphStyle]:
    styles = getSampleStyleSheet()
    return {
        "title": ParagraphStyle(
            "TitleNexo",
            parent=styles["Title"],
            fontName=font_name,
            fontSize=24,
            leading=28,
            textColor=PALETTE["text"],
            spaceAfter=8,
        ),
        "h2": ParagraphStyle(
            "H2Nexo",
            parent=styles["Heading2"],
            fontName=font_name,
            fontSize=13,
            leading=16,
            textColor=PALETTE["gold"],
            spaceBefore=9,
            spaceAfter=5,
        ),
        "body": ParagraphStyle(
            "BodyNexo",
            parent=styles["BodyText"],
            fontName=font_name,
            fontSize=9.4,
            leading=12.6,
            textColor=PALETTE["text"],
        ),
        "muted": ParagraphStyle(
            "MutedNexo",
            parent=styles["BodyText"],
            fontName=font_name,
            fontSize=8.4,
            leading=11,
            textColor=PALETTE["muted"],
        ),
        "small": ParagraphStyle(
            "SmallNexo",
            parent=styles["BodyText"],
            fontName=font_name,
            fontSize=7.6,
            leading=9.8,
            textColor=PALETTE["muted"],
        ),
    }


def _card_table(data: Sequence[Sequence[Any]], col_widths: Sequence[float], *, header: bool = False) -> Table:
    tbl = Table(data, colWidths=list(col_widths), repeatRows=1 if header else 0)
    style = [
        ("BACKGROUND", (0, 0), (-1, -1), PALETTE["panel"]),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("GRID", (0, 0), (-1, -1), 0.35, PALETTE["gold_soft"]),
        ("LEFTPADDING", (0, 0), (-1, -1), 7),
        ("RIGHTPADDING", (0, 0), (-1, -1), 7),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]
    if header:
        style.extend(
            [
                ("BACKGROUND", (0, 0), (-1, 0), PALETTE["panel_soft"]),
                ("LINEBELOW", (0, 0), (-1, 0), 0.6, PALETTE["gold"]),
            ]
        )
    tbl.setStyle(TableStyle(style))
    return tbl


def _build_story(payload: Dict[str, Any], styles: Dict[str, ParagraphStyle], page_width: float) -> List[Any]:
    story: List[Any] = []

    # 1) Header / Cover
    cover_rows = [
        [
            _safe_paragraph("NEXODIFY REPORT PERIZIA", styles["title"]),
            _safe_paragraph(f"Template: {TEMPLATE_VERSION}", styles["small"]),
        ],
        [
            _safe_paragraph(
                f"File: {payload['file_name']}<br/>Analysis ID: {payload['analysis_id']}<br/>Case ID: {payload['case_id']}",
                styles["muted"],
            ),
            _safe_paragraph(f"Generato il: {payload['generated_at']}", styles["muted"]),
        ],
    ]
    cover = _card_table(cover_rows, [page_width * 0.72, page_width * 0.28])
    story.append(cover)
    story.append(Spacer(1, 10))

    header = payload["header"]
    header_rows = [
        [_safe_paragraph("Procedura", styles["small"]), _safe_paragraph(header["procedura"], styles["body"])],
        [_safe_paragraph("Tribunale", styles["small"]), _safe_paragraph(header["tribunale"], styles["body"])],
        [_safe_paragraph("Lotto", styles["small"]), _safe_paragraph(header["lotto"], styles["body"])],
        [_safe_paragraph("Indirizzo", styles["small"]), _safe_paragraph(header["indirizzo"], styles["body"], context="address")],
    ]
    story.append(_card_table(header_rows, [page_width * 0.23, page_width * 0.77]))
    story.append(Spacer(1, 8))

    # 2) Panoramica
    story.append(_safe_paragraph("2. Panoramica", styles["h2"]))
    pano = payload["panoramica"]
    pano_rows = [
        [_safe_paragraph("Semaforo", styles["small"]), _safe_paragraph(pano["semaforo"], styles["body"])],
        [_safe_paragraph("Driver", styles["small"]), _safe_paragraph(pano["driver"], styles["body"])],
        [_safe_paragraph("Sintesi IT", styles["small"]), _safe_paragraph(pano["summary_it"], styles["body"])],
    ]
    if pano["summary_en"] and not _is_placeholder(pano["summary_en"]):
        pano_rows.append([_safe_paragraph("Summary EN", styles["small"]), _safe_paragraph(pano["summary_en"], styles["muted"])])
    story.append(_card_table(pano_rows, [page_width * 0.2, page_width * 0.8]))

    # 3) Lots / Lot composition
    story.append(_safe_paragraph("3. Lots / Composizione Lotto", styles["h2"]))
    lot_rows: List[List[Any]] = [[
        _safe_paragraph("Lotto", styles["small"]),
        _safe_paragraph("Prezzo base", styles["small"]),
        _safe_paragraph("Tipologia", styles["small"]),
        _safe_paragraph("Ubicazione", styles["small"]),
        _safe_paragraph("Superficie", styles["small"]),
        _safe_paragraph("Diritto", styles["small"]),
    ]]
    lots = payload["lots"]
    if lots:
        for lot in lots:
            lot_rows.append(
                [
                    _safe_paragraph(lot["lotto"], styles["body"]),
                    _safe_paragraph(lot["prezzo"], styles["body"], context="money"),
                    _safe_paragraph(lot["tipologia"], styles["body"]),
                    _safe_paragraph(lot["ubicazione"], styles["body"], context="address"),
                    _safe_paragraph(lot["superficie"], styles["body"]),
                    _safe_paragraph(lot["diritto"], styles["body"]),
                ]
            )
    else:
        lot_rows.append([_safe_paragraph("Nessun lotto strutturato disponibile", styles["muted"]), "", "", "", "", ""])
    story.append(_card_table(lot_rows, [page_width * 0.08, page_width * 0.16, page_width * 0.14, page_width * 0.3, page_width * 0.12, page_width * 0.2], header=True))

    # 4) Decisione Rapida
    story.append(_safe_paragraph("4. Decisione Rapida", styles["h2"]))
    decisione = payload["decisione"]
    decision_rows = [
        [_safe_paragraph("Rischio", styles["small"]), _safe_paragraph(decisione["risk"], styles["body"])],
        [_safe_paragraph("Sintesi", styles["small"]), _safe_paragraph(decisione["summary_it"], styles["body"])],
    ]
    if decisione["summary_en"]:
        decision_rows.append([_safe_paragraph("Summary EN", styles["small"]), _safe_paragraph(decisione["summary_en"], styles["muted"])])
    story.append(_card_table(decision_rows, [page_width * 0.16, page_width * 0.84]))

    # 5) Money Box / Costi
    story.append(_safe_paragraph("5. Money Box / Costi", styles["h2"]))
    cost_rows = [[
        _safe_paragraph("Codice", styles["small"]),
        _safe_paragraph("Voce", styles["small"]),
        _safe_paragraph("Stima", styles["small"]),
        _safe_paragraph("Nota", styles["small"]),
        _safe_paragraph("Fonte", styles["small"]),
    ]]
    for item in payload["costi"]:
        cost_rows.append(
            [
                _safe_paragraph(item["code"], styles["body"]),
                _safe_paragraph(item["label"], styles["body"]),
                _safe_paragraph(item["stima"], styles["body"], context="money"),
                _safe_paragraph(item["nota"], styles["muted"]),
                _safe_paragraph(item["fonte"], styles["muted"]),
            ]
        )

    story.append(_card_table(cost_rows, [page_width * 0.08, page_width * 0.3, page_width * 0.17, page_width * 0.25, page_width * 0.2], header=True))
    story.append(Spacer(1, 5))
    story.append(_card_table(
        [[_safe_paragraph("Totale costi extra", styles["small"]), _safe_paragraph(payload["costi_totale"], styles["body"], context="money")]],
        [page_width * 0.35, page_width * 0.65],
    ))

    # 6) Legal Killers
    story.append(_safe_paragraph("6. Legal Killers", styles["h2"]))
    legal = payload["legal_killers"]
    legal_rows = [[
        _safe_paragraph("Voce", styles["small"]),
        _safe_paragraph("Status", styles["small"]),
        _safe_paragraph("Azione", styles["small"]),
        _safe_paragraph("Evidenza", styles["small"]),
    ]]
    if legal:
        for item in legal:
            legal_rows.append(
                [
                    _safe_paragraph(item["killer"], styles["body"], context="legal"),
                    _safe_paragraph(item["status"], styles["body"], context="status"),
                    _safe_paragraph(item["action"], styles["muted"], context="action"),
                    _safe_paragraph(item["evidence"], styles["small"]),
                ]
            )
    else:
        legal_rows.append([_safe_paragraph("Nessun legal killer strutturato disponibile", styles["muted"]), "", "", ""])
    story.append(_card_table(legal_rows, [page_width * 0.26, page_width * 0.14, page_width * 0.28, page_width * 0.32], header=True))

    # 7) Dettagli per bene
    story.append(_safe_paragraph("7. Dettagli per bene", styles["h2"]))
    details = payload["dettagli_beni"]
    for detail in details:
        section_header = _safe_paragraph(detail["title"], styles["body"])
        rows: List[List[Any]] = []
        for label, value in detail["rows"]:
            rows.append([_safe_paragraph(label, styles["small"]), _safe_paragraph(value, styles["body"])])
        card = _card_table(rows, [page_width * 0.28, page_width * 0.72])
        story.append(KeepTogether([section_header, Spacer(1, 2), card, Spacer(1, 6)]))

    # 8) Red Flags
    story.append(_safe_paragraph("8. Red Flags", styles["h2"]))
    flags = payload["red_flags"]
    flag_rows = [[
        _safe_paragraph("Severita", styles["small"]),
        _safe_paragraph("Voce", styles["small"]),
        _safe_paragraph("Dettaglio", styles["small"]),
    ]]
    if flags:
        for f in flags:
            sev = sanitize_value(f.get("severity"), context="status")
            flag_rows.append(
                [
                    _safe_paragraph(sev, styles["body"]),
                    _safe_paragraph(sanitize_value(f.get("title"), context="action"), styles["body"]),
                    _safe_paragraph(sanitize_value(f.get("detail"), context="action"), styles["muted"]),
                ]
            )
    else:
        flag_rows.append([_safe_paragraph("AMBER", styles["body"]), _safe_paragraph("Nessuna red flag strutturata", styles["body"]), _safe_paragraph("Verifica manuale consigliata", styles["muted"])])

    story.append(_card_table(flag_rows, [page_width * 0.15, page_width * 0.3, page_width * 0.55], header=True))

    # 9) Disclaimer / Footer
    story.append(_safe_paragraph("9. Disclaimer / Footer", styles["h2"]))
    disclaimer_rows = [
        [_safe_paragraph("Disclaimer IT", styles["small"]), _safe_paragraph(payload["disclaimer_it"], styles["muted"])],
        [_safe_paragraph("Disclaimer EN", styles["small"]), _safe_paragraph(payload["disclaimer_en"], styles["small"])],
        [_safe_paragraph("Brand", styles["small"]), _safe_paragraph("Nexodify", styles["muted"])],
    ]
    story.append(_card_table(disclaimer_rows, [page_width * 0.2, page_width * 0.8]))

    return story


@lru_cache(maxsize=1)
def _load_brand_paths() -> Tuple[str, ...]:
    try:
        content = open(BRAND_ICON_SOURCE_PATH, "r", encoding="utf-8").read()
    except Exception:
        return tuple()
    paths = re.findall(r'd:\s*"([^"]+)"', content)
    return tuple(paths)


def _svg_numbers(blob: str) -> List[float]:
    return [float(x) for x in re.findall(r"[-+]?(?:\d*\.\d+|\d+)(?:[eE][-+]?\d+)?", blob)]


def _path_tokens(path_d: str) -> List[str]:
    return re.findall(r"[MmLlHhVvCcSsZz]|[-+]?(?:\d*\.\d+|\d+)(?:[eE][-+]?\d+)?", path_d)


def _draw_svg_path(canvas_obj: Any, d: str) -> None:
    tokens = _path_tokens(d)
    i = 0
    cmd = ""
    current_x = 0.0
    current_y = 0.0
    start_x = 0.0
    start_y = 0.0
    prev_ctrl_x: Optional[float] = None
    prev_ctrl_y: Optional[float] = None
    p = canvas_obj.beginPath()

    def has_num(idx: int) -> bool:
        return idx < len(tokens) and re.match(r"[-+0-9.]", tokens[idx]) is not None

    while i < len(tokens):
        tok = tokens[i]
        if re.match(r"^[A-Za-z]$", tok):
            cmd = tok
            i += 1
        if not cmd:
            break

        if cmd in ("M", "m"):
            first = True
            while has_num(i) and has_num(i + 1):
                x = float(tokens[i])
                y = float(tokens[i + 1])
                i += 2
                if cmd == "m":
                    x += current_x
                    y += current_y
                if first:
                    p.moveTo(x, y)
                    start_x, start_y = x, y
                    first = False
                else:
                    p.lineTo(x, y)
                current_x, current_y = x, y
            prev_ctrl_x = prev_ctrl_y = None
            continue

        if cmd in ("L", "l"):
            while has_num(i) and has_num(i + 1):
                x = float(tokens[i])
                y = float(tokens[i + 1])
                i += 2
                if cmd == "l":
                    x += current_x
                    y += current_y
                p.lineTo(x, y)
                current_x, current_y = x, y
            prev_ctrl_x = prev_ctrl_y = None
            continue

        if cmd in ("H", "h"):
            while has_num(i):
                x = float(tokens[i])
                i += 1
                if cmd == "h":
                    x += current_x
                p.lineTo(x, current_y)
                current_x = x
            prev_ctrl_x = prev_ctrl_y = None
            continue

        if cmd in ("V", "v"):
            while has_num(i):
                y = float(tokens[i])
                i += 1
                if cmd == "v":
                    y += current_y
                p.lineTo(current_x, y)
                current_y = y
            prev_ctrl_x = prev_ctrl_y = None
            continue

        if cmd in ("C", "c"):
            while has_num(i) and has_num(i + 5):
                x1 = float(tokens[i])
                y1 = float(tokens[i + 1])
                x2 = float(tokens[i + 2])
                y2 = float(tokens[i + 3])
                x = float(tokens[i + 4])
                y = float(tokens[i + 5])
                i += 6
                if cmd == "c":
                    x1 += current_x
                    y1 += current_y
                    x2 += current_x
                    y2 += current_y
                    x += current_x
                    y += current_y
                p.curveTo(x1, y1, x2, y2, x, y)
                current_x, current_y = x, y
                prev_ctrl_x, prev_ctrl_y = x2, y2
            continue

        if cmd in ("S", "s"):
            while has_num(i) and has_num(i + 3):
                x2 = float(tokens[i])
                y2 = float(tokens[i + 1])
                x = float(tokens[i + 2])
                y = float(tokens[i + 3])
                i += 4
                if prev_ctrl_x is None or prev_ctrl_y is None:
                    x1, y1 = current_x, current_y
                else:
                    x1 = 2 * current_x - prev_ctrl_x
                    y1 = 2 * current_y - prev_ctrl_y
                if cmd == "s":
                    x2 += current_x
                    y2 += current_y
                    x += current_x
                    y += current_y
                p.curveTo(x1, y1, x2, y2, x, y)
                current_x, current_y = x, y
                prev_ctrl_x, prev_ctrl_y = x2, y2
            continue

        if cmd in ("Z", "z"):
            p.lineTo(start_x, start_y)
            current_x, current_y = start_x, start_y
            prev_ctrl_x = prev_ctrl_y = None
            continue

        # Unknown command: consume one token to avoid infinite loop.
        i += 1

    canvas_obj.drawPath(p, stroke=1, fill=0)


def _draw_brand_icon(canvas_obj: Any, x: float, y: float, size: float, *, stroke: colors.Color, alpha: float) -> None:
    paths = _load_brand_paths()
    if not paths:
        return
    canvas_obj.saveState()
    canvas_obj.translate(x, y)
    canvas_obj.scale(size / 24.0, size / 24.0)
    canvas_obj.translate(0, 24)
    canvas_obj.scale(1, -1)
    canvas_obj.setLineWidth(1.6)
    canvas_obj.setLineCap(1)
    canvas_obj.setLineJoin(1)
    canvas_obj.setStrokeColor(stroke)
    try:
        canvas_obj.setStrokeAlpha(alpha)
    except Exception:
        pass
    for d in paths:
        _draw_svg_path(canvas_obj, d)
    canvas_obj.restoreState()


def _draw_page_chrome(canvas_obj: Any, doc: Any) -> None:
    width, height = A4

    # Header brand lockup.
    _draw_brand_icon(canvas_obj, doc.leftMargin, height - 18 * mm, 9 * mm, stroke=PALETTE["gold"], alpha=0.95)
    canvas_obj.saveState()
    canvas_obj.setFont(doc._fontname, 9)
    canvas_obj.setFillColor(PALETTE["gold"])
    canvas_obj.drawString(doc.leftMargin + 11 * mm, height - 14.8 * mm, "NEXODIFY")
    canvas_obj.restoreState()

    # Watermark (subtle, non-invasive).
    canvas_obj.saveState()
    try:
        canvas_obj.setFillAlpha(0.045)
        canvas_obj.setStrokeAlpha(0.06)
    except Exception:
        pass
    canvas_obj.setFillColor(PALETTE["gold"])
    canvas_obj.setStrokeColor(PALETTE["gold"])
    canvas_obj.setFont(doc._fontname, 42)
    canvas_obj.drawCentredString(width / 2.0, height / 2.0, "NEXODIFY")
    _draw_brand_icon(canvas_obj, width / 2.0 - 12 * mm, height / 2.0 + 6 * mm, 24 * mm, stroke=PALETTE["gold"], alpha=0.08)
    canvas_obj.restoreState()

    # Footer.
    canvas_obj.saveState()
    canvas_obj.setFont(doc._fontname, 7.5)
    canvas_obj.setFillColor(PALETTE["muted"])
    canvas_obj.drawString(doc.leftMargin, 9 * mm, f"Template {TEMPLATE_VERSION}")
    canvas_obj.drawRightString(width - doc.rightMargin, 9 * mm, f"Pag. {canvas_obj.getPageNumber()}")
    canvas_obj.restoreState()


def build_perizia_pdf_document(analysis: Dict[str, Any], result: Dict[str, Any]) -> Tuple[bytes, Dict[str, Any]]:
    font = _try_register_font()
    styles = _build_styles(font)

    payload = _payload_from_result(analysis, result)
    preflight = _layout_preflight(payload)

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=16 * mm,
        rightMargin=16 * mm,
        topMargin=24 * mm,
        bottomMargin=14 * mm,
        title="Nexodify Perizia Report",
        author="Nexodify",
    )
    # Keep font accessible from onPage callback.
    doc._fontname = font  # type: ignore[attr-defined]

    content_width = A4[0] - doc.leftMargin - doc.rightMargin
    story = _build_story(payload, styles, content_width)

    doc.build(story, onFirstPage=_draw_page_chrome, onLaterPages=_draw_page_chrome)
    pdf_bytes = buf.getvalue()

    meta = {
        "template_version": TEMPLATE_VERSION,
        "required_sections": REQUIRED_SECTIONS,
        "legacy_section_markers": LEGACY_SECTION_MARKERS,
        "brand_asset_path": BRAND_ICON_SOURCE_PATH,
        "watermark": {
            "enabled": True,
            "style": "gold_text_plus_scale_icon",
            "opacity": 0.045,
        },
        "preflight": preflight,
        "payload_snapshot": payload,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "pdf_size_bytes": len(pdf_bytes),
    }
    return pdf_bytes, meta


def build_perizia_pdf_bytes(analysis: Dict[str, Any], result: Dict[str, Any]) -> bytes:
    pdf_bytes, _meta = build_perizia_pdf_document(analysis, result)
    return pdf_bytes


def build_perizia_pdf_debug(analysis: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
    _pdf_bytes, meta = build_perizia_pdf_document(analysis, result)
    # Keep this deterministic and compact for validators.
    return {
        "template_version": meta["template_version"],
        "required_sections": meta["required_sections"],
        "legacy_section_markers": meta["legacy_section_markers"],
        "brand_asset_path": meta["brand_asset_path"],
        "watermark": meta["watermark"],
        "preflight": meta["preflight"],
        "payload_snapshot": meta["payload_snapshot"],
    }


if __name__ == "__main__":
    # Quick local smoke tool when run directly.
    demo_analysis = {"analysis_id": "demo", "case_id": "case_demo", "file_name": "demo.pdf", "created_at": datetime.now(timezone.utc).isoformat()}
    demo_result = {
        "case_header": {"procedure": {"value": "R.G.E. 123/2024"}, "tribunale": {"value": "Roma"}, "lotto": {"value": "1"}, "address": {"value": "Via Demo 1"}},
        "section_1_semaforo_generale": {"status": "AMBER", "reason_it": "Verifica documentale richiesta"},
        "decision_rapida_client": {"risk_level_it": "Rischio Medio", "summary_it": "Procedere con verifiche tecniche e legali."},
        "lots": [{"lot_number": "1", "prezzo_base_eur": "EUR 100000", "ubicazione": "Roma", "superficie_mq": "85", "diritto_reale": "Piena proprieta"}],
        "money_box": {"items": [{"code": "A", "label_it": "Regolarizzazione", "stima_euro": 12000, "stima_nota": "Stima indicativa"}]},
        "legal_killers_checklist": {"items": [{"killer": "Occupazione", "status": "AMBER", "reason_it": "Da verificare"}]},
        "summary_for_client": {"summary_it": "Demo", "disclaimer_it": "Documento informativo"},
    }
    pdf, debug = build_perizia_pdf_document(demo_analysis, demo_result)
    open("/tmp/demo_nexodify_report.pdf", "wb").write(pdf)
    open("/tmp/demo_nexodify_report.debug.json", "w", encoding="utf-8").write(json.dumps(debug, ensure_ascii=False, indent=2))
    print("WROTE /tmp/demo_nexodify_report.pdf")
