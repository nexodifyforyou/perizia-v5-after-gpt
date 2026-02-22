from __future__ import annotations

from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple
import re

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from xml.sax.saxutils import escape


def _try_register_font() -> str:
    """
    Use DejaVuSans if available (better Unicode/€), fallback to Helvetica.
    """
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


def _normalize(val: Any) -> str:
    # Map internal placeholders to customer-safe output
    if val in [None, "None", "N/A", "NOT_SPECIFIED_IN_PERIZIA", "NOT_SPECIFIED", "UNKNOWN", ""]:
        return "NON SPECIFICATO IN PERIZIA"
    if isinstance(val, str) and val.strip().upper().startswith("LOW_CONFIDENCE"):
        return "DA VERIFICARE"
    if isinstance(val, dict):
        # common pattern: {"value": "...", "evidence": [...]}
        if "value" in val:
            return _normalize(val.get("value"))
        # address pattern: {"full": "...", "street": "...", "city": "..."}
        if "full" in val and val.get("full"):
            full = val.get("full")
            # If "full" is LOW_CONFIDENCE, ignore it and build from parts instead
            if not (isinstance(full, str) and full.strip().upper().startswith("LOW_CONFIDENCE")):
                return _normalize(full)
        parts = []
        for k in ("street", "number", "city", "province", "cap"):
            v = val.get(k)
            if v and _normalize(v) != "NON SPECIFICATO IN PERIZIA":
                parts.append(str(v))
        return " ".join(parts) if parts else "NON SPECIFICATO IN PERIZIA"
    if isinstance(val, list):
        return ", ".join(_normalize(x) for x in val if _normalize(x) != "NON SPECIFICATO IN PERIZIA") or "NON SPECIFICATO IN PERIZIA"
    return str(val)


def _get_value(obj: Any, fallback: str = "NON SPECIFICATO IN PERIZIA") -> str:
    if isinstance(obj, dict) and "value" in obj:
        return _normalize(obj.get("value"))
    return _normalize(obj) if obj is not None else fallback


def P(text: Any, style: ParagraphStyle) -> Paragraph:
    # Paragraph-safe (escape markup) + normalized placeholders
    return Paragraph(escape(_normalize(text)), style)


def _evidence_lines(evs: Any, max_lines: int = 3) -> List[str]:
    """
    Accept evidence as list[dict], try to format page + quote/snippet.
    """
    if not isinstance(evs, list):
        return []
    out: List[str] = []
    for ev in evs:
        if not isinstance(ev, dict):
            continue
        p = ev.get("page_number") or ev.get("page") or ev.get("pageIndex") or ev.get("page_idx")
        quote = ev.get("quote") or ev.get("text") or ev.get("snippet") or ev.get("excerpt") or ""
        quote = str(quote).strip().replace("\n", " ")
        if len(quote) > 180:
            quote = quote[:177] + "..."
        if p is not None and str(p).strip() != "":
            line = f"p.{p}: {quote}" if quote else f"p.{p}"
        else:
            line = quote
        if line:
            out.append(line)
        if len(out) >= max_lines:
            break
    return out


def _map_risk_level_it(value: Any) -> str:
    raw = str(value or "").strip().upper()
    mapping = {
        "LOW_RISK": "RISCHIO BASSO",
        "MEDIUM_RISK": "RISCHIO MEDIO",
        "HIGH_RISK": "RISCHIO ALTO",
        "LOW": "RISCHIO BASSO",
        "MEDIUM": "RISCHIO MEDIO",
        "HIGH": "RISCHIO ALTO",
        "GREEN": "RISCHIO BASSO",
        "AMBER": "RISCHIO MEDIO",
        "RED": "RISCHIO ALTO",
    }
    return mapping.get(raw, str(value or "").strip())


def parse_money_value(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    if "TBD" in text.upper():
        return None
    cleaned = re.sub(r"[€\s]", "", text)
    cleaned = cleaned.replace("EUR", "").replace("euro", "")
    cleaned = cleaned.strip()
    if not cleaned:
        return None
    if "," in cleaned and "." in cleaned:
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    elif "," in cleaned:
        cleaned = cleaned.replace(",", ".")
    elif "." in cleaned:
        parts = cleaned.split(".")
        if len(parts) > 1 and all(p.isdigit() for p in parts):
            if len(parts[-1]) == 3:
                cleaned = "".join(parts)
    try:
        return float(cleaned)
    except Exception:
        return None


def _format_euro_value(value: float) -> str:
    formatted = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"€ {formatted}"


def build_perizia_pdf_bytes(analysis: Dict[str, Any], result: Dict[str, Any]) -> bytes:
    font = _try_register_font()
    styles = getSampleStyleSheet()

    base = ParagraphStyle(
        "Base",
        parent=styles["BodyText"],
        fontName=font,
        fontSize=10,
        leading=13,
        spaceAfter=4,
    )
    h1 = ParagraphStyle("H1", parent=styles["Heading1"], fontName=font, fontSize=16, leading=20, spaceAfter=10)
    h2 = ParagraphStyle("H2", parent=styles["Heading2"], fontName=font, fontSize=12, leading=16, spaceBefore=10, spaceAfter=6)
    small = ParagraphStyle("Small", parent=base, fontName=font, fontSize=8, leading=10, textColor=colors.grey)

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=18 * mm,
        rightMargin=18 * mm,
        topMargin=16 * mm,
        bottomMargin=16 * mm,
        title="Nexodify Perizia Report",
    )

    story: List[Any] = []

    file_name = (analysis.get("file_name") or analysis.get("case_title") or "perizia.pdf")
    analysis_id = analysis.get("analysis_id", "")
    case_id = analysis.get("case_id", "")
    generated_at = (result.get("run", {}) or {}).get("generated_at_utc") or analysis.get("created_at") or ""

    story.append(Paragraph(f"Nexodify Report — {file_name}", h1))
    story.append(Paragraph(f"Analysis ID: {analysis_id} &nbsp;&nbsp; Case ID: {case_id} &nbsp;&nbsp; Generated: {generated_at}", small))
    story.append(Spacer(1, 8))

    # Headers (support old/new)
    report_header = result.get("report_header", {}) or {}
    case_header = result.get("case_header", report_header) or report_header

    def header_field(label: str, key: str) -> Tuple[str, str]:
        val = case_header.get(key)
        return (label, _get_value(val))

    header_rows = [
        header_field("Procedura", "procedure"),
        header_field("Tribunale", "tribunale"),
        header_field("Lotto", "lotto"),
        header_field("Indirizzo", "address"),
    ]
    t = Table(header_rows, colWidths=[30 * mm, 150 * mm])
    t.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, -1), font),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ROWBACKGROUNDS", (0, 0), (-1, -1), [colors.whitesmoke, colors.white]),
        ("LINEBELOW", (0, 0), (-1, -1), 0.25, colors.lightgrey),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    story.append(t)

    # Semaforo
    semaforo = result.get("section_1_semaforo_generale", {}) or result.get("semaforo_generale", {}) or {}
    status = _normalize(semaforo.get("status", "DA_VERIFICARE"))
    story.append(Paragraph("Semaforo Generale", h2))
    story.append(Paragraph(f"Status: <b>{status}</b>", base))
    ev_lines = _evidence_lines(semaforo.get("evidence", []))
    for line in ev_lines:
        story.append(Paragraph(line, small))

    # Decisione rapida
    decision = result.get("decision_rapida_client", {}) or result.get("section_2_decisione_rapida", {}) or {}
    story.append(Paragraph("Decisione Rapida", h2))
    if isinstance(decision, dict):
        def _decision_text(val: Any) -> str:
            if val is None:
                return ""
            normalized = _normalize(val)
            if normalized == "NON SPECIFICATO IN PERIZIA":
                return ""
            return str(val).strip()

        risk_it = _decision_text(decision.get("risk_level_it") or _map_risk_level_it(decision.get("risk_level")))
        risk_en = _decision_text(decision.get("risk_level_en"))
        summary_it = _decision_text(decision.get("summary_it") or decision.get("summary") or decision.get("value") or decision.get("text"))
        summary_en = _decision_text(decision.get("summary_en"))
        if risk_it:
            story.append(Paragraph(f"RISCHIO: <b>{escape(risk_it)}</b>", base))
        if risk_en:
            story.append(Paragraph(escape(risk_en), small))
        if summary_it:
            story.append(Paragraph(escape(summary_it), base))
        if summary_en:
            story.append(Paragraph(escape(summary_en), small))
    else:
        decision_text = str(decision).strip()
        if decision_text:
            story.append(Paragraph(escape(decision_text), base))

    # Multi-lot summary if present
    lots = result.get("lots", []) or []
    if isinstance(lots, list) and len(lots) > 0:
        story.append(Paragraph("Lotti", h2))
        rows = [["Lotto", "Prezzo Base", "Ubicazione", "Superficie", "Diritto"]]
        for lot in lots:
            if not isinstance(lot, dict):
                continue
            rows.append([
                f"{lot.get('lot_number', '')}",
                _normalize(lot.get("prezzo_base_eur") or lot.get("prezzo_base_value") or ""),
                _normalize(lot.get("ubicazione") or ""),
                _normalize(lot.get("superficie_mq") or ""),
                _normalize(lot.get("diritto_reale") or ""),
            ])
        rows2 = []
        for r in rows:
            if r == rows[0]:
                rows2.append(r)
                continue
            rows2.append([
                r[0],
                r[1],
                P(r[2], base),
                r[3],
                P(r[4], base),
            ])
        tbl = Table(rows2, colWidths=[14 * mm, 26 * mm, 80 * mm, 24 * mm, 30 * mm])
        tbl.setStyle(TableStyle([
            ("FONTNAME", (0, 0), (-1, -1), font),
            ("FONTSIZE", (0, 0), (-1, 0), 10),
            ("FONTSIZE", (0, 1), (-1, -1), 9),
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("LINEBELOW", (0, 0), (-1, 0), 0.5, colors.grey),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.lightgrey),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ]))
        story.append(tbl)

    # Money box
    money_box = result.get("section_3_money_box", {}) or result.get("money_box", {}) or {}
    story.append(Paragraph("Money Box", h2))
    items = money_box.get("items", []) if isinstance(money_box, dict) else []
    rows = [["Voce", "Stima (€)", "Fonte (Perizia)"]]
    total = 0.0
    any_missing_or_tbd = False
    has_item = False
    for it in items:
        if not isinstance(it, dict):
            continue
        has_item = True
        voce = it.get("voce") or it.get("label_it") or it.get("code") or ""
        stima = it.get("stima_euro")
        fonte = it.get("fonte_perizia", {})
        fonte_val = _get_value(fonte, "")
        stima_val = parse_money_value(stima)
        if stima_val is None:
            any_missing_or_tbd = True
            stima_disp = "TBD"
        else:
            total += stima_val
            stima_disp = _format_euro_value(stima_val)
        rows.append([_normalize(voce), stima_disp, _normalize(fonte_val)])
    total_disp = _format_euro_value(total) if has_item and not any_missing_or_tbd else "TBD"
    rows.append(["Totale (min)", total_disp, ""])
    rows2 = []
    for i, r in enumerate(rows):
        if i == 0:
            rows2.append(r)
        elif i == len(rows) - 1:
            rows2.append(r)
        else:
            rows2.append([P(r[0], base), r[1], P(r[2], base)])
    tbl = Table(rows2, colWidths=[78 * mm, 22 * mm, 80 * mm])
    tbl.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, -1), font),
        ("FONTSIZE", (0, 0), (-1, 0), 10),
        ("FONTSIZE", (0, 1), (-1, -2), 9),
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("LINEBELOW", (0, 0), (-1, 0), 0.5, colors.grey),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.lightgrey),
        ("BACKGROUND", (0, -1), (-1, -1), colors.whitesmoke),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    story.append(tbl)

    # Legal killers
    lk = result.get("section_9_legal_killers", {}) or {}
    story.append(Paragraph("Legal Killers", h2))
    lk_items = lk.get("items", []) if isinstance(lk, dict) else []
    lk_rows = [["Item", "Status", "Azione", "Evidence (estratto)"]]
    for it in lk_items:
        if not isinstance(it, dict):
            continue
        label = _normalize(it.get("killer") or it.get("label_it") or it.get("label") or it.get("code") or "")
        st = _normalize(it.get("status_it") or it.get("status") or "DA_VERIFICARE")
        action_raw = it.get("reason_it") or it.get("action_required_it") or "NON SPECIFICATO IN PERIZIA"
        action = _normalize(action_raw)
        ev = " | ".join(_evidence_lines(it.get("evidence", []), max_lines=2))
        lk_rows.append([label, st, action, ev])
    lk_rows2 = []
    for i, r in enumerate(lk_rows):
        if i == 0:
            lk_rows2.append(r)
        else:
            lk_rows2.append([P(r[0], base), r[1], P(r[2], base), P(r[3], small)])
    lk_tbl = Table(lk_rows2, colWidths=[55 * mm, 20 * mm, 45 * mm, 60 * mm])
    lk_tbl.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, -1), font),
        ("FONTSIZE", (0, 0), (-1, 0), 10),
        ("FONTSIZE", (0, 1), (-1, -1), 8.5),
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.lightgrey),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    story.append(lk_tbl)

    # Summary
    summary = result.get("summary_for_client", {})
    story.append(Paragraph("Summary for Client", h2))
    if isinstance(summary, dict):
        it = summary.get("it") or summary.get("value") or summary.get("text") or ""
        en = summary.get("en") or ""
        if it:
            story.append(Paragraph(_normalize(it), base))
        if en:
            story.append(Paragraph(_normalize(en), small))
    else:
        story.append(Paragraph(_normalize(summary), base))

    story.append(Spacer(1, 10))
    story.append(Paragraph("<b>AVVISO IMPORTANTE</b> — Documento informativo. Non costituisce consulenza legale. Consultare un professionista qualificato.", small))

    doc.build(story)
    return buf.getvalue()
