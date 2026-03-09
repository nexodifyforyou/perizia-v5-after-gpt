#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Tuple

SCRIPT_PATH = Path(__file__).resolve()
BACKEND_DIR = SCRIPT_PATH.parents[1]
EXPECTED_PYTHON = BACKEND_DIR / '.venv' / 'bin' / 'python'
if (
    EXPECTED_PYTHON.exists()
    and Path(sys.executable).resolve() != EXPECTED_PYTHON.resolve()
    and os.environ.get('_PERIZIA_VENV_REEXEC') != '1'
):
    env = os.environ.copy()
    env['_PERIZIA_VENV_REEXEC'] = '1'
    os.execve(str(EXPECTED_PYTHON), [str(EXPECTED_PYTHON), str(SCRIPT_PATH), *sys.argv[1:]], env)

import fitz  # type: ignore
from dotenv import dotenv_values
from PyPDF2 import PdfReader
from pymongo import MongoClient

sys.path.insert(0, str(BACKEND_DIR))
from pdf_report import (
    LEGACY_SECTION_MARKERS,
    REQUIRED_SECTIONS,
    TEMPLATE_VERSION,
    build_perizia_pdf_document,
)


ENV_PATH = Path('/srv/perizia/app/backend/.env')
BANNED_PLACEHOLDER_PATTERNS = [
    r'\bTBD\b',
    r'\bUNKNOWN\b',
    r'\bNOT_SPECIFIED\b',
    r'\bNOT_SPECIFIED_IN_PERIZIA\b',
    r'\bnull\b',
    r'\bundefined\b',
]
REQUIRED_TEXT_MARKERS = [
    'NEXODIFY REPORT PERIZIA',
    '2. Panoramica',
    '3. Lots / Composizione Lotto',
    '4. Decisione Rapida',
    '5. Costi',
    '6. Legal Killers',
    '7. Dettagli per bene',
    '8. Red Flags',
    '9. Disclaimer / Footer',
]


@dataclass
class ValidationResult:
    ok: bool
    failures: List[str]
    warnings: List[str]
    analysis_id: str
    pdf_path: str
    checks: Dict[str, Any]


def _load_db() -> Any:
    cfg = dotenv_values(str(ENV_PATH))
    cli = MongoClient(cfg['MONGO_URL'])
    return cli[cfg['DB_NAME']]


def _extract_pdf_text(pdf_path: Path) -> str:
    reader = PdfReader(str(pdf_path))
    return '\n'.join((p.extract_text() or '') for p in reader.pages)


def _bbox_overflow_check(pdf_path: Path, margin_tol: float = 1.5) -> Tuple[bool, List[str]]:
    issues: List[str] = []
    doc = fitz.open(str(pdf_path))
    try:
        for page_idx, page in enumerate(doc, start=1):
            rect = page.rect
            blocks = page.get_text('blocks')
            for b in blocks:
                x0, y0, x1, y1, text = b[:5]
                t = (text or '').strip()
                if not t:
                    continue
                if x0 < -margin_tol or y0 < -margin_tol or x1 > rect.width + margin_tol or y1 > rect.height + margin_tol:
                    issues.append(
                        f'page {page_idx}: text block out of page bounds [{x0:.2f},{y0:.2f},{x1:.2f},{y1:.2f}]'
                    )
    finally:
        doc.close()
    return len(issues) == 0, issues


def _latest_real_analysis(db: Any) -> Dict[str, Any]:
    cursor = db.perizia_analyses.find({}, {'_id': 0}).sort('created_at', -1)
    for doc in cursor:
        user_id = str(doc.get('user_id') or '').lower()
        if user_id in {'offline_qa', 'test'} or user_id.startswith('test-') or user_id.startswith('test_'):
            continue
        return doc
    raise RuntimeError('No suitable real analysis found')


def _load_analysis(db: Any, analysis_id: str, latest_real: bool) -> Dict[str, Any]:
    if analysis_id:
        doc = db.perizia_analyses.find_one({'analysis_id': analysis_id}, {'_id': 0})
        if not doc:
            raise RuntimeError(f'analysis_id not found: {analysis_id}')
        return doc
    if latest_real:
        return _latest_real_analysis(db)
    raise RuntimeError('Provide --analysis-id or --latest-real')


def _run_validation(analysis: Dict[str, Any], pdf_path: Path, debug_meta: Dict[str, Any]) -> ValidationResult:
    failures: List[str] = []
    warnings: List[str] = []

    text = _extract_pdf_text(pdf_path)
    text_upper = text.upper()

    checks: Dict[str, Any] = {
        'template_version': debug_meta.get('template_version'),
        'brand_asset_path': debug_meta.get('brand_asset_path'),
        'required_sections_contract': REQUIRED_SECTIONS,
        'legacy_markers_contract': LEGACY_SECTION_MARKERS,
    }

    # Required sections
    missing_markers = [m for m in REQUIRED_TEXT_MARKERS if m.upper() not in text_upper]
    checks['required_text_markers'] = {'expected': REQUIRED_TEXT_MARKERS, 'missing': missing_markers}
    if missing_markers:
        failures.append(f'Missing required section markers: {missing_markers}')

    # No placeholders
    placeholder_hits: List[str] = []
    for pat in BANNED_PLACEHOLDER_PATTERNS:
        if re.search(pat, text, flags=re.IGNORECASE):
            placeholder_hits.append(pat)
    checks['placeholder_hits'] = placeholder_hits
    if placeholder_hits:
        failures.append(f'Banned placeholders found in PDF text: {placeholder_hits}')

    # No legacy sections leakage
    legacy_hits = [marker for marker in LEGACY_SECTION_MARKERS if marker.upper() in text_upper]
    checks['legacy_hits'] = legacy_hits
    if legacy_hits:
        failures.append(f'Legacy sections leaked into new PDF: {legacy_hits}')

    # Preflight overflow risk
    preflight = debug_meta.get('preflight') or {}
    checks['preflight'] = preflight
    if preflight.get('has_long_unbroken_tokens'):
        failures.append('Layout preflight detected long unbroken tokens with overflow risk')

    # Rendered text bbox bounds (overflow/clipping proxy)
    bbox_ok, bbox_issues = _bbox_overflow_check(pdf_path)
    checks['bbox_overflow'] = {'ok': bbox_ok, 'issues': bbox_issues[:20]}
    if not bbox_ok:
        failures.append('Detected text blocks outside page bounds (possible clipping/overflow)')

    # Ensure section headers present when payload has content
    payload = debug_meta.get('payload_snapshot') or {}
    conditional = []
    if payload.get('lots'):
        conditional.append(('3. Lots / Composizione Lotto', True))
    if payload.get('legal_killers'):
        conditional.append(('6. Legal Killers', True))
    if payload.get('dettagli_beni'):
        conditional.append(('7. Dettagli per bene', True))
    if payload.get('red_flags'):
        conditional.append(('8. Red Flags', True))

    conditional_missing = [h for h, req in conditional if req and h.upper() not in text_upper]
    checks['conditional_sections'] = {'required': conditional, 'missing': conditional_missing}
    if conditional_missing:
        failures.append(f'Payload had data but section header missing: {conditional_missing}')

    # Branding/watermark checks (text-level deterministic proxy)
    branding_ok = 'NEXODIFY' in text_upper and bool(debug_meta.get('brand_asset_path'))
    checks['branding'] = {
        'branding_text_present': 'NEXODIFY' in text_upper,
        'brand_asset_path': debug_meta.get('brand_asset_path'),
        'watermark_enabled': bool((debug_meta.get('watermark') or {}).get('enabled')),
    }
    if not branding_ok:
        failures.append('Branding marker missing (NEXODIFY text or asset metadata absent)')

    # Template guard
    if debug_meta.get('template_version') != TEMPLATE_VERSION:
        failures.append(
            f"Unexpected template version {debug_meta.get('template_version')} (expected {TEMPLATE_VERSION})"
        )

    return ValidationResult(
        ok=not failures,
        failures=failures,
        warnings=warnings,
        analysis_id=str(analysis.get('analysis_id') or ''),
        pdf_path=str(pdf_path),
        checks=checks,
    )


def main() -> None:
    ap = argparse.ArgumentParser(description='Deterministic QA validation for regenerated perizia PDFs.')
    ap.add_argument('--analysis-id', default='')
    ap.add_argument('--pdf', default='', help='Validate an existing PDF path instead of regenerating')
    ap.add_argument('--latest-real', action='store_true', help='Use latest suitable real analysis in DB')
    ap.add_argument('--out-dir', default='/tmp/perizia_pdf_validation')
    ap.add_argument('--json-out', default='')
    args = ap.parse_args()

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    db = _load_db()
    analysis = _load_analysis(db, args.analysis_id.strip(), args.latest_real)
    analysis_id = str(analysis.get('analysis_id'))

    pdf_arg = str(args.pdf or '').strip()
    pdf_bytes, debug_meta = build_perizia_pdf_document(analysis, analysis.get('result') or {})
    if pdf_arg:
        pdf_path = Path(pdf_arg).resolve()
        if not pdf_path.exists():
            raise RuntimeError(f'--pdf path not found: {pdf_path}')
    else:
        pdf_path = out_dir / f'nexodify_report_{analysis_id}.pdf'
        pdf_path.write_bytes(pdf_bytes)
    debug_path = out_dir / f'nexodify_report_{analysis_id}.debug.json'
    debug_path.write_text(json.dumps(debug_meta, ensure_ascii=False, indent=2), encoding='utf-8')

    result = _run_validation(analysis, pdf_path, debug_meta)

    output = {
        'ok': result.ok,
        'analysis_id': result.analysis_id,
        'pdf_path': result.pdf_path,
        'debug_path': str(debug_path),
        'failures': result.failures,
        'warnings': result.warnings,
        'checks': result.checks,
    }

    if args.json_out:
        Path(args.json_out).resolve().write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding='utf-8')

    print(json.dumps(output, ensure_ascii=False, indent=2))
    if not result.ok:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
