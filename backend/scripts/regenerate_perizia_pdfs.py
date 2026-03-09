#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any, Dict, List

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

from dotenv import dotenv_values
from pymongo import MongoClient

sys.path.insert(0, str(BACKEND_DIR))
from pdf_report import TEMPLATE_VERSION, build_perizia_pdf_document


ENV_PATH = Path('/srv/perizia/app/backend/.env')
DEFAULT_OUT = Path('/tmp/perizia_regenerated_pdfs')


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_db() -> Any:
    cfg = dotenv_values(str(ENV_PATH))
    mongo_url = cfg.get('MONGO_URL')
    db_name = cfg.get('DB_NAME')
    if not mongo_url or not db_name:
        raise RuntimeError(f'MONGO_URL/DB_NAME missing in {ENV_PATH}')
    cli = MongoClient(mongo_url)
    return cli[db_name]


def _is_real_user(user_id: str) -> bool:
    u = (user_id or '').strip().lower()
    if not u:
        return False
    if u in {'offline_qa', 'test', 'tester'}:
        return False
    if u.startswith('test-') or u.startswith('test_'):
        return False
    return True


def _select_analyses(db: Any, analysis_id: str, latest_real: bool, limit: int) -> List[Dict[str, Any]]:
    coll = db.perizia_analyses
    if analysis_id:
        doc = coll.find_one({'analysis_id': analysis_id}, {'_id': 0})
        if not doc:
            raise RuntimeError(f'analysis_id not found: {analysis_id}')
        return [doc]

    cursor = coll.find({}, {'_id': 0}).sort('created_at', -1)
    selected: List[Dict[str, Any]] = []
    for doc in cursor:
        if latest_real and not _is_real_user(str(doc.get('user_id') or '')):
            continue
        selected.append(doc)
        if len(selected) >= limit:
            break
    if not selected:
        raise RuntimeError('no analyses available for selection')
    return selected


def regenerate(db: Any, analyses: List[Dict[str, Any]], out_dir: Path, write_back: bool) -> Dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    coll = db.perizia_analyses

    generated: List[Dict[str, Any]] = []
    for analysis in analyses:
        analysis_id = str(analysis.get('analysis_id') or '').strip()
        result = analysis.get('result') or {}
        pdf_bytes, meta = build_perizia_pdf_document(analysis, result)

        pdf_path = out_dir / f'nexodify_report_{analysis_id}.pdf'
        debug_path = out_dir / f'nexodify_report_{analysis_id}.debug.json'
        pdf_path.write_bytes(pdf_bytes)
        debug_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')

        regen_meta = {
            'template_version': TEMPLATE_VERSION,
            'generated_at_utc': _utc_now(),
            'pdf_path': str(pdf_path),
            'debug_path': str(debug_path),
            'pdf_size_bytes': len(pdf_bytes),
        }

        if write_back:
            coll.update_one(
                {'analysis_id': analysis_id},
                {
                    '$set': {
                        'pdf_regeneration.latest': regen_meta,
                        'pdf_template_version': TEMPLATE_VERSION,
                    }
                },
                upsert=False,
            )

        generated.append(
            {
                'analysis_id': analysis_id,
                'user_id': analysis.get('user_id'),
                'created_at': analysis.get('created_at'),
                'file_name': analysis.get('file_name'),
                **regen_meta,
            }
        )

    return {
        'ok': True,
        'count': len(generated),
        'write_back': write_back,
        'items': generated,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description='Regenerate Nexodify perizia PDFs with current template.')
    ap.add_argument('--analysis-id', default='', help='Single analysis_id to regenerate')
    ap.add_argument('--latest-real', action='store_true', help='Select most recent analyses from real (non-test/offline) users')
    ap.add_argument('--limit', type=int, default=1, help='How many analyses to regenerate when --analysis-id is not set')
    ap.add_argument('--out-dir', default=str(DEFAULT_OUT), help='Output directory for regenerated PDFs')
    ap.add_argument('--write-back', action='store_true', help='Write regeneration metadata back into DB')
    args = ap.parse_args()

    db = _load_db()
    analyses = _select_analyses(db, args.analysis_id.strip(), args.latest_real, max(1, args.limit))
    result = regenerate(db, analyses, Path(args.out_dir).resolve(), bool(args.write_back))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
