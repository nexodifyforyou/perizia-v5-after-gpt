from __future__ import annotations

import json
from pathlib import Path

from .corpus_registry import load_cases
from .runner import build_context, write_manifest


def main() -> None:
    rows = []
    for case in load_cases():
        ctx = build_context(case.case_key)
        manifest = write_manifest(ctx)
        rows.append({
            "case_key": case.case_key,
            "label": case.label,
            "run_dir": str(ctx.run_dir),
            "manifest": str(manifest),
            "status": "BOOTSTRAPPED",
        })

    out = Path("/srv/perizia/_qa/canonical_pipeline/bootstrap_summary.json")
    out.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(out)
    print(f"COUNT={len(rows)}")


if __name__ == "__main__":
    main()
