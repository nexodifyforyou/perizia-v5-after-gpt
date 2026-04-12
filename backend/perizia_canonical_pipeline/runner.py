from __future__ import annotations

import argparse
import json
from pathlib import Path

from .contracts import ArtifactName, PipelineContext, PipelinePaths
from .corpus_registry import WORKING_CORPUS_REGISTRY, get_case, list_case_keys


def build_context(case_key: str) -> PipelineContext:
    case = get_case(case_key)
    backend_root = Path("/srv/perizia/app/backend")
    pipeline_root = backend_root / "perizia_canonical_pipeline"
    artifact_root = Path("/srv/perizia/_qa/canonical_pipeline/runs")
    run_dir = artifact_root / case.case_key
    artifact_dir = run_dir / "artifacts"
    artifact_dir.mkdir(parents=True, exist_ok=True)

    return PipelineContext(
        case=case,
        paths=PipelinePaths(
            backend_root=backend_root,
            corpus_registry_path=WORKING_CORPUS_REGISTRY,
            pipeline_root=pipeline_root,
            artifact_root=artifact_root,
        ),
        run_dir=run_dir,
        artifact_dir=artifact_dir,
    )


def write_manifest(ctx: PipelineContext) -> Path:
    manifest = {
        "pipeline_name": "perizia_canonical_pipeline",
        "pipeline_version": "0.1.0",
        "case": {
            "case_key": ctx.case.case_key,
            "label": ctx.case.label,
            "pdf_path": ctx.case.pdf_path,
            "sha256": ctx.case.sha256,
            "size_bytes": ctx.case.size_bytes,
            "analysis_id": ctx.case.analysis_id,
            "case_id": ctx.case.case_id,
        },
        "corpus_registry_path": str(ctx.paths.corpus_registry_path),
        "artifacts_expected": [a.value for a in ArtifactName],
    }
    out = ctx.artifact_dir / ArtifactName.MANIFEST.value
    out.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Canonical pipeline bootstrap runner")
    parser.add_argument("--case", required=True, choices=list_case_keys())
    args = parser.parse_args()

    ctx = build_context(args.case)
    manifest = write_manifest(ctx)

    print(f"BOOTSTRAPPED_CASE={ctx.case.case_key}")
    print(f"RUN_DIR={ctx.run_dir}")
    print(f"ARTIFACT_DIR={ctx.artifact_dir}")
    print(f"MANIFEST={manifest}")


if __name__ == "__main__":
    main()
