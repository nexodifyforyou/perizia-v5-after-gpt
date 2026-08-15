#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

cd "$REPO_ROOT/backend"
ISOLATED_ARTIFACT_ROOT="$(mktemp -d /tmp/perizia_eight_case_artifacts.XXXXXX)"
trap 'rm -rf -- "$ISOLATED_ARTIFACT_ROOT"' EXIT
export CORRECTNESS_V2_ARTIFACTS_ROOT="$ISOLATED_ARTIFACT_ROOT"
export PERIZIA_PYTEST_ACTIVE=1
.venv/bin/python -m pytest -q \
  correctness_v2/tests/test_seven_case_customer_report_regression.py \
  correctness_v2/tests/test_beta_multilot_case_regression.py
