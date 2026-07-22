#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

cd "$REPO_ROOT/backend"
exec .venv/bin/python -m pytest -q \
  correctness_v2/tests/test_seven_case_customer_report_regression.py \
  correctness_v2/tests/test_beta_multilot_case_regression.py
