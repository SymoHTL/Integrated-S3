#!/usr/bin/env bash
# Compare the latest benchmark run against the committed baseline (the regression gate).
# Usage:
#   scripts/bench-compare.sh                 # compare; exit 1 on regression
#   scripts/bench-compare.sh --update-baseline   # promote current run to baseline
#   scripts/bench-compare.sh --mean-threshold 0.15 --alloc-threshold 0.0
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PY="${PYTHON:-python}"
exec "$PY" "$SCRIPT_DIR/bench-compare.py" \
  --baseline "$REPO_ROOT/benchmarks/baseline" \
  --current "$REPO_ROOT/benchmarks/artifacts" \
  "$@"
