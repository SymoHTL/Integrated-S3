#!/usr/bin/env bash
# Run the IntegratedS3 BenchmarkDotNet hot-path suite (local only; never in shared CI).
# Usage: scripts/bench.sh [FILTER]   (FILTER is a BenchmarkDotNet glob, default '*')
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

FILTER="${1:-*}"
PROJECT="src/IntegratedS3/IntegratedS3.Benchmarks/IntegratedS3.Benchmarks.csproj"
ARTIFACTS="$REPO_ROOT/benchmarks/artifacts"

echo "==> Building benchmarks (Release)"
dotnet build "$PROJECT" -c Release --nologo -v minimal

echo "==> Running benchmarks (filter: $FILTER)"
rm -rf "$ARTIFACTS"
dotnet run -c Release --no-build --project "$PROJECT" -- --filter "$FILTER" --artifacts "$ARTIFACTS"

echo "==> Results written to $ARTIFACTS/results"
echo "    Compare against baseline: scripts/bench-compare.sh"
