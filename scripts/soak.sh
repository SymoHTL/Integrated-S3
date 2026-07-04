#!/usr/bin/env bash
# Optional local soak: run the full E2E suite in a loop to shake out flakiness / resource leaks.
# Usage: scripts/soak.sh [ITERATIONS]   (default 10)
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

ITERATIONS="${1:-10}"
PROJECT="src/IntegratedS3/IntegratedS3.E2E.Tests/IntegratedS3.E2E.Tests.csproj"

echo "==> Building (Release)"
dotnet build "$PROJECT" -c Release --nologo -v minimal

for i in $(seq 1 "$ITERATIONS"); do
  echo "==> Soak iteration $i / $ITERATIONS"
  dotnet test "$PROJECT" -c Release --no-build
done
echo "==> Soak complete: $ITERATIONS iteration(s) passed."
