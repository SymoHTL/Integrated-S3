#!/usr/bin/env bash
# Fast, offline E2E smoke subset (< ~30s) — the pre-push / free-CI gate.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"
exec dotnet test src/IntegratedS3/IntegratedS3.E2E.Tests/IntegratedS3.E2E.Tests.csproj \
  -c Release --filter "Suite=Smoke" "$@"
