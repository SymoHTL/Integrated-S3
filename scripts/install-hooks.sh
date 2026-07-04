#!/usr/bin/env bash
# Opt-in: point git at the repo's tracked hooks (scripts/hooks/) so the pre-push gate runs before pushes.
# Undo with: git config --unset core.hooksPath
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
chmod +x scripts/hooks/* 2>/dev/null || true
git config core.hooksPath scripts/hooks
echo "Installed: core.hooksPath -> scripts/hooks (pre-push now runs build + unit + fast E2E smoke)."
