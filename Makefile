# Convenience wrappers for the local benchmark + E2E suites (see scripts/ and benchmarks/baseline/README.md).
.PHONY: help bench bench-compare bench-baseline e2e e2e-smoke soak install-hooks

help:
	@echo "Targets:"
	@echo "  make e2e-smoke      Fast offline E2E smoke subset (pre-push gate)"
	@echo "  make e2e            Full offline E2E suite"
	@echo "  make soak           Local soak (full E2E in a loop)"
	@echo "  make bench          Run the BenchmarkDotNet hot-path suite (local only)"
	@echo "  make bench-compare  Gate the last run against the committed baseline"
	@echo "  make bench-baseline Run benchmarks and promote the result to the baseline"
	@echo "  make install-hooks  Install the pre-push git hook"

e2e-smoke: ; bash scripts/e2e-smoke.sh
e2e:       ; bash scripts/e2e.sh
soak:      ; bash scripts/soak.sh
bench:     ; bash scripts/bench.sh
bench-compare: ; bash scripts/bench-compare.sh
bench-baseline: ; bash scripts/bench.sh '*' && bash scripts/bench-compare.sh --update-baseline
install-hooks: ; bash scripts/install-hooks.sh
