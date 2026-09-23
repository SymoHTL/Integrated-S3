---
name: benchmark-gate
description: How to run the BenchmarkDotNet regression gate, and where it lies - it only works in-process, the baseline holds only for the machine and toolchain it was recorded on (BDN 0.15.2, now 0.15.8), a missing benchmark only warns, and a compare without a fresh run compares whatever stale artifacts the checkout holds and can print PASS.
metadata:
  type: reference
---

**Recipe:**

1. `bash scripts/bench.sh '*'` runs every benchmark into `benchmarks/artifacts`. It takes about 6
   minutes.
2. `bash scripts/bench-compare.sh` compares `benchmarks/artifacts` with `benchmarks/baseline`. On
   the maintainer's machine, `python` is the Microsoft Store stub, so run it as
   `PYTHON=py bash scripts/bench-compare.sh`.
3. It fails when a benchmark's mean grows by more than 15 % or its allocations grow at all.
4. A regression is fixed, or the baseline is re-recorded (`make bench-baseline`, or
   `bench-compare.sh --update-baseline`) in the same PR, with the reason in the PR description.
   Re-record on the baseline machine, and in the same PR as any BenchmarkDotNet, SDK or runtime
   bump.

**Where it lies:**

- **Stale input.** `bench-compare.sh` compares whatever is in `benchmarks/artifacts`, which is
  gitignored. A fresh checkout has none and exits 2 ("no current benchmark results found"). A
  checkout that still holds the 2026-07-04 run the baseline was promoted from compares that run
  with itself: every row +0.0 %, "PASS", exit 0.
- **A missing benchmark only warns.** A baseline benchmark missing from the run prints `WARNING`
  and still passes. A new benchmark is not gated.
- **Stale baseline.** The baseline (`benchmarks/baseline/README.md`) was captured on 2026-07-04 on
  a Ryzen 9 9950X3D, with SDK 10.0.204, runtime 10.0.9 and BenchmarkDotNet 0.15.2.
  `Directory.Packages.props` now pins BenchmarkDotNet 0.15.8.
- **In-process only.** `HotPathBenchmarkConfig` uses `InProcessEmitToolchain`. BenchmarkDotNet's
  generated out-of-process project breaks under this repo's `TreatWarningsAsErrors`, SourceLink and
  analyzers.
- **Never in CI.** The `benchmarks` CI job needs a self-hosted runner labelled `benchmarks`, and
  it has never run ([[ci-green-proves-less-than-you-think]]).

History: PR #234 (572d666) replaced the earlier Stopwatch harness with BenchmarkDotNet and checked
the gate in both directions: a self-compare passes, and a synthetic +20 %/+10 % run fails.
`README.md` still calls the harness Stopwatch-based (#269).

**Why:** a PASS from a stale or partial run looks exactly like a PASS from a real one.

**How to apply:** always run `bench.sh` right before `bench-compare.sh`. Read the `WARNING` lines as
failures.

Gate: local `bench-compare.sh` only. #270 proposes an allocation-only gate on a hosted runner that
fails on a missing benchmark.
