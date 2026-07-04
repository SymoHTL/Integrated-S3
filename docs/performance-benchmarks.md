# IntegratedS3 benchmarks & E2E tests

This repository ships two local-first test suites for catching correctness and performance regressions
early, both designed to run on a developer machine (or a self-hosted runner) with **no paid CI**:

- **`IntegratedS3.Benchmarks`** — a real [BenchmarkDotNet](https://benchmarkdotnet.org/) micro-benchmark
  suite over the hot paths, reporting mean time **and** allocations.
- **`IntegratedS3.E2E.Tests`** — end-to-end tests that boot the real host on a Kestrel loopback socket
  with the Disk provider and drive it with a genuine AWS SDK for .NET S3 client.

Perf benchmarks are deliberately kept off GitHub-hosted CI: shared runners have no CPU isolation, so
benchmark numbers there are noise. The free CI runs only fast correctness checks; the heavy suites are
opt-in (see [CI strategy](#ci-strategy)).

## Benchmarks

The suite (`src/IntegratedS3/IntegratedS3.Benchmarks`) uses BenchmarkDotNet's in-process (emit) toolchain
— this repository sets `TreatWarningsAsErrors=true` together with SourceLink and code-style analyzers,
which break BenchmarkDotNet's default out-of-process generated project; the in-process toolchain runs the
already-optimized assembly directly and still measures allocations via `MemoryDiagnoser`. (Runs must use
`-c Release`, which the scripts do.)

| Benchmark class | Hot paths |
| --- | --- |
| `SigV4Benchmarks` | SigV4 canonical-request build, string-to-sign, HMAC signature; SigV4a ECDSA key derivation / sign / verify |
| `S3XmlBenchmarks` | `WriteListBucketResult` at 1 / 100 / 1000 keys; parse CompleteMultipartUpload & DeleteObjects |
| `ChecksumBenchmarks` | MD5 (ETag), SHA-1, SHA-256, CRC-32C over 64 KiB / 1 MiB / 8 MiB |
| `DiskObjectBenchmarks` | Disk provider PutObject / GetObject / multipart Complete |
| `DiskListingBenchmarks` | ListObjects over 1 / 100 / 1000 objects |

### Running

```bash
scripts/bench.sh '*'                 # run everything (Release); pass a BenchmarkDotNet filter to narrow
scripts/bench-compare.sh             # gate the last run against benchmarks/baseline/*.json
scripts/bench-compare.sh --update-baseline   # promote the last run to the committed baseline
```

`scripts/bench.ps1` / `scripts/bench-compare.ps1` are the PowerShell equivalents; `make bench` /
`make bench-compare` / `make bench-baseline` wrap them.

### Regression gate

`scripts/bench-compare.sh` reads the BenchmarkDotNet `JsonExporter.Full` reports for the current run and
the committed baseline, matches benchmarks by full name + parameters, and **fails (exit 1) when any
benchmark's mean time regresses by more than 15% or its allocations grow**. Thresholds are tunable with
`--mean-threshold` / `--alloc-threshold`.

Baselines are **hardware-specific**; the committed set and the capture hardware are documented in
[`benchmarks/baseline/README.md`](../benchmarks/baseline/README.md). Regenerate the baseline on the
machine that will run the gate.

## E2E tests

`IntegratedS3.E2E.Tests` boots the reference host (`WebUiApplication`) on `http://127.0.0.1:0` with the
Disk provider and a seeded SigV4 credential, then drives it with `AmazonS3Client` (path-style, HTTP). It
is split by trait:

- **`Suite=Smoke`** (fast, < ~30s, offline): bucket + object CRUD, listing, a 404 path, presigned round
  trip, plus the pure protocol property tests (S3 XML write↔reparse, SigV4 canonicalization invariants,
  SigV4a sign/verify).
- **`Suite=Full`**: multipart upload / complete / abort, list v1 & v2 with prefix/delimiter/pagination,
  versioning + delete markers, conditional GETs (304 / 412), and the 403 bad-signature path.

```bash
scripts/e2e-smoke.sh    # fast smoke subset
scripts/e2e.sh          # full suite
scripts/soak.sh 20      # optional: run the full suite in a loop (leak / flakiness soak)
```

## CI strategy

`.github/workflows/ci.yml` runs, automatically on push/PR, a single lean **free-tier** job: restore +
build + unit/integration tests + the `Suite=Smoke` E2E subset (all offline). Everything heavier is opt-in
via `workflow_dispatch`:

- `run-heavy` → full E2E (Smoke + Full), AOT publish validation, and coverage across ubuntu + windows.
- `run-benchmarks` → the BenchmarkDotNet suite + regression gate, on a **self-hosted** runner labelled
  `benchmarks` only (never on hosted runners).

To wire a self-hosted benchmark runner later: register a runner on a quiet machine with labels
`[self-hosted, benchmarks]`, commit a baseline generated on that box, then dispatch CI with
`run-benchmarks=true`.

## Optional local pre-push gate

`scripts/install-hooks.sh` points `core.hooksPath` at `scripts/hooks/`, so `pre-push` runs build +
unit/integration tests + the fast E2E smoke before every push. Bypass once with `git push --no-verify`.
