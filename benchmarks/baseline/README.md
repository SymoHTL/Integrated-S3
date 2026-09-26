# IntegratedS3 benchmark baseline

Committed BenchmarkDotNet `JsonExporter.Full` reports used by the regression gate
([`scripts/bench-compare.py`](../../scripts/bench-compare.py)). One `*-report-full.json`
per benchmark class.

## How the baseline was produced

```bash
scripts/bench.sh '*'                 # run the whole suite (Release, in-process toolchain)
scripts/bench-compare.sh --update-baseline   # promote the run into this folder
```

## Baseline hardware / toolchain

Benchmarks are **hardware-specific** — regenerate the baseline on the machine that will run the
gate. This baseline was captured on:

| | |
|---|---|
| CPU | AMD Ryzen 9 9950X3D (16C / 32T, 4.3 GHz base) |
| RAM | 31.5 GB |
| OS | Windows 11 (10.0.26200.7840) |
| .NET | SDK 10.0.204 / runtime 10.0.9, X64 RyuJIT AVX-512 |
| BenchmarkDotNet | 0.15.2, `InProcessEmitToolchain`, Warmup=3 Iteration=6 |
| Captured | 2026-07-04 |

> BenchmarkDotNet reports "Unknown processor" on this box; the CPU above is from `Win32_Processor`.

`ChecksumBenchmarks` was re-recorded on 2026-09-26 on the same machine (Windows 10.0.26200.9457,
runtime 10.0.12, BenchmarkDotNet 0.15.8), when its CRC-32C case began to run the shipped
`Crc32Accumulator`, whose `GetHashBytes` allocates the 4-byte result (32 B per operation). In that
run the untouched `Md5_ETag` case at 1 MiB allocated 46 B against 40 B here: per-operation
allocations of the large payloads carry a few bytes of noise, which the 0% threshold reads as a
regression.

## Regression gate

`scripts/bench-compare.sh` fails (exit 1) when, versus this baseline, any benchmark's **mean time
regresses > 15%** or its **allocations grow at all**. Tune with `--mean-threshold` /
`--alloc-threshold`.

Because shared CI runners have no CPU isolation, this gate is **local / self-hosted only** — it is not
run in hosted GitHub Actions.
