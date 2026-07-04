#!/usr/bin/env python3
"""Benchmark regression gate for BenchmarkDotNet Full-JSON exports.

Compares a *current* benchmark run against a committed *baseline* and fails
(exit code 1) when any benchmark's mean time regresses beyond a threshold or its
allocations grow. This is the "sniff out regressions early" gate; it is designed
to run locally / on a self-hosted runner (never on noisy shared CI).

Usage:
    python bench-compare.py --baseline <dir> --current <dir>
                            [--mean-threshold 0.15] [--alloc-threshold 0.0]
    python bench-compare.py --current <dir> --baseline <dir> --update-baseline

Both directories are searched (non-recursively and one level deep under
``results/``) for ``*-report-full.json`` files produced by BenchmarkDotNet's
``JsonExporter.Full``.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import shutil
import sys


def _iter_report_files(directory: str):
    patterns = [
        os.path.join(directory, "*-report-full.json"),
        os.path.join(directory, "results", "*-report-full.json"),
    ]
    seen = set()
    for pattern in patterns:
        for path in glob.glob(pattern):
            real = os.path.realpath(path)
            if real not in seen:
                seen.add(real)
                yield path


def load_benchmarks(directory: str) -> dict:
    """Return {key: {'mean': ns, 'alloc': bytes, 'full': str, 'params': str}}."""
    results: dict = {}
    files = list(_iter_report_files(directory))
    if not files:
        return results
    for path in files:
        with open(path, "r", encoding="utf-8-sig") as handle:
            document = json.load(handle)
        for bench in document.get("Benchmarks", []):
            full = bench.get("FullName", "")
            params = bench.get("Parameters", "") or ""
            key = f"{full} [{params}]" if params else full
            stats = bench.get("Statistics") or {}
            mean = stats.get("Mean")
            memory = bench.get("Memory") or {}
            alloc = memory.get("BytesAllocatedPerOperation", 0) or 0
            if mean is None:
                continue
            results[key] = {"mean": float(mean), "alloc": int(alloc), "full": full, "params": params}
    return results


def update_baseline(current_dir: str, baseline_dir: str) -> int:
    files = list(_iter_report_files(current_dir))
    if not files:
        print(f"ERROR: no *-report-full.json found under {current_dir}", file=sys.stderr)
        return 2
    os.makedirs(baseline_dir, exist_ok=True)
    for path in files:
        shutil.copy2(path, os.path.join(baseline_dir, os.path.basename(path)))
    print(f"Updated baseline: copied {len(files)} report file(s) into {baseline_dir}")
    return 0


def _fmt_ns(ns: float) -> str:
    if ns >= 1_000_000:
        return f"{ns / 1_000_000:.3f} ms"
    if ns >= 1_000:
        return f"{ns / 1_000:.3f} us"
    return f"{ns:.1f} ns"


def compare(baseline: dict, current: dict, mean_threshold: float, alloc_threshold: float) -> int:
    if not baseline:
        print("ERROR: baseline is empty; run the benchmarks and update the baseline first.", file=sys.stderr)
        return 2
    if not current:
        print("ERROR: no current benchmark results found.", file=sys.stderr)
        return 2

    regressions = []
    rows = []
    missing = [k for k in baseline if k not in current]
    new = [k for k in current if k not in baseline]

    for key in sorted(current):
        cur = current[key]
        base = baseline.get(key)
        if base is None:
            rows.append((key, "NEW", _fmt_ns(cur["mean"]), "-", f"{cur['alloc']}B", "-"))
            continue
        mean_ratio = (cur["mean"] / base["mean"]) - 1.0 if base["mean"] else 0.0
        if base["alloc"] > 0:
            alloc_ratio = (cur["alloc"] / base["alloc"]) - 1.0
        else:
            alloc_ratio = 0.0 if cur["alloc"] == 0 else 1.0
        mean_bad = mean_ratio > mean_threshold
        alloc_bad = alloc_ratio > alloc_threshold and cur["alloc"] > base["alloc"]
        status = "OK"
        if mean_bad or alloc_bad:
            status = "REGRESSED"
            regressions.append((key, mean_ratio, alloc_ratio, mean_bad, alloc_bad))
        rows.append((
            key,
            status,
            _fmt_ns(cur["mean"]),
            f"{mean_ratio * 100:+.1f}%",
            f"{cur['alloc']}B",
            f"{alloc_ratio * 100:+.1f}%",
        ))

    name_width = max((len(r[0]) for r in rows), default=10)
    header = f"{'Benchmark':<{name_width}}  {'Status':<10}  {'Mean':>11}  {'dMean':>8}  {'Alloc':>12}  {'dAlloc':>8}"
    print(header)
    print("-" * len(header))
    for name, status, mean, dmean, alloc, dalloc in rows:
        print(f"{name:<{name_width}}  {status:<10}  {mean:>11}  {dmean:>8}  {alloc:>12}  {dalloc:>8}")

    print()
    if missing:
        print(f"WARNING: {len(missing)} baseline benchmark(s) missing from current run:")
        for key in sorted(missing):
            print(f"  - {key}")
    if new:
        print(f"NOTE: {len(new)} new benchmark(s) not in baseline (not gated): {len(new)}")

    print()
    print(f"Thresholds: mean regression > {mean_threshold * 100:.0f}% OR allocations grow > {alloc_threshold * 100:.0f}%")
    if regressions:
        print(f"FAIL: {len(regressions)} regression(s) detected:")
        for key, mean_ratio, alloc_ratio, mean_bad, alloc_bad in regressions:
            reasons = []
            if mean_bad:
                reasons.append(f"mean {mean_ratio * 100:+.1f}%")
            if alloc_bad:
                reasons.append(f"alloc {alloc_ratio * 100:+.1f}%")
            print(f"  - {key}: {', '.join(reasons)}")
        return 1

    print("PASS: no regressions beyond thresholds.")
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="BenchmarkDotNet regression gate.")
    parser.add_argument("--baseline", required=True, help="Directory with committed baseline *-report-full.json.")
    parser.add_argument("--current", required=True, help="Directory with the current run's *-report-full.json.")
    parser.add_argument("--mean-threshold", type=float, default=0.15, help="Max allowed mean increase (fraction). Default 0.15 = 15%%.")
    parser.add_argument("--alloc-threshold", type=float, default=0.0, help="Max allowed allocation increase (fraction). Default 0.0 = any growth fails.")
    parser.add_argument("--update-baseline", action="store_true", help="Copy current results into the baseline directory instead of comparing.")
    args = parser.parse_args(argv)

    if args.update_baseline:
        return update_baseline(args.current, args.baseline)

    baseline = load_benchmarks(args.baseline)
    current = load_benchmarks(args.current)
    return compare(baseline, current, args.mean_threshold, args.alloc_threshold)


if __name__ == "__main__":
    raise SystemExit(main())
