---
name: ci-green-proves-less-than-you-think
description: A green CI run on a push or PR proves an ubuntu build, IntegratedS3.Tests and E2E Smoke - nothing about AOT, Windows, E2E Full or benchmarks - and nothing requires even that before merge: 27 of 31 merged PRs in #206-#237 merged before their CI finished.
metadata:
  type: project
---

What runs where (`.github/workflows/ci.yml`):

| Job | Trigger | What it proves |
|---|---|---|
| `build-test-smoke` | push to `main`, PRs | ubuntu Release build, `IntegratedS3.Tests`, E2E `Suite=Smoke` |
| `knowledge-lint` | push to `main`, PRs | `INDEX.md` and `knowledge/` pass `scripts/lint_knowledge.py` |
| `heavy` | `workflow_dispatch` with `run-heavy` | full suite with E2E Full and coverage, AOT script, on ubuntu and windows |
| `benchmarks` | `workflow_dispatch` with `run-benchmarks`, self-hosted runner | BenchmarkDotNet regression gate |

The record:

- **`heavy` has run once**: run 34780275969 on 2026-09-13 at b17fd00, green on both systems.
  Windows file semantics (#120) are exercised only by its windows leg.
- **`benchmarks` has never run.** No runner is labelled `benchmarks`.
- **`cancel-in-progress` covers every ref**, so pushes to `main` cancel each other. 43 of the 74
  `main` runs in July 2026 were cancelled.
- **Nothing requires CI before merge.** Ruleset 13904561 has only the deletion and non-fast-forward
  rules. 27 of the 31 merged PRs in #206–#237 merged before any CI run on their head sha finished;
  #208 merged 4 seconds after its checks started.
- **#140 passed every gate.** It was reflection-based JSON in the EF package. The AOT script
  publishes only `WebUi`, which does not reference `IntegratedS3.EntityFramework`, and no library
  sets `IsAotCompatible` (#264).
- **Gates have been loosened to get green before.** 791a286 relaxed the CI build warning gate "to
  match current repo baseline" (2026-03-21), and NuGet audit codes were demoted until #130
  ([[cve-suppression-outlives-reason]]).

**Why:** "CI is green" is read as "the change is safe". Here it covers one OS and one test subset,
and a merge does not even wait for it.

**How to apply:**

- Before merging, wait for the head sha's `build-test-smoke` run to finish green.
- For serialization, reflection, DI wiring or filesystem changes, dispatch `heavy` and wait for
  the result: `gh workflow run ci.yml -R SymoHTL/Integrated-S3 --ref <branch> -f run-heavy=true`.
  A hot-path change has no gate: `heavy` measures no performance, and `benchmarks` has never run.
- A red gate is fixed, never relaxed.

Gate: none (#270 for merge gating, #264 for AOT).
