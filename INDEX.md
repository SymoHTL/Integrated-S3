# INDEX

One line per entry in `knowledge/`: a link and a hook that carries the payload, so a reader who
never opens the file still avoids the trap. Add the line in the same PR as the entry.
`scripts/lint_knowledge.py` (CI job `Knowledge lint`) fails on the problems its docstring lists, an
entry without a line and a line without an entry among them.

HARD = a trap that bit at least once; read the file before touching what it names.
RECIPE = a short working procedure to copy; a longer one is a skill in `.claude/skills/`. Rules
live in `CLAUDE.md` beside their gates; open work lives in GitHub issues.

- [S3 error code and status diverge](knowledge/s3-error-code-and-status-diverge.md) HARD — the
  status is `SuggestedHttpStatusCode ?? ToStatusCode`, the `<Code>` is `ToS3ErrorCode` (default
  `InternalError`), two hand-kept maps plus a reverse one in `S3ErrorTranslator`; seven defects
  shipped a wrong code or status (#118, #139, #147, #150, #152, #157, #164). Assert both through
  the endpoint, with a fake that leaves `SuggestedHttpStatusCode` unset
- [In-process tests miss the real server and SDK](knowledge/in-process-tests-miss-real-server-and-sdk.md)
  HARD — TestServer has no body limit and HttpClient checks no checksums, so Kestrel's 413 above
  28.6 MiB (#81) and the SDK rejecting every ranged GET (#233) passed the suite; test the wire with
  `CreateLoopbackIsolatedClientAsync` and `AmazonS3Client`
- [A subresource needs every registration point](knowledge/subresource-needs-every-registration-point.md)
  HARD — about ten places, from the `Known*QueryParameters` allow-list to the repair switch; a
  missed allow-list made finished handlers unreachable (#153), and 25 replicated operation types
  have no working repair arm (#275)
- [CI green proves less than you think](knowledge/ci-green-proves-less-than-you-think.md) HARD —
  the only automatic test gate is one ubuntu build, `IntegratedS3.Tests` and E2E Smoke; `heavy` ran
  once, benchmarks never, and 27 of 31 merged PRs in #206–#237 merged before their CI finished.
  Dispatch `heavy` for serialization, reflection, DI or filesystem work
- [Absent state treated as success](knowledge/absent-state-treated-as-success.md) HARD — seven
  times a missing credential, policy, signing context, check, version or health datum meant "pass"
  (#82, #86, #101, #114, #126, #127, #131); replica tag writes and deletes still do (#274). Test
  the absent case, and assert the rejection or the recorded failure
- [Request rebuild drops fields](knowledge/request-rebuild-drops-fields.md) HARD — `PutObjectRequest`
  is a class copied field by field; repair lost tags (#107), and write-through PutObject copies 9 of
  20 properties, dropping SSE and `If-None-Match` (#273). Gate copies with a reflection test
- [Early-return tests report Passed](knowledge/early-return-tests-report-passed.md) HARD — 28
  conformance tests `return` when their environment is missing, so CI says "Skipped 0" while the S3
  provider never ran against a real endpoint (#263); set `Skip` in a `Fact` subclass instead
- [A SigV4 fix needs its SigV4a twin](knowledge/sigv4-fix-needs-sigv4a-twin.md) HARD — twin blocks
  tested by self round trips: #103's key derivation passed every test, #132/#133/#161 each needed
  both blocks, and SigV4a signatures are P1363 where AWS uses DER (#276). Pin crypto with external
  vectors
- [Same-key race leaves two latest rows](knowledge/same-key-race-two-latest-rows.md) HARD — every
  store with a latest flag shipped one (#84, #110, #111, #123, #124; PersonalS3 #62 open); per-key
  stripes, a transaction and the filtered unique index, proven by a concurrent same-key test
- [Grep blind spots](knowledge/grep-blind-spots.md) HARD — a raw NUL byte makes
  `CorrelationIdValidationTests.cs` binary to git, grep and ripgrep (use `grep -a`), five test
  files declare classes named like the production `ScopeBasedIntegratedS3AuthorizationService`, and
  the endpoint file is 12,550 lines
- [A public interface member is a major](knowledge/public-interface-member-is-a-major.md) HARD —
  11.0.0 added eight abstract members, four EF columns and two indexes; `EnsureCreated` never
  alters an existing database, so 10.0.x EF databases fail object reads and writes (#272). Nothing
  packs or API-diffs in CI
- [CVE suppressions outlive their reason](knowledge/cve-suppression-outlives-reason.md) HARD — both
  audit gates still suppress two advisories that no restore has resolved since 2026-09-13, in two
  hand-kept lists (#267); audit codes were demoted to get green once (#130)
- [NuGet release postmortem](knowledge/nuget-release-postmortem.md) HARD — an unbumped publish
  run is green and ships nothing (three on 2026-04-07), and nuget.org versions are immutable; the
  procedure is the `release-and-consume` skill
- [Audit to issues postmortem](knowledge/audit-to-issues-postmortem.md) HARD — about 18 finders at
  once tripped rate limits, one synthesis agent for about 50 issues stalled, and a third of a sweep
  was low severity; the procedure is the `audit-to-issues` skill
- [Benchmark gate](knowledge/benchmark-gate.md) RECIPE — `bench.sh` right before
  `bench-compare.sh` (`PYTHON=py` on the maintainer machine); a stale `benchmarks/artifacts`
  compares to PASS, a missing benchmark only warns, and the baseline predates BenchmarkDotNet
  0.15.8
