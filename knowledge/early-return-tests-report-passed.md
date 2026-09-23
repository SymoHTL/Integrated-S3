---
name: early-return-tests-report-passed
description: 28 conformance tests return early when their environment is missing and xUnit v2 counts them as Passed, so every CI run says "Skipped 0" while the S3 provider has never run against a real endpoint (#263). A test that needs an environment sets Skip; it never returns.
metadata:
  type: project
---

The tests:

- **28 in `S3CompatibleEndpointConformanceTests`** (`[Trait("Category", "LocalS3Compatible")]`).
  26 start with `if (settings is null) return;`, and 2 return the same way inside
  `AssertMultipartCopyChecksumConformanceAsync`. `settings` is null unless the three
  `INTEGRATEDS3_S3COMPAT_*` variables are set, and CI never sets them.
- **5 virtual-hosted-style tests in `IntegratedS3AwsSdkCompatibilityTests`** return when
  `integrateds3-loopback-probe.localhost` does not resolve to loopback. It does on the ubuntu
  runners (they took 37–175 ms in publish run 34785277030) and on Windows 11, so today they run;
  on a machine without `*.localhost` resolution they pass without running.

The 28 report Passed in about a millisecond each. A CI run of `IntegratedS3.Tests` says "Passed 1284,
Skipped 0", and nothing in the output shows that the S3 provider's conformance suite did not run.
That is how the reverse error-map gap in #261 stays invisible.

xUnit here is v2 (2.9.3), which has no runtime skip (`Assert.Skip` arrived in v3). SymoHTL/PersonalS3
solves the same problem with a `FactAttribute` subclass that sets `Skip` in its constructor when the
variables are missing (`DiscordIntegrationFactAttribute`), so its live tests show as skipped.

**Why:** an early `return` is indistinguishable from a pass in every report, so a test that never
ran reads as a test that passed.

**How to apply:** a test that needs an environment uses a `Fact`/`Theory` subclass that sets `Skip`,
and asserts from its first line. When you read a test summary, check "Skipped" against what you
expect to be missing.

Gate: none. #263 adds the attribute and a real endpoint for the `heavy` job.
