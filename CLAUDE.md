# CLAUDE.md — Integrated-S3

Rules for changing this repo, for agents and humans alike. Each rule names the test or CI step
that goes red when it is broken. A rule with no such gate is a labelled **HAZARD** with its ticket.
Open work lives in GitHub issues (`gh -R SymoHTL/Integrated-S3`). The stories behind the rules live
in `knowledge/`, indexed by `INDEX.md`. The consumer app is `SymoHTL/PersonalS3` (no hyphen; branch
`master`), which pins these packages from nuget.org.

## Build & Test

Run everything from the repo root. `make` is not installed in the maintainer's git-bash, so call
the scripts directly.

| What | Command | Time |
|---|---|---|
| Build | `dotnet build src/IntegratedS3/IntegratedS3.slnx -c Release` | 28 s |
| Unit + integration (CI) | `dotnet test src/IntegratedS3/IntegratedS3.Tests/IntegratedS3.Tests.csproj -c Release --no-build` | 30-34 s |
| E2E smoke (CI) | `dotnet test src/IntegratedS3/IntegratedS3.E2E.Tests/IntegratedS3.E2E.Tests.csproj -c Release --no-build --filter "Suite=Smoke"` | 4 s |
| E2E smoke + full | `bash scripts/e2e.sh` | 7 s |
| Everything, with coverage (what `heavy` runs) | `dotnet test src/IntegratedS3/IntegratedS3.slnx -c Release --no-build --collect:"XPlat Code Coverage"` | ~2 min |
| AOT gate | `pwsh -File eng/Invoke-AotPublishValidation.ps1` | 67 s |
| Benchmarks | `bash scripts/bench.sh '*'`, then `bash scripts/bench-compare.sh` | ~6 min |

Times were measured on 2026-09-23 on the maintainer's machine with a warm NuGet cache. There,
`python` resolves to the Microsoft Store stub (exit 49), so run the comparison as
`PYTHON=py bash scripts/bench-compare.sh`.

- `IntegratedS3.Tests` has about 1,300 tests and no `Suite` traits. `IntegratedS3.E2E.Tests` has
  22: 16 `Suite=Smoke`, 6 `Suite=Full`. xUnit v2 on VSTest. An untagged E2E class never runs in
  automatic CI.
- `IntegratedS3EndpointRouteBuilderExtensions.cs` is over 600 KB and 12,550 lines: grep it, then
  read line ranges.

### Stale binaries & phantom results: check BEFORE debugging the diff

1. **A filter that matches nothing is a green run.** `--filter "Suite=Smok"` prints "No test
   matches the given testcase filter" and exits 0. Check what ran: the summary must say
   `Total: N` with N > 0.
2. **"Skipped: 0" does not mean everything ran.** A test that `return`s early when its
   environment is missing counts as Passed. The 28 in `S3CompatibleEndpointConformanceTests` need
   the `INTEGRATEDS3_S3COMPAT_*` variables, which CI never sets. The 5 virtual-hosted-style tests
   in `IntegratedS3AwsSdkCompatibilityTests` do the same on a machine where `*.localhost` does not
   resolve to loopback; it does on the ubuntu runners and on Windows 11 (HAZARD, #263).
3. **`--no-build` runs whatever is in `bin/`**, months-old binaries or Debug instead of Release,
   without a warning. Chain it after a build with `&&`, never `;`, and pass `-c Release` to both.
   `--no-restore` after a `Directory.Packages.props` bump builds against the old package, too.
4. **The AOT gate can print "0 warnings" without compiling anything.** After a plain
   `dotnet publish`, ILC was skipped and the script passed in 8 s. An honest run takes about a
   minute and its log (`$TEMP/IntegratedS3-aot-publish.log`, one file for every worktree on the
   machine) contains "Generating native code" once. If it does not, delete
   `src/IntegratedS3/WebUi/obj/Release/net10.0/win-x64` and rerun (HAZARD, #264).
5. **An AOT publish fails in the Claude Code shell on Windows** with `link.exe … exited with code
   123` after "'vswhere.exe' is not recognized", because that shell sets
   `NoDefaultCurrentDirectoryInExePath`. Prefix the command with
   `env -u NoDefaultCurrentDirectoryInExePath`.
6. **TestServer is not Kestrel, and HttpClient is not the AWS SDK.** TestServer enforces no request
   body limit, and HttpClient validates no response checksums, so #81 (uploads over 28.6 MiB got
   413) and #233 (every ranged GET rejected by AWS SDK v4) passed the whole in-process suite. Wire
   behaviour goes through `WebUiApplicationFactory.CreateLoopbackIsolatedClientAsync` and
   `AmazonS3Client`.
7. **A running host locks `bin/`.** A no-change build still "succeeds"; the first real rebuild
   fails with MSB3027/MSB3021 ("The file is locked by: WebUi"). Check
   `tasklist | grep -iE "WebUi|testhost"` first; `dotnet build-server shutdown` after.
8. **Restore needs nuget.org.** With NuGetAudit and warnings-as-errors, an unreachable feed fails
   restore with NU1900 on every project, and a newly published advisory turns an unchanged commit
   red. That is the audit gate working, not a flake.
9. **CI runs the newest 10.0 SDK; local runs what `global.json` rolls forward to.** With
   `AnalysisLevel=latest` and warnings-as-errors, a newer analyzer can fail CI while the local
   build is green.
10. **Text search has blind spots.** `IntegratedS3.Tests/CorrelationIdValidationTests.cs` holds a
    raw NUL byte, so `git grep` and `grep` print only "Binary file … matches", `git diff` shows no
    lines, and ripgrep-based tools skip the file; use `grep -a`. Five test files declare
    private classes named `ScopeBasedIntegratedS3AuthorizationService`, like the production class
    in `IntegratedS3.Core`: check the path before trusting a class-name hit.
11. **`bench-compare.sh` compares whatever is in `benchmarks/artifacts`**, which is gitignored. A
    checkout that still holds the run the baseline was promoted from prints PASS without a fresh
    run, and a benchmark missing from the run only warns (HAZARD, #270).

## Tests: mandatory for every change

Clauses 1 to 5 each come from defects that passed a green suite here.

1. **An error test asserts the S3 `<Code>` and the HTTP status, through the endpoint.** They come
   from two independent sources (`SuggestedHttpStatusCode ?? ToStatusCode(code)` and
   `ToS3ErrorCode(code)`), and seven defects shipped a wrong code, a wrong status or both (#118,
   #139, #147, #150, #152, #157, #164). A fake service leaves `SuggestedHttpStatusCode` unset so
   the mapping runs.
2. **A write test reads the write back**: the bytes, headers and tags. A list test pins the full
   ordered key sequence and the continuation tokens.
3. **Wire behaviour a real server or SDK enforces is tested on the loopback Kestrel host with
   `AmazonS3Client`**, not TestServer and HttpClient (phantom result 6).
4. **Crypto is pinned by an external known-answer vector**, never only by sign-then-verify. In
   #103, signer and verifier shared the same SigV4a key-derivation bug and the round trip was
   green.
5. **A test that needs an environment reports Skipped** (a `Fact` subclass that sets `Skip`), and
   never `return`s early (#263).
6. **Seen red first.** Run the new test against the code without the fix and watch it fail; the PR
   says where it was seen red.
7. **A bug is a class.** A provider-level bug goes into the contract harness in
   `IntegratedS3.Testing` (#268), a SigV4 fix goes into its SigV4a twin, and an endpoint fix goes
   into every endpoint of the same shape.

Gate: review only. HAZARD until the automated reviewer exists (#270).

## CI

- **`ci.yml` `build-test-smoke`** runs on push to `main` and on every PR: restore, Release build
  (warnings are errors), `IntegratedS3.Tests`, E2E `Suite=Smoke`. It is the only automatic test
  gate. `cancel-in-progress` is on for every ref, so pushes to `main` cancel each other: 43 of 74
  `main` runs in July 2026 were cancelled, and a merged sha can end with no finished run.
- **`security-scan.yml`** runs on push to `main`, on every PR, weekly and on dispatch.
  `vulnerable-packages` fails on any advisory outside its allowlist. `codeql` uploads its alerts
  and fails on none of them.
- **`heavy`** runs on `workflow_dispatch` with `run-heavy`: the full suite with E2E Full and
  coverage, plus the AOT script, on ubuntu and windows. Dispatch it before calling done any change
  to serialization, reflection, DI wiring or filesystem semantics:
  `gh workflow run ci.yml -R SymoHTL/Integrated-S3 --ref <branch> -f run-heavy=true`.
- **`benchmarks`** needs a self-hosted runner labelled `benchmarks`. None exists, so it has never
  run, and a hot-path change has no performance gate (HAZARD, #270).
- **`nuget-publish.yml`** runs on dispatch only: `validate` (full suite and AOT script), pack, push
  with `--skip-duplicate`, then tag `v{version}` and create the GitHub Release.
- **HAZARD: merges are not gated.** Ruleset `main protection` blocks only deletion and force-push.
  There is no required check, no required PR and no required conversation resolution. #208 merged
  four seconds after its checks started, and direct pushes to `main` happen (8e6e1b1). See #270.

## Architecture

- `IntegratedS3.Abstractions`: provider-agnostic contracts (`IStorageBackend`, `IStorageService`,
  `StorageError`, capabilities, the object and multipart state stores). No package references.
- `IntegratedS3.Protocol`: the S3 wire protocol (SigV4 and SigV4a signing and parsing, XML). No
  package references.
- `IntegratedS3.Core`: orchestration (`OrchestratedStorageService`, the catalog store, replicas
  and repair, authorization services). References Abstractions and Protocol.
- `IntegratedS3.AspNetCore`: HTTP endpoints (`IntegratedS3EndpointRouteBuilderExtensions`), the SigV4
  authenticator, DI. References Core.
- `IntegratedS3.Provider.Disk` and `IntegratedS3.Provider.S3`: backends. They reference Abstractions
  and Protocol only.
- `IntegratedS3.EntityFramework` (EF catalog and multipart stores), `IntegratedS3.Client`, and
  `IntegratedS3.Testing` (the shipped provider contract harness) are the other packages, 9 in all.
- `WebUi` is the reference host (`PublishAot`, `InvariantGlobalization`). It is composed in
  `WebUiApplication.ConfigureServices` and `ConfigurePipeline`, which the tests'
  `WebUiApplicationFactory` also calls. Host wiring goes there, not into `Program.cs`, or the
  tests stop exercising it (HAZARD, #270). Without `PublishAot` the AOT script passes without
  compiling anything (HAZARD, #264). `WebUi.MvcRazor`, `WebUi.BlazorWasm` and
  `WebUi.BlazorWasm.Client` are samples.
- `IntegratedS3.Tests`, `IntegratedS3.E2E.Tests` and `IntegratedS3.Benchmarks` (BenchmarkDotNet,
  in-process toolchain) are not shipped.

## Critical rules

### A rule ships with its gate (hard rule)

A rule about code ships with the assertion that fails when it is broken, or it does not ship.
Before adding a rule to this file, name the test, analyzer or CI step that goes red when someone
breaks it, and land it in the same PR. If no such gate can be built, what you have is a
**HAZARD**, not a rule: label it as one and file the issue that removes it. A trap written down is
not a trap prevented. `CONTRIBUTING.md` said CI enforced the full suite and AOT while both ran
only on dispatch, and the capability matrix it asks to keep current already disagrees with the
code (#262).

### Code rules

- **Providers fail explicitly.** An unsupported operation returns `StorageError.Unsupported` (501
  `NotImplemented`), never a silent degrade; the `IStorageBackend` default members already do. The
  capability matrix in `docs/protocol-compatibility.md` and the reported `StorageCapabilities`
  change in the same PR. Gate: `ProviderContract_BucketDefaultEncryption_IsExplicitlySupportedOrRejected`,
  for that one feature only. HAZARD for the rest (#262).
- **A new `StorageErrorCode` gets its forward `<Code>` and status and its reverse
  `S3ErrorTranslator` arm in the same PR.** Gate: the forward code and status, for the rows of
  `S3CompatibleBucketSubresource_WhenConfigAbsent_ReturnsNoSuchCodeWithNotFoundStatus` only.
  HAZARD for the rest, the reverse arm included (#261).
- **Client input never reaches a throwing `ToDictionary` or `Parse`.** Duplicates and malformed
  values answer 400 with the S3 code, never 500 (#118: a duplicate `Authorization` parameter; #150:
  an invalid `max-keys`). HAZARD (#270), with live instances in bucket-configuration XML (#278).
- **A new subresource or query parameter lands at every registration point in one PR**: the
  `Known*QueryParameters` allow-lists, the dispatch arm and handler, the error mapping,
  `StorageOperationType`, `AuthorizingStorageService`, the replica write policy, the repair switch,
  both providers (or an explicit `NotImplemented`) and the docs matrix. Missing the allow-list made
  a finished handler unreachable (#153). Each endpoint's own HTTP tests are the only gate. HAZARD
  for the cross-check (#270); 25 replicated operation types have no working repair arm (#275).
- **A missing input is never success.** No credentials, no signing context, no chunk signature, no
  body hash, no resolved version or no health data means reject, or record a failure. Seven defects
  treated absence as "nothing to check" (#82, #86, #101, #114, #126, #127, #131). Gate, each for
  the case it names: `UnsignedRequest_WithSigV4Enabled_IsRejectedWith403`,
  `PutObject_WithTrailerBackedPayloadHashAndTrailerSignatureButNoSigningContext_ReturnsAccessDenied`,
  `PutObject_WithSignedContentSha256NotMatchingBody_ReturnsXAmzContentSHA256Mismatch`, and the
  tampered aws-chunked tests from #208. HAZARD for the rest (#270), and for replica writes, which
  still report success without reaching the replica (#274).
- **A request is copied whole, never rebuilt by listing its properties**: a field added later is
  dropped silently. Repair lost object tags that way (#107). Gate:
  `StorageReplicaRepairService_RepairReplicaObject_PreservesPrimaryObjectTags` (tags only). HAZARD
  for the rest (#270); write-through PutObject still drops 11 of 20 fields (#273).
- **A SigV4 change lands in its SigV4a twin, with the twin's own boundary test.** #132, #133 and
  #161 each needed the same edit in the SigV4 block and the SigV4a block of
  `AwsSignatureV4RequestAuthenticator`. Gate:
  `DeriveEcdsaKey_MatchesAwsCrtKnownAnswerVector` covers the key only. HAZARD (#270), and the
  signature format is wrong today: P1363 where AWS uses DER (#276).
- **Same-key writes are serialized per key and tested concurrently.** Five defects lost a version,
  left two latest rows or mixed up concurrent multipart calls (#84, #110, #111, #123, #124). A
  stored "latest" flag also needs a transaction and a database unique constraint. Gate:
  `DiskStorage_ConcurrentSameKeyPuts_PreserveEveryVersion`,
  `UpsertObjectAsync_ConcurrentWritesToSameKey_LeaveExactlyOneLatest` (flaky, #283). HAZARD for
  new providers until the test lives in the contract harness (#268).
- **Lifting a server default ships its replacement bound.** #93 lifted Kestrel's body limit, and
  #115 (disk-exhaustion DoS) was filed eight hours after it merged. Gate, for the body limit
  only: `IntegratedS3Options_MaxObjectSizeBytes_DefaultsToFiveGibibytes` and
  `KestrelHostedPutObject_ExceedingConfiguredMaxObjectSizeBytes_ReturnsPayloadTooLarge`. HAZARD for
  other defaults (#270).
- **Streaming first: no request path buffers a whole body.** HAZARD, and broken today by signed-PUT
  hashing (#238).
- **Packable libraries stay AOT- and trim-clean, and JSON goes through source-generated contexts
  only.** Gate: the dispatch-only AOT script, which sees only what `WebUi` reaches. #140
  (reflection JSON in the EF package) passed every gate. HAZARD (#264).
- **Layering**: Abstractions and Protocol at the bottom, Core above them, AspNetCore above Core;
  providers reference Abstractions and Protocol only; no EF, AWSSDK or ASP.NET Core reference in the
  core packages. MSBuild rejects cycles, but nothing checks direction. HAZARD (#265).
- **Zero warnings, and no suppression to get green.** Gate: `TreatWarningsAsErrors`, nullable
  warnings as errors, and NuGetAudit in `src/IntegratedS3/Directory.Build.props`. None of them sees
  a new `<NoWarn>` or `#pragma warning disable` (HAZARD, #270). The CVE suppressions are stale
  (#267). The code-style preferences gate nothing, because every one is a
  `:suggestion` (#266).
- **XML is read only through `HardenedXml`** (#104: every S3 XML endpoint expanded DTD entities, a
  billion-laughs DoS), **and written with the UTF-8 writer, never a `StringWriter`** (f93d097:
  rclone broke on a UTF-16 declaration). Gate: `XmlResponses_EmitUtf8EncodingDeclaration`, for the
  error, `ListBucketResult` and `CompleteMultipartUploadResult` writers only. HAZARD for the other
  writers and for readers (#270).
- **No raw control characters in source; write `\0`.** HAZARD (#270).

### Releases

- `VersionPrefix` lives in `src/IntegratedS3/Directory.Build.props`. Bump it with
  `eng/Bump-Version.ps1` (it changes nothing else) in its own release commit, and move
  `CHANGELOG.md` `Unreleased` into the version section in the same commit. Dispatch
  `nuget-publish.yml` with `dry-run` first. nuget.org versions are immutable, and an unbumped run
  goes green while pushing nothing: three green runs on 2026-04-07 shipped nothing. Gate: the
  tag-conflict step, which fails only after that no-op push.
- A new abstract member on a public interface, or a new EF column or index, is a major version,
  with consumer migration notes in `CHANGELOG.md`. The EF stores create their schema with
  `EnsureCreated`, which never alters an existing database, so 10.0.x databases break on 11.0.0
  (#272). HAZARD (#270).
- Consumers move after the release: PersonalS3 bumps its pins in `Directory.Packages.props` (as in
  its #82). A local probe pack gets a unique prerelease version, never a released one: restore never
  replaces a cached version.

### Git & PRs

- One branch per change, in its own worktree off a freshly fetched `origin/main`, or off the
  branch below it for a stacked PR. Never switch branches in a checkout another session may be
  using. No direct pushes to `main` (HAZARD, #270).
- PRs are squash-merged with the PR body as the commit message:
  `gh pr merge <n> --squash --subject "<title> (#<n>)" --body-file <body>`. The repo's default
  squash message is the branch's commit messages instead (HAZARD, #270). Keep the body's claims
  true to the merged code.
- A PR merges only after CI on its head sha has finished green and every review thread is fixed or
  answered with a written reason (HAZARD, not enforced: #270). `heavy` is dispatched and green for
  changes that need it. A benchmark regression is fixed, or the baseline is re-recorded in the same
  PR with the reason.
- `CHANGELOG.md` `Unreleased` gets a line for every user-visible change (HAZARD, #270).

## Where facts go

- **In-flight state** (what is open, filed or released): GitHub issues and PRs only. No task lists,
  handoffs or status in repo markdown; `docs/integrated-s3-implementation-plan.md` is a historical
  snapshot and is not maintained.
- **Durable lessons** (a trap that bit, a postmortem, a recipe): one file per fact in `knowledge/`
  plus one line in `INDEX.md`, added in the PR that learned it.
- **User docs**: `README.md` and `docs/`. The dated audit snapshots
  (`docs/s3-compliance-audit-2026-07-04.md`, `docs/seaweedfs-comparison-2026-07-04.md`) stay as
  they were written.
- **Security findings**: a private draft advisory on the repo's Security tab, never a public issue
  or PR, as `SECURITY.md` asks. The public tracker gets the issue after the fix ships.
- **Rules**: this file, each beside its gate, or labelled HAZARD with its ticket.
  `CONTRIBUTING.md` and `.github/copilot-instructions.md` point here instead of restating them.
- **Private agent memory**: machine- or user-bound facts only. A lesson found there is promoted to
  `knowledge/` before the session ends.
