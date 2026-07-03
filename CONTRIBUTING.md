# Contributing to IntegratedS3

Thank you for your interest in contributing! This document describes how to set up a development environment, the quality bar changes must meet, and how to get your work merged.

## Reporting issues

- Search [existing issues](https://github.com/SymoHTL/Integrated-S3/issues) before opening a new one.
- Use the issue templates (bug report / feature request) and fill in every section — especially reproduction steps and the provider (`Disk`, `S3`, or custom) you were using.
- **Do not report security vulnerabilities through public issues.** Follow [SECURITY.md](SECURITY.md) instead.

## Development setup

Prerequisites:

- .NET SDK matching [`global.json`](global.json) (currently `10.0.2xx`; `rollForward: latestFeature` means any newer 10.0 feature band works).
- PowerShell 7+ (`pwsh`) if you want to run the AOT publish validation script.

Clone and validate your environment:

```bash
git clone https://github.com/SymoHTL/Integrated-S3.git
cd Integrated-S3

# Build (warnings are errors — the build must be completely clean)
dotnet build src/IntegratedS3/IntegratedS3.slnx

# Run the full test suite
dotnet test src/IntegratedS3/IntegratedS3.slnx

# Run the reference host
dotnet run --project src/IntegratedS3/WebUi/WebUi.csproj

# Validate AOT/trimming compatibility (what CI runs)
pwsh -File eng/Invoke-AotPublishValidation.ps1
```

## Quality bar

All of the following are enforced by CI (`.github/workflows/ci.yml`) and must pass locally before you open a PR:

1. **Zero warnings.** `TreatWarningsAsErrors` is enabled solution-wide, including nullable-reference warnings and code-style analyzers (`EnforceCodeStyleInBuild`). Do not suppress warnings to get a green build; fix the cause or discuss the suppression in the PR.
2. **All tests pass** on the full solution (`dotnet test src/IntegratedS3/IntegratedS3.slnx`).
3. **AOT/trimming stays clean.** Production code must not introduce `IL2026`/`IL3050` (or related) trim/AOT warnings. Run the AOT validation script when touching serialization, reflection, or DI wiring.
4. **New behavior comes with tests.** Bug fixes include a regression test; features include unit and, where applicable, HTTP-surface integration tests.

## Design conventions

- **Providers fail explicitly.** Operations a provider does not support must return an S3-style `NotImplemented`/unsupported error, never silently degrade. Update the [provider capability matrix](docs/protocol-compatibility.md#provider-capability-matrix) when provider support changes.
- **Layering.** Provider-agnostic contracts live in `IntegratedS3.Abstractions`, orchestration in `IntegratedS3.Core`, wire protocol in `IntegratedS3.Protocol`, and HTTP integration in `IntegratedS3.AspNetCore`. Keep dependencies pointing in that direction.
- **Optional integrations stay optional.** Don't add mandatory dependencies to the core packages (for example, EF Core support lives in its own `IntegratedS3.EntityFramework` package).
- **Custom backends** are validated with the contract-test harness in `IntegratedS3.Testing` — extend it when you extend `IStorageBackend`.

## Pull requests

1. Fork (or branch, if you have write access) from `main`.
2. Keep PRs focused — one logical change per PR.
3. Make sure `dotnet build` and `dotnet test` pass locally with zero warnings.
4. Update documentation affected by your change (`README.md`, `docs/`, XML doc comments) — in particular the capability matrix for provider-support changes.
5. Add a short entry to the `Unreleased` section of [CHANGELOG.md](CHANGELOG.md) for user-visible changes.
6. Fill in the pull request template. Link the issue the PR addresses.
7. A maintainer (see [CODEOWNERS](.github/CODEOWNERS)) will review your PR. CI must be green before merge.

## Commit messages

- Use concise, imperative subject lines (`Add UploadPartCopy support to disk provider`).
- Reference issues where relevant (`Fixes #123`).

## Releases

Releases are cut by maintainers via the `Publish NuGet Packages` workflow, which packs all 9 packages from `src/IntegratedS3/Directory.Build.props` (`VersionPrefix`), pushes them to NuGet.org, tags the commit (`v{version}`), and creates a GitHub Release. Version bumps happen in `Directory.Build.props` together with a matching `CHANGELOG.md` entry.

## License

By contributing, you agree that your contributions will be licensed under the [BSD 3-Clause License](LICENSE).
