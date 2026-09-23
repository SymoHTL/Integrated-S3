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

# Validate AOT/trimming compatibility (what the dispatch-only heavy CI job and the release workflow run)
pwsh -File eng/Invoke-AotPublishValidation.ps1
```

## Quality bar and design conventions

[CLAUDE.md](CLAUDE.md) holds the rules every change must meet, for human and AI contributors alike. Each rule names the test or CI step that goes red when it is broken. A rule that nothing enforces yet is marked **HAZARD** and links the issue that will add its gate. The file also lists the ways a local run can report green without testing your change, such as a filter that matches nothing, stale binaries, or tests that return early.

Know what CI does not check: the CI section of [CLAUDE.md](CLAUDE.md#ci) says what runs on every PR and what runs only on dispatch.

## Pull requests

1. Fork (or branch, if you have write access) from `main`.
2. Keep PRs focused — one logical change per PR.
3. Make sure `dotnet build` and `dotnet test` pass locally with zero warnings.
4. Update documentation affected by your change (`README.md`, `docs/`, XML doc comments) — in particular the capability matrix for provider-support changes.
5. Add a short entry to the `Unreleased` section of [CHANGELOG.md](CHANGELOG.md) for user-visible changes.
6. Fill in the pull request template. Link the issue the PR addresses.
7. A maintainer (see [CODEOWNERS](.github/CODEOWNERS)) will review your PR. CI on the PR's head commit must have finished green before merge.

## Commit messages

- Use concise, imperative subject lines (`Add UploadPartCopy support to disk provider`).
- Reference issues where relevant (`Fixes #123`).

## Releases

Releases are cut by maintainers, following the Releases section of [CLAUDE.md](CLAUDE.md#releases).

## License

By contributing, you agree that your contributions will be licensed under the [BSD 3-Clause License](LICENSE).
