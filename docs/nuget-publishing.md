# NuGet Publishing Guide

## Prerequisites

1. **NuGet API Key**: Add a repository secret named `NUGET_API_KEY` in **Settings → Secrets and variables → Actions**.
   - Generate at [nuget.org/account/apikeys](https://www.nuget.org/account/apikeys)
   - Scope: Push new packages and package versions
   - Glob pattern: `IntegratedS3.*`

## Publishing a Release

### Preview Release

1. Go to **Actions → Publish NuGet Packages → Run workflow**
2. Set **version-suffix** to `preview.1` (or `rc.1`, `beta.1`, etc.)
3. Set **Push to NuGet.org** to `true`
4. Set **Dry run** to `false`
5. Click **Run workflow**

This publishes packages like `IntegratedS3.AspNetCore.0.1.0-preview.1.nupkg`.

### Stable Release

1. Go to **Actions → Publish NuGet Packages → Run workflow**
2. Leave **version-suffix** empty
3. Set **Push to NuGet.org** to `true`
4. Set **Dry run** to `false`
5. Click **Run workflow**

This publishes packages like `IntegratedS3.AspNetCore.0.1.0.nupkg`.

### Dry Run (Verification)

1. Run the workflow with **Dry run** = `true` (default)
2. Check the uploaded artifacts to verify packages look correct
3. Re-run with dry run disabled to actually publish

## Versioning

The base version (`0.1.0`) is set in `src/IntegratedS3/Directory.Build.props` as `<VersionPrefix>`.
The workflow's **version-suffix** input appends a prerelease label.

To bump the version for a new release, update `<VersionPrefix>` in `Directory.Build.props`.

## Packages Published

| Package | Description |
|---|---|
| IntegratedS3.Abstractions | Provider-agnostic storage contracts |
| IntegratedS3.Core | Orchestration, policies, authorization |
| IntegratedS3.AspNetCore | ASP.NET Core DI + endpoint mapping |
| IntegratedS3.Protocol | S3 wire protocol (XML, SigV4, presigning) |
| IntegratedS3.Provider.Disk | Disk-backed storage provider |
| IntegratedS3.Provider.S3 | Native AWS S3 storage provider |
| IntegratedS3.EntityFramework | EF Core catalog persistence |
| IntegratedS3.Client | First-party .NET HTTP client |
| IntegratedS3.Testing | Provider contract tests and helpers |
