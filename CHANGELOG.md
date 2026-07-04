# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Versions match the `VersionPrefix` in `src/IntegratedS3/Directory.Build.props`, which is the version all 9 published `IntegratedS3.*` NuGet packages share. Entries for 10.0.0–10.0.4 were reconstructed from git history; going forward, the publish workflow tags each published release (`v{version}`) and creates a matching GitHub Release.

## [Unreleased]

### Added

- **Presigned `DELETE`, `HEAD`, and multipart `UploadPart` URLs** — presign issuance and validation now cover `DeleteObject`, `HeadObject`, and `UploadPart` in addition to `GetObject`/`PutObject`.
- **Pluggable SigV4 credential resolution** — new `IIntegratedS3CredentialResolver` abstraction (with a configuration-backed default) so hosts can resolve access keys from external stores and rotate keys without a restart.
- **Object Lock default-retention enforcement in the disk provider** — bucket default retention from the Object Lock configuration now blocks in-window permanent version deletes instead of being stored without effect.
- **`MaxObjectSizeBytes` host option** — object upload endpoints (`PutObject`, `UploadPart`) replace the Kestrel per-request body-size limit so uploads beyond ~28.6 MiB no longer fail with `413`; configurable cap that now **defaults to 5 GiB** (the S3 per-request maximum) instead of being unbounded. Set it to `null` to opt out of the application-level limit.
- **Scheduled maintenance: abandoned multipart upload expiry** — the maintenance job set can expire and abort stale multipart uploads.
- **Replica repair divergence kinds and orphan reconciliation** — repair entries now describe content/metadata/version divergence explicitly, and reconciliation can garbage-collect orphaned provider-side artifacts.
- **Code coverage in CI plus Dependabot** — CI collects and publishes coverage reports; Dependabot keeps NuGet and GitHub Actions dependencies current.
- **Provider capability matrix** — `docs/protocol-compatibility.md` now documents per-provider support, config-only behavior, and `NotImplemented` operations for the disk provider, the S3 provider, and custom-backend defaults.
- **Community and governance files** — `CONTRIBUTING.md`, `SECURITY.md` (private vulnerability reporting), issue and pull request templates, and `CODEOWNERS`.

### Changed

- **Warnings are errors** — the solution builds with `TreatWarningsAsErrors`, nullable warnings as errors, and code-style analysis enforced in build.
- **CI consolidated into a single workflow** — one `ci.yml` (build/test matrix, AOT validation, pack) with concurrency cancellation and NuGet caching; the legacy track-specific workflow was removed.
- **Publishing is traceable** — the NuGet publish workflow tags the release commit (`v{version}`) and creates a GitHub Release with the packed artifacts after a successful push.
- **README claims qualified** — feature bullets now state per-provider limits (SSE, retention/legal hold, config-only bucket subresources) instead of implying uniform support.

### Security

- **Bounded upload size and aws-chunked temp spooling (disk-exhaustion DoS)** — `MaxObjectSizeBytes` now defaults to 5 GiB instead of `null`, so object uploads have an application-level cap out of the box. The `Content-Encoding: aws-chunked` decode path additionally enforces this cap while spooling to a temp file — rejecting a decoded body that exceeds the cap (or whose `x-amz-decoded-content-length` already declares an oversize) with `413 EntityTooLarge` — instead of writing an unbounded stream of client-controlled data to the temp volume.

### Fixed

- **Backslash object-key aliasing in the disk provider** — keys containing `\` no longer normalize onto the same stored path as their `/` counterparts.
- **Disk provider concurrency control** — concurrent mutations of the same bucket/object no longer race on shared metadata files.

### Removed

- **Root process artifacts** — `agent-handoff.md`, `plan.md` (superseded by `docs/integrated-s3-implementation-plan.md`), `analyze_logs.py`, and tracked `.idea` project stubs (now gitignored).

## [10.0.4] - 2026-04-07

### Fixed

- **GetObject bytes metric accuracy** — `integrateds3.storage.operation.bytes` for GetObject now counts actual bytes streamed to the client via a metering stream wrapper, instead of recording the full `TotalContentLength` at response creation time. Previously, a client aborting mid-download would still report the entire object size.
- **HTTP bytes-sent metric timing** — `integrateds3.http.bytes_sent` for GetObject is now recorded after the response stream is fully copied, not before streaming begins.
- **Disk provider listing pagination** — improved pagination logic and added a version-marker index method for correct continuation across versioned listings.
- **Repository links** — corrected the repository name (`Intergrated-S3` → `Integrated-S3`) in package metadata and docs.

## [10.0.3] - 2026-03-27

### Changed

- **EntityFramework storage internals** — refactored EF-backed storage classes for more robust `DbContext` resolution and error handling.

## [10.0.2] - 2026-03-22

### Added

- **Storage throughput metric** — `integrateds3.storage.operation.bytes` counter with per-provider tracking.
- **HTTP throughput metrics** — `integrateds3.http.bytes_received` / `integrateds3.http.bytes_sent`.
- **Prometheus/Grafana overview dashboard** — importable IntegratedS3 overview dashboard for the emitted metrics.
- **Version management script** — repository script for bumping the shared package version.

## [10.0.1] - 2026-03-22

### Added

- **Tagging headers on write paths** — `x-amz-tagging` support for upload, copy, and multipart flows.
- **CRC64NVME checksum surfacing** — CRC64NVME added to `CopyObject` and `CompleteMultipartUpload` results.
- **Observability documentation** — expanded docs with Grafana dashboard import instructions.

### Fixed

- **XML declarations** — corrected UTF-16 XML declarations on S3-compatible responses.
- **Publish workflow** — fails fast when the NuGet publish secret is missing instead of failing mid-push.

## [10.0.0] - 2026-03-21

First stable release of the 9 `IntegratedS3.*` NuGet packages.

### Added

- **S3-Compatible REST API** — Full S3 protocol coverage including bucket CRUD, object CRUD, multipart uploads, versioning, object lock, bucket configurations (lifecycle, replication, notification, analytics, metrics, inventory, intelligent tiering, website, logging, request payment, accelerate, tagging, CORS, policy, ACL, encryption), and GetObjectAttributes.
- **Pluggable Storage Providers** — Disk-backed provider (`IntegratedS3.Provider.Disk`) and native AWS S3 provider (`IntegratedS3.Provider.S3`) with custom backend support via `IStorageBackend`.
- **Multi-Backend Orchestration** — Primary/replica topology with configurable consistency modes, automatic replication, and repair backlog.
- **SigV4 & SigV4a Authentication** — Full AWS Signature Version 4 and SigV4a (ECDSA P-256) support for header-based, presigned URL, and chunked-transfer authentication.
- **Presigned URLs** — Server-side presigned URL generation for both SigV4 and SigV4a with configurable expiry.
- **Authorization** — ClaimsPrincipal-based authorization with bucket policy evaluation (Allow/Deny/Conditions) and per-endpoint-group route authorization.
- **Health Checks** — ASP.NET Core health check integration with backend probing and dynamic health snapshots.
- **Observability** — OpenTelemetry-native tracing, metrics, and structured logging across all layers (core, providers, protocol, endpoints, maintenance). Optional OTLP export.
- **Scheduled Maintenance** — Opt-in hosted service for replica repair replay, orphan detection, and multipart upload cleanup.
- **Entity Framework Integration** — Optional EF Core catalog persistence via `IntegratedS3.EntityFramework`.
- **First-Party Client** — Typed HTTP client (`IntegratedS3.Client`) with presign and transfer extensions.
- **Testing Support** — Provider contract test base class, in-memory state stores, and checksum helpers via `IntegratedS3.Testing`.
- **AOT/Trimming Support** — Full Native AOT and trimming compatibility with zero IL2026/IL3050 warnings.
- **NuGet Packaging** — 9 modular packages with SourceLink, symbol packages, and XML documentation.
- **CI/CD** — GitHub Actions workflows for continuous integration and manual NuGet publishing.
