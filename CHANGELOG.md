# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Versions match the `VersionPrefix` in `src/IntegratedS3/Directory.Build.props`, which is the version all 9 published `IntegratedS3.*` NuGet packages share. Entries for 10.0.0–10.0.4 were reconstructed from git history; going forward, the publish workflow tags each published release (`v{version}`) and creates a matching GitHub Release.

## [Unreleased]

## [11.0.0] - 2026-09-13

78 commits since 10.0.4: a security hardening pass on request authentication and the aws-chunked path, a broad AWS-conformance sweep (issues #101–#167), replica version mapping, and the first release cut with the traceable publish workflow (`v11.0.0` tag + GitHub Release). Major version bump because public interfaces gained abstract members and several defaults changed — see **Breaking**.

### Breaking

- **`IStorageCatalogStore` gained four abstract members** — custom catalog stores must implement them:
  - `GetObjectAsync(providerName, bucketName, key, versionId?)` — point lookup of one catalog row (latest when `versionId` is null/empty), pushed into the query instead of listing the bucket (#138).
  - `ListObjectsAsync(providerName?, bucketName?, keyPrefix?)` — the `keyPrefix` predicate is pushed into the persistence query; a null/whitespace prefix means no filter (#102).
  - `RecordReplicaVersionMappingAsync(replicaProviderName, bucketName, key, primaryVersionId, replicaVersionId)` and `GetReplicaVersionIdForPrimaryAsync(replicaProviderName, bucketName, key, primaryVersionId)` — the replica row remembers which primary version it mirrors, so version-addressed operations against a replica (e.g. `PutObjectTags`/`DeleteObjectTags`) can translate a primary version id into the replica's own (#126).
- **`IStorageService` gained three abstract members** — `GetBucketPublicAccessBlockAsync`, `PutBucketPublicAccessBlockAsync`, `DeleteBucketPublicAccessBlockAsync` (#154). The matching `IStorageBackend` members and all Ownership Controls members ship with `NotImplemented` default implementations, so backends compile unchanged.
- **`IStorageReplicaRepairBacklog` gained `RevertToPendingAsync`** — custom backlogs must implement it (#142).
- **Default behaviour changes**
  - Authentication fails closed: when authentication is required, a request that presents no credentials is rejected with `403 AccessDenied` instead of falling through as an anonymous principal. Opt back in per route with `AllowAnonymous`, or globally with the new `IntegratedS3Options.AllowAnonymousRequests`; `RequireAuthenticatedRequests` forces the closed behaviour for custom authenticators (#82).
  - Authorization is scope-based (#86).
  - `IntegratedS3Options.MaxObjectSizeBytes` defaults to 5 GiB (the S3 per-request maximum) instead of unbounded; `null` opts out (#115).
  - `AllowedSignatureClockSkewMinutes` defaults to 15 (was 5) and presigned-URL expiry is capped at 604800 s, matching AWS (#161). Presigned expiry is enforced as an absolute bound, not expiry + skew (#132).
  - Listings order keys by UTF-8 bytes and honour whitespace `delimiter`/`start-after` (#167); `max-keys` is clamped to 1000 and `max-keys=0` returns an empty page (#149); invalid ListObjects V1/V2/Versions query arguments are rejected with `400 InvalidArgument` (#150).
  - Error codes for bucket, quota, object-lock, multipart and config-not-found failures now match AWS (#164, #147, #152, #139).
- **`IntegratedS3.Testing`** depends on `xunit.core` + `xunit.assert` instead of the `xunit` meta-package (#144).

### Security

- **Fail-closed request authentication and scope-based authorization** (#82, #86).
- **Signed `aws-chunked` uploads verify the per-chunk SigV4/SigV4a signature chain** (#101); the signed `x-amz-content-sha256` is verified against the received body (#131); signed-streaming requests without a signing context are rejected (#114); duplicate `Authorization`-header parameters are rejected with 400 (#118).
- **SigV4a P-256 key derivation corrected to match aws-crt** (#103).
- **Bounded upload size and `aws-chunked` temp spooling (disk-exhaustion DoS)** — `MaxObjectSizeBytes` defaults to 5 GiB and the `aws-chunked` decode path enforces the same cap while spooling, rejecting oversize bodies with `413 EntityTooLarge` (#115); `aws-chunked` line length is capped (#116); S3 XML request readers are hardened against DTD/entity-expansion (#104); the inbound correlation-id header is validated before it is reflected or logged (#135).
- **CVE gating** — NuGet audit runs on every restore as a build gate plus a scheduled `security-scan` workflow (#130).

### Added

- **Presigned `DELETE`, `HEAD`, and multipart `UploadPart` URLs** — presign issuance and validation now cover `DeleteObject`, `HeadObject`, and `UploadPart` in addition to `GetObject`/`PutObject`.
- **Pluggable SigV4 credential resolution** — `IIntegratedS3CredentialResolver` (default: `IOptionsMonitor<IntegratedS3Options>`-backed) so hosts can resolve access keys from a database or secret store and rotate keys without a restart.
- **Bucket `PublicAccessBlock` subresource** (#154); **`OwnershipControls`, `GetBucketPolicyStatus`, `GetObjectTorrent`** (#166); **`x-amz-storage-class` persisted and echoed** on GET/HEAD/List (#155); **`FULL_OBJECT` checksum type** and `InvalidDigest` for malformed `Content-MD5` (#156); **`response-content-*` override query parameters** on GET/HEAD (#148); **HTTP `Date` header fallback** when `x-amz-date` is absent (#133).
- **`MaxObjectSizeBytes` host option** — upload endpoints replace the host's per-request body-size limit with this value, so uploads beyond Kestrel's ~28.6 MiB default no longer fail with `413` (#115).
- **Object Lock default-retention enforcement in the disk provider** — bucket default retention now blocks in-window permanent version deletes instead of being stored without effect.
- **Scheduled maintenance: abandoned multipart upload expiry** — the maintenance job set can expire and abort stale multipart uploads.
- **Replica repair divergence kinds and orphan reconciliation** — repair entries describe content/metadata/version divergence explicitly, and reconciliation can garbage-collect orphaned provider-side artifacts.
- **Local-first BenchmarkDotNet suite and AWS-SDK E2E harness** (#234) with a baseline regression gate.
- **Code coverage in CI plus Dependabot**; **provider capability matrix** (`docs/protocol-compatibility.md`); **community and governance files** (`CONTRIBUTING.md`, `SECURITY.md`, issue and PR templates, `CODEOWNERS`).
- `Utf8OrdinalComparer` in `IntegratedS3.Abstractions` (the S3 key ordering).

### Changed

- **Warnings are errors** — `TreatWarningsAsErrors`, nullable warnings as errors, and code-style analysis enforced in build.
- **CI consolidated into a single workflow** — one `ci.yml` (build/test matrix, AOT validation, pack) with concurrency cancellation and NuGet caching; the legacy track-specific workflow was removed.
- **Publishing is traceable** — the NuGet publish workflow tags the release commit (`v{version}`) and creates a GitHub Release with the packed artifacts after a successful push.
- **README claims qualified** — feature bullets state per-provider limits (SSE, retention/legal hold, config-only bucket subresources) instead of implying uniform support.

### Fixed

- **Listing** — `ListObjectVersions` pagination and response shape (#159), `KeyMarker`/`VersionIdMarker` always emitted on the first page (#136); `max-keys` and the continuation cursor are pushed into storage instead of materializing the whole bucket (#102).
- **Versioning** — `x-amz-version-id` only on versioning-enabled buckets and null-version delete markers when suspended (#151); invalid `PutBucketVersioning` status rejected with `IllegalVersioningConfigurationException` (#160); `PutObjectTags`/`DeleteObjectTags` against a replica translate the primary version via the recorded mapping (#126).
- **Multipart** — concurrent same-part uploads serialized and each call returns its own bytes' metadata (#124); `AbortMultipartUpload` serialized against `Complete` with idempotent cleanup (#123); CRC64NVME multipart uploads rejected up front for a consistent checksum set (#119); `EntityTooSmall` enforced on disk `CompleteMultipartUpload` (#137); disk object and part ETags derived from content (#105).
- **Wire format and errors** — `PutObject`/`CreateBucket`/`Copy`/`CompleteMultipartUpload` responses and error `HostId` (#163); S3 `xmlns` emitted on empty configuration responses (#117); unhandled exceptions translated into S3 `<Error>` XML (#157); generic 409s routed to `VersionConflict` (#139); object header/attribute conformance nits (#165); `Content-Range` on 416 and `Range` honoured on HEAD (#162); whole-object `x-amz-checksum-*` header omitted on ranged 206 responses (#233); invalid conditional-copy dates ignored instead of 400 (#158); `?intelligent-tiering` requests routed to their handlers (#153).
- **Disk provider** — backslash object-key aliasing; per-object concurrency control; write-path temp files flushed to stable storage before the atomic rename (#121); lock-free read/copy-source TOCTOU races translated to 404 and readers opened with `FileShare.Delete` (#120); resumed download appends rolled back on I/O error or cancellation (#112); buffered temp file cleaned up when a write-through `PutObject`/`CopyObject` copy fails (#125); checksum validation disposed when resume seeding throws (#141).
- **Replication and repair** — Object Lock divergences repaired by re-applying lock state rather than re-PUTting the body (#108); object tags preserved during async replica repair (#107); repair dispatch tracked and drained on shutdown (#142); `ReportFailure` no longer masks failing backends on non-transport errors (#127).
- **S3 provider** — transport/service failures translated instead of leaking raw exceptions (#129); S3 Select results streamed with the event stream disposed and cancellation honoured (#109).
- **Catalog** — transactional, concurrency-safe `UpsertObjectAsync` (#111, #110).
- **CORS** — the opened `GetObjectResponse` is disposed when the CORS wrapper throws before delegation (#128).

### Performance

- `aws-chunked` framing reads are buffered, dropping the per-byte `ReadAsync`/`byte[1]` allocation (#134); `PutObject` hashes inline and computes only the requested digests (#122); EF catalog/multipart JSON columns go through a source-generated `JsonSerializerContext` (#140); listing and point-lookup predicates are pushed into the catalog query (#102, #138).

### Build

- Redundant `Microsoft.SourceLink.GitHub` reference removed (#143); unused `packages: write` scope dropped from the publish workflow (#145); dependency bumps via Dependabot (`AWSSDK.S3` group, test SDKs, GitHub Actions).

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
