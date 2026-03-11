# Integrated S3 Implementation Plan

Build a modular, AOT-conscious ASP.NET storage platform that exposes an S3-compatible HTTP surface, supports pluggable backends (`S3`, disk, future providers), uses `ClaimsPrincipal`-driven authorization, and ships as reusable NuGet packages. The recommended approach is to separate protocol compatibility, storage orchestration, provider implementations, and ASP.NET integration so the system can grow toward broad S3 parity without becoming tightly coupled or hard to optimize.

## Current Implementation Status (March 2026)

The repository has already moved beyond initial scaffolding and now contains a working vertical slice.

### Implemented snapshot (refresh intentionally, not every feature PR)

- `IntegratedS3.Provider.S3` object-operations foundation slice with:
  - `ListBucketsAsync`, `CreateBucketAsync`, `HeadBucketAsync`, `DeleteBucketAsync` implemented via native AWS SDK
  - `ListObjectsAsync` with auto-pagination via continuation tokens and per-page `PageSize` control
  - `ListObjectVersionsAsync` using SDK `ListVersionsAsync`; delete markers and historical versions exposed via `IsDeleteMarker`/`IsLatest` on `ObjectInfo`
  - `HeadObjectAsync` via `GetObjectMetadataAsync`, returns `ObjectNotFound` for 404
  - `GetObjectAsync` with byte-range, If-Match/If-None-Match/If-Modified-Since/If-Unmodified-Since conditionals; 304 translated to `IsNotModified = true`; 412 translated to `PreconditionFailed`
  - `PutObjectAsync` streaming upload with content-type and custom metadata headers
  - `DeleteObjectAsync` returning delete-marker status and version ID
  - `GetObjectTagsAsync`, `PutObjectTagsAsync`, `DeleteObjectTagsAsync` via AWS tagging API
  - `GetBucketVersioningAsync` and `PutBucketVersioningAsync` via native SDK versioning API
  - `S3ErrorTranslator` extended with `PreconditionFailed` (412) and per-error-code object key context
  - `S3StorageCapabilities` updated: `ObjectCrud`, `ObjectMetadata`, `ListObjects`, `Pagination`, `RangeRequests`, `ConditionalRequests`, `ObjectTags`, `Versioning` all `Native`; multipart/copy remain `Unsupported`
  - `IS3StorageClient` and `AwsS3StorageClient` extended with all new object + versioning operations using SDK v4 API surface
  - Internal model records added: `S3ObjectEntry`, `S3ObjectListPage`, `S3ObjectVersionListPage`, `S3GetObjectResult`, `S3DeleteObjectResult`, `S3VersioningEntry`
  - `FakeS3Client` in tests extended; unit tests now cover capabilities, paged listing, CRUD, tags, versioning, 304/412 flows, and get-object disposal behavior
- modular project structure under `src/IntegratedS3/`
- initial abstractions package with:
  - canonical error/result model
  - capability descriptors including `StorageSupportStateOwnership` and `StorageSupportStateDescriptor`
  - async storage contracts
  - backend abstraction via `IStorageBackend`
  - request models for range, conditional, pagination, copy-object, multipart-upload, and object-tagging flows
  - `IStorageObjectStateStore` — optional platform-managed object metadata/tags/versions/checksums abstraction
  - `IStorageMultipartStateStore` — optional platform-managed multipart upload state abstraction
- `IntegratedS3.Core` orchestration layer with:
  - `IStorageService` orchestration over registered backends
  - provider selection through registered backends
  - first multi-backend consistency primitive via `PrimaryOnly` and `WriteThroughAll`
  - first read-routing policies via `PrimaryOnly`, `PreferPrimary`, and `PreferHealthyReplica`
  - overrideable backend-health evaluation via `IStorageBackendHealthEvaluator`
  - catalog synchronization hooks for bucket/object operations
  - first-class copy orchestration over registered backends
  - multipart orchestration over the primary backend with explicit unsupported behavior under write-through replication
  - current-version object-tag read/write orchestration with catalog refresh and write-through replication support
  - storage-aware authorization via an `IStorageService` decorator over orchestration
  - `CatalogStorageObjectStateStore` — default `IStorageObjectStateStore` implementation that delegates to the registered `IStorageCatalogStore`
- default `IntegratedS3.Core` registration with an overrideable `IStorageCatalogStore`
- default `IntegratedS3.Core` authorization registration with:
  - an allow-by-default `IIntegratedS3AuthorizationService`
  - an ambient `IIntegratedS3RequestContextAccessor`
- extracted optional EF-backed catalog persistence in `IntegratedS3.EntityFramework` with:
  - consumer-owned `DbContext` registration
  - catalog model mapping helpers
  - generic EF-backed `IStorageCatalogStore` registration
  - `CatalogStorageObjectStateStore` registered as `IStorageObjectStateStore` via `services.Replace(...)` so it activates alongside the EF catalog
  - `EntityFrameworkStorageMultipartStateStore<TDbContext>` registered as `IStorageMultipartStateStore` via `services.Replace(...)` for platform-managed multipart state
- `IntegratedS3.Provider.Disk` backend with:
  - bucket CRUD
  - object CRUD
  - lexicographic object listing with continuation-token pagination
  - range reads
  - ETag/date-based conditional reads
  - copy operations with source preconditions
  - multipart upload initiate/upload-part/complete/abort behavior with persisted upload state and part assembly
  - current-version object-tag persistence through sidecar metadata
  - bucket-level versioning configuration persistence with enable/suspend controls
  - historical object version archiving plus version-id-aware read/head/delete/tag flows for current and archived object versions
  - S3-style delete-marker creation and promotion behavior for version-enabled deletes
  - list-object-versions semantics for current objects, archived versions, and delete markers
  - object metadata, tags, versioning state, and checksums managed through `IStorageObjectStateStore` when registered (defaults to platform-managed via EF catalog), with filesystem sidecars retained as the fallback for standalone deployments
  - multipart upload state managed through `IStorageMultipartStateStore` when registered, with sidecar files retained as the fallback
- computed SHA-256, SHA1, and CRC32 object checksum persistence/exposure for the current write, copy, and multipart paths, including per-part checksum echoes and composite multipart completion responses
- request-side SHA-256, SHA1, and CRC32 checksum validation for direct put-object writes plus multipart SHA-256 and SHA1 part validation on the supported surface
  - streaming reads/writes
  - basic path traversal protection
- `IntegratedS3.AspNetCore` integration with:
  - `AddIntegratedS3(...)`
  - `MapIntegratedS3Endpoints(...)`
  - backend-derived service/provider descriptors and capabilities
  - `HttpContext.User` flow into Core authorization request context
  - source-generated JSON serialization
- sample host in `src/IntegratedS3/WebUi` exposing:
  - service document endpoint
  - capability endpoint
  - bucket endpoints
  - object endpoints with metadata-header round-tripping, pagination, range, conditional, copy, and multipart behavior
  - S3-compatible XML list-buckets, list-objects-v2, batch-delete, error, and copy-object responses for the currently supported surface area
  - S3-compatible multipart initiate/part-upload/complete/abort flows for the currently supported surface area
  - S3-compatible object tagging via `GET` / `PUT ?tagging` for current and archived versions on the currently supported surface
  - S3-compatible bucket versioning configuration via `GET` / `PUT ?versioning`
  - S3-compatible object access via `versionId` on the currently supported object/tagging/delete routes for current and archived versions
  - S3-compatible `x-amz-version-id` response headers for current object versions
  - S3-compatible `x-amz-delete-marker` response headers and delete-result XML fields for versioned deletes
  - S3-compatible `GET ?versions` bucket listing with `key-marker` / `version-id-marker` handling
- S3-compatible `x-amz-checksum-sha256`, `x-amz-checksum-sha1`, and `x-amz-checksum-crc32` request validation for current direct put-object flows
  - `ListObjectsV2` delimiter/common-prefix and `start-after` handling for S3-compatible hierarchical listings
  - AWS Signature Version 4 request authentication for both authorization-header and presigned-query request flows when enabled
  - SigV4-compatible `aws-chunked` request body decoding for current write flows including multipart part upload
  - optional virtual-hosted-style request routing when enabled via configuration
  - conditional authentication middleware activation when auth services are registered
- automated tests covering:
  - bootstrap registration
  - disk provider behavior including pagination, range, conditional requests, copy operations, multipart upload flows, bucket versioning controls, historical object version access/tagging/deletion, and checksum request validation
  - HTTP integration behavior including metadata headers, pagination, range, conditional requests, copy operations, multipart upload flows, bucket versioning controls, historical object `versionId` access, checksum-header validation, S3-compatible XML list/delete/versioning behavior, `ListObjectsV2` delimiter/common-prefix and `start-after` flows, and virtual-hosted-style routing
  - S3-compatible list-object-versions and delete-marker behavior across HTTP and AWS SDK compatibility flows
  - Core orchestration behavior including catalog synchronization for copied objects, mirrored write-through replication, and replicated object-tag updates
  - Core read routing behavior for unhealthy primaries, replica-preferred reads, and provider-unavailable read failover
  - Core multipart behavior including explicit rejection when write-through replication is enabled
  - ClaimsPrincipal-driven authorization behavior in both Core and HTTP flows
  - SigV4 header authentication, presigned-query authentication, and invalid-signature handling on the HTTP surface
  - AWS SDK compatibility behavior for path-style and virtual-hosted-style CRUD/list flows, root bucket listing, delimiter/start-after object listing, multipart upload flows, plus presigned URL and copy/conditional coverage
  - overrideability of the catalog persistence abstraction

### Important current architectural decision

`IntegratedS3.Core` should remain **persistence-agnostic by default**. Applications that want catalog/metadata persistence can either:

- register their own `IStorageCatalogStore`, or
- opt into the extracted `IntegratedS3.EntityFramework` package by supplying their own `DbContext` type through a generic registration such as `AddEntityFrameworkStorageCatalog<TDbContext>(...)`

That keeps Core from owning an internal application database model or forcing a specific provider such as SQLite into every consumer.

Another important architectural decision now in place is that service/provider descriptors and reported capabilities should come from registered backends/Core rather than from sample-host manual composition. `IntegratedS3Options` still provides fallback metadata for bootstrap-only scenarios, but the default runtime path now reflects actual backend registrations.

Another important architectural decision now in place is that authorization is separated from orchestration. `IStorageService` resolves to an authorizing decorator that wraps `OrchestratedStorageService`, which keeps bucket/key/version-aware authorization in Core without repeating authorization logic inside the orchestration implementation itself.

## Parallel Collaboration Layout

This plan now distinguishes between stable reference material and the active execution board so multiple contributors can work in parallel without constantly touching the same hot paragraphs.

### Reference-first sections

Treat these sections as architecture/snapshot material and refresh them intentionally instead of in every feature PR:

- `Implemented snapshot`
- `Goals`
- `Architecture Summary`
- `Phase 0` through `Phase 11`
- `Relevant Repository Files`
- `Verification Checklist`
- `Key Decisions`

### Active execution sections

Use these sections for day-to-day planning updates:

- `Current capability matrix snapshot` for cross-track status changes
- `Remaining Implementation Work by Parallel Track` for open work and dependencies
- `Recommended Next Execution Slices` and `Suggested First Parallel Batch` when priorities shift

### Update rules for low-conflict planning

1. Treat the `Implemented snapshot` and milestone prose as periodic rollups, not as per-PR checklists.
2. Put open work only in the owning track subsection under `Remaining Implementation Work by Parallel Track` instead of duplicating backlog items across phases, milestones, and next-step notes.
3. When a capability changes, update one matrix row and one owning track subsection rather than editing multiple narrative summaries.
4. If work spans tracks, record the dependency in the owning track subsection instead of rewriting another track's status bullets.

## Goals

- Ship as **NuGet packages**
- Provide **easy DI registration** and **easy endpoint mapping**
- Move toward **full S3-compatible behavior**
- Support **multiple storage backends**:
  - native S3
  - disk
  - future providers
- Allow **Blazor WebAssembly** and other frontends to work using standard auth flows and S3-compatible behavior
- Use `ClaimsPrincipal` for authz decisions in ASP.NET
- Make all important backend services **overrideable/configurable**
- Keep the entire stack **high-performance**, **streaming-first**, and **AOT/trimming aware**
- Expose efficient in-process services so consuming apps can directly access storage without HTTP overhead when desired

## Architecture Summary

The platform should be split into focused packages instead of building everything into the current `WebUi` project.

### Cross-cutting architecture to do — provider-native, externalized, and passthrough feature support

The current disk-provider sidecar approach is a valid **provider-specific emulation strategy**, but it should not become an implicit requirement for every future backend. The platform should stay viable for backends that:

- can only store raw object/blob content and need auxiliary state elsewhere
- can forward operations to an upstream S3-compatible API instead of re-owning behavior locally
- need hybrid composition such as “blob content upstream, metadata/state in a local catalog”
- may prefer redirect/delegation flows over proxy-streaming for downloads or uploads

This is especially important for potential future providers such as MediaFire-style integrations, redirect-heavy backends, and true S3 middleman/passthrough scenarios.

Detailed to do:

- **define auxiliary support-state ownership explicitly**

  - add a second backend-reported descriptor in addition to capability support status so the system can express where non-blob support state lives
  - this should answer questions such as:
    - does the backend itself own metadata persistence?
    - does IntegratedS3 own that state through a platform-managed database/catalog layer?
    - is the state delegated to an upstream system?
  - evaluate an enum/descriptor shape along the lines of:
    - `BackendOwned`
    - `PlatformManaged`
    - `Delegated`
    - `None` / `NotApplicable`
  - apply this concept to metadata, tags, checksums, version catalogs, multipart state, retention state, and redirect/location state rather than assuming a single storage strategy for all concerns
  - capability support should answer **whether** a feature exists, while this second descriptor should answer **who persists or resolves its support state**
  - for the target disk-provider architecture, object bytes live on disk but metadata and other auxiliary state should default to `PlatformManaged` rather than sidecar files

Status:

- support-state ownership descriptors (`StorageSupportStateOwnership`, `StorageSupportStateDescriptor`) exist and can report `PlatformManaged` ownership independently from capability support
- `IStorageObjectStateStore` and `IStorageMultipartStateStore` are the implemented optional platform-managed state abstractions for object auxiliary state and multipart upload state respectively
- `CatalogStorageObjectStateStore` (Core) wraps `IStorageCatalogStore` and serves as the default `IStorageObjectStateStore` when the EF integration registers it
- `EntityFrameworkStorageMultipartStateStore` (EF package) is the default `IStorageMultipartStateStore` when the EF integration is registered
- the disk provider's `GetSupportStateDescriptorAsync()` now dynamically reports `PlatformManaged` or `BackendOwned` for each concern based on whether the corresponding optional state store is injected
- disk sidecars remain as the standalone fallback when no platform-managed stores are registered; this is the settled architecture, not a transitional state

- **define provider modes explicitly**

  - document and support at least these provider shapes:
    - **managed provider**: IntegratedS3 owns most behavior and may emulate missing features locally
    - **delegating / passthrough provider**: IntegratedS3 forwards to an upstream S3-compatible or storage API with minimal local state
    - **hybrid provider**: object bytes live in one system while metadata, multipart state, tags, or version catalogs live in another
  - make sure provider mode is an explicit architectural concept rather than an accidental implementation detail

- **keep raw storage transport separate from auxiliary feature state**

  - avoid assuming every `IStorageBackend` can or should persist arbitrary extra metadata next to the object
  - do not require local providers such as the disk backend to own metadata persistence just because they own object bytes
  - prefer an architecture where the backend focuses on bucket/object transport and a separate platform-managed persistence layer stores auxiliary state
  - treat the following as potentially externalized concerns rather than mandatory backend-owned concerns:
    - object metadata persistence
    - object tags
    - checksum persistence and validation metadata
    - version chains / logical versions
    - multipart upload state and cleanup metadata
    - redirect/location indirection data
  - preserve the rule that provider contracts should model what the backend naturally owns, not every feature the platform may emulate around it

- **introduce optional support-service abstractions for non-native features**

  - evaluate dedicated abstractions/services for capabilities that may be provider-native, externalized, or unsupported, for example:
    - metadata store / metadata resolver
    - multipart state store
    - version catalog store
    - tag store
    - checksum store / validator
    - redirect or object-location resolver
  - make the platform-managed database layer the default composition point for these services in managed deployments instead of assuming filesystem sidecars in each provider
  - design the disk provider specifically so it can store bytes only while metadata, tags, versions, multipart state, and checksums are persisted through these support services
  - ensure these services are optional and overrideable through DI
  - keep `IntegratedS3.Core` persistence-agnostic while still allowing composition with EF, custom databases, caches, or provider-native implementations

  Status:

  - `IStorageObjectStateStore` (object metadata, tags, versioning, checksums) and `IStorageMultipartStateStore` (multipart upload state) are implemented as the primary optional support-service abstractions
  - the disk provider is wired to use these via constructor injection, falling back to filesystem sidecars when neither is registered
  - redirect/location resolver and further split-concern abstractions (e.g. tag store, checksum store as dedicated services) are still pending

- **clarify capability reporting beyond simple local emulation**

  - revisit whether `native | emulated | unsupported` is expressive enough for long-term provider diversity
  - keep capability support and support-state ownership as separate concepts so a feature can be reported as `emulated` while its state ownership is `PlatformManaged`
  - evaluate whether the system needs additional semantics such as:
    - delegated
    - externalized
    - proxied
  - if the capability enum should remain small, add another descriptor surface that explains *how* a capability is fulfilled and where its support state lives without breaking current compatibility reporting
  - document clearly that disk sidecars are only one emulation technique, not the canonical expectation for all providers

- **design a first-class passthrough / middleman mode**

  - support the scenario where IntegratedS3 acts primarily as:
    - an authorization gateway
    - a compatibility layer
    - a request-signing / request-validation boundary
    - a proxy or redirector to another S3-compatible API
  - minimize unnecessary local persistence in this mode
  - allow large transfers to remain streaming-first and avoid forced buffering unless the protocol truly requires it
  - preserve room for request forwarding, upstream resigning, response passthrough, and selective header translation

- **model object access outcomes more flexibly than “always return a stream”**

  - evaluate whether object reads/downloads should be able to produce:
    - a local/proxied stream
    - a redirect result
    - a delegated presigned URL
    - an upstream passthrough response shape
  - do the same evaluation for upload initiation flows where direct-to-provider upload may be preferable to proxying through ASP.NET
  - keep the HTTP surface compatible while allowing richer in-process orchestration decisions

- **ensure future provider implementations are not blocked by local-sidecar assumptions**

  - add tests and design checks for providers that cannot write arbitrary sidecar files or auxiliary metadata next to the blob
  - add tests and design checks for providers that intentionally persist all metadata in the platform-managed database layer while the backend stores only object bytes
  - validate at least one future-oriented scenario such as:
    - upstream S3 passthrough provider
    - remote blob provider with external metadata catalog
    - redirect/delegated-download provider
  - make sure these scenarios can be implemented without forcing fake local filesystem semantics

- **treat disk sidecars as an implementation choice, not a platform law**

  - disk sidecars are the standalone/fallback persistence path; the target architecture has the disk provider storing object bytes while `IStorageObjectStateStore` and `IStorageMultipartStateStore` handle auxiliary state
  - this is now the settled design: sidecars remain supported for deployments without a catalog, but sidecar semantics must not leak into shared abstractions or orchestration assumptions
  - whenever a new shared contract is introduced, explicitly ask whether it requires backend-owned persistence or can be satisfied through composition

  Status: **architecture is settled** — sidecar fallback is preserved, platform-managed state via the optional store abstractions is the default when the EF integration is registered

- **sequence this work alongside the next capability slices**

  - versioning, tags, and checksums are now implemented as the primary capability slice
  - the new contracts support native, externalized, and platform-managed support-state ownership as designed
  - the disk provider now serves as the first proof point for `PlatformManaged` support-state ownership when an object-state store is registered
  - remaining work in this area: broader checksum algorithm coverage (CRC32C), deeper versioning/tagging edge-case parity, and advanced subresource combinations

### Recommended package layout

- `IntegratedS3.Abstractions`
  - backend-agnostic interfaces
  - capabilities
  - shared models
  - canonical error model

- `IntegratedS3.Protocol`
  - S3-compatible request/response models
  - XML writers/parsers
  - signature helpers
  - protocol fidelity utilities

- `IntegratedS3.Core`
  - storage orchestration
  - mirror/backup routing
  - authorization services
  - reconciliation logic
  - policies and defaults

- `IntegratedS3.EntityFramework`
  - optional EF Core catalog persistence integration
  - consumer-owned `DbContext` mapping helpers
  - generic DI registration for EF-backed `IStorageCatalogStore`

- `IntegratedS3.AspNetCore`
  - DI registration
  - endpoint mapping
  - request binding
  - auth integration
  - hosted services for ASP.NET

- `IntegratedS3.Provider.S3`
  - native S3 backend implementation
  - AWS SDK integration
  - canonical error translation

- `IntegratedS3.Provider.Disk`
  - disk backend implementation
  - local blob storage with externally managed metadata/support-state by default
  - local streaming/file I/O

- `IntegratedS3.Client`
  - first-party .NET client package
  - useful for backend callers and Blazor-hosted consumers

- `IntegratedS3.Testing`
  - fakes
  - test helpers
  - provider verification tools

- `IntegratedS3.Tests`
  - current automated test project for unit/integration coverage while the broader testing package strategy evolves

- `benchmarks`
  - hot-path benchmarking
  - throughput, allocation, latency validation

## Phase 0 — Solution and Package Foundation

1. Restructure the solution under `src/IntegratedS3/` into modular projects.
2. Keep dependencies one-directional: providers depend on abstractions/protocol; optional integration packages (such as EF Core persistence) depend on Core, not the other way around; ASP.NET integration depends on abstractions/protocol/core; sample apps depend on packages, not the other way around.
3. Keep the existing `WebUi` as the first sample/reference host.
4. Define public API boundaries early: public extension methods, public options types, public service abstractions, and internal implementation details.
5. Freeze naming conventions and namespaces before implementation expands.

## Phase 1 — Canonical Storage Contract

Define a backend-agnostic storage contract that supports S3-level semantics while still allowing non-S3 backends.

### Core abstractions

- bucket operations
- object CRUD
- metadata
- tags
- checksums
- versions
- conditional requests
- range reads
- multipart upload lifecycle
- copy/move semantics
- retention/legal hold descriptors
- encryption descriptors
- capability discovery

### Auxiliary support-state ownership

Capability support alone is not enough. The system also needs a separate backend-reported descriptor that explains where non-blob support state is owned or resolved.

Design in place:

- `StorageSupportStateOwnership` enum: `BackendOwned`, `PlatformManaged`, `Delegated`, `NotApplicable`
- `StorageSupportStateDescriptor` reports ownership per concern: `ObjectMetadata`, `ObjectTags`, `MultipartState`, `Versioning`, `Checksums`, `Retention`, `RedirectLocations`
- optional `IStorageObjectStateStore` serves as the platform-managed store for object metadata/tags/versioning/checksums
- optional `IStorageMultipartStateStore` serves as the platform-managed store for multipart upload state
- backends report `PlatformManaged` or `BackendOwned` dynamically based on whether these stores are injected
- `CatalogStorageObjectStateStore` (Core) bridges `IStorageCatalogStore` to `IStorageObjectStateStore`; `EntityFrameworkStorageMultipartStateStore` (EF package) bridges EF to `IStorageMultipartStateStore`

The disk backend now implements this model: it stores raw object bytes, while metadata, tags, versioning state, checksums, and multipart state default to platform-managed storage when the EF integration is registered, with filesystem sidecars as the fallback.

### Required design rules

- no provider-specific SDK types should leak into public contracts
- all storage operations must be async and streaming-capable
- unsupported features must be explicit through capability reporting, not hidden or silently degraded
- backend contracts must not require providers to persist auxiliary metadata or other support state when that state can be externalized through platform services
- error behavior must be normalized

### Canonical error model

Create a stable error/result model for:

- object not found
- bucket not found
- access denied
- invalid range
- precondition failed
- version conflict
- bucket already exists
- multipart conflict
- throttling
- provider unavailable
- unsupported capability
- quota exceeded

## Phase 2 — Define “Full S3 Support” as Capability Slices

“Full support” should not be treated as a single checkbox. It needs a capability matrix and milestone tracking.

### Capability slices

- bucket create/delete/list/head
- object get/put/delete/head
- object metadata and headers
- list objects semantics
- pagination/continuation tokens
- range requests
- conditional requests
- multipart uploads
- copy operations
- presigned URLs
- object tags
- versioning
- batch delete
- ACL and policy-related behavior
- CORS behavior
- object lock / legal hold / retention
- server-side encryption variants
- checksum support
- XML-compatible S3 error responses
- path-style and virtual-hosted-style addressing

### Per-backend support status

For each feature, every backend should declare one of:

- native
- emulated
- unsupported

### Per-backend support-state ownership

For metadata-bearing or emulated features, every backend should also declare where the auxiliary support state lives:

- `BackendOwned`
- `PlatformManaged`
- `Delegated`
- `None` / `NotApplicable`

This second descriptor is especially important for local and passthrough providers, because it prevents the architecture from assuming that `emulated` automatically means `persisted by the storage driver itself`.

Examples:

- disk provider: object bytes may be backend-owned on disk while metadata/tags/versioning/checksum state is `PlatformManaged`
- S3 passthrough provider: support state may be `Delegated` to the upstream S3-compatible system
- custom website/blob provider: object transport may be backend-owned while metadata is either `PlatformManaged` or `Delegated` depending on integration shape

### Disk backend emulation needs

The disk provider will likely need externally managed catalog/state support — database-backed by default — for:

- ETags
- metadata
- tags
- version chains
- retention state
- ACL-equivalent descriptors
- checksum persistence

### Current capability matrix snapshot

The project now has enough implemented surface area that the capability slices can be tracked concretely instead of only aspirationally.

| Capability slice | Core / abstractions | Disk backend | ASP.NET / S3 surface | Automated coverage | Current notes |
| --- | --- | --- | --- | --- | --- |
| bucket create/delete/list/head | implemented | native | implemented | yes | available through both JSON convenience routes and S3-compatible routes |
| object get/put/delete/head | implemented | native | implemented | yes | supports streaming request/response flows |
| object metadata and headers | implemented | emulated | implemented | yes | current disk implementation uses sidecars for metadata-header round-tripping; target architecture moves this support state into the platform-managed database/catalog layer |
| list objects semantics | implemented | native plus emulated version catalogs | implemented | yes | S3 surface now supports both `ListObjectsV2` and `GET ?versions` for the currently supported versioned-object lifecycle |
| pagination / continuation tokens | implemented | native | implemented | yes | JSON route exposes a continuation header; S3 route exposes `NextContinuationToken` |
| delimiter / common-prefix listing | implemented | emulated at protocol layer | implemented | yes | current S3-compatible behavior is exercised for hierarchical listings |
| range requests | implemented | native | implemented | yes | single-range byte requests only today |
| conditional requests | implemented | native | partially implemented | yes | current parity centers on ETag and HTTP-date validators for `GET` / `HEAD` |
| copy operations | implemented | native | implemented | yes | `PUT` plus `x-amz-copy-source` returns S3-style XML copy results |
| batch delete | implemented at HTTP layer | backend composed via per-object delete | implemented | yes | S3-compatible `POST ?delete` is supported for the current bucket-level route |
| XML-compatible S3 errors | implemented | n/a | implemented | yes | storage endpoint failures are translated to XML error documents |
| path-style addressing | implemented | n/a | implemented | yes | current baseline routing model |
| virtual-hosted-style addressing | implemented | n/a | implemented | yes | optional and configuration-gated; AWS SDK compatibility coverage exists |
| SigV4 header authentication | implemented | n/a | implemented | yes | authorization-header validation is covered in HTTP and AWS SDK tests |
| SigV4 presigned-query validation | implemented | n/a | implemented | yes | request validation exists even though first-party presign generation does not |
| first-party presign generation | not started | unsupported | not exposed | no | next step belongs in client/core surface design, not only protocol helpers |
| multipart upload lifecycle | implemented | emulated | implemented | yes | initiate/upload-part/complete/abort flows are implemented; current orchestration remains primary-backend-only and rejects write-through replication |
| object tags | partially implemented | emulated | implemented | yes | direct contracts and S3-compatible `GET` / `PUT` / `DELETE ?tagging` now cover current and archived object versions on the currently supported surface, including version-id-aware delete-tagging parity; current and archived tag persistence can now default to platform-managed object state when available, while broader tagging edge cases are still pending |
| versioning | implemented for the current vertical slice | emulated with platform-managed historical catalogs | implemented for the current supported surface | yes | current object versions receive persisted opaque version IDs, overwrites archive historical versions, `versionId` read/head/delete/tagging/copy-source flows can target archived versions, `GET` / `PUT ?versioning` round-trip through Core/disk/HTTP, `GET ?versions` now lists historical versions and delete markers, version-enabled deletes now create S3-compatible delete markers, delete-marker reads now cover current-object `NoSuchKey` plus explicit-version `MethodNotAllowed` fidelity with the expected delete-marker/version/`Last-Modified` headers, and historical plus current auxiliary object state can be platform-managed when an object-state store is registered |
| checksums | partially implemented | emulated | partially implemented | yes | disk now computes and persists checksums for the current put/copy/multipart flows, preserves them through platform-managed object state, validates direct put-object SHA-256, SHA1, CRC32, and CRC32C request headers, supports checksum-enabled multipart SHA-256, SHA1, and CRC32C initiate/upload-part/complete parity including per-part checksum echoes plus completion checksum/type exposure for the algorithms that support composition, and now emits supported checksum fields in S3-compatible copy-object XML/AWS SDK copy responses plus HTTP object headers; deeper checksum/header edge cases are still pending |
| ACL / policy behavior | not started | unsupported | not implemented | no | authorization is `ClaimsPrincipal`-driven rather than S3 ACL compatible today |
| CORS | not started | unsupported | not implemented | no | expected to land later as explicit HTTP integration work |
| object lock / retention / legal hold | not started | unsupported | not implemented | no | needs both abstractions and provider persistence shape |
| server-side encryption variants | not started | unsupported | not implemented | no | should remain capability-driven rather than implied |

This matrix should now be treated as the authoritative cross-track capability summary for the current vertical slice. Day-to-day backlog movement should live in `Remaining Implementation Work by Parallel Track` so contributors can update one matrix row and one owning subsection instead of editing repeated status prose across the document.

## Phase 3 — Multi-Backend Orchestration and Backup Support

Support for “different storage backups” requires a proper orchestration layer, not just multiple registrations.

### Orchestration responsibilities

- primary storage selection
- mirrored write execution
- read routing
- health-aware provider choice
- failover policy
- replication/reconciliation tracking
- checksum comparison
- orphan cleanup

### Consistency modes

Recommended supported modes:

- `PrimaryOnly`
- `WriteThroughAll`
- `WriteToPrimaryAsyncReplicas`
- `ReadPreferPrimary`
- `ReadPreferHealthyReplica`

### Backup semantics to define clearly

Document what “backup” means in each topology:

- synchronous mirror
- asynchronous replica
- archive target
- cold backup
- read replica

### Failure semantics to specify up front

- primary succeeds, backup fails
- backup succeeds, primary fails
- metadata diverges
- object content diverges
- replica is stale
- provider is unhealthy
- reconciliation cannot complete

Without these rules, multi-backend support becomes operational chaos.

Status:

- multiple registered backends are now supported by the Core orchestration path
- `PrimaryOnly` and `WriteThroughAll` are implemented as the first concrete consistency modes
- write-through mirroring currently covers bucket create/delete, object put/delete, and copy-object behavior
- read routing now supports `PrimaryOnly`, `PreferPrimary`, and `PreferHealthyReplica`
- backend health can now be injected through `IStorageBackendHealthEvaluator` so unhealthy providers can be deprioritized without coupling providers to a specific health framework
- Core now layers dynamic backend-health snapshots on top of the evaluator and can optionally use `IStorageBackendHealthProbe` plus `StorageBackendHealthOptions` to actively probe backends, avoid recently unhealthy replicas after failover-triggering errors, and restore preferred routing after recovery
- asynchronous replicas, health endpoints, reconciliation, and divergence repair are still pending

## Phase 4 — ASP.NET Integration and Developer Ergonomics

The consuming developer experience should be excellent.

### DI registration API

Provide a top-level extension:

- `AddIntegratedS3(...)`

Status:

- initial `AddIntegratedS3(...)` is implemented
- `AddIntegratedS3Core(...)` is implemented for orchestration without forcing EF registration
- EF-backed catalog persistence is available through the extracted `IntegratedS3.EntityFramework` package and a dedicated generic registration against a consumer-owned `DbContext`
- catalog model mapping is exposed through `modelBuilder.MapIntegratedS3Catalog()` so consumers can keep the schema inside their own EF model
- critical persistence services are overrideable via DI today through `IStorageCatalogStore`

Support overloads for:

- configuration binding
- inline configuration
- named providers
- manual provider registration
- advanced service overrides

For the EF integration, the intended consumer flow is:

- register the application `DbContext`
- call `AddIntegratedS3Core(...)`
- call `AddEntityFrameworkStorageCatalog<TDbContext>(...)`
- map the catalog entities from `OnModelCreating(...)` via `modelBuilder.MapIntegratedS3Catalog()`

### Endpoint mapping API

Provide endpoint mapping like:

- `MapIntegratedS3Endpoints(...)`

Status:

- initial endpoint mapping is implemented for service, bucket, and object operations
- object endpoints now include pagination, range requests, conditional GET/HEAD support, and copy-object behavior via `PUT` with `x-amz-copy-source`
- S3-compatible multipart initiate/upload-part/complete/abort routing is implemented on the current object surface
- S3-compatible current-version object tagging is implemented through `GET` / `PUT ?tagging`
- `versionId` handling is implemented for the currently supported object read/head/delete and tagging routes, and copy-source parsing now honors `versionId` for archived-version source reads as well
- current object responses now emit `x-amz-version-id`, direct put-object requests can validate `x-amz-checksum-sha256` / `x-amz-checksum-crc32`, and bucket routes now support S3-compatible `GET` / `PUT ?versioning`
- S3-compatible root/bucket/object routing is implemented alongside the JSON convenience endpoints for the currently supported operations
- AWS Signature Version 4 request validation is implemented for authorization-header and presigned-query requests, though first-party presign generation APIs are still pending
- SigV4-compatible `aws-chunked` request bodies are decoded for the currently supported write flows, including multipart part upload
- batch delete and S3-compatible XML list responses are implemented for the supported bucket-level routes, including delimiter/common-prefix and `start-after` handling on the `list-type=2` surface
- authenticated ASP.NET requests now flow `HttpContext.User` into Core authorization evaluation
- feature-group toggles are not implemented yet

Allow feature-group toggles such as:

- service endpoints
- bucket endpoints
- object endpoints
- multipart endpoints
- admin endpoints

### Overrideability requirements

All important services should be replaceable via DI:

- authorization evaluator
- provider factory
- metadata index
- checksum calculator
- mirror coordinator
- presign strategy
- clock
- tenant resolver
- object naming policy
- stream pool
- request authenticator
- XML response writer
- hosted reconciliation services

### Hosting requirements

The ASP.NET integration must work with:

- Minimal APIs
- `CreateSlimBuilder`
- trimming
- AOT
- OpenAPI where practical

The default sample host should avoid opting into EF automatically so the baseline hosting path stays clean for trimming/AOT validation.

## Phase 5 — Authentication and ClaimsPrincipal Authorization

Authentication should remain normal ASP.NET authentication. Authorization should be storage-aware.

### Auth flow

1. ASP.NET authentication creates a `ClaimsPrincipal`
2. storage operations pass through a storage authorization service
3. authz resolves:
   - allowed operation
   - bucket access
   - key prefix access
   - tenant scope
   - version access
   - quotas/policies

### Recommended authz abstraction

Use a dedicated service such as:

- `IIntegratedS3AuthorizationService`

Status:

- `IIntegratedS3AuthorizationService` is implemented
- `IIntegratedS3RequestContextAccessor` is implemented so ASP.NET can flow the current `ClaimsPrincipal` into Core
- authorization currently executes in an `IStorageService` decorator (`AuthorizingStorageService`) over `OrchestratedStorageService`
- coarse endpoint-level authorization policies are still optional and not wired as first-class route-group configuration yet

This service should receive:

- principal
- operation intent
- bucket
- key
- version
- metadata intent
- request context

### Browser/WASM support model

Preferred design:

- **presigned URL hybrid**

That means:

- frontend authenticates to ASP.NET
- backend authorizes with `ClaimsPrincipal`
- backend returns presigned upload/download URLs when allowed
- frontend talks directly to storage for large transfers

Also support:

- pure proxy mode when direct storage access is not allowed

## Phase 6 — S3-Compatible Endpoint Surface

If standard S3 clients should work, the server must behave like an S3-compatible endpoint surface, not just expose “storage-ish” REST routes.

### Endpoint areas

- service-level operations
- bucket-level operations
- object-level operations
- multipart lifecycle
- utility/admin endpoints
- presign support

### Required protocol fidelity

- canonical request parsing
- signature handling
- signed header processing
- query-signed requests
- proper header behavior
- ETag fidelity
- XML error responses
- continuation token behavior
- delete batch response behavior
- multipart completion document compatibility
- conditional request status mapping
- range request handling

Status:

- path-style service, bucket, and object routing is implemented
- virtual-hosted-style bucket/object routing is implemented for compatible requests when enabled via configuration
- range request handling is implemented for disk-backed `GET` object operations
- conditional request status mapping is partially implemented for `GET`/`HEAD` via ETag and HTTP-date validators
- continuation-token pagination is implemented for both the JSON convenience endpoint (custom continuation header) and the S3-compatible `list-type=2` XML bucket-listing route
- S3-compatible `ListObjectsV2` now supports delimiter/common-prefix and `start-after` semantics for the currently supported surface area
- multipart upload initiate/upload-part/complete/abort behavior is implemented across the current abstractions, disk backend, XML request/response models, and S3-compatible HTTP object routes
- object tagging is implemented across the current abstractions, disk backend, XML request/response models, and S3-compatible HTTP object routes for current and archived versions on the supported surface
- historical object-addressing is now implemented for the currently supported `versionId` object/tagging/delete flows, with `x-amz-version-id` emitted on current and archived object responses
- bucket-level versioning controls are implemented for the currently supported `GET` / `PUT ?versioning` route surface
- copy-object behavior is implemented through `PUT` plus `x-amz-copy-source`, and the HTTP surface now returns an S3-style XML `CopyObjectResult`
- batch delete is implemented through `POST ?delete`, and the HTTP surface now returns an S3-style XML `DeleteResult`
- S3-style XML error responses are now returned for storage endpoint failures
- SigV4 authorization-header and presigned-query request authentication are implemented for the currently supported surface area, and SigV4-compatible `aws-chunked` request decoding now supports current write flows including multipart part upload
- direct put-object requests now validate SHA-256 checksum headers on the current supported surface area and map mismatches to S3-style `BadDigest` responses

### Current protocol fidelity gaps worth prioritizing

The current HTTP surface is real and useful, but it still has some clearly bounded fidelity gaps that should be treated as deliberate backlog rather than invisible debt:

- multipart upload support currently covers the core initiate/upload-part/complete/abort workflow, but broader parity such as multipart listing/discovery semantics and richer edge-case hardening is still pending
- object tagging now covers current and archived object versions on the currently supported surface, including S3-compatible delete-tagging; broader S3 tagging edge-case behavior is still pending
- only the currently supported bucket subresources are implemented; unsupported S3 bucket/object subresources intentionally return `NotImplemented`
- S3-compatible bucket listing now covers both `list-type=2` and the current `?versions` subresource, while additional subresource combinations are still pending
- conditional behavior is solid for the current `GET` / `HEAD` paths, but broader S3 precedence and edge-case parity still need hardening
- SigV4 validation and `aws-chunked` decoding work for the implemented routing surface, but canonical-request edge cases and parity hardening should continue before claiming wider compatibility
- first-party presign generation is not yet exposed even though presigned request validation is implemented

### Addressing strategy

Recommended milestone approach:

- path-style routing first
- virtual-hosted-style support later as an explicit milestone

## Phase 7 — Provider Implementation Order

### 1. Disk provider first

Build `IntegratedS3.Provider.Disk` first to validate:

- abstraction quality
- metadata/index strategy
- versioning model
- atomic file writes
- streaming
- error translation
- reconciliation logic locally

Status:

- this milestone is substantially complete for the current vertical slice
- disk backend validates streaming CRUD, paginated listing, range reads, conditional requests, copy-object, multipart upload lifecycle, bucket versioning controls, historical object version access/tagging/deletion, list-object-versions, delete-marker creation/promotion, delete-marker `GET` / `HEAD` fidelity, and direct put-object checksum validation
- object metadata, tags, versioning state, and checksums are managed through `IStorageObjectStateStore` (platform-managed via EF catalog by default) with sidecar fallback for standalone deployments
- multipart upload state is managed through `IStorageMultipartStateStore` (platform-managed via EF by default) with sidecar fallback
- retention, remaining checksum/header edge cases, and advanced versioning/tagging edge cases are still pending

### 2. Native S3 provider second

Build `IntegratedS3.Provider.S3` next:

- use native S3 APIs
- preserve streaming semantics
- translate provider errors to canonical storage errors
- support presign generation
- preserve metadata/version/checksum semantics as much as possible

Status: **object/list/versioning foundation slice is implemented**

- `S3StorageService` implements bucket CRUD plus object CRUD, object/version listing, object tagging, and bucket versioning through the native AWS SDK
- `IS3StorageClient` / `AwsS3StorageClient` bridge the AWS SDK v4 surface for range, conditional, tagging, and version-aware object flows
- canonical error translation, DI registration, and options are in place
- `S3StorageCapabilities` now declare bucket/object/list/range/conditional/tag/versioning support as `Native`; copy, multipart, presign, checksums, and advanced features remain the next steps
- copy-object, multipart upload, first-party presign generation, checksums, and broader parity/conformance coverage are still open

### 3. Future providers later

Potential future providers:

- Azure Blob
- Google Cloud Storage
- Cloudflare R2
- MinIO-specific tuning

## Phase 8 — Direct In-Process Developer Access

The platform should register efficient services so app developers can directly use storage without HTTP overhead.

### In-process service goals

- direct upload/download/list APIs
- provider-agnostic contract
- efficient streaming
- optional authz hooks
- metadata/tag/version support
- mirror-aware behavior when needed

### First-party client package

`IntegratedS3.Client` should support:

- typed calls to the ASP.NET endpoint surface
- auth token forwarding
- streaming upload/download
- presign helpers
- Blazor-friendly usage patterns

## Phase 9 — Performance and Optimization

Performance must be designed in from the start.

### Performance rules

- streaming-first I/O
- avoid large memory buffering
- source-generated serialization where possible
- minimize allocations in hot paths
- span-based parsing in signature/header handling
- pool buffers
- avoid reflection-heavy activation
- keep provider SDK details internal
- optimize range and multipart paths specifically

### Hot paths to benchmark

- request auth/signature validation
- metadata lookup
- object upload
- object download
- multipart part upload/complete
- mirrored writes
- list operations
- presign generation

### Metrics to track

- throughput
- p50/p95/p99 latency
- allocations
- LOH pressure
- temp file churn
- thread-pool pressure
- provider latency breakdown

## Phase 10 — Reconciliation, Health, and Observability

### Optional hosted/background services

- mirror replay
- orphan detection
- checksum verification
- provider health probes
- multipart cleanup
- index compaction
- expired temporary artifact cleanup

### Observability requirements

- structured logs
- metrics
- traces
- correlation IDs
- provider tags
- auth failure visibility
- mirror lag visibility
- reconciliation backlog visibility
- health endpoints

## Phase 11 — Testing, Conformance, and Packaging

### Test layers

- unit tests for contracts and policies
- provider tests for disk and S3
- integration tests for ASP.NET endpoints
- conformance tests for S3-like behavior
- fault-injection tests for backup/mirroring
- trimming/AOT publish tests
- benchmark regressions

Status:

- unit tests exist
- disk-provider tests exist
- ASP.NET integration tests exist
- Core orchestration tests exist
- pagination/range/conditional/copy/multipart/tagging behavior is covered in automated tests today
- AWS SDK compatibility coverage now includes virtual-hosted-style CRUD/list plus host-style presigned URL, multipart upload, and copy/conditional flows
- initial fault-injection coverage now exists for mirrored-write replica failures in Core orchestration tests
- S3 conformance, broader fault-injection coverage, trimming/AOT publish verification, and benchmark automation are still pending

### Consumer validation

Ship sample apps for:

- Minimal API
- MVC/Razor
- Blazor WebAssembly

### Packaging goals

- clean XML docs
- sensible package dependencies
- optional integrations such as EF Core should stay in dedicated packages so consumers do not pay for them implicitly
- analyzers/diagnostics if helpful
- versioned protocol compatibility
- easy onboarding docs

## Recommended Milestone Sequence

### M1 — Package scaffolding and abstractions

- create projects
- define contracts
- define options
- define canonical errors
- define capability descriptors

Status: **substantially complete**

### M2 — Disk provider and local endpoint prototype

- disk provider
- metadata/index approach
- local sample host
- direct service usage

Status: **substantially complete**

- disk provider exists
- local sample host exists
- direct service usage exists through `IStorageService`
- metadata, tags, versioning state, and checksums use platform-managed support state through `IStorageObjectStateStore` when available (e.g. with the EF integration), with filesystem sidecars as the standalone fallback
- multipart upload state uses platform-managed support state through `IStorageMultipartStateStore` when available, with sidecar files as the fallback
- paginated listing, range reads, conditional requests, copy operations, multipart upload, versioning, tagging, and checksum persistence/validation are all implemented
- remaining gaps: remaining checksum/header edge-case parity, deeper versioning/tagging edge-case parity, and advanced subresource combinations

### M3 — Native S3 provider and presigned URL support

- S3 provider
- presign flow
- ClaimsPrincipal authz integration
- initial client package support

Status: **in progress / partially complete**

- ClaimsPrincipal authz integration is implemented through `IIntegratedS3AuthorizationService`, an ambient request context accessor, ASP.NET request-context flow, and an authorizing `IStorageService` decorator
- SigV4 authorization-header and presigned-query request validation are implemented on the ASP.NET surface for compatibility scenarios, even though first-party presign generation is still not exposed as an application-facing feature yet
- native S3 provider — **object/list/versioning foundation slice is now implemented**:
  - `S3StorageService` implements `ListBucketsAsync`, `CreateBucketAsync`, `HeadBucketAsync`, and `DeleteBucketAsync` via the native AWS SDK
  - `S3StorageService` now also implements `ListObjectsAsync`, `ListObjectVersionsAsync`, `HeadObjectAsync`, `GetObjectAsync`, `PutObjectAsync`, `DeleteObjectAsync`, `GetObjectTagsAsync`, `PutObjectTagsAsync`, `DeleteObjectTagsAsync`, `GetBucketVersioningAsync`, and `PutBucketVersioningAsync`
  - `IS3StorageClient` / `AwsS3StorageClient` now bridge the AWS SDK v4 surface for object CRUD, paged listings, conditional GETs, range requests, tagging, and versioning
  - `S3ErrorTranslator` maps `AmazonS3Exception` to canonical `StorageErrorCode` values (`BucketNotFound`, `BucketAlreadyExists`, `AccessDenied`, `PreconditionFailed`, `Throttled`, `ProviderUnavailable`) with provider name and bucket/object context preserved
  - `CreateBucketAsync` still rejects `EnableVersioning = true` during bucket creation; versioning is enabled afterward through `PutBucketVersioningAsync`
  - `S3StorageCapabilities` now report `BucketOperations`, `ObjectCrud`, `ObjectMetadata`, `ListObjects`, `Pagination`, `RangeRequests`, `ConditionalRequests`, `ObjectTags`, and `Versioning` as `Native`; multipart, copy, presign, checksums, and advanced features remain `Unsupported`
  - DI registration and options wiring are in place for the S3 provider
  - automated xUnit coverage for the S3 provider foundation slice is in place (`S3StorageServiceTests`): capabilities snapshot, support state descriptor, bucket operations, paged listings, object CRUD, tags, versioning, 304/412 flows, disposal behavior, and DI bootstrap
- multipart upload, copy-object, first-party presign generation, checksums, and broader S3 integration/conformance coverage are still pending
- first-party presign generation flow is still not started
- initial client package support beyond scaffolding is still not started

### M4 — Multipart, range, copy, conditional support

- multipart lifecycle
- range requests
- copy APIs
- conditional request handling

Status: **in progress / partially complete**

- multipart lifecycle scaffolding is now implemented across abstractions, Core orchestration, the disk backend, the S3-compatible HTTP surface, XML payload handling, and automated tests
- range requests are implemented for disk-backed `GET` object operations
- conditional request handling is implemented for `GET`/`HEAD` via ETag and HTTP-date validators
- copy APIs are implemented through the storage abstractions, Core orchestration, disk backend, and HTTP `PUT` with `x-amz-copy-source`
- XML error, copy-object, list-bucket, list-buckets, and batch-delete responses are now implemented on the HTTP surface for the currently supported S3-compatible routes
- `ListObjectsV2` delimiter/common-prefix and `start-after` behavior are now implemented for the current S3-compatible bucket-listing surface
- SigV4 request validation is implemented for the current HTTP surface area
- SigV4-compatible `aws-chunked` request bodies are now decoded on the HTTP surface for current write flows including multipart part upload
- current multipart orchestration is intentionally limited to primary-backend semantics and returns an explicit unsupported-capability error when write-through replication is enabled
- broader protocol fidelity such as multipart listing semantics, additional bucket/object subresources, remaining checksum/header edge cases, and deeper edge-case compatibility is still pending

### M5 — Mirroring, backup, and reconciliation

- multiple backends
- write routing
- health-aware reads
- background reconciliation

Status: **in progress / partially complete**

- multiple backends can now be registered and surfaced through descriptors
- write routing now supports `PrimaryOnly` and `WriteThroughAll`
- mirrored writes currently cover bucket create/delete, object put/delete, and copy-object operations
- read routing now supports `PrimaryOnly`, `PreferPrimary`, and `PreferHealthyReplica`
- first health-aware provider selection is now implemented through an overrideable backend-health evaluator used by Core read orchestration
- Core now also tracks dynamic backend-health snapshots, marks failover-triggering read failures as temporarily unhealthy, and can optionally probe backends to restore healthy read preference without replacing existing evaluator overrides
- background reconciliation is still not started

### M6 — Versioning, tags, and checksums

- version chains
- metadata fidelity
- checksum persistence and validation
- tag support

Status: **in progress / partially complete**

- object tagging is implemented across abstractions, Core orchestration, HTTP `?tagging` routes, and automated tests for current and archived versions on the supported surface, including explicit delete-tagging flows, version-id-aware response parity, and platform-managed tag persistence by default when an object-state store is registered
- persisted checksum metadata is now implemented for the current disk/Core/HTTP object lifecycle paths, including platform-managed object-state composition, SHA-256/SHA1/CRC32/CRC32C response and header exposure, direct put-object request-side SHA-256, SHA1, CRC32, and CRC32C validation, and checksum-enabled multipart SHA-256, SHA1, and CRC32C initiate/upload-part/complete parity with completion checksum metadata for the supported algorithms
- AWS SDK and S3-compatible HTTP coverage now round-trip the current supported multipart checksum slice and copy-object checksum XML fields, including per-part checksum echoes, composite `ChecksumType` / `x-amz-checksum-type` exposure where applicable, supported copy-response checksum field parity, and SHA1 plus CRC32C checksum field/header/XML coverage on the current supported surface
- version-id scaffolding is now in place for current object versions across disk/Core/catalog/HTTP, bucket-level versioning controls round-trip through Core/disk/HTTP, the disk/HTTP surface supports archived-version access, tagging, delete-tagging, deletion, copy-source reads, list-object-versions, and S3-compatible delete markers, current delete-marker `GET` / `HEAD` requests now return `NoSuchKey` plus `x-amz-delete-marker` / `x-amz-version-id`, and explicit delete-marker `versionId` reads now return `MethodNotAllowed` with delete-marker/version/`Last-Modified` fidelity; remaining checksum/header edge cases and advanced versioning/tagging edge cases are still pending

### M7 — Advanced S3-compatible features

- ACL/policy mapping
- CORS
- object lock / retention / legal hold
- encryption-related support

Status: **not started**

### M8 — Hardening

- conformance improvement
- benchmark tuning
- docs
- end-to-end sample polish

Status: **not started**

## Remaining Implementation Work by Parallel Track

This section is the execution board for the remaining implementation backlog. Assign one contributor or PR stream per track and keep status updates confined to that track subsection plus any affected capability-matrix row above.

### Ready-now track selection

- Tracks A, B, D, E, and H can move immediately with today's abstractions and package boundaries.
- Track C can start on API design immediately, but its final surface should settle after Track B finishes the native S3 copy/multipart/presign slice.
- Track F can start on orchestration semantics and hosted-service scaffolding now, with end-to-end repair behavior benefiting from richer multi-provider parity.
- Track G should start after Track A settles the remaining retention/support-state abstraction questions.

### Track A — Architecture follow-through and provider modes

- Packages: `IntegratedS3.Abstractions`, `IntegratedS3.Core`, `IntegratedS3.EntityFramework`, `docs/`
- Ready: now
- Depends on: none
- Remaining scope:
  - add redirect/location resolver support for delegated or passthrough object-location scenarios
  - decide whether the current `native | emulated | unsupported` capability model needs extra semantics such as delegated, externalized, or proxied for future providers
  - formalize managed, delegating, passthrough, and hybrid provider modes so future providers do not inherit disk-specific assumptions
  - evaluate whether the remaining split support services should stay bundled inside `IStorageObjectStateStore` or be extracted into dedicated tag/checksum/location abstractions
  - carry the chosen model into provider descriptors, DI composition, and this plan so future providers can plug in cleanly

### Track B — Native S3 provider completion

- Packages: `IntegratedS3.Provider.S3`, `IntegratedS3.Abstractions`, `IntegratedS3.Tests`
- Ready: now
- Depends on: none for copy/multipart basics; coordinate with Track A only if new support abstractions are introduced
- Remaining scope:
  - implement `CopyObjectAsync` and expose `CopyOperations = Native`
  - implement the multipart upload lifecycle (`InitiateMultipartUploadAsync`, `UploadMultipartPartAsync`, `CompleteMultipartUploadAsync`, `AbortMultipartUploadAsync`)
  - add S3-provider checksum support where the provider can surface it cleanly
  - harden native S3 behavior against a local S3-compatible endpoint and extend provider-specific parity coverage
  - keep public contracts provider-agnostic while surfacing the new provider capability state accurately

### Track C — First-party presign and client surface

- Packages: `IntegratedS3.Client`, `IntegratedS3.AspNetCore`, `IntegratedS3.Protocol`, `IntegratedS3.Core`
- Ready: API design now, full implementation after Track B stabilizes the native object-copy/multipart surface
- Depends on: Track B for final provider-backed presign semantics
- Remaining scope:
  - expose first-party presign generation as an application-facing capability instead of validation-only protocol support
  - implement the currently scaffolded `IntegratedS3.Client` package beyond the empty project shell
  - add typed client calls, auth token forwarding, streaming upload/download helpers, and presign helpers
  - support the preferred browser/WASM presign-hybrid flow without forcing proxy-only transfers
  - document how direct-storage and proxy flows map onto `ClaimsPrincipal` authorization

### Track D — ASP.NET endpoint ergonomics and authorization surface

- Packages: `IntegratedS3.AspNetCore`, `WebUi`, `IntegratedS3.Tests`
- Ready: now
- Depends on: none for endpoint-grouping work; coordinate with Track C if presign endpoints/options are added
- Remaining scope:
  - round out the DI/configuration surface with any still-missing overloads for configuration binding, inline configuration, named providers, manual provider registration, and advanced overrides
  - add feature-group toggles for service, bucket, object, multipart, and admin endpoint groups
  - make route-group-level authorization and policy wiring first-class instead of leaving it as optional coarse endpoint setup
  - keep the host story aligned across `AddIntegratedS3(...)`, `MapIntegratedS3Endpoints(...)`, and the sample host composition helpers
  - preserve `CreateSlimBuilder(...)`, Minimal API, trimming, and AOT friendliness while expanding configurability

### Track E — S3 protocol fidelity and edge-case hardening

- Packages: `IntegratedS3.AspNetCore`, `IntegratedS3.Protocol`, `IntegratedS3.Provider.Disk`, `IntegratedS3.Tests`
- Ready: now
- Depends on: coordinate with Tracks B and C as new copy/multipart/presign features land
- Remaining scope:
  - add multipart listing/discovery semantics beyond initiate/upload-part/complete/abort
  - close additional bucket/object subresource and subresource-combination gaps on the S3-compatible HTTP surface
  - harden conditional precedence, checksum/header behavior, and canonical-request edge cases
  - continue versioning/tagging/delete-marker parity work for the remaining advanced edge cases
  - keep `aws-chunked`, presigned-query, and virtual-hosted-style compatibility tightening against real client behavior

### Track F — Multi-backend async replication, health, and reconciliation

- Packages: `IntegratedS3.Core`, `IntegratedS3.Provider.*`, `IntegratedS3.AspNetCore`, `IntegratedS3.Tests`
- Ready: orchestration semantics now; richer repair/testing once Tracks B and H add more parity coverage
- Depends on: benefits from Track B for stronger second-provider scenarios
- Remaining scope:
  - implement `WriteToPrimaryAsyncReplicas` and any remaining replica-write orchestration modes
  - add reconciliation and divergence-repair semantics for content, metadata, and version drift
  - add background services such as mirror replay, orphan detection, checksum verification, multipart cleanup, index compaction, and expired-artifact cleanup
  - expose health visibility and any required health/admin endpoints without coupling providers to a fixed hosting model
  - specify and then automate failure semantics for stale replicas, unhealthy providers, and incomplete repair attempts

### Track G — Advanced S3 feature slices

- Packages: `IntegratedS3.Abstractions`, `IntegratedS3.Core`, `IntegratedS3.AspNetCore`, `IntegratedS3.Provider.Disk`, `IntegratedS3.Provider.S3`, `IntegratedS3.Tests`
- Ready: after Track A settles the remaining retention/support-state design questions
- Depends on: Track A first; coordinate with Track F if cross-backend behavior matters
- Remaining scope:
  - ACL/policy-compatible behavior where it makes sense alongside the existing `ClaimsPrincipal` authorization model
  - CORS support on the S3-compatible HTTP surface
  - object lock, retention, and legal-hold abstractions plus provider persistence shape
  - server-side-encryption-related descriptors and behavior without leaking provider SDK details into shared contracts

### Track H — Hardening, conformance, performance, samples, and release polish

- Packages: `IntegratedS3.Tests`, `IntegratedS3.Testing`, `WebUi`, `docs/`
- Ready: now
- Depends on: can scaffold immediately; full coverage expands as Tracks B through G land
- Remaining scope:
  - add broader S3 conformance coverage, including the remaining protocol edge cases and client-compatibility scenarios
  - deepen fault-injection coverage for mirrored writes, async replicas, reconciliation, and unhealthy-provider routing
  - add structured logs, metrics, traces, correlation IDs, provider tags, auth-failure visibility, mirror-lag visibility, and reconciliation-backlog visibility
  - benchmark the hot paths called out in this plan and track throughput, latency, allocation, and provider-breakdown baselines
  - automate trimming/AOT publish verification and benchmark baselines
  - add the planned MVC/Razor and Blazor WebAssembly sample consumers
  - finish package polish items such as XML docs, onboarding docs, versioned protocol compatibility guidance, and any analyzers/diagnostics worth shipping

## Relevant Repository Files

- `src/IntegratedS3/WebUi/Program.cs`
  - current minimal host
  - good candidate to become the first sample consumer

- `src/IntegratedS3/WebUi/WebUiApplication.cs`
  - current reusable sample-host composition root
  - used both by the runtime host and in-process integration tests

- `src/IntegratedS3/WebUi/WebUi.csproj`
  - current AOT-enabled ASP.NET app
  - useful as the initial sample host

- `src/IntegratedS3/WebUi/appsettings.json`
  - should evolve into provider/auth/endpoint examples

- `src/IntegratedS3/IntegratedS3.slnx`
  - should include all package, test, and benchmark projects

- `src/IntegratedS3/IntegratedS3.Core/DependencyInjection/IntegratedS3CoreServiceCollectionExtensions.cs`
  - current Core registration entry point for orchestration and default non-persistent catalog behavior

- `src/IntegratedS3/IntegratedS3.Core/Options/IntegratedS3CoreOptions.cs`
  - current Core orchestration options including consistency-mode selection

- `src/IntegratedS3/IntegratedS3.Core/Services/AuthorizingStorageService.cs`
  - storage-aware authorization decorator over the orchestration service

- `src/IntegratedS3/IntegratedS3.EntityFramework/DependencyInjection/EntityFrameworkStorageCatalogServiceCollectionExtensions.cs`
  - generic EF catalog registration against a consumer-owned `DbContext`

- `src/IntegratedS3/IntegratedS3.EntityFramework/Persistence/IntegratedS3CatalogModelBuilderExtensions.cs`
  - model builder extension for mapping the catalog entities into a consumer-owned EF model

- `src/IntegratedS3/IntegratedS3.EntityFramework/IntegratedS3.EntityFramework.csproj`
  - optional EF Core integration package boundary

- `src/IntegratedS3/IntegratedS3.Core/Services/CatalogStorageObjectStateStore.cs`
  - default `IStorageObjectStateStore` implementation that delegates to the registered `IStorageCatalogStore`

- `src/IntegratedS3/IntegratedS3.Abstractions/Services/IStorageObjectStateStore.cs`
  - optional platform-managed object state abstraction covering metadata, tags, versioning, and checksums

- `src/IntegratedS3/IntegratedS3.Abstractions/Services/IStorageMultipartStateStore.cs`
  - optional platform-managed multipart upload state abstraction

- `src/IntegratedS3/IntegratedS3.Abstractions/Capabilities/StorageSupportStateDescriptor.cs`
  - per-concern support-state ownership descriptor reported by each backend

- `src/IntegratedS3/IntegratedS3.Abstractions/Capabilities/StorageSupportStateOwnership.cs`
  - `BackendOwned` / `PlatformManaged` / `Delegated` / `NotApplicable` ownership enum

- `src/IntegratedS3/IntegratedS3.Core/Services/OrchestratedStorageService.cs`
  - current orchestration layer implementation

- `src/IntegratedS3/IntegratedS3.Abstractions/Requests/CopyObjectRequest.cs`
  - first-class copy-object contract used by Core, providers, and HTTP copy handling

- `src/IntegratedS3/IntegratedS3.Abstractions/Requests/InitiateMultipartUploadRequest.cs`
  - provider-agnostic multipart initiation contract

- `src/IntegratedS3/IntegratedS3.Abstractions/Requests/UploadMultipartPartRequest.cs`
  - provider-agnostic multipart part-upload contract

- `src/IntegratedS3/IntegratedS3.Abstractions/Requests/CompleteMultipartUploadRequest.cs`
  - provider-agnostic multipart completion contract and ordered part list

- `src/IntegratedS3/IntegratedS3.Core/Services/IStorageCatalogStore.cs`
  - overrideable metadata/catalog persistence abstraction

- `src/IntegratedS3/IntegratedS3.Provider.Disk/DiskStorageService.cs`
  - current disk backend implementation including pagination, range, conditional, copy, and multipart behavior

- `src/IntegratedS3/IntegratedS3.AspNetCore/Endpoints/IntegratedS3EndpointRouteBuilderExtensions.cs`
  - current HTTP surface for service, bucket, and object operations including conditional reads, pagination, copy-source handling, multipart routing, and `aws-chunked` request decoding

- `src/IntegratedS3/IntegratedS3.Tests/`
  - current unit and integration coverage for bootstrap, disk, HTTP, and Core orchestration behavior

- `docs/integrated-s3-implementation-plan.md`
  - this document

## Verification Checklist

1. Ensure package dependencies are acyclic.
2. Ensure `IntegratedS3.Core` remains persistence-agnostic and does not require EF Core packages.
3. Ensure `IntegratedS3.AspNetCore` can be consumed without forcing all provider SDKs into every app.
4. Ensure optional integrations such as `IntegratedS3.EntityFramework` are only brought in when explicitly referenced by the consumer.
5. Publish a trimmed/AOT sample host and verify startup and endpoint behavior.
6. Validate S3-compatible request handling with conformance cases covering signing (authorization header and presigned query), XML list and batch-delete responses, virtual-hosted-style addressing, multipart, range, conditional requests, and XML errors.
7. Verify parity behavior between disk and S3 providers.
8. Run failure tests for backup/mirroring behavior.
9. Benchmark critical paths and track regression baselines.
10. Verify service overrideability via DI in tests.
11. Validate end-to-end behavior with a Minimal API sample, an MVC/Razor sample, and a Blazor WebAssembly sample.

## Key Decisions

- **Package shape:** modular packages first
- **Preferred browser model:** presigned URL hybrid
- **Goal:** broad S3-compatible endpoint support tracked through milestones
- **Important boundary:** “full support” must be represented by a capability matrix
- **Provider implementation order:** disk first, native S3 second
- **Current `WebUi` role:** sample/reference host, not final architecture container

## Recommended Next Execution Slices

The best immediate parallel execution batch is no longer a single serial step. The highest-value ready-now slices are the native S3 provider completion track, the protocol-fidelity hardening track, the ASP.NET ergonomics track, and the hardening/conformance track.

Why these should come next:

- they touch different primary packages (`IntegratedS3.Provider.S3`, `IntegratedS3.AspNetCore`, `IntegratedS3.Protocol`, `IntegratedS3.Tests`) so multiple contributors can move without colliding constantly
- finishing copy/multipart/checksum parity on the S3 provider unlocks cleaner first-party presign and client design
- protocol-fidelity hardening and conformance work can proceed in parallel with provider completion and will immediately validate the new surface area
- endpoint-toggle and route-group authorization work is mostly isolated to the ASP.NET integration layer and does not need to wait for the deeper backlog

Recommended dependency-aware order:

1. execute Track B to finish native S3 copy/multipart/checksum parity
2. execute Track E in parallel to harden multipart/subresource/conditional/header fidelity on the S3-compatible surface
3. execute Track D in parallel to add feature-group toggles and first-class route-group authorization configuration
4. execute Track H in parallel to expand conformance, fault-injection, and publish verification around the slices landing from Tracks B, D, and E
5. start Track C once Track B's provider-backed presign semantics are stable enough to expose cleanly
6. start Track F and then Track G after the above slices reduce abstraction churn

## Suggested First Parallel Batch

Given the current implementation state, the first parallel batch should be:

1. Track B — implement `CopyObjectAsync` plus the multipart upload lifecycle in `IntegratedS3.Provider.S3`
2. Track D — add feature-group endpoint toggles and route-group authorization configuration in `IntegratedS3.AspNetCore`
3. Track E — close multipart listing, subresource, conditional-precedence, and checksum/header edge cases on the S3-compatible HTTP surface
4. Track H — add local S3-compatible integration coverage, broader conformance cases, and publish/fault-injection automation around the above work
