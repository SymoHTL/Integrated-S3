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
  - `PutObjectAsync` streaming upload with content-type, custom metadata headers, and native checksum request/response mapping
  - `DeleteObjectAsync` returning delete-marker status and version ID
  - `CopyObjectAsync` via native SDK copy with source preconditions, overwrite guarding, checksum surfacing, and follow-up metadata enrichment
  - multipart upload lifecycle (`InitiateMultipartUploadAsync`, `UploadMultipartPartAsync`, `CompleteMultipartUploadAsync`, `AbortMultipartUploadAsync`) plus provider-native multipart upload listing via the AWS SDK
  - `GetObjectTagsAsync`, `PutObjectTagsAsync`, `DeleteObjectTagsAsync` via AWS tagging API
  - `GetBucketVersioningAsync` and `PutBucketVersioningAsync` via native SDK versioning API
  - `GetBucketCorsAsync`, `PutBucketCorsAsync`, and `DeleteBucketCorsAsync` via native SDK bucket CORS APIs
  - initial object-level server-side encryption (`AES256`, `aws:kms`) request mapping for put/copy/initiate-multipart plus returned SSE metadata on put/copy/get/head results
  - `S3ErrorTranslator` extended with native multipart/checksum error mappings (`NoSuchUpload`, `InvalidPart`, `InvalidPartOrder`, `EntityTooSmall`, `BadDigest`) plus per-error-code object key context
  - `S3StorageCapabilities` updated: `ObjectCrud`, `ObjectMetadata`, `ListObjects`, `Pagination`, `RangeRequests`, `ConditionalRequests`, `ObjectTags`, `Versioning`, `Cors`, `CopyOperations`, `MultipartUploads`, `PresignedUrls`, `Checksums`, and `ServerSideEncryption` are `Native`
  - `IS3StorageClient` and `AwsS3StorageClient` extended with native copy, multipart, presign, checksum, versioning, bucket CORS, and initial object-level SSE operations using the SDK v4 API surface
  - `AddS3Storage(...)` now also registers an S3-backed `IStorageObjectLocationResolver` so delegated read presigns can surface provider URLs with safe proxy fallback
  - Internal model records added/extended: `S3ObjectEntry`, `S3ObjectListPage`, `S3ObjectVersionListPage`, `S3MultipartUploadListPage`, `S3GetObjectResult`, `S3DeleteObjectResult`, `S3VersioningEntry`, `S3CorsConfigurationEntry`
  - `FakeS3Client` in tests extended; unit tests now cover capabilities, copy, multipart lifecycle/listing, checksum mapping, paged listing, CRUD, tags, versioning, 304/412 flows, and get-object disposal behavior
- modular project structure under `src/IntegratedS3/`
- initial abstractions package with:
  - canonical error/result model
  - capability descriptors including `StorageSupportStateOwnership` and `StorageSupportStateDescriptor`
  - explicit provider-mode and object-location architecture via `StorageProviderMode`, `StorageObjectLocationDescriptor`, and `IStorageObjectLocationResolver`
  - async storage contracts
  - backend abstraction via `IStorageBackend`
  - request models for range, conditional, pagination, copy-object, multipart-upload, and object-tagging flows
  - `IStorageObjectStateStore` — optional platform-managed object metadata/tags/versions/checksums abstraction
  - `IStorageMultipartStateStore` — optional platform-managed multipart upload state abstraction
- `IntegratedS3.Core` orchestration layer with:
  - `IStorageService` orchestration over registered backends
  - provider selection through registered backends
  - current multi-backend consistency modes via `PrimaryOnly`, synchronous `WriteThroughAll`, and `WriteToPrimaryAsyncReplicas`
  - first read-routing policies via `PrimaryOnly`, `PreferPrimary`, and `PreferHealthyReplica`, with outstanding-repair-aware replica filtering by default
  - overrideable backend-health evaluation via `IStorageBackendHealthEvaluator` plus `StorageBackendHealthMonitor` support for dynamic snapshots and optional `IStorageBackendHealthProbe` recovery probing
  - catalog synchronization hooks for bucket/object operations
  - first-class copy orchestration over registered backends
  - multipart orchestration over the primary backend with explicit unsupported behavior under both replicated write modes
  - current-version object-tag read/write orchestration with catalog refresh and write-through replication support
  - bucket CORS read/write/delete orchestration with authorization coverage and write-through replication support
  - provider-agnostic replica-repair backlog/dispatcher seams that record async replica work, partial-write divergence, and failed-repair visibility without requiring a built-in always-on worker
  - storage-aware authorization via an `IStorageService` decorator over orchestration
  - authorization-aware presign orchestration over provider-agnostic backend direct grants, delegated read-location resolution, and proxy fallback
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
  - bucket-level CORS configuration persistence in the existing bucket-metadata sidecar
  - historical object version archiving plus version-id-aware read/head/delete/tag flows for current and archived object versions
  - S3-style delete-marker creation and promotion behavior for version-enabled deletes
  - list-object-versions semantics for current objects, archived versions, and delete markers
  - object metadata, tags, versioning state, and checksums managed through `IStorageObjectStateStore` when registered (defaults to platform-managed via EF catalog), with filesystem sidecars retained as the fallback for standalone deployments
  - multipart upload state managed through `IStorageMultipartStateStore` when registered, with sidecar files retained as the fallback
  - explicit rejection of managed object-level SSE request shapes on the current get/head/put/copy/initiate-multipart surface
- computed SHA-256, SHA1, and CRC32 object checksum persistence/exposure for the current write, copy, and multipart paths, including per-part checksum echoes and composite multipart completion responses
- request-side SHA-256, SHA1, and CRC32 checksum validation for direct put-object writes plus multipart SHA-256 and SHA1 part validation on the supported surface
  - streaming reads/writes
  - basic path traversal protection
- `IntegratedS3.AspNetCore` integration with:
  - `AddIntegratedS3(...)`
  - `MapIntegratedS3Endpoints(...)`
  - combined configuration-binding + inline-configuration overloads plus `AddIntegratedS3Provider(...)` helpers for named/manual configured-provider metadata registration and `AddIntegratedS3Backend(...)` helpers for custom backend DI activation
  - feature-group endpoint toggles for service, bucket, object, multipart, and admin surfaces
  - `IntegratedS3EndpointOptions` can now bind from `IntegratedS3:Endpoints`, and map-time overrides start from those configured defaults so hosts/tests can keep endpoint toggles aligned without manual re-wiring
  - first-class whole-route, per-feature, and explicit shared root/compatibility route-group configuration on endpoint mapping, including authorization/policy wiring
  - `IntegratedS3EndpointFeature` plus `SetFeatureRouteGroupConfiguration(...)` / `GetFeatureRouteGroupConfiguration(...)` as the stable per-feature callback pattern for future endpoint surfaces, with the current `Configure*RouteGroup` members retained as convenience wrappers for today's built-in features
  - backend-derived service/provider descriptors and capabilities
  - provider descriptors and service documents that now surface provider mode plus object-location access shape separately from capability support and support-state ownership
  - capability reporting on the HTTP metadata surface remains backend/Core-derived runtime metadata, not a full end-to-end HTTP-conformance report
  - `HttpContext.User` flow into Core authorization request context
  - provider-aware presign issuance via `POST /integrated-s3/presign/object`, with configurable signing-credential resolution and public-base-url fallback for proxy grants plus backend-direct/object-location seams for direct or delegated access
  - read-only replica-repair backlog visibility via `GET /integrated-s3/admin/repairs`, including optional replica filtering on the admin surface
  - source-generated JSON serialization
- `IntegratedS3.Client` package with:
  - `IIntegratedS3Client` / `IntegratedS3Client` wrappers over the presign endpoint
  - presign convenience helpers for object `GET` / `PUT`, including access-mode preference overloads
  - first-party `AddIntegratedS3Client(...)` DI registration with named `IHttpClientFactory` integration, configuration binding, and overrideable `HttpClientBuilder` customization
  - `StoragePresignedRequest.CreateHttpRequestMessage(...)` to materialize ready-to-send `HttpRequestMessage` instances from returned grants
  - typed streaming/file transfer helpers that compose presign issuance with upload/download execution
- sample host in `src/IntegratedS3/WebUi` exposing:
  - host composition helpers that can forward endpoint-mapping options while preserving the slim minimal-hosting style
  - shared runtime/test composition through `WebUiApplication.ConfigureServices(...)` and `ConfigurePipeline(...)`, with a dedicated reference-host guide in `docs/webui-reference-host.md`
  - service document endpoint
  - capability endpoint
  - bucket endpoints
  - object endpoints with metadata-header round-tripping, pagination, range, conditional, copy, and multipart behavior
  - S3-compatible XML list-buckets, list-objects-v2, batch-delete, error, and copy-object responses for the currently supported surface area
  - S3-compatible multipart initiate/part-upload/complete/abort flows plus bucket-level `GET ?uploads` listing/discovery for the currently supported surface area
  - S3-compatible object tagging via `GET` / `PUT ?tagging` for current and archived versions on the currently supported surface
  - S3-compatible bucket versioning configuration via `GET` / `PUT ?versioning`
  - S3-compatible bucket CORS configuration via `GET` / `PUT` / `DELETE ?cors`
  - bucket-aware browser-facing CORS handling for bucket/object routes, including unauthenticated preflight `OPTIONS` evaluation and actual-response `Access-Control-*` headers without global ASP.NET CORS middleware
  - S3-compatible object access via `versionId` on the currently supported object/tagging/delete routes for current and archived versions
  - S3-compatible `x-amz-version-id` response headers for current object versions
  - S3-compatible `x-amz-delete-marker` response headers and delete-result XML fields for versioned deletes
  - S3-compatible `GET ?versions` bucket listing with `key-marker` / `version-id-marker` handling
  - S3-compatible `x-amz-checksum-sha256`, `x-amz-checksum-sha1`, and `x-amz-checksum-crc32` request validation for current direct put-object flows
  - S3-compatible object-level SSE header validation/normalization for `AES256` / `aws:kms` put/copy/initiate-multipart requests plus returned response headers on put/copy/get/head object flows
  - `ListObjectsV2` delimiter/common-prefix and `start-after` handling for S3-compatible hierarchical listings
  - AWS Signature Version 4 request authentication for both authorization-header and presigned-query request flows when enabled
  - SigV4-compatible `aws-chunked` request body decoding for current write flows including multipart part upload
  - optional virtual-hosted-style request routing when enabled via configuration
  - conditional authentication middleware activation when auth services are registered
  - conditional authorization middleware activation when authorization services are registered
- automated tests covering:
  - bootstrap registration
  - disk provider behavior including pagination, range, conditional requests, copy operations, multipart upload flows, bucket versioning controls, bucket CORS persistence, historical object version access/tagging/deletion, checksum request validation, and explicit SSE rejection guardrails
  - HTTP integration behavior including metadata headers, pagination, range, conditional requests, copy operations, multipart upload flows, bucket-level multipart listing/discovery, bucket versioning controls, bucket CORS XML/runtime handling, unsupported mixed bucket-subresource rejection, repair-backlog admin endpoint behavior, historical object `versionId` access, checksum-header validation, S3-compatible XML list/delete/versioning behavior, object-level SSE header parsing/response emission, `ListObjectsV2` delimiter/common-prefix and `start-after` flows, and virtual-hosted-style routing
  - S3-compatible list-object-versions and delete-marker behavior across HTTP and AWS SDK compatibility flows
  - Core orchestration behavior including catalog synchronization for copied objects, mirrored write-through replication, async replica recording/dispatch, unhealthy-replica preflight, unhealthy-snapshot recovery probing, outstanding-repair read behavior, partial-write backlog semantics, failed-repair visibility, multi-replica dispatch-recording failure isolation, mixed replay success/failure, replicated object-tag updates, and replicated bucket CORS configuration updates
  - Core read routing behavior for unhealthy primaries, replica-preferred reads, and provider-unavailable read failover
  - Core multipart behavior including explicit rejection when either replicated write mode is enabled
  - ClaimsPrincipal-driven authorization behavior in both Core and HTTP flows
  - SigV4 header authentication, presigned-query authentication, and invalid-signature handling on the HTTP surface
  - provider-agnostic SSE contract, native S3 provider mapping/metadata, and catalog-persistence behavior for the current `AES256` / `aws:kms` slice
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
  - local blob storage with sidecar-backed metadata/support-state by default, with optional platform-managed state stores when registered
  - local streaming/file I/O

- `IntegratedS3.Client`
  - first-party .NET client package
  - useful for backend callers and Blazor-hosted consumers

- `IntegratedS3.Testing`
  - placeholder package for future shared fakes/test helpers; currently minimal
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
| copy operations | implemented | native | implemented | yes | `PUT` plus `x-amz-copy-source` returns S3-style XML copy results, and the native S3 provider now maps copy requests/responses through the AWS SDK with provider-agnostic `ObjectInfo` checksum surfacing |
| batch delete | implemented at HTTP layer | backend composed via per-object delete | implemented | yes | S3-compatible `POST ?delete` is supported for the current bucket-level route |
| XML-compatible S3 errors | implemented | n/a | implemented | yes | storage endpoint failures are translated to XML error documents |
| path-style addressing | implemented | n/a | implemented | yes | current baseline routing model |
| virtual-hosted-style addressing | implemented | n/a | implemented | yes | optional and configuration-gated; AWS SDK compatibility coverage exists |
| SigV4 header authentication | implemented | n/a | implemented | yes | authorization-header validation is covered in HTTP and AWS SDK tests |
| SigV4 presigned-query validation | implemented | n/a | implemented | yes | request validation exists separately from the first-party presign surface, which can now return proxy, backend-direct, or delegated-read grants depending on provider capabilities |
| first-party presign generation | implemented | n/a | implemented | yes | provider-agnostic Core contracts plus `POST /integrated-s3/presign/object` and `IntegratedS3.Client` now cover proxy grants, backend-direct overrides when a provider implements them, and delegated-read S3 provider URLs with safe proxy fallback; native S3 `PUT` still falls back to proxy |
| multipart upload lifecycle | implemented | emulated | implemented | yes | initiate/upload-part/complete/abort plus bucket-level `GET ?uploads` listing/discovery are implemented on the current disk/Core/HTTP surface, and the native S3 provider now implements the same provider-facing lifecycle/listing natively through the AWS SDK; orchestration remains primary-backend-only and still rejects both replicated write modes |
| object tags | partially implemented | emulated | implemented | yes | direct contracts and S3-compatible `GET` / `PUT` / `DELETE ?tagging` now cover current and archived object versions on the currently supported surface, including version-id-aware delete-tagging parity; current and archived tag persistence can now default to platform-managed object state when available, while broader tagging edge cases are still pending |
| versioning | implemented for the current vertical slice | emulated with platform-managed historical catalogs | implemented for the current supported surface | yes | current object versions receive persisted opaque version IDs, overwrites archive historical versions, `versionId` read/head/delete/tagging/copy-source flows can target archived versions, `GET` / `PUT ?versioning` round-trip through Core/disk/HTTP, `GET ?versions` now lists historical versions and delete markers, version-enabled deletes now create S3-compatible delete markers, delete-marker reads now cover current-object `NoSuchKey` plus explicit-version `MethodNotAllowed` fidelity with the expected delete-marker/version/`Last-Modified` headers, and historical plus current auxiliary object state can be platform-managed when an object-state store is registered |
| checksums | partially implemented | emulated | partially implemented | yes | disk now computes and persists checksums for the current put/copy/multipart flows, the native S3 provider now forwards/echoes checksum data on native put/copy/multipart paths where the upstream SDK exposes it cleanly, and the current HTTP surface emits the supported checksum fields in copy-object XML plus object-response headers; deeper checksum/header edge cases and broader parity hardening are still pending |
| ACL / policy behavior | not started | unsupported | not implemented | no | authorization is `ClaimsPrincipal`-driven rather than S3 ACL compatible today, but provider support-state descriptors can now report access-control ownership independently from capability support |
| CORS | implemented for the current slice | emulated | implemented | yes | bucket-level CORS contracts/configuration now round-trip through Core; disk reports `Cors = Emulated`, the native S3 provider reports `Cors = Native`, and ASP.NET now serves bucket-aware preflight plus actual-response headers without global middleware, including `Vary` handling for both allowed and rejected browser-facing CORS evaluations |
| object lock / retention / legal hold | not started | unsupported | not implemented | no | behavior still needs abstractions and provider persistence shape, but retention ownership can now be surfaced separately in provider support-state descriptors |
| server-side encryption variants | implemented for the current slice | unsupported with explicit guardrails | implemented for the current slice | yes | provider-agnostic object-level SSE request/response models now cover the current `AES256` / `aws:kms` flows plus object-metadata surfacing; the native S3 provider, Core/catalog flow, and ASP.NET S3-compatible header parsing/response emission now cover put/copy/initiate-multipart plus put/copy/get/head response metadata, while the disk provider explicitly rejects managed SSE request shapes and broader variants/provider expansion remain pending |

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

Current/documented semantics for Track F should stay provider-agnostic:

- primary succeeds, backup is pending or fails:
  - under today's strict `WriteThroughAll`, required replicas are preflighted for health/currentness before primary mutation; if a replica still fails after that point, the overall request fails and the divergence is recorded as outstanding repair work
  - under today's `WriteToPrimaryAsyncReplicas`, the primary request can succeed while provider-agnostic repair backlog/dispatcher seams record replica work for later dispatch and visibility
- backup succeeds, primary fails:
  - the overall request still fails because the primary remains authoritative for current orchestration
  - any replica-side effect is cleanup/reconciliation work, not committed success
- metadata diverges / object content diverges:
  - divergence should remain visible as outstanding repair work instead of being silently normalized inside provider-specific code
- replica is stale:
  - stale replicas are not considered caught up
  - replicas with outstanding repairs are treated as not current for strict write-through preflight and are avoided for replica-preferred reads by default unless explicitly configured otherwise
  - tracked outstanding repairs improve routing/preflight decisions, but they are still not a general durability proof that every replica is fully caught up
- provider is unhealthy:
  - unhealthy providers can be deprioritized or skipped for reads via evaluator, probe, and dynamic snapshot state
  - health state alone does not imply automatic write replay, repair completion, or topology mutation
- reconciliation cannot complete:
  - incomplete repair attempts must remain visible as backlog/divergence and must not be reported as success just because some replicas were updated

Without these rules, multi-backend support becomes operational chaos.

Status:

- multiple registered backends are now supported by the Core orchestration path
- `PrimaryOnly`, synchronous `WriteThroughAll`, and `WriteToPrimaryAsyncReplicas` are implemented as the current concrete consistency modes
- mirrored writes currently cover bucket create/delete/versioning, object put/delete, copy-object, and current-version tag mutation behavior, and async replica work is recorded through provider-agnostic repair backlog/dispatcher seams
- read routing now supports `PrimaryOnly`, `PreferPrimary`, and `PreferHealthyReplica`
- backend health can now be injected through `IStorageBackendHealthEvaluator` so unhealthy providers can be deprioritized without coupling providers to a specific health framework
- Core now layers dynamic backend-health snapshots on top of the evaluator and can optionally use `IStorageBackendHealthProbe` plus `StorageBackendHealthOptions` to actively probe backends, avoid recently unhealthy replicas after failover-triggering errors, restore preferred routing after recovery, and skip replicas with outstanding repairs by default
- strict `WriteThroughAll` can fail before primary mutation when a required replica is unhealthy or not current, while post-primary partial failures remain visible as backlog entries instead of silent retries; health/admin endpoints and richer repair execution are still pending

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
- current object responses now emit `x-amz-version-id`, direct put-object requests can validate the currently supported `x-amz-checksum-*` headers, and bucket routes now support S3-compatible `GET` / `PUT ?versioning`
- S3-compatible root/bucket/object routing is implemented alongside the JSON convenience endpoints for the currently supported operations
- AWS Signature Version 4 request validation is implemented for authorization-header and presigned-query requests, and first-party presign generation is now available separately through `POST /integrated-s3/presign/object`
- SigV4-compatible `aws-chunked` request bodies are decoded for the currently supported write flows, including multipart part upload
- batch delete and S3-compatible XML list responses are implemented for the supported bucket-level routes, including delimiter/common-prefix and `start-after` handling on the `list-type=2` surface
- authenticated ASP.NET requests now flow `HttpContext.User` into Core authorization evaluation
- feature-group toggles are implemented through `IntegratedS3EndpointOptions` plus map-time overrides

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

Current first slice status:

- first-party presign issuance is now exposed as an application-facing feature
- returned grants can now stay proxy-mode or use provider-backed direct/delegated reads depending on the caller hint, backend support, and resolver results
- native S3 delegated `GET` presigns are live today, while unresolved flows and current native S3 `PUT` requests still safely fall back to proxy-mode against the IntegratedS3 host

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

- multipart upload support now covers initiate/upload-part/complete/abort plus bucket-level `GET ?uploads` listing/discovery for the current disk/Core/HTTP surface, but broader parity such as `encoding-type=url`, deeper client-compat edge cases, and richer multipart subresource hardening is still pending
- object tagging now covers current and archived object versions on the currently supported surface, including S3-compatible delete-tagging; broader S3 tagging edge-case behavior is still pending
- only the currently supported bucket subresources are implemented; unsupported S3 bucket/object subresources intentionally return `NotImplemented`
- S3-compatible bucket listing now covers both `list-type=2` and the current `?versions` subresource, while additional subresource combinations are still pending
- conditional behavior is solid for the current `GET` / `HEAD` paths, but broader S3 precedence and edge-case parity still need hardening
- SigV4 validation and `aws-chunked` decoding work for the implemented routing surface, but canonical-request edge cases and parity hardening should continue before claiming wider compatibility
- first-party presign generation now supports proxy-mode object `GET` / `PUT` plus provider-backed direct/delegated reads where the current backend or resolver seams can satisfy the request; native S3 `PUT` still falls back to proxy today

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

Status: **native copy/multipart/presign/checksum provider slice is now implemented**

- `S3StorageService` now implements bucket CRUD, object CRUD, object/version listing, object tagging, bucket versioning, bucket CORS, native copy, and the multipart upload lifecycle/listing surface through the AWS SDK
- `IS3StorageClient` / `AwsS3StorageClient` now bridge the AWS SDK v4 surface for range, conditional, tagging, version-aware object flows, native copy, multipart upload listing/lifecycle, presigned GET URL generation, and checksum request/response mapping
- canonical error translation, DI registration, options, and S3-backed delegated-read resolver registration are in place
- `S3StorageCapabilities` now declare bucket/object/list/range/conditional/tag/versioning/copy/multipart/presign/checksum/CORS support as `Native`
- next steps center on broader parity/conformance hardening against local S3-compatible endpoints and deciding whether backend-direct presign adds enough value beyond the current delegated-read resolver path

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

Status:

- the first presign-centric client slice is now implemented with `IIntegratedS3Client` / `IntegratedS3Client`, typed presign request/response contracts, convenience `PresignGetObjectAsync` / `PresignPutObjectAsync` helpers, and `HttpRequestMessage` materialization for returned grants
- the client package now also exposes first-party `AddIntegratedS3Client(...)` registration so consumers can resolve `IIntegratedS3Client` / `IntegratedS3Client` from DI, bind `IntegratedS3:Client` settings, and reuse the same named `IHttpClientFactory` client for additional policies or transfer helpers
- typed streaming upload/download helpers are now implemented in `IntegratedS3ClientTransferExtensions`: `UploadStreamAsync`, `UploadFileAsync`, `DownloadToStreamAsync`, `DownloadToFileAsync` — all streaming-first, AOT-friendly, and accepting separate presign and transfer `HttpClient` instances; covered by dedicated tests in `IntegratedS3ClientTransferTests`
- `DownloadToFileAsync` bug fixed: presigned URL is now obtained before the destination file is opened, and a `finally` block deletes the partial/empty file on any failure or cancellation — four targeted tests cover presign-failure, transfer-error, cancellation, and access-mode overload failure scenarios
- `IntegratedS3Client.PresignObjectAsync` now reads the response body before throwing on non-success status, surfacing actionable server error detail in the `HttpRequestException` message; covered by `IntegratedS3ClientTests` (403, 400, 503, empty-body cases)
- `IntegratedS3.Client` now hardens the higher-level transfer surface for larger-object and checksum-aware flows: checksum-aware `PresignPutObjectAsync` / `UploadStreamAsync` / `UploadFileAsync` overloads compute supported full-object checksums (`SHA256`, `SHA1`, `CRC32`, `CRC32C`) in a streaming pre-pass, forward them through first-party presign grants as signed headers, and validate echoed upload checksum headers when present; file uploads now open with `SequentialScan`, and download helpers validate full-object checksum headers on streamed and resume-aware downloads while intentionally skipping multipart/composite checksum values that cannot yet be recomputed from the plain body alone; covered by `IntegratedS3ClientPresignExtensionsTests`, `IntegratedS3HttpPresignStrategyTests`, and `IntegratedS3ClientTransferTests`

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

These are optional host integrations around Core orchestration seams, not a commitment that reconciliation/repair must ship as a built-in always-on background-service architecture. Track F now exposes provider-agnostic divergence/backlog/dispatch seams so consumers can host replay/repair via `IHostedService`, external schedulers, or other operational tooling without being forced into a mandatory built-in hosted service.

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
- focused fault-injection/orchestration coverage now exists for async replica recording/dispatch, unhealthy-replica preflight, outstanding-repair read behavior, partial-write backlog semantics, and failed-repair visibility in Core orchestration tests
- S3 conformance, broader multi-provider fault-injection coverage, trimming/AOT publish verification, and benchmark automation are still pending

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
- SigV4 authorization-header and presigned-query request validation are implemented on the ASP.NET surface for compatibility scenarios
- first-party presign generation is now exposed as an application-facing capability through provider-agnostic Core contracts (`StoragePresignRequest`, `StoragePresignedRequest`, `IStoragePresignService`, `IStoragePresignStrategy`) and an ASP.NET `POST /integrated-s3/presign/object` endpoint for object `GET` / `PUT` flows; issuance can stay proxy-mode or return backend-direct / delegated-read grants when the current provider seams can satisfy the request
- `IntegratedS3.Client` now ships the first meaningful surface: `IIntegratedS3Client` / `IntegratedS3Client`, presign convenience helpers, a helper that turns returned grants into `HttpRequestMessage` instances, and typed streaming/file transfer helpers
- native S3 provider — **copy/multipart/presign/checksum slice is now implemented on top of the object/list/versioning foundation**:
  - `S3StorageService` implements `ListBucketsAsync`, `CreateBucketAsync`, `HeadBucketAsync`, and `DeleteBucketAsync` via the native AWS SDK
  - `S3StorageService` now also implements `ListObjectsAsync`, `ListObjectVersionsAsync`, `HeadObjectAsync`, `GetObjectAsync`, `PutObjectAsync`, `DeleteObjectAsync`, `CopyObjectAsync`, the multipart upload lifecycle/listing surface, `GetObjectTagsAsync`, `PutObjectTagsAsync`, `DeleteObjectTagsAsync`, `GetBucketVersioningAsync`, `PutBucketVersioningAsync`, `GetBucketCorsAsync`, `PutBucketCorsAsync`, and `DeleteBucketCorsAsync`
  - `IS3StorageClient` / `AwsS3StorageClient` now bridge the AWS SDK v4 surface for object CRUD, paged listings, conditional GETs, range requests, tagging, versioning, bucket CORS, native copy/multipart flows, presigned GET URL generation, and checksum request/response mapping
  - `S3ErrorTranslator` maps `AmazonS3Exception` to canonical `StorageErrorCode` values (`BucketNotFound`, `CorsConfigurationNotFound`, `BucketAlreadyExists`, `AccessDenied`, `PreconditionFailed`, `Throttled`, `ProviderUnavailable`) with provider name and bucket/object context preserved
  - `CreateBucketAsync` still rejects `EnableVersioning = true` during bucket creation; versioning is enabled afterward through `PutBucketVersioningAsync`
  - `S3StorageCapabilities` now report `BucketOperations`, `ObjectCrud`, `ObjectMetadata`, `ListObjects`, `Pagination`, `RangeRequests`, `ConditionalRequests`, `ObjectTags`, `Versioning`, `Cors`, `MultipartUploads`, `CopyOperations`, `PresignedUrls`, and `Checksums` as `Native`; advanced features remain `Unsupported`
  - DI registration and options wiring are in place for the S3 provider, including S3-backed delegated read-location resolution through `IStorageObjectLocationResolver`
  - automated xUnit coverage for the S3 provider foundation slice is in place (`S3StorageServiceTests`): capabilities snapshot, support state descriptor, bucket operations, paged listings, object CRUD, copy, multipart lifecycle/listing, tags, versioning, checksum mapping, bucket CORS, 304/412 flows, disposal behavior, and DI bootstrap
- multipart upload, copy-object, delegated-read presign semantics, and checksums are now implemented for the native S3 provider; broader S3 integration/conformance coverage and any optional backend-direct `PUT` presign follow-on work are still pending
- current first-party presign generation now supports backend-direct or delegated reads when available and safely falls back to proxy-mode against the IntegratedS3 host when those provider paths are unavailable or incompatible
- initial client package support now includes typed streaming/file transfer helpers, while larger-object, resume, and checksum-aware ergonomics remain pending

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
- current multipart orchestration is intentionally limited to primary-backend semantics and returns an explicit unsupported-capability error when either replicated write mode is enabled
- broader protocol fidelity such as additional bucket/object subresources, remaining checksum/header edge cases, and deeper edge-case compatibility is still pending

### M5 — Mirroring, backup, and reconciliation

- multiple backends
- write routing
- health-aware reads
- background reconciliation

Status: **in progress / partially complete**

- multiple backends can now be registered and surfaced through descriptors
- write routing now supports `PrimaryOnly`, strict `WriteThroughAll`, and `WriteToPrimaryAsyncReplicas`
- mirrored writes currently cover bucket create/delete/versioning, object put/delete, copy-object, and current-version object-tag mutation operations, with async replica work recorded through provider-agnostic repair backlog/dispatcher seams
- read routing now supports `PrimaryOnly`, `PreferPrimary`, and `PreferHealthyReplica`, and replica-preferred reads now avoid outstanding-repair replicas by default unless explicitly configured otherwise
- first health-aware provider selection is now implemented through an overrideable backend-health evaluator used by Core read orchestration
- Core now also tracks dynamic backend-health snapshots, marks failover-triggering read failures as temporarily unhealthy, can optionally probe backends to restore healthy read preference without replacing existing evaluator overrides, and preflights required replicas before strict write-through primary mutation when they are unhealthy or not current
- replica-write divergence is now surfaced through the repair backlog for both async replica dispatch and post-primary partial synchronous failures; multipart remains explicitly unsupported under both replicated write modes
- background reconciliation/repair execution is still scaffolded and optional-host-integrated rather than a required Core background-service model

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

Status: **in progress / partially complete**

- bucket-level CORS is now implemented across abstractions, Core orchestration, the disk provider, the native S3 provider, and the ASP.NET S3-compatible surface
- ASP.NET now supports bucket-aware preflight `OPTIONS` handling and actual-response `Access-Control-*` headers without relying on global middleware
- ACL/policy mapping, object lock / retention / legal hold, and encryption-related support are still not started

### M8 — Hardening

- conformance improvement
- benchmark tuning
- docs
- end-to-end sample polish

Status: **in progress / partially complete**

- conformance coverage now includes version-aware presigned reads, mixed bucket-subresource rejection, multipart `encoding-type=url` rejection, and additional AWS SDK compatibility cases
- build/test/self-contained publish validation plus the current AOT warning posture are now automated through `.github\workflows\trackh-publish-aot-ci.yml` and `eng\Invoke-AotPublishValidation.ps1`
- `docs/webui-reference-host.md` now captures the current reference-host surface and validation commands
- benchmark baselines and additional sample consumers remain pending

## Remaining Implementation Work by Parallel Track

This section is the execution board for the remaining implementation backlog. Assign one contributor or PR stream per track and keep status updates confined to that track subsection plus any affected capability-matrix row above.

### Ready-now track selection

- Tracks A, D, E, and H can move immediately with today's abstractions and package boundaries.
- Track B is now in hardening mode rather than foundation-build mode: the native S3 copy/multipart/checksum/SSE/delegated-read slice is already landed, so the next work is local-endpoint parity plus broader provider-specific conformance.
- Track C can continue immediately: provider-agnostic direct-presign plumbing, delegated-read S3 wiring, and typed transfer helpers are now in place, so the remaining work is higher-level client/API hardening rather than foundational presign plumbing.
- Track F can continue on orchestration semantics, failure/backlog boundaries, and optional hosted-service seams now, with end-to-end repair behavior still benefiting from richer multi-provider parity.
- Track G is now active for the bucket-CORS plus initial SSE slice; the remaining ACL/policy and retention/legal-hold follow-on work still benefits from Track A design follow-through.

### Track A — Architecture follow-through and provider modes

- Packages: `IntegratedS3.Abstractions`, `IntegratedS3.Core`, `IntegratedS3.EntityFramework`, `docs/`
- Ready: now
- Depends on: none
- Status update:
  - `StorageProviderMode` is now explicit on `IStorageBackend`, provider descriptors, configured-provider metadata, and the service-document surface
  - `StorageObjectLocationDescriptor` now reports provider-agnostic object-access shapes (`ProxyStream`, `Redirect`, `Delegated`, `Passthrough`) separately from capability support
  - `IStorageObjectLocationResolver` is now the additive location-resolution seam; Core registers a null/default implementation so future providers can opt in without forcing persistence or redirect behavior onto current deployments
  - current providers now advertise their architecture explicitly: disk reports `Managed` or `Hybrid` depending on whether support state is externalized, while the native S3 provider reports `Delegated`
- Remaining scope:
  - decide whether the current `native | emulated | unsupported` capability model needs extra semantics such as delegated, externalized, or proxied for future providers
  - wire actual redirect/delegated object-read execution paths onto the new location abstractions once provider-backed presign/direct-access semantics settle
  - evaluate whether the remaining split support services should stay bundled inside `IStorageObjectStateStore` or be extracted into dedicated tag/checksum/location abstractions
  - decide whether upload-initiation and passthrough-response flows should reuse `IStorageObjectLocationResolver` or graduate into additional dedicated access-resolution abstractions

### Track B — Native S3 provider completion

- Packages: `IntegratedS3.Provider.S3`, `IntegratedS3.Abstractions`, `IntegratedS3.Tests`
- Ready: now
- Depends on: none for copy/multipart basics; coordinate with Track A only if new support abstractions are introduced
- Status:
  - the core native S3 copy/multipart/checksum/delegated-read presign slice is now implemented in `IntegratedS3.Provider.S3`
  - `S3StorageCapabilities` now report `CopyOperations`, `MultipartUploads`, `PresignedUrls`, and `Checksums` as `Native`, and provider-specific unit coverage now covers copy, multipart lifecycle/listing, checksum mapping, bucket CORS, and capability/provider-mode reporting
  - delegated `GET` presign semantics are now wired end-to-end for the native S3 provider: `ResolveObjectLocationRequest` now carries requested expiry, `IntegratedS3HttpPresignStrategy` forwards provider/expiry/version metadata through the resolver seam, `AddS3Storage(...)` registers an S3-backed object-location resolver, and the provider now advertises native presigned-URL plus delegated read-location support while the separate backend-direct presign seam remains available for future providers and native S3 `PUT` presigns stay proxy-only
  - focused presign/provider tests now cover delegated S3 URL issuance, expiry/version forwarding, proxy fallback for unavailable or incompatible delegated resolution, and the intentional proxy-only `PUT` behavior
- Remaining scope:
  - harden native S3 behavior against local S3-compatible endpoints and extend provider-specific parity/conformance coverage around the already-landed copy/multipart/checksum/presign slice
  - evaluate later whether the native S3 backend should also use the now-available backend direct-presign seam for direct `GET` / `PUT` in addition to the implemented delegated-read resolver path
  - keep public contracts provider-agnostic while preserving the clarified provider capability/support-state reporting

### Track C — First-party presign and client surface

- Packages: `IntegratedS3.Client`, `IntegratedS3.AspNetCore`, `IntegratedS3.Protocol`, `IntegratedS3.Core`
- Ready: now
- Depends on: none for the current presign/client slice; future provider-native overrides remain optional follow-on work
- Status:
  - provider-agnostic presign contracts and authorization-aware Core services are now implemented
  - ASP.NET now exposes `POST /integrated-s3/presign/object` with configurable signing-credential resolution and public-base-url fallback for proxy issuance, while the same endpoint can also surface backend-direct or delegated read grants through the new provider seams
  - `IntegratedS3.Client` now wraps the endpoint, can materialize upload/download `HttpRequestMessage` instances from returned grants, exposes direct-access convenience overloads for object `GET` / `PUT` presigns, and ships typed streaming/file transfer helpers
  - the first-party client package now also ships `AddIntegratedS3Client(...)` with named `IHttpClientFactory` integration so consumers can register the client from configuration or inline options instead of manually constructing `HttpClient` instances
  - `StorageAccessMode` now covers `Proxy`, `Direct`, and `Delegated` modes; `StoragePresignRequest` now carries an optional `PreferredAccessMode` field so callers can express access-mode preference; strategies may honour, downgrade, or ignore the preference depending on provider capabilities
  - `IStorageBackend` now includes a provider-agnostic direct-presign seam for object `GET` / `PUT`; `IntegratedS3HttpPresignStrategy` consults the primary backend first when `PreferredAccessMode` is `Direct`, then falls back to the existing resolver/proxy paths when no backend-native grant is available
  - `IntegratedS3HttpPresignStrategy` still consults `IStorageObjectLocationResolver` when `PreferredAccessMode` is `Direct` or `Delegated` for `GetObject` presigns; maps `StorageObjectAccessMode.Delegated`/`Passthrough` → `StorageAccessMode.Delegated` and `StorageObjectAccessMode.Redirect` → `StorageAccessMode.Direct`; falls back to proxy-mode issuance when the resolver returns null, an incompatible mode, or the operation is not a read
  - native S3-backed delegated `GET` presigns now flow through that resolver seam: resolver requests carry provider/expiry/version metadata, the S3 provider returns real provider URLs for delegated reads, and unsupported or incompatible requests still fall back to proxy-mode issuance
  - client transfer helpers now accept explicit preferred access-mode hints, keep streaming/file transfers on the high-level client surface, delete failed download targets instead of leaving partial files behind, and surface presign-endpoint error bodies in thrown `HttpRequestException` messages
  - the first-party client now also exposes explicit resume-aware file download helpers that issue `Range` requests against existing partial files, rewrite from the start when a server ignores the range, treat matching `416 RequestedRangeNotSatisfiable` responses as already complete, and preserve pre-existing partial files while cleaning up files created by the current attempt
  - first-party client defaults now intentionally keep access-mode selection explicit at the caller boundary: overloads that omit `PreferredAccessMode` / `preferredAccessMode` leave the hint unset, do not infer `Direct` / `Delegated` access from provider/service discovery, and therefore keep proxy-mode as the stable default; callers can inspect provider descriptors for UX and then opt into `Direct` / `Delegated` explicitly where appropriate
  - focused tests lock in: backend-direct selection for read and write presigns; primary-backend-only direct selection with safe proxy fallback; delegated-mode selection when resolver supplies a `Delegated` or `Passthrough` location; direct-mode selection when resolver supplies a `Redirect` location; proxy fallback for null/incompatible resolver results; resolver not consulted for write presigns or when no preferred mode is set; default client/presign overloads leave access-mode unset; version ID forwarded to resolver and returned in the grant; resolved headers forwarded to the grant; expiry derived from the resolver when provided, falling back to request expiry
- Remaining scope:
  - harden the higher-level client/transfer surface around larger-object and checksum-aware scenarios now that access-mode selection stays explicit at the caller boundary
  - evaluate later whether the first-party client should expose optional discovery helpers for host UX/inspection without silently changing transfer routing or presign defaults

### Track D — ASP.NET endpoint ergonomics and authorization surface

- Packages: `IntegratedS3.AspNetCore`, `WebUi`, `IntegratedS3.Tests`
- Ready: now
- Depends on: none for endpoint-grouping work; coordinate with Track C if presign endpoints/options are added
- Status:
  - `AddIntegratedS3(...)` now supports configuration binding plus inline overrides in the same call, `AddIntegratedS3Provider(...)` adds named/manual configured-provider metadata without forcing direct list mutation, and `AddIntegratedS3Backend(...)` now provides higher-level singleton DI registration for custom backend types or factories while still wiring the standard runtime descriptor/capability services
  - `MapIntegratedS3Endpoints(...)` now accepts endpoint options for service, bucket, object, multipart, and admin toggles plus whole-route, per-feature, and explicit shared root/compatibility route-group configuration callbacks for authorization/policy wiring
  - shared `GET /` and `/{**s3Path}` routes now use explicit `ConfigureRootRouteGroup` / `ConfigureCompatibilityRouteGroup` callbacks (or whole-route protection) instead of inheriting multiple feature-group callbacks onto one route
  - `WebUiApplication.ConfigurePipeline(...)` and the isolated test-host wiring can now forward endpoint-mapping options and conditionally enable `UseAuthorization()` alongside `UseAuthentication()`
  - `IntegratedS3EndpointOptions` now bind from `IntegratedS3:Endpoints`, and the map-time `MapIntegratedS3Endpoints(...)` overloads now start from configured endpoint defaults so host-configured toggles remain overrideable per host/test
  - `IntegratedS3EndpointOptions` now expose `IntegratedS3EndpointFeature`-keyed `SetFeatureRouteGroupConfiguration(...)` / `GetFeatureRouteGroupConfiguration(...)` helpers, and the current `Configure*RouteGroup` properties are wrappers over that shared per-feature registry so future endpoint surfaces can follow the same callback pattern
- Remaining scope:
  - evaluate whether common authorization conventions should also be exposed as configuration-bound host options in addition to the current map-time route-group callbacks
  - revisit whether future endpoint surfaces should automatically grow matching per-feature route-group callbacks as they are added
  - preserve `CreateSlimBuilder(...)`, Minimal API, trimming, and AOT friendliness as any follow-up ergonomics polish lands

### Track E — S3 protocol fidelity and edge-case hardening

- Packages: `IntegratedS3.AspNetCore`, `IntegratedS3.Protocol`, `IntegratedS3.Provider.Disk`, `IntegratedS3.Tests`
- Ready: now
- Depends on: coordinate with Tracks B and C as parity and client-surface hardening follow-ons land
- Status update:
  - bucket-level multipart listing/discovery (`GET ?uploads`) is now implemented end-to-end for the current disk/Core/HTTP surface
  - the new slice adds provider/service contracts for multipart listing, platform-managed multipart-state enumeration via `IStorageMultipartStateStore`, disk-provider sidecar fallback enumeration, S3-compatible `ListMultipartUploadsResult` XML output, delimiter/common-prefix handling, marker pagination (`key-marker` / `upload-id-marker`), and multipart feature-toggle enforcement on the bucket route
  - focused coverage now locks in empty listings, completed/aborted upload exclusion, platform-managed multipart-state enumeration, virtual-hosted-style HTTP behavior, feature-toggle blocking, and AWS SDK `ListMultipartUploadsAsync` compatibility for the current supported surface
  - bucket/object-compatible subresource validation now uses an explicit supported-matrix for bucket `?versioning`, `?cors`, `?uploads`, and `?versions` plus object `?tagging`, `?versionId`, and multipart workflows, rejects remaining unsupported single subresources with consistent `NotImplemented` responses, and returns explicit unsupported-combination results for invalid mixed query sets
  - focused HTTP coverage now locks in that SigV4 presign query parameters such as `X-Amz-*` and `x-id` continue to be ignored during bucket/object subresource validation for the currently supported paths
  - protocol/conformance coverage now locks in canonical empty-value subresource signing plus presigned bucket-versioning and historical-version reads on the S3-compatible route
  - non-empty bucket deletion now preserves S3-compatible `BucketNotEmpty` / `409 Conflict` semantics across the disk provider, native S3 translation path, and S3-compatible HTTP DeleteBucket surface, with focused regression coverage at each layer
- Remaining scope:
  - next: harden conditional precedence, checksum/header behavior, and canonical-request edge cases now that the bucket/object subresource matrix is explicit on the S3-compatible HTTP surface
  - continue versioning/tagging/delete-marker parity work for the remaining advanced edge cases
  - keep `aws-chunked`, presigned-query, and virtual-hosted-style compatibility tightening against real client behavior
  - decide whether multipart `encoding-type=url` and further multipart-listing edge semantics should be implemented next or remain explicitly unsupported for now

### Track F — Multi-backend async replication, health, and reconciliation

- Packages: `IntegratedS3.Core`, `IntegratedS3.Provider.*`, `IntegratedS3.AspNetCore`, `IntegratedS3.Tests`
- Ready: orchestration semantics now; richer repair/testing once Tracks B and H add more parity coverage
- Depends on: benefits from Track B for stronger second-provider scenarios
- Status update:
  - synchronous mirrored writes are implemented today via `WriteThroughAll`; that path currently covers bucket create/delete/versioning, object put/delete, copy-object, and current-version tag mutation flows
  - `WriteToPrimaryAsyncReplicas` is now implemented in Core for the current replicated-write surface by recording provider-agnostic `IStorageReplicaRepairBacklog` entries and dispatching through `IStorageReplicaRepairDispatcher`; the default in-process dispatch path is optional and durable replay remains a host concern
  - health-aware reads now combine `IStorageBackendHealthEvaluator`, dynamic snapshots, and optional `IStorageBackendHealthProbe` probing so recently unhealthy providers can be deprioritized and later reconsidered for read preference, while replicas with outstanding repairs are avoided by default unless explicitly configured otherwise
  - strict `WriteThroughAll` now preflights required replicas for health/currentness before mutating the primary, and post-primary partial failures are recorded as repair backlog entries instead of being rolled back or silently normalized
  - the admin HTTP surface now exposes read-only repair-backlog visibility at `GET /integrated-s3/admin/repairs`, including optional `replicaBackend` filtering and route-group auth/endpoint-toggle coverage
  - focused orchestration coverage now locks in async replica recording/dispatch, unhealthy-replica preflight, outstanding-repair read behavior, partial-write backlog semantics, failed-repair visibility, multi-replica dispatch-recording failure isolation, mixed replay success/failure, and backlog growth for only the replicas that remain stale
- Current semantics and boundaries:
  - replicas with outstanding repairs are treated as not current for strict write-through preflight and are excluded from replica-preferred reads by default, but that remains a tracked divergence signal rather than a blanket durability proof that every replica is fully caught up
  - unhealthy providers affect read routing today and repair backlog visibility is now exposed on the admin surface, but health state still does not imply automatic async replay completion, provider eviction, or topology mutation
  - if a synchronous replica fails after the primary succeeds, the request still fails; Core does not roll the primary back, and the divergence remains visible in the repair backlog until a later repair pass finishes
  - incomplete or failed repair attempts remain visible as reconciliation backlog or outstanding divergence and can influence subsequent reads and writes; partial repair must not be reported as success
  - multipart remains explicitly unsupported under both replicated write modes
  - reconciliation/repair work in this track stays provider-agnostic and optional-host-integrated rather than locking the product into a mandatory built-in background-service architecture
- Remaining scope:
  - add richer reconciliation and divergence-repair semantics for content, metadata, and version drift beyond the current backlog/dispatcher scaffold
  - add optional host integrations such as mirror replay, orphan detection, checksum verification, multipart cleanup, index compaction, and expired-artifact cleanup without coupling providers or Core to a fixed hosting model
  - expand admin visibility beyond repair backlog into provider health, replica lag, and richer repair diagnostics without coupling providers to a fixed hosting model
  - broaden failure-semantics and multi-provider fault-injection coverage beyond the focused async-replication/backlog/read-policy scenarios already covered

### Track G — Advanced S3 feature slices

- Packages: `IntegratedS3.Abstractions`, `IntegratedS3.Core`, `IntegratedS3.AspNetCore`, `IntegratedS3.Provider.Disk`, `IntegratedS3.Provider.S3`, `IntegratedS3.Tests`
- Ready: active
- Depends on: Track A for the remaining retention/support-state follow-ons; coordinate with Track F if cross-backend behavior matters
- Status update:
  - bucket-level CORS contracts and request models are now implemented in `IntegratedS3.Abstractions`, `IStorageService`, `IStorageBackend`, Core authorization, and Core write-through replication
  - the disk provider now persists bucket CORS in the existing bucket-metadata sidecar and reports `Cors = Emulated`
  - the native S3 provider now maps bucket CORS through the AWS SDK and reports `Cors = Native`
  - ASP.NET now supports S3-compatible `GET` / `PUT` / `DELETE ?cors` XML handling plus bucket-aware preflight `OPTIONS` and actual-response `Access-Control-*` headers without global middleware, including anonymous preflight evaluation against bucket CORS rules
  - provider support-state descriptors now surface `AccessControl` and `ServerSideEncryption` ownership alongside `Retention`, with disk remaining `NotApplicable` and the native S3 provider reporting delegated ownership for those upstream-owned concerns
  - `IntegratedS3.Abstractions` now includes provider-agnostic object-level SSE request/response models for the initial `AES256` / key-managed slice, and catalog-backed object metadata can now carry optional SSE algorithm/key metadata without leaking provider SDK types into shared contracts
  - the native S3 provider now reports `ServerSideEncryption = Native`, maps the initial object-level `AES256` / `aws:kms` request slice through the AWS SDK for put/copy/initiate-multipart, and preserves returned object SSE metadata on provider-facing `ObjectInfo` results
  - ASP.NET now validates and normalizes the initial object-level `AES256` / `aws:kms` SSE headers for S3-compatible put/copy/initiate-multipart requests, rejects unsupported SSE request shapes explicitly, and emits returned SSE response headers on put/copy/get/head object responses
  - the disk provider now rejects the managed SSE request shapes explicitly across the current get/head/put/copy/initiate-multipart surface so unsupported requests fail consistently instead of being ignored
  - `IntegratedS3.Tests` now exercises the end-to-end SSE slice across contracts, S3 provider mapping, disk guardrails, HTTP header/response behavior, and catalog persistence for the shipped `AES256` / `aws:kms` surface
  - browser-facing CORS handling now preserves `Vary` headers for both allowed and rejected preflight/actual responses, and S3-compatible bucket subresource validation now ignores SigV4 presign query parameters so presigned bucket-versioning reads continue to work
- Remaining scope:
  - ACL/policy-compatible behavior where it makes sense alongside the existing `ClaimsPrincipal` authorization model
  - object lock, retention, and legal-hold abstractions plus provider persistence shape
  - any remaining server-side-encryption follow-on work beyond the implemented native-S3-plus-explicit-disk-guardrails `AES256` / `aws:kms` slice, including broader SSE variants and any future provider expansion, without leaking provider SDK details into shared contracts
  - any remaining CORS conformance hardening beyond the implemented bucket-configuration, `Vary` handling, and browser-flow slice

### Track H — Hardening, conformance, performance, samples, and release polish

- Packages: `IntegratedS3.Tests`, `IntegratedS3.Testing`, `WebUi`, `docs/`
- Ready: now
- Depends on: can scaffold immediately; full coverage expands as the remaining Tracks B through G hardening slices land
- Status update:
  - `IntegratedS3SigV4ConformanceTests` now cover presigned bucket-versioning reads, presigned historical-version reads, and presigned expiry/clock-skew XML error behavior, while `IntegratedS3SigV4ProtocolTests` lock in canonical empty-value subresource signing.
  - `IntegratedS3HttpEndpointsTests` now cover unsupported mixed bucket subresources and multipart `encoding-type=url` rejection, and `IntegratedS3AwsSdkCompatibilityTests` now include version-id-aware metadata/read coverage.
  - `IntegratedS3CoreOrchestrationTests` now cover provider-unavailable read failover, no failover on not-found, unhealthy snapshot expiry recovery, probe-timeout handling, async replica recording/dispatch, unhealthy-replica preflight, outstanding-repair read policy, partial-write backlog semantics, failed-repair visibility, multi-replica dispatch-recording failure isolation, mixed replay success/failure, and backlog growth for replicas that remain stale.
  - `src\IntegratedS3\WebUi` now has a dedicated reference-host guide in `docs/webui-reference-host.md`, with local sample storage kept under `App_Data` and excluded from build/publish outputs.
  - CI automation now lives in `.github\workflows\trackh-publish-aot-ci.yml`, and `eng\Invoke-AotPublishValidation.ps1` enforces the current self-contained publish warning posture without depending on exact line numbers.
- Verification status (March 2026):
  - `dotnet build src\IntegratedS3\IntegratedS3.slnx` passed in the current Track E/H worktree.
  - `dotnet test src\IntegratedS3\IntegratedS3.slnx` passed in the current Track E/H worktree.
  - `dotnet publish -c Release --self-contained src\IntegratedS3\WebUi\WebUi.csproj` passed in the current Track E/H worktree, and `eng\Invoke-AotPublishValidation.ps1` now tracks the remaining Minimal API / trimming-sensitive warning posture without depending on exact line numbers.
- Remaining scope:
  - extend conformance beyond the current versioned-read, presigned-expiry/clock-skew, and AWS SDK version-id coverage into the remaining protocol edge cases and broader client-compatibility scenarios
  - extend fault-injection beyond the current unhealthy-provider, async-replication/backlog, partial-write-through, and newly added multi-replica replay coverage into broader repair/reconciliation scenarios
  - add structured logs, metrics, traces, correlation IDs, provider tags, auth-failure visibility, mirror-lag visibility, and reconciliation-backlog visibility
  - benchmark the hot paths called out in this plan and track throughput, latency, allocation, and provider-breakdown baselines
  - keep the new trimming/AOT publish automation in CI aligned with the supported host surface and reduce or document the remaining publish warnings alongside benchmark baselines
  - add the planned MVC/Razor and Blazor WebAssembly sample consumers
  - finish package polish items such as XML docs, onboarding docs, versioned protocol compatibility guidance, and any analyzers/diagnostics worth shipping
- Next recommended steps:
  - triage the remaining observed IL2026/IL3050 native AOT warnings in `IntegratedS3.AspNetCore` / `WebUi` and decide whether they should be eliminated further, annotated more precisely, or explicitly documented for consumers
  - extend conformance and protocol hardening into conditional-precedence, checksum/header, and delete-marker/versioning edge cases now that the current subresource/presign gaps are covered
  - add benchmark baselines for representative disk plus HTTP get/put/list paths before broadening the remaining release-polish work

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

Status note (March 2026): current Track H validation covers checklist items 5, 6, 8, and 10 at the command/test level; item 5 still carries 12 native AOT publish warnings, while items 9 and 11 remain open work.

## Key Decisions

- **Package shape:** modular packages first
- **Preferred browser model:** presigned URL hybrid
- **Goal:** broad S3-compatible endpoint support tracked through milestones
- **Important boundary:** “full support” must be represented by a capability matrix
- **Provider implementation order:** disk first, native S3 second
- **Current `WebUi` role:** sample/reference host, not final architecture container

## Recommended Next Execution Slices

Track A's contract slice, Track B's native S3 copy/multipart/checksum/delegated-read slice, and Track C's first direct/delegated presign-client slice are now in place, so the best next execution step is to harden the already-landed provider and protocol surfaces rather than reopen shared abstractions. In parallel, Track E can keep closing the remaining S3-compatible edge cases, Track H can expand conformance/fault-injection/publish/benchmark validation around those paths, while Track D mostly shifts to lower-priority follow-up ergonomics after the new toggle/authorization slice.

Why these should come next:

- they still touch different primary packages (`IntegratedS3.Provider.S3`, `IntegratedS3.AspNetCore`, `IntegratedS3.Protocol`, `IntegratedS3.Client`, `IntegratedS3.Tests`) so multiple contributors can move without colliding constantly
- local-endpoint parity and broader conformance now validate already-landed copy/multipart/checksum/presign and bucket-CORS slices instead of waiting on new shared contracts
- the new provider-mode and object-location contracts already reduce abstraction churn, so remaining Track B / Track C work is mostly hardening and optional backend-direct follow-on behavior rather than another foundational redesign
- protocol-fidelity hardening and conformance work can proceed in parallel with provider hardening and will immediately validate the current surface area
- any remaining ASP.NET ergonomics follow-up is mostly isolated to the integration layer and can continue without reopening the shared contracts

Recommended dependency-aware order:

1. execute Track E to harden remaining bucket/object subresource, conditional-precedence, checksum/header, and delete-marker/versioning edge cases on the S3-compatible surface
2. execute Track H in parallel to expand conformance, fault-injection, local-endpoint coverage, benchmark baselines, and publish verification around the current Track B / E / G slices
3. execute Track B in parallel to harden the landed native S3 copy/multipart/checksum/SSE/delegated-read slice against local S3-compatible endpoints and decide whether backend-direct presign should complement the current resolver path
4. continue Track C in parallel with larger-object and checksum-aware client ergonomics now that direct/delegated read paths, typed transfer helpers, and resume-aware file downloads are already shipped
5. revisit Track D follow-up only if consumer feedback warrants configuration-bound authorization conventions or more backend-registration sugar
6. continue Track F in parallel and use the remaining Track G budget for ACL/policy plus retention/encryption behavior and richer health/admin visibility now that endpoint toggles, repair-backlog visibility, descriptor follow-ons, and CORS `Vary` hardening are in place

## Suggested First Parallel Batch

Given the current implementation state, the first parallel batch should be:

1. Track E — close the remaining bucket/object subresource, conditional-precedence, checksum/header, and delete-marker/versioning edge cases on the S3-compatible HTTP surface
2. Track H — add local S3-compatible integration coverage, broader conformance/fault-injection cases, benchmark baselines, and publish automation around the already-landed provider/protocol slices
3. Track B — harden the landed native S3 copy/multipart/checksum/SSE/delegated-read slice against local S3-compatible endpoints and evaluate whether backend-direct presign should complement the current resolver path in `IntegratedS3.Provider.S3`
4. Track C follow-up — harden larger-object and checksum-aware client transfer flows on top of the current proxy/direct/delegated presign surface

### Fleet-ready agent decomposition (March 2026)

To maximize safe parallel execution without reopening shared abstractions too early, split the next wave into the following agent-ready slices:

1. `track-e-subresource-matrix`
   - Packages: `IntegratedS3.AspNetCore`, `IntegratedS3.Protocol`, `IntegratedS3.Tests`
   - Scope: close remaining bucket/object subresource recognition and unsupported-combination gaps on the S3-compatible HTTP surface
   - Depends on: none
2. `track-e-conditional-checksum`
   - Packages: `IntegratedS3.AspNetCore`, `IntegratedS3.Protocol`, `IntegratedS3.Tests`
   - Scope: harden conditional precedence, checksum/header behavior, and canonical-request edge cases
   - Depends on: none
3. `track-e-versioning-delete-marker`
   - Packages: `IntegratedS3.AspNetCore`, `IntegratedS3.Provider.Disk`, `IntegratedS3.Tests`
   - Scope: finish advanced versioning/tagging/delete-marker parity and pagination edge cases
   - Depends on: none
4. `track-b-local-s3-parity`
   - Packages: `IntegratedS3.Provider.S3`, `IntegratedS3.Tests`
   - Scope: validate and harden native S3 copy, multipart, checksum, SSE, and delegated-read behavior against local S3-compatible endpoints
   - Depends on: none
5. `track-c-transfer-hardening`
   - Packages: `IntegratedS3.Client`, `IntegratedS3.AspNetCore`, `IntegratedS3.Tests`
   - Scope: harden larger-object and checksum-aware client transfer flows on top of the current presign surface
   - Depends on: none
6. `track-h-local-conformance-harness`
   - Packages: `IntegratedS3.Tests`, `IntegratedS3.Testing`
   - Scope: expand reusable local S3-compatible integration coverage and fault-injection harnesses around the already-landed provider/protocol slices
   - Depends on: none
7. `track-h-publish-benchmark-validation`
   - Packages: `eng\`, `.github\workflows\`, `docs\`, `WebUi`
   - Scope: refresh publish validation, AOT-warning tracking, and benchmark baselines for hot paths
   - Depends on: none
8. `track-f-admin-health-visibility`
   - Packages: `IntegratedS3.Core`, `IntegratedS3.AspNetCore`, `IntegratedS3.Tests`
   - Scope: extend provider-agnostic admin visibility for health, lag, and repair diagnostics without coupling Core to a fixed hosting model
   - Depends on: none

Queue the following follow-on work behind the first wave instead of starting another abstraction redesign immediately:

- `track-h-expanded-conformance`
  - Depends on: `track-e-subresource-matrix`, `track-e-conditional-checksum`, `track-e-versioning-delete-marker`, `track-b-local-s3-parity`, `track-c-transfer-hardening`, `track-h-local-conformance-harness`
  - Scope: consolidate broader end-to-end conformance coverage after the first hardening wave lands
- `track-a-location-abstraction-review`
  - Depends on: `track-b-local-s3-parity`, `track-c-transfer-hardening`
  - Scope: revisit direct/delegated/location abstraction boundaries using provider and client hardening feedback
- `track-g-retention-encryption-design`
  - Depends on: `track-a-location-abstraction-review`
  - Scope: plan retention/legal-hold and encryption slices once the abstraction follow-through is clearer
