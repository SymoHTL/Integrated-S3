# Integrated S3 Implementation Plan

Build a modular, AOT-conscious ASP.NET storage platform that exposes an S3-compatible HTTP surface, supports pluggable backends (`S3`, disk, future providers), uses `ClaimsPrincipal`-driven authorization, and ships as reusable NuGet packages. The recommended approach is to separate protocol compatibility, storage orchestration, provider implementations, and ASP.NET integration so the system can grow toward broad S3 parity without becoming tightly coupled or hard to optimize.

## Current Implementation Status (July 2025)

The repository has already moved beyond initial scaffolding and now contains a broad working implementation with deep S3 protocol coverage.

### Recently completed implementation wave

A large implementation wave landed 13 bucket configuration subresource families, additional object-level operations, protocol/HTTP improvements, and hardening/conformance fixes end-to-end across the full stack (Abstractions → Protocol XML → Core orchestration with replication → Auth decorator → Disk provider metadata persistence → S3 provider AWS SDK delegation → HTTP endpoints). Build: 0 errors, 0 warnings. Tests: 901 passing (up from 855 baseline — 46 new tests). All production code AOT/trimming compatible.

**Bucket Configuration Operations (all 13, wired end-to-end):**
- Bucket Tagging (`GET`/`PUT`/`DELETE ?tagging`)
- Bucket Logging (`GET`/`PUT ?logging`)
- Bucket Website (`GET`/`PUT`/`DELETE ?website`)
- Bucket Request Payment (`GET`/`PUT ?requestPayment`)
- Bucket Accelerate (`GET`/`PUT ?accelerate`)
- Bucket Lifecycle (`GET`/`PUT`/`DELETE ?lifecycle`)
- Bucket Replication (`GET`/`PUT`/`DELETE ?replication`)
- Bucket Notifications (`GET`/`PUT ?notification`)
- Object Lock Configuration (`GET`/`PUT ?object-lock`)
- Bucket Analytics (`GET`/`PUT`/`DELETE ?analytics` with `&id=`)
- Bucket Metrics (`GET`/`PUT`/`DELETE ?metrics` with `&id=`)
- Bucket Inventory (`GET`/`PUT`/`DELETE ?inventory` with `&id=`)
- Bucket Intelligent-Tiering (`GET`/`PUT`/`DELETE ?intelligent-tiering` with `&id=`)

**Object-Level Operations:**
- `PutObjectRetention` (`PUT ?retention`) — full S3 provider, disk unsupported
- `PutObjectLegalHold` (`PUT ?legal-hold`) — full S3 provider, disk unsupported
- `SelectObjectContent` (`POST ?select`) — S3 passthrough, disk unsupported
- `RestoreObject` (`POST ?restore`) — S3 passthrough, disk unsupported
- `PostObject` (browser form upload with policy validation)
- `GetObjectAttributes` (`GET ?attributes`) — full stack: Abstractions request/response types, `StorageOperationType.GetObjectAttributes`, `IStorageBackend`/`IStorageService` contracts, Core orchestration + auth, Disk provider (delegates to HeadObject and filters by requested attributes), S3 provider (AWS SDK `GetObjectAttributesAsync`), Protocol XML writer (`GetObjectAttributesResponse`), HTTP endpoint with `x-amz-object-attributes` header parsing, S3ActionMapping + IsObjectLevelOperation, test fakes

**Protocol/HTTP Improvements:**
- Storage Class Headers (`x-amz-storage-class` on PUT/COPY/InitiateMultipart)
- Conditional Writes (`If-Match`/`If-None-Match` on PutObject)
- Multi-range Requests (`multipart/byteranges` responses)
- `x-amz-checksum-mode: ENABLED` support
- CRC64NVME checksum algorithm recognition
- SSE Remaining (`bucket-key-enabled`, response headers on all operations)

**Hardening & Conformance:**
- XML Namespace Parity (`xmlns` on all S3 XML responses, `x-amz-request-id`/`x-amz-id-2` headers)
- `encoding-type=url` Consistency (all list responses)
- `BucketNotEmpty` for Versioned Buckets (counts versions + delete markers)
- ACL Expansion (`public-read-write`, `authenticated-read`, full XML grants)
- Bucket Policy Expansion (`Deny`, conditions, CIDR matching, wildcards)
- Multipart Replication support
- `GetObjectAttributes` (`GET ?attributes`) — full stack: Abstractions request/response types, `StorageOperationType.GetObjectAttributes`, `IStorageBackend`/`IStorageService` contracts, Core orchestration + auth, Disk provider (delegates to HeadObject and filters by requested attributes), S3 provider (AWS SDK `GetObjectAttributesAsync`), Protocol XML writer (`GetObjectAttributesResponse`), HTTP endpoint with `x-amz-object-attributes` header parsing, S3ActionMapping + IsObjectLevelOperation, test fakes

**Infrastructure:**
- SigV4a Full Support (ECDSA P-256 header/presigned authentication, streaming trailer verification)
- HeadBucket `x-amz-bucket-region` response header (best-effort from GetBucketLocation)
- SSE `BucketKeyEnabled` fully supported on PutBucketDefaultEncryption (previously rejected with 501)
- List*Configurations for ID-based configs: `ListBucketAnalyticsConfigurations`, `ListBucketMetricsConfigurations`, `ListBucketInventoryConfigurations`, `ListBucketIntelligentTieringConfigurations`
- Bucket Policy `Deny` effect accepted at endpoint layer (full evaluation in authorization)

**Remaining blocked:** None in the current scope. SigV4a is now fully supported for header-based, presigned-query, and streaming trailer verification flows.

### Developer Experience Enhancements — ✅ Complete

1. **Root README.md** — Comprehensive getting-started guide with quickstart code, package table, configuration reference, provider examples, health checks, observability, and build/test commands.
2. **XML Documentation** — Full `<summary>`, `<param>`, `<returns>` coverage on all public APIs across all 9 library packages (~200+ public types). IntelliSense now works for all consumer-facing types including `IStorageService`, `IStorageBackend`, `AddIntegratedS3()`, `MapIntegratedS3Endpoints()`, `IntegratedS3Options`, and all provider/client/testing APIs.
3. **Startup Validation** — `IntegratedS3OptionsValidator` (`IValidateOptions`) validates auth credentials, route prefix, clock skew, and presign expiry. `MapIntegratedS3Endpoints()` validates at least one `IStorageBackend` is registered, producing actionable error messages instead of cryptic DI failures.
4. **NuGet Packaging Metadata** — `Directory.Build.props` now includes `GenerateDocumentationFile`, `PackageLicenseExpression` (MIT), `PackageTags`, `RepositoryUrl`, `RepositoryType`. XML doc files ship in NuGet packages.

Build: 0 errors, 0 warnings, 0 AOT warnings. Tests: **944 passing**.

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
  - SSE-C (customer-provided key) support: `ObjectCustomerEncryptionSettings`/`ObjectCustomerEncryptionInfo` models wired through the full S3 provider layer — put, get, head, copy (source + destination), initiate-multipart, upload-part, and upload-part-copy operations apply and surface SSE-C headers via `S3ServerSideEncryptionMapper`; bucket default encryption auto-resolve is bypassed when customer encryption is specified
  - `S3ErrorTranslator` extended with native multipart/checksum error mappings (`NoSuchUpload`, `InvalidPart`, `InvalidPartOrder`, `EntityTooSmall`, `BadDigest`) plus per-error-code object key context
  - `S3StorageCapabilities` updated: `ObjectCrud`, `ObjectMetadata`, `ListObjects`, `Pagination`, `RangeRequests`, `ConditionalRequests`, `ObjectTags`, `Versioning`, `Cors`, `CopyOperations`, `MultipartUploads`, `PresignedUrls`, `Checksums`, and `ServerSideEncryption` are `Native`
- `IS3StorageClient` and `AwsS3StorageClient` extended with native copy, multipart, direct/delegated presign, checksum, versioning, bucket CORS, and initial object-level SSE operations using the SDK v4 API surface
  - `AddS3Storage(...)` now also registers an S3-backed `IStorageObjectLocationResolver` so delegated read presigns can surface provider URLs with safe proxy fallback
  - Internal model records added/extended: `S3ObjectEntry`, `S3ObjectListPage`, `S3ObjectVersionListPage`, `S3MultipartUploadListPage`, `S3GetObjectResult`, `S3DeleteObjectResult`, `S3VersioningEntry`, `S3CorsConfigurationEntry`
- `FakeS3Client` in tests extended; unit tests now cover capabilities, copy, multipart lifecycle/listing, checksum mapping, direct/delegated presign flows, paged listing, CRUD, tags, versioning, 304/412 flows, and get-object disposal behavior
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
- computed SHA-256, SHA1, CRC32, and CRC32C object checksum persistence/exposure for the current write, copy, and multipart paths, including per-part checksum echoes and composite multipart completion responses
- request-side SHA-256, SHA1, CRC32, and CRC32C checksum validation for direct put-object writes plus multipart SHA-256, SHA1, and CRC32C part validation on the supported surface
  - streaming reads/writes
  - basic path traversal protection
- `IntegratedS3.AspNetCore` integration with:
  - `AddIntegratedS3(...)`
  - `MapIntegratedS3Endpoints(...)`
  - combined configuration-binding + inline-configuration overloads plus `AddIntegratedS3Provider(...)` helpers for named/manual configured-provider metadata registration and `AddIntegratedS3Backend(...)` helpers for custom backend DI activation
  - feature-group endpoint toggles for service, bucket, object, multipart, and admin surfaces
  - `IntegratedS3EndpointOptions` can now bind from `IntegratedS3:Endpoints`, including common `RequireAuthorization` / named-policy / `AllowAnonymous` route-group conventions, and map-time overrides start from those configured defaults so hosts/tests can keep endpoint toggles aligned without manual re-wiring
  - reusable endpoint mapping now also has a configuration-only path backed by `IntegratedS3EndpointConfigurationOptions`, so the current AOT-enabled `WebUi` host can keep reusable composition helpers without surfacing Minimal API publish warnings while callback-based mapping remains available for tests and advanced consumers
  - first-class whole-route, per-feature, and explicit shared root/compatibility route-group configuration on endpoint mapping, including authorization/policy wiring
  - `IntegratedS3EndpointFeature` plus `SetFeatureRouteGroupConfiguration(...)` / `GetFeatureRouteGroupConfiguration(...)` as the stable per-feature callback pattern for future endpoint surfaces, with the current `Configure*RouteGroup` members retained as convenience wrappers for today's built-in features
  - backend-derived service/provider descriptors and capabilities
  - provider descriptors and service documents that now surface provider mode plus object-location access shape separately from capability support and support-state ownership
  - capability reporting on the HTTP metadata surface remains backend/Core-derived runtime metadata, not a full end-to-end HTTP-conformance report
  - `HttpContext.User` flow into Core authorization request context
  - provider-aware presign issuance via `POST /integrated-s3/presign/object`, with configurable signing-credential resolution and public-base-url fallback for proxy grants plus backend-direct/object-location seams for direct or delegated access
  - provider-agnostic admin diagnostics via `GET /integrated-s3/admin/diagnostics`, combining provider health, replica lag, and grouped repair diagnostics, while preserving read-only replica-repair backlog visibility via `GET /integrated-s3/admin/repairs`
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
  - S3-compatible current `x-amz-checksum-*` request validation for direct put-object flows, single-algorithm `x-amz-checksum-algorithm` echoes on successful checksum-declared put-object responses, plus `UploadPart` declared-checksum enforcement on the supported multipart surface including trailer-backed checksum values and `UploadPartCopy` checksum algorithm/value request parity for copy-part flows
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
  - Core multipart behavior including write-through replication support
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
  - remaining work in this area: deeper versioning/tagging edge-case parity, broader provider/conformance coverage for the supported checksum slice, and advanced subresource combinations

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
  - reusable provider contract/conformance harness for custom `IStorageBackend` implementations
  - shared xUnit contract tests, in-memory support-state stores, and checksum helpers

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
| range requests | implemented | native | implemented | yes | single-range and multi-range (`multipart/byteranges`) byte requests supported |
| conditional requests | implemented | native | implemented | yes | current parity covers ETag and HTTP-date validators for `GET` / `HEAD` plus `If-Match` / `If-None-Match` conditional writes on `PutObject` |
| copy operations | implemented | native | implemented | yes | `PUT` plus `x-amz-copy-source` returns S3-style XML copy results, the native S3 provider now maps copy requests/responses through the AWS SDK with provider-agnostic `ObjectInfo` checksum surfacing, and focused provider coverage now locks in same-key `REPLACE` self-copy modtime updates (`x-amz-meta-mtime`) without changing object content on the current supported surface |
| batch delete | implemented at HTTP layer | backend composed via per-object delete | implemented | yes | S3-compatible `POST ?delete` is supported for the current bucket-level route |
| XML-compatible S3 errors | implemented | n/a | implemented | yes | storage endpoint failures are translated to XML error documents |
| path-style addressing | implemented | n/a | implemented | yes | current baseline routing model |
| virtual-hosted-style addressing | implemented | n/a | implemented | yes | optional and configuration-gated; AWS SDK compatibility coverage exists |
| SigV4 header authentication | implemented | n/a | implemented | yes | authorization-header validation is covered in HTTP and AWS SDK tests, including current space-containing key paths that arrive pre-escaped on the supported surface; unsupported SigV4a (`AWS4-ECDSA-P256-SHA256`) headers are now rejected explicitly with S3-style XML errors instead of falling through as generic auth failures |
| SigV4 presigned-query validation | implemented | n/a | implemented | yes | request validation exists separately from the first-party presign surface, which can now return proxy, backend-direct, or delegated-read grants depending on provider capabilities; unsupported SigV4a (`AWS4-ECDSA-P256-SHA256`) presigned requests are now rejected explicitly with S3-style XML errors |
| first-party presign generation | implemented | n/a | implemented | yes | provider-agnostic Core contracts plus `POST /integrated-s3/presign/object` and `IntegratedS3.Client` now cover proxy grants, backend-direct overrides when a provider implements them, and delegated-read S3 provider URLs with safe proxy fallback; native S3 `PUT` still falls back to proxy |
| multipart upload lifecycle | implemented | emulated | implemented | yes | initiate/upload-part/complete/abort plus bucket-level `GET ?uploads` listing/discovery are implemented on the current disk/Core/HTTP surface, and the native S3 provider now implements the same provider-facing lifecycle/listing natively through the AWS SDK; multipart operations now participate in write-through replication alongside bucket and object operations |
| object tags | partially implemented | emulated | implemented | yes | direct contracts and S3-compatible `GET` / `PUT` / `DELETE ?tagging` now cover current and archived object versions on the currently supported surface, including version-id-aware delete-tagging parity; current and archived tag persistence can now default to platform-managed object state when available, while broader tagging edge cases are still pending |
| versioning | implemented for the current vertical slice | emulated with platform-managed historical catalogs | implemented for the current supported surface | yes | current object versions receive persisted opaque version IDs, overwrites archive historical versions, `versionId` read/head/delete/tagging/copy-source flows can target archived versions, `GET` / `PUT ?versioning` round-trip through Core/disk/HTTP, `GET ?versions` now lists historical versions and delete markers, version-enabled deletes now create S3-compatible delete markers, delete-marker reads now cover current-object `NoSuchKey` plus explicit-version `MethodNotAllowed` fidelity with the expected delete-marker/version/`Last-Modified` headers, and historical plus current auxiliary object state can be platform-managed when an object-state store is registered |
| bucket location discovery | implemented for the current slice | emulated default constraint | implemented | yes | `GET ?location` now returns S3-compatible `LocationConstraint` XML; disk reports the default empty constraint (`us-east-1` semantics), while the native S3 provider maps `GetBucketLocation` through the AWS SDK and the HTTP/AWS SDK compatibility surface is covered |
| checksums | implemented | emulated | implemented | yes | disk now computes and persists checksums for the current put/copy/multipart flows; CRC64NVME recognized as valid algorithm with client-provided pass-through; `x-amz-checksum-mode: ENABLED` now gates checksum header emission on GET/HEAD paths; the native S3 provider forwards checksum algorithms plus explicit checksum headers and echoes checksum data on native put/copy/multipart paths, and the current HTTP surface accepts `CopyObject` and `UploadPartCopy` checksum request headers, emits the supported checksum fields plus single-algorithm `x-amz-checksum-algorithm` on successful responses, and enforces direct-write-style declared-checksum consistency plus trailer-backed `aws-chunked` validation; broader checksum-override, multipart-finalization/read-mode, and broader AWS SDK signed-trailer algorithm coverage beyond SHA1 still needs CRT-enabled SDK coverage |
| ACL / policy behavior | implemented for the current expanded slice | emulated | implemented for the current expanded slice | yes | the platform now layers a comprehensive ACL/policy compatibility model over `ClaimsPrincipal` authorization: bucket/object canned ACLs (`private`, `public-read`, `public-read-write`, `authenticated-read`, `bucket-owner-full-control` normalization), full XML `AccessControlPolicy` grant rendering for `GET ?acl`, `GET` / `PUT ?acl`, `GET` / `PUT` / `DELETE ?policy`, anonymous `ListObjects` / `HeadBucket` / `GetObject` / `HeadObject` fallback for stored public grants, anonymous `PutObject` for `PublicReadWrite`; bucket policy now supports `Deny` effect, condition operators, CIDR source-IP matching, and wildcard resource patterns; `StorageAclPermission.Write` is a first-class enum member; explicit `NotImplemented` errors for unsupported grants, version-specific ACLs, and multipart-initiation ACL headers |
| CORS | implemented for the current slice | emulated | implemented | yes | bucket-level CORS contracts/configuration now round-trip through Core; disk reports `Cors = Emulated`, the native S3 provider reports `Cors = Native`, and ASP.NET now serves bucket-aware preflight plus actual-response headers without global middleware, including `Vary` handling for both allowed and rejected browser-facing CORS evaluations, S3-aligned `Access-Control-Allow-Credentials: true` emission for successful non-wildcard evaluations, and literal `*` origin responses that intentionally omit the credentials header for literal wildcard rules |
| object lock / retention / legal hold | implemented for read and write paths | S3 native write-path; disk unsupported | implemented for read and write paths | yes | `GetObjectRetention`, `GetObjectLegalHold` read paths now wired end-to-end; `PutObjectRetention` (`PUT ?retention`) and `PutObjectLegalHold` (`PUT ?legal-hold`) write paths now implemented for the S3 provider; disk provider continues to fail object-lock operations as unsupported; bucket-level `GET`/`PUT ?object-lock` configuration now wired as a bucket configuration subresource |
| server-side encryption variants | implemented for managed, SSE-C, and bucket-key-enabled slices | unsupported with explicit guardrails | implemented for managed, SSE-C, and bucket-key-enabled slices | yes | provider-agnostic object-level SSE request/response models now cover `AES256`, `aws:kms`, and `aws:kms:dsse` plus object-metadata surfacing; `ObjectServerSideEncryptionInfo.BucketKeyEnabled` is now populated from AWS SDK responses on GET/HEAD/PUT/COPY/InitiateMultipart/CompleteMultipart and emitted via `x-amz-server-side-encryption-bucket-key-enabled`; SSE-C (customer-provided key) is wired through the full S3 provider layer for put/get/head/copy/multipart operations including source and destination keys; the ASP.NET HTTP endpoint layer now parses SSE-C request headers (`x-amz-server-side-encryption-customer-algorithm`, `x-amz-server-side-encryption-customer-key`, `x-amz-server-side-encryption-customer-key-MD5`) and copy-source variants for all applicable operations (put, get, head, copy, initiate-multipart, upload-part, upload-part-copy), emits SSE-C response headers (`algorithm` + `key-MD5`) on put/get/head/copy/initiate-multipart/upload-part/upload-part-copy response paths, validates SSE-C algorithm (only AES256 accepted), rejects incomplete SSE-C headers (algorithm without key/MD5), enforces mutual exclusivity between SSE-C and managed SSE headers, and no longer rejects SSE-C headers with `501 NotImplemented`; `StorageCapabilities` now expose provider-agnostic SSE variant details for request style/operations; the disk provider now explicitly rejects both managed SSE and SSE-C customer-provided encryption requests with `UnsupportedCapability` errors on put/get/head/copy/initiate-multipart/upload-part operations; provider-level test coverage includes disk SSE-C rejection tests, S3StorageService SSE-C pass-through and bucket-default-skip tests, and gated real-S3 SSE-C conformance tests; broader provider-expansion work remains pending |
| bucket configuration subresources | implemented (13 families) | emulated (metadata persistence) | implemented | yes | Tagging, Logging, Website, RequestPayment, Accelerate, Lifecycle, Replication, Notifications, ObjectLockConfiguration, Analytics, Metrics, Inventory, and IntelligentTiering are all wired end-to-end through Abstractions → Protocol XML → Core orchestration with replication → Auth decorator → Disk provider (metadata persistence) → S3 provider (AWS SDK delegation) → HTTP endpoints; id-based families (Analytics, Metrics, Inventory, IntelligentTiering) support `&id=` scoping |
| conditional writes | implemented | native | implemented | yes | `If-Match` and `If-None-Match` headers on `PutObject` for optimistic concurrency and overwrite prevention; disk evaluates conditions before write; S3 provider passes through to AWS SDK |
| storage class headers | implemented | disk ignores (no storage class concept) | implemented | yes | `x-amz-storage-class` parsed on PUT, COPY, and InitiateMultipartUpload; S3 provider maps to `S3StorageClass`; disk provider accepts and ignores |
| multi-range requests | implemented | native | implemented | yes | `Range: bytes=0-100,200-300` multi-range requests produce proper `multipart/byteranges` MIME responses; each range fetched individually from the storage backend |
| checksum mode | implemented | emulated | implemented | yes | `x-amz-checksum-mode: ENABLED` respected on GET/HEAD; checksum value headers conditionally emitted only when enabled on read paths; write responses continue to emit checksums unconditionally |
| CRC64NVME | implemented | pass-through (no server-side validation) | implemented | yes | recognized as valid checksum algorithm throughout disk provider; client-provided values accepted as pass-through since no .NET implementation is available; S3 provider already has full support via AWS SDK |
| PostObject (browser form upload) | implemented | native | implemented | yes | bucket-level `POST` with `multipart/form-data` supports browser-based uploads; key/file/Content-Type/x-amz-acl/success_action_status/success_action_redirect/x-amz-meta-* fields plus optional base64 JSON policy with basic condition validation; `${filename}` substitution supported |
| SelectObjectContent | implemented (S3 only) | unsupported | implemented | yes | `POST ?select` S3 passthrough for SQL-based object content selection; disk returns explicit unsupported error |
| RestoreObject | implemented (S3 only) | unsupported | implemented | yes | `POST ?restore` S3 passthrough for Glacier/archive restoration; disk returns explicit unsupported error |
| multipart replication | implemented | n/a | implemented | yes | multipart operations now participate in write-through replication alongside bucket and object operations |
| SigV4a authentication | implemented | n/a | implemented | yes | `AWS4-ECDSA-P256-SHA256` requests are now fully supported for header-based and presigned-query authentication with ECDSA P-256 signing/verification, region-set validation (wildcard and specific), and streaming `aws-chunked` trailer signature verification; credential scope uses the SigV4a 3-part format (`dateStamp/service/aws4_request`); `integrateds3:auth-type` claim is `sigv4a` for SigV4a-authenticated principals |

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
- startup validation is implemented: `MapIntegratedS3Endpoints()` validates that at least one `IStorageBackend` is registered and throws an actionable `InvalidOperationException` if not
- `IValidateOptions<IntegratedS3Options>` validates SigV4-credential consistency, route-prefix format, and positive clock-skew/expiry values

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
- coarse endpoint-level authorization policies are optional and now support first-class route-group configuration through both map-time callbacks and configuration-bound endpoint authorization conventions

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
- direct checksum validation now maps mismatches to S3-style `BadDigest` on the current supported write surface, and the unsigned trailer-backed `aws-chunked` SHA-256, SHA1, and CRC32C slices now have matching success-path plus mismatch parity for both `PUT Object` and multipart `UploadPart` across the raw HTTP and SigV4 header-auth paths

### Current protocol fidelity gaps worth prioritizing

The current HTTP surface is real and useful, but it still has some clearly bounded fidelity gaps that should be treated as deliberate backlog rather than invisible debt:

- multipart upload support now covers initiate/upload-part/complete/abort plus bucket-level `GET ?uploads` listing/discovery for the current disk/Core/HTTP surface, including `encoding-type=url` plus owner/initiator XML parity; deeper client-compat edge cases and richer multipart subresource hardening are still pending
- object tagging now covers current and archived object versions on the currently supported surface, including S3-compatible delete-tagging; broader S3 tagging edge-case behavior is still pending
- only the currently supported bucket subresources are implemented; unsupported S3 bucket/object subresources intentionally return `NotImplemented`
- S3-compatible bucket listing now covers legacy `ListObjects` (V1), `ListObjectsV2` including `fetch-owner` and `encoding-type=url`, and the current `?versions` subresource, while additional subresource combinations are still pending
- conditional behavior is solid for the current `GET` / `HEAD` paths, but broader S3 precedence and edge-case parity still need hardening
- SigV4 validation and `aws-chunked` decoding work for the implemented routing surface, but canonical-request edge cases and parity hardening should continue before claiming wider compatibility
- multipart upload support now covers initiate/upload-part/complete/abort plus bucket-level `GET ?uploads` listing/discovery for the current disk/Core/HTTP surface, but broader parity such as `encoding-type=url`, deeper client-compat edge cases, and richer multipart subresource hardening is still pending
- object tagging now covers current and archived object versions on the currently supported surface, including S3-compatible delete-tagging, `InvalidTag` validation for the documented count/length/duplicate/reserved-prefix and tag-character rules, strict UTF-8 `x-amz-tagging` decoding that preserves literal `+`, and `x-amz-tagging-count` on successful object `GET` / `HEAD`
- multipart upload support now covers initiate/upload-part/complete/abort plus bucket-level `GET ?uploads` listing/discovery for the current disk/Core/HTTP surface, including `encoding-type=url` response encoding plus explicit multipart marker/limit validation on the S3-compatible route; deeper client-compat edge cases and richer multipart subresource hardening are still pending
- object tagging now covers current and archived object versions on the currently supported surface, including S3-compatible delete-tagging; broader S3 tagging edge-case behavior is still pending
- only the currently supported bucket subresources are implemented; unsupported S3 bucket/object subresources intentionally return `NotImplemented`
- S3-compatible bucket listing now covers both `list-type=2` and the current `?versions` subresource, while additional subresource combinations are still pending
- conditional behavior is solid for the current `GET` / `HEAD` paths and now covers copy-source precedence failures, but remaining delete-marker/versioning edge-case parity still needs hardening
- direct write validation now rejects ambiguous multi-checksum `x-amz-checksum-*` request headers, while deeper checksum override and trailing-checksum parity are still pending
- SigV4 validation and `aws-chunked` decoding work for the implemented routing surface, and raw-query canonicalization now preserves literal `+` and duplicate query parameter edges; broader virtual-hosted-style and remaining parity hardening should continue before claiming wider compatibility
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
- retention, remaining checksum/header edge cases, broader S3 tag-character validation, and the remaining advanced versioning/tagging fidelity work are still pending

### 2. Native S3 provider second

Build `IntegratedS3.Provider.S3` next:

- use native S3 APIs
- preserve streaming semantics
- translate provider errors to canonical storage errors
- support presign generation
- preserve metadata/version/checksum semantics as much as possible

Status: **native copy/multipart/presign/checksum provider slice is now implemented**

- `S3StorageService` now implements bucket CRUD, object CRUD, object/version listing, object tagging, bucket versioning, bucket CORS, native copy, and the multipart upload lifecycle/listing surface through the AWS SDK
- `IS3StorageClient` / `AwsS3StorageClient` now bridge the AWS SDK v4 surface for range, conditional, tagging, version-aware object flows, native copy, multipart upload listing/lifecycle, presigned `GET` / `PUT` URL generation, and checksum request/response mapping
- canonical error translation, DI registration, options, and S3-backed delegated-read resolver registration are in place
- `S3StorageCapabilities` now declare bucket/object/list/range/conditional/tag/versioning/copy/multipart/presign/checksum/CORS support as `Native`
- backend-direct native S3 presign is now enabled for explicit `Direct` `GET` / `PUT` requests alongside the existing delegated-read resolver path, preserving proxy fallback when no compatible direct or delegated grant is available
- custom `ServiceUrl` endpoints now use explicit signing-region + required-only flexible-checksum defaults and preserve the configured endpoint scheme for delegated `GET` presigns, reducing common MinIO/LocalStack mismatches without changing provider-agnostic contracts
- `IntegratedS3.Tests` now includes focused custom-endpoint config/presign coverage plus an opt-in local S3-compatible conformance test that exercises copy, multipart, checksum, AES256 SSE, versioning, and delegated-read flows when `INTEGRATEDS3_S3COMPAT_*` environment variables are provided
- next steps center on continuously running that local S3-compatible harness against concrete endpoints and broadening parity/conformance hardening now that explicit-direct native S3 `GET` / `PUT` presigns and delegated-read resolver flows both exist

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
- `IntegratedS3.Client` now hardens the higher-level transfer surface for larger-object and checksum-aware flows: checksum-aware `PresignPutObjectAsync` / `UploadStreamAsync` / `UploadFileAsync` overloads compute supported full-object checksums (`SHA256`, `SHA1`, `CRC32`, `CRC32C`) in a streaming pre-pass, forward them through first-party presign grants as signed headers, and validate echoed upload checksum headers when present; file uploads now open with `SequentialScan`, and download helpers validate full-object checksum headers on streamed and resume-aware downloads, only append resumed `206 Partial Content` responses when `Content-Range.From` matches the local file length, rewrite from the start when resumed range metadata is missing or misaligned, and intentionally skip multipart/composite checksum values that cannot yet be recomputed from the plain body alone; covered by `IntegratedS3ClientPresignExtensionsTests`, `IntegratedS3HttpPresignStrategyTests`, and `IntegratedS3ClientTransferTests`

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

These are optional host integrations around Core orchestration seams, not a commitment that reconciliation/repair must ship as a built-in always-on background-service architecture. Track F now exposes provider-agnostic divergence/backlog/dispatch seams so consumers can host replay/repair via `IHostedService`, external schedulers, or other operational tooling without being forced into a mandatory built-in hosted service. `IntegratedS3.AspNetCore` now includes `AddIntegratedS3MaintenanceJob(...)`, `IIntegratedS3MaintenanceJob`, and `IntegratedS3MaintenanceJobNames` so hosts can schedule mirror replay, orphan detection, checksum verification, multipart cleanup, index compaction, and expired-artifact cleanup with the same opt-in model; reference guidance lives in `docs\host-maintenance-jobs.md`.

- mirror replay
- orphan detection
- checksum verification
- provider health probes
- multipart cleanup
- index compaction
- expired temporary artifact cleanup

### Observability (Logging, Tracing, Metrics) — ✅ Complete

**Architecture:**
- Single `ActivitySource` ("IntegratedS3") and single `Meter` ("IntegratedS3") shared across all layers via `IntegratedS3Observability`
- Tag constants centralized in `IntegratedS3Observability.Tags`
- Metric naming convention: `integrateds3.{layer}.{instrument}`

**Instrumented Layers:**

1. **Core Orchestration** (pre-existing) — 8 metrics, 4 activity types, rich structured logging
2. **Disk Provider** — `DiskStorageTelemetry` helper with `integrateds3.disk.operation.duration` histogram + `integrateds3.disk.operation.errors` counter + Activity spans on 11 key methods + ILogger structured logging
3. **S3 Provider** — `S3StorageTelemetry` helper with `integrateds3.s3.operation.duration` histogram + `integrateds3.s3.operation.errors` counter + Activity spans on 12 AWS SDK methods + ILogger structured logging
4. **Protocol** — `ProtocolTelemetry` error-path counters for XML parse errors + signature verification errors (no happy-path instrumentation to avoid hot-path allocations)
5. **Endpoints** — HTTP request counter (`integrateds3.http.request.count`) + duration histogram (`integrateds3.http.request.duration`) + structured request logging at dispatch level
6. **Maintenance** — Exception handling + Activity tracing + job duration/failure metrics on scheduled maintenance

**Additional signals:**
- Correlation IDs via canonical `x-integrateds3-correlation-id` header plus request-context/log/activity propagation
- Provider, primary-provider, and replica-backend tags on operation, repair, backlog, and backend-health telemetry
- Auth-failure visibility via warning logs plus `integrateds3.http.authentication.failures` and `integrateds3.storage.authorization.failures`
- Mirror-lag and reconciliation-backlog visibility via `integrateds3.replication.backlog.size`, `integrateds3.replication.backlog.oldest_age`, and the admin repair-backlog endpoint
- Backend health visibility via `integrateds3.backend.health.status`

**OpenTelemetry Integration:**
- WebUi host wires `AddOpenTelemetry()` with tracing + metrics + optional OTLP export
- Configured via `OpenTelemetry:OtlpEndpoint` in appsettings.json (disabled by default)
- AOT-compatible: 0 IL2026/IL3050 warnings
- Dedicated health endpoints remain host-owned for now; see `docs/observability.md`

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
- AWS SDK compatibility coverage now includes path-style plus virtual-hosted-style CRUD/list, `aws-chunked` PUT uploads, host-style and virtual-hosted-style presigned URL round-trips, multipart upload, and copy/conditional flows
- focused fault-injection/orchestration coverage now exists for async replica recording/dispatch, unhealthy-replica preflight, outstanding-repair read behavior, partial-write backlog semantics, and failed-repair visibility in Core orchestration tests
- S3 conformance, broader multi-provider fault-injection coverage, trimming/AOT publish verification, and benchmark automation are still pending, while the Minimal API, MVC/Razor, and Blazor WebAssembly sample consumers now ship under `src\IntegratedS3`

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
- SigV4 authorization-header and presigned-query request validation are implemented on the ASP.NET surface for compatibility scenarios, including strict positive `X-Amz-Expires` handling that matches first-party presign generation rules
- first-party presign generation is now exposed as an application-facing capability through provider-agnostic Core contracts (`StoragePresignRequest`, `StoragePresignedRequest`, `IStoragePresignService`, `IStoragePresignStrategy`) and an ASP.NET `POST /integrated-s3/presign/object` endpoint for object `GET` / `PUT` flows; issuance can stay proxy-mode or return backend-direct / delegated-read grants when the current provider seams can satisfy the request
- `IntegratedS3.Client` now ships the first meaningful surface: `IIntegratedS3Client` / `IntegratedS3Client`, presign convenience helpers, a helper that turns returned grants into `HttpRequestMessage` instances, and typed streaming/file transfer helpers
- native S3 provider — **copy/multipart/presign/checksum slice is now implemented on top of the object/list/versioning foundation**:
  - `S3StorageService` implements `ListBucketsAsync`, `CreateBucketAsync`, `HeadBucketAsync`, and `DeleteBucketAsync` via the native AWS SDK
  - `S3StorageService` now also implements `ListObjectsAsync`, `ListObjectVersionsAsync`, `HeadObjectAsync`, `GetObjectAsync`, `PutObjectAsync`, `DeleteObjectAsync`, `CopyObjectAsync`, the multipart upload lifecycle/listing surface, `GetObjectTagsAsync`, `PutObjectTagsAsync`, `DeleteObjectTagsAsync`, `GetBucketVersioningAsync`, `PutBucketVersioningAsync`, `GetBucketCorsAsync`, `PutBucketCorsAsync`, and `DeleteBucketCorsAsync`
  - `IS3StorageClient` / `AwsS3StorageClient` now bridge the AWS SDK v4 surface for object CRUD, paged listings, conditional GETs, range requests, tagging, versioning, bucket CORS, native copy/multipart flows, presigned `GET` / `PUT` URL generation, and checksum request/response mapping
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
- multipart orchestration now participates in write-through replication alongside bucket and object operations
- broader protocol fidelity such as additional bucket/object subresources, remaining checksum/header edge cases, and deeper edge-case compatibility is still pending

### M5 — Mirroring, backup, and reconciliation

- multiple backends
- write routing
- health-aware reads
- background reconciliation

Status: **in progress / partially complete**

- multiple backends can now be registered and surfaced through descriptors
- write routing now supports `PrimaryOnly`, strict `WriteThroughAll`, and `WriteToPrimaryAsyncReplicas`
- mirrored writes currently cover bucket create/delete/versioning, object put/delete, copy-object, current-version object-tag mutation, and multipart upload operations, with async replica work recorded through provider-agnostic repair backlog/dispatcher seams
- read routing now supports `PrimaryOnly`, `PreferPrimary`, and `PreferHealthyReplica`, and replica-preferred reads now avoid outstanding-repair replicas by default unless explicitly configured otherwise
- first health-aware provider selection is now implemented through an overrideable backend-health evaluator used by Core read orchestration
- Core now also tracks dynamic backend-health snapshots, marks failover-triggering read failures as temporarily unhealthy, can optionally probe backends to restore healthy read preference without replacing existing evaluator overrides, and preflights required replicas before strict write-through primary mutation when they are unhealthy or not current
- replica-write divergence is now surfaced through the repair backlog for both async replica dispatch and post-primary partial synchronous failures; multipart operations now participate in replication
- background reconciliation/repair execution is still scaffolded and optional-host-integrated rather than a required Core background-service model

### M6 — Versioning, tags, and checksums

- version chains
- metadata fidelity
- checksum persistence and validation
- tag support

Status: **in progress / partially complete**

- object tagging is implemented across abstractions, Core orchestration, HTTP `?tagging` routes, and automated tests for current and archived versions on the supported surface, including explicit delete-tagging flows, version-id-aware response parity, and platform-managed tag persistence by default when an object-state store is registered
- persisted checksum metadata is now implemented for the current disk/Core/HTTP object lifecycle paths, including platform-managed object-state composition, SHA-256/SHA1/CRC32/CRC32C response and header exposure, direct put-object request-side SHA-256, SHA1, CRC32, and CRC32C validation, and checksum-enabled multipart SHA-256, SHA1, and CRC32C initiate/upload-part/complete parity with completion checksum metadata for the supported algorithms
- trailer-backed checksum hardening on the current HTTP/SigV4 surface now has raw HTTP plus header-auth SigV4 success and `BadDigest` mismatch parity for unsigned `STREAMING-UNSIGNED-PAYLOAD-TRAILER` SHA256, SHA1, CRC32, and CRC32C `PUT Object` / `UploadPart` flows and signed `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER` SHA1, CRC32, and CRC32C `PUT Object` / `UploadPart` flows, plus focused official AWS .NET SDK coverage proving signed-trailer SHA1 `PutObject` / `UploadPart` requests work against the current HTTP harness without relying on the HTTPS-only unsigned-payload path; broader AWS SDK signed-trailer algorithm coverage beyond SHA1 remains pending because trailer generation for algorithms such as CRC32C currently throws unless `AWSSDK.Extensions.CrtIntegration` is referenced, and multipart-finalization/read-side checksum-mode work is still open
- AWS SDK and S3-compatible HTTP coverage now round-trip the current supported multipart checksum slice and copy-object checksum XML fields, including per-part checksum echoes, composite `ChecksumType` / `x-amz-checksum-type` exposure where applicable, supported copy-response checksum field parity, SHA1 plus CRC32C checksum field/header/XML coverage on the current supported surface, explicit request-shape assertions for signed-trailer SHA1 `PutObject` / `UploadPart` interoperability, and the existing note that official SDK unsigned-trailer coverage remains blocked by the HTTPS-only `DisablePayloadSigning=true` requirement on the current harness
- version-id scaffolding is now in place for current object versions across disk/Core/catalog/HTTP, bucket-level versioning controls round-trip through Core/disk/HTTP, the disk/HTTP surface supports archived-version access, tagging, delete-tagging, deletion, copy-source reads, list-object-versions, and S3-compatible delete markers, current delete-marker `GET` / `HEAD` requests now return `NoSuchKey` plus `x-amz-delete-marker` / `x-amz-version-id`, and explicit delete-marker `versionId` reads now return `MethodNotAllowed` with delete-marker/version/`Last-Modified` fidelity; remaining checksum/header edge cases and advanced versioning/tagging edge cases are still pending

### M7 — Advanced S3-compatible features

- ACL/policy mapping
- CORS
- object lock / retention / legal hold
- encryption-related support

Status: **substantially complete**

- bucket-level CORS is now implemented across abstractions, Core orchestration, the disk provider, the native S3 provider, and the ASP.NET S3-compatible surface
- ASP.NET now supports bucket-aware preflight `OPTIONS` handling and actual-response `Access-Control-*` headers without relying on global middleware, including S3-aligned successful-credential headers for non-wildcard origins and literal `*` origin responses that intentionally omit `Access-Control-Allow-Credentials` for literal wildcard rules
- ACL/policy compatibility is now expanded: bucket/object canned ACLs now include `public-read-write` and `authenticated-read` alongside `private`, `public-read`, and `bucket-owner-full-control`; full XML `AccessControlPolicy` grant rendering with AllUsers READ+WRITE and AuthenticatedUsers READ grants; bucket policy now supports `Deny` effect, condition operators, CIDR source-IP matching, and wildcard resource patterns; anonymous `PutObject` for `PublicReadWrite` buckets/objects
- object lock / retention / legal hold now has read paths (`GetObjectRetention`, `GetObjectLegalHold`) plus write paths (`PutObjectRetention`, `PutObjectLegalHold`) implemented for the S3 provider; bucket-level `GET`/`PUT ?object-lock` configuration wired as a bucket configuration subresource; disk provider continues to fail object-lock operations as unsupported
- encryption now includes SSE bucket-key-enabled support (`x-amz-server-side-encryption-bucket-key-enabled` header)
- 13 bucket configuration subresource families (Tagging, Logging, Website, RequestPayment, Accelerate, Lifecycle, Replication, Notifications, ObjectLockConfiguration, Analytics, Metrics, Inventory, IntelligentTiering) wired end-to-end
- remaining: version-specific ACLs, multipart-initiation ACL headers, object lock write-path enforcement on disk, bucket-level default-retention semantics, and broader encryption follow-ons

### M8 — Hardening

- conformance improvement
- benchmark tuning
- docs
- end-to-end sample polish

Status: **in progress / partially complete**

- conformance coverage now includes version-aware presigned reads, mixed bucket-subresource rejection, encoded/owner-aware object and multipart listing responses, and additional AWS SDK compatibility cases
- build/test/self-contained publish validation remains scriptable through the standard `dotnet` commands plus `eng\Invoke-AotPublishValidation.ps1`, but the repository currently has no checked-in `.github\workflows\` automation; restoring CI validation is tracked in [#15](https://github.com/SymoHTL/Intergrated-S3/issues/15)
- `docs/webui-reference-host.md` now captures the current reference-host surface and validation commands, while broader secure/composed host guidance is tracked in [#19](https://github.com/SymoHTL/Intergrated-S3/issues/19)
- `src\IntegratedS3\WebUi.MvcRazor`, `src\IntegratedS3\WebUi.BlazorWasm`, and `src\IntegratedS3\WebUi.BlazorWasm.Client` now provide the planned additional sample consumers, and `docs/web-consumer-samples.md` documents how to run them
- benchmark baselines remain pending ([#14](https://github.com/SymoHTL/Intergrated-S3/issues/14), [#13](https://github.com/SymoHTL/Intergrated-S3/issues/13))

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
  - delegated `GET` presign semantics are wired end-to-end for the native S3 provider: `ResolveObjectLocationRequest` carries requested expiry, `IntegratedS3HttpPresignStrategy` forwards provider/expiry/version metadata through the resolver seam, `AddS3Storage(...)` registers an S3-backed object-location resolver, and delegated-mode callers receive provider URLs with safe proxy fallback
  - backend-direct native S3 presign is also enabled for explicit `Direct` requests: the provider's existing provider-agnostic `IStorageBackend.PresignObjectDirectAsync(...)` seam now issues native S3 `GET` / `PUT` grants without changing the delegated-read path or proxy fallback behavior
  - focused presign/provider tests now cover delegated S3 URL issuance, backend-direct S3 `GET` / `PUT` selection, expiry/version forwarding, proxy fallback for unavailable or incompatible direct/delegated resolution, and custom-endpoint defaults
  - custom `ServiceUrl` endpoints now use the configured region as the AWS SDK authentication region, preserve `http` delegated-read presigns instead of silently upgrading to `https`, and reduce AWS SDK v4 flexible-checksum behavior to required-only mode so copy/multipart/checksum flows are less brittle against local S3-compatible backends
  - `IntegratedS3.Tests` now adds focused config/presign regression coverage for those custom-endpoint defaults plus an opt-in local S3-compatible conformance test that exercises copy, multipart, checksum, AES256 SSE, versioning, and delegated-read flows via `INTEGRATEDS3_S3COMPAT_SERVICE_URL`, `..._ACCESS_KEY`, `..._SECRET_KEY`, optional `..._REGION`, and optional `..._FORCE_PATH_STYLE`
  - `ListParts` is now implemented end-to-end for `GET ?uploadId=...`: the shared abstractions/core stack now exposes multipart part listing, disk and native-S3 providers enumerate part metadata/checksums, the HTTP surface supports `max-parts`, `part-number-marker`, and `encoding-type=url`, `ListPartsResult` XML now includes pagination/checksum fields, and unsupported multipart subresource combinations still fail explicitly
  - `UploadPartCopy` is now implemented end-to-end for `PUT ?partNumber=...&uploadId=...` copy-source flows: the shared abstractions/core stack now exposes provider-agnostic copy-part requests, disk and native-S3 providers honor versioned and ranged source reads plus source preconditions, the HTTP surface parses checksum algorithm/value copy-part headers and emits `CopyPartResult` XML with requested checksum and source-version fidelity, and focused disk/HTTP/AWS SDK/provider/local-conformance tests cover the path
  - same-key `CopyObject` with `x-amz-metadata-directive: REPLACE` is already supported for the current rclone-style `x-amz-meta-mtime` update flow when callers resend the replacement metadata they want to keep, and focused disk plus native-S3 provider tests now lock in that replacement metadata wins while object content stays unchanged
  - focused native-S3 provider and official AWS .NET SDK coverage now also lock in same-key self-copy with explicit historical `SourceVersionId` plus `MetadataDirective=REPLACE`, proving the current stack creates a new latest version with the historical payload plus replacement metadata while leaving the historical version unchanged
  - `S3StorageServiceTests` now also include a narrow regression that historical copy-source preconditions and `SourceVersionId` are forwarded unchanged to the underlying AWS client path
  - `S3CompatibleEndpointConformanceTests` now add opt-in local S3-compatible coverage for historical-version conditional `GET` behavior, copy-source preconditions against `SourceVersionId`, same-key historical self-copy with replacement metadata, delegated historical reads that stay pinned after a current delete marker, and historical-version copy succeeding while a delete marker is current, so configured MinIO/LocalStack-style endpoints can validate those semantics against real backends

- Remaining scope:
  - continue hardening the remaining multipart copy/listing edge semantics and real-endpoint parity now that `ListParts` and `UploadPartCopy` are wired through the current stack
  - continue widening same-key/version-targeted copy parity into any remaining metadata-directive, multipart-copy, or delete-marker-adjacent differences discovered against real S3-compatible endpoints without changing the current `REPLACE` semantics unless compatibility evidence requires it
  - run the local S3-compatible conformance harness continuously against concrete MinIO/LocalStack-style endpoints, capture any intentionally unsupported checksum/SSE/copy differences explicitly, and extend provider-specific parity coverage from those findings
  - keep reviewing whether backend-direct presign should remain enabled for both native S3 `GET` and `PUT` long-term or be narrowed once more real-endpoint compatibility data is gathered
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
  - native S3-backed direct `GET` / `PUT` presigns now flow through that backend seam for explicit `Direct` requests, while delegated S3 `GET` presigns continue to flow through the resolver seam with provider/expiry/version metadata forwarding and safe proxy fallback for unsupported or incompatible requests
  - client transfer helpers now accept explicit preferred access-mode hints, keep streaming/file transfers on the high-level client surface, delete failed download targets instead of leaving partial files behind, and surface presign-endpoint error bodies in thrown `HttpRequestException` messages
  - the first-party client now also exposes explicit resume-aware file download helpers that issue `Range` requests against existing partial files, rewrite from the start when a server ignores the range or returns `206 Partial Content` without a matching `Content-Range.From`, treat matching `416 RequestedRangeNotSatisfiable` responses as already complete, and preserve pre-existing partial files while cleaning up files created by the current attempt
  - checksum-aware resume downloads now also roll the destination file back to its pre-resume length when append validation fails, so missing or mismatched resumed range data does not corrupt an existing partial file
  - first-party client defaults now intentionally keep access-mode selection explicit at the caller boundary: overloads that omit `PreferredAccessMode` / `preferredAccessMode` leave the hint unset, do not infer `Direct` / `Delegated` access from provider/service discovery, and therefore keep proxy-mode as the stable default; callers can inspect provider descriptors for UX and then opt into `Direct` / `Delegated` explicitly where appropriate
  - focused tests lock in: backend-direct selection for read and write presigns; primary-backend-only direct selection with safe proxy fallback; delegated-mode selection when resolver supplies a `Delegated` or `Passthrough` location; direct-mode selection when resolver supplies a `Redirect` location; proxy fallback for null/incompatible resolver results; resolver not consulted for write presigns or when no preferred mode is set; default client/presign overloads leave access-mode unset; version ID forwarded to resolver and returned in the grant; resolved headers forwarded to the grant; expiry derived from the resolver when provided, falling back to request expiry
- Remaining scope:
  - shift the remaining client follow-on work toward broader large-object/live-endpoint ergonomics (for example composite-checksum UX, direct/delegated transfer validation against real providers, and any optional discovery helpers) now that resume-path integrity is hardened
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
    - `IntegratedS3EndpointOptions` now bind from `IntegratedS3:Endpoints`, including configuration-bound whole-route, shared-route, and per-feature authorization conventions, and the map-time `MapIntegratedS3Endpoints(...)` overloads now start from configured endpoint defaults so host-configured toggles remain overrideable per host/test
    - `IntegratedS3EndpointOptions` now expose `IntegratedS3EndpointFeature`-keyed `SetFeatureRouteGroupConfiguration(...)` / `GetFeatureRouteGroupConfiguration(...)` helpers, and the current `Configure*RouteGroup` properties are wrappers over that shared per-feature registry so future endpoint surfaces can follow the same callback pattern
  - Remaining scope:
    - decide whether custom `IStorageBackend` registration needs more DI sugar than direct service registration plus the new configured-provider helpers
    - evaluate whether common authorization conventions should also be exposed as configuration-bound host options in addition to the current map-time route-group callbacks
  - revisit whether future endpoint surfaces should automatically grow matching per-feature route-group callbacks as they are added
  - preserve `CreateSlimBuilder(...)`, Minimal API, trimming, and AOT friendliness as any follow-up ergonomics polish lands

### Track E — S3 protocol fidelity and edge-case hardening

- Packages: `IntegratedS3.Abstractions`, `IntegratedS3.AspNetCore`, `IntegratedS3.Core`, `IntegratedS3.Provider.Disk`, `IntegratedS3.Provider.S3`, `IntegratedS3.EntityFramework`, `IntegratedS3.Protocol`, `IntegratedS3.Tests`
- Ready: now
- Depends on: coordinate with Tracks B and C as parity and client-surface hardening follow-ons land
- Status update:
  - bucket-level multipart listing/discovery (`GET ?uploads`) is now implemented end-to-end for the current disk/Core/HTTP surface
  - the new slice adds provider/service contracts for multipart listing, platform-managed multipart-state enumeration via `IStorageMultipartStateStore`, disk-provider sidecar fallback enumeration, S3-compatible `ListMultipartUploadsResult` XML output, delimiter/common-prefix handling, marker pagination (`key-marker` / `upload-id-marker`), and multipart feature-toggle enforcement on the bucket route
  - focused coverage now locks in empty listings, completed/aborted upload exclusion, platform-managed multipart-state enumeration, virtual-hosted-style HTTP behavior, feature-toggle blocking, and AWS SDK `ListMultipartUploadsAsync` compatibility for the current supported surface
  - bucket-compatible listing now also covers legacy `ListObjects` (V1) routing when `list-type` is absent, `encoding-type=url` across object and multipart list responses, `fetch-owner` handling for `ListObjectsV2`, and owner/initiator XML parity across object and multipart listing payloads
  - bucket/object-compatible subresource validation now uses an explicit supported-matrix for bucket `?versioning`, `?cors`, `?uploads`, and `?versions` plus object `?tagging`, `?versionId`, and multipart workflows, rejects remaining unsupported single subresources with consistent `NotImplemented` responses, and returns explicit unsupported-combination results for invalid mixed query sets
  - focused HTTP coverage now locks in that SigV4 presign query parameters such as `X-Amz-*` and `x-id` continue to be ignored during bucket/object subresource validation for the currently supported paths
  - protocol/conformance coverage now locks in canonical empty-value subresource signing plus presigned bucket-versioning and historical-version reads on the S3-compatible route, alongside AWS SDK compatibility for legacy and fetch-owner list flows
  - focused coverage now locks in empty listings, completed/aborted upload exclusion, platform-managed multipart-state enumeration, virtual-hosted-style HTTP behavior, feature-toggle blocking, AWS SDK `ListMultipartUploadsAsync` compatibility for the current supported surface, and explicit multipart `encoding-type=url` / ignored lone `upload-id-marker` / `max-uploads` guardrail behavior
  - bucket/object-compatible subresource validation now uses an explicit supported-matrix for bucket `?versioning`, `?cors`, `?uploads`, and `?versions` plus object `?tagging`, `?versionId`, and multipart workflows, rejects remaining unsupported single subresources with consistent `NotImplemented` responses, and returns explicit unsupported-combination results for invalid mixed query sets
  - focused HTTP coverage now locks in that SigV4 presign query parameters such as `X-Amz-*` and `x-id` continue to be ignored during bucket/object subresource validation for the currently supported paths
  - protocol/conformance coverage now locks in canonical empty-value subresource signing plus presigned bucket-versioning and historical-version reads on the S3-compatible route
  - object-tagging header parity is now implemented end-to-end for `PUT Object`, `CopyObject`, and `InitiateMultipartUpload`: the S3-compatible HTTP surface now parses `x-amz-tagging` and `x-amz-tagging-directive`, copy requests honor `COPY` vs `REPLACE` semantics, and the resulting tags flow through Core replication, disk/native-S3 providers, and platform-managed multipart state so completed multipart objects retain their initiated tags
  - focused coverage now locks in direct disk-provider persistence, S3-provider forwarding, EF-backed multipart-state round-tripping, and end-to-end HTTP behavior for put/copy/multipart tagging-header flows
  - standard object-header parity now covers `x-amz-meta-*`, `Cache-Control`, `Content-Disposition`, `Content-Encoding`, `Content-Language`, `Expires`, multipart-initiation header persistence, and `x-amz-metadata-directive` copy replacement semantics across the current HTTP, disk, S3-provider, and persisted-state surfaces; the sample-host-only `x-integrateds3-meta-*` prefix remains accepted/emitted for compatibility
  - object-tagging header parity is now implemented end-to-end for `PUT Object`, `CopyObject`, and `InitiateMultipartUpload`: the S3-compatible HTTP surface now parses `x-amz-tagging` and `x-amz-tagging-directive`, copy requests honor `COPY` vs `REPLACE` semantics, and the resulting tags flow through Core replication, disk/native-S3 providers, and platform-managed multipart state so completed multipart objects retain their initiated tags
  - focused coverage now locks in direct disk-provider persistence, S3-provider forwarding, EF-backed multipart-state round-tripping, and end-to-end HTTP behavior for put/copy/multipart tagging-header flows
  - standard object-header parity now covers `x-amz-meta-*`, `Cache-Control`, `Content-Disposition`, `Content-Encoding`, `Content-Language`, `Expires`, multipart-initiation header persistence, and `x-amz-metadata-directive` copy replacement semantics across the current HTTP, disk, S3-provider, and persisted-state surfaces; the sample-host-only `x-integrateds3-meta-*` prefix remains accepted/emitted for compatibility
  - presigned-query authentication now keeps host-only presigned URLs valid when a client also sends an unsigned `x-amz-content-sha256` header, which matches common SDK/browser request behavior
  - AWS SDK compatibility coverage now locks in path-style `aws-chunked` PUT uploads, virtual-hosted-style `aws-chunked` PUT uploads, and virtual-hosted-style presigned PUT uploads that sign `content-type`
  - copy-object source conditionals now preserve the S3 precedence rules across both combined validator pairs, with focused HTTP and AWS SDK coverage for `if-match` / `if-unmodified-since` success precedence and `if-none-match` / `if-modified-since` failure precedence
  - direct write validation now rejects ambiguous multi-checksum `x-amz-checksum-*` request header combinations instead of accepting them silently on the current S3-compatible write surface
  - SigV4 request canonicalization now preserves both raw-query literal `+` / duplicate query parameter edges and already-escaped path segments (`%XX`) during canonical-request construction, with focused protocol, HTTP conformance, and AWS SDK compatibility coverage for the current supported surface
  - temporary-session SigV4 credentials are now modeled end-to-end on the current HTTP surface: configured access-key credentials accept optional `SessionToken` values, header authentication requires a signed `x-amz-security-token` header when temporary credentials are configured, presigned-query validation enforces `X-Amz-Security-Token` for temporary credentials, and first-party proxy presigns now emit the security token
- trailer-backed `aws-chunked` write validation is now hardened on the current HTTP/SigV4 surface: trailer-mode payload hashes require `aws-chunked` content encoding plus a signed `x-amz-trailer` declaration; when `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER` is used, `PUT` and `UploadPart` require `x-amz-trailer-signature` and now canonicalize trailer headers (lowercasing/sorting/whitespace normalization while excluding the signature trailer) before hashing so mixed-case checksum trailer names verify correctly against the captured zero-length final-chunk signature; `STREAMING-UNSIGNED-PAYLOAD-TRAILER` success paths remain covered for checksum-trailer uploads without that signature trailer, unsigned trailer-backed SHA256, SHA1, CRC32, and CRC32C checksum values now have both success and S3-style `BadDigest` mismatch parity on raw HTTP and SigV4 header-auth `PUT Object` / `UploadPart` flows, and signed `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER` checksum trailers now have matching SHA1, CRC32, and CRC32C success plus `BadDigest` mismatch parity on those same raw HTTP and SigV4 surfaces; checksum trailers must be declared and present exactly once, duplicate header+trailer checksum submission is rejected, and focused protocol plus SigV4/HTTP tests now cover both valid uploads and representative malformed, signature-mismatch, and checksum-mismatch cases
- `CopyObject` checksum request parity is now wired end-to-end for the current supported copy path: `CopyObjectRequest` carries checksum algorithm plus explicit checksum values, the S3-compatible HTTP surface parses `x-amz-sdk-checksum-algorithm` / `x-amz-checksum-algorithm` and explicit `x-amz-checksum-*` copy headers without forcing a value when only an algorithm is declared, disk copy recomputes and persists the requested destination checksum, native S3 copy forwards the checksum algorithm plus explicit checksum headers through the AWS SDK, and focused disk/HTTP/S3-provider/AWS SDK coverage now locks in the single-algorithm copy-response header emission
- `PutObject` checksum-algorithm response parity is now wired end-to-end for the current supported write path: when a direct put declares exactly one supported checksum algorithm, successful responses now echo the same single `x-amz-checksum-algorithm` value alongside the matching checksum field, and focused HTTP plus AWS SDK/raw-header coverage now locks in SHA256, SHA1, CRC32C, and trailer-backed `aws-chunked` success paths without claiming broader checksum read-mode coverage
- direct `PutObject` explicit-checksum override parity is now narrowed end-to-end for the current supported write path: when a put request supplies an explicit single checksum value, disk persistence stores only that requested checksum instead of retaining extra computed checksum fields, so the immediate `PUT` success plus later `HEAD` / `GET` responses no longer emit unrequested checksum headers; focused disk, HTTP, and AWS SDK coverage now locks in SHA1 and CRC32C absence-of-extra-checksum behavior
- `UploadPart` checksum handling now matches direct writes on the supported HTTP surface: when a checksum algorithm is declared, the matching single checksum value must be supplied exactly once, including the current trailer-backed `aws-chunked` multipart path, and focused HTTP coverage now locks in missing-checksum failures, declared-trailer-without-trailer-value rejection, and valid trailer-backed success
- duplicate same-name `aws-chunked` checksum trailers are now rejected on the current `PUT Object` / `UploadPart` HTTP surface instead of silently accepting the last repeated value, and focused raw HTTP coverage now locks in `InvalidRequest` behavior for both direct object writes and multipart-part uploads
- `UploadPartCopy` checksum request parity is now wired end-to-end for the current supported multipart-copy path: `UploadPartCopyRequest` carries checksum algorithm plus explicit checksum values, the S3-compatible HTTP surface parses `x-amz-sdk-checksum-algorithm` / `x-amz-checksum-algorithm` and explicit `x-amz-checksum-*` copy-part headers without forcing a value when only an algorithm is declared, disk part-copy validates requested checksum algorithms/values against the upload and copied payload, native S3 copy-part forwards checksum headers through the AWS SDK, and focused disk/HTTP/S3-provider/AWS SDK coverage now locks in requested-checksum success plus mismatched-header `InvalidRequest` behavior without claiming multipart-finalization or read-side checksum parity
- focused AWS SDK compatibility coverage now proves the official AWS .NET SDK can exercise signed-trailer SHA1 `PutObject` / `UploadPart` requests against the current HTTP harness, and records the concrete broader signed-trailer limiter: algorithms beyond SHA1 (notably CRC32C) currently throw during trailer generation unless `AWSSDK.Extensions.CrtIntegration` is referenced; unsigned trailer mode still needs `DisablePayloadSigning=true`, which the SDK rejects against the current HTTP-only harness because it requires HTTPS, and multipart CRC32 initiate also still returns `501` there, so broader official SDK trailer coverage remains blocked until those prerequisites are addressed
- continued versioning/tagging/delete-marker hardening now makes simple deletes idempotent for missing keys, creates current delete markers for missing keys in versioned buckets, emits `x-amz-tagging-count` on successful current and historical object reads, validates documented object-tag limits and S3-compatible character rules as `InvalidTag`, decodes `x-amz-tagging` with strict UTF-8 while preserving literal `+`, returns `NoSuchVersion` for explicit missing-version entries inside `POST ?delete`, now fully honors batch-delete `Quiet=true` while retaining `<Error>` entries, preserves delete-marker response fidelity so explicit delete-marker-version removals return both `VersionId` and `DeleteMarkerVersionId`, and also normalizes explicit missing single-object `versionId` reads (`GET` / `HEAD` / copy-source) to S3-compatible `NoSuchVersion` responses at the HTTP edge; the broader multi-surface versioning/delete-marker stack is now locked in across raw HTTP, SigV4 header authentication, and official AWS .NET SDK compatibility coverage, including bucket versioning enable/suspend reads, current delete-marker GET/HEAD behavior, explicit historical-version GET/HEAD by `versionId`, delete-marker response headers/version IDs, and focused list-versions prefix/pagination fidelity
- non-empty bucket deletion now preserves S3-compatible `BucketNotEmpty` / `409 Conflict` semantics across the disk provider, native S3 translation path, and S3-compatible HTTP DeleteBucket surface, with focused regression coverage at each layer
- unsupported SigV4a (`AWS4-ECDSA-P256-SHA256`) requests are now fully authenticated: header-based and presigned-query ECDSA P-256 verification, region-set validation, and streaming `aws-chunked` trailer signature verification are all implemented and tested
- Remaining scope:
  - next: use the now-landed native-S3-provider-backed AWS SDK loopback harness to expand only the remaining meaningful write-time encryption follow-ons (for example richer multipart/read-side SSE surfacing where the SDK exposes it), then shift broader effort back to Track H / Track B hardening while still deferring broader official SDK trailer-algorithm expansion until the CRT/HTTPS harness decision is made
  - revisit broader AWS SDK signed-trailer algorithm coverage only after deciding whether a scoped `AWSSDK.Extensions.CrtIntegration` dependency and any related harness work are acceptable for this repo
  - keep `aws-chunked`, presigned-query, and virtual-hosted-style compatibility tightening against real client behavior beyond the now-covered PUT/presign routing and escaped-path flows
  - keep tightening the remaining `aws-chunked` edge cases beyond the newly enforced trailer-declaration/signing rules, especially around broader client permutations and remaining non-signature trailing-checksum parity
  - continue tightening deeper multipart-listing/client-compat edge semantics beyond the now-supported `encoding-type=url`, ignored lone `upload-id-marker`, explicit `max-uploads` validation behavior, and the landed `ListParts` pagination/checksum surface
  - all S3 XML responses now include `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"` on root elements for canonical namespace parity (#32); error responses now emit `x-amz-request-id` and `x-amz-id-2` headers; `S3XmlRequestReader` already uses `Name.LocalName` for namespace-tolerant parsing
  - `ListObjectVersions` (`?versions`) now supports `encoding-type=url`: key/prefix/delimiter/marker fields are URL-encoded consistently in the `ListVersionsResult` XML when requested, matching the existing behavior in `ListBucketResult`, `ListMultipartUploadsResult`, and `ListPartsResult`; duplicate `<EncodingType>` emission in multipart uploads listing is also fixed
  - full SigV4a support is now implemented: ECDSA P-256 header/presigned authentication with region-set validation, streaming `aws-chunked` trailer verification, and `integrateds3:auth-type=sigv4a` claim propagation

### Track F — Multi-backend async replication, health, and reconciliation

- Packages: `IntegratedS3.Core`, `IntegratedS3.Provider.*`, `IntegratedS3.AspNetCore`, `IntegratedS3.Tests`
- Ready: orchestration semantics now; richer repair/testing once Tracks B and H add more parity coverage
- Depends on: benefits from Track B for stronger second-provider scenarios
- Status update:
  - synchronous mirrored writes are implemented today via `WriteThroughAll`; that path currently covers bucket create/delete/versioning, object put/delete, copy-object, current-version tag mutation flows, and multipart upload operations
  - `WriteToPrimaryAsyncReplicas` is now implemented in Core for the current replicated-write surface by recording provider-agnostic `IStorageReplicaRepairBacklog` entries and dispatching through `IStorageReplicaRepairDispatcher`; the default in-process dispatch path is optional and durable replay remains a host concern
  - **multipart replication is now supported**: multipart upload operations (initiate, upload-part, complete, abort) now participate in write-through replication alongside bucket and object operations, removing the previous explicit rejection under replicated write modes
  - health-aware reads now combine `IStorageBackendHealthEvaluator`, dynamic snapshots, and optional `IStorageBackendHealthProbe` probing so recently unhealthy providers can be deprioritized and later reconsidered for read preference, while replicas with outstanding repairs are avoided by default unless explicitly configured otherwise
  - strict `WriteThroughAll` now preflights required replicas for health/currentness before mutating the primary, and post-primary partial failures are recorded as repair backlog entries instead of being rolled back or silently normalized
  - the admin HTTP surface now exposes read-only repair-backlog visibility at `GET /integrated-s3/admin/repairs`, including optional `replicaBackend` filtering and route-group auth/endpoint-toggle coverage
  - focused orchestration coverage now locks in async replica recording/dispatch, unhealthy-replica preflight, outstanding-repair read behavior, partial-write backlog semantics, failed-repair visibility, multi-replica dispatch-recording failure isolation, mixed replay success/failure, backlog growth for only the replicas that remain stale, and broader multi-replica repair semantics for bucket versioning, bucket CORS deletion, object-tag deletion, and write-through bucket-CORS partial failures
  - ASP.NET Core hosts can now bridge the existing backend-health monitor into readiness/liveness probes via `AddHealthChecks().AddIntegratedS3BackendHealthCheck()` plus `MapIntegratedS3HealthEndpoints()`, keeping Core provider-agnostic while giving hosts a supported `/health/live` and `/health/ready` path
  - the admin HTTP surface now exposes provider-agnostic diagnostics at `GET /integrated-s3/admin/diagnostics`, combining provider health, replica lag, and grouped repair diagnostics, while preserving read-only repair-backlog visibility at `GET /integrated-s3/admin/repairs` with optional `replicaBackend` filtering and route-group auth/endpoint-toggle coverage
  - focused orchestration coverage now locks in async replica recording/dispatch, unhealthy-replica preflight, outstanding-repair read behavior, partial-write backlog semantics, failed-repair visibility, multi-replica dispatch-recording failure isolation, mixed replay success/failure, and backlog growth for only the replicas that remain stale
  - async object-repair replay now reconciles primary-side `ObjectNotFound` / `BucketNotFound` by deleting stale replica state instead of leaving a failed backlog entry, and focused orchestration coverage now also locks in delete replay idempotence after out-of-band replica deletion plus current-topology-only replica targeting when removed-replica backlog entries remain
- Current semantics and boundaries:
  - replicas with outstanding repairs are treated as not current for strict write-through preflight and are excluded from replica-preferred reads by default, but that remains a tracked divergence signal rather than a blanket durability proof that every replica is fully caught up
  - admin-exposed replica lag is intentionally provider-agnostic: it is reported as outstanding repair counts plus oldest-outstanding age/activity rather than as a provider-native byte-offset, LSN, or checkpoint metric
  - unhealthy providers affect read routing today and admin diagnostics now surface that health state alongside lag/repair summaries, but health state still does not imply automatic async replay completion, provider eviction, or topology mutation
  - if a synchronous replica fails after the primary succeeds, the request still fails; Core does not roll the primary back, and the divergence remains visible in the repair backlog until a later repair pass finishes
  - incomplete or failed repair attempts remain visible as reconciliation backlog or outstanding divergence and can influence subsequent reads and writes; partial repair must not be reported as success
  - multipart operations now participate in write-through replication; edge cases around partial multipart replication failures are still being hardened
  - reconciliation/repair work in this track stays provider-agnostic and optional-host-integrated rather than locking the product into a mandatory built-in background-service architecture
- optional host scheduling for replay/cleanup work is now exposed through `IntegratedS3.AspNetCore` via `AddIntegratedS3MaintenanceJob(...)`, `IIntegratedS3MaintenanceJob`, and `IntegratedS3MaintenanceJobNames`, with fresh DI scopes per run and configuration-bindable `Enabled` / `RunOnStartup` / `Interval` settings for mirror replay, orphan detection, checksum verification, multipart cleanup, index compaction, and expired-artifact cleanup
- reference guidance now documents the recommended host-owned seams for each maintenance category, including the current limitation that the repair backlog captures divergence visibility but not yet enough durable operation intent to deliver a fully generic replay engine after process restarts
- Remaining scope:
  - add richer reconciliation and divergence-repair semantics for content, metadata, and version drift beyond the current backlog/dispatcher scaffold
  - persist richer repair inputs and maintenance-specific contracts so hosts can build more generic durable replay/cleanup implementations without provider-specific or store-specific glue
  - expand admin visibility beyond repair backlog into provider health, replica lag, and richer repair diagnostics without coupling providers to a fixed hosting model
  - continue fault-injection hardening beyond the current unhealthy-provider, async-replication/backlog/read-policy, bucket-versioning/CORS/tag-delete repair, and write-through delete scenarios into backlog-saturation, topology-change, and richer repair-diagnostics slices
  - add optional host integrations such as mirror replay, orphan detection, checksum verification, multipart cleanup, index compaction, and expired-artifact cleanup without coupling providers or Core to a fixed hosting model
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
  - bucket-location discovery is now implemented end-to-end: `BucketLocationInfo` flows through `IStorageService` / `IStorageBackend`, Core authorization/read routing, the disk provider reports the default empty location constraint (`us-east-1` semantics), the native S3 provider maps `GetBucketLocation` through the AWS SDK, and ASP.NET now serves S3-compatible `GET ?location` XML responses
  - provider support-state descriptors now surface `AccessControl` and `ServerSideEncryption` ownership alongside `Retention`, with disk remaining `NotApplicable` and the native S3 provider reporting delegated ownership for those upstream-owned concerns
  - `IntegratedS3.Abstractions` now includes provider-agnostic object-level SSE request/response models for the managed `AES256` / `aws:kms` / `aws:kms:dsse` slice, and `StorageCapabilities` now expose provider-agnostic SSE variant details so future providers can advertise supported request styles and operations without leaking SDK types into shared contracts
  - the native S3 provider now reports `ServerSideEncryption = Native` with managed-variant details, maps the managed `AES256` / `aws:kms` / `aws:kms:dsse` request slice through the AWS SDK for put/copy/initiate-multipart, and preserves returned object SSE metadata on provider-facing `ObjectInfo` results
  - ASP.NET now validates and normalizes the managed `AES256` / `aws:kms` / `aws:kms:dsse` SSE headers for S3-compatible put/copy/initiate-multipart requests, rejects unsupported SSE request shapes explicitly, and emits returned SSE response headers on put/copy/get/head object responses
  - the disk provider now rejects the managed SSE request shapes explicitly across the current get/head/put/copy/initiate-multipart surface so unsupported requests fail consistently instead of being ignored
  - `IntegratedS3.Tests` now exercises the end-to-end SSE slice across contracts, S3 provider mapping, disk guardrails, HTTP header/response behavior, and catalog persistence for the shipped `AES256` / `aws:kms` surface
  - bucket default encryption now spans both control-plane configuration and the current native-S3 write-time parity slice: provider-agnostic contracts still model a single default encryption rule, ASP.NET still parses and writes S3-compatible `ServerSideEncryptionConfiguration` XML with explicit validation, the native S3 provider still maps bucket-encryption APIs directly, and native S3 put/copy/initiate-multipart writes now auto-apply bucket defaults whenever explicit request SSE is absent, while the disk provider preserves `NoSuchBucket` for missing buckets and otherwise fails explicitly as unsupported
  - focused tests now cover bucket default encryption HTTP round-tripping and validation, missing-bucket plus missing-configuration error shaping, core write-through replication behavior, native S3 mapping, explicit disk-provider non-support, focused `S3StorageServiceTests` coverage for native-S3 put/copy/initiate-multipart auto-application plus explicit-request precedence and missing-configuration no-op behavior, and official AWS .NET SDK compatibility coverage on a native-S3-provider-backed loopback harness for bucket-encryption control-plane CRUD plus put/copy inheritance and explicit-SSE override behavior
  - the default disk-backed `WebUiApplicationFactory` loopback host is still not sufficient for native-S3-only encryption parity by itself, but `IntegratedS3AwsSdkCompatibilityTests` now includes a reusable native-S3-provider-backed loopback path for this slice rather than leaving the official-client validation gap open
  - browser-facing CORS handling now preserves `Vary` headers for both allowed and rejected preflight/actual responses, successful non-wildcard evaluations emit `Access-Control-Allow-Credentials: true`, literal `*` origin rules return `Access-Control-Allow-Origin: *` while intentionally omitting the credentials header, and S3-compatible bucket subresource validation ignores SigV4 presign query parameters so presigned bucket-versioning reads continue to work
  - Core now includes a platform-managed ACL/policy compatibility service that tracks current bucket/object canned ACL state plus limited bucket public-policy documents, resets current-object ACL state on writes/deletes, and augments the existing `ClaimsPrincipal` authorization decorator with anonymous/public read/list fallback instead of replacing the host authorization model
  - ASP.NET now supports S3-compatible bucket/object `GET` / `PUT ?acl`, bucket `GET` / `PUT` / `DELETE ?policy`, `x-amz-acl` handling on bucket create and object put/copy, compatible `AccessControlPolicy` XML body parsing for `PUT ?acl`, and object `x-amz-grant-read`, `x-amz-grant-read-acp`, `x-amz-grant-write-acp`, and `x-amz-grant-full-control` compatibility on object `PUT` and `PUT ?acl`, while conflicting, malformed, and still-unsupported grant-header permutations continue to fail explicitly with `InvalidRequest`, `InvalidArgument`, or `NotImplemented`
  - `IntegratedS3.Tests` now cover bucket ACL public-list behavior, object ACL public-read behavior, policy-backed anonymous list/read behavior, ACL reset-on-overwrite semantics, XML ACL body parsing, object grant-header persistence/rendering/reset behavior, and explicit unsupported ACL/policy request errors
  - read-only object-lock metadata exposure is now implemented via provider-agnostic retention/legal-hold models and requests, S3-compatible object `GET ?retention` / `GET ?legal-hold` XML responses, and `x-amz-object-lock-mode`, `x-amz-object-lock-retain-until-date`, and `x-amz-object-lock-legal-hold` headers on ordinary object `GET` / `HEAD` responses when metadata is present
  - the native S3 provider now maps `GetObjectRetention` / `GetObjectLegalHold`, hydrates retention/legal-hold metadata from S3 `GET` / `HEAD` object responses, and advertises object-lock capability, while the disk provider continues to fail object-lock reads explicitly as unsupported instead of pretending to enforce retention semantics
  - focused tests now cover retention/legal-hold HTTP XML round-tripping, object read headers, disk-provider `NotImplemented` behavior, native S3 object-lock provider reads, and retention/legal-hold metadata hydration on S3 object reads
- Remaining scope:
  - broader ACL/policy parity beyond the implemented public-read/list plus object grant-header slice, including version-specific ACLs, multipart-initiation ACL persistence, richer principals/actions/resources/conditions, broader bucket-policy parsing/validation, and any future provider-owned access-control passthrough work
  - object lock write-path enforcement, bucket-level object-lock configuration/default-retention semantics, and any richer provider persistence beyond the landed read-only retention/legal-hold exposure
  - any remaining server-side-encryption follow-on work beyond the implemented native-S3-plus-explicit-disk-guardrails object-level `AES256` / `aws:kms` slice plus landed bucket default encryption control-plane, write-time auto-application behavior, and native-S3-backed AWS SDK loopback coverage, especially richer multipart/read-side surfacing, broader SSE variants, and any future provider expansion, without leaking provider SDK details into shared contracts
  - `IntegratedS3.Tests` now exercise the extended managed SSE slice across contracts, capability reporting, S3 provider mapping, disk guardrails, HTTP header/response behavior, and catalog persistence for the shipped `AES256` / `aws:kms` / `aws:kms:dsse` surface
  - browser-facing CORS handling now preserves `Vary` headers for both allowed and rejected preflight/actual responses, and S3-compatible bucket subresource validation now ignores SigV4 presign query parameters so presigned bucket-versioning reads continue to work
  - `IntegratedS3.Tests` now exercises the end-to-end SSE slice across contracts, S3 provider mapping, disk guardrails, HTTP header/response behavior, and catalog persistence for the shipped `AES256` / `aws:kms` surface
  - browser-facing CORS handling now preserves `Vary` headers for both allowed and rejected preflight/actual responses, S3-compatible bucket subresource validation now ignores SigV4 presign query parameters, and the current test suite now covers the shipped bucket-location slice across disk, HTTP, native-S3 mapping, and AWS SDK compatibility flows
  - rclone S3 compatibility hardening: `RcloneS3MultipartEncodingCompatibilityTests` now validates 20 scenarios covering multipart upload lifecycle (initiate/upload/complete/abort/list-uploads/list-parts/pagination), special character key encoding (spaces, plus signs, Unicode, URL-special characters, directory-style delimiters), copy-object with encoded keys, and S3 error handling (NoSuchKey, NoSuchBucket, NoSuchUpload, error XML format, request-id presence). Bug fix: `CompleteMultipartUploadAsync` endpoint now returns `NoSuchUpload` (404) instead of `InvalidRequest` (409) when the upload ID does not exist, matching S3 protocol behavior expected by rclone.
  - rclone S3 CRUD/metadata compatibility: `RcloneS3CrudCompatibilityTests` now validates 22 scenarios covering the core rclone object lifecycle — `Content-MD5` integrity verification (valid and invalid hashes), `x-amz-meta-mtime` round-trip on GET/HEAD, multiple metadata header persistence, full HEAD header verification (Content-Length, Content-Type, ETag quoting, Last-Modified, Accept-Ranges, metadata, empty body), full GET content verification, Range request variants (middle/suffix/single-byte with 206 + Content-Range), `If-None-Match` conditional (304 and 200), `If-Modified-Since` conditional (304 and 200), single DELETE (204 + 404 verification), batch `?delete` with 5-object purge, batch delete with non-existent keys (S3 idempotent success), zero-byte directory marker (`testdir/` with `application/x-directory`), `CopyObject` with `REPLACE` metadata directive (rclone touch pattern), `CopyObject` with `COPY` metadata directive, cross-bucket copy, and Content-Type handling (explicit and default). Bug fix: `Content-MD5` header is now validated by the endpoint layer — the disk storage provider computes MD5 alongside existing checksums, and `TryParseRequestChecksums` feeds it through the standard validation pipeline, returning `BadDigest` for mismatches.
- Remaining scope:
  - ACL/policy-compatible behavior where it makes sense alongside the existing `ClaimsPrincipal` authorization model
  - object lock write-path enforcement, bucket-level object-lock configuration/default-retention semantics, and any richer provider persistence beyond the landed read-only retention/legal-hold exposure
  - any remaining server-side-encryption follow-on work beyond the implemented native-S3-plus-explicit-disk-guardrails managed `AES256` / `aws:kms` / `aws:kms:dsse` slice plus SSE-C customer-provided-key support, bucket default encryption control-plane, write-time auto-application support, and native-S3-backed AWS SDK loopback coverage, especially richer multipart/read-side SSE surfacing and any future provider expansion beyond the new provider-agnostic SSE variant descriptors
  - any remaining CORS conformance hardening beyond the implemented bucket-configuration, `Vary` handling, and browser-flow slice
  - next best low-risk follow-on: reuse the new native-S3-provider-backed AWS SDK loopback harness for any remaining encryption/client parity that exposes observable SDK metadata, while object-lock write-path enforcement, bucket-level object-lock configuration/default-retention, and richer multipart/read-side SSE surfacing remain deferred
- Newly implemented (endpoint surface expansion):
  - ACL expansion: `StorageCannedAcl` now supports `PublicReadWrite` and `AuthenticatedRead` in addition to `Private`, `PublicRead`, and `BucketOwnerFullControl`; `x-amz-acl` header parsing, XML `AccessControlPolicy` body resolution, and ACL grant rendering now handle the new canned ACL types with proper AllUsers READ+WRITE and AuthenticatedUsers READ grants; `StorageAclPermission.Write` is now a first-class enum member; the in-memory authorization compatibility service now allows anonymous `PutObject` for `PublicReadWrite` buckets/objects and anonymous `GetObject`/`HeadObject` for `PublicReadWrite` objects; `TryResolveCannedAcl` and `TryResolveObjectAcl` now accept the AuthenticatedUsers group URI and WRITE permission for AllUsers
  - checksum mode: `x-amz-checksum-mode: ENABLED` header is now respected on `GET` and `HEAD` object endpoints; checksum value headers (`x-amz-checksum-crc32`, `x-amz-checksum-crc32c`, `x-amz-checksum-sha1`, `x-amz-checksum-sha256`, `x-amz-checksum-type`) are now conditionally emitted — only when the request includes `x-amz-checksum-mode: ENABLED` on GET/HEAD paths, while write responses (PUT, COPY, multipart) continue to emit checksums unconditionally
  - multi-range GET: `Range: bytes=0-100,200-300` multi-range requests are now supported with proper `multipart/byteranges` responses including MIME boundaries; each range is fetched individually from the storage backend; single-range and no-range requests continue to work as before
  - POST object (browser form upload): bucket-level `POST` with `multipart/form-data` encoding now supports browser-based object uploads; the handler parses `key`, `file`, `Content-Type`, `x-amz-acl`, `success_action_status` (200/201/204), `success_action_redirect`, `x-amz-meta-*` form fields, and optional `policy` (base64 JSON) with basic condition validation (expiration, exact-match, starts-with); the response supports 204 (default), 200, 201 (XML PostResponse), and redirect modes; `${filename}` placeholder substitution in key is supported; `S3XmlResponseWriter.WritePostObjectResult` generates the 201 XML response
- Remaining scope:
  - version-specific ACLs, multipart-initiation ACL persistence, richer principals/actions/resources/conditions beyond AllUsers and AuthenticatedUsers groups
  - checksum-mode interactions with multipart composite checksums, broader CRC64NVME support
  - multi-range: efficiency improvements (single backend call with seeking instead of per-range calls), range overlap validation, configurable max-range limit
  - POST object: signature validation (AWS Signature Version 2/4 on the policy), broader policy condition operators (content-length-range), provider-specific ACL passthrough

### Track H — Hardening, conformance, performance, samples, and release polish

- Packages: `IntegratedS3.Benchmarks`, `IntegratedS3.Tests`, `IntegratedS3.Testing`, `WebUi`, `docs/`
- Ready: now
- Depends on: can scaffold immediately; full coverage expands as the remaining Tracks B through G hardening slices land
- Status update:
  - `IntegratedS3SigV4ConformanceTests` now cover presigned bucket-versioning reads, presigned historical-version reads, and presigned expiry/clock-skew XML error behavior, while `IntegratedS3SigV4ProtocolTests` lock in canonical empty-value subresource signing.
  - `IntegratedS3HttpEndpointsTests` now cover unsupported mixed bucket subresources and multipart `encoding-type=url` rejection, and `IntegratedS3AwsSdkCompatibilityTests` now include version-id-aware metadata/read coverage.
  - `IntegratedS3CoreOrchestrationTests` now cover provider-unavailable read failover, no failover on not-found, unhealthy snapshot expiry recovery, probe-timeout handling, async replica recording/dispatch, unhealthy-replica preflight, outstanding-repair read policy, partial-write backlog semantics, failed-repair visibility, multi-replica dispatch-recording failure isolation, mixed replay success/failure, backlog growth for replicas that remain stale, and broader repair/reconciliation fault injection across bucket versioning, bucket-CORS deletion, object-tag deletion, and write-through bucket-CORS delete failures.
  - `IntegratedS3HttpEndpointsTests` now cover unsupported mixed bucket subresources plus legacy/V2/multipart listing parity details such as `encoding-type=url`, `fetch-owner`, and owner/initiator XML fields, and `IntegratedS3AwsSdkCompatibilityTests` now include version-id-aware metadata/read coverage together with legacy/fetch-owner listing checks.
  - `IntegratedS3HttpEndpointsTests` now cover unsupported mixed bucket subresources plus multipart `encoding-type=url` response encoding, ignored lone `upload-id-marker`, invalid `encoding-type`, and explicit multipart `max-uploads` validation, and `IntegratedS3AwsSdkCompatibilityTests` now include version-id-aware metadata/read coverage.
  - `IntegratedS3HttpEndpointsTests` now cover unsupported mixed bucket subresources, multipart `encoding-type=url` rejection, S3-compatible copy-source `versionId` handling for historical versions plus current/explicit delete-marker failures, and `NoSuchVersion` fidelity for explicit missing single-object `GET` / `HEAD` / copy-source reads.
  - `IntegratedS3AwsSdkCompatibilityTests` now include version-id-aware metadata/read coverage plus `CopyObject.SourceVersionId` historical-copy, explicit missing-version, and delete-marker compatibility coverage.
  - `IntegratedS3HttpEndpointsTests` and `IntegratedS3AwsSdkCompatibilityTests` now also cover batch-delete `Quiet=true` success suppression, retained `<Error>` entries for mixed outcomes, and delete-marker response fidelity for both creating current delete markers and explicitly deleting delete-marker versions.
  - `S3CompatibleEndpointConformanceTests` now include an opt-in local S3-compatible multipart upload listing lifecycle scenario that verifies same-key marker pagination, prefix filtering, and completed/aborted upload exclusion against configured `INTEGRATEDS3_S3COMPAT_*` endpoints.
  - `S3CompatibleEndpointConformanceTests` now also include opt-in local `SHA1` and `CRC32C` multipart-upload plus copy checksum scenarios that assert composite checksum metadata on completed multipart objects and copied objects against configured `INTEGRATEDS3_S3COMPAT_*` endpoints.
  - `DiskStorageServiceTests`, `IntegratedS3HttpEndpointsTests`, `IntegratedS3AwsSdkCompatibilityTests`, `S3StorageServiceTests`, and `S3CompatibleEndpointConformanceTests` now cover `UploadPartCopy` versioned/ranged source reads, source preconditions, checksum request forwarding/validation, requested checksum response fields, and explicit missing-version fidelity across the disk and native-S3 paths.
  - `IntegratedS3CoreOrchestrationTests` now cover provider-unavailable read failover, no failover on not-found, unhealthy snapshot expiry recovery, probe-timeout handling, async replica recording/dispatch, unhealthy-replica preflight, outstanding-repair read policy, partial-write backlog semantics, failed-repair visibility, multi-replica dispatch-recording failure isolation, mixed replay success/failure, and backlog growth for replicas that remain stale.
  - `IntegratedS3.Testing` now ships a supported provider contract harness via `StorageProviderContractFixture`, `StorageProviderContractTests`, reusable in-memory support-state stores, checksum helpers, and DI registration helpers, with the disk provider consuming that path in first-party tests and onboarding documented in `docs/provider-contract-testing.md`.
  - `IntegratedS3.Testing` shared contract coverage now also locks in conditional read and copy-source precedence, conditional-plus-range behavior, historical conditional reads, configurable checksum algorithm persistence, and multi-checksum conflict rejection so providers inherit those edge-case expectations through the common harness.
  - `IntegratedS3.Testing` shared provider-contract coverage now also locks in current delete-marker `GET` / `HEAD` failures, historical-version reads after a current delete marker exists, deleting an explicit delete-marker version to restore the previous latest object, and `ListObjectVersions` marker pagination across delete markers so first-party providers inherit those versioning semantics through the common harness.
  - `S3StorageServiceTests`, `IntegratedS3AwsSdkCompatibilityTests`, and `S3CompatibleEndpointConformanceTests` now also cover same-key historical self-copy with replacement metadata plus historical-version copy behavior while a delete marker is current, keeping native-S3 provider forwarding, official AWS SDK compatibility, and opt-in real-endpoint conformance aligned for the current rclone-style flow.
  - `IntegratedS3CoreOrchestrationTests` now also cover async replica replay outcomes for copied and deleted objects when sibling repairs diverge, missing-primary replay cleanup that removes stale replica state, delete replay completion after out-of-band replica deletion, current-topology-only async targeting when removed-replica backlog remains, plus write-through copy partial-failure backlog recording and idempotent delete success when a replica already reports `ObjectNotFound`.
  - `S3CompatibleEndpointConformanceTests` now also add opt-in local direct historical-version presigned `GET` coverage that remains pinned after a current delete marker, direct presigned `PUT` version/content-type round-tripping, and `ListObjectVersions` key-marker/version-id-marker pagination across delete markers, so the local real-endpoint lane can validate the newer direct/pagination semantics once `INTEGRATEDS3_S3COMPAT_*` is configured.
  - `src\IntegratedS3\WebUi` now has a dedicated reference-host guide in `docs/webui-reference-host.md`, with local sample storage kept under `App_Data` and excluded from build/publish outputs.
  - `src\IntegratedS3\WebUi` now also supports config-driven disk-or-S3 provider selection plus optional route-policy mapping under `IntegratedS3:ReferenceHost`, and the reference-host guide now documents SigV4/authz composition, presign credential selection/custom-resolver overrides, and the supported route-policy limitations around SigV4-authenticated requests.
  - `WebUiApplication.ConfigurePipeline(WebApplication app)` now resolves `IntegratedS3EndpointConfigurationOptions` from DI and maps them through `MapIntegratedS3Endpoints(IEndpointRouteBuilder, IntegratedS3EndpointConfigurationOptions)`, while `IntegratedS3EndpointRouteBuilderExtensions` routes both configuration-bound and callback-driven registration through the same shared core helper so bindable endpoint defaults stay aligned with the minimal host and isolated tests.
  - CI automation now lives in `.github\workflows\trackh-publish-aot-ci.yml`, and `eng\Invoke-AotPublishValidation.ps1` enforces the current self-contained publish warning posture without depending on exact line numbers.
  - `IntegratedS3.Abstractions.Observability.IntegratedS3Observability` now exposes the shared activity-source, meter, metric-name, tag-name, and correlation-header contract for hosts, while `IntegratedS3.AspNetCore` and `IntegratedS3.Core` emit structured logs, traces, metrics, provider tags, auth-failure signals, and replica/backlog/backend-health visibility on the shipped request/auth/orchestration paths. `IntegratedS3AspNetCoreTelemetry` now also records per-endpoint HTTP request count and duration histograms (`integrateds3.http.request.count`, `integrateds3.http.request.duration`) with `http.request.method`, `integrateds3.operation`, and `http.response.status_code` tags. Both S3-compatible dispatch methods (`ExecuteS3CompatibleBucketRequestAsync`, `ExecuteS3CompatibleObjectRequestAsync`) and native MinimalAPI bucket/object endpoints now emit structured `LogDebug` entry logs, `LogWarning` for 4xx+ responses, `LogError` for unhandled exceptions, and metric recording at the dispatch chokepoint.
  - `IntegratedS3.Protocol.Internal.ProtocolTelemetry` now instruments error paths in the Protocol layer: every `S3XmlRequestReader` parse method records `integrateds3.protocol.xml.parse_errors` counter + Activity event on `FormatException`/`XmlException`; `S3SigV4aSigner.DeriveEcdsaKey` and `VerifySignature` record `integrateds3.protocol.signature.errors` counter + Activity event on key-derivation failure and malformed signature hex. Happy paths remain untouched.
  - `IntegratedS3.Provider.S3` now has full observability: `S3StorageTelemetry` (static helper) emits `integrateds3.s3.operation.duration` histogram and `integrateds3.s3.operation.errors` counter with operation/provider/result/error tags via the shared `IntegratedS3` Meter, plus `Activity` spans (kind=Client) named `IntegratedS3.S3.{OperationName}` via the shared `ActivitySource`. `AwsS3StorageClient` instruments the 12 core AWS SDK call sites (CreateBucket, HeadBucket, DeleteBucket, ListObjects, ListObjectVersions, HeadObject, GetObject, PutObject, DeleteObject, CopyObject, InitiateMultipartUpload, UploadPart, CompleteMultipartUpload) with per-call `Stopwatch` latency, `LogDebug` start/complete, `LogWarning` for `AmazonS3Exception` (error code + message), `LogError` for unexpected exceptions, and success/failure metric recording. `S3StorageService` emits `LogDebug` routing traces for key operations. Both classes accept optional `ILogger` via DI; the DI registration in `S3StorageServiceCollectionExtensions` now resolves loggers from `ILoggerFactory`.
  - `docs/observability.md` now documents the supported host integration path, including the shared `IntegratedS3` source/meter names, correlation-header behavior, backlog/health visibility, and the current limitation that dedicated health endpoints remain host-owned.
  - `src\IntegratedS3\IntegratedS3.Benchmarks` now benchmarks the Track H hot paths across disk-backed service flows, write-through mirrored writes, loopback HTTP `HEAD` / `GET` / `PUT` / multipart `UploadPart` / `LIST` / SigV4 auth flows, first-party presign generation, and a repo-local AWS SDK path-style client-comparison slice (`GetObjectMetadata`, `PutObject`, `UploadPart`, `GetObject`, and `ListObjectsV2`) against the loopback S3-compatible endpoint; the checked-in baseline now covers 19 scenarios and is refreshed by `eng\Invoke-HotPathBenchmarkBaselines.ps1` under `docs\benchmarks`.
  - `docs\performance-benchmarks.md` now documents the supported benchmark workflow, metric model, provider-breakdown strategy, and the currently supported limitations for repo-local baseline refreshes, including the broader metadata-lookup plus request/response-body client-comparison slice.
  - `IntegratedS3.Tests` now verifies benchmark scenario coverage plus the summary/report generation path without requiring the full benchmark suite to run inside `dotnet test`, including the AWS SDK client-comparison scenario registrations.
  - `IntegratedS3.Client` now re-presigns follow-on `GET` requests whenever resume recovery abandons a misaligned `206` or mismatched `416` and restarts from zero, so preferred access mode plus version pinning remain stable across proxy, direct, and delegated fallback.
  - `IntegratedS3.Core` orchestration now coalesces superseded same-target repair entries only after the newer repair is durably recorded or dispatched, limits cross-version coalescing to non-versioned buckets, and focused orchestration tests now cover repeated same-key async/write-through backlog saturation plus active-topology coalescing behavior.
  - `S3CompatibleEndpointConformanceTests` now add gated real-endpoint coverage for historical `CopyObject` and `UploadPartCopy` after a current delete marker with requested `SHA256` plus `AES256` SSE, plus direct presigned `PUT` after a delete marker so historical reads and version listing remain correct.
- Verification status (July 2025):
  - `dotnet build src\IntegratedS3\IntegratedS3.slnx` — 0 errors, 0 warnings
  - `dotnet test src\IntegratedS3\IntegratedS3.slnx` — **944 tests passing** (up from 855 baseline)
  - All production code AOT/trimming compatible
  - The recently completed implementation wave landed 13 bucket configuration subresource families, PutObjectRetention/PutObjectLegalHold write paths, SelectObjectContent/RestoreObject S3 passthrough, PostObject browser form upload, storage class headers, conditional writes, multi-range requests, checksum-mode support, CRC64NVME recognition, SSE bucket-key-enabled, XML namespace parity, encoding-type=url consistency, BucketNotEmpty for versioned buckets, ACL expansion, bucket policy expansion, multipart replication, and SigV4a detection — all wired end-to-end across the full stack
  - Previous verification baseline: `dotnet build` passed with no IL warnings; `eng\Invoke-AotPublishValidation.ps1` passed with `0` IL2026/IL3050 warnings; `dotnet run --configuration Release --project src\IntegratedS3\IntegratedS3.Benchmarks\IntegratedS3.Benchmarks.csproj -- --list-scenarios` confirmed 19-scenario benchmark catalog
  - `src\IntegratedS3\WebUi` now wires the OpenTelemetry SDK (`OpenTelemetry.Extensions.Hosting`, `OpenTelemetry.Exporter.OpenTelemetryProtocol`, `OpenTelemetry.Instrumentation.AspNetCore`, `OpenTelemetry.Instrumentation.Http`) so the existing `IntegratedS3` ActivitySource and Meter signals plus ASP.NET Core and HttpClient instrumentation are collected and optionally exported via OTLP when `OpenTelemetry:OtlpEndpoint` is configured. All packages are AOT-compatible; `eng\Invoke-AotPublishValidation.ps1` passes with 0 IL2026/IL3050 warnings.
- Remaining scope:
  - extend the new provider contract harness beyond the current bucket/object/versioning/CORS/tags/copy/multipart/state-store/delete-marker-pagination coverage into the remaining protocol edge cases and broader client-compatibility scenarios
  - extend fault-injection beyond the current unhealthy-provider, async-replication/backlog, partial-write-through, and newly added multi-replica replay coverage into broader repair/reconciliation scenarios
  - extend conformance beyond the current historical conditional-read/presign pinning, same-key historical self-copy, delete-marker-aware versioned copy, presigned-expiry/clock-skew, and AWS SDK version-id coverage into the remaining protocol edge cases and broader client-compatibility scenarios
  - rerun the landed local checksum plus version-targeted versioning/presign/copy conformance slices against configured real endpoints and broaden them beyond the current `SHA1` / `CRC32C` multipart/copy plus historical read/copy coverage as additional algorithms/providers land
  - extend fault-injection beyond the current unhealthy-provider, async-replication/backlog, partial-write-through, multi-replica replay, bucket-versioning/CORS/tag-delete repair, and write-through bucket-CORS delete coverage into backlog-saturation, topology-change, and other durable repair/reconciliation scenarios
  - extend conformance beyond the current versioned-read/copy, presigned-expiry/clock-skew, and AWS SDK version-id metadata/read/copy coverage into the remaining protocol edge cases and broader client-compatibility scenarios
  - extend fault-injection beyond the current unhealthy-provider, async-replication/backlog, partial-write-through, and newly added multi-replica replay coverage into broader repair/reconciliation scenarios
  - benchmark the hot paths called out in this plan and track throughput, latency, allocation, and provider-breakdown baselines
  - expand the benchmark suite beyond the current disk, mirrored-disk, loopback HTTP, AWS SDK path-style client-comparison, and first-party presign baseline set into reproducible native-S3 and broader client-comparison scenarios
  - keep the new trimming/AOT publish automation in CI aligned with the supported host surface and preserve the current zero-warning endpoint-map posture alongside benchmark baselines
  - add the planned MVC/Razor and Blazor WebAssembly sample consumers
  - continue package polish with versioned protocol compatibility guidance and any analyzers/diagnostics worth shipping
- Next recommended steps:
  - keep the zero-warning rebuild-backed publish validation running in CI so future `IntegratedS3.AspNetCore` / `WebUi` changes do not reintroduce IL2026/IL3050 regressions
  - rerun the expanded local S3-compatible conformance suite against configured real endpoints, especially the new direct historical `GET`, direct `PUT`, and `ListObjectVersions` marker-pagination slices
  - extend Track H fault-injection beyond the current missing-primary replay cleanup/current-topology targeting coverage into backlog-saturation, topology-change, and broader durable repair/reconciliation scenarios
  - expand the benchmark suite beyond the current 19-scenario disk/loopback/AWS-SDK baseline into reproducible native-S3 and broader client-comparison coverage, then keep the checked-in reports current
  - continue broader post-resume client ergonomics, composite-checksum, and direct-vs-delegated follow-ons now that the resume range/rollback guards are in place
  - finish package polish items such as versioned protocol compatibility guidance and any analyzers/diagnostics worth shipping (XML docs, onboarding docs, startup validation, and NuGet metadata are now complete)
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

- `src/IntegratedS3/WebUi.MvcRazor/`
  - MVC/Razor sample consumer showing direct `IStorageService` usage plus the HTTP surface

- `src/IntegratedS3/WebUi.BlazorWasm/`
  - hosted Blazor WebAssembly sample server that maps the IntegratedS3 endpoints and serves the browser client

- `src/IntegratedS3/WebUi.BlazorWasm.Client/`
  - browser-side sample client using `IntegratedS3.Client` presign helpers alongside JSON endpoint calls

- `docs/web-consumer-samples.md`
  - run guide and scope notes for the additional MVC/Razor and Blazor WebAssembly samples

- `src/IntegratedS3/WebUi/appsettings.json`
  - should evolve into provider/auth/endpoint examples

- `src/IntegratedS3/IntegratedS3.slnx`
  - should include all package, test, and benchmark projects

- `src/IntegratedS3/IntegratedS3.Benchmarks/Program.cs`
  - benchmark entrypoint plus scenario catalog and report emission
  - exercises the public service and ASP.NET integration surfaces for hot-path baselines

- `eng/Invoke-HotPathBenchmarkBaselines.ps1`
  - refreshes the committed JSON / Markdown benchmark baselines
  - wraps the benchmark project with release-mode defaults and scenario filtering

- `docs/performance-benchmarks.md`
  - supported benchmark workflow and metric model
  - documents the current benchmark-specific limitations and provider-breakdown behavior

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

Status note (July 2025): current Track H validation covers checklist items 5, 6, 8, 9, 10, and 11 at the command/test level; item 5 now publishes the current `WebUi` host without IL2026/IL3050 warnings, item 6 now includes SSE-C customer-provided-key header parsing, validation, provider delegation, 13 bucket configuration subresource families wired end-to-end, PutObjectRetention/PutObjectLegalHold write paths, SelectObjectContent/RestoreObject passthrough, PostObject browser form upload, conditional writes, multi-range requests, checksum-mode gating, CRC64NVME recognition, SSE bucket-key-enabled, XML namespace parity, encoding-type=url consistency, expanded ACL/policy support (public-read-write, authenticated-read, Deny, conditions, CIDR matching, wildcards), multipart replication, and explicit SigV4a rejection, and item 9 now has a checked-in 19-scenario hot-path baseline. Current test baseline: **901 tests**, 0 publish warnings.

## Key Decisions

- **Package shape:** modular packages first
- **Preferred browser model:** presigned URL hybrid
- **Goal:** broad S3-compatible endpoint support tracked through milestones
- **Important boundary:** “full support” must be represented by a capability matrix
- **Provider implementation order:** disk first, native S3 second
- **Current `WebUi` role:** sample/reference host, not final architecture container

## Recommended Next Execution Slices

The recently completed implementation wave landed a massive amount of new surface area: 13 bucket configuration subresource families, object-level retention/legal-hold write paths, SelectObjectContent/RestoreObject S3 passthrough, PostObject browser form upload, storage class headers, conditional writes, multi-range requests, checksum-mode support, CRC64NVME recognition, SSE bucket-key-enabled, XML namespace parity, encoding-type=url consistency, BucketNotEmpty for versioned buckets, expanded ACL/policy support, bucket policy expansion (Deny/conditions/CIDR/wildcards), multipart replication, and full SigV4a authentication (ECDSA P-256 header/presigned/streaming-trailer) — all end-to-end across the full stack with 936 passing tests and 0 warnings. SigV4a is no longer a blocked item.

The next realistic move is to shift fully toward hardening, conformance, and real-endpoint validation now that the core surface area is comprehensive:

Why these should come next:

- the implemented surface now covers the vast majority of S3-compatible bucket/object operations, so remaining work is hardening and edge-case coverage rather than new foundational feature work
- they still touch different primary packages (`IntegratedS3.Provider.S3`, `IntegratedS3.AspNetCore`, `IntegratedS3.Protocol`, `IntegratedS3.Client`, `IntegratedS3.Tests`) so multiple contributors can move without colliding constantly
- protocol-fidelity hardening and conformance work can proceed in parallel with provider hardening and will immediately validate the current surface area
- the SigV4a blocker is now resolved — full ECDSA P-256 header/presigned/streaming-trailer authentication is implemented and tested

Recommended dependency-aware order:

1. ~~**SigV4a full support**~~ — **done**: ECDSA P-256 header-based and presigned-query authentication, region-set validation (wildcard and specific regions), streaming `aws-chunked` trailer signature verification, and 9 new conformance tests
2. execute Track H to expand fault-injection coverage, broaden benchmarks beyond the current 19-scenario disk/loopback/AWS-SDK baseline, and keep publish/AOT verification at the zero-warning posture
3. execute Track B in parallel to rerun and widen real-endpoint validation against MinIO/LocalStack-style endpoints, especially bucket configuration subresources, the newer object-level operations, and remaining checksum/SSE cases
4. continue Track F in parallel to harden multipart replication edge cases and expand health/admin visibility now that multipart operations participate in replication
5. continue Track C in parallel with broader post-resume client ergonomics and composite-checksum UX
6. revisit Track D follow-up only if consumer feedback warrants more backend-registration sugar
7. reuse the native-S3-provider-backed AWS SDK loopback harness opportunistically for future encryption/client parity

## Suggested First Parallel Batch

Given the current implementation state, the first parallel batch should be:

1. ~~**SigV4a investigation**~~ — **done**: full SigV4a authentication is implemented using .NET built-in `ECDsa`/`HMACSHA256` with NIST SP 800-108 KDF key derivation, 3-part credential scope, region-set validation, and streaming trailer verification
2. Track H — extend fault-injection coverage, validate the newly landed 46 tests against broader edge cases, broaden benchmarks, and keep publish/AOT verification at zero-warning posture
3. Track B — rerun and broaden local S3-compatible endpoint validation for the 13 new bucket configuration subresources, object-level retention/legal-hold, SelectObjectContent/RestoreObject, and PostObject against configured real endpoints
4. Track E / Track G follow-on — harden remaining protocol fidelity gaps around the newly expanded bucket subresource surface, conditional-write edge cases, multi-range efficiency, and PostObject signature validation
5. Track C follow-up — validate broader composite-checksum, live-endpoint, and discovery-helper ergonomics

### March 2026 audit-backed GitHub issue backlog

A March 2026 audit of code, tests, and docs created GitHub issues [#1](https://github.com/SymoHTL/Intergrated-S3/issues/1) through [#40](https://github.com/SymoHTL/Intergrated-S3/issues/40) to track every currently validated missing/problematic S3 and downstream feature. The audits re-ran `dotnet test src\IntegratedS3\IntegratedS3.slnx` and `dotnet publish -c Release --self-contained src\IntegratedS3\WebUi\WebUi.csproj`; tests passed, publish succeeded, and the publish output still surfaced 12 AOT/trimming warnings now tracked in [#15](https://github.com/SymoHTL/Intergrated-S3/issues/15).

- Track B:
  - [#20](https://github.com/SymoHTL/Intergrated-S3/issues/20) backend-direct native S3 presign support
  - [#21](https://github.com/SymoHTL/Intergrated-S3/issues/21) local S3-compatible endpoint hardening
- Track C:
  - [#1](https://github.com/SymoHTL/Intergrated-S3/issues/1) first-party client direct/delegated default behavior
  - [#2](https://github.com/SymoHTL/Intergrated-S3/issues/2) `AddIntegratedS3Client` / `IHttpClientFactory` integration
  - [#3](https://github.com/SymoHTL/Intergrated-S3/issues/3) larger-object and checksum-aware transfer hardening
- Track D:
  - [#4](https://github.com/SymoHTL/Intergrated-S3/issues/4) custom backend DI sugar
  - [#5](https://github.com/SymoHTL/Intergrated-S3/issues/5) configuration-bound authorization conventions
  - [#6](https://github.com/SymoHTL/Intergrated-S3/issues/6) future endpoint route-group callback behavior
- Track E:
  - [#22](https://github.com/SymoHTL/Intergrated-S3/issues/22) multipart-listing parity
  - [#23](https://github.com/SymoHTL/Intergrated-S3/issues/23) conditional/checksum/canonical-request hardening
  - [#24](https://github.com/SymoHTL/Intergrated-S3/issues/24) `aws-chunked`, presigned-query, and virtual-hosted-style compatibility
  - [#25](https://github.com/SymoHTL/Intergrated-S3/issues/25) versioning/tagging/delete-marker parity
  - [#31](https://github.com/SymoHTL/Intergrated-S3/issues/31) bucket/object subresource matrix hardening
  - [#32](https://github.com/SymoHTL/Intergrated-S3/issues/32) canonical S3 XML namespace parity
  - [#33](https://github.com/SymoHTL/Intergrated-S3/issues/33) `BucketNotEmpty` delete-bucket semantics
  - [#34](https://github.com/SymoHTL/Intergrated-S3/issues/34) multi-delete integrity and object-count limits
  - [#35](https://github.com/SymoHTL/Intergrated-S3/issues/35) object and multipart list API parity
  - [#36](https://github.com/SymoHTL/Intergrated-S3/issues/36) `ListParts`
  - [#37](https://github.com/SymoHTL/Intergrated-S3/issues/37) `UploadPartCopy`
  - [#38](https://github.com/SymoHTL/Intergrated-S3/issues/38) standard object header parity
  - [#39](https://github.com/SymoHTL/Intergrated-S3/issues/39) upload/copy/multipart tagging-header parity
- Track F:
  - [#7](https://github.com/SymoHTL/Intergrated-S3/issues/7) admin health, lag, and repair diagnostics
  - [#8](https://github.com/SymoHTL/Intergrated-S3/issues/8) ASP.NET Core health-check integration
  - [#9](https://github.com/SymoHTL/Intergrated-S3/issues/9) broader multi-provider fault-injection coverage
  - [#10](https://github.com/SymoHTL/Intergrated-S3/issues/10) multipart support for replicated write modes
  - [#11](https://github.com/SymoHTL/Intergrated-S3/issues/11) optional maintenance/replay host integrations
  - [#12](https://github.com/SymoHTL/Intergrated-S3/issues/12) richer reconciliation and divergence-repair semantics
- Track G:
  - extend conformance and protocol hardening into the remaining delete-marker/versioning, checksum-override, and `aws-chunked` edge cases now that the current subresource/presign, conditional-precedence, and raw-query SigV4 gaps are covered
  - [#27](https://github.com/SymoHTL/Intergrated-S3/issues/27) broader server-side-encryption variants
  - [#28](https://github.com/SymoHTL/Intergrated-S3/issues/28) remaining S3/browser CORS conformance hardening
  - [#29](https://github.com/SymoHTL/Intergrated-S3/issues/29) object lock, retention, and legal hold
  - [#40](https://github.com/SymoHTL/Intergrated-S3/issues/40) next bucket-subresource surface expansion
- Track H:
  - [#13](https://github.com/SymoHTL/Intergrated-S3/issues/13) MVC/Razor and Blazor WebAssembly sample consumers
  - [#14](https://github.com/SymoHTL/Intergrated-S3/issues/14) benchmark baselines
  - [#15](https://github.com/SymoHTL/Intergrated-S3/issues/15) checked-in CI publish validation plus AOT warning posture
  - [#16](https://github.com/SymoHTL/Intergrated-S3/issues/16) `IntegratedS3.Testing` contract harness
  - [#17](https://github.com/SymoHTL/Intergrated-S3/issues/17) structured observability
  - [#18](https://github.com/SymoHTL/Intergrated-S3/issues/18) package polish and onboarding docs
  - [#19](https://github.com/SymoHTL/Intergrated-S3/issues/19) secure reference-host composition docs
  - [#30](https://github.com/SymoHTL/Intergrated-S3/issues/30) broader protocol/client conformance

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
   - Scope: expand the reusable provider contract harness plus local S3-compatible integration/fault-injection coverage around the already-landed provider/protocol slices
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

### Provider-level hardening (completed)

The following provider-level features were implemented in a single pass:

1. **bucket-not-empty-versioned** — DiskStorageService.DeleteBucketAsync now explicitly checks the version store directory for archived versions/delete markers and the multipart uploads directory for in-progress uploads, rather than relying solely on top-level file system entries. Empty system directories (.integrateds3-versions, .integrateds3-multipart) are cleaned up automatically so they do not prevent deletion of genuinely empty buckets.

2. **conditional-writes** — PutObjectRequest now exposes `IfMatchETag` and `IfNoneMatchETag` properties. The disk provider evaluates `If-None-Match: *` (prevent overwrite) and `If-Match: <etag>` (optimistic concurrency) before writing. The S3 provider passes both conditions through to the AWS SDK's `PutObjectRequest.IfMatch`/`IfNoneMatch` properties. The endpoint layer parses the standard HTTP `If-Match` and `If-None-Match` headers on PUT and passes them to the storage service.

3. **storage-class-headers** — `StorageClass` property added to `PutObjectRequest`, `CopyObjectRequest`, and `InitiateMultipartUploadRequest`. The endpoint layer parses `x-amz-storage-class` on PUT, COPY, and InitiateMultipartUpload. The S3 provider maps the value to `S3StorageClass` on the AWS SDK requests. The disk provider ignores the value (no storage class concept).

4. **sse-remaining** — `ObjectServerSideEncryptionInfo` now includes a `BucketKeyEnabled` property. The S3 provider populates it from AWS SDK responses on GET, HEAD, PUT, COPY, InitiateMultipart, and CompleteMultipart. The endpoint layer emits the `x-amz-server-side-encryption-bucket-key-enabled` response header when the flag is set.

5. **crc64nvme** — CRC64NVME is now recognized as a valid checksum algorithm throughout the disk provider pipeline (`TryNormalizeChecksumAlgorithm`, `ValidateRequestedChecksums`, multipart upload initiation). Since no .NET CRC64NVME implementation is available, the disk provider accepts client-provided CRC64NVME values as pass-through without server-side validation. The endpoint layer emits the `x-amz-checksum-crc64nvme` response header. The S3 provider already had full CRC64NVME support via the AWS SDK.
