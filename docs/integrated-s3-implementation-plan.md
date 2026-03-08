# Integrated S3 Implementation Plan

Build a modular, AOT-conscious ASP.NET storage platform that exposes an S3-compatible HTTP surface, supports pluggable backends (`S3`, disk, future providers), uses `ClaimsPrincipal`-driven authorization, and ships as reusable NuGet packages. The recommended approach is to separate protocol compatibility, storage orchestration, provider implementations, and ASP.NET integration so the system can grow toward broad S3 parity without becoming tightly coupled or hard to optimize.

## Current Implementation Status (March 2026)

The repository has already moved beyond initial scaffolding and now contains a working vertical slice.

### Implemented so far

- modular project structure under `src/IntegratedS3/`
- initial abstractions package with:
  - canonical error/result model
  - capability descriptors
  - async storage contracts
  - backend abstraction via `IStorageBackend`
  - request models for range, conditional, pagination, and copy-object flows
- `IntegratedS3.Core` orchestration layer with:
  - `IStorageService` orchestration over registered backends
  - provider selection through registered backends
  - first multi-backend consistency primitive via `PrimaryOnly` and `WriteThroughAll`
  - first read-routing policies via `PrimaryOnly`, `PreferPrimary`, and `PreferHealthyReplica`
  - overrideable backend-health evaluation via `IStorageBackendHealthEvaluator`
  - catalog synchronization hooks for bucket/object operations
  - first-class copy orchestration over registered backends
  - storage-aware authorization via an `IStorageService` decorator over orchestration
- default `IntegratedS3.Core` registration with an overrideable `IStorageCatalogStore`
- default `IntegratedS3.Core` authorization registration with:
  - an allow-by-default `IIntegratedS3AuthorizationService`
  - an ambient `IIntegratedS3RequestContextAccessor`
- extracted optional EF-backed catalog persistence in `IntegratedS3.EntityFramework` with:
  - consumer-owned `DbContext` registration
  - catalog model mapping helpers
  - generic EF-backed `IStorageCatalogStore` registration
- `IntegratedS3.Provider.Disk` backend with:
  - bucket CRUD
  - object CRUD
  - lexicographic object listing with continuation-token pagination
  - range reads
  - ETag/date-based conditional reads
  - copy operations with source preconditions
  - streaming reads/writes
  - sidecar metadata persistence
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
  - object endpoints with metadata-header round-tripping, pagination, range, conditional, and copy behavior
  - S3-compatible XML list-buckets, list-objects-v2, batch-delete, error, and copy-object responses for the currently supported surface area
  - `ListObjectsV2` delimiter/common-prefix and `start-after` handling for S3-compatible hierarchical listings
  - AWS Signature Version 4 request authentication for both authorization-header and presigned-query request flows when enabled
  - optional virtual-hosted-style request routing when enabled via configuration
  - conditional authentication middleware activation when auth services are registered
- automated tests covering:
  - bootstrap registration
  - disk provider behavior including pagination, range, conditional requests, and copy operations
  - HTTP integration behavior including metadata headers, pagination, range, conditional requests, copy operations, S3-compatible XML list/delete behavior, `ListObjectsV2` delimiter/common-prefix and `start-after` flows, and virtual-hosted-style routing
  - Core orchestration behavior including catalog synchronization for copied objects and mirrored write-through replication
  - Core read routing behavior for unhealthy primaries, replica-preferred reads, and provider-unavailable read failover
  - ClaimsPrincipal-driven authorization behavior in both Core and HTTP flows
  - SigV4 header authentication, presigned-query authentication, and invalid-signature handling on the HTTP surface
  - AWS SDK compatibility behavior for path-style and virtual-hosted-style CRUD/list flows, root bucket listing, delimiter/start-after object listing, plus presigned URL and copy/conditional coverage
  - overrideability of the catalog persistence abstraction

### Important current architectural decision

`IntegratedS3.Core` should remain **persistence-agnostic by default**. Applications that want catalog/metadata persistence can either:

- register their own `IStorageCatalogStore`, or
- opt into the extracted `IntegratedS3.EntityFramework` package by supplying their own `DbContext` type through a generic registration such as `AddEntityFrameworkStorageCatalog<TDbContext>(...)`

That keeps Core from owning an internal application database model or forcing a specific provider such as SQLite into every consumer.

Another important architectural decision now in place is that service/provider descriptors and reported capabilities should come from registered backends/Core rather than from sample-host manual composition. `IntegratedS3Options` still provides fallback metadata for bootstrap-only scenarios, but the default runtime path now reflects actual backend registrations.

Another important architectural decision now in place is that authorization is separated from orchestration. `IStorageService` resolves to an authorizing decorator that wraps `OrchestratedStorageService`, which keeps bucket/key/version-aware authorization in Core without repeating authorization logic inside the orchestration implementation itself.

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

- **define provider modes explicitly**

  - document and support at least these provider shapes:
    - **managed provider**: IntegratedS3 owns most behavior and may emulate missing features locally
    - **delegating / passthrough provider**: IntegratedS3 forwards to an upstream S3-compatible or storage API with minimal local state
    - **hybrid provider**: object bytes live in one system while metadata, multipart state, tags, or version catalogs live in another
  - make sure provider mode is an explicit architectural concept rather than an accidental implementation detail

- **keep raw storage transport separate from auxiliary feature state**

  - avoid assuming every `IStorageBackend` can or should persist arbitrary extra metadata next to the object
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
  - ensure these services are optional and overrideable through DI
  - keep `IntegratedS3.Core` persistence-agnostic while still allowing composition with EF, custom databases, caches, or provider-native implementations

- **clarify capability reporting beyond simple local emulation**

  - revisit whether `native | emulated | unsupported` is expressive enough for long-term provider diversity
  - evaluate whether the system needs additional semantics such as:
    - delegated
    - externalized
    - proxied
  - if the enum should remain small, add another descriptor surface that explains *how* a capability is fulfilled without breaking current compatibility reporting
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
  - validate at least one future-oriented scenario such as:
    - upstream S3 passthrough provider
    - remote blob provider with external metadata catalog
    - redirect/delegated-download provider
  - make sure these scenarios can be implemented without forcing fake local filesystem semantics

- **treat disk sidecars as an implementation choice, not a platform law**

  - keep the disk backend free to use sidecars/index files where that remains pragmatic
  - avoid letting that implementation choice leak upward into shared abstractions or orchestration assumptions
  - whenever a new shared contract is introduced, explicitly ask whether it requires backend-owned persistence or can be satisfied through composition

- **sequence this work alongside the next capability slices**

  - versioning, tags, and checksums should still be the next major capability slice
  - but while doing that work, validate that the new contracts can support:
    - native implementations
    - externalized support-state implementations
    - delegated/passthrough implementations
  - do not bake disk-specific persistence assumptions into the new versioning/tag/checksum APIs

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
  - metadata/version sidecar support
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

### Required design rules

- no provider-specific SDK types should leak into public contracts
- all storage operations must be async and streaming-capable
- unsupported features must be explicit through capability reporting, not hidden or silently degraded
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

### Disk backend emulation needs

The disk provider will likely need sidecar/index support for:

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
| object metadata and headers | implemented | emulated | implemented | yes | disk uses sidecar metadata persistence and metadata-header round-tripping |
| list objects semantics | implemented | native | partially implemented | yes | S3 surface currently focuses on `ListObjectsV2`; version-aware listing is still pending |
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
| multipart upload lifecycle | not started | unsupported | not implemented | no | currently the largest protocol fidelity gap |
| object tags | not started | unsupported | not implemented | no | no shared contract or disk-side persistence yet |
| versioning | placeholder only | unsupported | not implemented | no | contracts mention version-shaped flows, but end-to-end behavior is still absent |
| checksums | not started | unsupported | not implemented | no | hash persistence/validation and header parity are both still pending |
| ACL / policy behavior | not started | unsupported | not implemented | no | authorization is `ClaimsPrincipal`-driven rather than S3 ACL compatible today |
| CORS | not started | unsupported | not implemented | no | expected to land later as explicit HTTP integration work |
| object lock / retention / legal hold | not started | unsupported | not implemented | no | needs both abstractions and provider persistence shape |
| server-side encryption variants | not started | unsupported | not implemented | no | should remain capability-driven rather than implied |

This matrix should now be treated as the authoritative status view for the current vertical slice and updated alongside each meaningful feature increment.

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
- health-aware reads, asynchronous replicas, reconciliation, and divergence repair are still pending

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
- S3-compatible root/bucket/object routing is implemented alongside the JSON convenience endpoints for the currently supported operations
- AWS Signature Version 4 request validation is implemented for authorization-header and presigned-query requests, though first-party presign generation APIs are still pending
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
- copy-object behavior is implemented through `PUT` plus `x-amz-copy-source`, and the HTTP surface now returns an S3-style XML `CopyObjectResult`
- batch delete is implemented through `POST ?delete`, and the HTTP surface now returns an S3-style XML `DeleteResult`
- S3-style XML error responses are now returned for storage endpoint failures
- SigV4 authorization-header and presigned-query request authentication are implemented for the currently supported surface area, while multipart and broader parity hardening are still pending

### Current protocol fidelity gaps worth prioritizing

The current HTTP surface is real and useful, but it still has some clearly bounded fidelity gaps that should be treated as deliberate backlog rather than invisible debt:

- multipart upload lifecycle is entirely absent across abstractions, providers, endpoints, and tests
- only the currently supported bucket subresources are implemented; unsupported S3 bucket/object subresources intentionally return `NotImplemented`
- S3-compatible bucket listing is centered on `list-type=2`; broader listing parity such as version-aware listing and additional subresource combinations is still pending
- conditional behavior is solid for the current `GET` / `HEAD` paths, but broader S3 precedence and edge-case parity still need hardening
- SigV4 validation works for the implemented routing surface, but canonical-request edge cases and parity hardening should continue before claiming wider compatibility
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

- this milestone is partially implemented
- current disk backend validates the basic abstraction shape, streaming CRUD, metadata sidecars, local orchestration/catalog persistence, paginated listing, range reads, conditional requests, and copy-object behavior
- advanced disk-emulated features such as versioning, multipart, checksums, retention, tags, and richer indexing are still pending

### 2. Native S3 provider second

Build `IntegratedS3.Provider.S3` next:

- use native S3 APIs
- preserve streaming semantics
- translate provider errors to canonical storage errors
- support presign generation
- preserve metadata/version/checksum semantics as much as possible

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
- pagination/range/conditional/copy behavior is covered in automated tests today
- AWS SDK compatibility coverage now includes virtual-hosted-style CRUD/list plus host-style presigned URL and copy/conditional flows
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

Status: **in progress / partially complete**

- disk provider exists
- local sample host exists
- direct service usage exists through `IStorageService`
- current metadata approach is sidecar-based and catalog-assisted, with paginated listing, range reads, conditional requests, and copy operations now implemented
- the disk backend is still not feature-complete for versioning/tags/checksums

### M3 — Native S3 provider and presigned URL support

- S3 provider
- presign flow
- ClaimsPrincipal authz integration
- initial client package support

Status: **in progress / partially complete**

- ClaimsPrincipal authz integration is implemented through `IIntegratedS3AuthorizationService`, an ambient request context accessor, ASP.NET request-context flow, and an authorizing `IStorageService` decorator
- SigV4 authorization-header and presigned-query request validation are implemented on the ASP.NET surface for compatibility scenarios, even though first-party presign generation is still not exposed as an application-facing feature yet
- native S3 provider implementation is still not started
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
- broader protocol fidelity such as multipart listing semantics, additional bucket/object subresources, checksum parity, and deeper edge-case compatibility is still pending

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
- background reconciliation is still not started

### M6 — Versioning, tags, and checksums

- version chains
- metadata fidelity
- checksum persistence and validation
- tag support

Status: **not started**

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

- `src/IntegratedS3/IntegratedS3.Core/Services/OrchestratedStorageService.cs`
  - current orchestration layer implementation

- `src/IntegratedS3/IntegratedS3.Abstractions/Requests/CopyObjectRequest.cs`
  - first-class copy-object contract used by Core, providers, and HTTP copy handling

- `src/IntegratedS3/IntegratedS3.Core/Services/IStorageCatalogStore.cs`
  - overrideable metadata/catalog persistence abstraction

- `src/IntegratedS3/IntegratedS3.Provider.Disk/DiskStorageService.cs`
  - current disk backend implementation including pagination, range, conditional, and copy behavior

- `src/IntegratedS3/IntegratedS3.AspNetCore/Endpoints/IntegratedS3EndpointRouteBuilderExtensions.cs`
  - current HTTP surface for service, bucket, and object operations including conditional reads, pagination, and copy-source handling

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

## Recommended Next Execution Slice

The most pragmatic next step is to make **versioning, tags, and checksum fidelity** the next primary capability slice.

Why this should come next:

- multipart scaffolding has now landed, so the next biggest application-facing fidelity gap is object lifecycle and metadata parity rather than basic transfer flow
- versioning and checksum-aware writes are natural follow-ons to multipart because they pressure the same object identity and completion boundaries
- tags and richer metadata fidelity help keep both the in-process service surface and the HTTP surface aligned before more advanced access-control features are added
- this slice also provides a clearer basis for eventual native S3-provider parity than jumping directly into ACLs, retention, or reconciliation internals

Recommended implementation sequence:

1. add provider-agnostic contracts for version identifiers, object tags, and checksum metadata in `IntegratedS3.Abstractions`
2. extend Core orchestration and catalog persistence to preserve version/tag/checksum metadata without leaking provider-specific types
3. implement a first disk-backed emulation strategy for version chains, tag sidecars, and persisted checksum metadata
4. expose the initial S3-compatible HTTP endpoints and headers for the supported subset
5. add conformance-style tests plus AWS SDK compatibility coverage for the supported versioning/tag/checksum scenarios
6. after that slice lands, choose between richer reconciliation/health behavior or the next set of advanced S3-compatible bucket and object subresources

## Suggested Next Step

Given the current implementation state, the next recommended step should be:

1. implement versioning, tags, and checksum scaffolding as the next explicit capability slice across abstractions, Core, disk, HTTP, and tests
2. while doing that work, keep hardening the remaining S3-compatible fidelity edges around multipart listing semantics, checksum headers, and unsupported subresources
3. extend backend health from static evaluation into active probes, health snapshots, and richer read failover semantics once the object-fidelity surface is less lopsided
4. document the extracted EF integration pattern further with migrations guidance and consumer-owned model examples
5. introduce reconciliation semantics for stale or divergent replicas after versioning and multipart foundations are clearer

