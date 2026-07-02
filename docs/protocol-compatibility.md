# IntegratedS3 protocol compatibility guidance

IntegratedS3 exposes a versioned package surface and an evolving S3-compatible HTTP surface. This document describes what compatibility means today and how consumers should reason about upgrades.

## Version alignment guidance

Use matching versions of the `IntegratedS3.*` packages within a host or client. The packages are developed from a single solution and are intended to move together. Until a separate protocol-negotiation mechanism exists, aligned package versions plus runtime capability metadata are the supported compatibility contract.

When planning upgrades:

- treat the overall HTTP and orchestration surface as package-versioned
- keep server and first-party client versions aligned where practical
- use the capability and provider-descriptor endpoints to discover what a specific deployment supports at runtime

## Current signing and addressing baseline

The current compatibility baseline is:

- AWS Signature Version 4 request authentication and presign flows
- path-style and configurable virtual-hosted-style request interpretation in the ASP.NET host
- S3-style XML responses for the supported compatibility routes

The platform is intentionally S3-compatible rather than a promise of full wire-level parity with every AWS S3 feature. Consumers should treat the capability surface as additive and verify required features explicitly.

## Supported compatibility surface

The current host and provider stack support the following S3-oriented areas on the supported route surface:

- bucket CRUD
- object CRUD plus metadata headers
- list-objects-v2
- list-object-versions
- multipart upload initiate, upload-part, complete, abort, and bucket-level upload listing
- object tagging
- bucket versioning configuration
- bucket CORS configuration
- copy-object behavior
- batch delete
- presigned object `GET` / `PUT` / `DELETE` / `HEAD` and presigned multipart `UploadPart`
- checksums on the supported put, copy, and multipart flows

For the current HTTP view of a deployment, use:

- `GET /integrated-s3` for the service document
- `GET /integrated-s3/capabilities` for runtime capability metadata
- the provider descriptors surfaced by the JSON service document for provider mode, support-state ownership, and object-location access shape

## Provider capability matrix

Not every operation on the compatibility surface is supported by every provider. Operations a provider does not support fail explicitly with an S3-style `NotImplemented` error (HTTP `501`) rather than silently degrading. The tables below reflect the current implementations of `IntegratedS3.Provider.Disk`, `IntegratedS3.Provider.S3`, and the default behavior a custom `IStorageBackend` inherits when it does not override the corresponding member.

Legend:

- **Supported** — implemented by the provider.
- **Config only** — the configuration document is validated, persisted, and served back, but the provider does not act on it (no rules are executed).
- **`NotImplemented`** — the request fails with the S3 `NotImplemented` error (HTTP `501`).
- **Required** — the member is abstract on `IStorageBackend`; custom providers must implement it.
- **Default `NotImplemented`** — `IStorageBackend` ships a default implementation that returns an unsupported-capability error; custom providers get `NotImplemented` until they override it.

### Object and data operations

| Operation | Disk provider | S3 provider | Custom backend default |
|---|---|---|---|
| Object CRUD (`GET`/`HEAD`/`PUT`/`DELETE`), list-objects-v2, list-object-versions | Supported | Supported (native) | Required |
| CopyObject | Supported | Supported (native) | Required |
| Object tagging (`?tagging`) | Supported | Supported (native) | Required |
| Batch delete (`POST ?delete`) | Supported | Supported (native) | Required (built on object delete) |
| Multipart initiate / upload-part / complete / abort | Supported | Supported (native) | Required |
| UploadPartCopy | Supported | Supported (native) | Default `NotImplemented` |
| ListParts (`GET ?uploadId=`) and list multipart uploads (`?uploads`) | Supported | Supported (native) | Default `NotImplemented` |
| GetObjectAttributes (`?attributes`) | Supported | Supported (native) | Default `NotImplemented` |
| Checksums (`x-amz-checksum-*`, `x-amz-sdk-checksum-algorithm`) | Supported (CRC64NVME accepted pass-through) | Supported (native) | Backend-specific (values forwarded on requests) |
| Server-side encryption (managed `AES256` / `aws:kms` / `aws:kms:dsse` and SSE-C headers) | `NotImplemented` (all SSE requests rejected) | Supported (native) | Backend-specific (settings forwarded; implement or reject) |
| Per-object retention (`?retention` `GET`/`PUT`) | `NotImplemented` | Supported (native) | Default `NotImplemented` |
| Per-object legal hold (`?legal-hold` `GET`/`PUT`) | `NotImplemented` | Supported (native) | Default `NotImplemented` |
| RestoreObject (`POST ?restore`) | `NotImplemented` | Supported (native) | Default `NotImplemented` |
| SelectObjectContent (`POST ?select&select-type=2`) | `NotImplemented` | Supported (native) | Default `NotImplemented` |

### Bucket operations and subresources

| Operation | Disk provider | S3 provider | Custom backend default |
|---|---|---|---|
| Bucket CRUD (`PUT`/`HEAD`/`DELETE`, list buckets) | Supported | Supported (native) | Required |
| Versioning configuration (`?versioning`) | Supported (versioned stores and delete markers) | Supported (native) | Required |
| Bucket CORS (`?cors`) | Supported | Supported (native) | Default `NotImplemented` |
| Bucket location (`?location`) | Supported | Supported (native) | Default `NotImplemented` |
| Bucket tagging (`?tagging`) | Supported | Supported (native) | Default `NotImplemented` |
| Object Lock configuration (`?object-lock`) | Supported — configuration is persisted and bucket default retention is enforced (in-window permanent version deletes are rejected) | Supported (native) | Default `NotImplemented` |
| Bucket default encryption (`?encryption`) | `NotImplemented` | Supported (native) | Default `NotImplemented` |
| Lifecycle (`?lifecycle`) | Config only — rules are stored and served, never executed | Supported (native; the backing service applies rules) | Default `NotImplemented` |
| Replication (`?replication`) | Config only — no replication is performed by the provider | Supported (native) | Default `NotImplemented` |
| Website (`?website`) | Config only — no website hosting is served | Supported (native) | Default `NotImplemented` |
| Logging (`?logging`) | Config only — no access logs are produced | Supported (native) | Default `NotImplemented` |
| Notification (`?notification`) | Config only — no events are published | Supported (native) | Default `NotImplemented` |
| Request payment (`?requestPayment`) / accelerate (`?accelerate`) | Config only — no behavioral effect | Supported (native) | Default `NotImplemented` |
| Analytics / metrics / inventory / intelligent-tiering configurations (`?analytics`, `?metrics`, `?inventory`, `?intelligent-tiering` with `&id=`) | Config only — no reports or tiering transitions are generated | Supported (native) | Default `NotImplemented` |

Multi-backend replication across providers is a separate `IntegratedS3.Core` orchestration feature (primary/replica topologies) and is unrelated to the S3 `?replication` bucket subresource above.

### Host-managed behavior (provider-independent)

Some compatibility behavior is implemented by the ASP.NET Core host layer and works the same way for every provider:

- bucket policy (`?policy`) storage and evaluation, including principal, action, resource, and condition evaluation at authorization time; `PUT ?policy` documents that contain `Condition` blocks are currently rejected as `NotImplemented` on the HTTP surface
- object and bucket ACLs (`?acl`) for the supported canned-ACL and `AccessControlPolicy` permutations; unsupported grant-header permutations return `NotImplemented`
- presigned URL issuance and validation for `GetObject`, `PutObject`, `DeleteObject`, `HeadObject`, and multipart `UploadPart` (SigV4 and SigV4a)
- unrecognized or unsupported subresources (for example `?ownershipControls`) and unsupported method/subresource combinations return `NotImplemented`
- the `x-amz-server-side-encryption-bucket-key-enabled` request header is rejected as `NotImplemented` before reaching any provider

## Provider-mode and feature guidance

Capability support varies by provider and orchestration strategy:

- `IntegratedS3.Provider.S3` prefers native provider behavior where the AWS SDK exposes it directly.
- `IntegratedS3.Provider.Disk` emulates the supported S3-compatible feature set on local disk, including versioning, tagging, checksums, multipart uploads, bucket CORS, and Object Lock default-retention enforcement. Operations marked `NotImplemented` or config-only in the matrix above are the intentional disk-provider gaps.
- `IntegratedS3.Core` can combine providers, but the HTTP surface should still be treated as capability-driven rather than assumed from package presence alone.

Provider mode and object-location descriptors help explain how a deployment behaves:

- `Managed` means IntegratedS3 owns the behavior directly.
- `Delegated` or delegated object access means the deployment passes through provider-managed access.
- `Passthrough` and `Hybrid` indicate deployments that mix direct provider behavior with IntegratedS3 orchestration.

## Access-mode guidance for presigned URLs

Presign responses are also versioned behavior. Current guidance:

- omit a preferred access mode to stay on the default proxy path
- request `Direct` only when the deployment advertises direct object locations or the primary backend can mint direct grants
- request `Delegated` for provider-managed presigned downloads when the deployment supports them
- expect the server to fall back to proxy mode if the requested access mode is unavailable

## Upgrade expectations

When upgrading across package versions:

- review the release notes and the implementation plan for newly added compatibility slices
- validate any client SDK assumptions against the current capability endpoint
- rerun the repository build, test, and publish validation commands if you are modifying or extending the protocol surface yourself

If you need a feature that is not yet listed, treat it as unsupported rather than inferred from neighboring S3 behavior.
