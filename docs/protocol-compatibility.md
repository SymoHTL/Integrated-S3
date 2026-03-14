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
- presigned object `GET` / `PUT`
- checksums on the supported put, copy, and multipart flows

For the current HTTP view of a deployment, use:

- `GET /integrated-s3` for the service document
- `GET /integrated-s3/capabilities` for runtime capability metadata
- the provider descriptors surfaced by the JSON service document for provider mode, support-state ownership, and object-location access shape

## Provider-mode and feature guidance

Capability support varies by provider and orchestration strategy:

- `IntegratedS3.Provider.S3` prefers native provider behavior where the AWS SDK exposes it directly.
- `IntegratedS3.Provider.Disk` emulates the supported S3-compatible feature set on local disk, including versioning, tagging, checksums, multipart uploads, and bucket CORS.
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
