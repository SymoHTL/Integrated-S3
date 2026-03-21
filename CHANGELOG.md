# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - Unreleased

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
