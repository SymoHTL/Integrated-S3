# IntegratedS3

**A modular ASP.NET Core library that exposes S3-compatible HTTP endpoints backed by pluggable storage providers.**

[![CI](https://github.com/SymoHTL/Intergrated-S3/actions/workflows/ci.yml/badge.svg)](https://github.com/SymoHTL/Intergrated-S3/actions/workflows/ci.yml)
[![License: BSD-3-Clause](https://img.shields.io/badge/License-BSD--3--Clause-blue.svg)](LICENSE)

---

## Features

- **S3-compatible REST API** — bucket and object operations over standard HTTP
- **Pluggable backends** — ship with disk and native S3 providers, or bring your own
- **SigV4 / SigV4a authentication** — optional AWS Signature V4 request signing and verification
- **Multi-backend orchestration** — replication policies across providers
- **Presigned URLs** — time-limited, shareable download/upload links
- **Multipart uploads** — chunked uploads with server-managed assembly
- **Versioning & object lock** — per-bucket version history and retention policies
- **Bucket configuration** — tagging, lifecycle, CORS, and policy support
- **Server-side encryption (SSE)** — encryption at the storage layer
- **Health checks** — liveness and readiness probes for each backend
- **OpenTelemetry observability** — built-in traces, metrics, and structured logging
- **AOT & trimming compatible** — targets .NET 10 with zero AOT warnings

---

## Quick Start

Install the main consumer package and a storage provider:

```
dotnet add package IntegratedS3.AspNetCore
dotnet add package IntegratedS3.Provider.Disk
```

**Program.cs**

```csharp
var builder = WebApplication.CreateSlimBuilder(args);

builder.Services.AddIntegratedS3(builder.Configuration);
builder.Services.AddDiskStorage(new DiskStorageOptions
{
    RootPath = "App_Data/IntegratedS3",
    CreateRootDirectory = true
});

var app = builder.Build();
app.MapIntegratedS3Endpoints();
app.Run();
```

**appsettings.json**

```json
{
  "IntegratedS3": {
    "ServiceName": "My S3 Host"
  }
}
```

Your S3-compatible endpoint is now running at `/integrated-s3`.

---

## Packages

| Package | Description |
|---|---|
| **IntegratedS3.AspNetCore** | ASP.NET Core DI registration + endpoint mapping (main consumer package) |
| **IntegratedS3.Provider.Disk** | Disk-backed storage provider |
| **IntegratedS3.Provider.S3** | Native AWS S3 / S3-compatible storage provider |
| **IntegratedS3.Abstractions** | Provider-agnostic contracts and observability surface |
| **IntegratedS3.Core** | Orchestration, replication policies, authorization |
| **IntegratedS3.Protocol** | S3 wire protocol — XML serialization, SigV4, presigning |
| **IntegratedS3.EntityFramework** | EF Core catalog and multipart state persistence |
| **IntegratedS3.Client** | First-party .NET HTTP client for IntegratedS3 hosts |
| **IntegratedS3.Testing** | Provider contract tests and test helpers (xUnit) |

---

## Configuration

`AddIntegratedS3(builder.Configuration)` binds the `IntegratedS3` configuration section to `IntegratedS3Options`. Key properties:

| Property | Default | Description |
|---|---|---|
| `ServiceName` | `"Integrated S3"` | Display name for the service |
| `RoutePrefix` | `"/integrated-s3"` | Base path for all S3 endpoints |
| `EnableAwsSignatureV4Authentication` | `false` | Require SigV4 signed requests |
| `SignatureAuthenticationRegion` | `"us-east-1"` | SigV4 signing region |
| `AccessKeyCredentials` | `[]` | List of access key / secret key pairs |
| `MaximumPresignedUrlExpirySeconds` | `3600` | Max presigned URL lifetime |
| `EnableVirtualHostedStyleAddressing` | `false` | Support virtual-hosted-style bucket URLs |

See [docs/webui-reference-host.md](docs/webui-reference-host.md) for the full configuration reference.

---

## Storage Providers

### Disk

```csharp
builder.Services.AddDiskStorage(new DiskStorageOptions
{
    RootPath = "App_Data/IntegratedS3",
    CreateRootDirectory = true
});
```

### S3

```csharp
builder.Services.AddS3Storage(new S3StorageOptions
{
    Region = "us-east-1",
    ServiceUrl = "http://localhost:9000", // optional — for MinIO, LocalStack, etc.
    ForcePathStyle = true                 // required for most S3-compatible endpoints
});
```

### Custom

Implement `IStorageBackend` and register it:

```csharp
builder.Services.AddIntegratedS3Backend<MyCustomBackend>();
```

See [docs/provider-contract-testing.md](docs/provider-contract-testing.md) for the xUnit harness that validates your implementation against the full storage contract.

---

## Health Checks

IntegratedS3 ships with ASP.NET Core health check integration for backend liveness and readiness:

```csharp
builder.Services
    .AddHealthChecks()
    .AddIntegratedS3BackendHealthCheck();

// ...

app.MapIntegratedS3HealthEndpoints(); // maps /health/live + /health/ready
```

---

## Observability

IntegratedS3 emits traces and metrics through a built-in `ActivitySource` and `Meter` (both named `"IntegratedS3"`). Wire them into the OpenTelemetry SDK:

```csharp
using IntegratedS3.Abstractions.Observability;

builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddSource(IntegratedS3Observability.ActivitySourceName))
    .WithMetrics(m => m.AddMeter(IntegratedS3Observability.MeterName));
```

See [docs/observability.md](docs/observability.md) for the full list of instruments, correlation ID propagation, and per-layer telemetry details.

---

## Documentation

| Document | Description |
|---|---|
| [Implementation Plan](docs/integrated-s3-implementation-plan.md) | Architecture overview, module breakdown, and roadmap |
| [WebUi Reference Host](docs/webui-reference-host.md) | Full configuration and wiring reference for the sample host |
| [Consumer Samples](docs/web-consumer-samples.md) | Minimal API, MVC/Razor, and Blazor WASM sample apps |
| [Observability](docs/observability.md) | Traces, metrics, structured logging, and OpenTelemetry integration |
| [Provider Contract Testing](docs/provider-contract-testing.md) | xUnit harness for validating custom `IStorageBackend` implementations |
| [Host Maintenance Jobs](docs/host-maintenance-jobs.md) | Opt-in recurring maintenance hosted service |
| [Performance Benchmarks](docs/performance-benchmarks.md) | BenchmarkDotNet harness and hot-path scenario catalog |

---

## Building & Testing

```bash
# Build
dotnet build src/IntegratedS3/IntegratedS3.slnx

# Test
dotnet test src/IntegratedS3/IntegratedS3.slnx

# Run the reference host
dotnet run --project src/IntegratedS3/WebUi/WebUi.csproj

# Validate AOT/trimming
dotnet publish -c Release --self-contained src/IntegratedS3/WebUi/WebUi.csproj
```

---

## License

This project is licensed under the terms of the [BSD 3-Clause License](LICENSE).
