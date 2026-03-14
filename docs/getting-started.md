# IntegratedS3 getting started

This guide is the package-first onboarding path for hosts that want to embed IntegratedS3 without starting from the `WebUi` reference host.

## Choose the packages you need

Start with the smallest set of packages that matches your scenario:

- `IntegratedS3.Abstractions` if you are implementing a provider or consuming contracts directly.
- `IntegratedS3.Core` if you need orchestration services outside ASP.NET.
- `IntegratedS3.AspNetCore` if you want the ready-made HTTP surface and DI wiring.
- `IntegratedS3.Provider.Disk` for local or single-node storage.
- `IntegratedS3.Provider.S3` for native AWS SDK-backed storage.
- `IntegratedS3.EntityFramework` only when you want EF Core-backed catalog or multipart state persistence.
- `IntegratedS3.Client` for presign issuance and transfer helpers from another .NET application.

Keep package versions aligned across the `IntegratedS3.*` packages you consume. The packages are developed and versioned from one solution, so matching versions are the supported baseline.

## Minimal ASP.NET host

The recommended host shape stays close to the current reference host:

```csharp
using IntegratedS3.AspNetCore.DependencyInjection;
using IntegratedS3.AspNetCore.Endpoints;
using IntegratedS3.Provider.Disk.DependencyInjection;

var builder = WebApplication.CreateSlimBuilder(args);

builder.Services.AddIntegratedS3(options =>
{
    options.ServiceName = "My Integrated S3 Host";
});

builder.Services.AddDiskStorage(options =>
{
    options.ProviderName = "disk-primary";
    options.RootPath = "App_Data/IntegratedS3";
    options.CreateRootDirectory = true;
});

var app = builder.Build();
app.MapIntegratedS3Endpoints();
app.Run();
```

That gives you:

- JSON convenience routes under the configured route prefix.
- The S3-compatible compatibility route under `/{**s3Path}` beneath that prefix.
- Presign issuance through `POST /integrated-s3/presign/object`.
- Bucket-aware browser-facing CORS handling without global ASP.NET Core CORS middleware.

## Provider selection guidance

Choose a provider based on how much of the S3 surface you want to delegate to another system:

- Use `IntegratedS3.Provider.Disk` when you want a self-contained host, local development storage, or a provider that can emulate features such as versioning, CORS, tags, multipart uploads, and checksums on local disk.
- Use `IntegratedS3.Provider.S3` when your source of truth is an existing S3-compatible service and you want native provider behavior for features such as copy, multipart uploads, presigned URLs, and bucket CORS.
- Implement your own `IStorageBackend` when you need a provider-specific integration that does not fit the disk or S3 packages.

Optional integrations stay optional by design. For example, EF Core persistence lives in `IntegratedS3.EntityFramework` so consumers do not pay for that dependency unless they opt in.

## Configuration highlights

The most important configuration types are:

- `IntegratedS3Options` for service name, route prefix, Signature Version 4 authentication, presign defaults, virtual-hosted-style addressing, provider descriptors, and capability metadata.
- `IntegratedS3EndpointOptions` for enabling or disabling endpoint groups and customizing route-group conventions.
- `DiskStorageOptions` for provider name, storage root, and root-directory creation.
- `S3StorageOptions` for region, endpoint URL, path-style behavior, and explicit credentials when ambient credentials are not appropriate.
- `EntityFrameworkCatalogOptions` for EF-backed catalog initialization behavior.
- `IntegratedS3CoreOptions` for consistency, read-routing, replication, and backend-health policies.

## Authorization and request context

Authorization stays centered on `ClaimsPrincipal` and the `IIntegratedS3AuthorizationService` / `IIntegratedS3RequestContextAccessor` services registered by `IntegratedS3.Core`.

- In ASP.NET hosts, `HttpContext.User` flows into the Core authorization request context automatically.
- Replace the default allow-all authorization service if you need policy-aware bucket/object authorization.
- If you are building presign flows, make sure the authenticated principal has the scopes or claims your authorization layer expects for read/write operations.

## Presign and client integration

`IntegratedS3.Client` provides two layers:

- `IntegratedS3Client` and `IIntegratedS3Client` for direct calls to the presign endpoint.
- Convenience helpers such as `PresignGetObjectAsync`, `PresignPutObjectAsync`, `UploadStreamAsync`, and `DownloadToFileAsync`.

The presign request model supports an optional preferred access mode. Omit it to stay on the default proxy path, or use the access-mode overloads when you want to request direct or delegated reads explicitly.

## Where to go next

- Use `docs\webui-reference-host.md` if you want the sample/reference host wiring and validation flow.
- Use `docs\protocol-compatibility.md` if you need the supported S3-compatible surface and version-alignment guidance.
- Use `docs\aot-trimming-guidance.md` before shipping a trimmed or native AOT host.
