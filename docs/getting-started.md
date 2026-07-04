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

Support differs per provider — for example, per-object retention, legal hold, `RestoreObject`, `SelectObjectContent`, and server-side encryption are only supported by the native S3 provider and return `NotImplemented` on the disk provider. See the [provider capability matrix](protocol-compatibility.md#provider-capability-matrix) before relying on a specific operation.

Optional integrations stay optional by design. For example, EF Core persistence lives in `IntegratedS3.EntityFramework` so consumers do not pay for that dependency unless they opt in.

## Configuration highlights

The most important configuration types are:

- `IntegratedS3Options` for service name, route prefix, Signature Version 4 authentication, presign defaults, virtual-hosted-style addressing, provider descriptors, and capability metadata.
- `IntegratedS3EndpointOptions` for enabling or disabling endpoint groups and customizing route-group conventions.
- `DiskStorageOptions` for provider name, storage root, and root-directory creation.
- `S3StorageOptions` for region, endpoint URL, path-style behavior, and explicit credentials when ambient credentials are not appropriate.
- `EntityFrameworkCatalogOptions` for EF-backed catalog initialization behavior.
- `IntegratedS3CoreOptions` for consistency, read-routing, replication, and backend-health policies.

## Large object uploads and request body limits

ASP.NET Core hosts cap request bodies at the server default (Kestrel rejects bodies over 30,000,000 bytes, roughly 28.6 MiB) unless configured otherwise, which would fail larger `PutObject` and `UploadPart` payloads with `413 Payload Too Large` before the endpoint runs. The object upload endpoints therefore replace the host's per-request body-size limit with `IntegratedS3Options.MaxObjectSizeBytes` before the body is read:

- Leave `MaxObjectSizeBytes` at its default (`5368709120`, the S3 5 GiB per-request maximum) so uploads up to that size succeed without host tuning, while still rejecting anything larger with `413 EntityTooLarge`. This default also bounds the temporary spool file written while decoding `Content-Encoding: aws-chunked` bodies, so a client cannot exhaust the temp volume by streaming an unbounded body.
- Set a smaller explicit byte count (for example `104857600` for 100 MiB) to tighten the cap; larger requests are rejected with `413 EntityTooLarge`.
- Set `MaxObjectSizeBytes` to `null` to fully remove the application-level limit on `PutObject` and `UploadPart` (uploads are then bounded only by the host's own limit, if any). This is discouraged because it re-enables the aws-chunked disk-exhaustion risk.
- All other endpoints keep the host-configured limit, so lifting it for uploads does not loosen the rest of the surface.

The override only applies where the server exposes a mutable per-request limit (Kestrel and IIS in-process do). If a reverse proxy such as nginx or IIS ARR sits in front of the host, raise its request body limit as well or large uploads will still be rejected upstream.

## Authorization and request context

Authorization stays centered on `ClaimsPrincipal` and the `IIntegratedS3AuthorizationService` / `IIntegratedS3RequestContextAccessor` services registered by `IntegratedS3.Core`.

- In ASP.NET hosts, `HttpContext.User` flows into the Core authorization request context automatically.
- Replace the default allow-all authorization service if you need policy-aware bucket/object authorization.
- If you are building presign flows, make sure the authenticated principal has the scopes or claims your authorization layer expects for read/write operations.

## SigV4 credentials and key rotation

When `EnableAwsSignatureV4Authentication` is on, incoming SigV4/SigV4a requests resolve their access key through the `IIntegratedS3CredentialResolver` service.

The default resolver reads `IntegratedS3Options.AccessKeyCredentials` through `IOptionsMonitor<IntegratedS3Options>`, so credential changes from a reloadable configuration source (for example `appsettings.json` with `reloadOnChange: true`, or a secret-manager configuration provider that raises reload tokens) take effect on the next request without an application restart. To rotate a key with the default source:

1. Add the new access key/secret pair to the `IntegratedS3:AccessKeyCredentials` configuration section alongside the old one.
2. Roll clients over to the new key.
3. Remove the old entry; requests signed with it are rejected with `InvalidAccessKeyId` as soon as the configuration reloads.

To back credentials with a database or secret manager instead, register a custom resolver before (or after) `AddIntegratedS3`; the registration wins over the built-in options-backed default. Returning `null` rejects the request with `InvalidAccessKeyId`, which is how revocation takes effect immediately. With a custom resolver registered, `AccessKeyCredentials` may stay empty. An EF Core-backed example:

```csharp
public sealed class EfCredentialResolver(IDbContextFactory<AppDbContext> contextFactory) : IIntegratedS3CredentialResolver
{
    public async ValueTask<IntegratedS3AccessKeyCredential?> ResolveAsync(
        string accessKeyId,
        CancellationToken cancellationToken = default)
    {
        await using var dbContext = await contextFactory.CreateDbContextAsync(cancellationToken);
        var record = await dbContext.AccessKeys
            .AsNoTracking()
            .SingleOrDefaultAsync(key => key.AccessKeyId == accessKeyId && !key.Revoked, cancellationToken);

        return record is null
            ? null
            : new IntegratedS3AccessKeyCredential
            {
                AccessKeyId = record.AccessKeyId,
                SecretAccessKey = record.SecretAccessKey,
                DisplayName = record.DisplayName,
                Scopes = record.Scopes.ToList()
            };
    }
}

builder.Services.AddSingleton<IIntegratedS3CredentialResolver, EfCredentialResolver>();
```

The resolver sits on the request hot path, so cache lookups (for example with `IMemoryCache` and a short TTL) if your credential store is remote. Note that first-party presigned URL *generation* selects its signing credential through the separate `IIntegratedS3PresignCredentialResolver`; customize both when credentials leave the options list entirely.

## Presign and client integration

`IntegratedS3.Client` provides two layers:

- `IntegratedS3Client` and `IIntegratedS3Client` for direct calls to the presign endpoint.
- Convenience helpers such as `PresignGetObjectAsync`, `PresignPutObjectAsync`, `UploadStreamAsync`, and `DownloadToFileAsync`.

Presign issuance covers `GetObject`, `PutObject`, `DeleteObject`, `HeadObject`, and `UploadPart` (browser-direct multipart part uploads). The corresponding client helpers are `PresignDeleteObjectAsync`, `PresignHeadObjectAsync`, and `PresignUploadPartAsync`, with transfer-level counterparts `DeleteObjectAsync`, `HeadObjectAsync`, and `UploadPartStreamAsync` (which returns the part ETag needed to complete the multipart upload).

The presign request model supports an optional preferred access mode. Omit it to stay on the default proxy path, or use the access-mode overloads when you want to request direct or delegated reads explicitly.

## Where to go next

- Use `docs\webui-reference-host.md` if you want the sample/reference host wiring and validation flow.
- Use `docs\protocol-compatibility.md` if you need the supported S3-compatible surface and version-alignment guidance.
- Use `docs\aot-trimming-guidance.md` before shipping a trimmed or native AOT host.
