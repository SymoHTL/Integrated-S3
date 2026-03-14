# WebUi reference host

`src\IntegratedS3\WebUi` is the current reference/sample host for IntegratedS3. It demonstrates the minimal ASP.NET hosting, DI registration, and endpoint-mapping experience for the supported reference-host shapes:

- anonymous local development with the disk provider
- config-driven S3 provider composition for a real upstream bucket store
- SigV4-compatible request authentication plus host-owned authorization/policy wiring
- first-party presign credential selection and override seams

It is intentionally **not** the final architecture container for the broader platform.

## Run locally

```powershell
dotnet run --project src\IntegratedS3\WebUi\WebUi.csproj
```

Default local behavior:

- the `http` launch profile listens on `http://localhost:5298`
- `/` redirects to `/integrated-s3`
- `/integrated-s3` returns the service document
- `/openapi/v1.json` is available in the Development environment

## Reference surface snapshot

The sample host demonstrates more than the service document alone:

- JSON convenience routes under `/integrated-s3`, including service, capability, bucket, and object operations
- S3-compatible bucket/object routing under `/integrated-s3/{**s3Path}` for the current supported surface, including multipart, tagging, versioning, and bucket-CORS configuration flows
- `POST /integrated-s3/presign/object` for the current first-party presign flow
- bucket-aware browser-facing CORS handling on bucket/object routes, including unauthenticated preflight `OPTIONS` evaluation and actual-response `Access-Control-*` headers without global ASP.NET CORS middleware

## Configuration map

The sample host reads settings from `src\IntegratedS3\WebUi\appsettings.json`.

Core `IntegratedS3` settings:

- `IntegratedS3:ServiceName` — display name shown by the service document
- `IntegratedS3:RoutePrefix` — base path for the IntegratedS3 HTTP surface
- `IntegratedS3:EnableAwsSignatureV4Authentication` — enables SigV4 header and presigned-query request authentication on the IntegratedS3 HTTP surface
- `IntegratedS3:SignatureAuthenticationRegion` / `IntegratedS3:SignatureAuthenticationService` — expected SigV4 credential-scope values
- `IntegratedS3:AccessKeyCredentials` — access keys used for SigV4 request authentication and first-party presign generation
- `IntegratedS3:PresignAccessKeyId` — optional preferred access key for first-party presign generation when multiple credentials are configured
- `IntegratedS3:PresignPublicBaseUrl` — optional externally visible base URL used when presign generation does not have an active request host to infer from

Reference-host-specific settings:

- `IntegratedS3:ReferenceHost:StorageProvider` — `Disk` (default) or `S3`
- `IntegratedS3:ReferenceHost:RoutePolicies:*` — optional ASP.NET authorization policy names applied to the mapped IntegratedS3 route groups (`Route`, `Root`, `Compatibility`, `Service`, `Bucket`, `Object`, `Multipart`, `Admin`)
- `IntegratedS3:Disk:*` — disk-backed sample storage settings
- `IntegratedS3:S3:*` — native S3 provider settings (`ProviderName`, `Region`, `ServiceUrl`, `ForcePathStyle`, `AccessKey`, `SecretKey`)

By default, sample data is stored under `App_Data\IntegratedS3`. Runtime storage data is ignored by source control and excluded from build/publish outputs so local sample usage does not leak into release artifacts.

## Disk baseline

The committed `appsettings.json` keeps the reference host on the low-friction disk-backed baseline:

```json
"IntegratedS3": {
  "ReferenceHost": {
    "StorageProvider": "Disk"
  },
  "Disk": {
    "ProviderName": "local-disk",
    "RootPath": "App_Data/IntegratedS3",
    "CreateRootDirectory": true
  }
}
```

That is still the recommended starting point for local exploration and isolated test-host scenarios.

## Switching the reference host to S3

Set `IntegratedS3:ReferenceHost:StorageProvider` to `S3` and populate the `IntegratedS3:S3` section.

```json
"IntegratedS3": {
  "ReferenceHost": {
    "StorageProvider": "S3"
  },
  "S3": {
    "ProviderName": "minio-primary",
    "Region": "us-east-1",
    "ServiceUrl": "http://127.0.0.1:9000",
    "ForcePathStyle": true,
    "AccessKey": "minioadmin",
    "SecretKey": "minioadmin"
  }
}
```

Use explicit `AccessKey` / `SecretKey` for self-hosted S3-compatible endpoints such as MinIO or LocalStack. Leave them blank only when the host should rely on the ambient AWS credential chain.

## SigV4, storage authorization, and route-policy composition

Enable SigV4 authentication by configuring one or more `AccessKeyCredentials` and setting `EnableAwsSignatureV4Authentication` to `true`.

```json
"IntegratedS3": {
  "EnableAwsSignatureV4Authentication": true,
  "SignatureAuthenticationRegion": "us-east-1",
  "SignatureAuthenticationService": "s3",
  "AccessKeyCredentials": [
    {
      "AccessKeyId": "sample-access",
      "SecretAccessKey": "sample-secret",
      "DisplayName": "sample-user",
      "Scopes": [ "storage.read", "storage.write" ]
    }
  ]
}
```

IntegratedS3's built-in SigV4 authenticator creates a `ClaimsPrincipal` with:

- `integrateds3:auth-type = sigv4`
- `integrateds3:access-key-id = <configured access key id>`
- any configured `Scopes` emitted as `scope` claims

The S3-compatible request path should be authorized through `IIntegratedS3AuthorizationService`, because that service runs after the IntegratedS3 request-authentication filter has established the SigV4 principal. Route-group `RequireAuthorization(...)` metadata is still useful, but it runs in ASP.NET authorization middleware **before** the IntegratedS3 endpoint filter. In practice that means:

- use `IIntegratedS3AuthorizationService` for SigV4-native bucket/object authorization decisions
- use `IntegratedS3:ReferenceHost:RoutePolicies:*` plus normal ASP.NET authentication/authorization when the host wants to protect JSON/admin routes or layer host-owned auth on top of the sample host
- if you want route policies to protect S3-compatible routes as well, bridge the caller into the regular ASP.NET auth pipeline before `UseAuthorization()`; SigV4 alone does not satisfy route-policy middleware by itself

Example host-owned route policies:

```csharp
builder.Services.AddAuthentication().AddJwtBearer();
builder.Services.AddAuthorization(options => {
    options.AddPolicy("IntegratedS3BucketWrite", policy => {
        policy.RequireAuthenticatedUser();
        policy.RequireClaim("scope", "bucket.write");
    });
    options.AddPolicy("IntegratedS3AdminRead", policy => {
        policy.RequireAuthenticatedUser();
        policy.RequireClaim("scope", "admin.read");
    });
});
builder.Services.AddSingleton<IIntegratedS3AuthorizationService, MyIntegratedS3AuthorizationService>();
```

Matching config-driven route-policy wiring:

```json
"IntegratedS3": {
  "ReferenceHost": {
    "RoutePolicies": {
      "Bucket": "IntegratedS3BucketWrite",
      "Admin": "IntegratedS3AdminRead"
    }
  }
}
```

## Presign credential selection and customization

`ConfiguredIntegratedS3PresignCredentialResolver` is the default presign credential resolver. Its behavior is:

1. if the current principal already has `integrateds3:access-key-id`, use that configured credential
2. otherwise, if `PresignAccessKeyId` is configured, use that credential
3. otherwise, if exactly one `AccessKeyCredentials` entry exists, use it
4. otherwise, fail explicitly and require `PresignAccessKeyId` or a custom resolver

Useful configuration for the default resolver:

```json
"IntegratedS3": {
  "PresignAccessKeyId": "presign-access",
  "PresignPublicBaseUrl": "https://storage.example.com/",
  "AccessKeyCredentials": [
    {
      "AccessKeyId": "runtime-access",
      "SecretAccessKey": "runtime-secret"
    },
    {
      "AccessKeyId": "presign-access",
      "SecretAccessKey": "presign-secret"
    }
  ]
}
```

If the host needs tenant-aware, per-user, or external-secret-manager-driven presign credentials, replace the resolver in DI:

```csharp
builder.Services.AddSingleton<IIntegratedS3PresignCredentialResolver, MyPresignCredentialResolver>();
```

## Quick smoke test

After the host is running, these requests validate the reference surface without needing an S3 client:

```powershell
Invoke-WebRequest http://localhost:5298/integrated-s3 | Select-Object -ExpandProperty Content
Invoke-WebRequest http://localhost:5298/integrated-s3/capabilities | Select-Object -ExpandProperty Content
Invoke-WebRequest -Method Put http://localhost:5298/integrated-s3/buckets/demo-bucket
Invoke-WebRequest http://localhost:5298/integrated-s3/buckets | Select-Object -ExpandProperty Content
```

For an S3-backed reference host, the same service and capability routes remain the quickest way to confirm the selected provider metadata before moving on to AWS SDK or SigV4 client checks.

## Validation commands

Use the existing repository validation commands when polishing or updating the sample host:

```powershell
dotnet build src\IntegratedS3\IntegratedS3.slnx
dotnet test src\IntegratedS3\IntegratedS3.slnx
dotnet publish -c Release --self-contained src\IntegratedS3\WebUi\WebUi.csproj
```

Treat the publish step as the trimming/AOT validation pass for the reference host, not just as an optional packaging command.

## Test-host alignment

`src\IntegratedS3\IntegratedS3.Tests\Infrastructure\WebUiApplicationFactory.cs` reuses `WebUiApplication.ConfigureServices(...)` and `WebUiApplication.ConfigurePipeline(...)` so runtime and test wiring stay aligned.

- use `CreateIsolatedClientAsync(...)` for isolated in-process HTTP tests with temp storage and per-test builder overrides
- use `CreateLoopbackIsolatedClientAsync(...)` when real loopback networking is required, such as AWS SDK compatibility scenarios

## Scope guardrails

Keep `WebUi` focused on sample-host responsibilities:

- show the recommended `AddIntegratedS3(...)` and `MapIntegratedS3Endpoints(...)` flow
- keep runtime wiring easy to inspect and easy to reuse in tests
- document how to run and validate the sample host, including secure composition options

Keep reusable platform behavior in the package layers (`IntegratedS3.Core`, `IntegratedS3.AspNetCore`, provider packages) rather than expanding `WebUi` into the final architecture container.
