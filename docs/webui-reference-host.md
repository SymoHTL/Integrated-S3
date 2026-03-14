# WebUi reference host

`src\IntegratedS3\WebUi` is the current reference/sample host for IntegratedS3. It exists to demonstrate the minimal ASP.NET hosting, DI registration, and endpoint-mapping experience with the disk provider. It is intentionally **not** the final architecture container for the broader platform.

## Run locally

```powershell
dotnet run --project src\IntegratedS3\WebUi\WebUi.csproj
```

Default local behavior:

- the `http` launch profile listens on `http://localhost:5298`
- `/` redirects to `/integrated-s3`
- `/integrated-s3` returns the service document
- `/health/live` exposes the process liveness endpoint
- `/health/ready` exposes IntegratedS3 backend readiness through ASP.NET Core health checks
- `/openapi/v1.json` is available in the Development environment

## Reference surface snapshot

The sample host currently demonstrates more than the service document alone:

- JSON convenience routes under `/integrated-s3`, including service, capability, bucket, and object operations
- S3-compatible bucket/object routing under `/integrated-s3/{**s3Path}` for the current supported surface, including multipart, tagging, versioning, and bucket-CORS configuration flows
- `POST /integrated-s3/presign/object` for the current first-party proxy-mode object `GET` / `PUT` presign flow
- bucket-aware browser-facing CORS handling on bucket/object routes, including unauthenticated preflight `OPTIONS` evaluation and actual-response `Access-Control-*` headers without global ASP.NET CORS middleware
- ASP.NET Core liveness/readiness probes at `/health/live` and `/health/ready`, with readiness backed by the IntegratedS3 backend health monitor/probe services

## Default configuration

The sample host reads settings from `src\IntegratedS3\WebUi\appsettings.json`.

- `IntegratedS3:ServiceName` — display name shown by the service document
- `IntegratedS3:RoutePrefix` — base path for the IntegratedS3 HTTP surface
- `IntegratedS3:Disk:ProviderName` — provider name reported by the sample disk backend
- `IntegratedS3:Disk:RootPath` — disk-backed object storage location; relative paths are resolved from the WebUi content root
- `IntegratedS3:Disk:CreateRootDirectory` — creates the storage root automatically on startup when needed

By default, sample data is stored under `App_Data\IntegratedS3`. Runtime storage data is ignored by source control and excluded from build/publish outputs so local sample usage does not leak into release artifacts.

## Health check wiring

The reference host shows the supported ASP.NET Core integration path for backend health:

```csharp
builder.Services.AddIntegratedS3(builder.Configuration, ...);
builder.Services.AddDiskStorage(diskOptions);
builder.Services.AddHealthChecks()
    .AddIntegratedS3BackendHealthCheck();

app.MapIntegratedS3HealthEndpoints();
```

`/health/live` stays a process liveness probe, while `/health/ready` runs the IntegratedS3 backend readiness check. The readiness mapper treats both `Degraded` and `Unhealthy` results as HTTP `503` by default so hosts can use it directly for readiness probes.

## Quick smoke test

After the host is running, these requests validate the reference surface without needing an S3 client:

```powershell
Invoke-WebRequest http://localhost:5298/integrated-s3 | Select-Object -ExpandProperty Content
Invoke-WebRequest http://localhost:5298/integrated-s3/capabilities | Select-Object -ExpandProperty Content
Invoke-WebRequest -Method Put http://localhost:5298/integrated-s3/buckets/demo-bucket
Invoke-WebRequest http://localhost:5298/integrated-s3/buckets | Select-Object -ExpandProperty Content
Invoke-WebRequest http://localhost:5298/health/live | Select-Object -ExpandProperty Content
Invoke-WebRequest http://localhost:5298/health/ready | Select-Object -ExpandProperty Content
```

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
- document how to run and validate the sample host

Keep reusable platform behavior in the package layers (`IntegratedS3.Core`, `IntegratedS3.AspNetCore`, provider packages) rather than expanding `WebUi` into the final architecture container.
