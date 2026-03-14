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
- `POST /integrated-s3/presign/object` for first-party object `GET` / `PUT` presign flows, with explicit opt-in `Direct` / `Delegated` access-mode hints and proxy-mode as the default when no preference is sent
- bucket-aware browser-facing CORS handling on bucket/object routes, including unauthenticated preflight `OPTIONS` evaluation and actual-response `Access-Control-*` headers without global ASP.NET CORS middleware
- ASP.NET Core liveness/readiness probes at `/health/live` and `/health/ready`, with readiness backed by the IntegratedS3 backend health monitor/probe services

The service document may report provider `ObjectLocation` defaults for host inspection or UX, but the first-party presign and transfer helpers do not apply those defaults implicitly. Callers that want `Direct` or `Delegated` access should send `PreferredAccessMode` / `preferredAccessMode` explicitly; omitting it keeps proxy streaming through the IntegratedS3 host as the stable default.

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
## Custom backend registration

Hosts that implement their own `IStorageBackend` can now use `AddIntegratedS3Backend(...)` instead of manually pairing `AddSingleton<IStorageBackend>(...)` with the rest of the IntegratedS3 runtime wiring.

```csharp
builder.Services.AddIntegratedS3(builder.Configuration);
builder.Services.AddIntegratedS3Backend<MyCustomStorageBackend>();

// Optional companion seams stay explicit.
builder.Services.AddSingleton<IStorageObjectLocationResolver, MyCustomObjectLocationResolver>();
```

If the backend needs custom constructor arguments or named options, use the factory overload:

```csharp
builder.Services.AddIntegratedS3Backend(serviceProvider => new MyCustomStorageBackend(
    serviceProvider.GetRequiredService<IOptions<MyCustomStorageOptions>>().Value,
    serviceProvider.GetRequiredService<TimeProvider>()));
```

The helper keeps provider descriptors and reported capabilities backend-derived at runtime. Extra seams such as `IStorageObjectLocationResolver` and `IStoragePresignStrategy` remain explicit registrations so hosts can opt into them deliberately.

## Quick smoke test

After the host is running, these requests validate the reference surface without needing an S3 client:

```powershell
Invoke-WebRequest http://localhost:5298/integrated-s3 | Select-Object -ExpandProperty Content
Invoke-WebRequest http://localhost:5298/integrated-s3/capabilities | Select-Object -ExpandProperty Content
Invoke-WebRequest http://localhost:5298/integrated-s3/admin/diagnostics | Select-Object -ExpandProperty Content
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

## Observability

The reference host now emits IntegratedS3 request/auth/storage observability through standard .NET logging, tracing, and metrics without baking in a fixed exporter stack.

- responses on the IntegratedS3 HTTP surface echo `x-integrateds3-correlation-id`
- the platform emits traces from the shared `IntegratedS3` activity source and metrics from the shared `IntegratedS3` meter
- replica drift and backlog visibility are available through the admin repairs route plus the shared backlog/health metrics

See `docs/observability.md` for the supported host-integration path and the current limitation that dedicated health endpoints remain host-owned.

## Test-host alignment

`src\IntegratedS3\IntegratedS3.Tests\Infrastructure\WebUiApplicationFactory.cs` reuses `WebUiApplication.ConfigureServices(...)` and `WebUiApplication.ConfigurePipeline(...)` so runtime and test wiring stay aligned.

- use `CreateIsolatedClientAsync(...)` for isolated in-process HTTP tests with temp storage and per-test builder overrides
- use `CreateLoopbackIsolatedClientAsync(...)` when real loopback networking is required, such as AWS SDK compatibility scenarios

## Endpoint route-group customization

`WebUiApplication.ConfigurePipeline(...)` forwards `Action<IntegratedS3EndpointOptions>` into `MapIntegratedS3Endpoints(...)`, so sample hosts and tests can apply authorization or policy conventions at map time.

- use `ConfigureRouteGroup` for the whole `/integrated-s3` surface
- use `ConfigureRootRouteGroup` or `ConfigureCompatibilityRouteGroup` when a shared route can serve multiple features at once
- use `SetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.<Feature>, ...)` for per-feature route-group callbacks; the current `ConfigureBucketRouteGroup`, `ConfigureObjectRouteGroup`, and similar properties remain convenience wrappers for the built-in features

Future endpoint surfaces should follow the enum-based feature callback pattern instead of inventing a separate configuration style, so host/test customization stays predictable as the HTTP surface grows.

## Scope guardrails

Keep `WebUi` focused on sample-host responsibilities:

- show the recommended `AddIntegratedS3(...)` and `MapIntegratedS3Endpoints(...)` flow
- keep runtime wiring easy to inspect and easy to reuse in tests
- document how to run and validate the sample host

Keep reusable platform behavior in the package layers (`IntegratedS3.Core`, `IntegratedS3.AspNetCore`, provider packages) rather than expanding `WebUi` into the final architecture container.
