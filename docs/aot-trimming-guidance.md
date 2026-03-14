# IntegratedS3 trimming and native AOT guidance

IntegratedS3 is designed to stay trimming-aware and native-AOT-conscious, but the current host surface still has some diagnostics that consumers should understand.

## Current guidance

The lower-level packages are the safest building blocks for trimmed or native AOT deployments:

- `IntegratedS3.Abstractions`
- `IntegratedS3.Protocol`
- `IntegratedS3.Core`
- provider packages, when their own dependencies are acceptable for your deployment

`IntegratedS3.AspNetCore` intentionally keeps the current Minimal API host shape, which means the public endpoint-mapping methods carry trimming diagnostics:

- `RequiresUnreferencedCode`
- `RequiresDynamicCode`

Those annotations are intentional. They surface the current Minimal API registration trade-off instead of hiding it from consumers.

## What to validate

Use the existing publish validation command whenever you change host composition or endpoint wiring:

```powershell
dotnet publish -c Release --self-contained src\IntegratedS3\WebUi\WebUi.csproj
```

The repository also tracks a convenience validation script in `eng\Invoke-AotPublishValidation.ps1`.

## Recommended host practices

To keep trimming and AOT behavior understandable:

- keep runtime composition in small helpers such as `WebUiApplication.ConfigureServices(...)` and `ConfigurePipeline(...)`
- prefer the existing source-generated JSON contexts over reflection-heavy serialization
- avoid introducing reflection-heavy frameworks or dynamic plugin loading on hot paths
- keep optional integrations, such as EF Core persistence, in dedicated packages rather than in the base platform registration path

## Diagnostics policy

IntegratedS3 does not currently ship a separate custom analyzer package. The supported diagnostics story today is:

- .NET compiler, trimming, and AOT warnings from the SDK
- `RequiresUnreferencedCode` / `RequiresDynamicCode` annotations on the Minimal API mapping surface
- repository publish validation in CI and local validation commands

If a future release adds a dedicated analyzer package, it should complement this guidance rather than replace the existing runtime and publish diagnostics.
