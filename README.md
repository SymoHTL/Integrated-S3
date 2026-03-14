# IntegratedS3

IntegratedS3 is a modular, AOT-conscious .NET storage platform that exposes an S3-compatible HTTP surface while keeping provider orchestration, protocol helpers, ASP.NET integration, and provider implementations in separate packages.

## Package overview

The repository currently ships the following NuGet-oriented packages:

- `IntegratedS3.Abstractions` for provider-agnostic contracts, canonical errors, and capability descriptors.
- `IntegratedS3.Protocol` for S3-compatible protocol models, XML helpers, and Signature Version 4 utilities.
- `IntegratedS3.Core` for orchestration, authorization, replication, and presign services.
- `IntegratedS3.AspNetCore` for `AddIntegratedS3(...)` and `MapIntegratedS3Endpoints(...)`.
- `IntegratedS3.Client` for first-party presign and transfer helpers.
- `IntegratedS3.Provider.Disk` for the disk-backed provider.
- `IntegratedS3.Provider.S3` for the AWS SDK-backed provider.
- `IntegratedS3.EntityFramework` for optional EF Core-backed catalog persistence.
- `IntegratedS3.Testing` for test helpers and fakes as that package grows.

## Quick start

For the smallest host, register the platform package plus a provider and then map the endpoints:

```csharp
using IntegratedS3.AspNetCore.DependencyInjection;
using IntegratedS3.AspNetCore.Endpoints;
using IntegratedS3.Provider.Disk.DependencyInjection;

var builder = WebApplication.CreateSlimBuilder(args);

builder.Services.AddIntegratedS3();
builder.Services.AddDiskStorage(options =>
{
    options.ProviderName = "disk-primary";
    options.RootPath = "App_Data/IntegratedS3";
});

var app = builder.Build();
app.MapIntegratedS3Endpoints();
app.Run();
```

If you need catalog persistence, add `IntegratedS3.EntityFramework` and register `AddEntityFrameworkStorageCatalog<TDbContext>()`. If you need native AWS-backed storage, add `IntegratedS3.Provider.S3` and register `AddS3Storage(...)`.

## Consumer guides

- Getting started: <https://github.com/SymoHTL/Intergrated-S3/blob/main/docs/getting-started.md>
- Protocol compatibility guidance: <https://github.com/SymoHTL/Intergrated-S3/blob/main/docs/protocol-compatibility.md>
- Trimming and native AOT guidance: <https://github.com/SymoHTL/Intergrated-S3/blob/main/docs/aot-trimming-guidance.md>
- Reference/sample host notes: <https://github.com/SymoHTL/Intergrated-S3/blob/main/docs/webui-reference-host.md>

## Validation

Use the existing repository validation commands when updating packages or docs:

```powershell
dotnet build src\IntegratedS3\IntegratedS3.slnx
dotnet test src\IntegratedS3\IntegratedS3.slnx
dotnet publish -c Release --self-contained src\IntegratedS3\WebUi\WebUi.csproj
```
