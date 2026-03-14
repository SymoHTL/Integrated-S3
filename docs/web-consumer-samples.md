# Web consumer samples

IntegratedS3 now ships three sample consumers under `src\IntegratedS3`:

- `WebUi` — the minimal API reference host that doubles as the current trimming/AOT validation target.
- `WebUi.MvcRazor` — an ASP.NET Core MVC/Razor consumer that injects `IStorageService` directly into a controller and renders a server-side dashboard.
- `WebUi.BlazorWasm` plus `WebUi.BlazorWasm.Client` — a hosted Blazor WebAssembly consumer that lists buckets through the JSON endpoints and uses `IntegratedS3.Client` to request presigned uploads/downloads.

## Run locally

```powershell
dotnet run --project src\IntegratedS3\WebUi.MvcRazor\WebUi.MvcRazor.csproj
dotnet run --project src\IntegratedS3\WebUi.BlazorWasm\WebUi.BlazorWasm.csproj
```

Both samples default to the disk provider and keep runtime data under a local `App_Data\IntegratedS3` folder that is ignored by source control.

## What each sample demonstrates

`WebUi.MvcRazor` focuses on in-process consumption:

- `AddIntegratedS3(...)` plus `AddDiskStorage(...)` registration from a conventional MVC app
- direct `IStorageService` injection into a controller
- side-by-side server-rendered HTML and the `/integrated-s3` HTTP surface in the same host

`WebUi.BlazorWasm` focuses on browser-friendly consumption:

- serving a standalone Blazor WebAssembly client from an ASP.NET Core host that also maps the IntegratedS3 endpoints
- calling `/integrated-s3` JSON endpoints for service discovery, bucket listing, and object listing
- using `IntegratedS3.Client` to request presigned `PUT` / `GET` transfers before uploading or downloading object content in the browser client

## Validation and current limitation

Use the existing repository validation commands when updating these samples:

```powershell
dotnet build src\IntegratedS3\IntegratedS3.slnx
dotnet test src\IntegratedS3\IntegratedS3.slnx
```

`WebUi` remains the current trimmed/native-AOT reference host and the publish-validation target. The MVC/Razor and Blazor WebAssembly samples are build/test-validated consumer examples, but they are not yet part of the current self-contained publish/AOT gate.
