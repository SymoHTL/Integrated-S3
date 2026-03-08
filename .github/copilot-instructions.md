# Project Guidelines

## Code Style

- Target `.NET 10` and keep the codebase compatible with trimming and native AOT.
- Preserve the current minimal ASP.NET style used in `src/IntegratedS3/WebUi/Program.cs` and `src/IntegratedS3/WebUi/WebUiApplication.cs`:
  - prefer `WebApplication.CreateSlimBuilder(...)`
  - prefer Minimal APIs and `MapGroup(...)`
  - prefer typed results where it improves clarity
- Keep nullable reference types enabled and annotate public APIs carefully.
- Prefer async, streaming-first APIs for storage operations; avoid buffering full objects into memory unless a small payload explicitly justifies it.
- Use source-generated serialization for request/response DTOs. Do not introduce reflection-heavy JSON approaches.
- Keep public contracts provider-agnostic. Do not leak provider SDK types into shared abstractions.

## Architecture

- Treat `src/IntegratedS3/WebUi` as the current sample/reference host, not the final architecture container.
- Follow the implementation direction documented in `docs/integrated-s3-implementation-plan.md`.
- Keep package dependencies acyclic. The intended dependency direction is:
  - abstractions/protocol at the bottom
  - core orchestration above them
  - ASP.NET integration above core
  - provider packages depending on abstractions/protocol, not on ASP.NET integration
- `IntegratedS3.Core` owns the application-facing orchestration layer; storage providers should plug in through backend abstractions rather than becoming the public service surface directly.
- Design for pluggable providers (`S3`, disk, future providers) and overrideable DI services.
- Keep `IntegratedS3.Core` persistence-agnostic by default; if EF Core catalog persistence is used, prefer extracting it into a dedicated integration package and wiring it through a consumer-owned `DbContext`.
- Keep authorization centered on `ClaimsPrincipal` and dedicated authorization services rather than hardcoding rules inside endpoints.
- When adding S3-compatible behavior, prefer explicit capability modeling instead of silently degrading unsupported features.

## Build and Test

- Build the solution with `dotnet build src/IntegratedS3/IntegratedS3.slnx`.
- Run the automated tests with `dotnet test src/IntegratedS3/IntegratedS3.slnx`.
- Run the current sample host with `dotnet run --project src/IntegratedS3/WebUi/WebUi.csproj`.
- Validate trimming/AOT-sensitive changes with `dotnet publish -c Release --self-contained src/IntegratedS3/WebUi/WebUi.csproj`.
- Add or update automated tests alongside feature work; prefer in-process unit/integration coverage in `src/IntegratedS3/IntegratedS3.Tests` over manual PowerShell smoke tests.
- For major architecture or endpoint changes, prefer validating build and publish behavior before considering the change complete.

## Conventions

- Keep `PublishAot=true` and `InvariantGlobalization=true` unless there is a documented, intentional architectural change.
- Avoid reflection-heavy frameworks or patterns unless their AOT/trimming behavior is explicitly understood and acceptable.
- Prefer small, composable DI registrations such as `AddIntegratedS3(...)` and endpoint mapping extensions such as `MapIntegratedS3Endpoints(...)` as the platform evolves.
- Keep reusable host composition in dedicated setup helpers such as `WebUiApplication.ConfigureServices(...)` and `WebUiApplication.ConfigurePipeline(...)` so runtime wiring and test wiring stay aligned.
- For storage features, design both in-process service access and HTTP endpoint behavior deliberately; do not assume one replaces the other.
- Treat performance as a first-class concern:
  - favor streaming
  - minimize allocations on hot paths
  - avoid unnecessary copies
  - be careful with multipart and range request implementations
- For endpoint work, reuse the existing test-host pattern in `src/IntegratedS3/IntegratedS3.Tests/Infrastructure/WebUiApplicationFactory.cs` to keep tests isolated from repo-local state and temp-file collisions.
- If you add new docs for architecture or feature behavior, place them under `docs/` and link to them from instructions instead of duplicating large guidance here.
- At the end of a task, prefer updating `docs/integrated-s3-implementation-plan.md` with the newly implemented capabilities/status and the next best implementation step, and keep the user-facing completion message concise instead of writing a large retrospective response.
