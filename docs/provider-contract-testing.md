# Provider contract testing with `IntegratedS3.Testing`

`IntegratedS3.Testing` now provides a supported xUnit-based harness for backend implementers who want to validate a custom `IStorageBackend` against the current IntegratedS3 storage contract.

## What the package includes

- `StorageProviderContractFixture`
  - base fixture that owns the backend-under-test service provider
  - supports restart scenarios when a provider needs to be recreated against the same external state

- `StorageProviderContractTests`
  - reusable xUnit contract suite for bucket lifecycle, object CRUD, checksums, versioning, bucket CORS, range reads, conditional reads, copy, tags, multipart uploads, and optional platform-managed state-store seams
  - optional scenarios are gated by the backend's reported capabilities

- `StorageProviderContractTestOptions`
  - opt-in switches for platform-managed object-state and multipart-state store tests

- `InMemoryObjectStateStore` and `InMemoryMultipartStateStore`
  - reusable platform-managed support-state fakes for providers that externalize metadata/version or multipart state

- `StorageProviderTestingServiceCollectionExtensions`
  - `AddInMemoryStorageObjectStateStore(...)`
  - `AddInMemoryStorageMultipartStateStore(...)`

- `ChecksumTestAlgorithms`
  - shared SHA-1, SHA-256, CRC32C, and multipart CRC32C helpers for provider tests

## Minimal usage

Create a provider-specific fixture that registers your backend:

```csharp
using IntegratedS3.Testing;
using Microsoft.Extensions.DependencyInjection;

internal sealed class CustomStorageFixture : StorageProviderContractFixture
{
    protected override ValueTask<IServiceProvider> CreateServiceProviderAsync(
        Action<IServiceCollection>? configureServices,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var services = new ServiceCollection();
        configureServices?.Invoke(services);
        services.AddCustomStorage(/* options */);

        return ValueTask.FromResult<IServiceProvider>(services.BuildServiceProvider());
    }
}
```

Then inherit the shared contract suite in your test project:

```csharp
using IntegratedS3.Testing;

public sealed class CustomStorageContractTests : StorageProviderContractTests
{
    protected override StorageProviderContractTestOptions ContractOptions => new()
    {
        SupportsPlatformObjectStateStore = true,
        SupportsPlatformMultipartStateStore = true
    };

    protected override StorageProviderContractFixture CreateFixture() => new CustomStorageFixture();
}
```

## Platform-managed support-state tests

If your provider uses `IStorageObjectStateStore` or `IStorageMultipartStateStore`, the shared harness can register the bundled in-memory implementations for you:

```csharp
services.AddInMemoryStorageObjectStateStore();
services.AddInMemoryStorageMultipartStateStore();
```

You can also pass a specific store instance when you need restart/migration coverage:

```csharp
var objectStateStore = new InMemoryObjectStateStore();
services.AddInMemoryStorageObjectStateStore(objectStateStore);
```

## First-party reference usage

The repo's disk provider now consumes this supported path via:

- `src\IntegratedS3\IntegratedS3.Tests\DiskStorageContractTests.cs`
- `src\IntegratedS3\IntegratedS3.Tests\Infrastructure\DiskStorageFixture.cs`

Use that pairing as the reference shape for custom provider conformance coverage.
