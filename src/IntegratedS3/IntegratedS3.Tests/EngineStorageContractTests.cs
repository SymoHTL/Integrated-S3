using IntegratedS3.Abstractions.Blobs;
using IntegratedS3.Testing;
using IntegratedS3.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace IntegratedS3.Tests;

public sealed class EngineStorageContractTests : StorageProviderContractTests
{
    protected override StorageProviderContractTestOptions ContractOptions => new()
    {
        SupportedChecksumAlgorithms = ["crc32c", "sha256", "sha1"]
    };

    protected override StorageProviderContractFixture CreateFixture() => new EngineStorageFixture();
}

/// <summary>
/// The provider contract with every body in blobs of at most 16 bytes, on a store that serves no byte ranges, lists
/// two entries a page and refuses every third delete: the engine meets a store's limits without the caller seeing
/// them. Throttling of the other calls answers SlowDown, which the contract does not retry;
/// <see cref="EngineStorageBackendTests"/> covers it.
/// </summary>
public sealed class EngineOnConstrainedBlobStoreContractTests : StorageProviderContractTests
{
    protected override StorageProviderContractTestOptions ContractOptions => new()
    {
        SupportedChecksumAlgorithms = ["crc32c", "sha256", "sha1"]
    };

    protected override StorageProviderContractFixture CreateFixture() => new EngineStorageFixture(
        static services => services.AddSingleton<IBlobStore>(new InMemoryBlobStore(new InMemoryBlobStoreOptions
        {
            MaxBlobSize = 16,
            SupportsRangeReads = false,
            MaxListPageSize = 2,
            ThrottleEveryNthDelete = 3
        })),
        static options => options.InlineThresholdBytes = 0);
}
