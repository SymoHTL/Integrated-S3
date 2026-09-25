using IntegratedS3.Testing;
using IntegratedS3.Tests.Infrastructure;

namespace IntegratedS3.Tests;

public sealed class EngineStorageContractTests : StorageProviderContractTests
{
    protected override StorageProviderContractTestOptions ContractOptions => new()
    {
        SupportedChecksumAlgorithms = ["crc32c", "sha256", "sha1"]
    };

    protected override StorageProviderContractFixture CreateFixture() => new EngineStorageFixture();
}
