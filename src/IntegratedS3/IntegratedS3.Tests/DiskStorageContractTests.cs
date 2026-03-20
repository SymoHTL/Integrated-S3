using IntegratedS3.Testing;
using IntegratedS3.Tests.Infrastructure;

namespace IntegratedS3.Tests;

public sealed class DiskStorageContractTests : StorageProviderContractTests
{
    protected override StorageProviderContractTestOptions ContractOptions => new()
    {
        SupportsPlatformObjectStateStore = true,
        SupportsPlatformMultipartStateStore = true,
        SupportedChecksumAlgorithms = ["crc32c", "sha256", "sha1"]
    };

    protected override StorageProviderContractFixture CreateFixture()
    {
        return new DiskStorageFixture();
    }
}
