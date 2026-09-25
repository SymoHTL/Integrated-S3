using IntegratedS3.Abstractions.Blobs;
using IntegratedS3.Engine.Blobs;
using IntegratedS3.Testing;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class LocalDiskBlobStoreContractTests : BlobStoreContractTests, IDisposable
{
    private readonly string _rootPath = Path.Combine(Path.GetTempPath(), "IntegratedS3.Tests", "blobs-" + Guid.NewGuid().ToString("N"));

    protected override ValueTask<IBlobStore> CreateStoreAsync()
        => ValueTask.FromResult<IBlobStore>(new LocalDiskBlobStore(_rootPath));

    // The contract's never-issued locators are malformed for this store; a well-formed one has no directories yet.
    [Fact]
    public async Task Delete_OfAWellFormedLocatorWhoseDirectoriesDoNotExist_Succeeds()
    {
        const string locator = "0123456789abcdef0123456789abcdef";
        var store = new LocalDiskBlobStore(_rootPath);

        await store.DeleteAsync(locator);

        await Assert.ThrowsAsync<BlobNotFoundException>(() => store.OpenReadAsync(locator).AsTask());
    }

    public void Dispose()
    {
        if (Directory.Exists(_rootPath)) {
            Directory.Delete(_rootPath, recursive: true);
        }
    }
}

public sealed class InMemoryBlobStoreContractTests : BlobStoreContractTests
{
    protected override ValueTask<IBlobStore> CreateStoreAsync()
        => ValueTask.FromResult<IBlobStore>(new InMemoryBlobStore());
}

public sealed class ConstrainedInMemoryBlobStoreContractTests : BlobStoreContractTests
{
    protected override ValueTask<IBlobStore> CreateStoreAsync()
        => ValueTask.FromResult<IBlobStore>(InMemoryBlobStore.CreateConstrained());
}
