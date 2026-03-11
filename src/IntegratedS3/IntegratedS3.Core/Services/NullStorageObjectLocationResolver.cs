using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Services;

namespace IntegratedS3.Core.Services;

public sealed class NullStorageObjectLocationResolver : IStorageObjectLocationResolver
{
    public StorageSupportStateOwnership Ownership => StorageSupportStateOwnership.NotApplicable;

    public ValueTask<StorageResolvedObjectLocation?> ResolveReadLocationAsync(
        ResolveObjectLocationRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        return ValueTask.FromResult<StorageResolvedObjectLocation?>(null);
    }
}
