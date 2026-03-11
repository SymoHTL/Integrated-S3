using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;

namespace IntegratedS3.Abstractions.Services;

public interface IStorageObjectLocationResolver
{
    StorageSupportStateOwnership Ownership { get; }

    ValueTask<StorageResolvedObjectLocation?> ResolveReadLocationAsync(
        ResolveObjectLocationRequest request,
        CancellationToken cancellationToken = default);
}
