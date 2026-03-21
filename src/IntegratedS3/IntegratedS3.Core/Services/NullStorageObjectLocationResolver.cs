using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Services;

namespace IntegratedS3.Core.Services;

/// <summary>
/// A no-op <see cref="IStorageObjectLocationResolver"/> that always returns <see langword="null"/>,
/// indicating that location resolution is not supported or not applicable.
/// </summary>
public sealed class NullStorageObjectLocationResolver : IStorageObjectLocationResolver
{
    /// <inheritdoc />
    public StorageSupportStateOwnership Ownership => StorageSupportStateOwnership.NotApplicable;

    /// <inheritdoc />
    public ValueTask<StorageResolvedObjectLocation?> ResolveReadLocationAsync(
        ResolveObjectLocationRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        return ValueTask.FromResult<StorageResolvedObjectLocation?>(null);
    }
}
