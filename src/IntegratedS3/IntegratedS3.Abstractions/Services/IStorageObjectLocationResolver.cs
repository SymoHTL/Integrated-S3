using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;

namespace IntegratedS3.Abstractions.Services;

/// <summary>
/// Resolves the physical read location for a stored object, enabling redirect or direct-access patterns.
/// </summary>
public interface IStorageObjectLocationResolver
{
    /// <summary>
    /// Gets a value indicating who owns the location resolution logic.
    /// </summary>
    StorageSupportStateOwnership Ownership { get; }

    /// <summary>
    /// Resolves where to read an object from, returning location details or <c>null</c> if the location is not resolvable.
    /// </summary>
    /// <param name="request">The <see cref="ResolveObjectLocationRequest"/> describing the object to locate.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResolvedObjectLocation"/> with the resolved location, or <c>null</c>.</returns>
    ValueTask<StorageResolvedObjectLocation?> ResolveReadLocationAsync(
        ResolveObjectLocationRequest request,
        CancellationToken cancellationToken = default);
}
