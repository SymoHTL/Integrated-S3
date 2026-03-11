using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Provider.S3.Internal;

namespace IntegratedS3.Provider.S3;

internal sealed class S3StorageObjectLocationResolver(S3StorageOptions options, IS3StorageClient client) : IStorageObjectLocationResolver
{
    private readonly IS3StorageClient _client = client;

    public StorageSupportStateOwnership Ownership => StorageSupportStateOwnership.Delegated;

    public async ValueTask<StorageResolvedObjectLocation?> ResolveReadLocationAsync(
        ResolveObjectLocationRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        if (!string.Equals(request.ProviderName, options.ProviderName, StringComparison.Ordinal)
            || request.ExpiresAtUtc is null) {
            return null;
        }

        var presignedUrl = await _client.CreatePresignedGetObjectUrlAsync(
            request.BucketName,
            request.Key,
            request.VersionId,
            request.ExpiresAtUtc.Value,
            cancellationToken).ConfigureAwait(false);

        return new StorageResolvedObjectLocation
        {
            AccessMode = StorageObjectAccessMode.Delegated,
            Location = presignedUrl,
            ExpiresAtUtc = request.ExpiresAtUtc
        };
    }
}
