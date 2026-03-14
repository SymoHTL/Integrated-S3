using IntegratedS3.Core.Models;

namespace IntegratedS3.Client;

/// <summary>
/// Convenience helpers for first-party object presign flows.
/// </summary>
/// <remarks>
/// Overloads without a <c>preferredAccessMode</c> parameter intentionally keep access-mode selection
/// explicit at the caller boundary. They do not infer <see cref="StorageAccessMode.Direct" /> or
/// <see cref="StorageAccessMode.Delegated" /> from service/provider discovery and therefore preserve
/// the server's proxy-mode default. Callers opt into non-proxy flows through the overloads that accept
/// <see cref="StorageAccessMode" />.
/// </remarks>
public static class IntegratedS3ClientPresignExtensions
{
    public static ValueTask<StoragePresignedRequest> PresignGetObjectAsync(
        this IIntegratedS3Client client,
        string bucketName,
        string key,
        int expiresInSeconds,
        string? versionId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);

        return client.PresignObjectAsync(new StoragePresignRequest
        {
            Operation = StoragePresignOperation.GetObject,
            BucketName = bucketName,
            Key = key,
            ExpiresInSeconds = expiresInSeconds,
            VersionId = versionId
        }, cancellationToken);
    }

    public static ValueTask<StoragePresignedRequest> PresignGetObjectAsync(
        this IIntegratedS3Client client,
        string bucketName,
        string key,
        int expiresInSeconds,
        StorageAccessMode preferredAccessMode,
        string? versionId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);

        return client.PresignObjectAsync(new StoragePresignRequest
        {
            Operation = StoragePresignOperation.GetObject,
            BucketName = bucketName,
            Key = key,
            ExpiresInSeconds = expiresInSeconds,
            VersionId = versionId,
            PreferredAccessMode = preferredAccessMode
        }, cancellationToken);
    }

    public static ValueTask<StoragePresignedRequest> PresignPutObjectAsync(
        this IIntegratedS3Client client,
        string bucketName,
        string key,
        int expiresInSeconds,
        string? contentType = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);

        return client.PresignObjectAsync(new StoragePresignRequest
        {
            Operation = StoragePresignOperation.PutObject,
            BucketName = bucketName,
            Key = key,
            ExpiresInSeconds = expiresInSeconds,
            ContentType = contentType
        }, cancellationToken);
    }

    public static ValueTask<StoragePresignedRequest> PresignPutObjectAsync(
        this IIntegratedS3Client client,
        string bucketName,
        string key,
        int expiresInSeconds,
        StorageAccessMode preferredAccessMode,
        string? contentType = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);

        return client.PresignObjectAsync(new StoragePresignRequest
        {
            Operation = StoragePresignOperation.PutObject,
            BucketName = bucketName,
            Key = key,
            ExpiresInSeconds = expiresInSeconds,
            ContentType = contentType,
            PreferredAccessMode = preferredAccessMode
        }, cancellationToken);
    }
}
