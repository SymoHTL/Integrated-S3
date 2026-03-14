using IntegratedS3.Core.Models;

namespace IntegratedS3.Client;

/// <summary>
/// Convenience helpers that construct <see cref="StoragePresignRequest"/> values for common object operations.
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
    /// <summary>
    /// Presigns an object download using the deployment's default access mode.
    /// </summary>
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

    /// <summary>
    /// Presigns an object download while requesting a specific access mode.
    /// </summary>
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

    /// <summary>
    /// Presigns an object upload using the deployment's default access mode.
    /// </summary>
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

    /// <summary>
    /// Presigns an object upload while requesting a specific access mode.
    /// </summary>
    public static ValueTask<StoragePresignedRequest> PresignPutObjectAsync(
        this IIntegratedS3Client client,
        string bucketName,
        string key,
        int expiresInSeconds,
        IntegratedS3TransferChecksumAlgorithm checksumAlgorithm,
        string checksumValue,
        string? contentType = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrWhiteSpace(checksumValue);

        var checksumKey = IntegratedS3ClientTransferChecksumHelper.ToProtocolValue(checksumAlgorithm);

        return client.PresignObjectAsync(new StoragePresignRequest
        {
            Operation = StoragePresignOperation.PutObject,
            BucketName = bucketName,
            Key = key,
            ExpiresInSeconds = expiresInSeconds,
            ContentType = contentType,
            ChecksumAlgorithm = checksumKey,
            Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [checksumKey] = checksumValue
            }
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

    public static ValueTask<StoragePresignedRequest> PresignPutObjectAsync(
        this IIntegratedS3Client client,
        string bucketName,
        string key,
        int expiresInSeconds,
        StorageAccessMode preferredAccessMode,
        IntegratedS3TransferChecksumAlgorithm checksumAlgorithm,
        string checksumValue,
        string? contentType = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrWhiteSpace(checksumValue);

        var checksumKey = IntegratedS3ClientTransferChecksumHelper.ToProtocolValue(checksumAlgorithm);

        return client.PresignObjectAsync(new StoragePresignRequest
        {
            Operation = StoragePresignOperation.PutObject,
            BucketName = bucketName,
            Key = key,
            ExpiresInSeconds = expiresInSeconds,
            ContentType = contentType,
            ChecksumAlgorithm = checksumKey,
            Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [checksumKey] = checksumValue
            },
            PreferredAccessMode = preferredAccessMode
        }, cancellationToken);
    }
}
