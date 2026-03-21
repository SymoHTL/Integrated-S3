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
    /// <summary>
    /// Presigns a GET (download) request for the specified object, using the server's default access mode.
    /// </summary>
    /// <param name="client">The <see cref="IIntegratedS3Client"/> used to issue the presign request.</param>
    /// <param name="bucketName">The name of the bucket containing the object.</param>
    /// <param name="key">The object key to download.</param>
    /// <param name="expiresInSeconds">How long the presigned URL should remain valid, in seconds.</param>
    /// <param name="versionId">An optional version identifier for versioned objects.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StoragePresignedRequest"/> containing the presigned URL, HTTP method, and required headers.</returns>
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
    /// Presigns a GET (download) request for the specified object with an explicit <paramref name="preferredAccessMode"/>.
    /// </summary>
    /// <param name="client">The <see cref="IIntegratedS3Client"/> used to issue the presign request.</param>
    /// <param name="bucketName">The name of the bucket containing the object.</param>
    /// <param name="key">The object key to download.</param>
    /// <param name="expiresInSeconds">How long the presigned URL should remain valid, in seconds.</param>
    /// <param name="preferredAccessMode">The preferred <see cref="StorageAccessMode"/> hint forwarded to the server.</param>
    /// <param name="versionId">An optional version identifier for versioned objects.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StoragePresignedRequest"/> containing the presigned URL, HTTP method, and required headers.</returns>
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
    /// Presigns a PUT (upload) request for the specified object, using the server's default access mode.
    /// </summary>
    /// <param name="client">The <see cref="IIntegratedS3Client"/> used to issue the presign request.</param>
    /// <param name="bucketName">The target bucket name.</param>
    /// <param name="key">The target object key.</param>
    /// <param name="expiresInSeconds">How long the presigned URL should remain valid, in seconds.</param>
    /// <param name="contentType">Optional MIME type for the object.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StoragePresignedRequest"/> containing the presigned URL, HTTP method, and required headers.</returns>
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
    /// Presigns a checksum-aware PUT (upload) request for the specified object, using the server's default access mode.
    /// </summary>
    /// <param name="client">The <see cref="IIntegratedS3Client"/> used to issue the presign request.</param>
    /// <param name="bucketName">The target bucket name.</param>
    /// <param name="key">The target object key.</param>
    /// <param name="expiresInSeconds">How long the presigned URL should remain valid, in seconds.</param>
    /// <param name="checksumAlgorithm">The <see cref="IntegratedS3TransferChecksumAlgorithm"/> used for integrity verification.</param>
    /// <param name="checksumValue">The pre-computed checksum value for the object payload.</param>
    /// <param name="contentType">Optional MIME type for the object.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StoragePresignedRequest"/> containing the presigned URL, HTTP method, and required headers.</returns>
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

    /// <summary>
    /// Presigns a PUT (upload) request for the specified object with an explicit <paramref name="preferredAccessMode"/>.
    /// </summary>
    /// <param name="client">The <see cref="IIntegratedS3Client"/> used to issue the presign request.</param>
    /// <param name="bucketName">The target bucket name.</param>
    /// <param name="key">The target object key.</param>
    /// <param name="expiresInSeconds">How long the presigned URL should remain valid, in seconds.</param>
    /// <param name="preferredAccessMode">The preferred <see cref="StorageAccessMode"/> hint forwarded to the server.</param>
    /// <param name="contentType">Optional MIME type for the object.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StoragePresignedRequest"/> containing the presigned URL, HTTP method, and required headers.</returns>
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

    /// <summary>
    /// Presigns a checksum-aware PUT (upload) request for the specified object with an explicit <paramref name="preferredAccessMode"/>.
    /// </summary>
    /// <param name="client">The <see cref="IIntegratedS3Client"/> used to issue the presign request.</param>
    /// <param name="bucketName">The target bucket name.</param>
    /// <param name="key">The target object key.</param>
    /// <param name="expiresInSeconds">How long the presigned URL should remain valid, in seconds.</param>
    /// <param name="preferredAccessMode">The preferred <see cref="StorageAccessMode"/> hint forwarded to the server.</param>
    /// <param name="checksumAlgorithm">The <see cref="IntegratedS3TransferChecksumAlgorithm"/> used for integrity verification.</param>
    /// <param name="checksumValue">The pre-computed checksum value for the object payload.</param>
    /// <param name="contentType">Optional MIME type for the object.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StoragePresignedRequest"/> containing the presigned URL, HTTP method, and required headers.</returns>
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
