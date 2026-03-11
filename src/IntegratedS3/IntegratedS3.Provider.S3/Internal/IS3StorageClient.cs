using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Provider.S3.Internal;

internal interface IS3StorageClient : IDisposable
{
    // Bucket operations
    Task<IReadOnlyList<S3BucketEntry>> ListBucketsAsync(CancellationToken cancellationToken = default);
    Task<S3BucketEntry> CreateBucketAsync(string bucketName, CancellationToken cancellationToken = default);
    Task<S3BucketEntry?> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default);
    Task DeleteBucketAsync(string bucketName, CancellationToken cancellationToken = default);

    // Bucket versioning
    Task<S3VersioningEntry> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default);
    Task<S3VersioningEntry> SetBucketVersioningAsync(string bucketName, BucketVersioningStatus status, CancellationToken cancellationToken = default);

    // Object listing
    Task<S3ObjectListPage> ListObjectsAsync(
        string bucketName,
        string? prefix,
        string? continuationToken,
        int? maxKeys,
        CancellationToken cancellationToken = default);

    Task<S3ObjectVersionListPage> ListObjectVersionsAsync(
        string bucketName,
        string? prefix,
        string? delimiter,
        string? keyMarker,
        string? versionIdMarker,
        int? maxKeys,
        CancellationToken cancellationToken = default);

    // Object CRUD
    Task<S3ObjectEntry?> HeadObjectAsync(
        string bucketName,
        string key,
        string? versionId,
        CancellationToken cancellationToken = default);

    Task<S3GetObjectResult> GetObjectAsync(
        string bucketName,
        string key,
        string? versionId,
        ObjectRange? range,
        string? ifMatchETag,
        string? ifNoneMatchETag,
        DateTimeOffset? ifModifiedSinceUtc,
        DateTimeOffset? ifUnmodifiedSinceUtc,
        CancellationToken cancellationToken = default);

    Task<S3ObjectEntry> PutObjectAsync(
        string bucketName,
        string key,
        Stream content,
        long? contentLength,
        string? contentType,
        IReadOnlyDictionary<string, string>? metadata,
        CancellationToken cancellationToken = default);

    Task<S3DeleteObjectResult> DeleteObjectAsync(
        string bucketName,
        string key,
        string? versionId,
        CancellationToken cancellationToken = default);

    // Object tags
    Task<IReadOnlyDictionary<string, string>> GetObjectTagsAsync(
        string bucketName,
        string key,
        string? versionId,
        CancellationToken cancellationToken = default);

    Task PutObjectTagsAsync(
        string bucketName,
        string key,
        string? versionId,
        IReadOnlyDictionary<string, string> tags,
        CancellationToken cancellationToken = default);

    Task DeleteObjectTagsAsync(
        string bucketName,
        string key,
        string? versionId,
        CancellationToken cancellationToken = default);
}
