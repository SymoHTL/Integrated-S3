using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Provider.S3.Internal;

internal interface IS3StorageClient : IDisposable
{
    // Bucket operations
    Task<IReadOnlyList<S3BucketEntry>> ListBucketsAsync(CancellationToken cancellationToken = default);
    Task<S3BucketEntry> CreateBucketAsync(string bucketName, CancellationToken cancellationToken = default);
    Task<S3BucketEntry?> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default);
    Task DeleteBucketAsync(string bucketName, CancellationToken cancellationToken = default);
    Task<S3BucketLocationEntry> GetBucketLocationAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromException<S3BucketLocationEntry>(new NotSupportedException("Bucket location is not implemented by this S3 storage client."));

    // Bucket versioning
    Task<S3VersioningEntry> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default);
    Task<S3VersioningEntry> SetBucketVersioningAsync(string bucketName, BucketVersioningStatus status, CancellationToken cancellationToken = default);

    // Bucket CORS
    Task<S3CorsConfigurationEntry?> GetBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromException<S3CorsConfigurationEntry?>(new NotSupportedException("Bucket CORS is not implemented by this S3 storage client."));
    Task<S3CorsConfigurationEntry> SetBucketCorsAsync(string bucketName, IReadOnlyList<S3CorsRuleEntry> rules, CancellationToken cancellationToken = default)
        => Task.FromException<S3CorsConfigurationEntry>(new NotSupportedException("Bucket CORS is not implemented by this S3 storage client."));
    Task DeleteBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromException(new NotSupportedException("Bucket CORS is not implemented by this S3 storage client."));

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

    Task<Uri> CreatePresignedGetObjectUrlAsync(
        string bucketName,
        string key,
        string? versionId,
        DateTimeOffset expiresAtUtc,
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
        IReadOnlyDictionary<string, string>? checksums,
        ObjectServerSideEncryptionSettings? serverSideEncryption,
        CancellationToken cancellationToken = default);

    Task<S3DeleteObjectResult> DeleteObjectAsync(
        string bucketName,
        string key,
        string? versionId,
        CancellationToken cancellationToken = default);

    // Copy + multipart
    Task<S3ObjectEntry> CopyObjectAsync(
        string sourceBucketName,
        string sourceKey,
        string destinationBucketName,
        string destinationKey,
        string? sourceVersionId,
        string? sourceIfMatchETag,
        string? sourceIfNoneMatchETag,
        DateTimeOffset? sourceIfModifiedSinceUtc,
        DateTimeOffset? sourceIfUnmodifiedSinceUtc,
        bool overwriteIfExists,
        ObjectServerSideEncryptionSettings? destinationServerSideEncryption,
        CancellationToken cancellationToken = default);

    Task<MultipartUploadInfo> InitiateMultipartUploadAsync(
        string bucketName,
        string key,
        string? contentType,
        IReadOnlyDictionary<string, string>? metadata,
        string? checksumAlgorithm,
        ObjectServerSideEncryptionSettings? serverSideEncryption,
        CancellationToken cancellationToken = default);

    Task<MultipartUploadPart> UploadMultipartPartAsync(
        string bucketName,
        string key,
        string uploadId,
        int partNumber,
        Stream content,
        long? contentLength,
        string? checksumAlgorithm,
        IReadOnlyDictionary<string, string>? checksums,
        CancellationToken cancellationToken = default);

    Task<S3ObjectEntry> CompleteMultipartUploadAsync(
        string bucketName,
        string key,
        string uploadId,
        IReadOnlyList<MultipartUploadPart> parts,
        CancellationToken cancellationToken = default);

    Task AbortMultipartUploadAsync(
        string bucketName,
        string key,
        string uploadId,
        CancellationToken cancellationToken = default);

    Task<S3MultipartUploadListPage> ListMultipartUploadsAsync(
        string bucketName,
        string? prefix,
        string? keyMarker,
        string? uploadIdMarker,
        int? maxUploads,
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
