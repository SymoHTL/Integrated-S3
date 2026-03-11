using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Provider.S3.Internal;

internal sealed class AwsS3StorageClient : IS3StorageClient
{
    private readonly IAmazonS3 _s3;

    public AwsS3StorageClient(S3StorageOptions options)
    {
        var config = new AmazonS3Config
        {
            ForcePathStyle = options.ForcePathStyle
        };

        if (!string.IsNullOrWhiteSpace(options.ServiceUrl))
            config.ServiceURL = options.ServiceUrl;
        else
            config.RegionEndpoint = RegionEndpoint.GetBySystemName(options.Region);

        _s3 = !string.IsNullOrWhiteSpace(options.AccessKey) && !string.IsNullOrWhiteSpace(options.SecretKey)
            ? new AmazonS3Client(new BasicAWSCredentials(options.AccessKey, options.SecretKey), config)
            : new AmazonS3Client(config);
    }

    // -------------------------------------------------------------------------
    // Bucket operations
    // -------------------------------------------------------------------------

    public async Task<IReadOnlyList<S3BucketEntry>> ListBucketsAsync(CancellationToken cancellationToken = default)
    {
        var response = await _s3.ListBucketsAsync(cancellationToken).ConfigureAwait(false);
        return response.Buckets
            .Select(b => new S3BucketEntry(
                b.BucketName,
                b.CreationDate.HasValue
                    ? new DateTimeOffset(b.CreationDate.Value, TimeSpan.Zero)
                    : DateTimeOffset.UtcNow))
            .ToList();
    }

    public async Task<S3BucketEntry> CreateBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new PutBucketRequest { BucketName = bucketName };
        await _s3.PutBucketAsync(request, cancellationToken).ConfigureAwait(false);
        return new S3BucketEntry(bucketName, DateTimeOffset.UtcNow);
    }

    public async Task<S3BucketEntry?> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        try
        {
            var request = new HeadBucketRequest { BucketName = bucketName };
            await _s3.HeadBucketAsync(request, cancellationToken).ConfigureAwait(false);
            return new S3BucketEntry(bucketName, DateTimeOffset.UtcNow);
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task DeleteBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new DeleteBucketRequest { BucketName = bucketName };
        await _s3.DeleteBucketAsync(request, cancellationToken).ConfigureAwait(false);
    }

    // -------------------------------------------------------------------------
    // Bucket versioning
    // -------------------------------------------------------------------------

    public async Task<S3VersioningEntry> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new GetBucketVersioningRequest { BucketName = bucketName };
        var response = await _s3.GetBucketVersioningAsync(request, cancellationToken).ConfigureAwait(false);
        var status = MapVersioningStatus(response.VersioningConfig?.Status);
        return new S3VersioningEntry(status);
    }

    public async Task<S3VersioningEntry> SetBucketVersioningAsync(string bucketName, BucketVersioningStatus status, CancellationToken cancellationToken = default)
    {
        var request = new PutBucketVersioningRequest
        {
            BucketName = bucketName,
            VersioningConfig = new S3BucketVersioningConfig
            {
                Status = MapVersioningStatusToSdk(status)
            }
        };
        await _s3.PutBucketVersioningAsync(request, cancellationToken).ConfigureAwait(false);
        return new S3VersioningEntry(status);
    }

    // -------------------------------------------------------------------------
    // Object listing
    // -------------------------------------------------------------------------

    public async Task<S3ObjectListPage> ListObjectsAsync(
        string bucketName,
        string? prefix,
        string? continuationToken,
        int? maxKeys,
        CancellationToken cancellationToken = default)
    {
        var request = new ListObjectsV2Request
        {
            BucketName = bucketName,
            Prefix = prefix,
            ContinuationToken = continuationToken
        };

        if (maxKeys.HasValue)
            request.MaxKeys = maxKeys.Value;

        var response = await _s3.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);

        var entries = (response.S3Objects ?? [])
            .Select(o => new S3ObjectEntry(
                Key: o.Key ?? string.Empty,
                ContentLength: o.Size ?? 0,
                ContentType: null,
                ETag: o.ETag,
                LastModifiedUtc: ToDateTimeOffset(o.LastModified),
                Metadata: null,
                VersionId: null))
            .ToList();

        return new S3ObjectListPage(
            entries,
            response.IsTruncated == true ? response.NextContinuationToken : null);
    }

    public async Task<S3ObjectVersionListPage> ListObjectVersionsAsync(
        string bucketName,
        string? prefix,
        string? delimiter,
        string? keyMarker,
        string? versionIdMarker,
        int? maxKeys,
        CancellationToken cancellationToken = default)
    {
        var request = new ListVersionsRequest
        {
            BucketName = bucketName,
            Prefix = prefix,
            Delimiter = delimiter,
            KeyMarker = keyMarker,
            VersionIdMarker = versionIdMarker
        };

        if (maxKeys.HasValue)
            request.MaxKeys = maxKeys.Value;

        var response = await _s3.ListVersionsAsync(request, cancellationToken).ConfigureAwait(false);

        // In SDK v4, Versions contains both object versions and delete markers (distinguished by IsDeleteMarker)
        var entries = (response.Versions ?? [])
            .Select(v => new S3ObjectEntry(
                Key: v.Key ?? string.Empty,
                ContentLength: v.IsDeleteMarker == true ? 0 : (v.Size ?? 0),
                ContentType: null,
                ETag: v.IsDeleteMarker == true ? null : v.ETag,
                LastModifiedUtc: ToDateTimeOffset(v.LastModified),
                Metadata: null,
                VersionId: v.VersionId,
                IsLatest: v.IsLatest == true,
                IsDeleteMarker: v.IsDeleteMarker == true))
            .ToList();

        return new S3ObjectVersionListPage(
            entries,
            response.IsTruncated == true ? response.NextKeyMarker : null,
            response.IsTruncated == true ? response.NextVersionIdMarker : null);
    }

    // -------------------------------------------------------------------------
    // Object CRUD
    // -------------------------------------------------------------------------

    public async Task<S3ObjectEntry?> HeadObjectAsync(
        string bucketName,
        string key,
        string? versionId,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var request = new GetObjectMetadataRequest
            {
                BucketName = bucketName,
                Key = key,
                VersionId = versionId
            };

            var response = await _s3.GetObjectMetadataAsync(request, cancellationToken).ConfigureAwait(false);

            return new S3ObjectEntry(
                Key: key,
                ContentLength: response.ContentLength,
                ContentType: response.ContentType,
                ETag: response.ETag,
                LastModifiedUtc: ToDateTimeOffset(response.LastModified),
                Metadata: BuildMetadataDictionary(response.Metadata),
                VersionId: response.VersionId);
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task<S3GetObjectResult> GetObjectAsync(
        string bucketName,
        string key,
        string? versionId,
        ObjectRange? range,
        string? ifMatchETag,
        string? ifNoneMatchETag,
        DateTimeOffset? ifModifiedSinceUtc,
        DateTimeOffset? ifUnmodifiedSinceUtc,
        CancellationToken cancellationToken = default)
    {
        var request = new Amazon.S3.Model.GetObjectRequest
        {
            BucketName = bucketName,
            Key = key,
            VersionId = versionId
        };

        if (range?.Start.HasValue == true || range?.End.HasValue == true)
        {
            request.ByteRange = range switch
            {
                { Start: not null, End: not null } => new ByteRange(range.Start.Value, range.End.Value),
                { Start: not null } => new ByteRange($"bytes={range.Start.Value}-"),
                { End: not null } => new ByteRange($"bytes=-{range.End.Value}"),
                _ => null
            };
        }

        if (!string.IsNullOrEmpty(ifMatchETag))
            request.EtagToMatch = ifMatchETag;

        if (!string.IsNullOrEmpty(ifNoneMatchETag))
            request.EtagToNotMatch = ifNoneMatchETag;

        if (ifModifiedSinceUtc.HasValue)
            request.ModifiedSinceDate = ifModifiedSinceUtc.Value.UtcDateTime;

        if (ifUnmodifiedSinceUtc.HasValue)
            request.UnmodifiedSinceDate = ifUnmodifiedSinceUtc.Value.UtcDateTime;

        var response = await _s3.GetObjectAsync(request, cancellationToken).ConfigureAwait(false);

        var entry = new S3ObjectEntry(
            Key: key,
            ContentLength: response.ContentLength,
            ContentType: response.Headers.ContentType,
            ETag: response.ETag,
            LastModifiedUtc: ToDateTimeOffset(response.LastModified),
            Metadata: BuildMetadataDictionary(response.Metadata),
            VersionId: response.VersionId);

        long totalContentLength = TryParseContentRangeTotal(response.ContentRange)
            ?? response.ContentLength;

        return new S3GetObjectResult(entry, response.ResponseStream, totalContentLength, response);
    }

    public async Task<S3ObjectEntry> PutObjectAsync(
        string bucketName,
        string key,
        Stream content,
        long? contentLength,
        string? contentType,
        IReadOnlyDictionary<string, string>? metadata,
        CancellationToken cancellationToken = default)
    {
        var request = new Amazon.S3.Model.PutObjectRequest
        {
            BucketName = bucketName,
            Key = key,
            InputStream = content,
            ContentType = contentType ?? "application/octet-stream",
            AutoCloseStream = false
        };

        if (contentLength.HasValue)
            request.Headers.ContentLength = contentLength.Value;

        if (metadata is not null)
        {
            foreach (var (k, v) in metadata)
                request.Metadata[k] = v;
        }

        var response = await _s3.PutObjectAsync(request, cancellationToken).ConfigureAwait(false);

        return new S3ObjectEntry(
            Key: key,
            ContentLength: contentLength ?? 0,
            ContentType: contentType,
            ETag: response.ETag,
            LastModifiedUtc: DateTimeOffset.UtcNow,
            Metadata: metadata,
            VersionId: response.VersionId);
    }

    public async Task<S3DeleteObjectResult> DeleteObjectAsync(
        string bucketName,
        string key,
        string? versionId,
        CancellationToken cancellationToken = default)
    {
        var request = new Amazon.S3.Model.DeleteObjectRequest
        {
            BucketName = bucketName,
            Key = key,
            VersionId = versionId
        };

        var response = await _s3.DeleteObjectAsync(request, cancellationToken).ConfigureAwait(false);

        // In SDK v4, DeleteMarker is a string ("true"/"false") not a bool
        var isDeleteMarker = string.Equals(response.DeleteMarker, "true", StringComparison.OrdinalIgnoreCase);

        return new S3DeleteObjectResult(key, response.VersionId, isDeleteMarker);
    }

    // -------------------------------------------------------------------------
    // Object tags
    // -------------------------------------------------------------------------

    public async Task<IReadOnlyDictionary<string, string>> GetObjectTagsAsync(
        string bucketName,
        string key,
        string? versionId,
        CancellationToken cancellationToken = default)
    {
        var request = new GetObjectTaggingRequest
        {
            BucketName = bucketName,
            Key = key,
            VersionId = versionId
        };

        var response = await _s3.GetObjectTaggingAsync(request, cancellationToken).ConfigureAwait(false);

        return response.Tagging
            .ToDictionary(t => t.Key, t => t.Value, StringComparer.Ordinal);
    }

    public async Task PutObjectTagsAsync(
        string bucketName,
        string key,
        string? versionId,
        IReadOnlyDictionary<string, string> tags,
        CancellationToken cancellationToken = default)
    {
        var request = new PutObjectTaggingRequest
        {
            BucketName = bucketName,
            Key = key,
            VersionId = versionId,
            Tagging = new Tagging
            {
                TagSet = tags.Select(kvp => new Tag { Key = kvp.Key, Value = kvp.Value }).ToList()
            }
        };

        await _s3.PutObjectTaggingAsync(request, cancellationToken).ConfigureAwait(false);
    }

    public async Task DeleteObjectTagsAsync(
        string bucketName,
        string key,
        string? versionId,
        CancellationToken cancellationToken = default)
    {
        var request = new DeleteObjectTaggingRequest
        {
            BucketName = bucketName,
            Key = key,
            VersionId = versionId
        };

        await _s3.DeleteObjectTaggingAsync(request, cancellationToken).ConfigureAwait(false);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    public void Dispose() => _s3.Dispose();

    private static DateTimeOffset ToDateTimeOffset(DateTime? value)
        => value.HasValue ? new DateTimeOffset(DateTime.SpecifyKind(value.Value, DateTimeKind.Utc)) : DateTimeOffset.UtcNow;

    private static IReadOnlyDictionary<string, string>? BuildMetadataDictionary(MetadataCollection? metadata)
    {
        if (metadata is null || metadata.Count == 0)
            return null;

        var dict = new Dictionary<string, string>(metadata.Count, StringComparer.OrdinalIgnoreCase);
        foreach (var key in metadata.Keys)
            dict[key] = metadata[key];

        return dict;
    }

    private static BucketVersioningStatus MapVersioningStatus(Amazon.S3.VersionStatus? sdkStatus)
    {
        if (sdkStatus == Amazon.S3.VersionStatus.Enabled)
            return BucketVersioningStatus.Enabled;
        if (sdkStatus == Amazon.S3.VersionStatus.Suspended)
            return BucketVersioningStatus.Suspended;
        return BucketVersioningStatus.Disabled;
    }

    private static Amazon.S3.VersionStatus MapVersioningStatusToSdk(BucketVersioningStatus status) => status switch
    {
        BucketVersioningStatus.Enabled => Amazon.S3.VersionStatus.Enabled,
        BucketVersioningStatus.Suspended => Amazon.S3.VersionStatus.Suspended,
        _ => Amazon.S3.VersionStatus.Off
    };

    private static long? TryParseContentRangeTotal(string? contentRange)
    {
        // Content-Range: bytes 0-499/1234
        if (string.IsNullOrEmpty(contentRange))
            return null;

        var slashIndex = contentRange.LastIndexOf('/');
        if (slashIndex < 0 || slashIndex == contentRange.Length - 1)
            return null;

        var totalStr = contentRange.AsSpan(slashIndex + 1);
        return long.TryParse(totalStr, out var total) ? total : null;
    }
}
