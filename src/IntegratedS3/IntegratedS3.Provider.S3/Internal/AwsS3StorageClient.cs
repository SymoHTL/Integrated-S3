using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using IntegratedS3.Abstractions.Models;
using System.Globalization;

namespace IntegratedS3.Provider.S3.Internal;

internal sealed class AwsS3StorageClient : IS3StorageClient
{
    private readonly IAmazonS3 _s3;
    private readonly Uri? _serviceUri;

    public AwsS3StorageClient(S3StorageOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        _serviceUri = TryCreateAbsoluteUri(options.ServiceUrl);
        var config = CreateConfig(options);
        _s3 = !string.IsNullOrWhiteSpace(options.AccessKey) && !string.IsNullOrWhiteSpace(options.SecretKey)
            ? new AmazonS3Client(new BasicAWSCredentials(options.AccessKey, options.SecretKey), config)
            : new AmazonS3Client(config);
    }

    internal static AmazonS3Config CreateConfig(S3StorageOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var config = new AmazonS3Config
        {
            ForcePathStyle = options.ForcePathStyle
        };

        if (!string.IsNullOrWhiteSpace(options.ServiceUrl))
        {
            config.ServiceURL = options.ServiceUrl;
            config.AuthenticationRegion = options.Region;

            if (TryCreateAbsoluteUri(options.ServiceUrl) is { } serviceUri)
                config.UseHttp = string.Equals(serviceUri.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase);

            // Local S3-compatible endpoints frequently lag the AWS SDK v4
            // flexible-checksum defaults; required-only mode preserves explicit
            // checksum flows without injecting extra compatibility failures.
            config.RequestChecksumCalculation = RequestChecksumCalculation.WHEN_REQUIRED;
            config.ResponseChecksumValidation = ResponseChecksumValidation.WHEN_REQUIRED;
        }
        else
        {
            config.RegionEndpoint = RegionEndpoint.GetBySystemName(options.Region);
        }

        return config;
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

    public async Task<S3BucketLocationEntry> GetBucketLocationAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new GetBucketLocationRequest { BucketName = bucketName };
        var response = await _s3.GetBucketLocationAsync(request, cancellationToken).ConfigureAwait(false);
        return new S3BucketLocationEntry(response.Location?.Value);
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
    // Bucket CORS
    // -------------------------------------------------------------------------

    public async Task<S3CorsConfigurationEntry?> GetBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new GetCORSConfigurationRequest
        {
            BucketName = bucketName
        };

        var response = await _s3.GetCORSConfigurationAsync(request, cancellationToken).ConfigureAwait(false);
        return new S3CorsConfigurationEntry(
            (response.Configuration?.Rules ?? [])
                .Select(MapCorsRule)
                .ToArray());
    }

    public async Task<S3CorsConfigurationEntry> SetBucketCorsAsync(string bucketName, IReadOnlyList<S3CorsRuleEntry> rules, CancellationToken cancellationToken = default)
    {
        var request = new PutCORSConfigurationRequest
        {
            BucketName = bucketName,
            Configuration = new CORSConfiguration
            {
                Rules = rules.Select(MapCorsRule).ToList()
            }
        };

        await _s3.PutCORSConfigurationAsync(request, cancellationToken).ConfigureAwait(false);
        return new S3CorsConfigurationEntry(rules.ToArray());
    }

    public async Task DeleteBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new DeleteCORSConfigurationRequest
        {
            BucketName = bucketName
        };

        await _s3.DeleteCORSConfigurationAsync(request, cancellationToken).ConfigureAwait(false);
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
                VersionId: response.VersionId,
                CacheControl: response.CacheControl,
                ContentDisposition: response.ContentDisposition,
                ContentEncoding: response.ContentEncoding,
                ContentLanguage: response.ContentLanguage,
                ExpiresUtc: ParseExpiresString(response.ExpiresString),
                ServerSideEncryption: S3ServerSideEncryptionMapper.ToInfo(
                    response.ServerSideEncryptionMethod,
                    response.ServerSideEncryptionKeyManagementServiceKeyId));
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public Task<Uri> CreatePresignedGetObjectUrlAsync(
        string bucketName,
        string key,
        string? versionId,
        DateTimeOffset expiresAtUtc,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var request = new GetPreSignedUrlRequest
        {
            BucketName = bucketName,
            Key = key,
            VersionId = versionId,
            Expires = expiresAtUtc.UtcDateTime,
            Verb = HttpVerb.GET
        };

        var presignedUrl = _s3.GetPreSignedURL(request);
        return Task.FromResult(AlignPresignedUrlWithServiceUrl(new Uri(presignedUrl, UriKind.Absolute)));
    }

    public Task<Uri> CreatePresignedPutObjectUrlAsync(
        string bucketName,
        string key,
        string? contentType,
        DateTimeOffset expiresAtUtc,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var request = new GetPreSignedUrlRequest
        {
            BucketName = bucketName,
            Key = key,
            ContentType = contentType,
            Expires = expiresAtUtc.UtcDateTime,
            Verb = HttpVerb.PUT
        };

        var presignedUrl = _s3.GetPreSignedURL(request);
        return Task.FromResult(new Uri(presignedUrl, UriKind.Absolute));
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
            VersionId: response.VersionId,
            CacheControl: response.Headers.CacheControl,
            ContentDisposition: response.Headers.ContentDisposition,
            ContentEncoding: response.Headers.ContentEncoding,
            ContentLanguage: GetHeaderValue(response.Headers, "Content-Language"),
            ExpiresUtc: ParseExpiresString(response.ExpiresString),
            ServerSideEncryption: S3ServerSideEncryptionMapper.ToInfo(
                response.ServerSideEncryptionMethod,
                response.ServerSideEncryptionKeyManagementServiceKeyId));

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
        string? cacheControl,
        string? contentDisposition,
        string? contentEncoding,
        string? contentLanguage,
        DateTimeOffset? expiresUtc,
        IReadOnlyDictionary<string, string>? metadata,
        IReadOnlyDictionary<string, string>? tags,
        IReadOnlyDictionary<string, string>? checksums,
        ObjectServerSideEncryptionSettings? serverSideEncryption,
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

        ApplyStandardObjectHeaders(request.Headers, cacheControl, contentDisposition, contentEncoding, contentLanguage, expiresUtc);

        if (metadata is not null)
        {
            foreach (var (k, v) in metadata)
                request.Metadata[k] = v;
        }

        request.TagSet = BuildTagSet(tags);
        ApplyChecksumHeaders(request, checksumAlgorithm: null, checksums);
        S3ServerSideEncryptionMapper.ApplyTo(request, serverSideEncryption);

        var response = await _s3.PutObjectAsync(request, cancellationToken).ConfigureAwait(false);

        return new S3ObjectEntry(
            Key: key,
            ContentLength: contentLength ?? 0,
            ContentType: contentType,
            ETag: response.ETag,
            LastModifiedUtc: DateTimeOffset.UtcNow,
            Metadata: metadata,
            VersionId: response.VersionId,
            Checksums: BuildChecksums(
                response.ChecksumCRC32,
                response.ChecksumCRC32C,
                response.ChecksumCRC64NVME,
                response.ChecksumSHA1,
                response.ChecksumSHA256),
            CacheControl: cacheControl,
            ContentDisposition: contentDisposition,
            ContentEncoding: contentEncoding,
            ContentLanguage: contentLanguage,
            ExpiresUtc: expiresUtc,
            ServerSideEncryption: S3ServerSideEncryptionMapper.ToInfo(
                response.ServerSideEncryptionMethod,
                response.ServerSideEncryptionKeyManagementServiceKeyId));
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
    // Copy + multipart
    // -------------------------------------------------------------------------

    public async Task<S3ObjectEntry> CopyObjectAsync(
        string sourceBucketName,
        string sourceKey,
        string destinationBucketName,
        string destinationKey,
        string? sourceVersionId,
        string? sourceIfMatchETag,
        string? sourceIfNoneMatchETag,
        DateTimeOffset? sourceIfModifiedSinceUtc,
        DateTimeOffset? sourceIfUnmodifiedSinceUtc,
        CopyObjectMetadataDirective metadataDirective,
        string? contentType,
        string? cacheControl,
        string? contentDisposition,
        string? contentEncoding,
        string? contentLanguage,
        DateTimeOffset? expiresUtc,
        IReadOnlyDictionary<string, string>? metadata,
        bool overwriteIfExists,
        ObjectTaggingDirective taggingDirective,
        IReadOnlyDictionary<string, string>? tags,
        ObjectServerSideEncryptionSettings? destinationServerSideEncryption,
        CancellationToken cancellationToken = default)
    {
        var request = new CopyObjectRequest
        {
            SourceBucket = sourceBucketName,
            SourceKey = sourceKey,
            SourceVersionId = sourceVersionId,
            DestinationBucket = destinationBucketName,
            DestinationKey = destinationKey,
            ETagToMatch = sourceIfMatchETag,
            ETagToNotMatch = sourceIfNoneMatchETag
        };

        if (sourceIfModifiedSinceUtc.HasValue)
            request.ModifiedSinceDate = sourceIfModifiedSinceUtc.Value.UtcDateTime;

        if (sourceIfUnmodifiedSinceUtc.HasValue)
            request.UnmodifiedSinceDate = sourceIfUnmodifiedSinceUtc.Value.UtcDateTime;

        if (!overwriteIfExists)
            request.IfNoneMatch = "*";

        request.MetadataDirective = metadataDirective == CopyObjectMetadataDirective.Replace
            ? S3MetadataDirective.REPLACE
            : S3MetadataDirective.COPY;

        if (metadataDirective == CopyObjectMetadataDirective.Replace)
        {
            request.ContentType = contentType;
            ApplyStandardObjectHeaders(request.Headers, cacheControl, contentDisposition, contentEncoding, contentLanguage, expiresUtc);

            if (metadata is not null)
            {
                foreach (var (name, value) in metadata)
                    request.Metadata[name] = value;
            }
        }

        request.TaggingDirective = taggingDirective == ObjectTaggingDirective.Replace
            ? TaggingDirective.REPLACE
            : TaggingDirective.COPY;

        if (taggingDirective == ObjectTaggingDirective.Replace)
        {
            request.TagSet = BuildTagSet(tags) ?? new List<Tag>();
        }

        S3ServerSideEncryptionMapper.ApplyTo(request, destinationServerSideEncryption);

        var response = await _s3.CopyObjectAsync(request, cancellationToken).ConfigureAwait(false);

        return new S3ObjectEntry(
            Key: destinationKey,
            ContentLength: 0,
            ContentType: metadataDirective == CopyObjectMetadataDirective.Replace ? contentType : null,
            ETag: response.ETag,
            LastModifiedUtc: string.IsNullOrEmpty(response.LastModified) ? DateTimeOffset.UtcNow
                : DateTime.TryParse(response.LastModified, out var dt) ? new DateTimeOffset(dt, TimeSpan.Zero) : DateTimeOffset.UtcNow,
            Metadata: metadataDirective == CopyObjectMetadataDirective.Replace ? metadata : null,
            VersionId: response.VersionId,
            Checksums: BuildChecksums(
                response.ChecksumCRC32,
                response.ChecksumCRC32C,
                response.ChecksumCRC64NVME,
                response.ChecksumSHA1,
                response.ChecksumSHA256),
            CacheControl: metadataDirective == CopyObjectMetadataDirective.Replace ? cacheControl : null,
            ContentDisposition: metadataDirective == CopyObjectMetadataDirective.Replace ? contentDisposition : null,
            ContentEncoding: metadataDirective == CopyObjectMetadataDirective.Replace ? contentEncoding : null,
            ContentLanguage: metadataDirective == CopyObjectMetadataDirective.Replace ? contentLanguage : null,
            ExpiresUtc: metadataDirective == CopyObjectMetadataDirective.Replace ? expiresUtc : null,
            ServerSideEncryption: S3ServerSideEncryptionMapper.ToInfo(
                response.ServerSideEncryptionMethod,
                response.ServerSideEncryptionKeyManagementServiceKeyId));
    }

    public async Task<MultipartUploadInfo> InitiateMultipartUploadAsync(
        string bucketName,
        string key,
        string? contentType,
        string? cacheControl,
        string? contentDisposition,
        string? contentEncoding,
        string? contentLanguage,
        DateTimeOffset? expiresUtc,
        IReadOnlyDictionary<string, string>? metadata,
        IReadOnlyDictionary<string, string>? tags,
        string? checksumAlgorithm,
        ObjectServerSideEncryptionSettings? serverSideEncryption,
        CancellationToken cancellationToken = default)
    {
        var request = new InitiateMultipartUploadRequest
        {
            BucketName = bucketName,
            Key = key,
            ContentType = contentType
        };

        ApplyStandardObjectHeaders(request.Headers, cacheControl, contentDisposition, contentEncoding, contentLanguage, expiresUtc);

        if (metadata is not null)
        {
            foreach (var (name, value) in metadata)
                request.Metadata[name] = value;
        }

        request.TagSet = BuildTagSet(tags);
        var sdkChecksumAlgorithm = MapChecksumAlgorithm(checksumAlgorithm);
        if (sdkChecksumAlgorithm is not null)
            request.ChecksumAlgorithm = sdkChecksumAlgorithm;

        S3ServerSideEncryptionMapper.ApplyTo(request, serverSideEncryption);

        var response = await _s3.InitiateMultipartUploadAsync(request, cancellationToken).ConfigureAwait(false);

        return new MultipartUploadInfo
        {
            BucketName = bucketName,
            Key = key,
            UploadId = response.UploadId,
            InitiatedAtUtc = DateTimeOffset.UtcNow,
            ChecksumAlgorithm = NormalizeChecksumAlgorithm(response.ChecksumAlgorithm?.ToString()) ?? NormalizeChecksumAlgorithm(checksumAlgorithm)
        };
    }

    public async Task<MultipartUploadPart> UploadMultipartPartAsync(
        string bucketName,
        string key,
        string uploadId,
        int partNumber,
        Stream content,
        long? contentLength,
        string? checksumAlgorithm,
        IReadOnlyDictionary<string, string>? checksums,
        CancellationToken cancellationToken = default)
    {
        var request = new UploadPartRequest
        {
            BucketName = bucketName,
            Key = key,
            UploadId = uploadId,
            PartNumber = partNumber,
            InputStream = content
        };

        if (contentLength.HasValue)
            request.PartSize = contentLength.Value;

        ApplyChecksumHeaders(request, checksumAlgorithm, checksums);

        var response = await _s3.UploadPartAsync(request, cancellationToken).ConfigureAwait(false);

        return new MultipartUploadPart
        {
            PartNumber = response.PartNumber.GetValueOrDefault(partNumber),
            ETag = response.ETag ?? string.Empty,
            ContentLength = contentLength ?? 0,
            LastModifiedUtc = DateTimeOffset.UtcNow,
            Checksums = BuildChecksums(
                response.ChecksumCRC32,
                response.ChecksumCRC32C,
                response.ChecksumCRC64NVME,
                response.ChecksumSHA1,
                response.ChecksumSHA256)
        };
    }

    public async Task<S3ObjectEntry> CompleteMultipartUploadAsync(
        string bucketName,
        string key,
        string uploadId,
        IReadOnlyList<MultipartUploadPart> parts,
        CancellationToken cancellationToken = default)
    {
        var request = new CompleteMultipartUploadRequest
        {
            BucketName = bucketName,
            Key = key,
            UploadId = uploadId
        };

        request.PartETags = parts
            .OrderBy(static part => part.PartNumber)
            .Select(CreatePartETag)
            .ToList();

        var response = await _s3.CompleteMultipartUploadAsync(request, cancellationToken).ConfigureAwait(false);

        return new S3ObjectEntry(
            Key: key,
            ContentLength: 0,
            ContentType: null,
            ETag: response.ETag,
            LastModifiedUtc: DateTimeOffset.UtcNow,
            Metadata: null,
            VersionId: response.VersionId,
            Checksums: BuildChecksums(
                response.ChecksumCRC32,
                response.ChecksumCRC32C,
                response.ChecksumCRC64NVME,
                response.ChecksumSHA1,
                response.ChecksumSHA256),
            ServerSideEncryption: S3ServerSideEncryptionMapper.ToInfo(
                response.ServerSideEncryptionMethod,
                response.ServerSideEncryptionKeyManagementServiceKeyId));
    }

    public async Task AbortMultipartUploadAsync(
        string bucketName,
        string key,
        string uploadId,
        CancellationToken cancellationToken = default)
    {
        var request = new AbortMultipartUploadRequest
        {
            BucketName = bucketName,
            Key = key,
            UploadId = uploadId
        };

        await _s3.AbortMultipartUploadAsync(request, cancellationToken).ConfigureAwait(false);
    }

    public async Task<S3MultipartUploadListPage> ListMultipartUploadsAsync(
        string bucketName,
        string? prefix,
        string? keyMarker,
        string? uploadIdMarker,
        int? maxUploads,
        CancellationToken cancellationToken = default)
    {
        var request = new ListMultipartUploadsRequest
        {
            BucketName = bucketName,
            Prefix = prefix,
            KeyMarker = keyMarker,
            UploadIdMarker = uploadIdMarker
        };

        if (maxUploads.HasValue)
            request.MaxUploads = Math.Min(maxUploads.Value, 1000);

        var response = await _s3.ListMultipartUploadsAsync(request, cancellationToken).ConfigureAwait(false);

        var entries = (response.MultipartUploads ?? [])
            .Select(upload => new MultipartUploadInfo
            {
                BucketName = bucketName,
                Key = upload.Key ?? string.Empty,
                UploadId = upload.UploadId ?? string.Empty,
                InitiatedAtUtc = ToDateTimeOffset(upload.Initiated),
                ChecksumAlgorithm = NormalizeChecksumAlgorithm(upload.ChecksumAlgorithm?.ToString())
            })
            .ToList();

        return new S3MultipartUploadListPage(
            entries,
            response.IsTruncated == true ? response.NextKeyMarker : null,
            response.IsTruncated == true ? response.NextUploadIdMarker : null);
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

    private static DateTimeOffset? ParseExpiresString(string? rawValue)
    {
        if (string.IsNullOrWhiteSpace(rawValue))
            return null;

        return DateTimeOffset.TryParse(
            rawValue,
            CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
            out var parsedValue)
            ? parsedValue
            : null;
    }

    private static IReadOnlyDictionary<string, string>? BuildMetadataDictionary(MetadataCollection? metadata)
    {
        if (metadata is null || metadata.Count == 0)
            return null;

        var dict = new Dictionary<string, string>(metadata.Count, StringComparer.OrdinalIgnoreCase);
        foreach (var key in metadata.Keys)
            dict[key] = metadata[key];

        return dict;
    }

    private static IReadOnlyDictionary<string, string>? BuildChecksums(
        string? checksumCRC32,
        string? checksumCRC32C,
        string? checksumCRC64NVME,
        string? checksumSHA1,
        string? checksumSHA256)
    {
        Dictionary<string, string>? checksums = null;

        AddChecksum(ref checksums, "crc32", checksumCRC32);
        AddChecksum(ref checksums, "crc32c", checksumCRC32C);
        AddChecksum(ref checksums, "crc64nvme", checksumCRC64NVME);
        AddChecksum(ref checksums, "sha1", checksumSHA1);
        AddChecksum(ref checksums, "sha256", checksumSHA256);

        return checksums;
    }

    private static void AddChecksum(ref Dictionary<string, string>? checksums, string algorithm, string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return;

        checksums ??= new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        checksums[algorithm] = value;
    }

    private static List<Tag>? BuildTagSet(IReadOnlyDictionary<string, string>? tags)
    {
        return tags is null || tags.Count == 0
            ? null
            : tags.Select(static pair => new Tag
            {
                Key = pair.Key,
                Value = pair.Value
            }).ToList();

    private static void ApplyStandardObjectHeaders(
        HeadersCollection headers,
        string? cacheControl,
        string? contentDisposition,
        string? contentEncoding,
        string? contentLanguage,
        DateTimeOffset? expiresUtc)
    {
        headers.CacheControl = cacheControl;
        headers.ContentDisposition = contentDisposition;
        headers.ContentEncoding = contentEncoding;
        headers["Content-Language"] = contentLanguage;
        headers.Expires = expiresUtc?.UtcDateTime;
    }

    private static string? GetHeaderValue(HeadersCollection headers, string headerName)
    {
        var value = headers[headerName];
        return string.IsNullOrWhiteSpace(value)
            ? null
            : value;
    }

    private static void ApplyChecksumHeaders(PutObjectRequest request, string? checksumAlgorithm, IReadOnlyDictionary<string, string>? checksums)
    {
        var sdkChecksumAlgorithm = MapChecksumAlgorithm(checksumAlgorithm);
        if (sdkChecksumAlgorithm is not null)
            request.ChecksumAlgorithm = sdkChecksumAlgorithm;

        if (TryGetChecksumValue(checksums, "crc32", out var checksumCRC32))
            request.ChecksumCRC32 = checksumCRC32;
        if (TryGetChecksumValue(checksums, "crc32c", out var checksumCRC32C))
            request.ChecksumCRC32C = checksumCRC32C;
        if (TryGetChecksumValue(checksums, "crc64nvme", out var checksumCRC64NVME))
            request.ChecksumCRC64NVME = checksumCRC64NVME;
        if (TryGetChecksumValue(checksums, "sha1", out var checksumSHA1))
            request.ChecksumSHA1 = checksumSHA1;
        if (TryGetChecksumValue(checksums, "sha256", out var checksumSHA256))
            request.ChecksumSHA256 = checksumSHA256;
    }

    private static void ApplyChecksumHeaders(UploadPartRequest request, string? checksumAlgorithm, IReadOnlyDictionary<string, string>? checksums)
    {
        var sdkChecksumAlgorithm = MapChecksumAlgorithm(checksumAlgorithm);
        if (sdkChecksumAlgorithm is not null)
            request.ChecksumAlgorithm = sdkChecksumAlgorithm;

        if (TryGetChecksumValue(checksums, "crc32", out var checksumCRC32))
            request.ChecksumCRC32 = checksumCRC32;
        if (TryGetChecksumValue(checksums, "crc32c", out var checksumCRC32C))
            request.ChecksumCRC32C = checksumCRC32C;
        if (TryGetChecksumValue(checksums, "crc64nvme", out var checksumCRC64NVME))
            request.ChecksumCRC64NVME = checksumCRC64NVME;
        if (TryGetChecksumValue(checksums, "sha1", out var checksumSHA1))
            request.ChecksumSHA1 = checksumSHA1;
        if (TryGetChecksumValue(checksums, "sha256", out var checksumSHA256))
            request.ChecksumSHA256 = checksumSHA256;
    }

    private static PartETag CreatePartETag(MultipartUploadPart part)
    {
        var partETag = new PartETag(part.PartNumber, part.ETag);

        if (TryGetChecksumValue(part.Checksums, "crc32", out var checksumCRC32))
            partETag.ChecksumCRC32 = checksumCRC32;
        if (TryGetChecksumValue(part.Checksums, "crc32c", out var checksumCRC32C))
            partETag.ChecksumCRC32C = checksumCRC32C;
        if (TryGetChecksumValue(part.Checksums, "crc64nvme", out var checksumCRC64NVME))
            partETag.ChecksumCRC64NVME = checksumCRC64NVME;
        if (TryGetChecksumValue(part.Checksums, "sha1", out var checksumSHA1))
            partETag.ChecksumSHA1 = checksumSHA1;
        if (TryGetChecksumValue(part.Checksums, "sha256", out var checksumSHA256))
            partETag.ChecksumSHA256 = checksumSHA256;

        return partETag;
    }

    private static bool TryGetChecksumValue(IReadOnlyDictionary<string, string>? checksums, string algorithm, out string value)
    {
        value = string.Empty;
        if (checksums is null)
            return false;

        if (checksums.TryGetValue(algorithm, out var directValue) && !string.IsNullOrWhiteSpace(directValue))
        {
            value = directValue;
            return true;
        }

        foreach (var checksum in checksums)
        {
            if (string.Equals(checksum.Key, algorithm, StringComparison.OrdinalIgnoreCase)
                && !string.IsNullOrWhiteSpace(checksum.Value))
            {
                value = checksum.Value;
                return true;
            }
        }

        return false;
    }

    private Uri AlignPresignedUrlWithServiceUrl(Uri presignedUrl)
    {
        if (_serviceUri is null
            || string.Equals(presignedUrl.Scheme, _serviceUri.Scheme, StringComparison.OrdinalIgnoreCase))
        {
            return presignedUrl;
        }

        var builder = new UriBuilder(presignedUrl)
        {
            Scheme = _serviceUri.Scheme,
            Port = _serviceUri.IsDefaultPort ? -1 : _serviceUri.Port
        };

        return builder.Uri;
    }

    private static Uri? TryCreateAbsoluteUri(string? value)
    {
        return Uri.TryCreate(value, UriKind.Absolute, out var uri)
            ? uri
            : null;
    }

    private static string? NormalizeChecksumAlgorithm(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        return value switch
        {
            "SHA256" => "sha256",
            "SHA1" => "sha1",
            "CRC32" => "crc32",
            "CRC32C" => "crc32c",
            "CRC64NVME" => "crc64nvme",
            _ => value.ToLowerInvariant()
        };
    }

    private static ChecksumAlgorithm? MapChecksumAlgorithm(string? value)
    {
        return NormalizeChecksumAlgorithm(value) switch
        {
            "sha256" => ChecksumAlgorithm.SHA256,
            "sha1" => ChecksumAlgorithm.SHA1,
            "crc32" => ChecksumAlgorithm.CRC32,
            "crc32c" => ChecksumAlgorithm.CRC32C,
            "crc64nvme" => ChecksumAlgorithm.CRC64NVME,
            _ => null
        };
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

    private static S3CorsRuleEntry MapCorsRule(CORSRule rule)
    {
        return new S3CorsRuleEntry(
            rule.Id,
            (rule.AllowedOrigins ?? []).ToArray(),
            (rule.AllowedMethods ?? []).Select(MapCorsMethod).ToArray(),
            (rule.AllowedHeaders ?? []).ToArray(),
            (rule.ExposeHeaders ?? []).ToArray(),
            rule.MaxAgeSeconds);
    }

    private static CORSRule MapCorsRule(S3CorsRuleEntry rule)
    {
        return new CORSRule
        {
            Id = rule.Id,
            AllowedOrigins = rule.AllowedOrigins.ToList(),
            AllowedMethods = rule.AllowedMethods.Select(MapCorsMethod).ToList(),
            AllowedHeaders = rule.AllowedHeaders.ToList(),
            ExposeHeaders = rule.ExposeHeaders.ToList(),
            MaxAgeSeconds = rule.MaxAgeSeconds
        };
    }

    private static BucketCorsMethod MapCorsMethod(string method)
    {
        return method switch
        {
            "GET" => BucketCorsMethod.Get,
            "PUT" => BucketCorsMethod.Put,
            "POST" => BucketCorsMethod.Post,
            "DELETE" => BucketCorsMethod.Delete,
            "HEAD" => BucketCorsMethod.Head,
            _ => throw new InvalidOperationException($"Unsupported S3 CORS method '{method}'.")
        };
    }

    private static string MapCorsMethod(BucketCorsMethod method)
    {
        return method switch
        {
            BucketCorsMethod.Get => "GET",
            BucketCorsMethod.Put => "PUT",
            BucketCorsMethod.Post => "POST",
            BucketCorsMethod.Delete => "DELETE",
            BucketCorsMethod.Head => "HEAD",
            _ => throw new ArgumentOutOfRangeException(nameof(method), method, "Unsupported CORS method.")
        };
    }

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
