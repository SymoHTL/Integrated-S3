using System.Runtime.CompilerServices;
using Amazon.S3;
using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Provider.S3.Internal;

namespace IntegratedS3.Provider.S3;

internal sealed class S3StorageService(S3StorageOptions options, IS3StorageClient client) : IStorageBackend
{
    private readonly IS3StorageClient _client = client;
    public string Name => options.ProviderName;
    public string Kind => "s3";
    public bool IsPrimary => options.IsPrimary;
    public string? Description => $"Native S3-backed provider targeting '{(string.IsNullOrWhiteSpace(options.ServiceUrl) ? options.Region : options.ServiceUrl)}'.";

    public ValueTask<StorageCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(S3StorageCapabilities.CreateDefault(options));
    }

    public ValueTask<StorageSupportStateDescriptor> GetSupportStateDescriptorAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(new StorageSupportStateDescriptor());
    }

    // -------------------------------------------------------------------------
    // Bucket operations
    // -------------------------------------------------------------------------

    public async IAsyncEnumerable<BucketInfo> ListBucketsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Materialize the list before yielding so exceptions are not thrown inside the iterator body.
        var entries = await _client.ListBucketsAsync(cancellationToken).ConfigureAwait(false);

        foreach (var entry in entries)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return new BucketInfo
            {
                Name = entry.Name,
                CreatedAtUtc = entry.CreatedAtUtc
            };
        }
    }

    public async ValueTask<StorageResult<BucketInfo>> CreateBucketAsync(CreateBucketRequest request, CancellationToken cancellationToken = default)
    {
        if (request.EnableVersioning)
        {
            return StorageResult<BucketInfo>.Failure(StorageError.Unsupported(
                "Versioning cannot be enabled at bucket creation time via the S3 provider. Create the bucket first, then call PutBucketVersioningAsync.",
                request.BucketName));
        }

        try
        {
            var entry = await _client.CreateBucketAsync(request.BucketName, cancellationToken).ConfigureAwait(false);
            return StorageResult<BucketInfo>.Success(new BucketInfo
            {
                Name = entry.Name,
                CreatedAtUtc = entry.CreatedAtUtc
            });
        }
        catch (AmazonS3Exception ex)
        {
            return StorageResult<BucketInfo>.Failure(S3ErrorTranslator.Translate(ex, Name, request.BucketName));
        }
    }

    public async ValueTask<StorageResult<BucketVersioningInfo>> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        try
        {
            var entry = await _client.GetBucketVersioningAsync(bucketName, cancellationToken).ConfigureAwait(false);
            return StorageResult<BucketVersioningInfo>.Success(new BucketVersioningInfo
            {
                BucketName = bucketName,
                Status = entry.Status
            });
        }
        catch (AmazonS3Exception ex)
        {
            return StorageResult<BucketVersioningInfo>.Failure(S3ErrorTranslator.Translate(ex, Name, bucketName));
        }
    }

    public async ValueTask<StorageResult<BucketVersioningInfo>> PutBucketVersioningAsync(PutBucketVersioningRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            var entry = await _client.SetBucketVersioningAsync(request.BucketName, request.Status, cancellationToken).ConfigureAwait(false);
            return StorageResult<BucketVersioningInfo>.Success(new BucketVersioningInfo
            {
                BucketName = request.BucketName,
                Status = entry.Status
            });
        }
        catch (AmazonS3Exception ex)
        {
            return StorageResult<BucketVersioningInfo>.Failure(S3ErrorTranslator.Translate(ex, Name, request.BucketName));
        }
    }

    public async ValueTask<StorageResult<BucketInfo>> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        try
        {
            var entry = await _client.HeadBucketAsync(bucketName, cancellationToken).ConfigureAwait(false);
            if (entry is null)
            {
                return StorageResult<BucketInfo>.Failure(new StorageError
                {
                    Code = StorageErrorCode.BucketNotFound,
                    Message = $"Bucket '{bucketName}' does not exist.",
                    BucketName = bucketName,
                    ProviderName = Name,
                    SuggestedHttpStatusCode = 404
                });
            }

            return StorageResult<BucketInfo>.Success(new BucketInfo
            {
                Name = entry.Name,
                CreatedAtUtc = entry.CreatedAtUtc
            });
        }
        catch (AmazonS3Exception ex)
        {
            return StorageResult<BucketInfo>.Failure(S3ErrorTranslator.Translate(ex, Name, bucketName));
        }
    }

    public async ValueTask<StorageResult> DeleteBucketAsync(DeleteBucketRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            await _client.DeleteBucketAsync(request.BucketName, cancellationToken).ConfigureAwait(false);
            return StorageResult.Success();
        }
        catch (AmazonS3Exception ex)
        {
            return StorageResult.Failure(S3ErrorTranslator.Translate(ex, Name, request.BucketName));
        }
    }

    // -------------------------------------------------------------------------
    // Object listing
    // -------------------------------------------------------------------------

    public async IAsyncEnumerable<ObjectInfo> ListObjectsAsync(ListObjectsRequest request, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.PageSize is <= 0)
            throw new ArgumentException("Page size must be greater than zero.", nameof(request));

        string? continuationToken = request.ContinuationToken;
        var remaining = request.PageSize;

        do
        {
            cancellationToken.ThrowIfCancellationRequested();

            S3ObjectListPage page;
            try
            {
                page = await _client.ListObjectsAsync(
                    request.BucketName,
                    request.Prefix,
                    continuationToken,
                    remaining,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (AmazonS3Exception ex)
            {
                throw new InvalidOperationException(
                    S3ErrorTranslator.Translate(ex, Name, request.BucketName).Message, ex);
            }

            foreach (var entry in page.Entries)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return EntryToObjectInfo(request.BucketName, entry);

                if (remaining.HasValue)
                {
                    remaining--;
                    if (remaining <= 0)
                        yield break;
                }
            }

            continuationToken = page.NextContinuationToken;
        }
        while (continuationToken is not null);
    }

    public async IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(ListObjectVersionsRequest request, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.PageSize is <= 0)
            throw new ArgumentException("Page size must be greater than zero.", nameof(request));

        string? keyMarker = request.KeyMarker;
        string? versionIdMarker = request.VersionIdMarker;
        var remaining = request.PageSize;

        do
        {
            cancellationToken.ThrowIfCancellationRequested();

            S3ObjectVersionListPage page;
            try
            {
                page = await _client.ListObjectVersionsAsync(
                    request.BucketName,
                    request.Prefix,
                    request.Delimiter,
                    keyMarker,
                    versionIdMarker,
                    remaining,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (AmazonS3Exception ex)
            {
                throw new InvalidOperationException(
                    S3ErrorTranslator.Translate(ex, Name, request.BucketName).Message, ex);
            }

            foreach (var entry in page.Entries)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return EntryToObjectInfo(request.BucketName, entry);

                if (remaining.HasValue)
                {
                    remaining--;
                    if (remaining <= 0)
                        yield break;
                }
            }

            keyMarker = page.NextKeyMarker;
            versionIdMarker = page.NextVersionIdMarker;
        }
        while (keyMarker is not null || versionIdMarker is not null);
    }

    // -------------------------------------------------------------------------
    // Object CRUD
    // -------------------------------------------------------------------------

    public async ValueTask<StorageResult<ObjectInfo>> HeadObjectAsync(HeadObjectRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            var entry = await _client.HeadObjectAsync(request.BucketName, request.Key, request.VersionId, cancellationToken).ConfigureAwait(false);
            if (entry is null)
            {
                return StorageResult<ObjectInfo>.Failure(ObjectNotFound(request.BucketName, request.Key, request.VersionId));
            }

            var info = EntryToObjectInfo(request.BucketName, entry);
            var preconditionError = EvaluatePreconditions(request.IfMatchETag, request.IfNoneMatchETag, request.IfModifiedSinceUtc, request.IfUnmodifiedSinceUtc, info);
            if (preconditionError is not null)
                return StorageResult<ObjectInfo>.Failure(preconditionError);

            return StorageResult<ObjectInfo>.Success(info);
        }
        catch (AmazonS3Exception ex)
        {
            return StorageResult<ObjectInfo>.Failure(S3ErrorTranslator.Translate(ex, Name, request.BucketName, request.Key));
        }
    }

    public async ValueTask<StorageResult<GetObjectResponse>> GetObjectAsync(GetObjectRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            var result = await _client.GetObjectAsync(
                request.BucketName,
                request.Key,
                request.VersionId,
                request.Range,
                request.IfMatchETag,
                request.IfNoneMatchETag,
                request.IfModifiedSinceUtc,
                request.IfUnmodifiedSinceUtc,
                cancellationToken).ConfigureAwait(false);

            var objectInfo = EntryToObjectInfo(request.BucketName, result.Entry);

            return StorageResult<GetObjectResponse>.Success(new GetObjectResponse
            {
                Object = objectInfo,
                Content = result.Content,
                TotalContentLength = result.TotalContentLength,
                Range = NormalizeRange(request.Range, result.TotalContentLength)
            });
        }
        catch (AmazonS3Exception ex) when ((int)ex.StatusCode == 304)
        {
            // If-None-Match matched — not modified. Retrieve metadata to return a complete ObjectInfo.
            var headEntry = await _client.HeadObjectAsync(request.BucketName, request.Key, request.VersionId, cancellationToken).ConfigureAwait(false);
            var objectInfo = headEntry is not null
                ? EntryToObjectInfo(request.BucketName, headEntry)
                : new ObjectInfo { BucketName = request.BucketName, Key = request.Key, VersionId = request.VersionId };

            return StorageResult<GetObjectResponse>.Success(new GetObjectResponse
            {
                Object = objectInfo,
                Content = Stream.Null,
                TotalContentLength = objectInfo.ContentLength,
                IsNotModified = true
            });
        }
        catch (AmazonS3Exception ex)
        {
            return StorageResult<GetObjectResponse>.Failure(S3ErrorTranslator.Translate(ex, Name, request.BucketName, request.Key));
        }
    }

    public async ValueTask<StorageResult<ObjectInfo>> PutObjectAsync(PutObjectRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            var entry = await _client.PutObjectAsync(
                request.BucketName,
                request.Key,
                request.Content,
                request.ContentLength,
                request.ContentType,
                request.Metadata,
                cancellationToken).ConfigureAwait(false);

            return StorageResult<ObjectInfo>.Success(EntryToObjectInfo(request.BucketName, entry));
        }
        catch (AmazonS3Exception ex)
        {
            return StorageResult<ObjectInfo>.Failure(S3ErrorTranslator.Translate(ex, Name, request.BucketName, request.Key));
        }
    }

    public async ValueTask<StorageResult<DeleteObjectResult>> DeleteObjectAsync(DeleteObjectRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            var result = await _client.DeleteObjectAsync(request.BucketName, request.Key, request.VersionId, cancellationToken).ConfigureAwait(false);

            return StorageResult<DeleteObjectResult>.Success(new DeleteObjectResult
            {
                BucketName = request.BucketName,
                Key = request.Key,
                VersionId = result.VersionId,
                IsDeleteMarker = result.IsDeleteMarker
            });
        }
        catch (AmazonS3Exception ex)
        {
            return StorageResult<DeleteObjectResult>.Failure(S3ErrorTranslator.Translate(ex, Name, request.BucketName, request.Key));
        }
    }

    // -------------------------------------------------------------------------
    // Object tags
    // -------------------------------------------------------------------------

    public async ValueTask<StorageResult<ObjectTagSet>> GetObjectTagsAsync(GetObjectTagsRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            var tags = await _client.GetObjectTagsAsync(request.BucketName, request.Key, request.VersionId, cancellationToken).ConfigureAwait(false);
            return StorageResult<ObjectTagSet>.Success(new ObjectTagSet
            {
                BucketName = request.BucketName,
                Key = request.Key,
                VersionId = request.VersionId,
                Tags = tags
            });
        }
        catch (AmazonS3Exception ex)
        {
            return StorageResult<ObjectTagSet>.Failure(S3ErrorTranslator.Translate(ex, Name, request.BucketName, request.Key));
        }
    }

    public async ValueTask<StorageResult<ObjectTagSet>> PutObjectTagsAsync(PutObjectTagsRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            await _client.PutObjectTagsAsync(request.BucketName, request.Key, request.VersionId, request.Tags, cancellationToken).ConfigureAwait(false);
            return StorageResult<ObjectTagSet>.Success(new ObjectTagSet
            {
                BucketName = request.BucketName,
                Key = request.Key,
                VersionId = request.VersionId,
                Tags = request.Tags
            });
        }
        catch (AmazonS3Exception ex)
        {
            return StorageResult<ObjectTagSet>.Failure(S3ErrorTranslator.Translate(ex, Name, request.BucketName, request.Key));
        }
    }

    public async ValueTask<StorageResult<ObjectTagSet>> DeleteObjectTagsAsync(DeleteObjectTagsRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            await _client.DeleteObjectTagsAsync(request.BucketName, request.Key, request.VersionId, cancellationToken).ConfigureAwait(false);
            return StorageResult<ObjectTagSet>.Success(new ObjectTagSet
            {
                BucketName = request.BucketName,
                Key = request.Key,
                VersionId = request.VersionId,
                Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            });
        }
        catch (AmazonS3Exception ex)
        {
            return StorageResult<ObjectTagSet>.Failure(S3ErrorTranslator.Translate(ex, Name, request.BucketName, request.Key));
        }
    }

    // -------------------------------------------------------------------------
    // Unsupported operations (multipart + copy)
    // -------------------------------------------------------------------------

    public ValueTask<StorageResult<ObjectInfo>> CopyObjectAsync(CopyObjectRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<ObjectInfo>.Failure(StorageError.Unsupported("Copy operations are not yet enabled for the S3 provider.", request.SourceBucketName, request.SourceKey)));

    public ValueTask<StorageResult<MultipartUploadInfo>> InitiateMultipartUploadAsync(InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<MultipartUploadInfo>.Failure(StorageError.Unsupported("Multipart upload operations are not yet enabled for the S3 provider.", request.BucketName, request.Key)));

    public ValueTask<StorageResult<MultipartUploadPart>> UploadMultipartPartAsync(UploadMultipartPartRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<MultipartUploadPart>.Failure(StorageError.Unsupported("Multipart upload operations are not yet enabled for the S3 provider.", request.BucketName, request.Key)));

    public ValueTask<StorageResult<ObjectInfo>> CompleteMultipartUploadAsync(CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<ObjectInfo>.Failure(StorageError.Unsupported("Multipart upload operations are not yet enabled for the S3 provider.", request.BucketName, request.Key)));

    public ValueTask<StorageResult> AbortMultipartUploadAsync(AbortMultipartUploadRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult.Failure(StorageError.Unsupported("Multipart upload operations are not yet enabled for the S3 provider.", request.BucketName, request.Key)));

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static ObjectInfo EntryToObjectInfo(string bucketName, S3ObjectEntry entry) => new()
    {
        BucketName = bucketName,
        Key = entry.Key,
        VersionId = entry.VersionId,
        IsLatest = entry.IsLatest,
        IsDeleteMarker = entry.IsDeleteMarker,
        ContentLength = entry.ContentLength,
        ContentType = entry.ContentType,
        ETag = entry.ETag,
        LastModifiedUtc = entry.LastModifiedUtc,
        Metadata = entry.Metadata
    };

    private StorageError ObjectNotFound(string bucketName, string key, string? versionId) => new()
    {
        Code = StorageErrorCode.ObjectNotFound,
        Message = versionId is not null
            ? $"Object '{key}' version '{versionId}' does not exist in bucket '{bucketName}'."
            : $"Object '{key}' does not exist in bucket '{bucketName}'.",
        BucketName = bucketName,
        ObjectKey = key,
        ProviderName = Name,
        SuggestedHttpStatusCode = 404
    };

    /// <summary>
    /// Evaluates precondition headers against the resolved object metadata.
    /// Returns a <see cref="StorageError"/> if a precondition failed (412) or null if all conditions pass.
    /// Note: 304 Not Modified is handled as a successful response upstream, not via this method.
    /// </summary>
    private StorageError? EvaluatePreconditions(
        string? ifMatchETag,
        string? ifNoneMatchETag,
        DateTimeOffset? ifModifiedSinceUtc,
        DateTimeOffset? ifUnmodifiedSinceUtc,
        ObjectInfo info)
    {
        if (!string.IsNullOrEmpty(ifMatchETag) && info.ETag is not null
            && !string.Equals(NormalizeETag(info.ETag), NormalizeETag(ifMatchETag), StringComparison.OrdinalIgnoreCase))
        {
            return new StorageError
            {
                Code = StorageErrorCode.PreconditionFailed,
                Message = $"ETag mismatch for object '{info.Key}' in bucket '{info.BucketName}'.",
                BucketName = info.BucketName,
                ObjectKey = info.Key,
                ProviderName = Name,
                SuggestedHttpStatusCode = 412
            };
        }

        if (ifUnmodifiedSinceUtc.HasValue && info.LastModifiedUtc > ifUnmodifiedSinceUtc.Value)
        {
            return new StorageError
            {
                Code = StorageErrorCode.PreconditionFailed,
                Message = $"Object '{info.Key}' in bucket '{info.BucketName}' was modified after '{ifUnmodifiedSinceUtc.Value:O}'.",
                BucketName = info.BucketName,
                ObjectKey = info.Key,
                ProviderName = Name,
                SuggestedHttpStatusCode = 412
            };
        }

        return null;
    }

    private static ObjectRange? NormalizeRange(ObjectRange? requestedRange, long totalContentLength)
    {
        if (requestedRange is null || totalContentLength <= 0)
            return null;

        var lastByte = totalContentLength - 1;
        if (requestedRange.Start.HasValue)
        {
            var start = Math.Max(requestedRange.Start.Value, 0);
            var end = requestedRange.End.HasValue
                ? Math.Min(requestedRange.End.Value, lastByte)
                : lastByte;

            return new ObjectRange { Start = start, End = end };
        }

        if (!requestedRange.End.HasValue || requestedRange.End.Value <= 0)
            return null;

        var suffixLength = requestedRange.End.Value;
        var startOffset = Math.Max(totalContentLength - suffixLength, 0);
        return new ObjectRange { Start = startOffset, End = lastByte };
    }

    private static string NormalizeETag(string etag) =>
        etag.StartsWith('"') && etag.EndsWith('"') ? etag : $"\"{etag}\"";
}
