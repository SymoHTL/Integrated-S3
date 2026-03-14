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
        return ValueTask.FromResult(new StorageSupportStateDescriptor
        {
            ObjectMetadata = StorageSupportStateOwnership.Delegated,
            ObjectTags = StorageSupportStateOwnership.Delegated,
            MultipartState = StorageSupportStateOwnership.Delegated,
            Versioning = StorageSupportStateOwnership.Delegated,
            Checksums = StorageSupportStateOwnership.Delegated,
            AccessControl = StorageSupportStateOwnership.Delegated,
            Retention = StorageSupportStateOwnership.Delegated,
            ServerSideEncryption = StorageSupportStateOwnership.Delegated,
            RedirectLocations = StorageSupportStateOwnership.Delegated
        });
    }

    public ValueTask<StorageProviderMode> GetProviderModeAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(StorageProviderMode.Delegated);
    }

    public ValueTask<StorageObjectLocationDescriptor> GetObjectLocationDescriptorAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(new StorageObjectLocationDescriptor
        {
            DefaultAccessMode = StorageObjectAccessMode.Delegated,
            SupportedAccessModes = [StorageObjectAccessMode.Delegated, StorageObjectAccessMode.ProxyStream]
        });
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

    public async ValueTask<StorageResult<BucketLocationInfo>> GetBucketLocationAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        try
        {
            var entry = await _client.GetBucketLocationAsync(bucketName, cancellationToken).ConfigureAwait(false);
            return StorageResult<BucketLocationInfo>.Success(new BucketLocationInfo
            {
                BucketName = bucketName,
                LocationConstraint = string.IsNullOrWhiteSpace(entry.LocationConstraint)
                    ? null
                    : entry.LocationConstraint
            });
        }
        catch (AmazonS3Exception ex)
        {
            return StorageResult<BucketLocationInfo>.Failure(S3ErrorTranslator.Translate(ex, Name, bucketName));
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

    public async ValueTask<StorageResult<BucketCorsConfiguration>> GetBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        try
        {
            var entry = await _client.GetBucketCorsAsync(bucketName, cancellationToken).ConfigureAwait(false);
            if (entry is null)
            {
                return StorageResult<BucketCorsConfiguration>.Failure(CorsConfigurationNotFound(bucketName));
            }

            return StorageResult<BucketCorsConfiguration>.Success(ToBucketCorsConfiguration(bucketName, entry));
        }
        catch (AmazonS3Exception ex)
        {
            return StorageResult<BucketCorsConfiguration>.Failure(S3ErrorTranslator.Translate(ex, Name, bucketName));
        }
    }

    public async ValueTask<StorageResult<BucketCorsConfiguration>> PutBucketCorsAsync(PutBucketCorsRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            var entry = await _client.SetBucketCorsAsync(
                request.BucketName,
                request.Rules.Select(ToS3CorsRule).ToArray(),
                cancellationToken).ConfigureAwait(false);
            return StorageResult<BucketCorsConfiguration>.Success(ToBucketCorsConfiguration(request.BucketName, entry));
        }
        catch (AmazonS3Exception ex)
        {
            return StorageResult<BucketCorsConfiguration>.Failure(S3ErrorTranslator.Translate(ex, Name, request.BucketName));
        }
    }

    public async ValueTask<StorageResult> DeleteBucketCorsAsync(DeleteBucketCorsRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            await _client.DeleteBucketCorsAsync(request.BucketName, cancellationToken).ConfigureAwait(false);
            return StorageResult.Success();
        }
        catch (AmazonS3Exception ex)
        {
            return StorageResult.Failure(S3ErrorTranslator.Translate(ex, Name, request.BucketName));
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

    public async IAsyncEnumerable<MultipartUploadInfo> ListMultipartUploadsAsync(ListMultipartUploadsRequest request, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.PageSize is <= 0)
            throw new ArgumentException("Page size must be greater than zero.", nameof(request));

        string? keyMarker = request.KeyMarker;
        string? uploadIdMarker = request.UploadIdMarker;
        var remaining = request.PageSize;

        do
        {
            cancellationToken.ThrowIfCancellationRequested();

            S3MultipartUploadListPage page;
            try
            {
                page = await _client.ListMultipartUploadsAsync(
                    request.BucketName,
                    request.Prefix,
                    keyMarker,
                    uploadIdMarker,
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
                yield return entry;

                if (remaining.HasValue)
                {
                    remaining--;
                    if (remaining <= 0)
                        yield break;
                }
            }

            keyMarker = page.NextKeyMarker;
            uploadIdMarker = page.NextUploadIdMarker;
        }
        while (keyMarker is not null || uploadIdMarker is not null);
    }

    // -------------------------------------------------------------------------
    // Object CRUD
    // -------------------------------------------------------------------------

    public async ValueTask<StorageResult<ObjectInfo>> HeadObjectAsync(HeadObjectRequest request, CancellationToken cancellationToken = default)
    {
        var serverSideEncryptionError = ValidateReadServerSideEncryptionRequest(request.ServerSideEncryption, request.BucketName, request.Key, "HEAD");
        if (serverSideEncryptionError is not null)
            return StorageResult<ObjectInfo>.Failure(serverSideEncryptionError);

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
        catch (S3ServerSideEncryptionNotSupportedException ex)
        {
            return StorageResult<ObjectInfo>.Failure(StorageError.Unsupported(ex.Message, request.BucketName, request.Key));
        }
    }

    public async ValueTask<StorageResult<GetObjectResponse>> GetObjectAsync(GetObjectRequest request, CancellationToken cancellationToken = default)
    {
        var serverSideEncryptionError = ValidateReadServerSideEncryptionRequest(request.ServerSideEncryption, request.BucketName, request.Key, "GET");
        if (serverSideEncryptionError is not null)
            return StorageResult<GetObjectResponse>.Failure(serverSideEncryptionError);

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
            try
            {
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
            catch (S3ServerSideEncryptionNotSupportedException sseEx)
            {
                return StorageResult<GetObjectResponse>.Failure(StorageError.Unsupported(sseEx.Message, request.BucketName, request.Key));
            }
        }
        catch (AmazonS3Exception ex)
        {
            return StorageResult<GetObjectResponse>.Failure(S3ErrorTranslator.Translate(ex, Name, request.BucketName, request.Key));
        }
        catch (S3ServerSideEncryptionNotSupportedException ex)
        {
            return StorageResult<GetObjectResponse>.Failure(StorageError.Unsupported(ex.Message, request.BucketName, request.Key));
        }
    }

    public async ValueTask<StorageResult<ObjectInfo>> PutObjectAsync(PutObjectRequest request, CancellationToken cancellationToken = default)
    {
        var serverSideEncryptionError = ValidateWriteServerSideEncryptionRequest(request.ServerSideEncryption, request.BucketName, request.Key);
        if (serverSideEncryptionError is not null)
            return StorageResult<ObjectInfo>.Failure(serverSideEncryptionError);

        try
        {
            var entry = await _client.PutObjectAsync(
                request.BucketName,
                request.Key,
                request.Content,
                request.ContentLength,
                request.ContentType,
                request.CacheControl,
                request.ContentDisposition,
                request.ContentEncoding,
                request.ContentLanguage,
                request.ExpiresUtc,
                request.Metadata,
                request.Checksums,
                request.ServerSideEncryption,
                cancellationToken).ConfigureAwait(false);

            return StorageResult<ObjectInfo>.Success(EntryToObjectInfo(request.BucketName, entry));
        }
        catch (AmazonS3Exception ex)
        {
            return StorageResult<ObjectInfo>.Failure(S3ErrorTranslator.Translate(ex, Name, request.BucketName, request.Key));
        }
        catch (S3ServerSideEncryptionNotSupportedException ex)
        {
            return StorageResult<ObjectInfo>.Failure(StorageError.Unsupported(ex.Message, request.BucketName, request.Key));
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
    // Copy + multipart operations
    // -------------------------------------------------------------------------

    public async ValueTask<StorageResult<ObjectInfo>> CopyObjectAsync(CopyObjectRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var sourceServerSideEncryptionError = ValidateCopySourceServerSideEncryptionRequest(
            request.SourceServerSideEncryption,
            request.SourceBucketName,
            request.SourceKey);
        if (sourceServerSideEncryptionError is not null)
            return StorageResult<ObjectInfo>.Failure(sourceServerSideEncryptionError);

        var destinationServerSideEncryptionError = ValidateWriteServerSideEncryptionRequest(
            request.DestinationServerSideEncryption,
            request.DestinationBucketName,
            request.DestinationKey);
        if (destinationServerSideEncryptionError is not null)
            return StorageResult<ObjectInfo>.Failure(destinationServerSideEncryptionError);

        try
        {
            var copiedEntry = await _client.CopyObjectAsync(
                request.SourceBucketName,
                request.SourceKey,
                request.DestinationBucketName,
                request.DestinationKey,
                request.SourceVersionId,
                request.SourceIfMatchETag,
                request.SourceIfNoneMatchETag,
                request.SourceIfModifiedSinceUtc,
                request.SourceIfUnmodifiedSinceUtc,
                request.MetadataDirective,
                request.ContentType,
                request.CacheControl,
                request.ContentDisposition,
                request.ContentEncoding,
                request.ContentLanguage,
                request.ExpiresUtc,
                request.Metadata,
                request.OverwriteIfExists,
                request.DestinationServerSideEncryption,
                cancellationToken).ConfigureAwait(false);

            var enrichedEntry = await EnrichObjectEntryAsync(
                request.DestinationBucketName,
                request.DestinationKey,
                copiedEntry,
                cancellationToken).ConfigureAwait(false);

            return StorageResult<ObjectInfo>.Success(EntryToObjectInfo(request.DestinationBucketName, enrichedEntry));
        }
        catch (AmazonS3Exception ex)
        {
            return StorageResult<ObjectInfo>.Failure(TranslateCopyObjectError(ex, request));
        }
        catch (S3ServerSideEncryptionNotSupportedException ex)
        {
            return StorageResult<ObjectInfo>.Failure(StorageError.Unsupported(ex.Message, request.DestinationBucketName, request.DestinationKey));
        }
    }

    public async ValueTask<StorageResult<MultipartUploadInfo>> InitiateMultipartUploadAsync(InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var serverSideEncryptionError = ValidateWriteServerSideEncryptionRequest(request.ServerSideEncryption, request.BucketName, request.Key);
        if (serverSideEncryptionError is not null)
            return StorageResult<MultipartUploadInfo>.Failure(serverSideEncryptionError);

        try
        {
            var upload = await _client.InitiateMultipartUploadAsync(
                request.BucketName,
                request.Key,
                request.ContentType,
                request.CacheControl,
                request.ContentDisposition,
                request.ContentEncoding,
                request.ContentLanguage,
                request.ExpiresUtc,
                request.Metadata,
                request.ChecksumAlgorithm,
                request.ServerSideEncryption,
                cancellationToken).ConfigureAwait(false);

            return StorageResult<MultipartUploadInfo>.Success(upload);
        }
        catch (AmazonS3Exception ex)
        {
            return StorageResult<MultipartUploadInfo>.Failure(S3ErrorTranslator.Translate(ex, Name, request.BucketName, request.Key));
        }
        catch (S3ServerSideEncryptionNotSupportedException ex)
        {
            return StorageResult<MultipartUploadInfo>.Failure(StorageError.Unsupported(ex.Message, request.BucketName, request.Key));
        }
    }

    public async ValueTask<StorageResult<MultipartUploadPart>> UploadMultipartPartAsync(UploadMultipartPartRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        try
        {
            var part = await _client.UploadMultipartPartAsync(
                request.BucketName,
                request.Key,
                request.UploadId,
                request.PartNumber,
                request.Content,
                request.ContentLength,
                request.ChecksumAlgorithm,
                request.Checksums,
                cancellationToken).ConfigureAwait(false);

            return StorageResult<MultipartUploadPart>.Success(part);
        }
        catch (AmazonS3Exception ex)
        {
            return StorageResult<MultipartUploadPart>.Failure(S3ErrorTranslator.Translate(ex, Name, request.BucketName, request.Key));
        }
    }

    public async ValueTask<StorageResult<ObjectInfo>> CompleteMultipartUploadAsync(CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        try
        {
            var completedEntry = await _client.CompleteMultipartUploadAsync(
                request.BucketName,
                request.Key,
                request.UploadId,
                request.Parts,
                cancellationToken).ConfigureAwait(false);

            var enrichedEntry = await EnrichObjectEntryAsync(
                request.BucketName,
                request.Key,
                completedEntry,
                cancellationToken).ConfigureAwait(false);

            return StorageResult<ObjectInfo>.Success(EntryToObjectInfo(request.BucketName, enrichedEntry));
        }
        catch (AmazonS3Exception ex)
        {
            return StorageResult<ObjectInfo>.Failure(S3ErrorTranslator.Translate(ex, Name, request.BucketName, request.Key));
        }
        catch (S3ServerSideEncryptionNotSupportedException ex)
        {
            return StorageResult<ObjectInfo>.Failure(StorageError.Unsupported(ex.Message, request.BucketName, request.Key));
        }
    }

    public async ValueTask<StorageResult> AbortMultipartUploadAsync(AbortMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        try
        {
            await _client.AbortMultipartUploadAsync(
                request.BucketName,
                request.Key,
                request.UploadId,
                cancellationToken).ConfigureAwait(false);

            return StorageResult.Success();
        }
        catch (AmazonS3Exception ex)
        {
            return StorageResult.Failure(S3ErrorTranslator.Translate(ex, Name, request.BucketName, request.Key));
        }
    }

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
        CacheControl = entry.CacheControl,
        ContentDisposition = entry.ContentDisposition,
        ContentEncoding = entry.ContentEncoding,
        ContentLanguage = entry.ContentLanguage,
        ExpiresUtc = entry.ExpiresUtc,
        ETag = entry.ETag,
        LastModifiedUtc = entry.LastModifiedUtc,
        Metadata = entry.Metadata,
        Checksums = entry.Checksums,
        ServerSideEncryption = entry.ServerSideEncryption
    };

    private async Task<S3ObjectEntry> EnrichObjectEntryAsync(
        string bucketName,
        string key,
        S3ObjectEntry entry,
        CancellationToken cancellationToken)
    {
        try
        {
            var headEntry = await _client.HeadObjectAsync(bucketName, key, entry.VersionId, cancellationToken).ConfigureAwait(false);
            return headEntry is null ? entry : MergeObjectEntries(headEntry, entry);
        }
        catch (AmazonS3Exception)
        {
            return entry;
        }
    }

    private StorageError TranslateCopyObjectError(AmazonS3Exception exception, CopyObjectRequest request)
    {
        if (string.Equals(exception.ErrorCode, "NoSuchKey", StringComparison.OrdinalIgnoreCase))
        {
            return S3ErrorTranslator.Translate(exception, Name, request.SourceBucketName, request.SourceKey);
        }

        if (string.Equals(exception.ErrorCode, "PreconditionFailed", StringComparison.OrdinalIgnoreCase)
            || (int)exception.StatusCode == 412)
        {
            var isDestinationOverwriteConflict = !request.OverwriteIfExists
                && string.IsNullOrWhiteSpace(request.SourceIfMatchETag)
                && string.IsNullOrWhiteSpace(request.SourceIfNoneMatchETag)
                && !request.SourceIfModifiedSinceUtc.HasValue
                && !request.SourceIfUnmodifiedSinceUtc.HasValue;

            return isDestinationOverwriteConflict
                ? S3ErrorTranslator.Translate(exception, Name, request.DestinationBucketName, request.DestinationKey)
                : S3ErrorTranslator.Translate(exception, Name, request.SourceBucketName, request.SourceKey);
        }

        return S3ErrorTranslator.Translate(exception, Name, request.DestinationBucketName, request.DestinationKey);
    }

    private static S3ObjectEntry MergeObjectEntries(S3ObjectEntry preferred, S3ObjectEntry fallback)
    {
        return preferred with
        {
            ContentType = preferred.ContentType ?? fallback.ContentType,
            CacheControl = preferred.CacheControl ?? fallback.CacheControl,
            ContentDisposition = preferred.ContentDisposition ?? fallback.ContentDisposition,
            ContentEncoding = preferred.ContentEncoding ?? fallback.ContentEncoding,
            ContentLanguage = preferred.ContentLanguage ?? fallback.ContentLanguage,
            ExpiresUtc = preferred.ExpiresUtc ?? fallback.ExpiresUtc,
            ETag = preferred.ETag ?? fallback.ETag,
            LastModifiedUtc = preferred.LastModifiedUtc == default ? fallback.LastModifiedUtc : preferred.LastModifiedUtc,
            Metadata = MergeValueDictionaries(preferred.Metadata, fallback.Metadata),
            VersionId = preferred.VersionId ?? fallback.VersionId,
            Checksums = MergeValueDictionaries(preferred.Checksums, fallback.Checksums),
            ServerSideEncryption = preferred.ServerSideEncryption ?? fallback.ServerSideEncryption
        };
    }

    private StorageError? ValidateReadServerSideEncryptionRequest(
        ObjectServerSideEncryptionSettings? serverSideEncryption,
        string bucketName,
        string key,
        string operation)
    {
        if (serverSideEncryption is null)
            return null;

        return StorageError.Unsupported(
            $"The S3 provider does not accept managed server-side encryption request settings for {operation} object reads. Read-time SSE headers are only used with customer-provided keys.",
            bucketName,
            key);
    }

    private StorageError? ValidateCopySourceServerSideEncryptionRequest(
        ObjectServerSideEncryptionSettings? serverSideEncryption,
        string bucketName,
        string key)
    {
        if (serverSideEncryption is null)
            return null;

        return StorageError.Unsupported(
            "The S3 provider does not support managed copy-source server-side encryption settings. Source-side SSE headers are only used with customer-provided keys.",
            bucketName,
            key);
    }

    private StorageError? ValidateWriteServerSideEncryptionRequest(
        ObjectServerSideEncryptionSettings? serverSideEncryption,
        string bucketName,
        string key)
    {
        if (serverSideEncryption is null)
            return null;

        return serverSideEncryption.Algorithm switch
        {
            ObjectServerSideEncryptionAlgorithm.Aes256 when serverSideEncryption.KeyId is not null || serverSideEncryption.Context is not null
                => StorageError.Unsupported(
                    "AES256 server-side encryption does not support key identifiers or encryption context in the native S3 provider.",
                    bucketName,
                    key),
            ObjectServerSideEncryptionAlgorithm.Aes256 => null,
            ObjectServerSideEncryptionAlgorithm.Kms or ObjectServerSideEncryptionAlgorithm.KmsDsse
                when HasInvalidServerSideEncryptionContext(serverSideEncryption.Context)
                => StorageError.Unsupported(
                    "KMS-managed server-side encryption context keys and values must be non-empty strings in the native S3 provider.",
                    bucketName,
                    key),
            ObjectServerSideEncryptionAlgorithm.KmsDsse => null,
            ObjectServerSideEncryptionAlgorithm.Kms => null,
            _ => StorageError.Unsupported(
                $"Server-side encryption algorithm '{serverSideEncryption.Algorithm}' is not supported by the native S3 provider.",
                bucketName,
                key)
        };
    }

    private static bool HasInvalidServerSideEncryptionContext(IReadOnlyDictionary<string, string>? context)
    {
        if (context is null || context.Count == 0)
            return false;

        foreach (var (name, value) in context)
        {
            if (string.IsNullOrWhiteSpace(name) || string.IsNullOrWhiteSpace(value))
                return true;
        }

        return false;
    }

    private static IReadOnlyDictionary<string, string>? MergeValueDictionaries(
        IReadOnlyDictionary<string, string>? preferred,
        IReadOnlyDictionary<string, string>? fallback)
    {
        if (preferred is null || preferred.Count == 0)
            return fallback;

        if (fallback is null || fallback.Count == 0)
            return preferred;

        var merged = new Dictionary<string, string>(fallback.Count + preferred.Count, StringComparer.OrdinalIgnoreCase);
        foreach (var (key, value) in fallback)
            merged[key] = value;

        foreach (var (key, value) in preferred)
            merged[key] = value;

        return merged;
    }

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

    private StorageError CorsConfigurationNotFound(string bucketName) => new()
    {
        Code = StorageErrorCode.CorsConfigurationNotFound,
        Message = $"Bucket '{bucketName}' does not have a CORS configuration.",
        BucketName = bucketName,
        ProviderName = Name,
        SuggestedHttpStatusCode = 404
    };

    private static BucketCorsConfiguration ToBucketCorsConfiguration(string bucketName, S3CorsConfigurationEntry entry)
    {
        return new BucketCorsConfiguration
        {
            BucketName = bucketName,
            Rules = entry.Rules.Select(ToBucketCorsRule).ToArray()
        };
    }

    private static BucketCorsRule ToBucketCorsRule(S3CorsRuleEntry entry)
    {
        return new BucketCorsRule
        {
            Id = entry.Id,
            AllowedOrigins = entry.AllowedOrigins,
            AllowedMethods = entry.AllowedMethods,
            AllowedHeaders = entry.AllowedHeaders,
            ExposeHeaders = entry.ExposeHeaders,
            MaxAgeSeconds = entry.MaxAgeSeconds
        };
    }

    private static S3CorsRuleEntry ToS3CorsRule(BucketCorsRule rule)
    {
        return new S3CorsRuleEntry(
            rule.Id,
            rule.AllowedOrigins.Where(static origin => !string.IsNullOrWhiteSpace(origin)).Select(static origin => origin.Trim()).ToArray(),
            rule.AllowedMethods.ToArray(),
            rule.AllowedHeaders.Where(static header => !string.IsNullOrWhiteSpace(header)).Select(static header => header.Trim()).ToArray(),
            rule.ExposeHeaders.Where(static header => !string.IsNullOrWhiteSpace(header)).Select(static header => header.Trim()).ToArray(),
            rule.MaxAgeSeconds);
    }

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
