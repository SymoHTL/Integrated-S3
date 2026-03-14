using System.Security.Cryptography;
using System.Runtime.CompilerServices;
using System.Text.Json;
using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Provider.Disk.Internal;

namespace IntegratedS3.Provider.Disk;

internal sealed class DiskStorageService(
    DiskStorageOptions options,
    IStorageObjectStateStore? objectStateStore = null,
    IStorageMultipartStateStore? multipartStateStore = null) : IStorageBackend
{
    private const string MetadataSuffix = ".integrateds3.json";
    private const string BucketMetadataFileName = ".integrateds3.bucket.json";
    private const string VersionStoreDirectoryName = ".integrateds3-versions";
    private const string MultipartUploadsDirectoryName = ".integrateds3-multipart";
    private const string MultipartStateFileName = "upload.json";
    private const string Sha256ChecksumAlgorithm = "sha256";
    private const string Sha1ChecksumAlgorithm = "sha1";
    private const string Crc32ChecksumAlgorithm = "crc32";
    private const string Crc32cChecksumAlgorithm = "crc32c";

    private readonly string _rootPath = InitializeRootPath(options);
    private readonly IStorageObjectStateStore? _objectStateStore = objectStateStore;
    private readonly IStorageMultipartStateStore? _multipartStateStore = multipartStateStore;

    public string Name => options.ProviderName;

    public string Kind => "disk";

    public bool IsPrimary => options.IsPrimary;

    public string? Description => $"Disk-backed provider rooted at '{_rootPath}'.";

    public ValueTask<StorageCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(DiskStorageCapabilities.CreateDefault());
    }

    public ValueTask<StorageSupportStateDescriptor> GetSupportStateDescriptorAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var objectStateOwnership = _objectStateStore?.Ownership ?? StorageSupportStateOwnership.BackendOwned;
        var multipartStateOwnership = _multipartStateStore?.Ownership ?? StorageSupportStateOwnership.BackendOwned;
        return ValueTask.FromResult(new StorageSupportStateDescriptor
        {
            ObjectMetadata = objectStateOwnership,
            ObjectTags = objectStateOwnership,
            MultipartState = multipartStateOwnership,
            Versioning = objectStateOwnership,
            Checksums = objectStateOwnership,
            AccessControl = StorageSupportStateOwnership.NotApplicable,
            Retention = StorageSupportStateOwnership.NotApplicable,
            ServerSideEncryption = StorageSupportStateOwnership.NotApplicable,
            RedirectLocations = StorageSupportStateOwnership.NotApplicable
        });
    }

    public ValueTask<StorageProviderMode> GetProviderModeAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var objectStateOwnership = _objectStateStore?.Ownership ?? StorageSupportStateOwnership.BackendOwned;
        var multipartStateOwnership = _multipartStateStore?.Ownership ?? StorageSupportStateOwnership.BackendOwned;
        var usesExternalSupportState = objectStateOwnership is not (StorageSupportStateOwnership.BackendOwned or StorageSupportStateOwnership.NotApplicable)
            || multipartStateOwnership is not (StorageSupportStateOwnership.BackendOwned or StorageSupportStateOwnership.NotApplicable);

        return ValueTask.FromResult(usesExternalSupportState ? StorageProviderMode.Hybrid : StorageProviderMode.Managed);
    }

    public ValueTask<StorageObjectLocationDescriptor> GetObjectLocationDescriptorAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        return ValueTask.FromResult(new StorageObjectLocationDescriptor
        {
            SupportedAccessModes = [StorageObjectAccessMode.ProxyStream]
        });
    }

    public async IAsyncEnumerable<BucketInfo> ListBucketsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (!Directory.Exists(_rootPath)) {
            yield break;
        }

        foreach (var directoryPath in Directory.EnumerateDirectories(_rootPath)) {
            cancellationToken.ThrowIfCancellationRequested();

            var directoryInfo = new DirectoryInfo(directoryPath);
            if (string.Equals(directoryInfo.Name, MultipartUploadsDirectoryName, StringComparison.OrdinalIgnoreCase)) {
                continue;
            }

            var bucketMetadata = await ReadBucketMetadataAsync(directoryPath, cancellationToken);

            yield return new BucketInfo
            {
                Name = directoryInfo.Name,
                CreatedAtUtc = directoryInfo.CreationTimeUtc,
                VersioningEnabled = bucketMetadata.VersioningStatus == BucketVersioningStatus.Enabled
            };

            await Task.Yield();
        }
    }

    public async ValueTask<StorageResult<BucketInfo>> CreateBucketAsync(CreateBucketRequest request, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var bucketPath = GetBucketPath(request.BucketName);
        if (Directory.Exists(bucketPath)) {
            return StorageResult<BucketInfo>.Failure(new StorageError
            {
                Code = StorageErrorCode.BucketAlreadyExists,
                Message = $"Bucket '{request.BucketName}' already exists.",
                BucketName = request.BucketName,
                ProviderName = options.ProviderName
            });
        }

        Directory.CreateDirectory(bucketPath);
        var directoryInfo = new DirectoryInfo(bucketPath);
        var versioningStatus = request.EnableVersioning
            ? BucketVersioningStatus.Enabled
            : BucketVersioningStatus.Disabled;

        if (versioningStatus != BucketVersioningStatus.Disabled) {
            await WriteBucketMetadataAsync(bucketPath, new DiskBucketMetadata
            {
                VersioningStatus = versioningStatus
            }, cancellationToken);
        }

        return StorageResult<BucketInfo>.Success(new BucketInfo
        {
            Name = request.BucketName,
            CreatedAtUtc = directoryInfo.CreationTimeUtc,
            VersioningEnabled = versioningStatus == BucketVersioningStatus.Enabled
        });
    }

    public ValueTask<StorageResult<BucketLocationInfo>> GetBucketLocationAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var bucketPath = GetBucketPath(bucketName);
        if (!Directory.Exists(bucketPath)) {
            return ValueTask.FromResult(StorageResult<BucketLocationInfo>.Failure(BucketNotFound(bucketName)));
        }

        return ValueTask.FromResult(StorageResult<BucketLocationInfo>.Success(new BucketLocationInfo
        {
            BucketName = bucketName,
            LocationConstraint = null
        }));
    }

    public async ValueTask<StorageResult<BucketVersioningInfo>> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var bucketPath = GetBucketPath(bucketName);
        if (!Directory.Exists(bucketPath)) {
            return StorageResult<BucketVersioningInfo>.Failure(BucketNotFound(bucketName));
        }

        var metadata = await ReadBucketMetadataAsync(bucketPath, cancellationToken);
        return StorageResult<BucketVersioningInfo>.Success(new BucketVersioningInfo
        {
            BucketName = bucketName,
            Status = metadata.VersioningStatus
        });
    }

    public async ValueTask<StorageResult<BucketVersioningInfo>> PutBucketVersioningAsync(PutBucketVersioningRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        var bucketPath = GetBucketPath(request.BucketName);
        if (!Directory.Exists(bucketPath)) {
            return StorageResult<BucketVersioningInfo>.Failure(BucketNotFound(request.BucketName));
        }

        var existingMetadata = await ReadBucketMetadataAsync(bucketPath, cancellationToken);
        var metadata = new DiskBucketMetadata
        {
            VersioningStatus = request.Status,
            CorsConfiguration = existingMetadata.CorsConfiguration
        };

        if (!ShouldPersistBucketMetadata(metadata)) {
            DeleteBucketMetadata(bucketPath);
        }
        else {
            await WriteBucketMetadataAsync(bucketPath, metadata, cancellationToken);
        }

        return StorageResult<BucketVersioningInfo>.Success(new BucketVersioningInfo
        {
            BucketName = request.BucketName,
            Status = request.Status
        });
    }

    public async ValueTask<StorageResult<BucketCorsConfiguration>> GetBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var bucketPath = GetBucketPath(bucketName);
        if (!Directory.Exists(bucketPath)) {
            return StorageResult<BucketCorsConfiguration>.Failure(BucketNotFound(bucketName));
        }

        var metadata = await ReadBucketMetadataAsync(bucketPath, cancellationToken);
        if (!HasBucketCorsConfiguration(metadata)) {
            return StorageResult<BucketCorsConfiguration>.Failure(CorsConfigurationNotFound(bucketName));
        }

        return StorageResult<BucketCorsConfiguration>.Success(ToBucketCorsConfiguration(bucketName, metadata.CorsConfiguration!));
    }

    public async ValueTask<StorageResult<BucketCorsConfiguration>> PutBucketCorsAsync(PutBucketCorsRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        var bucketPath = GetBucketPath(request.BucketName);
        if (!Directory.Exists(bucketPath)) {
            return StorageResult<BucketCorsConfiguration>.Failure(BucketNotFound(request.BucketName));
        }

        var existingMetadata = await ReadBucketMetadataAsync(bucketPath, cancellationToken);
        var updatedMetadata = new DiskBucketMetadata
        {
            VersioningStatus = existingMetadata.VersioningStatus,
            CorsConfiguration = new DiskBucketCorsConfiguration
            {
                Rules = request.Rules.Select(ToDiskBucketCorsRule).ToArray()
            }
        };

        if (!ShouldPersistBucketMetadata(updatedMetadata)) {
            DeleteBucketMetadata(bucketPath);
        }
        else {
            await WriteBucketMetadataAsync(bucketPath, updatedMetadata, cancellationToken);
        }

        return StorageResult<BucketCorsConfiguration>.Success(ToBucketCorsConfiguration(request.BucketName, updatedMetadata.CorsConfiguration!));
    }

    public async ValueTask<StorageResult> DeleteBucketCorsAsync(DeleteBucketCorsRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        var bucketPath = GetBucketPath(request.BucketName);
        if (!Directory.Exists(bucketPath)) {
            return StorageResult.Failure(BucketNotFound(request.BucketName));
        }

        var existingMetadata = await ReadBucketMetadataAsync(bucketPath, cancellationToken);
        var updatedMetadata = new DiskBucketMetadata
        {
            VersioningStatus = existingMetadata.VersioningStatus,
            CorsConfiguration = null
        };

        if (!ShouldPersistBucketMetadata(updatedMetadata)) {
            DeleteBucketMetadata(bucketPath);
        }
        else {
            await WriteBucketMetadataAsync(bucketPath, updatedMetadata, cancellationToken);
        }

        return StorageResult.Success();
    }

    public async ValueTask<StorageResult<BucketInfo>> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var bucketPath = GetBucketPath(bucketName);
        if (!Directory.Exists(bucketPath)) {
            return StorageResult<BucketInfo>.Failure(BucketNotFound(bucketName));
        }

        return await HeadBucketCoreAsync(bucketName, bucketPath, cancellationToken);
    }

    public ValueTask<StorageResult> DeleteBucketAsync(DeleteBucketRequest request, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var bucketPath = GetBucketPath(request.BucketName);
        if (!Directory.Exists(bucketPath)) {
            return ValueTask.FromResult(StorageResult.Failure(BucketNotFound(request.BucketName)));
        }

        if (Directory.EnumerateFileSystemEntries(bucketPath).Any(static path => !IsBucketMetadataFile(path))) {
            return ValueTask.FromResult(StorageResult.Failure(new StorageError
            {
                Code = StorageErrorCode.BucketNotEmpty,
                Message = $"Bucket '{request.BucketName}' must be empty before it can be deleted.",
                BucketName = request.BucketName,
                ProviderName = options.ProviderName,
                SuggestedHttpStatusCode = 409
            }));
        }

        DeleteBucketMetadata(bucketPath);
        Directory.Delete(bucketPath, recursive: false);
        return ValueTask.FromResult(StorageResult.Success());
    }

    public async IAsyncEnumerable<ObjectInfo> ListObjectsAsync(ListObjectsRequest request, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var bucketPath = GetBucketPath(request.BucketName);
        if (!Directory.Exists(bucketPath)) {
            yield break;
        }

        var prefix = NormalizeKey(request.Prefix);
        var continuationToken = NormalizeContinuationToken(request.ContinuationToken);
        var pageSize = request.PageSize;
        if (pageSize is <= 0) {
            throw new ArgumentException("Page size must be greater than zero.", nameof(request));
        }

        var files = Directory.EnumerateFiles(bucketPath, "*", SearchOption.AllDirectories)
            .Where(filePath => !IsSystemFile(bucketPath, filePath))
            .Select(filePath => new
            {
                FilePath = filePath,
                ObjectKey = GetObjectKey(bucketPath, filePath)
            })
            .Where(entry => string.IsNullOrEmpty(prefix) || entry.ObjectKey.StartsWith(prefix, StringComparison.Ordinal))
            .OrderBy(entry => entry.ObjectKey, StringComparer.Ordinal);

        var yielded = 0;

        foreach (var entry in files) {
            cancellationToken.ThrowIfCancellationRequested();

            if (!string.IsNullOrEmpty(continuationToken)
                && StringComparer.Ordinal.Compare(entry.ObjectKey, continuationToken) <= 0) {
                continue;
            }

            yield return await CreateObjectInfoAsync(request.BucketName, entry.FilePath, isLatest: true, cancellationToken);

            yielded++;
            if (pageSize is not null && yielded >= pageSize.Value) {
                yield break;
            }
        }

        if (request.IncludeVersions) {
            await foreach (var version in ListObjectVersionsAsync(new ListObjectVersionsRequest
            {
                BucketName = request.BucketName,
                Prefix = request.Prefix,
                PageSize = request.PageSize,
                KeyMarker = request.ContinuationToken
            }, cancellationToken).WithCancellation(cancellationToken)) {
                if (version.IsLatest && !version.IsDeleteMarker) {
                    continue;
                }

                yielded++;
                if (pageSize is not null && yielded > pageSize.Value) {
                    yield break;
                }

                yield return version;
            }
        }
    }

    public async IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(ListObjectVersionsRequest request, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var bucketPath = GetBucketPath(request.BucketName);
        if (!Directory.Exists(bucketPath)) {
            yield break;
        }

        if (request.PageSize is <= 0) {
            throw new ArgumentException("Page size must be greater than zero.", nameof(request));
        }

        var versions = await GetOrderedObjectVersionsAsync(request.BucketName, request.Prefix, cancellationToken);
        var yielded = 0;
        foreach (var version in versions) {
            cancellationToken.ThrowIfCancellationRequested();

            if (!IsVersionAfterMarker(version, request.KeyMarker, request.VersionIdMarker)) {
                continue;
            }

            yield return version;
            yielded++;
            if (request.PageSize is not null && yielded >= request.PageSize.Value) {
                yield break;
            }
        }
    }

    public async IAsyncEnumerable<MultipartUploadInfo> ListMultipartUploadsAsync(ListMultipartUploadsRequest request, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var bucketPath = GetBucketPath(request.BucketName);
        if (!Directory.Exists(bucketPath)) {
            yield break;
        }

        if (request.PageSize is <= 0) {
            throw new ArgumentException("Page size must be greater than zero.", nameof(request));
        }

        var uploads = await GetOrderedMultipartUploadsAsync(request.BucketName, request.Prefix, cancellationToken);
        var yielded = 0;
        foreach (var upload in uploads) {
            cancellationToken.ThrowIfCancellationRequested();

            if (!IsMultipartUploadAfterMarker(upload, request.KeyMarker, request.UploadIdMarker)) {
                continue;
            }

            yield return upload;
            yielded++;
            if (request.PageSize is not null && yielded >= request.PageSize.Value) {
                yield break;
            }
        }
    }

    public async ValueTask<StorageResult<GetObjectResponse>> GetObjectAsync(GetObjectRequest request, CancellationToken cancellationToken = default)
    {
        var serverSideEncryptionError = GetUnsupportedServerSideEncryptionError(
            request.ServerSideEncryption,
            request.BucketName,
            request.Key,
            "object retrieval");
        if (serverSideEncryptionError is not null) {
            return StorageResult<GetObjectResponse>.Failure(serverSideEncryptionError);
        }

        var storedObjectResult = await ResolveStoredObjectAsync(request.BucketName, request.Key, request.VersionId, cancellationToken);
        if (!storedObjectResult.IsSuccess) {
            return StorageResult<GetObjectResponse>.Failure(storedObjectResult.Error!);
        }

        var storedObject = storedObjectResult.Value!;
        if (storedObject.IsDeleteMarker) {
            return StorageResult<GetObjectResponse>.Failure(GetDeleteMarkerAccessError(request.BucketName, request.Key, request.VersionId, storedObject.Metadata));
        }

        if (string.IsNullOrWhiteSpace(storedObject.ContentPath)) {
            return StorageResult<GetObjectResponse>.Failure(ObjectNotFound(request.BucketName, request.Key, request.VersionId));
        }

        var objectInfo = await CreateObjectInfoAsync(request.BucketName, request.Key, storedObject.ContentPath, storedObject.Metadata, cancellationToken);

        var preconditionFailure = EvaluatePreconditions(request, objectInfo);
        if (preconditionFailure is not null) {
            return StorageResult<GetObjectResponse>.Failure(preconditionFailure);
        }

        if (IsNotModified(request, objectInfo)) {
            return StorageResult<GetObjectResponse>.Success(new GetObjectResponse
            {
                Object = objectInfo,
                Content = Stream.Null,
                TotalContentLength = objectInfo.ContentLength,
                IsNotModified = true
            });
        }

        var normalizedRange = NormalizeRange(request.Range, objectInfo.ContentLength, request.BucketName, request.Key, out var rangeError);
        if (rangeError is not null) {
            return StorageResult<GetObjectResponse>.Failure(rangeError);
        }

        var stream = new FileStream(storedObject.ContentPath, FileMode.Open, FileAccess.Read, FileShare.Read, 81920, FileOptions.Asynchronous | FileOptions.SequentialScan);

        Stream responseStream = stream;
        ObjectInfo responseObject = objectInfo;
        if (normalizedRange is not null) {
            var rangeLength = normalizedRange.End!.Value - normalizedRange.Start!.Value + 1;
            stream.Seek(normalizedRange.Start.Value, SeekOrigin.Begin);
            responseStream = new ReadOnlySubStream(stream, rangeLength);
            responseObject = new ObjectInfo
            {
                BucketName = objectInfo.BucketName,
                Key = objectInfo.Key,
                VersionId = objectInfo.VersionId,
                ContentLength = rangeLength,
                ContentType = objectInfo.ContentType,
                ETag = objectInfo.ETag,
                LastModifiedUtc = objectInfo.LastModifiedUtc,
                Metadata = objectInfo.Metadata,
                Tags = objectInfo.Tags,
                Checksums = objectInfo.Checksums
            };
        }

        return StorageResult<GetObjectResponse>.Success(new GetObjectResponse
        {
            Object = responseObject,
            Content = responseStream,
            TotalContentLength = objectInfo.ContentLength,
            Range = normalizedRange
        });
    }

    public async ValueTask<StorageResult<ObjectTagSet>> GetObjectTagsAsync(GetObjectTagsRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var storedObjectResult = await ResolveStoredObjectAsync(request.BucketName, request.Key, request.VersionId, cancellationToken);
        if (!storedObjectResult.IsSuccess) {
            return StorageResult<ObjectTagSet>.Failure(storedObjectResult.Error!);
        }

        var storedObject = storedObjectResult.Value!;
        if (storedObject.IsDeleteMarker) {
            return StorageResult<ObjectTagSet>.Failure(ObjectNotFound(request.BucketName, request.Key, request.VersionId));
        }

        var metadata = storedObject.Metadata;

        return StorageResult<ObjectTagSet>.Success(new ObjectTagSet
        {
            BucketName = request.BucketName,
            Key = request.Key,
            VersionId = metadata.VersionId,
            Tags = metadata.Tags is null
                ? new Dictionary<string, string>(StringComparer.Ordinal)
                : new Dictionary<string, string>(metadata.Tags, StringComparer.Ordinal)
        });
    }

    public async ValueTask<StorageResult<ObjectInfo>> CopyObjectAsync(CopyObjectRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var sourceServerSideEncryptionError = GetUnsupportedServerSideEncryptionError(
            request.SourceServerSideEncryption,
            request.SourceBucketName,
            request.SourceKey,
            "copy source requests");
        if (sourceServerSideEncryptionError is not null) {
            return StorageResult<ObjectInfo>.Failure(sourceServerSideEncryptionError);
        }

        var destinationServerSideEncryptionError = GetUnsupportedServerSideEncryptionError(
            request.DestinationServerSideEncryption,
            request.DestinationBucketName,
            request.DestinationKey,
            "copy destination requests");
        if (destinationServerSideEncryptionError is not null) {
            return StorageResult<ObjectInfo>.Failure(destinationServerSideEncryptionError);
        }

        var sourceObjectResult = await ResolveStoredObjectAsync(request.SourceBucketName, request.SourceKey, request.SourceVersionId, cancellationToken);
        if (!sourceObjectResult.IsSuccess) {
            return StorageResult<ObjectInfo>.Failure(sourceObjectResult.Error!);
        }

        var sourceObject = sourceObjectResult.Value!;
        if (sourceObject.IsDeleteMarker) {
            return StorageResult<ObjectInfo>.Failure(GetDeleteMarkerAccessError(request.SourceBucketName, request.SourceKey, request.SourceVersionId, sourceObject.Metadata));
        }

        if (string.IsNullOrWhiteSpace(sourceObject.ContentPath)) {
            return StorageResult<ObjectInfo>.Failure(ObjectNotFound(request.SourceBucketName, request.SourceKey, request.SourceVersionId));
        }

        var sourcePath = sourceObject.ContentPath;

        var destinationBucketPath = GetBucketPath(request.DestinationBucketName);
        if (!Directory.Exists(destinationBucketPath)) {
            return StorageResult<ObjectInfo>.Failure(BucketNotFound(request.DestinationBucketName));
        }

        var sourceInfo = await CreateObjectInfoAsync(request.SourceBucketName, request.SourceKey, sourcePath, sourceObject.Metadata, cancellationToken);

        var preconditionFailure = EvaluateCopyPreconditions(request, sourceInfo);
        if (preconditionFailure is not null) {
            return StorageResult<ObjectInfo>.Failure(preconditionFailure);
        }

        var destinationPath = GetObjectPath(request.DestinationBucketName, request.DestinationKey);
        var destinationDirectoryPath = Path.GetDirectoryName(destinationPath)!;
        Directory.CreateDirectory(destinationDirectoryPath);

        if (!request.OverwriteIfExists && File.Exists(destinationPath)) {
            return StorageResult<ObjectInfo>.Failure(new StorageError
            {
                Code = StorageErrorCode.PreconditionFailed,
                Message = $"Object '{request.DestinationKey}' already exists in bucket '{request.DestinationBucketName}'.",
                BucketName = request.DestinationBucketName,
                ObjectKey = request.DestinationKey,
                ProviderName = options.ProviderName,
                SuggestedHttpStatusCode = 412
            });
        }

        var tempDestinationPath = $"{destinationPath}.{Guid.NewGuid():N}.tmp";
        try {
            await using (var sourceStream = new FileStream(sourcePath, FileMode.Open, FileAccess.Read, FileShare.Read, 81920, FileOptions.Asynchronous | FileOptions.SequentialScan))
            await using (var destinationStream = new FileStream(tempDestinationPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 81920, FileOptions.Asynchronous | FileOptions.SequentialScan)) {
                await sourceStream.CopyToAsync(destinationStream, cancellationToken);
            }

            if (await HasCurrentVersionStateAsync(request.DestinationBucketName, request.DestinationKey, cancellationToken)
                && await IsVersioningEnabledAsync(request.DestinationBucketName, cancellationToken)) {
                await ArchiveCurrentObjectVersionAsync(request.DestinationBucketName, request.DestinationKey, destinationPath, cancellationToken);
            }

            File.Move(tempDestinationPath, destinationPath, overwrite: true);

            var sourceMetadata = sourceObject.Metadata;
            var checksums = sourceMetadata.Checksums ?? await ComputeChecksumsAsync(destinationPath, cancellationToken);
            var versionId = CreateVersionId();
            var useReplacementMetadata = request.MetadataDirective == CopyObjectMetadataDirective.Replace;
            await WriteStoredObjectStateAsync(
                request.DestinationBucketName,
                request.DestinationKey,
                destinationPath,
                versionId,
                useReplacementMetadata
                    ? string.IsNullOrWhiteSpace(request.ContentType) ? "application/octet-stream" : request.ContentType
                    : sourceMetadata.ContentType,
                useReplacementMetadata
                    ? request.Metadata
                    : sourceMetadata.Metadata,
                sourceMetadata.Tags,
                checksums,
                isDeleteMarker: false,
                isLatest: true,
                lastModifiedUtc: null,
                cancellationToken: cancellationToken,
                cacheControl: useReplacementMetadata ? request.CacheControl : sourceMetadata.CacheControl,
                contentDisposition: useReplacementMetadata ? request.ContentDisposition : sourceMetadata.ContentDisposition,
                contentEncoding: useReplacementMetadata ? request.ContentEncoding : sourceMetadata.ContentEncoding,
                contentLanguage: useReplacementMetadata ? request.ContentLanguage : sourceMetadata.ContentLanguage,
                expiresUtc: useReplacementMetadata ? request.ExpiresUtc : sourceMetadata.ExpiresUtc);
        }
        finally {
            if (File.Exists(tempDestinationPath)) {
                File.Delete(tempDestinationPath);
            }
        }

        return StorageResult<ObjectInfo>.Success(await CreateObjectInfoAsync(request.DestinationBucketName, destinationPath, cancellationToken));
    }

    public async ValueTask<StorageResult<ObjectInfo>> PutObjectAsync(PutObjectRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Content);

        var serverSideEncryptionError = GetUnsupportedServerSideEncryptionError(
            request.ServerSideEncryption,
            request.BucketName,
            request.Key,
            "object writes");
        if (serverSideEncryptionError is not null) {
            return StorageResult<ObjectInfo>.Failure(serverSideEncryptionError);
        }

        var bucketPath = GetBucketPath(request.BucketName);
        if (!Directory.Exists(bucketPath)) {
            return StorageResult<ObjectInfo>.Failure(BucketNotFound(request.BucketName));
        }

        var objectPath = GetObjectPath(request.BucketName, request.Key);
        var objectDirectoryPath = Path.GetDirectoryName(objectPath)!;
        Directory.CreateDirectory(objectDirectoryPath);

        if (!request.OverwriteIfExists && File.Exists(objectPath)) {
            return StorageResult<ObjectInfo>.Failure(new StorageError
            {
                Code = StorageErrorCode.PreconditionFailed,
                Message = $"Object '{request.Key}' already exists in bucket '{request.BucketName}'.",
                BucketName = request.BucketName,
                ObjectKey = request.Key,
                ProviderName = options.ProviderName,
                SuggestedHttpStatusCode = 412
            });
        }

        var tempFilePath = $"{objectPath}.{Guid.NewGuid():N}.tmp";
        try {
            await using (var tempStream = new FileStream(tempFilePath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 81920, FileOptions.Asynchronous | FileOptions.SequentialScan)) {
                await request.Content.CopyToAsync(tempStream, cancellationToken);
            }

            var actualChecksums = await ComputeChecksumsAsync(tempFilePath, cancellationToken);
            var checksumValidationError = ValidateRequestedChecksums(request.Checksums, actualChecksums, request.BucketName, request.Key);
            if (checksumValidationError is not null) {
                return StorageResult<ObjectInfo>.Failure(checksumValidationError);
            }

            if (await HasCurrentVersionStateAsync(request.BucketName, request.Key, cancellationToken)
                && await IsVersioningEnabledAsync(request.BucketName, cancellationToken)) {
                await ArchiveCurrentObjectVersionAsync(request.BucketName, request.Key, objectPath, cancellationToken);
            }

            File.Move(tempFilePath, objectPath, overwrite: true);
            var versionId = CreateVersionId();
            await WriteStoredObjectStateAsync(
                request.BucketName,
                request.Key,
                objectPath,
                versionId,
                string.IsNullOrWhiteSpace(request.ContentType) ? "application/octet-stream" : request.ContentType,
                request.Metadata,
                tags: null,
                actualChecksums,
                isDeleteMarker: false,
                isLatest: true,
                lastModifiedUtc: null,
                cancellationToken: cancellationToken,
                cacheControl: request.CacheControl,
                contentDisposition: request.ContentDisposition,
                contentEncoding: request.ContentEncoding,
                contentLanguage: request.ContentLanguage,
                expiresUtc: request.ExpiresUtc);
        }
        finally {
            if (File.Exists(tempFilePath)) {
                File.Delete(tempFilePath);
            }
        }

        return StorageResult<ObjectInfo>.Success(await CreateObjectInfoAsync(request.BucketName, objectPath, cancellationToken));
    }

    public async ValueTask<StorageResult<ObjectTagSet>> PutObjectTagsAsync(PutObjectTagsRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        return await UpdateObjectTagsAsync(request.BucketName, request.Key, request.VersionId, request.Tags, cancellationToken);
    }

    public async ValueTask<StorageResult<ObjectTagSet>> DeleteObjectTagsAsync(DeleteObjectTagsRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        return await UpdateObjectTagsAsync(request.BucketName, request.Key, request.VersionId, tags: null, cancellationToken);
    }

    private async ValueTask<StorageResult<ObjectTagSet>> UpdateObjectTagsAsync(
        string bucketName,
        string key,
        string? versionId,
        IReadOnlyDictionary<string, string>? tags,
        CancellationToken cancellationToken)
    {
        var tagValidationError = ObjectTagValidation.Validate(tags);
        if (tagValidationError is not null) {
            return StorageResult<ObjectTagSet>.Failure(InvalidTag(tagValidationError, bucketName, key));
        }

        var storedObjectResult = await ResolveStoredObjectAsync(bucketName, key, versionId, cancellationToken);
        if (!storedObjectResult.IsSuccess) {
            return StorageResult<ObjectTagSet>.Failure(storedObjectResult.Error!);
        }

        var storedObject = storedObjectResult.Value!;
        if (storedObject.IsDeleteMarker) {
            return StorageResult<ObjectTagSet>.Failure(ObjectNotFound(bucketName, key, versionId));
        }

        var metadata = storedObject.Metadata;

        var normalizedTags = tags is null || tags.Count == 0
            ? null
            : new Dictionary<string, string>(tags, StringComparer.Ordinal);

        if (storedObject.IsCurrent) {
            await WriteStoredObjectStateAsync(
                bucketName,
                key,
                storedObject.ContentPath!,
                metadata.VersionId,
                metadata.ContentType,
                metadata.Metadata,
                normalizedTags,
                metadata.Checksums,
                isDeleteMarker: false,
                isLatest: true,
                lastModifiedUtc: metadata.LastModifiedUtc,
                cancellationToken: cancellationToken,
                cacheControl: metadata.CacheControl,
                contentDisposition: metadata.ContentDisposition,
                contentEncoding: metadata.ContentEncoding,
                contentLanguage: metadata.ContentLanguage,
                expiresUtc: metadata.ExpiresUtc);
        }
        else {
            await WriteStoredObjectStateAsync(
                bucketName,
                key,
                GetArchivedVersionContentPath(bucketName, key, metadata.VersionId!),
                metadata.VersionId,
                metadata.ContentType,
                metadata.Metadata,
                normalizedTags,
                metadata.Checksums,
                isDeleteMarker: false,
                isLatest: false,
                lastModifiedUtc: metadata.LastModifiedUtc,
                cancellationToken: cancellationToken,
                cacheControl: metadata.CacheControl,
                contentDisposition: metadata.ContentDisposition,
                contentEncoding: metadata.ContentEncoding,
                contentLanguage: metadata.ContentLanguage,
                expiresUtc: metadata.ExpiresUtc);
        }

        return StorageResult<ObjectTagSet>.Success(new ObjectTagSet
        {
            BucketName = bucketName,
            Key = key,
            VersionId = metadata.VersionId,
            Tags = normalizedTags ?? new Dictionary<string, string>(StringComparer.Ordinal)
        });
    }

    public ValueTask<StorageResult<MultipartUploadInfo>> InitiateMultipartUploadAsync(InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        var serverSideEncryptionError = GetUnsupportedServerSideEncryptionError(
            request.ServerSideEncryption,
            request.BucketName,
            request.Key,
            "multipart upload initiation");
        if (serverSideEncryptionError is not null) {
            return ValueTask.FromResult(StorageResult<MultipartUploadInfo>.Failure(serverSideEncryptionError));
        }

        if (!Directory.Exists(GetBucketPath(request.BucketName))) {
            return ValueTask.FromResult(StorageResult<MultipartUploadInfo>.Failure(BucketNotFound(request.BucketName)));
        }

        if (!TryNormalizeChecksumAlgorithm(request.ChecksumAlgorithm, out var checksumAlgorithm)) {
            return ValueTask.FromResult(StorageResult<MultipartUploadInfo>.Failure(StorageError.Unsupported(
                $"Checksum algorithm '{request.ChecksumAlgorithm}' is not currently supported for multipart uploads.",
                request.BucketName,
                request.Key)));
        }

        if (!string.IsNullOrWhiteSpace(checksumAlgorithm)
            && !string.Equals(checksumAlgorithm, Sha256ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
            && !string.Equals(checksumAlgorithm, Sha1ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
            && !string.Equals(checksumAlgorithm, Crc32cChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)) {
            return ValueTask.FromResult(StorageResult<MultipartUploadInfo>.Failure(StorageError.Unsupported(
                $"Checksum algorithm '{request.ChecksumAlgorithm}' is not currently supported for multipart uploads.",
                request.BucketName,
                request.Key)));
        }

        _ = GetObjectPath(request.BucketName, request.Key);

        var uploadId = Guid.CreateVersion7().ToString("N");
        var uploadDirectoryPath = GetMultipartUploadPath(request.BucketName, uploadId);
        Directory.CreateDirectory(uploadDirectoryPath);

        var uploadInfo = new MultipartUploadInfo
        {
            BucketName = request.BucketName,
            Key = request.Key,
            UploadId = uploadId,
            InitiatedAtUtc = DateTimeOffset.UtcNow,
            ChecksumAlgorithm = checksumAlgorithm
        };

        return WriteMultipartStateAndReturnAsync(uploadDirectoryPath, uploadInfo, request, cancellationToken);
    }

    public async ValueTask<StorageResult<MultipartUploadPart>> UploadMultipartPartAsync(UploadMultipartPartRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Content);

        if (request.PartNumber <= 0) {
            return StorageResult<MultipartUploadPart>.Failure(MultipartConflict(
                "Multipart part numbers must be greater than zero.",
                request.BucketName,
                request.Key));
        }

        if (!Directory.Exists(GetBucketPath(request.BucketName))) {
            return StorageResult<MultipartUploadPart>.Failure(BucketNotFound(request.BucketName));
        }

        var uploadStateResult = await ReadMultipartStateAsync(request.BucketName, request.Key, request.UploadId, cancellationToken);
        if (!uploadStateResult.IsSuccess) {
            return StorageResult<MultipartUploadPart>.Failure(uploadStateResult.Error!);
        }

        var uploadDirectoryPath = uploadStateResult.Value!.UploadDirectoryPath;
        var uploadChecksumAlgorithm = uploadStateResult.Value.State.ChecksumAlgorithm;
        if (!string.IsNullOrWhiteSpace(uploadChecksumAlgorithm)
            && !string.Equals(uploadChecksumAlgorithm, Sha256ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
            && !string.Equals(uploadChecksumAlgorithm, Sha1ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
            && !string.Equals(uploadChecksumAlgorithm, Crc32cChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)) {
            return StorageResult<MultipartUploadPart>.Failure(StorageError.Unsupported(
                $"Checksum algorithm '{uploadChecksumAlgorithm}' is not currently supported for multipart uploads.",
                request.BucketName,
                request.Key));
        }

        if (!TryNormalizeChecksumAlgorithm(request.ChecksumAlgorithm, out var requestChecksumAlgorithm)) {
            return StorageResult<MultipartUploadPart>.Failure(StorageError.Unsupported(
                $"Checksum algorithm '{request.ChecksumAlgorithm}' is not currently supported for multipart uploads.",
                request.BucketName,
                request.Key));
        }

        if (!string.IsNullOrWhiteSpace(uploadChecksumAlgorithm)
            && requestChecksumAlgorithm is not null
            && !string.Equals(uploadChecksumAlgorithm, requestChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)) {
            return StorageResult<MultipartUploadPart>.Failure(MultipartInvalidRequest(
                $"Multipart upload '{request.UploadId}' requires checksum algorithm '{uploadChecksumAlgorithm.ToUpperInvariant()}'.",
                request.BucketName,
                request.Key));
        }

        if (!string.IsNullOrWhiteSpace(uploadChecksumAlgorithm)
            && !TryGetChecksumValue(request.Checksums, uploadChecksumAlgorithm, out _)) {
            return StorageResult<MultipartUploadPart>.Failure(MultipartInvalidRequest(
                $"The supplied part is missing the '{uploadChecksumAlgorithm.ToUpperInvariant()}' checksum required by multipart upload '{request.UploadId}'.",
                request.BucketName,
                request.Key));
        }

        Directory.CreateDirectory(GetMultipartPartsDirectoryPath(uploadDirectoryPath));

        var partPath = GetMultipartPartPath(uploadDirectoryPath, request.PartNumber);
        var tempPartPath = $"{partPath}.{Guid.NewGuid():N}.tmp";
        try {
            await using (var tempStream = new FileStream(tempPartPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 81920, FileOptions.Asynchronous | FileOptions.SequentialScan)) {
                await request.Content.CopyToAsync(tempStream, cancellationToken);
            }

            File.Move(tempPartPath, partPath, overwrite: true);
        }
        finally {
            if (File.Exists(tempPartPath)) {
                File.Delete(tempPartPath);
            }
        }

        var partInfo = new FileInfo(partPath);
        var actualChecksums = await ComputeChecksumsAsync(partPath, cancellationToken);
        var checksumValidationError = ValidateRequestedChecksums(request.Checksums, actualChecksums, request.BucketName, request.Key);
        if (checksumValidationError is not null) {
            return StorageResult<MultipartUploadPart>.Failure(checksumValidationError);
        }

        return StorageResult<MultipartUploadPart>.Success(new MultipartUploadPart
        {
            PartNumber = request.PartNumber,
            ETag = BuildETag(partInfo),
            ContentLength = partInfo.Length,
            LastModifiedUtc = partInfo.LastWriteTimeUtc,
            Checksums = CreateMultipartPartResponseChecksums(actualChecksums, uploadChecksumAlgorithm, request.Checksums)
        });
    }

    public async ValueTask<StorageResult<ObjectInfo>> CompleteMultipartUploadAsync(CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (request.Parts.Count == 0) {
            return StorageResult<ObjectInfo>.Failure(MultipartConflict(
                "At least one multipart part is required to complete an upload.",
                request.BucketName,
                request.Key));
        }

        if (!Directory.Exists(GetBucketPath(request.BucketName))) {
            return StorageResult<ObjectInfo>.Failure(BucketNotFound(request.BucketName));
        }

        var uploadStateResult = await ReadMultipartStateAsync(request.BucketName, request.Key, request.UploadId, cancellationToken);
        if (!uploadStateResult.IsSuccess) {
            return StorageResult<ObjectInfo>.Failure(uploadStateResult.Error!);
        }

        var uploadState = uploadStateResult.Value!;
        var uploadChecksumAlgorithm = uploadState.State.ChecksumAlgorithm;
        if (!string.IsNullOrWhiteSpace(uploadChecksumAlgorithm)
            && !string.Equals(uploadChecksumAlgorithm, Sha256ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
            && !string.Equals(uploadChecksumAlgorithm, Sha1ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
            && !string.Equals(uploadChecksumAlgorithm, Crc32cChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)) {
            return StorageResult<ObjectInfo>.Failure(StorageError.Unsupported(
                $"Checksum algorithm '{uploadChecksumAlgorithm}' is not currently supported for multipart uploads.",
                request.BucketName,
                request.Key));
        }

        var objectPath = GetObjectPath(request.BucketName, request.Key);
        var objectDirectoryPath = Path.GetDirectoryName(objectPath)!;
        Directory.CreateDirectory(objectDirectoryPath);

        var tempObjectPath = $"{objectPath}.{Guid.NewGuid():N}.tmp";
        List<string>? compositePartChecksums = (string.Equals(uploadChecksumAlgorithm, Sha256ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
                || string.Equals(uploadChecksumAlgorithm, Sha1ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
                || string.Equals(uploadChecksumAlgorithm, Crc32cChecksumAlgorithm, StringComparison.OrdinalIgnoreCase))
            ? new List<string>(request.Parts.Count)
            : null;
        try {
            await using (var destinationStream = new FileStream(tempObjectPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 81920, FileOptions.Asynchronous | FileOptions.SequentialScan)) {
                foreach (var requestedPart in request.Parts.OrderBy(static part => part.PartNumber)) {
                    if (requestedPart.PartNumber <= 0) {
                        return StorageResult<ObjectInfo>.Failure(MultipartConflict(
                            "Multipart part numbers must be greater than zero.",
                            request.BucketName,
                            request.Key));
                    }

                    var partPath = GetMultipartPartPath(uploadState.UploadDirectoryPath, requestedPart.PartNumber);
                    if (!File.Exists(partPath)) {
                        return StorageResult<ObjectInfo>.Failure(MultipartConflict(
                            $"Multipart part '{requestedPart.PartNumber}' was not found for upload '{request.UploadId}'.",
                            request.BucketName,
                            request.Key));
                    }

                    var actualETag = BuildETag(new FileInfo(partPath));
                    if (!string.Equals(NormalizeETag(requestedPart.ETag), NormalizeETag(actualETag), StringComparison.Ordinal)) {
                        return StorageResult<ObjectInfo>.Failure(MultipartConflict(
                            $"Multipart part '{requestedPart.PartNumber}' does not match the supplied ETag.",
                            request.BucketName,
                            request.Key));
                    }

                    IReadOnlyDictionary<string, string>? actualPartChecksums = null;
                    if (compositePartChecksums is not null
                        || (requestedPart.Checksums is not null && requestedPart.Checksums.Count > 0)) {
                        actualPartChecksums = await ComputeChecksumsAsync(partPath, cancellationToken);
                    }

                    var partChecksumValidationError = ValidateRequestedChecksums(requestedPart.Checksums, actualPartChecksums, request.BucketName, request.Key);
                    if (partChecksumValidationError is not null) {
                        return StorageResult<ObjectInfo>.Failure(partChecksumValidationError);
                    }

                    if (compositePartChecksums is not null) {
                        var compositeChecksumAlgorithm = uploadChecksumAlgorithm!;
                        if (!TryGetChecksumValue(actualPartChecksums, compositeChecksumAlgorithm, out var actualPartChecksum)) {
                            return StorageResult<ObjectInfo>.Failure(StorageError.Unsupported(
                                $"Multipart {compositeChecksumAlgorithm.ToUpperInvariant()} checksum synthesis requires per-part {compositeChecksumAlgorithm.ToUpperInvariant()} digests.",
                                request.BucketName,
                                request.Key));
                        }

                        compositePartChecksums.Add(actualPartChecksum);
                    }

                    await using var sourceStream = new FileStream(partPath, FileMode.Open, FileAccess.Read, FileShare.Read, 81920, FileOptions.Asynchronous | FileOptions.SequentialScan);
                    await sourceStream.CopyToAsync(destinationStream, cancellationToken);
                }
            }

            if (await HasCurrentVersionStateAsync(request.BucketName, request.Key, cancellationToken)
                && await IsVersioningEnabledAsync(request.BucketName, cancellationToken)) {
                await ArchiveCurrentObjectVersionAsync(request.BucketName, request.Key, objectPath, cancellationToken);
            }

            File.Move(tempObjectPath, objectPath, overwrite: true);
            IReadOnlyDictionary<string, string> checksums = compositePartChecksums is not null
                ? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    [uploadChecksumAlgorithm!] = BuildCompositeChecksum(uploadChecksumAlgorithm!, compositePartChecksums)
                }
                : await ComputeChecksumsAsync(objectPath, cancellationToken);
            var versionId = CreateVersionId();
            await WriteStoredObjectStateAsync(
                request.BucketName,
                request.Key,
                objectPath,
                versionId,
                string.IsNullOrWhiteSpace(uploadState.State.ContentType) ? "application/octet-stream" : uploadState.State.ContentType,
                uploadState.State.Metadata,
                tags: null,
                checksums,
                isDeleteMarker: false,
                isLatest: true,
                lastModifiedUtc: null,
                cancellationToken: cancellationToken,
                cacheControl: uploadState.State.CacheControl,
                contentDisposition: uploadState.State.ContentDisposition,
                contentEncoding: uploadState.State.ContentEncoding,
                contentLanguage: uploadState.State.ContentLanguage,
                expiresUtc: uploadState.State.ExpiresUtc);

            await DeleteStoredMultipartStateAsync(request.BucketName, request.Key, request.UploadId, uploadState.UploadDirectoryPath, cancellationToken);
            Directory.Delete(uploadState.UploadDirectoryPath, recursive: true);
        }
        finally {
            if (File.Exists(tempObjectPath)) {
                File.Delete(tempObjectPath);
            }
        }

        return StorageResult<ObjectInfo>.Success(await CreateObjectInfoAsync(request.BucketName, objectPath, cancellationToken));
    }

    public async ValueTask<StorageResult> AbortMultipartUploadAsync(AbortMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        if (!Directory.Exists(GetBucketPath(request.BucketName))) {
            return StorageResult.Failure(BucketNotFound(request.BucketName));
        }

        var uploadStateResult = await ReadMultipartStateAsync(request.BucketName, request.Key, request.UploadId, cancellationToken);
        if (!uploadStateResult.IsSuccess) {
            return StorageResult.Failure(uploadStateResult.Error!);
        }

        Directory.Delete(uploadStateResult.Value!.UploadDirectoryPath, recursive: true);
        await DeleteStoredMultipartStateAsync(request.BucketName, request.Key, request.UploadId, uploadStateResult.Value.UploadDirectoryPath, cancellationToken);
        DeleteEmptyParentDirectories(Path.GetDirectoryName(uploadStateResult.Value.UploadDirectoryPath), GetMultipartRootPath());
        return StorageResult.Success();
    }

    public async ValueTask<StorageResult<ObjectInfo>> HeadObjectAsync(HeadObjectRequest request, CancellationToken cancellationToken = default)
    {
        var serverSideEncryptionError = GetUnsupportedServerSideEncryptionError(
            request.ServerSideEncryption,
            request.BucketName,
            request.Key,
            "object metadata lookups");
        if (serverSideEncryptionError is not null) {
            return StorageResult<ObjectInfo>.Failure(serverSideEncryptionError);
        }

        var storedObjectResult = await ResolveStoredObjectAsync(request.BucketName, request.Key, request.VersionId, cancellationToken);
        if (!storedObjectResult.IsSuccess) {
            return StorageResult<ObjectInfo>.Failure(storedObjectResult.Error!);
        }

        var storedObject = storedObjectResult.Value!;
        if (storedObject.IsDeleteMarker) {
            return StorageResult<ObjectInfo>.Failure(GetDeleteMarkerAccessError(request.BucketName, request.Key, request.VersionId, storedObject.Metadata));
        }

        if (string.IsNullOrWhiteSpace(storedObject.ContentPath)) {
            return StorageResult<ObjectInfo>.Failure(ObjectNotFound(request.BucketName, request.Key, request.VersionId));
        }

        var objectInfo = await CreateObjectInfoAsync(request.BucketName, request.Key, storedObject.ContentPath, storedObject.Metadata, cancellationToken);

        return StorageResult<ObjectInfo>.Success(objectInfo);
    }

    public async ValueTask<StorageResult<DeleteObjectResult>> DeleteObjectAsync(DeleteObjectRequest request, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var bucketPath = GetBucketPath(request.BucketName);
        if (!Directory.Exists(bucketPath)) {
            return StorageResult<DeleteObjectResult>.Failure(BucketNotFound(request.BucketName));
        }

        var filePath = GetObjectPath(request.BucketName, request.Key);
        var versioningEnabled = await IsVersioningEnabledAsync(request.BucketName, cancellationToken);

        if (!string.IsNullOrWhiteSpace(request.VersionId)) {
            var storedObjectResult = await ResolveStoredObjectAsync(request.BucketName, request.Key, request.VersionId, cancellationToken);
            if (!storedObjectResult.IsSuccess) {
                return StorageResult<DeleteObjectResult>.Failure(storedObjectResult.Error!);
            }

            var storedObject = storedObjectResult.Value!;
            if (!storedObject.IsDeleteMarker && !string.IsNullOrWhiteSpace(storedObject.ContentPath) && File.Exists(storedObject.ContentPath)) {
                File.Delete(storedObject.ContentPath);
            }

            if (storedObject.IsCurrent) {
                await DeleteStoredObjectStateAsync(request.BucketName, request.Key, filePath, request.VersionId, cancellationToken);
                if (!string.IsNullOrWhiteSpace(storedObject.ContentPath)) {
                    DeleteEmptyParentDirectories(Path.GetDirectoryName(storedObject.ContentPath), GetBucketPath(request.BucketName));
                }

                var promotedCurrent = await PromoteLatestArchivedVersionAsync(request.BucketName, request.Key, cancellationToken);
                return StorageResult<DeleteObjectResult>.Success(new DeleteObjectResult
                {
                    BucketName = request.BucketName,
                    Key = request.Key,
                    VersionId = request.VersionId,
                    IsDeleteMarker = storedObject.IsDeleteMarker,
                    CurrentObject = promotedCurrent
                });
            }

            await DeleteArchivedVersionStateAsync(request.BucketName, request.Key, request.VersionId, storedObject.ContentPath, cancellationToken);
            if (!string.IsNullOrWhiteSpace(storedObject.ContentPath)) {
                DeleteEmptyParentDirectories(Path.GetDirectoryName(storedObject.ContentPath), GetVersionsRootPath(request.BucketName));
            }

            return StorageResult<DeleteObjectResult>.Success(new DeleteObjectResult
            {
                BucketName = request.BucketName,
                Key = request.Key,
                VersionId = request.VersionId,
                IsDeleteMarker = storedObject.IsDeleteMarker
            });
        }

        if (!await HasCurrentVersionStateAsync(request.BucketName, request.Key, cancellationToken)) {
            if (versioningEnabled && !request.BypassDeleteMarkerCreation) {
                var deleteMarker = await CreateCurrentDeleteMarkerAsync(request.BucketName, request.Key, cancellationToken);
                return StorageResult<DeleteObjectResult>.Success(new DeleteObjectResult
                {
                    BucketName = request.BucketName,
                    Key = request.Key,
                    VersionId = deleteMarker.VersionId,
                    IsDeleteMarker = true,
                    CurrentObject = deleteMarker
                });
            }

            return StorageResult<DeleteObjectResult>.Success(new DeleteObjectResult
            {
                BucketName = request.BucketName,
                Key = request.Key
            });
        }

        if (versioningEnabled && !request.BypassDeleteMarkerCreation) {
            await ArchiveCurrentObjectVersionAsync(request.BucketName, request.Key, filePath, cancellationToken);

            if (File.Exists(filePath)) {
                File.Delete(filePath);
            }

            var deleteMarker = await CreateCurrentDeleteMarkerAsync(request.BucketName, request.Key, cancellationToken);
            return StorageResult<DeleteObjectResult>.Success(new DeleteObjectResult
            {
                BucketName = request.BucketName,
                Key = request.Key,
                VersionId = deleteMarker.VersionId,
                IsDeleteMarker = true,
                CurrentObject = deleteMarker
            });
        }

        if (File.Exists(filePath)) {
            File.Delete(filePath);
        }

        await DeleteStoredObjectStateAsync(request.BucketName, request.Key, filePath, versionId: null, cancellationToken);

        DeleteEmptyParentDirectories(Path.GetDirectoryName(filePath), GetBucketPath(request.BucketName));
        return StorageResult<DeleteObjectResult>.Success(new DeleteObjectResult
        {
            BucketName = request.BucketName,
            Key = request.Key
        });
    }

    private static string InitializeRootPath(DiskStorageOptions options)
    {
        var rootPath = Path.GetFullPath(options.RootPath);
        if (options.CreateRootDirectory) {
            Directory.CreateDirectory(rootPath);
            Directory.CreateDirectory(Path.Combine(rootPath, MultipartUploadsDirectoryName));
        }

        return rootPath;
    }

    private static bool IsMetadataFile(string filePath)
    {
        return filePath.EndsWith(MetadataSuffix, StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsSystemFile(string bucketPath, string filePath)
    {
        if (IsMetadataFile(filePath) || IsBucketMetadataFile(filePath)) {
            return true;
        }

        var relativePath = Path.GetRelativePath(bucketPath, filePath).Replace('\\', '/');
        return relativePath.StartsWith($"{VersionStoreDirectoryName}/", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsBucketMetadataFile(string filePath)
    {
        return string.Equals(Path.GetFileName(filePath), BucketMetadataFileName, StringComparison.OrdinalIgnoreCase);
    }

    private static string NormalizeKey(string? key)
    {
        return string.IsNullOrWhiteSpace(key)
            ? string.Empty
            : key.Replace('\\', '/').TrimStart('/');
    }

    private static string? NormalizeContinuationToken(string? continuationToken)
    {
        return string.IsNullOrWhiteSpace(continuationToken)
            ? null
            : NormalizeKey(continuationToken);
    }

    private string GetBucketPath(string bucketName)
    {
        if (string.IsNullOrWhiteSpace(bucketName)) {
            throw new ArgumentException("Bucket name is required.", nameof(bucketName));
        }

        if (bucketName.Contains(Path.DirectorySeparatorChar) || bucketName.Contains(Path.AltDirectorySeparatorChar) || bucketName.Contains("..", StringComparison.Ordinal)) {
            throw new ArgumentException("Bucket name contains invalid path characters.", nameof(bucketName));
        }

        return Path.Combine(_rootPath, bucketName);
    }

    private string GetObjectPath(string bucketName, string key)
    {
        if (string.IsNullOrWhiteSpace(key)) {
            throw new ArgumentException("Object key is required.", nameof(key));
        }

        var bucketPath = GetBucketPath(bucketName);
        var normalizedKey = NormalizeKey(key);
        var segments = normalizedKey.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        if (segments.Length == 0 || segments.Any(static segment => segment == "." || segment == "..")) {
            throw new ArgumentException("Object key contains invalid path segments.", nameof(key));
        }

        var pathParts = new string[segments.Length + 1];
        pathParts[0] = bucketPath;
        Array.Copy(segments, 0, pathParts, 1, segments.Length);

        var fullPath = Path.GetFullPath(Path.Combine(pathParts));
        var bucketRoot = Path.GetFullPath(bucketPath);
        if (!fullPath.StartsWith(bucketRoot, StringComparison.OrdinalIgnoreCase)) {
            throw new ArgumentException("Object key resolves outside the bucket root.", nameof(key));
        }

        return fullPath;
    }

    private static string GetObjectKey(string bucketPath, string filePath)
    {
        return Path.GetRelativePath(bucketPath, filePath).Replace('\\', '/');
    }

    private static string GetMetadataPath(string objectPath)
    {
        return objectPath + MetadataSuffix;
    }

    private static string GetBucketMetadataPath(string bucketPath)
    {
        return Path.Combine(bucketPath, BucketMetadataFileName);
    }

    private string GetVersionsRootPath(string bucketName)
    {
        return Path.Combine(GetBucketPath(bucketName), VersionStoreDirectoryName);
    }

    private string GetObjectVersionsDirectoryPath(string bucketName, string key)
    {
        var versionsRootPath = GetVersionsRootPath(bucketName);
        var normalizedKey = NormalizeKey(key);
        var segments = normalizedKey.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        if (segments.Length == 0 || segments.Any(static segment => segment == "." || segment == "..")) {
            throw new ArgumentException("Object key contains invalid path segments.", nameof(key));
        }

        var pathParts = new string[segments.Length + 1];
        pathParts[0] = versionsRootPath;
        Array.Copy(segments, 0, pathParts, 1, segments.Length);

        var fullPath = Path.GetFullPath(Path.Combine(pathParts));
        var versionsRoot = Path.GetFullPath(versionsRootPath);
        if (!fullPath.StartsWith(versionsRoot, StringComparison.OrdinalIgnoreCase)) {
            throw new ArgumentException("Object key resolves outside the version store root.", nameof(key));
        }

        return fullPath;
    }

    private string GetArchivedVersionContentPath(string bucketName, string key, string versionId)
    {
        if (string.IsNullOrWhiteSpace(versionId)) {
            throw new ArgumentException("Version ID is required.", nameof(versionId));
        }

        if (versionId.Contains(Path.DirectorySeparatorChar)
            || versionId.Contains(Path.AltDirectorySeparatorChar)
            || versionId.Contains("..", StringComparison.Ordinal)) {
            throw new ArgumentException("Version ID contains invalid path characters.", nameof(versionId));
        }

        return Path.Combine(GetObjectVersionsDirectoryPath(bucketName, key), versionId, "content");
    }

    private string GetMultipartRootPath()
    {
        return Path.Combine(_rootPath, MultipartUploadsDirectoryName);
    }

    private string GetMultipartBucketPath(string bucketName)
    {
        _ = GetBucketPath(bucketName);
        return Path.Combine(GetMultipartRootPath(), bucketName);
    }

    private string GetMultipartUploadPath(string bucketName, string uploadId)
    {
        if (string.IsNullOrWhiteSpace(uploadId)) {
            throw new ArgumentException("Upload ID is required.", nameof(uploadId));
        }

        if (uploadId.Contains(Path.DirectorySeparatorChar)
            || uploadId.Contains(Path.AltDirectorySeparatorChar)
            || uploadId.Contains("..", StringComparison.Ordinal)) {
            throw new ArgumentException("Upload ID contains invalid path characters.", nameof(uploadId));
        }

        var uploadPath = Path.Combine(GetMultipartRootPath(), bucketName, uploadId);
        var fullPath = Path.GetFullPath(uploadPath);
        var multipartRootPath = Path.GetFullPath(GetMultipartRootPath());
        if (!fullPath.StartsWith(multipartRootPath, StringComparison.OrdinalIgnoreCase)) {
            throw new ArgumentException("Upload ID resolves outside the multipart root.", nameof(uploadId));
        }

        return fullPath;
    }

    private static string GetMultipartStatePath(string uploadDirectoryPath)
    {
        return Path.Combine(uploadDirectoryPath, MultipartStateFileName);
    }

    private static string GetMultipartPartsDirectoryPath(string uploadDirectoryPath)
    {
        return Path.Combine(uploadDirectoryPath, "parts");
    }

    private static string GetMultipartPartPath(string uploadDirectoryPath, int partNumber)
    {
        return Path.Combine(GetMultipartPartsDirectoryPath(uploadDirectoryPath), $"{partNumber:D5}.part");
    }

    private async Task<ObjectInfo> CreateObjectInfoAsync(string bucketName, string filePath, bool isLatest, CancellationToken cancellationToken)
    {
        var objectKey = GetObjectKey(GetBucketPath(bucketName), filePath);
        var metadata = await ReadStoredObjectMetadataAsync(bucketName, objectKey, filePath, versionId: null, cancellationToken);
        return CreateObjectInfo(bucketName, objectKey, filePath, metadata, isLatest);
    }

    private Task<ObjectInfo> CreateObjectInfoAsync(string bucketName, string filePath, CancellationToken cancellationToken)
    {
        return CreateObjectInfoAsync(bucketName, filePath, isLatest: true, cancellationToken);
    }

    private ObjectInfo CreateObjectInfo(string bucketName, string objectKey, string? contentPath, DiskObjectMetadata metadata, bool isLatest)
    {
        var fileInfo = !string.IsNullOrWhiteSpace(contentPath) && File.Exists(contentPath)
            ? new FileInfo(contentPath)
            : null;

        var lastModifiedUtc = metadata.LastModifiedUtc
            ?? fileInfo?.LastWriteTimeUtc
            ?? DateTimeOffset.UtcNow;

        return new ObjectInfo
        {
            BucketName = bucketName,
            Key = objectKey,
            VersionId = metadata.VersionId,
            IsLatest = isLatest,
            IsDeleteMarker = metadata.IsDeleteMarker,
            ContentLength = metadata.IsDeleteMarker ? 0 : fileInfo?.Length ?? 0,
            ContentType = metadata.IsDeleteMarker ? null : metadata.ContentType ?? "application/octet-stream",
            CacheControl = metadata.IsDeleteMarker ? null : metadata.CacheControl,
            ContentDisposition = metadata.IsDeleteMarker ? null : metadata.ContentDisposition,
            ContentEncoding = metadata.IsDeleteMarker ? null : metadata.ContentEncoding,
            ContentLanguage = metadata.IsDeleteMarker ? null : metadata.ContentLanguage,
            ExpiresUtc = metadata.IsDeleteMarker ? null : metadata.ExpiresUtc,
            ETag = metadata.IsDeleteMarker ? null : fileInfo is null ? null : BuildETag(fileInfo),
            LastModifiedUtc = lastModifiedUtc,
            Metadata = metadata.Metadata,
            Tags = metadata.Tags,
            Checksums = metadata.Checksums
        };
    }

    private Task<ObjectInfo> CreateObjectInfoAsync(string bucketName, string objectKey, string contentPath, DiskObjectMetadata metadata, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(CreateObjectInfo(bucketName, objectKey, contentPath, metadata, metadata.IsLatest));
    }

    private async Task<DiskObjectMetadata> ReadStoredObjectMetadataAsync(string bucketName, string key, string objectPath, string? versionId, CancellationToken cancellationToken)
    {
        if (_objectStateStore is not null) {
            var objectInfo = await _objectStateStore.GetObjectInfoAsync(Name, bucketName, key, versionId, cancellationToken);
            if (objectInfo is not null) {
                return ToDiskObjectMetadata(objectInfo);
            }
        }

        var metadata = await ReadMetadataAsync(objectPath, cancellationToken);
        if (_objectStateStore is null) {
            return metadata;
        }

        return await PromoteLegacyStoredObjectStateAsync(
            bucketName,
            key,
            objectPath,
            metadata,
            versionId: string.IsNullOrWhiteSpace(versionId) ? metadata.VersionId : versionId,
            isLatest: true,
            cancellationToken);
    }

    private async Task<DiskObjectMetadata?> TryReadCurrentObjectMetadataAsync(string bucketName, string key, string objectPath, CancellationToken cancellationToken)
    {
        if (_objectStateStore is not null) {
            var objectInfo = await _objectStateStore.GetObjectInfoAsync(Name, bucketName, key, versionId: null, cancellationToken);
            if (objectInfo is not null) {
                return ToDiskObjectMetadata(objectInfo);
            }
        }

        var metadataPath = GetMetadataPath(objectPath);
        if (!File.Exists(metadataPath)) {
            return null;
        }

        var metadata = await ReadMetadataAsync(objectPath, cancellationToken);
        if (_objectStateStore is null) {
            return metadata;
        }

        return await PromoteLegacyStoredObjectStateAsync(
            bucketName,
            key,
            objectPath,
            metadata,
            versionId: metadata.VersionId,
            isLatest: true,
            cancellationToken);
    }

    private async Task<DiskObjectMetadata?> ReadArchivedObjectMetadataAsync(string bucketName, string key, string versionId, CancellationToken cancellationToken)
    {
        if (_objectStateStore is not null) {
            var objectInfo = await _objectStateStore.GetObjectInfoAsync(Name, bucketName, key, versionId, cancellationToken);
            if (objectInfo is not null) {
                return ToDiskObjectMetadata(objectInfo);
            }
        }

        var archivedContentPath = GetArchivedVersionContentPath(bucketName, key, versionId);
        var archivedMetadataPath = GetMetadataPath(archivedContentPath);
        if (!File.Exists(archivedContentPath) && !File.Exists(archivedMetadataPath)) {
            return null;
        }

        var metadata = await ReadMetadataAsync(archivedContentPath, cancellationToken);
        if (_objectStateStore is null) {
            return metadata;
        }

        return await PromoteLegacyStoredObjectStateAsync(
            bucketName,
            key,
            archivedContentPath,
            metadata,
            versionId,
            isLatest: false,
            cancellationToken);
    }

    private async Task<DiskObjectMetadata> PromoteLegacyStoredObjectStateAsync(
        string bucketName,
        string key,
        string objectPath,
        DiskObjectMetadata metadata,
        string? versionId,
        bool isLatest,
        CancellationToken cancellationToken)
    {
        if (_objectStateStore is null) {
            return metadata;
        }

        var resolvedVersionId = string.IsNullOrWhiteSpace(versionId)
            ? metadata.VersionId
            : versionId;

        await WriteStoredObjectStateAsync(
            bucketName,
            key,
            objectPath,
            resolvedVersionId,
            metadata.ContentType,
            metadata.Metadata,
            metadata.Tags,
            metadata.Checksums,
            metadata.IsDeleteMarker,
            isLatest,
            metadata.LastModifiedUtc,
            cancellationToken: cancellationToken,
            cacheControl: metadata.CacheControl,
            contentDisposition: metadata.ContentDisposition,
            contentEncoding: metadata.ContentEncoding,
            contentLanguage: metadata.ContentLanguage,
            expiresUtc: metadata.ExpiresUtc);

        var hydratedObjectInfo = await _objectStateStore.GetObjectInfoAsync(Name, bucketName, key, resolvedVersionId, cancellationToken);
        return hydratedObjectInfo is null
            ? metadata
            : ToDiskObjectMetadata(hydratedObjectInfo);
    }

    private async Task WriteStoredObjectStateAsync(
        string bucketName,
        string key,
        string objectPath,
        string? versionId,
        string? contentType,
        IReadOnlyDictionary<string, string>? metadata,
        IReadOnlyDictionary<string, string>? tags,
        IReadOnlyDictionary<string, string>? checksums,
        bool isDeleteMarker,
        bool isLatest,
        DateTimeOffset? lastModifiedUtc,
        CancellationToken cancellationToken = default,
        string? cacheControl = null,
        string? contentDisposition = null,
        string? contentEncoding = null,
        string? contentLanguage = null,
        DateTimeOffset? expiresUtc = null)
    {
        if (_objectStateStore is null) {
            await WriteMetadataAsync(objectPath, new DiskObjectMetadata
            {
                VersionId = versionId,
                IsLatest = isLatest,
                IsDeleteMarker = isDeleteMarker,
                LastModifiedUtc = lastModifiedUtc,
                ContentType = contentType,
                CacheControl = isDeleteMarker ? null : cacheControl,
                ContentDisposition = isDeleteMarker ? null : contentDisposition,
                ContentEncoding = isDeleteMarker ? null : contentEncoding,
                ContentLanguage = isDeleteMarker ? null : contentLanguage,
                ExpiresUtc = isDeleteMarker ? null : expiresUtc,
                Metadata = metadata is null ? null : new Dictionary<string, string>(metadata, StringComparer.Ordinal),
                Tags = tags is null ? null : new Dictionary<string, string>(tags, StringComparer.Ordinal),
                Checksums = checksums is null ? null : new Dictionary<string, string>(checksums, StringComparer.OrdinalIgnoreCase)
            }, cancellationToken);

            return;
        }

        FileInfo? fileInfo = !isDeleteMarker && File.Exists(objectPath)
            ? new FileInfo(objectPath)
            : null;

        await _objectStateStore.UpsertObjectInfoAsync(Name, new ObjectInfo
        {
            BucketName = bucketName,
            Key = key,
            VersionId = versionId,
            IsLatest = isLatest,
            IsDeleteMarker = isDeleteMarker,
            ContentLength = isDeleteMarker ? 0 : fileInfo?.Length ?? 0,
            ContentType = isDeleteMarker ? null : contentType,
            CacheControl = isDeleteMarker ? null : cacheControl,
            ContentDisposition = isDeleteMarker ? null : contentDisposition,
            ContentEncoding = isDeleteMarker ? null : contentEncoding,
            ContentLanguage = isDeleteMarker ? null : contentLanguage,
            ExpiresUtc = isDeleteMarker ? null : expiresUtc,
            ETag = isDeleteMarker ? null : fileInfo is null ? null : BuildETag(fileInfo),
            LastModifiedUtc = lastModifiedUtc ?? fileInfo?.LastWriteTimeUtc ?? DateTimeOffset.UtcNow,
            Metadata = metadata is null ? null : new Dictionary<string, string>(metadata, StringComparer.Ordinal),
            Tags = tags is null ? null : new Dictionary<string, string>(tags, StringComparer.Ordinal),
            Checksums = checksums is null ? null : new Dictionary<string, string>(checksums, StringComparer.OrdinalIgnoreCase)
        }, cancellationToken);

        DeleteMetadataFileIfPresent(objectPath);
    }

    private Task WriteStoredObjectStateAsync(
        string bucketName,
        string key,
        string objectPath,
        string? versionId,
        string? contentType,
        IReadOnlyDictionary<string, string>? metadata,
        IReadOnlyDictionary<string, string>? tags,
        IReadOnlyDictionary<string, string>? checksums,
        CancellationToken cancellationToken = default,
        string? cacheControl = null,
        string? contentDisposition = null,
        string? contentEncoding = null,
        string? contentLanguage = null,
        DateTimeOffset? expiresUtc = null)
    {
        return WriteStoredObjectStateAsync(
            bucketName,
            key,
            objectPath,
            versionId,
            contentType,
            metadata,
            tags,
            checksums,
            isDeleteMarker: false,
            isLatest: true,
            lastModifiedUtc: null,
            cancellationToken: cancellationToken,
            cacheControl: cacheControl,
            contentDisposition: contentDisposition,
            contentEncoding: contentEncoding,
            contentLanguage: contentLanguage,
            expiresUtc: expiresUtc);
    }

    private async Task DeleteStoredObjectStateAsync(string bucketName, string key, string objectPath, string? versionId, CancellationToken cancellationToken)
    {
        if (_objectStateStore is not null) {
            var resolvedVersionId = versionId;
            if (string.IsNullOrWhiteSpace(resolvedVersionId)) {
                resolvedVersionId = (await _objectStateStore.GetObjectInfoAsync(Name, bucketName, key, versionId: null, cancellationToken))?.VersionId;
            }

            if (!string.IsNullOrWhiteSpace(resolvedVersionId)) {
                await _objectStateStore.RemoveObjectInfoAsync(Name, bucketName, key, resolvedVersionId, cancellationToken);
            }

            DeleteMetadataFileIfPresent(objectPath);
            return;
        }

        DeleteMetadataFileIfPresent(objectPath);
    }

    private Task DeleteStoredObjectStateAsync(string bucketName, string key, string objectPath, CancellationToken cancellationToken)
    {
        return DeleteStoredObjectStateAsync(bucketName, key, objectPath, versionId: null, cancellationToken);
    }

    private async Task<DiskObjectMetadata> ReadMetadataAsync(string objectPath, CancellationToken cancellationToken)
    {
        var metadataPath = GetMetadataPath(objectPath);
        if (!File.Exists(metadataPath)) {
            return new DiskObjectMetadata();
        }

        await using var stream = new FileStream(metadataPath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.Asynchronous | FileOptions.SequentialScan);
        return await JsonSerializer.DeserializeAsync(stream, DiskStorageJsonSerializerContext.Default.DiskObjectMetadata, cancellationToken)
            ?? new DiskObjectMetadata();
    }

    private static void DeleteMetadataFileIfPresent(string objectPath)
    {
        var metadataPath = GetMetadataPath(objectPath);
        if (File.Exists(metadataPath)) {
            File.Delete(metadataPath);
        }
    }

    private async ValueTask<StorageResult<ResolvedStoredObject>> ResolveStoredObjectAsync(string bucketName, string key, string? versionId, CancellationToken cancellationToken)
    {
        var currentPath = GetObjectPath(bucketName, key);
        var currentResolution = await TryResolveCurrentStoredObjectAsync(bucketName, key, currentPath, cancellationToken);

        if (string.IsNullOrWhiteSpace(versionId)) {
            if (currentResolution is null) {
                return StorageResult<ResolvedStoredObject>.Failure(ObjectNotFound(bucketName, key));
            }

            return StorageResult<ResolvedStoredObject>.Success(currentResolution);
        }

        if (currentResolution is not null && string.Equals(currentResolution.Metadata.VersionId, versionId, StringComparison.Ordinal)) {
            return StorageResult<ResolvedStoredObject>.Success(currentResolution);
        }

        var archivedMetadata = await ReadArchivedObjectMetadataAsync(bucketName, key, versionId, cancellationToken);
        if (archivedMetadata is null) {
            return StorageResult<ResolvedStoredObject>.Failure(ObjectNotFound(bucketName, key, versionId));
        }

        var archivedContentPath = GetArchivedVersionContentPath(bucketName, key, versionId);
        if (archivedMetadata.IsDeleteMarker) {
            return StorageResult<ResolvedStoredObject>.Success(new ResolvedStoredObject(null, archivedMetadata, false, true));
        }

        if (!File.Exists(archivedContentPath)) {
            return StorageResult<ResolvedStoredObject>.Failure(ObjectNotFound(bucketName, key, versionId));
        }

        return StorageResult<ResolvedStoredObject>.Success(new ResolvedStoredObject(archivedContentPath, archivedMetadata, false, false));
    }

    private async Task<bool> IsVersioningEnabledAsync(string bucketName, CancellationToken cancellationToken)
    {
        var bucketPath = GetBucketPath(bucketName);
        if (!Directory.Exists(bucketPath)) {
            return false;
        }

        var metadata = await ReadBucketMetadataAsync(bucketPath, cancellationToken);
        return metadata.VersioningStatus == BucketVersioningStatus.Enabled;
    }

    private async Task ArchiveCurrentObjectVersionAsync(string bucketName, string key, string currentPath, CancellationToken cancellationToken)
    {
        var currentObject = await TryResolveCurrentStoredObjectAsync(bucketName, key, currentPath, cancellationToken);
        if (currentObject is null) {
            return;
        }

        var versionId = string.IsNullOrWhiteSpace(currentObject.Metadata.VersionId)
            ? CreateVersionId()
            : currentObject.Metadata.VersionId;
        var archivedContentPath = GetArchivedVersionContentPath(bucketName, key, versionId!);
        var archivedDirectoryPath = Path.GetDirectoryName(archivedContentPath)!;
        Directory.CreateDirectory(archivedDirectoryPath);

        if (!currentObject.IsDeleteMarker && !string.IsNullOrWhiteSpace(currentObject.ContentPath) && File.Exists(currentObject.ContentPath)) {
            File.Copy(currentObject.ContentPath, archivedContentPath, overwrite: true);
            File.SetLastWriteTimeUtc(archivedContentPath, File.GetLastWriteTimeUtc(currentObject.ContentPath));
        }

        await WriteStoredObjectStateAsync(
            bucketName,
            key,
            archivedContentPath,
            versionId,
            currentObject.Metadata.ContentType,
            currentObject.Metadata.Metadata,
            currentObject.Metadata.Tags,
            currentObject.Metadata.Checksums,
            currentObject.IsDeleteMarker,
            isLatest: false,
            currentObject.Metadata.LastModifiedUtc,
            cancellationToken: cancellationToken,
            cacheControl: currentObject.Metadata.CacheControl,
            contentDisposition: currentObject.Metadata.ContentDisposition,
            contentEncoding: currentObject.Metadata.ContentEncoding,
            contentLanguage: currentObject.Metadata.ContentLanguage,
            expiresUtc: currentObject.Metadata.ExpiresUtc);
    }

    private ArchivedVersionEntry? TryParseArchivedVersionPath(string bucketName, string archivedMetadataPath)
    {
        if (!IsMetadataFile(archivedMetadataPath)) {
            return null;
        }

        var versionsRootPath = GetVersionsRootPath(bucketName);
        var relativePath = Path.GetRelativePath(versionsRootPath, archivedMetadataPath).Replace('\\', '/');
        var segments = relativePath.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (segments.Length < 3 || !string.Equals(segments[^1], $"content{MetadataSuffix}", StringComparison.Ordinal)) {
            return null;
        }

        var versionId = segments[^2];
        var objectKey = string.Join('/', segments[..^2]);
        return string.IsNullOrWhiteSpace(objectKey)
            ? null
            : new ArchivedVersionEntry(objectKey, versionId, GetArchivedVersionContentPath(bucketName, objectKey, versionId));
    }

    private async Task<ResolvedStoredObject?> TryResolveCurrentStoredObjectAsync(string bucketName, string key, string currentPath, CancellationToken cancellationToken)
    {
        if (File.Exists(currentPath)) {
            var currentMetadata = await ReadStoredObjectMetadataAsync(bucketName, key, currentPath, versionId: null, cancellationToken);
            return new ResolvedStoredObject(currentPath, currentMetadata, true, false);
        }

        var currentMetadataWithoutContent = await TryReadCurrentObjectMetadataAsync(bucketName, key, currentPath, cancellationToken);
        return currentMetadataWithoutContent?.IsDeleteMarker == true
            ? new ResolvedStoredObject(null, currentMetadataWithoutContent, true, true)
            : null;
    }

    private async Task<IReadOnlyList<ObjectInfo>> GetOrderedObjectVersionsAsync(string bucketName, string? prefix, CancellationToken cancellationToken)
    {
        var versions = new List<ObjectInfo>();
        var seenVersions = new HashSet<(string Key, string? VersionId, bool IsDeleteMarker)>();

        void AddVersion(ObjectInfo version)
        {
            if (seenVersions.Add((version.Key, version.VersionId, version.IsDeleteMarker))) {
                versions.Add(version);
            }
        }

        if (_objectStateStore is not null) {
            await foreach (var version in _objectStateStore.ListObjectVersionsAsync(Name, bucketName, prefix, cancellationToken).WithCancellation(cancellationToken)) {
                AddVersion(version);
            }
        }

        var bucketPath = GetBucketPath(bucketName);

        if (Directory.Exists(bucketPath)) {
            foreach (var filePath in Directory.EnumerateFiles(bucketPath, "*", SearchOption.AllDirectories)
                         .Where(filePath => !IsSystemFile(bucketPath, filePath))) {
                cancellationToken.ThrowIfCancellationRequested();
                var objectKey = GetObjectKey(bucketPath, filePath);
                if (!string.IsNullOrWhiteSpace(prefix) && !objectKey.StartsWith(prefix, StringComparison.Ordinal)) {
                    continue;
                }

                AddVersion(await CreateObjectInfoAsync(bucketName, filePath, isLatest: true, cancellationToken));
            }

            foreach (var deleteMarker in await EnumerateCurrentDeleteMarkersAsync(bucketName, cancellationToken)) {
                if (!string.IsNullOrWhiteSpace(prefix) && !deleteMarker.Key.StartsWith(prefix, StringComparison.Ordinal)) {
                    continue;
                }

                AddVersion(deleteMarker);
            }
        }

        var archivedVersionsRootPath = GetVersionsRootPath(bucketName);
        if (Directory.Exists(archivedVersionsRootPath)) {
            foreach (var archivedMetadataPath in Directory.EnumerateFiles(archivedVersionsRootPath, $"*{MetadataSuffix}", SearchOption.AllDirectories)) {
                cancellationToken.ThrowIfCancellationRequested();
                var archivedVersion = TryParseArchivedVersionPath(bucketName, archivedMetadataPath);
                if (archivedVersion is null) {
                    continue;
                }

                if (!string.IsNullOrWhiteSpace(prefix) && !archivedVersion.ObjectKey.StartsWith(prefix, StringComparison.Ordinal)) {
                    continue;
                }

                var metadata = await ReadArchivedObjectMetadataAsync(bucketName, archivedVersion.ObjectKey, archivedVersion.VersionId, cancellationToken);
                if (metadata is null) {
                    continue;
                }

                AddVersion(CreateObjectInfo(bucketName, archivedVersion.ObjectKey, metadata.IsDeleteMarker ? null : archivedVersion.ContentPath, metadata, isLatest: false));
            }
        }

        return versions
            .OrderBy(version => version.Key, StringComparer.Ordinal)
            .ThenByDescending(version => version.IsLatest)
            .ThenByDescending(version => version.VersionId, StringComparer.Ordinal)
            .ToArray();
    }

    private async Task<IReadOnlyList<MultipartUploadInfo>> GetOrderedMultipartUploadsAsync(string bucketName, string? prefix, CancellationToken cancellationToken)
    {
        if (_multipartStateStore is not null) {
            var uploadsFromStateStore = new List<MultipartUploadInfo>();
            await foreach (var state in _multipartStateStore.ListMultipartUploadStatesAsync(Name, bucketName, prefix, cancellationToken).WithCancellation(cancellationToken)) {
                uploadsFromStateStore.Add(ToMultipartUploadInfo(state));
            }

            return uploadsFromStateStore
                .OrderBy(static upload => upload.Key, StringComparer.Ordinal)
                .ThenBy(upload => upload.InitiatedAtUtc)
                .ThenBy(static upload => upload.UploadId, StringComparer.Ordinal)
                .ToArray();
        }

        var bucketMultipartPath = GetMultipartBucketPath(bucketName);
        if (!Directory.Exists(bucketMultipartPath)) {
            return [];
        }

        var normalizedPrefix = NormalizeKey(prefix);
        var uploads = new List<MultipartUploadInfo>();

        foreach (var uploadDirectoryPath in Directory.EnumerateDirectories(bucketMultipartPath, "*", SearchOption.TopDirectoryOnly)) {
            cancellationToken.ThrowIfCancellationRequested();

            var uploadId = Path.GetFileName(uploadDirectoryPath);
            if (string.IsNullOrWhiteSpace(uploadId)) {
                continue;
            }

            var diskState = await ReadDiskMultipartUploadStateAsync(GetMultipartStatePath(uploadDirectoryPath), cancellationToken);
            if (diskState is null) {
                continue;
            }

            var state = ToMultipartUploadState(diskState);
            if (!string.IsNullOrWhiteSpace(normalizedPrefix)
                && !state.Key.StartsWith(normalizedPrefix, StringComparison.Ordinal)) {
                continue;
            }

            uploads.Add(ToMultipartUploadInfo(state));
        }

        return uploads
            .OrderBy(static upload => upload.Key, StringComparer.Ordinal)
            .ThenBy(upload => upload.InitiatedAtUtc)
            .ThenBy(static upload => upload.UploadId, StringComparer.Ordinal)
            .ToArray();
    }

    private async Task<bool> HasCurrentVersionStateAsync(string bucketName, string key, CancellationToken cancellationToken)
    {
        var currentPath = GetObjectPath(bucketName, key);
        return await TryResolveCurrentStoredObjectAsync(bucketName, key, currentPath, cancellationToken) is not null;
    }

    private async Task<ObjectInfo> CreateCurrentDeleteMarkerAsync(string bucketName, string key, CancellationToken cancellationToken)
    {
        var currentPath = GetObjectPath(bucketName, key);
        var versionId = CreateVersionId();
        var lastModifiedUtc = DateTimeOffset.UtcNow;

        await WriteStoredObjectStateAsync(
            bucketName,
            key,
            currentPath,
            versionId,
            contentType: null,
            metadata: null,
            tags: null,
            checksums: null,
            isDeleteMarker: true,
            isLatest: true,
            lastModifiedUtc,
            cancellationToken);

        return CreateObjectInfo(bucketName, key, null, new DiskObjectMetadata
        {
            VersionId = versionId,
            IsLatest = true,
            IsDeleteMarker = true,
            LastModifiedUtc = lastModifiedUtc
        }, isLatest: true);
    }

    private async Task DeleteArchivedVersionStateAsync(string bucketName, string key, string versionId, string? contentPath, CancellationToken cancellationToken)
    {
        var archivedContentPath = string.IsNullOrWhiteSpace(contentPath)
            ? GetArchivedVersionContentPath(bucketName, key, versionId)
            : contentPath;

        await DeleteStoredObjectStateAsync(bucketName, key, archivedContentPath, versionId, cancellationToken);
    }

    private async Task<ObjectInfo?> PromoteLatestArchivedVersionAsync(string bucketName, string key, CancellationToken cancellationToken)
    {
        var latestArchived = await TryGetLatestArchivedVersionAsync(bucketName, key, cancellationToken);
        if (latestArchived is null) {
            return null;
        }

        var archivedContentPath = GetArchivedVersionContentPath(bucketName, key, latestArchived.Metadata.VersionId!);
        var currentPath = GetObjectPath(bucketName, key);
        var currentDirectoryPath = Path.GetDirectoryName(currentPath)!;
        Directory.CreateDirectory(currentDirectoryPath);

        if (latestArchived.IsDeleteMarker) {
            await WriteStoredObjectStateAsync(
                bucketName,
                key,
                currentPath,
                latestArchived.Metadata.VersionId,
                contentType: null,
                latestArchived.Metadata.Metadata,
                latestArchived.Metadata.Tags,
                latestArchived.Metadata.Checksums,
                isDeleteMarker: true,
                isLatest: true,
                latestArchived.Metadata.LastModifiedUtc,
                cancellationToken: cancellationToken,
                cacheControl: latestArchived.Metadata.CacheControl,
                contentDisposition: latestArchived.Metadata.ContentDisposition,
                contentEncoding: latestArchived.Metadata.ContentEncoding,
                contentLanguage: latestArchived.Metadata.ContentLanguage,
                expiresUtc: latestArchived.Metadata.ExpiresUtc);
        }
        else {
            if (string.IsNullOrWhiteSpace(latestArchived.ContentPath) || !File.Exists(latestArchived.ContentPath)) {
                return null;
            }

            File.Move(latestArchived.ContentPath, currentPath, overwrite: true);
            await WriteStoredObjectStateAsync(
                bucketName,
                key,
                currentPath,
                latestArchived.Metadata.VersionId,
                latestArchived.Metadata.ContentType,
                latestArchived.Metadata.Metadata,
                latestArchived.Metadata.Tags,
                latestArchived.Metadata.Checksums,
                isDeleteMarker: false,
                isLatest: true,
                latestArchived.Metadata.LastModifiedUtc,
                cancellationToken: cancellationToken,
                cacheControl: latestArchived.Metadata.CacheControl,
                contentDisposition: latestArchived.Metadata.ContentDisposition,
                contentEncoding: latestArchived.Metadata.ContentEncoding,
                contentLanguage: latestArchived.Metadata.ContentLanguage,
                expiresUtc: latestArchived.Metadata.ExpiresUtc);
        }

        if (_objectStateStore is null) {
            await DeleteArchivedVersionStateAsync(bucketName, key, latestArchived.Metadata.VersionId!, latestArchived.ContentPath, cancellationToken);
        }
        else {
            DeleteMetadataFileIfPresent(archivedContentPath);
        }

        return CreateObjectInfo(bucketName, key, latestArchived.IsDeleteMarker ? null : currentPath, latestArchived.Metadata, isLatest: true);
    }

    private async Task<ResolvedStoredObject?> TryGetLatestArchivedVersionAsync(string bucketName, string key, CancellationToken cancellationToken)
    {
        ResolvedStoredObject? latestArchived = null;

        if (_objectStateStore is not null) {
            await foreach (var candidate in _objectStateStore.ListObjectVersionsAsync(Name, bucketName, key, cancellationToken).WithCancellation(cancellationToken)) {
                if (!string.Equals(candidate.Key, key, StringComparison.Ordinal) || candidate.IsLatest) {
                    continue;
                }

                if (latestArchived is null || StringComparer.Ordinal.Compare(candidate.VersionId, latestArchived.Metadata.VersionId) > 0) {
                    latestArchived = new ResolvedStoredObject(
                        candidate.IsDeleteMarker ? null : GetArchivedVersionContentPath(bucketName, key, candidate.VersionId!),
                        ToDiskObjectMetadata(candidate),
                        IsCurrent: false,
                        IsDeleteMarker: candidate.IsDeleteMarker);
                }
            }
        }

        var versionsDirectoryPath = GetObjectVersionsDirectoryPath(bucketName, key);
        if (!Directory.Exists(versionsDirectoryPath)) {
            return latestArchived;
        }

        foreach (var metadataPath in Directory.EnumerateFiles(versionsDirectoryPath, $"*{MetadataSuffix}", SearchOption.AllDirectories)) {
            cancellationToken.ThrowIfCancellationRequested();

            var archivedVersion = TryParseArchivedVersionPath(bucketName, metadataPath);
            if (archivedVersion is null || !string.Equals(archivedVersion.ObjectKey, key, StringComparison.Ordinal)) {
                continue;
            }

            var metadata = await ReadArchivedObjectMetadataAsync(bucketName, archivedVersion.ObjectKey, archivedVersion.VersionId, cancellationToken);
            if (metadata is null) {
                continue;
            }

            var candidate = new ResolvedStoredObject(
                metadata.IsDeleteMarker ? null : archivedVersion.ContentPath,
                metadata,
                IsCurrent: false,
                IsDeleteMarker: metadata.IsDeleteMarker);

            if (latestArchived is null || StringComparer.Ordinal.Compare(candidate.Metadata.VersionId, latestArchived.Metadata.VersionId) > 0) {
                latestArchived = candidate;
            }
        }

        return latestArchived;
    }

    private async Task<IReadOnlyList<ObjectInfo>> EnumerateCurrentDeleteMarkersAsync(string bucketName, CancellationToken cancellationToken)
    {
        var bucketPath = GetBucketPath(bucketName);
        if (!Directory.Exists(bucketPath)) {
            return [];
        }

        var results = new List<ObjectInfo>();

        foreach (var metadataPath in Directory.EnumerateFiles(bucketPath, $"*{MetadataSuffix}", SearchOption.AllDirectories)
                     .Where(path => !IsBucketMetadataFile(path) && !path.Contains($"{VersionStoreDirectoryName}{Path.DirectorySeparatorChar}", StringComparison.OrdinalIgnoreCase))) {
            cancellationToken.ThrowIfCancellationRequested();
            var objectPath = metadataPath[..^MetadataSuffix.Length];
            if (File.Exists(objectPath)) {
                continue;
            }

            var objectKey = GetObjectKey(bucketPath, objectPath);
            var metadata = await TryReadCurrentObjectMetadataAsync(bucketName, objectKey, objectPath, cancellationToken);
            if (metadata is null) {
                continue;
            }

            if (!metadata.IsDeleteMarker) {
                continue;
            }

            results.Add(CreateObjectInfo(bucketName, objectKey, null, metadata, isLatest: true));
        }

        return results;
    }

    private static bool IsVersionAfterMarker(ObjectInfo version, string? keyMarker, string? versionIdMarker)
    {
        if (string.IsNullOrWhiteSpace(keyMarker)) {
            return true;
        }

        var keyComparison = StringComparer.Ordinal.Compare(version.Key, NormalizeKey(keyMarker));
        if (keyComparison > 0) {
            return true;
        }

        if (keyComparison < 0) {
            return false;
        }

        return !string.IsNullOrWhiteSpace(versionIdMarker)
               && StringComparer.Ordinal.Compare(version.VersionId, versionIdMarker) < 0;
    }

    private static bool IsMultipartUploadAfterMarker(MultipartUploadInfo upload, string? keyMarker, string? uploadIdMarker)
    {
        if (string.IsNullOrWhiteSpace(keyMarker)) {
            return true;
        }

        var keyComparison = StringComparer.Ordinal.Compare(upload.Key, NormalizeKey(keyMarker));
        if (keyComparison > 0) {
            return true;
        }

        if (keyComparison < 0) {
            return false;
        }

        return !string.IsNullOrWhiteSpace(uploadIdMarker)
               && StringComparer.Ordinal.Compare(upload.UploadId, uploadIdMarker) > 0;
    }

    private static MultipartUploadInfo ToMultipartUploadInfo(MultipartUploadState state)
    {
        return new MultipartUploadInfo
        {
            BucketName = state.BucketName,
            Key = state.Key,
            UploadId = state.UploadId,
            InitiatedAtUtc = state.InitiatedAtUtc,
            ChecksumAlgorithm = state.ChecksumAlgorithm
        };
    }

    private static MultipartUploadState ToMultipartUploadState(DiskMultipartUploadState diskState)
    {
        return new MultipartUploadState
        {
            BucketName = diskState.BucketName,
            Key = diskState.Key,
            UploadId = diskState.UploadId,
            InitiatedAtUtc = diskState.InitiatedAtUtc,
            ContentType = diskState.ContentType,
            CacheControl = diskState.CacheControl,
            ContentDisposition = diskState.ContentDisposition,
            ContentEncoding = diskState.ContentEncoding,
            ContentLanguage = diskState.ContentLanguage,
            ExpiresUtc = diskState.ExpiresUtc,
            Metadata = diskState.Metadata,
            ChecksumAlgorithm = diskState.ChecksumAlgorithm
        };
    }

    private static DiskMultipartUploadState ToDiskMultipartUploadState(MultipartUploadState state)
    {
        return new DiskMultipartUploadState
        {
            BucketName = state.BucketName,
            Key = state.Key,
            UploadId = state.UploadId,
            InitiatedAtUtc = state.InitiatedAtUtc,
            ContentType = state.ContentType,
            CacheControl = state.CacheControl,
            ContentDisposition = state.ContentDisposition,
            ContentEncoding = state.ContentEncoding,
            ContentLanguage = state.ContentLanguage,
            ExpiresUtc = state.ExpiresUtc,
            Metadata = state.Metadata is null ? null : new Dictionary<string, string>(state.Metadata, StringComparer.Ordinal),
            ChecksumAlgorithm = state.ChecksumAlgorithm
        };
    }

    private static async ValueTask<DiskMultipartUploadState?> ReadDiskMultipartUploadStateAsync(string statePath, CancellationToken cancellationToken)
    {
        if (!File.Exists(statePath)) {
            return null;
        }

        await using var stream = new FileStream(statePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.Asynchronous | FileOptions.SequentialScan);
        return await JsonSerializer.DeserializeAsync(stream, DiskStorageJsonSerializerContext.Default.DiskMultipartUploadState, cancellationToken);
    }

    private static DiskObjectMetadata ToDiskObjectMetadata(ObjectInfo? objectInfo)
    {
        return objectInfo is null
            ? new DiskObjectMetadata()
            : new DiskObjectMetadata
            {
                VersionId = objectInfo.VersionId,
                IsLatest = objectInfo.IsLatest,
                IsDeleteMarker = objectInfo.IsDeleteMarker,
                LastModifiedUtc = objectInfo.LastModifiedUtc,
                ContentType = objectInfo.ContentType,
                CacheControl = objectInfo.CacheControl,
                ContentDisposition = objectInfo.ContentDisposition,
                ContentEncoding = objectInfo.ContentEncoding,
                ContentLanguage = objectInfo.ContentLanguage,
                ExpiresUtc = objectInfo.ExpiresUtc,
                Metadata = objectInfo.Metadata is null ? null : new Dictionary<string, string>(objectInfo.Metadata, StringComparer.Ordinal),
                Tags = objectInfo.Tags is null ? null : new Dictionary<string, string>(objectInfo.Tags, StringComparer.Ordinal),
                Checksums = objectInfo.Checksums is null ? null : new Dictionary<string, string>(objectInfo.Checksums, StringComparer.OrdinalIgnoreCase)
            };
    }

    private static bool HasBucketCorsConfiguration(DiskBucketMetadata metadata)
    {
        return metadata.CorsConfiguration?.Rules.Length > 0;
    }

    private static bool ShouldPersistBucketMetadata(DiskBucketMetadata metadata)
    {
        return metadata.VersioningStatus != BucketVersioningStatus.Disabled || HasBucketCorsConfiguration(metadata);
    }

    private static BucketCorsConfiguration ToBucketCorsConfiguration(string bucketName, DiskBucketCorsConfiguration configuration)
    {
        return new BucketCorsConfiguration
        {
            BucketName = bucketName,
            Rules = configuration.Rules.Select(ToBucketCorsRule).ToArray()
        };
    }

    private static BucketCorsRule ToBucketCorsRule(DiskBucketCorsRule rule)
    {
        return new BucketCorsRule
        {
            Id = rule.Id,
            AllowedOrigins = rule.AllowedOrigins,
            AllowedMethods = rule.AllowedMethods.Select(ParseBucketCorsMethod).ToArray(),
            AllowedHeaders = rule.AllowedHeaders,
            ExposeHeaders = rule.ExposeHeaders,
            MaxAgeSeconds = rule.MaxAgeSeconds
        };
    }

    private static DiskBucketCorsRule ToDiskBucketCorsRule(BucketCorsRule rule)
    {
        return new DiskBucketCorsRule
        {
            Id = string.IsNullOrWhiteSpace(rule.Id) ? null : rule.Id,
            AllowedOrigins = rule.AllowedOrigins.Where(static origin => !string.IsNullOrWhiteSpace(origin)).Select(static origin => origin.Trim()).ToArray(),
            AllowedMethods = rule.AllowedMethods.Select(ToBucketCorsMethodString).ToArray(),
            AllowedHeaders = rule.AllowedHeaders.Where(static header => !string.IsNullOrWhiteSpace(header)).Select(static header => header.Trim()).ToArray(),
            ExposeHeaders = rule.ExposeHeaders.Where(static header => !string.IsNullOrWhiteSpace(header)).Select(static header => header.Trim()).ToArray(),
            MaxAgeSeconds = rule.MaxAgeSeconds
        };
    }

    private static BucketCorsMethod ParseBucketCorsMethod(string method)
    {
        return method switch
        {
            "GET" => BucketCorsMethod.Get,
            "PUT" => BucketCorsMethod.Put,
            "POST" => BucketCorsMethod.Post,
            "DELETE" => BucketCorsMethod.Delete,
            "HEAD" => BucketCorsMethod.Head,
            _ => throw new InvalidOperationException($"Unsupported persisted CORS method '{method}'.")
        };
    }

    private static string ToBucketCorsMethodString(BucketCorsMethod method)
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

    private async Task<StorageResult<BucketInfo>> HeadBucketCoreAsync(string bucketName, string bucketPath, CancellationToken cancellationToken)
    {
        var directoryInfo = new DirectoryInfo(bucketPath);
        var metadata = await ReadBucketMetadataAsync(bucketPath, cancellationToken);
        return StorageResult<BucketInfo>.Success(new BucketInfo
        {
            Name = bucketName,
            CreatedAtUtc = directoryInfo.CreationTimeUtc,
            VersioningEnabled = metadata.VersioningStatus == BucketVersioningStatus.Enabled
        });
    }

    private static void DeleteBucketMetadata(string bucketPath)
    {
        var bucketMetadataPath = GetBucketMetadataPath(bucketPath);
        if (File.Exists(bucketMetadataPath)) {
            File.Delete(bucketMetadataPath);
        }
    }

    private static async Task<DiskBucketMetadata> ReadBucketMetadataAsync(string bucketPath, CancellationToken cancellationToken)
    {
        var bucketMetadataPath = GetBucketMetadataPath(bucketPath);
        if (!File.Exists(bucketMetadataPath)) {
            return new DiskBucketMetadata();
        }

        await using var stream = new FileStream(bucketMetadataPath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.Asynchronous | FileOptions.SequentialScan);
        return await JsonSerializer.DeserializeAsync(stream, DiskStorageJsonSerializerContext.Default.DiskBucketMetadata, cancellationToken)
            ?? new DiskBucketMetadata();
    }

    private static async Task WriteBucketMetadataAsync(string bucketPath, DiskBucketMetadata metadata, CancellationToken cancellationToken)
    {
        var bucketMetadataPath = GetBucketMetadataPath(bucketPath);
        var tempBucketMetadataPath = $"{bucketMetadataPath}.{Guid.NewGuid():N}.tmp";
        try {
            await using (var stream = new FileStream(tempBucketMetadataPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous | FileOptions.SequentialScan)) {
                await JsonSerializer.SerializeAsync(stream, metadata, DiskStorageJsonSerializerContext.Default.DiskBucketMetadata, cancellationToken);
            }

            File.Move(tempBucketMetadataPath, bucketMetadataPath, overwrite: true);
        }
        finally {
            if (File.Exists(tempBucketMetadataPath)) {
                File.Delete(tempBucketMetadataPath);
            }
        }
    }

    private StorageError CorsConfigurationNotFound(string bucketName)
    {
        return new StorageError
        {
            Code = StorageErrorCode.CorsConfigurationNotFound,
            Message = $"Bucket '{bucketName}' does not have a CORS configuration.",
            BucketName = bucketName,
            ProviderName = Name,
            SuggestedHttpStatusCode = 404
        };
    }

    private async ValueTask<StorageResult<MultipartUploadStateContext>> ReadMultipartStateAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken)
    {
        var uploadDirectoryPath = GetMultipartUploadPath(bucketName, uploadId);
        var statePath = GetMultipartStatePath(uploadDirectoryPath);
        if (_multipartStateStore is null && !File.Exists(statePath)) {
            return StorageResult<MultipartUploadStateContext>.Failure(MultipartConflict(
                $"Multipart upload '{uploadId}' was not found.",
                bucketName,
                key));
        }

        var state = await ReadStoredMultipartStateAsync(bucketName, key, uploadId, statePath, cancellationToken);
        if (state is null
            || !string.Equals(state.BucketName, bucketName, StringComparison.Ordinal)
            || !string.Equals(state.Key, key, StringComparison.Ordinal)
            || !string.Equals(state.UploadId, uploadId, StringComparison.Ordinal)) {
            return StorageResult<MultipartUploadStateContext>.Failure(MultipartConflict(
                $"Multipart upload '{uploadId}' does not match the supplied bucket or key.",
                bucketName,
                key));
        }

        return StorageResult<MultipartUploadStateContext>.Success(new MultipartUploadStateContext(uploadDirectoryPath, state));
    }

    private async ValueTask<StorageResult<MultipartUploadInfo>> WriteMultipartStateAndReturnAsync(
        string uploadDirectoryPath,
        MultipartUploadInfo uploadInfo,
        InitiateMultipartUploadRequest request,
        CancellationToken cancellationToken)
    {
        var state = new MultipartUploadState
        {
            BucketName = uploadInfo.BucketName,
            Key = uploadInfo.Key,
            UploadId = uploadInfo.UploadId,
            InitiatedAtUtc = uploadInfo.InitiatedAtUtc,
            ContentType = request.ContentType,
            CacheControl = request.CacheControl,
            ContentDisposition = request.ContentDisposition,
            ContentEncoding = request.ContentEncoding,
            ContentLanguage = request.ContentLanguage,
            ExpiresUtc = request.ExpiresUtc,
            Metadata = request.Metadata is null ? null : new Dictionary<string, string>(request.Metadata),
            ChecksumAlgorithm = uploadInfo.ChecksumAlgorithm
        };

        var statePath = GetMultipartStatePath(uploadDirectoryPath);
        var tempStatePath = $"{statePath}.{Guid.NewGuid():N}.tmp";
        var diskState = ToDiskMultipartUploadState(state);
        try {
            await using (var stream = new FileStream(tempStatePath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous | FileOptions.SequentialScan)) {
                await JsonSerializer.SerializeAsync(stream, diskState, DiskStorageJsonSerializerContext.Default.DiskMultipartUploadState, cancellationToken);
            }

            File.Move(tempStatePath, statePath, overwrite: true);

            if (_multipartStateStore is not null) {
                try {
                    await _multipartStateStore.UpsertMultipartUploadStateAsync(Name, state, cancellationToken);
                }
                catch {
                    if (File.Exists(statePath)) {
                        File.Delete(statePath);
                    }

                    throw;
                }
            }

            Directory.CreateDirectory(GetMultipartPartsDirectoryPath(uploadDirectoryPath));
            return StorageResult<MultipartUploadInfo>.Success(uploadInfo);
        }
        finally {
            if (File.Exists(tempStatePath)) {
                File.Delete(tempStatePath);
            }
        }
    }

    private async ValueTask<MultipartUploadState?> ReadStoredMultipartStateAsync(
        string bucketName,
        string key,
        string uploadId,
        string statePath,
        CancellationToken cancellationToken)
    {
        if (_multipartStateStore is not null) {
            return await _multipartStateStore.GetMultipartUploadStateAsync(Name, bucketName, key, uploadId, cancellationToken);
        }

        var diskState = await ReadDiskMultipartUploadStateAsync(statePath, cancellationToken);
        return diskState is null
            ? null
            : ToMultipartUploadState(diskState);
    }

    private async Task DeleteStoredMultipartStateAsync(
        string bucketName,
        string key,
        string uploadId,
        string uploadDirectoryPath,
        CancellationToken cancellationToken)
    {
        if (_multipartStateStore is not null) {
            await _multipartStateStore.RemoveMultipartUploadStateAsync(Name, bucketName, key, uploadId, cancellationToken);
            return;
        }

        var statePath = GetMultipartStatePath(uploadDirectoryPath);
        if (File.Exists(statePath)) {
            File.Delete(statePath);
        }
    }

    private static async Task WriteMetadataAsync(string objectPath, DiskObjectMetadata metadata, CancellationToken cancellationToken)
    {
        var metadataPath = GetMetadataPath(objectPath);
        var metadataDirectory = Path.GetDirectoryName(metadataPath);
        if (!string.IsNullOrWhiteSpace(metadataDirectory)) {
            Directory.CreateDirectory(metadataDirectory);
        }

        var tempMetadataPath = $"{metadataPath}.{Guid.NewGuid():N}.tmp";
        try {
            await using (var stream = new FileStream(tempMetadataPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous | FileOptions.SequentialScan)) {
                await JsonSerializer.SerializeAsync(stream, metadata, DiskStorageJsonSerializerContext.Default.DiskObjectMetadata, cancellationToken);
            }

            File.Move(tempMetadataPath, metadataPath, overwrite: true);
        }
        finally {
            if (File.Exists(tempMetadataPath)) {
                File.Delete(tempMetadataPath);
            }
        }
    }

    private static string BuildETag(FileInfo fileInfo)
    {
        return $"{fileInfo.Length:x}-{fileInfo.LastWriteTimeUtc.Ticks:x}";
    }

    private static string CreateVersionId()
    {
        return Guid.CreateVersion7().ToString("N");
    }

    private static async Task<IReadOnlyDictionary<string, string>> ComputeChecksumsAsync(string objectPath, CancellationToken cancellationToken)
    {
        await using var stream = new FileStream(objectPath, FileMode.Open, FileAccess.Read, FileShare.Read, 81920, FileOptions.Asynchronous | FileOptions.SequentialScan);
        using var sha256 = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        using var sha1 = IncrementalHash.CreateHash(HashAlgorithmName.SHA1);
        var crc32 = Crc32Accumulator.Create();
        var crc32c = Crc32Accumulator.CreateCastagnoli();
        var buffer = new byte[81920];

        while (true) {
            var read = await stream.ReadAsync(buffer.AsMemory(0, buffer.Length), cancellationToken);
            if (read == 0) {
                break;
            }

            sha256.AppendData(buffer, 0, read);
            sha1.AppendData(buffer, 0, read);
            crc32.Append(buffer.AsSpan(0, read));
            crc32c.Append(buffer.AsSpan(0, read));
        }

        return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["sha256"] = Convert.ToBase64String(sha256.GetHashAndReset()),
            ["sha1"] = Convert.ToBase64String(sha1.GetHashAndReset()),
            ["crc32"] = Convert.ToBase64String(crc32.GetHashBytes()),
            ["crc32c"] = Convert.ToBase64String(crc32c.GetHashBytes())
        };
    }

    private static StorageError? GetUnsupportedServerSideEncryptionError(
        ObjectServerSideEncryptionSettings? serverSideEncryption,
        string bucketName,
        string objectKey,
        string operationDescription)
    {
        return serverSideEncryption is null
            ? null
            : StorageError.Unsupported(
                $"Server-side encryption is not currently supported by the disk provider for {operationDescription}.",
                bucketName,
                objectKey);
    }

    private StorageError? ValidateRequestedChecksums(
        IReadOnlyDictionary<string, string>? requestedChecksums,
        IReadOnlyDictionary<string, string>? actualChecksums,
        string bucketName,
        string objectKey)
    {
        if (requestedChecksums is null || requestedChecksums.Count == 0) {
            return null;
        }

        foreach (var requestedChecksum in requestedChecksums) {
            if (!string.Equals(requestedChecksum.Key, Sha256ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
                && !string.Equals(requestedChecksum.Key, Sha1ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
                && !string.Equals(requestedChecksum.Key, Crc32ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
                && !string.Equals(requestedChecksum.Key, Crc32cChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)) {
                return StorageError.Unsupported(
                    $"Checksum algorithm '{requestedChecksum.Key}' is not currently supported for request validation.",
                    bucketName,
                    objectKey);
            }

            if (actualChecksums is null
                || !actualChecksums.TryGetValue(requestedChecksum.Key, out var actualChecksum)
                || !string.Equals(requestedChecksum.Value, actualChecksum, StringComparison.Ordinal)) {
                return new StorageError
                {
                    Code = StorageErrorCode.InvalidChecksum,
                    Message = $"The supplied {requestedChecksum.Key.ToUpperInvariant()} checksum for object '{objectKey}' does not match the uploaded content.",
                    BucketName = bucketName,
                    ObjectKey = objectKey,
                    ProviderName = options.ProviderName,
                    SuggestedHttpStatusCode = 400
                };
            }
        }

        return null;
    }

    private static bool TryNormalizeChecksumAlgorithm(string? value, out string? checksumAlgorithm)
    {
        if (string.IsNullOrWhiteSpace(value)) {
            checksumAlgorithm = null;
            return true;
        }

        if (string.Equals(value, Sha256ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "SHA256", StringComparison.OrdinalIgnoreCase)) {
            checksumAlgorithm = Sha256ChecksumAlgorithm;
            return true;
        }

        if (string.Equals(value, Sha1ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "SHA1", StringComparison.OrdinalIgnoreCase)) {
            checksumAlgorithm = Sha1ChecksumAlgorithm;
            return true;
        }

        if (string.Equals(value, Crc32ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "CRC32", StringComparison.OrdinalIgnoreCase)) {
            checksumAlgorithm = Crc32ChecksumAlgorithm;
            return true;
        }

        if (string.Equals(value, Crc32cChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "CRC32C", StringComparison.OrdinalIgnoreCase)) {
            checksumAlgorithm = Crc32cChecksumAlgorithm;
            return true;
        }

        checksumAlgorithm = null;
        return false;
    }

    private static bool TryGetChecksumValue(IReadOnlyDictionary<string, string>? checksums, string? algorithm, out string value)
    {
        value = string.Empty;
        if (checksums is null || string.IsNullOrWhiteSpace(algorithm)) {
            return false;
        }

        if (checksums.TryGetValue(algorithm, out var directValue) && !string.IsNullOrWhiteSpace(directValue)) {
            value = directValue;
            return true;
        }

        foreach (var checksum in checksums) {
            if (string.Equals(checksum.Key, algorithm, StringComparison.OrdinalIgnoreCase)
                && !string.IsNullOrWhiteSpace(checksum.Value)) {
                value = checksum.Value;
                return true;
            }
        }

        value = string.Empty;
        return false;
    }

    private static IReadOnlyDictionary<string, string>? CreateMultipartPartResponseChecksums(
        IReadOnlyDictionary<string, string> actualChecksums,
        string? uploadChecksumAlgorithm,
        IReadOnlyDictionary<string, string>? requestedChecksums)
    {
        if (!string.IsNullOrWhiteSpace(uploadChecksumAlgorithm)
            && TryGetChecksumValue(actualChecksums, uploadChecksumAlgorithm, out var uploadChecksum)) {
            return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [uploadChecksumAlgorithm] = uploadChecksum
            };
        }

        if (requestedChecksums is null || requestedChecksums.Count == 0) {
            return null;
        }

        var responseChecksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var requestedChecksum in requestedChecksums) {
            if (TryGetChecksumValue(actualChecksums, requestedChecksum.Key, out var actualChecksum)) {
                responseChecksums[requestedChecksum.Key] = actualChecksum;
            }
        }

        return responseChecksums.Count == 0
            ? null
            : responseChecksums;
    }

    private static string BuildCompositeChecksum(string algorithm, IReadOnlyList<string> partChecksums)
    {
        if (string.Equals(algorithm, Sha256ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)) {
            return BuildCompositeSha256Checksum(partChecksums);
        }

        if (string.Equals(algorithm, Sha1ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)) {
            return BuildCompositeSha1Checksum(partChecksums);
        }

        if (string.Equals(algorithm, Crc32cChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)) {
            return BuildCompositeCrc32cChecksum(partChecksums);
        }

        throw new InvalidOperationException($"Multipart checksum algorithm '{algorithm}' is not supported for composite checksum synthesis.");
    }

    private static string BuildCompositeSha256Checksum(IReadOnlyList<string> partChecksums)
    {
        using var checksum = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        foreach (var partChecksum in partChecksums) {
            checksum.AppendData(Convert.FromBase64String(partChecksum));
        }

        return $"{Convert.ToBase64String(checksum.GetHashAndReset())}-{partChecksums.Count}";
    }

    private static string BuildCompositeSha1Checksum(IReadOnlyList<string> partChecksums)
    {
        using var checksum = IncrementalHash.CreateHash(HashAlgorithmName.SHA1);
        foreach (var partChecksum in partChecksums) {
            checksum.AppendData(Convert.FromBase64String(partChecksum));
        }

        return $"{Convert.ToBase64String(checksum.GetHashAndReset())}-{partChecksums.Count}";
    }

    private static string BuildCompositeCrc32cChecksum(IReadOnlyList<string> partChecksums)
    {
        var checksum = Crc32Accumulator.CreateCastagnoli();
        foreach (var partChecksum in partChecksums) {
            checksum.Append(Convert.FromBase64String(partChecksum));
        }

        return $"{Convert.ToBase64String(checksum.GetHashBytes())}-{partChecksums.Count}";
    }

    private static StorageError? EvaluatePreconditions(GetObjectRequest request, ObjectInfo objectInfo)
    {
        if (!MatchesIfMatch(request.IfMatchETag, objectInfo.ETag)) {
            return new StorageError
            {
                Code = StorageErrorCode.PreconditionFailed,
                Message = $"The object '{objectInfo.Key}' does not match the supplied If-Match precondition.",
                BucketName = objectInfo.BucketName,
                ObjectKey = objectInfo.Key,
                SuggestedHttpStatusCode = 412
            };
        }

        if (ShouldEvaluateIfUnmodifiedSince(request.IfMatchETag, objectInfo.ETag)
            && request.IfUnmodifiedSinceUtc is { } ifUnmodifiedSinceUtc
            && WasModifiedAfter(objectInfo.LastModifiedUtc, ifUnmodifiedSinceUtc)) {
            return new StorageError
            {
                Code = StorageErrorCode.PreconditionFailed,
                Message = $"The object '{objectInfo.Key}' was modified after the supplied If-Unmodified-Since precondition.",
                BucketName = objectInfo.BucketName,
                ObjectKey = objectInfo.Key,
                SuggestedHttpStatusCode = 412
            };
        }

        return null;
    }

    private static bool IsNotModified(GetObjectRequest request, ObjectInfo objectInfo)
    {
        if (MatchesAnyETag(request.IfNoneMatchETag, objectInfo.ETag)) {
            return true;
        }

        return string.IsNullOrWhiteSpace(request.IfNoneMatchETag)
               && request.IfModifiedSinceUtc is { } ifModifiedSinceUtc
               && !WasModifiedAfter(objectInfo.LastModifiedUtc, ifModifiedSinceUtc);
    }

    private static StorageError? EvaluateCopyPreconditions(CopyObjectRequest request, ObjectInfo sourceInfo)
    {
        if (!MatchesIfMatch(request.SourceIfMatchETag, sourceInfo.ETag)) {
            return new StorageError
            {
                Code = StorageErrorCode.PreconditionFailed,
                Message = $"The source object '{sourceInfo.Key}' does not match the supplied copy If-Match precondition.",
                BucketName = sourceInfo.BucketName,
                ObjectKey = sourceInfo.Key,
                SuggestedHttpStatusCode = 412
            };
        }

        if (ShouldEvaluateIfUnmodifiedSince(request.SourceIfMatchETag, sourceInfo.ETag)
            && request.SourceIfUnmodifiedSinceUtc is { } ifUnmodifiedSinceUtc
            && WasModifiedAfter(sourceInfo.LastModifiedUtc, ifUnmodifiedSinceUtc)) {
            return new StorageError
            {
                Code = StorageErrorCode.PreconditionFailed,
                Message = $"The source object '{sourceInfo.Key}' was modified after the supplied copy If-Unmodified-Since precondition.",
                BucketName = sourceInfo.BucketName,
                ObjectKey = sourceInfo.Key,
                SuggestedHttpStatusCode = 412
            };
        }

        if (MatchesAnyETag(request.SourceIfNoneMatchETag, sourceInfo.ETag)) {
            return new StorageError
            {
                Code = StorageErrorCode.PreconditionFailed,
                Message = $"The source object '{sourceInfo.Key}' matches the supplied copy If-None-Match precondition.",
                BucketName = sourceInfo.BucketName,
                ObjectKey = sourceInfo.Key,
                SuggestedHttpStatusCode = 412
            };
        }

        if (request.SourceIfModifiedSinceUtc is { } ifModifiedSinceUtc
            && !WasModifiedAfter(sourceInfo.LastModifiedUtc, ifModifiedSinceUtc)) {
            return new StorageError
            {
                Code = StorageErrorCode.PreconditionFailed,
                Message = $"The source object '{sourceInfo.Key}' was not modified after the supplied copy If-Modified-Since precondition.",
                BucketName = sourceInfo.BucketName,
                ObjectKey = sourceInfo.Key,
                SuggestedHttpStatusCode = 412
            };
        }

        return null;
    }

    private static ObjectRange? NormalizeRange(ObjectRange? requestedRange, long contentLength, string bucketName, string objectKey, out StorageError? error)
    {
        error = null;

        if (requestedRange is null) {
            return null;
        }

        if (contentLength <= 0) {
            error = InvalidRange("Cannot satisfy a range request for an empty object.", bucketName, objectKey);
            return null;
        }

        long start;
        long end;

        if (requestedRange.Start is null) {
            var suffixLength = requestedRange.End;
            if (suffixLength is null || suffixLength <= 0) {
                error = InvalidRange("The requested suffix range is invalid.", bucketName, objectKey);
                return null;
            }

            var effectiveLength = Math.Min(suffixLength.Value, contentLength);
            start = contentLength - effectiveLength;
            end = contentLength - 1;
        }
        else {
            start = requestedRange.Start.Value;
            end = requestedRange.End ?? contentLength - 1;

            if (start < 0 || end < start) {
                error = InvalidRange("The requested byte range is invalid.", bucketName, objectKey);
                return null;
            }

            if (start >= contentLength) {
                error = InvalidRange("The requested range starts beyond the end of the object.", bucketName, objectKey);
                return null;
            }

            end = Math.Min(end, contentLength - 1);
        }

        return new ObjectRange
        {
            Start = start,
            End = end
        };
    }

    private static StorageError InvalidRange(string message, string bucketName, string objectKey)
    {
        return new StorageError
        {
            Code = StorageErrorCode.InvalidRange,
            Message = message,
            BucketName = bucketName,
            ObjectKey = objectKey,
            SuggestedHttpStatusCode = 416
        };
    }

    private StorageError InvalidTag(string message, string bucketName, string objectKey)
    {
        return new StorageError
        {
            Code = StorageErrorCode.InvalidTag,
            Message = message,
            BucketName = bucketName,
            ObjectKey = objectKey,
            ProviderName = options.ProviderName,
            SuggestedHttpStatusCode = 400
        };
    }

    private static bool MatchesIfMatch(string? rawHeader, string? currentETag)
    {
        if (string.IsNullOrWhiteSpace(rawHeader)) {
            return true;
        }

        if (rawHeader.Trim() == "*") {
            return true;
        }

        return MatchesAnyETag(rawHeader, currentETag);
    }

    private static bool ShouldEvaluateIfUnmodifiedSince(string? rawIfMatch, string? currentETag)
    {
        return string.IsNullOrWhiteSpace(rawIfMatch) || !MatchesIfMatch(rawIfMatch, currentETag);
    }

    private static bool MatchesAnyETag(string? rawHeader, string? currentETag)
    {
        if (string.IsNullOrWhiteSpace(rawHeader) || string.IsNullOrWhiteSpace(currentETag)) {
            return false;
        }

        var normalizedCurrent = NormalizeETag(currentETag);
        foreach (var candidate in rawHeader.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)) {
            if (candidate == "*" || NormalizeETag(candidate) == normalizedCurrent) {
                return true;
            }
        }

        return false;
    }

    private static string NormalizeETag(string value)
    {
        var trimmed = value.Trim();
        if (trimmed.StartsWith("W/", StringComparison.OrdinalIgnoreCase)) {
            trimmed = trimmed[2..].Trim();
        }

        if (trimmed.Length >= 2 && trimmed.StartsWith('"') && trimmed.EndsWith('"')) {
            trimmed = trimmed[1..^1];
        }

        return trimmed;
    }

    private StorageError MultipartConflict(string message, string bucketName, string objectKey)
    {
        return new StorageError
        {
            Code = StorageErrorCode.MultipartConflict,
            Message = message,
            BucketName = bucketName,
            ObjectKey = objectKey,
            ProviderName = options.ProviderName,
            SuggestedHttpStatusCode = 409
        };
    }

    private StorageError MultipartInvalidRequest(string message, string bucketName, string objectKey)
    {
        return new StorageError
        {
            Code = StorageErrorCode.MultipartConflict,
            Message = message,
            BucketName = bucketName,
            ObjectKey = objectKey,
            ProviderName = options.ProviderName,
            SuggestedHttpStatusCode = 400
        };
    }

    private static bool WasModifiedAfter(DateTimeOffset lastModifiedUtc, DateTimeOffset comparisonUtc)
    {
        return TruncateToWholeSeconds(lastModifiedUtc) > TruncateToWholeSeconds(comparisonUtc);
    }

    private static DateTimeOffset TruncateToWholeSeconds(DateTimeOffset value)
    {
        var utcValue = value.ToUniversalTime();
        return utcValue.AddTicks(-(utcValue.Ticks % TimeSpan.TicksPerSecond));
    }

    private static void DeleteEmptyParentDirectories(string? currentDirectoryPath, string stopAtDirectoryPath)
    {
        while (!string.IsNullOrWhiteSpace(currentDirectoryPath)
               && !string.Equals(currentDirectoryPath, stopAtDirectoryPath, StringComparison.OrdinalIgnoreCase)
               && Directory.Exists(currentDirectoryPath)
               && !Directory.EnumerateFileSystemEntries(currentDirectoryPath).Any()) {
            var parentPath = Directory.GetParent(currentDirectoryPath)?.FullName;
            Directory.Delete(currentDirectoryPath, recursive: false);
            currentDirectoryPath = parentPath;
        }
    }

    private StorageError BucketNotFound(string bucketName)
    {
        return new StorageError
        {
            Code = StorageErrorCode.BucketNotFound,
            Message = $"Bucket '{bucketName}' was not found.",
            BucketName = bucketName,
            ProviderName = options.ProviderName,
            SuggestedHttpStatusCode = 404
        };
    }

    private StorageError ObjectNotFound(string bucketName, string objectKey, string? versionId = null)
    {
        return new StorageError
        {
            Code = StorageErrorCode.ObjectNotFound,
            Message = string.IsNullOrWhiteSpace(versionId)
                ? $"Object '{objectKey}' was not found in bucket '{bucketName}'."
                : $"Object '{objectKey}' with version '{versionId}' was not found in bucket '{bucketName}'.",
            BucketName = bucketName,
            ObjectKey = objectKey,
            ProviderName = options.ProviderName,
            SuggestedHttpStatusCode = 404
        };
    }

    private StorageError GetDeleteMarkerAccessError(string bucketName, string objectKey, string? requestedVersionId, DiskObjectMetadata deleteMarkerMetadata)
    {
        return string.IsNullOrWhiteSpace(requestedVersionId)
            ? CurrentDeleteMarkerNotFound(bucketName, objectKey, deleteMarkerMetadata)
            : DeleteMarkerMethodNotAllowed(bucketName, objectKey, deleteMarkerMetadata);
    }

    private StorageError CurrentDeleteMarkerNotFound(string bucketName, string objectKey, DiskObjectMetadata deleteMarkerMetadata)
    {
        return new StorageError
        {
            Code = StorageErrorCode.ObjectNotFound,
            Message = $"Object '{objectKey}' was not found in bucket '{bucketName}'.",
            BucketName = bucketName,
            ObjectKey = objectKey,
            VersionId = deleteMarkerMetadata.VersionId,
            IsDeleteMarker = true,
            ProviderName = options.ProviderName,
            SuggestedHttpStatusCode = 404
        };
    }

    private StorageError DeleteMarkerMethodNotAllowed(string bucketName, string objectKey, DiskObjectMetadata deleteMarkerMetadata)
    {
        return new StorageError
        {
            Code = StorageErrorCode.MethodNotAllowed,
            Message = $"The specified version '{deleteMarkerMetadata.VersionId}' is a delete marker and does not support this operation.",
            BucketName = bucketName,
            ObjectKey = objectKey,
            VersionId = deleteMarkerMetadata.VersionId,
            IsDeleteMarker = true,
            LastModifiedUtc = deleteMarkerMetadata.LastModifiedUtc,
            ProviderName = options.ProviderName,
            SuggestedHttpStatusCode = 405
        };
    }

    private sealed record MultipartUploadStateContext(string UploadDirectoryPath, MultipartUploadState State);

    private sealed record ResolvedStoredObject(string? ContentPath, DiskObjectMetadata Metadata, bool IsCurrent, bool IsDeleteMarker);

    private sealed record ArchivedVersionEntry(string ObjectKey, string VersionId, string ContentPath);

    private struct Crc32Accumulator
    {
        private static readonly uint[] Crc32Table = CreateTable(0xEDB88320u);
        private static readonly uint[] Crc32cTable = CreateTable(0x82F63B78u);

        private readonly uint[] _table;
        private uint _current;

        public static Crc32Accumulator Create()
        {
            return new Crc32Accumulator(Crc32Table);
        }

        public static Crc32Accumulator CreateCastagnoli()
        {
            return new Crc32Accumulator(Crc32cTable);
        }

        private Crc32Accumulator(uint[] table)
        {
            _table = table;
            _current = 0xFFFFFFFFu;
        }

        public void Append(ReadOnlySpan<byte> buffer)
        {
            foreach (var value in buffer) {
                _current = (_current >> 8) ^ _table[(byte)(_current ^ value)];
            }
        }

        public byte[] GetHashBytes()
        {
            var finalized = ~_current;
            return
            [
                (byte)(finalized >> 24),
                (byte)(finalized >> 16),
                (byte)(finalized >> 8),
                (byte)finalized
            ];
        }

        private static uint[] CreateTable(uint polynomial)
        {
            var table = new uint[256];
            for (uint i = 0; i < table.Length; i++) {
                var value = i;
                for (var bit = 0; bit < 8; bit++) {
                    value = (value & 1) == 0
                        ? value >> 1
                        : polynomial ^ (value >> 1);
                }

                table[i] = value;
            }

            return table;
        }
    }

}
