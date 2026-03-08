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

internal sealed class DiskStorageService(DiskStorageOptions options) : IStorageBackend
{
    private const string MetadataSuffix = ".integrateds3.json";
    private const string MultipartUploadsDirectoryName = ".integrateds3-multipart";
    private const string MultipartStateFileName = "upload.json";

    private readonly string _rootPath = InitializeRootPath(options);

    public string Name => options.ProviderName;

    public string Kind => "disk";

    public bool IsPrimary => options.IsPrimary;

    public string? Description => $"Disk-backed provider rooted at '{_rootPath}'.";

    public ValueTask<StorageCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(DiskStorageCapabilities.CreateDefault());
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

            yield return new BucketInfo
            {
                Name = directoryInfo.Name,
                CreatedAtUtc = directoryInfo.CreationTimeUtc,
                VersioningEnabled = false
            };

            await Task.Yield();
        }
    }

    public ValueTask<StorageResult<BucketInfo>> CreateBucketAsync(CreateBucketRequest request, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var bucketPath = GetBucketPath(request.BucketName);
        if (Directory.Exists(bucketPath)) {
            return ValueTask.FromResult(StorageResult<BucketInfo>.Failure(new StorageError
            {
                Code = StorageErrorCode.BucketAlreadyExists,
                Message = $"Bucket '{request.BucketName}' already exists.",
                BucketName = request.BucketName,
                ProviderName = options.ProviderName
            }));
        }

        Directory.CreateDirectory(bucketPath);
        var directoryInfo = new DirectoryInfo(bucketPath);

        return ValueTask.FromResult(StorageResult<BucketInfo>.Success(new BucketInfo
        {
            Name = request.BucketName,
            CreatedAtUtc = directoryInfo.CreationTimeUtc,
            VersioningEnabled = false
        }));
    }

    public ValueTask<StorageResult<BucketInfo>> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var bucketPath = GetBucketPath(bucketName);
        if (!Directory.Exists(bucketPath)) {
            return ValueTask.FromResult(StorageResult<BucketInfo>.Failure(BucketNotFound(bucketName)));
        }

        var directoryInfo = new DirectoryInfo(bucketPath);
        return ValueTask.FromResult(StorageResult<BucketInfo>.Success(new BucketInfo
        {
            Name = bucketName,
            CreatedAtUtc = directoryInfo.CreationTimeUtc,
            VersioningEnabled = false
        }));
    }

    public ValueTask<StorageResult> DeleteBucketAsync(DeleteBucketRequest request, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var bucketPath = GetBucketPath(request.BucketName);
        if (!Directory.Exists(bucketPath)) {
            return ValueTask.FromResult(StorageResult.Failure(BucketNotFound(request.BucketName)));
        }

        if (Directory.EnumerateFileSystemEntries(bucketPath).Any()) {
            return ValueTask.FromResult(StorageResult.Failure(new StorageError
            {
                Code = StorageErrorCode.PreconditionFailed,
                Message = $"Bucket '{request.BucketName}' must be empty before it can be deleted.",
                BucketName = request.BucketName,
                ProviderName = options.ProviderName,
                SuggestedHttpStatusCode = 412
            }));
        }

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
            .Where(static filePath => !IsMetadataFile(filePath))
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

            yield return await CreateObjectInfoAsync(request.BucketName, entry.FilePath, cancellationToken);

            yielded++;
            if (pageSize is not null && yielded >= pageSize.Value) {
                yield break;
            }
        }
    }

    public async ValueTask<StorageResult<GetObjectResponse>> GetObjectAsync(GetObjectRequest request, CancellationToken cancellationToken = default)
    {
        var filePath = GetObjectPath(request.BucketName, request.Key);
        if (!File.Exists(filePath)) {
            return StorageResult<GetObjectResponse>.Failure(ObjectNotFound(request.BucketName, request.Key));
        }

        var objectInfo = await CreateObjectInfoAsync(request.BucketName, filePath, cancellationToken);
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

        var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 81920, FileOptions.Asynchronous | FileOptions.SequentialScan);

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
                Metadata = objectInfo.Metadata
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

    public async ValueTask<StorageResult<ObjectInfo>> CopyObjectAsync(CopyObjectRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var sourcePath = GetObjectPath(request.SourceBucketName, request.SourceKey);
        if (!File.Exists(sourcePath)) {
            return StorageResult<ObjectInfo>.Failure(ObjectNotFound(request.SourceBucketName, request.SourceKey));
        }

        var destinationBucketPath = GetBucketPath(request.DestinationBucketName);
        if (!Directory.Exists(destinationBucketPath)) {
            return StorageResult<ObjectInfo>.Failure(BucketNotFound(request.DestinationBucketName));
        }

        var sourceInfo = await CreateObjectInfoAsync(request.SourceBucketName, sourcePath, cancellationToken);
        var preconditionFailure = EvaluateCopyPreconditions(request, sourceInfo);
        if (preconditionFailure is not null) {
            return StorageResult<ObjectInfo>.Failure(preconditionFailure);
        }

        if (IsCopyNotModified(request, sourceInfo)) {
            return StorageResult<ObjectInfo>.Success(sourceInfo);
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

            File.Move(tempDestinationPath, destinationPath, overwrite: true);

            var sourceMetadata = await ReadMetadataAsync(sourcePath, cancellationToken);
            await WriteMetadataAsync(destinationPath, new DiskObjectMetadata
            {
                ContentType = sourceMetadata.ContentType,
                Metadata = sourceMetadata.Metadata is null ? null : new Dictionary<string, string>(sourceMetadata.Metadata)
            }, cancellationToken);
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

            File.Move(tempFilePath, objectPath, overwrite: true);
            await WriteMetadataAsync(objectPath, new DiskObjectMetadata
            {
                ContentType = string.IsNullOrWhiteSpace(request.ContentType) ? "application/octet-stream" : request.ContentType,
                Metadata = request.Metadata is null ? null : new Dictionary<string, string>(request.Metadata)
            }, cancellationToken);
        }
        finally {
            if (File.Exists(tempFilePath)) {
                File.Delete(tempFilePath);
            }
        }

        return StorageResult<ObjectInfo>.Success(await CreateObjectInfoAsync(request.BucketName, objectPath, cancellationToken));
    }

    public ValueTask<StorageResult<MultipartUploadInfo>> InitiateMultipartUploadAsync(InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        if (!Directory.Exists(GetBucketPath(request.BucketName))) {
            return ValueTask.FromResult(StorageResult<MultipartUploadInfo>.Failure(BucketNotFound(request.BucketName)));
        }

        _ = GetObjectPath(request.BucketName, request.Key);

        var uploadId = Guid.NewGuid().ToString("N");
        var uploadDirectoryPath = GetMultipartUploadPath(request.BucketName, uploadId);
        Directory.CreateDirectory(uploadDirectoryPath);

        var uploadInfo = new MultipartUploadInfo
        {
            BucketName = request.BucketName,
            Key = request.Key,
            UploadId = uploadId,
            InitiatedAtUtc = DateTimeOffset.UtcNow
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
        return StorageResult<MultipartUploadPart>.Success(new MultipartUploadPart
        {
            PartNumber = request.PartNumber,
            ETag = BuildETag(partInfo),
            ContentLength = partInfo.Length,
            LastModifiedUtc = partInfo.LastWriteTimeUtc
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
        var objectPath = GetObjectPath(request.BucketName, request.Key);
        var objectDirectoryPath = Path.GetDirectoryName(objectPath)!;
        Directory.CreateDirectory(objectDirectoryPath);

        var tempObjectPath = $"{objectPath}.{Guid.NewGuid():N}.tmp";
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

                    await using var sourceStream = new FileStream(partPath, FileMode.Open, FileAccess.Read, FileShare.Read, 81920, FileOptions.Asynchronous | FileOptions.SequentialScan);
                    await sourceStream.CopyToAsync(destinationStream, cancellationToken);
                }
            }

            File.Move(tempObjectPath, objectPath, overwrite: true);
            await WriteMetadataAsync(objectPath, new DiskObjectMetadata
            {
                ContentType = string.IsNullOrWhiteSpace(uploadState.State.ContentType) ? "application/octet-stream" : uploadState.State.ContentType,
                Metadata = uploadState.State.Metadata is null ? null : new Dictionary<string, string>(uploadState.State.Metadata)
            }, cancellationToken);

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
        DeleteEmptyParentDirectories(Path.GetDirectoryName(uploadStateResult.Value.UploadDirectoryPath), GetMultipartRootPath());
        return StorageResult.Success();
    }

    public async ValueTask<StorageResult<ObjectInfo>> HeadObjectAsync(HeadObjectRequest request, CancellationToken cancellationToken = default)
    {
        var filePath = GetObjectPath(request.BucketName, request.Key);
        if (!File.Exists(filePath)) {
            return StorageResult<ObjectInfo>.Failure(ObjectNotFound(request.BucketName, request.Key));
        }

        return StorageResult<ObjectInfo>.Success(await CreateObjectInfoAsync(request.BucketName, filePath, cancellationToken));
    }

    public ValueTask<StorageResult> DeleteObjectAsync(DeleteObjectRequest request, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var filePath = GetObjectPath(request.BucketName, request.Key);
        if (!File.Exists(filePath)) {
            return ValueTask.FromResult(StorageResult.Failure(ObjectNotFound(request.BucketName, request.Key)));
        }

        File.Delete(filePath);

        var metadataPath = GetMetadataPath(filePath);
        if (File.Exists(metadataPath)) {
            File.Delete(metadataPath);
        }

        DeleteEmptyParentDirectories(Path.GetDirectoryName(filePath), GetBucketPath(request.BucketName));
        return ValueTask.FromResult(StorageResult.Success());
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

    private string GetMultipartRootPath()
    {
        return Path.Combine(_rootPath, MultipartUploadsDirectoryName);
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

    private async Task<ObjectInfo> CreateObjectInfoAsync(string bucketName, string filePath, CancellationToken cancellationToken)
    {
        var fileInfo = new FileInfo(filePath);
        var metadata = await ReadMetadataAsync(filePath, cancellationToken);
        return new ObjectInfo
        {
            BucketName = bucketName,
            Key = GetObjectKey(GetBucketPath(bucketName), filePath),
            ContentLength = fileInfo.Length,
            ContentType = metadata.ContentType ?? "application/octet-stream",
            ETag = BuildETag(fileInfo),
            LastModifiedUtc = fileInfo.LastWriteTimeUtc,
            Metadata = metadata.Metadata
        };
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

    private async ValueTask<StorageResult<MultipartUploadStateContext>> ReadMultipartStateAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken)
    {
        var uploadDirectoryPath = GetMultipartUploadPath(bucketName, uploadId);
        var statePath = GetMultipartStatePath(uploadDirectoryPath);
        if (!File.Exists(statePath)) {
            return StorageResult<MultipartUploadStateContext>.Failure(MultipartConflict(
                $"Multipart upload '{uploadId}' was not found.",
                bucketName,
                key));
        }

        await using var stream = new FileStream(statePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.Asynchronous | FileOptions.SequentialScan);
        var state = await JsonSerializer.DeserializeAsync(stream, DiskStorageJsonSerializerContext.Default.DiskMultipartUploadState, cancellationToken);
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
        var state = new DiskMultipartUploadState
        {
            BucketName = uploadInfo.BucketName,
            Key = uploadInfo.Key,
            UploadId = uploadInfo.UploadId,
            InitiatedAtUtc = uploadInfo.InitiatedAtUtc,
            ContentType = request.ContentType,
            Metadata = request.Metadata is null ? null : new Dictionary<string, string>(request.Metadata)
        };

        var statePath = GetMultipartStatePath(uploadDirectoryPath);
        var tempStatePath = $"{statePath}.{Guid.NewGuid():N}.tmp";
        try {
            await using (var stream = new FileStream(tempStatePath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous | FileOptions.SequentialScan)) {
                await JsonSerializer.SerializeAsync(stream, state, DiskStorageJsonSerializerContext.Default.DiskMultipartUploadState, cancellationToken);
            }

            File.Move(tempStatePath, statePath, overwrite: true);
            Directory.CreateDirectory(GetMultipartPartsDirectoryPath(uploadDirectoryPath));
            return StorageResult<MultipartUploadInfo>.Success(uploadInfo);
        }
        finally {
            if (File.Exists(tempStatePath)) {
                File.Delete(tempStatePath);
            }
        }
    }

    private static async Task WriteMetadataAsync(string objectPath, DiskObjectMetadata metadata, CancellationToken cancellationToken)
    {
        var metadataPath = GetMetadataPath(objectPath);
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

        if (string.IsNullOrWhiteSpace(request.IfMatchETag)
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

        if (string.IsNullOrWhiteSpace(request.SourceIfMatchETag)
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

        return null;
    }

    private static bool IsCopyNotModified(CopyObjectRequest request, ObjectInfo sourceInfo)
    {
        if (MatchesAnyETag(request.SourceIfNoneMatchETag, sourceInfo.ETag)) {
            return true;
        }

        return string.IsNullOrWhiteSpace(request.SourceIfNoneMatchETag)
               && request.SourceIfModifiedSinceUtc is { } ifModifiedSinceUtc
               && !WasModifiedAfter(sourceInfo.LastModifiedUtc, ifModifiedSinceUtc);
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

    private StorageError ObjectNotFound(string bucketName, string objectKey)
    {
        return new StorageError
        {
            Code = StorageErrorCode.ObjectNotFound,
            Message = $"Object '{objectKey}' was not found in bucket '{bucketName}'.",
            BucketName = bucketName,
            ObjectKey = objectKey,
            ProviderName = options.ProviderName,
            SuggestedHttpStatusCode = 404
        };
    }

    private sealed record MultipartUploadStateContext(string UploadDirectoryPath, DiskMultipartUploadState State);

}
