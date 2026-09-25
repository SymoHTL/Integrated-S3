using System.Runtime.CompilerServices;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Engine.Blobs;
using IntegratedS3.Engine.Metadata;
using IntegratedS3.Shared;
using static IntegratedS3.Shared.ObjectChecksums;

namespace IntegratedS3.Engine;

internal sealed partial class EngineStorageBackend
{
    private const int MaxPartNumber = 10_000;
    private const long MinimumPartSize = 5L * 1024 * 1024;

    public async ValueTask<StorageResult<MultipartUploadInfo>> InitiateMultipartUploadAsync(InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (UnsupportedEncryption(request.ServerSideEncryption, request.CustomerEncryption, request.BucketName, request.Key, "multipart upload initiation") is { } encryptionError) {
            return StorageResult<MultipartUploadInfo>.Failure(encryptionError);
        }

        if (ObjectTagValidation.Validate(request.Tags) is { } tagError) {
            return StorageResult<MultipartUploadInfo>.Failure(InvalidTag(tagError, request.BucketName, request.Key));
        }

        if (!TryNormalizeChecksumAlgorithm(request.ChecksumAlgorithm, out var checksumAlgorithm)
            || !IsMultipartSupportedChecksumAlgorithm(checksumAlgorithm)) {
            return StorageResult<MultipartUploadInfo>.Failure(StorageError.Unsupported(
                $"Checksum algorithm '{request.ChecksumAlgorithm}' is not currently supported for multipart uploads.",
                request.BucketName,
                request.Key));
        }

        if (!TryEncodeKey(request.Key, out var key)) {
            return StorageResult<MultipartUploadInfo>.Failure(InvalidKey(request.BucketName, request.Key));
        }

        await using var transaction = await BeginAsync(write: true, cancellationToken);
        await transaction.LockBucketAsync(request.BucketName, exclusive: false, cancellationToken);
        if (await transaction.GetBucketAsync(request.BucketName, cancellationToken) is not { } bucket) {
            return StorageResult<MultipartUploadInfo>.Failure(BucketNotFound(request.BucketName));
        }

        var now = await transaction.GetClockAsync(cancellationToken);
        var upload = new UploadRow
        {
            BucketId = bucket.Id,
            // Time-ordered, so ordering a key's uploads by id lists the oldest first.
            UploadId = Guid.CreateVersion7().ToString("N"),
            Key = key,
            InitiatedUs = now,
            Meta = new UploadMeta
            {
                Object = new VersionMeta
                {
                    ContentType = request.ContentType,
                    CacheControl = request.CacheControl,
                    ContentDisposition = request.ContentDisposition,
                    ContentEncoding = request.ContentEncoding,
                    ContentLanguage = request.ContentLanguage,
                    ExpiresUtc = request.ExpiresUtc,
                    Expires = request.Expires,
                    Metadata = CopyOrNull(request.Metadata),
                    Tags = CopyOrNull(request.Tags)
                },
                ChecksumAlgorithm = checksumAlgorithm,
                StorageClass = NormalizeStoredStorageClass(request.StorageClass)
            }
        };
        await transaction.InsertUploadAsync(upload, cancellationToken);
        await transaction.CommitAsync();
        return StorageResult<MultipartUploadInfo>.Success(ToUploadInfo(request.BucketName, request.Key, upload));
    }

    public async ValueTask<StorageResult<MultipartUploadPart>> UploadMultipartPartAsync(UploadMultipartPartRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.PartNumber is < 1 or > MaxPartNumber) {
            return StorageResult<MultipartUploadPart>.Failure(PartNumberOutOfRange(request.BucketName, request.Key));
        }

        var upload = await FindUploadAsync(request.BucketName, request.Key, request.UploadId, cancellationToken);
        if (!upload.IsSuccess) {
            return StorageResult<MultipartUploadPart>.Failure(upload.Error!);
        }

        var uploadAlgorithm = upload.Value!.Meta.ChecksumAlgorithm;
        if (!TryNormalizeChecksumAlgorithm(request.ChecksumAlgorithm, out var requestAlgorithm)) {
            return StorageResult<MultipartUploadPart>.Failure(StorageError.Unsupported(
                $"Checksum algorithm '{request.ChecksumAlgorithm}' is not currently supported for multipart uploads.",
                request.BucketName,
                request.Key));
        }

        if (!string.IsNullOrWhiteSpace(uploadAlgorithm)
            && requestAlgorithm is not null
            && !string.Equals(uploadAlgorithm, requestAlgorithm, StringComparison.OrdinalIgnoreCase)) {
            return StorageResult<MultipartUploadPart>.Failure(MultipartInvalidRequest(
                $"Multipart upload '{request.UploadId}' requires checksum algorithm '{uploadAlgorithm.ToUpperInvariant()}'.",
                request.BucketName,
                request.Key));
        }

        var algorithms = DetermineRequiredChecksumAlgorithms(request.Checksums, uploadAlgorithm) | ToChecksumAlgorithmFlag(requestAlgorithm);
        string? copySourceVersionId = null;
        StorageResult<StoredBody> stored;
        if (!string.IsNullOrWhiteSpace(request.CopySourceBucketName) || !string.IsNullOrWhiteSpace(request.CopySourceKey)) {
            if (!string.IsNullOrWhiteSpace(request.ChecksumAlgorithm) || request.Checksums is { Count: > 0 }) {
                return StorageResult<MultipartUploadPart>.Failure(MultipartInvalidRequest(
                    "Checksum request headers are not supported for UploadPartCopy requests.",
                    request.BucketName,
                    request.Key));
            }

            copySourceVersionId = request.CopySourceVersionId;
            stored = await CopySourceIntoPartAsync(
                request.BucketName, request.Key, request.CopySourceBucketName ?? string.Empty, request.CopySourceKey ?? string.Empty,
                request.CopySourceVersionId, request.CopySourceRange, algorithms,
                source => ObjectPreconditions.EvaluateMultipartCopyPreconditions(request, source), cancellationToken);
        }
        else {
            ArgumentNullException.ThrowIfNull(request.Content);
            if (!string.IsNullOrWhiteSpace(uploadAlgorithm) && !TryGetChecksumValue(request.Checksums, uploadAlgorithm, out _)) {
                return StorageResult<MultipartUploadPart>.Failure(MultipartInvalidRequest(
                    $"The supplied part is missing the '{uploadAlgorithm.ToUpperInvariant()}' checksum required by multipart upload '{request.UploadId}'.",
                    request.BucketName,
                    request.Key));
            }

            stored = await StorePartBodyAsync(request.BucketName, request.Key, request.Content, algorithms, cancellationToken);
        }

        if (!stored.IsSuccess) {
            return StorageResult<MultipartUploadPart>.Failure(stored.Error!);
        }

        // The client's checksum is checked before the part is recorded, so a part with a bad digest never becomes
        // part of the upload.
        if (ValidateRequestedChecksums(request.Checksums, stored.Value!.Checksums, request.BucketName, request.Key, Name) is { } checksumError) {
            await BodyWriter.DeleteQuietlyAsync(_blobs, stored.Value.Locators);
            return StorageResult<MultipartUploadPart>.Failure(checksumError);
        }

        return await CommitPartAsync(request.BucketName, request.Key, request.UploadId, request.PartNumber, stored.Value, uploadAlgorithm,
            requestAlgorithm, request.Checksums, copySourceVersionId, cancellationToken);
    }

    public async ValueTask<StorageResult<MultipartUploadPart>> UploadPartCopyAsync(UploadPartCopyRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.PartNumber is < 1 or > MaxPartNumber) {
            return StorageResult<MultipartUploadPart>.Failure(PartNumberOutOfRange(request.BucketName, request.Key));
        }

        if (request.SourceRange is not null && (request.SourceRange.Start is null || request.SourceRange.End is null)) {
            return StorageResult<MultipartUploadPart>.Failure(MultipartInvalidRequest(
                "Multipart part copy ranges must specify both a start and end byte offset.",
                request.BucketName,
                request.Key));
        }

        if (UnsupportedEncryption(null, request.SourceCustomerEncryption, request.SourceBucketName, request.SourceKey, "copy source requests") is { } sourceEncryptionError) {
            return StorageResult<MultipartUploadPart>.Failure(sourceEncryptionError);
        }

        if (UnsupportedEncryption(null, request.DestinationCustomerEncryption, request.BucketName, request.Key, "copy destination requests") is { } destinationEncryptionError) {
            return StorageResult<MultipartUploadPart>.Failure(destinationEncryptionError);
        }

        var upload = await FindUploadAsync(request.BucketName, request.Key, request.UploadId, cancellationToken);
        if (!upload.IsSuccess) {
            return StorageResult<MultipartUploadPart>.Failure(upload.Error!);
        }

        var uploadAlgorithm = upload.Value!.Meta.ChecksumAlgorithm;
        if (!TryNormalizeChecksumAlgorithm(request.ChecksumAlgorithm, out var requestAlgorithm)) {
            return StorageResult<MultipartUploadPart>.Failure(StorageError.Unsupported(
                $"Checksum algorithm '{request.ChecksumAlgorithm}' is not currently supported for multipart uploads.",
                request.BucketName,
                request.Key));
        }

        var algorithms = DetermineRequiredChecksumAlgorithms(request.Checksums, uploadAlgorithm) | ToChecksumAlgorithmFlag(requestAlgorithm);
        var stored = await CopySourceIntoPartAsync(
            request.BucketName, request.Key, request.SourceBucketName, request.SourceKey, request.SourceVersionId, request.SourceRange, algorithms,
            source => ObjectPreconditions.EvaluateCopyPreconditions(request, source), cancellationToken);
        if (!stored.IsSuccess) {
            return StorageResult<MultipartUploadPart>.Failure(stored.Error!);
        }

        if (ValidateRequestedChecksums(request.Checksums, stored.Value!.Checksums, request.BucketName, request.Key, Name) is { } checksumError) {
            await BodyWriter.DeleteQuietlyAsync(_blobs, stored.Value.Locators);
            return StorageResult<MultipartUploadPart>.Failure(checksumError);
        }

        return await CommitPartAsync(request.BucketName, request.Key, request.UploadId, request.PartNumber, stored.Value, uploadAlgorithm,
            requestAlgorithm, request.Checksums, request.SourceVersionId, cancellationToken);
    }

    /// <summary>
    /// Completes an upload without copying a byte: the new version's manifest lists the parts' blobs. The upload's
    /// row is the lock that orders Complete against Abort, a second Complete and a late UploadPart.
    /// </summary>
    public async ValueTask<StorageResult<ObjectInfo>> CompleteMultipartUploadAsync(CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.Parts.Count == 0) {
            return StorageResult<ObjectInfo>.Failure(Error(StorageErrorCode.MultipartConflict,
                "At least one multipart part is required to complete an upload.", request.BucketName, request.Key, 409));
        }

        var previous = 0;
        foreach (var part in request.Parts) {
            if (part.PartNumber is < 1 or > MaxPartNumber) {
                return StorageResult<ObjectInfo>.Failure(PartNumberOutOfRange(request.BucketName, request.Key));
            }

            if (part.PartNumber <= previous) {
                return StorageResult<ObjectInfo>.Failure(Error(StorageErrorCode.InvalidPartOrder, part.PartNumber == previous
                        ? $"The list of parts was not in ascending order. Part '{part.PartNumber}' is listed more than once."
                        : $"The list of parts was not in ascending order. Part '{part.PartNumber}' followed part '{previous}'.",
                    request.BucketName, request.Key, 400));
            }

            previous = part.PartNumber;
        }

        if (!TryEncodeKey(request.Key, out var key)) {
            return StorageResult<ObjectInfo>.Failure(InvalidKey(request.BucketName, request.Key));
        }

        await using var transaction = await BeginAsync(write: true, cancellationToken);
        await transaction.LockBucketAsync(request.BucketName, exclusive: false, cancellationToken);
        if (await transaction.GetBucketAsync(request.BucketName, cancellationToken) is not { } bucket) {
            return StorageResult<ObjectInfo>.Failure(BucketNotFound(request.BucketName));
        }

        var upload = await transaction.GetUploadAsync(bucket.Id, request.UploadId, exclusive: true, cancellationToken);
        if (upload is null || !upload.Key.AsSpan().SequenceEqual(key)) {
            return StorageResult<ObjectInfo>.Failure(NoSuchUpload(request.BucketName, request.Key, request.UploadId));
        }

        var uploadAlgorithm = upload.Meta.ChecksumAlgorithm;
        var stored = new Dictionary<int, PartRow>();
        foreach (var part in await transaction.ListPartsAsync(upload.Id, 0, MaxPartNumber, cancellationToken)) {
            stored[part.PartNumber] = part;
        }

        var manifest = new List<Extent>();
        var partMd5s = new List<string>(request.Parts.Count);
        var compositeChecksums = uploadAlgorithm is null ? null : new List<string>(request.Parts.Count);
        long size = 0;
        for (var index = 0; index < request.Parts.Count; index++) {
            var requested = request.Parts[index];
            if (!stored.Remove(requested.PartNumber, out var part)) {
                return StorageResult<ObjectInfo>.Failure(Error(StorageErrorCode.InvalidPart,
                    $"One or more of the specified parts could not be found. Part '{requested.PartNumber}' was not uploaded for upload '{request.UploadId}'.",
                    request.BucketName, request.Key, 400));
            }

            if (index != request.Parts.Count - 1 && part.Size < MinimumPartSize) {
                return StorageResult<ObjectInfo>.Failure(MultipartInvalidRequest(
                    $"Your proposed upload is smaller than the minimum allowed size. Part '{requested.PartNumber}' is {part.Size} bytes, but every part except the last must be at least {MinimumPartSize} bytes.",
                    request.BucketName, request.Key));
            }

            if (!string.Equals(ObjectETags.NormalizeETag(requested.ETag), ObjectETags.NormalizeETag(part.ETag), StringComparison.Ordinal)) {
                return StorageResult<ObjectInfo>.Failure(Error(StorageErrorCode.InvalidPart,
                    $"The ETag supplied for part '{requested.PartNumber}' does not match the ETag of the uploaded part.",
                    request.BucketName, request.Key, 400));
            }

            if (ValidateRequestedChecksums(requested.Checksums, part.Meta.Checksums, request.BucketName, request.Key, Name) is { } checksumError) {
                return StorageResult<ObjectInfo>.Failure(checksumError);
            }

            if (compositeChecksums is not null) {
                if (!TryGetChecksumValue(part.Meta.Checksums, uploadAlgorithm, out var partChecksum)) {
                    return StorageResult<ObjectInfo>.Failure(StorageError.Unsupported(
                        $"Multipart {uploadAlgorithm!.ToUpperInvariant()} checksum synthesis requires per-part {uploadAlgorithm.ToUpperInvariant()} digests.",
                        request.BucketName, request.Key));
                }

                compositeChecksums.Add(partChecksum);
            }

            partMd5s.Add(part.Meta.Checksums[Md5ChecksumAlgorithm]);
            manifest.AddRange(part.Manifest);
            size += part.Size;
        }

        // ponytail: without an upload checksum algorithm the object gets no whole-object checksum, since computing one
        // would read every byte again (S3 behaves the same for such uploads). The ETag is the multipart ETag.
        Dictionary<string, string>? checksums = compositeChecksums is null
            ? null
            : new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [uploadAlgorithm!] = BuildCompositeChecksum(uploadAlgorithm!, compositeChecksums)
            };

        var objectMeta = upload.Meta.Object;
        var meta = objectMeta with
        {
            ContentType = string.IsNullOrWhiteSpace(objectMeta.ContentType) ? DefaultObjectContentType : objectMeta.ContentType,
            Checksums = checksums
        };

        // The parts' blobs move into the version and keep their references; parts uploaded but not listed are freed.
        var body = new StoredBody { Length = size, Manifest = manifest, Checksums = new Dictionary<string, string>() };
        return await CommitNewVersionAsync(transaction, request.BucketName, request.Key, key, body, ObjectETags.BuildMultipartETag(partMd5s),
            meta, upload.Meta.StorageClass, new WriteConditions(IfMatch: null, IfNoneMatch: null, OverwriteIfExists: true), cancellationToken,
            completeUpload: upload, unlistedParts: stored.Values);
    }

    public async ValueTask<StorageResult> AbortMultipartUploadAsync(AbortMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (!TryEncodeKey(request.Key, out var key)) {
            return StorageResult.Failure(InvalidKey(request.BucketName, request.Key));
        }

        await using var transaction = await BeginAsync(write: true, cancellationToken);
        await transaction.LockBucketAsync(request.BucketName, exclusive: false, cancellationToken);
        if (await transaction.GetBucketAsync(request.BucketName, cancellationToken) is not { } bucket) {
            return StorageResult.Failure(BucketNotFound(request.BucketName));
        }

        var upload = await transaction.GetUploadAsync(bucket.Id, request.UploadId, exclusive: true, cancellationToken);
        if (upload is null || !upload.Key.AsSpan().SequenceEqual(key)) {
            return StorageResult.Failure(NoSuchUpload(request.BucketName, request.Key, request.UploadId));
        }

        var now = await transaction.GetClockAsync(cancellationToken);
        var parts = await transaction.ListPartsAsync(upload.Id, 0, MaxPartNumber, cancellationToken);
        await transaction.DeleteUploadAsync(upload.Id, cancellationToken);
        await transaction.EnqueueGarbageAsync(parts.SelectMany(static part => part.Manifest.Select(static extent => extent.Locator)), GarbageNotBefore(now), cancellationToken);
        await transaction.CommitAsync();
        return StorageResult.Success();
    }

    public async IAsyncEnumerable<MultipartUploadInfo> ListMultipartUploadsAsync(ListMultipartUploadsRequest request, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.PageSize is <= 0) {
            throw new ArgumentException("Page size must be greater than zero.", nameof(request));
        }

        var prefix = KeyRange.Encode(request.Prefix ?? string.Empty);
        var afterKey = string.IsNullOrEmpty(request.KeyMarker) ? null : KeyRange.Encode(request.KeyMarker);
        var afterUploadId = afterKey is null || string.IsNullOrEmpty(request.UploadIdMarker) ? null : request.UploadIdMarker;
        var remaining = request.PageSize ?? int.MaxValue;
        while (remaining > 0) {
            List<UploadRow> batch;
            await using (var transaction = await BeginAsync(write: false, cancellationToken)) {
                if (await transaction.GetBucketAsync(request.BucketName, cancellationToken) is not { } bucket) {
                    yield break;
                }

                batch = await transaction.ListUploadsAsync(bucket.Id, prefix, afterKey, afterUploadId, Math.Min(remaining, ListBatchSize), cancellationToken);
            }

            foreach (var upload in batch) {
                yield return ToUploadInfo(request.BucketName, KeyRange.Decode(upload.Key), upload);
            }

            remaining -= batch.Count;
            if (batch.Count < ListBatchSize || remaining <= 0) {
                yield break;
            }

            afterKey = batch[^1].Key;
            afterUploadId = batch[^1].UploadId;
        }
    }

    public async IAsyncEnumerable<MultipartUploadPart> ListMultipartUploadPartsAsync(ListMultipartUploadPartsRequest request, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.PageSize is <= 0) {
            throw new ArgumentException("Page size must be greater than zero.", nameof(request));
        }

        if (request.PartNumberMarker < 0) {
            throw new ArgumentException("Part number marker must be zero or greater.", nameof(request));
        }

        if (!TryEncodeKey(request.Key, out var key)) {
            yield break;
        }

        List<PartRow> parts;
        string? uploadAlgorithm;
        await using (var transaction = await BeginAsync(write: false, cancellationToken)) {
            if (await transaction.GetBucketAsync(request.BucketName, cancellationToken) is not { } bucket
                || await transaction.GetUploadAsync(bucket.Id, request.UploadId, exclusive: false, cancellationToken) is not { } upload
                || !upload.Key.AsSpan().SequenceEqual(key)) {
                yield break;
            }

            uploadAlgorithm = upload.Meta.ChecksumAlgorithm;
            parts = await transaction.ListPartsAsync(upload.Id, request.PartNumberMarker ?? 0, request.PageSize ?? MaxPartNumber, cancellationToken);
        }

        foreach (var part in parts) {
            yield return new MultipartUploadPart
            {
                PartNumber = part.PartNumber,
                ETag = part.ETag,
                ContentLength = part.Size,
                LastModifiedUtc = EngineClock.FromMicroseconds(part.LastModifiedUs),
                Checksums = CreateMultipartPartResponseChecksums(part.Meta.Checksums, uploadAlgorithm, requestedChecksumAlgorithm: null, requestedChecksums: null),
                CopySourceVersionId = part.Meta.CopySourceVersionId
            };
        }
    }

    // ----- Helpers -----

    private async ValueTask<StorageResult<UploadRow>> FindUploadAsync(string bucketName, string keyText, string uploadId, CancellationToken cancellationToken)
    {
        if (!TryEncodeKey(keyText, out var key)) {
            return StorageResult<UploadRow>.Failure(InvalidKey(bucketName, keyText));
        }

        await using var transaction = await BeginAsync(write: false, cancellationToken);
        if (await transaction.GetBucketAsync(bucketName, cancellationToken) is not { } bucket) {
            return StorageResult<UploadRow>.Failure(BucketNotFound(bucketName));
        }

        var upload = await transaction.GetUploadAsync(bucket.Id, uploadId, exclusive: false, cancellationToken);
        return upload is not null && upload.Key.AsSpan().SequenceEqual(key)
            ? StorageResult<UploadRow>.Success(upload)
            : StorageResult<UploadRow>.Failure(NoSuchUpload(bucketName, keyText, uploadId));
    }

    /// <summary>
    /// Records a part under the upload's shared lock, replacing a part with the same number, whose blobs are freed.
    /// The upload must still exist: a part that arrives after Complete or Abort gets NoSuchUpload.
    /// </summary>
    private async ValueTask<StorageResult<MultipartUploadPart>> CommitPartAsync(
        string bucketName,
        string keyText,
        string uploadId,
        int partNumber,
        StoredBody body,
        string? uploadAlgorithm,
        string? requestAlgorithm,
        IReadOnlyDictionary<string, string>? requestedChecksums,
        string? copySourceVersionId,
        CancellationToken cancellationToken)
    {
        var etag = ObjectETags.BuildPartETag(body.Checksums);
        StorageResult<MultipartUploadPart> result;
        await using (var transaction = await BeginAsync(write: true, cancellationToken)) {
            result = await CommitPartAsync(transaction, bucketName, keyText, uploadId, partNumber, body, etag, uploadAlgorithm, requestAlgorithm,
                requestedChecksums, copySourceVersionId, cancellationToken);
        }

        if (!result.IsSuccess) {
            await BodyWriter.DeleteQuietlyAsync(_blobs, body.Locators);
        }

        return result;
    }

    private async ValueTask<StorageResult<MultipartUploadPart>> CommitPartAsync(
        MetadataTransaction transaction,
        string bucketName,
        string keyText,
        string uploadId,
        int partNumber,
        StoredBody body,
        string etag,
        string? uploadAlgorithm,
        string? requestAlgorithm,
        IReadOnlyDictionary<string, string>? requestedChecksums,
        string? copySourceVersionId,
        CancellationToken cancellationToken)
    {
        await transaction.LockBucketAsync(bucketName, exclusive: false, cancellationToken);
        if (await transaction.GetBucketAsync(bucketName, cancellationToken) is not { } bucket) {
            return StorageResult<MultipartUploadPart>.Failure(BucketNotFound(bucketName));
        }

        var upload = await transaction.GetUploadAsync(bucket.Id, uploadId, exclusive: false, cancellationToken);
        if (upload is null || !upload.Key.AsSpan().SequenceEqual(KeyRange.Encode(keyText))) {
            return StorageResult<MultipartUploadPart>.Failure(NoSuchUpload(bucketName, keyText, uploadId));
        }

        var now = await transaction.GetClockAsync(cancellationToken);
        var replaced = await transaction.UpsertPartAsync(new PartRow
        {
            UploadRowId = upload.Id,
            PartNumber = partNumber,
            Size = body.Length,
            ETag = etag,
            LastModifiedUs = now,
            Meta = new PartMeta
            {
                Checksums = new Dictionary<string, string>(body.Checksums, StringComparer.OrdinalIgnoreCase),
                CopySourceVersionId = copySourceVersionId
            },
            Manifest = body.Manifest
        }, cancellationToken);

        if (!await transaction.ReferenceBlobsAsync(body.Locators, cancellationToken)) {
            return StorageResult<MultipartUploadPart>.Failure(BlobClaimed(bucketName, keyText));
        }

        if (replaced is not null) {
            await transaction.EnqueueGarbageAsync(replaced.Manifest.Select(static extent => extent.Locator), GarbageNotBefore(now), cancellationToken);
        }

        await transaction.CommitAsync();
        return StorageResult<MultipartUploadPart>.Success(new MultipartUploadPart
        {
            PartNumber = partNumber,
            ETag = etag,
            ContentLength = body.Length,
            LastModifiedUtc = EngineClock.FromMicroseconds(now),
            Checksums = CreateMultipartPartResponseChecksums(body.Checksums, uploadAlgorithm, requestAlgorithm, requestedChecksums),
            CopySourceVersionId = copySourceVersionId
        });
    }

    /// <summary>
    /// Stores a part's body. Parts are never inline, since their manifest is what the object's manifest is built
    /// from; an empty part has an empty manifest.
    /// </summary>
    private async ValueTask<StorageResult<StoredBody>> StorePartBodyAsync(string bucketName, string key, Stream content, ChecksumAlgorithms algorithms, CancellationToken cancellationToken)
    {
        try {
            var body = await BodyWriter.WriteAsync(_blobs, content, algorithms, inlineThreshold: 0, cancellationToken);
            return StorageResult<StoredBody>.Success(body.InlineData is null
                ? body
                : new StoredBody { Length = 0, Manifest = [], Checksums = body.Checksums });
        }
        catch (Abstractions.Blobs.BlobStoreThrottledException) {
            return StorageResult<StoredBody>.Failure(Throttled(bucketName, key));
        }
    }

    private async ValueTask<StorageResult<StoredBody>> CopySourceIntoPartAsync(
        string bucketName,
        string key,
        string sourceBucketName,
        string sourceKey,
        string? sourceVersionId,
        ObjectRange? sourceRange,
        ChecksumAlgorithms algorithms,
        Func<ObjectInfo, StorageError?> evaluatePreconditions,
        CancellationToken cancellationToken)
    {
        var resolved = await ResolveVersionAsync(sourceBucketName, sourceKey, sourceVersionId, cancellationToken);
        if (!resolved.IsSuccess) {
            return StorageResult<StoredBody>.Failure(resolved.Error!.IsDeleteMarker && !string.IsNullOrWhiteSpace(sourceVersionId)
                ? MultipartInvalidRequest(
                    $"The source object version '{sourceVersionId}' cannot be used as an UploadPartCopy source because it is a delete marker.",
                    sourceBucketName, sourceKey)
                : resolved.Error!.IsDeleteMarker ? ObjectNotFound(sourceBucketName, sourceKey, sourceVersionId) : resolved.Error!);
        }

        var source = resolved.Value!;
        if (evaluatePreconditions(ToObjectInfo(sourceBucketName, sourceKey, source)) is { } preconditionFailure) {
            return StorageResult<StoredBody>.Failure(preconditionFailure);
        }

        var range = ObjectRanges.NormalizeRange(sourceRange, source.Size, sourceBucketName, sourceKey, out var rangeError);
        if (rangeError is not null) {
            return StorageResult<StoredBody>.Failure(rangeError);
        }

        var start = range?.Start ?? 0;
        var length = range is null ? source.Size : range.End!.Value - range.Start!.Value + 1;
        var content = await OpenContentAsync(sourceBucketName, sourceKey, source, start, length, cancellationToken);
        if (!content.IsSuccess) {
            return StorageResult<StoredBody>.Failure(content.Error!);
        }

        await using var stream = content.Value!;
        return await StorePartBodyAsync(bucketName, key, stream, algorithms, cancellationToken);
    }

    private static MultipartUploadInfo ToUploadInfo(string bucketName, string key, UploadRow upload) => new()
    {
        BucketName = bucketName,
        Key = key,
        UploadId = upload.UploadId,
        InitiatedAtUtc = EngineClock.FromMicroseconds(upload.InitiatedUs),
        ChecksumAlgorithm = upload.Meta.ChecksumAlgorithm
    };

    private StorageError NoSuchUpload(string bucketName, string key, string uploadId)
        => Error(StorageErrorCode.NoSuchUpload, $"Multipart upload '{uploadId}' was not found.", bucketName, key, 404);

    private StorageError MultipartInvalidRequest(string message, string bucketName, string key)
        => Error(StorageErrorCode.MultipartConflict, message, bucketName, key, 400);

    private StorageError PartNumberOutOfRange(string bucketName, string key)
        => Error(StorageErrorCode.InvalidArgument, $"Part number must be an integer between 1 and {MaxPartNumber}, inclusive.", bucketName, key, 400);
}
