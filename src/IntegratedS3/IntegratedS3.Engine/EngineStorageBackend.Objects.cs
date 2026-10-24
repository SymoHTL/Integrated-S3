using System.Runtime.CompilerServices;
using IntegratedS3.Abstractions.Blobs;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Engine.Blobs;
using IntegratedS3.Engine.Metadata;
using IntegratedS3.Shared;
using Microsoft.Extensions.Logging;
using static IntegratedS3.Shared.ObjectChecksums;

namespace IntegratedS3.Engine;

internal sealed partial class EngineStorageBackend
{
    private const int ListBatchSize = 1000;

    // ----- Reads -----

    public async ValueTask<StorageResult<ObjectInfo>> HeadObjectAsync(HeadObjectRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (UnsupportedEncryption(request.ServerSideEncryption, request.CustomerEncryption, request.BucketName, request.Key, "object metadata lookups") is { } encryptionError) {
            return StorageResult<ObjectInfo>.Failure(encryptionError);
        }

        var resolved = await ResolveVersionAsync(request.BucketName, request.Key, request.VersionId, cancellationToken);
        return resolved.IsSuccess
            ? StorageResult<ObjectInfo>.Success(ToObjectInfo(request.BucketName, request.Key, resolved.Value!))
            : StorageResult<ObjectInfo>.Failure(resolved.Error!);
    }

    public async ValueTask<StorageResult<GetObjectResponse>> GetObjectAsync(GetObjectRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (UnsupportedEncryption(request.ServerSideEncryption, request.CustomerEncryption, request.BucketName, request.Key, "object retrieval") is { } encryptionError) {
            return StorageResult<GetObjectResponse>.Failure(encryptionError);
        }

        var resolved = await ResolveVersionAsync(request.BucketName, request.Key, request.VersionId, cancellationToken);
        if (!resolved.IsSuccess) {
            return StorageResult<GetObjectResponse>.Failure(resolved.Error!);
        }

        var version = resolved.Value!;
        var objectInfo = ToObjectInfo(request.BucketName, request.Key, version);
        if (ObjectPreconditions.EvaluatePreconditions(request, objectInfo) is { } preconditionFailure) {
            return StorageResult<GetObjectResponse>.Failure(preconditionFailure);
        }

        if (ObjectPreconditions.IsNotModified(request, objectInfo)) {
            return StorageResult<GetObjectResponse>.Success(new GetObjectResponse
            {
                Object = objectInfo,
                Content = Stream.Null,
                TotalContentLength = objectInfo.ContentLength,
                IsNotModified = true
            });
        }

        var range = ObjectRanges.NormalizeRange(request.Range, version.Size, request.BucketName, request.Key, out var rangeError);
        if (rangeError is not null) {
            return StorageResult<GetObjectResponse>.Failure(rangeError);
        }

        var start = range?.Start ?? 0;
        var length = range is null ? version.Size : range.End!.Value - range.Start!.Value + 1;
        var content = await OpenContentAsync(request.BucketName, request.Key, version, start, length, cancellationToken);
        if (!content.IsSuccess) {
            return StorageResult<GetObjectResponse>.Failure(content.Error!);
        }

        return StorageResult<GetObjectResponse>.Success(new GetObjectResponse
        {
            Object = range is null ? objectInfo : ToObjectInfo(request.BucketName, request.Key, version, contentLength: length),
            Content = content.Value!,
            TotalContentLength = version.Size,
            Range = range
        });
    }

    public async ValueTask<StorageResult<GetObjectAttributesResponse>> GetObjectAttributesAsync(GetObjectAttributesRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        var resolved = await ResolveVersionAsync(request.BucketName, request.Key, request.VersionId, cancellationToken);
        if (!resolved.IsSuccess) {
            return StorageResult<GetObjectAttributesResponse>.Failure(resolved.Error!);
        }

        var info = ToObjectInfo(request.BucketName, request.Key, resolved.Value!);
        bool Wants(string attribute) => request.ObjectAttributes.Any(requested => string.Equals(requested, attribute, StringComparison.OrdinalIgnoreCase));

        // Like AWS without MaxParts: the part count from the multipart ETag, and no part list.
        ObjectPartsInfo? parts = Wants("ObjectParts") && ObjectETags.TryGetMultipartPartCount(info.ETag, out var partCount)
            ? new ObjectPartsInfo { TotalPartsCount = partCount, IsTruncated = false }
            : null;

        return StorageResult<GetObjectAttributesResponse>.Success(new GetObjectAttributesResponse
        {
            VersionId = info.VersionId,
            IsDeleteMarker = info.IsDeleteMarker,
            LastModifiedUtc = info.LastModifiedUtc,
            ETag = Wants("ETag") ? info.ETag : null,
            ObjectSize = Wants("ObjectSize") ? info.ContentLength : null,
            StorageClass = Wants("StorageClass") ? StorageClass.NormalizeForEcho(info.StorageClass) : null,
            Checksums = Wants("Checksum") ? info.Checksums : null,
            ObjectParts = parts
        });
    }

    public async ValueTask<StorageResult<ObjectTagSet>> GetObjectTagsAsync(GetObjectTagsRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        var resolved = await ResolveVersionAsync(request.BucketName, request.Key, request.VersionId, cancellationToken, deleteMarkerIsNotFound: true);
        if (!resolved.IsSuccess) {
            return StorageResult<ObjectTagSet>.Failure(resolved.Error!);
        }

        var version = resolved.Value!;
        return StorageResult<ObjectTagSet>.Success(new ObjectTagSet
        {
            BucketName = request.BucketName,
            Key = request.Key,
            VersionId = ToApiVersionId(version.VersionId),
            Tags = version.Meta.Tags is { } tags ? new Dictionary<string, string>(tags, StringComparer.Ordinal) : new Dictionary<string, string>(StringComparer.Ordinal)
        });
    }

    public async IAsyncEnumerable<ObjectInfo> ListObjectsAsync(ListObjectsRequest request, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.PageSize is <= 0) {
            throw new ArgumentException("Page size must be greater than zero.", nameof(request));
        }

        var prefix = KeyRange.Encode(request.Prefix ?? string.Empty);
        var after = string.IsNullOrWhiteSpace(request.ContinuationToken) ? null : KeyRange.Encode(request.ContinuationToken);
        var remaining = request.PageSize ?? int.MaxValue;
        while (remaining > 0) {
            List<VersionRow> batch;
            await using (var transaction = await BeginAsync(write: false, cancellationToken)) {
                if (await transaction.GetBucketAsync(request.BucketName, cancellationToken) is not { } bucket) {
                    yield break;
                }

                batch = await transaction.ListCurrentAsync(bucket.Id, prefix, after, Math.Min(remaining, ListBatchSize), cancellationToken);
            }

            foreach (var version in batch) {
                yield return ToObjectInfo(request.BucketName, KeyRange.Decode(version.Key), version);
            }

            remaining -= batch.Count;
            if (batch.Count < ListBatchSize || remaining <= 0) {
                break;
            }

            after = batch[^1].Key;
        }

        if (!request.IncludeVersions || remaining <= 0) {
            yield break;
        }

        // The noncurrent versions and delete markers follow the current objects, as the Disk provider lists them.
        await foreach (var version in ListObjectVersionsAsync(new ListObjectVersionsRequest
        {
            BucketName = request.BucketName,
            Prefix = request.Prefix,
            KeyMarker = request.ContinuationToken
        }, cancellationToken)) {
            if (version.IsLatest && !version.IsDeleteMarker) {
                continue;
            }

            yield return version;
            if (--remaining <= 0) {
                yield break;
            }
        }
    }

    public async IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(ListObjectVersionsRequest request, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.PageSize is <= 0) {
            throw new ArgumentException("Page size must be greater than zero.", nameof(request));
        }

        var prefix = KeyRange.Encode(request.Prefix ?? string.Empty);
        var afterKey = string.IsNullOrEmpty(request.KeyMarker) ? null : KeyRange.Encode(request.KeyMarker);
        long? afterSeq = null;
        var resolveMarker = afterKey is not null && !string.IsNullOrEmpty(request.VersionIdMarker);
        var remaining = request.PageSize ?? int.MaxValue;
        while (remaining > 0) {
            List<VersionRow> batch;
            await using (var transaction = await BeginAsync(write: false, cancellationToken)) {
                if (await transaction.GetBucketAsync(request.BucketName, cancellationToken) is not { } bucket) {
                    yield break;
                }

                if (resolveMarker) {
                    afterSeq = await ResolveVersionMarkerAsync(transaction, bucket.Id, afterKey!, request.VersionIdMarker!, cancellationToken);
                    resolveMarker = false;
                }

                batch = await transaction.ListVersionsAsync(bucket.Id, prefix, afterKey, afterSeq, Math.Min(remaining, ListBatchSize), cancellationToken);
            }

            foreach (var version in batch) {
                yield return ToObjectInfo(request.BucketName, KeyRange.Decode(version.Key), version);
            }

            remaining -= batch.Count;
            if (batch.Count < ListBatchSize || remaining <= 0) {
                yield break;
            }

            afterKey = batch[^1].Key;
            afterSeq = batch[^1].Seq;
        }
    }

    // ----- Writes -----

    public async ValueTask<StorageResult<ObjectInfo>> PutObjectAsync(PutObjectRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Content);
        if (UnsupportedEncryption(request.ServerSideEncryption, request.CustomerEncryption, request.BucketName, request.Key, "object writes") is { } encryptionError) {
            return StorageResult<ObjectInfo>.Failure(encryptionError);
        }

        if (ObjectTagValidation.Validate(request.Tags) is { } tagError) {
            return StorageResult<ObjectInfo>.Failure(InvalidTag(tagError, request.BucketName, request.Key));
        }

        if (!TryEncodeKey(request.Key, out var key)) {
            return StorageResult<ObjectInfo>.Failure(InvalidKey(request.BucketName, request.Key));
        }

        var conditions = new WriteConditions(request.IfMatchETag, request.IfNoneMatchETag, request.OverwriteIfExists);

        // Fail fast before the body is read; the check that counts runs again under the key's lock.
        if (await CheckWriteEarlyAsync(request.BucketName, request.Key, key, conditions, cancellationToken) is { } earlyError) {
            return StorageResult<ObjectInfo>.Failure(earlyError);
        }

        var stored = await StoreBodyAsync(request.BucketName, request.Key, request.Content,
            DetermineRequiredChecksumAlgorithms(request.Checksums, computeAllWhenNoneRequested: true), cancellationToken);
        if (!stored.IsSuccess) {
            return StorageResult<ObjectInfo>.Failure(stored.Error!);
        }

        var body = stored.Value!;
        if (ValidateRequestedChecksums(request.Checksums, body.Checksums, request.BucketName, request.Key, Name) is { } checksumError) {
            await BodyWriter.DeleteQuietlyAsync(_blobs, body.Locators);
            return StorageResult<ObjectInfo>.Failure(checksumError);
        }

        var meta = new VersionMeta
        {
            ContentType = string.IsNullOrWhiteSpace(request.ContentType) ? DefaultObjectContentType : request.ContentType,
            CacheControl = request.CacheControl,
            ContentDisposition = request.ContentDisposition,
            ContentEncoding = request.ContentEncoding,
            ContentLanguage = request.ContentLanguage,
            ExpiresUtc = request.ExpiresUtc,
            Expires = request.Expires,
            Metadata = CopyOrNull(request.Metadata),
            Tags = CopyOrNull(request.Tags),
            Checksums = new Dictionary<string, string>(CreatePutObjectChecksums(body.Checksums, request.Checksums), StringComparer.OrdinalIgnoreCase)
        };

        return await CommitNewVersionAsync(request.BucketName, request.Key, key, body, ObjectETags.BuildPartETag(body.Checksums), meta,
            NormalizeStoredStorageClass(request.StorageClass), conditions, cancellationToken);
    }

    public async ValueTask<StorageResult<ObjectInfo>> CopyObjectAsync(CopyObjectRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (UnsupportedEncryption(request.SourceServerSideEncryption, request.SourceCustomerEncryption, request.SourceBucketName, request.SourceKey, "copy source requests") is { } sourceEncryptionError) {
            return StorageResult<ObjectInfo>.Failure(sourceEncryptionError);
        }

        if (UnsupportedEncryption(request.DestinationServerSideEncryption, request.DestinationCustomerEncryption, request.DestinationBucketName, request.DestinationKey, "copy destination requests") is { } destinationEncryptionError) {
            return StorageResult<ObjectInfo>.Failure(destinationEncryptionError);
        }

        if (request.TaggingDirective == ObjectTaggingDirective.Replace && ObjectTagValidation.Validate(request.Tags) is { } tagError) {
            return StorageResult<ObjectInfo>.Failure(InvalidTag(tagError, request.DestinationBucketName, request.DestinationKey));
        }

        if (!TryNormalizeChecksumAlgorithm(request.ChecksumAlgorithm, out var checksumAlgorithm)) {
            return StorageResult<ObjectInfo>.Failure(StorageError.Unsupported(
                $"Checksum algorithm '{request.ChecksumAlgorithm}' is not currently supported for copy operations.",
                request.DestinationBucketName,
                request.DestinationKey));
        }

        if (!TryEncodeKey(request.DestinationKey, out var destinationKey)) {
            return StorageResult<ObjectInfo>.Failure(InvalidKey(request.DestinationBucketName, request.DestinationKey));
        }

        var resolved = await ResolveVersionAsync(request.SourceBucketName, request.SourceKey, request.SourceVersionId, cancellationToken);
        if (!resolved.IsSuccess) {
            return StorageResult<ObjectInfo>.Failure(resolved.Error!);
        }

        var source = resolved.Value!;
        if (ObjectPreconditions.EvaluateCopyPreconditions(request, ToObjectInfo(request.SourceBucketName, request.SourceKey, source)) is { } preconditionFailure) {
            return StorageResult<ObjectInfo>.Failure(preconditionFailure);
        }

        var conditions = new WriteConditions(IfMatch: null, IfNoneMatch: null, request.OverwriteIfExists);
        if (await CheckWriteEarlyAsync(request.DestinationBucketName, request.DestinationKey, destinationKey, conditions, cancellationToken) is { } earlyError) {
            return StorageResult<ObjectInfo>.Failure(earlyError);
        }

        var requiresActualChecksums = checksumAlgorithm is not null || source.Meta.Checksums is null || request.Checksums is { Count: > 0 };
        var algorithms = requiresActualChecksums
            ? DetermineRequiredChecksumAlgorithms(request.Checksums, checksumAlgorithm, computeAllWhenNoneRequested: true)
            : ChecksumAlgorithms.None;

        var sourceContent = await OpenContentAsync(request.SourceBucketName, request.SourceKey, source, 0, source.Size, cancellationToken);
        if (!sourceContent.IsSuccess) {
            return StorageResult<ObjectInfo>.Failure(sourceContent.Error!);
        }

        StorageResult<StoredBody> stored;
        await using (var sourceStream = sourceContent.Value!) {
            stored = await StoreBodyAsync(request.DestinationBucketName, request.DestinationKey, sourceStream, algorithms, cancellationToken);
        }

        if (!stored.IsSuccess) {
            return StorageResult<ObjectInfo>.Failure(stored.Error!);
        }

        var body = stored.Value!;
        if (requiresActualChecksums
            && ValidateRequestedChecksums(request.Checksums, body.Checksums, request.DestinationBucketName, request.DestinationKey, Name) is { } checksumError) {
            await BodyWriter.DeleteQuietlyAsync(_blobs, body.Locators);
            return StorageResult<ObjectInfo>.Failure(checksumError);
        }

        var checksums = CreateCopyObjectChecksums(requiresActualChecksums ? body.Checksums : null, source.Meta.Checksums, checksumAlgorithm);
        var replace = request.MetadataDirective == CopyObjectMetadataDirective.Replace;
        var sourceMeta = source.Meta;
        var meta = new VersionMeta
        {
            ContentType = replace
                ? string.IsNullOrWhiteSpace(request.ContentType) ? DefaultObjectContentType : request.ContentType
                : sourceMeta.ContentType,
            CacheControl = replace ? request.CacheControl : sourceMeta.CacheControl,
            ContentDisposition = replace ? request.ContentDisposition : sourceMeta.ContentDisposition,
            ContentEncoding = replace ? request.ContentEncoding : sourceMeta.ContentEncoding,
            ContentLanguage = replace ? request.ContentLanguage : sourceMeta.ContentLanguage,
            ExpiresUtc = replace ? request.ExpiresUtc : sourceMeta.ExpiresUtc,
            Expires = replace ? request.Expires : sourceMeta.Expires,
            Metadata = replace ? CopyOrNull(request.Metadata) : sourceMeta.Metadata,
            Tags = request.TaggingDirective == ObjectTaggingDirective.Replace ? CopyOrNull(request.Tags) : sourceMeta.Tags,
            Checksums = checksums is null ? null : new Dictionary<string, string>(checksums, StringComparer.OrdinalIgnoreCase)
        };

        // AWS applies the request's storage class to the copy (STANDARD when absent), not the source's.
        return await CommitNewVersionAsync(request.DestinationBucketName, request.DestinationKey, destinationKey, body,
            ObjectETags.BuildPartETag(body.Checksums), meta, NormalizeStoredStorageClass(request.StorageClass), conditions, cancellationToken);
    }

    public ValueTask<StorageResult<ObjectTagSet>> PutObjectTagsAsync(PutObjectTagsRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return UpdateObjectTagsAsync(request.BucketName, request.Key, request.VersionId, request.Tags, cancellationToken);
    }

    public ValueTask<StorageResult<ObjectTagSet>> DeleteObjectTagsAsync(DeleteObjectTagsRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return UpdateObjectTagsAsync(request.BucketName, request.Key, request.VersionId, tags: null, cancellationToken);
    }

    public async ValueTask<StorageResult<DeleteObjectResult>> DeleteObjectAsync(DeleteObjectRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (!TryEncodeKey(request.Key, out var key)) {
            return StorageResult<DeleteObjectResult>.Failure(InvalidKey(request.BucketName, request.Key));
        }

        await using var transaction = await BeginAsync(write: true, cancellationToken);
        await transaction.LockBucketAsync(request.BucketName, exclusive: false, cancellationToken);
        if (await transaction.GetBucketAsync(request.BucketName, cancellationToken) is not { } bucket) {
            return StorageResult<DeleteObjectResult>.Failure(BucketNotFound(request.BucketName));
        }

        var head = await transaction.LockKeyAsync(bucket.Id, key, cancellationToken);
        var now = await transaction.GetClockAsync(cancellationToken);

        if (!string.IsNullOrWhiteSpace(request.VersionId)) {
            if (await transaction.GetVersionAsync(bucket.Id, key, ToStoredVersionId(request.VersionId), cancellationToken) is not { } target) {
                return StorageResult<DeleteObjectResult>.Failure(ObjectNotFound(request.BucketName, request.Key, request.VersionId));
            }

            var promoted = await RemoveVersionAsync(transaction, bucket, key, head, target, now, cancellationToken);
            await transaction.CommitAsync();
            return StorageResult<DeleteObjectResult>.Success(new DeleteObjectResult
            {
                BucketName = request.BucketName,
                Key = request.Key,
                VersionId = request.VersionId,
                IsDeleteMarker = target.IsDeleteMarker,
                CurrentObject = head.CurrentSeq == target.Seq && promoted is not null
                    ? ToObjectInfo(request.BucketName, request.Key, promoted)
                    : null
            });
        }

        var current = head.CurrentSeq is null ? null : await transaction.GetCurrentVersionAsync(bucket.Id, key, cancellationToken);
        if (bucket.Versioning != StoredVersioning.Disabled && !request.BypassDeleteMarkerCreation) {
            // Enabled: a new delete marker. Suspended: a delete marker that is the null version, replacing it.
            var seq = head.Seq + 1;
            var versionId = bucket.Versioning == StoredVersioning.Enabled ? EngineVersionIds.Create(seq) : VersionRow.NullVersionId;
            var garbage = versionId == VersionRow.NullVersionId
                ? await RemoveNullVersionAsync(transaction, bucket.Id, key, cancellationToken)
                : [];
            var marker = new VersionRow
            {
                BucketId = bucket.Id,
                Key = key,
                Seq = seq,
                VersionId = versionId,
                IsDeleteMarker = true,
                IsLatest = true,
                LastModifiedUs = Math.Max(now, current?.LastModifiedUs ?? 0),
                Meta = new VersionMeta()
            };
            await transaction.InsertVersionAsync(marker, cancellationToken);
            await transaction.SetHeadAsync(bucket.Id, key, seq, seq, live: false, cancellationToken);
            await transaction.EnqueueGarbageAsync(garbage, GarbageNotBefore(now), cancellationToken);
            await transaction.CommitAsync();
            var markerInfo = ToObjectInfo(request.BucketName, request.Key, marker);
            return StorageResult<DeleteObjectResult>.Success(new DeleteObjectResult
            {
                BucketName = request.BucketName,
                Key = request.Key,
                VersionId = markerInfo.VersionId,
                IsDeleteMarker = true,
                CurrentObject = markerInfo
            });
        }

        // Unversioned, or a versioned delete that bypasses the marker: the current version goes for good.
        if (current is not null) {
            await RemoveVersionAsync(transaction, bucket, key, head, current, now, cancellationToken);
            await transaction.CommitAsync();
        }

        return StorageResult<DeleteObjectResult>.Success(new DeleteObjectResult
        {
            BucketName = request.BucketName,
            Key = request.Key
        });
    }

    // ----- Helpers -----

    private readonly record struct WriteConditions(string? IfMatch, string? IfNoneMatch, bool OverwriteIfExists);

    /// <summary>
    /// Commits a new current version of a key: the shape every object write shares. Under the bucket's and the
    /// key's locks it checks the bucket, evaluates the conditions against the key's current version, replaces the
    /// null version when the bucket does not keep versions, and records the body's blobs. On failure nothing is
    /// committed and the body's blobs are deleted.
    /// </summary>
    private async ValueTask<StorageResult<ObjectInfo>> CommitNewVersionAsync(
        string bucketName,
        string keyText,
        byte[] key,
        StoredBody body,
        string? etag,
        VersionMeta meta,
        string? storageClass,
        WriteConditions conditions,
        CancellationToken cancellationToken)
    {
        StorageResult<ObjectInfo> result;
        await using (var transaction = await BeginAsync(write: true, cancellationToken)) {
            result = await CommitNewVersionAsync(transaction, bucketName, keyText, key, body, etag, meta, storageClass, conditions, cancellationToken);
        }

        if (!result.IsSuccess) {
            // Rolled back: no committed row references the body. (An exception from the commit itself leaves the
            // blobs to the orphan sweep, since the commit may have succeeded.)
            await BodyWriter.DeleteQuietlyAsync(_blobs, body.Locators);
        }

        return result;
    }

    private async ValueTask<StorageResult<ObjectInfo>> CommitNewVersionAsync(
        MetadataTransaction transaction,
        string bucketName,
        string keyText,
        byte[] key,
        StoredBody body,
        string? etag,
        VersionMeta meta,
        string? storageClass,
        WriteConditions conditions,
        CancellationToken cancellationToken,
        UploadRow? completeUpload = null,
        IEnumerable<PartRow>? unlistedParts = null)
    {
        await transaction.LockBucketAsync(bucketName, exclusive: false, cancellationToken);
        if (await transaction.GetBucketAsync(bucketName, cancellationToken) is not { } bucket) {
            return StorageResult<ObjectInfo>.Failure(BucketNotFound(bucketName));
        }

        var head = await transaction.LockKeyAsync(bucket.Id, key, cancellationToken);
        var current = head.CurrentSeq is null ? null : await transaction.GetCurrentVersionAsync(bucket.Id, key, cancellationToken);
        if (EvaluateWriteConditions(bucketName, keyText, current, conditions) is { } conditionError) {
            return StorageResult<ObjectInfo>.Failure(conditionError);
        }

        var now = await transaction.GetClockAsync(cancellationToken);
        var seq = head.Seq + 1;
        var versionId = bucket.Versioning == StoredVersioning.Enabled ? EngineVersionIds.Create(seq) : VersionRow.NullVersionId;
        var garbage = versionId == VersionRow.NullVersionId
            ? await RemoveNullVersionAsync(transaction, bucket.Id, key, cancellationToken)
            : [];

        var version = new VersionRow
        {
            BucketId = bucket.Id,
            Key = key,
            Seq = seq,
            VersionId = versionId,
            IsLatest = true,
            Size = body.Length,
            ETag = etag,
            // Never earlier than the version it supersedes, so a newer version never looks older.
            LastModifiedUs = Math.Max(now, current?.LastModifiedUs ?? 0),
            StorageClass = storageClass,
            Meta = meta,
            InlineData = body.InlineData,
            Manifest = body.InlineData is null ? body.Manifest : null
        };
        await transaction.InsertVersionAsync(version, cancellationToken);
        await transaction.SetHeadAsync(bucket.Id, key, seq, seq, live: true, cancellationToken);
        if (!await transaction.ReferenceBlobsAsync(body.Locators, cancellationToken)) {
            return StorageResult<ObjectInfo>.Failure(BlobClaimed(bucketName, keyText));
        }

        await transaction.EnqueueGarbageAsync(garbage, GarbageNotBefore(now), cancellationToken);
        if (completeUpload is not null) {
            // The listed parts' blobs now belong to the version; the parts uploaded but not listed are freed.
            await transaction.DeleteUploadAsync(completeUpload.Id, cancellationToken);
            await transaction.EnqueueGarbageAsync(
                (unlistedParts ?? []).SelectMany(static part => part.Manifest.Select(static extent => extent.Locator)),
                GarbageNotBefore(now),
                cancellationToken);
        }

        await transaction.CommitAsync();
        return StorageResult<ObjectInfo>.Success(ToObjectInfo(bucketName, keyText, version));
    }

    /// <summary>
    /// Deletes the key's null version, wherever it is in the key's history, and returns its blobs for garbage
    /// collection. A bucket that does not keep versions (never enabled, or suspended) has one null version per key.
    /// </summary>
    private static async ValueTask<List<string>> RemoveNullVersionAsync(MetadataTransaction transaction, long bucketId, byte[] key, CancellationToken cancellationToken)
    {
        if (await transaction.GetVersionAsync(bucketId, key, VersionRow.NullVersionId, cancellationToken) is not { } existing) {
            return [];
        }

        await transaction.DeleteVersionAsync(bucketId, key, existing.Seq, cancellationToken);
        return Locators(existing);
    }

    /// <summary>
    /// Removes one version and queues its blobs. When it was current, the newest remaining version becomes current,
    /// and the key's row goes when none remains. Returns the new current version, if any.
    /// </summary>
    private async ValueTask<VersionRow?> RemoveVersionAsync(MetadataTransaction transaction, BucketRow bucket, byte[] key, HeadRow head, VersionRow target, long now, CancellationToken cancellationToken)
    {
        await transaction.DeleteVersionAsync(bucket.Id, key, target.Seq, cancellationToken);
        await transaction.EnqueueGarbageAsync(Locators(target), GarbageNotBefore(now), cancellationToken);
        if (head.CurrentSeq != target.Seq) {
            return null;
        }

        var newest = await transaction.GetNewestVersionAsync(bucket.Id, key, cancellationToken);
        if (newest is null) {
            await transaction.DeleteHeadAsync(bucket.Id, key, cancellationToken);
            return null;
        }

        await transaction.SetHeadAsync(bucket.Id, key, head.Seq, newest.Seq, live: !newest.IsDeleteMarker, cancellationToken);
        return new VersionRow
        {
            BucketId = newest.BucketId,
            Key = newest.Key,
            Seq = newest.Seq,
            VersionId = newest.VersionId,
            IsDeleteMarker = newest.IsDeleteMarker,
            IsLatest = true,
            Size = newest.Size,
            ETag = newest.ETag,
            LastModifiedUs = newest.LastModifiedUs,
            StorageClass = newest.StorageClass,
            RetainUntilUs = newest.RetainUntilUs,
            LegalHold = newest.LegalHold,
            Meta = newest.Meta
        };
    }

    private StorageError? EvaluateWriteConditions(string bucketName, string key, VersionRow? current, WriteConditions conditions)
    {
        var exists = current is { IsDeleteMarker: false };
        if (exists && conditions.IfNoneMatch?.Trim() == "*") {
            return PreconditionFailed($"Object '{key}' already exists in bucket '{bucketName}' (If-None-Match: *).", bucketName, key);
        }

        if (exists && !conditions.OverwriteIfExists) {
            return PreconditionFailed($"Object '{key}' already exists in bucket '{bucketName}'.", bucketName, key);
        }

        if (!string.IsNullOrWhiteSpace(conditions.IfMatch)) {
            // As AWS answers a conditional write: no object, or a delete marker, is 404; another ETag is 412.
            if (!exists) {
                return ObjectNotFound(bucketName, key);
            }

            if (!ObjectETags.MatchesIfMatch(conditions.IfMatch, current!.ETag)) {
                return PreconditionFailed($"Object '{key}' ETag does not match the supplied If-Match precondition.", bucketName, key);
            }
        }

        return null;
    }

    /// <summary>
    /// Checks the bucket and the write conditions on a snapshot, before a body is read. A pass proves nothing;
    /// the commit checks again under the key's lock. A failure is final.
    /// </summary>
    private async ValueTask<StorageError?> CheckWriteEarlyAsync(string bucketName, string keyText, byte[] key, WriteConditions conditions, CancellationToken cancellationToken)
    {
        await using var transaction = await BeginAsync(write: false, cancellationToken);
        if (await transaction.GetBucketAsync(bucketName, cancellationToken) is not { } bucket) {
            return BucketNotFound(bucketName);
        }

        if (conditions.IfMatch is null && conditions.IfNoneMatch is null && conditions.OverwriteIfExists) {
            return null;
        }

        var current = await transaction.GetCurrentVersionAsync(bucket.Id, key, cancellationToken);
        return EvaluateWriteConditions(bucketName, keyText, current, conditions);
    }

    private async ValueTask<StorageResult<ObjectTagSet>> UpdateObjectTagsAsync(string bucketName, string keyText, string? versionId, IReadOnlyDictionary<string, string>? tags, CancellationToken cancellationToken)
    {
        if (ObjectTagValidation.Validate(tags) is { } tagError) {
            return StorageResult<ObjectTagSet>.Failure(InvalidTag(tagError, bucketName, keyText));
        }

        if (!TryEncodeKey(keyText, out var key)) {
            return StorageResult<ObjectTagSet>.Failure(InvalidKey(bucketName, keyText));
        }

        await using var transaction = await BeginAsync(write: true, cancellationToken);
        await transaction.LockBucketAsync(bucketName, exclusive: false, cancellationToken);
        if (await transaction.GetBucketAsync(bucketName, cancellationToken) is not { } bucket) {
            return StorageResult<ObjectTagSet>.Failure(BucketNotFound(bucketName));
        }

        await transaction.LockKeyAsync(bucket.Id, key, cancellationToken);
        var version = string.IsNullOrWhiteSpace(versionId)
            ? await transaction.GetCurrentVersionAsync(bucket.Id, key, cancellationToken)
            : await transaction.GetVersionAsync(bucket.Id, key, ToStoredVersionId(versionId), cancellationToken);
        if (version is null || version.IsDeleteMarker) {
            return StorageResult<ObjectTagSet>.Failure(ObjectNotFound(bucketName, keyText, versionId));
        }

        var normalized = CopyOrNull(tags);
        await transaction.UpdateVersionMetaAsync(bucket.Id, key, version.Seq, version.Meta with { Tags = normalized }, cancellationToken);
        await transaction.CommitAsync();
        return StorageResult<ObjectTagSet>.Success(new ObjectTagSet
        {
            BucketName = bucketName,
            Key = keyText,
            VersionId = ToApiVersionId(version.VersionId),
            Tags = normalized ?? new Dictionary<string, string>(StringComparer.Ordinal)
        });
    }

    /// <summary>
    /// Resolves the version a read addresses: the current one, or the one with the given id. A delete marker is an
    /// error: 404 when current, 405 when asked for by id, or 404 either way when <paramref name="deleteMarkerIsNotFound"/>.
    /// </summary>
    private async ValueTask<StorageResult<VersionRow>> ResolveVersionAsync(string bucketName, string keyText, string? versionId, CancellationToken cancellationToken, bool deleteMarkerIsNotFound = false)
    {
        if (!TryEncodeKey(keyText, out var key)) {
            return StorageResult<VersionRow>.Failure(InvalidKey(bucketName, keyText));
        }

        VersionRow? version;
        await using (var transaction = await BeginAsync(write: false, cancellationToken)) {
            if (await transaction.GetBucketAsync(bucketName, cancellationToken) is not { } bucket) {
                return StorageResult<VersionRow>.Failure(BucketNotFound(bucketName));
            }

            version = string.IsNullOrWhiteSpace(versionId)
                ? await transaction.GetCurrentVersionAsync(bucket.Id, key, cancellationToken)
                : await transaction.GetVersionAsync(bucket.Id, key, ToStoredVersionId(versionId), cancellationToken);
        }

        if (version is null) {
            return StorageResult<VersionRow>.Failure(ObjectNotFound(bucketName, keyText, versionId));
        }

        if (version.IsDeleteMarker) {
            return StorageResult<VersionRow>.Failure(deleteMarkerIsNotFound
                ? ObjectNotFound(bucketName, keyText, versionId)
                : DeleteMarkerError(bucketName, keyText, versionId, version));
        }

        return StorageResult<VersionRow>.Success(version);
    }

    /// <summary>
    /// Finds the position of a version-id marker within its key. When the marker's version is gone, the sequence
    /// number in an engine version id still gives its place; the null version's place is lost with it, and the
    /// listing restarts at the key's newest version rather than skip any.
    /// </summary>
    private static async ValueTask<long> ResolveVersionMarkerAsync(MetadataTransaction transaction, long bucketId, byte[] key, string versionIdMarker, CancellationToken cancellationToken)
    {
        if (await transaction.GetVersionAsync(bucketId, key, ToStoredVersionId(versionIdMarker), cancellationToken) is { } marker) {
            return marker.Seq;
        }

        return EngineVersionIds.TryGetSeq(versionIdMarker, out var seq) ? seq : long.MaxValue;
    }

    /// <summary>
    /// Stores a body in the blob store, mapping the store's throttling to SlowDown.
    /// </summary>
    private async ValueTask<StorageResult<StoredBody>> StoreBodyAsync(string bucketName, string key, Stream content, ChecksumAlgorithms algorithms, CancellationToken cancellationToken)
    {
        try {
            return StorageResult<StoredBody>.Success(await BodyWriter.WriteAsync(_blobs, content, algorithms, _options.InlineThresholdBytes, cancellationToken));
        }
        catch (BlobStoreThrottledException) {
            return StorageResult<StoredBody>.Failure(Throttled(bucketName, key));
        }
    }

    /// <summary>
    /// Opens [<paramref name="start"/>, <paramref name="start"/> + <paramref name="length"/>) of a version's bytes.
    /// </summary>
    private async ValueTask<StorageResult<Stream>> OpenContentAsync(string bucketName, string key, VersionRow version, long start, long length, CancellationToken cancellationToken)
    {
        if (version.InlineData is { } inline) {
            return StorageResult<Stream>.Success(new MemoryStream(inline, (int)start, (int)length, writable: false));
        }

        try {
            return StorageResult<Stream>.Success(await ManifestReadStream.OpenAsync(_blobs, version.Manifest ?? [], start, length, cancellationToken));
        }
        catch (BlobNotFoundException exception) {
            _logger?.LogError(exception, "Engine: blob {Locator} of {BucketName}/{Key} is missing", exception.Locator, bucketName, key);
            return StorageResult<Stream>.Failure(BlobMissing(bucketName, key, exception.Locator));
        }
        catch (BlobStoreThrottledException) {
            return StorageResult<Stream>.Failure(Throttled(bucketName, key));
        }
    }

    private ObjectInfo ToObjectInfo(string bucketName, string key, VersionRow version, long? contentLength = null)
    {
        var meta = version.Meta;
        var marker = version.IsDeleteMarker;
        return new ObjectInfo
        {
            BucketName = bucketName,
            Key = key,
            VersionId = ToApiVersionId(version.VersionId),
            IsLatest = version.IsLatest,
            IsDeleteMarker = marker,
            ContentLength = marker ? 0 : contentLength ?? version.Size,
            ContentType = marker ? null : meta.ContentType ?? DefaultObjectContentType,
            CacheControl = marker ? null : meta.CacheControl,
            ContentDisposition = marker ? null : meta.ContentDisposition,
            ContentEncoding = marker ? null : meta.ContentEncoding,
            ContentLanguage = marker ? null : meta.ContentLanguage,
            ExpiresUtc = marker ? null : meta.ExpiresUtc,
            Expires = marker ? null : meta.Expires,
            ETag = version.ETag,
            LastModifiedUtc = EngineClock.FromMicroseconds(version.LastModifiedUs),
            Metadata = meta.Metadata,
            Tags = meta.Tags,
            Checksums = meta.Checksums is null ? null : new Dictionary<string, string>(meta.Checksums, StringComparer.OrdinalIgnoreCase),
            StorageClass = marker ? null : StorageClass.NormalizeForEcho(version.StorageClass)
        };
    }

    private static List<string> Locators(VersionRow version)
        => version.Manifest is null ? [] : [.. version.Manifest.Select(static extent => extent.Locator)];

    private static Dictionary<string, string>? CopyOrNull(IReadOnlyDictionary<string, string>? values)
        => values is null || values.Count == 0 ? null : new Dictionary<string, string>(values, StringComparer.Ordinal);

    private static string? NormalizeStoredStorageClass(string? storageClass)
        => StorageClass.IsNonStandard(storageClass) ? storageClass : null;

    private StorageError? UnsupportedEncryption(ObjectServerSideEncryptionSettings? serverSide, ObjectCustomerEncryptionSettings? customer, string bucketName, string key, string operation)
    {
        if (serverSide is not null) {
            return StorageError.Unsupported($"Server-side encryption is not currently supported by the engine for {operation}.", bucketName, key);
        }

        return customer is null
            ? null
            : StorageError.Unsupported($"Customer-provided encryption keys are not currently supported by the engine for {operation}.", bucketName, key);
    }

    private StorageError InvalidTag(string message, string bucketName, string key)
        => Error(StorageErrorCode.InvalidTag, message, bucketName, key, 400);

    private StorageError PreconditionFailed(string message, string bucketName, string key)
        => Error(StorageErrorCode.PreconditionFailed, message, bucketName, key, 412);
}
