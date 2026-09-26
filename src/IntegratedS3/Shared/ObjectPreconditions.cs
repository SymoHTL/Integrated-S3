using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using static IntegratedS3.Shared.ObjectETags;

namespace IntegratedS3.Shared;

/// <summary>
/// S3 conditional-request semantics for GetObject, CopyObject, UploadPartCopy and UploadPart with a copy source.
/// </summary>
internal static class ObjectPreconditions
{
    internal static StorageError? EvaluatePreconditions(GetObjectRequest request, ObjectInfo objectInfo)
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

    internal static bool IsNotModified(GetObjectRequest request, ObjectInfo objectInfo)
    {
        if (MatchesAnyETag(request.IfNoneMatchETag, objectInfo.ETag)) {
            return true;
        }

        return string.IsNullOrWhiteSpace(request.IfNoneMatchETag)
               && request.IfModifiedSinceUtc is { } ifModifiedSinceUtc
               && !WasModifiedAfter(objectInfo.LastModifiedUtc, ifModifiedSinceUtc);
    }

    internal static StorageError? EvaluateCopyPreconditions(CopyObjectRequest request, ObjectInfo sourceInfo)
    {
        return EvaluateCopyPreconditions(
            sourceInfo,
            request.SourceIfMatchETag,
            request.SourceIfNoneMatchETag,
            request.SourceIfModifiedSinceUtc,
            request.SourceIfUnmodifiedSinceUtc);
    }

    internal static StorageError? EvaluateCopyPreconditions(UploadPartCopyRequest request, ObjectInfo sourceInfo)
    {
        return EvaluateCopyPreconditions(
            sourceInfo,
            request.SourceIfMatchETag,
            request.SourceIfNoneMatchETag,
            request.SourceIfModifiedSinceUtc,
            request.SourceIfUnmodifiedSinceUtc);
    }

    private static StorageError? EvaluateCopyPreconditions(
        ObjectInfo sourceInfo,
        string? sourceIfMatchETag,
        string? sourceIfNoneMatchETag,
        DateTimeOffset? sourceIfModifiedSinceUtc,
        DateTimeOffset? sourceIfUnmodifiedSinceUtc)
    {
        if (!MatchesIfMatch(sourceIfMatchETag, sourceInfo.ETag)) {
            return new StorageError
            {
                Code = StorageErrorCode.PreconditionFailed,
                Message = $"The source object '{sourceInfo.Key}' does not match the supplied copy If-Match precondition.",
                BucketName = sourceInfo.BucketName,
                ObjectKey = sourceInfo.Key,
                SuggestedHttpStatusCode = 412
            };
        }

        if (ShouldEvaluateIfUnmodifiedSince(sourceIfMatchETag, sourceInfo.ETag)
            && sourceIfUnmodifiedSinceUtc is { } ifUnmodifiedSinceUtc
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

        if (MatchesAnyETag(sourceIfNoneMatchETag, sourceInfo.ETag)) {
            return new StorageError
            {
                Code = StorageErrorCode.PreconditionFailed,
                Message = $"The source object '{sourceInfo.Key}' matches the supplied copy If-None-Match precondition.",
                BucketName = sourceInfo.BucketName,
                ObjectKey = sourceInfo.Key,
                SuggestedHttpStatusCode = 412
            };
        }

        if (string.IsNullOrWhiteSpace(sourceIfNoneMatchETag)
            && sourceIfModifiedSinceUtc is { } ifModifiedSinceUtc
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

    private static bool WasModifiedAfter(DateTimeOffset lastModifiedUtc, DateTimeOffset comparisonUtc)
    {
        return TruncateToWholeSeconds(lastModifiedUtc) > TruncateToWholeSeconds(comparisonUtc);
    }

    internal static StorageError? EvaluateMultipartCopyPreconditions(UploadMultipartPartRequest request, ObjectInfo sourceInfo)
    {
        if (!MatchesIfMatch(request.CopySourceIfMatchETag, sourceInfo.ETag)) {
            return new StorageError
            {
                Code = StorageErrorCode.PreconditionFailed,
                Message = $"The source object '{sourceInfo.Key}' does not match the supplied copy If-Match precondition.",
                BucketName = sourceInfo.BucketName,
                ObjectKey = sourceInfo.Key,
                SuggestedHttpStatusCode = 412
            };
        }

        if (ShouldEvaluateIfUnmodifiedSince(request.CopySourceIfMatchETag, sourceInfo.ETag)
            && request.CopySourceIfUnmodifiedSinceUtc is { } ifUnmodifiedSinceUtc
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

        if (MatchesAnyETag(request.CopySourceIfNoneMatchETag, sourceInfo.ETag)) {
            return new StorageError
            {
                Code = StorageErrorCode.PreconditionFailed,
                Message = $"The source object '{sourceInfo.Key}' matched the supplied copy If-None-Match precondition.",
                BucketName = sourceInfo.BucketName,
                ObjectKey = sourceInfo.Key,
                SuggestedHttpStatusCode = 412
            };
        }

        if (string.IsNullOrWhiteSpace(request.CopySourceIfNoneMatchETag)
            && request.CopySourceIfModifiedSinceUtc is { } ifModifiedSinceUtc
            && !WasModifiedAfter(sourceInfo.LastModifiedUtc, ifModifiedSinceUtc)) {
            return new StorageError
            {
                Code = StorageErrorCode.PreconditionFailed,
                Message = $"The source object '{sourceInfo.Key}' did not satisfy the supplied copy If-Modified-Since precondition.",
                BucketName = sourceInfo.BucketName,
                ObjectKey = sourceInfo.Key,
                SuggestedHttpStatusCode = 412
            };
        }

        return null;
    }

    private static DateTimeOffset TruncateToWholeSeconds(DateTimeOffset value)
    {
        var utcValue = value.ToUniversalTime();
        return utcValue.AddTicks(-(utcValue.Ticks % TimeSpan.TicksPerSecond));
    }
}
