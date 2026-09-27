using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Shared;

/// <summary>
/// S3 byte-range semantics: resolves a requested range against the object length, or answers InvalidRange (416).
/// </summary>
internal static class ObjectRanges
{
    internal static ObjectRange? NormalizeRange(ObjectRange? requestedRange, long contentLength, string bucketName, string objectKey, out StorageError? error)
    {
        error = null;

        if (requestedRange is null) {
            return null;
        }

        if (contentLength <= 0) {
            error = InvalidRange("Cannot satisfy a range request for an empty object.", bucketName, objectKey, contentLength);
            return null;
        }

        long start;
        long end;

        if (requestedRange.Start is null) {
            var suffixLength = requestedRange.End;
            if (suffixLength is null || suffixLength <= 0) {
                error = InvalidRange("The requested suffix range is invalid.", bucketName, objectKey, contentLength);
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
                error = InvalidRange("The requested byte range is invalid.", bucketName, objectKey, contentLength);
                return null;
            }

            if (start >= contentLength) {
                error = InvalidRange("The requested range starts beyond the end of the object.", bucketName, objectKey, contentLength);
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

    private static StorageError InvalidRange(string message, string bucketName, string objectKey, long resourceSize)
    {
        return new StorageError
        {
            Code = StorageErrorCode.InvalidRange,
            Message = message,
            BucketName = bucketName,
            ObjectKey = objectKey,
            SuggestedHttpStatusCode = 416,
            ResourceSize = resourceSize
        };
    }
}
