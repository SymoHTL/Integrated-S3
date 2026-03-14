using Amazon.S3;
using IntegratedS3.Abstractions.Errors;

namespace IntegratedS3.Provider.S3.Internal;

internal static class S3ErrorTranslator
{
    public static StorageError Translate(
        AmazonS3Exception ex,
        string providerName,
        string? bucketName = null,
        string? objectKey = null)
    {
        var (code, message) = ex.ErrorCode switch
        {
            "NoSuchKey" =>
                (StorageErrorCode.ObjectNotFound,
                 $"Object '{objectKey}' does not exist in bucket '{bucketName}'."),

            "NoSuchBucket" =>
                (StorageErrorCode.BucketNotFound,
                 $"Bucket '{bucketName}' does not exist."),

            "NoSuchCORSConfiguration" =>
                (StorageErrorCode.CorsConfigurationNotFound,
                 $"Bucket '{bucketName}' does not have a CORS configuration."),

            "NoSuchUpload" =>
                (StorageErrorCode.MultipartConflict,
                 $"Multipart upload for object '{objectKey}' in bucket '{bucketName}' does not exist or is no longer active."),

            "BucketAlreadyExists" =>
                (StorageErrorCode.BucketAlreadyExists,
                 $"Bucket '{bucketName}' already exists (owned by another account)."),

            "BucketAlreadyOwnedByYou" =>
                (StorageErrorCode.BucketAlreadyExists,
                 $"Bucket '{bucketName}' already exists and is owned by you."),

            "BadDigest" =>
                (StorageErrorCode.InvalidChecksum,
                 !string.IsNullOrEmpty(objectKey)
                     ? $"The supplied checksum for object '{objectKey}' in bucket '{bucketName}' did not match the received payload."
                     : ex.Message),

            "AccessDenied" =>
                (StorageErrorCode.AccessDenied,
                 $"Access denied for bucket '{bucketName}': {ex.Message}"),

            "BucketNotEmpty" =>
                (StorageErrorCode.BucketNotEmpty,
                 $"Bucket '{bucketName}' is not empty and cannot be deleted."),

            "PreconditionFailed" =>
                (StorageErrorCode.PreconditionFailed,
                 !string.IsNullOrEmpty(objectKey)
                     ? $"Precondition failed for object '{objectKey}' in bucket '{bucketName}'."
                     : $"Precondition failed for bucket '{bucketName}'."),

            "InvalidPart" =>
                (StorageErrorCode.MultipartConflict,
                 $"One or more multipart parts for object '{objectKey}' in bucket '{bucketName}' were missing or had mismatched ETags/checksums."),

            "InvalidPartOrder" =>
                (StorageErrorCode.MultipartConflict,
                 $"Multipart parts for object '{objectKey}' in bucket '{bucketName}' were not supplied in ascending part-number order."),

            "EntityTooSmall" =>
                (StorageErrorCode.MultipartConflict,
                 $"At least one multipart part for object '{objectKey}' in bucket '{bucketName}' was smaller than the minimum supported size."),

            "SlowDown" or "RequestThrottled" or "Throttling" =>
                (StorageErrorCode.Throttled,
                 $"S3 provider '{providerName}' is throttling requests: {ex.Message}"),

            "ServiceUnavailable" or "InternalError" =>
                (StorageErrorCode.ProviderUnavailable,
                 $"S3 provider '{providerName}' is temporarily unavailable: {ex.Message}"),

            _ when (int)ex.StatusCode == 404 && !string.IsNullOrEmpty(objectKey) =>
                (StorageErrorCode.ObjectNotFound,
                 $"Object '{objectKey}' does not exist in bucket '{bucketName}'."),

            _ when (int)ex.StatusCode == 404 =>
                (StorageErrorCode.BucketNotFound,
                 $"Bucket '{bucketName}' does not exist."),

            _ when (int)ex.StatusCode == 403 =>
                (StorageErrorCode.AccessDenied,
                 $"Access denied for bucket '{bucketName}': {ex.Message}"),

            _ when (int)ex.StatusCode == 412 =>
                (StorageErrorCode.PreconditionFailed,
                 !string.IsNullOrEmpty(objectKey)
                     ? $"Precondition failed for object '{objectKey}' in bucket '{bucketName}'."
                     : $"Precondition failed for bucket '{bucketName}'."),

            _ when (int)ex.StatusCode == 400 && string.Equals(ex.ErrorCode, "BadDigest", StringComparison.OrdinalIgnoreCase) =>
                (StorageErrorCode.InvalidChecksum,
                 !string.IsNullOrEmpty(objectKey)
                     ? $"The supplied checksum for object '{objectKey}' in bucket '{bucketName}' did not match the received payload."
                     : ex.Message),

            _ when (int)ex.StatusCode == 409 && !string.IsNullOrEmpty(objectKey) =>
                (StorageErrorCode.MultipartConflict,
                 $"A conflicting operation prevented the request for object '{objectKey}' in bucket '{bucketName}' from completing: {ex.Message}"),

            _ when (int)ex.StatusCode == 409 =>
                (StorageErrorCode.BucketAlreadyExists,
                 $"Bucket '{bucketName}' already exists."),

            _ when (int)ex.StatusCode == 503 =>
                (StorageErrorCode.ProviderUnavailable,
                 $"S3 provider '{providerName}' is temporarily unavailable: {ex.Message}"),

            _ when (int)ex.StatusCode == 429 =>
                (StorageErrorCode.Throttled,
                 $"S3 provider '{providerName}' is throttling requests: {ex.Message}"),

            _ => (StorageErrorCode.Unknown, ex.Message)
        };

        return new StorageError
        {
            Code = code,
            Message = message,
            BucketName = bucketName,
            ObjectKey = objectKey,
            ProviderName = providerName,
            SuggestedHttpStatusCode = (int)ex.StatusCode
        };
    }
}
