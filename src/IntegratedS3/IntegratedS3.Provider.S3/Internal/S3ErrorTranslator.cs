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

            "BucketAlreadyExists" =>
                (StorageErrorCode.BucketAlreadyExists,
                 $"Bucket '{bucketName}' already exists (owned by another account)."),

            "BucketAlreadyOwnedByYou" =>
                (StorageErrorCode.BucketAlreadyExists,
                 $"Bucket '{bucketName}' already exists and is owned by you."),

            "AccessDenied" =>
                (StorageErrorCode.AccessDenied,
                 $"Access denied for bucket '{bucketName}': {ex.Message}"),

            "BucketNotEmpty" =>
                (StorageErrorCode.PreconditionFailed,
                 $"Bucket '{bucketName}' is not empty and cannot be deleted."),

            "PreconditionFailed" =>
                (StorageErrorCode.PreconditionFailed,
                 !string.IsNullOrEmpty(objectKey)
                     ? $"Precondition failed for object '{objectKey}' in bucket '{bucketName}'."
                     : $"Precondition failed for bucket '{bucketName}'."),

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
