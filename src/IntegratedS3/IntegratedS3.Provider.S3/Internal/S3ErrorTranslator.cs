using System.IO;
using System.Net.Http;
using System.Net.Sockets;
using Amazon.Runtime;
using IntegratedS3.Abstractions.Errors;

namespace IntegratedS3.Provider.S3.Internal;

internal static class S3ErrorTranslator
{
    public static StorageError Translate(
        AmazonServiceException ex,
        string providerName,
        string? bucketName = null,
        string? objectKey = null)
    {
        var (code, message) = ex.ErrorCode switch
        {
            "NoSuchKey" =>
                (StorageErrorCode.ObjectNotFound,
                 $"Object '{objectKey}' does not exist in bucket '{bucketName}'."),

            "NoSuchVersion" =>
                (StorageErrorCode.ObjectNotFound,
                 $"The requested version of object '{objectKey}' does not exist in bucket '{bucketName}'."),

            "NoSuchBucket" =>
                (StorageErrorCode.BucketNotFound,
                 $"Bucket '{bucketName}' does not exist."),

            "NoSuchCORSConfiguration" =>
                (StorageErrorCode.CorsConfigurationNotFound,
                 $"Bucket '{bucketName}' does not have a CORS configuration."),

            "ServerSideEncryptionConfigurationNotFoundError" or "ServerSideEncryptionConfigurationNotFound" =>
                (StorageErrorCode.BucketEncryptionConfigurationNotFound,
                 $"Bucket '{bucketName}' does not have a default encryption configuration."),

            "NoSuchUpload" =>
                (StorageErrorCode.NoSuchUpload,
                 $"Multipart upload for object '{objectKey}' in bucket '{bucketName}' does not exist or is no longer active."),

            "BucketAlreadyExists" =>
                (StorageErrorCode.BucketAlreadyExists,
                 $"Bucket '{bucketName}' already exists (owned by another account)."),

            "BucketAlreadyOwnedByYou" =>
                (StorageErrorCode.BucketAlreadyOwnedByYou,
                 $"Your previous request to create the named bucket '{bucketName}' succeeded and you already own it."),

            "BadDigest" =>
                (StorageErrorCode.InvalidChecksum,
                 !string.IsNullOrEmpty(objectKey)
                     ? $"The supplied checksum for object '{objectKey}' in bucket '{bucketName}' did not match the received payload."
                     : ex.Message),

            "InvalidTag" =>
                (StorageErrorCode.InvalidTag,
                 !string.IsNullOrEmpty(objectKey)
                     ? $"One or more tags supplied for object '{objectKey}' in bucket '{bucketName}' were invalid."
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

            "InvalidRange" =>
                (StorageErrorCode.InvalidRange,
                 !string.IsNullOrEmpty(objectKey)
                     ? $"The requested range is invalid for object '{objectKey}' in bucket '{bucketName}'."
                     : ex.Message),

            "InvalidPart" =>
                (StorageErrorCode.InvalidPart,
                 $"One or more multipart parts for object '{objectKey}' in bucket '{bucketName}' were missing or had mismatched ETags/checksums."),

            "InvalidPartOrder" =>
                (StorageErrorCode.InvalidPartOrder,
                 $"Multipart parts for object '{objectKey}' in bucket '{bucketName}' were not supplied in ascending part-number order."),

            "InvalidArgument" =>
                (StorageErrorCode.InvalidArgument,
                 !string.IsNullOrEmpty(objectKey)
                     ? $"An argument supplied for object '{objectKey}' in bucket '{bucketName}' was invalid: {ex.Message}"
                     : ex.Message),

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

            // Any other 5xx from the upstream (including a bare AmazonServiceException with a
            // 500-class status but no recognized S3 error code) is a transient provider fault.
            _ when (int)ex.StatusCode >= 500 =>
                (StorageErrorCode.ProviderUnavailable,
                 $"S3 provider '{providerName}' is temporarily unavailable: {ex.Message}"),

            // A service exception with no HTTP status (StatusCode == 0) means the request never
            // completed against the upstream — a transport-level failure (connection reset,
            // timeout, DNS/TLS error) surfaced by the AWS SDK as AmazonServiceException.
            _ when (int)ex.StatusCode == 0 =>
                (StorageErrorCode.ProviderUnavailable,
                 $"S3 provider '{providerName}' could not be reached: {ex.Message}"),

            _ => (StorageErrorCode.Unknown, ex.Message)
        };

        return new StorageError
        {
            Code = code,
            Message = message,
            BucketName = bucketName,
            ObjectKey = objectKey,
            ProviderName = providerName,
            SuggestedHttpStatusCode = MapSuggestedStatus(code, (int)ex.StatusCode)
        };
    }

    /// <summary>
    /// Translates a raw transport-layer exception (connection reset, timeout, DNS/TLS failure)
    /// that the AWS SDK surfaced without an <see cref="AmazonServiceException"/> wrapper — for
    /// example <see cref="HttpRequestException"/>, <see cref="IOException"/>,
    /// <see cref="SocketException"/>, or a non-cancellation <see cref="TaskCanceledException"/>
    /// (request timeout) — into a retryable <see cref="StorageErrorCode.ProviderUnavailable"/>
    /// error. The originating exception is preserved in the message so it is not swallowed.
    /// </summary>
    public static StorageError TranslateTransport(
        Exception ex,
        string providerName,
        string? bucketName = null,
        string? objectKey = null)
    {
        return new StorageError
        {
            Code = StorageErrorCode.ProviderUnavailable,
            Message = $"S3 provider '{providerName}' could not be reached: {ex.Message}",
            BucketName = bucketName,
            ObjectKey = objectKey,
            ProviderName = providerName,
            SuggestedHttpStatusCode = 503
        };
    }

    /// <summary>
    /// Determines whether <paramref name="ex"/> is a raw transport-layer failure that should be
    /// translated into a retryable provider error rather than propagated untranslated. Returns
    /// <see langword="false"/> for cancellation triggered by the caller's
    /// <paramref name="cancellationToken"/> so that genuine caller cancellation is re-thrown and
    /// not misreported as a provider fault.
    /// </summary>
    public static bool IsTransportFailure(Exception ex, CancellationToken cancellationToken)
    {
        // Caller-initiated cancellation must not be swallowed or reclassified as a provider fault.
        if (ex is OperationCanceledException && cancellationToken.IsCancellationRequested)
            return false;

        return ex switch
        {
            HttpRequestException => true,
            SocketException => true,
            IOException => true,
            // A TaskCanceledException whose cancellation was NOT requested by the caller is an
            // HttpClient request timeout, i.e. a transport failure.
            TaskCanceledException => true,
            OperationCanceledException => true,
            _ => false
        };
    }

    private static int MapSuggestedStatus(StorageErrorCode code, int httpStatus)
    {
        // A recognized transient failure with no usable upstream status (0) should still map to a
        // sensible retryable HTTP status for the S3-compatible facade.
        if (httpStatus > 0)
            return httpStatus;

        return code switch
        {
            StorageErrorCode.Throttled => 429,
            StorageErrorCode.ProviderUnavailable => 503,
            _ => 500
        };
    }
}
