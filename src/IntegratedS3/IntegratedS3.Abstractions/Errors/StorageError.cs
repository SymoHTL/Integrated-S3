namespace IntegratedS3.Abstractions.Errors;

public sealed class StorageError
{
    public required StorageErrorCode Code { get; init; }

    public required string Message { get; init; }

    public string? BucketName { get; init; }

    public string? ObjectKey { get; init; }

    public string? ProviderName { get; init; }

    public int? SuggestedHttpStatusCode { get; init; }

    public static StorageError Unsupported(string message, string? bucketName = null, string? objectKey = null)
    {
        return new StorageError
        {
            Code = StorageErrorCode.UnsupportedCapability,
            Message = message,
            BucketName = bucketName,
            ObjectKey = objectKey,
            SuggestedHttpStatusCode = 501
        };
    }
}
