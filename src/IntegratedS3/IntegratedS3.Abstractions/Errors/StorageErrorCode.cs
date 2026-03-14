namespace IntegratedS3.Abstractions.Errors;

public enum StorageErrorCode
{
    Unknown,
    ObjectNotFound,
    BucketNotFound,
    CorsConfigurationNotFound,
    AccessDenied,
    InvalidTag,
    InvalidChecksum,
    InvalidRange,
    PreconditionFailed,
    MethodNotAllowed,
    VersionConflict,
    BucketAlreadyExists,
    BucketNotEmpty,
    MultipartConflict,
    Throttled,
    ProviderUnavailable,
    UnsupportedCapability,
    QuotaExceeded
}
