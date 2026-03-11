namespace IntegratedS3.Abstractions.Errors;

public enum StorageErrorCode
{
    Unknown,
    ObjectNotFound,
    BucketNotFound,
    CorsConfigurationNotFound,
    AccessDenied,
    InvalidChecksum,
    InvalidRange,
    PreconditionFailed,
    MethodNotAllowed,
    VersionConflict,
    BucketAlreadyExists,
    MultipartConflict,
    Throttled,
    ProviderUnavailable,
    UnsupportedCapability,
    QuotaExceeded
}
