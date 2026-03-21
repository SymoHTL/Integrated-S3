namespace IntegratedS3.Abstractions.Errors;

/// <summary>
/// Enumerates the machine-readable error codes returned by storage operations.
/// </summary>
public enum StorageErrorCode
{
    /// <summary>An unclassified or unexpected error.</summary>
    Unknown,

    /// <summary>The requested object does not exist in the bucket.</summary>
    ObjectNotFound,

    /// <summary>The specified bucket does not exist.</summary>
    BucketNotFound,

    /// <summary>No CORS configuration exists for the bucket.</summary>
    CorsConfigurationNotFound,

    /// <summary>No default encryption configuration exists for the bucket.</summary>
    BucketEncryptionConfigurationNotFound,

    /// <summary>The caller lacks permission for the requested operation.</summary>
    AccessDenied,

    /// <summary>A tag key or value violates S3 tagging constraints.</summary>
    InvalidTag,

    /// <summary>The checksum provided does not match the computed value.</summary>
    InvalidChecksum,

    /// <summary>The requested byte range is not satisfiable for the object.</summary>
    InvalidRange,

    /// <summary>A conditional request precondition (e.g., If-Match, If-Unmodified-Since) was not met.</summary>
    PreconditionFailed,

    /// <summary>The HTTP method is not allowed on this resource.</summary>
    MethodNotAllowed,

    /// <summary>A concurrent modification was detected on the same object version.</summary>
    VersionConflict,

    /// <summary>A bucket with the specified name already exists.</summary>
    BucketAlreadyExists,

    /// <summary>The bucket cannot be deleted because it still contains objects.</summary>
    BucketNotEmpty,

    /// <summary>A multipart upload ID conflict or invalid state transition was detected.</summary>
    MultipartConflict,

    /// <summary>The request was rate-limited by the storage backend.</summary>
    Throttled,

    /// <summary>The storage backend is temporarily unreachable.</summary>
    ProviderUnavailable,

    /// <summary>The requested operation is not supported by this storage backend.</summary>
    UnsupportedCapability,

    /// <summary>A storage quota or limit has been exceeded.</summary>
    QuotaExceeded,

    /// <summary>No tagging configuration exists for the bucket.</summary>
    TaggingConfigurationNotFound,

    /// <summary>No logging configuration exists for the bucket.</summary>
    LoggingConfigurationNotFound,

    /// <summary>No website configuration exists for the bucket.</summary>
    WebsiteConfigurationNotFound,

    /// <summary>No lifecycle configuration exists for the bucket.</summary>
    LifecycleConfigurationNotFound,

    /// <summary>No replication configuration exists for the bucket.</summary>
    ReplicationConfigurationNotFound,

    /// <summary>No object-lock configuration exists for the bucket.</summary>
    ObjectLockConfigurationNotFound,

    /// <summary>No analytics configuration exists for the bucket.</summary>
    AnalyticsConfigurationNotFound,

    /// <summary>No metrics configuration exists for the bucket.</summary>
    MetricsConfigurationNotFound,

    /// <summary>No inventory configuration exists for the bucket.</summary>
    InventoryConfigurationNotFound,

    /// <summary>No intelligent-tiering configuration exists for the bucket.</summary>
    IntelligentTieringConfigurationNotFound,

    /// <summary>The object is protected by an object-lock retention policy or legal hold.</summary>
    ObjectLocked,
}
