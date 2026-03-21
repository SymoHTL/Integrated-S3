namespace IntegratedS3.Core.Models;

/// <summary>
/// Enumerates all S3-compatible storage operations recognized by the system.
/// Used for authorization, auditing, and operation routing.
/// </summary>
public enum StorageOperationType
{
    // ── Service-level operations ────────────────────────────────────

    /// <summary>List all buckets owned by the authenticated user.</summary>
    ListBuckets,

    // ── Bucket CRUD ─────────────────────────────────────────────────

    /// <summary>Create a new bucket.</summary>
    CreateBucket,

    /// <summary>Check whether a bucket exists and is accessible.</summary>
    HeadBucket,

    /// <summary>Delete a bucket.</summary>
    DeleteBucket,

    // ── Bucket ACL &amp; policy ──────────────────────────────────────────

    /// <summary>Retrieve the access control list (ACL) of a bucket.</summary>
    GetBucketAcl,

    /// <summary>Set the access control list (ACL) of a bucket.</summary>
    PutBucketAcl,

    /// <summary>Retrieve the bucket policy document.</summary>
    GetBucketPolicy,

    /// <summary>Set the bucket policy document.</summary>
    PutBucketPolicy,

    /// <summary>Delete the bucket policy document.</summary>
    DeleteBucketPolicy,

    // ── Bucket configuration ────────────────────────────────────────

    /// <summary>Retrieve the location (region) of a bucket.</summary>
    GetBucketLocation,

    /// <summary>Retrieve the versioning configuration of a bucket.</summary>
    GetBucketVersioning,

    /// <summary>Set the versioning configuration of a bucket.</summary>
    PutBucketVersioning,

    /// <summary>Retrieve the CORS configuration of a bucket.</summary>
    GetBucketCors,

    /// <summary>Set the CORS configuration of a bucket.</summary>
    PutBucketCors,

    /// <summary>Delete the CORS configuration of a bucket.</summary>
    DeleteBucketCors,

    /// <summary>Retrieve the default server-side encryption configuration of a bucket.</summary>
    GetBucketDefaultEncryption,

    /// <summary>Set the default server-side encryption configuration of a bucket.</summary>
    PutBucketDefaultEncryption,

    /// <summary>Delete the default server-side encryption configuration of a bucket.</summary>
    DeleteBucketDefaultEncryption,

    // ── Object listing ──────────────────────────────────────────────

    /// <summary>List objects in a bucket.</summary>
    ListObjects,

    /// <summary>List object versions in a versioning-enabled bucket.</summary>
    ListObjectVersions,

    /// <summary>List in-progress multipart uploads for a bucket.</summary>
    ListMultipartUploads,

    /// <summary>List uploaded parts of a multipart upload.</summary>
    ListMultipartParts,

    // ── Object CRUD ─────────────────────────────────────────────────

    /// <summary>Download an object.</summary>
    GetObject,

    /// <summary>Retrieve metadata of an object without downloading its body.</summary>
    HeadObject,

    /// <summary>Retrieve the access control list (ACL) of an object.</summary>
    GetObjectAcl,

    /// <summary>Set the access control list (ACL) of an object.</summary>
    PutObjectAcl,

    /// <summary>Retrieve the tags associated with an object.</summary>
    GetObjectTags,

    /// <summary>Copy an object to a new location.</summary>
    CopyObject,

    /// <summary>Upload an object.</summary>
    PutObject,

    /// <summary>Delete an object (or create a delete marker in versioned buckets).</summary>
    DeleteObject,

    // ── Presigned operations ────────────────────────────────────────

    /// <summary>Generate a presigned URL for uploading an object.</summary>
    PresignPutObject,

    /// <summary>Generate a presigned URL for downloading an object.</summary>
    PresignGetObject,

    // ── Object tagging ──────────────────────────────────────────────

    /// <summary>Set tags on an object.</summary>
    PutObjectTags,

    /// <summary>Delete tags from an object.</summary>
    DeleteObjectTags,

    // ── Multipart upload ────────────────────────────────────────────

    /// <summary>Initiate a new multipart upload.</summary>
    InitiateMultipartUpload,

    /// <summary>Upload a single part of a multipart upload.</summary>
    UploadMultipartPart,

    /// <summary>Copy a part from an existing object into a multipart upload.</summary>
    UploadPartCopy,

    /// <summary>Complete a multipart upload by assembling previously uploaded parts.</summary>
    CompleteMultipartUpload,

    /// <summary>Abort a multipart upload and discard uploaded parts.</summary>
    AbortMultipartUpload,

    // ── Bucket tagging ──────────────────────────────────────────────

    /// <summary>Retrieve the tag set of a bucket.</summary>
    GetBucketTagging,

    /// <summary>Set the tag set of a bucket.</summary>
    PutBucketTagging,

    /// <summary>Delete the tag set of a bucket.</summary>
    DeleteBucketTagging,

    // ── Bucket logging ──────────────────────────────────────────────

    /// <summary>Retrieve the logging configuration of a bucket.</summary>
    GetBucketLogging,

    /// <summary>Set the logging configuration of a bucket.</summary>
    PutBucketLogging,

    // ── Bucket website ──────────────────────────────────────────────

    /// <summary>Retrieve the static website hosting configuration of a bucket.</summary>
    GetBucketWebsite,

    /// <summary>Set the static website hosting configuration of a bucket.</summary>
    PutBucketWebsite,

    /// <summary>Delete the static website hosting configuration of a bucket.</summary>
    DeleteBucketWebsite,

    // ── Bucket request payment ──────────────────────────────────────

    /// <summary>Retrieve the request-payment configuration of a bucket.</summary>
    GetBucketRequestPayment,

    /// <summary>Set the request-payment configuration of a bucket.</summary>
    PutBucketRequestPayment,

    // ── Bucket transfer acceleration ────────────────────────────────

    /// <summary>Retrieve the transfer acceleration configuration of a bucket.</summary>
    GetBucketAccelerate,

    /// <summary>Set the transfer acceleration configuration of a bucket.</summary>
    PutBucketAccelerate,

    // ── Bucket lifecycle ────────────────────────────────────────────

    /// <summary>Retrieve the lifecycle configuration of a bucket.</summary>
    GetBucketLifecycle,

    /// <summary>Set the lifecycle configuration of a bucket.</summary>
    PutBucketLifecycle,

    /// <summary>Delete the lifecycle configuration of a bucket.</summary>
    DeleteBucketLifecycle,

    // ── Bucket replication ──────────────────────────────────────────

    /// <summary>Retrieve the replication configuration of a bucket.</summary>
    GetBucketReplication,

    /// <summary>Set the replication configuration of a bucket.</summary>
    PutBucketReplication,

    /// <summary>Delete the replication configuration of a bucket.</summary>
    DeleteBucketReplication,

    // ── Bucket notifications ────────────────────────────────────────

    /// <summary>Retrieve the notification configuration of a bucket.</summary>
    GetBucketNotificationConfiguration,

    /// <summary>Set the notification configuration of a bucket.</summary>
    PutBucketNotificationConfiguration,

    // ── Object lock ─────────────────────────────────────────────────

    /// <summary>Retrieve the Object Lock configuration of a bucket.</summary>
    GetObjectLockConfiguration,

    /// <summary>Set the Object Lock configuration of a bucket.</summary>
    PutObjectLockConfiguration,

    // ── Bucket analytics ────────────────────────────────────────────

    /// <summary>Retrieve an analytics configuration for a bucket.</summary>
    GetBucketAnalyticsConfiguration,

    /// <summary>Set an analytics configuration for a bucket.</summary>
    PutBucketAnalyticsConfiguration,

    /// <summary>Delete an analytics configuration from a bucket.</summary>
    DeleteBucketAnalyticsConfiguration,

    // ── Bucket metrics ──────────────────────────────────────────────

    /// <summary>Retrieve a metrics configuration for a bucket.</summary>
    GetBucketMetricsConfiguration,

    /// <summary>Set a metrics configuration for a bucket.</summary>
    PutBucketMetricsConfiguration,

    /// <summary>Delete a metrics configuration from a bucket.</summary>
    DeleteBucketMetricsConfiguration,

    // ── Bucket inventory ────────────────────────────────────────────

    /// <summary>Retrieve an inventory configuration for a bucket.</summary>
    GetBucketInventoryConfiguration,

    /// <summary>Set an inventory configuration for a bucket.</summary>
    PutBucketInventoryConfiguration,

    /// <summary>Delete an inventory configuration from a bucket.</summary>
    DeleteBucketInventoryConfiguration,

    // ── Bucket intelligent tiering ──────────────────────────────────

    /// <summary>Retrieve an Intelligent-Tiering configuration for a bucket.</summary>
    GetBucketIntelligentTieringConfiguration,

    /// <summary>Set an Intelligent-Tiering configuration for a bucket.</summary>
    PutBucketIntelligentTieringConfiguration,

    /// <summary>Delete an Intelligent-Tiering configuration from a bucket.</summary>
    DeleteBucketIntelligentTieringConfiguration,

    // ── List configurations ─────────────────────────────────────────

    /// <summary>List all analytics configurations for a bucket.</summary>
    ListBucketAnalyticsConfigurations,

    /// <summary>List all metrics configurations for a bucket.</summary>
    ListBucketMetricsConfigurations,

    /// <summary>List all inventory configurations for a bucket.</summary>
    ListBucketInventoryConfigurations,

    /// <summary>List all Intelligent-Tiering configurations for a bucket.</summary>
    ListBucketIntelligentTieringConfigurations,

    // ── Object retention &amp; legal hold ────────────────────────────────

    /// <summary>Set the retention settings of an object.</summary>
    PutObjectRetention,

    /// <summary>Set or remove the legal hold on an object.</summary>
    PutObjectLegalHold,

    // ── Advanced object operations ──────────────────────────────────

    /// <summary>Run an SQL expression against the content of an object (S3 Select).</summary>
    SelectObjectContent,

    /// <summary>Initiate a restore of an archived object.</summary>
    RestoreObject,

    /// <summary>Upload an object using an HTML form POST.</summary>
    PostObject,

    /// <summary>Retrieve a subset of object attributes without downloading the full object.</summary>
    GetObjectAttributes,
}
