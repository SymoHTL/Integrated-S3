using System.Text.Json.Serialization;
using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Provider.Disk.Internal;

internal sealed class DiskBucketMetadata
{
    public BucketVersioningStatus VersioningStatus { get; init; } = BucketVersioningStatus.Disabled;

    public DiskBucketCorsConfiguration? CorsConfiguration { get; init; }

    [JsonPropertyName("taggingConfiguration")]
    public DiskBucketTaggingConfiguration? TaggingConfiguration { get; init; }

    [JsonPropertyName("loggingConfiguration")]
    public DiskBucketLoggingConfiguration? LoggingConfiguration { get; init; }

    [JsonPropertyName("websiteConfiguration")]
    public DiskBucketWebsiteConfiguration? WebsiteConfiguration { get; init; }

    [JsonPropertyName("requestPaymentConfiguration")]
    public DiskBucketRequestPaymentConfiguration? RequestPaymentConfiguration { get; init; }

    [JsonPropertyName("accelerateConfiguration")]
    public DiskBucketAccelerateConfiguration? AccelerateConfiguration { get; init; }

    [JsonPropertyName("lifecycleConfiguration")]
    public DiskBucketLifecycleConfiguration? LifecycleConfiguration { get; init; }

    [JsonPropertyName("replicationConfiguration")]
    public DiskBucketReplicationConfiguration? ReplicationConfiguration { get; init; }

    [JsonPropertyName("notificationConfiguration")]
    public DiskBucketNotificationConfiguration? NotificationConfiguration { get; init; }

    [JsonPropertyName("objectLockConfiguration")]
    public DiskBucketObjectLockConfiguration? ObjectLockConfiguration { get; init; }

    [JsonPropertyName("analyticsConfigurations")]
    public Dictionary<string, DiskBucketAnalyticsConfiguration>? AnalyticsConfigurations { get; init; }

    [JsonPropertyName("metricsConfigurations")]
    public Dictionary<string, DiskBucketMetricsConfiguration>? MetricsConfigurations { get; init; }

    [JsonPropertyName("inventoryConfigurations")]
    public Dictionary<string, DiskBucketInventoryConfiguration>? InventoryConfigurations { get; init; }

    [JsonPropertyName("intelligentTieringConfigurations")]
    public Dictionary<string, DiskBucketIntelligentTieringConfiguration>? IntelligentTieringConfigurations { get; init; }
}

internal sealed class DiskBucketCorsConfiguration
{
    public DiskBucketCorsRule[] Rules { get; init; } = [];
}

internal sealed class DiskBucketCorsRule
{
    public string? Id { get; init; }

    public string[] AllowedOrigins { get; init; } = [];

    public string[] AllowedMethods { get; init; } = [];

    public string[] AllowedHeaders { get; init; } = [];

    public string[] ExposeHeaders { get; init; } = [];

    public int? MaxAgeSeconds { get; init; }
}

// --- Tagging ---

internal sealed class DiskBucketTaggingConfiguration
{
    [JsonPropertyName("tags")]
    public Dictionary<string, string> Tags { get; init; } = new();
}

// --- Logging ---

internal sealed class DiskBucketLoggingConfiguration
{
    [JsonPropertyName("targetBucket")]
    public string? TargetBucket { get; init; }

    [JsonPropertyName("targetPrefix")]
    public string? TargetPrefix { get; init; }
}

// --- Website ---

internal sealed class DiskBucketWebsiteConfiguration
{
    [JsonPropertyName("indexDocumentSuffix")]
    public string? IndexDocumentSuffix { get; init; }

    [JsonPropertyName("errorDocumentKey")]
    public string? ErrorDocumentKey { get; init; }

    [JsonPropertyName("redirectAllRequestsTo")]
    public DiskBucketWebsiteRedirectAllRequestsTo? RedirectAllRequestsTo { get; init; }

    [JsonPropertyName("routingRules")]
    public DiskBucketWebsiteRoutingRule[] RoutingRules { get; init; } = [];
}

internal sealed class DiskBucketWebsiteRedirectAllRequestsTo
{
    [JsonPropertyName("hostName")]
    public string HostName { get; init; } = string.Empty;

    [JsonPropertyName("protocol")]
    public string? Protocol { get; init; }
}

internal sealed class DiskBucketWebsiteRoutingRule
{
    [JsonPropertyName("condition")]
    public DiskBucketWebsiteRoutingRuleCondition? Condition { get; init; }

    [JsonPropertyName("redirect")]
    public DiskBucketWebsiteRoutingRuleRedirect Redirect { get; init; } = new();
}

internal sealed class DiskBucketWebsiteRoutingRuleCondition
{
    [JsonPropertyName("keyPrefixEquals")]
    public string? KeyPrefixEquals { get; init; }

    [JsonPropertyName("httpErrorCodeReturnedEquals")]
    public int? HttpErrorCodeReturnedEquals { get; init; }
}

internal sealed class DiskBucketWebsiteRoutingRuleRedirect
{
    [JsonPropertyName("hostName")]
    public string? HostName { get; init; }

    [JsonPropertyName("protocol")]
    public string? Protocol { get; init; }

    [JsonPropertyName("replaceKeyPrefixWith")]
    public string? ReplaceKeyPrefixWith { get; init; }

    [JsonPropertyName("replaceKeyWith")]
    public string? ReplaceKeyWith { get; init; }

    [JsonPropertyName("httpRedirectCode")]
    public int? HttpRedirectCode { get; init; }
}

// --- Request Payment ---

internal sealed class DiskBucketRequestPaymentConfiguration
{
    [JsonPropertyName("payer")]
    public string Payer { get; init; } = "BucketOwner";
}

// --- Accelerate ---

internal sealed class DiskBucketAccelerateConfiguration
{
    [JsonPropertyName("status")]
    public string Status { get; init; } = "Suspended";
}

// --- Lifecycle ---

internal sealed class DiskBucketLifecycleConfiguration
{
    [JsonPropertyName("rules")]
    public DiskBucketLifecycleRule[] Rules { get; init; } = [];
}

internal sealed class DiskBucketLifecycleRule
{
    [JsonPropertyName("id")]
    public string? Id { get; init; }

    [JsonPropertyName("filterPrefix")]
    public string? FilterPrefix { get; init; }

    [JsonPropertyName("filterTags")]
    public Dictionary<string, string>? FilterTags { get; init; }

    [JsonPropertyName("status")]
    public string Status { get; init; } = "Disabled";

    [JsonPropertyName("expirationDays")]
    public int? ExpirationDays { get; init; }

    [JsonPropertyName("expirationDate")]
    public DateTimeOffset? ExpirationDate { get; init; }

    [JsonPropertyName("expiredObjectDeleteMarker")]
    public bool? ExpiredObjectDeleteMarker { get; init; }

    [JsonPropertyName("noncurrentVersionExpirationDays")]
    public int? NoncurrentVersionExpirationDays { get; init; }

    [JsonPropertyName("abortIncompleteMultipartUploadDaysAfterInitiation")]
    public int? AbortIncompleteMultipartUploadDaysAfterInitiation { get; init; }

    [JsonPropertyName("transitions")]
    public DiskBucketLifecycleTransition[] Transitions { get; init; } = [];

    [JsonPropertyName("noncurrentVersionTransitions")]
    public DiskBucketLifecycleNoncurrentVersionTransition[] NoncurrentVersionTransitions { get; init; } = [];
}

internal sealed class DiskBucketLifecycleTransition
{
    [JsonPropertyName("days")]
    public int? Days { get; init; }

    [JsonPropertyName("date")]
    public DateTimeOffset? Date { get; init; }

    [JsonPropertyName("storageClass")]
    public string StorageClass { get; init; } = string.Empty;
}

internal sealed class DiskBucketLifecycleNoncurrentVersionTransition
{
    [JsonPropertyName("noncurrentDays")]
    public int? NoncurrentDays { get; init; }

    [JsonPropertyName("storageClass")]
    public string StorageClass { get; init; } = string.Empty;
}

// --- Replication ---

internal sealed class DiskBucketReplicationConfiguration
{
    [JsonPropertyName("role")]
    public string? Role { get; init; }

    [JsonPropertyName("rules")]
    public DiskBucketReplicationRule[] Rules { get; init; } = [];
}

internal sealed class DiskBucketReplicationRule
{
    [JsonPropertyName("id")]
    public string? Id { get; init; }

    [JsonPropertyName("status")]
    public string Status { get; init; } = "Disabled";

    [JsonPropertyName("filterPrefix")]
    public string? FilterPrefix { get; init; }

    [JsonPropertyName("destination")]
    public DiskBucketReplicationDestination Destination { get; init; } = new();

    [JsonPropertyName("priority")]
    public int? Priority { get; init; }

    [JsonPropertyName("deleteMarkerReplication")]
    public bool DeleteMarkerReplication { get; init; }
}

internal sealed class DiskBucketReplicationDestination
{
    [JsonPropertyName("bucket")]
    public string Bucket { get; init; } = string.Empty;

    [JsonPropertyName("storageClass")]
    public string? StorageClass { get; init; }

    [JsonPropertyName("account")]
    public string? Account { get; init; }
}

// --- Notification ---

internal sealed class DiskBucketNotificationConfiguration
{
    [JsonPropertyName("topicConfigurations")]
    public DiskBucketNotificationTopicConfiguration[] TopicConfigurations { get; init; } = [];

    [JsonPropertyName("queueConfigurations")]
    public DiskBucketNotificationQueueConfiguration[] QueueConfigurations { get; init; } = [];

    [JsonPropertyName("lambdaFunctionConfigurations")]
    public DiskBucketNotificationLambdaConfiguration[] LambdaFunctionConfigurations { get; init; } = [];
}

internal sealed class DiskBucketNotificationTopicConfiguration
{
    [JsonPropertyName("id")]
    public string? Id { get; init; }

    [JsonPropertyName("topicArn")]
    public string TopicArn { get; init; } = string.Empty;

    [JsonPropertyName("events")]
    public string[] Events { get; init; } = [];

    [JsonPropertyName("filter")]
    public DiskBucketNotificationFilter? Filter { get; init; }
}

internal sealed class DiskBucketNotificationQueueConfiguration
{
    [JsonPropertyName("id")]
    public string? Id { get; init; }

    [JsonPropertyName("queueArn")]
    public string QueueArn { get; init; } = string.Empty;

    [JsonPropertyName("events")]
    public string[] Events { get; init; } = [];

    [JsonPropertyName("filter")]
    public DiskBucketNotificationFilter? Filter { get; init; }
}

internal sealed class DiskBucketNotificationLambdaConfiguration
{
    [JsonPropertyName("id")]
    public string? Id { get; init; }

    [JsonPropertyName("lambdaFunctionArn")]
    public string LambdaFunctionArn { get; init; } = string.Empty;

    [JsonPropertyName("events")]
    public string[] Events { get; init; } = [];

    [JsonPropertyName("filter")]
    public DiskBucketNotificationFilter? Filter { get; init; }
}

internal sealed class DiskBucketNotificationFilter
{
    [JsonPropertyName("keyFilterRules")]
    public DiskBucketNotificationFilterRule[] KeyFilterRules { get; init; } = [];
}

internal sealed class DiskBucketNotificationFilterRule
{
    [JsonPropertyName("name")]
    public string Name { get; init; } = string.Empty;

    [JsonPropertyName("value")]
    public string Value { get; init; } = string.Empty;
}

// --- Object Lock ---

internal sealed class DiskBucketObjectLockConfiguration
{
    [JsonPropertyName("objectLockEnabled")]
    public bool ObjectLockEnabled { get; init; }

    [JsonPropertyName("defaultRetention")]
    public DiskObjectLockDefaultRetention? DefaultRetention { get; init; }
}

internal sealed class DiskObjectLockDefaultRetention
{
    [JsonPropertyName("mode")]
    public string Mode { get; init; } = string.Empty;

    [JsonPropertyName("days")]
    public int? Days { get; init; }

    [JsonPropertyName("years")]
    public int? Years { get; init; }
}

// --- Analytics ---

internal sealed class DiskBucketAnalyticsConfiguration
{
    [JsonPropertyName("id")]
    public string Id { get; init; } = string.Empty;

    [JsonPropertyName("filterPrefix")]
    public string? FilterPrefix { get; init; }

    [JsonPropertyName("filterTags")]
    public Dictionary<string, string>? FilterTags { get; init; }

    [JsonPropertyName("storageClassAnalysis")]
    public DiskBucketAnalyticsStorageClassAnalysis? StorageClassAnalysis { get; init; }
}

internal sealed class DiskBucketAnalyticsStorageClassAnalysis
{
    [JsonPropertyName("dataExport")]
    public DiskBucketAnalyticsDataExport? DataExport { get; init; }
}

internal sealed class DiskBucketAnalyticsDataExport
{
    [JsonPropertyName("outputSchemaVersion")]
    public string OutputSchemaVersion { get; init; } = "V_1";

    [JsonPropertyName("destination")]
    public DiskBucketAnalyticsS3BucketDestination? Destination { get; init; }
}

internal sealed class DiskBucketAnalyticsS3BucketDestination
{
    [JsonPropertyName("format")]
    public string Format { get; init; } = "CSV";

    [JsonPropertyName("bucketAccountId")]
    public string? BucketAccountId { get; init; }

    [JsonPropertyName("bucket")]
    public string Bucket { get; init; } = string.Empty;

    [JsonPropertyName("prefix")]
    public string? Prefix { get; init; }
}

// --- Metrics ---

internal sealed class DiskBucketMetricsConfiguration
{
    [JsonPropertyName("id")]
    public string Id { get; init; } = string.Empty;

    [JsonPropertyName("filter")]
    public DiskBucketMetricsFilter? Filter { get; init; }
}

internal sealed class DiskBucketMetricsFilter
{
    [JsonPropertyName("prefix")]
    public string? Prefix { get; init; }

    [JsonPropertyName("accessPointArn")]
    public string? AccessPointArn { get; init; }

    [JsonPropertyName("tags")]
    public Dictionary<string, string> Tags { get; init; } = new();
}

// --- Inventory ---

internal sealed class DiskBucketInventoryConfiguration
{
    [JsonPropertyName("id")]
    public string Id { get; init; } = string.Empty;

    [JsonPropertyName("isEnabled")]
    public bool IsEnabled { get; init; }

    [JsonPropertyName("destination")]
    public DiskBucketInventoryDestination? Destination { get; init; }

    [JsonPropertyName("schedule")]
    public DiskBucketInventorySchedule? Schedule { get; init; }

    [JsonPropertyName("filter")]
    public DiskBucketInventoryFilter? Filter { get; init; }

    [JsonPropertyName("includedObjectVersions")]
    public string IncludedObjectVersions { get; init; } = "All";

    [JsonPropertyName("optionalFields")]
    public string[] OptionalFields { get; init; } = [];
}

internal sealed class DiskBucketInventoryDestination
{
    [JsonPropertyName("s3BucketDestination")]
    public DiskBucketInventoryS3BucketDestination? S3BucketDestination { get; init; }
}

internal sealed class DiskBucketInventoryS3BucketDestination
{
    [JsonPropertyName("format")]
    public string Format { get; init; } = "CSV";

    [JsonPropertyName("accountId")]
    public string? AccountId { get; init; }

    [JsonPropertyName("bucket")]
    public string Bucket { get; init; } = string.Empty;

    [JsonPropertyName("prefix")]
    public string? Prefix { get; init; }
}

internal sealed class DiskBucketInventorySchedule
{
    [JsonPropertyName("frequency")]
    public string Frequency { get; init; } = "Daily";
}

internal sealed class DiskBucketInventoryFilter
{
    [JsonPropertyName("prefix")]
    public string? Prefix { get; init; }
}

// --- Intelligent-Tiering ---

internal sealed class DiskBucketIntelligentTieringConfiguration
{
    [JsonPropertyName("id")]
    public string Id { get; init; } = string.Empty;

    [JsonPropertyName("status")]
    public string Status { get; init; } = "Enabled";

    [JsonPropertyName("filter")]
    public DiskBucketIntelligentTieringFilter? Filter { get; init; }

    [JsonPropertyName("tierings")]
    public DiskBucketIntelligentTiering[] Tierings { get; init; } = [];
}

internal sealed class DiskBucketIntelligentTieringFilter
{
    [JsonPropertyName("prefix")]
    public string? Prefix { get; init; }

    [JsonPropertyName("tags")]
    public Dictionary<string, string> Tags { get; init; } = new();
}

internal sealed class DiskBucketIntelligentTiering
{
    [JsonPropertyName("accessTier")]
    public string AccessTier { get; init; } = string.Empty;

    [JsonPropertyName("days")]
    public int Days { get; init; }
}
