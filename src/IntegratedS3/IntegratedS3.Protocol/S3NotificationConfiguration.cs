namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the event notification configuration for an S3 bucket.
/// </summary>
public sealed class S3NotificationConfiguration
{
    /// <summary>The list of SNS topic notification configurations.</summary>
    public IReadOnlyList<S3TopicConfiguration> TopicConfigurations { get; init; } = [];

    /// <summary>The list of SQS queue notification configurations.</summary>
    public IReadOnlyList<S3QueueConfiguration> QueueConfigurations { get; init; } = [];

    /// <summary>The list of Lambda function notification configurations.</summary>
    public IReadOnlyList<S3CloudFunctionConfiguration> CloudFunctionConfigurations { get; init; } = [];
}

/// <summary>
/// Represents an SNS topic notification configuration for S3 events.
/// </summary>
public sealed class S3TopicConfiguration
{
    /// <summary>An optional identifier for this notification configuration.</summary>
    public string? Id { get; init; }

    /// <summary>The ARN of the SNS topic to publish events to.</summary>
    public string Topic { get; init; } = string.Empty;

    /// <summary>The S3 event types that trigger notifications.</summary>
    public IReadOnlyList<string> Events { get; init; } = [];

    /// <summary>The filter rules for object key name matching.</summary>
    public S3NotificationFilterRuleSet? Filter { get; init; }
}

/// <summary>
/// Represents an SQS queue notification configuration for S3 events.
/// </summary>
public sealed class S3QueueConfiguration
{
    /// <summary>An optional identifier for this notification configuration.</summary>
    public string? Id { get; init; }

    /// <summary>The ARN of the SQS queue to send events to.</summary>
    public string Queue { get; init; } = string.Empty;

    /// <summary>The S3 event types that trigger notifications.</summary>
    public IReadOnlyList<string> Events { get; init; } = [];

    /// <summary>The filter rules for object key name matching.</summary>
    public S3NotificationFilterRuleSet? Filter { get; init; }
}

/// <summary>
/// Represents a Lambda function notification configuration for S3 events.
/// </summary>
public sealed class S3CloudFunctionConfiguration
{
    /// <summary>An optional identifier for this notification configuration.</summary>
    public string? Id { get; init; }

    /// <summary>The ARN of the Lambda function to invoke.</summary>
    public string CloudFunction { get; init; } = string.Empty;

    /// <summary>The S3 event types that trigger notifications.</summary>
    public IReadOnlyList<string> Events { get; init; } = [];

    /// <summary>The filter rules for object key name matching.</summary>
    public S3NotificationFilterRuleSet? Filter { get; init; }
}

/// <summary>
/// Contains the S3 key filter rules used to match object keys for event notifications.
/// </summary>
public sealed class S3NotificationFilterRuleSet
{
    /// <summary>The list of S3 key name filter rules.</summary>
    public IReadOnlyList<S3NotificationFilterRule> S3KeyRules { get; init; } = [];
}

/// <summary>
/// Represents a single S3 key name filter rule for event notifications.
/// </summary>
public sealed class S3NotificationFilterRule
{
    /// <summary>The filter rule name (<c>prefix</c> or <c>suffix</c>).</summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>The value to match against the object key.</summary>
    public string Value { get; init; } = string.Empty;
}
