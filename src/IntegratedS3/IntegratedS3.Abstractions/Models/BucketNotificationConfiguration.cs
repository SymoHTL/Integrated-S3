namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Event notification configuration for a bucket, including topic, queue, and Lambda function destinations.
/// </summary>
public sealed class BucketNotificationConfiguration
{
    /// <summary>
    /// The name of the bucket.
    /// </summary>
    public string BucketName { get; init; } = string.Empty;

    /// <summary>
    /// SNS topic notification configurations.
    /// </summary>
    public IReadOnlyList<BucketNotificationTopicConfiguration> TopicConfigurations { get; init; } = [];

    /// <summary>
    /// SQS queue notification configurations.
    /// </summary>
    public IReadOnlyList<BucketNotificationQueueConfiguration> QueueConfigurations { get; init; } = [];

    /// <summary>
    /// Lambda function notification configurations.
    /// </summary>
    public IReadOnlyList<BucketNotificationLambdaConfiguration> LambdaFunctionConfigurations { get; init; } = [];
}

/// <summary>
/// Notification configuration that publishes events to an SNS topic.
/// </summary>
public sealed class BucketNotificationTopicConfiguration
{
    /// <summary>
    /// An optional identifier for this configuration.
    /// </summary>
    public string? Id { get; init; }

    /// <summary>
    /// The ARN of the SNS topic to publish events to.
    /// </summary>
    public string TopicArn { get; init; } = string.Empty;

    /// <summary>
    /// The S3 event types that trigger this notification (e.g., "s3:ObjectCreated:*").
    /// </summary>
    public IReadOnlyList<string> Events { get; init; } = [];

    /// <summary>
    /// An optional filter that restricts which objects trigger notifications.
    /// </summary>
    public BucketNotificationFilter? Filter { get; init; }
}

/// <summary>
/// Notification configuration that publishes events to an SQS queue.
/// </summary>
public sealed class BucketNotificationQueueConfiguration
{
    /// <summary>
    /// An optional identifier for this configuration.
    /// </summary>
    public string? Id { get; init; }

    /// <summary>
    /// The ARN of the SQS queue to publish events to.
    /// </summary>
    public string QueueArn { get; init; } = string.Empty;

    /// <summary>
    /// The S3 event types that trigger this notification.
    /// </summary>
    public IReadOnlyList<string> Events { get; init; } = [];

    /// <summary>
    /// An optional filter that restricts which objects trigger notifications.
    /// </summary>
    public BucketNotificationFilter? Filter { get; init; }
}

/// <summary>
/// Notification configuration that invokes a Lambda function.
/// </summary>
public sealed class BucketNotificationLambdaConfiguration
{
    /// <summary>
    /// An optional identifier for this configuration.
    /// </summary>
    public string? Id { get; init; }

    /// <summary>
    /// The ARN of the Lambda function to invoke.
    /// </summary>
    public string LambdaFunctionArn { get; init; } = string.Empty;

    /// <summary>
    /// The S3 event types that trigger this notification.
    /// </summary>
    public IReadOnlyList<string> Events { get; init; } = [];

    /// <summary>
    /// An optional filter that restricts which objects trigger notifications.
    /// </summary>
    public BucketNotificationFilter? Filter { get; init; }
}

/// <summary>
/// Key-based filter for notification configurations.
/// </summary>
public sealed class BucketNotificationFilter
{
    /// <summary>
    /// The key filter rules used to match object keys.
    /// </summary>
    public IReadOnlyList<BucketNotificationFilterRule> KeyFilterRules { get; init; } = [];
}

/// <summary>
/// A single key filter rule that matches object key prefixes or suffixes.
/// </summary>
public sealed class BucketNotificationFilterRule
{
    /// <summary>
    /// The filter rule name (e.g., "prefix" or "suffix").
    /// </summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>
    /// The filter rule value to match against.
    /// </summary>
    public string Value { get; init; } = string.Empty;
}
