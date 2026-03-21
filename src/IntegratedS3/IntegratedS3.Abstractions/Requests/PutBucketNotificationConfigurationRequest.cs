using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the PutBucketNotificationConfiguration operation.</summary>
public sealed class PutBucketNotificationConfigurationRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }

    /// <summary>The SNS topic notification configurations.</summary>
    public IReadOnlyList<BucketNotificationTopicConfiguration> TopicConfigurations { get; init; } = [];

    /// <summary>The SQS queue notification configurations.</summary>
    public IReadOnlyList<BucketNotificationQueueConfiguration> QueueConfigurations { get; init; } = [];

    /// <summary>The Lambda function notification configurations.</summary>
    public IReadOnlyList<BucketNotificationLambdaConfiguration> LambdaFunctionConfigurations { get; init; } = [];
}
