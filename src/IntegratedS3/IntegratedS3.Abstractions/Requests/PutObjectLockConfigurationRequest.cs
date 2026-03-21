using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the PutObjectLockConfiguration operation.</summary>
public sealed class PutObjectLockConfigurationRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }

    /// <summary>When <see langword="true"/>, enables Object Lock on the bucket.</summary>
    public bool ObjectLockEnabled { get; init; }

    /// <summary>The default retention settings for new objects in the bucket.</summary>
    public ObjectLockDefaultRetention? DefaultRetention { get; init; }
}
