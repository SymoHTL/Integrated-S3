using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the PutObjectRetention operation.</summary>
public sealed class PutObjectRetentionRequest
{
    /// <summary>The name of the bucket containing the object.</summary>
    public required string BucketName { get; init; }

    /// <summary>The object key.</summary>
    public required string Key { get; init; }

    /// <summary>The version identifier of the object.</summary>
    public string? VersionId { get; init; }

    /// <summary>The retention mode to apply to the object.</summary>
    public ObjectRetentionMode? Mode { get; init; }

    /// <summary>The date until which the object must be retained, in UTC.</summary>
    public DateTimeOffset? RetainUntilDateUtc { get; init; }

    /// <summary>When <see langword="true"/>, bypasses governance-mode retention restrictions.</summary>
    public bool BypassGovernanceRetention { get; init; }
}
