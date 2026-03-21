using System.Text.Json.Serialization;

namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Retention state for a specific object version.
/// </summary>
public sealed class ObjectRetentionInfo
{
    /// <summary>
    /// The name of the bucket that contains the object.
    /// </summary>
    public string BucketName { get; init; } = string.Empty;

    /// <summary>
    /// The object key within the bucket.
    /// </summary>
    public string Key { get; init; } = string.Empty;

    /// <summary>
    /// The version identifier, or <see langword="null"/> for the current version.
    /// </summary>
    public string? VersionId { get; init; }

    /// <summary>
    /// The retention mode applied to this object version.
    /// </summary>
    public ObjectRetentionMode? Mode { get; init; }

    /// <summary>
    /// The date until which the object is retained, in UTC.
    /// </summary>
    public DateTimeOffset? RetainUntilDateUtc { get; init; }
}

/// <summary>
/// Legal hold state for a specific object version.
/// </summary>
public sealed class ObjectLegalHoldInfo
{
    /// <summary>
    /// The name of the bucket that contains the object.
    /// </summary>
    public string BucketName { get; init; } = string.Empty;

    /// <summary>
    /// The object key within the bucket.
    /// </summary>
    public string Key { get; init; } = string.Empty;

    /// <summary>
    /// The version identifier, or <see langword="null"/> for the current version.
    /// </summary>
    public string? VersionId { get; init; }

    /// <summary>
    /// The legal hold status for this object version.
    /// </summary>
    public ObjectLegalHoldStatus? Status { get; init; }
}

/// <summary>
/// The retention mode applied to an object.
/// </summary>
[JsonConverter(typeof(JsonStringEnumConverter<ObjectRetentionMode>))]
public enum ObjectRetentionMode
{
    /// <summary>
    /// Retention can be bypassed by users with the appropriate permission.
    /// </summary>
    Governance,

    /// <summary>
    /// Retention cannot be shortened or removed by any user.
    /// </summary>
    Compliance
}

/// <summary>
/// Whether a legal hold is placed on an object.
/// </summary>
[JsonConverter(typeof(JsonStringEnumConverter<ObjectLegalHoldStatus>))]
public enum ObjectLegalHoldStatus
{
    /// <summary>
    /// No legal hold is in effect.
    /// </summary>
    Off,

    /// <summary>
    /// A legal hold is in effect; the object cannot be deleted.
    /// </summary>
    On
}
