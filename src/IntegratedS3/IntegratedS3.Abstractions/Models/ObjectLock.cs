using System.Text.Json.Serialization;

namespace IntegratedS3.Abstractions.Models;

public sealed class ObjectRetentionInfo
{
    public string BucketName { get; init; } = string.Empty;

    public string Key { get; init; } = string.Empty;

    public string? VersionId { get; init; }

    public ObjectRetentionMode? Mode { get; init; }

    public DateTimeOffset? RetainUntilDateUtc { get; init; }
}

public sealed class ObjectLegalHoldInfo
{
    public string BucketName { get; init; } = string.Empty;

    public string Key { get; init; } = string.Empty;

    public string? VersionId { get; init; }

    public ObjectLegalHoldStatus? Status { get; init; }
}

[JsonConverter(typeof(JsonStringEnumConverter<ObjectRetentionMode>))]
public enum ObjectRetentionMode
{
    Governance,
    Compliance
}

[JsonConverter(typeof(JsonStringEnumConverter<ObjectLegalHoldStatus>))]
public enum ObjectLegalHoldStatus
{
    Off,
    On
}
