namespace IntegratedS3.Core.Models;

/// <summary>
/// Represents a required HTTP header on a presigned request.
/// </summary>
public sealed class StoragePresignedHeader
{
    /// <summary>The header name.</summary>
    public required string Name { get; init; }

    /// <summary>The header value.</summary>
    public required string Value { get; init; }
}
