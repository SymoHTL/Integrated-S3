namespace IntegratedS3.Core.Models;

/// <summary>
/// A single HTTP header that must be included when executing a presigned request.
/// </summary>
public sealed class StoragePresignedHeader
{
    /// <summary>The header name (e.g. <c>x-amz-content-sha256</c>).</summary>
    public required string Name { get; init; }

    /// <summary>The header value.</summary>
    public required string Value { get; init; }
}
