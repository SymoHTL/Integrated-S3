using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the PutBucketCors operation.</summary>
public sealed class PutBucketCorsRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }

    /// <summary>The CORS rules to apply to the bucket.</summary>
    public IReadOnlyList<BucketCorsRule> Rules { get; init; } = [];
}
