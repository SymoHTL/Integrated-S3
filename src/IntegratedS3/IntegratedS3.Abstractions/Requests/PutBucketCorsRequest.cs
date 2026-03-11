using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

public sealed class PutBucketCorsRequest
{
    public required string BucketName { get; init; }

    public IReadOnlyList<BucketCorsRule> Rules { get; init; } = [];
}
