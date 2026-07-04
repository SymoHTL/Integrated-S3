namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the PutBucketOwnershipControls operation.</summary>
public sealed class PutBucketOwnershipControlsRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }

    /// <summary>
    /// The object ownership setting to apply. One of <c>BucketOwnerPreferred</c>,
    /// <c>ObjectWriter</c>, or <c>BucketOwnerEnforced</c>.
    /// </summary>
    public required string ObjectOwnership { get; init; }
}
