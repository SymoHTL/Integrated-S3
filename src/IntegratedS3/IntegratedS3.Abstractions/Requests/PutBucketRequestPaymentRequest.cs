using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the PutBucketRequestPayment operation.</summary>
public sealed class PutBucketRequestPaymentRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }

    /// <summary>The payer responsible for request and data transfer costs.</summary>
    public BucketPayer Payer { get; init; } = BucketPayer.BucketOwner;
}
