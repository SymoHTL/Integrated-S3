namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Request payment configuration for a bucket, controlling who pays for request and data transfer costs.
/// </summary>
public sealed class BucketRequestPaymentConfiguration
{
    /// <summary>
    /// The name of the bucket.
    /// </summary>
    public string BucketName { get; init; } = string.Empty;

    /// <summary>
    /// Who pays for requests and data transfer costs.
    /// </summary>
    public BucketPayer Payer { get; init; } = BucketPayer.BucketOwner;
}

/// <summary>
/// Identifies who pays for bucket requests and data transfer.
/// </summary>
public enum BucketPayer
{
    /// <summary>
    /// The bucket owner pays for all requests and data transfer.
    /// </summary>
    BucketOwner,

    /// <summary>
    /// The requester pays for requests and data transfer.
    /// </summary>
    Requester
}
