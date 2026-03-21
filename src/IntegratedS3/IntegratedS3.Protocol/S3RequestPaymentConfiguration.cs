namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the request payment configuration for an S3 bucket.
/// </summary>
public sealed class S3RequestPaymentConfiguration
{
    /// <summary>Who pays for requests and data transfer (<c>BucketOwner</c> or <c>Requester</c>).</summary>
    public string Payer { get; init; } = "BucketOwner";
}
