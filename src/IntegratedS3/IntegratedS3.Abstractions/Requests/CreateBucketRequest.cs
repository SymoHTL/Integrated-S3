namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the CreateBucket operation.</summary>
public sealed class CreateBucketRequest
{
    /// <summary>The name of the bucket to create.</summary>
    public required string BucketName { get; init; }

    /// <summary>When <see langword="true"/>, enables versioning on the new bucket.</summary>
    public bool EnableVersioning { get; init; }
}
