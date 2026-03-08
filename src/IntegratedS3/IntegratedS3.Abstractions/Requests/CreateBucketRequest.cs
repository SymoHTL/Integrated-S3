namespace IntegratedS3.Abstractions.Requests;

public sealed class CreateBucketRequest
{
    public required string BucketName { get; init; }

    public bool EnableVersioning { get; init; }
}
