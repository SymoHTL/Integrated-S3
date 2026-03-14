namespace IntegratedS3.Protocol;

public sealed class S3BucketEncryptionConfiguration
{
    public IReadOnlyList<S3BucketEncryptionRule> Rules { get; init; } = [];
}

public sealed class S3BucketEncryptionRule
{
    public required S3BucketEncryptionByDefault DefaultEncryption { get; init; }

    public bool? BucketKeyEnabled { get; init; }
}

public sealed class S3BucketEncryptionByDefault
{
    public string? SseAlgorithm { get; init; }

    public string? KmsMasterKeyId { get; init; }
}
