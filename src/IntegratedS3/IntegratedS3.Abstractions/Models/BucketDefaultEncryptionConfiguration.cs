namespace IntegratedS3.Abstractions.Models;

public sealed class BucketDefaultEncryptionConfiguration
{
    public string BucketName { get; init; } = string.Empty;

    public required BucketDefaultEncryptionRule Rule { get; init; }
}

public sealed class BucketDefaultEncryptionRule
{
    public required ObjectServerSideEncryptionAlgorithm Algorithm { get; init; }

    public string? KeyId { get; init; }
}
