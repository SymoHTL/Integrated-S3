namespace IntegratedS3.Protocol;

public sealed class S3AccessControlPolicy
{
    public S3BucketOwner Owner { get; init; } = new();

    public IReadOnlyList<S3AccessControlGrant> Grants { get; init; } = [];
}

public sealed class S3AccessControlGrant
{
    public S3AccessControlGrantee Grantee { get; init; } = new();

    public string Permission { get; init; } = string.Empty;
}

public sealed class S3AccessControlGrantee
{
    public string Type { get; init; } = string.Empty;

    public string? Id { get; init; }

    public string? DisplayName { get; init; }

    public string? Uri { get; init; }
}
