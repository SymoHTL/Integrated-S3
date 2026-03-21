namespace IntegratedS3.Protocol;

/// <summary>
/// Represents an S3 Access Control List (ACL) policy containing an owner and a list of grants.
/// </summary>
public sealed class S3AccessControlPolicy
{
    /// <summary>The owner of the bucket or object.</summary>
    public S3BucketOwner Owner { get; init; } = new();

    /// <summary>The list of <see cref="S3AccessControlGrant"/> entries in the ACL.</summary>
    public IReadOnlyList<S3AccessControlGrant> Grants { get; init; } = [];
}

/// <summary>
/// Represents a single grant in an S3 access control policy.
/// </summary>
public sealed class S3AccessControlGrant
{
    /// <summary>The grantee to whom the permission is assigned.</summary>
    public S3AccessControlGrantee Grantee { get; init; } = new();

    /// <summary>The permission granted (e.g., <c>FULL_CONTROL</c>, <c>READ</c>, <c>WRITE</c>).</summary>
    public string Permission { get; init; } = string.Empty;
}

/// <summary>
/// Identifies the grantee in an S3 access control grant.
/// </summary>
public sealed class S3AccessControlGrantee
{
    /// <summary>The grantee type (e.g., <c>CanonicalUser</c>, <c>Group</c>).</summary>
    public string Type { get; init; } = string.Empty;

    /// <summary>The canonical user ID of the grantee.</summary>
    public string? Id { get; init; }

    /// <summary>The display name of the grantee.</summary>
    public string? DisplayName { get; init; }

    /// <summary>The URI of the grantee group (e.g., for predefined Amazon S3 groups).</summary>
    public string? Uri { get; init; }
}
