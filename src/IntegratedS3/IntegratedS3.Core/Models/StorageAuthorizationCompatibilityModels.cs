namespace IntegratedS3.Core.Models;

public enum StorageCannedAcl
{
    Private,
    PublicRead,
    BucketOwnerFullControl
}

public enum StorageAclPermission
{
    Read,
    ReadAcp,
    WriteAcp,
    FullControl
}

public enum StorageAclGranteeType
{
    CanonicalUser,
    Group
}

public sealed record StorageAclGrantee
{
    public required StorageAclGranteeType Type { get; init; }

    public string? Id { get; init; }

    public string? Uri { get; init; }
}

public sealed record StorageAclGrant
{
    public required StorageAclGrantee Grantee { get; init; }

    public required StorageAclPermission Permission { get; init; }
}

public sealed record ObjectAclCompatibilityState
{
    public required StorageCannedAcl CannedAcl { get; init; }

    public IReadOnlyList<StorageAclGrant> AdditionalGrants { get; init; } = [];
}

public sealed class BucketPolicyCompatibilityDocument
{
    public required string Document { get; init; }

    public bool AllowsPublicList { get; init; }

    public bool AllowsPublicRead { get; init; }
}

public sealed class PutBucketAclCompatibilityRequest
{
    public required string BucketName { get; init; }

    public required StorageCannedAcl CannedAcl { get; init; }
}

public sealed class PutObjectAclCompatibilityRequest
{
    public required string BucketName { get; init; }

    public required string Key { get; init; }

    public required StorageCannedAcl CannedAcl { get; init; }

    public ObjectAclCompatibilityState? Acl { get; init; }
}

public sealed class PutBucketPolicyCompatibilityRequest
{
    public required string BucketName { get; init; }

    public required BucketPolicyCompatibilityDocument Policy { get; init; }
}
