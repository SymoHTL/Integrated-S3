namespace IntegratedS3.Core.Models;

public enum StorageCannedAcl
{
    Private,
    PublicRead,
    BucketOwnerFullControl
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
}

public sealed class PutBucketPolicyCompatibilityRequest
{
    public required string BucketName { get; init; }

    public required BucketPolicyCompatibilityDocument Policy { get; init; }
}
