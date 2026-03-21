namespace IntegratedS3.Core.Models;

/// <summary>
/// Well-known canned ACL presets compatible with the S3 ACL model.
/// </summary>
public enum StorageCannedAcl
{
    /// <summary>Owner gets <see cref="StorageAclPermission.FullControl"/>. No other access is granted.</summary>
    Private,

    /// <summary>Owner gets <see cref="StorageAclPermission.FullControl"/>. The <c>AllUsers</c> group gets <see cref="StorageAclPermission.Read"/>.</summary>
    PublicRead,

    /// <summary>Owner gets <see cref="StorageAclPermission.FullControl"/>. The <c>AllUsers</c> group gets <see cref="StorageAclPermission.Read"/> and <see cref="StorageAclPermission.Write"/>.</summary>
    PublicReadWrite,

    /// <summary>Owner gets <see cref="StorageAclPermission.FullControl"/>. The <c>AuthenticatedUsers</c> group gets <see cref="StorageAclPermission.Read"/>.</summary>
    AuthenticatedRead,

    /// <summary>Both the object owner and the bucket owner get <see cref="StorageAclPermission.FullControl"/>.</summary>
    BucketOwnerFullControl
}

/// <summary>
/// Represents an individual ACL permission that can be granted.
/// </summary>
public enum StorageAclPermission
{
    /// <summary>Allows reading the object data or listing the bucket contents.</summary>
    Read,

    /// <summary>Allows creating, overwriting, or deleting objects in a bucket.</summary>
    Write,

    /// <summary>Allows reading the ACL of the resource.</summary>
    ReadAcp,

    /// <summary>Allows writing (replacing) the ACL of the resource.</summary>
    WriteAcp,

    /// <summary>Grants all of the above permissions.</summary>
    FullControl
}

/// <summary>
/// Identifies the kind of entity that receives an ACL grant.
/// </summary>
public enum StorageAclGranteeType
{
    /// <summary>A specific user identified by a canonical user ID.</summary>
    CanonicalUser,

    /// <summary>A predefined group identified by a well-known URI.</summary>
    Group
}

/// <summary>
/// Identifies the recipient of an ACL grant.
/// </summary>
public sealed record StorageAclGrantee
{
    /// <summary>The type of grantee (user or group).</summary>
    public required StorageAclGranteeType Type { get; init; }

    /// <summary>The canonical user ID when <see cref="Type"/> is <see cref="StorageAclGranteeType.CanonicalUser"/>.</summary>
    public string? Id { get; init; }

    /// <summary>The group URI when <see cref="Type"/> is <see cref="StorageAclGranteeType.Group"/>.</summary>
    public string? Uri { get; init; }
}

/// <summary>
/// A single ACL grant that pairs a <see cref="StorageAclGrantee"/> with a <see cref="StorageAclPermission"/>.
/// </summary>
public sealed record StorageAclGrant
{
    /// <summary>The entity receiving the permission.</summary>
    public required StorageAclGrantee Grantee { get; init; }

    /// <summary>The permission being granted.</summary>
    public required StorageAclPermission Permission { get; init; }
}

/// <summary>
/// Captures the effective ACL state of an object for S3-compatibility purposes.
/// </summary>
public sealed record ObjectAclCompatibilityState
{
    /// <summary>The canned ACL that best describes the current access policy.</summary>
    public required StorageCannedAcl CannedAcl { get; init; }

    /// <summary>Any additional explicit grants beyond the canned ACL.</summary>
    public IReadOnlyList<StorageAclGrant> AdditionalGrants { get; init; } = [];
}

/// <summary>
/// Wraps a bucket policy JSON document together with pre-computed public-access flags.
/// </summary>
public sealed class BucketPolicyCompatibilityDocument
{
    /// <summary>The raw JSON policy document.</summary>
    public required string Document { get; init; }

    /// <summary>Indicates whether the policy permits unauthenticated listing of bucket contents.</summary>
    public bool AllowsPublicList { get; init; }

    /// <summary>Indicates whether the policy permits unauthenticated object reads.</summary>
    public bool AllowsPublicRead { get; init; }
}

/// <summary>
/// Request to set a canned ACL on a bucket for S3-compatibility.
/// </summary>
public sealed class PutBucketAclCompatibilityRequest
{
    /// <summary>The target bucket name.</summary>
    public required string BucketName { get; init; }

    /// <summary>The canned ACL to apply.</summary>
    public required StorageCannedAcl CannedAcl { get; init; }
}

/// <summary>
/// Request to set a canned ACL (and optional explicit grants) on an object for S3-compatibility.
/// </summary>
public sealed class PutObjectAclCompatibilityRequest
{
    /// <summary>The bucket containing the target object.</summary>
    public required string BucketName { get; init; }

    /// <summary>The key of the target object.</summary>
    public required string Key { get; init; }

    /// <summary>The canned ACL to apply.</summary>
    public required StorageCannedAcl CannedAcl { get; init; }

    /// <summary>Optional full ACL state to apply instead of (or in addition to) the canned ACL.</summary>
    public ObjectAclCompatibilityState? Acl { get; init; }
}

/// <summary>
/// Request to set a bucket policy document for S3-compatibility.
/// </summary>
public sealed class PutBucketPolicyCompatibilityRequest
{
    /// <summary>The target bucket name.</summary>
    public required string BucketName { get; init; }

    /// <summary>The policy document to apply.</summary>
    public required BucketPolicyCompatibilityDocument Policy { get; init; }
}
