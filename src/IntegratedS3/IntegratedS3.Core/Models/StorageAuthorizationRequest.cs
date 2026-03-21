namespace IntegratedS3.Core.Models;

/// <summary>
/// Describes the context of a storage operation that requires authorization.
/// Passed to authorization services so they can make allow/deny decisions.
/// </summary>
public sealed class StorageAuthorizationRequest
{
    /// <summary>The <see cref="StorageOperationType"/> being requested.</summary>
    public required StorageOperationType Operation { get; init; }

    /// <summary>The target bucket name, or <see langword="null"/> for service-level operations such as <see cref="StorageOperationType.ListBuckets"/>.</summary>
    public string? BucketName { get; init; }

    /// <summary>The target object key, or <see langword="null"/> for bucket-level operations.</summary>
    public string? Key { get; init; }

    /// <summary>The source bucket name for copy operations, or <see langword="null"/> when not applicable.</summary>
    public string? SourceBucketName { get; init; }

    /// <summary>The source object key for copy operations, or <see langword="null"/> when not applicable.</summary>
    public string? SourceKey { get; init; }

    /// <summary>The object version identifier, or <see langword="null"/> for the latest version.</summary>
    public string? VersionId { get; init; }

    /// <summary>Indicates whether the operation includes or modifies object metadata.</summary>
    public bool IncludesMetadata { get; init; }
}