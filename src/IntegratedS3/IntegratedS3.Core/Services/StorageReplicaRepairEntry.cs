using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Core.Models;

namespace IntegratedS3.Core.Services;

/// <summary>
/// Represents a single entry in the replica repair backlog, tracking a pending
/// or in-progress attempt to reconcile data between the primary and a replica backend.
/// </summary>
public sealed record StorageReplicaRepairEntry
{
    /// <summary>The unique identifier of this repair entry.</summary>
    public required string Id { get; init; }

    /// <summary>How this repair entry was created (e.g. async replication or partial-write failure).</summary>
    public required StorageReplicaRepairOrigin Origin { get; init; }

    /// <summary>The current status of the repair attempt.</summary>
    public required StorageReplicaRepairStatus Status { get; init; }

    /// <summary>The storage operation that needs to be replayed on the replica.</summary>
    public required StorageOperationType Operation { get; init; }

    /// <summary>The name of the primary backend that holds the authoritative data.</summary>
    public required string PrimaryBackendName { get; init; }

    /// <summary>The name of the replica backend that needs repair.</summary>
    public required string ReplicaBackendName { get; init; }

    /// <summary>The bucket involved in the repair operation.</summary>
    public required string BucketName { get; init; }

    /// <summary>The object key involved in the repair, or <see langword="null"/> for bucket-level operations.</summary>
    public string? ObjectKey { get; init; }

    /// <summary>The object version identifier, or <see langword="null"/> when not applicable.</summary>
    public string? VersionId { get; init; }

    /// <summary>The UTC date/time when this repair entry was created.</summary>
    public required DateTimeOffset CreatedAtUtc { get; init; }

    /// <summary>The UTC date/time when this repair entry was last updated.</summary>
    public required DateTimeOffset UpdatedAtUtc { get; init; }

    /// <summary>The number of repair attempts made so far.</summary>
    public int AttemptCount { get; init; }

    /// <summary>The error code from the most recent failed attempt, or <see langword="null"/> if no failure has occurred.</summary>
    public StorageErrorCode? LastErrorCode { get; init; }

    /// <summary>A human-readable message from the most recent failed attempt, or <see langword="null"/>.</summary>
    public string? LastErrorMessage { get; init; }
}
