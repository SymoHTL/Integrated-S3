using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Core.Models;

namespace IntegratedS3.Core.Services;

public sealed record StorageReplicaRepairEntry
{
    public required string Id { get; init; }

    public required StorageReplicaRepairOrigin Origin { get; init; }

    public required StorageReplicaRepairStatus Status { get; init; }

    public required StorageOperationType Operation { get; init; }

    public required string PrimaryBackendName { get; init; }

    public required string ReplicaBackendName { get; init; }

    public required string BucketName { get; init; }

    public string? ObjectKey { get; init; }

    public string? VersionId { get; init; }

    public required DateTimeOffset CreatedAtUtc { get; init; }

    public required DateTimeOffset UpdatedAtUtc { get; init; }

    public int AttemptCount { get; init; }

    public StorageErrorCode? LastErrorCode { get; init; }

    public string? LastErrorMessage { get; init; }
}
