using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Core.Models;

namespace IntegratedS3.Core.Services;

public sealed record StorageReplicaRepairEntry
{
    public required string Id { get; init; }

    public required StorageReplicaRepairOrigin Origin { get; init; }

    public required StorageReplicaRepairStatus Status { get; init; }

    public required StorageOperationType Operation { get; init; }

    public StorageReplicaRepairDivergenceKind DivergenceKinds { get; init; }

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

    public static StorageReplicaRepairDivergenceKind GetDefaultDivergenceKinds(StorageOperationType operation)
    {
        return operation switch
        {
            StorageOperationType.CreateBucket or StorageOperationType.DeleteBucket
                => StorageReplicaRepairDivergenceKind.Metadata | StorageReplicaRepairDivergenceKind.Version,
            StorageOperationType.PutBucketVersioning
                => StorageReplicaRepairDivergenceKind.Version,
            StorageOperationType.PutBucketCors or StorageOperationType.DeleteBucketCors
                => StorageReplicaRepairDivergenceKind.Metadata,
            StorageOperationType.CopyObject or StorageOperationType.PutObject
                => StorageReplicaRepairDivergenceKind.Content | StorageReplicaRepairDivergenceKind.Metadata | StorageReplicaRepairDivergenceKind.Version,
            StorageOperationType.PutObjectTags or StorageOperationType.DeleteObjectTags
                => StorageReplicaRepairDivergenceKind.Metadata | StorageReplicaRepairDivergenceKind.Version,
            StorageOperationType.DeleteObject
                => StorageReplicaRepairDivergenceKind.Content | StorageReplicaRepairDivergenceKind.Version,
            _ => StorageReplicaRepairDivergenceKind.Metadata
        };
    }
}
