namespace IntegratedS3.Core.Models;

public sealed class StoragePresignedHeader
{
    public required string Name { get; init; }

    public required string Value { get; init; }
}
