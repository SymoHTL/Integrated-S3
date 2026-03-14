namespace IntegratedS3.Abstractions.Models;

public sealed class MultipartUploadPart
{
    public required int PartNumber { get; init; }

    public required string ETag { get; init; }

    public long ContentLength { get; init; }

    public DateTimeOffset LastModifiedUtc { get; init; }

    public IReadOnlyDictionary<string, string>? Checksums { get; init; }

    public string? CopySourceVersionId { get; init; }
}
