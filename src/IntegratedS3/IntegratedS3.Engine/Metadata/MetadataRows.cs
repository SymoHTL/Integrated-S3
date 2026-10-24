using System.Text.Json.Serialization;

namespace IntegratedS3.Engine.Metadata;

/// <summary>
/// A bucket's versioning state as stored. The numbers are part of the schema.
/// </summary>
internal enum StoredVersioning
{
    Disabled = 0,
    Enabled = 1,
    Suspended = 2
}

internal sealed record BucketRow(long Id, string Name, StoredVersioning Versioning, bool ObjectLockEnabled, long CreatedUs);

/// <summary>
/// The row that locks a key: the last sequence number handed out for it, and the current version, if any.
/// </summary>
internal sealed record HeadRow(long Seq, long? CurrentSeq);

/// <summary>
/// One version of an object, or a delete marker. The null version has the version id <see cref="NullVersionId"/>.
/// A row read by a listing carries neither <see cref="InlineData"/> nor <see cref="Manifest"/>.
/// </summary>
internal sealed class VersionRow
{
    public const string NullVersionId = "null";

    public required long BucketId { get; init; }

    public required byte[] Key { get; init; }

    public required long Seq { get; init; }

    public required string VersionId { get; init; }

    public bool IsDeleteMarker { get; init; }

    /// <summary>
    /// Gets whether this is the key's current version. Set by reads that know it; never written.
    /// </summary>
    public bool IsLatest { get; init; }

    public long Size { get; init; }

    public string? ETag { get; init; }

    public required long LastModifiedUs { get; init; }

    public string? StorageClass { get; init; }

    public long? RetainUntilUs { get; init; }

    public bool LegalHold { get; init; }

    public required VersionMeta Meta { get; init; }

    public byte[]? InlineData { get; init; }

    public IReadOnlyList<Extent>? Manifest { get; init; }
}

/// <summary>
/// The descriptive part of a version, stored as one JSON document.
/// </summary>
internal sealed record VersionMeta
{
    public string? ContentType { get; init; }

    public string? CacheControl { get; init; }

    public string? ContentDisposition { get; init; }

    public string? ContentEncoding { get; init; }

    public string? ContentLanguage { get; init; }

    public DateTimeOffset? ExpiresUtc { get; init; }

    public string? Expires { get; init; }

    public Dictionary<string, string>? Metadata { get; init; }

    public Dictionary<string, string>? Tags { get; init; }

    public Dictionary<string, string>? Checksums { get; init; }
}

/// <summary>
/// A byte range of a blob; an object's manifest is the ordered list of its extents.
/// </summary>
internal readonly record struct Extent(
    [property: JsonPropertyName("l")] string Locator,
    [property: JsonPropertyName("o")] long Offset,
    [property: JsonPropertyName("n")] long Length);

internal sealed class UploadRow
{
    public long Id { get; init; }

    public required long BucketId { get; init; }

    public required string UploadId { get; init; }

    public required byte[] Key { get; init; }

    public required long InitiatedUs { get; init; }

    public required UploadMeta Meta { get; init; }
}

internal sealed record UploadMeta
{
    public required VersionMeta Object { get; init; }

    public string? ChecksumAlgorithm { get; init; }

    public string? StorageClass { get; init; }
}

internal sealed class PartRow
{
    public required long UploadRowId { get; init; }

    public required int PartNumber { get; init; }

    public required long Size { get; init; }

    public required string ETag { get; init; }

    public required long LastModifiedUs { get; init; }

    public required PartMeta Meta { get; init; }

    public required IReadOnlyList<Extent> Manifest { get; init; }
}

internal sealed record PartMeta
{
    /// <summary>
    /// Gets the digests of the part, MD5 included; the MD5 feeds the multipart ETag.
    /// </summary>
    public required Dictionary<string, string> Checksums { get; init; }

    public string? CopySourceVersionId { get; init; }
}

[JsonSourceGenerationOptions(DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
[JsonSerializable(typeof(VersionMeta))]
[JsonSerializable(typeof(UploadMeta))]
[JsonSerializable(typeof(PartMeta))]
[JsonSerializable(typeof(Extent[]))]
internal sealed partial class MetadataJsonContext : JsonSerializerContext
{
}
