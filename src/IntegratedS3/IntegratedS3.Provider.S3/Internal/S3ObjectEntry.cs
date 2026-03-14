using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Provider.S3.Internal;

/// <summary>
/// Represents the metadata for a single S3 object as returned by HEAD or GET operations.
/// Content-type and metadata are only populated by HeadObject/GetObject (not by list operations).
/// </summary>
internal sealed record S3ObjectEntry(
    string Key,
    long ContentLength,
    string? ContentType,
    string? ETag,
    DateTimeOffset LastModifiedUtc,
    IReadOnlyDictionary<string, string>? Metadata,
    string? VersionId,
    bool IsLatest = true,
    bool IsDeleteMarker = false,
    IReadOnlyDictionary<string, string>? Checksums = null,
    ObjectServerSideEncryptionInfo? ServerSideEncryption = null,
    string? CacheControl = null,
    string? ContentDisposition = null,
    string? ContentEncoding = null,
    string? ContentLanguage = null,
    DateTimeOffset? ExpiresUtc = null);
