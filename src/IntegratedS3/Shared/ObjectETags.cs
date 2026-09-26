using System.Globalization;
using System.Security.Cryptography;
using static IntegratedS3.Shared.ObjectChecksums;

namespace IntegratedS3.Shared;

/// <summary>
/// S3 ETag semantics: part and multipart ETags, ETag normalization, and If-Match / If-None-Match matching.
/// </summary>
internal static class ObjectETags
{
    /// <summary>
    /// Builds the S3 multipart object ETag: <c>&lt;hex(MD5(concat(partMd5Bytes)))&gt;-&lt;partCount&gt;</c>.
    /// Each element of <paramref name="partMd5Base64"/> is the base64 MD5 of one part's bytes.
    /// </summary>
    internal static string BuildMultipartETag(IReadOnlyList<string> partMd5Base64)
    {
        using var md5 = IncrementalHash.CreateHash(HashAlgorithmName.MD5);
        foreach (var partMd5 in partMd5Base64) {
            md5.AppendData(Convert.FromBase64String(partMd5));
        }

        return $"{Convert.ToHexStringLower(md5.GetHashAndReset())}-{partMd5Base64.Count}";
    }

    /// <summary>
    /// Determines whether <paramref name="etag"/> is a multipart composite ETag of the form
    /// <c>&lt;hex(MD5)&gt;-&lt;partCount&gt;</c> and, if so, extracts the trailing part count. Single-part
    /// objects (plain hex MD5, no suffix) and delete markers return <see langword="false"/>.
    /// </summary>
    internal static bool TryGetMultipartPartCount(string? etag, out int partCount)
    {
        partCount = 0;
        if (string.IsNullOrEmpty(etag)) {
            return false;
        }

        var separatorIndex = etag.LastIndexOf('-');
        if (separatorIndex <= 0 || separatorIndex == etag.Length - 1) {
            return false;
        }

        return int.TryParse(
            etag.AsSpan(separatorIndex + 1),
            NumberStyles.None,
            CultureInfo.InvariantCulture,
            out partCount)
            && partCount > 0;
    }

    /// <summary>
    /// Computes the S3 ETag of a single uploaded multipart part: the lowercase-hex MD5 of the part's
    /// bytes, taken from the MD5 already computed by <see cref="ObjectChecksums.ChecksumComputation"/>.
    /// </summary>
    internal static string BuildPartETag(IReadOnlyDictionary<string, string> partChecksums)
    {
        return TryGetChecksumValue(partChecksums, Md5ChecksumAlgorithm, out var md5Base64)
            ? Convert.ToHexStringLower(Convert.FromBase64String(md5Base64))
            : throw new InvalidOperationException("Multipart part MD5 checksum is required to derive the part ETag.");
    }

    internal static bool MatchesIfMatch(string? rawHeader, string? currentETag)
    {
        if (string.IsNullOrWhiteSpace(rawHeader)) {
            return true;
        }

        if (rawHeader.Trim() == "*") {
            return true;
        }

        return MatchesAnyETag(rawHeader, currentETag);
    }

    internal static bool ShouldEvaluateIfUnmodifiedSince(string? rawIfMatch, string? currentETag)
    {
        return string.IsNullOrWhiteSpace(rawIfMatch) || !MatchesIfMatch(rawIfMatch, currentETag);
    }

    internal static bool MatchesAnyETag(string? rawHeader, string? currentETag)
    {
        if (string.IsNullOrWhiteSpace(rawHeader) || string.IsNullOrWhiteSpace(currentETag)) {
            return false;
        }

        var normalizedCurrent = NormalizeETag(currentETag);
        foreach (var candidate in rawHeader.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)) {
            if (candidate == "*" || NormalizeETag(candidate) == normalizedCurrent) {
                return true;
            }
        }

        return false;
    }

    internal static string NormalizeETag(string value)
    {
        var trimmed = value.Trim();
        if (trimmed.StartsWith("W/", StringComparison.OrdinalIgnoreCase)) {
            trimmed = trimmed[2..].Trim();
        }

        if (trimmed.Length >= 2 && trimmed.StartsWith('"') && trimmed.EndsWith('"')) {
            trimmed = trimmed[1..^1];
        }

        return trimmed;
    }
}
