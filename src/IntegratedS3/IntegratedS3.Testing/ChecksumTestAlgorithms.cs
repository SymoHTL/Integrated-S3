using System.Security.Cryptography;
using System.Text;
using IntegratedS3.Shared;

namespace IntegratedS3.Testing;

/// <summary>
/// Shared checksum helpers for provider tests and compatibility fixtures.
/// </summary>
public static class ChecksumTestAlgorithms
{
    /// <summary>
    /// Computes a SHA-1 hash of the UTF-8 encoded <paramref name="content"/> and returns it as a base-64 string.
    /// </summary>
    /// <param name="content">The string content to hash.</param>
    /// <returns>The base-64 encoded SHA-1 hash.</returns>
    public static string ComputeSha1Base64(string content)
    {
        ArgumentNullException.ThrowIfNull(content);
        return Convert.ToBase64String(SHA1.HashData(Encoding.UTF8.GetBytes(content)));
    }

    /// <summary>
    /// Computes a SHA-256 hash of the UTF-8 encoded <paramref name="content"/> and returns it as a base-64 string.
    /// </summary>
    /// <param name="content">The string content to hash.</param>
    /// <returns>The base-64 encoded SHA-256 hash.</returns>
    public static string ComputeSha256Base64(string content)
    {
        ArgumentNullException.ThrowIfNull(content);
        return Convert.ToBase64String(SHA256.HashData(Encoding.UTF8.GetBytes(content)));
    }

    /// <summary>
    /// Computes a CRC-32C (Castagnoli) checksum of the UTF-8 encoded <paramref name="content"/> and returns it as a base-64 string.
    /// </summary>
    /// <param name="content">The string content to checksum.</param>
    /// <returns>The base-64 encoded CRC-32C checksum.</returns>
    public static string ComputeCrc32cBase64(string content)
    {
        ArgumentNullException.ThrowIfNull(content);

        var checksum = Crc32Accumulator.CreateCastagnoli();
        checksum.Append(Encoding.UTF8.GetBytes(content));
        return Convert.ToBase64String(checksum.GetHashBytes());
    }

    /// <summary>
    /// Computes a composite CRC-32C checksum from individual base-64 part checksums, in the
    /// S3-style <c>{hash}-{partCount}</c> format used for multipart uploads.
    /// </summary>
    /// <param name="partChecksums">Base-64 encoded CRC-32C checksums for each part.</param>
    /// <returns>The composite checksum string in <c>{base64}-{partCount}</c> format.</returns>
    public static string ComputeMultipartCrc32cBase64(params string[] partChecksums)
    {
        ArgumentNullException.ThrowIfNull(partChecksums);

        var checksum = Crc32Accumulator.CreateCastagnoli();
        foreach (var partChecksum in partChecksums) {
            checksum.Append(Convert.FromBase64String(partChecksum));
        }

        return $"{Convert.ToBase64String(checksum.GetHashBytes())}-{partChecksums.Length}";
    }
}
