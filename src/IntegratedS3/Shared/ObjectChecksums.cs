using System.Security.Cryptography;
using IntegratedS3.Abstractions.Errors;

namespace IntegratedS3.Shared;

/// <summary>
/// S3 checksum semantics: the algorithm names, the digests a write computes, request validation and
/// multipart composite checksums. Compiled into each backend that needs them as linked source.
/// </summary>
internal static class ObjectChecksums
{
    internal const string Md5ChecksumAlgorithm = "md5";
    internal const string Sha256ChecksumAlgorithm = "sha256";
    internal const string Sha1ChecksumAlgorithm = "sha1";
    internal const string Crc32ChecksumAlgorithm = "crc32";
    internal const string Crc32cChecksumAlgorithm = "crc32c";
    internal const string Crc64NvmeChecksumAlgorithm = "crc64nvme";

    /// <summary>
    /// The set of content digests to compute over an object body. MD5 is implicitly always computed
    /// (needed for every ETag / per-part ETag), so it has no flag of its own; the flags select the
    /// additional, more expensive digests to compute in the same single pass.
    /// </summary>
    [Flags]
    internal enum ChecksumAlgorithms
    {
        None = 0,
        Sha256 = 1 << 0,
        Sha1 = 1 << 1,
        Crc32 = 1 << 2,
        Crc32c = 1 << 3,
        All = Sha256 | Sha1 | Crc32 | Crc32c
    }

    /// <summary>
    /// Maps a client-visible checksum algorithm key (<c>sha256</c>, <c>crc32</c>, …) to the
    /// corresponding <see cref="ChecksumAlgorithms"/> flag. MD5 maps to <see cref="ChecksumAlgorithms.None"/>
    /// (always computed) and CRC64NVME to <see cref="ChecksumAlgorithms.None"/> (pass-through, never
    /// server-computed). Returns <see cref="ChecksumAlgorithms.None"/> for a null/blank/unknown key.
    /// </summary>
    internal static ChecksumAlgorithms ToChecksumAlgorithmFlag(string? algorithm)
    {
        if (string.IsNullOrWhiteSpace(algorithm)) {
            return ChecksumAlgorithms.None;
        }

        if (string.Equals(algorithm, Sha256ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)) {
            return ChecksumAlgorithms.Sha256;
        }

        if (string.Equals(algorithm, Sha1ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)) {
            return ChecksumAlgorithms.Sha1;
        }

        if (string.Equals(algorithm, Crc32ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)) {
            return ChecksumAlgorithms.Crc32;
        }

        if (string.Equals(algorithm, Crc32cChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)) {
            return ChecksumAlgorithms.Crc32c;
        }

        return ChecksumAlgorithms.None;
    }

    /// <summary>
    /// Builds the additional-digest set a write path must compute: every algorithm the client either
    /// supplied a value for (for server-side validation) or asked the server to compute
    /// (<paramref name="requiredAlgorithm"/>). MD5 is always computed regardless. Returns the smallest
    /// set that still satisfies validation and the requested checksum response.
    /// <para>
    /// When neither a value nor an algorithm is requested and <paramref name="computeAllWhenNoneRequested"/>
    /// is <see langword="true"/>, all digests are computed. Object-level write paths (PutObject,
    /// CopyObject, the non-composite CompleteMultipartUpload) set this so an object stored without a
    /// requested checksum still persists the full digest set for later retrieval, preserving the
    /// existing stored-checksum contract; per-part paths leave it <see langword="false"/> because a
    /// part only ever exposes the upload/requested algorithm.
    /// </para>
    /// </summary>
    internal static ChecksumAlgorithms DetermineRequiredChecksumAlgorithms(
        IReadOnlyDictionary<string, string>? requestedChecksums,
        string? requiredAlgorithm = null,
        bool computeAllWhenNoneRequested = false)
    {
        var algorithms = ToChecksumAlgorithmFlag(requiredAlgorithm);

        if (requestedChecksums is not null) {
            foreach (var requestedChecksum in requestedChecksums) {
                algorithms |= ToChecksumAlgorithmFlag(requestedChecksum.Key);
            }
        }

        return computeAllWhenNoneRequested && algorithms == ChecksumAlgorithms.None
            ? ChecksumAlgorithms.All
            : algorithms;
    }

    /// <summary>
    /// Incremental multi-digest accumulator. MD5 is always computed; SHA-1/SHA-256/CRC32/CRC32C are
    /// only allocated and fed when their flag is set in the requested <see cref="ChecksumAlgorithms"/>.
    /// Used both for the streaming (inline, tee) PutObject write path and for the read-back paths.
    /// </summary>
    internal struct ChecksumComputation : IDisposable
    {
        private readonly IncrementalHash _md5;
        private readonly IncrementalHash? _sha256;
        private readonly IncrementalHash? _sha1;
        private Crc32Accumulator _crc32;
        private Crc32Accumulator _crc32c;
        private readonly bool _hasCrc32;
        private readonly bool _hasCrc32c;

        public ChecksumComputation(ChecksumAlgorithms algorithms)
        {
            _md5 = IncrementalHash.CreateHash(HashAlgorithmName.MD5);
            _sha256 = algorithms.HasFlag(ChecksumAlgorithms.Sha256) ? IncrementalHash.CreateHash(HashAlgorithmName.SHA256) : null;
            _sha1 = algorithms.HasFlag(ChecksumAlgorithms.Sha1) ? IncrementalHash.CreateHash(HashAlgorithmName.SHA1) : null;
            _hasCrc32 = algorithms.HasFlag(ChecksumAlgorithms.Crc32);
            _hasCrc32c = algorithms.HasFlag(ChecksumAlgorithms.Crc32c);
            _crc32 = _hasCrc32 ? Crc32Accumulator.Create() : default;
            _crc32c = _hasCrc32c ? Crc32Accumulator.CreateCastagnoli() : default;
        }

        public void Append(ReadOnlySpan<byte> buffer)
        {
            _md5.AppendData(buffer);
            _sha256?.AppendData(buffer);
            _sha1?.AppendData(buffer);
            if (_hasCrc32) {
                _crc32.Append(buffer);
            }

            if (_hasCrc32c) {
                _crc32c.Append(buffer);
            }
        }

        public readonly IReadOnlyDictionary<string, string> ToDictionary()
        {
            var checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [Md5ChecksumAlgorithm] = Convert.ToBase64String(_md5.GetHashAndReset())
            };

            if (_sha256 is not null) {
                checksums[Sha256ChecksumAlgorithm] = Convert.ToBase64String(_sha256.GetHashAndReset());
            }

            if (_sha1 is not null) {
                checksums[Sha1ChecksumAlgorithm] = Convert.ToBase64String(_sha1.GetHashAndReset());
            }

            if (_hasCrc32) {
                checksums[Crc32ChecksumAlgorithm] = Convert.ToBase64String(_crc32.GetHashBytes());
            }

            if (_hasCrc32c) {
                checksums[Crc32cChecksumAlgorithm] = Convert.ToBase64String(_crc32c.GetHashBytes());
            }

            return checksums;
        }

        public readonly void Dispose()
        {
            _md5.Dispose();
            _sha256?.Dispose();
            _sha1?.Dispose();
        }
    }

    internal static StorageError? ValidateRequestedChecksums(
        IReadOnlyDictionary<string, string>? requestedChecksums,
        IReadOnlyDictionary<string, string>? actualChecksums,
        string bucketName,
        string objectKey,
        string providerName)
    {
        if (requestedChecksums is null || requestedChecksums.Count == 0) {
            return null;
        }

        foreach (var requestedChecksum in requestedChecksums) {
            // CRC64NVME is accepted as pass-through (cannot be server-validated)
            if (string.Equals(requestedChecksum.Key, Crc64NvmeChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)) {
                continue;
            }

            if (!string.Equals(requestedChecksum.Key, Md5ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
                && !string.Equals(requestedChecksum.Key, Sha256ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
                && !string.Equals(requestedChecksum.Key, Sha1ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
                && !string.Equals(requestedChecksum.Key, Crc32ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
                && !string.Equals(requestedChecksum.Key, Crc32cChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)) {
                return StorageError.Unsupported(
                    $"Checksum algorithm '{requestedChecksum.Key}' is not currently supported for request validation.",
                    bucketName,
                    objectKey);
            }

            if (actualChecksums is null
                || !actualChecksums.TryGetValue(requestedChecksum.Key, out var actualChecksum)
                || !string.Equals(requestedChecksum.Value, actualChecksum, StringComparison.Ordinal)) {
                return new StorageError
                {
                    Code = StorageErrorCode.InvalidChecksum,
                    Message = $"The supplied {requestedChecksum.Key.ToUpperInvariant()} checksum for object '{objectKey}' does not match the uploaded content.",
                    BucketName = bucketName,
                    ObjectKey = objectKey,
                    ProviderName = providerName,
                    SuggestedHttpStatusCode = 400
                };
            }
        }

        return null;
    }

    internal static bool TryNormalizeChecksumAlgorithm(string? value, out string? checksumAlgorithm)
    {
        if (string.IsNullOrWhiteSpace(value)) {
            checksumAlgorithm = null;
            return true;
        }

        if (string.Equals(value, Sha256ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "SHA256", StringComparison.OrdinalIgnoreCase)) {
            checksumAlgorithm = Sha256ChecksumAlgorithm;
            return true;
        }

        if (string.Equals(value, Sha1ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "SHA1", StringComparison.OrdinalIgnoreCase)) {
            checksumAlgorithm = Sha1ChecksumAlgorithm;
            return true;
        }

        if (string.Equals(value, Crc32ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "CRC32", StringComparison.OrdinalIgnoreCase)) {
            checksumAlgorithm = Crc32ChecksumAlgorithm;
            return true;
        }

        if (string.Equals(value, Crc32cChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "CRC32C", StringComparison.OrdinalIgnoreCase)) {
            checksumAlgorithm = Crc32cChecksumAlgorithm;
            return true;
        }

        if (string.Equals(value, Crc64NvmeChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "CRC64NVME", StringComparison.OrdinalIgnoreCase)) {
            checksumAlgorithm = Crc64NvmeChecksumAlgorithm;
            return true;
        }

        checksumAlgorithm = null;
        return false;
    }

    // Single source of truth for which checksum algorithms the multipart lifecycle can carry end-to-end.
    // A blank algorithm is always allowed (no checksum requested). CRC64NVME is intentionally excluded:
    // although it is a valid single-part checksum (accepted as pass-through), the multipart composite
    // path (BuildCompositeChecksum) cannot synthesize it, so accepting it at initiate would leave the
    // upload dead at UploadPart/Complete. All multipart lifecycle gates (initiate, upload part,
    // upload-part-copy, complete, list parts) must use this helper so the accepted set cannot drift.
    internal static bool IsMultipartSupportedChecksumAlgorithm(string? checksumAlgorithm)
    {
        if (string.IsNullOrWhiteSpace(checksumAlgorithm)) {
            return true;
        }

        return string.Equals(checksumAlgorithm, Sha256ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
            || string.Equals(checksumAlgorithm, Sha1ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
            || string.Equals(checksumAlgorithm, Crc32ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)
            || string.Equals(checksumAlgorithm, Crc32cChecksumAlgorithm, StringComparison.OrdinalIgnoreCase);
    }

    internal static bool TryGetChecksumValue(IReadOnlyDictionary<string, string>? checksums, string? algorithm, out string value)
    {
        value = string.Empty;
        if (checksums is null || string.IsNullOrWhiteSpace(algorithm)) {
            return false;
        }

        if (checksums.TryGetValue(algorithm, out var directValue) && !string.IsNullOrWhiteSpace(directValue)) {
            value = directValue;
            return true;
        }

        foreach (var checksum in checksums) {
            if (string.Equals(checksum.Key, algorithm, StringComparison.OrdinalIgnoreCase)
                && !string.IsNullOrWhiteSpace(checksum.Value)) {
                value = checksum.Value;
                return true;
            }
        }

        value = string.Empty;
        return false;
    }

    internal static IReadOnlyDictionary<string, string>? CreateMultipartPartResponseChecksums(
        IReadOnlyDictionary<string, string> actualChecksums,
        string? uploadChecksumAlgorithm,
        string? requestedChecksumAlgorithm,
        IReadOnlyDictionary<string, string>? requestedChecksums)
    {
        if (!string.IsNullOrWhiteSpace(uploadChecksumAlgorithm)
            && TryGetChecksumValue(actualChecksums, uploadChecksumAlgorithm, out var uploadChecksum)) {
            return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [uploadChecksumAlgorithm] = uploadChecksum
            };
        }

        if (!string.IsNullOrWhiteSpace(requestedChecksumAlgorithm)
            && TryGetChecksumValue(actualChecksums, requestedChecksumAlgorithm, out var requestedChecksum)) {
            return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [requestedChecksumAlgorithm] = requestedChecksum
            };
        }

        if (requestedChecksums is null || requestedChecksums.Count == 0) {
            return null;
        }

        var responseChecksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var requestedChecksumEntry in requestedChecksums) {
            if (TryGetChecksumValue(actualChecksums, requestedChecksumEntry.Key, out var actualChecksum)) {
                responseChecksums[requestedChecksumEntry.Key] = actualChecksum;
            }
        }

        return responseChecksums.Count == 0
            ? null
            : responseChecksums;
    }

    internal static IReadOnlyDictionary<string, string> CreatePutObjectChecksums(
        IReadOnlyDictionary<string, string> actualChecksums,
        IReadOnlyDictionary<string, string>? requestedChecksums)
    {
        if (requestedChecksums is null || requestedChecksums.Count == 0) {
            return actualChecksums;
        }

        var persistedChecksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var requestedChecksumEntry in requestedChecksums) {
            if (TryGetChecksumValue(actualChecksums, requestedChecksumEntry.Key, out var actualChecksum)) {
                persistedChecksums[requestedChecksumEntry.Key] = actualChecksum;
            }
        }

        return persistedChecksums.Count == 0
            ? actualChecksums
            : persistedChecksums;
    }

    internal static IReadOnlyDictionary<string, string>? CreateCopyObjectChecksums(
        IReadOnlyDictionary<string, string>? actualChecksums,
        IReadOnlyDictionary<string, string>? sourceChecksums,
        string? checksumAlgorithm)
    {
        if (!string.IsNullOrWhiteSpace(checksumAlgorithm)
            && TryGetChecksumValue(actualChecksums, checksumAlgorithm, out var checksumValue)) {
            return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [checksumAlgorithm] = checksumValue
            };
        }

        return sourceChecksums ?? actualChecksums;
    }

    internal static string BuildCompositeChecksum(string algorithm, IReadOnlyList<string> partChecksums)
    {
        if (string.Equals(algorithm, Sha256ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)) {
            return BuildCompositeSha256Checksum(partChecksums);
        }

        if (string.Equals(algorithm, Sha1ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)) {
            return BuildCompositeSha1Checksum(partChecksums);
        }

        if (string.Equals(algorithm, Crc32ChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)) {
            return BuildCompositeCrc32Checksum(partChecksums);
        }

        if (string.Equals(algorithm, Crc32cChecksumAlgorithm, StringComparison.OrdinalIgnoreCase)) {
            return BuildCompositeCrc32cChecksum(partChecksums);
        }

        throw new InvalidOperationException($"Multipart checksum algorithm '{algorithm}' is not supported for composite checksum synthesis.");
    }

    private static string BuildCompositeSha256Checksum(IReadOnlyList<string> partChecksums)
    {
        using var checksum = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        foreach (var partChecksum in partChecksums) {
            checksum.AppendData(Convert.FromBase64String(partChecksum));
        }

        return $"{Convert.ToBase64String(checksum.GetHashAndReset())}-{partChecksums.Count}";
    }

    private static string BuildCompositeSha1Checksum(IReadOnlyList<string> partChecksums)
    {
        using var checksum = IncrementalHash.CreateHash(HashAlgorithmName.SHA1);
        foreach (var partChecksum in partChecksums) {
            checksum.AppendData(Convert.FromBase64String(partChecksum));
        }

        return $"{Convert.ToBase64String(checksum.GetHashAndReset())}-{partChecksums.Count}";
    }

    private static string BuildCompositeCrc32Checksum(IReadOnlyList<string> partChecksums)
    {
        var checksum = Crc32Accumulator.Create();
        foreach (var partChecksum in partChecksums) {
            checksum.Append(Convert.FromBase64String(partChecksum));
        }

        return $"{Convert.ToBase64String(checksum.GetHashBytes())}-{partChecksums.Count}";
    }

    private static string BuildCompositeCrc32cChecksum(IReadOnlyList<string> partChecksums)
    {
        var checksum = Crc32Accumulator.CreateCastagnoli();
        foreach (var partChecksum in partChecksums) {
            checksum.Append(Convert.FromBase64String(partChecksum));
        }

        return $"{Convert.ToBase64String(checksum.GetHashBytes())}-{partChecksums.Count}";
    }
}
