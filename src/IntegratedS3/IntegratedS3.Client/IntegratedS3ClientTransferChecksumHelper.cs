using System.Buffers;
using System.Globalization;
using System.Security.Cryptography;

namespace IntegratedS3.Client;

internal static class IntegratedS3ClientTransferChecksumHelper
{
    private const string SdkChecksumAlgorithmHeaderName = "x-amz-sdk-checksum-algorithm";
    private const string ChecksumTypeHeaderName = "x-amz-checksum-type";
    private const string ChecksumCrc32HeaderName = "x-amz-checksum-crc32";
    private const string ChecksumCrc32cHeaderName = "x-amz-checksum-crc32c";
    private const string ChecksumSha1HeaderName = "x-amz-checksum-sha1";
    private const string ChecksumSha256HeaderName = "x-amz-checksum-sha256";
    private const int BufferSize = 65536;

    internal static async ValueTask<PreparedUploadChecksum> PrepareUploadChecksumAsync(
        Stream content,
        IntegratedS3TransferChecksumAlgorithm algorithm,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(content);

        if (!content.CanSeek) {
            throw new NotSupportedException(
                "Checksum-aware uploads require a seekable source stream. Use UploadFileAsync or provide a seekable stream.");
        }

        var originalPosition = content.Position;
        using var checksum = CreateStreamingChecksum(algorithm);

        try {
            var buffer = ArrayPool<byte>.Shared.Rent(BufferSize);
            try {
                while (true) {
                    var bytesRead = await content.ReadAsync(
                        buffer.AsMemory(0, BufferSize),
                        cancellationToken).ConfigureAwait(false);
                    if (bytesRead == 0) {
                        break;
                    }

                    checksum.Append(buffer.AsSpan(0, bytesRead));
                }
            }
            finally {
                ArrayPool<byte>.Shared.Return(buffer);
            }

            return new PreparedUploadChecksum(
                AlgorithmKey: checksum.AlgorithmKey,
                AlgorithmHeaderValue: checksum.AlgorithmHeaderValue,
                ChecksumHeaderName: checksum.HeaderName,
                ChecksumValue: checksum.GetBase64Hash());
        }
        finally {
            content.Position = originalPosition;
        }
    }

    internal static IReadOnlyDictionary<string, string> CreateChecksumMap(in PreparedUploadChecksum checksum)
    {
        return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            [checksum.AlgorithmKey] = checksum.ChecksumValue
        };
    }

    internal static void ValidateUploadResponseChecksum(
        HttpResponseMessage response,
        in PreparedUploadChecksum checksum)
    {
        ArgumentNullException.ThrowIfNull(response);

        if (HasCompositeChecksumType(response)) {
            return;
        }

        if (!TryGetSingleHeaderValue(response, checksum.ChecksumHeaderName, out var actualChecksum)
            || string.IsNullOrWhiteSpace(actualChecksum)) {
            return;
        }

        var trimmedActualChecksum = actualChecksum.Trim();
        if (IsCompositeChecksumValue(trimmedActualChecksum)) {
            return;
        }

        if (string.Equals(trimmedActualChecksum, checksum.ChecksumValue, StringComparison.Ordinal)) {
            return;
        }

        throw new InvalidDataException(
            $"The upload response reported a {checksum.AlgorithmHeaderValue} checksum of '{trimmedActualChecksum}', but the client uploaded '{checksum.ChecksumValue}'.");
    }

    internal static ResponseChecksumValidation? CreateDownloadValidation(HttpResponseMessage response)
    {
        ArgumentNullException.ThrowIfNull(response);

        if (HasCompositeChecksumType(response)) {
            return null;
        }

        List<ExpectedChecksum>? expectedChecksums = null;

        TryAddExpectedChecksum(IntegratedS3TransferChecksumAlgorithm.Crc32, ChecksumCrc32HeaderName);
        TryAddExpectedChecksum(IntegratedS3TransferChecksumAlgorithm.Crc32C, ChecksumCrc32cHeaderName);
        TryAddExpectedChecksum(IntegratedS3TransferChecksumAlgorithm.Sha1, ChecksumSha1HeaderName);
        TryAddExpectedChecksum(IntegratedS3TransferChecksumAlgorithm.Sha256, ChecksumSha256HeaderName);

        if (expectedChecksums is null || expectedChecksums.Count == 0) {
            return null;
        }

        return new ResponseChecksumValidation(expectedChecksums.ToArray());

        void TryAddExpectedChecksum(
            IntegratedS3TransferChecksumAlgorithm algorithm,
            string headerName)
        {
            if (!TryGetSingleHeaderValue(response, headerName, out var expectedValue)
                || string.IsNullOrWhiteSpace(expectedValue)) {
                return;
            }

            var trimmedExpectedValue = expectedValue.Trim();
            if (IsCompositeChecksumValue(trimmedExpectedValue)) {
                return;
            }

            expectedChecksums ??= [];
            expectedChecksums.Add(new ExpectedChecksum(
                CreateStreamingChecksum(algorithm),
                trimmedExpectedValue));
        }
    }

    internal static async Task SeedExistingBytesAsync(
        ResponseChecksumValidation validation,
        string filePath,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(validation);
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        await using var existingStream = new FileStream(
            filePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            BufferSize,
            FileOptions.Asynchronous | FileOptions.SequentialScan);

        var buffer = ArrayPool<byte>.Shared.Rent(BufferSize);
        try {
            while (true) {
                var bytesRead = await existingStream.ReadAsync(
                    buffer.AsMemory(0, BufferSize),
                    cancellationToken).ConfigureAwait(false);
                if (bytesRead == 0) {
                    break;
                }

                validation.Append(buffer.AsSpan(0, bytesRead));
            }
        }
        finally {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    internal static async Task CopyToAsync(
        HttpContent content,
        Stream destination,
        ResponseChecksumValidation? validation,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(content);
        ArgumentNullException.ThrowIfNull(destination);

        using (validation) {
            await using var source = await content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
            var buffer = ArrayPool<byte>.Shared.Rent(BufferSize);

            try {
                while (true) {
                    var bytesRead = await source.ReadAsync(
                        buffer.AsMemory(0, BufferSize),
                        cancellationToken).ConfigureAwait(false);
                    if (bytesRead == 0) {
                        break;
                    }

                    validation?.Append(buffer.AsSpan(0, bytesRead));
                    await destination.WriteAsync(
                        buffer.AsMemory(0, bytesRead),
                        cancellationToken).ConfigureAwait(false);
                }

                validation?.ValidateOrThrow();
            }
            finally {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
    }

    internal static string ToProtocolValue(IntegratedS3TransferChecksumAlgorithm algorithm)
    {
        return algorithm switch
        {
            IntegratedS3TransferChecksumAlgorithm.Crc32 => "crc32",
            IntegratedS3TransferChecksumAlgorithm.Crc32C => "crc32c",
            IntegratedS3TransferChecksumAlgorithm.Sha1 => "sha1",
            IntegratedS3TransferChecksumAlgorithm.Sha256 => "sha256",
            _ => throw new ArgumentOutOfRangeException(nameof(algorithm), algorithm, "Unsupported checksum algorithm.")
        };
    }

    internal static string ToSdkHeaderValue(IntegratedS3TransferChecksumAlgorithm algorithm)
    {
        return algorithm switch
        {
            IntegratedS3TransferChecksumAlgorithm.Crc32 => "CRC32",
            IntegratedS3TransferChecksumAlgorithm.Crc32C => "CRC32C",
            IntegratedS3TransferChecksumAlgorithm.Sha1 => "SHA1",
            IntegratedS3TransferChecksumAlgorithm.Sha256 => "SHA256",
            _ => throw new ArgumentOutOfRangeException(nameof(algorithm), algorithm, "Unsupported checksum algorithm.")
        };
    }

    internal static string GetChecksumHeaderName(IntegratedS3TransferChecksumAlgorithm algorithm)
    {
        return algorithm switch
        {
            IntegratedS3TransferChecksumAlgorithm.Crc32 => ChecksumCrc32HeaderName,
            IntegratedS3TransferChecksumAlgorithm.Crc32C => ChecksumCrc32cHeaderName,
            IntegratedS3TransferChecksumAlgorithm.Sha1 => ChecksumSha1HeaderName,
            IntegratedS3TransferChecksumAlgorithm.Sha256 => ChecksumSha256HeaderName,
            _ => throw new ArgumentOutOfRangeException(nameof(algorithm), algorithm, "Unsupported checksum algorithm.")
        };
    }

    private static bool TryGetSingleHeaderValue(
        HttpResponseMessage response,
        string headerName,
        out string? value)
    {
        ArgumentNullException.ThrowIfNull(response);
        ArgumentException.ThrowIfNullOrWhiteSpace(headerName);

        if (response.Headers.TryGetValues(headerName, out var headerValues)) {
            value = headerValues.FirstOrDefault(static candidate => !string.IsNullOrWhiteSpace(candidate));
            return !string.IsNullOrWhiteSpace(value);
        }

        if (response.Content is not null
            && response.Content.Headers.TryGetValues(headerName, out var contentHeaderValues)) {
            value = contentHeaderValues.FirstOrDefault(static candidate => !string.IsNullOrWhiteSpace(candidate));
            return !string.IsNullOrWhiteSpace(value);
        }

        value = null;
        return false;
    }

    private static bool HasCompositeChecksumType(HttpResponseMessage response)
    {
        ArgumentNullException.ThrowIfNull(response);

        return TryGetSingleHeaderValue(response, ChecksumTypeHeaderName, out var checksumType)
            && string.Equals(checksumType, "COMPOSITE", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsCompositeChecksumValue(string? checksum)
    {
        if (string.IsNullOrWhiteSpace(checksum)) {
            return false;
        }

        var separatorIndex = checksum.LastIndexOf('-');
        if (separatorIndex <= 0 || separatorIndex >= checksum.Length - 1) {
            return false;
        }

        return int.TryParse(
                checksum[(separatorIndex + 1)..],
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var partCount)
            && partCount > 0;
    }

    private static StreamingChecksum CreateStreamingChecksum(IntegratedS3TransferChecksumAlgorithm algorithm)
    {
        return algorithm switch
        {
            IntegratedS3TransferChecksumAlgorithm.Crc32 => new Crc32StreamingChecksum(
                "crc32",
                "CRC32",
                ChecksumCrc32HeaderName,
                Crc32Accumulator.CreateStandard()),
            IntegratedS3TransferChecksumAlgorithm.Crc32C => new Crc32StreamingChecksum(
                "crc32c",
                "CRC32C",
                ChecksumCrc32cHeaderName,
                Crc32Accumulator.CreateCastagnoli()),
            IntegratedS3TransferChecksumAlgorithm.Sha1 => new IncrementalHashStreamingChecksum(
                "sha1",
                "SHA1",
                ChecksumSha1HeaderName,
                HashAlgorithmName.SHA1),
            IntegratedS3TransferChecksumAlgorithm.Sha256 => new IncrementalHashStreamingChecksum(
                "sha256",
                "SHA256",
                ChecksumSha256HeaderName,
                HashAlgorithmName.SHA256),
            _ => throw new ArgumentOutOfRangeException(nameof(algorithm), algorithm, "Unsupported checksum algorithm.")
        };
    }

    internal readonly record struct PreparedUploadChecksum(
        string AlgorithmKey,
        string AlgorithmHeaderValue,
        string ChecksumHeaderName,
        string ChecksumValue);

    internal sealed class ResponseChecksumValidation(ExpectedChecksum[] expectedChecksums) : IDisposable
    {
        private readonly ExpectedChecksum[] _expectedChecksums = expectedChecksums;

        public void Append(ReadOnlySpan<byte> buffer)
        {
            foreach (var expectedChecksum in _expectedChecksums) {
                expectedChecksum.Checksum.Append(buffer);
            }
        }

        public void ValidateOrThrow()
        {
            foreach (var expectedChecksum in _expectedChecksums) {
                var actualValue = expectedChecksum.Checksum.GetBase64Hash();
                if (string.Equals(actualValue, expectedChecksum.ExpectedValue, StringComparison.Ordinal)) {
                    continue;
                }

                throw new InvalidDataException(
                    $"The downloaded object failed {expectedChecksum.Checksum.AlgorithmHeaderValue} checksum validation. Expected '{expectedChecksum.ExpectedValue}', but computed '{actualValue}'.");
            }
        }

        public void Dispose()
        {
            foreach (var expectedChecksum in _expectedChecksums) {
                expectedChecksum.Checksum.Dispose();
            }
        }
    }

    internal readonly record struct ExpectedChecksum(
        StreamingChecksum Checksum,
        string ExpectedValue);

    internal abstract class StreamingChecksum : IDisposable
    {
        public abstract string AlgorithmKey { get; }

        public abstract string AlgorithmHeaderValue { get; }

        public abstract string HeaderName { get; }

        public abstract void Append(ReadOnlySpan<byte> buffer);

        public abstract string GetBase64Hash();

        public abstract void Dispose();
    }

    private sealed class IncrementalHashStreamingChecksum(
        string algorithmKey,
        string algorithmHeaderValue,
        string headerName,
        HashAlgorithmName hashAlgorithmName) : StreamingChecksum
    {
        private readonly IncrementalHash _hash = IncrementalHash.CreateHash(hashAlgorithmName);

        public override string AlgorithmKey => algorithmKey;

        public override string AlgorithmHeaderValue => algorithmHeaderValue;

        public override string HeaderName => headerName;

        public override void Append(ReadOnlySpan<byte> buffer)
        {
            _hash.AppendData(buffer);
        }

        public override string GetBase64Hash()
        {
            return Convert.ToBase64String(_hash.GetHashAndReset());
        }

        public override void Dispose()
        {
            _hash.Dispose();
        }
    }

    private sealed class Crc32StreamingChecksum(
        string algorithmKey,
        string algorithmHeaderValue,
        string headerName,
        Crc32Accumulator accumulator) : StreamingChecksum
    {
        private Crc32Accumulator _accumulator = accumulator;

        public override string AlgorithmKey => algorithmKey;

        public override string AlgorithmHeaderValue => algorithmHeaderValue;

        public override string HeaderName => headerName;

        public override void Append(ReadOnlySpan<byte> buffer)
        {
            _accumulator.Append(buffer);
        }

        public override string GetBase64Hash()
        {
            return Convert.ToBase64String(_accumulator.GetHashBytes());
        }

        public override void Dispose()
        {
        }
    }

    private struct Crc32Accumulator
    {
        private static readonly uint[] StandardTable = CreateTable(0xEDB88320u);
        private static readonly uint[] CastagnoliTable = CreateTable(0x82F63B78u);

        private readonly uint[] _table;
        private uint _current;

        public static Crc32Accumulator CreateStandard()
        {
            return new Crc32Accumulator(StandardTable);
        }

        public static Crc32Accumulator CreateCastagnoli()
        {
            return new Crc32Accumulator(CastagnoliTable);
        }

        private Crc32Accumulator(uint[] table)
        {
            _table = table;
            _current = 0xFFFFFFFFu;
        }

        public void Append(ReadOnlySpan<byte> buffer)
        {
            foreach (var value in buffer) {
                _current = (_current >> 8) ^ _table[(byte)(_current ^ value)];
            }
        }

        public byte[] GetHashBytes()
        {
            var finalized = ~_current;
            return
            [
                (byte)(finalized >> 24),
                (byte)(finalized >> 16),
                (byte)(finalized >> 8),
                (byte)finalized
            ];
        }

        private static uint[] CreateTable(uint polynomial)
        {
            var table = new uint[256];
            for (uint i = 0; i < table.Length; i++) {
                var value = i;
                for (var bit = 0; bit < 8; bit++) {
                    value = (value & 1) == 0
                        ? value >> 1
                        : polynomial ^ (value >> 1);
                }

                table[i] = value;
            }

            return table;
        }
    }
}
