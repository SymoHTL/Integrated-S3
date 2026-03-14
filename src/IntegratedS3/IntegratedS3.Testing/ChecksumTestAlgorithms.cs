using System.Security.Cryptography;
using System.Text;

namespace IntegratedS3.Testing;

/// <summary>
/// Shared checksum helpers for provider tests and compatibility fixtures.
/// </summary>
public static class ChecksumTestAlgorithms
{
    public static string ComputeSha1Base64(string content)
    {
        ArgumentNullException.ThrowIfNull(content);
        return Convert.ToBase64String(SHA1.HashData(Encoding.UTF8.GetBytes(content)));
    }

    public static string ComputeSha256Base64(string content)
    {
        ArgumentNullException.ThrowIfNull(content);
        return Convert.ToBase64String(SHA256.HashData(Encoding.UTF8.GetBytes(content)));
    }

    public static string ComputeCrc32cBase64(string content)
    {
        ArgumentNullException.ThrowIfNull(content);

        var checksum = Crc32Accumulator.CreateCastagnoli();
        checksum.Append(Encoding.UTF8.GetBytes(content));
        return Convert.ToBase64String(checksum.GetHashBytes());
    }

    public static string ComputeMultipartCrc32cBase64(params string[] partChecksums)
    {
        ArgumentNullException.ThrowIfNull(partChecksums);

        var checksum = Crc32Accumulator.CreateCastagnoli();
        foreach (var partChecksum in partChecksums) {
            checksum.Append(Convert.FromBase64String(partChecksum));
        }

        return $"{Convert.ToBase64String(checksum.GetHashBytes())}-{partChecksums.Length}";
    }

    private struct Crc32Accumulator
    {
        private static readonly uint[] CastagnoliTable = CreateTable(0x82F63B78u);

        private readonly uint[] _table;
        private uint _current;

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
