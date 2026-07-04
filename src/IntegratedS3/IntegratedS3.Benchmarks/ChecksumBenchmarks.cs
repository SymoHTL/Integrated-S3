using System.Security.Cryptography;
using BenchmarkDotNet.Attributes;

namespace IntegratedS3.Benchmarks;

/// <summary>
/// Benchmarks the ETag / checksum compute hot paths over representative object-payload sizes:
/// MD5 (ETag), SHA-1, SHA-256, and CRC-32C (Castagnoli). The CRC-32C accumulator mirrors the
/// shipping polynomial (0x82F63B78) used by <c>ChecksumTestAlgorithms</c> and the wire checksums.
/// </summary>
[MemoryDiagnoser]
public class ChecksumBenchmarks
{
    [Params(64 * 1024, 1024 * 1024, 8 * 1024 * 1024)]
    public int PayloadBytes { get; set; }

    private byte[] _payload = [];

    [GlobalSetup]
    public void Setup()
    {
        _payload = new byte[PayloadBytes];
        // Deterministic, non-trivial fill so the compiler / JIT cannot elide the hash work.
        for (var i = 0; i < _payload.Length; i++)
        {
            _payload[i] = (byte)(i * 31 + 7);
        }
    }

    [Benchmark(Baseline = true)]
    public byte[] Md5_ETag() => MD5.HashData(_payload);

    [Benchmark]
    public byte[] Sha1() => SHA1.HashData(_payload);

    [Benchmark]
    public byte[] Sha256() => SHA256.HashData(_payload);

    [Benchmark]
    public uint Crc32c() => Crc32C.Compute(_payload);

    /// <summary>CRC-32C (Castagnoli) — same polynomial as the shipping checksum helpers.</summary>
    private static class Crc32C
    {
        private static readonly uint[] Table = CreateTable(0x82F63B78u);

        public static uint Compute(ReadOnlySpan<byte> buffer)
        {
            var current = 0xFFFFFFFFu;
            foreach (var value in buffer)
            {
                current = (current >> 8) ^ Table[(byte)(current ^ value)];
            }

            return ~current;
        }

        private static uint[] CreateTable(uint polynomial)
        {
            var table = new uint[256];
            for (uint i = 0; i < table.Length; i++)
            {
                var value = i;
                for (var bit = 0; bit < 8; bit++)
                {
                    value = (value & 1) == 0 ? value >> 1 : polynomial ^ (value >> 1);
                }

                table[i] = value;
            }

            return table;
        }
    }
}
