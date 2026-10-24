using System.Security.Cryptography;
using BenchmarkDotNet.Attributes;
using IntegratedS3.Shared;

namespace IntegratedS3.Benchmarks;

/// <summary>
/// Benchmarks the ETag / checksum compute hot paths over representative object-payload sizes:
/// MD5 (ETag), SHA-1, SHA-256, and CRC-32C (Castagnoli). CRC-32C runs the shipped
/// <c>Crc32Accumulator</c>, linked from <c>src/IntegratedS3/Shared</c>.
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
    public byte[] Crc32c()
    {
        var crc = Crc32Accumulator.CreateCastagnoli();
        crc.Append(_payload);
        return crc.GetHashBytes();
    }
}
