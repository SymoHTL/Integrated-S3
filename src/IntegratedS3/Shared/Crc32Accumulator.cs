namespace IntegratedS3.Shared;

/// <summary>
/// Incremental CRC-32 (IEEE) and CRC-32C (Castagnoli) with the S3 wire encoding: big-endian digest bytes.
/// </summary>
internal struct Crc32Accumulator
{
    private static readonly uint[] Crc32Table = CreateTable(0xEDB88320u);
    private static readonly uint[] Crc32cTable = CreateTable(0x82F63B78u);

    private readonly uint[] _table;
    private uint _current;

    public static Crc32Accumulator Create()
    {
        return new Crc32Accumulator(Crc32Table);
    }

    public static Crc32Accumulator CreateCastagnoli()
    {
        return new Crc32Accumulator(Crc32cTable);
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
