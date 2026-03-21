namespace IntegratedS3.Client;

/// <summary>
/// Supported checksum algorithms for checksum-aware high-level client transfers.
/// </summary>
public enum IntegratedS3TransferChecksumAlgorithm
{
    /// <summary>CRC-32 (IEEE 802.3) checksum.</summary>
    Crc32,
    /// <summary>CRC-32C (Castagnoli) checksum.</summary>
    Crc32C,
    /// <summary>SHA-1 hash.</summary>
    Sha1,
    /// <summary>SHA-256 hash.</summary>
    Sha256
}
