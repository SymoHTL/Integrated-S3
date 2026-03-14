namespace IntegratedS3.Client;

/// <summary>
/// Supported checksum algorithms for checksum-aware high-level client transfers.
/// </summary>
public enum IntegratedS3TransferChecksumAlgorithm
{
    Crc32,
    Crc32C,
    Sha1,
    Sha256
}
