using Xunit;

namespace IntegratedS3.Tests;

/// <summary>
/// The test helpers and the product compute CRC-32C from one source file (<c>src/IntegratedS3/Shared/Crc32Accumulator.cs</c>),
/// so a test that compares the two can no longer catch a CRC-32C defect. This pins it to the published check value.
/// </summary>
public sealed class Crc32cKnownAnswerTests
{
    // CRC-32C (Castagnoli) of the ASCII bytes "123456789" is 0xE3069283; S3 sends it as big-endian bytes, base64-encoded.
    [Fact]
    public void ComputeCrc32cBase64_OfTheStandardCheckInput_IsTheCatalogueCheckValue()
    {
        Assert.Equal(Convert.ToBase64String(new byte[] { 0xE3, 0x06, 0x92, 0x83 }), ChecksumTestAlgorithms.ComputeCrc32cBase64("123456789"));
    }
}
