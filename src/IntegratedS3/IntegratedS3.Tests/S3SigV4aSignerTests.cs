using IntegratedS3.Protocol;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class S3SigV4aSignerTests
{
    [Fact]
    public void DeriveEcdsaKey_ReturnsValidP256Key()
    {
        using var key = S3SigV4aSigner.DeriveEcdsaKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "AKIDEXAMPLE");
        var ecParams = key.ExportParameters(includePrivateParameters: true);
        Assert.Equal(32, ecParams.D!.Length);
        Assert.Equal(32, ecParams.Q.X!.Length);
        Assert.Equal(32, ecParams.Q.Y!.Length);
    }

    [Fact]
    public void DeriveEcdsaKey_IsDeterministic()
    {
        using var key1 = S3SigV4aSigner.DeriveEcdsaKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "AKIDEXAMPLE");
        using var key2 = S3SigV4aSigner.DeriveEcdsaKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "AKIDEXAMPLE");
        var params1 = key1.ExportParameters(includePrivateParameters: true);
        var params2 = key2.ExportParameters(includePrivateParameters: true);
        Assert.Equal(params1.D, params2.D);
    }

    [Fact]
    public void DeriveEcdsaKey_DifferentAccessKeyIds_ProduceDifferentKeys()
    {
        using var key1 = S3SigV4aSigner.DeriveEcdsaKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "AKID1");
        using var key2 = S3SigV4aSigner.DeriveEcdsaKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "AKID2");
        var params1 = key1.ExportParameters(includePrivateParameters: true);
        var params2 = key2.ExportParameters(includePrivateParameters: true);
        Assert.NotEqual(params1.D, params2.D);
    }

    [Fact]
    public void DeriveEcdsaKey_DifferentSecretKeys_ProduceDifferentKeys()
    {
        using var key1 = S3SigV4aSigner.DeriveEcdsaKey("secret1", "AKIDEXAMPLE");
        using var key2 = S3SigV4aSigner.DeriveEcdsaKey("secret2", "AKIDEXAMPLE");
        var params1 = key1.ExportParameters(includePrivateParameters: true);
        var params2 = key2.ExportParameters(includePrivateParameters: true);
        Assert.NotEqual(params1.D, params2.D);
    }

    [Fact]
    public void SignAndVerify_RoundTrip_Succeeds()
    {
        using var key = S3SigV4aSigner.DeriveEcdsaKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "AKIDEXAMPLE");
        var stringToSign = S3SigV4aSigner.BuildStringToSign(
            new DateTimeOffset(2024, 1, 15, 12, 0, 0, TimeSpan.Zero),
            "20240115/s3/aws4_request",
            S3SigV4Signer.ComputeSha256Hex("test canonical request"));

        var signature = S3SigV4aSigner.ComputeSignature(key, stringToSign);

        Assert.Equal(128, signature.Length);
        Assert.True(S3SigV4aSigner.VerifySignature(key, stringToSign, signature));
    }

    [Fact]
    public void VerifySignature_WrongData_Fails()
    {
        using var key = S3SigV4aSigner.DeriveEcdsaKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "AKIDEXAMPLE");
        var stringToSign = "test string to sign";
        var signature = S3SigV4aSigner.ComputeSignature(key, stringToSign);

        Assert.False(S3SigV4aSigner.VerifySignature(key, "different string", signature));
    }

    [Fact]
    public void VerifySignature_WrongKey_Fails()
    {
        using var key1 = S3SigV4aSigner.DeriveEcdsaKey("secret1", "AKID1");
        using var key2 = S3SigV4aSigner.DeriveEcdsaKey("secret2", "AKID2");
        var stringToSign = "test string to sign";
        var signature = S3SigV4aSigner.ComputeSignature(key1, stringToSign);

        Assert.False(S3SigV4aSigner.VerifySignature(key2, stringToSign, signature));
    }

    [Fact]
    public void BuildStringToSign_FormatsCorrectly()
    {
        var result = S3SigV4aSigner.BuildStringToSign(
            new DateTimeOffset(2024, 1, 15, 12, 30, 45, TimeSpan.Zero),
            "20240115/s3/aws4_request",
            "abcdef1234567890");

        var lines = result.Split('\n');
        Assert.Equal(4, lines.Length);
        Assert.Equal("AWS4-ECDSA-P256-SHA256", lines[0]);
        Assert.Equal("20240115T123045Z", lines[1]);
        Assert.Equal("20240115/s3/aws4_request", lines[2]);
        Assert.Equal("abcdef1234567890", lines[3]);
    }

    [Fact]
    public void BuildCredentialScopeString_FormatsCorrectly()
    {
        var result = S3SigV4aSigner.BuildCredentialScopeString("20240115", "s3");
        Assert.Equal("20240115/s3/aws4_request", result);
    }

    [Fact]
    public void BuildStreamingPayloadStringToSign_FormatsCorrectly()
    {
        var result = S3SigV4aSigner.BuildStreamingPayloadStringToSign(
            new DateTimeOffset(2024, 1, 15, 12, 0, 0, TimeSpan.Zero),
            "20240115/s3/aws4_request",
            "previousSig",
            "chunkHash");

        var lines = result.Split('\n');
        Assert.Equal(6, lines.Length);
        Assert.Equal("AWS4-ECDSA-P256-SHA256-PAYLOAD", lines[0]);
        Assert.Equal("20240115T120000Z", lines[1]);
        Assert.Equal("20240115/s3/aws4_request", lines[2]);
        Assert.Equal("previousSig", lines[3]);
        Assert.Equal(S3SigV4Signer.ComputeSha256Hex(string.Empty), lines[4]);
        Assert.Equal("chunkHash", lines[5]);
    }

    [Fact]
    public void BuildStreamingTrailerStringToSign_FormatsCorrectly()
    {
        var result = S3SigV4aSigner.BuildStreamingTrailerStringToSign(
            new DateTimeOffset(2024, 1, 15, 12, 0, 0, TimeSpan.Zero),
            "20240115/s3/aws4_request",
            "previousSig",
            "trailerHash");

        var lines = result.Split('\n');
        Assert.Equal(5, lines.Length);
        Assert.Equal("AWS4-ECDSA-P256-SHA256-TRAILER", lines[0]);
        Assert.Equal("20240115T120000Z", lines[1]);
        Assert.Equal("20240115/s3/aws4_request", lines[2]);
        Assert.Equal("previousSig", lines[3]);
        Assert.Equal("trailerHash", lines[4]);
    }

    [Fact]
    public void ComputeSignature_IsNotDeterministic_ButAlwaysVerifies()
    {
        using var key = S3SigV4aSigner.DeriveEcdsaKey("secret", "AKID");
        var stringToSign = "consistent input";

        var sig1 = S3SigV4aSigner.ComputeSignature(key, stringToSign);
        var sig2 = S3SigV4aSigner.ComputeSignature(key, stringToSign);

        Assert.True(S3SigV4aSigner.VerifySignature(key, stringToSign, sig1));
        Assert.True(S3SigV4aSigner.VerifySignature(key, stringToSign, sig2));
    }
}
