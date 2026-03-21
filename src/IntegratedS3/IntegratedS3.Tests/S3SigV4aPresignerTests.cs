using IntegratedS3.Protocol;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class S3SigV4aPresignerTests
{
    [Fact]
    public void Presign_ProducesValidPresignedRequest()
    {
        var result = S3SigV4aPresigner.Presign(new S3SigV4aPresignParameters
        {
            HttpMethod = "GET",
            Path = "/my-bucket/my-key",
            AccessKeyId = "AKIDEXAMPLE",
            SecretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            Service = "s3",
            RegionSet = ["us-east-1"],
            SignedAtUtc = new DateTimeOffset(2024, 1, 15, 12, 0, 0, TimeSpan.Zero),
            ExpiresInSeconds = 3600,
            SignedHeaders = ["host"],
            Headers = [new("host", "s3.amazonaws.com")]
        });

        Assert.NotNull(result);
        Assert.Equal(128, result.Signature.Length); // ECDSA P-256 = 64 bytes = 128 hex chars
        Assert.Contains(result.QueryParameters, qp => qp.Key == "X-Amz-Algorithm" && qp.Value == "AWS4-ECDSA-P256-SHA256");
        Assert.Contains(result.QueryParameters, qp => qp.Key == "X-Amz-Region-Set" && qp.Value == "us-east-1");
        Assert.Contains(result.QueryParameters, qp => qp.Key == "X-Amz-Credential" && qp.Value!.Contains("/s3/aws4_request"));
        Assert.Contains(result.QueryParameters, qp => qp.Key == "X-Amz-Signature");
    }

    [Fact]
    public void Presign_WildcardRegionSet_Works()
    {
        var result = S3SigV4aPresigner.Presign(new S3SigV4aPresignParameters
        {
            HttpMethod = "GET",
            Path = "/test",
            AccessKeyId = "AKID",
            SecretAccessKey = "secret",
            Service = "s3",
            RegionSet = ["*"],
            SignedAtUtc = DateTimeOffset.UtcNow,
            ExpiresInSeconds = 300,
            SignedHeaders = ["host"],
            Headers = [new("host", "localhost")]
        });

        Assert.Contains(result.QueryParameters, qp => qp.Key == "X-Amz-Region-Set" && qp.Value == "*");
    }

    [Fact]
    public void Presign_MultiRegionSet_JoinsCommas()
    {
        var result = S3SigV4aPresigner.Presign(new S3SigV4aPresignParameters
        {
            HttpMethod = "GET",
            Path = "/test",
            AccessKeyId = "AKID",
            SecretAccessKey = "secret",
            Service = "s3",
            RegionSet = ["us-east-1", "eu-west-1"],
            SignedAtUtc = DateTimeOffset.UtcNow,
            ExpiresInSeconds = 300,
            SignedHeaders = ["host"],
            Headers = [new("host", "localhost")]
        });

        Assert.Contains(result.QueryParameters, qp => qp.Key == "X-Amz-Region-Set" && qp.Value == "us-east-1,eu-west-1");
    }

    [Fact]
    public void Presign_CredentialScopeHasNoRegion()
    {
        var result = S3SigV4aPresigner.Presign(new S3SigV4aPresignParameters
        {
            HttpMethod = "GET",
            Path = "/test",
            AccessKeyId = "AKID",
            SecretAccessKey = "secret",
            Service = "s3",
            RegionSet = ["us-east-1"],
            SignedAtUtc = new DateTimeOffset(2024, 6, 15, 0, 0, 0, TimeSpan.Zero),
            ExpiresInSeconds = 300,
            SignedHeaders = ["host"],
            Headers = [new("host", "localhost")]
        });

        var credentialParam = result.QueryParameters.First(qp => qp.Key == "X-Amz-Credential").Value!;
        // Should be AKID/20240615/s3/aws4_request (no region)
        Assert.Equal("AKID/20240615/s3/aws4_request", credentialParam);
    }

    [Fact]
    public void Presign_SignatureCanBeVerified()
    {
        var parameters = new S3SigV4aPresignParameters
        {
            HttpMethod = "GET",
            Path = "/my-bucket/my-key",
            AccessKeyId = "AKIDEXAMPLE",
            SecretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            Service = "s3",
            RegionSet = ["us-east-1"],
            SignedAtUtc = new DateTimeOffset(2024, 1, 15, 12, 0, 0, TimeSpan.Zero),
            ExpiresInSeconds = 3600,
            SignedHeaders = ["host"],
            Headers = [new("host", "s3.amazonaws.com")]
        };

        var result = S3SigV4aPresigner.Presign(parameters);

        // Verify: derive the same key and verify the signature
        using var ecdsaKey = S3SigV4aSigner.DeriveEcdsaKey(parameters.SecretAccessKey, parameters.AccessKeyId);
        var credentialScopeString = S3SigV4aSigner.BuildCredentialScopeString(
            result.CredentialScope.DateStamp, parameters.Service);
        var stringToSign = S3SigV4aSigner.BuildStringToSign(
            result.SignedAtUtc, credentialScopeString, result.CanonicalRequest.CanonicalRequestHashHex);

        Assert.True(S3SigV4aSigner.VerifySignature(ecdsaKey, stringToSign, result.Signature));
    }

    [Fact]
    public void Presign_SecurityToken_Included()
    {
        var result = S3SigV4aPresigner.Presign(new S3SigV4aPresignParameters
        {
            HttpMethod = "GET",
            Path = "/test",
            AccessKeyId = "AKID",
            SecretAccessKey = "secret",
            Service = "s3",
            RegionSet = ["*"],
            SignedAtUtc = DateTimeOffset.UtcNow,
            ExpiresInSeconds = 300,
            SignedHeaders = ["host"],
            Headers = [new("host", "localhost")],
            SecurityToken = "session-token-value"
        });

        Assert.Contains(result.QueryParameters, qp => qp.Key == "X-Amz-Security-Token" && qp.Value == "session-token-value");
    }

    [Fact]
    public void Presign_ExpiresAtUtc_IsCorrect()
    {
        var signedAt = new DateTimeOffset(2024, 6, 15, 12, 0, 0, TimeSpan.Zero);
        var result = S3SigV4aPresigner.Presign(new S3SigV4aPresignParameters
        {
            HttpMethod = "GET",
            Path = "/test",
            AccessKeyId = "AKID",
            SecretAccessKey = "secret",
            Service = "s3",
            RegionSet = ["*"],
            SignedAtUtc = signedAt,
            ExpiresInSeconds = 7200,
            SignedHeaders = ["host"],
            Headers = [new("host", "localhost")]
        });

        Assert.Equal(signedAt.AddSeconds(7200), result.ExpiresAtUtc);
    }

    [Fact]
    public void Presign_EmptyRegionSet_Throws()
    {
        Assert.Throws<ArgumentException>(() => S3SigV4aPresigner.Presign(new S3SigV4aPresignParameters
        {
            HttpMethod = "GET",
            Path = "/test",
            AccessKeyId = "AKID",
            SecretAccessKey = "secret",
            Service = "s3",
            RegionSet = [],
            SignedAtUtc = DateTimeOffset.UtcNow,
            ExpiresInSeconds = 300,
            SignedHeaders = ["host"],
            Headers = [new("host", "localhost")]
        }));
    }

    [Fact]
    public void Presign_ZeroExpiry_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => S3SigV4aPresigner.Presign(new S3SigV4aPresignParameters
        {
            HttpMethod = "GET",
            Path = "/test",
            AccessKeyId = "AKID",
            SecretAccessKey = "secret",
            Service = "s3",
            SignedAtUtc = DateTimeOffset.UtcNow,
            ExpiresInSeconds = 0,
            SignedHeaders = ["host"],
            Headers = [new("host", "localhost")]
        }));
    }

    [Fact]
    public void Presign_NoSignedHeaders_Throws()
    {
        Assert.Throws<ArgumentException>(() => S3SigV4aPresigner.Presign(new S3SigV4aPresignParameters
        {
            HttpMethod = "GET",
            Path = "/test",
            AccessKeyId = "AKID",
            SecretAccessKey = "secret",
            Service = "s3",
            SignedAtUtc = DateTimeOffset.UtcNow,
            ExpiresInSeconds = 300,
            SignedHeaders = [],
            Headers = [new("host", "localhost")]
        }));
    }
}
