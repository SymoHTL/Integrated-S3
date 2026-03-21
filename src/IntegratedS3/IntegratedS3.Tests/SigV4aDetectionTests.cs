using IntegratedS3.Protocol;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class SigV4aDetectionTests
{
    [Fact]
    public void TryParseAuthorizationHeader_ParsesSigV4a_Successfully()
    {
        var header = "AWS4-ECDSA-P256-SHA256 Credential=AKID/20240101/s3/aws4_request, SignedHeaders=host, Signature=abc123";
        var headers = new[] { new KeyValuePair<string, string?>("x-amz-region-set", "us-east-1") };

        var found = S3SigV4RequestParser.TryParseAuthorizationHeader(header, headers, out var authorization, out var error);

        Assert.True(found);
        Assert.NotNull(authorization);
        Assert.Null(error);
        Assert.Equal("AWS4-ECDSA-P256-SHA256", authorization.Algorithm);
        Assert.Equal("AKID", authorization.CredentialScope.AccessKeyId);
        Assert.Equal("20240101", authorization.CredentialScope.DateStamp);
        Assert.Equal("*", authorization.CredentialScope.Region);
        Assert.Equal("s3", authorization.CredentialScope.Service);
        Assert.Equal("aws4_request", authorization.CredentialScope.Terminator);
        Assert.Equal(["host"], authorization.SignedHeaders);
        Assert.Equal("abc123", authorization.Signature);
        Assert.NotNull(authorization.RegionSet);
        Assert.Equal(["us-east-1"], authorization.RegionSet);
    }

    [Fact]
    public void TryParseAuthorizationHeader_StandardSigV4_StillWorks()
    {
        var header = "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc123";

        var found = S3SigV4RequestParser.TryParseAuthorizationHeader(header, out var authorization, out var error);

        Assert.True(found);
        Assert.NotNull(authorization);
        Assert.Null(error);
        Assert.Equal("AWS4-HMAC-SHA256", authorization.Algorithm);
        Assert.Null(authorization.RegionSet);
    }

    [Fact]
    public void TryParsePresignedRequest_ParsesSigV4a_Successfully()
    {
        var queryParams = new Dictionary<string, string?>
        {
            ["X-Amz-Algorithm"] = "AWS4-ECDSA-P256-SHA256",
            ["X-Amz-Credential"] = "AKID/20240101/s3/aws4_request",
            ["X-Amz-Date"] = "20240101T000000Z",
            ["X-Amz-Expires"] = "3600",
            ["X-Amz-SignedHeaders"] = "host",
            ["X-Amz-Signature"] = "abc123",
            ["X-Amz-Region-Set"] = "us-east-1,eu-west-1"
        };

        var found = S3SigV4RequestParser.TryParsePresignedRequest(queryParams, out var presigned, out var error);

        Assert.True(found);
        Assert.NotNull(presigned);
        Assert.Null(error);
        Assert.Equal("AWS4-ECDSA-P256-SHA256", presigned.Algorithm);
        Assert.Equal("AKID", presigned.CredentialScope.AccessKeyId);
        Assert.Equal("20240101", presigned.CredentialScope.DateStamp);
        Assert.Equal("*", presigned.CredentialScope.Region);
        Assert.Equal("s3", presigned.CredentialScope.Service);
        Assert.NotNull(presigned.RegionSet);
        Assert.Equal(["us-east-1", "eu-west-1"], presigned.RegionSet);
        Assert.Null(presigned.SecurityToken);
    }

    [Fact]
    public void IsSigV4aRequest_DetectsHeaderBased()
    {
        Assert.True(S3SigV4RequestParser.IsSigV4aRequest("AWS4-ECDSA-P256-SHA256 Credential=...", null));
        Assert.True(S3SigV4RequestParser.IsSigV4aRequest("  AWS4-ECDSA-P256-SHA256 Credential=...  ", null));
        Assert.False(S3SigV4RequestParser.IsSigV4aRequest("AWS4-HMAC-SHA256 Credential=...", null));
        Assert.False(S3SigV4RequestParser.IsSigV4aRequest(null, null));
    }

    [Fact]
    public void IsSigV4aRequest_DetectsQueryBased()
    {
        Assert.True(S3SigV4RequestParser.IsSigV4aRequest(null, "AWS4-ECDSA-P256-SHA256"));
        Assert.True(S3SigV4RequestParser.IsSigV4aRequest(null, "  AWS4-ECDSA-P256-SHA256  "));
        Assert.False(S3SigV4RequestParser.IsSigV4aRequest(null, "AWS4-HMAC-SHA256"));
    }

    [Fact]
    public void TryParseAuthorizationHeader_UnknownAlgorithm_ReturnsGenericError()
    {
        var header = "AWS4-UNKNOWN Credential=...";

        var found = S3SigV4RequestParser.TryParseAuthorizationHeader(header, out var authorization, out var error);

        Assert.True(found);
        Assert.Null(authorization);
        Assert.NotNull(error);
        Assert.Contains("AWS4-HMAC-SHA256", error);
    }

    [Fact]
    public void TryParseAuthorizationHeader_SigV4a_WildcardRegionSet()
    {
        var header = "AWS4-ECDSA-P256-SHA256 Credential=AKID/20240101/s3/aws4_request, SignedHeaders=host, Signature=abc123";
        var headers = new[] { new KeyValuePair<string, string?>("x-amz-region-set", "*") };

        var found = S3SigV4RequestParser.TryParseAuthorizationHeader(header, headers, out var authorization, out var error);

        Assert.True(found);
        Assert.NotNull(authorization);
        Assert.Null(error);
        Assert.NotNull(authorization.RegionSet);
        Assert.Equal(["*"], authorization.RegionSet);
    }

    [Fact]
    public void TryParseAuthorizationHeader_SigV4a_InvalidCredentialScope_ReturnsError()
    {
        var header = "AWS4-ECDSA-P256-SHA256 Credential=AKID/20240101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc123";

        var found = S3SigV4RequestParser.TryParseAuthorizationHeader(header, [], out var authorization, out var error);

        Assert.True(found);
        Assert.Null(authorization);
        Assert.NotNull(error);
        Assert.Contains("SigV4a credential scope", error);
    }

    [Fact]
    public void TryParseAuthorizationHeader_SigV4a_NoRegionSetHeader_ReturnsNullRegionSet()
    {
        var header = "AWS4-ECDSA-P256-SHA256 Credential=AKID/20240101/s3/aws4_request, SignedHeaders=host, Signature=abc123";

        var found = S3SigV4RequestParser.TryParseAuthorizationHeader(header, [], out var authorization, out var error);

        Assert.True(found);
        Assert.NotNull(authorization);
        Assert.Null(error);
        Assert.Null(authorization.RegionSet);
    }

    [Fact]
    public void TryParsePresignedRequest_SigV4a_NoRegionSet_ReturnsNullRegionSet()
    {
        var queryParams = new Dictionary<string, string?>
        {
            ["X-Amz-Algorithm"] = "AWS4-ECDSA-P256-SHA256",
            ["X-Amz-Credential"] = "AKID/20240101/s3/aws4_request",
            ["X-Amz-Date"] = "20240101T000000Z",
            ["X-Amz-Expires"] = "3600",
            ["X-Amz-SignedHeaders"] = "host",
            ["X-Amz-Signature"] = "abc123"
        };

        var found = S3SigV4RequestParser.TryParsePresignedRequest(queryParams, out var presigned, out var error);

        Assert.True(found);
        Assert.NotNull(presigned);
        Assert.Null(error);
        Assert.Null(presigned.RegionSet);
    }
}
