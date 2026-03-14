using IntegratedS3.Protocol;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class IntegratedS3SigV4ProtocolTests
{
    [Fact]
    public void CanonicalRequestBuilder_NormalizesPathQueryAndHeaders()
    {
        var canonicalRequest = S3SigV4Signer.BuildCanonicalRequest(
            "GET",
            "/integrated-s3/demo bucket/object.txt",
            [
                new KeyValuePair<string, string?>("b", "two words"),
                new KeyValuePair<string, string?>("a", "1")
            ],
            [
                new KeyValuePair<string, string?>("x-amz-date", "20260308T120000Z"),
                new KeyValuePair<string, string?>("host", "example.test")
            ],
            ["host", "x-amz-date"],
            "UNSIGNED-PAYLOAD");

        var expected = string.Join('\n', [
            "GET",
            "/integrated-s3/demo%20bucket/object.txt",
            "a=1&b=two%20words",
            "host:example.test\nx-amz-date:20260308T120000Z\n",
            "host;x-amz-date",
            "UNSIGNED-PAYLOAD"
        ]);

        Assert.Equal(expected, canonicalRequest.CanonicalRequest);
    }

    [Fact]
    public void CanonicalRequestBuilder_PreservesEmptySubresourceQueryValues()
    {
        var canonicalRequest = S3SigV4Signer.BuildCanonicalRequest(
            "GET",
            "/integrated-s3/demo-bucket",
            [
                new KeyValuePair<string, string?>("X-Amz-Date", "20260311T180000Z"),
                new KeyValuePair<string, string?>("X-Amz-Expires", "300"),
                new KeyValuePair<string, string?>("versioning", string.Empty)
            ],
            [
                new KeyValuePair<string, string?>("host", "example.test")
            ],
            ["host"],
            "UNSIGNED-PAYLOAD");

        var expected = string.Join('\n', [
            "GET",
            "/integrated-s3/demo-bucket",
            "X-Amz-Date=20260311T180000Z&X-Amz-Expires=300&versioning=",
            "host:example.test\n",
            "host",
            "UNSIGNED-PAYLOAD"
        ]);

        Assert.Equal(expected, canonicalRequest.CanonicalRequest);
    }

    [Fact]
    public void CanonicalRequestBuilder_PreservesLiteralPlusSignsAndDuplicateRawQueryParameters()
    {
        var canonicalRequest = S3SigV4Signer.BuildCanonicalRequest(
            "GET",
            "/integrated-s3/demo-bucket/object.txt",
            S3SigV4QueryStringParser.Parse("?x-id=GetObject+Test&x-id=GetObject%2BSecond&uploads"),
            [
                new KeyValuePair<string, string?>("host", "example.test")
            ],
            ["host"],
            "UNSIGNED-PAYLOAD");

        var expected = string.Join('\n', [
            "GET",
            "/integrated-s3/demo-bucket/object.txt",
            "uploads=&x-id=GetObject%2BSecond&x-id=GetObject%2BTest",
            "host:example.test\n",
            "host",
            "UNSIGNED-PAYLOAD"
        ]);

        Assert.Equal(expected, canonicalRequest.CanonicalRequest);
    }

    [Fact]
    public void Presigner_BuildsExpectedQueryParametersAndExpiry()
    {
        var signedAtUtc = new DateTimeOffset(2026, 3, 11, 18, 0, 0, TimeSpan.Zero);
        var presignedRequest = S3SigV4Presigner.Presign(new S3SigV4PresignParameters
        {
            HttpMethod = "GET",
            Path = "/integrated-s3/buckets/demo-bucket/objects/docs/readme.txt",
            Headers =
            [
                new KeyValuePair<string, string?>("host", "example.test")
            ],
            SignedHeaders = ["host"],
            AccessKeyId = "demo-access",
            SecretAccessKey = "demo-secret",
            Region = "us-east-1",
            Service = "s3",
            SignedAtUtc = signedAtUtc,
            ExpiresInSeconds = 300
        });

        Assert.Equal("demo-access", presignedRequest.CredentialScope.AccessKeyId);
        Assert.Equal(signedAtUtc.AddMinutes(5), presignedRequest.ExpiresAtUtc);
        Assert.Contains(presignedRequest.QueryParameters, static pair => pair.Key == "X-Amz-Algorithm" && pair.Value == "AWS4-HMAC-SHA256");
        Assert.Contains(presignedRequest.QueryParameters, static pair => pair.Key == "X-Amz-SignedHeaders" && pair.Value == "host");
        Assert.Contains(presignedRequest.QueryParameters, static pair => pair.Key == "X-Amz-Signature" && !string.IsNullOrWhiteSpace(pair.Value));
    }
}
