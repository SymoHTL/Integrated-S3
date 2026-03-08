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
}
