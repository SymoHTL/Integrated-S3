using System.Text;
using System.Xml.Linq;
using IntegratedS3.Protocol;
using Xunit;

namespace IntegratedS3.E2E.Tests;

/// <summary>
/// Cheap, offline property / round-trip tests for the wire protocol: S3 XML write→reparse and
/// request-parse fidelity, plus SigV4 canonicalization invariants and SigV4a sign/verify round trips.
/// These catch serializer and canonicalization regressions without booting a host.
/// </summary>
[Trait("Suite", "Smoke")]
public sealed class S3ProtocolPropertyTests
{
    private const string S3Ns = "http://s3.amazonaws.com/doc/2006-03-01/";

    [Theory]
    [InlineData(1)]
    [InlineData(10)]
    [InlineData(100)]
    public void ListBucketResult_WriteThenReparse_PreservesEveryKey(int keyCount)
    {
        var expectedKeys = Enumerable.Range(0, keyCount).Select(i => $"prefix/object-{i:D5}.bin").ToArray();
        var result = new S3ListBucketResult
        {
            Name = "prop-bucket",
            IsV2 = true,
            KeyCount = keyCount,
            MaxKeys = 1000,
            Contents = expectedKeys.Select(k => new S3ListBucketObject
            {
                Key = k,
                ETag = "\"5d41402abc4b2a76b9719d911017c592\"",
                Size = 1,
                LastModifiedUtc = new DateTimeOffset(2026, 7, 4, 0, 0, 0, TimeSpan.Zero),
            }).ToArray(),
        };

        var xml = S3XmlResponseWriter.WriteListBucketResult(result);
        var document = XDocument.Parse(xml);
        XNamespace ns = S3Ns;
        var parsedKeys = document.Descendants(ns + "Contents").Select(c => c.Element(ns + "Key")!.Value).ToArray();

        Assert.Equal(expectedKeys, parsedKeys);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(50)]
    [InlineData(500)]
    public async Task CompleteMultipartUpload_Parse_PreservesPartNumbers(int partCount)
    {
        var builder = new StringBuilder().Append($"<CompleteMultipartUpload xmlns=\"{S3Ns}\">");
        for (var i = 1; i <= partCount; i++)
        {
            builder.Append("<Part><PartNumber>").Append(i).Append("</PartNumber><ETag>&quot;etag-")
                .Append(i).Append("&quot;</ETag></Part>");
        }

        builder.Append("</CompleteMultipartUpload>");

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(builder.ToString()));
        var parsed = await S3XmlRequestReader.ReadCompleteMultipartUploadRequestAsync(stream);

        Assert.Equal(partCount, parsed.Parts.Count);
        Assert.Equal(Enumerable.Range(1, partCount), parsed.Parts.Select(p => p.PartNumber));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(25)]
    [InlineData(250)]
    public async Task DeleteObjects_Parse_PreservesEveryKey(int keyCount)
    {
        var expectedKeys = Enumerable.Range(0, keyCount).Select(i => $"folder/key-{i:D4}").ToArray();
        var builder = new StringBuilder().Append($"<Delete xmlns=\"{S3Ns}\">");
        foreach (var key in expectedKeys)
        {
            builder.Append("<Object><Key>").Append(key).Append("</Key></Object>");
        }

        builder.Append("<Quiet>false</Quiet></Delete>");

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(builder.ToString()));
        var parsed = await S3XmlRequestReader.ReadDeleteObjectsRequestAsync(stream);

        Assert.Equal(expectedKeys, parsed.Objects.Select(o => o.Key).ToArray());
    }

    [Fact]
    public void SigV4_CanonicalRequest_IsInvariantToHeaderAndQueryOrder()
    {
        const string payloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        string[] signed = ["host", "x-amz-content-sha256", "x-amz-date"];

        KeyValuePair<string, string?>[] headersA =
        [
            new("host", "bucket.s3.amazonaws.com"),
            new("x-amz-content-sha256", payloadHash),
            new("x-amz-date", "20260704T120000Z"),
        ];
        KeyValuePair<string, string?>[] headersB =
        [
            new("x-amz-date", "20260704T120000Z"),
            new("host", "bucket.s3.amazonaws.com"),
            new("x-amz-content-sha256", payloadHash),
        ];
        KeyValuePair<string, string?>[] queryA = [new("prefix", "a"), new("max-keys", "10")];
        KeyValuePair<string, string?>[] queryB = [new("max-keys", "10"), new("prefix", "a")];

        var a = S3SigV4Signer.BuildCanonicalRequest("GET", "/bucket", queryA, headersA, signed, payloadHash);
        var b = S3SigV4Signer.BuildCanonicalRequest("GET", "/bucket", queryB, headersB, signed, payloadHash);

        Assert.Equal(a.CanonicalRequestHashHex, b.CanonicalRequestHashHex);
        Assert.Equal(a.CanonicalRequest, b.CanonicalRequest);
    }

    [Fact]
    public void SigV4_ComputeSignature_IsDeterministic()
    {
        var scope = new S3SigV4CredentialScope
        {
            AccessKeyId = "AKIAEXAMPLE",
            DateStamp = "20260704",
            Region = "us-east-1",
            Service = "s3",
            Terminator = "aws4_request",
        };
        const string stringToSign = "AWS4-HMAC-SHA256\n20260704T120000Z\n20260704/us-east-1/s3/aws4_request\nabc123";
        const string secret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";

        var first = S3SigV4Signer.ComputeSignature(secret, scope, stringToSign);
        var second = S3SigV4Signer.ComputeSignature(secret, scope, stringToSign);

        Assert.Equal(first, second);
        Assert.Equal(64, first.Length); // hex-encoded HMAC-SHA256
    }

    [Fact]
    public void SigV4a_SignThenVerify_RoundTripsAndRejectsTampering()
    {
        using var key = S3SigV4aSigner.DeriveEcdsaKey("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "AKIAEXAMPLE");
        var scope = S3SigV4aSigner.BuildCredentialScopeString("20260704", "s3");
        var stringToSign = S3SigV4aSigner.BuildStringToSign(
            new DateTimeOffset(2026, 7, 4, 12, 0, 0, TimeSpan.Zero),
            scope,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");

        var signature = S3SigV4aSigner.ComputeSignature(key, stringToSign);

        Assert.True(S3SigV4aSigner.VerifySignature(key, stringToSign, signature));
        Assert.False(S3SigV4aSigner.VerifySignature(key, stringToSign + "tampered", signature));
    }
}
