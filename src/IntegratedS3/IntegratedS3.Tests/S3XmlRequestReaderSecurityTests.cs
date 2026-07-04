using System.Text;
using IntegratedS3.Protocol;
using Xunit;

namespace IntegratedS3.Tests;

/// <summary>
/// Regression tests for issue #104: S3 XML request readers must reject inline DTDs and refuse
/// to expand internal entities, preventing "billion laughs" entity-expansion denial-of-service.
/// </summary>
public sealed class S3XmlRequestReaderSecurityTests
{
    // Classic "billion laughs" payload: a tiny body that, when entities are expanded, amplifies
    // into a huge string. A hardened parser rejects the DOCTYPE before any expansion happens.
    private const string BillionLaughsDelete = """
        <?xml version="1.0"?>
        <!DOCTYPE Delete [
          <!ENTITY lol "lolololololololololol">
          <!ENTITY lol1 "&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;">
          <!ENTITY lol2 "&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;">
          <!ENTITY lol3 "&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;">
        ]>
        <Delete><Object><Key>&lol3;</Key></Object></Delete>
        """;

    // A minimal DOCTYPE with no entity payload at all — still an inline DTD, which AWS S3 rejects.
    private const string BareDoctypeVersioning = """
        <?xml version="1.0"?>
        <!DOCTYPE VersioningConfiguration>
        <VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>
        """;

    private static Stream ToStream(string xml) => new MemoryStream(Encoding.UTF8.GetBytes(xml));

    [Fact]
    public async Task ReadDeleteObjectsRequestAsync_RejectsBillionLaughsPayload()
    {
        await using var stream = ToStream(BillionLaughsDelete);

        // The reader wraps the underlying XmlException (DTD prohibited) as FormatException; the
        // key property is that it THROWS during parse rather than expanding the entity into MBs.
        await Assert.ThrowsAsync<FormatException>(
            () => S3XmlRequestReader.ReadDeleteObjectsRequestAsync(stream));
    }

    [Fact]
    public async Task ReadDeleteObjectsRequestAsync_DoesNotExpandEntities()
    {
        // Guard against a regression where the DTD is tolerated but entities silently expand:
        // if any expansion occurred the resulting Key would be enormous. We assert rejection.
        await using var stream = ToStream(BillionLaughsDelete);

        var exception = await Assert.ThrowsAsync<FormatException>(
            () => S3XmlRequestReader.ReadDeleteObjectsRequestAsync(stream));

        Assert.NotNull(exception);
    }

    [Fact]
    public async Task ReadBucketVersioningConfigurationAsync_RejectsInlineDtd()
    {
        await using var stream = ToStream(BareDoctypeVersioning);

        await Assert.ThrowsAsync<FormatException>(
            () => S3XmlRequestReader.ReadBucketVersioningConfigurationAsync(stream));
    }

    [Fact]
    public async Task ReadCompleteMultipartUploadRequestAsync_RejectsInlineDtd()
    {
        const string payload = """
            <?xml version="1.0"?>
            <!DOCTYPE CompleteMultipartUpload [ <!ENTITY x "y"> ]>
            <CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>&x;</ETag></Part></CompleteMultipartUpload>
            """;
        await using var stream = ToStream(payload);

        await Assert.ThrowsAsync<FormatException>(
            () => S3XmlRequestReader.ReadCompleteMultipartUploadRequestAsync(stream));
    }

    [Fact]
    public async Task ReadDeleteObjectsRequestAsync_AcceptsWellFormedBodyWithoutDtd()
    {
        // Sanity check: hardening must not break legitimate DTD-free request bodies.
        const string payload = """
            <?xml version="1.0"?>
            <Delete><Object><Key>example.txt</Key></Object></Delete>
            """;
        await using var stream = ToStream(payload);

        var request = await S3XmlRequestReader.ReadDeleteObjectsRequestAsync(stream);

        Assert.Single(request.Objects);
        Assert.Equal("example.txt", request.Objects[0].Key);
    }
}
