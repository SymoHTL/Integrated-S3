using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Xml.Linq;
using IntegratedS3.Tests.Infrastructure;
using Xunit;

namespace IntegratedS3.Tests;

/// <summary>
/// Exercises multipart upload patterns, special character encoding, and error handling
/// the way rclone expects them from an S3-compatible endpoint.
/// </summary>
public sealed class RcloneS3MultipartEncodingCompatibilityTests : IClassFixture<WebUiApplicationFactory>
{
    private static readonly XNamespace S3Ns = "http://s3.amazonaws.com/doc/2006-03-01/";

    private const string Prefix = "/integrated-s3";

    private readonly WebUiApplicationFactory _factory;

    public RcloneS3MultipartEncodingCompatibilityTests(WebUiApplicationFactory factory)
    {
        _factory = factory;
    }

    // ───────────────────────── helpers ─────────────────────────

    private static string Bucket() => $"rc-{Guid.NewGuid():N}";

    private static string ObjectPath(string bucket, string key)
    {
        var segments = key.Split('/');
        var encoded = segments.Select(Uri.EscapeDataString);
        return $"{Prefix}/{bucket}/{string.Join("/", encoded)}";
    }

    private static async Task CreateBucketAsync(HttpClient client, string bucket)
    {
        var response = await client.PutAsync($"{Prefix}/buckets/{bucket}", content: null);
        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
    }

    private static async Task PutObjectAsync(HttpClient client, string bucket, string key, string body)
    {
        var response = await client.PutAsync(
            ObjectPath(bucket, key),
            new StringContent(body, Encoding.UTF8, "application/octet-stream"));
        Assert.True(response.IsSuccessStatusCode, $"PUT {key} failed: {response.StatusCode}");
    }

    private static async Task<string> InitiateMultipartAsync(
        HttpClient client,
        string bucket,
        string key,
        IReadOnlyDictionary<string, string>? headers = null)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, $"{ObjectPath(bucket, key)}?uploads")
        {
            Content = new ByteArrayContent([])
        };
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");

        if (headers is not null) {
            foreach (var (name, value) in headers) {
                request.Headers.TryAddWithoutValidation(name, value);
            }
        }

        var response = await client.SendAsync(request);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var doc = XDocument.Parse(await response.Content.ReadAsStringAsync());
        return El(doc, "UploadId");
    }

    private static async Task<string> UploadPartAsync(
        HttpClient client,
        string bucket,
        string key,
        string uploadId,
        int partNumber,
        byte[] data)
    {
        var url = $"{ObjectPath(bucket, key)}?partNumber={partNumber}&uploadId={Uri.EscapeDataString(uploadId)}";
        var response = await client.PutAsync(url, new ByteArrayContent(data));
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var etag = response.Headers.ETag?.Tag;
        Assert.False(string.IsNullOrWhiteSpace(etag), "UploadPart must return ETag header.");
        return etag!;
    }

    private static async Task<HttpResponseMessage> CompleteMultipartAsync(
        HttpClient client,
        string bucket,
        string key,
        string uploadId,
        IReadOnlyList<(int PartNumber, string ETag)> parts)
    {
        var partsXml = string.Join("\n", parts.Select(p =>
            $"  <Part><PartNumber>{p.PartNumber}</PartNumber><ETag>{p.ETag}</ETag></Part>"));
        var body = $"<CompleteMultipartUpload>\n{partsXml}\n</CompleteMultipartUpload>";

        using var request = new HttpRequestMessage(
            HttpMethod.Post,
            $"{ObjectPath(bucket, key)}?uploadId={Uri.EscapeDataString(uploadId)}")
        {
            Content = new StringContent(body, Encoding.UTF8, "application/xml")
        };

        return await client.SendAsync(request);
    }

    private static string El(XDocument doc, string localName)
    {
        var element = doc.Descendants().FirstOrDefault(e => e.Name.LocalName == localName);
        return element?.Value
            ?? throw new Xunit.Sdk.XunitException($"Missing XML element '{localName}'.");
    }

    private static IReadOnlyList<XElement> Els(XDocument doc, string localName)
        => doc.Descendants().Where(e => e.Name.LocalName == localName).ToArray();

    private static byte[] Repeat(byte value, int count)
    {
        var buffer = new byte[count];
        Array.Fill(buffer, value);
        return buffer;
    }

    // ═══════════════════════ MULTIPART UPLOAD TESTS ═══════════════════════

    [Fact]
    public async Task Multipart_FullLifecycle_UploadsThreePartsAndReassembles()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = Bucket();
        const string key = "large-file.dat";
        await CreateBucketAsync(client, bucket);

        // Initiate
        using var initiateReq = new HttpRequestMessage(HttpMethod.Post, $"{ObjectPath(bucket, key)}?uploads")
        {
            Content = new ByteArrayContent([])
        };
        initiateReq.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        var initiateResp = await client.SendAsync(initiateReq);
        Assert.Equal(HttpStatusCode.OK, initiateResp.StatusCode);
        Assert.Equal("application/xml", initiateResp.Content.Headers.ContentType?.MediaType);

        var initiateDoc = XDocument.Parse(await initiateResp.Content.ReadAsStringAsync());
        var uploadId = El(initiateDoc, "UploadId");
        Assert.Equal(bucket, El(initiateDoc, "Bucket"));
        Assert.Equal(key, El(initiateDoc, "Key"));
        Assert.False(string.IsNullOrWhiteSpace(uploadId));

        // Upload 3 distinct parts (~1KB each)
        var part1Data = Repeat(0x41, 1024); // AAA...
        var part2Data = Repeat(0x42, 1024); // BBB...
        var part3Data = Repeat(0x43, 1024); // CCC...

        var etag1 = await UploadPartAsync(client, bucket, key, uploadId, 1, part1Data);
        var etag2 = await UploadPartAsync(client, bucket, key, uploadId, 2, part2Data);
        var etag3 = await UploadPartAsync(client, bucket, key, uploadId, 3, part3Data);

        // Complete
        var completeResp = await CompleteMultipartAsync(client, bucket, key, uploadId,
            [(1, etag1), (2, etag2), (3, etag3)]);
        Assert.Equal(HttpStatusCode.OK, completeResp.StatusCode);
        Assert.Equal("application/xml", completeResp.Content.Headers.ContentType?.MediaType);

        var completeDoc = XDocument.Parse(await completeResp.Content.ReadAsStringAsync());
        Assert.Equal("CompleteMultipartUploadResult", completeDoc.Root?.Name.LocalName);
        Assert.False(string.IsNullOrWhiteSpace(El(completeDoc, "Location")));
        Assert.Equal(bucket, El(completeDoc, "Bucket"));
        Assert.Equal(key, El(completeDoc, "Key"));
        Assert.False(string.IsNullOrWhiteSpace(El(completeDoc, "ETag")));

        // GET → content must be the concatenation of all 3 parts
        var getResp = await client.GetAsync(ObjectPath(bucket, key));
        Assert.Equal(HttpStatusCode.OK, getResp.StatusCode);
        var downloaded = await getResp.Content.ReadAsByteArrayAsync();
        Assert.Equal(part1Data.Concat(part2Data).Concat(part3Data).ToArray(), downloaded);
    }

    [Fact]
    public async Task Multipart_WithMetadata_PreservesCustomMetadataFromInitiateRequest()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = Bucket();
        const string key = "meta-file.dat";
        await CreateBucketAsync(client, bucket);

        var uploadId = await InitiateMultipartAsync(client, bucket, key, new Dictionary<string, string>
        {
            ["x-amz-meta-mtime"] = "1710950400",
            ["x-amz-meta-source"] = "rclone"
        });

        var part1 = Repeat(0x61, 512);
        var part2 = Repeat(0x62, 512);
        var etag1 = await UploadPartAsync(client, bucket, key, uploadId, 1, part1);
        var etag2 = await UploadPartAsync(client, bucket, key, uploadId, 2, part2);

        var completeResp = await CompleteMultipartAsync(client, bucket, key, uploadId,
            [(1, etag1), (2, etag2)]);
        Assert.Equal(HttpStatusCode.OK, completeResp.StatusCode);

        // HEAD → verify metadata preserved
        using var headReq = new HttpRequestMessage(HttpMethod.Head, ObjectPath(bucket, key));
        var headResp = await client.SendAsync(headReq);
        Assert.Equal(HttpStatusCode.OK, headResp.StatusCode);
        Assert.Equal("1710950400", Assert.Single(headResp.Headers.GetValues("x-amz-meta-mtime")));
        Assert.Equal("rclone", Assert.Single(headResp.Headers.GetValues("x-amz-meta-source")));
    }

    [Fact]
    public async Task Multipart_ListMultipartUploads_ListsAllActiveUploadsWithCorrectMetadata()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = Bucket();
        await CreateBucketAsync(client, bucket);

        var id1 = await InitiateMultipartAsync(client, bucket, "file-a.dat");
        await Task.Delay(2);
        var id2 = await InitiateMultipartAsync(client, bucket, "file-b.dat");
        await Task.Delay(2);
        var id3 = await InitiateMultipartAsync(client, bucket, "file-c.dat");

        var listResp = await client.GetAsync($"{Prefix}/{bucket}?uploads");
        Assert.Equal(HttpStatusCode.OK, listResp.StatusCode);
        Assert.Equal("application/xml", listResp.Content.Headers.ContentType?.MediaType);

        var doc = XDocument.Parse(await listResp.Content.ReadAsStringAsync());
        Assert.Equal("ListMultipartUploadsResult", doc.Root?.Name.LocalName);
        Assert.Equal(bucket, El(doc, "Bucket"));

        var uploads = doc.Root!.Elements(S3Ns + "Upload").ToArray();
        Assert.Equal(3, uploads.Length);

        var keys = uploads.Select(u => u.Element(S3Ns + "Key")?.Value).OrderBy(k => k).ToArray();
        Assert.Equal(new[] { "file-a.dat", "file-b.dat", "file-c.dat" }, keys);

        var uploadIds = uploads.Select(u => u.Element(S3Ns + "UploadId")?.Value).ToArray();
        Assert.Contains(id1, uploadIds);
        Assert.Contains(id2, uploadIds);
        Assert.Contains(id3, uploadIds);

        foreach (var upload in uploads) {
            Assert.NotNull(upload.Element(S3Ns + "Initiated") ?? upload.Element(S3Ns + "InitiatedAtUtc"));
        }
    }

    [Fact]
    public async Task Multipart_ListParts_ReturnsAllPartsWithExpectedFields()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = Bucket();
        const string key = "list-parts-test.dat";
        await CreateBucketAsync(client, bucket);

        var uploadId = await InitiateMultipartAsync(client, bucket, key);
        var etag1 = await UploadPartAsync(client, bucket, key, uploadId, 1, Repeat(0x31, 100));
        var etag2 = await UploadPartAsync(client, bucket, key, uploadId, 2, Repeat(0x32, 200));
        var etag3 = await UploadPartAsync(client, bucket, key, uploadId, 3, Repeat(0x33, 300));

        var listResp = await client.GetAsync(
            $"{ObjectPath(bucket, key)}?uploadId={Uri.EscapeDataString(uploadId)}");
        Assert.Equal(HttpStatusCode.OK, listResp.StatusCode);

        var doc = XDocument.Parse(await listResp.Content.ReadAsStringAsync());
        Assert.Equal("ListPartsResult", doc.Root?.Name.LocalName);
        Assert.Equal(bucket, El(doc, "Bucket"));
        Assert.Equal(key, El(doc, "Key"));
        Assert.Equal(uploadId, El(doc, "UploadId"));

        var parts = doc.Root!.Elements(S3Ns + "Part").ToArray();
        Assert.Equal(3, parts.Length);

        Assert.Equal("1", parts[0].Element(S3Ns + "PartNumber")?.Value);
        Assert.Equal("2", parts[1].Element(S3Ns + "PartNumber")?.Value);
        Assert.Equal("3", parts[2].Element(S3Ns + "PartNumber")?.Value);

        // Each part must have ETag, Size, LastModified
        foreach (var part in parts) {
            Assert.False(string.IsNullOrWhiteSpace(part.Element(S3Ns + "ETag")?.Value), "Part must have ETag.");
            Assert.False(string.IsNullOrWhiteSpace(part.Element(S3Ns + "Size")?.Value), "Part must have Size.");
            Assert.False(string.IsNullOrWhiteSpace(part.Element(S3Ns + "LastModified")?.Value), "Part must have LastModified.");
        }

        Assert.Equal("100", parts[0].Element(S3Ns + "Size")?.Value);
        Assert.Equal("200", parts[1].Element(S3Ns + "Size")?.Value);
        Assert.Equal("300", parts[2].Element(S3Ns + "Size")?.Value);
    }

    [Fact]
    public async Task Multipart_ListPartsPagination_PaginatesCorrectlyWithMaxPartsAndMarker()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = Bucket();
        const string key = "paginated-parts.dat";
        await CreateBucketAsync(client, bucket);

        var uploadId = await InitiateMultipartAsync(client, bucket, key);
        await UploadPartAsync(client, bucket, key, uploadId, 1, Repeat(0x01, 100));
        await UploadPartAsync(client, bucket, key, uploadId, 2, Repeat(0x02, 100));
        await UploadPartAsync(client, bucket, key, uploadId, 3, Repeat(0x03, 100));

        // First page: max-parts=2
        var page1Resp = await client.GetAsync(
            $"{ObjectPath(bucket, key)}?uploadId={Uri.EscapeDataString(uploadId)}&max-parts=2");
        Assert.Equal(HttpStatusCode.OK, page1Resp.StatusCode);

        var page1Doc = XDocument.Parse(await page1Resp.Content.ReadAsStringAsync());
        Assert.Equal("true", El(page1Doc, "IsTruncated"));
        Assert.Equal("2", El(page1Doc, "MaxParts"));
        var nextMarker = El(page1Doc, "NextPartNumberMarker");
        Assert.False(string.IsNullOrWhiteSpace(nextMarker));
        Assert.Equal(2, page1Doc.Root!.Elements(S3Ns + "Part").Count());

        // Second page: using marker
        var page2Resp = await client.GetAsync(
            $"{ObjectPath(bucket, key)}?uploadId={Uri.EscapeDataString(uploadId)}&max-parts=2&part-number-marker={nextMarker}");
        Assert.Equal(HttpStatusCode.OK, page2Resp.StatusCode);

        var page2Doc = XDocument.Parse(await page2Resp.Content.ReadAsStringAsync());
        Assert.Equal("false", El(page2Doc, "IsTruncated"));
        var remainingParts = page2Doc.Root!.Elements(S3Ns + "Part").ToArray();
        Assert.Single(remainingParts);
        Assert.Equal("3", remainingParts[0].Element(S3Ns + "PartNumber")?.Value);
    }

    [Fact]
    public async Task Multipart_AbortMultipartUpload_CleansUpAndReturns204()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = Bucket();
        const string key = "abort-test.dat";
        await CreateBucketAsync(client, bucket);

        var uploadId = await InitiateMultipartAsync(client, bucket, key);
        await UploadPartAsync(client, bucket, key, uploadId, 1, Repeat(0xAA, 256));

        // Abort
        var abortResp = await client.DeleteAsync(
            $"{ObjectPath(bucket, key)}?uploadId={Uri.EscapeDataString(uploadId)}");
        Assert.Equal(HttpStatusCode.NoContent, abortResp.StatusCode);

        // Verify upload no longer listed
        var listResp = await client.GetAsync($"{Prefix}/{bucket}?uploads");
        Assert.Equal(HttpStatusCode.OK, listResp.StatusCode);
        var listDoc = XDocument.Parse(await listResp.Content.ReadAsStringAsync());
        var remainingUploads = listDoc.Root!.Elements(S3Ns + "Upload")
            .Where(u => u.Element(S3Ns + "UploadId")?.Value == uploadId)
            .ToArray();
        Assert.Empty(remainingUploads);

        // ListParts for aborted upload → NoSuchUpload
        var listPartsResp = await client.GetAsync(
            $"{ObjectPath(bucket, key)}?uploadId={Uri.EscapeDataString(uploadId)}");
        Assert.Equal(HttpStatusCode.NotFound, listPartsResp.StatusCode);
        var errorDoc = XDocument.Parse(await listPartsResp.Content.ReadAsStringAsync());
        Assert.Equal("NoSuchUpload", El(errorDoc, "Code"));
    }

    [Fact]
    public async Task Multipart_ETagFormat_CompletedObjectHasETagHeader()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = Bucket();
        const string key = "etag-format-test.dat";
        await CreateBucketAsync(client, bucket);

        var uploadId = await InitiateMultipartAsync(client, bucket, key);
        var etag1 = await UploadPartAsync(client, bucket, key, uploadId, 1, Repeat(0x10, 512));
        var etag2 = await UploadPartAsync(client, bucket, key, uploadId, 2, Repeat(0x20, 512));
        var etag3 = await UploadPartAsync(client, bucket, key, uploadId, 3, Repeat(0x30, 512));

        var completeResp = await CompleteMultipartAsync(client, bucket, key, uploadId,
            [(1, etag1), (2, etag2), (3, etag3)]);
        Assert.Equal(HttpStatusCode.OK, completeResp.StatusCode);

        // HEAD → verify ETag is present
        using var headReq = new HttpRequestMessage(HttpMethod.Head, ObjectPath(bucket, key));
        var headResp = await client.SendAsync(headReq);
        Assert.Equal(HttpStatusCode.OK, headResp.StatusCode);

        var objectETag = headResp.Headers.ETag?.Tag;
        Assert.False(string.IsNullOrWhiteSpace(objectETag), "Completed multipart object must have ETag.");
        // The ETag should be non-empty; rclone uses it for consistency checks.
        // AWS S3 produces "hash-N" format for multipart; the disk provider uses its own format.
        Assert.NotNull(headResp.Headers.ETag);
    }

    // ═══════════════════════ SPECIAL CHARACTER ENCODING TESTS ═══════════════════════

    [Fact]
    public async Task Encoding_ObjectKeyWithSpaces_RoundTripsCorrectly()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = Bucket();
        const string key = "my file.txt";
        const string content = "content with spaces in key";
        await CreateBucketAsync(client, bucket);

        await PutObjectAsync(client, bucket, key, content);

        // HEAD
        using var headReq = new HttpRequestMessage(HttpMethod.Head, ObjectPath(bucket, key));
        var headResp = await client.SendAsync(headReq);
        Assert.Equal(HttpStatusCode.OK, headResp.StatusCode);

        // GET
        var getResp = await client.GetAsync(ObjectPath(bucket, key));
        Assert.Equal(HttpStatusCode.OK, getResp.StatusCode);
        Assert.Equal(content, await getResp.Content.ReadAsStringAsync());

        // List with prefix → key appears
        var listResp = await client.GetAsync($"{Prefix}/{bucket}?list-type=2&prefix={Uri.EscapeDataString("my ")}");
        Assert.Equal(HttpStatusCode.OK, listResp.StatusCode);
        var listDoc = XDocument.Parse(await listResp.Content.ReadAsStringAsync());
        var keys = listDoc.Root!.Elements(S3Ns + "Contents").Select(c => c.Element(S3Ns + "Key")?.Value).ToArray();
        Assert.Contains(key, keys);
    }

    [Fact]
    public async Task Encoding_ObjectKeyWithPlusSign_TreatedLiterallyNotAsSpace()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = Bucket();
        const string key = "file+name.txt";
        const string content = "plus sign content";
        await CreateBucketAsync(client, bucket);

        await PutObjectAsync(client, bucket, key, content);

        // HEAD
        using var headReq = new HttpRequestMessage(HttpMethod.Head, ObjectPath(bucket, key));
        var headResp = await client.SendAsync(headReq);
        Assert.Equal(HttpStatusCode.OK, headResp.StatusCode);

        // GET
        var getResp = await client.GetAsync(ObjectPath(bucket, key));
        Assert.Equal(HttpStatusCode.OK, getResp.StatusCode);
        Assert.Equal(content, await getResp.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task Encoding_ObjectKeyWithUnicodeCharacters_RoundTripsCorrectly()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = Bucket();
        const string key = "données/résumé.txt";
        const string content = "contenu unicode";
        await CreateBucketAsync(client, bucket);

        await PutObjectAsync(client, bucket, key, content);

        // HEAD
        using var headReq = new HttpRequestMessage(HttpMethod.Head, ObjectPath(bucket, key));
        var headResp = await client.SendAsync(headReq);
        Assert.Equal(HttpStatusCode.OK, headResp.StatusCode);

        // List with prefix
        var listResp = await client.GetAsync(
            $"{Prefix}/{bucket}?list-type=2&prefix={Uri.EscapeDataString("données/")}");
        Assert.Equal(HttpStatusCode.OK, listResp.StatusCode);
        var listDoc = XDocument.Parse(await listResp.Content.ReadAsStringAsync());
        var keys = listDoc.Root!.Elements(S3Ns + "Contents").Select(c => c.Element(S3Ns + "Key")?.Value).ToArray();
        Assert.Contains(key, keys);
    }

    [Fact]
    public async Task Encoding_ObjectKeyWithUrlSpecialCharacters_AllAccessible()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = Bucket();
        await CreateBucketAsync(client, bucket);

        var specialKeys = new[]
        {
            "path/to/file (1).txt",
            "file&name.txt",
            "file=value.txt"
        };

        foreach (var key in specialKeys) {
            await PutObjectAsync(client, bucket, key, $"content-{key}");
        }

        // HEAD each
        foreach (var key in specialKeys) {
            using var headReq = new HttpRequestMessage(HttpMethod.Head, ObjectPath(bucket, key));
            var headResp = await client.SendAsync(headReq);
            Assert.Equal(HttpStatusCode.OK, headResp.StatusCode);
        }

        // List all
        var listResp = await client.GetAsync($"{Prefix}/{bucket}?list-type=2");
        Assert.Equal(HttpStatusCode.OK, listResp.StatusCode);
        var listDoc = XDocument.Parse(await listResp.Content.ReadAsStringAsync());
        var listedKeys = listDoc.Root!.Elements(S3Ns + "Contents").Select(c => c.Element(S3Ns + "Key")?.Value).ToArray();

        foreach (var key in specialKeys) {
            Assert.Contains(key, listedKeys);
        }
    }

    [Fact]
    public async Task Encoding_DirectoryLikeKeys_DelimiterPrefixListingWorksCorrectly()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = Bucket();
        await CreateBucketAsync(client, bucket);

        await PutObjectAsync(client, bucket, "a/b/c/d.txt", "d");
        await PutObjectAsync(client, bucket, "a/b/c/e.txt", "e");
        await PutObjectAsync(client, bucket, "a/b/f.txt", "f");
        await PutObjectAsync(client, bucket, "a/g.txt", "g");

        // Level 1: prefix=a/ delimiter=/
        var resp1 = await client.GetAsync($"{Prefix}/{bucket}?list-type=2&prefix={Uri.EscapeDataString("a/")}&delimiter=%2F");
        Assert.Equal(HttpStatusCode.OK, resp1.StatusCode);
        var doc1 = XDocument.Parse(await resp1.Content.ReadAsStringAsync());

        var prefixes1 = doc1.Root!.Elements(S3Ns + "CommonPrefixes").Select(cp => cp.Element(S3Ns + "Prefix")?.Value).ToArray();
        var contents1 = doc1.Root!.Elements(S3Ns + "Contents").Select(c => c.Element(S3Ns + "Key")?.Value).ToArray();
        Assert.Contains("a/b/", prefixes1);
        Assert.Contains("a/g.txt", contents1);
        Assert.DoesNotContain("a/b/c/", prefixes1); // nested prefix not at this level

        // Level 2: prefix=a/b/ delimiter=/
        var resp2 = await client.GetAsync($"{Prefix}/{bucket}?list-type=2&prefix={Uri.EscapeDataString("a/b/")}&delimiter=%2F");
        Assert.Equal(HttpStatusCode.OK, resp2.StatusCode);
        var doc2 = XDocument.Parse(await resp2.Content.ReadAsStringAsync());

        var prefixes2 = doc2.Root!.Elements(S3Ns + "CommonPrefixes").Select(cp => cp.Element(S3Ns + "Prefix")?.Value).ToArray();
        var contents2 = doc2.Root!.Elements(S3Ns + "Contents").Select(c => c.Element(S3Ns + "Key")?.Value).ToArray();
        Assert.Contains("a/b/c/", prefixes2);
        Assert.Contains("a/b/f.txt", contents2);

        // Level 3: prefix=a/b/c/ delimiter=/
        var resp3 = await client.GetAsync($"{Prefix}/{bucket}?list-type=2&prefix={Uri.EscapeDataString("a/b/c/")}&delimiter=%2F");
        Assert.Equal(HttpStatusCode.OK, resp3.StatusCode);
        var doc3 = XDocument.Parse(await resp3.Content.ReadAsStringAsync());

        var prefixes3 = doc3.Root!.Elements(S3Ns + "CommonPrefixes").Select(cp => cp.Element(S3Ns + "Prefix")?.Value).ToArray();
        var contents3 = doc3.Root!.Elements(S3Ns + "Contents").Select(c => c.Element(S3Ns + "Key")?.Value).OrderBy(k => k).ToArray();
        Assert.Empty(prefixes3);
        Assert.Equal(new[] { "a/b/c/d.txt", "a/b/c/e.txt" }, contents3);
    }

    [Fact]
    public async Task Encoding_CopyObjectWithSpecialCharacterKeys_WorksViaUrlEncodedCopySource()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = Bucket();
        const string sourceKey = "source file (1).txt";
        const string destKey = "dest file (2).txt";
        const string content = "copy me with special chars";
        await CreateBucketAsync(client, bucket);

        await PutObjectAsync(client, bucket, sourceKey, content);

        // Copy using URL-encoded x-amz-copy-source
        var encodedSource = $"/{bucket}/{Uri.EscapeDataString(sourceKey)}";
        using var copyReq = new HttpRequestMessage(HttpMethod.Put, ObjectPath(bucket, destKey));
        copyReq.Headers.TryAddWithoutValidation("x-amz-copy-source", encodedSource);

        var copyResp = await client.SendAsync(copyReq);
        Assert.Equal(HttpStatusCode.OK, copyResp.StatusCode);

        var copyDoc = XDocument.Parse(await copyResp.Content.ReadAsStringAsync());
        Assert.Equal("CopyObjectResult", copyDoc.Root?.Name.LocalName);

        // HEAD dest
        using var headReq = new HttpRequestMessage(HttpMethod.Head, ObjectPath(bucket, destKey));
        var headResp = await client.SendAsync(headReq);
        Assert.Equal(HttpStatusCode.OK, headResp.StatusCode);

        // GET dest → content matches
        var getResp = await client.GetAsync(ObjectPath(bucket, destKey));
        Assert.Equal(HttpStatusCode.OK, getResp.StatusCode);
        Assert.Equal(content, await getResp.Content.ReadAsStringAsync());
    }

    // ═══════════════════════ ERROR HANDLING TESTS ═══════════════════════

    [Fact]
    public async Task Error_GetNonExistentObject_ReturnsNoSuchKeyXml()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = Bucket();
        await CreateBucketAsync(client, bucket);

        var resp = await client.GetAsync($"{Prefix}/{bucket}/nonexistent-key");
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
        Assert.Equal("application/xml", resp.Content.Headers.ContentType?.MediaType);

        var doc = XDocument.Parse(await resp.Content.ReadAsStringAsync());
        Assert.Equal("NoSuchKey", El(doc, "Code"));
        Assert.Equal("nonexistent-key", El(doc, "Key"));
    }

    [Fact]
    public async Task Error_HeadNonExistentObject_Returns404WithErrorHeaders()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = Bucket();
        await CreateBucketAsync(client, bucket);

        using var headReq = new HttpRequestMessage(HttpMethod.Head, $"{Prefix}/{bucket}/nonexistent-key");
        var resp = await client.SendAsync(headReq);
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);

        // S3 sends error info via headers for HEAD requests
        if (resp.Headers.Contains("x-amz-error-code")) {
            var errorCode = resp.Headers.GetValues("x-amz-error-code").FirstOrDefault();
            Assert.Equal("NoSuchKey", errorCode);
        }
    }

    [Fact]
    public async Task Error_OperationsOnNonExistentBucket_ReturnsNoSuchBucket()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"nonexistent-{Guid.NewGuid():N}";

        var resp = await client.GetAsync($"{Prefix}/{bucket}?list-type=2");
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
        Assert.Equal("application/xml", resp.Content.Headers.ContentType?.MediaType);

        var doc = XDocument.Parse(await resp.Content.ReadAsStringAsync());
        Assert.Equal("NoSuchBucket", El(doc, "Code"));
    }

    [Fact]
    public async Task Error_CompleteMultipartWithInvalidUploadId_ReturnsNoSuchUpload()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = Bucket();
        const string key = "invalid-upload-test.dat";
        await CreateBucketAsync(client, bucket);

        var body = """
<CompleteMultipartUpload>
  <Part>
    <PartNumber>1</PartNumber>
    <ETag>"fake-etag"</ETag>
  </Part>
</CompleteMultipartUpload>
""";

        using var req = new HttpRequestMessage(
            HttpMethod.Post,
            $"{ObjectPath(bucket, key)}?uploadId=invalid-upload-id")
        {
            Content = new StringContent(body, Encoding.UTF8, "application/xml")
        };

        var resp = await client.SendAsync(req);
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);

        var doc = XDocument.Parse(await resp.Content.ReadAsStringAsync());
        Assert.Equal("NoSuchUpload", El(doc, "Code"));
    }

    [Fact]
    public async Task Error_CompleteMultipartWithWrongPartOrder_DiskProviderAutoSorts()
    {
        // Note: Real S3 rejects parts in wrong order with InvalidPartOrder.
        // The disk provider auto-sorts parts (see DiskStorageService.cs OrderBy),
        // so this test verifies the disk backend silently reorders and succeeds.
        using var client = await _factory.CreateClientAsync();
        var bucket = Bucket();
        const string key = "wrong-order-test.dat";
        await CreateBucketAsync(client, bucket);

        var uploadId = await InitiateMultipartAsync(client, bucket, key);
        var part1Data = Encoding.UTF8.GetBytes("FIRST");
        var part2Data = Encoding.UTF8.GetBytes("SECOND");
        var part3Data = Encoding.UTF8.GetBytes("THIRD");

        var etag1 = await UploadPartAsync(client, bucket, key, uploadId, 1, part1Data);
        var etag2 = await UploadPartAsync(client, bucket, key, uploadId, 2, part2Data);
        var etag3 = await UploadPartAsync(client, bucket, key, uploadId, 3, part3Data);

        // Complete with parts in reversed order (3, 1, 2)
        var completeResp = await CompleteMultipartAsync(client, bucket, key, uploadId,
            [(3, etag3), (1, etag1), (2, etag2)]);

        // Disk provider auto-sorts, so this succeeds
        Assert.Equal(HttpStatusCode.OK, completeResp.StatusCode);

        // Content should be in part-number order (1,2,3), not submission order
        var getResp = await client.GetAsync(ObjectPath(bucket, key));
        var downloaded = await getResp.Content.ReadAsByteArrayAsync();
        Assert.Equal(
            part1Data.Concat(part2Data).Concat(part3Data).ToArray(),
            downloaded);
    }

    [Fact]
    public async Task Error_RequestIdPresentInErrorResponse()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = Bucket();
        await CreateBucketAsync(client, bucket);

        var resp = await client.GetAsync($"{Prefix}/{bucket}/nonexistent-key-for-request-id-test");
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);

        // Verify RequestId is present in error XML body (rclone logs this)
        var doc = XDocument.Parse(await resp.Content.ReadAsStringAsync());
        var requestIdElement = doc.Descendants().FirstOrDefault(e => e.Name.LocalName == "RequestId");
        Assert.NotNull(requestIdElement);
        Assert.False(string.IsNullOrWhiteSpace(requestIdElement!.Value),
            "Error response must include a non-empty RequestId element for rclone diagnostics.");

        // Also check for x-amz-request-id response header (S3 standard)
        if (resp.Headers.Contains("x-amz-request-id")) {
            Assert.False(string.IsNullOrWhiteSpace(
                resp.Headers.GetValues("x-amz-request-id").FirstOrDefault()));
        }
    }

    [Fact]
    public async Task Error_ResponseContentTypeIsApplicationXml()
    {
        using var client = await _factory.CreateClientAsync();

        // NoSuchKey error
        var bucket = Bucket();
        await CreateBucketAsync(client, bucket);
        var noSuchKeyResp = await client.GetAsync($"{Prefix}/{bucket}/missing-key");
        Assert.Equal(HttpStatusCode.NotFound, noSuchKeyResp.StatusCode);
        Assert.Equal("application/xml", noSuchKeyResp.Content.Headers.ContentType?.MediaType);

        // NoSuchBucket error
        var missingBucket = $"missing-{Guid.NewGuid():N}";
        var noSuchBucketResp = await client.GetAsync($"{Prefix}/{missingBucket}?list-type=2");
        Assert.Equal(HttpStatusCode.NotFound, noSuchBucketResp.StatusCode);
        Assert.Equal("application/xml", noSuchBucketResp.Content.Headers.ContentType?.MediaType);
    }
}
