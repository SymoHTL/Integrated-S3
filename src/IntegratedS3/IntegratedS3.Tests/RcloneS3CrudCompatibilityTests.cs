using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Text;
using System.Xml.Linq;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Tests.Infrastructure;
using Xunit;

namespace IntegratedS3.Tests;

/// <summary>
/// Exercises the exact CRUD and metadata patterns that rclone uses against
/// an S3-compatible server, validating IntegratedS3 compatibility.
/// </summary>
public sealed class RcloneS3CrudCompatibilityTests : IClassFixture<WebUiApplicationFactory>
{
    private static readonly System.Text.Json.JsonSerializerOptions JsonOptions = new(System.Text.Json.JsonSerializerDefaults.Web);

    private readonly WebUiApplicationFactory _factory;

    public RcloneS3CrudCompatibilityTests(WebUiApplicationFactory factory)
    {
        _factory = factory;
    }

    // ──────────────────────────────────────────────
    // 1. PutObject with Content-MD5
    // ──────────────────────────────────────────────

    [Fact]
    public async Task PutObject_WithContentMD5_AcceptsValidHash()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-md5-valid-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);

        const string payload = "rclone md5 test payload";
        var payloadBytes = Encoding.UTF8.GetBytes(payload);
        var md5Hash = Convert.ToBase64String(MD5.HashData(payloadBytes));

        using var request = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucket}/docs/md5.txt")
        {
            Content = new ByteArrayContent(payloadBytes)
        };
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("text/plain");
        request.Content.Headers.ContentMD5 = MD5.HashData(payloadBytes);

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.NotNull(response.Headers.ETag);
        Assert.False(string.IsNullOrWhiteSpace(response.Headers.ETag?.Tag));
    }

    [Fact]
    public async Task PutObject_WithInvalidContentMD5_ReturnsBadDigest()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-md5-invalid-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);

        const string payload = "rclone md5 bad digest test";
        var payloadBytes = Encoding.UTF8.GetBytes(payload);
        var invalidMd5 = Convert.ToBase64String(MD5.HashData(Encoding.UTF8.GetBytes("wrong content")));

        using var request = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucket}/docs/bad-md5.txt")
        {
            Content = new ByteArrayContent(payloadBytes)
        };
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("text/plain");
        request.Content.Headers.ContentMD5 = MD5.HashData(Encoding.UTF8.GetBytes("wrong content"));

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var doc = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("BadDigest", GetRequiredElementValue(doc, "Code"));
    }

    // ──────────────────────────────────────────────
    // 2. PutObject with rclone Metadata (x-amz-meta-mtime)
    // ──────────────────────────────────────────────

    [Fact]
    public async Task PutObject_WithRcloneMtimeMetadata_RoundTripsOnGetAndHead()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-mtime-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);

        const string payload = "mtime metadata test";
        const string mtime = "1710950400";

        using var putRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucket}/docs/mtime.txt")
        {
            Content = new StringContent(payload, Encoding.UTF8, "text/plain")
        };
        putRequest.Headers.TryAddWithoutValidation("x-amz-meta-mtime", mtime);
        var putResponse = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);

        // GET should return the metadata
        var getResponse = await client.GetAsync($"/integrated-s3/{bucket}/docs/mtime.txt");
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        Assert.Equal(mtime, Assert.Single(getResponse.Headers.GetValues("x-amz-meta-mtime")));

        // HEAD should return the metadata
        using var headRequest = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{bucket}/docs/mtime.txt");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        Assert.Equal(mtime, Assert.Single(headResponse.Headers.GetValues("x-amz-meta-mtime")));
    }

    // ──────────────────────────────────────────────
    // 3. PutObject with Multiple Metadata Headers
    // ──────────────────────────────────────────────

    [Fact]
    public async Task PutObject_WithMultipleMetadataHeaders_AllRoundTripCorrectly()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-multi-meta-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);

        const string payload = "multi metadata payload";

        using var putRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucket}/docs/multi.txt")
        {
            Content = new StringContent(payload, Encoding.UTF8, "text/plain")
        };
        putRequest.Headers.TryAddWithoutValidation("x-amz-meta-mtime", "1710950400");
        putRequest.Headers.TryAddWithoutValidation("x-amz-meta-md5chksum", "abc123");
        putRequest.Headers.TryAddWithoutValidation("x-amz-meta-custom", "value");

        var putResponse = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);

        // HEAD verification
        using var headRequest = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{bucket}/docs/multi.txt");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        Assert.Equal("1710950400", Assert.Single(headResponse.Headers.GetValues("x-amz-meta-mtime")));
        Assert.Equal("abc123", Assert.Single(headResponse.Headers.GetValues("x-amz-meta-md5chksum")));
        Assert.Equal("value", Assert.Single(headResponse.Headers.GetValues("x-amz-meta-custom")));

        // GET verification
        var getResponse = await client.GetAsync($"/integrated-s3/{bucket}/docs/multi.txt");
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        Assert.Equal("1710950400", Assert.Single(getResponse.Headers.GetValues("x-amz-meta-mtime")));
        Assert.Equal("abc123", Assert.Single(getResponse.Headers.GetValues("x-amz-meta-md5chksum")));
        Assert.Equal("value", Assert.Single(getResponse.Headers.GetValues("x-amz-meta-custom")));
    }

    // ──────────────────────────────────────────────
    // 4. HeadObject Full Header Verification
    // ──────────────────────────────────────────────

    [Fact]
    public async Task HeadObject_ReturnsCompleteHeaderSet()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-head-full-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);

        const string payload = "full head verification payload";

        using var putRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucket}/docs/full-head.txt")
        {
            Content = new StringContent(payload, Encoding.UTF8, "text/plain")
        };
        putRequest.Headers.TryAddWithoutValidation("x-amz-meta-author", "rclone-test");
        var putResponse = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);

        using var headRequest = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{bucket}/docs/full-head.txt");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);

        // Content-Length
        Assert.Equal(Encoding.UTF8.GetByteCount(payload), headResponse.Content.Headers.ContentLength);

        // Content-Type
        Assert.Equal("text/plain", headResponse.Content.Headers.ContentType?.MediaType);

        // ETag (quoted, non-empty)
        Assert.NotNull(headResponse.Headers.ETag);
        var etagValue = headResponse.Headers.ETag!.Tag;
        Assert.StartsWith("\"", etagValue);
        Assert.EndsWith("\"", etagValue);
        Assert.True(etagValue.Length > 2);

        // Last-Modified (valid date)
        Assert.NotNull(headResponse.Content.Headers.LastModified);

        // Accept-Ranges: bytes
        Assert.Equal("bytes", headResponse.Headers.GetValues("Accept-Ranges").FirstOrDefault());

        // x-amz-meta-* headers
        Assert.Equal("rclone-test", Assert.Single(headResponse.Headers.GetValues("x-amz-meta-author")));

        // No body in HEAD response
        var bodyBytes = await headResponse.Content.ReadAsByteArrayAsync();
        Assert.Empty(bodyBytes);
    }

    // ──────────────────────────────────────────────
    // 5. GetObject Full Content
    // ──────────────────────────────────────────────

    [Fact]
    public async Task GetObject_ReturnsFullContentWithCorrectHeaders()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-get-full-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);

        const string payload = "Hello rclone world!";

        using var putRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucket}/docs/hello.txt")
        {
            Content = new StringContent(payload, Encoding.UTF8, "text/plain")
        };
        var putResponse = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);

        var getResponse = await client.GetAsync($"/integrated-s3/{bucket}/docs/hello.txt");
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        Assert.Equal(Encoding.UTF8.GetByteCount(payload), getResponse.Content.Headers.ContentLength);
        Assert.Equal("text/plain", getResponse.Content.Headers.ContentType?.MediaType);
        Assert.Equal(payload, await getResponse.Content.ReadAsStringAsync());
    }

    // ──────────────────────────────────────────────
    // 6. GetObject with Range Header (rclone resume)
    // ──────────────────────────────────────────────

    [Fact]
    public async Task GetObject_WithRangeMiddle_ReturnsPartialContent()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-range-mid-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);

        // Create 100-byte content
        var payload = new string('A', 10) + new string('B', 40) + new string('C', 50);
        Assert.Equal(100, payload.Length);

        using var putRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucket}/docs/range.bin")
        {
            Content = new ByteArrayContent(Encoding.UTF8.GetBytes(payload))
        };
        putRequest.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        var putResponse = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);

        // Range: bytes=10-49 → exactly bytes 10-49 (40 bytes)
        using var rangeRequest = new HttpRequestMessage(HttpMethod.Get, $"/integrated-s3/{bucket}/docs/range.bin");
        rangeRequest.Headers.Range = new RangeHeaderValue(10, 49);

        var rangeResponse = await client.SendAsync(rangeRequest);
        Assert.Equal(HttpStatusCode.PartialContent, rangeResponse.StatusCode);
        Assert.Equal("bytes 10-49/100", rangeResponse.Content.Headers.ContentRange?.ToString());
        var rangeBody = await rangeResponse.Content.ReadAsStringAsync();
        Assert.Equal(40, rangeBody.Length);
        Assert.Equal(payload.Substring(10, 40), rangeBody);
    }

    [Fact]
    public async Task GetObject_WithRangeToEnd_ReturnsRemainingBytes()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-range-end-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);

        var payload = new string('X', 100);
        await PutObjectAsync(client, bucket, "docs/range-end.bin", payload, "application/octet-stream");

        // Range: bytes=50- → bytes 50 to end (50 bytes)
        using var request = new HttpRequestMessage(HttpMethod.Get, $"/integrated-s3/{bucket}/docs/range-end.bin");
        request.Headers.Range = new RangeHeaderValue(50, null);

        var response = await client.SendAsync(request);
        Assert.Equal(HttpStatusCode.PartialContent, response.StatusCode);
        Assert.Equal("bytes 50-99/100", response.Content.Headers.ContentRange?.ToString());
        var body = await response.Content.ReadAsStringAsync();
        Assert.Equal(50, body.Length);
    }

    [Fact]
    public async Task GetObject_WithRangeSingleByte_ReturnsSingleByte()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-range-byte-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);

        const string payload = "ABCDEFGHIJ";
        await PutObjectAsync(client, bucket, "docs/range-byte.bin", payload, "application/octet-stream");

        // Range: bytes=0-0 → single byte
        using var request = new HttpRequestMessage(HttpMethod.Get, $"/integrated-s3/{bucket}/docs/range-byte.bin");
        request.Headers.Range = new RangeHeaderValue(0, 0);

        var response = await client.SendAsync(request);
        Assert.Equal(HttpStatusCode.PartialContent, response.StatusCode);
        Assert.Equal("bytes 0-0/10", response.Content.Headers.ContentRange?.ToString());
        var body = await response.Content.ReadAsStringAsync();
        Assert.Equal("A", body);
    }

    // ──────────────────────────────────────────────
    // 7. GetObject Conditional: If-None-Match (rclone caching)
    // ──────────────────────────────────────────────

    [Fact]
    public async Task GetObject_WithIfNoneMatchMatchingETag_Returns304()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-inm-match-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);

        const string payload = "if-none-match test";
        var putResponse = await PutObjectAsync(client, bucket, "docs/inm.txt", payload, "text/plain");
        var etag = putResponse.Headers.ETag?.Tag;
        Assert.NotNull(etag);

        // GET with matching ETag → 304
        using var request = new HttpRequestMessage(HttpMethod.Get, $"/integrated-s3/{bucket}/docs/inm.txt");
        request.Headers.TryAddWithoutValidation("If-None-Match", etag!);

        var response = await client.SendAsync(request);
        Assert.Equal(HttpStatusCode.NotModified, response.StatusCode);
        Assert.Empty(await response.Content.ReadAsByteArrayAsync());
    }

    [Fact]
    public async Task GetObject_WithIfNoneMatchNonMatchingETag_Returns200()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-inm-nomatch-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);

        const string payload = "if-none-match mismatch test";
        await PutObjectAsync(client, bucket, "docs/inm2.txt", payload, "text/plain");

        // GET with non-matching ETag → 200 with full body
        using var request = new HttpRequestMessage(HttpMethod.Get, $"/integrated-s3/{bucket}/docs/inm2.txt");
        request.Headers.TryAddWithoutValidation("If-None-Match", "\"wrong-etag\"");

        var response = await client.SendAsync(request);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(payload, await response.Content.ReadAsStringAsync());
    }

    // ──────────────────────────────────────────────
    // 8. GetObject Conditional: If-Modified-Since
    // ──────────────────────────────────────────────

    [Fact]
    public async Task GetObject_WithIfModifiedSinceAfterLastModified_Returns304()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-ims-after-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);

        const string payload = "if-modified-since test";
        await PutObjectAsync(client, bucket, "docs/ims.txt", payload, "text/plain");

        // Get the Last-Modified from HEAD
        using var headRequest = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{bucket}/docs/ims.txt");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        var lastModified = headResponse.Content.Headers.LastModified;
        Assert.NotNull(lastModified);

        // GET with If-Modified-Since AFTER Last-Modified → 304
        using var request = new HttpRequestMessage(HttpMethod.Get, $"/integrated-s3/{bucket}/docs/ims.txt");
        request.Headers.IfModifiedSince = lastModified!.Value.AddMinutes(5);

        var response = await client.SendAsync(request);
        Assert.Equal(HttpStatusCode.NotModified, response.StatusCode);
    }

    [Fact]
    public async Task GetObject_WithIfModifiedSinceBeforeLastModified_Returns200()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-ims-before-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);

        const string payload = "if-modified-since old date test";
        await PutObjectAsync(client, bucket, "docs/ims2.txt", payload, "text/plain");

        // GET with If-Modified-Since set to far in the past → 200
        using var request = new HttpRequestMessage(HttpMethod.Get, $"/integrated-s3/{bucket}/docs/ims2.txt");
        request.Headers.IfModifiedSince = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero);

        var response = await client.SendAsync(request);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(payload, await response.Content.ReadAsStringAsync());
    }

    // ──────────────────────────────────────────────
    // 9. DeleteObject Single
    // ──────────────────────────────────────────────

    [Fact]
    public async Task DeleteObject_RemovesObjectSuccessfully()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-delete-single-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);
        await PutObjectAsync(client, bucket, "docs/delete-me.txt", "to be deleted", "text/plain");

        // Verify exists
        using var headBefore = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{bucket}/docs/delete-me.txt");
        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(headBefore)).StatusCode);

        // DELETE
        var deleteResponse = await client.DeleteAsync($"/integrated-s3/{bucket}/docs/delete-me.txt");
        Assert.Equal(HttpStatusCode.NoContent, deleteResponse.StatusCode);

        // Verify gone
        using var headAfter = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{bucket}/docs/delete-me.txt");
        Assert.Equal(HttpStatusCode.NotFound, (await client.SendAsync(headAfter)).StatusCode);
    }

    // ──────────────────────────────────────────────
    // 10. DeleteObjects Batch (rclone purge pattern)
    // ──────────────────────────────────────────────

    [Fact]
    public async Task DeleteObjects_Batch_DeletesAllFiveObjects()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-batch-del-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);

        var keys = new[] { "file1.txt", "file2.txt", "file3.txt", "file4.txt", "file5.txt" };
        foreach (var key in keys) {
            await PutObjectAsync(client, bucket, key, $"content of {key}", "text/plain");
        }

        var deleteXml = "<Delete>\n" +
            string.Join("\n", keys.Select(static k => $"  <Object><Key>{k}</Key></Object>")) +
            "\n</Delete>";

        using var deleteRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucket}?delete")
        {
            Content = new StringContent(deleteXml, Encoding.UTF8, "application/xml")
        };

        var deleteResponse = await client.SendAsync(deleteRequest);
        Assert.Equal(HttpStatusCode.OK, deleteResponse.StatusCode);
        Assert.Equal("application/xml", deleteResponse.Content.Headers.ContentType?.MediaType);

        var doc = XDocument.Parse(await deleteResponse.Content.ReadAsStringAsync());
        Assert.Equal("DeleteResult", doc.Root?.Name.LocalName);

        var deletedKeys = doc.Root!.Elements("Deleted")
            .Select(static d => d.Element("Key")?.Value)
            .OrderBy(static k => k, StringComparer.Ordinal)
            .ToArray();
        Assert.Equal(keys.OrderBy(static k => k, StringComparer.Ordinal).ToArray(), deletedKeys);
        Assert.Empty(doc.Root.Elements("Error"));

        // Verify all gone
        foreach (var key in keys) {
            using var head = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{bucket}/{key}");
            Assert.Equal(HttpStatusCode.NotFound, (await client.SendAsync(head)).StatusCode);
        }
    }

    // ──────────────────────────────────────────────
    // 11. DeleteObjects Batch with Non-Existent Keys
    // ──────────────────────────────────────────────

    [Fact]
    public async Task DeleteObjects_Batch_NonExistentKeysReportSuccess()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-batch-missing-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);

        // Put only 2 objects but request deletion of 4 (2 existing + 2 non-existing)
        await PutObjectAsync(client, bucket, "exists1.txt", "content1", "text/plain");
        await PutObjectAsync(client, bucket, "exists2.txt", "content2", "text/plain");

        const string deleteXml = """
<Delete>
  <Object><Key>exists1.txt</Key></Object>
  <Object><Key>exists2.txt</Key></Object>
  <Object><Key>ghost1.txt</Key></Object>
  <Object><Key>ghost2.txt</Key></Object>
</Delete>
""";

        using var deleteRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucket}?delete")
        {
            Content = new StringContent(deleteXml, Encoding.UTF8, "application/xml")
        };

        var deleteResponse = await client.SendAsync(deleteRequest);
        Assert.Equal(HttpStatusCode.OK, deleteResponse.StatusCode);

        var doc = XDocument.Parse(await deleteResponse.Content.ReadAsStringAsync());
        var deletedKeys = doc.Root!.Elements("Deleted")
            .Select(static d => d.Element("Key")?.Value)
            .OrderBy(static k => k, StringComparer.Ordinal)
            .ToArray();

        // S3 behavior: delete of non-existent key is success
        Assert.Contains("exists1.txt", deletedKeys);
        Assert.Contains("exists2.txt", deletedKeys);
        Assert.Contains("ghost1.txt", deletedKeys);
        Assert.Contains("ghost2.txt", deletedKeys);

        // No errors expected
        Assert.Empty(doc.Root.Elements("Error"));
    }

    // ──────────────────────────────────────────────
    // 12. Zero-Byte Directory Marker (rclone mkdir pattern)
    // ──────────────────────────────────────────────

    [Fact]
    public async Task PutObject_ZeroByteDirectoryMarker_RoundTripsCorrectly()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-mkdir-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);

        // PUT zero-byte object with trailing slash and directory content-type
        using var putRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucket}/testdir/")
        {
            Content = new ByteArrayContent([])
        };
        putRequest.Content.Headers.ContentType = new MediaTypeHeaderValue("application/x-directory");
        var putResponse = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);

        // HEAD → 200, Content-Length=0
        using var headRequest = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{bucket}/testdir/");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        Assert.Equal(0, headResponse.Content.Headers.ContentLength);

        // GET → 200, empty body
        var getResponse = await client.GetAsync($"/integrated-s3/{bucket}/testdir/");
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        Assert.Empty(await getResponse.Content.ReadAsByteArrayAsync());

        // DELETE → success
        var deleteResponse = await client.DeleteAsync($"/integrated-s3/{bucket}/testdir/");
        Assert.Equal(HttpStatusCode.NoContent, deleteResponse.StatusCode);

        // Verify gone
        using var headAfter = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{bucket}/testdir/");
        Assert.Equal(HttpStatusCode.NotFound, (await client.SendAsync(headAfter)).StatusCode);
    }

    // ──────────────────────────────────────────────
    // 13. CopyObject with REPLACE Metadata (rclone touch pattern)
    // ──────────────────────────────────────────────

    [Fact]
    public async Task CopyObject_WithReplaceMetadataDirective_UpdatesMtime()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-copy-replace-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);

        const string payload = "rclone touch copy payload";

        // PUT with original mtime
        using var putRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucket}/docs/touch.txt")
        {
            Content = new StringContent(payload, Encoding.UTF8, "text/plain")
        };
        putRequest.Headers.TryAddWithoutValidation("x-amz-meta-mtime", "1000000");
        var putResponse = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);

        // Copy to same key with REPLACE metadata directive and new mtime
        using var copyRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucket}/docs/touch.txt")
        {
            Content = new ByteArrayContent([])
        };
        copyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source", $"/{bucket}/docs/touch.txt");
        copyRequest.Headers.TryAddWithoutValidation("x-amz-metadata-directive", "REPLACE");
        copyRequest.Content.Headers.ContentType = new MediaTypeHeaderValue("text/plain");
        copyRequest.Headers.TryAddWithoutValidation("x-amz-meta-mtime", "2000000");

        var copyResponse = await client.SendAsync(copyRequest);
        Assert.Equal(HttpStatusCode.OK, copyResponse.StatusCode);

        var copyDoc = XDocument.Parse(await copyResponse.Content.ReadAsStringAsync());
        Assert.Equal("CopyObjectResult", copyDoc.Root?.Name.LocalName);

        // HEAD → verify mtime is updated
        using var headRequest = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{bucket}/docs/touch.txt");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        Assert.Equal("2000000", Assert.Single(headResponse.Headers.GetValues("x-amz-meta-mtime")));

        // Verify content is preserved
        var getResponse = await client.GetAsync($"/integrated-s3/{bucket}/docs/touch.txt");
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        Assert.Equal(payload, await getResponse.Content.ReadAsStringAsync());
    }

    // ──────────────────────────────────────────────
    // 14. CopyObject with COPY Metadata (default behavior)
    // ──────────────────────────────────────────────

    [Fact]
    public async Task CopyObject_WithCopyMetadataDirective_PreservesSourceMetadata()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-copy-meta-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);

        const string payload = "copy metadata default test";

        // PUT source with metadata
        using var putRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucket}/docs/source-copy.txt")
        {
            Content = new StringContent(payload, Encoding.UTF8, "text/plain")
        };
        putRequest.Headers.TryAddWithoutValidation("x-amz-meta-mtime", "1710950400");
        putRequest.Headers.TryAddWithoutValidation("x-amz-meta-origin", "source");
        var putResponse = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);

        // Copy to different key with COPY metadata (default)
        using var copyRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucket}/docs/dest-copy.txt");
        copyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source", $"/{bucket}/docs/source-copy.txt");

        var copyResponse = await client.SendAsync(copyRequest);
        Assert.Equal(HttpStatusCode.OK, copyResponse.StatusCode);

        // HEAD destination → metadata matches source
        using var headRequest = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{bucket}/docs/dest-copy.txt");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        Assert.Equal("1710950400", Assert.Single(headResponse.Headers.GetValues("x-amz-meta-mtime")));
        Assert.Equal("source", Assert.Single(headResponse.Headers.GetValues("x-amz-meta-origin")));

        // Verify content
        var getResponse = await client.GetAsync($"/integrated-s3/{bucket}/docs/dest-copy.txt");
        Assert.Equal(payload, await getResponse.Content.ReadAsStringAsync());
    }

    // ──────────────────────────────────────────────
    // 15. CopyObject Cross-Bucket
    // ──────────────────────────────────────────────

    [Fact]
    public async Task CopyObject_CrossBucket_CopiesObjectCorrectly()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket1 = $"rclone-xbucket-src-{Guid.NewGuid():N}";
        var bucket2 = $"rclone-xbucket-dst-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket1);
        await CreateBucketAsync(client, bucket2);

        const string payload = "cross-bucket copy payload";

        // PUT object in bucket1
        using var putRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucket1}/docs/cross.txt")
        {
            Content = new StringContent(payload, Encoding.UTF8, "text/plain")
        };
        putRequest.Headers.TryAddWithoutValidation("x-amz-meta-tag", "cross-bucket");
        var putResponse = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);

        // Copy from bucket1 to bucket2
        using var copyRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucket2}/docs/cross.txt");
        copyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source", $"/{bucket1}/docs/cross.txt");

        var copyResponse = await client.SendAsync(copyRequest);
        Assert.Equal(HttpStatusCode.OK, copyResponse.StatusCode);

        // HEAD in bucket2 → verify object exists with correct metadata
        using var headRequest = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{bucket2}/docs/cross.txt");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        Assert.Equal("cross-bucket", Assert.Single(headResponse.Headers.GetValues("x-amz-meta-tag")));

        // Verify content
        var getResponse = await client.GetAsync($"/integrated-s3/{bucket2}/docs/cross.txt");
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        Assert.Equal(payload, await getResponse.Content.ReadAsStringAsync());
    }

    // ──────────────────────────────────────────────
    // 16. PutObject Content-Type Handling
    // ──────────────────────────────────────────────

    [Fact]
    public async Task PutObject_WithExplicitContentType_ReturnedOnHeadAndGet()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-ctype-explicit-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);

        using var putRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucket}/docs/typed.json")
        {
            Content = new StringContent("{}", Encoding.UTF8, "application/json")
        };
        var putResponse = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);

        // HEAD should return the explicit content-type
        using var headRequest = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{bucket}/docs/typed.json");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        Assert.Equal("application/json", headResponse.Content.Headers.ContentType?.MediaType);

        // GET should return the same
        var getResponse = await client.GetAsync($"/integrated-s3/{bucket}/docs/typed.json");
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        Assert.Equal("application/json", getResponse.Content.Headers.ContentType?.MediaType);
    }

    [Fact]
    public async Task PutObject_WithoutContentType_AssignsDefaultType()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-ctype-default-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);

        // PUT without content-type
        using var putRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucket}/docs/untyped.bin")
        {
            Content = new ByteArrayContent(Encoding.UTF8.GetBytes("binary-ish data"))
        };
        var putResponse = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);

        // HEAD should return a default content-type
        using var headRequest = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{bucket}/docs/untyped.bin");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        var contentType = headResponse.Content.Headers.ContentType?.MediaType;
        Assert.NotNull(contentType);
        // Server should assign a default (application/octet-stream or similar)
        Assert.False(string.IsNullOrWhiteSpace(contentType));
    }

    // ──────────────────────────────────────────────
    // Helpers
    // ──────────────────────────────────────────────

    private static async Task CreateBucketAsync(HttpClient client, string bucketName)
    {
        var response = await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null);
        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
    }

    private static async Task<HttpResponseMessage> PutObjectAsync(HttpClient client, string bucketName, string key, string payload, string contentType)
    {
        using var request = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/{key}")
        {
            Content = new StringContent(payload, Encoding.UTF8, contentType)
        };
        var response = await client.SendAsync(request);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        return response;
    }

    private static string GetRequiredElementValue(XDocument document, string elementName)
    {
        return document.Root?.Element(elementName)?.Value
            ?? throw new Xunit.Sdk.XunitException($"Missing XML element '{elementName}'.");
    }
}
