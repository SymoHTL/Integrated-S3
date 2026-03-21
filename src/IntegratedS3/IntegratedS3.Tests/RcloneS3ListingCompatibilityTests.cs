using System.Net;
using System.Text;
using System.Xml.Linq;
using IntegratedS3.Tests.Infrastructure;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class RcloneS3ListingCompatibilityTests : IClassFixture<WebUiApplicationFactory>
{
    private static readonly XNamespace S3Ns = "http://s3.amazonaws.com/doc/2006-03-01/";

    private readonly WebUiApplicationFactory _factory;

    public RcloneS3ListingCompatibilityTests(WebUiApplicationFactory factory)
    {
        _factory = factory;
    }

    [Fact]
    public async Task ListObjectsV2_Basic_ReturnsAllObjectsWithExpectedXmlStructure()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-basic-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);
        await PutObjectAsync(client, bucket, "flat.txt", "flat");
        await PutObjectAsync(client, bucket, "nested/deep/file.txt", "nested-deep");
        await PutObjectAsync(client, bucket, "nested/shallow.txt", "nested-shallow");
        await PutObjectAsync(client, bucket, "another.bin", "binary");

        var response = await client.GetAsync($"/integrated-s3/{bucket}?list-type=2");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var doc = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("ListBucketResult", doc.Root?.Name.LocalName);
        Assert.Equal(bucket, RequiredValue(doc, "Name"));
        Assert.Equal("", RequiredValue(doc, "Prefix"));
        Assert.Equal("1000", RequiredValue(doc, "MaxKeys"));
        Assert.Equal("false", RequiredValue(doc, "IsTruncated"));

        var contents = doc.Root!.Elements(S3Ns + "Contents").ToArray();
        Assert.Equal("4", RequiredValue(doc, "KeyCount"));
        Assert.Equal(4, contents.Length);

        var keys = contents.Select(c => c.Element(S3Ns + "Key")?.Value).OrderBy(k => k).ToArray();
        Assert.Equal(["another.bin", "flat.txt", "nested/deep/file.txt", "nested/shallow.txt"], keys);

        foreach (var content in contents) {
            Assert.NotNull(content.Element(S3Ns + "Key")?.Value);
            Assert.NotNull(content.Element(S3Ns + "LastModified")?.Value);
            Assert.NotNull(content.Element(S3Ns + "ETag")?.Value);
            Assert.NotNull(content.Element(S3Ns + "Size")?.Value);
            Assert.NotNull(content.Element(S3Ns + "StorageClass")?.Value);
        }
    }

    [Fact]
    public async Task ListObjectsV2_WithDelimiter_ReturnsCommonPrefixes()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-delim-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);
        await PutObjectAsync(client, bucket, "file1.txt", "f1");
        await PutObjectAsync(client, bucket, "dir1/file2.txt", "f2");
        await PutObjectAsync(client, bucket, "dir1/file3.txt", "f3");
        await PutObjectAsync(client, bucket, "dir2/file4.txt", "f4");

        var response = await client.GetAsync($"/integrated-s3/{bucket}?list-type=2&delimiter=/");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var doc = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("/", RequiredValue(doc, "Delimiter"));

        var objectKeys = doc.Root!.Elements(S3Ns + "Contents")
            .Select(c => c.Element(S3Ns + "Key")?.Value)
            .ToArray();
        Assert.Single(objectKeys);
        Assert.Equal("file1.txt", objectKeys[0]);

        var prefixes = doc.Root.Elements(S3Ns + "CommonPrefixes")
            .Select(cp => cp.Element(S3Ns + "Prefix")?.Value)
            .OrderBy(p => p)
            .ToArray();
        Assert.Equal(2, prefixes.Length);
        Assert.Equal("dir1/", prefixes[0]);
        Assert.Equal("dir2/", prefixes[1]);

        // KeyCount = objects + common prefixes = 3
        Assert.Equal("3", RequiredValue(doc, "KeyCount"));
    }

    [Fact]
    public async Task ListObjectsV2_WithPrefixAndDelimiter_ListsSubdirectoryContents()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-pfxdelim-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);
        await PutObjectAsync(client, bucket, "file1.txt", "f1");
        await PutObjectAsync(client, bucket, "dir1/file2.txt", "f2");
        await PutObjectAsync(client, bucket, "dir1/file3.txt", "f3");
        await PutObjectAsync(client, bucket, "dir2/file4.txt", "f4");

        var response = await client.GetAsync(
            $"/integrated-s3/{bucket}?list-type=2&prefix={Uri.EscapeDataString("dir1/")}&delimiter=/");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var doc = XDocument.Parse(await response.Content.ReadAsStringAsync());

        Assert.Equal("dir1/", RequiredValue(doc, "Prefix"));

        var objectKeys = doc.Root!.Elements(S3Ns + "Contents")
            .Select(c => c.Element(S3Ns + "Key")?.Value)
            .OrderBy(k => k)
            .ToArray();
        Assert.Equal(2, objectKeys.Length);
        Assert.Equal("dir1/file2.txt", objectKeys[0]);
        Assert.Equal("dir1/file3.txt", objectKeys[1]);

        Assert.Empty(doc.Root.Elements(S3Ns + "CommonPrefixes"));
    }

    [Fact]
    public async Task ListObjectsV2_Pagination_ReturnsAllObjectsAcrossPages()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-page-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);
        for (var i = 0; i < 5; i++) {
            await PutObjectAsync(client, bucket, $"obj-{i:D2}.txt", $"content-{i}");
        }

        var allKeys = new List<string>();
        string? continuationToken = null;
        var pageCount = 0;

        do {
            var url = $"/integrated-s3/{bucket}?list-type=2&max-keys=2";
            if (continuationToken is not null) {
                url += $"&continuation-token={Uri.EscapeDataString(continuationToken)}";
            }

            var response = await client.GetAsync(url);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);

            var doc = XDocument.Parse(await response.Content.ReadAsStringAsync());
            var keys = doc.Root!.Elements(S3Ns + "Contents")
                .Select(c => c.Element(S3Ns + "Key")?.Value!)
                .ToList();
            allKeys.AddRange(keys);
            pageCount++;

            var isTruncated = RequiredValue(doc, "IsTruncated");
            if (isTruncated == "true") {
                continuationToken = doc.Root.Element(S3Ns + "NextContinuationToken")?.Value;
                Assert.False(string.IsNullOrWhiteSpace(continuationToken), "NextContinuationToken must be present when IsTruncated=true");
            }
            else {
                continuationToken = null;
            }
        } while (continuationToken is not null);

        Assert.True(pageCount >= 3, $"Expected at least 3 pages, got {pageCount}");
        Assert.Equal(5, allKeys.Count);
        Assert.Equal(
            ["obj-00.txt", "obj-01.txt", "obj-02.txt", "obj-03.txt", "obj-04.txt"],
            allKeys.OrderBy(k => k).ToArray());
    }

    [Fact]
    public async Task ListObjectsV2_WithEncodingTypeUrl_ReturnsUrlEncodedKeys()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-enc-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);
        await PutObjectAsync(client, bucket, "my file.txt", "spaced");
        await PutObjectAsync(client, bucket, "path/to/my doc.txt", "nested-spaced");

        var response = await client.GetAsync(
            $"/integrated-s3/{bucket}?list-type=2&encoding-type=url");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var doc = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("url", RequiredValue(doc, "EncodingType"));

        var keys = doc.Root!.Elements(S3Ns + "Contents")
            .Select(c => c.Element(S3Ns + "Key")?.Value)
            .OrderBy(k => k)
            .ToArray();

        // Keys should be URL-encoded
        Assert.Contains(keys, k => k == "my%20file.txt");
        Assert.Contains(keys, k => k == "path%2Fto%2Fmy%20doc.txt");
    }

    [Fact]
    public async Task ListObjectsV2_EmptyBucket_ReturnsEmptyResult()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-empty-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);

        var response = await client.GetAsync($"/integrated-s3/{bucket}?list-type=2");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var doc = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("0", RequiredValue(doc, "KeyCount"));
        Assert.Equal("false", RequiredValue(doc, "IsTruncated"));
        Assert.Empty(doc.Root!.Elements(S3Ns + "Contents"));
    }

    [Fact]
    public async Task ListObjectsV2_WithFetchOwner_IncludesOwnerElement()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-owner-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);
        await PutObjectAsync(client, bucket, "test.txt", "content");

        // With fetch-owner=true → Owner should be present
        var withOwner = await client.GetAsync(
            $"/integrated-s3/{bucket}?list-type=2&fetch-owner=true");
        Assert.Equal(HttpStatusCode.OK, withOwner.StatusCode);
        var withOwnerDoc = XDocument.Parse(await withOwner.Content.ReadAsStringAsync());
        var ownerElement = Assert.Single(withOwnerDoc.Root!.Elements(S3Ns + "Contents"))
            .Element(S3Ns + "Owner");
        Assert.NotNull(ownerElement);
        Assert.False(string.IsNullOrWhiteSpace(ownerElement!.Element(S3Ns + "ID")?.Value));

        // Without fetch-owner → Owner should NOT be present
        var withoutOwner = await client.GetAsync(
            $"/integrated-s3/{bucket}?list-type=2");
        Assert.Equal(HttpStatusCode.OK, withoutOwner.StatusCode);
        var withoutOwnerDoc = XDocument.Parse(await withoutOwner.Content.ReadAsStringAsync());
        var noOwner = Assert.Single(withoutOwnerDoc.Root!.Elements(S3Ns + "Contents"))
            .Element(S3Ns + "Owner");
        Assert.Null(noOwner);
    }

    [Fact]
    public async Task ListObjectsV1_MarkerBased_ReturnsFallbackXml()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-v1-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);
        await PutObjectAsync(client, bucket, "a.txt", "A");
        await PutObjectAsync(client, bucket, "b.txt", "B");
        await PutObjectAsync(client, bucket, "c.txt", "C");
        await PutObjectAsync(client, bucket, "d.txt", "D");

        // V1 list (no list-type parameter)
        var response = await client.GetAsync($"/integrated-s3/{bucket}");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var doc = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("ListBucketResult", doc.Root?.Name.LocalName);

        // V1 should have Marker, NOT KeyCount
        Assert.NotNull(doc.Root!.Element(S3Ns + "Marker"));
        Assert.Empty(doc.Root.Elements(S3Ns + "KeyCount"));
        Assert.Empty(doc.Root.Elements(S3Ns + "ContinuationToken"));
        Assert.Empty(doc.Root.Elements(S3Ns + "NextContinuationToken"));

        Assert.Equal(4, doc.Root.Elements(S3Ns + "Contents").Count());

        // V1 pagination with marker + max-keys
        var page1 = await client.GetAsync(
            $"/integrated-s3/{bucket}?max-keys=2");
        Assert.Equal(HttpStatusCode.OK, page1.StatusCode);
        var page1Doc = XDocument.Parse(await page1.Content.ReadAsStringAsync());
        Assert.Equal("true", RequiredValue(page1Doc, "IsTruncated"));
        Assert.Equal("2", RequiredValue(page1Doc, "MaxKeys"));
        var page1Keys = page1Doc.Root!.Elements(S3Ns + "Contents")
            .Select(c => c.Element(S3Ns + "Key")?.Value)
            .ToArray();
        Assert.Equal(2, page1Keys.Length);

        // Second page using marker
        var lastKey = page1Keys[^1]!;
        var page2 = await client.GetAsync(
            $"/integrated-s3/{bucket}?marker={Uri.EscapeDataString(lastKey)}&max-keys=2");
        Assert.Equal(HttpStatusCode.OK, page2.StatusCode);
        var page2Doc = XDocument.Parse(await page2.Content.ReadAsStringAsync());
        var page2Keys = page2Doc.Root!.Elements(S3Ns + "Contents")
            .Select(c => c.Element(S3Ns + "Key")?.Value)
            .ToArray();
        Assert.Equal(2, page2Keys.Length);
        Assert.True(StringComparer.Ordinal.Compare(page2Keys[0], lastKey) > 0,
            "Second page keys must come after the marker");
    }

    [Fact]
    public async Task DirectoryMarkerObjects_ListedViaCommonPrefixes_WhenChildObjectsExist()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-dirmarker-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);

        // rclone creates directory markers as zero-byte objects with trailing slash keys.
        // The disk storage provider implicitly treats object key prefixes as directories.
        // Put files that create implicit directory prefixes.
        await PutObjectAsync(client, bucket, "mydir/file.txt", "inside");
        await PutObjectAsync(client, bucket, "otherdir/other.txt", "other");
        await PutObjectAsync(client, bucket, "root.txt", "root");

        // ListObjectsV2 with delimiter=/ should show mydir/ and otherdir/ in CommonPrefixes
        var listResponse = await client.GetAsync(
            $"/integrated-s3/{bucket}?list-type=2&delimiter=/");
        Assert.Equal(HttpStatusCode.OK, listResponse.StatusCode);
        var doc = XDocument.Parse(await listResponse.Content.ReadAsStringAsync());

        var prefixes = doc.Root!.Elements(S3Ns + "CommonPrefixes")
            .Select(cp => cp.Element(S3Ns + "Prefix")?.Value)
            .OrderBy(p => p)
            .ToArray();
        Assert.Contains("mydir/", prefixes);
        Assert.Contains("otherdir/", prefixes);

        // root.txt should appear in Contents (not under a prefix)
        var objectKeys = doc.Root.Elements(S3Ns + "Contents")
            .Select(c => c.Element(S3Ns + "Key")?.Value)
            .ToArray();
        Assert.Single(objectKeys);
        Assert.Equal("root.txt", objectKeys[0]);

        // HeadObject on a file within the directory should succeed
        using var headRequest = new HttpRequestMessage(HttpMethod.Head,
            $"/integrated-s3/{bucket}/mydir/file.txt");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
    }

    [Fact]
    public async Task ListObjectsV2_NestedCommonPrefixes_WorksAcrossMultipleLevels()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-nested-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);
        await PutObjectAsync(client, bucket, "a/b/c.txt", "c");
        await PutObjectAsync(client, bucket, "a/b/d.txt", "d");
        await PutObjectAsync(client, bucket, "a/e/f.txt", "f");

        // List at prefix=a/ with delimiter=/ → should see a/b/ and a/e/
        var response1 = await client.GetAsync(
            $"/integrated-s3/{bucket}?list-type=2&delimiter=/&prefix={Uri.EscapeDataString("a/")}");
        Assert.Equal(HttpStatusCode.OK, response1.StatusCode);
        var doc1 = XDocument.Parse(await response1.Content.ReadAsStringAsync());

        var prefixes = doc1.Root!.Elements(S3Ns + "CommonPrefixes")
            .Select(cp => cp.Element(S3Ns + "Prefix")?.Value)
            .OrderBy(p => p)
            .ToArray();
        Assert.Equal(2, prefixes.Length);
        Assert.Equal("a/b/", prefixes[0]);
        Assert.Equal("a/e/", prefixes[1]);
        Assert.Empty(doc1.Root.Elements(S3Ns + "Contents"));

        // List at prefix=a/b/ with delimiter=/ → should see actual files
        var response2 = await client.GetAsync(
            $"/integrated-s3/{bucket}?list-type=2&delimiter=/&prefix={Uri.EscapeDataString("a/b/")}");
        Assert.Equal(HttpStatusCode.OK, response2.StatusCode);
        var doc2 = XDocument.Parse(await response2.Content.ReadAsStringAsync());

        var objectKeys = doc2.Root!.Elements(S3Ns + "Contents")
            .Select(c => c.Element(S3Ns + "Key")?.Value)
            .OrderBy(k => k)
            .ToArray();
        Assert.Equal(2, objectKeys.Length);
        Assert.Equal("a/b/c.txt", objectKeys[0]);
        Assert.Equal("a/b/d.txt", objectKeys[1]);
        Assert.Empty(doc2.Root.Elements(S3Ns + "CommonPrefixes"));
    }

    [Fact]
    public async Task ListObjectsV2_StartAfter_SkipsEarlierKeys()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-startafter-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);
        await PutObjectAsync(client, bucket, "a.txt", "A");
        await PutObjectAsync(client, bucket, "b.txt", "B");
        await PutObjectAsync(client, bucket, "c.txt", "C");
        await PutObjectAsync(client, bucket, "d.txt", "D");

        var response = await client.GetAsync(
            $"/integrated-s3/{bucket}?list-type=2&start-after=b.txt");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var doc = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("b.txt", RequiredValue(doc, "StartAfter"));

        var keys = doc.Root!.Elements(S3Ns + "Contents")
            .Select(c => c.Element(S3Ns + "Key")?.Value)
            .ToArray();
        Assert.Equal(2, keys.Length);
        Assert.Equal("c.txt", keys[0]);
        Assert.Equal("d.txt", keys[1]);
    }

    [Fact]
    public async Task ListObjectVersions_ReturnsBothVersionsWithDistinctVersionIds()
    {
        using var client = await _factory.CreateClientAsync();
        var bucket = $"rclone-versions-{Guid.NewGuid():N}";

        await CreateBucketAsync(client, bucket);
        await EnableBucketVersioningAsync(client, bucket);

        // PUT the same key twice to create two versions
        var v1Response = await client.PutAsync(
            $"/integrated-s3/{bucket}/versioned.txt",
            new StringContent("version one", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, v1Response.StatusCode);
        var v1Id = v1Response.Headers.TryGetValues("x-amz-version-id", out var v1Vals)
            ? v1Vals.FirstOrDefault() : null;

        var v2Response = await client.PutAsync(
            $"/integrated-s3/{bucket}/versioned.txt",
            new StringContent("version two", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, v2Response.StatusCode);
        var v2Id = v2Response.Headers.TryGetValues("x-amz-version-id", out var v2Vals)
            ? v2Vals.FirstOrDefault() : null;

        Assert.NotNull(v1Id);
        Assert.NotNull(v2Id);
        Assert.NotEqual(v1Id, v2Id);

        // GET /?versions to list all versions
        var versionsResponse = await client.GetAsync($"/integrated-s3/{bucket}?versions");
        Assert.Equal(HttpStatusCode.OK, versionsResponse.StatusCode);
        Assert.Equal("application/xml", versionsResponse.Content.Headers.ContentType?.MediaType);

        var doc = XDocument.Parse(await versionsResponse.Content.ReadAsStringAsync());
        Assert.Equal("ListVersionsResult", doc.Root?.Name.LocalName);

        var versions = doc.Root!.Elements(S3Ns + "Version").ToArray();
        Assert.True(versions.Length >= 2, $"Expected at least 2 versions, got {versions.Length}");

        var versionIds = versions
            .Select(v => v.Element(S3Ns + "VersionId")?.Value)
            .Where(id => id is not null)
            .ToArray();
        Assert.Contains(v1Id, versionIds);
        Assert.Contains(v2Id, versionIds);

        // Verify Version elements have Key, VersionId, IsLatest, LastModified, ETag, Size, StorageClass
        foreach (var version in versions) {
            Assert.NotNull(version.Element(S3Ns + "Key")?.Value);
            Assert.NotNull(version.Element(S3Ns + "VersionId")?.Value);
            Assert.NotNull(version.Element(S3Ns + "IsLatest")?.Value);
            Assert.NotNull(version.Element(S3Ns + "LastModified")?.Value);
            Assert.NotNull(version.Element(S3Ns + "ETag")?.Value);
            Assert.NotNull(version.Element(S3Ns + "Size")?.Value);
            Assert.NotNull(version.Element(S3Ns + "StorageClass")?.Value);
        }

        // Exactly one version should be marked IsLatest=true for this key
        var latestVersions = versions
            .Where(v => v.Element(S3Ns + "Key")?.Value == "versioned.txt"
                        && v.Element(S3Ns + "IsLatest")?.Value == "true")
            .ToArray();
        Assert.Single(latestVersions);
    }

    // --- Helper methods ---

    private static async Task CreateBucketAsync(HttpClient client, string bucket)
    {
        var response = await client.PutAsync($"/integrated-s3/buckets/{bucket}", content: null);
        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
    }

    private static async Task PutObjectAsync(HttpClient client, string bucket, string key, string content)
    {
        var encodedKey = string.Join('/', key.Split('/').Select(Uri.EscapeDataString));
        var response = await client.PutAsync(
            $"/integrated-s3/buckets/{bucket}/objects/{encodedKey}",
            new StringContent(content, Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    private static async Task EnableBucketVersioningAsync(HttpClient client, string bucket)
    {
        using var request = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucket}?versioning")
        {
            Content = new StringContent("""
<VersioningConfiguration>
  <Status>Enabled</Status>
</VersioningConfiguration>
""", Encoding.UTF8, "application/xml")
        };

        var response = await client.SendAsync(request);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    private static string RequiredValue(XDocument document, string elementName)
    {
        return document.Root?.Element(S3Ns + elementName)?.Value
            ?? throw new Xunit.Sdk.XunitException($"Missing XML element '{elementName}'.");
    }
}
