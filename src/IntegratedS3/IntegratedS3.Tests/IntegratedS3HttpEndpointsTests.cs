using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Globalization;
using System.Xml;
using System.Xml.Linq;
using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Observability;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.AspNetCore;
using IntegratedS3.AspNetCore.DependencyInjection;
using IntegratedS3.AspNetCore.Endpoints;
using IntegratedS3.Client;
using IntegratedS3.Core.Models;
using IntegratedS3.Core.Services;
using IntegratedS3.Protocol;
using IntegratedS3.Tests.Infrastructure;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class IntegratedS3HttpEndpointsTests : IClassFixture<WebUiApplicationFactory>
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private static readonly uint[] Crc32Table = CreateCrc32Table();

    private readonly WebUiApplicationFactory _factory;

    public IntegratedS3HttpEndpointsTests(WebUiApplicationFactory factory)
    {
        _factory = factory;
    }

    [Fact]
    public async Task BucketAndObjectEndpoints_RoundTripSuccessfully()
    {
        using var client = await _factory.CreateClientAsync();

        var createBucketResponse = await client.PutAsync("/integrated-s3/buckets/test-bucket", content: null);
        Assert.Equal(HttpStatusCode.Created, createBucketResponse.StatusCode);

        using var uploadRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/buckets/test-bucket/objects/docs/hello.txt")
        {
            Content = new StringContent("hello from xunit", Encoding.UTF8, "text/plain")
        };
        uploadRequest.Headers.Add("x-integrateds3-meta-author", "copilot");
        var expectedChecksum = Convert.ToBase64String(SHA256.HashData(Encoding.UTF8.GetBytes("hello from xunit")));
        var expectedChecksumCrc32c = ChecksumTestAlgorithms.ComputeCrc32cBase64("hello from xunit");
        var uploadResponse = await client.SendAsync(uploadRequest);
        Assert.Equal(HttpStatusCode.OK, uploadResponse.StatusCode);

        var buckets = await client.GetFromJsonAsync<BucketInfo[]>("/integrated-s3/buckets", JsonOptions);
        Assert.NotNull(buckets);
        Assert.Contains(buckets!, bucket => bucket.Name == "test-bucket");

        var objects = await client.GetFromJsonAsync<ObjectInfo[]>("/integrated-s3/buckets/test-bucket/objects", JsonOptions);
        Assert.NotNull(objects);
        var uploadedObject = Assert.Single(objects!);
        Assert.Equal("docs/hello.txt", uploadedObject.Key);
        Assert.StartsWith("text/plain", uploadedObject.ContentType, StringComparison.OrdinalIgnoreCase);
        Assert.Equal("copilot", uploadedObject.Metadata!["author"]);
        Assert.Equal(expectedChecksum, uploadedObject.Checksums!["sha256"]);
        Assert.Equal(expectedChecksumCrc32c, uploadedObject.Checksums["crc32c"]);

        using var headRequest = new HttpRequestMessage(HttpMethod.Head, "/integrated-s3/buckets/test-bucket/objects/docs/hello.txt");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        Assert.Equal("copilot", Assert.Single(headResponse.Headers.GetValues("x-integrateds3-meta-author")));
        Assert.Equal(expectedChecksum, Assert.Single(headResponse.Headers.GetValues("x-amz-checksum-sha256")));
        Assert.Equal(expectedChecksumCrc32c, Assert.Single(headResponse.Headers.GetValues("x-amz-checksum-crc32c")));
        Assert.Equal("text/plain", headResponse.Content.Headers.ContentType?.MediaType);

        var downloadResponse = await client.GetAsync("/integrated-s3/buckets/test-bucket/objects/docs/hello.txt");
        Assert.Equal(HttpStatusCode.OK, downloadResponse.StatusCode);
        Assert.Equal("copilot", Assert.Single(downloadResponse.Headers.GetValues("x-integrateds3-meta-author")));
        Assert.Equal(expectedChecksum, Assert.Single(downloadResponse.Headers.GetValues("x-amz-checksum-sha256")));
        Assert.Equal(expectedChecksumCrc32c, Assert.Single(downloadResponse.Headers.GetValues("x-amz-checksum-crc32c")));
        Assert.Equal("hello from xunit", await downloadResponse.Content.ReadAsStringAsync());

        var deleteObjectResponse = await client.DeleteAsync("/integrated-s3/buckets/test-bucket/objects/docs/hello.txt");
        Assert.Equal(HttpStatusCode.NoContent, deleteObjectResponse.StatusCode);

        var deleteBucketResponse = await client.DeleteAsync("/integrated-s3/buckets/test-bucket");
        Assert.Equal(HttpStatusCode.NoContent, deleteBucketResponse.StatusCode);
    }

    [Fact]
    public async Task S3CompatibleObjectRoutes_RoundTripStandardHeadersAndMetadata()
    {
        using var client = await _factory.CreateClientAsync();
        var expiresUtc = new DateTimeOffset(2026, 3, 14, 15, 0, 0, TimeSpan.Zero);

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/standard-header-bucket", content: null)).StatusCode);

        using var uploadRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/standard-header-bucket/docs/headers.txt")
        {
            Content = new StringContent("standard headers payload", Encoding.UTF8, "text/plain")
        };
        uploadRequest.Headers.CacheControl = CacheControlHeaderValue.Parse("no-store");
        uploadRequest.Content.Headers.ContentDisposition = new ContentDispositionHeaderValue("attachment")
        {
            FileName = "headers.txt"
        };
        uploadRequest.Content.Headers.ContentEncoding.Add("identity");
        uploadRequest.Content.Headers.ContentLanguage.Add("en-US");
        uploadRequest.Content.Headers.Expires = expiresUtc;
        uploadRequest.Headers.TryAddWithoutValidation("x-amz-meta-author", "copilot");
        uploadRequest.Headers.TryAddWithoutValidation("x-amz-meta-origin", "s3-compatible");

        var uploadResponse = await client.SendAsync(uploadRequest);
        Assert.Equal(HttpStatusCode.OK, uploadResponse.StatusCode);

        using var headRequest = new HttpRequestMessage(HttpMethod.Head, "/integrated-s3/standard-header-bucket/docs/headers.txt");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        Assert.Equal("copilot", Assert.Single(headResponse.Headers.GetValues("x-amz-meta-author")));
        Assert.Equal("copilot", Assert.Single(headResponse.Headers.GetValues("x-integrateds3-meta-author")));
        Assert.Equal("s3-compatible", Assert.Single(headResponse.Headers.GetValues("x-amz-meta-origin")));
        Assert.Equal("no-store", headResponse.Headers.CacheControl?.ToString());
        Assert.Equal("attachment", headResponse.Content.Headers.ContentDisposition?.DispositionType);
        Assert.Equal("headers.txt", headResponse.Content.Headers.ContentDisposition?.FileName?.Trim('"'));
        Assert.Contains("identity", headResponse.Content.Headers.ContentEncoding);
        Assert.Contains("en-US", headResponse.Content.Headers.ContentLanguage);
        Assert.Equal(expiresUtc, headResponse.Content.Headers.Expires);
        Assert.Equal("text/plain", headResponse.Content.Headers.ContentType?.MediaType);

        var downloadResponse = await client.GetAsync("/integrated-s3/standard-header-bucket/docs/headers.txt");
        Assert.Equal(HttpStatusCode.OK, downloadResponse.StatusCode);
        Assert.Equal("copilot", Assert.Single(downloadResponse.Headers.GetValues("x-amz-meta-author")));
        Assert.Equal("copilot", Assert.Single(downloadResponse.Headers.GetValues("x-integrateds3-meta-author")));
        Assert.Equal("no-store", downloadResponse.Headers.CacheControl?.ToString());
        Assert.Equal("attachment", downloadResponse.Content.Headers.ContentDisposition?.DispositionType);
        Assert.Equal("headers.txt", downloadResponse.Content.Headers.ContentDisposition?.FileName?.Trim('"'));
        Assert.Contains("identity", downloadResponse.Content.Headers.ContentEncoding);
        Assert.Contains("en-US", downloadResponse.Content.Headers.ContentLanguage);
        Assert.Equal(expiresUtc, downloadResponse.Content.Headers.Expires);
        Assert.Equal("standard headers payload", await downloadResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task PutObject_WithChecksumHeaders_ValidatesPayloadAndEmitsCurrentVersionHeaders()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/versioned-bucket", content: null)).StatusCode);

        const string payload = "hello versioned checksum";
        var checksum = Convert.ToBase64String(SHA256.HashData(Encoding.UTF8.GetBytes(payload)));

        using var putRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/versioned-bucket/docs/versioned.txt")
        {
            Content = new StringContent(payload, Encoding.UTF8, "text/plain")
        };
        putRequest.Headers.TryAddWithoutValidation("x-amz-sdk-checksum-algorithm", "SHA256");
        putRequest.Headers.TryAddWithoutValidation("x-amz-checksum-sha256", checksum);

        var putResponse = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);
        Assert.Equal("SHA256", Assert.Single(putResponse.Headers.GetValues("x-amz-checksum-algorithm")));
        Assert.Equal(checksum, Assert.Single(putResponse.Headers.GetValues("x-amz-checksum-sha256")));
        var versionId = Assert.Single(putResponse.Headers.GetValues("x-amz-version-id"));
        Assert.False(string.IsNullOrWhiteSpace(versionId));

        using var putTaggingRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/versioned-bucket/docs/versioned.txt?tagging&versionId={Uri.EscapeDataString(versionId)}")
        {
            Content = new StringContent("""
<Tagging>
  <TagSet>
    <Tag><Key>owner</Key><Value>copilot</Value></Tag>
  </TagSet>
</Tagging>
""", Encoding.UTF8, "application/xml")
        };

        var putTaggingResponse = await client.SendAsync(putTaggingRequest);
        Assert.Equal(HttpStatusCode.OK, putTaggingResponse.StatusCode);

        var getVersionedObjectResponse = await client.GetAsync($"/integrated-s3/versioned-bucket/docs/versioned.txt?versionId={Uri.EscapeDataString(versionId)}");
        Assert.Equal(HttpStatusCode.OK, getVersionedObjectResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(getVersionedObjectResponse.Headers.GetValues("x-amz-checksum-sha256")));
        Assert.Equal("1", Assert.Single(getVersionedObjectResponse.Headers.GetValues("x-amz-tagging-count")));
        Assert.Equal(versionId, Assert.Single(getVersionedObjectResponse.Headers.GetValues("x-amz-version-id")));
        Assert.Equal(payload, await getVersionedObjectResponse.Content.ReadAsStringAsync());

        using var headVersionedObjectRequest = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/versioned-bucket/docs/versioned.txt?versionId={Uri.EscapeDataString(versionId)}");
        var headVersionedObjectResponse = await client.SendAsync(headVersionedObjectRequest);
        Assert.Equal(HttpStatusCode.OK, headVersionedObjectResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(headVersionedObjectResponse.Headers.GetValues("x-amz-checksum-sha256")));
        Assert.Equal("1", Assert.Single(headVersionedObjectResponse.Headers.GetValues("x-amz-tagging-count")));
        Assert.Equal(versionId, Assert.Single(headVersionedObjectResponse.Headers.GetValues("x-amz-version-id")));

        var getTaggingResponse = await client.GetAsync($"/integrated-s3/versioned-bucket/docs/versioned.txt?tagging&versionId={Uri.EscapeDataString(versionId)}");
        Assert.Equal(HttpStatusCode.OK, getTaggingResponse.StatusCode);
        var taggingDocument = XDocument.Parse(await getTaggingResponse.Content.ReadAsStringAsync());
        Assert.Equal("copilot", taggingDocument.Root!.Element("TagSet")!.Element("Tag")!.Element("Value")!.Value);

        var wrongVersionResponse = await client.GetAsync("/integrated-s3/versioned-bucket/docs/versioned.txt?versionId=missing-version");
        Assert.Equal(HttpStatusCode.NotFound, wrongVersionResponse.StatusCode);
        var errorDocument = XDocument.Parse(await wrongVersionResponse.Content.ReadAsStringAsync());
        Assert.Equal("NoSuchVersion", GetRequiredElementValue(errorDocument, "Code"));

        using var wrongVersionHeadRequest = new HttpRequestMessage(HttpMethod.Head, "/integrated-s3/versioned-bucket/docs/versioned.txt?versionId=missing-version");
        var wrongVersionHeadResponse = await client.SendAsync(wrongVersionHeadRequest);
        Assert.Equal(HttpStatusCode.NotFound, wrongVersionHeadResponse.StatusCode);
        Assert.Equal("NoSuchVersion", Assert.Single(wrongVersionHeadResponse.Headers.GetValues("x-amz-error-code")));
    }

    [Theory]
    [InlineData("x-amz-sdk-checksum-algorithm", "sha1-header-bucket")]
    [InlineData("x-amz-checksum-algorithm", "sha1-algorithm-header-bucket")]
    public async Task PutObject_WithSha1ChecksumHeaders_ValidatesPayloadAndEmitsSha1Headers(string algorithmHeaderName, string bucketName)
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        const string payload = "hello sha1 checksum";
        var checksum = ComputeSha1Base64(payload);

        using var putRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/docs/sha1.txt")
        {
            Content = new StringContent(payload, Encoding.UTF8, "text/plain")
        };
        putRequest.Headers.TryAddWithoutValidation(algorithmHeaderName, "SHA1");
        putRequest.Headers.TryAddWithoutValidation("x-amz-checksum-sha1", checksum);

        var putResponse = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);
        Assert.Equal("SHA1", Assert.Single(putResponse.Headers.GetValues("x-amz-checksum-algorithm")));
        Assert.Equal(checksum, Assert.Single(putResponse.Headers.GetValues("x-amz-checksum-sha1")));
        Assert.False(putResponse.Headers.Contains("x-amz-checksum-sha256"));
        Assert.False(putResponse.Headers.Contains("x-amz-checksum-crc32"));
        Assert.False(putResponse.Headers.Contains("x-amz-checksum-crc32c"));

        using var headRequest = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{bucketName}/docs/sha1.txt");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(headResponse.Headers.GetValues("x-amz-checksum-sha1")));
        Assert.False(headResponse.Headers.Contains("x-amz-checksum-sha256"));
        Assert.False(headResponse.Headers.Contains("x-amz-checksum-crc32"));
        Assert.False(headResponse.Headers.Contains("x-amz-checksum-crc32c"));

        var getResponse = await client.GetAsync($"/integrated-s3/{bucketName}/docs/sha1.txt");
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(getResponse.Headers.GetValues("x-amz-checksum-sha1")));
        Assert.False(getResponse.Headers.Contains("x-amz-checksum-sha256"));
        Assert.False(getResponse.Headers.Contains("x-amz-checksum-crc32"));
        Assert.False(getResponse.Headers.Contains("x-amz-checksum-crc32c"));
        Assert.Equal(payload, await getResponse.Content.ReadAsStringAsync());

        using var badPutRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/docs/bad-sha1.txt")
        {
            Content = new StringContent("sha1 checksum mismatch", Encoding.UTF8, "text/plain")
        };
        badPutRequest.Headers.TryAddWithoutValidation(algorithmHeaderName, "SHA1");
        badPutRequest.Headers.TryAddWithoutValidation("x-amz-checksum-sha1", "invalid-checksum");

        var badPutResponse = await client.SendAsync(badPutRequest);
        Assert.Equal(HttpStatusCode.BadRequest, badPutResponse.StatusCode);
        Assert.Equal("application/xml", badPutResponse.Content.Headers.ContentType?.MediaType);

        var errorDocument = XDocument.Parse(await badPutResponse.Content.ReadAsStringAsync());
        Assert.Equal("BadDigest", GetRequiredElementValue(errorDocument, "Code"));
    }

    [Fact]
    public async Task PutObject_WithCrc32cChecksumHeaders_ValidatesPayloadAndEmitsCrc32cHeaders()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/crc32c-header-bucket", content: null)).StatusCode);

        const string payload = "hello crc32c checksum";
        var checksum = ChecksumTestAlgorithms.ComputeCrc32cBase64(payload);

        using var putRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/crc32c-header-bucket/docs/crc32c.txt")
        {
            Content = new StringContent(payload, Encoding.UTF8, "text/plain")
        };
        putRequest.Headers.TryAddWithoutValidation("x-amz-sdk-checksum-algorithm", "CRC32C");
        putRequest.Headers.TryAddWithoutValidation("x-amz-checksum-crc32c", checksum);

        var putResponse = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);
        Assert.Equal("CRC32C", Assert.Single(putResponse.Headers.GetValues("x-amz-checksum-algorithm")));
        Assert.Equal(checksum, Assert.Single(putResponse.Headers.GetValues("x-amz-checksum-crc32c")));
        Assert.False(putResponse.Headers.Contains("x-amz-checksum-sha256"));
        Assert.False(putResponse.Headers.Contains("x-amz-checksum-sha1"));
        Assert.False(putResponse.Headers.Contains("x-amz-checksum-crc32"));

        using var headRequest = new HttpRequestMessage(HttpMethod.Head, "/integrated-s3/crc32c-header-bucket/docs/crc32c.txt");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(headResponse.Headers.GetValues("x-amz-checksum-crc32c")));
        Assert.False(headResponse.Headers.Contains("x-amz-checksum-sha256"));
        Assert.False(headResponse.Headers.Contains("x-amz-checksum-sha1"));
        Assert.False(headResponse.Headers.Contains("x-amz-checksum-crc32"));

        var getResponse = await client.GetAsync("/integrated-s3/crc32c-header-bucket/docs/crc32c.txt");
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(getResponse.Headers.GetValues("x-amz-checksum-crc32c")));
        Assert.False(getResponse.Headers.Contains("x-amz-checksum-sha256"));
        Assert.False(getResponse.Headers.Contains("x-amz-checksum-sha1"));
        Assert.False(getResponse.Headers.Contains("x-amz-checksum-crc32"));
        Assert.Equal(payload, await getResponse.Content.ReadAsStringAsync());

        using var badPutRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/crc32c-header-bucket/docs/bad-crc32c.txt")
        {
            Content = new StringContent("crc32c checksum mismatch", Encoding.UTF8, "text/plain")
        };
        badPutRequest.Headers.TryAddWithoutValidation("x-amz-sdk-checksum-algorithm", "CRC32C");
        badPutRequest.Headers.TryAddWithoutValidation("x-amz-checksum-crc32c", "invalid-checksum");

        var badPutResponse = await client.SendAsync(badPutRequest);
        Assert.Equal(HttpStatusCode.BadRequest, badPutResponse.StatusCode);
        Assert.Equal("application/xml", badPutResponse.Content.Headers.ContentType?.MediaType);

        var errorDocument = XDocument.Parse(await badPutResponse.Content.ReadAsStringAsync());
        Assert.Equal("BadDigest", GetRequiredElementValue(errorDocument, "Code"));
    }

    [Fact]
    public async Task PutObject_WithMismatchedChecksumHeader_ReturnsBadDigest()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/bad-digest-bucket", content: null)).StatusCode);

        using var putRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/bad-digest-bucket/docs/bad.txt")
        {
            Content = new StringContent("checksum mismatch", Encoding.UTF8, "text/plain")
        };
        putRequest.Headers.TryAddWithoutValidation("x-amz-checksum-sha256", "bad-digest");

        var response = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("BadDigest", GetRequiredElementValue(document, "Code"));
    }

    [Fact]
    public async Task PutObject_WithMultipleChecksumHeaders_ReturnsInvalidRequest()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/multi-checksum-bucket", content: null)).StatusCode);

        const string payload = "multiple checksum headers";
        using var request = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/multi-checksum-bucket/docs/multi.txt")
        {
            Content = new StringContent(payload, Encoding.UTF8, "text/plain")
        };
        request.Headers.TryAddWithoutValidation("x-amz-checksum-sha256", ComputeSha256Base64(payload));
        request.Headers.TryAddWithoutValidation("x-amz-checksum-sha1", ComputeSha1Base64(payload));

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("InvalidRequest", GetRequiredElementValue(document, "Code"));
    }

    [Fact]
    public async Task PutObject_WithAwsChunkedTrailerChecksum_ValidatesPayloadAndEmitsChecksumHeaders()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "aws-chunked-trailer-valid-bucket";
        const string objectKey = "docs/trailer.txt";
        const string payload = "hello from aws chunked trailer";
        var checksum = ComputeSha256Base64(payload);

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var putRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}",
            payload,
            sdkChecksumAlgorithm: "SHA256",
            trailerHeaders: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha256"] = checksum
            });

        var putResponse = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);
        Assert.Equal("SHA256", Assert.Single(putResponse.Headers.GetValues("x-amz-checksum-algorithm")));
        Assert.Equal(checksum, Assert.Single(putResponse.Headers.GetValues("x-amz-checksum-sha256")));

        using var headRequest = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{bucketName}/{objectKey}");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(headResponse.Headers.GetValues("x-amz-checksum-sha256")));

        var getResponse = await client.GetAsync($"/integrated-s3/{bucketName}/{objectKey}");
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(getResponse.Headers.GetValues("x-amz-checksum-sha256")));
        Assert.Equal(payload, await getResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task PutObject_WithUnsignedTrailerBackedPayloadHashWithoutTrailerSignature_Succeeds()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "aws-chunked-unsigned-trailer-valid-bucket";
        const string objectKey = "docs/unsigned-trailer.txt";
        const string payload = "hello from unsigned aws chunked trailer";
        var checksum = ComputeSha256Base64(payload);

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var putRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}",
            payload,
            sdkChecksumAlgorithm: "SHA256",
            trailerHeaders: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha256"] = checksum
            },
            declaredTrailerHeaderNames: ["x-amz-checksum-sha256"]);
        putRequest.Headers.TryAddWithoutValidation("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER");

        var putResponse = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);
        Assert.Equal("SHA256", Assert.Single(putResponse.Headers.GetValues("x-amz-checksum-algorithm")));
        Assert.Equal(checksum, Assert.Single(putResponse.Headers.GetValues("x-amz-checksum-sha256")));

        using var headRequest = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{bucketName}/{objectKey}");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(headResponse.Headers.GetValues("x-amz-checksum-sha256")));

        var getResponse = await client.GetAsync($"/integrated-s3/{bucketName}/{objectKey}");
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(getResponse.Headers.GetValues("x-amz-checksum-sha256")));
        Assert.Equal(payload, await getResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task PutObject_WithUnsignedTrailerBackedSha1Checksum_PersistsAndEmitsSha1Headers()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "aws-chunked-unsigned-trailer-sha1-valid-bucket";
        const string objectKey = "docs/unsigned-trailer-sha1.txt";
        const string payload = "hello from unsigned aws chunked sha1 trailer";
        var checksum = ComputeSha1Base64(payload);

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var putRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}",
            payload,
            sdkChecksumAlgorithm: "SHA1",
            trailerHeaders: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha1"] = checksum
            },
            declaredTrailerHeaderNames: ["x-amz-checksum-sha1"]);
        putRequest.Headers.TryAddWithoutValidation("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER");

        var putResponse = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);
        Assert.Equal("SHA1", Assert.Single(putResponse.Headers.GetValues("x-amz-checksum-algorithm")));
        Assert.Equal(checksum, Assert.Single(putResponse.Headers.GetValues("x-amz-checksum-sha1")));
        Assert.False(putResponse.Headers.Contains("x-amz-checksum-sha256"));
        Assert.False(putResponse.Headers.Contains("x-amz-checksum-crc32"));
        Assert.False(putResponse.Headers.Contains("x-amz-checksum-crc32c"));

        using var headRequest = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{bucketName}/{objectKey}");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(headResponse.Headers.GetValues("x-amz-checksum-sha1")));
        Assert.False(headResponse.Headers.Contains("x-amz-checksum-sha256"));
        Assert.False(headResponse.Headers.Contains("x-amz-checksum-crc32"));
        Assert.False(headResponse.Headers.Contains("x-amz-checksum-crc32c"));

        var getResponse = await client.GetAsync($"/integrated-s3/{bucketName}/{objectKey}");
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(getResponse.Headers.GetValues("x-amz-checksum-sha1")));
        Assert.False(getResponse.Headers.Contains("x-amz-checksum-sha256"));
        Assert.False(getResponse.Headers.Contains("x-amz-checksum-crc32"));
        Assert.False(getResponse.Headers.Contains("x-amz-checksum-crc32c"));
        Assert.Equal(payload, await getResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task PutObject_WithUnsignedTrailerBackedCrc32cChecksum_PersistsAndEmitsCrc32cHeaders()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "aws-chunked-unsigned-trailer-crc32c-valid-bucket";
        const string objectKey = "docs/unsigned-trailer-crc32c.txt";
        const string payload = "hello from unsigned aws chunked crc32c trailer";
        var checksum = ChecksumTestAlgorithms.ComputeCrc32cBase64(payload);

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var putRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}",
            payload,
            sdkChecksumAlgorithm: "CRC32C",
            trailerHeaders: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-crc32c"] = checksum
            },
            declaredTrailerHeaderNames: ["x-amz-checksum-crc32c"]);
        putRequest.Headers.TryAddWithoutValidation("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER");

        var putResponse = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);
        Assert.Equal("CRC32C", Assert.Single(putResponse.Headers.GetValues("x-amz-checksum-algorithm")));
        Assert.Equal(checksum, Assert.Single(putResponse.Headers.GetValues("x-amz-checksum-crc32c")));
        Assert.False(putResponse.Headers.Contains("x-amz-checksum-sha256"));
        Assert.False(putResponse.Headers.Contains("x-amz-checksum-sha1"));
        Assert.False(putResponse.Headers.Contains("x-amz-checksum-crc32"));

        using var headRequest = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{bucketName}/{objectKey}");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(headResponse.Headers.GetValues("x-amz-checksum-crc32c")));
        Assert.False(headResponse.Headers.Contains("x-amz-checksum-sha256"));
        Assert.False(headResponse.Headers.Contains("x-amz-checksum-sha1"));
        Assert.False(headResponse.Headers.Contains("x-amz-checksum-crc32"));

        var getResponse = await client.GetAsync($"/integrated-s3/{bucketName}/{objectKey}");
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(getResponse.Headers.GetValues("x-amz-checksum-crc32c")));
        Assert.False(getResponse.Headers.Contains("x-amz-checksum-sha256"));
        Assert.False(getResponse.Headers.Contains("x-amz-checksum-sha1"));
        Assert.False(getResponse.Headers.Contains("x-amz-checksum-crc32"));
        Assert.Equal(payload, await getResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task PutObject_WithUnsignedTrailerBackedCrc32Checksum_PersistsAndEmitsCrc32Headers()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "aws-chunked-unsigned-trailer-crc32-valid-bucket";
        const string objectKey = "docs/unsigned-trailer-crc32.txt";
        const string payload = "hello from unsigned aws chunked crc32 trailer";
        var checksum = ComputeCrc32Base64(payload);

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var putRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}",
            payload,
            sdkChecksumAlgorithm: "CRC32",
            trailerHeaders: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-crc32"] = checksum
            },
            declaredTrailerHeaderNames: ["x-amz-checksum-crc32"]);
        putRequest.Headers.TryAddWithoutValidation("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER");

        var putResponse = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);
        Assert.Equal("CRC32", Assert.Single(putResponse.Headers.GetValues("x-amz-checksum-algorithm")));
        Assert.Equal(checksum, Assert.Single(putResponse.Headers.GetValues("x-amz-checksum-crc32")));
        Assert.False(putResponse.Headers.Contains("x-amz-checksum-sha256"));
        Assert.False(putResponse.Headers.Contains("x-amz-checksum-sha1"));
        Assert.False(putResponse.Headers.Contains("x-amz-checksum-crc32c"));

        using var headRequest = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{bucketName}/{objectKey}");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(headResponse.Headers.GetValues("x-amz-checksum-crc32")));
        Assert.False(headResponse.Headers.Contains("x-amz-checksum-sha256"));
        Assert.False(headResponse.Headers.Contains("x-amz-checksum-sha1"));
        Assert.False(headResponse.Headers.Contains("x-amz-checksum-crc32c"));

        var getResponse = await client.GetAsync($"/integrated-s3/{bucketName}/{objectKey}");
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(getResponse.Headers.GetValues("x-amz-checksum-crc32")));
        Assert.False(getResponse.Headers.Contains("x-amz-checksum-sha256"));
        Assert.False(getResponse.Headers.Contains("x-amz-checksum-sha1"));
        Assert.False(getResponse.Headers.Contains("x-amz-checksum-crc32c"));
        Assert.Equal(payload, await getResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task PutObject_WithUnsignedTrailerBackedChecksumMismatch_ReturnsBadDigest()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "aws-chunked-unsigned-trailer-bad-digest-bucket";
        const string objectKey = "docs/unsigned-trailer-bad-digest.txt";
        const string payload = "hello from unsigned aws chunked trailer";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var putRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}",
            payload,
            sdkChecksumAlgorithm: "SHA256",
            trailerHeaders: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha256"] = ComputeSha256Base64("different payload")
            },
            declaredTrailerHeaderNames: ["x-amz-checksum-sha256"]);
        putRequest.Headers.TryAddWithoutValidation("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER");

        var response = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("BadDigest", GetRequiredElementValue(document, "Code"));
        var message = GetRequiredElementValue(document, "Message");
        Assert.Contains("SHA256", message, StringComparison.Ordinal);
        Assert.Contains(objectKey, message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutObject_WithUnsignedTrailerBackedSha1ChecksumMismatch_ReturnsBadDigest()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "aws-chunked-unsigned-trailer-sha1-bad-digest-bucket";
        const string objectKey = "docs/unsigned-trailer-sha1-bad-digest.txt";
        const string payload = "hello from unsigned aws chunked sha1 trailer";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var putRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}",
            payload,
            sdkChecksumAlgorithm: "SHA1",
            trailerHeaders: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha1"] = ComputeSha1Base64("different payload")
            },
            declaredTrailerHeaderNames: ["x-amz-checksum-sha1"]);
        putRequest.Headers.TryAddWithoutValidation("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER");

        var response = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("BadDigest", GetRequiredElementValue(document, "Code"));
        var message = GetRequiredElementValue(document, "Message");
        Assert.Contains("SHA1", message, StringComparison.Ordinal);
        Assert.Contains(objectKey, message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutObject_WithUnsignedTrailerBackedCrc32cChecksumMismatch_ReturnsBadDigest()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "aws-chunked-unsigned-trailer-crc32c-bad-digest-bucket";
        const string objectKey = "docs/unsigned-trailer-crc32c-bad-digest.txt";
        const string payload = "hello from unsigned aws chunked crc32c trailer";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var putRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}",
            payload,
            sdkChecksumAlgorithm: "CRC32C",
            trailerHeaders: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-crc32c"] = ChecksumTestAlgorithms.ComputeCrc32cBase64("different payload")
            },
            declaredTrailerHeaderNames: ["x-amz-checksum-crc32c"]);
        putRequest.Headers.TryAddWithoutValidation("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER");

        var response = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("BadDigest", GetRequiredElementValue(document, "Code"));
        var message = GetRequiredElementValue(document, "Message");
        Assert.Contains("CRC32C", message, StringComparison.Ordinal);
        Assert.Contains(objectKey, message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutObject_WithUnsignedTrailerBackedCrc32ChecksumMismatch_ReturnsBadDigest()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "aws-chunked-unsigned-trailer-crc32-bad-digest-bucket";
        const string objectKey = "docs/unsigned-trailer-crc32-bad-digest.txt";
        const string payload = "hello from unsigned aws chunked crc32 trailer";
        var checksum = ComputeCrc32Base64("different payload");

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var putRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}",
            payload,
            sdkChecksumAlgorithm: "CRC32",
            trailerHeaders: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-crc32"] = checksum
            },
            declaredTrailerHeaderNames: ["x-amz-checksum-crc32"]);
        putRequest.Headers.TryAddWithoutValidation("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER");

        var response = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("BadDigest", GetRequiredElementValue(document, "Code"));
        var message = GetRequiredElementValue(document, "Message");
        Assert.Contains("CRC32", message, StringComparison.Ordinal);
        Assert.Contains(objectKey, message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutObject_WithTrailerBackedPayloadHashWithoutTrailerSignature_ReturnsInvalidRequest()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "aws-chunked-trailer-signature-missing-bucket";
        const string objectKey = "docs/trailer-signature-missing.txt";
        const string payload = "hello from missing aws chunked trailer signature";
        var checksum = ComputeSha256Base64(payload);

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var putRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}",
            payload,
            sdkChecksumAlgorithm: "SHA256",
            trailerHeaders: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha256"] = checksum
            },
            declaredTrailerHeaderNames: ["x-amz-checksum-sha256"]);
        putRequest.Headers.TryAddWithoutValidation("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER");

        var response = await client.SendAsync(putRequest);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("InvalidRequest", GetRequiredElementValue(document, "Code"));
        Assert.Contains("x-amz-trailer-signature", GetRequiredElementValue(document, "Message"), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("SHA1", "x-amz-checksum-sha1", "aws-chunked-signed-trailer-sha1-valid-bucket", "docs/signed-trailer-sha1.txt", "hello from signed aws chunked sha1 trailer")]
    [InlineData("CRC32", "x-amz-checksum-crc32", "aws-chunked-signed-trailer-crc32-valid-bucket", "docs/signed-trailer-crc32.txt", "hello from signed aws chunked crc32 trailer")]
    [InlineData("CRC32C", "x-amz-checksum-crc32c", "aws-chunked-signed-trailer-crc32c-valid-bucket", "docs/signed-trailer-crc32c.txt", "hello from signed aws chunked crc32c trailer")]
    public async Task PutObject_WithSigV4AwsChunkedTrailerSignatureAndNonSha256Checksum_PersistsAndEmitsCompatibleChecksumHeaders(
        string sdkChecksumAlgorithm,
        string trailerHeaderName,
        string bucketName,
        string objectKey,
        string payload)
    {
        const string accessKeyId = "sigv4-http-chunked-signed-trailer-access";
        const string secretAccessKey = "sigv4-http-chunked-signed-trailer-secret";
        var checksum = ComputeTrailerChecksumBase64(trailerHeaderName, payload);

        await using var isolatedClient = await CreateSigV4AuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var putRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/{bucketName}/{objectKey}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [trailerHeaderName] = checksum
            });
        await AssertSigV4AwsChunkedTrailerRequestAsync(putRequest, sdkChecksumAlgorithm, trailerHeaderName);

        var putResponse = await client.SendAsync(putRequest);

        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);
        Assert.Equal(sdkChecksumAlgorithm, Assert.Single(putResponse.Headers.GetValues("x-amz-checksum-algorithm")));
        AssertOnlyExpectedChecksumHeader(putResponse.Headers, trailerHeaderName, checksum);

        using var headRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Head, $"/integrated-s3/{bucketName}/{objectKey}", accessKeyId, secretAccessKey);
        var headResponse = await client.SendAsync(headRequest);

        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        AssertOnlyExpectedChecksumHeader(headResponse.Headers, trailerHeaderName, checksum);

        using var getRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Get, $"/integrated-s3/{bucketName}/{objectKey}", accessKeyId, secretAccessKey);
        var getResponse = await client.SendAsync(getRequest);

        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        AssertOnlyExpectedChecksumHeader(getResponse.Headers, trailerHeaderName, checksum);
        Assert.Equal(payload, await getResponse.Content.ReadAsStringAsync());
    }

    [Theory]
    [InlineData("SHA1", "x-amz-checksum-sha1", "aws-chunked-signed-trailer-sha1-bad-digest-bucket", "docs/signed-trailer-sha1-bad-digest.txt", "hello from signed aws chunked sha1 trailer bad digest")]
    [InlineData("CRC32", "x-amz-checksum-crc32", "aws-chunked-signed-trailer-crc32-bad-digest-bucket", "docs/signed-trailer-crc32-bad-digest.txt", "hello from signed aws chunked crc32 trailer bad digest")]
    [InlineData("CRC32C", "x-amz-checksum-crc32c", "aws-chunked-signed-trailer-crc32c-bad-digest-bucket", "docs/signed-trailer-crc32c-bad-digest.txt", "hello from signed aws chunked crc32c trailer bad digest")]
    public async Task PutObject_WithSigV4AwsChunkedTrailerSignatureAndNonSha256ChecksumMismatch_ReturnsBadDigest(
        string sdkChecksumAlgorithm,
        string trailerHeaderName,
        string bucketName,
        string objectKey,
        string payload)
    {
        const string accessKeyId = "sigv4-http-chunked-signed-trailer-access";
        const string secretAccessKey = "sigv4-http-chunked-signed-trailer-secret";
        var mismatchedChecksum = ComputeTrailerChecksumBase64(trailerHeaderName, "different payload");

        await using var isolatedClient = await CreateSigV4AuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var putRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/{bucketName}/{objectKey}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [trailerHeaderName] = mismatchedChecksum
            });
        await AssertSigV4AwsChunkedTrailerRequestAsync(putRequest, sdkChecksumAlgorithm, trailerHeaderName);

        var response = await client.SendAsync(putRequest);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("BadDigest", GetRequiredElementValue(document, "Code"));
        var message = GetRequiredElementValue(document, "Message");
        Assert.Contains(sdkChecksumAlgorithm, message, StringComparison.Ordinal);
        Assert.Contains(objectKey, message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutObject_WithAwsChunkedTrailerChecksumWithoutTrailerDeclaration_ReturnsInvalidRequest()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "aws-chunked-trailer-missing-declaration-bucket";
        const string objectKey = "docs/trailer-missing-declaration.txt";
        const string payload = "hello from aws chunked trailer declaration";
        var checksum = ComputeSha256Base64(payload);

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var putRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}",
            payload,
            sdkChecksumAlgorithm: "SHA256",
            trailerHeaders: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha256"] = checksum
            },
            declaredTrailerHeaderNames: []);

        var response = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("InvalidRequest", GetRequiredElementValue(document, "Code"));
        Assert.Equal("Missing required header for this request: x-amz-trailer", GetRequiredElementValue(document, "Message"));
    }

    [Fact]
    public async Task PutObject_WithAwsChunkedTrailerMultipleChecksums_ReturnsInvalidRequest()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "aws-chunked-trailer-multi-checksum-bucket";
        const string objectKey = "docs/trailer-multi.txt";
        const string payload = "multiple trailer checksum headers";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var putRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}",
            payload,
            trailerHeaders: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha256"] = ComputeSha256Base64(payload),
                ["x-amz-checksum-sha1"] = ComputeSha1Base64(payload)
            });

        var response = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("InvalidRequest", GetRequiredElementValue(document, "Code"));
        Assert.Equal("Expecting a single x-amz-checksum-* header. Multiple checksum types are not allowed.", GetRequiredElementValue(document, "Message"));
    }

    [Fact]
    public async Task PutObject_WithDuplicateAwsChunkedChecksumTrailer_ReturnsInvalidRequest()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "aws-chunked-trailer-duplicate-bucket";
        const string objectKey = "docs/trailer-duplicate.txt";
        const string payload = "duplicate trailer checksum headers";
        var checksum = ComputeSha256Base64(payload);

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var putRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}",
            payload,
            sdkChecksumAlgorithm: "SHA256",
            declaredTrailerHeaderNames: ["x-amz-checksum-sha256"],
            trailerHeaderEntries:
            [
                new KeyValuePair<string, string>("x-amz-checksum-sha256", checksum),
                new KeyValuePair<string, string>("x-amz-checksum-sha256", ComputeSha256Base64("different payload"))
            ]);

        var response = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("InvalidRequest", GetRequiredElementValue(document, "Code"));
        Assert.Equal(
            "The aws-chunked trailer header 'x-amz-checksum-sha256' must not be repeated.",
            GetRequiredElementValue(document, "Message"));
    }

    [Fact]
    public async Task PutObject_WithAwsChunkedTrailerChecksumMismatch_ReturnsBadDigest()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "aws-chunked-trailer-bad-digest-bucket";
        const string objectKey = "docs/trailer-bad.txt";
        const string payload = "aws chunked trailer checksum mismatch";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var putRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}",
            payload,
            sdkChecksumAlgorithm: "SHA256",
            trailerHeaders: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha256"] = ComputeSha256Base64("different payload")
            });

        var response = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("BadDigest", GetRequiredElementValue(document, "Code"));
        var message = GetRequiredElementValue(document, "Message");
        Assert.Contains("SHA256", message, StringComparison.Ordinal);
        Assert.Contains(objectKey, message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task S3CompatibleBucketVersioning_RoundTripsXmlPayload()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/versioning-config-bucket", content: null)).StatusCode);

        var initialGet = await client.GetAsync("/integrated-s3/versioning-config-bucket?versioning");
        Assert.Equal(HttpStatusCode.OK, initialGet.StatusCode);
        var initialDocument = XDocument.Parse(await initialGet.Content.ReadAsStringAsync());
        Assert.Equal("VersioningConfiguration", initialDocument.Root?.Name.LocalName);
        Assert.Null(initialDocument.Root?.Element("Status"));

        using var putVersioningRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/versioning-config-bucket?versioning")
        {
            Content = new StringContent("""
<VersioningConfiguration>
  <Status>Enabled</Status>
</VersioningConfiguration>
""", Encoding.UTF8, "application/xml")
        };

        var putVersioningResponse = await client.SendAsync(putVersioningRequest);
        Assert.Equal(HttpStatusCode.OK, putVersioningResponse.StatusCode);

        var enabledGet = await client.GetAsync("/integrated-s3/versioning-config-bucket?versioning");
        Assert.Equal(HttpStatusCode.OK, enabledGet.StatusCode);
        var enabledDocument = XDocument.Parse(await enabledGet.Content.ReadAsStringAsync());
        Assert.Equal("Enabled", GetRequiredElementValue(enabledDocument, "Status"));
    }

    [Fact]
    public async Task S3CompatibleBucketLocation_ReturnsXmlPayload()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/location-config-bucket", content: null)).StatusCode);

        var locationResponse = await client.GetAsync("/integrated-s3/location-config-bucket?location");
        Assert.Equal(HttpStatusCode.OK, locationResponse.StatusCode);
        Assert.Equal("application/xml", locationResponse.Content.Headers.ContentType?.MediaType);

        var locationDocument = XDocument.Parse(await locationResponse.Content.ReadAsStringAsync());
        Assert.Equal("LocationConstraint", locationDocument.Root?.Name.LocalName);
        Assert.Equal(string.Empty, locationDocument.Root?.Value);
    }

    [Fact]
    public async Task S3CompatibleBucketCors_RoundTripsXmlPayload()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/cors-config-bucket", content: null)).StatusCode);

        var missingGet = await client.GetAsync("/integrated-s3/cors-config-bucket?cors");
        Assert.Equal(HttpStatusCode.NotFound, missingGet.StatusCode);
        var missingDocument = XDocument.Parse(await missingGet.Content.ReadAsStringAsync());
        Assert.Equal("NoSuchCORSConfiguration", GetRequiredElementValue(missingDocument, "Code"));

        using var putCorsRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/cors-config-bucket?cors")
        {
            Content = new StringContent("""
<CORSConfiguration>
  <CORSRule>
    <ID>browser-rule</ID>
    <AllowedOrigin>https://app.example</AllowedOrigin>
    <AllowedMethod>GET</AllowedMethod>
    <AllowedMethod>PUT</AllowedMethod>
    <AllowedHeader>authorization</AllowedHeader>
    <AllowedHeader>x-amz-*</AllowedHeader>
    <ExposeHeader>etag</ExposeHeader>
    <MaxAgeSeconds>600</MaxAgeSeconds>
  </CORSRule>
</CORSConfiguration>
""", Encoding.UTF8, "application/xml")
        };

        var putCorsResponse = await client.SendAsync(putCorsRequest);
        Assert.Equal(HttpStatusCode.OK, putCorsResponse.StatusCode);

        var getCorsResponse = await client.GetAsync("/integrated-s3/cors-config-bucket?cors");
        Assert.Equal(HttpStatusCode.OK, getCorsResponse.StatusCode);
        Assert.Equal("application/xml", getCorsResponse.Content.Headers.ContentType?.MediaType);

        var corsDocument = XDocument.Parse(await getCorsResponse.Content.ReadAsStringAsync());
        Assert.Equal("CORSConfiguration", corsDocument.Root?.Name.LocalName);
        var rule = Assert.Single(corsDocument.Root!.Elements("CORSRule"));
        Assert.Equal("browser-rule", rule.Element("ID")?.Value);
        Assert.Equal(["GET", "PUT"], rule.Elements("AllowedMethod").Select(static element => element.Value).ToArray());
        Assert.Equal(["https://app.example"], rule.Elements("AllowedOrigin").Select(static element => element.Value).ToArray());
        Assert.Equal(["authorization", "x-amz-*"], rule.Elements("AllowedHeader").Select(static element => element.Value).ToArray());
        Assert.Equal("etag", rule.Element("ExposeHeader")?.Value);
        Assert.Equal("600", rule.Element("MaxAgeSeconds")?.Value);

        var deleteCorsResponse = await client.DeleteAsync("/integrated-s3/cors-config-bucket?cors");
        Assert.Equal(HttpStatusCode.NoContent, deleteCorsResponse.StatusCode);

        var missingAfterDelete = await client.GetAsync("/integrated-s3/cors-config-bucket?cors");
        Assert.Equal(HttpStatusCode.NotFound, missingAfterDelete.StatusCode);
        var deletedDocument = XDocument.Parse(await missingAfterDelete.Content.ReadAsStringAsync());
        Assert.Equal("NoSuchCORSConfiguration", GetRequiredElementValue(deletedDocument, "Code"));
    }

    [Fact]
    public async Task S3CompatibleBucketDefaultEncryption_WithMissingBucket_ReturnsNoSuchBucket()
    {
        using var client = await _factory.CreateClientAsync();

        var getResponse = await client.GetAsync("/integrated-s3/missing-encryption-bucket?encryption");
        await AssertErrorResponseAsync(getResponse, HttpStatusCode.NotFound, "NoSuchBucket");

        using var putRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/missing-encryption-bucket?encryption")
        {
            Content = new StringContent("""
<ServerSideEncryptionConfiguration>
  <Rule>
    <ApplyServerSideEncryptionByDefault>
      <SSEAlgorithm>AES256</SSEAlgorithm>
    </ApplyServerSideEncryptionByDefault>
  </Rule>
</ServerSideEncryptionConfiguration>
""", Encoding.UTF8, "application/xml")
        };

        var putResponse = await client.SendAsync(putRequest);
        await AssertErrorResponseAsync(putResponse, HttpStatusCode.NotFound, "NoSuchBucket");

        var deleteResponse = await client.DeleteAsync("/integrated-s3/missing-encryption-bucket?encryption");
        await AssertErrorResponseAsync(deleteResponse, HttpStatusCode.NotFound, "NoSuchBucket");
    }

    [Fact]
    public async Task S3CompatibleBucketDefaultEncryption_WithUnsupportedDiskProvider_ReturnsNotImplemented()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/encryption-unsupported-bucket", content: null)).StatusCode);

        var getResponse = await client.GetAsync("/integrated-s3/encryption-unsupported-bucket?encryption");
        await AssertNotImplementedResponseAsync(getResponse);

        using var putRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/encryption-unsupported-bucket?encryption")
        {
            Content = new StringContent("""
<ServerSideEncryptionConfiguration>
  <Rule>
    <ApplyServerSideEncryptionByDefault>
      <SSEAlgorithm>AES256</SSEAlgorithm>
    </ApplyServerSideEncryptionByDefault>
  </Rule>
</ServerSideEncryptionConfiguration>
""", Encoding.UTF8, "application/xml")
        };

        var putResponse = await client.SendAsync(putRequest);
        await AssertNotImplementedResponseAsync(putResponse);

        var deleteResponse = await client.DeleteAsync("/integrated-s3/encryption-unsupported-bucket?encryption");
        await AssertNotImplementedResponseAsync(deleteResponse);
    }

    [Fact]
    public async Task S3CompatibleBucketDefaultEncryption_RoundTripsXmlPayload()
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        using var putRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/encryption-config-bucket?encryption")
        {
            Content = new StringContent("""
<ServerSideEncryptionConfiguration>
  <Rule>
    <ApplyServerSideEncryptionByDefault>
      <SSEAlgorithm>aws:kms:dsse</SSEAlgorithm>
      <KMSMasterKeyID>alias/default-key</KMSMasterKeyID>
    </ApplyServerSideEncryptionByDefault>
  </Rule>
</ServerSideEncryptionConfiguration>
""", Encoding.UTF8, "application/xml")
        };

        var putResponse = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);

        var putDefaultEncryption = storageService.LastPutBucketDefaultEncryptionRequest
            ?? throw new Xunit.Sdk.XunitException("Expected bucket default encryption request to reach the storage service.");
        Assert.Equal(ObjectServerSideEncryptionAlgorithm.KmsDsse, putDefaultEncryption.Rule.Algorithm);
        Assert.Equal("alias/default-key", putDefaultEncryption.Rule.KeyId);

        var getResponse = await client.GetAsync("/integrated-s3/encryption-config-bucket?encryption");
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        Assert.Equal("application/xml", getResponse.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await getResponse.Content.ReadAsStringAsync());
        Assert.Equal("ServerSideEncryptionConfiguration", document.Root?.Name.LocalName);
        var rule = Assert.Single(document.Root!.Elements("Rule"));
        var defaultEncryption = rule.Element("ApplyServerSideEncryptionByDefault")
            ?? throw new Xunit.Sdk.XunitException("Expected ApplyServerSideEncryptionByDefault element.");
        Assert.Equal("aws:kms:dsse", defaultEncryption.Element("SSEAlgorithm")?.Value);
        Assert.Equal("alias/default-key", defaultEncryption.Element("KMSMasterKeyID")?.Value);

        var deleteResponse = await client.DeleteAsync("/integrated-s3/encryption-config-bucket?encryption");
        Assert.Equal(HttpStatusCode.NoContent, deleteResponse.StatusCode);

        var missingResponse = await client.GetAsync("/integrated-s3/encryption-config-bucket?encryption");
        await AssertErrorResponseAsync(missingResponse, HttpStatusCode.NotFound, "ServerSideEncryptionConfigurationNotFoundError");
    }

    [Fact]
    public async Task S3CompatibleBucketDefaultEncryption_WithAes256AndKmsKeyId_ReturnsInvalidArgument()
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        using var putRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/encryption-invalid-bucket?encryption")
        {
            Content = new StringContent("""
<ServerSideEncryptionConfiguration>
  <Rule>
    <ApplyServerSideEncryptionByDefault>
      <SSEAlgorithm>AES256</SSEAlgorithm>
      <KMSMasterKeyID>alias/invalid</KMSMasterKeyID>
    </ApplyServerSideEncryptionByDefault>
  </Rule>
</ServerSideEncryptionConfiguration>
""", Encoding.UTF8, "application/xml")
        };

        var response = await client.SendAsync(putRequest);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("InvalidArgument", GetRequiredElementValue(document, "Code"));
        Assert.Contains("KMSMasterKeyID", GetRequiredElementValue(document, "Message"));
        Assert.Null(storageService.LastPutBucketDefaultEncryptionRequest);
    }

    [Fact]
    public async Task S3CompatibleBucketDefaultEncryption_WithBucketKeyEnabledTrue_ReturnsNotImplemented()
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        using var putRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/encryption-bucket-key-bucket?encryption")
        {
            Content = new StringContent("""
<ServerSideEncryptionConfiguration>
  <Rule>
    <ApplyServerSideEncryptionByDefault>
      <SSEAlgorithm>aws:kms</SSEAlgorithm>
      <KMSMasterKeyID>alias/default-key</KMSMasterKeyID>
    </ApplyServerSideEncryptionByDefault>
    <BucketKeyEnabled>true</BucketKeyEnabled>
  </Rule>
</ServerSideEncryptionConfiguration>
""", Encoding.UTF8, "application/xml")
        };

        var response = await client.SendAsync(putRequest);

        await AssertNotImplementedResponseAsync(response);
        Assert.Null(storageService.LastPutBucketDefaultEncryptionRequest);
    }

    [Fact]
    public async Task S3CompatibleBucketDefaultEncryption_WithMultipleRules_ReturnsInvalidRequest()
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        using var putRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/encryption-multi-rule-bucket?encryption")
        {
            Content = new StringContent("""
<ServerSideEncryptionConfiguration>
  <Rule>
    <ApplyServerSideEncryptionByDefault>
      <SSEAlgorithm>AES256</SSEAlgorithm>
    </ApplyServerSideEncryptionByDefault>
  </Rule>
  <Rule>
    <ApplyServerSideEncryptionByDefault>
      <SSEAlgorithm>aws:kms</SSEAlgorithm>
    </ApplyServerSideEncryptionByDefault>
  </Rule>
</ServerSideEncryptionConfiguration>
""", Encoding.UTF8, "application/xml")
        };

        var response = await client.SendAsync(putRequest);

        await AssertErrorResponseAsync(response, HttpStatusCode.BadRequest, "InvalidRequest");
        Assert.Null(storageService.LastPutBucketDefaultEncryptionRequest);
    }

    [Fact]
    public async Task BucketCors_PreflightSkipsAuthorization_AndMatchingRequestsEmitHeaders()
    {
        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(builder => {
            builder.Services.AddAuthentication("TestHeader")
                .AddScheme<AuthenticationSchemeOptions, TestHeaderAuthenticationHandler>("TestHeader", static _ => { });
            builder.Services.AddSingleton<IIntegratedS3AuthorizationService, ScopeBasedIntegratedS3AuthorizationService>();
        });

        using var client = isolatedClient.Client;
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "storage.read storage.write");

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/browser-cors-bucket", content: null)).StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await client.PutAsync(
            "/integrated-s3/buckets/browser-cors-bucket/objects/docs/browser.txt",
            new StringContent("hello browser", Encoding.UTF8, "text/plain"))).StatusCode);

        using var putCorsRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/browser-cors-bucket?cors")
        {
            Content = new StringContent("""
<CORSConfiguration>
  <CORSRule>
    <AllowedOrigin>https://app.example</AllowedOrigin>
    <AllowedMethod>GET</AllowedMethod>
    <AllowedMethod>PUT</AllowedMethod>
    <AllowedHeader>authorization</AllowedHeader>
    <AllowedHeader>x-amz-*</AllowedHeader>
    <ExposeHeader>etag</ExposeHeader>
    <MaxAgeSeconds>600</MaxAgeSeconds>
  </CORSRule>
</CORSConfiguration>
""", Encoding.UTF8, "application/xml")
        };
        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(putCorsRequest)).StatusCode);

        client.DefaultRequestHeaders.Authorization = null;

        using var preflightRequest = new HttpRequestMessage(HttpMethod.Options, "/integrated-s3/buckets/browser-cors-bucket/objects/docs/browser.txt");
        preflightRequest.Headers.Add("Origin", "https://app.example");
        preflightRequest.Headers.TryAddWithoutValidation("Access-Control-Request-Method", "PUT");
        preflightRequest.Headers.TryAddWithoutValidation("Access-Control-Request-Headers", "authorization, x-amz-meta-test");

        var preflightResponse = await client.SendAsync(preflightRequest);
        Assert.Equal(HttpStatusCode.OK, preflightResponse.StatusCode);
        Assert.Equal("https://app.example", Assert.Single(preflightResponse.Headers.GetValues("Access-Control-Allow-Origin")));
        Assert.Equal("true", Assert.Single(preflightResponse.Headers.GetValues("Access-Control-Allow-Credentials")));
        Assert.Equal("PUT", Assert.Single(preflightResponse.Headers.GetValues("Access-Control-Allow-Methods")));
        Assert.Equal("authorization, x-amz-meta-test", Assert.Single(preflightResponse.Headers.GetValues("Access-Control-Allow-Headers")));
        Assert.Equal("etag", Assert.Single(preflightResponse.Headers.GetValues("Access-Control-Expose-Headers")));
        Assert.Equal("600", Assert.Single(preflightResponse.Headers.GetValues("Access-Control-Max-Age")));
        var preflightVary = string.Join(", ", preflightResponse.Headers.Vary);
        Assert.Contains("Origin", preflightVary, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Access-Control-Request-Method", preflightVary, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Access-Control-Request-Headers", preflightVary, StringComparison.OrdinalIgnoreCase);

        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "storage.read");

        using var getRequest = new HttpRequestMessage(HttpMethod.Get, "/integrated-s3/browser-cors-bucket/docs/browser.txt");
        getRequest.Headers.Add("Origin", "https://app.example");

        var getResponse = await client.SendAsync(getRequest);
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        Assert.Equal("https://app.example", Assert.Single(getResponse.Headers.GetValues("Access-Control-Allow-Origin")));
        Assert.Equal("true", Assert.Single(getResponse.Headers.GetValues("Access-Control-Allow-Credentials")));
        Assert.Equal("etag", Assert.Single(getResponse.Headers.GetValues("Access-Control-Expose-Headers")));
        Assert.Contains("Origin", string.Join(", ", getResponse.Headers.Vary), StringComparison.OrdinalIgnoreCase);
        Assert.Equal("hello browser", await getResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task BucketCors_DisallowedOriginsStillEmitVaryHeaders()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/browser-cors-vary-bucket", content: null)).StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await client.PutAsync(
            "/integrated-s3/buckets/browser-cors-vary-bucket/objects/docs/browser.txt",
            new StringContent("hello browser", Encoding.UTF8, "text/plain"))).StatusCode);

        using var putCorsRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/browser-cors-vary-bucket?cors")
        {
            Content = new StringContent("""
<CORSConfiguration>
  <CORSRule>
    <AllowedOrigin>https://app.example</AllowedOrigin>
    <AllowedMethod>GET</AllowedMethod>
    <AllowedMethod>PUT</AllowedMethod>
    <AllowedHeader>authorization</AllowedHeader>
    <AllowedHeader>x-amz-*</AllowedHeader>
    <ExposeHeader>etag</ExposeHeader>
    <MaxAgeSeconds>600</MaxAgeSeconds>
  </CORSRule>
</CORSConfiguration>
""", Encoding.UTF8, "application/xml")
        };
        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(putCorsRequest)).StatusCode);

        using var preflightRequest = new HttpRequestMessage(HttpMethod.Options, "/integrated-s3/buckets/browser-cors-vary-bucket/objects/docs/browser.txt");
        preflightRequest.Headers.Add("Origin", "https://other.example");
        preflightRequest.Headers.TryAddWithoutValidation("Access-Control-Request-Method", "PUT");
        preflightRequest.Headers.TryAddWithoutValidation("Access-Control-Request-Headers", "authorization");

        var preflightResponse = await client.SendAsync(preflightRequest);
        Assert.Equal(HttpStatusCode.Forbidden, preflightResponse.StatusCode);
        Assert.False(preflightResponse.Headers.Contains("Access-Control-Allow-Origin"));
        Assert.False(preflightResponse.Headers.Contains("Access-Control-Allow-Credentials"));
        var preflightVary = string.Join(", ", preflightResponse.Headers.Vary);
        Assert.Contains("Origin", preflightVary, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Access-Control-Request-Method", preflightVary, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Access-Control-Request-Headers", preflightVary, StringComparison.OrdinalIgnoreCase);

        using var getRequest = new HttpRequestMessage(HttpMethod.Get, "/integrated-s3/browser-cors-vary-bucket/docs/browser.txt");
        getRequest.Headers.Add("Origin", "https://other.example");

        var getResponse = await client.SendAsync(getRequest);
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        Assert.False(getResponse.Headers.Contains("Access-Control-Allow-Origin"));
        Assert.False(getResponse.Headers.Contains("Access-Control-Allow-Credentials"));
        Assert.Contains("Origin", string.Join(", ", getResponse.Headers.Vary), StringComparison.OrdinalIgnoreCase);
        Assert.Equal("hello browser", await getResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task BucketCors_WildcardOriginsReturnAsteriskAndOmitCredentialsHeaders()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/browser-cors-wildcard-bucket", content: null)).StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await client.PutAsync(
            "/integrated-s3/buckets/browser-cors-wildcard-bucket/objects/docs/browser.txt",
            new StringContent("hello browser", Encoding.UTF8, "text/plain"))).StatusCode);

        using var putCorsRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/browser-cors-wildcard-bucket?cors")
        {
            Content = new StringContent("""
<CORSConfiguration>
  <CORSRule>
    <AllowedOrigin>*</AllowedOrigin>
    <AllowedMethod>GET</AllowedMethod>
    <AllowedMethod>PUT</AllowedMethod>
    <AllowedHeader>*</AllowedHeader>
    <ExposeHeader>etag</ExposeHeader>
    <MaxAgeSeconds>600</MaxAgeSeconds>
  </CORSRule>
</CORSConfiguration>
""", Encoding.UTF8, "application/xml")
        };
        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(putCorsRequest)).StatusCode);

        using var preflightRequest = new HttpRequestMessage(HttpMethod.Options, "/integrated-s3/buckets/browser-cors-wildcard-bucket/objects/docs/browser.txt");
        preflightRequest.Headers.Add("Origin", "https://any.example");
        preflightRequest.Headers.TryAddWithoutValidation("Access-Control-Request-Method", "PUT");
        preflightRequest.Headers.TryAddWithoutValidation("Access-Control-Request-Headers", "x-amz-meta-test");

        var preflightResponse = await client.SendAsync(preflightRequest);
        Assert.Equal(HttpStatusCode.OK, preflightResponse.StatusCode);
        Assert.Equal("*", Assert.Single(preflightResponse.Headers.GetValues("Access-Control-Allow-Origin")));
        Assert.False(preflightResponse.Headers.Contains("Access-Control-Allow-Credentials"));
        Assert.Equal("PUT", Assert.Single(preflightResponse.Headers.GetValues("Access-Control-Allow-Methods")));
        Assert.Equal("x-amz-meta-test", Assert.Single(preflightResponse.Headers.GetValues("Access-Control-Allow-Headers")));
        Assert.Equal("etag", Assert.Single(preflightResponse.Headers.GetValues("Access-Control-Expose-Headers")));
        Assert.Equal("600", Assert.Single(preflightResponse.Headers.GetValues("Access-Control-Max-Age")));

        using var getRequest = new HttpRequestMessage(HttpMethod.Get, "/integrated-s3/browser-cors-wildcard-bucket/docs/browser.txt");
        getRequest.Headers.Add("Origin", "https://any.example");

        var getResponse = await client.SendAsync(getRequest);
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        Assert.Equal("*", Assert.Single(getResponse.Headers.GetValues("Access-Control-Allow-Origin")));
        Assert.False(getResponse.Headers.Contains("Access-Control-Allow-Credentials"));
        Assert.Equal("etag", Assert.Single(getResponse.Headers.GetValues("Access-Control-Expose-Headers")));
        Assert.Equal("hello browser", await getResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task S3CompatibleHistoricalVersionAccess_RoundTripsVersionIdsAfterOverwrite()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/history-http-bucket", content: null)).StatusCode);

        using var enableVersioningRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/history-http-bucket?versioning")
        {
            Content = new StringContent("""
<VersioningConfiguration>
  <Status>Enabled</Status>
</VersioningConfiguration>
""", Encoding.UTF8, "application/xml")
        };

        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(enableVersioningRequest)).StatusCode);

        var v1Response = await client.PutAsync(
            "/integrated-s3/history-http-bucket/docs/history.txt",
            new StringContent("http version one", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, v1Response.StatusCode);
        var v1VersionId = Assert.Single(v1Response.Headers.GetValues("x-amz-version-id"));

        var v2Response = await client.PutAsync(
            "/integrated-s3/history-http-bucket/docs/history.txt",
            new StringContent("http version two", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, v2Response.StatusCode);
        var v2VersionId = Assert.Single(v2Response.Headers.GetValues("x-amz-version-id"));
        Assert.NotEqual(v1VersionId, v2VersionId);

        var currentGet = await client.GetAsync("/integrated-s3/history-http-bucket/docs/history.txt");
        Assert.Equal(HttpStatusCode.OK, currentGet.StatusCode);
        Assert.Equal(v2VersionId, Assert.Single(currentGet.Headers.GetValues("x-amz-version-id")));
        Assert.Equal("http version two", await currentGet.Content.ReadAsStringAsync());

        var historicalGet = await client.GetAsync($"/integrated-s3/history-http-bucket/docs/history.txt?versionId={Uri.EscapeDataString(v1VersionId)}");
        Assert.Equal(HttpStatusCode.OK, historicalGet.StatusCode);
        Assert.Equal(v1VersionId, Assert.Single(historicalGet.Headers.GetValues("x-amz-version-id")));
        Assert.Equal("http version one", await historicalGet.Content.ReadAsStringAsync());

        var deleteHistorical = await client.DeleteAsync($"/integrated-s3/history-http-bucket/docs/history.txt?versionId={Uri.EscapeDataString(v1VersionId)}");
        Assert.Equal(HttpStatusCode.NoContent, deleteHistorical.StatusCode);

        var missingHistorical = await client.GetAsync($"/integrated-s3/history-http-bucket/docs/history.txt?versionId={Uri.EscapeDataString(v1VersionId)}");
        Assert.Equal(HttpStatusCode.NotFound, missingHistorical.StatusCode);

        var currentStillThere = await client.GetAsync("/integrated-s3/history-http-bucket/docs/history.txt");
        Assert.Equal(HttpStatusCode.OK, currentStillThere.StatusCode);
        Assert.Equal("http version two", await currentStillThere.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task S3CompatibleHeadObject_WithIfMatchAndIfUnmodifiedSince_PrefersIfMatchPrecedence()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/head-precedence-bucket", content: null)).StatusCode);

        var uploadResponse = await client.PutAsync(
            "/integrated-s3/head-precedence-bucket/docs/conditional.txt",
            new StringContent("hello integrated s3", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, uploadResponse.StatusCode);

        var objectHeaders = await GetHeadObjectMetadataAsync(client, "/integrated-s3/head-precedence-bucket/docs/conditional.txt");

        using var failedRequest = new HttpRequestMessage(HttpMethod.Head, "/integrated-s3/head-precedence-bucket/docs/conditional.txt");
        failedRequest.Headers.TryAddWithoutValidation("If-Match", "\"different\"");
        failedRequest.Headers.TryAddWithoutValidation("If-Unmodified-Since", objectHeaders.LastModifiedUtc.AddMinutes(5).ToString("R"));

        var failedResponse = await client.SendAsync(failedRequest);
        Assert.Equal(HttpStatusCode.PreconditionFailed, failedResponse.StatusCode);
        Assert.Equal(objectHeaders.ETag, failedResponse.Headers.ETag?.Tag);
        Assert.Equal(objectHeaders.LastModifiedUtc.ToString("R"), failedResponse.Content.Headers.LastModified?.ToString("R"));

        using var successfulRequest = new HttpRequestMessage(HttpMethod.Head, "/integrated-s3/head-precedence-bucket/docs/conditional.txt");
        successfulRequest.Headers.TryAddWithoutValidation("If-Match", objectHeaders.ETag);
        successfulRequest.Headers.TryAddWithoutValidation("If-Unmodified-Since", objectHeaders.LastModifiedUtc.AddMinutes(-5).ToString("R"));

        var successfulResponse = await client.SendAsync(successfulRequest);
        Assert.Equal(HttpStatusCode.OK, successfulResponse.StatusCode);
        Assert.Equal(objectHeaders.ETag, successfulResponse.Headers.ETag?.Tag);
        Assert.Equal(objectHeaders.LastModifiedUtc.ToString("R"), successfulResponse.Content.Headers.LastModified?.ToString("R"));
    }

    [Fact]
    public async Task S3CompatibleHistoricalVersionAccess_WithIfNoneMatchAndIfModifiedSince_UsesRequestedVersionMetadata()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/history-conditional-http-bucket", content: null)).StatusCode);
        await EnableBucketVersioningAsync(client, "history-conditional-http-bucket");

        var v1Response = await client.PutAsync(
            "/integrated-s3/history-conditional-http-bucket/docs/history.txt",
            new StringContent("http version one", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, v1Response.StatusCode);
        var v1VersionId = Assert.Single(v1Response.Headers.GetValues("x-amz-version-id"));

        var v2Response = await client.PutAsync(
            "/integrated-s3/history-conditional-http-bucket/docs/history.txt",
            new StringContent("http version two", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, v2Response.StatusCode);
        var v2VersionId = Assert.Single(v2Response.Headers.GetValues("x-amz-version-id"));
        Assert.NotEqual(v1VersionId, v2VersionId);

        var v1Path = $"/integrated-s3/history-conditional-http-bucket/docs/history.txt?versionId={Uri.EscapeDataString(v1VersionId)}";
        var v2Path = $"/integrated-s3/history-conditional-http-bucket/docs/history.txt?versionId={Uri.EscapeDataString(v2VersionId)}";
        var v1Headers = await GetHeadObjectMetadataAsync(client, v1Path);
        var v2Headers = await GetHeadObjectMetadataAsync(client, v2Path);
        Assert.Equal(v1VersionId, v1Headers.VersionId);
        Assert.Equal(v2VersionId, v2Headers.VersionId);
        Assert.NotEqual(v1Headers.ETag, v2Headers.ETag);

        using var historicalGetRequest = new HttpRequestMessage(HttpMethod.Get, v1Path);
        historicalGetRequest.Headers.TryAddWithoutValidation("If-None-Match", v2Headers.ETag);
        historicalGetRequest.Headers.IfModifiedSince = v1Headers.LastModifiedUtc.AddMinutes(5);

        var historicalGet = await client.SendAsync(historicalGetRequest);
        Assert.Equal(HttpStatusCode.OK, historicalGet.StatusCode);
        Assert.Equal(v1VersionId, Assert.Single(historicalGet.Headers.GetValues("x-amz-version-id")));
        Assert.Equal(v1Headers.ETag, historicalGet.Headers.ETag?.Tag);
        Assert.Equal("http version one", await historicalGet.Content.ReadAsStringAsync());

        using var notModifiedRequest = new HttpRequestMessage(HttpMethod.Get, v1Path);
        notModifiedRequest.Headers.TryAddWithoutValidation("If-None-Match", v1Headers.ETag);
        notModifiedRequest.Headers.IfModifiedSince = v1Headers.LastModifiedUtc.AddMinutes(-5);

        var notModifiedResponse = await client.SendAsync(notModifiedRequest);
        Assert.Equal(HttpStatusCode.NotModified, notModifiedResponse.StatusCode);
        Assert.Equal(v1VersionId, Assert.Single(notModifiedResponse.Headers.GetValues("x-amz-version-id")));
        Assert.Equal(v1Headers.ETag, notModifiedResponse.Headers.ETag?.Tag);
        Assert.Empty(await notModifiedResponse.Content.ReadAsByteArrayAsync());
    }

    [Fact]
    public async Task S3CompatibleDeleteObjectTagging_ClearsCurrentAndHistoricalVersionTagSets()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/tag-delete-bucket", content: null)).StatusCode);

        using var enableVersioningRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/tag-delete-bucket?versioning")
        {
            Content = new StringContent("""
<VersioningConfiguration>
  <Status>Enabled</Status>
</VersioningConfiguration>
""", Encoding.UTF8, "application/xml")
        };
        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(enableVersioningRequest)).StatusCode);

        var v1Response = await client.PutAsync(
            "/integrated-s3/tag-delete-bucket/docs/tagged.txt",
            new StringContent("http version one", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, v1Response.StatusCode);
        var v1VersionId = Assert.Single(v1Response.Headers.GetValues("x-amz-version-id"));

        using var putV1TaggingRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/tag-delete-bucket/docs/tagged.txt?tagging&versionId={Uri.EscapeDataString(v1VersionId)}")
        {
            Content = new StringContent("""
<Tagging>
  <TagSet>
    <Tag><Key>generation</Key><Value>one</Value></Tag>
  </TagSet>
</Tagging>
""", Encoding.UTF8, "application/xml")
        };
        var putV1TaggingResponse = await client.SendAsync(putV1TaggingRequest);
        Assert.Equal(HttpStatusCode.OK, putV1TaggingResponse.StatusCode);
        Assert.Equal(v1VersionId, Assert.Single(putV1TaggingResponse.Headers.GetValues("x-amz-version-id")));

        var v2Response = await client.PutAsync(
            "/integrated-s3/tag-delete-bucket/docs/tagged.txt",
            new StringContent("http version two", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, v2Response.StatusCode);
        var v2VersionId = Assert.Single(v2Response.Headers.GetValues("x-amz-version-id"));
        Assert.NotEqual(v1VersionId, v2VersionId);

        using var putCurrentTaggingRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/tag-delete-bucket/docs/tagged.txt?tagging")
        {
            Content = new StringContent("""
<Tagging>
  <TagSet>
    <Tag><Key>generation</Key><Value>two</Value></Tag>
  </TagSet>
</Tagging>
""", Encoding.UTF8, "application/xml")
        };
        var putCurrentTaggingResponse = await client.SendAsync(putCurrentTaggingRequest);
        Assert.Equal(HttpStatusCode.OK, putCurrentTaggingResponse.StatusCode);
        Assert.Equal(v2VersionId, Assert.Single(putCurrentTaggingResponse.Headers.GetValues("x-amz-version-id")));

        var deleteCurrentTags = await client.DeleteAsync("/integrated-s3/tag-delete-bucket/docs/tagged.txt?tagging");
        Assert.Equal(HttpStatusCode.NoContent, deleteCurrentTags.StatusCode);
        Assert.Equal(v2VersionId, Assert.Single(deleteCurrentTags.Headers.GetValues("x-amz-version-id")));

        var currentTagsResponse = await client.GetAsync("/integrated-s3/tag-delete-bucket/docs/tagged.txt?tagging");
        Assert.Equal(HttpStatusCode.OK, currentTagsResponse.StatusCode);
        Assert.Equal(v2VersionId, Assert.Single(currentTagsResponse.Headers.GetValues("x-amz-version-id")));
        var currentTagsDocument = XDocument.Parse(await currentTagsResponse.Content.ReadAsStringAsync());
        Assert.Empty(currentTagsDocument.Root!.Element("TagSet")!.Elements("Tag"));

        var historicalTagsBeforeDelete = await client.GetAsync($"/integrated-s3/tag-delete-bucket/docs/tagged.txt?tagging&versionId={Uri.EscapeDataString(v1VersionId)}");
        Assert.Equal(HttpStatusCode.OK, historicalTagsBeforeDelete.StatusCode);
        Assert.Equal(v1VersionId, Assert.Single(historicalTagsBeforeDelete.Headers.GetValues("x-amz-version-id")));
        var historicalTagsDocument = XDocument.Parse(await historicalTagsBeforeDelete.Content.ReadAsStringAsync());
        Assert.Equal("one", historicalTagsDocument.Root!.Element("TagSet")!.Element("Tag")!.Element("Value")!.Value);

        var deleteHistoricalTags = await client.DeleteAsync($"/integrated-s3/tag-delete-bucket/docs/tagged.txt?tagging&versionId={Uri.EscapeDataString(v1VersionId)}");
        Assert.Equal(HttpStatusCode.NoContent, deleteHistoricalTags.StatusCode);
        Assert.Equal(v1VersionId, Assert.Single(deleteHistoricalTags.Headers.GetValues("x-amz-version-id")));

        var historicalTagsAfterDelete = await client.GetAsync($"/integrated-s3/tag-delete-bucket/docs/tagged.txt?tagging&versionId={Uri.EscapeDataString(v1VersionId)}");
        Assert.Equal(HttpStatusCode.OK, historicalTagsAfterDelete.StatusCode);
        Assert.Equal(v1VersionId, Assert.Single(historicalTagsAfterDelete.Headers.GetValues("x-amz-version-id")));
        var clearedHistoricalTagsDocument = XDocument.Parse(await historicalTagsAfterDelete.Content.ReadAsStringAsync());
        Assert.Empty(clearedHistoricalTagsDocument.Root!.Element("TagSet")!.Elements("Tag"));
    }

    [Fact]
    public async Task S3CompatibleDeleteMarker_TaggingOperationsReturnNoSuchKey()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/delete-marker-tagging-bucket", content: null)).StatusCode);

        using var enableVersioningRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/delete-marker-tagging-bucket?versioning")
        {
            Content = new StringContent("""
<VersioningConfiguration>
  <Status>Enabled</Status>
</VersioningConfiguration>
""", Encoding.UTF8, "application/xml")
        };
        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(enableVersioningRequest)).StatusCode);

        var putObjectResponse = await client.PutAsync(
            "/integrated-s3/delete-marker-tagging-bucket/docs/tagged.txt",
            new StringContent("delete marker tags", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, putObjectResponse.StatusCode);

        var deleteCurrent = await client.DeleteAsync("/integrated-s3/delete-marker-tagging-bucket/docs/tagged.txt");
        Assert.Equal(HttpStatusCode.NoContent, deleteCurrent.StatusCode);
        Assert.Equal("true", Assert.Single(deleteCurrent.Headers.GetValues("x-amz-delete-marker")));
        var deleteMarkerVersionId = Assert.Single(deleteCurrent.Headers.GetValues("x-amz-version-id"));

        using var putTaggingRequest = new HttpRequestMessage(
            HttpMethod.Put,
            $"/integrated-s3/delete-marker-tagging-bucket/docs/tagged.txt?tagging&versionId={Uri.EscapeDataString(deleteMarkerVersionId)}")
        {
            Content = new StringContent("""
<Tagging>
  <TagSet>
    <Tag><Key>generation</Key><Value>delete-marker</Value></Tag>
  </TagSet>
</Tagging>
""", Encoding.UTF8, "application/xml")
        };

        await AssertNoSuchKeyResponseAsync(await client.SendAsync(putTaggingRequest));
        await AssertNoSuchKeyResponseAsync(await client.GetAsync($"/integrated-s3/delete-marker-tagging-bucket/docs/tagged.txt?tagging&versionId={Uri.EscapeDataString(deleteMarkerVersionId)}"));
        await AssertNoSuchKeyResponseAsync(await client.DeleteAsync($"/integrated-s3/delete-marker-tagging-bucket/docs/tagged.txt?tagging&versionId={Uri.EscapeDataString(deleteMarkerVersionId)}"));

        static async Task AssertNoSuchKeyResponseAsync(HttpResponseMessage response)
        {
            Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
            Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
            var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
            Assert.Equal("NoSuchKey", GetRequiredElementValue(errorDocument, "Code"));
        }
    }

    [Fact]
    public async Task S3CompatibleDeleteMarker_GetAndHeadRequestsEmitDeleteMarkerHeaders()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/delete-marker-read-bucket", content: null)).StatusCode);

        using var enableVersioningRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/delete-marker-read-bucket?versioning")
        {
            Content = new StringContent("""
<VersioningConfiguration>
  <Status>Enabled</Status>
</VersioningConfiguration>
""", Encoding.UTF8, "application/xml")
        };
        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(enableVersioningRequest)).StatusCode);

        var putObjectResponse = await client.PutAsync(
            "/integrated-s3/delete-marker-read-bucket/docs/deleted.txt",
            new StringContent("delete marker reads", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, putObjectResponse.StatusCode);

        var deleteCurrent = await client.DeleteAsync("/integrated-s3/delete-marker-read-bucket/docs/deleted.txt");
        Assert.Equal(HttpStatusCode.NoContent, deleteCurrent.StatusCode);
        Assert.Equal("true", Assert.Single(deleteCurrent.Headers.GetValues("x-amz-delete-marker")));
        var deleteMarkerVersionId = Assert.Single(deleteCurrent.Headers.GetValues("x-amz-version-id"));

        var versionsResponse = await client.GetAsync("/integrated-s3/delete-marker-read-bucket?versions&prefix=docs/deleted.txt");
        Assert.Equal(HttpStatusCode.OK, versionsResponse.StatusCode);
        var versionsDocument = XDocument.Parse(await versionsResponse.Content.ReadAsStringAsync());
        var deleteMarker = Assert.Single(versionsDocument.Root!.Elements("DeleteMarker"));
        var expectedLastModified = DateTimeOffset.Parse(deleteMarker.Element("LastModified")!.Value).ToString("R");

        var currentGet = await client.GetAsync("/integrated-s3/delete-marker-read-bucket/docs/deleted.txt");
        Assert.Equal(HttpStatusCode.NotFound, currentGet.StatusCode);
        Assert.Equal("true", Assert.Single(currentGet.Headers.GetValues("x-amz-delete-marker")));
        Assert.Equal(deleteMarkerVersionId, Assert.Single(currentGet.Headers.GetValues("x-amz-version-id")));
        Assert.Null(currentGet.Content.Headers.LastModified);
        var currentGetError = XDocument.Parse(await currentGet.Content.ReadAsStringAsync());
        Assert.Equal("NoSuchKey", GetRequiredElementValue(currentGetError, "Code"));

        using var currentHeadRequest = new HttpRequestMessage(HttpMethod.Head, "/integrated-s3/delete-marker-read-bucket/docs/deleted.txt");
        var currentHead = await client.SendAsync(currentHeadRequest);
        Assert.Equal(HttpStatusCode.NotFound, currentHead.StatusCode);
        Assert.Equal("true", Assert.Single(currentHead.Headers.GetValues("x-amz-delete-marker")));
        Assert.Equal(deleteMarkerVersionId, Assert.Single(currentHead.Headers.GetValues("x-amz-version-id")));
        Assert.Null(currentHead.Content.Headers.LastModified);

        var explicitGet = await client.GetAsync($"/integrated-s3/delete-marker-read-bucket/docs/deleted.txt?versionId={Uri.EscapeDataString(deleteMarkerVersionId)}");
        Assert.Equal(HttpStatusCode.MethodNotAllowed, explicitGet.StatusCode);
        Assert.Equal("true", Assert.Single(explicitGet.Headers.GetValues("x-amz-delete-marker")));
        Assert.Equal(deleteMarkerVersionId, Assert.Single(explicitGet.Headers.GetValues("x-amz-version-id")));
        Assert.Equal(expectedLastModified, explicitGet.Content.Headers.LastModified?.ToString("R"));
        var explicitGetError = XDocument.Parse(await explicitGet.Content.ReadAsStringAsync());
        Assert.Equal("MethodNotAllowed", GetRequiredElementValue(explicitGetError, "Code"));

        using var explicitHeadRequest = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/delete-marker-read-bucket/docs/deleted.txt?versionId={Uri.EscapeDataString(deleteMarkerVersionId)}");
        var explicitHead = await client.SendAsync(explicitHeadRequest);
        Assert.Equal(HttpStatusCode.MethodNotAllowed, explicitHead.StatusCode);
        Assert.Equal("true", Assert.Single(explicitHead.Headers.GetValues("x-amz-delete-marker")));
        Assert.Equal(deleteMarkerVersionId, Assert.Single(explicitHead.Headers.GetValues("x-amz-version-id")));
        Assert.Equal(expectedLastModified, explicitHead.Content.Headers.LastModified?.ToString("R"));
    }

    [Fact]
    public async Task S3CompatibleDeleteMarker_ConditionalReadRequestsPreserveCurrentAndExplicitVersionResponses()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/delete-marker-conditional-bucket", content: null)).StatusCode);
        await EnableBucketVersioningAsync(client, "delete-marker-conditional-bucket");

        var putObjectResponse = await client.PutAsync(
            "/integrated-s3/delete-marker-conditional-bucket/docs/deleted.txt",
            new StringContent("delete marker reads", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, putObjectResponse.StatusCode);

        var deleteCurrent = await client.DeleteAsync("/integrated-s3/delete-marker-conditional-bucket/docs/deleted.txt");
        Assert.Equal(HttpStatusCode.NoContent, deleteCurrent.StatusCode);
        Assert.Equal("true", Assert.Single(deleteCurrent.Headers.GetValues("x-amz-delete-marker")));
        var deleteMarkerVersionId = Assert.Single(deleteCurrent.Headers.GetValues("x-amz-version-id"));

        var versionsResponse = await client.GetAsync("/integrated-s3/delete-marker-conditional-bucket?versions&prefix=docs/deleted.txt");
        Assert.Equal(HttpStatusCode.OK, versionsResponse.StatusCode);
        var versionsDocument = XDocument.Parse(await versionsResponse.Content.ReadAsStringAsync());
        var deleteMarker = Assert.Single(versionsDocument.Root!.Elements("DeleteMarker"));
        var expectedLastModified = DateTimeOffset.Parse(deleteMarker.Element("LastModified")!.Value).ToString("R");

        using var currentGetRequest = new HttpRequestMessage(HttpMethod.Get, "/integrated-s3/delete-marker-conditional-bucket/docs/deleted.txt");
        currentGetRequest.Headers.TryAddWithoutValidation("If-None-Match", "\"different\"");
        currentGetRequest.Headers.IfModifiedSince = DateTimeOffset.UtcNow.AddMinutes(5);

        var currentGet = await client.SendAsync(currentGetRequest);
        Assert.Equal(HttpStatusCode.NotFound, currentGet.StatusCode);
        Assert.Equal("true", Assert.Single(currentGet.Headers.GetValues("x-amz-delete-marker")));
        Assert.Equal(deleteMarkerVersionId, Assert.Single(currentGet.Headers.GetValues("x-amz-version-id")));
        Assert.Null(currentGet.Content.Headers.LastModified);
        var currentGetError = XDocument.Parse(await currentGet.Content.ReadAsStringAsync());
        Assert.Equal("NoSuchKey", GetRequiredElementValue(currentGetError, "Code"));

        using var explicitHeadRequest = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/delete-marker-conditional-bucket/docs/deleted.txt?versionId={Uri.EscapeDataString(deleteMarkerVersionId)}");
        explicitHeadRequest.Headers.TryAddWithoutValidation("If-Match", "\"different\"");
        explicitHeadRequest.Headers.TryAddWithoutValidation("If-Unmodified-Since", DateTimeOffset.UtcNow.AddMinutes(5).ToString("R"));

        var explicitHead = await client.SendAsync(explicitHeadRequest);
        Assert.Equal(HttpStatusCode.MethodNotAllowed, explicitHead.StatusCode);
        Assert.Equal("true", Assert.Single(explicitHead.Headers.GetValues("x-amz-delete-marker")));
        Assert.Equal(deleteMarkerVersionId, Assert.Single(explicitHead.Headers.GetValues("x-amz-version-id")));
        Assert.Equal(expectedLastModified, explicitHead.Content.Headers.LastModified?.ToString("R"));
    }

    [Fact]
    public async Task S3CompatibleListObjectVersions_ReportsDeleteMarkersAndHistoricalVersions()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/history-versions-bucket", content: null)).StatusCode);

        using var enableVersioningRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/history-versions-bucket?versioning")
        {
            Content = new StringContent("""
<VersioningConfiguration>
  <Status>Enabled</Status>
</VersioningConfiguration>
""", Encoding.UTF8, "application/xml")
        };

        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(enableVersioningRequest)).StatusCode);

        var v1Response = await client.PutAsync(
            "/integrated-s3/history-versions-bucket/docs/history.txt",
            new StringContent("http version one", Encoding.UTF8, "text/plain"));
        var v1VersionId = Assert.Single(v1Response.Headers.GetValues("x-amz-version-id"));

        var v2Response = await client.PutAsync(
            "/integrated-s3/history-versions-bucket/docs/history.txt",
            new StringContent("http version two", Encoding.UTF8, "text/plain"));
        var v2VersionId = Assert.Single(v2Response.Headers.GetValues("x-amz-version-id"));

        var deleteCurrent = await client.DeleteAsync("/integrated-s3/history-versions-bucket/docs/history.txt");
        Assert.Equal(HttpStatusCode.NoContent, deleteCurrent.StatusCode);
        Assert.Equal("true", Assert.Single(deleteCurrent.Headers.GetValues("x-amz-delete-marker")));
        var deleteMarkerVersionId = Assert.Single(deleteCurrent.Headers.GetValues("x-amz-version-id"));

        var currentGet = await client.GetAsync("/integrated-s3/history-versions-bucket/docs/history.txt");
        Assert.Equal(HttpStatusCode.NotFound, currentGet.StatusCode);

        var versionsResponse = await client.GetAsync("/integrated-s3/history-versions-bucket?versions&prefix=docs/history.txt");
        Assert.Equal(HttpStatusCode.OK, versionsResponse.StatusCode);
        Assert.Equal("application/xml", versionsResponse.Content.Headers.ContentType?.MediaType);

        var versionsDocument = XDocument.Parse(await versionsResponse.Content.ReadAsStringAsync());
        Assert.Equal("ListVersionsResult", versionsDocument.Root?.Name.LocalName);
        Assert.Equal("history-versions-bucket", GetRequiredElementValue(versionsDocument, "Name"));

        var deleteMarkers = versionsDocument.Root!.Elements("DeleteMarker").ToArray();
        var versions = versionsDocument.Root!.Elements("Version").ToArray();

        var deleteMarker = Assert.Single(deleteMarkers);
        Assert.Equal("docs/history.txt", deleteMarker.Element("Key")?.Value);
        Assert.Equal(deleteMarkerVersionId, deleteMarker.Element("VersionId")?.Value);
        Assert.Equal("true", deleteMarker.Element("IsLatest")?.Value);

        Assert.Equal(2, versions.Length);
        Assert.Contains(versions, version => version.Element("VersionId")?.Value == v1VersionId);
        Assert.Contains(versions, version => version.Element("VersionId")?.Value == v2VersionId);

        var deleteDeleteMarker = await client.DeleteAsync($"/integrated-s3/history-versions-bucket/docs/history.txt?versionId={Uri.EscapeDataString(deleteMarkerVersionId)}");
        Assert.Equal(HttpStatusCode.NoContent, deleteDeleteMarker.StatusCode);
        Assert.Equal("true", Assert.Single(deleteDeleteMarker.Headers.GetValues("x-amz-delete-marker")));
        Assert.Equal(deleteMarkerVersionId, Assert.Single(deleteDeleteMarker.Headers.GetValues("x-amz-version-id")));

        var restoredGet = await client.GetAsync("/integrated-s3/history-versions-bucket/docs/history.txt");
        Assert.Equal(HttpStatusCode.OK, restoredGet.StatusCode);
        Assert.Equal(v2VersionId, Assert.Single(restoredGet.Headers.GetValues("x-amz-version-id")));
        Assert.Equal("http version two", await restoredGet.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task S3CompatibleListObjectVersions_WithKeyAndVersionMarkers_PaginatesWithinSameKey()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/history-version-markers-bucket", content: null)).StatusCode);

        using var enableVersioningRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/history-version-markers-bucket?versioning")
        {
            Content = new StringContent("""
<VersioningConfiguration>
  <Status>Enabled</Status>
</VersioningConfiguration>
""", Encoding.UTF8, "application/xml")
        };
        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(enableVersioningRequest)).StatusCode);

        const string primaryKey = "docs/history.txt";
        const string secondaryKey = "docs/zeta.txt";

        var v1Response = await client.PutAsync(
            $"/integrated-s3/history-version-markers-bucket/{primaryKey}",
            new StringContent("http version one", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, v1Response.StatusCode);
        var v1VersionId = Assert.Single(v1Response.Headers.GetValues("x-amz-version-id"));

        var v2Response = await client.PutAsync(
            $"/integrated-s3/history-version-markers-bucket/{primaryKey}",
            new StringContent("http version two", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, v2Response.StatusCode);
        var v2VersionId = Assert.Single(v2Response.Headers.GetValues("x-amz-version-id"));

        var deleteCurrent = await client.DeleteAsync($"/integrated-s3/history-version-markers-bucket/{primaryKey}");
        Assert.Equal(HttpStatusCode.NoContent, deleteCurrent.StatusCode);
        Assert.Equal("true", Assert.Single(deleteCurrent.Headers.GetValues("x-amz-delete-marker")));
        var deleteMarkerVersionId = Assert.Single(deleteCurrent.Headers.GetValues("x-amz-version-id"));

        var secondaryObjectResponse = await client.PutAsync(
            $"/integrated-s3/history-version-markers-bucket/{secondaryKey}",
            new StringContent("http version zeta", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, secondaryObjectResponse.StatusCode);
        var secondaryVersionId = Assert.Single(secondaryObjectResponse.Headers.GetValues("x-amz-version-id"));

        var firstPageResponse = await client.GetAsync("/integrated-s3/history-version-markers-bucket?versions&prefix=docs/&max-keys=1");
        Assert.Equal(HttpStatusCode.OK, firstPageResponse.StatusCode);
        Assert.Equal("application/xml", firstPageResponse.Content.Headers.ContentType?.MediaType);

        var firstPageDocument = XDocument.Parse(await firstPageResponse.Content.ReadAsStringAsync());
        Assert.Equal("ListVersionsResult", firstPageDocument.Root?.Name.LocalName);
        Assert.Equal("true", GetRequiredElementValue(firstPageDocument, "IsTruncated"));
        Assert.Equal(primaryKey, GetRequiredElementValue(firstPageDocument, "NextKeyMarker"));
        Assert.Equal(deleteMarkerVersionId, GetRequiredElementValue(firstPageDocument, "NextVersionIdMarker"));
        Assert.Empty(firstPageDocument.Root!.Elements("Version"));
        var firstPageDeleteMarker = Assert.Single(firstPageDocument.Root!.Elements("DeleteMarker"));
        Assert.Equal(primaryKey, firstPageDeleteMarker.Element("Key")?.Value);
        Assert.Equal(deleteMarkerVersionId, firstPageDeleteMarker.Element("VersionId")?.Value);

        var secondPageResponse = await client.GetAsync(
            $"/integrated-s3/history-version-markers-bucket?versions&prefix=docs/&max-keys=10&key-marker={Uri.EscapeDataString(primaryKey)}&version-id-marker={Uri.EscapeDataString(deleteMarkerVersionId)}");
        Assert.Equal(HttpStatusCode.OK, secondPageResponse.StatusCode);
        Assert.Equal("application/xml", secondPageResponse.Content.Headers.ContentType?.MediaType);

        var secondPageDocument = XDocument.Parse(await secondPageResponse.Content.ReadAsStringAsync());
        Assert.Equal(primaryKey, GetRequiredElementValue(secondPageDocument, "KeyMarker"));
        Assert.Equal(deleteMarkerVersionId, GetRequiredElementValue(secondPageDocument, "VersionIdMarker"));
        Assert.Equal("false", GetRequiredElementValue(secondPageDocument, "IsTruncated"));
        Assert.Empty(secondPageDocument.Root!.Elements("DeleteMarker"));

        var secondPageVersions = secondPageDocument.Root!.Elements("Version").ToArray();
        Assert.Collection(
            secondPageVersions,
            version => {
                Assert.Equal(primaryKey, version.Element("Key")?.Value);
                Assert.Equal(v2VersionId, version.Element("VersionId")?.Value);
                Assert.Equal("false", version.Element("IsLatest")?.Value);
            },
            version => {
                Assert.Equal(primaryKey, version.Element("Key")?.Value);
                Assert.Equal(v1VersionId, version.Element("VersionId")?.Value);
                Assert.Equal("false", version.Element("IsLatest")?.Value);
            },
            version => {
                Assert.Equal(secondaryKey, version.Element("Key")?.Value);
                Assert.Equal(secondaryVersionId, version.Element("VersionId")?.Value);
                Assert.Equal("true", version.Element("IsLatest")?.Value);
            });
    }

    [Fact]
    public async Task S3CompatibleListMultipartUploads_PaginatesWithinSameKeyAndReturnsCommonPrefixes()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "multipart-list-bucket";
        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        async Task<string> InitiateMultipartUploadAsync(string objectKey)
        {
            using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
            initiateRequest.Headers.TryAddWithoutValidation("Content-Type", "text/plain");

            var initiateResponse = await client.SendAsync(initiateRequest);
            Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);

            var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
            return GetRequiredElementValue(initiateDocument, "UploadId");
        }

        var firstUploadId = await InitiateMultipartUploadAsync("docs/alpha.txt");
        await Task.Delay(2);
        var secondUploadId = await InitiateMultipartUploadAsync("docs/alpha.txt");
        await Task.Delay(2);
        await InitiateMultipartUploadAsync("docs/nested/beta.txt");

        var firstPageResponse = await client.GetAsync($"/integrated-s3/{bucketName}?uploads&prefix=docs/&delimiter=/&max-uploads=2");
        Assert.Equal(HttpStatusCode.OK, firstPageResponse.StatusCode);
        Assert.Equal("application/xml", firstPageResponse.Content.Headers.ContentType?.MediaType);

        var firstPageDocument = XDocument.Parse(await firstPageResponse.Content.ReadAsStringAsync());
        Assert.Equal("ListMultipartUploadsResult", firstPageDocument.Root?.Name.LocalName);
        Assert.Equal(bucketName, GetRequiredElementValue(firstPageDocument, "Bucket"));
        Assert.Equal("docs/", GetRequiredElementValue(firstPageDocument, "Prefix"));
        Assert.Equal("/", GetRequiredElementValue(firstPageDocument, "Delimiter"));
        Assert.Equal("true", GetRequiredElementValue(firstPageDocument, "IsTruncated"));
        Assert.Equal("docs/alpha.txt", GetRequiredElementValue(firstPageDocument, "NextKeyMarker"));
        Assert.Equal(secondUploadId, GetRequiredElementValue(firstPageDocument, "NextUploadIdMarker"));

        static void AssertUploadIdentity(XElement upload)
        {
            var initiator = Assert.IsType<XElement>(upload.Element("Initiator"));
            var owner = Assert.IsType<XElement>(upload.Element("Owner"));
            Assert.False(string.IsNullOrWhiteSpace(initiator.Element("ID")?.Value));
            Assert.Equal(initiator.Element("ID")?.Value, owner.Element("ID")?.Value);
        }

        var firstPageUploads = firstPageDocument.Root!.Elements("Upload").ToArray();
        Assert.Collection(
            firstPageUploads,
            upload => {
                Assert.Equal("docs/alpha.txt", upload.Element("Key")?.Value);
                Assert.Equal(firstUploadId, upload.Element("UploadId")?.Value);
                AssertUploadIdentity(upload);
            },
            upload => {
                Assert.Equal("docs/alpha.txt", upload.Element("Key")?.Value);
                Assert.Equal(secondUploadId, upload.Element("UploadId")?.Value);
                AssertUploadIdentity(upload);
            });
        Assert.Empty(firstPageDocument.Root!.Elements("CommonPrefixes"));

        var secondPageResponse = await client.GetAsync(
            $"/integrated-s3/{bucketName}?uploads&prefix=docs/&delimiter=/&max-uploads=10&key-marker={Uri.EscapeDataString("docs/alpha.txt")}&upload-id-marker={Uri.EscapeDataString(secondUploadId)}");
        Assert.Equal(HttpStatusCode.OK, secondPageResponse.StatusCode);
        Assert.Equal("application/xml", secondPageResponse.Content.Headers.ContentType?.MediaType);

        var secondPageDocument = XDocument.Parse(await secondPageResponse.Content.ReadAsStringAsync());
        Assert.Equal("docs/alpha.txt", GetRequiredElementValue(secondPageDocument, "KeyMarker"));
        Assert.Equal(secondUploadId, GetRequiredElementValue(secondPageDocument, "UploadIdMarker"));
        Assert.Equal("false", GetRequiredElementValue(secondPageDocument, "IsTruncated"));
        Assert.Empty(secondPageDocument.Root!.Elements("Upload"));

        var commonPrefix = Assert.Single(secondPageDocument.Root!.Elements("CommonPrefixes"));
        Assert.Equal("docs/nested/", commonPrefix.Element("Prefix")?.Value);
    }

    [Fact]
    public async Task S3CompatibleListMultipartUploads_ReturnsEmptyXmlResultWhenBucketHasNoUploads()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "multipart-empty-bucket";
        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        var response = await client.GetAsync($"/integrated-s3/{bucketName}?uploads");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("ListMultipartUploadsResult", document.Root?.Name.LocalName);
        Assert.Equal(bucketName, GetRequiredElementValue(document, "Bucket"));
        Assert.Equal("false", GetRequiredElementValue(document, "IsTruncated"));
        Assert.Equal("1000", GetRequiredElementValue(document, "MaxUploads"));
        Assert.Empty(document.Root!.Elements("Upload"));
        Assert.Empty(document.Root!.Elements("CommonPrefixes"));
    }

    [Fact]
    public async Task S3CompatibleListMultipartUploads_WithEncodingTypeUrl_EncodesMultipartResponseFields()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "multipart-encoding-bucket";
        const string prefix = "docs/reports & more/";
        const string key = "docs/reports & more/(draft).txt";
        const string nestedKey = "docs/reports & more/nested/file.txt";
        const string nestedPrefix = "docs/reports & more/nested/";
        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        async Task<string> InitiateMultipartUploadAsync(string objectKey)
        {
            using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
            initiateRequest.Headers.TryAddWithoutValidation("Content-Type", "text/plain");

            var initiateResponse = await client.SendAsync(initiateRequest);
            Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);

            var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
            return GetRequiredElementValue(initiateDocument, "UploadId");
        }

        var firstUploadId = await InitiateMultipartUploadAsync(key);
        await Task.Delay(2);
        var secondUploadId = await InitiateMultipartUploadAsync(key);
        await Task.Delay(2);
        await InitiateMultipartUploadAsync(nestedKey);

        var firstPageResponse = await client.GetAsync(
            $"/integrated-s3/{bucketName}?uploads&prefix={Uri.EscapeDataString(prefix)}&delimiter=%2F&max-uploads=2&encoding-type=url");
        Assert.Equal(HttpStatusCode.OK, firstPageResponse.StatusCode);

        var firstPageDocument = XDocument.Parse(await firstPageResponse.Content.ReadAsStringAsync());
        Assert.Equal("url", GetRequiredElementValue(firstPageDocument, "EncodingType"));
        Assert.Equal(string.Empty, GetRequiredElementValue(firstPageDocument, "KeyMarker"));
        Assert.Equal(string.Empty, GetRequiredElementValue(firstPageDocument, "UploadIdMarker"));
        Assert.Equal(Uri.EscapeDataString(prefix), GetRequiredElementValue(firstPageDocument, "Prefix"));
        Assert.Equal(Uri.EscapeDataString("/"), GetRequiredElementValue(firstPageDocument, "Delimiter"));
        Assert.Equal(Uri.EscapeDataString(key), GetRequiredElementValue(firstPageDocument, "NextKeyMarker"));
        Assert.Equal(secondUploadId, GetRequiredElementValue(firstPageDocument, "NextUploadIdMarker"));

        var firstPageUploads = firstPageDocument.Root!.Elements("Upload").ToArray();
        Assert.Collection(
            firstPageUploads,
            upload => {
                Assert.Equal(Uri.EscapeDataString(key), upload.Element("Key")?.Value);
                Assert.Equal(firstUploadId, upload.Element("UploadId")?.Value);
            },
            upload => {
                Assert.Equal(Uri.EscapeDataString(key), upload.Element("Key")?.Value);
                Assert.Equal(secondUploadId, upload.Element("UploadId")?.Value);
            });

        var secondPageResponse = await client.GetAsync(
            $"/integrated-s3/{bucketName}?uploads&prefix={Uri.EscapeDataString(prefix)}&delimiter=%2F&key-marker={Uri.EscapeDataString(key)}&upload-id-marker={Uri.EscapeDataString(secondUploadId)}&encoding-type=url");
        Assert.Equal(HttpStatusCode.OK, secondPageResponse.StatusCode);

        var secondPageDocument = XDocument.Parse(await secondPageResponse.Content.ReadAsStringAsync());
        Assert.Equal(Uri.EscapeDataString(key), GetRequiredElementValue(secondPageDocument, "KeyMarker"));
        Assert.Equal(secondUploadId, GetRequiredElementValue(secondPageDocument, "UploadIdMarker"));
        var commonPrefix = Assert.Single(secondPageDocument.Root!.Elements("CommonPrefixes"));
        Assert.Equal(Uri.EscapeDataString(nestedPrefix), commonPrefix.Element("Prefix")?.Value);
    }

    [Fact]
    public async Task S3CompatibleListMultipartUploads_IgnoresUploadIdMarkerWithoutKeyMarker()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "multipart-ignore-upload-id-marker-bucket";
        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        async Task<string> InitiateMultipartUploadAsync(string objectKey)
        {
            using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
            initiateRequest.Headers.TryAddWithoutValidation("Content-Type", "text/plain");

            var initiateResponse = await client.SendAsync(initiateRequest);
            Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);

            var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
            return GetRequiredElementValue(initiateDocument, "UploadId");
        }

        await InitiateMultipartUploadAsync("docs/alpha.txt");
        await InitiateMultipartUploadAsync("docs/beta.txt");

        var response = await client.GetAsync($"/integrated-s3/{bucketName}?uploads&upload-id-marker={Uri.EscapeDataString("ignored-upload-id")}");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal(string.Empty, GetRequiredElementValue(document, "KeyMarker"));
        Assert.Equal(string.Empty, GetRequiredElementValue(document, "UploadIdMarker"));
        Assert.Equal(2, document.Root!.Elements("Upload").Count());
    }

    [Fact]
    public async Task DuplicateBucketCreate_ReturnsXmlErrorConflict()
    {
        using var client = await _factory.CreateClientAsync();

        var firstResponse = await client.PutAsync("/integrated-s3/buckets/conflict-bucket", content: null);
        Assert.Equal(HttpStatusCode.Created, firstResponse.StatusCode);

        var secondResponse = await client.PutAsync("/integrated-s3/buckets/conflict-bucket", content: null);
        Assert.Equal(HttpStatusCode.Conflict, secondResponse.StatusCode);
        Assert.Equal("application/xml", secondResponse.Content.Headers.ContentType?.MediaType);

        var errorDocument = XDocument.Parse(await secondResponse.Content.ReadAsStringAsync());
        Assert.Equal("Error", errorDocument.Root?.Name.LocalName);
        Assert.Equal("BucketAlreadyExists", GetRequiredElementValue(errorDocument, "Code"));
        Assert.Contains("already exists", GetRequiredElementValue(errorDocument, "Message"), StringComparison.OrdinalIgnoreCase);
        Assert.Equal("/conflict-bucket", GetRequiredElementValue(errorDocument, "Resource"));
    }

    [Fact]
    public async Task DeleteNonEmptyBucket_S3CompatibleRoute_ReturnsBucketNotEmptyConflict()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/non-empty-delete-bucket", content: null)).StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await client.PutAsync(
            "/integrated-s3/buckets/non-empty-delete-bucket/objects/docs/hello.txt",
            new StringContent("hello", Encoding.UTF8, "text/plain"))).StatusCode);

        var deleteResponse = await client.DeleteAsync("/integrated-s3/non-empty-delete-bucket");

        Assert.Equal(HttpStatusCode.Conflict, deleteResponse.StatusCode);
        Assert.Equal("application/xml", deleteResponse.Content.Headers.ContentType?.MediaType);

        var errorDocument = XDocument.Parse(await deleteResponse.Content.ReadAsStringAsync());
        Assert.Equal("Error", errorDocument.Root?.Name.LocalName);
        Assert.Equal("BucketNotEmpty", GetRequiredElementValue(errorDocument, "Code"));
        Assert.Contains("empty", GetRequiredElementValue(errorDocument, "Message"), StringComparison.OrdinalIgnoreCase);
        Assert.Equal("/non-empty-delete-bucket", GetRequiredElementValue(errorDocument, "Resource"));
    }

    [Fact]
    public async Task ServiceDocument_AdvertisesDiskProviderCapabilities()
    {
        using var client = await _factory.CreateClientAsync();

        using var response = await client.GetAsync("/integrated-s3/");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var payload = await response.Content.ReadAsStringAsync();

        using var jsonDocument = JsonDocument.Parse(payload);
        var supportStateElement = jsonDocument.RootElement
            .GetProperty("providers")[0]
            .GetProperty("supportState");
        Assert.True(supportStateElement.TryGetProperty("accessControl", out _));
        Assert.True(supportStateElement.TryGetProperty("retention", out _));
        Assert.True(supportStateElement.TryGetProperty("serverSideEncryption", out _));

        var document = JsonSerializer.Deserialize<StorageServiceDocument>(payload, JsonOptions);

        Assert.NotNull(document);
        Assert.Equal("Integrated S3 Sample Host", document!.ServiceName);
        var provider = Assert.Single(document.Providers);
        Assert.Equal("disk", provider.Kind);
        Assert.True(provider.IsPrimary);
        Assert.Equal("test-disk", provider.Name);
        Assert.Equal(StorageProviderMode.Managed, provider.Mode);
        Assert.Equal(StorageCapabilitySupport.Native, provider.Capabilities.ObjectCrud);
        Assert.Equal(StorageCapabilitySupport.Emulated, provider.Capabilities.Cors);
        Assert.Equal(StorageCapabilitySupport.Native, document.Capabilities.ObjectCrud);
        Assert.Equal(StorageCapabilitySupport.Native, provider.Capabilities.Pagination);
        Assert.Equal(StorageObjectAccessMode.ProxyStream, provider.ObjectLocation.DefaultAccessMode);
        Assert.Equal([StorageObjectAccessMode.ProxyStream], provider.ObjectLocation.SupportedAccessModes);
        Assert.Equal(StorageSupportStateOwnership.NotApplicable, provider.SupportState.AccessControl);
        Assert.Equal(StorageSupportStateOwnership.NotApplicable, provider.SupportState.Retention);
        Assert.Equal(StorageSupportStateOwnership.NotApplicable, provider.SupportState.ServerSideEncryption);
    }

    [Fact]
    public async Task GetObject_WithRangeHeader_ReturnsPartialContent()
    {
        using var client = await _factory.CreateClientAsync();

        await client.PutAsync("/integrated-s3/buckets/range-bucket", content: null);
        await client.PutAsync(
            "/integrated-s3/buckets/range-bucket/objects/docs/range.txt",
            new StringContent("hello integrated s3", Encoding.UTF8, "text/plain"));

        using var request = new HttpRequestMessage(HttpMethod.Get, "/integrated-s3/buckets/range-bucket/objects/docs/range.txt");
        request.Headers.Range = new System.Net.Http.Headers.RangeHeaderValue(6, 15);

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.PartialContent, response.StatusCode);
        Assert.Equal("bytes 6-15/19", response.Content.Headers.ContentRange?.ToString());
        Assert.Equal("integrated", await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task GetObject_WithIfNoneMatchHeader_ReturnsNotModified()
    {
        using var client = await _factory.CreateClientAsync();

        await client.PutAsync("/integrated-s3/buckets/etag-bucket", content: null);
        var uploadResponse = await client.PutAsync(
            "/integrated-s3/buckets/etag-bucket/objects/docs/etag.txt",
            new StringContent("hello integrated s3", Encoding.UTF8, "text/plain"));
        var uploadedObject = await uploadResponse.Content.ReadFromJsonAsync<ObjectInfo>(JsonOptions);

        using var request = new HttpRequestMessage(HttpMethod.Get, "/integrated-s3/buckets/etag-bucket/objects/docs/etag.txt");
        request.Headers.TryAddWithoutValidation("If-None-Match", $"\"{uploadedObject!.ETag}\"");

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.NotModified, response.StatusCode);
        Assert.Empty(await response.Content.ReadAsByteArrayAsync());
    }

    [Fact]
    public async Task ListObjects_WithPageSize_ReturnsContinuationTokenHeader()
    {
        using var client = await _factory.CreateClientAsync();

        await client.PutAsync("/integrated-s3/buckets/paged-bucket", content: null);
        foreach (var key in new[] { "a.txt", "b.txt", "c.txt" }) {
            var uploadResponse = await client.PutAsync(
                $"/integrated-s3/buckets/paged-bucket/objects/{key}",
                new StringContent(key, Encoding.UTF8, "text/plain"));
            Assert.Equal(HttpStatusCode.OK, uploadResponse.StatusCode);
        }

        var firstPageResponse = await client.GetAsync("/integrated-s3/buckets/paged-bucket/objects?pageSize=2");
        Assert.Equal(HttpStatusCode.OK, firstPageResponse.StatusCode);
        var firstPage = await firstPageResponse.Content.ReadFromJsonAsync<ObjectInfo[]>(JsonOptions);
        Assert.NotNull(firstPage);
        Assert.Equal(["a.txt", "b.txt"], firstPage!.Select(static item => item.Key).ToArray());

        Assert.True(firstPageResponse.Headers.TryGetValues("x-integrateds3-continuation-token", out var continuationValues));
        var continuationToken = Assert.Single(continuationValues);
        Assert.Equal("b.txt", continuationToken);

        var secondPage = await client.GetFromJsonAsync<ObjectInfo[]>($"/integrated-s3/buckets/paged-bucket/objects?pageSize=2&continuationToken={Uri.EscapeDataString(continuationToken)}", JsonOptions);
        Assert.NotNull(secondPage);
        Assert.Equal(["c.txt"], secondPage!.Select(static item => item.Key).ToArray());
    }

    [Fact]
    public async Task HeadObject_WithIfNoneMatchHeader_ReturnsNotModified()
    {
        using var client = await _factory.CreateClientAsync();

        await client.PutAsync("/integrated-s3/buckets/head-bucket", content: null);
        var uploadResponse = await client.PutAsync(
            "/integrated-s3/buckets/head-bucket/objects/docs/head.txt",
            new StringContent("hello integrated s3", Encoding.UTF8, "text/plain"));
        var uploadedObject = await uploadResponse.Content.ReadFromJsonAsync<ObjectInfo>(JsonOptions);

        using var request = new HttpRequestMessage(HttpMethod.Head, "/integrated-s3/buckets/head-bucket/objects/docs/head.txt");
        request.Headers.TryAddWithoutValidation("If-None-Match", $"\"{uploadedObject!.ETag}\"");

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.NotModified, response.StatusCode);
        Assert.Equal($"\"{uploadedObject.ETag}\"", response.Headers.ETag?.Tag);
    }

    [Fact]
    public async Task GetObject_WithIfModifiedSinceHeader_ReturnsNotModified()
    {
        using var client = await _factory.CreateClientAsync();

        await client.PutAsync("/integrated-s3/buckets/date-bucket", content: null);
        var uploadResponse = await client.PutAsync(
            "/integrated-s3/buckets/date-bucket/objects/docs/date.txt",
            new StringContent("hello integrated s3", Encoding.UTF8, "text/plain"));
        var uploadedObject = await uploadResponse.Content.ReadFromJsonAsync<ObjectInfo>(JsonOptions);

        using var request = new HttpRequestMessage(HttpMethod.Get, "/integrated-s3/buckets/date-bucket/objects/docs/date.txt");
        request.Headers.IfModifiedSince = uploadedObject!.LastModifiedUtc.AddMinutes(5);

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.NotModified, response.StatusCode);
        Assert.Empty(await response.Content.ReadAsByteArrayAsync());
    }

    [Fact]
    public async Task HeadObject_WithIfUnmodifiedSinceHeader_ReturnsPreconditionFailed()
    {
        using var client = await _factory.CreateClientAsync();

        await client.PutAsync("/integrated-s3/buckets/head-date-bucket", content: null);
        var uploadResponse = await client.PutAsync(
            "/integrated-s3/buckets/head-date-bucket/objects/docs/date-head.txt",
            new StringContent("hello integrated s3", Encoding.UTF8, "text/plain"));
        var uploadedObject = await uploadResponse.Content.ReadFromJsonAsync<ObjectInfo>(JsonOptions);

        using var request = new HttpRequestMessage(HttpMethod.Head, "/integrated-s3/buckets/head-date-bucket/objects/docs/date-head.txt");
        request.Headers.TryAddWithoutValidation("If-Unmodified-Since", uploadedObject!.LastModifiedUtc.AddMinutes(-5).ToString("R"));

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.PreconditionFailed, response.StatusCode);
        Assert.Equal($"\"{uploadedObject.ETag}\"", response.Headers.ETag?.Tag);
    }

    [Fact]
    public async Task PutObject_WithCopySourceHeader_CopiesObjectAndPreservesMetadata()
    {
        using var client = await _factory.CreateClientAsync();
        const string payload = "copied payload";

        await client.PutAsync("/integrated-s3/buckets/copy-source-bucket", content: null);
        await client.PutAsync("/integrated-s3/buckets/copy-target-bucket", content: null);

        using var uploadRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/buckets/copy-source-bucket/objects/docs/source.txt")
        {
            Content = new StringContent(payload, Encoding.UTF8, "text/plain")
        };
        uploadRequest.Headers.Add("x-integrateds3-meta-origin", "copied-from-source");
        var uploadResponse = await client.SendAsync(uploadRequest);
        Assert.Equal(HttpStatusCode.OK, uploadResponse.StatusCode);
        var expectedChecksumCrc32 = Assert.Single(uploadResponse.Headers.GetValues("x-amz-checksum-crc32"));
        var expectedChecksumCrc32c = Assert.Single(uploadResponse.Headers.GetValues("x-amz-checksum-crc32c"));
        var expectedChecksumSha256 = Assert.Single(uploadResponse.Headers.GetValues("x-amz-checksum-sha256"));

        using var copyRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/buckets/copy-target-bucket/objects/docs/copied.txt");
        copyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source", "/copy-source-bucket/docs/source.txt");

        var copyResponse = await client.SendAsync(copyRequest);
        Assert.Equal(HttpStatusCode.OK, copyResponse.StatusCode);
        Assert.Equal("application/xml", copyResponse.Content.Headers.ContentType?.MediaType);
        Assert.Equal(expectedChecksumCrc32, Assert.Single(copyResponse.Headers.GetValues("x-amz-checksum-crc32")));
        Assert.Equal(expectedChecksumCrc32c, Assert.Single(copyResponse.Headers.GetValues("x-amz-checksum-crc32c")));
        Assert.Equal(expectedChecksumSha256, Assert.Single(copyResponse.Headers.GetValues("x-amz-checksum-sha256")));
        var copyDocument = XDocument.Parse(await copyResponse.Content.ReadAsStringAsync());
        Assert.Equal("CopyObjectResult", copyDocument.Root?.Name.LocalName);
        Assert.False(string.IsNullOrWhiteSpace(GetRequiredElementValue(copyDocument, "LastModified")));
        Assert.False(string.IsNullOrWhiteSpace(GetRequiredElementValue(copyDocument, "ETag")));
        Assert.Equal(expectedChecksumCrc32, GetRequiredElementValue(copyDocument, "ChecksumCRC32"));
        Assert.Equal(expectedChecksumCrc32c, GetRequiredElementValue(copyDocument, "ChecksumCRC32C"));
        Assert.Equal(expectedChecksumSha256, GetRequiredElementValue(copyDocument, "ChecksumSHA256"));

        var downloadedResponse = await client.GetAsync("/integrated-s3/buckets/copy-target-bucket/objects/docs/copied.txt");
        Assert.Equal(HttpStatusCode.OK, downloadedResponse.StatusCode);
        Assert.Equal("copied-from-source", Assert.Single(downloadedResponse.Headers.GetValues("x-integrateds3-meta-origin")));
        Assert.Equal(payload, await downloadedResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task S3CompatibleCopyObject_WithChecksumAlgorithmHeader_EchoesRequestedChecksumAlgorithmAndPersistsRequestedChecksum()
    {
        using var client = await _factory.CreateClientAsync();
        const string payload = "copy object checksum algorithm";
        var expectedChecksumSha1 = ComputeSha1Base64(payload);

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/copy-checksum-source", content: null)).StatusCode);
        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/copy-checksum-target", content: null)).StatusCode);

        using var sourceUploadRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/copy-checksum-source/docs/source.txt")
        {
            Content = new StringContent(payload, Encoding.UTF8, "text/plain")
        };

        var sourceUploadResponse = await client.SendAsync(sourceUploadRequest);
        Assert.Equal(HttpStatusCode.OK, sourceUploadResponse.StatusCode);

        using var copyRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/copy-checksum-target/docs/copied.txt");
        copyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source", "/copy-checksum-source/docs/source.txt");
        copyRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "SHA1");

        var copyResponse = await client.SendAsync(copyRequest);

        Assert.Equal(HttpStatusCode.OK, copyResponse.StatusCode);
        Assert.Equal("application/xml", copyResponse.Content.Headers.ContentType?.MediaType);
        Assert.Equal("SHA1", Assert.Single(copyResponse.Headers.GetValues("x-amz-checksum-algorithm")));
        Assert.Equal(expectedChecksumSha1, Assert.Single(copyResponse.Headers.GetValues("x-amz-checksum-sha1")));
        var copyDocument = XDocument.Parse(await copyResponse.Content.ReadAsStringAsync());
        Assert.Equal("CopyObjectResult", copyDocument.Root?.Name.LocalName);
        Assert.Equal(expectedChecksumSha1, GetRequiredElementValue(copyDocument, "ChecksumSHA1"));

        using var headRequest = new HttpRequestMessage(HttpMethod.Head, "/integrated-s3/copy-checksum-target/docs/copied.txt");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        Assert.Equal(expectedChecksumSha1, Assert.Single(headResponse.Headers.GetValues("x-amz-checksum-sha1")));

        var downloadResponse = await client.GetAsync("/integrated-s3/copy-checksum-target/docs/copied.txt");
        Assert.Equal(HttpStatusCode.OK, downloadResponse.StatusCode);
        Assert.Equal(expectedChecksumSha1, Assert.Single(downloadResponse.Headers.GetValues("x-amz-checksum-sha1")));
        Assert.Equal(payload, await downloadResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task S3CompatibleCopyObject_WithMismatchedChecksumAlgorithmHeaders_ReturnsInvalidRequest()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/copy-checksum-mismatch-source", content: null)).StatusCode);
        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/copy-checksum-mismatch-target", content: null)).StatusCode);

        using var sourceUploadRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/copy-checksum-mismatch-source/docs/source.txt")
        {
            Content = new StringContent("copy checksum mismatch", Encoding.UTF8, "text/plain")
        };

        var sourceUploadResponse = await client.SendAsync(sourceUploadRequest);
        Assert.Equal(HttpStatusCode.OK, sourceUploadResponse.StatusCode);

        using var copyRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/copy-checksum-mismatch-target/docs/copied.txt");
        copyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source", "/copy-checksum-mismatch-source/docs/source.txt");
        copyRequest.Headers.TryAddWithoutValidation("x-amz-sdk-checksum-algorithm", "CRC32C");
        copyRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "SHA256");

        var response = await client.SendAsync(copyRequest);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("InvalidRequest", GetRequiredElementValue(document, "Code"));
        Assert.Equal(
            "The 'x-amz-sdk-checksum-algorithm' and 'x-amz-checksum-algorithm' headers must match when both are supplied.",
            GetRequiredElementValue(document, "Message"));
    }

    [Fact]
    public async Task S3CompatibleCopyObject_WithMetadataDirectiveReplace_ReplacesHeadersAndMetadata()
    {
        using var client = await _factory.CreateClientAsync();
        var replacementExpiresUtc = new DateTimeOffset(2026, 3, 14, 16, 0, 0, TimeSpan.Zero);

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/copy-replace-source", content: null)).StatusCode);
        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/copy-replace-target", content: null)).StatusCode);

        using var sourceUploadRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/copy-replace-source/docs/source.txt")
        {
            Content = new StringContent("copy me", Encoding.UTF8, "text/plain")
        };
        sourceUploadRequest.Headers.CacheControl = CacheControlHeaderValue.Parse("public, max-age=60");
        sourceUploadRequest.Content.Headers.ContentDisposition = new ContentDispositionHeaderValue("attachment")
        {
            FileName = "source.txt"
        };
        sourceUploadRequest.Content.Headers.ContentEncoding.Add("identity");
        sourceUploadRequest.Content.Headers.ContentLanguage.Add("de-DE");
        sourceUploadRequest.Content.Headers.Expires = replacementExpiresUtc.AddHours(-1);
        sourceUploadRequest.Headers.TryAddWithoutValidation("x-amz-meta-origin", "source");
        sourceUploadRequest.Headers.TryAddWithoutValidation("x-amz-meta-source-only", "keep-me");

        var sourceUploadResponse = await client.SendAsync(sourceUploadRequest);
        Assert.Equal(HttpStatusCode.OK, sourceUploadResponse.StatusCode);

        using var copyRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/copy-replace-target/docs/copied.txt")
        {
            Content = new ByteArrayContent([])
        };
        copyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source", "/copy-replace-source/docs/source.txt");
        copyRequest.Headers.TryAddWithoutValidation("x-amz-metadata-directive", "REPLACE");
        copyRequest.Headers.TryAddWithoutValidation("Cache-Control", "no-store");
        copyRequest.Content.Headers.ContentType = new MediaTypeHeaderValue("text/markdown");
        copyRequest.Content.Headers.ContentDisposition = new ContentDispositionHeaderValue("inline")
        {
            FileName = "copied.txt"
        };
        copyRequest.Content.Headers.ContentEncoding.Add("identity");
        copyRequest.Content.Headers.ContentLanguage.Add("en-US");
        copyRequest.Content.Headers.Expires = replacementExpiresUtc;
        copyRequest.Headers.TryAddWithoutValidation("x-amz-meta-origin", "replaced");

        var copyResponse = await client.SendAsync(copyRequest);
        Assert.Equal(HttpStatusCode.OK, copyResponse.StatusCode);

        using var headRequest = new HttpRequestMessage(HttpMethod.Head, "/integrated-s3/copy-replace-target/docs/copied.txt");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        Assert.Equal("replaced", Assert.Single(headResponse.Headers.GetValues("x-amz-meta-origin")));
        Assert.False(headResponse.Headers.Contains("x-amz-meta-source-only"));
        Assert.Equal("no-store", headResponse.Headers.CacheControl?.ToString());
        Assert.Equal("inline", headResponse.Content.Headers.ContentDisposition?.DispositionType);
        Assert.Equal("copied.txt", headResponse.Content.Headers.ContentDisposition?.FileName?.Trim('"'));
        Assert.Contains("identity", headResponse.Content.Headers.ContentEncoding);
        Assert.Contains("en-US", headResponse.Content.Headers.ContentLanguage);
        Assert.Equal(replacementExpiresUtc, headResponse.Content.Headers.Expires);
        Assert.Equal("text/markdown", headResponse.Content.Headers.ContentType?.MediaType);

        var downloadResponse = await client.GetAsync("/integrated-s3/copy-replace-target/docs/copied.txt");
        Assert.Equal(HttpStatusCode.OK, downloadResponse.StatusCode);
        Assert.Equal("replaced", Assert.Single(downloadResponse.Headers.GetValues("x-amz-meta-origin")));
        Assert.False(downloadResponse.Headers.Contains("x-amz-meta-source-only"));
        Assert.Equal("no-store", downloadResponse.Headers.CacheControl?.ToString());
        Assert.Equal("inline", downloadResponse.Content.Headers.ContentDisposition?.DispositionType);
        Assert.Equal("copied.txt", downloadResponse.Content.Headers.ContentDisposition?.FileName?.Trim('"'));
        Assert.Contains("identity", downloadResponse.Content.Headers.ContentEncoding);
        Assert.Contains("en-US", downloadResponse.Content.Headers.ContentLanguage);
        Assert.Equal(replacementExpiresUtc, downloadResponse.Content.Headers.Expires);
        Assert.Equal("copy me", await downloadResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task S3CompatibleCopyObject_CopyingObjectOntoItselfWithMetadataDirectiveReplace_UpdatesMtimeMetadata()
    {
        using var client = await _factory.CreateClientAsync();
        const string bucketName = "copy-self-replace-bucket";
        const string objectKey = "docs/rclone.txt";
        const string payload = "same-key copy keeps the payload";
        const string originalMtime = "2024-10-11T12:13:14.123456789Z";
        const string replacementMtime = "2025-01-02T03:04:05.987654321Z";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var sourceUploadRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/{objectKey}")
        {
            Content = new StringContent(payload, Encoding.UTF8, "text/plain")
        };
        sourceUploadRequest.Headers.TryAddWithoutValidation("x-amz-meta-mtime", originalMtime);
        sourceUploadRequest.Headers.TryAddWithoutValidation("x-amz-meta-source-only", "remove-me");

        var sourceUploadResponse = await client.SendAsync(sourceUploadRequest);
        Assert.Equal(HttpStatusCode.OK, sourceUploadResponse.StatusCode);

        using var copyRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/{objectKey}")
        {
            Content = new ByteArrayContent([])
        };
        copyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source", $"/{bucketName}/{objectKey}");
        copyRequest.Headers.TryAddWithoutValidation("x-amz-metadata-directive", "REPLACE");
        copyRequest.Content.Headers.ContentType = new MediaTypeHeaderValue("text/plain");
        copyRequest.Headers.TryAddWithoutValidation("x-amz-meta-mtime", replacementMtime);
        copyRequest.Headers.TryAddWithoutValidation("x-amz-meta-updated-by", "rclone");

        var copyResponse = await client.SendAsync(copyRequest);
        Assert.Equal(HttpStatusCode.OK, copyResponse.StatusCode);
        var copyDocument = XDocument.Parse(await copyResponse.Content.ReadAsStringAsync());
        Assert.Equal("CopyObjectResult", copyDocument.Root?.Name.LocalName);
        Assert.False(string.IsNullOrWhiteSpace(GetRequiredElementValue(copyDocument, "LastModified")));
        Assert.False(string.IsNullOrWhiteSpace(GetRequiredElementValue(copyDocument, "ETag")));

        using var headRequest = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{bucketName}/{objectKey}");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        Assert.Equal(replacementMtime, Assert.Single(headResponse.Headers.GetValues("x-amz-meta-mtime")));
        Assert.Equal("rclone", Assert.Single(headResponse.Headers.GetValues("x-amz-meta-updated-by")));
        Assert.False(headResponse.Headers.Contains("x-amz-meta-source-only"));

        var downloadResponse = await client.GetAsync($"/integrated-s3/{bucketName}/{objectKey}");
        Assert.Equal(HttpStatusCode.OK, downloadResponse.StatusCode);
        Assert.Equal(replacementMtime, Assert.Single(downloadResponse.Headers.GetValues("x-amz-meta-mtime")));
        Assert.Equal("rclone", Assert.Single(downloadResponse.Headers.GetValues("x-amz-meta-updated-by")));
        Assert.False(downloadResponse.Headers.Contains("x-amz-meta-source-only"));
        Assert.Equal(payload, await downloadResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task PutObject_WithCopySourcePreconditionHeader_ReturnsPreconditionFailed()
    {
        using var client = await _factory.CreateClientAsync();

        await client.PutAsync("/integrated-s3/buckets/copy-precondition-source", content: null);
        await client.PutAsync("/integrated-s3/buckets/copy-precondition-target", content: null);

        var uploadResponse = await client.PutAsync(
            "/integrated-s3/buckets/copy-precondition-source/objects/docs/source.txt",
            new StringContent("copied payload", Encoding.UTF8, "text/plain"));
        var sourceObject = await uploadResponse.Content.ReadFromJsonAsync<ObjectInfo>(JsonOptions);

        using var failedCopyRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/buckets/copy-precondition-target/objects/docs/copied.txt");
        failedCopyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source", "/copy-precondition-source/docs/source.txt");
        failedCopyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source-if-match", "\"different\"");

        var failedCopyResponse = await client.SendAsync(failedCopyRequest);
        Assert.Equal(HttpStatusCode.PreconditionFailed, failedCopyResponse.StatusCode);
        Assert.Equal("application/xml", failedCopyResponse.Content.Headers.ContentType?.MediaType);
        var failedCopyError = XDocument.Parse(await failedCopyResponse.Content.ReadAsStringAsync());
        Assert.Equal("PreconditionFailed", GetRequiredElementValue(failedCopyError, "Code"));

        using var noneMatchCopyRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/buckets/copy-precondition-target/objects/docs/copied.txt");
        noneMatchCopyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source", "/copy-precondition-source/docs/source.txt");
        noneMatchCopyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source-if-none-match", $"\"{sourceObject!.ETag}\"");

        var noneMatchCopyResponse = await client.SendAsync(noneMatchCopyRequest);
        Assert.Equal(HttpStatusCode.PreconditionFailed, noneMatchCopyResponse.StatusCode);
        Assert.Equal("application/xml", noneMatchCopyResponse.Content.Headers.ContentType?.MediaType);
        var noneMatchCopyError = XDocument.Parse(await noneMatchCopyResponse.Content.ReadAsStringAsync());
        Assert.Equal("PreconditionFailed", GetRequiredElementValue(noneMatchCopyError, "Code"));

        var targetHead = await client.SendAsync(new HttpRequestMessage(HttpMethod.Head, "/integrated-s3/buckets/copy-precondition-target/objects/docs/copied.txt"));
        Assert.Equal(HttpStatusCode.NotFound, targetHead.StatusCode);
    }

    [Fact]
    public async Task PutObject_WithCopySourceConditionalPrecedenceHeaders_UsesS3PrecedenceRules()
    {
        using var client = await _factory.CreateClientAsync();

        await client.PutAsync("/integrated-s3/buckets/copy-precedence-source", content: null);
        await client.PutAsync("/integrated-s3/buckets/copy-precedence-target", content: null);

        var uploadResponse = await client.PutAsync(
            "/integrated-s3/buckets/copy-precedence-source/objects/docs/source.txt",
            new StringContent("precedence payload", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, uploadResponse.StatusCode);
        var sourceObject = await uploadResponse.Content.ReadFromJsonAsync<ObjectInfo>(JsonOptions);
        Assert.NotNull(sourceObject);

        using var ifMatchPrecedenceCopyRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/buckets/copy-precedence-target/objects/docs/if-match-copy.txt");
        ifMatchPrecedenceCopyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source", "/copy-precedence-source/docs/source.txt");
        ifMatchPrecedenceCopyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source-if-match", $"\"{sourceObject!.ETag}\"");
        ifMatchPrecedenceCopyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source-if-unmodified-since", sourceObject.LastModifiedUtc.AddMinutes(-5).ToString("R"));

        var ifMatchPrecedenceCopyResponse = await client.SendAsync(ifMatchPrecedenceCopyRequest);
        Assert.Equal(HttpStatusCode.OK, ifMatchPrecedenceCopyResponse.StatusCode);
        Assert.Equal("application/xml", ifMatchPrecedenceCopyResponse.Content.Headers.ContentType?.MediaType);
        var ifMatchPrecedenceCopyDocument = XDocument.Parse(await ifMatchPrecedenceCopyResponse.Content.ReadAsStringAsync());
        Assert.Equal("CopyObjectResult", ifMatchPrecedenceCopyDocument.Root?.Name.LocalName);

        var copiedObjectResponse = await client.GetAsync("/integrated-s3/buckets/copy-precedence-target/objects/docs/if-match-copy.txt");
        Assert.Equal(HttpStatusCode.OK, copiedObjectResponse.StatusCode);
        Assert.Equal("precedence payload", await copiedObjectResponse.Content.ReadAsStringAsync());

        using var ifNoneMatchPrecedenceCopyRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/buckets/copy-precedence-target/objects/docs/if-none-match-copy.txt");
        ifNoneMatchPrecedenceCopyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source", "/copy-precedence-source/docs/source.txt");
        ifNoneMatchPrecedenceCopyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source-if-none-match", $"\"{sourceObject.ETag}\"");
        ifNoneMatchPrecedenceCopyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source-if-modified-since", sourceObject.LastModifiedUtc.AddMinutes(-5).ToString("R"));

        var ifNoneMatchPrecedenceCopyResponse = await client.SendAsync(ifNoneMatchPrecedenceCopyRequest);
        Assert.Equal(HttpStatusCode.PreconditionFailed, ifNoneMatchPrecedenceCopyResponse.StatusCode);
        Assert.Equal("application/xml", ifNoneMatchPrecedenceCopyResponse.Content.Headers.ContentType?.MediaType);
        var ifNoneMatchPrecedenceCopyError = XDocument.Parse(await ifNoneMatchPrecedenceCopyResponse.Content.ReadAsStringAsync());
        Assert.Equal("PreconditionFailed", GetRequiredElementValue(ifNoneMatchPrecedenceCopyError, "Code"));

        var blockedTargetHead = await client.SendAsync(new HttpRequestMessage(HttpMethod.Head, "/integrated-s3/buckets/copy-precedence-target/objects/docs/if-none-match-copy.txt"));
        Assert.Equal(HttpStatusCode.NotFound, blockedTargetHead.StatusCode);
    }

    [Fact]
    public async Task S3CompatibleBucketRoute_ListObjectsV1_WithMarkerAndEncodingType_ReturnsLegacyXmlPayload()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "legacy-list-bucket";

        static string EncodeObjectPath(string key)
        {
            return string.Join('/', key.Split('/').Select(Uri.EscapeDataString));
        }

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);
        foreach (var key in new[]
                 {
                     "docs/a file(1).txt",
                     "docs/b file(2).txt",
                     "docs/c file(3).txt"
                 }) {
            Assert.Equal(
                HttpStatusCode.OK,
                (await client.PutAsync(
                    $"/integrated-s3/buckets/{bucketName}/objects/{EncodeObjectPath(key)}",
                    new StringContent(key, Encoding.UTF8, "text/plain"))).StatusCode);
        }

        var response = await client.GetAsync(
            $"/integrated-s3/{bucketName}?prefix={Uri.EscapeDataString("docs/")}&marker={Uri.EscapeDataString("docs/a file(1).txt")}&max-keys=1&encoding-type=url");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("ListBucketResult", document.Root?.Name.LocalName);
        Assert.Equal(bucketName, GetRequiredElementValue(document, "Name"));
        Assert.Equal("docs%2F", GetRequiredElementValue(document, "Prefix"));
        Assert.Equal("docs%2Fa%20file%281%29.txt", GetRequiredElementValue(document, "Marker"));
        Assert.Equal("url", GetRequiredElementValue(document, "EncodingType"));
        Assert.Equal("1", GetRequiredElementValue(document, "MaxKeys"));
        Assert.Equal("true", GetRequiredElementValue(document, "IsTruncated"));
        Assert.Empty(document.Root!.Elements("KeyCount"));
        Assert.Empty(document.Root.Elements("ContinuationToken"));
        Assert.Empty(document.Root.Elements("NextContinuationToken"));
        Assert.Empty(document.Root.Elements("NextMarker"));

        var listedObject = Assert.Single(document.Root.Elements("Contents"));
        Assert.Equal("docs%2Fb%20file%282%29.txt", listedObject.Element("Key")?.Value);
        Assert.False(string.IsNullOrWhiteSpace(listedObject.Element("Owner")?.Element("ID")?.Value));

        var secondPageResponse = await client.GetAsync(
            $"/integrated-s3/{bucketName}?prefix={Uri.EscapeDataString("docs/")}&marker={Uri.EscapeDataString("docs/b file(2).txt")}&max-keys=10&encoding-type=url");

        Assert.Equal(HttpStatusCode.OK, secondPageResponse.StatusCode);
        var secondPageDocument = XDocument.Parse(await secondPageResponse.Content.ReadAsStringAsync());
        Assert.Equal("docs%2Fc%20file%283%29.txt", Assert.Single(secondPageDocument.Root!.Elements("Contents")).Element("Key")?.Value);
    }

    [Fact]
    public async Task S3CompatibleCopyObject_SourceVersionIdUsesHistoricalVersionPreconditionsAndRejectsDeleteMarkers()
    {
        using var client = await _factory.CreateClientAsync();

        const string sourceBucketName = "s3-compatible-copy-version-source";
        const string targetBucketName = "s3-compatible-copy-version-target";
        const string sourceKey = "docs/source.txt";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{sourceBucketName}", content: null)).StatusCode);
        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{targetBucketName}", content: null)).StatusCode);
        await EnableBucketVersioningAsync(client, sourceBucketName);

        var v1Response = await client.PutAsync(
            $"/integrated-s3/{sourceBucketName}/{sourceKey}",
            new StringContent("version one", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, v1Response.StatusCode);
        var v1VersionId = Assert.Single(v1Response.Headers.GetValues("x-amz-version-id"));

        var v2Response = await client.PutAsync(
            $"/integrated-s3/{sourceBucketName}/{sourceKey}",
            new StringContent("version two", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, v2Response.StatusCode);
        var v2VersionId = Assert.Single(v2Response.Headers.GetValues("x-amz-version-id"));
        Assert.NotEqual(v1VersionId, v2VersionId);

        var historicalSourcePath = $"/integrated-s3/{sourceBucketName}/{sourceKey}?versionId={Uri.EscapeDataString(v1VersionId)}";
        var historicalMetadata = await GetHeadObjectMetadataAsync(client, historicalSourcePath);

        var deleteCurrent = await client.DeleteAsync($"/integrated-s3/{sourceBucketName}/{sourceKey}");
        Assert.Equal(HttpStatusCode.NoContent, deleteCurrent.StatusCode);
        Assert.Equal("true", Assert.Single(deleteCurrent.Headers.GetValues("x-amz-delete-marker")));
        var deleteMarkerVersionId = Assert.Single(deleteCurrent.Headers.GetValues("x-amz-version-id"));

        using var historicalCopyRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{targetBucketName}/docs/historical-copy.txt");
        historicalCopyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source", $"/{sourceBucketName}/{sourceKey}?versionId={Uri.EscapeDataString(v1VersionId)}");
        historicalCopyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source-if-match", historicalMetadata.ETag);
        historicalCopyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source-if-unmodified-since", historicalMetadata.LastModifiedUtc.AddMinutes(5).ToString("R"));

        var historicalCopyResponse = await client.SendAsync(historicalCopyRequest);
        Assert.Equal(HttpStatusCode.OK, historicalCopyResponse.StatusCode);
        Assert.Equal(v1VersionId, Assert.Single(historicalCopyResponse.Headers.GetValues("x-amz-copy-source-version-id")));
        var historicalCopyDocument = XDocument.Parse(await historicalCopyResponse.Content.ReadAsStringAsync());
        Assert.Equal("CopyObjectResult", historicalCopyDocument.Root?.Name.LocalName);

        var copiedHistoricalObject = await client.GetAsync($"/integrated-s3/{targetBucketName}/docs/historical-copy.txt");
        Assert.Equal(HttpStatusCode.OK, copiedHistoricalObject.StatusCode);
        Assert.Equal("version one", await copiedHistoricalObject.Content.ReadAsStringAsync());

        using var currentCopyRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{targetBucketName}/docs/current-copy.txt");
        currentCopyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source", $"/{sourceBucketName}/{sourceKey}");

        var currentCopyResponse = await client.SendAsync(currentCopyRequest);
        Assert.Equal(HttpStatusCode.NotFound, currentCopyResponse.StatusCode);
        Assert.Equal("true", Assert.Single(currentCopyResponse.Headers.GetValues("x-amz-delete-marker")));
        Assert.Equal(deleteMarkerVersionId, Assert.Single(currentCopyResponse.Headers.GetValues("x-amz-version-id")));
        Assert.Null(currentCopyResponse.Content.Headers.LastModified);
        var currentCopyError = XDocument.Parse(await currentCopyResponse.Content.ReadAsStringAsync());
        Assert.Equal("NoSuchKey", GetRequiredElementValue(currentCopyError, "Code"));

        var currentCopyTargetHead = await client.SendAsync(new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{targetBucketName}/docs/current-copy.txt"));
        Assert.Equal(HttpStatusCode.NotFound, currentCopyTargetHead.StatusCode);

        var versionsResponse = await client.GetAsync($"/integrated-s3/{sourceBucketName}?versions&prefix={Uri.EscapeDataString(sourceKey)}");
        Assert.Equal(HttpStatusCode.OK, versionsResponse.StatusCode);
        var versionsDocument = XDocument.Parse(await versionsResponse.Content.ReadAsStringAsync());
        var deleteMarker = Assert.Single(versionsDocument.Root!.Elements("DeleteMarker"));
        var expectedDeleteMarkerLastModified = DateTimeOffset.Parse(deleteMarker.Element("LastModified")!.Value, CultureInfo.InvariantCulture).ToString("R");

        using var explicitDeleteMarkerCopyRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{targetBucketName}/docs/delete-marker-copy.txt");
        explicitDeleteMarkerCopyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source", $"/{sourceBucketName}/{sourceKey}?versionId={Uri.EscapeDataString(deleteMarkerVersionId)}");

        var explicitDeleteMarkerCopyResponse = await client.SendAsync(explicitDeleteMarkerCopyRequest);
        Assert.Equal(HttpStatusCode.MethodNotAllowed, explicitDeleteMarkerCopyResponse.StatusCode);
        Assert.Equal("true", Assert.Single(explicitDeleteMarkerCopyResponse.Headers.GetValues("x-amz-delete-marker")));
        Assert.Equal(deleteMarkerVersionId, Assert.Single(explicitDeleteMarkerCopyResponse.Headers.GetValues("x-amz-version-id")));
        Assert.Equal(expectedDeleteMarkerLastModified, explicitDeleteMarkerCopyResponse.Content.Headers.LastModified?.ToString("R"));
        var explicitDeleteMarkerCopyError = XDocument.Parse(await explicitDeleteMarkerCopyResponse.Content.ReadAsStringAsync());
        Assert.Equal("MethodNotAllowed", GetRequiredElementValue(explicitDeleteMarkerCopyError, "Code"));

        using var missingVersionCopyRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{targetBucketName}/docs/missing-version-copy.txt");
        missingVersionCopyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source", $"/{sourceBucketName}/{sourceKey}?versionId=missing-version");

        var missingVersionCopyResponse = await client.SendAsync(missingVersionCopyRequest);
        Assert.Equal(HttpStatusCode.NotFound, missingVersionCopyResponse.StatusCode);
        var missingVersionCopyError = XDocument.Parse(await missingVersionCopyResponse.Content.ReadAsStringAsync());
        Assert.Equal("NoSuchVersion", GetRequiredElementValue(missingVersionCopyError, "Code"));

        var deleteMarkerCopyTargetHead = await client.SendAsync(new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{targetBucketName}/docs/delete-marker-copy.txt"));
        Assert.Equal(HttpStatusCode.NotFound, deleteMarkerCopyTargetHead.StatusCode);
    }

    [Fact]
    public async Task S3CompatibleBucketRoute_ListType2_ReturnsXmlPayload()
    {
        using var client = await _factory.CreateClientAsync();

        await client.PutAsync("/integrated-s3/buckets/xml-list-bucket", content: null);
        await client.PutAsync("/integrated-s3/buckets/xml-list-bucket/objects/a.txt", new StringContent("A", Encoding.UTF8, "text/plain"));
        await client.PutAsync("/integrated-s3/buckets/xml-list-bucket/objects/b.txt", new StringContent("B", Encoding.UTF8, "text/plain"));
        await client.PutAsync("/integrated-s3/buckets/xml-list-bucket/objects/c.txt", new StringContent("C", Encoding.UTF8, "text/plain"));

        var response = await client.GetAsync("/integrated-s3/xml-list-bucket?list-type=2&max-keys=2");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("ListBucketResult", document.Root?.Name.LocalName);
        Assert.Equal("xml-list-bucket", GetRequiredElementValue(document, "Name"));
        Assert.Equal("2", GetRequiredElementValue(document, "KeyCount"));
        Assert.Equal("2", GetRequiredElementValue(document, "MaxKeys"));
        Assert.Equal("true", GetRequiredElementValue(document, "IsTruncated"));
        Assert.Equal("b.txt", GetRequiredElementValue(document, "NextContinuationToken"));

        var contents = document.Root!.Elements("Contents").Select(static content => content.Element("Key")?.Value).ToArray();
        Assert.Collection(contents,
            static key => Assert.Equal("a.txt", key),
            static key => Assert.Equal("b.txt", key));
        Assert.All(document.Root.Elements("Contents"), static content => Assert.Null(content.Element("Owner")));
    }

    [Fact]
    public async Task S3CompatibleBucketRoute_ListType2_WithFetchOwnerAndEncodingType_ReturnsOwnerMetadata()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "fetch-owner-list-bucket";

        static string EncodeObjectPath(string key)
        {
            return string.Join('/', key.Split('/').Select(Uri.EscapeDataString));
        }

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);
        Assert.Equal(
            HttpStatusCode.OK,
            (await client.PutAsync(
                $"/integrated-s3/buckets/{bucketName}/objects/{EncodeObjectPath("docs/fetch owner(1).txt")}",
                new StringContent("owner", Encoding.UTF8, "text/plain"))).StatusCode);

        var response = await client.GetAsync(
            $"/integrated-s3/{bucketName}?list-type=2&prefix={Uri.EscapeDataString("docs/")}&fetch-owner=true&encoding-type=url");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("url", GetRequiredElementValue(document, "EncodingType"));
        Assert.Equal("docs%2F", GetRequiredElementValue(document, "Prefix"));
        Assert.Equal("1", GetRequiredElementValue(document, "KeyCount"));

        var listedObject = Assert.Single(document.Root!.Elements("Contents"));
        Assert.Equal("docs%2Ffetch%20owner%281%29.txt", listedObject.Element("Key")?.Value);
        Assert.False(string.IsNullOrWhiteSpace(listedObject.Element("Owner")?.Element("ID")?.Value));
    }

    [Fact]
    public async Task S3CompatibleBucketRoute_ListType2_WithDelimiterAndContinuationToken_ReturnsCommonPrefixes()
    {
        using var client = await _factory.CreateClientAsync();

        await client.PutAsync("/integrated-s3/buckets/delimiter-bucket", content: null);
        await client.PutAsync("/integrated-s3/buckets/delimiter-bucket/objects/docs/a.txt", new StringContent("A", Encoding.UTF8, "text/plain"));
        await client.PutAsync("/integrated-s3/buckets/delimiter-bucket/objects/docs/b.txt", new StringContent("B", Encoding.UTF8, "text/plain"));
        await client.PutAsync("/integrated-s3/buckets/delimiter-bucket/objects/images/cat.jpg", new StringContent("C", Encoding.UTF8, "text/plain"));
        await client.PutAsync("/integrated-s3/buckets/delimiter-bucket/objects/images/dog.jpg", new StringContent("D", Encoding.UTF8, "text/plain"));
        await client.PutAsync("/integrated-s3/buckets/delimiter-bucket/objects/readme.txt", new StringContent("R", Encoding.UTF8, "text/plain"));

        var firstResponse = await client.GetAsync("/integrated-s3/delimiter-bucket?list-type=2&delimiter=%2F&max-keys=2");

        Assert.Equal(HttpStatusCode.OK, firstResponse.StatusCode);
        var firstDocument = XDocument.Parse(await firstResponse.Content.ReadAsStringAsync());
        Assert.Equal("/", GetRequiredElementValue(firstDocument, "Delimiter"));
        Assert.Equal("2", GetRequiredElementValue(firstDocument, "KeyCount"));
        Assert.Equal("true", GetRequiredElementValue(firstDocument, "IsTruncated"));

        var firstPrefixes = firstDocument.Root!.Elements("CommonPrefixes")
            .Select(static prefix => prefix.Element("Prefix")?.Value)
            .ToArray();
        Assert.Collection(firstPrefixes,
            static prefix => Assert.Equal("docs/", prefix),
            static prefix => Assert.Equal("images/", prefix));
        Assert.False(firstDocument.Root.Elements("Contents").Any());

        var continuationToken = GetRequiredElementValue(firstDocument, "NextContinuationToken");
        var secondResponse = await client.GetAsync($"/integrated-s3/delimiter-bucket?list-type=2&delimiter=%2F&max-keys=2&continuation-token={Uri.EscapeDataString(continuationToken)}");

        Assert.Equal(HttpStatusCode.OK, secondResponse.StatusCode);
        var secondDocument = XDocument.Parse(await secondResponse.Content.ReadAsStringAsync());
        Assert.Equal("1", GetRequiredElementValue(secondDocument, "KeyCount"));
        Assert.Equal("false", GetRequiredElementValue(secondDocument, "IsTruncated"));
        Assert.Equal("readme.txt", Assert.Single(secondDocument.Root!.Elements("Contents")).Element("Key")?.Value);
    }

    [Fact]
    public async Task S3CompatibleBucketRoute_ListType2_WithStartAfter_SkipsEarlierKeys()
    {
        using var client = await _factory.CreateClientAsync();

        await client.PutAsync("/integrated-s3/buckets/start-after-bucket", content: null);
        await client.PutAsync("/integrated-s3/buckets/start-after-bucket/objects/a.txt", new StringContent("A", Encoding.UTF8, "text/plain"));
        await client.PutAsync("/integrated-s3/buckets/start-after-bucket/objects/b.txt", new StringContent("B", Encoding.UTF8, "text/plain"));
        await client.PutAsync("/integrated-s3/buckets/start-after-bucket/objects/c.txt", new StringContent("C", Encoding.UTF8, "text/plain"));

        var response = await client.GetAsync("/integrated-s3/start-after-bucket?list-type=2&start-after=a.txt");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("a.txt", GetRequiredElementValue(document, "StartAfter"));

        var keys = document.Root!.Elements("Contents")
            .Select(static content => content.Element("Key")?.Value)
            .ToArray();
        Assert.Collection(keys,
            static key => Assert.Equal("b.txt", key),
            static key => Assert.Equal("c.txt", key));
    }

    [Fact]
    public async Task S3CompatibleDeleteMissingObjects_AreIdempotentAndVersionedDeletesCreateDeleteMarkers()
    {
        using var client = await _factory.CreateClientAsync();

        await client.PutAsync("/integrated-s3/buckets/delete-missing-bucket", content: null);

        var deleteMissingResponse = await client.DeleteAsync("/integrated-s3/delete-missing-bucket/docs/missing.txt");
        Assert.Equal(HttpStatusCode.NoContent, deleteMissingResponse.StatusCode);
        Assert.False(deleteMissingResponse.Headers.Contains("x-amz-delete-marker"));
        Assert.False(deleteMissingResponse.Headers.Contains("x-amz-version-id"));

        await client.PutAsync("/integrated-s3/buckets/delete-missing-versioned-bucket", content: null);
        await EnableBucketVersioningAsync(client, "delete-missing-versioned-bucket");

        var deleteVersionedResponse = await client.DeleteAsync("/integrated-s3/delete-missing-versioned-bucket/docs/missing.txt");
        Assert.Equal(HttpStatusCode.NoContent, deleteVersionedResponse.StatusCode);
        Assert.Equal("true", Assert.Single(deleteVersionedResponse.Headers.GetValues("x-amz-delete-marker")));
        var deleteMarkerVersionId = Assert.Single(deleteVersionedResponse.Headers.GetValues("x-amz-version-id"));

        var currentGetResponse = await client.GetAsync("/integrated-s3/delete-missing-versioned-bucket/docs/missing.txt");
        Assert.Equal(HttpStatusCode.NotFound, currentGetResponse.StatusCode);
        Assert.Equal("true", Assert.Single(currentGetResponse.Headers.GetValues("x-amz-delete-marker")));
        Assert.Equal(deleteMarkerVersionId, Assert.Single(currentGetResponse.Headers.GetValues("x-amz-version-id")));

        var versionsResponse = await client.GetAsync("/integrated-s3/delete-missing-versioned-bucket?versions");
        Assert.Equal(HttpStatusCode.OK, versionsResponse.StatusCode);
        var versionsDocument = XDocument.Parse(await versionsResponse.Content.ReadAsStringAsync());
        var deleteMarker = Assert.Single(versionsDocument.Root!.Elements("DeleteMarker"));
        Assert.Equal("docs/missing.txt", deleteMarker.Element("Key")?.Value);
        Assert.Equal(deleteMarkerVersionId, deleteMarker.Element("VersionId")?.Value);
        Assert.Equal("true", deleteMarker.Element("IsLatest")?.Value);
    }

    [Fact]
    public async Task S3CompatibleBatchDelete_ReturnsXmlDeleteResultAndDeletesObjects()
    {
        using var client = await _factory.CreateClientAsync();

        await client.PutAsync("/integrated-s3/buckets/batch-delete-bucket", content: null);
        await client.PutAsync("/integrated-s3/buckets/batch-delete-bucket/objects/a.txt", new StringContent("A", Encoding.UTF8, "text/plain"));
        await client.PutAsync("/integrated-s3/buckets/batch-delete-bucket/objects/b.txt", new StringContent("B", Encoding.UTF8, "text/plain"));

        const string deleteBody = """
<Delete>
  <Object><Key>a.txt</Key></Object>
  <Object><Key>missing.txt</Key></Object>
</Delete>
""";

        using var deleteRequest = new HttpRequestMessage(HttpMethod.Post, "/integrated-s3/batch-delete-bucket?delete")
        {
            Content = new StringContent(deleteBody, Encoding.UTF8, "application/xml")
        };

        var deleteResponse = await client.SendAsync(deleteRequest);

        Assert.Equal(HttpStatusCode.OK, deleteResponse.StatusCode);
        Assert.Equal("application/xml", deleteResponse.Content.Headers.ContentType?.MediaType);

        var deleteDocument = XDocument.Parse(await deleteResponse.Content.ReadAsStringAsync());
        Assert.Equal("DeleteResult", deleteDocument.Root?.Name.LocalName);

        var deletedKeys = deleteDocument.Root!.Elements("Deleted")
            .Select(static deleted => deleted.Element("Key")?.Value)
            .ToArray();
        Assert.Collection(deletedKeys,
            static key => Assert.Equal("a.txt", key),
            static key => Assert.Equal("missing.txt", key));

        var deletedHead = await client.SendAsync(new HttpRequestMessage(HttpMethod.Head, "/integrated-s3/buckets/batch-delete-bucket/objects/a.txt"));
        Assert.Equal(HttpStatusCode.NotFound, deletedHead.StatusCode);

        var survivingHead = await client.SendAsync(new HttpRequestMessage(HttpMethod.Head, "/integrated-s3/buckets/batch-delete-bucket/objects/b.txt"));
        Assert.Equal(HttpStatusCode.OK, survivingHead.StatusCode);
    }

    [Fact]
    public async Task S3CompatibleBatchDelete_ReturnsNoSuchVersionForExplicitMissingVersions()
    {
        using var client = await _factory.CreateClientAsync();

        await client.PutAsync("/integrated-s3/buckets/batch-delete-versions-bucket", content: null);
        await EnableBucketVersioningAsync(client, "batch-delete-versions-bucket");

        using var putObjectRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/batch-delete-versions-bucket/docs/history.txt")
        {
            Content = new StringContent("version one", Encoding.UTF8, "text/plain")
        };

        var putObjectResponse = await client.SendAsync(putObjectRequest);
        Assert.Equal(HttpStatusCode.OK, putObjectResponse.StatusCode);
        var versionId = Assert.Single(putObjectResponse.Headers.GetValues("x-amz-version-id"));

        var deleteBody = $"""
<Delete>
  <Object><Key>docs/history.txt</Key><VersionId>{versionId}</VersionId></Object>
  <Object><Key>docs/history.txt</Key><VersionId>missing-version</VersionId></Object>
</Delete>
""";

        using var deleteRequest = new HttpRequestMessage(HttpMethod.Post, "/integrated-s3/batch-delete-versions-bucket?delete")
        {
            Content = new StringContent(deleteBody, Encoding.UTF8, "application/xml")
        };

        var deleteResponse = await client.SendAsync(deleteRequest);
        Assert.Equal(HttpStatusCode.OK, deleteResponse.StatusCode);

        var deleteDocument = XDocument.Parse(await deleteResponse.Content.ReadAsStringAsync());
        var deleted = Assert.Single(deleteDocument.Root!.Elements("Deleted"));
        Assert.Equal("docs/history.txt", deleted.Element("Key")?.Value);
        Assert.Equal(versionId, deleted.Element("VersionId")?.Value);

        var error = Assert.Single(deleteDocument.Root.Elements("Error"));
        Assert.Equal("docs/history.txt", error.Element("Key")?.Value);
        Assert.Equal("missing-version", error.Element("VersionId")?.Value);
        Assert.Equal("NoSuchVersion", error.Element("Code")?.Value);
    }

    [Fact]
    public async Task S3CompatibleBatchDelete_QuietModeSuppressesDeletedResultsButRetainsErrors()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "batch-delete-quiet-bucket";
        const string objectKey = "docs/history.txt";
        const string missingVersionId = "missing-version";

        await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null);
        await EnableBucketVersioningAsync(client, bucketName);

        using var putObjectRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/{objectKey}")
        {
            Content = new StringContent("version one", Encoding.UTF8, "text/plain")
        };

        var putObjectResponse = await client.SendAsync(putObjectRequest);
        Assert.Equal(HttpStatusCode.OK, putObjectResponse.StatusCode);

        var deleteBody = $"""
<Delete>
  <Object><Key>{objectKey}</Key></Object>
  <Object><Key>{objectKey}</Key><VersionId>{missingVersionId}</VersionId></Object>
  <Quiet>true</Quiet>
</Delete>
""";

        using var deleteRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}?delete")
        {
            Content = new StringContent(deleteBody, Encoding.UTF8, "application/xml")
        };

        var deleteResponse = await client.SendAsync(deleteRequest);
        Assert.Equal(HttpStatusCode.OK, deleteResponse.StatusCode);

        var deleteDocument = XDocument.Parse(await deleteResponse.Content.ReadAsStringAsync());
        Assert.Empty(deleteDocument.Root!.Elements("Deleted"));

        var error = Assert.Single(deleteDocument.Root.Elements("Error"));
        Assert.Equal(objectKey, error.Element("Key")?.Value);
        Assert.Equal(missingVersionId, error.Element("VersionId")?.Value);
        Assert.Equal("NoSuchVersion", error.Element("Code")?.Value);

        var currentGetResponse = await client.GetAsync($"/integrated-s3/{bucketName}/{objectKey}");
        Assert.Equal(HttpStatusCode.NotFound, currentGetResponse.StatusCode);
        Assert.Equal("true", Assert.Single(currentGetResponse.Headers.GetValues("x-amz-delete-marker")));
        Assert.False(string.IsNullOrWhiteSpace(Assert.Single(currentGetResponse.Headers.GetValues("x-amz-version-id"))));
    }

    [Fact]
    public async Task S3CompatibleBatchDelete_PreservesDeleteMarkerVersionIdsForVersionedDeletes()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "batch-delete-delete-marker-bucket";
        const string objectKey = "docs/history.txt";

        await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null);
        await EnableBucketVersioningAsync(client, bucketName);

        using var putObjectRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/{objectKey}")
        {
            Content = new StringContent("version one", Encoding.UTF8, "text/plain")
        };

        var putObjectResponse = await client.SendAsync(putObjectRequest);
        Assert.Equal(HttpStatusCode.OK, putObjectResponse.StatusCode);
        var objectVersionId = Assert.Single(putObjectResponse.Headers.GetValues("x-amz-version-id"));

        const string createDeleteMarkerBody = """
<Delete>
  <Object><Key>docs/history.txt</Key></Object>
</Delete>
""";

        using var createDeleteMarkerRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}?delete")
        {
            Content = new StringContent(createDeleteMarkerBody, Encoding.UTF8, "application/xml")
        };

        var createDeleteMarkerResponse = await client.SendAsync(createDeleteMarkerRequest);
        Assert.Equal(HttpStatusCode.OK, createDeleteMarkerResponse.StatusCode);

        var createDeleteMarkerDocument = XDocument.Parse(await createDeleteMarkerResponse.Content.ReadAsStringAsync());
        var createdDeleteMarker = Assert.Single(createDeleteMarkerDocument.Root!.Elements("Deleted"));
        Assert.Empty(createDeleteMarkerDocument.Root.Elements("Error"));
        Assert.Equal(objectKey, createdDeleteMarker.Element("Key")?.Value);
        Assert.Null(createdDeleteMarker.Element("VersionId"));
        Assert.Equal("true", createdDeleteMarker.Element("DeleteMarker")?.Value);
        var deleteMarkerVersionId = Assert.IsType<string>(createdDeleteMarker.Element("DeleteMarkerVersionId")?.Value);
        Assert.NotEqual(objectVersionId, deleteMarkerVersionId);

        var currentGetResponse = await client.GetAsync($"/integrated-s3/{bucketName}/{objectKey}");
        Assert.Equal(HttpStatusCode.NotFound, currentGetResponse.StatusCode);
        Assert.Equal("true", Assert.Single(currentGetResponse.Headers.GetValues("x-amz-delete-marker")));
        Assert.Equal(deleteMarkerVersionId, Assert.Single(currentGetResponse.Headers.GetValues("x-amz-version-id")));

        var deleteDeleteMarkerBody = $"""
<Delete>
  <Object><Key>{objectKey}</Key><VersionId>{deleteMarkerVersionId}</VersionId></Object>
</Delete>
""";

        using var deleteDeleteMarkerRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}?delete")
        {
            Content = new StringContent(deleteDeleteMarkerBody, Encoding.UTF8, "application/xml")
        };

        var deleteDeleteMarkerResponse = await client.SendAsync(deleteDeleteMarkerRequest);
        Assert.Equal(HttpStatusCode.OK, deleteDeleteMarkerResponse.StatusCode);

        var deleteDeleteMarkerDocument = XDocument.Parse(await deleteDeleteMarkerResponse.Content.ReadAsStringAsync());
        var deletedDeleteMarker = Assert.Single(deleteDeleteMarkerDocument.Root!.Elements("Deleted"));
        Assert.Empty(deleteDeleteMarkerDocument.Root.Elements("Error"));
        Assert.Equal(objectKey, deletedDeleteMarker.Element("Key")?.Value);
        Assert.Equal(deleteMarkerVersionId, deletedDeleteMarker.Element("VersionId")?.Value);
        Assert.Equal("true", deletedDeleteMarker.Element("DeleteMarker")?.Value);
        Assert.Equal(deleteMarkerVersionId, deletedDeleteMarker.Element("DeleteMarkerVersionId")?.Value);

        var restoredGetResponse = await client.GetAsync($"/integrated-s3/{bucketName}/{objectKey}");
        Assert.Equal(HttpStatusCode.OK, restoredGetResponse.StatusCode);
        Assert.Equal("version one", await restoredGetResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task S3CompatibleObjectTagging_RoundTripsXmlPayload()
    {
        using var client = await _factory.CreateClientAsync();

        await client.PutAsync("/integrated-s3/buckets/tagging-bucket", content: null);
        await client.PutAsync(
            "/integrated-s3/buckets/tagging-bucket/objects/docs/tagged.txt",
            new StringContent("hello tags", Encoding.UTF8, "text/plain"));

        const string taggingBody = """
<Tagging>
  <TagSet>
    <Tag><Key>café+owner</Key><Value>München 𐐀 + copilot</Value></Tag>
    <Tag><Key>release/team</Key><Value>v1.0</Value></Tag>
  </TagSet>
</Tagging>
""";

        using var putTaggingRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/tagging-bucket/docs/tagged.txt?tagging")
        {
            Content = new StringContent(taggingBody, Encoding.UTF8, "application/xml")
        };

        var putTaggingResponse = await client.SendAsync(putTaggingRequest);
        Assert.Equal(HttpStatusCode.OK, putTaggingResponse.StatusCode);

        var getTaggingResponse = await client.GetAsync("/integrated-s3/tagging-bucket/docs/tagged.txt?tagging");
        Assert.Equal(HttpStatusCode.OK, getTaggingResponse.StatusCode);
        Assert.Equal("application/xml", getTaggingResponse.Content.Headers.ContentType?.MediaType);

        var taggingDocument = XDocument.Parse(await getTaggingResponse.Content.ReadAsStringAsync());
        Assert.Equal("Tagging", taggingDocument.Root?.Name.LocalName);
        var tags = taggingDocument.Root!.Element("TagSet")!.Elements("Tag")
            .ToDictionary(
                static tag => tag.Element("Key")?.Value ?? string.Empty,
                static tag => tag.Element("Value")?.Value ?? string.Empty,
                StringComparer.Ordinal);

        Assert.Equal("München 𐐀 + copilot", tags["café+owner"]);
        Assert.Equal("v1.0", tags["release/team"]);

        var getObjectResponse = await client.GetAsync("/integrated-s3/buckets/tagging-bucket/objects/docs/tagged.txt");
        Assert.Equal(HttpStatusCode.OK, getObjectResponse.StatusCode);
        Assert.Equal("2", Assert.Single(getObjectResponse.Headers.GetValues("x-amz-tagging-count")));

        var headObjectResponse = await client.SendAsync(new HttpRequestMessage(HttpMethod.Head, "/integrated-s3/buckets/tagging-bucket/objects/docs/tagged.txt"));
        Assert.Equal(HttpStatusCode.OK, headObjectResponse.StatusCode);
        Assert.Equal("2", Assert.Single(headObjectResponse.Headers.GetValues("x-amz-tagging-count")));
    }

    [Fact]
    public async Task S3CompatibleObjectTagging_RejectsInvalidTagSets()
    {
        using var client = await _factory.CreateClientAsync();

        await client.PutAsync("/integrated-s3/buckets/invalid-tagging-bucket", content: null);
        await client.PutAsync(
            "/integrated-s3/buckets/invalid-tagging-bucket/objects/docs/tagged.txt",
            new StringContent("hello tags", Encoding.UTF8, "text/plain"));

        var tagSet = string.Join(Environment.NewLine, Enumerable.Range(0, 11).Select(index =>
            $"    <Tag><Key>tag-{index}</Key><Value>value-{index}</Value></Tag>"));
        var taggingBody = $"""
<Tagging>
  <TagSet>
{tagSet}
  </TagSet>
</Tagging>
""";

        using var putTaggingRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/invalid-tagging-bucket/docs/tagged.txt?tagging")
        {
            Content = new StringContent(taggingBody, Encoding.UTF8, "application/xml")
        };

        var putTaggingResponse = await client.SendAsync(putTaggingRequest);
        Assert.Equal(HttpStatusCode.BadRequest, putTaggingResponse.StatusCode);

        var errorDocument = XDocument.Parse(await putTaggingResponse.Content.ReadAsStringAsync());
        Assert.Equal("InvalidTag", GetRequiredElementValue(errorDocument, "Code"));
    }

    [Fact]
    public async Task S3CompatiblePutObject_WithTaggingHeader_PersistsObjectTags()
    {
        using var client = await _factory.CreateClientAsync();

        await client.PutAsync("/integrated-s3/buckets/tagging-header-put-bucket", content: null);

        using var putRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/tagging-header-put-bucket/docs/tagged.txt")
        {
            Content = new StringContent("hello tags", Encoding.UTF8, "text/plain")
        };
        putRequest.Headers.TryAddWithoutValidation("x-amz-tagging", BuildTaggingHeader(
            ("café+owner", "München 𐐀 + copilot"),
            ("release/team", "v1.0")));

        var putResponse = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);

        var tags = await GetObjectTagsAsync(client, "tagging-header-put-bucket", "docs/tagged.txt");
        Assert.Equal("München 𐐀 + copilot", tags["café+owner"]);
        Assert.Equal("v1.0", tags["release/team"]);
    }

    [Fact]
    public async Task S3CompatibleObjectTagging_RejectsInvalidCharactersAcrossXmlAndHeaderPaths()
    {
        using var client = await _factory.CreateClientAsync();

        await client.PutAsync("/integrated-s3/buckets/invalid-tag-character-bucket", content: null);
        await client.PutAsync(
            "/integrated-s3/buckets/invalid-tag-character-bucket/objects/docs/xml.txt",
            new StringContent("xml tags", Encoding.UTF8, "text/plain"));

        const string invalidTagValue = "blue\u2028green";
        var taggingBody = $$"""
<Tagging>
  <TagSet>
    <Tag><Key>environment</Key><Value>{{invalidTagValue}}</Value></Tag>
  </TagSet>
</Tagging>
""";

        using var putTaggingRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/invalid-tag-character-bucket/docs/xml.txt?tagging")
        {
            Content = new StringContent(taggingBody, Encoding.UTF8, "application/xml")
        };

        var putTaggingResponse = await client.SendAsync(putTaggingRequest);
        Assert.Equal(HttpStatusCode.BadRequest, putTaggingResponse.StatusCode);
        Assert.Equal("InvalidTag", GetRequiredElementValue(XDocument.Parse(await putTaggingResponse.Content.ReadAsStringAsync()), "Code"));

        using var putRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/invalid-tag-character-bucket/docs/header.txt")
        {
            Content = new StringContent("header tags", Encoding.UTF8, "text/plain")
        };
        putRequest.Headers.TryAddWithoutValidation("x-amz-tagging", BuildTaggingHeader(("environment", invalidTagValue)));

        var putResponse = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.BadRequest, putResponse.StatusCode);
        Assert.Equal("InvalidTag", GetRequiredElementValue(XDocument.Parse(await putResponse.Content.ReadAsStringAsync()), "Code"));
    }

    [Fact]
    public async Task S3CompatibleCopyObject_WithReplaceTaggingDirective_PersistsReplacementTags()
    {
        using var client = await _factory.CreateClientAsync();

        await client.PutAsync("/integrated-s3/buckets/tagging-copy-bucket", content: null);
        await client.PutAsync(
            "/integrated-s3/buckets/tagging-copy-bucket/objects/docs/source.txt",
            new StringContent("copy me", Encoding.UTF8, "text/plain"));

        using var putTaggingRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/tagging-copy-bucket/docs/source.txt?tagging")
        {
            Content = new StringContent("""
<Tagging>
  <TagSet>
    <Tag><Key>environment</Key><Value>source</Value></Tag>
    <Tag><Key>owner</Key><Value>original</Value></Tag>
  </TagSet>
</Tagging>
""", Encoding.UTF8, "application/xml")
        };
        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(putTaggingRequest)).StatusCode);

        using var copyRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/tagging-copy-bucket/docs/copied.txt");
        copyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source", "/tagging-copy-bucket/docs/source.txt");
        copyRequest.Headers.TryAddWithoutValidation("x-amz-tagging-directive", "REPLACE");
        copyRequest.Headers.TryAddWithoutValidation("x-amz-tagging", "environment=target&owner=copilot");

        var copyResponse = await client.SendAsync(copyRequest);
        Assert.Equal(HttpStatusCode.OK, copyResponse.StatusCode);

        var copiedTags = await GetObjectTagsAsync(client, "tagging-copy-bucket", "docs/copied.txt");
        Assert.Equal("target", copiedTags["environment"]);
        Assert.Equal("copilot", copiedTags["owner"]);
        Assert.DoesNotContain("original", copiedTags.Values);
    }

    [Fact]
    public async Task VirtualHostedStyleRequests_ResolveBucketFromHost()
    {
        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(builder => {
            builder.Services.Configure<IntegratedS3Options>(options => {
                options.EnableVirtualHostedStyleAddressing = true;
                options.VirtualHostedStyleHostSuffixes = ["localhost"];
            });
        });

        using var client = isolatedClient.Client;

        using var createBucketRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/");
        createBucketRequest.Headers.Host = "virtual-bucket.localhost";
        var createBucketResponse = await client.SendAsync(createBucketRequest);
        Assert.Equal(HttpStatusCode.OK, createBucketResponse.StatusCode);

        using var putObjectRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/docs/virtual.txt")
        {
            Content = new StringContent("hello from host style", Encoding.UTF8, "text/plain")
        };
        putObjectRequest.Headers.Host = "virtual-bucket.localhost";
        var putObjectResponse = await client.SendAsync(putObjectRequest);
        Assert.Equal(HttpStatusCode.OK, putObjectResponse.StatusCode);

        using var listRequest = new HttpRequestMessage(HttpMethod.Get, "/integrated-s3/?list-type=2");
        listRequest.Headers.Host = "virtual-bucket.localhost";
        var listResponse = await client.SendAsync(listRequest);
        Assert.Equal(HttpStatusCode.OK, listResponse.StatusCode);
        var listDocument = XDocument.Parse(await listResponse.Content.ReadAsStringAsync());
        Assert.Equal("virtual-bucket", GetRequiredElementValue(listDocument, "Name"));
        Assert.Equal("docs/virtual.txt", Assert.Single(listDocument.Root!.Elements("Contents")).Element("Key")?.Value);

        using var getObjectRequest = new HttpRequestMessage(HttpMethod.Get, "/integrated-s3/docs/virtual.txt");
        getObjectRequest.Headers.Host = "virtual-bucket.localhost";
        var getObjectResponse = await client.SendAsync(getObjectRequest);
        Assert.Equal(HttpStatusCode.OK, getObjectResponse.StatusCode);
        Assert.Equal("hello from host style", await getObjectResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task VirtualHostedStyleRequests_CanGetBucketVersioningConfigurationFromBucketRoute()
    {
        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(builder => {
            builder.Services.Configure<IntegratedS3Options>(options => {
                options.EnableVirtualHostedStyleAddressing = true;
                options.VirtualHostedStyleHostSuffixes = ["localhost"];
            });
        });

        using var client = isolatedClient.Client;

        using var createBucketRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/");
        createBucketRequest.Headers.Host = "virtual-versioning.localhost";
        var createBucketResponse = await client.SendAsync(createBucketRequest);
        Assert.Equal(HttpStatusCode.OK, createBucketResponse.StatusCode);

        using var putVersioningRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/?versioning")
        {
            Content = new StringContent("""
<VersioningConfiguration>
  <Status>Enabled</Status>
</VersioningConfiguration>
""", Encoding.UTF8, "application/xml")
        };
        putVersioningRequest.Headers.Host = "virtual-versioning.localhost";
        var putVersioningResponse = await client.SendAsync(putVersioningRequest);
        Assert.Equal(HttpStatusCode.OK, putVersioningResponse.StatusCode);

        using var getVersioningRequest = new HttpRequestMessage(HttpMethod.Get, "/integrated-s3/?versioning");
        getVersioningRequest.Headers.Host = "virtual-versioning.localhost";
        var getVersioningResponse = await client.SendAsync(getVersioningRequest);

        Assert.Equal(HttpStatusCode.OK, getVersioningResponse.StatusCode);
        Assert.Equal("application/xml", getVersioningResponse.Content.Headers.ContentType?.MediaType);
        var versioningDocument = XDocument.Parse(await getVersioningResponse.Content.ReadAsStringAsync());
        Assert.Equal("VersioningConfiguration", versioningDocument.Root?.Name.LocalName);
        Assert.Equal("Enabled", GetRequiredElementValue(versioningDocument, "Status"));
    }

    [Fact]
    public async Task VirtualHostedStyleRequests_CanListMultipartUploadsFromBucketRoute()
    {
        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(builder => {
            builder.Services.Configure<IntegratedS3Options>(options => {
                options.EnableVirtualHostedStyleAddressing = true;
                options.VirtualHostedStyleHostSuffixes = ["localhost"];
            });
        });

        using var client = isolatedClient.Client;

        using var createBucketRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/");
        createBucketRequest.Headers.Host = "virtual-multipart.localhost";
        var createBucketResponse = await client.SendAsync(createBucketRequest);
        Assert.Equal(HttpStatusCode.OK, createBucketResponse.StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, "/integrated-s3/docs/upload.txt?uploads");
        initiateRequest.Headers.Host = "virtual-multipart.localhost";
        var initiateResponse = await client.SendAsync(initiateRequest);
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var listRequest = new HttpRequestMessage(HttpMethod.Get, "/integrated-s3/?uploads");
        listRequest.Headers.Host = "virtual-multipart.localhost";
        var listResponse = await client.SendAsync(listRequest);
        Assert.Equal(HttpStatusCode.OK, listResponse.StatusCode);

        var listDocument = XDocument.Parse(await listResponse.Content.ReadAsStringAsync());
        Assert.Equal("virtual-multipart", GetRequiredElementValue(listDocument, "Bucket"));
        var upload = Assert.Single(listDocument.Root!.Elements("Upload"));
        Assert.Equal("docs/upload.txt", upload.Element("Key")?.Value);
        Assert.Equal(uploadId, upload.Element("UploadId")?.Value);
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_AllowsStorageRequestsThroughAuthorizationLayer()
    {
        const string accessKeyId = "sigv4-access";
        const string secretAccessKey = "sigv4-secret";

        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(builder => {
            builder.Services.Configure<IntegratedS3Options>(options => {
                options.EnableAwsSignatureV4Authentication = true;
                options.AccessKeyCredentials =
                [
                    new IntegratedS3AccessKeyCredential
                    {
                        AccessKeyId = accessKeyId,
                        SecretAccessKey = secretAccessKey,
                        DisplayName = "sigv4-user",
                        Scopes = ["storage.read", "storage.write"]
                    }
                ];
            });
            builder.Services.AddSingleton<IIntegratedS3AuthorizationService, ScopeBasedIntegratedS3AuthorizationService>();
        });

        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, "/integrated-s3/buckets/signed-bucket", accessKeyId, secretAccessKey);
        var createBucketResponse = await client.SendAsync(createBucketRequest);
        Assert.Equal(HttpStatusCode.Created, createBucketResponse.StatusCode);

        using var putObjectRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, "/integrated-s3/buckets/signed-bucket/objects/docs/hello.txt", accessKeyId, secretAccessKey, body: "hello from sigv4", contentType: "text/plain");
        var putObjectResponse = await client.SendAsync(putObjectRequest);
        Assert.Equal(HttpStatusCode.OK, putObjectResponse.StatusCode);

        using var getObjectRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Get, "/integrated-s3/buckets/signed-bucket/objects/docs/hello.txt", accessKeyId, secretAccessKey);
        var getObjectResponse = await client.SendAsync(getObjectRequest);
        Assert.Equal(HttpStatusCode.OK, getObjectResponse.StatusCode);
        Assert.Equal("hello from sigv4", await getObjectResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task SigV4PresignedQueryAuthentication_AllowsReads()
    {
        const string accessKeyId = "presign-access";
        const string secretAccessKey = "presign-secret";

        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(builder => {
            builder.Services.Configure<IntegratedS3Options>(options => {
                options.EnableAwsSignatureV4Authentication = true;
                options.AccessKeyCredentials =
                [
                    new IntegratedS3AccessKeyCredential
                    {
                        AccessKeyId = accessKeyId,
                        SecretAccessKey = secretAccessKey,
                        DisplayName = "presign-user",
                        Scopes = ["storage.read", "storage.write"]
                    }
                ];
            });
            builder.Services.AddSingleton<IIntegratedS3AuthorizationService, ScopeBasedIntegratedS3AuthorizationService>();
        });

        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, "/integrated-s3/buckets/presigned-bucket", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var putObjectRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, "/integrated-s3/buckets/presigned-bucket/objects/docs/presigned.txt", accessKeyId, secretAccessKey, body: "presigned hello", contentType: "text/plain");
        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(putObjectRequest)).StatusCode);

        using var presignedRequest = CreateSigV4PresignedRequest(HttpMethod.Get, "/integrated-s3/buckets/presigned-bucket/objects/docs/presigned.txt", accessKeyId, secretAccessKey, expiresSeconds: 120);
        var presignedResponse = await client.SendAsync(presignedRequest);

        Assert.Equal(HttpStatusCode.OK, presignedResponse.StatusCode);
        Assert.Equal("presigned hello", await presignedResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task FirstPartyPresignClient_CreatesUploadAndDownloadRequests()
    {
        const string accessKeyId = "first-party-presign-access";
        const string secretAccessKey = "first-party-presign-secret";
        const string bucketName = "first-party-presign-bucket";
        const string objectKey = "docs/client-presigned.txt";
        const string payload = "hello from first-party client";

        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(builder => {
            builder.Services.Configure<IntegratedS3Options>(options => {
                options.EnableAwsSignatureV4Authentication = true;
                options.PresignAccessKeyId = accessKeyId;
                options.AccessKeyCredentials =
                [
                    new IntegratedS3AccessKeyCredential
                    {
                        AccessKeyId = accessKeyId,
                        SecretAccessKey = secretAccessKey,
                        DisplayName = "first-party-presign-user",
                        Scopes = ["storage.read", "storage.write"]
                    }
                ];
            });
            builder.Services.AddAuthentication("TestHeader")
                .AddScheme<AuthenticationSchemeOptions, TestHeaderAuthenticationHandler>("TestHeader", static _ => { });
            builder.Services.AddSingleton<IIntegratedS3AuthorizationService, ScopeBasedIntegratedS3AuthorizationService>();
        });

        using var client = isolatedClient.Client;
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "storage.read storage.write");

        var integratedClient = new IntegratedS3Client(client);

        var createBucketResponse = await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null);
        Assert.Equal(HttpStatusCode.Created, createBucketResponse.StatusCode);

        var uploadPresign = await integratedClient.PresignPutObjectAsync(bucketName, objectKey, expiresInSeconds: 300, contentType: "text/plain");
        Assert.Equal(StorageAccessMode.Proxy, uploadPresign.AccessMode);
        Assert.Equal("PUT", uploadPresign.Method);
        Assert.Contains(uploadPresign.Headers, static header => header.Name == "Content-Type" && header.Value == "text/plain");

        client.DefaultRequestHeaders.Authorization = null;

        using (var uploadRequest = uploadPresign.CreateHttpRequestMessage(new StringContent(payload, Encoding.UTF8))) {
            var uploadResponse = await client.SendAsync(uploadRequest);
            Assert.Equal(HttpStatusCode.OK, uploadResponse.StatusCode);
        }

        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "storage.read storage.write");
        var downloadPresign = await integratedClient.PresignGetObjectAsync(bucketName, objectKey, expiresInSeconds: 300);
        Assert.Equal(StorageAccessMode.Proxy, downloadPresign.AccessMode);
        Assert.Equal("GET", downloadPresign.Method);

        client.DefaultRequestHeaders.Authorization = null;

        using var downloadRequest = downloadPresign.CreateHttpRequestMessage();
        var downloadResponse = await client.SendAsync(downloadRequest);
        Assert.Equal(HttpStatusCode.OK, downloadResponse.StatusCode);
        Assert.Equal(payload, await downloadResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task FirstPartyPresignClient_WithSessionToken_CreatesUploadAndDownloadRequests()
    {
        const string accessKeyId = "first-party-presign-session-access";
        const string secretAccessKey = "first-party-presign-session-secret";
        const string sessionToken = "first-party-presign-session-token";
        const string bucketName = "first-party-presign-session-bucket";
        const string objectKey = "docs/client-presigned-session.txt";
        const string payload = "hello from first-party client session";

        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(builder => {
            builder.Services.Configure<IntegratedS3Options>(options => {
                options.EnableAwsSignatureV4Authentication = true;
                options.PresignAccessKeyId = accessKeyId;
                options.AccessKeyCredentials =
                [
                    new IntegratedS3AccessKeyCredential
                    {
                        AccessKeyId = accessKeyId,
                        SecretAccessKey = secretAccessKey,
                        SessionToken = sessionToken,
                        DisplayName = "first-party-presign-session-user",
                        Scopes = ["storage.read", "storage.write"]
                    }
                ];
            });
            builder.Services.AddAuthentication("TestHeader")
                .AddScheme<AuthenticationSchemeOptions, TestHeaderAuthenticationHandler>("TestHeader", static _ => { });
            builder.Services.AddSingleton<IIntegratedS3AuthorizationService, ScopeBasedIntegratedS3AuthorizationService>();
        });

        using var client = isolatedClient.Client;
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "storage.read storage.write");

        var integratedClient = new IntegratedS3Client(client);

        var createBucketResponse = await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null);
        Assert.Equal(HttpStatusCode.Created, createBucketResponse.StatusCode);

        var uploadPresign = await integratedClient.PresignPutObjectAsync(bucketName, objectKey, expiresInSeconds: 300, contentType: "text/plain");
        Assert.Equal(StorageAccessMode.Proxy, uploadPresign.AccessMode);
        Assert.Equal(sessionToken, QueryHelpers.ParseQuery(uploadPresign.Url.Query)["X-Amz-Security-Token"].ToString());

        client.DefaultRequestHeaders.Authorization = null;

        using (var uploadRequest = uploadPresign.CreateHttpRequestMessage(new StringContent(payload, Encoding.UTF8))) {
            var uploadResponse = await client.SendAsync(uploadRequest);
            Assert.Equal(HttpStatusCode.OK, uploadResponse.StatusCode);
        }

        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "storage.read storage.write");
        var downloadPresign = await integratedClient.PresignGetObjectAsync(bucketName, objectKey, expiresInSeconds: 300);
        Assert.Equal(StorageAccessMode.Proxy, downloadPresign.AccessMode);
        Assert.Equal(sessionToken, QueryHelpers.ParseQuery(downloadPresign.Url.Query)["X-Amz-Security-Token"].ToString());

        client.DefaultRequestHeaders.Authorization = null;

        using var downloadRequest = downloadPresign.CreateHttpRequestMessage();
        var downloadResponse = await client.SendAsync(downloadRequest);
        Assert.Equal(HttpStatusCode.OK, downloadResponse.StatusCode);
        Assert.Equal(payload, await downloadResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task PresignEndpoint_WhenExpiryExceedsMaximum_Returns400()
    {
        const string accessKeyId = "presign-maxexpiry-access";
        const string secretAccessKey = "presign-maxexpiry-secret";
        const int maxExpiry = 1800;

        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(builder => {
            builder.Services.Configure<IntegratedS3Options>(options => {
                options.EnableAwsSignatureV4Authentication = true;
                options.MaximumPresignedUrlExpirySeconds = maxExpiry;
                options.PresignAccessKeyId = accessKeyId;
                options.AccessKeyCredentials =
                [
                    new IntegratedS3AccessKeyCredential
                    {
                        AccessKeyId = accessKeyId,
                        SecretAccessKey = secretAccessKey,
                        DisplayName = "presign-maxexpiry-user",
                        Scopes = ["storage.read", "storage.write"]
                    }
                ];
            });
            builder.Services.AddAuthentication("TestHeader")
                .AddScheme<AuthenticationSchemeOptions, TestHeaderAuthenticationHandler>("TestHeader", static _ => { });
            builder.Services.AddSingleton<IIntegratedS3AuthorizationService, ScopeBasedIntegratedS3AuthorizationService>();
        });

        using var client = isolatedClient.Client;
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "storage.read");

        var integratedClient = new IntegratedS3Client(client);

        var exception = await Assert.ThrowsAsync<HttpRequestException>(
            () => integratedClient.PresignGetObjectAsync("bucket", "key", expiresInSeconds: maxExpiry + 1).AsTask());

        Assert.Equal(HttpStatusCode.BadRequest, exception.StatusCode);
    }


    [Fact]
    public async Task SigV4HeaderAuthentication_RootGetReturnsS3CompatibleBucketListXml()
    {
        const string accessKeyId = "sigv4-list-access";
        const string secretAccessKey = "sigv4-list-secret";

        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(builder => {
            builder.Services.Configure<IntegratedS3Options>(options => {
                options.EnableAwsSignatureV4Authentication = true;
                options.AccessKeyCredentials =
                [
                    new IntegratedS3AccessKeyCredential
                    {
                        AccessKeyId = accessKeyId,
                        SecretAccessKey = secretAccessKey,
                        DisplayName = "sigv4-list-user",
                        Scopes = ["storage.read", "storage.write"]
                    }
                ];
            });
            builder.Services.AddSingleton<IIntegratedS3AuthorizationService, ScopeBasedIntegratedS3AuthorizationService>();
        });

        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, "/integrated-s3/buckets/root-list-bucket", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var listBucketsRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Get, "/integrated-s3/", accessKeyId, secretAccessKey);
        var listBucketsResponse = await client.SendAsync(listBucketsRequest);

        Assert.Equal(HttpStatusCode.OK, listBucketsResponse.StatusCode);
        Assert.Equal("application/xml", listBucketsResponse.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await listBucketsResponse.Content.ReadAsStringAsync());
        Assert.Equal("ListAllMyBucketsResult", document.Root?.Name.LocalName);
        Assert.Equal("Integrated S3 Sample Host", document.Root?.Element("Owner")?.Element("DisplayName")?.Value);

        var bucket = Assert.Single(document.Root!.Element("Buckets")!.Elements("Bucket"));
        Assert.Equal("root-list-bucket", bucket.Element("Name")?.Value);
        Assert.False(string.IsNullOrWhiteSpace(bucket.Element("CreationDate")?.Value));
    }

    [Fact]
    public async Task SigV4Authentication_InvalidSignature_ReturnsXmlError()
    {
        const string accessKeyId = "bad-signature-access";
        const string secretAccessKey = "bad-signature-secret";

        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(builder => {
            builder.Services.Configure<IntegratedS3Options>(options => {
                options.EnableAwsSignatureV4Authentication = true;
                options.AccessKeyCredentials =
                [
                    new IntegratedS3AccessKeyCredential
                    {
                        AccessKeyId = accessKeyId,
                        SecretAccessKey = secretAccessKey,
                        Scopes = ["storage.write"]
                    }
                ];
            });
            builder.Services.AddSingleton<IIntegratedS3AuthorizationService, ScopeBasedIntegratedS3AuthorizationService>();
        });

        using var client = isolatedClient.Client;

        using var request = CreateSigV4HeaderSignedRequest(HttpMethod.Put, "/integrated-s3/buckets/invalid-signature-bucket", accessKeyId, secretAccessKey);
        request.Headers.Remove("Authorization");
        request.Headers.TryAddWithoutValidation("Authorization", CreateCorruptedAuthorizationHeader(HttpMethod.Put, "/integrated-s3/buckets/invalid-signature-bucket", accessKeyId, secretAccessKey));

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("SignatureDoesNotMatch", GetRequiredElementValue(errorDocument, "Code"));
    }

    [Fact]
    public async Task SigV4Authentication_InvalidSignature_EmitsCorrelationHeaderLogsMetricsAndTrace()
    {
        const string accessKeyId = "telemetry-access";
        const string secretAccessKey = "telemetry-secret";
        using var observability = new TestObservabilityCollector();

        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(builder => {
            builder.Logging.ClearProviders();
            builder.Logging.SetMinimumLevel(LogLevel.Debug);
            builder.Logging.AddProvider(observability);
            builder.Services.Configure<IntegratedS3Options>(options => {
                options.EnableAwsSignatureV4Authentication = true;
                options.AccessKeyCredentials =
                [
                    new IntegratedS3AccessKeyCredential
                    {
                        AccessKeyId = accessKeyId,
                        SecretAccessKey = secretAccessKey,
                        Scopes = ["storage.write"]
                    }
                ];
            });
            builder.Services.AddSingleton<IIntegratedS3AuthorizationService, ScopeBasedIntegratedS3AuthorizationService>();
        });

        using var client = isolatedClient.Client;
        using var request = CreateSigV4HeaderSignedRequest(HttpMethod.Put, "/integrated-s3/buckets/telemetry-invalid-signature", accessKeyId, secretAccessKey);
        request.Headers.Remove("Authorization");
        request.Headers.TryAddWithoutValidation("Authorization", CreateCorruptedAuthorizationHeader(HttpMethod.Put, "/integrated-s3/buckets/telemetry-invalid-signature", accessKeyId, secretAccessKey));
        request.Headers.TryAddWithoutValidation(IntegratedS3Observability.CorrelationIdHeaderName, "http-correlation-001");

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Equal("http-correlation-001", Assert.Single(response.Headers.GetValues(IntegratedS3Observability.CorrelationIdHeaderName)));

        Assert.Contains(observability.Measurements, measurement =>
            string.Equals(measurement.InstrumentName, IntegratedS3Observability.Metrics.HttpAuthenticationFailures, StringComparison.Ordinal)
            && measurement.Tags.TryGetValue(IntegratedS3Observability.Tags.AuthType, out var authType)
            && string.Equals(authType, "sigv4-header", StringComparison.Ordinal)
            && measurement.Tags.TryGetValue(IntegratedS3Observability.Tags.ErrorCode, out var errorCode)
            && string.Equals(errorCode, "SignatureDoesNotMatch", StringComparison.Ordinal));

        Assert.Contains(observability.Activities, activity =>
            string.Equals(activity.OperationName, "IntegratedS3.Authenticate", StringComparison.Ordinal)
            && string.Equals(activity.Tags[IntegratedS3Observability.Tags.CorrelationId], "http-correlation-001", StringComparison.Ordinal)
            && string.Equals(activity.Tags[IntegratedS3Observability.Tags.ErrorCode], "SignatureDoesNotMatch", StringComparison.Ordinal));

        Assert.Contains(observability.Logs, entry =>
            entry.Level == LogLevel.Warning
            && entry.CategoryName.EndsWith("AwsSignatureV4RequestAuthenticator", StringComparison.Ordinal)
            && string.Equals(entry.State["CorrelationId"], "http-correlation-001", StringComparison.Ordinal)
            && entry.Message.Contains("authentication failed", StringComparison.OrdinalIgnoreCase));
    }

    [Theory]
    [InlineData("ownershipControls")]
    [InlineData("versioning&list-type=2")]
    [InlineData("cors&versioning")]
    public async Task S3CompatibleBucketRoute_UnsupportedSubresource_ReturnsNotImplemented(string query)
    {
        using var client = await _factory.CreateClientAsync();

        await client.PutAsync("/integrated-s3/buckets/subresource-bucket", content: null);

        var response = await client.GetAsync($"/integrated-s3/subresource-bucket?{query}");

        await AssertNotImplementedResponseAsync(response);
    }

    [Theory]
    [InlineData("DELETE", "acl")]
    [InlineData("DELETE", "versioning")]
    [InlineData("POST", "versioning")]
    [InlineData("PUT", "cors&versioning")]
    [InlineData("POST", "delete&versioning")]
    public async Task S3CompatibleBucketRoute_UnsupportedMethodSubresourceMatrix_ReturnsNotImplemented(string method, string query)
    {
        using var client = await _factory.CreateClientAsync();
        var bucketName = $"bucket-subresource-matrix-{Guid.NewGuid():N}";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var request = new HttpRequestMessage(new HttpMethod(method), $"/integrated-s3/{bucketName}?{query}");
        var response = await client.SendAsync(request);

        await AssertNotImplementedResponseAsync(response);
    }

    [Fact]
    public async Task S3CompatibleListMultipartUploads_WithEncodingType_ReturnsEncodedKeys()
    {
        using var client = await _factory.CreateClientAsync();

        var bucketName = $"multipart-subresource-bucket-{Guid.NewGuid():N}";
        const string objectKey = "docs/test file(3).txt";

        static string EncodeObjectPath(string key)
        {
            return string.Join('/', key.Split('/').Select(Uri.EscapeDataString));
        }

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using (var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{EncodeObjectPath(objectKey)}?uploads")) {
            initiateRequest.Headers.TryAddWithoutValidation("Content-Type", "text/plain");
            Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(initiateRequest)).StatusCode);
        }

        var response = await client.GetAsync($"/integrated-s3/{bucketName}?uploads&prefix={Uri.EscapeDataString("docs/")}&encoding-type=url");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("url", GetRequiredElementValue(document, "EncodingType"));
        Assert.Equal("docs%2F", GetRequiredElementValue(document, "Prefix"));
        var upload = Assert.Single(document.Root!.Elements("Upload"));
        Assert.Equal("docs%2Ftest%20file%283%29.txt", upload.Element("Key")?.Value);
        Assert.False(string.IsNullOrWhiteSpace(upload.Element("Owner")?.Element("ID")?.Value));
        Assert.False(string.IsNullOrWhiteSpace(upload.Element("Initiator")?.Element("ID")?.Value));
    }

    [Fact]
    public async Task S3CompatibleListMultipartUploads_UnsupportedEncodingType_ReturnsInvalidArgument()
    {
        using var client = await _factory.CreateClientAsync();

        var bucketName = $"multipart-subresource-invalid-encoding-{Guid.NewGuid():N}";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        var response = await client.GetAsync($"/integrated-s3/{bucketName}?uploads&encoding-type=base64");

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("InvalidArgument", GetRequiredElementValue(errorDocument, "Code"));
        Assert.Contains("encoding-type", GetRequiredElementValue(errorDocument, "Message"), StringComparison.Ordinal);
    }

    [Fact]
    public async Task S3CompatibleListMultipartUploads_MaxUploadsAboveLimit_ReturnsInvalidArgument()
    {
        using var client = await _factory.CreateClientAsync();

        await client.PutAsync("/integrated-s3/buckets/multipart-max-uploads-bucket", content: null);

        var response = await client.GetAsync("/integrated-s3/multipart-max-uploads-bucket?uploads&max-uploads=1001");

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("InvalidArgument", GetRequiredElementValue(errorDocument, "Code"));
        Assert.Contains("max-uploads", GetRequiredElementValue(errorDocument, "Message"), StringComparison.Ordinal);
    }

    [Fact]
    public async Task S3CompatibleListMultipartUploads_NonIntegerMaxUploads_ReturnsInvalidArgument()
    {
        using var client = await _factory.CreateClientAsync();

        await client.PutAsync("/integrated-s3/buckets/multipart-max-uploads-parse-bucket", content: null);

        var response = await client.GetAsync("/integrated-s3/multipart-max-uploads-parse-bucket?uploads&max-uploads=abc");

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("InvalidArgument", GetRequiredElementValue(errorDocument, "Code"));
        Assert.Contains("max-uploads", GetRequiredElementValue(errorDocument, "Message"), StringComparison.Ordinal);
    }

    [Fact]
    public async Task S3CompatibleGetObjectRetention_ReturnsXmlAndVersionHeader()
    {
        var retainUntilDateUtc = DateTimeOffset.Parse("2034-05-06T07:08:09Z", CultureInfo.InvariantCulture);
        var storageService = new RecordingStorageService
        {
            GetObjectRetentionResult = new ObjectRetentionInfo
            {
                BucketName = "lock-bucket",
                Key = "docs/locked.txt",
                VersionId = "version-123",
                Mode = ObjectRetentionMode.Governance,
                RetainUntilDateUtc = retainUntilDateUtc
            }
        };
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        var response = await client.GetAsync("/integrated-s3/lock-bucket/docs/locked.txt?retention&versionId=version-123");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("version-123", Assert.Single(response.Headers.GetValues("x-amz-version-id")));

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("Retention", document.Root?.Name.LocalName);
        Assert.Equal("GOVERNANCE", GetRequiredElementValue(document, "Mode"));
        Assert.Equal(retainUntilDateUtc, DateTimeOffset.Parse(GetRequiredElementValue(document, "RetainUntilDate"), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal));

        Assert.NotNull(storageService.LastGetObjectRetentionRequest);
        Assert.Equal("version-123", storageService.LastGetObjectRetentionRequest!.VersionId);
        Assert.Null(storageService.LastGetObjectLegalHoldRequest);
    }

    [Fact]
    public async Task S3CompatibleGetObjectLegalHold_ReturnsXmlAndVersionHeader()
    {
        var storageService = new RecordingStorageService
        {
            GetObjectLegalHoldResult = new ObjectLegalHoldInfo
            {
                BucketName = "lock-bucket",
                Key = "docs/locked.txt",
                VersionId = "version-456",
                Status = ObjectLegalHoldStatus.On
            }
        };
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        var response = await client.GetAsync("/integrated-s3/lock-bucket/docs/locked.txt?legal-hold&versionId=version-456");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("version-456", Assert.Single(response.Headers.GetValues("x-amz-version-id")));

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("LegalHold", document.Root?.Name.LocalName);
        Assert.Equal("ON", GetRequiredElementValue(document, "Status"));

        Assert.NotNull(storageService.LastGetObjectLegalHoldRequest);
        Assert.Equal("version-456", storageService.LastGetObjectLegalHoldRequest!.VersionId);
        Assert.Null(storageService.LastGetObjectRetentionRequest);
    }

    [Fact]
    public async Task S3CompatibleGetObjectRetention_WithUnsupportedDiskProvider_ReturnsNotImplemented()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/object-lock-disk-bucket", content: null)).StatusCode);
        Assert.Equal(
            HttpStatusCode.OK,
            (await client.PutAsync(
                "/integrated-s3/buckets/object-lock-disk-bucket/objects/docs/value.txt",
                new StringContent("hello object lock", Encoding.UTF8, "text/plain"))).StatusCode);

        await AssertNotImplementedResponseAsync(await client.GetAsync("/integrated-s3/object-lock-disk-bucket/docs/value.txt?retention"));
    }

    [Fact]
    public async Task S3CompatibleGetObjectLegalHold_WithUnsupportedDiskProvider_ReturnsNotImplemented()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/object-legal-hold-disk-bucket", content: null)).StatusCode);
        Assert.Equal(
            HttpStatusCode.OK,
            (await client.PutAsync(
                "/integrated-s3/buckets/object-legal-hold-disk-bucket/objects/docs/value.txt",
                new StringContent("hello legal hold", Encoding.UTF8, "text/plain"))).StatusCode);

        await AssertNotImplementedResponseAsync(await client.GetAsync("/integrated-s3/object-legal-hold-disk-bucket/docs/value.txt?legal-hold"));
    }

    [Theory]
    [InlineData("PUT", "retention")]
    [InlineData("PUT", "legal-hold")]
    [InlineData("DELETE", "retention")]
    [InlineData("POST", "versionId=historical-version")]
    public async Task S3CompatibleObjectRoute_UnsupportedSingleSubresource_ReturnsNotImplemented(string method, string query)
    {
        using var client = await _factory.CreateClientAsync();
        var bucketName = $"object-subresource-matrix-{Guid.NewGuid():N}";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);
        Assert.Equal(
            HttpStatusCode.OK,
            (await client.PutAsync(
                $"/integrated-s3/buckets/{bucketName}/objects/docs/value.txt",
                new StringContent("hello subresource matrix", Encoding.UTF8, "text/plain"))).StatusCode);

        using var request = new HttpRequestMessage(new HttpMethod(method), $"/integrated-s3/{bucketName}/docs/value.txt?{query}");
        var response = await client.SendAsync(request);

        await AssertNotImplementedResponseAsync(response);
    }

    [Theory]
    [InlineData("GET", "acl&versionId=historical-version")]
    [InlineData("GET", "tagging&uploadId=upload-123")]
    [InlineData("GET", "versionId=historical-version&uploadId=upload-123")]
    [InlineData("GET", "uploadId=upload-123&partNumber=1")]
    [InlineData("PUT", "tagging&partNumber=1&uploadId=upload-123")]
    [InlineData("DELETE", "tagging&uploadId=upload-123")]
    [InlineData("POST", "uploads&versionId=historical-version")]
    public async Task S3CompatibleObjectRoute_InvalidSubresourceCombination_ReturnsNotImplemented(string method, string query)
    {
        using var client = await _factory.CreateClientAsync();
        var bucketName = $"object-subresource-combo-{Guid.NewGuid():N}";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);
        Assert.Equal(
            HttpStatusCode.OK,
            (await client.PutAsync(
                $"/integrated-s3/buckets/{bucketName}/objects/docs/value.txt",
                new StringContent("hello subresource combinations", Encoding.UTF8, "text/plain"))).StatusCode);

        using var request = new HttpRequestMessage(new HttpMethod(method), $"/integrated-s3/{bucketName}/docs/value.txt?{query}");
        var response = await client.SendAsync(request);

        await AssertNotImplementedResponseAsync(response);
    }

    [Fact]
    public async Task S3CompatibleBucketSubresourceValidation_IgnoresSigV4PresignQueryParameters()
    {
        using var client = await _factory.CreateClientAsync();
        var bucketName = $"bucket-presign-subresource-{Guid.NewGuid():N}";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        var versioningResponse = await client.GetAsync($"/integrated-s3/{bucketName}?versioning&x-id=GetBucketVersioning&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20260311T180000Z");
        Assert.Equal(HttpStatusCode.OK, versioningResponse.StatusCode);
        var versioningDocument = XDocument.Parse(await versioningResponse.Content.ReadAsStringAsync());
        Assert.Equal("VersioningConfiguration", versioningDocument.Root?.Name.LocalName);

        var uploadsResponse = await client.GetAsync($"/integrated-s3/{bucketName}?uploads&x-id=ListMultipartUploads&X-Amz-Expires=300");
        Assert.Equal(HttpStatusCode.OK, uploadsResponse.StatusCode);
        var uploadsDocument = XDocument.Parse(await uploadsResponse.Content.ReadAsStringAsync());
        Assert.Equal("ListMultipartUploadsResult", uploadsDocument.Root?.Name.LocalName);
    }

    [Fact]
    public async Task S3CompatibleObjectSubresourceValidation_IgnoresSigV4PresignQueryParameters()
    {
        using var client = await _factory.CreateClientAsync();
        var bucketName = $"object-presign-subresource-{Guid.NewGuid():N}";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);
        await EnableBucketVersioningAsync(client, bucketName);

        var putObjectResponse = await client.PutAsync(
            $"/integrated-s3/buckets/{bucketName}/objects/docs/versioned.txt",
            new StringContent("hello presign validation", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, putObjectResponse.StatusCode);
        var versionId = Assert.Single(putObjectResponse.Headers.GetValues("x-amz-version-id"));

        using var putTaggingRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/docs/versioned.txt?tagging&versionId={Uri.EscapeDataString(versionId)}")
        {
            Content = new StringContent("""
<Tagging>
  <TagSet>
    <Tag><Key>mode</Key><Value>presign</Value></Tag>
  </TagSet>
</Tagging>
""", Encoding.UTF8, "application/xml")
        };
        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(putTaggingRequest)).StatusCode);

        var getVersionResponse = await client.GetAsync($"/integrated-s3/{bucketName}/docs/versioned.txt?versionId={Uri.EscapeDataString(versionId)}&x-id=GetObject&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20260311T180000Z");
        Assert.Equal(HttpStatusCode.OK, getVersionResponse.StatusCode);
        Assert.Equal(versionId, Assert.Single(getVersionResponse.Headers.GetValues("x-amz-version-id")));

        var getTaggingResponse = await client.GetAsync($"/integrated-s3/{bucketName}/docs/versioned.txt?tagging&versionId={Uri.EscapeDataString(versionId)}&x-id=GetObjectTagging&X-Amz-Expires=300");
        Assert.Equal(HttpStatusCode.OK, getTaggingResponse.StatusCode);
        var taggingDocument = XDocument.Parse(await getTaggingResponse.Content.ReadAsStringAsync());
        Assert.Equal("presign", taggingDocument.Root!.Element("TagSet")!.Element("Tag")!.Element("Value")!.Value);
    }

    [Fact]
    public async Task S3CompatibleMultipartUpload_ListParts_ReturnsPaginatedXmlWithChecksums()
    {
        using var client = await _factory.CreateClientAsync();

        var bucketName = $"multipart-list-parts-{Guid.NewGuid():N}";
        const string objectKey = "docs/list-parts.txt";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads")
        {
            Content = new ByteArrayContent([])
        };
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-sdk-checksum-algorithm", "SHA256");
        initiateRequest.Content.Headers.ContentType = new MediaTypeHeaderValue("text/plain");

        var initiateResponse = await client.SendAsync(initiateRequest);
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        async Task<(string ETag, string Checksum)> UploadPartAsync(int partNumber, string payload)
        {
            var checksum = ComputeSha256Base64(payload);
            using var request = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/{objectKey}?partNumber={partNumber}&uploadId={Uri.EscapeDataString(uploadId)}")
            {
                Content = new StringContent(payload, Encoding.UTF8, "text/plain")
            };
            request.Headers.TryAddWithoutValidation("x-amz-sdk-checksum-algorithm", "SHA256");
            request.Headers.TryAddWithoutValidation("x-amz-checksum-sha256", checksum);

            var response = await client.SendAsync(request);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal(checksum, Assert.Single(response.Headers.GetValues("x-amz-checksum-sha256")));
            return (response.Headers.ETag?.Tag ?? throw new Xunit.Sdk.XunitException("Expected multipart part ETag header."), checksum);
        }

        var part1 = await UploadPartAsync(1, "hello ");
        var part2 = await UploadPartAsync(2, "world");
        var part3 = await UploadPartAsync(3, "!");

        var firstPageResponse = await client.GetAsync($"/integrated-s3/{bucketName}/{objectKey}?uploadId={Uri.EscapeDataString(uploadId)}&max-parts=2");
        Assert.Equal(HttpStatusCode.OK, firstPageResponse.StatusCode);
        Assert.Equal("application/xml", firstPageResponse.Content.Headers.ContentType?.MediaType);

        var firstPageDocument = XDocument.Parse(await firstPageResponse.Content.ReadAsStringAsync());
        Assert.Equal("ListPartsResult", firstPageDocument.Root?.Name.LocalName);
        Assert.Equal(bucketName, GetRequiredElementValue(firstPageDocument, "Bucket"));
        Assert.Equal(objectKey, GetRequiredElementValue(firstPageDocument, "Key"));
        Assert.Equal(uploadId, GetRequiredElementValue(firstPageDocument, "UploadId"));
        Assert.Equal("STANDARD", GetRequiredElementValue(firstPageDocument, "StorageClass"));
        Assert.Equal("SHA256", GetRequiredElementValue(firstPageDocument, "ChecksumAlgorithm"));
        Assert.Equal("COMPOSITE", GetRequiredElementValue(firstPageDocument, "ChecksumType"));
        Assert.Equal("0", GetRequiredElementValue(firstPageDocument, "PartNumberMarker"));
        Assert.Equal("2", GetRequiredElementValue(firstPageDocument, "NextPartNumberMarker"));
        Assert.Equal("2", GetRequiredElementValue(firstPageDocument, "MaxParts"));
        Assert.Equal("true", GetRequiredElementValue(firstPageDocument, "IsTruncated"));
        Assert.NotNull(firstPageDocument.Root!.Element("Owner"));
        Assert.NotNull(firstPageDocument.Root!.Element("Initiator"));

        var firstPageParts = firstPageDocument.Root.Elements("Part").ToArray();
        Assert.Equal(2, firstPageParts.Length);
        Assert.Equal(
            ["PartNumber", "LastModified", "ETag", "Size", "ChecksumSHA256"],
            firstPageParts[0].Elements().Select(static element => element.Name.LocalName).ToArray());
        Assert.Equal("1", firstPageParts[0].Element("PartNumber")?.Value);
        Assert.Equal(part1.ETag, firstPageParts[0].Element("ETag")?.Value);
        Assert.Equal(part1.Checksum, firstPageParts[0].Element("ChecksumSHA256")?.Value);
        Assert.Equal("2", firstPageParts[1].Element("PartNumber")?.Value);
        Assert.Equal(part2.ETag, firstPageParts[1].Element("ETag")?.Value);
        Assert.Equal(part2.Checksum, firstPageParts[1].Element("ChecksumSHA256")?.Value);

        var secondPageResponse = await client.GetAsync($"/integrated-s3/{bucketName}/{objectKey}?uploadId={Uri.EscapeDataString(uploadId)}&part-number-marker=2");
        Assert.Equal(HttpStatusCode.OK, secondPageResponse.StatusCode);

        var secondPageDocument = XDocument.Parse(await secondPageResponse.Content.ReadAsStringAsync());
        Assert.Equal("false", GetRequiredElementValue(secondPageDocument, "IsTruncated"));
        var remainingPart = Assert.Single(secondPageDocument.Root!.Elements("Part"));
        Assert.Equal("3", remainingPart.Element("PartNumber")?.Value);
        Assert.Equal(part3.ETag, remainingPart.Element("ETag")?.Value);
        Assert.Equal(part3.Checksum, remainingPart.Element("ChecksumSHA256")?.Value);
    }

    [Fact]
    public async Task S3CompatibleMultipartUpload_ListParts_WithInvalidPartNumberMarker_ReturnsBadRequest()
    {
        using var client = await _factory.CreateClientAsync();

        var bucketName = $"multipart-list-parts-invalid-marker-{Guid.NewGuid():N}";
        const string objectKey = "docs/list-parts.txt";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);
        var initiateResponse = await client.SendAsync(new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads"));
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        var response = await client.GetAsync($"/integrated-s3/{bucketName}/{objectKey}?uploadId={Uri.EscapeDataString(uploadId)}&part-number-marker=abc");

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("InvalidArgument", GetRequiredElementValue(errorDocument, "Code"));
        Assert.Contains("part-number-marker", GetRequiredElementValue(errorDocument, "Message"), StringComparison.Ordinal);
    }

    [Fact]
    public async Task S3CompatibleMultipartUpload_ListParts_WithInvalidMaxParts_ReturnsBadRequest()
    {
        using var client = await _factory.CreateClientAsync();

        var bucketName = $"multipart-list-parts-invalid-max-{Guid.NewGuid():N}";
        const string objectKey = "docs/list-parts.txt";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);
        var initiateResponse = await client.SendAsync(new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads"));
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        var response = await client.GetAsync($"/integrated-s3/{bucketName}/{objectKey}?uploadId={Uri.EscapeDataString(uploadId)}&max-parts=1001");

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("InvalidArgument", GetRequiredElementValue(errorDocument, "Code"));
        Assert.Contains("max-parts", GetRequiredElementValue(errorDocument, "Message"), StringComparison.Ordinal);
    }

    [Fact]
    public async Task S3CompatibleMultipartUpload_UploadPartCopy_CopiesHistoricalRangeAndReturnsXml()
    {
        using var client = await _factory.CreateClientAsync();

        var bucketName = $"multipart-copy-http-{Guid.NewGuid():N}";
        const string sourceKey = "docs/source.txt";
        const string destinationKey = "docs/copied.txt";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);
        await EnableBucketVersioningAsync(client, bucketName);

        var v1Response = await client.PutAsync(
            $"/integrated-s3/{bucketName}/{sourceKey}",
            new StringContent("hello world", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, v1Response.StatusCode);
        var v1VersionId = Assert.Single(v1Response.Headers.GetValues("x-amz-version-id"));
        var v1ETag = v1Response.Headers.ETag?.Tag ?? throw new Xunit.Sdk.XunitException("Expected source object ETag.");

        var v2Response = await client.PutAsync(
            $"/integrated-s3/{bucketName}/{sourceKey}",
            new StringContent("goodbye world", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, v2Response.StatusCode);
        Assert.NotEqual(v1VersionId, Assert.Single(v2Response.Headers.GetValues("x-amz-version-id")));

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{destinationKey}?uploads")
        {
            Content = new ByteArrayContent([])
        };
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-sdk-checksum-algorithm", "SHA256");
        initiateRequest.Content.Headers.ContentType = new MediaTypeHeaderValue("text/plain");

        var initiateResponse = await client.SendAsync(initiateRequest);
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var copyPartRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/{destinationKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}");
        copyPartRequest.Headers.TryAddWithoutValidation("x-amz-copy-source", $"/{bucketName}/{sourceKey}?versionId={Uri.EscapeDataString(v1VersionId)}");
        copyPartRequest.Headers.TryAddWithoutValidation("x-amz-copy-source-if-match", v1ETag);
        copyPartRequest.Headers.TryAddWithoutValidation("x-amz-copy-source-range", "bytes=6-10");

        var copyPartResponse = await client.SendAsync(copyPartRequest);
        Assert.Equal(HttpStatusCode.OK, copyPartResponse.StatusCode);
        Assert.Equal("application/xml", copyPartResponse.Content.Headers.ContentType?.MediaType);
        Assert.Equal(v1VersionId, Assert.Single(copyPartResponse.Headers.GetValues("x-amz-copy-source-version-id")));
        Assert.Equal(ComputeSha256Base64("world"), Assert.Single(copyPartResponse.Headers.GetValues("x-amz-checksum-sha256")));

        var copiedPartETag = copyPartResponse.Headers.ETag?.Tag ?? throw new Xunit.Sdk.XunitException("Expected copied part ETag header.");
        var copyPartDocument = XDocument.Parse(await copyPartResponse.Content.ReadAsStringAsync());
        Assert.Equal("CopyPartResult", copyPartDocument.Root?.Name.LocalName);
        Assert.Equal(copiedPartETag, GetRequiredElementValue(copyPartDocument, "ETag"));
        Assert.Equal(ComputeSha256Base64("world"), GetRequiredElementValue(copyPartDocument, "ChecksumSHA256"));
        Assert.False(string.IsNullOrWhiteSpace(GetRequiredElementValue(copyPartDocument, "LastModified")));

        using var completeRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{destinationKey}?uploadId={Uri.EscapeDataString(uploadId)}")
        {
            Content = new StringContent($"""
<CompleteMultipartUpload>
  <Part>
    <PartNumber>1</PartNumber>
    <ETag>{copiedPartETag}</ETag>
    <ChecksumSHA256>{ComputeSha256Base64("world")}</ChecksumSHA256>
  </Part>
</CompleteMultipartUpload>
""", Encoding.UTF8, "application/xml")
        };

        var completeResponse = await client.SendAsync(completeRequest);
        Assert.Equal(HttpStatusCode.OK, completeResponse.StatusCode);

        var getResponse = await client.GetAsync($"/integrated-s3/{bucketName}/{destinationKey}");
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        Assert.Equal("world", await getResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task S3CompatibleMultipartUpload_UploadPartCopy_WithMissingSourceVersion_ReturnsNoSuchVersion()
    {
        using var client = await _factory.CreateClientAsync();

        var bucketName = $"multipart-copy-http-missing-version-{Guid.NewGuid():N}";
        const string sourceKey = "docs/source.txt";
        const string destinationKey = "docs/copied.txt";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);
        await EnableBucketVersioningAsync(client, bucketName);

        var putSource = await client.PutAsync(
            $"/integrated-s3/{bucketName}/{sourceKey}",
            new StringContent("hello world", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, putSource.StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{destinationKey}?uploads");
        var initiateResponse = await client.SendAsync(initiateRequest);
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        var uploadId = GetRequiredElementValue(XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync()), "UploadId");

        using var copyPartRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/{destinationKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}");
        copyPartRequest.Headers.TryAddWithoutValidation("x-amz-copy-source", $"/{bucketName}/{sourceKey}?versionId={Uri.EscapeDataString("missing-version")}");

        var response = await client.SendAsync(copyPartRequest);
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);

        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("NoSuchVersion", GetRequiredElementValue(errorDocument, "Code"));
        Assert.Equal("The specified version does not exist.", GetRequiredElementValue(errorDocument, "Message"));
    }

    [Theory]
    [InlineData("x-amz-sdk-checksum-algorithm")]
    [InlineData("x-amz-checksum-algorithm")]
    public async Task S3CompatibleMultipartUpload_UploadPartCopy_WithRequestedChecksumAlgorithm_ReturnsRequestedChecksum(string algorithmHeaderName)
    {
        using var client = await _factory.CreateClientAsync();
        var bucketName = algorithmHeaderName.Contains("sdk", StringComparison.Ordinal)
            ? $"multipart-copy-sdk-requested-checksum-{Guid.NewGuid():N}"
            : $"multipart-copy-requested-checksum-{Guid.NewGuid():N}";
        const string sourceKey = "docs/source.txt";
        const string destinationKey = "docs/copied.txt";
        const string sourcePayload = "hello world";
        const string copiedPayload = "world";
        var expectedChecksumSha1 = ComputeSha1Base64(copiedPayload);

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        var putSource = await client.PutAsync(
            $"/integrated-s3/{bucketName}/{sourceKey}",
            new StringContent(sourcePayload, Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, putSource.StatusCode);
        var sourceETag = putSource.Headers.ETag?.Tag ?? throw new Xunit.Sdk.XunitException("Expected source object ETag.");

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{destinationKey}?uploads");
        var initiateResponse = await client.SendAsync(initiateRequest);
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        var uploadId = GetRequiredElementValue(XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync()), "UploadId");

        using var copyPartRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/{destinationKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}");
        copyPartRequest.Headers.TryAddWithoutValidation("x-amz-copy-source", $"/{bucketName}/{sourceKey}");
        copyPartRequest.Headers.TryAddWithoutValidation("x-amz-copy-source-if-match", sourceETag);
        copyPartRequest.Headers.TryAddWithoutValidation("x-amz-copy-source-range", "bytes=6-10");
        copyPartRequest.Headers.TryAddWithoutValidation(algorithmHeaderName, "SHA1");

        var copyPartResponse = await client.SendAsync(copyPartRequest);

        Assert.Equal(HttpStatusCode.OK, copyPartResponse.StatusCode);
        Assert.Equal("application/xml", copyPartResponse.Content.Headers.ContentType?.MediaType);
        Assert.Equal(expectedChecksumSha1, Assert.Single(copyPartResponse.Headers.GetValues("x-amz-checksum-sha1")));
        Assert.False(copyPartResponse.Headers.Contains("x-amz-checksum-sha256"));

        var copyPartDocument = XDocument.Parse(await copyPartResponse.Content.ReadAsStringAsync());
        Assert.Equal("CopyPartResult", copyPartDocument.Root?.Name.LocalName);
        Assert.Equal(expectedChecksumSha1, GetRequiredElementValue(copyPartDocument, "ChecksumSHA1"));
        Assert.DoesNotContain(copyPartDocument.Root!.Elements(), static element => string.Equals(element.Name.LocalName, "ChecksumSHA256", StringComparison.Ordinal));
    }

    [Fact]
    public async Task S3CompatibleMultipartUpload_UploadPartCopy_WithMismatchedChecksumAlgorithmHeaders_ReturnsInvalidRequest()
    {
        using var client = await _factory.CreateClientAsync();

        var bucketName = $"multipart-copy-checksum-mismatch-{Guid.NewGuid():N}";
        const string sourceKey = "docs/source.txt";
        const string destinationKey = "docs/copied.txt";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        var putSource = await client.PutAsync(
            $"/integrated-s3/{bucketName}/{sourceKey}",
            new StringContent("copy part checksum mismatch", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, putSource.StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{destinationKey}?uploads");
        var initiateResponse = await client.SendAsync(initiateRequest);
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        var uploadId = GetRequiredElementValue(XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync()), "UploadId");

        using var copyPartRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/{destinationKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}");
        copyPartRequest.Headers.TryAddWithoutValidation("x-amz-copy-source", $"/{bucketName}/{sourceKey}");
        copyPartRequest.Headers.TryAddWithoutValidation("x-amz-sdk-checksum-algorithm", "CRC32C");
        copyPartRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "SHA256");

        var response = await client.SendAsync(copyPartRequest);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("InvalidRequest", GetRequiredElementValue(document, "Code"));
        Assert.Equal(
            "The 'x-amz-sdk-checksum-algorithm' and 'x-amz-checksum-algorithm' headers must match when both are supplied.",
            GetRequiredElementValue(document, "Message"));
    }

    [Fact]
    public async Task S3CompatibleMultipartUpload_RoundTripsXmlWorkflow()
    {
        using var client = await _factory.CreateClientAsync();
        var expiresUtc = new DateTimeOffset(2026, 3, 14, 17, 0, 0, TimeSpan.Zero);
        const string completedPayload = "hello world";
        var expectedChecksumSha256 = ComputeSha256Base64(completedPayload);
        var expectedChecksumCrc32c = ChecksumTestAlgorithms.ComputeCrc32cBase64(completedPayload);

        await client.PutAsync("/integrated-s3/buckets/multipart-bucket", content: null);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, "/integrated-s3/multipart-bucket/docs/multipart.txt?uploads")
        {
            Content = new ByteArrayContent([])
        };
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-meta-origin", "http-test");
        initiateRequest.Headers.TryAddWithoutValidation("Cache-Control", "no-store");
        initiateRequest.Content.Headers.ContentType = new MediaTypeHeaderValue("text/plain");
        initiateRequest.Content.Headers.ContentDisposition = new ContentDispositionHeaderValue("attachment")
        {
            FileName = "multipart.txt"
        };
        initiateRequest.Content.Headers.ContentEncoding.Add("identity");
        initiateRequest.Content.Headers.ContentLanguage.Add("en-US");
        initiateRequest.Content.Headers.Expires = expiresUtc;

        var initiateResponse = await client.SendAsync(initiateRequest);
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        Assert.Equal("application/xml", initiateResponse.Content.Headers.ContentType?.MediaType);

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");
        Assert.Equal("multipart-bucket", GetRequiredElementValue(initiateDocument, "Bucket"));
        Assert.Equal("docs/multipart.txt", GetRequiredElementValue(initiateDocument, "Key"));

        var part1Response = await client.PutAsync(
                $"/integrated-s3/multipart-bucket/docs/multipart.txt?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
                new StringContent("hello ", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, part1Response.StatusCode);
        var part1ETag = part1Response.Headers.ETag?.Tag ?? throw new Xunit.Sdk.XunitException("Expected multipart part ETag header.");

        var part2Response = await client.PutAsync(
                $"/integrated-s3/multipart-bucket/docs/multipart.txt?partNumber=2&uploadId={Uri.EscapeDataString(uploadId)}",
                new StringContent("world", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, part2Response.StatusCode);
        var part2ETag = part2Response.Headers.ETag?.Tag ?? throw new Xunit.Sdk.XunitException("Expected multipart part ETag header.");

        var completeBody = $"""
<CompleteMultipartUpload>
    <Part>
        <PartNumber>1</PartNumber>
        <ETag>{part1ETag}</ETag>
    </Part>
    <Part>
        <PartNumber>2</PartNumber>
        <ETag>{part2ETag}</ETag>
    </Part>
</CompleteMultipartUpload>
""";

        using var completeRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/multipart-bucket/docs/multipart.txt?uploadId={Uri.EscapeDataString(uploadId)}")
        {
                Content = new StringContent(completeBody, Encoding.UTF8, "application/xml")
        };

        var completeResponse = await client.SendAsync(completeRequest);
        Assert.Equal(HttpStatusCode.OK, completeResponse.StatusCode);
        Assert.Equal("application/xml", completeResponse.Content.Headers.ContentType?.MediaType);
        Assert.Equal(expectedChecksumSha256, Assert.Single(completeResponse.Headers.GetValues("x-amz-checksum-sha256")));
        Assert.Equal(expectedChecksumCrc32c, Assert.Single(completeResponse.Headers.GetValues("x-amz-checksum-crc32c")));

        var completeDocument = XDocument.Parse(await completeResponse.Content.ReadAsStringAsync());
        Assert.Equal("CompleteMultipartUploadResult", completeDocument.Root?.Name.LocalName);
        Assert.Equal("multipart-bucket", GetRequiredElementValue(completeDocument, "Bucket"));
        Assert.Equal("docs/multipart.txt", GetRequiredElementValue(completeDocument, "Key"));
        Assert.Equal(expectedChecksumSha256, GetRequiredElementValue(completeDocument, "ChecksumSHA256"));
        Assert.Equal(expectedChecksumCrc32c, GetRequiredElementValue(completeDocument, "ChecksumCRC32C"));

        using var headRequest = new HttpRequestMessage(HttpMethod.Head, "/integrated-s3/multipart-bucket/docs/multipart.txt");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        Assert.Equal(expectedChecksumSha256, Assert.Single(headResponse.Headers.GetValues("x-amz-checksum-sha256")));
        Assert.Equal(expectedChecksumCrc32c, Assert.Single(headResponse.Headers.GetValues("x-amz-checksum-crc32c")));

        var downloadResponse = await client.GetAsync("/integrated-s3/multipart-bucket/docs/multipart.txt");
        Assert.Equal(HttpStatusCode.OK, downloadResponse.StatusCode);
        Assert.Equal("http-test", Assert.Single(downloadResponse.Headers.GetValues("x-amz-meta-origin")));
        Assert.Equal("http-test", Assert.Single(downloadResponse.Headers.GetValues("x-integrateds3-meta-origin")));
        Assert.Equal("no-store", downloadResponse.Headers.CacheControl?.ToString());
        Assert.Equal("attachment", downloadResponse.Content.Headers.ContentDisposition?.DispositionType);
        Assert.Equal("multipart.txt", downloadResponse.Content.Headers.ContentDisposition?.FileName?.Trim('"'));
        Assert.Contains("identity", downloadResponse.Content.Headers.ContentEncoding);
        Assert.Contains("en-US", downloadResponse.Content.Headers.ContentLanguage);
        Assert.Equal(expiresUtc, downloadResponse.Content.Headers.Expires);
        Assert.Equal(expectedChecksumSha256, Assert.Single(downloadResponse.Headers.GetValues("x-amz-checksum-sha256")));
        Assert.Equal(expectedChecksumCrc32c, Assert.Single(downloadResponse.Headers.GetValues("x-amz-checksum-crc32c")));
        Assert.Equal(completedPayload, await downloadResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task S3CompatibleMultipartUpload_WithTaggingHeader_PersistsCompletedObjectTags()
    {
        using var client = await _factory.CreateClientAsync();

        await client.PutAsync("/integrated-s3/buckets/multipart-tagging-bucket", content: null);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, "/integrated-s3/multipart-tagging-bucket/docs/tagged.txt?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("Content-Type", "text/plain");
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-tagging", "environment=test&owner=copilot");

        var initiateResponse = await client.SendAsync(initiateRequest);
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        var partResponse = await client.PutAsync(
            $"/integrated-s3/multipart-tagging-bucket/docs/tagged.txt?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            new StringContent("multipart payload", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, partResponse.StatusCode);
        var partETag = partResponse.Headers.ETag?.Tag ?? throw new Xunit.Sdk.XunitException("Expected multipart part ETag header.");

        using var completeRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/multipart-tagging-bucket/docs/tagged.txt?uploadId={Uri.EscapeDataString(uploadId)}")
        {
            Content = new StringContent($"""
<CompleteMultipartUpload>
  <Part>
    <PartNumber>1</PartNumber>
    <ETag>{partETag}</ETag>
  </Part>
</CompleteMultipartUpload>
""", Encoding.UTF8, "application/xml")
        };

        var completeResponse = await client.SendAsync(completeRequest);
        Assert.Equal(HttpStatusCode.OK, completeResponse.StatusCode);

        var tags = await GetObjectTagsAsync(client, "multipart-tagging-bucket", "docs/tagged.txt");
        Assert.Equal("test", tags["environment"]);
        Assert.Equal("copilot", tags["owner"]);
    }

    [Theory]
    [InlineData("x-amz-sdk-checksum-algorithm")]
    [InlineData("x-amz-checksum-algorithm")]
    public async Task S3CompatibleMultipartUpload_UploadPartWithDeclaredChecksumAlgorithmButMissingChecksumValue_ReturnsInvalidRequest(string algorithmHeaderName)
    {
        using var client = await _factory.CreateClientAsync();

        var bucketName = algorithmHeaderName.Contains("sdk", StringComparison.Ordinal)
            ? "multipart-uploadpart-sdk-checksum-bucket"
            : "multipart-uploadpart-checksum-bucket";
        const string objectKey = "docs/missing-checksum.txt";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("Content-Type", "text/plain");

        var initiateResponse = await client.SendAsync(initiateRequest);
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var partRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}")
        {
            Content = new StringContent("part payload", Encoding.UTF8, "text/plain")
        };
        partRequest.Headers.TryAddWithoutValidation(algorithmHeaderName, "SHA256");

        var response = await client.SendAsync(partRequest);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("InvalidRequest", GetRequiredElementValue(document, "Code"));
        Assert.Equal(
            "The 'x-amz-checksum-sha256' header is required when either 'x-amz-sdk-checksum-algorithm=SHA256' or 'x-amz-checksum-algorithm=SHA256' is supplied.",
            GetRequiredElementValue(document, "Message"));
    }

    [Theory]
    [InlineData("x-amz-sdk-checksum-algorithm")]
    [InlineData("x-amz-checksum-algorithm")]
    public async Task S3CompatibleMultipartUpload_UploadPartWithDeclaredChecksumAlgorithmButMissingChecksumTrailer_ReturnsInvalidRequest(string algorithmHeaderName)
    {
        using var client = await _factory.CreateClientAsync();

        var bucketName = algorithmHeaderName.Contains("sdk", StringComparison.Ordinal)
            ? "multipart-uploadpart-sdk-checksum-trailer-bucket"
            : "multipart-uploadpart-checksum-trailer-bucket";
        const string objectKey = "docs/missing-checksum-trailer.txt";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("Content-Type", "text/plain");

        var initiateResponse = await client.SendAsync(initiateRequest);
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var partRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            "part payload");
        partRequest.Headers.TryAddWithoutValidation(algorithmHeaderName, "SHA256");

        var response = await client.SendAsync(partRequest);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("InvalidRequest", GetRequiredElementValue(document, "Code"));
        Assert.Equal(
            "The 'x-amz-checksum-sha256' header is required when either 'x-amz-sdk-checksum-algorithm=SHA256' or 'x-amz-checksum-algorithm=SHA256' is supplied.",
            GetRequiredElementValue(document, "Message"));
    }

    [Fact]
    public async Task S3CompatibleMultipartUpload_UploadPartWithDeclaredChecksumTrailerButMissingTrailerValue_ReturnsInvalidRequest()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "multipart-uploadpart-declared-trailer-missing-bucket";
        const string objectKey = "docs/missing-declared-trailer-checksum.txt";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("Content-Type", "text/plain");

        var initiateResponse = await client.SendAsync(initiateRequest);
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var partRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            "part payload",
            declaredTrailerHeaderNames: ["x-amz-checksum-sha256"]);

        var response = await client.SendAsync(partRequest);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("InvalidRequest", GetRequiredElementValue(document, "Code"));
        Assert.Equal(
            "The 'x-amz-trailer' header declared 'x-amz-checksum-sha256', but the aws-chunked trailer did not include that checksum header.",
            GetRequiredElementValue(document, "Message"));
    }

    [Fact]
    public async Task S3CompatibleMultipartUpload_UploadPartWithDuplicateAwsChunkedChecksumTrailer_ReturnsInvalidRequest()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "multipart-uploadpart-duplicate-trailer-bucket";
        const string objectKey = "docs/duplicate-trailer-checksum.txt";
        const string payload = "part payload";
        var checksum = ComputeSha256Base64(payload);

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("Content-Type", "text/plain");
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "SHA256");

        var initiateResponse = await client.SendAsync(initiateRequest);
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var partRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            payload,
            sdkChecksumAlgorithm: "SHA256",
            declaredTrailerHeaderNames: ["x-amz-checksum-sha256"],
            trailerHeaderEntries:
            [
                new KeyValuePair<string, string>("x-amz-checksum-sha256", checksum),
                new KeyValuePair<string, string>("x-amz-checksum-sha256", ComputeSha256Base64("different payload"))
            ]);

        var response = await client.SendAsync(partRequest);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("InvalidRequest", GetRequiredElementValue(document, "Code"));
        Assert.Equal(
            "The aws-chunked trailer header 'x-amz-checksum-sha256' must not be repeated.",
            GetRequiredElementValue(document, "Message"));
    }

    [Fact]
    public async Task S3CompatibleMultipartUpload_UploadPartWithTrailerBackedPayloadHashWithoutTrailerSignature_ReturnsInvalidRequest()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "multipart-uploadpart-trailer-signature-missing-bucket";
        const string objectKey = "docs/trailer-signature-missing.txt";
        const string payload = "part payload";
        var checksum = ComputeSha256Base64(payload);

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("Content-Type", "text/plain");

        var initiateResponse = await client.SendAsync(initiateRequest);
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var partRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            payload,
            sdkChecksumAlgorithm: "SHA256",
            trailerHeaders: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha256"] = checksum
            },
            declaredTrailerHeaderNames: ["x-amz-checksum-sha256"]);
        partRequest.Headers.TryAddWithoutValidation("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER");

        var response = await client.SendAsync(partRequest);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("InvalidRequest", GetRequiredElementValue(document, "Code"));
        Assert.Contains("x-amz-trailer-signature", GetRequiredElementValue(document, "Message"), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("SHA1", "x-amz-checksum-sha1", "multipart-uploadpart-signed-trailer-sha1-valid-bucket", "docs/signed-uploadpart-sha1.txt", "hello multipart signed trailer sha1 checksum")]
    [InlineData("CRC32", "x-amz-checksum-crc32", "multipart-uploadpart-signed-trailer-crc32-valid-bucket", "docs/signed-uploadpart-crc32.txt", "hello multipart signed trailer crc32 checksum")]
    [InlineData("CRC32C", "x-amz-checksum-crc32c", "multipart-uploadpart-signed-trailer-crc32c-valid-bucket", "docs/signed-uploadpart-crc32c.txt", "hello multipart signed trailer crc32c checksum")]
    public async Task S3CompatibleMultipartUpload_UploadPartWithSigV4AwsChunkedTrailerSignatureAndNonSha256Checksum_PersistsAndListsChecksum(
        string sdkChecksumAlgorithm,
        string trailerHeaderName,
        string bucketName,
        string objectKey,
        string payload)
    {
        const string accessKeyId = "sigv4-http-uploadpart-chunked-signed-trailer-access";
        const string secretAccessKey = "sigv4-http-uploadpart-chunked-signed-trailer-secret";
        var checksum = ComputeTrailerChecksumBase64(trailerHeaderName, payload);

        await using var isolatedClient = await CreateSigV4AuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", sdkChecksumAlgorithm);
        SignSigV4HeaderRequest(
            initiateRequest,
            $"/integrated-s3/{bucketName}/{objectKey}?uploads",
            accessKeyId,
            secretAccessKey,
            Convert.ToHexStringLower(SHA256.HashData(Array.Empty<byte>())),
            additionalSignedHeaders: ["x-amz-checksum-algorithm"]);

        var initiateResponse = await client.SendAsync(initiateRequest);

        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        Assert.Equal(sdkChecksumAlgorithm, Assert.Single(initiateResponse.Headers.GetValues("x-amz-checksum-algorithm")));

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var partRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [trailerHeaderName] = checksum
            });
        await AssertSigV4AwsChunkedTrailerRequestAsync(partRequest, sdkChecksumAlgorithm, trailerHeaderName);

        var partResponse = await client.SendAsync(partRequest);

        Assert.Equal(HttpStatusCode.OK, partResponse.StatusCode);
        AssertOnlyExpectedChecksumHeader(partResponse.Headers, trailerHeaderName, checksum);
        var partETag = partResponse.Headers.ETag?.Tag ?? throw new Xunit.Sdk.XunitException("Expected multipart part ETag header.");

        using var listPartsRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/{bucketName}/{objectKey}?uploadId={Uri.EscapeDataString(uploadId)}",
            accessKeyId,
            secretAccessKey);
        var listPartsResponse = await client.SendAsync(listPartsRequest);

        Assert.Equal(HttpStatusCode.OK, listPartsResponse.StatusCode);

        var listPartsDocument = XDocument.Parse(await listPartsResponse.Content.ReadAsStringAsync());
        Assert.Equal(sdkChecksumAlgorithm, GetRequiredElementValue(listPartsDocument, "ChecksumAlgorithm"));
        Assert.Equal("COMPOSITE", GetRequiredElementValue(listPartsDocument, "ChecksumType"));

        var listedPart = Assert.Single(listPartsDocument.Root!.Elements("Part"));
        Assert.Equal("1", listedPart.Element("PartNumber")?.Value);
        Assert.Equal(partETag, listedPart.Element("ETag")?.Value);
        AssertOnlyExpectedMultipartChecksumElement(listedPart, GetMultipartChecksumElementName(trailerHeaderName), checksum);
    }

    [Theory]
    [InlineData("SHA1", "x-amz-checksum-sha1", "multipart-uploadpart-signed-trailer-sha1-bad-digest-bucket", "docs/signed-uploadpart-sha1-bad-digest.txt", "hello multipart signed trailer sha1 bad digest")]
    [InlineData("CRC32", "x-amz-checksum-crc32", "multipart-uploadpart-signed-trailer-crc32-bad-digest-bucket", "docs/signed-uploadpart-crc32-bad-digest.txt", "hello multipart signed trailer crc32 bad digest")]
    [InlineData("CRC32C", "x-amz-checksum-crc32c", "multipart-uploadpart-signed-trailer-crc32c-bad-digest-bucket", "docs/signed-uploadpart-crc32c-bad-digest.txt", "hello multipart signed trailer crc32c bad digest")]
    public async Task S3CompatibleMultipartUpload_UploadPartWithSigV4AwsChunkedTrailerSignatureAndNonSha256ChecksumMismatch_ReturnsBadDigest(
        string sdkChecksumAlgorithm,
        string trailerHeaderName,
        string bucketName,
        string objectKey,
        string payload)
    {
        const string accessKeyId = "sigv4-http-uploadpart-chunked-signed-trailer-access";
        const string secretAccessKey = "sigv4-http-uploadpart-chunked-signed-trailer-secret";
        var mismatchedChecksum = ComputeTrailerChecksumBase64(trailerHeaderName, "different payload");

        await using var isolatedClient = await CreateSigV4AuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", sdkChecksumAlgorithm);
        SignSigV4HeaderRequest(
            initiateRequest,
            $"/integrated-s3/{bucketName}/{objectKey}?uploads",
            accessKeyId,
            secretAccessKey,
            Convert.ToHexStringLower(SHA256.HashData(Array.Empty<byte>())),
            additionalSignedHeaders: ["x-amz-checksum-algorithm"]);

        var initiateResponse = await client.SendAsync(initiateRequest);

        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        Assert.Equal(sdkChecksumAlgorithm, Assert.Single(initiateResponse.Headers.GetValues("x-amz-checksum-algorithm")));

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var partRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [trailerHeaderName] = mismatchedChecksum
            });
        await AssertSigV4AwsChunkedTrailerRequestAsync(partRequest, sdkChecksumAlgorithm, trailerHeaderName);

        var response = await client.SendAsync(partRequest);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("BadDigest", GetRequiredElementValue(document, "Code"));
        var message = GetRequiredElementValue(document, "Message");
        Assert.Contains(sdkChecksumAlgorithm, message, StringComparison.Ordinal);
        Assert.Contains(objectKey, message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task S3CompatibleMultipartUpload_UploadPartWithUnsignedTrailerBackedPayloadHashWithoutTrailerSignature_Succeeds()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "multipart-uploadpart-unsigned-trailer-valid-bucket";
        const string objectKey = "docs/unsigned-trailer-checksum.txt";
        const string payload = "hello multipart unsigned trailer checksum";
        var checksum = ComputeSha256Base64(payload);

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("Content-Type", "text/plain");
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "SHA256");

        var initiateResponse = await client.SendAsync(initiateRequest);
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        Assert.Equal("SHA256", Assert.Single(initiateResponse.Headers.GetValues("x-amz-checksum-algorithm")));

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var partRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            payload,
            trailerHeaders: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha256"] = checksum
            },
            declaredTrailerHeaderNames: ["x-amz-checksum-sha256"]);
        partRequest.Headers.TryAddWithoutValidation("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER");
        partRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "SHA256");

        var partResponse = await client.SendAsync(partRequest);

        Assert.Equal(HttpStatusCode.OK, partResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(partResponse.Headers.GetValues("x-amz-checksum-sha256")));
        var partETag = partResponse.Headers.ETag?.Tag ?? throw new Xunit.Sdk.XunitException("Expected multipart part ETag header.");

        var listPartsResponse = await client.GetAsync($"/integrated-s3/{bucketName}/{objectKey}?uploadId={Uri.EscapeDataString(uploadId)}");
        Assert.Equal(HttpStatusCode.OK, listPartsResponse.StatusCode);

        var listPartsDocument = XDocument.Parse(await listPartsResponse.Content.ReadAsStringAsync());
        Assert.Equal("SHA256", GetRequiredElementValue(listPartsDocument, "ChecksumAlgorithm"));
        Assert.Equal("COMPOSITE", GetRequiredElementValue(listPartsDocument, "ChecksumType"));

        var listedPart = Assert.Single(listPartsDocument.Root!.Elements("Part"));
        Assert.Equal("1", listedPart.Element("PartNumber")?.Value);
        Assert.Equal(partETag, listedPart.Element("ETag")?.Value);
        Assert.Equal(checksum, listedPart.Element("ChecksumSHA256")?.Value);
    }

    [Fact]
    public async Task S3CompatibleMultipartUpload_UploadPartWithUnsignedTrailerBackedSha1Checksum_PersistsAndListsSha1Checksum()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "multipart-uploadpart-unsigned-trailer-sha1-valid-bucket";
        const string objectKey = "docs/unsigned-trailer-sha1-checksum.txt";
        const string payload = "hello multipart unsigned trailer sha1 checksum";
        var checksum = ComputeSha1Base64(payload);

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("Content-Type", "text/plain");
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "SHA1");

        var initiateResponse = await client.SendAsync(initiateRequest);
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        Assert.Equal("SHA1", Assert.Single(initiateResponse.Headers.GetValues("x-amz-checksum-algorithm")));

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var partRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            payload,
            trailerHeaders: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha1"] = checksum
            },
            declaredTrailerHeaderNames: ["x-amz-checksum-sha1"]);
        partRequest.Headers.TryAddWithoutValidation("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER");
        partRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "SHA1");

        var partResponse = await client.SendAsync(partRequest);

        Assert.Equal(HttpStatusCode.OK, partResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(partResponse.Headers.GetValues("x-amz-checksum-sha1")));
        Assert.False(partResponse.Headers.Contains("x-amz-checksum-sha256"));
        var partETag = partResponse.Headers.ETag?.Tag ?? throw new Xunit.Sdk.XunitException("Expected multipart part ETag header.");

        var listPartsResponse = await client.GetAsync($"/integrated-s3/{bucketName}/{objectKey}?uploadId={Uri.EscapeDataString(uploadId)}");
        Assert.Equal(HttpStatusCode.OK, listPartsResponse.StatusCode);

        var listPartsDocument = XDocument.Parse(await listPartsResponse.Content.ReadAsStringAsync());
        Assert.Equal("SHA1", GetRequiredElementValue(listPartsDocument, "ChecksumAlgorithm"));
        Assert.Equal("COMPOSITE", GetRequiredElementValue(listPartsDocument, "ChecksumType"));

        var listedPart = Assert.Single(listPartsDocument.Root!.Elements("Part"));
        Assert.Equal("1", listedPart.Element("PartNumber")?.Value);
        Assert.Equal(partETag, listedPart.Element("ETag")?.Value);
        Assert.Equal(checksum, listedPart.Element("ChecksumSHA1")?.Value);
        Assert.Null(listedPart.Element("ChecksumSHA256"));
    }

    [Fact]
    public async Task S3CompatibleMultipartUpload_UploadPartWithUnsignedTrailerBackedCrc32cChecksum_PersistsAndListsCrc32cChecksum()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "multipart-uploadpart-unsigned-trailer-crc32c-valid-bucket";
        const string objectKey = "docs/unsigned-trailer-crc32c-checksum.txt";
        const string payload = "hello multipart unsigned trailer crc32c checksum";
        var checksum = ChecksumTestAlgorithms.ComputeCrc32cBase64(payload);

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("Content-Type", "text/plain");
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "CRC32C");

        var initiateResponse = await client.SendAsync(initiateRequest);
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        Assert.Equal("CRC32C", Assert.Single(initiateResponse.Headers.GetValues("x-amz-checksum-algorithm")));

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var partRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            payload,
            trailerHeaders: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-crc32c"] = checksum
            },
            declaredTrailerHeaderNames: ["x-amz-checksum-crc32c"]);
        partRequest.Headers.TryAddWithoutValidation("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER");
        partRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "CRC32C");

        var partResponse = await client.SendAsync(partRequest);

        Assert.Equal(HttpStatusCode.OK, partResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(partResponse.Headers.GetValues("x-amz-checksum-crc32c")));
        Assert.False(partResponse.Headers.Contains("x-amz-checksum-sha256"));
        var partETag = partResponse.Headers.ETag?.Tag ?? throw new Xunit.Sdk.XunitException("Expected multipart part ETag header.");

        var listPartsResponse = await client.GetAsync($"/integrated-s3/{bucketName}/{objectKey}?uploadId={Uri.EscapeDataString(uploadId)}");
        Assert.Equal(HttpStatusCode.OK, listPartsResponse.StatusCode);

        var listPartsDocument = XDocument.Parse(await listPartsResponse.Content.ReadAsStringAsync());
        Assert.Equal("CRC32C", GetRequiredElementValue(listPartsDocument, "ChecksumAlgorithm"));
        Assert.Equal("COMPOSITE", GetRequiredElementValue(listPartsDocument, "ChecksumType"));

        var listedPart = Assert.Single(listPartsDocument.Root!.Elements("Part"));
        Assert.Equal("1", listedPart.Element("PartNumber")?.Value);
        Assert.Equal(partETag, listedPart.Element("ETag")?.Value);
        Assert.Equal(checksum, listedPart.Element("ChecksumCRC32C")?.Value);
        Assert.Null(listedPart.Element("ChecksumSHA256"));
    }

    [Fact]
    public async Task S3CompatibleMultipartUpload_UploadPartWithUnsignedTrailerBackedCrc32Checksum_PersistsAndListsCrc32Checksum()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "multipart-uploadpart-unsigned-trailer-crc32-valid-bucket";
        const string objectKey = "docs/unsigned-trailer-crc32-checksum.txt";
        const string payload = "hello multipart unsigned trailer crc32 checksum";
        var checksum = ComputeCrc32Base64(payload);

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("Content-Type", "text/plain");
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "CRC32");

        var initiateResponse = await client.SendAsync(initiateRequest);
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        Assert.Equal("CRC32", Assert.Single(initiateResponse.Headers.GetValues("x-amz-checksum-algorithm")));

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var partRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            payload,
            trailerHeaders: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-crc32"] = checksum
            },
            declaredTrailerHeaderNames: ["x-amz-checksum-crc32"]);
        partRequest.Headers.TryAddWithoutValidation("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER");
        partRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "CRC32");

        var partResponse = await client.SendAsync(partRequest);

        Assert.Equal(HttpStatusCode.OK, partResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(partResponse.Headers.GetValues("x-amz-checksum-crc32")));
        Assert.False(partResponse.Headers.Contains("x-amz-checksum-sha256"));
        Assert.False(partResponse.Headers.Contains("x-amz-checksum-sha1"));
        Assert.False(partResponse.Headers.Contains("x-amz-checksum-crc32c"));
        var partETag = partResponse.Headers.ETag?.Tag ?? throw new Xunit.Sdk.XunitException("Expected multipart part ETag header.");

        var listPartsResponse = await client.GetAsync($"/integrated-s3/{bucketName}/{objectKey}?uploadId={Uri.EscapeDataString(uploadId)}");
        Assert.Equal(HttpStatusCode.OK, listPartsResponse.StatusCode);

        var listPartsDocument = XDocument.Parse(await listPartsResponse.Content.ReadAsStringAsync());
        Assert.Equal("CRC32", GetRequiredElementValue(listPartsDocument, "ChecksumAlgorithm"));
        Assert.Equal("COMPOSITE", GetRequiredElementValue(listPartsDocument, "ChecksumType"));

        var listedPart = Assert.Single(listPartsDocument.Root!.Elements("Part"));
        Assert.Equal("1", listedPart.Element("PartNumber")?.Value);
        Assert.Equal(partETag, listedPart.Element("ETag")?.Value);
        Assert.Equal(checksum, listedPart.Element("ChecksumCRC32")?.Value);
        Assert.Null(listedPart.Element("ChecksumSHA256"));
        Assert.Null(listedPart.Element("ChecksumCRC32C"));
    }

    [Fact]
    public async Task S3CompatibleMultipartUpload_UploadPartWithUnsignedTrailerBackedChecksumMismatch_ReturnsBadDigest()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "multipart-uploadpart-unsigned-trailer-bad-digest-bucket";
        const string objectKey = "docs/unsigned-trailer-bad-digest.txt";
        const string payload = "hello multipart unsigned trailer checksum";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("Content-Type", "text/plain");
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "SHA256");

        var initiateResponse = await client.SendAsync(initiateRequest);
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        Assert.Equal("SHA256", Assert.Single(initiateResponse.Headers.GetValues("x-amz-checksum-algorithm")));

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var partRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            payload,
            trailerHeaders: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha256"] = ComputeSha256Base64("different payload")
            },
            declaredTrailerHeaderNames: ["x-amz-checksum-sha256"]);
        partRequest.Headers.TryAddWithoutValidation("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER");
        partRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "SHA256");

        var response = await client.SendAsync(partRequest);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("BadDigest", GetRequiredElementValue(document, "Code"));
        var message = GetRequiredElementValue(document, "Message");
        Assert.Contains("SHA256", message, StringComparison.Ordinal);
        Assert.Contains(objectKey, message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task S3CompatibleMultipartUpload_UploadPartWithUnsignedTrailerBackedSha1ChecksumMismatch_ReturnsBadDigest()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "multipart-uploadpart-unsigned-trailer-sha1-bad-digest-bucket";
        const string objectKey = "docs/unsigned-trailer-sha1-bad-digest.txt";
        const string payload = "hello multipart unsigned trailer sha1 checksum";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("Content-Type", "text/plain");
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "SHA1");

        var initiateResponse = await client.SendAsync(initiateRequest);
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        Assert.Equal("SHA1", Assert.Single(initiateResponse.Headers.GetValues("x-amz-checksum-algorithm")));

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var partRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            payload,
            trailerHeaders: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha1"] = ComputeSha1Base64("different payload")
            },
            declaredTrailerHeaderNames: ["x-amz-checksum-sha1"]);
        partRequest.Headers.TryAddWithoutValidation("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER");
        partRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "SHA1");

        var response = await client.SendAsync(partRequest);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("BadDigest", GetRequiredElementValue(document, "Code"));
        var message = GetRequiredElementValue(document, "Message");
        Assert.Contains("SHA1", message, StringComparison.Ordinal);
        Assert.Contains(objectKey, message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task S3CompatibleMultipartUpload_UploadPartWithUnsignedTrailerBackedCrc32cChecksumMismatch_ReturnsBadDigest()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "multipart-uploadpart-unsigned-trailer-crc32c-bad-digest-bucket";
        const string objectKey = "docs/unsigned-trailer-crc32c-bad-digest.txt";
        const string payload = "hello multipart unsigned trailer crc32c checksum";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("Content-Type", "text/plain");
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "CRC32C");

        var initiateResponse = await client.SendAsync(initiateRequest);
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        Assert.Equal("CRC32C", Assert.Single(initiateResponse.Headers.GetValues("x-amz-checksum-algorithm")));

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var partRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            payload,
            trailerHeaders: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-crc32c"] = ChecksumTestAlgorithms.ComputeCrc32cBase64("different payload")
            },
            declaredTrailerHeaderNames: ["x-amz-checksum-crc32c"]);
        partRequest.Headers.TryAddWithoutValidation("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER");
        partRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "CRC32C");

        var response = await client.SendAsync(partRequest);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("BadDigest", GetRequiredElementValue(document, "Code"));
        var message = GetRequiredElementValue(document, "Message");
        Assert.Contains("CRC32C", message, StringComparison.Ordinal);
        Assert.Contains(objectKey, message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task S3CompatibleMultipartUpload_UploadPartWithUnsignedTrailerBackedCrc32ChecksumMismatch_ReturnsBadDigest()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "multipart-uploadpart-unsigned-trailer-crc32-bad-digest-bucket";
        const string objectKey = "docs/unsigned-trailer-crc32-bad-digest.txt";
        const string payload = "hello multipart unsigned trailer crc32 checksum";
        var checksum = ComputeCrc32Base64("different payload");

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("Content-Type", "text/plain");
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "CRC32");

        var initiateResponse = await client.SendAsync(initiateRequest);
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        Assert.Equal("CRC32", Assert.Single(initiateResponse.Headers.GetValues("x-amz-checksum-algorithm")));

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var partRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            payload,
            trailerHeaders: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-crc32"] = checksum
            },
            declaredTrailerHeaderNames: ["x-amz-checksum-crc32"]);
        partRequest.Headers.TryAddWithoutValidation("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER");
        partRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "CRC32");

        var response = await client.SendAsync(partRequest);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("BadDigest", GetRequiredElementValue(document, "Code"));
        var message = GetRequiredElementValue(document, "Message");
        Assert.Contains("CRC32", message, StringComparison.Ordinal);
        Assert.Contains(objectKey, message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task S3CompatibleMultipartUpload_UploadPartWithChecksumAlgorithmHeaderAndChecksumTrailer_Succeeds()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "multipart-uploadpart-trailer-checksum-valid-bucket";
        const string objectKey = "docs/trailer-checksum.txt";
        const string payload = "hello multipart trailer checksum";
        var checksum = ComputeSha256Base64(payload);

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("Content-Type", "text/plain");
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "SHA256");

        var initiateResponse = await client.SendAsync(initiateRequest);
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        Assert.Equal("SHA256", Assert.Single(initiateResponse.Headers.GetValues("x-amz-checksum-algorithm")));

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var partRequest = CreateAwsChunkedPutObjectRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            payload,
            trailerHeaders: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha256"] = checksum
            });
        partRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "SHA256");

        var partResponse = await client.SendAsync(partRequest);

        Assert.Equal(HttpStatusCode.OK, partResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(partResponse.Headers.GetValues("x-amz-checksum-sha256")));
        var partETag = partResponse.Headers.ETag?.Tag ?? throw new Xunit.Sdk.XunitException("Expected multipart part ETag header.");

        var listPartsResponse = await client.GetAsync($"/integrated-s3/{bucketName}/{objectKey}?uploadId={Uri.EscapeDataString(uploadId)}");
        Assert.Equal(HttpStatusCode.OK, listPartsResponse.StatusCode);

        var listPartsDocument = XDocument.Parse(await listPartsResponse.Content.ReadAsStringAsync());
        Assert.Equal("SHA256", GetRequiredElementValue(listPartsDocument, "ChecksumAlgorithm"));
        Assert.Equal("COMPOSITE", GetRequiredElementValue(listPartsDocument, "ChecksumType"));

        var listedPart = Assert.Single(listPartsDocument.Root!.Elements("Part"));
        Assert.Equal("1", listedPart.Element("PartNumber")?.Value);
        Assert.Equal(partETag, listedPart.Element("ETag")?.Value);
        Assert.Equal(checksum, listedPart.Element("ChecksumSHA256")?.Value);
    }

    [Fact]
    public async Task S3CompatibleMultipartUpload_WithChecksumHeaders_EmitsCompositeChecksumHeaders()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "multipart-checksum-bucket";
        const string objectKey = "docs/checksum.txt";
        const string part1Payload = "hello ";
        const string part2Payload = "world";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        var part1Checksum = ComputeSha256Base64(part1Payload);
        var part2Checksum = ComputeSha256Base64(part2Payload);
        var compositeChecksum = ComputeMultipartSha256Base64(part1Checksum, part2Checksum);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-sdk-checksum-algorithm", "SHA256");
        initiateRequest.Headers.TryAddWithoutValidation("Content-Type", "text/plain");

        var initiateResponse = await client.SendAsync(initiateRequest);
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var part1Request = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}")
        {
            Content = new StringContent(part1Payload, Encoding.UTF8, "text/plain")
        };
        part1Request.Headers.TryAddWithoutValidation("x-amz-sdk-checksum-algorithm", "SHA256");
        part1Request.Headers.TryAddWithoutValidation("x-amz-checksum-sha256", part1Checksum);

        var part1Response = await client.SendAsync(part1Request);
        Assert.Equal(HttpStatusCode.OK, part1Response.StatusCode);
        Assert.Equal(part1Checksum, Assert.Single(part1Response.Headers.GetValues("x-amz-checksum-sha256")));
        var part1ETag = part1Response.Headers.ETag?.Tag ?? throw new Xunit.Sdk.XunitException("Expected multipart part ETag header.");

        using var part2Request = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/{objectKey}?partNumber=2&uploadId={Uri.EscapeDataString(uploadId)}")
        {
            Content = new StringContent(part2Payload, Encoding.UTF8, "text/plain")
        };
        part2Request.Headers.TryAddWithoutValidation("x-amz-sdk-checksum-algorithm", "SHA256");
        part2Request.Headers.TryAddWithoutValidation("x-amz-checksum-sha256", part2Checksum);

        var part2Response = await client.SendAsync(part2Request);
        Assert.Equal(HttpStatusCode.OK, part2Response.StatusCode);
        Assert.Equal(part2Checksum, Assert.Single(part2Response.Headers.GetValues("x-amz-checksum-sha256")));
        var part2ETag = part2Response.Headers.ETag?.Tag ?? throw new Xunit.Sdk.XunitException("Expected multipart part ETag header.");

        var completeBody = $"""
<CompleteMultipartUpload>
    <Part>
        <PartNumber>1</PartNumber>
        <ETag>{part1ETag}</ETag>
    </Part>
    <Part>
        <PartNumber>2</PartNumber>
        <ETag>{part2ETag}</ETag>
    </Part>
</CompleteMultipartUpload>
""";

        using var completeRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploadId={Uri.EscapeDataString(uploadId)}")
        {
            Content = new StringContent(completeBody, Encoding.UTF8, "application/xml")
        };

        var completeResponse = await client.SendAsync(completeRequest);
        Assert.Equal(HttpStatusCode.OK, completeResponse.StatusCode);
        Assert.Equal(compositeChecksum, Assert.Single(completeResponse.Headers.GetValues("x-amz-checksum-sha256")));

        var completeDocument = XDocument.Parse(await completeResponse.Content.ReadAsStringAsync());
        Assert.Equal("CompleteMultipartUploadResult", completeDocument.Root?.Name.LocalName);
        Assert.Equal(bucketName, GetRequiredElementValue(completeDocument, "Bucket"));
        Assert.Equal(objectKey, GetRequiredElementValue(completeDocument, "Key"));

        using var headRequest = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{bucketName}/{objectKey}");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        Assert.Equal(compositeChecksum, Assert.Single(headResponse.Headers.GetValues("x-amz-checksum-sha256")));

        var downloadResponse = await client.GetAsync($"/integrated-s3/{bucketName}/{objectKey}");
        Assert.Equal(HttpStatusCode.OK, downloadResponse.StatusCode);
        Assert.Equal(compositeChecksum, Assert.Single(downloadResponse.Headers.GetValues("x-amz-checksum-sha256")));
        Assert.Equal(part1Payload + part2Payload, await downloadResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task S3CompatibleMultipartUpload_WithSha1ChecksumHeaders_EmitsCompositeChecksumHeadersAndXml()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "multipart-sha1-checksum-bucket";
        const string objectKey = "docs/sha1-checksum.txt";
        const string part1Payload = "hello ";
        const string part2Payload = "world";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        var part1Checksum = ComputeSha1Base64(part1Payload);
        var part2Checksum = ComputeSha1Base64(part2Payload);
        var compositeChecksum = ComputeMultipartSha1Base64(part1Checksum, part2Checksum);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-sdk-checksum-algorithm", "SHA1");
        initiateRequest.Headers.TryAddWithoutValidation("Content-Type", "text/plain");

        var initiateResponse = await client.SendAsync(initiateRequest);
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        Assert.Equal("SHA1", Assert.Single(initiateResponse.Headers.GetValues("x-amz-checksum-algorithm")));

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var part1Request = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}")
        {
            Content = new StringContent(part1Payload, Encoding.UTF8, "text/plain")
        };
        part1Request.Headers.TryAddWithoutValidation("x-amz-sdk-checksum-algorithm", "SHA1");
        part1Request.Headers.TryAddWithoutValidation("x-amz-checksum-sha1", part1Checksum);

        var part1Response = await client.SendAsync(part1Request);
        Assert.Equal(HttpStatusCode.OK, part1Response.StatusCode);
        Assert.Equal(part1Checksum, Assert.Single(part1Response.Headers.GetValues("x-amz-checksum-sha1")));
        var part1ETag = part1Response.Headers.ETag?.Tag ?? throw new Xunit.Sdk.XunitException("Expected multipart part ETag header.");

        using var part2Request = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/{objectKey}?partNumber=2&uploadId={Uri.EscapeDataString(uploadId)}")
        {
            Content = new StringContent(part2Payload, Encoding.UTF8, "text/plain")
        };
        part2Request.Headers.TryAddWithoutValidation("x-amz-sdk-checksum-algorithm", "SHA1");
        part2Request.Headers.TryAddWithoutValidation("x-amz-checksum-sha1", part2Checksum);

        var part2Response = await client.SendAsync(part2Request);
        Assert.Equal(HttpStatusCode.OK, part2Response.StatusCode);
        Assert.Equal(part2Checksum, Assert.Single(part2Response.Headers.GetValues("x-amz-checksum-sha1")));
        var part2ETag = part2Response.Headers.ETag?.Tag ?? throw new Xunit.Sdk.XunitException("Expected multipart part ETag header.");

        var completeBody = $"""
<CompleteMultipartUpload>
    <Part>
        <PartNumber>1</PartNumber>
        <ETag>{part1ETag}</ETag>
        <ChecksumSHA1>{part1Checksum}</ChecksumSHA1>
    </Part>
    <Part>
        <PartNumber>2</PartNumber>
        <ETag>{part2ETag}</ETag>
        <ChecksumSHA1>{part2Checksum}</ChecksumSHA1>
    </Part>
</CompleteMultipartUpload>
""";

        using var completeRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploadId={Uri.EscapeDataString(uploadId)}")
        {
            Content = new StringContent(completeBody, Encoding.UTF8, "application/xml")
        };

        var completeResponse = await client.SendAsync(completeRequest);
        Assert.Equal(HttpStatusCode.OK, completeResponse.StatusCode);
        Assert.Equal(compositeChecksum, Assert.Single(completeResponse.Headers.GetValues("x-amz-checksum-sha1")));
        Assert.Equal("COMPOSITE", Assert.Single(completeResponse.Headers.GetValues("x-amz-checksum-type")));

        var completeDocument = XDocument.Parse(await completeResponse.Content.ReadAsStringAsync());
        Assert.Equal("CompleteMultipartUploadResult", completeDocument.Root?.Name.LocalName);
        Assert.Equal(bucketName, GetRequiredElementValue(completeDocument, "Bucket"));
        Assert.Equal(objectKey, GetRequiredElementValue(completeDocument, "Key"));
        Assert.Equal(compositeChecksum, GetRequiredElementValue(completeDocument, "ChecksumSHA1"));
        Assert.Equal("COMPOSITE", GetRequiredElementValue(completeDocument, "ChecksumType"));

        using var headRequest = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{bucketName}/{objectKey}");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        Assert.Equal(compositeChecksum, Assert.Single(headResponse.Headers.GetValues("x-amz-checksum-sha1")));
        Assert.Equal("COMPOSITE", Assert.Single(headResponse.Headers.GetValues("x-amz-checksum-type")));

        var downloadResponse = await client.GetAsync($"/integrated-s3/{bucketName}/{objectKey}");
        Assert.Equal(HttpStatusCode.OK, downloadResponse.StatusCode);
        Assert.Equal(compositeChecksum, Assert.Single(downloadResponse.Headers.GetValues("x-amz-checksum-sha1")));
        Assert.Equal("COMPOSITE", Assert.Single(downloadResponse.Headers.GetValues("x-amz-checksum-type")));
        Assert.Equal(part1Payload + part2Payload, await downloadResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task S3CompatibleMultipartUpload_WithCrc32cChecksumHeaders_EmitsCompositeChecksumHeadersAndXml()
    {
        using var client = await _factory.CreateClientAsync();

        const string bucketName = "multipart-crc32c-checksum-bucket";
        const string objectKey = "docs/crc32c-checksum.txt";
        const string part1Payload = "hello ";
        const string part2Payload = "world";

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        var part1Checksum = ChecksumTestAlgorithms.ComputeCrc32cBase64(part1Payload);
        var part2Checksum = ChecksumTestAlgorithms.ComputeCrc32cBase64(part2Payload);
        var compositeChecksum = ChecksumTestAlgorithms.ComputeMultipartCrc32cBase64(part1Checksum, part2Checksum);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-sdk-checksum-algorithm", "CRC32C");
        initiateRequest.Headers.TryAddWithoutValidation("Content-Type", "text/plain");

        var initiateResponse = await client.SendAsync(initiateRequest);
        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        Assert.Equal("CRC32C", Assert.Single(initiateResponse.Headers.GetValues("x-amz-checksum-algorithm")));

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var part1Request = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}")
        {
            Content = new StringContent(part1Payload, Encoding.UTF8, "text/plain")
        };
        part1Request.Headers.TryAddWithoutValidation("x-amz-sdk-checksum-algorithm", "CRC32C");
        part1Request.Headers.TryAddWithoutValidation("x-amz-checksum-crc32c", part1Checksum);

        var part1Response = await client.SendAsync(part1Request);
        Assert.Equal(HttpStatusCode.OK, part1Response.StatusCode);
        Assert.Equal(part1Checksum, Assert.Single(part1Response.Headers.GetValues("x-amz-checksum-crc32c")));
        var part1ETag = part1Response.Headers.ETag?.Tag ?? throw new Xunit.Sdk.XunitException("Expected multipart part ETag header.");

        using var part2Request = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/{objectKey}?partNumber=2&uploadId={Uri.EscapeDataString(uploadId)}")
        {
            Content = new StringContent(part2Payload, Encoding.UTF8, "text/plain")
        };
        part2Request.Headers.TryAddWithoutValidation("x-amz-sdk-checksum-algorithm", "CRC32C");
        part2Request.Headers.TryAddWithoutValidation("x-amz-checksum-crc32c", part2Checksum);

        var part2Response = await client.SendAsync(part2Request);
        Assert.Equal(HttpStatusCode.OK, part2Response.StatusCode);
        Assert.Equal(part2Checksum, Assert.Single(part2Response.Headers.GetValues("x-amz-checksum-crc32c")));
        var part2ETag = part2Response.Headers.ETag?.Tag ?? throw new Xunit.Sdk.XunitException("Expected multipart part ETag header.");

        var completeBody = $"""
<CompleteMultipartUpload>
    <Part>
        <PartNumber>1</PartNumber>
        <ETag>{part1ETag}</ETag>
        <ChecksumCRC32C>{part1Checksum}</ChecksumCRC32C>
    </Part>
    <Part>
        <PartNumber>2</PartNumber>
        <ETag>{part2ETag}</ETag>
        <ChecksumCRC32C>{part2Checksum}</ChecksumCRC32C>
    </Part>
</CompleteMultipartUpload>
""";

        using var completeRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploadId={Uri.EscapeDataString(uploadId)}")
        {
            Content = new StringContent(completeBody, Encoding.UTF8, "application/xml")
        };

        var completeResponse = await client.SendAsync(completeRequest);
        Assert.Equal(HttpStatusCode.OK, completeResponse.StatusCode);
        Assert.Equal(compositeChecksum, Assert.Single(completeResponse.Headers.GetValues("x-amz-checksum-crc32c")));
        Assert.Equal("COMPOSITE", Assert.Single(completeResponse.Headers.GetValues("x-amz-checksum-type")));

        var completeDocument = XDocument.Parse(await completeResponse.Content.ReadAsStringAsync());
        Assert.Equal("CompleteMultipartUploadResult", completeDocument.Root?.Name.LocalName);
        Assert.Equal(bucketName, GetRequiredElementValue(completeDocument, "Bucket"));
        Assert.Equal(objectKey, GetRequiredElementValue(completeDocument, "Key"));
        Assert.Equal(compositeChecksum, GetRequiredElementValue(completeDocument, "ChecksumCRC32C"));
        Assert.Equal("COMPOSITE", GetRequiredElementValue(completeDocument, "ChecksumType"));

        using var headRequest = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{bucketName}/{objectKey}");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        Assert.Equal(compositeChecksum, Assert.Single(headResponse.Headers.GetValues("x-amz-checksum-crc32c")));
        Assert.Equal("COMPOSITE", Assert.Single(headResponse.Headers.GetValues("x-amz-checksum-type")));

        var downloadResponse = await client.GetAsync($"/integrated-s3/{bucketName}/{objectKey}");
        Assert.Equal(HttpStatusCode.OK, downloadResponse.StatusCode);
        Assert.Equal(compositeChecksum, Assert.Single(downloadResponse.Headers.GetValues("x-amz-checksum-crc32c")));
        Assert.Equal("COMPOSITE", Assert.Single(downloadResponse.Headers.GetValues("x-amz-checksum-type")));
        Assert.Equal(part1Payload + part2Payload, await downloadResponse.Content.ReadAsStringAsync());
    }

    [Theory]
    [InlineData("aws:kms", ObjectServerSideEncryptionAlgorithm.Kms)]
    [InlineData("aws:kms:dsse", ObjectServerSideEncryptionAlgorithm.KmsDsse)]
    public async Task S3CompatiblePutObject_WithManagedKmsServerSideEncryptionHeaders_ParsesRequestAndEmitsResponseHeaders(
        string headerValue,
        ObjectServerSideEncryptionAlgorithm expectedAlgorithm)
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        var encryptionContext = Convert.ToBase64String(Encoding.UTF8.GetBytes("""{"tenant":"alpha","environment":"test"}"""));

        using var request = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/sse-put-bucket/docs/encrypted.txt")
        {
            Content = new StringContent("encrypted payload", Encoding.UTF8, "text/plain")
        };
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption", headerValue);
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-aws-kms-key-id", "alias/test-key");
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-context", encryptionContext);

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(headerValue, Assert.Single(response.Headers.GetValues("x-amz-server-side-encryption")));
        Assert.Equal("alias/test-key", Assert.Single(response.Headers.GetValues("x-amz-server-side-encryption-aws-kms-key-id")));

        var putRequest = storageService.LastPutObjectRequest ?? throw new Xunit.Sdk.XunitException("Expected PUT request to reach the storage service.");
        var serverSideEncryption = putRequest.ServerSideEncryption ?? throw new Xunit.Sdk.XunitException("Expected PUT request server-side encryption settings.");
        Assert.Equal(expectedAlgorithm, serverSideEncryption.Algorithm);
        Assert.Equal("alias/test-key", serverSideEncryption.KeyId);
        Assert.Equal("alpha", serverSideEncryption.Context!["tenant"]);
        Assert.Equal("test", serverSideEncryption.Context["environment"]);
    }

    [Fact]
    public async Task S3CompatiblePutObject_WithAes256AndKmsHeaders_ReturnsInvalidRequest()
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        using var request = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/sse-invalid-bucket/docs/invalid.txt")
        {
            Content = new StringContent("invalid encryption request", Encoding.UTF8, "text/plain")
        };
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption", "AES256");
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-aws-kms-key-id", "alias/invalid");

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("InvalidRequest", GetRequiredElementValue(document, "Code"));
        Assert.Contains("x-amz-server-side-encryption-aws-kms-key-id", GetRequiredElementValue(document, "Message"));
        Assert.Null(storageService.LastPutObjectRequest);
    }

    [Fact]
    public async Task S3CompatiblePutObject_WithInvalidServerSideEncryptionContext_ReturnsInvalidArgument()
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        using var request = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/sse-context-bucket/docs/invalid-context.txt")
        {
            Content = new StringContent("invalid context", Encoding.UTF8, "text/plain")
        };
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption", "aws:kms");
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-context", "not-base64");

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("InvalidArgument", GetRequiredElementValue(document, "Code"));
        Assert.Contains("x-amz-server-side-encryption-context", GetRequiredElementValue(document, "Message"));
        Assert.Null(storageService.LastPutObjectRequest);
    }

    [Fact]
    public async Task S3CompatiblePutObject_WithEmptyServerSideEncryptionContext_ReturnsInvalidArgument()
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        using var request = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/sse-context-bucket/docs/empty-context.txt")
        {
            Content = new StringContent("empty context", Encoding.UTF8, "text/plain")
        };
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption", "aws:kms");
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-context", Convert.ToBase64String(Encoding.UTF8.GetBytes("{}")));

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("InvalidArgument", GetRequiredElementValue(document, "Code"));
        Assert.Contains("at least one key-value pair", GetRequiredElementValue(document, "Message"));
        Assert.Null(storageService.LastPutObjectRequest);
    }

    [Fact]
    public async Task S3CompatiblePutObject_WithEmptyServerSideEncryptionContextValue_ReturnsInvalidArgument()
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        using var request = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/sse-context-bucket/docs/empty-context-value.txt")
        {
            Content = new StringContent("empty context value", Encoding.UTF8, "text/plain")
        };
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption", "aws:kms:dsse");
        request.Headers.TryAddWithoutValidation(
            "x-amz-server-side-encryption-context",
            Convert.ToBase64String(Encoding.UTF8.GetBytes("""{"tenant":""}""")));

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("InvalidArgument", GetRequiredElementValue(document, "Code"));
        Assert.Contains("non-empty string values", GetRequiredElementValue(document, "Message"));
        Assert.Null(storageService.LastPutObjectRequest);
    }

    [Fact]
    public async Task S3CompatiblePutObject_WithCustomerEncryptionHeaders_ParsesRequestAndEmitsResponseHeaders()
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        var keyBytes = new byte[32];
        System.Security.Cryptography.RandomNumberGenerator.Fill(keyBytes);
        var key = Convert.ToBase64String(keyBytes);
        var keyMd5 = Convert.ToBase64String(System.Security.Cryptography.MD5.HashData(keyBytes));

        using var request = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/sse-c-bucket/docs/encrypted.txt")
        {
            Content = new StringContent("customer encrypted payload", Encoding.UTF8, "text/plain")
        };
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-algorithm", "AES256");
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key", key);
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key-MD5", keyMd5);

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("AES256", Assert.Single(response.Headers.GetValues("x-amz-server-side-encryption-customer-algorithm")));
        Assert.Equal(keyMd5, Assert.Single(response.Headers.GetValues("x-amz-server-side-encryption-customer-key-MD5")));

        var putRequest = storageService.LastPutObjectRequest ?? throw new Xunit.Sdk.XunitException("Expected PUT request to reach the storage service.");
        var customerEncryption = putRequest.CustomerEncryption ?? throw new Xunit.Sdk.XunitException("Expected customer encryption settings.");
        Assert.Equal("AES256", customerEncryption.Algorithm);
        Assert.Equal(key, customerEncryption.Key);
        Assert.Equal(keyMd5, customerEncryption.KeyMd5);
    }

    [Fact]
    public async Task S3CompatiblePutObject_WithIncompleteCustomerEncryptionHeaders_IgnoredGracefully()
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        using var request = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/sse-c-partial-bucket/docs/partial.txt")
        {
            Content = new StringContent("partial sse-c headers", Encoding.UTF8, "text/plain")
        };
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-algorithm", "AES256");

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var putRequest = storageService.LastPutObjectRequest ?? throw new Xunit.Sdk.XunitException("Expected PUT request to reach the storage service.");
        Assert.Null(putRequest.CustomerEncryption);
    }

    [Fact]
    public async Task S3CompatibleGetObject_WithCustomerEncryptionHeaders_ParsesAndDelegates()
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        var keyBytes = new byte[32];
        System.Security.Cryptography.RandomNumberGenerator.Fill(keyBytes);
        var key = Convert.ToBase64String(keyBytes);
        var keyMd5 = Convert.ToBase64String(System.Security.Cryptography.MD5.HashData(keyBytes));

        using var request = new HttpRequestMessage(HttpMethod.Get, "/integrated-s3/sse-c-get-bucket/docs/encrypted.txt");
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-algorithm", "AES256");
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key", key);
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key-MD5", keyMd5);

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("AES256", Assert.Single(response.Headers.GetValues("x-amz-server-side-encryption-customer-algorithm")));
        Assert.Equal(keyMd5, Assert.Single(response.Headers.GetValues("x-amz-server-side-encryption-customer-key-MD5")));

        var getRequest = storageService.LastGetObjectRequest ?? throw new Xunit.Sdk.XunitException("Expected GET request to reach the storage service.");
        var customerEncryption = getRequest.CustomerEncryption ?? throw new Xunit.Sdk.XunitException("Expected customer encryption settings.");
        Assert.Equal("AES256", customerEncryption.Algorithm);
        Assert.Equal(key, customerEncryption.Key);
        Assert.Equal(keyMd5, customerEncryption.KeyMd5);
    }

    [Fact]
    public async Task S3CompatibleHeadObject_WithCustomerEncryptionHeaders_ParsesAndDelegates()
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        var keyBytes = new byte[32];
        System.Security.Cryptography.RandomNumberGenerator.Fill(keyBytes);
        var key = Convert.ToBase64String(keyBytes);
        var keyMd5 = Convert.ToBase64String(System.Security.Cryptography.MD5.HashData(keyBytes));

        using var request = new HttpRequestMessage(HttpMethod.Head, "/integrated-s3/sse-c-head-bucket/docs/encrypted.txt");
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-algorithm", "AES256");
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key", key);
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key-MD5", keyMd5);

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("AES256", Assert.Single(response.Headers.GetValues("x-amz-server-side-encryption-customer-algorithm")));
        Assert.Equal(keyMd5, Assert.Single(response.Headers.GetValues("x-amz-server-side-encryption-customer-key-MD5")));

        var headRequest = storageService.LastHeadObjectRequest ?? throw new Xunit.Sdk.XunitException("Expected HEAD request to reach the storage service.");
        var customerEncryption = headRequest.CustomerEncryption ?? throw new Xunit.Sdk.XunitException("Expected customer encryption settings.");
        Assert.Equal("AES256", customerEncryption.Algorithm);
        Assert.Equal(key, customerEncryption.Key);
        Assert.Equal(keyMd5, customerEncryption.KeyMd5);
    }

    [Fact]
    public async Task S3CompatibleCopyObject_WithSourceCustomerEncryptionHeaders_ParsesAndDelegates()
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        var keyBytes = new byte[32];
        System.Security.Cryptography.RandomNumberGenerator.Fill(keyBytes);
        var sourceKey = Convert.ToBase64String(keyBytes);
        var sourceKeyMd5 = Convert.ToBase64String(System.Security.Cryptography.MD5.HashData(keyBytes));

        using var request = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/sse-c-copy-dest/docs/copied.txt");
        request.Headers.TryAddWithoutValidation("x-amz-copy-source", "/sse-c-copy-src/docs/source.txt");
        request.Headers.TryAddWithoutValidation("x-amz-copy-source-server-side-encryption-customer-algorithm", "AES256");
        request.Headers.TryAddWithoutValidation("x-amz-copy-source-server-side-encryption-customer-key", sourceKey);
        request.Headers.TryAddWithoutValidation("x-amz-copy-source-server-side-encryption-customer-key-MD5", sourceKeyMd5);

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var copyRequest = storageService.LastCopyObjectRequest ?? throw new Xunit.Sdk.XunitException("Expected COPY request to reach the storage service.");
        var sourceEncryption = copyRequest.SourceCustomerEncryption ?? throw new Xunit.Sdk.XunitException("Expected source customer encryption settings.");
        Assert.Equal("AES256", sourceEncryption.Algorithm);
        Assert.Equal(sourceKey, sourceEncryption.Key);
        Assert.Equal(sourceKeyMd5, sourceEncryption.KeyMd5);
        Assert.Null(copyRequest.DestinationCustomerEncryption);
    }

    [Fact]
    public async Task S3CompatibleCopyObject_WithDestinationCustomerEncryptionHeaders_ParsesAndDelegates()
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        var keyBytes = new byte[32];
        System.Security.Cryptography.RandomNumberGenerator.Fill(keyBytes);
        var destKey = Convert.ToBase64String(keyBytes);
        var destKeyMd5 = Convert.ToBase64String(System.Security.Cryptography.MD5.HashData(keyBytes));

        using var request = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/sse-c-copy-dest2/docs/copied.txt");
        request.Headers.TryAddWithoutValidation("x-amz-copy-source", "/sse-c-copy-src2/docs/source.txt");
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-algorithm", "AES256");
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key", destKey);
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key-MD5", destKeyMd5);

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("AES256", Assert.Single(response.Headers.GetValues("x-amz-server-side-encryption-customer-algorithm")));
        Assert.Equal(destKeyMd5, Assert.Single(response.Headers.GetValues("x-amz-server-side-encryption-customer-key-MD5")));

        var copyRequest = storageService.LastCopyObjectRequest ?? throw new Xunit.Sdk.XunitException("Expected COPY request to reach the storage service.");
        Assert.Null(copyRequest.SourceCustomerEncryption);
        var destEncryption = copyRequest.DestinationCustomerEncryption ?? throw new Xunit.Sdk.XunitException("Expected destination customer encryption settings.");
        Assert.Equal("AES256", destEncryption.Algorithm);
        Assert.Equal(destKey, destEncryption.Key);
        Assert.Equal(destKeyMd5, destEncryption.KeyMd5);
    }

    [Fact]
    public async Task S3CompatibleCopyObject_WithBothSourceAndDestinationCustomerEncryptionHeaders_ParsesAndDelegates()
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        var srcKeyBytes = new byte[32];
        System.Security.Cryptography.RandomNumberGenerator.Fill(srcKeyBytes);
        var srcKey = Convert.ToBase64String(srcKeyBytes);
        var srcKeyMd5 = Convert.ToBase64String(System.Security.Cryptography.MD5.HashData(srcKeyBytes));

        var destKeyBytes = new byte[32];
        System.Security.Cryptography.RandomNumberGenerator.Fill(destKeyBytes);
        var destKey = Convert.ToBase64String(destKeyBytes);
        var destKeyMd5 = Convert.ToBase64String(System.Security.Cryptography.MD5.HashData(destKeyBytes));

        using var request = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/sse-c-copy-both-dest/docs/copied.txt");
        request.Headers.TryAddWithoutValidation("x-amz-copy-source", "/sse-c-copy-both-src/docs/source.txt");
        request.Headers.TryAddWithoutValidation("x-amz-copy-source-server-side-encryption-customer-algorithm", "AES256");
        request.Headers.TryAddWithoutValidation("x-amz-copy-source-server-side-encryption-customer-key", srcKey);
        request.Headers.TryAddWithoutValidation("x-amz-copy-source-server-side-encryption-customer-key-MD5", srcKeyMd5);
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-algorithm", "AES256");
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key", destKey);
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key-MD5", destKeyMd5);

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var copyRequest = storageService.LastCopyObjectRequest ?? throw new Xunit.Sdk.XunitException("Expected COPY request to reach the storage service.");
        var sourceEncryption = copyRequest.SourceCustomerEncryption ?? throw new Xunit.Sdk.XunitException("Expected source customer encryption settings.");
        Assert.Equal("AES256", sourceEncryption.Algorithm);
        Assert.Equal(srcKey, sourceEncryption.Key);
        Assert.Equal(srcKeyMd5, sourceEncryption.KeyMd5);

        var destEncryption = copyRequest.DestinationCustomerEncryption ?? throw new Xunit.Sdk.XunitException("Expected destination customer encryption settings.");
        Assert.Equal("AES256", destEncryption.Algorithm);
        Assert.Equal(destKey, destEncryption.Key);
        Assert.Equal(destKeyMd5, destEncryption.KeyMd5);
    }

    [Fact]
    public async Task S3CompatibleInitiateMultipartUpload_WithCustomerEncryptionHeaders_ParsesAndDelegates()
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        var keyBytes = new byte[32];
        System.Security.Cryptography.RandomNumberGenerator.Fill(keyBytes);
        var key = Convert.ToBase64String(keyBytes);
        var keyMd5 = Convert.ToBase64String(System.Security.Cryptography.MD5.HashData(keyBytes));

        using var request = new HttpRequestMessage(HttpMethod.Post, "/integrated-s3/sse-c-mp-bucket/docs/encrypted.txt?uploads");
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-algorithm", "AES256");
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key", key);
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key-MD5", keyMd5);

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("AES256", Assert.Single(response.Headers.GetValues("x-amz-server-side-encryption-customer-algorithm")));
        Assert.Equal(keyMd5, Assert.Single(response.Headers.GetValues("x-amz-server-side-encryption-customer-key-MD5")));

        var initiateRequest = storageService.LastInitiateMultipartUploadRequest ?? throw new Xunit.Sdk.XunitException("Expected initiate multipart request to reach the storage service.");
        var customerEncryption = initiateRequest.CustomerEncryption ?? throw new Xunit.Sdk.XunitException("Expected customer encryption settings.");
        Assert.Equal("AES256", customerEncryption.Algorithm);
        Assert.Equal(key, customerEncryption.Key);
        Assert.Equal(keyMd5, customerEncryption.KeyMd5);
    }

    [Fact]
    public async Task S3CompatibleUploadPart_WithCustomerEncryptionHeaders_ParsesAndDelegates()
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        var keyBytes = new byte[32];
        System.Security.Cryptography.RandomNumberGenerator.Fill(keyBytes);
        var key = Convert.ToBase64String(keyBytes);
        var keyMd5 = Convert.ToBase64String(System.Security.Cryptography.MD5.HashData(keyBytes));

        using var request = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/sse-c-part-bucket/docs/encrypted.txt?uploadId=upload-123&partNumber=1")
        {
            Content = new StringContent("part data", Encoding.UTF8, "application/octet-stream")
        };
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-algorithm", "AES256");
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key", key);
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key-MD5", keyMd5);

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var uploadPartRequest = storageService.LastUploadMultipartPartRequest ?? throw new Xunit.Sdk.XunitException("Expected upload part request to reach the storage service.");
        var customerEncryption = uploadPartRequest.CustomerEncryption ?? throw new Xunit.Sdk.XunitException("Expected customer encryption settings.");
        Assert.Equal("AES256", customerEncryption.Algorithm);
        Assert.Equal(key, customerEncryption.Key);
        Assert.Equal(keyMd5, customerEncryption.KeyMd5);
    }

    [Fact]
    public async Task S3CompatibleUploadPartCopy_WithCustomerEncryptionHeaders_ParsesAndDelegates()
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        var srcKeyBytes = new byte[32];
        System.Security.Cryptography.RandomNumberGenerator.Fill(srcKeyBytes);
        var srcKey = Convert.ToBase64String(srcKeyBytes);
        var srcKeyMd5 = Convert.ToBase64String(System.Security.Cryptography.MD5.HashData(srcKeyBytes));

        var destKeyBytes = new byte[32];
        System.Security.Cryptography.RandomNumberGenerator.Fill(destKeyBytes);
        var destKey = Convert.ToBase64String(destKeyBytes);
        var destKeyMd5 = Convert.ToBase64String(System.Security.Cryptography.MD5.HashData(destKeyBytes));

        using var request = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/sse-c-partcopy-bucket/docs/copied.txt?uploadId=upload-456&partNumber=1");
        request.Headers.TryAddWithoutValidation("x-amz-copy-source", "/sse-c-partcopy-src/docs/source.txt");
        request.Headers.TryAddWithoutValidation("x-amz-copy-source-server-side-encryption-customer-algorithm", "AES256");
        request.Headers.TryAddWithoutValidation("x-amz-copy-source-server-side-encryption-customer-key", srcKey);
        request.Headers.TryAddWithoutValidation("x-amz-copy-source-server-side-encryption-customer-key-MD5", srcKeyMd5);
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-algorithm", "AES256");
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key", destKey);
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key-MD5", destKeyMd5);

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var partCopyRequest = storageService.LastUploadPartCopyRequest ?? throw new Xunit.Sdk.XunitException("Expected upload part copy request to reach the storage service.");
        var sourceEncryption = partCopyRequest.SourceCustomerEncryption ?? throw new Xunit.Sdk.XunitException("Expected source customer encryption settings.");
        Assert.Equal("AES256", sourceEncryption.Algorithm);
        Assert.Equal(srcKey, sourceEncryption.Key);
        Assert.Equal(srcKeyMd5, sourceEncryption.KeyMd5);

        var destEncryption = partCopyRequest.DestinationCustomerEncryption ?? throw new Xunit.Sdk.XunitException("Expected destination customer encryption settings.");
        Assert.Equal("AES256", destEncryption.Algorithm);
        Assert.Equal(destKey, destEncryption.Key);
        Assert.Equal(destKeyMd5, destEncryption.KeyMd5);
    }

    [Fact]
    public async Task S3CompatiblePutObject_WithTrulyUnsupportedServerSideEncryptionHeader_ReturnsNotImplemented()
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        using var request = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/sse-unsupported-bucket/docs/unsupported.txt")
        {
            Content = new StringContent("unsupported encryption request", Encoding.UTF8, "text/plain")
        };
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-bucket-key-enabled", "true");

        var response = await client.SendAsync(request);

        await AssertNotImplementedResponseAsync(response);
        Assert.Null(storageService.LastPutObjectRequest);
    }

    [Fact]
    public async Task S3CompatibleCopyObject_WithServerSideEncryptionHeaders_ParsesDestinationSettingsAndEmitsResponseHeaders()
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        using var request = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/destination-bucket/docs/copied.txt");
        request.Headers.TryAddWithoutValidation("x-amz-copy-source", "/source-bucket/docs/source.txt");
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption", "AES256");

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("AES256", Assert.Single(response.Headers.GetValues("x-amz-server-side-encryption")));

        var copyRequest = storageService.LastCopyObjectRequest ?? throw new Xunit.Sdk.XunitException("Expected COPY request to reach the storage service.");
        Assert.Null(copyRequest.SourceServerSideEncryption);
        var serverSideEncryption = copyRequest.DestinationServerSideEncryption ?? throw new Xunit.Sdk.XunitException("Expected COPY destination server-side encryption settings.");
        Assert.Equal(ObjectServerSideEncryptionAlgorithm.Aes256, serverSideEncryption.Algorithm);
    }

    [Theory]
    [InlineData("x-amz-sdk-checksum-algorithm")]
    [InlineData("x-amz-checksum-algorithm")]
    public async Task S3CompatibleCopyObject_WithChecksumAlgorithmHeader_ParsesRequestWithoutChecksumValue(string algorithmHeaderName)
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        using var request = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/checksum-copy-bucket/docs/copied.txt");
        request.Headers.TryAddWithoutValidation("x-amz-copy-source", "/checksum-source-bucket/docs/source.txt");
        request.Headers.TryAddWithoutValidation(algorithmHeaderName, "CRC32C");

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var copyRequest = storageService.LastCopyObjectRequest ?? throw new Xunit.Sdk.XunitException("Expected COPY request to reach the storage service.");
        Assert.Equal("crc32c", copyRequest.ChecksumAlgorithm);
        Assert.Null(copyRequest.Checksums);
    }

    [Fact]
    public async Task S3CompatibleCopyObject_WithChecksumHeaders_ParsesChecksumAlgorithmAndValue()
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        const string checksum = "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=";

        using var request = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/checksum-copy-bucket/docs/copied.txt");
        request.Headers.TryAddWithoutValidation("x-amz-copy-source", "/checksum-source-bucket/docs/source.txt");
        request.Headers.TryAddWithoutValidation("x-amz-sdk-checksum-algorithm", "SHA256");
        request.Headers.TryAddWithoutValidation("x-amz-checksum-sha256", checksum);

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var copyRequest = storageService.LastCopyObjectRequest ?? throw new Xunit.Sdk.XunitException("Expected COPY request to reach the storage service.");
        Assert.Equal("sha256", copyRequest.ChecksumAlgorithm);
        Assert.Equal(checksum, copyRequest.Checksums!["sha256"]);
    }

    [Theory]
    [InlineData("x-amz-sdk-checksum-algorithm")]
    [InlineData("x-amz-checksum-algorithm")]
    public async Task S3CompatibleMultipartUpload_UploadPartCopy_WithChecksumAlgorithmHeader_ParsesRequestWithoutChecksumValue(string algorithmHeaderName)
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        using var request = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/checksum-copy-bucket/docs/copied.txt?partNumber=1&uploadId=upload-123");
        request.Headers.TryAddWithoutValidation("x-amz-copy-source", "/checksum-source-bucket/docs/source.txt");
        request.Headers.TryAddWithoutValidation(algorithmHeaderName, "CRC32C");

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var copyRequest = storageService.LastUploadPartCopyRequest ?? throw new Xunit.Sdk.XunitException("Expected multipart COPY request to reach the storage service.");
        Assert.Equal("crc32c", copyRequest.ChecksumAlgorithm);
        Assert.Null(copyRequest.Checksums);
    }

    [Fact]
    public async Task S3CompatibleMultipartUpload_UploadPartCopy_WithChecksumHeaders_ParsesChecksumAlgorithmAndValue()
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        const string checksum = "2jmj7l5rSw0yVb/vlWAYkK/YBwk=";

        using var request = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/checksum-copy-bucket/docs/copied.txt?partNumber=1&uploadId=upload-123");
        request.Headers.TryAddWithoutValidation("x-amz-copy-source", "/checksum-source-bucket/docs/source.txt");
        request.Headers.TryAddWithoutValidation("x-amz-sdk-checksum-algorithm", "SHA1");
        request.Headers.TryAddWithoutValidation("x-amz-checksum-sha1", checksum);

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var copyRequest = storageService.LastUploadPartCopyRequest ?? throw new Xunit.Sdk.XunitException("Expected multipart COPY request to reach the storage service.");
        Assert.Equal("sha1", copyRequest.ChecksumAlgorithm);
        Assert.Equal(checksum, copyRequest.Checksums!["sha1"]);
    }

    [Fact]
    public async Task S3CompatibleInitiateMultipartUpload_WithServerSideEncryptionHeaders_ParsesRequest()
    {
        var storageService = new RecordingStorageService
        {
            InitiateMultipartUploadResult = new MultipartUploadInfo
            {
                BucketName = "multipart-sse-bucket",
                Key = "docs/upload.txt",
                UploadId = "upload-123",
                InitiatedAtUtc = DateTimeOffset.Parse("2026-03-01T00:00:00Z", CultureInfo.InvariantCulture),
                ServerSideEncryption = new ObjectServerSideEncryptionInfo
                {
                    Algorithm = ObjectServerSideEncryptionAlgorithm.Kms,
                    KeyId = "alias/multipart-key"
                }
            }
        };
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        var encryptionContext = Convert.ToBase64String(Encoding.UTF8.GetBytes("""{"tenant":"beta"}"""));

        using var request = new HttpRequestMessage(HttpMethod.Post, "/integrated-s3/multipart-sse-bucket/docs/upload.txt?uploads");
        request.Headers.TryAddWithoutValidation("Content-Type", "text/plain");
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption", "aws:kms");
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-context", encryptionContext);

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("aws:kms", Assert.Single(response.Headers.GetValues("x-amz-server-side-encryption")));
        Assert.Equal("alias/multipart-key", Assert.Single(response.Headers.GetValues("x-amz-server-side-encryption-aws-kms-key-id")));

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("upload-123", GetRequiredElementValue(document, "UploadId"));

        var initiateRequest = storageService.LastInitiateMultipartUploadRequest ?? throw new Xunit.Sdk.XunitException("Expected multipart initiation request to reach the storage service.");
        var serverSideEncryption = initiateRequest.ServerSideEncryption ?? throw new Xunit.Sdk.XunitException("Expected multipart initiation server-side encryption settings.");
        Assert.Equal(ObjectServerSideEncryptionAlgorithm.Kms, serverSideEncryption.Algorithm);
        Assert.Null(serverSideEncryption.KeyId);
        Assert.Equal("beta", serverSideEncryption.Context!["tenant"]);
    }

    [Theory]
    [InlineData("GET", ObjectServerSideEncryptionAlgorithm.Kms, "aws:kms")]
    [InlineData("HEAD", ObjectServerSideEncryptionAlgorithm.Kms, "aws:kms")]
    [InlineData("GET", ObjectServerSideEncryptionAlgorithm.KmsDsse, "aws:kms:dsse")]
    [InlineData("HEAD", ObjectServerSideEncryptionAlgorithm.KmsDsse, "aws:kms:dsse")]
    public async Task S3CompatibleReadObject_EmitsServerSideEncryptionHeaders(
        string method,
        ObjectServerSideEncryptionAlgorithm algorithm,
        string expectedHeaderValue)
    {
        var objectInfo = new ObjectInfo
        {
            BucketName = "sse-read-bucket",
            Key = "docs/encrypted.txt",
            ContentLength = 15,
            ContentType = "text/plain",
            ETag = "read-etag",
            LastModifiedUtc = DateTimeOffset.Parse("2026-03-01T00:00:00Z", CultureInfo.InvariantCulture),
            ServerSideEncryption = new ObjectServerSideEncryptionInfo
            {
                Algorithm = algorithm,
                KeyId = "alias/read-key"
            }
        };
        var storageService = new RecordingStorageService
        {
            GetObjectResult = new GetObjectResponse
            {
                Object = objectInfo,
                Content = new MemoryStream(Encoding.UTF8.GetBytes("sse read payload")),
                TotalContentLength = objectInfo.ContentLength
            },
            HeadObjectResult = objectInfo
        };
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        using var request = new HttpRequestMessage(new HttpMethod(method), "/integrated-s3/sse-read-bucket/docs/encrypted.txt");
        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(expectedHeaderValue, Assert.Single(response.Headers.GetValues("x-amz-server-side-encryption")));
        Assert.Equal("alias/read-key", Assert.Single(response.Headers.GetValues("x-amz-server-side-encryption-aws-kms-key-id")));

        if (string.Equals(method, HttpMethod.Get.Method, StringComparison.Ordinal)) {
            Assert.Equal("sse read payload", await response.Content.ReadAsStringAsync());
            Assert.NotNull(storageService.LastGetObjectRequest);
            Assert.Null(storageService.LastHeadObjectRequest);
        }
        else {
            Assert.Equal(string.Empty, await response.Content.ReadAsStringAsync());
            Assert.NotNull(storageService.LastHeadObjectRequest);
            Assert.Null(storageService.LastGetObjectRequest);
        }
    }

    [Theory]
    [InlineData("GET")]
    [InlineData("HEAD")]
    public async Task S3CompatibleReadObject_EmitsObjectLockHeaders(string method)
    {
        var retainUntilDateUtc = DateTimeOffset.Parse("2035-06-07T08:09:10Z", CultureInfo.InvariantCulture);
        var objectInfo = new ObjectInfo
        {
            BucketName = "object-lock-read-bucket",
            Key = "docs/locked.txt",
            ContentLength = 19,
            ContentType = "text/plain",
            ETag = "object-lock-etag",
            LastModifiedUtc = DateTimeOffset.Parse("2026-03-01T00:00:00Z", CultureInfo.InvariantCulture),
            RetentionMode = ObjectRetentionMode.Compliance,
            RetainUntilDateUtc = retainUntilDateUtc,
            LegalHoldStatus = ObjectLegalHoldStatus.On
        };
        var storageService = new RecordingStorageService
        {
            GetObjectResult = new GetObjectResponse
            {
                Object = objectInfo,
                Content = new MemoryStream(Encoding.UTF8.GetBytes("object lock payload")),
                TotalContentLength = objectInfo.ContentLength
            },
            HeadObjectResult = objectInfo
        };
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        using var request = new HttpRequestMessage(new HttpMethod(method), "/integrated-s3/object-lock-read-bucket/docs/locked.txt");
        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("COMPLIANCE", Assert.Single(response.Headers.GetValues("x-amz-object-lock-mode")));
        Assert.Equal("ON", Assert.Single(response.Headers.GetValues("x-amz-object-lock-legal-hold")));
        Assert.Equal(XmlConvert.ToString(retainUntilDateUtc.UtcDateTime, XmlDateTimeSerializationMode.Utc), Assert.Single(response.Headers.GetValues("x-amz-object-lock-retain-until-date")));

        if (string.Equals(method, HttpMethod.Get.Method, StringComparison.Ordinal)) {
            Assert.Equal("object lock payload", await response.Content.ReadAsStringAsync());
            Assert.NotNull(storageService.LastGetObjectRequest);
            Assert.Null(storageService.LastHeadObjectRequest);
        }
        else {
            Assert.Equal(string.Empty, await response.Content.ReadAsStringAsync());
            Assert.NotNull(storageService.LastHeadObjectRequest);
            Assert.Null(storageService.LastGetObjectRequest);
        }
    }

    [Theory]
    [InlineData("GET")]
    [InlineData("HEAD")]
    public async Task S3CompatibleReadObject_WithServerSideEncryptionRequestHeaders_ReturnsInvalidRequest(string method)
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        using var request = new HttpRequestMessage(new HttpMethod(method), "/integrated-s3/sse-read-bucket/docs/encrypted.txt");
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption", "AES256");

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("InvalidRequest", GetRequiredElementValue(document, "Code"));

        Assert.Null(storageService.LastGetObjectRequest);
        Assert.Null(storageService.LastHeadObjectRequest);
    }

    [Fact]
    public async Task Endpoints_RespectClaimsPrincipalDrivenAuthorization()
    {
        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(builder => {
            builder.Services.AddAuthentication("TestHeader")
                .AddScheme<AuthenticationSchemeOptions, TestHeaderAuthenticationHandler>("TestHeader", static _ => { });
            builder.Services.AddSingleton<IIntegratedS3AuthorizationService, ScopeBasedIntegratedS3AuthorizationService>();
        });

        using var client = isolatedClient.Client;

        var anonymousCreate = await client.PutAsync("/integrated-s3/buckets/secured-bucket", content: null);
        Assert.Equal(HttpStatusCode.Forbidden, anonymousCreate.StatusCode);

        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "storage.write");

        var createBucket = await client.PutAsync("/integrated-s3/buckets/secured-bucket", content: null);
        Assert.Equal(HttpStatusCode.Created, createBucket.StatusCode);

        var putObject = await client.PutAsync(
            "/integrated-s3/buckets/secured-bucket/objects/docs/secret.txt",
            new StringContent("classified", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, putObject.StatusCode);

        var writeOnlyReadAttempt = await client.GetAsync("/integrated-s3/buckets/secured-bucket/objects/docs/secret.txt");
        Assert.Equal(HttpStatusCode.Forbidden, writeOnlyReadAttempt.StatusCode);

        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "storage.read");

        var readObject = await client.GetAsync("/integrated-s3/buckets/secured-bucket/objects/docs/secret.txt");
        Assert.Equal(HttpStatusCode.OK, readObject.StatusCode);
        Assert.Equal("classified", await readObject.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task S3CompatibleBucketAcl_PublicRead_AllowsAnonymousBucketListing()
    {
        await using var isolatedClient = await CreateScopeAuthorizationIsolatedClientAsync();
        using var authClient = isolatedClient.Client;
        using var anonymousClient = isolatedClient.CreateAdditionalClient();
        var bucketName = $"bucket-acl-public-{Guid.NewGuid():N}";

        authClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "storage.write");
        Assert.Equal(HttpStatusCode.Created, (await authClient.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);
        Assert.Equal(
            HttpStatusCode.OK,
            (await authClient.PutAsync(
                $"/integrated-s3/buckets/{bucketName}/objects/docs/private.txt",
                new StringContent("bucket acl payload", Encoding.UTF8, "text/plain"))).StatusCode);

        using var putAclRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}?acl");
        putAclRequest.Headers.TryAddWithoutValidation("x-amz-acl", "public-read");
        var putAclResponse = await authClient.SendAsync(putAclRequest);
        Assert.Equal(HttpStatusCode.OK, putAclResponse.StatusCode);

        var getAclResponse = await authClient.GetAsync($"/integrated-s3/{bucketName}?acl");
        Assert.Equal(HttpStatusCode.OK, getAclResponse.StatusCode);
        var aclDocument = XDocument.Parse(await getAclResponse.Content.ReadAsStringAsync());
        Assert.Equal("AccessControlPolicy", aclDocument.Root?.Name.LocalName);
        Assert.Contains("http://acs.amazonaws.com/groups/global/AllUsers", aclDocument.Descendants("URI").Select(static element => element.Value));

        var anonymousListResponse = await anonymousClient.GetAsync($"/integrated-s3/{bucketName}?list-type=2");
        Assert.Equal(HttpStatusCode.OK, anonymousListResponse.StatusCode);
        var listDocument = XDocument.Parse(await anonymousListResponse.Content.ReadAsStringAsync());
        Assert.Equal("ListBucketResult", listDocument.Root?.Name.LocalName);

        var anonymousGetResponse = await anonymousClient.GetAsync($"/integrated-s3/{bucketName}/docs/private.txt");
        Assert.Equal(HttpStatusCode.Forbidden, anonymousGetResponse.StatusCode);
        var anonymousGetError = XDocument.Parse(await anonymousGetResponse.Content.ReadAsStringAsync());
        Assert.Equal("AccessDenied", GetRequiredElementValue(anonymousGetError, "Code"));
    }

    [Fact]
    public async Task S3CompatibleObjectAcl_PublicRead_AllowsAnonymousRead_AndOverwriteResetsToPrivate()
    {
        await using var isolatedClient = await CreateScopeAuthorizationIsolatedClientAsync();
        using var authClient = isolatedClient.Client;
        using var anonymousClient = isolatedClient.CreateAdditionalClient();
        var bucketName = $"object-acl-public-{Guid.NewGuid():N}";

        authClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "storage.write");
        Assert.Equal(HttpStatusCode.Created, (await authClient.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var putRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/docs/public.txt")
        {
            Content = new StringContent("public acl payload", Encoding.UTF8, "text/plain")
        };
        putRequest.Headers.TryAddWithoutValidation("x-amz-acl", "public-read");
        var putResponse = await authClient.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);

        using var anonymousHeadRequest = new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/{bucketName}/docs/public.txt");
        var anonymousHeadResponse = await anonymousClient.SendAsync(anonymousHeadRequest);
        Assert.Equal(HttpStatusCode.OK, anonymousHeadResponse.StatusCode);

        var anonymousGetResponse = await anonymousClient.GetAsync($"/integrated-s3/{bucketName}/docs/public.txt");
        Assert.Equal(HttpStatusCode.OK, anonymousGetResponse.StatusCode);
        Assert.Equal("public acl payload", await anonymousGetResponse.Content.ReadAsStringAsync());

        var getAclResponse = await authClient.GetAsync($"/integrated-s3/{bucketName}/docs/public.txt?acl");
        Assert.Equal(HttpStatusCode.OK, getAclResponse.StatusCode);
        var aclDocument = XDocument.Parse(await getAclResponse.Content.ReadAsStringAsync());
        Assert.Contains("http://acs.amazonaws.com/groups/global/AllUsers", aclDocument.Descendants("URI").Select(static element => element.Value));

        var overwriteResponse = await authClient.PutAsync(
            $"/integrated-s3/{bucketName}/docs/public.txt",
            new StringContent("private overwrite payload", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, overwriteResponse.StatusCode);

        var anonymousGetAfterOverwrite = await anonymousClient.GetAsync($"/integrated-s3/{bucketName}/docs/public.txt");
        Assert.Equal(HttpStatusCode.Forbidden, anonymousGetAfterOverwrite.StatusCode);
        var errorDocument = XDocument.Parse(await anonymousGetAfterOverwrite.Content.ReadAsStringAsync());
        Assert.Equal("AccessDenied", GetRequiredElementValue(errorDocument, "Code"));
    }

    [Fact]
    public async Task S3CompatibleBucketPolicy_PublicReadAndList_AllowsAnonymousReads_AndCanBeDeleted()
    {
        await using var isolatedClient = await CreateScopeAuthorizationIsolatedClientAsync();
        using var authClient = isolatedClient.Client;
        using var anonymousClient = isolatedClient.CreateAdditionalClient();
        var bucketName = $"bucket-policy-public-{Guid.NewGuid():N}";

        authClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "storage.write");
        Assert.Equal(HttpStatusCode.Created, (await authClient.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);
        Assert.Equal(
            HttpStatusCode.OK,
            (await authClient.PutAsync(
                $"/integrated-s3/buckets/{bucketName}/objects/docs/public.txt",
                new StringContent("bucket policy payload", Encoding.UTF8, "text/plain"))).StatusCode);

        var policyBody = $$"""
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::{{bucketName}}"
    },
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "*"
      },
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion"
      ],
      "Resource": "arn:aws:s3:::{{bucketName}}/*"
    }
  ]
}
""";

        using var putPolicyRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}?policy")
        {
            Content = new StringContent(policyBody, Encoding.UTF8, "application/json")
        };
        var putPolicyResponse = await authClient.SendAsync(putPolicyRequest);
        Assert.Equal(HttpStatusCode.NoContent, putPolicyResponse.StatusCode);

        var getPolicyResponse = await authClient.GetAsync($"/integrated-s3/{bucketName}?policy");
        Assert.Equal(HttpStatusCode.OK, getPolicyResponse.StatusCode);
        using var returnedPolicyDocument = JsonDocument.Parse(await getPolicyResponse.Content.ReadAsStringAsync());
        Assert.Equal(JsonValueKind.Object, returnedPolicyDocument.RootElement.ValueKind);
        Assert.True(returnedPolicyDocument.RootElement.TryGetProperty("Statement", out _));

        var anonymousListResponse = await anonymousClient.GetAsync($"/integrated-s3/{bucketName}?list-type=2");
        Assert.Equal(HttpStatusCode.OK, anonymousListResponse.StatusCode);

        var anonymousGetResponse = await anonymousClient.GetAsync($"/integrated-s3/{bucketName}/docs/public.txt");
        Assert.Equal(HttpStatusCode.OK, anonymousGetResponse.StatusCode);
        Assert.Equal("bucket policy payload", await anonymousGetResponse.Content.ReadAsStringAsync());

        var deletePolicyResponse = await authClient.DeleteAsync($"/integrated-s3/{bucketName}?policy");
        Assert.Equal(HttpStatusCode.NoContent, deletePolicyResponse.StatusCode);

        var missingPolicyResponse = await authClient.GetAsync($"/integrated-s3/{bucketName}?policy");
        Assert.Equal(HttpStatusCode.NotFound, missingPolicyResponse.StatusCode);
        var missingPolicyDocument = XDocument.Parse(await missingPolicyResponse.Content.ReadAsStringAsync());
        Assert.Equal("NoSuchBucketPolicy", GetRequiredElementValue(missingPolicyDocument, "Code"));

        var anonymousGetAfterDelete = await anonymousClient.GetAsync($"/integrated-s3/{bucketName}/docs/public.txt");
        Assert.Equal(HttpStatusCode.Forbidden, anonymousGetAfterDelete.StatusCode);
        var anonymousErrorDocument = XDocument.Parse(await anonymousGetAfterDelete.Content.ReadAsStringAsync());
        Assert.Equal("AccessDenied", GetRequiredElementValue(anonymousErrorDocument, "Code"));
    }

    [Fact]
    public async Task S3CompatibleBucketPolicy_WithCondition_ReturnsNotImplemented()
    {
        await using var isolatedClient = await CreateScopeAuthorizationIsolatedClientAsync();
        using var authClient = isolatedClient.Client;
        var bucketName = $"bucket-policy-condition-{Guid.NewGuid():N}";

        authClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "storage.write");
        Assert.Equal(HttpStatusCode.Created, (await authClient.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        var policyBody = $$"""
{
  "Version": "2012-10-17",
  "Statement": {
    "Effect": "Allow",
    "Principal": "*",
    "Action": "s3:GetObject",
    "Resource": "arn:aws:s3:::{{bucketName}}/*",
    "Condition": {
      "StringEquals": {
        "aws:PrincipalOrgID": "o-test"
      }
    }
  }
}
""";

        using var request = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}?policy")
        {
            Content = new StringContent(policyBody, Encoding.UTF8, "application/json")
        };
        var response = await authClient.SendAsync(request);

        await AssertNotImplementedResponseAsync(response);
    }

    [Fact]
    public async Task S3CompatibleObjectAcl_WithAccessControlPolicyBody_PublicReadIsSupported()
    {
        await using var isolatedClient = await CreateScopeAuthorizationIsolatedClientAsync();
        using var authClient = isolatedClient.Client;
        using var anonymousClient = isolatedClient.CreateAdditionalClient();
        var bucketName = $"object-acl-body-{Guid.NewGuid():N}";

        authClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "storage.write");
        Assert.Equal(HttpStatusCode.Created, (await authClient.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);
        Assert.Equal(
            HttpStatusCode.OK,
            (await authClient.PutAsync(
                $"/integrated-s3/buckets/{bucketName}/objects/docs/public-body.txt",
                new StringContent("body acl payload", Encoding.UTF8, "text/plain"))).StatusCode);

        using var putAclRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/docs/public-body.txt?acl")
        {
            Content = new StringContent("""
<AccessControlPolicy xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <Owner>
    <ID>ignored-owner</ID>
  </Owner>
  <AccessControlList>
    <Grant>
      <Grantee xsi:type="CanonicalUser">
        <ID>ignored-owner</ID>
      </Grantee>
      <Permission>FULL_CONTROL</Permission>
    </Grant>
    <Grant>
      <Grantee xsi:type="Group">
        <URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>
      </Grantee>
      <Permission>READ</Permission>
    </Grant>
  </AccessControlList>
</AccessControlPolicy>
""", Encoding.UTF8, "application/xml")
        };
        var putAclResponse = await authClient.SendAsync(putAclRequest);
        Assert.Equal(HttpStatusCode.OK, putAclResponse.StatusCode);

        var anonymousGetResponse = await anonymousClient.GetAsync($"/integrated-s3/{bucketName}/docs/public-body.txt");
        Assert.Equal(HttpStatusCode.OK, anonymousGetResponse.StatusCode);
        Assert.Equal("body acl payload", await anonymousGetResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task S3CompatibleObjectWrite_WithGrantHeaders_PersistsAclAndOverwriteResetsToPrivate()
    {
        await using var isolatedClient = await CreateScopeAuthorizationIsolatedClientAsync();
        using var authClient = isolatedClient.Client;
        using var anonymousClient = isolatedClient.CreateAdditionalClient();
        var bucketName = $"object-acl-grant-{Guid.NewGuid():N}";
        const string allUsersUri = "http://acs.amazonaws.com/groups/global/AllUsers";
        const string readGrantId = "grant-read-user";
        const string readAcpGrantId = "grant-read-acp-user";
        const string writeAcpGrantId = "grant-write-acp-user";
        const string fullControlGrantId = "grant-full-control-user";

        authClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "storage.write");
        Assert.Equal(HttpStatusCode.Created, (await authClient.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var request = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/docs/grants.txt")
        {
            Content = new StringContent("grant acl payload", Encoding.UTF8, "text/plain")
        };
        request.Headers.TryAddWithoutValidation("x-amz-grant-read", $"id=\"{readGrantId}\", uri=\"{allUsersUri}\"");
        request.Headers.TryAddWithoutValidation("x-amz-grant-read-acp", $"id=\"{readAcpGrantId}\"");
        request.Headers.TryAddWithoutValidation("x-amz-grant-write-acp", $"id=\"{writeAcpGrantId}\"");
        request.Headers.TryAddWithoutValidation("x-amz-grant-full-control", $"id=\"{fullControlGrantId}\"");

        var putResponse = await authClient.SendAsync(request);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);

        var anonymousGetResponse = await anonymousClient.GetAsync($"/integrated-s3/{bucketName}/docs/grants.txt");
        Assert.Equal(HttpStatusCode.OK, anonymousGetResponse.StatusCode);
        Assert.Equal("grant acl payload", await anonymousGetResponse.Content.ReadAsStringAsync());

        var getAclResponse = await authClient.GetAsync($"/integrated-s3/{bucketName}/docs/grants.txt?acl");
        Assert.Equal(HttpStatusCode.OK, getAclResponse.StatusCode);
        var aclDocument = XDocument.Parse(await getAclResponse.Content.ReadAsStringAsync());
        AssertAclGrantExists(aclDocument, "FULL_CONTROL", canonicalUserId: "integrated-s3");
        AssertAclGrantExists(aclDocument, "READ", canonicalUserId: readGrantId);
        AssertAclGrantExists(aclDocument, "READ", uri: allUsersUri);
        AssertAclGrantExists(aclDocument, "READ_ACP", canonicalUserId: readAcpGrantId);
        AssertAclGrantExists(aclDocument, "WRITE_ACP", canonicalUserId: writeAcpGrantId);
        AssertAclGrantExists(aclDocument, "FULL_CONTROL", canonicalUserId: fullControlGrantId);

        var overwriteResponse = await authClient.PutAsync(
            $"/integrated-s3/{bucketName}/docs/grants.txt",
            new StringContent("private overwrite payload", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, overwriteResponse.StatusCode);

        var anonymousGetAfterOverwrite = await anonymousClient.GetAsync($"/integrated-s3/{bucketName}/docs/grants.txt");
        Assert.Equal(HttpStatusCode.Forbidden, anonymousGetAfterOverwrite.StatusCode);
        var overwriteErrorDocument = XDocument.Parse(await anonymousGetAfterOverwrite.Content.ReadAsStringAsync());
        Assert.Equal("AccessDenied", GetRequiredElementValue(overwriteErrorDocument, "Code"));

        var getAclAfterOverwriteResponse = await authClient.GetAsync($"/integrated-s3/{bucketName}/docs/grants.txt?acl");
        Assert.Equal(HttpStatusCode.OK, getAclAfterOverwriteResponse.StatusCode);
        var aclAfterOverwrite = XDocument.Parse(await getAclAfterOverwriteResponse.Content.ReadAsStringAsync());
        Assert.Single(aclAfterOverwrite.Descendants("Grant"));
    }

    [Fact]
    public async Task S3CompatibleObjectAcl_WithGrantHeadersOnAclSubresource_PersistsAndRendersAcl()
    {
        await using var isolatedClient = await CreateScopeAuthorizationIsolatedClientAsync();
        using var authClient = isolatedClient.Client;
        using var anonymousClient = isolatedClient.CreateAdditionalClient();
        var bucketName = $"object-acl-subresource-grant-{Guid.NewGuid():N}";

        authClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "storage.write");
        Assert.Equal(HttpStatusCode.Created, (await authClient.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);
        Assert.Equal(
            HttpStatusCode.OK,
            (await authClient.PutAsync(
                $"/integrated-s3/buckets/{bucketName}/objects/docs/subresource.txt",
                new StringContent("subresource acl payload", Encoding.UTF8, "text/plain"))).StatusCode);

        using var putAclRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/docs/subresource.txt?acl");
        putAclRequest.Headers.TryAddWithoutValidation("x-amz-grant-read-acp", "id=\"subresource-read-acp\"");
        putAclRequest.Headers.TryAddWithoutValidation("x-amz-grant-write-acp", "id=\"subresource-write-acp\"");
        putAclRequest.Headers.TryAddWithoutValidation("x-amz-grant-full-control", "id=\"subresource-full-control\"");

        var putAclResponse = await authClient.SendAsync(putAclRequest);
        Assert.Equal(HttpStatusCode.OK, putAclResponse.StatusCode);

        var getAclResponse = await authClient.GetAsync($"/integrated-s3/{bucketName}/docs/subresource.txt?acl");
        Assert.Equal(HttpStatusCode.OK, getAclResponse.StatusCode);
        var aclDocument = XDocument.Parse(await getAclResponse.Content.ReadAsStringAsync());
        AssertAclGrantExists(aclDocument, "READ_ACP", canonicalUserId: "subresource-read-acp");
        AssertAclGrantExists(aclDocument, "WRITE_ACP", canonicalUserId: "subresource-write-acp");
        AssertAclGrantExists(aclDocument, "FULL_CONTROL", canonicalUserId: "subresource-full-control");
        Assert.DoesNotContain("http://acs.amazonaws.com/groups/global/AllUsers", aclDocument.Descendants("URI").Select(static element => element.Value));

        var anonymousGetResponse = await anonymousClient.GetAsync($"/integrated-s3/{bucketName}/docs/subresource.txt");
        Assert.Equal(HttpStatusCode.Forbidden, anonymousGetResponse.StatusCode);
    }

    [Fact]
    public async Task S3CompatibleObjectWrite_WithConflictingCannedAclAndGrantHeaders_ReturnsInvalidRequest()
    {
        await using var isolatedClient = await CreateScopeAuthorizationIsolatedClientAsync();
        using var authClient = isolatedClient.Client;
        var bucketName = $"object-acl-conflict-{Guid.NewGuid():N}";

        authClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "storage.write");
        Assert.Equal(HttpStatusCode.Created, (await authClient.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var request = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/docs/conflict.txt")
        {
            Content = new StringContent("conflicting acl headers", Encoding.UTF8, "text/plain")
        };
        request.Headers.TryAddWithoutValidation("x-amz-acl", "public-read");
        request.Headers.TryAddWithoutValidation("x-amz-grant-full-control", "id=\"conflict-user\"");

        var response = await authClient.SendAsync(request);

        await AssertErrorResponseAsync(response, HttpStatusCode.BadRequest, "InvalidRequest");
    }

    [Fact]
    public async Task S3CompatibleObjectWrite_WithMalformedGrantHeader_ReturnsInvalidArgument()
    {
        await using var isolatedClient = await CreateScopeAuthorizationIsolatedClientAsync();
        using var authClient = isolatedClient.Client;
        var bucketName = $"object-acl-malformed-{Guid.NewGuid():N}";

        authClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "storage.write");
        Assert.Equal(HttpStatusCode.Created, (await authClient.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        using var request = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/docs/malformed.txt")
        {
            Content = new StringContent("malformed acl grant", Encoding.UTF8, "text/plain")
        };
        request.Headers.TryAddWithoutValidation("x-amz-grant-read", "uri=http://acs.amazonaws.com/groups/global/AllUsers");

        var response = await authClient.SendAsync(request);

        await AssertErrorResponseAsync(response, HttpStatusCode.BadRequest, "InvalidArgument");
    }

    [Fact]
    public async Task S3CompatibleObjectAcl_WithUnsupportedGrantHeaderPermutation_ReturnsNotImplemented()
    {
        await using var isolatedClient = await CreateScopeAuthorizationIsolatedClientAsync();
        using var authClient = isolatedClient.Client;
        var bucketName = $"object-acl-unsupported-{Guid.NewGuid():N}";

        authClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "storage.write");
        Assert.Equal(HttpStatusCode.Created, (await authClient.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);
        Assert.Equal(
            HttpStatusCode.OK,
            (await authClient.PutAsync(
                $"/integrated-s3/buckets/{bucketName}/objects/docs/unsupported.txt",
                new StringContent("unsupported acl grant", Encoding.UTF8, "text/plain"))).StatusCode);

        using var request = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}/docs/unsupported.txt?acl");
        request.Headers.TryAddWithoutValidation("x-amz-grant-read-acp", "uri=\"http://acs.amazonaws.com/groups/global/AllUsers\"");

        var response = await authClient.SendAsync(request);

        await AssertNotImplementedResponseAsync(response);
    }

    [Fact]
    public async Task EndpointFeatureToggles_CanDisableAdminObjectAndMultipartGroups()
    {
        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(
            configureIntegratedS3Endpoints: options => {
                options.EnableAdminEndpoints = false;
                options.EnableObjectEndpoints = false;
                options.EnableMultipartEndpoints = false;
            });

        using var client = isolatedClient.Client;

        var createBucketResponse = await client.PutAsync("/integrated-s3/buckets/feature-toggle-bucket", content: null);
        Assert.Equal(HttpStatusCode.Created, createBucketResponse.StatusCode);

        var createCompatibleBucketResponse = await client.PutAsync("/integrated-s3/feature-toggle-compatible-bucket", content: null);
        Assert.Equal(HttpStatusCode.OK, createCompatibleBucketResponse.StatusCode);

        var capabilitiesResponse = await client.GetAsync("/integrated-s3/capabilities");
        Assert.Equal(HttpStatusCode.NotFound, capabilitiesResponse.StatusCode);

        var diagnosticsResponse = await client.GetAsync("/integrated-s3/admin/diagnostics");
        Assert.Equal(HttpStatusCode.NotFound, diagnosticsResponse.StatusCode);

        var repairsResponse = await client.GetAsync("/integrated-s3/admin/repairs");
        Assert.Equal(HttpStatusCode.NotFound, repairsResponse.StatusCode);

        var restObjectResponse = await client.PutAsync(
            "/integrated-s3/buckets/feature-toggle-bucket/objects/docs/blocked.txt",
            new StringContent("blocked", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.NotFound, restObjectResponse.StatusCode);

        var compatibleObjectResponse = await client.PutAsync(
            "/integrated-s3/feature-toggle-bucket/docs/blocked.txt",
            new StringContent("blocked", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.NotFound, compatibleObjectResponse.StatusCode);

        using var initiateMultipartRequest = new HttpRequestMessage(HttpMethod.Post, "/integrated-s3/feature-toggle-bucket/docs/blocked.txt?uploads");
        var multipartResponse = await client.SendAsync(initiateMultipartRequest);
        Assert.Equal(HttpStatusCode.NotFound, multipartResponse.StatusCode);

        var listMultipartResponse = await client.GetAsync("/integrated-s3/feature-toggle-compatible-bucket?uploads");
        Assert.Equal(HttpStatusCode.NotFound, listMultipartResponse.StatusCode);
    }

    [Fact]
    public async Task EndpointFeatureToggles_RespectConfiguredEndpointOptions()
    {
        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(builder => {
            builder.Services.PostConfigure<IntegratedS3EndpointConfigurationOptions>(options => {
                options.EnableAdminEndpoints = false;
                options.EnableObjectEndpoints = false;
                options.EnableMultipartEndpoints = false;
            });
        });

        using var client = isolatedClient.Client;

        var createBucketResponse = await client.PutAsync("/integrated-s3/buckets/configured-toggle-bucket", content: null);
        Assert.Equal(HttpStatusCode.Created, createBucketResponse.StatusCode);

        var capabilitiesResponse = await client.GetAsync("/integrated-s3/capabilities");
        Assert.Equal(HttpStatusCode.NotFound, capabilitiesResponse.StatusCode);

        var restObjectResponse = await client.PutAsync(
            "/integrated-s3/buckets/configured-toggle-bucket/objects/docs/blocked.txt",
            new StringContent("blocked", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.NotFound, restObjectResponse.StatusCode);

        using var initiateMultipartRequest = new HttpRequestMessage(HttpMethod.Post, "/integrated-s3/configured-toggle-bucket/docs/blocked.txt?uploads");
        var multipartResponse = await client.SendAsync(initiateMultipartRequest);
        Assert.Equal(HttpStatusCode.NotFound, multipartResponse.StatusCode);
    }

    [Fact]
    public async Task EndpointFeatureToggles_CanOverrideConfiguredOptionsAtMapTime()
    {
        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(
            builder => {
                builder.Services.PostConfigure<IntegratedS3EndpointConfigurationOptions>(options => {
                    options.EnableAdminEndpoints = false;
                    options.EnableObjectEndpoints = false;
                    options.EnableMultipartEndpoints = false;
                });
            },
            configureIntegratedS3Endpoints: options => {
                options.EnableObjectEndpoints = true;
            });

        using var client = isolatedClient.Client;

        var createBucketResponse = await client.PutAsync("/integrated-s3/buckets/override-toggle-bucket", content: null);
        Assert.Equal(HttpStatusCode.Created, createBucketResponse.StatusCode);

        var capabilitiesResponse = await client.GetAsync("/integrated-s3/capabilities");
        Assert.Equal(HttpStatusCode.NotFound, capabilitiesResponse.StatusCode);

        var putObjectResponse = await client.PutAsync(
            "/integrated-s3/buckets/override-toggle-bucket/objects/docs/enabled.txt",
            new StringContent("enabled", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, putObjectResponse.StatusCode);

        var getObjectResponse = await client.GetAsync("/integrated-s3/buckets/override-toggle-bucket/objects/docs/enabled.txt");
        Assert.Equal(HttpStatusCode.OK, getObjectResponse.StatusCode);
        Assert.Equal("enabled", await getObjectResponse.Content.ReadAsStringAsync());

        using var initiateMultipartRequest = new HttpRequestMessage(HttpMethod.Post, "/integrated-s3/override-toggle-bucket/docs/blocked.txt?uploads");
        var multipartResponse = await client.SendAsync(initiateMultipartRequest);
        Assert.Equal(HttpStatusCode.NotFound, multipartResponse.StatusCode);
    }

    [Fact]
    public async Task EndpointRouteGroupAuthorization_CanBeConfiguredThroughMappingOptions()
    {
        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(
            builder => {
                builder.Services.AddAuthentication("TestHeader")
                    .AddScheme<AuthenticationSchemeOptions, TestHeaderAuthenticationHandler>("TestHeader", static _ => { });
                builder.Services.AddAuthorization(options => {
                    options.AddPolicy("IntegratedS3Route", policy => {
                        policy.AddAuthenticationSchemes("TestHeader");
                        policy.RequireAuthenticatedUser();
                    });
                });
            },
            configureIntegratedS3Endpoints: options => {
                options.ConfigureRouteGroup = static group => group.RequireAuthorization("IntegratedS3Route");
            });

        using var client = isolatedClient.Client;

        var anonymousResponse = await client.GetAsync("/integrated-s3/capabilities");
        Assert.Equal(HttpStatusCode.Unauthorized, anonymousResponse.StatusCode);

        var anonymousDiagnosticsResponse = await client.GetAsync("/integrated-s3/admin/diagnostics");
        Assert.Equal(HttpStatusCode.Unauthorized, anonymousDiagnosticsResponse.StatusCode);

        var anonymousRepairsResponse = await client.GetAsync("/integrated-s3/admin/repairs");
        Assert.Equal(HttpStatusCode.Unauthorized, anonymousRepairsResponse.StatusCode);

        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "storage.read");

        var authenticatedResponse = await client.GetAsync("/integrated-s3/capabilities");
        Assert.Equal(HttpStatusCode.OK, authenticatedResponse.StatusCode);

        var authenticatedDiagnosticsResponse = await client.GetAsync("/integrated-s3/admin/diagnostics");
        Assert.Equal(HttpStatusCode.OK, authenticatedDiagnosticsResponse.StatusCode);

        var authenticatedRepairsResponse = await client.GetAsync("/integrated-s3/admin/repairs");
        Assert.Equal(HttpStatusCode.OK, authenticatedRepairsResponse.StatusCode);
        var diagnostics = await authenticatedDiagnosticsResponse.Content.ReadFromJsonAsync<StorageAdminDiagnostics>(JsonOptions);
        Assert.NotNull(diagnostics);
        var repairs = await authenticatedRepairsResponse.Content.ReadFromJsonAsync<StorageReplicaRepairEntry[]>(JsonOptions);
        Assert.NotNull(repairs);
        Assert.Empty(repairs!);
    }

    [Fact]
    public async Task EndpointRouteGroupAuthorization_CanBeConfiguredThroughBoundEndpointOptions()
    {
        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(
            builder => {
                builder.Services.AddAuthentication("TestHeader")
                    .AddScheme<AuthenticationSchemeOptions, TestHeaderAuthenticationHandler>("TestHeader", static _ => { });
                builder.Services.AddAuthorization(options => {
                    options.AddPolicy("IntegratedS3Route", policy => {
                        policy.AddAuthenticationSchemes("TestHeader");
                        policy.RequireAuthenticatedUser();
                    });
                });
            },
            configureConfiguration: configuration => {
                configuration.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["IntegratedS3:Endpoints:RouteAuthorization:PolicyNames:0"] = "IntegratedS3Route"
                });
            });

        using var client = isolatedClient.Client;

        var anonymousResponse = await client.GetAsync("/integrated-s3/capabilities");
        Assert.Equal(HttpStatusCode.Unauthorized, anonymousResponse.StatusCode);

        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "storage.read");

        var authenticatedResponse = await client.GetAsync("/integrated-s3/capabilities");
        Assert.Equal(HttpStatusCode.OK, authenticatedResponse.StatusCode);

        var authenticatedRepairsResponse = await client.GetAsync("/integrated-s3/admin/repairs");
        Assert.Equal(HttpStatusCode.OK, authenticatedRepairsResponse.StatusCode);
    }

    [Fact]
    public async Task EndpointFeatureRouteGroupAuthorization_CanBeConfiguredThroughFeatureRegistry()
    {
        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(
            builder => ConfigureTestHeaderRoutePolicies(
                builder.Services,
                ("IntegratedS3BucketRoute", "bucket.write"),
                ("IntegratedS3AdminRoute", "admin.read")),
            configureIntegratedS3Endpoints: options => {
                options.EnableObjectEndpoints = false;
                options.EnableMultipartEndpoints = false;
                options.SetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Bucket, static group => group.RequireAuthorization("IntegratedS3BucketRoute"));
                options.SetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Admin, static group => group.RequireAuthorization("IntegratedS3AdminRoute"));
            });

        using var client = isolatedClient.Client;

        var anonymousBucketResponse = await client.PutAsync("/integrated-s3/buckets/feature-registry-bucket", content: null);
        Assert.Equal(HttpStatusCode.Unauthorized, anonymousBucketResponse.StatusCode);

        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "bucket.write");

        var createBucketResponse = await client.PutAsync("/integrated-s3/buckets/feature-registry-bucket", content: null);
        Assert.Equal(HttpStatusCode.Created, createBucketResponse.StatusCode);

        client.DefaultRequestHeaders.Authorization = null;

        var anonymousCompatibilityBucketResponse = await client.GetAsync("/integrated-s3/feature-registry-bucket?versioning");
        Assert.Equal(HttpStatusCode.Unauthorized, anonymousCompatibilityBucketResponse.StatusCode);

        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "bucket.write");

        var compatibilityBucketResponse = await client.GetAsync("/integrated-s3/feature-registry-bucket?versioning");
        Assert.Equal(HttpStatusCode.OK, compatibilityBucketResponse.StatusCode);

        var bucketScopedAdminResponse = await client.GetAsync("/integrated-s3/admin/repairs");
        Assert.Equal(HttpStatusCode.Forbidden, bucketScopedAdminResponse.StatusCode);

        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "admin.read");

        var adminResponse = await client.GetAsync("/integrated-s3/admin/repairs");
        Assert.Equal(HttpStatusCode.OK, adminResponse.StatusCode);
    }

    [Fact]
    public async Task EndpointAuthorizationConventions_RejectConflictingAnonymousAndAuthorizedConfiguration()
    {
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => _factory.CreateIsolatedClientAsync(
            configureConfiguration: configuration => {
                configuration.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["IntegratedS3:Endpoints:RouteAuthorization:AllowAnonymous"] = "true",
                    ["IntegratedS3:Endpoints:RouteAuthorization:RequireAuthorization"] = "true"
                });
            }));

        Assert.Contains(nameof(IntegratedS3EndpointOptions.RouteAuthorization), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task EndpointFeatureRouteGroupAuthorization_CanProtectBucketObjectAndAdminEndpoints()
    {
        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(
            builder => ConfigureTestHeaderRoutePolicies(
                builder.Services,
                ("IntegratedS3CompatibilityRoute", "compat.access"),
                ("IntegratedS3BucketRoute", "bucket.write"),
                ("IntegratedS3ObjectRoute", "object.write"),
                ("IntegratedS3AdminRoute", "admin.read")),
            configureIntegratedS3Endpoints: options => {
                options.ConfigureCompatibilityRouteGroup = static group => group.RequireAuthorization("IntegratedS3CompatibilityRoute");
                options.ConfigureBucketRouteGroup = static group => group.RequireAuthorization("IntegratedS3BucketRoute");
                options.ConfigureObjectRouteGroup = static group => group.RequireAuthorization("IntegratedS3ObjectRoute");
                options.ConfigureAdminRouteGroup = static group => group.RequireAuthorization("IntegratedS3AdminRoute");
            });

        using var client = isolatedClient.Client;

        var anonymousBucketResponse = await client.PutAsync("/integrated-s3/buckets/grouped-bucket", content: null);
        Assert.Equal(HttpStatusCode.Unauthorized, anonymousBucketResponse.StatusCode);

        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "bucket.write");

        var createBucketResponse = await client.PutAsync("/integrated-s3/buckets/grouped-bucket", content: null);
        Assert.Equal(HttpStatusCode.Created, createBucketResponse.StatusCode);

        var bucketScopedObjectResponse = await client.PutAsync(
            "/integrated-s3/buckets/grouped-bucket/objects/docs/blocked.txt",
            new StringContent("blocked", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.Forbidden, bucketScopedObjectResponse.StatusCode);

        var bucketScopedAdminResponse = await client.GetAsync("/integrated-s3/capabilities");
        Assert.Equal(HttpStatusCode.Forbidden, bucketScopedAdminResponse.StatusCode);

        var bucketScopedDiagnosticsResponse = await client.GetAsync("/integrated-s3/admin/diagnostics");
        Assert.Equal(HttpStatusCode.Forbidden, bucketScopedDiagnosticsResponse.StatusCode);

        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "object.write");

        var objectScopedResponse = await client.PutAsync(
            "/integrated-s3/buckets/grouped-bucket/objects/docs/allowed.txt",
            new StringContent("allowed", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, objectScopedResponse.StatusCode);

        var objectScopedAdminResponse = await client.GetAsync("/integrated-s3/admin/repairs");
        Assert.Equal(HttpStatusCode.Forbidden, objectScopedAdminResponse.StatusCode);

        var objectScopedDiagnosticsResponse = await client.GetAsync("/integrated-s3/admin/diagnostics");
        Assert.Equal(HttpStatusCode.Forbidden, objectScopedDiagnosticsResponse.StatusCode);

        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "admin.read");

        var capabilitiesResponse = await client.GetAsync("/integrated-s3/capabilities");
        Assert.Equal(HttpStatusCode.OK, capabilitiesResponse.StatusCode);

        var diagnosticsResponse = await client.GetAsync("/integrated-s3/admin/diagnostics");
        Assert.Equal(HttpStatusCode.OK, diagnosticsResponse.StatusCode);

        var repairsResponse = await client.GetAsync("/integrated-s3/admin/repairs");
        Assert.Equal(HttpStatusCode.OK, repairsResponse.StatusCode);
    }

    [Fact]
    public async Task EndpointSharedRootRouteGroupAuthorization_UsesExplicitRootConfigurationWithoutStackingFeaturePolicies()
    {
        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(
            builder => ConfigureTestHeaderRoutePolicies(
                builder.Services,
                ("IntegratedS3RootRoute", "root.access"),
                ("IntegratedS3ServiceRoute", "service.read"),
                ("IntegratedS3BucketRoute", "bucket.write")),
            configureIntegratedS3Endpoints: options => {
                options.ConfigureRootRouteGroup = static group => group.RequireAuthorization("IntegratedS3RootRoute");
                options.ConfigureServiceRouteGroup = static group => group.RequireAuthorization("IntegratedS3ServiceRoute");
                options.ConfigureBucketRouteGroup = static group => group.RequireAuthorization("IntegratedS3BucketRoute");
            });

        using var client = isolatedClient.Client;

        var anonymousRootResponse = await client.GetAsync("/integrated-s3/");
        Assert.Equal(HttpStatusCode.Unauthorized, anonymousRootResponse.StatusCode);

        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "root.access");

        var sharedRootResponse = await client.GetAsync("/integrated-s3/");
        Assert.Equal(HttpStatusCode.OK, sharedRootResponse.StatusCode);

        var dedicatedBucketResponse = await client.PutAsync("/integrated-s3/buckets/root-route-bucket", content: null);
        Assert.Equal(HttpStatusCode.Forbidden, dedicatedBucketResponse.StatusCode);
    }

    [Fact]
    public async Task EndpointMultipartRouteGroupAuthorization_CanBeConfiguredThroughMappingOptions()
    {
        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(
            builder => ConfigureTestHeaderRoutePolicies(
                builder.Services,
                ("IntegratedS3MultipartRoute", "multipart.write")),
            configureIntegratedS3Endpoints: options => {
                options.ConfigureMultipartRouteGroup = static group => group.RequireAuthorization("IntegratedS3MultipartRoute");
            });

        using var client = isolatedClient.Client;

        var createBucketResponse = await client.PutAsync("/integrated-s3/buckets/multipart-group-bucket", content: null);
        Assert.Equal(HttpStatusCode.Created, createBucketResponse.StatusCode);

        var anonymousObjectResponse = await client.PutAsync(
            "/integrated-s3/buckets/multipart-group-bucket/objects/docs/open.txt",
            new StringContent("open", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, anonymousObjectResponse.StatusCode);

        using var anonymousMultipartRequest = new HttpRequestMessage(HttpMethod.Post, "/integrated-s3/multipart-group-bucket/docs/protected.txt?uploads");
        var anonymousMultipartResponse = await client.SendAsync(anonymousMultipartRequest);
        Assert.Equal(HttpStatusCode.Unauthorized, anonymousMultipartResponse.StatusCode);

        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "multipart.write");

        using var authorizedMultipartRequest = new HttpRequestMessage(HttpMethod.Post, "/integrated-s3/multipart-group-bucket/docs/protected.txt?uploads");
        var authorizedMultipartResponse = await client.SendAsync(authorizedMultipartRequest);
        Assert.Equal(HttpStatusCode.OK, authorizedMultipartResponse.StatusCode);
    }

    [Fact]
    public async Task EndpointSharedCompatibilityRouteGroupAuthorization_UsesExplicitCompatibilityConfigurationWithoutStackingFeaturePolicies()
    {
        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(
            builder => ConfigureTestHeaderRoutePolicies(
                builder.Services,
                ("IntegratedS3CompatibilityRoute", "compat.access"),
                ("IntegratedS3BucketRoute", "bucket.write"),
                ("IntegratedS3ObjectRoute", "object.write"),
                ("IntegratedS3MultipartRoute", "multipart.write")),
            configureIntegratedS3Endpoints: options => {
                options.ConfigureCompatibilityRouteGroup = static group => group.RequireAuthorization("IntegratedS3CompatibilityRoute");
                options.ConfigureBucketRouteGroup = static group => group.RequireAuthorization("IntegratedS3BucketRoute");
                options.ConfigureObjectRouteGroup = static group => group.RequireAuthorization("IntegratedS3ObjectRoute");
                options.ConfigureMultipartRouteGroup = static group => group.RequireAuthorization("IntegratedS3MultipartRoute");
            });

        using var client = isolatedClient.Client;

        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "bucket.write");

        var createBucketResponse = await client.PutAsync("/integrated-s3/buckets/shared-compat-route-bucket", content: null);
        Assert.Equal(HttpStatusCode.Created, createBucketResponse.StatusCode);

        client.DefaultRequestHeaders.Authorization = null;

        var anonymousCompatibleResponse = await client.PutAsync(
            "/integrated-s3/shared-compat-route-bucket/docs/anonymous.txt",
            new StringContent("anonymous", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.Unauthorized, anonymousCompatibleResponse.StatusCode);

        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "compat.access");

        var sharedCompatibilityResponse = await client.PutAsync(
            "/integrated-s3/shared-compat-route-bucket/docs/allowed.txt",
            new StringContent("allowed", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, sharedCompatibilityResponse.StatusCode);

        var dedicatedObjectResponse = await client.PutAsync(
            "/integrated-s3/buckets/shared-compat-route-bucket/objects/docs/blocked.txt",
            new StringContent("blocked", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.Forbidden, dedicatedObjectResponse.StatusCode);
    }

    [Fact]
    public async Task EndpointSharedRouteGroupAuthorization_RequiresExplicitSharedConfigurationWhenMultipleFeaturePoliciesAreConfigured()
    {
        var rootRouteException = await Assert.ThrowsAsync<InvalidOperationException>(() => _factory.CreateIsolatedClientAsync(
            builder => ConfigureTestHeaderRoutePolicies(
                builder.Services,
                ("IntegratedS3ServiceRoute", "service.read"),
                ("IntegratedS3BucketRoute", "bucket.write")),
            configureIntegratedS3Endpoints: options => {
                options.ConfigureServiceRouteGroup = static group => group.RequireAuthorization("IntegratedS3ServiceRoute");
                options.ConfigureBucketRouteGroup = static group => group.RequireAuthorization("IntegratedS3BucketRoute");
            }));

        Assert.Contains(nameof(IntegratedS3EndpointOptions.ConfigureRootRouteGroup), rootRouteException.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(IntegratedS3EndpointOptions.SetFeatureRouteGroupConfiguration), rootRouteException.Message, StringComparison.Ordinal);

        var compatibilityRouteException = await Assert.ThrowsAsync<InvalidOperationException>(() => _factory.CreateIsolatedClientAsync(
            builder => ConfigureTestHeaderRoutePolicies(
                builder.Services,
                ("IntegratedS3BucketRoute", "bucket.write"),
                ("IntegratedS3ObjectRoute", "object.write")),
            configureIntegratedS3Endpoints: options => {
                options.ConfigureBucketRouteGroup = static group => group.RequireAuthorization("IntegratedS3BucketRoute");
                options.ConfigureObjectRouteGroup = static group => group.RequireAuthorization("IntegratedS3ObjectRoute");
            }));

        Assert.Contains(nameof(IntegratedS3EndpointOptions.ConfigureCompatibilityRouteGroup), compatibilityRouteException.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(IntegratedS3EndpointOptions.SetFeatureRouteGroupConfiguration), compatibilityRouteException.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task EndpointFeatureRouteGroupAuthorization_ComposesWithWholeRouteConfiguration()
    {
        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(
            builder => ConfigureTestHeaderRoutePolicies(
                builder.Services,
                ("IntegratedS3Route", "route.access"),
                ("IntegratedS3ServiceRoute", "service.read")),
            configureIntegratedS3Endpoints: options => {
                options.EnableBucketEndpoints = false;
                options.ConfigureRouteGroup = static group => group.RequireAuthorization("IntegratedS3Route");
                options.ConfigureServiceRouteGroup = static group => group.RequireAuthorization("IntegratedS3ServiceRoute");
            });

        using var client = isolatedClient.Client;

        var anonymousResponse = await client.GetAsync("/integrated-s3/");
        Assert.Equal(HttpStatusCode.Unauthorized, anonymousResponse.StatusCode);

        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "route.access");

        var routeOnlyResponse = await client.GetAsync("/integrated-s3/");
        Assert.Equal(HttpStatusCode.Forbidden, routeOnlyResponse.StatusCode);

        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "route.access,service.read");

        var fullyAuthorizedResponse = await client.GetAsync("/integrated-s3/");
        Assert.Equal(HttpStatusCode.OK, fullyAuthorizedResponse.StatusCode);
    }

    [Fact]
    public async Task AdminDiagnosticsEndpoint_ReturnsProviderHealthReplicaLagAndRepairDiagnostics()
    {
        var observedAtUtc = DateTimeOffset.Parse("2025-02-03T04:10:06+00:00", CultureInfo.InvariantCulture);
        var backlog = new TestStorageReplicaRepairBacklog([
            CreateRepairEntry(
                id: "failed-repair",
                origin: StorageReplicaRepairOrigin.PartialWriteFailure,
                status: StorageReplicaRepairStatus.Failed,
                operation: StorageOperationType.PutObject,
                primaryBackendName: "primary-memory",
                replicaBackendName: "replica-memory",
                bucketName: "repair-bucket",
                objectKey: "docs/failed.txt",
                createdAtUtc: observedAtUtc.AddMinutes(-5),
                attemptCount: 2,
                lastError: new StorageError
                {
                    Code = StorageErrorCode.ProviderUnavailable,
                    Message = "Replica write failed.",
                    BucketName = "repair-bucket",
                    ObjectKey = "docs/failed.txt",
                    ProviderName = "replica-memory",
                    SuggestedHttpStatusCode = 503
                }),
            CreateRepairEntry(
                id: "pending-repair",
                origin: StorageReplicaRepairOrigin.AsyncReplication,
                status: StorageReplicaRepairStatus.Pending,
                operation: StorageOperationType.DeleteObject,
                primaryBackendName: "primary-memory",
                replicaBackendName: "replica-memory",
                bucketName: "repair-bucket",
                objectKey: "docs/pending.txt",
                createdAtUtc: observedAtUtc.AddMinutes(-2))
        ]);
        var primaryBackend = new PassiveDiagnosticsStorageBackend("primary-memory", isPrimary: true);
        var laggingReplicaBackend = new PassiveDiagnosticsStorageBackend("replica-memory");
        var currentReplicaBackend = new PassiveDiagnosticsStorageBackend("current-replica");

        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(builder => {
            builder.Services.RemoveAll<IStorageBackend>();
            builder.Services.RemoveAll<IStorageReplicaRepairBacklog>();
            builder.Services.RemoveAll<IStorageBackendHealthEvaluator>();
            builder.Services.RemoveAll<TimeProvider>();
            builder.Services.AddSingleton<TimeProvider>(new FixedTimeProvider(observedAtUtc));
            builder.Services.AddSingleton<IStorageBackend>(primaryBackend);
            builder.Services.AddSingleton<IStorageBackend>(laggingReplicaBackend);
            builder.Services.AddSingleton<IStorageBackend>(currentReplicaBackend);
            builder.Services.AddSingleton<IStorageReplicaRepairBacklog>(backlog);
            builder.Services.AddSingleton<IStorageBackendHealthEvaluator>(new ConfigurableStorageBackendHealthEvaluator(
                new Dictionary<string, StorageBackendHealthStatus>(StringComparer.Ordinal)
                {
                    [primaryBackend.Name] = StorageBackendHealthStatus.Healthy,
                    [laggingReplicaBackend.Name] = StorageBackendHealthStatus.Unhealthy,
                    [currentReplicaBackend.Name] = StorageBackendHealthStatus.Unknown
                }));
        });

        using var client = isolatedClient.Client;

        var diagnostics = await client.GetFromJsonAsync<StorageAdminDiagnostics>("/integrated-s3/admin/diagnostics", JsonOptions);
        Assert.NotNull(diagnostics);
        Assert.Equal(observedAtUtc, diagnostics!.ObservedAtUtc);
        Assert.Collection(
            diagnostics.Providers,
            primaryProvider => {
                Assert.Equal("primary-memory", primaryProvider.BackendName);
                Assert.Equal("test", primaryProvider.Kind);
                Assert.True(primaryProvider.IsPrimary);
                Assert.Equal(StorageProviderMode.Managed, primaryProvider.Mode);
                Assert.Equal(StorageBackendHealthStatus.Healthy, primaryProvider.HealthStatus);
                Assert.Null(primaryProvider.ReplicaLag);
            },
            laggingReplicaProvider => {
                Assert.Equal("replica-memory", laggingReplicaProvider.BackendName);
                Assert.False(laggingReplicaProvider.IsPrimary);
                Assert.Equal(StorageBackendHealthStatus.Unhealthy, laggingReplicaProvider.HealthStatus);
                var lag = Assert.IsType<StorageAdminReplicaLagDiagnostics>(laggingReplicaProvider.ReplicaLag);
                Assert.True(lag.HasOutstandingRepairs);
                Assert.False(lag.IsCurrent);
                Assert.Equal(2, lag.OutstandingRepairCount);
                Assert.Equal(1, lag.PendingRepairCount);
                Assert.Equal(0, lag.InProgressRepairCount);
                Assert.Equal(1, lag.FailedRepairCount);
                Assert.Equal(observedAtUtc.AddMinutes(-5), lag.OldestOutstandingRepairCreatedAtUtc);
                Assert.Equal(observedAtUtc.AddMinutes(-2), lag.LatestRepairActivityAtUtc);
                Assert.Equal(TimeSpan.FromMinutes(5), lag.ApproximateLag);
            },
            currentReplicaProvider => {
                Assert.Equal("current-replica", currentReplicaProvider.BackendName);
                Assert.False(currentReplicaProvider.IsPrimary);
                Assert.Equal(StorageBackendHealthStatus.Unknown, currentReplicaProvider.HealthStatus);
                var lag = Assert.IsType<StorageAdminReplicaLagDiagnostics>(currentReplicaProvider.ReplicaLag);
                Assert.False(lag.HasOutstandingRepairs);
                Assert.True(lag.IsCurrent);
                Assert.Equal(0, lag.OutstandingRepairCount);
                Assert.Equal(0, lag.PendingRepairCount);
                Assert.Equal(0, lag.InProgressRepairCount);
                Assert.Equal(0, lag.FailedRepairCount);
                Assert.Null(lag.OldestOutstandingRepairCreatedAtUtc);
                Assert.Null(lag.LatestRepairActivityAtUtc);
                Assert.Null(lag.ApproximateLag);
            });

        Assert.Equal(2, diagnostics.Repairs.OutstandingRepairCount);
        Assert.Equal(1, diagnostics.Repairs.PendingRepairCount);
        Assert.Equal(0, diagnostics.Repairs.InProgressRepairCount);
        Assert.Equal(1, diagnostics.Repairs.FailedRepairCount);
        Assert.Equal(["replica-memory"], diagnostics.Repairs.ReplicaBackendsWithOutstandingRepairs);
        Assert.Equal(observedAtUtc.AddMinutes(-5), diagnostics.Repairs.OldestOutstandingRepairCreatedAtUtc);
        Assert.Equal(observedAtUtc.AddMinutes(-2), diagnostics.Repairs.LatestRepairActivityAtUtc);
        Assert.Equal(TimeSpan.FromMinutes(5), diagnostics.Repairs.ApproximateMaxReplicaLag);
        Assert.Collection(
            diagnostics.Repairs.OutstandingRepairs,
            failedRepair => {
                Assert.Equal("failed-repair", failedRepair.Id);
                Assert.Equal(StorageReplicaRepairStatus.Failed, failedRepair.Status);
                Assert.Equal(2, failedRepair.AttemptCount);
                Assert.Equal(StorageErrorCode.ProviderUnavailable, failedRepair.LastErrorCode);
            },
            pendingRepair => {
                Assert.Equal("pending-repair", pendingRepair.Id);
                Assert.Equal(StorageReplicaRepairStatus.Pending, pendingRepair.Status);
                Assert.Equal(StorageOperationType.DeleteObject, pendingRepair.Operation);
                Assert.Null(pendingRepair.LastErrorCode);
            });
    }

    [Fact]
    public async Task AdminRepairsEndpoint_ReturnsOutstandingRepairsAndSupportsReplicaBackendFilter()
    {
        var timestamp = DateTimeOffset.Parse("2025-02-03T04:05:06+00:00", CultureInfo.InvariantCulture);
        var backlog = new TestStorageReplicaRepairBacklog([
            CreateRepairEntry(
                id: "failed-repair",
                origin: StorageReplicaRepairOrigin.PartialWriteFailure,
                status: StorageReplicaRepairStatus.Failed,
                operation: StorageOperationType.PutObject,
                primaryBackendName: "primary-memory",
                replicaBackendName: "replica-memory",
                bucketName: "repair-bucket",
                objectKey: "docs/failed.txt",
                createdAtUtc: timestamp,
                attemptCount: 1,
                lastError: new StorageError
                {
                    Code = StorageErrorCode.ProviderUnavailable,
                    Message = "Replica write failed.",
                    BucketName = "repair-bucket",
                    ObjectKey = "docs/failed.txt",
                    ProviderName = "replica-memory",
                    SuggestedHttpStatusCode = 503
                }),
            CreateRepairEntry(
                id: "pending-repair",
                origin: StorageReplicaRepairOrigin.AsyncReplication,
                status: StorageReplicaRepairStatus.Pending,
                operation: StorageOperationType.PutObject,
                primaryBackendName: "primary-memory",
                replicaBackendName: "trailing-replica",
                bucketName: "repair-bucket",
                objectKey: "docs/pending.txt",
                createdAtUtc: timestamp.AddMinutes(1)),
            CreateRepairEntry(
                id: "completed-repair",
                origin: StorageReplicaRepairOrigin.AsyncReplication,
                status: StorageReplicaRepairStatus.Completed,
                operation: StorageOperationType.PutObject,
                primaryBackendName: "primary-memory",
                replicaBackendName: "ignored-replica",
                bucketName: "repair-bucket",
                objectKey: "docs/completed.txt",
                createdAtUtc: timestamp.AddMinutes(2))
        ]);

        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(builder => {
            builder.Services.RemoveAll<IStorageReplicaRepairBacklog>();
            builder.Services.AddSingleton<IStorageReplicaRepairBacklog>(backlog);
        });

        using var client = isolatedClient.Client;

        var repairs = await client.GetFromJsonAsync<StorageReplicaRepairEntry[]>("/integrated-s3/admin/repairs", JsonOptions);
        Assert.NotNull(repairs);
        Assert.Collection(
            repairs!,
            failedRepair => {
                Assert.Equal("failed-repair", failedRepair.Id);
                Assert.Equal(StorageReplicaRepairOrigin.PartialWriteFailure, failedRepair.Origin);
                Assert.Equal(StorageReplicaRepairStatus.Failed, failedRepair.Status);
                Assert.Equal("replica-memory", failedRepair.ReplicaBackendName);
                Assert.Equal(1, failedRepair.AttemptCount);
                Assert.Equal(StorageErrorCode.ProviderUnavailable, failedRepair.LastErrorCode);
                Assert.Equal("Replica write failed.", failedRepair.LastErrorMessage);
            },
            pendingRepair => {
                Assert.Equal("pending-repair", pendingRepair.Id);
                Assert.Equal(StorageReplicaRepairOrigin.AsyncReplication, pendingRepair.Origin);
                Assert.Equal(StorageReplicaRepairStatus.Pending, pendingRepair.Status);
                Assert.Equal("trailing-replica", pendingRepair.ReplicaBackendName);
                Assert.Equal(0, pendingRepair.AttemptCount);
                Assert.Null(pendingRepair.LastErrorCode);
                Assert.Null(pendingRepair.LastErrorMessage);
            });

        var filteredRepairs = await client.GetFromJsonAsync<StorageReplicaRepairEntry[]>(
            "/integrated-s3/admin/repairs?replicaBackend=replica-memory",
            JsonOptions);
        var filteredRepair = Assert.Single(filteredRepairs!);
        Assert.Equal("failed-repair", filteredRepair.Id);
        Assert.Equal(StorageReplicaRepairStatus.Failed, filteredRepair.Status);
    }

    [Fact]
    public async Task DeleteObject_WithVersioningEnabled_ReturnsDeleteMarkerWithHeaders()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/delete-marker-bucket", content: null)).StatusCode);
        await EnableBucketVersioningAsync(client, "delete-marker-bucket");

        var putResponse = await client.PutAsync(
            "/integrated-s3/delete-marker-bucket/test.txt",
            new StringContent("content", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);

        var deleteResponse = await client.DeleteAsync("/integrated-s3/delete-marker-bucket/test.txt");
        Assert.Equal(HttpStatusCode.NoContent, deleteResponse.StatusCode);
        Assert.Equal("true", Assert.Single(deleteResponse.Headers.GetValues("x-amz-delete-marker")));
        var deleteMarkerVersionId = Assert.Single(deleteResponse.Headers.GetValues("x-amz-version-id"));
        Assert.False(string.IsNullOrWhiteSpace(deleteMarkerVersionId));
    }

    [Fact]
    public async Task DeleteObject_AfterDeleteMarkerCreation_HeadAndGetReflectDeleteMarker()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/delete-marker-response-bucket", content: null)).StatusCode);
        await EnableBucketVersioningAsync(client, "delete-marker-response-bucket");

        var putResponse = await client.PutAsync(
            "/integrated-s3/delete-marker-response-bucket/obj.txt",
            new StringContent("data", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);

        var deleteResponse = await client.DeleteAsync("/integrated-s3/delete-marker-response-bucket/obj.txt");
        Assert.Equal(HttpStatusCode.NoContent, deleteResponse.StatusCode);
        var deleteMarkerVersionId = Assert.Single(deleteResponse.Headers.GetValues("x-amz-version-id"));

        var headResponse = await client.SendAsync(new HttpRequestMessage(HttpMethod.Head, "/integrated-s3/delete-marker-response-bucket/obj.txt"));
        Assert.Equal(HttpStatusCode.NotFound, headResponse.StatusCode);
        Assert.Equal("true", Assert.Single(headResponse.Headers.GetValues("x-amz-delete-marker")));
        Assert.Equal(deleteMarkerVersionId, Assert.Single(headResponse.Headers.GetValues("x-amz-version-id")));

        var getResponse = await client.GetAsync("/integrated-s3/delete-marker-response-bucket/obj.txt");
        Assert.Equal(HttpStatusCode.NotFound, getResponse.StatusCode);
        Assert.Equal("true", Assert.Single(getResponse.Headers.GetValues("x-amz-delete-marker")));
        Assert.Equal(deleteMarkerVersionId, Assert.Single(getResponse.Headers.GetValues("x-amz-version-id")));
    }

    [Fact]
    public async Task DeleteObject_WithVersionId_RemovesSpecificVersionAndReturnsDeleteMarkerInfo()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/delete-version-bucket", content: null)).StatusCode);
        await EnableBucketVersioningAsync(client, "delete-version-bucket");

        var v1 = await client.PutAsync(
            "/integrated-s3/delete-version-bucket/file.txt",
            new StringContent("v1", Encoding.UTF8, "text/plain"));
        var v1VersionId = Assert.Single(v1.Headers.GetValues("x-amz-version-id"));

        var v2 = await client.PutAsync(
            "/integrated-s3/delete-version-bucket/file.txt",
            new StringContent("v2", Encoding.UTF8, "text/plain"));
        var v2VersionId = Assert.Single(v2.Headers.GetValues("x-amz-version-id"));

        var deleteV1Response = await client.DeleteAsync($"/integrated-s3/delete-version-bucket/file.txt?versionId={Uri.EscapeDataString(v1VersionId)}");
        Assert.Equal(HttpStatusCode.NoContent, deleteV1Response.StatusCode);
        Assert.False(deleteV1Response.Headers.Contains("x-amz-delete-marker"));
        Assert.Equal(v1VersionId, Assert.Single(deleteV1Response.Headers.GetValues("x-amz-version-id")));

        var currentGet = await client.GetAsync("/integrated-s3/delete-version-bucket/file.txt");
        Assert.Equal(HttpStatusCode.OK, currentGet.StatusCode);
        Assert.Equal("v2", await currentGet.Content.ReadAsStringAsync());
        Assert.Equal(v2VersionId, Assert.Single(currentGet.Headers.GetValues("x-amz-version-id")));
    }

    [Fact]
    public async Task GetObject_WithVersionId_ReturnsSpecificVersion()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/version-recovery-bucket", content: null)).StatusCode);
        await EnableBucketVersioningAsync(client, "version-recovery-bucket");

        var v1 = await client.PutAsync(
            "/integrated-s3/version-recovery-bucket/doc.txt",
            new StringContent("version one", Encoding.UTF8, "text/plain"));
        var v1VersionId = Assert.Single(v1.Headers.GetValues("x-amz-version-id"));

        var v2 = await client.PutAsync(
            "/integrated-s3/version-recovery-bucket/doc.txt",
            new StringContent("version two", Encoding.UTF8, "text/plain"));
        var v2VersionId = Assert.Single(v2.Headers.GetValues("x-amz-version-id"));

        var currentGet = await client.GetAsync("/integrated-s3/version-recovery-bucket/doc.txt");
        Assert.Equal(HttpStatusCode.OK, currentGet.StatusCode);
        Assert.Equal("version two", await currentGet.Content.ReadAsStringAsync());
        Assert.Equal(v2VersionId, Assert.Single(currentGet.Headers.GetValues("x-amz-version-id")));

        var v1Get = await client.GetAsync($"/integrated-s3/version-recovery-bucket/doc.txt?versionId={Uri.EscapeDataString(v1VersionId)}");
        Assert.Equal(HttpStatusCode.OK, v1Get.StatusCode);
        Assert.Equal("version one", await v1Get.Content.ReadAsStringAsync());
        Assert.Equal(v1VersionId, Assert.Single(v1Get.Headers.GetValues("x-amz-version-id")));
    }

    [Fact]
    public async Task HeadObject_WithVersionId_ReturnsVersionHeaders()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/head-version-bucket", content: null)).StatusCode);
        await EnableBucketVersioningAsync(client, "head-version-bucket");

        var v1 = await client.PutAsync(
            "/integrated-s3/head-version-bucket/content.bin",
            new StringContent("v1 binary", Encoding.UTF8, "application/octet-stream"));
        var v1VersionId = Assert.Single(v1.Headers.GetValues("x-amz-version-id"));

        var v2 = await client.PutAsync(
            "/integrated-s3/head-version-bucket/content.bin",
            new StringContent("v2 binary", Encoding.UTF8, "application/octet-stream"));
        var v2VersionId = Assert.Single(v2.Headers.GetValues("x-amz-version-id"));

        var v1Head = await client.SendAsync(new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/head-version-bucket/content.bin?versionId={Uri.EscapeDataString(v1VersionId)}"));
        Assert.Equal(HttpStatusCode.OK, v1Head.StatusCode);
        Assert.Equal(v1VersionId, Assert.Single(v1Head.Headers.GetValues("x-amz-version-id")));
        Assert.Equal("application/octet-stream", v1Head.Content.Headers.ContentType?.MediaType);

        var v2Head = await client.SendAsync(new HttpRequestMessage(HttpMethod.Head, $"/integrated-s3/head-version-bucket/content.bin?versionId={Uri.EscapeDataString(v2VersionId)}"));
        Assert.Equal(HttpStatusCode.OK, v2Head.StatusCode);
        Assert.Equal(v2VersionId, Assert.Single(v2Head.Headers.GetValues("x-amz-version-id")));
    }

    [Fact]
    public async Task GetObject_WithInvalidVersionId_ReturnsNotFound()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/invalid-version-bucket", content: null)).StatusCode);
        await EnableBucketVersioningAsync(client, "invalid-version-bucket");

        await client.PutAsync(
            "/integrated-s3/invalid-version-bucket/test.txt",
            new StringContent("data", Encoding.UTF8, "text/plain"));

        var response = await client.GetAsync("/integrated-s3/invalid-version-bucket/test.txt?versionId=nonexistent");
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        var error = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("NoSuchVersion", GetRequiredElementValue(error, "Code"));
    }

    [Fact]
    public async Task HeadObject_WithInvalidVersionId_ReturnsNotFound()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/invalid-head-version-bucket", content: null)).StatusCode);
        await EnableBucketVersioningAsync(client, "invalid-head-version-bucket");

        await client.PutAsync(
            "/integrated-s3/invalid-head-version-bucket/test.txt",
            new StringContent("data", Encoding.UTF8, "text/plain"));

        var response = await client.SendAsync(new HttpRequestMessage(HttpMethod.Head, "/integrated-s3/invalid-head-version-bucket/test.txt?versionId=invalid"));
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.Equal("NoSuchVersion", Assert.Single(response.Headers.GetValues("x-amz-error-code")));
    }

    [Fact]
    public async Task PutBucketVersioning_EnablesVersioning()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/enable-versioning-bucket", content: null)).StatusCode);

        using var enableRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/enable-versioning-bucket?versioning")
        {
            Content = new StringContent("""
<VersioningConfiguration>
  <Status>Enabled</Status>
</VersioningConfiguration>
""", Encoding.UTF8, "application/xml")
        };
        var enableResponse = await client.SendAsync(enableRequest);
        Assert.Equal(HttpStatusCode.OK, enableResponse.StatusCode);

        var putV1 = await client.PutAsync(
            "/integrated-s3/enable-versioning-bucket/file.txt",
            new StringContent("v1", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, putV1.StatusCode);
        var v1VersionId = Assert.Single(putV1.Headers.GetValues("x-amz-version-id"));

        var putV2 = await client.PutAsync(
            "/integrated-s3/enable-versioning-bucket/file.txt",
            new StringContent("v2", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, putV2.StatusCode);
        var v2VersionId = Assert.Single(putV2.Headers.GetValues("x-amz-version-id"));

        Assert.NotEqual(v1VersionId, v2VersionId);
    }

    [Fact]
    public async Task PutBucketVersioning_SuspendsVersioning()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/suspend-versioning-bucket", content: null)).StatusCode);
        await EnableBucketVersioningAsync(client, "suspend-versioning-bucket");

        var v1 = await client.PutAsync(
            "/integrated-s3/suspend-versioning-bucket/file.txt",
            new StringContent("v1", Encoding.UTF8, "text/plain"));
        var v1VersionId = Assert.Single(v1.Headers.GetValues("x-amz-version-id"));

        using var suspendRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/suspend-versioning-bucket?versioning")
        {
            Content = new StringContent("""
<VersioningConfiguration>
  <Status>Suspended</Status>
</VersioningConfiguration>
""", Encoding.UTF8, "application/xml")
        };
        var suspendResponse = await client.SendAsync(suspendRequest);
        Assert.Equal(HttpStatusCode.OK, suspendResponse.StatusCode);

        var v2 = await client.PutAsync(
            "/integrated-s3/suspend-versioning-bucket/file.txt",
            new StringContent("v2", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, v2.StatusCode);
        var v2VersionId = Assert.Single(v2.Headers.GetValues("x-amz-version-id"));

        Assert.NotEqual(v1VersionId, v2VersionId);
    }

    [Fact]
    public async Task GetBucketVersioning_ReturnsCurrentStatus()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/status-bucket", content: null)).StatusCode);

        var getDisabledResponse = await client.GetAsync("/integrated-s3/status-bucket?versioning");
        Assert.Equal(HttpStatusCode.OK, getDisabledResponse.StatusCode);
        var disabledDocument = XDocument.Parse(await getDisabledResponse.Content.ReadAsStringAsync());
        Assert.Equal("VersioningConfiguration", disabledDocument.Root?.Name.LocalName);

        await EnableBucketVersioningAsync(client, "status-bucket");

        var getEnabledResponse = await client.GetAsync("/integrated-s3/status-bucket?versioning");
        Assert.Equal(HttpStatusCode.OK, getEnabledResponse.StatusCode);
        var enabledDocument = XDocument.Parse(await getEnabledResponse.Content.ReadAsStringAsync());
        Assert.Equal("Enabled", enabledDocument.Root?.Element("Status")?.Value);

        using var suspendRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/status-bucket?versioning")
        {
            Content = new StringContent("""
<VersioningConfiguration>
  <Status>Suspended</Status>
</VersioningConfiguration>
""", Encoding.UTF8, "application/xml")
        };
        await client.SendAsync(suspendRequest);

        var getSuspendedResponse = await client.GetAsync("/integrated-s3/status-bucket?versioning");
        Assert.Equal(HttpStatusCode.OK, getSuspendedResponse.StatusCode);
        var suspendedDocument = XDocument.Parse(await getSuspendedResponse.Content.ReadAsStringAsync());
        Assert.Equal("Suspended", suspendedDocument.Root?.Element("Status")?.Value);
    }

    [Fact]
    public async Task ListObjectVersions_ReturnsAllVersionsAndDeleteMarkers()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/list-versions-bucket", content: null)).StatusCode);
        await EnableBucketVersioningAsync(client, "list-versions-bucket");

        var v1 = await client.PutAsync(
            "/integrated-s3/list-versions-bucket/docs/file.txt",
            new StringContent("v1", Encoding.UTF8, "text/plain"));
        var v1VersionId = Assert.Single(v1.Headers.GetValues("x-amz-version-id"));

        var v2 = await client.PutAsync(
            "/integrated-s3/list-versions-bucket/docs/file.txt",
            new StringContent("v2", Encoding.UTF8, "text/plain"));
        var v2VersionId = Assert.Single(v2.Headers.GetValues("x-amz-version-id"));

        var deleteResponse = await client.DeleteAsync("/integrated-s3/list-versions-bucket/docs/file.txt");
        var deleteMarkerVersionId = Assert.Single(deleteResponse.Headers.GetValues("x-amz-version-id"));

        var listResponse = await client.GetAsync("/integrated-s3/list-versions-bucket?versions");
        Assert.Equal(HttpStatusCode.OK, listResponse.StatusCode);
        var versionsDocument = XDocument.Parse(await listResponse.Content.ReadAsStringAsync());

        var versions = versionsDocument.Root!.Elements("Version").ToArray();
        var deleteMarkers = versionsDocument.Root!.Elements("DeleteMarker").ToArray();

        Assert.Equal(2, versions.Length);
        Assert.Single(deleteMarkers);

        Assert.Contains(versions, v => v.Element("VersionId")?.Value == v1VersionId);
        Assert.Contains(versions, v => v.Element("VersionId")?.Value == v2VersionId);
        Assert.Equal(deleteMarkerVersionId, Assert.Single(deleteMarkers).Element("VersionId")?.Value);
        Assert.Equal("true", Assert.Single(deleteMarkers).Element("IsLatest")?.Value);
    }

    [Fact]
    public async Task ListObjectVersions_WithPrefix_FiltersResults()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/prefix-versions-bucket", content: null)).StatusCode);
        await EnableBucketVersioningAsync(client, "prefix-versions-bucket");

        await client.PutAsync(
            "/integrated-s3/prefix-versions-bucket/docs/a.txt",
            new StringContent("data", Encoding.UTF8, "text/plain"));

        await client.PutAsync(
            "/integrated-s3/prefix-versions-bucket/docs/b.txt",
            new StringContent("data", Encoding.UTF8, "text/plain"));

        await client.PutAsync(
            "/integrated-s3/prefix-versions-bucket/other/c.txt",
            new StringContent("data", Encoding.UTF8, "text/plain"));

        var listDocsResponse = await client.GetAsync("/integrated-s3/prefix-versions-bucket?versions&prefix=docs/");
        Assert.Equal(HttpStatusCode.OK, listDocsResponse.StatusCode);
        var docsDocument = XDocument.Parse(await listDocsResponse.Content.ReadAsStringAsync());
        var docsVersions = docsDocument.Root!.Elements("Version").ToArray();

        Assert.Equal(2, docsVersions.Length);
        Assert.Contains(docsVersions, v => v.Element("Key")?.Value == "docs/a.txt");
        Assert.Contains(docsVersions, v => v.Element("Key")?.Value == "docs/b.txt");

        var listOtherResponse = await client.GetAsync("/integrated-s3/prefix-versions-bucket?versions&prefix=other/");
        Assert.Equal(HttpStatusCode.OK, listOtherResponse.StatusCode);
        var otherDocument = XDocument.Parse(await listOtherResponse.Content.ReadAsStringAsync());
        var otherVersions = otherDocument.Root!.Elements("Version").ToArray();

        Assert.Single(otherVersions);
        Assert.Equal("other/c.txt", Assert.Single(otherVersions).Element("Key")?.Value);
    }

    [Fact]
    public async Task ListObjectVersions_WithKeyAndVersionMarkers_PaginatesCorrectly()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/pagination-versions-bucket", content: null)).StatusCode);
        await EnableBucketVersioningAsync(client, "pagination-versions-bucket");

        var v1 = await client.PutAsync(
            "/integrated-s3/pagination-versions-bucket/file.txt",
            new StringContent("v1", Encoding.UTF8, "text/plain"));
        var v1VersionId = Assert.Single(v1.Headers.GetValues("x-amz-version-id"));

        var v2 = await client.PutAsync(
            "/integrated-s3/pagination-versions-bucket/file.txt",
            new StringContent("v2", Encoding.UTF8, "text/plain"));
        var v2VersionId = Assert.Single(v2.Headers.GetValues("x-amz-version-id"));

        var v3 = await client.PutAsync(
            "/integrated-s3/pagination-versions-bucket/file.txt",
            new StringContent("v3", Encoding.UTF8, "text/plain"));
        var v3VersionId = Assert.Single(v3.Headers.GetValues("x-amz-version-id"));

        var listAllResponse = await client.GetAsync("/integrated-s3/pagination-versions-bucket?versions");
        Assert.Equal(HttpStatusCode.OK, listAllResponse.StatusCode);
        var allDocument = XDocument.Parse(await listAllResponse.Content.ReadAsStringAsync());
        var allVersions = allDocument.Root!.Elements("Version").ToArray();

        Assert.Equal(3, allVersions.Length);
    }

    [Fact]
    public async Task ListObjectVersions_DeleteMarkerNotInGetResponse_OnlyInVersionsList()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/delete-marker-versions-bucket", content: null)).StatusCode);
        await EnableBucketVersioningAsync(client, "delete-marker-versions-bucket");

        var putResponse = await client.PutAsync(
            "/integrated-s3/delete-marker-versions-bucket/file.txt",
            new StringContent("content", Encoding.UTF8, "text/plain"));
        var versionId = Assert.Single(putResponse.Headers.GetValues("x-amz-version-id"));

        var deleteResponse = await client.DeleteAsync("/integrated-s3/delete-marker-versions-bucket/file.txt");
        Assert.Equal(HttpStatusCode.NoContent, deleteResponse.StatusCode);
        var deleteMarkerVersionId = Assert.Single(deleteResponse.Headers.GetValues("x-amz-version-id"));

        var listResponse = await client.GetAsync("/integrated-s3/delete-marker-versions-bucket?versions");
        Assert.Equal(HttpStatusCode.OK, listResponse.StatusCode);
        var versionsDocument = XDocument.Parse(await listResponse.Content.ReadAsStringAsync());

        var versions = versionsDocument.Root!.Elements("Version").ToArray();
        var deleteMarkers = versionsDocument.Root!.Elements("DeleteMarker").ToArray();

        Assert.Single(versions);
        Assert.Equal(versionId, Assert.Single(versions).Element("VersionId")?.Value);
        Assert.Equal("false", Assert.Single(versions).Element("IsLatest")?.Value);

        Assert.Single(deleteMarkers);
        Assert.Equal(deleteMarkerVersionId, Assert.Single(deleteMarkers).Element("VersionId")?.Value);
        Assert.Equal("true", Assert.Single(deleteMarkers).Element("IsLatest")?.Value);
    }

    private sealed class ScopeBasedIntegratedS3AuthorizationService : IIntegratedS3AuthorizationService
    {
        public ValueTask<StorageResult> AuthorizeAsync(ClaimsPrincipal principal, StorageAuthorizationRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var requiredScope = request.Operation switch
            {
                StorageOperationType.ListBuckets => "storage.read",
                StorageOperationType.HeadBucket => "storage.read",
                StorageOperationType.ListObjects => "storage.read",
                StorageOperationType.ListObjectVersions => "storage.read",
                StorageOperationType.ListMultipartUploads => "storage.read",
                StorageOperationType.ListMultipartParts => "storage.read",
                StorageOperationType.GetObject => "storage.read",
                StorageOperationType.PresignGetObject => "storage.read",
                StorageOperationType.GetBucketLocation => "storage.read",
                StorageOperationType.GetBucketCors => "storage.read",
                StorageOperationType.GetBucketDefaultEncryption => "storage.read",
                StorageOperationType.GetObjectTags => "storage.read",
                StorageOperationType.HeadObject => "storage.read",
                StorageOperationType.PresignPutObject => "storage.write",
                _ => "storage.write"
            };

            if (principal.HasClaim("scope", requiredScope)) {
                return ValueTask.FromResult(StorageResult.Success());
            }

            return ValueTask.FromResult(StorageResult.Failure(new StorageError
            {
                Code = StorageErrorCode.AccessDenied,
                Message = $"Missing required scope '{requiredScope}'.",
                BucketName = request.BucketName,
                ObjectKey = request.Key,
                SuggestedHttpStatusCode = 403
            }));
        }
    }

    private Task<WebUiApplicationFactory.IsolatedWebUiClient> CreateScopeAuthorizationIsolatedClientAsync()
    {
        return _factory.CreateIsolatedClientAsync(builder => {
            builder.Services.AddAuthentication("TestHeader")
                .AddScheme<AuthenticationSchemeOptions, TestHeaderAuthenticationHandler>("TestHeader", static _ => { });
            builder.Services.AddSingleton<IIntegratedS3AuthorizationService, ScopeBasedIntegratedS3AuthorizationService>();
        });
    }

    private Task<WebUiApplicationFactory.IsolatedWebUiClient> CreateSigV4AuthenticatedClientAsync(string accessKeyId, string secretAccessKey)
    {
        return _factory.CreateIsolatedClientAsync(builder => {
            builder.Services.Configure<IntegratedS3Options>(options => {
                options.EnableAwsSignatureV4Authentication = true;
                options.AccessKeyCredentials =
                [
                    new IntegratedS3AccessKeyCredential
                    {
                        AccessKeyId = accessKeyId,
                        SecretAccessKey = secretAccessKey,
                        DisplayName = "sigv4-http-trailer-user",
                        Scopes = ["storage.read", "storage.write"]
                    }
                ];
            });
            builder.Services.AddSingleton<IIntegratedS3AuthorizationService, ScopeBasedIntegratedS3AuthorizationService>();
        });
    }

    private sealed class ConfigurableStorageBackendHealthEvaluator(IReadOnlyDictionary<string, StorageBackendHealthStatus> statuses) : IStorageBackendHealthEvaluator
    {
        public ValueTask<StorageBackendHealthStatus> GetStatusAsync(IStorageBackend backend, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(statuses.TryGetValue(backend.Name, out var status)
                ? status
                : StorageBackendHealthStatus.Healthy);
        }
    }

    private sealed class FixedTimeProvider(DateTimeOffset fixedNow) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => fixedNow;
    }

    private Task<WebUiApplicationFactory.IsolatedWebUiClient> CreateStorageServiceIsolatedClientAsync(IStorageService storageService)
    {
        ArgumentNullException.ThrowIfNull(storageService);

        return _factory.CreateIsolatedClientAsync(builder => {
            builder.Services.RemoveAll<IStorageService>();
            builder.Services.AddSingleton(storageService);
        });
    }

    private sealed class RecordingStorageService : IStorageService
    {
        private static readonly DateTimeOffset DefaultTimestampUtc = DateTimeOffset.Parse("2026-03-01T00:00:00Z", CultureInfo.InvariantCulture);

        public PutObjectRequest? LastPutObjectRequest { get; private set; }

        public CopyObjectRequest? LastCopyObjectRequest { get; private set; }

        public GetObjectRequest? LastGetObjectRequest { get; private set; }

        public HeadObjectRequest? LastHeadObjectRequest { get; private set; }

        public GetObjectRetentionRequest? LastGetObjectRetentionRequest { get; private set; }

        public GetObjectLegalHoldRequest? LastGetObjectLegalHoldRequest { get; private set; }

        public InitiateMultipartUploadRequest? LastInitiateMultipartUploadRequest { get; private set; }

        public UploadPartCopyRequest? LastUploadPartCopyRequest { get; private set; }

        public PutBucketDefaultEncryptionRequest? LastPutBucketDefaultEncryptionRequest { get; private set; }

        public DeleteBucketDefaultEncryptionRequest? LastDeleteBucketDefaultEncryptionRequest { get; private set; }

        public ObjectInfo? PutObjectResult { get; set; }

        public ObjectInfo? CopyObjectResult { get; set; }

        public GetObjectResponse? GetObjectResult { get; set; }

        public ObjectInfo? HeadObjectResult { get; set; }

        public ObjectRetentionInfo? GetObjectRetentionResult { get; set; }

        public ObjectLegalHoldInfo? GetObjectLegalHoldResult { get; set; }

        public MultipartUploadInfo? InitiateMultipartUploadResult { get; set; }

        public MultipartUploadPart? UploadPartCopyResult { get; set; }

        private readonly Dictionary<string, BucketDefaultEncryptionConfiguration> _bucketDefaultEncryptions = new(StringComparer.Ordinal);

        public IAsyncEnumerable<BucketInfo> ListBucketsAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<BucketInfo>> CreateBucketAsync(CreateBucketRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<BucketLocationInfo>> GetBucketLocationAsync(string bucketName, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<BucketVersioningInfo>> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<BucketVersioningInfo>> PutBucketVersioningAsync(PutBucketVersioningRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<BucketCorsConfiguration>> GetBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<BucketCorsConfiguration>> PutBucketCorsAsync(PutBucketCorsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult> DeleteBucketCorsAsync(DeleteBucketCorsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<BucketDefaultEncryptionConfiguration>> GetBucketDefaultEncryptionAsync(string bucketName, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            return ValueTask.FromResult(_bucketDefaultEncryptions.TryGetValue(bucketName, out var configuration)
                ? StorageResult<BucketDefaultEncryptionConfiguration>.Success(CloneBucketDefaultEncryptionConfiguration(configuration))
                : StorageResult<BucketDefaultEncryptionConfiguration>.Failure(CreateBucketDefaultEncryptionNotFound(bucketName)));
        }

        public ValueTask<StorageResult<BucketDefaultEncryptionConfiguration>> PutBucketDefaultEncryptionAsync(PutBucketDefaultEncryptionRequest request, CancellationToken cancellationToken = default)
        {
            LastPutBucketDefaultEncryptionRequest = request;

            var configuration = new BucketDefaultEncryptionConfiguration
            {
                BucketName = request.BucketName,
                Rule = CloneBucketDefaultEncryptionRule(request.Rule)
            };

            _bucketDefaultEncryptions[request.BucketName] = configuration;
            return ValueTask.FromResult(StorageResult<BucketDefaultEncryptionConfiguration>.Success(CloneBucketDefaultEncryptionConfiguration(configuration)));
        }

        public ValueTask<StorageResult> DeleteBucketDefaultEncryptionAsync(DeleteBucketDefaultEncryptionRequest request, CancellationToken cancellationToken = default)
        {
            LastDeleteBucketDefaultEncryptionRequest = request;
            _bucketDefaultEncryptions.Remove(request.BucketName);
            return ValueTask.FromResult(StorageResult.Success());
        }

        public ValueTask<StorageResult<BucketInfo>> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult> DeleteBucketAsync(DeleteBucketRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public IAsyncEnumerable<ObjectInfo> ListObjectsAsync(ListObjectsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(ListObjectVersionsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public IAsyncEnumerable<MultipartUploadInfo> ListMultipartUploadsAsync(ListMultipartUploadsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public IAsyncEnumerable<MultipartUploadPart> ListMultipartPartsAsync(ListMultipartPartsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<GetObjectResponse>> GetObjectAsync(GetObjectRequest request, CancellationToken cancellationToken = default)
        {
            LastGetObjectRequest = request;

            return new ValueTask<StorageResult<GetObjectResponse>>(StorageResult<GetObjectResponse>.Success(
                GetObjectResult ?? CreateGetObjectResponse(request.BucketName, request.Key, "recorded response", serverSideEncryption: null,
                    customerEncryption: ToCustomerEncryptionInfo(request.CustomerEncryption))));
        }

        public ValueTask<StorageResult<ObjectRetentionInfo>> GetObjectRetentionAsync(GetObjectRetentionRequest request, CancellationToken cancellationToken = default)
        {
            LastGetObjectRetentionRequest = request;

            return new ValueTask<StorageResult<ObjectRetentionInfo>>(StorageResult<ObjectRetentionInfo>.Success(
                GetObjectRetentionResult ?? new ObjectRetentionInfo
                {
                    BucketName = request.BucketName,
                    Key = request.Key,
                    VersionId = request.VersionId
                }));
        }

        public ValueTask<StorageResult<ObjectLegalHoldInfo>> GetObjectLegalHoldAsync(GetObjectLegalHoldRequest request, CancellationToken cancellationToken = default)
        {
            LastGetObjectLegalHoldRequest = request;

            return new ValueTask<StorageResult<ObjectLegalHoldInfo>>(StorageResult<ObjectLegalHoldInfo>.Success(
                GetObjectLegalHoldResult ?? new ObjectLegalHoldInfo
                {
                    BucketName = request.BucketName,
                    Key = request.Key,
                    VersionId = request.VersionId
                }));
        }

        public ValueTask<StorageResult<ObjectTagSet>> GetObjectTagsAsync(GetObjectTagsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<ObjectInfo>> CopyObjectAsync(CopyObjectRequest request, CancellationToken cancellationToken = default)
        {
            LastCopyObjectRequest = request;

            return new ValueTask<StorageResult<ObjectInfo>>(StorageResult<ObjectInfo>.Success(
                CopyObjectResult ?? CreateObjectInfo(
                    request.DestinationBucketName,
                    request.DestinationKey,
                    contentType: null,
                    ToServerSideEncryptionInfo(request.DestinationServerSideEncryption),
                    customerEncryption: ToCustomerEncryptionInfo(request.DestinationCustomerEncryption))));
        }

        public ValueTask<StorageResult<ObjectInfo>> PutObjectAsync(PutObjectRequest request, CancellationToken cancellationToken = default)
        {
            LastPutObjectRequest = request;

            return new ValueTask<StorageResult<ObjectInfo>>(StorageResult<ObjectInfo>.Success(
                PutObjectResult ?? CreateObjectInfo(
                    request.BucketName,
                    request.Key,
                    request.ContentType,
                    ToServerSideEncryptionInfo(request.ServerSideEncryption),
                    customerEncryption: ToCustomerEncryptionInfo(request.CustomerEncryption))));
        }

        public ValueTask<StorageResult<ObjectTagSet>> PutObjectTagsAsync(PutObjectTagsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<ObjectTagSet>> DeleteObjectTagsAsync(DeleteObjectTagsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<MultipartUploadInfo>> InitiateMultipartUploadAsync(InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default)
        {
            LastInitiateMultipartUploadRequest = request;

            return new ValueTask<StorageResult<MultipartUploadInfo>>(StorageResult<MultipartUploadInfo>.Success(
                InitiateMultipartUploadResult ?? new MultipartUploadInfo
                {
                    BucketName = request.BucketName,
                    Key = request.Key,
                    UploadId = "upload-123",
                    InitiatedAtUtc = DefaultTimestampUtc,
                    ChecksumAlgorithm = request.ChecksumAlgorithm,
                    CustomerEncryption = ToCustomerEncryptionInfo(request.CustomerEncryption)
                }));
        }

        public UploadMultipartPartRequest? LastUploadMultipartPartRequest { get; private set; }

        public ValueTask<StorageResult<MultipartUploadPart>> UploadMultipartPartAsync(UploadMultipartPartRequest request, CancellationToken cancellationToken = default)
        {
            LastUploadMultipartPartRequest = request;

            return new ValueTask<StorageResult<MultipartUploadPart>>(StorageResult<MultipartUploadPart>.Success(
                new MultipartUploadPart
                {
                    PartNumber = request.PartNumber,
                    ETag = "\"recorded-upload-part\"",
                    ContentLength = request.ContentLength ?? 0,
                    LastModifiedUtc = DefaultTimestampUtc
                }));
        }

        public ValueTask<StorageResult<MultipartUploadPart>> UploadPartCopyAsync(UploadPartCopyRequest request, CancellationToken cancellationToken = default)
        {
            LastUploadPartCopyRequest = request;

            return new ValueTask<StorageResult<MultipartUploadPart>>(StorageResult<MultipartUploadPart>.Success(
                UploadPartCopyResult ?? new MultipartUploadPart
                {
                    PartNumber = request.PartNumber,
                    ETag = "\"recorded-copy-part\"",
                    ContentLength = request.SourceRange is { Start: { } start, End: { } end }
                        ? end - start + 1
                        : 0,
                    LastModifiedUtc = DefaultTimestampUtc
                }));
        }

        public ValueTask<StorageResult<ObjectInfo>> CompleteMultipartUploadAsync(CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult> AbortMultipartUploadAsync(AbortMultipartUploadRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<ObjectInfo>> HeadObjectAsync(HeadObjectRequest request, CancellationToken cancellationToken = default)
        {
            LastHeadObjectRequest = request;

            return new ValueTask<StorageResult<ObjectInfo>>(StorageResult<ObjectInfo>.Success(
                HeadObjectResult ?? CreateObjectInfo(request.BucketName, request.Key, contentType: null, serverSideEncryption: null,
                    customerEncryption: ToCustomerEncryptionInfo(request.CustomerEncryption))));
        }

        public ValueTask<StorageResult<DeleteObjectResult>> DeleteObjectAsync(DeleteObjectRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        private static GetObjectResponse CreateGetObjectResponse(string bucketName, string key, string payload, ObjectServerSideEncryptionInfo? serverSideEncryption, ObjectCustomerEncryptionInfo? customerEncryption = null)
        {
            var payloadBytes = Encoding.UTF8.GetBytes(payload);
            return new GetObjectResponse
            {
                Object = CreateObjectInfo(bucketName, key, "text/plain", serverSideEncryption, payloadBytes.Length, customerEncryption),
                Content = new MemoryStream(payloadBytes),
                TotalContentLength = payloadBytes.Length
            };
        }

        private static ObjectInfo CreateObjectInfo(string bucketName, string key, string? contentType, ObjectServerSideEncryptionInfo? serverSideEncryption, long contentLength = 0, ObjectCustomerEncryptionInfo? customerEncryption = null)
        {
            return new ObjectInfo
            {
                BucketName = bucketName,
                Key = key,
                ContentLength = contentLength,
                ContentType = contentType,
                ETag = "recording-etag",
                LastModifiedUtc = DefaultTimestampUtc,
                ServerSideEncryption = serverSideEncryption,
                CustomerEncryption = customerEncryption
            };
        }

        private static BucketDefaultEncryptionConfiguration CloneBucketDefaultEncryptionConfiguration(BucketDefaultEncryptionConfiguration configuration)
        {
            return new BucketDefaultEncryptionConfiguration
            {
                BucketName = configuration.BucketName,
                Rule = CloneBucketDefaultEncryptionRule(configuration.Rule)
            };
        }

        private static BucketDefaultEncryptionRule CloneBucketDefaultEncryptionRule(BucketDefaultEncryptionRule rule)
        {
            return new BucketDefaultEncryptionRule
            {
                Algorithm = rule.Algorithm,
                KeyId = rule.KeyId
            };
        }

        private static StorageError CreateBucketDefaultEncryptionNotFound(string bucketName)
        {
            return new StorageError
            {
                Code = StorageErrorCode.BucketEncryptionConfigurationNotFound,
                Message = $"Bucket '{bucketName}' does not have a default encryption configuration.",
                BucketName = bucketName,
                SuggestedHttpStatusCode = 404
            };
        }

        private static ObjectServerSideEncryptionInfo? ToServerSideEncryptionInfo(ObjectServerSideEncryptionSettings? settings)
        {
            return settings is null
                ? null
                : new ObjectServerSideEncryptionInfo
                {
                    Algorithm = settings.Algorithm,
                    KeyId = settings.KeyId
            };
        }

        private static ObjectCustomerEncryptionInfo? ToCustomerEncryptionInfo(ObjectCustomerEncryptionSettings? settings)
        {
            return settings is null
                ? null
                : new ObjectCustomerEncryptionInfo
                {
                    Algorithm = settings.Algorithm,
                    KeyMd5 = settings.KeyMd5
                };
        }
    }

    private sealed class PassiveDiagnosticsStorageBackend(string name, bool isPrimary = false) : IStorageBackend
    {
        public string Name => name;

        public string Kind => "test";

        public bool IsPrimary => isPrimary;

        public string? Description => $"Passive diagnostics backend '{name}'.";

        public ValueTask<StorageCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(new StorageCapabilities());
        }

        public ValueTask<StorageSupportStateDescriptor> GetSupportStateDescriptorAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(new StorageSupportStateDescriptor());
        }

        public ValueTask<StorageProviderMode> GetProviderModeAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageProviderMode.Managed);
        }

        public ValueTask<StorageObjectLocationDescriptor> GetObjectLocationDescriptorAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(new StorageObjectLocationDescriptor());
        }

        public ValueTask<StorageResult<StorageDirectObjectAccessGrant>> PresignObjectDirectAsync(
            StorageDirectObjectAccessRequest request,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(request);
            cancellationToken.ThrowIfCancellationRequested();

            return ValueTask.FromResult(StorageResult<StorageDirectObjectAccessGrant>.Failure(
                StorageError.Unsupported(
                    "Direct object presign generation is not implemented by this storage backend.",
                    request.BucketName,
                    request.Key)));
        }

        public async IAsyncEnumerable<BucketInfo> ListBucketsAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }

        public ValueTask<StorageResult<BucketInfo>> CreateBucketAsync(CreateBucketRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync<BucketInfo>();

        public ValueTask<StorageResult<BucketVersioningInfo>> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default) => UnexpectedAsync<BucketVersioningInfo>();

        public ValueTask<StorageResult<BucketVersioningInfo>> PutBucketVersioningAsync(PutBucketVersioningRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync<BucketVersioningInfo>();

        public ValueTask<StorageResult<BucketInfo>> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default) => UnexpectedAsync<BucketInfo>();

        public ValueTask<StorageResult> DeleteBucketAsync(DeleteBucketRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync();

        public async IAsyncEnumerable<ObjectInfo> ListObjectsAsync(ListObjectsRequest request, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }

        public async IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(ListObjectVersionsRequest request, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }

        public ValueTask<StorageResult<GetObjectResponse>> GetObjectAsync(GetObjectRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync<GetObjectResponse>();

        public ValueTask<StorageResult<ObjectTagSet>> GetObjectTagsAsync(GetObjectTagsRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync<ObjectTagSet>();

        public ValueTask<StorageResult<ObjectInfo>> CopyObjectAsync(CopyObjectRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync<ObjectInfo>();

        public ValueTask<StorageResult<ObjectInfo>> PutObjectAsync(PutObjectRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync<ObjectInfo>();

        public ValueTask<StorageResult<ObjectTagSet>> PutObjectTagsAsync(PutObjectTagsRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync<ObjectTagSet>();

        public ValueTask<StorageResult<ObjectTagSet>> DeleteObjectTagsAsync(DeleteObjectTagsRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync<ObjectTagSet>();

        public ValueTask<StorageResult<MultipartUploadInfo>> InitiateMultipartUploadAsync(InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync<MultipartUploadInfo>();

        public ValueTask<StorageResult<MultipartUploadPart>> UploadMultipartPartAsync(UploadMultipartPartRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync<MultipartUploadPart>();

        public ValueTask<StorageResult<ObjectInfo>> CompleteMultipartUploadAsync(CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync<ObjectInfo>();

        public ValueTask<StorageResult> AbortMultipartUploadAsync(AbortMultipartUploadRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync();

        public ValueTask<StorageResult<ObjectInfo>> HeadObjectAsync(HeadObjectRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync<ObjectInfo>();

        public ValueTask<StorageResult<DeleteObjectResult>> DeleteObjectAsync(DeleteObjectRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync<DeleteObjectResult>();

        public ValueTask<StorageResult<BucketCorsConfiguration>> GetBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult<BucketCorsConfiguration>.Failure(StorageError.Unsupported("Bucket CORS is not used in admin diagnostics tests.", bucketName)));
        }

        public ValueTask<StorageResult<BucketCorsConfiguration>> PutBucketCorsAsync(PutBucketCorsRequest request, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(request);
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult<BucketCorsConfiguration>.Failure(StorageError.Unsupported("Bucket CORS is not used in admin diagnostics tests.", request.BucketName)));
        }

        public ValueTask<StorageResult> DeleteBucketCorsAsync(DeleteBucketCorsRequest request, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(request);
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult.Failure(StorageError.Unsupported("Bucket CORS is not used in admin diagnostics tests.", request.BucketName)));
        }

        public async IAsyncEnumerable<MultipartUploadInfo> ListMultipartUploadsAsync(ListMultipartUploadsRequest request, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(request);
            cancellationToken.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }

        private static ValueTask<StorageResult<T>> UnexpectedAsync<T>() => throw new NotSupportedException("This backend is only used for admin diagnostics tests.");

        private static ValueTask<StorageResult> UnexpectedAsync() => throw new NotSupportedException("This backend is only used for admin diagnostics tests.");
    }

    private sealed class TestStorageReplicaRepairBacklog(IEnumerable<StorageReplicaRepairEntry> entries) : IStorageReplicaRepairBacklog
    {
        private readonly Dictionary<string, StorageReplicaRepairEntry> _entries = entries.ToDictionary(static entry => entry.Id, StringComparer.Ordinal);

        public ValueTask AddAsync(StorageReplicaRepairEntry entry, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(entry);
            cancellationToken.ThrowIfCancellationRequested();

            _entries[entry.Id] = entry;
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> HasOutstandingRepairsAsync(string replicaBackendName, CancellationToken cancellationToken = default)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(replicaBackendName);
            cancellationToken.ThrowIfCancellationRequested();

            return ValueTask.FromResult(_entries.Values.Any(entry =>
                entry.Status != StorageReplicaRepairStatus.Completed
                && string.Equals(entry.ReplicaBackendName, replicaBackendName, StringComparison.Ordinal)));
        }

        public ValueTask<IReadOnlyList<StorageReplicaRepairEntry>> ListOutstandingAsync(string? replicaBackendName = null, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            IEnumerable<StorageReplicaRepairEntry> repairs = _entries.Values.Where(static entry => entry.Status != StorageReplicaRepairStatus.Completed);
            if (!string.IsNullOrWhiteSpace(replicaBackendName)) {
                repairs = repairs.Where(entry => string.Equals(entry.ReplicaBackendName, replicaBackendName, StringComparison.Ordinal));
            }

            return ValueTask.FromResult<IReadOnlyList<StorageReplicaRepairEntry>>(repairs
                .OrderBy(entry => entry.CreatedAtUtc)
                .ThenBy(entry => entry.ReplicaBackendName, StringComparer.Ordinal)
                .ToArray());
        }

        public ValueTask MarkInProgressAsync(string repairId, CancellationToken cancellationToken = default)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(repairId);
            cancellationToken.ThrowIfCancellationRequested();

            if (_entries.TryGetValue(repairId, out var entry)) {
                _entries[repairId] = entry with
                {
                    Status = StorageReplicaRepairStatus.InProgress,
                    AttemptCount = entry.AttemptCount + 1
                };
            }

            return ValueTask.CompletedTask;
        }

        public ValueTask MarkCompletedAsync(string repairId, CancellationToken cancellationToken = default)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(repairId);
            cancellationToken.ThrowIfCancellationRequested();

            _entries.Remove(repairId);
            return ValueTask.CompletedTask;
        }

        public ValueTask MarkFailedAsync(string repairId, StorageError error, CancellationToken cancellationToken = default)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(repairId);
            ArgumentNullException.ThrowIfNull(error);
            cancellationToken.ThrowIfCancellationRequested();

            if (_entries.TryGetValue(repairId, out var entry)) {
                _entries[repairId] = entry with
                {
                    Status = StorageReplicaRepairStatus.Failed,
                    LastErrorCode = error.Code,
                    LastErrorMessage = error.Message
                };
            }

            return ValueTask.CompletedTask;
        }
    }

    private static void ConfigureTestHeaderRoutePolicies(IServiceCollection services, params (string PolicyName, string RequiredScope)[] policies)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(policies);

        services.AddAuthentication("TestHeader")
            .AddScheme<AuthenticationSchemeOptions, TestHeaderAuthenticationHandler>("TestHeader", static _ => { });
        services.AddAuthorization(options => {
            foreach (var (policyName, requiredScope) in policies) {
                options.AddPolicy(policyName, policy => {
                    policy.AddAuthenticationSchemes("TestHeader");
                    policy.RequireAuthenticatedUser();
                    policy.RequireClaim("scope", requiredScope);
                });
            }
        });
    }

    private static string GetRequiredElementValue(XDocument document, string elementName)
    {
        return document.Root?.Element(elementName)?.Value
            ?? throw new Xunit.Sdk.XunitException($"Missing XML element '{elementName}'.");
    }

    private static void AssertAclGrantExists(XDocument document, string permission, string? canonicalUserId = null, string? uri = null)
    {
        ArgumentNullException.ThrowIfNull(document);
        ArgumentException.ThrowIfNullOrWhiteSpace(permission);

        Assert.Contains(document.Descendants("Grant"), grant =>
        {
            var grantee = grant.Element("Grantee");
            var actualPermission = grant.Element("Permission")?.Value;
            var actualId = grantee?.Element("ID")?.Value;
            var actualUri = grantee?.Element("URI")?.Value;

            return string.Equals(actualPermission, permission, StringComparison.Ordinal)
                && (canonicalUserId is null || string.Equals(actualId, canonicalUserId, StringComparison.Ordinal))
                && (uri is null || string.Equals(actualUri, uri, StringComparison.Ordinal));
        });
    }

    private static async Task<IReadOnlyDictionary<string, string>> GetObjectTagsAsync(HttpClient client, string bucketName, string key)
    {
        var response = await client.GetAsync($"/integrated-s3/{bucketName}/{key}?tagging");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        return document.Root!.Element("TagSet")!.Elements("Tag")
            .ToDictionary(
                static tag => tag.Element("Key")?.Value ?? string.Empty,
                static tag => tag.Element("Value")?.Value ?? string.Empty,
                StringComparer.Ordinal);
    }

    private static string BuildTaggingHeader(params (string Key, string Value)[] tags)
    {
        return string.Join("&", tags.Select(static tag => $"{EscapeTaggingHeaderComponent(tag.Key)}={EscapeTaggingHeaderComponent(tag.Value)}"));
    }

    private static string EscapeTaggingHeaderComponent(string value)
    {
        return Uri.EscapeDataString(value).Replace("%2B", "+", StringComparison.Ordinal);
    }

    private static async Task AssertNotImplementedResponseAsync(HttpResponseMessage response)
        => await AssertErrorResponseAsync(response, HttpStatusCode.NotImplemented, "NotImplemented");

    private static async Task AssertErrorResponseAsync(HttpResponseMessage response, HttpStatusCode expectedStatusCode, string expectedCode)
    {
        Assert.Equal(expectedStatusCode, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal(expectedCode, GetRequiredElementValue(errorDocument, "Code"));
    }

    private static async Task EnableBucketVersioningAsync(HttpClient client, string bucketName)
    {
        using var request = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/{bucketName}?versioning")
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

    private static async Task<(string ETag, DateTimeOffset LastModifiedUtc, string? VersionId)> GetHeadObjectMetadataAsync(HttpClient client, string pathAndQuery)
    {
        using var request = new HttpRequestMessage(HttpMethod.Head, pathAndQuery);
        var response = await client.SendAsync(request);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var eTag = response.Headers.ETag?.Tag
            ?? throw new Xunit.Sdk.XunitException("Expected ETag header.");
        var lastModifiedUtc = response.Content.Headers.LastModified
            ?? throw new Xunit.Sdk.XunitException("Expected Last-Modified header.");

        return (
            eTag,
            lastModifiedUtc,
            response.Headers.TryGetValues("x-amz-version-id", out var versionIds)
                ? Assert.Single(versionIds)
                : null);
    }

    private static StorageReplicaRepairEntry CreateRepairEntry(
        string id,
        StorageReplicaRepairOrigin origin,
        StorageReplicaRepairStatus status,
        StorageOperationType operation,
        string primaryBackendName,
        string replicaBackendName,
        string bucketName,
        string? objectKey,
        DateTimeOffset createdAtUtc,
        DateTimeOffset? updatedAtUtc = null,
        string? versionId = null,
        int attemptCount = 0,
        StorageError? lastError = null)
    {
        return new StorageReplicaRepairEntry
        {
            Id = id,
            Origin = origin,
            Status = status,
            Operation = operation,
            PrimaryBackendName = primaryBackendName,
            ReplicaBackendName = replicaBackendName,
            BucketName = bucketName,
            ObjectKey = objectKey,
            VersionId = versionId,
            CreatedAtUtc = createdAtUtc,
            UpdatedAtUtc = updatedAtUtc ?? createdAtUtc,
            AttemptCount = attemptCount,
            LastErrorCode = lastError?.Code,
            LastErrorMessage = lastError?.Message
        };
    }

    private static string ComputeSha1Base64(string content)
    {
        return Convert.ToBase64String(SHA1.HashData(Encoding.UTF8.GetBytes(content)));
    }

    private static string ComputeMultipartSha1Base64(params string[] partChecksums)
    {
        using var checksum = IncrementalHash.CreateHash(HashAlgorithmName.SHA1);
        foreach (var partChecksum in partChecksums) {
            checksum.AppendData(Convert.FromBase64String(partChecksum));
        }

        return $"{Convert.ToBase64String(checksum.GetHashAndReset())}-{partChecksums.Length}";
    }

    private static string ComputeSha256Base64(string content)
    {
        return Convert.ToBase64String(SHA256.HashData(Encoding.UTF8.GetBytes(content)));
    }

    private static string ComputeCrc32Base64(string content)
    {
        var current = 0xFFFFFFFFu;
        foreach (var value in Encoding.UTF8.GetBytes(content)) {
            current = (current >> 8) ^ Crc32Table[(byte)(current ^ value)];
        }

        var finalized = ~current;
        return Convert.ToBase64String(
        [
            (byte)(finalized >> 24),
            (byte)(finalized >> 16),
            (byte)(finalized >> 8),
            (byte)finalized
        ]);
    }

    private static uint[] CreateCrc32Table()
    {
        var table = new uint[256];
        for (uint i = 0; i < table.Length; i++) {
            var value = i;
            for (var bit = 0; bit < 8; bit++) {
                value = (value & 1) == 0
                    ? value >> 1
                    : 0xEDB88320u ^ (value >> 1);
            }

            table[i] = value;
        }

        return table;
    }

    private static HttpRequestMessage CreateAwsChunkedPutObjectRequest(
        string path,
        string payload,
        string? sdkChecksumAlgorithm = null,
        IReadOnlyDictionary<string, string>? trailerHeaders = null,
        IReadOnlyList<string>? declaredTrailerHeaderNames = null,
        IReadOnlyList<KeyValuePair<string, string>>? trailerHeaderEntries = null)
    {
        var payloadBytes = Encoding.UTF8.GetBytes(payload);
        var effectiveTrailerHeaderEntries = trailerHeaderEntries
            ?? trailerHeaders?.ToArray();
        var request = new HttpRequestMessage(HttpMethod.Put, path)
        {
            Content = new ByteArrayContent(BuildAwsChunkedPayload(payloadBytes, effectiveTrailerHeaderEntries))
        };
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("text/plain");
        request.Content.Headers.ContentEncoding.Add("aws-chunked");
        request.Headers.TryAddWithoutValidation("x-amz-decoded-content-length", payloadBytes.Length.ToString(CultureInfo.InvariantCulture));
        if (!string.IsNullOrWhiteSpace(sdkChecksumAlgorithm)) {
            request.Headers.TryAddWithoutValidation("x-amz-sdk-checksum-algorithm", sdkChecksumAlgorithm);
        }

        var trailerHeaderNames = declaredTrailerHeaderNames
            ?? effectiveTrailerHeaderEntries?
                .Select(static trailerHeader => trailerHeader.Key)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();
        if (trailerHeaderNames is not null && trailerHeaderNames.Count > 0) {
            request.Headers.TryAddWithoutValidation("x-amz-trailer", string.Join(",", trailerHeaderNames));
        }

        return request;
    }

    private static byte[] BuildAwsChunkedPayload(byte[] payloadBytes, IReadOnlyList<KeyValuePair<string, string>>? trailerHeaderEntries)
    {
        using var stream = new MemoryStream();
        WriteAscii(stream, $"{payloadBytes.Length:x}\r\n");
        stream.Write(payloadBytes, 0, payloadBytes.Length);
        WriteAscii(stream, "\r\n0\r\n");
        if (trailerHeaderEntries is not null) {
            foreach (var trailerHeader in trailerHeaderEntries) {
                WriteAscii(stream, $"{trailerHeader.Key}:{trailerHeader.Value}\r\n");
            }
        }

        WriteAscii(stream, "\r\n");
        return stream.ToArray();
    }

    private static void WriteAscii(Stream stream, string value)
    {
        var bytes = Encoding.ASCII.GetBytes(value);
        stream.Write(bytes, 0, bytes.Length);
    }

    private static string ComputeMultipartSha256Base64(params string[] partChecksums)
    {
        using var checksum = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        foreach (var partChecksum in partChecksums) {
            checksum.AppendData(Convert.FromBase64String(partChecksum));
        }

        return $"{Convert.ToBase64String(checksum.GetHashAndReset())}-{partChecksums.Length}";
    }

    private static HttpRequestMessage CreateSigV4HeaderSignedRequest(HttpMethod method, string pathAndQuery, string accessKeyId, string secretAccessKey, string? body = null, string? contentType = null, string host = "localhost")
    {
        var request = new HttpRequestMessage(method, pathAndQuery);
        if (body is not null) {
            request.Content = new StringContent(body, Encoding.UTF8, contentType ?? "text/plain");
        }

        var timestampUtc = DateTimeOffset.UtcNow;
        var payloadBytes = body is null ? Array.Empty<byte>() : Encoding.UTF8.GetBytes(body);
        var payloadHash = Convert.ToHexStringLower(SHA256.HashData(payloadBytes));
        request.Headers.Host = host;
        request.Headers.TryAddWithoutValidation("x-amz-date", timestampUtc.ToString("yyyyMMdd'T'HHmmss'Z'"));
        request.Headers.TryAddWithoutValidation("x-amz-content-sha256", payloadHash);

        var credentialScope = new S3SigV4CredentialScope
        {
            AccessKeyId = accessKeyId,
            DateStamp = timestampUtc.ToString("yyyyMMdd"),
            Region = "us-east-1",
            Service = "s3",
            Terminator = "aws4_request"
        };

        var signedHeaders = new[] { "host", "x-amz-content-sha256", "x-amz-date" };
        var requestUri = CreateUri(pathAndQuery, host);
        var canonicalRequest = S3SigV4Signer.BuildCanonicalRequest(
            method.Method,
            requestUri.AbsolutePath,
            EnumerateQueryParameters(requestUri),
            [
                new KeyValuePair<string, string?>("host", host),
                new KeyValuePair<string, string?>("x-amz-content-sha256", payloadHash),
                new KeyValuePair<string, string?>("x-amz-date", timestampUtc.ToString("yyyyMMdd'T'HHmmss'Z'"))
            ],
            signedHeaders,
            payloadHash);

        var stringToSign = S3SigV4Signer.BuildStringToSign("AWS4-HMAC-SHA256", timestampUtc, credentialScope, canonicalRequest.CanonicalRequestHashHex);
        var signature = S3SigV4Signer.ComputeSignature(secretAccessKey, credentialScope, stringToSign);
        var authorizationHeader = $"AWS4-HMAC-SHA256 Credential={accessKeyId}/{credentialScope.Scope}, SignedHeaders={string.Join(';', signedHeaders)}, Signature={signature}";
        request.Headers.TryAddWithoutValidation("Authorization", authorizationHeader);

        return request;
    }

    private static HttpRequestMessage CreateSigV4AwsChunkedTrailerRequest(
        string pathAndQuery,
        string accessKeyId,
        string secretAccessKey,
        string payload,
        IReadOnlyDictionary<string, string> trailerHeaders,
        string host = "localhost")
    {
        var timestampUtc = DateTimeOffset.UtcNow;
        var payloadBytes = Encoding.UTF8.GetBytes(payload);
        var request = new HttpRequestMessage(HttpMethod.Put, pathAndQuery)
        {
            Content = new ByteArrayContent(Array.Empty<byte>())
        };
        request.Content.Headers.TryAddWithoutValidation("Content-Type", "text/plain");
        request.Content.Headers.ContentEncoding.Add("aws-chunked");
        request.Headers.TryAddWithoutValidation("x-amz-decoded-content-length", payloadBytes.Length.ToString(CultureInfo.InvariantCulture));
        request.Headers.TryAddWithoutValidation("x-amz-sdk-checksum-algorithm", ResolveSdkChecksumAlgorithm(trailerHeaders));
        request.Headers.TryAddWithoutValidation("x-amz-trailer", string.Join(",", trailerHeaders.Keys));

        SignSigV4HeaderRequest(
            request,
            pathAndQuery,
            accessKeyId,
            secretAccessKey,
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
            host: host,
            signedAtUtc: timestampUtc,
            additionalSignedHeaders:
            [
                "content-encoding",
                "x-amz-decoded-content-length",
                "x-amz-sdk-checksum-algorithm",
                "x-amz-trailer"
            ]);

        request.Content = new ByteArrayContent(
            BuildSigV4AwsChunkedPayload(
                secretAccessKey,
                CreateSigV4CredentialScope(accessKeyId, timestampUtc),
                timestampUtc,
                GetAuthorizationSignature(request),
                payloadBytes,
                trailerHeaders));
        request.Content.Headers.TryAddWithoutValidation("Content-Type", "text/plain");
        request.Content.Headers.ContentEncoding.Add("aws-chunked");

        return request;
    }

    private static void SignSigV4HeaderRequest(
        HttpRequestMessage request,
        string pathAndQuery,
        string accessKeyId,
        string secretAccessKey,
        string payloadHash,
        string host = "localhost",
        DateTimeOffset? signedAtUtc = null,
        IEnumerable<string>? additionalSignedHeaders = null)
    {
        var timestampUtc = signedAtUtc ?? DateTimeOffset.UtcNow;
        var timestampText = timestampUtc.ToString("yyyyMMdd'T'HHmmss'Z'");

        request.Headers.Host = host;
        request.Headers.Remove("x-amz-date");
        request.Headers.TryAddWithoutValidation("x-amz-date", timestampText);
        request.Headers.Remove("x-amz-content-sha256");
        request.Headers.TryAddWithoutValidation("x-amz-content-sha256", payloadHash);

        var credentialScope = CreateSigV4CredentialScope(accessKeyId, timestampUtc);
        var signedHeaders = new List<string> { "host", "x-amz-content-sha256", "x-amz-date" };
        if (additionalSignedHeaders is not null) {
            signedHeaders.AddRange(additionalSignedHeaders);
        }

        var normalizedSignedHeaders = signedHeaders
            .Select(static header => header.ToLowerInvariant())
            .Distinct(StringComparer.Ordinal)
            .ToList();
        var canonicalHeaders = normalizedSignedHeaders
            .Select(header => new KeyValuePair<string, string?>(header, GetHeaderValue(request, header, host)))
            .ToList();

        var requestUri = CreateUri(pathAndQuery, host);
        var canonicalRequest = S3SigV4Signer.BuildCanonicalRequest(
            request.Method.Method,
            requestUri.AbsolutePath,
            EnumerateQueryParameters(requestUri),
            canonicalHeaders,
            normalizedSignedHeaders,
            payloadHash);

        var stringToSign = S3SigV4Signer.BuildStringToSign("AWS4-HMAC-SHA256", timestampUtc, credentialScope, canonicalRequest.CanonicalRequestHashHex);
        var signature = S3SigV4Signer.ComputeSignature(secretAccessKey, credentialScope, stringToSign);
        var authorizationHeader = $"AWS4-HMAC-SHA256 Credential={accessKeyId}/{credentialScope.Scope}, SignedHeaders={string.Join(';', normalizedSignedHeaders)}, Signature={signature}";
        request.Headers.Remove("Authorization");
        request.Headers.TryAddWithoutValidation("Authorization", authorizationHeader);
    }

    private static HttpRequestMessage CreateSigV4PresignedRequest(HttpMethod method, string pathAndQuery, string accessKeyId, string secretAccessKey, int expiresSeconds, string host = "localhost")
    {
        var timestampUtc = DateTimeOffset.UtcNow;
        var credentialScope = new S3SigV4CredentialScope
        {
            AccessKeyId = accessKeyId,
            DateStamp = timestampUtc.ToString("yyyyMMdd"),
            Region = "us-east-1",
            Service = "s3",
            Terminator = "aws4_request"
        };

        var uri = CreateUri(pathAndQuery, host);
        var baseQuery = S3SigV4QueryStringParser.Parse(uri.Query).ToList();

        baseQuery.AddRange(
        [
            new KeyValuePair<string, string?>("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
            new KeyValuePair<string, string?>("X-Amz-Credential", $"{accessKeyId}/{credentialScope.Scope}"),
            new KeyValuePair<string, string?>("X-Amz-Date", timestampUtc.ToString("yyyyMMdd'T'HHmmss'Z'")),
            new KeyValuePair<string, string?>("X-Amz-Expires", expiresSeconds.ToString()),
            new KeyValuePair<string, string?>("X-Amz-SignedHeaders", "host")
        ]);

        var canonicalRequest = S3SigV4Signer.BuildCanonicalRequest(
            method.Method,
            uri.AbsolutePath,
            baseQuery,
            [new KeyValuePair<string, string?>("host", host)],
            ["host"],
            "UNSIGNED-PAYLOAD",
            unsignedQueryKey: "X-Amz-Signature");
        var stringToSign = S3SigV4Signer.BuildStringToSign("AWS4-HMAC-SHA256", timestampUtc, credentialScope, canonicalRequest.CanonicalRequestHashHex);
        var signature = S3SigV4Signer.ComputeSignature(secretAccessKey, credentialScope, stringToSign);

        var finalQuery = new List<KeyValuePair<string, string?>>(baseQuery)
        {
            new("X-Amz-Signature", signature)
        };

        var presignedUri = QueryHelpers.AddQueryString(uri.GetLeftPart(UriPartial.Path), finalQuery.ToDictionary(static pair => pair.Key, static pair => pair.Value, StringComparer.Ordinal));
        var request = new HttpRequestMessage(method, presignedUri);
        request.Headers.Host = host;
        return request;
    }

    private static async Task AssertSigV4AwsChunkedTrailerRequestAsync(HttpRequestMessage request, string expectedChecksumAlgorithm, string expectedTrailerHeaderName)
    {
        Assert.Equal("STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER", Assert.Single(request.Headers.GetValues("x-amz-content-sha256")));
        Assert.Equal(expectedChecksumAlgorithm, Assert.Single(request.Headers.GetValues("x-amz-sdk-checksum-algorithm")));
        Assert.Equal(expectedTrailerHeaderName, Assert.Single(request.Headers.GetValues("x-amz-trailer")));

        var bodyText = Encoding.ASCII.GetString(await request.Content!.ReadAsByteArrayAsync());
        Assert.Contains("x-amz-trailer-signature:", bodyText, StringComparison.OrdinalIgnoreCase);
    }

    private static void AssertOnlyExpectedChecksumHeader(HttpHeaders headers, string expectedHeaderName, string expectedChecksum)
    {
        Assert.Equal(expectedChecksum, Assert.Single(headers.GetValues(expectedHeaderName)));

        foreach (var headerName in new[] { "x-amz-checksum-sha256", "x-amz-checksum-sha1", "x-amz-checksum-crc32", "x-amz-checksum-crc32c" }) {
            if (!string.Equals(headerName, expectedHeaderName, StringComparison.OrdinalIgnoreCase)) {
                Assert.False(headers.Contains(headerName));
            }
        }
    }

    private static void AssertOnlyExpectedMultipartChecksumElement(XElement partElement, string expectedElementName, string expectedChecksum)
    {
        Assert.Equal(expectedChecksum, partElement.Element(expectedElementName)?.Value);

        foreach (var elementName in new[] { "ChecksumSHA256", "ChecksumSHA1", "ChecksumCRC32", "ChecksumCRC32C" }) {
            if (!string.Equals(elementName, expectedElementName, StringComparison.Ordinal)) {
                Assert.Null(partElement.Element(elementName));
            }
        }
    }

    private static string ComputeTrailerChecksumBase64(string trailerHeaderName, string content)
    {
        return trailerHeaderName.ToLowerInvariant() switch
        {
            "x-amz-checksum-sha1" => ComputeSha1Base64(content),
            "x-amz-checksum-crc32" => ComputeCrc32Base64(content),
            "x-amz-checksum-crc32c" => ChecksumTestAlgorithms.ComputeCrc32cBase64(content),
            _ => throw new Xunit.Sdk.XunitException($"Unsupported trailer checksum header '{trailerHeaderName}'.")
        };
    }

    private static string GetMultipartChecksumElementName(string trailerHeaderName)
    {
        return trailerHeaderName.ToLowerInvariant() switch
        {
            "x-amz-checksum-sha1" => "ChecksumSHA1",
            "x-amz-checksum-crc32" => "ChecksumCRC32",
            "x-amz-checksum-crc32c" => "ChecksumCRC32C",
            _ => throw new Xunit.Sdk.XunitException($"Unsupported trailer checksum header '{trailerHeaderName}'.")
        };
    }

    private static string ResolveSdkChecksumAlgorithm(IReadOnlyDictionary<string, string> trailerHeaders)
    {
        foreach (var headerName in trailerHeaders.Keys) {
            switch (headerName.ToLowerInvariant()) {
                case "x-amz-checksum-crc32":
                    return "CRC32";
                case "x-amz-checksum-crc32c":
                    return "CRC32C";
                case "x-amz-checksum-sha1":
                    return "SHA1";
                case "x-amz-checksum-sha256":
                    return "SHA256";
            }
        }

        throw new Xunit.Sdk.XunitException("Expected a supported x-amz-checksum-* trailer header.");
    }

    private static string GetHeaderValue(HttpRequestMessage request, string headerName, string host)
    {
        if (string.Equals(headerName, "host", StringComparison.Ordinal)) {
            return host;
        }

        if (request.Headers.TryGetValues(headerName, out var requestValues)) {
            return string.Join(",", requestValues);
        }

        if (request.Content?.Headers.TryGetValues(headerName, out var contentValues) == true) {
            return string.Join(",", contentValues);
        }

        throw new Xunit.Sdk.XunitException($"Expected signed header '{headerName}' to be present on the request.");
    }

    private static byte[] BuildSigV4AwsChunkedPayload(
        string secretAccessKey,
        S3SigV4CredentialScope credentialScope,
        DateTimeOffset signedAtUtc,
        string seedSignature,
        byte[] payloadBytes,
        IReadOnlyDictionary<string, string> trailerHeaders)
    {
        var payloadChunkSignature = ComputeSigV4ChunkSignature(secretAccessKey, credentialScope, signedAtUtc, seedSignature, payloadBytes);
        var finalChunkSignature = ComputeSigV4ChunkSignature(secretAccessKey, credentialScope, signedAtUtc, payloadChunkSignature, Array.Empty<byte>());
        var trailerSignature = ComputeSigV4TrailerSignature(secretAccessKey, credentialScope, signedAtUtc, finalChunkSignature, trailerHeaders);

        using var stream = new MemoryStream();
        WriteAscii(stream, $"{payloadBytes.Length:x};chunk-signature={payloadChunkSignature}\r\n");
        stream.Write(payloadBytes, 0, payloadBytes.Length);
        WriteAscii(stream, "\r\n");
        WriteAscii(stream, $"0;chunk-signature={finalChunkSignature}\r\n");
        foreach (var trailerHeader in trailerHeaders.OrderBy(static header => header.Key, StringComparer.Ordinal)) {
            WriteAscii(stream, $"{trailerHeader.Key}:{trailerHeader.Value}\r\n");
        }

        WriteAscii(stream, $"x-amz-trailer-signature:{trailerSignature}\r\n");
        WriteAscii(stream, "\r\n");
        return stream.ToArray();
    }

    private static string ComputeSigV4ChunkSignature(
        string secretAccessKey,
        S3SigV4CredentialScope credentialScope,
        DateTimeOffset signedAtUtc,
        string previousSignature,
        ReadOnlySpan<byte> chunkData)
    {
        const string emptyPayloadSha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        var stringToSign = string.Join('\n', [
            "AWS4-HMAC-SHA256-PAYLOAD",
            signedAtUtc.ToString("yyyyMMdd'T'HHmmss'Z'"),
            credentialScope.Scope,
            previousSignature,
            emptyPayloadSha256,
            S3SigV4Signer.ComputeSha256Hex(chunkData)
        ]);

        return S3SigV4Signer.ComputeSignature(secretAccessKey, credentialScope, stringToSign);
    }

    private static string ComputeSigV4TrailerSignature(
        string secretAccessKey,
        S3SigV4CredentialScope credentialScope,
        DateTimeOffset signedAtUtc,
        string previousSignature,
        IReadOnlyDictionary<string, string> trailerHeaders)
    {
        var canonicalTrailerHeaders = S3SigV4Signer.BuildCanonicalStreamingTrailerHeaders(trailerHeaders);
        var stringToSign = S3SigV4Signer.BuildStreamingTrailerStringToSign(
            signedAtUtc,
            credentialScope,
            previousSignature,
            S3SigV4Signer.ComputeSha256Hex(canonicalTrailerHeaders));

        return S3SigV4Signer.ComputeSignature(secretAccessKey, credentialScope, stringToSign);
    }

    private static string GetAuthorizationSignature(HttpRequestMessage request)
    {
        var authorizationHeader = Assert.Single(request.Headers.GetValues("Authorization"));
        const string signatureMarker = "Signature=";
        var signatureIndex = authorizationHeader.LastIndexOf(signatureMarker, StringComparison.Ordinal);
        if (signatureIndex < 0) {
            throw new Xunit.Sdk.XunitException("Expected Authorization header to contain a Signature component.");
        }

        return authorizationHeader[(signatureIndex + signatureMarker.Length)..].Trim();
    }

    private static S3SigV4CredentialScope CreateSigV4CredentialScope(string accessKeyId, DateTimeOffset signedAtUtc)
    {
        return new S3SigV4CredentialScope
        {
            AccessKeyId = accessKeyId,
            DateStamp = signedAtUtc.ToString("yyyyMMdd"),
            Region = "us-east-1",
            Service = "s3",
            Terminator = "aws4_request"
        };
    }

    private static string CreateCorruptedAuthorizationHeader(HttpMethod method, string pathAndQuery, string accessKeyId, string secretAccessKey)
    {
        using var request = CreateSigV4HeaderSignedRequest(method, pathAndQuery, accessKeyId, secretAccessKey);
        var validValue = request.Headers.GetValues("Authorization").Single();
        return validValue[..^1] + (validValue[^1] == '0' ? '1' : '0');
    }

    private static Uri CreateUri(string pathAndQuery, string host)
    {
        return new Uri($"http://{host}{pathAndQuery}", UriKind.Absolute);
    }

    private static IEnumerable<KeyValuePair<string, string?>> EnumerateQueryParameters(Uri uri)
    {
        return S3SigV4QueryStringParser.Parse(uri.Query);
    }
}
