using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Globalization;
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
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class IntegratedS3HttpEndpointsTests : IClassFixture<WebUiApplicationFactory>
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

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
        Assert.Equal("NoSuchKey", GetRequiredElementValue(errorDocument, "Code"));
    }

    [Fact]
    public async Task PutObject_WithSha1ChecksumHeaders_ValidatesPayloadAndEmitsSha1Headers()
    {
        using var client = await _factory.CreateClientAsync();

        Assert.Equal(HttpStatusCode.Created, (await client.PutAsync("/integrated-s3/buckets/sha1-header-bucket", content: null)).StatusCode);

        const string payload = "hello sha1 checksum";
        var checksum = ComputeSha1Base64(payload);

        using var putRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/sha1-header-bucket/docs/sha1.txt")
        {
            Content = new StringContent(payload, Encoding.UTF8, "text/plain")
        };
        putRequest.Headers.TryAddWithoutValidation("x-amz-sdk-checksum-algorithm", "SHA1");
        putRequest.Headers.TryAddWithoutValidation("x-amz-checksum-sha1", checksum);

        var putResponse = await client.SendAsync(putRequest);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(putResponse.Headers.GetValues("x-amz-checksum-sha1")));

        using var headRequest = new HttpRequestMessage(HttpMethod.Head, "/integrated-s3/sha1-header-bucket/docs/sha1.txt");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(headResponse.Headers.GetValues("x-amz-checksum-sha1")));

        var getResponse = await client.GetAsync("/integrated-s3/sha1-header-bucket/docs/sha1.txt");
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(getResponse.Headers.GetValues("x-amz-checksum-sha1")));
        Assert.Equal(payload, await getResponse.Content.ReadAsStringAsync());

        using var badPutRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/sha1-header-bucket/docs/bad-sha1.txt")
        {
            Content = new StringContent("sha1 checksum mismatch", Encoding.UTF8, "text/plain")
        };
        badPutRequest.Headers.TryAddWithoutValidation("x-amz-sdk-checksum-algorithm", "SHA1");
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
        Assert.Equal(checksum, Assert.Single(putResponse.Headers.GetValues("x-amz-checksum-crc32c")));

        using var headRequest = new HttpRequestMessage(HttpMethod.Head, "/integrated-s3/crc32c-header-bucket/docs/crc32c.txt");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(headResponse.Headers.GetValues("x-amz-checksum-crc32c")));

        var getResponse = await client.GetAsync("/integrated-s3/crc32c-header-bucket/docs/crc32c.txt");
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(getResponse.Headers.GetValues("x-amz-checksum-crc32c")));
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
        var preflightVary = string.Join(", ", preflightResponse.Headers.Vary);
        Assert.Contains("Origin", preflightVary, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Access-Control-Request-Method", preflightVary, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Access-Control-Request-Headers", preflightVary, StringComparison.OrdinalIgnoreCase);

        using var getRequest = new HttpRequestMessage(HttpMethod.Get, "/integrated-s3/browser-cors-vary-bucket/docs/browser.txt");
        getRequest.Headers.Add("Origin", "https://other.example");

        var getResponse = await client.SendAsync(getRequest);
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        Assert.False(getResponse.Headers.Contains("Access-Control-Allow-Origin"));
        Assert.Contains("Origin", string.Join(", ", getResponse.Headers.Vary), StringComparison.OrdinalIgnoreCase);
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

        var firstPageUploads = firstPageDocument.Root!.Elements("Upload").ToArray();
        Assert.Collection(
            firstPageUploads,
            upload => {
                Assert.Equal("docs/alpha.txt", upload.Element("Key")?.Value);
                Assert.Equal(firstUploadId, upload.Element("UploadId")?.Value);
            },
            upload => {
                Assert.Equal("docs/alpha.txt", upload.Element("Key")?.Value);
                Assert.Equal(secondUploadId, upload.Element("UploadId")?.Value);
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

        using var notModifiedCopyRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/buckets/copy-precondition-target/objects/docs/copied.txt");
        notModifiedCopyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source", "/copy-precondition-source/docs/source.txt");
        notModifiedCopyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source-if-none-match", $"\"{sourceObject!.ETag}\"");

        var notModifiedCopyResponse = await client.SendAsync(notModifiedCopyRequest);
        Assert.Equal(HttpStatusCode.OK, notModifiedCopyResponse.StatusCode);
        Assert.Equal("application/xml", notModifiedCopyResponse.Content.Headers.ContentType?.MediaType);
        var notModifiedCopyXml = XDocument.Parse(await notModifiedCopyResponse.Content.ReadAsStringAsync());
        Assert.Equal("CopyObjectResult", notModifiedCopyXml.Root?.Name.LocalName);

        var targetHead = await client.SendAsync(new HttpRequestMessage(HttpMethod.Head, "/integrated-s3/buckets/copy-precondition-target/objects/docs/copied.txt"));
        Assert.Equal(HttpStatusCode.NotFound, targetHead.StatusCode);
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
    <Tag><Key>environment</Key><Value>test</Value></Tag>
    <Tag><Key>owner</Key><Value>copilot</Value></Tag>
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

        Assert.Equal("test", tags["environment"]);
        Assert.Equal("copilot", tags["owner"]);

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
    [InlineData("acl")]
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
    [InlineData("PUT", "acl")]
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
    public async Task S3CompatibleListMultipartUploads_UnsupportedEncodingType_ReturnsInvalidArgument()
    {
        using var client = await _factory.CreateClientAsync();

        await client.PutAsync("/integrated-s3/buckets/multipart-subresource-bucket", content: null);

        var response = await client.GetAsync("/integrated-s3/multipart-subresource-bucket?uploads&encoding-type=base64");

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

    [Theory]
    [InlineData("GET", "acl")]
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
    [InlineData("GET", "tagging&uploadId=upload-123")]
    [InlineData("GET", "versionId=historical-version&uploadId=upload-123")]
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
    public async Task S3CompatibleMultipartUpload_RoundTripsXmlWorkflow()
    {
        using var client = await _factory.CreateClientAsync();

        await client.PutAsync("/integrated-s3/buckets/multipart-bucket", content: null);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, "/integrated-s3/multipart-bucket/docs/multipart.txt?uploads");
        initiateRequest.Headers.Add("x-integrateds3-meta-origin", "http-test");
        initiateRequest.Headers.TryAddWithoutValidation("Content-Type", "text/plain");

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

        var completeDocument = XDocument.Parse(await completeResponse.Content.ReadAsStringAsync());
        Assert.Equal("CompleteMultipartUploadResult", completeDocument.Root?.Name.LocalName);
        Assert.Equal("multipart-bucket", GetRequiredElementValue(completeDocument, "Bucket"));
        Assert.Equal("docs/multipart.txt", GetRequiredElementValue(completeDocument, "Key"));

        var downloadResponse = await client.GetAsync("/integrated-s3/buckets/multipart-bucket/objects/docs/multipart.txt");
        Assert.Equal(HttpStatusCode.OK, downloadResponse.StatusCode);
        Assert.Equal("http-test", Assert.Single(downloadResponse.Headers.GetValues("x-integrateds3-meta-origin")));
        var multipartChecksum = Assert.Single(downloadResponse.Headers.GetValues("x-amz-checksum-sha256"));
        Assert.False(string.IsNullOrWhiteSpace(multipartChecksum));
        Assert.Equal("hello world", await downloadResponse.Content.ReadAsStringAsync());
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

    [Fact]
    public async Task S3CompatiblePutObject_WithKmsServerSideEncryptionHeaders_ParsesRequestAndEmitsResponseHeaders()
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        var encryptionContext = Convert.ToBase64String(Encoding.UTF8.GetBytes("""{"tenant":"alpha","environment":"test"}"""));

        using var request = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/sse-put-bucket/docs/encrypted.txt")
        {
            Content = new StringContent("encrypted payload", Encoding.UTF8, "text/plain")
        };
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption", "aws:kms");
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-aws-kms-key-id", "alias/test-key");
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-context", encryptionContext);

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("aws:kms", Assert.Single(response.Headers.GetValues("x-amz-server-side-encryption")));
        Assert.Equal("alias/test-key", Assert.Single(response.Headers.GetValues("x-amz-server-side-encryption-aws-kms-key-id")));

        var putRequest = storageService.LastPutObjectRequest ?? throw new Xunit.Sdk.XunitException("Expected PUT request to reach the storage service.");
        var serverSideEncryption = putRequest.ServerSideEncryption ?? throw new Xunit.Sdk.XunitException("Expected PUT request server-side encryption settings.");
        Assert.Equal(ObjectServerSideEncryptionAlgorithm.Kms, serverSideEncryption.Algorithm);
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
    public async Task S3CompatiblePutObject_WithUnsupportedServerSideEncryptionHeader_ReturnsNotImplemented()
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        using var request = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/sse-unsupported-bucket/docs/unsupported.txt")
        {
            Content = new StringContent("unsupported encryption request", Encoding.UTF8, "text/plain")
        };
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-algorithm", "AES256");

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

    [Fact]
    public async Task S3CompatibleInitiateMultipartUpload_WithServerSideEncryptionHeaders_ParsesRequest()
    {
        var storageService = new RecordingStorageService();
        await using var isolatedClient = await CreateStorageServiceIsolatedClientAsync(storageService);
        using var client = isolatedClient.Client;

        var encryptionContext = Convert.ToBase64String(Encoding.UTF8.GetBytes("""{"tenant":"beta"}"""));

        using var request = new HttpRequestMessage(HttpMethod.Post, "/integrated-s3/multipart-sse-bucket/docs/upload.txt?uploads");
        request.Headers.TryAddWithoutValidation("Content-Type", "text/plain");
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption", "aws:kms");
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-context", encryptionContext);

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("upload-123", GetRequiredElementValue(document, "UploadId"));

        var initiateRequest = storageService.LastInitiateMultipartUploadRequest ?? throw new Xunit.Sdk.XunitException("Expected multipart initiation request to reach the storage service.");
        var serverSideEncryption = initiateRequest.ServerSideEncryption ?? throw new Xunit.Sdk.XunitException("Expected multipart initiation server-side encryption settings.");
        Assert.Equal(ObjectServerSideEncryptionAlgorithm.Kms, serverSideEncryption.Algorithm);
        Assert.Null(serverSideEncryption.KeyId);
        Assert.Equal("beta", serverSideEncryption.Context!["tenant"]);
    }

    [Theory]
    [InlineData("GET")]
    [InlineData("HEAD")]
    public async Task S3CompatibleReadObject_EmitsServerSideEncryptionHeaders(string method)
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
                Algorithm = ObjectServerSideEncryptionAlgorithm.Kms,
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
        Assert.Equal("aws:kms", Assert.Single(response.Headers.GetValues("x-amz-server-side-encryption")));
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
            builder.Services.PostConfigure<IntegratedS3EndpointOptions>(options => {
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
                builder.Services.PostConfigure<IntegratedS3EndpointOptions>(options => {
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
                StorageOperationType.GetObject => "storage.read",
                StorageOperationType.PresignGetObject => "storage.read",
                StorageOperationType.GetBucketLocation => "storage.read",
                StorageOperationType.GetBucketCors => "storage.read",
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

        public InitiateMultipartUploadRequest? LastInitiateMultipartUploadRequest { get; private set; }

        public ObjectInfo? PutObjectResult { get; set; }

        public ObjectInfo? CopyObjectResult { get; set; }

        public GetObjectResponse? GetObjectResult { get; set; }

        public ObjectInfo? HeadObjectResult { get; set; }

        public MultipartUploadInfo? InitiateMultipartUploadResult { get; set; }

        public IAsyncEnumerable<BucketInfo> ListBucketsAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<BucketInfo>> CreateBucketAsync(CreateBucketRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<BucketLocationInfo>> GetBucketLocationAsync(string bucketName, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<BucketVersioningInfo>> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<BucketVersioningInfo>> PutBucketVersioningAsync(PutBucketVersioningRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<BucketCorsConfiguration>> GetBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<BucketCorsConfiguration>> PutBucketCorsAsync(PutBucketCorsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult> DeleteBucketCorsAsync(DeleteBucketCorsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<BucketInfo>> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult> DeleteBucketAsync(DeleteBucketRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public IAsyncEnumerable<ObjectInfo> ListObjectsAsync(ListObjectsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(ListObjectVersionsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public IAsyncEnumerable<MultipartUploadInfo> ListMultipartUploadsAsync(ListMultipartUploadsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<GetObjectResponse>> GetObjectAsync(GetObjectRequest request, CancellationToken cancellationToken = default)
        {
            LastGetObjectRequest = request;

            return new ValueTask<StorageResult<GetObjectResponse>>(StorageResult<GetObjectResponse>.Success(
                GetObjectResult ?? CreateGetObjectResponse(request.BucketName, request.Key, "recorded response", serverSideEncryption: null)));
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
                    ToServerSideEncryptionInfo(request.DestinationServerSideEncryption))));
        }

        public ValueTask<StorageResult<ObjectInfo>> PutObjectAsync(PutObjectRequest request, CancellationToken cancellationToken = default)
        {
            LastPutObjectRequest = request;

            return new ValueTask<StorageResult<ObjectInfo>>(StorageResult<ObjectInfo>.Success(
                PutObjectResult ?? CreateObjectInfo(
                    request.BucketName,
                    request.Key,
                    request.ContentType,
                    ToServerSideEncryptionInfo(request.ServerSideEncryption))));
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
                    ChecksumAlgorithm = request.ChecksumAlgorithm
                }));
        }

        public ValueTask<StorageResult<MultipartUploadPart>> UploadMultipartPartAsync(UploadMultipartPartRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<ObjectInfo>> CompleteMultipartUploadAsync(CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult> AbortMultipartUploadAsync(AbortMultipartUploadRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<ObjectInfo>> HeadObjectAsync(HeadObjectRequest request, CancellationToken cancellationToken = default)
        {
            LastHeadObjectRequest = request;

            return new ValueTask<StorageResult<ObjectInfo>>(StorageResult<ObjectInfo>.Success(
                HeadObjectResult ?? CreateObjectInfo(request.BucketName, request.Key, contentType: null, serverSideEncryption: null)));
        }

        public ValueTask<StorageResult<DeleteObjectResult>> DeleteObjectAsync(DeleteObjectRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        private static GetObjectResponse CreateGetObjectResponse(string bucketName, string key, string payload, ObjectServerSideEncryptionInfo? serverSideEncryption)
        {
            var payloadBytes = Encoding.UTF8.GetBytes(payload);
            return new GetObjectResponse
            {
                Object = CreateObjectInfo(bucketName, key, "text/plain", serverSideEncryption, payloadBytes.Length),
                Content = new MemoryStream(payloadBytes),
                TotalContentLength = payloadBytes.Length
            };
        }

        private static ObjectInfo CreateObjectInfo(string bucketName, string key, string? contentType, ObjectServerSideEncryptionInfo? serverSideEncryption, long contentLength = 0)
        {
            return new ObjectInfo
            {
                BucketName = bucketName,
                Key = key,
                ContentLength = contentLength,
                ContentType = contentType,
                ETag = "recording-etag",
                LastModifiedUtc = DefaultTimestampUtc,
                ServerSideEncryption = serverSideEncryption
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

    private static async Task AssertNotImplementedResponseAsync(HttpResponseMessage response)
    {
        Assert.Equal(HttpStatusCode.NotImplemented, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("NotImplemented", GetRequiredElementValue(errorDocument, "Code"));
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
        var canonicalRequest = S3SigV4Signer.BuildCanonicalRequest(
            method.Method,
            CreateUri(pathAndQuery, host).AbsolutePath,
            EnumerateQueryParameters(CreateUri(pathAndQuery, host)),
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
        var baseQuery = QueryHelpers.ParseQuery(uri.Query)
            .SelectMany(static pair => pair.Value, static (pair, value) => new KeyValuePair<string, string?>(pair.Key, value))
            .ToList();

        baseQuery.AddRange([
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
        foreach (var pair in QueryHelpers.ParseQuery(uri.Query)) {
            if (pair.Value.Count == 0) {
                yield return new KeyValuePair<string, string?>(pair.Key, string.Empty);
                continue;
            }

            foreach (var value in pair.Value) {
                yield return new KeyValuePair<string, string?>(pair.Key, value);
            }
        }
    }
}
