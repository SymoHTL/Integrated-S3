using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Xml.Linq;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.AspNetCore;
using IntegratedS3.AspNetCore.Endpoints;
using IntegratedS3.Core.Models;
using IntegratedS3.Core.Services;
using IntegratedS3.Protocol;
using IntegratedS3.Tests.Infrastructure;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.DependencyInjection;
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

        using var headRequest = new HttpRequestMessage(HttpMethod.Head, "/integrated-s3/buckets/test-bucket/objects/docs/hello.txt");
        var headResponse = await client.SendAsync(headRequest);
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);
        Assert.Equal("copilot", Assert.Single(headResponse.Headers.GetValues("x-integrateds3-meta-author")));
        Assert.Equal("text/plain", headResponse.Content.Headers.ContentType?.MediaType);

        var downloadResponse = await client.GetAsync("/integrated-s3/buckets/test-bucket/objects/docs/hello.txt");
        Assert.Equal(HttpStatusCode.OK, downloadResponse.StatusCode);
        Assert.Equal("copilot", Assert.Single(downloadResponse.Headers.GetValues("x-integrateds3-meta-author")));
        Assert.Equal("hello from xunit", await downloadResponse.Content.ReadAsStringAsync());

        var deleteObjectResponse = await client.DeleteAsync("/integrated-s3/buckets/test-bucket/objects/docs/hello.txt");
        Assert.Equal(HttpStatusCode.NoContent, deleteObjectResponse.StatusCode);

        var deleteBucketResponse = await client.DeleteAsync("/integrated-s3/buckets/test-bucket");
        Assert.Equal(HttpStatusCode.NoContent, deleteBucketResponse.StatusCode);
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
    public async Task ServiceDocument_AdvertisesDiskProviderCapabilities()
    {
        using var client = await _factory.CreateClientAsync();

        var document = await client.GetFromJsonAsync<StorageServiceDocument>("/integrated-s3/", JsonOptions);

        Assert.NotNull(document);
        Assert.Equal("Integrated S3 Sample Host", document!.ServiceName);
        var provider = Assert.Single(document.Providers);
        Assert.Equal("disk", provider.Kind);
        Assert.True(provider.IsPrimary);
        Assert.Equal("test-disk", provider.Name);
        Assert.Equal(StorageCapabilitySupport.Native, provider.Capabilities.ObjectCrud);
        Assert.Equal(StorageCapabilitySupport.Native, document.Capabilities.ObjectCrud);
        Assert.Equal(StorageCapabilitySupport.Native, provider.Capabilities.Pagination);
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

        await client.PutAsync("/integrated-s3/buckets/copy-source-bucket", content: null);
        await client.PutAsync("/integrated-s3/buckets/copy-target-bucket", content: null);

        using var uploadRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/buckets/copy-source-bucket/objects/docs/source.txt")
        {
            Content = new StringContent("copied payload", Encoding.UTF8, "text/plain")
        };
        uploadRequest.Headers.Add("x-integrateds3-meta-origin", "copied-from-source");
        var uploadResponse = await client.SendAsync(uploadRequest);
        Assert.Equal(HttpStatusCode.OK, uploadResponse.StatusCode);

        using var copyRequest = new HttpRequestMessage(HttpMethod.Put, "/integrated-s3/buckets/copy-target-bucket/objects/docs/copied.txt");
        copyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source", "/copy-source-bucket/docs/source.txt");

        var copyResponse = await client.SendAsync(copyRequest);
        Assert.Equal(HttpStatusCode.OK, copyResponse.StatusCode);
        Assert.Equal("application/xml", copyResponse.Content.Headers.ContentType?.MediaType);
        var copyDocument = XDocument.Parse(await copyResponse.Content.ReadAsStringAsync());
        Assert.Equal("CopyObjectResult", copyDocument.Root?.Name.LocalName);
        Assert.False(string.IsNullOrWhiteSpace(GetRequiredElementValue(copyDocument, "LastModified")));
        Assert.False(string.IsNullOrWhiteSpace(GetRequiredElementValue(copyDocument, "ETag")));

        var downloadedResponse = await client.GetAsync("/integrated-s3/buckets/copy-target-bucket/objects/docs/copied.txt");
        Assert.Equal(HttpStatusCode.OK, downloadedResponse.StatusCode);
        Assert.Equal("copied-from-source", Assert.Single(downloadedResponse.Headers.GetValues("x-integrateds3-meta-origin")));
        Assert.Equal("copied payload", await downloadedResponse.Content.ReadAsStringAsync());
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
    public async Task S3CompatibleBucketRoute_UnsupportedSubresource_ReturnsNotImplemented()
    {
        using var client = await _factory.CreateClientAsync();

        await client.PutAsync("/integrated-s3/buckets/subresource-bucket", content: null);

        var response = await client.GetAsync("/integrated-s3/subresource-bucket?acl");

        Assert.Equal(HttpStatusCode.NotImplemented, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("NotImplemented", GetRequiredElementValue(errorDocument, "Code"));
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
                Assert.Equal("hello world", await downloadResponse.Content.ReadAsStringAsync());
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
                StorageOperationType.HeadObject => "storage.read",
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

    private static string GetRequiredElementValue(XDocument document, string elementName)
    {
        return document.Root?.Element(elementName)?.Value
            ?? throw new Xunit.Sdk.XunitException($"Missing XML element '{elementName}'.");
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
