using System.Net;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Xml.Linq;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.AspNetCore;
using IntegratedS3.Core.Models;
using IntegratedS3.Core.Services;
using IntegratedS3.Protocol;
using IntegratedS3.Tests.Infrastructure;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class IntegratedS3SigV4ConformanceTests : IClassFixture<WebUiApplicationFactory>
{
    private readonly WebUiApplicationFactory _factory;

    public IntegratedS3SigV4ConformanceTests(WebUiApplicationFactory factory)
    {
        _factory = factory;
    }

    [Fact]
    public async Task SigV4PresignedQueryAuthentication_AllowsBucketVersioningReads()
    {
        const string accessKeyId = "sigv4-presign-bucket-versioning-access";
        const string secretAccessKey = "sigv4-presign-bucket-versioning-secret";
        const string bucketName = "sigv4-presign-bucket-versioning";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var enableVersioningRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/{bucketName}?versioning",
            accessKeyId,
            secretAccessKey,
            body: """
<VersioningConfiguration>
  <Status>Enabled</Status>
</VersioningConfiguration>
""",
            contentType: "application/xml");
        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(enableVersioningRequest)).StatusCode);

        using var getVersioningRequest = CreateSigV4PresignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/{bucketName}?versioning",
            accessKeyId,
            secretAccessKey,
            expiresSeconds: 300);
        var getVersioningResponse = await client.SendAsync(getVersioningRequest);

        Assert.Equal(HttpStatusCode.OK, getVersioningResponse.StatusCode);
        Assert.Equal("application/xml", getVersioningResponse.Content.Headers.ContentType?.MediaType);
        var versioningDocument = XDocument.Parse(await getVersioningResponse.Content.ReadAsStringAsync());
        Assert.Equal("VersioningConfiguration", versioningDocument.Root?.Name.LocalName);
        Assert.Equal("Enabled", GetRequiredElementValue(versioningDocument, "Status"));
    }

    [Fact]
    public async Task SigV4PresignedQueryAuthentication_IgnoresUnsignedPayloadHashHeader()
    {
        const string accessKeyId = "sigv4-presign-payloadhash-access";
        const string secretAccessKey = "sigv4-presign-payloadhash-secret";
        const string bucketName = "sigv4-presign-payloadhash-bucket";
        const string objectKey = "docs/payloadhash.txt";
        const string payload = "presigned payload hash compatibility";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var putObjectRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/{bucketName}/{objectKey}",
            accessKeyId,
            secretAccessKey,
            body: payload,
            contentType: "text/plain");
        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(putObjectRequest)).StatusCode);

        using var presignedRequest = CreateSigV4PresignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/{bucketName}/{objectKey}",
            accessKeyId,
            secretAccessKey,
            expiresSeconds: 300);
        presignedRequest.Headers.TryAddWithoutValidation("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD");
        var response = await client.SendAsync(presignedRequest);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(payload, await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_AllowsLiteralPlusSignsInSignedIgnoredQueryParameters()
    {
        const string accessKeyId = "sigv4-plus-access";
        const string secretAccessKey = "sigv4-plus-secret";
        const string bucketName = "sigv4-plus-bucket";
        const string objectKey = "docs/plus.txt";
        const string payload = "signed plus query";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var putObjectRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}?x-id=PutObject+Test&x-id=PutObject%2BSecond",
            accessKeyId,
            secretAccessKey,
            body: payload,
            contentType: "text/plain");
        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(putObjectRequest)).StatusCode);

        using var getObjectRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}?x-id=GetObject+Test&x-id=GetObject%2BSecond",
            accessKeyId,
            secretAccessKey);
        var getObjectResponse = await client.SendAsync(getObjectRequest);

        Assert.Equal(HttpStatusCode.OK, getObjectResponse.StatusCode);
        Assert.Equal(payload, await getObjectResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task SigV4PresignedQueryAuthentication_AllowsHistoricalVersionReads()
    {
        const string accessKeyId = "sigv4-presign-version-access";
        const string secretAccessKey = "sigv4-presign-version-secret";
        const string bucketName = "sigv4-presign-version-bucket";
        const string objectKey = "docs/history.txt";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var enableVersioningRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/{bucketName}?versioning",
            accessKeyId,
            secretAccessKey,
            body: """
<VersioningConfiguration>
  <Status>Enabled</Status>
</VersioningConfiguration>
""",
            contentType: "application/xml");
        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(enableVersioningRequest)).StatusCode);

        using var putV1Request = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/{bucketName}/{objectKey}",
            accessKeyId,
            secretAccessKey,
            body: "historical version",
            contentType: "text/plain");
        var v1Response = await client.SendAsync(putV1Request);
        Assert.Equal(HttpStatusCode.OK, v1Response.StatusCode);
        var v1VersionId = Assert.Single(v1Response.Headers.GetValues("x-amz-version-id"));

        using var putV2Request = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/{bucketName}/{objectKey}",
            accessKeyId,
            secretAccessKey,
            body: "current version",
            contentType: "text/plain");
        var v2Response = await client.SendAsync(putV2Request);
        Assert.Equal(HttpStatusCode.OK, v2Response.StatusCode);
        Assert.NotEqual(v1VersionId, Assert.Single(v2Response.Headers.GetValues("x-amz-version-id")));

        using var historicalGetRequest = CreateSigV4PresignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/{bucketName}/{objectKey}?versionId={Uri.EscapeDataString(v1VersionId)}",
            accessKeyId,
            secretAccessKey,
            expiresSeconds: 300);
        var historicalGetResponse = await client.SendAsync(historicalGetRequest);

        Assert.Equal(HttpStatusCode.OK, historicalGetResponse.StatusCode);
        Assert.Equal(v1VersionId, Assert.Single(historicalGetResponse.Headers.GetValues("x-amz-version-id")));
        Assert.Equal("historical version", await historicalGetResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task SigV4PresignedQueryAuthentication_ExpiredRequest_ReturnsXmlError()
    {
        const string accessKeyId = "sigv4-expired-access";
        const string secretAccessKey = "sigv4-expired-secret";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(
            accessKeyId,
            secretAccessKey,
            options => options.AllowedSignatureClockSkewMinutes = 1);
        using var client = isolatedClient.Client;

        using var expiredRequest = CreateSigV4PresignedRequest(
            HttpMethod.Get,
            "/integrated-s3/buckets/expired-bucket/objects/docs/expired.txt",
            accessKeyId,
            secretAccessKey,
            expiresSeconds: 1,
            signedAtUtc: DateTimeOffset.UtcNow.AddMinutes(-2));
        var response = await client.SendAsync(expiredRequest);

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("AccessDenied", GetRequiredElementValue(errorDocument, "Code"));
        Assert.Contains("expired", GetRequiredElementValue(errorDocument, "Message"), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task SigV4PresignedQueryAuthentication_ExpiryBeyondConfiguredMaximum_ReturnsXmlError()
    {
        const string accessKeyId = "sigv4-expiry-limit-access";
        const string secretAccessKey = "sigv4-expiry-limit-secret";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(
            accessKeyId,
            secretAccessKey,
            options => options.MaximumPresignedUrlExpirySeconds = 60);
        using var client = isolatedClient.Client;

        using var request = CreateSigV4PresignedRequest(
            HttpMethod.Get,
            "/integrated-s3/buckets/expiry-limit-bucket/objects/docs/limit.txt",
            accessKeyId,
            secretAccessKey,
            expiresSeconds: 120);
        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("AuthorizationQueryParametersError", GetRequiredElementValue(errorDocument, "Code"));
        Assert.Contains("60", GetRequiredElementValue(errorDocument, "Message"), StringComparison.Ordinal);
    }

    [Fact]
    public async Task SigV4PresignedQueryAuthentication_FutureTimestampOutsideClockSkew_ReturnsXmlError()
    {
        const string accessKeyId = "sigv4-future-access";
        const string secretAccessKey = "sigv4-future-secret";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(
            accessKeyId,
            secretAccessKey,
            options => options.AllowedSignatureClockSkewMinutes = 1);
        using var client = isolatedClient.Client;

        using var request = CreateSigV4PresignedRequest(
            HttpMethod.Get,
            "/integrated-s3/buckets/future-bucket/objects/docs/future.txt",
            accessKeyId,
            secretAccessKey,
            expiresSeconds: 300,
            signedAtUtc: DateTimeOffset.UtcNow.AddMinutes(2));
        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("RequestTimeTooSkewed", GetRequiredElementValue(errorDocument, "Code"));
        Assert.Contains("future", GetRequiredElementValue(errorDocument, "Message"), StringComparison.OrdinalIgnoreCase);
    }

    private Task<WebUiApplicationFactory.IsolatedWebUiClient> CreateAuthenticatedClientAsync(
        string accessKeyId,
        string secretAccessKey,
        Action<IntegratedS3Options>? configureOptions = null)
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
                        DisplayName = "sigv4-user",
                        Scopes = ["storage.read", "storage.write"]
                    }
                ];
                configureOptions?.Invoke(options);
            });
            builder.Services.AddSingleton<IIntegratedS3AuthorizationService, ScopeBasedIntegratedS3AuthorizationService>();
        });
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

    private static string GetRequiredElementValue(XDocument document, string elementName)
    {
        return document.Root?.Element(elementName)?.Value
            ?? throw new Xunit.Sdk.XunitException($"Missing XML element '{elementName}'.");
    }

    private static HttpRequestMessage CreateSigV4HeaderSignedRequest(
        HttpMethod method,
        string pathAndQuery,
        string accessKeyId,
        string secretAccessKey,
        string? body = null,
        string? contentType = null,
        string host = "localhost",
        DateTimeOffset? signedAtUtc = null)
    {
        var request = new HttpRequestMessage(method, pathAndQuery);
        if (body is not null) {
            request.Content = new StringContent(body, Encoding.UTF8, contentType ?? "text/plain");
        }

        var timestampUtc = signedAtUtc ?? DateTimeOffset.UtcNow;
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

    private static HttpRequestMessage CreateSigV4PresignedRequest(
        HttpMethod method,
        string pathAndQuery,
        string accessKeyId,
        string secretAccessKey,
        int expiresSeconds,
        string host = "localhost",
        DateTimeOffset? signedAtUtc = null)
    {
        var timestampUtc = signedAtUtc ?? DateTimeOffset.UtcNow;
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

        var presignedUri = QueryHelpers.AddQueryString(
            uri.GetLeftPart(UriPartial.Path),
            finalQuery.ToDictionary(static pair => pair.Key, static pair => pair.Value, StringComparer.Ordinal));
        var request = new HttpRequestMessage(method, presignedUri);
        request.Headers.Host = host;
        return request;
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
