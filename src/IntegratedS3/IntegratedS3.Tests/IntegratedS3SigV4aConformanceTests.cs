using System.Globalization;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Xml.Linq;
using IntegratedS3.AspNetCore;
using IntegratedS3.Protocol;
using IntegratedS3.Tests.Infrastructure;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class IntegratedS3SigV4aConformanceTests : IClassFixture<WebUiApplicationFactory>
{
    private static readonly XNamespace S3Ns = "http://s3.amazonaws.com/doc/2006-03-01/";

    private const string StreamingSigV4aPayloadTrailer = "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER";
    private const string StreamingUnsignedPayloadTrailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";
    private readonly WebUiApplicationFactory _factory;

    public IntegratedS3SigV4aConformanceTests(WebUiApplicationFactory factory)
    {
        _factory = factory;
    }

    // ── Header-based SigV4a ────────────────────────────────────────────────

    [Fact]
    public async Task SigV4aHeaderAuthentication_AllowsListBuckets()
    {
        const string accessKeyId = "sigv4a-header-list-access";
        const string secretAccessKey = "sigv4a-header-list-secret";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var request = CreateSigV4aHeaderSignedRequest(
            HttpMethod.Get,
            "/integrated-s3/",
            accessKeyId,
            secretAccessKey);
        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task SigV4aHeaderAuthentication_AllowsBucketCreationAndObjectRoundTrip()
    {
        const string accessKeyId = "sigv4a-header-roundtrip-access";
        const string secretAccessKey = "sigv4a-header-roundtrip-secret";
        const string bucketName = "sigv4a-header-roundtrip";
        const string objectKey = "docs/sigv4a.txt";
        const string payload = "hello from sigv4a header auth";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4aHeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/buckets/{bucketName}",
            accessKeyId,
            secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var putObjectRequest = CreateSigV4aHeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            body: payload,
            contentType: "text/plain");
        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(putObjectRequest)).StatusCode);

        using var getObjectRequest = CreateSigV4aHeaderSignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey);
        var getObjectResponse = await client.SendAsync(getObjectRequest);

        Assert.Equal(HttpStatusCode.OK, getObjectResponse.StatusCode);
        Assert.Equal(payload, await getObjectResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task SigV4aHeaderAuthentication_WrongSecretKey_ReturnsSignatureDoesNotMatch()
    {
        const string accessKeyId = "sigv4a-header-wrong-secret-access";
        const string correctSecret = "sigv4a-header-wrong-secret-correct";
        const string wrongSecret = "sigv4a-header-wrong-secret-wrong";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, correctSecret);
        using var client = isolatedClient.Client;

        using var request = CreateSigV4aHeaderSignedRequest(
            HttpMethod.Get,
            "/integrated-s3/",
            accessKeyId,
            wrongSecret);
        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("SignatureDoesNotMatch", GetRequiredElementValue(errorDocument, "Code"));
    }

    [Fact]
    public async Task SigV4aHeaderAuthentication_UnknownAccessKeyId_ReturnsInvalidAccessKeyId()
    {
        const string accessKeyId = "sigv4a-header-unknown-key-access";
        const string secretAccessKey = "sigv4a-header-unknown-key-secret";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var request = CreateSigV4aHeaderSignedRequest(
            HttpMethod.Get,
            "/integrated-s3/",
            "nonexistent-access-key",
            secretAccessKey);
        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("InvalidAccessKeyId", GetRequiredElementValue(errorDocument, "Code"));
    }

    [Fact]
    public async Task SigV4aHeaderAuthentication_SpecificRegionSet_Succeeds()
    {
        const string accessKeyId = "sigv4a-header-region-access";
        const string secretAccessKey = "sigv4a-header-region-secret";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        // Use configured region "us-east-1" instead of wildcard
        using var request = CreateSigV4aHeaderSignedRequest(
            HttpMethod.Get,
            "/integrated-s3/",
            accessKeyId,
            secretAccessKey,
            regionSet: ["us-east-1"]);
        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    // ── Presigned-query SigV4a ──────────────────────────────────────────────

    [Fact]
    public async Task SigV4aPresignedQueryAuthentication_AllowsObjectRetrieval()
    {
        const string accessKeyId = "sigv4a-presigned-roundtrip-access";
        const string secretAccessKey = "sigv4a-presigned-roundtrip-secret";
        const string bucketName = "sigv4a-presigned-roundtrip";
        const string objectKey = "docs/presigned.txt";
        const string payload = "hello from sigv4a presigned";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        // Create bucket and put object with header auth
        using var createBucketRequest = CreateSigV4aHeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/buckets/{bucketName}",
            accessKeyId,
            secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var putObjectRequest = CreateSigV4aHeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            body: payload,
            contentType: "text/plain");
        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(putObjectRequest)).StatusCode);

        // Retrieve with presigned request
        using var presignedRequest = CreateSigV4aPresignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            expiresSeconds: 300);
        var response = await client.SendAsync(presignedRequest);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(payload, await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task SigV4aPresignedQueryAuthentication_WrongSecretKey_ReturnsSignatureDoesNotMatch()
    {
        const string accessKeyId = "sigv4a-presigned-wrong-access";
        const string correctSecret = "sigv4a-presigned-wrong-correct";
        const string wrongSecret = "sigv4a-presigned-wrong-wrong";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, correctSecret);
        using var client = isolatedClient.Client;

        using var presignedRequest = CreateSigV4aPresignedRequest(
            HttpMethod.Get,
            "/integrated-s3/",
            accessKeyId,
            wrongSecret,
            expiresSeconds: 300);
        var response = await client.SendAsync(presignedRequest);

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("SignatureDoesNotMatch", GetRequiredElementValue(errorDocument, "Code"));
    }

    [Fact]
    public async Task SigV4aPresignedQueryAuthentication_ExpiredUrl_ReturnsAccessDenied()
    {
        const string accessKeyId = "sigv4a-presigned-expired-access";
        const string secretAccessKey = "sigv4a-presigned-expired-secret";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var presignedRequest = CreateSigV4aPresignedRequest(
            HttpMethod.Get,
            "/integrated-s3/",
            accessKeyId,
            secretAccessKey,
            expiresSeconds: 1,
            signedAtUtc: DateTimeOffset.UtcNow.AddMinutes(-20));
        var response = await client.SendAsync(presignedRequest);

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("AccessDenied", GetRequiredElementValue(errorDocument, "Code"));
    }

    // ── Streaming aws-chunked trailer (SigV4a) ─────────────────────────────

    [Fact]
    public async Task SigV4aHeaderAuthentication_AllowsAwsChunkedTrailerChecksumUploadsWithTrailerSignature()
    {
        const string accessKeyId = "sigv4a-chunked-trailer-access";
        const string secretAccessKey = "sigv4a-chunked-trailer-secret";
        const string bucketName = "sigv4a-chunked-trailer-bucket";
        const string objectKey = "docs/trailer-sigv4a.txt";
        const string payload = "hello from sigv4a aws chunked trailer";
        var checksum = ComputeSha256Base64(payload);

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4aHeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/buckets/{bucketName}",
            accessKeyId,
            secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var putObjectRequest = CreateSigV4aAwsChunkedTrailerRequest(
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha256"] = checksum
            },
            includeTrailerSignature: true);
        var putObjectResponse = await client.SendAsync(putObjectRequest);

        Assert.Equal(HttpStatusCode.OK, putObjectResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(putObjectResponse.Headers.GetValues("x-amz-checksum-sha256")));

        using var getObjectRequest = CreateSigV4aHeaderSignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey);
        var getObjectResponse = await client.SendAsync(getObjectRequest);

        Assert.Equal(HttpStatusCode.OK, getObjectResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(getObjectResponse.Headers.GetValues("x-amz-checksum-sha256")));
        Assert.Equal(payload, await getObjectResponse.Content.ReadAsStringAsync());
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private Task<WebUiApplicationFactory.IsolatedWebUiClient> CreateAuthenticatedClientAsync(
        string accessKeyId,
        string secretAccessKey)
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
                        DisplayName = "sigv4a-user",
                        Scopes = ["storage.read", "storage.write"]
                    }
                ];
            });
        });
    }

    private static string GetRequiredElementValue(XDocument document, string elementName)
    {
        return document.Root?.Element(S3Ns + elementName)?.Value
            ?? throw new Xunit.Sdk.XunitException($"Missing XML element '{elementName}'.");
    }

    private static string ComputeSha256Base64(string content)
    {
        return Convert.ToBase64String(SHA256.HashData(Encoding.UTF8.GetBytes(content)));
    }

    // ── SigV4a header signing ───────────────────────────────────────────────

    private static HttpRequestMessage CreateSigV4aHeaderSignedRequest(
        HttpMethod method,
        string pathAndQuery,
        string accessKeyId,
        string secretAccessKey,
        string? body = null,
        string? contentType = null,
        string host = "localhost",
        DateTimeOffset? signedAtUtc = null,
        IReadOnlyList<string>? regionSet = null)
    {
        var request = new HttpRequestMessage(method, pathAndQuery);
        if (body is not null) {
            request.Content = new StringContent(body, Encoding.UTF8, contentType ?? "text/plain");
        }

        var payloadBytes = body is null ? Array.Empty<byte>() : Encoding.UTF8.GetBytes(body);
        var payloadHash = Convert.ToHexStringLower(SHA256.HashData(payloadBytes));
        SignSigV4aHeaderRequest(request, pathAndQuery, accessKeyId, secretAccessKey, payloadHash, host, signedAtUtc, regionSet: regionSet);
        return request;
    }

    private static void SignSigV4aHeaderRequest(
        HttpRequestMessage request,
        string pathAndQuery,
        string accessKeyId,
        string secretAccessKey,
        string payloadHash,
        string host = "localhost",
        DateTimeOffset? signedAtUtc = null,
        IEnumerable<string>? additionalSignedHeaders = null,
        IReadOnlyList<string>? regionSet = null)
    {
        var timestampUtc = signedAtUtc ?? DateTimeOffset.UtcNow;
        var timestampText = timestampUtc.ToString("yyyyMMdd'T'HHmmss'Z'");
        var effectiveRegionSet = regionSet ?? ["*"];

        request.Headers.Host = host;
        request.Headers.Remove("x-amz-date");
        request.Headers.TryAddWithoutValidation("x-amz-date", timestampText);
        request.Headers.Remove("x-amz-content-sha256");
        request.Headers.TryAddWithoutValidation("x-amz-content-sha256", payloadHash);
        request.Headers.Remove("x-amz-region-set");
        request.Headers.TryAddWithoutValidation("x-amz-region-set", string.Join(",", effectiveRegionSet));

        var credentialScope = new S3SigV4CredentialScope
        {
            AccessKeyId = accessKeyId,
            DateStamp = timestampUtc.ToString("yyyyMMdd"),
            Region = "*",
            Service = "s3",
            Terminator = "aws4_request"
        };

        var signedHeaders = new List<string> { "host", "x-amz-content-sha256", "x-amz-date", "x-amz-region-set" };
        if (additionalSignedHeaders is not null) {
            signedHeaders.AddRange(additionalSignedHeaders);
        }

        var normalizedSignedHeaders = signedHeaders
            .Select(static header => header.ToLowerInvariant())
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static h => h, StringComparer.Ordinal)
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

        var credentialScopeString = S3SigV4aSigner.BuildCredentialScopeString(
            credentialScope.DateStamp,
            credentialScope.Service);
        var stringToSign = S3SigV4aSigner.BuildStringToSign(timestampUtc, credentialScopeString, canonicalRequest.CanonicalRequestHashHex);

        using var ecdsaKey = S3SigV4aSigner.DeriveEcdsaKey(secretAccessKey, accessKeyId);
        var signature = S3SigV4aSigner.ComputeSignature(ecdsaKey, stringToSign);
        var authorizationHeader = $"AWS4-ECDSA-P256-SHA256 Credential={accessKeyId}/{credentialScopeString}, SignedHeaders={string.Join(';', normalizedSignedHeaders)}, Signature={signature}";
        request.Headers.Remove("Authorization");
        request.Headers.TryAddWithoutValidation("Authorization", authorizationHeader);
    }

    // ── SigV4a presigned ────────────────────────────────────────────────────

    private static HttpRequestMessage CreateSigV4aPresignedRequest(
        HttpMethod method,
        string pathAndQuery,
        string accessKeyId,
        string secretAccessKey,
        int expiresSeconds,
        string host = "localhost",
        DateTimeOffset? signedAtUtc = null,
        IReadOnlyList<string>? regionSet = null)
    {
        var timestampUtc = signedAtUtc ?? DateTimeOffset.UtcNow;
        var effectiveRegionSet = regionSet ?? ["*"];

        var credentialScope = new S3SigV4CredentialScope
        {
            AccessKeyId = accessKeyId,
            DateStamp = timestampUtc.ToString("yyyyMMdd"),
            Region = "*",
            Service = "s3",
            Terminator = "aws4_request"
        };

        var credentialScopeString = S3SigV4aSigner.BuildCredentialScopeString(
            credentialScope.DateStamp,
            credentialScope.Service);

        var uri = CreateUri(pathAndQuery, host);
        var baseQuery = S3SigV4QueryStringParser.Parse(uri.Query).ToList();

        baseQuery.AddRange(
        [
            new KeyValuePair<string, string?>("X-Amz-Algorithm", "AWS4-ECDSA-P256-SHA256"),
            new KeyValuePair<string, string?>("X-Amz-Credential", $"{accessKeyId}/{credentialScopeString}"),
            new KeyValuePair<string, string?>("X-Amz-Date", timestampUtc.ToString("yyyyMMdd'T'HHmmss'Z'")),
            new KeyValuePair<string, string?>("X-Amz-Expires", expiresSeconds.ToString()),
            new KeyValuePair<string, string?>("X-Amz-Region-Set", string.Join(",", effectiveRegionSet)),
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
        var stringToSign = S3SigV4aSigner.BuildStringToSign(timestampUtc, credentialScopeString, canonicalRequest.CanonicalRequestHashHex);

        using var ecdsaKey = S3SigV4aSigner.DeriveEcdsaKey(secretAccessKey, accessKeyId);
        var signature = S3SigV4aSigner.ComputeSignature(ecdsaKey, stringToSign);

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

    // ── SigV4a aws-chunked trailer ──────────────────────────────────────────

    private static HttpRequestMessage CreateSigV4aAwsChunkedTrailerRequest(
        string pathAndQuery,
        string accessKeyId,
        string secretAccessKey,
        string payload,
        IReadOnlyDictionary<string, string> trailerHeaders,
        string payloadHashOverride = StreamingSigV4aPayloadTrailer,
        bool includeTrailerSignature = false,
        string host = "localhost",
        DateTimeOffset? signedAtUtc = null)
    {
        var timestampUtc = signedAtUtc ?? DateTimeOffset.UtcNow;
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

        SignSigV4aHeaderRequest(
            request,
            pathAndQuery,
            accessKeyId,
            secretAccessKey,
            payloadHashOverride,
            host: host,
            signedAtUtc: timestampUtc,
            additionalSignedHeaders:
            [
                "content-encoding",
                "x-amz-decoded-content-length",
                "x-amz-sdk-checksum-algorithm",
                "x-amz-trailer"
            ]);

        var seedSignature = GetAuthorizationSignature(request);

        byte[] contentBytes;
        if (string.Equals(payloadHashOverride, StreamingUnsignedPayloadTrailer, StringComparison.OrdinalIgnoreCase)) {
            contentBytes = BuildUnsignedAwsChunkedPayload(payloadBytes, trailerHeaders);
        }
        else {
            contentBytes = BuildSigV4aAwsChunkedPayload(
                secretAccessKey,
                accessKeyId,
                timestampUtc,
                seedSignature,
                payloadBytes,
                trailerHeaders,
                includeTrailerSignature);
        }

        request.Content = new ByteArrayContent(contentBytes);
        request.Content.Headers.TryAddWithoutValidation("Content-Type", "text/plain");
        request.Content.Headers.ContentEncoding.Add("aws-chunked");

        return request;
    }

    private static byte[] BuildSigV4aAwsChunkedPayload(
        string secretAccessKey,
        string accessKeyId,
        DateTimeOffset signedAtUtc,
        string seedSignature,
        byte[] payloadBytes,
        IReadOnlyDictionary<string, string> trailerHeaders,
        bool includeTrailerSignature)
    {
        var credentialScopeString = S3SigV4aSigner.BuildCredentialScopeString(
            signedAtUtc.ToString("yyyyMMdd"),
            "s3");

        var payloadChunkSignature = ComputeSigV4aChunkSignature(secretAccessKey, accessKeyId, credentialScopeString, signedAtUtc, seedSignature, payloadBytes);
        var finalChunkSignature = ComputeSigV4aChunkSignature(secretAccessKey, accessKeyId, credentialScopeString, signedAtUtc, payloadChunkSignature, Array.Empty<byte>());

        string? trailerSignature = null;
        if (includeTrailerSignature) {
            trailerSignature = ComputeSigV4aTrailerSignature(secretAccessKey, accessKeyId, credentialScopeString, signedAtUtc, finalChunkSignature, trailerHeaders);
        }

        using var stream = new MemoryStream();
        WriteAscii(stream, $"{payloadBytes.Length:x};chunk-signature={payloadChunkSignature}\r\n");
        stream.Write(payloadBytes, 0, payloadBytes.Length);
        WriteAscii(stream, "\r\n");
        WriteAscii(stream, $"0;chunk-signature={finalChunkSignature}\r\n");
        foreach (var trailerHeader in trailerHeaders.OrderBy(static header => header.Key, StringComparer.Ordinal)) {
            WriteAscii(stream, $"{trailerHeader.Key}:{trailerHeader.Value}\r\n");
        }

        if (trailerSignature is not null) {
            WriteAscii(stream, $"x-amz-trailer-signature:{trailerSignature}\r\n");
        }
        WriteAscii(stream, "\r\n");
        return stream.ToArray();
    }

    private static string ComputeSigV4aChunkSignature(
        string secretAccessKey,
        string accessKeyId,
        string credentialScopeString,
        DateTimeOffset signedAtUtc,
        string previousSignature,
        ReadOnlySpan<byte> chunkData)
    {
        var stringToSign = S3SigV4aSigner.BuildStreamingPayloadStringToSign(
            signedAtUtc,
            credentialScopeString,
            previousSignature,
            S3SigV4Signer.ComputeSha256Hex(chunkData));

        using var ecdsaKey = S3SigV4aSigner.DeriveEcdsaKey(secretAccessKey, accessKeyId);
        return S3SigV4aSigner.ComputeSignature(ecdsaKey, stringToSign);
    }

    private static string ComputeSigV4aTrailerSignature(
        string secretAccessKey,
        string accessKeyId,
        string credentialScopeString,
        DateTimeOffset signedAtUtc,
        string previousSignature,
        IReadOnlyDictionary<string, string> trailerHeaders)
    {
        var canonicalTrailerHeaders = S3SigV4Signer.BuildCanonicalStreamingTrailerHeaders(trailerHeaders);
        var stringToSign = S3SigV4aSigner.BuildStreamingTrailerStringToSign(
            signedAtUtc,
            credentialScopeString,
            previousSignature,
            S3SigV4Signer.ComputeSha256Hex(canonicalTrailerHeaders));

        using var ecdsaKey = S3SigV4aSigner.DeriveEcdsaKey(secretAccessKey, accessKeyId);
        return S3SigV4aSigner.ComputeSignature(ecdsaKey, stringToSign);
    }

    private static byte[] BuildUnsignedAwsChunkedPayload(byte[] payloadBytes, IReadOnlyDictionary<string, string>? trailerHeaders)
    {
        using var stream = new MemoryStream();
        WriteAscii(stream, $"{payloadBytes.Length:x}\r\n");
        stream.Write(payloadBytes, 0, payloadBytes.Length);
        WriteAscii(stream, "\r\n0\r\n");
        if (trailerHeaders is not null) {
            foreach (var trailerHeader in trailerHeaders) {
                WriteAscii(stream, $"{trailerHeader.Key}:{trailerHeader.Value}\r\n");
            }
        }

        WriteAscii(stream, "\r\n");
        return stream.ToArray();
    }

    // ── Shared utilities ────────────────────────────────────────────────────

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

    private static Uri CreateUri(string pathAndQuery, string host)
    {
        return new Uri($"http://{host}{pathAndQuery}", UriKind.Absolute);
    }

    private static IEnumerable<KeyValuePair<string, string?>> EnumerateQueryParameters(Uri uri)
    {
        return S3SigV4QueryStringParser.Parse(uri.Query);
    }

    private static void WriteAscii(Stream stream, string value)
    {
        var bytes = Encoding.ASCII.GetBytes(value);
        stream.Write(bytes, 0, bytes.Length);
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
}
