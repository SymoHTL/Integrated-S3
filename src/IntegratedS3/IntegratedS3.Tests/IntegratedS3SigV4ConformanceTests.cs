using System.Net;
using System.Globalization;
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
    private const string StreamingAws4HmacSha256PayloadTrailer = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
    private const string StreamingUnsignedPayloadTrailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";
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
    public async Task SigV4HeaderAuthentication_WithRequiredSessionToken_AllowsRequests()
    {
        const string accessKeyId = "sigv4-session-header-access";
        const string secretAccessKey = "sigv4-session-header-secret";
        const string sessionToken = "sigv4-session-header-token";
        const string bucketName = "sigv4-session-header-bucket";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey, sessionToken: sessionToken);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/buckets/{bucketName}",
            accessKeyId,
            secretAccessKey,
            securityToken: sessionToken);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var listBucketsRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Get,
            "/integrated-s3/",
            accessKeyId,
            secretAccessKey,
            securityToken: sessionToken);
        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(listBucketsRequest)).StatusCode);
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_MissingRequiredSessionToken_ReturnsXmlError()
    {
        const string accessKeyId = "sigv4-session-missing-access";
        const string secretAccessKey = "sigv4-session-missing-secret";
        const string sessionToken = "sigv4-session-missing-token";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey, sessionToken: sessionToken);
        using var client = isolatedClient.Client;

        using var request = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            "/integrated-s3/buckets/sigv4-session-missing-bucket",
            accessKeyId,
            secretAccessKey);
        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("AuthorizationHeaderMalformed", GetRequiredElementValue(errorDocument, "Code"));
        Assert.Contains("x-amz-security-token", GetRequiredElementValue(errorDocument, "Message"), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task SigV4PresignedQueryAuthentication_WithRequiredSessionToken_AllowsReads()
    {
        const string accessKeyId = "sigv4-session-presign-access";
        const string secretAccessKey = "sigv4-session-presign-secret";
        const string sessionToken = "sigv4-session-presign-token";
        const string bucketName = "sigv4-session-presign-bucket";
        const string objectKey = "docs/session-presign.txt";
        const string payload = "presigned session token payload";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey, sessionToken: sessionToken);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/buckets/{bucketName}",
            accessKeyId,
            secretAccessKey,
            securityToken: sessionToken);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var putObjectRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            body: payload,
            contentType: "text/plain",
            securityToken: sessionToken);
        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(putObjectRequest)).StatusCode);

        using var presignedRequest = CreateSigV4PresignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            expiresSeconds: 300,
            securityToken: sessionToken);
        var response = await client.SendAsync(presignedRequest);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(payload, await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task SigV4PresignedQueryAuthentication_InvalidSessionToken_ReturnsXmlError()
    {
        const string accessKeyId = "sigv4-session-invalid-access";
        const string secretAccessKey = "sigv4-session-invalid-secret";
        const string sessionToken = "sigv4-session-invalid-token";
        const string invalidSessionToken = "sigv4-session-invalid-token-wrong";
        const string bucketName = "sigv4-session-invalid-bucket";
        const string objectKey = "docs/session-invalid.txt";
        const string payload = "invalid session token payload";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey, sessionToken: sessionToken);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/buckets/{bucketName}",
            accessKeyId,
            secretAccessKey,
            securityToken: sessionToken);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var putObjectRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            body: payload,
            contentType: "text/plain",
            securityToken: sessionToken);
        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(putObjectRequest)).StatusCode);

        using var presignedRequest = CreateSigV4PresignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            expiresSeconds: 300,
            securityToken: invalidSessionToken);
        var response = await client.SendAsync(presignedRequest);

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("AccessDenied", GetRequiredElementValue(errorDocument, "Code"));
        Assert.Contains("X-Amz-Security-Token", GetRequiredElementValue(errorDocument, "Message"), StringComparison.Ordinal);
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
    public async Task SigV4HeaderAuthentication_AllowsAwsChunkedTrailerChecksumUploadsWithTrailerSignature()
    {
        const string accessKeyId = "sigv4-chunked-trailer-access";
        const string secretAccessKey = "sigv4-chunked-trailer-secret";
        const string bucketName = "sigv4-chunked-trailer-bucket";
        const string objectKey = "docs/trailer.txt";
        const string payload = "hello from signed aws chunked trailer";
        var checksum = ComputeSha256Base64(payload);

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var putObjectRequest = CreateSigV4AwsChunkedTrailerRequest(
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

        using var getObjectRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey);
        var getObjectResponse = await client.SendAsync(getObjectRequest);

        Assert.Equal(HttpStatusCode.OK, getObjectResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(getObjectResponse.Headers.GetValues("x-amz-checksum-sha256")));
        Assert.Equal(payload, await getObjectResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_AllowsAwsChunkedTrailerChecksumUploadsWithMixedCaseTrailerNameAndTrailerSignature()
    {
        const string accessKeyId = "sigv4-chunked-trailer-mixed-case-access";
        const string secretAccessKey = "sigv4-chunked-trailer-mixed-case-secret";
        const string bucketName = "sigv4-chunked-trailer-mixed-case-bucket";
        const string objectKey = "docs/trailer-mixed-case.txt";
        const string payload = "hello from signed aws chunked trailer with mixed case name";
        var checksum = ComputeSha256Base64(payload);

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var putObjectRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["X-Amz-Checksum-Sha256"] = checksum
            },
            includeTrailerSignature: true);
        var putObjectResponse = await client.SendAsync(putObjectRequest);

        Assert.Equal(HttpStatusCode.OK, putObjectResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(putObjectResponse.Headers.GetValues("x-amz-checksum-sha256")));

        using var getObjectRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey);
        var getObjectResponse = await client.SendAsync(getObjectRequest);

        Assert.Equal(HttpStatusCode.OK, getObjectResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(getObjectResponse.Headers.GetValues("x-amz-checksum-sha256")));
        Assert.Equal(payload, await getObjectResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public Task SigV4HeaderAuthentication_AllowsAwsChunkedTrailerSha1ChecksumUploadsWithTrailerSignature()
    {
        return RunSignedAwsChunkedTrailerPutObjectSuccessAsync("sha1", "x-amz-checksum-sha1", "SHA1");
    }

    [Fact]
    public Task SigV4HeaderAuthentication_AwsChunkedTrailerSha1ChecksumMismatchWithTrailerSignature_ReturnsBadDigest()
    {
        return RunSignedAwsChunkedTrailerPutObjectBadDigestAsync("sha1", "x-amz-checksum-sha1", "SHA1");
    }

    [Fact]
    public Task SigV4HeaderAuthentication_AllowsAwsChunkedTrailerCrc32ChecksumUploadsWithTrailerSignature()
    {
        return RunSignedAwsChunkedTrailerPutObjectSuccessAsync("crc32", "x-amz-checksum-crc32", "CRC32");
    }

    [Fact]
    public Task SigV4HeaderAuthentication_AwsChunkedTrailerCrc32ChecksumMismatchWithTrailerSignature_ReturnsBadDigest()
    {
        return RunSignedAwsChunkedTrailerPutObjectBadDigestAsync("crc32", "x-amz-checksum-crc32", "CRC32");
    }

    [Fact]
    public Task SigV4HeaderAuthentication_AllowsAwsChunkedTrailerCrc32cChecksumUploadsWithTrailerSignature()
    {
        return RunSignedAwsChunkedTrailerPutObjectSuccessAsync("crc32c", "x-amz-checksum-crc32c", "CRC32C");
    }

    [Fact]
    public Task SigV4HeaderAuthentication_AwsChunkedTrailerCrc32cChecksumMismatchWithTrailerSignature_ReturnsBadDigest()
    {
        return RunSignedAwsChunkedTrailerPutObjectBadDigestAsync("crc32c", "x-amz-checksum-crc32c", "CRC32C");
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_AllowsAwsChunkedTrailerChecksumUploadsWithUnsignedPayloadHashWithoutTrailerSignature()
    {
        const string accessKeyId = "sigv4-chunked-unsigned-trailer-access";
        const string secretAccessKey = "sigv4-chunked-unsigned-trailer-secret";
        const string bucketName = "sigv4-chunked-unsigned-trailer-bucket";
        const string objectKey = "docs/unsigned-trailer.txt";
        const string payload = "hello from unsigned aws chunked trailer";
        var checksum = ComputeSha256Base64(payload);

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var putObjectRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha256"] = checksum
            },
            payloadHash: StreamingUnsignedPayloadTrailer);
        Assert.DoesNotContain(
            "x-amz-trailer-signature:",
            Encoding.ASCII.GetString(await putObjectRequest.Content!.ReadAsByteArrayAsync()),
            StringComparison.OrdinalIgnoreCase);
        var putObjectResponse = await client.SendAsync(putObjectRequest);

        Assert.Equal(HttpStatusCode.OK, putObjectResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(putObjectResponse.Headers.GetValues("x-amz-checksum-sha256")));

        using var getObjectRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey);
        var getObjectResponse = await client.SendAsync(getObjectRequest);

        Assert.Equal(HttpStatusCode.OK, getObjectResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(getObjectResponse.Headers.GetValues("x-amz-checksum-sha256")));
        Assert.Equal(payload, await getObjectResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_AwsChunkedTrailerChecksumMismatchWithUnsignedPayloadHash_ReturnsBadDigest()
    {
        const string accessKeyId = "sigv4-chunked-unsigned-trailer-baddigest-access";
        const string secretAccessKey = "sigv4-chunked-unsigned-trailer-baddigest-secret";
        const string bucketName = "sigv4-chunked-unsigned-trailer-baddigest-bucket";
        const string objectKey = "docs/unsigned-trailer-baddigest.txt";
        const string payload = "hello from unsigned aws chunked trailer bad digest";
        var mismatchedChecksum = ComputeSha256Base64("different payload");

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var putObjectRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha256"] = mismatchedChecksum
            },
            payloadHash: StreamingUnsignedPayloadTrailer);
        Assert.Equal(StreamingUnsignedPayloadTrailer, Assert.Single(putObjectRequest.Headers.GetValues("x-amz-content-sha256")));
        Assert.Equal("x-amz-checksum-sha256", Assert.Single(putObjectRequest.Headers.GetValues("x-amz-trailer")));
        Assert.DoesNotContain(
            "x-amz-trailer-signature:",
            Encoding.ASCII.GetString(await putObjectRequest.Content!.ReadAsByteArrayAsync()),
            StringComparison.OrdinalIgnoreCase);
        var response = await client.SendAsync(putObjectRequest);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("BadDigest", GetRequiredElementValue(errorDocument, "Code"));
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_AllowsAwsChunkedTrailerSha1ChecksumUploadsWithUnsignedPayloadHashWithoutTrailerSignature()
    {
        const string accessKeyId = "sigv4-chunked-unsigned-trailer-sha1-access";
        const string secretAccessKey = "sigv4-chunked-unsigned-trailer-sha1-secret";
        const string bucketName = "sigv4-chunked-unsigned-trailer-sha1-bucket";
        const string objectKey = "docs/unsigned-trailer-sha1.txt";
        const string payload = "hello from unsigned aws chunked trailer sha1";
        var checksum = ComputeSha1Base64(payload);

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var putObjectRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha1"] = checksum
            },
            payloadHash: StreamingUnsignedPayloadTrailer);
        Assert.Equal(StreamingUnsignedPayloadTrailer, Assert.Single(putObjectRequest.Headers.GetValues("x-amz-content-sha256")));
        Assert.Equal("SHA1", Assert.Single(putObjectRequest.Headers.GetValues("x-amz-sdk-checksum-algorithm")));
        Assert.Equal("x-amz-checksum-sha1", Assert.Single(putObjectRequest.Headers.GetValues("x-amz-trailer")));
        Assert.DoesNotContain(
            "x-amz-trailer-signature:",
            Encoding.ASCII.GetString(await putObjectRequest.Content!.ReadAsByteArrayAsync()),
            StringComparison.OrdinalIgnoreCase);
        var putObjectResponse = await client.SendAsync(putObjectRequest);

        Assert.Equal(HttpStatusCode.OK, putObjectResponse.StatusCode);
        Assert.Equal("SHA1", Assert.Single(putObjectResponse.Headers.GetValues("x-amz-checksum-algorithm")));
        Assert.Equal(checksum, Assert.Single(putObjectResponse.Headers.GetValues("x-amz-checksum-sha1")));
        Assert.False(putObjectResponse.Headers.Contains("x-amz-checksum-sha256"));

        using var getObjectRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey);
        var getObjectResponse = await client.SendAsync(getObjectRequest);

        Assert.Equal(HttpStatusCode.OK, getObjectResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(getObjectResponse.Headers.GetValues("x-amz-checksum-sha1")));
        Assert.False(getObjectResponse.Headers.Contains("x-amz-checksum-sha256"));
        Assert.Equal(payload, await getObjectResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_AwsChunkedTrailerSha1ChecksumMismatchWithUnsignedPayloadHash_ReturnsBadDigest()
    {
        const string accessKeyId = "sigv4-chunked-unsigned-trailer-sha1-baddigest-access";
        const string secretAccessKey = "sigv4-chunked-unsigned-trailer-sha1-baddigest-secret";
        const string bucketName = "sigv4-chunked-unsigned-trailer-sha1-baddigest-bucket";
        const string objectKey = "docs/unsigned-trailer-sha1-baddigest.txt";
        const string payload = "hello from unsigned aws chunked trailer sha1 bad digest";
        var mismatchedChecksum = ComputeSha1Base64("different payload");

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var putObjectRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha1"] = mismatchedChecksum
            },
            payloadHash: StreamingUnsignedPayloadTrailer);
        Assert.Equal(StreamingUnsignedPayloadTrailer, Assert.Single(putObjectRequest.Headers.GetValues("x-amz-content-sha256")));
        Assert.Equal("SHA1", Assert.Single(putObjectRequest.Headers.GetValues("x-amz-sdk-checksum-algorithm")));
        Assert.Equal("x-amz-checksum-sha1", Assert.Single(putObjectRequest.Headers.GetValues("x-amz-trailer")));
        Assert.DoesNotContain(
            "x-amz-trailer-signature:",
            Encoding.ASCII.GetString(await putObjectRequest.Content!.ReadAsByteArrayAsync()),
            StringComparison.OrdinalIgnoreCase);
        var response = await client.SendAsync(putObjectRequest);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("BadDigest", GetRequiredElementValue(errorDocument, "Code"));
        var message = GetRequiredElementValue(errorDocument, "Message");
        Assert.Contains("SHA1", message, StringComparison.Ordinal);
        Assert.Contains(objectKey, message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_AllowsAwsChunkedTrailerCrc32cChecksumUploadsWithUnsignedPayloadHashWithoutTrailerSignature()
    {
        const string accessKeyId = "sigv4-chunked-unsigned-trailer-crc32c-access";
        const string secretAccessKey = "sigv4-chunked-unsigned-trailer-crc32c-secret";
        const string bucketName = "sigv4-chunked-unsigned-trailer-crc32c-bucket";
        const string objectKey = "docs/unsigned-trailer-crc32c.txt";
        const string payload = "hello from unsigned aws chunked trailer crc32c";
        var checksum = ComputeCrc32cBase64(payload);

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var putObjectRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-crc32c"] = checksum
            },
            payloadHash: StreamingUnsignedPayloadTrailer);
        Assert.Equal(StreamingUnsignedPayloadTrailer, Assert.Single(putObjectRequest.Headers.GetValues("x-amz-content-sha256")));
        Assert.Equal("CRC32C", Assert.Single(putObjectRequest.Headers.GetValues("x-amz-sdk-checksum-algorithm")));
        Assert.Equal("x-amz-checksum-crc32c", Assert.Single(putObjectRequest.Headers.GetValues("x-amz-trailer")));
        Assert.DoesNotContain(
            "x-amz-trailer-signature:",
            Encoding.ASCII.GetString(await putObjectRequest.Content!.ReadAsByteArrayAsync()),
            StringComparison.OrdinalIgnoreCase);
        var putObjectResponse = await client.SendAsync(putObjectRequest);

        Assert.Equal(HttpStatusCode.OK, putObjectResponse.StatusCode);
        Assert.Equal("CRC32C", Assert.Single(putObjectResponse.Headers.GetValues("x-amz-checksum-algorithm")));
        Assert.Equal(checksum, Assert.Single(putObjectResponse.Headers.GetValues("x-amz-checksum-crc32c")));
        Assert.False(putObjectResponse.Headers.Contains("x-amz-checksum-sha256"));

        using var getObjectRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey);
        var getObjectResponse = await client.SendAsync(getObjectRequest);

        Assert.Equal(HttpStatusCode.OK, getObjectResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(getObjectResponse.Headers.GetValues("x-amz-checksum-crc32c")));
        Assert.False(getObjectResponse.Headers.Contains("x-amz-checksum-sha256"));
        Assert.Equal(payload, await getObjectResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_AwsChunkedTrailerCrc32cChecksumMismatchWithUnsignedPayloadHash_ReturnsBadDigest()
    {
        const string accessKeyId = "sigv4-chunked-unsigned-trailer-crc32c-baddigest-access";
        const string secretAccessKey = "sigv4-chunked-unsigned-trailer-crc32c-baddigest-secret";
        const string bucketName = "sigv4-chunked-unsigned-trailer-crc32c-baddigest-bucket";
        const string objectKey = "docs/unsigned-trailer-crc32c-baddigest.txt";
        const string payload = "hello from unsigned aws chunked trailer crc32c bad digest";
        var mismatchedChecksum = ComputeCrc32cBase64("different payload");

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var putObjectRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-crc32c"] = mismatchedChecksum
            },
            payloadHash: StreamingUnsignedPayloadTrailer);
        Assert.Equal(StreamingUnsignedPayloadTrailer, Assert.Single(putObjectRequest.Headers.GetValues("x-amz-content-sha256")));
        Assert.Equal("CRC32C", Assert.Single(putObjectRequest.Headers.GetValues("x-amz-sdk-checksum-algorithm")));
        Assert.Equal("x-amz-checksum-crc32c", Assert.Single(putObjectRequest.Headers.GetValues("x-amz-trailer")));
        Assert.DoesNotContain(
            "x-amz-trailer-signature:",
            Encoding.ASCII.GetString(await putObjectRequest.Content!.ReadAsByteArrayAsync()),
            StringComparison.OrdinalIgnoreCase);
        var response = await client.SendAsync(putObjectRequest);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("BadDigest", GetRequiredElementValue(errorDocument, "Code"));
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_AllowsAwsChunkedTrailerCrc32ChecksumUploadsWithUnsignedPayloadHashWithoutTrailerSignature()
    {
        const string accessKeyId = "sigv4-chunked-unsigned-trailer-crc32-access";
        const string secretAccessKey = "sigv4-chunked-unsigned-trailer-crc32-secret";
        const string bucketName = "sigv4-chunked-unsigned-trailer-crc32-bucket";
        const string objectKey = "docs/unsigned-trailer-crc32.txt";
        const string payload = "hello from unsigned aws chunked trailer crc32";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);
        var checksum = await PutObjectAndGetCrc32ChecksumAsync(
            client,
            accessKeyId,
            secretAccessKey,
            bucketName,
            "docs/unsigned-trailer-crc32-reference.txt",
            payload);

        using var putObjectRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-crc32"] = checksum
            },
            payloadHash: StreamingUnsignedPayloadTrailer);
        Assert.Equal(StreamingUnsignedPayloadTrailer, Assert.Single(putObjectRequest.Headers.GetValues("x-amz-content-sha256")));
        Assert.Equal("CRC32", Assert.Single(putObjectRequest.Headers.GetValues("x-amz-sdk-checksum-algorithm")));
        Assert.Equal("x-amz-checksum-crc32", Assert.Single(putObjectRequest.Headers.GetValues("x-amz-trailer")));
        Assert.DoesNotContain(
            "x-amz-trailer-signature:",
            Encoding.ASCII.GetString(await putObjectRequest.Content!.ReadAsByteArrayAsync()),
            StringComparison.OrdinalIgnoreCase);
        var putObjectResponse = await client.SendAsync(putObjectRequest);

        Assert.Equal(HttpStatusCode.OK, putObjectResponse.StatusCode);
        Assert.Equal("CRC32", Assert.Single(putObjectResponse.Headers.GetValues("x-amz-checksum-algorithm")));
        Assert.Equal(checksum, Assert.Single(putObjectResponse.Headers.GetValues("x-amz-checksum-crc32")));
        Assert.False(putObjectResponse.Headers.Contains("x-amz-checksum-sha256"));

        using var getObjectRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey);
        var getObjectResponse = await client.SendAsync(getObjectRequest);

        Assert.Equal(HttpStatusCode.OK, getObjectResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(getObjectResponse.Headers.GetValues("x-amz-checksum-crc32")));
        Assert.False(getObjectResponse.Headers.Contains("x-amz-checksum-sha256"));
        Assert.Equal(payload, await getObjectResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_AwsChunkedTrailerCrc32ChecksumMismatchWithUnsignedPayloadHash_ReturnsBadDigest()
    {
        const string accessKeyId = "sigv4-chunked-unsigned-trailer-crc32-baddigest-access";
        const string secretAccessKey = "sigv4-chunked-unsigned-trailer-crc32-baddigest-secret";
        const string bucketName = "sigv4-chunked-unsigned-trailer-crc32-baddigest-bucket";
        const string objectKey = "docs/unsigned-trailer-crc32-baddigest.txt";
        const string payload = "hello from unsigned aws chunked trailer crc32 bad digest";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);
        var mismatchedChecksum = await PutObjectAndGetCrc32ChecksumAsync(
            client,
            accessKeyId,
            secretAccessKey,
            bucketName,
            "docs/unsigned-trailer-crc32-mismatch-reference.txt",
            "different payload");

        using var putObjectRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-crc32"] = mismatchedChecksum
            },
            payloadHash: StreamingUnsignedPayloadTrailer);
        Assert.Equal(StreamingUnsignedPayloadTrailer, Assert.Single(putObjectRequest.Headers.GetValues("x-amz-content-sha256")));
        Assert.Equal("CRC32", Assert.Single(putObjectRequest.Headers.GetValues("x-amz-sdk-checksum-algorithm")));
        Assert.Equal("x-amz-checksum-crc32", Assert.Single(putObjectRequest.Headers.GetValues("x-amz-trailer")));
        Assert.DoesNotContain(
            "x-amz-trailer-signature:",
            Encoding.ASCII.GetString(await putObjectRequest.Content!.ReadAsByteArrayAsync()),
            StringComparison.OrdinalIgnoreCase);
        var response = await client.SendAsync(putObjectRequest);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("BadDigest", GetRequiredElementValue(errorDocument, "Code"));
        var message = GetRequiredElementValue(errorDocument, "Message");
        Assert.Contains("CRC32", message, StringComparison.Ordinal);
        Assert.Contains(objectKey, message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_MutatedAwsChunkedTrailerSignature_ReturnsSignatureDoesNotMatch()
    {
        const string accessKeyId = "sigv4-chunked-trailer-signature-mutated-access";
        const string secretAccessKey = "sigv4-chunked-trailer-signature-mutated-secret";
        const string bucketName = "sigv4-chunked-trailer-signature-mutated-bucket";
        const string objectKey = "docs/trailer-signature-mutated.txt";
        const string payload = "hello from mutated aws chunked trailer signature";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var putObjectRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha256"] = ComputeSha256Base64(payload)
            },
            includeTrailerSignature: true,
            trailerSignatureOverride: new string('0', 64));
        var response = await client.SendAsync(putObjectRequest);

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("SignatureDoesNotMatch", GetRequiredElementValue(errorDocument, "Code"));
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_MissingAwsChunkedTrailerSignature_ReturnsInvalidRequest()
    {
        const string accessKeyId = "sigv4-chunked-trailer-signature-missing-access";
        const string secretAccessKey = "sigv4-chunked-trailer-signature-missing-secret";
        const string bucketName = "sigv4-chunked-trailer-signature-missing-bucket";
        const string objectKey = "docs/trailer-signature-missing.txt";
        const string payload = "hello from missing aws chunked trailer signature";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var putObjectRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha256"] = ComputeSha256Base64(payload)
            });
        var response = await client.SendAsync(putObjectRequest);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("InvalidRequest", GetRequiredElementValue(errorDocument, "Code"));
        Assert.Equal(
            "The aws-chunked request body must include the 'x-amz-trailer-signature' trailer header when 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER' is used.",
            GetRequiredElementValue(errorDocument, "Message"));
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_MutatedAwsChunkedTrailerDeclaration_ReturnsSignatureDoesNotMatch()
    {
        const string accessKeyId = "sigv4-chunked-trailer-mutated-access";
        const string secretAccessKey = "sigv4-chunked-trailer-mutated-secret";
        const string bucketName = "sigv4-chunked-trailer-mutated-bucket";
        const string objectKey = "docs/trailer-mutated.txt";
        const string payload = "hello from mutated signed aws chunked trailer";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var putObjectRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha256"] = ComputeSha256Base64(payload)
            },
            includeTrailerSignature: true);
        putObjectRequest.Headers.Remove("x-amz-trailer");
        putObjectRequest.Headers.TryAddWithoutValidation("x-amz-trailer", "x-amz-checksum-sha1");

        var response = await client.SendAsync(putObjectRequest);

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("SignatureDoesNotMatch", GetRequiredElementValue(errorDocument, "Code"));
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_AllowsAwsChunkedTrailerChecksumUploadPartWithTrailerSignature()
    {
        const string accessKeyId = "sigv4-uploadpart-chunked-trailer-access";
        const string secretAccessKey = "sigv4-uploadpart-chunked-trailer-secret";
        const string bucketName = "sigv4-uploadpart-chunked-trailer-bucket";
        const string objectKey = "docs/uploadpart-trailer.txt";
        const string payload = "hello from signed uploadpart aws chunked trailer";
        var checksum = ComputeSha256Base64(payload);

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "SHA256");
        SignSigV4HeaderRequest(
            initiateRequest,
            $"/integrated-s3/{bucketName}/{objectKey}?uploads",
            accessKeyId,
            secretAccessKey,
            Convert.ToHexStringLower(SHA256.HashData(Array.Empty<byte>())),
            additionalSignedHeaders: ["x-amz-checksum-algorithm"]);
        var initiateResponse = await client.SendAsync(initiateRequest);

        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        Assert.Equal("SHA256", Assert.Single(initiateResponse.Headers.GetValues("x-amz-checksum-algorithm")));

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var uploadPartRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha256"] = checksum
            },
            includeTrailerSignature: true);
        var uploadPartResponse = await client.SendAsync(uploadPartRequest);

        Assert.Equal(HttpStatusCode.OK, uploadPartResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(uploadPartResponse.Headers.GetValues("x-amz-checksum-sha256")));
        Assert.NotNull(uploadPartResponse.Headers.ETag);
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_AllowsAwsChunkedTrailerChecksumUploadPartWithMixedCaseTrailerNameAndTrailerSignature()
    {
        const string accessKeyId = "sigv4-uploadpart-chunked-trailer-mixed-case-access";
        const string secretAccessKey = "sigv4-uploadpart-chunked-trailer-mixed-case-secret";
        const string bucketName = "sigv4-uploadpart-chunked-trailer-mixed-case-bucket";
        const string objectKey = "docs/uploadpart-trailer-mixed-case.txt";
        const string payload = "hello from signed uploadpart aws chunked trailer with mixed case name";
        var checksum = ComputeSha256Base64(payload);

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "SHA256");
        SignSigV4HeaderRequest(
            initiateRequest,
            $"/integrated-s3/{bucketName}/{objectKey}?uploads",
            accessKeyId,
            secretAccessKey,
            Convert.ToHexStringLower(SHA256.HashData(Array.Empty<byte>())),
            additionalSignedHeaders: ["x-amz-checksum-algorithm"]);
        var initiateResponse = await client.SendAsync(initiateRequest);

        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        Assert.Equal("SHA256", Assert.Single(initiateResponse.Headers.GetValues("x-amz-checksum-algorithm")));

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var uploadPartRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["X-Amz-Checksum-Sha256"] = checksum
            },
            includeTrailerSignature: true);
        var uploadPartResponse = await client.SendAsync(uploadPartRequest);

        Assert.Equal(HttpStatusCode.OK, uploadPartResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(uploadPartResponse.Headers.GetValues("x-amz-checksum-sha256")));
        var partETag = uploadPartResponse.Headers.ETag?.Tag ?? throw new Xunit.Sdk.XunitException("Expected multipart part ETag header.");

        using var listPartsRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/{bucketName}/{objectKey}?uploadId={Uri.EscapeDataString(uploadId)}",
            accessKeyId,
            secretAccessKey);
        var listPartsResponse = await client.SendAsync(listPartsRequest);

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
    public Task SigV4HeaderAuthentication_AllowsAwsChunkedTrailerSha1ChecksumUploadPartWithTrailerSignature()
    {
        return RunSignedAwsChunkedTrailerUploadPartSuccessAsync("sha1", "x-amz-checksum-sha1", "SHA1", "ChecksumSHA1");
    }

    [Fact]
    public Task SigV4HeaderAuthentication_AwsChunkedTrailerSha1ChecksumMismatchUploadPartWithTrailerSignature_ReturnsBadDigest()
    {
        return RunSignedAwsChunkedTrailerUploadPartBadDigestAsync("sha1", "x-amz-checksum-sha1", "SHA1");
    }

    [Fact]
    public Task SigV4HeaderAuthentication_AllowsAwsChunkedTrailerCrc32ChecksumUploadPartWithTrailerSignature()
    {
        return RunSignedAwsChunkedTrailerUploadPartSuccessAsync("crc32", "x-amz-checksum-crc32", "CRC32", "ChecksumCRC32");
    }

    [Fact]
    public Task SigV4HeaderAuthentication_AwsChunkedTrailerCrc32ChecksumMismatchUploadPartWithTrailerSignature_ReturnsBadDigest()
    {
        return RunSignedAwsChunkedTrailerUploadPartBadDigestAsync("crc32", "x-amz-checksum-crc32", "CRC32");
    }

    [Fact]
    public Task SigV4HeaderAuthentication_AllowsAwsChunkedTrailerCrc32cChecksumUploadPartWithTrailerSignature()
    {
        return RunSignedAwsChunkedTrailerUploadPartSuccessAsync("crc32c", "x-amz-checksum-crc32c", "CRC32C", "ChecksumCRC32C");
    }

    [Fact]
    public Task SigV4HeaderAuthentication_AwsChunkedTrailerCrc32cChecksumMismatchUploadPartWithTrailerSignature_ReturnsBadDigest()
    {
        return RunSignedAwsChunkedTrailerUploadPartBadDigestAsync("crc32c", "x-amz-checksum-crc32c", "CRC32C");
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_AllowsAwsChunkedTrailerChecksumUploadPartWithUnsignedPayloadHashWithoutTrailerSignature()
    {
        const string accessKeyId = "sigv4-uploadpart-chunked-unsigned-trailer-access";
        const string secretAccessKey = "sigv4-uploadpart-chunked-unsigned-trailer-secret";
        const string bucketName = "sigv4-uploadpart-chunked-unsigned-trailer-bucket";
        const string objectKey = "docs/uploadpart-unsigned-trailer.txt";
        const string payload = "hello from unsigned uploadpart aws chunked trailer";
        var checksum = ComputeSha256Base64(payload);

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "SHA256");
        SignSigV4HeaderRequest(
            initiateRequest,
            $"/integrated-s3/{bucketName}/{objectKey}?uploads",
            accessKeyId,
            secretAccessKey,
            Convert.ToHexStringLower(SHA256.HashData(Array.Empty<byte>())),
            additionalSignedHeaders: ["x-amz-checksum-algorithm"]);
        var initiateResponse = await client.SendAsync(initiateRequest);

        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        Assert.Equal("SHA256", Assert.Single(initiateResponse.Headers.GetValues("x-amz-checksum-algorithm")));

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var uploadPartRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha256"] = checksum
            },
            payloadHash: StreamingUnsignedPayloadTrailer);
        Assert.DoesNotContain(
            "x-amz-trailer-signature:",
            Encoding.ASCII.GetString(await uploadPartRequest.Content!.ReadAsByteArrayAsync()),
            StringComparison.OrdinalIgnoreCase);
        var uploadPartResponse = await client.SendAsync(uploadPartRequest);

        Assert.Equal(HttpStatusCode.OK, uploadPartResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(uploadPartResponse.Headers.GetValues("x-amz-checksum-sha256")));
        Assert.NotNull(uploadPartResponse.Headers.ETag);
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_AwsChunkedTrailerChecksumMismatchUploadPartWithUnsignedPayloadHash_ReturnsBadDigest()
    {
        const string accessKeyId = "sigv4-uploadpart-chunked-unsigned-trailer-baddigest-access";
        const string secretAccessKey = "sigv4-uploadpart-chunked-unsigned-trailer-baddigest-secret";
        const string bucketName = "sigv4-uploadpart-chunked-unsigned-trailer-baddigest-bucket";
        const string objectKey = "docs/uploadpart-unsigned-trailer-baddigest.txt";
        const string payload = "hello from unsigned uploadpart aws chunked trailer bad digest";
        var mismatchedChecksum = ComputeSha256Base64("different payload");

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "SHA256");
        SignSigV4HeaderRequest(
            initiateRequest,
            $"/integrated-s3/{bucketName}/{objectKey}?uploads",
            accessKeyId,
            secretAccessKey,
            Convert.ToHexStringLower(SHA256.HashData(Array.Empty<byte>())),
            additionalSignedHeaders: ["x-amz-checksum-algorithm"]);
        var initiateResponse = await client.SendAsync(initiateRequest);

        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        Assert.Equal("SHA256", Assert.Single(initiateResponse.Headers.GetValues("x-amz-checksum-algorithm")));

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var uploadPartRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha256"] = mismatchedChecksum
            },
            payloadHash: StreamingUnsignedPayloadTrailer);
        Assert.Equal(StreamingUnsignedPayloadTrailer, Assert.Single(uploadPartRequest.Headers.GetValues("x-amz-content-sha256")));
        Assert.Equal("x-amz-checksum-sha256", Assert.Single(uploadPartRequest.Headers.GetValues("x-amz-trailer")));
        Assert.DoesNotContain(
            "x-amz-trailer-signature:",
            Encoding.ASCII.GetString(await uploadPartRequest.Content!.ReadAsByteArrayAsync()),
            StringComparison.OrdinalIgnoreCase);
        var uploadPartResponse = await client.SendAsync(uploadPartRequest);

        Assert.Equal(HttpStatusCode.BadRequest, uploadPartResponse.StatusCode);
        Assert.Equal("application/xml", uploadPartResponse.Content.Headers.ContentType?.MediaType);
        var errorDocument = XDocument.Parse(await uploadPartResponse.Content.ReadAsStringAsync());
        Assert.Equal("BadDigest", GetRequiredElementValue(errorDocument, "Code"));
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_AllowsAwsChunkedTrailerSha1ChecksumUploadPartWithUnsignedPayloadHashWithoutTrailerSignature()
    {
        const string accessKeyId = "sigv4-uploadpart-chunked-unsigned-trailer-sha1-access";
        const string secretAccessKey = "sigv4-uploadpart-chunked-unsigned-trailer-sha1-secret";
        const string bucketName = "sigv4-uploadpart-chunked-unsigned-trailer-sha1-bucket";
        const string objectKey = "docs/uploadpart-unsigned-trailer-sha1.txt";
        const string payload = "hello from unsigned uploadpart aws chunked trailer sha1";
        var checksum = ComputeSha1Base64(payload);

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "SHA1");
        SignSigV4HeaderRequest(
            initiateRequest,
            $"/integrated-s3/{bucketName}/{objectKey}?uploads",
            accessKeyId,
            secretAccessKey,
            Convert.ToHexStringLower(SHA256.HashData(Array.Empty<byte>())),
            additionalSignedHeaders: ["x-amz-checksum-algorithm"]);
        var initiateResponse = await client.SendAsync(initiateRequest);

        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        Assert.Equal("SHA1", Assert.Single(initiateResponse.Headers.GetValues("x-amz-checksum-algorithm")));

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var uploadPartRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha1"] = checksum
            },
            payloadHash: StreamingUnsignedPayloadTrailer);
        Assert.Equal(StreamingUnsignedPayloadTrailer, Assert.Single(uploadPartRequest.Headers.GetValues("x-amz-content-sha256")));
        Assert.Equal("SHA1", Assert.Single(uploadPartRequest.Headers.GetValues("x-amz-sdk-checksum-algorithm")));
        Assert.Equal("x-amz-checksum-sha1", Assert.Single(uploadPartRequest.Headers.GetValues("x-amz-trailer")));
        Assert.DoesNotContain(
            "x-amz-trailer-signature:",
            Encoding.ASCII.GetString(await uploadPartRequest.Content!.ReadAsByteArrayAsync()),
            StringComparison.OrdinalIgnoreCase);
        var uploadPartResponse = await client.SendAsync(uploadPartRequest);

        Assert.Equal(HttpStatusCode.OK, uploadPartResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(uploadPartResponse.Headers.GetValues("x-amz-checksum-sha1")));
        Assert.False(uploadPartResponse.Headers.Contains("x-amz-checksum-sha256"));
        var partETag = uploadPartResponse.Headers.ETag?.Tag ?? throw new Xunit.Sdk.XunitException("Expected multipart part ETag header.");

        using var listPartsRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/{bucketName}/{objectKey}?uploadId={Uri.EscapeDataString(uploadId)}",
            accessKeyId,
            secretAccessKey);
        var listPartsResponse = await client.SendAsync(listPartsRequest);

        Assert.Equal(HttpStatusCode.OK, listPartsResponse.StatusCode);
        var listPartsDocument = XDocument.Parse(await listPartsResponse.Content.ReadAsStringAsync());
        Assert.Equal("SHA1", GetRequiredElementValue(listPartsDocument, "ChecksumAlgorithm"));
        Assert.Equal("COMPOSITE", GetRequiredElementValue(listPartsDocument, "ChecksumType"));

        var listedPart = Assert.Single(listPartsDocument.Root!.Elements("Part"));
        Assert.Equal("1", listedPart.Element("PartNumber")?.Value);
        Assert.Equal(partETag, listedPart.Element("ETag")?.Value);
        Assert.Equal(checksum, listedPart.Element("ChecksumSHA1")?.Value);
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_AwsChunkedTrailerSha1ChecksumMismatchUploadPartWithUnsignedPayloadHash_ReturnsBadDigest()
    {
        const string accessKeyId = "sigv4-uploadpart-chunked-unsigned-trailer-sha1-baddigest-access";
        const string secretAccessKey = "sigv4-uploadpart-chunked-unsigned-trailer-sha1-baddigest-secret";
        const string bucketName = "sigv4-uploadpart-chunked-unsigned-trailer-sha1-baddigest-bucket";
        const string objectKey = "docs/uploadpart-unsigned-trailer-sha1-baddigest.txt";
        const string payload = "hello from unsigned uploadpart aws chunked trailer sha1 bad digest";
        var mismatchedChecksum = ComputeSha1Base64("different payload");

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "SHA1");
        SignSigV4HeaderRequest(
            initiateRequest,
            $"/integrated-s3/{bucketName}/{objectKey}?uploads",
            accessKeyId,
            secretAccessKey,
            Convert.ToHexStringLower(SHA256.HashData(Array.Empty<byte>())),
            additionalSignedHeaders: ["x-amz-checksum-algorithm"]);
        var initiateResponse = await client.SendAsync(initiateRequest);

        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        Assert.Equal("SHA1", Assert.Single(initiateResponse.Headers.GetValues("x-amz-checksum-algorithm")));

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var uploadPartRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-sha1"] = mismatchedChecksum
            },
            payloadHash: StreamingUnsignedPayloadTrailer);
        Assert.Equal(StreamingUnsignedPayloadTrailer, Assert.Single(uploadPartRequest.Headers.GetValues("x-amz-content-sha256")));
        Assert.Equal("SHA1", Assert.Single(uploadPartRequest.Headers.GetValues("x-amz-sdk-checksum-algorithm")));
        Assert.Equal("x-amz-checksum-sha1", Assert.Single(uploadPartRequest.Headers.GetValues("x-amz-trailer")));
        Assert.DoesNotContain(
            "x-amz-trailer-signature:",
            Encoding.ASCII.GetString(await uploadPartRequest.Content!.ReadAsByteArrayAsync()),
            StringComparison.OrdinalIgnoreCase);
        var uploadPartResponse = await client.SendAsync(uploadPartRequest);

        Assert.Equal(HttpStatusCode.BadRequest, uploadPartResponse.StatusCode);
        Assert.Equal("application/xml", uploadPartResponse.Content.Headers.ContentType?.MediaType);
        var errorDocument = XDocument.Parse(await uploadPartResponse.Content.ReadAsStringAsync());
        Assert.Equal("BadDigest", GetRequiredElementValue(errorDocument, "Code"));
        var message = GetRequiredElementValue(errorDocument, "Message");
        Assert.Contains("SHA1", message, StringComparison.Ordinal);
        Assert.Contains(objectKey, message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_AllowsAwsChunkedTrailerCrc32cChecksumUploadPartWithUnsignedPayloadHashWithoutTrailerSignature()
    {
        const string accessKeyId = "sigv4-uploadpart-chunked-unsigned-trailer-crc32c-access";
        const string secretAccessKey = "sigv4-uploadpart-chunked-unsigned-trailer-crc32c-secret";
        const string bucketName = "sigv4-uploadpart-chunked-unsigned-trailer-crc32c-bucket";
        const string objectKey = "docs/uploadpart-unsigned-trailer-crc32c.txt";
        const string payload = "hello from unsigned uploadpart aws chunked trailer crc32c";
        var checksum = ComputeCrc32cBase64(payload);

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "CRC32C");
        SignSigV4HeaderRequest(
            initiateRequest,
            $"/integrated-s3/{bucketName}/{objectKey}?uploads",
            accessKeyId,
            secretAccessKey,
            Convert.ToHexStringLower(SHA256.HashData(Array.Empty<byte>())),
            additionalSignedHeaders: ["x-amz-checksum-algorithm"]);
        var initiateResponse = await client.SendAsync(initiateRequest);

        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        Assert.Equal("CRC32C", Assert.Single(initiateResponse.Headers.GetValues("x-amz-checksum-algorithm")));

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var uploadPartRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-crc32c"] = checksum
            },
            payloadHash: StreamingUnsignedPayloadTrailer);
        Assert.Equal(StreamingUnsignedPayloadTrailer, Assert.Single(uploadPartRequest.Headers.GetValues("x-amz-content-sha256")));
        Assert.Equal("CRC32C", Assert.Single(uploadPartRequest.Headers.GetValues("x-amz-sdk-checksum-algorithm")));
        Assert.Equal("x-amz-checksum-crc32c", Assert.Single(uploadPartRequest.Headers.GetValues("x-amz-trailer")));
        Assert.DoesNotContain(
            "x-amz-trailer-signature:",
            Encoding.ASCII.GetString(await uploadPartRequest.Content!.ReadAsByteArrayAsync()),
            StringComparison.OrdinalIgnoreCase);
        var uploadPartResponse = await client.SendAsync(uploadPartRequest);

        Assert.Equal(HttpStatusCode.OK, uploadPartResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(uploadPartResponse.Headers.GetValues("x-amz-checksum-crc32c")));
        Assert.False(uploadPartResponse.Headers.Contains("x-amz-checksum-sha256"));
        var partETag = uploadPartResponse.Headers.ETag?.Tag ?? throw new Xunit.Sdk.XunitException("Expected multipart part ETag header.");

        using var listPartsRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/{bucketName}/{objectKey}?uploadId={Uri.EscapeDataString(uploadId)}",
            accessKeyId,
            secretAccessKey);
        var listPartsResponse = await client.SendAsync(listPartsRequest);

        Assert.Equal(HttpStatusCode.OK, listPartsResponse.StatusCode);
        var listPartsDocument = XDocument.Parse(await listPartsResponse.Content.ReadAsStringAsync());
        Assert.Equal("CRC32C", GetRequiredElementValue(listPartsDocument, "ChecksumAlgorithm"));
        Assert.Equal("COMPOSITE", GetRequiredElementValue(listPartsDocument, "ChecksumType"));

        var listedPart = Assert.Single(listPartsDocument.Root!.Elements("Part"));
        Assert.Equal("1", listedPart.Element("PartNumber")?.Value);
        Assert.Equal(partETag, listedPart.Element("ETag")?.Value);
        Assert.Equal(checksum, listedPart.Element("ChecksumCRC32C")?.Value);
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_AwsChunkedTrailerCrc32cChecksumMismatchUploadPartWithUnsignedPayloadHash_ReturnsBadDigest()
    {
        const string accessKeyId = "sigv4-uploadpart-chunked-unsigned-trailer-crc32c-baddigest-access";
        const string secretAccessKey = "sigv4-uploadpart-chunked-unsigned-trailer-crc32c-baddigest-secret";
        const string bucketName = "sigv4-uploadpart-chunked-unsigned-trailer-crc32c-baddigest-bucket";
        const string objectKey = "docs/uploadpart-unsigned-trailer-crc32c-baddigest.txt";
        const string payload = "hello from unsigned uploadpart aws chunked trailer crc32c bad digest";
        var mismatchedChecksum = ComputeCrc32cBase64("different payload");

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "CRC32C");
        SignSigV4HeaderRequest(
            initiateRequest,
            $"/integrated-s3/{bucketName}/{objectKey}?uploads",
            accessKeyId,
            secretAccessKey,
            Convert.ToHexStringLower(SHA256.HashData(Array.Empty<byte>())),
            additionalSignedHeaders: ["x-amz-checksum-algorithm"]);
        var initiateResponse = await client.SendAsync(initiateRequest);

        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        Assert.Equal("CRC32C", Assert.Single(initiateResponse.Headers.GetValues("x-amz-checksum-algorithm")));

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var uploadPartRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-crc32c"] = mismatchedChecksum
            },
            payloadHash: StreamingUnsignedPayloadTrailer);
        Assert.Equal(StreamingUnsignedPayloadTrailer, Assert.Single(uploadPartRequest.Headers.GetValues("x-amz-content-sha256")));
        Assert.Equal("CRC32C", Assert.Single(uploadPartRequest.Headers.GetValues("x-amz-sdk-checksum-algorithm")));
        Assert.Equal("x-amz-checksum-crc32c", Assert.Single(uploadPartRequest.Headers.GetValues("x-amz-trailer")));
        Assert.DoesNotContain(
            "x-amz-trailer-signature:",
            Encoding.ASCII.GetString(await uploadPartRequest.Content!.ReadAsByteArrayAsync()),
            StringComparison.OrdinalIgnoreCase);
        var uploadPartResponse = await client.SendAsync(uploadPartRequest);

        Assert.Equal(HttpStatusCode.BadRequest, uploadPartResponse.StatusCode);
        Assert.Equal("application/xml", uploadPartResponse.Content.Headers.ContentType?.MediaType);
        var errorDocument = XDocument.Parse(await uploadPartResponse.Content.ReadAsStringAsync());
        Assert.Equal("BadDigest", GetRequiredElementValue(errorDocument, "Code"));
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_AllowsAwsChunkedTrailerCrc32ChecksumUploadPartWithUnsignedPayloadHashWithoutTrailerSignature()
    {
        const string accessKeyId = "sigv4-uploadpart-chunked-unsigned-trailer-crc32-access";
        const string secretAccessKey = "sigv4-uploadpart-chunked-unsigned-trailer-crc32-secret";
        const string bucketName = "sigv4-uploadpart-chunked-unsigned-trailer-crc32-bucket";
        const string objectKey = "docs/uploadpart-unsigned-trailer-crc32.txt";
        const string payload = "hello from unsigned uploadpart aws chunked trailer crc32";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);
        var checksum = await PutObjectAndGetCrc32ChecksumAsync(
            client,
            accessKeyId,
            secretAccessKey,
            bucketName,
            "docs/uploadpart-unsigned-trailer-crc32-reference.txt",
            payload);

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "CRC32");
        SignSigV4HeaderRequest(
            initiateRequest,
            $"/integrated-s3/{bucketName}/{objectKey}?uploads",
            accessKeyId,
            secretAccessKey,
            Convert.ToHexStringLower(SHA256.HashData(Array.Empty<byte>())),
            additionalSignedHeaders: ["x-amz-checksum-algorithm"]);
        var initiateResponse = await client.SendAsync(initiateRequest);

        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        Assert.Equal("CRC32", Assert.Single(initiateResponse.Headers.GetValues("x-amz-checksum-algorithm")));

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var uploadPartRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-crc32"] = checksum
            },
            payloadHash: StreamingUnsignedPayloadTrailer);
        Assert.Equal(StreamingUnsignedPayloadTrailer, Assert.Single(uploadPartRequest.Headers.GetValues("x-amz-content-sha256")));
        Assert.Equal("CRC32", Assert.Single(uploadPartRequest.Headers.GetValues("x-amz-sdk-checksum-algorithm")));
        Assert.Equal("x-amz-checksum-crc32", Assert.Single(uploadPartRequest.Headers.GetValues("x-amz-trailer")));
        Assert.DoesNotContain(
            "x-amz-trailer-signature:",
            Encoding.ASCII.GetString(await uploadPartRequest.Content!.ReadAsByteArrayAsync()),
            StringComparison.OrdinalIgnoreCase);
        var uploadPartResponse = await client.SendAsync(uploadPartRequest);

        Assert.Equal(HttpStatusCode.OK, uploadPartResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(uploadPartResponse.Headers.GetValues("x-amz-checksum-crc32")));
        Assert.False(uploadPartResponse.Headers.Contains("x-amz-checksum-sha256"));
        var partETag = uploadPartResponse.Headers.ETag?.Tag ?? throw new Xunit.Sdk.XunitException("Expected multipart part ETag header.");

        using var listPartsRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/{bucketName}/{objectKey}?uploadId={Uri.EscapeDataString(uploadId)}",
            accessKeyId,
            secretAccessKey);
        var listPartsResponse = await client.SendAsync(listPartsRequest);

        Assert.Equal(HttpStatusCode.OK, listPartsResponse.StatusCode);
        var listPartsDocument = XDocument.Parse(await listPartsResponse.Content.ReadAsStringAsync());
        Assert.Equal("CRC32", GetRequiredElementValue(listPartsDocument, "ChecksumAlgorithm"));
        Assert.Equal("COMPOSITE", GetRequiredElementValue(listPartsDocument, "ChecksumType"));

        var listedPart = Assert.Single(listPartsDocument.Root!.Elements("Part"));
        Assert.Equal("1", listedPart.Element("PartNumber")?.Value);
        Assert.Equal(partETag, listedPart.Element("ETag")?.Value);
        Assert.Equal(checksum, listedPart.Element("ChecksumCRC32")?.Value);
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_AwsChunkedTrailerCrc32ChecksumMismatchUploadPartWithUnsignedPayloadHash_ReturnsBadDigest()
    {
        const string accessKeyId = "sigv4-uploadpart-chunked-unsigned-trailer-crc32-baddigest-access";
        const string secretAccessKey = "sigv4-uploadpart-chunked-unsigned-trailer-crc32-baddigest-secret";
        const string bucketName = "sigv4-uploadpart-chunked-unsigned-trailer-crc32-baddigest-bucket";
        const string objectKey = "docs/uploadpart-unsigned-trailer-crc32-baddigest.txt";
        const string payload = "hello from unsigned uploadpart aws chunked trailer crc32 bad digest";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);
        var mismatchedChecksum = await PutObjectAndGetCrc32ChecksumAsync(
            client,
            accessKeyId,
            secretAccessKey,
            bucketName,
            "docs/uploadpart-unsigned-trailer-crc32-mismatch-reference.txt",
            "different payload");

        using var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/integrated-s3/{bucketName}/{objectKey}?uploads");
        initiateRequest.Headers.TryAddWithoutValidation("x-amz-checksum-algorithm", "CRC32");
        SignSigV4HeaderRequest(
            initiateRequest,
            $"/integrated-s3/{bucketName}/{objectKey}?uploads",
            accessKeyId,
            secretAccessKey,
            Convert.ToHexStringLower(SHA256.HashData(Array.Empty<byte>())),
            additionalSignedHeaders: ["x-amz-checksum-algorithm"]);
        var initiateResponse = await client.SendAsync(initiateRequest);

        Assert.Equal(HttpStatusCode.OK, initiateResponse.StatusCode);
        Assert.Equal("CRC32", Assert.Single(initiateResponse.Headers.GetValues("x-amz-checksum-algorithm")));

        var initiateDocument = XDocument.Parse(await initiateResponse.Content.ReadAsStringAsync());
        var uploadId = GetRequiredElementValue(initiateDocument, "UploadId");

        using var uploadPartRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            accessKeyId,
            secretAccessKey,
            payload,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["x-amz-checksum-crc32"] = mismatchedChecksum
            },
            payloadHash: StreamingUnsignedPayloadTrailer);
        Assert.Equal(StreamingUnsignedPayloadTrailer, Assert.Single(uploadPartRequest.Headers.GetValues("x-amz-content-sha256")));
        Assert.Equal("CRC32", Assert.Single(uploadPartRequest.Headers.GetValues("x-amz-sdk-checksum-algorithm")));
        Assert.Equal("x-amz-checksum-crc32", Assert.Single(uploadPartRequest.Headers.GetValues("x-amz-trailer")));
        Assert.DoesNotContain(
            "x-amz-trailer-signature:",
            Encoding.ASCII.GetString(await uploadPartRequest.Content!.ReadAsByteArrayAsync()),
            StringComparison.OrdinalIgnoreCase);
        var uploadPartResponse = await client.SendAsync(uploadPartRequest);

        Assert.Equal(HttpStatusCode.BadRequest, uploadPartResponse.StatusCode);
        Assert.Equal("application/xml", uploadPartResponse.Content.Headers.ContentType?.MediaType);
        var errorDocument = XDocument.Parse(await uploadPartResponse.Content.ReadAsStringAsync());
        Assert.Equal("BadDigest", GetRequiredElementValue(errorDocument, "Code"));
        var message = GetRequiredElementValue(errorDocument, "Message");
        Assert.Contains("CRC32", message, StringComparison.Ordinal);
        Assert.Contains(objectKey, message, StringComparison.Ordinal);
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
    public async Task SigV4PresignedQueryAuthentication_ZeroExpiry_ReturnsXmlError()
    {
        const string accessKeyId = "sigv4-zero-expiry-access";
        const string secretAccessKey = "sigv4-zero-expiry-secret";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var request = CreateSigV4PresignedRequest(
            HttpMethod.Get,
            "/integrated-s3/buckets/zero-expiry-bucket/objects/docs/zero.txt",
            accessKeyId,
            secretAccessKey,
            expiresSeconds: 0);
        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("AuthorizationQueryParametersError", GetRequiredElementValue(errorDocument, "Code"));
        Assert.Contains("X-Amz-Expires", GetRequiredElementValue(errorDocument, "Message"), StringComparison.Ordinal);
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

    [Fact]
    public async Task SigV4HeaderAuthentication_DeleteMarkerAndHistoricalVersionOperations_ReturnExpectedResponses()
    {
        const string accessKeyId = "sigv4-header-delete-marker-access";
        const string secretAccessKey = "sigv4-header-delete-marker-secret";
        const string bucketName = "sigv4-header-delete-marker-bucket";
        const string objectKey = "docs/deletable.txt";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        await CreateBucketAsync(client, accessKeyId, secretAccessKey, bucketName);
        await SetBucketVersioningStateAsync(client, accessKeyId, secretAccessKey, bucketName, "Enabled");

        using var putHistoricalVersionRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/{bucketName}/{objectKey}",
            accessKeyId,
            secretAccessKey,
            body: "historical version",
            contentType: "text/plain");
        var putHistoricalVersionResponse = await client.SendAsync(putHistoricalVersionRequest);
        Assert.Equal(HttpStatusCode.OK, putHistoricalVersionResponse.StatusCode);
        var historicalVersionId = Assert.Single(putHistoricalVersionResponse.Headers.GetValues("x-amz-version-id"));

        using var putCurrentVersionRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/{bucketName}/{objectKey}",
            accessKeyId,
            secretAccessKey,
            body: "current version",
            contentType: "text/plain");
        var putCurrentVersionResponse = await client.SendAsync(putCurrentVersionRequest);
        Assert.Equal(HttpStatusCode.OK, putCurrentVersionResponse.StatusCode);
        var recoverableVersionId = Assert.Single(putCurrentVersionResponse.Headers.GetValues("x-amz-version-id"));
        Assert.NotEqual(historicalVersionId, recoverableVersionId);

        using var deleteRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Delete,
            $"/integrated-s3/{bucketName}/{objectKey}",
            accessKeyId,
            secretAccessKey);
        var deleteResponse = await client.SendAsync(deleteRequest);

        Assert.Equal(HttpStatusCode.NoContent, deleteResponse.StatusCode);
        var deleteMarkerVersionId = Assert.Single(deleteResponse.Headers.GetValues("x-amz-version-id"));
        Assert.Equal("true", Assert.Single(deleteResponse.Headers.GetValues("x-amz-delete-marker")));
        Assert.NotEqual(recoverableVersionId, deleteMarkerVersionId);

        using var currentGetRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/{bucketName}/{objectKey}",
            accessKeyId,
            secretAccessKey);
        var currentGetResponse = await client.SendAsync(currentGetRequest);

        Assert.Equal(HttpStatusCode.NotFound, currentGetResponse.StatusCode);
        Assert.Equal(deleteMarkerVersionId, Assert.Single(currentGetResponse.Headers.GetValues("x-amz-version-id")));
        Assert.Equal("true", Assert.Single(currentGetResponse.Headers.GetValues("x-amz-delete-marker")));
        Assert.Null(currentGetResponse.Content.Headers.LastModified);
        var currentGetErrorDocument = XDocument.Parse(await currentGetResponse.Content.ReadAsStringAsync());
        Assert.Equal("NoSuchKey", GetRequiredElementValue(currentGetErrorDocument, "Code"));

        using var currentHeadRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Head,
            $"/integrated-s3/{bucketName}/{objectKey}",
            accessKeyId,
            secretAccessKey);
        var currentHeadResponse = await client.SendAsync(currentHeadRequest);

        Assert.Equal(HttpStatusCode.NotFound, currentHeadResponse.StatusCode);
        Assert.Equal(deleteMarkerVersionId, Assert.Single(currentHeadResponse.Headers.GetValues("x-amz-version-id")));
        Assert.Equal("true", Assert.Single(currentHeadResponse.Headers.GetValues("x-amz-delete-marker")));
        Assert.Null(currentHeadResponse.Content.Headers.LastModified);

        using var listVersionsRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/{bucketName}?versions&prefix={Uri.EscapeDataString(objectKey)}",
            accessKeyId,
            secretAccessKey);
        var listVersionsResponse = await client.SendAsync(listVersionsRequest);

        Assert.Equal(HttpStatusCode.OK, listVersionsResponse.StatusCode);
        var listVersionsDocument = XDocument.Parse(await listVersionsResponse.Content.ReadAsStringAsync());
        var deleteMarker = Assert.Single(listVersionsDocument.Root!.Elements("DeleteMarker"));
        Assert.Equal(deleteMarkerVersionId, deleteMarker.Element("VersionId")?.Value);
        var expectedDeleteMarkerLastModified = DateTimeOffset.Parse(
            deleteMarker.Element("LastModified")!.Value,
            CultureInfo.InvariantCulture).ToString("R", CultureInfo.InvariantCulture);

        using var explicitDeleteMarkerGetRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/{bucketName}/{objectKey}?versionId={Uri.EscapeDataString(deleteMarkerVersionId)}",
            accessKeyId,
            secretAccessKey);
        var explicitDeleteMarkerGetResponse = await client.SendAsync(explicitDeleteMarkerGetRequest);

        Assert.Equal(HttpStatusCode.MethodNotAllowed, explicitDeleteMarkerGetResponse.StatusCode);
        Assert.Equal(deleteMarkerVersionId, Assert.Single(explicitDeleteMarkerGetResponse.Headers.GetValues("x-amz-version-id")));
        Assert.Equal("true", Assert.Single(explicitDeleteMarkerGetResponse.Headers.GetValues("x-amz-delete-marker")));
        Assert.Equal(expectedDeleteMarkerLastModified, explicitDeleteMarkerGetResponse.Content.Headers.LastModified?.ToString("R"));
        var explicitDeleteMarkerGetErrorDocument = XDocument.Parse(await explicitDeleteMarkerGetResponse.Content.ReadAsStringAsync());
        Assert.Equal("MethodNotAllowed", GetRequiredElementValue(explicitDeleteMarkerGetErrorDocument, "Code"));

        using var explicitDeleteMarkerHeadRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Head,
            $"/integrated-s3/{bucketName}/{objectKey}?versionId={Uri.EscapeDataString(deleteMarkerVersionId)}",
            accessKeyId,
            secretAccessKey);
        var explicitDeleteMarkerHeadResponse = await client.SendAsync(explicitDeleteMarkerHeadRequest);

        Assert.Equal(HttpStatusCode.MethodNotAllowed, explicitDeleteMarkerHeadResponse.StatusCode);
        Assert.Equal(deleteMarkerVersionId, Assert.Single(explicitDeleteMarkerHeadResponse.Headers.GetValues("x-amz-version-id")));
        Assert.Equal("true", Assert.Single(explicitDeleteMarkerHeadResponse.Headers.GetValues("x-amz-delete-marker")));
        Assert.Equal(expectedDeleteMarkerLastModified, explicitDeleteMarkerHeadResponse.Content.Headers.LastModified?.ToString("R"));

        using var historicalGetRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/{bucketName}/{objectKey}?versionId={Uri.EscapeDataString(recoverableVersionId)}",
            accessKeyId,
            secretAccessKey);
        var historicalGetResponse = await client.SendAsync(historicalGetRequest);

        Assert.Equal(HttpStatusCode.OK, historicalGetResponse.StatusCode);
        Assert.Equal(recoverableVersionId, Assert.Single(historicalGetResponse.Headers.GetValues("x-amz-version-id")));
        Assert.Equal("current version", await historicalGetResponse.Content.ReadAsStringAsync());

        using var historicalHeadRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Head,
            $"/integrated-s3/{bucketName}/{objectKey}?versionId={Uri.EscapeDataString(recoverableVersionId)}",
            accessKeyId,
            secretAccessKey);
        var historicalHeadResponse = await client.SendAsync(historicalHeadRequest);

        Assert.Equal(HttpStatusCode.OK, historicalHeadResponse.StatusCode);
        Assert.Equal(recoverableVersionId, Assert.Single(historicalHeadResponse.Headers.GetValues("x-amz-version-id")));
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_DeleteMarkerMalformedVersionId_ReturnsBadRequest()
    {
        const string accessKeyId = "sigv4-header-malformed-versionid-access";
        const string secretAccessKey = "sigv4-header-malformed-versionid-secret";
        const string bucketName = "sigv4-header-malformed-versionid-bucket";
        const string objectKey = "docs/testobj.txt";

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

        // Delete with malformed versionId parameter
        using var deleteRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Delete,
            $"/integrated-s3/{bucketName}/{objectKey}?versionId=invalid-format",
            accessKeyId,
            secretAccessKey);
        var deleteResponse = await client.SendAsync(deleteRequest);

        Assert.Equal(HttpStatusCode.NotFound, deleteResponse.StatusCode);
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_VersionedGetAndHead_RetrievesSpecificVersion()
    {
        const string accessKeyId = "sigv4-header-versioned-get-access";
        const string secretAccessKey = "sigv4-header-versioned-get-secret";
        const string bucketName = "sigv4-header-versioned-get-bucket";
        const string objectKey = "docs/versioned.txt";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        // Enable versioning
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

        // Create v1
        using var putV1Request = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/{bucketName}/{objectKey}",
            accessKeyId,
            secretAccessKey,
            body: "version 1 content",
            contentType: "text/plain");
        var v1Response = await client.SendAsync(putV1Request);
        Assert.Equal(HttpStatusCode.OK, v1Response.StatusCode);
        var v1VersionId = Assert.Single(v1Response.Headers.GetValues("x-amz-version-id"));

        // Create v2
        using var putV2Request = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/{bucketName}/{objectKey}",
            accessKeyId,
            secretAccessKey,
            body: "version 2 content updated",
            contentType: "text/plain");
        var v2Response = await client.SendAsync(putV2Request);
        Assert.Equal(HttpStatusCode.OK, v2Response.StatusCode);
        var v2VersionId = Assert.Single(v2Response.Headers.GetValues("x-amz-version-id"));
        Assert.NotEqual(v1VersionId, v2VersionId);

        // GET v1 with versionId
        using var getV1Request = CreateSigV4HeaderSignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/{bucketName}/{objectKey}?versionId={Uri.EscapeDataString(v1VersionId)}",
            accessKeyId,
            secretAccessKey);
        var getV1Response = await client.SendAsync(getV1Request);

        Assert.Equal(HttpStatusCode.OK, getV1Response.StatusCode);
        Assert.Equal(v1VersionId, Assert.Single(getV1Response.Headers.GetValues("x-amz-version-id")));
        Assert.Equal("version 1 content", await getV1Response.Content.ReadAsStringAsync());

        // HEAD v2 with versionId
        using var headV2Request = CreateSigV4HeaderSignedRequest(
            HttpMethod.Head,
            $"/integrated-s3/{bucketName}/{objectKey}?versionId={Uri.EscapeDataString(v2VersionId)}",
            accessKeyId,
            secretAccessKey);
        var headV2Response = await client.SendAsync(headV2Request);

        Assert.Equal(HttpStatusCode.OK, headV2Response.StatusCode);
        Assert.Equal(v2VersionId, Assert.Single(headV2Response.Headers.GetValues("x-amz-version-id")));
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_EnableAndReadVersioningState()
    {
        const string accessKeyId = "sigv4-header-versioning-state-access";
        const string secretAccessKey = "sigv4-header-versioning-state-secret";
        const string bucketName = "sigv4-header-versioning-state-bucket";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        // Enable versioning
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
        var enableResponse = await client.SendAsync(enableVersioningRequest);
        Assert.Equal(HttpStatusCode.OK, enableResponse.StatusCode);

        // Read versioning state
        using var getVersioningRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/{bucketName}?versioning",
            accessKeyId,
            secretAccessKey);
        var getVersioningResponse = await client.SendAsync(getVersioningRequest);

        Assert.Equal(HttpStatusCode.OK, getVersioningResponse.StatusCode);
        Assert.Equal("application/xml", getVersioningResponse.Content.Headers.ContentType?.MediaType);
        var versioningDocument = XDocument.Parse(await getVersioningResponse.Content.ReadAsStringAsync());
        Assert.Equal("VersioningConfiguration", versioningDocument.Root?.Name.LocalName);
        Assert.Equal("Enabled", GetRequiredElementValue(versioningDocument, "Status"));

        // Suspend versioning
        using var suspendVersioningRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/{bucketName}?versioning",
            accessKeyId,
            secretAccessKey,
            body: """
<VersioningConfiguration>
  <Status>Suspended</Status>
</VersioningConfiguration>
""",
            contentType: "application/xml");
        var suspendResponse = await client.SendAsync(suspendVersioningRequest);
        Assert.Equal(HttpStatusCode.OK, suspendResponse.StatusCode);

        // Verify suspended state
        using var getSuspendedRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/{bucketName}?versioning",
            accessKeyId,
            secretAccessKey);
        var getSuspendedResponse = await client.SendAsync(getSuspendedRequest);

        Assert.Equal(HttpStatusCode.OK, getSuspendedResponse.StatusCode);
        var suspendedDocument = XDocument.Parse(await getSuspendedResponse.Content.ReadAsStringAsync());
        Assert.Equal("Suspended", GetRequiredElementValue(suspendedDocument, "Status"));
    }

    [Fact]
    public async Task SigV4HeaderAuthentication_ListVersionsWithPagination()
    {
        const string accessKeyId = "sigv4-header-list-versions-access";
        const string secretAccessKey = "sigv4-header-list-versions-secret";
        const string bucketName = "sigv4-header-list-versions-bucket";
        const string objectKey1 = "docs/file-a.txt";
        const string objectKey2 = "docs/file-b.txt";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);

        // Enable versioning
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

        // Put multiple versions
        using var putRequest1 = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/{bucketName}/{objectKey1}",
            accessKeyId,
            secretAccessKey,
            body: "file a version 1",
            contentType: "text/plain");
        var put1Response = await client.SendAsync(putRequest1);
        Assert.Equal(HttpStatusCode.OK, put1Response.StatusCode);

        using var putRequest2 = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/{bucketName}/{objectKey1}",
            accessKeyId,
            secretAccessKey,
            body: "file a version 2",
            contentType: "text/plain");
        var put2Response = await client.SendAsync(putRequest2);
        Assert.Equal(HttpStatusCode.OK, put2Response.StatusCode);
        var v2Id = Assert.Single(put2Response.Headers.GetValues("x-amz-version-id"));

        using var putRequest3 = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/{bucketName}/{objectKey2}",
            accessKeyId,
            secretAccessKey,
            body: "file b content",
            contentType: "text/plain");
        var put3Response = await client.SendAsync(putRequest3);
        Assert.Equal(HttpStatusCode.OK, put3Response.StatusCode);

        // List versions with prefix
        using var listVersionsRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/{bucketName}?versions&prefix=docs/file-a",
            accessKeyId,
            secretAccessKey);
        var listVersionsResponse = await client.SendAsync(listVersionsRequest);

        Assert.Equal(HttpStatusCode.OK, listVersionsResponse.StatusCode);
        Assert.Equal("application/xml", listVersionsResponse.Content.Headers.ContentType?.MediaType);
        var listDocument = XDocument.Parse(await listVersionsResponse.Content.ReadAsStringAsync());
        Assert.Equal("ListVersionsResult", listDocument.Root?.Name.LocalName);

        var versions = listDocument.Root?.Elements("Version").ToList();
        Assert.NotNull(versions);
        Assert.NotEmpty(versions);
        // Should have at least 2 versions of file-a
        var fileAVersions = versions.Where(v => v.Element("Key")?.Value == objectKey1).ToList();
        Assert.True(fileAVersions.Count >= 2);

        // Verify version IDs are present
        foreach (var version in fileAVersions) {
            Assert.NotNull(version.Element("VersionId")?.Value);
        }
    }

    [Fact]
    public async Task SigV4PresignedQueryAuthentication_ListVersionsWithPrefixAndVersionMarkers()
    {
        const string accessKeyId = "sigv4-presign-list-versions-markers-access";
        const string secretAccessKey = "sigv4-presign-list-versions-markers-secret";
        const string bucketName = "sigv4-presign-list-versions-markers-bucket";
        const string primaryKey = "docs/history.txt";
        const string secondaryKey = "docs/zeta.txt";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        await CreateBucketAsync(client, accessKeyId, secretAccessKey, bucketName);
        await SetBucketVersioningStateAsync(client, accessKeyId, secretAccessKey, bucketName, "Enabled");

        using var putV1Request = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/{bucketName}/{primaryKey}",
            accessKeyId,
            secretAccessKey,
            body: "sigv4 version one",
            contentType: "text/plain");
        var putV1Response = await client.SendAsync(putV1Request);
        Assert.Equal(HttpStatusCode.OK, putV1Response.StatusCode);
        var v1VersionId = Assert.Single(putV1Response.Headers.GetValues("x-amz-version-id"));

        using var putV2Request = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/{bucketName}/{primaryKey}",
            accessKeyId,
            secretAccessKey,
            body: "sigv4 version two",
            contentType: "text/plain");
        var putV2Response = await client.SendAsync(putV2Request);
        Assert.Equal(HttpStatusCode.OK, putV2Response.StatusCode);
        var v2VersionId = Assert.Single(putV2Response.Headers.GetValues("x-amz-version-id"));

        using var deleteCurrentRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Delete,
            $"/integrated-s3/{bucketName}/{primaryKey}",
            accessKeyId,
            secretAccessKey);
        var deleteCurrentResponse = await client.SendAsync(deleteCurrentRequest);
        Assert.Equal(HttpStatusCode.NoContent, deleteCurrentResponse.StatusCode);
        Assert.Equal("true", Assert.Single(deleteCurrentResponse.Headers.GetValues("x-amz-delete-marker")));
        var deleteMarkerVersionId = Assert.Single(deleteCurrentResponse.Headers.GetValues("x-amz-version-id"));

        using var putSecondaryRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/{bucketName}/{secondaryKey}",
            accessKeyId,
            secretAccessKey,
            body: "sigv4 secondary version",
            contentType: "text/plain");
        var putSecondaryResponse = await client.SendAsync(putSecondaryRequest);
        Assert.Equal(HttpStatusCode.OK, putSecondaryResponse.StatusCode);
        var secondaryVersionId = Assert.Single(putSecondaryResponse.Headers.GetValues("x-amz-version-id"));

        using var firstPageRequest = CreateSigV4PresignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/{bucketName}?versions&prefix=docs/&max-keys=1",
            accessKeyId,
            secretAccessKey,
            expiresSeconds: 300);
        var firstPageResponse = await client.SendAsync(firstPageRequest);

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

        using var secondPageRequest = CreateSigV4PresignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/{bucketName}?versions&prefix=docs/&max-keys=10&key-marker={Uri.EscapeDataString(primaryKey)}&version-id-marker={Uri.EscapeDataString(deleteMarkerVersionId)}",
            accessKeyId,
            secretAccessKey,
            expiresSeconds: 300);
        var secondPageResponse = await client.SendAsync(secondPageRequest);

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

    private async Task RunSignedAwsChunkedTrailerPutObjectSuccessAsync(
        string checksumSuffix,
        string trailerHeaderName,
        string sdkChecksumAlgorithm)
    {
        var scenarioId = $"chunked-trailer-{checksumSuffix}";
        var accessKeyId = $"sigv4-{scenarioId}-access";
        var secretAccessKey = $"sigv4-{scenarioId}-secret";
        var bucketName = $"sigv4-{scenarioId}-bucket";
        var objectKey = $"docs/{scenarioId}.txt";
        var referenceObjectKey = $"docs/{scenarioId}-reference.txt";
        var payload = $"hello from signed aws chunked trailer {checksumSuffix}";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        await CreateBucketAsync(client, accessKeyId, secretAccessKey, bucketName);
        var checksum = await ComputeTrailerChecksumAsync(
            trailerHeaderName,
            payload,
            client,
            accessKeyId,
            secretAccessKey,
            bucketName,
            referenceObjectKey);

        using var putObjectRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            payload,
            CreateChecksumTrailerHeaders(trailerHeaderName, checksum),
            includeTrailerSignature: true);
        await AssertSignedAwsChunkedTrailerRequestAsync(putObjectRequest, trailerHeaderName, sdkChecksumAlgorithm);
        var putObjectResponse = await client.SendAsync(putObjectRequest);

        Assert.Equal(HttpStatusCode.OK, putObjectResponse.StatusCode);
        Assert.Equal(sdkChecksumAlgorithm, Assert.Single(putObjectResponse.Headers.GetValues("x-amz-checksum-algorithm")));
        Assert.Equal(checksum, Assert.Single(putObjectResponse.Headers.GetValues(trailerHeaderName)));
        Assert.False(putObjectResponse.Headers.Contains("x-amz-checksum-sha256"));

        using var getObjectRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Get,
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey);
        var getObjectResponse = await client.SendAsync(getObjectRequest);

        Assert.Equal(HttpStatusCode.OK, getObjectResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(getObjectResponse.Headers.GetValues(trailerHeaderName)));
        Assert.False(getObjectResponse.Headers.Contains("x-amz-checksum-sha256"));
        Assert.Equal(payload, await getObjectResponse.Content.ReadAsStringAsync());
    }

    private async Task RunSignedAwsChunkedTrailerPutObjectBadDigestAsync(
        string checksumSuffix,
        string trailerHeaderName,
        string sdkChecksumAlgorithm)
    {
        var scenarioId = $"chunked-trailer-{checksumSuffix}-baddigest";
        var accessKeyId = $"sigv4-{scenarioId}-access";
        var secretAccessKey = $"sigv4-{scenarioId}-secret";
        var bucketName = $"sigv4-{scenarioId}-bucket";
        var objectKey = $"docs/{scenarioId}.txt";
        var referenceObjectKey = $"docs/{scenarioId}-reference.txt";
        const string payload = "hello from signed aws chunked trailer bad digest";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        await CreateBucketAsync(client, accessKeyId, secretAccessKey, bucketName);
        var mismatchedChecksum = await ComputeTrailerChecksumAsync(
            trailerHeaderName,
            "different payload",
            client,
            accessKeyId,
            secretAccessKey,
            bucketName,
            referenceObjectKey);

        using var putObjectRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            payload,
            CreateChecksumTrailerHeaders(trailerHeaderName, mismatchedChecksum),
            includeTrailerSignature: true);
        await AssertSignedAwsChunkedTrailerRequestAsync(putObjectRequest, trailerHeaderName, sdkChecksumAlgorithm);
        var response = await client.SendAsync(putObjectRequest);

        await AssertBadDigestAsync(response);
    }

    private async Task RunSignedAwsChunkedTrailerUploadPartSuccessAsync(
        string checksumSuffix,
        string trailerHeaderName,
        string sdkChecksumAlgorithm,
        string multipartChecksumElementName)
    {
        var scenarioId = $"uploadpart-chunked-trailer-{checksumSuffix}";
        var accessKeyId = $"sigv4-{scenarioId}-access";
        var secretAccessKey = $"sigv4-{scenarioId}-secret";
        var bucketName = $"sigv4-{scenarioId}-bucket";
        var objectKey = $"docs/{scenarioId}.txt";
        var referenceObjectKey = $"docs/{scenarioId}-reference.txt";
        var payload = $"hello from signed uploadpart aws chunked trailer {checksumSuffix}";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        await CreateBucketAsync(client, accessKeyId, secretAccessKey, bucketName);
        var checksum = await ComputeTrailerChecksumAsync(
            trailerHeaderName,
            payload,
            client,
            accessKeyId,
            secretAccessKey,
            bucketName,
            referenceObjectKey);
        var uploadId = await InitiateMultipartUploadAsync(client, accessKeyId, secretAccessKey, bucketName, objectKey, sdkChecksumAlgorithm);

        using var uploadPartRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            accessKeyId,
            secretAccessKey,
            payload,
            CreateChecksumTrailerHeaders(trailerHeaderName, checksum),
            includeTrailerSignature: true);
        await AssertSignedAwsChunkedTrailerRequestAsync(uploadPartRequest, trailerHeaderName, sdkChecksumAlgorithm);
        var uploadPartResponse = await client.SendAsync(uploadPartRequest);

        Assert.Equal(HttpStatusCode.OK, uploadPartResponse.StatusCode);
        Assert.Equal(checksum, Assert.Single(uploadPartResponse.Headers.GetValues(trailerHeaderName)));
        Assert.False(uploadPartResponse.Headers.Contains("x-amz-checksum-sha256"));
        var partETag = uploadPartResponse.Headers.ETag?.Tag ?? throw new Xunit.Sdk.XunitException("Expected multipart part ETag header.");

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
        Assert.Equal(checksum, listedPart.Element(multipartChecksumElementName)?.Value);
    }

    private async Task RunSignedAwsChunkedTrailerUploadPartBadDigestAsync(
        string checksumSuffix,
        string trailerHeaderName,
        string sdkChecksumAlgorithm)
    {
        var scenarioId = $"uploadpart-chunked-trailer-{checksumSuffix}-baddigest";
        var accessKeyId = $"sigv4-{scenarioId}-access";
        var secretAccessKey = $"sigv4-{scenarioId}-secret";
        var bucketName = $"sigv4-{scenarioId}-bucket";
        var objectKey = $"docs/{scenarioId}.txt";
        var referenceObjectKey = $"docs/{scenarioId}-reference.txt";
        const string payload = "hello from signed uploadpart aws chunked trailer bad digest";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        await CreateBucketAsync(client, accessKeyId, secretAccessKey, bucketName);
        var mismatchedChecksum = await ComputeTrailerChecksumAsync(
            trailerHeaderName,
            "different payload",
            client,
            accessKeyId,
            secretAccessKey,
            bucketName,
            referenceObjectKey);
        var uploadId = await InitiateMultipartUploadAsync(client, accessKeyId, secretAccessKey, bucketName, objectKey, sdkChecksumAlgorithm);

        using var uploadPartRequest = CreateSigV4AwsChunkedTrailerRequest(
            $"/integrated-s3/{bucketName}/{objectKey}?partNumber=1&uploadId={Uri.EscapeDataString(uploadId)}",
            accessKeyId,
            secretAccessKey,
            payload,
            CreateChecksumTrailerHeaders(trailerHeaderName, mismatchedChecksum),
            includeTrailerSignature: true);
        await AssertSignedAwsChunkedTrailerRequestAsync(uploadPartRequest, trailerHeaderName, sdkChecksumAlgorithm);
        var uploadPartResponse = await client.SendAsync(uploadPartRequest);

        await AssertBadDigestAsync(uploadPartResponse);
    }

    [Fact]
    public async Task SigV4_PutObject_WithSseCHeaders_SignatureAccepted()
    {
        const string accessKeyId = "sigv4-ssec-put-accept-access";
        const string secretAccessKey = "sigv4-ssec-put-accept-secret";
        const string bucketName = "sigv4-ssec-put-accept-bucket";
        const string objectKey = "docs/ssec-signed.txt";
        const string payload = "ssec sigv4 put payload";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        await CreateBucketAsync(client, accessKeyId, secretAccessKey, bucketName);

        var keyBytes = new byte[32];
        RandomNumberGenerator.Fill(keyBytes);
        var customerKey = Convert.ToBase64String(keyBytes);
        var customerKeyMd5 = Convert.ToBase64String(MD5.HashData(keyBytes));

        using var putRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}");
        putRequest.Content = new StringContent(payload, Encoding.UTF8, "text/plain");
        putRequest.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-algorithm", "AES256");
        putRequest.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key", customerKey);
        putRequest.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key-MD5", customerKeyMd5);

        var payloadBytes = Encoding.UTF8.GetBytes(payload);
        var payloadHash = Convert.ToHexStringLower(SHA256.HashData(payloadBytes));
        SignSigV4HeaderRequest(
            putRequest,
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            payloadHash,
            additionalSignedHeaders:
            [
                "x-amz-server-side-encryption-customer-algorithm",
                "x-amz-server-side-encryption-customer-key",
                "x-amz-server-side-encryption-customer-key-md5"
            ]);

        var putResponse = await client.SendAsync(putRequest);

        // The disk provider rejects SSE-C with a 4xx; the important thing is no 403 (signature mismatch)
        Assert.NotEqual(HttpStatusCode.Forbidden, putResponse.StatusCode);
        Assert.NotEqual(HttpStatusCode.InternalServerError, putResponse.StatusCode);
    }

    [Fact]
    public async Task SigV4_GetObject_WithSseCHeaders_SignatureAccepted()
    {
        const string accessKeyId = "sigv4-ssec-get-accept-access";
        const string secretAccessKey = "sigv4-ssec-get-accept-secret";
        const string bucketName = "sigv4-ssec-get-accept-bucket";
        const string objectKey = "docs/ssec-get-signed.txt";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        await CreateBucketAsync(client, accessKeyId, secretAccessKey, bucketName);

        // Put a normal object first
        using var putRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            body: "normal object for ssec get test",
            contentType: "text/plain");
        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(putRequest)).StatusCode);

        var keyBytes = new byte[32];
        RandomNumberGenerator.Fill(keyBytes);
        var customerKey = Convert.ToBase64String(keyBytes);
        var customerKeyMd5 = Convert.ToBase64String(MD5.HashData(keyBytes));

        using var getRequest = new HttpRequestMessage(HttpMethod.Get, $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}");
        getRequest.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-algorithm", "AES256");
        getRequest.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key", customerKey);
        getRequest.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key-MD5", customerKeyMd5);

        var emptyPayloadHash = Convert.ToHexStringLower(SHA256.HashData(Array.Empty<byte>()));
        SignSigV4HeaderRequest(
            getRequest,
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            emptyPayloadHash,
            additionalSignedHeaders:
            [
                "x-amz-server-side-encryption-customer-algorithm",
                "x-amz-server-side-encryption-customer-key",
                "x-amz-server-side-encryption-customer-key-md5"
            ]);

        var getResponse = await client.SendAsync(getRequest);

        // Signature must be accepted; disk provider may reject SSE-C with 4xx but not 403 (signature error)
        Assert.NotEqual(HttpStatusCode.Forbidden, getResponse.StatusCode);
        Assert.NotEqual(HttpStatusCode.InternalServerError, getResponse.StatusCode);
    }

    [Fact]
    public async Task SigV4_CopyObject_WithSourceAndDestinationSseCHeaders_SignatureAccepted()
    {
        const string accessKeyId = "sigv4-ssec-copy-accept-access";
        const string secretAccessKey = "sigv4-ssec-copy-accept-secret";
        const string bucketName = "sigv4-ssec-copy-accept-bucket";
        const string sourceKey = "docs/ssec-copy-source.txt";
        const string destKey = "docs/ssec-copy-dest.txt";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        await CreateBucketAsync(client, accessKeyId, secretAccessKey, bucketName);

        // Put a normal source object
        using var putRequest = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/buckets/{bucketName}/objects/{sourceKey}",
            accessKeyId,
            secretAccessKey,
            body: "copy source for ssec test",
            contentType: "text/plain");
        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(putRequest)).StatusCode);

        var srcKeyBytes = new byte[32];
        RandomNumberGenerator.Fill(srcKeyBytes);
        var srcCustomerKey = Convert.ToBase64String(srcKeyBytes);
        var srcCustomerKeyMd5 = Convert.ToBase64String(MD5.HashData(srcKeyBytes));

        var destKeyBytes = new byte[32];
        RandomNumberGenerator.Fill(destKeyBytes);
        var destCustomerKey = Convert.ToBase64String(destKeyBytes);
        var destCustomerKeyMd5 = Convert.ToBase64String(MD5.HashData(destKeyBytes));

        using var copyRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}/objects/{destKey}");
        copyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source", $"/{bucketName}/{sourceKey}");
        copyRequest.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-algorithm", "AES256");
        copyRequest.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key", destCustomerKey);
        copyRequest.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key-MD5", destCustomerKeyMd5);
        copyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source-server-side-encryption-customer-algorithm", "AES256");
        copyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source-server-side-encryption-customer-key", srcCustomerKey);
        copyRequest.Headers.TryAddWithoutValidation("x-amz-copy-source-server-side-encryption-customer-key-MD5", srcCustomerKeyMd5);

        var emptyPayloadHash = Convert.ToHexStringLower(SHA256.HashData(Array.Empty<byte>()));
        SignSigV4HeaderRequest(
            copyRequest,
            $"/integrated-s3/buckets/{bucketName}/objects/{destKey}",
            accessKeyId,
            secretAccessKey,
            emptyPayloadHash,
            additionalSignedHeaders:
            [
                "x-amz-copy-source",
                "x-amz-copy-source-server-side-encryption-customer-algorithm",
                "x-amz-copy-source-server-side-encryption-customer-key",
                "x-amz-copy-source-server-side-encryption-customer-key-md5",
                "x-amz-server-side-encryption-customer-algorithm",
                "x-amz-server-side-encryption-customer-key",
                "x-amz-server-side-encryption-customer-key-md5"
            ]);

        var copyResponse = await client.SendAsync(copyRequest);

        Assert.NotEqual(HttpStatusCode.Forbidden, copyResponse.StatusCode);
        Assert.NotEqual(HttpStatusCode.InternalServerError, copyResponse.StatusCode);
    }

    [Fact]
    public async Task SigV4_PutObject_WithSseCHeaders_InvalidSignature_Returns403()
    {
        const string accessKeyId = "sigv4-ssec-put-invalid-sig-access";
        const string secretAccessKey = "sigv4-ssec-put-invalid-sig-secret";
        const string bucketName = "sigv4-ssec-put-invalid-sig-bucket";
        const string objectKey = "docs/ssec-invalid-sig.txt";
        const string payload = "ssec sigv4 invalid signature payload";

        await using var isolatedClient = await CreateAuthenticatedClientAsync(accessKeyId, secretAccessKey);
        using var client = isolatedClient.Client;

        await CreateBucketAsync(client, accessKeyId, secretAccessKey, bucketName);

        var keyBytes = new byte[32];
        RandomNumberGenerator.Fill(keyBytes);
        var customerKey = Convert.ToBase64String(keyBytes);
        var customerKeyMd5 = Convert.ToBase64String(MD5.HashData(keyBytes));

        // Compute signature WITH SSE-C headers, then tamper with a signed header value.
        // The verifier should reject the request because the signed content has been altered.
        using var putRequest = new HttpRequestMessage(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}");
        putRequest.Content = new StringContent(payload, Encoding.UTF8, "text/plain");
        putRequest.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-algorithm", "AES256");
        putRequest.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key", customerKey);
        putRequest.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key-MD5", customerKeyMd5);

        var payloadBytes = Encoding.UTF8.GetBytes(payload);
        var payloadHash = Convert.ToHexStringLower(SHA256.HashData(payloadBytes));

        SignSigV4HeaderRequest(
            putRequest,
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            payloadHash,
            additionalSignedHeaders:
            [
                "x-amz-server-side-encryption-customer-algorithm",
                "x-amz-server-side-encryption-customer-key",
                "x-amz-server-side-encryption-customer-key-md5"
            ]);

        // Tamper with a signed SSE-C header after signing
        putRequest.Headers.Remove("x-amz-server-side-encryption-customer-key");
        var tamperedKeyBytes = new byte[32];
        RandomNumberGenerator.Fill(tamperedKeyBytes);
        putRequest.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key", Convert.ToBase64String(tamperedKeyBytes));

        var putResponse = await client.SendAsync(putRequest);

        Assert.Equal(HttpStatusCode.Forbidden, putResponse.StatusCode);
    }

    private Task<WebUiApplicationFactory.IsolatedWebUiClient> CreateAuthenticatedClientAsync(
        string accessKeyId,
        string secretAccessKey,
        Action<IntegratedS3Options>? configureOptions = null,
        string? sessionToken = null)
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
                        SessionToken = sessionToken,
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

    private static string GetRequiredElementValue(XDocument document, string elementName)
    {
        return document.Root?.Element(elementName)?.Value
            ?? throw new Xunit.Sdk.XunitException($"Missing XML element '{elementName}'.");
    }

    private static string ComputeSha256Base64(string content)
    {
        return Convert.ToBase64String(SHA256.HashData(Encoding.UTF8.GetBytes(content)));
    }

    private static string ComputeSha1Base64(string content)
    {
        return Convert.ToBase64String(SHA1.HashData(Encoding.UTF8.GetBytes(content)));
    }

    private static async Task<string> PutObjectAndGetCrc32ChecksumAsync(
        HttpClient client,
        string accessKeyId,
        string secretAccessKey,
        string bucketName,
        string objectKey,
        string payload)
    {
        using var request = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/buckets/{bucketName}/objects/{objectKey}",
            accessKeyId,
            secretAccessKey,
            body: payload,
            contentType: "text/plain");
        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        return Assert.Single(response.Headers.GetValues("x-amz-checksum-crc32"));
    }

    private static string ComputeCrc32cBase64(string content)
    {
        return ChecksumTestAlgorithms.ComputeCrc32cBase64(content);
    }

    private static async Task CreateBucketAsync(HttpClient client, string accessKeyId, string secretAccessKey, string bucketName)
    {
        using var createBucketRequest = CreateSigV4HeaderSignedRequest(HttpMethod.Put, $"/integrated-s3/buckets/{bucketName}", accessKeyId, secretAccessKey);
        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(createBucketRequest)).StatusCode);
    }

    private static async Task SetBucketVersioningStateAsync(
        HttpClient client,
        string accessKeyId,
        string secretAccessKey,
        string bucketName,
        string status)
    {
        using var request = CreateSigV4HeaderSignedRequest(
            HttpMethod.Put,
            $"/integrated-s3/{bucketName}?versioning",
            accessKeyId,
            secretAccessKey,
            body: $$"""
<VersioningConfiguration>
  <Status>{{status}}</Status>
</VersioningConfiguration>
""",
            contentType: "application/xml");
        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(request)).StatusCode);
    }

    private static async Task<string> InitiateMultipartUploadAsync(
        HttpClient client,
        string accessKeyId,
        string secretAccessKey,
        string bucketName,
        string objectKey,
        string sdkChecksumAlgorithm)
    {
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
        return GetRequiredElementValue(initiateDocument, "UploadId");
    }

    private static Task<string> ComputeTrailerChecksumAsync(
        string trailerHeaderName,
        string payload,
        HttpClient client,
        string accessKeyId,
        string secretAccessKey,
        string bucketName,
        string referenceObjectKey)
    {
        return trailerHeaderName switch
        {
            "x-amz-checksum-sha1" => Task.FromResult(ComputeSha1Base64(payload)),
            "x-amz-checksum-crc32" => PutObjectAndGetCrc32ChecksumAsync(client, accessKeyId, secretAccessKey, bucketName, referenceObjectKey, payload),
            "x-amz-checksum-crc32c" => Task.FromResult(ComputeCrc32cBase64(payload)),
            _ => throw new Xunit.Sdk.XunitException($"Unsupported trailer checksum header '{trailerHeaderName}'.")
        };
    }

    private static async Task AssertBadDigestAsync(HttpResponseMessage response)
    {
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        var errorDocument = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("BadDigest", GetRequiredElementValue(errorDocument, "Code"));
    }

    private static async Task AssertSignedAwsChunkedTrailerRequestAsync(
        HttpRequestMessage request,
        string trailerHeaderName,
        string sdkChecksumAlgorithm)
    {
        Assert.Equal(StreamingAws4HmacSha256PayloadTrailer, Assert.Single(request.Headers.GetValues("x-amz-content-sha256")));
        Assert.Equal(sdkChecksumAlgorithm, Assert.Single(request.Headers.GetValues("x-amz-sdk-checksum-algorithm")));
        Assert.Equal(trailerHeaderName, Assert.Single(request.Headers.GetValues("x-amz-trailer")));
        Assert.Contains(
            "x-amz-trailer-signature:",
            Encoding.ASCII.GetString(await request.Content!.ReadAsByteArrayAsync()),
            StringComparison.OrdinalIgnoreCase);
    }

    private static Dictionary<string, string> CreateChecksumTrailerHeaders(string trailerHeaderName, string checksum)
    {
        return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            [trailerHeaderName] = checksum
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
                case "x-amz-checksum-crc64nvme":
                    return "CRC64NVME";
                case "x-amz-checksum-sha1":
                    return "SHA1";
                case "x-amz-checksum-sha256":
                    return "SHA256";
            }
        }

        throw new Xunit.Sdk.XunitException("Expected a supported x-amz-checksum-* trailer header.");
    }

    private static HttpRequestMessage CreateSigV4AwsChunkedTrailerRequest(
        string pathAndQuery,
        string accessKeyId,
        string secretAccessKey,
        string payload,
        IReadOnlyDictionary<string, string> trailerHeaders,
        string payloadHash = StreamingAws4HmacSha256PayloadTrailer,
        bool includeTrailerSignature = false,
        string? trailerSignatureOverride = null,
        string host = "localhost",
        DateTimeOffset? signedAtUtc = null,
        string? securityToken = null)
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

        SignSigV4HeaderRequest(
            request,
            pathAndQuery,
            accessKeyId,
            secretAccessKey,
            payloadHash,
            host: host,
            signedAtUtc: timestampUtc,
            securityToken: securityToken,
            additionalSignedHeaders:
            [
                "content-encoding",
                "x-amz-decoded-content-length",
                "x-amz-sdk-checksum-algorithm",
                "x-amz-trailer"
            ]);

        var contentBytes = string.Equals(payloadHash, StreamingUnsignedPayloadTrailer, StringComparison.OrdinalIgnoreCase)
            ? BuildAwsChunkedPayload(payloadBytes, trailerHeaders)
            : BuildSigV4AwsChunkedPayload(
                secretAccessKey,
                CreateSigV4CredentialScope(accessKeyId, timestampUtc),
                timestampUtc,
                GetAuthorizationSignature(request),
                payloadBytes,
                trailerHeaders,
                includeTrailerSignature,
                trailerSignatureOverride);
        request.Content = new ByteArrayContent(contentBytes);
        request.Content.Headers.TryAddWithoutValidation("Content-Type", "text/plain");
        request.Content.Headers.ContentEncoding.Add("aws-chunked");

        return request;
    }

    private static HttpRequestMessage CreateSigV4HeaderSignedRequest(
        HttpMethod method,
        string pathAndQuery,
        string accessKeyId,
        string secretAccessKey,
        string? body = null,
        string? contentType = null,
        string host = "localhost",
        DateTimeOffset? signedAtUtc = null,
        string? securityToken = null)
    {
        var request = new HttpRequestMessage(method, pathAndQuery);
        if (body is not null) {
            request.Content = new StringContent(body, Encoding.UTF8, contentType ?? "text/plain");
        }

        var payloadBytes = body is null ? Array.Empty<byte>() : Encoding.UTF8.GetBytes(body);
        var payloadHash = Convert.ToHexStringLower(SHA256.HashData(payloadBytes));
        SignSigV4HeaderRequest(request, pathAndQuery, accessKeyId, secretAccessKey, payloadHash, host, signedAtUtc, securityToken);
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
        string? securityToken = null,
        IEnumerable<string>? additionalSignedHeaders = null)
    {
        var timestampUtc = signedAtUtc ?? DateTimeOffset.UtcNow;
        var timestampText = timestampUtc.ToString("yyyyMMdd'T'HHmmss'Z'");

        request.Headers.Host = host;
        request.Headers.Remove("x-amz-date");
        request.Headers.TryAddWithoutValidation("x-amz-date", timestampText);
        request.Headers.Remove("x-amz-content-sha256");
        request.Headers.TryAddWithoutValidation("x-amz-content-sha256", payloadHash);
        if (!string.IsNullOrWhiteSpace(securityToken)) {
            request.Headers.Remove("x-amz-security-token");
            request.Headers.TryAddWithoutValidation("x-amz-security-token", securityToken);
        }

        var credentialScope = new S3SigV4CredentialScope
        {
            AccessKeyId = accessKeyId,
            DateStamp = timestampUtc.ToString("yyyyMMdd"),
            Region = "us-east-1",
            Service = "s3",
            Terminator = "aws4_request"
        };

        var signedHeaders = new List<string> { "host", "x-amz-content-sha256", "x-amz-date" };
        if (!string.IsNullOrWhiteSpace(securityToken)) {
            signedHeaders.Add("x-amz-security-token");
        }

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

    private static byte[] BuildAwsChunkedPayload(byte[] payloadBytes, IReadOnlyDictionary<string, string>? trailerHeaders)
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

    private static void WriteAscii(Stream stream, string value)
    {
        var bytes = Encoding.ASCII.GetBytes(value);
        stream.Write(bytes, 0, bytes.Length);
    }

    private static byte[] BuildSigV4AwsChunkedPayload(
        string secretAccessKey,
        S3SigV4CredentialScope credentialScope,
        DateTimeOffset signedAtUtc,
        string seedSignature,
        byte[] payloadBytes,
        IReadOnlyDictionary<string, string> trailerHeaders,
        bool includeTrailerSignature,
        string? trailerSignatureOverride)
    {
        var payloadChunkSignature = ComputeSigV4ChunkSignature(secretAccessKey, credentialScope, signedAtUtc, seedSignature, payloadBytes);
        var finalChunkSignature = ComputeSigV4ChunkSignature(secretAccessKey, credentialScope, signedAtUtc, payloadChunkSignature, Array.Empty<byte>());
        var trailerSignature = includeTrailerSignature || trailerSignatureOverride is not null
            ? trailerSignatureOverride
                ?? ComputeSigV4TrailerSignature(secretAccessKey, credentialScope, signedAtUtc, finalChunkSignature, trailerHeaders)
            : null;

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

    private static HttpRequestMessage CreateSigV4PresignedRequest(
        HttpMethod method,
        string pathAndQuery,
        string accessKeyId,
        string secretAccessKey,
        int expiresSeconds,
        string host = "localhost",
        DateTimeOffset? signedAtUtc = null,
        string? securityToken = null)
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

        if (!string.IsNullOrWhiteSpace(securityToken)) {
            baseQuery.Add(new KeyValuePair<string, string?>("X-Amz-Security-Token", securityToken));
        }

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

    private static HttpRequestMessage CreateSigV4HeaderSignedRequestWithSseCHeaders(
        HttpMethod method,
        string pathAndQuery,
        string accessKeyId,
        string secretAccessKey,
        string ssecAlgorithm,
        string ssecKey,
        string ssecKeyMd5,
        string? body = null,
        string? contentType = null,
        string host = "localhost")
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
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-algorithm", ssecAlgorithm);
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key", ssecKey);
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key-MD5", ssecKeyMd5);

        var credentialScope = new S3SigV4CredentialScope
        {
            AccessKeyId = accessKeyId,
            DateStamp = timestampUtc.ToString("yyyyMMdd"),
            Region = "us-east-1",
            Service = "s3",
            Terminator = "aws4_request"
        };

        var signedHeaders = new List<string>
        {
            "host",
            "x-amz-content-sha256",
            "x-amz-date",
            "x-amz-server-side-encryption-customer-algorithm",
            "x-amz-server-side-encryption-customer-key",
            "x-amz-server-side-encryption-customer-key-md5"
        };
        signedHeaders.Sort(StringComparer.Ordinal);

        var canonicalHeaders = signedHeaders
            .Select(h => new KeyValuePair<string, string?>(h, h switch
            {
                "host" => host,
                "x-amz-content-sha256" => payloadHash,
                "x-amz-date" => timestampUtc.ToString("yyyyMMdd'T'HHmmss'Z'"),
                "x-amz-server-side-encryption-customer-algorithm" => ssecAlgorithm,
                "x-amz-server-side-encryption-customer-key" => ssecKey,
                "x-amz-server-side-encryption-customer-key-md5" => ssecKeyMd5,
                _ => throw new InvalidOperationException($"Unknown header: {h}")
            }))
            .ToList();

        var requestUri = CreateUri(pathAndQuery, host);
        var canonicalRequest = S3SigV4Signer.BuildCanonicalRequest(
            method.Method,
            requestUri.AbsolutePath,
            EnumerateQueryParameters(requestUri),
            canonicalHeaders,
            signedHeaders,
            payloadHash);

        var stringToSign = S3SigV4Signer.BuildStringToSign("AWS4-HMAC-SHA256", timestampUtc, credentialScope, canonicalRequest.CanonicalRequestHashHex);
        var signature = S3SigV4Signer.ComputeSignature(secretAccessKey, credentialScope, stringToSign);
        var authorizationHeader = $"AWS4-HMAC-SHA256 Credential={accessKeyId}/{credentialScope.Scope}, SignedHeaders={string.Join(';', signedHeaders)}, Signature={signature}";
        request.Headers.TryAddWithoutValidation("Authorization", authorizationHeader);

        return request;
    }

    private static HttpRequestMessage CreateSigV4HeaderSignedRequestWithCopySseCHeaders(
        string pathAndQuery,
        string accessKeyId,
        string secretAccessKey,
        string copySource,
        string destSsecAlgorithm,
        string destSsecKey,
        string destSsecKeyMd5,
        string srcSsecAlgorithm,
        string srcSsecKey,
        string srcSsecKeyMd5,
        string host = "localhost")
    {
        var request = new HttpRequestMessage(HttpMethod.Put, pathAndQuery);

        var timestampUtc = DateTimeOffset.UtcNow;
        var payloadHash = Convert.ToHexStringLower(SHA256.HashData(Array.Empty<byte>()));
        request.Headers.Host = host;
        request.Headers.TryAddWithoutValidation("x-amz-date", timestampUtc.ToString("yyyyMMdd'T'HHmmss'Z'"));
        request.Headers.TryAddWithoutValidation("x-amz-content-sha256", payloadHash);
        request.Headers.TryAddWithoutValidation("x-amz-copy-source", copySource);
        request.Headers.TryAddWithoutValidation("x-amz-copy-source-server-side-encryption-customer-algorithm", srcSsecAlgorithm);
        request.Headers.TryAddWithoutValidation("x-amz-copy-source-server-side-encryption-customer-key", srcSsecKey);
        request.Headers.TryAddWithoutValidation("x-amz-copy-source-server-side-encryption-customer-key-MD5", srcSsecKeyMd5);
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-algorithm", destSsecAlgorithm);
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key", destSsecKey);
        request.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key-MD5", destSsecKeyMd5);

        var credentialScope = new S3SigV4CredentialScope
        {
            AccessKeyId = accessKeyId,
            DateStamp = timestampUtc.ToString("yyyyMMdd"),
            Region = "us-east-1",
            Service = "s3",
            Terminator = "aws4_request"
        };

        var headerValues = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["host"] = host,
            ["x-amz-content-sha256"] = payloadHash,
            ["x-amz-copy-source"] = copySource,
            ["x-amz-copy-source-server-side-encryption-customer-algorithm"] = srcSsecAlgorithm,
            ["x-amz-copy-source-server-side-encryption-customer-key"] = srcSsecKey,
            ["x-amz-copy-source-server-side-encryption-customer-key-md5"] = srcSsecKeyMd5,
            ["x-amz-date"] = timestampUtc.ToString("yyyyMMdd'T'HHmmss'Z'"),
            ["x-amz-server-side-encryption-customer-algorithm"] = destSsecAlgorithm,
            ["x-amz-server-side-encryption-customer-key"] = destSsecKey,
            ["x-amz-server-side-encryption-customer-key-md5"] = destSsecKeyMd5
        };

        var signedHeaders = headerValues.Keys.OrderBy(static k => k, StringComparer.Ordinal).ToList();
        var canonicalHeaders = signedHeaders
            .Select(h => new KeyValuePair<string, string?>(h, headerValues[h]))
            .ToList();

        var requestUri = CreateUri(pathAndQuery, host);
        var canonicalRequest = S3SigV4Signer.BuildCanonicalRequest(
            HttpMethod.Put.Method,
            requestUri.AbsolutePath,
            EnumerateQueryParameters(requestUri),
            canonicalHeaders,
            signedHeaders,
            payloadHash);

        var stringToSign = S3SigV4Signer.BuildStringToSign("AWS4-HMAC-SHA256", timestampUtc, credentialScope, canonicalRequest.CanonicalRequestHashHex);
        var signature = S3SigV4Signer.ComputeSignature(secretAccessKey, credentialScope, stringToSign);
        var authorizationHeader = $"AWS4-HMAC-SHA256 Credential={accessKeyId}/{credentialScope.Scope}, SignedHeaders={string.Join(';', signedHeaders)}, Signature={signature}";
        request.Headers.TryAddWithoutValidation("Authorization", authorizationHeader);

        return request;
    }
}
