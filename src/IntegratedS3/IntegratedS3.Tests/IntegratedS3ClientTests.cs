using System.Net;
using System.Net.Http.Json;
using System.Text;
using IntegratedS3.Client;
using IntegratedS3.Core.Models;
using Xunit;

namespace IntegratedS3.Tests;

/// <summary>
/// Tests for <see cref="IntegratedS3Client"/> covering error-reporting behavior on the presign endpoint.
/// </summary>
public sealed class IntegratedS3ClientTests
{
    // -------------------------------------------------------------------------
    // Unit — PresignObjectAsync surfaces server error details
    // -------------------------------------------------------------------------

    [Fact]
    public async Task PresignObjectAsync_ServerReturns403_ThrowsHttpRequestExceptionWithStatusCode()
    {
        using var httpClient = CreateHttpClient(
            HttpStatusCode.Forbidden,
            body: null);
        var client = new IntegratedS3Client(httpClient);

        var ex = await Assert.ThrowsAsync<HttpRequestException>(() =>
            client.PresignObjectAsync(new StoragePresignRequest
            {
                Operation = StoragePresignOperation.GetObject,
                BucketName = "b",
                Key = "k",
                ExpiresInSeconds = 60
            }).AsTask());

        Assert.Equal(HttpStatusCode.Forbidden, ex.StatusCode);
    }

    [Fact]
    public async Task PresignObjectAsync_ServerReturnsErrorWithBody_ExceptionMessageContainsBody()
    {
        const string errorBody = "{\"title\":\"Forbidden\",\"detail\":\"Presign permission denied.\"}";

        using var httpClient = CreateHttpClient(
            HttpStatusCode.Forbidden,
            body: errorBody);
        var client = new IntegratedS3Client(httpClient);

        var ex = await Assert.ThrowsAsync<HttpRequestException>(() =>
            client.PresignObjectAsync(new StoragePresignRequest
            {
                Operation = StoragePresignOperation.GetObject,
                BucketName = "b",
                Key = "k",
                ExpiresInSeconds = 60
            }).AsTask());

        Assert.Contains(errorBody, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PresignObjectAsync_ServerReturns400_ExceptionMessageContainsStatusCode()
    {
        using var httpClient = CreateHttpClient(
            HttpStatusCode.BadRequest,
            body: "Bad request detail");
        var client = new IntegratedS3Client(httpClient);

        var ex = await Assert.ThrowsAsync<HttpRequestException>(() =>
            client.PresignObjectAsync(new StoragePresignRequest
            {
                Operation = StoragePresignOperation.PutObject,
                BucketName = "b",
                Key = "k",
                ExpiresInSeconds = 60
            }).AsTask());

        Assert.Contains("400", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PresignObjectAsync_ServerReturnsErrorWithEmptyBody_ThrowsWithoutCrashing()
    {
        using var httpClient = CreateHttpClient(
            HttpStatusCode.ServiceUnavailable,
            body: "");
        var client = new IntegratedS3Client(httpClient);

        var ex = await Assert.ThrowsAsync<HttpRequestException>(() =>
            client.PresignObjectAsync(new StoragePresignRequest
            {
                Operation = StoragePresignOperation.GetObject,
                BucketName = "b",
                Key = "k",
                ExpiresInSeconds = 60
            }).AsTask());

        Assert.Equal(HttpStatusCode.ServiceUnavailable, ex.StatusCode);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static HttpClient CreateHttpClient(HttpStatusCode statusCode, string? body)
    {
        return new HttpClient(new FixedResponseHandler(statusCode, body))
        {
            BaseAddress = new Uri("https://integrated-s3.test/", UriKind.Absolute)
        };
    }

    /// <summary>Handler that returns a fixed status code and optional body text.</summary>
    private sealed class FixedResponseHandler(HttpStatusCode statusCode, string? body) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            var content = body is null
                ? new ByteArrayContent([])
                : new StringContent(body, Encoding.UTF8, "application/json");

            return Task.FromResult(new HttpResponseMessage(statusCode) { Content = content });
        }
    }
}
