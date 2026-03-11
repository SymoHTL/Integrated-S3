using System.Net;
using System.Net.Http.Headers;
using System.Security.Claims;
using System.Text;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.AspNetCore;
using IntegratedS3.Client;
using IntegratedS3.Core.Models;
using IntegratedS3.Core.Services;
using IntegratedS3.Tests.Infrastructure;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace IntegratedS3.Tests;

/// <summary>
/// Tests for <see cref="IntegratedS3ClientTransferExtensions"/> and
/// <see cref="StoragePresignedRequestExtensions"/> that cover client-side upload/download helpers.
/// </summary>
public sealed class IntegratedS3ClientTransferTests(WebUiApplicationFactory factory) : IClassFixture<WebUiApplicationFactory>
{
    private readonly WebUiApplicationFactory _factory = factory;

    // -------------------------------------------------------------------------
    // Integration — stream/file round-trip via proxy-mode presigned URLs
    // -------------------------------------------------------------------------

    [Fact]
    public async Task UploadStreamAsync_ThenDownloadToStreamAsync_RoundTripsContent()
    {
        const string bucketName = "transfer-stream-bucket";
        const string objectKey = "docs/transfer-stream.txt";
        const string payload = "hello from UploadStreamAsync";

        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(ConfigurePresignHost("transfer-stream-access", "transfer-stream-secret"));

        using var authClient = isolatedClient.Client;
        authClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "storage.read storage.write");

        using var transferClient = isolatedClient.CreateAdditionalClient();

        var integratedClient = new IntegratedS3Client(authClient);

        Assert.Equal(HttpStatusCode.Created, (await authClient.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes(payload));
        await integratedClient.UploadStreamAsync(transferClient, bucketName, objectKey, uploadStream, expiresInSeconds: 300, contentType: "text/plain");

        await using var downloadStream = new MemoryStream();
        await integratedClient.DownloadToStreamAsync(transferClient, bucketName, objectKey, downloadStream, expiresInSeconds: 300);

        Assert.Equal(payload, Encoding.UTF8.GetString(downloadStream.ToArray()));
    }

    [Fact]
    public async Task UploadFileAsync_ThenDownloadToFileAsync_RoundTripsContent()
    {
        const string bucketName = "transfer-file-bucket";
        const string objectKey = "docs/transfer-file.txt";
        const string payload = "hello from UploadFileAsync round-trip";

        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(ConfigurePresignHost("transfer-file-access", "transfer-file-secret"));

        using var authClient = isolatedClient.Client;
        authClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "storage.read storage.write");

        using var transferClient = isolatedClient.CreateAdditionalClient();

        var integratedClient = new IntegratedS3Client(authClient);

        Assert.Equal(HttpStatusCode.Created, (await authClient.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        var tempDir = Path.Combine(Path.GetTempPath(), "IntegratedS3.ClientTransferTests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);
        try {
            var uploadPath = Path.Combine(tempDir, "upload.txt");
            var downloadPath = Path.Combine(tempDir, "download.txt");
            await File.WriteAllTextAsync(uploadPath, payload);

            await integratedClient.UploadFileAsync(transferClient, bucketName, objectKey, uploadPath, expiresInSeconds: 300, contentType: "text/plain");
            await integratedClient.DownloadToFileAsync(transferClient, bucketName, objectKey, downloadPath, expiresInSeconds: 300);

            Assert.Equal(payload, await File.ReadAllTextAsync(downloadPath));
        }
        finally {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    [Fact]
    public async Task UploadStreamAsync_WithoutContentType_SucceedsAndBodyIsPreserved()
    {
        const string bucketName = "transfer-no-ct-bucket";
        const string objectKey = "docs/transfer-no-ct.bin";
        var payload = new byte[] { 0x01, 0x02, 0x03, 0xFF };

        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(ConfigurePresignHost("transfer-noct-access", "transfer-noct-secret"));

        using var authClient = isolatedClient.Client;
        authClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "storage.read storage.write");

        using var transferClient = isolatedClient.CreateAdditionalClient();

        var integratedClient = new IntegratedS3Client(authClient);

        Assert.Equal(HttpStatusCode.Created, (await authClient.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null)).StatusCode);

        await using var uploadStream = new MemoryStream(payload);
        await integratedClient.UploadStreamAsync(transferClient, bucketName, objectKey, uploadStream, expiresInSeconds: 300);

        await using var downloadStream = new MemoryStream();
        await integratedClient.DownloadToStreamAsync(transferClient, bucketName, objectKey, downloadStream, expiresInSeconds: 300);

        Assert.Equal(payload, downloadStream.ToArray());
    }

    // -------------------------------------------------------------------------
    // Unit — access-mode overloads forward preference through presign request
    // -------------------------------------------------------------------------

    [Theory]
    [InlineData(StorageAccessMode.Direct)]
    [InlineData(StorageAccessMode.Delegated)]
    [InlineData(StorageAccessMode.Proxy)]
    public async Task DownloadToStreamAsync_WithPreferredAccessMode_ForwardsPreferenceToPresignRequest(StorageAccessMode preferredMode)
    {
        var capturingClient = new CapturingIntegratedS3Client();
        await using var destination = new MemoryStream();

        await capturingClient.DownloadToStreamAsync(
            capturingClient.CreateNoOpTransferClient(),
            "bucket", "key", destination,
            expiresInSeconds: 60,
            preferredAccessMode: preferredMode);

        Assert.Equal(StoragePresignOperation.GetObject, capturingClient.LastRequest?.Operation);
        Assert.Equal(preferredMode, capturingClient.LastRequest?.PreferredAccessMode);
    }

    [Theory]
    [InlineData(StorageAccessMode.Direct)]
    [InlineData(StorageAccessMode.Delegated)]
    [InlineData(StorageAccessMode.Proxy)]
    public async Task DownloadToStreamAsync_WithPreferredAccessModeAndVersionId_ForwardsBoth(StorageAccessMode preferredMode)
    {
        var capturingClient = new CapturingIntegratedS3Client();
        await using var destination = new MemoryStream();

        await capturingClient.DownloadToStreamAsync(
            capturingClient.CreateNoOpTransferClient(),
            "bucket", "key", destination,
            expiresInSeconds: 60,
            preferredAccessMode: preferredMode,
            versionId: "v-99");

        Assert.Equal(preferredMode, capturingClient.LastRequest?.PreferredAccessMode);
        Assert.Equal("v-99", capturingClient.LastRequest?.VersionId);
    }

    [Theory]
    [InlineData(StorageAccessMode.Direct)]
    [InlineData(StorageAccessMode.Delegated)]
    [InlineData(StorageAccessMode.Proxy)]
    public async Task DownloadToFileAsync_WithPreferredAccessMode_ForwardsPreferenceToPresignRequest(StorageAccessMode preferredMode)
    {
        var capturingClient = new CapturingIntegratedS3Client();

        var tempFile = Path.GetTempFileName();
        try {
            await capturingClient.DownloadToFileAsync(
                capturingClient.CreateNoOpTransferClient(),
                "bucket", "key", tempFile,
                expiresInSeconds: 60,
                preferredAccessMode: preferredMode);

            Assert.Equal(StoragePresignOperation.GetObject, capturingClient.LastRequest?.Operation);
            Assert.Equal(preferredMode, capturingClient.LastRequest?.PreferredAccessMode);
        }
        finally {
            File.Delete(tempFile);
        }
    }

    [Theory]
    [InlineData(StorageAccessMode.Direct)]
    [InlineData(StorageAccessMode.Delegated)]
    [InlineData(StorageAccessMode.Proxy)]
    public async Task UploadStreamAsync_WithPreferredAccessMode_ForwardsPreferenceToPresignRequest(StorageAccessMode preferredMode)
    {
        var capturingClient = new CapturingIntegratedS3Client();
        await using var content = new MemoryStream("payload"u8.ToArray());

        await capturingClient.UploadStreamAsync(
            capturingClient.CreateNoOpTransferClient(),
            "bucket", "key", content,
            expiresInSeconds: 60,
            preferredAccessMode: preferredMode);

        Assert.Equal(StoragePresignOperation.PutObject, capturingClient.LastRequest?.Operation);
        Assert.Equal(preferredMode, capturingClient.LastRequest?.PreferredAccessMode);
    }

    [Theory]
    [InlineData(StorageAccessMode.Direct)]
    [InlineData(StorageAccessMode.Delegated)]
    [InlineData(StorageAccessMode.Proxy)]
    public async Task UploadStreamAsync_WithPreferredAccessModeAndContentType_ForwardsBoth(StorageAccessMode preferredMode)
    {
        var capturingClient = new CapturingIntegratedS3Client();
        await using var content = new MemoryStream("payload"u8.ToArray());

        await capturingClient.UploadStreamAsync(
            capturingClient.CreateNoOpTransferClient(),
            "bucket", "key", content,
            expiresInSeconds: 60,
            preferredAccessMode: preferredMode,
            contentType: "application/octet-stream");

        Assert.Equal(preferredMode, capturingClient.LastRequest?.PreferredAccessMode);
        Assert.Equal("application/octet-stream", capturingClient.LastRequest?.ContentType);
    }

    [Theory]
    [InlineData(StorageAccessMode.Direct)]
    [InlineData(StorageAccessMode.Delegated)]
    [InlineData(StorageAccessMode.Proxy)]
    public async Task UploadFileAsync_WithPreferredAccessMode_ForwardsPreferenceToPresignRequest(StorageAccessMode preferredMode)
    {
        var capturingClient = new CapturingIntegratedS3Client();

        var tempFile = Path.GetTempFileName();
        try {
            await File.WriteAllTextAsync(tempFile, "test content");
            await capturingClient.UploadFileAsync(
                capturingClient.CreateNoOpTransferClient(),
                "bucket", "key", tempFile,
                expiresInSeconds: 60,
                preferredAccessMode: preferredMode);

            Assert.Equal(StoragePresignOperation.PutObject, capturingClient.LastRequest?.Operation);
            Assert.Equal(preferredMode, capturingClient.LastRequest?.PreferredAccessMode);
        }
        finally {
            File.Delete(tempFile);
        }
    }

    // -------------------------------------------------------------------------
    // Unit — CreateHttpRequestMessage with various access modes
    // -------------------------------------------------------------------------

    [Fact]
    public void CreateHttpRequestMessage_WithProxyPresignedRequest_UsesPresignedUrl()
    {
        var presignedUrl = new Uri("http://localhost/integrated-s3/buckets/docs/objects/guide.txt?X-Amz-Signature=abc", UriKind.Absolute);
        var presigned = new StoragePresignedRequest
        {
            Operation = StoragePresignOperation.GetObject,
            AccessMode = StorageAccessMode.Proxy,
            Method = "GET",
            Url = presignedUrl,
            ExpiresAtUtc = DateTimeOffset.UtcNow.AddMinutes(5),
            BucketName = "docs",
            Key = "guide.txt"
        };

        using var request = presigned.CreateHttpRequestMessage();

        Assert.Equal(presignedUrl, request.RequestUri);
        Assert.Equal(HttpMethod.Get, request.Method);
        Assert.Null(request.Content);
    }

    [Fact]
    public void CreateHttpRequestMessage_WithDelegatedPresignedRequest_UsesProviderUrl()
    {
        var delegatedUrl = new Uri("https://s3.us-east-1.amazonaws.com/docs/guide.txt?X-Amz-Signature=provider123", UriKind.Absolute);
        var presigned = new StoragePresignedRequest
        {
            Operation = StoragePresignOperation.GetObject,
            AccessMode = StorageAccessMode.Delegated,
            Method = "GET",
            Url = delegatedUrl,
            ExpiresAtUtc = DateTimeOffset.UtcNow.AddMinutes(15),
            BucketName = "docs",
            Key = "guide.txt"
        };

        using var request = presigned.CreateHttpRequestMessage();

        Assert.Equal(delegatedUrl, request.RequestUri);
        Assert.Equal(HttpMethod.Get, request.Method);
    }

    [Fact]
    public void CreateHttpRequestMessage_WithDirectPresignedRequest_UsesPublicUrl()
    {
        var directUrl = new Uri("https://cdn.example.com/docs/guide.txt", UriKind.Absolute);
        var presigned = new StoragePresignedRequest
        {
            Operation = StoragePresignOperation.GetObject,
            AccessMode = StorageAccessMode.Direct,
            Method = "GET",
            Url = directUrl,
            ExpiresAtUtc = DateTimeOffset.UtcNow.AddHours(1),
            BucketName = "docs",
            Key = "guide.txt"
        };

        using var request = presigned.CreateHttpRequestMessage();

        Assert.Equal(directUrl, request.RequestUri);
    }

    [Fact]
    public void CreateHttpRequestMessage_WithPutAndContentType_AppliesContentTypeToContent()
    {
        var presignedUrl = new Uri("https://host.test/integrated-s3/buckets/b/objects/k?X-Amz-Signature=x", UriKind.Absolute);
        var presigned = new StoragePresignedRequest
        {
            Operation = StoragePresignOperation.PutObject,
            AccessMode = StorageAccessMode.Proxy,
            Method = "PUT",
            Url = presignedUrl,
            ExpiresAtUtc = DateTimeOffset.UtcNow.AddMinutes(5),
            BucketName = "b",
            Key = "k",
            ContentType = "text/plain",
            Headers = [new StoragePresignedHeader { Name = "Content-Type", Value = "text/plain" }]
        };

        using var content = new StreamContent(new MemoryStream("payload"u8.ToArray()));
        using var request = presigned.CreateHttpRequestMessage(content);

        Assert.Equal("text/plain", request.Content!.Headers.ContentType?.MediaType);
    }

    // -------------------------------------------------------------------------
    // Unit — argument validation
    // -------------------------------------------------------------------------

    [Fact]
    public async Task UploadStreamAsync_NullClient_Throws()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            IntegratedS3ClientTransferExtensions.UploadStreamAsync(
                null!, new HttpClient(), "b", "k", new MemoryStream(), 60));
    }

    [Fact]
    public async Task UploadStreamAsync_NullTransferClient_Throws()
    {
        var client = new StubIntegratedS3Client();
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            client.UploadStreamAsync(null!, "b", "k", new MemoryStream(), 60));
    }

    [Fact]
    public async Task UploadStreamAsync_NullContent_Throws()
    {
        var client = new StubIntegratedS3Client();
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            client.UploadStreamAsync(new HttpClient(), "b", "k", null!, 60));
    }

    [Fact]
    public async Task UploadFileAsync_NullOrWhitespaceFilePath_Throws()
    {
        var client = new StubIntegratedS3Client();
        await Assert.ThrowsAsync<ArgumentException>(() =>
            client.UploadFileAsync(new HttpClient(), "b", "k", "  ", 60));
    }

    [Fact]
    public async Task DownloadToStreamAsync_NullClient_Throws()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            IntegratedS3ClientTransferExtensions.DownloadToStreamAsync(
                null!, new HttpClient(), "b", "k", new MemoryStream(), 60));
    }

    [Fact]
    public async Task DownloadToStreamAsync_NullDestination_Throws()
    {
        var client = new StubIntegratedS3Client();
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            client.DownloadToStreamAsync(new HttpClient(), "b", "k", null!, 60));
    }

    [Fact]
    public async Task DownloadToFileAsync_NullOrWhitespaceFilePath_Throws()
    {
        var client = new StubIntegratedS3Client();
        await Assert.ThrowsAsync<ArgumentException>(() =>
            client.DownloadToFileAsync(new HttpClient(), "b", "k", "", 60));
    }

    // -------------------------------------------------------------------------
    // Unit — DownloadToFileAsync failure cleanup
    // -------------------------------------------------------------------------

    [Fact]
    public async Task DownloadToFileAsync_PresignFails_DoesNotCreateDestinationFile()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "IntegratedS3.FailureCleanupTests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);
        try {
            var destPath = Path.Combine(tempDir, "should-not-exist.txt");
            var failingClient = new FailingPresignClient();

            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                failingClient.DownloadToFileAsync(
                    new HttpClient(new NoOpHttpMessageHandler()),
                    "bucket", "key", destPath, expiresInSeconds: 60));

            Assert.False(File.Exists(destPath), "No file should be created when presign fails.");
        }
        finally {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    [Fact]
    public async Task DownloadToFileAsync_TransferReturnsError_DeletesPartialFile()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "IntegratedS3.FailureCleanupTests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);
        try {
            var destPath = Path.Combine(tempDir, "partial.txt");
            var capturingClient = new CapturingIntegratedS3Client();
            using var transferClient = new HttpClient(new FixedStatusHttpMessageHandler(HttpStatusCode.InternalServerError));

            await Assert.ThrowsAsync<HttpRequestException>(() =>
                capturingClient.DownloadToFileAsync(
                    transferClient,
                    "bucket", "key", destPath, expiresInSeconds: 60));

            Assert.False(File.Exists(destPath), "Partial file must be deleted when the transfer returns an error status.");
        }
        finally {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    [Fact]
    public async Task DownloadToFileAsync_TransferCancelled_DeletesPartialFile()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "IntegratedS3.FailureCleanupTests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);
        try {
            var destPath = Path.Combine(tempDir, "cancelled.txt");
            var capturingClient = new CapturingIntegratedS3Client();
            using var cts = new CancellationTokenSource();
            await cts.CancelAsync();

            // Cancellation fires before or during the transfer; either way the file should not survive.
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                capturingClient.DownloadToFileAsync(
                    new HttpClient(new NoOpHttpMessageHandler()),
                    "bucket", "key", destPath,
                    expiresInSeconds: 60,
                    cancellationToken: cts.Token));

            Assert.False(File.Exists(destPath), "Destination file must be removed when the download is cancelled.");
        }
        finally {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    [Fact]
    public async Task DownloadToFileAsync_WithAccessMode_TransferReturnsError_DeletesPartialFile()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "IntegratedS3.FailureCleanupTests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);
        try {
            var destPath = Path.Combine(tempDir, "partial-am.txt");
            var capturingClient = new CapturingIntegratedS3Client();
            using var transferClient = new HttpClient(new FixedStatusHttpMessageHandler(HttpStatusCode.NotFound));

            await Assert.ThrowsAsync<HttpRequestException>(() =>
                capturingClient.DownloadToFileAsync(
                    transferClient,
                    "bucket", "key", destPath,
                    expiresInSeconds: 60,
                    preferredAccessMode: StorageAccessMode.Proxy));

            Assert.False(File.Exists(destPath), "Partial file must be deleted when the access-mode overload transfer returns an error.");
        }
        finally {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Configures an isolated test host with SigV4 + TestHeader authentication
    /// and an allow-all authorization policy, matching the presign test setup.
    /// </summary>
    private static Action<Microsoft.AspNetCore.Builder.WebApplicationBuilder> ConfigurePresignHost(
        string accessKeyId,
        string secretAccessKey)
    {
        return builder => {
            builder.Services.Configure<IntegratedS3Options>(options => {
                options.EnableAwsSignatureV4Authentication = true;
                options.PresignAccessKeyId = accessKeyId;
                options.AccessKeyCredentials =
                [
                    new IntegratedS3AccessKeyCredential
                    {
                        AccessKeyId = accessKeyId,
                        SecretAccessKey = secretAccessKey,
                        DisplayName = "transfer-test-user",
                        Scopes = ["storage.read", "storage.write"]
                    }
                ];
            });
            builder.Services.AddAuthentication("TestHeader")
                .AddScheme<AuthenticationSchemeOptions, TestHeaderAuthenticationHandler>("TestHeader", static _ => { });
            builder.Services.AddSingleton<IIntegratedS3AuthorizationService, AllowAllTransferAuthorizationService>();
        };
    }

    /// <summary>Allows all operations unconditionally; used so transfer tests are not blocked by auth logic.</summary>
    private sealed class AllowAllTransferAuthorizationService : IIntegratedS3AuthorizationService
    {
        public ValueTask<StorageResult> AuthorizeAsync(
            ClaimsPrincipal principal,
            StorageAuthorizationRequest request,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(principal);
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult.Success());
        }
    }

    /// <summary>Stub client that always throws; used only to trigger argument-validation paths.</summary>
    private sealed class StubIntegratedS3Client : IIntegratedS3Client
    {
        public ValueTask<StoragePresignedRequest> PresignObjectAsync(
            StoragePresignRequest request,
            CancellationToken cancellationToken = default)
            => throw new NotSupportedException("Stub — should not be called in argument-validation tests.");
    }

    /// <summary>Client whose <see cref="IIntegratedS3Client.PresignObjectAsync"/> always throws, used to verify that
    /// <see cref="IntegratedS3ClientTransferExtensions.DownloadToFileAsync"/> does not create the destination file
    /// when presigning fails before the file is opened.</summary>
    private sealed class FailingPresignClient : IIntegratedS3Client
    {
        public ValueTask<StoragePresignedRequest> PresignObjectAsync(
            StoragePresignRequest request,
            CancellationToken cancellationToken = default)
            => throw new InvalidOperationException("Simulated presign failure.");
    }

    /// <summary>
    /// Capturing client that records the last presign request and returns a stub response,
    /// paired with a no-op <see cref="HttpClient"/> so transfer helpers complete successfully.
    /// </summary>
    private sealed class CapturingIntegratedS3Client : IIntegratedS3Client
    {
        public StoragePresignRequest? LastRequest { get; private set; }

        public ValueTask<StoragePresignedRequest> PresignObjectAsync(
            StoragePresignRequest request,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(request);
            cancellationToken.ThrowIfCancellationRequested();

            LastRequest = request;
            return ValueTask.FromResult(new StoragePresignedRequest
            {
                Operation = request.Operation,
                AccessMode = request.PreferredAccessMode ?? StorageAccessMode.Proxy,
                Method = request.Operation == StoragePresignOperation.GetObject ? "GET" : "PUT",
                Url = new Uri("https://example.test/presign", UriKind.Absolute),
                ExpiresAtUtc = DateTimeOffset.UtcNow.AddMinutes(5),
                BucketName = request.BucketName,
                Key = request.Key,
                VersionId = request.VersionId,
                ContentType = request.ContentType
            });
        }

        /// <summary>
        /// Returns an <see cref="HttpClient"/> whose handler always returns HTTP 200 with an empty body,
        /// allowing transfer helpers to complete without a real server.
        /// </summary>
        public HttpClient CreateNoOpTransferClient()
            => new(new NoOpHttpMessageHandler());
    }

    /// <summary>Handler that always returns HTTP 200 with an empty body.</summary>
    private sealed class NoOpHttpMessageHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
            => Task.FromResult(new HttpResponseMessage(System.Net.HttpStatusCode.OK)
            {
                Content = new ByteArrayContent([])
            });
    }

    /// <summary>Handler that always returns the given status code with an empty body.</summary>
    private sealed class FixedStatusHttpMessageHandler(HttpStatusCode statusCode) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
            => Task.FromResult(new HttpResponseMessage(statusCode)
            {
                Content = new ByteArrayContent([])
            });
    }
}
