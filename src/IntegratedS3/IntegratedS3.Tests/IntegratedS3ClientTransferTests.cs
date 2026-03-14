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
    // Default behavior — overloads without preferred access mode leave selection explicit
    // -------------------------------------------------------------------------

    [Fact]
    public async Task DownloadToStreamAsync_WithoutPreferredAccessMode_LeavesPreferenceUnset()
    {
        var capturingClient = new CapturingIntegratedS3Client();
        await using var destination = new MemoryStream();

        await capturingClient.DownloadToStreamAsync(
            capturingClient.CreateNoOpTransferClient(),
            "bucket",
            "key",
            destination,
            expiresInSeconds: 60);

        Assert.Equal(StoragePresignOperation.GetObject, capturingClient.LastRequest?.Operation);
        Assert.Null(capturingClient.LastRequest?.PreferredAccessMode);
    }

    [Fact]
    public async Task UploadStreamAsync_WithoutPreferredAccessMode_LeavesPreferenceUnset()
    {
        var capturingClient = new CapturingIntegratedS3Client();
        await using var content = new MemoryStream("payload"u8.ToArray());

        await capturingClient.UploadStreamAsync(
            capturingClient.CreateNoOpTransferClient(),
            "bucket",
            "key",
            content,
            expiresInSeconds: 60);

        Assert.Equal(StoragePresignOperation.PutObject, capturingClient.LastRequest?.Operation);
        Assert.Null(capturingClient.LastRequest?.PreferredAccessMode);
    }

    [Fact]
    public async Task DownloadToFileWithResumeAsync_WithoutPreferredAccessMode_LeavesPreferenceUnset()
    {
        var capturingClient = new CapturingIntegratedS3Client();
        var tempDir = CreateTransferTempDirectory();
        Directory.CreateDirectory(tempDir);

        try {
            var destinationPath = Path.Combine(tempDir, "default-access-mode.txt");

            await capturingClient.DownloadToFileWithResumeAsync(
                capturingClient.CreateNoOpTransferClient(),
                "bucket",
                "key",
                destinationPath,
                expiresInSeconds: 60);

            Assert.Equal(StoragePresignOperation.GetObject, capturingClient.LastRequest?.Operation);
            Assert.Null(capturingClient.LastRequest?.PreferredAccessMode);
        }
        finally {
            Directory.Delete(tempDir, recursive: true);
        }
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
    // Unit — DownloadToFileWithResumeAsync resume and cleanup behavior
    // -------------------------------------------------------------------------

    [Fact]
    public async Task DownloadToFileWithResumeAsync_PartialContent_AppendsRemainingBytes()
    {
        var tempDir = CreateTransferTempDirectory();
        Directory.CreateDirectory(tempDir);
        try {
            var destPath = Path.Combine(tempDir, "resume-append.txt");
            await File.WriteAllTextAsync(destPath, "hello ");

            var capturingClient = new CapturingIntegratedS3Client();
            var existingLength = new FileInfo(destPath).Length;
            using var transferClient = new HttpClient(new DelegateHttpMessageHandler((request, cancellationToken) => {
                Assert.Equal("GET", request.Method.Method);
                Assert.NotNull(request.Headers.Range);
                var range = Assert.Single(request.Headers.Range!.Ranges);
                Assert.Equal(existingLength, range.From);
                Assert.Null(range.To);

                var response = new HttpResponseMessage(HttpStatusCode.PartialContent)
                {
                    Content = new ByteArrayContent(Encoding.UTF8.GetBytes("world"))
                };
                response.Content.Headers.ContentRange = new ContentRangeHeaderValue(
                    existingLength,
                    existingLength + "world".Length - 1,
                    existingLength + "world".Length);

                return Task.FromResult(response);
            }));

            await capturingClient.DownloadToFileWithResumeAsync(
                transferClient,
                "bucket",
                "key",
                destPath,
                expiresInSeconds: 60);

            Assert.Equal("hello world", await File.ReadAllTextAsync(destPath));
        }
        finally {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    [Fact]
    public async Task DownloadToFileWithResumeAsync_RangeIgnored_RewritesFromStartAndForwardsAccessMode()
    {
        var tempDir = CreateTransferTempDirectory();
        Directory.CreateDirectory(tempDir);
        try {
            var destPath = Path.Combine(tempDir, "range-ignored.txt");
            const string rewrittenPayload = "fresh complete payload";
            await File.WriteAllTextAsync(destPath, "stale partial payload");

            var capturingClient = new CapturingIntegratedS3Client();
            var existingLength = new FileInfo(destPath).Length;
            var requestCount = 0;
            using var transferClient = new HttpClient(new DelegateHttpMessageHandler((request, cancellationToken) => {
                requestCount++;
                Assert.NotNull(request.Headers.Range);
                var range = Assert.Single(request.Headers.Range!.Ranges);
                Assert.Equal(existingLength, range.From);
                Assert.Null(range.To);

                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new ByteArrayContent(Encoding.UTF8.GetBytes(rewrittenPayload))
                });
            }));

            await capturingClient.DownloadToFileWithResumeAsync(
                transferClient,
                "bucket",
                "key",
                destPath,
                expiresInSeconds: 60,
                preferredAccessMode: StorageAccessMode.Delegated);

            Assert.Equal(1, requestCount);
            Assert.Equal(StorageAccessMode.Delegated, capturingClient.LastRequest?.PreferredAccessMode);
            Assert.Equal(rewrittenPayload, await File.ReadAllTextAsync(destPath));
        }
        finally {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    [Fact]
    public async Task DownloadToFileWithResumeAsync_RequestedRangeNotSatisfiableWithMatchingLength_TreatsDownloadAsComplete()
    {
        var tempDir = CreateTransferTempDirectory();
        Directory.CreateDirectory(tempDir);
        try {
            var destPath = Path.Combine(tempDir, "already-complete.txt");
            const string existingPayload = "already complete";
            await File.WriteAllTextAsync(destPath, existingPayload);

            var existingLength = new FileInfo(destPath).Length;
            var capturingClient = new CapturingIntegratedS3Client();
            using var transferClient = new HttpClient(new DelegateHttpMessageHandler((request, cancellationToken) => {
                Assert.NotNull(request.Headers.Range);
                var range = Assert.Single(request.Headers.Range!.Ranges);
                Assert.Equal(existingLength, range.From);
                Assert.Null(range.To);

                var response = new HttpResponseMessage(HttpStatusCode.RequestedRangeNotSatisfiable)
                {
                    Content = new ByteArrayContent([])
                };
                response.Content.Headers.ContentRange = new ContentRangeHeaderValue(existingLength);
                return Task.FromResult(response);
            }));

            await capturingClient.DownloadToFileWithResumeAsync(
                transferClient,
                "bucket",
                "key",
                destPath,
                expiresInSeconds: 60);

            Assert.Equal(existingPayload, await File.ReadAllTextAsync(destPath));
        }
        finally {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    [Fact]
    public async Task DownloadToFileWithResumeAsync_RequestedRangeNotSatisfiableWithMismatchedLength_RewritesFromStart()
    {
        var tempDir = CreateTransferTempDirectory();
        Directory.CreateDirectory(tempDir);
        try {
            var destPath = Path.Combine(tempDir, "range-mismatch.txt");
            const string existingPayload = "stale-local-copy";
            const string rewrittenPayload = "rewritten after mismatch";
            await File.WriteAllTextAsync(destPath, existingPayload);

            var existingLength = new FileInfo(destPath).Length;
            var capturingClient = new CapturingIntegratedS3Client();
            var requestCount = 0;
            using var transferClient = new HttpClient(new DelegateHttpMessageHandler((request, cancellationToken) => {
                requestCount++;

                if (requestCount == 1) {
                    Assert.NotNull(request.Headers.Range);
                    var range = Assert.Single(request.Headers.Range!.Ranges);
                    Assert.Equal(existingLength, range.From);
                    Assert.Null(range.To);

                    var response = new HttpResponseMessage(HttpStatusCode.RequestedRangeNotSatisfiable)
                    {
                        Content = new ByteArrayContent([])
                    };
                    response.Content.Headers.ContentRange = new ContentRangeHeaderValue(existingLength + 10);
                    return Task.FromResult(response);
                }

                Assert.Null(request.Headers.Range);
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new ByteArrayContent(Encoding.UTF8.GetBytes(rewrittenPayload))
                });
            }));

            await capturingClient.DownloadToFileWithResumeAsync(
                transferClient,
                "bucket",
                "key",
                destPath,
                expiresInSeconds: 60);

            Assert.Equal(2, requestCount);
            Assert.Equal(rewrittenPayload, await File.ReadAllTextAsync(destPath));
        }
        finally {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    [Fact]
    public async Task DownloadToFileWithResumeAsync_ResumeTransferFails_PreservesExistingPartialFile()
    {
        var tempDir = CreateTransferTempDirectory();
        Directory.CreateDirectory(tempDir);
        try {
            var destPath = Path.Combine(tempDir, "resume-failure.txt");
            await File.WriteAllTextAsync(destPath, "partial-");

            var capturingClient = new CapturingIntegratedS3Client();
            var existingLength = new FileInfo(destPath).Length;
            using var transferClient = new HttpClient(new DelegateHttpMessageHandler((request, cancellationToken) => {
                var response = new HttpResponseMessage(HttpStatusCode.PartialContent)
                {
                    Content = new StreamContent(new ThrowingReadStream(
                        Encoding.UTF8.GetBytes("tail"),
                        bytesBeforeFailure: 2,
                        new IOException("Simulated resume failure.")))
                };
                response.Content.Headers.ContentRange = new ContentRangeHeaderValue(
                    existingLength,
                    existingLength + 3,
                    existingLength + 4);
                return Task.FromResult(response);
            }));

            var exception = await Assert.ThrowsAsync<HttpRequestException>(() =>
                capturingClient.DownloadToFileWithResumeAsync(
                    transferClient,
                    "bucket",
                    "key",
                    destPath,
                    expiresInSeconds: 60));

            Assert.IsType<IOException>(exception.InnerException);
            Assert.True(File.Exists(destPath), "Pre-existing partial files should be preserved on resume failure.");
            Assert.Equal("partial-ta", await File.ReadAllTextAsync(destPath));
        }
        finally {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    [Fact]
    public async Task DownloadToFileWithResumeAsync_NewDestinationTransferFails_DeletesCreatedFile()
    {
        var tempDir = CreateTransferTempDirectory();
        Directory.CreateDirectory(tempDir);
        try {
            var destPath = Path.Combine(tempDir, "new-destination-failure.txt");
            var capturingClient = new CapturingIntegratedS3Client();
            using var transferClient = new HttpClient(new DelegateHttpMessageHandler((request, cancellationToken) =>
                Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StreamContent(new ThrowingReadStream(
                        Encoding.UTF8.GetBytes("payload"),
                        bytesBeforeFailure: 3,
                        new IOException("Simulated new file failure.")))
                })));

            var exception = await Assert.ThrowsAsync<HttpRequestException>(() =>
                capturingClient.DownloadToFileWithResumeAsync(
                    transferClient,
                    "bucket",
                    "key",
                    destPath,
                    expiresInSeconds: 60));

            Assert.IsType<IOException>(exception.InnerException);
            Assert.False(File.Exists(destPath), "Files created during this call should be removed when the transfer fails.");
        }
        finally {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static string CreateTransferTempDirectory()
        => Path.Combine(Path.GetTempPath(), "IntegratedS3.ClientTransferTests", Guid.NewGuid().ToString("N"));

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

    private sealed class DelegateHttpMessageHandler(
        Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> handler) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(request);
            return handler(request, cancellationToken);
        }
    }

    private sealed class ThrowingReadStream(
        byte[] payload,
        int bytesBeforeFailure,
        Exception failure) : Stream
    {
        private readonly byte[] _payload = payload;
        private readonly int _failureOffset = Math.Clamp(bytesBeforeFailure, 0, payload.Length);
        private readonly Exception _failure = failure;
        private int _position;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => _payload.Length;
        public override long Position
        {
            get => _position;
            set => throw new NotSupportedException();
        }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            ArgumentNullException.ThrowIfNull(buffer);
            return ReadCore(buffer.AsSpan(offset, count));
        }

        public override int Read(Span<byte> buffer)
            => ReadCore(buffer);

        public override ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(ReadCore(buffer.Span));
        }

        public override Task<int> ReadAsync(
            byte[] buffer,
            int offset,
            int count,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(Read(buffer, offset, count));
        }

        public override long Seek(long offset, SeekOrigin origin)
            => throw new NotSupportedException();

        public override void SetLength(long value)
            => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count)
            => throw new NotSupportedException();

        private int ReadCore(Span<byte> buffer)
        {
            if (buffer.IsEmpty) {
                return 0;
            }

            if (_position >= _failureOffset) {
                throw _failure;
            }

            var bytesRemainingBeforeFailure = _failureOffset - _position;
            var bytesRemaining = Math.Min(bytesRemainingBeforeFailure, _payload.Length - _position);
            var bytesToCopy = Math.Min(buffer.Length, bytesRemaining);
            if (bytesToCopy <= 0) {
                throw _failure;
            }

            _payload.AsSpan(_position, bytesToCopy).CopyTo(buffer);
            _position += bytesToCopy;
            return bytesToCopy;
        }
    }
}
