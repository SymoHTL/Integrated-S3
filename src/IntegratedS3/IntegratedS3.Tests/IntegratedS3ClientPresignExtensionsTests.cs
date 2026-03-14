using IntegratedS3.Client;
using IntegratedS3.Core.Models;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class IntegratedS3ClientPresignExtensionsTests
{
    [Fact]
    public async Task PresignGetObjectAsync_WithoutPreferredAccessMode_LeavesPreferenceNullAndForwardsVersion()
    {
        var client = new CapturingIntegratedS3Client();

        await client.PresignGetObjectAsync(
            "docs",
            "guide.txt",
            expiresInSeconds: 300,
            versionId: "v-123");

        Assert.Equal(StoragePresignOperation.GetObject, client.LastRequest?.Operation);
        Assert.Null(client.LastRequest?.PreferredAccessMode);
        Assert.Equal("v-123", client.LastRequest?.VersionId);
    }

    [Fact]
    public async Task PresignGetObjectAsync_WithPreferredAccessMode_ForwardsPreferenceAndVersion()
    {
        var client = new CapturingIntegratedS3Client();

        await client.PresignGetObjectAsync(
            "docs",
            "guide.txt",
            expiresInSeconds: 300,
            preferredAccessMode: StorageAccessMode.Direct,
            versionId: "v-123");

        Assert.Equal(StoragePresignOperation.GetObject, client.LastRequest?.Operation);
        Assert.Equal(StorageAccessMode.Direct, client.LastRequest?.PreferredAccessMode);
        Assert.Equal("v-123", client.LastRequest?.VersionId);
    }

    [Fact]
    public async Task PresignPutObjectAsync_WithoutPreferredAccessMode_LeavesPreferenceNullAndForwardsContentType()
    {
        var client = new CapturingIntegratedS3Client();

        await client.PresignPutObjectAsync(
            "docs",
            "guide.txt",
            expiresInSeconds: 300,
            contentType: "text/plain");

        Assert.Equal(StoragePresignOperation.PutObject, client.LastRequest?.Operation);
        Assert.Null(client.LastRequest?.PreferredAccessMode);
        Assert.Equal("text/plain", client.LastRequest?.ContentType);
    }

    [Fact]
    public async Task PresignPutObjectAsync_WithPreferredAccessMode_ForwardsPreferenceAndContentType()
    {
        var client = new CapturingIntegratedS3Client();

        await client.PresignPutObjectAsync(
            "docs",
            "guide.txt",
            expiresInSeconds: 300,
            preferredAccessMode: StorageAccessMode.Direct,
            contentType: "text/plain");

        Assert.Equal(StoragePresignOperation.PutObject, client.LastRequest?.Operation);
        Assert.Equal(StorageAccessMode.Direct, client.LastRequest?.PreferredAccessMode);
        Assert.Equal("text/plain", client.LastRequest?.ContentType);
    }

    [Fact]
    public async Task PresignPutObjectAsync_WithChecksum_ForwardsChecksumAlgorithmAndValue()
    {
        var client = new CapturingIntegratedS3Client();

        await client.PresignPutObjectAsync(
            "docs",
            "guide.txt",
            expiresInSeconds: 300,
            checksumAlgorithm: IntegratedS3TransferChecksumAlgorithm.Sha256,
            checksumValue: "abc123==",
            contentType: "text/plain");

        Assert.Equal(StoragePresignOperation.PutObject, client.LastRequest?.Operation);
        Assert.Equal("text/plain", client.LastRequest?.ContentType);
        Assert.Equal("sha256", client.LastRequest?.ChecksumAlgorithm);
        Assert.NotNull(client.LastRequest?.Checksums);
        Assert.Equal("abc123==", client.LastRequest!.Checksums!["sha256"]);
    }

    [Fact]
    public async Task PresignPutObjectAsync_WithPreferredAccessModeAndChecksum_ForwardsEverything()
    {
        var client = new CapturingIntegratedS3Client();

        await client.PresignPutObjectAsync(
            "docs",
            "guide.txt",
            expiresInSeconds: 300,
            preferredAccessMode: StorageAccessMode.Proxy,
            checksumAlgorithm: IntegratedS3TransferChecksumAlgorithm.Crc32C,
            checksumValue: "crc32c-base64==",
            contentType: "application/octet-stream");

        Assert.Equal(StorageAccessMode.Proxy, client.LastRequest?.PreferredAccessMode);
        Assert.Equal("crc32c", client.LastRequest?.ChecksumAlgorithm);
        Assert.Equal("crc32c-base64==", client.LastRequest!.Checksums!["crc32c"]);
        Assert.Equal("application/octet-stream", client.LastRequest?.ContentType);
    }

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
    }
}
