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

    [Fact]
    public async Task PresignDeleteObjectAsync_WithoutPreferredAccessMode_LeavesPreferenceNullAndForwardsVersion()
    {
        var client = new CapturingIntegratedS3Client();

        await client.PresignDeleteObjectAsync(
            "docs",
            "guide.txt",
            expiresInSeconds: 300,
            versionId: "v-123");

        Assert.Equal(StoragePresignOperation.DeleteObject, client.LastRequest?.Operation);
        Assert.Null(client.LastRequest?.PreferredAccessMode);
        Assert.Equal("v-123", client.LastRequest?.VersionId);
    }

    [Fact]
    public async Task PresignDeleteObjectAsync_WithPreferredAccessMode_ForwardsPreference()
    {
        var client = new CapturingIntegratedS3Client();

        await client.PresignDeleteObjectAsync(
            "docs",
            "guide.txt",
            expiresInSeconds: 300,
            preferredAccessMode: StorageAccessMode.Direct);

        Assert.Equal(StoragePresignOperation.DeleteObject, client.LastRequest?.Operation);
        Assert.Equal(StorageAccessMode.Direct, client.LastRequest?.PreferredAccessMode);
    }

    [Fact]
    public async Task PresignHeadObjectAsync_WithoutPreferredAccessMode_LeavesPreferenceNullAndForwardsVersion()
    {
        var client = new CapturingIntegratedS3Client();

        await client.PresignHeadObjectAsync(
            "docs",
            "guide.txt",
            expiresInSeconds: 300,
            versionId: "v-456");

        Assert.Equal(StoragePresignOperation.HeadObject, client.LastRequest?.Operation);
        Assert.Null(client.LastRequest?.PreferredAccessMode);
        Assert.Equal("v-456", client.LastRequest?.VersionId);
    }

    [Fact]
    public async Task PresignHeadObjectAsync_WithPreferredAccessMode_ForwardsPreference()
    {
        var client = new CapturingIntegratedS3Client();

        await client.PresignHeadObjectAsync(
            "docs",
            "guide.txt",
            expiresInSeconds: 300,
            preferredAccessMode: StorageAccessMode.Delegated);

        Assert.Equal(StoragePresignOperation.HeadObject, client.LastRequest?.Operation);
        Assert.Equal(StorageAccessMode.Delegated, client.LastRequest?.PreferredAccessMode);
    }

    [Fact]
    public async Task PresignUploadPartAsync_ForwardsUploadIdAndPartNumber()
    {
        var client = new CapturingIntegratedS3Client();

        await client.PresignUploadPartAsync(
            "docs",
            "guide.txt",
            uploadId: "upload-123",
            partNumber: 4,
            expiresInSeconds: 300);

        Assert.Equal(StoragePresignOperation.UploadPart, client.LastRequest?.Operation);
        Assert.Null(client.LastRequest?.PreferredAccessMode);
        Assert.Equal("upload-123", client.LastRequest?.UploadId);
        Assert.Equal(4, client.LastRequest?.PartNumber);
    }

    [Fact]
    public async Task PresignUploadPartAsync_WithPreferredAccessMode_ForwardsEverything()
    {
        var client = new CapturingIntegratedS3Client();

        await client.PresignUploadPartAsync(
            "docs",
            "guide.txt",
            uploadId: "upload-123",
            partNumber: 5,
            expiresInSeconds: 300,
            preferredAccessMode: StorageAccessMode.Direct);

        Assert.Equal(StoragePresignOperation.UploadPart, client.LastRequest?.Operation);
        Assert.Equal(StorageAccessMode.Direct, client.LastRequest?.PreferredAccessMode);
        Assert.Equal("upload-123", client.LastRequest?.UploadId);
        Assert.Equal(5, client.LastRequest?.PartNumber);
    }

    [Fact]
    public async Task PresignUploadPartAsync_WithBlankUploadId_Throws()
    {
        var client = new CapturingIntegratedS3Client();

        await Assert.ThrowsAsync<ArgumentException>(() => client.PresignUploadPartAsync(
            "docs",
            "guide.txt",
            uploadId: " ",
            partNumber: 1,
            expiresInSeconds: 300).AsTask());
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
