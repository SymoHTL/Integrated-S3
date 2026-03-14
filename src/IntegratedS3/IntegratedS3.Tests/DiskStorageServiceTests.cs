using System.Text;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Testing;
using IntegratedS3.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using static IntegratedS3.Tests.ChecksumTestAlgorithms;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class DiskStorageServiceTests
{
    [Fact]
    public async Task DiskStorage_RoundTripsBucketAndObjectContent()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        var createBucket = await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "photos"
        });

        Assert.True(createBucket.IsSuccess);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("hello integrated s3"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "photos",
            Key = "2026/launch.txt",
            Content = uploadStream,
            ContentType = "text/plain",
            Metadata = new Dictionary<string, string>
            {
                ["author"] = "copilot"
            }
        });

        Assert.True(putResult.IsSuccess);
        Assert.False(string.IsNullOrWhiteSpace(putResult.Value!.VersionId));
        Assert.Equal("text/plain", putResult.Value!.ContentType);
        Assert.Equal("copilot", putResult.Value.Metadata!["author"]);
        Assert.Equal(ComputeSha256Base64("hello integrated s3"), putResult.Value.Checksums!["sha256"]);
        Assert.Equal(ChecksumTestAlgorithms.ComputeCrc32cBase64("hello integrated s3"), putResult.Value.Checksums["crc32c"]);

        var objects = await storageService.ListObjectsAsync(new ListObjectsRequest
        {
            BucketName = "photos"
        }).ToArrayAsync();

        Assert.Single(objects);
        Assert.Equal("2026/launch.txt", objects[0].Key);

        var getResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "photos",
            Key = "2026/launch.txt"
        });

        Assert.True(getResult.IsSuccess);
        await using (var response = getResult.Value!) {
            using var reader = new StreamReader(response.Content, Encoding.UTF8, leaveOpen: false);
            var content = await reader.ReadToEndAsync();

            Assert.Equal("hello integrated s3", content);
            Assert.Equal(putResult.Value.VersionId, response.Object.VersionId);
            Assert.Equal("copilot", response.Object.Metadata!["author"]);
            Assert.Equal(ComputeSha256Base64("hello integrated s3"), response.Object.Checksums!["sha256"]);
            Assert.Equal(ChecksumTestAlgorithms.ComputeCrc32cBase64("hello integrated s3"), response.Object.Checksums["crc32c"]);
        }

        var deleteObject = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "photos",
            Key = "2026/launch.txt"
        });

        Assert.True(deleteObject.IsSuccess);

        var deleteBucket = await storageService.DeleteBucketAsync(new DeleteBucketRequest
        {
            BucketName = "photos"
        });

        Assert.True(deleteBucket.IsSuccess);
    }

    [Fact]
    public async Task DeleteBucketAsync_ReturnsBucketNotEmpty_WhenBucketContainsObjects()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "non-empty-delete"
        })).IsSuccess);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("still here"));
        Assert.True((await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "non-empty-delete",
            Key = "docs/hello.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        })).IsSuccess);

        var deleteBucket = await storageService.DeleteBucketAsync(new DeleteBucketRequest
        {
            BucketName = "non-empty-delete"
        });

        Assert.False(deleteBucket.IsSuccess);
        Assert.Equal(StorageErrorCode.BucketNotEmpty, deleteBucket.Error!.Code);
        Assert.Equal(409, deleteBucket.Error.SuggestedHttpStatusCode);
        Assert.Contains("empty", deleteBucket.Error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task DiskStorage_ValidatesRequestedChecksumsOnPut()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "checksums"
        })).IsSuccess);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("checksum payload"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "checksums",
            Key = "docs/checksum.txt",
            Content = uploadStream,
            ContentType = "text/plain",
            Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["sha256"] = "invalid-checksum"
            }
        });

        Assert.False(putResult.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.InvalidChecksum, putResult.Error!.Code);

        var headResult = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "checksums",
            Key = "docs/checksum.txt"
        });

        Assert.False(headResult.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.ObjectNotFound, headResult.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_PutObject_RejectsServerSideEncryptionRequests()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "encrypted-put"
        })).IsSuccess);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("encrypted payload"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "encrypted-put",
            Key = "docs/object.txt",
            Content = uploadStream,
            ServerSideEncryption = CreateServerSideEncryptionSettings()
        });

        Assert.False(putResult.IsSuccess);
        AssertUnsupportedServerSideEncryption(putResult.Error, "encrypted-put", "docs/object.txt");

        var headResult = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "encrypted-put",
            Key = "docs/object.txt"
        });

        Assert.False(headResult.IsSuccess);
        Assert.Equal(StorageErrorCode.ObjectNotFound, headResult.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_PutObject_WithSha1Checksum_RoundTripsAndRejectsMismatch()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "sha1-checksums"
        })).IsSuccess);

        const string payload = "sha1 checksum payload";
        var checksum = ComputeSha1Base64(payload);

        await using var validUploadStream = new MemoryStream(Encoding.UTF8.GetBytes(payload));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "sha1-checksums",
            Key = "docs/sha1.txt",
            Content = validUploadStream,
            ContentType = "text/plain",
            Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["sha1"] = checksum
            }
        });

        Assert.True(putResult.IsSuccess);
        Assert.Equal(checksum, putResult.Value!.Checksums!["sha1"]);

        var headResult = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "sha1-checksums",
            Key = "docs/sha1.txt"
        });

        Assert.True(headResult.IsSuccess);
        Assert.Equal(checksum, headResult.Value!.Checksums!["sha1"]);

        var getResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "sha1-checksums",
            Key = "docs/sha1.txt"
        });

        Assert.True(getResult.IsSuccess);
        await using (var response = getResult.Value!) {
            using var reader = new StreamReader(response.Content, Encoding.UTF8, leaveOpen: false);
            Assert.Equal(payload, await reader.ReadToEndAsync());
            Assert.Equal(checksum, response.Object.Checksums!["sha1"]);
        }

        await using var invalidUploadStream = new MemoryStream(Encoding.UTF8.GetBytes("sha1 checksum mismatch"));
        var invalidPutResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "sha1-checksums",
            Key = "docs/invalid-sha1.txt",
            Content = invalidUploadStream,
            ContentType = "text/plain",
            Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["sha1"] = "invalid-checksum"
            }
        });

        Assert.False(invalidPutResult.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.InvalidChecksum, invalidPutResult.Error!.Code);

        var invalidHeadResult = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "sha1-checksums",
            Key = "docs/invalid-sha1.txt"
        });

        Assert.False(invalidHeadResult.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.ObjectNotFound, invalidHeadResult.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_PutObject_WithCrc32cChecksum_RoundTripsAndRejectsMismatch()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "crc32c-checksums"
        })).IsSuccess);

        const string payload = "crc32c checksum payload";
        var checksum = ChecksumTestAlgorithms.ComputeCrc32cBase64(payload);

        await using var validUploadStream = new MemoryStream(Encoding.UTF8.GetBytes(payload));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "crc32c-checksums",
            Key = "docs/crc32c.txt",
            Content = validUploadStream,
            ContentType = "text/plain",
            Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["crc32c"] = checksum
            }
        });

        Assert.True(putResult.IsSuccess);
        Assert.Equal(checksum, putResult.Value!.Checksums!["crc32c"]);

        var headResult = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "crc32c-checksums",
            Key = "docs/crc32c.txt"
        });

        Assert.True(headResult.IsSuccess);
        Assert.Equal(checksum, headResult.Value!.Checksums!["crc32c"]);

        var getResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "crc32c-checksums",
            Key = "docs/crc32c.txt"
        });

        Assert.True(getResult.IsSuccess);
        await using (var response = getResult.Value!) {
            using var reader = new StreamReader(response.Content, Encoding.UTF8, leaveOpen: false);
            Assert.Equal(payload, await reader.ReadToEndAsync());
            Assert.Equal(checksum, response.Object.Checksums!["crc32c"]);
        }

        await using var invalidUploadStream = new MemoryStream(Encoding.UTF8.GetBytes("crc32c checksum mismatch"));
        var invalidPutResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "crc32c-checksums",
            Key = "docs/invalid-crc32c.txt",
            Content = invalidUploadStream,
            ContentType = "text/plain",
            Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["crc32c"] = "invalid-checksum"
            }
        });

        Assert.False(invalidPutResult.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.InvalidChecksum, invalidPutResult.Error!.Code);

        var invalidHeadResult = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "crc32c-checksums",
            Key = "docs/invalid-crc32c.txt"
        });

        Assert.False(invalidHeadResult.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.ObjectNotFound, invalidHeadResult.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_CurrentVersionRequests_RespectPersistedVersionId()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "versions"
        })).IsSuccess);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("versioned payload"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "versions",
            Key = "docs/versioned.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        Assert.True(putResult.IsSuccess);
        var versionId = Assert.IsType<string>(putResult.Value!.VersionId);
        Assert.False(string.IsNullOrWhiteSpace(versionId));

        var putTagsResult = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "versions",
            Key = "docs/versioned.txt",
            VersionId = versionId,
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["owner"] = "copilot"
            }
        });

        Assert.True(putTagsResult.IsSuccess);
        Assert.Equal(versionId, putTagsResult.Value!.VersionId);

        var getTagsResult = await storageService.GetObjectTagsAsync(new GetObjectTagsRequest
        {
            BucketName = "versions",
            Key = "docs/versioned.txt",
            VersionId = versionId
        });

        Assert.True(getTagsResult.IsSuccess);
        Assert.Equal(versionId, getTagsResult.Value!.VersionId);
        Assert.Equal("copilot", getTagsResult.Value.Tags["owner"]);

        var getResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "versions",
            Key = "docs/versioned.txt",
            VersionId = versionId
        });

        Assert.True(getResult.IsSuccess);
        await using (var response = getResult.Value!) {
            Assert.Equal(versionId, response.Object.VersionId);
            Assert.Equal(ComputeSha256Base64("versioned payload"), response.Object.Checksums!["sha256"]);
        }

        var wrongVersionHead = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "versions",
            Key = "docs/versioned.txt",
            VersionId = "wrong-version"
        });

        Assert.False(wrongVersionHead.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.ObjectNotFound, wrongVersionHead.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_BucketVersioning_CanBeEnabledAndSuspended()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        var createResult = await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "bucket-versioning",
            EnableVersioning = true
        });

        Assert.True(createResult.IsSuccess);
        Assert.True(createResult.Value!.VersioningEnabled);

        var initialVersioning = await storageService.GetBucketVersioningAsync("bucket-versioning");
        Assert.True(initialVersioning.IsSuccess);
        Assert.Equal(BucketVersioningStatus.Enabled, initialVersioning.Value!.Status);

        var suspended = await storageService.PutBucketVersioningAsync(new PutBucketVersioningRequest
        {
            BucketName = "bucket-versioning",
            Status = BucketVersioningStatus.Suspended
        });

        Assert.True(suspended.IsSuccess);
        Assert.Equal(BucketVersioningStatus.Suspended, suspended.Value!.Status);

        var headBucket = await storageService.HeadBucketAsync("bucket-versioning");
        Assert.True(headBucket.IsSuccess);
        Assert.False(headBucket.Value!.VersioningEnabled);

        var listBuckets = await storageService.ListBucketsAsync().ToArrayAsync();
        Assert.Contains(listBuckets, static bucket => bucket.Name == "bucket-versioning" && !bucket.VersioningEnabled);
    }

    [Fact]
    public async Task DiskStorage_BucketLocation_ReturnsDefaultEmptyConstraint()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "bucket-location"
        })).IsSuccess);

        var location = await storageService.GetBucketLocationAsync("bucket-location");

        Assert.True(location.IsSuccess);
        Assert.Equal("bucket-location", location.Value!.BucketName);
        Assert.Null(location.Value.LocationConstraint);
    }

    [Fact]
    public async Task DiskStorage_BucketCors_RoundTripsAndPreservesVersioningMetadata()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        var createResult = await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "bucket-cors",
            EnableVersioning = true
        });

        Assert.True(createResult.IsSuccess);

        var putCors = await storageService.PutBucketCorsAsync(new PutBucketCorsRequest
        {
            BucketName = "bucket-cors",
            Rules =
            [
                new BucketCorsRule
                {
                    Id = "browser-rule",
                    AllowedOrigins = ["https://app.example"],
                    AllowedMethods = [BucketCorsMethod.Get, BucketCorsMethod.Put],
                    AllowedHeaders = ["authorization", "x-amz-*"],
                    ExposeHeaders = ["etag"],
                    MaxAgeSeconds = 900
                }
            ]
        });

        Assert.True(putCors.IsSuccess);
        var initialRule = Assert.Single(putCors.Value!.Rules);
        Assert.Equal("browser-rule", initialRule.Id);

        var suspended = await storageService.PutBucketVersioningAsync(new PutBucketVersioningRequest
        {
            BucketName = "bucket-cors",
            Status = BucketVersioningStatus.Suspended
        });

        Assert.True(suspended.IsSuccess);

        var getCors = await storageService.GetBucketCorsAsync("bucket-cors");
        Assert.True(getCors.IsSuccess);
        var storedRule = Assert.Single(getCors.Value!.Rules);
        Assert.Equal([BucketCorsMethod.Get, BucketCorsMethod.Put], storedRule.AllowedMethods);
        Assert.Equal(["authorization", "x-amz-*"], storedRule.AllowedHeaders);

        var versioning = await storageService.GetBucketVersioningAsync("bucket-cors");
        Assert.True(versioning.IsSuccess);
        Assert.Equal(BucketVersioningStatus.Suspended, versioning.Value!.Status);

        var deleteCors = await storageService.DeleteBucketCorsAsync(new DeleteBucketCorsRequest
        {
            BucketName = "bucket-cors"
        });

        Assert.True(deleteCors.IsSuccess);

        var missingCors = await storageService.GetBucketCorsAsync("bucket-cors");
        Assert.False(missingCors.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.CorsConfigurationNotFound, missingCors.Error!.Code);

        var preservedVersioning = await storageService.GetBucketVersioningAsync("bucket-cors");
        Assert.True(preservedVersioning.IsSuccess);
        Assert.Equal(BucketVersioningStatus.Suspended, preservedVersioning.Value!.Status);
    }

    [Fact]
    public async Task DiskStorage_VersionedOverwrite_PreservesHistoricalVersionAccess()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "version-history",
            EnableVersioning = true
        })).IsSuccess);

        await using var v1Stream = new MemoryStream(Encoding.UTF8.GetBytes("version one"));
        var v1Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "version-history",
            Key = "docs/history.txt",
            Content = v1Stream,
            ContentType = "text/plain"
        });

        await using var v2Stream = new MemoryStream(Encoding.UTF8.GetBytes("version two"));
        var v2Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "version-history",
            Key = "docs/history.txt",
            Content = v2Stream,
            ContentType = "text/plain"
        });

        Assert.True(v1Put.IsSuccess);
        Assert.True(v2Put.IsSuccess);
        Assert.NotEqual(v1Put.Value!.VersionId, v2Put.Value!.VersionId);

        var currentGet = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "version-history",
            Key = "docs/history.txt"
        });

        Assert.True(currentGet.IsSuccess);
        await using (var currentResponse = currentGet.Value!) {
            using var reader = new StreamReader(currentResponse.Content, Encoding.UTF8);
            Assert.Equal("version two", await reader.ReadToEndAsync());
            Assert.Equal(v2Put.Value.VersionId, currentResponse.Object.VersionId);
        }

        var historicalGet = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "version-history",
            Key = "docs/history.txt",
            VersionId = v1Put.Value.VersionId
        });

        Assert.True(historicalGet.IsSuccess);
        await using (var historicalResponse = historicalGet.Value!) {
            using var reader = new StreamReader(historicalResponse.Content, Encoding.UTF8);
            Assert.Equal("version one", await reader.ReadToEndAsync());
            Assert.Equal(v1Put.Value.VersionId, historicalResponse.Object.VersionId);
        }

        var historicalTags = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "version-history",
            Key = "docs/history.txt",
            VersionId = v1Put.Value.VersionId,
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["generation"] = "one"
            }
        });

        Assert.True(historicalTags.IsSuccess);

        var rereadHistoricalTags = await storageService.GetObjectTagsAsync(new GetObjectTagsRequest
        {
            BucketName = "version-history",
            Key = "docs/history.txt",
            VersionId = v1Put.Value.VersionId
        });

        Assert.True(rereadHistoricalTags.IsSuccess);
        Assert.Equal("one", rereadHistoricalTags.Value!.Tags["generation"]);
    }

    [Fact]
    public async Task DiskStorage_DeleteSpecificHistoricalVersion_RemovesOnlyThatVersion()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "version-delete",
            EnableVersioning = true
        })).IsSuccess);

        await using var v1Stream = new MemoryStream(Encoding.UTF8.GetBytes("delete me first"));
        var v1Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "version-delete",
            Key = "docs/delete.txt",
            Content = v1Stream,
            ContentType = "text/plain"
        });

        await using var v2Stream = new MemoryStream(Encoding.UTF8.GetBytes("keep me current"));
        var v2Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "version-delete",
            Key = "docs/delete.txt",
            Content = v2Stream,
            ContentType = "text/plain"
        });

        Assert.True(v1Put.IsSuccess);
        Assert.True(v2Put.IsSuccess);

        var deleteV1 = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "version-delete",
            Key = "docs/delete.txt",
            VersionId = v1Put.Value!.VersionId
        });

        Assert.True(deleteV1.IsSuccess);

        var missingV1 = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "version-delete",
            Key = "docs/delete.txt",
            VersionId = v1Put.Value.VersionId
        });

        Assert.False(missingV1.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.ObjectNotFound, missingV1.Error!.Code);

        var currentGet = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "version-delete",
            Key = "docs/delete.txt"
        });

        Assert.True(currentGet.IsSuccess);
        await using (var currentResponse = currentGet.Value!) {
            using var reader = new StreamReader(currentResponse.Content, Encoding.UTF8);
            Assert.Equal("keep me current", await reader.ReadToEndAsync());
            Assert.Equal(v2Put.Value!.VersionId, currentResponse.Object.VersionId);
        }
    }

    [Fact]
    public async Task DiskStorage_DeleteMissingObject_IsIdempotentAndCreatesVersionedDeleteMarkerWhenNeeded()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "delete-missing"
        })).IsSuccess);

        var deleteMissing = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "delete-missing",
            Key = "docs/missing.txt"
        });

        Assert.True(deleteMissing.IsSuccess);
        Assert.False(deleteMissing.Value!.IsDeleteMarker);
        Assert.Null(deleteMissing.Value.VersionId);

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "delete-missing-versioned",
            EnableVersioning = true
        })).IsSuccess);

        var deleteVersionedMissing = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "delete-missing-versioned",
            Key = "docs/missing.txt"
        });

        Assert.True(deleteVersionedMissing.IsSuccess);
        Assert.True(deleteVersionedMissing.Value!.IsDeleteMarker);
        var deleteMarkerVersionId = Assert.IsType<string>(deleteVersionedMissing.Value.VersionId);

        var currentGet = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "delete-missing-versioned",
            Key = "docs/missing.txt"
        });

        Assert.False(currentGet.IsSuccess);
        Assert.Equal(StorageErrorCode.ObjectNotFound, currentGet.Error!.Code);
        Assert.True(currentGet.Error.IsDeleteMarker);
        Assert.Equal(deleteMarkerVersionId, currentGet.Error.VersionId);

        var versions = await storageService.ListObjectVersionsAsync(new ListObjectVersionsRequest
        {
            BucketName = "delete-missing-versioned"
        }).ToArrayAsync();

        var deleteMarker = Assert.Single(versions);
        Assert.Equal("docs/missing.txt", deleteMarker.Key);
        Assert.Equal(deleteMarkerVersionId, deleteMarker.VersionId);
        Assert.True(deleteMarker.IsDeleteMarker);
        Assert.True(deleteMarker.IsLatest);
    }

    [Fact]
    public async Task DiskStorage_VersionedDelete_CreatesDeleteMarkerAndListsVersions()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "version-delete-marker",
            EnableVersioning = true
        })).IsSuccess);

        await using var v1Stream = new MemoryStream(Encoding.UTF8.GetBytes("version one"));
        var v1Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "version-delete-marker",
            Key = "docs/history.txt",
            Content = v1Stream,
            ContentType = "text/plain"
        });

        await using var v2Stream = new MemoryStream(Encoding.UTF8.GetBytes("version two"));
        var v2Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "version-delete-marker",
            Key = "docs/history.txt",
            Content = v2Stream,
            ContentType = "text/plain"
        });

        var deleteCurrent = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "version-delete-marker",
            Key = "docs/history.txt"
        });

        Assert.True(deleteCurrent.IsSuccess);
        Assert.True(deleteCurrent.Value!.IsDeleteMarker);
        Assert.False(string.IsNullOrWhiteSpace(deleteCurrent.Value.VersionId));

        var currentGet = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "version-delete-marker",
            Key = "docs/history.txt"
        });

        Assert.False(currentGet.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.ObjectNotFound, currentGet.Error!.Code);

        var versions = await storageService.ListObjectVersionsAsync(new ListObjectVersionsRequest
        {
            BucketName = "version-delete-marker",
            Prefix = "docs/history.txt"
        }).ToArrayAsync();

        Assert.Equal(3, versions.Length);
        Assert.Equal("docs/history.txt", versions[0].Key);
        Assert.True(versions[0].IsDeleteMarker);
        Assert.True(versions[0].IsLatest);
        Assert.Equal(deleteCurrent.Value.VersionId, versions[0].VersionId);
        Assert.Contains(versions, static version => !version.IsDeleteMarker && version.VersionId is not null && version.IsLatest is false);
        Assert.Contains(versions, version => version.VersionId == v2Put.Value!.VersionId && !version.IsDeleteMarker);
        Assert.Contains(versions, version => version.VersionId == v1Put.Value!.VersionId && !version.IsDeleteMarker);
    }

    [Fact]
    public async Task DiskStorage_DeleteMarkerGetAndHeadRequestsPreserveDeleteMarkerFidelity()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "version-delete-marker-fidelity",
            EnableVersioning = true
        })).IsSuccess);

        await using var putStream = new MemoryStream(Encoding.UTF8.GetBytes("current version"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "version-delete-marker-fidelity",
            Key = "docs/history.txt",
            Content = putStream,
            ContentType = "text/plain"
        });
        Assert.True(putResult.IsSuccess);

        var deleteCurrent = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "version-delete-marker-fidelity",
            Key = "docs/history.txt"
        });

        Assert.True(deleteCurrent.IsSuccess);
        var deleteMarkerVersionId = Assert.IsType<string>(deleteCurrent.Value!.VersionId);

        var deleteMarkerVersion = Assert.Single(await storageService.ListObjectVersionsAsync(new ListObjectVersionsRequest
        {
            BucketName = "version-delete-marker-fidelity",
            Prefix = "docs/history.txt"
        }).Where(static version => version.IsDeleteMarker).ToArrayAsync());

        var currentGet = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "version-delete-marker-fidelity",
            Key = "docs/history.txt"
        });

        Assert.False(currentGet.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.ObjectNotFound, currentGet.Error!.Code);
        Assert.True(currentGet.Error.IsDeleteMarker);
        Assert.Equal(deleteMarkerVersionId, currentGet.Error.VersionId);
        Assert.Null(currentGet.Error.LastModifiedUtc);

        var currentHead = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "version-delete-marker-fidelity",
            Key = "docs/history.txt"
        });

        Assert.False(currentHead.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.ObjectNotFound, currentHead.Error!.Code);
        Assert.True(currentHead.Error.IsDeleteMarker);
        Assert.Equal(deleteMarkerVersionId, currentHead.Error.VersionId);
        Assert.Null(currentHead.Error.LastModifiedUtc);

        var explicitGet = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "version-delete-marker-fidelity",
            Key = "docs/history.txt",
            VersionId = deleteMarkerVersionId
        });

        Assert.False(explicitGet.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.MethodNotAllowed, explicitGet.Error!.Code);
        Assert.True(explicitGet.Error.IsDeleteMarker);
        Assert.Equal(deleteMarkerVersionId, explicitGet.Error.VersionId);
        Assert.Equal(deleteMarkerVersion.LastModifiedUtc, explicitGet.Error.LastModifiedUtc);

        var explicitHead = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "version-delete-marker-fidelity",
            Key = "docs/history.txt",
            VersionId = deleteMarkerVersionId
        });

        Assert.False(explicitHead.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.MethodNotAllowed, explicitHead.Error!.Code);
        Assert.True(explicitHead.Error.IsDeleteMarker);
        Assert.Equal(deleteMarkerVersionId, explicitHead.Error.VersionId);
        Assert.Equal(deleteMarkerVersion.LastModifiedUtc, explicitHead.Error.LastModifiedUtc);
        Assert.Equal(deleteMarkerVersionId, deleteMarkerVersion.VersionId);
    }

    [Fact]
    public async Task DiskStorage_DeletingCurrentDeleteMarkerByVersionId_RestoresPreviousVersion()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "version-delete-marker-restore",
            EnableVersioning = true
        })).IsSuccess);

        await using var v1Stream = new MemoryStream(Encoding.UTF8.GetBytes("version one"));
        var v1Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "version-delete-marker-restore",
            Key = "docs/history.txt",
            Content = v1Stream,
            ContentType = "text/plain"
        });

        await using var v2Stream = new MemoryStream(Encoding.UTF8.GetBytes("version two"));
        var v2Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "version-delete-marker-restore",
            Key = "docs/history.txt",
            Content = v2Stream,
            ContentType = "text/plain"
        });

        var deleteCurrent = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "version-delete-marker-restore",
            Key = "docs/history.txt"
        });

        Assert.True(deleteCurrent.IsSuccess);
        Assert.True(deleteCurrent.Value!.IsDeleteMarker);

        var deleteMarkerVersion = deleteCurrent.Value.VersionId;
        var deleteMarkerDelete = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "version-delete-marker-restore",
            Key = "docs/history.txt",
            VersionId = deleteMarkerVersion
        });

        Assert.True(deleteMarkerDelete.IsSuccess);
        Assert.True(deleteMarkerDelete.Value!.IsDeleteMarker);
        Assert.NotNull(deleteMarkerDelete.Value.CurrentObject);
        Assert.Equal(v2Put.Value!.VersionId, deleteMarkerDelete.Value.CurrentObject!.VersionId);

        var currentGet = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "version-delete-marker-restore",
            Key = "docs/history.txt"
        });

        Assert.True(currentGet.IsSuccess);
        await using var currentResponse = currentGet.Value!;
        using var reader = new StreamReader(currentResponse.Content, Encoding.UTF8);
        Assert.Equal("version two", await reader.ReadToEndAsync());
        Assert.Equal(v2Put.Value.VersionId, currentResponse.Object.VersionId);
        Assert.NotEqual(v1Put.Value!.VersionId, currentResponse.Object.VersionId);
    }

    [Fact]
    public async Task DiskStorage_RejectsPathTraversalKeys()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "safe" });

        await using var uploadStream = new MemoryStream([1, 2, 3]);

        await Assert.ThrowsAsync<ArgumentException>(async () => {
            await storageService.PutObjectAsync(new PutObjectRequest
            {
                BucketName = "safe",
                Key = "../escape.txt",
                Content = uploadStream
            });
        });
    }

    [Fact]
    public async Task DiskStorage_SupportsRangeRequests()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "docs" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("hello integrated s3"));
        await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "docs",
            Key = "range.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        var getResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "docs",
            Key = "range.txt",
            Range = new ObjectRange
            {
                Start = 6,
                End = 15
            }
        });

        Assert.True(getResult.IsSuccess);
        await using var response = getResult.Value!;
        using var reader = new StreamReader(response.Content, Encoding.UTF8, leaveOpen: false);
        var content = await reader.ReadToEndAsync();

        Assert.Equal("integrated", content);
        Assert.NotNull(response.Range);
        Assert.Equal(6, response.Range!.Start);
        Assert.Equal(15, response.Range.End);
        Assert.Equal(10, response.Object.ContentLength);
        Assert.Equal(19, response.TotalContentLength);
    }

    [Fact]
    public async Task DiskStorage_HonorsConditionalRequests()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "docs" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("hello integrated s3"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "docs",
            Key = "conditions.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        var currentETag = putResult.Value!.ETag;

        var ifMatchResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "docs",
            Key = "conditions.txt",
            IfMatchETag = $"\"{currentETag}\""
        });

        Assert.True(ifMatchResult.IsSuccess);
        await using (var response = ifMatchResult.Value!) {
            Assert.False(response.IsNotModified);
        }

        var ifNoneMatchResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "docs",
            Key = "conditions.txt",
            IfNoneMatchETag = $"\"{currentETag}\""
        });

        Assert.True(ifNoneMatchResult.IsSuccess);
        await using (var response = ifNoneMatchResult.Value!) {
            Assert.True(response.IsNotModified);
        }

        var failedIfMatch = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "docs",
            Key = "conditions.txt",
            IfMatchETag = "\"different\""
        });

        Assert.False(failedIfMatch.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.PreconditionFailed, failedIfMatch.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_PaginatesObjectsUsingContinuationToken()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "docs" });

        foreach (var key in new[] { "a.txt", "b.txt", "c.txt" }) {
            await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes(key));
            var putResult = await storageService.PutObjectAsync(new PutObjectRequest
            {
                BucketName = "docs",
                Key = key,
                Content = uploadStream,
                ContentType = "text/plain"
            });
            Assert.True(putResult.IsSuccess);
        }

        var firstPage = await storageService.ListObjectsAsync(new ListObjectsRequest
        {
            BucketName = "docs",
            PageSize = 2
        }).ToArrayAsync();

        Assert.Equal(["a.txt", "b.txt"], firstPage.Select(static item => item.Key).ToArray());

        var secondPage = await storageService.ListObjectsAsync(new ListObjectsRequest
        {
            BucketName = "docs",
            ContinuationToken = firstPage[^1].Key,
            PageSize = 2
        }).ToArrayAsync();

        Assert.Equal(["c.txt"], secondPage.Select(static item => item.Key).ToArray());
    }

    [Fact]
    public async Task DiskStorage_HonorsDateBasedConditionalGetRequests()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "docs" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("hello integrated s3"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "docs",
            Key = "dates.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        var lastModifiedUtc = putResult.Value!.LastModifiedUtc;

        var notModifiedResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "docs",
            Key = "dates.txt",
            IfModifiedSinceUtc = lastModifiedUtc.AddMinutes(5)
        });

        Assert.True(notModifiedResult.IsSuccess);
        await using (var response = notModifiedResult.Value!) {
            Assert.True(response.IsNotModified);
        }

        var failedPrecondition = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "docs",
            Key = "dates.txt",
            IfUnmodifiedSinceUtc = lastModifiedUtc.AddMinutes(-5)
        });

        Assert.False(failedPrecondition.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.PreconditionFailed, failedPrecondition.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_GetObject_IfMatchTakesPrecedenceOverIfUnmodifiedSince()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "docs" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("hello integrated s3"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "docs",
            Key = "precedence.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        Assert.True(putResult.IsSuccess);
        var currentETag = $"\"{putResult.Value!.ETag}\"";
        var lastModifiedUtc = putResult.Value.LastModifiedUtc;

        var result = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "docs",
            Key = "precedence.txt",
            IfMatchETag = currentETag,
            IfUnmodifiedSinceUtc = lastModifiedUtc.AddMinutes(-5)
        });

        Assert.True(result.IsSuccess);
        await using var response = result.Value!;
        Assert.False(response.IsNotModified);
        using var reader = new StreamReader(response.Content, Encoding.UTF8);
        Assert.Equal("hello integrated s3", await reader.ReadToEndAsync());

        var failedMixedConditions = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "docs",
            Key = "precedence.txt",
            IfMatchETag = "\"different\"",
            IfUnmodifiedSinceUtc = lastModifiedUtc.AddMinutes(5)
        });

        Assert.False(failedMixedConditions.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.PreconditionFailed, failedMixedConditions.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_GetAndHeadObject_RejectServerSideEncryptionRequests()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "encrypted-reads"
        })).IsSuccess);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("read me"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "encrypted-reads",
            Key = "docs/object.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        Assert.True(putResult.IsSuccess);

        var getResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "encrypted-reads",
            Key = "docs/object.txt",
            ServerSideEncryption = CreateServerSideEncryptionSettings()
        });

        Assert.False(getResult.IsSuccess);
        AssertUnsupportedServerSideEncryption(getResult.Error, "encrypted-reads", "docs/object.txt");

        var headResult = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "encrypted-reads",
            Key = "docs/object.txt",
            ServerSideEncryption = CreateServerSideEncryptionSettings()
        });

        Assert.False(headResult.IsSuccess);
        AssertUnsupportedServerSideEncryption(headResult.Error, "encrypted-reads", "docs/object.txt");

        var plainHeadResult = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "encrypted-reads",
            Key = "docs/object.txt"
        });

        Assert.True(plainHeadResult.IsSuccess);
        Assert.Equal(putResult.Value!.VersionId, plainHeadResult.Value!.VersionId);
    }

    [Fact]
    public async Task DiskStorage_CopiesObjectsAndPreservesMetadata()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "source" });
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "target" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("copy me"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "source",
            Key = "docs/source.txt",
            Content = uploadStream,
            ContentType = "text/plain",
            Metadata = new Dictionary<string, string>
            {
                ["origin"] = "tests"
            },
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["copied"] = "true"
            }
        });

        var copyResult = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "target",
            DestinationKey = "docs/copied.txt"
        });

        Assert.True(copyResult.IsSuccess);
        Assert.Equal("text/plain", copyResult.Value!.ContentType);
        Assert.Equal("tests", copyResult.Value.Metadata!["origin"]);
        Assert.Equal("true", copyResult.Value.Tags!["copied"]);
        Assert.Equal(ComputeSha256Base64("copy me"), copyResult.Value.Checksums!["sha256"]);

        var downloaded = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "target",
            Key = "docs/copied.txt"
        });

        Assert.True(downloaded.IsSuccess);
        await using var response = downloaded.Value!;
        using var reader = new StreamReader(response.Content, Encoding.UTF8);
        Assert.Equal("copy me", await reader.ReadToEndAsync());
        Assert.Equal("tests", response.Object.Metadata!["origin"]);
        Assert.Equal("true", response.Object.Tags!["copied"]);
        Assert.Equal(ComputeSha256Base64("copy me"), response.Object.Checksums!["sha256"]);
        Assert.NotEqual(putResult.Value!.BucketName, copyResult.Value.BucketName);
    }

    [Fact]
    public async Task DiskStorage_CopyObject_WithReplaceTaggingDirective_UsesReplacementTags()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "source" });
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "target" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("copy me"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "source",
            Key = "docs/source.txt",
            Content = uploadStream,
            ContentType = "text/plain",
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["environment"] = "source",
                ["owner"] = "original"
            }
        });

        Assert.True(putResult.IsSuccess);

        var copyResult = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "target",
            DestinationKey = "docs/copied.txt",
            TaggingDirective = ObjectTaggingDirective.Replace,
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["environment"] = "target",
                ["owner"] = "copilot"
            }
        });

        Assert.True(copyResult.IsSuccess);
        Assert.Equal("target", copyResult.Value!.Tags!["environment"]);
        Assert.Equal("copilot", copyResult.Value.Tags["owner"]);

        var getTagsResult = await storageService.GetObjectTagsAsync(new GetObjectTagsRequest
        {
            BucketName = "target",
            Key = "docs/copied.txt"
        });

        Assert.True(getTagsResult.IsSuccess);
        Assert.Equal("target", getTagsResult.Value!.Tags["environment"]);
        Assert.Equal("copilot", getTagsResult.Value.Tags["owner"]);
        Assert.DoesNotContain("original", getTagsResult.Value.Tags.Values);
    }

    [Fact]
    public async Task DiskStorage_CopyObject_RejectsServerSideEncryptionRequests()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "source" });
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "target" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("copy me"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "source",
            Key = "docs/source.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        Assert.True(putResult.IsSuccess);

        var sourceEncryptedCopy = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "target",
            DestinationKey = "docs/source-encrypted.txt",
            SourceServerSideEncryption = CreateServerSideEncryptionSettings()
        });

        Assert.False(sourceEncryptedCopy.IsSuccess);
        AssertUnsupportedServerSideEncryption(sourceEncryptedCopy.Error, "source", "docs/source.txt");

        var destinationEncryptedCopy = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "target",
            DestinationKey = "docs/destination-encrypted.txt",
            DestinationServerSideEncryption = CreateServerSideEncryptionSettings()
        });

        Assert.False(destinationEncryptedCopy.IsSuccess);
        AssertUnsupportedServerSideEncryption(destinationEncryptedCopy.Error, "target", "docs/destination-encrypted.txt");

        Assert.False((await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "target",
            Key = "docs/source-encrypted.txt"
        })).IsSuccess);
        Assert.False((await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "target",
            Key = "docs/destination-encrypted.txt"
        })).IsSuccess);
    }

    [Fact]
    public async Task DiskStorage_CopyObject_HonorsSourcePreconditions()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "source" });
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "target" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("copy me"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "source",
            Key = "docs/source.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        var failedCopy = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "target",
            DestinationKey = "docs/copied.txt",
            SourceIfMatchETag = "\"different\""
        });

        Assert.False(failedCopy.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.PreconditionFailed, failedCopy.Error!.Code);

        var failedIfNoneMatchCopy = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "target",
            DestinationKey = "docs/copied.txt",
            SourceIfNoneMatchETag = $"\"{putResult.Value!.ETag}\""
        });

        Assert.False(failedIfNoneMatchCopy.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.PreconditionFailed, failedIfNoneMatchCopy.Error!.Code);

        var failedIfModifiedSinceCopy = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "target",
            DestinationKey = "docs/copied.txt",
            SourceIfNoneMatchETag = "\"different\"",
            SourceIfModifiedSinceUtc = putResult.Value.LastModifiedUtc.AddMinutes(5)
        });

        Assert.False(failedIfModifiedSinceCopy.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.PreconditionFailed, failedIfModifiedSinceCopy.Error!.Code);
        Assert.False((await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "target",
            Key = "docs/copied.txt"
        })).IsSuccess);
    }

    [Fact]
    public async Task DiskStorage_CopyObject_SourceIfMatchTakesPrecedenceOverSourceIfUnmodifiedSince()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "source" });
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "target" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("copy me"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "source",
            Key = "docs/source.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        Assert.True(putResult.IsSuccess);
        var currentETag = $"\"{putResult.Value!.ETag}\"";
        var lastModifiedUtc = putResult.Value.LastModifiedUtc;

        var successfulCopy = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "target",
            DestinationKey = "docs/precedence-copy.txt",
            SourceIfMatchETag = currentETag,
            SourceIfUnmodifiedSinceUtc = lastModifiedUtc.AddMinutes(-5)
        });

        Assert.True(successfulCopy.IsSuccess);

        var copiedObject = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "target",
            Key = "docs/precedence-copy.txt"
        });

        Assert.True(copiedObject.IsSuccess);
        await using (var response = copiedObject.Value!) {
            using var reader = new StreamReader(response.Content, Encoding.UTF8);
            Assert.Equal("copy me", await reader.ReadToEndAsync());
        }

        var failedMixedConditions = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "target",
            DestinationKey = "docs/blocked-copy.txt",
            SourceIfMatchETag = "\"different\"",
            SourceIfUnmodifiedSinceUtc = lastModifiedUtc.AddMinutes(5)
        });

        Assert.False(failedMixedConditions.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.PreconditionFailed, failedMixedConditions.Error!.Code);
        Assert.False((await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "target",
            Key = "docs/blocked-copy.txt"
        })).IsSuccess);
    }

    [Fact]
    public async Task DiskStorage_CopyObject_SourceVersionIdCanTargetHistoricalVersionsAndRejectDeleteMarkers()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "source",
            EnableVersioning = true
        })).IsSuccess);
        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "target"
        })).IsSuccess);

        await using var v1Stream = new MemoryStream(Encoding.UTF8.GetBytes("version one"));
        var v1Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "source",
            Key = "docs/source.txt",
            Content = v1Stream,
            ContentType = "text/plain"
        });

        await using var v2Stream = new MemoryStream(Encoding.UTF8.GetBytes("version two"));
        var v2Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "source",
            Key = "docs/source.txt",
            Content = v2Stream,
            ContentType = "text/plain"
        });

        Assert.True(v1Put.IsSuccess);
        Assert.True(v2Put.IsSuccess);
        Assert.NotEqual(v1Put.Value!.VersionId, v2Put.Value!.VersionId);

        var deleteCurrent = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "source",
            Key = "docs/source.txt"
        });

        Assert.True(deleteCurrent.IsSuccess);
        Assert.True(deleteCurrent.Value!.IsDeleteMarker);

        var deleteMarkerVersionId = Assert.IsType<string>(deleteCurrent.Value!.VersionId);
        var deleteMarkerVersion = Assert.Single(await storageService.ListObjectVersionsAsync(new ListObjectVersionsRequest
        {
            BucketName = "source",
            Prefix = "docs/source.txt"
        }).Where(static version => version.IsDeleteMarker).ToArrayAsync());

        var historicalCopy = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            SourceVersionId = v1Put.Value.VersionId,
            DestinationBucketName = "target",
            DestinationKey = "docs/historical-copy.txt"
        });

        Assert.True(historicalCopy.IsSuccess);

        var historicalCopyGet = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "target",
            Key = "docs/historical-copy.txt"
        });

        Assert.True(historicalCopyGet.IsSuccess);
        await using (var response = historicalCopyGet.Value!) {
            using var reader = new StreamReader(response.Content, Encoding.UTF8);
            Assert.Equal("version one", await reader.ReadToEndAsync());
        }

        var currentCopy = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "target",
            DestinationKey = "docs/current-copy.txt"
        });

        Assert.False(currentCopy.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.ObjectNotFound, currentCopy.Error!.Code);
        Assert.True(currentCopy.Error.IsDeleteMarker);
        Assert.Equal(deleteMarkerVersionId, currentCopy.Error.VersionId);
        Assert.Null(currentCopy.Error.LastModifiedUtc);
        Assert.False((await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "target",
            Key = "docs/current-copy.txt"
        })).IsSuccess);

        var explicitCopy = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            SourceVersionId = deleteMarkerVersionId,
            DestinationBucketName = "target",
            DestinationKey = "docs/versioned-copy.txt"
        });

        Assert.False(explicitCopy.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.MethodNotAllowed, explicitCopy.Error!.Code);
        Assert.True(explicitCopy.Error.IsDeleteMarker);
        Assert.Equal(deleteMarkerVersionId, explicitCopy.Error.VersionId);
        Assert.Equal(deleteMarkerVersion.LastModifiedUtc, explicitCopy.Error.LastModifiedUtc);
        Assert.False((await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "target",
            Key = "docs/versioned-copy.txt"
        })).IsSuccess);
    }

    [Fact]
    public async Task DiskStorage_ObjectTags_RoundTripAndFlowThroughHeadObject()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "tags" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("tag me"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "tags",
            Key = "docs/tagged.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        Assert.True(putResult.IsSuccess);

        var putTagsResult = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "tags",
            Key = "docs/tagged.txt",
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["environment"] = "test",
                ["owner"] = "copilot"
            }
        });

        Assert.True(putTagsResult.IsSuccess);
        Assert.Equal("test", putTagsResult.Value!.Tags["environment"]);

        var getTagsResult = await storageService.GetObjectTagsAsync(new GetObjectTagsRequest
        {
            BucketName = "tags",
            Key = "docs/tagged.txt"
        });

        Assert.True(getTagsResult.IsSuccess);
        Assert.Equal("copilot", getTagsResult.Value!.Tags["owner"]);

        var headObjectResult = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "tags",
            Key = "docs/tagged.txt"
        });

        Assert.True(headObjectResult.IsSuccess);
        Assert.Equal("test", headObjectResult.Value!.Tags!["environment"]);
    }

    [Fact]
    public async Task DiskStorage_PutObject_WithTags_PersistsOnWrite()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "tagged-put" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("tagged write"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "tagged-put",
            Key = "docs/object.txt",
            Content = uploadStream,
            ContentType = "text/plain",
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["environment"] = "test",
                ["owner"] = "copilot"
            }
        });

        Assert.True(putResult.IsSuccess);
        Assert.Equal("test", putResult.Value!.Tags!["environment"]);

        var getTagsResult = await storageService.GetObjectTagsAsync(new GetObjectTagsRequest
        {
            BucketName = "tagged-put",
            Key = "docs/object.txt"
        });

        Assert.True(getTagsResult.IsSuccess);
        Assert.Equal("copilot", getTagsResult.Value!.Tags["owner"]);
    }

    [Fact]
    public async Task DiskStorage_PutObjectTags_RejectsInvalidTagSets()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "invalid-tags"
        })).IsSuccess);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("tagged payload"));
        Assert.True((await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "invalid-tags",
            Key = "docs/tagged.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        })).IsSuccess);

        var putTagsResult = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "invalid-tags",
            Key = "docs/tagged.txt",
            Tags = Enumerable.Range(0, 11).ToDictionary(
                static index => $"tag-{index}",
                static index => $"value-{index}",
                StringComparer.Ordinal)
        });

        Assert.False(putTagsResult.IsSuccess);
        Assert.Equal(StorageErrorCode.InvalidTag, putTagsResult.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_DeleteObjectTags_ClearsCurrentAndHistoricalVersionTagsIndependently()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "delete-tags",
            EnableVersioning = true
        })).IsSuccess);

        await using var v1Stream = new MemoryStream(Encoding.UTF8.GetBytes("version one"));
        var v1Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "delete-tags",
            Key = "docs/tagged.txt",
            Content = v1Stream,
            ContentType = "text/plain"
        });
        Assert.True(v1Put.IsSuccess);

        var putV1Tags = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "delete-tags",
            Key = "docs/tagged.txt",
            VersionId = v1Put.Value!.VersionId,
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["generation"] = "one"
            }
        });
        Assert.True(putV1Tags.IsSuccess);
        Assert.Equal(v1Put.Value.VersionId, putV1Tags.Value!.VersionId);

        await using var v2Stream = new MemoryStream(Encoding.UTF8.GetBytes("version two"));
        var v2Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "delete-tags",
            Key = "docs/tagged.txt",
            Content = v2Stream,
            ContentType = "text/plain"
        });
        Assert.True(v2Put.IsSuccess);

        var putCurrentTags = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "delete-tags",
            Key = "docs/tagged.txt",
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["generation"] = "two"
            }
        });
        Assert.True(putCurrentTags.IsSuccess);
        Assert.Equal(v2Put.Value!.VersionId, putCurrentTags.Value!.VersionId);

        var deleteCurrentTags = await storageService.DeleteObjectTagsAsync(new DeleteObjectTagsRequest
        {
            BucketName = "delete-tags",
            Key = "docs/tagged.txt"
        });
        Assert.True(deleteCurrentTags.IsSuccess);
        Assert.Equal(v2Put.Value.VersionId, deleteCurrentTags.Value!.VersionId);
        Assert.Empty(deleteCurrentTags.Value.Tags);

        var currentTags = await storageService.GetObjectTagsAsync(new GetObjectTagsRequest
        {
            BucketName = "delete-tags",
            Key = "docs/tagged.txt"
        });
        Assert.True(currentTags.IsSuccess);
        Assert.Equal(v2Put.Value.VersionId, currentTags.Value!.VersionId);
        Assert.Empty(currentTags.Value.Tags);

        var historicalTagsBeforeDelete = await storageService.GetObjectTagsAsync(new GetObjectTagsRequest
        {
            BucketName = "delete-tags",
            Key = "docs/tagged.txt",
            VersionId = v1Put.Value.VersionId
        });
        Assert.True(historicalTagsBeforeDelete.IsSuccess);
        Assert.Equal("one", historicalTagsBeforeDelete.Value!.Tags["generation"]);

        var deleteHistoricalTags = await storageService.DeleteObjectTagsAsync(new DeleteObjectTagsRequest
        {
            BucketName = "delete-tags",
            Key = "docs/tagged.txt",
            VersionId = v1Put.Value.VersionId
        });
        Assert.True(deleteHistoricalTags.IsSuccess);
        Assert.Equal(v1Put.Value.VersionId, deleteHistoricalTags.Value!.VersionId);
        Assert.Empty(deleteHistoricalTags.Value.Tags);

        var historicalTagsAfterDelete = await storageService.GetObjectTagsAsync(new GetObjectTagsRequest
        {
            BucketName = "delete-tags",
            Key = "docs/tagged.txt",
            VersionId = v1Put.Value.VersionId
        });
        Assert.True(historicalTagsAfterDelete.IsSuccess);
        Assert.Equal(v1Put.Value.VersionId, historicalTagsAfterDelete.Value!.VersionId);
        Assert.Empty(historicalTagsAfterDelete.Value.Tags);
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_CompletesObjectAndPreservesMetadata()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart" });

        var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart",
            Key = "docs/assembled.txt",
            ContentType = "text/plain",
            Metadata = new Dictionary<string, string>
            {
                ["source"] = "multipart"
            },
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["upload"] = "multipart"
            }
        });

        Assert.True(initiateResult.IsSuccess);

        await using var part1Stream = new MemoryStream(Encoding.UTF8.GetBytes("hello "));
        var part1 = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "multipart",
            Key = "docs/assembled.txt",
            UploadId = initiateResult.Value!.UploadId,
            PartNumber = 1,
            Content = part1Stream
        });

        await using var part2Stream = new MemoryStream(Encoding.UTF8.GetBytes("world"));
        var part2 = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "multipart",
            Key = "docs/assembled.txt",
            UploadId = initiateResult.Value!.UploadId,
            PartNumber = 2,
            Content = part2Stream
        });

        Assert.True(part1.IsSuccess);
        Assert.True(part2.IsSuccess);

        var completeResult = await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "multipart",
            Key = "docs/assembled.txt",
            UploadId = initiateResult.Value.UploadId,
            Parts = [part1.Value!, part2.Value!]
        });

        Assert.True(completeResult.IsSuccess);
        Assert.Equal("text/plain", completeResult.Value!.ContentType);
        Assert.Equal("multipart", completeResult.Value.Metadata!["source"]);
        Assert.Equal("multipart", completeResult.Value.Tags!["upload"]);
        var multipartChecksum = Assert.IsType<string>(completeResult.Value.Checksums!["sha256"]);
        Assert.False(string.IsNullOrWhiteSpace(multipartChecksum));

        var getResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "multipart",
            Key = "docs/assembled.txt"
        });

        Assert.True(getResult.IsSuccess);
        await using var response = getResult.Value!;
        using var reader = new StreamReader(response.Content, Encoding.UTF8);
        Assert.Equal("hello world", await reader.ReadToEndAsync());
        Assert.Equal("multipart", response.Object.Metadata!["source"]);
        Assert.Equal("multipart", response.Object.Tags!["upload"]);
        Assert.Equal(multipartChecksum, response.Object.Checksums!["sha256"]);
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_RejectsServerSideEncryptionRequests()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-sse" });

        var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-sse",
            Key = "docs/assembled.txt",
            ServerSideEncryption = CreateServerSideEncryptionSettings()
        });

        Assert.False(initiateResult.IsSuccess);
        AssertUnsupportedServerSideEncryption(initiateResult.Error, "multipart-sse", "docs/assembled.txt");

        var uploads = await storageService.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
        {
            BucketName = "multipart-sse"
        }).ToArrayAsync();

        Assert.Empty(uploads);
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_CanCopyPartRangeWithPreconditions()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-copy" });

        var sourcePut = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "multipart-copy",
            Key = "docs/source.txt",
            Content = new MemoryStream(Encoding.UTF8.GetBytes("0123456789")),
            ContentType = "text/plain"
        });
        Assert.True(sourcePut.IsSuccess);

        var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-copy",
            Key = "docs/copied.txt",
            ContentType = "text/plain",
            ChecksumAlgorithm = "SHA256"
        });
        Assert.True(initiateResult.IsSuccess);

        var copiedPart = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "multipart-copy",
            Key = "docs/copied.txt",
            UploadId = initiateResult.Value!.UploadId,
            PartNumber = 1,
            CopySourceBucketName = "multipart-copy",
            CopySourceKey = "docs/source.txt",
            CopySourceIfMatchETag = sourcePut.Value!.ETag,
            CopySourceRange = new ObjectRange
            {
                Start = 2,
                End = 6
            }
        });

        Assert.True(copiedPart.IsSuccess);
        Assert.False(string.IsNullOrWhiteSpace(copiedPart.Value!.Checksums!["sha256"]));

        var failedPart = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "multipart-copy",
            Key = "docs/copied.txt",
            UploadId = initiateResult.Value.UploadId,
            PartNumber = 2,
            CopySourceBucketName = "multipart-copy",
            CopySourceKey = "docs/source.txt",
            CopySourceIfMatchETag = "\"different\""
        });

        Assert.False(failedPart.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.PreconditionFailed, failedPart.Error!.Code);

        var completeResult = await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "multipart-copy",
            Key = "docs/copied.txt",
            UploadId = initiateResult.Value.UploadId,
            Parts = [copiedPart.Value]
        });

        Assert.True(completeResult.IsSuccess);

        var getResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "multipart-copy",
            Key = "docs/copied.txt"
        });

        Assert.True(getResult.IsSuccess);
        await using var response = getResult.Value!;
        using var reader = new StreamReader(response.Content, Encoding.UTF8);
        Assert.Equal("23456", await reader.ReadToEndAsync());
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_CanBeAborted()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-abort" });

        var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-abort",
            Key = "docs/aborted.txt"
        });

        Assert.True(initiateResult.IsSuccess);

        await using var partStream = new MemoryStream(Encoding.UTF8.GetBytes("temporary"));
        var uploadPartResult = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "multipart-abort",
            Key = "docs/aborted.txt",
            UploadId = initiateResult.Value!.UploadId,
            PartNumber = 1,
            Content = partStream
        });

        Assert.True(uploadPartResult.IsSuccess);

        var abortResult = await storageService.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
        {
            BucketName = "multipart-abort",
            Key = "docs/aborted.txt",
            UploadId = initiateResult.Value.UploadId
        });

        Assert.True(abortResult.IsSuccess);

        var completeResult = await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "multipart-abort",
            Key = "docs/aborted.txt",
            UploadId = initiateResult.Value.UploadId,
            Parts = [uploadPartResult.Value!]
        });

        Assert.False(completeResult.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.MultipartConflict, completeResult.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_ListMultipartUploads_AppliesPrefixMarkersAndPageSize()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-list" });

        var firstUpload = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-list",
            Key = "docs/alpha.txt"
        });
        await Task.Delay(2);

        var secondUpload = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-list",
            Key = "docs/alpha.txt"
        });
        await Task.Delay(2);

        var thirdUpload = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-list",
            Key = "docs/beta.txt"
        });

        await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-list",
            Key = "videos/clip.txt"
        });

        Assert.True(firstUpload.IsSuccess);
        Assert.True(secondUpload.IsSuccess);
        Assert.True(thirdUpload.IsSuccess);

        var firstPage = await storageService.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
        {
            BucketName = "multipart-list",
            Prefix = "docs/",
            PageSize = 2
        }).ToArrayAsync();

        Assert.Collection(
            firstPage,
            upload => {
                Assert.Equal("docs/alpha.txt", upload.Key);
                Assert.Equal(firstUpload.Value!.UploadId, upload.UploadId);
            },
            upload => {
                Assert.Equal("docs/alpha.txt", upload.Key);
                Assert.Equal(secondUpload.Value!.UploadId, upload.UploadId);
            });

        var secondPage = await storageService.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
        {
            BucketName = "multipart-list",
            Prefix = "docs/",
            KeyMarker = "docs/alpha.txt",
            UploadIdMarker = secondUpload.Value!.UploadId
        }).ToArrayAsync();

        var remainingUpload = Assert.Single(secondPage);
        Assert.Equal("docs/beta.txt", remainingUpload.Key);
        Assert.Equal(thirdUpload.Value!.UploadId, remainingUpload.UploadId);
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_ListMultipartUploads_ReturnsEmptySequenceWhenBucketHasNoUploads()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-empty" });

        var uploads = await storageService.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
        {
            BucketName = "multipart-empty"
        }).ToArrayAsync();

        Assert.Empty(uploads);
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_ListMultipartUploads_ExcludesCompletedAndAbortedUploads()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-filtered" });

        var pendingUpload = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-filtered",
            Key = "docs/pending.txt"
        });
        var completedUpload = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-filtered",
            Key = "docs/completed.txt"
        });
        var abortedUpload = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-filtered",
            Key = "docs/aborted.txt"
        });

        Assert.True(pendingUpload.IsSuccess);
        Assert.True(completedUpload.IsSuccess);
        Assert.True(abortedUpload.IsSuccess);

        await using var completedPartStream = new MemoryStream(Encoding.UTF8.GetBytes("complete"));
        var completedPart = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "multipart-filtered",
            Key = "docs/completed.txt",
            UploadId = completedUpload.Value!.UploadId,
            PartNumber = 1,
            Content = completedPartStream
        });

        await using var abortedPartStream = new MemoryStream(Encoding.UTF8.GetBytes("abort"));
        var abortedPart = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "multipart-filtered",
            Key = "docs/aborted.txt",
            UploadId = abortedUpload.Value!.UploadId,
            PartNumber = 1,
            Content = abortedPartStream
        });

        Assert.True(completedPart.IsSuccess);
        Assert.True(abortedPart.IsSuccess);

        var completeResult = await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "multipart-filtered",
            Key = "docs/completed.txt",
            UploadId = completedUpload.Value.UploadId,
            Parts = [completedPart.Value!]
        });
        Assert.True(completeResult.IsSuccess);

        var abortResult = await storageService.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
        {
            BucketName = "multipart-filtered",
            Key = "docs/aborted.txt",
            UploadId = abortedUpload.Value.UploadId
        });
        Assert.True(abortResult.IsSuccess);

        var uploads = await storageService.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
        {
            BucketName = "multipart-filtered"
        }).ToArrayAsync();

        var remainingUpload = Assert.Single(uploads);
        Assert.Equal("docs/pending.txt", remainingUpload.Key);
        Assert.Equal(pendingUpload.Value!.UploadId, remainingUpload.UploadId);
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_ListMultipartUploads_UsesPlatformStateStoreWithoutSidecars()
    {
        await using var fixture = new DiskStorageFixture(services => {
            services.AddSingleton<InMemoryMultipartStateStore>();
            services.AddSingleton<IStorageMultipartStateStore>(static serviceProvider => serviceProvider.GetRequiredService<InMemoryMultipartStateStore>());
        });

        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        var multipartStateStore = fixture.Services.GetRequiredService<InMemoryMultipartStateStore>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-platform-list" });

        var baseInitiatedAtUtc = DateTimeOffset.UtcNow;
        await multipartStateStore.UpsertMultipartUploadStateAsync("test-disk", new MultipartUploadState
        {
            BucketName = "multipart-platform-list",
            Key = "docs/alpha.txt",
            UploadId = "upload-002",
            InitiatedAtUtc = baseInitiatedAtUtc.AddSeconds(1)
        });
        await multipartStateStore.UpsertMultipartUploadStateAsync("test-disk", new MultipartUploadState
        {
            BucketName = "multipart-platform-list",
            Key = "docs/alpha.txt",
            UploadId = "upload-001",
            InitiatedAtUtc = baseInitiatedAtUtc
        });
        await multipartStateStore.UpsertMultipartUploadStateAsync("test-disk", new MultipartUploadState
        {
            BucketName = "multipart-platform-list",
            Key = "docs/nested/beta.txt",
            UploadId = "upload-003",
            InitiatedAtUtc = baseInitiatedAtUtc.AddSeconds(2)
        });

        var uploads = await storageService.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
        {
            BucketName = "multipart-platform-list",
            Prefix = "docs/"
        }).ToArrayAsync();

        Assert.Collection(
            uploads,
            upload => {
                Assert.Equal("docs/alpha.txt", upload.Key);
                Assert.Equal("upload-001", upload.UploadId);
            },
            upload => {
                Assert.Equal("docs/alpha.txt", upload.Key);
                Assert.Equal("upload-002", upload.UploadId);
            },
            upload => {
                Assert.Equal("docs/nested/beta.txt", upload.Key);
                Assert.Equal("upload-003", upload.UploadId);
            });
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_ListMultipartUploadParts_AppliesMarkersAndPageSize()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-parts" });

        var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-parts",
            Key = "docs/parts.txt",
            ChecksumAlgorithm = "sha256"
        });

        Assert.True(initiateResult.IsSuccess);

        var partPayloads = new Dictionary<int, string>
        {
            [1] = "alpha",
            [2] = "bravo",
            [3] = "charlie"
        };

        foreach (var (partNumber, payload) in partPayloads) {
            await using var partStream = new MemoryStream(Encoding.UTF8.GetBytes(payload));
            var uploadPartResult = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
            {
                BucketName = "multipart-parts",
                Key = "docs/parts.txt",
                UploadId = initiateResult.Value!.UploadId,
                PartNumber = partNumber,
                Content = partStream,
                ChecksumAlgorithm = "sha256",
                Checksums = new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["sha256"] = ComputeSha256Base64(payload)
                }
            });

            Assert.True(uploadPartResult.IsSuccess);
        }

        var firstPage = await storageService.ListMultipartUploadPartsAsync(new ListMultipartUploadPartsRequest
        {
            BucketName = "multipart-parts",
            Key = "docs/parts.txt",
            UploadId = initiateResult.Value!.UploadId,
            PageSize = 2
        }).ToArrayAsync();

        Assert.Collection(
            firstPage,
            part => {
                Assert.Equal(1, part.PartNumber);
                Assert.Equal(ComputeSha256Base64(partPayloads[1]), part.Checksums!["sha256"]);
            },
            part => {
                Assert.Equal(2, part.PartNumber);
                Assert.Equal(ComputeSha256Base64(partPayloads[2]), part.Checksums!["sha256"]);
            });

        var secondPage = await storageService.ListMultipartUploadPartsAsync(new ListMultipartUploadPartsRequest
        {
            BucketName = "multipart-parts",
            Key = "docs/parts.txt",
            UploadId = initiateResult.Value!.UploadId,
            PartNumberMarker = 2
        }).ToArrayAsync();

        var remainingPart = Assert.Single(secondPage);
        Assert.Equal(3, remainingPart.PartNumber);
        Assert.Equal(ComputeSha256Base64(partPayloads[3]), remainingPart.Checksums!["sha256"]);
    }

    [Fact]
    public async Task DiskStorage_UsesRegisteredPlatformObjectStateStoreForMetadataAndTags()
    {
        await using var fixture = new DiskStorageFixture(services => {
            services.AddSingleton<IStorageObjectStateStore, InMemoryObjectStateStore>();
        });

        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "external-state" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("hello external state"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "external-state",
            Key = "docs/external.txt",
            Content = uploadStream,
            ContentType = "text/plain",
            Metadata = new Dictionary<string, string>
            {
                ["owner"] = "platform-store"
            }
        });

        Assert.True(putResult.IsSuccess);

        var putTagsResult = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "external-state",
            Key = "docs/external.txt",
            Tags = new Dictionary<string, string>
            {
                ["environment"] = "test"
            }
        });

        Assert.True(putTagsResult.IsSuccess);

        var getResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "external-state",
            Key = "docs/external.txt"
        });

        Assert.True(getResult.IsSuccess);
        await using (var response = getResult.Value!) {
            Assert.Equal("platform-store", response.Object.Metadata!["owner"]);
            Assert.Equal("test", response.Object.Tags!["environment"]);
            Assert.Equal(ComputeSha256Base64("hello external state"), response.Object.Checksums!["sha256"]);
        }

        var supportState = await storageService.GetSupportStateDescriptorAsync();
        Assert.Equal(IntegratedS3.Abstractions.Capabilities.StorageSupportStateOwnership.PlatformManaged, supportState.ObjectMetadata);
        Assert.Equal(IntegratedS3.Abstractions.Capabilities.StorageSupportStateOwnership.PlatformManaged, supportState.ObjectTags);
        Assert.Equal(IntegratedS3.Abstractions.Capabilities.StorageSupportStateOwnership.PlatformManaged, supportState.Checksums);
        Assert.Equal(IntegratedS3.Abstractions.Capabilities.StorageSupportStateOwnership.NotApplicable, supportState.AccessControl);
        Assert.Equal(IntegratedS3.Abstractions.Capabilities.StorageSupportStateOwnership.NotApplicable, supportState.Retention);
        Assert.Equal(IntegratedS3.Abstractions.Capabilities.StorageSupportStateOwnership.NotApplicable, supportState.ServerSideEncryption);

        var capabilities = await storageService.GetCapabilitiesAsync();
        Assert.Equal(IntegratedS3.Abstractions.Capabilities.StorageCapabilitySupport.Unsupported, capabilities.ServerSideEncryption);
        Assert.Empty(capabilities.ServerSideEncryptionDetails.Variants);

        var providerMode = await storageService.GetProviderModeAsync();
        Assert.Equal(IntegratedS3.Abstractions.Models.StorageProviderMode.Hybrid, providerMode);

        var objectLocation = await storageService.GetObjectLocationDescriptorAsync();
        Assert.Equal(IntegratedS3.Abstractions.Models.StorageObjectAccessMode.ProxyStream, objectLocation.DefaultAccessMode);
        Assert.Equal([IntegratedS3.Abstractions.Models.StorageObjectAccessMode.ProxyStream], objectLocation.SupportedAccessModes);

        var sidecarPath = Path.Combine(fixture.RootPath, "external-state", "docs", "external.txt.integrateds3.json");
        Assert.False(File.Exists(sidecarPath));
    }

    [Fact]
    public async Task DiskStorage_UsesRegisteredPlatformObjectStateStoreForHistoricalVersionsAndDeleteMarkers()
    {
        await using var fixture = new DiskStorageFixture(services => {
            services.AddSingleton<IStorageObjectStateStore, InMemoryObjectStateStore>();
        });

        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "external-version-state",
            EnableVersioning = true
        });

        await using var v1Stream = new MemoryStream(Encoding.UTF8.GetBytes("version one"));
        var v1Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "external-version-state",
            Key = "docs/history.txt",
            Content = v1Stream,
            ContentType = "text/plain"
        });

        await using var v2Stream = new MemoryStream(Encoding.UTF8.GetBytes("version two"));
        var v2Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "external-version-state",
            Key = "docs/history.txt",
            Content = v2Stream,
            ContentType = "text/plain"
        });

        var deleteCurrent = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "external-version-state",
            Key = "docs/history.txt"
        });

        Assert.True(deleteCurrent.IsSuccess);
        Assert.True(deleteCurrent.Value!.IsDeleteMarker);

        var versions = await storageService.ListObjectVersionsAsync(new ListObjectVersionsRequest
        {
            BucketName = "external-version-state",
            Prefix = "docs/history.txt"
        }).ToArrayAsync();

        Assert.Equal(3, versions.Length);
        Assert.Contains(versions, version => version.VersionId == v1Put.Value!.VersionId);
        Assert.Contains(versions, version => version.VersionId == v2Put.Value!.VersionId);
        Assert.Contains(versions, static version => version.IsDeleteMarker && version.IsLatest);

        var versionMetadataFiles = Directory.Exists(Path.Combine(fixture.RootPath, "external-version-state", ".integrateds3-versions"))
            ? Directory.EnumerateFiles(Path.Combine(fixture.RootPath, "external-version-state", ".integrateds3-versions"), "*.integrateds3.json", SearchOption.AllDirectories).ToArray()
            : [];

        Assert.Empty(versionMetadataFiles);
    }

    [Fact]
    public async Task DiskStorage_MigratesLegacyCurrentObjectStateIntoRegisteredStoreAcrossRestarts()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "legacy-current-state"
        })).IsSuccess);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("legacy current payload"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "legacy-current-state",
            Key = "docs/current.txt",
            Content = uploadStream,
            ContentType = "text/plain",
            Metadata = new Dictionary<string, string>
            {
                ["owner"] = "legacy"
            }
        });

        Assert.True(putResult.IsSuccess);

        var putTagsResult = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "legacy-current-state",
            Key = "docs/current.txt",
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["environment"] = "migration"
            }
        });

        Assert.True(putTagsResult.IsSuccess);

        var versionId = putResult.Value!.VersionId;
        var checksum = putResult.Value.Checksums!["sha256"];
        var metadataSidecarPath = Path.Combine(fixture.RootPath, "legacy-current-state", "docs", "current.txt.integrateds3.json");
        Assert.True(File.Exists(metadataSidecarPath));

        var stateStore = new InMemoryObjectStateStore();
        await fixture.RestartAsync(services => {
            services.AddSingleton<IStorageObjectStateStore>(stateStore);
        });

        storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        var migratedHead = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "legacy-current-state",
            Key = "docs/current.txt"
        });

        Assert.True(migratedHead.IsSuccess);
        Assert.Equal(versionId, migratedHead.Value!.VersionId);
        Assert.Equal("legacy", migratedHead.Value.Metadata!["owner"]);
        Assert.Equal("migration", migratedHead.Value.Tags!["environment"]);
        Assert.Equal(checksum, migratedHead.Value.Checksums!["sha256"]);
        Assert.False(File.Exists(metadataSidecarPath));

        var migratedState = await stateStore.GetObjectInfoAsync("test-disk", "legacy-current-state", "docs/current.txt", versionId);
        Assert.NotNull(migratedState);
        Assert.Equal(versionId, migratedState!.VersionId);
        Assert.True(migratedState.IsLatest);
        Assert.False(migratedState.IsDeleteMarker);
        Assert.Equal("legacy", migratedState.Metadata!["owner"]);
        Assert.Equal("migration", migratedState.Tags!["environment"]);
        Assert.Equal(checksum, migratedState.Checksums!["sha256"]);

        await fixture.RestartAsync(services => {
            services.AddSingleton<IStorageObjectStateStore>(stateStore);
        });

        storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        var updatedTags = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "legacy-current-state",
            Key = "docs/current.txt",
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["environment"] = "platform-managed",
                ["phase"] = "after-restart"
            }
        });

        Assert.True(updatedTags.IsSuccess);
        Assert.Equal(versionId, updatedTags.Value!.VersionId);

        var rereadHead = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "legacy-current-state",
            Key = "docs/current.txt"
        });

        Assert.True(rereadHead.IsSuccess);
        Assert.Equal(versionId, rereadHead.Value!.VersionId);
        Assert.Equal("legacy", rereadHead.Value.Metadata!["owner"]);
        Assert.Equal("platform-managed", rereadHead.Value.Tags!["environment"]);
        Assert.Equal("after-restart", rereadHead.Value.Tags["phase"]);
        Assert.Equal(checksum, rereadHead.Value.Checksums!["sha256"]);
        Assert.False(File.Exists(metadataSidecarPath));
    }

    [Fact]
    public async Task DiskStorage_MigratesLegacyVersionHistoryAndDeleteMarkerStateIntoRegisteredStoreAcrossRestarts()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "legacy-version-state",
            EnableVersioning = true
        })).IsSuccess);

        await using var v1Stream = new MemoryStream(Encoding.UTF8.GetBytes("legacy version one"));
        var v1Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "legacy-version-state",
            Key = "docs/history.txt",
            Content = v1Stream,
            ContentType = "text/plain",
            Metadata = new Dictionary<string, string>
            {
                ["generation"] = "one"
            }
        });

        Assert.True(v1Put.IsSuccess);

        var v1Tags = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "legacy-version-state",
            Key = "docs/history.txt",
            VersionId = v1Put.Value!.VersionId,
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["release"] = "one"
            }
        });

        Assert.True(v1Tags.IsSuccess);

        await using var v2Stream = new MemoryStream(Encoding.UTF8.GetBytes("legacy version two"));
        var v2Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "legacy-version-state",
            Key = "docs/history.txt",
            Content = v2Stream,
            ContentType = "text/plain",
            Metadata = new Dictionary<string, string>
            {
                ["generation"] = "two"
            }
        });

        Assert.True(v2Put.IsSuccess);

        var v2Tags = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "legacy-version-state",
            Key = "docs/history.txt",
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["release"] = "two"
            }
        });

        Assert.True(v2Tags.IsSuccess);

        var deleteCurrent = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "legacy-version-state",
            Key = "docs/history.txt"
        });

        Assert.True(deleteCurrent.IsSuccess);
        Assert.True(deleteCurrent.Value!.IsDeleteMarker);

        var deleteMarkerVersionId = deleteCurrent.Value.VersionId;
        var currentDeleteMarkerSidecar = Path.Combine(fixture.RootPath, "legacy-version-state", "docs", "history.txt.integrateds3.json");
        var archivedV1Sidecar = Path.Combine(fixture.RootPath, "legacy-version-state", ".integrateds3-versions", "docs", "history.txt", v1Put.Value.VersionId!, "content.integrateds3.json");
        var archivedV2Sidecar = Path.Combine(fixture.RootPath, "legacy-version-state", ".integrateds3-versions", "docs", "history.txt", v2Put.Value!.VersionId!, "content.integrateds3.json");

        Assert.True(File.Exists(currentDeleteMarkerSidecar));
        Assert.True(File.Exists(archivedV1Sidecar));
        Assert.True(File.Exists(archivedV2Sidecar));

        var stateStore = new InMemoryObjectStateStore();
        await fixture.RestartAsync(services => {
            services.AddSingleton<IStorageObjectStateStore>(stateStore);
        });

        storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        var versions = await storageService.ListObjectVersionsAsync(new ListObjectVersionsRequest
        {
            BucketName = "legacy-version-state",
            Prefix = "docs/history.txt"
        }).ToArrayAsync();

        Assert.Equal(3, versions.Length);

        var deleteMarkerVersion = Assert.Single(versions, version => version.VersionId == deleteMarkerVersionId);
        Assert.True(deleteMarkerVersion.IsDeleteMarker);
        Assert.True(deleteMarkerVersion.IsLatest);

        var migratedV1 = Assert.Single(versions, version => version.VersionId == v1Put.Value.VersionId);
        Assert.False(migratedV1.IsDeleteMarker);
        Assert.False(migratedV1.IsLatest);
        Assert.Equal("one", migratedV1.Metadata!["generation"]);
        Assert.Equal("one", migratedV1.Tags!["release"]);
        Assert.Equal(v1Put.Value.Checksums!["sha256"], migratedV1.Checksums!["sha256"]);

        var migratedV2 = Assert.Single(versions, version => version.VersionId == v2Put.Value.VersionId);
        Assert.False(migratedV2.IsDeleteMarker);
        Assert.False(migratedV2.IsLatest);
        Assert.Equal("two", migratedV2.Metadata!["generation"]);
        Assert.Equal("two", migratedV2.Tags!["release"]);
        Assert.Equal(v2Put.Value.Checksums!["sha256"], migratedV2.Checksums!["sha256"]);

        Assert.False(File.Exists(currentDeleteMarkerSidecar));
        Assert.False(File.Exists(archivedV1Sidecar));
        Assert.False(File.Exists(archivedV2Sidecar));

        var updatedHistoricalTags = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "legacy-version-state",
            Key = "docs/history.txt",
            VersionId = v1Put.Value.VersionId,
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["release"] = "one-migrated",
                ["migration"] = "complete"
            }
        });

        Assert.True(updatedHistoricalTags.IsSuccess);
        Assert.Equal(v1Put.Value.VersionId, updatedHistoricalTags.Value!.VersionId);

        var historicalHead = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "legacy-version-state",
            Key = "docs/history.txt",
            VersionId = v1Put.Value.VersionId
        });

        Assert.True(historicalHead.IsSuccess);
        Assert.Equal(v1Put.Value.VersionId, historicalHead.Value!.VersionId);
        Assert.Equal("one", historicalHead.Value.Metadata!["generation"]);
        Assert.Equal("one-migrated", historicalHead.Value.Tags!["release"]);
        Assert.Equal("complete", historicalHead.Value.Tags["migration"]);
        Assert.Equal(v1Put.Value.Checksums!["sha256"], historicalHead.Value.Checksums!["sha256"]);

        await fixture.RestartAsync(services => {
            services.AddSingleton<IStorageObjectStateStore>(stateStore);
        });

        storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        var deleteMarkerDelete = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "legacy-version-state",
            Key = "docs/history.txt",
            VersionId = deleteMarkerVersionId
        });

        Assert.True(deleteMarkerDelete.IsSuccess);
        Assert.True(deleteMarkerDelete.Value!.IsDeleteMarker);
        Assert.NotNull(deleteMarkerDelete.Value.CurrentObject);
        Assert.Equal(v2Put.Value.VersionId, deleteMarkerDelete.Value.CurrentObject!.VersionId);

        var currentHead = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "legacy-version-state",
            Key = "docs/history.txt"
        });

        Assert.True(currentHead.IsSuccess);
        Assert.Equal(v2Put.Value.VersionId, currentHead.Value!.VersionId);
        Assert.Equal("two", currentHead.Value.Metadata!["generation"]);
        Assert.Equal("two", currentHead.Value.Tags!["release"]);
        Assert.Equal(v2Put.Value.Checksums!["sha256"], currentHead.Value.Checksums!["sha256"]);
        Assert.False(File.Exists(currentDeleteMarkerSidecar));
        Assert.False(File.Exists(archivedV2Sidecar));

        var currentStoreState = await stateStore.GetObjectInfoAsync("test-disk", "legacy-version-state", "docs/history.txt");
        Assert.NotNull(currentStoreState);
        Assert.Equal(v2Put.Value.VersionId, currentStoreState!.VersionId);
        Assert.True(currentStoreState.IsLatest);
        Assert.False(currentStoreState.IsDeleteMarker);
        Assert.Equal("two", currentStoreState.Tags!["release"]);
        Assert.Equal(v2Put.Value.Checksums!["sha256"], currentStoreState.Checksums!["sha256"]);
    }

    [Fact]
    public async Task DiskStorage_UsesRegisteredPlatformMultipartStateStoreForUploadState()
    {
        await using var fixture = new DiskStorageFixture(services => {
            services.AddSingleton<InMemoryMultipartStateStore>();
            services.AddSingleton<IStorageMultipartStateStore>(static serviceProvider => serviceProvider.GetRequiredService<InMemoryMultipartStateStore>());
        });

        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        var multipartStateStore = fixture.Services.GetRequiredService<InMemoryMultipartStateStore>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-external" });

        var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-external",
            Key = "docs/assembled.txt",
            ContentType = "text/plain",
            Metadata = new Dictionary<string, string>
            {
                ["source"] = "external-multipart"
            },
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["upload"] = "external"
            }
        });

        Assert.True(initiateResult.IsSuccess);

        var uploadId = initiateResult.Value!.UploadId;
        var uploadState = await multipartStateStore.GetMultipartUploadStateAsync("test-disk", "multipart-external", "docs/assembled.txt", uploadId);
        Assert.NotNull(uploadState);
        Assert.Equal("text/plain", uploadState!.ContentType);
        Assert.Equal("external-multipart", uploadState.Metadata!["source"]);
        Assert.Equal("external", uploadState.Tags!["upload"]);

        var listedUploads = await storageService.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
        {
            BucketName = "multipart-external"
        }).ToArrayAsync();
        var listedUpload = Assert.Single(listedUploads);
        Assert.Equal(uploadId, listedUpload.UploadId);
        Assert.Equal("docs/assembled.txt", listedUpload.Key);

        await using var part1Stream = new MemoryStream(Encoding.UTF8.GetBytes("hello "));
        var part1 = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "multipart-external",
            Key = "docs/assembled.txt",
            UploadId = uploadId,
            PartNumber = 1,
            Content = part1Stream
        });

        await using var part2Stream = new MemoryStream(Encoding.UTF8.GetBytes("world"));
        var part2 = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "multipart-external",
            Key = "docs/assembled.txt",
            UploadId = uploadId,
            PartNumber = 2,
            Content = part2Stream
        });

        Assert.True(part1.IsSuccess);
        Assert.True(part2.IsSuccess);

        var completeResult = await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "multipart-external",
            Key = "docs/assembled.txt",
            UploadId = uploadId,
            Parts = [part1.Value!, part2.Value!]
        });

        Assert.True(completeResult.IsSuccess);
        Assert.Equal("text/plain", completeResult.Value!.ContentType);
        Assert.Equal("external-multipart", completeResult.Value.Metadata!["source"]);
        Assert.Equal("external", completeResult.Value.Tags!["upload"]);

        var removedState = await multipartStateStore.GetMultipartUploadStateAsync("test-disk", "multipart-external", "docs/assembled.txt", uploadId);
        Assert.Null(removedState);

        var supportState = await storageService.GetSupportStateDescriptorAsync();
        Assert.Equal(IntegratedS3.Abstractions.Capabilities.StorageSupportStateOwnership.PlatformManaged, supportState.MultipartState);
    }

    private static void AssertUnsupportedServerSideEncryption(StorageError? error, string bucketName, string objectKey)
    {
        var actual = Assert.IsType<StorageError>(error);
        Assert.Equal(StorageErrorCode.UnsupportedCapability, actual.Code);
        Assert.Equal(bucketName, actual.BucketName);
        Assert.Equal(objectKey, actual.ObjectKey);
        Assert.Equal(501, actual.SuggestedHttpStatusCode);
        Assert.Contains("server-side encryption", actual.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static ObjectServerSideEncryptionSettings CreateServerSideEncryptionSettings()
    {
        return new ObjectServerSideEncryptionSettings
        {
            Algorithm = ObjectServerSideEncryptionAlgorithm.Kms,
            KeyId = "disk-provider-test-key",
            Context = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["tenant"] = "tests"
            }
        };
    }
}
