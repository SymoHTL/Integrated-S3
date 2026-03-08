using System.Text;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Provider.Disk;
using IntegratedS3.Provider.Disk.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
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
        Assert.Equal("text/plain", putResult.Value!.ContentType);
        Assert.Equal("copilot", putResult.Value.Metadata!["author"]);

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
            Assert.Equal("copilot", response.Object.Metadata!["author"]);
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
        Assert.NotEqual(putResult.Value!.BucketName, copyResult.Value.BucketName);
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

        var notModifiedCopy = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "target",
            DestinationKey = "docs/copied.txt",
            SourceIfNoneMatchETag = $"\"{putResult.Value!.ETag}\""
        });

        Assert.True(notModifiedCopy.IsSuccess);
        Assert.Equal("source", notModifiedCopy.Value!.BucketName);
        Assert.Equal("docs/source.txt", notModifiedCopy.Value.Key);
        Assert.False((await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "target",
            Key = "docs/copied.txt"
        })).IsSuccess);
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

    private sealed class DiskStorageFixture : IAsyncDisposable
    {
        public DiskStorageFixture()
        {
            RootPath = Path.Combine(Path.GetTempPath(), "IntegratedS3.Tests", Guid.NewGuid().ToString("N"));

            var services = new ServiceCollection();
            services.AddDiskStorage(new DiskStorageOptions
            {
                ProviderName = "test-disk",
                RootPath = RootPath,
                CreateRootDirectory = true
            });

            Services = services.BuildServiceProvider();
        }

        public string RootPath { get; }

        public ServiceProvider Services { get; }

        public async ValueTask DisposeAsync()
        {
            await Services.DisposeAsync();

            if (Directory.Exists(RootPath)) {
                Directory.Delete(RootPath, recursive: true);
            }
        }
    }
}
