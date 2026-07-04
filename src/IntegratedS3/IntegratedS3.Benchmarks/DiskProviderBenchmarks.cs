using BenchmarkDotNet.Attributes;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Provider.Disk;
using IntegratedS3.Provider.Disk.DependencyInjection;
using IntegratedS3.Testing;
using Microsoft.Extensions.DependencyInjection;

namespace IntegratedS3.Benchmarks;

/// <summary>
/// Shared helpers for the Disk-provider benchmarks: builds a real <see cref="IStorageBackend"/> over a
/// temporary directory (with in-memory state stores so versioning/multipart capabilities are enabled).
/// </summary>
internal static class DiskBenchmarkHarness
{
    public static (ServiceProvider Provider, IStorageBackend Backend, string RootPath) Create()
    {
        var rootPath = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "IntegratedS3.Benchmarks", Guid.NewGuid().ToString("N"));
        var services = new ServiceCollection();
        services.AddSingleton<IStorageObjectStateStore, InMemoryObjectStateStore>();
        services.AddSingleton<IStorageMultipartStateStore, InMemoryMultipartStateStore>();
        services.AddDiskStorage(new DiskStorageOptions
        {
            ProviderName = "bench-disk",
            RootPath = rootPath,
            CreateRootDirectory = true,
        });

        var provider = services.BuildServiceProvider();
        return (provider, provider.GetRequiredService<IStorageBackend>(), rootPath);
    }

    public static T Require<T>(StorageResult<T> result)
        => result.IsSuccess ? result.Value! : throw new InvalidOperationException($"Storage operation failed: {result.Error?.Code}");

    public static void Cleanup(ServiceProvider? provider, string? rootPath)
    {
        provider?.Dispose();
        if (rootPath is not null && Directory.Exists(rootPath))
        {
            try
            {
                Directory.Delete(rootPath, recursive: true);
            }
            catch (IOException)
            {
                // Best-effort cleanup of the temp benchmark directory.
            }
        }
    }
}

/// <summary>
/// Benchmarks object PUT / GET and the multipart-complete assembly path through the Disk provider.
/// </summary>
[MemoryDiagnoser]
public class DiskObjectBenchmarks
{
    private const string Bucket = "bench";
    private const string GetKey = "get-target.bin";
    private const string PutKey = "put-target.bin";

    [Params(64 * 1024, 1024 * 1024)]
    public int PayloadBytes { get; set; }

    private ServiceProvider _provider = null!;
    private IStorageBackend _backend = null!;
    private string _rootPath = string.Empty;
    private byte[] _payload = [];
    private int _multipartCounter;

    [GlobalSetup]
    public async Task Setup()
    {
        (_provider, _backend, _rootPath) = DiskBenchmarkHarness.Create();
        _payload = new byte[PayloadBytes];
        for (var i = 0; i < _payload.Length; i++)
        {
            _payload[i] = (byte)(i * 31 + 7);
        }

        DiskBenchmarkHarness.Require(await _backend.CreateBucketAsync(new CreateBucketRequest { BucketName = Bucket }));
        DiskBenchmarkHarness.Require(await _backend.PutObjectAsync(new PutObjectRequest
        {
            BucketName = Bucket,
            Key = GetKey,
            Content = new MemoryStream(_payload, writable: false),
            ContentType = "application/octet-stream",
        }));
    }

    [GlobalCleanup]
    public void Cleanup() => DiskBenchmarkHarness.Cleanup(_provider, _rootPath);

    [Benchmark]
    public async Task<long> PutObject()
    {
        var info = DiskBenchmarkHarness.Require(await _backend.PutObjectAsync(new PutObjectRequest
        {
            BucketName = Bucket,
            Key = PutKey,
            Content = new MemoryStream(_payload, writable: false),
            ContentType = "application/octet-stream",
        }));
        return info.ContentLength;
    }

    [Benchmark]
    public async Task<long> GetObject()
    {
        var response = DiskBenchmarkHarness.Require(await _backend.GetObjectAsync(new GetObjectRequest
        {
            BucketName = Bucket,
            Key = GetKey,
        }));
        await using (response)
        {
            await response.Content.CopyToAsync(Stream.Null);
            return response.Object.ContentLength;
        }
    }

    [Benchmark]
    public async Task<long> MultipartComplete3Parts()
    {
        var key = $"mp/upload-{Interlocked.Increment(ref _multipartCounter)}.bin";
        var initiated = DiskBenchmarkHarness.Require(await _backend.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = Bucket,
            Key = key,
            ContentType = "application/octet-stream",
        }));

        var parts = new List<MultipartUploadPart>(3);
        for (var partNumber = 1; partNumber <= 3; partNumber++)
        {
            parts.Add(DiskBenchmarkHarness.Require(await _backend.UploadMultipartPartAsync(new UploadMultipartPartRequest
            {
                BucketName = Bucket,
                Key = key,
                UploadId = initiated.UploadId,
                PartNumber = partNumber,
                Content = new MemoryStream(_payload, writable: false),
            })));
        }

        var completed = DiskBenchmarkHarness.Require(await _backend.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = Bucket,
            Key = key,
            UploadId = initiated.UploadId,
            Parts = parts,
        }));
        return completed.ContentLength;
    }
}

/// <summary>
/// Benchmarks ListObjects over buckets seeded with 1 / 100 / 1000 objects (listing + pagination scan cost).
/// </summary>
[MemoryDiagnoser]
public class DiskListingBenchmarks
{
    private const string Bucket = "list";

    [Params(1, 100, 1000)]
    public int ObjectCount { get; set; }

    private ServiceProvider _provider = null!;
    private IStorageBackend _backend = null!;
    private string _rootPath = string.Empty;

    [GlobalSetup]
    public async Task Setup()
    {
        (_provider, _backend, _rootPath) = DiskBenchmarkHarness.Create();
        DiskBenchmarkHarness.Require(await _backend.CreateBucketAsync(new CreateBucketRequest { BucketName = Bucket }));

        var payload = new byte[64];
        for (var i = 0; i < ObjectCount; i++)
        {
            DiskBenchmarkHarness.Require(await _backend.PutObjectAsync(new PutObjectRequest
            {
                BucketName = Bucket,
                Key = $"obj/{i:D6}.bin",
                Content = new MemoryStream(payload, writable: false),
                ContentType = "application/octet-stream",
            }));
        }
    }

    [GlobalCleanup]
    public void Cleanup() => DiskBenchmarkHarness.Cleanup(_provider, _rootPath);

    [Benchmark]
    public async Task<int> ListObjectsPage()
    {
        var count = 0;
        await foreach (var _ in _backend.ListObjectsAsync(new ListObjectsRequest
        {
            BucketName = Bucket,
            Prefix = "obj/",
            PageSize = ObjectCount,
        }))
        {
            count++;
        }

        return count;
    }
}
