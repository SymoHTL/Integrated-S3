using System.Net;
using System.Security.Claims;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Core.Models;

namespace IntegratedS3.Benchmarks;

internal static class BenchmarkPayloads
{
    public const int MetadataBytes = 64 * 1024;
    public const int UploadBytes = 1024 * 1024;
    public const int DownloadBytes = 4 * 1024 * 1024;
    public const int MultipartPartBytes = 2 * 1024 * 1024;
    public const int MultipartCompletePartBytes = 1024 * 1024;
    public const int ListObjectCount = 256;
    public const int ListObjectPayloadBytes = 4 * 1024;
    public static readonly byte[] MetadataObject = CreateBytes(MetadataBytes, seed: 11);
    public static readonly byte[] UploadObject = CreateBytes(UploadBytes, seed: 23);
    public static readonly byte[] DownloadObject = CreateBytes(DownloadBytes, seed: 31);
    public static readonly byte[] MultipartPart = CreateBytes(MultipartPartBytes, seed: 47);
    public static readonly byte[] MultipartCompletePart = CreateBytes(MultipartCompletePartBytes, seed: 61);
    public static readonly byte[] ListObject = CreateBytes(ListObjectPayloadBytes, seed: 73);

    private static byte[] CreateBytes(int length, byte seed)
    {
        var bytes = new byte[length];
        for (var index = 0; index < bytes.Length; index++) {
            bytes[index] = (byte)(seed + index % 251);
        }

        return bytes;
    }
}

internal abstract class HotPathBenchmarkScenarioBase : IHotPathBenchmarkScenario
{
    public abstract BenchmarkScenarioDefinition Definition { get; }

    public virtual IReadOnlyList<string> ObservedDirectories => [];

    public abstract Task InitializeAsync(CancellationToken cancellationToken);

    public virtual Task PrepareIterationAsync(bool isWarmup, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }

    public abstract ValueTask ExecuteIterationAsync(CancellationToken cancellationToken);

    public abstract ValueTask DisposeAsync();

    protected static async Task EnsureBucketExistsAsync(IStorageService storageService, string bucketName, CancellationToken cancellationToken)
    {
        var createResult = await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = bucketName
        }, cancellationToken);

        EnsureSuccess(createResult, $"Create bucket '{bucketName}'");
    }

    protected static async Task PutSeedObjectAsync(IStorageService storageService, string bucketName, string key, byte[] content, CancellationToken cancellationToken)
    {
        await using var stream = new MemoryStream(content, writable: false);
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = key,
            Content = stream,
            ContentLength = content.Length,
            ContentType = "application/octet-stream",
            OverwriteIfExists = true
        }, cancellationToken);

        EnsureSuccess(putResult, $"Put seed object '{bucketName}/{key}'");
    }

    protected static void EnsureSuccess(StorageResult result, string operation)
    {
        if (result.IsSuccess) {
            return;
        }

        throw new InvalidOperationException($"{operation} failed: {result.Error?.Message ?? "Unknown error."}");
    }

    protected static T EnsureSuccess<T>(StorageResult<T> result, string operation) where T : class
    {
        if (result.IsSuccess && result.Value is not null) {
            return result.Value;
        }

        throw new InvalidOperationException($"{operation} failed: {result.Error?.Message ?? "Unknown error."}");
    }
}

internal abstract class DiskScenarioBase : HotPathBenchmarkScenarioBase
{
    private readonly bool _mirrored;

    protected DiskScenarioBase(bool mirrored)
    {
        _mirrored = mirrored;
    }

    protected DiskServiceBenchmarkEnvironment Environment { get; private set; } = null!;

    public override IReadOnlyList<string> ObservedDirectories => Environment.ObservedRoots;

    public sealed override async Task InitializeAsync(CancellationToken cancellationToken)
    {
        Environment = _mirrored
            ? await DiskServiceBenchmarkEnvironment.CreateMirrorAsync(cancellationToken)
            : await DiskServiceBenchmarkEnvironment.CreateSingleAsync(cancellationToken);

        await InitializeCoreAsync(cancellationToken);
    }

    protected abstract Task InitializeCoreAsync(CancellationToken cancellationToken);

    public override async ValueTask DisposeAsync()
    {
        if (Environment is not null) {
            await Environment.DisposeAsync();
        }
    }
}

internal abstract class LoopbackHttpScenarioBase : HotPathBenchmarkScenarioBase
{
    protected LoopbackHttpBenchmarkEnvironment Environment { get; private set; } = null!;

    public override IReadOnlyList<string> ObservedDirectories => Environment.ObservedRoots;

    public sealed override async Task InitializeAsync(CancellationToken cancellationToken)
    {
        Environment = await LoopbackHttpBenchmarkEnvironment.CreateAsync(cancellationToken);
        await InitializeCoreAsync(cancellationToken);
    }

    protected abstract Task InitializeCoreAsync(CancellationToken cancellationToken);

    public override async ValueTask DisposeAsync()
    {
        if (Environment is not null) {
            await Environment.DisposeAsync();
        }
    }
}

internal sealed class MetadataLookupScenario : DiskScenarioBase
{
    private const string BucketName = "metadata-bucket";
    private const string Key = "docs/head-target.bin";
    private HeadObjectRequest? _request;

    public MetadataLookupScenario() : base(false)
    {
    }

    public override BenchmarkScenarioDefinition Definition => new()
    {
        Id = "disk-head-object-metadata",
        HotPath = "metadata lookup",
        Transport = "in-process service",
        ProviderTopology = "single disk backend",
        WorkloadDescription = "Head an existing disk-backed object through IStorageService.",
        PayloadBytes = BenchmarkPayloads.MetadataBytes
    };

    protected override async Task InitializeCoreAsync(CancellationToken cancellationToken)
    {
        await EnsureBucketExistsAsync(Environment.StorageService, BucketName, cancellationToken);
        await PutSeedObjectAsync(Environment.StorageService, BucketName, Key, BenchmarkPayloads.MetadataObject, cancellationToken);
    }

    public override Task PrepareIterationAsync(bool isWarmup, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _request = new HeadObjectRequest
        {
            BucketName = BucketName,
            Key = Key
        };
        return Task.CompletedTask;
    }

    public override async ValueTask ExecuteIterationAsync(CancellationToken cancellationToken)
    {
        var result = await Environment.StorageService.HeadObjectAsync(_request!, cancellationToken);
        _ = EnsureSuccess(result, "Head metadata object");
    }
}

internal sealed class DiskPutObjectScenario : DiskScenarioBase
{
    private const string BucketName = "upload-bucket";
    private const string Key = "payloads/upload.bin";
    private PutObjectRequest? _request;

    public DiskPutObjectScenario() : base(false)
    {
    }

    public override BenchmarkScenarioDefinition Definition => new()
    {
        Id = "disk-put-object",
        HotPath = "object upload",
        Transport = "in-process service",
        ProviderTopology = "single disk backend",
        WorkloadDescription = "Overwrite a 1 MiB object through the disk-backed IStorageService.",
        PayloadBytes = BenchmarkPayloads.UploadBytes
    };

    protected override async Task InitializeCoreAsync(CancellationToken cancellationToken)
    {
        await EnsureBucketExistsAsync(Environment.StorageService, BucketName, cancellationToken);
    }

    public override Task PrepareIterationAsync(bool isWarmup, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _request = new PutObjectRequest
        {
            BucketName = BucketName,
            Key = Key,
            Content = new MemoryStream(BenchmarkPayloads.UploadObject, writable: false),
            ContentLength = BenchmarkPayloads.UploadObject.Length,
            ContentType = "application/octet-stream",
            OverwriteIfExists = true
        };
        return Task.CompletedTask;
    }

    public override async ValueTask ExecuteIterationAsync(CancellationToken cancellationToken)
    {
        await using var content = _request!.Content;
        var result = await Environment.StorageService.PutObjectAsync(_request, cancellationToken);
        _ = EnsureSuccess(result, "Put upload object");
    }
}

internal sealed class DiskGetObjectScenario : DiskScenarioBase
{
    private const string BucketName = "download-bucket";
    private const string Key = "payloads/download.bin";
    private GetObjectRequest? _request;

    public DiskGetObjectScenario() : base(false)
    {
    }

    public override BenchmarkScenarioDefinition Definition => new()
    {
        Id = "disk-get-object",
        HotPath = "object download",
        Transport = "in-process service",
        ProviderTopology = "single disk backend",
        WorkloadDescription = "Stream a 4 MiB disk-backed object through IStorageService.",
        PayloadBytes = BenchmarkPayloads.DownloadBytes
    };

    protected override async Task InitializeCoreAsync(CancellationToken cancellationToken)
    {
        await EnsureBucketExistsAsync(Environment.StorageService, BucketName, cancellationToken);
        await PutSeedObjectAsync(Environment.StorageService, BucketName, Key, BenchmarkPayloads.DownloadObject, cancellationToken);
    }

    public override Task PrepareIterationAsync(bool isWarmup, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _request = new GetObjectRequest
        {
            BucketName = BucketName,
            Key = Key
        };
        return Task.CompletedTask;
    }

    public override async ValueTask ExecuteIterationAsync(CancellationToken cancellationToken)
    {
        var result = await Environment.StorageService.GetObjectAsync(_request!, cancellationToken);
        await using var response = EnsureSuccess(result, "Get download object");
        await response.Content.CopyToAsync(Stream.Null, cancellationToken);
    }
}

internal sealed class MultipartPartUploadScenario : DiskScenarioBase
{
    private const string BucketName = "multipart-part-bucket";
    private const string Key = "multipart/object.bin";
    private string _uploadId = string.Empty;
    private UploadMultipartPartRequest? _request;

    public MultipartPartUploadScenario() : base(false)
    {
    }

    public override BenchmarkScenarioDefinition Definition => new()
    {
        Id = "disk-upload-multipart-part",
        HotPath = "multipart part upload",
        Transport = "in-process service",
        ProviderTopology = "single disk backend",
        WorkloadDescription = "Upload a 2 MiB multipart part against an existing disk-backed upload.",
        PayloadBytes = BenchmarkPayloads.MultipartPartBytes
    };

    protected override async Task InitializeCoreAsync(CancellationToken cancellationToken)
    {
        await EnsureBucketExistsAsync(Environment.StorageService, BucketName, cancellationToken);
        var upload = EnsureSuccess(await Environment.StorageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = BucketName,
            Key = Key,
            ContentType = "application/octet-stream"
        }, cancellationToken), "Initiate multipart upload for part benchmark");
        _uploadId = upload.UploadId;
    }

    public override Task PrepareIterationAsync(bool isWarmup, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _request = new UploadMultipartPartRequest
        {
            BucketName = BucketName,
            Key = Key,
            UploadId = _uploadId,
            PartNumber = 1,
            Content = new MemoryStream(BenchmarkPayloads.MultipartPart, writable: false),
            ContentLength = BenchmarkPayloads.MultipartPart.Length
        };
        return Task.CompletedTask;
    }

    public override async ValueTask ExecuteIterationAsync(CancellationToken cancellationToken)
    {
        await using var content = _request!.Content;
        var result = await Environment.StorageService.UploadMultipartPartAsync(_request, cancellationToken);
        _ = EnsureSuccess(result, "Upload multipart part");
    }
}

internal sealed class MultipartCompleteScenario : DiskScenarioBase
{
    private const string BucketName = "multipart-complete-bucket";
    private const string Key = "multipart/complete.bin";
    private CompleteMultipartUploadRequest? _request;

    public MultipartCompleteScenario() : base(false)
    {
    }

    public override BenchmarkScenarioDefinition Definition => new()
    {
        Id = "disk-complete-multipart-upload",
        HotPath = "multipart complete",
        Transport = "in-process service",
        ProviderTopology = "single disk backend",
        WorkloadDescription = "Complete a three-part disk-backed multipart upload after the parts are staged.",
        PayloadBytes = BenchmarkPayloads.MultipartCompletePartBytes * 3
    };

    protected override async Task InitializeCoreAsync(CancellationToken cancellationToken)
    {
        await EnsureBucketExistsAsync(Environment.StorageService, BucketName, cancellationToken);
    }

    public override async Task PrepareIterationAsync(bool isWarmup, CancellationToken cancellationToken)
    {
        var upload = EnsureSuccess(await Environment.StorageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = BucketName,
            Key = Key,
            ContentType = "application/octet-stream"
        }, cancellationToken), "Initiate multipart upload for completion benchmark");

        var parts = new List<MultipartUploadPart>(capacity: 3);
        for (var partNumber = 1; partNumber <= 3; partNumber++) {
            await using var stream = new MemoryStream(BenchmarkPayloads.MultipartCompletePart, writable: false);
            var part = EnsureSuccess(await Environment.StorageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
            {
                BucketName = BucketName,
                Key = Key,
                UploadId = upload.UploadId,
                PartNumber = partNumber,
                Content = stream,
                ContentLength = BenchmarkPayloads.MultipartCompletePart.Length
            }, cancellationToken), $"Upload staged multipart part {partNumber}");
            parts.Add(part);
        }

        _request = new CompleteMultipartUploadRequest
        {
            BucketName = BucketName,
            Key = Key,
            UploadId = upload.UploadId,
            Parts = parts
        };
    }

    public override async ValueTask ExecuteIterationAsync(CancellationToken cancellationToken)
    {
        var result = await Environment.StorageService.CompleteMultipartUploadAsync(_request!, cancellationToken);
        _ = EnsureSuccess(result, "Complete multipart upload");
    }
}

internal sealed class MirroredWriteScenario : DiskScenarioBase
{
    private const string BucketName = "mirror-bucket";
    private const string Key = "mirrored/object.bin";
    private PutObjectRequest? _request;

    public MirroredWriteScenario() : base(true)
    {
    }

    public override BenchmarkScenarioDefinition Definition => new()
    {
        Id = "disk-mirror-put-object",
        HotPath = "mirrored writes",
        Transport = "in-process service",
        ProviderTopology = "write-through primary plus replica disk backends",
        WorkloadDescription = "Write-through overwrite of a 1 MiB object across primary and replica disk backends.",
        PayloadBytes = BenchmarkPayloads.UploadBytes
    };

    protected override async Task InitializeCoreAsync(CancellationToken cancellationToken)
    {
        await EnsureBucketExistsAsync(Environment.StorageService, BucketName, cancellationToken);
    }

    public override Task PrepareIterationAsync(bool isWarmup, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _request = new PutObjectRequest
        {
            BucketName = BucketName,
            Key = Key,
            Content = new MemoryStream(BenchmarkPayloads.UploadObject, writable: false),
            ContentLength = BenchmarkPayloads.UploadObject.Length,
            ContentType = "application/octet-stream",
            OverwriteIfExists = true
        };
        return Task.CompletedTask;
    }

    public override async ValueTask ExecuteIterationAsync(CancellationToken cancellationToken)
    {
        await using var content = _request!.Content;
        var result = await Environment.StorageService.PutObjectAsync(_request, cancellationToken);
        _ = EnsureSuccess(result, "Put mirrored object");
    }
}

internal sealed class ListObjectsScenario : DiskScenarioBase
{
    private const string BucketName = "list-bucket";
    private ListObjectsRequest? _request;

    public ListObjectsScenario() : base(false)
    {
    }

    public override BenchmarkScenarioDefinition Definition => new()
    {
        Id = "disk-list-objects",
        HotPath = "list operations",
        Transport = "in-process service",
        ProviderTopology = "single disk backend",
        WorkloadDescription = "Enumerate 256 disk-backed objects through IStorageService.",
        LogicalItemCount = BenchmarkPayloads.ListObjectCount
    };

    protected override async Task InitializeCoreAsync(CancellationToken cancellationToken)
    {
        await EnsureBucketExistsAsync(Environment.StorageService, BucketName, cancellationToken);
        for (var index = 0; index < BenchmarkPayloads.ListObjectCount; index++) {
            await PutSeedObjectAsync(Environment.StorageService, BucketName, $"list/object-{index:D4}.bin", BenchmarkPayloads.ListObject, cancellationToken);
        }
    }

    public override Task PrepareIterationAsync(bool isWarmup, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _request = new ListObjectsRequest
        {
            BucketName = BucketName,
            PageSize = BenchmarkPayloads.ListObjectCount
        };
        return Task.CompletedTask;
    }

    public override async ValueTask ExecuteIterationAsync(CancellationToken cancellationToken)
    {
        var count = 0;
        await foreach (var _ in Environment.StorageService.ListObjectsAsync(_request!, cancellationToken).WithCancellation(cancellationToken)) {
            count++;
        }

        if (count != BenchmarkPayloads.ListObjectCount) {
            throw new InvalidOperationException($"Expected {BenchmarkPayloads.ListObjectCount} listed objects, but observed {count}.");
        }
    }
}

internal sealed class PresignScenario : DiskScenarioBase
{
    private const string BucketName = "presign-bucket";
    private const string Key = "presign/object.bin";
    private StoragePresignRequest? _request;

    public PresignScenario() : base(false)
    {
    }

    public override BenchmarkScenarioDefinition Definition => new()
    {
        Id = "service-presign-get-object",
        HotPath = "presign generation",
        Transport = "in-process service",
        ProviderTopology = "first-party HTTP presign strategy",
        WorkloadDescription = "Generate a first-party GET-object presigned URL via IStoragePresignService."
    };

    protected override async Task InitializeCoreAsync(CancellationToken cancellationToken)
    {
        await EnsureBucketExistsAsync(Environment.StorageService, BucketName, cancellationToken);
        await PutSeedObjectAsync(Environment.StorageService, BucketName, Key, BenchmarkPayloads.MetadataObject, cancellationToken);
    }

    public override Task PrepareIterationAsync(bool isWarmup, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _request = new StoragePresignRequest
        {
            Operation = StoragePresignOperation.GetObject,
            BucketName = BucketName,
            Key = Key,
            ExpiresInSeconds = 300
        };
        return Task.CompletedTask;
    }

    public override async ValueTask ExecuteIterationAsync(CancellationToken cancellationToken)
    {
        var result = await Environment.PresignService.PresignObjectAsync(Environment.PresignPrincipal, _request!, cancellationToken);
        var presignedRequest = EnsureSuccess(result, "Generate presigned URL");
        if (!presignedRequest.Url.IsAbsoluteUri) {
            throw new InvalidOperationException("The generated presigned URL must be absolute.");
        }
    }
}

internal sealed class HttpSigV4HeadBucketScenario : LoopbackHttpScenarioBase
{
    private const string BucketName = "http-auth-bucket";
    private SigV4RequestTemplate? _template;
    private HttpRequestMessage? _request;

    public override BenchmarkScenarioDefinition Definition => new()
    {
        Id = "http-sigv4-head-bucket-auth",
        HotPath = "request auth/signature validation",
        Transport = "loopback HTTP",
        ProviderTopology = "ASP.NET endpoint plus disk backend",
        WorkloadDescription = "Send a SigV4-signed HEAD bucket request through the loopback HTTP host."
    };

    protected override async Task InitializeCoreAsync(CancellationToken cancellationToken)
    {
        await EnsureBucketExistsAsync(Environment.StorageService, BucketName, cancellationToken);
        _template = SigV4RequestTemplate.CreateHeaderSigned(HttpMethod.Head, Environment.Client.BaseAddress!, $"{BenchmarkDefaults.RoutePrefix}/buckets/{BucketName}");
    }

    public override Task PrepareIterationAsync(bool isWarmup, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _request = _template!.CreateRequestMessage();
        return Task.CompletedTask;
    }

    public override async ValueTask ExecuteIterationAsync(CancellationToken cancellationToken)
    {
        using var response = await Environment.Client.SendAsync(_request!, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        BenchmarkHttpProviderLatencyHeader.RecordFromResponse(response);
        if (response.StatusCode != HttpStatusCode.OK) {
            throw new InvalidOperationException($"Expected HTTP 200 from signed HEAD bucket request, but received {(int)response.StatusCode}.");
        }
    }
}

internal sealed class HttpPutObjectScenario : LoopbackHttpScenarioBase
{
    private const string BucketName = "http-put-bucket";
    private const string Key = "http/upload.bin";
    private SigV4RequestTemplate? _template;
    private HttpRequestMessage? _request;

    public override BenchmarkScenarioDefinition Definition => new()
    {
        Id = "http-put-object",
        HotPath = "object upload",
        Transport = "loopback HTTP",
        ProviderTopology = "ASP.NET endpoint plus disk backend",
        WorkloadDescription = "Send a SigV4-signed 1 MiB PUT object request through the loopback HTTP host.",
        PayloadBytes = BenchmarkPayloads.UploadBytes
    };

    protected override async Task InitializeCoreAsync(CancellationToken cancellationToken)
    {
        await EnsureBucketExistsAsync(Environment.StorageService, BucketName, cancellationToken);
        _template = SigV4RequestTemplate.CreateHeaderSigned(
            HttpMethod.Put,
            Environment.Client.BaseAddress!,
            $"{BenchmarkDefaults.RoutePrefix}/buckets/{BucketName}/objects/{Key}",
            BenchmarkPayloads.UploadObject,
            "application/octet-stream");
    }

    public override Task PrepareIterationAsync(bool isWarmup, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _request = _template!.CreateRequestMessage();
        return Task.CompletedTask;
    }

    public override async ValueTask ExecuteIterationAsync(CancellationToken cancellationToken)
    {
        using var response = await Environment.Client.SendAsync(_request!, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        BenchmarkHttpProviderLatencyHeader.RecordFromResponse(response);
        if (!response.IsSuccessStatusCode) {
            throw new InvalidOperationException($"Expected a successful HTTP PUT, but received {(int)response.StatusCode}.");
        }
    }
}

internal sealed class HttpGetObjectScenario : LoopbackHttpScenarioBase
{
    private const string BucketName = "http-get-bucket";
    private const string Key = "http/download.bin";
    private SigV4RequestTemplate? _template;
    private HttpRequestMessage? _request;

    public override BenchmarkScenarioDefinition Definition => new()
    {
        Id = "http-get-object",
        HotPath = "object download",
        Transport = "loopback HTTP",
        ProviderTopology = "ASP.NET endpoint plus disk backend",
        WorkloadDescription = "Send a SigV4-signed GET object request through the loopback HTTP host and drain the response body.",
        PayloadBytes = BenchmarkPayloads.DownloadBytes
    };

    protected override async Task InitializeCoreAsync(CancellationToken cancellationToken)
    {
        await EnsureBucketExistsAsync(Environment.StorageService, BucketName, cancellationToken);
        await PutSeedObjectAsync(Environment.StorageService, BucketName, Key, BenchmarkPayloads.DownloadObject, cancellationToken);
        _template = SigV4RequestTemplate.CreateHeaderSigned(HttpMethod.Get, Environment.Client.BaseAddress!, $"{BenchmarkDefaults.RoutePrefix}/buckets/{BucketName}/objects/{Key}");
    }

    public override Task PrepareIterationAsync(bool isWarmup, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _request = _template!.CreateRequestMessage();
        return Task.CompletedTask;
    }

    public override async ValueTask ExecuteIterationAsync(CancellationToken cancellationToken)
    {
        using var response = await Environment.Client.SendAsync(_request!, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        BenchmarkHttpProviderLatencyHeader.RecordFromResponse(response);
        if (!response.IsSuccessStatusCode) {
            throw new InvalidOperationException($"Expected a successful HTTP GET, but received {(int)response.StatusCode}.");
        }

        await response.Content.CopyToAsync(Stream.Null, cancellationToken);
    }
}

internal sealed class HttpListObjectsScenario : LoopbackHttpScenarioBase
{
    private const string BucketName = "http-list-bucket";
    private SigV4RequestTemplate? _template;
    private HttpRequestMessage? _request;

    public override BenchmarkScenarioDefinition Definition => new()
    {
        Id = "http-list-objects",
        HotPath = "list operations",
        Transport = "loopback HTTP",
        ProviderTopology = "ASP.NET endpoint plus disk backend",
        WorkloadDescription = "Send a SigV4-signed HTTP list-objects request and drain the serialized response body.",
        LogicalItemCount = BenchmarkPayloads.ListObjectCount
    };

    protected override async Task InitializeCoreAsync(CancellationToken cancellationToken)
    {
        await EnsureBucketExistsAsync(Environment.StorageService, BucketName, cancellationToken);
        for (var index = 0; index < BenchmarkPayloads.ListObjectCount; index++) {
            await PutSeedObjectAsync(Environment.StorageService, BucketName, $"http-list/object-{index:D4}.bin", BenchmarkPayloads.ListObject, cancellationToken);
        }

        _template = SigV4RequestTemplate.CreateHeaderSigned(
            HttpMethod.Get,
            Environment.Client.BaseAddress!,
            $"{BenchmarkDefaults.RoutePrefix}/buckets/{BucketName}/objects?pageSize={BenchmarkPayloads.ListObjectCount}");
    }

    public override Task PrepareIterationAsync(bool isWarmup, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _request = _template!.CreateRequestMessage();
        return Task.CompletedTask;
    }

    public override async ValueTask ExecuteIterationAsync(CancellationToken cancellationToken)
    {
        using var response = await Environment.Client.SendAsync(_request!, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        BenchmarkHttpProviderLatencyHeader.RecordFromResponse(response);
        if (!response.IsSuccessStatusCode) {
            throw new InvalidOperationException($"Expected a successful HTTP list operation, but received {(int)response.StatusCode}.");
        }

        await response.Content.CopyToAsync(Stream.Null, cancellationToken);
    }
}
