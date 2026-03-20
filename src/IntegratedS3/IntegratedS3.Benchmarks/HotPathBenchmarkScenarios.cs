using Amazon.Runtime;
using Amazon.S3;
using S3GetObjectRequest = Amazon.S3.Model.GetObjectRequest;
using S3GetObjectMetadataRequest = Amazon.S3.Model.GetObjectMetadataRequest;
using S3ListObjectsV2Request = Amazon.S3.Model.ListObjectsV2Request;
using S3PutObjectRequest = Amazon.S3.Model.PutObjectRequest;
using S3UploadPartRequest = Amazon.S3.Model.UploadPartRequest;
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

internal abstract class AwsSdkLoopbackScenarioBase : LoopbackHttpScenarioBase
{
    protected AmazonS3Client? S3Client { get; private set; }

    protected sealed override async Task InitializeCoreAsync(CancellationToken cancellationToken)
    {
        S3Client = CreateS3Client(Environment.Client.BaseAddress!);
        S3Client.AfterResponseEvent += CaptureProviderLatencyHeaders;

        try {
            await InitializeSdkScenarioAsync(cancellationToken);
        }
        catch {
            S3Client.AfterResponseEvent -= CaptureProviderLatencyHeaders;
            S3Client.Dispose();
            S3Client = null;
            throw;
        }
    }

    protected abstract Task InitializeSdkScenarioAsync(CancellationToken cancellationToken);

    public override async ValueTask DisposeAsync()
    {
        if (S3Client is not null) {
            S3Client.AfterResponseEvent -= CaptureProviderLatencyHeaders;
            S3Client.Dispose();
            S3Client = null;
        }

        await base.DisposeAsync();
    }

    protected AmazonS3Client GetS3Client()
    {
        return S3Client ?? throw new InvalidOperationException("The AWS SDK benchmark client has not been initialized.");
    }

    private static AmazonS3Client CreateS3Client(Uri baseAddress)
    {
        var serviceUrl = new Uri(baseAddress, BenchmarkDefaults.RoutePrefix).ToString().TrimEnd('/');
        return new AmazonS3Client(
            new BasicAWSCredentials(BenchmarkDefaults.AccessKeyId, BenchmarkDefaults.SecretAccessKey),
            new AmazonS3Config
            {
                ServiceURL = serviceUrl,
                ForcePathStyle = true,
                UseHttp = true,
                AuthenticationRegion = BenchmarkDefaults.Region
            });
    }

    private static void CaptureProviderLatencyHeaders(object? _, ResponseEventArgs eventArgs)
    {
        if (eventArgs is WebServiceResponseEventArgs responseEventArgs) {
            BenchmarkHttpProviderLatencyHeader.RecordFromHeaders(responseEventArgs.ResponseHeaders);
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

internal sealed class HttpHeadObjectScenario : LoopbackHttpScenarioBase
{
    private const string BucketName = "http-head-bucket";
    private const string Key = "http/head-target.bin";
    private SigV4RequestTemplate? _template;
    private HttpRequestMessage? _request;

    public override BenchmarkScenarioDefinition Definition => new()
    {
        Id = "http-head-object-metadata",
        HotPath = "metadata lookup",
        Transport = "loopback HTTP",
        ProviderTopology = "ASP.NET endpoint plus disk backend",
        WorkloadDescription = "Send a SigV4-signed HEAD object request through the loopback HTTP host.",
        PayloadBytes = BenchmarkPayloads.MetadataBytes
    };

    protected override async Task InitializeCoreAsync(CancellationToken cancellationToken)
    {
        await EnsureBucketExistsAsync(Environment.StorageService, BucketName, cancellationToken);
        await PutSeedObjectAsync(Environment.StorageService, BucketName, Key, BenchmarkPayloads.MetadataObject, cancellationToken);
        _template = SigV4RequestTemplate.CreateHeaderSigned(HttpMethod.Head, Environment.Client.BaseAddress!, $"{BenchmarkDefaults.RoutePrefix}/buckets/{BucketName}/objects/{Key}");
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
            throw new InvalidOperationException($"Expected HTTP 200 from signed HEAD object request, but received {(int)response.StatusCode}.");
        }

        if (response.Content.Headers.ContentLength != BenchmarkPayloads.MetadataBytes) {
            throw new InvalidOperationException($"Expected HEAD object content length {BenchmarkPayloads.MetadataBytes}, but received {response.Content.Headers.ContentLength?.ToString() ?? "<null>"}.");
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

internal sealed class HttpUploadMultipartPartScenario : LoopbackHttpScenarioBase
{
    private const string BucketName = "http-multipart-part-bucket";
    private const string Key = "http-multipart/object.bin";
    private const int PartNumber = 1;

    private SigV4RequestTemplate? _template;
    private HttpRequestMessage? _request;

    public override BenchmarkScenarioDefinition Definition => new()
    {
        Id = "http-upload-multipart-part",
        HotPath = "multipart part upload",
        Transport = "loopback HTTP",
        ProviderTopology = "ASP.NET endpoint plus disk backend",
        WorkloadDescription = "Send a SigV4-signed 2 MiB UploadPart request through the loopback HTTP host.",
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
        }, cancellationToken), "Initiate multipart upload for HTTP part benchmark");

        _template = SigV4RequestTemplate.CreateHeaderSigned(
            HttpMethod.Put,
            Environment.Client.BaseAddress!,
            $"{BenchmarkDefaults.RoutePrefix}/buckets/{BucketName}/objects/{Key}?uploadId={Uri.EscapeDataString(upload.UploadId)}&partNumber={PartNumber}",
            BenchmarkPayloads.MultipartPart,
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
            throw new InvalidOperationException($"Expected a successful HTTP multipart UploadPart request, but received {(int)response.StatusCode}.");
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

internal sealed class AwsSdkPutObjectScenario : AwsSdkLoopbackScenarioBase
{
    private const string BucketName = "aws-sdk-put-bucket";
    private const string Key = "sdk/upload.bin";
    private S3PutObjectRequest? _request;

    public override BenchmarkScenarioDefinition Definition => new()
    {
        Id = "aws-sdk-path-put-object",
        HotPath = "object upload",
        Transport = "AWS SDK path-style loopback HTTP",
        ProviderTopology = "ASP.NET S3-compatible endpoint plus disk backend",
        WorkloadDescription = "Upload a 1 MiB object through AmazonS3Client against the loopback S3-compatible endpoint.",
        PayloadBytes = BenchmarkPayloads.UploadBytes
    };

    protected override async Task InitializeSdkScenarioAsync(CancellationToken cancellationToken)
    {
        await EnsureBucketExistsAsync(Environment.StorageService, BucketName, cancellationToken);
    }

    public override Task PrepareIterationAsync(bool isWarmup, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _request = new S3PutObjectRequest
        {
            BucketName = BucketName,
            Key = Key,
            InputStream = new MemoryStream(BenchmarkPayloads.UploadObject, writable: false),
            ContentType = "application/octet-stream",
            UseChunkEncoding = false
        };
        return Task.CompletedTask;
    }

    public override async ValueTask ExecuteIterationAsync(CancellationToken cancellationToken)
    {
        var request = _request ?? throw new InvalidOperationException("The AWS SDK PUT benchmark request was not prepared.");
        _request = null;

        await using var inputStream = request.InputStream;
        var response = await GetS3Client().PutObjectAsync(request, cancellationToken);
        if (response.HttpStatusCode != HttpStatusCode.OK) {
            throw new InvalidOperationException($"Expected a successful AWS SDK PUT, but received {(int)response.HttpStatusCode}.");
        }
    }
}

internal sealed class AwsSdkGetObjectMetadataScenario : AwsSdkLoopbackScenarioBase
{
    private const string BucketName = "aws-sdk-head-bucket";
    private const string Key = "sdk/head-target.bin";
    private S3GetObjectMetadataRequest? _request;

    public override BenchmarkScenarioDefinition Definition => new()
    {
        Id = "aws-sdk-path-get-object-metadata",
        HotPath = "metadata lookup",
        Transport = "AWS SDK path-style loopback HTTP",
        ProviderTopology = "ASP.NET S3-compatible endpoint plus disk backend",
        WorkloadDescription = "Fetch object metadata through AmazonS3Client.GetObjectMetadataAsync(...) against the loopback S3-compatible endpoint.",
        PayloadBytes = BenchmarkPayloads.MetadataBytes
    };

    protected override async Task InitializeSdkScenarioAsync(CancellationToken cancellationToken)
    {
        await EnsureBucketExistsAsync(Environment.StorageService, BucketName, cancellationToken);
        await PutSeedObjectAsync(Environment.StorageService, BucketName, Key, BenchmarkPayloads.MetadataObject, cancellationToken);
    }

    public override Task PrepareIterationAsync(bool isWarmup, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _request = new S3GetObjectMetadataRequest
        {
            BucketName = BucketName,
            Key = Key
        };
        return Task.CompletedTask;
    }

    public override async ValueTask ExecuteIterationAsync(CancellationToken cancellationToken)
    {
        var request = _request ?? throw new InvalidOperationException("The AWS SDK metadata benchmark request was not prepared.");
        _request = null;

        var response = await GetS3Client().GetObjectMetadataAsync(request, cancellationToken);
        if (response.HttpStatusCode != HttpStatusCode.OK) {
            throw new InvalidOperationException($"Expected a successful AWS SDK metadata lookup, but received {(int)response.HttpStatusCode}.");
        }

        if (response.ContentLength != BenchmarkPayloads.MetadataBytes) {
            throw new InvalidOperationException($"Expected AWS SDK metadata content length {BenchmarkPayloads.MetadataBytes}, but received {response.ContentLength}.");
        }
    }
}

internal sealed class AwsSdkUploadMultipartPartScenario : AwsSdkLoopbackScenarioBase
{
    private const string BucketName = "aws-sdk-multipart-part-bucket";
    private const string Key = "sdk-multipart/object.bin";
    private const int PartNumber = 1;

    private string? _uploadId;
    private S3UploadPartRequest? _request;

    public override BenchmarkScenarioDefinition Definition => new()
    {
        Id = "aws-sdk-path-upload-multipart-part",
        HotPath = "multipart part upload",
        Transport = "AWS SDK path-style loopback HTTP",
        ProviderTopology = "ASP.NET S3-compatible endpoint plus disk backend",
        WorkloadDescription = "Upload a 2 MiB multipart part through AmazonS3Client against the loopback S3-compatible endpoint.",
        PayloadBytes = BenchmarkPayloads.MultipartPartBytes
    };

    protected override async Task InitializeSdkScenarioAsync(CancellationToken cancellationToken)
    {
        await EnsureBucketExistsAsync(Environment.StorageService, BucketName, cancellationToken);
        var upload = EnsureSuccess(await Environment.StorageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = BucketName,
            Key = Key,
            ContentType = "application/octet-stream"
        }, cancellationToken), "Initiate multipart upload for AWS SDK part benchmark");

        _uploadId = upload.UploadId;
    }

    public override Task PrepareIterationAsync(bool isWarmup, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _request = new S3UploadPartRequest
        {
            BucketName = BucketName,
            Key = Key,
            UploadId = _uploadId,
            PartNumber = PartNumber,
            InputStream = new MemoryStream(BenchmarkPayloads.MultipartPart, writable: false),
            PartSize = BenchmarkPayloads.MultipartPart.Length,
            IsLastPart = true,
            UseChunkEncoding = false
        };
        return Task.CompletedTask;
    }

    public override async ValueTask ExecuteIterationAsync(CancellationToken cancellationToken)
    {
        var request = _request ?? throw new InvalidOperationException("The AWS SDK UploadPart benchmark request was not prepared.");
        _request = null;

        await using var inputStream = request.InputStream;
        var response = await GetS3Client().UploadPartAsync(request, cancellationToken);
        if (response.HttpStatusCode != HttpStatusCode.OK) {
            throw new InvalidOperationException($"Expected a successful AWS SDK UploadPart request, but received {(int)response.HttpStatusCode}.");
        }
    }
}

internal sealed class AwsSdkGetObjectScenario : AwsSdkLoopbackScenarioBase
{
    private const string BucketName = "aws-sdk-get-bucket";
    private const string Key = "sdk/download.bin";
    private S3GetObjectRequest? _request;

    public override BenchmarkScenarioDefinition Definition => new()
    {
        Id = "aws-sdk-path-get-object",
        HotPath = "object download",
        Transport = "AWS SDK path-style loopback HTTP",
        ProviderTopology = "ASP.NET S3-compatible endpoint plus disk backend",
        WorkloadDescription = "Download and drain a 4 MiB object through AmazonS3Client against the loopback S3-compatible endpoint.",
        PayloadBytes = BenchmarkPayloads.DownloadBytes
    };

    protected override async Task InitializeSdkScenarioAsync(CancellationToken cancellationToken)
    {
        await EnsureBucketExistsAsync(Environment.StorageService, BucketName, cancellationToken);
        await PutSeedObjectAsync(Environment.StorageService, BucketName, Key, BenchmarkPayloads.DownloadObject, cancellationToken);
    }

    public override Task PrepareIterationAsync(bool isWarmup, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _request = new S3GetObjectRequest
        {
            BucketName = BucketName,
            Key = Key
        };
        return Task.CompletedTask;
    }

    public override async ValueTask ExecuteIterationAsync(CancellationToken cancellationToken)
    {
        var request = _request ?? throw new InvalidOperationException("The AWS SDK GET benchmark request was not prepared.");
        _request = null;

        using var response = await GetS3Client().GetObjectAsync(request, cancellationToken);
        if (response.HttpStatusCode != HttpStatusCode.OK) {
            throw new InvalidOperationException($"Expected a successful AWS SDK GET, but received {(int)response.HttpStatusCode}.");
        }

        await response.ResponseStream.CopyToAsync(Stream.Null, cancellationToken);
    }
}

internal sealed class AwsSdkListObjectsScenario : AwsSdkLoopbackScenarioBase
{
    private const string BucketName = "aws-sdk-list-bucket";
    private S3ListObjectsV2Request? _request;

    public override BenchmarkScenarioDefinition Definition => new()
    {
        Id = "aws-sdk-path-list-objects-v2",
        HotPath = "list operations",
        Transport = "AWS SDK path-style loopback HTTP",
        ProviderTopology = "ASP.NET S3-compatible endpoint plus disk backend",
        WorkloadDescription = "List 256 objects through AmazonS3Client against the loopback S3-compatible endpoint.",
        LogicalItemCount = BenchmarkPayloads.ListObjectCount
    };

    protected override async Task InitializeSdkScenarioAsync(CancellationToken cancellationToken)
    {
        await EnsureBucketExistsAsync(Environment.StorageService, BucketName, cancellationToken);
        for (var index = 0; index < BenchmarkPayloads.ListObjectCount; index++) {
            await PutSeedObjectAsync(Environment.StorageService, BucketName, $"sdk-list/object-{index:D4}.bin", BenchmarkPayloads.ListObject, cancellationToken);
        }
    }

    public override Task PrepareIterationAsync(bool isWarmup, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _request = new S3ListObjectsV2Request
        {
            BucketName = BucketName,
            MaxKeys = BenchmarkPayloads.ListObjectCount
        };
        return Task.CompletedTask;
    }

    public override async ValueTask ExecuteIterationAsync(CancellationToken cancellationToken)
    {
        var request = _request ?? throw new InvalidOperationException("The AWS SDK LIST benchmark request was not prepared.");
        _request = null;

        var response = await GetS3Client().ListObjectsV2Async(request, cancellationToken);
        if (response.HttpStatusCode != HttpStatusCode.OK) {
            throw new InvalidOperationException($"Expected a successful AWS SDK list operation, but received {(int)response.HttpStatusCode}.");
        }

        if (response.S3Objects.Count != BenchmarkPayloads.ListObjectCount) {
            throw new InvalidOperationException($"Expected {BenchmarkPayloads.ListObjectCount} listed objects, but received {response.S3Objects.Count}.");
        }
    }
}
