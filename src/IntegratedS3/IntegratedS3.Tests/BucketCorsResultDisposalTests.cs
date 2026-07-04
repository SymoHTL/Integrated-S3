using System.Runtime.CompilerServices;
using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.AspNetCore.Endpoints;
using IntegratedS3.AspNetCore.Services;
using IntegratedS3.Core.Options;
using IntegratedS3.Core.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace IntegratedS3.Tests;

/// <summary>
/// Regression tests for issue #128: when the bucket CORS wrapper performs throwing async I/O
/// (the actual-CORS lookup) BEFORE delegating to the inner result, the inner result — which on
/// the GET-object hot path already owns an opened <c>GetObjectResponse</c> / upstream connection —
/// must still be disposed. Otherwise every affected request leaks one upstream connection and one
/// <c>Activity</c>.
/// </summary>
public sealed class BucketCorsResultDisposalTests
{
    private const string OriginHeaderName = "Origin";

    [Fact]
    public async Task ExecuteWithBucketCorsAsync_DisposesInnerResult_WhenCorsLookupThrowsBeforeDelegation()
    {
        // The Origin header + GET method force GetActualResponseAsync to reach the throwing backend.
        var httpContext = CreateHttpContext(origin: "https://example.test", corsBackendThrows: true);
        var innerResult = new SpyDisposableResult();

        // The CORS pre-delegation work throws; the wrapper must surface the exception AND dispose the inner result.
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            IntegratedS3EndpointRouteBuilderExtensions.ExecuteWithBucketCorsAsync("bucket", innerResult, httpContext));

        Assert.False(innerResult.WasExecuted);
        Assert.Equal(1, innerResult.DisposeCount);
    }

    [Fact]
    public async Task ExecuteWithBucketCorsAsync_DoesNotDisposeInnerResult_WhenNoOriginPresent()
    {
        // Without an Origin header the CORS lookup short-circuits and never throws, so the inner
        // result is executed normally and owns its own disposal (control case: no double dispose).
        var httpContext = CreateHttpContext(origin: null, corsBackendThrows: true);
        var innerResult = new SpyDisposableResult();

        await IntegratedS3EndpointRouteBuilderExtensions.ExecuteWithBucketCorsAsync("bucket", innerResult, httpContext);

        Assert.True(innerResult.WasExecuted);
        Assert.Equal(0, innerResult.DisposeCount);
    }

    private static DefaultHttpContext CreateHttpContext(string? origin, bool corsBackendThrows)
    {
        var services = new ServiceCollection();
        services.AddSingleton(Options.Create(new IntegratedS3CoreOptions()));
        services.AddSingleton<IStorageBackendHealthEvaluator, AlwaysHealthyEvaluator>();
        services.AddSingleton<IStorageBackend>(new ThrowingCorsStorageBackend("primary", isPrimary: true, throwOnCors: corsBackendThrows));
        services.AddSingleton<BucketCorsRuntimeService>();

        var httpContext = new DefaultHttpContext
        {
            RequestServices = services.BuildServiceProvider()
        };
        httpContext.Request.Method = "GET";
        if (origin is not null) {
            httpContext.Request.Headers[OriginHeaderName] = origin;
        }

        return httpContext;
    }

    private sealed class SpyDisposableResult : IResult, IAsyncDisposable
    {
        public bool WasExecuted { get; private set; }

        public int DisposeCount { get; private set; }

        public Task ExecuteAsync(HttpContext httpContext)
        {
            WasExecuted = true;
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            DisposeCount++;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class AlwaysHealthyEvaluator : IStorageBackendHealthEvaluator
    {
        public ValueTask<StorageBackendHealthStatus> GetStatusAsync(IStorageBackend backend, CancellationToken cancellationToken = default)
            => ValueTask.FromResult(StorageBackendHealthStatus.Healthy);
    }

    // Minimal backend whose only interesting behaviour is throwing from the CORS-config getter,
    // reproducing a transient backend failure during the actual-CORS lookup on the GET hot path.
    private sealed class ThrowingCorsStorageBackend(string name, bool isPrimary, bool throwOnCors) : IStorageBackend
    {
        public string Name => name;
        public string Kind => "test";
        public bool IsPrimary => isPrimary;
        public string? Description => null;

        public ValueTask<StorageResult<BucketCorsConfiguration>> GetBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default)
        {
            if (throwOnCors) {
                throw new InvalidOperationException("Simulated transient CORS-config lookup failure (#128).");
            }

            return ValueTask.FromResult(StorageResult<BucketCorsConfiguration>.Failure(
                StorageError.Unsupported("CORS not configured.", bucketName)));
        }

        public ValueTask<StorageCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default) => ValueTask.FromResult(new StorageCapabilities());
        public ValueTask<StorageSupportStateDescriptor> GetSupportStateDescriptorAsync(CancellationToken cancellationToken = default) => ValueTask.FromResult(new StorageSupportStateDescriptor());
        public ValueTask<StorageProviderMode> GetProviderModeAsync(CancellationToken cancellationToken = default) => ValueTask.FromResult(StorageProviderMode.Managed);
        public ValueTask<StorageObjectLocationDescriptor> GetObjectLocationDescriptorAsync(CancellationToken cancellationToken = default) => ValueTask.FromResult(new StorageObjectLocationDescriptor());

        public async IAsyncEnumerable<BucketInfo> ListBucketsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }

        public async IAsyncEnumerable<ObjectInfo> ListObjectsAsync(ListObjectsRequest request, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }

        public async IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(ListObjectVersionsRequest request, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }

        private static NotSupportedException Boom() => new("Not used by the #128 disposal regression test.");

        public ValueTask<StorageResult<BucketInfo>> CreateBucketAsync(CreateBucketRequest request, CancellationToken cancellationToken = default) => throw Boom();
        public ValueTask<StorageResult<BucketVersioningInfo>> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default) => throw Boom();
        public ValueTask<StorageResult<BucketVersioningInfo>> PutBucketVersioningAsync(PutBucketVersioningRequest request, CancellationToken cancellationToken = default) => throw Boom();
        public ValueTask<StorageResult<BucketInfo>> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default) => throw Boom();
        public ValueTask<StorageResult> DeleteBucketAsync(DeleteBucketRequest request, CancellationToken cancellationToken = default) => throw Boom();
        public ValueTask<StorageResult<GetObjectResponse>> GetObjectAsync(GetObjectRequest request, CancellationToken cancellationToken = default) => throw Boom();
        public ValueTask<StorageResult<ObjectTagSet>> GetObjectTagsAsync(GetObjectTagsRequest request, CancellationToken cancellationToken = default) => throw Boom();
        public ValueTask<StorageResult<ObjectInfo>> CopyObjectAsync(CopyObjectRequest request, CancellationToken cancellationToken = default) => throw Boom();
        public ValueTask<StorageResult<ObjectInfo>> PutObjectAsync(PutObjectRequest request, CancellationToken cancellationToken = default) => throw Boom();
        public ValueTask<StorageResult<ObjectTagSet>> PutObjectTagsAsync(PutObjectTagsRequest request, CancellationToken cancellationToken = default) => throw Boom();
        public ValueTask<StorageResult<ObjectTagSet>> DeleteObjectTagsAsync(DeleteObjectTagsRequest request, CancellationToken cancellationToken = default) => throw Boom();
        public ValueTask<StorageResult<MultipartUploadInfo>> InitiateMultipartUploadAsync(InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default) => throw Boom();
        public ValueTask<StorageResult<MultipartUploadPart>> UploadMultipartPartAsync(UploadMultipartPartRequest request, CancellationToken cancellationToken = default) => throw Boom();
        public ValueTask<StorageResult<ObjectInfo>> CompleteMultipartUploadAsync(CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default) => throw Boom();
        public ValueTask<StorageResult> AbortMultipartUploadAsync(AbortMultipartUploadRequest request, CancellationToken cancellationToken = default) => throw Boom();
        public ValueTask<StorageResult<ObjectInfo>> HeadObjectAsync(HeadObjectRequest request, CancellationToken cancellationToken = default) => throw Boom();
        public ValueTask<StorageResult<DeleteObjectResult>> DeleteObjectAsync(DeleteObjectRequest request, CancellationToken cancellationToken = default) => throw Boom();
    }
}
