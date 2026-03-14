using System.Diagnostics;
using System.Globalization;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading;
using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Abstractions.Services;

namespace IntegratedS3.Benchmarks;

internal static class StorageBackendProfilingContext
{
    private static readonly AsyncLocal<ProviderLatencyCollector?> CurrentCollector = new();

    public static IDisposable Begin(ProviderLatencyCollector collector)
    {
        ArgumentNullException.ThrowIfNull(collector);

        var previousCollector = CurrentCollector.Value;
        CurrentCollector.Value = collector;
        return new Scope(previousCollector);
    }

    public static void Record(string providerName, long elapsedTicks)
    {
        CurrentCollector.Value?.Record(providerName, elapsedTicks);
    }

    private sealed class Scope(ProviderLatencyCollector? previousCollector) : IDisposable
    {
        public void Dispose()
        {
            CurrentCollector.Value = previousCollector;
        }
    }
}

internal static class BenchmarkHttpProviderLatencyHeader
{
    public const string HeaderName = "x-integrateds3-benchmark-provider-latency";

    public static string Serialize(IReadOnlyDictionary<string, long> ticksByProvider)
    {
        ArgumentNullException.ThrowIfNull(ticksByProvider);

        return string.Join(';', ticksByProvider
            .OrderBy(static entry => entry.Key, StringComparer.Ordinal)
            .Select(static entry => $"{Uri.EscapeDataString(entry.Key)}={entry.Value.ToString(CultureInfo.InvariantCulture)}"));
    }

    public static void RecordFromResponse(HttpResponseMessage response)
    {
        ArgumentNullException.ThrowIfNull(response);

        if (!response.Headers.TryGetValues(HeaderName, out var headerValues)) {
            return;
        }

        foreach (var headerValue in headerValues) {
            foreach (var segment in headerValue.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)) {
                var parts = segment.Split('=', 2);
                if (parts.Length != 2 || !long.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var elapsedTicks)) {
                    continue;
                }

                StorageBackendProfilingContext.Record(Uri.UnescapeDataString(parts[0]), elapsedTicks);
            }
        }
    }
}

internal sealed class ProviderLatencyCollector
{
    private readonly Dictionary<string, long> _ticksByProvider = new(StringComparer.Ordinal);

    public void Record(string providerName, long elapsedTicks)
    {
        if (_ticksByProvider.TryGetValue(providerName, out var currentTicks)) {
            _ticksByProvider[providerName] = currentTicks + elapsedTicks;
            return;
        }

        _ticksByProvider[providerName] = elapsedTicks;
    }

    public IReadOnlyDictionary<string, long> SnapshotTicks()
    {
        return new Dictionary<string, long>(_ticksByProvider, StringComparer.Ordinal);
    }
}

public sealed class ProfilingStorageBackend(IStorageBackend inner) : IStorageBackend
{
    public string Name => inner.Name;

    public string Kind => inner.Kind;

    public bool IsPrimary => inner.IsPrimary;

    public string? Description => inner.Description;

    public async ValueTask<StorageCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default)
        => await MeasureAsync(inner.Name, inner.GetCapabilitiesAsync, cancellationToken);

    public async ValueTask<StorageSupportStateDescriptor> GetSupportStateDescriptorAsync(CancellationToken cancellationToken = default)
        => await MeasureAsync(inner.Name, inner.GetSupportStateDescriptorAsync, cancellationToken);

    public async ValueTask<StorageProviderMode> GetProviderModeAsync(CancellationToken cancellationToken = default)
        => await MeasureAsync(inner.Name, inner.GetProviderModeAsync, cancellationToken);

    public async ValueTask<StorageObjectLocationDescriptor> GetObjectLocationDescriptorAsync(CancellationToken cancellationToken = default)
        => await MeasureAsync(inner.Name, inner.GetObjectLocationDescriptorAsync, cancellationToken);

    public async ValueTask<StorageResult<StorageDirectObjectAccessGrant>> PresignObjectDirectAsync(StorageDirectObjectAccessRequest request, CancellationToken cancellationToken = default)
        => await MeasureAsync(inner.Name, static (backend, request, ct) => backend.PresignObjectDirectAsync(request, ct), inner, request, cancellationToken);

    public IAsyncEnumerable<BucketInfo> ListBucketsAsync(CancellationToken cancellationToken = default)
        => MeasureAsyncEnumerable(inner.Name, inner.ListBucketsAsync(cancellationToken), cancellationToken);

    public async ValueTask<StorageResult<BucketInfo>> CreateBucketAsync(CreateBucketRequest request, CancellationToken cancellationToken = default)
        => await MeasureAsync(inner.Name, static (backend, request, ct) => backend.CreateBucketAsync(request, ct), inner, request, cancellationToken);

    public async ValueTask<StorageResult<BucketVersioningInfo>> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default)
        => await MeasureAsync(inner.Name, static (backend, bucketName, ct) => backend.GetBucketVersioningAsync(bucketName, ct), inner, bucketName, cancellationToken);

    public async ValueTask<StorageResult<BucketVersioningInfo>> PutBucketVersioningAsync(PutBucketVersioningRequest request, CancellationToken cancellationToken = default)
        => await MeasureAsync(inner.Name, static (backend, request, ct) => backend.PutBucketVersioningAsync(request, ct), inner, request, cancellationToken);

    public async ValueTask<StorageResult<BucketCorsConfiguration>> GetBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default)
        => await MeasureAsync(inner.Name, static (backend, bucketName, ct) => backend.GetBucketCorsAsync(bucketName, ct), inner, bucketName, cancellationToken);

    public async ValueTask<StorageResult<BucketCorsConfiguration>> PutBucketCorsAsync(PutBucketCorsRequest request, CancellationToken cancellationToken = default)
        => await MeasureAsync(inner.Name, static (backend, request, ct) => backend.PutBucketCorsAsync(request, ct), inner, request, cancellationToken);

    public async ValueTask<StorageResult> DeleteBucketCorsAsync(DeleteBucketCorsRequest request, CancellationToken cancellationToken = default)
        => await MeasureAsync(inner.Name, static (backend, request, ct) => backend.DeleteBucketCorsAsync(request, ct), inner, request, cancellationToken);

    public async ValueTask<StorageResult<BucketInfo>> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default)
        => await MeasureAsync(inner.Name, static (backend, bucketName, ct) => backend.HeadBucketAsync(bucketName, ct), inner, bucketName, cancellationToken);

    public async ValueTask<StorageResult> DeleteBucketAsync(DeleteBucketRequest request, CancellationToken cancellationToken = default)
        => await MeasureAsync(inner.Name, static (backend, request, ct) => backend.DeleteBucketAsync(request, ct), inner, request, cancellationToken);

    public IAsyncEnumerable<ObjectInfo> ListObjectsAsync(ListObjectsRequest request, CancellationToken cancellationToken = default)
        => MeasureAsyncEnumerable(inner.Name, inner.ListObjectsAsync(request, cancellationToken), cancellationToken);

    public IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(ListObjectVersionsRequest request, CancellationToken cancellationToken = default)
        => MeasureAsyncEnumerable(inner.Name, inner.ListObjectVersionsAsync(request, cancellationToken), cancellationToken);

    public IAsyncEnumerable<MultipartUploadInfo> ListMultipartUploadsAsync(ListMultipartUploadsRequest request, CancellationToken cancellationToken = default)
        => MeasureAsyncEnumerable(inner.Name, inner.ListMultipartUploadsAsync(request, cancellationToken), cancellationToken);

    public async ValueTask<StorageResult<GetObjectResponse>> GetObjectAsync(GetObjectRequest request, CancellationToken cancellationToken = default)
        => await MeasureAsync(inner.Name, static (backend, request, ct) => backend.GetObjectAsync(request, ct), inner, request, cancellationToken);

    public async ValueTask<StorageResult<ObjectTagSet>> GetObjectTagsAsync(GetObjectTagsRequest request, CancellationToken cancellationToken = default)
        => await MeasureAsync(inner.Name, static (backend, request, ct) => backend.GetObjectTagsAsync(request, ct), inner, request, cancellationToken);

    public async ValueTask<StorageResult<ObjectInfo>> CopyObjectAsync(CopyObjectRequest request, CancellationToken cancellationToken = default)
        => await MeasureAsync(inner.Name, static (backend, request, ct) => backend.CopyObjectAsync(request, ct), inner, request, cancellationToken);

    public async ValueTask<StorageResult<ObjectInfo>> PutObjectAsync(PutObjectRequest request, CancellationToken cancellationToken = default)
        => await MeasureAsync(inner.Name, static (backend, request, ct) => backend.PutObjectAsync(request, ct), inner, request, cancellationToken);

    public async ValueTask<StorageResult<ObjectTagSet>> PutObjectTagsAsync(PutObjectTagsRequest request, CancellationToken cancellationToken = default)
        => await MeasureAsync(inner.Name, static (backend, request, ct) => backend.PutObjectTagsAsync(request, ct), inner, request, cancellationToken);

    public async ValueTask<StorageResult<ObjectTagSet>> DeleteObjectTagsAsync(DeleteObjectTagsRequest request, CancellationToken cancellationToken = default)
        => await MeasureAsync(inner.Name, static (backend, request, ct) => backend.DeleteObjectTagsAsync(request, ct), inner, request, cancellationToken);

    public async ValueTask<StorageResult<MultipartUploadInfo>> InitiateMultipartUploadAsync(InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default)
        => await MeasureAsync(inner.Name, static (backend, request, ct) => backend.InitiateMultipartUploadAsync(request, ct), inner, request, cancellationToken);

    public async ValueTask<StorageResult<MultipartUploadPart>> UploadMultipartPartAsync(UploadMultipartPartRequest request, CancellationToken cancellationToken = default)
        => await MeasureAsync(inner.Name, static (backend, request, ct) => backend.UploadMultipartPartAsync(request, ct), inner, request, cancellationToken);

    public async ValueTask<StorageResult<ObjectInfo>> CompleteMultipartUploadAsync(CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default)
        => await MeasureAsync(inner.Name, static (backend, request, ct) => backend.CompleteMultipartUploadAsync(request, ct), inner, request, cancellationToken);

    public async ValueTask<StorageResult> AbortMultipartUploadAsync(AbortMultipartUploadRequest request, CancellationToken cancellationToken = default)
        => await MeasureAsync(inner.Name, static (backend, request, ct) => backend.AbortMultipartUploadAsync(request, ct), inner, request, cancellationToken);

    public async ValueTask<StorageResult<ObjectInfo>> HeadObjectAsync(HeadObjectRequest request, CancellationToken cancellationToken = default)
        => await MeasureAsync(inner.Name, static (backend, request, ct) => backend.HeadObjectAsync(request, ct), inner, request, cancellationToken);

    public async ValueTask<StorageResult<DeleteObjectResult>> DeleteObjectAsync(DeleteObjectRequest request, CancellationToken cancellationToken = default)
        => await MeasureAsync(inner.Name, static (backend, request, ct) => backend.DeleteObjectAsync(request, ct), inner, request, cancellationToken);

    private static async ValueTask<T> MeasureAsync<T>(
        string providerName,
        Func<CancellationToken, ValueTask<T>> operation,
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        try {
            return await operation(cancellationToken);
        }
        finally {
            stopwatch.Stop();
            StorageBackendProfilingContext.Record(providerName, stopwatch.ElapsedTicks);
        }
    }

    private static async ValueTask<TResult> MeasureAsync<TState, TResult>(
        string providerName,
        Func<TState, CancellationToken, ValueTask<TResult>> operation,
        TState state,
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        try {
            return await operation(state, cancellationToken);
        }
        finally {
            stopwatch.Stop();
            StorageBackendProfilingContext.Record(providerName, stopwatch.ElapsedTicks);
        }
    }

    private static async ValueTask<TResult> MeasureAsync<TState1, TState2, TResult>(
        string providerName,
        Func<TState1, TState2, CancellationToken, ValueTask<TResult>> operation,
        TState1 state1,
        TState2 state2,
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        try {
            return await operation(state1, state2, cancellationToken);
        }
        finally {
            stopwatch.Stop();
            StorageBackendProfilingContext.Record(providerName, stopwatch.ElapsedTicks);
        }
    }

    private static async IAsyncEnumerable<T> MeasureAsyncEnumerable<T>(
        string providerName,
        IAsyncEnumerable<T> source,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        try {
            await foreach (var item in source.WithCancellation(cancellationToken)) {
                yield return item;
            }
        }
        finally {
            stopwatch.Stop();
            StorageBackendProfilingContext.Record(providerName, stopwatch.ElapsedTicks);
        }
    }
}

internal sealed class FileSystemActivityTracker : IDisposable
{
    private readonly FileSystemWatcher[] _watchers;
    private int _filesCreated;
    private int _filesDeleted;
    private int _filesRenamed;

    public FileSystemActivityTracker(IReadOnlyList<string> roots)
    {
        ArgumentNullException.ThrowIfNull(roots);

        _watchers = roots
            .Where(Directory.Exists)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Select(CreateWatcher)
            .ToArray();
    }

    public void Reset()
    {
        Interlocked.Exchange(ref _filesCreated, 0);
        Interlocked.Exchange(ref _filesDeleted, 0);
        Interlocked.Exchange(ref _filesRenamed, 0);
    }

    public async Task<FileSystemActivitySnapshot> CaptureAsync(CancellationToken cancellationToken)
    {
        await Task.Delay(25, cancellationToken);
        return new FileSystemActivitySnapshot(
            FilesCreated: Volatile.Read(ref _filesCreated),
            FilesDeleted: Volatile.Read(ref _filesDeleted),
            FilesRenamed: Volatile.Read(ref _filesRenamed));
    }

    public void Dispose()
    {
        foreach (var watcher in _watchers) {
            watcher.EnableRaisingEvents = false;
            watcher.Dispose();
        }
    }

    public static long MeasureDirectoryBytes(IReadOnlyList<string> roots)
    {
        ArgumentNullException.ThrowIfNull(roots);

        long totalBytes = 0;
        foreach (var root in roots.Where(Directory.Exists).Distinct(StringComparer.OrdinalIgnoreCase)) {
            foreach (var filePath in Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories)) {
                try {
                    totalBytes += new FileInfo(filePath).Length;
                }
                catch (FileNotFoundException) {
                }
            }
        }

        return totalBytes;
    }

    private FileSystemWatcher CreateWatcher(string rootPath)
    {
        var watcher = new FileSystemWatcher(rootPath)
        {
            IncludeSubdirectories = true,
            NotifyFilter = NotifyFilters.FileName | NotifyFilters.DirectoryName | NotifyFilters.Size | NotifyFilters.LastWrite,
            EnableRaisingEvents = true
        };

        watcher.Created += OnCreated;
        watcher.Deleted += OnDeleted;
        watcher.Renamed += OnRenamed;
        return watcher;
    }

    private void OnCreated(object sender, FileSystemEventArgs e)
    {
        Interlocked.Increment(ref _filesCreated);
    }

    private void OnDeleted(object sender, FileSystemEventArgs e)
    {
        Interlocked.Increment(ref _filesDeleted);
    }

    private void OnRenamed(object sender, RenamedEventArgs e)
    {
        Interlocked.Increment(ref _filesRenamed);
    }
}

internal sealed record FileSystemActivitySnapshot(int FilesCreated, int FilesDeleted, int FilesRenamed);

internal readonly record struct ThreadPoolSnapshot(
    long PendingWorkItems,
    int ThreadCount,
    int AvailableWorkerThreads,
    int AvailableIoThreads)
{
    public static ThreadPoolSnapshot Capture()
    {
        ThreadPool.GetAvailableThreads(out var availableWorkerThreads, out var availableIoThreads);
        return new ThreadPoolSnapshot(
            ThreadPool.PendingWorkItemCount,
            ThreadPool.ThreadCount,
            availableWorkerThreads,
            availableIoThreads);
    }
}

internal static class BenchmarkRuntimeMetrics
{
    public static long CaptureLargeObjectHeapSizeBytes()
    {
        var generationInfo = GC.GetGCMemoryInfo().GenerationInfo;
        return generationInfo.Length > 3
            ? generationInfo[3].SizeAfterBytes
            : 0;
    }
}
