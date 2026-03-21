using System.Runtime.CompilerServices;
using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Services;

namespace IntegratedS3.Testing;

/// <summary>
/// In-memory <see cref="IStorageMultipartStateStore" /> implementation for provider tests.
/// </summary>
/// <remarks>
/// This implementation is intended for unit and integration testing only. 
/// It is not thread-safe and should not be used as a production singleton service.
/// </remarks>
public sealed class InMemoryMultipartStateStore : IStorageMultipartStateStore
{
    private readonly Dictionary<(string ProviderName, string BucketName, string Key, string UploadId), MultipartUploadState> _states = new();

    /// <inheritdoc />
    public StorageSupportStateOwnership Ownership => StorageSupportStateOwnership.PlatformManaged;

    /// <inheritdoc />
    public ValueTask<MultipartUploadState?> GetMultipartUploadStateAsync(
        string providerName,
        string bucketName,
        string key,
        string uploadId,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(_states.TryGetValue((providerName, bucketName, key, uploadId), out var value) ? value : null);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<MultipartUploadState> ListMultipartUploadStatesAsync(
        string providerName,
        string bucketName,
        string? prefix = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var entry in _states.Where(existing => string.Equals(existing.Key.ProviderName, providerName, StringComparison.Ordinal)
                     && string.Equals(existing.Key.BucketName, bucketName, StringComparison.Ordinal)
                     && (string.IsNullOrWhiteSpace(prefix) || existing.Key.Key.StartsWith(prefix, StringComparison.Ordinal)))
                 .OrderBy(existing => existing.Value.Key, StringComparer.Ordinal)
                 .ThenBy(existing => existing.Value.InitiatedAtUtc)
                 .ThenBy(existing => existing.Value.UploadId, StringComparer.Ordinal)) {
            cancellationToken.ThrowIfCancellationRequested();
            yield return entry.Value;
            await Task.Yield();
        }
    }

    /// <inheritdoc />
    public ValueTask UpsertMultipartUploadStateAsync(string providerName, MultipartUploadState state, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _states[(providerName, state.BucketName, state.Key, state.UploadId)] = state;
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask RemoveMultipartUploadStateAsync(
        string providerName,
        string bucketName,
        string key,
        string uploadId,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _states.Remove((providerName, bucketName, key, uploadId));
        return ValueTask.CompletedTask;
    }
}
