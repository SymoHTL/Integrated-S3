using IntegratedS3.Abstractions.Services;
using Microsoft.Extensions.DependencyInjection;

namespace IntegratedS3.Testing;

/// <summary>
/// Base fixture for provider contract tests. Provider-specific test projects supply the
/// service registration logic while the fixture manages service-provider lifecycle and restarts.
/// </summary>
public abstract class StorageProviderContractFixture : IAsyncDisposable
{
    private IServiceProvider? _services;
    private bool _disposed;

    /// <summary>
    /// Gets the service provider backing this fixture.
    /// </summary>
    public IServiceProvider Services
        => _services ?? throw new InvalidOperationException("The provider fixture has not been initialized.");

    /// <summary>
    /// Gets the configured storage backend under test.
    /// </summary>
    public IStorageBackend Backend => Services.GetRequiredService<IStorageBackend>();

    /// <summary>
    /// Gets whether the fixture has already built its service provider.
    /// </summary>
    public bool IsInitialized => _services is not null;

    /// <summary>
    /// Resolves a required service from the provider under test.
    /// </summary>
    public T GetRequiredService<T>() where T : notnull
    {
        return Services.GetRequiredService<T>();
    }

    /// <summary>
    /// Initializes the fixture when the constructor did not pre-build the service provider.
    /// </summary>
    public async Task InitializeAsync(Action<IServiceCollection>? configureServices = null, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        if (_services is not null) {
            throw new InvalidOperationException("The provider fixture is already initialized.");
        }

        _services = await CreateServiceProviderAsync(configureServices, cancellationToken);
    }

    /// <summary>
    /// Rebuilds the provider under test while preserving any fixture-owned external resources.
    /// </summary>
    public async Task RestartAsync(Action<IServiceCollection>? configureServices = null, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        if (_services is not null) {
            await DisposeServiceProviderAsync(_services);
            _services = null;
        }

        _services = await CreateServiceProviderAsync(configureServices, cancellationToken);
    }

    /// <summary>
    /// Allows derived fixtures with synchronous construction to seed the initial service provider.
    /// </summary>
    protected void SetServices(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        ThrowIfDisposed();

        if (_services is not null) {
            throw new InvalidOperationException("The provider fixture is already initialized.");
        }

        _services = services;
    }

    /// <summary>
    /// Builds the service provider for the storage backend under test.
    /// </summary>
    protected abstract ValueTask<IServiceProvider> CreateServiceProviderAsync(
        Action<IServiceCollection>? configureServices,
        CancellationToken cancellationToken);

    /// <summary>
    /// Releases fixture-owned resources after the service provider has been disposed.
    /// </summary>
    protected virtual ValueTask DisposeFixtureResourcesAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        if (_disposed) {
            return;
        }

        _disposed = true;

        if (_services is not null) {
            await DisposeServiceProviderAsync(_services);
            _services = null;
        }

        await DisposeFixtureResourcesAsync();
    }

    private void ThrowIfDisposed()
    {
        if (_disposed) {
            throw new ObjectDisposedException(GetType().FullName);
        }
    }

    private static async ValueTask DisposeServiceProviderAsync(IServiceProvider services)
    {
        if (services is IAsyncDisposable asyncDisposable) {
            await asyncDisposable.DisposeAsync();
            return;
        }

        if (services is IDisposable disposable) {
            disposable.Dispose();
        }
    }
}
