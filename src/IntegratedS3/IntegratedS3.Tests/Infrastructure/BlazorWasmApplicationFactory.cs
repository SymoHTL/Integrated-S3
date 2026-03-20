using IntegratedS3.AspNetCore;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using WebUi.BlazorWasm;

namespace IntegratedS3.Tests.Infrastructure;

public sealed class BlazorWasmApplicationFactory : IAsyncDisposable
{
    private readonly string _storageRootPath = Path.Combine(Path.GetTempPath(), "IntegratedS3.WebUi.BlazorWasm.Tests", Guid.NewGuid().ToString("N"));
    private WebApplication? _application;
    private readonly List<IsolatedBlazorWasmClient> _isolatedClients = [];

    public async Task<HttpClient> CreateClientAsync()
    {
        if (_application is not null) {
            return _application.GetTestClient();
        }

        _application = await CreateApplicationAsync(_storageRootPath, configureBuilder: null, configureIntegratedS3Endpoints: null);
        return _application.GetTestClient();
    }

    public async Task<IsolatedBlazorWasmClient> CreateIsolatedClientAsync(
        Action<WebApplicationBuilder>? configureBuilder = null,
        Action<IntegratedS3EndpointOptions>? configureIntegratedS3Endpoints = null)
    {
        var storageRootPath = Path.Combine(Path.GetTempPath(), "IntegratedS3.WebUi.BlazorWasm.Tests", Guid.NewGuid().ToString("N"));
        var application = await CreateApplicationAsync(storageRootPath, configureBuilder, configureIntegratedS3Endpoints);
        var client = new IsolatedBlazorWasmClient(application, storageRootPath, application.GetTestClient());
        _isolatedClients.Add(client);
        return client;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var isolatedClient in _isolatedClients) {
            await isolatedClient.DisposeAsync();
        }

        if (_application is not null) {
            await _application.DisposeAsync();
        }

        if (Directory.Exists(_storageRootPath)) {
            Directory.Delete(_storageRootPath, recursive: true);
        }
    }

    private static async Task<WebApplication> CreateApplicationAsync(
        string storageRootPath,
        Action<WebApplicationBuilder>? configureBuilder,
        Action<IntegratedS3EndpointOptions>? configureIntegratedS3Endpoints)
    {
        Directory.CreateDirectory(storageRootPath);

        var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions
        {
            EnvironmentName = Environments.Development,
            ApplicationName = typeof(BlazorWasmApplication).Assembly.FullName,
            ContentRootPath = ResolveContentRootPath()
        });

        builder.WebHost.UseTestServer();
        builder.WebHost.UseSetting(WebHostDefaults.StaticWebAssetsKey, ResolveStaticWebAssetsManifestPath());
        builder.WebHost.UseStaticWebAssets();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["IntegratedS3:ServiceName"] = "Integrated S3 Blazor WebAssembly Sample",
            ["IntegratedS3:RoutePrefix"] = "/integrated-s3",
            ["IntegratedS3:Disk:ProviderName"] = "test-disk",
            ["IntegratedS3:Disk:RootPath"] = storageRootPath,
            ["IntegratedS3:Disk:CreateRootDirectory"] = "true"
        });

        BlazorWasmApplication.ConfigureServices(builder);
        configureBuilder?.Invoke(builder);

        var application = builder.Build();
        BlazorWasmApplication.ConfigurePipeline(application, configureIntegratedS3Endpoints);
        await application.StartAsync();
        return application;
    }

    private static string ResolveContentRootPath()
    {
        var contentRootPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "WebUi.BlazorWasm"));
        if (!Directory.Exists(contentRootPath)) {
            throw new DirectoryNotFoundException($"Unable to locate the Blazor WebAssembly sample content root at '{contentRootPath}'.");
        }

        return contentRootPath;
    }

    private static string ResolveStaticWebAssetsManifestPath()
    {
        var manifestPath = Path.Combine(AppContext.BaseDirectory, "WebUi.BlazorWasm.staticwebassets.runtime.json");
        if (!File.Exists(manifestPath)) {
            throw new FileNotFoundException($"Unable to locate the Blazor WebAssembly static web assets manifest at '{manifestPath}'.", manifestPath);
        }

        return manifestPath;
    }

    public sealed class IsolatedBlazorWasmClient(WebApplication application, string storageRootPath, HttpClient client) : IAsyncDisposable
    {
        private bool _disposed;

        public HttpClient Client { get; } = client;

        public IServiceProvider Services => application.Services;

        public HttpClient CreateAdditionalClient() => application.GetTestClient();

        public async ValueTask DisposeAsync()
        {
            if (_disposed) {
                return;
            }

            _disposed = true;
            Client.Dispose();
            await application.DisposeAsync();

            if (Directory.Exists(storageRootPath)) {
                Directory.Delete(storageRootPath, recursive: true);
            }
        }
    }
}
