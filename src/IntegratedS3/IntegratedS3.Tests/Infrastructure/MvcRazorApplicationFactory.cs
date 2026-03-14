using IntegratedS3.AspNetCore;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using WebUi.MvcRazor;

namespace IntegratedS3.Tests.Infrastructure;

public sealed class MvcRazorApplicationFactory : IAsyncDisposable
{
    private readonly string _rootPath = Path.Combine(Path.GetTempPath(), "IntegratedS3.WebUi.MvcRazor.Tests", Guid.NewGuid().ToString("N"));
    private WebApplication? _application;
    private readonly List<IsolatedMvcRazorClient> _isolatedClients = [];

    public async Task<HttpClient> CreateClientAsync()
    {
        if (_application is not null) {
            return _application.GetTestClient();
        }

        _application = await CreateApplicationAsync(_rootPath, configureBuilder: null, configureIntegratedS3Endpoints: null);
        return _application.GetTestClient();
    }

    public async Task<IsolatedMvcRazorClient> CreateIsolatedClientAsync(
        Action<WebApplicationBuilder>? configureBuilder = null,
        Action<IntegratedS3EndpointOptions>? configureIntegratedS3Endpoints = null)
    {
        var rootPath = Path.Combine(Path.GetTempPath(), "IntegratedS3.WebUi.MvcRazor.Tests", Guid.NewGuid().ToString("N"));
        var application = await CreateApplicationAsync(rootPath, configureBuilder, configureIntegratedS3Endpoints);
        var client = new IsolatedMvcRazorClient(application, rootPath, application.GetTestClient());
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

        if (Directory.Exists(_rootPath)) {
            Directory.Delete(_rootPath, recursive: true);
        }
    }

    private static async Task<WebApplication> CreateApplicationAsync(
        string rootPath,
        Action<WebApplicationBuilder>? configureBuilder,
        Action<IntegratedS3EndpointOptions>? configureIntegratedS3Endpoints)
    {
        Directory.CreateDirectory(rootPath);

        var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions
        {
            EnvironmentName = Environments.Development,
            ApplicationName = typeof(MvcRazorApplication).Assembly.FullName,
            ContentRootPath = rootPath
        });

        builder.WebHost.UseTestServer();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["IntegratedS3:ServiceName"] = "Integrated S3 MVC/Razor Sample",
            ["IntegratedS3:RoutePrefix"] = "/integrated-s3",
            ["IntegratedS3:Disk:ProviderName"] = "test-disk",
            ["IntegratedS3:Disk:RootPath"] = rootPath,
            ["IntegratedS3:Disk:CreateRootDirectory"] = "true"
        });

        MvcRazorApplication.ConfigureServices(builder);
        configureBuilder?.Invoke(builder);

        var application = builder.Build();
        MvcRazorApplication.ConfigurePipeline(application, configureIntegratedS3Endpoints);
        await application.StartAsync();
        return application;
    }

    public sealed class IsolatedMvcRazorClient(WebApplication application, string rootPath, HttpClient client) : IAsyncDisposable
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

            if (Directory.Exists(rootPath)) {
                Directory.Delete(rootPath, recursive: true);
            }
        }
    }
}
