using IntegratedS3.AspNetCore;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace IntegratedS3.Tests.Infrastructure;

public sealed class WebUiApplicationFactory : IAsyncDisposable
{
    private readonly string _rootPath = Path.Combine(Path.GetTempPath(), "IntegratedS3.WebUi.Tests", Guid.NewGuid().ToString("N"));
    private WebApplication? _application;
    private readonly List<IsolatedWebUiClient> _isolatedClients = [];

    public string RootPath => _rootPath;

    public async Task<HttpClient> CreateClientAsync()
    {
        if (_application is not null) {
            return _application.GetTestClient();
        }

        _application = await CreateApplicationAsync(_rootPath, configureBuilder: null, configureIntegratedS3Endpoints: null, configureConfiguration: null, useTestServer: true);

        return _application.GetTestClient();
    }

    public async Task<IsolatedWebUiClient> CreateIsolatedClientAsync(
        Action<WebApplicationBuilder>? configureBuilder = null,
        Action<IntegratedS3EndpointOptions>? configureIntegratedS3Endpoints = null,
        Action<ConfigurationManager>? configureConfiguration = null)
    {
        var rootPath = Path.Combine(Path.GetTempPath(), "IntegratedS3.WebUi.Tests", Guid.NewGuid().ToString("N"));
        var application = await CreateApplicationAsync(rootPath, configureBuilder, configureIntegratedS3Endpoints, configureConfiguration, useTestServer: true);
        var client = new IsolatedWebUiClient(application, rootPath, application.GetTestClient(), application.GetTestClient().BaseAddress);
        _isolatedClients.Add(client);
        return client;
    }

    public async Task<IsolatedWebUiClient> CreateLoopbackIsolatedClientAsync(
        Action<WebApplicationBuilder>? configureBuilder = null,
        Action<IntegratedS3EndpointOptions>? configureIntegratedS3Endpoints = null,
        Action<ConfigurationManager>? configureConfiguration = null)
    {
        var rootPath = Path.Combine(Path.GetTempPath(), "IntegratedS3.WebUi.Tests", Guid.NewGuid().ToString("N"));
        var application = await CreateApplicationAsync(rootPath, configureBuilder, configureIntegratedS3Endpoints, configureConfiguration, useTestServer: false);
        var address = application.Urls.SingleOrDefault();
        if (string.IsNullOrWhiteSpace(address)) {
            address = application.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()?.Addresses.SingleOrDefault();
        }

        if (string.IsNullOrWhiteSpace(address)) {
            await application.DisposeAsync();
            if (Directory.Exists(rootPath)) {
                Directory.Delete(rootPath, recursive: true);
            }

            throw new InvalidOperationException("The loopback test host did not expose an address.");
        }

        var client = new HttpClient
        {
            BaseAddress = new Uri(address, UriKind.Absolute)
        };

        var isolatedClient = new IsolatedWebUiClient(application, rootPath, client, client.BaseAddress);
        _isolatedClients.Add(isolatedClient);
        return isolatedClient;
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
        Action<IntegratedS3EndpointOptions>? configureIntegratedS3Endpoints,
        Action<ConfigurationManager>? configureConfiguration,
        bool useTestServer)
    {
        Directory.CreateDirectory(rootPath);

        var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions
        {
            EnvironmentName = Environments.Development,
            ApplicationName = typeof(WebUiApplication).Assembly.FullName,
            ContentRootPath = rootPath
        });

        if (useTestServer) {
            builder.WebHost.UseTestServer();
        }
        else {
            builder.WebHost.UseSetting(WebHostDefaults.ServerUrlsKey, "http://127.0.0.1:0");
        }
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["IntegratedS3:ServiceName"] = "Integrated S3 Sample Host",
            ["IntegratedS3:RoutePrefix"] = "/integrated-s3",
            ["IntegratedS3:Disk:ProviderName"] = "test-disk",
            ["IntegratedS3:Disk:RootPath"] = rootPath,
            ["IntegratedS3:Disk:CreateRootDirectory"] = "true"
        });
        configureConfiguration?.Invoke(builder.Configuration);

        WebUiApplication.ConfigureServices(builder);
        configureBuilder?.Invoke(builder);

        var application = builder.Build();
        if (configureIntegratedS3Endpoints is null) {
            WebUiApplication.ConfigurePipeline(application);
        }
        else {
            WebUiApplication.ConfigurePipeline(application, configureIntegratedS3Endpoints);
        }
        await application.StartAsync();
        return application;
    }

    public sealed class IsolatedWebUiClient(WebApplication application, string rootPath, HttpClient client, Uri? baseAddress) : IAsyncDisposable
    {
        private bool _disposed;

        public HttpClient Client { get; } = client;

        public Uri? BaseAddress { get; } = baseAddress;

        /// <summary>
        /// Creates an additional <see cref="HttpClient"/> backed by the same in-process test server.
        /// Useful when a test needs two independent clients (e.g. one with auth for presigning
        /// and one without auth for the actual object transfer).
        /// </summary>
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
