using Amazon.Runtime;
using Amazon.S3;
using IntegratedS3.AspNetCore;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Xunit;

namespace IntegratedS3.E2E.Tests;

/// <summary>
/// Boots the real IntegratedS3 reference host on a Kestrel loopback socket with the Disk provider and a
/// seeded SigV4 credential, so end-to-end tests can drive it with a genuine AWS SDK S3 client over real HTTP.
/// Shared across the E2E collection; one host + temp disk root for the whole assembly.
/// </summary>
public sealed class E2EHostFixture : IAsyncLifetime
{
    public const string AccessKeyId = "e2e-access-key";
    public const string SecretAccessKey = "e2e-secret-key-0123456789abcdef01234567";
    public const string RoutePrefix = "/integrated-s3";
    public const string Region = "us-east-1";

    private WebApplication? _app;
    private string _rootPath = string.Empty;

    public Uri BaseAddress { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        _rootPath = Path.Combine(Path.GetTempPath(), "IntegratedS3.E2E", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_rootPath);

        var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions
        {
            EnvironmentName = Environments.Development,
            ApplicationName = typeof(WebUiApplication).Assembly.FullName,
            ContentRootPath = _rootPath,
        });
        builder.WebHost.UseSetting(WebHostDefaults.ServerUrlsKey, "http://127.0.0.1:0");
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["IntegratedS3:ServiceName"] = "IntegratedS3 E2E Host",
            ["IntegratedS3:RoutePrefix"] = RoutePrefix,
            ["IntegratedS3:Disk:ProviderName"] = "e2e-disk",
            ["IntegratedS3:Disk:RootPath"] = _rootPath,
            ["IntegratedS3:Disk:CreateRootDirectory"] = "true",
        });

        WebUiApplication.ConfigureServices(builder);
        builder.Services.Configure<IntegratedS3Options>(options =>
        {
            options.EnableAwsSignatureV4Authentication = true;
            options.AccessKeyCredentials.Add(new IntegratedS3AccessKeyCredential
            {
                AccessKeyId = AccessKeyId,
                SecretAccessKey = SecretAccessKey,
                DisplayName = "e2e",
                Scopes = { "storage.read", "storage.write" },
            });
        });

        _app = builder.Build();
        WebUiApplication.ConfigurePipeline(_app);
        await _app.StartAsync();

        var address = _app.Services.GetRequiredService<IServer>().Features
            .Get<IServerAddressesFeature>()!.Addresses.First();
        BaseAddress = new Uri(address);
    }

    /// <summary>Creates an AWS SDK S3 client pointed at the loopback host (path-style, HTTP).</summary>
    public IAmazonS3 CreateClient(string? accessKeyId = null, string? secretAccessKey = null)
    {
        var serviceUrl = new Uri(BaseAddress, RoutePrefix).ToString().TrimEnd('/');
        return new AmazonS3Client(
            new BasicAWSCredentials(accessKeyId ?? AccessKeyId, secretAccessKey ?? SecretAccessKey),
            new AmazonS3Config
            {
                ServiceURL = serviceUrl,
                ForcePathStyle = true,
                UseHttp = true,
                AuthenticationRegion = Region,
            });
    }

    /// <summary>A plain HttpClient for presigned-URL round trips (no ambient credentials).</summary>
    public HttpClient CreatePlainHttpClient() => new();

    public async Task DisposeAsync()
    {
        if (_app is not null)
        {
            await _app.StopAsync();
            await _app.DisposeAsync();
        }

        try
        {
            if (Directory.Exists(_rootPath))
            {
                Directory.Delete(_rootPath, recursive: true);
            }
        }
        catch (IOException)
        {
            // Best-effort cleanup of the E2E disk root.
        }
    }
}

/// <summary>xUnit collection so every E2E class shares one booted host.</summary>
[CollectionDefinition(Name)]
public sealed class E2ECollection : ICollectionFixture<E2EHostFixture>
{
    public const string Name = "IntegratedS3 E2E";
}
