using System.Net;
using System.Net.Http.Json;
using IntegratedS3.Client;
using IntegratedS3.Client.DependencyInjection;
using IntegratedS3.Core.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class IntegratedS3ClientDependencyInjectionTests
{
    [Fact]
    public async Task AddIntegratedS3Client_BaseAddressOverload_RegistersNamedFactoryClientAndDefaultRoute()
    {
        var handler = new RecordingHandler();
        var services = new ServiceCollection();

        services.AddIntegratedS3Client(new Uri("https://integrated-s3.test/root", UriKind.Absolute))
            .ConfigureHttpClient(client => client.DefaultRequestHeaders.Add("X-IntegratedS3-Test", "configured"))
            .ConfigurePrimaryHttpMessageHandler(() => handler);

        using var provider = services.BuildServiceProvider();

        var factory = provider.GetRequiredService<IHttpClientFactory>();
        using var namedClient = factory.CreateClient(IntegratedS3ClientServiceCollectionExtensions.HttpClientName);
        Assert.Equal(new Uri("https://integrated-s3.test/root/"), namedClient.BaseAddress);
        Assert.True(namedClient.DefaultRequestHeaders.Contains("X-IntegratedS3-Test"));

        var client = provider.GetRequiredService<IIntegratedS3Client>();
        Assert.IsType<IntegratedS3Client>(provider.GetRequiredService<IntegratedS3Client>());

        _ = await client.PresignObjectAsync(CreatePresignRequest()).AsTask();

        Assert.Equal(
            new Uri("https://integrated-s3.test/root/integrated-s3/presign/object"),
            handler.LastRequest?.RequestUri);
        Assert.NotNull(handler.LastRequest);
        Assert.True(handler.LastRequest.Headers.TryGetValues("X-IntegratedS3-Test", out var headerValues));
        Assert.Contains("configured", headerValues);
    }

    [Fact]
    public async Task AddIntegratedS3Client_ConfigureOverload_BindsCustomRoutePrefix()
    {
        var handler = new RecordingHandler();
        var services = new ServiceCollection();

        services.AddIntegratedS3Client(options => {
            options.BaseAddress = new Uri("https://integrated-s3.test/service/", UriKind.Absolute);
            options.RoutePrefix = "/custom-client/";
        }).ConfigurePrimaryHttpMessageHandler(() => handler);

        using var provider = services.BuildServiceProvider();
        var client = provider.GetRequiredService<IntegratedS3Client>();

        _ = await client.PresignObjectAsync(CreatePresignRequest()).AsTask();

        Assert.Equal(
            new Uri("https://integrated-s3.test/service/custom-client/presign/object"),
            handler.LastRequest?.RequestUri);
    }

    [Fact]
    public async Task AddIntegratedS3Client_ConfigurationOverload_BindsDefaultConfigurationSection()
    {
        var handler = new RecordingHandler();
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                [$"{IntegratedS3ClientServiceCollectionExtensions.ConfigurationSectionName}:BaseAddress"] = "https://integrated-s3.test/app",
                [$"{IntegratedS3ClientServiceCollectionExtensions.ConfigurationSectionName}:RoutePrefix"] = "/bound-client/"
            })
            .Build();
        var services = new ServiceCollection();

        services.AddIntegratedS3Client(configuration)
            .ConfigurePrimaryHttpMessageHandler(() => handler);

        using var provider = services.BuildServiceProvider();
        var client = provider.GetRequiredService<IIntegratedS3Client>();

        _ = await client.PresignObjectAsync(CreatePresignRequest()).AsTask();

        Assert.Equal(
            new Uri("https://integrated-s3.test/app/bound-client/presign/object"),
            handler.LastRequest?.RequestUri);
    }

    private static StoragePresignRequest CreatePresignRequest()
    {
        return new StoragePresignRequest
        {
            Operation = StoragePresignOperation.GetObject,
            BucketName = "bucket",
            Key = "key",
            ExpiresInSeconds = 60
        };
    }

    private static StoragePresignedRequest CreatePresignedResponse()
    {
        return new StoragePresignedRequest
        {
            Operation = StoragePresignOperation.GetObject,
            AccessMode = StorageAccessMode.Proxy,
            Method = HttpMethod.Get.Method,
            Url = new Uri("https://integrated-s3.test/proxy/object", UriKind.Absolute),
            ExpiresAtUtc = DateTimeOffset.UtcNow.AddMinutes(5),
            BucketName = "bucket",
            Key = "key"
        };
    }

    private sealed class RecordingHandler : HttpMessageHandler
    {
        public HttpRequestMessage? LastRequest { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            LastRequest = request;

            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = JsonContent.Create(CreatePresignedResponse())
            });
        }
    }
}
