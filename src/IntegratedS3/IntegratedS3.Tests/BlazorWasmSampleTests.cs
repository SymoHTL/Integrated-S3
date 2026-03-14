using IntegratedS3.Client;
using IntegratedS3.Tests.Infrastructure;
using WebUi.BlazorWasm.Client.Services;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class BlazorWasmSampleTests(BlazorWasmApplicationFactory factory) : IClassFixture<BlazorWasmApplicationFactory>
{
    private readonly BlazorWasmApplicationFactory _factory = factory;

    [Fact]
    public async Task RootPage_ServesBlazorApplicationShell()
    {
        await using var isolatedClient = await _factory.CreateIsolatedClientAsync();

        var response = await isolatedClient.Client.GetAsync("/");
        response.EnsureSuccessStatusCode();

        var html = await response.Content.ReadAsStringAsync();
        Assert.Contains("Integrated S3 Blazor WebAssembly Sample", html, StringComparison.Ordinal);
        Assert.Contains("_framework/blazor.webassembly", html, StringComparison.Ordinal);
    }

    [Fact]
    public async Task BrowserSampleService_CanUploadListAndDownloadThroughHostedSample()
    {
        await using var isolatedClient = await _factory.CreateIsolatedClientAsync();
        var browserSampleService = new IntegratedS3BrowserSampleService(
            isolatedClient.Client,
            new IntegratedS3Client(isolatedClient.CreateAdditionalClient()));

        var dashboard = await browserSampleService.GetDashboardAsync();
        Assert.Equal("Integrated S3 Blazor WebAssembly Sample", dashboard.ServiceName);
        Assert.Contains("test-disk", dashboard.ProviderNames);
        Assert.Contains(dashboard.Buckets, static bucket => bucket.Name == "browser-sample");

        var uploadResult = await browserSampleService.UploadTextAsync("browser-demo", "notes.txt", "Hello from the sample test.");
        Assert.Equal("browser-demo", uploadResult.BucketName);
        Assert.Equal("notes.txt", uploadResult.Key);
        Assert.True(uploadResult.Url.IsAbsoluteUri);

        var refreshedDashboard = await browserSampleService.GetDashboardAsync();
        var uploadedBucket = Assert.Single(refreshedDashboard.Buckets, static bucket => bucket.Name == "browser-demo");
        Assert.Contains(uploadedBucket.Objects, static objectEntry => objectEntry.Key == "notes.txt");

        var downloadResult = await browserSampleService.DownloadTextAsync("browser-demo", "notes.txt");
        Assert.Equal("Hello from the sample test.", downloadResult.Content);
    }
}
