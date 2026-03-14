using System.Net.Http.Json;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Tests.Infrastructure;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class MvcRazorSampleTests(MvcRazorApplicationFactory factory) : IClassFixture<MvcRazorApplicationFactory>
{
    private readonly MvcRazorApplicationFactory _factory = factory;

    [Fact]
    public async Task StoragePage_RendersSeededBucketAndIntegratedS3Links()
    {
        await using var isolatedClient = await _factory.CreateIsolatedClientAsync();

        var response = await isolatedClient.Client.GetAsync("/");
        response.EnsureSuccessStatusCode();

        var html = await response.Content.ReadAsStringAsync();
        Assert.Contains("Integrated S3 MVC/Razor Sample", html, StringComparison.Ordinal);
        Assert.Contains("mvc-sample", html, StringComparison.Ordinal);
        Assert.Contains("welcome.txt", html, StringComparison.Ordinal);
        Assert.Contains("/integrated-s3", html, StringComparison.Ordinal);
    }

    [Fact]
    public async Task IntegratedS3Endpoints_RemainAvailableAlongsideMvcRoutes()
    {
        await using var isolatedClient = await _factory.CreateIsolatedClientAsync();

        var response = await isolatedClient.Client.GetAsync("/integrated-s3/buckets");
        response.EnsureSuccessStatusCode();

        var buckets = await response.Content.ReadFromJsonAsync<BucketInfo[]>();
        Assert.NotNull(buckets);
        Assert.Contains(buckets!, static bucket => bucket.Name == "mvc-sample");
    }
}
