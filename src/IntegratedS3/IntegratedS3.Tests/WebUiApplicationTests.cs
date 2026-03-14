using System.Net;
using System.Net.Http.Headers;
using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Tests.Infrastructure;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class WebUiApplicationTests
{
    [Fact]
    public async Task ConfigureServices_UsesS3StorageWhenReferenceHostSelectsS3()
    {
        var rootPath = CreateRootPath();
        Directory.CreateDirectory(rootPath);

        try {
            var builder = CreateBuilder(rootPath, new Dictionary<string, string?>
            {
                ["IntegratedS3:ReferenceHost:StorageProvider"] = "S3",
                ["IntegratedS3:S3:ProviderName"] = "reference-s3",
                ["IntegratedS3:S3:Region"] = "us-east-1",
                ["IntegratedS3:S3:ServiceUrl"] = "http://localhost:9000",
                ["IntegratedS3:S3:ForcePathStyle"] = "true",
                ["IntegratedS3:S3:AccessKey"] = "minioadmin",
                ["IntegratedS3:S3:SecretKey"] = "minioadmin"
            });

            WebUiApplication.ConfigureServices(builder);

            await using var application = builder.Build();
            var descriptorProvider = application.Services.GetRequiredService<IStorageServiceDescriptorProvider>();
            var descriptor = await descriptorProvider.GetServiceDescriptorAsync();

            var provider = Assert.Single(descriptor.Providers);
            Assert.Equal("reference-s3", provider.Name);
            Assert.Equal("s3", provider.Kind);
            Assert.True(provider.IsPrimary);
            Assert.Equal(StorageCapabilitySupport.Native, provider.Capabilities.PathStyleAddressing);
            Assert.Equal(StorageCapabilitySupport.Unsupported, provider.Capabilities.VirtualHostedStyleAddressing);
        }
        finally {
            if (Directory.Exists(rootPath)) {
                Directory.Delete(rootPath, recursive: true);
            }
        }
    }

    [Fact]
    public async Task ConfigurePipeline_AppliesConfiguredBucketRoutePolicy()
    {
        var rootPath = CreateRootPath();
        Directory.CreateDirectory(rootPath);
        WebApplication? application = null;

        try {
            var builder = CreateBuilder(rootPath, new Dictionary<string, string?>
            {
                ["IntegratedS3:ReferenceHost:RoutePolicies:Bucket"] = "ReferenceHostBucketWrite"
            });

            WebUiApplication.ConfigureServices(builder);
            ConfigureTestHeaderPolicy(builder.Services, "ReferenceHostBucketWrite", "bucket.write");

            application = builder.Build();
            WebUiApplication.ConfigurePipeline(application);
            await application.StartAsync();

            using var client = application.GetTestClient();

            var anonymousResponse = await client.PutAsync("/integrated-s3/buckets/configured-policy-bucket", content: null);
            Assert.Equal(HttpStatusCode.Unauthorized, anonymousResponse.StatusCode);

            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("TestHeader", "bucket.write");

            var authorizedResponse = await client.PutAsync("/integrated-s3/buckets/configured-policy-bucket", content: null);
            Assert.Equal(HttpStatusCode.Created, authorizedResponse.StatusCode);
        }
        finally {
            if (application is not null) {
                await application.DisposeAsync();
            }

            if (Directory.Exists(rootPath)) {
                Directory.Delete(rootPath, recursive: true);
            }
        }
    }

    private static WebApplicationBuilder CreateBuilder(string rootPath, IDictionary<string, string?> configurationValues)
    {
        ArgumentNullException.ThrowIfNull(rootPath);
        ArgumentNullException.ThrowIfNull(configurationValues);

        var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions
        {
            EnvironmentName = Environments.Development,
            ApplicationName = typeof(Program).Assembly.FullName,
            ContentRootPath = rootPath
        });

        builder.WebHost.UseTestServer();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["IntegratedS3:ServiceName"] = "Integrated S3 Sample Host",
            ["IntegratedS3:RoutePrefix"] = "/integrated-s3",
            ["IntegratedS3:Disk:ProviderName"] = "test-disk",
            ["IntegratedS3:Disk:RootPath"] = rootPath,
            ["IntegratedS3:Disk:CreateRootDirectory"] = "true"
        });
        builder.Configuration.AddInMemoryCollection(configurationValues);
        return builder;
    }

    private static void ConfigureTestHeaderPolicy(IServiceCollection services, string policyName, string requiredScope)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(policyName);
        ArgumentException.ThrowIfNullOrWhiteSpace(requiredScope);

        services.AddAuthentication("TestHeader")
            .AddScheme<AuthenticationSchemeOptions, TestHeaderAuthenticationHandler>("TestHeader", static _ => { });
        services.AddAuthorization(options => {
            options.AddPolicy(policyName, policy => {
                policy.AddAuthenticationSchemes("TestHeader");
                policy.RequireAuthenticatedUser();
                policy.RequireClaim("scope", requiredScope);
            });
        });
    }

    private static string CreateRootPath()
    {
        return Path.Combine(Path.GetTempPath(), "IntegratedS3.WebUi.ReferenceHost.Tests", Guid.NewGuid().ToString("N"));
    }
}
