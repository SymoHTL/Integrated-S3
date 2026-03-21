using IntegratedS3.AspNetCore;
using IntegratedS3.AspNetCore.DependencyInjection;
using IntegratedS3.AspNetCore.Endpoints;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class IntegratedS3StartupValidationTests
{
    [Fact]
    public void MapIntegratedS3Endpoints_ThrowsWhenNoBackendRegistered()
    {
        var rootPath = Path.Combine(Path.GetTempPath(), "IntegratedS3.ValidationTests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(rootPath);

        try
        {
            var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions
            {
                EnvironmentName = Environments.Development,
                ContentRootPath = rootPath
            });

            builder.WebHost.UseTestServer();
            builder.Services.AddIntegratedS3(options =>
            {
                options.RoutePrefix = "/integrated-s3";
            });

            var app = builder.Build();

            var exception = Assert.Throws<InvalidOperationException>(() => app.MapIntegratedS3Endpoints());
            Assert.Contains("No IStorageBackend is registered", exception.Message);
            Assert.Contains("AddDiskStorage()", exception.Message);
            Assert.Contains("AddIntegratedS3Backend<T>()", exception.Message);
        }
        finally
        {
            if (Directory.Exists(rootPath))
            {
                Directory.Delete(rootPath, recursive: true);
            }
        }
    }

    [Fact]
    public void OptionsValidation_FailsWhenSigV4EnabledWithoutCredentials()
    {
        var services = new ServiceCollection();
        services.AddIntegratedS3(options =>
        {
            options.EnableAwsSignatureV4Authentication = true;
            options.AccessKeyCredentials = [];
        });

        using var serviceProvider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            () => serviceProvider.GetRequiredService<IOptions<IntegratedS3Options>>().Value);
        Assert.Contains("EnableAwsSignatureV4Authentication is true", exception.Message);
        Assert.Contains("AccessKeyCredentials", exception.Message);
    }

    [Fact]
    public void OptionsValidation_FailsWhenRoutePrefixMissesLeadingSlash()
    {
        var validator = new IntegratedS3OptionsValidator();

        var result = validator.Validate(null, new IntegratedS3Options
        {
            RoutePrefix = "no-slash"
        });

        Assert.True(result.Failed);
        Assert.Contains("RoutePrefix must start with '/'", result.FailureMessage);
    }

    [Fact]
    public void OptionsValidation_FailsWhenClockSkewIsNotPositive()
    {
        var validator = new IntegratedS3OptionsValidator();

        var result = validator.Validate(null, new IntegratedS3Options
        {
            AllowedSignatureClockSkewMinutes = 0
        });

        Assert.True(result.Failed);
        Assert.Contains("AllowedSignatureClockSkewMinutes must be a positive integer", result.FailureMessage);
    }

    [Fact]
    public void OptionsValidation_FailsWhenPresignedUrlExpiryIsNotPositive()
    {
        var validator = new IntegratedS3OptionsValidator();

        var result = validator.Validate(null, new IntegratedS3Options
        {
            MaximumPresignedUrlExpirySeconds = -1
        });

        Assert.True(result.Failed);
        Assert.Contains("MaximumPresignedUrlExpirySeconds must be a positive integer", result.FailureMessage);
    }

    [Fact]
    public void OptionsValidation_SucceedsWithValidDefaults()
    {
        var validator = new IntegratedS3OptionsValidator();

        var result = validator.Validate(null, new IntegratedS3Options());

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void OptionsValidation_SucceedsWhenSigV4EnabledWithValidCredentials()
    {
        var validator = new IntegratedS3OptionsValidator();

        var result = validator.Validate(null, new IntegratedS3Options
        {
            EnableAwsSignatureV4Authentication = true,
            AccessKeyCredentials =
            [
                new IntegratedS3AccessKeyCredential
                {
                    AccessKeyId = "AKID",
                    SecretAccessKey = "secret"
                }
            ]
        });

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void OptionsValidation_FailsWhenSigV4EnabledWithOnlyBlankCredentials()
    {
        var validator = new IntegratedS3OptionsValidator();

        var result = validator.Validate(null, new IntegratedS3Options
        {
            EnableAwsSignatureV4Authentication = true,
            AccessKeyCredentials =
            [
                new IntegratedS3AccessKeyCredential
                {
                    AccessKeyId = "  ",
                    SecretAccessKey = ""
                }
            ]
        });

        Assert.True(result.Failed);
        Assert.Contains("AccessKeyCredentials", result.FailureMessage);
    }
}
