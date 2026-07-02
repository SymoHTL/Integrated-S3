using IntegratedS3.AspNetCore;
using IntegratedS3.AspNetCore.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace IntegratedS3.Tests;

/// <summary>
/// Tests for the options-backed <see cref="IIntegratedS3CredentialResolver"/> covering exact-match
/// resolution and hot key rotation through configuration reloads (no application restart).
/// </summary>
public sealed class IntegratedS3CredentialResolverTests
{
    [Fact]
    public async Task ConfiguredResolver_ResolvesCredentialByExactAccessKeyId()
    {
        var services = new ServiceCollection();
        services.AddIntegratedS3(options => {
            options.EnableAwsSignatureV4Authentication = true;
            options.AccessKeyCredentials =
            [
                new IntegratedS3AccessKeyCredential
                {
                    AccessKeyId = "resolver-access",
                    SecretAccessKey = "resolver-secret",
                    Scopes = ["storage.read"]
                }
            ];
        });
        await using var serviceProvider = services.BuildServiceProvider();

        var resolver = serviceProvider.GetRequiredService<IIntegratedS3CredentialResolver>();

        var credential = await resolver.ResolveAsync("resolver-access");
        Assert.NotNull(credential);
        Assert.Equal("resolver-secret", credential!.SecretAccessKey);
        Assert.Contains("storage.read", credential.Scopes);

        Assert.Null(await resolver.ResolveAsync("unknown-access"));
        Assert.Null(await resolver.ResolveAsync("RESOLVER-ACCESS")); // access key ids compare ordinally
    }

    [Fact]
    public async Task ConfiguredResolver_ReflectsConfigurationReloadWithoutRestart()
    {
        var configurationSource = new MutableConfigurationSource();
        configurationSource.Provider.SetCredential("initial-access", "initial-secret");

        var configuration = new ConfigurationBuilder()
            .Add(configurationSource)
            .Build();

        var services = new ServiceCollection();
        services.AddIntegratedS3(configuration.GetSection("IntegratedS3"));
        await using var serviceProvider = services.BuildServiceProvider();

        var resolver = serviceProvider.GetRequiredService<IIntegratedS3CredentialResolver>();

        var initial = await resolver.ResolveAsync("initial-access");
        Assert.Equal("initial-secret", initial?.SecretAccessKey);
        Assert.Null(await resolver.ResolveAsync("rotated-access"));

        // Rotate the credential in the configuration source; the resolver observes the change
        // on the next lookup without rebuilding the service provider.
        configurationSource.Provider.SetCredential("rotated-access", "rotated-secret");

        Assert.Null(await resolver.ResolveAsync("initial-access"));
        var rotated = await resolver.ResolveAsync("rotated-access");
        Assert.Equal("rotated-secret", rotated?.SecretAccessKey);
    }

    [Fact]
    public async Task ConfiguredResolver_ThrowsForBlankAccessKeyId()
    {
        var services = new ServiceCollection();
        services.AddIntegratedS3(static options => options.EnableAwsSignatureV4Authentication = false);
        await using var serviceProvider = services.BuildServiceProvider();

        var resolver = serviceProvider.GetRequiredService<IIntegratedS3CredentialResolver>();

        await Assert.ThrowsAsync<ArgumentException>(() => resolver.ResolveAsync(" ").AsTask());
    }

    private sealed class MutableConfigurationSource : IConfigurationSource
    {
        public MutableConfigurationProvider Provider { get; } = new();

        public IConfigurationProvider Build(IConfigurationBuilder builder) => Provider;
    }

    /// <summary>
    /// A reloadable configuration provider standing in for sources like appsettings.json with
    /// <c>reloadOnChange</c>; <see cref="SetCredential"/> replaces the credential and raises a reload token.
    /// </summary>
    private sealed class MutableConfigurationProvider : ConfigurationProvider
    {
        public void SetCredential(string accessKeyId, string secretAccessKey)
        {
            Data = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["IntegratedS3:EnableAwsSignatureV4Authentication"] = "true",
                ["IntegratedS3:AccessKeyCredentials:0:AccessKeyId"] = accessKeyId,
                ["IntegratedS3:AccessKeyCredentials:0:SecretAccessKey"] = secretAccessKey
            };

            OnReload();
        }
    }
}
