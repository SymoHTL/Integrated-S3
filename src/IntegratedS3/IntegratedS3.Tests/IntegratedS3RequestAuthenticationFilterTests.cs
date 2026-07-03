using System.Net;
using IntegratedS3.Tests.Infrastructure;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace IntegratedS3.Tests;

/// <summary>
/// Covers the fail-closed behavior of <c>IntegratedS3RequestAuthenticationEndpointFilter</c>: when
/// authentication is required, an unsigned request must be rejected with 403 instead of falling through
/// anonymously (ticket #82).
/// </summary>
public sealed class IntegratedS3RequestAuthenticationFilterTests
{
    private static Dictionary<string, string?> SigV4Enabled(bool allowAnonymous = false) => new()
    {
        ["IntegratedS3:EnableAwsSignatureV4Authentication"] = "true",
        ["IntegratedS3:AllowAnonymousRequests"] = allowAnonymous ? "true" : "false",
        ["IntegratedS3:AccessKeyCredentials:0:AccessKeyId"] = "AKIDEXAMPLE",
        ["IntegratedS3:AccessKeyCredentials:0:SecretAccessKey"] = "example-secret-key"
    };

    [Fact]
    public async Task UnsignedRequest_WithSigV4Enabled_IsRejectedWith403()
    {
        await using var factory = new WebUiApplicationFactory();
        var isolated = await factory.CreateIsolatedClientAsync(
            configureConfiguration: configuration => configuration.AddInMemoryCollection(SigV4Enabled()));

        var response = await isolated.Client.GetAsync("/integrated-s3/");

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("AccessDenied", body);
    }

    [Fact]
    public async Task UnsignedWriteRequest_WithSigV4Enabled_IsRejectedWith403()
    {
        await using var factory = new WebUiApplicationFactory();
        var isolated = await factory.CreateIsolatedClientAsync(
            configureConfiguration: configuration => configuration.AddInMemoryCollection(SigV4Enabled()));

        var response = await isolated.Client.PutAsync(
            "/integrated-s3/anonymous-bucket",
            new StringContent(string.Empty));

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
    }

    [Fact]
    public async Task UnsignedRequest_WithAllowAnonymousRequests_IsNotRejected()
    {
        await using var factory = new WebUiApplicationFactory();
        var isolated = await factory.CreateIsolatedClientAsync(
            configureConfiguration: configuration => configuration.AddInMemoryCollection(SigV4Enabled(allowAnonymous: true)));

        var response = await isolated.Client.GetAsync("/integrated-s3/");

        Assert.NotEqual(HttpStatusCode.Forbidden, response.StatusCode);
    }

    [Fact]
    public async Task UnsignedRequest_WithSigV4Disabled_IsNotRejected()
    {
        await using var factory = new WebUiApplicationFactory();
        var isolated = await factory.CreateIsolatedClientAsync(
            configureConfiguration: configuration => configuration.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["IntegratedS3:EnableAwsSignatureV4Authentication"] = "false"
            }));

        var response = await isolated.Client.GetAsync("/integrated-s3/");

        Assert.NotEqual(HttpStatusCode.Forbidden, response.StatusCode);
    }
}
