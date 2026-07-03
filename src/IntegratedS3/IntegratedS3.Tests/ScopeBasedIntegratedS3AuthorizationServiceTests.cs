using System.Security.Claims;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Core.DependencyInjection;
using IntegratedS3.Core.Models;
using IntegratedS3.Core.Options;
using IntegratedS3.Core.Services;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class ScopeBasedIntegratedS3AuthorizationServiceTests
{
    private static IIntegratedS3AuthorizationService CreateService(
        Action<ScopeBasedIntegratedS3AuthorizationOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddIntegratedS3ScopeBasedAuthorization(configure);
        return services.BuildServiceProvider().GetRequiredService<IIntegratedS3AuthorizationService>();
    }

    private static ClaimsPrincipal PrincipalWithScopes(params string[] scopes)
    {
        var claims = scopes.Select(static scope => new Claim("scope", scope)).ToList();
        return new ClaimsPrincipal(new ClaimsIdentity(claims, authenticationType: "test"));
    }

    private static async Task<bool> IsAuthorizedAsync(
        IIntegratedS3AuthorizationService service,
        ClaimsPrincipal principal,
        StorageOperationType operation,
        string? bucketName = null,
        string? key = null)
    {
        var result = await service.AuthorizeAsync(
            principal,
            new StorageAuthorizationRequest { Operation = operation, BucketName = bucketName, Key = key });
        return result.IsSuccess;
    }

    [Fact]
    public async Task ReadScope_PermitsReadOperations()
    {
        var service = CreateService();
        var principal = PrincipalWithScopes("read");

        Assert.True(await IsAuthorizedAsync(service, principal, StorageOperationType.ListObjects, "bucket"));
        Assert.True(await IsAuthorizedAsync(service, principal, StorageOperationType.GetObject, "bucket", "key"));
        Assert.True(await IsAuthorizedAsync(service, principal, StorageOperationType.HeadObject, "bucket", "key"));
        Assert.True(await IsAuthorizedAsync(service, principal, StorageOperationType.ListBuckets));
    }

    [Fact]
    public async Task ReadScope_DeniesWriteOperations()
    {
        var service = CreateService();
        var principal = PrincipalWithScopes("read");

        Assert.False(await IsAuthorizedAsync(service, principal, StorageOperationType.PutObject, "bucket", "key"));
        Assert.False(await IsAuthorizedAsync(service, principal, StorageOperationType.DeleteObject, "bucket", "key"));
        Assert.False(await IsAuthorizedAsync(service, principal, StorageOperationType.CreateBucket, "bucket"));
    }

    [Fact]
    public async Task WriteScope_PermitsWriteButNotRead()
    {
        var service = CreateService();
        var principal = PrincipalWithScopes("write");

        Assert.True(await IsAuthorizedAsync(service, principal, StorageOperationType.PutObject, "bucket", "key"));
        Assert.True(await IsAuthorizedAsync(service, principal, StorageOperationType.DeleteObject, "bucket", "key"));
        Assert.False(await IsAuthorizedAsync(service, principal, StorageOperationType.GetObject, "bucket", "key"));
    }

    [Fact]
    public async Task AdminScope_PermitsEverything()
    {
        var service = CreateService();
        var principal = PrincipalWithScopes("admin");

        Assert.True(await IsAuthorizedAsync(service, principal, StorageOperationType.GetObject, "bucket", "key"));
        Assert.True(await IsAuthorizedAsync(service, principal, StorageOperationType.PutObject, "bucket", "key"));
        Assert.True(await IsAuthorizedAsync(service, principal, StorageOperationType.DeleteBucket, "bucket"));
    }

    [Fact]
    public async Task WildcardScope_PermitsEverything()
    {
        var service = CreateService();
        var principal = PrincipalWithScopes("*");

        Assert.True(await IsAuthorizedAsync(service, principal, StorageOperationType.DeleteObject, "bucket", "key"));
    }

    [Fact]
    public async Task BucketScope_PermitsOnlyMatchingBucket()
    {
        var service = CreateService();
        var principal = PrincipalWithScopes("bucket:allowed");

        Assert.True(await IsAuthorizedAsync(service, principal, StorageOperationType.PutObject, "allowed", "key"));
        Assert.True(await IsAuthorizedAsync(service, principal, StorageOperationType.GetObject, "allowed", "key"));
        Assert.False(await IsAuthorizedAsync(service, principal, StorageOperationType.GetObject, "other", "key"));
    }

    [Fact]
    public async Task BucketScopedRead_PermitsReadDeniesWriteOnBucket()
    {
        var service = CreateService();
        var principal = PrincipalWithScopes("bucket:reports:read");

        Assert.True(await IsAuthorizedAsync(service, principal, StorageOperationType.GetObject, "reports", "key"));
        Assert.False(await IsAuthorizedAsync(service, principal, StorageOperationType.PutObject, "reports", "key"));
        Assert.False(await IsAuthorizedAsync(service, principal, StorageOperationType.GetObject, "other", "key"));
    }

    [Fact]
    public async Task BucketScope_DoesNotGrantServiceLevelOperations()
    {
        var service = CreateService();
        var principal = PrincipalWithScopes("bucket:reports");

        // ListBuckets has no target bucket, so a bucket-scoped grant must not authorize it.
        Assert.False(await IsAuthorizedAsync(service, principal, StorageOperationType.ListBuckets));
    }

    [Fact]
    public async Task NoScopes_DeniedByDefault()
    {
        var service = CreateService();
        var principal = PrincipalWithScopes();

        Assert.False(await IsAuthorizedAsync(service, principal, StorageOperationType.GetObject, "bucket", "key"));
        Assert.False(await IsAuthorizedAsync(service, principal, StorageOperationType.ListBuckets));
    }

    [Fact]
    public async Task NoScopes_AllowedWhenUnscopedPrincipalsPermitted()
    {
        var service = CreateService(options => options.AllowUnscopedPrincipals = true);
        var principal = PrincipalWithScopes();

        Assert.True(await IsAuthorizedAsync(service, principal, StorageOperationType.PutObject, "bucket", "key"));
    }

    [Fact]
    public async Task MultipleScopes_GrantedWhenAnyMatches()
    {
        var service = CreateService();
        var principal = PrincipalWithScopes("read", "bucket:uploads:write");

        Assert.True(await IsAuthorizedAsync(service, principal, StorageOperationType.ListObjects, "anything"));
        Assert.True(await IsAuthorizedAsync(service, principal, StorageOperationType.PutObject, "uploads", "key"));
        Assert.False(await IsAuthorizedAsync(service, principal, StorageOperationType.PutObject, "other", "key"));
    }

    [Fact]
    public async Task Denial_ReportsAccessDeniedWith403()
    {
        var service = CreateService();
        var principal = PrincipalWithScopes("read");

        var result = await service.AuthorizeAsync(
            principal,
            new StorageAuthorizationRequest { Operation = StorageOperationType.PutObject, BucketName = "bucket", Key = "key" });

        Assert.False(result.IsSuccess);
        Assert.NotNull(result.Error);
        Assert.Equal(StorageErrorCode.AccessDenied, result.Error!.Code);
        Assert.Equal(403, result.Error.SuggestedHttpStatusCode);
    }
}
