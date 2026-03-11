using System.Security.Claims;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Core.DependencyInjection;
using IntegratedS3.Core.Models;
using IntegratedS3.Core.Services;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class IntegratedS3PresignServiceTests
{
    [Fact]
    public async Task PresignObjectAsync_UsesPrincipalAuthorizationAndDelegatesToStrategy()
    {
        var authorizationService = new RecordingAuthorizationService(StorageResult.Success());
        var strategy = new RecordingPresignStrategy(StorageResult<StoragePresignedRequest>.Success(new StoragePresignedRequest
        {
            Operation = StoragePresignOperation.GetObject,
            AccessMode = StorageAccessMode.Proxy,
            Method = "GET",
            Url = new Uri("https://example.test/integrated-s3/buckets/docs", UriKind.Absolute),
            ExpiresAtUtc = DateTimeOffset.UtcNow.AddMinutes(5),
            BucketName = "docs",
            Key = "guide.txt"
        }));

        var services = new ServiceCollection();
        services.AddIntegratedS3Core();
        services.AddSingleton<IIntegratedS3AuthorizationService>(authorizationService);
        services.AddSingleton<IStoragePresignStrategy>(strategy);
        await using var serviceProvider = services.BuildServiceProvider();

        var presignService = serviceProvider.GetRequiredService<IStoragePresignService>();
        var principal = new ClaimsPrincipal(new ClaimsIdentity([new Claim("scope", "storage.read")], authenticationType: "Tests"));
        var request = new StoragePresignRequest
        {
            Operation = StoragePresignOperation.GetObject,
            BucketName = "docs",
            Key = "guide.txt",
            ExpiresInSeconds = 300
        };

        var result = await presignService.PresignObjectAsync(principal, request);

        Assert.True(result.IsSuccess);
        Assert.NotNull(authorizationService.LastRequest);
        Assert.Equal(StorageOperationType.PresignGetObject, authorizationService.LastRequest!.Operation);
        Assert.Equal("docs", authorizationService.LastRequest.BucketName);
        Assert.Equal("guide.txt", authorizationService.LastRequest.Key);
        Assert.Same(principal, strategy.LastPrincipal);
        Assert.Equal(request, strategy.LastRequest);
    }

    [Fact]
    public async Task PresignObjectAsync_ForwardsPreferredAccessModeToStrategy()
    {
        var delegatedUrl = new Uri("https://s3.us-east-1.amazonaws.com/docs/guide.txt?X-Amz-Signature=abc", UriKind.Absolute);
        var strategy = new RecordingPresignStrategy(StorageResult<StoragePresignedRequest>.Success(new StoragePresignedRequest
        {
            Operation = StoragePresignOperation.GetObject,
            AccessMode = StorageAccessMode.Delegated,
            Method = "GET",
            Url = delegatedUrl,
            ExpiresAtUtc = DateTimeOffset.UtcNow.AddMinutes(5),
            BucketName = "docs",
            Key = "guide.txt"
        }));

        var services = new ServiceCollection();
        services.AddIntegratedS3Core();
        services.AddSingleton<IIntegratedS3AuthorizationService>(new RecordingAuthorizationService(StorageResult.Success()));
        services.AddSingleton<IStoragePresignStrategy>(strategy);
        await using var serviceProvider = services.BuildServiceProvider();

        var presignService = serviceProvider.GetRequiredService<IStoragePresignService>();
        var principal = new ClaimsPrincipal(new ClaimsIdentity([new Claim("scope", "storage.read")], authenticationType: "Tests"));
        var request = new StoragePresignRequest
        {
            Operation = StoragePresignOperation.GetObject,
            BucketName = "docs",
            Key = "guide.txt",
            ExpiresInSeconds = 300,
            PreferredAccessMode = StorageAccessMode.Delegated
        };

        var result = await presignService.PresignObjectAsync(principal, request);

        Assert.True(result.IsSuccess);
        Assert.Equal(StorageAccessMode.Delegated, result.Value?.AccessMode);
        Assert.Equal(delegatedUrl, result.Value?.Url);
        Assert.Equal(StorageAccessMode.Delegated, strategy.LastRequest?.PreferredAccessMode);
    }

    [Fact]
    public void StoragePresignRequest_PreferredAccessMode_DefaultsToNull()
    {
        var request = new StoragePresignRequest
        {
            Operation = StoragePresignOperation.GetObject,
            BucketName = "docs",
            Key = "readme.txt",
            ExpiresInSeconds = 60
        };

        Assert.Null(request.PreferredAccessMode);
    }

    [Theory]
    [InlineData(StorageAccessMode.Proxy)]
    [InlineData(StorageAccessMode.Direct)]
    [InlineData(StorageAccessMode.Delegated)]
    public void StorageAccessMode_AllValuesRoundTrip(StorageAccessMode mode)
    {
        var response = new StoragePresignedRequest
        {
            Operation = StoragePresignOperation.GetObject,
            AccessMode = mode,
            Method = "GET",
            Url = new Uri("https://example.test/obj", UriKind.Absolute),
            ExpiresAtUtc = DateTimeOffset.UtcNow.AddMinutes(10),
            BucketName = "b",
            Key = "k"
        };

        Assert.Equal(mode, response.AccessMode);
    }

    [Fact]
    public async Task PresignObjectAsync_WhenAuthorizationFails_DoesNotInvokeStrategy()
    {
        var authorizationService = new RecordingAuthorizationService(StorageResult.Failure(new StorageError
        {
            Code = StorageErrorCode.AccessDenied,
            Message = "Missing scope.",
            SuggestedHttpStatusCode = 403
        }));
        var strategy = new RecordingPresignStrategy(StorageResult<StoragePresignedRequest>.Success(new StoragePresignedRequest
        {
            Operation = StoragePresignOperation.PutObject,
            AccessMode = StorageAccessMode.Proxy,
            Method = "PUT",
            Url = new Uri("https://example.test/integrated-s3/buckets/docs", UriKind.Absolute),
            ExpiresAtUtc = DateTimeOffset.UtcNow.AddMinutes(5),
            BucketName = "docs",
            Key = "guide.txt"
        }));

        var services = new ServiceCollection();
        services.AddIntegratedS3Core();
        services.AddSingleton<IIntegratedS3AuthorizationService>(authorizationService);
        services.AddSingleton<IStoragePresignStrategy>(strategy);
        await using var serviceProvider = services.BuildServiceProvider();

        var presignService = serviceProvider.GetRequiredService<IStoragePresignService>();
        var principal = new ClaimsPrincipal(new ClaimsIdentity(authenticationType: "Tests"));
        var request = new StoragePresignRequest
        {
            Operation = StoragePresignOperation.PutObject,
            BucketName = "docs",
            Key = "guide.txt",
            ExpiresInSeconds = 300,
            ContentType = "text/plain"
        };

        var result = await presignService.PresignObjectAsync(principal, request);

        Assert.False(result.IsSuccess);
        Assert.Equal(StorageErrorCode.AccessDenied, result.Error?.Code);
        Assert.Equal(0, strategy.InvocationCount);
    }

    private sealed class RecordingAuthorizationService(StorageResult result) : IIntegratedS3AuthorizationService
    {
        public StorageAuthorizationRequest? LastRequest { get; private set; }

        public ValueTask<StorageResult> AuthorizeAsync(ClaimsPrincipal principal, StorageAuthorizationRequest request, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(principal);
            cancellationToken.ThrowIfCancellationRequested();

            LastRequest = request;
            return ValueTask.FromResult(result);
        }
    }

    private sealed class RecordingPresignStrategy(StorageResult<StoragePresignedRequest> result) : IStoragePresignStrategy
    {
        public ClaimsPrincipal? LastPrincipal { get; private set; }

        public StoragePresignRequest? LastRequest { get; private set; }

        public int InvocationCount { get; private set; }

        public ValueTask<StorageResult<StoragePresignedRequest>> PresignObjectAsync(
            ClaimsPrincipal principal,
            StoragePresignRequest request,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(principal);
            ArgumentNullException.ThrowIfNull(request);
            cancellationToken.ThrowIfCancellationRequested();

            LastPrincipal = principal;
            LastRequest = request;
            InvocationCount++;
            return ValueTask.FromResult(result);
        }
    }
}
