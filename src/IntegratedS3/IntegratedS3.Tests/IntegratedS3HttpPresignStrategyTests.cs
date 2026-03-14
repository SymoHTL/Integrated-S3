using System.Security.Claims;
using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.AspNetCore;
using IntegratedS3.AspNetCore.DependencyInjection;
using IntegratedS3.Core.Models;
using IntegratedS3.Core.Services;
using IntegratedS3.Provider.S3;
using IntegratedS3.Provider.S3.DependencyInjection;
using IntegratedS3.Provider.S3.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Xunit;

namespace IntegratedS3.Tests;

/// <summary>
/// Tests for the server-side presign strategy covering direct/delegated access-mode selection
/// and proxy fallback behavior. The strategy is <see langword="internal"/> so it is exercised
/// through the public <see cref="IStoragePresignService"/> surface wired via DI.
/// </summary>
public sealed class IntegratedS3HttpPresignStrategyTests
{
    private static readonly ClaimsPrincipal AnyPrincipal =
        new(new ClaimsIdentity([new Claim("scope", "storage.read")], authenticationType: "Tests"));

    // -------------------------------------------------------------------------
    // Delegated mode — resolver returns a provider-presigned URL
    // -------------------------------------------------------------------------

    [Fact]
    public async Task PresignObjectAsync_WhenDelegatedPreferred_AndResolverReturnsDelegated_ReturnsDelegated()
    {
        var delegatedUrl = new Uri("https://s3.us-east-1.amazonaws.com/bucket/key?X-Amz-Signature=abc", UriKind.Absolute);
        var expires = DateTimeOffset.UtcNow.AddMinutes(5);

        var presignService = BuildPresignService(
            new StubLocationResolver(new StorageResolvedObjectLocation
            {
                AccessMode = StorageObjectAccessMode.Delegated,
                Location = delegatedUrl,
                ExpiresAtUtc = expires
            }),
            enableSigV4: false);

        var request = new StoragePresignRequest
        {
            Operation = StoragePresignOperation.GetObject,
            BucketName = "bucket",
            Key = "key",
            ExpiresInSeconds = 300,
            PreferredAccessMode = StorageAccessMode.Delegated
        };

        var result = await presignService.PresignObjectAsync(AnyPrincipal, request);

        Assert.True(result.IsSuccess);
        Assert.Equal(StorageAccessMode.Delegated, result.Value?.AccessMode);
        Assert.Equal(delegatedUrl, result.Value?.Url);
        Assert.Equal(expires, result.Value?.ExpiresAtUtc);
        Assert.Equal("GET", result.Value?.Method);
        Assert.Equal("bucket", result.Value?.BucketName);
        Assert.Equal("key", result.Value?.Key);
    }

    [Fact]
    public async Task PresignObjectAsync_WhenDelegatedPreferred_AndResolverReturnsPassthrough_ReturnsDelegated()
    {
        var passthroughUrl = new Uri("https://cdn.example.com/bucket/key", UriKind.Absolute);

        var presignService = BuildPresignService(
            new StubLocationResolver(new StorageResolvedObjectLocation
            {
                AccessMode = StorageObjectAccessMode.Passthrough,
                Location = passthroughUrl
            }),
            enableSigV4: false);

        var request = new StoragePresignRequest
        {
            Operation = StoragePresignOperation.GetObject,
            BucketName = "bucket",
            Key = "key",
            ExpiresInSeconds = 300,
            PreferredAccessMode = StorageAccessMode.Delegated
        };

        var result = await presignService.PresignObjectAsync(AnyPrincipal, request);

        Assert.True(result.IsSuccess);
        Assert.Equal(StorageAccessMode.Delegated, result.Value?.AccessMode);
        Assert.Equal(passthroughUrl, result.Value?.Url);
    }

    [Fact]
    public async Task PresignObjectAsync_WhenDelegatedPreferred_ForwardsResolvedHeaders()
    {
        var resolvedHeaders = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["x-custom-token"] = "tok-abc",
            ["cache-control"] = "no-store"
        };

        var presignService = BuildPresignService(
            new StubLocationResolver(new StorageResolvedObjectLocation
            {
                AccessMode = StorageObjectAccessMode.Delegated,
                Location = new Uri("https://s3.example.com/bucket/key?sig=x", UriKind.Absolute),
                Headers = resolvedHeaders
            }),
            enableSigV4: false);

        var request = new StoragePresignRequest
        {
            Operation = StoragePresignOperation.GetObject,
            BucketName = "bucket",
            Key = "key",
            ExpiresInSeconds = 300,
            PreferredAccessMode = StorageAccessMode.Delegated
        };

        var result = await presignService.PresignObjectAsync(AnyPrincipal, request);

        Assert.True(result.IsSuccess);
        Assert.Contains(result.Value!.Headers, h => h.Name == "x-custom-token" && h.Value == "tok-abc");
        Assert.Contains(result.Value.Headers, h => h.Name == "cache-control" && h.Value == "no-store");
    }

    [Fact]
    public async Task PresignObjectAsync_WhenDelegatedPreferred_AndResolverHasNoExpiry_UsesRequestExpiry()
    {
        var fakeNow = new DateTimeOffset(2025, 6, 15, 10, 0, 0, TimeSpan.Zero);
        var fakeTime = new FakeTimeProvider(fakeNow);

        var presignService = BuildPresignService(
            new StubLocationResolver(new StorageResolvedObjectLocation
            {
                AccessMode = StorageObjectAccessMode.Delegated,
                Location = new Uri("https://s3.example.com/bucket/key?sig=x", UriKind.Absolute),
                ExpiresAtUtc = null
            }),
            enableSigV4: false,
            timeProvider: fakeTime);

        var request = new StoragePresignRequest
        {
            Operation = StoragePresignOperation.GetObject,
            BucketName = "bucket",
            Key = "key",
            ExpiresInSeconds = 600,
            PreferredAccessMode = StorageAccessMode.Delegated
        };

        var result = await presignService.PresignObjectAsync(AnyPrincipal, request);

        Assert.True(result.IsSuccess);
        Assert.Equal(fakeNow.AddSeconds(600), result.Value?.ExpiresAtUtc);
    }

    // -------------------------------------------------------------------------
    // Direct mode — resolver returns a plain redirect URL
    // -------------------------------------------------------------------------

    [Fact]
    public async Task PresignObjectAsync_WhenDirectPreferred_AndResolverReturnsRedirect_ReturnsDirect()
    {
        var directUrl = new Uri("https://public.example.com/bucket/key", UriKind.Absolute);

        var presignService = BuildPresignService(
            new StubLocationResolver(new StorageResolvedObjectLocation
            {
                AccessMode = StorageObjectAccessMode.Redirect,
                Location = directUrl
            }),
            enableSigV4: false);

        var request = new StoragePresignRequest
        {
            Operation = StoragePresignOperation.GetObject,
            BucketName = "bucket",
            Key = "key",
            ExpiresInSeconds = 300,
            PreferredAccessMode = StorageAccessMode.Direct
        };

        var result = await presignService.PresignObjectAsync(AnyPrincipal, request);

        Assert.True(result.IsSuccess);
        Assert.Equal(StorageAccessMode.Direct, result.Value?.AccessMode);
        Assert.Equal(directUrl, result.Value?.Url);
    }

    [Fact]
    public async Task PresignObjectAsync_WhenDirectPreferred_AndPrimaryBackendProvidesDirectGrant_ReturnsDirectWithoutCallingResolver()
    {
        var directUrl = new Uri("https://primary.example.com/bucket/key?sig=primary", UriKind.Absolute);
        var resolver = new RecordingLocationResolver(new StorageResolvedObjectLocation
        {
            AccessMode = StorageObjectAccessMode.Redirect,
            Location = new Uri("https://resolver.example.com/bucket/key", UriKind.Absolute)
        });
        var backend = new DirectPresignStorageBackend(
            "primary",
            isPrimary: true,
            StorageResult<StorageDirectObjectAccessGrant>.Success(new StorageDirectObjectAccessGrant
            {
                Url = directUrl,
                ExpiresAtUtc = DateTimeOffset.UtcNow.AddMinutes(10),
                Headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    ["x-provider-token"] = "abc"
                }
            }));

        var presignService = BuildPresignService(
            resolver,
            enableSigV4: false,
            backends: [backend]);

        var request = new StoragePresignRequest
        {
            Operation = StoragePresignOperation.GetObject,
            BucketName = "bucket",
            Key = "key",
            ExpiresInSeconds = 300,
            PreferredAccessMode = StorageAccessMode.Direct
        };

        var result = await presignService.PresignObjectAsync(AnyPrincipal, request);

        Assert.True(result.IsSuccess);
        Assert.Equal(StorageAccessMode.Direct, result.Value?.AccessMode);
        Assert.Equal(directUrl, result.Value?.Url);
        Assert.Contains(result.Value!.Headers, static header => header.Name == "x-provider-token" && header.Value == "abc");
        Assert.Equal(StorageDirectObjectAccessOperation.GetObject, backend.LastRequest?.Operation);
        Assert.Equal(0, resolver.CallCount);
    }

    [Fact]
    public async Task PresignObjectAsync_WhenPutDirectPreferred_AndPrimaryBackendProvidesDirectGrant_ReturnsDirect()
    {
        var backend = new DirectPresignStorageBackend(
            "primary",
            isPrimary: true,
            StorageResult<StorageDirectObjectAccessGrant>.Success(new StorageDirectObjectAccessGrant
            {
                Url = new Uri("https://primary.example.com/bucket/key?sig=put", UriKind.Absolute),
                ExpiresAtUtc = DateTimeOffset.UtcNow.AddMinutes(5),
                Headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    ["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD"
                }
            }));

        var presignService = BuildPresignService(
            new StubLocationResolver(resolvedLocation: null),
            enableSigV4: false,
            backends: [backend]);

        var request = new StoragePresignRequest
        {
            Operation = StoragePresignOperation.PutObject,
            BucketName = "bucket",
            Key = "key",
            ExpiresInSeconds = 300,
            ContentType = "text/plain",
            PreferredAccessMode = StorageAccessMode.Direct
        };

        var result = await presignService.PresignObjectAsync(AnyPrincipal, request);

        Assert.True(result.IsSuccess);
        Assert.Equal(StorageAccessMode.Direct, result.Value?.AccessMode);
        Assert.Equal("PUT", result.Value?.Method);
        Assert.Contains(result.Value!.Headers, static header => header.Name == "x-amz-content-sha256" && header.Value == "UNSIGNED-PAYLOAD");
        Assert.Equal(StorageDirectObjectAccessOperation.PutObject, backend.LastRequest?.Operation);
        Assert.Equal("text/plain", backend.LastRequest?.ContentType);
    }

    // -------------------------------------------------------------------------
    // Proxy fallback — resolver returns null or an incompatible mode
    // -------------------------------------------------------------------------

    [Theory]
    [InlineData(StorageAccessMode.Delegated)]
    [InlineData(StorageAccessMode.Direct)]
    public async Task PresignObjectAsync_WhenPreferredMode_AndResolverReturnsNull_FallsBackToProxy(StorageAccessMode preferred)
    {
        var presignService = BuildPresignService(
            new StubLocationResolver(resolvedLocation: null),
            enableSigV4: true);

        var request = new StoragePresignRequest
        {
            Operation = StoragePresignOperation.GetObject,
            BucketName = "bucket",
            Key = "key",
            ExpiresInSeconds = 300,
            PreferredAccessMode = preferred
        };

        var result = await presignService.PresignObjectAsync(AnyPrincipal, request);

        Assert.True(result.IsSuccess);
        Assert.Equal(StorageAccessMode.Proxy, result.Value?.AccessMode);
    }

    [Theory]
    [InlineData(StorageAccessMode.Delegated, StorageObjectAccessMode.Redirect)]   // Redirect is incompatible with Delegated preference
    [InlineData(StorageAccessMode.Direct, StorageObjectAccessMode.Delegated)]      // Delegated is incompatible with Direct preference
    [InlineData(StorageAccessMode.Direct, StorageObjectAccessMode.ProxyStream)]    // ProxyStream is incompatible with any non-proxy preference
    public async Task PresignObjectAsync_WhenResolverModeIncompatibleWithPreference_FallsBackToProxy(
        StorageAccessMode preferred, StorageObjectAccessMode resolvedMode)
    {
        var presignService = BuildPresignService(
            new StubLocationResolver(new StorageResolvedObjectLocation
            {
                AccessMode = resolvedMode,
                Location = new Uri("https://example.com/obj", UriKind.Absolute)
            }),
            enableSigV4: true);

        var request = new StoragePresignRequest
        {
            Operation = StoragePresignOperation.GetObject,
            BucketName = "bucket",
            Key = "key",
            ExpiresInSeconds = 300,
            PreferredAccessMode = preferred
        };

        var result = await presignService.PresignObjectAsync(AnyPrincipal, request);

        Assert.True(result.IsSuccess);
        Assert.Equal(StorageAccessMode.Proxy, result.Value?.AccessMode);
    }

    [Fact]
    public async Task PresignObjectAsync_WhenOnlyReplicaProvidesDirectGrant_FallsBackToProxy()
    {
        var primaryBackend = new NonPresigningStorageBackend("primary", isPrimary: true);
        var replicaBackend = new DirectPresignStorageBackend(
            "replica",
            isPrimary: false,
            StorageResult<StorageDirectObjectAccessGrant>.Success(new StorageDirectObjectAccessGrant
            {
                Url = new Uri("https://replica.example.com/bucket/key?sig=replica", UriKind.Absolute),
                ExpiresAtUtc = DateTimeOffset.UtcNow.AddMinutes(5)
            }));

        var presignService = BuildPresignService(
            new StubLocationResolver(resolvedLocation: null),
            enableSigV4: true,
            backends: [primaryBackend, replicaBackend]);

        var request = new StoragePresignRequest
        {
            Operation = StoragePresignOperation.GetObject,
            BucketName = "bucket",
            Key = "key",
            ExpiresInSeconds = 300,
            PreferredAccessMode = StorageAccessMode.Direct
        };

        var result = await presignService.PresignObjectAsync(AnyPrincipal, request);

        Assert.True(result.IsSuccess);
        Assert.Equal(StorageAccessMode.Proxy, result.Value?.AccessMode);
        Assert.Equal(0, replicaBackend.CallCount);
    }

    // -------------------------------------------------------------------------
    // Without a backend direct-presign capability, PutObject still takes the proxy
    // path because the location resolver only handles reads today.
    // -------------------------------------------------------------------------

    [Theory]
    [InlineData(StorageAccessMode.Delegated)]
    [InlineData(StorageAccessMode.Direct)]
    public async Task PresignObjectAsync_PutObject_WhenPreferredDelegatedOrDirect_DoesNotCallResolverAndFallsBackToProxy(
        StorageAccessMode preferred)
    {
        var resolver = new RecordingLocationResolver(new StorageResolvedObjectLocation
        {
            AccessMode = StorageObjectAccessMode.Delegated,
            Location = new Uri("https://s3.example.com/bucket/key?sig=x", UriKind.Absolute)
        });

        var presignService = BuildPresignService(resolver, enableSigV4: true);

        var request = new StoragePresignRequest
        {
            Operation = StoragePresignOperation.PutObject,
            BucketName = "bucket",
            Key = "key",
            ExpiresInSeconds = 300,
            ContentType = "text/plain",
            PreferredAccessMode = preferred
        };

        var result = await presignService.PresignObjectAsync(AnyPrincipal, request);

        Assert.True(result.IsSuccess);
        Assert.Equal(StorageAccessMode.Proxy, result.Value?.AccessMode);
        Assert.Equal(0, resolver.CallCount); // resolver must not be consulted for write presigns
    }

    [Fact]
    public async Task PresignObjectAsync_PutObjectWithChecksumHeaders_ReturnsSignedChecksumHeaders()
    {
        var presignService = BuildPresignService(
            new StubLocationResolver(resolvedLocation: null),
            enableSigV4: true);

        var request = new StoragePresignRequest
        {
            Operation = StoragePresignOperation.PutObject,
            BucketName = "bucket",
            Key = "key",
            ExpiresInSeconds = 300,
            ContentType = "application/octet-stream",
            ChecksumAlgorithm = "sha256",
            Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["sha256"] = "abc123=="
            }
        };

        var result = await presignService.PresignObjectAsync(AnyPrincipal, request);

        Assert.True(result.IsSuccess);
        Assert.Equal(StorageAccessMode.Proxy, result.Value?.AccessMode);
        Assert.Contains(result.Value!.Headers, static header => header.Name == "Content-Type" && header.Value == "application/octet-stream");
        Assert.Contains(result.Value.Headers, static header => header.Name == "x-amz-sdk-checksum-algorithm" && header.Value == "SHA256");
        Assert.Contains(result.Value.Headers, static header => header.Name == "x-amz-checksum-sha256" && header.Value == "abc123==");
    }

    // -------------------------------------------------------------------------
    // Default behavior — no preferred mode bypasses resolver entirely
    // -------------------------------------------------------------------------

    [Fact]
    public async Task PresignObjectAsync_WhenNoPreferredMode_ResolverIsNotCalledAndResultIsProxy()
    {
        var resolver = new RecordingLocationResolver(new StorageResolvedObjectLocation
        {
            AccessMode = StorageObjectAccessMode.Delegated,
            Location = new Uri("https://s3.example.com/bucket/key?sig=x", UriKind.Absolute)
        });

        var presignService = BuildPresignService(resolver, enableSigV4: true);

        var request = new StoragePresignRequest
        {
            Operation = StoragePresignOperation.GetObject,
            BucketName = "bucket",
            Key = "key",
            ExpiresInSeconds = 300
        };

        var result = await presignService.PresignObjectAsync(AnyPrincipal, request);

        Assert.True(result.IsSuccess);
        Assert.Equal(StorageAccessMode.Proxy, result.Value?.AccessMode);
        Assert.Equal(0, resolver.CallCount);
    }

    // -------------------------------------------------------------------------
    // Resolver request metadata forwarded from the presign request
    // -------------------------------------------------------------------------

    [Fact]
    public async Task PresignObjectAsync_WhenDelegatedWithVersionId_ForwardsProviderNameExpiryVersionToResolverAndResponse()
    {
        const string versionId = "v-123";
        var fakeNow = new DateTimeOffset(2025, 6, 15, 10, 0, 0, TimeSpan.Zero);
        ResolveObjectLocationRequest? capturedRequest = null;

        var presignService = BuildPresignService(
            new CapturingLocationResolver(r => {
                capturedRequest = r;
                return new StorageResolvedObjectLocation
                {
                    AccessMode = StorageObjectAccessMode.Delegated,
                    Location = new Uri("https://s3.example.com/bucket/key?versionId=v-123&sig=x", UriKind.Absolute)
                };
            }),
            enableSigV4: false,
            backends: [new NonPresigningStorageBackend("s3-primary", isPrimary: true)],
            timeProvider: new FakeTimeProvider(fakeNow));

        var request = new StoragePresignRequest
        {
            Operation = StoragePresignOperation.GetObject,
            BucketName = "bucket",
            Key = "key",
            VersionId = versionId,
            ExpiresInSeconds = 300,
            PreferredAccessMode = StorageAccessMode.Delegated
        };

        var result = await presignService.PresignObjectAsync(AnyPrincipal, request);

        Assert.True(result.IsSuccess);
        Assert.Equal(StorageAccessMode.Delegated, result.Value?.AccessMode);
        Assert.Equal("s3-primary", capturedRequest?.ProviderName);
        Assert.Equal(versionId, capturedRequest?.VersionId);
        Assert.Equal(fakeNow.AddSeconds(300), capturedRequest?.ExpiresAtUtc);
        Assert.Equal(versionId, result.Value?.VersionId);
        Assert.Equal(fakeNow.AddSeconds(300), result.Value?.ExpiresAtUtc);
    }

    // -------------------------------------------------------------------------
    // S3-backed delegated presign integration
    // -------------------------------------------------------------------------

    [Fact]
    public async Task PresignObjectAsync_WhenDelegatedPreferred_AndS3StorageRegistered_ReturnsProviderUrlAndForwardsExpiryVersion()
    {
        const string versionId = "v-123";
        var fakeNow = new DateTimeOffset(2025, 6, 15, 11, 0, 0, TimeSpan.Zero);
        var fakeClient = new FakeS3Client
        {
            PresignedGetObjectUrl = new Uri("https://s3.us-east-1.amazonaws.com/bucket/key?versionId=v-123&X-Amz-Signature=abc", UriKind.Absolute)
        };

        var presignService = BuildS3BackedPresignService(
            fakeClient,
            enableSigV4: false,
            timeProvider: new FakeTimeProvider(fakeNow));

        var request = new StoragePresignRequest
        {
            Operation = StoragePresignOperation.GetObject,
            BucketName = "bucket",
            Key = "key",
            VersionId = versionId,
            ExpiresInSeconds = 600,
            PreferredAccessMode = StorageAccessMode.Delegated
        };

        var result = await presignService.PresignObjectAsync(AnyPrincipal, request);

        Assert.True(result.IsSuccess);
        Assert.Equal(StorageAccessMode.Delegated, result.Value?.AccessMode);
        Assert.Equal(fakeClient.PresignedGetObjectUrl, result.Value?.Url);
        Assert.Equal(fakeNow.AddSeconds(600), result.Value?.ExpiresAtUtc);
        Assert.Equal(versionId, result.Value?.VersionId);
        Assert.Equal(1, fakeClient.PresignedGetObjectUrlCalls);
        Assert.Equal("bucket", fakeClient.LastPresignedBucketName);
        Assert.Equal("key", fakeClient.LastPresignedKey);
        Assert.Equal(versionId, fakeClient.LastPresignedVersionId);
        Assert.Equal(fakeNow.AddSeconds(600), fakeClient.LastPresignedExpiresAtUtc);
    }

    [Fact]
    public async Task PresignObjectAsync_WhenDirectPreferred_AndS3StorageOnlyProvidesDelegatedLocation_FallsBackToProxy()
    {
        var fakeClient = new FakeS3Client
        {
            PresignedGetObjectUrl = new Uri("https://s3.us-east-1.amazonaws.com/bucket/key?X-Amz-Signature=abc", UriKind.Absolute)
        };

        var presignService = BuildS3BackedPresignService(fakeClient, enableSigV4: true);

        var request = new StoragePresignRequest
        {
            Operation = StoragePresignOperation.GetObject,
            BucketName = "bucket",
            Key = "key",
            ExpiresInSeconds = 300,
            PreferredAccessMode = StorageAccessMode.Direct
        };

        var result = await presignService.PresignObjectAsync(AnyPrincipal, request);

        Assert.True(result.IsSuccess);
        Assert.Equal(StorageAccessMode.Proxy, result.Value?.AccessMode);
        Assert.Equal(1, fakeClient.PresignedGetObjectUrlCalls);
    }

    [Fact]
    public async Task PresignObjectAsync_WhenPutDelegatedPreferred_AndS3StorageRegistered_StaysProxyOnly()
    {
        var fakeClient = new FakeS3Client
        {
            PresignedGetObjectUrl = new Uri("https://s3.us-east-1.amazonaws.com/bucket/key?X-Amz-Signature=abc", UriKind.Absolute)
        };

        var presignService = BuildS3BackedPresignService(fakeClient, enableSigV4: true);

        var request = new StoragePresignRequest
        {
            Operation = StoragePresignOperation.PutObject,
            BucketName = "bucket",
            Key = "key",
            ExpiresInSeconds = 300,
            ContentType = "text/plain",
            PreferredAccessMode = StorageAccessMode.Delegated
        };

        var result = await presignService.PresignObjectAsync(AnyPrincipal, request);

        Assert.True(result.IsSuccess);
        Assert.Equal(StorageAccessMode.Proxy, result.Value?.AccessMode);
        Assert.Equal(0, fakeClient.PresignedGetObjectUrlCalls);
    }

    [Fact]
    public async Task PresignObjectAsync_WhenDelegatedPreferred_AndPrimaryBackendDoesNotMatchS3Resolver_FallsBackToProxy()
    {
        var fakeClient = new FakeS3Client
        {
            PresignedGetObjectUrl = new Uri("https://s3.us-east-1.amazonaws.com/bucket/key?X-Amz-Signature=abc", UriKind.Absolute)
        };

        var presignService = BuildS3BackedPresignService(
            fakeClient,
            enableSigV4: true,
            backends: [new NonPresigningStorageBackend("disk-primary", isPrimary: true)],
            options: new S3StorageOptions
            {
                ProviderName = "s3-replica",
                Region = "us-east-1",
                IsPrimary = false
            });

        var request = new StoragePresignRequest
        {
            Operation = StoragePresignOperation.GetObject,
            BucketName = "bucket",
            Key = "key",
            ExpiresInSeconds = 300,
            PreferredAccessMode = StorageAccessMode.Delegated
        };

        var result = await presignService.PresignObjectAsync(AnyPrincipal, request);

        Assert.True(result.IsSuccess);
        Assert.Equal(StorageAccessMode.Proxy, result.Value?.AccessMode);
        Assert.Equal(0, fakeClient.PresignedGetObjectUrlCalls);
    }

    // -------------------------------------------------------------------------
    // Helpers — build IStoragePresignService instances backed by the real HTTP strategy
    // -------------------------------------------------------------------------

    private static IStoragePresignService BuildPresignService(
        IStorageObjectLocationResolver resolver,
        bool enableSigV4,
        IEnumerable<IStorageBackend>? backends = null,
        TimeProvider? timeProvider = null)
    {
        var services = new ServiceCollection();

        services.AddIntegratedS3(options => {
            options.EnableAwsSignatureV4Authentication = enableSigV4;
            options.PresignPublicBaseUrl = "https://storage.example.com/";
            options.SignatureAuthenticationRegion = "us-east-1";
            options.SignatureAuthenticationService = "s3";
            options.MaximumPresignedUrlExpirySeconds = 3600;
            options.PresignAccessKeyId = "test-key";
            options.AccessKeyCredentials =
            [
                new IntegratedS3AccessKeyCredential
                {
                    AccessKeyId = "test-key",
                    SecretAccessKey = "test-secret"
                }
            ];
        });

        // Override the default null resolver with the test-supplied one.
        services.Replace(ServiceDescriptor.Singleton(resolver));

        if (backends is not null) {
            foreach (var backend in backends) {
                services.AddSingleton<IStorageBackend>(backend);
            }
        }

        if (timeProvider is not null) {
            services.Replace(ServiceDescriptor.Singleton(timeProvider));
        }

        var sp = services.BuildServiceProvider();
        return sp.GetRequiredService<IStoragePresignService>();
    }

    private static IStoragePresignService BuildS3BackedPresignService(
        FakeS3Client client,
        bool enableSigV4,
        IEnumerable<IStorageBackend>? backends = null,
        S3StorageOptions? options = null,
        TimeProvider? timeProvider = null)
    {
        var services = new ServiceCollection();

        services.AddIntegratedS3(configure => {
            configure.EnableAwsSignatureV4Authentication = enableSigV4;
            configure.PresignPublicBaseUrl = "https://storage.example.com/";
            configure.SignatureAuthenticationRegion = "us-east-1";
            configure.SignatureAuthenticationService = "s3";
            configure.MaximumPresignedUrlExpirySeconds = 3600;
            configure.PresignAccessKeyId = "test-key";
            configure.AccessKeyCredentials =
            [
                new IntegratedS3AccessKeyCredential
                {
                    AccessKeyId = "test-key",
                    SecretAccessKey = "test-secret"
                }
            ];
        });

        if (backends is not null) {
            foreach (var backend in backends) {
                services.AddSingleton<IStorageBackend>(backend);
            }
        }

        services.AddS3Storage(options ?? new S3StorageOptions
        {
            ProviderName = "s3-primary",
            Region = "us-east-1"
        });
        services.Replace(ServiceDescriptor.Singleton<IS3StorageClient>(client));

        if (timeProvider is not null) {
            services.Replace(ServiceDescriptor.Singleton(timeProvider));
        }

        var serviceProvider = services.BuildServiceProvider();
        return serviceProvider.GetRequiredService<IStoragePresignService>();
    }

    // -------------------------------------------------------------------------
    // Oversized expiry — exceeds MaximumPresignedUrlExpirySeconds
    // -------------------------------------------------------------------------

    [Fact]
    public async Task PresignObjectAsync_WhenExpiryExceedsMaximum_ReturnsFailureInsteadOfThrowing()
    {
        const int maxExpiry = 3600;
        const int oversizedExpiry = maxExpiry + 1;

        var presignService = BuildPresignService(
            new StubLocationResolver(resolvedLocation: null),
            enableSigV4: true);

        var request = new StoragePresignRequest
        {
            Operation = StoragePresignOperation.GetObject,
            BucketName = "bucket",
            Key = "key",
            ExpiresInSeconds = oversizedExpiry
        };

        var result = await presignService.PresignObjectAsync(AnyPrincipal, request);

        Assert.False(result.IsSuccess);
        Assert.Equal(StorageErrorCode.InvalidRange, result.Error?.Code);
        Assert.Equal(400, result.Error?.SuggestedHttpStatusCode);
        Assert.NotNull(result.Error?.Message);
        Assert.Contains(maxExpiry.ToString(), result.Error!.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PresignObjectAsync_WhenExpiryExactlyAtMaximum_Succeeds()
    {
        var presignService = BuildPresignService(
            new StubLocationResolver(resolvedLocation: null),
            enableSigV4: true);

        var request = new StoragePresignRequest
        {
            Operation = StoragePresignOperation.GetObject,
            BucketName = "bucket",
            Key = "key",
            ExpiresInSeconds = 3600  // exactly at the configured maximum
        };

        var result = await presignService.PresignObjectAsync(AnyPrincipal, request);

        Assert.True(result.IsSuccess);
        Assert.Equal(StorageAccessMode.Proxy, result.Value?.AccessMode);
    }

    [Theory]
    [InlineData(StorageAccessMode.Delegated)]
    [InlineData(StorageAccessMode.Direct)]
    public async Task PresignObjectAsync_WhenPreferredMode_AndExpiryExceedsMaximum_ReturnsFailure(StorageAccessMode preferred)
    {
        var presignService = BuildPresignService(
            new StubLocationResolver(resolvedLocation: null),
            enableSigV4: true);

        var request = new StoragePresignRequest
        {
            Operation = StoragePresignOperation.GetObject,
            BucketName = "bucket",
            Key = "key",
            ExpiresInSeconds = 9999,  // exceeds the 3600s maximum configured in BuildPresignService
            PreferredAccessMode = preferred
        };

        var result = await presignService.PresignObjectAsync(AnyPrincipal, request);

        Assert.False(result.IsSuccess);
        Assert.Equal(StorageErrorCode.InvalidRange, result.Error?.Code);
        Assert.Equal(400, result.Error?.SuggestedHttpStatusCode);
    }

    // -------------------------------------------------------------------------
    // Test doubles
    // -------------------------------------------------------------------------

    private sealed class StubLocationResolver(StorageResolvedObjectLocation? resolvedLocation) : IStorageObjectLocationResolver
    {
        public StorageSupportStateOwnership Ownership => StorageSupportStateOwnership.NotApplicable;

        public ValueTask<StorageResolvedObjectLocation?> ResolveReadLocationAsync(
            ResolveObjectLocationRequest request,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(request);
            return ValueTask.FromResult(resolvedLocation);
        }
    }

    private sealed class RecordingLocationResolver(StorageResolvedObjectLocation resolvedLocation) : IStorageObjectLocationResolver
    {
        public int CallCount { get; private set; }

        public StorageSupportStateOwnership Ownership => StorageSupportStateOwnership.NotApplicable;

        public ValueTask<StorageResolvedObjectLocation?> ResolveReadLocationAsync(
            ResolveObjectLocationRequest request,
            CancellationToken cancellationToken = default)
        {
            CallCount++;
            return ValueTask.FromResult<StorageResolvedObjectLocation?>(resolvedLocation);
        }
    }

    private sealed class CapturingLocationResolver(
        Func<ResolveObjectLocationRequest, StorageResolvedObjectLocation?> resolve) : IStorageObjectLocationResolver
    {
        public StorageSupportStateOwnership Ownership => StorageSupportStateOwnership.NotApplicable;

        public ValueTask<StorageResolvedObjectLocation?> ResolveReadLocationAsync(
            ResolveObjectLocationRequest request,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(request);
            return ValueTask.FromResult(resolve(request));
        }
    }

    private sealed class FakeTimeProvider(DateTimeOffset fixedNow) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => fixedNow;
    }

    private sealed class NonPresigningStorageBackend(string name, bool isPrimary) : PassiveStorageBackend(name, isPrimary);

    private sealed class DirectPresignStorageBackend(
        string name,
        bool isPrimary,
        StorageResult<StorageDirectObjectAccessGrant> result) : PassiveStorageBackend(name, isPrimary)
    {
        public StorageDirectObjectAccessRequest? LastRequest { get; private set; }

        public int CallCount { get; private set; }

        public override ValueTask<StorageResult<StorageDirectObjectAccessGrant>> PresignObjectDirectAsync(
            StorageDirectObjectAccessRequest request,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(request);
            cancellationToken.ThrowIfCancellationRequested();

            LastRequest = request;
            CallCount++;
            return ValueTask.FromResult(result);
        }
    }

    private abstract class PassiveStorageBackend(string name, bool isPrimary) : IStorageBackend
    {
        public string Name => name;

        public string Kind => "test";

        public bool IsPrimary => isPrimary;

        public string? Description => $"Passive test backend '{name}'.";

        public ValueTask<StorageCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(new StorageCapabilities());
        }

        public ValueTask<StorageSupportStateDescriptor> GetSupportStateDescriptorAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(new StorageSupportStateDescriptor());
        }

        public ValueTask<StorageProviderMode> GetProviderModeAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageProviderMode.Managed);
        }

        public ValueTask<StorageObjectLocationDescriptor> GetObjectLocationDescriptorAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(new StorageObjectLocationDescriptor());
        }

        public virtual ValueTask<StorageResult<StorageDirectObjectAccessGrant>> PresignObjectDirectAsync(
            StorageDirectObjectAccessRequest request,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(request);
            cancellationToken.ThrowIfCancellationRequested();

            return ValueTask.FromResult(StorageResult<StorageDirectObjectAccessGrant>.Failure(
                StorageError.Unsupported(
                    "Direct object presign generation is not implemented by this storage backend.",
                    request.BucketName,
                    request.Key)));
        }

        public async IAsyncEnumerable<BucketInfo> ListBucketsAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }

        public ValueTask<StorageResult<BucketInfo>> CreateBucketAsync(CreateBucketRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync<BucketInfo>();

        public ValueTask<StorageResult<BucketVersioningInfo>> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default) => UnexpectedAsync<BucketVersioningInfo>();

        public ValueTask<StorageResult<BucketVersioningInfo>> PutBucketVersioningAsync(PutBucketVersioningRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync<BucketVersioningInfo>();

        public ValueTask<StorageResult<BucketInfo>> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default) => UnexpectedAsync<BucketInfo>();

        public ValueTask<StorageResult> DeleteBucketAsync(DeleteBucketRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync();

        public async IAsyncEnumerable<ObjectInfo> ListObjectsAsync(ListObjectsRequest request, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }

        public async IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(ListObjectVersionsRequest request, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }

        public ValueTask<StorageResult<GetObjectResponse>> GetObjectAsync(GetObjectRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync<GetObjectResponse>();

        public ValueTask<StorageResult<ObjectTagSet>> GetObjectTagsAsync(GetObjectTagsRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync<ObjectTagSet>();

        public ValueTask<StorageResult<ObjectInfo>> CopyObjectAsync(CopyObjectRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync<ObjectInfo>();

        public ValueTask<StorageResult<ObjectInfo>> PutObjectAsync(PutObjectRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync<ObjectInfo>();

        public ValueTask<StorageResult<ObjectTagSet>> PutObjectTagsAsync(PutObjectTagsRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync<ObjectTagSet>();

        public ValueTask<StorageResult<ObjectTagSet>> DeleteObjectTagsAsync(DeleteObjectTagsRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync<ObjectTagSet>();

        public ValueTask<StorageResult<MultipartUploadInfo>> InitiateMultipartUploadAsync(InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync<MultipartUploadInfo>();

        public ValueTask<StorageResult<MultipartUploadPart>> UploadMultipartPartAsync(UploadMultipartPartRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync<MultipartUploadPart>();

        public ValueTask<StorageResult<ObjectInfo>> CompleteMultipartUploadAsync(CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync<ObjectInfo>();

        public ValueTask<StorageResult> AbortMultipartUploadAsync(AbortMultipartUploadRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync();

        public ValueTask<StorageResult<ObjectInfo>> HeadObjectAsync(HeadObjectRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync<ObjectInfo>();

        public ValueTask<StorageResult<DeleteObjectResult>> DeleteObjectAsync(DeleteObjectRequest request, CancellationToken cancellationToken = default) => UnexpectedAsync<DeleteObjectResult>();

        public ValueTask<StorageResult<BucketCorsConfiguration>> GetBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult<BucketCorsConfiguration>.Failure(StorageError.Unsupported("Bucket CORS is not used in presign strategy tests.", bucketName)));
        }

        public ValueTask<StorageResult<BucketCorsConfiguration>> PutBucketCorsAsync(PutBucketCorsRequest request, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(request);
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult<BucketCorsConfiguration>.Failure(StorageError.Unsupported("Bucket CORS is not used in presign strategy tests.", request.BucketName)));
        }

        public ValueTask<StorageResult> DeleteBucketCorsAsync(DeleteBucketCorsRequest request, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(request);
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult.Failure(StorageError.Unsupported("Bucket CORS is not used in presign strategy tests.", request.BucketName)));
        }

        public async IAsyncEnumerable<MultipartUploadInfo> ListMultipartUploadsAsync(ListMultipartUploadsRequest request, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(request);
            cancellationToken.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }

        private static ValueTask<StorageResult<T>> UnexpectedAsync<T>() => throw new NotSupportedException("This backend is only used for presign strategy tests.");

        private static ValueTask<StorageResult> UnexpectedAsync() => throw new NotSupportedException("This backend is only used for presign strategy tests.");
    }
}
