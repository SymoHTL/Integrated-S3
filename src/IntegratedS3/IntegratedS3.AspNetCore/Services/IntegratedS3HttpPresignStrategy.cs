using System.Security.Claims;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Core.Models;
using IntegratedS3.Core.Services;
using IntegratedS3.Protocol;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;

namespace IntegratedS3.AspNetCore.Services;

internal sealed class IntegratedS3HttpPresignStrategy(
    IOptions<IntegratedS3Options> options,
    IIntegratedS3PresignCredentialResolver credentialResolver,
    IEnumerable<IStorageBackend> backends,
    IHttpContextAccessor httpContextAccessor,
    IEnumerable<IStorageObjectLocationResolver> locationResolvers,
    TimeProvider timeProvider) : IStoragePresignStrategy
{
    private const string HostHeaderName = "host";
    private const string ContentTypeHeaderName = "content-type";
    private const string SdkChecksumAlgorithmHeaderName = "x-amz-sdk-checksum-algorithm";
    private const string ChecksumCrc32HeaderName = "x-amz-checksum-crc32";
    private const string ChecksumCrc32cHeaderName = "x-amz-checksum-crc32c";
    private const string ChecksumSha1HeaderName = "x-amz-checksum-sha1";
    private const string ChecksumSha256HeaderName = "x-amz-checksum-sha256";
    private readonly IStorageBackend[] _backends = backends.ToArray();
    private readonly IStorageObjectLocationResolver[] _locationResolvers = locationResolvers.ToArray();

    public async ValueTask<StorageResult<StoragePresignedRequest>> PresignObjectAsync(
        ClaimsPrincipal principal,
        StoragePresignRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(principal);
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        if (request.PreferredAccessMode == StorageAccessMode.Direct) {
            var directResult = await TryPresignDirectAsync(request, cancellationToken);
            if (directResult is not null) {
                return directResult;
            }
        }

        // When the caller prefers direct or delegated access for a read, try to resolve
        // a provider-supplied location before falling through to proxy-mode issuance.
        if (request.Operation == StoragePresignOperation.GetObject
            && request.PreferredAccessMode is StorageAccessMode.Direct or StorageAccessMode.Delegated) {
            var providerResult = await TryResolveProviderLocationAsync(request, cancellationToken);
            if (providerResult is not null) {
                return StorageResult<StoragePresignedRequest>.Success(providerResult);
            }
        }

        var settings = options.Value;
        if (!settings.EnableAwsSignatureV4Authentication) {
            return CreateFailure(
                request,
                StorageErrorCode.UnsupportedCapability,
                "First-party presign generation requires EnableAwsSignatureV4Authentication to be enabled.",
                statusCode: 501);
        }

        if (request.ExpiresInSeconds > settings.MaximumPresignedUrlExpirySeconds) {
            return CreateFailure(
                request,
                StorageErrorCode.InvalidRange,
                $"The presign expiry of {request.ExpiresInSeconds} seconds exceeds the maximum allowed value of {settings.MaximumPresignedUrlExpirySeconds} seconds.",
                statusCode: 400);
        }

        if (!TryResolveApplicationBaseUri(settings, httpContextAccessor.HttpContext, out var applicationBaseUri, out var baseUriError)) {
            return CreateFailure(
                request,
                StorageErrorCode.UnsupportedCapability,
                baseUriError!,
                statusCode: 501);
        }

        var credentialResolution = await credentialResolver.ResolveAsync(principal, cancellationToken);
        if (!credentialResolution.Succeeded || credentialResolution.Credential is null) {
            return CreateFailure(
                request,
                StorageErrorCode.AccessDenied,
                credentialResolution.ErrorMessage ?? "A signing credential could not be resolved for first-party presign generation.",
                statusCode: 403);
        }

        var targetUri = BuildTargetUri(applicationBaseUri!, settings.RoutePrefix, request);
        var signedHeaders = BuildSignedHeaders(targetUri, request);
        var method = ResolveMethod(request.Operation);

        var presignedRequest = S3SigV4Presigner.Presign(new S3SigV4PresignParameters
        {
            HttpMethod = method,
            Path = targetUri.AbsolutePath,
            QueryParameters = BuildQueryParameters(request),
            Headers = signedHeaders,
            SignedHeaders = signedHeaders.Select(static header => header.Key).ToArray(),
            AccessKeyId = credentialResolution.Credential.AccessKeyId,
            SecretAccessKey = credentialResolution.Credential.SecretAccessKey,
            Region = settings.SignatureAuthenticationRegion,
            Service = settings.SignatureAuthenticationService,
            SignedAtUtc = timeProvider.GetUtcNow(),
            ExpiresInSeconds = request.ExpiresInSeconds
        });

        return StorageResult<StoragePresignedRequest>.Success(new StoragePresignedRequest
        {
            Operation = request.Operation,
            AccessMode = StorageAccessMode.Proxy,
            Method = method,
            Url = BuildPresignedUri(targetUri, presignedRequest.QueryParameters),
            ExpiresAtUtc = presignedRequest.ExpiresAtUtc,
            BucketName = request.BucketName,
            Key = request.Key,
            VersionId = request.VersionId,
            ContentType = request.ContentType,
            Headers = BuildResponseHeaders(request)
        });
    }

    private async ValueTask<StorageResult<StoragePresignedRequest>?> TryPresignDirectAsync(
        StoragePresignRequest request,
        CancellationToken cancellationToken)
    {
        var primaryBackend = ResolvePrimaryBackend();
        if (primaryBackend is null) {
            return null;
        }

        var directResult = await primaryBackend.PresignObjectDirectAsync(
            new StorageDirectObjectAccessRequest
            {
                Operation = MapDirectOperation(request.Operation),
                BucketName = request.BucketName,
                Key = request.Key,
                ExpiresInSeconds = request.ExpiresInSeconds,
                VersionId = request.VersionId,
                ContentType = request.ContentType,
                ChecksumAlgorithm = request.ChecksumAlgorithm,
                Checksums = request.Checksums
            },
            cancellationToken);

        if (!directResult.IsSuccess) {
            return directResult.Error is null || directResult.Error.Code == StorageErrorCode.UnsupportedCapability
                ? null
                : StorageResult<StoragePresignedRequest>.Failure(directResult.Error);
        }

        if (directResult.Value is not { } directGrant) {
            return CreateFailure(
                request,
                StorageErrorCode.ProviderUnavailable,
                "The storage backend returned an empty direct presign grant.",
                statusCode: 502);
        }

        return StorageResult<StoragePresignedRequest>.Success(new StoragePresignedRequest
        {
            Operation = request.Operation,
            AccessMode = StorageAccessMode.Direct,
            Method = ResolveMethod(request.Operation),
            Url = directGrant.Url,
            ExpiresAtUtc = directGrant.ExpiresAtUtc,
            BucketName = request.BucketName,
            Key = request.Key,
            VersionId = request.VersionId,
            ContentType = request.ContentType,
            Headers = BuildResolvedHeaders(directGrant.Headers)
        });
    }

    private async ValueTask<StoragePresignedRequest?> TryResolveProviderLocationAsync(
        StoragePresignRequest request,
        CancellationToken cancellationToken)
    {
        var resolvedLocationRequest = new ResolveObjectLocationRequest
        {
            ProviderName = ResolvePrimaryBackend()?.Name ?? string.Empty,
            BucketName = request.BucketName,
            Key = request.Key,
            VersionId = request.VersionId,
            ExpiresAtUtc = timeProvider.GetUtcNow().AddSeconds(request.ExpiresInSeconds)
        };

        foreach (var resolver in _locationResolvers) {
            var resolved = await resolver.ResolveReadLocationAsync(resolvedLocationRequest, cancellationToken);
            if (resolved?.Location is null) {
                continue;
            }

            var mappedMode = MapResolvedAccessMode(resolved.AccessMode, request.PreferredAccessMode!.Value);
            if (mappedMode is null) {
                continue;
            }

            return new StoragePresignedRequest
            {
                Operation = request.Operation,
                AccessMode = mappedMode.Value,
                Method = ResolveMethod(request.Operation),
                Url = resolved.Location,
                ExpiresAtUtc = resolved.ExpiresAtUtc ?? resolvedLocationRequest.ExpiresAtUtc ?? timeProvider.GetUtcNow().AddSeconds(request.ExpiresInSeconds),
                BucketName = request.BucketName,
                Key = request.Key,
                VersionId = request.VersionId,
                ContentType = request.ContentType,
                Headers = BuildResolvedHeaders(resolved.Headers)
            };
        }

        return null;
    }

    /// <summary>
    /// Maps a resolver-supplied <see cref="StorageObjectAccessMode"/> to a <see cref="StorageAccessMode"/> that
    /// satisfies the caller's <paramref name="preferredMode"/>, or <see langword="null"/> if the resolved mode
    /// is incompatible with the preference and the strategy should fall back to proxy issuance.
    /// </summary>
    private static StorageAccessMode? MapResolvedAccessMode(
        StorageObjectAccessMode resolvedMode,
        StorageAccessMode preferredMode)
    {
        return preferredMode switch
        {
            StorageAccessMode.Delegated when resolvedMode is StorageObjectAccessMode.Delegated or StorageObjectAccessMode.Passthrough
                => StorageAccessMode.Delegated,
            StorageAccessMode.Direct when resolvedMode is StorageObjectAccessMode.Redirect
                => StorageAccessMode.Direct,
            _ => null
        };
    }

    private static IReadOnlyList<StoragePresignedHeader> BuildResolvedHeaders(Dictionary<string, string> headers)
    {
        if (headers.Count == 0) {
            return [];
        }

        return headers.Select(static kvp => new StoragePresignedHeader
        {
            Name = kvp.Key,
            Value = kvp.Value
        }).ToArray();
    }

    private IStorageBackend? ResolvePrimaryBackend()
    {
        return _backends.FirstOrDefault(static backend => backend.IsPrimary)
            ?? _backends.FirstOrDefault();
    }

    private static StorageDirectObjectAccessOperation MapDirectOperation(StoragePresignOperation operation)
    {
        return operation switch
        {
            StoragePresignOperation.GetObject => StorageDirectObjectAccessOperation.GetObject,
            StoragePresignOperation.PutObject => StorageDirectObjectAccessOperation.PutObject,
            _ => throw new ArgumentOutOfRangeException(nameof(operation), operation, "The requested direct-presign operation is not supported.")
        };
    }

    private static Uri BuildTargetUri(Uri applicationBaseUri, string routePrefix, StoragePresignRequest request)
    {
        var rawPath = $"{routePrefix}/buckets/{request.BucketName}/objects/{request.Key}";
        var escapedPath = new PathString(rawPath).ToUriComponent();
        return new Uri(applicationBaseUri, escapedPath.TrimStart('/'));
    }

    private static IReadOnlyList<KeyValuePair<string, string?>> BuildSignedHeaders(Uri targetUri, StoragePresignRequest request)
    {
        var headers = new List<KeyValuePair<string, string?>>
        {
            new(HostHeaderName, targetUri.IsDefaultPort ? targetUri.Host : targetUri.Authority)
        };

        if (request.Operation == StoragePresignOperation.PutObject
            && !string.IsNullOrWhiteSpace(request.ContentType)) {
            headers.Add(new KeyValuePair<string, string?>(ContentTypeHeaderName, request.ContentType));
        }

        if (request.Operation == StoragePresignOperation.PutObject
            && !string.IsNullOrWhiteSpace(request.ChecksumAlgorithm)
            && ToS3ChecksumAlgorithmValue(request.ChecksumAlgorithm) is { } checksumAlgorithmValue) {
            headers.Add(new KeyValuePair<string, string?>(SdkChecksumAlgorithmHeaderName, checksumAlgorithmValue));
        }

        if (request.Operation == StoragePresignOperation.PutObject
            && request.Checksums is not null) {
            foreach (var checksum in request.Checksums) {
                if (string.IsNullOrWhiteSpace(checksum.Value)
                    || ResolveChecksumHeaderName(checksum.Key) is not { } headerName) {
                    continue;
                }

                headers.Add(new KeyValuePair<string, string?>(headerName, checksum.Value));
            }
        }

        return headers;
    }

    private static IReadOnlyList<KeyValuePair<string, string?>> BuildQueryParameters(StoragePresignRequest request)
    {
        if (request.Operation == StoragePresignOperation.GetObject
            && !string.IsNullOrWhiteSpace(request.VersionId)) {
            return [new KeyValuePair<string, string?>("versionId", request.VersionId)];
        }

        return [];
    }

    private static IReadOnlyList<StoragePresignedHeader> BuildResponseHeaders(StoragePresignRequest request)
    {
        if (request.Operation == StoragePresignOperation.PutObject
            && (!string.IsNullOrWhiteSpace(request.ContentType)
                || !string.IsNullOrWhiteSpace(request.ChecksumAlgorithm)
                || request.Checksums is not null)) {
            var headers = new List<StoragePresignedHeader>();

            if (!string.IsNullOrWhiteSpace(request.ContentType)) {
                headers.Add(new StoragePresignedHeader
                {
                    Name = "Content-Type",
                    Value = request.ContentType
                });
            }

            if (!string.IsNullOrWhiteSpace(request.ChecksumAlgorithm)
                && ToS3ChecksumAlgorithmValue(request.ChecksumAlgorithm) is { } checksumAlgorithmValue) {
                headers.Add(new StoragePresignedHeader
                {
                    Name = SdkChecksumAlgorithmHeaderName,
                    Value = checksumAlgorithmValue
                });
            }

            if (request.Checksums is not null) {
                foreach (var checksum in request.Checksums) {
                    if (string.IsNullOrWhiteSpace(checksum.Value)
                        || ResolveChecksumHeaderName(checksum.Key) is not { } headerName) {
                        continue;
                    }

                    headers.Add(new StoragePresignedHeader
                    {
                        Name = headerName,
                        Value = checksum.Value
                    });
                }
            }

            return headers;
        }

        return [];
    }

    private static string? ResolveChecksumHeaderName(string? checksumAlgorithm)
    {
        return checksumAlgorithm switch
        {
            "crc32" => ChecksumCrc32HeaderName,
            "crc32c" => ChecksumCrc32cHeaderName,
            "sha1" => ChecksumSha1HeaderName,
            "sha256" => ChecksumSha256HeaderName,
            _ => null
        };
    }

    private static string? ToS3ChecksumAlgorithmValue(string? checksumAlgorithm)
    {
        return checksumAlgorithm switch
        {
            "sha256" => "SHA256",
            "sha1" => "SHA1",
            "crc32" => "CRC32",
            "crc32c" => "CRC32C",
            _ => null
        };
    }

    private static string ResolveMethod(StoragePresignOperation operation)
    {
        return operation switch
        {
            StoragePresignOperation.GetObject => HttpMethods.Get,
            StoragePresignOperation.PutObject => HttpMethods.Put,
            _ => throw new ArgumentOutOfRangeException(nameof(operation), operation, "The requested presign operation is not supported.")
        };
    }

    private static bool TryResolveApplicationBaseUri(
        IntegratedS3Options options,
        HttpContext? httpContext,
        out Uri? applicationBaseUri,
        out string? error)
    {
        if (httpContext?.Request.Host.HasValue == true) {
            var request = httpContext.Request;
            applicationBaseUri = EnsureTrailingSlash(new UriBuilder(request.Scheme, request.Host.Host, request.Host.Port ?? -1)
            {
                Path = request.PathBase.HasValue ? $"{request.PathBase.Value!.TrimEnd('/')}/" : "/"
            }.Uri);
            error = null;
            return true;
        }

        if (!string.IsNullOrWhiteSpace(options.PresignPublicBaseUrl)
            && Uri.TryCreate(options.PresignPublicBaseUrl, UriKind.Absolute, out var configuredBaseUri)) {
            applicationBaseUri = EnsureTrailingSlash(new UriBuilder(configuredBaseUri)
            {
                Query = string.Empty,
                Fragment = string.Empty
            }.Uri);
            error = null;
            return true;
        }

        applicationBaseUri = null;
        error = "A public base URL could not be resolved for presign generation. Use an active HTTP request or configure PresignPublicBaseUrl.";
        return false;
    }

    private static Uri EnsureTrailingSlash(Uri uri)
    {
        return uri.AbsoluteUri.EndsWith("/", StringComparison.Ordinal)
            ? uri
            : new Uri($"{uri.AbsoluteUri}/", UriKind.Absolute);
    }

    private static Uri BuildPresignedUri(Uri targetUri, IReadOnlyList<KeyValuePair<string, string?>> queryParameters)
    {
        var queryText = string.Join("&", queryParameters.Select(static pair =>
            $"{Uri.EscapeDataString(pair.Key)}={Uri.EscapeDataString(pair.Value ?? string.Empty)}"));

        return new UriBuilder(targetUri)
        {
            Query = queryText
        }.Uri;
    }

    private static StorageResult<StoragePresignedRequest> CreateFailure(
        StoragePresignRequest request,
        StorageErrorCode code,
        string message,
        int statusCode)
    {
        return StorageResult<StoragePresignedRequest>.Failure(new StorageError
        {
            Code = code,
            Message = message,
            BucketName = request.BucketName,
            ObjectKey = request.Key,
            VersionId = request.VersionId,
            SuggestedHttpStatusCode = statusCode
        });
    }
}
