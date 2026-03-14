using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Core.Options;
using IntegratedS3.Core.Services;
using Microsoft.Extensions.Options;

namespace IntegratedS3.AspNetCore.Services;

internal sealed class BucketCorsRuntimeService(
    IEnumerable<IStorageBackend> backends,
    IOptions<IntegratedS3CoreOptions> coreOptions,
    IStorageBackendHealthEvaluator healthEvaluator)
{
    private readonly IStorageBackend[] _backends = backends.ToArray();
    private readonly Lazy<IStorageBackend> _primaryBackend = new(() => ResolvePrimaryBackend(backends.ToArray()));

    public async ValueTask<BucketCorsActualResponse?> GetActualResponseAsync(
        string bucketName,
        string? origin,
        string requestMethod,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(bucketName)
            || string.IsNullOrWhiteSpace(origin)
            || !TryParseCorsMethod(requestMethod, out var method)) {
            return null;
        }

        var configurationResult = await GetBucketCorsAsync(bucketName, cancellationToken);
        if (!configurationResult.IsSuccess || configurationResult.Value is null) {
            return null;
        }

        var match = FindMatchingRule(configurationResult.Value, origin, method);
        if (match is null) {
            return null;
        }

        return new BucketCorsActualResponse(
            match.Value.AllowOrigin,
            match.Value.AllowCredentials,
            NormalizeHeaderList(match.Value.Rule.ExposeHeaders));
    }

    public async ValueTask<StorageResult<BucketCorsPreflightResponse?>> GetPreflightResponseAsync(
        string bucketName,
        string origin,
        string requestedMethod,
        IReadOnlyList<string> requestedHeaders,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(bucketName)) {
            throw new ArgumentException("Bucket name is required.", nameof(bucketName));
        }

        if (!TryParseCorsMethod(requestedMethod, out var method)) {
            return StorageResult<BucketCorsPreflightResponse?>.Success(null);
        }

        var configurationResult = await GetBucketCorsAsync(bucketName, cancellationToken);
        if (!configurationResult.IsSuccess) {
            return configurationResult.Error?.Code == StorageErrorCode.CorsConfigurationNotFound
                ? StorageResult<BucketCorsPreflightResponse?>.Success(null)
                : StorageResult<BucketCorsPreflightResponse?>.Failure(configurationResult.Error!);
        }

        var normalizedRequestedHeaders = NormalizeHeaderList(requestedHeaders);
        var match = FindMatchingRule(configurationResult.Value!, origin, method, normalizedRequestedHeaders);
        if (match is null) {
            return StorageResult<BucketCorsPreflightResponse?>.Success(null);
        }

        return StorageResult<BucketCorsPreflightResponse?>.Success(new BucketCorsPreflightResponse(
            match.Value.AllowOrigin,
            match.Value.AllowCredentials,
            requestedMethod.Trim().ToUpperInvariant(),
            normalizedRequestedHeaders,
            NormalizeHeaderList(match.Value.Rule.ExposeHeaders),
            match.Value.Rule.MaxAgeSeconds));
    }

    private async ValueTask<StorageResult<BucketCorsConfiguration>> GetBucketCorsAsync(string bucketName, CancellationToken cancellationToken)
    {
        StorageResult<BucketCorsConfiguration>? lastFailure = null;

        foreach (var backend in await GetOrderedReadBackendsAsync(cancellationToken)) {
            var result = await backend.GetBucketCorsAsync(bucketName, cancellationToken);
            if (result.IsSuccess) {
                return result;
            }

            lastFailure = result;
            if (!ShouldFailoverRead(result.Error)) {
                return result;
            }
        }

        return lastFailure ?? StorageResult<BucketCorsConfiguration>.Failure(new StorageError
        {
            Code = StorageErrorCode.ProviderUnavailable,
            Message = "No storage backend could resolve the bucket CORS configuration.",
            BucketName = bucketName,
            ProviderName = _primaryBackend.Value.Name,
            SuggestedHttpStatusCode = 503
        });
    }

    private async ValueTask<IReadOnlyList<IStorageBackend>> GetOrderedReadBackendsAsync(CancellationToken cancellationToken)
    {
        var primaryBackend = _primaryBackend.Value;
        if (_backends.Length <= 1 || coreOptions.Value.ReadRoutingMode == StorageReadRoutingMode.PrimaryOnly) {
            return [primaryBackend];
        }

        var candidates = new List<ReadBackendCandidate>(_backends.Length);
        foreach (var backend in _backends) {
            var healthStatus = await healthEvaluator.GetStatusAsync(backend, cancellationToken);
            candidates.Add(new ReadBackendCandidate(backend, healthStatus));
        }

        return candidates
            .OrderBy(candidate => GetReadPriority(candidate, primaryBackend, coreOptions.Value.ReadRoutingMode))
            .ThenBy(candidate => GetOriginalIndex(candidate.Backend))
            .Select(candidate => candidate.Backend)
            .ToArray();
    }

    private BucketCorsRuleMatch? FindMatchingRule(
        BucketCorsConfiguration configuration,
        string origin,
        BucketCorsMethod method,
        IReadOnlyList<string>? requestedHeaders = null)
    {
        foreach (var rule in configuration.Rules) {
            var matchedOriginPattern = FindMatchingPattern(rule.AllowedOrigins, origin, StringComparison.OrdinalIgnoreCase);
            if (matchedOriginPattern is null) {
                continue;
            }

            if (!rule.AllowedMethods.Contains(method)) {
                continue;
            }

            if (requestedHeaders is not null && !AreRequestedHeadersAllowed(rule.AllowedHeaders, requestedHeaders)) {
                continue;
            }

            var allowOrigin = matchedOriginPattern == "*"
                ? "*"
                : origin.Trim();
            return new BucketCorsRuleMatch(rule, allowOrigin, allowOrigin != "*");
        }

        return null;
    }

    private static bool AreRequestedHeadersAllowed(IReadOnlyList<string> allowedHeaders, IReadOnlyList<string> requestedHeaders)
    {
        if (requestedHeaders.Count == 0) {
            return true;
        }

        if (allowedHeaders.Count == 0) {
            return false;
        }

        foreach (var requestedHeader in requestedHeaders) {
            if (!MatchesAnyPattern(allowedHeaders, requestedHeader, StringComparison.OrdinalIgnoreCase)) {
                return false;
            }
        }

        return true;
    }

    private static bool MatchesAnyPattern(IReadOnlyList<string> patterns, string candidate, StringComparison comparison)
    {
        for (var index = 0; index < patterns.Count; index++) {
            if (MatchesPattern(patterns[index], candidate, comparison)) {
                return true;
            }
        }

        return false;
    }

    private static string? FindMatchingPattern(IReadOnlyList<string> patterns, string candidate, StringComparison comparison)
    {
        for (var index = 0; index < patterns.Count; index++) {
            if (MatchesPattern(patterns[index], candidate, comparison)) {
                return patterns[index];
            }
        }

        return null;
    }

    private static bool MatchesPattern(string pattern, string candidate, StringComparison comparison)
    {
        if (string.IsNullOrWhiteSpace(pattern) || string.IsNullOrWhiteSpace(candidate)) {
            return false;
        }

        if (pattern == "*") {
            return true;
        }

        var wildcardIndex = pattern.IndexOf('*');
        if (wildcardIndex < 0) {
            return string.Equals(pattern, candidate, comparison);
        }

        if (pattern.IndexOf('*', wildcardIndex + 1) >= 0) {
            return false;
        }

        var prefix = pattern[..wildcardIndex];
        var suffix = pattern[(wildcardIndex + 1)..];

        return candidate.Length >= prefix.Length + suffix.Length
            && candidate.StartsWith(prefix, comparison)
            && candidate.EndsWith(suffix, comparison);
    }

    private static IReadOnlyList<string> NormalizeHeaderList(IReadOnlyList<string> headers)
    {
        if (headers.Count == 0) {
            return [];
        }

        var normalized = new List<string>(headers.Count);
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var header in headers) {
            if (string.IsNullOrWhiteSpace(header)) {
                continue;
            }

            var trimmed = header.Trim();
            if (seen.Add(trimmed)) {
                normalized.Add(trimmed);
            }
        }

        return normalized;
    }

    private static bool TryParseCorsMethod(string rawMethod, out BucketCorsMethod method)
    {
        switch (rawMethod?.Trim().ToUpperInvariant()) {
            case "GET":
                method = BucketCorsMethod.Get;
                return true;
            case "PUT":
                method = BucketCorsMethod.Put;
                return true;
            case "POST":
                method = BucketCorsMethod.Post;
                return true;
            case "DELETE":
                method = BucketCorsMethod.Delete;
                return true;
            case "HEAD":
                method = BucketCorsMethod.Head;
                return true;
            default:
                method = default;
                return false;
        }
    }

    private static IStorageBackend ResolvePrimaryBackend(IEnumerable<IStorageBackend> backends)
    {
        var resolvedBackends = backends.ToArray();
        return resolvedBackends.FirstOrDefault(static backend => backend.IsPrimary)
            ?? resolvedBackends.FirstOrDefault()
            ?? throw new InvalidOperationException("No storage backends have been registered.");
    }

    private int GetOriginalIndex(IStorageBackend backend)
    {
        for (var index = 0; index < _backends.Length; index++) {
            if (ReferenceEquals(_backends[index], backend)) {
                return index;
            }
        }

        return int.MaxValue;
    }

    private static int GetReadPriority(ReadBackendCandidate candidate, IStorageBackend primaryBackend, StorageReadRoutingMode readRoutingMode)
    {
        var isPrimary = ReferenceEquals(candidate.Backend, primaryBackend);
        return readRoutingMode switch
        {
            StorageReadRoutingMode.PreferPrimary => candidate.HealthStatus switch
            {
                StorageBackendHealthStatus.Healthy when isPrimary => 0,
                StorageBackendHealthStatus.Healthy => 1,
                StorageBackendHealthStatus.Unknown when isPrimary => 2,
                StorageBackendHealthStatus.Unknown => 3,
                StorageBackendHealthStatus.Unhealthy when isPrimary => 4,
                _ => 5
            },
            StorageReadRoutingMode.PreferHealthyReplica => candidate.HealthStatus switch
            {
                StorageBackendHealthStatus.Healthy when !isPrimary => 0,
                StorageBackendHealthStatus.Healthy => 1,
                StorageBackendHealthStatus.Unknown when !isPrimary => 2,
                StorageBackendHealthStatus.Unknown => 3,
                StorageBackendHealthStatus.Unhealthy when !isPrimary => 4,
                _ => 5
            },
            _ => isPrimary ? 0 : 1
        };
    }

    private static bool ShouldFailoverRead(StorageError? error)
    {
        return error?.Code is StorageErrorCode.ProviderUnavailable or StorageErrorCode.Throttled;
    }

    private readonly record struct BucketCorsRuleMatch(BucketCorsRule Rule, string AllowOrigin, bool AllowCredentials);
    private readonly record struct ReadBackendCandidate(IStorageBackend Backend, StorageBackendHealthStatus HealthStatus);
}

internal sealed record BucketCorsActualResponse(
    string AllowOrigin,
    bool AllowCredentials,
    IReadOnlyList<string> ExposeHeaders);

internal sealed record BucketCorsPreflightResponse(
    string AllowOrigin,
    bool AllowCredentials,
    string AllowMethod,
    IReadOnlyList<string> AllowHeaders,
    IReadOnlyList<string> ExposeHeaders,
    int? MaxAgeSeconds);
